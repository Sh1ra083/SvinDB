//! Контур измерений ядра.
//!
//! ## Зачем он заведён
//!
//! Заявлений о скорости в этом дереве много, а защитить их в CI было нечем: ни
//! `criterion`, ни `[[bench]]`, а сам `svindb-bench` даже не входил в
//! workspace и не собирался. Числа снимались руками через HTTP, то есть вместе
//! с разбором JSON, сетью и пулом потоков — а оптимизировать предстоит обход
//! колонки, который во всём этом занимает проценты.
//!
//! Поэтому здесь меряется **ядро напрямую**: `Segments` без сервера.
//!
//! ## Почему своими руками, а не `criterion`
//!
//! По той же причине, по которой в ядре нет зависимостей вовсе: своё сжатие,
//! свой JSON, свои контрольные суммы. Добавить дерево зависимостей ради таймера
//! и медианы значило бы завести исключение там, где правило держится намеренно.
//! Нужно ровно три вещи: прогрев, лучшее из N и медиана.
//!
//! ## Каждый замер обязан проверить свой ответ
//!
//! Первая же сборка этого файла показала `0,000 мс` на `match_all` — и это был
//! не рекорд, а пустой запрос: родная форма требует пустого булева дерева, без
//! него совпадений нет вовсе. Замер, не проверяющий ответ, меряет тишину и
//! рапортует об успехе. Поэтому [`Case::run`] возвращает пересчитанное число, а
//! [`check`] сверяет его с известным наперёд и роняет прогон при расхождении.
//!
//! ## Договор вывода
//!
//! `--json` печатает машинный вид, `--baseline` сравнивает с прошлым прогоном и
//! возвращает ненулевой код, если стало хуже больше чем на `--max-regress`
//! процентов. Это и есть ворота: заявление «стало быстрее» либо подтверждается
//! файлом, либо не делается.

use std::time::Instant;

use svindb_core::aggs::{Agg, Kind as AggKind, Over, Space, Value};
use svindb_core::index::Kind;
use svindb_core::indexer::{Cell, Indexer};
use svindb_core::segments::{AggOutcome, QuerySpec, Segments, SpecBool, SpecRange, SpecWord, Term};

/// Поля корпуса. Порядок задаёт номера, на которые ссылаются замеры.
const F_TEXT: usize = 0;
const F_VIEWS: usize = 1;
const F_CHANNEL: usize = 2;
const F_KW: usize = 3;

/// Различных значений у `channel` и `kw`.
const DISTINCT: u64 = 137;
/// Один документ из скольких несёт редкое слово.
const RARE_EVERY: u64 = 10_000;

const RUNS: usize = 15;
const WARM: usize = 3;

/// Ниже этого порога ворота не срабатывают.
///
/// Замер в одну микросекунду меняется на десятки процентов от того, куда лёг
/// планировщик. Судить по нему — значит завести генератор ложных отказов:
/// первый же прогон отметил `search/rare_word` как ухудшение на 9,4%, пройдя
/// путь `0,001 мс` → `0,001 мс`. Такие замеры печатаются, но не судятся.
const GATE_FLOOR_MS: f64 = 0.05;

struct Opts {
    docs: u32,
    /// Документов на сегмент. Один сегмент — тот случай, где параллелизма нет
    /// и где отставание видно; много сегментов — проверка, что он появился.
    per_segment: u32,
    threads: usize,
    json: bool,
    baseline: Option<String>,
    max_regress: f64,
    filter: Option<String>,
    keep: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            docs: 200_000,
            per_segment: u32::MAX,
            threads: 1,
            json: false,
            baseline: None,
            max_regress: 5.0,
            filter: None,
            keep: false,
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "svindb-bench — измерение ядра без сервера

  --docs=N            документов в корпусе (умолчание 200000)
  --per-segment=N     документов на сегмент (умолчание: все в один)
  --threads=N         потоков, отдаваемых агрегации и поиску (умолчание 1)
  --only=подстрока    гонять только совпавшие замеры
  --json              машинный вывод
  --baseline=файл     сравнить с прошлым прогоном в формате --json
  --max-regress=ПРОЦ  насколько допустимо ухудшение (умолчание 5)
  --keep              не удалять корпус после прогона

Один сегмент и один поток — это тот случай, где агрегация упирается в обход
колонки. Он и взят умолчанием: мерить надо то, что чинится."
    );
    std::process::exit(2)
}

fn parse_args() -> Opts {
    let mut o = Opts::default();
    for a in std::env::args().skip(1) {
        let (key, val) = match a.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (a.as_str(), None),
        };
        let num = |v: Option<&str>| -> u64 {
            v.and_then(|v| v.parse().ok()).filter(|n| *n > 0).unwrap_or_else(|| usage())
        };
        match key {
            "--docs" => o.docs = u32::try_from(num(val)).unwrap_or_else(|_| usage()),
            "--per-segment" => o.per_segment = u32::try_from(num(val)).unwrap_or_else(|_| usage()),
            "--threads" => o.threads = usize::try_from(num(val)).unwrap_or_else(|_| usage()),
            "--only" => {
                o.filter =
                    Some(val.filter(|v| !v.is_empty()).unwrap_or_else(|| usage()).to_string())
            }
            "--json" => o.json = true,
            "--keep" => o.keep = true,
            "--baseline" => {
                o.baseline =
                    Some(val.filter(|v| !v.is_empty()).unwrap_or_else(|| usage()).to_string())
            }
            "--max-regress" => {
                o.max_regress = val
                    .and_then(|v| v.parse().ok())
                    .filter(|v: &f64| v.is_finite() && *v >= 0.0)
                    .unwrap_or_else(|| usage())
            }
            "-h" | "--help" => usage(),
            _ => usage(),
        }
    }
    o
}

/// Строит корпус, похожий на боевой по форме, а не по объёму.
///
/// Значения выбраны так, чтобы у каждого замера был известный наперёд ответ:
/// `views` — `d % 1000`, `channel` и `kw` — `d % 137`, редкое слово стоит у
/// одного документа из десяти тысяч.
fn build(dir: &str, docs: u32, per_segment: u32) {
    std::fs::remove_dir_all(dir).ok();
    let mut ix = Indexer::typed(
        dir,
        vec!["text".into(), "views".into(), "channel".into(), "kw".into()],
        vec![1.0; 4],
        vec![Kind::Text, Kind::Int64, Kind::Int64, Kind::Keyword],
        per_segment,
    );
    for d in 0..docs {
        let text = if u64::from(d) % RARE_EVERY == 7 {
            format!("документ {d} редкоеслово общее")
        } else {
            format!("документ {d} общее слово{}", d % 97)
        };
        let views = (d % 1000).to_string();
        let channel = (u64::from(d) % DISTINCT).to_string();
        let kw = format!("к{}", u64::from(d) % DISTINCT);
        let cells = vec![
            Cell { field: F_TEXT, slot: 0, text: &text },
            Cell { field: F_VIEWS, slot: 0, text: &views },
            Cell { field: F_CHANNEL, slot: 0, text: &channel },
            Cell { field: F_KW, slot: 0, text: &kw },
        ];
        ix.add_cells(&format!("d{d:09}"), &[1, 1, 1, 1], &cells);
    }
    ix.flush_now();
}

/// `match_all` в том виде, в каком его строит HTTP-слой: пустое булево дерево.
///
/// Без `nested` это не «все документы», а запрос без единого условия, который
/// не находит ничего и меряется в ноль.
fn all() -> QuerySpec {
    QuerySpec { nested: Some(SpecBool::default()), k: 1, ..QuerySpec::default() }
}

/// Диапазон, накрывающий весь корпус. Ответ тот же, что у [`all`], —
/// расходится только цена, и ровно это здесь и меряется.
fn full_range() -> QuerySpec {
    QuerySpec {
        filter: vec![SpecRange { fid: F_VIEWS, from: 0, to: i64::MAX }],
        k: 1,
        ..QuerySpec::default()
    }
}

/// Диапазон по `channel`, накрывающий весь корпус.
///
/// Нужен, чтобы отделить цену самого отбора от повторного чтения колонки: в
/// [`full_range`] фильтр и агрегация смотрят одно поле, и колонка читается
/// дважды за документ. Здесь поля разные.
fn full_range_other() -> QuerySpec {
    QuerySpec {
        filter: vec![SpecRange { fid: F_CHANNEL, from: 0, to: i64::MAX }],
        k: 1,
        ..QuerySpec::default()
    }
}

/// Два условия, оба накрывают весь корпус.
///
/// Два — намеренно: `count_by_buckets` берётся за дело только при **ровно
/// одном** условии, и второе закрывает быстрый путь по корзинам. Остаётся
/// чистый обход с двумя чтениями колонки и без чтения со стороны агрегации —
/// то, чем меряется цена одного чтения значения.
fn range_two() -> QuerySpec {
    QuerySpec {
        filter: vec![
            SpecRange { fid: F_VIEWS, from: 0, to: i64::MAX },
            SpecRange { fid: F_CHANNEL, from: 0, to: i64::MAX },
        ],
        k: 1,
        ..QuerySpec::default()
    }
}

/// Половина корпуса: `views` разложены как `d % 1000`.
fn half_range() -> QuerySpec {
    QuerySpec {
        filter: vec![SpecRange { fid: F_VIEWS, from: 0, to: 499 }],
        k: 1,
        ..QuerySpec::default()
    }
}

fn word(fid: usize, term: &str, k: usize) -> QuerySpec {
    QuerySpec {
        words: vec![SpecWord { given: None, fid, terms: vec![Term::parse(term)] }],
        k,
        ..QuerySpec::default()
    }
}

fn agg(name: &str, kind: AggKind) -> Agg {
    Agg { name: name.into(), kind, over: Over::Docs, space: Space::Int }
}

/// Число наблюдений из первой агрегации ответа.
fn count_of(o: &AggOutcome) -> u64 {
    match &o.values.first().expect("агрегация не вернула значений").value
    {
        Value::Stats { count, .. } => *count,
        other => panic!("ожидалась статистика, получено {other:?}"),
    }
}

/// Сумма наблюдений по корзинам.
fn buckets_of(o: &AggOutcome) -> u64 {
    match &o.values.first().expect("агрегация не вернула значений").value
    {
        Value::Buckets(b) => b.iter().map(|(_, n)| *n).sum(),
        other => panic!("ожидались корзины, получено {other:?}"),
    }
}

/// Верхушка значений: наблюдения в корзинах плюс явно названный остаток.
///
/// Сумма обязана сойтись с корпусом — именно это и делает замер `terms`
/// проверяемым, хотя сама верхушка обрезана до `shard_size`.
fn terms_of(o: &AggOutcome) -> u64 {
    match &o.values.first().expect("агрегация не вернула значений").value
    {
        Value::Terms { top, other, .. } => top.iter().map(|(_, n)| *n).sum::<u64>() + *other,
        other => panic!("ожидалась верхушка значений, получено {other:?}"),
    }
}

/// Оценка мощности.
fn cardinality_of(o: &AggOutcome) -> u64 {
    match &o.values.first().expect("агрегация не вернула значений").value
    {
        Value::Cardinality { value } => *value,
        other => panic!("ожидалась мощность, получено {other:?}"),
    }
}

/// Сколько документов несут редкое слово при корпусе в `docs` документов.
fn rare_count(docs: u64) -> u64 {
    if docs <= 7 {
        0
    } else {
        (docs - 8) / RARE_EVERY + 1
    }
}

/// Что делает замер: считает по составу заданным числом потоков и возвращает
/// пересчитанный ответ.
type Run = Box<dyn Fn(&Segments, usize) -> u64>;

struct Case {
    name: &'static str,
    /// Пересчитанный ответ. Сверяется с [`Case::want`] до всякого замера.
    run: Run,
    /// Что обязано получиться. Разошлось — прогон падает, а не рапортует.
    want: u64,
    /// Допуск: у мощности ответ приближённый по определению.
    tolerance: u64,
}

fn cases(docs: u64) -> Vec<Case> {
    let mut v: Vec<Case> = Vec::new();
    let mut add = |name, run, want, tolerance| v.push(Case { name, run, want, tolerance });

    add(
        "agg/match_all+count",
        Box::new(|s: &Segments, t| {
            let a = [agg("n", AggKind::Stats { field: None })];
            count_of(&s.aggregate_with(&all(), &a, t))
        }),
        docs,
        0,
    );
    add(
        "agg/match_all+stats",
        Box::new(|s: &Segments, t| {
            let a = [agg("v", AggKind::Stats { field: Some(F_VIEWS) })];
            count_of(&s.aggregate_with(&all(), &a, t))
        }),
        docs,
        0,
    );
    // Тот же ответ, что и у предыдущего, но записанный диапазоном: замерен
    // трёхкратный штраф за форму запроса, и он должен исчезнуть.
    add(
        "agg/range_all+stats",
        Box::new(|s: &Segments, t| {
            let a = [agg("v", AggKind::Stats { field: Some(F_VIEWS) })];
            count_of(&s.aggregate_with(&full_range(), &a, t))
        }),
        docs,
        0,
    );
    // Отбор по тому же полю, но без чтения колонки агрегацией: остаётся чистая
    // цена самого отбора.
    add(
        "agg/range_all+count",
        Box::new(|s: &Segments, t| {
            let a = [agg("n", AggKind::Stats { field: None })];
            count_of(&s.aggregate_with(&full_range(), &a, t))
        }),
        docs,
        0,
    );
    // Два условия и счёт: быстрый путь по корзинам закрыт, чтений колонки два,
    // а агрегация не читает ничего. Разность с `match_all+count` делится на два
    // и даёт цену одного чтения значения.
    add(
        "agg/range_two+count",
        Box::new(|s: &Segments, t| {
            let a = [agg("n", AggKind::Stats { field: None })];
            count_of(&s.aggregate_with(&range_two(), &a, t))
        }),
        docs,
        0,
    );
    // Отбор и агрегация по разным полям: колонка читается дважды, но разная.
    add(
        "agg/range_other+stats",
        Box::new(|s: &Segments, t| {
            let a = [agg("v", AggKind::Stats { field: Some(F_VIEWS) })];
            count_of(&s.aggregate_with(&full_range_other(), &a, t))
        }),
        docs,
        0,
    );
    add(
        "agg/range_half+stats",
        Box::new(|s: &Segments, t| {
            let a = [agg("v", AggKind::Stats { field: Some(F_VIEWS) })];
            count_of(&s.aggregate_with(&half_range(), &a, t))
        }),
        docs.div_ceil(2),
        0,
    );
    add(
        "agg/match_all+terms",
        Box::new(|s: &Segments, t| {
            let a = [agg("c", AggKind::Terms { field: F_KW, size: 10 })];
            terms_of(&s.aggregate_with(&all(), &a, t))
        }),
        docs,
        0,
    );
    add(
        "agg/match_all+cardinality",
        Box::new(|s: &Segments, t| {
            let a = [agg("c", AggKind::Cardinality { field: F_CHANNEL })];
            cardinality_of(&s.aggregate_with(&all(), &a, t))
        }),
        DISTINCT,
        DISTINCT / 10,
    );
    add(
        "agg/match_all+histogram",
        Box::new(|s: &Segments, t| {
            let a = [agg(
                "h",
                AggKind::Histogram {
                    field: F_VIEWS,
                    interval: 100,
                    offset: 0,
                    step: svindb_core::aggs::Step::Int { interval: 100, offset: 0 },
                },
            )];
            buckets_of(&s.aggregate_with(&all(), &a, t))
        }),
        docs,
        0,
    );
    add(
        "agg/word+stats",
        Box::new(|s: &Segments, t| {
            let a = [agg("v", AggKind::Stats { field: Some(F_VIEWS) })];
            count_of(&s.aggregate_with(&word(F_TEXT, "общее", 1), &a, t))
        }),
        docs,
        0,
    );
    add(
        "agg/rare_word+stats",
        Box::new(|s: &Segments, t| {
            let a = [agg("v", AggKind::Stats { field: Some(F_VIEWS) })];
            count_of(&s.aggregate_with(&word(F_TEXT, "редкоеслово", 1), &a, t))
        }),
        rare_count(docs),
        0,
    );

    // Замерено, что `match_all` при `k=1` обходит весь корпус без раннего
    // выхода. Он здесь, чтобы это стало числом, а не наблюдением.
    add(
        "search/match_all_k1",
        Box::new(|s: &Segments, t| s.search_with(&all(), t).hits.len() as u64),
        1,
        0,
    );
    add(
        "search/rare_word",
        Box::new(|s: &Segments, t| {
            s.search_with(&word(F_TEXT, "редкоеслово", 10), t).hits.len() as u64
        }),
        rare_count(docs).min(10),
        0,
    );
    add(
        "search/frequent_word",
        Box::new(|s: &Segments, t| s.search_with(&word(F_TEXT, "общее", 10), t).hits.len() as u64),
        10.min(docs),
        0,
    );
    add(
        "search/frequent_word_k1000",
        Box::new(|s: &Segments, t| {
            s.search_with(&word(F_TEXT, "общее", 1000), t).hits.len() as u64
        }),
        1000.min(docs),
        0,
    );
    v
}

/// Сверяет ответ до замера. Расхождение — отказ, а не строка в выводе.
fn check(case: &Case, segs: &Segments, threads: usize) {
    if case.tolerance == u64::MAX {
        return;
    }
    let got = (case.run)(segs, threads);
    let ok = got.abs_diff(case.want) <= case.tolerance;
    assert!(
        ok,
        "замер {} считает не то: получено {got}, ожидалось {} (допуск {})",
        case.name, case.want, case.tolerance
    );
}

struct Measured {
    name: &'static str,
    best_ms: f64,
    median_ms: f64,
}

fn measure(case: &Case, segs: &Segments, threads: usize) -> Measured {
    let mut guard = 0u64;
    for _ in 0..WARM {
        guard = guard.wrapping_add((case.run)(segs, threads));
    }
    let mut ts: Vec<f64> = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let t = Instant::now();
        guard = guard.wrapping_add((case.run)(segs, threads));
        ts.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    // Контрольное число обязано быть использовано, иначе работу вправе выбросить.
    std::hint::black_box(guard);
    ts.sort_by(f64::total_cmp);
    Measured { name: case.name, best_ms: ts[0], median_ms: ts[RUNS / 2] }
}

struct Baseline {
    format: u64,
    docs: u64,
    per_segment: u64,
    segments: u64,
    threads: u64,
    os: String,
    arch: String,
    measurements: Vec<(String, f64)>,
}

/// Разбор прошлого прогона. Формат свой и намеренно простой: строка на поле.
fn read_baseline(path: &str) -> Result<Baseline, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let mut out = Vec::new();
    let mut format = None;
    let mut docs = None;
    let mut per_segment = None;
    let mut segments = None;
    let mut threads = None;
    let mut os = None;
    let mut arch = None;
    for line in text.lines() {
        let line = line.trim().trim_start_matches('{').trim_end_matches([',', '}']);
        let Some((name, rest)) = line.split_once("\":") else { continue };
        let name = name.trim().trim_start_matches('"');
        let rest = rest.trim();
        match name {
            "_format" => format = rest.parse().ok(),
            "_docs" => docs = rest.parse().ok(),
            "_per_segment" => per_segment = rest.parse().ok(),
            "_segments" => segments = rest.parse().ok(),
            "_threads" => threads = rest.parse().ok(),
            "_os" => os = Some(rest.trim_matches('"').to_string()),
            "_arch" => arch = Some(rest.trim_matches('"').to_string()),
            _ if name.starts_with('_') => {}
            _ => {
                let Ok(v) = rest.parse::<f64>() else { continue };
                if v.is_finite() && v > 0.0 {
                    out.push((name.to_string(), v));
                }
            }
        }
    }
    if out.is_empty() {
        return Err(format!("{path}: не нашлось ни одного замера"));
    }
    Ok(Baseline {
        format: format.ok_or_else(|| format!("{path}: нет _format"))?,
        docs: docs.ok_or_else(|| format!("{path}: нет _docs"))?,
        per_segment: per_segment.ok_or_else(|| format!("{path}: нет _per_segment"))?,
        segments: segments.ok_or_else(|| format!("{path}: нет _segments"))?,
        threads: threads.ok_or_else(|| format!("{path}: нет _threads"))?,
        os: os.ok_or_else(|| format!("{path}: нет _os"))?,
        arch: arch.ok_or_else(|| format!("{path}: нет _arch"))?,
        measurements: out,
    })
}

fn main() {
    let o = parse_args();
    let dir = std::env::temp_dir()
        .join(format!("svindb-bench-{}-{}", o.docs, std::process::id()))
        .to_string_lossy()
        .to_string();

    if !o.json {
        eprintln!("корпус: {} документов, готовится в {dir}", o.docs);
    }
    let t = Instant::now();
    build(&dir, o.docs, o.per_segment);
    let build_s = t.elapsed().as_secs_f64();

    let segs = Segments::load(&dir).expect("индекс");
    let docs = segs.docs() as u64;
    if !o.json {
        eprintln!(
            "готов за {build_s:.1} с: сегментов {}, документов {docs}, потоков {}",
            segs.list.len(),
            o.threads
        );
        eprintln!();
    }

    let all_cases = cases(docs);
    let chosen: Vec<&Case> = all_cases
        .iter()
        .filter(|c| o.filter.as_deref().is_none_or(|f| c.name.contains(f)))
        .collect();
    if chosen.is_empty() {
        eprintln!("под --only ничего не подошло");
        std::process::exit(2);
    }

    let mut got: Vec<Measured> = Vec::with_capacity(chosen.len());
    for c in &chosen {
        check(c, &segs, o.threads);
        got.push(measure(c, &segs, o.threads));
    }

    if o.json {
        println!("{{");
        println!("  \"_format\": 1,");
        println!("  \"_docs\": {docs},");
        println!("  \"_per_segment\": {},", o.per_segment);
        println!("  \"_segments\": {},", segs.list.len());
        println!("  \"_threads\": {},", o.threads);
        println!("  \"_os\": \"{}\",", std::env::consts::OS);
        println!("  \"_arch\": \"{}\",", std::env::consts::ARCH);
        for (i, m) in got.iter().enumerate() {
            let comma = if i + 1 == got.len() { "" } else { "," };
            println!("  \"{}\": {:.4}{comma}", m.name, m.best_ms);
        }
        println!("}}");
    } else {
        println!("{:<30} {:>10} {:>10} {:>12}", "замер", "лучшее мс", "медиана", "нс/документ");
        println!("{}", "-".repeat(66));
        for m in &got {
            let per = m.best_ms * 1e6 / docs.max(1) as f64;
            println!("{:<30} {:>10.3} {:>10.3} {:>12.1}", m.name, m.best_ms, m.median_ms, per);
        }
        println!("{}", "-".repeat(66));
        println!(
            "лучшее из {RUNS} после {WARM} прогревов; {docs} документов, {} сегм., {} пот.",
            segs.list.len(),
            o.threads
        );
    }

    let mut failed = false;
    if let Some(path) = o.baseline.as_deref() {
        match read_baseline(path) {
            Err(e) => {
                eprintln!("основание не прочиталось: {e}");
                failed = true;
            }
            Ok(base) => {
                let conditions = (
                    1u64,
                    docs,
                    u64::from(o.per_segment),
                    segs.list.len() as u64,
                    o.threads as u64,
                    std::env::consts::OS,
                    std::env::consts::ARCH,
                );
                let expected = (
                    base.format,
                    base.docs,
                    base.per_segment,
                    base.segments,
                    base.threads,
                    base.os.as_str(),
                    base.arch.as_str(),
                );
                if conditions != expected {
                    eprintln!(
                        "основание снято в других условиях: было format/docs/per_segment/segments/threads/os/arch={expected:?}, сейчас {conditions:?}"
                    );
                    failed = true;
                } else {
                    eprintln!();
                    eprintln!("сравнение с {path}, допуск {:.1}%", o.max_regress);
                    for m in &got {
                        let Some((_, was)) = base.measurements.iter().find(|(n, _)| n == m.name)
                        else {
                            eprintln!("  {:<30} новый замер, сравнивать не с чем", m.name);
                            continue;
                        };
                        let delta = (m.best_ms - was) / was * 100.0;
                        let judged = *was >= GATE_FLOOR_MS && m.best_ms >= GATE_FLOOR_MS;
                        let mark = if !judged {
                            "ниже порога шума"
                        } else if delta > o.max_regress {
                            failed = true;
                            "ХУЖЕ"
                        } else if delta < -o.max_regress {
                            "лучше"
                        } else {
                            ""
                        };
                        eprintln!(
                            "  {:<30} было {:>8.3}  стало {:>8.3}  {:+6.1}%  {mark}",
                            m.name, was, m.best_ms, delta
                        );
                    }
                }
            }
        }
    }

    if o.keep {
        eprintln!("корпус оставлен: {dir}");
    } else {
        drop(segs);
        std::fs::remove_dir_all(&dir).ok();
    }
    if failed {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, body: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "svindb-bench-{name}-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, body).expect("baseline");
        path.to_string_lossy().to_string()
    }

    #[test]
    fn baseline_keeps_the_conditions_that_make_times_comparable() {
        let path = file(
            "conditions",
            r#"{
              "_format": 1,
              "_docs": 200000,
              "_per_segment": 4294967295,
              "_segments": 1,
              "_threads": 8,
              "_os": "linux",
              "_arch": "x86_64",
              "agg/count": 1.25
            }"#,
        );
        let got = read_baseline(&path).expect("baseline разобрался");
        assert_eq!(got.format, 1);
        assert_eq!(
            (got.docs, got.per_segment, got.segments, got.threads),
            (200_000, u64::from(u32::MAX), 1, 8)
        );
        assert_eq!((got.os.as_str(), got.arch.as_str()), ("linux", "x86_64"));
        assert_eq!(got.measurements, vec![("agg/count".to_string(), 1.25)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn a_legacy_or_non_finite_baseline_is_not_silently_compared() {
        let legacy = file("legacy", r#"{"agg/count": 1.25}"#);
        assert!(read_baseline(&legacy).is_err(), "baseline без условий принят");
        std::fs::remove_file(legacy).ok();

        let broken = file(
            "nan",
            r#"{
              "_format": 1,
              "_docs": 1,
              "_per_segment": 4294967295,
              "_segments": 1,
              "_threads": 1,
              "_os": "linux",
              "_arch": "x86_64",
              "agg/count": NaN
            }"#,
        );
        assert!(read_baseline(&broken).is_err(), "NaN принят за время");
        std::fs::remove_file(broken).ok();
    }
}
