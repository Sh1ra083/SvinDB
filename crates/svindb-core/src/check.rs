//! Проверка целостности индекса: только чтение, без починки.

use crate::index::{BLOCK_META, TERM_META};
use crate::mmap::Mapped;

/// Насколько глубоко смотреть.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Depth {
    /// Заголовки, размеры файлов, состав индекса.
    Fast,
    /// Плюс проход по потокам: порядок, границы, сходимость счётчиков.
    Full,
}

/// Насколько всё плохо.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    /// Заметка: не поломка, но человеку стоит знать.
    Note,
    /// Поломка: поиск по такому индексу отдаёт неверное, а не отказывает.
    Broken,
}

impl Level {
    pub fn name(self) -> &'static str {
        match self {
            Level::Note => "заметка",
            Level::Broken => "поломка",
        }
    }
}

/// Одна находка.
pub struct Finding {
    pub level: Level,
    /// Где: `seg0003/terms.bin` или `индекс`.
    pub scope: String,
    /// Имя проверки — по нему находка ищется в коде.
    pub check: &'static str,
    /// Что именно не сошлось, с числами.
    pub text: String,
    /// Что это значит для выдачи. Не «файл битый», а последствие.
    pub means: &'static str,
}

/// Итог проверки.
pub struct Report {
    pub depth: Depth,
    pub dir: String,
    pub segments: usize,
    pub docs: usize,
    pub findings: Vec<Finding>,
}

impl Report {
    pub fn broken(&self) -> usize {
        self.findings.iter().filter(|f| f.level == Level::Broken).count()
    }

    pub fn notes(&self) -> usize {
        self.findings.iter().filter(|f| f.level == Level::Note).count()
    }

    /// Можно ли пускать поиск.
    pub fn ok(&self) -> bool {
        self.broken() == 0
    }
}

/// Собирает находки, не роняя проверку на первой.
struct Sink {
    scope: String,
    out: Vec<Finding>,
}

impl Sink {
    fn new() -> Sink {
        Sink { scope: "индекс".into(), out: Vec::new() }
    }

    fn at(&mut self, scope: &str) {
        self.scope = scope.to_string();
    }

    fn broken(&mut self, check: &'static str, means: &'static str, text: String) {
        self.out.push(Finding {
            level: Level::Broken,
            scope: self.scope.clone(),
            check,
            text,
            means,
        });
    }

    fn note(&mut self, check: &'static str, means: &'static str, text: String) {
        self.out.push(Finding {
            level: Level::Note,
            scope: self.scope.clone(),
            check,
            text,
            means,
        });
    }
}

/// Размер файла. `None` — файла нет.
fn size_of(path: &str) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

/// Проверяет индекс. Ничего не пишет и ничего не чинит.
pub fn check(dir: &str, depth: Depth) -> Report {
    let mut s = Sink::new();
    // «Списка нет» и «список пуст» — разные вещи, и путать их нельзя.
    //
    // Пустой список законен: так выглядит индекс, у которого уборка забрала все
    // сегменты, потому что все их документы погашены. Обход каталога в этот миг
    // подберёт снятые с публикации каталоги, ждущие удаления, и объявит поломкой
    // то, что поломкой не является, — их и не открывает никто.
    let listing = crate::manifest::listing(dir);
    let listed = listing.clone().unwrap_or_default();
    let scanned = crate::manifest::scan(dir);

    // Состав индекса. Список — источник истины: порядок в нём задаёт нумерацию
    // документов, и каталог, которого в нём нет, поиску не виден.
    let names: Vec<String> = if listing.as_ref().is_some_and(|l| l.is_empty()) {
        if scanned.is_empty() && !std::path::Path::new(&format!("{dir}/head.tsv")).exists() {
            s.note(
                "пустой индекс",
                "искать не по чему, но это не поломка: так выглядит каталог до первого сброса",
                "список сегментов пуст".into(),
            );
        } else {
            s.note(
                "список сегментов пуст",
                "все сегменты забрала уборка: их документы погашены. Каталоги, ещё лежащие \
                 рядом, сняты с публикации и будут удалены",
                format!("{} каталог(ов) ждут удаления", scanned.len()),
            );
        }
        return Report { depth, dir: dir.into(), segments: 0, docs: 0, findings: s.out };
    } else if listed.is_empty() {
        if scanned.is_empty() && !std::path::Path::new(&format!("{dir}/head.tsv")).exists() {
            s.note(
                "пустой индекс",
                "искать не по чему, но это не поломка: так выглядит каталог до первого сброса",
                "ни списка сегментов, ни заголовка".into(),
            );
            return Report { depth, dir: dir.into(), segments: 0, docs: 0, findings: s.out };
        }
        if scanned.is_empty() {
            vec![dir.to_string()]
        } else {
            s.note(
                "нет списка сегментов",
                "порядок сегментов выведен из имён, а не записан: после слияния он может разойтись",
                format!("{} найдено обходом каталога", scanned.len()),
            );
            scanned.clone()
        }
    } else {
        listed.clone()
    };

    orphans(dir, &names, &mut s);

    let mut docs_total: u64 = 0;
    let mut sizes: Vec<usize> = Vec::new();
    let mut schema = Schema::new();
    let mut schema_sink = Sink::new();
    schema_sink.at("индекс");
    if depth == Depth::Full {
        eprintln!("check --full: проверка сегментов в {dir}");
    }
    for path in names.iter() {
        let name = crate::manifest::name_of(path);
        s.at(&name);
        if !std::path::Path::new(path).exists() {
            s.broken(
                "нет сегмента",
                "номера оставшихся сдвинутся: выданные раньше курсоры страниц укажут на другие документы",
                "назван в списке, а на диске его нет".into(),
            );
            continue;
        }
        let head = match crate::index::read_head(path) {
            Ok(h) => h,
            Err(e) => {
                s.broken(
                    "заголовок",
                    "сегмент не откроется вовсе: без заголовка неизвестно ни число документов, ни поля",
                    format!("{e}"),
                );
                continue;
            }
        };
        if !(crate::index::LAYOUT_MIN..=crate::index::LAYOUT).contains(&head.layout) {
            s.broken(
                "раскладка",
                "читать нечем: раскладка менялась несовместимо",
                format!(
                    "раскладка {}, читаются {}–{}",
                    head.layout,
                    crate::index::LAYOUT_MIN,
                    crate::index::LAYOUT
                ),
            );
            continue;
        }
        docs_total += head.docs as u64;
        segment(path, &head, depth, &mut s);
        schema.observe(&name, &head, &mut schema_sink);
        sizes.push(head.docs);
        if depth == Depth::Full && sizes.len().is_multiple_of(100) {
            eprintln!("check --full: проверено {} сегментов", sizes.len());
        }
    }

    s.at("индекс");
    s.out.extend(schema_sink.out);
    numbering(&sizes, &mut s);
    if depth == Depth::Fast && !sizes.is_empty() {
        // Быстрая проверка обязана сказать, чего она не смотрела.
        //
        // Иначе она врёт тем убедительнее, чем спокойнее выглядит: «всё цело»
        // после проверки, которая не открывала ни одного потока, читается как
        // «индекс исправен», а означает «размеры сошлись». Подмена содержимого
        // документа, разъехавшийся порядок списков и указатель позиций за
        // концом файла ей не видны по построению.
        s.note(
            "глубина",
            "быстрая проверка сверяет размеры и состав; порядок внутри потоков и \
             покрытие хранилища ей не видны — для них нужна --full",
            "потоки не открывались".into(),
        );
    }

    Report {
        depth,
        dir: dir.into(),
        segments: sizes.len(),
        docs: docs_total as usize,
        findings: s.out,
    }
}

/// Каталоги сегментов вне списка.
///
/// Это остаток прерванного слияния: поиск их не видит, место они занимают.
/// Проверка их **называет**, но не трогает — уборка это запись, а здесь только
/// чтение. Убирает их `serve` под замком писателя на старте.
fn orphans(dir: &str, names: &[String], s: &mut Sink) {
    let live: std::collections::HashSet<String> =
        names.iter().map(|p| crate::manifest::name_of(p)).collect();
    let Ok(it) = std::fs::read_dir(dir) else { return };
    let mut lost: Vec<(String, u64)> = Vec::new();
    for e in it.filter_map(|e| e.ok()) {
        let p = e.path();
        let n = p.file_name().map(|x| x.to_string_lossy().to_string()).unwrap_or_default();
        if !p.is_dir() || live.contains(&n) {
            continue;
        }
        // Приставки берутся из одного места с уборкой. Держать здесь свой
        // список — верный способ разойтись с ней: так и вышло с `cln`, который
        // уборка не знала, а значит его каталог не убирал никто и никогда.
        if !crate::manifest::looks_like_segment(&n) {
            continue;
        }
        let bytes = std::fs::read_dir(&p)
            .map(|f| {
                f.filter_map(|x| x.ok())
                    .filter_map(|x| x.metadata().ok())
                    .map(|m| m.len())
                    .sum::<u64>()
            })
            .unwrap_or(0);
        lost.push((n, bytes));
    }
    lost.sort();
    for (n, bytes) in lost {
        s.note(
            "потерянный сегмент",
            "место занято, поиску не видно: остаток прерванного слияния",
            format!("{n} вне списка, {bytes} байт"),
        );
    }
}

/// Поля сегментов обязаны сходиться **по устойчивому номеру**.
///
/// Сверялась позиция, и это ловило подмену — но заодно объявляло поломкой
/// любое изменение схемы: у сегмента, собранного до объявления поля, состав
/// короче, и позиции у общих полей разъезжаются. Такой индекс исправен, и
/// звать его сломанным значит гнать хозяина чинить то, что не ломалось.
///
/// По номеру проверка держит ровно то, ради чего заведена: два разных поля под
/// одним номером — это подмена, а поле, которого в старом сегменте нет, —
/// просто его возраст.
struct SchemaDefinition {
    first_name: String,
    name: String,
    kind: &'static str,
    stored: bool,
    positioned: bool,
    searched: bool,
}

struct Schema {
    // В памяти остаётся только первое объявление каждого устойчивого номера,
    // а не весь заголовок каждого сегмента.
    first: std::collections::HashMap<u32, SchemaDefinition>,
}

impl Schema {
    fn new() -> Schema {
        Schema { first: std::collections::HashMap::new() }
    }

    fn observe(&mut self, segment_name: &str, head: &crate::index::Head, s: &mut Sink) {
        for f in head.fields.iter() {
            self.observe_definition(
                segment_name,
                f.fid,
                (&f.name, f.kind.name(), f.stored, f.positioned, f.searched),
                s,
            );
        }
    }

    fn observe_definition(
        &mut self,
        segment_name: &str,
        fid: u32,
        shape: (&str, &'static str, bool, bool, bool),
        s: &mut Sink,
    ) {
        let (name, kind, stored, positioned, searched) = shape;
        let shape = (name, kind, stored, positioned, searched);
        let Some(first) = self.first.get(&fid) else {
            self.first.insert(
                fid,
                SchemaDefinition {
                    first_name: segment_name.to_string(),
                    name: name.to_string(),
                    kind,
                    stored,
                    positioned,
                    searched,
                },
            );
            return;
        };
        if (first.name.as_str(), first.kind, first.stored, first.positioned, first.searched)
            != shape
        {
            s.broken(
                "схема",
                "поле вернётся клиенту под чужим именем: номер один, а объявления разные",
                format!(
                    "{segment_name}: поле {fid} — {name}:{kind}{}{}{} против {}: {}:{}{}{}{}",
                    if stored { "" } else { ":nostore" },
                    if searched { "" } else { ":noindex" },
                    if positioned { "" } else { ":nopos" },
                    first.first_name,
                    first.name,
                    first.kind,
                    if first.stored { "" } else { ":nostore" },
                    if first.searched { "" } else { ":noindex" },
                    if first.positioned { "" } else { ":nopos" },
                ),
            );
        }
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn schema_allows_evolution_and_keeps_first_definition() {
        let mut schema = Schema::new();
        let mut sink = Sink::new();
        schema.observe_definition("seg0", 7, ("title", "text", true, true, true), &mut sink);
        schema.observe_definition("seg1", 8, ("year", "int32", false, false, true), &mut sink);
        schema.observe_definition("seg1", 7, ("title", "text", true, true, true), &mut sink);
        assert!(sink.out.is_empty());
        assert_eq!(schema.first.len(), 2);
        let first = schema.first.get(&8).expect("первая декларация");
        assert_eq!(first.first_name, "seg1");
        assert_eq!(first.name, "year");
        assert_eq!(first.kind, "int32");
        assert!(!first.stored);
        assert!(!first.positioned);
        assert!(first.searched);
    }

    #[test]
    fn schema_conflict_does_not_replace_first_definition() {
        let mut schema = Schema::new();
        let mut sink = Sink::new();
        schema.observe_definition("seg0", 3, ("title", "text", true, true, true), &mut sink);
        let variants = [
            ("renamed", "text", true, true, true),
            ("title", "keyword", true, true, true),
            ("title", "text", false, true, true),
            ("title", "text", true, false, true),
            ("title", "text", true, true, false),
        ];
        for (name, kind, stored, positioned, searched) in variants {
            schema.observe_definition(
                "seg1",
                3,
                (name, kind, stored, positioned, searched),
                &mut sink,
            );
        }
        assert_eq!(sink.out.len(), variants.len());
        assert!(sink.out.iter().all(|f| f.text.contains("seg0: title:text")));
        assert_eq!(schema.first.get(&3).expect("первая декларация").name, "title");
    }

    #[test]
    fn schema_retention_is_bounded_by_unique_field_ids() {
        let mut schema = Schema::new();
        let mut sink = Sink::new();
        for _segment in 0..1_000 {
            for fid in 0..514 {
                schema.observe_definition(
                    "seg0",
                    fid,
                    ("field", "text", true, true, true),
                    &mut sink,
                );
            }
        }
        assert!(sink.out.is_empty());
        assert_eq!(schema.first.len(), 514);
    }

    #[test]
    fn numbering_preserves_boundary_error() {
        let mut sink = Sink::new();
        numbering(&[crate::segments::MAX_DOCS as usize, 1], &mut sink);
        assert_eq!(sink.out.len(), 1);
        assert_eq!(sink.out[0].check, "нумерация");
    }
}

/// Общее пространство номеров.
fn numbering(sizes: &[usize], s: &mut Sink) {
    if let Err(e) = crate::segments::bases(sizes) {
        s.broken(
            "нумерация",
            "хвост поздних сегментов получит номера ранних, и выдача вернёт чужое содержимое",
            e,
        );
    }
}

/// Проверки одного сегмента.
fn segment(dir: &str, head: &crate::index::Head, depth: Depth, s: &mut Sink) {
    let docs = head.docs;
    let name = crate::manifest::name_of(dir);
    // Источник заводится один на сегмент и раздаётся проверкам. Без него каждая
    // из них искала бы куски файлами — и на упакованном сегменте объявила бы
    // пропавшим всё, что лежит внутри контейнера.
    //
    // Здесь же проверяется и само оглавление, и намеренно не своим разбором:
    // [`crate::container::Table`] сверяет опознание, согласие заголовка с
    // концевиком, сумму оглавления, порядок имён, выход смещений за куски и
    // нахлёсты — то есть ровно то, чем сегмент открывается в бою. Заведи
    // проверка вторую, свою правду о формате — и разойтись они могли бы в любую
    // сторону: либо `check` объявляет поломкой то, что читается, либо молчит о
    // том, на чём читатель споткнётся. Сумм самих кусков в этом разборе нет
    // (это чтение всего сегмента) — их считает [`pieces`] при полной проверке.
    let src = match crate::container::SegmentSource::of(dir, head.container) {
        Ok(src) => std::sync::Arc::new(src),
        Err(e) => {
            s.at(&name);
            s.broken(
                "контейнер не открывается",
                "сегмент не откроется вовсе: все его куски лежат внутри одного файла",
                format!("{e}"),
            );
            return;
        }
    };

    // Ноль в заголовке — поломка **только когда он врёт**, то есть когда
    // соответствующий кусок не пуст.
    //
    // Прежде ноль объявлялся поломкой сам по себе, и это давало ложную
    // тревогу на здоровых каталогах. Метаданные блоков пишутся только спискам
    // длиннее одного блока, а длина списка не больше числа документов: у
    // сегмента из трёх документов `blocks.bin` пуст по построению. Так же и
    // `terms.bin` — у сегмента, где текстовые поля пусты у всех документов.
    // `check --full` сообщал поломку на только что заведённом индексе, а
    // поломка, которая бывает ложной, перестаёт что-либо значить.
    //
    // Согласованность «ноль в заголовке ↔ пустой кусок» — это и есть то, что
    // сторож хотел ловить: заголовок с неразобранным числом при непустом куске
    // отключил бы сверку размеров бесшумно.
    //
    // Длина спрашивается у источника, и место сторожа здесь, а не в обходе
    // списка сегментов, ровно по этой причине. Раньше он мерил `blocks.bin`
    // файлом в каталоге — а у упакованного сегмента такого файла нет вовсе,
    // metadata отвечала «нет», размер вырождался в ноль, и условие «ноль в
    // заголовке при непустом куске» не выполнялось никогда. То есть сторож
    // молчал ровно на том виде сегмента, какой выдаёт приём.
    s.at(&name);
    let piece_len = |f: &str| src.len_of(f).unwrap_or(0);
    let zero: Vec<&str> = [
        ("docs", head.docs as u64, 1u64),
        ("blocks", head.blocks as u64, piece_len("blocks.bin")),
        ("terms", head.terms as u64, piece_len("terms.bin")),
    ]
    .iter()
    .filter(|(_, v, bytes)| *v == 0 && *bytes > 0)
    .map(|(n, _, _)| *n)
    .collect();
    if !zero.is_empty() {
        s.broken(
            "число в заголовке",
            "сверка размеров отключается бесшумно: ожидаемым становится ноль, и подходит любой файл",
            format!("нулевые или неразобранные: {}", zero.join(", ")),
        );
    }

    // Точные размеры. Каждое из этих чисел заголовок несёт сам, и до появления
    // этой проверки их не сверял никто: чтение за границей отображения отдаёт
    // ноль, и короткий файл не отказывает, а меняет смысл.
    let mut exact: Vec<(&str, u64, &'static str)> = vec![
        (
            "norms.bin",
            (head.fields.iter().map(|f| f.norms_band as usize + 1).max().unwrap_or(0) * docs)
                as u64,
            "документ прочтётся как нулевой длины, а это наибольший множитель BM25: всплывёт первым",
        ),
        (
            "blocks.bin",
            (head.blocks * BLOCK_META) as u64,
            "длина блока прочтётся нулём, курсор не сдвинется и список кончится раньше времени",
        ),
    ];
    // У седьмой раскладки метаданные слова переменной длины, и выводимого
    // размера у `terms.bin` нет. Зато он есть у опор — по одной на каждые
    // [`crate::layout::ANCHOR`] слов, — а сам поток проверяется проходом.
    if head.layout >= 7 {
        exact.push((
            "terms.idx",
            (crate::layout::Terms::groups(head.terms) * crate::layout::RESTART) as u64,
            "группа слов начнётся не там: разбор поедет и список прочтёт чужие номера",
        ));
    } else {
        exact.push((
            "terms.bin",
            (head.terms * TERM_META) as u64,
            "у слова прочтётся df = 0, то есть «здесь такого нет»: живой документ перестанет находиться",
        ));
    }
    for (file, want, means) in exact {
        s.at(&format!("{name}/{file}"));
        match src.len_of(file) {
            None => s.broken("нет файла", means, "файла нет".into()),
            Some(got) if got < want => {
                s.broken("размер", means, format!("{got} байт вместо {want}"))
            }
            Some(got) if got > want => s.note(
                "размер",
                "лишние байты в конце: читаются они не будут, но откуда взялись — вопрос",
                format!("{got} байт при ожидаемых {want}"),
            ),
            Some(_) => {}
        }
    }

    // Карта живых: битовая карта плюс слово признака за ней.
    s.at(&format!("{name}/{}", crate::live::FILE));
    // Ожидается карта **с печатью**: её пишут и приём, и слияние. Карта без
    // печати короче на три слова, и её длина разбирается отдельно — как старый
    // образец, а не как обрезанная.
    let plain_live = (docs.div_ceil(64) + 1) as u64 * 8;
    let want_live = plain_live + crate::live::SEAL_WORDS as u64 * 8;
    live_seal(dir, docs, &name, s);
    match size_of(&format!("{dir}/{}", crate::live::FILE)) {
        None => s.broken(
            "нет файла",
            "погашенные документы воскреснут: отсутствие карты читается как «живы все»",
            "файла нет".into(),
        ),
        Some(got) if got == plain_live => {
            // Старый образец: печати нет, но карта целая. Об этом уже сказано
            // заметкой в `live_seal`, второй раз повторять незачем.
            // Признак проверяется и без печати: нулевой flag при погашенных
            // битах иначе делает read-only пересборку источником resurrection.
            live_flag(dir, docs, s);
        }
        Some(got) if got < want_live => s.broken(
            "размер",
            "погашенные документы воскреснут: биты, не попавшие в файл, читаются как живые",
            format!("{got} байт вместо {want_live}"),
        ),
        Some(_) => live_flag(dir, docs, s),
    }

    // Обязательные, но без выводимого размера.
    for file in ["postings.bin", "terms.bin", "dict.blob", "dict.idx", "ids.bin", "ids.idx"] {
        s.at(&format!("{name}/{file}"));
        if src.len_of(file).is_none() {
            s.broken("нет куска", "сегмент не откроется", "куска нет".into());
        }
    }

    let packed = src.len_of(crate::ids::PACKED);
    let plain = src.len_of("idmap.docs");
    if let Some((nm, got, want)) = match (packed, plain) {
        (Some(g), _) => {
            let w = src.try_open(crate::ids::PACKED).map(|m| m.u8_at(0)).unwrap_or(0) as u32;
            Some((crate::ids::PACKED, g, crate::bits::window_len(docs, w.max(1)) as u64 + 1))
        }
        (None, Some(g)) => Some(("idmap.docs", g, (docs * 4) as u64)),
        (None, None) => None,
    } {
        s.at(&format!("{name}/{nm}"));
        if got < want {
            s.broken(
                "размер",
                "гашение прочтёт номер документа нулём и убьёт документ 0 вместо искомого",
                format!("{got} байт вместо {want}"),
            );
        }
    }

    // Хранилище документов: засечка ровно 16 байт, и кратность — первое, что
    // ломается у обрезанного файла.
    for f in head.fields.iter().filter(|f| f.stored) {
        s.at(&format!("{name}/{}.smrk", f.stem));
        let blocks = match src.len_of(&format!("{}.smrk", f.stem)) {
            None => continue,
            Some(got) if got % crate::store::MARK as u64 != 0 => {
                s.broken(
                    "кратность засечек",
                    "двоичный поиск куска уедет: последняя засечка прочтётся наполовину",
                    format!("{got} байт не кратно {}", crate::store::MARK),
                );
                continue;
            }
            Some(got) => got / crate::store::MARK as u64,
        };
        // Сумм распакованного может не быть вовсе, но если они есть — их обязано
        // быть ровно по одной на кусок. Короткий файл читался бы нулями, то есть
        // объявлял бы сошедшимся то, чего не сверял.
        let Some(got) = src.len_of(&format!("{}.scrc", f.stem)) else { continue };
        s.at(&format!("{name}/{}.scrc", f.stem));
        if got != blocks * 4 {
            s.broken(
                "число сумм не то",
                "проверка распаковки сверит не тот кусок или не сверит вовсе: суммы \
                 сопоставляются с кусками по порядку",
                format!("{got} байт при {blocks} кусках, то есть вместо {}", blocks * 4),
            );
        }
    }

    inventory(&src, head, s);
    dictionaries(&src, head, s);
    ceilings(&src, head, s);

    if depth == Depth::Full {
        streams(&src, head, s);
        pieces(&src, s);
        checksums(dir, s);
    }
}

/// Суммы кусков контейнера: каждый кусок против своей записи в оглавлении.
///
/// ## Зачем это отдельно от сводки
///
/// Сводка сумм называет куски поимённо (см. [`crate::sums::compute`]), и на
/// первый взгляд второй такой проверки не нужно. Нужно, и разница не в
/// подробности отчёта, а в том, **чьё это обещание**.
///
/// Сводка — запись внешняя и пересчитываемая: её пишут заново приём, слияние и
/// разбор контейнера, каждый раз по тому, что лежит на диске сейчас. Испорти
/// байт внутри `seg.dat` и вызови пересчёт — сводка сойдётся сама с собой и
/// подтвердит порчу как законное содержимое. Сумма в оглавлении посчитана
/// **писателем в тот момент, когда кусок ложился в файл**, и пересчитать её,
/// не переписав весь контейнер, нечем. Она отвечает не на вопрос «менялся ли
/// файл после сборки», а на вопрос «тот ли это кусок, какой записали».
///
/// Вторая причина проще: там, где сводки нет или она обнулена, эта проверка
/// остаётся единственной, что вообще смотрит внутрь кусков. О самой пропаже
/// сводки говорит отдельная находка, но говорить и сверять — разные дела.
///
/// Само оглавление здесь уже разобрано: смещения, нахлёсты, согласие заголовка
/// с концевиком проверены при открытии источника. Сумм кусков в том разборе нет
/// намеренно — это чтение всего сегмента, и место ему ровно здесь, в полной
/// проверке.
fn pieces(src: &std::sync::Arc<crate::container::SegmentSource>, s: &mut Sink) {
    let Some(table) = src.table() else { return };
    let name = crate::manifest::name_of(src.dir());
    for m in table.members() {
        s.at(&format!("{name}/{}", m.name));
        // Кусок назван оглавлением, а источник его не отдаёт — такого быть не
        // может, и потому молчать тут нельзя: значит разошлись оглавление и то,
        // чем куски достают читатели.
        let Some(body) = src.try_open(&m.name) else {
            s.broken(
                "куска нет вопреки оглавлению",
                "читатель возьмёт пустоту вместо содержимого: оглавление обещает кусок, \
                 которого не отдают",
                format!("{:?} назван в оглавлении, а не открывается", m.name),
            );
            continue;
        };
        if body.len_bytes() as u64 != m.len {
            s.broken(
                "длина куска не та",
                "чтение уйдёт за кусок или оборвётся раньше времени: длина в оглавлении \
                 и длина области разошлись",
                format!("{} байт вместо {}", body.len_bytes(), m.len),
            );
            continue;
        }
        let got = crate::wal::crc32(body.slice(0, body.len_bytes()));
        if got != m.crc {
            s.broken(
                "содержимое куска не то",
                "длина сошлась, а байты другие: номер документа, позиция или потолок \
                 читаются чужими",
                format!("сумма {got:08x} вместо {:08x}", m.crc),
            );
        }
    }
}

/// Насколько файл близок к потолку тридцатидвухбитного смещения.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reach {
    /// Запас есть.
    Fits,
    /// Запас есть, но кончается.
    Near,
    /// Смещение в такой файл четырьмя байтами уже не выражается.
    Over,
}

/// С какого размера пора предупреждать: три и три четверти гигабайта.
///
/// Число выбрано так, чтобы заметка появлялась **до** того, как очередное
/// укрупнение удвоит сегмент: удвоение с этой отметки перескакивает потолок, а
/// уменьшать `target_docs` придётся до укрупнения, не после.
pub const NEAR_CEILING: u64 = 15 << 28;

/// Куда попал файл такого размера.
///
/// Отдельно от отчёта, потому что края здесь несущие и проверяются точными
/// значениями: `u32::MAX` — ещё влезает, `u32::MAX + 1` — уже нет. Сдвиг границы
/// на единицу либо объявил бы поломкой сегмент, который открывается, либо
/// пропустил бы тот, который врёт.
pub fn reach(bytes: u64) -> Reach {
    if bytes > crate::dict::OFFSET_CEILING {
        Reach::Over
    } else if bytes >= NEAR_CEILING {
        Reach::Near
    } else {
        Reach::Fits
    }
}

/// Смещения на пороге четырёх гигабайт.
///
/// ## Что здесь ловится
///
/// Внутри сегмента **все** смещения тридцатидвухбитные: засечка хранилища в
/// `<поле>.smrk`, опора словаря в `dict.idx`, опора идентификаторов в `ids.idx`,
/// опора слов в `terms.idx`, указатель полосы в `g*.aidx`. Пока файл, на который
/// они указывают, меньше четырёх гигабайт, вопроса нет. Как только он их
/// перевалил, смещение заворачивается — и указывает не за конец файла, а в его
/// начало, то есть на **чужую запись того же файла**. Ни размер, ни кратность,
/// ни контрольная сумма этого не видят: файл цел, просто читается не тот кусок.
/// Из пяти такой заворот ловил только `terms.bin` — и то на следующем открытии,
/// когда исходные сегменты уже снесены уборкой.
///
/// Писатели с этого раздела отказываются раньше, чем усечённое смещение ляжет в
/// файл (см. `store::fits_mark`, `dict::fits_anchor`, `ids::fits_anchor`,
/// `slots::fits_band`, `merge::fits_offset`). Проверка нужна тому, что уже лежит
/// на диске: собранное прежними сборками, перенесённое с чужой машины,
/// восстановленное из архива.
///
/// Размером меряется не всё: у полосы арности рядом со смещением лежит ординал,
/// и он упирается в потолок тогда, когда файл ещё мелкий. Это считается
/// отдельно — см. [`slot_count`].
///
/// ## Почему поломка, а не заметка
///
/// Законный писатель может перевалить за потолок ровно на хвост последней
/// записи: потолок сверяется перед записью смещения, а тело пишется после.
/// Такой файл ещё читается целиком, и поломка на нём формально строга. Выбрано
/// всё равно «поломка»: цена пропущенного заворота — молча чужой документ в
/// выдаче, цена лишней тревоги — сегмент, который и без того упёрся в потолок и
/// подлежит уменьшению. Ошибаться в эту сторону дешевле.
fn ceilings(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);
    // Пары «файл со смещениями внутрь — файл, который эти смещения держит».
    let mut pairs: Vec<(String, String)> = vec![
        ("postings.bin".into(), "terms.idx".into()),
        ("pos.bin".into(), "pos.idx".into()),
        ("terms.bin".into(), "terms.idx".into()),
        ("dict.blob".into(), "dict.idx".into()),
        ("ids.bin".into(), "ids.idx".into()),
        ("idmap.blob".into(), "idmap.idx".into()),
    ];
    // Хранимое содержимое — самый крупный файл сегмента, и в потолок оно
    // упирается первым: на семидесяти трёх байтах документа около девяноста
    // семи миллионов документов.
    for f in head.fields.iter().filter(|f| f.stored) {
        pairs.push((format!("{}.sbin", f.stem), format!("{}.smrk", f.stem)));
    }
    for g in head.groups.iter().filter(|g| g.kind == crate::slots::Kind::Dense) {
        pairs.push((format!("g{}.arity", g.id), format!("g{}.aidx", g.id)));
    }

    // Длина спрашивается у источника, а не у каталога. У упакованного сегмента
    // отдельных файлов нет вовсе, и опрос каталога отвечал бы «такого нет» на
    // каждую из шести пар — то есть вся сверка потолка молча отключалась бы
    // ровно на тех сегментах, которые до потолка и дорастают.
    for (file, idx) in pairs {
        let Some(bytes) = src.len_of(&file) else { continue };
        match reach(bytes) {
            Reach::Fits => {}
            Reach::Near => {
                s.at(&format!("{name}/{file}"));
                s.note(
                    "порог смещения",
                    "запас до заворота почти выбран: сегмент такого размера укрупнять уже нельзя",
                    format!(
                        "{bytes} байт при потолке {} — до заворота смещений в {idx} осталось {} байт",
                        crate::dict::OFFSET_CEILING,
                        crate::dict::OFFSET_CEILING - bytes
                    ),
                );
            }
            Reach::Over => {
                s.at(&format!("{name}/{file}"));
                s.broken(
                    "смещение за потолком",
                    "смещение завернулось на начало файла: чтение отдаёт чужую запись молча, без отказа",
                    format!(
                        "{bytes} байт при потолке {}: смещения в {idx} тридцатидвухбитные, \
                         и {} байт хвоста ими не выражаются",
                        crate::dict::OFFSET_CEILING,
                        bytes - crate::dict::OFFSET_CEILING
                    ),
                );
            }
        }
    }

    slot_count(src, head, s);
}

/// Число слотов группы — потолок, до которого размер файла не доходит.
///
/// ## Почему отдельно от размеров
///
/// В `g*.aidx` на каждый блок лежат **два** тридцатидвухбитных числа: смещение
/// блока внутри `g*.arity` и первый ординал блока. Сверка выше меряет только
/// первое, потому что смещение растёт вместе с файлом. Ординал растёт с числом
/// слотов, а оно с размером не связано вовсе: полоса упакована битами, и при
/// арности до трёх на документ блок из ста двадцати восьми документов занимает
/// тридцать три байта. Четыре миллиарда слотов — это около двух миллиардов
/// документов, шестнадцать миллионов блоков и меньше гигабайта полосы, то есть
/// меньше четверти потолка по размеру. Ординал при этом уже завернулся. Мерить
/// размер файла в этом месте всё равно что не мерить ничего.
///
/// Заворот ординала не отказывает и ничего не портит: `range` отдаёт документу
/// отрезок, начинающийся у чужого документа, и значение вложенного поля
/// приписывается не тому элементу массива — молча, на выдаче.
///
/// ## Откуда берётся число
///
/// Из заголовка: полоса своего счётчика не держит, а пересчёт арностей стоил бы
/// прохода по всему `g*.arity` на каждой проверке. Заголовок здесь не «на
/// веру» — он же и есть то, чем открывается группа: `Slots::open` берёт число
/// слотов оттуда, и врущее число врёт всем читателям одинаково.
///
/// Плотные группы, и только они: у `identity` слотов ровно столько же, сколько
/// документов, а число документов упирается в тот же потолок раньше и своей
/// проверкой.
///
/// ## Почему проверка нужна, если заголовок с таким числом и так не читается
///
/// `read_head` отвергает группу, у которой слотов больше `u32::MAX`: такой
/// сегмент не открывается вовсе. Но отвергает он только **свершившийся**
/// заворот, а до него молчит — группа в четыре миллиарда слотов без одного
/// открывается как ни в чём не бывало, и о том, что укрупнять её уже нельзя,
/// оператор узнаёт из отказа самого укрупнения, когда исходные сегменты выбраны
/// и работа начата. Заметка здесь — единственное, что говорит об этом заранее.
/// Ступень «за потолком» стоит рядом с ней затем, чтобы правило было целым в
/// одном месте: разведи их, и «уже завернулось» держалось бы на одной строке
/// чужого разбора заголовка.
fn slot_count(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);
    for g in head.groups.iter().filter(|g| g.kind == crate::slots::Kind::Dense) {
        let units = g.units as u64;
        match reach(units) {
            Reach::Fits => {}
            Reach::Near => {
                s.at(&format!("{name}/g{}.aidx", g.id));
                s.note(
                    "порог числа слотов",
                    "запас до заворота ординала почти выбран: такую группу укрупнять уже нельзя",
                    format!(
                        "{units} слотов при потолке {} — до заворота первого ординала блока \
                         осталось {}; по размеру полосы этого не видно, она упакована битами",
                        crate::dict::OFFSET_CEILING,
                        crate::dict::OFFSET_CEILING - units
                    ),
                );
            }
            Reach::Over => {
                s.at(&format!("{name}/g{}.aidx", g.id));
                s.broken(
                    "число слотов за потолком",
                    "ординал завернулся: отрезок документа начинается у чужого, \
                     и значения массива приписаны не тем элементам",
                    format!(
                        "{units} слотов при потолке {}: первый ординал блока тридцатидвухбитный, \
                         и {} из них им не выражаются",
                        crate::dict::OFFSET_CEILING,
                        units - crate::dict::OFFSET_CEILING
                    ),
                );
            }
        }
    }
}

/// Словари: число записей обязано сойтись с заголовком.
///
/// Длина этих файлов из заголовка не выводится — фронт-кодирование даёт разный
/// размер на разных словах. Зато выводится **число записей**, и оно считается
/// проходом по последней опоре. Обрезанный словарь даёт меньше записей, и это
/// видно, не читая его целиком.
///
/// Что теряется при расхождении: у словаря терминов — слова, которых больше не
/// найти; у словаря идентификаторов — документы, которые больше не погасить; у
/// прямых идентификаторов — пустой ответ там, где документ есть.
fn dictionaries(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);

    s.at(&format!("{name}/dict.blob"));
    let dict = crate::dict::Dict::open_named(src, "dict", head.terms, head.layout);
    if dict.len() != head.terms {
        s.broken(
            "число слов",
            "часть слов перестанет находиться: их записей в словаре больше нет",
            format!("{} записей при {} в заголовке", dict.len(), head.terms),
        );
    }

    // Прямые идентификаторы своего счётчика не имеют — `Ids::len` отдаёт то,
    // что ему передали. Зато опоры считаются точно: по одной на каждые 64
    // документа, четыре байта на опору.
    s.at(&format!("{name}/ids.idx"));
    let want = head.docs.div_ceil(crate::ids::ANCHOR) as u64 * 4;
    match src.len_of("ids.idx") {
        None => {}
        Some(got) if got != want => s.broken(
            "число опор",
            "переход к идентификатору уедет: опора номер N перестанет указывать на документ 64·N",
            format!("{got} байт вместо {want}"),
        ),
        Some(_) => {}
    }

    // Сам поток идентификаторов сверяется проходом, и без него никак: обход по
    // нему за концом файла не отказывает, а отдаёт **пустые строки**. Документ
    // получает пустой идентификатор, укрупнение переносит это в слитый сегмент,
    // и дальше один запрос на удаление гасит их все разом — потому что пустой
    // идентификатор совпадает сам с собой.
    s.at(&format!("{name}/ids.bin"));
    if let Some(blob) = src.try_open("ids.bin") {
        let src = blob.slice(0, blob.len_bytes());
        let mut at = 0usize;
        let mut n = 0usize;
        let mut prev = 0usize;
        while at < src.len() && n < head.docs {
            let common = varint(src, &mut at) as usize;
            let tail = varint(src, &mut at) as usize;
            if at + tail > src.len() || common > prev {
                break;
            }
            prev = common + tail;
            at += tail;
            n += 1;
        }
        if n != head.docs || at != src.len() {
            s.broken(
                "поток идентификаторов",
                "документ вернётся с пустым идентификатором: обход за концом файла отдаёт пустую строку, а не отказ",
                format!(
                    "разобрано {n} из {} документов, прочитано {at} из {} байт",
                    head.docs,
                    src.len()
                ),
            );
        }
    }

    s.at(&format!("{name}/idmap.blob"));
    let idmap = crate::ids::IdMap::open(src, head.docs, head.layout);
    // При нынешней раскладке обратное отображение пишут и приём, и слияние —
    // всегда и все четыре файла. Значит его отсутствие это не «старый сегмент»,
    // а поломка: удаление по идентификатору в таком сегменте не найдёт ничего.
    if idmap.is_empty() {
        s.broken(
            "нет обратного отображения",
            "удаление по идентификатору не найдёт документов этого сегмента и молча вернёт ноль",
            "словарь идентификаторов пуст".into(),
        );
    } else if let Err(e) = idmap.check(head.docs) {
        s.broken(
            "обратное отображение",
            "гашение прочтёт номер документа нулём и убьёт документ 0 вместо искомого",
            e,
        );
    }
}

/// Число переменной длины — как его пишет фронт-кодирование.
fn varint(src: &[u8], at: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0u32;
    while let Some(&b) = src.get(*at) {
        *at += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            break;
        }
        shift += 7;
    }
    v
}

/// Слово признака «есть погашенные» против настоящего счёта.
///
/// Нарушение не косметическое: признак — быстрый путь проверки живости, и на
/// узле-реплике, где каталог только на чтение, починки не случится вовсе.
/// Погашенные документы вернутся в выдачу.
/// Печать карты живых: то, чем ловится порча битов внутри неё.
///
/// Сумма из `sums.tsv` карту не накрывает — карта меняется после публикации
/// сегмента, когда документы гасят, а сумма относилась бы к состоянию на момент
/// сборки. Поэтому у карты печать своя, внутри неё же.
///
/// Снятая печать поломкой не считается, и это не поблажка. Она значит одно из
/// двух: карту правят прямо сейчас (тогда проверка застала живую работу) либо
/// узел убили между гашением и сбросом (тогда гашение вернёт журнал приёма).
/// Объявить это поломкой значило бы завести тревогу, которая срабатывает на
/// исправном узле, — а такая обесценивает и настоящие.
fn live_seal(dir: &str, docs: usize, name: &str, s: &mut Sink) {
    let Ok(map) = Mapped::open(&format!("{dir}/{}", crate::live::FILE)) else { return };
    match crate::live::verify_map(&map, docs) {
        crate::live::Seal::Ok => {}
        crate::live::Seal::Absent => {
            s.at(&format!("{name}/{}", crate::live::FILE));
            s.note(
                "карта живых без печати",
                "порча бита внутри карты не будет замечена: погашенный документ воскреснет либо живой пропадёт",
                "сегмент собран до появления печати".into(),
            );
        }
        crate::live::Seal::Open => {
            s.at(&format!("{name}/{}", crate::live::FILE));
            s.note(
                "печать карты живых снята",
                "судить о целости карты нельзя: её либо правят прямо сейчас, либо узел убили между гашением и сбросом",
                "счётчик печати чётный".into(),
            );
        }
        crate::live::Seal::Mismatch { want, got } => {
            s.at(&format!("{name}/{}", crate::live::FILE));
            s.broken(
                "печать карты живых не сходится",
                "карта изменилась мимо гашения: погашенный документ воскреснет либо живой пропадёт из выдачи",
                format!("сумма {got:016x} вместо {want:016x}"),
            );
        }
    }
}

fn live_flag(dir: &str, docs: usize, s: &mut Sink) {
    let Ok(map) = Mapped::open(&format!("{dir}/{}", crate::live::FILE)) else { return };
    let words = docs.div_ceil(64);
    let mut live = 0usize;
    for w in 0..words {
        let mut x = map.u64_at(w * 8);
        if w + 1 == words && !docs.is_multiple_of(64) {
            x &= (1u64 << (docs % 64)) - 1;
        }
        live += x.count_ones() as usize;
    }
    let dead = docs - live.min(docs);
    let flag = map.u64_at(words * 8);
    if dead > 0 && flag == 0 {
        s.broken(
            "признак погашенных",
            "удалённые документы вернутся в выдачу: быстрый путь проверки живости решит, что гасить нечего",
            format!("погашено {dead}, а признак нулевой"),
        );
    }
    // Хвостовые биты за числом документов обязаны быть нулевыми, иначе счёт
    // мёртвых поедет на любом сегменте, чей размер не кратен шестидесяти четырём.
    if !docs.is_multiple_of(64) {
        let last = map.u64_at((words - 1) * 8);
        if last >> (docs % 64) != 0 {
            s.note(
                "хвост карты живых",
                "счёт погашенных будет завышен или занижен: биты за числом документов не нулевые",
                "за последним документом в карте стоят единицы".into(),
            );
        }
    }
}

/// Опись файлов сегмента: чего не хватает и что лишнее.
///
/// Нужна затем, что «файла нет» и «файл появился» — разные болезни. Первая
/// известна по списку обязательных. Вторая известна только описи: файл, которого
/// формат не знает, это либо остаток прерванной записи, либо чужая раскладка,
/// либо кто-то положил рядом своё. Молчать про такое нельзя — но и поломкой это
/// не является, поэтому заметка.
///
/// Хранилище поля проверяется тройкой: `.sbin`, `.smrk` и `.sdic` осмысленны
/// только вместе. Двух из трёх достаточно, чтобы содержимое не читалось, а поле
/// при этом объявлено хранимым — то есть выдача молча вернёт пустое значение.
fn inventory(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);
    s.at(&name);

    let mut known: Vec<String> = [
        "head.tsv",
        "postings.bin",
        "blocks.bin",
        "terms.bin",
        "terms.idx",
        "dict.blob",
        "dict.idx",
        "terms.bloom",
        "norms.bin",
        "ids.bin",
        "ids.idx",
        "idmap.blob",
        "idmap.idx",
        "idmap.docs",
        crate::ids::PACKED,
        "idmap.bloom",
        "pos.bin",
        "pos.idx",
        crate::live::FILE,
        crate::sums::FILE,
    ]
    .iter()
    .map(|x| x.to_string())
    .collect();

    // Полосы арности групп. Без них проверка объявляла «формат такого файла не
    // знает» о файлах, которые формат знает прекрасно: всякий индекс с массивом
    // получал по две заметки на группу. Заметка, которая бывает ложной на
    // здоровом индексе, обесценивает и все остальные — их перестают читать.
    for g in head.groups.iter().filter(|g| g.kind == crate::slots::Kind::Dense) {
        known.push(format!("g{}.aidx", g.id));
        known.push(format!("g{}.arity", g.id));
    }

    for f in head.fields.iter() {
        // Хранилище — тройка файлов, и она либо есть целиком, либо её нет.
        let trio = ["sbin", "smrk", "sdic"];
        let have: Vec<bool> = trio
            .iter()
            .map(|ext| {
                known.push(format!("{}.{ext}", f.stem));
                src.has(&format!("{}.{ext}", f.stem))
            })
            .collect();
        if f.stored && have.iter().any(|x| *x) && !have.iter().all(|x| *x) {
            let missing: Vec<&str> =
                trio.iter().zip(have.iter()).filter(|(_, h)| !**h).map(|(e, _)| *e).collect();
            s.broken(
                "неполное хранилище",
                "поле объявлено хранимым, а содержимое вернётся пустым: выдача не отличит это от пустого документа",
                format!("у поля {} нет {}", f.name, missing.join(", ")),
            );
        }
        // Таблица словарей и суммы распакованных кусков. Обе необязательны:
        // таблица заводится только при нескольких словарях, суммы появились
        // позже сегментов, которые их не держат.
        for ext in ["sdix", "scrc"] {
            known.push(format!("{}.{ext}", f.stem));
        }
        if src.has(&format!("{}.sdix", f.stem)) {
            // Таблица без того, что она описывает, — не «лишний файл», а
            // потерянное хранилище: она отдельно осмысленной не бывает.
            let missing: Vec<&str> = trio
                .iter()
                .zip(have.iter())
                .filter(|(e, h)| !**h && **e != "sbin")
                .map(|(e, _)| *e)
                .collect();
            if !missing.is_empty() {
                s.broken(
                    "таблица словарей без словарей",
                    "поле хранимое, а куски распаковать нечем: содержимое вернётся пустым, и \
                     выдача не отличит это от пустого документа",
                    format!("у поля {} есть {}.sdix, а {} нет", f.name, f.stem, missing.join(", ")),
                );
            }
            // Раскладка ниже девятой таблицы словарей не знает. Двоичный файл,
            // который её не знает, взял бы из `.sdic` первую запись и
            // распаковывал бы ею **чужие** куски: чаще отказ, иногда мусор нужной
            // длины. Заголовок и файлы обязаны говорить одно.
            if head.layout < 9 {
                s.broken(
                    "таблица словарей не по раскладке",
                    "читатель этой раскладки распакует куски чужим словарём: содержимое \
                     документа окажется правдоподобным и неверным",
                    format!(
                        "у поля {} есть {}.sdix при раскладке {} — таблица заведена с девятой",
                        f.name, f.stem, head.layout
                    ),
                );
            }
        }
        // Колонка значений и её надстройка — только у числовых полей.
        for ext in ["col", "cmeta", "bkt", "bktidx", "has", "scol", "smeta", "nmv"] {
            known.push(format!("{}.{ext}", f.stem));
        }
        if f.multi_entries > 0 {
            let units = head.groups.get(f.group as usize).map_or(head.docs, |g| g.units);
            let checked = src.open(&format!("{}.nmv", f.stem)).and_then(|raw| {
                crate::multivalue::MultiColumn::open(raw, units, f.multi_entries, f.multi_values)
            });
            if let Err(error) = checked {
                s.broken(
                    "числовой массив повреждён",
                    "range/sort/stats потеряли typed значения",
                    format!("поле {}: {error}", f.name),
                );
            }
        } else if src.has(&format!("{}.nmv", f.stem)) {
            s.broken(
                "необъявленный числовой массив",
                "заголовок обязан объявлять все typed значения",
                format!("поле {}", f.name),
            );
        }
        // Заголовок обещает раскладку, и обещание надо сверить с диском.
        //
        // Без этого «файла нет, потому что он не нужен» и «файл потерялся»
        // неотличимы. Отличать их обязательно: у поля с раскладкой `dense`
        // пропавший `.col` означает, что отбор перестал находить документы, а
        // у поля с `none` того же файла не должно быть вовсе.
        if f.kind.numeric() || f.kind.keyword() {
            let there = |ext: &str| src.has(&format!("{}.{ext}", f.stem));
            match f.layout {
                crate::column::Layout::None => {
                    for ext in ["col", "cmeta", "has"] {
                        if there(ext) {
                            s.broken(
                                "лишняя колонка",
                                "заголовок обещает, что значений нет вовсе, а файл на месте: \
                                 одно из двух неверно",
                                format!(
                                    "у поля {} есть {}.{ext} при раскладке none",
                                    f.name, f.stem
                                ),
                            );
                        }
                    }
                }
                crate::column::Layout::Sparse => {
                    let mut missing: Vec<&str> = Vec::new();
                    if !there("scol") {
                        missing.push("scol");
                    }
                    if !there("smeta") {
                        missing.push("smeta");
                    }
                    if !missing.is_empty() {
                        s.broken(
                            "неполная колонка",
                            "отбор и сортировка по полю перестанут находить документы, а \
                             выдача не отличит это от их отсутствия",
                            format!("у поля {} нет {}", f.name, missing.join(", ")),
                        );
                    }
                    // Разреженная не держит ни битов присутствия, ни плотных
                    // данных: присутствие и есть наличие в списке.
                    for ext in ["col", "cmeta", "has"] {
                        if there(ext) {
                            s.broken(
                                "лишняя колонка",
                                "заголовок обещает разреженную раскладку, а плотные файлы на \
                                 месте: одно из двух неверно",
                                format!(
                                    "у поля {} есть {}.{ext} при раскладке sparse",
                                    f.name, f.stem
                                ),
                            );
                        }
                    }
                }
                crate::column::Layout::Dense => {
                    let mut missing: Vec<&str> = Vec::new();
                    if !there("col") {
                        missing.push("col");
                    }
                    if !there("cmeta") {
                        missing.push("cmeta");
                    }
                    if f.presence == crate::column::Presence::Bits && !there("has") {
                        missing.push("has");
                    }
                    if !missing.is_empty() {
                        s.broken(
                            "неполная колонка",
                            "отбор и сортировка по полю перестанут находить документы, а \
                             выдача не отличит это от их отсутствия",
                            format!("у поля {} нет {}", f.name, missing.join(", ")),
                        );
                    }
                    if f.presence == crate::column::Presence::All && there("has") {
                        s.broken(
                            "лишняя колонка",
                            "заголовок обещает значение у каждого документа, а биты \
                             присутствия на месте: одно из двух неверно",
                            format!("у поля {} есть {}.has при presence all", f.name, f.stem),
                        );
                    }
                }
            }
        }
        // Коды плотного вектора. Без этого проверка звала их лишними файлами —
        // то есть «остатком прерванной записи», — и всякий, кто ей верит,
        // удалил бы половину индекса.
        if f.kind.dims().is_some() {
            for ext in ["q8", "qm", "ivf", "ivfb"] {
                known.push(format!("{}.{ext}", f.stem));
            }
            if !valid_ivf(src, &f.stem, head.docs, f.kind.dims().unwrap_or(0)) {
                s.broken(
                    "разбиение IVF не сходится",
                    "приближённый поиск использует повреждённые центроиды или адреса групп",
                    format!("поле {}: размеры, значения либо пара ivf/ivfb невалидны", f.name),
                );
            }
            // И раз уж мы про них знаем — заодно сверяем, что они сходятся друг
            // с другом и с объявленной размерностью. Файл кодов не той длины
            // читался бы по соседним байтам, и выдача осталась бы правдоподобной.
            match crate::vector::Vectors::open(src, &f.stem) {
                Some(v) if head.layout < 13 && v.uses_versioned_format() => {
                    s.broken(
                        "версия векторного кодека",
                        "старый reader не умеет читать этот кодек",
                        format!("поле {}: версия q8/qm требует layout 13", f.name),
                    );
                }
                Some(v) if v.docs() != head.docs || Some(v.dims()) != f.kind.dims() => {
                    s.broken(
                        "векторы не сходятся",
                        "расстояние посчитается по чужим байтам, и выдача будет правдоподобной",
                        format!(
                            "поле {}: в кодах {} документов по {} измерений,                              а в заголовке {} по {}",
                            f.name,
                            v.docs(),
                            v.dims(),
                            head.docs,
                            f.kind.dims().unwrap_or(0)
                        ),
                    );
                }
                Some(_) => {}
                // Кодов нет вовсе — законно: поле могло быть не заполнено ни у
                // одного документа сегмента. Но если хоть один файл на месте, а
                // открытие не удалось, значит пара разъехалась.
                None => {
                    // Куски вектора зовутся по **основе** имён, а не по имени
                    // поля: у пути с точкой основой служит `f{fid}` (см.
                    // [`crate::schema::FieldDef::stem`]), и `media.embedding.q8`
                    // не существует ни у одного сегмента. Спрос по имени
                    // означал бы, что эта ветвь не срабатывает никогда: пара
                    // разъехавшихся кусков вложенного векторного поля проходила
                    // бы проверку чистой, а поиск по нему молча ничего не
                    // находил.
                    let has = |ext: &str| src.has(&format!("{}.{ext}", f.stem));
                    if has("q8") || has("qm") {
                        s.broken(
                            "векторы не открываются",
                            "поиск по полю молча ничего не найдёт, будто вектора и не было",
                            format!("поле {}: файлы есть, а раскладка не сошлась", f.name),
                        );
                    }
                }
            }
        }
    }

    // У упакованного сегмента на диске лежат три файла, а куски перечисляет
    // оглавление контейнера. Считать «лишним» всё, чего нет среди этих трёх,
    // значило бы объявить остатком прерванной записи весь сегмент; и наоборот,
    // кусок с незнакомым именем внутри контейнера обязан называться так же, как
    // назывался бы лишний файл рядом.
    let mut present: Vec<String> = Vec::new();
    if let Some(t) = src.table() {
        present.extend(t.members().iter().map(|m| m.name.clone()));
        known.push(crate::container::FILE.to_string());
    }
    let Ok(it) = std::fs::read_dir(dir) else { return };
    for e in it.filter_map(|e| e.ok()) {
        if e.path().is_dir() {
            continue;
        }
        present.push(e.file_name().to_string_lossy().to_string());
    }
    let mut extra: Vec<String> = Vec::new();
    for n in present {
        if known.contains(&n) || extra.contains(&n) {
            continue;
        }
        extra.push(n);
    }
    extra.sort();
    for n in extra {
        s.note(
            "лишний файл",
            "формат такого файла не знает: остаток прерванной записи, чужая раскладка или что-то положенное рядом",
            n,
        );
    }
}

/// Производные IVF-файлы старого writer легальны, но неизвестные bytes нельзя
/// молча принять при миграции. Проверяем также адреса групп и конечность чисел.
fn valid_ivf(src: &crate::container::SegmentSource, stem: &str, docs: usize, dims: usize) -> bool {
    let a = format!("{stem}.ivf");
    let b = format!("{stem}.ivfb");
    if !src.has(&a) && !src.has(&b) {
        return true;
    }
    let (Ok(cent), Ok(band)) = (src.open(&a), src.open(&b)) else { return false };
    if cent.len_bytes() < 8 {
        return false;
    }
    let n = cent.u32_at(0) as usize;
    if !(2..=u16::MAX as usize).contains(&n) || cent.u32_at(4) as usize != dims {
        return false;
    }
    let Some(values) = n.checked_mul(dims) else { return false };
    if values.checked_mul(4).and_then(|v| v.checked_add(8)) != Some(cent.len_bytes())
        || docs.checked_mul(2) != Some(band.len_bytes())
    {
        return false;
    }
    (0..values).all(|i| f32::from_bits(cent.u32_at(8 + i * 4)).is_finite())
        && (0..docs).all(|i| (band.u16_at(i * 2) as usize) < n)
}

/// Полный проход по метаданным слов седьмой раскладки.
///
/// ## Почему здесь нельзя обойтись сверкой размеров
///
/// Поэтому проход честный, запись за записью. Он же проверяет главный договор
/// раскладки: `postings.bin` и `pos.bin` — это **склейка кусков, чьи длины лежат
/// в записях слов**, а число блоков — сумма по длинным спискам. Три числа,
/// которые обязаны сойтись с тремя файлами.
fn streams7(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);
    let Ok(t7) = crate::layout::Terms::open(src, head.terms) else { return };
    let post_len = src.len_of("postings.bin").unwrap_or(0);
    let pos_len = src.len_of("pos.bin").unwrap_or(0);
    let groups = crate::layout::Terms::groups(head.terms);

    s.at(&format!("{name}/terms.bin"));
    let (mut at, mut post, mut pos, mut block) = (0usize, 0usize, 0usize, 0usize);
    let mut bad = 0usize;
    let mut far = 0u64;
    // Слово без позиций законно ровно до тех пор, пока его частота не выше
    // границы из заголовка. Выше — граница врёт, и врёт она планировщику: по ней
    // он решает, укладывается ли проверка фразы по тексту в бюджет.
    let verifiable: Vec<bool> =
        head.fields.iter().map(|f| f.stored && f.kind == crate::index::Kind::Text).collect();
    let any_verifiable = verifiable.iter().any(|&v| v);
    // Какому полю принадлежит слово — по словарю. Без этого проверка не может
    // отличить «позиции потерялись» от «у этого поля их и не бывает»: у
    // разреженного вектора, ключевого слова и числа порядка слов нет вовсе, и
    // все их слова законно лежат без позиций.
    //
    // Соответствие строится, только когда схема **смешанная**: если позиции
    // есть у всех полей, различать нечего, и байт на слово не тратится.
    let mixed = verifiable.iter().any(|&v| !v) && any_verifiable;
    let owner: Vec<u32> =
        match mixed.then(|| crate::dict::Dict::open(src, head.terms, head.layout).ok()).flatten() {
            Some(d) => {
                let mut t = vec![u32::MAX; head.terms];
                let mut w = d.walk();
                while let Some((f, _, list)) = w.next() {
                    if let Some(slot) = t.get_mut(list as usize) {
                        *slot = f;
                    }
                }
                t
            }
            None => Vec::new(),
        };
    // Годится ли слово к проверке по тексту. Список без хозяина в словаре —
    // сам по себе беда, но не эта: о нём отчитывается разбор словаря.
    let checkable = |list: usize| -> bool {
        // Смешанная схема, но словарь не открылся — молчим. Отчитываться о нём
        // будет разбор словаря; выдумывать поломку по недочитанному нельзя.
        if !mixed {
            return any_verifiable;
        }
        if owner.is_empty() {
            return false;
        }
        match owner.get(list).copied() {
            Some(f) if f != u32::MAX => verifiable.get(f as usize).copied().unwrap_or(false),
            _ => false,
        }
    };
    let (mut over, mut worst) = (0u64, 0u32);
    for g in 0..groups {
        let mut w = t7.walk(g);
        let (start, p0, s0, b0) = w.here();
        if (start, p0, s0, b0) != (at, post, pos, block) {
            s.broken(
                "опора не там",
                "группа слов начнётся не с той записи: разбор поедет и списки прочтут чужие номера",
                format!(
                    "группа {g} обещает {start}/{p0}/{s0}/{b0}, \
                     а прошлая кончилась на {at}/{post}/{pos}/{block}"
                ),
            );
            bad += 1;
            if bad > 4 {
                break;
            }
        }
        let want = (head.terms - g * crate::layout::ANCHOR).min(crate::layout::ANCHOR);
        let mut seen = 0usize;
        while let Some(loc) = w.next() {
            let r = loc.rec;
            // Одиночное слово несёт номер документа в себе, и номер этот обязан
            // существовать: иначе обход выйдет за нормы и за карту живых.
            if r.df == 1 && r.doc as usize >= head.docs {
                far += 1;
            }
            if (loc.post + r.post_len as usize) as u64 > post_len {
                far += 1;
            }
            if (loc.pos + r.pos_len as usize) as u64 > pos_len {
                far += 1;
            }
            if checkable(g * crate::layout::ANCHOR + seen) && !r.positioned && r.df > head.posmin {
                over += 1;
                worst = worst.max(r.df);
            }
            seen += 1;
        }
        if seen != want {
            s.broken(
                "запись не разобралась",
                "слово прочтёт чужой список, а не пустой: у переменной длины сдвиг \
                 уносит и все последующие записи группы",
                format!("в группе {g} разобрано {seen} слов из {want}"),
            );
            bad += 1;
            if bad > 4 {
                break;
            }
        }
        (at, post, pos, block) = w.here();
    }
    if far > 0 {
        s.broken(
            "запись показывает за файл",
            "список прочтёт нули и окажется пустым, а одиночное слово — чужой документ",
            format!("{far} записей с номером или куском за границей файла"),
        );
    }
    if over > 0 {
        s.broken(
            "граница проверки занижена",
            "фраза с таким словом пойдёт проверяться по тексту, а планировщик \
             будет считать, что кандидатов не больше объявленного — и бюджет \
             перестанет быть бюджетом",
            format!(
                "{over} слов без позиций с частотой выше posmin = {}; \
                 самое частое из них встречается в {worst} документах",
                head.posmin
            ),
        );
    }
    if bad == 0 {
        for (file, got, want, means) in [
            (
                "terms.bin",
                t7.bytes() as u64,
                at as u64,
                "часть слов лежит за концом файла либо после них дописано лишнее",
            ),
            (
                "postings.bin",
                post_len,
                post as u64,
                "куски списков не сходятся с их длинами: список прочтёт чужие номера",
            ),
            (
                "pos.bin",
                pos_len,
                pos as u64,
                "куски позиций не сходятся с их длинами: фраза найдётся не там",
            ),
            (
                "blocks.bin",
                head.blocks as u64,
                block as u64,
                "номера блоков разъедутся, и длинный список прочтёт чужие потолки",
            ),
        ] {
            if got != want {
                s.at(&format!("{name}/{file}"));
                s.broken("не сходится с метаданными слов", means, format!("{got} против {want}"));
            }
        }
    }

    blocks_meta(src, head, s);
    positions(src, head, s);
    stores(src, head, s);
}

/// Полный проход по потокам сегмента.
fn streams(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    if head.layout >= 7 {
        return streams7(src, head, s);
    }
    let name = crate::manifest::name_of(dir);
    let Some(terms) = src.try_open("terms.bin") else { return };
    let post_len = src.len_of("postings.bin").unwrap_or(0);

    // Сходимость счётчиков: сумма блоков по всем спискам обязана дать blocks из
    // заголовка. Расхождение означает, что номера блоков поехали, и список
    // читает чужие метаданные.
    s.at(&format!("{name}/terms.bin"));
    let mut want_blocks = 0u64;
    let mut prev_start = 0u64;
    let mut prev_first = 0u64;
    for i in 0..head.terms {
        let df = terms.u32_at(i * TERM_META);
        let first = terms.u32_at(i * TERM_META + 4) as u64;
        let start = terms.u32_at(i * TERM_META + 8) as u64;
        if df == 0 {
            s.broken(
                "пустой список",
                "слово есть в словаре, а документов у него нет: обход отбросит его молча",
                format!("список {i} с df = 0"),
            );
        }
        if i > 0 && start < prev_start {
            s.broken(
                "порядок смещений",
                "список прочтёт чужие байты: смещения обязаны возрастать",
                format!("список {i}: смещение {start} после {prev_start}"),
            );
        }
        if i > 0 && first < prev_first {
            s.broken(
                "порядок блоков",
                "список прочтёт чужие метаданные блоков",
                format!("список {i}: первый блок {first} после {prev_first}"),
            );
        }
        if start > post_len {
            s.broken(
                "смещение за файлом",
                "список прочтёт нули и окажется пустым",
                format!("список {i}: смещение {start} при длине postings.bin {post_len}"),
            );
        }
        want_blocks += crate::index::blocks_of(df) as u64;
        prev_start = start;
        prev_first = first;
    }
    if want_blocks != head.blocks as u64 {
        s.broken(
            "счёт блоков",
            "номера блоков разъедутся, и список прочтёт чужие потолки и длины",
            format!("по спискам {want_blocks}, в заголовке {}", head.blocks),
        );
    }

    blocks_meta(src, head, s);
    positions(src, head, s);
    stores(src, head, s);
}

/// Сверка с сохранёнными контрольными суммами.
///
/// Всё остальное здесь проверяет **устройство**: длины против заголовка, суммы
/// кусков против записей слов, возрастание указателей. Это ловит обрезку,
/// дописанное и сдвиг — то есть всё, что разъезжается со счётом.
///
/// Подмену байта внутри куска не ловит ничто из перечисленного: длины сходятся,
/// счётчики сходятся, структура цела, а номер документа другой. Здесь она и
/// ловится — и только здесь.
fn checksums(dir: &str, s: &mut Sink) {
    let name = crate::manifest::name_of(dir);
    let Some(got) = crate::sums::inspect(dir) else {
        s.at(&name);
        s.broken(
            "нет сводки сумм",
            "проверять нечем: без сумм порча внутри файла не видна, а «всё цело» \
             становится утверждением, которого нечем подкрепить",
            "sums.tsv не читается или сегмент собран до его появления".into(),
        );
        return;
    };
    // Сводка, не назвавшая ни одного файла, — сломанная сводка.
    //
    // Обнулённый `sums.tsv` разбирается как пустой: ни одной строки, значит
    // сверять не с чем, и каждый файл сегмента становится «лишним» — заметкой.
    // Проверено: после обнуления сводки `check --full` отвечал «поломок 0» и
    // кодом 0 **даже на заведомо испорченном** postings.bin. То есть проверяльщик
    // отключался порчей одного файла, а тот, кто на него полагался, узнавал об
    // этом никогда.
    //
    // Спрашивается это у самой сводки — сколько строк она назвала, — а не у
    // перечня находок, и разница не в аккуратности. По находкам два случая
    // неразличимы: у пустой сводки расхождений нет, потому что расходиться
    // нечему, а у полной и сошедшейся — потому что всё сошлось. Прежнее условие
    // «расхождений ноль, а лишний файл есть» смешивало их и объявляло здоровый
    // сегмент битым от одной сироты прерванной записи, да ещё словами «сверять
    // не с чем» при полной сводке из двадцати одной сошедшейся строки — то есть
    // говорило оператору, что проверка ослепла, ровно там, где она видела всё.
    if got.named == 0 {
        s.at(&name);
        s.broken(
            "сводка сумм пуста",
            "она есть, но не покрывает ни одного файла: сверять не с чем, а порча \
             внутри любого файла остаётся незамеченной",
            // Считаются имена, а не файлы: у упакованного сегмента сверке
            // подлежат и куски внутри `seg.dat`, и их в этом числе больше, чем
            // файлов в каталоге.
            format!("строк в сводке ни одной, имён к сверке в сегменте: {}", got.bad.len()),
        );
        return;
    }
    for (file, v) in got.bad {
        s.at(&format!("{name}/{file}"));
        match v {
            crate::sums::Verdict::Content { want, got } => s.broken(
                "содержимое не то",
                "длина сошлась, а байты другие: номер документа, позиция или потолок читаются чужими",
                format!("сумма {got:08x} вместо {want:08x}"),
            ),
            crate::sums::Verdict::Length { got, want } => s.broken(
                "длина не та",
                "файл дописан или обрезан после сборки сегмента",
                format!("{got} байт вместо {want}"),
            ),
            crate::sums::Verdict::Gone => s.broken(
                "файла нет",
                "он был при сборке сегмента и пропал после неё",
                "назван в сводке, а на диске его нет".into(),
            ),
            // Кусок контейнера без строки в сводке — не безобидная сирота:
            // он входит в работающий сегмент, но его байты уже нечем
            // подтвердить. Так выглядел `sums.tsv`, обрезанный ровно между
            // строками: `check --full` видел кусок, но возвращал код ноль.
            crate::sums::Verdict::Extra if file.starts_with("seg.dat/") => s.broken(
                "куска нет в сводке",
                "кусок входит в контейнер сегмента, но контрольной суммы для него нет",
                "есть в оглавлении seg.dat, а в sums.tsv не назван".into(),
            ),
            crate::sums::Verdict::Extra => s.note(
                "файла нет в сводке",
                "появился после сборки: остаток прерванной записи или что-то положенное рядом",
                "есть на диске, а в сводке нет".into(),
            ),
            // «Не прочёлся» — не «его нет»: сегмент, возможно, цел, а не хватило
            // прав, памяти или не разобралось оглавление. Поломка всё равно —
            // то, чего не читает проверка, не прочтёт и поиск, — но названа она
            // тем, что случилось, а не догадкой о данных, которых никто не видел.
            crate::sums::Verdict::Unreadable { why } => s.broken(
                "прочесть не удалось",
                "судить о целости нечем: до содержимого дело не дошло",
                why,
            ),
            crate::sums::Verdict::Same => {}
        }
    }
}

/// Метаданные блоков: последний номер обязан быть номером живущего документа.
fn blocks_meta(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);
    let Some(blocks) = src.try_open("blocks.bin") else { return };
    s.at(&format!("{name}/blocks.bin"));
    let mut bad_doc = 0u64;
    for b in 0..head.blocks {
        if blocks.u32_at(b * BLOCK_META) as usize >= head.docs {
            bad_doc += 1;
        }
    }
    if bad_doc > 0 {
        s.broken(
            "номер за числом документов",
            "обход выйдет за карту живых и за нормы: документа с таким номером в сегменте нет",
            format!("{bad_doc} блоков с последним номером не меньше {}", head.docs),
        );
    }
}

/// Хранилище документов: куски обязаны покрывать номера подряд и без нахлёстов.
///
/// Возрастания поля «первый номер» мало, и это главное, что здесь проверяется.
/// Уменьшить его на восемь можно так, что порядок уцелеет и все проверки по
/// метаданным пройдут, — а `get` начнёт отдавать содержимое **соседнего**
/// документа: без отказа, без паники и без единого признака в ответе. Ловится
/// это только числом значений внутри куска против разности первых номеров, то
/// есть распаковкой каждого куска.
fn stores(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);
    for f in head.fields.iter().filter(|f| f.stored) {
        let Some(store) = crate::store::Store::open(src, &f.stem) else {
            let trio = ["sbin", "smrk", "sdic"];
            let have = trio.iter().filter(|e| src.has(&format!("{}.{e}", f.stem))).count();
            if have == trio.len() {
                // Куски на месте, а хранилище не открылось — молча пропустить
                // нельзя. Так выглядит гнутая таблица словарей: `.sdix`
                // отвергнут, поле осталось без содержимого, и без этой строки
                // проверка о том не сказала бы ни слова.
                s.at(&format!("{name}/{}.sdic", f.stem));
                s.broken(
                    "хранилище не открывается",
                    "файлы на месте, а содержимое поля вернётся пустым: выдача не отличит \
                     это от пустого документа",
                    format!(
                        "поле {}: словарь, таблица словарей или суммы кусков не сошлись",
                        f.name
                    ),
                );
            } else if have == 0 {
                gone_store(src, head, f, &name, s);
            }
            // Часть кусков на месте, часть нет — об этом уже сказала опись
            // («неполное хранилище»), и второй раз о том же говорить незачем.
            continue;
        };
        s.at(&format!("{name}/{}.smrk", f.stem));
        let n = store.blocks();
        let mut want_first = 0u32;
        let mut bad = 0usize;
        for i in 0..n {
            let Some((first, count, raw)) = store.chunk_at(i) else {
                s.broken(
                    "кусок не читается",
                    "содержимое поля вернётся пустым, и выдача не отличит это от пустого документа",
                    format!("кусок {i} из {n}"),
                );
                bad += 1;
                if bad > 4 {
                    break;
                }
                continue;
            };
            // Сумма распакованного: `sums.tsv` доказывает, что байты `.sbin`
            // целы, но не то, что они распаковываются в то же самое. У
            // перенесённого укрупнением куска сводка новая, посчитанная уже по
            // перенесённому, — и подмену словаря ловит **только** это.
            if let Some(want) = store.chunk_crc(i) {
                let got = crate::wal::crc32(&raw);
                if got != want {
                    s.broken(
                        "кусок распаковался не в то",
                        "содержимое документа вернётся другим: байты целы, а распакованное \
                         не сошлось с суммой, записанной при сборке",
                        format!("кусок {i} из {n}: сумма {got:08x} вместо {want:08x}"),
                    );
                    bad += 1;
                    if bad > 4 {
                        break;
                    }
                }
            }
            if first != want_first {
                s.broken(
                    "номера кусков разошлись",
                    "выдача вернёт содержимое соседнего документа: двоичный поиск попадёт не в тот кусок",
                    format!("кусок {i}: первый номер {first}, а по счёту значений {want_first}"),
                );
                bad += 1;
                if bad > 4 {
                    break;
                }
            }
            want_first = first + count as u32;
        }
        if bad == 0 && want_first as usize != head.docs {
            s.broken(
                "хранилище не покрывает документы",
                "у части документов содержимое вернётся пустым",
                format!("куски накрыли {want_first} номеров при {} документах", head.docs),
            );
        }
    }
}

/// Хранимое поле без единого куска хранилища: когда это молчание, а когда поломка.
///
/// ## Почему само отсутствие поломкой не объявляется
///
/// Оно законно, и встречается на здоровых сегментах сплошь. Точное числовое поле
/// хранимым объявлено, а хранилища не получает никогда — его значение печатает
/// обратно колонка (см. `indexer`). Полю, у которого во всём сегменте не
/// набралось содержимого, словарь сжатия не обучается, и `store::build` честно
/// отвечает «хранилище такому полю не нужно». Объявить это поломкой значило бы
/// завести тревогу, срабатывающую на исправном индексе, — а такая обесценивает и
/// настоящие.
///
/// ## Почему молчать тоже нельзя
///
/// Ровно так выглядит хранилище, потерянное слиянием. И ни одна другая проверка
/// этого не видит: охрана открытия (`index::load`) требует, чтобы **все три**
/// куска были на месте, — здесь нет ни одного; опись состава считает поломкой
/// неполную тройку, а пустая тройка для неё не поломка; сводка сумм перечисляет
/// то, что нашла, а не то, что обещает заголовок, и строка про пропавший кусок
/// исчезает вместе с самим куском. `fetch` при этом отдаёт пустую строку на
/// каждый документ, и выдача не отличает её от пустого документа.
///
/// ## Чем два случая различаются
///
/// Полосой длин. У текстового поля уровень длины считается по числу слов, и
/// ненулевой он ровно тогда, когда слова были: содержимое у поля в сегменте есть,
/// а хранилища нет — значит его потеряли.
///
/// Остальным видам полос не свидетель, и потому они здесь пропускаются молча:
/// числовому полю, вектору и `noindex` полоса пишется нулями по построению, а
/// ключевому — постоянной единицей независимо от того, было значение или нет.
/// Пропуск честнее догадки: поломка, которая бывает ложной, ничего не значит.
///
/// Отсюда же и глубина: полосу надо прочесть, и место этому в полной проверке.
/// Быстрая о том, чего не смотрела, говорит отдельной заметкой.
fn gone_store(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    f: &crate::index::Field,
    name: &str,
    s: &mut Sink,
) {
    if f.kind != crate::index::Kind::Text {
        return;
    }
    let Some(norms) = src.try_open("norms.bin") else { return };
    let Some(at) = (f.norms_band as usize).checked_mul(head.docs) else { return };
    let band = norms.slice(at, head.docs);
    // Полоса короче числа документов — это уже сказано сверкой размеров
    // `norms.bin`, и судить по недочитанному нельзя: «слов не было» и «полосу
    // обрезало» здесь неотличимы.
    if band.len() != head.docs || !band.iter().any(|&q| q != 0) {
        return;
    }
    s.at(&format!("{name}/{}.sbin", f.stem));
    s.broken(
        "нет хранилища",
        "содержимое поля вернётся пустым у каждого документа, и выдача не отличит это \
         от пустого документа",
        format!(
            "поле {} объявлено хранимым и не пусто по полосе длин, а кусков хранилища \
             нет ни одного",
            f.name
        ),
    );
}

/// Позиции: длина указателя, монотонность, границы.
fn positions(
    src: &std::sync::Arc<crate::container::SegmentSource>,
    head: &crate::index::Head,
    s: &mut Sink,
) {
    let dir = src.dir();
    let name = crate::manifest::name_of(dir);
    let pos_len = src.len_of("pos.bin").unwrap_or(0);
    let Some(idx) = src.try_open("pos.idx") else { return };

    s.at(&format!("{name}/pos.idx"));
    let want = (head.blocks * 4) as u64;
    if (idx.len_bytes() as u64) < want {
        s.broken(
            "размер",
            "фраза по одному полю прочтёт позиции другого: номер блока перестанет совпадать",
            format!("{} байт вместо {want}", idx.len_bytes()),
        );
        return;
    }
    let mut prev = 0u32;
    let mut back = 0u64;
    for b in 0..head.blocks {
        let at = idx.u32_at(b * 4);
        if at < prev {
            back += 1;
        }
        if at as u64 > pos_len {
            s.broken(
                "указатель за файлом",
                "чтение позиций уйдёт за конец и вернёт нули: фраза не найдётся",
                format!("блок {b}: указатель {at} при длине pos.bin {pos_len}"),
            );
            break;
        }
        prev = at;
    }
    if back > 0 {
        s.broken(
            "порядок указателей",
            "блок прочтёт чужие позиции: указатели обязаны не убывать",
            format!("{back} указателей меньше предыдущего"),
        );
    }
}

/// Средства правки **упакованного** сегмента для проверок ниже.
///
/// ## Зачем они, а не `container::unpack`
///
/// Проверки портят куски поимённо: обрезают, подменяют, убирают. У рассыпанного
/// сегмента это правка файла; у упакованного кусок лежит внутри `seg.dat` между
/// соседями, и укоротить его на месте нечем — смещения соседей записаны в
/// оглавлении. Разложить сегмент обратно по файлам было бы проще всего, и это
/// подменяет **предмет** проверки: боевой индекс состоит из контейнеров, и
/// проверка, прошедшая по россыпи, о контейнере не сказала ничего.
///
/// Поэтому контейнер пересобирается: куски читаются из оглавления, нужный
/// подменяется, и писатель складывает их заново — ровно тем же путём, каким
/// сложил бы приём. Заголовок получает новую обещанную длину, сводка — новые
/// суммы. Пересчёт сводки здесь не поблажка, а ужесточение: с ним подделку
/// обязана поймать сама проверка строения, а не сверка с суммами сборки.
#[cfg(test)]
mod packing {
    /// Раскладка берётся у самого контейнера: подделывается кусок, а не заголовок.
    fn layout_of(whole: &[u8]) -> u32 {
        u32::from_le_bytes(whole[12..16].try_into().expect("четыре байта"))
    }

    /// Куски сегмента именами и телами, по порядку оглавления.
    pub(super) fn pieces(seg: &str) -> Vec<(String, Vec<u8>)> {
        let path = format!("{seg}/{}", crate::container::FILE);
        let whole = std::fs::read(&path).expect("контейнер сегмента");
        let table = crate::container::Table::parse(&path, &whole).expect("оглавление");
        table
            .members()
            .iter()
            .map(|m| (m.name.clone(), whole[m.off as usize..(m.off + m.len) as usize].to_vec()))
            .collect()
    }

    /// Длина куска сегмента.
    pub(super) fn len_of(seg: &str, name: &str) -> u64 {
        pieces(seg)
            .into_iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("куска {name} в сегменте нет"))
            .1
            .len() as u64
    }

    /// Пересобирает контейнер, пропустив каждый кусок через `edit`.
    ///
    /// `edit` отдаёт `None` — кусок исчезает. `extra` кладётся сверх, подменяя
    /// одноимённый: два куска под одним именем писатель не примет.
    pub(super) fn repack(
        seg: &str,
        mut edit: impl FnMut(&str, Vec<u8>) -> Option<Vec<u8>>,
        extra: &[(String, Vec<u8>)],
    ) {
        let path = format!("{seg}/{}", crate::container::FILE);
        let whole = std::fs::read(&path).expect("контейнер сегмента");
        let layout = layout_of(&whole);
        let table = crate::container::Table::parse(&path, &whole).expect("оглавление");
        let mut keep: Vec<(String, Vec<u8>)> = Vec::new();
        for m in table.members() {
            let body = whole[m.off as usize..(m.off + m.len) as usize].to_vec();
            if let Some(b) = edit(&m.name, body) {
                keep.push((m.name.clone(), b));
            }
        }
        for (n, b) in extra {
            keep.retain(|(k, _)| k != n);
            keep.push((n.clone(), b.clone()));
        }
        let mut w = crate::container::Writer::create(seg, layout).expect("новый контейнер");
        for (n, b) in keep.iter() {
            w.put(n, b).expect("кусок");
        }
        let total = w.finish().expect("контейнер закрыт");
        // Заголовок обещает длину контейнера, и она изменилась. Оставь его как
        // был — и сегмент не открылся бы вовсе: проверялось бы расхождение
        // длин, а не то, ради чего подделка затевалась.
        let head_path = format!("{seg}/head.tsv");
        let text = std::fs::read_to_string(&head_path).expect("заголовок");
        let fixed: String = text
            .lines()
            .map(|l| {
                if l.starts_with("container\t") {
                    format!("container\t{}\t{total}\n", crate::container::FILE)
                } else {
                    format!("{l}\n")
                }
            })
            .collect();
        std::fs::write(&head_path, fixed).expect("заголовок");
        crate::sums::write(seg).expect("сводка");
    }

    /// Подменяет тело куска, заводя его, если такого не было.
    pub(super) fn put(seg: &str, name: &str, body: &[u8]) {
        repack(seg, |_, b| Some(b), &[(name.to_string(), body.to_vec())]);
    }

    /// Убирает кусок целиком.
    pub(super) fn drop_piece(seg: &str, name: &str) {
        let mut found = false;
        repack(
            seg,
            |n, b| {
                if n == name {
                    found = true;
                    return None;
                }
                Some(b)
            },
            &[],
        );
        assert!(found, "куска {name} в сегменте нет — убирать нечего");
    }

    /// Отрезает у куска хвост.
    ///
    /// Два пути, и оба нужны: `live.bin` в контейнер не кладётся никогда (он
    /// единственная область сегмента на запись), а всё остальное лежит куском.
    pub(super) fn cut(seg: &str, name: &str, tail: usize) {
        let path = format!("{seg}/{name}");
        if std::path::Path::new(&path).is_file() {
            let whole = std::fs::read(&path).expect("файл");
            std::fs::write(&path, &whole[..whole.len() - tail]).expect("запись");
            crate::sums::write(seg).expect("сводка");
            return;
        }
        let mut found = false;
        repack(
            seg,
            |n, b| {
                if n != name {
                    return Some(b);
                }
                found = true;
                assert!(b.len() >= tail, "кусок {name} короче отрезаемого хвоста");
                Some(b[..b.len() - tail].to_vec())
            },
            &[],
        );
        assert!(found, "куска {name} в сегменте нет — резать нечего");
    }

    /// Снимок каталога сегмента.
    pub(super) fn snap(seg: &str) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(seg)
            .expect("каталог сегмента")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .map(|e| {
                (
                    e.file_name().to_string_lossy().to_string(),
                    std::fs::read(e.path()).expect("файл"),
                )
            })
            .collect();
        out.sort();
        out
    }

    /// Возвращает каталог к снимку, убирая появившееся.
    pub(super) fn back(seg: &str, snap: &[(String, Vec<u8>)]) {
        for e in std::fs::read_dir(seg).expect("каталог сегмента").filter_map(|e| e.ok())
        {
            let n = e.file_name().to_string_lossy().to_string();
            if !snap.iter().any(|(k, _)| *k == n) {
                std::fs::remove_file(e.path()).expect("удаление");
            }
        }
        for (n, b) in snap {
            std::fs::write(format!("{seg}/{n}"), b).expect("возврат");
        }
    }
}

#[cfg(test)]
mod tests {
    /// Порча бита внутри карты живых обязана быть замечена.
    ///
    /// Это единственное, чего проверка не видела: сумма из `sums.tsv` карту не
    /// накрывает, потому что карта меняется **после** публикации сегмента —
    /// когда документы гасят. Разрушитель ловил это как «открылся и отвечает»
    /// при 127 пойманных случаях из 128.
    ///
    /// Последствие у такой порчи прямое: погашенный документ воскресает в
    /// выдаче либо живой из неё пропадает — молча, без отказа.
    #[test]
    fn a_flipped_bit_in_the_live_map_is_found() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-live-seal-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = crate::indexer::Indexer::new(
            &dir,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            10_000,
        );
        for i in 0..500u32 {
            ix.add(&format!("d{i}"), &[&format!("док {i}"), "общее тело"]);
        }
        ix.flush();
        ix.drain();
        let seg = crate::manifest::segments(&dir).first().cloned().expect("сегмент");
        assert!(check(&dir, Depth::Full).ok(), "свежий индекс обязан быть цел");

        // Гашение и сброс: печать обязана встать заново.
        {
            let live = crate::live::Live::open(&seg, 500).expect("карта");
            assert!(live.kill(3), "гашение прошло");
            live.sync().expect("сброс");
        }
        let r = check(&dir, Depth::Full);
        assert!(r.ok(), "после гашения со сбросом индекс обязан быть цел: {}", r.broken());

        // А теперь порча бита мимо гашения — ровно то, чего не видели.
        let path = format!("{seg}/{}", crate::live::FILE);
        let mut bytes = std::fs::read(&path).expect("карта читается");
        bytes[7] ^= 0b0100_0000;
        std::fs::write(&path, &bytes).expect("записано");
        let r = check(&dir, Depth::Full);
        assert!(!r.ok(), "порча бита обязана быть поломкой");
        assert!(
            r.findings.iter().any(|f| f.check == "печать карты живых не сходится"),
            "поломка обязана назвать причину: {:?}",
            r.findings.iter().map(|f| f.check).collect::<Vec<_>>()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Испорченную сводку сумм нельзя выдать за здоровый индекс.
    ///
    /// Сводка — то, чем проверяются все остальные файлы. Обнулённый `sums.tsv`
    /// разбирался как пустой: сверять не с чем, каждый файл становился «лишним»,
    /// то есть заметкой, и `check --full` отвечал «поломок 0» с кодом ноль —
    /// **даже когда рядом лежал заведомо испорченный `postings.bin`**. Один
    /// испорченный файл отключал проверяльщик целиком, и полагавшийся на него
    /// узнавал об этом никогда.
    #[test]
    fn a_broken_summary_is_not_a_healthy_index() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-sums-broken-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = crate::indexer::Indexer::new(
            &dir,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            10_000,
        );
        for i in 0..600u32 {
            ix.add(&format!("d{i}"), &[&format!("док {i}"), "общее тело слово"]);
        }
        ix.flush();
        ix.drain();
        let seg = crate::manifest::segments(&dir).first().cloned().expect("сегмент есть");

        assert!(check(&dir, Depth::Full).ok(), "здоровый индекс обязан быть цел");

        // Обнулить сводку — она есть, но не покрывает ничего.
        let sums = format!("{seg}/{}", crate::sums::FILE);
        let complete = std::fs::read_to_string(&sums).expect("сводка читается");
        let without_last = complete.trim_end().rsplit_once('\n').expect("строк много").0;
        std::fs::write(&sums, format!("{without_last}\n")).expect("последняя строка убрана");
        let r = check(&dir, Depth::Full);
        assert!(!r.ok(), "непокрытый кусок контейнера обязан быть поломкой");
        assert!(
            r.findings.iter().any(|f| f.check == "куска нет в сводке"),
            "поломка обязана назвать отсутствующую сумму: {:?}",
            r.findings.iter().map(|f| f.check).collect::<Vec<_>>()
        );

        // Отдельно проверяется полностью пустая сводка: это другой диагноз.
        std::fs::write(&sums, &complete).expect("полная сводка возвращена");
        let n = std::fs::metadata(&sums).expect("сводка есть").len() as usize;
        std::fs::write(&sums, vec![0u8; n]).expect("записано");
        let r = check(&dir, Depth::Full);
        assert!(!r.ok(), "обнулённая сводка обязана быть поломкой, а не заметкой");
        assert!(
            r.findings.iter().any(|f| f.check == "сводка сумм пуста"),
            "поломка обязана назвать причину: {:?}",
            r.findings.iter().map(|f| f.check).collect::<Vec<_>>()
        );

        // Убрать сводку совсем — проверять по-прежнему нечем.
        std::fs::remove_file(&sums).expect("удалено");
        let r = check(&dir, Depth::Full);
        assert!(!r.ok(), "отсутствие сводки обязано быть поломкой");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Один лишний файл не превращает здоровый сегмент в битый.
    ///
    /// Обратная сторона предыдущего теста, и без неё тот проходил бы даже у
    /// проверки, которая объявляет поломкой всё подряд. Сирота под временным
    /// именем — штатный остаток оборванной подмены куска или недоснесённой
    /// упаковки, и обещана за неё ровно заметка.
    ///
    /// Условие «покрытых расхождений ноль, а лишний файл есть» выдавало вместо
    /// заметки поломку, да ещё словами «сводка сумм пуста, сверять не с чем» — на
    /// полной сводке, сошедшейся до последней строки. Хуже текста было
    /// последствие: находка уходила с `return`, то есть один посторонний файл
    /// рядом отключал сверку сумм целиком — ровно то ослепление, о котором лгал
    /// текст. Поэтому здесь проверяются оба: и что сирота осталась заметкой, и
    /// что порча внутри куска при ней по-прежнему называется.
    #[test]
    fn one_stray_file_is_a_note_and_does_not_blind_the_summary() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-sums-stray-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = crate::indexer::Indexer::new(
            &dir,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            10_000,
        );
        for i in 0..400u32 {
            ix.add(&format!("d{i}"), &[&format!("док {i}"), "общее тело слово"]);
        }
        ix.flush();
        ix.drain();
        let seg = crate::manifest::segments(&dir).first().cloned().expect("сегмент есть");
        assert!(check(&dir, Depth::Full).ok(), "здоровый индекс обязан быть цел");

        let listed = crate::sums::read(&seg).expect("сводка читается");
        assert!(listed.len() > 10, "сводка полна: строк {}", listed.len());

        // Ровно то имя, что оставляет оборванная подмена куска.
        let orphan = format!("{seg}/.{}.tmp-777-0", crate::container::FILE);
        std::fs::write(&orphan, "половина оборванной записи").expect("сирота положена");
        let r = check(&dir, Depth::Full);
        let broken: Vec<(&str, String)> = r
            .findings
            .iter()
            .filter(|f| f.level == Level::Broken)
            .map(|f| (f.check, f.text.clone()))
            .collect();
        assert!(r.ok(), "лишний файл при полной сводке — заметка, а не поломка: {broken:?}");
        assert!(
            r.findings
                .iter()
                .any(|f| f.check == "файла нет в сводке" && f.scope.ends_with(".tmp-777-0")),
            "сироту обязано назвать заметкой поимённо: {:?}",
            r.findings.iter().map(|f| (f.check, f.scope.clone())).collect::<Vec<_>>()
        );

        // Сверка от сироты не ослепла: порча внутри куска называется как прежде.
        let path = format!("{seg}/{}", crate::container::FILE);
        let table = crate::container::Table::read(&path).expect("оглавление контейнера");
        let at =
            table.members().iter().find(|m| m.name == "norms.bin").expect("нормы лежат куском").off
                as usize;
        let mut bytes = std::fs::read(&path).expect("контейнер прочтён");
        bytes[at] ^= 1;
        std::fs::write(&path, &bytes).expect("порча записана");
        let r = check(&dir, Depth::Full);
        assert!(
            r.findings
                .iter()
                .any(|f| f.check == "содержимое не то" && f.scope.ends_with("seg.dat/norms.bin")),
            "порча куска обязана называться и при лишнем файле рядом: {:?}",
            r.findings.iter().map(|f| (f.check, f.scope.clone())).collect::<Vec<_>>()
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    use super::*;

    /// Собирает индекс из двух сегментов и отдаёт каталог.
    fn corpus(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-check-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(
            &d,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            150,
        );
        for i in 0..300u32 {
            ix.add(
                &format!("d{i:04}"),
                &[&format!("загол{}", i % 17), &format!("общее слово{} тело{}", i % 7, i % 53)],
            );
        }
        ix.flush();
        // Сегмент остаётся **упакованным**, каким его собрал приём. Раскладка
        // подменила бы предмет проверки: боевой индекс состоит из контейнеров, и
        // всё, что проверено по россыпи, про него не сказано. Портятся куски
        // средствами [`super::packing`].
        for seg in crate::manifest::segments(&d) {
            assert!(
                std::path::Path::new(&format!("{seg}/{}", crate::container::FILE)).exists(),
                "приём собрал {seg} россыпью — проверять тут нечего"
            );
        }
        d
    }

    /// Целый индекс проходит обе глубины без единой находки.
    #[test]
    fn a_whole_index_has_nothing_to_say() {
        let d = corpus("whole");
        for depth in [Depth::Fast, Depth::Full] {
            let r = check(&d, depth);
            assert!(r.ok(), "поломки на целом индексе: {:?}", texts(&r));
            // Заметка о глубине — не находка о данных, а честность инструмента:
            // быстрая проверка обязана сказать, чего не смотрела.
            let real: Vec<&Finding> = r.findings.iter().filter(|f| f.check != "глубина").collect();
            assert!(real.is_empty(), "заметки на целом индексе: {:?}", texts(&r));
            assert_eq!(
                r.findings.iter().any(|f| f.check == "глубина"),
                depth == Depth::Fast,
                "оговорка о глубине обязана быть ровно у быстрой проверки"
            );
            assert_eq!(r.segments, 2);
            assert_eq!(r.docs, 300);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    fn texts(r: &Report) -> Vec<String> {
        r.findings.iter().map(|f| format!("{} {}: {}", f.level.name(), f.scope, f.text)).collect()
    }

    /// Обрезка любого обязательного куска обязана быть найдена.
    ///
    /// Матрицей, а не по одному случаю: проверка ценна ровно настолько,
    /// насколько полон её список. Отрезается **одна запись** — самая мелкая
    /// порча, какая бывает, и та, которую труднее всего заметить иначе.
    ///
    /// Куски режутся внутри контейнера, и это ужесточение, а не оформление.
    /// Прежде проверка шла по разложенному сегменту, где обрезанный файл
    /// расходился со сводкой сумм, — и находку могла давать одна сводка, ничего
    /// не говоря о том, заметила ли обрезку сама проверка строения. Пересборка
    /// контейнера сводку **пересчитывает**: она сходится, суммы кусков в
    /// оглавлении сходятся, и назвать обрезанный кусок обязана проверка
    /// строения и только она.
    #[test]
    fn every_truncated_piece_is_found() {
        let d = corpus("cut");
        let seg = format!("{d}/seg0000");
        let cases: [(&str, usize); 9] = [
            ("terms.bin", TERM_META),
            ("blocks.bin", BLOCK_META),
            ("norms.bin", 1),
            ("live.bin", 8),
            ("ids.bin", 4),
            ("pos.idx", 4),
            // У этих троих размер из заголовка не выводится — сходится число
            // записей, и считается оно проходом.
            ("dict.blob", 8),
            ("ids.bin", 8),
            ("ids.idx", 4),
        ];
        for (file, tail) in cases {
            let was = super::packing::snap(&seg);
            super::packing::cut(&seg, file, tail);
            let r = check(&d, Depth::Full);
            assert!(!r.ok(), "обрезанный {file} принят как целый");
            assert!(
                r.findings.iter().any(|f| f.scope.contains(file)),
                "находка не назвала {file}: {:?}",
                texts(&r)
            );
            super::packing::back(&seg, &was);
            assert!(check(&d, Depth::Full).ok(), "возвращённый {file} всё ещё считается битым");
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Пересборка контейнера сама по себе ничего не меняет.
    ///
    /// Утверждение служебное и потому необходимое: все проверки выше портят
    /// куски пересборкой, и не будь она нейтральной, они меряли бы её саму.
    #[test]
    fn repacking_a_whole_segment_changes_nothing_check_can_see() {
        let d = corpus("repack");
        let seg = format!("{d}/seg0000");
        super::packing::repack(&seg, |_, b| Some(b), &[]);
        let r = check(&d, Depth::Full);
        assert!(r.ok(), "пересобранный сегмент объявлен битым: {:?}", texts(&r));
        assert!(r.findings.is_empty(), "пересборка добавила заметок: {:?}", texts(&r));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Ноль в заголовке при непустом куске обязан ловиться **у контейнера**.
    ///
    /// ## Что молчало
    ///
    /// Сторож брал размер `std::fs::metadata` по имени файла в каталоге
    /// сегмента. У упакованного сегмента файла `blocks.bin` там нет вовсе — есть
    /// кусок внутри `seg.dat`, — и размер выходил нулём. Условие «ноль в
    /// заголовке при непустом куске» не выполнялось никогда, и поломка
    /// вырождалась в заметку о лишних байтах: «160 байт при ожидаемых 0». То
    /// есть единственная охрана от неразобранного числа в заголовке молчала
    /// ровно на том виде сегмента, какой выдаёт приём.
    ///
    /// Заметка о лишних байтах проверяется здесь же и не случайно: она никуда не
    /// делась, и без утверждения о **поломке** этот тест прошёл бы и на старом
    /// сторже — по ней.
    #[test]
    fn a_zero_in_the_head_is_caught_inside_the_container() {
        let d = corpus("zero");
        let seg = format!("{d}/seg0000");
        let blocks = super::packing::len_of(&seg, "blocks.bin");
        assert!(blocks > 0, "кусок метаданных блоков пуст — заголовку не о чем врать");

        let path = format!("{seg}/head.tsv");
        let text = std::fs::read_to_string(&path).expect("заголовок");
        let was = text
            .lines()
            .find(|l| l.starts_with("blocks\t"))
            .expect("строка blocks обязана быть")
            .to_string();
        assert_ne!(was, "blocks\t0", "в заголовке и так ноль — подделывать нечего");
        std::fs::write(&path, text.replace(&was, "blocks\t0")).expect("запись");
        crate::sums::write(&seg).expect("сводка");

        let r = check(&d, Depth::Fast);
        assert!(!r.ok(), "ноль в заголовке при непустом куске принят: {:?}", texts(&r));
        let f = r
            .findings
            .iter()
            .find(|f| f.check == "число в заголовке")
            .unwrap_or_else(|| panic!("неразобранное число не названо: {:?}", texts(&r)));
        assert_eq!(f.level, Level::Broken, "выродилось в заметку");
        assert!(f.text.contains("blocks"), "не названо, какое именно число: {}", f.text);
        assert!(f.scope.contains("seg0000"), "находка приписана не сегменту: {}", f.scope);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Пара кусков вектора ищется по **основе** имён, а не по имени поля.
    ///
    /// ## Что не срабатывало
    ///
    /// У поля, чей путь содержит точку, именем файла служит `f{fid}` — точка в
    /// имени файла означала бы расширение (см. [`crate::schema::FieldDef::stem`]).
    /// Куски зовутся `f7.q8` и `f7.qm`, а ветвь «файлы есть, а раскладка не
    /// сошлась» спрашивала `media.embedding.q8`. Такого куска не бывает ни у
    /// одного сегмента, значит ветвь не срабатывала **никогда**: сегмент с
    /// разъехавшейся парой кодов вложенного векторного поля проходил
    /// `check --full` чистым, а поиск по этому полю молча ничего не находил.
    ///
    /// Оба случая проверяются разом — поле без точки и поле с точкой, — иначе
    /// доказано было бы только то, что ветвь срабатывает хоть когда-нибудь.
    #[test]
    fn a_dotted_vector_field_is_looked_up_by_its_stem() {
        let d = corpus("vecstem");
        let mut head = crate::index::read_head(&format!("{d}/seg0000")).expect("заголовок");

        for (name, stem) in [("emb", "emb"), ("media.embedding", "f7")] {
            let hole = std::env::temp_dir()
                .join(format!("svindb-check-vecstem-{stem}-{}", std::process::id()))
                .to_string_lossy()
                .to_string();
            std::fs::remove_dir_all(&hole).ok();
            std::fs::create_dir_all(&hole).expect("каталог");
            // Куски лежат **куском контейнера**, а не файлом: спрос по имени
            // файла у упакованного сегмента не нашёл бы и правильного имени.
            let mut w = crate::container::Writer::create(&hole, head.layout).expect("контейнер");
            w.put(&format!("{stem}.q8"), &[1u8; 16]).expect("коды");
            // Заголовок кодов короче обязательного — ровно та разъехавшаяся
            // пара, из-за которой `Vectors::open` отдаёт `None`.
            w.put(&format!("{stem}.qm"), &[2u8; 8]).expect("раскладка");
            let total = w.finish().expect("закрытие");
            let src = std::sync::Arc::new(
                crate::container::SegmentSource::of(&hole, Some(total)).expect("контейнер"),
            );
            assert!(
                crate::vector::Vectors::open(&src, stem).is_none(),
                "{name}: раскладка сошлась — проверять нечего"
            );

            head.fields[0].kind = crate::index::Kind::Dense(4);
            head.fields[0].name = name.to_string();
            head.fields[0].stem = stem.to_string();
            let mut s = Sink::new();
            inventory(&src, &head, &mut s);
            let say: Vec<String> =
                s.out.iter().map(|f| format!("{} {}: {}", f.check, f.scope, f.text)).collect();
            let hit: Vec<&Finding> =
                s.out.iter().filter(|f| f.check == "векторы не открываются").collect();
            assert_eq!(hit.len(), 1, "{name}: разъехавшаяся пара не названа: {say:?}");
            assert_eq!(hit[0].level, Level::Broken, "{name}: не тот уровень");
            assert!(hit[0].text.contains(name), "{name}: поле не названо: {}", hit[0].text);
            std::fs::remove_dir_all(&hole).ok();
        }
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn legacy_ivf_accepts_valid_pair_and_refuses_corruption() {
        let root = std::env::temp_dir().join(format!("svindb-check-ivf-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut cent = Vec::new();
        cent.extend(2u32.to_le_bytes());
        cent.extend(2u32.to_le_bytes());
        for x in [0f32, 1., 2., 3.] {
            cent.extend(x.to_le_bytes());
        }
        std::fs::write(root.join("f7.ivf"), &cent).unwrap();
        std::fs::write(root.join("f7.ivfb"), [0, 0, 1, 0, 0, 0]).unwrap();
        let valid = || {
            let src = crate::container::SegmentSource::of(&root.to_string_lossy(), None).unwrap();
            valid_ivf(&src, "f7", 3, 2)
        };
        assert!(valid());
        std::fs::write(root.join("f7.ivfb"), [0, 0, 2, 0, 0, 0]).unwrap();
        assert!(!valid(), "адрес вне числа центроидов");
        std::fs::write(root.join("f7.ivfb"), [0, 0, 1, 0, 0, 0]).unwrap();
        cent[8..12].copy_from_slice(&f32::NAN.to_le_bytes());
        std::fs::write(root.join("f7.ivf"), &cent).unwrap();
        assert!(!valid(), "NaN в центроидах");
        std::fs::remove_file(root.join("f7.ivf")).unwrap();
        assert!(!valid(), "одинокая band-карта");
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Пропавший сегмент — поломка, и она называет его по имени.
    #[test]
    fn a_missing_segment_is_named() {
        let d = corpus("missing");
        std::fs::remove_dir_all(format!("{d}/seg0001")).expect("сегмент");
        let r = check(&d, Depth::Fast);
        assert!(!r.ok());
        let f = r.findings.iter().find(|f| f.check == "нет сегмента").expect("находка");
        assert!(f.scope.contains("seg0001"), "не назван: {}", f.scope);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Потерянный сегмент — заметка, а не поломка, и трогать его нельзя.
    ///
    /// Каталог вне списка это остаток прерванного слияния: поиску он не виден,
    /// место занимает. Но проверка только читает — удаляет их подъём под замком
    /// писателя, и путать эти две роли нельзя.
    #[test]
    fn an_orphan_is_a_note_and_stays_where_it_is() {
        let d = corpus("orphan");
        // Все три приставки, а не одна: список у описи и у уборки обязан быть
        // общим. Разошёлся он ровно на `cln` — уборка его не знала, и каталог
        // прерванной уборки мёртвых не убирал никто и никогда.
        for name in ["seg9099", "mrg9099", "cln9099"] {
            let orphan = format!("{d}/{name}");
            std::fs::create_dir_all(&orphan).expect("каталог");
            std::fs::write(format!("{orphan}/head.tsv"), "docs\t1\n").expect("заголовок");
        }

        let r = check(&d, Depth::Fast);
        assert!(r.ok(), "потерянный сегмент не повод не пускать поиск");
        let lost: Vec<&Finding> =
            r.findings.iter().filter(|f| f.check == "потерянный сегмент").collect();
        assert_eq!(lost.len(), 3, "{:?}", texts(&r));
        for name in ["seg9099", "mrg9099", "cln9099"] {
            assert!(lost.iter().any(|f| f.text.contains(name)), "не назван {name}");
        }
        let orphan = format!("{d}/mrg9099");
        assert!(std::path::Path::new(&orphan).exists(), "проверка обязана только читать");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Проверка не открывает ни одного файла на запись.
    #[test]
    fn checking_never_writes() {
        let d = corpus("readonly");
        let before: Vec<(String, std::time::SystemTime, u64)> = files(&d);
        super::packing::cut(&format!("{d}/seg0000"), "terms.bin", TERM_META);
        let after_cut: Vec<(String, std::time::SystemTime, u64)> = files(&d);

        let r = check(&d, Depth::Full);
        assert!(!r.ok());
        assert_eq!(files(&d), after_cut, "проверка тронула файлы");
        assert_ne!(before, after_cut, "тест обязан был испортить хотя бы один файл");
        std::fs::remove_dir_all(&d).ok();
    }

    fn files(dir: &str) -> Vec<(String, std::time::SystemTime, u64)> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_string()];
        while let Some(d) = stack.pop() {
            let Ok(it) = std::fs::read_dir(&d) else { continue };
            for e in it.filter_map(|e| e.ok()) {
                let p = e.path().to_string_lossy().to_string();
                let Ok(m) = e.metadata() else { continue };
                if m.is_dir() {
                    stack.push(p);
                } else {
                    out.push((p, m.modified().unwrap_or(std::time::UNIX_EPOCH), m.len()));
                }
            }
        }
        out.sort();
        out
    }

    /// Край потолка смещения: `u32::MAX` ещё влезает, на байт больше — уже нет.
    ///
    /// Край здесь несущий с обеих сторон. Сдвинь границу на единицу вниз — и
    /// проверка объявит поломкой сегмент, который писатели считают законным и
    /// который открывается. Сдвинь вверх — и она промолчит о завёрнутом
    /// смещении, то есть о чужом документе в выдаче. Порог заметки проверяется
    /// вместе с ним: без него «вот-вот упрётся» и «уже упёрлось» слились бы в
    /// одну ступень, и предупредить оператора было бы нечем.
    #[test]
    fn the_offset_ceiling_is_inclusive_of_u32_max() {
        assert_eq!(reach(0), Reach::Fits, "пустой файл влезает");
        assert_eq!(reach(NEAR_CEILING - 1), Reach::Fits, "до порога заметки — молчание");
        assert_eq!(reach(NEAR_CEILING), Reach::Near, "порог заметки включающий");
        assert_eq!(
            reach(crate::dict::OFFSET_CEILING),
            Reach::Near,
            "сам u32::MAX — законное смещение, поломкой быть не может"
        );
        assert_eq!(
            reach(crate::dict::OFFSET_CEILING + 1),
            Reach::Over,
            "на байт за потолком смещение уже не выражается"
        );
        assert_eq!(reach(u64::MAX), Reach::Over);
        // Порог заметки обязан лежать **ниже** потолка: иначе заметки не бывает
        // вовсе, а предупреждать после заворота уже поздно. Проверяется через
        // сам `reach`, а не сравнением постоянных: сравнение постоянных
        // вычислилось бы при сборке и ничего бы не проверяло.
        assert_eq!(reach(crate::dict::OFFSET_CEILING - 1), Reach::Near, "заметка обязана быть");
    }

    /// Индекс с массивом: у такого есть все пять файлов со смещениями сразу.
    fn corpus_with_arrays(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-check-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::typed(
            &d,
            vec!["text".into(), "emoji".into()],
            vec![1.0, 1.0],
            vec![crate::index::Kind::Text, crate::index::Kind::Keyword],
            10_000,
        );
        ix.set_groups(vec![0, 1], vec![String::new(), "reactions".into()]);
        for i in 0..300u32 {
            // Арность гуляет, иначе полоса выродится в `identity` и файла
            // `g1.arity` не будет вовсе.
            let k = i % 3;
            let mut cells =
                vec![crate::indexer::Cell { field: 0, slot: 0, text: "документ тело" }];
            for s in 0..k {
                cells.push(crate::indexer::Cell {
                    field: 1,
                    slot: s,
                    text: if s == 0 { "up" } else { "fire" },
                });
            }
            ix.add_cells(&format!("d{i:04}"), &[1, k], &cells);
        }
        ix.flush();
        ix.drain();
        d
    }

    /// Меняет размер файла, не трогая содержимое. Отдаёт прежний размер.
    ///
    /// Раздувается **дырой**: на APFS и ext4 `ftruncate` вверх не занимает ни
    /// одного блока, а размер, который смотрит проверка, настоящий. Иначе
    /// проверить порог в четыре гигабайта было бы нечем.
    fn resize(path: &str, to: u64) -> u64 {
        let was = std::fs::metadata(path).unwrap_or_else(|e| panic!("{path}: {e}")).len();
        let f = std::fs::OpenOptions::new().write(true).open(path).expect("файл открыт");
        f.set_len(to).expect("размер поставлен");
        was
    }

    /// Файл за четырьмя гигабайтами — поломка у каждого из пяти носителей смещений.
    ///
    /// ## Что ломалось бы без этой проверки
    ///
    /// Смещения внутри сегмента тридцатидвухбитные все до одного: засечка в
    /// `<поле>.smrk`, опоры в `dict.idx`, `ids.idx`, `terms.idx`, указатель в
    /// `g*.aidx`. Пока `check` этого не смотрел, сегмент с завёрнутым смещением
    /// проходил проверку целиком: размеры сходятся, кратность соблюдена, суммы
    /// совпадают — а `get` отдаёт чужой документ, `_id` принадлежит соседу и
    /// ординалы группы съехали. Ровно тот случай, когда молчание хуже отказа.
    ///
    /// Проверяется на настоящем индексе, а не на выдуманном отчёте: файл
    /// раздувается дырой до `u32::MAX + 1`, и `check` обязан назвать его сам.
    ///
    /// ## Почему именно здесь сегмент раскладывается
    ///
    /// Это единственная проверка, которой нужен **рассыпанный** сегмент, и
    /// нужен он по существу: четыре гигабайта делаются дырой через `set_len`, а
    /// дыру можно проделать только в файле — у куска контейнера длина записана в
    /// оглавлении, и раздуть его значило бы записать четыре гигабайта. Россыпь
    /// при этом не поддавки: сегменты, собранные до контейнера, остаются
    /// рассыпанными навсегда, и потолок смещений у них тот же. Тот же порог
    /// внутри контейнера меряется отдельно — см.
    /// `packed_segment::the_offset_ceiling_is_measured_inside_the_container_too`.
    #[test]
    fn a_file_past_four_gigabytes_is_broken_for_every_offset_bearing_file() {
        let d = corpus_with_arrays("ceiling");
        let seg = crate::manifest::segments(&d).first().cloned().expect("сегмент есть");
        assert!(
            crate::container::unpack(&seg).expect("разбор контейнера"),
            "сегмент не был упакован — приём перестал писать контейнер"
        );
        let r = check(&d, Depth::Fast);
        assert!(r.ok(), "исходный индекс обязан быть цел: {:?}", texts(&r));
        // Ни одной находки, кроме оговорки о глубине: полосы арности — файлы
        // формата, а не «остаток прерванной записи», и заметка о них была
        // ложной на всяком индексе с массивом.
        let real: Vec<&Finding> = r.findings.iter().filter(|f| f.check != "глубина").collect();
        assert!(real.is_empty(), "здоровый индекс с массивом молчать обязан: {:?}", texts(&r));

        for file in ["text.sbin", "dict.blob", "ids.bin", "terms.bin", "g1.arity"] {
            let path = format!("{seg}/{file}");
            let was = resize(&path, crate::dict::OFFSET_CEILING + 1);

            let r = check(&d, Depth::Fast);
            let hit: Vec<&Finding> = r
                .findings
                .iter()
                .filter(|f| f.check == "смещение за потолком" && f.scope.ends_with(file))
                .collect();
            assert_eq!(hit.len(), 1, "{file}: обрыв не назван: {:?}", texts(&r));
            assert_eq!(hit[0].level, Level::Broken, "{file}: обрыв обязан быть поломкой");
            assert!(!r.ok(), "{file}: поиск по такому сегменту пускать нельзя");
            assert!(
                hit[0].text.contains("4294967296") && hit[0].text.contains("4294967295"),
                "{file}: отказ не назвал ни размер, ни потолок: {}",
                hit[0].text
            );

            // Ровно на потолке — ещё не поломка: такое смещение представимо, и
            // писатель такой сегмент считает законным.
            resize(&path, crate::dict::OFFSET_CEILING);
            let r = check(&d, Depth::Fast);
            assert!(
                !r.findings.iter().any(|f| f.check == "смещение за потолком"),
                "{file}: сам u32::MAX объявлен поломкой: {:?}",
                texts(&r)
            );
            assert!(
                r.findings.iter().any(|f| f.check == "порог смещения" && f.scope.ends_with(file)),
                "{file}: подход к потолку прошёл молча: {:?}",
                texts(&r)
            );

            // Вернуть как было: следующий файл меряется на здоровом индексе.
            resize(&path, was);
        }

        let r = check(&d, Depth::Fast);
        assert!(r.ok(), "после возврата размеров индекс обязан быть цел: {:?}", texts(&r));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Число слотов упирается в потолок **отдельно** от размера полосы.
    ///
    /// ## Что пропускала сверка размеров
    ///
    /// В `g*.aidx` рядом со смещением блока лежит его первый ординал — такое же
    /// тридцатидвухбитное число. Сверка размеров ловит только первое. Второе
    /// растёт с числом слотов, а полоса упакована битами и остаётся мелкой:
    /// четыре миллиарда слотов при арности до трёх — это меньше гигабайта
    /// `g*.arity`, то есть меньше четверти пути до потолка по размеру. Проверка
    /// по размеру в этой точке молчит, а ординал уже завернулся, и значения
    /// вложенного поля приписаны чужим элементам массива.
    ///
    /// Поэтому здесь число слотов подставляется в заголовок, а полоса на диске
    /// остаётся той же самой — в сотни байт. Пойди новая проверка через размер,
    /// она бы промолчала на всех трёх значениях; размер и проверяется отдельным
    /// утверждением, чтобы это было видно, а не подразумевалось.
    #[test]
    fn a_group_with_too_many_slots_is_named_though_its_band_stays_small() {
        let d = corpus_with_arrays("slots");
        let seg = crate::manifest::segments(&d).first().cloned().expect("сегмент есть");
        let mut head = crate::index::read_head(&seg).expect("заголовок прочитан");
        let at = head
            .groups
            .iter()
            .position(|g| g.kind == crate::slots::Kind::Dense)
            .expect("плотная группа обязана быть, иначе проверять нечего");
        let gid = head.groups[at].id;

        // Полоса — сотни байт: по размеру ей до потолка бесконечно далеко, и
        // сверка размеров обязана молчать при любом числе слотов в заголовке.
        let band = super::packing::len_of(&seg, &format!("g{gid}.arity"));
        assert!(band < NEAR_CEILING, "полоса великовата, проверка потеряет смысл: {band} байт");

        // Источник — настоящий, упакованный: сверка потолка обязана мерить
        // куски контейнера, а не имена файлов в каталоге, которых у него нет.
        let src = std::sync::Arc::new(
            crate::container::SegmentSource::of(&seg, head.container).expect("контейнер"),
        );
        assert!(src.is_packed(), "сегмент рассыпан — мерилось бы не то");
        let run = |head: &crate::index::Head| {
            let mut s = Sink::new();
            ceilings(&src, head, &mut s);
            s.out
        };
        // `Finding` не печатается сам, а находка без текста в отчёте проверки
        // не объясняет ничего.
        let say = |f: &[Finding]| -> Vec<String> {
            f.iter()
                .map(|f| format!("{} {} {}: {}", f.level.name(), f.scope, f.check, f.text))
                .collect()
        };

        let real = head.groups[at].units;
        assert!(run(&head).is_empty(), "здоровая группа обязана молчать: {:?}", say(&run(&head)));

        for (units, want, level) in [
            (NEAR_CEILING, "порог числа слотов", Level::Note),
            (crate::dict::OFFSET_CEILING, "порог числа слотов", Level::Note),
            (crate::dict::OFFSET_CEILING + 1, "число слотов за потолком", Level::Broken),
        ] {
            head.groups[at].units = units as usize;
            let found = run(&head);
            assert_eq!(found.len(), 1, "{units} слотов: находок не одна: {:?}", say(&found));
            assert_eq!(found[0].check, want, "{units} слотов: не та ступень");
            assert_eq!(found[0].level, level, "{units} слотов: не тот уровень");
            assert!(
                found[0].scope.ends_with(&format!("g{gid}.aidx")),
                "{units} слотов: находка приписана не указателю: {}",
                found[0].scope
            );
            assert!(
                found[0].text.contains(&units.to_string()),
                "{units} слотов: отказ не назвал достигнутое: {}",
                found[0].text
            );
        }

        // Край снизу: на единицу ниже порога заметки — снова молчание. Без
        // этого проверка прошла бы и у той, что тревожится на всяком числе.
        head.groups[at].units = (NEAR_CEILING - 1) as usize;
        assert!(run(&head).is_empty(), "заметка сработала до порога: {:?}", say(&run(&head)));

        // Настоящее число слотов вернуть — на случай, если проверка вырастет.
        head.groups[at].units = real;
        assert!(run(&head).is_empty());
        std::fs::remove_dir_all(&d).ok();
    }
}

/// Таблица словарей и суммы распакованных кусков: две новые поверхности порчи.
#[cfg(test)]
mod store_files {
    use super::*;

    /// Односегментный индекс с известным числом документов.
    fn one_segment(name: &str, docs: u32) -> (String, String) {
        let d = std::env::temp_dir()
            .join(format!("svindb-check-store-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(
            &d,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            100_000,
        );
        for i in 0..docs {
            ix.add(&format!("d{i:05}"), &[&format!("загол{}", i % 13), &format!("тело {i} слово")]);
        }
        ix.flush();
        let seg = crate::manifest::segments(&d).first().cloned().expect("сегмент");
        // Сегмент остаётся упакованным: куски хранилища правятся пересборкой
        // контейнера. Разбор — у [`super::packing`].
        assert!(
            std::path::Path::new(&format!("{seg}/{}", crate::container::FILE)).exists(),
            "приём собрал сегмент россыпью — проверять тут нечего"
        );
        (d, seg)
    }

    fn names(r: &Report) -> Vec<&'static str> {
        r.findings.iter().map(|f| f.check).collect()
    }

    /// Свежий сегмент несёт суммы кусков и проходит обе глубины молча.
    ///
    /// Заодно это и проверка того, что новый файл не читается как «лишний»: одна
    /// ложная заметка на здоровом индексе обесценивает все остальные — их
    /// перестают читать.
    #[test]
    fn a_fresh_segment_carries_chunk_sums_and_says_nothing() {
        let (d, seg) = one_segment("fresh", 400);
        let scrc = super::packing::len_of(&seg, "body.scrc");
        let smrk = super::packing::len_of(&seg, "body.smrk");
        assert!(smrk > 0, "засечек нет — проверять нечего");
        assert_eq!(scrc, smrk / crate::store::MARK as u64 * 4, "сумм не по куску");
        for depth in [Depth::Fast, Depth::Full] {
            let r = check(&d, depth);
            let real: Vec<&Finding> = r.findings.iter().filter(|f| f.check != "глубина").collect();
            assert!(real.is_empty(), "находки на целом сегменте: {:?}", names(&r));
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Хранилище, пропавшее целиком, не проходит молча — и пустое поле не
    /// поднимает ложной тревоги.
    ///
    /// ## Что молчало
    ///
    /// Ни одна проверка не смотрела на **полное** отсутствие хранилища у поля,
    /// объявленного хранимым. Охрана открытия (`index::load`) отвергает сегмент,
    /// только когда все три куска на месте, а хранилище не открылось; опись
    /// состава считает поломкой неполную тройку, а пустая тройка для неё не
    /// поломка; сводка сумм перечисляет то, что нашла в оглавлении, и строка про
    /// пропавший кусок исчезает вместе с ним; `stores` уходил в `continue`. Так
    /// выглядит хранилище, потерянное укрупнением: `fetch` отдаёт пустую строку
    /// на каждый документ, а обе проверки говорят «поломок 0».
    ///
    /// ## Почему второй половины теста мало не бывает
    ///
    /// Отсутствие хранилища бывает и законным: полю, у которого во всём сегменте
    /// нет содержимого, хранилище не нужно, и приём вправе его не заводить.
    /// Тревога, срабатывающая на исправном индексе, обесценивает и настоящие.
    /// Различает два случая полоса длин, и вторая половина проверяет ровно её:
    /// над сегментом, у которого хранимое поле пусто у **всех** документов,
    /// проделывается то же самое — и молчание обязано быть.
    #[test]
    fn a_stored_field_that_lost_its_whole_store_is_named() {
        let (d, seg) = one_segment("nostore", 400);
        assert!(check(&d, Depth::Full).ok(), "исходный сегмент обязан быть цел");

        let gone = ["body.sbin", "body.smrk", "body.sdic", "body.scrc"];
        super::packing::repack(&seg, |n, b| (!gone.contains(&n)).then_some(b), &[]);
        let r = check(&d, Depth::Full);
        assert!(!r.ok(), "хранилище пропало целиком, а сегмент объявлен целым: {:?}", names(&r));
        let f = r
            .findings
            .iter()
            .find(|f| f.check == "нет хранилища")
            .unwrap_or_else(|| panic!("пропажа хранилища не названа: {:?}", names(&r)));
        assert_eq!(f.level, Level::Broken);
        assert!(f.text.contains("body"), "поле не названо: {}", f.text);
        assert!(f.scope.ends_with("body.sbin"), "находка приписана не хранилищу: {}", f.scope);
        std::fs::remove_dir_all(&d).ok();

        // Поле, которому нечего было хранить: приём не завёл ему хранилища сам,
        // и говорить тут не о чем.
        let e = std::env::temp_dir()
            .join(format!("svindb-check-store-empty-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&e).ok();
        let mut ix = crate::indexer::Indexer::new(
            &e,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            100_000,
        );
        for i in 0..300u32 {
            ix.add(&format!("d{i:05}"), &[&format!("загол{}", i % 13), ""]);
        }
        ix.flush();
        let empty = crate::manifest::segments(&e).first().cloned().expect("сегмент");
        super::packing::repack(&empty, |n, b| (!gone.contains(&n)).then_some(b), &[]);
        let r = check(&e, Depth::Full);
        assert!(
            !names(&r).contains(&"нет хранилища"),
            "полю без содержимого приписана потеря хранилища: {:?}",
            names(&r)
        );
        assert!(r.ok(), "сегмент с пустым полем объявлен битым: {:?}", names(&r));
        std::fs::remove_dir_all(&e).ok();
    }

    /// Обрезанные суммы — поломка, а не «сумм просто нет».
    ///
    /// Короткий файл читался бы нулями, то есть объявлял бы сошедшимся то, чего
    /// не сверял: проверка распаковки молча перестала бы что-либо доказывать.
    #[test]
    fn short_chunk_sums_are_a_break_not_an_absence() {
        let (d, seg) = one_segment("shortcrc", 4000);
        let was = super::packing::len_of(&seg, "body.scrc");
        assert!(was > 8, "сумм всего {was} байт — резать нечего");
        super::packing::cut(&seg, "body.scrc", 4);
        let r = check(&d, Depth::Fast);
        assert!(!r.ok(), "обрезанные суммы прошли как целые");
        assert!(names(&r).contains(&"число сумм не то"), "не та находка: {:?}", names(&r));

        // Сумм нет вовсе — это не поломка: так собраны все прежние сегменты.
        super::packing::drop_piece(&seg, "body.scrc");
        let r = check(&d, Depth::Full);
        let real: Vec<&Finding> = r.findings.iter().filter(|f| f.check != "глубина").collect();
        assert!(real.is_empty(), "отсутствие сумм объявлено поломкой: {:?}", names(&r));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Таблица словарей без словарей и таблица не по раскладке — поломки.
    #[test]
    fn a_dictionary_table_must_match_the_layout_and_the_files() {
        let (d, seg) = one_segment("sdix", 300);
        // Таблица берётся настоящая: один словарь на все куски. Годной она тут
        // быть и должна — проверяется её **уместность**, а не строение.
        let blocks = super::packing::len_of(&seg, "body.smrk") / crate::store::MARK as u64;
        let sdic = super::packing::len_of(&seg, "body.sdic");
        let table = crate::store::build_sdix(
            &[crate::store::DictSpan { at: 0, len: sdic as u32, first: 0 }],
            blocks as u32,
        );
        super::packing::put(&seg, "body.sdix", &table);
        let r = check(&d, Depth::Full);
        let real: Vec<&Finding> = r.findings.iter().filter(|f| f.check != "глубина").collect();
        assert!(real.is_empty(), "годная таблица объявлена поломкой: {:?}", names(&r));

        // Раскладка ниже девятой таблицы не знает: читатель той раскладки взял бы
        // из `.sdic` первую запись и распаковывал бы ею чужие куски.
        let head = format!("{seg}/head.tsv");
        let text = std::fs::read_to_string(&head).expect("заголовок");
        let current = format!("layout\t{}\n", crate::index::LAYOUT);
        assert!(text.contains(&current), "заголовок пишет не текущую раскладку: {text}");
        std::fs::write(&head, text.replace(&current, "layout\t8\n")).expect("запись");
        crate::sums::write(&seg).expect("сводка");
        let r = check(&d, Depth::Fast);
        assert!(!r.ok(), "таблица при восьмой раскладке принята");
        assert!(
            names(&r).contains(&"таблица словарей не по раскладке"),
            "не та находка: {:?}",
            names(&r)
        );
        std::fs::write(&head, text).expect("возврат");
        crate::sums::write(&seg).expect("сводка");

        // Таблица есть, а словаря нет — хранилища больше нет, и молчать нельзя.
        super::packing::drop_piece(&seg, "body.sdic");
        let r = check(&d, Depth::Fast);
        assert!(!r.ok(), "таблица без словаря принята");
        assert!(
            names(&r).contains(&"таблица словарей без словарей"),
            "не та находка: {:?}",
            names(&r)
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Гнутая таблица не проходит молча: хранилище не открылось — значит поле
    /// вернуло бы пустоту, и об этом обязано быть сказано.
    #[test]
    fn a_bent_table_is_named_not_skipped() {
        let (d, seg) = one_segment("bentsdix", 300);
        // Таблица обещает два словаря там, где записей на них нет.
        let sdic = super::packing::len_of(&seg, "body.sdic") as u32;
        let table = crate::store::build_sdix(
            &[
                crate::store::DictSpan { at: 0, len: sdic / 2, first: 0 },
                crate::store::DictSpan { at: sdic / 2, len: sdic - sdic / 2, first: 1 },
            ],
            (super::packing::len_of(&seg, "body.smrk") / crate::store::MARK as u64) as u32,
        );
        super::packing::put(&seg, "body.sdix", &table);
        let r = check(&d, Depth::Full);
        assert!(!r.ok(), "гнутая таблица прошла молча");
        assert!(names(&r).contains(&"хранилище не открывается"), "не та находка: {:?}", names(&r));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Подменённый байт в `.sbin`, который **распаковался**, ловится только
    /// суммой распакованного — и это показано снятием сумм.
    ///
    /// ## Что здесь ловится
    ///
    /// `sums.tsv` доказывает, что байты `.sbin` те же, какими легли. Она ничего
    /// не говорит о том, что они распаковываются в то же самое. Разница станет
    /// несущей у укрупнения: оно будет переносить куски между сегментами, и
    /// сводка у нового сегмента своя, посчитанная уже по перенесённому.
    ///
    /// Поэтому сводка здесь **пересчитывается** после порчи: она сходится, все
    /// проверки строения сходятся, число значений в куске сходится. Не сходится
    /// только содержимое — и второй половиной теста показано, что без `.scrc`
    /// проверка объявляет такой сегмент здоровым.
    #[test]
    fn a_flipped_byte_that_still_unpacks_is_caught_only_by_the_chunk_sums() {
        let (d, seg) = one_segment("unpack", 371);
        // Хранимое поле пересобирается на шуме: у сжатого куска перевёрнутый байт
        // почти всегда рвёт поток, и меряться стал бы отказ распаковщика. Шум
        // ложится в файл как есть, и порча в нём переживает распаковку.
        let mut seed = 0x51ED_270B_9F1A_C3D7u64;
        let mut vals: Vec<Vec<u8>> = Vec::new();
        // Кусок закрывается на шестнадцати килобайтах, то есть на семьдесят
        // четвёртом значении по двести двадцать четыре байта: ровно пять кусков,
        // а хвостовым остаётся одно короткое значение. Выборка под словарь при
        // этом накрывает лишь начало колонки, поэтому хвост ей незнаком и сжатию
        // не поддаётся вовсе.
        for i in 0..371usize {
            let len = if i < 370 { 220 } else { 64 };
            vals.push(
                (0..len)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        seed as u8
                    })
                    .collect(),
            );
        }
        let built = crate::store::build_with(
            |e: &mut dyn FnMut(&[u8])| vals.iter().for_each(|v| e(v)),
            crate::codec::Effort::Fast,
            1,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано");
        super::packing::repack(
            &seg,
            |_, b| Some(b),
            &[
                ("body.sbin".to_string(), built.data.clone()),
                ("body.smrk".to_string(), built.marks.clone()),
                ("body.sdic".to_string(), built.dict.clone()),
                ("body.scrc".to_string(), built.crcs.clone()),
            ],
        );
        assert!(check(&d, Depth::Full).ok(), "пересобранное хранилище обязано быть целым");

        // Ищем кусок, легший как есть: первый байт тела — признак «без сжатия».
        let total = crate::index::read_head(&seg).expect("заголовок").container;
        let src = std::sync::Arc::new(
            crate::container::SegmentSource::of(&seg, total).expect("контейнер"),
        );
        let store = crate::store::Store::open(&src, "body").expect("открылось");
        let marks = built.marks.clone();
        let at = |i: usize, o: usize| {
            u32::from_le_bytes(
                marks[i * crate::store::MARK + o..i * crate::store::MARK + o + 4]
                    .try_into()
                    .expect("четыре байта"),
            ) as usize
        };
        let victim = (0..store.blocks())
            .find(|i| built.data[at(*i, 0)] == 0)
            .expect("шум обязан лечь хотя бы одним куском без сжатия");
        drop(store);
        drop(src);

        let mut data = built.data.clone();
        let off = at(victim, 0) + at(victim, 4) / 2;
        data[off] ^= 0x20;
        // Пересборка заново считает и сводку, и сумму куска в оглавлении: пусть
        // сойдутся обе, как сойдутся у куска, перенесённого укрупнением в новый
        // сегмент. Ловить обязана именно сумма распакованного.
        super::packing::put(&seg, "body.sbin", &data);
        let r = check(&d, Depth::Full);
        assert!(!r.ok(), "подмена, пережившая распаковку, прошла молча");
        assert!(
            names(&r).contains(&"кусок распаковался не в то"),
            "не та находка: {:?}",
            names(&r)
        );

        // А вот и доказательство, что ловит именно она: снимаем суммы — и та же
        // порча объявляется здоровым сегментом.
        super::packing::drop_piece(&seg, "body.scrc");
        let r = check(&d, Depth::Full);
        assert!(
            r.ok(),
            "без сумм подмену поймало что-то ещё — проверка меряет не то: {:?}",
            names(&r)
        );
        std::fs::remove_dir_all(&d).ok();
    }
}

/// Сегмент-контейнер: то, чего проверка не видела, пока сегмент был россыпью.
///
/// Разница не в оформлении. У рассыпанного сегмента опись состава — это обход
/// каталога, порча куска — это порча файла, а сводка сумм накрывает каждый кусок
/// по отдельности. У упакованного всё это одно и то же: четыре файла в каталоге,
/// один из них на гигабайты. Проверка, которой не сказали про контейнер, на таком
/// сегменте не находит ничего — и не потому, что он цел.
#[cfg(test)]
mod packed_segment {
    use super::*;
    use crate::container::{Table, ENTRY, FILE, NAME};

    /// Односегментный **упакованный** индекс. Раскладывать его здесь нельзя:
    /// проверяется ровно то, что он упакован.
    fn packed(name: &str, docs: u32) -> (String, String) {
        let d = std::env::temp_dir()
            .join(format!("svindb-check-packed-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(
            &d,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            100_000,
        );
        for i in 0..docs {
            ix.add(
                &format!("d{i:05}"),
                &[&format!("загол{}", i % 13), &format!("тело {i} общее слово{}", i % 7)],
            );
        }
        ix.flush();
        let seg = crate::manifest::segments(&d).first().cloned().expect("сегмент");
        assert!(
            std::path::Path::new(&format!("{seg}/{FILE}")).exists(),
            "приём собрал сегмент россыпью — проверять тут нечего"
        );
        (d, seg)
    }

    fn texts(r: &Report) -> Vec<String> {
        r.findings
            .iter()
            .map(|f| format!("{} {} {}: {}", f.level.name(), f.scope, f.check, f.text))
            .collect()
    }

    /// Находки, кроме оговорки о глубине: она про инструмент, а не про данные.
    fn real(r: &Report) -> Vec<&Finding> {
        r.findings.iter().filter(|f| f.check != "глубина").collect()
    }

    /// Правит оглавление на месте и пересчитывает его сумму — сразу в заголовке
    /// и в концевике.
    ///
    /// Пересчёт обязателен: без него разбор отказал бы по сумме оглавления, и
    /// проверялась бы совсем не та охрана, ради которой подделка затевалась.
    fn forge(whole: &[u8], edit: impl Fn(&mut [u8], u64)) -> Vec<u8> {
        let mut b = whole.to_vec();
        let off = u64::from_le_bytes(b[24..32].try_into().expect("восемь")) as usize;
        let len = u64::from_le_bytes(b[32..40].try_into().expect("восемь")) as usize;
        edit(&mut b[off..off + len], off as u64);
        let crc = crate::wal::crc32(&b[off..off + len]);
        b[48..52].copy_from_slice(&crc.to_le_bytes());
        let n = b.len();
        b[n - 16..n - 12].copy_from_slice(&crc.to_le_bytes());
        assert_ne!(b, whole, "подделка ничего не изменила");
        b
    }

    /// Кладёт в поле записи оглавления восьмибайтовое число.
    fn put_u64(table: &mut [u8], entry: usize, at: usize, v: u64) {
        let p = entry * ENTRY + NAME + at;
        table[p..p + 8].copy_from_slice(&v.to_le_bytes());
    }

    /// Упакованный сегмент цел и молчит — по обеим глубинам.
    ///
    /// ## Зачем это утверждение
    ///
    /// Молчание тут дороже любой находки. Опись состава строится по оглавлению
    /// контейнера, и стоит ей сбиться — заговорит она сразу обо всём: либо
    /// каждый кусок объявит «лишним файлом», либо каждый обязательный —
    /// пропавшим. Отчёт на полсотни ложных строк равен отсутствию отчёта, его
    /// перестают читать.
    ///
    /// Отдельно сверяется, что в каталоге и правда лежат четыре файла, а кусков
    /// внутри — десятки: иначе тест прошёл бы и на рассыпанном сегменте, ничего
    /// про контейнер не доказав.
    #[test]
    fn a_packed_segment_is_whole_and_says_nothing() {
        let (d, seg) = packed("silent", 400);
        let table = Table::read(&format!("{seg}/{FILE}")).expect("оглавление");
        assert!(table.members().len() > 12, "кусков всего {}", table.members().len());

        let mut on_disk: Vec<String> = std::fs::read_dir(&seg)
            .expect("каталог сегмента")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        on_disk.sort();
        assert_eq!(
            on_disk,
            vec!["head.tsv", "live.bin", FILE, "sums.tsv"],
            "в каталоге упакованного сегмента лежит что-то ещё"
        );

        for depth in [Depth::Fast, Depth::Full] {
            let r = check(&d, depth);
            assert!(r.ok(), "поломки на целом контейнере: {:?}", texts(&r));
            assert!(real(&r).is_empty(), "заметки на целом контейнере: {:?}", texts(&r));
            assert_eq!(r.segments, 1);
            assert_eq!(r.docs, 400);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Кусок с незнакомым именем внутри контейнера — такая же заметка, как
    /// лишний файл рядом с сегментом.
    ///
    /// Опись обязана строиться по оглавлению: на диске лежат четыре файла, и
    /// обход каталога о содержимом контейнера не знает ничего. Кусок, которого
    /// формат не знает, — это остаток прерванной записи либо чужая раскладка, и
    /// молчать о нём нельзя.
    ///
    /// Заодно проверяется вторая половина того же: пропажа известного куска
    /// видна по имени, а не по «сегмент не открылся».
    #[test]
    fn an_unknown_piece_inside_the_container_is_named() {
        let (d, seg) = packed("extra", 300);
        let path = format!("{seg}/{FILE}");
        let whole = std::fs::read(&path).expect("контейнер");
        let table = Table::read(&path).expect("оглавление");
        let at = table
            .members()
            .iter()
            .position(|m| m.name == "norms.bin")
            .expect("нормы обязаны быть куском");

        // Имя меняется на незнакомое, но остающееся на своём месте в порядке:
        // «norms.zzz» стоит там же, где стояло «norms.bin», и разбор оглавления
        // не отвлекается на порядок.
        let bent = forge(&whole, |t, _| {
            let p = at * ENTRY;
            t[p..p + NAME].fill(0);
            t[p..p + 9].copy_from_slice(b"norms.zzz");
        });
        std::fs::write(&path, &bent).expect("запись");
        crate::sums::write(&seg).expect("сводка");

        let r = check(&d, Depth::Fast);
        assert!(
            r.findings.iter().any(|f| f.check == "лишний файл" && f.text == "norms.zzz"),
            "кусок с незнакомым именем не назван: {:?}",
            texts(&r)
        );
        assert!(!r.ok(), "нормы пропали, а сегмент объявлен целым");
        assert!(
            r.findings.iter().any(|f| f.check == "нет файла" && f.scope.ends_with("norms.bin")),
            "пропажа известного куска не названа по имени: {:?}",
            texts(&r)
        );

        std::fs::write(&path, &whole).expect("возврат");
        crate::sums::write(&seg).expect("сводка");
        assert!(check(&d, Depth::Full).ok(), "возвращённый контейнер всё ещё битый");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Перевёрнутый бит внутри контейнера обязан назвать **кусок**, а не файл.
    ///
    /// ## Что здесь ловится
    ///
    /// Сводка сумм после порчи **пересчитывается** — намеренно и в этом весь
    /// смысл проверки. Пересчёт сводки это не выдумка теста: её пишут заново и
    /// приём, и слияние, и разбор контейнера, каждый раз по тому, что лежит на
    /// диске сейчас. Пересчитанная сводка сходится сама с собой и подтверждает
    /// порчу как законное содержимое.
    ///
    /// Остаётся сумма из оглавления: её посчитал писатель, когда кусок ложился
    /// в файл, и пересчитать её, не переписав весь контейнер, нечем. Она одна и
    /// обязана назвать испорченный кусок по имени — без имени отчёт на
    /// гигабайтном `seg.dat` не указывает никуда.
    ///
    /// Проверяется каждый непустой кусок, а не один: смысл суммы в том, что она
    /// не разбирается в содержимом и потому одинаково видит подмену везде.
    #[test]
    fn a_flipped_byte_inside_the_container_names_the_piece() {
        let (d, seg) = packed("crc", 300);
        let path = format!("{seg}/{FILE}");
        let whole = std::fs::read(&path).expect("контейнер");
        let victims: Vec<(String, u64)> = Table::read(&path)
            .expect("оглавление")
            .members()
            .iter()
            .filter(|m| m.len > 0)
            .map(|m| (m.name.clone(), m.off + m.len / 2))
            .collect();
        assert!(victims.len() > 10, "непустых кусков всего {}", victims.len());

        for (name, at) in victims {
            let mut bent = whole.clone();
            bent[at as usize] ^= 1;
            std::fs::write(&path, &bent).expect("запись");
            crate::sums::write(&seg).expect("сводка");

            let r = check(&d, Depth::Full);
            assert!(!r.ok(), "порча в куске {name} прошла как целый сегмент: {:?}", texts(&r));
            let hit: Vec<&Finding> =
                r.findings.iter().filter(|f| f.check == "содержимое куска не то").collect();
            assert_eq!(hit.len(), 1, "порча в куске {name}: находок не одна: {:?}", texts(&r));
            assert!(
                hit[0].scope.ends_with(&name),
                "находка приписана не тому куску: {} вместо {name}",
                hit[0].scope
            );
            assert_eq!(hit[0].level, Level::Broken);

            std::fs::write(&path, &whole).expect("возврат");
            crate::sums::write(&seg).expect("сводка");
            assert!(
                check(&d, Depth::Full).ok(),
                "возвращённый кусок {name} всё ещё считается битым"
            );
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Кладёт в каталог контейнер с одним куском объявленной длины.
    ///
    /// Кусок — **дыра**: `set_len` вверх не занимает на APFS и ext4 ни одного
    /// блока, а длина, которую видит проверка, настоящая. Собрать кусок в четыре
    /// гигабайта записью значило бы записать четыре гигабайта.
    ///
    /// Контейнер здесь пишется руками, а не [`crate::container::Writer`], ровно
    /// по этой причине: писателю кусок надо отдать байтами. Всё остальное —
    /// опознание, согласие заголовка с концевиком, сумма оглавления — настоящее,
    /// иначе разбор отверг бы файл раньше, чем дело дошло бы до потолка.
    fn hollow(dir: &str, piece: &str, len: u64) -> u64 {
        use crate::container::{ALIGN, ENTRY, FORMAT, HEAD, MAGIC, TAIL, TAIL_MAGIC};
        use std::io::{Seek, Write};

        let off = HEAD as u64;
        let dir_off = (off + len).div_ceil(ALIGN) * ALIGN;
        let dir_len = ENTRY as u64;
        let total = dir_off + dir_len + TAIL as u64;

        let mut entry = vec![0u8; ENTRY];
        entry[..piece.len()].copy_from_slice(piece.as_bytes());
        entry[NAME..NAME + 8].copy_from_slice(&off.to_le_bytes());
        entry[NAME + 8..NAME + 16].copy_from_slice(&len.to_le_bytes());
        let crc_dir = crate::wal::crc32(&entry);

        let mut head = vec![0u8; HEAD];
        head[..8].copy_from_slice(&MAGIC);
        head[8..12].copy_from_slice(&FORMAT.to_le_bytes());
        head[12..16].copy_from_slice(&9u32.to_le_bytes());
        head[16..20].copy_from_slice(&1u32.to_le_bytes());
        head[24..32].copy_from_slice(&dir_off.to_le_bytes());
        head[32..40].copy_from_slice(&dir_len.to_le_bytes());
        head[40..48].copy_from_slice(&total.to_le_bytes());
        head[48..52].copy_from_slice(&crc_dir.to_le_bytes());

        let mut tail = vec![0u8; TAIL];
        tail[..8].copy_from_slice(&dir_off.to_le_bytes());
        tail[8..16].copy_from_slice(&dir_len.to_le_bytes());
        tail[16..20].copy_from_slice(&crc_dir.to_le_bytes());
        tail[20..28].copy_from_slice(&total.to_le_bytes());
        tail[28..32].copy_from_slice(&TAIL_MAGIC);

        let mut f = std::fs::File::create(format!("{dir}/{FILE}")).expect("контейнер");
        f.set_len(total).expect("длина");
        f.write_all(&head).expect("заголовок");
        f.seek(std::io::SeekFrom::Start(dir_off)).expect("к оглавлению");
        f.write_all(&entry).expect("оглавление");
        f.seek(std::io::SeekFrom::Start(total - TAIL as u64)).expect("к концевику");
        f.write_all(&tail).expect("концевик");
        total
    }

    /// Порог четырёх гигабайт меряется и у куска контейнера.
    ///
    /// ## Что молчало
    ///
    /// Смещения внутри сегмента тридцатидвухбитные все до одного, и упаковка
    /// этого не отменила: опора `dict.idx` показывает внутрь `dict.blob` четырьмя
    /// байтами, лежит `dict.blob` файлом рядом или куском внутри `seg.dat`.
    /// А сверка спрашивала **каталог** — где у упакованного сегмента таких имён
    /// нет вовсе. Ответ «такого файла нет» пропускал все шесть пар разом: проверка
    /// молчала не потому, что запас есть, а потому, что не нашла, что мерить.
    /// И молчала ровно на тех сегментах, которые до потолка и дорастают, —
    /// упакованных и крупных.
    ///
    /// Ступени проверяются все три, а не только поломка: сдвинь границу на
    /// единицу вниз — и проверка объявит поломкой законный сегмент, вверх — и
    /// промолчит о завёрнутом смещении, то есть о чужой записи в выдаче.
    #[test]
    fn the_offset_ceiling_is_measured_inside_the_container_too() {
        let (d, seg) = packed("ceiling", 200);
        let head = crate::index::read_head(&seg).expect("заголовок");
        let hole = std::env::temp_dir()
            .join(format!("svindb-check-hollow-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&hole).ok();
        std::fs::create_dir_all(&hole).expect("каталог");

        for (len, want) in [
            (NEAR_CEILING - 1, None),
            (NEAR_CEILING, Some(("порог смещения", Level::Note))),
            (crate::dict::OFFSET_CEILING, Some(("порог смещения", Level::Note))),
            (crate::dict::OFFSET_CEILING + 1, Some(("смещение за потолком", Level::Broken))),
        ] {
            let total = hollow(&hole, "dict.blob", len);
            let src = std::sync::Arc::new(
                crate::container::SegmentSource::of(&hole, Some(total)).expect("контейнер"),
            );
            assert!(src.is_packed(), "{len}: источник не упакован");
            assert_eq!(src.len_of("dict.blob"), Some(len), "{len}: длина куска не та");
            // Файла с таким именем в каталоге нет — иначе мерилось бы не то,
            // ради чего проверка переведена на источник.
            assert!(
                !std::path::Path::new(&format!("{hole}/dict.blob")).exists(),
                "{len}: кусок оказался файлом"
            );

            let mut s = Sink::new();
            ceilings(&src, &head, &mut s);
            let say: Vec<String> = s
                .out
                .iter()
                .map(|f| format!("{} {} {}: {}", f.level.name(), f.scope, f.check, f.text))
                .collect();
            match want {
                None => assert!(s.out.is_empty(), "{len}: тревога до порога: {say:?}"),
                Some((check, level)) => {
                    assert_eq!(s.out.len(), 1, "{len}: находок не одна: {say:?}");
                    assert_eq!(s.out[0].check, check, "{len}: не та ступень");
                    assert_eq!(s.out[0].level, level, "{len}: не тот уровень");
                    assert!(
                        s.out[0].scope.ends_with("dict.blob"),
                        "{len}: находка приписана не куску: {}",
                        s.out[0].scope
                    );
                    assert!(
                        s.out[0].text.contains(&len.to_string()),
                        "{len}: отказ не назвал достигнутое: {}",
                        s.out[0].text
                    );
                }
            }
        }

        std::fs::remove_dir_all(&hole).ok();
        std::fs::remove_dir_all(&d).ok();
    }

    /// Оглавление, которое врёт, обязано быть отвергнуто внятно — и до того,
    /// как из него прочтут хоть один кусок.
    ///
    /// ## Четыре подделки и почему они разные
    ///
    /// * **Хвост срезан.** Обрыв записи режет конец файла, и концевик пропадает
    ///   первым. Прочитать такой контейнер наполовину нельзя: смещения кусков
    ///   лежат в оглавлении, а оглавления больше нет.
    /// * **Концевика нет при прежней длине.** То же, но длина сходится с
    ///   обещанной заголовком сегмента — значит на длине проверка кончиться не
    ///   вправе.
    /// * **Смещение за пределы кусков.** Съехавшее смещение не отказывает: оно
    ///   отдаёт байты, которые там лежат, — правдоподобный мусор нужной длины.
    /// * **Нахлёст.** Два куска на одном месте: один из них читается под чужим
    ///   именем, и это опять не отказ, а подмена.
    ///
    /// Каждый раз проверяется, что отказ называет **причину**, а не отделывается
    /// словами «сегмент не открылся».
    #[test]
    fn a_container_that_lies_about_itself_is_refused_with_a_reason() {
        let (d, seg) = packed("frame", 300);
        let name = crate::manifest::name_of(&seg);
        let path = format!("{seg}/{FILE}");
        let whole = std::fs::read(&path).expect("контейнер");
        let table = Table::read(&path).expect("оглавление");
        // Два соседних по смещению непустых куска — их и столкнём лбами.
        let mut order: Vec<(u64, u64, usize)> = table
            .members()
            .iter()
            .enumerate()
            .filter(|(_, m)| m.len > 0)
            .map(|(i, m)| (m.off, m.len, i))
            .collect();
        order.sort();
        let (first_off, first_len, first_at) = order[0];
        let next_off = order[1].0;
        let victim = table.members()[first_at].name.clone();

        let say = |bent: &[u8], what: &str| -> Report {
            std::fs::write(&path, bent).expect("запись");
            let r = check(&d, Depth::Fast);
            assert!(!r.ok(), "{what}: контейнер принят");
            let f = r
                .findings
                .iter()
                .find(|f| f.check == "контейнер не открывается")
                .unwrap_or_else(|| panic!("{what}: отказ не про контейнер: {:?}", texts(&r)));
            assert_eq!(f.scope, name, "{what}: отказ приписан не сегменту");
            assert!(f.text.contains(FILE), "{what}: отказ не назвал файл: {}", f.text);
            r
        };

        // 1. Хвост срезан.
        let r = say(&whole[..whole.len() - 1], "срезанный хвост");
        assert!(
            texts(&r).iter().any(|t| t.contains("обещает")),
            "срезанный хвост: отказ не сослался на обещанную длину: {:?}",
            texts(&r)
        );

        // 2. Длина прежняя, опознание концевика стёрто.
        let mut bent = whole.clone();
        let n = bent.len();
        bent[n - 4..].fill(0);
        let r = say(&bent, "стёртый концевик");
        assert!(
            texts(&r).iter().any(|t| t.contains("концевик")),
            "стёртый концевик: отказ не назвал причину: {:?}",
            texts(&r)
        );

        // 3. Смещение первого куска уведено за область кусков.
        let bent = forge(&whole, |t, dir_off| put_u64(t, first_at, 0, dir_off));
        let r = say(&bent, "смещение за куски");
        assert!(
            texts(&r).iter().any(|t| t.contains(&victim)),
            "смещение за куски: отказ не назвал кусок {victim}: {:?}",
            texts(&r)
        );

        // 4. Первый кусок дотянут до следующего — нахлёст.
        assert!(next_off >= first_off + first_len, "куски и без подделки налезают");
        let bent = forge(&whole, |t, _| put_u64(t, first_at, 8, next_off - first_off + 1));
        let r = say(&bent, "нахлёст");
        assert!(
            texts(&r).iter().any(|t| t.contains("накладываются") && t.contains(&victim)),
            "нахлёст: отказ не назвал куски: {:?}",
            texts(&r)
        );

        std::fs::write(&path, &whole).expect("возврат");
        assert!(check(&d, Depth::Full).ok(), "возвращённый контейнер всё ещё битый");
        std::fs::remove_dir_all(&d).ok();
    }
}
