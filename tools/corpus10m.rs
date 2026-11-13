//! Детерминированный генератор корпусов для полной измерительной кампании.
//!
//! Собирается отдельно от workspace, чтобы генерация десятков гигабайт не
//! добавляла зависимостей продуктовому бинарнику:
//!
//! ```text
//! rustc -O tools/corpus10m.rs -o /tmp/svindb-corpus10m
//! /tmp/svindb-corpus10m --output /path/to/flat --kind flat --docs 10000000
//! /tmp/svindb-corpus10m --output /path/to/mixed --kind mixed --docs 10000000
//! ```

use std::env;
use std::fmt::Write as FmtWrite;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

const DEFAULT_DOCS: u64 = 10_000_000;
const DEFAULT_PARTS: usize = 30;
const DEFAULT_SEED: u64 = 0x5a17_2026_0813_cafe;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Flat,
    Mixed,
}

impl Kind {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "flat" => Ok(Self::Flat),
            "mixed" => Ok(Self::Mixed),
            _ => Err(format!(
                "неизвестный корпус {value:?}; ожидается flat или mixed"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Mixed => "mixed",
        }
    }

    fn schema(self) -> &'static str {
        match self {
            Self::Flat => "message:2,views:int64,ts:date,channel:keyword",
            Self::Mixed => concat!(
                "text:2,title:1,channel:keyword,status:keyword,views:int64,sequence:int32,",
                "published:date:iso,flag:bool,score:double,ratio:float,place:geo_point,",
                "author.name:keyword,reactions.emoji:keyword:slots,",
                "reactions.count:int32:slots,payload:binary,raw:keyword:noindex,",
                "embedding:dense_vector:384,embedding_sparse:sparse_vector"
            ),
        }
    }
}

struct Config {
    output: PathBuf,
    kind: Kind,
    docs: u64,
    parts: usize,
    seed: u64,
}

#[derive(Default)]
struct PartStats {
    docs: u64,
    bytes: u64,
    body_bytes: u64,
    classes: [u64; 5],
}

#[derive(Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

fn parse_args() -> Result<Config, String> {
    let mut output = None;
    let mut kind = None;
    let mut docs = DEFAULT_DOCS;
    let mut parts = DEFAULT_PARTS;
    let mut seed = DEFAULT_SEED;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("для {arg} не задано значение"))?;
        match arg.as_str() {
            "--output" => output = Some(PathBuf::from(value)),
            "--kind" => kind = Some(Kind::parse(&value)?),
            "--docs" => docs = value.parse().map_err(|_| "--docs не является числом")?,
            "--parts" => parts = value.parse().map_err(|_| "--parts не является числом")?,
            "--seed" => {
                seed = value
                    .strip_prefix("0x")
                    .map_or_else(|| value.parse(), |v| u64::from_str_radix(v, 16))
                    .map_err(|_| "--seed не является u64")?
            }
            _ => return Err(format!("неизвестный аргумент {arg}")),
        }
    }
    let output = output.ok_or("обязателен --output")?;
    let kind = kind.ok_or("обязателен --kind")?;
    if docs == 0 || parts == 0 {
        return Err("--docs и --parts должны быть положительными".into());
    }
    Ok(Config {
        output,
        kind,
        docs,
        parts,
        seed,
    })
}

fn mix(seed: u64, value: u64) -> u64 {
    let mut z = value.wrapping_add(seed).wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn rank(rng: &mut Rng, vocabulary: usize) -> usize {
    // Минимум трёх равномерных величин даёт тяжёлую голову, но сохраняет
    // растущий хвост. Словарь растёт по Heaps как sqrt(N).
    rng.below(vocabulary)
        .min(rng.below(vocabulary))
        .min(rng.below(vocabulary))
}

fn body_class(i: u64) -> usize {
    match mix(0x811c_9dc5, i) % 10_000 {
        0..=6499 => 0,
        6500..=8999 => 1,
        9000..=9799 => 2,
        9800..=9979 => 3,
        _ => 4,
    }
}

fn target_body_bytes(i: u64, class: usize, rng: &mut Rng) -> usize {
    let (lo, hi) = match class {
        0 => (0, 160),
        // Границы класса полные, но масса смещена к короткому краю: иначе при
        // заданных долях корпус далеко выходит за ожидаемые 20--25 ГиБ.
        1 => (160, 1400),
        2 => (2 << 10, 10 << 10),
        3 => (16 << 10, 42 << 10),
        // Среднее около 256 КиБ; редкие документы доходят до самого лимита.
        _ if i % 997 == 0 => (900 << 10, 1 << 20),
        _ => (64 << 10, 256 << 10),
    };
    lo + rng.below(hi - lo + 1)
}

fn mixed_body(i: u64, rng: &mut Rng) -> (String, usize) {
    let class = body_class(i);
    let target = target_body_bytes(i, class, rng);
    if i == 0 && class == 0 {
        return (String::new(), class);
    }
    let minimum = format!("unique_{i:x} ");
    let target = target.max(minimum.len());
    let vocabulary = ((i + 2) as f64).sqrt() as usize * 48 + 1024;
    let languages = [
        "общий",
        "поиск",
        "документ",
        "янгилик",
        "қабул",
        "tezlik",
        "qidiruv",
        "index",
        "search",
        "storage",
        "東京",
        "検索",
        "資料",
        "emoji🙂",
        "e\u{301}",
        "URL_https://example.test/a?x=1",
        "CaseFold",
        "casefold",
        "123456",
        "punctuation-a_b",
    ];
    let mut chunk = minimum;
    if i % 2 == 0 {
        chunk.push_str("frequent_term ");
    }
    if i % 100 == 0 {
        chunk.push_str("medium_term ");
    }
    if i % 1_000 == 0 {
        chunk.push_str("rare_term ");
    }
    if i % 100_000 == 0 {
        chunk.push_str("ultra_rare_term ");
    }
    if i % 8 == 0 {
        chunk.push_str(
            "phraseone phrasetwo phrasethree phrasefour phrasefive phrasesix phraseseven phraseeight ",
        );
    }
    if i % 10 == 0 {
        let _ = write!(chunk, "prefx{:05} ", i % 10_000);
    }
    for _ in 0..32 {
        let lang = languages[rng.below(languages.len())];
        let _ = write!(chunk, "{lang} word_{:x} ", rank(rng, vocabulary));
    }
    // Высокий TF и почти однословные документы выделены явно.
    if i % 251 == 0 {
        chunk.clear();
        let _ = write!(
            chunk,
            "unique_{i:x} frequent_term frequent_term frequent_term "
        );
    }
    let mut body = String::with_capacity(target);
    body.push_str(&format!("unique_{i:x} "));
    while body.len() + chunk.len() <= target {
        body.push_str(&chunk);
    }
    while body.len() < target {
        body.push('x');
    }
    (body, class)
}

fn write_dense(out: &mut String, i: u64) {
    out.push('[');
    for d in 0..384u64 {
        if d > 0 {
            out.push(',');
        }
        let value = ((mix(i, d) % 2001) as i64 - 1000) as f64 / 1000.0;
        let _ = write!(out, "{value:.3}");
    }
    out.push(']');
}

fn flat_document(i: u64, rng: &mut Rng) -> (String, usize) {
    let words = 4 + rng.below(47);
    let vocabulary = ((i + 2) as f64).sqrt() as usize * 96 + 4096;
    let mut message = format!("unique_{i:x} common ");
    for _ in 0..words {
        let _ = write!(message, "term_{:x} ", rank(rng, vocabulary));
    }
    let doc = format!(
        "{{\"id\":\"flat-{i:010}\",\"message\":\"{message}\",\"views\":{},\"ts\":{},\"channel\":\"channel-{}\"}}",
        i % 1_000_000,
        1_700_000_000 + (i % 31_536_000),
        i % 1024
    );
    (doc, message.len())
}

fn mixed_document(i: u64, rng: &mut Rng) -> (String, usize, usize) {
    let (body, class) = mixed_body(i, rng);
    // Генератор использует только безопасные для JSON символы: управляющих,
    // кавычек и обратной косой в теле нет. Это проверяется тестом ниже.
    debug_assert!(!body.bytes().any(|b| b < 0x20 || b == b'"' || b == b'\\'));
    let mut doc = String::with_capacity(body.len() + 4096);
    let _ = write!(
        doc,
        "{{\"id\":\"mixed-{i:010}\",\"text\":\"{body}\",\"title\":\"title {}\"",
        i % 10_000
    );
    if i % 10 < 7 {
        let _ = write!(
            doc,
            ",\"channel\":\"channel-{}\",\"status\":\"{}\",\"views\":{},\
             \"sequence\":{},\"published\":\"2025-{:02}-{:02}T{:02}:00:00Z\",\
             \"flag\":{},\"author\":{{\"name\":\"author-{}\"}}",
            i % 257,
            if i % 3 == 0 { "active" } else { "archive" },
            i % 10_000_000,
            i % i32::MAX as u64,
            i % 12 + 1,
            i % 28 + 1,
            i % 24,
            i % 2 == 0,
            i % 100_000
        );
    }
    if i % 4 == 0 {
        let _ = write!(
            doc,
            ",\"score\":{:.3},\"ratio\":{:.4},\"place\":{{\"lat\":{:.4},\"lon\":{:.4}}}",
            (i % 1_000_000) as f64 / 1000.0,
            (i % 10_000) as f64 / 10_000.0,
            -80.0 + (i % 1_600_000) as f64 / 10_000.0,
            -170.0 + (i % 3_400_000) as f64 / 10_000.0
        );
    }
    if i % 5 == 0 {
        doc.push_str(",\"reactions\":[");
        let slots = (mix(0x51_075, i) % 33) as usize;
        for slot in 0..slots {
            if slot > 0 {
                doc.push(',');
            }
            let _ = write!(
                doc,
                "{{\"emoji\":\"{}\",\"count\":{}}}",
                if slot % 3 == 0 {
                    "up"
                } else if slot % 3 == 1 {
                    "fire"
                } else {
                    "heart"
                },
                (i as usize + slot) % 10_000
            );
        }
        doc.push(']');
    }
    if i % 10 == 0 {
        let _ = write!(
            doc,
            ",\"payload\":\"AQIDBAUGBwgJCgsMDQ4P\",\"raw\":\"raw-{i:010}-not-indexed\""
        );
    }
    if i % 20 == 0 {
        doc.push_str(",\"embedding\":");
        write_dense(&mut doc, i);
    }
    if i % 20 == 1 {
        doc.push_str(",\"embedding_sparse\":{");
        let n = 16 + rng.below(113);
        for d in 0..n {
            if d > 0 {
                doc.push(',');
            }
            let _ = write!(
                doc,
                "\"sparse_{:x}\":{:.3}",
                mix(i, d as u64) % 65_536,
                0.1 + d as f64 / n as f64
            );
        }
        doc.push('}');
    }
    doc.push('}');
    (doc, body.len(), class)
}

fn generate_part(
    dir: &Path,
    part: usize,
    begin: u64,
    end: u64,
    kind: Kind,
    seed: u64,
) -> Result<PartStats, String> {
    let path = dir.join(format!("part-{part:03}.ndjson"));
    let file = File::create(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut out = BufWriter::with_capacity(8 << 20, file);
    let mut stats = PartStats::default();
    for i in begin..end {
        let mut rng = Rng::new(mix(seed, i));
        let (doc, body_bytes, class) = match kind {
            Kind::Flat => {
                let (doc, body) = flat_document(i, &mut rng);
                (doc, body, 0)
            }
            Kind::Mixed => mixed_document(i, &mut rng),
        };
        out.write_all(doc.as_bytes())
            .map_err(|e| format!("{}: {e}", path.display()))?;
        out.write_all(b"\n")
            .map_err(|e| format!("{}: {e}", path.display()))?;
        stats.docs += 1;
        stats.bytes += doc.len() as u64 + 1;
        stats.body_bytes += body_bytes as u64;
        stats.classes[class] += 1;
    }
    out.flush()
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(stats)
}

fn run(config: Config) -> Result<(), String> {
    if config.output.exists() {
        return Err(format!(
            "{} уже существует; генератор никогда не перезаписывает корпус",
            config.output.display()
        ));
    }
    fs::create_dir_all(&config.output).map_err(|e| e.to_string())?;
    let began = Instant::now();
    let mut handles = Vec::with_capacity(config.parts);
    for part in 0..config.parts {
        let dir = config.output.clone();
        let begin = config.docs * part as u64 / config.parts as u64;
        let end = config.docs * (part + 1) as u64 / config.parts as u64;
        let kind = config.kind;
        let seed = config.seed;
        handles.push(thread::spawn(move || {
            generate_part(&dir, part, begin, end, kind, seed)
        }));
    }
    let mut total = PartStats::default();
    for handle in handles {
        let part = handle
            .join()
            .map_err(|_| "поток генератора аварийно завершился")??;
        total.docs += part.docs;
        total.bytes += part.bytes;
        total.body_bytes += part.body_bytes;
        for (to, from) in total.classes.iter_mut().zip(part.classes) {
            *to += from;
        }
    }
    if total.docs != config.docs {
        return Err(format!(
            "сгенерировано {}, ожидалось {}",
            total.docs, config.docs
        ));
    }
    let elapsed = began.elapsed().as_secs_f64();
    let manifest = format!(
        concat!(
            "{{\n  \"format\":1,\n  \"kind\":\"{}\",\n  \"seed\":{},\n",
            "  \"documents\":{},\n  \"parts\":{},\n  \"bytes\":{},\n",
            "  \"body_bytes\":{},\n  \"classes\":[{},{},{},{},{}],\n",
            "  \"schema\":\"{}\"\n}}\n"
        ),
        config.kind.name(),
        config.seed,
        total.docs,
        config.parts,
        total.bytes,
        total.body_bytes,
        total.classes[0],
        total.classes[1],
        total.classes[2],
        total.classes[3],
        total.classes[4],
        config.kind.schema(),
    );
    fs::write(config.output.join("corpus.json"), &manifest).map_err(|e| e.to_string())?;
    println!(
        "{{\"output\":\"{}\",\"kind\":\"{}\",\"documents\":{},\"bytes\":{},\"seconds\":{elapsed:.3}}}",
        config.output.display(),
        config.kind.name(),
        total.docs,
        total.bytes
    );
    Ok(())
}

fn main() {
    if let Err(error) = parse_args().and_then(run) {
        eprintln!("ошибка генератора: {error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_has_exact_declared_buckets() {
        let mut classes = [0u64; 5];
        for i in 0..1_000_000 {
            classes[body_class(i)] += 1;
        }
        let shares: Vec<f64> = classes.iter().map(|&n| n as f64 / 1_000_000.0).collect();
        let expected = [0.65, 0.25, 0.08, 0.018, 0.002];
        for (got, want) in shares.iter().zip(expected) {
            assert!((got - want).abs() < 0.002, "{shares:?}");
        }
    }

    #[test]
    fn bodies_are_unique_and_safe_to_embed() {
        let mut seen = std::collections::HashSet::new();
        for i in 0..10_000 {
            let mut rng = Rng::new(mix(DEFAULT_SEED, i));
            let (body, _) = mixed_body(i, &mut rng);
            assert!(seen.insert(body.clone()), "повтор тела у {i}");
            assert!(!body.bytes().any(|b| b < 0x20 || b == b'"' || b == b'\\'));
        }
    }
}
