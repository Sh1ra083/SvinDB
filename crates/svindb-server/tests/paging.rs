//! Страница выдачи по настоящему сокету.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Node {
    child: Child,
    port: u16,
    dir: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn node(docs: usize) -> Node {
    node_with(docs, 1000, &[])
}

/// Тот же узел, но с заданной нарезкой и флагами.
///
/// Нарезка здесь не украшение: единственный путь, где страница ломалась, —
/// **один** сегмент и несколько потоков, и на трёх сегментах он не исполняется
/// вовсе. Стенд, строящий только многосегментный индекс, отчитывался бы зелёным
/// именно там, где ответ стократный.
fn node_with(docs: usize, per_segment: usize, flags: &[&str]) -> Node {
    let dir = std::env::temp_dir()
        .join(format!("svindb-page-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::create_dir_all(&dir).unwrap();
    let ndjson = format!("{dir}/c.ndjson");
    let mut f = std::io::BufWriter::new(std::fs::File::create(&ndjson).unwrap());
    for i in 0..docs {
        // Все документы содержат «общее», чтобы страница была глубокой;
        // «редкое{i}» даёт различимый вес и устойчивый порядок.
        writeln!(f, "{{\"title\":\"т{i}\",\"body\":\"общее редкое{i}\"}}").unwrap();
    }
    drop(f);
    let idx = format!("{dir}/index");
    assert!(Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["index", &ndjson, &idx, &per_segment.to_string()])
        .output()
        .unwrap()
        .status
        .success());
    let port = free_port();
    let mut args =
        vec!["serve".to_string(), idx.clone(), port.to_string(), "--read-only".to_string()];
    args.extend(flags.iter().map(|f| f.to_string()));
    let child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(&args)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let n = Node { child, port, dir };
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return n;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("узел не поднялся");
}

fn search(port: u16, body: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        s,
        "POST / HTTP/1.1\r\nHost: l\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    s.flush().unwrap();
    let mut r = BufReader::new(s);
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    let mut len = None;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).unwrap() == 0 || h.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                len = v.trim().parse::<usize>().ok();
            }
        }
    }
    let mut buf = vec![0u8; len.unwrap_or(0)];
    r.read_exact(&mut buf).unwrap();
    String::from_utf8_lossy(&buf).to_string()
}

#[test]
fn numeric_cursors_preserve_int64_edges_floats_and_missing() {
    use svindb_core::index::Kind;
    let root = std::env::temp_dir().join(format!(
        "svindb-typed-page-{}-{}",
        std::process::id(),
        free_port()
    ));
    let dir = root.to_string_lossy().to_string();
    let index = root.join("index").to_string_lossy().to_string();
    let mut writer = svindb_core::Indexer::typed(
        &index,
        vec!["rank".into(), "rating".into()],
        vec![1.0, 1.0],
        vec![Kind::Int64, Kind::Double],
        3,
    );
    let rows = [
        ("a", Some(i64::MIN), Some(-0.5f64)),
        ("b", Some(-1), Some(1.0)),
        ("c", Some(0), Some(0.25)),
        ("d", Some(i64::MAX), Some(2.5)),
        ("e", None, None),
        ("f", None, None),
        ("g", Some(2), Some(-0.0)),
        ("h", Some(3), Some(0.0)),
    ];
    for (id, rank, rating) in rows {
        writer.add(
            id,
            &[
                &rank.map_or(String::new(), |v| v.to_string()),
                &rating.map_or(String::new(), |v| v.to_string()),
            ],
        );
    }
    writer.flush_now();
    drop(writer);
    let port = free_port();
    let child = Command::new(BIN)
        .args(["serve", &index, &port.to_string(), "--read-only", "--single-index"])
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _node = Node { child, port, dir };
    let mut ready = false;
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            ready = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(ready, "узел не поднялся");
    for (field, direction, expected) in [
        ("rank", "asc", ["a", "b", "c", "g", "h", "d", "e", "f"]),
        ("rank", "desc", ["d", "h", "g", "c", "b", "a", "e", "f"]),
        ("rating", "asc", ["a", "g", "h", "c", "b", "d", "e", "f"]),
        ("rating", "desc", ["d", "b", "c", "h", "g", "a", "e", "f"]),
    ] {
        let mut body = serde_json::json!({"k": 2, "match_all": true, "ids": true,
            "sort": [{"field": field, "order": direction}], "partial": false,
            "pit": {"keep_alive_ms": 5000}});
        let mut got = Vec::new();
        for _ in 0..8 {
            let reply = search(port, &body.to_string());
            let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
            assert!(reply.get("error").is_none(), "{reply}");
            assert_eq!(reply["complete"], true, "{reply}");
            let hits = reply["hits"].as_array().unwrap();
            if hits.is_empty() {
                break;
            }
            for hit in hits {
                let id = hit["id"].as_str().unwrap();
                got.push(id.to_string());
                let row = rows.iter().find(|row| row.0 == id).unwrap();
                let value = &hit["sort"][0];
                if field == "rank" {
                    assert_eq!(value.as_i64(), row.1, "{hit}");
                } else {
                    assert_eq!(value.as_f64().map(f64::to_bits), row.2.map(f64::to_bits), "{hit}");
                }
                if row.1.is_none() {
                    assert!(value.is_null(), "missing не обозначен null: {hit}");
                }
            }
            body["search_after"] = hits.last().unwrap()["sort"].clone();
            body["pit"] = serde_json::json!({"id": reply["pit"], "keep_alive_ms": 5000});
        }
        assert_eq!(got, expected, "{field} {direction}: курсор повторил или потерял документы");
    }
}

/// Номера документов выдачи по порядку. Разбирается грубо и намеренно: тест не
/// должен зависеть от разборщика, который сам же и проверяет.
fn docs_of(json: &str) -> Vec<u32> {
    let mut out = Vec::new();
    let mut rest = json;
    while let Some(at) = rest.find("\"doc\":") {
        rest = &rest[at + 6..];
        let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        out.push(rest[..end].parse().unwrap());
    }
    out
}

#[test]
fn a_page_is_exactly_as_long_as_it_was_asked_to_be() {
    let n = node(3000);
    let word = |from: usize, size: usize| {
        format!(
            "{{\"from\":{from},\"size\":{size},\"words\":[{{\"field\":1,\"terms\":[\"общее\"]}}]}}"
        )
    };

    for (from, size) in [(0usize, 10usize), (5, 2), (100, 25), (990, 10), (1500, 1)] {
        let got = docs_of(&search(n.port, &word(from, size)));
        assert_eq!(got.len(), size, "from={from} size={size}: страница не той длины");
    }
}

/// Страница — именно **хвост** общего порядка, а не другая выборка. Длина сама
/// по себе ничего не доказывает: десять случайных документов тоже десять.
#[test]
fn a_page_is_the_tail_of_the_same_order() {
    let n = node(3000);
    let whole = docs_of(&search(
        n.port,
        "{\"from\":0,\"size\":1000,\"words\":[{\"field\":1,\"terms\":[\"общее\"]}]}",
    ));
    assert_eq!(whole.len(), 1000, "опорная выдача не набралась");
    for (from, size) in [(0usize, 10usize), (7, 3), (500, 20), (990, 10)] {
        let page = docs_of(&search(
            n.port,
            &format!(
                "{{\"from\":{from},\"size\":{size},\"words\":[{{\"field\":1,\"terms\":[\"общее\"]}}]}}"
            ),
        ));
        assert_eq!(page, whole[from..from + size], "from={from} size={size}: не тот кусок");
    }
}

/// Потолок глубины считается по сумме, а не по одному размеру страницы, и
/// отказ называет её.
#[test]
fn beyond_the_depth_limit_it_refuses_and_says_the_sum() {
    let n = node(100);
    let body = search(
        n.port,
        "{\"from\":9995,\"size\":10,\"words\":[{\"field\":1,\"terms\":[\"общее\"]}]}",
    );
    assert!(body.contains("10005"), "отказ не назвал сумму: {body}");
}

/// Тот же вопрос на **слитом** индексе и в несколько потоков.
#[test]
fn one_segment_and_many_threads_answer_the_same_page() {
    // Шестьдесят тысяч, а не три: путь в несколько потоков берётся только
    // когда постингов набирается больше `PARALLEL_POSTINGS` (пятьдесят тысяч).
    // На трёх тысячах движок уходит на последовательный путь, и тест
    // отчитывался бы зелёным, не исполнив проверяемого.
    let n = node_with(60_000, 100_000, &["--threads=8"]);
    let whole = docs_of(&search(
        n.port,
        "{\"from\":0,\"size\":1000,\"words\":[{\"field\":1,\"terms\":[\"общее\"]}]}",
    ));
    assert_eq!(whole.len(), 1000, "опорная выдача не набралась");
    for (from, size) in [(0usize, 10usize), (5, 2), (990, 10)] {
        let page = docs_of(&search(
            n.port,
            &format!(
                "{{\"from\":{from},\"size\":{size},\"words\":[{{\"field\":1,\"terms\":[\"общее\"]}}]}}"
            ),
        ));
        assert_eq!(page.len(), size, "from={from} size={size}: страница не той длины");
        assert_eq!(page, whole[from..from + size], "from={from} size={size}: не тот кусок");
    }
}
