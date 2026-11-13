//! Сквозной gate для runtime-индекса с частыми заменами.
//!
//! Здесь важен настоящий HTTP, WAL, фоновый писатель и manifest. Внутренний
//! тест индексатора не поймал бы регрессию, при которой обработчик снова начал
//! синхронно вызывать flush после каждого heartbeat.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Node {
    child: Child,
    port: u16,
    root: String,
}

impl Node {
    fn wait_ready(&mut self) {
        for _ in 0..200 {
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!("узел завершился до открытия порта: {status}");
            }
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("узел не открыл порт {}", self.port);
    }

    fn restart(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.child = launch(&format!("{}/index", self.root), self.port);
        self.wait_ready();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::fs::remove_dir_all(&self.root).ok();
    }
}

struct Reply {
    code: u16,
    body: String,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let body = body.unwrap_or("");
    write!(
        socket,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    socket.flush().unwrap();

    let mut input = BufReader::new(socket);
    let mut status = String::new();
    input.read_line(&mut status).unwrap();
    let code = status.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let mut len = None;
    loop {
        let mut line = String::new();
        if input.read_line(&mut line).unwrap() == 0 || line.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                len = value.trim().parse::<usize>().ok();
            }
        }
    }
    let mut bytes = vec![0; len.unwrap_or(0)];
    input.read_exact(&mut bytes).unwrap();
    Reply { code, body: String::from_utf8_lossy(&bytes).into_owned() }
}

fn launch(index: &str, port: u16) -> Child {
    Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args([
            "serve",
            index,
            &port.to_string(),
            "--single-index",
            // Этот gate проверяет атомарность пачки, WAL и bounded flush. Фоновое
            // обслуживание проверяется отдельными stress/limits-сценариями и не
            // должно успевать между `flush_in_progress=false` и чтением health:
            // на быстрой Linux-машине оно уже успевало переписать 2100 физических
            // версий в 100 живых, из-за чего тест зависел от планировщика, хотя
            // оба состояния корректны. Здесь состав фиксируется намеренно.
            "--no-maintain",
            "--fields=revision:int64,payload:keyword",
            "--flush=100000",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn start() -> Node {
    let port = free_port();
    let root = std::env::temp_dir()
        .join(format!("svindb-update-heavy-{}-{port}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let index = format!("{root}/index");
    let child = launch(&index, port);
    let mut node = Node { child, port, root };
    node.wait_ready();
    node
}

fn batch(revision: u32, expected: Option<u32>) -> String {
    let docs = (0..100)
        .map(|id| {
            format!(
                "{{\"id\":\"queue-{id:03}\",\"revision\":{revision},\"payload\":\"lease-{revision}\"}}"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let conditions = (0..100)
        .map(|id| {
            let equals = expected.map_or_else(|| "null".to_string(), |n| n.to_string());
            format!("{{\"id\":\"queue-{id:03}\",\"field\":\"revision\",\"equals\":{equals}}}")
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{\"documents\":[{docs}],\"conditions\":[{conditions}]}}")
}

fn health(port: u16) -> serde_json::Value {
    let reply = call(port, "GET", "/_health", None);
    assert_eq!(reply.code, 200, "health: {}", reply.body);
    serde_json::from_str(&reply.body).unwrap()
}

#[test]
fn update_heavy_runtime_batches_revisions_and_bounds_flush_wait() {
    let mut node = start();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{
                "refresh_interval_ms":100,
                "refresh_min_docs":10000,
                "refresh_max_interval_ms":2000,
                "wal_group_commit_ms":5,
                "max_pending_writes":16,
                "flush_wait_ms":0,
                "merge_policy":"update_heavy"
            }"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);
    assert!(settings.body.contains("update_heavy"), "{}", settings.body);

    // Сто queue items и двадцать полных heartbeat/progress-переходов. Все
    // переходы durable после ответа, но ни один не просит отдельный flush.
    let created = call(node.port, "POST", "/_put", Some(&batch(0, None)));
    assert_eq!(created.code, 200, "создание: {}", created.body);
    for revision in 1..=20 {
        let reply = call(node.port, "POST", "/_put", Some(&batch(revision, Some(revision - 1))));
        assert_eq!(reply.code, 200, "revision {revision}: {}", reply.body);
    }

    // Устаревший владелец lease получает конфликт и ничего не переписывает.
    let stale = call(
        node.port,
        "POST",
        "/_put",
        Some(
            r#"{
              "documents":[{"id":"queue-000","revision":999,"payload":"stale"}],
              "conditions":[{"id":"queue-000","field":"revision","equals":19}]
            }"#,
        ),
    );
    assert_eq!(stale.code, 409, "устаревший CAS: {}", stale.body);

    // Нулевой wait не привязан к длительности записи сегмента. На очень
    // быстрой машине работа может успеть и дать 200; в обычном случае это 202.
    let began = Instant::now();
    let flush = call(node.port, "POST", "/_flush", Some("{}"));
    let waited = began.elapsed();
    assert!(matches!(flush.code, 200 | 202), "flush: {}", flush.body);
    assert!(
        waited < Duration::from_millis(500),
        "bounded flush ждал {waited:?} вместо быстрого 200/202"
    );

    let deadline = Instant::now() + Duration::from_secs(20);
    let settled = loop {
        let h = health(node.port);
        let busy = h["flush_in_progress"].as_bool().unwrap_or(true);
        let buffered = h["buffered"].as_u64().unwrap_or(u64::MAX);
        if !busy && buffered == 0 {
            break h;
        }
        assert!(Instant::now() < deadline, "flush не завершился: {h}");
        std::thread::sleep(Duration::from_millis(25));
    };

    assert_eq!(settled["segments"].as_u64(), Some(1), "{settled}");
    assert_eq!(settled["docs"].as_u64(), Some(2100), "{settled}");
    assert_eq!(settled["dead"].as_u64(), Some(2000), "{settled}");
    assert_eq!(settled["wal"]["torn_bytes"].as_u64(), Some(0), "{settled}");
    assert_eq!(
        settled["wal"]["seq"].as_u64(),
        settled["wal"]["checkpoint"].as_u64(),
        "после публикации WAL не дошёл до checkpoint: {settled}"
    );

    let metrics = call(node.port, "GET", "/_metrics", None);
    assert_eq!(metrics.code, 200, "metrics: {}", metrics.body);
    for family in [
        "svindb_segments_created_total 1",
        "svindb_compaction_pending_segments",
        "svindb_write_queue_pending",
        "svindb_wal_commit_ms",
        "svindb_flush_ms_count{} 1",
        "svindb_tombstone_ratio 0.952381",
    ] {
        assert!(metrics.body.contains(family), "в метриках нет {family}:\n{}", metrics.body);
    }

    // Ещё одна пачка получает durable ACK, но намеренно не публикуется. После
    // немедленного убийства процесса replay обязан увидеть одну целую WAL-
    // транзакцию и восстановить все сто документов, а не её случайный префикс.
    let durable = call(node.port, "POST", "/_put", Some(&batch(21, Some(20))));
    assert_eq!(durable.code, 200, "durable batch: {}", durable.body);
    node.restart();
    let replayed = health(node.port);
    assert_eq!(replayed["wal"]["replayed_puts"].as_u64(), Some(100), "{replayed}");
    assert_eq!(replayed["wal"]["torn_bytes"].as_u64(), Some(0), "{replayed}");

    let old_owner = call(
        node.port,
        "POST",
        "/_put",
        Some(
            r#"{
              "documents":[{"id":"queue-000","revision":999,"payload":"stale"}],
              "conditions":[{"id":"queue-000","field":"revision","equals":20}]
            }"#,
        ),
    );
    assert_eq!(old_owner.code, 409, "replay потерял новую revision: {}", old_owner.body);

    println!(
        "update-heavy gate: 2100 мутаций -> 1 сегмент; bounded flush {:?}; \
         100 документов восстановлены одной WAL-пачкой; torn_bytes=0",
        waited
    );
}

#[test]
fn a_saturated_http_write_queue_returns_retryable_429() {
    let node = start();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{
              "refresh_interval_ms":0,
              "wal_group_commit_ms":100,
              "max_pending_writes":1
            }"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    let port = node.port;
    let first = std::thread::spawn(move || {
        call(
            port,
            "POST",
            "/_put",
            Some(
                r#"{
                  "documents":[{"id":"first","revision":1,"payload":"one"}],
                  "conditions":[{"id":"first","field":"revision","equals":null}]
                }"#,
            ),
        )
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let pending = health(node.port)["write_queue"]["pending"].as_u64().unwrap_or(0);
        if pending == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "первый запрос не вошёл в write queue");
        std::thread::sleep(Duration::from_millis(1));
    }

    let refused = call(
        node.port,
        "POST",
        "/_put",
        Some(
            r#"{
              "documents":[{"id":"second","revision":1,"payload":"two"}],
              "conditions":[{"id":"second","field":"revision","equals":null}]
            }"#,
        ),
    );
    assert_eq!(refused.code, 429, "переполнение: {}", refused.body);
    assert!(refused.body.contains("\"retryable\":true"), "{}", refused.body);
    let accepted = first.join().unwrap();
    assert_eq!(accepted.code, 200, "первый запрос: {}", accepted.body);

    let metrics = call(node.port, "GET", "/_metrics", None);
    assert!(metrics.body.contains("svindb_write_queue_refused_total 1"), "{}", metrics.body);
}
