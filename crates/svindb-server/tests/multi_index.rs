//! Приёмочный договор многoиндексного процесса.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static UNIQUE: AtomicU64 = AtomicU64::new(1);

struct Reply {
    code: u16,
    body: String,
}

struct Node {
    child: Child,
    port: u16,
}

impl Node {
    fn start(root: &Path) -> Node {
        Self::start_with(root, &[])
    }

    fn start_with(root: &Path, extra: &[&str]) -> Node {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("свободный порт");
        let port = listener.local_addr().expect("адрес").port();
        drop(listener);
        let mut command = Command::new(env!("CARGO_BIN_EXE_svindb"));
        command
            .args([
                "serve",
                &root.to_string_lossy(),
                &port.to_string(),
                "--sync-flush",
                "--flush=1000",
                "--target=1000",
                "--refresh-ms=25",
            ])
            .args(extra)
            // This suite tests namespace/lifecycle semantics, not host free space.
            // Disk refusal itself is covered by host::disk_tests.
            .env("SVINDB_DISK_FLOOR_MB", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = command.spawn().expect("узел");
        let mut node = Node { child, port };
        let until = Instant::now() + Duration::from_secs(20);
        while Instant::now() < until {
            if let Some(status) = node.child.try_wait().expect("статус") {
                panic!("многoиндексный узел завершился до порта: {status}");
            }
            if call(port, "GET", "/_health", None).code == 200 {
                return node;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("узел не открыл порт {port}");
    }

    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn root(tag: &str) -> PathBuf {
    let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("svindb-multi-{tag}-{}-{unique}", std::process::id()));
    std::fs::remove_dir_all(&path).ok();
    path
}

fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return Reply { code: 0, body: String::new() };
    };
    stream.set_read_timeout(Some(Duration::from_secs(20))).expect("срок чтения");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("запрос");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).expect("ответ");
    let text = String::from_utf8(bytes).expect("ответ UTF-8");
    let head = text.lines().next().unwrap_or("");
    let code = head.split_whitespace().nth(1).and_then(|word| word.parse().ok()).unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, body)| body).to_string();
    Reply { code, body }
}

fn flush(port: u16, index: &str) {
    let reply = call(port, "POST", &format!("/{index}/_flush"), None);
    assert!(reply.code == 200 || reply.code == 202, "flush {index}: {}", reply.body);
}

#[test]
fn two_indices_are_isolated_survive_restart_and_release_their_lock_on_delete() {
    let root = root("lifecycle");
    let node = Node::start(&root);
    let orders =
        call(node.port, "PUT", "/orders", Some(r#"{"fields":"message:text,price:double"}"#));
    assert_eq!(orders.code, 200, "orders: {}", orders.body);
    let users = call(
        node.port,
        "PUT",
        "/users",
        Some(
            r#"{"mappings":{"dynamic":"strict","properties":{"name":{"type":"text"},"age":{"type":"integer"}}}}"#,
        ),
    );
    assert_eq!(users.code, 200, "users: {}", users.body);

    let order = call(
        node.port,
        "POST",
        "/orders/_bulk",
        Some(r#"{"documents":[{"id":"o1","message":"красный заказ","price":19.5}]}"#),
    );
    let user = call(
        node.port,
        "POST",
        "/users/_bulk",
        Some(r#"{"documents":[{"id":"u1","name":"Анна Смирнова","age":31}]}"#),
    );
    assert_eq!(order.code, 200, "order: {}", order.body);
    assert_eq!(user.code, 200, "user: {}", user.body);
    let wrong_index = call(
        node.port,
        "POST",
        "/orders/_bulk",
        Some("{\"index\":{\"_index\":\"users\",\"_id\":\"wrong\"}}\n{\"message\":\"не туда\"}\n"),
    );
    assert_eq!(wrong_index.code, 200, "NDJSON item response: {}", wrong_index.body);
    let wrong: serde_json::Value = serde_json::from_str(&wrong_index.body).unwrap();
    assert_eq!(wrong["errors"], true, "чужой _index проигнорирован: {wrong}");
    assert_eq!(wrong["items"][0]["index"]["status"], 400, "{wrong}");
    assert_eq!(call(node.port, "GET", "/orders/_doc/wrong", None).code, 404);
    assert_eq!(call(node.port, "GET", "/users/_doc/wrong", None).code, 404);
    flush(node.port, "orders");
    flush(node.port, "users");

    // Конкретный индекс в обычном multi-index layout — одна физическая цель,
    // а не fan-out. PIT обязан открываться и продолжаться именно здесь: иначе
    // resumable scan работал бы только в legacy `--single-index` режиме.
    let first_page = call(
        node.port,
        "POST",
        "/orders/_search",
        Some(r#"{"match_all":true,"k":10,"pit":{"keep_alive_ms":5000}}"#),
    );
    assert_eq!(first_page.code, 200, "PIT конкретного индекса: {}", first_page.body);
    let first_json: serde_json::Value = serde_json::from_str(&first_page.body).expect("ответ PIT");
    let pit = first_json["pit"].as_str().expect("PIT id");
    assert_eq!(first_json["hits"][0]["_index"], "orders");
    let continued = call(
        node.port,
        "POST",
        "/orders/_search",
        Some(
            &serde_json::json!({
                "match_all": true,
                "k": 10,
                "pit": {"id": pit, "keep_alive_ms": 5000}
            })
            .to_string(),
        ),
    );
    assert_eq!(continued.code, 200, "продолжение PIT: {}", continued.body);
    assert!(continued.body.contains("\"_index\":\"orders\""), "{}", continued.body);

    let found = call(node.port, "GET", "/orders/_doc/o1", None);
    assert_eq!(found.code, 200, "{}", found.body);
    assert!(found.body.contains("красный заказ"), "{}", found.body);
    let wrong_schema = call(
        node.port,
        "POST",
        "/orders/_search",
        Some(r#"{"k":10,"must":[{"field":"name","terms":["анна"]}]}"#),
    );
    assert_eq!(wrong_schema.code, 400, "индексы смешались: {}", wrong_schema.body);
    node.stop();

    let node = Node::start(&root);
    assert_eq!(call(node.port, "GET", "/orders/_doc/o1", None).code, 200);
    assert_eq!(call(node.port, "GET", "/users/_doc/u1", None).code, 200);
    let removed = call(node.port, "DELETE", "/users", None);
    assert_eq!(removed.code, 200, "delete: {}", removed.body);
    let recreated =
        call(node.port, "PUT", "/users", Some(r#"{"fields":"handle:keyword,bio:text"}"#));
    assert_eq!(recreated.code, 200, "writer lock не освободился: {}", recreated.body);
    assert_eq!(call(node.port, "GET", "/users/_doc/u1", None).code, 404);
    assert_eq!(call(node.port, "GET", "/orders/_doc/o1", None).code, 200);
    drop(node);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn concurrent_creation_publishes_exactly_one_index() {
    let root = root("concurrent-create");
    let node = Node::start(&root);
    let port = node.port;
    let first = std::thread::spawn(move || {
        call(port, "PUT", "/same", Some(r#"{"fields":"body:text"}"#)).code
    });
    let port = node.port;
    let second = std::thread::spawn(move || {
        call(port, "PUT", "/same", Some(r#"{"fields":"body:text"}"#)).code
    });
    let mut codes = [first.join().expect("первый"), second.join().expect("второй")];
    codes.sort_unstable();
    assert_eq!(codes, [200, 409]);
    let catalog = call(node.port, "GET", "/_cat/indices", None);
    assert_eq!(catalog.code, 200);
    assert_eq!(catalog.body.matches("\"index\":\"same\"").count(), 1, "{}", catalog.body);
    drop(node);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn a_second_writable_process_cannot_own_the_same_multi_root() {
    let root = root("root-lock");
    let node = Node::start(&root);
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("свободный порт");
    let port = listener.local_addr().expect("адрес").port();
    drop(listener);
    let mut second = Command::new(env!("CARGO_BIN_EXE_svindb"))
        .args(["serve", &root.to_string_lossy(), &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("второй процесс");
    let until = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = second.try_wait().expect("статус второго") {
            break status;
        }
        if Instant::now() >= until {
            let _ = second.kill();
            panic!("второй писатель не получил быстрый отказ");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(!status.success(), "два писателя одновременно владеют одним root");

    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("порт читателя");
    let reader_port = listener.local_addr().expect("адрес читателя").port();
    drop(listener);
    let mut reader = Command::new(env!("CARGO_BIN_EXE_svindb"))
        .args(["serve", &root.to_string_lossy(), &reader_port.to_string(), "--read-only"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("конкурирующий читатель");
    let until = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = reader.try_wait().expect("статус читателя") {
            break status;
        }
        if Instant::now() >= until {
            let _ = reader.kill();
            panic!("read-only процесс открыл меняющийся под ним multi-root");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(!status.success(), "читатель сосуществует с lifecycle-писателем");
    assert_eq!(call(node.port, "GET", "/_health", None).code, 200);
    drop(node);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn the_process_limit_and_read_only_restart_are_enforced() {
    let root = root("limit-read-only");
    let node = Node::start_with(&root, &["--max-indices=2"]);
    for name in ["first", "second"] {
        let made = call(node.port, "PUT", &format!("/{name}"), Some(r#"{"fields":"body:text"}"#));
        assert_eq!(made.code, 200, "{name}: {}", made.body);
    }
    let excess = call(node.port, "PUT", "/third", Some(r#"{"fields":"body:text"}"#));
    assert_eq!(excess.code, 429, "предел процесса не сработал: {}", excess.body);
    let oversized =
        call(node.port, "PUT", "/first/_settings", Some(r#"{"flush_mb":9223372036854775807}"#));
    assert_eq!(
        oversized.code, 409,
        "настройка сверх процессного бюджета применена: {}",
        oversized.body
    );
    let settings = call(node.port, "GET", "/first/_settings", None);
    assert_eq!(settings.code, 200, "{}", settings.body);
    assert!(
        settings.body.contains("\"flush_mb\":0"),
        "настройка частично записалась: {}",
        settings.body
    );
    let indexed = call(
        node.port,
        "POST",
        "/first/_bulk",
        Some(r#"{"documents":[{"id":"one","body":"сохранено"}]}"#),
    );
    assert_eq!(indexed.code, 200, "{}", indexed.body);
    flush(node.port, "first");
    node.stop();

    let node = Node::start_with(&root, &["--read-only", "--max-indices=2"]);
    let second_reader = Node::start_with(&root, &["--read-only", "--max-indices=2"]);
    let stored = call(node.port, "GET", "/first/_doc/one", None);
    assert_eq!(stored.code, 200, "read-only не открыл данные: {}", stored.body);
    assert_eq!(
        call(second_reader.port, "GET", "/first/_doc/one", None).code,
        200,
        "два read-only процесса не разделили стабильный root"
    );
    let create = call(node.port, "PUT", "/third", Some(r#"{"fields":"body:text"}"#));
    assert_eq!(create.code, 403, "read-only создал индекс: {}", create.body);
    let write = call(
        node.port,
        "POST",
        "/first/_bulk",
        Some(r#"{"documents":[{"id":"two","body":"нельзя"}]}"#),
    );
    assert_eq!(write.code, 403, "read-only принял документ: {}", write.body);
    drop(second_reader);
    drop(node);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn concurrent_search_update_delete_and_insert_stay_inside_their_indices() {
    const INDICES: [&str; 3] = ["alpha", "beta", "gamma"];
    const DOCS: usize = 48;

    let root = root("mixed-load");
    let node = Node::start(&root);
    for index in INDICES {
        let made = call(
            node.port,
            "PUT",
            &format!("/{index}"),
            Some(r#"{"fields":"tenant:keyword,body:text,revision:int64"}"#),
        );
        assert_eq!(made.code, 200, "{index}: {}", made.body);
        let documents = (0..DOCS)
            .map(|n| {
                format!(
                    r#"{{"id":"{index}-{n:03}","tenant":"{index}","body":"исходный документ {index}","revision":1}}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let seeded = call(
            node.port,
            "POST",
            &format!("/{index}/_bulk"),
            Some(&format!(r#"{{"documents":[{documents}]}}"#)),
        );
        assert_eq!(seeded.code, 200, "seed {index}: {}", seeded.body);
        flush(node.port, index);
    }

    let mut workers = Vec::new();
    for index in INDICES {
        let port = node.port;
        workers.push(std::thread::spawn(move || {
            for n in 0..DOCS {
                let body = format!(
                    r#"{{"documents":[{{"id":"{index}-{n:03}","tenant":"{index}","body":"обновлённый документ {index}","revision":2}}]}}"#
                );
                let updated = call(port, "POST", &format!("/{index}/_put"), Some(&body));
                assert_eq!(updated.code, 200, "update {index}/{n}: {}", updated.body);
                if n % 4 == 0 {
                    let deleted = call(
                        port,
                        "POST",
                        &format!("/{index}/_delete"),
                        Some(&format!(r#"{{"ids":["{index}-{n:03}"]}}"#)),
                    );
                    assert_eq!(deleted.code, 200, "delete {index}/{n}: {}", deleted.body);
                }
            }
        }));

        let port = node.port;
        workers.push(std::thread::spawn(move || {
            for _ in 0..80 {
                let searched = call(
                    port,
                    "POST",
                    &format!("/{index}/_search"),
                    Some(&format!(
                        r#"{{"k":100,"ids":true,"must":[{{"field":"tenant","terms":["{index}"]}}]}}"#
                    )),
                );
                if searched.code == 429 || searched.code == 503 {
                    assert!(
                        searched.body.contains("\"retryable\":true")
                            && searched.body.contains("\"reason\":\"workload_queue\""),
                        "отказ поиска {index} не объясняет насыщение очереди: {}",
                        searched.body
                    );
                    continue;
                }
                assert_eq!(searched.code, 200, "search {index}: {}", searched.body);
                for other in INDICES {
                    if other != index {
                        assert!(
                            !searched.body.contains(&format!("{other}-")),
                            "поиск {index} вернул документ {other}: {}",
                            searched.body
                        );
                    }
                }
            }
        }));
    }
    for worker in workers {
        worker.join().expect("поток смешанной нагрузки");
    }
    for index in INDICES {
        flush(node.port, index);
        for n in 0..DOCS {
            let got = call(node.port, "GET", &format!("/{index}/_doc/{index}-{n:03}"), None);
            if n % 4 == 0 {
                assert_eq!(got.code, 404, "удалённый {index}/{n} воскрес: {}", got.body);
            } else {
                assert_eq!(got.code, 200, "живой {index}/{n} исчез: {}", got.body);
                assert!(got.body.contains("\"revision\":2"), "не последняя версия: {}", got.body);
                assert!(got.body.contains(&format!("\"tenant\":\"{index}\"")), "{}", got.body);
            }
        }
    }
    node.stop();

    let node = Node::start(&root);
    for index in INDICES {
        let catalog = call(node.port, "GET", &format!("/{index}"), None);
        assert_eq!(catalog.code, 200, "restart не открыл {index}: {}", catalog.body);
        assert!(catalog.body.contains("\"docs\":36"), "неверный состав {index}: {}", catalog.body);
        for n in 0..DOCS {
            let got = call(node.port, "GET", &format!("/{index}/_doc/{index}-{n:03}"), None);
            assert_eq!(
                got.code,
                if n % 4 == 0 { 404 } else { 200 },
                "restart изменил {index}/{n}: {}",
                got.body
            );
            if got.code == 200 {
                assert!(got.body.contains("\"revision\":2"), "{}", got.body);
            }
        }
    }
    drop(node);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn snapshots_with_the_same_name_are_namespaced_by_index() {
    let root = root("snapshot-scope");
    let snapshots = root.with_extension("snapshots");
    std::fs::create_dir_all(&snapshots).expect("приёмник снимков");
    let config = root.with_extension("config.json");
    std::fs::write(
        &config,
        format!(r#"{{"snapshot":{{"allow":[{:?}]}}}}"#, snapshots.to_string_lossy()),
    )
    .expect("настройки снимков");
    let config_flag = format!("--config={}", config.to_string_lossy());
    let node = Node::start_with(&root, &[&config_flag]);

    for index in ["orders", "users"] {
        let made = call(node.port, "PUT", &format!("/{index}"), Some(r#"{"fields":"body:text"}"#));
        assert_eq!(made.code, 200, "{index}: {}", made.body);
        let data = format!(r#"{{"documents":[{{"id":"one","body":"{index}"}}]}}"#);
        let indexed = call(node.port, "POST", &format!("/{index}/_bulk"), Some(&data));
        assert_eq!(indexed.code, 200, "{index}: {}", indexed.body);
        flush(node.port, index);
        let snapshot = call(node.port, "POST", &format!("/{index}/_snapshot/daily"), None);
        assert_eq!(snapshot.code, 201, "snapshot {index}: {}", snapshot.body);
    }

    for index in ["orders", "users"] {
        let got = call(node.port, "GET", &format!("/{index}/_snapshot/daily"), None);
        assert_eq!(got.code, 200, "snapshot {index} не читается: {}", got.body);
        assert!(got.body.contains("\"docs\":1"), "{}", got.body);
        let listed = call(node.port, "GET", &format!("/{index}/_snapshot"), None);
        assert_eq!(listed.code, 200, "list {index}: {}", listed.body);
        assert_eq!(listed.body.matches("\"name\":\"daily\"").count(), 1, "{}", listed.body);
    }
    let scopes =
        std::fs::read_dir(snapshots.join("svindb-indices")).expect("scoped-каталог").count();
    assert_eq!(scopes, 2, "снимки двух индексов разделили один каталог");

    drop(node);
    std::fs::remove_file(config).ok();
    std::fs::remove_dir_all(snapshots).ok();
    std::fs::remove_dir_all(root).ok();
}
