//! Договор durable-каталога состава: обновление формата, восстановление после
//! аварии и отказ вместо догадки.
//!
//! Проверяется не «каталог записался», а то, ради чего он заведён: подтверждённое
//! изменение переживает `kill -9`, неподтверждённое не оставляет полусостояния, а
//! потерянный каталог останавливает запуск вместо тихой пересборки по листингу.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");
static UNIQUE: AtomicU64 = AtomicU64::new(1);

struct Reply {
    code: u16,
    body: String,
}

struct Node {
    child: Option<Child>,
    port: u16,
}

fn root(name: &str) -> PathBuf {
    let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("svindb-каталог-{name}-{}-{unique}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("свободный порт");
    let port = listener.local_addr().expect("адрес").port();
    drop(listener);
    port
}

impl Node {
    fn start(root: &Path) -> Node {
        Self::start_with(root, None).expect("узел не поднялся")
    }

    /// Поднимает узел. `failpoints` вооружает точки отказа на этот запуск.
    fn start_with(root: &Path, failpoints: Option<&str>) -> Option<Node> {
        let port = free_port();
        let mut command = Command::new(BIN);
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
            // Пол свободного места — пять процентов тома, и на почти полном
            // диске он один валит любой тест приёма. Здесь проверяется каталог
            // состава, а не место, поэтому пол снимается явно.
            .env("SVINDB_DISK_FLOOR_MB", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match failpoints {
            Some(spec) => {
                command.env("SVINDB_FAILPOINTS", spec);
            }
            None => {
                command.env_remove("SVINDB_FAILPOINTS");
            }
        }
        let child = command.spawn().expect("узел");
        let mut node = Node { child: Some(child), port };
        let until = Instant::now() + Duration::from_secs(20);
        while Instant::now() < until {
            if let Some(status) = node.child.as_mut().expect("процесс").try_wait().expect("статус")
            {
                let _ = status;
                node.child = None;
                return None;
            }
            if call(port, "GET", "/_health", None).code == 200 {
                return Some(node);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        None
    }

    /// Ждёт, пока узел не умрёт сам, и возвращает признак ненулевого выхода.
    #[cfg(feature = "failpoints")]
    fn wait_for_death(&mut self) -> bool {
        let until = Instant::now() + Duration::from_secs(20);
        while Instant::now() < until {
            if let Some(status) = self.child.as_mut().expect("процесс").try_wait().expect("статус")
            {
                self.child = None;
                return !status.success();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("узел не умер на вооружённой точке отказа");
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return Reply { code: 0, body: String::new() };
    };
    stream.set_read_timeout(Some(Duration::from_secs(20))).expect("срок чтения");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return Reply { code: 0, body: String::new() };
    }
    let mut bytes = Vec::new();
    if stream.read_to_end(&mut bytes).is_err() {
        return Reply { code: 0, body: String::new() };
    }
    let text = String::from_utf8_lossy(&bytes).to_string();
    let head = text.lines().next().unwrap_or("");
    let code = head.split_whitespace().nth(1).and_then(|w| w.parse().ok()).unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, body)| body).to_string();
    Reply { code, body }
}

fn create(port: u16, index: &str) -> Reply {
    call(port, "PUT", &format!("/{index}"), Some(r#"{"fields":"tenant:keyword,body:text"}"#))
}

/// Имена и UUID индексов из каталога процесса.
fn catalog_of(port: u16) -> Vec<(String, String)> {
    let reply = call(port, "GET", "/_cat/indices", None);
    assert_eq!(reply.code, 200, "каталог: {}", reply.body);
    let rows: serde_json::Value = serde_json::from_str(&reply.body).expect("каталог JSON");
    let mut out: Vec<(String, String)> = rows
        .as_array()
        .expect("массив")
        .iter()
        .map(|row| {
            (
                row.get("index").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                row.get("uuid").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

fn marker_format(root: &Path) -> u64 {
    let raw = std::fs::read(root.join("multi-index-v1.json")).expect("marker");
    let value: serde_json::Value = serde_json::from_slice(&raw).expect("marker JSON");
    value.get("format").and_then(serde_json::Value::as_u64).unwrap_or(0)
}

/// Превращает готовый корень обратно в корень формата 1.
///
/// Это и есть fixture 1.2.0: на диске у той версии не было ни каталога состава,
/// ни UUID в метаданных индекса, а marker объявлял формат 1. Сегменты, схема и
/// WAL между версиями не менялись, поэтому подделывать их не нужно.
fn downgrade_to_format_one(root: &Path) {
    std::fs::write(root.join("multi-index-v1.json"), b"{\"format\":1}\n").expect("marker");
    let _ = std::fs::remove_dir_all(root.join("catalog"));
    let indices = root.join("indices");
    for entry in std::fs::read_dir(&indices).expect("каталог индексов") {
        let path = entry.expect("элемент").path();
        let meta_path = path.join("index.json");
        let raw = std::fs::read(&meta_path).expect("метаданные");
        let mut value: serde_json::Value = serde_json::from_slice(&raw).expect("метаданные JSON");
        let map = value.as_object_mut().expect("объект");
        map.remove("uuid");
        map.insert("format".to_string(), serde_json::json!(1));
        std::fs::write(&meta_path, serde_json::to_vec(&value).expect("кодирование"))
            .expect("запись метаданных");
    }
}

#[test]
fn a_v1_2_root_is_upgraded_in_place_without_reindexing() {
    let dir = root("обновление");
    {
        let node = Node::start(&dir);
        for index in ["orders", "users"] {
            assert_eq!(create(node.port, index).code, 200);
        }
        let seeded = call(
            node.port,
            "POST",
            "/orders/_bulk",
            Some(r#"{"documents":[{"id":"o-1","tenant":"acme","body":"первый заказ"}]}"#),
        );
        assert_eq!(seeded.code, 200, "{}", seeded.body);
        assert_eq!(call(node.port, "POST", "/orders/_flush", None).code, 200);
    }
    downgrade_to_format_one(&dir);
    assert!(!dir.join("catalog").exists(), "fixture обязан быть без каталога состава");

    let node = Node::start(&dir);
    let after = catalog_of(node.port);
    assert_eq!(
        after.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(),
        vec!["orders", "users"],
        "оба индекса обязаны пережить обновление"
    );
    for (name, uuid) in &after {
        assert_eq!(uuid.len(), 36, "индексу {name} обязан быть выдан UUID");
    }
    // Документ на месте: обновление меняет состав, а не данные.
    let found = call(node.port, "GET", "/orders/_doc/o-1", None);
    assert_eq!(found.code, 200, "{}", found.body);
    assert!(found.body.contains("первый заказ"), "{}", found.body);
    assert_eq!(marker_format(&dir), 2, "marker обязан подняться до второго формата");
}

#[test]
fn uuids_survive_a_restart() {
    let dir = root("устойчивость");
    let before = {
        let node = Node::start(&dir);
        assert_eq!(create(node.port, "orders").code, 200);
        assert_eq!(create(node.port, "users").code, 200);
        catalog_of(node.port)
    };
    let node = Node::start(&dir);
    assert_eq!(catalog_of(node.port), before, "UUID обязаны быть теми же после перезапуска");
    assert!(before.iter().all(|(_, uuid)| uuid.len() == 36));
}

#[test]
fn a_recreated_index_gets_a_new_identity() {
    let dir = root("пересоздание");
    let node = Node::start(&dir);
    assert_eq!(create(node.port, "orders").code, 200);
    let first = catalog_of(node.port);
    assert_eq!(call(node.port, "DELETE", "/orders", None).code, 200);
    assert_eq!(create(node.port, "orders").code, 200);
    let second = catalog_of(node.port);
    assert_ne!(
        first[0].1, second[0].1,
        "то же имя после удаления — другой индекс, и привязка не должна наследоваться молча"
    );
}

#[test]
fn a_format_two_root_without_its_catalog_refuses_to_start() {
    let dir = root("без-каталога");
    {
        let node = Node::start(&dir);
        assert_eq!(create(node.port, "orders").code, 200);
    }
    std::fs::remove_dir_all(dir.join("catalog")).expect("удаление каталога состава");
    assert!(
        Node::start_with(&dir, None).is_none(),
        "корень с индексами и без каталога обязан останавливать запуск: пересборка состава \
         по листингу вернула бы имена, но молча уничтожила бы aliases, блоки и теги"
    );
}

#[test]
fn a_corrupt_catalog_snapshot_refuses_to_start() {
    let dir = root("порча");
    {
        let node = Node::start(&dir);
        assert_eq!(create(node.port, "orders").code, 200);
    }
    let snapshot = dir.join("catalog").join("snapshot.bin");
    let mut bytes = std::fs::read(&snapshot).expect("снимок");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&snapshot, &bytes).expect("запись");
    assert!(
        Node::start_with(&dir, None).is_none(),
        "испорченный снимок состава обязан останавливать запуск, а не подниматься наполовину"
    );
}

#[test]
fn an_index_directory_without_a_catalog_record_is_not_resurrected() {
    let dir = root("сирота");
    let node = Node::start(&dir);
    assert_eq!(create(node.port, "orders").code, 200);
    drop(node);

    // Каталог на диске остаётся, запись из состава исчезает — так выглядит
    // авария между двумя durable шагами удаления.
    let orphan = dir.join("indices").join(format!("i-{}", hex("ghost")));
    let source = dir.join("indices").join(format!("i-{}", hex("orders")));
    copy_tree(&source, &orphan);
    let meta_path = orphan.join("index.json");
    let raw = std::fs::read(&meta_path).expect("метаданные");
    let mut value: serde_json::Value = serde_json::from_slice(&raw).expect("метаданные JSON");
    let map = value.as_object_mut().expect("объект");
    map.insert("name".to_string(), serde_json::json!("ghost"));
    std::fs::write(&meta_path, serde_json::to_vec(&value).expect("кодирование")).expect("запись");

    let node = Node::start(&dir);
    let names: Vec<String> = catalog_of(node.port).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, vec!["orders"], "каталог на диске сам по себе не объявляет о существовании");
    assert_eq!(call(node.port, "GET", "/ghost", None).code, 404);
}

fn hex(name: &str) -> String {
    name.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("каталог");
    for entry in std::fs::read_dir(from).expect("чтение") {
        let entry = entry.expect("элемент");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("тип").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("копия");
        }
    }
}

#[test]
fn alias_rollover_routes_writes_and_searches_every_generation_after_restart() {
    let dir = root("alias-rollover");
    {
        let node = Node::start(&dir);
        let created = call(
            node.port,
            "PUT",
            "/messages-000001",
            Some(
                r#"{"fields":"body:text,ts:int64","metadata":{"description":"сообщения","tags":["messages","hot"]},"aliases":{"all_messages":{"is_write_index":true}}}"#,
            ),
        );
        assert_eq!(created.code, 200, "{}", created.body);
        assert_eq!(
            call(
                node.port,
                "POST",
                "/all_messages/_bulk",
                Some(r#"{"documents":[{"id":"old","body":"общее старое","ts":1}]}"#),
            )
            .code,
            200
        );
        assert_eq!(call(node.port, "POST", "/all_messages/_flush", Some("{}")).code, 200);
        let rolled = call(node.port, "POST", "/all_messages/_rollover", Some("{}"));
        assert_eq!(rolled.code, 200, "{}", rolled.body);
        assert!(rolled.body.contains("messages-000002"), "{}", rolled.body);
        assert_eq!(
            call(
                node.port,
                "POST",
                "/all_messages/_bulk",
                Some(r#"{"documents":[{"id":"new","body":"общее новое","ts":2}]}"#),
            )
            .code,
            200
        );
        assert_eq!(call(node.port, "POST", "/all_messages/_flush", Some("{}")).code, 200);
        let old_write = call(
            node.port,
            "POST",
            "/messages-000001/_bulk",
            Some(r#"{"documents":[{"id":"forbidden","body":"нет","ts":3}]}"#),
        );
        assert_eq!(old_write.code, 403, "{}", old_write.body);
    }

    let node = Node::start(&dir);
    let search = call(
        node.port,
        "POST",
        "/all_messages/_search",
        Some(r#"{"match_all":true,"k":10,"ids":true,"source":true,"aggs":{"count":{"stats":{}}}}"#),
    );
    assert_eq!(search.code, 200, "{}", search.body);
    let value: serde_json::Value = serde_json::from_str(&search.body).expect("search JSON");
    let hits = value.get("hits").and_then(serde_json::Value::as_array).expect("hits");
    assert_eq!(hits.len(), 2, "{}", search.body);
    let indices: std::collections::BTreeSet<&str> = hits
        .iter()
        .filter_map(|hit| hit.get("_index").and_then(serde_json::Value::as_str))
        .collect();
    assert_eq!(indices, ["messages-000001", "messages-000002"].into_iter().collect());
    assert_eq!(value.pointer("/aggs/count/count").and_then(serde_json::Value::as_u64), Some(2));
    for target in
        ["/messages-*/_search", "/messages-000001,messages-000002/_search", "/_all/_search"]
    {
        let reply =
            call(node.port, "POST", target, Some(r#"{"match_all":true,"k":10,"ids":true}"#));
        assert_eq!(reply.code, 200, "{target}: {}", reply.body);
        let value: serde_json::Value = serde_json::from_str(&reply.body).expect("selector JSON");
        assert_eq!(
            value.get("hits").and_then(serde_json::Value::as_array).map(Vec::len),
            Some(2),
            "{target}: {}",
            reply.body
        );
    }
    let first_page = call(
        node.port,
        "POST",
        "/all_messages/_search",
        Some(r#"{"query":{"match_all":{}},"size":1,"sort":[{"ts":"asc"}],"_source":true}"#),
    );
    assert_eq!(first_page.code, 200, "{}", first_page.body);
    let first: serde_json::Value = serde_json::from_str(&first_page.body).expect("page JSON");
    let hit = &first["hits"][0];
    assert_eq!(hit.get("_id").and_then(serde_json::Value::as_str), Some("old"));
    let cursor = hit.get("sort").cloned().expect("sort cursor");
    let second_body = serde_json::json!({
        "query": {"match_all": {}},
        "size": 1,
        "sort": [{"ts": "asc"}],
        "search_after": cursor,
        "_source": true,
    });
    let second_page =
        call(node.port, "POST", "/all_messages/_search", Some(&second_body.to_string()));
    assert_eq!(second_page.code, 200, "{}", second_page.body);
    let second: serde_json::Value = serde_json::from_str(&second_page.body).expect("page JSON");
    assert_eq!(second["hits"][0].get("_id").and_then(serde_json::Value::as_str), Some("new"));
    let ambiguous = call(node.port, "GET", "/all_messages/_doc/old", None);
    assert_eq!(ambiguous.code, 400, "{}", ambiguous.body);
}

#[test]
fn metadata_blocks_and_true_close_have_independent_semantics() {
    let dir = root("metadata-blocks-close");
    let node = Node::start(&dir);
    assert_eq!(create(node.port, "archive").code, 200);
    let metadata = call(
        node.port,
        "PUT",
        "/archive/_metadata",
        Some(r#"{"description":"архив","tags":["cold","cold","messages"]}"#),
    );
    assert_eq!(metadata.code, 200, "{}", metadata.body);
    assert_eq!(call(node.port, "PUT", "/archive/_block/metadata", Some("{}")).code, 200);
    assert_eq!(
        call(node.port, "PUT", "/archive/_metadata", Some(r#"{"description":"не пройти"}"#),).code,
        403
    );
    // Metadata block снимает сам себя, иначе индекс оказался бы заперт навсегда.
    assert_eq!(call(node.port, "DELETE", "/archive/_block/metadata", Some("{}")).code, 200);
    assert_eq!(call(node.port, "POST", "/archive/_close", Some("{}")).code, 200);
    assert_eq!(
        call(node.port, "POST", "/archive/_search", Some(r#"{"match_all":true}"#),).code,
        400
    );
    // Wildcard по умолчанию раскрывает только open. Явный `closed` обязан
    // увидеть закрытый target и вернуть index_closed, а сочетание с
    // ignore_unavailable + allow_no_indices — законный пустой ответ. Параметр
    // не может быть просто синтаксически принят и затем проигнорирован.
    assert_eq!(call(node.port, "POST", "/arch*/_search", Some(r#"{"match_all":true}"#)).code, 404);
    assert_eq!(
        call(
            node.port,
            "POST",
            "/arch*/_search?expand_wildcards=closed",
            Some(r#"{"match_all":true}"#),
        )
        .code,
        400
    );
    let skipped = call(
        node.port,
        "POST",
        "/arch*/_search?expand_wildcards=closed&ignore_unavailable=true&allow_no_indices=true",
        Some(r#"{"match_all":true}"#),
    );
    assert_eq!(skipped.code, 200, "{}", skipped.body);
    assert!(skipped.body.contains(r#""hits":[]"#), "{}", skipped.body);
    let rows = call(node.port, "GET", "/_cat/indices?tag=cold", None);
    assert_eq!(rows.code, 200, "{}", rows.body);
    let rows: serde_json::Value = serde_json::from_str(&rows.body).expect("cat JSON");
    assert_eq!(rows.as_array().map(Vec::len), Some(1));
    assert_eq!(call(node.port, "POST", "/archive/_open", Some("{}")).code, 200);
    let metadata = call(node.port, "GET", "/archive/_metadata", None);
    assert!(metadata.body.contains("архив") && metadata.body.contains("messages"));
}

#[test]
fn atomic_alias_actions_accept_equal_schemas_with_different_metadata() {
    let dir = root("alias-actions");
    let node = Node::start(&dir);
    for (index, description) in [("part-a", "первая"), ("part-b", "вторая")] {
        let body = serde_json::json!({
            "fields": "tenant:keyword,body:text",
            "metadata": {"description": description, "tags": [index]},
        });
        let created = call(node.port, "PUT", &format!("/{index}"), Some(&body.to_string()));
        assert_eq!(created.code, 200, "{}", created.body);
    }
    let aliases = call(
        node.port,
        "POST",
        "/_aliases",
        Some(
            r#"{"actions":[{"add":{"index":"part-a","alias":"all_parts","is_write_index":false}},{"add":{"index":"part-b","alias":"all_parts","is_write_index":true}}]}"#,
        ),
    );
    assert_eq!(aliases.code, 200, "{}", aliases.body);
    assert_eq!(
        call(
            node.port,
            "POST",
            "/part-a/_bulk",
            Some(r#"{"documents":[{"id":"a","tenant":"x","body":"один"}]}"#),
        )
        .code,
        200
    );
    assert_eq!(
        call(
            node.port,
            "POST",
            "/all_parts/_bulk",
            Some(r#"{"documents":[{"id":"b","tenant":"x","body":"два"}]}"#),
        )
        .code,
        200
    );
    assert_eq!(call(node.port, "POST", "/part-a/_flush", Some("{}")).code, 200);
    assert_eq!(call(node.port, "POST", "/all_parts/_flush", Some("{}")).code, 200);
    let search = call(
        node.port,
        "POST",
        "/all_parts/_search",
        Some(r#"{"match_all":true,"k":10,"ids":true}"#),
    );
    assert_eq!(search.code, 200, "{}", search.body);
    let value: serde_json::Value = serde_json::from_str(&search.body).expect("search JSON");
    assert_eq!(value["hits"].as_array().map(Vec::len), Some(2));

    // У single-target alias явное false отличается от отсутствующего поля.
    assert_eq!(
        call(
            node.port,
            "PUT",
            "/part-a/_alias/read_only_part",
            Some(r#"{"is_write_index":false}"#),
        )
        .code,
        200
    );
    let refused = call(
        node.port,
        "POST",
        "/read_only_part/_bulk",
        Some(r#"{"documents":[{"id":"bad","tenant":"x","body":"нет"}]}"#),
    );
    assert_eq!(refused.code, 400, "{}", refused.body);
}

// ─────────────────────── аварии на durable-шагах ───────────────────────

#[cfg(feature = "failpoints")]
#[test]
fn a_kill_after_reserving_a_name_leaves_no_phantom_index() {
    let dir = root("бронь");
    {
        let node = Node::start(&dir);
        assert_eq!(create(node.port, "orders").code, 200);
    }
    let mut node = Node::start_with(&dir, Some("rootcat.create.after_reserve=kill"))
        .expect("узел с точкой отказа");
    let port = node.port;
    // Создание умрёт сразу после durable-брони имени.
    let _ = create(port, "users");
    assert!(node.wait_for_death(), "точка отказа не сработала");

    let node = Node::start(&dir);
    let names: Vec<String> = catalog_of(node.port).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, vec!["orders"], "неподтверждённое создание не обязано появиться");
    // И, что важнее, имя снова свободно: полуиндекс не занял его навсегда.
    assert_eq!(create(node.port, "users").code, 200, "имя обязано освободиться");
}

#[cfg(feature = "failpoints")]
#[test]
fn a_kill_after_marking_a_delete_does_not_resurrect_the_index() {
    let dir = root("удаление");
    {
        let node = Node::start(&dir);
        assert_eq!(create(node.port, "orders").code, 200);
        assert_eq!(create(node.port, "users").code, 200);
    }
    let mut node = Node::start_with(&dir, Some("rootcat.delete.after_mark=kill"))
        .expect("узел с точкой отказа");
    let port = node.port;
    let _ = call(port, "DELETE", "/users", None);
    assert!(node.wait_for_death(), "точка отказа не сработала");

    let node = Node::start(&dir);
    let names: Vec<String> = catalog_of(node.port).into_iter().map(|(name, _)| name).collect();
    assert_eq!(
        names,
        vec!["orders"],
        "помеченный к удалению индекс не воскресает: у него и прежнее, и последующее состояние \
         одинаково непригодны, поэтому восстановление идёт вперёд"
    );
    assert_eq!(call(node.port, "GET", "/users", None).code, 404);
}

#[cfg(feature = "failpoints")]
#[test]
fn a_kill_between_the_catalog_and_the_marker_converges_on_restart() {
    let dir = root("окно-обновления");
    {
        let node = Node::start(&dir);
        assert_eq!(create(node.port, "orders").code, 200);
        assert_eq!(create(node.port, "users").code, 200);
    }
    downgrade_to_format_one(&dir);

    // Каталог состава ляжет, marker подняться не успеет. Обновление идёт на
    // запуске, поэтому узел умирает **до** открытия порта — ждать порт здесь
    // означало бы ждать того, чего по условию не будет.
    let status = Command::new(BIN)
        .args(["serve", &dir.to_string_lossy(), &free_port().to_string(), "--sync-flush"])
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .env("SVINDB_FAILPOINTS", "rootcat.upgrade.after_catalog=kill")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("узел с точкой отказа");
    assert!(!status.success(), "точка отказа не сработала: {status}");
    assert_eq!(marker_format(&dir), 1, "marker обязан остаться прежним");
    assert!(dir.join("catalog").join("snapshot.bin").exists(), "каталог состава обязан лечь");

    // Повтор обязан сойтись: шаг проставления UUID идемпотентен, поэтому
    // второй заход выдаёт тот же состав и доводит marker до конца.
    let node = Node::start(&dir);
    let names: Vec<String> = catalog_of(node.port).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, vec!["orders", "users"]);
    assert_eq!(marker_format(&dir), 2);
}

#[cfg(feature = "failpoints")]
#[test]
fn an_alias_transaction_fsynced_before_kill_is_visible_after_restart() {
    let dir = root("alias-fsync-kill");
    {
        let node = Node::start(&dir);
        assert_eq!(create(node.port, "one").code, 200);
        assert_eq!(create(node.port, "two").code, 200);
    }
    let mut node =
        Node::start_with(&dir, Some("rootcat.after_fsync=kill")).expect("узел с точкой отказа");
    let _ = call(
        node.port,
        "POST",
        "/_aliases",
        Some(
            r#"{"actions":[{"add":{"index":"one","alias":"both"}},{"add":{"index":"two","alias":"both","is_write_index":true}}]}"#,
        ),
    );
    assert!(node.wait_for_death());

    let node = Node::start(&dir);
    let aliases = call(node.port, "GET", "/_alias/both", None);
    assert_eq!(aliases.code, 200, "{}", aliases.body);
    assert!(aliases.body.contains("one") && aliases.body.contains("two"));
    let write = call(
        node.port,
        "POST",
        "/both/_bulk",
        Some(r#"{"documents":[{"id":"x","tenant":"t","body":"после аварии"}]}"#),
    );
    assert_eq!(write.code, 200, "{}", write.body);
}

#[cfg(feature = "failpoints")]
#[test]
fn a_kill_after_durable_rollover_keeps_one_write_index_and_the_old_block() {
    let dir = root("rollover-fsync-kill");
    {
        let node = Node::start(&dir);
        let created = call(
            node.port,
            "PUT",
            "/messages-000001",
            Some(r#"{"fields":"body:text","aliases":{"all_messages":{"is_write_index":true}}}"#),
        );
        assert_eq!(created.code, 200, "{}", created.body);
    }
    let mut node =
        Node::start_with(&dir, Some("rootcat.after_fsync=kill")).expect("узел с точкой отказа");
    let _ = call(node.port, "POST", "/all_messages/_rollover", Some("{}"));
    assert!(node.wait_for_death());

    let node = Node::start(&dir);
    let rows = call(node.port, "GET", "/_cat/indices", None);
    assert_eq!(rows.code, 200, "{}", rows.body);
    let value: serde_json::Value = serde_json::from_str(&rows.body).expect("cat JSON");
    let rows = value.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "{}", value);
    let old = rows
        .iter()
        .find(|row| row.get("index").and_then(serde_json::Value::as_str) == Some("messages-000001"))
        .expect("старое поколение");
    assert!(old
        .get("blocks")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|blocks| blocks.iter().any(|block| block.as_str() == Some("write"))));
    let aliases = call(node.port, "GET", "/_alias/all_messages", None);
    assert_eq!(aliases.code, 200, "{}", aliases.body);
    let value: serde_json::Value = serde_json::from_str(&aliases.body).expect("alias JSON");
    assert_eq!(
        value
            .pointer("/messages-000002/aliases/all_messages/is_write_index")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}
