//! Релизный контракт lossless source и Elasticsearch-semantics.
//!
//! Это воспроизведения дефектов, из-за которых вложенный документ с массивами
//! приходилось прятать строкой в `payload`. Проверяется настоящий HTTP binary,
//! буфер, сегмент и повторное открытие того же каталога.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Node {
    child: Child,
    port: u16,
    root: String,
}

impl Node {
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = spawn(&self.root, self.port);
        wait(&mut self.child, self.port);
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

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn spawn(root: &str, port: u16) -> Child {
    Command::new(BIN)
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args([
            "serve",
            root,
            &port.to_string(),
            "--bind=127.0.0.1",
            "--no-maintain",
            "--sync-flush",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn wait(child: &mut Child, port: u16) {
    for _ in 0..300 {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("узел завершился до открытия порта: {status}");
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("узел не открыл порт {port}");
}

fn start() -> Node {
    let port = free_port();
    let root = std::env::temp_dir()
        .join(format!("svindb-document-contract-{}-{port}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let mut child = spawn(&root, port);
    wait(&mut child, port);
    Node { child, port, root }
}

fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let body = body.unwrap_or("");
    write!(
        socket,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
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

fn create(port: u16) {
    let mapping = r#"{
      "settings":{"analysis":{"normalizer":{"lower":{"type":"custom","filter":["lowercase"]}}}},
      "mappings":{"dynamic":"strict","_source":{"enabled":true},"_meta":{"schema":"contract"},
        "properties":{
          "exact":{"type":"keyword","ignore_above":256},
          "folded":{"type":"keyword","normalizer":"lower"},
          "family":{"type":"constant_keyword","value":"message"},
          "title":{"type":"text","analyzer":"standard","fields":{"exact":{"type":"keyword"}}},
          "n":{"type":"long"},"nums":{"type":"long"},"optional_long":{"type":"long"},
          "flag":{"type":"boolean"},
          "when":{"type":"date","format":"strict_date_optional_time||epoch_millis"},
          "blob":{"type":"binary"},
          "price":{"type":"scaled_float","scaling_factor":100},
          "embedding":{"type":"dense_vector","dims":2},
          "embedding_sparse":{"type":"sparse_vector"},
          "source_only":{"type":"object","enabled":false},
          "changes":{"type":"nested","properties":{
            "path":{"type":"keyword"},"after_text":{"type":"text"},
            "details":{"type":"nested","properties":{"kind":{"type":"keyword"},"value":{"type":"long"}}}
          }}
        }
      }
    }"#;
    let reply = call(port, "PUT", "/contract", Some(mapping));
    assert_eq!(reply.code, 200, "mapping: {}", reply.body);
}

fn ids(reply: &Reply) -> Vec<String> {
    reply.json()["hits"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|hit| hit["_id"].as_str().map(str::to_string))
        .collect()
}

fn wait_for_ids(port: u16, body: &str, expected: &[&str], stage: &str) -> Reply {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let reply = call(port, "POST", "/flat/_search", Some(body));
        assert_eq!(reply.code, 200, "{stage}: {}", reply.body);
        let got = ids(&reply);
        if got == expected {
            return reply;
        }
        assert!(
            Instant::now() < deadline,
            "{stage}: search visibility не наступила за 10 с: ожидались {expected:?}, \
             получены {got:?}; ответ: {}",
            reply.body
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn files_named(dir: &std::path::Path, name: &str, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files_named(&path, name, out);
        } else if path.file_name().and_then(|part| part.to_str()) == Some(name) {
            out.push(path);
        }
    }
}

#[test]
fn flat_source_keeps_missing_distinct_from_empty_through_restart() {
    let mut node = start();
    let mapping = r#"{
      "mappings":{"dynamic":"strict","properties":{
        "exact":{"type":"keyword"},"title":{"type":"text"}
      }}
    }"#;
    let created = call(node.port, "PUT", "/flat", Some(mapping));
    assert_eq!(created.code, 200, "mapping: {}", created.body);

    for (id, source) in [
        ("missing", serde_json::json!({"title":"нет ключа"})),
        ("empty", serde_json::json!({"exact":"","title":"пустое значение"})),
        ("value", serde_json::json!({"exact":"x","title":"обычное значение"})),
    ] {
        let reply = call(node.port, "PUT", &format!("/flat/_doc/{id}"), Some(&source.to_string()));
        assert_eq!(reply.code, 200, "{id}: {}", reply.body);
    }

    for stage in ["buffer", "segment", "restart"] {
        if stage == "segment" {
            assert_eq!(call(node.port, "POST", "/flat/_flush", None).code, 200);
        } else if stage == "restart" {
            node.restart();
        }
        // ACK и search visibility — разные границы. Для buffer-стадии ждём
        // штатный refresh, не подменяя контракт искусственным flush на диск.
        // Фиксированный сон был гонкой с scheduler загруженного release CI:
        // проверять нужно bounded eventual visibility, а не удачную миллисекунду.
        let exists = wait_for_ids(
            node.port,
            r#"{"query":{"exists":{"field":"exact"}},"_source":false,"size":10}"#,
            &["empty", "value"],
            stage,
        );
        assert_eq!(ids(&exists), ["empty", "value"], "{stage}: presence потерян: {}", exists.body);

        let empty = wait_for_ids(
            node.port,
            r#"{"query":{"term":{"exact":""}},"_source":false,"size":10}"#,
            &["empty"],
            stage,
        );
        assert_eq!(ids(&empty), ["empty"], "{stage}: missing склеился с empty");
        assert_eq!(
            call(node.port, "GET", "/flat/_doc/missing", None).json()["_source"],
            serde_json::json!({"title":"нет ключа"}),
            "{stage}: sparse source изменён"
        );
    }
}

#[test]
fn source_disabled_keeps_search_and_only_explicit_stored_fields_through_restart() {
    let mut node = start();
    let mapping = r#"{
      "mappings":{"dynamic":"strict","_source":{"enabled":false},"properties":{
        "body":{"type":"text","index":true,"store":false},
        "label":{"type":"keyword","index":true,"store":true},
        "stored_text":{"type":"text","index":true,"store":true},
        "n":{"type":"long","index":true,"store":false}
      }}
    }"#;
    let created = call(node.port, "PUT", "/lean", Some(mapping));
    assert_eq!(created.code, 200, "source-disabled mapping: {}", created.body);

    let source = serde_json::json!({
        "body":"needle in a haystack","label":"visible",
        "stored_text":"stored highlight","n":42
    });
    let put = call(node.port, "PUT", "/lean/_doc/a", Some(&source.to_string()));
    assert_eq!(put.code, 200, "put: {}", put.body);
    assert_eq!(call(node.port, "POST", "/lean/_flush", None).code, 200);

    let assert_contract = |port: u16, stage: &str| {
        let search = call(
            port,
            "POST",
            "/lean/_search",
            Some(
                r#"{"query":{"match":{"body":"needle"}},"_source":true,"svindb":{"fields":["body","label","n"]}}"#,
            ),
        );
        assert_eq!(search.code, 200, "{stage}: search: {}", search.body);
        assert_eq!(ids(&search), ["a"], "{stage}: searchable postings потеряны");
        let search_json = search.json();
        let hit = &search_json["hits"][0];
        assert!(hit.get("_source").is_none(), "{stage}: source был сохранён: {hit}");
        assert_eq!(hit["fields"]["label"], "visible", "{stage}: store:true потерян");
        assert!(hit["fields"].get("body").is_none(), "{stage}: store:false materialized");
        assert!(hit["fields"].get("n").is_none(), "{stage}: numeric store:false materialized");

        let exact = call(
            port,
            "POST",
            "/lean/_search",
            Some(r#"{"query":{"term":{"n":42}},"_source":true}"#),
        );
        assert_eq!(ids(&exact), ["a"], "{stage}: numeric column не ищется: {}", exact.body);
        assert!(exact.json()["hits"][0].get("_source").is_none());

        let get = call(port, "GET", "/lean/_doc/a", None);
        assert_eq!(get.code, 200, "{stage}: GET: {}", get.body);
        assert!(get.json().get("_source").is_none(), "{stage}: GET вернул source");
        assert_eq!(get.json()["fields"]["label"], "visible");
        assert!(get.json()["fields"].get("body").is_none());
        assert!(get.json()["fields"].get("n").is_none());

        let highlight = call(
            port,
            "POST",
            "/lean/_search",
            Some(
                r#"{"query":{"match":{"body":"needle"}},"highlight":{"fields":["body"]},"_source":false}"#,
            ),
        );
        assert_eq!(highlight.code, 400, "{stage}: highlight без текста был принят");
        assert!(highlight.body.contains("store:true"), "{stage}: нет причины: {}", highlight.body);

        let stored_highlight = call(
            port,
            "POST",
            "/lean/_search",
            Some(
                r#"{"query":{"match":{"stored_text":"stored"}},"highlight":{"fields":["stored_text"]},"_source":false}"#,
            ),
        );
        assert_eq!(
            stored_highlight.code, 200,
            "{stage}: store:true highlight: {}",
            stored_highlight.body
        );
        assert!(
            stored_highlight.body.contains("<em>stored</em>"),
            "{stage}: store:true highlight потерян: {}",
            stored_highlight.body
        );

        let stats = call(port, "GET", "/lean/_stats", None);
        assert_eq!(stats.code, 200, "{stage}: stats: {}", stats.body);
        assert_eq!(stats.json()["schema"]["source_mode"], "disabled");
    };

    assert_contract(node.port, "segment");
    let mut schemas = Vec::new();
    files_named(std::path::Path::new(&node.root), "schema.json", &mut schemas);
    assert_eq!(schemas.len(), 1, "неоднозначный набор schema: {schemas:?}");
    let schema = std::fs::read_to_string(&schemas[0]).unwrap();
    assert!(!schema.contains("__svindb_source"), "source попал в schema: {schema}");
    assert!(!schema.contains("__svindb_projection"), "projection попал в schema: {schema}");
    let mut mappings = Vec::new();
    files_named(std::path::Path::new(&node.root), "mapping.json", &mut mappings);
    assert_eq!(mappings.len(), 1, "неоднозначный набор mapping: {mappings:?}");
    let persisted: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&mappings[0]).unwrap()).unwrap();
    assert_eq!(persisted["mappings"]["_source"]["enabled"], false);

    node.restart();
    assert_contract(node.port, "restart");
}

#[test]
fn source_arrays_sparse_fields_and_restart_are_lossless() {
    let mut node = start();
    create(node.port);
    let source = serde_json::json!({
        "exact": "a\u{1f}b",
        "folded": "MiXeD",
        "title": "Первый документ",
        "n": [7, 9],
        "source_only": {"unknown": [true, null, {"deep": 42}]},
        "changes": [
            {"path":"title","after_text":"Старое","details":[{"kind":"x","value":1}]},
            {"path":"about","after_text":"Новое","details":[{"kind":"y","value":2}]}
        ]
    });
    let reply = call(node.port, "PUT", "/contract/_doc/a", Some(&source.to_string()));
    assert_eq!(reply.code, 200, "put: {}", reply.body);

    let unknown_nested = serde_json::json!({
        "exact":"x", "folded":"x", "title":"x", "n":1,
        "changes":[{"path":"title", "alien":{"deep":42}}]
    });
    let rejected =
        call(node.port, "PUT", "/contract/_doc/unknown", Some(&unknown_nested.to_string()));
    assert_eq!(rejected.code, 400, "dynamic strict пропустил вложенное поле: {}", rejected.body);
    assert!(rejected.body.contains("changes[0].alien"), "отказ не назвал путь: {}", rejected.body);

    let sparse = serde_json::json!({"exact":"", "folded":"", "title":"", "n":11});
    assert_eq!(call(node.port, "PUT", "/contract/_doc/empty", Some(&sparse.to_string())).code, 200);

    for stage in ["buffer", "segment", "restart"] {
        if stage == "segment" {
            assert_eq!(call(node.port, "POST", "/contract/_flush", None).code, 200);
        } else if stage == "restart" {
            node.restart();
        }
        let got = call(node.port, "GET", "/contract/_doc/a", None);
        assert_eq!(got.code, 200, "{stage}: {}", got.body);
        assert_eq!(got.json()["_id"], "a", "{stage}: {}", got.body);
        assert_eq!(got.json()["_source"], source, "{stage}: source изменился");
        assert_eq!(got.json()["fields"]["n"], serde_json::json!([7, 9]));
        assert!(got.json()["fields"].get("optional_long").is_none());

        let mget = call(node.port, "POST", "/contract/_mget", Some(r#"{"ids":["a","empty"]}"#));
        assert_eq!(mget.code, 200, "{stage}: {}", mget.body);
        assert_eq!(mget.json()["docs"][0]["_source"], source);
    }

    let inner_same = call(
        node.port,
        "POST",
        "/contract/_search",
        Some(
            r#"{"query":{"nested":{"path":"changes.details","query":{"bool":{"must":[{"term":{"changes.details.kind":"x"}},{"term":{"changes.details.value":1}}]}}}},"_source":false}"#,
        ),
    );
    assert_eq!(inner_same.code, 200, "{}", inner_same.body);
    assert_eq!(ids(&inner_same), ["a"]);
    let inner_crossed = call(
        node.port,
        "POST",
        "/contract/_search",
        Some(
            r#"{"query":{"nested":{"path":"changes.details","query":{"bool":{"must":[{"term":{"changes.details.kind":"x"}},{"term":{"changes.details.value":2}}]}}}},"_source":false}"#,
        ),
    );
    assert!(ids(&inner_crossed).is_empty(), "внутренние slots склеились: {}", inner_crossed.body);

    let hierarchy_same = call(
        node.port,
        "POST",
        "/contract/_search",
        Some(
            r#"{"query":{"nested":{"path":"changes","query":{"bool":{"must":[{"term":{"changes.path":"title"}},{"nested":{"path":"changes.details","query":{"term":{"changes.details.kind":"x"}}}}]}}}},"_source":false}"#,
        ),
    );
    assert_eq!(hierarchy_same.code, 200, "{}", hierarchy_same.body);
    assert_eq!(ids(&hierarchy_same), ["a"]);
    let hierarchy_crossed = call(
        node.port,
        "POST",
        "/contract/_search",
        Some(
            r#"{"query":{"nested":{"path":"changes","query":{"bool":{"must":[{"term":{"changes.path":"title"}},{"nested":{"path":"changes.details","query":{"term":{"changes.details.kind":"y"}}}}]}}}},"_source":false}"#,
        ),
    );
    assert!(
        ids(&hierarchy_crossed).is_empty(),
        "parent-child связь потеряна: {}",
        hierarchy_crossed.body
    );

    // Массив `["a","b"]` и одна строка с прежним разделителем физически
    // различны: source не реконструируется из U+001F.
    let array = serde_json::json!({"exact":["a","b"],"folded":"z","title":"array","n":1});
    assert_eq!(call(node.port, "PUT", "/contract/_doc/array", Some(&array.to_string())).code, 200);
    assert_eq!(call(node.port, "GET", "/contract/_doc/array", None).json()["_source"], array);
    let source_only = call(node.port, "GET", "/contract/_doc/array?source_only=true", None);
    assert_eq!(source_only.code, 200, "{}", source_only.body);
    assert_eq!(source_only.json()["_source"], array);
    assert!(
        source_only.json().get("fields").is_none(),
        "source-only point read всё ещё строит declared fields: {}",
        source_only.body
    );
    assert_eq!(
        call(node.port, "GET", "/contract/_doc/array?source_only=maybe", None).code,
        400,
        "негодный source_only принят молча"
    );
    assert_ne!(
        call(node.port, "GET", "/contract/_doc/a", None).json()["_source"],
        call(node.port, "GET", "/contract/_doc/array", None).json()["_source"]
    );

    let long = "Ж".repeat(300);
    let long_source = serde_json::json!({"exact":long.clone(),"folded":"z","title":"long","n":2});
    assert_eq!(
        call(node.port, "PUT", "/contract/_doc/long", Some(&long_source.to_string())).code,
        200
    );
    let query = serde_json::json!({"query":{"term":{"exact":long}},"_source":false});
    let reply = call(node.port, "POST", "/contract/_search", Some(&query.to_string()));
    assert!(ids(&reply).is_empty(), "ignore_above не применён: {}", reply.body);
    assert_eq!(call(node.port, "GET", "/contract/_doc/long", None).json()["_source"], long_source);
}

#[test]
fn typed_queries_normalizers_nested_and_strict_parameters_are_exact() {
    let node = start();
    create(node.port);
    for (id, source) in [
        (
            "a",
            serde_json::json!({"exact":"https://e/Case/A","folded":"MiXeD","title":"Alpha beta","n":7,"price":1.234,
                "flag":true,"when":"2026-08-17T12:34:56.789Z","blob":"AQID","embedding":[1.0,0.0],
                "embedding_sparse":{"signal":2.0},
                "changes":[{"path":"title","after_text":"Старое"},{"path":"about","after_text":"Новое"}]}),
        ),
        (
            "b",
            serde_json::json!({"exact":"https://e/case/a","folded":"mixed","title":"Alpha","n":8,"optional_long":0,
                "embedding":[0.0,1.0],"embedding_sparse":{"other":2.0},
                "changes":[{"path":"title","after_text":"Другое"}]}),
        ),
        (
            "c",
            serde_json::json!({"exact":"other","folded":"Other","title":"beta","n":9,"nums":[9,10]}),
        ),
        ("d", serde_json::json!({"exact":"","folded":"","title":"","n":11,"optional_long":null})),
    ] {
        let reply =
            call(node.port, "PUT", &format!("/contract/_doc/{id}"), Some(&source.to_string()));
        assert_eq!(reply.code, 200, "{id}: {}", reply.body);
    }
    assert_eq!(call(node.port, "POST", "/contract/_flush", None).code, 200);

    let search = |body: &str| call(node.port, "POST", "/contract/_search", Some(body));
    assert_eq!(
        ids(&search(r#"{"query":{"term":{"exact":"https://e/Case/A"}},"_source":false}"#)),
        ["a"]
    );
    assert_eq!(
        ids(&search(r#"{"query":{"term":{"exact":"https://e/case/a"}},"_source":false}"#)),
        ["b"]
    );
    assert_eq!(
        ids(&search(r#"{"query":{"term":{"folded":"MIXED"}},"_source":false}"#)),
        ["a", "b"]
    );
    assert_eq!(ids(&search(r#"{"query":{"term":{"nums":10}},"_source":false}"#)), ["c"]);
    let numeric_range =
        search(r#"{"query":{"range":{"nums":{"gte":9,"lte":10}}},"_source":false}"#);
    assert_eq!(
        ids(&numeric_range),
        ["c"],
        "range обязан читать все значения numeric массива: {}",
        numeric_range.body
    );
    let numeric_stats = search(
        r#"{"size":0,"query":{"match_all":{}},"aggs":{"values":{"stats":{"field":"nums"}}}}"#,
    );
    assert_eq!(numeric_stats.code, 200, "numeric stats: {}", numeric_stats.body);
    assert_eq!(numeric_stats.json()["aggs"]["values"]["count"], 2, "{}", numeric_stats.body);
    assert_eq!(numeric_stats.json()["aggs"]["values"]["sum"], 19, "{}", numeric_stats.body);
    assert_eq!(ids(&search(r#"{"query":{"terms":{"n":[7,9,999]}},"_source":false}"#)), ["a", "c"]);
    assert_eq!(ids(&search(r#"{"query":{"term":{"flag":true}},"_source":false}"#)), ["a"]);
    assert_eq!(
        ids(&search(r#"{"query":{"term":{"when":"2026-08-17T12:34:56.789Z"}},"_source":false}"#)),
        ["a"]
    );
    assert_eq!(ids(&search(r#"{"query":{"term":{"blob":"AQID"}},"_source":false}"#)), ["a"]);
    assert_eq!(ids(&search(r#"{"query":{"term":{"price":1.23}},"_source":false}"#)), ["a"]);
    assert_eq!(ids(&search(r#"{"query":{"term":{"family":"message"}},"_source":false}"#)).len(), 4);

    let minimum = search(
        r#"{"query":{"bool":{"should":[{"term":{"folded":"mixed"}},{"match":{"title":"beta"}}],"minimum_should_match":2}},"_source":false}"#,
    );
    assert_eq!(ids(&minimum), ["a"]);
    let filtered =
        search(r#"{"query":{"bool":{"filter":[{"term":{"folded":"mixed"}}]}},"_source":false}"#);
    assert_eq!(ids(&filtered), ["a", "b"]);
    assert!(filtered.json()["hits"].as_array().unwrap().iter().all(|hit| hit["score"] == 0));

    // Elasticsearch mapping по умолчанию ставит store=false. Highlight всё
    // равно обязан работать: новый индекс берёт текст из той же атомарной
    // проекции, что GET/MGET и fields, а не требует скрытой второй копии leaf.
    let highlighted = search(
        r#"{"query":{"match":{"title":"beta"}},"highlight":{"fields":["title"],"size":80},"_source":false}"#,
    );
    assert_eq!(highlighted.code, 200, "{}", highlighted.body);
    assert!(
        highlighted.body.contains("<em>beta</em>"),
        "highlight из source-backed поля потерян: {}",
        highlighted.body
    );

    let vector = search(
        r#"{"k":1,"vector":{"field":"embedding","query":[1.0,0.0]},"fields":["title"],"source":true}"#,
    );
    assert_eq!(vector.code, 200, "{}", vector.body);
    assert_eq!(vector.json()["hits"][0]["_id"], "a", "{}", vector.body);
    assert_eq!(vector.json()["hits"][0]["fields"]["title"], "Alpha beta");
    assert_eq!(vector.json()["hits"][0]["_source"]["embedding"], serde_json::json!([1.0, 0.0]));

    let sparse_vector = search(
        r#"{"k":1,"sparse":{"field":"embedding_sparse","query":{"signal":1.0}},"ids":true}"#,
    );
    assert_eq!(sparse_vector.code, 200, "{}", sparse_vector.body);
    assert_eq!(sparse_vector.json()["hits"][0]["_id"], "a", "{}", sparse_vector.body);

    let same = search(
        r#"{"query":{"nested":{"path":"changes","query":{"bool":{"must":[{"term":{"changes.path":"title"}},{"match":{"changes.after_text":"Старое"}}]}}}},"_source":false}"#,
    );
    assert_eq!(ids(&same), ["a"]);
    let crossed = search(
        r#"{"query":{"nested":{"path":"changes","query":{"bool":{"must":[{"term":{"changes.path":"title"}},{"match":{"changes.after_text":"Новое"}}]}}}},"_source":false}"#,
    );
    assert!(ids(&crossed).is_empty(), "ложная связь элементов: {}", crossed.body);

    assert_eq!(ids(&search(r#"{"query":{"exists":{"field":"exact"}},"_source":false}"#)).len(), 4);
    assert_eq!(ids(&search(r#"{"query":{"exists":{"field":"title"}},"_source":false}"#)).len(), 4);
    assert_eq!(
        ids(&search(r#"{"query":{"exists":{"field":"optional_long"}},"_source":false}"#)),
        ["b"]
    );

    let sorted = search(
        r#"{"query":{"match_all":{}},"sort":[{"n":{"order":"desc"}}],"_source":false,"size":10}"#,
    );
    assert_eq!(sorted.code, 200, "{}", sorted.body);
    assert_eq!(ids(&sorted), ["d", "c", "b", "a"]);

    for key in ["post_filter", "terminate_after", "stored_fields"] {
        let body = format!(r#"{{"query":{{"match_all":{{}}}},"{key}":true}}"#);
        let reply = search(&body);
        assert_eq!(reply.code, 400, "{key} был проигнорирован: {}", reply.body);
        assert!(reply.body.contains(key), "{key} не назван: {}", reply.body);
    }
    let tracked = search(r#"{"query":{"match_all":{}},"track_total_hits":true,"_source":false}"#);
    assert_eq!(tracked.code, 200, "track_total_hits не поддержан: {}", tracked.body);
    assert_eq!(tracked.json()["total"]["value"], 4, "{}", tracked.body);
    assert_eq!(tracked.json()["total"]["relation"], "eq", "{}", tracked.body);
}
