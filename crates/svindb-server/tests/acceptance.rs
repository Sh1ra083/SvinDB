//! Приёмка: шесть гарантий, которых не хватало update-heavy control plane.
//!
//! Каждая проверка здесь названа по пункту исходного разбора и проверяет именно
//! его, а не «примерно то же». Файл существует затем, чтобы ответ на вопрос
//! «это правда исправлено?» не приходилось собирать из девяти разрозненных
//! тестов.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");
static UNIQUE: AtomicU64 = AtomicU64::new(1);

struct Node {
    child: Child,
    port: u16,
    root: String,
    log: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::fs::remove_dir_all(&self.root).ok();
        std::fs::remove_file(&self.log).ok();
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

fn start() -> Node {
    let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir()
        .join(format!("svindb-приёмка-{}-{unique}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let log = PathBuf::from(format!("{root}.server.log"));
    let output = std::fs::File::create(&log).unwrap();
    let child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args([
            "serve",
            &format!("{root}/index"),
            "0",
            "--single-index",
            "--fields=kind:keyword,revision:int64,state:keyword",
            "--flush=100000",
        ])
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap();
    let mut node = Node { child, port: 0, root, log };
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Some(status) = node.child.try_wait().unwrap() {
            panic!(
                "узел завершился до готовности: {status}; лог: {}",
                std::fs::read_to_string(&node.log).unwrap_or_default()
            );
        }
        if let Ok(output) = std::fs::read_to_string(&node.log) {
            if let Some(port) = output.lines().find_map(|line| {
                line.strip_prefix("SVINDB_LISTEN ")
                    .and_then(|address| address.parse::<std::net::SocketAddr>().ok())
                    .map(|address| address.port())
            }) {
                node.port = port;
                // Одного принятого TCP-соединения недостаточно: listener уже
                // существует, но HTTP runtime ещё может не обслуживать запросы.
                if healthy(port) {
                    return node;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "узел не стал готов за 20 секунд (порт {}); лог: {}",
        node.port,
        std::fs::read_to_string(&node.log).unwrap_or_default()
    );
}

fn healthy(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(250)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(250)));
    if stream
        .write_all(b"GET /_health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    if stream.read_to_string(&mut response).is_err() {
        return false;
    }
    let Some((header, body)) = response.split_once("\r\n\r\n") else {
        return false;
    };
    header.starts_with("HTTP/1.1 200 ")
        && serde_json::from_str::<serde_json::Value>(body)
            .map(|value| value["ok"] == true)
            .unwrap_or(false)
}

/// Профиль, при котором сброс на диск заведомо не случится сам: всё, что найдёт
/// поиск, найдено благодаря живому сегменту, а не публикации.
fn update_heavy(node: &Node) {
    let r = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{"refresh_interval_ms":100,"refresh_min_docs":1,
                "refresh_max_interval_ms":600000,"live_max_docs":50000,
                "flush_wait_ms":10000,"merge_policy":"update_heavy"}"#,
        ),
    );
    assert_eq!(r.code, 200, "настройки: {}", r.body);
}

fn put(port: u16, id: &str, revision: u32, expected: Option<u32>) -> Reply {
    let condition = match expected {
        Some(n) => {
            format!(r#","conditions":[{{"id":"{id}","field":"revision","equals":{n}}}]"#)
        }
        None => String::new(),
    };
    let body = format!(
        r#"{{"documents":[{{"id":"{id}","kind":"run","revision":{revision},
             "state":"running"}}]{condition}}}"#
    );
    call(port, "POST", "/_put", Some(&body))
}

/// Вся семья `run` одним запросом: столько же, сколько документов, и с полями.
fn family(port: u16) -> Reply {
    call(
        port,
        "POST",
        "/",
        Some(
            r#"{"k":1000,"ids":true,"fields":["revision","state"],
                "must":[{"field":"kind","terms":["run"]}]}"#,
        ),
    )
}

fn publish(node: &Node) {
    for _ in 0..60 {
        let r = call(node.port, "POST", "/_flush", None);
        assert!(r.code == 200 || r.code == 202, "flush: {}", r.body);
        if r.code == 200 {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("публикация не закончилась");
}

/// Пункт 1: точечное чтение по идентификатору всегда отдаёт последнюю
/// подтверждённую версию — сразу после ответа на запись, без всякого ожидания.
#[test]
fn point_read_always_returns_the_last_acknowledged_revision() {
    let node = start();
    update_heavy(&node);

    // Сто замен подряд, и после **каждой** проверяется, что точка отдаёт именно
    // ту версию, ответ на которую только что получен. Одна проверка в конце
    // ничего бы не значила: она попала бы в состояние, которое давно улеглось.
    for revision in 1..=100u32 {
        let expected = (revision > 1).then_some(revision - 1);
        let r = put(node.port, "parser_runs/current", revision, expected);
        assert_eq!(r.code, 200, "revision {revision}: {}", r.body);

        let doc = call(node.port, "GET", "/_doc/parser_runs%2Fcurrent", None);
        assert_eq!(doc.code, 200, "revision {revision}: {}", doc.body);
        assert_eq!(
            doc.json()["fields"]["revision"],
            revision,
            "точка отстала на замене {revision}: {}",
            doc.body
        );
    }

    // И после публикации — она же.
    publish(&node);
    let doc = call(node.port, "GET", "/_doc/parser_runs%2Fcurrent", None);
    assert_eq!(doc.json()["fields"]["revision"], 100, "публикация сбила точку: {}", doc.body);
}

/// Пункт 2: условие CAS сверяется с последней подтверждённой версией, а не с
/// тем, что видно поиску.
#[test]
fn cas_compares_against_the_authoritative_version() {
    let node = start();
    update_heavy(&node);

    assert_eq!(put(node.port, "lease", 1, None).code, 200);
    // Версия 1 опубликована, версия 2 — нет. Дальше условие «равно двум» обязано
    // пройти, хотя поиск в этот момент мог ещё показывать единицу.
    publish(&node);
    assert_eq!(put(node.port, "lease", 2, Some(1)).code, 200);

    let r = put(node.port, "lease", 3, Some(2));
    assert_eq!(r.code, 200, "CAS сверился с поисковым снимком, а не с версией 2: {}", r.body);
    // Устаревшее условие обязано получить конфликт — иначе «прошло» ничего не
    // значит: прошло бы и любое.
    let stale = put(node.port, "lease", 4, Some(2));
    assert_eq!(stale.code, 409, "устаревшее условие прошло: {}", stale.body);
    assert!(stale.body.contains("conflict"), "{}", stale.body);

    // Условие «версии нет» верно только для несуществующего документа.
    assert_eq!(put(node.port, "lease-2", 1, None).code, 200);
    let r = call(
        node.port,
        "POST",
        "/_put",
        Some(
            r#"{"documents":[{"id":"lease-2","kind":"run","revision":9,"state":"running"}],
                "conditions":[{"id":"lease-2","field":"revision","equals":null}]}"#,
        ),
    );
    assert_eq!(r.code, 409, "CAS не увидел уже принятую версию: {}", r.body);
}

/// Смешанная canonical mutation либо заменяет manifest и удаляет прежнюю
/// derived-проекцию целиком, либо при stale CAS не делает ничего.
#[test]
fn conditional_mutation_applies_upserts_and_deletes_as_one_transition() {
    let node = start();
    update_heavy(&node);
    assert_eq!(put(node.port, "manifest", 1, None).code, 200);
    assert_eq!(put(node.port, "relation-old", 1, None).code, 200);

    let changed = call(
        node.port,
        "POST",
        "/_mutate",
        Some(
            r#"{"documents":[{"id":"manifest","kind":"run","revision":2,"state":"running"},
                              {"id":"relation-new","kind":"run","revision":1,"state":"running"}],
                "deletes":["relation-old"],
                "conditions":[{"id":"manifest","field":"revision","equals":1},
                              {"id":"relation-new","field":"revision","equals":null}]}"#,
        ),
    );
    assert_eq!(changed.code, 200, "мутация не прошла: {}", changed.body);
    assert_eq!(changed.json()["added"], 2);
    assert_eq!(changed.json()["deleted"], 1);
    assert_eq!(
        changed.json()["durable"],
        true,
        "HTTP 200 смешанной mutation обязан явно подтверждать durable WAL: {}",
        changed.body
    );
    assert_eq!(call(node.port, "GET", "/_doc/relation-old", None).code, 404);
    assert_eq!(call(node.port, "GET", "/_doc/relation-new", None).code, 200);

    let stale = call(
        node.port,
        "POST",
        "/_mutate",
        Some(
            r#"{"documents":[{"id":"manifest","kind":"run","revision":3,"state":"running"}],
                "deletes":["relation-new"],
                "conditions":[{"id":"manifest","field":"revision","equals":1}]}"#,
        ),
    );
    assert_eq!(stale.code, 409, "stale мутация прошла: {}", stale.body);
    assert_eq!(call(node.port, "GET", "/_doc/relation-new", None).code, 200);
    assert_eq!(call(node.port, "GET", "/_doc/manifest", None).json()["fields"]["revision"], 2);
}

/// Пункт 3: старая физическая версия не вытесняет новую из ограниченной выдачи.
///
/// Проверяется на верхушке размером ровно в число документов семьи: будь у
/// какого-то `id` две живые версии, одна из них заняла бы место чужого
/// документа, и в выдаче кого-то бы не хватило.
#[test]
fn old_versions_never_crowd_out_the_new_one() {
    let node = start();
    update_heavy(&node);

    let ids: Vec<String> = (0..40).map(|n| format!("queue/{n:03}")).collect();
    for id in ids.iter() {
        assert_eq!(put(node.port, id, 1, None).code, 200);
    }
    publish(&node);

    for revision in 2..=15u32 {
        for id in ids.iter() {
            assert_eq!(put(node.port, id, revision, Some(revision - 1)).code, 200);
        }
        // Верхушка ровно по числу документов: лишняя живая версия вытеснила бы
        // чей-то документ, и он бы пропал.
        let r = call(
            node.port,
            "POST",
            "/",
            Some(&format!(
                r#"{{"k":{},"ids":true,"fields":["revision"],
                     "must":[{{"field":"kind","terms":["run"]}}]}}"#,
                ids.len()
            )),
        );
        assert_eq!(r.code, 200, "{}", r.body);
        let hits = r.json()["hits"].as_array().cloned().unwrap_or_default();
        let mut seen: Vec<String> =
            hits.iter().map(|h| h["id"].as_str().unwrap_or("").to_string()).collect();
        seen.sort();
        let mut unique = seen.clone();
        unique.dedup();
        assert_eq!(
            seen.len(),
            unique.len(),
            "на замене {revision} один id пришёл дважды: {}",
            r.body
        );
        assert_eq!(
            unique.len(),
            ids.len(),
            "на замене {revision} в верхушке не все документы семьи: {}",
            r.body
        );
    }
}

/// Пункт 4: обход семьи не колеблется между старыми и новыми версиями.
///
/// Колебание — это когда одна и та же выдача то показывает документ, то нет,
/// либо показывает версию старее уже показанной. Проверяется по каждому `id`
/// отдельно и на всём потоке замен, а не на его итоге.
#[test]
fn a_family_scan_never_oscillates() {
    let node = start();
    update_heavy(&node);

    let ids: Vec<String> = (0..25).map(|n| format!("pool/{n:03}")).collect();
    for id in ids.iter() {
        assert_eq!(put(node.port, id, 1, None).code, 200);
    }
    publish(&node);

    let mut best: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut probes = 0usize;
    for revision in 2..=20u32 {
        for id in ids.iter() {
            assert_eq!(put(node.port, id, revision, Some(revision - 1)).code, 200);
        }
        for _ in 0..3 {
            let r = family(node.port);
            assert_eq!(r.code, 200, "{}", r.body);
            let hits = r.json()["hits"].as_array().cloned().unwrap_or_default();
            assert_eq!(
                hits.len(),
                ids.len(),
                "семья потеряла документы на замене {revision}: {}",
                r.body
            );
            for h in hits.iter() {
                let id = h["id"].as_str().unwrap_or_default().to_string();
                let got = h["fields"]["revision"].as_i64().unwrap_or(-1);
                let was = best.entry(id.clone()).or_insert(0);
                assert!(got >= *was, "{id}: выдача пошла назад — было {was}, стало {got}");
                *was = got;
            }
            probes += 1;
        }
    }
    assert!(probes >= 50, "проб вышло {probes} — проверять почти нечего");
}

/// Пункт 5: после `/_flush` со `200` всё подтверждённое видно поиску.
#[test]
fn flush_gives_a_visibility_guarantee() {
    let node = start();
    update_heavy(&node);

    for n in 0..50u32 {
        assert_eq!(put(node.port, &format!("job/{n:03}"), 1, None).code, 200);
    }
    // До публикации свежесть даёт живой сегмент, но гарантию — только flush.
    publish(&node);

    let r = family(node.port);
    let hits = r.json()["hits"].as_array().cloned().unwrap_or_default();
    assert_eq!(hits.len(), 50, "после flush видно не всё принятое: {}", r.body);

    // Замены поверх опубликованного — та же гарантия.
    for n in 0..50u32 {
        assert_eq!(put(node.port, &format!("job/{n:03}"), 2, Some(1)).code, 200);
    }
    publish(&node);
    let r = family(node.port);
    let hits = r.json()["hits"].as_array().cloned().unwrap_or_default();
    assert_eq!(hits.len(), 50, "после flush замен видно не всё: {}", r.body);
    for h in hits.iter() {
        assert_eq!(h["fields"]["revision"], 2, "после flush видна старая версия: {}", r.body);
    }
}

/// Пункт 6: точечное чтение по идентификатору есть и не зависит от публикации —
/// ни поодиночке, ни набором.
#[test]
fn point_reads_do_not_depend_on_publication() {
    let node = start();
    update_heavy(&node);

    // Половина набора опубликована, половина — только принята.
    for n in 0..10u32 {
        assert_eq!(put(node.port, &format!("acc/{n:02}"), 1, None).code, 200);
    }
    publish(&node);
    for n in 10..20u32 {
        assert_eq!(put(node.port, &format!("acc/{n:02}"), 1, None).code, 200);
    }

    let ids: Vec<String> = (0..20).map(|n| format!("\"acc/{n:02}\"")).collect();
    let r = call(node.port, "POST", "/_mget", Some(&format!(r#"{{"ids":[{}]}}"#, ids.join(","))));
    assert_eq!(r.code, 200, "{}", r.body);
    let docs = r.json()["docs"].as_array().cloned().unwrap_or_default();
    assert_eq!(docs.len(), 20, "набор пришёл не целиком: {}", r.body);
    let found = docs.iter().filter(|d| d["found"] == true).count();
    assert_eq!(found, 20, "часть набора не прочиталась: {}", r.body);
    // Половина ответов обязана быть из буфера — иначе проверка ничего не стоит:
    // она прошла бы и на полностью опубликованном наборе.
    let buffered = docs.iter().filter(|d| d["buffered"] == true).count();
    assert_eq!(buffered, 10, "непубликованные документы читались не из буфера: {}", r.body);

    // Поодиночке — то же самое.
    for n in 0..20u32 {
        let doc = call(node.port, "GET", &format!("/_doc/acc%2F{n:02}"), None);
        assert_eq!(doc.code, 200, "acc/{n:02}: {}", doc.body);
        assert_eq!(doc.json()["fields"]["revision"], 1, "acc/{n:02}: {}", doc.body);
    }
}

/// PIT замораживает и состав сегментов, и live bitmap: delete/update между
/// страницами не должен ни потерять старый документ, ни подмешать новый.
#[test]
fn pit_and_search_after_resume_one_immutable_read_view() {
    let node = start();
    update_heavy(&node);
    let original: Vec<String> = (0..20).map(|n| format!("pit/{n:02}")).collect();
    for (revision, id) in original.iter().enumerate() {
        assert_eq!(put(node.port, id, revision as u32, None).code, 200, "{id}");
    }
    publish(&node);

    let first_body = serde_json::json!({
        "k": 5,
        "ids": true,
        "must": [{"field":"kind","terms":["run"]}],
        "sort": [{"field":"revision","order":"asc"}],
        "pit": {"keep_alive_ms": 5_000}
    });
    let first = call(node.port, "POST", "/", Some(&first_body.to_string()));
    assert_eq!(first.code, 200, "{}", first.body);
    let first_json = first.json();
    let pit = first_json["pit"].as_str().expect("ответ не вернул PIT").to_string();
    let mut seen: Vec<String> = first_json["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .filter_map(|hit| hit["id"].as_str().map(str::to_string))
        .collect();
    let mut cursor = first_json["hits"].as_array().unwrap().last().unwrap()["sort"].clone();

    // Текущий ReadView меняется радикально: один ещё не показанный документ
    // гасится, а новый попадает внутрь диапазона следующих страниц.
    let deleted = call(node.port, "POST", "/_delete", Some(r#"{"ids":["pit/10"]}"#));
    assert_eq!(deleted.code, 200, "{}", deleted.body);
    assert_eq!(put(node.port, "pit/new", 6, None).code, 200);

    loop {
        let body = serde_json::json!({
            "k": 5,
            "ids": true,
            "must": [{"field":"kind","terms":["run"]}],
            "sort": [{"field":"revision","order":"asc"}],
            "search_after": cursor,
            "pit": {"id": pit, "keep_alive_ms": 5_000}
        });
        let page = call(node.port, "POST", "/", Some(&body.to_string()));
        assert_eq!(page.code, 200, "{}", page.body);
        let page = page.json();
        let hits = page["hits"].as_array().expect("hits");
        if hits.is_empty() {
            break;
        }
        seen.extend(hits.iter().filter_map(|hit| hit["id"].as_str().map(str::to_string)));
        cursor = hits.last().unwrap()["sort"].clone();
    }
    assert_eq!(seen, original, "PIT смешал поколения ReadView");
    assert!(!seen.iter().any(|id| id == "pit/new"));

    // Истёкший PIT не переходит молча на текущий состав.
    let short = serde_json::json!({
        "k": 1,
        "must": [{"field":"kind","terms":["run"]}],
        "pit": {"keep_alive_ms": 20}
    });
    let short = call(node.port, "POST", "/", Some(&short.to_string()));
    let expired = short.json()["pit"].as_str().expect("короткий PIT").to_string();
    std::thread::sleep(Duration::from_millis(60));
    let retry = serde_json::json!({
        "k": 1,
        "must": [{"field":"kind","terms":["run"]}],
        "pit": {"id": expired, "keep_alive_ms": 20}
    });
    let retry = call(node.port, "POST", "/", Some(&retry.to_string()));
    assert_eq!(retry.code, 404, "истёкший PIT молча заменён текущим: {}", retry.body);
}

/// Prefix expansion имеет два явных глобальных бюджета и никогда не выдаёт
/// усечённое раскрытие за полный результат.
#[test]
fn prefix_expansion_reports_examined_and_accepted_budgets() {
    let node = start();
    let body = r#"{"documents":[
        {"id":"prefix/1","kind":"run","revision":1,"state":"running"},
        {"id":"prefix/2","kind":"run","revision":2,"state":"ready"},
        {"id":"prefix/3","kind":"run","revision":3,"state":"retry"}
    ]}"#;
    assert_eq!(call(node.port, "POST", "/_bulk", Some(body)).code, 200);
    publish(&node);

    let bounded = r#"{"k":10,"words":[{"field":"state","terms":["r*"]}],
        "max_expansions":1,"max_expansion_examined":1}"#;
    let reply = call(node.port, "POST", "/", Some(bounded));
    assert_eq!(reply.code, 200, "{}", reply.body);
    let value = reply.json();
    assert_eq!(value["partial"], true, "усечённый prefix назван полным: {}", reply.body);
    assert_eq!(value["terminated"], "expansion_budget", "{}", reply.body);
    assert!(value["expansion"]["examined"].as_u64().unwrap_or(u64::MAX) <= 1);
    assert!(value["expansion"]["accepted"].as_u64().unwrap_or(u64::MAX) <= 1);

    let strict = call(
        node.port,
        "POST",
        "/",
        Some(
            r#"{"k":10,"words":[{"field":"state","terms":["r*"]}],
                "max_expansions":1,"max_expansion_examined":1,"partial":false}"#,
        ),
    );
    assert_eq!(strict.code, 503, "строгий prefix принял partial: {}", strict.body);

    let complete = call(
        node.port,
        "POST",
        "/",
        Some(
            r#"{"k":10,"words":[{"field":"state","terms":["r*"]}],
                "max_expansions":10,"max_expansion_examined":100}"#,
        ),
    );
    assert_eq!(complete.code, 200, "{}", complete.body);
    assert_eq!(complete.json()["complete"], true, "{}", complete.body);
    assert_eq!(complete.json()["hits"].as_array().map(Vec::len), Some(3), "{}", complete.body);
}

#[test]
fn total_hits_contract_supports_exact_and_bounded_count_without_implicit_count() {
    let node = start();
    for revision in 0..5u32 {
        assert_eq!(put(node.port, &format!("count/{revision}"), revision, None).code, 200);
    }
    publish(&node);
    let query = |track: &str| {
        let body = format!(
            r#"{{"k":2,"must":[{{"field":"kind","terms":["run"]}}],"track_total_hits":{track}}}"#
        );
        call(node.port, "POST", "/", Some(&body))
    };
    let exact = query("true");
    assert_eq!(exact.code, 200, "{}", exact.body);
    assert_eq!(exact.json()["total"]["value"], 5, "{}", exact.body);
    assert_eq!(exact.json()["total"]["relation"], "eq", "{}", exact.body);
    assert_eq!(exact.json()["hits"].as_array().map(Vec::len), Some(2), "{}", exact.body);

    let bounded = query("2");
    assert_eq!(bounded.code, 200, "{}", bounded.body);
    assert_eq!(bounded.json()["total"]["value"], 2, "{}", bounded.body);
    assert_eq!(bounded.json()["total"]["relation"], "gte", "{}", bounded.body);

    let omitted = query("false");
    assert_eq!(omitted.code, 200, "{}", omitted.body);
    assert!(
        omitted.json().get("total").is_none(),
        "false всё ещё запускает count: {}",
        omitted.body
    );
}

/// Свежесть без публикации: то, ради чего заводился живой сегмент.
///
/// Не входит в исходные шесть пунктов — это то, что было выбрано сверх них.
#[test]
fn writes_reach_search_without_touching_disk() {
    let node = start();
    update_heavy(&node);

    let segments = || -> u64 {
        call(node.port, "GET", "/_health", None).json()["segments"].as_u64().unwrap_or(0)
    };
    let before = segments();

    assert_eq!(put(node.port, "run/свежий", 1, None).code, 200);
    let began = Instant::now();
    let mut seen = false;
    while began.elapsed() < Duration::from_secs(3) {
        if family(node.port).json()["hits"].as_array().is_some_and(|h| h.len() == 1) {
            seen = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(seen, "запись не дошла до поиска за три секунды");
    assert!(
        began.elapsed() < Duration::from_secs(1),
        "запись шла до поиска {:?} — это уже не свежесть",
        began.elapsed()
    );

    // Число сегментов выросло ровно на живой: на диск ничего не легло.
    assert_eq!(segments(), before + 1, "видимость обошлась сегментом на диске");
    let inspect = call(node.port, "GET", "/_inspect", None);
    assert_eq!(inspect.code, 200, "{}", inspect.body);
    assert_eq!(inspect.json()["live"]["segments"], 1, "{}", inspect.body);
    assert_eq!(inspect.json()["live"]["docs"], 1, "{}", inspect.body);
    assert_eq!(
        inspect.json()["segments"],
        0,
        "на диске всё-таки появился сегмент: {}",
        inspect.body
    );
}

/// На пределе keep-alive узел обязан **предупредить**, а не молча закрыть.
///
/// ## Что было
///
/// Работник обслуживает соединение циклом в сто запросов — чтобы один клиент
/// не занимал место вечно. Но признак `keep` считался только по заголовку
/// клиента и о пределе не знал: сотый ответ уходил с `Connection: keep-alive`,
/// после чего узел закрывал сокет. Клиент, поверивший заголовку, посылал сто
/// первый запрос в уже закрытое соединение и получал обрыв.
///
/// На стенде это давало ровно двадцать четыре `Broken pipe` за пятнадцать
/// миллионов документов: три тысячи запросов на восемь потоков — по триста
/// семьдесят пять на соединение, по три закрытия на каждом. Документы не
/// терялись только потому, что загрузчик повторял пачку; клиент без повторов
/// потерял бы их молча.
///
/// Проверяется весь предел целиком, а не первые два ответа: ошибка была ровно
/// на границе, и проверка, не доходящая до неё, прошла бы и на сломанном коде.
#[test]
fn the_last_answer_on_a_connection_says_it_is_closing() {
    let node = start();
    let mut socket = TcpStream::connect(("127.0.0.1", node.port)).unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut input = BufReader::new(socket.try_clone().unwrap());

    let mut closing_at = None;
    for i in 0..100 {
        write!(
            socket,
            "GET /_health HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"
        )
        .unwrap();
        socket.flush().unwrap();

        let mut status = String::new();
        input.read_line(&mut status).unwrap();
        assert!(status.contains(" 200 "), "запрос {i} на живом соединении не удался: {status:?}");
        let (mut len, mut closes) = (None, false);
        loop {
            let mut line = String::new();
            if input.read_line(&mut line).unwrap() == 0 || line.trim().is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    len = value.trim().parse::<usize>().ok();
                } else if name.eq_ignore_ascii_case("connection")
                    && value.trim().eq_ignore_ascii_case("close")
                {
                    closes = true;
                }
            }
        }
        let mut bytes = vec![0; len.unwrap_or(0)];
        input.read_exact(&mut bytes).unwrap();
        if closes && closing_at.is_none() {
            closing_at = Some(i);
        }
    }
    assert_eq!(
        closing_at,
        Some(99),
        "узел закрывает соединение на сотом ответе — сказать об этом он обязан \
         именно там: раньше значит рвать без нужды, позже значит не сказать вовсе"
    );
}

/// Гашение не обязано лечь сегментом — а `/_flush` обязан это знать.
///
/// ## Что здесь ловится
///
/// `flushed_seq` двигает только легший сегмент; `seq` двигает **всякая** запись
/// журнала, в том числе гашение, у которого документа нет. Пока `/_flush` ждал
/// `flushed_seq >= seq`, после `/_delete` при пустом буфере он ждал события,
/// которое не наступит: узел навсегда отвечал `202`, поток публикации крутился
/// с шагом 5 мс, занимая ядро, а `sync_live` — единственное, что делает правку
/// карты живых долговечной, — не звался ни разу.
///
/// Проверяется не «ответ пришёл», а **и код, и повторяемость**: одиночный `202`
/// законен (сброс мог идти), поэтому утверждение стоит на том, что `200`
/// наступает, и наступает снова после второго гашения.
#[test]
fn a_delete_does_not_wedge_the_flush_forever() {
    let node = start();
    update_heavy(&node);
    for n in 0..20u32 {
        assert_eq!(put(node.port, &format!("job/{n:03}"), 1, None).code, 200);
    }
    publish(&node);

    // Гашение — и сразу сброс, при заведомо пустом буфере.
    let r = call(node.port, "POST", "/_delete", Some(r#"{"ids":["job/000"]}"#));
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(r.json()["deleted"].as_i64(), Some(1), "{}", r.body);

    // `publish` сам паникует, если `200` не наступил за шесть секунд.
    publish(&node);

    // И это не разовая удача: второе гашение проходит так же.
    let r = call(node.port, "POST", "/_delete", Some(r#"{"ids":["job/001"]}"#));
    assert_eq!(r.code, 200, "{}", r.body);
    publish(&node);

    // Погашенное действительно ушло, а не просто «сброс ответил».
    let r = family(node.port);
    let hits = r.json()["hits"].as_array().cloned().unwrap_or_default();
    assert_eq!(hits.len(), 18, "после двух гашений видно не то: {}", r.body);
}
