//! Видимость одиночного документа при частой замене.
//!
//! Сценарий control plane: один документ-синглтон (`parser_runs/current`)
//! обновляется чаще, чем идёт публикация. Между заменой и публикацией поиск
//! обязан отдавать **какую-то** версию документа — старую или новую, — но не
//! пустоту: на пустоте админка видит `stopped` там, где воркер видит `running`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_svindb");
static UNIQUE: AtomicU64 = AtomicU64::new(1);

struct Node {
    child: Child,
    port: u16,
    root: String,
    flush_docs: u32,
}

impl Node {
    /// Обрывает узел, не давая ему закрыться: всё, что переживёт это, обязано
    /// быть в журнале, а не в чьей-то памяти.
    fn restart(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.child = launch(&format!("{}/index", self.root), self.port, self.flush_docs);
        for _ in 0..200 {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("узел не поднялся после перезапуска");
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

fn launch(index: &str, port: u16, flush_docs: u32) -> Child {
    launch_with(index, port, flush_docs, None)
}

fn launch_with(index: &str, port: u16, flush_docs: u32, config: Option<&str>) -> Child {
    let mut args = vec![
        "serve".to_string(),
        index.to_string(),
        port.to_string(),
        "--single-index".to_string(),
        "--fields=revision:int64,state:keyword".to_string(),
        format!("--flush={flush_docs}"),
    ];
    if let Some(path) = config {
        args.push(format!("--config={path}"));
    }
    Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(&args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

/// Узел, которому разрешено снимать снимки в подкаталог своего стенда.
fn start_with_snapshots() -> (Node, String) {
    let port = free_port();
    let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir()
        .join(format!("svindb-singleton-{}-{port}-{unique}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let dest = format!("{root}/снимки");
    std::fs::create_dir_all(&dest).unwrap();
    let config = format!("{root}/config.json");
    let real = std::fs::canonicalize(&dest).unwrap().to_string_lossy().into_owned();
    std::fs::write(
        &config,
        format!(r#"{{"snapshot":{{"allow":[{}]}}}}"#, serde_json::to_string(&real).unwrap()),
    )
    .unwrap();
    let child = launch_with(&format!("{root}/index"), port, 100000, Some(&config));
    let mut node = Node { child, port, root, flush_docs: 100000 };
    for _ in 0..200 {
        if let Ok(Some(status)) = node.child.try_wait() {
            panic!("узел завершился до открытия порта: {status}");
        }
        if TcpStream::connect(("127.0.0.1", node.port)).is_ok() {
            return (node, real);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("узел не открыл порт {}", node.port);
}

fn start() -> Node {
    start_with(100000)
}

fn start_with(flush_docs: u32) -> Node {
    let port = free_port();
    let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir()
        .join(format!("svindb-singleton-{}-{port}-{unique}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let child = launch(&format!("{root}/index"), port, flush_docs);
    let mut node = Node { child, port, root, flush_docs };
    for _ in 0..200 {
        if let Ok(Some(status)) = node.child.try_wait() {
            panic!("узел завершился до открытия порта: {status}");
        }
        if TcpStream::connect(("127.0.0.1", node.port)).is_ok() {
            return node;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("узел не открыл порт {}", node.port);
}

/// Поиск синглтона: фильтр по наличию `revision`, других документов в индексе нет.
fn search(port: u16) -> Reply {
    call(
        port,
        "POST",
        "/",
        Some(
            r#"{"k":10,"ids":true,"fields":["revision","state"],"filter":[{"field":"revision"}]}"#,
        ),
    )
}

fn hits(port: u16) -> (usize, String) {
    let reply = search(port);
    assert_eq!(reply.code, 200, "поиск: {}", reply.body);
    let value: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
    let n = value["hits"].as_array().map_or(0, |h| h.len());
    (n, reply.body)
}

/// Публикует принятое и ждёт, пока фоновый писатель ляжет.
fn publish(node: &Node) {
    for _ in 0..30 {
        let flush = call(node.port, "POST", "/_flush", None);
        assert!(flush.code == 200 || flush.code == 202, "flush: {}", flush.body);
        if flush.code == 200 {
            std::thread::sleep(Duration::from_millis(150));
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("публикация не закончилась");
}

fn put(port: u16, revision: u32, expected: Option<u32>) -> Reply {
    let condition = match expected {
        Some(n) => format!(
            r#","conditions":[{{"id":"parser_runs/current","field":"revision","equals":{n}}}]"#
        ),
        None => String::new(),
    };
    let body = format!(
        r#"{{"documents":[{{"id":"parser_runs/current","revision":{revision},"state":"running"}}]{condition}}}"#
    );
    call(port, "POST", "/_put", Some(&body))
}

#[test]
fn singleton_never_disappears_from_search_between_updates() {
    let node = start();
    // Публикация отложена намеренно: так работает update-heavy профиль, где
    // /_flush после каждого heartbeat запрещён из-за шторма сегментов.
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{"refresh_interval_ms":100,"refresh_min_docs":1,
                "refresh_max_interval_ms":60000,"flush_wait_ms":5000}"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    // Первая версия публикуется явно: дальше проверяется именно замена.
    assert_eq!(put(node.port, 1, None).code, 200);
    let flush = call(node.port, "POST", "/_flush", None);
    assert!(flush.code == 200 || flush.code == 202, "flush: {}", flush.body);
    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "опубликованный синглтон не нашёлся: {body}");

    // Дальше — замены без flush, как их делает воркер.
    let mut blind = Vec::new();
    for revision in 2..=12u32 {
        assert_eq!(put(node.port, revision, Some(revision - 1)).code, 200);
        for probe in 0..3 {
            let (n, body) = hits(node.port);
            if n == 0 {
                blind.push(format!("revision {revision}, попытка {probe}: {body}"));
            }
        }
    }

    // Точка чтения по идентификатору для сравнения: она смотрит в буфер.
    let doc = call(node.port, "GET", "/_doc/parser_runs%2Fcurrent", None);

    assert!(
        blind.is_empty(),
        "документ пропадал из поиска {} раз(а) при живом /_doc ({} {}):\n{}",
        blind.len(),
        doc.code,
        doc.body,
        blind.join("\n")
    );
}

/// Гашение старой версии идёт по **текущему** набору сегментов. Сегмент,
/// отданный фоновому писателю, в набор ещё не внесён, и версия внутри него
/// гашения не получает. Когда он ляжет, у одного `id` окажется две живые
/// физические версии, и выдача начнёт колебаться между ними.
#[test]
fn replacement_leaves_no_second_live_version() {
    // Порог сегмента низкий: сброс случается посреди потока замен, как и на
    // настоящем update-heavy индексе, только чаще.
    let node = start_with(4);
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(r#"{"refresh_interval_ms":50,"refresh_min_docs":1,"flush_wait_ms":5000}"#),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    assert_eq!(put(node.port, 1, None).code, 200);
    for revision in 2..=60u32 {
        let reply = put(node.port, revision, Some(revision - 1));
        assert_eq!(reply.code, 200, "revision {revision}: {}", reply.body);
    }

    // Публикуем всё принятое и даём фоновому писателю лечь.
    for _ in 0..20 {
        let flush = call(node.port, "POST", "/_flush", None);
        assert!(flush.code == 200 || flush.code == 202, "flush: {}", flush.body);
        if flush.code == 200 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_millis(300));

    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "у одного id живых версий не одна: {body}");
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        value["hits"][0]["fields"]["revision"], 60,
        "поиск отдал не последнюю версию: {body}"
    );
}

/// Выдача по одному `id` не должна ходить назад: увидев revision N, следующий
/// запрос обязан увидеть N или больше, но не N-1. Проверяется прямо во время
/// потока замен, а не после того, как всё улеглось.
#[test]
fn search_revision_never_goes_backwards() {
    let node = start_with(4);
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(r#"{"refresh_interval_ms":50,"refresh_min_docs":1,"flush_wait_ms":2000}"#),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    assert_eq!(put(node.port, 1, None).code, 200);
    // Первую версию публикуем явно: пока не лёг ни один сегмент, поиск не знает
    // схему и отвергает фильтр по объявленному полю.
    publish(&node);
    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "первая версия не опубликовалась: {body}");

    let mut best = 0i64;
    let mut back = Vec::new();
    let mut doubled = Vec::new();
    let mut blind = 0usize;
    for revision in 2..=80u32 {
        let written = put(node.port, revision, Some(revision - 1));
        assert_eq!(written.code, 200, "revision={revision}: {}", written.body);
        let (n, body) = hits(node.port);
        if n > 1 {
            doubled.push(format!("после revision {revision}: {body}"));
        }
        if n == 0 {
            blind += 1;
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        let seen = value["hits"][0]["fields"]["revision"].as_i64().unwrap_or(-1);
        if seen < best {
            back.push(format!("после revision {revision}: видели {best}, стало {seen}"));
        }
        best = best.max(seen);
    }
    assert!(doubled.is_empty(), "две живые версии одного id:\n{}", doubled.join("\n"));
    assert!(back.is_empty(), "выдача поиска пошла назад:\n{}", back.join("\n"));
    assert_eq!(
        blind, 0,
        "документ был невидим поиску на {blind} из 79 замен — монотонность проверять не на чем"
    );
}

/// Пакетное чтение набора синглтонов: то, чем control plane сверяет dashboard
/// и пулы аккаунтов, не завися от refresh.
#[test]
fn mget_reads_the_whole_set_including_unpublished() {
    let node = start();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{"refresh_interval_ms":100,"refresh_min_docs":1,
                "refresh_max_interval_ms":60000,"flush_wait_ms":5000}"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    // Первый документ публикуется, второй остаётся в буфере: набор нарочно
    // собран из разных поколений.
    let landed = r#"{"documents":[{"id":"pool/a","revision":7,"state":"idle"}]}"#;
    assert_eq!(call(node.port, "POST", "/_put", Some(landed)).code, 200);
    publish(&node);
    let buffered = r#"{"documents":[{"id":"pool/b","revision":9,"state":"running"}]}"#;
    assert_eq!(call(node.port, "POST", "/_put", Some(buffered)).code, 200);

    let reply =
        call(node.port, "POST", "/_mget", Some(r#"{"ids":["pool/a","pool/b","pool/нет-такого"]}"#));
    assert_eq!(reply.code, 200, "mget: {}", reply.body);
    let v: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
    let docs = v["docs"].as_array().expect("docs");
    assert_eq!(docs.len(), 3, "порядок и длина набора: {}", reply.body);

    assert_eq!(docs[0]["id"], "pool/a");
    assert_eq!(docs[0]["found"], true);
    assert_eq!(docs[0]["fields"]["revision"], 7, "{}", reply.body);
    assert!(docs[0]["doc"].is_number(), "опубликованный без номера: {}", reply.body);

    assert_eq!(docs[1]["id"], "pool/b");
    assert_eq!(docs[1]["found"], true, "непубликованный не прочитался: {}", reply.body);
    assert_eq!(docs[1]["buffered"], true, "{}", reply.body);
    assert_eq!(docs[1]["fields"]["revision"], 9, "{}", reply.body);

    // Отсутствующий остаётся на своём месте, а не выпадает из набора.
    assert_eq!(docs[2]["id"], "pool/нет-такого");
    assert_eq!(docs[2]["found"], false, "{}", reply.body);

    // Повторные ID остаются отдельными позициями и после дырки отсутствия.
    let repeated = call(
        node.port,
        "POST",
        "/_mget",
        Some(r#"{"ids":["pool/b","pool/нет-такого","pool/b","pool/a","pool/b"]}"#),
    );
    assert_eq!(repeated.code, 200, "{}", repeated.body);
    let repeated: serde_json::Value = serde_json::from_str(&repeated.body).unwrap();
    for at in [0, 2, 4] {
        assert_eq!(repeated["docs"][at]["id"], "pool/b");
        assert_eq!(repeated["docs"][at]["fields"]["revision"], 9);
    }
    assert_eq!(repeated["docs"][1]["found"], false);
    assert_eq!(repeated["docs"][3]["fields"]["revision"], 7);

    let memory_only = call(node.port, "POST", "/_mget", Some(r#"{"ids":["pool/b","pool/b"]}"#));
    assert_eq!(memory_only.code, 200, "{}", memory_only.body);
    let memory_only: serde_json::Value = serde_json::from_str(&memory_only.body).unwrap();
    assert_eq!(memory_only["docs"].as_array().unwrap().len(), 2);
    for doc in memory_only["docs"].as_array().unwrap() {
        assert_eq!(doc["fields"]["revision"], 9);
        assert_eq!(doc["found"], true);
    }

    // Пустой идентификатор адресует все документы без id — и потому запрещён.
    let empty = call(node.port, "POST", "/_mget", Some(r#"{"ids":[""]}"#));
    assert_eq!(empty.code, 400, "пустой id принят: {}", empty.body);
    let broken = call(node.port, "POST", "/_mget", Some(r#"{"нет":1}"#));
    assert_eq!(broken.code, 400, "тело без ids принято: {}", broken.body);
}

/// Схема запроса берётся у объявленного реестра, а не только у легших сегментов.
///
/// Пока она бралась у сегментов, узел с только что заведённым индексом отвечал
/// на фильтр по **объявленному** полю `400 «поля нет — объявлены title, body»`.
/// То есть ровно в тот промежуток, в котором клиент поднимает индекс и шлёт
/// первую пачку, поиск отрицал схему, которую сам же и объявил.
#[test]
fn declared_field_is_queryable_before_the_first_segment() {
    let node = start();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{"refresh_interval_ms":100,"refresh_min_docs":10000,
                "refresh_max_interval_ms":60000,"flush_wait_ms":5000}"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    // Ни одного сегмента: спрашивать схему у них нечего.
    let cold = search(node.port);
    assert_eq!(cold.code, 200, "холодный индекс отверг объявленное поле: {}", cold.body);

    // Принято, но не опубликовано: состав по-прежнему пуст.
    assert_eq!(put(node.port, 1, None).code, 200);
    let buffered = search(node.port);
    assert_eq!(buffered.code, 200, "непустой буфер не помог составу: {}", buffered.body);

    // Тот же запрос после публикации находит документ — значит проверялось
    // именно поле, а не отсутствие индекса.
    publish(&node);
    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "после публикации запрос перестал находить: {body}");

    // Незаявленное поле по-прежнему отвергается: послабление коснулось
    // объявленного, а не всякого имени.
    let unknown = call(node.port, "POST", "/", Some(r#"{"k":10,"filter":[{"field":"нетполя"}]}"#));
    assert_eq!(unknown.code, 400, "незаявленное поле приняли: {}", unknown.body);
}

/// Отложенное гашение обязано пережить обрыв узла.
///
/// Прежние версии гасятся не при записи, а при публикации. Значит в момент
/// обрыва на диске может лежать опубликованная старая версия, чья замена
/// существует только в журнале. Подъём обязан свести их к одной — новой.
/// Ошибись он, и после перезапуска у документа оказалось бы две живые версии
/// либо ни одной; и то и другое молча.
#[test]
fn deferred_superseding_survives_a_restart() {
    let mut node = start();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{"refresh_interval_ms":100,"refresh_min_docs":10000,
                "refresh_max_interval_ms":60000,"flush_wait_ms":5000}"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    // Первая версия опубликована: именно она рискует остаться живой.
    assert_eq!(put(node.port, 1, None).code, 200);
    publish(&node);
    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "первая версия не опубликовалась: {body}");

    // Дальше замены без публикации: их прежние версии ещё не погашены.
    for revision in 2..=6u32 {
        assert_eq!(put(node.port, revision, Some(revision - 1)).code, 200);
    }

    node.restart();
    // Ровно одна живая версия: ни пустоты, ни двух. Пустота — то, ради чего всё
    // это делалось; две — обратная ошибка, гашение не доехало.
    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "после перезапуска живых версий не одна: {body}");
    // Версия не уехала назад: до обрыва поиск показывал первую — опубликовано
    // было только её, — и меньше первой он показать не вправе.
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let seen = v["hits"][0]["fields"]["revision"].as_i64().unwrap_or(-1);
    assert!((1..=6).contains(&seen), "поиск показал версию {seen}: {body}");

    // Точечное чтение авторитетно и после подъёма: оно смотрит в буфер, куда
    // журнал переиграл всё принятое. Расхождение между ним и поиском и было
    // тем, из-за чего админка видела одно, а воркер другое.
    let doc = call(node.port, "GET", "/_doc/parser_runs%2Fcurrent", None);
    assert_eq!(doc.code, 200, "документ не прочитался: {}", doc.body);
    let d: serde_json::Value = serde_json::from_str(&doc.body).unwrap();
    assert_eq!(d["fields"]["revision"], 6, "журнал переиграл не всё: {}", doc.body);

    // CAS сверяется с уцелевшей версией, а не с той, что видна поиску.
    assert_eq!(put(node.port, 8, Some(6)).code, 200, "CAS не увидел версию 6");
    assert_eq!(put(node.port, 9, Some(6)).code, 409, "устаревшее условие прошло");

    // После публикации поиск догоняет — и живая версия по-прежнему одна.
    publish(&node);
    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "после публикации живых версий не одна: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["hits"][0]["fields"]["revision"], 8, "публикация показала не последнюю: {body}");
}

/// Запись видна поиску до сброса на диск.
///
/// Ради этого и заводился живой сегмент: буфер приёма участвует в поиске, а не
/// ждёт публикации. Проверяется не «когда-нибудь появится», а появление в
/// пределах срока свежести — и притом что сброса на диск за это время не было.
#[test]
fn a_write_becomes_visible_to_search_before_it_reaches_disk() {
    let node = start();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            // Сброс на диск отодвинут заведомо далеко: минимум в десять тысяч
            // документов и минута жёсткого возраста. Всё, что найдётся за это
            // время, найдено живым сегментом, а не сегментом на диске.
            r#"{"refresh_interval_ms":100,"refresh_min_docs":1,
                "refresh_max_interval_ms":60000,"live_max_docs":10000,
                "flush_wait_ms":5000}"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    let segments_now = || -> u64 {
        let reply = call(node.port, "GET", "/_health", None);
        assert_eq!(reply.code, 200, "health: {}", reply.body);
        let v: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
        v["segments"].as_u64().unwrap_or(0)
    };
    let before = segments_now();

    assert_eq!(put(node.port, 1, None).code, 200);

    // Ждём не дольше пяти сроков свежести: это проверка свежести, а не терпения.
    let mut seen = None;
    for _ in 0..50 {
        let (n, body) = hits(node.port);
        if n == 1 {
            seen = Some(body);
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let body = seen.expect("запись так и не стала видимой поиску за полсекунды");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["hits"][0]["fields"]["revision"], 1, "нашлась не та версия: {body}");

    // И теперь то, ради чего всё это: сброса на диск не было.
    assert_eq!(
        segments_now() - before,
        1,
        "видимость обошлась сегментом на диске — живой сегмент ни при чём"
    );

    // Замены тоже видны сразу, и по-прежнему в одном экземпляре.
    for revision in 2..=8u32 {
        assert_eq!(put(node.port, revision, Some(revision - 1)).code, 200);
    }
    let mut latest = None;
    for _ in 0..50 {
        let (n, body) = hits(node.port);
        if n == 1 {
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            if v["hits"][0]["fields"]["revision"] == 8 {
                latest = Some(body);
                break;
            }
        }
        assert!(n <= 1, "у одного id оказалось {n} живых версий: {body}");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(latest.is_some(), "последняя замена не стала видимой поиску");
    assert_eq!(segments_now() - before, 1, "замены всё-таки уехали на диск");
}

/// Порог `live_max_docs` возвращает прежнее поведение, а не ломает его.
///
/// Выше порога живой сегмент не собирается: массовой загрузке нужна пропускная
/// способность, а не свежесть. Но и окна пустоты там быть не должно — гашение
/// прежних версий обязано остаться отложенным.
#[test]
fn above_the_live_limit_visibility_falls_back_to_the_flush() {
    let node = start();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            r#"{"refresh_interval_ms":100,"refresh_min_docs":10000,
                "refresh_max_interval_ms":60000,"live_max_docs":0,
                "flush_wait_ms":5000}"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    assert_eq!(put(node.port, 1, None).code, 200);
    publish(&node);
    let (n, _) = hits(node.port);
    assert_eq!(n, 1, "первая версия не опубликовалась");

    // Живой сегмент выключен — замены поиску не видны до сброса. Но прежняя
    // версия обязана оставаться видимой: пустоты быть не должно.
    let mut blind = 0usize;
    for revision in 2..=10u32 {
        assert_eq!(put(node.port, revision, Some(revision - 1)).code, 200);
        let (n, body) = hits(node.port);
        if n == 0 {
            blind += 1;
        }
        assert!(n <= 1, "две живые версии при выключенном живом сегменте: {body}");
    }
    assert_eq!(blind, 0, "при выключенном живом сегменте вернулось окно пустоты");

    // После сброса видна последняя версия — прежнее поведение целиком.
    publish(&node);
    let (n, body) = hits(node.port);
    assert_eq!(n, 1, "после сброса живых версий не одна: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["hits"][0]["fields"]["revision"], 10, "сброс показал не последнюю: {body}");
}

/// Снимок содержит то, что видно поиску, а не только то, что уже легло на диск.
///
/// Снимок собирается по списку сегментов на диске. Всё принятое и ещё не
/// сброшенное живёт в буфере и в живом сегменте — то есть находится поиском, но
/// в снимок не попадало. Узел отвечал «снимок готов», а в нём не хватало
/// последних минут приёма, и узнать об этом было неоткуда до восстановления.
#[test]
fn a_snapshot_contains_what_search_can_see() {
    let (node, dest) = start_with_snapshots();
    let settings = call(
        node.port,
        "PUT",
        "/_settings",
        Some(
            // Сброс на диск отодвинут: без публикации перед снятием документ
            // остался бы только в буфере.
            r#"{"refresh_interval_ms":100,"refresh_min_docs":1,
                "refresh_max_interval_ms":60000,"flush_wait_ms":5000}"#,
        ),
    );
    assert_eq!(settings.code, 200, "настройки: {}", settings.body);

    assert_eq!(put(node.port, 42, None).code, 200);

    // Документ виден поиску — значит и в снимке ему быть обязано.
    let mut visible = false;
    for _ in 0..50 {
        if hits(node.port).0 == 1 {
            visible = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(visible, "документ не стал видимым — проверять было бы нечего");

    let body = format!(r#"{{"dest":{}}}"#, serde_json::to_string(&dest).unwrap());
    let made = call(node.port, "POST", "/_snapshot/ночной", Some(&body));
    assert_eq!(made.code, 201, "снимок не снялся: {}", made.body);
    let v: serde_json::Value = serde_json::from_str(&made.body).unwrap();
    assert_eq!(v["docs"], 1, "снимок собран без документа, который виден поиску: {}", made.body);
}
