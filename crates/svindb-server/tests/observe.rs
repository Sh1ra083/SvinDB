//! Точки, которыми узел рассказывает о себе: `/_doc/{id}`, `/_stats`,
//! `/_inspect` — и то, что подсказка не поднимает базу.
//!
//! По настоящему сокету, а не внутренними вызовами. Половина того, что здесь
//! может сломаться, живёт между разбором цели и обработчиком: право, метод,
//! раскодирование идентификатора. Тест, зовущий обработчик напрямую, остался бы
//! зелёным ровно на тех ошибках, ради которых он написан.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Node {
    child: Child,
    port: u16,
    dir: String,
    log: Option<String>,
}

impl Drop for Node {
    fn drop(&mut self) {
        let failed = std::thread::panicking();
        if failed {
            eprintln!(
                "observe evidence: pid={} status={:?} dir={} log={:?}",
                self.child.id(),
                self.child.try_wait(),
                self.dir,
                self.log
            );
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Пустой путь означает «каталог отдан другому узлу»: удалять его здесь
        // нельзя, иначе второй узел останется без индекса.
        if !failed && !self.dir.is_empty() {
            std::fs::remove_dir_all(&self.dir).ok();
        }
        if !failed {
            if let Some(log) = &self.log {
                std::fs::remove_file(log).ok();
            }
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn tmp(tag: &str) -> String {
    let d = std::env::temp_dir()
        .join(format!("svindb-obs-{tag}-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&d).ok();
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Узел со схемой из пяти полей разных видов.
///
/// Пять, а не два: точка на один документ обязана отдавать **всю** схему, и
/// проверить это на индексе из одних текстовых полей нельзя — умолчание
/// `title, body` совпало бы с правильным ответом.
fn node(tag: &str) -> Node {
    let dir = tmp(tag);
    let port = 0;
    let log = format!("{dir}.server.log");
    let output = std::fs::File::create(&log).unwrap();
    let child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args([
            "serve",
            &dir,
            &port.to_string(),
            "--single-index",
            "--no-maintain",
            "--fields=title:2,body:1,status:keyword,views:int64,ts:date",
        ])
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap();
    let mut node = Node { child, port, dir, log: Some(log) };
    for _ in 0..200 {
        assert!(node.child.try_wait().unwrap().is_none(), "тестовый узел завершился при startup");
        if let Ok(output) = std::fs::read_to_string(node.log.as_ref().unwrap()) {
            if let Some(address) =
                output.lines().find_map(|line| line.strip_prefix("SVINDB_LISTEN "))
            {
                node.port = address.parse::<std::net::SocketAddr>().unwrap().port();
                if healthy_port(node.port) {
                    return node;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("узел не поднялся на {port}");
}

fn healthy_port(port: u16) -> bool {
    let check = || -> Option<bool> {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(200))).ok()?;
        stream.set_write_timeout(Some(std::time::Duration::from_millis(200))).ok()?;
        stream
            .write_all(b"GET /_health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .ok()?;
        let mut response = String::new();
        stream.take(1 << 20).read_to_string(&mut response).ok()?;
        let (header, body) = response.split_once("\r\n\r\n")?;
        let health: serde_json::Value = serde_json::from_str(body).ok()?;
        Some(header.starts_with("HTTP/1.1 200 ") && health["ok"] == true)
    };
    check() == Some(true)
}

struct Reply {
    code: u16,
    body: String,
}

/// Свой клиент, а не `curl`: тест обязан работать там, где `curl` не поставлен.
fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    let mut s = TcpStream::connect(("127.0.0.1", port))
        .unwrap_or_else(|error| panic!("{method} {path} port={port}: {error}"));
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(b);
    }
    s.write_all(req.as_bytes()).unwrap();
    s.flush().unwrap();

    let mut r = BufReader::new(s);
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    let code: u16 = line.split_whitespace().nth(1).unwrap_or("0").parse().unwrap_or(0);
    let mut len = None;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).unwrap() == 0 || h.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                len = v.trim().parse::<usize>().ok();
            }
        }
    }
    let mut body = String::new();
    match len {
        Some(n) => {
            let mut buf = vec![0u8; n];
            r.read_exact(&mut buf).unwrap();
            body = String::from_utf8_lossy(&buf).to_string();
        }
        None => {
            r.read_to_string(&mut body).ok();
        }
    }
    Reply { code, body }
}

/// Документ читается по идентификатору — и до публикации, и после неё.
///
/// Обе половины в одном тесте намеренно: у них разные источники — буфер приёма
/// и сегмент, — и ответ обязан быть **одинаковым**. Разойдясь, они дали бы
/// документ, который меняет содержимое сам по себе через секунду после записи.
#[test]
fn settled_advances_after_flush_even_when_compaction_execution_is_disabled() {
    let n = node("settled-without-maintain");
    let written = call(
        n.port,
        "POST",
        "/_bulk",
        Some(r#"{"documents":[{"id":"settled","title":"проверка"}]}"#),
    );
    assert_eq!(written.code, 200, "{}", written.body);
    assert_eq!(call(n.port, "POST", "/_flush", Some("{}")).code, 200);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let reply = call(n.port, "GET", "/_health", None);
        assert_eq!(reply.code, 200);
        let health: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
        let marks = &health["watermarks"];
        let ack = marks["ack"].as_u64().unwrap();
        if ack > 0 && marks["settled"].as_u64() == Some(ack) {
            assert_eq!(marks["ready"].as_u64(), Some(ack));
            assert_eq!(health["compaction"]["pending_segments"], 0);
            return;
        }
        assert!(std::time::Instant::now() < deadline, "тихий flush не settled: {}", reply.body);
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

#[test]
fn an_unworked_compaction_debt_is_not_settled_by_restart() {
    let mut n = node("unsettled-restart");
    for id in 0..20 {
        let body = format!(r#"{{"documents":[{{"id":"{id}","title":"проверка"}}]}}"#);
        let written = call(n.port, "POST", "/_bulk", Some(&body));
        assert_eq!(written.code, 200, "{}", written.body);
        assert_eq!(call(n.port, "POST", "/_flush", Some("{}")).code, 200);
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let reply = call(n.port, "GET", "/_health", None);
        let health: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
        if health["compaction"]["pending_segments"].as_u64().unwrap() > 0 {
            let marks = &health["watermarks"];
            assert!(marks["settled"].as_u64().unwrap() < marks["ready"].as_u64().unwrap());
            break;
        }
        assert!(std::time::Instant::now() < deadline, "долг не наблюдается: {}", reply.body);
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    n.child.kill().unwrap();
    n.child.wait().unwrap();
    n.child = Command::new(BIN)
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args([
            "serve",
            &n.dir,
            &n.port.to_string(),
            "--single-index",
            "--no-maintain",
            "--fields=title:2,body:1,status:keyword,views:int64,ts:date",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while TcpStream::connect(("127.0.0.1", n.port)).is_err() {
        assert!(std::time::Instant::now() < until);
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let reply = call(n.port, "GET", "/_health", None);
    let health: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
    assert_eq!(health["docs"], 20);
    assert!(health["compaction"]["pending_segments"].as_u64().unwrap() > 0);
    let marks = &health["watermarks"];
    assert!(
        marks["settled"].as_u64().unwrap() < marks["ready"].as_u64().unwrap(),
        "restart без merge объявил долг погашенным: {}",
        reply.body
    );
}

#[test]
fn a_document_reads_back_by_id_before_and_after_publication() {
    let n = node("чтение");
    let doc = r#"{"documents":[{"id":"м1","title":"Первый дом","body":"тело первого",
                   "status":"live","views":10,"ts":1700000000}]}"#;
    assert_eq!(call(n.port, "POST", "/_bulk", Some(doc)).code, 200);

    // До публикации: ответ идёт из буфера приёма.
    let before = call(n.port, "GET", "/_doc/%D0%BC1", None);
    assert_eq!(before.code, 200, "{}", before.body);
    assert!(before.body.contains("\"found\":true"), "{}", before.body);
    assert!(before.body.contains("\"buffered\":true"), "{}", before.body);

    // Все объявленные поля, а не только текстовые. Числа — числами, дата —
    // числом: тем же видом, каким их отдаёт поиск.
    for want in [
        "\"title\":\"Первый дом\"",
        "\"body\":\"тело первого\"",
        "\"status\":\"live\"",
        "\"views\":10",
        "\"ts\":1700000000",
    ] {
        assert!(before.body.contains(want), "в буфере нет {want}: {}", before.body);
    }

    assert_eq!(call(n.port, "POST", "/_flush", Some("{}")).code, 200);

    // После публикации: тот же ответ, но уже из сегмента и с номером документа.
    let after = call(n.port, "GET", "/_doc/%D0%BC1", None);
    assert_eq!(after.code, 200, "{}", after.body);
    assert!(after.body.contains("\"found\":true"), "{}", after.body);
    assert!(after.body.contains("\"doc\":"), "номера документа нет: {}", after.body);
    for want in
        ["\"title\":\"Первый дом\"", "\"status\":\"live\"", "\"views\":10", "\"ts\":1700000000"]
    {
        assert!(after.body.contains(want), "в сегменте нет {want}: {}", after.body);
    }
}

/// Идентификатор из пути и идентификатор из тела — один и тот же документ.
///
/// Ровно та ошибка, из-за которой заведено раскодирование: `PUT /_doc/м1` клал
/// документ под именем `%d0%bc1`, а `/_bulk` — под именем `м1`, и два способа
/// положить один документ давали два разных. Оба при этом отвечали двумястами.
#[test]
fn the_path_id_and_the_body_id_mean_the_same_document() {
    let n = node("кодировка");
    let one = r#"{"title":"через путь","body":"тело","status":"live","views":1,"ts":1}"#;
    assert_eq!(call(n.port, "PUT", "/сообщения/_doc/%D0%BC1", Some(one)).code, 200);

    let got = call(n.port, "GET", "/_doc/%D0%BC1", None);
    assert_eq!(got.code, 200, "{}", got.body);
    assert!(got.body.contains("\"id\":\"м1\""), "идентификатор не раскодирован: {}", got.body);

    // И гашение по тому же пути находит именно его.
    assert_eq!(call(n.port, "DELETE", "/_doc/%D0%BC1", None).code, 200);
    let gone = call(n.port, "GET", "/_doc/%D0%BC1", None);
    assert_eq!(gone.code, 404, "{}", gone.body);
    assert!(gone.body.contains("\"found\":false"), "{}", gone.body);
}

/// Чтение документа — это чтение, а не запись.
///
/// Проверяется на узле `--read-only`: пока обе операции жили одной точкой, `GET
/// /_doc/{id}` числился пишущим и отвечал «приём выключен» на запрос, который
/// ничего не принимает.
#[test]
fn reading_a_document_works_on_a_read_only_node() {
    // Сперва обычный узел кладёт документ и публикует его.
    //
    // Каталог обязан пережить этот узел, а процесс — нет: писатель держит
    // `.writer.lock`, и оставить его работать значило бы разбирать потом чужой
    // узел, который никто не гасил. Поэтому первый узел гасится **явно**, а от
    // его `Drop` отбирается только удаление каталога — подменой пути на пустой.
    let mut first = node("только-чтение-подготовка");
    let doc = r#"{"documents":[{"id":"м1","title":"дом","body":"тело",
                   "status":"live","views":7,"ts":1700000000}]}"#;
    assert_eq!(call(first.port, "POST", "/_bulk", Some(doc)).code, 200);
    assert_eq!(call(first.port, "POST", "/_flush", Some("{}")).code, 200);
    let dir = std::mem::take(&mut first.dir);
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    drop(first);

    let port = free_port();
    let child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["serve", &dir, &port.to_string(), "--single-index", "--read-only"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let n = Node { child, port, dir, log: None };
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let got = call(n.port, "GET", "/_doc/%D0%BC1", None);
    assert_eq!(got.code, 200, "чтение отказано на узле только для чтения: {}", got.body);
    assert!(got.body.contains("\"views\":7"), "{}", got.body);

    // А запись по той же точке по-прежнему отказана.
    let write = call(n.port, "PUT", "/_doc/м2", Some(r#"{"title":"нельзя"}"#));
    assert_eq!(write.code, 400, "{}", write.body);
    assert!(write.body.contains("read-only"), "{}", write.body);
}

/// `/_stats` называет узел: версию, режим, схему и место.
#[test]
fn stats_describe_the_node() {
    let n = node("сведения");
    let got = call(n.port, "GET", "/_stats", None);
    assert_eq!(got.code, 200, "{}", got.body);
    for want in [
        "\"version\":\"",
        "\"uptime_seconds\":",
        "\"read_only\":false",
        "\"maintain\":false",
        "\"wal\":true",
        "\"coordinator\":false",
        "\"fields\":5",
        "\"index_bytes\":",
        "\"enabled\":false",
    ] {
        assert!(got.body.contains(want), "в сведениях нет {want}: {}", got.body);
    }
    // Узел запущен с `--no-maintain`: обслуживание выключено, а запись нет.
    // Если бы `maintain` читался у наличия писателя, здесь стояло бы `true`.
    assert!(got.body.contains("\"maintain\":false"), "{}", got.body);

    // Другим методом эта точка не отвечает и в выдачу сведений не проваливается.
    assert_ne!(call(n.port, "POST", "/_stats", Some("{}")).code, 200);
}

/// `/_inspect` считает место и не расходится с тем, что говорит `/_stats`.
#[test]
fn inspect_counts_the_same_bytes_as_stats() {
    let n = node("место");
    let doc = r#"{"documents":[{"id":"м1","title":"дом","body":"тело",
                   "status":"live","views":1,"ts":1}]}"#;
    assert_eq!(call(n.port, "POST", "/_bulk", Some(doc)).code, 200);
    assert_eq!(call(n.port, "POST", "/_flush", Some("{}")).code, 200);

    let got = call(n.port, "GET", "/_inspect", None);
    assert_eq!(got.code, 200, "{}", got.body);
    assert!(got.body.contains("\"segments\":1"), "{}", got.body);
    assert!(got.body.contains("\"docs\":1"), "{}", got.body);
    assert!(got.body.contains("\"index_bytes\":"), "{}", got.body);
}

/// Новые виды запросов попадают в счётчики своим именем.
#[test]
fn a_document_read_is_counted_apart_from_search() {
    let n = node("счётчики");
    call(n.port, "GET", "/_doc/нет-такого", None);
    let m = call(n.port, "GET", "/_metrics", None);
    assert_eq!(m.code, 200);
    assert!(
        m.body.contains("svindb_requests_total{kind=\"get\"}"),
        "чтение документа не отделено от поиска: {}",
        m.body
    );
    // И расход процесса тоже отдаётся — это то, ради чего точку и спрашивает
    // наблюдатель.
    for want in ["svindb_build_info{version=", "svindb_uptime_seconds", "svindb_index_bytes"] {
        assert!(m.body.contains(want), "нет метрики {want}");
    }
}

/// Подсказка — это подсказка, а не запуск базы.
///
/// Пока здесь стоял один разбор «всё неузнанное поднимает узел», `svindb --help`
/// брал writer-lock на каталог `data` рядом с бинарником и слушал 19300, ни
/// словом об этом не сказав. То же случалось на любой опечатке в подкоманде.
#[test]
fn a_flag_asks_for_help_instead_of_starting_a_node() {
    // Не смотрим на `data` рядом с общим target/debug/svindb: соседний тест
    // или оборванный ручной запуск мог законно оставить там свой индекс, и
    // тогда проверка обвиняла `--help` в чужой записи. Собственная копия
    // бинарника даёт собственный заведомо пустой каталог по умолчанию.
    let root = std::path::PathBuf::from(tmp("help"));
    let binary = root.join("svindb");
    std::fs::copy(BIN, &binary).expect("копия проверяемого бинарника");
    let out = Command::new(&binary)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .arg("--help")
        .output()
        .unwrap();
    assert!(out.status.success(), "у просьбы о подсказке ненулевой код");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("svindb serve"), "подсказка ушла не в stdout: {text}");

    let ver = Command::new(&binary)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .arg("--version")
        .output()
        .unwrap();
    assert!(ver.status.success());
    assert!(String::from_utf8_lossy(&ver.stdout).starts_with("svindb "));

    // Неузнанный флаг называет себя и не поднимает узел.
    let bad = Command::new(&binary)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .arg("--такого-флага-нет")
        .output()
        .unwrap();
    assert_eq!(bad.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&bad.stderr).contains("неизвестный флаг"));

    // Ни один из трёх запусков не должен был создать каталог данных рядом с
    // бинарником: именно это и было ценой прежнего поведения.
    let data = root.join("data");
    assert!(
        !data.join(".writer.lock").exists(),
        "подсказка всё-таки подняла узел и взяла writer-lock"
    );
    std::fs::remove_dir_all(root).ok();
}

/// Набор по идентификаторам не держит замок приёма на всё чтение.
///
/// ## Что здесь измеряется
///
/// Ровно одно число: `svindb_write_lock_held_ms{lock="ingest"}` — сколько
/// **удержан** замок буфера, а не сколько его ждали. Прежде `_mget` брал его
/// первым и отпускал последним: под ним шли и обход сегментов, и распаковка
/// содержимого, то есть приём стоял столько, сколько длится самое дорогое
/// чтение. Проба по буферу — это хеш и сравнение строки; вся оговорка про
/// согласованность снимка касается только её, и держать замок дольше не за чем.
///
/// Мерка — доля: удержание против длительности самого запроса. Абсолютное число
/// зависело бы от машины, доля — нет. Если замок снова начнут держать на всё
/// чтение, доля уйдёт к единице.
#[test]
fn a_batch_read_does_not_hold_the_ingest_lock() {
    // Подробные lock-histograms — opt-in feature: production может отключить
    // их для минимальной стоимости hot path. Сам договор проверяется отдельным
    // Linux-прогоном с `--features stats`; без feature это не ложный провал.
    if !svindb_core::stats::ENABLED {
        return;
    }
    let n = node("mget-lock");
    // Корпус побольше и в несколько сегментов: на десяти документах и чтение, и
    // удержание одинаково близки к нулю, и доля ничего не покажет.
    for batch in 0..8 {
        let docs: Vec<String> = (0..500)
            .map(|i| {
                let d = batch * 500 + i;
                format!(
                    r#"{{"id":"d{d}","title":"заголовок {d}","body":"тело документа номер {d} со словами для веса","status":"live","views":{d},"ts":{d}}}"#
                )
            })
            .collect();
        let put = call(
            n.port,
            "POST",
            "/_bulk",
            Some(&format!(r#"{{"documents":[{}]}}"#, docs.join(","))),
        );
        assert_eq!(put.code, 200, "приём: {}", put.body);
        assert_eq!(call(n.port, "POST", "/_flush", Some("{}")).code, 200);
    }

    let held_of = |body: &str| -> f64 {
        body.lines()
            .find(|l| l.starts_with("svindb_write_lock_held_ms_sum{lock=\"ingest\"}"))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(-1.0)
    };
    let before = held_of(&call(n.port, "GET", "/_metrics", None).body);
    assert!(before >= 0.0, "счётчика удержания замка нет в метриках");

    let ids: Vec<String> = (0..1000).map(|d| format!("\"d{d}\"")).collect();
    let ask = format!(r#"{{"ids":[{}]}}"#, ids.join(","));
    let t0 = std::time::Instant::now();
    let got = call(n.port, "POST", "/_mget", Some(&ask));
    let took = t0.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(got.code, 200, "набор: {}", got.body);
    assert_eq!(got.body.matches("\"found\":true").count(), 1000, "нашлись не все");

    let after = held_of(&call(n.port, "GET", "/_metrics", None).body);
    let held = after - before;
    assert!(held >= 0.0, "удержание не выросло вовсе — счётчик не тот");
    // Пятая часть — с большим запасом: держи замок на всё чтение, и доля была бы
    // около единицы. Запас нужен потому, что в удержание попадает и метрика,
    // которую тест спрашивает следом.
    assert!(
        held < took / 5.0,
        "замок удержан {held:.1} мс при запросе в {took:.1} мс — \
         значит его снова держат на всё чтение"
    );
}

/// РЕЦЕНЗИЯ (временный тест): документ, который никуда не девался, отдаётся 404.
/// Публикация посреди чтения набора не роняет живой документ.
///
/// ## Что здесь на самом деле проверяется
///
/// Снимок состава — это `Arc` на **список** сегментов. Он закрепляет список и
/// начала номеров, но **не карту живых**: гашение правит биты прямо в общем
/// отображении тех же сегментов. А публикация идёт двумя шагами: сперва новая
/// версия попадает в новый список, которого у читателя нет, и только потом
/// гаснет старая. Читатель, чей снимок взят между шагами, видит старую версию
/// мёртвой, а новую — нигде.
///
/// ## Чего этот тест НЕ доказывает
///
/// Сказать прямо: **на этой машине он зелен и без исправления**. Разбор
/// воспроизвёл потерю на выпускной сборке с отдельными настройками свежести —
/// 106 промахов из 4693 чтений, — а здесь окно не открывается ни при полутора
/// сотнях целей, ни при двух тысячах отсутствующих, ни при четырёх читателях.
///
/// Значит он сторож, а не доказательство: он закрепляет форму нагрузки, при
/// которой потеря наблюдалась, и поймает её, если она станет шире. Само
/// исправление держится на разборе кода и на чужом воспроизведении, а не на
/// этом тесте, и записано это здесь затем, чтобы никто не принял зелёный цвет
/// за проверку.
#[test]
fn a_batch_read_does_not_lose_a_document_to_a_publication() {
    let n = node("mget-race");
    // Fixture намеренно сохраняет 150+ сегментов без maintenance ради гонки
    // ReadView. Теперь debt наблюдается и в этом режиме; production hard cap
    // нельзя молча обходить отсутствием измерения. Здесь нужен отдельный
    // явный предел стенда, а сам bounded backpressure проверяется update_heavy.
    let settings = call(
        n.port,
        "PUT",
        "/_settings",
        Some(r#"{"l0_soft_segments":512,"l0_hard_segments":1024}"#),
    );
    assert_eq!(settings.code, 200, "{}", settings.body);
    const TARGETS: usize = 150;
    // Каждая цель — своим сбросом, то есть своим сегментом: чем старше сегмент,
    // тем позже до него доходит обход и тем шире окно.
    for d in 0..TARGETS {
        let doc = format!(
            r#"{{"documents":[{{"id":"цель-{d}","title":"первая","body":"тело {d}","status":"live","views":{d},"ts":{d}}}]}}"#
        );
        assert_eq!(call(n.port, "POST", "/_bulk", Some(&doc)).code, 200);
        assert_eq!(call(n.port, "POST", "/_flush", Some("{}")).code, 200);
    }

    // К целям добавляются заведомо отсутствующие: они не находятся нигде, и
    // обход из-за них длится достаточно долго — то есть окно между снимком и
    // пробой старого сегмента расширяется настолько, чтобы публикация в него
    // попадала. Без них повторные круги проходили мимо, и тест был зелёным
    // на сломанном коде.
    // Достаточно отсутствующих целей, чтобы обход пересёк старые сегменты,
    // но не настолько много, чтобы время ответа зависело от скорости
    // конкретного CI-раннера.
    let mut ids: Vec<String> = (0..500).map(|d| format!("\"нет-{d}\"")).collect();
    ids.extend((0..TARGETS).map(|d| format!("\"цель-{d}\"")));
    let ask = std::sync::Arc::new(format!(r#"{{"ids":[{}]}}"#, ids.join(",")));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lost = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (port, ask, stop, lost, reads) = (
                n.port,
                std::sync::Arc::clone(&ask),
                std::sync::Arc::clone(&stop),
                std::sync::Arc::clone(&lost),
                std::sync::Arc::clone(&reads),
            );
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let got = call(port, "POST", "/_mget", Some(&ask));
                    if got.code != 200 {
                        continue;
                    }
                    reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Считаются только пропавшие **цели**: отсутствующие
                    // отсутствуют по делу и в счёт не идут.
                    let miss = (0..TARGETS)
                        .filter(|d| {
                            got.body.contains(&format!("{{\"id\":\"цель-{d}\",\"found\":false}}"))
                        })
                        .count();
                    if miss > 0 {
                        lost.fetch_add(miss, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    // Писатель по очереди заменяет каждую цель — новая версия ложится своим
    // сегментом, старая гаснет. После первого круга продолжаем заменять цели,
    // пока читатели не подтвердят достаточное число полных снимков. Это
    // проверяет пересечение чтения и публикации, а не случайное число чтений
    // за фиксированное время; общий deadline всё равно ограничивает зависание.
    const REQUIRED_READS: usize = 51;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut written = 0;
    while written < TARGETS || reads.load(std::sync::atomic::Ordering::Acquire) < REQUIRED_READS {
        assert!(
            std::time::Instant::now() < deadline,
            "читатели не подтвердили {REQUIRED_READS} снимков до deadline (получено {})",
            reads.load(std::sync::atomic::Ordering::Relaxed)
        );
        let d = written % TARGETS;
        let doc = format!(
            r#"{{"documents":[{{"id":"цель-{d}","title":"вторая","body":"новое {d}","status":"live","views":7,"ts":7}}]}}"#
        );
        assert_eq!(call(n.port, "POST", "/_bulk", Some(&doc)).code, 200);
        assert_eq!(call(n.port, "POST", "/_flush", Some("{}")).code, 200);
        written += 1;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    let seen = reads.load(std::sync::atomic::Ordering::Relaxed);
    assert!(seen >= REQUIRED_READS, "чтений всего {seen} — проверка вхолостую");
    assert_eq!(
        lost.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "набор потерял живой документ за {seen} чтений"
    );
}

#[test]
fn an_aggregation_reads_the_same_column_the_filter_does() {
    let dir = tmp("fid");
    let a = format!("{dir}/a.ndjson");
    let b = format!("{dir}/b.ndjson");
    let ix = format!("{dir}/ix");
    {
        use std::io::Write as _;
        let mut fa = std::fs::File::create(&a).unwrap();
        let mut fb = std::fs::File::create(&b).unwrap();
        for i in 0..20i64 {
            writeln!(fa, r#"{{"id":"a{i}","title":"документ","price":{}}}"#, i * 10).unwrap();
            writeln!(
                fb,
                r#"{{"id":"b{i}","title":"документ","extra":{},"price":{}}}"#,
                1000 - i * 10,
                i * 10
            )
            .unwrap();
        }
    }
    // Второй проход объявляет `extra` **между** уже существующими полями:
    // устойчивый номер у него новый, а позиция — вторая.
    for (src, fields) in
        [(&a, "title:text,price:int32"), (&b, "title:text,extra:int32,price:int32")]
    {
        let out = Command::new(BIN)
            .env("SVINDB_DISK_FLOOR_MB", "1")
            .args(["index", src, &ix, "1000000", fields])
            .output()
            .unwrap();
        assert!(out.status.success(), "index: {}", String::from_utf8_lossy(&out.stderr));
    }

    let port = free_port();
    let child = Command::new(BIN)
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["serve", &ix, &port.to_string(), "--no-maintain"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let n = Node { child, port, dir, log: None };
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", n.port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // Цена идёт 0, 10, … 190 в обоих сегментах: сорок документов, сумма 3800.
    // Прежде выходило max 1000 и sum 20000 — это `extra` второго сегмента.
    let got = call(
        n.port,
        "POST",
        "/_search",
        Some(
            r#"{"filter":[{"field":"price","from":0,"to":190}],"k":0,
                "aggs":{"s":{"stats":{"field":"price"}}}}"#,
        ),
    );
    assert_eq!(got.code, 200, "{}", got.body);
    // Разбор по подстроке: у этого набора проверок свой мини-клиент без JSON.
    for want in ["\"count\":40", "\"max\":190", "\"sum\":3800"] {
        assert!(
            got.body.contains(want),
            "статистика прочитала чужую колонку, нет {want}: {}",
            got.body
        );
    }

    // И зеркально: у `extra` значения 810…1000 только во втором сегменте.
    let other = call(
        n.port,
        "POST",
        "/_search",
        Some(
            r#"{"filter":[{"field":"extra","from":810,"to":1000}],"k":0,
                "aggs":{"s":{"stats":{"field":"extra"}}}}"#,
        ),
    );
    for want in ["\"count\":20", "\"min\":810", "\"max\":1000"] {
        assert!(other.body.contains(want), "нет {want}: {}", other.body);
    }
}

/// Два разных имени индекса на один узел — отказ, а не тихая общая куча.
///
/// ## Что здесь было не так
///
/// Имя разбиралось и выбрасывалось: `/заказы/_bulk` и `/пользователи/_bulk`
/// писали в один и тот же индекс. Молча. Клиент, разделивший данные по именам,
/// узнавал об этом не отказом, а тем, что его запрос находит чужие документы.
///
/// ## Почему проверяется постоянство, а не совпадение с чем-то
///
/// Узлу безразлично, **как** зовут его единственный индекс: различать нечего.
/// Вредны именно два разных имени. Привязка к имени каталога данных была бы
/// проще и хуже — каталог называет оператор для себя, клиент его не знает, — и
/// сломала бы всякого совместимого клиента, не поймав ни одной настоящей
/// ошибки. Это и поймал гейт: существующая проверка кодировки обращается к
/// `/сообщения/...`, и такая привязка её отвергла.
///
/// Разбор — в `docs/adr/0002`.
#[test]
fn two_index_names_on_one_node_are_refused() {
    let n = node("имя");
    let doc = r#"{"documents":[{"id":"один","title":"первый","body":"тело",
                  "status":"live","views":1,"ts":1}]}"#;

    // Любое имя годится — пока оно одно.
    let first = call(n.port, "POST", "/заказы/_bulk", Some(doc));
    assert_eq!(first.code, 200, "первое имя отвергнуто: {}", first.body);
    let again = call(
        n.port,
        "POST",
        "/заказы/_search",
        Some(r#"{"k":1,"words":[{"field":"title","terms":["первый"]}]}"#),
    );
    assert_eq!(again.code, 200, "то же имя отвергнуто во второй раз: {}", again.body);

    // Второе имя — отказ, и отказ называет оба.
    let other = call(n.port, "POST", "/пользователи/_bulk", Some(doc));
    assert_eq!(other.code, 404, "второе имя принято: {}", other.body);
    assert!(
        other.body.contains("заказы") && other.body.contains("пользователи"),
        "отказ не назвал обоих имён: {}",
        other.body
    );

    // `_all` и `*` годятся всегда: для узла с одним индексом они верны буквально.
    let q = r#"{"k":1,"words":[{"field":"title","terms":["первый"]}]}"#;
    for any in ["_all", "*"] {
        let r = call(n.port, "POST", &format!("/{any}/_search"), Some(q));
        assert_eq!(r.code, 200, "{any} отвергнут: {}", r.body);
    }

    // И без имени вовсе — как было всегда.
    assert_eq!(call(n.port, "POST", "/_search", Some(q)).code, 200);

    // Отказ приходит **до** разбора тела: имя решает раньше, чем запрос читают.
    let early = call(n.port, "POST", "/пользователи/_search", Some("не json вовсе"));
    assert_eq!(early.code, 404, "чужое имя пропущено к разбору тела: {}", early.body);
}
