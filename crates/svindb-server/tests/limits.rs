//! Память слияния: узел сливает, пока влезает.
//!
//! Слияние — единственная работа узла, чей расход памяти растёт вместе с
//! индексом. До сих пор предела у него не было: `memory_budget_mb` стережёт
//! поиск, `max_merged_mb` — размер выхода на диске. Узел, взявшийся за слияние,
//! которое не влезает, не отказывал — его убивало ядро, посреди работы и вместе
//! со всеми соединениями.
//!
//! Правило теперь такое же, как у прочих потолков окна: не влезает — окно
//! короче. Не влезает даже одиночный сегмент — работа просто не берётся.
//! Последствие — сегментов в индексе больше, и это осознанный размен: запрос
//! платит за сегмент около микросекунды, а альтернатива — смерть узла посреди
//! работы.
//!
//! Здесь и проверяется именно размен: тот же корпус при тесном бюджете
//! оставляет больше сегментов, но узел остаётся здоров, принимает запись и
//! отвечает на поиск.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Node {
    child: Child,
    port: u16,
    root: String,
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
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
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

/// Узел с искусственно малым бюджетом слияния.
fn start_with_budget(mb: u64) -> Node {
    let port = free_port();
    let root = std::env::temp_dir()
        .join(format!("svindb-пределы-{}-{port}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args([
            "serve",
            &format!("{root}/index"),
            &port.to_string(),
            "--single-index",
            "--fields=kind:keyword,body:1",
            // Мелкий сегмент: обслуживанию есть что укрупнять уже через
            // несколько пачек.
            "--flush=500",
            "--target=100000",
        ])
        .env("SVINDB_MERGE_BUDGET_MB", mb.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut node = Node { child, port, root };
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

fn batch(from: u64, n: u64) -> String {
    let docs: Vec<String> = (from..from + n)
        .map(|i| {
            format!(
                r#"{{"id":"д{i:07}","kind":"груз","body":"нагрузка тело документа номер {i} поиск индекс слово"}}"#
            )
        })
        .collect();
    format!(r#"{{"documents":[{}]}}"#, docs.join(","))
}

/// Узел, дошедший до ресурсной границы, становится тихим без ложной аварии.
///
/// ## Зачем это отдельная проверка
///
/// Соседняя ниже смотрит на следствие — сегментов стало больше. Здесь
/// проверяется состояние после этого: каждый сегмент, пара которого уже не
/// влезает в объявленный бюджет, достиг разрешённой ресурсной границы. Это не
/// долг, потому что ожидание ничего не изменит и оператору не надо увеличивать
/// память ради корректности.
///
/// Здесь бюджет сведён к мегабайту, то есть заведомо ниже постоянного расхода
/// слияния. Узел обязан перейти в тихое состояние с нулём доступной работы и
/// пустым `blocked`: ложный `over_peak` сделал бы любой малый контейнер вечно
/// красным, хотя приём, поиск и долговечность исправны.
#[test]
fn a_resource_limited_node_settles_without_a_false_blockage() {
    let node = start_with_budget(1);
    let port = node.port;
    // Сегментов нужно **больше допустимого состава**, иначе планировщику нечего
    // сливать, и он молчит по совершенно законному поводу. Двадцать четыре
    // пачки по пятьсот при пороге сегмента в пятьсот дают два с лишним десятка
    // сегментов при допустимых десяти.
    for n in 0..24u64 {
        let r = call(port, "POST", "/_bulk", Some(&batch(n * 500, 500)));
        assert_eq!(r.code, 200, "приём отказал: {}", r.body);
    }
    // Обслуживание заходит раз в две секунды. Ждём именно согласованного покоя,
    // а не просто пустой строки между двумя заходами.
    let mut compaction = serde_json::Value::Null;
    for _ in 0..40 {
        let h = call(port, "GET", "/_health", None);
        let v = h.json();
        compaction = v["compaction"].clone();
        if compaction["pending_schedulable"].as_u64() == Some(0)
            && compaction["running_segments"].as_u64() == Some(0)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(400));
    }
    assert_eq!(
        compaction["pending_schedulable"].as_u64(),
        Some(0),
        "узел оставил работу, которую не способен выполнить: {compaction}"
    );
    assert_eq!(
        compaction["running_segments"].as_u64(),
        Some(0),
        "обслуживание не успокоилось: {compaction}"
    );
    assert_eq!(
        compaction["blocked"].as_str(),
        Some(""),
        "достигнутая ресурсная граница названа отказом: {compaction}"
    );
    // И при этом узел жив: приём идёт, поиск отвечает.
    let more = call(port, "POST", "/_bulk", Some(&batch(1_000_000, 10)));
    assert_eq!(more.code, 200, "приём остановлен: {}", more.body);
    let found =
        call(port, "POST", "/", Some(r#"{"words":[{"field":1,"terms":["нагрузка"]}],"k":3}"#));
    assert_eq!(found.code, 200, "поиск отказал: {}", found.body);
}

/// Долг, который нельзя разобрать, не считается работой.
///
/// ## Что здесь ловится
///
/// Сегмент, доросший до двух третей целевого размера, не сливается ни с одним
/// соседом: любая пара выходит за потолок окна по документам. Таких сегментов
/// со временем становится больше допустимого состава, и превышение остаётся
/// **навсегда**. Прежде обслуживание сообщало его долгом, и узел выглядел не
/// досведённым, будучи полностью в порядке: на десяти миллионах это давало
/// пятнадцать сегментов, вечный долг в три и `settled`, достигнутый в одном
/// прогоне кампании из шести.
///
/// Долг остаётся в ответе — он мера того, насколько состав дальше желаемого. Но
/// рядом обязано стоять число того, что можно убрать сейчас, и оно обязано быть
/// нулём.
#[test]
fn a_debt_that_cannot_be_worked_off_is_not_reported_as_work() {
    let node = start_with_budget(4096);
    let port = node.port;
    // Потолок сегмента низкий, а порог сброса — чуть больше половины от него:
    // каждый сегмент дорастает до размера, при котором пара уже не помещается.
    let r = call(
        port,
        "PUT",
        "/_settings",
        Some(r#"{"flush_docs":3000,"target_docs":5000,"merge_at_once":8}"#),
    );
    assert_eq!(r.code, 200, "настройки: {}", r.body);
    for n in 0..40u64 {
        let r = call(port, "POST", "/_bulk", Some(&batch(n * 500, 500)));
        assert_eq!(r.code, 200, "приём отказал: {}", r.body);
    }
    call(port, "POST", "/_flush", None);

    let mut pending = 0u64;
    let mut schedulable = 1u64;
    let mut running = 1u64;
    for _ in 0..60 {
        let v = call(port, "GET", "/_health", None).json();
        let c = &v["compaction"];
        pending = c["pending_segments"].as_u64().unwrap_or(0);
        schedulable = c["pending_schedulable"].as_u64().unwrap_or(u64::MAX);
        running = c["running_segments"].as_u64().unwrap_or(u64::MAX);
        if schedulable == 0 && running == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(400));
    }
    assert_eq!(running, 0, "обслуживание не успокоилось");
    assert_eq!(
        schedulable, 0,
        "обслуживание сообщает работу, которой нет: долг {pending}, к исполнению {schedulable}"
    );
    // И узел при этом жив и отвечает.
    let found =
        call(port, "POST", "/", Some(r#"{"words":[{"field":1,"terms":["нагрузка"]}],"k":3}"#));
    assert_eq!(found.code, 200, "поиск отказал: {}", found.body);
}

/// Тесный бюджет оставляет больше сегментов — и ничего больше не меняет.
///
/// Обе половины важны одинаково. Больше сегментов — значит предел правда
/// действует, а не написан для виду. Здоровый узел, принятая запись и живой
/// поиск — значит он действует так, как задумано: сливать меньше, а не падать
/// и не останавливаться.
#[test]
fn a_tight_budget_leaves_more_segments_and_nothing_else_changes() {
    const BATCHES: u64 = 24;
    let counted = |mb: u64| -> u64 {
        let node = start_with_budget(mb);
        let port = node.port;
        for n in 0..BATCHES {
            let r = call(port, "POST", "/_bulk", Some(&batch(n * 500, 500)));
            assert_eq!(r.code, 200, "бюджет {mb} МБ: пачка {n} отвергнута: {}", r.body);
        }
        assert!(matches!(call(port, "POST", "/_flush", None).code, 200 | 202));
        // Обслуживание заходит раз в две секунды; даём ему сойтись.
        std::thread::sleep(Duration::from_secs(8));

        let health = call(port, "GET", "/_health", None);
        let v = health.json();
        // Узел здоров при любом бюджете: тесный бюджет — это меньше слияний, а
        // не отказ работать.
        assert_eq!(v["ok"], true, "бюджет {mb} МБ: узел нездоров: {}", health.body);
        assert_eq!(v["ingest_error"], "", "бюджет {mb} МБ: {}", health.body);
        assert_eq!(
            v["docs"].as_u64(),
            Some(BATCHES * 500),
            "бюджет {mb} МБ: документы потерялись: {}",
            health.body
        );
        // И принимает дальше — остановки приёма быть не должно.
        let more = call(port, "POST", "/_bulk", Some(&batch(1_000_000, 10)));
        assert_eq!(more.code, 200, "бюджет {mb} МБ: приём остановлен: {}", more.body);
        // И отвечает на поиск.
        let found = call(
            port,
            "POST",
            "/",
            Some(r#"{"k":10,"ids":true,"must":[{"field":"kind","terms":["груз"]}]}"#),
        );
        assert_eq!(found.code, 200, "бюджет {mb} МБ: поиск молчит: {}", found.body);
        assert_eq!(
            found.json()["hits"].as_array().map(|h| h.len()),
            Some(10),
            "бюджет {mb} МБ: {}",
            found.body
        );
        v["segments"].as_u64().unwrap_or(0)
    };

    // Один мегабайт: постоянный расход слияния и то больше, значит не влезет
    // никакое слияние вообще.
    let tight = counted(1);
    let roomy = counted(4096);
    assert!(
        tight > roomy,
        "тесный бюджет оставил {tight} сегментов, свободный — {roomy}: предел не действует"
    );
}
