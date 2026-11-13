//! Primary и реплики: договор подтверждения, split brain и отставшая копия.
//!
//! ## Что здесь считается доказательством
//!
//! Не «запрос вернул 200». Все три проверяемые вещи различимы только по тому,
//! **что видно на другой машине**, поэтому каждое утверждение опирается на
//! чтение с реплики напрямую, а не на ответ primary о самом себе.
//!
//! Отдельно проверяется, что недостача подтверждений — **не** `200`. Клиент,
//! попросивший `quorum` и получивший `one`, купил не то, что просил: разница
//! ровно в том, переживёт ли запись отказ узла.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Node {
    child: Child,
    port: u16,
    dir: String,
}

impl Node {
    fn kill(&mut self) {
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

fn try_call(port: u16, method: &str, path: &str, body: Option<&str>) -> Option<Reply> {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).ok()?;
    socket.set_read_timeout(Some(Duration::from_secs(60))).ok()?;
    socket.set_write_timeout(Some(Duration::from_secs(60))).ok()?;
    let body = body.unwrap_or("");
    write!(
        socket,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .ok()?;
    socket.flush().ok()?;
    let mut input = BufReader::new(socket);
    let mut status = String::new();
    if input.read_line(&mut status).ok()? == 0 {
        return None;
    }
    let code = status.split_whitespace().nth(1).and_then(|s| s.parse().ok())?;
    let mut len = None;
    loop {
        let mut line = String::new();
        if input.read_line(&mut line).ok()? == 0 || line.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                len = value.trim().parse::<usize>().ok();
            }
        }
    }
    let mut bytes = vec![0; len.unwrap_or(0)];
    input.read_exact(&mut bytes).ok()?;
    Some(Reply { code, body: String::from_utf8_lossy(&bytes).into_owned() })
}

fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    for _ in 0..60 {
        if let Some(r) = try_call(port, method, path, body) {
            return r;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{method} {path}: узел на {port} не ответил");
}

fn spawn(dir: &str, port: u16, extra: &[String]) -> Node {
    spawn_with_fail(dir, port, extra, None)
}

fn spawn_with_fail(dir: &str, port: u16, extra: &[String], failpoint: Option<&str>) -> Node {
    std::fs::create_dir_all(dir).unwrap();
    let mut args = vec![
        "serve".to_string(),
        dir.to_string(),
        port.to_string(),
        "--single-index".to_string(),
        "--fields=kind:keyword,body:1".to_string(),
        "--flush=100000".to_string(),
    ];
    args.extend(extra.iter().cloned());
    let log = std::fs::File::create(format!("{dir}.log")).expect("файл вывода");
    let mut command = Command::new(BIN);
    command.env("SVINDB_DISK_FLOOR_MB", "1");
    if let Some(point) = failpoint {
        command.env("SVINDB_FAILPOINTS", point);
    }
    let child = command
        .args(&args)
        .stdout(Stdio::from(log.try_clone().expect("вывод")))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let node = Node { child, port, dir: dir.to_string() };
    for _ in 0..400 {
        if TcpStream::connect(("127.0.0.1", node.port)).is_ok() {
            return node;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("узел {dir} не открыл порт {port}");
}

struct Stand {
    root: String,
    primary: Node,
    replicas: Vec<Node>,
}

impl Drop for Stand {
    fn drop(&mut self) {
        // Сначала останавливаются владельцы файлов, затем удаляется каталог.
        // Прежний порядок делал наоборот: `remove_dir_all` шёл по дереву, пока
        // три процесса ещё писали WAL, manifest и логи. На медленной CI-машине
        // это оставляло уборку зависимой от планировщика и отнимало у следующего
        // теста дескрипторы/дисковую полосу.
        self.primary.kill();
        for replica in &mut self.replicas {
            replica.kill();
        }
        std::fs::remove_dir_all(&self.root).ok();
    }
}

/// Primary с двумя репликами. Двух, а не одной: при одной `quorum` от двух — это
/// двое, то есть неотличимо от `all`, и договор перестаёт проверяться.
fn stand(tag: &str, ack: &str) -> Stand {
    let root = std::env::temp_dir()
        .join(format!("svindb-реплика-{tag}-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let replicas: Vec<Node> =
        (0..2).map(|i| spawn(&format!("{root}/копия{i}"), free_port(), &[])).collect();
    let list =
        replicas.iter().map(|r| format!("127.0.0.1:{}", r.port)).collect::<Vec<_>>().join(",");
    let primary = spawn(
        &format!("{root}/главный"),
        free_port(),
        &[format!("--replicas={list}"), "--term=1".into(), format!("--ack={ack}")],
    );
    Stand { root, primary, replicas }
}

fn put(port: u16, id: &str) -> Reply {
    let body = format!(r#"{{"documents":[{{"id":"{id}","kind":"груз","body":"тело {id}"}}]}}"#);
    call(port, "POST", "/_bulk", Some(&body))
}

fn found(port: u16, id: &str) -> bool {
    let r = read_doc(port, id);
    r.json()["docs"][0]["found"].as_bool() == Some(true)
}

fn read_doc(port: u16, id: &str) -> Reply {
    call(port, "POST", "/_mget", Some(&format!(r#"{{"ids":["{id}"]}}"#)))
}

/// Записанное primary видно **на репликах**, а не только в его ответе.
#[test]
fn what_the_primary_accepts_the_replicas_hold() {
    let s = stand("держат", "all");
    let r = put(s.primary.port, "д1");
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(r.json()["ack"].as_str(), Some("all"), "договор не назван: {}", r.body);
    assert_eq!(r.json()["acked"].as_i64(), Some(3), "{}", r.body);
    assert_eq!(r.json()["of"].as_i64(), Some(3), "{}", r.body);

    for (i, rep) in s.replicas.iter().enumerate() {
        assert!(found(rep.port, "д1"), "копия {i} не получила документ");
    }

    // Гашение реплицируется тем же путём.
    let r = call(s.primary.port, "POST", "/_delete", Some(r#"{"ids":["д1"]}"#));
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(r.json()["acked"].as_i64(), Some(3), "{}", r.body);
    for (i, rep) in s.replicas.iter().enumerate() {
        assert!(!found(rep.port, "д1"), "копия {i} держит погашенное");
    }
}

/// Недостача подтверждений — не `200`, и недостающий назван поимённо.
#[test]
fn a_short_ack_is_not_success() {
    let mut s = stand("недостача", "quorum");
    // Большинство от троих — двое: с одной убитой репликой договор ещё выполним.
    s.replicas[1].kill();
    let r = put(s.primary.port, "д2");
    assert_eq!(r.code, 200, "кворум из двоих не собрался: {}", r.body);
    assert_eq!(r.json()["acked"].as_i64(), Some(2), "{}", r.body);
    let missing = r.json()["missing"].as_array().cloned().unwrap_or_default();
    assert_eq!(missing.len(), 1, "недостающий не назван: {}", r.body);

    // А с двумя убитыми — уже нет, и это обязано быть видно кодом ответа.
    s.replicas[0].kill();
    let r = put(s.primary.port, "д3");
    assert_eq!(r.code, 503, "запись без кворума выдана за принятую: {}", r.body);
    assert_eq!(r.json()["acked"].as_i64(), Some(1), "{}", r.body);
    assert_eq!(r.json()["of"].as_i64(), Some(3), "{}", r.body);

    // И при этом у primary документ **есть**: ответ говорит правду про
    // договор, а не про то, что запись не состоялась.
    assert!(found(s.primary.port, "д3"), "primary потерял то, что применил");
}

/// Смещённый primary останавливается репликой, а не собственной совестью.
///
/// Здесь весь смысл терма. Второй primary поднимается с термом `2` над той же
/// репликой; первый об этом не знает и продолжает писать. Реплика обязана его
/// отвергнуть — и отвергнуть **сама**, ни у кого не спрашивая.
#[test]
fn a_deposed_primary_is_stopped_by_the_replica() {
    let s = stand("split-brain", "all");
    assert_eq!(put(s.primary.port, "до").code, 200);

    // Второй primary над той же репликой, терм выше.
    let root = s.root.clone();
    let addr = format!("127.0.0.1:{}", s.replicas[0].port);
    let _new = spawn(
        &format!("{root}/новый"),
        free_port(),
        &[format!("--replicas={addr}"), "--term=2".into(), "--ack=all".into()],
    );
    assert_eq!(put(_new.port, "после").code, 200, "новый primary не смог писать");

    // Прежний primary всё ещё в терме 1 — и получает отказ от реплики.
    let r = put(s.primary.port, "поздно");
    assert_eq!(r.code, 503, "запись смещённого primary принята репликой: {}", r.body);
    let missing = r.json()["missing"].as_array().cloned().unwrap_or_default();
    let why =
        missing.iter().map(|m| m["error"].as_str().unwrap_or("")).collect::<Vec<_>>().join(" ");
    assert!(why.contains("409") || why.contains("терм"), "отказ не объясняется термом: {}", r.body);
    // Реплика не приняла — значит и не держит.
    assert!(!found(s.replicas[0].port, "поздно"), "реплика приняла запись смещённого");
}

/// Вернувшаяся копия догоняется, а не остаётся дырявой навсегда.
///
/// ## Что здесь проверяется
///
/// Реплику убивают, пропускают мимо неё две записи и поднимают обратно. У неё на
/// диске прежний номер, primary предлагает номер через два — разрыв. Реплика
/// обязана его назвать, а primary — досчитать пропущенное и повторить текущее.
///
/// Доказательство — **содержимое копии**, а не код ответа: после догона у неё
/// обязаны быть все три документа, включая тот, которого она не видела вовсе.
#[test]
fn a_replica_that_fell_behind_is_caught_up() {
    let mut s = stand("догон", "one");
    assert_eq!(put(s.primary.port, "р1").code, 200);

    let port = s.replicas[0].port;
    let dir = s.replicas[0].dir.clone();
    s.replicas[0].kill();
    // Эти две до неё не дойдут: договор `one` выполнен одним primary.
    assert_eq!(put(s.primary.port, "р2").code, 200);
    assert_eq!(put(s.primary.port, "р3").code, 200);
    // Поднимаем ту же копию на том же каталоге и порту: номер она помнит.
    s.replicas[0] = spawn(&dir, port, &[]);
    assert!(!found(port, "р2"), "копия каким-то образом получила пропущенное");

    let r = put(s.primary.port, "р4");
    assert_eq!(r.code, 200, "{}", r.body);
    assert!(
        r.json()["missing"].as_array().map(|m| m.is_empty()).unwrap_or(true),
        "догон не состоялся: {}",
        r.body
    );
    for id in ["р1", "р2", "р3", "р4"] {
        assert!(found(port, id), "после догона копия не держит {id}");
    }
}

/// Догон, который невозможен, назван невозможным.
///
/// Кольцо разосланного не бесконечно: копия, отставшая настолько, что нужные
/// записи из него вытеснены, пересылкой не лечится — её лечит перенос сегментов,
/// которого нет. Здесь проверяется, что этот случай **называется**, а не
/// подменяется тишиной или частичным догоном.
#[test]
fn a_catchup_that_cannot_work_says_so() {
    let root = std::env::temp_dir()
        .join(format!("svindb-реплика-невозможно-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let mut copy = spawn(&format!("{root}/копия"), free_port(), &[]);
    let addr = format!("127.0.0.1:{}", copy.port);
    // Кольцо в ноль мегабайт: держится последняя запись и только она.
    let primary = spawn(
        &format!("{root}/главный"),
        free_port(),
        &[
            format!("--replicas={addr}"),
            "--term=1".into(),
            "--ack=one".into(),
            "--replicate-keep-mb=0".into(),
        ],
    );
    assert_eq!(put(primary.port, "н1").code, 200);

    let port = copy.port;
    let dir = copy.dir.clone();
    copy.kill();
    for id in ["н2", "н3", "н4"] {
        assert_eq!(put(primary.port, id).code, 200);
    }
    let _copy = spawn(&dir, port, &[]);

    let r = put(primary.port, "н5");
    let missing = r.json()["missing"].as_array().cloned().unwrap_or_default();
    let why =
        missing.iter().map(|m| m["error"].as_str().unwrap_or("")).collect::<Vec<_>>().join(" ");
    assert!(why.contains("вытеснены"), "невозможный догон не назван невозможным: {}", r.body);
    // И копия действительно осталась без пропущенного — тишины не было.
    assert!(!found(port, "н3"), "копия получила то, чего доставить было нечем");
    std::fs::remove_dir_all(&root).ok();
}

/// Одновременные записи доходят до копии в том же порядке, в каком пронумерованы.
///
/// ## Почему это не «просто потокобезопасность»
///
/// Реплика принимает запись, только если её номер идёт сразу за применённым —
/// на этом стоит обнаружение дыры. Значит порядок доставки не пожелание, а
/// условие работы: выдай номера в одном порядке, а отправь в другом — и вторая
/// запись получит отказ по разрыву, которого нет. Проверяется исход, который
/// это ловит: **все** одновременные записи приняты и **все** видны на копии.
#[test]
fn concurrent_writes_reach_the_replica_in_order() {
    let s = stand("одновременно", "all");
    const N: usize = 24;
    let port = s.primary.port;
    let bad = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for i in 0..N {
            let bad = std::sync::Arc::clone(&bad);
            scope.spawn(move || {
                let r = put(port, &format!("о{i:03}"));
                if r.code != 200 {
                    eprintln!("о{i:03}: {} {}", r.code, r.body);
                    bad.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(
        bad.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "одновременные записи получили отказ по разрыву, которого не было"
    );
    for i in 0..N {
        let id = format!("о{i:03}");
        assert!(found(s.replicas[0].port, &id), "копия не держит {id}");
        assert!(found(s.replicas[1].port, &id), "вторая копия не держит {id}");
    }
}

/// Перезапуск primary в том же терме не обрывает репликацию.
///
/// ## Что здесь ловится
///
/// Свой исходящий номер primary держит в памяти. Пока при подъёме он начинал с
/// единицы, перезапуск уводил его в разрыв со всеми живыми копиями сразу: они на
/// номере K, он шлёт первый — отказ, догон из свежего пустого кольца ничего не
/// находит, и **вся** дальнейшая репликация прекращается. При `--ack=one`
/// клиент этого не видит вовсе: primary считает себя сам и отвечает `200`.
///
/// Доказательство — содержимое копии после перезапуска, а не код ответа.
#[test]
fn a_restarted_primary_keeps_replicating() {
    let mut s = stand("перезапуск-primary", "all");
    for i in 0..5 {
        assert_eq!(put(s.primary.port, &format!("до{i}")).code, 200);
    }

    let dir = s.primary.dir.clone();
    let port = s.primary.port;
    let list =
        s.replicas.iter().map(|r| format!("127.0.0.1:{}", r.port)).collect::<Vec<_>>().join(",");
    s.primary.kill();
    s.primary =
        spawn(&dir, port, &[format!("--replicas={list}"), "--term=1".into(), "--ack=all".into()]);

    for i in 0..5 {
        let r = put(s.primary.port, &format!("после{i}"));
        assert_eq!(r.code, 200, "после перезапуска запись не реплицируется: {}", r.body);
        assert_eq!(
            r.json()["acked"].as_i64(),
            Some(3),
            "копии не подтвердили запись перезапущенного primary: {}",
            r.body
        );
    }
    for rep in s.replicas.iter() {
        for i in 0..5 {
            for (id, message) in
                [(format!("до{i}"), "копия потеряла"), (format!("после{i}"), "копия не получила")]
            {
                let got = read_doc(rep.port, &id);
                assert_eq!(
                    got.code, 200,
                    "{message} {id}: _mget ответил {}: {}",
                    got.code, got.body
                );
                assert_eq!(
                    got.json()["docs"][0]["found"].as_bool(),
                    Some(true),
                    "{message} {id}: {}",
                    got.body
                );
            }
        }
    }
}

/// Негодное тело не двигает отметку применённого.
///
/// Реплика пишет терм на диск до применения — он и есть защита от split brain.
/// Отметку применённого она обязана ставить **после**: иначе любой отказ
/// применения оставлял бы «применено до N» без N, и обнаружитель разрыва молчал
/// бы навсегда, потому что номера сходятся.
#[test]
fn a_body_that_fails_to_apply_does_not_advance_the_mark() {
    let s = stand("отметка-после", "one");
    assert_eq!(put(s.primary.port, "т1").code, 200);
    let rep = s.replicas[0].port;

    // Запись с негодным телом: терм и номер верные, документов нет ни одного —
    // тело не разберётся как документ.
    let bad = r#"{"term":1,"at":2,"op":"add","body":"не json вовсе"}"#;
    let r = call(rep, "POST", "/_replicate", Some(bad));
    assert!(r.code >= 400, "негодное тело принято: {} {}", r.code, r.body);

    // Тот же номер с годным телом обязан примениться, а не считаться повтором.
    let good = format!(
        r#"{{"term":1,"at":2,"op":"add","body":{}}}"#,
        serde_json::Value::String(
            r#"{"documents":[{"id":"т2","kind":"груз","body":"тело т2"}]}"#.into()
        )
    );
    let r = call(rep, "POST", "/_replicate", Some(&good));
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(
        r.json()["applied"].as_bool(),
        Some(true),
        "запись сочтена повтором, хотя применена не была: {}",
        r.body
    );
    assert!(found(rep, "т2"), "копия не держит применённое");
}

/// Повтор после падения между применением записи на копии и durable-отметкой
/// не создаёт второй живой экземпляр bulk-документа.
#[cfg(feature = "failpoints")]
#[test]
fn a_replica_retry_after_apply_before_done_is_idempotent() {
    let root = std::env::temp_dir()
        .join(format!("svindb-реплика-повтор-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    let dir = format!("{root}/копия");
    let port = free_port();
    let mut copy =
        spawn_with_fail(&dir, port, &[], Some("replication.after_apply_before_done=kill"));
    let record = format!(
        r#"{{"term":1,"at":1,"op":"add","body":{}}}"#,
        serde_json::Value::String(
            r#"{"documents":[{"id":"retry-one","kind":"retry","body":"одна версия"}]}"#.into(),
        )
    );
    let first = try_call(port, "POST", "/_replicate", Some(&record));
    assert!(first.is_none(), "точка отказа не оборвала первый ответ");
    let status = copy.child.wait().expect("копия не дождалась конца");
    assert!(!status.success(), "точка отказа не сняла копию: {status}");
    drop(copy);

    let copy = spawn(&dir, port, &[]);
    let retried = call(port, "POST", "/_replicate", Some(&record));
    assert_eq!(retried.code, 200, "повтор не применился: {}", retried.body);
    assert_eq!(retried.json()["applied"], true, "повтор не завершил отметку: {}", retried.body);
    assert!(found(port, "retry-one"), "повтор не оставил документ на копии");
    for _ in 0..60 {
        let flush = call(port, "POST", "/_flush", Some("{}"));
        assert!(matches!(flush.code, 200 | 202), "flush копии отказал: {}", flush.body);
        if flush.code == 200 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let search = call(
        port,
        "POST",
        "/",
        Some(r#"{"k":100,"ids":true,"must":[{"field":"kind","terms":["retry"]}]}"#),
    );
    assert_eq!(search.code, 200, "поиск на копии отказал: {}", search.body);
    let decoded = search.json();
    let ids: Vec<&str> = decoded["hits"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|hit| hit["id"].as_str())
        .collect();
    assert_eq!(ids, vec!["retry-one"], "повтор создал вторую живую версию: {ids:?}");
    drop(copy);
    std::fs::remove_dir_all(&root).ok();
}
