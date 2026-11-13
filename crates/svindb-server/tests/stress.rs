//! Стенд под разнородной нагрузкой: все пути узла работают одновременно.
//!
//! ## Зачем не «много одинаковых запросов»
//!
//! Однородная нагрузка меряет пропускную способность и почти ничего не находит:
//! она держит узел в одном состоянии. Ломается же он на **стыках** — когда
//! замена идёт под слиянием, точечное чтение под публикацией, а удаление под
//! сбросом. Поэтому здесь одновременно работают приём пачками, замена с CAS,
//! удаление, поиск, точечное чтение, явная публикация и правка настроек, а
//! пороги подобраны так, чтобы сегменты сменялись и сливались всё это время.
//!
//! ## Что проверяется в конце
//!
//! Не «узел не упал», а сходимость: у каждого документа ровно одна живая версия
//! и ровно та, ответ на которую был получен. Всё, что клиент считает
//! подтверждённым, обязано найтись — и поиском, и точкой.

use std::cell::Cell;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

/// Сколько длится нагрузка. Не больше: стенд обязан оставаться тем, что гоняют
/// на каждой правке, а не раз в месяц.
const LOAD: Duration = Duration::from_secs(20);

struct Node {
    child: Child,
    port: u16,
    root: String,
}

impl Node {
    fn restart(&mut self) {
        // `kill` уже умершего процесса — не ошибка стенда: узел мог уйти сам,
        // сработав точкой отказа. Отличать эти два случая обязан тест, а не
        // подъёмник.
        let _ = self.child.kill();
        self.child.wait().unwrap();
        self.revive();
    }

    /// Поднимает узел заново **без** взведённых точек отказа.
    ///
    /// Отдельно от [`Node::restart`]: подъём после сработавшей точки обязан
    /// идти обычным двоичным файлом, иначе он умер бы там же, где и в прошлый
    /// раз, и проверялось бы не восстановление, а повтор падения.
    fn revive(&mut self) {
        self.child = launch_with(&format!("{}/index", self.root), self.port, None);
        for _ in 0..400 {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("узел не поднялся после обрыва");
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

/// Итог по разрядам ответов. Отказ перегрузки — не ошибка стенда, но и не
/// невидимка: узел обязан отказывать явно и считаемо.
#[derive(Default)]
struct Tally {
    ok: AtomicU64,
    /// `409` — проигранный CAS. Законен и ожидаем.
    conflict: AtomicU64,
    /// `429`/`503` — обратное давление. Законно, но считается отдельно.
    backpressure: AtomicU64,
    /// `404` от точечного чтения. Законен: читатель ходит по идентификаторам,
    /// которые потоки замен ещё только заводят, и «такого нет» — правильный
    /// ответ, а не отказ. Для прочих точек `404` остаётся отказом.
    missing: AtomicU64,
    /// Всё прочее не-2xx. Каждый такой ответ — повод разбираться.
    refused: Mutex<Vec<String>>,
    /// Сорванные соединения. Ожидаемы только вокруг обрыва узла.
    dropped: AtomicU64,
    /// Место, на котором клиент потерял транспорт. Один счётчик `dropped`
    /// доказывает нарушение контракта, но не отличает отказ `connect` от
    /// закрытия без status line или обрыва посреди тела ответа. Без этого
    /// release-only гонка превращается в гадание по общему числу ошибок.
    drop_phases: Mutex<HashMap<&'static str, u64>>,
}

impl Tally {
    fn note(&self, what: &str, r: Option<Reply>) -> Option<Reply> {
        match &r {
            None => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                let phase = LAST_TRANSPORT_FAILURE.get();
                *self.drop_phases.lock().expect("разряды обрывов").entry(phase).or_default() += 1;
            }
            Some(r) if r.code < 300 => {
                self.ok.fetch_add(1, Ordering::Relaxed);
            }
            Some(r) if r.code == 409 => {
                self.conflict.fetch_add(1, Ordering::Relaxed);
            }
            Some(r) if r.code == 429 || r.code == 503 => {
                self.backpressure.fetch_add(1, Ordering::Relaxed);
            }
            Some(r) if r.code == 404 && what == "doc" => {
                self.missing.fetch_add(1, Ordering::Relaxed);
            }
            Some(r) => {
                let mut bad = self.refused.lock().expect("список отказов");
                if bad.len() < 32 {
                    bad.push(format!("{what}: {} {}", r.code, r.body));
                }
            }
        }
        r
    }
}

thread_local! {
    /// Последняя транспортная ошибка текущего клиентского потока. Все вызовы
    /// `Tally::note` оборачивают `try_call` синхронно в том же потоке, поэтому
    /// значение не может быть перепутано с соседним запросом.
    static LAST_TRANSPORT_FAILURE: Cell<&'static str> = const { Cell::new("неизвестно") };
}

fn dropped_at<T>(phase: &'static str) -> Option<T> {
    LAST_TRANSPORT_FAILURE.set(phase);
    None
}

/// Долгоживущий клиент одного нагрузочного потока.
///
/// Release-сборка успевает выполнить десятки тысяч коротких запросов за
/// двадцать секунд. Если каждый раз открывать `Connection: close`, macOS
/// исчерпывает 16 384 исходящих порта, пока старые сокеты лежат в `TIME_WAIT`.
/// Это ошибка нагрузчика до `connect`, а не обрыв со стороны узла. Реальные
/// клиенты переиспользуют соединения; сервер сам ограничивает каждое сотней
/// запросов и сообщает о закрытии заголовком. Здесь проверяется ровно этот
/// контракт: молчаливое закрытие всё ещё даст транспортную ошибку.
struct Client {
    port: u16,
    input: Option<BufReader<TcpStream>>,
}

impl Client {
    fn new(port: u16) -> Self {
        Self { port, input: None }
    }

    fn try_call(&mut self, method: &str, path: &str, body: Option<&str>) -> Option<Reply> {
        LAST_TRANSPORT_FAILURE.set("неизвестно");
        if self.input.is_none() {
            let socket = match TcpStream::connect(("127.0.0.1", self.port)) {
                Ok(socket) => socket,
                Err(_) => return dropped_at("connect"),
            };
            socket.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
            socket.set_write_timeout(Some(Duration::from_secs(30))).ok()?;
            self.input = Some(BufReader::new(socket));
        }
        let result =
            exchange(self.input.as_mut().expect("соединение заведено"), method, path, body, true);
        match result {
            Some((reply, close)) => {
                if close {
                    self.input = None;
                }
                Some(reply)
            }
            None => {
                self.input = None;
                None
            }
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Запрос, который **не** роняет поток на сорванном соединении: под обрывом узла
/// это обычное дело, и падать здесь значило бы проверять живучесть стенда, а не
/// узла.
fn try_call(port: u16, method: &str, path: &str, body: Option<&str>) -> Option<Reply> {
    LAST_TRANSPORT_FAILURE.set("неизвестно");
    let socket = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(socket) => socket,
        Err(_) => return dropped_at("connect"),
    };
    socket.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    socket.set_write_timeout(Some(Duration::from_secs(30))).ok()?;
    let mut input = BufReader::new(socket);
    exchange(&mut input, method, path, body, false).map(|(reply, _)| reply)
}

/// Один HTTP/1.1 обмен на уже открытом сокете. Второй элемент результата
/// сообщает, велел ли сервер закрыть соединение после полного ответа.
fn exchange(
    input: &mut BufReader<TcpStream>,
    method: &str,
    path: &str,
    body: Option<&str>,
    keep: bool,
) -> Option<(Reply, bool)> {
    let body = body.unwrap_or("");
    if write!(
        input.get_mut(),
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: {}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        if keep { "keep-alive" } else { "close" },
        body.len()
    )
    .is_err()
    {
        return dropped_at("request_write");
    }
    if input.get_mut().flush().is_err() {
        return dropped_at("request_flush");
    }
    let mut status = String::new();
    match input.read_line(&mut status) {
        Ok(0) => return dropped_at("status_eof"),
        Ok(_) => {}
        Err(_) => return dropped_at("status_read"),
    }
    let Some(code) = status.split_whitespace().nth(1).and_then(|s| s.parse().ok()) else {
        return dropped_at("status_parse");
    };
    let mut len = None;
    let mut close = false;
    loop {
        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) => return dropped_at("headers_eof"),
            Ok(_) if line.trim().is_empty() => break,
            Ok(_) => {}
            Err(_) => return dropped_at("headers_read"),
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                len = value.trim().parse::<usize>().ok();
            } else if name.eq_ignore_ascii_case("connection") {
                close = value.trim().eq_ignore_ascii_case("close");
            }
        }
    }
    let mut bytes = vec![0; len.unwrap_or(0)];
    if input.read_exact(&mut bytes).is_err() {
        return dropped_at("body_read");
    }
    Some((Reply { code, body: String::from_utf8_lossy(&bytes).into_owned() }, close))
}

fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    for _ in 0..40 {
        if let Some(r) = try_call(port, method, path, body) {
            return r;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{method} {path}: узел не ответил");
}

/// Подъём узла. `failpoints` — взведённые точки отказа либо `None`.
///
/// Точки задаются средой, а не переключателем внутри процесса: проверки
/// долговечности поднимают **настоящий** двоичный файл отдельным процессом, и
/// иначе до него не дотянуться.
fn launch_with(index: &str, port: u16, failpoints: Option<&str>) -> Child {
    // Вывод узла не глушится, а собирается рядом с индексом: под нагрузкой он —
    // единственный, кто расскажет про отказ обслуживания или карты живых.
    let log = std::fs::File::create(format!("{index}.log"))
        .or_else(|_| {
            std::fs::create_dir_all(index)
                .and_then(|_| std::fs::File::create(format!("{index}.log")))
        })
        .expect("файл вывода узла");
    let mut cmd = Command::new(BIN);
    // Запас места сведён к минимуму: проверки поднимают узлы на той же машине,
    // где идёт сборка, а правило «пять процентов ёмкости» на полутерабайтном
    // томе резервирует двадцать три гигабайта. Узел, отказывающий от всякой
    // записи, проверял бы не то, ради чего заведён.
    cmd.env("SVINDB_DISK_FLOOR_MB", "1");
    match failpoints {
        Some(spec) => cmd.env("SVINDB_FAILPOINTS", spec),
        // Снимаем унаследованное: соседний прогон матрицы не имеет права
        // взводить точки в узле, который поднимается уже без них.
        None => cmd.env_remove("SVINDB_FAILPOINTS"),
    };
    cmd.args([
        "serve",
        index,
        &port.to_string(),
        "--single-index",
        "--fields=kind:keyword,revision:int64,state:keyword,body:1,weight:double",
        // Порог сегмента низкий намеренно: сегменты обязаны сменяться и
        // сливаться прямо во время нагрузки, иначе слияние и уборка не
        // проверяются вовсе.
        "--flush=2000",
        "--target=8000",
    ])
    .stdout(Stdio::from(log.try_clone().expect("вывод")))
    .stderr(Stdio::from(log))
    .spawn()
    .unwrap()
}

fn start(tag: &str) -> Node {
    start_with(tag, None)
}

/// Тот же стенд, но узел поднимается со взведёнными точками отказа.
#[cfg(feature = "failpoints")]
fn start_with_fail(tag: &str, failpoints: &str) -> Node {
    start_with(tag, Some(failpoints))
}

fn start_with(tag: &str, failpoints: Option<&str>) -> Node {
    let port = free_port();
    let root = std::env::temp_dir()
        .join(format!("svindb-стресс-{tag}-{}-{port}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();
    let child = launch_with(&format!("{root}/index"), port, failpoints);
    let mut node = Node { child, port, root };
    for _ in 0..400 {
        if let Ok(Some(status)) = node.child.try_wait() {
            panic!("узел завершился до открытия порта: {status}");
        }
        if TcpStream::connect(("127.0.0.1", node.port)).is_ok() {
            let r = call(
                node.port,
                "PUT",
                "/_settings",
                Some(
                    r#"{"refresh_interval_ms":100,"refresh_min_docs":500,
                        "refresh_max_interval_ms":3000,"live_max_docs":20000,
                        "flush_wait_ms":5000,"wal_group_commit_ms":3,
                        "merge_policy":"update_heavy","merge_at_once":8,
                        "dead_fraction":0.2}"#,
                ),
            );
            assert_eq!(r.code, 200, "настройки стенда: {}", r.body);
            return node;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("узел не открыл порт {}", node.port);
}

/// Дешёвый разброс без генератора случайных чисел: стенд обязан быть
/// воспроизводимым, а «разное» здесь нужно только чтобы не ходить по одной
/// дорожке.
fn spread(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn doc(id: &str, kind: &str, revision: u64, n: u64) -> String {
    format!(
        r#"{{"id":"{id}","kind":"{kind}","revision":{revision},"state":"running",
             "body":"нагрузка тело документа номер {n} поиск индекс",
             "weight":{}.5}}"#,
        n % 100
    )
}

/// Что каждый поток замен довёл до подтверждённого состояния.
type Owned = HashMap<String, u64>;

#[test]
fn mixed_load_converges_to_exactly_one_version_per_document() {
    let node = start("смесь");
    let port = node.port;
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(Tally::default());
    let deadline = Instant::now() + LOAD;

    let mut handles = Vec::new();
    let owned: Arc<Mutex<Owned>> = Arc::new(Mutex::new(HashMap::new()));

    // 1. Замена с CAS. Четыре потока, у каждого свои идентификаторы: так
    //    подтверждённая версия известна точно, без общей блокировки.
    for worker in 0..4u64 {
        let (stop, tally, owned) = (Arc::clone(&stop), Arc::clone(&tally), Arc::clone(&owned));
        handles.push(std::thread::spawn(move || {
            let mut client = Client::new(port);
            let ids: Vec<String> = (0..8).map(|n| format!("lease/{worker}/{n}")).collect();
            let mut at: HashMap<String, u64> = HashMap::new();
            while !stop.load(Ordering::Relaxed) {
                for id in ids.iter() {
                    let was = at.get(id).copied();
                    let next = was.unwrap_or(0) + 1;
                    let condition = match was {
                        Some(v) => format!(
                            r#","conditions":[{{"id":"{id}","field":"revision","equals":{v}}}]"#
                        ),
                        None => String::new(),
                    };
                    let body =
                        format!(r#"{{"documents":[{}]{condition}}}"#, doc(id, "lease", next, next));
                    let r = tally.note("put", client.try_call("POST", "/_put", Some(&body)));
                    // Версия запоминается **только** по ответу `200`. В этом вся
                    // проверка: в конце сверяется именно подтверждённое.
                    if r.is_some_and(|r| r.code == 200) {
                        at.insert(id.clone(), next);
                    }
                }
            }
            owned.lock().expect("итог замен").extend(at);
        }));
    }

    // 2. Приём пачками: документы копятся, сегменты сменяются, слияние работает.
    let inserted = Arc::new(AtomicU64::new(0));
    // Докуда приёмщик ноль довёл **подтверждённую** нумерацию. По ней удаление
    // выбирает, что можно удалять: удали оно то, что ещё не принято, и документ
    // появился бы уже после удаления — стенд считал бы это воскрешением, хотя
    // виноват был бы сам.
    let bulk0 = Arc::new(AtomicU64::new(0));
    for worker in 0..2u64 {
        let (stop, tally, inserted, bulk0) =
            (Arc::clone(&stop), Arc::clone(&tally), Arc::clone(&inserted), Arc::clone(&bulk0));
        handles.push(std::thread::spawn(move || {
            let mut client = Client::new(port);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let batch: Vec<String> = (0..50)
                    .map(|k| {
                        let at = n * 50 + k;
                        doc(&format!("bulk/{worker}/{at:06}"), "bulk", 1, at)
                    })
                    .collect();
                let body = format!(r#"{{"documents":[{}]}}"#, batch.join(","));
                let r = tally.note("bulk", client.try_call("POST", "/_bulk", Some(&body)));
                if r.is_some_and(|r| r.code == 200) {
                    inserted.fetch_add(50, Ordering::Relaxed);
                    n += 1;
                    if worker == 0 {
                        bulk0.store(n * 50, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    // 3. Удаление: гашение идёт немедленно и обязано не задеть соседей.
    let deleted = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        let (stop, tally, deleted, bulk0) =
            (Arc::clone(&stop), Arc::clone(&tally), Arc::clone(&deleted), Arc::clone(&bulk0));
        handles.push(std::thread::spawn(move || {
            let mut client = Client::new(port);
            // Курсор идёт только по уже подтверждённой нумерации приёмщика ноль:
            // удалять непринятое бессмысленно, а стенд от этого начал бы врать.
            let mut cursor = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let done_upto = bulk0.load(Ordering::Relaxed);
                let mut batch: Vec<String> = Vec::new();
                while batch.len() < 5 && cursor + 10 <= done_upto {
                    batch.push(format!("bulk/0/{cursor:06}"));
                    cursor += 10;
                }
                if batch.is_empty() {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                let quoted: Vec<String> = batch.iter().map(|id| format!("\"{id}\"")).collect();
                let body = format!(r#"{{"ids":[{}]}}"#, quoted.join(","));
                let r = tally.note("delete", client.try_call("POST", "/_delete", Some(&body)));
                if r.is_some_and(|r| r.code == 200) {
                    deleted.lock().expect("список удалённых").extend(batch);
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }));
    }

    // 4. Поиск: разные ветви разбора, а не один и тот же запрос.
    for shape in 0..3u64 {
        let (stop, tally) = (Arc::clone(&stop), Arc::clone(&tally));
        handles.push(std::thread::spawn(move || {
            let mut client = Client::new(port);
            let mut seed = 0x5eed + shape;
            while !stop.load(Ordering::Relaxed) {
                let body = match spread(&mut seed) % 4 {
                    0 => r#"{"k":20,"ids":true,"must":[{"field":"kind","terms":["lease"]}]}"#
                        .to_string(),
                    1 => r#"{"k":20,"ids":true,"words":[{"field":"body","terms":["нагрузка","индекс"]}],
                            "filter":[{"field":"weight","from":0,"to":50}]}"#
                        .to_string(),
                    2 => r#"{"k":20,"ids":true,"must":[{"field":"kind","terms":["bulk"]}],
                            "sort":[{"field":"revision","order":"desc"}]}"#
                        .to_string(),
                    _ => r#"{"k":5,"aggs":{"по_виду":{"terms":{"field":"kind","size":5}}},
                            "filter":[{"field":"revision"}]}"#
                        .to_string(),
                };
                tally.note("search", client.try_call("POST", "/", Some(&body)));
            }
        }));
    }

    // 5. Точечное чтение по тем же идентификаторам, которые прямо сейчас
    //    заменяются: это проверка порядка замков, а не скорости.
    {
        let (stop, tally) = (Arc::clone(&stop), Arc::clone(&tally));
        handles.push(std::thread::spawn(move || {
            let mut client = Client::new(port);
            let mut seed = 0xd0c;
            while !stop.load(Ordering::Relaxed) {
                let w = spread(&mut seed) % 4;
                let n = spread(&mut seed) % 8;
                let single = format!("/_doc/lease%2F{w}%2F{n}");
                tally.note("doc", client.try_call("GET", &single, None));
                let ids: Vec<String> = (0..8).map(|k| format!("\"lease/{w}/{k}\"")).collect();
                let body = format!(r#"{{"ids":[{}]}}"#, ids.join(","));
                tally.note("mget", client.try_call("POST", "/_mget", Some(&body)));
            }
        }));
    }

    // 6. Явная публикация и правка настроек на ходу: обе точки трогают состав.
    {
        let (stop, tally) = (Arc::clone(&stop), Arc::clone(&tally));
        handles.push(std::thread::spawn(move || {
            let mut client = Client::new(port);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(400));
                tally.note("flush", client.try_call("POST", "/_flush", None));
                n += 1;
                if n.is_multiple_of(3) {
                    // Порог живого сегмента меняется на ходу: путь «настройка
                    // применяется немедленно» обязан работать под нагрузкой.
                    let live = if n.is_multiple_of(6) { 20000 } else { 4000 };
                    let body = format!(r#"{{"live_max_docs":{live}}}"#);
                    tally.note("settings", client.try_call("PUT", "/_settings", Some(&body)));
                }
                tally.note("health", client.try_call("GET", "/_health", None));
                tally.note("metrics", client.try_call("GET", "/_metrics", None));
            }
        }));
    }

    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("поток нагрузки упал");
    }

    verify(&node, &owned.lock().unwrap(), &deleted.lock().unwrap(), &tally, &inserted);
}

/// Падение между публикацией сегмента и переносом отметки журнала.
///
/// ## Почему обычного `kill -9` мало
///
/// Окно живёт миллисекунды: сегмент уже внесён в состав и долговечен, а отметка
/// журнала ещё стоит там, где стояла. Кампания на десяти миллионах попала в него
/// случайно и записала сто отказов оракула из ста проверенных — но соседний
/// прогон того же стенда мог бы не попасть ни разу. Здесь окно названо по имени
/// (`manifest.after_publish`), поэтому проверка воспроизводима.
///
/// ## Почему приём пачками, а не замена
///
/// Соседний обрывной стенд гоняет `/_put`, и это **самозалечивающийся** путь:
/// переигрывание замены проходит через гашение прежней версии, поэтому лишняя
/// копия убивает сама себя. У `/_bulk` такого шага нет — запись переигрывается
/// как новый документ, и у идентификатора становится две живые версии. Именно
/// этот путь и дал дубли на кампании.
///
/// ## Что он поймал
///
/// До полосного переигрывания: 4700 строк выдачи на 2700 различных
/// идентификаторов — 2000 повторов, ровно один опубликованный сегмент.
/// `replayed_adds=2700`, `dead=0`: подъём добавлял заново всё, не погасив
/// ничего. Тест был написан красным намеренно и стал зелёным **от исправления**,
/// а не от правки самого себя.
#[cfg(feature = "failpoints")]
#[test]
fn a_kill_between_publish_and_checkpoint_leaves_one_live_version() {
    let mut node = start_with_fail("публикация", "manifest.after_publish=kill");
    let port = node.port;

    // Льём пачками, пока узел не исчезнет. Порог сегмента — 2000 документов,
    // значит первый сброс случится задолго до конца отпущенного.
    const BATCH: usize = 100;
    let mut acked: Vec<String> = Vec::new();
    let mut batch = 0u64;
    while acked.len() < 6000 {
        let base = batch * BATCH as u64;
        let ids: Vec<String> = (0..BATCH as u64).map(|k| format!("lease/{}", base + k)).collect();
        let docs: Vec<String> =
            ids.iter().enumerate().map(|(k, id)| doc(id, "lease", 1, base + k as u64)).collect();
        let body = format!(r#"{{"documents":[{}]}}"#, docs.join(","));
        match try_call(port, "POST", "/_bulk", Some(&body)) {
            Some(r) if r.code == 200 => acked.extend(ids),
            // Обратное давление законно: подождём и повторим ту же пачку.
            Some(r) if r.code == 429 || r.code == 503 => {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            Some(r) => panic!("приём отказал: {} {}", r.code, r.body),
            // Соединение оборвано — узел исчез. Это и есть точка отказа.
            None => break,
        }
        batch += 1;
    }

    let status = node.child.wait().expect("узел не дождался конца");
    assert!(
        !status.success(),
        "точка отказа не сработала: узел дожил до конца со статусом {status}"
    );
    assert!(!acked.is_empty(), "ни одна пачка не была подтверждена до падения — проверять нечего");

    node.revive();

    // Подтверждённое обязано быть на месте — и ровно в одном экземпляре.
    let hits = exactly_one_live_version(port, 10_000, None);
    let live: std::collections::HashSet<&str> =
        hits.iter().filter_map(|h| h["id"].as_str()).collect();
    for id in acked.iter() {
        assert!(live.contains(id.as_str()), "{id} подтверждён, но после подъёма не найден");
    }
    // Сверх подтверждённого допустима **ровно одна** пачка — та, что была в
    // полёте, когда узел исчез. Её записи уже пережили `fsync`, но ответа
    // клиент не получил, и долговечный контракт разрешает обе судьбы: либо
    // применить целиком, либо не применять вовсе. Требовать здесь точного
    // равенства значило бы проверять не контракт, а везение планировщика —
    // ровно на этом первая версия проверки и упала, когда последняя пачка
    // успела зафиксироваться на полмиллисекунды раньше падения.
    //
    // Чего не разрешает контракт и что проверяется строго: половина пачки,
    // потеря подтверждённого и вторая живая версия. Первые две ловятся здесь,
    // третья — в `exactly_one_live_version`.
    assert!(
        hits.len() >= acked.len(),
        "после подъёма живых {} против подтверждённых {} — подтверждённое потеряно",
        hits.len(),
        acked.len()
    );
    let extra = hits.len() - acked.len();
    assert!(
        extra.is_multiple_of(BATCH) && extra <= BATCH,
        "сверх подтверждённого {extra} документов при пачке в {BATCH}: \
         это либо половина пачки, либо больше одной — контракт не разрешает ни того, ни другого"
    );
    healthy(&node);
}

/// Падение после гашения, но до продвижения применённого номера не воскрешает
/// подтверждённо существовавший документ.
///
/// Это точное окно порядка в `Writer::delete`: WAL уже пережил `fsync`, бит
/// живости изменён только в отображении, а `Indexer::seq` ещё не сдвинут. Если
/// переставить номер перед гашением, соседний flush вправе обрезать WAL по
/// операции, которой нет на диске, и после рестарта документ оживёт.
#[cfg(feature = "failpoints")]
#[test]
fn a_kill_after_delete_apply_before_seq_does_not_resurrect_the_document() {
    let mut node = start_with_fail("гашение-до-номера", "delete.after_apply_before_seq=kill");
    let id = "lease/delete-window";
    let body = format!(r#"{{"documents":[{}]}}"#, doc(id, "delete_window", 1, 1));
    let added = call(node.port, "POST", "/_bulk", Some(&body));
    assert_eq!(added.code, 200, "документ не принят: {}", added.body);
    let flushed = call(node.port, "POST", "/_flush", Some("{}"));
    assert!(
        matches!(flushed.code, 200 | 202),
        "исходный документ не сброшен: {} {}",
        flushed.code,
        flushed.body
    );

    let before = call(node.port, "GET", "/_doc/lease%2Fdelete-window", None);
    assert_eq!(before.code, 200, "до гашения документа уже нет: {}", before.body);
    let deletion =
        try_call(node.port, "POST", "/_delete", Some(r#"{"ids":["lease/delete-window"]}"#));
    assert!(deletion.is_none(), "точка отказа не оборвала ответ");
    let status = node.child.wait().expect("узел не дождался конца");
    assert!(!status.success(), "точка отказа не сняла узел: {status}");

    node.revive();
    let after = call(node.port, "GET", "/_doc/lease%2Fdelete-window", None);
    assert_eq!(
        after.code, 404,
        "гашение из durable WAL не переигралось, документ воскрес: {}",
        after.body
    );
    let search = call(
        node.port,
        "POST",
        "/",
        Some(r#"{"k":10,"words":[{"field":"kind","terms":["delete_window"]}],"ids":true}"#),
    );
    assert_eq!(search.code, 200, "поиск после подъёма отказал: {}", search.body);
    assert!(
        search.json()["hits"].as_array().is_some_and(Vec::is_empty),
        "погашенный документ нашёлся поиском: {}",
        search.body
    );
    healthy(&node);
}

/// Upsert и delete одной WAL-мутации сходятся в одно состояние при падении
/// после любой половины применения.
#[cfg(feature = "failpoints")]
#[test]
fn a_crash_inside_mixed_mutation_replays_both_halves() {
    for point in ["mutation.after_replace_before_delete", "mutation.after_delete_before_ack"] {
        let mut node = start_with_fail("мутация-из-двух-половин", &format!("{point}=kill"));
        let old = format!(r#"{{"documents":[{}]}}"#, doc("lease/derived-old", "old", 1, 1));
        let added = call(node.port, "POST", "/_bulk", Some(&old));
        assert_eq!(added.code, 200, "{point}: исходный документ не принят: {}", added.body);
        let flushed = call(node.port, "POST", "/_flush", Some("{}"));
        assert!(matches!(flushed.code, 200 | 202), "{point}: flush: {}", flushed.body);

        let body = format!(
            r#"{{"documents":[{}],"deletes":["lease/derived-old"]}}"#,
            doc("lease/derived-new", "new", 2, 2)
        );
        let changed = try_call(node.port, "POST", "/_mutate", Some(&body));
        assert!(changed.is_none(), "{point}: точка отказа не оборвала ответ");
        let status = node.child.wait().expect("узел не дождался конца");
        assert!(!status.success(), "{point}: точка отказа не сняла узел: {status}");

        node.revive();
        let new = call(node.port, "GET", "/_doc/lease%2Fderived-new", None);
        assert_eq!(new.code, 200, "{point}: upsert-половина потерялась: {}", new.body);
        assert_eq!(
            new.json()["fields"]["revision"],
            2,
            "{point}: восстановилась не та версия: {}",
            new.body
        );
        let old = call(node.port, "GET", "/_doc/lease%2Fderived-old", None);
        assert_eq!(old.code, 404, "{point}: delete-половина потерялась: {}", old.body);
        healthy(&node);
    }
}

/// Три окна до HTTP ACK: после fsync WAL, после применения add и после
/// применения put. Во всех трёх клиент не знает исход, но повторный подъём
/// обязан дать целое состояние без потери и без старой версии.
#[cfg(feature = "failpoints")]
#[test]
fn durable_operations_survive_every_pre_ack_failpoint() {
    for (point, replacing) in [
        ("wal.after_fsync_before_ack", false),
        ("add.after_apply_before_ack", false),
        ("put.after_apply_before_ack", true),
    ] {
        // Native bulk is now an upsert, so the initial replacement seed also
        // reaches put's checkpoint. Arm the following operation for that case.
        let armed =
            if replacing { format!("{point}=kill,after=1") } else { format!("{point}=kill") };
        let mut node = start_with_fail("до-ack", &armed);
        let id = format!("lease/pre-ack-{}", point.replace('.', "-"));
        if replacing {
            let initial = format!(r#"{{"documents":[{}]}}"#, doc(&id, "before", 1, 1));
            let added = call(node.port, "POST", "/_bulk", Some(&initial));
            assert_eq!(added.code, 200, "{point}: исходная версия не принята: {}", added.body);
            let flushed = call(node.port, "POST", "/_flush", Some("{}"));
            assert!(matches!(flushed.code, 200 | 202), "{point}: flush: {}", flushed.body);
        }
        let revision = if replacing { 2 } else { 1 };
        let automatic_id = point == "add.after_apply_before_ack";
        // The append path is exercised by actual public ES auto-ID writes;
        // explicit-ID bulk correctly follows the replacement path now.
        let body = if automatic_id {
            let source = serde_json::json!({"kind":&id,"revision":revision,
                "state":"ok","body":"preack","weight":revision});
            format!("{{\"index\":{{}}}}\n{source}\n")
        } else {
            format!(r#"{{"documents":[{}]}}"#, doc(&id, "after", revision, revision))
        };
        let endpoint = if replacing { "/_put" } else { "/_bulk" };
        let reply = try_call(node.port, "POST", endpoint, Some(&body));
        assert!(reply.is_none(), "{point}: точка отказа не оборвала ответ");
        let status = node.child.wait().expect("узел не дождался конца");
        assert!(!status.success(), "{point}: точка отказа не сняла узел: {status}");

        node.revive();
        let recovered_id = if automatic_id {
            // The connection died before returning the generated ID. Recover
            // it by a unique indexed marker, then verify the authoritative GET.
            let query = serde_json::json!({"ids":true,"must":[{"field":"kind","terms":[&id]}]});
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let _ = call(node.port, "POST", "/_flush", Some("{}"));
                let found = call(node.port, "POST", "/", Some(&query.to_string()));
                let hits = found.json()["hits"].as_array().cloned().unwrap_or_default();
                if found.code == 200 && hits.len() == 1 {
                    break hits[0]["_id"].as_str().expect("recovered generated ID").to_string();
                }
                assert!(
                    Instant::now() < deadline,
                    "{point}: generated-ID document missing: {}",
                    found.body
                );
                std::thread::sleep(Duration::from_millis(25));
            }
        } else {
            id.clone()
        };
        let path = format!("/_doc/{}", recovered_id.replace('/', "%2F"));
        let got = call(node.port, "GET", &path, None);
        assert_eq!(got.code, 200, "{point}: durable операция потеряна: {}", got.body);
        assert_eq!(
            got.json()["fields"]["revision"],
            revision,
            "{point}: поднялась не та версия: {}",
            got.body
        );
        let flush = call(node.port, "POST", "/_flush", Some("{}"));
        assert!(matches!(flush.code, 200 | 202), "{point}: recovery flush: {}", flush.body);
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let health = call(node.port, "GET", "/_health", None);
            let value = health.json();
            if !value["flush_in_progress"].as_bool().unwrap_or(true)
                && value["buffered"].as_u64() == Some(0)
                && value["wal"]["checkpoint"].as_u64().unwrap_or(0) > 0
            {
                break;
            }
            assert!(Instant::now() < deadline, "{point}: recovery не осел: {}", health.body);
            std::thread::sleep(Duration::from_millis(25));
        }
        healthy(&node);
    }
}

/// Обрыв под нагрузкой: подтверждённое обязано пережить его целиком.
#[test]
fn an_abrupt_restart_under_load_loses_nothing_acknowledged() {
    let mut node = start("обрыв");
    let port = node.port;
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(Tally::default());
    let owned: Arc<Mutex<Owned>> = Arc::new(Mutex::new(HashMap::new()));
    let mut handles = Vec::new();

    for worker in 0..3u64 {
        let (stop, tally, owned) = (Arc::clone(&stop), Arc::clone(&tally), Arc::clone(&owned));
        handles.push(std::thread::spawn(move || {
            let ids: Vec<String> = (0..6).map(|n| format!("lease/{worker}/{n}")).collect();
            let mut at: HashMap<String, u64> = HashMap::new();
            while !stop.load(Ordering::Relaxed) {
                for id in ids.iter() {
                    let was = at.get(id).copied();
                    let next = was.unwrap_or(0) + 1;
                    let condition = match was {
                        Some(v) => format!(
                            r#","conditions":[{{"id":"{id}","field":"revision","equals":{v}}}]"#
                        ),
                        None => String::new(),
                    };
                    let body =
                        format!(r#"{{"documents":[{}]{condition}}}"#, doc(id, "lease", next, next));
                    let r = tally.note("put", try_call(port, "POST", "/_put", Some(&body)));
                    match r {
                        Some(r) if r.code == 200 => {
                            at.insert(id.clone(), next);
                        }
                        // Узел поднялся заново и не знает про нашу последнюю
                        // попытку — условие не совпало. Спрашиваем у него, что
                        // он считает текущим, и продолжаем от этого.
                        Some(r) if r.code == 409 => {
                            let path = format!("/_doc/{}", id.replace('/', "%2F"));
                            if let Some(d) = try_call(port, "GET", &path, None) {
                                if let Some(v) = d.json()["fields"]["revision"].as_u64() {
                                    at.insert(id.clone(), v);
                                }
                            }
                        }
                        _ => std::thread::sleep(Duration::from_millis(20)),
                    }
                }
            }
            owned.lock().expect("итог замен").extend(at);
        }));
    }

    // Сторож: раз увиденный документ больше не имеет права исчезать. Ловится
    // не итог, а **миг** пропажи — вместе с состоянием узла в этот миг.
    let lost: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let (stop, lost) = (Arc::clone(&stop), Arc::clone(&lost));
        let root = node.root.clone();
        handles.push(std::thread::spawn(move || {
            let all: Vec<String> = (0..3)
                .flat_map(|w| (0..6).map(move |n| format!("lease/{w}/{n}")))
                .collect();
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            while !stop.load(Ordering::Relaxed) {
                let quoted: Vec<String> = all.iter().map(|id| format!("\"{id}\"")).collect();
                let body = format!(r#"{{"ids":[{}]}}"#, quoted.join(","));
                let Some(r) = try_call(port, "POST", "/_mget", Some(&body)) else { continue };
                if r.code != 200 {
                    continue;
                }
                for d in r.json()["docs"].as_array().cloned().unwrap_or_default() {
                    let id = d["id"].as_str().unwrap_or_default().to_string();
                    if d["found"] == true {
                        seen.insert(id);
                    } else if seen.contains(&id) {
                        let mut lost = lost.lock().expect("список пропаж");
                        if lost.iter().all(|l: &String| !l.starts_with(&id)) {
                            let health = try_call(port, "GET", "/_health", None)
                                .map(|h| h.body)
                                .unwrap_or_default();
                            let inspect = try_call(port, "GET", "/_inspect", None)
                                .map(|h| h.body)
                                .unwrap_or_default();
                            let metrics = try_call(port, "GET", "/_metrics", None)
                                .map(|m| {
                                    m.body
                                        .lines()
                                        .filter(|l| {
                                            l.starts_with("svindb_segments")
                                                || l.starts_with("svindb_flushes")
                                                || l.starts_with("svindb_live_docs")
                                                || l.starts_with("svindb_buffered_docs")
                                        })
                                        .collect::<Vec<_>>()
                                        .join(" | ")
                                })
                                .unwrap_or_default();
                            let manifest = std::fs::read_to_string(format!("{root}/index/segments.tsv"))
                                .unwrap_or_else(|e| format!("<{e}>"));
                            let dirs = std::fs::read_dir(format!("{root}/index"))
                                .map(|d| {
                                    let mut v: Vec<String> = d
                                        .filter_map(|e| e.ok())
                                        .map(|e| e.file_name().to_string_lossy().into_owned())
                                        .collect();
                                    v.sort();
                                    v
                                })
                                .unwrap_or_default();
                            lost.push(format!(
                                "{id} пропал\n  здоровье: {health}\n  опись: {inspect}\n  список: {:?}\n  каталог: {dirs:?}\n  метрики: {metrics}",
                                manifest.lines().collect::<Vec<_>>()
                            ));
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(30));
            }
        }));
    }

    std::thread::sleep(Duration::from_secs(6));
    node.restart();
    // После обрыва «виденное» законно исчезает на миг подъёма: узел ещё
    // переигрывает журнал. Сторож это переживает — он спрашивает заново.
    std::thread::sleep(Duration::from_millis(500));
    lost.lock().expect("список пропаж").clear();
    std::thread::sleep(Duration::from_secs(6));

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("поток нагрузки упал");
    }

    let lost = lost.lock().expect("список пропаж");
    if !lost.is_empty() {
        let log = std::fs::read_to_string(format!("{}/index.log", node.root)).unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(30).collect();
        panic!(
            "документы исчезали под нагрузкой:\n{}\n--- вывод узла (хвост) ---\n{}",
            lost.join("\n"),
            tail.into_iter().rev().collect::<Vec<_>>().join("\n")
        );
    }
    drop(lost);

    // Всё подтверждённое обязано найтись — и точкой, и поиском.
    let done = owned.lock().unwrap();
    assert!(done.len() >= 18, "потоки успели меньше, чем должны были: {}", done.len());
    for (id, want) in done.iter() {
        let path = format!("/_doc/{}", id.replace('/', "%2F"));
        let d = call(port, "GET", &path, None);
        if d.code != 200 {
            let health = call(port, "GET", "/_health", None);
            let all = call(
                port,
                "POST",
                "/",
                Some(
                    r#"{"k":100,"ids":true,"fields":["revision"],
                         "must":[{"field":"kind","terms":["lease"]}]}"#,
                ),
            );
            panic!(
                "{id} потерян после обрыва (подтверждена версия {want}): {}\nздоровье: {}\nвыдача: {}",
                d.body, health.body, all.body
            );
        }
        let got = d.json()["fields"]["revision"].as_u64().unwrap_or(0);
        assert!(got >= *want, "{id}: подтверждена версия {want}, после обрыва осталась {got}");
    }
    healthy(&node);
}

/// У каждого идентификатора семьи ровно одна живая версия. Отдаёт выдачу.
///
/// Вынесено из [`verify`] отдельной проверкой, потому что нужно и там, где
/// `verify` неприменима. Кампания на десяти миллионах нашла состояние, в
/// котором подтверждённые версии читаются верно, а живых копий одного
/// документа — две, и поймал его именно **поиск**: точечное чтение обходит
/// сегменты от новых к старым и довольствуется первой живой копией, поэтому
/// второй не видит вовсе. Проверять единственность точкой — значит не
/// проверять её.
///
/// Сравнивается длина выдачи с длиной её же без повторов, а не размер множества
/// с ожидаемым числом: дубль обязан быть виден как **лишняя строка в ответе**,
/// а не раствориться в `dedup`.
fn exactly_one_live_version(port: u16, k: usize, owned: Option<&Owned>) -> Vec<serde_json::Value> {
    let body = format!(
        r#"{{"k":{k},"ids":true,"fields":["revision"],
             "must":[{{"field":"kind","terms":["lease"]}}]}}"#
    );
    let r = call(port, "POST", "/", Some(&body));
    assert_eq!(r.code, 200, "{}", r.body);
    let hits = r.json()["hits"].as_array().cloned().unwrap_or_default();
    let mut seen: Vec<String> =
        hits.iter().map(|h| h["id"].as_str().unwrap_or_default().to_string()).collect();
    seen.sort();
    let mut unique = seen.clone();
    unique.dedup();
    if seen.len() != unique.len() {
        let mut by_id: std::collections::HashMap<&str, Vec<String>> =
            std::collections::HashMap::new();
        for h in hits.iter() {
            by_id
                .entry(h["id"].as_str().unwrap_or_default())
                .or_default()
                .push(format!("doc={} rev={}", h["doc"], h["fields"]["revision"]));
        }
        let mut lines: Vec<String> = by_id
            .iter()
            .filter(|(_, v)| v.len() > 1)
            .map(|(id, v)| match owned {
                Some(o) => {
                    format!("{id}: подтверждено {:?}; в выдаче {}", o.get(*id), v.join(", "))
                }
                None => format!("{id}: в выдаче {}", v.join(", ")),
            })
            .take(20)
            .collect();
        lines.sort();
        let health = call(port, "GET", "/_health", None);
        panic!(
            "в выдаче есть повторяющийся id ({} строк против {} различных, повторов {}):\n{}\nздоровье: {}",
            seen.len(),
            unique.len(),
            seen.len() - unique.len(),
            lines.join("\n"),
            health.body
        );
    }
    hits
}

/// Сходимость: одна живая версия на документ и ровно подтверждённая.
fn verify(node: &Node, owned: &Owned, deleted: &[String], tally: &Tally, inserted: &AtomicU64) {
    let port = node.port;
    // Публикация в конце: дальше проверяется установившееся состояние, а не
    // гонка. Свежесть уже проверена приёмкой.
    for _ in 0..60 {
        let r = call(port, "POST", "/_flush", None);
        if r.code == 200 {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let bad = tally.refused.lock().expect("список отказов");
    assert!(bad.is_empty(), "неожиданные отказы под нагрузкой:\n{}", bad.join("\n"));
    drop(bad);
    assert_eq!(
        tally.dropped.load(Ordering::Relaxed),
        0,
        "соединения рвались без обрыва узла: {:?}",
        tally.drop_phases.lock().expect("разряды обрывов")
    );
    let ok = tally.ok.load(Ordering::Relaxed);
    assert!(ok > 2000, "нагрузка вышла игрушечной: успешных ответов {ok}");

    // Каждая замена сошлась к подтверждённой версии.
    assert!(!owned.is_empty(), "потоки замен ничего не подтвердили");
    for (id, want) in owned.iter() {
        let path = format!("/_doc/{}", id.replace('/', "%2F"));
        let d = call(port, "GET", &path, None);
        assert_eq!(d.code, 200, "{id} не читается: {}", d.body);
        assert_eq!(
            d.json()["fields"]["revision"].as_u64(),
            Some(*want),
            "{id}: подтверждена версия {want}, лежит другая: {}",
            d.body
        );
    }

    // У каждого идентификатора семьи ровно одна живая версия.
    let hits = exactly_one_live_version(port, 1000, Some(owned));
    let unique = hits.len();
    assert_eq!(unique, owned.len(), "поиск нашёл не всю семью замен");
    for h in hits.iter() {
        let id = h["id"].as_str().unwrap_or_default();
        assert_eq!(
            h["fields"]["revision"].as_u64(),
            owned.get(id).copied(),
            "{id}: поиск отдал не подтверждённую версию"
        );
    }

    // Удалённое не воскресло, а соседнее по номеру — на месте.
    for id in deleted.iter().take(50) {
        let path = format!("/_doc/{}", id.replace('/', "%2F"));
        let d = call(port, "GET", &path, None);
        assert_eq!(d.code, 404, "удалённый {id} нашёлся: {}", d.body);
    }
    assert!(inserted.load(Ordering::Relaxed) > 0, "приём пачками ничего не принял");

    healthy(node);
}

/// Узел здоров и ничего не проглотил.
fn healthy(node: &Node) {
    let h = call(node.port, "GET", "/_health", None);
    assert_eq!(h.code, 200, "{}", h.body);
    let v = h.json();
    assert_eq!(v["ok"], true, "узел объявил себя нездоровым: {}", h.body);
    assert_eq!(v["reload_failures"], 0, "состав переставал перечитываться: {}", h.body);
    assert_eq!(v["ingest_error"], "", "приём остановлен ошибкой: {}", h.body);
    // Отметка журнала обязана двигаться: стоящая на нуле означает, что ничего не
    // легло на диск и restart переиграл бы всё с начала времён.
    let checkpoint = v["wal"]["checkpoint"].as_u64().unwrap_or(0);
    assert!(checkpoint > 0, "отметка журнала не сдвинулась: {}", h.body);
    let applied = v["wal"]["applied"].as_u64().unwrap_or(0);
    let seq = v["wal"]["seq"].as_u64().unwrap_or(0);
    assert!(
        checkpoint <= applied && applied <= seq,
        "порядок WAL нарушен: checkpoint={checkpoint}, applied={applied}, seq={seq}: {}",
        h.body
    );

    // Проверка формата — тем же кодом, каким её делает оператор.
    let out = std::process::Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["check", &format!("{}/index", node.root), "--full"])
        .output()
        .expect("check не запустился");
    assert!(
        out.status.success(),
        "проверка формата не прошла:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Объём: миллион документов через HTTP при работающем обслуживании.
///
/// ## Что здесь проверяется и чего нет
///
/// Не производительность — числа зависят от машины и в утверждениях им не
/// место. Проверяется, что при переходе от тысяч документов к миллиону ничего
/// не меняется по существу: счёт сходится, формат остаётся исправным, память
/// узла не растёт вместе с индексом, а поиск и точечное чтение отвечают то же,
/// что и на маленьком наборе.
///
/// Миллион — не девять миллиардов, и выдавать одно за другое нельзя. Но именно
/// на этом объёме сегменты успевают несколько раз слиться и убраться, а буфер —
/// смениться сотни раз; величины, которые растут с числом документов, здесь уже
/// видны, а те, что растут с числом сегментов, — тем более.
#[test]
fn a_million_documents_keep_the_count_and_the_format() {
    let node = start("объём");
    let port = node.port;
    // Профиль объёма: крупный сегмент, редкая публикация, живой сегмент выключен
    // — на массовой загрузке он не нужен и только стоил бы пересборок.
    let r = call(
        port,
        "PUT",
        "/_settings",
        Some(
            r#"{"refresh_interval_ms":1000,"refresh_min_docs":50000,
                "refresh_max_interval_ms":30000,"live_max_docs":0,
                "flush_docs":200000,"target_docs":2000000,
                "flush_wait_ms":30000,"merge_policy":"tiered","merge_at_once":8}"#,
        ),
    );
    assert_eq!(r.code, 200, "настройки объёма: {}", r.body);

    const TOTAL: u64 = 1_000_000;
    const BATCH: u64 = 2_000;
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(Tally::default());

    // Читатели работают всё время загрузки: поиск и точка обязаны отвечать, пока
    // под ними сменяются и сливаются сегменты.
    let mut readers = Vec::new();
    for shape in 0..2u64 {
        let (stop, tally) = (Arc::clone(&stop), Arc::clone(&tally));
        readers.push(std::thread::spawn(move || {
            let mut seed = 0xf00d + shape;
            while !stop.load(Ordering::Relaxed) {
                let n = spread(&mut seed) % TOTAL;
                tally.note("doc", try_call(port, "GET", &format!("/_doc/vol%2F{n:07}"), None));
                let body = format!(
                    r#"{{"k":10,"ids":true,"must":[{{"field":"kind","terms":["vol"]}}],
                         "filter":[{{"field":"revision","from":{},"to":{}}}]}}"#,
                    n % 100,
                    n % 100 + 5
                );
                tally.note("search", try_call(port, "POST", "/", Some(&body)));
            }
        }));
    }

    let began = Instant::now();
    let mut sent = 0u64;
    while sent < TOTAL {
        let docs: Vec<String> = (0..BATCH)
            .map(|k| {
                let at = sent + k;
                doc(&format!("vol/{at:07}"), "vol", at % 100, at)
            })
            .collect();
        let body = format!(r#"{{"documents":[{}]}}"#, docs.join(","));
        let r = tally.note("bulk", try_call(port, "POST", "/_bulk", Some(&body)));
        match r {
            Some(r) if r.code == 200 => sent += BATCH,
            // Обратное давление — законный ответ: повторяем ту же пачку.
            Some(_) | None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let load = began.elapsed();
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().expect("читатель упал");
    }

    // Публикуем всё принятое.
    for _ in 0..300 {
        let r = call(port, "POST", "/_flush", None);
        if r.code == 200 {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let bad = tally.refused.lock().expect("список отказов");
    assert!(bad.is_empty(), "отказы под объёмом:\n{}", bad.join("\n"));
    drop(bad);

    // Счёт сходится: столько принято, столько и живёт.
    let health = call(port, "GET", "/_health", None).json();
    if health["docs"].as_u64() != Some(TOTAL) {
        let log = std::fs::read_to_string(format!("{}/index.log", node.root)).unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(25).collect();
        let listing = std::fs::read_to_string(format!("{}/index/segments.tsv", node.root))
            .unwrap_or_else(|e| format!("<{e}>"));
        let dirs = std::fs::read_dir(format!("{}/index", node.root))
            .map(|d| {
                let mut v: Vec<String> = d
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect();
                v.sort();
                v
            })
            .unwrap_or_default();
        let heads: Vec<String> = dirs
            .iter()
            .filter(|d| d.starts_with("seg") || d.starts_with("mrg") || d.starts_with("cln"))
            .map(|d| {
                let head = format!("{}/index/{d}/head.tsv", node.root);
                match std::fs::read_to_string(&head) {
                    Ok(t) => format!("{d}: {}", t.lines().take(2).collect::<Vec<_>>().join(" | ")),
                    Err(e) => format!("{d}: <заголовка нет: {e}>"),
                }
            })
            .collect();
        let metrics = call(port, "GET", "/_metrics", None)
            .body
            .lines()
            .filter(|l| l.starts_with("svindb_flushes") || l.starts_with("svindb_segments"))
            .collect::<Vec<_>>()
            .join(" | ");
        panic!(
            "живых документов не столько, сколько принято: {health}\n\
             заголовки: {heads:?}\nметрики: {metrics}\n\
             список: {:?}\nкаталог: {dirs:?}\n--- вывод узла ---\n{}",
            listing.lines().collect::<Vec<_>>(),
            tail.into_iter().rev().collect::<Vec<_>>().join("\n")
        );
    }
    assert_eq!(health["ok"], true, "узел нездоров после объёма: {health}");
    assert_eq!(health["ingest_error"], "", "{health}");
    let segments = health["segments"].as_u64().unwrap_or(0);
    assert!(segments >= 1, "сегментов не осталось: {health}");

    // Память узла не пропорциональна индексу: отображения не в счёт, а вот куча
    // обязана остаться величиной с буфер и рабочие структуры.
    let rss = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &node.child.id().to_string()])
        .output()
        .expect("ps");
    let rss_mb: u64 =
        String::from_utf8_lossy(&rss.stdout).trim().parse::<u64>().unwrap_or(0) / 1024;
    assert!(
        rss_mb < 2048,
        "узел занял {rss_mb} МБ на миллионе документов — это уже не отображения"
    );

    // Выдача та же, что и на малом наборе: точка и поиск сходятся.
    for at in [0u64, 1, TOTAL / 2, TOTAL - 1] {
        let d = call(port, "GET", &format!("/_doc/vol%2F{at:07}"), None);
        assert_eq!(d.code, 200, "vol/{at:07} потерян: {}", d.body);
        assert_eq!(d.json()["fields"]["revision"], at % 100, "vol/{at:07}: {}", d.body);
    }
    let found = call(
        port,
        "POST",
        "/",
        Some(
            r#"{"k":5,"ids":true,"must":[{"field":"kind","terms":["vol"]}],
                 "filter":[{"field":"revision","from":7,"to":7}]}"#,
        ),
    );
    assert_eq!(found.code, 200, "{}", found.body);
    assert_eq!(
        found.json()["hits"].as_array().map(|h| h.len()),
        Some(5),
        "узкий отбор на миллионе не нашёл верхушку: {}",
        found.body
    );

    println!(
        "объём: {TOTAL} документов за {:.1} с ({:.0} док/с), сегментов {segments}, узел {rss_mb} МБ",
        load.as_secs_f64(),
        TOTAL as f64 / load.as_secs_f64()
    );

    healthy(&node);
}
