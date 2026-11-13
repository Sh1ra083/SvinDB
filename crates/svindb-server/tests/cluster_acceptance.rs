//! Приёмка кластера: топологии и отказы, а не «поднялось и отвечает».
//!
//! ## Что здесь считается ячейкой матрицы
//!
//! Ячейка — это **топология или отказ**, про которую сказано, что от неё
//! ожидается. Не «прошло/не прошло» вообще, а три различимых исхода:
//!
//! * работает как одиночный узел — и это сверено с одиночным узлом;
//! * деградирует **названно** — отказ есть, и он говорит, чего именно нет;
//! * не покрыто — и это записано здесь же, а не подразумевается зелёным цветом.
//!
//! ## Чего здесь нет и почему
//!
//! Задержка, потеря пакетов, раздел сети и полуоткрытый TCP не воспроизводятся:
//! на macOS без прав это не сделать честно, а поддельная эмуляция проверяла бы
//! эмуляцию. Эти ячейки остаются **непокрытыми**, и так они и названы в отчёте —
//! молчаливой зелени вместо них не будет.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Node {
    child: Child,
    port: u16,
    dir: String,
    args: Vec<String>,
}

impl Node {
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Поднимает тот же узел заново: тот же каталог, тот же порт, те же доводы.
    fn revive(&mut self) {
        self.kill();
        self.child = spawn_child(&self.dir, self.port, &self.args);
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
    for _ in 0..80 {
        if let Some(r) = try_call(port, method, path, body) {
            return r;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{method} {path}: узел на {port} не ответил");
}

/// Как [`call`], но без повторов: нужен там, где проверяется **отказ**.
fn once(port: u16, method: &str, path: &str, body: Option<&str>) -> Option<Reply> {
    try_call(port, method, path, body)
}

fn spawn(dir: &str, port: u16, extra: &[String]) -> Node {
    let child = spawn_child(dir, port, extra);
    Node { child, port, dir: dir.to_string(), args: extra.to_vec() }
}

fn spawn_child(dir: &str, port: u16, extra: &[String]) -> Child {
    std::fs::create_dir_all(dir).unwrap();
    let mut args = vec![
        "serve".to_string(),
        dir.to_string(),
        port.to_string(),
        "--single-index".to_string(),
        // `routing` объявлено намеренно: имя бытовое, и напороться на него мог
        // бы любой индекс про логистику или маршруты. Раскладка не вправе
        // толковать его как свой ключ — это проверяет отдельная ячейка.
        "--fields=kind:keyword,routing:keyword,body:1".to_string(),
        "--flush=100000".to_string(),
    ];
    args.extend(extra.iter().cloned());
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{dir}.log"))
        .expect("файл вывода");
    let mut child = Command::new(BIN)
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(&args)
        .stdout(Stdio::from(log.try_clone().expect("вывод")))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    for _ in 0..400 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return child;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    // Порт не открылся — узел всё равно надо похоронить, иначе он переживёт
    // тест и займёт порт следующему.
    let _ = child.kill();
    let _ = child.wait();
    panic!("узел {dir} не открыл порт {port}");
}

fn root(tag: &str) -> String {
    let p = std::env::temp_dir()
        .join(format!("svindb-приёмка-{tag}-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&p).ok();
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Документ, у которого вес **однозначен**, а не равен соседскому.
///
/// ## Почему корпус устроен именно так
///
/// Первая попытка давала всем документам одно и то же тело: два слова, одно из
/// них искомое. Тогда у всех трёхсот вес совпадал до последнего разряда, порядок
/// между ними был законно любой, и сравнивать веер с эталоном становилось нечем.
/// Тест краснел, и оба ответа были правы — то есть проверял он не то, что
/// написано в его имени.
///
/// Здесь первым сорока слово `общее` повторяется убывающее число раз, остальным
/// — однажды. Сорок при верхушке в тридцать: тем, кто в неё попадает, соседей по
/// весу нет.
///
/// Длина тела **одинакова** у всех, меняется только доля искомого слова. Это не
/// украшение: BM25 нормирует вес длиной документа относительно средней по полю,
/// а средняя у каждого шарда своя. Дай документам разную длину — и веер честно
/// посчитает иначе, чем одиночный индекс, хотя оба будут правы.
fn doc(i: usize) -> String {
    const LEN: usize = 40;
    let weight = if i < LEN { LEN - i } else { 1 };
    let mut body: Vec<&str> = std::iter::repeat_n("общее", weight).collect();
    body.extend(std::iter::repeat_n("фон", LEN - weight));
    format!(r#"{{"id":"п{i:05}","kind":"груз","body":"{} слово{i}"}}"#, body.join(" "))
}

fn bulk(port: u16, from: usize, to: usize) -> Reply {
    let all: Vec<String> = (from..to).map(doc).collect();
    call(port, "POST", "/_bulk", Some(&format!(r#"{{"documents":[{}]}}"#, all.join(","))))
}

fn flush_until_visible(port: u16) {
    for _ in 0..200 {
        let r = call(port, "POST", "/_flush", None);
        if r.code == 200 {
            return;
        }
        assert_eq!(r.code, 202, "сброс отказал: {}", r.body);
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("сброс не завершился");
}

fn top(port: u16, k: usize) -> Vec<String> {
    let body = format!(
        r#"{{"k":{k},"ids":true,"scoring":"global","words":[{{"field":"body","terms":["общее"]}}]}}"#
    );
    let r = call(port, "POST", "/", Some(&body));
    assert_eq!(r.code, 200, "{}", r.body);
    r.json()["hits"]
        .as_array()
        .map(|a| a.iter().filter_map(|h| h["id"].as_str().map(String::from)).collect())
        .unwrap_or_default()
}

fn found(port: u16, id: &str) -> bool {
    let r = call(port, "POST", "/_mget", Some(&format!(r#"{{"ids":["{id}"]}}"#)));
    r.json()["docs"][0]["found"].as_bool() == Some(true)
}

/// Собирает координатор над `n` шардами и заливает через него `docs` документов.
struct Cluster {
    root: String,
    shards: Vec<Node>,
    coord: Node,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).ok();
    }
}

fn cluster(tag: &str, n: usize, docs: usize) -> Cluster {
    let root = root(tag);
    let shards: Vec<Node> =
        (0..n).map(|i| spawn(&format!("{root}/шард{i}"), free_port(), &[])).collect();
    let list = shards.iter().map(|s| format!("127.0.0.1:{}", s.port)).collect::<Vec<_>>().join(",");
    let coord = spawn(&format!("{root}/веер"), free_port(), &[format!("--shards={list}")]);
    let r = bulk(coord.port, 0, docs);
    assert_eq!(r.code, 200, "заливка через координатор: {}", r.body);
    flush_until_visible(coord.port);
    Cluster { root, shards, coord }
}

// ---------------------------------------------------------------------------
// Топологии
// ---------------------------------------------------------------------------

/// 1×1: один шард, одна копия. Кластер обязан быть неотличим от одного узла.
///
/// Ячейка выглядит вырожденной, и в этом её ценность: если веер над одним шардом
/// уже расходится с одиночным узлом, все прочие ячейки проверяют не топологию, а
/// эту разницу.
#[test]
fn one_by_one_is_indistinguishable_from_a_single_node() {
    let c = cluster("1x1", 1, 200);
    let alone = spawn(&format!("{}/один", c.root), free_port(), &[]);
    assert_eq!(bulk(alone.port, 0, 200).code, 200);
    flush_until_visible(alone.port);

    assert_eq!(top(c.coord.port, 20), top(alone.port, 20), "веер над одним шардом дал иную выдачу");
    for id in ["п00000", "п00099", "п00199"] {
        assert!(found(c.coord.port, id), "{id} не нашёлся через координатор");
    }
    assert!(!found(c.coord.port, "п99999"), "нашёлся документ, которого нет");
}

/// Три шарда: корпус разъехался, а выдача — та же, что у одного узла.
///
/// Сверка с эталоном, а не «что-то нашлось»: веер легко проверить на непустоту и
/// ничего этим не доказать. Порядок сверяется тоже — при `scoring=global` он
/// определён, и расхождение в нём значит разошедшийся вес.
#[test]
fn three_shards_answer_like_one_index() {
    let c = cluster("три", 3, 300);
    let alone = spawn(&format!("{}/один", c.root), free_port(), &[]);
    assert_eq!(bulk(alone.port, 0, 300).code, 200);
    flush_until_visible(alone.port);

    // Разъехались: у каждого шарда есть документы и ни у кого не все.
    let mut per = Vec::new();
    for s in c.shards.iter() {
        let r = call(s.port, "GET", "/_stats", None);
        per.push(r.json()["docs"].as_i64().unwrap_or(-1));
    }
    assert_eq!(per.iter().sum::<i64>(), 300, "сумма по шардам не сходится: {per:?}");
    assert!(per.iter().all(|&x| x > 0 && x < 300), "раскладки не было: {per:?}");

    assert_eq!(top(c.coord.port, 25), top(alone.port, 25), "веер и эталон разошлись в выдаче");
}

/// Десять шардов: форма держится, когда шардов больше, чем документов на шард.
#[test]
fn ten_shards_still_hold_the_shape() {
    let c = cluster("десять", 10, 400);
    let alone = spawn(&format!("{}/один", c.root), free_port(), &[]);
    assert_eq!(bulk(alone.port, 0, 400).code, 200);
    flush_until_visible(alone.port);
    assert_eq!(top(c.coord.port, 30), top(alone.port, 30));
    for id in ["п00001", "п00222", "п00399"] {
        assert!(found(c.coord.port, id), "{id} потерялся на десяти шардах");
    }
}

/// Два координатора над одними шардами отвечают одинаково.
///
/// Координатор состояния не держит, и это утверждение — про то, что не держит
/// **никакого**: заведись у него кеш раскладки или счётчик обхода реплик,
/// влияющий на ответ, два координатора разошлись бы.
#[test]
fn two_coordinators_agree() {
    let c = cluster("два-веера", 3, 250);
    let list =
        c.shards.iter().map(|s| format!("127.0.0.1:{}", s.port)).collect::<Vec<_>>().join(",");
    let second = spawn(&format!("{}/веер2", c.root), free_port(), &[format!("--shards={list}")]);
    assert_eq!(top(c.coord.port, 20), top(second.port, 20), "два координатора разошлись");
    assert_eq!(found(second.port, "п00100"), found(c.coord.port, "п00100"));
}

// ---------------------------------------------------------------------------
// Отказы
// ---------------------------------------------------------------------------

/// Потеря шарда деградирует **названно**, а не молча урезает выдачу.
///
/// Неполная выдача, о неполноте которой никто не знает, хуже отказа: по ней
/// принимают решения.
#[test]
fn a_lost_shard_is_named_not_hidden() {
    let mut c = cluster("потеря-шарда", 3, 300);
    c.shards[1].kill();

    let body = r#"{"k":10,"ids":true,"words":[{"field":"body","terms":["общее"]}]}"#;
    let r = call(c.coord.port, "POST", "/", Some(body));
    assert!(
        r.body.contains("partial") || r.body.contains("failed") || r.code >= 500,
        "неполная выдача выдана за полную: {} {}",
        r.code,
        r.body
    );

    // Чтение по идентификатору с погибшего шарда — тоже не «нет такого».
    let r = call(c.coord.port, "POST", "/_mget", Some(r#"{"ids":["п00000","п00001","п00002"]}"#));
    assert_eq!(r.code, 200, "{}", r.body);
    let docs = r.json()["docs"].as_array().cloned().unwrap_or_default();
    let mute = docs.iter().filter(|d| d.get("failed").is_some()).count();
    let alive = docs.iter().filter(|d| d["found"].as_bool() == Some(true)).count();
    assert!(mute + alive == 3, "часть ответов ни жива, ни названа: {}", r.body);

    // Запись на погибший шард — отказ с числом, а не тихий `200`.
    let r = bulk(c.coord.port, 900, 1000);
    assert_eq!(r.code, 503, "запись на мёртвый шард выдана за принятую: {}", r.body);
    assert_eq!(r.json()["partial"].as_bool(), Some(true), "{}", r.body);
}

/// Потеря координатора не трогает данные: шарды отвечают сами.
#[test]
fn losing_the_coordinator_does_not_lose_the_data() {
    let mut c = cluster("потеря-веера", 3, 200);
    let ports: Vec<u16> = c.shards.iter().map(|s| s.port).collect();
    c.coord.kill();
    assert!(once(c.coord.port, "GET", "/_health", None).is_none(), "мёртвый координатор ответил");

    let mut total = 0i64;
    for p in ports {
        let r = call(p, "GET", "/_stats", None);
        assert_eq!(r.code, 200, "шард не пережил смерть координатора: {}", r.body);
        total += r.json()["docs"].as_i64().unwrap_or(0);
    }
    assert_eq!(total, 200, "после смерти координатора корпус не сошёлся");
}

/// Rolling restart: шарды перезапускаются по одному, корпус не теряется.
///
/// Проверяется не «узел поднялся», а то, что после круга перезапусков выдача
/// **та же самая**. Потеря принятого при перезапуске — самый дорогой из тихих
/// отказов, и увидеть её можно только сравнением до и после.
#[test]
fn a_rolling_restart_keeps_the_corpus() {
    let mut c = cluster("перезапуск", 3, 300);
    let before = top(c.coord.port, 25);
    assert_eq!(before.len(), 25, "проверять нечего: верхушка пуста");

    for i in 0..c.shards.len() {
        c.shards[i].kill();
        c.shards[i].revive();
        // Узел поднялся — координатор снова видит все три.
        let r = call(c.shards[i].port, "GET", "/_health", None);
        assert_eq!(r.code, 200, "шард {i} не поднялся: {}", r.body);
    }

    let after = top(c.coord.port, 25);
    assert_eq!(before, after, "круг перезапусков изменил выдачу");
    for id in ["п00000", "п00150", "п00299"] {
        assert!(found(c.coord.port, id), "{id} потерялся при перезапуске");
    }
}

/// Повтор старой записи: `_put` идемпотентен, `_bulk` — нет, и это разные точки.
///
/// ## Что здесь на самом деле проверяется
///
/// Первая редакция этой ячейки повторяла `_bulk` и требовала, чтобы ничего не
/// изменилось. Она краснела — и была права краснеть: `_bulk` **дописывает**, и
/// повтор создаёт вторую живую версию того же идентификатора. Идемпотентна
/// другая точка — `_put`.
///
/// Разница не академическая. Координатор отвечает `503` на пачку, дошедшую не до
/// всех шардов, и клиент после такого ответа повторяет. Повтори он `_bulk`
/// целиком — сдвоил бы всё, что дошло. Поэтому отказ несёт `at`: номера мест,
/// которые не дошли, и повторить надо ровно их.
#[test]
fn replaying_a_write_is_idempotent_only_where_it_promises_to_be() {
    let c = cluster("повтор", 3, 200);

    // `_put` — заменяет. Три повтора оставляют одну живую версию.
    let all: Vec<String> = (0..60).map(doc).collect();
    let body = format!(r#"{{"documents":[{}]}}"#, all.join(","));
    for _ in 0..3 {
        let r = call(c.coord.port, "POST", "/_put", Some(&body));
        assert_eq!(r.code, 200, "{}", r.body);
    }
    flush_until_visible(c.coord.port);

    let ids = top(c.coord.port, 30);
    let mut seen = std::collections::BTreeSet::new();
    for id in ids.iter() {
        assert!(seen.insert(id.clone()), "{id} в выдаче дважды после повторов `_put`");
    }
    let r = call(c.coord.port, "POST", "/_mget", Some(r#"{"ids":["п00000"]}"#));
    let docs = r.json()["docs"].as_array().cloned().unwrap_or_default();
    assert_eq!(docs.len(), 1, "{}", r.body);
    assert_eq!(docs[0]["found"].as_bool(), Some(true));
}

/// Отказ пачки называет **места**, которые не дошли, а не только их число.
///
/// Без этого повтор после `503` невозможен точно: `_bulk` дописывает, и повтор
/// целой пачки сдвоил бы всё, что дошло. По номерам мест клиент шлёт заново
/// ровно то, чего нет.
#[test]
fn a_partial_batch_names_the_places_that_did_not_land() {
    let mut c = cluster("места", 3, 50);
    c.shards[2].kill();

    let r = bulk(c.coord.port, 500, 560);
    assert_eq!(r.code, 503, "{}", r.body);
    let failed = r.json()["failed"].as_array().cloned().unwrap_or_default();
    assert_eq!(failed.len(), 1, "назван не один шард: {}", r.body);
    let at = failed[0]["at"].as_array().cloned().unwrap_or_default();
    let n = failed[0]["docs"].as_i64().unwrap_or(-1);
    assert_eq!(at.len() as i64, n, "число не сошлось со списком мест: {}", r.body);
    assert!(!at.is_empty(), "список мест пуст, а шард назван недошедшим: {}", r.body);
    // Места — номера в **исходном** списке клиента, значит все меньше его длины.
    for one in at.iter() {
        let k = one.as_i64().unwrap_or(-1);
        assert!((0..60).contains(&k), "место {k} вне исходного списка из 60");
    }
    // И дошедшее сосчитано отдельно: клиент видит обе половины.
    assert_eq!(
        r.json()["added"].as_i64().unwrap_or(0) + n,
        60,
        "дошедшее и недошедшее не складываются в пачку: {}",
        r.body
    );
}

/// `global` совпадает с одним индексом, `local` продолжает отвечать.
///
/// Обе выдачи законны: при `local` каждый шард нормирует вес своей статистикой,
/// и порядок между шардами законно иной. Утверждается ровно то, что проверено:
/// `global` сходится с эталоном, `local` отдаёт полную страницу. Утверждать, что
/// они **обязаны** разойтись, нельзя — на этом корпусе они могут и совпасть.
#[test]
fn global_scoring_matches_one_index_and_local_still_answers() {
    let c = cluster("скоринг", 3, 300);
    let alone = spawn(&format!("{}/один", c.root), free_port(), &[]);
    assert_eq!(bulk(alone.port, 0, 300).code, 200);
    flush_until_visible(alone.port);

    let global = top(c.coord.port, 25);
    let body = r#"{"k":25,"ids":true,"words":[{"field":"body","terms":["общее"]}]}"#;
    let r = call(c.coord.port, "POST", "/", Some(body));
    let local: Vec<String> = r.json()["hits"]
        .as_array()
        .map(|a| a.iter().filter_map(|h| h["id"].as_str().map(String::from)).collect())
        .unwrap_or_default();

    assert_eq!(global, top(alone.port, 25), "global разошёлся с одним индексом");
    assert_eq!(local.len(), 25, "local не отдал верхушку: {}", r.body);
}

// ---------------------------------------------------------------------------
// Деление шарда (Ф7.5)
// ---------------------------------------------------------------------------

/// Шард делится надвое, и ни один документ не теряется и не удваивается.
///
/// ## Как это делается и почему именно так
///
/// Копирование — снимком: каталоги сегментов неизменяемы, и снять их дешевле,
/// чем перечислять корпус по сети. После восстановления у обоих узлов **полный**
/// корпус, и каждому надо оставить свою половину полосы. Список идентификаторов
/// для этого не годится — их миллионы и клиент их не знает, — поэтому узел режет
/// себя сам по принадлежности ячейки: `POST /_reshard {keep_lo, keep_hi}`.
///
/// ## Что здесь считается доказательством
///
/// Не «обе половины отвечают». Проверяется тройка, которую поодиночке легко
/// удовлетворить неверной реализацией:
///
/// * **ничего не потеряно** — каждый документ найден ровно у того, чья полоса;
/// * **ничего не удвоено** — ни один не найден у обоих;
/// * **новый координатор с неравными полосами адресует туда же**, куда легло, —
///   иначе деление верно, а искать нечем.
#[test]
fn a_shard_splits_in_half_without_losing_or_doubling_a_document() {
    let base = root("деление");
    let one = spawn(&format!("{base}/шард"), free_port(), &[]);
    const N: usize = 400;
    assert_eq!(bulk(one.port, 0, N).code, 200);
    flush_until_visible(one.port);

    // Снимок и восстановление второй половиной.
    let store = format!("{base}/снимки");
    std::fs::create_dir_all(&store).unwrap();
    let snap = Command::new(BIN)
        .args(["snapshot", &format!("{base}/шард"), &store, "деление"])
        .output()
        .expect("снимок");
    assert!(
        snap.status.success(),
        "снимок не снялся: {}{}",
        String::from_utf8_lossy(&snap.stdout),
        String::from_utf8_lossy(&snap.stderr)
    );
    let archive = std::fs::read_dir(&store)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_string_lossy().to_string())
        .find(|p| p.contains("деление"))
        .expect("архив снимка не найден");
    let half = format!("{base}/половина");
    let back =
        Command::new(BIN).args(["restore", &archive, &half]).output().expect("восстановление");
    assert!(
        back.status.success(),
        "снимок не восстановился: {}{}",
        String::from_utf8_lossy(&back.stdout),
        String::from_utf8_lossy(&back.stderr)
    );

    // Граница деления — середина всего пространства ячеек.
    const SLOTS: u64 = 4096;
    const MID: u64 = SLOTS / 2;
    let low = one;
    let high = spawn(&half, free_port(), &[]);

    let r =
        call(low.port, "POST", "/_reshard", Some(&format!(r#"{{"keep_lo":0,"keep_hi":{MID}}}"#)));
    assert_eq!(r.code, 200, "нижняя половина не сузилась: {}", r.body);
    let kept_low = r.json()["kept"].as_i64().unwrap_or(-1);
    let cut_low = r.json()["removed"].as_i64().unwrap_or(-1);

    let r = call(
        high.port,
        "POST",
        "/_reshard",
        Some(&format!(r#"{{"keep_lo":{MID},"keep_hi":{SLOTS}}}"#)),
    );
    assert_eq!(r.code, 200, "верхняя половина не сузилась: {}", r.body);
    let kept_high = r.json()["kept"].as_i64().unwrap_or(-1);

    assert_eq!(
        kept_low + kept_high,
        N as i64,
        "половины не складываются в корпус: {kept_low} + {kept_high}"
    );
    assert!(kept_low > 0 && kept_high > 0, "вся полоса ушла одному: {kept_low}/{kept_high}");
    assert_eq!(cut_low, kept_high, "выкошено не столько, сколько ушло соседу");

    flush_until_visible(low.port);
    flush_until_visible(high.port);

    // Повтор ничего не меняет: обход идёт по принадлежности, а не по списку.
    let again =
        call(low.port, "POST", "/_reshard", Some(&format!(r#"{{"keep_lo":0,"keep_hi":{MID}}}"#)));
    assert_eq!(
        again.json()["removed"].as_i64(),
        Some(0),
        "повтор снова что-то гасил: {}",
        again.body
    );

    // Координатор с **неравными** полосами адресует туда же, куда легло.
    let list = format!("127.0.0.1:{}@0-{MID},127.0.0.1:{}@{MID}-{SLOTS}", low.port, high.port);
    let coord = spawn(&format!("{base}/веер"), free_port(), &[format!("--shards={list}")]);

    let mut lost = Vec::new();
    let mut doubled = Vec::new();
    for i in 0..N {
        let id = format!("п{i:05}");
        let here = found(low.port, &id);
        let there = found(high.port, &id);
        match (here, there) {
            (false, false) => lost.push(id.clone()),
            (true, true) => doubled.push(id.clone()),
            _ => {}
        }
        assert!(found(coord.port, &id), "координатор не нашёл {id} после деления");
    }
    assert!(lost.is_empty(), "потеряно при делении: {lost:?}");
    assert!(doubled.is_empty(), "удвоено при делении: {doubled:?}");

    std::fs::remove_dir_all(&base).ok();
}

/// Координатор с дырой в полосах не поднимается.
///
/// Дыра — документы, которые некуда адресовать: запись уйдёт в один шард, чтение
/// пойдёт в другой, и оба ответят без тени сомнения. Заметит это не координатор,
/// а тот, кто через месяц не найдёт своего документа. Поэтому отказ запуска.
#[test]
fn a_coordinator_with_a_hole_in_its_stripes_refuses_to_start() {
    let base = root("дыра");
    let a = spawn(&format!("{base}/а"), free_port(), &[]);
    let b = spawn(&format!("{base}/б"), free_port(), &[]);
    let bad = format!("127.0.0.1:{}@0-2000,127.0.0.1:{}@2048-4096", a.port, b.port);
    let out = Command::new(BIN)
        .args([
            "serve",
            &format!("{base}/веер"),
            &free_port().to_string(),
            "--fields=kind:keyword,body:1",
            &format!("--shards={bad}"),
        ])
        .output()
        .expect("запуск");
    assert!(!out.status.success(), "координатор с дырой поднялся");
    let why = String::from_utf8_lossy(&out.stderr) + String::from_utf8_lossy(&out.stdout);
    assert!(why.contains("не покрыты"), "отказ не называет дыру: {why}");

    // А перекрытие — тоже отказ, и тоже названный.
    let over = format!("127.0.0.1:{}@0-2500,127.0.0.1:{}@2048-4096", a.port, b.port);
    let out = Command::new(BIN)
        .args([
            "serve",
            &format!("{base}/веер2"),
            &free_port().to_string(),
            "--fields=kind:keyword,body:1",
            &format!("--shards={over}"),
        ])
        .output()
        .expect("запуск");
    assert!(!out.status.success(), "координатор с перекрытием поднялся");
    let why = String::from_utf8_lossy(&out.stderr) + String::from_utf8_lossy(&out.stdout);
    assert!(why.contains("перекрывает"), "отказ не называет перекрытие: {why}");

    std::fs::remove_dir_all(&base).ok();
}

/// Координатор пишет в primary шарда, а не в очередную его копию.
///
/// ## Почему без этого репликация теряет смысл
///
/// Круговой обход копий придуман для чтения: они равноправны. Для записи — нет:
/// рассылает копиям только primary, а реплика, получив запись напрямую, применит
/// её у себя и не расскажет никому. Половина корпуса оказалась бы только на
/// репликах, половина — только на primary, и обе стороны об этом промолчали бы.
#[test]
fn the_coordinator_writes_to_the_primary_not_to_a_replica() {
    let base = root("запись-в-primary");
    let replica = spawn(&format!("{base}/копия"), free_port(), &[]);
    let primary = spawn(
        &format!("{base}/главный"),
        free_port(),
        &[format!("--replicas=127.0.0.1:{}", replica.port), "--term=1".into(), "--ack=all".into()],
    );
    // Шард из двух копий: первая — primary. Порядок значащий.
    let list = format!("127.0.0.1:{}+127.0.0.1:{}", primary.port, replica.port);
    let coord = spawn(&format!("{base}/веер"), free_port(), &[format!("--shards={list}")]);

    const N: usize = 40;
    for i in 0..N {
        let one = doc(i);
        let r = call(coord.port, "POST", "/_bulk", Some(&format!(r#"{{"documents":[{one}]}}"#)));
        assert_eq!(r.code, 200, "{}", r.body);
    }
    flush_until_visible(primary.port);
    flush_until_visible(replica.port);

    // Всё у обоих: primary применил и разослал, реплика получила.
    for i in 0..N {
        let id = format!("п{i:05}");
        assert!(found(primary.port, &id), "{id} миновал primary — запись ушла в копию напрямую");
        assert!(found(replica.port, &id), "{id} не доехал до копии");
    }
    std::fs::remove_dir_all(&base).ok();
}

/// Условие compare-and-swap доезжает до шарда, а не теряется по дороге.
///
/// Раскладка пересобирала тело шарда из одного массива документов, и всё
/// остальное — включая `conditions` — оставалось у координатора. Условная замена
/// молча становилась безусловной и отвечала `200`: клиент получал перезапись
/// чужой версии и узнавал об этом не отказом, а расхождением данных.
#[test]
fn a_compare_and_swap_condition_survives_the_routing() {
    let c = cluster("условие", 3, 50);
    let body = r#"{"documents":[{"id":"п00007","kind":"груз","body":"подменённое"}],
                   "conditions":[{"id":"п00007","field":"kind","equals":"чужое"}]}"#;
    let r = call(c.coord.port, "POST", "/_put", Some(body));
    assert_ne!(r.code, 200, "условие потерялось: замена прошла безусловно: {}", r.body);
    assert!(
        r.body.contains("conflict") || r.body.contains("failed"),
        "отказ не называет конфликт условия: {} {}",
        r.code,
        r.body
    );

    // А сходящееся условие — проходит.
    let body = r#"{"documents":[{"id":"п00007","kind":"груз","body":"новое тело"}],
                   "conditions":[{"id":"п00007","field":"kind","equals":"груз"}]}"#;
    let r = call(c.coord.port, "POST", "/_put", Some(body));
    assert_eq!(r.code, 200, "сходящееся условие отвергнуто: {}", r.body);
}

/// Поле документа с именем `routing` не уводит его в чужой шард.
///
/// Ключ раскладки брался из тела документа, а ни одна точка чтения по
/// идентификатору его не передаёт: документ ложился по одному адресу, а искался
/// по другому — то есть пропадал. Имя при этом бытовое, и напороться на него мог
/// любой индекс про логистику или маршруты.
#[test]
fn a_user_field_named_routing_does_not_hijack_the_address() {
    let c = cluster("поле-routing", 3, 20);
    let body = r#"{"documents":[
        {"id":"м1","kind":"груз","routing":"москва-север","body":"общее"},
        {"id":"м2","kind":"груз","routing":"москва-юг","body":"общее"}
    ]}"#;
    let r = call(c.coord.port, "POST", "/_bulk", Some(body));
    assert_eq!(r.code, 200, "{}", r.body);
    flush_until_visible(c.coord.port);
    for id in ["м1", "м2"] {
        assert!(found(c.coord.port, id), "{id} записан по одному адресу, а ищется по другому");
    }
}

/// Смерть primary — отказ записи, а не запись в реплику.
///
/// Обход копий верен для чтения и вреден для записи: соседка — реплика, она
/// применит запись у себя и не расскажет никому. Отказ клиент увидит,
/// расхождение — нет, поэтому у записи обхода нет вовсе.
#[test]
fn a_dead_primary_refuses_the_write_instead_of_writing_to_a_replica() {
    let base = root("смерть-primary");
    let replica = spawn(&format!("{base}/копия"), free_port(), &[]);
    let mut primary = spawn(
        &format!("{base}/главный"),
        free_port(),
        &[format!("--replicas=127.0.0.1:{}", replica.port), "--term=1".into(), "--ack=one".into()],
    );
    let list = format!("127.0.0.1:{}+127.0.0.1:{}", primary.port, replica.port);
    let coord = spawn(&format!("{base}/веер"), free_port(), &[format!("--shards={list}")]);

    assert_eq!(bulk(coord.port, 0, 10).code, 200);
    flush_until_visible(coord.port);
    primary.kill();

    let r = bulk(coord.port, 100, 110);
    assert_eq!(r.code, 503, "запись при мёртвом primary ушла в реплику: {}", r.body);
    for i in 100..110 {
        assert!(
            !found(replica.port, &format!("п{i:05}")),
            "реплика приняла запись напрямую — копии разойдутся молча"
        );
    }
    // А чтение при этом живо: у него обход копий законен.
    assert!(found(coord.port, "п00000"), "чтение перестало работать вместе с записью");
    std::fs::remove_dir_all(&base).ok();
}
