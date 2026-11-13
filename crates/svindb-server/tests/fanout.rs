//! Координатор шардов: выдача, отказы шардов и чтение под нагрузкой.
//!
//! ## Почему рядом с шардами стоит эталон
//!
//! Веер легко проверить на «что-то нашлось» — и ничего этим не доказать. Всё
//! ценное здесь в сравнении: тот же корпус, залитый в один узел, обязан дать ту
//! же верхушку в том же порядке и с теми же весами. Разойдись они — виноват
//! веер, и видно это будет сразу, а не через недели по жалобам на выдачу.
//!
//! ## Что проверяется про отказы
//!
//! Не «узел не упал», а **назван ли** отказ. Неполная выдача, о неполноте
//! которой никто не знает, хуже отказа: по ней принимают решения.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

/// Сколько шардов у стенда. Три, а не два: при двух «сложилось поровну» и
/// «сложилось не тем» неразличимы.
const SHARDS: usize = 3;

struct Node {
    child: Child,
    port: u16,
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

struct Stand {
    root: String,
    shards: Vec<Node>,
    coordinator: Node,
    single: Node,
}

impl Drop for Stand {
    fn drop(&mut self) {
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

fn spawn(dir: &str, port: u16, shards: Option<&str>) -> Node {
    std::fs::create_dir_all(dir).unwrap();
    let mut args = vec![
        "serve".to_string(),
        dir.to_string(),
        port.to_string(),
        "--single-index".to_string(),
        "--fields=kind:keyword,body:1".to_string(),
        "--flush=100000".to_string(),
    ];
    if let Some(list) = shards {
        args.push(format!("--shards={list}"));
    }
    let log = std::fs::File::create(format!("{dir}.log")).expect("файл вывода");
    let child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(&args)
        .stdout(Stdio::from(log.try_clone().expect("вывод")))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let mut node = Node { child, port };
    for _ in 0..400 {
        if let Ok(Some(status)) = node.child.try_wait() {
            panic!("узел {dir} завершился до открытия порта: {status}");
        }
        if TcpStream::connect(("127.0.0.1", node.port)).is_ok() {
            return node;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("узел {dir} не открыл порт {port}");
}

fn stand(tag: &str) -> Stand {
    let root = std::env::temp_dir()
        .join(format!("svindb-веер-{tag}-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();

    let shards: Vec<Node> =
        (0..SHARDS).map(|i| spawn(&format!("{root}/шард{i}"), free_port(), None)).collect();
    let list = shards.iter().map(|s| format!("127.0.0.1:{}", s.port)).collect::<Vec<_>>().join(",");
    let coordinator = spawn(&format!("{root}/веер"), free_port(), Some(&list));
    let single = spawn(&format!("{root}/эталон"), free_port(), None);
    Stand { root, shards, coordinator, single }
}

/// Документ, у которого вес **однозначен**, а не равен соседскому.
///
/// Первым сорока слово `общее` повторяется убывающее число раз, остальным —
/// однажды. Иначе верхушка вырождается: у сотен документов вес совпадает до
/// последнего разряда, порядок между ними законно любой, и сравнивать веер с
/// эталоном становится нечем. Первая же попытка на равных весах это и показала —
/// эталон отдал одну два-дцатку, веер другую, и оба были правы.
///
/// Сорок при верхушке в двадцать: тем, кто в неё попадает, соседей по весу нет,
/// а шардов они задевают все три — номера идут подряд, раскладка по остатку.
fn doc(i: usize) -> String {
    const LEN: usize = 40;
    let weight = if i < LEN { LEN - i } else { 1 };
    // Длина тела **одинакова** у всех, меняется только доля искомого слова.
    //
    // Это не украшение корпуса, а условие сравнимости. BM25 нормирует вес
    // длиной документа относительно средней по полю, а средняя у каждого шарда
    // своя: общая статистика переносит число документов и частоты слов, но не
    // её. Дай документам разную длину — и веер честно посчитает иначе, чем
    // одиночный индекс, хотя оба правы. Первая попытка на убывающих длинах это
    // и показала: два разных, одинаково законных порядка.
    let mut body: Vec<&str> = std::iter::repeat_n("общее", weight).collect();
    body.extend(std::iter::repeat_n("фон", LEN - weight));
    format!(r#"{{"id":"д{i:06}","kind":"груз","body":"{} слово{i}"}}"#, body.join(" "))
}

/// Заливает корпус: в шарды по остатку от деления, в эталон целиком.
fn fill(s: &Stand, n: usize) {
    let mut by_shard: Vec<Vec<String>> = vec![Vec::new(); SHARDS];
    let mut all: Vec<String> = Vec::new();
    for i in 0..n {
        by_shard[i % SHARDS].push(doc(i));
        all.push(doc(i));
    }
    for (i, docs) in by_shard.iter().enumerate() {
        let body = format!(r#"{{"documents":[{}]}}"#, docs.join(","));
        let r = call(s.shards[i].port, "POST", "/_bulk", Some(&body));
        assert_eq!(r.code, 200, "шард {i} не принял: {}", r.body);
        assert_eq!(call(s.shards[i].port, "POST", "/_flush", None).code, 200);
    }
    let body = format!(r#"{{"documents":[{}]}}"#, all.join(","));
    let r = call(s.single.port, "POST", "/_bulk", Some(&body));
    assert_eq!(r.code, 200, "эталон не принял: {}", r.body);
    assert_eq!(call(s.single.port, "POST", "/_flush", None).code, 200);
}

fn search(port: u16, k: usize, global: bool) -> Reply {
    let scoring = if global { r#","scoring":"global""# } else { "" };
    let body = format!(
        r#"{{"k":{k},"ids":true,"words":[{{"field":"body","terms":["общее"]}}]{scoring}}}"#
    );
    call(port, "POST", "/", Some(&body))
}

fn ids_of(r: &Reply) -> Vec<String> {
    r.json()["hits"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|h| h["id"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Верхушка веера совпадает с верхушкой одиночного индекса.
///
/// Именно с общей статистикой: без неё веса считаются по каждому шарду отдельно
/// и расходятся законно — это проверяется отдельно ниже.
#[test]
fn the_fanned_top_matches_a_single_index() {
    let s = stand("верхушка");
    fill(&s, 600);

    let want = search(s.single.port, 20, false);
    assert_eq!(want.code, 200, "{}", want.body);
    let got = search(s.coordinator.port, 20, true);
    assert_eq!(got.code, 200, "{}", got.body);

    let page = call(
        s.coordinator.port,
        "POST",
        "/",
        Some(
            r#"{"from":7,"size":5,"ids":true,"scoring":"global","words":[{"field":"body","terms":["общее"]}]}"#,
        ),
    );
    assert_eq!(page.code, 200, "{}", page.body);
    assert_eq!(ids_of(&page), ids_of(&want)[7..12]);
    let (want_ids, got_ids) = (ids_of(&want), ids_of(&got));
    assert_eq!(want_ids.len(), 20, "эталон отдал не двадцать: {}", want.body);
    assert_eq!(
        got_ids, want_ids,
        "веер отдал другую верхушку:\nэталон {want_ids:?}\nвеер   {got_ids:?}"
    );

    // Веса тоже: расхождение в них означало бы, что общая статистика не доехала.
    let wv = want.json()["hits"].as_array().cloned().unwrap_or_default();
    let gv = got.json()["hits"].as_array().cloned().unwrap_or_default();
    for (a, b) in wv.iter().zip(gv.iter()) {
        let (x, y) = (a["score"].as_f64().unwrap_or(0.0), b["score"].as_f64().unwrap_or(0.0));
        assert!((x - y).abs() < 1e-9, "вес {} разошёлся: эталон {x}, веер {y}", a["id"]);
    }

    // И каждый шард назвал себя: без этого номера документов разных шардов
    // неотличимы, а они у каждого свои.
    let shards: Vec<i64> = gv.iter().filter_map(|h| h["shard"].as_i64()).collect();
    assert_eq!(shards.len(), gv.len(), "не у каждого попадания назван шард: {}", got.body);
    let mut uniq = shards.clone();
    uniq.sort_unstable();
    uniq.dedup();
    assert!(uniq.len() > 1, "все попадания пришли из одного шарда: {shards:?}");
}

/// Без общей статистики веса расходятся — и это законно, а не поломка.
///
/// Проверка стоит рядом с предыдущей нарочно: она показывает, что совпадение
/// выше — заслуга `scoring=global`, а не случайность корпуса.
#[test]
fn without_global_scoring_the_weights_drift() {
    let s = stand("веса");
    fill(&s, 600);
    let want = search(s.single.port, 20, false);
    let plain = search(s.coordinator.port, 20, false);
    assert_eq!(plain.code, 200, "{}", plain.body);
    let wv = want.json()["hits"].as_array().cloned().unwrap_or_default();
    let pv = plain.json()["hits"].as_array().cloned().unwrap_or_default();
    let same = wv.iter().zip(pv.iter()).all(|(a, b)| {
        (a["score"].as_f64().unwrap_or(0.0) - b["score"].as_f64().unwrap_or(0.0)).abs() < 1e-9
    });
    assert!(
        !same,
        "веса совпали и без общей статистики — значит проверка выше ничего не доказывает"
    );
}

/// Упавший шард **назван**, а не спрятан за неполной выдачей.
#[test]
fn a_dead_shard_is_named_not_hidden() {
    let mut s = stand("отказ");
    fill(&s, 300);
    s.shards[1].kill();

    // Явный строгий режим — отказ. Молча отдать выдачу двух шардов из трёх
    // значило бы соврать: клиент принял бы её за полную.
    let strict_body =
        r#"{"k":10,"ids":true,"partial":false,"words":[{"field":"body","terms":["общее"]}]}"#;
    let strict = call(s.coordinator.port, "POST", "/", Some(strict_body));
    assert_eq!(strict.code, 503, "неполная выдача пришла как полная: {}", strict.body);

    // Incomplete results require an explicit opt-in. Omitting partial
    // has the same strict contract as partial:false.
    let default_body = r#"{"k":10,"ids":true,"words":[{"field":"body","terms":["общее"]}]}"#;
    let default = call(s.coordinator.port, "POST", "/", Some(default_body));
    assert_eq!(default.code, 503, "{}", default.body);
    let body = r#"{"k":10,"ids":true,"partial":true,"words":[{"field":"body","terms":["общее"]}]}"#;
    let lax = call(s.coordinator.port, "POST", "/", Some(body));
    assert_eq!(lax.code, 200, "{}", lax.body);
    let v = lax.json();
    assert_eq!(v["partial"].as_bool(), Some(true), "неполнота не названа: {}", lax.body);
    assert_eq!(v["complete"].as_bool(), Some(false), "неполнота названа полной: {}", lax.body);
    assert_eq!(
        v["terminated"].as_str(),
        Some("shard_failure"),
        "причина неполноты потеряна: {}",
        lax.body
    );
    let failed = v["failed"].as_array().cloned().unwrap_or_default();
    assert_eq!(failed.len(), 1, "не назван ровно один отказавший: {}", lax.body);
    assert!(
        failed[0].as_str().unwrap_or_default().contains(&s.shards[1].port.to_string()),
        "назван не тот шард: {}",
        lax.body
    );
    assert!(
        !v["hits"].as_array().map(|h| h.is_empty()).unwrap_or(true),
        "уцелевшие шарды ничего не отдали: {}",
        lax.body
    );
    // Здоровье координатора говорит о веере, а не о своём пустом каталоге.
    let health = call(s.coordinator.port, "GET", "/_health", None).json();
    assert_eq!(health["coordinator"]["shards"], SHARDS, "{health}");
}

/// Чтение через веер работает, пока шарды принимают.
#[test]
fn reads_keep_working_while_shards_ingest() {
    let s = stand("нагрузка");
    fill(&s, 300);

    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for _ in 0..3 {
        let (stop, bad, done) = (Arc::clone(&stop), Arc::clone(&bad), Arc::clone(&done));
        let port = s.coordinator.port;
        readers.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match try_call(
                    port,
                    "POST",
                    "/",
                    Some(r#"{"k":10,"ids":true,"partial":true,"words":[{"field":"body","terms":["общее"]}]}"#),
                ) {
                    Some(r) if r.code == 200 => {
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        bad.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // Пауза не для нежности к узлу, а ради того, что здесь
                // проверяется. Один запрос к вееру — это три исходящих
                // соединения; читатели без паузы съедают эфемерные порты и
                // потоки быстрее, чем узел успевает их вернуть, и стенд начинает
                // мерить себя. Двадцать миллисекунд оставляют сотни запросов за
                // прогон — этого хватает, чтобы застать смену сегментов.
                std::thread::sleep(Duration::from_millis(20));
            }
        }));
    }

    let began = Instant::now();
    let mut at = 300usize;
    while began.elapsed() < Duration::from_secs(8) {
        let docs: Vec<String> = (at..at + 300).map(doc).collect();
        let body = format!(r#"{{"documents":[{}]}}"#, docs.join(","));
        let shard = (at / 300) % SHARDS;
        let r = call(s.shards[shard].port, "POST", "/_bulk", Some(&body));
        assert_eq!(r.code, 200, "шард {shard} отказал под чтением: {}", r.body);
        at += 300;
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().expect("читатель упал");
    }

    assert_eq!(bad.load(Ordering::Relaxed), 0, "поиск через веер отказывал под приёмом");
    assert!(
        done.load(Ordering::Relaxed) > 100,
        "читатели успели всего {} запросов — проверять почти нечего",
        done.load(Ordering::Relaxed)
    );

    // Принятое видно через веер после публикации.
    for n in s.shards.iter() {
        assert!(matches!(call(n.port, "POST", "/_flush", None).code, 200 | 202));
    }
    let r = search(s.coordinator.port, 10, false);
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(ids_of(&r).len(), 10, "{}", r.body);
}

/// Сбрасывает через координатор **до видимости**, а не до кода ответа.
///
/// `202` значит «сброс уже идёт»: записанное ещё не видно, и продолжать проверку
/// по нему нельзя. Одиночный узел отвечает так же, и веер это повторяет — а тест
/// обязан дождаться, иначе он проверял бы гонку, а не маршрутизацию.
fn flush_through(port: u16) {
    for _ in 0..200 {
        let r = call(port, "POST", "/_flush", None);
        if r.code == 200 {
            return;
        }
        assert_eq!(r.code, 202, "сброс через координатор отказал: {}", r.body);
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("сброс через координатор не завершился");
}

/// Записанное **через координатор** через него же и читается.
///
/// ## Что здесь доказывается, а что нет
///
/// Не «запрос принят». Принять и потерять — то же самое с точки зрения кода
/// ответа, и прежний контракт как раз это и запрещал: координатор отвергал
/// запись, потому что не умел её разложить, и отказ был честнее тишины.
///
/// Теперь он умеет, и проверяется ровно то, чего не хватало: документ, ушедший
/// в шард по остатку, **находится тем же остатком** — иначе он лёг бы в один
/// шард, а искался в другом, и пропажа была бы молчаливой.
///
/// Отдельно проверяется, что документы **разъехались**: сложись они все в один
/// шард, круговой обход дал бы те же зелёные утверждения, ничего не доказав.
#[test]
fn what_the_coordinator_wrote_the_coordinator_reads() {
    let s = stand("маршрут-записи");
    let port = s.coordinator.port;
    const N: usize = 300;

    let docs: Vec<String> =
        (0..N).map(|i| format!(r#"{{"id":"к{i:04}","kind":"груз","body":"слово{i}"}}"#)).collect();
    let r = call(port, "POST", "/_bulk", Some(&format!(r#"{{"documents":[{}]}}"#, docs.join(","))));
    assert_eq!(r.code, 200, "координатор не принял пачку: {}", r.body);
    assert_eq!(r.json()["added"].as_i64(), Some(N as i64), "принято не всё: {}", r.body);
    flush_through(port);

    // Разъехались ли. Спрашивается у шардов напрямую — координатор тут не
    // свидетель, он и есть подсудимый.
    let mut per: Vec<i64> = Vec::new();
    for n in s.shards.iter() {
        let r = call(n.port, "GET", "/_stats", None);
        assert_eq!(r.code, 200, "{}", r.body);
        per.push(r.json()["documents"].as_i64().or(r.json()["docs"].as_i64()).unwrap_or(-1));
    }
    assert_eq!(per.iter().sum::<i64>(), N as i64, "сумма по шардам не сходится: {per:?}");
    assert!(per.iter().all(|&x| x > 0), "шард остался пустым — раскладки не было: {per:?}");
    assert!(per.iter().all(|&x| x < N as i64), "всё легло в один шард: {per:?}");

    // Набор запрашивается **в обратном порядке**: собери координатор ответы по
    // готовности шардов, а не по местам, и каждый документ оказался бы подписан
    // чужим идентификатором. При прямом порядке эта ошибка часто незаметна.
    let ids: Vec<String> = (0..N).rev().map(|i| format!(r#""к{i:04}""#)).collect();
    let r = call(port, "POST", "/_mget", Some(&format!(r#"{{"ids":[{}]}}"#, ids.join(","))));
    assert_eq!(r.code, 200, "{}", r.body);
    let got = r.json();
    let list = got["docs"].as_array().expect("docs").clone();
    assert_eq!(list.len(), N, "вернулось не столько, сколько спросили");
    for (at, want) in (0..N).rev().enumerate() {
        let one = &list[at];
        assert_eq!(one["found"].as_bool(), Some(true), "к{want:04} не найден: {one}");
        let id = one["id"].as_str().or_else(|| one["_id"].as_str()).unwrap_or("");
        assert_eq!(id, format!("к{want:04}"), "место {at} подписано чужим id: {one}");
    }

    // Чужого идентификатора нет — и это «нет», а не дыра в списке.
    let r = call(port, "POST", "/_mget", Some(r#"{"ids":["к0001","такого-нет","к0002"]}"#));
    assert_eq!(r.code, 200, "{}", r.body);
    let list = r.json()["docs"].as_array().expect("docs").clone();
    assert_eq!(list.len(), 3, "{}", r.body);
    assert_eq!(list[0]["found"].as_bool(), Some(true));
    assert_eq!(list[1]["found"].as_bool(), Some(false), "чужой id: {}", list[1]);
    assert_eq!(list[2]["found"].as_bool(), Some(true));

    // Точка на один документ: тот же остаток, что и у набора.
    let r = call(port, "GET", "/_doc/к0123", None);
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(r.json()["found"].as_bool(), Some(true), "{}", r.body);
    assert_eq!(call(port, "GET", "/_doc/такого-нет", None).code, 404);

    // Запись одиночного документа и его гашение — тоже через координатор.
    let r = call(port, "POST", "/_doc/один", Some(r#"{"kind":"груз","body":"одиночное слово"}"#));
    assert_eq!(r.code, 200, "{}", r.body);
    flush_through(port);
    assert_eq!(call(port, "GET", "/_doc/один", None).json()["found"].as_bool(), Some(true));
    let r = call(port, "DELETE", "/_doc/один", None);
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(r.json()["deleted"].as_i64(), Some(1), "{}", r.body);
    flush_through(port);
    assert_eq!(call(port, "GET", "/_doc/один", None).code, 404);

    // Список — тем же путём.
    let r = call(port, "POST", "/_delete", Some(r#"{"ids":["к0000","к0001","к0002"]}"#));
    assert_eq!(r.code, 200, "{}", r.body);
    assert_eq!(r.json()["deleted"].as_i64(), Some(3), "{}", r.body);
}

/// Молчащий шард виден в ответе, а не тонет в нём.
///
/// Пачка, принятая наполовину, хуже отвергнутой: повтор сдвоит принятое, а
/// разобрать, что дошло, клиенту нечем. Здесь проверяется, что координатор не
/// отвечает `200` за то, чего не сделал.
#[test]
fn a_shard_that_is_gone_is_named_in_the_write_reply() {
    let mut s = stand("отказ-шарда-запись");
    let port = s.coordinator.port;
    s.shards[1].kill();

    let docs: Vec<String> =
        (0..90).map(|i| format!(r#"{{"id":"п{i:04}","kind":"груз","body":"слово{i}"}}"#)).collect();
    let r = call(port, "POST", "/_bulk", Some(&format!(r#"{{"documents":[{}]}}"#, docs.join(","))));
    assert_eq!(r.code, 503, "молчание шарда выдано за успех: {}", r.body);
    assert_eq!(r.json()["partial"].as_bool(), Some(true), "{}", r.body);
    let failed = r.json()["failed"].as_array().cloned().unwrap_or_default();
    assert_eq!(failed.len(), 1, "назван не один шард: {}", r.body);
    assert_eq!(failed[0]["shard"].as_i64(), Some(1), "назван чужой шард: {}", r.body);
    assert!(
        failed[0]["docs"].as_i64().unwrap_or(0) > 0,
        "не сказано, скольких документов это касается: {}",
        r.body
    );
    // Дошедшее сосчитано, а не отброшено: клиент видит и то, и другое.
    assert!(r.json()["added"].as_i64().unwrap_or(0) > 0, "{}", r.body);

    // Чтение: не найдено — это не «нет такого». Разница названа.
    let ids: Vec<String> = (0..90).map(|i| format!(r#""п{i:04}""#)).collect();
    let r = call(port, "POST", "/_mget", Some(&format!(r#"{{"ids":[{}]}}"#, ids.join(","))));
    assert_eq!(r.code, 200, "{}", r.body);
    let list = r.json()["docs"].as_array().cloned().unwrap_or_default();
    assert_eq!(list.len(), 90);
    let mute = list.iter().filter(|d| d.get("failed").is_some()).count();
    assert!(mute > 0, "молчание шарда неотличимо от отсутствия документа: {}", r.body);
}

/// Координатор отвергает то, чего не умеет **честно**, — и называет причину.
#[test]
fn a_coordinator_refuses_what_it_cannot_do_atomically() {
    let s = stand("отказ-мутации");
    let port = s.coordinator.port;
    let r = call(
        port,
        "POST",
        "/_mutate",
        Some(r#"{"documents":[{"id":"x","kind":"груз","body":"тело"}],"if":[]}"#),
    );
    assert_eq!(r.code, 400, "условная замена принята координатором: {}", r.body);
    assert!(r.body.contains("координатор"), "отказ не объясняет причину: {}", r.body);
    // А поиск — обслуживает.
    assert_eq!(search(port, 5, false).code, 200);
}

#[test]
fn a_partial_successful_shard_requires_explicit_opt_in() {
    use std::io::{BufRead, BufReader};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let shard = listener.local_addr().unwrap();
    let fake = std::thread::spawn(move || {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse::<usize>().unwrap();
                    }
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let payload = r#"{"hits":[{"id":"x","doc":0,"score":1}],"visited":1,"partial":true,"terminated":"deadline","complete":false}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            )
            .unwrap();
        }
    });
    let root = std::env::temp_dir().join(format!(
        "svindb-partial-shard-{}-{}",
        std::process::id(),
        free_port()
    ));
    let node = spawn(root.to_str().unwrap(), free_port(), Some(&shard.to_string()));
    for body in [r#"{"match_all":true}"#, r#"{"match_all":true,"partial":false}"#] {
        let reply = call(node.port, "POST", "/", Some(body));
        assert_eq!(reply.code, 503, "{}", reply.body);
    }
    let reply = call(node.port, "POST", "/", Some(r#"{"match_all":true,"partial":true}"#));
    assert_eq!(reply.code, 200, "{}", reply.body);
    assert_eq!(reply.json()["partial"], true);
    fake.join().unwrap();
    drop(node);
    let _ = std::fs::remove_dir_all(root);
}
