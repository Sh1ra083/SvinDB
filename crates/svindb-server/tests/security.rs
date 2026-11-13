//! Права доступа, CORS и снимки — по настоящему сокету, на поднятом узле.
//!
//! Проверять это внутренними вызовами нельзя: половина того, что здесь может
//! сломаться, живёт между разбором заголовка и маршрутизацией. Тест, зовущий
//! `Guard::check` напрямую, остался бы зелёным при точке, добавленной мимо
//! пропускного пункта, — а это ровно та ошибка, ради которой пункт один.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};

struct Node {
    child: Child,
    port: u16,
    dir: String,
    dest: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::fs::remove_dir_all(&self.dir).ok();
        std::fs::remove_dir_all(&self.dest).ok();
    }
}

/// Свободный порт спрашивается у ядра, а не берётся из головы: сборка идёт
/// параллельно, а занятый порт даёт узел, который отвечает **чужим** индексом и
/// выглядит при этом здоровым.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn tmp(tag: &str) -> String {
    let d = std::env::temp_dir()
        .join(format!("svindb-sec-{tag}-{}-{}", std::process::id(), free_port()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&d).ok();
    std::fs::create_dir_all(&d).unwrap();
    d
}

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

fn hash(password: &str) -> String {
    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .arg("passwd")
        .arg("10000")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            c.stdin.as_mut().unwrap().write_all(password.as_bytes())?;
            c.wait_with_output()
        })
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn node() -> Node {
    let dir = tmp("idx");
    let dest = tmp("arc");
    // Двадцать тысяч, а не двести: проверяются права, но снимок потом сверяется
    // полной проверкой, а на слишком коротких списках постингов блочные границы
    // не пишутся вовсе, и `check --full` считает это поломкой у **исходного**
    // индекса. Тест на снимке ловил бы тогда не снимок.
    let ndjson = format!("{dir}/corpus.ndjson");
    let mut f = std::io::BufWriter::new(std::fs::File::create(&ndjson).unwrap());
    for i in 0..20_000 {
        writeln!(f, "{{\"title\":\"загол{i}\",\"body\":\"общее слово{} тело{}\"}}", i % 97, i % 53)
            .unwrap();
    }
    drop(f);
    let idx = format!("{dir}/index");
    let ok = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["index", &ndjson, &idx, "10000"])
        .output()
        .unwrap();
    assert!(ok.status.success(), "индекс не собрался: {}", String::from_utf8_lossy(&ok.stderr));

    let cfg = format!("{dir}/svindb.json");
    std::fs::write(
        &cfg,
        format!(
            r#"{{
              "cors": {{ "origins": ["https://свои.рф"], "credentials": true }},
              "auth": {{
                "iterations": 10000,
                "keys_file": {keys},
                "roles": [
                  {{ "name": "чтение", "privileges": ["read"] }},
                  {{ "name": "запись", "privileges": ["read", "write"] }},
                  {{ "name": "главный", "privileges": ["admin"] }}
                ],
                "users": [
                  {{ "name": "читатель", "hash": {r}, "roles": ["чтение"] }},
                  {{ "name": "писатель", "hash": {w}, "roles": ["запись"] }},
                  {{ "name": "главный",  "hash": {a}, "roles": ["главный"] }}
                ]
              }},
              "snapshot": {{ "allow": [{dest_q}] }}
            }}"#,
            keys = json_str(&format!("{dir}/keys.json")),
            r = json_str(&hash("читатель-пароль")),
            w = json_str(&hash("писатель-пароль")),
            a = json_str(&hash("главный-пароль")),
            dest_q = json_str(&std::fs::canonicalize(&dest).unwrap().to_string_lossy()),
        ),
    )
    .unwrap();

    let port = free_port();
    let child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args([
            "serve",
            &idx,
            &port.to_string(),
            "--single-index",
            "--no-maintain",
            &format!("--config={cfg}"),
        ])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let node = Node { child, port, dir, dest };
    // Ждём, пока порт займётся именно нашим узлом.
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return node;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("узел не поднялся на {port}");
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

struct Reply {
    code: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Reply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

/// Свой клиент, а не `curl`: тест обязан работать там, где `curl` не поставлен,
/// и обязан видеть заголовки ответа дословно.
fn call(port: u16, method: &str, path: &str, extra: &[(&str, &str)], body: Option<&str>) -> Reply {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        if !extra.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            req.push_str("Content-Type: application/json\r\n");
        }
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
    let mut headers = Vec::new();
    let mut len = None;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).unwrap() == 0 || h.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            let (k, v) = (k.trim().to_string(), v.trim().to_string());
            if k.eq_ignore_ascii_case("content-length") {
                len = v.parse::<usize>().ok();
            }
            headers.push((k, v));
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
    Reply { code, headers, body }
}

fn basic(user: &str, pass: &str) -> String {
    // base64 вручную: тест не тянет ядро ради шестнадцати строк и заодно не
    // проверяет реализацию самой собой.
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let raw = format!("{user}:{pass}");
    let data = raw.as_bytes();
    let mut out = String::from("Basic ");
    for c in data.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(A[(n >> 18) as usize & 63] as char);
        out.push(A[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 { A[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if c.len() > 2 { A[n as usize & 63] as char } else { '=' });
    }
    out
}

#[test]
fn every_door_asks_who_is_there() {
    let n = node();
    let p = n.port;

    // Живость открыта намеренно: её спрашивает балансировщик, у которого учётной
    // записи нет, а состав индекса она не выдаёт.
    assert_eq!(call(p, "GET", "/_health", &[], None).code, 200, "живость закрылась");

    // Без ключа — 401, и обязательно с `WWW-Authenticate`: без него клиент не
    // узнает, чем представляться, а браузер не покажет окно входа.
    let r = call(
        p,
        "POST",
        "/_search",
        &[],
        Some(r#"{"k":5,"words":[{"field":1,"terms":["общее"]}]}"#),
    );
    assert_eq!(r.code, 401, "поиск пустил без ключа: {}", r.body);
    assert!(
        r.headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("www-authenticate")),
        "401 без WWW-Authenticate"
    );

    // Неверный пароль неотличим от несуществующего пользователя.
    let bad = basic("читатель", "не тот");
    assert_eq!(
        call(
            p,
            "POST",
            "/_search",
            &[("Authorization", &bad)],
            Some(r#"{"k":5,"words":[{"field":1,"terms":["общее"]}]}"#)
        )
        .code,
        401
    );
    let ghost = basic("такого-нет", "читатель-пароль");
    assert_eq!(
        call(
            p,
            "POST",
            "/_search",
            &[("Authorization", &ghost)],
            Some(r#"{"k":5,"words":[{"field":1,"terms":["общее"]}]}"#)
        )
        .code,
        401
    );

    // С нужным правом — 200.
    let reader = basic("читатель", "читатель-пароль");
    let r = call(
        p,
        "POST",
        "/_search",
        &[("Authorization", &reader)],
        Some(r#"{"k":5,"words":[{"field":1,"terms":["общее"]}]}"#),
    );
    assert_eq!(r.code, 200, "читателю не дали читать: {}", r.body);

    // С чужим — 403, а не 401: «тебе нельзя» вместо «представься».
    let r = call(
        p,
        "POST",
        "/_bulk",
        &[("Authorization", &reader)],
        Some(r#"{"documents":[{"id":"проба","title":"загол","body":"текст"}]}"#),
    );
    assert_eq!(r.code, 403, "читателю дали писать: {}", r.body);
    assert!(r.body.contains("write"), "в отказе не назвали нужное право: {}", r.body);

    let writer = basic("писатель", "писатель-пароль");
    let r = call(
        p,
        "POST",
        "/_bulk",
        &[("Authorization", &writer)],
        Some(r#"{"documents":[{"id":"новый","title":"загол","body":"текст"}]}"#),
    );
    assert_eq!(r.code, 200, "писателю не дали писать: {}", r.body);

    // Управляющие точки закрыты и от писателя.
    let r = call(p, "GET", "/_snapshot", &[("Authorization", &writer)], None);
    assert_eq!(r.code, 403, "писателю показали снимки: {}", r.body);
    let r = call(
        p,
        "POST",
        "/_security/api_key",
        &[("Authorization", &writer)],
        Some(r#"{"name":"свой","roles":["чтение"]}"#),
    );
    assert_eq!(r.code, 403, "писателю дали завести токен: {}", r.body);
}

/// Путь, о котором узел ничего не знает, всё равно требует прав. Иначе точка,
/// добавленная завтра, окажется открытой по умолчанию.
#[test]
fn an_unknown_path_is_closed_too() {
    let n = node();
    let r = call(n.port, "GET", "/чего-нибудь-новенького", &[], None);
    assert_eq!(r.code, 401, "неизвестный путь оказался открыт: {}", r.body);
}

#[test]
fn vector_text_outside_the_selector_is_a_bad_query_not_a_process_abort() {
    let n = node();
    let reader = basic("читатель", "читатель-пароль");
    let r =
        call(n.port, "POST", "/_search", &[("Authorization", &reader)], Some(r#"{"x":"vector"}"#));
    assert_eq!(r.code, 400, "произвольная строка vector выбрала KNN: {}", r.body);
    assert_eq!(
        call(n.port, "GET", "/_health", &[], None).code,
        200,
        "узел завершился после JSON со строкой vector"
    );
}

#[test]
fn a_token_works_and_stops_working_when_revoked() {
    let n = node();
    let p = n.port;
    let admin = basic("главный", "главный-пароль");

    let r = call(
        p,
        "POST",
        "/_security/api_key",
        &[("Authorization", &admin)],
        Some(r#"{"name":"сборщик","roles":["чтение"]}"#),
    );
    assert_eq!(r.code, 201, "токен не завёлся: {}", r.body);
    let encoded = field(&r.body, "encoded");
    let id = field(&r.body, "id");
    let head = format!("ApiKey {encoded}");

    let r = call(
        p,
        "POST",
        "/_search",
        &[("Authorization", &head)],
        Some(r#"{"k":5,"words":[{"field":1,"terms":["общее"]}]}"#),
    );
    assert_eq!(r.code, 200, "токен не пустил: {}", r.body);
    // Прав токена ровно столько, сколько у его ролей.
    let r = call(
        p,
        "POST",
        "/_bulk",
        &[("Authorization", &head)],
        Some(r#"{"documents":[{"id":"проба","title":"загол","body":"текст"}]}"#),
    );
    assert_eq!(r.code, 403, "токен чтения пустил в приём: {}", r.body);

    let r =
        call(p, "DELETE", &format!("/_security/api_key/{id}"), &[("Authorization", &admin)], None);
    assert_eq!(r.code, 200, "отзыв не сработал: {}", r.body);
    let r = call(
        p,
        "POST",
        "/_search",
        &[("Authorization", &head)],
        Some(r#"{"k":5,"words":[{"field":1,"terms":["общее"]}]}"#),
    );
    assert_eq!(r.code, 401, "отозванный токен продолжает работать: {}", r.body);

    // Список токенов секрета не выдаёт.
    let r = call(p, "GET", "/_security/api_key", &[("Authorization", &admin)], None);
    assert_eq!(r.code, 200);
    assert!(!r.body.contains(&encoded), "список выдал секрет");
}

/// CORS проверяется предполётным запросом, а не наличием заголовка в обычном
/// ответе: браузер шлёт `OPTIONS` **до** запроса и без ответа на него не
/// посылает ничего.
#[test]
fn the_preflight_answers_before_the_request() {
    let n = node();
    let p = n.port;

    let r = call(
        p,
        "OPTIONS",
        "/_search",
        &[
            ("Origin", "https://свои.рф"),
            ("Access-Control-Request-Method", "POST"),
            ("Access-Control-Request-Headers", "authorization, content-type"),
        ],
        None,
    );
    assert_eq!(r.code, 204, "предполётный запрос не прошёл: {}", r.body);
    assert_eq!(r.header("Access-Control-Allow-Origin"), Some("https://свои.рф"));
    assert_eq!(r.header("Access-Control-Allow-Credentials"), Some("true"));
    assert!(r.header("Access-Control-Allow-Methods").is_some());
    assert_eq!(
        r.header("Access-Control-Allow-Headers"),
        Some("authorization, content-type"),
        "запрошенные заголовки не отражены"
    );
    assert_eq!(r.header("Vary"), Some("Origin"), "без Vary кеш перепутает источники");

    // Предполётный запрос не требует пароля: браузер учётных данных в него не
    // кладёт, и потребовать их значило бы не пустить никого.
    assert!(r.header("WWW-Authenticate").is_none());

    // Чужому источнику не достаётся ничего.
    let r = call(p, "OPTIONS", "/_search", &[("Origin", "https://чужие.рф")], None);
    assert!(r.header("Access-Control-Allow-Origin").is_none(), "чужому дали доступ");

    // Заголовки уходят и с обычным ответом, иначе браузер отбросит уже
    // полученные данные.
    let reader = basic("читатель", "читатель-пароль");
    let r = call(
        p,
        "POST",
        "/_search",
        &[("Authorization", &reader), ("Origin", "https://свои.рф")],
        Some(r#"{"k":5,"words":[{"field":1,"terms":["общее"]}]}"#),
    );
    assert_eq!(r.code, 200);
    assert_eq!(r.header("Access-Control-Allow-Origin"), Some("https://свои.рф"));
}

#[test]
fn a_foreign_browser_origin_cannot_mutate_with_basic_credentials() {
    let n = node();
    let writer = basic("писатель", "писатель-пароль");
    let body = r#"{"documents":[{"id":"csrf","title":"загол","body":"текст"}]}"#;
    let r = call(
        n.port,
        "POST",
        "/_bulk",
        &[
            ("Authorization", &writer),
            ("Origin", "https://чужие.рф"),
            ("Content-Type", "text/plain"),
        ],
        Some(body),
    );
    assert_eq!(r.code, 403, "чужой Origin выполнил мутацию: {}", r.body);

    let r = call(
        n.port,
        "POST",
        "/_bulk",
        &[("Authorization", &writer), ("Content-Type", "text/plain")],
        Some(body),
    );
    assert_eq!(r.code, 415, "safelisted Content-Type принят: {}", r.body);

    let r = call(
        n.port,
        "POST",
        "/_bulk",
        &[("Authorization", &writer), ("Origin", "https://свои.рф")],
        Some(body),
    );
    assert_eq!(r.code, 200, "доверенный Origin перестал работать: {}", r.body);
}

#[test]
fn snapshots_are_created_and_listed_but_never_deleted() {
    let n = node();
    let p = n.port;
    let admin = basic("главный", "главный-пароль");

    let r = call(p, "POST", "/_snapshot/ночной", &[("Authorization", &admin)], Some("{}"));
    assert_eq!(r.code, 201, "снимок не снялся: {}", r.body);
    assert!(r.body.contains("\"docs\""), "{}", r.body);

    let r = call(p, "GET", "/_snapshot", &[("Authorization", &admin)], None);
    assert_eq!(r.code, 200);
    assert!(r.body.contains("ночной"), "снимка нет в списке: {}", r.body);

    let r = call(p, "GET", "/_snapshot/ночной", &[("Authorization", &admin)], None);
    assert_eq!(r.code, 200, "{}", r.body);
    assert!(r.body.contains("\"members\""), "{}", r.body);

    let r = call(p, "GET", "/_snapshot/../ночной", &[("Authorization", &admin)], None);
    assert_eq!(r.code, 400, "обход имени снимка принят: {}", r.body);

    // Даже совместимое оглавление за пределами allow не читается через ссылку.
    let outside = format!("{}/outside-snapshot", n.dir);
    std::fs::create_dir(&outside).unwrap();
    std::fs::copy(format!("{}/ночной/snapshot.json", n.dest), format!("{outside}/snapshot.json"))
        .unwrap();
    std::os::unix::fs::symlink(&outside, format!("{}/linked", n.dest)).unwrap();
    let r = call(p, "GET", "/_snapshot/linked", &[("Authorization", &admin)], None);
    assert_eq!(r.code, 404, "ссылка обошла snapshot.allow: {}", r.body);

    // Тем же именем второй раз — отказ, а не тихая перезапись.
    let r = call(p, "POST", "/_snapshot/ночной", &[("Authorization", &admin)], Some("{}"));
    assert_eq!(r.code, 409, "имя переиспользовалось молча: {}", r.body);

    // Удаления нет и не появится незаметно.
    let r = call(p, "DELETE", "/_snapshot/ночной", &[("Authorization", &admin)], None);
    assert_eq!(r.code, 405, "снимок удалили через API: {}", r.body);
    assert!(std::fs::read_dir(&n.dest).unwrap().count() > 0, "каталог снимка исчез после DELETE");

    // Приёмник вне разрешённого списка — отказ.
    let r = call(
        p,
        "POST",
        "/_snapshot/чужой",
        &[("Authorization", &admin)],
        Some(r#"{"dest":"/tmp"}"#),
    );
    assert_eq!(r.code, 403, "снимок ушёл в неразрешённый приёмник: {}", r.body);
}

/// Поднимается снятое — тем же двоичным файлом, командой, а не по сети.
#[test]
fn a_snapshot_taken_over_http_restores_from_the_command_line() {
    let n = node();
    let admin = basic("главный", "главный-пароль");
    let r =
        call(n.port, "POST", "/_snapshot/для-подъёма", &[("Authorization", &admin)], Some("{}"));
    assert_eq!(r.code, 201, "{}", r.body);

    let dst = tmp("restored");
    std::fs::remove_dir_all(&dst).ok();
    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["restore", &format!("{}/для-подъёма", n.dest), &dst])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "не поднялось: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    // Сперва исходный: снимок, сверяемый с заведомо больным индексом, ничего
    // не доказывает.
    let src_ok = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["check", &format!("{}/index", n.dir), "--full"])
        .output()
        .unwrap();
    assert!(
        src_ok.status.success(),
        "исходный индекс сам не цел — проверка снимка вхолостую: {}",
        String::from_utf8_lossy(&src_ok.stdout)
    );
    let check = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["check", &dst, "--full"])
        .output()
        .unwrap();
    assert!(
        check.status.success(),
        "поднятый индекс не прошёл проверку: {}{}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );
    std::fs::remove_dir_all(&dst).ok();
}

/// Значение из плоского JSON — тесту хватает, разбирать по-настоящему тут
/// нечего.
fn field(body: &str, key: &str) -> String {
    let pat = format!("\"{key}\":\"");
    let at = body.find(&pat).unwrap_or_else(|| panic!("нет поля {key} в {body}")) + pat.len();
    let rest = &body[at..];
    rest[..rest.find('"').unwrap()].to_string()
}

/// Настройки индекса: кто вправе их менять и что происходит с ними на диске.
///
/// Проверять это внутренними вызовами нельзя ровно по той же причине, по которой
/// здесь проверяются права: `PUT /_settings` не оканчивается ни на один суффикс
/// записи, и без явной строки в `auth::needed` он получил бы `Read` по
/// умолчанию — то есть любой читатель менял бы нарезку сегментов. Такую ошибку
/// видно только через настоящий сокет и настоящий пропускной пункт.
#[test]
fn settings_are_admin_only_and_survive_a_restart() {
    let node = node();
    let reader = basic("читатель", "читатель-пароль");
    let writer = basic("писатель", "писатель-пароль");
    let admin = basic("главный", "главный-пароль");

    // Читателю и писателю — отказ. Именно это и ломалось бы молча.
    for (who, head) in [("читатель", &reader), ("писатель", &writer)] {
        let r = call(node.port, "GET", "/_settings", &[("Authorization", head)], None);
        assert_eq!(r.code, 403, "{who} увидел настройки: {}", r.body);
        let r = call(
            node.port,
            "PUT",
            "/_settings",
            &[("Authorization", head)],
            Some(r#"{"target_docs":9000000}"#),
        );
        assert_eq!(r.code, 403, "{who} поменял настройки: {}", r.body);
    }

    // Главному — можно, и в ответе видны все настройки.
    let r = call(node.port, "GET", "/_settings", &[("Authorization", &admin)], None);
    assert_eq!(r.code, 200, "{}", r.body);
    for k in ["refresh_interval_ms", "flush_docs", "target_docs", "dead_fraction"] {
        assert!(r.body.contains(k), "в настройках нет {k}: {}", r.body);
    }

    // Опечатка — отказ с подсказкой, а не молчаливый успех.
    let r = call(
        node.port,
        "PUT",
        "/_settings",
        &[("Authorization", &admin)],
        Some(r#"{"target_dokc":9000000}"#),
    );
    assert_eq!(r.code, 400, "{}", r.body);
    assert!(r.body.contains("target_dokc"), "отказ не назвал ключ: {}", r.body);
    assert!(r.body.contains("target_docs"), "отказ не подсказал верный: {}", r.body);

    // Бессмыслица — отказ с причиной.
    let r = call(
        node.port,
        "PUT",
        "/_settings",
        &[("Authorization", &admin)],
        Some(r#"{"dead_fraction":9}"#),
    );
    assert_eq!(r.code, 400, "{}", r.body);

    // Настоящая правка: частичная, остальное не тронуто.
    let r = call(
        node.port,
        "PUT",
        "/_settings",
        &[("Authorization", &admin)],
        Some(r#"{"target_docs":3000000,"refresh_interval_ms":250}"#),
    );
    assert_eq!(r.code, 200, "{}", r.body);
    assert!(r.body.contains("3000000"), "{}", r.body);
    // Не тронутое правкой обязано остаться умолчанием — оно и проверяется,
    // а не конкретное число: умолчание меняется, смысл проверки нет.
    let want = format!("\"flush_docs\":{}", svindb_core::settings::Settings::default().flush_docs);
    assert!(r.body.contains(&want), "flush_docs съехал, ждали {want}: {}", r.body);

    // На диске — то же самое, и **до** ответа: узел, работающий по настройке,
    // не дожившей до перезапуска, объяснить было бы нечем.
    let on_disk = std::fs::read_to_string(format!("{}/index/settings.json", node.dir))
        .expect("settings.json не записан");
    assert!(on_disk.contains("3000000"), "на диске не то: {on_disk}");
    assert!(on_disk.contains("250"), "на диске не то: {on_disk}");
}
