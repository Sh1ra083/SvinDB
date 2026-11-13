//! Приёмка bounded point-read на настоящем сокете.
//!
//! Каталог `catalog.bin` намеренно публикуется после запуска: request path не
//! имеет права превращать холодный/устаревший inode-каталог в полный обход.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");
static UNIQUE: AtomicU64 = AtomicU64::new(1);

struct Reply {
    code: u16,
    body: String,
}

fn root(tag: &str) -> PathBuf {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("svindb-point-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn call(port: u16, method: &str, path: &str, body: Option<&str>) -> Reply {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return Reply { code: 0, body: String::new() };
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).expect("таймаут чтения");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return Reply { code: 0, body: String::new() };
    }
    let mut bytes = Vec::new();
    if stream.read_to_end(&mut bytes).is_err() {
        return Reply { code: 0, body: String::new() };
    }
    let text = String::from_utf8_lossy(&bytes);
    let code = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b).to_string();
    Reply { code, body }
}

struct Node {
    child: Child,
    port: u16,
    log: PathBuf,
}

impl Node {
    fn start(dir: &Path, extra: &[&str]) -> Node {
        let log = dir.with_extension("server.log");
        let output = std::fs::File::create(&log).expect("лог");
        let mut child = Command::new(BIN);
        child
            .args(["serve", &dir.to_string_lossy(), "0", "--no-maintain"])
            .args(extra)
            .env("SVINDB_DISK_FLOOR_MB", "1")
            .stdout(output.try_clone().expect("лог stdout"))
            .stderr(output);
        let child = child.spawn().expect("сервер");
        let mut node = Node { child, port: 0, log };
        let until = Instant::now() + Duration::from_secs(20);
        while Instant::now() < until {
            if let Some(status) = node.child.try_wait().expect("статус сервера") {
                panic!(
                    "сервер завершился до сокета: {status}; лог: {}",
                    std::fs::read_to_string(&node.log).unwrap_or_default()
                );
            }
            if let Some(port) = std::fs::read_to_string(&node.log).ok().and_then(|s| {
                s.lines()
                    .find_map(|l| l.strip_prefix("SVINDB_LISTEN "))
                    .and_then(|a| a.parse::<std::net::SocketAddr>().ok())
                    .map(|a| a.port())
            }) {
                node.port = port;
                if call(port, "GET", "/_health", None).code == 200 {
                    return node;
                }
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!(
            "сервер не поднялся; лог: {}",
            std::fs::read_to_string(&node.log).unwrap_or_default()
        );
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.log);
    }
}

fn make_segments(dir: &Path, count: u32) {
    let mut ix =
        svindb_core::Indexer::new(&dir.to_string_lossy(), vec!["body".into()], vec![1.0], 1);
    for n in 0..count {
        ix.add(&format!("doc-{n}"), &["содержимое"]);
        ix.flush_now();
    }
    drop(ix);
}

fn catalog_bytes(dir: &Path) -> Vec<u8> {
    let segs = svindb_core::Segments::load(&dir.to_string_lossy()).expect("состав сегментов");
    let sources: Vec<&svindb_core::index::Index> =
        segs.list.iter().map(std::sync::Arc::as_ref).collect();
    svindb_core::catalog::build(&sources)
}

fn publish_catalog(dir: &Path, bytes: &[u8]) {
    let tmp = dir.join("catalog.bin.point-readiness.tmp");
    std::fs::write(&tmp, bytes).expect("временный каталог");
    std::fs::rename(tmp, dir.join(svindb_core::catalog::FILE))
        .expect("атомарная публикация каталога");
}

fn wait_code(port: u16, method: &str, path: &str, body: Option<&str>, code: u16) -> Reply {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let reply = call(port, method, path, body);
        if reply.code == code {
            return reply;
        }
        assert!(
            Instant::now() < until,
            "ожидался HTTP {code}, получен {}: {}",
            reply.code,
            reply.body
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn missing_catalog_is_bounded_then_single_index_reads_after_atomic_publish() {
    let dir = root("single");
    make_segments(&dir, 10);
    let bytes = catalog_bytes(&dir);
    let node = Node::start(&dir, &["--single-index", "--read-only", "--fields=body:text"]);
    let blocked = wait_code(node.port, "GET", "/_doc/doc-9", None, 503);
    assert!(blocked.body.contains("id_directory_not_ready"), "bounded refusal: {}", blocked.body);
    publish_catalog(&dir, &bytes);
    let found = wait_code(node.port, "GET", "/_doc/doc-9", None, 200);
    assert!(found.body.contains("\"id\":\"doc-9\""), "point read: {}", found.body);
    assert_eq!(call(node.port, "GET", "/_doc/no-such", None).code, 404);
    let mget = wait_code(node.port, "POST", "/_mget", Some(r#"{"ids":["doc-9","no-such"]}"#), 200);
    assert!(mget.body.contains("\"found\":false"), "mget missing item: {}", mget.body);
    drop(node);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn valid_catalog_is_ready_on_start_without_warm() {
    let dir = root("single-ready");
    make_segments(&dir, 9);
    let bytes = catalog_bytes(&dir);
    publish_catalog(&dir, &bytes);
    let node = Node::start(&dir, &["--single-index", "--read-only", "--fields=body:text"]);
    let got = wait_code(node.port, "GET", "/_doc/doc-0", None, 200);
    assert!(got.body.contains("\"id\":\"doc-0\""), "готовый каталог: {}", got.body);
    let stats = call(node.port, "GET", "/_stats", None);
    assert!(stats.body.contains("\"id_directory_ready\":true"), "stats readiness: {}", stats.body);
    drop(node);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn missing_catalog_is_bounded_then_multi_index_reads_after_atomic_publish() {
    let dir = root("multi");
    let bootstrap = Node::start(&dir, &[]);
    assert_eq!(call(bootstrap.port, "PUT", "/alpha", Some(r#"{"fields":"body:text"}"#)).code, 200);
    drop(bootstrap);
    // Физическое имя кодируется root registry и не равно публичному имени.
    let paths: Vec<_> = std::fs::read_dir(dir.join("indices"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(paths.len(), 1);
    let index = paths[0].clone();
    make_segments(&index, 10);
    let bytes = catalog_bytes(&index);
    let catalog = index.join(svindb_core::catalog::FILE);
    let _ = std::fs::remove_file(&catalog);
    let node = Node::start(&dir, &["--read-only"]);
    let blocked = wait_code(node.port, "GET", "/alpha/_doc/doc-9", None, 503);
    assert!(blocked.body.contains("id_directory_not_ready"), "multi refusal: {}", blocked.body);
    publish_catalog(&index, &bytes);
    let got = wait_code(node.port, "GET", "/alpha/_doc/doc-9", None, 200);
    assert!(got.body.contains("\"id\":\"doc-9\""), "multi point read: {}", got.body);
    let mget =
        wait_code(node.port, "POST", "/alpha/_mget", Some(r#"{"ids":["doc-9","missing"]}"#), 200);
    assert!(mget.body.contains("\"found\":false"), "multi mget: {}", mget.body);
    drop(node);
    let _ = std::fs::remove_dir_all(dir);
}
