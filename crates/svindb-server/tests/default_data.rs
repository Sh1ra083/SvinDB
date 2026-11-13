//! Каталог данных по умолчанию привязан к бинарнику, а не к оболочке.

use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

const BIN: &str = env!("CARGO_BIN_EXE_svindb");
static UNIQUE: AtomicU64 = AtomicU64::new(1);

struct Node {
    child: Child,
    root: std::path::PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::fs::remove_dir_all(&self.root).ok();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn fixture(tag: &str) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "svindb-default-data-{tag}-{}-{}-{unique}",
        std::process::id(),
        free_port()
    ));
    std::fs::remove_dir_all(&root).ok();
    let working = root.join("чужой-текущий-каталог");
    std::fs::create_dir_all(&working).unwrap();
    let binary = root.join("svindb");
    std::fs::copy(BIN, &binary).unwrap();
    (root, working, binary)
}

#[test]
fn a_port_only_launch_creates_a_multi_root_next_to_the_binary() {
    let (root, working, binary) = fixture("обычный");
    let port = free_port();
    let child = Command::new(&binary)
        .current_dir(&working)
        .args(["serve", &port.to_string(), "--no-maintain"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut node = Node { child, root };

    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            assert!(node.root.join("data").is_dir(), "data рядом с бинарником не создан");
            assert!(
                node.root.join("data/multi-index-v1.json").is_file(),
                "новый каталог по умолчанию не стал multi-index root"
            );
            assert!(node.root.join("data/indices").is_dir(), "нет каталога независимых индексов");
            assert!(!working.join("data").exists(), "data ошибочно зависит от текущего каталога");
            return;
        }
        if let Some(status) = node.child.try_wait().unwrap() {
            panic!("узел завершился до открытия порта: {status}");
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("узел не открыл порт {port}");
}

#[test]
fn read_only_does_not_create_missing_default_data() {
    let (root, working, binary) = fixture("только-чтение");
    let output = Command::new(&binary)
        .current_dir(&working)
        .args(["serve", &free_port().to_string(), "--read-only"])
        .output()
        .unwrap();

    assert!(!output.status.success(), "режим только чтения создал пустую базу");
    assert!(!root.join("data").exists(), "--read-only записал каталог data");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--read-only запрещает создавать"),
        "отказ не объяснил причину: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_dir_all(root).ok();
}
