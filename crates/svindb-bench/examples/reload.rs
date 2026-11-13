//! Замер: полное открытие против инкрементального на многих сегментах.
use svindb_core::{indexer::Indexer, segments::Segments};

fn main() {
    let dir = std::env::temp_dir()
        .join(format!("svindb-bench-reload-{}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&dir).ok();
    let n: usize = std::env::var("SEGS").ok().and_then(|s| s.parse().ok()).unwrap_or(400);
    let per = 200;
    let mut ix = Indexer::new(&dir, vec!["body".into()], vec![1.0], per as u32);
    for i in 0..(n * per) {
        ix.add(&format!("d{i:07}"), &[&format!("общее слово{} тело{}", i % 97, i % 53)]);
    }
    ix.flush_now();
    drop(ix);

    let segs = Segments::load(&dir).expect("индекс");
    println!("сегментов: {}, документов: {}", segs.list.len(), segs.docs());

    let t = std::time::Instant::now();
    let full = Segments::load(&dir).expect("полное");
    let full_ms = t.elapsed().as_secs_f64() * 1000.0;

    let t = std::time::Instant::now();
    let inc = Segments::reopen(&dir, &segs).expect("инкрементальное");
    let inc_ms = t.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(full.list.len(), inc.list.len());
    assert_eq!(full.docs(), inc.docs());
    assert_eq!(full.base, inc.base, "начала обязаны совпасть");
    println!("полное открытие      : {full_ms:.1} мс");
    println!("инкрементальное      : {inc_ms:.1} мс");
    println!("быстрее в            : {:.0} раз", full_ms / inc_ms.max(0.001));
    std::fs::remove_dir_all(&dir).ok();
}
