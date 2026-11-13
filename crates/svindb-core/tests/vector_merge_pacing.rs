//! Изолированный процесс: глобальный hook не влияет на другие unit-тесты.
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use svindb_core::{container, vector, work};

#[derive(Default)]
struct Hook {
    bytes: AtomicU64,
    stop: AtomicBool,
}
impl work::CompactionHook for Hook {
    fn checkpoint(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    fn should_stop(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
}

#[test]
fn vector_merge_accounts_only_real_q8_and_obeys_shutdown() {
    let hook = Arc::new(Hook::default());
    assert!(work::install_compaction_hook(hook.clone()));
    let data: Vec<Vec<f32>> =
        (0..20000).map(|i| if i % 20 == 1 { vec![i as f32; 8] } else { vec![] }).collect();
    let built = vector::build(&data, 8).unwrap();
    let src = container::in_memory("paced", [("v.q8", &built.codes), ("v.qm", &built.meta)]);
    let vectors = vector::Vectors::open(&src, "v").unwrap();
    let mut codes = Vec::new();
    vector::merge(&[(Some(&vectors), data.len())], 8, &|_, _| true, &mut codes).unwrap().unwrap();
    assert_eq!(codes.len(), 1000 * 8);
    assert_eq!(hook.bytes.load(Ordering::Relaxed), codes.len() as u64);
    hook.stop.store(true, Ordering::Relaxed);
    let mut partial = Vec::new();
    let error =
        vector::merge(&[(Some(&vectors), data.len())], 8, &|_, _| true, &mut partial).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    assert!(partial.len() < codes.len());
}
