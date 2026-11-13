//! Ограниченный кэш распакованного authoritative source.
//!
//! Page cache и mmap уже принадлежат ядру Linux. Этот кэш решает другую
//! проблему: повторная распаковка одного и того же immutable source при
//! соседних поисках. Ключ содержит generation `Segments`, поэтому обновление,
//! flush или merge никогда не может вернуть source из старого физического
//! адреса. Размер ограничен байтами, а не числом записей: один большой JSON не
//! должен вытеснить весь рабочий набор мелких документов.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use svindb_core::work::Guard;

/// Кэш не удерживает entries тяжелее одного мегабайта: большой source обычно
/// читается один раз, а его удержание дороже повторной распаковки.
pub const MAX_ENTRY_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Key {
    generation: u64,
    doc: u64,
}

#[derive(Default)]
struct State {
    entries: HashMap<Key, Arc<[u8]>>,
    order: VecDeque<Key>,
    bytes: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
}

/// Snapshot кэша для `/_metrics` и `/_stats`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    pub capacity_bytes: usize,
    pub bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Generation-aware bounded source cache.
pub struct Cache {
    capacity_bytes: usize,
    state: Mutex<State>,
}

impl Cache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self { capacity_bytes, state: Mutex::new(State::default()) }
    }

    fn get(&self, key: Key) -> Option<Vec<u8>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let value = state.entries.get(&key).cloned();
        match value {
            Some(value) => {
                state.hits = state.hits.saturating_add(1);
                if let Some(at) = state.order.iter().position(|candidate| *candidate == key) {
                    state.order.remove(at);
                }
                state.order.push_back(key);
                Some(value.as_ref().to_vec())
            }
            None => {
                state.misses = state.misses.saturating_add(1);
                None
            }
        }
    }

    fn insert(&self, key: Key, raw: &[u8]) {
        if self.capacity_bytes == 0 || raw.is_empty() || raw.len() > MAX_ENTRY_BYTES {
            return;
        }
        let value: Arc<[u8]> = Arc::from(raw);
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(old) = state.entries.insert(key, Arc::clone(&value)) {
            state.bytes = state.bytes.saturating_sub(old.len());
            if let Some(at) = state.order.iter().position(|candidate| *candidate == key) {
                state.order.remove(at);
            }
        }
        state.bytes = state.bytes.saturating_add(value.len());
        state.order.push_back(key);
        while state.bytes > self.capacity_bytes {
            let Some(old_key) = state.order.pop_front() else { break };
            let Some(old) = state.entries.remove(&old_key) else { continue };
            state.bytes = state.bytes.saturating_sub(old.len());
            state.evictions = state.evictions.saturating_add(1);
        }
    }

    /// Возвращает значения в порядке `docs`, запрашивая reader только для
    /// cache-miss. Дубликаты в MGET намеренно сохраняют порядок и получают одну
    /// и ту же immutable копию; cache key не зависит от external ID.
    pub fn many<F>(
        &self,
        generation: u64,
        docs: &[u64],
        guard: Option<&Guard>,
        fetch: F,
    ) -> Vec<Option<Vec<u8>>>
    where
        F: FnOnce(&[u64], Option<&Guard>) -> Vec<Option<Vec<u8>>>,
    {
        let mut out = vec![None; docs.len()];
        let mut misses: Vec<(usize, u64)> = Vec::new();
        for (at, &doc) in docs.iter().enumerate() {
            if guard.is_some_and(|guard| !guard.check()) {
                return out;
            }
            let key = Key { generation, doc };
            if let Some(value) = self.get(key) {
                out[at] = Some(value);
            } else {
                misses.push((at, doc));
            }
        }
        if misses.is_empty() {
            return out;
        }
        let requested: Vec<u64> = misses.iter().map(|(_, doc)| *doc).collect();
        let fetched = fetch(&requested, guard);
        for ((at, doc), value) in misses.into_iter().zip(fetched) {
            if guard.is_some_and(|guard| !guard.check()) {
                break;
            }
            if let Some(raw) = value {
                self.insert(Key { generation, doc }, &raw);
                out[at] = Some(raw);
            }
        }
        out
    }

    /// Удаляет устаревший рабочий набор. Это вызывается только при pressure и
    /// не меняет correctness: cache не является canonical source.
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.entries.clear();
        state.order.clear();
        state.bytes = 0;
    }

    pub fn stats(&self) -> Stats {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        Stats {
            capacity_bytes: self.capacity_bytes,
            bytes: state.bytes,
            entries: state.entries.len(),
            hits: state.hits,
            misses: state.misses,
            evictions: state.evictions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn generation_change_never_returns_an_old_source() {
        let cache = Cache::new(1024);
        let calls = AtomicUsize::new(0);
        let docs = [7u64];
        let first = cache.many(1, &docs, None, |_, _| {
            calls.fetch_add(1, Ordering::Relaxed);
            vec![Some(br#"{"v":1}"#.to_vec())]
        });
        assert_eq!(first[0].as_deref(), Some(br#"{"v":1}"#.as_slice()));
        let second = cache.many(1, &docs, None, |_, _| {
            calls.fetch_add(1, Ordering::Relaxed);
            vec![Some(br#"{"v":2}"#.to_vec())]
        });
        assert_eq!(second[0].as_deref(), Some(br#"{"v":1}"#.as_slice()));
        let third = cache.many(2, &docs, None, |_, _| {
            calls.fetch_add(1, Ordering::Relaxed);
            vec![Some(br#"{"v":2}"#.to_vec())]
        });
        assert_eq!(third[0].as_deref(), Some(br#"{"v":2}"#.as_slice()));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn byte_limit_evicts_old_entries_and_skips_large_sources() {
        let cache = Cache::new(8);
        let docs = [1u64, 2u64];
        let _ = cache.many(1, &docs, None, |_, _| {
            vec![Some(b"12345678".to_vec()), Some(vec![b'x'; MAX_ENTRY_BYTES + 1])]
        });
        let stats = cache.stats();
        assert_eq!(stats.bytes, 8);
        assert_eq!(stats.entries, 1);
        let _ = cache.many(1, &[1], None, |_, _| vec![Some(b"refetched".to_vec())]);
        assert_eq!(cache.stats().misses, 2);
    }

    #[test]
    fn cancelled_lookup_does_not_call_reader() {
        let cache = Cache::new(1024);
        let guard = Guard::new(1);
        guard.cancel();
        let called = AtomicUsize::new(0);
        let out = cache.many(1, &[1], Some(&guard), |_, _| {
            called.fetch_add(1, Ordering::Relaxed);
            vec![Some(b"bad".to_vec())]
        });
        assert_eq!(out, vec![None]);
        assert_eq!(called.load(Ordering::Relaxed), 0);
    }
}
