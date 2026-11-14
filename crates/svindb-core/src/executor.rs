//! Постоянный search pool; admission и deadline принадлежат вызывающему.
//!
//! Здесь не запускаются compaction и point hydration. Внешний governor должен
//! зарезервировать lanes до вызова и удерживать permit до завершения запроса.

use rayon::prelude::*;
use std::sync::OnceLock;

static SEARCH: OnceLock<rayon::ThreadPool> = OnceLock::new();

/// Устанавливается до listener. Ошибка создания потоков не скрывается fallback.
pub fn install(width: usize) -> Result<(), String> {
    if width == 0 {
        return Err("ширина search executor должна быть положительной".into());
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(width)
        .thread_name(|n| format!("svindb-search-{n}"))
        .build()
        .map_err(|error| format!("search executor: {error}"))?;
    SEARCH.set(pool).map_err(|_| "search executor уже установлен".into())
}

struct CounterFrame(crate::stats::Counters);

impl Drop for CounterFrame {
    fn drop(&mut self) {
        let _ = crate::stats::take();
        crate::stats::add(self.0);
    }
}

#[derive(Clone, Copy, Default)]
struct ThreadIo {
    read_bytes: u64,
    write_bytes: u64,
}

#[cfg(target_os = "linux")]
fn thread_io() -> ThreadIo {
    let Ok(text) = std::fs::read_to_string("/proc/thread-self/io") else {
        return ThreadIo::default();
    };
    let value = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    ThreadIo { read_bytes: value("read_bytes:"), write_bytes: value("write_bytes:") }
}

#[cfg(not(target_os = "linux"))]
fn thread_io() -> ThreadIo {
    ThreadIo::default()
}

fn measured<R>(run: impl FnOnce() -> R) -> (R, crate::stats::Counters) {
    let frame = CounterFrame(crate::stats::take());
    let io_before = if crate::stats::ENABLED { thread_io() } else { ThreadIo::default() };
    let result = run();
    let io_after = if crate::stats::ENABLED { thread_io() } else { ThreadIo::default() };
    let mut counters = crate::stats::take();
    if crate::stats::ENABLED {
        counters.io_read_bytes = io_after.read_bytes.saturating_sub(io_before.read_bytes);
        counters.io_write_bytes = io_after.write_bytes.saturating_sub(io_before.write_bytes);
    }
    drop(frame);
    (result, counters)
}

fn pooled<R: Send, F: Fn(usize) -> R + Sync + Send>(
    pool: &rayon::ThreadPool,
    lanes: usize,
    run: F,
) -> Vec<R> {
    // Indexed parallel iterator сохраняет порядок lanes. Он не размножает
    // число исполнений: локальные heaps/aggregation state ровно по одному на lane.
    let parts: Vec<_> =
        pool.install(|| (0..lanes).into_par_iter().map(|at| measured(|| run(at))).collect());
    parts
        .into_iter()
        .map(|(result, counters)| {
            crate::stats::add(counters);
            result
        })
        .collect()
}

pub(crate) fn map<R: Send, F: Fn(usize) -> R + Sync + Send>(lanes: usize, run: F) -> Vec<R> {
    if let Some(pool) = SEARCH.get() {
        return pooled(pool, lanes, run);
    }
    // Library/CLI без server runtime сохраняют scoped ownership. Глобальный
    // Rayon pool не создаётся неявно первым запросом.
    std::thread::scope(|scope| {
        let run = &run;
        let handles: Vec<_> =
            (0..lanes).map(|at| scope.spawn(move || measured(|| run(at)))).collect();
        handles
            .into_iter()
            .map(|handle| {
                let (result, counters) = handle.join().expect("search lane");
                crate::stats::add(counters);
                result
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    #[test]
    fn concurrent_requests_share_fixed_workers_and_preserve_lane_order() {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(3).build().unwrap();
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let ids = Mutex::new(HashSet::new());
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    let output = pooled(&pool, 7, |lane| {
                        let current = active.fetch_add(1, Ordering::AcqRel) + 1;
                        peak.fetch_max(current, Ordering::AcqRel);
                        ids.lock().unwrap().insert(std::thread::current().id());
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        active.fetch_sub(1, Ordering::AcqRel);
                        lane * lane
                    });
                    assert_eq!(output, (0..7).map(|lane| lane * lane).collect::<Vec<_>>());
                });
            }
        });
        assert!(peak.load(Ordering::Acquire) <= 3);
        assert_eq!(ids.lock().unwrap().len(), 3);
    }

    #[cfg(feature = "stats")]
    #[test]
    fn counters_belong_to_each_call_not_to_the_reused_worker() {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap();
        let _ = crate::stats::take();
        for amount in [3, 7, 0] {
            let _: Vec<_> = pooled(&pool, 4, |_| crate::stats::scored(amount));
            assert_eq!(crate::stats::take().scored, amount * 4);
        }
    }

    #[test]
    fn running_lanes_observe_cancellation_and_release_the_pool() {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap();
        let guard = crate::work::Guard::new(2000);
        let entered = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let work = scope.spawn(|| {
                pooled(&pool, 2, |_| {
                    entered.fetch_add(1, Ordering::Release);
                    while guard.check() {
                        std::thread::yield_now();
                    }
                })
            });
            let until = std::time::Instant::now() + std::time::Duration::from_secs(1);
            while entered.load(Ordering::Acquire) != 2 && std::time::Instant::now() < until {
                std::thread::yield_now();
            }
            assert_eq!(entered.load(Ordering::Acquire), 2, "отменяется ещё не начатая работа");
            let cancelled = std::time::Instant::now();
            guard.cancel();
            work.join().unwrap();
            assert!(cancelled.elapsed() < std::time::Duration::from_millis(250));
        });
        assert_eq!(pooled(&pool, 2, |lane| lane), [0, 1]);
    }
}
