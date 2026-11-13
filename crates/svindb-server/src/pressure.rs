//! Короткое cgroup-окно без чтения procfs в горячих merge checkpoints.

use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};
use svindb_core::mem::Pressure;

#[derive(Clone, Copy, Debug, Default)]
pub struct Delta {
    pub memory_full_percent: Option<f64>,
    pub io_full_percent: Option<f64>,
    pub events_max: Option<u64>,
    pub direct_reclaim: Option<u64>,
    pub file_refaults: Option<u64>,
}

impl Delta {
    fn between(previous: Pressure, current: Pressure, elapsed: Duration) -> Self {
        let difference = |a: Option<u64>, b: Option<u64>| b?.checked_sub(a?);
        let percent = |a, b| {
            let us = elapsed.as_micros();
            if us == 0 {
                return None;
            }
            Some((difference(a, b)? as f64 * 100.0 / us as f64).min(100.0))
        };
        Self {
            memory_full_percent: percent(
                previous.memory_full_total_us,
                current.memory_full_total_us,
            ),
            io_full_percent: percent(previous.io_full_total_us, current.io_full_total_us),
            events_max: difference(previous.events_max, current.events_max),
            direct_reclaim: difference(previous.pgscan_direct, current.pgscan_direct),
            file_refaults: difference(
                previous.workingset_refault_file,
                current.workingset_refault_file,
            ),
        }
    }

    fn stalls(self) -> bool {
        self.memory_full_percent.is_some_and(|p| p >= 0.5)
            || self.io_full_percent.is_some_and(|p| p >= 0.5)
            || self.events_max.is_some_and(|n| n > 0)
            || self.direct_reclaim.is_some_and(|n| n > 0)
    }
}

#[derive(Clone, Copy)]
pub struct Sample {
    pub pressure: Pressure,
    pub delta: Delta,
    pub at: Instant,
}

fn published() -> &'static RwLock<Option<Sample>> {
    static SAMPLE: OnceLock<RwLock<Option<Sample>>> = OnceLock::new();
    SAMPLE.get_or_init(|| RwLock::new(None))
}

pub fn latest() -> Option<Sample> {
    *published().read().unwrap_or_else(|error| error.into_inner())
}

/// Неизвестное/просроченное окно не разрешает агрессивную фоновую работу.
pub fn background_stalled() -> bool {
    let Some(sample) = latest() else {
        return true;
    };
    sample.at.elapsed() > Duration::from_secs(1)
        || sample.pressure.io_overloaded()
        || (sample.pressure.cgroup_limited && sample.delta.stalls())
}

pub fn start() -> std::io::Result<std::thread::JoinHandle<()>> {
    // Первый снимок снимается до открытия listener, а не первым запросом.
    let first = Sample {
        pressure: svindb_core::mem::pressure(),
        delta: Delta::default(),
        at: Instant::now(),
    };
    *published().write().unwrap_or_else(|error| error.into_inner()) = Some(first);
    std::thread::Builder::new().name("svindb-pressure-sample".into()).spawn(move || {
        let mut previous = first;
        while !super::SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(250));
            let pressure = svindb_core::mem::pressure();
            let at = Instant::now();
            let next = Sample {
                pressure,
                delta: Delta::between(previous.pressure, pressure, at.duration_since(previous.at)),
                at,
            };
            // Ни procfs I/O, ни sleep не находятся под lock публикации.
            *published().write().unwrap_or_else(|error| error.into_inner()) = Some(next);
            previous = next;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_stalls_are_visible_before_long_averages_move() {
        let previous = Pressure {
            memory_full_total_us: Some(100),
            io_full_total_us: Some(1000),
            events_max: Some(42),
            pgscan_direct: Some(4),
            workingset_refault_file: Some(10),
            ..Pressure::default()
        };
        let current = Pressure {
            memory_full_total_us: Some(1350),
            io_full_total_us: Some(3500),
            events_max: Some(43),
            pgscan_direct: Some(6),
            workingset_refault_file: Some(13),
            ..previous
        };
        let delta = Delta::between(previous, current, Duration::from_millis(250));
        assert_eq!(delta.memory_full_percent, Some(0.5));
        assert_eq!(delta.io_full_percent, Some(1.0));
        assert_eq!(delta.events_max, Some(1));
        assert_eq!(delta.direct_reclaim, Some(2));
        assert_eq!(delta.file_refaults, Some(3));
        assert!(delta.stalls());
        assert!(!current.io_overloaded());
    }

    #[test]
    fn missing_reset_and_zero_window_are_not_fabricated_deltas() {
        let before =
            Pressure { io_full_total_us: Some(1000), events_max: Some(99), ..Pressure::default() };
        let after =
            Pressure { io_full_total_us: Some(1), events_max: Some(1), ..Pressure::default() };
        let delta = Delta::between(before, after, Duration::from_millis(250));
        assert_eq!(delta.io_full_percent, None);
        assert_eq!(delta.events_max, None);
        assert_eq!(delta.file_refaults, None);
        assert_eq!(Delta::between(before, before, Duration::ZERO).io_full_percent, None);
        assert_eq!(Delta::between(before, before, Duration::from_millis(250)).events_max, Some(0));
    }
}
