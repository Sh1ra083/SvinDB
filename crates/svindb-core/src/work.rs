//! Общий сторож тяжёлой работы: deadline, отмена и счёт фактически пройденного.
//!
//! Один guard принадлежит всему запросу и передаётся во все его стадии. Иначе
//! поиск мог остановиться, а hydration или агрегация продолжали занимать CPU,
//! память и permits уже после ухода клиента.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// Почему работа прекратилась.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Reason {
    Running = 0,
    Cancelled = 1,
    Deadline = 2,
}

impl Reason {
    pub fn name(self) -> &'static str {
        match self {
            Reason::Running => "running",
            Reason::Cancelled => "cancelled",
            Reason::Deadline => "deadline",
        }
    }
}

/// Cooperative cancellation/deadline одного запроса.
///
/// Проверка часов выполняется порциями: `Instant::elapsed` заметно дороже
/// атомарного чтения, поэтому tight inner loops зовут [`Guard::go`], а границы
/// файловых операций и фаз — [`Guard::check`].
#[derive(Debug)]
pub struct Guard {
    started: Instant,
    /// Ноль означает отсутствие deadline. Значение можно только уменьшать.
    limit_ms: AtomicU64,
    reason: AtomicU8,
    examined: AtomicU64,
    accepted: AtomicU64,
    /// Общий stop фона; обычные HTTP guards не требуют отдельного наблюдателя.
    cancel_flag: Option<Arc<AtomicBool>>,
}

/// Локальный счётчик прогресса одного рабочего обхода.
///
/// Документы накапливаются без общей атомарной операции; счётчик публикуется
/// порциями, а хвост дописывается при уничтожении счётчика. Пока обход ещё
/// идёт, live-диагностика отстаёт не более чем на 1023 шага на активный ticker;
/// у вложенных обходов остатки суммируются. После Drop итоговый счёт точен.
pub struct ProgressTicker<'a> {
    guard: Option<&'a Guard>,
    pending: u64,
    tick: u32,
}

impl<'a> ProgressTicker<'a> {
    pub fn new(guard: Option<&'a Guard>) -> Self {
        Self { guard, pending: 0, tick: 0 }
    }
    #[inline]
    pub fn go(&mut self) -> bool {
        self.pending += 1;
        if self.pending.is_multiple_of(Guard::STRIDE as u64) {
            if let Some(guard) = self.guard {
                guard.examined(self.pending);
            }
            self.pending = 0;
        }
        self.guard.is_none_or(|guard| guard.go(&mut self.tick))
    }
}

impl Drop for ProgressTicker<'_> {
    fn drop(&mut self) {
        if self.pending != 0 {
            if let Some(guard) = self.guard {
                guard.examined(self.pending);
            }
            self.pending = 0;
        }
    }
}

/// Крючок, которым сервер временно уступает тяжёлое обслуживание запросам.
///
/// Ядро не знает о HTTP, cgroup и очередях сервера, но merge/reclaim должны
/// иметь одну и ту же точку кооперативной уступки. Реализация устанавливается
/// сервером один раз при старте; библиотечные утилиты и тесты оставляют крючок
/// пустым и потому не получают скрытой задержки.
pub trait CompactionHook: Send + Sync {
    /// Проверить, можно ли продолжить небольшой кусок тяжёлой работы.
    /// Реализация может ненадолго уснуть, но не должна удерживать lock ядра.
    fn checkpoint(&self, logical_bytes: u64);

    /// Сигнал graceful shutdown: текущая порция обязана закончиться, но новый
    /// merge/flush уже не должен продолжать чтение или запись.
    fn should_stop(&self) -> bool {
        false
    }
}

static COMPACTION_HOOK: OnceLock<Arc<dyn CompactionHook>> = OnceLock::new();

/// Установить серверный крючок обслуживания.
///
/// Повторная установка запрещена намеренно: смешать два governor в одном
/// процессе значило бы считать одну и ту же полосу дважды.
pub fn install_compaction_hook(hook: Arc<dyn CompactionHook>) -> bool {
    COMPACTION_HOOK.set(hook).is_ok()
}

/// Точка уступки для merge/reclaim/catalog loops.
#[inline]
pub fn compaction_checkpoint(logical_bytes: u64) -> bool {
    if let Some(hook) = COMPACTION_HOOK.get() {
        hook.checkpoint(logical_bytes);
        !hook.should_stop()
    } else {
        true
    }
}

/// Дешёвый счётчик проверок для tight loops.
///
/// Проверка ограничена количеством элементов, bytes и временем между tick.
/// Один непрерываемый вызов I/O этим счётчиком не ограничивается.
#[derive(Debug)]
pub struct CompactionTicker {
    ticks: u32,
    logical_bytes: u64,
    last: Instant,
}

impl Default for CompactionTicker {
    fn default() -> Self {
        Self { ticks: 0, logical_bytes: 0, last: Instant::now() }
    }
}

impl CompactionTicker {
    pub const STRIDE: u32 = 1024;
    pub const BYTES: u64 = 4 << 20;
    pub const INTERVAL: Duration = Duration::from_millis(10);

    fn accumulate(&mut self, logical_bytes: u64, now: Instant) -> bool {
        self.ticks = self.ticks.saturating_add(1);
        self.logical_bytes = self.logical_bytes.saturating_add(logical_bytes);
        self.ticks >= Self::STRIDE
            || self.logical_bytes >= Self::BYTES
            || now.saturating_duration_since(self.last) >= Self::INTERVAL
    }

    fn reset(&mut self, now: Instant) {
        self.ticks = 0;
        self.logical_bytes = 0;
        self.last = now;
    }

    #[inline]
    pub fn tick(&mut self, logical_bytes: u64) -> bool {
        let mut allowed = true;
        if self.accumulate(logical_bytes, Instant::now()) {
            allowed = compaction_checkpoint(self.logical_bytes);
            // Пауза governor не расходует следующий CPU/I/O квант.
            self.reset(Instant::now());
        }
        allowed
    }

    #[inline]
    pub fn flush(&mut self) -> bool {
        if self.ticks != 0 {
            let allowed = compaction_checkpoint(self.logical_bytes);
            self.reset(Instant::now());
            return allowed;
        }
        true
    }
}

impl Guard {
    /// Через сколько единиц CPU-работы обязательно спрашивать deadline.
    pub const STRIDE: u32 = 1024;

    /// Новый guard с deadline от момента создания. `0` — без deadline.
    pub fn new(ms: u64) -> Guard {
        Guard {
            started: Instant::now(),
            limit_ms: AtomicU64::new(ms),
            reason: AtomicU8::new(Reason::Running as u8),
            examined: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            cancel_flag: None,
        }
    }

    /// Привязывает фоновую работу к остановке runtime без watcher-потока.
    pub fn with_cancel_flag(ms: u64, flag: Arc<AtomicBool>) -> Guard {
        Guard { cancel_flag: Some(flag), ..Guard::new(ms) }
    }

    /// Уменьшает deadline, но никогда не продлевает уже выданное время.
    pub fn tighten(&self, duration: Duration) {
        let want = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1);
        let mut seen = self.limit_ms.load(Ordering::Relaxed);
        loop {
            let next = if seen == 0 { want } else { seen.min(want) };
            if next == seen {
                return;
            }
            match self.limit_ms.compare_exchange_weak(
                seen,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => seen = actual,
            }
        }
    }

    /// Просит прекратить работу: клиент ушёл или узел завершает запрос.
    pub fn cancel(&self) {
        let _ = self.reason.compare_exchange(
            Reason::Running as u8,
            Reason::Cancelled as u8,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Быстрая проверка для tight loop.
    #[inline]
    pub fn go(&self, tick: &mut u32) -> bool {
        *tick = tick.wrapping_add(1);
        if !(*tick).is_multiple_of(Self::STRIDE) {
            return self.reason.load(Ordering::Relaxed) == Reason::Running as u8;
        }
        self.check()
    }

    /// Проверяет отмену и часы немедленно — для границ фаз и I/O chunks.
    pub fn check(&self) -> bool {
        if self.cancel_flag.as_ref().is_some_and(|flag| flag.load(Ordering::Acquire)) {
            self.cancel();
        }
        if self.reason.load(Ordering::Relaxed) != Reason::Running as u8 {
            return false;
        }
        let limit = self.limit_ms.load(Ordering::Relaxed);
        if limit > 0 && self.started.elapsed() >= Duration::from_millis(limit) {
            let _ = self.reason.compare_exchange(
                Reason::Running as u8,
                Reason::Deadline as u8,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            return false;
        }
        true
    }

    pub fn fired(&self) -> bool {
        self.reason.load(Ordering::Relaxed) != Reason::Running as u8
    }

    pub fn reason(&self) -> Reason {
        match self.reason.load(Ordering::Relaxed) {
            1 => Reason::Cancelled,
            2 => Reason::Deadline,
            _ => Reason::Running,
        }
    }

    pub fn examined(&self, n: u64) {
        self.examined.fetch_add(n, Ordering::Relaxed);
    }

    pub fn ticker(&self) -> ProgressTicker<'_> {
        ProgressTicker::new(Some(self))
    }

    pub fn accepted(&self, n: u64) {
        self.accepted.fetch_add(n, Ordering::Relaxed);
    }

    pub fn progress(&self) -> (u64, u64) {
        (self.examined.load(Ordering::Relaxed), self.accepted.load(Ordering::Relaxed))
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Сколько времени осталось до deadline.
    ///
    /// `None` означает бессрочный guard. Метод нужен планировщику ресурсов:
    /// ожидание permit является частью запроса и не должно переживать его
    /// собственный срок только потому, что очередь проверяется отдельным
    /// компонентом.
    pub fn remaining(&self) -> Option<Duration> {
        let limit = self.limit_ms.load(Ordering::Relaxed);
        (limit > 0).then(|| Duration::from_millis(limit).saturating_sub(self.started.elapsed()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_stop_is_observed_without_watcher_thread() {
        let stop = Arc::new(AtomicBool::new(false));
        let guard = Guard::with_cancel_flag(0, Arc::clone(&stop));
        assert!(guard.check());
        stop.store(true, Ordering::Release);
        assert!(!guard.check());
        assert_eq!(guard.reason(), Reason::Cancelled);
        assert!(guard.fired());
        // Снятие чужого флага не возобновляет уже отменённую работу.
        stop.store(false, Ordering::Release);
        assert!(!guard.check());
    }

    #[test]
    fn compaction_checks_bytes_time_and_zero_byte_work_independently() {
        let mut ticker = CompactionTicker::default();
        let start = ticker.last;
        assert!(!ticker.accumulate(CompactionTicker::BYTES - 1, start));
        assert!(ticker.accumulate(1, start));
        ticker.reset(start);
        assert!(!ticker.accumulate(0, start + Duration::from_millis(9)));
        assert!(ticker.accumulate(0, start + Duration::from_millis(10)));
        ticker.reset(start);
        for _ in 1..CompactionTicker::STRIDE {
            assert!(!ticker.accumulate(0, start));
        }
        assert!(ticker.accumulate(0, start));
        ticker.flush();
        assert_eq!(ticker.ticks, 0);
        assert_eq!(ticker.logical_bytes, 0);
    }

    #[test]
    fn deadline_cancel_and_progress_belong_to_one_guard() {
        let guard = Guard::new(60_000);
        guard.examined(10);
        guard.accepted(3);
        assert_eq!(guard.progress(), (10, 3));
        assert!(guard.check());
        guard.cancel();
        assert!(!guard.check());
        assert_eq!(guard.reason(), Reason::Cancelled);
    }

    #[test]
    fn tightening_never_extends_the_deadline() {
        let guard = Guard::new(50);
        guard.tighten(Duration::from_secs(1));
        std::thread::sleep(Duration::from_millis(60));
        assert!(!guard.check());
        assert_eq!(guard.reason(), Reason::Deadline);
    }

    #[test]
    fn a_tightened_unlimited_guard_expires() {
        let guard = Guard::new(0);
        guard.tighten(Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!guard.check());
    }

    #[test]
    fn progress_ticker_flushes_stride_and_tail_exactly() {
        let guard = Guard::new(60_000);
        {
            let mut ticker = guard.ticker();
            for _ in 0..(Guard::STRIDE + 7) {
                assert!(ticker.go());
            }
            assert_eq!(guard.progress().0, Guard::STRIDE as u64);
        }
        assert_eq!(guard.progress().0, Guard::STRIDE as u64 + 7);
    }

    #[test]
    fn progress_ticker_flushes_on_cancelled_early_return() {
        let guard = Guard::new(60_000);
        {
            let mut ticker = guard.ticker();
            for _ in 0..3 {
                assert!(ticker.go());
            }
            guard.cancel();
            assert!(!ticker.go());
        }
        assert_eq!(guard.progress().0, 4);
    }

    #[test]
    fn progress_ticker_flushes_when_unwinding() {
        let guard = Guard::new(60_000);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let guard = &guard;
            move || {
                let mut ticker = guard.ticker();
                for _ in 0..5 {
                    assert!(ticker.go());
                }
                panic!("проверка RAII");
            }
        }));
        assert!(result.is_err());
        assert_eq!(guard.progress().0, 5);
    }

    #[test]
    fn progress_ticker_counts_concurrent_workers_exactly() {
        let guard = std::sync::Arc::new(Guard::new(60_000));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let guard = std::sync::Arc::clone(&guard);
                scope.spawn(move || {
                    let mut ticker = guard.ticker();
                    for _ in 0..(Guard::STRIDE as usize + 11) {
                        assert!(ticker.go());
                    }
                });
            }
        });
        assert_eq!(guard.progress().0, 4 * (Guard::STRIDE as u64 + 11));
    }

    #[test]
    fn progress_ticker_without_guard_is_a_noop() {
        let mut ticker = ProgressTicker::new(None);
        for _ in 0..(Guard::STRIDE + 3) {
            assert!(ticker.go());
        }
    }
}
