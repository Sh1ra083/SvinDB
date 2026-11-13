//! Счётчики и гистограммы узла — только механизм, без общего состояния.
//!
//! ## Чем это отличается от [`crate::stats`]
//!
//! `stats` считает **работу одного запроса** и живёт в нити потока: обход
//! обнуляет счётчики перед запросом и снимает их после, чтобы сказать в ответе,
//! сколько курсор сделал шагов. Величина там короткоживущая и принадлежит
//! запросу.
//!
//! Здесь — величины **узла**: сколько запросов было всего, каких видов, с
//! какими кодами и за какое время. Они живут, пока живёт процесс, растут из
//! всех потоков сразу и никогда не обнуляются. Смешивать эти два вида было бы
//! ошибкой: снимок с обнулением у общеузлового счётчика означает, что двое
//! спрашивающих видят разные числа, а третий не видит ничего.
//!
//! ## Почему гистограмма, а не среднее
//!
//! Среднее время ответа — величина, по которой нельзя принять ни одного
//! решения. Узел, отвечающий девяноста девяти запросам за миллисекунду и одному
//! за минуту, покажет среднее в шестьсот миллисекунд, которого не было ни у
//! одного запроса. Нужны квантили, а для них — распределение.
//!
//! ## Почему корзины постоянные, а не считаются по данным
//!
//! Границы заданы таблицей и не меняются никогда. Это позволяет `observe`
//! обходиться без единого выделения памяти и без замка: линейный проход по
//! пятнадцати сравнениям и `fetch_add` в готовую ячейку. Измерение обязано
//! стоить меньше того, что оно измеряет.

use std::sync::atomic::{AtomicU64, Ordering};

/// Границы корзин в миллисекундах и их подписи.
///
/// Подпись хранится строкой рядом с числом, а не печатается из него: у
/// Prometheus значение `le` обязано совпадать текстуально между выдачами, иначе
/// он считает это разными рядами. Печать `f64` такого не обещает.
const EDGES: &[(f64, &str)] = &[
    (0.1, "0.1"),
    (0.25, "0.25"),
    (0.5, "0.5"),
    (1.0, "1"),
    (2.5, "2.5"),
    (5.0, "5"),
    (10.0, "10"),
    (25.0, "25"),
    (50.0, "50"),
    (100.0, "100"),
    (250.0, "250"),
    (500.0, "500"),
    (1000.0, "1000"),
    (2500.0, "2500"),
    (10000.0, "10000"),
];

/// Сколько корзин: по границе на каждую плюс «всё остальное».
pub const BUCKETS: usize = EDGES.len() + 1;

/// Распределение времён. Складывается из всех потоков сразу.
pub struct Hist {
    counts: [AtomicU64; BUCKETS],
    /// Сумма — в целых **микросекундах**, а не в битах `f64`.
    ///
    /// Атомарного сложения для `f64` нет; складывать биты через
    /// сравнение-с-обменом можно, но это цикл на горячем пути ради величины,
    /// которая и так приблизительная. Микросекунда — достаточная точность:
    /// на миллиарде запросов по секунде переполнения `u64` не случится и через
    /// полмиллиона лет.
    sum_us: AtomicU64,
}

impl Default for Hist {
    fn default() -> Self {
        Hist::new()
    }
}

impl Hist {
    pub const fn new() -> Hist {
        // `AtomicU64::new(0)` не `Copy`, поэтому массив выписан циклом в
        // константном контексте — иначе пришлось бы заводить его лениво.
        #[allow(clippy::declare_interior_mutable_const)]
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Hist { counts: [ZERO; BUCKETS], sum_us: AtomicU64::new(0) }
    }

    /// Записывает время. Ни одного выделения памяти и ни одного замка.
    pub fn observe(&self, ms: f64) {
        let at = EDGES.iter().position(|(b, _)| ms <= *b).unwrap_or(EDGES.len());
        self.counts[at].fetch_add(1, Ordering::Relaxed);
        // Отрицательное и не-число сюда попасть не должны, но если попадут —
        // пусть прибавят ноль, а не обернутся в огромное число.
        let us = if ms.is_finite() && ms > 0.0 { (ms * 1000.0) as u64 } else { 0 };
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    /// Сколько всего записано.
    pub fn count(&self) -> u64 {
        self.counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// Печатает корзины, сумму и счёт в текстовом виде Prometheus.
    ///
    /// Накопленные суммы считаются **при обходе**, а не хранятся. Каждая
    /// корзина по отдельности только растёт, поэтому сумма слева направо не
    /// убывает при любом чтении вразнобой — а Prometheus от `_bucket` именно
    /// этого и требует. Храни мы накопленные, одновременная запись в две
    /// корзины дала бы убывающий ряд.
    pub fn write(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        let mut acc = 0u64;
        for (i, (_, edge)) in EDGES.iter().enumerate() {
            acc += self.counts[i].load(Ordering::Relaxed);
            out.push_str(&format!("{name}_bucket{{{labels}{sep}le=\"{edge}\"}} {acc}\n"));
        }
        acc += self.counts[EDGES.len()].load(Ordering::Relaxed);
        out.push_str(&format!("{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {acc}\n"));
        let sum_ms = self.sum_us.load(Ordering::Relaxed) as f64 / 1000.0;
        out.push_str(&format!("{name}_sum{{{labels}}} {sum_ms:.3}\n"));
        out.push_str(&format!("{name}_count{{{labels}}} {acc}\n"));
    }
}

/// Простой растущий счётчик.
#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Counter {
        Counter(AtomicU64::new(0))
    }

    #[inline]
    pub fn add(&self, by: u64) {
        self.0.fetch_add(by, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Заголовок ряда в текстовом виде Prometheus.
pub fn header(out: &mut String, name: &str, help: &str, kind: &str) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
}

/// Одно число с подписями.
pub fn gauge(out: &mut String, name: &str, value: u64) {
    out.push_str(&format!("{name} {value}\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Накопленные суммы не убывают, и хвост совпадает со счётом.
    ///
    /// Это не украшение формата: Prometheus по убывающему `_bucket` считает ряд
    /// испорченным и выбрасывает всю выдачу целиком, а не одну строку.
    #[test]
    fn buckets_only_grow_and_the_tail_is_the_count() {
        let h = Hist::new();
        for ms in [0.05, 0.3, 0.7, 3.0, 40.0, 900.0, 99999.0] {
            h.observe(ms);
        }
        let mut out = String::new();
        h.write(&mut out, "проба", "вид=\"поиск\"");

        let nums: Vec<u64> = out
            .lines()
            .filter(|l| l.contains("_bucket"))
            .map(|l| l.rsplit(' ').next().expect("число").parse().expect("целое"))
            .collect();
        assert_eq!(nums.len(), BUCKETS, "корзина на границу плюс хвост");
        assert!(nums.windows(2).all(|w| w[0] <= w[1]), "ряд обязан не убывать: {nums:?}");
        assert_eq!(*nums.last().expect("хвост"), 7, "в хвосте — все записанные");
        assert_eq!(h.count(), 7);
        assert!(out.contains("проба_count{вид=\"поиск\"} 7"), "{out}");
    }

    /// Величина за последней границей попадает в хвост, а не теряется.
    #[test]
    fn what_is_beyond_the_last_edge_is_not_lost() {
        let h = Hist::new();
        h.observe(1e9);
        assert_eq!(h.count(), 1);
        let mut out = String::new();
        h.write(&mut out, "п", "");
        assert!(out.contains("п_bucket{le=\"+Inf\"} 1"), "{out}");
        assert!(out.contains("п_bucket{le=\"10000\"} 0"), "{out}");
    }

    /// Не-число и отрицательное не портят сумму.
    ///
    /// Время меряется вычитанием мгновений, и на переводе часов назад разность
    /// бывает отрицательной. Обернувшаяся сумма врала бы не на проценты.
    #[test]
    fn a_nonsense_measurement_does_not_wreck_the_sum() {
        let h = Hist::new();
        h.observe(f64::NAN);
        h.observe(-5.0);
        h.observe(f64::INFINITY);
        h.observe(2.0);
        let mut out = String::new();
        h.write(&mut out, "п", "");
        assert!(out.contains("п_sum{} 2.000"), "{out}");
        assert_eq!(h.count(), 4, "записаны все, но в сумму пошло только годное");
    }

    /// Счёт складывается из всех потоков, а не теряется.
    #[test]
    fn threads_add_up() {
        let h = Hist::new();
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        h.observe(1.0);
                    }
                });
            }
        });
        assert_eq!(h.count(), 8000);
    }
}
