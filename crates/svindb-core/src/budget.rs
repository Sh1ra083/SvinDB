//! Размыкатель по памяти: отказ вместо падения узла.
//!
//! ## Зачем
//!
//! Запрос из тридцати байт умеет попросить гигабайты. Не потому, что кто-то
//! злоумышляет, а потому, что цена запроса зависит не только от него: рабочая
//! память растёт с **числом сегментов**, которого тот, кто спрашивает, не видит.
//! На четырёх тысячах сегментов один запрос заводит четыре тысячи разобранных
//! запросов — по одному на сегмент, — и это уже мегабайты на каждый поиск.
//!
//! Дальше становится хуже: агрегаций разрешено тридцать две, у каждой до
//! [`crate::aggs::MAX_BUCKETS`] корзин, и корзины эти не в отображении, а в
//! куче. Верхушка на десять тысяч попаданий — тоже куча.
//!
//! Узел, который принял такой запрос и умер по памяти, теряет **все** остальные
//! запросы, а не только этот. Поэтому правильный ответ — отказ с причиной, и
//! ровно так же поступает Elasticsearch со своим circuit breaker.
//!
//! ## Почему оценка, а не учёт по факту
//!
//! Учитывать по факту значит останавливаться посреди работы: часть памяти уже
//! взята, часть ответа уже посчитана, и отдавать нечего — ни ответа, ни
//! внятного отказа. Оценка **до** начала даёт отказ, у которого есть причина и
//! число, и не отнимает памяти у соседних запросов.
//!
//! Оценка нарочно грубая и **сверху**: точность здесь не нужна, нужна граница.

/// Общий на весь узел счёт памяти запросов.
///
/// ## Зачем, если у запроса уже есть предел
///
/// Предел у запроса был, и он верен: `memory_budget_mb` не даёт одному запросу
/// вырасти сверх меры. Но он ничего не говорит о **сумме**. Рабочих потоков у
/// узла до двухсот пятидесяти шести, и каждый вправе взять свои двести
/// пятьдесят шесть мегабайт: шестьдесят четыре гигабайта законной одновременной
/// нагрузки, при которой ни один запрос не нарушил ни одного правила, а узел
/// снят ядром.
///
/// Здесь считается занятое всеми сразу. Отказ — ограниченный `429`, а не смерть
/// процесса: отказавший запрос повторят, а убитый узел потеряет и все остальные.
///
/// ## Почему счёт, а не ожидание
///
/// Ждать освобождения нельзя: ждущий поток держит свои уже взятые обязательства,
/// и очередь из таких ожидающих сходится в тупик. Отказ немедленный.
#[derive(Debug)]
pub struct Pool {
    used: std::sync::atomic::AtomicUsize,
    max: usize,
}

/// Место в общем счёте. Освобождается на любом пути, включая ранний отказ.
#[derive(Debug)]
pub struct Permit {
    pool: std::sync::Arc<Pool>,
    bytes: usize,
}

/// Отказ по общей памяти: сколько просили, сколько занято, каков предел.
#[derive(Debug)]
pub struct Full {
    pub want: usize,
    pub used: usize,
    pub max: usize,
}

impl Pool {
    pub fn new(max: usize) -> Pool {
        Pool { used: std::sync::atomic::AtomicUsize::new(0), max }
    }

    /// Занимает место. `Err` — сейчас столько не дадут.
    ///
    /// Нулевой предел означает «не считать»: так узел ведёт себя ровно как
    /// прежде, и включение счёта остаётся решением оператора, а не сюрпризом
    /// при обновлении.
    pub fn reserve(self: &std::sync::Arc<Self>, bytes: usize) -> Result<Permit, Full> {
        use std::sync::atomic::Ordering;
        if self.max == 0 {
            return Ok(Permit { pool: std::sync::Arc::clone(self), bytes: 0 });
        }
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let next = used.saturating_add(bytes);
            if next > self.max {
                return Err(Full { want: bytes, used, max: self.max });
            }
            match self.used.compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return Ok(Permit { pool: std::sync::Arc::clone(self), bytes }),
                Err(now) => used = now,
            }
        }
    }

    /// Сколько занято прямо сейчас — для наблюдаемости.
    pub fn used(&self) -> usize {
        self.used.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn max(&self) -> usize {
        self.max
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.pool.used.fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use std::sync::Arc;

    /// Сумма одновременных запросов ограничена, а не только каждый по себе.
    #[test]
    fn the_sum_is_bounded_not_only_each_request() {
        let pool = Arc::new(Pool::new(1000));
        let a = pool.reserve(600).expect("первый");
        let b = pool.reserve(300).expect("второй");
        let full = pool.reserve(200).expect_err("третий обязан не влезть");
        assert_eq!((full.want, full.max), (200, 1000));
        assert_eq!(pool.used(), 900);
        drop(b);
        assert_eq!(pool.used(), 600);
        // Освободилось — снова можно.
        let c = pool.reserve(200).expect("после освобождения");
        drop((a, c));
        assert_eq!(pool.used(), 0, "место не вернулось");
    }

    /// Место возвращается и на пути отказа, а не только на успешном.
    #[test]
    fn a_place_returns_even_when_the_request_fails() {
        let pool = Arc::new(Pool::new(100));
        {
            let _p = pool.reserve(50).expect("взято");
            assert!(pool.reserve(60).is_err());
        }
        assert_eq!(pool.used(), 0);
    }

    /// Нулевой предел означает «не считать»: поведение ровно прежнее.
    #[test]
    fn a_zero_limit_means_no_accounting_at_all() {
        let pool = Arc::new(Pool::new(0));
        let _a = pool.reserve(usize::MAX / 2).expect("без предела берут всё");
        assert_eq!(pool.used(), 0);
    }
}

/// Что запрос собирается сделать — в тех величинах, от которых зависит память.
#[derive(Clone, Copy, Debug)]
pub struct Ask {
    /// Сколько сегментов придётся обойти.
    pub segments: usize,
    /// Сколько попаданий вернуть.
    pub k: usize,
    /// Сколько слов и ветвей в запросе после раскрытия.
    pub clauses: usize,
    /// Сколько агрегаций.
    pub aggs: usize,
    /// Оценка байт hydration и сериализованного ответа.
    pub response_bytes: usize,
}

/// Разобранный запрос на сегмент: `Request` со своими векторами.
///
/// Не `size_of::<Request>()`: считается вместе с векторами слов, ветвей и
/// отбора, которые он держит. Полторы сотни байт на ветвь — замеренная сверху
/// оценка `QueryWord` с его списками.
const PER_CLAUSE: usize = 160;
/// Постоянная часть разобранного запроса на сегмент.
const PER_SEGMENT: usize = 512;
/// Одно попадание в верхушке: `Hit` с ключами сортировки.
const PER_HIT: usize = {
    let local = std::mem::size_of::<crate::search::Hit>();
    let merged = std::mem::size_of::<crate::segments::Hit>();
    if local > merged {
        local
    } else {
        merged
    }
};
/// Одна корзина агрегации: ключ и счётчик в отображении, с запасом на его
/// заполнение.
const PER_BUCKET: usize = 48;

impl Ask {
    fn base_bytes(&self, top_copies: usize) -> usize {
        // Складывать тоже надо с насыщением: числа приходят из запроса, а
        // обычный плюс на `usize::MAX` — паника в отладке и обёртка в выпуске,
        // то есть самый дорогой запрос прошёл бы как дешёвый.
        let per_seg = PER_SEGMENT.saturating_add(self.clauses.saturating_mul(PER_CLAUSE));
        let plan = self.segments.saturating_mul(per_seg);
        let top = if self.k == 0 {
            0
        } else {
            self.k.saturating_add(1).saturating_mul(PER_HIT).saturating_mul(top_copies)
        };
        plan.saturating_add(top).saturating_add(self.response_bytes)
    }

    /// Историческая оценка без сведений об executor. Для runtime admission
    /// параллельного поиска нужен [`Ask::bytes_with_execution`]: две верхушки
    /// не являются верхней границей нескольких одновременно работающих lanes.
    pub fn bytes(&self) -> usize {
        let aggs = self.aggs.saturating_mul(crate::aggs::MAX_BUCKETS).saturating_mul(PER_BUCKET);
        self.bytes_with_aggregation(aggs)
    }

    /// Сколько памяти запрос возьмёт с учётом конкретного плана агрегаций.
    ///
    /// `bytes` сохраняет консервативную совместимость для вызывающих, которые
    /// знают только число агрегаций. Сервер после разбора запроса знает форму
    /// каждого collector и вызывает этот метод: scalar count получает размер
    /// фиксированного аккумулятора, а bucket/cardinality — свой фактический
    /// верхний предел.
    pub fn bytes_with_aggregation(&self, aggregation_bytes: usize) -> usize {
        self.base_bytes(2).saturating_add(aggregation_bytes)
    }

    /// Реальный executor держит локальные heaps, lane-results и merge buffer
    /// одновременно. Vec::truncate не уменьшает capacity, а extend может
    /// удвоить её. Резерв учитывает эти ёмкости и фактически выданную ширину,
    /// а не предполагает две верхушки независимо от числа работников.
    pub fn bytes_with_execution(&self, aggregation_bytes: usize, workers: usize) -> usize {
        let copies = workers.max(1).saturating_mul(6).saturating_add(4);
        self.base_bytes(copies).saturating_add(aggregation_bytes)
    }

    /// Влезает ли в бюджет. `Err` — отказ с причиной и числами.
    ///
    /// Ноль означает «без предела»: узел, которому бюджет не задан, обязан
    /// работать как раньше, а не отказывать всему.
    pub fn check(&self, budget_bytes: usize) -> Result<(), String> {
        self.check_with_aggregation(
            budget_bytes,
            self.aggs.saturating_mul(crate::aggs::MAX_BUCKETS).saturating_mul(PER_BUCKET),
        )
    }

    /// Проверка с оценкой агрегаций, полученной из их типов.
    pub fn check_with_aggregation(
        &self,
        budget_bytes: usize,
        aggregation_bytes: usize,
    ) -> Result<(), String> {
        self.check_estimate(
            budget_bytes,
            aggregation_bytes,
            self.bytes_with_aggregation(aggregation_bytes),
        )
    }

    pub fn check_with_execution(
        &self,
        budget_bytes: usize,
        aggregation_bytes: usize,
        workers: usize,
    ) -> Result<(), String> {
        self.check_estimate(
            budget_bytes,
            aggregation_bytes,
            self.bytes_with_execution(aggregation_bytes, workers),
        )
    }

    fn check_estimate(
        &self,
        budget_bytes: usize,
        aggregation_bytes: usize,
        want: usize,
    ) -> Result<(), String> {
        if budget_bytes == 0 {
            return Ok(());
        }
        if want <= budget_bytes {
            return Ok(());
        }
        Err(format!(
            "запрос попросил бы около {} МиБ рабочей памяти при пределе {} МиБ \
             (сегментов {}, ветвей {}, верхушка {}, агрегаций {}, память агрегаций {} КиБ). \
             Уменьшите k, число агрегаций или поднимите memory_budget_mb",
            want / (1 << 20),
            budget_bytes / (1 << 20),
            self.segments,
            self.clauses,
            self.k,
            self.aggs,
            aggregation_bytes / 1024,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_width_and_retained_vector_capacity_enter_the_reservation() {
        let ask = Ask { segments: 20, k: 10_000, clauses: 2, aggs: 1, response_bytes: 128 };
        let one = ask.bytes_with_execution(256, 1);
        let eight = ask.bytes_with_execution(256, 8);
        assert!(eight > one);
        assert!(eight >= (8 * 6 + 4) * (ask.k + 1) * PER_HIT);
        assert!(ask.check_with_execution(one, 256, 8).is_err());
        assert!(ask.check_with_execution(eight, 256, 8).is_ok());
        let count = Ask { k: 0, ..ask };
        assert_eq!(count.bytes_with_execution(256, 1), count.bytes_with_execution(256, 8));
        assert_eq!(ask.bytes_with_execution(256, usize::MAX), usize::MAX);
    }

    fn ask(segments: usize, k: usize, clauses: usize, aggs: usize) -> Ask {
        Ask { segments, k, clauses, aggs, response_bytes: 0 }
    }

    /// Обычный запрос проходит при любом разумном бюджете.
    #[test]
    fn an_ordinary_query_passes() {
        let a = ask(100, 10, 2, 0);
        assert!(a.bytes() < 1 << 20, "обычный запрос оценён в {} байт", a.bytes());
        a.check(64 << 20).expect("обычный запрос отвергнут");
    }

    /// Оценка растёт со всеми четырьмя величинами, а не только с одной.
    ///
    /// Проверяется каждая по отдельности: пропусти одну — и запрос, дорогой
    /// ровно ею, прошёл бы сквозь размыкатель.
    #[test]
    fn every_dimension_raises_the_estimate() {
        let base = ask(100, 10, 2, 0).bytes();
        assert!(ask(1000, 10, 2, 0).bytes() > base, "сегменты не учтены");
        assert!(ask(100, 10_000, 2, 0).bytes() > base, "верхушка не учтена");
        assert!(ask(100, 10, 50, 0).bytes() > base, "ветви не учтены");
        assert!(ask(100, 10, 2, 1).bytes() > base, "агрегации не учтены");
    }

    #[test]
    fn a_type_aware_aggregation_estimate_can_admit_scalar_count() {
        let ask = ask(100, 10, 2, 1);
        let conservative = ask.bytes();
        let scalar = ask.bytes_with_aggregation(crate::aggs::memory_hint(&[crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }]));
        assert!(scalar < conservative, "scalar count всё ещё оценён как MAX_BUCKETS");
        assert!(scalar < 1 << 20, "план одного scalar count неожиданно велик: {scalar}");
    }

    /// Дорогой запрос отвергается, и отказ говорит, что именно уменьшать.
    #[test]
    fn an_expensive_query_is_refused_with_numbers() {
        // Тридцать две агрегации по 65 536 корзин — почти сотня мегабайт из
        // запроса в тридцать байт. Умножьте на число одновременных запросов, и
        // это уже вся память узла.
        let a = ask(100, 10, 2, 32);
        assert!(a.bytes() > 64 << 20, "оценка {} байт, ожидались десятки МиБ", a.bytes());
        let e = a.check(32 << 20).expect_err("дорогой запрос принят");
        for want in ["МиБ", "агрегаций", "memory_budget_mb"] {
            assert!(e.contains(want), "отказ не содержит {want:?}: {e}");
        }
    }

    /// Ноль — без предела: узел без бюджета работает как раньше.
    #[test]
    fn zero_means_no_limit() {
        ask(100_000, 10_000, 1000, 32).check(0).expect("нулевой бюджет обязан пропускать всё");
    }

    /// Переполнение при умножении не даёт паники и не проходит как «мало».
    ///
    /// Числа приходят из запроса, а `usize::MAX` в произведении обернулся бы в
    /// маленькое значение — то есть самый дорогой запрос прошёл бы как дешёвый.
    #[test]
    fn absurd_numbers_saturate_instead_of_wrapping() {
        let a = ask(usize::MAX, usize::MAX, usize::MAX, usize::MAX);
        assert_eq!(a.bytes(), usize::MAX, "произведение обернулось");
        assert!(a.check(1 << 30).is_err(), "обёрнутая оценка прошла как дешёвая");
    }
}
