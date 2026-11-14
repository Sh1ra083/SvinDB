//! Агрегации: считалки поверх колонки значений.

use std::collections::{hash_map::RandomState, HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};

use crate::index::Index;

/// Над чем считать: над документами или над элементами массива.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Over {
    /// Наблюдение — документ. Значение берётся из колонки по его номеру.
    Docs,
    /// Наблюдение — элемент массива. Значения берутся по всем ординалам
    /// документа, то есть один документ даёт столько наблюдений, сколько у него
    /// элементов.
    Slots { group: u32 },
}

/// Что считать.
#[derive(Clone, Debug)]
pub enum Kind {
    /// Число наблюдений, наименьшее, наибольшее, сумма и среднее — одним проходом.
    ///
    /// `field` = `None` — только счёт: столько наблюдений, сколько их есть.
    Stats { field: Option<usize> },
    /// Равномерная сетка: ключ корзины — `offset + interval·⌊(v − offset)/interval⌋`.
    ///
    /// Правило задаётся запросом, поэтому корзины совпадают у всех сегментов и
    /// у всех шардов — и складываются точно, а не приближённо.
    Histogram { field: usize, interval: i64, offset: i64, step: Step },
    /// Заданные диапазоны `[from, to)`. `None` снимает верхнюю границу,
    /// включая максимальное значение колонки.
    Range { field: usize, edges: Vec<(i64, Option<i64>)> },
    /// Верхушка значений по числу наблюдений.
    ///
    /// Для ключевого слова значением служит номер списка в словаре: он уже
    /// посчитан при сборке сегмента. Это то, что в Elasticsearch зовётся
    /// глобальными ординалами, только строить их не нужно.
    Terms { field: usize, size: usize },
    /// Приближённое число различных значений. HyperLogLog, 2^14 регистров.
    Cardinality { field: usize },
    /// Точное число различных значений. При превышении memory-safe потолка
    /// запрос явно отвергается, а не молча превращается в приблизительный.
    CardinalityExact { field: usize },
}

/// Одна запрошенная агрегация.
#[derive(Clone, Debug)]
pub struct Agg {
    pub name: String,
    pub kind: Kind,
    pub over: Over,
    /// В каком пространстве лежат значения колонки этого поля.
    ///
    /// Колонка хранит `i64` и сравнивается со знаком, поэтому дробное значение
    /// кладётся в неё **ключом** — монотонным преобразованием бит (см.
    /// [`crate::index::float_key`]). Порядок ключ сохраняет, а сложение — нет:
    /// сумма ключей не значит ничего. Пока агрегации о типе поля не знали, они
    /// и складывали ключи: `sum` по значениям 0.25, 0.5, 0.75 и 0.937421
    /// выходил девятнадцатизначным числом, и выходил молча.
    pub space: Space,
}

/// Только число документов, без value count и без счёта nested slots.
pub fn document_counts_only(aggs: &[Agg]) -> bool {
    !aggs.is_empty()
        && aggs.iter().all(|agg| {
            matches!(agg.kind, Kind::Stats { field: None }) && matches!(agg.over, Over::Docs)
        })
}

/// Шаг равномерной сетки.
///
/// Отдельным перечислением, а не «то же `i64`, но означает другое»: ровно эта
/// двузначность и породила прежнюю ошибку. Ведро в пространстве ключей — не
/// ведро: ключи дробных чисел не равномерны, и шаг по ним не значит ничего.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Step {
    /// Целое поле: шаг и смещение — числа.
    Int { interval: i64, offset: i64 },
    /// Дробное поле: шаг и смещение — тоже числа, но корзина считается по
    /// значению, а ключ корзины кладётся обратно в пространство ключей, чтобы
    /// сегменты складывались по одному и тому же ключу.
    Float { interval: f64, offset: f64 },
    /// Календарный шаг по метке времени в миллисекундах.
    ///
    /// Отдельно от [`Step::Int`], потому что месяц и год — не число
    /// миллисекунд: в них разное число дней, и равномерная сетка дала бы
    /// корзины, съезжающие с начала месяца. Сутки и неделя постоянны, но
    /// считаются здесь же, чтобы правило было одно.
    Calendar { unit: Unit, tz_ms: i64 },
}

/// Календарная единица.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Unit {
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    pub fn parse(word: &str) -> Option<Unit> {
        Some(match word {
            "1d" | "day" => Unit::Day,
            "1w" | "week" => Unit::Week,
            "1M" | "month" => Unit::Month,
            "1q" | "quarter" => Unit::Quarter,
            "1y" | "year" => Unit::Year,
            _ => return None,
        })
    }

    /// Начало корзины, в которую попадает мгновение.
    pub fn floor(self, millis: i64) -> i64 {
        let days = millis.div_euclid(86_400_000);
        let (y, m, _) = crate::iso::civil_from_days(days);
        let start = match self {
            Unit::Day => days,
            // Неделя считается от понедельника: 1970-01-01 был четвергом,
            // отсюда сдвиг на три дня.
            Unit::Week => days - (days + 3).rem_euclid(7),
            Unit::Month => crate::iso::days_from_civil(y, m, 1),
            Unit::Quarter => crate::iso::days_from_civil(y, (m - 1) / 3 * 3 + 1, 1),
            Unit::Year => crate::iso::days_from_civil(y, 1, 1),
        };
        start * 86_400_000
    }
}

/// Пространство, в котором лежат значения колонки.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Space {
    /// Целое: значение колонки и есть число.
    #[default]
    Int,
    /// Дробное: значение колонки — ключ, число получается [`crate::index::float_of`].
    Float,
    /// Ключевое слово: значение колонки — номер списка в словаре сегмента.
    Keyword,
}

/// Ключ верхушки: число у числового поля, строка у ключевого слова.
///
/// ## Почему у ключевого слова не номер
///
/// Внутри сегмента ключевое слово хранится номером списка в словаре, и по нему
/// же считается верхушка: номер уже посчитан при сборке, сравнивать целые
/// дешевле, чем строки. Но номер — это позиция слова в словаре **своего**
/// сегмента, и у соседнего сегмента та же позиция означает другую строку.
///
/// Пока номера складывались напрямую, счётчики несвязанных значений
/// суммировались, а печать разрешала ключ через словарь **нулевого** сегмента —
/// то есть подписывала сумму чужим именем. Замерено на трёх сегментах с разными
/// наборами цветов: «синий» и «алый» по 150 наблюдений превращались в 250 и 50.
///
/// Поэтому номер живёт ровно до конца своего сегмента: [`Sink::seal`] переводит
/// его в строку, и наружу выходит уже она. Слияние идёт по строкам, а это
/// единственное, что означает одно и то же во всех сегментах — и, когда дойдёт
/// до шардов, во всех шардах.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TermKey {
    Num(i64),
    Text(String),
}

impl TermKey {
    /// Число, если ключ числовой. Для строки — `None`.
    pub fn as_num(&self) -> Option<i64> {
        match self {
            TermKey::Num(v) => Some(*v),
            TermKey::Text(_) => None,
        }
    }

    /// Строка, если ключ строковый.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            TermKey::Num(_) => None,
            TermKey::Text(s) => Some(s),
        }
    }
}

/// Итог одной агрегации.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Stats {
        count: u64,
        min: Option<i64>,
        max: Option<i64>,
        sum: i128,
        /// Сумма дробного поля. `None` — поле целое, и сумма в `sum`.
        ///
        /// Порознь, а не одним числом: сумма ключей не переводится в сумму
        /// значений никаким преобразованием, и хранить их одним полем значило
        /// бы, что одно и то же число читают двумя способами.
        fsum: Option<f64>,
    },
    /// Корзины: ключ и число наблюдений.
    Buckets(Vec<(i64, u64)>),
    /// Верхушка значений: ключ, число наблюдений.
    ///
    /// `error` — граница ошибки счёта, `other` — сколько наблюдений осталось за
    /// верхушкой. Оба обязательны в ответе: приближённость, о которой не
    /// сказано, — это неверный ответ.
    Terms {
        top: Vec<(TermKey, u64)>,
        error: u64,
        other: u64,
    },
    Cardinality {
        value: u64,
    },
    ExactCardinality {
        value: u64,
    },
    /// Корзин вышло больше, чем позволено.
    TooMany {
        limit: usize,
    },
}

/// Результат с именем, как его просили.
#[derive(Clone, Debug, PartialEq)]
pub struct Result_ {
    pub name: String,
    pub value: Value,
    /// Пространство значений — то же, что у запрошенной агрегации.
    ///
    /// Едет вместе с итогом, потому что перевести ключ обратно в число может
    /// только тот, кто знает, ключ ли это. Восстанавливать пространство на краю
    /// по имени поля значило бы завести второй источник правды.
    pub space: Space,
}

/// Потолок числа корзин у одной агрегации.
///
/// Гистограмма с шагом в единицу по метке времени за пять лет даёт полтораста
/// миллионов корзин: запрос из тридцати байт съедает память узла. У
/// Elasticsearch на это стоит `search.max_buckets`, и по умолчанию столько же.
///
/// Отказ, а не молчаливое усечение: обрезанная гистограмма выглядит как
/// настоящая, и по ней принимают решения.
pub const MAX_BUCKETS: usize = 65_536;
/// Потолок явно перечисленных диапазонов одной агрегации.
pub const MAX_RANGES: usize = 256;

/// Консервативная оценка памяти одного набора накопителей.
///
/// Оценка нужна admission до начала обхода. Раньше вызывающий код умножал
/// число агрегаций на `MAX_BUCKETS`, поэтому обычный scalar `count` занимал в
/// расчёте столько же места, сколько самая широкая terms/histogram агрегация.
/// Это не влияло на результат, но без причины переводило несколько обычных
/// разделов в analytics refusal. Здесь размер зависит от состояния, которое
/// действительно может расти у конкретного collector. Оценка остаётся сверху:
/// HashMap/allocator overhead покрывается запасом на bucket.
pub fn memory_hint(aggs: &[Agg]) -> usize {
    const BASE: usize = 256;
    const BUCKET: usize = 64;
    const DEFERRED_HLL: usize = 2 * 1024 * 1024;
    aggs.iter()
        .map(|agg| {
            let units = match &agg.kind {
                // Счётчик, min/max и сумма — фиксированное состояние.
                Kind::Stats { .. } => 1,
                // До ограничения MAX_BUCKETS histogram может завести любую
                // корзину. Ключ и счётчик считаются отдельно от BASE.
                Kind::Histogram { .. } => MAX_BUCKETS,
                Kind::Range { edges, .. } => edges.len().max(1),
                // Terms собирает все уникальные значения до финального
                // отсечения top-N. `size` ограничивает только выдачу: если
                // принимать его за размер накопителя, запрос с size=10 на
                // поле с миллионами значений получает заниженную admission
                // оценку и может удерживать весь MAX_BUCKETS-набор.
                Kind::Terms { .. } => MAX_BUCKETS,
                // HLL — фиксированный массив регистров, а не HashMap из
                // корзин. Переводим его фактический размер в условные bucket
                // units с тем же служебным запасом, что и для остальных
                // collectors; это не маскирует дорогой exact cardinality.
                Kind::Cardinality { .. } => {
                    HLL_M.div_ceil(BUCKET)
                        + if agg.space == Space::Keyword {
                            DEFERRED_HLL.div_ceil(BUCKET)
                        } else {
                            0
                        }
                }
                Kind::CardinalityExact { .. } => MAX_BUCKETS,
            };
            BASE.saturating_add(units.saturating_mul(BUCKET))
        })
        .fold(0usize, usize::saturating_add)
}

/// Сколько значений собирать в сегменте под верхушку размера `size`.
///
/// Правило Elasticsearch: чем шире собрано в сегменте, тем меньше шанс, что
/// значение, частое в целом, выпадет из верхушки каждого по отдельности.
pub fn shard_size(size: usize) -> usize {
    size.saturating_mul(3) / 2 + 10
}

/// Накопитель одной агрегации по одному сегменту.
enum Acc {
    Stats {
        count: u64,
        min: i64,
        max: i64,
        sum: i128,
        /// Дробная сумма — своя, потому что складывать ключи бессмысленно.
        /// Компенсированное сложение по Нейману: на ста миллионах слагаемых
        /// наивное даёт заметный дрейф, а сумма — это ответ, а не оценка.
        fsum: f64,
        fcomp: f64,
        any: bool,
        /// Пространство значений — запоминается при заведении накопителя, чтобы
        /// не таскать его отдельным доводом через каждый шаг обхода.
        space: Space,
    },
    Buckets(IntMap),
    /// Верхушка внутри одного сегмента: ключ — значение колонки, а у ключевого
    /// слова это номер списка в словаре **этого** сегмента.
    Terms(IntMap),
    /// Маленькое keyword-поле: счёт по local ordinal без hash/probe на
    /// каждый документ. Перед seal/merge переводится в обычный накопитель.
    TermsDense {
        base: usize,
        counts: Vec<u64>,
    },
    /// Та же верхушка после [`Sink::seal`]: номера переведены в байты и стали
    /// сравнимыми между сегментами. HashMap сверяет полные байты при коллизиях;
    /// строки создаются только для итогового top-size. Числовые поля сюда не переходят — им
    /// перевод не нужен, число и так значит одно и то же везде.
    TermsBytes(HashMap<Vec<u8>, u64>),
    Hll(Box<[u8; HLL_M]>),
    Exact(IntMap),
    ExactText(HashSet<String>),
    Rejected {
        limit: usize,
    },
}

/// Хеш для целочисленных ключей корзин.
///
/// ## Почему не стандартный
///
/// Стандартный `RandomState` — это SipHash, и он защищает карту от подбора
/// ключей злоумышленником. Полностью отказываться от этой защиты нельзя:
/// numeric/keyword-значение колонки приходит из пользовательского документа,
/// поэтому атакующий способен заранее наполнить индекс специально выбранными
/// ключами. Здесь SipHash используется один раз — чтобы получить случайный seed
/// новой карты, — а на каждом документе работает быстрый целочисленный mixer.
///
/// Замерено: `terms` стоит двадцать наносекунд на документ против семи у
/// `stats` по той же колонке, и разница уходит в хеш и пробу карты.
///
/// Перемешивание взято то же, что у [`hll_add`], — финализатор murmur3: два
/// умножения и три сдвига. Слабый хеш здесь опаснее медленного, потому что
/// значения колонки идут подряд и вырождают открытую адресацию в линейный
/// поиск; этот же финализатор уже держит на себе оценку мощности.
#[derive(Clone, Copy)]
struct IntHasher(u64);

#[inline(always)]
fn mix64(v: u64) -> u64 {
    let mut h = v;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

impl Hasher for IntHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }

    /// Общий случай — на него карта целых чисел не попадает, но контракт
    /// `Hasher` обязан быть выполнен целиком: неверный `write` тихо испортил бы
    /// любой другой ключ, который однажды сюда положат.
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = mix64(self.0 ^ u64::from(b));
        }
    }

    #[inline(always)]
    fn write_i64(&mut self, v: i64) {
        self.0 = mix64(self.0 ^ v as u64);
    }

    #[inline(always)]
    fn write_u64(&mut self, v: u64) {
        self.0 = mix64(self.0 ^ v);
    }

    #[inline(always)]
    fn write_i32(&mut self, v: i32) {
        self.0 = mix64(self.0 ^ v as u64);
    }

    #[inline(always)]
    fn write_u32(&mut self, v: u32) {
        self.0 = mix64(self.0 ^ u64::from(v));
    }

    #[inline(always)]
    fn write_usize(&mut self, v: usize) {
        self.0 = mix64(self.0 ^ v as u64);
    }
}

/// Случайный seed отдельной карты закрывает заранее вычисленный hash-flooding,
/// не возвращая SipHash в горячий цикл по документам.
#[derive(Clone)]
struct IntBuildHasher {
    seed: u64,
}

impl Default for IntBuildHasher {
    fn default() -> Self {
        let random = RandomState::new();
        let mut hasher = random.build_hasher();
        hasher.write_u64(0x5356_494e_4442_4147);
        IntBuildHasher { seed: hasher.finish() }
    }
}

impl BuildHasher for IntBuildHasher {
    type Hasher = IntHasher;

    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher {
        IntHasher(self.seed)
    }
}

/// Карта счётчиков по целочисленному ключу.
type IntMap = HashMap<i64, u64, IntBuildHasher>;

const HLL_M: usize = 1 << 14;

fn hll_add(regs: &mut [u8; HLL_M], v: i64) {
    // Перемешивание: значения колонки часто идут подряд, а HLL требует
    // равномерного хеша, иначе оценка врёт в разы.
    let mut h = v as u64;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    let idx = (h >> (64 - 14)) as usize;
    let rank = ((h << 14) | (1 << 13)).leading_zeros() as u8 + 1;
    if regs[idx] < rank {
        regs[idx] = rank;
    }
}

fn hll_estimate(regs: &[u8; HLL_M]) -> u64 {
    let m = HLL_M as f64;
    let mut sum = 0f64;
    let mut zeros = 0usize;
    for &r in regs.iter() {
        sum += 1.0 / ((1u64 << r.min(63)) as f64);
        if r == 0 {
            zeros += 1;
        }
    }
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let raw = alpha * m * m / sum;
    // Поправка на малых мощностях: без неё оценка на десятке различных значений
    // ошибается кратно.
    if raw <= 2.5 * m && zeros > 0 {
        return (m * (m / zeros as f64).ln()).round() as u64;
    }
    raw.round() as u64
}

/// Верхушка из полностью собранных пар.
///
/// Накопители всех локальных сегментов сливаются до вызова этой функции,
/// поэтому наружу должен выйти запрошенный `size`, а не внутренний
/// `shard_size`. Возврат `shard_size` нарушал публичный договор: запрос
/// `size=50` на девяти сегментах отдавал 85 buckets, в отличие от ES и от
/// ожидаемого клиентами ограничения.
fn top_of(mut v: Vec<(TermKey, u64)>, k: &Kind) -> Value {
    let size = match k {
        Kind::Terms { size, .. } => *size,
        _ => 10,
    };
    let keep = size;
    // По убыванию счёта, при равенстве — по ключу: иначе выдача зависела бы от
    // порядка обхода отображения. Полностью сортировать U элементов не нужно:
    // partition за O(U) оставляет в начале ровно K лучших, а сортируется только
    // этот небольшой префикс.
    let order = |a: &(TermKey, u64), b: &(TermKey, u64)| b.1.cmp(&a.1).then(a.0.cmp(&b.0));
    if keep < v.len() {
        v.select_nth_unstable_by(keep, order);
    }
    let prefix = keep.min(v.len());
    v[..prefix].sort_unstable_by(order);
    let other: u64 = v.iter().skip(keep).map(|(_, c)| *c).sum();
    // Все локальные сегменты уже сведены в один накопитель, поэтому ошибка
    // счёта равна нулю. `other` остаётся точной суммой отброшенных документов.
    let error = 0;
    v.truncate(keep);
    Value::Terms { top: v, error, other }
}

/// В какую корзину равномерной сетки попадает значение.
///
/// Отдельной функцией, потому что спрашивают дважды: обход спрашивает про
/// значение документа, счёт по метаданным — про **края** внутреннего ведра. Два
/// написания одного правила разъехались бы молча, и разъезд был бы виден только
/// в числах.
///
/// Правило монотонно по значению при любом шаге — целом, дробном, календарном.
/// На этом и держится счёт по метаданным: если у концов отрезка ключ один, то он
/// один и у всего, что между ними.
pub fn bucket_key(step: &Step, v: i64) -> i64 {
    match step {
        Step::Int { interval, offset } => {
            let step = (*interval).max(1);
            // Деление с округлением **вниз** и для отрицательных: иначе корзина
            // около нуля вышла бы вдвое шире прочих.
            let d = v - *offset;
            let q = if d >= 0 { d / step } else { -((-d + step - 1) / step) };
            *offset + q * step
        }
        Step::Calendar { unit, tz_ms } => {
            // Смещение пояса прибавляется **до** округления и снимается после:
            // граница суток стоит по местному времени, а ключ остаётся в UTC,
            // иначе сегменты и шарды складывались бы по разным ключам.
            unit.floor(v.saturating_add(*tz_ms)).saturating_sub(*tz_ms)
        }
        Step::Float { interval, offset } => {
            let x = crate::index::float_of(v);
            let q = ((x - *offset) / *interval).floor();
            // Ключ корзины возвращается в пространство ключей: по нему
            // складываются сегменты и шарды, и он же уходит наружу, где
            // переводится обратно в число.
            crate::index::float_key(*offset + q * *interval)
        }
    }
}

impl Acc {
    fn materialize_terms(&mut self) {
        if !matches!(self, Acc::TermsDense { .. }) {
            return;
        }
        let dense = std::mem::replace(self, Acc::Terms(IntMap::default()));
        let Acc::TermsDense { base, counts } = dense else { unreachable!() };
        let Acc::Terms(values) = self else { unreachable!() };
        for (offset, count) in counts.into_iter().enumerate() {
            if count != 0 {
                values.insert((base + offset) as i64, count);
            }
        }
    }

    fn new(k: &Kind, space: Space) -> Acc {
        match k {
            Kind::Stats { .. } => Acc::Stats {
                count: 0,
                min: i64::MAX,
                max: i64::MIN,
                sum: 0,
                fsum: 0.0,
                fcomp: 0.0,
                any: false,
                space,
            },
            Kind::Histogram { .. } => Acc::Buckets(IntMap::default()),
            Kind::Range { edges, .. } if edges.len() > MAX_RANGES => {
                Acc::Rejected { limit: MAX_RANGES }
            }
            Kind::Range { .. } => Acc::Buckets(IntMap::default()),
            Kind::Terms { .. } => Acc::Terms(IntMap::default()),
            Kind::Cardinality { .. } => Acc::Hll(Box::new([0u8; HLL_M])),
            Kind::CardinalityExact { .. } => Acc::Exact(IntMap::default()),
        }
    }

    fn eat_many(
        &mut self,
        kind: &Kind,
        values: impl Iterator<Item = i64>,
        guard: Option<&crate::work::Guard>,
    ) {
        // Bucket count — число документов/слотов в bucket, а не число
        // повторяющихся значений. Stats, напротив, обязаны видеть повторы.
        let mut seen = HashSet::new();
        for (i, value) in values.enumerate() {
            if i.is_multiple_of(64) && guard.is_some_and(|g| !g.check()) {
                break;
            }
            match kind {
                Kind::Histogram { step, .. } => {
                    if seen.insert(bucket_key(step, value)) {
                        self.eat(kind, Some(value));
                    }
                }
                Kind::Terms { .. } => {
                    if seen.insert(value) {
                        self.eat(kind, Some(value));
                    }
                }
                Kind::Range { edges, .. } => {
                    if let Acc::Buckets(buckets) = self {
                        for (key, &(lo, hi)) in edges.iter().enumerate() {
                            if lo <= value
                                && hi.is_none_or(|hi| value < hi)
                                && seen.insert(key as i64)
                            {
                                *buckets.entry(key as i64).or_insert(0) += 1;
                            }
                        }
                    }
                }
                _ => self.eat(kind, Some(value)),
            }
            if seen.len() > MAX_BUCKETS {
                break;
            }
        }
    }

    fn eat(&mut self, k: &Kind, v: Option<i64>) {
        if let Acc::TermsDense { base, counts } = self {
            let Some(value) = v else { return };
            if let Some(count) = usize::try_from(value)
                .ok()
                .and_then(|v| v.checked_sub(*base))
                .and_then(|offset| counts.get_mut(offset))
            {
                *count += 1;
            } else {
                // Не теряем значение при неожиданном ordinal: безопасный
                // fallback вместо panics или молчаливого усечения.
                self.materialize_terms();
                self.eat(k, Some(value));
            }
            return;
        }
        match (self, k) {
            (
                Acc::Stats { count, min, max, sum, fsum, fcomp, any, space },
                Kind::Stats { field },
            ) => {
                if field.is_none() {
                    *count += 1;
                    return;
                }
                let Some(v) = v else { return };
                *count += 1;
                *any = true;
                // Наименьшее и наибольшее считаются по ключу: он монотонен, а
                // значит порядок у ключей тот же, что у чисел. Переводится
                // только выдача.
                *min = (*min).min(v);
                *max = (*max).max(v);
                if *space == Space::Float {
                    // Компенсированное сложение по Нейману.
                    let x = crate::index::float_of(v);
                    let t = *fsum + x;
                    *fcomp += if fsum.abs() >= x.abs() { (*fsum - t) + x } else { (x - t) + *fsum };
                    *fsum = t;
                } else {
                    *sum += v as i128;
                }
            }
            (Acc::Buckets(b), Kind::Histogram { step, .. }) => {
                let Some(v) = v else { return };
                // Переполнение запоминается и больше ничего не считает: работа
                // после него всё равно уйдёт в отказ.
                if b.len() > MAX_BUCKETS {
                    return;
                }
                *b.entry(bucket_key(step, v)).or_insert(0) += 1;
            }
            (Acc::Buckets(b), Kind::Range { edges, .. }) => {
                let Some(v) = v else { return };
                // Второй рубеж после HTTP-разбора: внутренний вызывающий тоже
                // не может превратить один документ в неограниченную работу.
                for (i, (lo, hi)) in edges.iter().enumerate() {
                    if v >= *lo && hi.is_none_or(|hi| v < hi) {
                        *b.entry(i as i64).or_insert(0) += 1;
                    }
                }
            }
            (Acc::Terms(m), Kind::Terms { .. }) => {
                let Some(v) = v else { return };
                // Верхушка собирает различные значения, и их может оказаться
                // столько же, сколько документов. Потолок тот же.
                if m.len() > MAX_BUCKETS && !m.contains_key(&v) {
                    return;
                }
                *m.entry(v).or_insert(0) += 1;
            }
            (Acc::Hll(regs), Kind::Cardinality { .. }) => {
                if let Some(v) = v {
                    hll_add(regs, v);
                }
            }
            (Acc::Exact(values), Kind::CardinalityExact { .. }) => {
                if let Some(v) = v {
                    if values.len() <= MAX_BUCKETS || values.contains_key(&v) {
                        values.insert(v, 1);
                    }
                }
            }
            _ => {}
        }
    }

    fn finish(mut self, k: &Kind) -> Value {
        self.materialize_terms();
        match self {
            Acc::Stats { count, min, max, sum, fsum, fcomp, any, space } => Value::Stats {
                count,
                min: any.then_some(min),
                max: any.then_some(max),
                sum,
                fsum: (space == Space::Float).then_some(fsum + fcomp),
            },
            Acc::Buckets(b) => {
                if b.len() > MAX_BUCKETS {
                    return Value::TooMany { limit: MAX_BUCKETS };
                }
                let mut v: Vec<(i64, u64)> = b.into_iter().collect();
                v.sort_unstable();
                Value::Buckets(v)
            }
            Acc::Terms(m) => {
                if m.len() > MAX_BUCKETS {
                    return Value::TooMany { limit: MAX_BUCKETS };
                }
                let v = m.into_iter().map(|(k, c)| (TermKey::Num(k), c)).collect();
                top_of(v, k)
            }
            Acc::TermsBytes(m) => {
                if m.len() > MAX_BUCKETS {
                    return Value::TooMany { limit: MAX_BUCKETS };
                }
                let Kind::Terms { size, .. } = k else { unreachable!() };
                let mut values: Vec<_> = m.into_iter().collect();
                let order =
                    |a: &(Vec<u8>, u64), b: &(Vec<u8>, u64)| b.1.cmp(&a.1).then(a.0.cmp(&b.0));
                if *size < values.len() {
                    values.select_nth_unstable_by(*size, order);
                }
                let keep = (*size).min(values.len());
                values[..keep].sort_unstable_by(order);
                let other = values[keep..].iter().map(|(_, count)| *count).sum();
                values.truncate(keep);
                let top = values
                    .into_iter()
                    .map(|(bytes, count)| {
                        (TermKey::Text(String::from_utf8_lossy(&bytes).into_owned()), count)
                    })
                    .collect();
                Value::Terms { top, error: 0, other }
            }
            Acc::Hll(regs) => Value::Cardinality { value: hll_estimate(&regs) },
            Acc::Exact(values) => {
                if values.len() > MAX_BUCKETS {
                    Value::TooMany { limit: MAX_BUCKETS }
                } else {
                    Value::ExactCardinality { value: values.len() as u64 }
                }
            }
            Acc::ExactText(values) => {
                if values.len() > MAX_BUCKETS {
                    Value::TooMany { limit: MAX_BUCKETS }
                } else {
                    Value::ExactCardinality { value: values.len() as u64 }
                }
            }
            Acc::Rejected { limit } => Value::TooMany { limit },
            Acc::TermsDense { .. } => unreachable!("dense terms преобразован перед finish"),
        }
    }

    /// Сливает накопитель другого сегмента.
    fn merge(&mut self, mut other: Acc) {
        self.materialize_terms();
        other.materialize_terms();
        match (self, other) {
            (
                Acc::Stats { count, min, max, sum, fsum, fcomp, any, .. },
                Acc::Stats {
                    count: c2,
                    min: m2,
                    max: x2,
                    sum: s2,
                    fsum: f2,
                    fcomp: k2,
                    any: a2,
                    ..
                },
            ) => {
                *count += c2;
                *sum += s2;
                *fsum += f2;
                *fcomp += k2;
                if a2 {
                    *min = (*min).min(m2);
                    *max = (*max).max(x2);
                    *any = true;
                }
            }
            (Acc::Buckets(a), Acc::Buckets(b)) | (Acc::Terms(a), Acc::Terms(b)) => {
                for (k, v) in b {
                    if a.len() > MAX_BUCKETS && !a.contains_key(&k) {
                        break;
                    }
                    *a.entry(k).or_insert(0) += v;
                }
            }
            (Acc::TermsBytes(a), Acc::TermsBytes(b)) => {
                for (k, v) in b {
                    if a.len() > MAX_BUCKETS && !a.contains_key(&k) {
                        break;
                    }
                    *a.entry(k).or_insert(0) += v;
                }
            }
            (Acc::Hll(a), Acc::Hll(b)) => {
                // Слияние HLL — поразрядный максимум: регистр хранит наибольший
                // ранг, а он не зависит от того, в каком сегменте встретился.
                for i in 0..HLL_M {
                    a[i] = a[i].max(b[i]);
                }
            }
            (Acc::Exact(a), Acc::Exact(b)) => {
                for (value, _) in b {
                    if a.len() > MAX_BUCKETS && !a.contains_key(&value) {
                        break;
                    }
                    a.insert(value, 1);
                }
            }
            (Acc::ExactText(a), Acc::ExactText(b)) => {
                for value in b {
                    if a.len() > MAX_BUCKETS && !a.contains(&value) {
                        break;
                    }
                    a.insert(value);
                }
            }
            _ => {}
        }
    }
}

/// Сторож работы счёта: срок и отмена.
///
/// ## Зачем он вообще нужен
///
/// У счёта не было **ничего**. Верхушка отсекает сама собой — набрала `k` и
/// подняла порог; счёт обязан увидеть каждое совпадение, и его работа равна
/// числу совпадений, а не размеру ответа. `count` без слов на десяти миллионах
/// документов шёл до конца на потоке соединения, и прекратить его было нельзя
/// ничем: ни сроком, ни отменой, ни уходом клиента.
///
/// ## Почему срок, а не число документов
///
/// Предел по числу пройденных документов пришлось бы назначать числом, а
/// правильного числа не существует: на миллионе он избыточен, на миллиарде
/// отказывает верному ответу. Оператора же интересует не число, а время —
/// сколько узел вправе потратить на один запрос. Оно переносится между
/// корпусами, число — нет.
///
/// ## Почему часы спрашиваются не на каждом документе
///
/// `Instant::now` стоит десятки наносекунд — на порядок больше самой проверки
/// документа. Сверка идёт раз в [`Guard::STRIDE`] документов: срок при этом
/// соблюдается с точностью до времени, которое узел тратит на тысячу
/// документов, и это на несколько порядков меньше любого осмысленного срока.
///
/// Отмена читается там же. Она не срочнее срока: клиент, уже ушедший, подождёт
/// ещё тысячу документов.
pub type Guard = crate::work::Guard;

/// Местные позиции полей агрегаций в этом сегменте.
///
/// ## Зачем перевод и почему его не было
///
/// Поле у нас адресуется **двумя** числами. Устойчивый номер (`fid`) один на
/// весь индекс и не меняется никогда; местная позиция — это место поля в списке
/// **конкретного** сегмента, и она у каждого своя. Совпадают они ровно до
/// первого сегмента, объявленного в другом порядке, — а это обычное следствие
/// добавления поля в середину схемы: реестр выдаёт новому полю следующий
/// свободный номер, а порядок объявления сохраняется как дан.
///
/// Отбор и сортировка переводят одно в другое (`QuerySpec::resolve`), а
/// агрегации адресовали устойчивым номером напрямую — то есть на таком
/// сегменте читали **чужую колонку**. Молча: числа на месте, порядок
/// осмысленный, данные не те. Замерено на двух сегментах по двадцать
/// документов: `stats` по цене выдавал `max=1000, sum=20000` вместо
/// `max=190, sum=3800`, а гистограмма по одному полю приходила с корзинами
/// другого.
///
/// [`crate::segments::NO_FIELD`] — поля в этом сегменте нет. Это не ошибка:
/// сегмент мог быть собран до того, как поле объявили, и наблюдений у него
/// тогда просто нет.
pub fn fields_in(ix: &Index, aggs: &[Agg]) -> Vec<usize> {
    let at = |fid: usize| ix.field_by_id(fid as u32).unwrap_or(crate::segments::NO_FIELD);
    aggs.iter()
        .map(|a| match a.kind {
            Kind::Stats { field: None } => crate::segments::NO_FIELD,
            Kind::Stats { field: Some(fid) }
            | Kind::Histogram { field: fid, .. }
            | Kind::Range { field: fid, .. }
            | Kind::Terms { field: fid, .. }
            | Kind::Cardinality { field: fid }
            | Kind::CardinalityExact { field: fid } => at(fid),
        })
        .collect()
}

/// Накопители всех агрегаций одного сегмента.
type KeywordHashCache = Option<Box<[(u32, i64); 256]>>;
const MAX_DEFERRED_ORDINALS: usize = 32_768;
const MAX_DENSE_ORDINALS: usize = 4_194_304;

enum DeferredOrdinals {
    Pending,
    Deferred(HashSet<u32>),
    Dense(Vec<u64>),
}

pub struct Sink {
    accs: Vec<Acc>,
    /// Память о текущем блоке колонки — по одной на агрегацию.
    blocks: Vec<crate::column::BlockCache>,
    /// Малый прямой кеш `ordinal -> hash(value)` для keyword-cardinality.
    ///
    /// Глобальные ординалы строить нельзя: они росли бы со словарём запроса и
    /// уничтожили бы фиксированный расход HLL. Повторно разбирать
    /// фронт-кодированное слово на каждом документе тоже дорого. 256 мест
    /// удерживают частые значения, но остаются строго ограниченными независимо
    /// от мощности поля.
    keyword_hashes: Vec<KeywordHashCache>,
    /// Ординалы keyword-кардинальности, которые можно захешировать одним
    /// последовательным проходом словаря на seal.
    deferred_ordinals: Vec<Option<DeferredOrdinals>>,
}

impl Sink {
    /// Выбор представления collector-а один раз на сегмент/полосу, до
    /// document loop. Поле с большим словарём сохраняет sparse IntMap.
    pub(crate) fn for_index(
        ix: &Index,
        aggs: &[Agg],
        at: &[usize],
        guard: Option<&crate::work::Guard>,
    ) -> Sink {
        let mut sink = Self::new(aggs);
        for ((acc, agg), &local) in sink.accs.iter_mut().zip(aggs).zip(at) {
            if matches!(agg.kind, Kind::Terms { .. })
                && ix.fields.get(local).is_some_and(|field| field.kind.keyword())
            {
                let Some(range) = ix.dict.field_ordinals_guarded(local as u32, guard) else {
                    break;
                };
                if !range.is_empty() && range.len() <= MAX_BUCKETS {
                    *acc = Acc::TermsDense { base: range.start, counts: vec![0; range.len()] };
                }
            }
        }
        sink
    }

    pub fn new(aggs: &[Agg]) -> Sink {
        Sink {
            accs: aggs.iter().map(|a| Acc::new(&a.kind, a.space)).collect(),
            blocks: vec![crate::column::BlockCache::default(); aggs.len()],
            keyword_hashes: std::iter::repeat_with(|| None).take(aggs.len()).collect(),
            deferred_ordinals: aggs
                .iter()
                .map(|agg| {
                    matches!(agg.kind, Kind::Cardinality { .. })
                        .then_some(DeferredOrdinals::Pending)
                })
                .collect(),
        }
    }

    /// Закрывает счёт по сегменту: номера словаря переводятся в значения.
    ///
    /// Звать **обязательно** до [`Sink::merge`] и ровно один раз, пока `ix` —
    /// тот самый сегмент, по которому считали. Номер ключевого слова
    /// действителен только внутри своего словаря, и после этого вызова его
    /// больше нет — есть значение, одинаковое во всех сегментах. Разбор — у
    /// [`TermKey`].
    ///
    /// Для terms выбранные ordinals посещаются по возрастанию, восстанавливая
    /// каждую anchor-группу один раз. Карта сохраняет байты для точного слияния;
    /// строки нужны только итоговым корзинам. Число ключей по-прежнему ограничено
    /// [`MAX_BUCKETS`], плюс один sentinel для явного отказа при overflow.
    pub fn seal(&mut self, ix: &Index, aggs: &[Agg], at: &[usize]) {
        self.seal_guarded(ix, aggs, at, None);
    }

    pub fn seal_guarded(
        &mut self,
        ix: &Index,
        aggs: &[Agg],
        at: &[usize],
        guard: Option<&crate::work::Guard>,
    ) {
        for (slot, (&field, deferred)) in
            at.iter().zip(self.deferred_ordinals.iter_mut()).enumerate()
        {
            let Some(ordinals) = deferred.take() else { continue };
            if !ix.fields.get(field).is_some_and(|f| f.kind.keyword()) {
                continue;
            }
            let Acc::Hll(regs) = &mut self.accs[slot] else { continue };
            match ordinals {
                DeferredOrdinals::Pending => {}
                DeferredOrdinals::Deferred(values) => {
                    let mut batch: Vec<usize> = values.into_iter().map(|v| v as usize).collect();
                    batch.sort_unstable();
                    apply_ordinal_hashes(regs, ix, field, &batch, guard);
                }
                DeferredOrdinals::Dense(bits) => {
                    // Даже полный bitmap не превращается в Vec на миллионы
                    // элементов: память промежуточного ответа строго bounded.
                    let mut batch = Vec::with_capacity(MAX_DEFERRED_ORDINALS);
                    'words: for (word, mut value) in bits.into_iter().enumerate() {
                        if word.is_multiple_of(256) && guard.is_some_and(|g| !g.check()) {
                            break;
                        }
                        while value != 0 {
                            batch.push(word * 64 + value.trailing_zeros() as usize);
                            value &= value - 1;
                            if batch.len() == MAX_DEFERRED_ORDINALS {
                                if !apply_ordinal_hashes(regs, ix, field, &batch, guard) {
                                    batch.clear();
                                    break 'words;
                                }
                                batch.clear();
                            }
                        }
                    }
                    if !batch.is_empty() {
                        apply_ordinal_hashes(regs, ix, field, &batch, guard);
                    }
                }
            }
        }
        for ((acc, agg), &field) in self.accs.iter_mut().zip(aggs.iter()).zip(at.iter()) {
            acc.materialize_terms();
            if matches!(agg.kind, Kind::CardinalityExact { .. })
                && ix.fields.get(field).is_some_and(|f| f.kind.keyword())
            {
                let taken = std::mem::replace(acc, Acc::Exact(IntMap::default()));
                let Acc::Exact(values) = taken else { continue };
                let mut text = HashSet::with_capacity(values.len());
                for (li, _) in values {
                    let (_, term) = ix.dict.term_at(li as usize);
                    if !term.is_empty() {
                        text.insert(
                            term.strip_prefix(crate::indexer::KEYWORD_TAG)
                                .unwrap_or(&term)
                                .to_string(),
                        );
                    }
                }
                *acc = Acc::ExactText(text);
                continue;
            }
            let Kind::Terms { .. } = agg.kind else { continue };
            // Числовому полю перевод не нужен и вреден: там ключ — само
            // значение, а не позиция в словаре.
            if !ix.fields.get(field).is_some_and(|f| f.kind.keyword()) {
                continue;
            }
            let Acc::Terms(m) = acc else { continue };
            let mut text: HashMap<Vec<u8>, u64> = HashMap::with_capacity(m.len());
            let mut ordinals: Vec<usize> =
                m.keys().filter_map(|&v| usize::try_from(v).ok()).collect();
            ordinals.sort_unstable();
            ix.dict.visit_sorted_guarded(&ordinals, guard, |ordinal, local, term| {
                if local as usize != field {
                    return;
                }
                let bytes =
                    term.strip_prefix(crate::indexer::KEYWORD_TAG.as_bytes()).unwrap_or(term);
                *text.entry(bytes.to_vec()).or_insert(0) += m[&(ordinal as i64)];
            });
            *acc = Acc::TermsBytes(text);
        }
    }

    /// Принимает совпавший документ.
    ///
    /// `at` — местные позиции полей в этом сегменте, см. [`fields_in`].
    /// Отдельным доводом, а не выведенным на месте: именно неявность этого
    /// перевода и стоила чтения чужой колонки.
    pub fn accept(&mut self, ix: &Index, aggs: &[Agg], at: &[usize], doc: u32) {
        self.accept_guarded(ix, aggs, at, doc, None);
    }

    pub fn accept_guarded(
        &mut self,
        ix: &Index,
        aggs: &[Agg],
        at: &[usize],
        doc: u32,
        guard: Option<&crate::work::Guard>,
    ) {
        for (((((acc, agg), cache), keyword_hashes), deferred), &local) in self
            .accs
            .iter_mut()
            .zip(aggs.iter())
            .zip(self.blocks.iter_mut())
            .zip(self.keyword_hashes.iter_mut())
            .zip(self.deferred_ordinals.iter_mut())
            .zip(at.iter())
        {
            // `Stats` без поля считает наблюдения и колонку не спрашивает —
            // это единственный случай, когда поля нет по смыслу, а не по
            // отсутствию его в сегменте.
            let field = match &agg.kind {
                Kind::Stats { field: None } => None,
                _ => Some(local),
            };
            match agg.over {
                Over::Docs => {
                    if let Some(values) = field
                        .and_then(|f| ix.fields.get(f))
                        .and_then(|f| f.multi_column.as_ref())
                        .and_then(|column| column.get(doc))
                    {
                        if matches!(agg.kind, Kind::Cardinality { .. })
                            && ix.fields.get(local).is_some_and(|f| f.kind.keyword())
                        {
                            for value in values {
                                if guard.is_some_and(|g| !g.check()) {
                                    break;
                                }
                                keyword_eat(acc, ix, local, value, deferred, keyword_hashes, guard);
                            }
                        } else {
                            acc.eat_many(&agg.kind, values, guard);
                        }
                        continue;
                    }
                    let mut v = field.and_then(|f| {
                        ix.fields
                            .get(f)
                            .and_then(|x| x.column.as_ref())
                            .and_then(|c| c.get_with(cache, doc))
                    });
                    if matches!(agg.kind, Kind::Cardinality { .. })
                        && ix.fields.get(local).is_some_and(|f| f.kind.keyword())
                    {
                        if let Some(value) = v {
                            keyword_eat(acc, ix, local, value, deferred, keyword_hashes, guard);
                            v = None;
                        }
                    }
                    acc.eat(&agg.kind, v);
                }
                Over::Slots { group } => {
                    let Some(slots) = ix.slots.get(group as usize) else { continue };
                    let (from, n) = slots.range(doc);
                    let col = field.and_then(|f| ix.fields.get(f)).and_then(|x| x.column.as_ref());
                    for s in 0..n {
                        if guard.is_some_and(|g| !g.check()) {
                            break;
                        }
                        if let Some(values) = field
                            .and_then(|f| ix.fields.get(f))
                            .and_then(|f| f.multi_column.as_ref())
                            .and_then(|column| column.get(from + s))
                        {
                            if matches!(agg.kind, Kind::Cardinality { .. })
                                && ix.fields.get(local).is_some_and(|f| f.kind.keyword())
                            {
                                for value in values {
                                    if guard.is_some_and(|g| !g.check()) {
                                        break;
                                    }
                                    keyword_eat(
                                        acc,
                                        ix,
                                        local,
                                        value,
                                        deferred,
                                        keyword_hashes,
                                        guard,
                                    );
                                }
                            } else {
                                acc.eat_many(&agg.kind, values, guard);
                            }
                            continue;
                        }
                        // Наблюдение — элемент. Элемент без значения считается
                        // только там, где считаются сами элементы: у `count`
                        // без поля.
                        let mut v = col.and_then(|c| c.get(from + s));
                        if v.is_none() && !matches!(agg.kind, Kind::Stats { field: None }) {
                            continue;
                        }
                        if matches!(agg.kind, Kind::Cardinality { .. })
                            && ix.fields.get(local).is_some_and(|f| f.kind.keyword())
                        {
                            if let Some(value) = v {
                                keyword_eat(acc, ix, local, value, deferred, keyword_hashes, guard);
                                v = None;
                            }
                        }
                        acc.eat(&agg.kind, v);
                    }
                }
            }
        }
    }

    pub fn merge(&mut self, other: Sink) {
        for (a, b) in self.accs.iter_mut().zip(other.accs) {
            a.merge(b);
        }
    }

    /// Засчитывает разом `n` наблюдений со значениями в `[lo, hi]`.
    ///
    /// ## Зачем
    ///
    /// Внутреннее ведро колонки уже знает, сколько в нём документов, — это
    /// число лежит в его заголовке. Когда ведро целиком внутри условия отбора и
    /// целиком внутри одной корзины счёта, читать его документы незачем: их
    /// вклад известен заранее. Работа становится `O(вёдер)` вместо `O(документов)`.
    ///
    /// ## Когда это не годится
    ///
    /// `false` — считать придётся по документу. Так бывает, когда
    /// агрегация смотрит на само значение (сумма, верхушка, мощность), а не
    /// только на факт попадания, или когда края ведра попадают в разные корзины.
    ///
    /// Проверка идёт **до** любого изменения: наполовину засчитанная порция
    /// хуже несосчитанной, потому что её уже нельзя ни доучесть, ни отменить.
    pub fn bulk(&mut self, aggs: &[Agg], lo: i64, hi: i64, n: u64) -> bool {
        if n == 0 {
            return true;
        }
        let fits = |acc: &Acc, agg: &Agg| -> bool {
            match (acc, &agg.kind) {
                // Счёт наблюдений: значения не смотрит вовсе.
                (Acc::Stats { .. }, Kind::Stats { field: None }) => true,
                (Acc::Buckets(b), Kind::Histogram { step, .. }) => {
                    b.len() <= MAX_BUCKETS && bucket_key(step, lo) == bucket_key(step, hi)
                }
                (Acc::Buckets(b), Kind::Range { edges, .. }) => {
                    // Диапазоны вправе перекрываться, и значение попадает
                    // сразу в несколько. Порция годится, только если **каждый**
                    // объявленный диапазон либо накрывает весь отрезок, либо не
                    // задевает его вовсе: тогда набор попаданий один на всю
                    // порцию. Совпадения на концах для этого мало — диапазон
                    // может целиком поместиться между ними.
                    b.len() <= MAX_BUCKETS
                        && edges.iter().all(|(a, z)| {
                            (*a <= lo && z.is_none_or(|z| hi < z))
                                || z.is_some_and(|z| z <= lo)
                                || *a > hi
                                || z.is_some_and(|z| z <= *a)
                        })
                }
                _ => false,
            }
        };
        if !self.accs.iter().zip(aggs.iter()).all(|(a, g)| fits(a, g)) {
            return false;
        }
        for (acc, agg) in self.accs.iter_mut().zip(aggs.iter()) {
            match (acc, &agg.kind) {
                (Acc::Stats { count, .. }, Kind::Stats { field: None }) => *count += n,
                (Acc::Buckets(b), Kind::Histogram { step, .. }) => {
                    *b.entry(bucket_key(step, lo)).or_insert(0) += n;
                }
                (Acc::Buckets(b), Kind::Range { edges, .. }) => {
                    // Берутся те диапазоны, что накрывают отрезок целиком.
                    // Проверка выше уже отвергла порцию, если хоть один
                    // диапазон задевал его частью, — значит набор попаданий
                    // один на всю порцию.
                    for (i, _) in edges
                        .iter()
                        .enumerate()
                        .filter(|(_, (a, z))| *a <= lo && z.is_none_or(|z| hi < z))
                    {
                        *b.entry(i as i64).or_insert(0) += n;
                    }
                }
                _ => unreachable!("порция проверена до изменения"),
            }
        }
        true
    }

    /// Забирает дробные суммы, обнуляя их в накопителях.
    ///
    /// ## Зачем их вынимать
    ///
    /// Всё остальное складывается в любом порядке и даёт то же число: счётчики
    /// и целые суммы точны, `min`/`max` не зависят от порядка, регистры HLL
    /// сливаются поразрядным максимумом. Дробная сумма — единственное, что
    /// порядок меняет: сложение `f64` не ассоциативно, и `(a+b)+c` вообще
    /// говоря не равно `a+(b+c)` побитово.
    ///
    /// Значит при обходе в несколько потоков её нельзя складывать там, где
    /// поток окажется первым. Она вынимается из каждого сегмента отдельно и
    /// складывается потом — по возрастанию номера сегмента, то есть ровно в том
    /// порядке, в каком её сложил бы один поток.
    ///
    /// Пара, а не число: компенсация Неймана хранится отдельно и складывается
    /// отдельно, иначе слияние потеряло бы младшие разряды, ради которых она и
    /// заведена.
    pub fn take_floats(&mut self) -> Vec<(f64, f64)> {
        self.accs
            .iter_mut()
            .map(|a| match a {
                Acc::Stats { fsum, fcomp, .. } => {
                    let out = (*fsum, *fcomp);
                    *fsum = 0.0;
                    *fcomp = 0.0;
                    out
                }
                _ => (0.0, 0.0),
            })
            .collect()
    }

    /// Возвращает дробные суммы на место — уже сложенные по порядку сегментов.
    pub fn set_floats(&mut self, v: &[(f64, f64)]) {
        for (a, &(s, c)) in self.accs.iter_mut().zip(v.iter()) {
            if let Acc::Stats { fsum, fcomp, .. } = a {
                *fsum = s;
                *fcomp = c;
            }
        }
    }

    /// Сколько накопителей — по одному на агрегацию.
    pub fn len(&self) -> usize {
        self.accs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accs.is_empty()
    }

    pub fn finish(self, aggs: &[Agg]) -> Vec<Result_> {
        self.accs
            .into_iter()
            .zip(aggs.iter())
            .map(|(acc, agg)| Result_ {
                name: agg.name.clone(),
                value: acc.finish(&agg.kind),
                space: agg.space,
            })
            .collect()
    }
}

/// Переводит местный ordinal keyword-колонки в устойчивый хеш значения.
///
/// `local` сверяется с полем словаря. Несовпадение означает повреждённую
/// колонку или чужой ordinal; считать его как законное значение нельзя.
fn apply_ordinal_hashes(
    regs: &mut [u8; HLL_M],
    ix: &Index,
    field: usize,
    ordinals: &[usize],
    guard: Option<&crate::work::Guard>,
) -> bool {
    let mut tick = 0;
    for (dict_field, hash) in
        ix.dict.hashes_at_sorted_guarded(ordinals, guard).into_iter().flatten()
    {
        if guard.is_some_and(|g| !g.go(&mut tick)) {
            return false;
        }
        if dict_field == field as u32 {
            hll_add(regs, hash as i64);
        }
    }
    guard.is_none_or(|g| g.check())
}

fn keyword_eat(
    acc: &mut Acc,
    ix: &Index,
    local: usize,
    value: i64,
    deferred: &mut Option<DeferredOrdinals>,
    cache: &mut KeywordHashCache,
    guard: Option<&crate::work::Guard>,
) {
    let Ok(ordinal) = u32::try_from(value) else { return };
    if matches!(deferred, Some(DeferredOrdinals::Pending)) {
        *deferred = Some(if ix.dict.len() <= MAX_DENSE_ORDINALS {
            DeferredOrdinals::Dense(vec![0; ix.dict.len().div_ceil(64)])
        } else {
            DeferredOrdinals::Deferred(HashSet::new())
        });
    }
    if let Some(DeferredOrdinals::Dense(bits)) = deferred {
        let at = ordinal as usize;
        if at < MAX_DENSE_ORDINALS && at < ix.dict.len() {
            bits[at / 64] |= 1u64 << (at % 64);
        }
        return;
    }
    if let Some(DeferredOrdinals::Deferred(values)) = deferred {
        values.insert(ordinal);
        if values.len() <= MAX_DEFERRED_ORDINALS {
            return;
        }
        let mut ordinals: Vec<usize> =
            std::mem::take(values).into_iter().map(|ordinal| ordinal as usize).collect();
        ordinals.sort_unstable();
        if let Acc::Hll(regs) = acc {
            apply_ordinal_hashes(regs, ix, local, &ordinals, guard);
        }
        return;
    }
    if let (Acc::Hll(regs), Some(hash)) = (acc, keyword_hash(ix, local, Some(value), cache)) {
        hll_add(regs, hash);
    }
}

fn keyword_hash(
    ix: &Index,
    local: usize,
    value: Option<i64>,
    cache: &mut KeywordHashCache,
) -> Option<i64> {
    let ordinal = u32::try_from(value?).ok()?;
    let entries = cache.get_or_insert_with(|| Box::new([(u32::MAX, 0); 256]));
    let slot = ordinal as usize % entries.len();
    if entries[slot].0 == ordinal {
        return Some(entries[slot].1);
    }
    let (field, hash) = ix.dict.hash_at(ordinal as usize)?;
    if field != local as u32 {
        return None;
    }
    let hash = hash as i64;
    entries[slot] = (ordinal, hash);
    Some(hash)
}

#[cfg(test)]
mod guard_tests {
    use super::*;

    /// Часы сторожа: истёкший срок останавливает, неистёкший — нет.
    #[test]
    fn an_expired_deadline_stops_the_walk() {
        let g = Guard::new(1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let mut tick = 0u32;
        // До шага сверки сторож не вмешивается — и это не упущение, а цена:
        // `Instant::now` стоит дороже самой проверки документа.
        for _ in 1..Guard::STRIDE {
            assert!(g.go(&mut tick), "сторож остановил обход раньше шага сверки");
        }
        assert!(!g.go(&mut tick), "срок вышел, а сторож пропустил");
        assert!(g.fired(), "сторож сработал и не признался");
        // Сработав, держит своё решение и между сверками.
        assert!(!g.go(&mut tick), "сторож передумал на следующем документе");
    }

    /// Не истёкший срок не мешает: сторож не должен быть налогом.
    #[test]
    fn a_live_deadline_does_not_interfere() {
        let g = Guard::new(60_000);
        let mut tick = 0u32;
        for _ in 0..Guard::STRIDE * 3 {
            assert!(g.go(&mut tick), "сторож остановил обход на живом сроке");
        }
        assert!(!g.fired(), "сторож объявил обход неполным ни с того ни с сего");
    }

    /// Без срока сторож молчит, пока его не попросят.
    #[test]
    fn without_a_deadline_only_cancel_speaks() {
        let g = Guard::new(0);
        let mut tick = 0u32;
        for _ in 0..Guard::STRIDE * 2 {
            assert!(g.go(&mut tick), "сторож без срока остановил обход");
        }
        g.cancel();
        // Отмена читается на ближайшей сверке, не позже.
        let mut seen = 0u32;
        while g.go(&mut tick) {
            seen += 1;
            assert!(seen <= Guard::STRIDE, "отмену не заметили за {seen} документов");
        }
        assert!(g.fired(), "отмена не отметилась");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_terms_keeps_missing_multivalue_dedup_and_safe_fallback() {
        let kind = Kind::Terms { field: 0, size: 10 };
        let mut dense = Acc::TermsDense { base: 100, counts: vec![0; 4] };
        let mut oracle = Acc::new(&kind, Space::Int);
        for values in [vec![100, 100, 101], vec![], vec![103, 101, 103]] {
            dense.eat_many(&kind, values.iter().copied(), None);
            oracle.eat_many(&kind, values.into_iter(), None);
        }
        dense.eat(&kind, None);
        assert!(matches!(dense, Acc::TermsDense { .. }));
        // Некорректная граница не должна ни паниковать, ни терять наблюдение.
        dense.eat(&kind, Some(500));
        oracle.eat(&kind, Some(500));
        assert!(matches!(dense, Acc::Terms(_)));
        assert_eq!(dense.finish(&kind), oracle.finish(&kind));
    }

    #[test]
    fn byte_terms_top_matches_string_oracle_with_unicode_ties_and_other() {
        let values = [("", 2), ("алый", 5), ("синий", 5), ("é", 5), ("z", 1)];
        for size in [0, 1, 3, 5, 10] {
            let kind = Kind::Terms { field: 0, size };
            let bytes = Acc::TermsBytes(
                values.iter().map(|(key, count)| (key.as_bytes().to_vec(), *count)).collect(),
            );
            let oracle = top_of(
                values.iter().map(|(key, count)| (TermKey::Text((*key).into()), *count)).collect(),
                &kind,
            );
            assert_eq!(bytes.finish(&kind), oracle);
        }
    }

    #[test]
    fn sequential_keyword_terms_merge_multivalues_missing_and_anchor_groups() {
        use crate::{
            indexer::{Cell, Indexer},
            segments::{QuerySpec, Segments},
        };
        let dir = std::env::temp_dir().join(format!("svindb-terms-bytes-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer = Indexer::typed(
            &dir,
            vec!["tag".into()],
            vec![1.0],
            vec![crate::index::Kind::KeywordExact],
            1000,
        );
        let mut expected = HashMap::<String, u64>::new();
        for doc in 0..3000 {
            let id = format!("d{doc:05}");
            if doc % 97 == 0 {
                writer.add_cells(&id, &[0], &[]);
                continue;
            }
            let tag = format!("тег-{doc:05}");
            let values = crate::indexer::encode_multi(&["общий".into(), tag.clone(), tag.clone()]);
            writer.add_cells(&id, &[1], &[Cell { field: 0, slot: 0, text: &values }]);
            *expected.entry("общий".into()).or_default() += 1;
            expected.insert(tag, 1);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        assert!(segments.list.len() >= 3);
        let kind = Kind::Terms { field: 0, size: 3 };
        let got = segments.aggregate(
            &QuerySpec::default(),
            &[Agg {
                name: "tags".into(),
                kind: kind.clone(),
                over: Over::Docs,
                space: Space::Keyword,
            }],
        );
        let oracle = top_of(
            expected.into_iter().map(|(key, count)| (TermKey::Text(key), count)).collect(),
            &kind,
        );
        assert_eq!(got[0].value, oracle);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_scalar_count_has_fixed_memory_hint() {
        let aggs = [Agg {
            name: "count".into(),
            kind: Kind::Stats { field: None },
            over: Over::Docs,
            space: Space::Int,
        }];
        assert!(memory_hint(&aggs) < 1024, "scalar count оценён как широкая агрегация");
    }

    #[test]
    fn a_bucket_aggregation_hint_is_bounded_by_its_kind() {
        let aggs = [Agg {
            name: "terms".into(),
            kind: Kind::Terms { field: 0, size: 100 },
            over: Over::Docs,
            space: Space::Keyword,
        }];
        let hint = memory_hint(&aggs);
        assert!(
            hint > memory_hint(&[Agg {
                name: "count".into(),
                kind: Kind::Stats { field: None },
                over: Over::Docs,
                space: Space::Int,
            }])
        );
        assert!(
            hint <= MAX_BUCKETS * 64 + 256,
            "terms получает ровно общий предел накопителя, а не неограниченную оценку"
        );
    }

    #[test]
    fn hll_hint_reflects_registers_and_exact_cardinality_keeps_the_hash_map_ceiling() {
        let hll = memory_hint(&[Agg {
            name: "cardinality".into(),
            kind: Kind::Cardinality { field: 0 },
            over: Over::Docs,
            space: Space::Keyword,
        }]);
        let exact = memory_hint(&[Agg {
            name: "cardinality".into(),
            kind: Kind::CardinalityExact { field: 0 },
            over: Over::Docs,
            space: Space::Keyword,
        }]);
        assert!(hll >= HLL_M, "HLL admission меньше массива регистров");
        assert!(hll < exact, "HLL не должен оцениваться как exact HashMap");
    }

    fn feed(k: Kind, vals: &[Option<i64>]) -> Value {
        let mut acc = Acc::new(&k, Space::Int);
        for v in vals {
            acc.eat(&k, *v);
        }
        acc.finish(&k)
    }

    /// Счёт, границы и сумма считаются только по тем, у кого значение есть.
    #[test]
    fn stats_ignore_what_has_no_value() {
        let v = feed(Kind::Stats { field: Some(0) }, &[Some(3), None, Some(-1), Some(10), None]);
        assert_eq!(v, Value::Stats { count: 3, min: Some(-1), max: Some(10), sum: 12, fsum: None });
        // Ни одного значения — границ нет, а не ноль: ноль это значение.
        let v = feed(Kind::Stats { field: Some(0) }, &[None, None]);
        assert_eq!(v, Value::Stats { count: 0, min: None, max: None, sum: 0, fsum: None });
    }

    /// Корзина около нуля не шире остальных.
    ///
    /// Деление в Rust округляет к нулю, и `-1 / 10` даёт ноль так же, как
    /// `1 / 10`. Корзина `[-10, 10)` вышла бы вдвое шире всех прочих — и
    /// гистограмма врала бы ровно посередине шкалы.
    #[test]
    fn a_histogram_bucket_near_zero_is_not_twice_as_wide() {
        let k = Kind::Histogram {
            field: 0,
            interval: 10,
            offset: 0,
            step: Step::Int { interval: 10, offset: 0 },
        };
        let vals: Vec<Option<i64>> = (-25..25).map(Some).collect();
        let Value::Buckets(b) = feed(k, &vals) else { panic!("не корзины") };
        let keys: Vec<i64> = b.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![-30, -20, -10, 0, 10, 20]);
        // Каждая полная корзина обязана нести ровно десять значений; крайние —
        // сколько попало. Проверяется честным пересчётом того же правила.
        for (key, n) in b.iter() {
            let want = (-25..25).filter(|v| *v >= *key && *v < key + 10).count() as u64;
            assert_eq!(*n, want, "корзина {key}");
        }
    }

    /// Верхушка отдаёт границу ошибки и остаток, а не молчит о них.
    /// Подряд идущие значения не сбиваются в одну корзину карты.
    ///
    /// ## Почему это проверяется, а не подразумевается
    ///
    /// Ключ корзины — значение колонки, а они идут подряд по самой природе
    /// данных: номера каналов, даты, счётчики. Хеш, оставляющий младшие разряды
    /// как есть, на таком входе вырождает открытую адресацию в линейный поиск —
    /// и карта, которую заводили ради скорости, становится медленнее той, что
    /// заменили. Отказа при этом не будет: ответ верный, просто медленный, и
    /// заметить это можно лишь замером.
    ///
    /// Проверяется дисперсия младших разрядов: именно по ним карта выбирает
    /// корзину.
    #[test]
    fn consecutive_keys_do_not_pile_into_one_bucket() {
        const N: u64 = 4096;
        let mut seen = vec![false; N as usize];
        let mut filled = 0usize;
        for v in 0..N {
            let slot = (mix64(v) % N) as usize;
            if !seen[slot] {
                seen[slot] = true;
                filled += 1;
            }
        }
        // Случайное отображение N значений в N корзин занимает примерно
        // N·(1−1/e) ≈ 0,632·N. Порог взят заметно ниже, чтобы тест ловил
        // вырождение, а не дрожание.
        assert!(
            filled > (N as usize) * 55 / 100,
            "подряд идущие ключи заняли всего {filled} корзин из {N} — хеш вырождается"
        );

        // И то же самое для сдвинутого начала: вырождение могло бы прятаться
        // у нуля.
        let mut seen = vec![false; N as usize];
        let mut filled = 0usize;
        for v in 1_000_000_000..1_000_000_000 + N {
            let slot = (mix64(v) % N) as usize;
            if !seen[slot] {
                seen[slot] = true;
                filled += 1;
            }
        }
        assert!(
            filled > (N as usize) * 55 / 100,
            "сдвинутые подряд идущие ключи заняли {filled} корзин из {N}"
        );
    }

    /// Две карты не раскрывают один и тот же детерминированный hash layout.
    /// Иначе набор коллизий можно один раз вычислить снаружи и затем прислать
    /// как значения индексируемого поля.
    #[test]
    fn integer_maps_receive_independent_unpredictable_seeds() {
        let seeds: std::collections::BTreeSet<u64> =
            (0..8).map(|_| IntBuildHasher::default().seed).collect();
        assert!(seeds.len() > 1, "все быстрые карты получили один seed");

        let build = IntBuildHasher::default();
        let hash = |value: i64| {
            let mut hasher = build.build_hasher();
            std::hash::Hash::hash(&value, &mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(42), hash(42), "одна карта обязана хешировать ключ устойчиво");
        assert_ne!(hash(42), hash(43), "соседние ключи не должны совпасть в полном хеше");
    }

    #[test]
    fn terms_report_what_they_dropped() {
        let mut vals: Vec<Option<i64>> = Vec::new();
        // Значение k встречается k раз, k от 1 до 40.
        for k in 1..=40i64 {
            for _ in 0..k {
                vals.push(Some(k));
            }
        }
        let Value::Terms { top, error, other } = feed(Kind::Terms { field: 0, size: 3 }, &vals)
        else {
            panic!("не верхушка")
        };
        assert_eq!(top.len(), 3, "публичный size обязан ограничивать ответ");
        assert_eq!(top[0], (TermKey::Num(40), 40), "самое частое");
        assert_eq!(top[1], (TermKey::Num(39), 39));
        let dropped: u64 = (1..=37).map(|k| k as u64).sum();
        assert_eq!(other, dropped, "остаток посчитан неверно");
        assert_eq!(error, 0, "после глобального слияния счёт точный");
    }

    #[test]
    fn terms_overflow_is_preserved_through_finish_and_merge() {
        let k = Kind::Terms { field: 0, size: 1 };
        for reverse in [false, true] {
            let mut a = Acc::new(&k, Space::Int);
            let mut b = Acc::new(&k, Space::Int);
            for value in 0..MAX_BUCKETS as i64 {
                if reverse {
                    b.eat(&k, Some(value));
                } else {
                    a.eat(&k, Some(value));
                }
            }
            if reverse {
                a.eat(&k, Some(MAX_BUCKETS as i64));
            } else {
                b.eat(&k, Some(MAX_BUCKETS as i64));
            }
            if reverse {
                b.merge(a);
                assert_eq!(b.finish(&k), Value::TooMany { limit: MAX_BUCKETS });
            } else {
                a.merge(b);
                assert_eq!(a.finish(&k), Value::TooMany { limit: MAX_BUCKETS });
            }
        }

        // Тот же overflow возникает у строковой карты уже после seal: это
        // отдельный путь слияния, и он также не должен молча обрезаться.
        let mut text_a = Acc::TermsBytes(HashMap::default());
        if let Acc::TermsBytes(values) = &mut text_a {
            for i in 0..MAX_BUCKETS {
                values.insert(format!("{i}").into_bytes(), 1);
            }
        }
        let mut text_b = Acc::TermsBytes(HashMap::default());
        if let Acc::TermsBytes(values) = &mut text_b {
            values.insert(format!("{}", MAX_BUCKETS).into_bytes(), 1);
        }
        text_a.merge(text_b);
        assert_eq!(text_a.finish(&k), Value::TooMany { limit: MAX_BUCKETS });
    }

    /// Верхушка с равными счётчиками обязана иметь тот же порядок, что и
    /// полный независимый oracle, независимо от порядка вставки в карту.
    #[test]
    fn terms_top_ties_match_sorted_oracle_for_random_inputs() {
        let k = Kind::Terms { field: 0, size: 11 };
        let mut values = Vec::new();
        let mut state = 0x9e37_79b9u64;
        for _ in 0..3_000 {
            state ^= state << 7;
            state ^= state >> 9;
            values.push((state % 257) as i64);
        }
        let mut counts = std::collections::BTreeMap::<i64, u64>::new();
        for value in &values {
            *counts.entry(*value).or_default() += 1;
        }
        let mut oracle: Vec<(TermKey, u64)> =
            counts.into_iter().map(|(value, count)| (TermKey::Num(value), count)).collect();
        oracle.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let Value::Terms { top, other, error } =
            feed(k, &values.iter().copied().map(Some).collect::<Vec<_>>())
        else {
            panic!("не верхушка")
        };
        assert_eq!(top, oracle[..11], "tie-breaker не совпал с полным oracle");
        assert_eq!(other, oracle[11..].iter().map(|(_, n)| n).sum(), "остаток oracle");
        assert_eq!(error, 0);
    }

    #[test]
    fn range_boundaries_are_half_open_and_unbounded_max_is_included() {
        let kind = Kind::Range {
            field: 0,
            edges: vec![(0, Some(10)), (10, Some(20)), (20, None), (10, Some(10))],
        };
        assert_eq!(
            feed(
                kind.clone(),
                &[Some(-1), Some(0), Some(9), Some(10), Some(19), Some(20), Some(i64::MAX)]
            ),
            Value::Buckets(vec![(0, 2), (1, 2), (2, 2)])
        );
        let mut acc = Acc::new(&kind, Space::Int);
        acc.eat_many(&kind, [0, 9, 10, 10, 19, 20, i64::MAX].into_iter(), None);
        assert_eq!(acc.finish(&kind), Value::Buckets(vec![(0, 1), (1, 1), (2, 1)]));

        let float_kind = Kind::Range {
            field: 0,
            edges: vec![
                (crate::index::float_key(0.0), Some(crate::index::float_key(0.5))),
                (crate::index::float_key(0.5), Some(crate::index::float_key(1.0))),
            ],
        };
        let floats = [0.0, 0.25, 0.5, 0.75, 1.0].map(|v| Some(crate::index::float_key(v)));
        assert_eq!(feed(float_kind, &floats), Value::Buckets(vec![(0, 2), (1, 2)]));
    }

    #[test]
    fn range_bulk_rejects_a_batch_crossing_the_exclusive_boundary() {
        let aggs = [Agg {
            name: "ranges".into(),
            kind: Kind::Range { field: 0, edges: vec![(0, Some(10)), (10, None)] },
            over: Over::Docs,
            space: Space::Int,
        }];
        let mut sink = Sink::new(&aggs);
        assert!(!sink.bulk(&aggs, 9, 10, 2));
        assert!(sink.bulk(&aggs, 0, 9, 10));
        assert!(sink.bulk(&aggs, 10, i64::MAX, 7));
        assert_eq!(sink.finish(&aggs)[0].value, Value::Buckets(vec![(0, 10), (1, 7)]));
    }

    /// Слияние сегментов даёт то же, что один проход по всем значениям.
    #[test]
    fn merging_segments_matches_one_pass() {
        let a: Vec<Option<i64>> = (0..500).map(|i| Some(i % 17)).collect();
        let b: Vec<Option<i64>> = (0..300).map(|i| Some(i % 23)).collect();
        for k in [
            Kind::Stats { field: Some(0) },
            Kind::Histogram {
                field: 0,
                interval: 5,
                offset: 0,
                step: Step::Int { interval: 5, offset: 0 },
            },
            Kind::Range { field: 0, edges: vec![(0, Some(5)), (5, Some(10)), (10, Some(101))] },
        ] {
            let whole: Vec<Option<i64>> = a.iter().chain(b.iter()).copied().collect();
            let want = feed(k.clone(), &whole);
            let mut x = Acc::new(&k, Space::Int);
            for v in a.iter() {
                x.eat(&k, *v);
            }
            let mut y = Acc::new(&k, Space::Int);
            for v in b.iter() {
                y.eat(&k, *v);
            }
            x.merge(y);
            assert_eq!(x.finish(&k), want, "слияние разошлось с одним проходом");
        }
    }

    /// Слишком мелкий шаг даёт отказ, а не съеденную память.
    ///
    /// Запрос из тридцати байт — гистограмма с шагом единица по метке времени —
    /// породил бы полтораста миллионов корзин. Обрезать её нельзя: обрезанная
    /// выглядит как настоящая.
    #[test]
    fn too_many_buckets_is_refused_not_truncated() {
        let k = Kind::Histogram {
            field: 0,
            interval: 1,
            offset: 0,
            step: Step::Int { interval: 1, offset: 0 },
        };
        let vals: Vec<Option<i64>> = (0..(MAX_BUCKETS as i64 + 100)).map(Some).collect();
        assert_eq!(feed(k, &vals), Value::TooMany { limit: MAX_BUCKETS });
        // А шаг, укладывающийся в потолок, считается как считался.
        let k = Kind::Histogram {
            field: 0,
            interval: 1000,
            offset: 0,
            step: Step::Int { interval: 1000, offset: 0 },
        };
        let Value::Buckets(b) = feed(k, &vals) else { panic!("не корзины") };
        assert_eq!(b.len(), (MAX_BUCKETS + 100).div_ceil(1000));
    }

    /// Оценка числа различных не врёт кратно.
    #[test]
    fn cardinality_stays_near_the_truth() {
        for n in [10i64, 1_000, 100_000] {
            let vals: Vec<Option<i64>> = (0..n).map(Some).collect();
            let Value::Cardinality { value } = feed(Kind::Cardinality { field: 0 }, &vals) else {
                panic!("не мощность")
            };
            let err = (value as f64 - n as f64).abs() / n as f64;
            assert!(err < 0.05, "различных {n}, оценка {value}, ошибка {err:.3}");
        }
    }

    /// Независимый oracle для отложенного keyword-HLL: больше одного чанка
    /// уникальных ordinal, повторы и ordinal другого поля не должны
    /// менять регистры и не должны пропускать значения нужного поля.
    #[test]
    fn deferred_keyword_hll_matches_direct_hash_oracle_over_multiple_chunks() {
        let dir =
            std::env::temp_dir().join(format!("svindb-aggs-deferred-hll-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut indexer = crate::indexer::Indexer::typed(
            &dir,
            vec!["left".into(), "right".into()],
            vec![1.0; 2],
            vec![crate::index::Kind::Keyword; 2],
            70_000,
        );
        for i in 0..65_540u32 {
            let term = format!("term{i:05}");
            let foreign = format!("foreign{i:05}");
            indexer.add(&format!("d{i}"), &[&term, &foreign]);
        }
        indexer.flush_now();
        drop(indexer);
        let segments = crate::segments::Segments::load(&dir).expect("индекс");
        assert_eq!(segments.list.len(), 1, "нужен один сегмент для локальных ordinal");
        let ix = segments.list[0].as_ref();
        let aggs = [Agg {
            name: "cardinality".into(),
            kind: Kind::Cardinality { field: 0 },
            over: Over::Docs,
            space: Space::Keyword,
        }];
        let ordinals: Vec<usize> = (0..ix.dict.len()).collect();
        assert!(ordinals.len() > MAX_DEFERRED_ORDINALS, "не пересекли границу deferred");
        let mut direct = Box::new([0u8; HLL_M]);
        for ordinal in ordinals.iter().copied().chain(ordinals.iter().copied()) {
            let (field, hash) = ix.dict.hash_at(ordinal).expect("ordinal");
            if field == 0 {
                hll_add(&mut direct, hash as i64);
            }
        }
        for sparse in [false, true] {
            let mut sink = Sink::new(&aggs);
            if sparse {
                // Проверяем fallback без создания многомиллионного словаря.
                sink.deferred_ordinals[0] = Some(DeferredOrdinals::Deferred(HashSet::new()));
            }
            for doc in 0..65_540u32 {
                sink.accept(ix, &aggs, &[0], doc);
                sink.accept(ix, &aggs, &[0], doc);
            }
            match &sink.deferred_ordinals[0] {
                Some(DeferredOrdinals::Dense(bits)) => {
                    assert!(!sparse);
                    assert!(bits.len() * 8 <= 512 * 1024);
                }
                Some(DeferredOrdinals::Deferred(values)) => {
                    assert!(sparse);
                    assert!(values.len() < MAX_DEFERRED_ORDINALS);
                }
                _ => panic!("collector не начал работу"),
            }
            // Чужие строки отличаются, иначе ошибочный hash был бы незаметен.
            for ordinal in 0..ix.dict.len() {
                if ix.dict.hash_at(ordinal).unwrap().0 == 1 {
                    keyword_eat(
                        &mut sink.accs[0],
                        ix,
                        0,
                        ordinal as i64,
                        &mut sink.deferred_ordinals[0],
                        &mut sink.keyword_hashes[0],
                        None,
                    );
                }
            }
            sink.seal(ix, &aggs, &[0]);
            let Acc::Hll(optimized) = &sink.accs[0] else { panic!("не HLL") };
            assert_eq!(optimized, &direct, "HLL разошёлся с oracle, sparse={sparse}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn exact_cardinality_is_exact_and_never_disguised_as_hll() {
        let vals: Vec<Option<i64>> = [1, 2, 2, 3, 3, 3, 10].into_iter().map(Some).collect();
        assert_eq!(
            feed(Kind::CardinalityExact { field: 0 }, &vals),
            Value::ExactCardinality { value: 4 }
        );
    }

    /// Дробное поле считается числами, а не ключами.
    ///
    /// Колонка хранит `i64`, и дробное значение кладётся в неё ключом —
    /// монотонным преобразованием бит. Порядок ключ сохраняет, сложение нет.
    /// Пока агрегации о типе поля не знали, они складывали ключи: сумма
    /// 0.25 + 0.5 + 0.75 + 0.937421 выходила девятнадцатизначным числом.
    #[test]
    fn a_floating_field_is_summed_as_numbers_not_as_keys() {
        let k = Kind::Stats { field: Some(0) };
        let mut acc = Acc::new(&k, Space::Float);
        for x in [0.25f64, 0.5, 0.75, 0.937421] {
            acc.eat(&k, Some(crate::index::float_key(x)));
        }
        let Value::Stats { count, min, max, fsum, .. } = acc.finish(&k) else {
            panic!("не считалка")
        };
        assert_eq!(count, 4);
        let sum = fsum.expect("у дробного поля сумма обязана быть дробной");
        assert!((sum - 2.437421).abs() < 1e-12, "сумма {sum} вместо 2.437421");
        // Наименьшее и наибольшее остаются ключами: порядок у них верен, а
        // переводится только выдача. Сверяем через обратное преобразование.
        assert!((crate::index::float_of(min.unwrap()) - 0.25).abs() < 1e-12);
        assert!((crate::index::float_of(max.unwrap()) - 0.937421).abs() < 1e-12);

        // У целого поля сумма по-прежнему целая — иначе проверка выше говорила
        // бы лишь о том, что сумма всегда дробная.
        let mut acc = Acc::new(&k, Space::Int);
        for v in [1i64, 2, 3] {
            acc.eat(&k, Some(v));
        }
        let Value::Stats { sum, fsum, .. } = acc.finish(&k) else {
            panic!("не считалка")
        };
        assert_eq!(sum, 6);
        assert!(fsum.is_none(), "у целого поля завелась дробная сумма");
    }

    /// Корзина дробной гистограммы считается по значению, а не по ключу.
    ///
    /// Ключи дробных чисел не равномерны: шаг по ним не значит ничего, и сетку
    /// в пространстве ключей нельзя было даже задать.
    #[test]
    fn a_floating_histogram_buckets_by_value() {
        let k = Kind::Histogram {
            field: 0,
            interval: 0,
            offset: 0,
            step: Step::Float { interval: 0.5, offset: 0.0 },
        };
        let mut acc = Acc::new(&k, Space::Float);
        for x in [0.25f64, 0.5, 0.75, 0.937421] {
            acc.eat(&k, Some(crate::index::float_key(x)));
        }
        let Value::Buckets(mut b) = acc.finish(&k) else { panic!("не корзины") };
        b.sort_by_key(|(k, _)| *k);
        let seen: Vec<(f64, u64)> =
            b.iter().map(|(k, n)| (crate::index::float_of(*k), *n)).collect();
        assert_eq!(seen, vec![(0.0, 1), (0.5, 3)], "корзины не по значению");
    }

    /// Календарная корзина стоит на начале месяца, а не через равные отрезки.
    ///
    /// Через високосный год равномерная сетка съезжает: в месяцах разное число
    /// дней, и корзины перестают совпадать с месяцами. Проверка идёт именно
    /// через февраль двадцать четвёртого года.
    #[test]
    fn a_calendar_month_starts_where_the_month_starts() {
        let ms = |t: &str| crate::iso::parse_millis(t).expect(t);
        for (moment, want) in [
            ("2024-01-31T23:59:59Z", "2024-01-01T00:00:00.000Z"),
            ("2024-02-29T12:00:00Z", "2024-02-01T00:00:00.000Z"),
            ("2024-03-01T00:00:00Z", "2024-03-01T00:00:00.000Z"),
        ] {
            let got = Unit::Month.floor(ms(moment));
            assert_eq!(crate::iso::to_text(got), want, "месяц для {moment}");
        }
        assert_eq!(
            crate::iso::to_text(Unit::Year.floor(ms("2024-07-04T00:00:00Z"))),
            "2024-01-01T00:00:00.000Z"
        );
        assert_eq!(
            crate::iso::to_text(Unit::Quarter.floor(ms("2024-05-15T00:00:00Z"))),
            "2024-04-01T00:00:00.000Z"
        );
    }

    /// Неделя начинается с понедельника, а сутки — с полуночи.
    #[test]
    fn a_week_starts_on_monday() {
        let ms = |t: &str| crate::iso::parse_millis(t).expect(t);
        // 2024-01-03 — среда; её неделя началась в понедельник первого.
        assert_eq!(
            crate::iso::to_text(Unit::Week.floor(ms("2024-01-03T15:00:00Z"))),
            "2024-01-01T00:00:00.000Z"
        );
        // А понедельник остаётся собой, а не уезжает на неделю назад.
        assert_eq!(
            crate::iso::to_text(Unit::Week.floor(ms("2024-01-01T00:00:00Z"))),
            "2024-01-01T00:00:00.000Z"
        );
        assert_eq!(
            crate::iso::to_text(Unit::Day.floor(ms("2024-01-03T15:00:00Z"))),
            "2024-01-03T00:00:00.000Z"
        );
    }

    /// Смещение пояса двигает границу суток, а ключ остаётся в UTC.
    #[test]
    fn a_time_zone_moves_the_day_boundary_but_not_the_key_space() {
        let k = Kind::Histogram {
            field: 0,
            interval: 1,
            offset: 0,
            step: Step::Calendar { unit: Unit::Day, tz_ms: 3 * 3_600_000 },
        };
        let mut acc = Acc::new(&k, Space::Int);
        // Двадцать два часа UTC — это уже следующие сутки по московскому
        // времени. Без пояса они попали бы в предыдущую корзину.
        acc.eat(&k, crate::iso::parse_millis("2024-01-01T22:00:00Z"));
        let Value::Buckets(b) = acc.finish(&k) else { panic!("не корзины") };
        assert_eq!(b.len(), 1);
        assert_eq!(
            crate::iso::to_text(b[0].0),
            "2024-01-01T21:00:00.000Z",
            "граница суток не сдвинулась на пояс"
        );

        // Без пояса та же метка попадает в начало первого января по UTC —
        // иначе проверка выше не отличала бы сдвиг от его отсутствия.
        let k0 = Kind::Histogram {
            field: 0,
            interval: 1,
            offset: 0,
            step: Step::Calendar { unit: Unit::Day, tz_ms: 0 },
        };
        let mut acc = Acc::new(&k0, Space::Int);
        acc.eat(&k0, crate::iso::parse_millis("2024-01-01T22:00:00Z"));
        let Value::Buckets(b) = acc.finish(&k0) else { panic!("не корзины") };
        assert_eq!(crate::iso::to_text(b[0].0), "2024-01-01T00:00:00.000Z");
    }
}
