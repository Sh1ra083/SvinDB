//! Счётчик памяти для тестов и диагностической `alloc-stats` сборки.
//!
//! ## Зачем
//!
//! Порог сброса буфера приёма считается оценкой: точная мера требовала бы
//! обходить хеш-карту постингов на каждом документе, то есть тратить на учёт
//! больше, чем на работу. Оценку надо чем-то поверять, иначе она однажды
//! разойдётся с действительностью — молча, как разошёлся прежний счёт по длине
//! текста.
//!
//! Сравнивать с `RSS` нельзя: распределитель отдаёт страницы операционной
//! системе когда сочтёт нужным, и проверка мигала бы от прогона к прогону.
//! Считать надо то, что запрошено у распределителя.
//!
//! В обычный production-бинарник атомарный счёт общего числа аллокаций не
//! попадает. Он включается только feature `alloc-stats` для коротких
//! профилировочных прогонов, где цена двух relaxed atomic на выделение явно
//! принята измерительным контрактом.
//!
//! ## Почему счётчик потоко-местный
//!
//! Проверки идут параллельно в одном процессе, и общий счётчик показывал бы
//! чужие выделения. Потоко-местный видит ровно то, что выделила эта нить, —
//! а `watch` и вызывает замыкание на ней же.
//!
//! ## Почему `const`-инициализация
//!
//! Ленивая инициализация потоко-местной ячейки сама выделяет память, а это
//! вызов распределителя изнутри распределителя. `const { Cell::new(0) }`
//! раскладывается статически и такой рекурсии не создаёт.

#[cfg(test)]
pub use imp::watch;

#[cfg(test)]
pub use imp::peak;

#[cfg(any(test, feature = "alloc-stats"))]
pub use imp::Counting;

#[cfg(any(test, feature = "alloc-stats"))]
pub use imp::mark;

#[cfg(feature = "alloc-stats")]
pub use imp::peak_of;

#[cfg(any(test, feature = "alloc-stats"))]
pub use imp::since;

#[cfg(feature = "alloc-stats")]
pub use imp::totals;

#[cfg(any(test, feature = "alloc-stats"))]
mod imp {
    use std::alloc::{GlobalAlloc, Layout, System};
    #[cfg(any(test, feature = "alloc-stats"))]
    use std::cell::Cell;
    #[cfg(feature = "alloc-stats")]
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(feature = "alloc-stats")]
    static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
    #[cfg(feature = "alloc-stats")]
    static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

    #[cfg(feature = "alloc-stats")]
    #[inline(always)]
    fn requested(bytes: usize) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[cfg(not(feature = "alloc-stats"))]
    #[inline(always)]
    fn requested(_bytes: usize) {}

    #[cfg(feature = "alloc-stats")]
    pub fn totals() -> (u64, u64) {
        (ALLOCATIONS.load(Ordering::Relaxed), ALLOCATED_BYTES.load(Ordering::Relaxed))
    }

    /// Занятое сейчас и наибольшее занятое с последней отсечки.
    ///
    /// Одной ячейкой, а не двумя: обращение к потоко-местной ячейке стоит
    /// дороже самого счёта, и на каждом выделении их должно быть одно.
    #[cfg(any(test, feature = "alloc-stats"))]
    #[derive(Clone, Copy)]
    struct Counters {
        /// Сколько байт эта нить держит выделенными. Может уходить в минус:
        /// нить вправе освободить то, что выделила другая.
        live: isize,
        /// Наибольшее значение `live` с последней отсечки [`peak`].
        high: isize,
    }

    #[cfg(any(test, feature = "alloc-stats"))]
    thread_local! {
        static C: Cell<Counters> = const { Cell::new(Counters { live: 0, high: 0 }) };
    }

    #[cfg(any(test, feature = "alloc-stats"))]
    #[inline(always)]
    fn add(by: isize) {
        // `try_with`, а не `with`: при разрушении потоко-местных ячеек в конце
        // нити обращение к уже разрушенной паникует, а паника в
        // распределителе — прерывание процесса.
        let _ = C.try_with(|c| {
            let mut v = c.get();
            v.live += by;
            if v.live > v.high {
                v.high = v.live;
            }
            c.set(v);
        });
    }

    #[cfg(not(any(test, feature = "alloc-stats")))]
    #[inline(always)]
    fn add(_by: isize) {}

    #[cfg(test)]
    fn now() -> isize {
        C.try_with(|c| c.get().live).unwrap_or(0)
    }

    /// Сколько байт прибавилось за время работы замыкания.
    ///
    /// Прибавилось, а не выделено всего: временные буферы, которые замыкание
    /// освободило, в счёт не идут — как и не идут они в след, оставленный им в
    /// памяти.
    #[cfg(test)]
    pub fn watch<T>(f: impl FnOnce() -> T) -> usize {
        let before = now();
        let out = f();
        let grew = now() - before;
        drop(out);
        grew.max(0) as usize
    }

    /// Ставит отсечку: с этого мига высшая отметка считается заново.
    ///
    /// ## Зачем это отдельно от [`peak`]
    ///
    /// `peak` меряет замыкание целиком и вложенных отсечек не терпит: внутренняя
    /// сбросила бы отметку внешней. Слияние же надо мерить **по фазам**, а
    /// фазы — это не замыкания, а участки одной длинной функции: списки,
    /// содержимое, колонки. Отсечка с явным основанием даёт то же измерение
    /// без вложенности — каждая фаза сама говорит, сколько держала.
    ///
    /// Отдаёт занятое сейчас; его же надо передать в [`since`].
    pub fn mark() -> isize {
        C.try_with(|c| {
            let mut v = c.get();
            v.high = v.live;
            c.set(v);
            v.live
        })
        .unwrap_or(0)
    }

    /// Сколько держалось сверх основания с последней отсечки.
    pub fn since(base: isize) -> usize {
        let high = C.try_with(|c| c.get().high).unwrap_or(0);
        (high - base).max(0) as usize
    }

    /// Наибольшее, что работа держала занятым, — для профилировочной сборки.
    ///
    /// ## Зачем то же самое, что и `peak`, но не под `cfg(test)`
    ///
    /// Оценка расхода слияния — обещание, и до сих пор поверить его можно было
    /// только тестом на игрушечных корпусах. Обещание же даётся боевому узлу и
    /// на боевых сегментах, где схема шире, мёртвых больше, а потоков десятки.
    /// Разойдись оценка с действительностью там — узнать об этом было бы не от
    /// кого: узел просто снимался бы ядром.
    ///
    /// Поэтому в сборке `alloc-stats` та же отсечка доступна наружу, и
    /// обслуживание меряет ею каждое своё слияние. Отношение «оценка к
    /// измеренному» уходит в метрику, и ворота выпуска смотрят на неё: хоть
    /// один отсчёт ниже единицы означает, что оценка перестала быть мажорантой.
    #[cfg(feature = "alloc-stats")]
    pub fn peak_of<T>(f: impl FnOnce() -> T) -> usize {
        let before = C
            .try_with(|c| {
                let mut v = c.get();
                v.high = v.live;
                c.set(v);
                v.live
            })
            .unwrap_or(0);
        let out = f();
        drop(out);
        let high = C.try_with(|c| c.get().high).unwrap_or(0);
        (high - before).max(0) as usize
    }

    /// Наибольшее, что замыкание держало занятым сверх уже занятого.
    ///
    /// ## Чем отличается от [`watch`]
    ///
    /// `watch` меряет **след** — то, что осталось. У слияния след нулевой: оно
    /// всё освобождает и отдаёт наружу файлы. Стоило же оно ровно тем, что
    /// держало *в середине*, и увидеть это может только высшая отметка.
    ///
    /// ## Чего он не видит
    ///
    /// Счётчик потоко-местный (разбор — в заголовке файла), поэтому выделения
    /// рабочих нитей, которые заводит вызванное, сюда не попадают. Для слияния
    /// это ровно то, что нужно: все четыре величины, линейные по размеру
    /// сегмента, жили на его собственной нити.
    ///
    /// Вложенных отсечек нет: внутренняя сбросила бы отметку внешней.
    #[cfg(test)]
    pub fn peak<T>(f: impl FnOnce() -> T) -> usize {
        let before = C
            .try_with(|c| {
                let mut v = c.get();
                v.high = v.live;
                c.set(v);
                v.live
            })
            .unwrap_or(0);
        let out = f();
        drop(out);
        let high = C.try_with(|c| c.get().high).unwrap_or(0);
        (high - before).max(0) as usize
    }

    pub struct Counting;

    // SAFETY: каждое действие делегируется `System`, который эти требования и
    // соблюдает; счётчик их не нарушает — он не трогает саму память.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
                add(l.size() as isize);
                requested(l.size());
            }
            p
        }

        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(l) };
            if !p.is_null() {
                add(l.size() as isize);
                requested(l.size());
            }
            p
        }

        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            add(-(l.size() as isize));
            unsafe { System.dealloc(p, l) }
        }

        /// Своя, а не умолчание типажа.
        ///
        /// Умолчание расписывает перевыделение в «выделить, скопировать,
        /// освободить», и рост любого вектора переставал бы происходить на
        /// месте. Счёт от этого не пострадал бы, а вот прогон проверок стал бы
        /// заметно медленнее — на ровном месте.
        unsafe fn realloc(&self, p: *mut u8, l: Layout, to: usize) -> *mut u8 {
            let out = unsafe { System.realloc(p, l, to) };
            if !out.is_null() {
                add(to as isize - l.size() as isize);
                requested(to);
            }
            out
        }
    }
}
