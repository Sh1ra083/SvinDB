//! Счётчики обхода — чтобы не гадать, куда уходит процессор.

/// Что происходило за запрос.
#[derive(Default, Clone, Copy, Debug)]
pub struct Counters {
    /// Шагов объединяющего курсора вперёд — `next` и `seek`.
    pub word_next: u64,
    pub word_seek: u64,
    pub sift_levels: u64,
    /// Сборов множества форм, стоящих на документе, и узлов в них.
    pub gather_calls: u64,
    pub gather_nodes: u64,
    /// Шагов и прыжков отдельного курсора по списку.
    pub cursor_next: u64,
    pub cursor_seek: u64,
    /// Распаковок блока — единственное место, где читаются сами постинги.
    pub cursor_fill: u64,
    /// Документов, доведённых до скора.
    pub scored: u64,
    /// Блоков, перешагнутых целиком по потолку.
    pub block_skips: u64,
    /// Списков, в которые был раскрыт префикс запроса.
    pub prefix_expansions: u64,
    /// Позиций, действительно прочитанных для фраз и slot-проверок.
    pub positions_read: u64,
    /// Сколько раз каталог идентификаторов ответил на запрос целиком.
    ///
    /// Он производный, и по ответу его не видно **никак** — в этом весь его
    /// смысл. Отличить «каталог помог» от «каталог отсутствует» можно только
    /// счётчиком, и без него сверка «с ним и без него» вырождается в сверку
    /// двух одинаковых проходов.
    pub catalog_used: u64,
    /// Сколько раз `catalog.bin` действительно mmap-ился и строил resident
    /// grid. Для point GET это должно быть один раз на ReadView, а не один раз
    /// на запрос.
    pub catalog_opened: u64,
    /// Документов, предъявленных **полному** обходу — тому, что ничего не
    /// отсекает и обязан увидеть каждое совпадение.
    ///
    /// Меряет ровно одно: сколько кандидатов дошло до проверки. Верхушечный
    /// обход считается `scored`, и путать их нельзя — там документ отбрасывают
    /// по весу, здесь его вообще не взвешивают.
    pub scan_visited: u64,
    /// Физические байты чтения, наблюдённые у worker thread на Linux.
    ///
    /// Это только покрытие конкретного lane: mmap/readahead и writeback могут
    /// остаться за пределами счётчика и должны показываться как process/cgroup
    /// residual, а не приписываться одному запросу.
    pub io_read_bytes: u64,
    /// Физические байты записи, наблюдённые у worker thread на Linux.
    pub io_write_bytes: u64,
}

impl Counters {
    /// Во сколько раз шагов обхода больше, чем оценённых документов.
    pub fn steps_per_scored(&self) -> f64 {
        if self.scored == 0 {
            0.0
        } else {
            (self.word_next + self.word_seek) as f64 / self.scored as f64
        }
    }
}

#[cfg(feature = "stats")]
mod imp {
    use super::Counters;
    use std::cell::Cell;

    macro_rules! counters {
        ($($name:ident),* $(,)?) => {
            struct Cells { $($name: Cell<u64>),* }
            thread_local! {
                static C: Cells = Cells { $($name: Cell::new(0)),* };
            }
            $(
                #[inline(always)]
                pub fn $name(by: u64) {
                    C.with(|c| c.$name.set(c.$name.get() + by));
                }
            )*
            /// Снимок счётчиков потока с обнулением.
            pub fn take() -> Counters {
                C.with(|c| {
                    let out = Counters { $($name: c.$name.get()),* };
                    $(c.$name.set(0);)*
                    out
                })
            }
            /// Приписывает измеренную работу завершённых lanes вызывающему запросу.
            pub fn add(other: Counters) {
                C.with(|c| { $(c.$name.set(c.$name.get().saturating_add(other.$name));)* });
            }
        };
    }

    counters!(
        word_next,
        word_seek,
        sift_levels,
        gather_calls,
        gather_nodes,
        cursor_next,
        cursor_seek,
        cursor_fill,
        scored,
        block_skips,
        prefix_expansions,
        positions_read,
        scan_visited,
        catalog_used,
        catalog_opened,
        io_read_bytes,
        io_write_bytes,
    );
}

#[cfg(not(feature = "stats"))]
mod imp {
    use super::Counters;

    macro_rules! nothing {
        ($($name:ident),* $(,)?) => {
            $(
                #[inline(always)]
                pub fn $name(_by: u64) {}
            )*
        };
    }

    nothing!(
        word_next,
        word_seek,
        sift_levels,
        gather_calls,
        gather_nodes,
        cursor_next,
        cursor_seek,
        cursor_fill,
        scored,
        block_skips,
        prefix_expansions,
        positions_read,
        scan_visited,
        catalog_used,
        catalog_opened,
        io_read_bytes,
        io_write_bytes,
    );

    /// Без счётчиков снимок всегда пуст — и это видно по нулям, а не молча.
    pub fn take() -> Counters {
        Counters::default()
    }

    #[inline(always)]
    pub fn add(_other: Counters) {}
}

pub use imp::*;

/// Собраны ли счётчики в этой сборке.
pub const ENABLED: bool = cfg!(feature = "stats");
