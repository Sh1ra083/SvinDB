//! Как узел делит свою память между подсистемами.
//!
//! ## Зачем это одно место, а не три
//!
//! Пределы были и раньше, но каждый жил у своего хозяина: приём брал долю под
//! буфер, слияние — свою под окно, поиск — свою под запросы. Каждый предел по
//! отдельности верен; беда в том, что про **сумму** не знал никто. Доли
//! складывались как получится, и получалось больше единицы: узел, у которого ни
//! одна подсистема не нарушила ни одного правила, снимался ядром.
//!
//! Здесь доли объявлены рядом, и рядом же стоит проверка, что они складываются
//! в целое. Разойтись они больше не могут молча: [`Class::SHARES`] обходится
//! тестом, и сумма сверяется с сотней.
//!
//! ## Почему доли, а не числа
//!
//! Зашитое число байт неверно дважды: на узле в четыре гигабайта оно отправит
//! его в своп, на узле в сто двадцать восемь — заставит отказывать работе,
//! которая прекрасно поместилась бы. Доля переносится между узлами; число — нет.
//!
//! ## Почему у управления доля не в процентах
//!
//! Управление — это `/_health`, метрики и тот, кто отвечает `429` и `503`.
//! Ему нужно немного и всегда одинаково: он не растёт ни с индексом, ни с
//! нагрузкой. Зато он обязан работать **именно тогда**, когда всё остальное
//! упёрлось в свои пределы, — иначе узел, отказавшийся от работы по памяти,
//! молча перестаёт отвечать и на вопрос «что с тобой». Поэтому его доля
//! отрезается первой и не одалживается никому.

/// Кому и сколько отведено.
///
/// Порядок вариантов — порядок отрезания: управление первым, остаток делится
/// между остальными.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// `/_health`, метрики, ответчик отказов. Отрезается первым, не одалживается.
    Control,
    /// Буферы приёма: набираемый и отданные писателю.
    Ingest,
    /// Рабочая память запросов: разобранные запросы, корзины, верхушки.
    Query,
    /// Окно укрупнения и уборки.
    Merge,
    /// Снимки и восстановление.
    Snapshot,
}

/// Сколько байт всегда за управлением, сколько бы памяти ни было у узла.
///
/// Не доля: на узле в гигабайт доля дала бы десяток мегабайт, а `/_health`
/// столько же и стоит на узле в сто двадцать восемь гигабайт.
pub const CONTROL_BYTES: u64 = 64 << 20;

/// Резерв латентности для control, point reads, ответа и внезапного reclaim.
///
/// Одних 64 МиБ control-памяти достаточно, чтобы отдать ошибку, но мало, чтобы
/// пережить несколько одновременно освобождаемых mmap/page-cache областей на
/// контейнере в десятки гигабайт. Поэтому data-plane admission оставляет
/// отдельный динамический запас: не меньше 1 ГиБ и 8% доступного cgroup.
pub const MIN_LATENCY_RESERVE: u64 = 1 << 30;

pub fn latency_reserve(limit: u64) -> u64 {
    if limit == 0 {
        return MIN_LATENCY_RESERVE;
    }
    MIN_LATENCY_RESERVE.max(limit.saturating_mul(8) / 100).min(limit / 2)
}

/// Ранний порог для write slowdown и cache relief.
///
/// Reclaim не мгновенный: если начать ровно у latency reserve, за 250-мс цикл
/// фоновой реакции поток успевает коснуться `memory.max`. На больших узлах
/// оставляем двойной запас, на малых не забираем больше четверти cgroup и
/// никогда не опускаемся ниже самого latency reserve.
pub fn pressure_relief_reserve(limit: u64) -> u64 {
    let latency = latency_reserve(limit);
    latency.saturating_mul(2).min(limit / 4).max(latency)
}

/// Доли остатка в сотых. Сумма меньше сотни нарочно — см. [`SLACK_PCT`].
const SHARES: [(Class, u64); 4] =
    [(Class::Ingest, 20), (Class::Query, 35), (Class::Merge, 25), (Class::Snapshot, 10)];

/// Сколько остатка не роздано никому.
///
/// Это не забытые проценты. Всякая оценка расхода — оценка: она мажоранта, но
/// мажоранта приблизительная, и цена промаха у неё несимметрична. Промах вверх
/// — отказ работе, которая поместилась бы. Промах вниз — узел, снятый ядром
/// посреди работы, вместе со всеми соединениями. Десятая часть, не розданная
/// никому, и есть плата за эту несимметричность.
pub const SLACK_PCT: u64 = 10;

/// Сколько байт отведено классу. `0` — памяти узла узнать не удалось.
///
/// ## Почему ноль означает «неизвестно», а не «нисколько»
///
/// Узнать память не удалось — значит делить нечего, и всякое названное число
/// было бы выдумкой. Вызывающий обязан решить это сам: приём в таком случае
/// берёт наименьший осмысленный буфер, слияние — свой запасной предел. Выдать
/// им общее число значило бы выдумать за обоих.
pub fn share(class: Class) -> u64 {
    let Some(ram) = crate::mem::available() else { return 0 };
    if class == Class::Control {
        // Управлению — его столько же на всяком узле, но не больше половины
        // машины: на узле в сто двадцать восемь мегабайт отдать шестьдесят
        // четыре значило бы отдать половину тому, кто ничего не индексирует.
        return CONTROL_BYTES.min(ram / 2);
    }
    let rest = ram.saturating_sub(CONTROL_BYTES.min(ram / 2));
    let pct = SHARES.iter().find(|(c, _)| *c == class).map_or(0, |(_, p)| *p);
    rest / 100 * pct
}

/// Складываются ли доли в целое. Наружу — для проверки и для `/_health`.
pub fn shares_pct() -> Vec<(Class, u64)> {
    SHARES.to_vec()
}

impl Class {
    /// Короткое имя для метрик и отказов.
    pub fn name(self) -> &'static str {
        match self {
            Class::Control => "control",
            Class::Ingest => "ingest",
            Class::Query => "query",
            Class::Merge => "merge",
            Class::Snapshot => "snapshot",
        }
    }
}

/// Счёт занятого по классам, один на узел.
///
/// ## Зачем реестр, если доли уже посчитаны
///
/// Доля отвечает на вопрос «сколько классу отведено». Она ничего не говорит о
/// том, **сколько он занял прямо сейчас**, — а именно это отличает узел,
/// который отказывает с числом, от узла, который молча растёт до смерти.
///
/// ## Почему счёт, а не ожидание
///
/// Ждать освобождения нельзя: ждущий держит уже взятое, и очередь ожидающих
/// сходится в тупик. Отказ немедленный, и у него есть число — сколько просили,
/// сколько занято, сколько отведено. По этому числу оператор решает; по
/// молчанию решить нельзя ничего.
#[derive(Debug)]
pub struct Ledger {
    ingest: crate::budget::Pool,
    query: crate::budget::Pool,
    merge: crate::budget::Pool,
    snapshot: crate::budget::Pool,
}

impl Ledger {
    /// Реестр с долями, выведенными из памяти узла.
    pub fn from_machine() -> Ledger {
        Ledger {
            ingest: crate::budget::Pool::new(share(Class::Ingest) as usize),
            query: crate::budget::Pool::new(share(Class::Query) as usize),
            merge: crate::budget::Pool::new(share(Class::Merge) as usize),
            snapshot: crate::budget::Pool::new(share(Class::Snapshot) as usize),
        }
    }

    /// Счёт своего класса. Управление своего не имеет — и это не упущение:
    /// его доля отрезана заранее и не выдаётся никому, в том числе ему самому.
    pub fn pool(&self, class: Class) -> Option<&crate::budget::Pool> {
        match class {
            Class::Control => None,
            Class::Ingest => Some(&self.ingest),
            Class::Query => Some(&self.query),
            Class::Merge => Some(&self.merge),
            Class::Snapshot => Some(&self.snapshot),
        }
    }

    /// Занятое и отведённое по всем классам — для `/_health` и метрик.
    pub fn used(&self) -> Vec<(Class, usize, usize)> {
        [Class::Ingest, Class::Query, Class::Merge, Class::Snapshot]
            .into_iter()
            .filter_map(|c| self.pool(c).map(|p| (c, p.used(), p.max())))
            .collect()
    }
}

#[cfg(test)]
mod ledger_tests {
    use super::*;
    use std::sync::Arc;

    /// Класс отказывает с числом, а не растёт до смерти узла.
    #[test]
    fn a_class_refuses_with_numbers_instead_of_growing() {
        let led = Ledger {
            ingest: crate::budget::Pool::new(1000),
            query: crate::budget::Pool::new(1000),
            merge: crate::budget::Pool::new(1000),
            snapshot: crate::budget::Pool::new(1000),
        };
        let merge = Arc::new(crate::budget::Pool::new(1000));
        let a = merge.reserve(700).expect("первое");
        let full = merge.reserve(400).expect_err("второе обязано не влезть");
        assert_eq!((full.want, full.used, full.max), (400, 700, 1000));
        drop(a);
        assert_eq!(merge.used(), 0, "место не вернулось");

        // Классы считаются порознь: занятое слиянием не отнимает у поиска.
        assert_eq!(led.pool(Class::Merge).map(|p| p.max()), Some(1000));
        assert_eq!(led.pool(Class::Query).map(|p| p.used()), Some(0));
        // У управления счёта нет: его доля отрезана заранее и не выдаётся.
        assert!(led.pool(Class::Control).is_none(), "управлению завели счёт");
    }

    /// Реестр, выведенный из машины, отдаёт каждому классу его долю.
    #[test]
    fn the_ledger_hands_each_class_its_share() {
        let led = Ledger::from_machine();
        let seen = led.used();
        assert_eq!(seen.len(), 4, "классов в реестре не четыре");
        if crate::mem::available().is_some() {
            for (c, used, max) in seen {
                assert_eq!(used, 0, "класс {c:?} занят на свежем реестре");
                assert_eq!(max as u64, share(c), "класс {c:?} получил не свою долю");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Доли обязаны складываться в сотню вместе с нерозданным запасом.
    ///
    /// Это и есть то, чего не хватало: пределы были у каждого свои, а суммы не
    /// знал никто. Разойдутся — узел, у которого ни одна подсистема не нарушила
    /// ни одного правила, снова окажется снят ядром.
    #[test]
    fn the_shares_add_up_to_the_whole() {
        let sum: u64 = SHARES.iter().map(|(_, p)| *p).sum();
        assert_eq!(sum + SLACK_PCT, 100, "доли {sum}% плюс запас {SLACK_PCT}% — это не целое");
    }

    #[test]
    fn latency_reserve_scales_with_the_cgroup_and_never_takes_everything() {
        assert_eq!(latency_reserve(25 << 30), 2 << 30);
        assert_eq!(latency_reserve(4 << 30), 1 << 30);
        assert_eq!(latency_reserve(512 << 20), 256 << 20);
    }

    #[test]
    fn pressure_relief_starts_before_the_hard_latency_boundary() {
        assert_eq!(pressure_relief_reserve(25 << 30), 4 << 30);
        assert_eq!(pressure_relief_reserve(4 << 30), 1 << 30);
        assert_eq!(pressure_relief_reserve(512 << 20), 256 << 20);
    }

    /// Управление получает своё раньше всех и не остаётся ни с чем.
    #[test]
    fn control_is_served_first_and_never_starved() {
        // На всяком узле, где память вообще известна, управлению достаётся
        // положительное число: иначе узел, упёршийся в пределы, перестаёт
        // отвечать и на вопрос «что с тобой».
        if crate::mem::available().is_some() {
            assert!(share(Class::Control) > 0, "управление осталось без памяти");
            for (c, _) in SHARES {
                assert!(share(c) > 0, "класс {c:?} остался без памяти");
            }
        }
    }

    /// Сумма отведённого не превосходит памяти узла.
    #[test]
    fn what_is_handed_out_does_not_exceed_what_there_is() {
        let Some(ram) = crate::mem::available() else { return };
        let handed: u64 =
            share(Class::Control) + SHARES.iter().map(|(c, _)| share(*c)).sum::<u64>();
        assert!(
            handed <= ram,
            "роздано {handed} Б при {ram} Б у узла — это и есть тот случай, \
             когда все правила соблюдены, а узел снят ядром"
        );
        // И запас не съеден: нерозданное обязано остаться заметным.
        let slack = ram - handed;
        assert!(
            slack * 100 / ram.max(1) >= SLACK_PCT - 1,
            "нерозданного {slack} Б из {ram} Б — запас съеден"
        );
    }
}
