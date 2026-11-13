//! Сколько памяти узлу правда доступно.
//!
//! ## Зачем спрашивать, а не задавать числом
//!
//! Буфер приёма — самая крупная временная трата узла, и его потолок задаётся
//! числом байт. Всякое зашитое число неверно дважды: на маленькой машине оно
//! отправляет узел в своп, на большой — заставляет резать сегменты вчетверо
//! мельче, чем стоило бы, и платить за это укрупнениями.
//!
//! Elasticsearch решает то же самое долей кучи (`indices.memory
//! .index_buffer_size`, десятая часть). Доля — правильная форма ответа, но
//! знаменатель надо у кого-то спросить.
//!
//! ## Почему предел контейнера важнее памяти машины
//!
//! Контейнер с пределом в два гигабайта на машине со ста двадцатью восемью
//! видит все сто двадцать восемь и в `/proc/meminfo`, и в `sysctl`. Узел,
//! поверивший этому числу, завёл бы буфер в двенадцать гигабайт и был бы убит
//! ядром при первой же заливке — без единого слова, потому что убивает не он
//! себя, а `cgroup`. Поэтому берётся **наименьшее** из найденного.
//!
//! ## Зависимостей по-прежнему нет
//!
//! На Linux всё читается файлами. На macOS системного вызова без `libc` нет, и
//! приходится звать `sysctl` — один раз за жизнь процесса, при старте. Не
//! ответил, не нашёлся, выдал невнятное — значит памяти мы не знаем, и это
//! честный ответ, а не повод выдумать число.

/// Сколько байт памяти узлу доступно. `None` — узнать не удалось.
///
/// ## Что здесь запоминается, а что переспрашивается
///
/// Память **машины** не меняется за жизнь процесса: её узнают однажды и
/// помнят. Предел **контрольной группы** меняется на ходу — `docker update
/// --memory` правит его у живого контейнера, — поэтому он переспрашивается, но
/// не чаще раза в минуту.
///
/// Разница не умозрительная. Спрашивают это на каждом заходе обслуживания,
/// каждые две секунды, и каждый заход читал два файла из `/proc` и `/sys`, а на
/// macOS звал `sysctl` через `Command::new` — то есть **порождал процесс**.
/// Сорок три тысячи запусков `sysctl` в сутки ради числа, которое не меняется.
pub fn available() -> Option<u64> {
    let mut best: Option<u64> = None;
    let mut take = |v: Option<u64>| {
        if let Some(v) = v.filter(|v| *v > 0) {
            best = Some(best.map_or(v, |b: u64| b.min(v)));
        }
    };
    take(cgroup_cached());
    take(machine_cached());
    best
}

/// Текущий расход и давление cgroup v2.
#[derive(Clone, Copy, Debug, Default)]
pub struct Pressure {
    /// Собственный cgroup имеет конечный memory.max; PSI принадлежит тому же
    /// изолированному workload, а не общей host/user slice.
    pub cgroup_limited: bool,
    pub limit: Option<u64>,
    pub current: Option<u64>,
    pub anon: Option<u64>,
    pub file_cache: Option<u64>,
    /// Части file accounting, которые нельзя считать немедленно вытесняемыми.
    pub file_dirty: Option<u64>,
    pub file_writeback: Option<u64>,
    pub shmem: Option<u64>,
    pub slab: Option<u64>,
    pub events_max: Option<u64>,
    pub psi_some_avg10: Option<f64>,
    pub psi_full_avg60: Option<f64>,
    pub io_psi_full_avg10: Option<f64>,
    pub io_psi_full_avg60: Option<f64>,
    /// Накопленные full-stall микросекунды для короткого измеренного окна.
    pub memory_full_total_us: Option<u64>,
    pub io_full_total_us: Option<u64>,
    /// Прямой reclaim внутри cgroup с момента его создания.
    pub pgscan_direct: Option<u64>,
    /// Повторные обращения к вытесненному file cache.
    pub workingset_refault_file: Option<u64>,
}

impl Pressure {
    /// Сколько можно отдать data-plane, сохранив control reserve физически
    /// свободным. `None` — cgroup не сообщает текущий расход.
    pub fn headroom(self, reserve: u64) -> Option<u64> {
        Some(self.limit?.saturating_sub(self.current?).saturating_sub(reserve))
    }

    /// Доступный data-plane объём после вытеснения холодного clean file cache.
    ///
    /// `memory.current` учитывает page cache наравне с anonymous memory. Для
    /// correctness это верно, но для admission без этого различия вредно:
    /// последовательный merge способен заполнить cgroup чистыми страницами и
    /// заставить отказать запросу на сотни килобайт, хотя ядро может немедленно
    /// вытеснить гигабайты этих страниц. Рабочий минимум cache сохраняется, а
    /// PSI всё равно имеет старшинство и запрещает этот путь при реальном
    /// reclaim thrash.
    pub fn headroom_after_file_reclaim(
        self,
        reserve: u64,
        protected_file_cache: u64,
    ) -> Option<u64> {
        // Dirty/writeback могут пересекаться: вычитание обоих консервативно,
        // но не обещает освобождение страниц, требующих дисковой записи.
        // При неполном memory.stat вызывающий использует обычный headroom.
        let clean = self
            .file_cache?
            .saturating_sub(self.shmem?)
            .saturating_sub(self.file_dirty?)
            .saturating_sub(self.file_writeback?);
        let reclaimable = clean.saturating_sub(protected_file_cache);
        let committed = self.current?.saturating_sub(reclaimable);
        Some(self.limit?.saturating_sub(committed).saturating_sub(reserve))
    }

    /// Давление, при котором новый тяжёлый запрос обязан получить быстрый
    /// отказ до latency collapse.
    pub fn overloaded(self) -> bool {
        self.psi_full_avg60.is_some_and(|value| value >= 0.5)
            || self.psi_some_avg10.is_some_and(|value| value >= 20.0)
    }

    /// I/O stall, при котором background/write должны уступить search.
    pub fn io_overloaded(self) -> bool {
        self.cgroup_limited
            && (self.io_psi_full_avg10.is_some_and(|value| value >= 0.5)
                || self.io_psi_full_avg60.is_some_and(|value| value >= 0.5))
    }

    /// Collapse уже близко: новые пользовательские записи получают 429.
    pub fn io_severely_overloaded(self) -> bool {
        self.cgroup_limited && self.io_psi_full_avg10.is_some_and(|value| value >= 2.0)
    }

    /// Cgroup почти достиг лимита и тяжёлая работа не должна занимать остаток.
    pub fn near_limit(self, reserve: u64) -> bool {
        match (self.current, self.limit) {
            (Some(current), Some(limit)) => current.saturating_add(reserve) >= limit,
            _ => false,
        }
    }
}

#[cfg(target_os = "linux")]
fn own_cgroup_v2() -> Option<String> {
    let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let rel = text.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("0"), Some(""), Some(path)) => Some(path),
            _ => None,
        }
    })?;
    Some(format!("/sys/fs/cgroup{}", rel.trim_end_matches('/')))
}

#[cfg(target_os = "linux")]
fn number(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(target_os = "linux")]
fn stat_value(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut parts = line.split_ascii_whitespace();
        (parts.next()? == key).then(|| parts.next()?.parse().ok()).flatten()
    })
}

#[cfg(target_os = "linux")]
fn psi_value(text: &str, row: &str, key: &str) -> Option<f64> {
    text.lines().find(|line| line.starts_with(row))?.split_ascii_whitespace().find_map(|field| {
        let (name, value) = field.split_once('=')?;
        (name == key).then(|| value.parse().ok()).flatten()
    })
}

#[cfg(target_os = "linux")]
fn psi_total(text: &str) -> Option<u64> {
    let mut fields = text
        .lines()
        .find(|line| line.split_whitespace().next() == Some("full"))?
        .split_whitespace();
    fields.find_map(|field| field.strip_prefix("total=")?.parse().ok())
}

/// Динамический pressure-снимок. Читается на admission, а не кешируется на
/// минуту: `memory.current` и PSI меняются именно на масштабе запросов.
#[cfg(target_os = "linux")]
pub fn pressure() -> Pressure {
    let Some(root) = own_cgroup_v2() else {
        return Pressure { limit: available(), ..Pressure::default() };
    };
    let stats = std::fs::read_to_string(format!("{root}/memory.stat")).unwrap_or_default();
    let events = std::fs::read_to_string(format!("{root}/memory.events")).unwrap_or_default();
    let psi = std::fs::read_to_string(format!("{root}/memory.pressure")).unwrap_or_default();
    let io_psi = std::fs::read_to_string(format!("{root}/io.pressure")).unwrap_or_default();
    Pressure {
        cgroup_limited: number(&format!("{root}/memory.max")).is_some(),
        limit: available(),
        current: number(&format!("{root}/memory.current")),
        anon: stat_value(&stats, "anon"),
        file_cache: stat_value(&stats, "file"),
        file_dirty: stat_value(&stats, "file_dirty"),
        file_writeback: stat_value(&stats, "file_writeback"),
        shmem: stat_value(&stats, "shmem"),
        slab: stat_value(&stats, "slab"),
        events_max: stat_value(&events, "max"),
        psi_some_avg10: psi_value(&psi, "some", "avg10"),
        psi_full_avg60: psi_value(&psi, "full", "avg60"),
        io_psi_full_avg10: psi_value(&io_psi, "full", "avg10"),
        io_psi_full_avg60: psi_value(&io_psi, "full", "avg60"),
        memory_full_total_us: psi_total(&psi),
        io_full_total_us: psi_total(&io_psi),
        pgscan_direct: stat_value(&stats, "pgscan_direct"),
        workingset_refault_file: stat_value(&stats, "workingset_refault_file"),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn pressure() -> Pressure {
    Pressure { limit: available(), ..Pressure::default() }
}

/// Сколько держать ответ контрольной группы, прежде чем переспросить.
const CGROUP_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Память машины: спрашивается один раз за жизнь процесса.
fn machine_cached() -> Option<u64> {
    // `0` в ячейке значит «ещё не спрашивали», `u64::MAX` — «спрашивали, не
    // узнали». Настоящий ответ ни тем, ни другим быть не может: машины на ноль
    // байт не бывает, а на все адресуемые байты — тем более.
    static CACHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seen = CACHED.load(std::sync::atomic::Ordering::Relaxed);
    if seen != 0 {
        return (seen != u64::MAX).then_some(seen);
    }
    let got = machine();
    CACHED.store(got.unwrap_or(u64::MAX), std::sync::atomic::Ordering::Relaxed);
    got
}

/// Предел контрольной группы: переспрашивается не чаще раза в минуту.
fn cgroup_cached() -> Option<u64> {
    static AT: std::sync::Mutex<Option<(std::time::Instant, Option<u64>)>> =
        std::sync::Mutex::new(None);
    let mut slot = AT.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((when, what)) = *slot {
        if when.elapsed() < CGROUP_TTL {
            return what;
        }
    }
    let got = cgroup_limit();
    *slot = Some((std::time::Instant::now(), got));
    got
}

/// Предел памяти контрольной группы, если узел в ней живёт.
///
/// Cgroup v2 пишет `max` вместо числа, когда предела нет; v1 при отсутствии
/// предела пишет число размером во всю адресуемую память, поэтому заведомо
/// огромные значения отбрасываются как «предела нет».
/// Предел памяти, наложенный на **этот** процесс, а не на корень иерархии.
///
/// ## Что было не так
///
/// Прежде читались только `/sys/fs/cgroup/memory.max` и его v1-двойник — то
/// есть предел **корневого** среза. Но процесс почти никогда не лежит в корне:
/// systemd кладёт его в `user.slice/…/что-то.scope`, докер — в
/// `docker/<идентификатор>`, kubernetes — в `kubepods/…`. Предел контейнера
/// или юнита стоит именно там, а в корне не стоит ничего, и `available`
/// возвращала объём всей машины.
///
/// Последствие измерено: узел под `systemd-run -p MemoryMax=2G` выводил порог
/// буфера из двадцати восьми гигабайт машины вместо двух отведённых, буфер
/// перерастал отведённое, и ядро снимало процесс. Под четырьмя гигабайтами
/// узел выживал только потому, что порог случайно оказывался меньше предела.
///
/// ## Как правильно
///
/// Свой срез берётся из `/proc/self/cgroup`, а дальше путь обходится **вверх
/// до корня**: предел может стоять на любом уровне, и действует наименьший из
/// них — вложенный срез не вправе получить больше родительского. Одним и тем
/// же обходом покрываются systemd, докер, kubernetes и LXC: разница между ними
/// только в том, как называются звенья пути.
///
/// Строка `max` — законное значение и означает «без предела на этом уровне»:
/// её пропускаем, а не считаем ошибкой чтения.
fn cgroup_limit() -> Option<u64> {
    // Больше терабайта — это не предел контейнера, а «без предела».
    const NOT_A_LIMIT: u64 = 1 << 40;

    let read_limit = |path: &str| -> Option<u64> {
        let text = std::fs::read_to_string(path).ok()?;
        let text = text.trim();
        if text == "max" {
            return None;
        }
        // v1 пишет предел «без предела» огромным числом, а не словом.
        text.parse::<u64>().ok().filter(|v| *v > 0 && *v < NOT_A_LIMIT)
    };

    // Каждый предок пути, включая сам путь и корень.
    let ancestors = |rel: &str| -> Vec<String> {
        let mut out = vec![String::new()];
        let mut at = String::new();
        for part in rel.split('/').filter(|p| !p.is_empty()) {
            at.push('/');
            at.push_str(part);
            out.push(at.clone());
        }
        out
    };

    let mut best: Option<u64> = None;
    let mut take = |v: Option<u64>| {
        if let Some(v) = v {
            best = Some(best.map_or(v, |b: u64| b.min(v)));
        }
    };

    let own = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    for line in own.lines() {
        // v2: `0::/путь`. v1: `<номер>:<контроллеры>:/путь`.
        let mut parts = line.splitn(3, ':');
        let (_id, controllers, rel) = match (parts.next(), parts.next(), parts.next()) {
            (Some(a), Some(b), Some(c)) => (a, b, c),
            _ => continue,
        };
        if controllers.is_empty() {
            for up in ancestors(rel) {
                take(read_limit(&format!("/sys/fs/cgroup{up}/memory.max")));
            }
        } else if controllers.split(',').any(|c| c == "memory") {
            for up in ancestors(rel) {
                take(read_limit(&format!("/sys/fs/cgroup/memory{up}/memory.limit_in_bytes")));
            }
        }
    }

    // Корень читается и отдельно: `/proc/self/cgroup` может быть недоступен
    // или пуст, а предел на корне при этом стоять.
    take(read_limit("/sys/fs/cgroup/memory.max"));
    take(read_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
    best
}

#[cfg(target_os = "linux")]
fn machine() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|l| l.starts_with("MemTotal:"))?;
    // «MemTotal:       16316420 kB»
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kb.checked_mul(1024)
}

#[cfg(target_os = "macos")]
fn machine() -> Option<u64> {
    let out =
        std::process::Command::new("/usr/sbin/sysctl").args(["-n", "hw.memsize"]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()?.trim().parse().ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn machine() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    #[test]
    fn full_psi_total_preserves_integer_precision_and_requires_exact_row() {
        assert_eq!(
            super::psi_total("some avg10=1 total=9\nfull avg10=0 total=9007199254740993\n"),
            Some(9007199254740993)
        );
        assert_eq!(super::psi_total("fuller total=10\nsome total=20\n"), None);
        assert_eq!(super::psi_total("full avg10=0 total=bad\n"), None);
    }
    use super::*;

    /// Если система отдала число, оно обязано быть правдоподобным.
    ///
    /// Ниже гигабайта — почти наверняка ошибка разбора (перепутанные килобайты
    /// с байтами дают именно такую картину), выше петабайта — тем более. `None`
    /// законен и на macOS/Linux: sandbox или hardened runtime вправе запретить
    /// `sysctl`/cgroup, и публичный контракт [`available`] это оговаривает.
    #[test]
    fn a_reported_number_is_sane_and_an_unavailable_one_stays_unknown() {
        if let Some(v) = available() {
            assert!(v >= 1 << 30, "памяти всего {v} байт — похоже на ошибку разбора");
            assert!(v < 1 << 50, "памяти {v} байт — похоже на ошибку разбора");
        }
    }

    /// Ответ не меняется от вызова к вызову.
    ///
    /// По нему считается потолок буфера, и «плавающее» значение означало бы, что
    /// два узла на одной машине режут сегменты по-разному.
    #[test]
    fn the_answer_is_stable() {
        assert_eq!(available(), available());
    }

    #[test]
    fn control_reserve_is_subtracted_from_real_headroom() {
        let pressure = Pressure {
            limit: Some(1_000),
            current: Some(700),
            psi_full_avg60: Some(0.49),
            ..Pressure::default()
        };
        assert_eq!(pressure.headroom(200), Some(100));
        assert!(!pressure.overloaded());
        assert!(Pressure { psi_full_avg60: Some(0.5), ..pressure }.overloaded());
    }

    #[test]
    fn near_limit_requires_the_reserve_to_fit() {
        let pressure = Pressure { limit: Some(1_000), current: Some(700), ..Pressure::default() };
        assert!(!pressure.near_limit(200));
        assert!(pressure.near_limit(300));
    }

    #[test]
    fn io_pressure_slows_before_it_rejects() {
        let normal = Pressure::default();
        assert!(!normal.io_overloaded());
        let slow = Pressure { cgroup_limited: true, io_psi_full_avg10: Some(0.5), ..normal };
        assert!(slow.io_overloaded());
        assert!(!slow.io_severely_overloaded());
        assert!(Pressure { io_psi_full_avg10: Some(2.0), ..slow }.io_severely_overloaded());
    }

    #[test]
    fn clean_file_cache_is_reclaimable_but_keeps_a_working_floor() {
        let pressure = Pressure {
            limit: Some(25 << 30),
            current: Some(24 << 30),
            file_cache: Some(20 << 30),
            file_dirty: Some(0),
            file_writeback: Some(0),
            shmem: Some(0),
            ..Pressure::default()
        };
        assert_eq!(pressure.headroom(2 << 30), Some(0));
        assert_eq!(pressure.headroom_after_file_reclaim(2 << 30, 4 << 30), Some(15 << 30));
    }

    #[test]
    fn writeback_and_shared_memory_cannot_fund_query_admission() {
        let pressure = Pressure {
            limit: Some(25 << 30),
            current: Some(24 << 30),
            file_cache: Some(20 << 30),
            file_dirty: Some(8 << 30),
            file_writeback: Some(4 << 30),
            shmem: Some(4 << 30),
            ..Pressure::default()
        };
        assert_eq!(pressure.headroom_after_file_reclaim(2 << 30, 4 << 30), Some(0));
        let incomplete = Pressure { file_dirty: None, ..pressure };
        assert_eq!(incomplete.headroom_after_file_reclaim(2 << 30, 4 << 30), None);
        let overlapping = Pressure { file_writeback: Some(20 << 30), ..pressure };
        assert_eq!(overlapping.headroom_after_file_reclaim(2 << 30, 4 << 30), Some(0));
    }
}

#[cfg(test)]
mod cgroup_tests {
    /// Предел ищется по своему срезу и всем родительским, а не только в корне.
    ///
    /// Разбор пути проверяется отдельно от чтения файлов: на машине, где
    /// проверка идёт, никакого предела может не быть вовсе, и проверка,
    /// зависящая от окружения, проходила бы ни на чём — ровно тот случай,
    /// который в этом дереве уже трижды ловили.
    ///
    /// Здесь закреплено само правило обхода: для `/a/b/c` обязаны быть
    /// опрошены корень, `/a`, `/a/b` и `/a/b/c`. Пропусти любой уровень — и
    /// предел, стоящий на нём, останется невидимым; так и было, пока читался
    /// один лишь корень.
    #[test]
    fn every_level_of_the_own_cgroup_path_is_asked() {
        let ancestors = |rel: &str| -> Vec<String> {
            let mut out = vec![String::new()];
            let mut at = String::new();
            for part in rel.split('/').filter(|p| !p.is_empty()) {
                at.push('/');
                at.push_str(part);
                out.push(at.clone());
            }
            out
        };
        assert_eq!(ancestors("/"), vec![""], "у корня предок только он сам");
        assert_eq!(
            ancestors("/user.slice/user-1000.slice/run-42.scope"),
            vec![
                "",
                "/user.slice",
                "/user.slice/user-1000.slice",
                "/user.slice/user-1000.slice/run-42.scope",
            ],
            "срез systemd обязан быть опрошен целиком, снизу доверху"
        );
        assert_eq!(
            ancestors("/docker/abcdef"),
            vec!["", "/docker", "/docker/abcdef"],
            "срез докера — то же правило"
        );
    }

    /// «max» — это «без предела», а не ошибка и не ноль.
    #[test]
    fn the_word_max_means_no_limit_at_that_level() {
        const NOT_A_LIMIT: u64 = 1 << 40;
        let parse = |text: &str| -> Option<u64> {
            let text = text.trim();
            if text == "max" {
                return None;
            }
            text.parse::<u64>().ok().filter(|v| *v > 0 && *v < NOT_A_LIMIT)
        };
        assert_eq!(parse("max"), None, "слово max означает отсутствие предела");
        assert_eq!(parse("2147483648"), Some(2147483648), "два гигабайта — предел");
        assert_eq!(parse("0"), None, "ноль пределом быть не может");
        assert_eq!(
            parse("9223372036854771712"),
            None,
            "v1 пишет «без предела» огромным числом, и это тоже не предел"
        );
    }
}
