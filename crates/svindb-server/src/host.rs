//! Что процесс тратит: память, время процессора, потоки, место на диске.
//!
//! ## Почему без зависимостей
//!
//! У ядра зависимостей нет вовсе, у сервера их одна. Внести сюда `sysinfo` или
//! `libc` ради шести чисел значило бы разменять это свойство на удобство
//! объявления. Поэтому здесь либо текст, который печатает само ядро
//! (`/proc/self/*`), либо вызов с **фиксированной** раскладкой полей.
//!
//! ## Почему не `getrusage`
//!
//! Он есть на обеих системах и выглядит переносимым, но `struct rusage` начинается
//! с двух `struct timeval`, а `timeval` у macOS и Linux разный: `suseconds_t` там
//! `int32`, здесь `long`. Объявить одну раскладку на обе системы — значит на одной
//! из них читать время процессора из чужих байт и напечатать правдоподобное
//! неверное число. Поэтому у каждой системы свой источник, и оба состоят из полей
//! известной ширины.
//!
//! ## Про свободное место на macOS
//!
//! Оно берётся `statfs`, а не `statvfs`, и это не вкусовщина: у macOS
//! `statvfs` объявляет `fsblkcnt_t` тридцатидвухбитным. На томе больше
//! шестнадцати терабайт счётчик заворачивается, и узел увидел бы почти пустой
//! диск на полном или наоборот. У `statfs` те же поля шестидесятичетырёхбитные,
//! а раскладка зафиксирована суффиксом `$INODE64` и с macOS 10.6 не менялась.
//!
//! Прежде здесь стоял отказ измерять вовсе, и он был честнее выдуманного числа.
//! Но у отказа своя цена: отказ по месту на macOS не наступал никогда, то есть
//! обслуживание бралось за слияния, которым некуда лечь.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Снимок расхода процесса. Поля, которых система не сообщает, — `None`.
#[derive(Default, Clone, Copy)]
pub struct Usage {
    /// Занятая физическая память, байты.
    pub resident_bytes: Option<u64>,
    /// Отведённое адресное пространство, байты.
    pub virtual_bytes: Option<u64>,
    /// Время процессора с начала работы, секунды. Пользовательское и системное
    /// вместе: разделять их незачем — тревогу ставят по сумме.
    pub cpu_seconds: Option<f64>,
    /// Сколько потоков у процесса прямо сейчас.
    pub threads: Option<u64>,
}

/// Счётчики `/proc/self/io`. `read_bytes/write_bytes` — именно обращения к
/// носителю после page cache, `rchar/wchar` — логические байты системных
/// вызовов. Обе пары нужны: первая показывает физическую амплификацию, вторая
/// объясняет, сколько работы скрыл кеш.
#[derive(Default, Clone, Copy)]
pub struct Io {
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
    pub rchar: Option<u64>,
    pub wchar: Option<u64>,
}

impl Io {
    pub fn delta(self, before: Io) -> Io {
        let sub = |after: Option<u64>, before: Option<u64>| {
            after.zip(before).map(|(after, before)| after.saturating_sub(before))
        };
        Io {
            read_bytes: sub(self.read_bytes, before.read_bytes),
            write_bytes: sub(self.write_bytes, before.write_bytes),
            rchar: sub(self.rchar, before.rchar),
            wchar: sub(self.wchar, before.wchar),
        }
    }
}

/// Когда процесс начал работать. Считается от первого обращения, а оно
/// происходит при сборке узла, до первого запроса.
static STARTED: Mutex<Option<Instant>> = Mutex::new(None);

/// Отмечает начало работы. Зовётся один раз при подъёме узла.
pub fn mark_start() {
    let mut at = STARTED.lock().expect("отметка старта");
    if at.is_none() {
        *at = Some(Instant::now());
    }
}

/// Сколько узел работает, секунды.
pub fn uptime_seconds() -> f64 {
    STARTED.lock().ok().and_then(|at| *at).map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0)
}

/// Версия сборки. Отдаётся отдельной меткой, а не числом: по ней сборщик
/// отличает узлы разных выпусков в одном наборе.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Точный git revision, переданный воспроизводимой release-сборкой.
pub fn revision() -> &'static str {
    option_env!("SVINDB_BUILD_REVISION").unwrap_or("unknown")
}

/// SOURCE_DATE_EPOCH release-сборки. `unknown` у developer binary.
pub fn build_epoch() -> &'static str {
    option_env!("SOURCE_DATE_EPOCH").unwrap_or("unknown")
}

pub fn target() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

// ───────────────────────────── Linux ─────────────────────────────

#[cfg(target_os = "linux")]
extern "C" {
    fn sysconf(name: i32) -> i64;
    fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
}

#[cfg(target_os = "linux")]
const POSIX_FADV_DONTNEED: i32 = 4;

/// `_SC_CLK_TCK` у glibc. Тиков в секунде — то, в чём `/proc/self/stat` печатает
/// время процессора.
#[cfg(target_os = "linux")]
const SC_CLK_TCK: i32 = 2;

#[cfg(target_os = "linux")]
const SC_PAGESIZE: i32 = 30;

#[cfg(target_os = "linux")]
fn sysconf_or(name: i32, fallback: i64) -> i64 {
    let v = unsafe { sysconf(name) };
    if v > 0 {
        v
    } else {
        fallback
    }
}

#[cfg(target_os = "linux")]
pub fn usage() -> Usage {
    let page = sysconf_or(SC_PAGESIZE, 4096) as u64;
    let ticks = sysconf_or(SC_CLK_TCK, 100) as f64;

    // `/proc/self/statm`: размер и резидентная часть — в страницах.
    let (virtual_bytes, resident_bytes) = match std::fs::read_to_string("/proc/self/statm") {
        Ok(text) => {
            let mut it = text.split_ascii_whitespace();
            let size = it.next().and_then(|x| x.parse::<u64>().ok());
            let rss = it.next().and_then(|x| x.parse::<u64>().ok());
            (size.map(|n| n * page), rss.map(|n| n * page))
        }
        Err(_) => (None, None),
    };

    // `/proc/self/stat`: имя процесса стоит в скобках и **может содержать и
    // пробел, и саму скобку**, поэтому разбор идёт от последней закрывающей, а
    // не от начала строки. Иначе узел, переименованный в `svin db)`, сдвинул бы
    // все поля и напечатал чужое число как время процессора.
    let cpu_seconds = std::fs::read_to_string("/proc/self/stat").ok().and_then(|text| {
        let tail = &text[text.rfind(')')? + 1..];
        let f: Vec<&str> = tail.split_ascii_whitespace().collect();
        // После скобки первым идёт поле 3 (`state`), значит поле N лежит по
        // указателю N-3: `utime` — 14, `stime` — 15.
        let utime: f64 = f.get(11)?.parse().ok()?;
        let stime: f64 = f.get(12)?.parse().ok()?;
        Some((utime + stime) / ticks)
    });

    let threads = std::fs::read_to_string("/proc/self/status").ok().and_then(|text| {
        text.lines()
            .find_map(|l| l.strip_prefix("Threads:"))
            .and_then(|v| v.trim().parse::<u64>().ok())
    });

    Usage { resident_bytes, virtual_bytes, cpu_seconds, threads }
}

#[cfg(target_os = "linux")]
pub fn io() -> Io {
    let Ok(text) = std::fs::read_to_string("/proc/self/io") else { return Io::default() };
    let value = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse::<u64>().ok())
    };
    Io {
        read_bytes: value("read_bytes:"),
        write_bytes: value("write_bytes:"),
        rchar: value("rchar:"),
        wchar: value("wchar:"),
    }
}

/// I/O именно текущего worker-thread.
///
/// `/proc/self/io` смешивает compaction, search и WAL всего процесса. Для job
/// amplification это неприемлемо: параллельный cold-cache search приписывался
/// маленькому merge. Linux экспортирует те же счётчики по TID; на macOS ниже
/// остаётся безопасный пустой снимок.
#[cfg(target_os = "linux")]
pub fn thread_io() -> Io {
    // Ядро само разрешает эту magic-ссылку в TID вызывающего потока. Это
    // переносимо между Linux-архитектурами и не требует держать номера
    // `gettid` syscall для x86_64/aarch64/riscv отдельно.
    let Ok(text) = std::fs::read_to_string("/proc/thread-self/io") else {
        return Io::default();
    };
    let value = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse::<u64>().ok())
    };
    Io {
        read_bytes: value("read_bytes:"),
        write_bytes: value("write_bytes:"),
        rchar: value("rchar:"),
        wchar: value("wchar:"),
    }
}

/// Убирает из page cache чистые immutable-файлы одного сегмента.
///
/// Вызывающий обязан исключить одновременный пользовательский read: совет по
/// source-файлу во время поиска превратил бы полезные страницы в refaults.
/// `live.bin` намеренно пропускается — карта изменяема и может быть dirty.
#[cfg(target_os = "linux")]
pub fn drop_immutable_segment_cache(dir: &str) -> u64 {
    use std::os::fd::AsRawFd;

    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    let mut advised = 0u64;
    for entry in entries.flatten() {
        if entry.file_name() == "live.bin" {
            continue;
        }
        let Ok(metadata) = entry.metadata() else { continue };
        if !metadata.is_file() || metadata.len() == 0 {
            continue;
        }
        let Ok(file) = std::fs::File::open(entry.path()) else { continue };
        if unsafe { posix_fadvise(file.as_raw_fd(), 0, 0, POSIX_FADV_DONTNEED) } == 0 {
            advised = advised.saturating_add(metadata.len());
        }
    }
    advised
}

// ───────────────────────────── macOS ─────────────────────────────

/// Сведения о задаче из `libproc`.
///
/// Раскладка взята у `<libproc.h>` и состоит **только** из полей известной
/// ширины: ни `long`, ни `time_t`, ни `size_t` в ней нет. Именно поэтому её
/// можно объявить здесь, а `struct rusage` — нельзя.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Default)]
struct ProcTaskInfo {
    virtual_size: u64,
    resident_size: u64,
    total_user: u64,
    total_system: u64,
    threads_user: u64,
    threads_system: u64,
    policy: i32,
    faults: i32,
    pageins: i32,
    cow_faults: i32,
    messages_sent: i32,
    messages_received: i32,
    syscalls_mach: i32,
    syscalls_unix: i32,
    csw: i32,
    threadnum: i32,
    numrunning: i32,
    priority: i32,
}

#[cfg(target_os = "macos")]
extern "C" {
    fn proc_pidinfo(pid: i32, flavor: i32, arg: u64, buffer: *mut u8, size: i32) -> i32;
    fn getpid() -> i32;
}

/// `PROC_PIDTASKINFO`.
#[cfg(target_os = "macos")]
const PROC_PIDTASKINFO: i32 = 4;

#[cfg(target_os = "macos")]
pub fn usage() -> Usage {
    let mut info = ProcTaskInfo::default();
    let size = std::mem::size_of::<ProcTaskInfo>() as i32;
    let got = unsafe {
        proc_pidinfo(getpid(), PROC_PIDTASKINFO, 0, (&mut info as *mut ProcTaskInfo).cast(), size)
    };
    // Вызов пишет столько байт, сколько записал. Меньше объявленного — значит
    // ядро отдало другую раскладку, и читать её как эту нельзя.
    if got != size {
        return Usage::default();
    }
    Usage {
        resident_bytes: Some(info.resident_size),
        virtual_bytes: Some(info.virtual_size),
        // Время в наносекундах — так его отдаёт mach.
        cpu_seconds: Some((info.total_user + info.total_system) as f64 / 1e9),
        threads: u64::try_from(info.threadnum).ok(),
    }
}

#[cfg(target_os = "macos")]
pub fn io() -> Io {
    Io::default()
}

#[cfg(target_os = "macos")]
pub fn thread_io() -> Io {
    Io::default()
}

#[cfg(not(target_os = "linux"))]
pub fn drop_immutable_segment_cache(_dir: &str) -> u64 {
    0
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn usage() -> Usage {
    Usage::default()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn io() -> Io {
    Io::default()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn thread_io() -> Io {
    Io::default()
}

// ───────────────────────── Свободное место ─────────────────────────

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Default)]
struct Statvfs {
    bsize: u64,
    frsize: u64,
    blocks: u64,
    bfree: u64,
    bavail: u64,
    files: u64,
    ffree: u64,
    favail: u64,
    fsid: u64,
    flag: u64,
    namemax: u64,
    spare: [i32; 6],
}

#[cfg(target_os = "linux")]
extern "C" {
    fn statvfs(path: *const u8, buf: *mut Statvfs) -> i32;
}

/// Свободные оператору байты на томе с индексом.
///
/// Именно `bavail`, а не `bfree`: у ext4 и xfs часть свободного зарезервирована
/// за root, и узел, работающий не от root, до неё не доберётся. Показать `bfree`
/// значило бы обещать место, которого приёму не дадут.
#[cfg(target_os = "linux")]
pub fn free_bytes(dir: &str) -> Option<u64> {
    let mut path: Vec<u8> = dir.as_bytes().to_vec();
    path.push(0);
    let mut fs = Statvfs::default();
    if unsafe { statvfs(path.as_ptr(), &mut fs) } != 0 {
        return None;
    }
    // `frsize` — единица, в которой считаются блоки; `bsize` это размер блока
    // ввода-вывода, и умножать на него было бы неверно на файловых системах, где
    // они расходятся.
    let unit = if fs.frsize > 0 { fs.frsize } else { fs.bsize };
    unit.checked_mul(fs.bavail)
}

/// Ёмкость тома целиком. Нужна затем, что запас берётся долей от неё.
#[cfg(target_os = "linux")]
pub fn total_bytes(dir: &str) -> Option<u64> {
    let mut path: Vec<u8> = dir.as_bytes().to_vec();
    path.push(0);
    let mut fs = Statvfs::default();
    if unsafe { statvfs(path.as_ptr(), &mut fs) } != 0 {
        return None;
    }
    let unit = if fs.frsize > 0 { fs.frsize } else { fs.bsize };
    unit.checked_mul(fs.blocks)
}

/// Раскладка `statfs` для macOS (`_DARWIN_FEATURE_64_BIT_INODE`).
///
/// Поля после `mntfromname` нам не нужны и оставлены хвостом, который никто не
/// читает: ядро заполняет структуру целиком, а размер её обязан совпадать.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Clone, Copy)]
struct Statfs {
    bsize: u32,
    iosize: i32,
    blocks: u64,
    bfree: u64,
    bavail: u64,
    files: u64,
    ffree: u64,
    fsid: [i32; 2],
    owner: u32,
    ty: u32,
    flags: u32,
    fssubtype: u32,
    fstypename: [u8; 16],
    mntonname: [u8; 1024],
    mntfromname: [u8; 1024],
    reserved: [u32; 8],
}

#[cfg(target_os = "macos")]
extern "C" {
    // Суффикс `$INODE64` есть только на x86_64: там он и отделял новую
    // раскладку от старой, тридцатидвухбитной. На arm64 старой не существовало
    // вовсе, и символ с суффиксом отсутствует — сборка падала бы на компоновке.
    #[cfg_attr(target_arch = "x86_64", link_name = "statfs$INODE64")]
    fn statfs(path: *const u8, buf: *mut Statfs) -> i32;
}

#[cfg(target_os = "macos")]
fn statfs_of(dir: &str) -> Option<Statfs> {
    let mut path: Vec<u8> = dir.as_bytes().to_vec();
    path.push(0);
    // Нулями, а не `Default`: массивы имён монтирования по килобайту, и
    // производного умолчания у них нет.
    // SAFETY: раскладка — числа и байтовые массивы, ни ссылок, ни инвариантов,
    // которые нули нарушили бы; ядро заполнит её целиком.
    let mut fs: Statfs = unsafe { std::mem::zeroed() };
    // SAFETY: путь оканчивается нулём, буфер наш и живёт дольше вызова.
    if unsafe { statfs(path.as_ptr(), &mut fs) } != 0 {
        return None;
    }
    Some(fs)
}

/// Свободные оператору байты. `bavail`, а не `bfree`, — по тому же доводу, что
/// и на Linux: часть свободного зарезервирована и обычному процессу не даётся.
#[cfg(target_os = "macos")]
pub fn free_bytes(dir: &str) -> Option<u64> {
    let fs = statfs_of(dir)?;
    (fs.bsize as u64).checked_mul(fs.bavail)
}

#[cfg(target_os = "macos")]
pub fn total_bytes(dir: &str) -> Option<u64> {
    let fs = statfs_of(dir)?;
    (fs.bsize as u64).checked_mul(fs.blocks)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn free_bytes(_dir: &str) -> Option<u64> {
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn total_bytes(_dir: &str) -> Option<u64> {
    None
}

// ─────────────────────────── Приём по месту ───────────────────────────

/// Неснижаемый запас места: меньше этого узел писать не начинает.
///
/// Абсолютная часть покрывает то, что узлу нужно, чтобы **остаться живым и
/// объяснимым**: дописать журнал, дописать отметку, довести до диска карты
/// живых. Относительная — от ёмкости тома, а не от свободного места на старте:
/// доля от свободного тает вместе с ним и к концу перестаёт быть запасом.
const DISK_FLOOR_ABS: u64 = 1 << 30;
const DISK_FLOOR_PCT: u64 = 5;

/// Запас, назначенный оператором вручную, в мегабайтах. Нет — правило выше.
///
/// ## Зачем рычаг, если правило верное
///
/// Правило верное **в среднем**, а работает оно на конкретной машине. Пять
/// процентов от полутерабайтного тома — двадцать три гигабайта, и узел на
/// разработческом ноутбуке с девятью свободными отказывает от всякой записи,
/// будучи полностью исправным. То же в тесной ячейке под контейнером.
///
/// Отказ при этом верен по правилу и бесполезен по существу: оператор видит
/// узел, который не пишет, знает почему и **не может** ничего сделать. Рычаг
/// возвращает ему решение — ровно как watermark у Elasticsearch, который тоже
/// снимается настройкой, а не пересборкой.
///
/// Переменная среды, а не файл настроек: это обстоятельство машины, а не
/// свойство индекса, и переезжать вместе с ним оно не должно.
fn floor_override() -> Option<u64> {
    std::env::var_os("SVINDB_DISK_FLOOR_MB")
        .and_then(|v| v.to_str().and_then(|s| s.trim().parse::<u64>().ok()))
        .map(|mb| mb << 20)
}

/// Неснижаемый запас для этого тома.
fn floor_of(total: u64) -> u64 {
    floor_override().unwrap_or_else(|| DISK_FLOOR_ABS.max(total / 100 * DISK_FLOOR_PCT))
}

/// Насколько долго ответ о свободном месте считается свежим.
///
/// Спрашивают его на каждой пачке приёма, а `statvfs` — системный вызов с
/// обходом суперблока. Секунда на порядки больше времени, за которое место
/// может уйти на величину запаса, и на порядки меньше времени пачки.
const DISK_FRESH: Duration = Duration::from_secs(1);

static DISK_FREE: AtomicU64 = AtomicU64::new(0);
static DISK_TOTAL: AtomicU64 = AtomicU64::new(0);
static DISK_AT: Mutex<Option<(Instant, String)>> = Mutex::new(None);

/// Свободно и всего, с кешем на [`DISK_FRESH`]. `None` — измерить нечем.
fn disk_now(dir: &str) -> Option<(u64, u64)> {
    let mut at = DISK_AT.lock().unwrap_or_else(|e| e.into_inner());
    // Свежесть — не только по времени, но и **по каталогу**.
    //
    // Прежде ключа не было вовсе, и первый же удачный замер отвечал за все
    // последующие вопросы, о каком бы каталоге ни спрашивали. У узла каталог
    // один, и вреда это не приносило; но всякий, кто спросит про другой путь —
    // снимок, восстановление, соседний индекс, — получал бы чужое число и
    // считал бы его своим. Отказ вызова при этом не наступал бы никогда: кэш
    // отвечал раньше.
    let same = at.as_ref().is_some_and(|(t, seen)| seen == dir && t.elapsed() < DISK_FRESH);
    if !same {
        let free = free_bytes(dir)?;
        let total = total_bytes(dir).unwrap_or(free);
        DISK_FREE.store(free, Ordering::Relaxed);
        DISK_TOTAL.store(total, Ordering::Relaxed);
        *at = Some((Instant::now(), dir.to_string()));
    }
    Some((DISK_FREE.load(Ordering::Relaxed), DISK_TOTAL.load(Ordering::Relaxed)))
}

/// Чем кончилась проверка места перед работой.
pub enum Admit {
    /// Писать можно; сколько свободно и сколько останется после.
    ///
    /// Числа отдаются наружу даже на разрешающем ответе: по ним `/_stats`
    /// показывает запас, и без них «место есть» осталось бы словом.
    Ok {
        #[allow(dead_code)] // читается наблюдаемостью, а не решением
        free: u64,
        #[allow(dead_code)]
        after: u64,
    },
    /// Писать нельзя: столько нужно, столько свободно, таков запас.
    Short { free: u64, need: u64, floor: u64 },
    /// Измерить нечем — на этой системе или на этом томе.
    ///
    /// Отдельный ответ, а не «можно»: узел обязан говорить, что не знает, а не
    /// делать вид, что проверил. Работу при этом не останавливаем: отказать по
    /// неизмеримому значило бы запретить приём там, где он законен.
    Unknown,
}

/// Хватит ли места на работу в `need` байт сверх нынешнего.
///
/// Проверка **до** начала, а не по ходу: `ENOSPC` посреди слияния или сброса
/// оставляет половину работы и половину места, и разобрать это некому. Отказ до
/// начала — это отказ с причиной и числом, после которого узел продолжает жить.
/// Сколько свободно на диске под каталогом. `None` — узнать не удалось.
///
/// Наружу — для обслуживания: планировщик слияний обязан знать, есть ли куда
/// лечь выходу, а спрашивать файловую систему из ядра индекса нечем и незачем.
pub fn disk_free(dir: &str) -> Option<u64> {
    disk_now(dir).map(|(free, _)| free)
}

pub fn admit_disk(dir: &str, need: u64) -> Admit {
    let Some((free, total)) = disk_now(dir) else { return Admit::Unknown };
    let floor = floor_of(total);
    let after = free.saturating_sub(need);
    if after < floor {
        Admit::Short { free, need, floor }
    } else {
        Admit::Ok { free, after }
    }
}

#[cfg(test)]
mod disk_tests {
    use super::*;

    /// Запас считается от **ёмкости**, а не от свободного на старте.
    ///
    /// Доля от свободного тает вместе с ним: на полупустом томе она вдвое
    /// меньше, чем была, а к концу перестаёт быть запасом вовсе — ровно тогда,
    /// когда он и нужен.
    #[test]
    fn the_reserve_is_a_share_of_capacity_not_of_what_is_left() {
        // Порог при ёмкости в сто гигабайт: пять процентов, то есть пять.
        let total = 100u64 << 30;
        let floor = DISK_FLOOR_ABS.max(total / 100 * DISK_FLOOR_PCT);
        assert_eq!(floor, 5 << 30);
        // На маленьком томе действует абсолютная часть: доля от него меньше.
        let small = 4u64 << 30;
        assert_eq!(DISK_FLOOR_ABS.max(small / 100 * DISK_FLOOR_PCT), DISK_FLOOR_ABS);
    }

    /// Неизмеримое место — отдельный ответ, а не «можно».
    ///
    /// Отказать по неизмеримому значило бы запретить приём там, где он законен;
    /// промолчать — сделать вид, что проверили. Узел обязан сказать, что не
    /// знает.
    #[test]
    fn unmeasurable_space_is_said_out_loud_and_does_not_stop_writing() {
        // Каталога нет, значит и `statvfs` по нему не пройдёт.
        let nowhere = "/такого/каталога/нет/никогда";
        assert!(matches!(admit_disk(nowhere, 1), Admit::Unknown));
    }

    /// На настоящем томе запрос обычного размера проходит, а невозможный — нет.
    #[test]
    fn a_normal_write_passes_and_an_impossible_one_does_not() {
        let dir = std::env::temp_dir().to_string_lossy().to_string();
        match admit_disk(&dir, 1 << 20) {
            Admit::Ok { .. } => {}
            // На переполненной машине проверка законно откажет — это не отказ
            // теста, а тот самый случай, ради которого всё написано.
            Admit::Short { .. } => {}
            Admit::Unknown => return,
        }
        // Запрос размером с эксабайт не пройдёт ни на каком томе.
        assert!(
            matches!(admit_disk(&dir, u64::MAX / 2), Admit::Short { .. }),
            "невозможная запись принята"
        );
    }

    /// Свободное место измеряется и похоже на правду.
    ///
    /// Проверка слабая нарочно: точного числа мы не знаем и знать не должны —
    /// его знает ядро. Зато три вещи обязаны выполняться на всякой машине, где
    /// измерение вообще есть: свободного не больше ёмкости, оба числа не нули,
    /// и том больше мегабайта. Ноль здесь означал бы, что раскладка структуры
    /// разошлась с системной, — а это отказ, не заметный ничем другим: узел
    /// просто перестал бы брать работу, считая диск полным.
    #[test]
    fn free_space_is_measured_and_plausible() {
        let dir = std::env::temp_dir().to_string_lossy().to_string();
        let (Some(free), Some(total)) = (free_bytes(&dir), total_bytes(&dir)) else {
            // Система не сообщает — законный случай, и врать не станем.
            return;
        };
        assert!(total > 0, "ёмкость тома нулевая — раскладка структуры разошлась");
        assert!(free > 0, "свободного нуль на временном каталоге — так не бывает");
        assert!(free <= total, "свободного {free} больше ёмкости {total}");
        assert!(total > (1 << 20), "ёмкость {total} Б — это не файловая система");
    }
}

// ─────────────────────────── Место индекса ───────────────────────────

/// Кеш размера **по каталогу**: значение и когда его считали.
///
/// Одно глобальное число было незаметно при договоре «процесс = индекс», но в
/// многoиндексном узле второй индекс десять секунд получал размер первого.
/// Ключ входит в состояние принципиально: метрика обязана описывать выбранный
/// индекс, а не последний индекс, который случайно спросили.
static SIZE_CACHE: std::sync::OnceLock<Mutex<std::collections::HashMap<String, (Instant, u64)>>> =
    std::sync::OnceLock::new();

/// Насколько долго ответ считается свежим.
///
/// Обход каталога стоит `stat` на файл. У сегмента их немного — он собран в
/// один контейнер, — но точку метрик спрашивают и чаще пятнадцати секунд, и не
/// одним сборщиком. Кеш ставит потолок на эту цену.
const SIZE_TTL: Duration = Duration::from_secs(10);

/// Сколько байт занимает каталог индекса: сегменты, журнал, служебные файлы.
///
/// Считается обходом, а не `du`: запускать процесс на каждый сбор метрик
/// дороже самого обхода и добавляет узлу зависимость от внешней программы.
pub fn index_bytes(dir: &str) -> u64 {
    let cache = SIZE_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    if let Some((_, bytes)) =
        cache.lock().expect("кеш размера").get(dir).filter(|(at, _)| at.elapsed() < SIZE_TTL)
    {
        return *bytes;
    }
    let total = walk(std::path::Path::new(dir));
    cache.lock().expect("кеш размера").insert(dir.to_string(), (Instant::now(), total));
    total
}

/// Забывает поколение каталога после hot-delete/create.
///
/// Путь при повторном создании тот же, а каталог уже другой. TTL без явного
/// сброса десять секунд выдавал бы размер удалённого поколения новому индексу.
pub fn forget_index_bytes(dir: &str) {
    if let Some(cache) = SIZE_CACHE.get() {
        cache.lock().expect("кеш размера").remove(dir);
    }
}

/// Сумма длин обычных файлов поддерева.
///
/// Символические ссылки не разыменовываются: снимок кладёт рядом ссылки на
/// сегменты, и пройти по ним значило бы посчитать одни и те же байты дважды.
fn walk(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_symlink() {
            continue;
        }
        if kind.is_dir() {
            total = total.saturating_add(walk(&entry.path()));
        } else if let Ok(meta) = entry.metadata() {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Расход процесса читается, и прочитанное правдоподобно.
    ///
    /// Проверяется не точное число — оно у каждого запуска своё, — а то, что
    /// источник вообще отвечает и не отдаёт ноль там, где ноля быть не может.
    #[test]
    fn the_process_reports_what_it_spends() {
        let u = usage();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let rss = u.resident_bytes.expect("резидентная память не прочлась");
            assert!(rss > 1 << 20, "резидентная память {rss} байт — столько не бывает");
            assert!(rss < 1 << 40, "резидентная память {rss} байт — столько тоже не бывает");
            assert!(u.virtual_bytes.expect("адресное пространство") >= rss);
            // Время процессора может быть и нулевым на очень быстром старте, но
            // само поле обязано читаться.
            assert!(u.cpu_seconds.expect("время процессора") >= 0.0);
            assert!(u.threads.expect("потоки") >= 1);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        assert!(u.resident_bytes.is_none());
    }

    /// Обход каталога считает файлы и не ходит по ссылкам.
    #[test]
    fn the_walk_sums_files_and_skips_links() {
        let dir = std::env::temp_dir().join(format!("svindb-host-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(dir.join("вложенный")).expect("каталог");
        std::fs::write(dir.join("а.bin"), vec![0u8; 1000]).expect("файл");
        std::fs::write(dir.join("вложенный/б.bin"), vec![0u8; 2000]).expect("файл");

        let sum = walk(&dir);
        assert_eq!(sum, 3000, "обход не сложил вложенное");

        // Ссылка на уже посчитанный файл не должна прибавить его второй раз.
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.join("а.bin"), dir.join("ссылка.bin")).expect("ссылка");
            assert_eq!(walk(&dir), 3000, "ссылка посчиталась как файл");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Кеш размера не смешивает два индекса одного процесса.
    #[test]
    fn directory_size_cache_is_keyed_by_index() {
        let base = std::env::temp_dir().join(format!(
            "svindb-host-cache-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let first = base.join("first");
        let second = base.join("second");
        std::fs::create_dir_all(&first).expect("первый каталог");
        std::fs::create_dir_all(&second).expect("второй каталог");
        std::fs::write(first.join("data"), vec![0u8; 17]).expect("первый файл");
        std::fs::write(second.join("data"), vec![0u8; 41]).expect("второй файл");
        assert_eq!(index_bytes(&first.to_string_lossy()), 17);
        assert_eq!(index_bytes(&second.to_string_lossy()), 41);
        assert_eq!(index_bytes(&first.to_string_lossy()), 17, "повтор взял чужой кеш");
        std::fs::remove_dir_all(base).ok();
    }

    /// Свободное место читается там, где система его сообщает.
    ///
    /// Раскладка `struct statvfs` объявлена здесь руками, и ошибка в ней даёт не
    /// отказ, а правдоподобное неверное число — то самое, что заметить нечем.
    /// Поэтому проверяется не «вызов прошёл», а что итог **осмыслен**: он не
    /// ноль, он меньше разумного потолка и он согласуется с тем, что тот же
    /// вызов говорит о корне.
    #[test]
    fn free_space_is_read_where_the_system_tells_it() {
        let tmp = std::env::temp_dir();
        let got = free_bytes(&tmp.to_string_lossy());

        #[cfg(target_os = "linux")]
        {
            let n = got.expect("linux обязан сообщать свободное место");
            assert!(n > 0, "свободного места ноль — столько не бывает у рабочей сборки");
            // Экзабайт: столько не бывает ни у одного тома, на котором это
            // запускают. Такое число означало бы, что поля прочлись со сдвигом.
            assert!(n < 1 << 60, "свободного места {n} байт — раскладка statvfs разъехалась");
            // Несуществующий путь — отказ вызова, а не выдуманное число.
            assert_eq!(free_bytes("/такого-пути-нет-и-не-будет-42"), None);
        }
        #[cfg(target_os = "macos")]
        {
            // macOS теперь тоже меряет — через `statfs`, а не `statvfs`.
            let n = got.expect("macOS обязан сообщать свободное место");
            assert!(n > 0, "свободного места ноль — столько не бывает");
            assert!(n < 1 << 60, "свободного места {n} байт — раскладка statfs разъехалась");
            assert_eq!(free_bytes("/такого-пути-нет-и-не-будет-42"), None);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        assert!(got.is_none(), "где не меряем — честное отсутствие, а не выдуманное число");
    }

    /// Время работы идёт от отметки, а не от нуля.
    #[test]
    fn uptime_runs_from_the_mark() {
        mark_start();
        std::thread::sleep(Duration::from_millis(5));
        assert!(uptime_seconds() > 0.0);
        // Повторная отметка не сбрасывает начало: узел стартует один раз.
        let first = uptime_seconds();
        mark_start();
        assert!(uptime_seconds() >= first);
    }
}
