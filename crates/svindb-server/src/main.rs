//! SvinDB — движок полнотекстового поиска.

mod auth;
mod bulk;
mod catalog;
mod config;
mod coord;
mod cors;
mod es;
mod esbulk;
mod host;
mod http;
mod httpc;
mod intake;
mod mapping;
mod metrics;
mod migration;
mod migration_encoded;
mod migration_preflight;
mod migration_segment;
mod multi;
mod ndjson;
mod nested;
mod pressure;
mod replicate;
mod route;
mod source_cache;
mod wire;

use std::collections::HashMap;
use std::io::Write;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use svindb_core::dict::DEFAULT_EXPANSIONS;
use svindb_core::index::Kind;
use svindb_core::index::{self, Index};
use svindb_core::indexer::Indexer;
use svindb_core::mmap::MapShortfall;
use svindb_core::segments::{Outcome, QuerySpec, Segments, SpecWord, Term};
use svindb_core::stats;
use svindb_core::{bloom, maintain, manifest, merge};

/// Единый флаг мягкой остановки data-plane.
///
/// Signal handler делает только lock-free store. Все операции с сокетами,
/// очередями, WAL и потоками выполняются обычным серверным потоком после того,
/// как nonblocking `accept` увидит флаг.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" {
    fn signal(number: i32, handler: usize) -> usize;
}

#[cfg(unix)]
extern "C" fn request_shutdown(_number: i32) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

fn install_shutdown_signals() {
    SHUTDOWN_REQUESTED.store(false, Ordering::Release);
    #[cfg(unix)]
    unsafe {
        // POSIX SIGINT/SIGTERM. Возврат прежнего обработчика здесь не нужен:
        // процесс устанавливает handlers один раз при старте serve runtime.
        signal(2, request_shutdown as *const () as usize);
        signal(15, request_shutdown as *const () as usize);
    }
}

fn prepare_listener_shutdown(listener: &TcpListener, ephemeral: bool) {
    listener.set_nonblocking(true).unwrap_or_else(|error| {
        eprintln!("слушатель не переведён в режим мягкой остановки: {error}");
        std::process::exit(1);
    });
    if ephemeral {
        // Порт 0 уже поддерживается CLI. Печатается именно адрес удерживаемого
        // listener: стенду больше не нужен racy поиск временно свободного порта.
        println!("SVINDB_LISTEN {}", listener.local_addr().expect("адрес listener"));
        let _ = std::io::stdout().flush();
    }
}

/// Все принятые TCP-соединения, которые надо разбудить при graceful shutdown.
///
/// Signal handler только ставит атомарный флаг. Accept-loop замечает его не
/// позднее своего 20-мс шага и вызывает `shutdown` уже в обычном контексте.
/// Это будит idle keep-alive reader и сообщает disconnect watcher тяжёлого
/// запроса, не выполняя lock/syscall из обработчика сигнала.
#[derive(Default)]
struct ActiveConnections {
    next: AtomicU64,
    sockets: Mutex<Vec<(u64, TcpStream)>>,
}

struct ActiveConnection {
    id: u64,
    owner: Arc<ActiveConnections>,
}

impl ActiveConnections {
    fn register(self: &Arc<Self>, wire: &wire::Wire) -> Option<ActiveConnection> {
        let socket = wire.transport_clone().ok()?;
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.sockets.lock().ok()?.push((id, socket));
        Some(ActiveConnection { id, owner: Arc::clone(self) })
    }

    fn shutdown_all(&self) {
        if let Ok(sockets) = self.sockets.lock() {
            for (_, socket) in sockets.iter() {
                let _ = socket.shutdown(Shutdown::Both);
            }
        }
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        if let Ok(mut sockets) = self.owner.sockets.lock() {
            sockets.retain(|(id, _)| *id != self.id);
        }
    }
}

/// Живой набор сегментов: читатели берут снимок, обновление подменяет целиком.
///
/// Замок держится ровно на время взятия ссылки, а сам поиск идёт уже по
/// снимку — то есть приём новых сегментов не останавливает запросы ни на
/// миллисекунду. Это то же свойство, ради которого сегменты неизменяемы.
type Live = Arc<RwLock<Arc<Segments>>>;

/// Приём и обслуживание внутри одного процесса.
///
/// Ответ «принято» даётся после того, как запись легла на диск и пережила
/// `fsync`. Чтобы это не стоило `fsync` на каждый запрос, ожидающие
/// объединяются: первый из них становится фиксатором и сбрасывает **всё**, что
/// накопилось, пока он к этому шёл; остальные ждут, пока рубеж не догонит их
/// номер.
struct Wal {
    dir: String,
    state: Mutex<WalState>,
    /// Файл отдельным замком от состояния, и это главное в устройстве.
    file: Mutex<WalFile>,
    /// Будит ожидающих, когда рубеж сдвинулся.
    done: Condvar,
    /// Живое окно group commit; меняется через `PUT /_settings`.
    group_commit_ms: AtomicU64,
    metrics: Arc<metrics::Metrics>,
}

struct WalFile {
    handle: std::fs::File,
    /// Сколько в нём уже байт — по этому заводится следующий.
    len: u64,
}

struct WalState {
    /// Номер последней выданной записи.
    seq: u64,
    /// Номер, до которого всё пережило `fsync`.
    synced: u64,
    /// Ещё не записанное.
    buf: Vec<u8>,
    /// Идёт ли сейчас чужая фиксация.
    committing: bool,
    /// Первый отказ записи делает журнал необратимо больным до перезапуска.
    ///
    /// Продолжить после частичного `write_all` нельзя: неизвестно, какая часть
    /// пачки попала в файл. Без липкого отказа следующий успешный `fsync`
    /// объявил бы сохранённым и потерянный промежуток номеров.
    failed: String,
}

/// Состояние bounded-wait публикации.
///
/// Каждый запрос получает свой номер. Фоновый работник может накрыть одним
/// сегментом сразу несколько номеров, но завершённым номер становится только
/// после того, как работник увидел запрос: поздний `/_flush` не вправе принять
/// за свой результат сегмент, начатый до предшествующего ему `_put`.
#[derive(Default)]
struct FlushState {
    requested: u64,
    completed: u64,
    running: bool,
    error: String,
}

/// Место запроса в ограниченной очереди записи.
struct WritePermit<'a> {
    pending: &'a AtomicUsize,
}

impl Drop for WritePermit<'_> {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PutCondition {
    id: String,
    field: String,
    expected: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct PutConflict {
    id: String,
    field: String,
}

/// Документ, разобранный до захвата замка буфера.
///
/// `values` — плоский вид: значения по номерам полей. `None` означает документ
/// с массивами, у которого путь укладки ячейковый.
#[derive(Default)]
struct Prepared {
    values: Option<Vec<String>>,
    /// Слова по номерам полей. Пустой буфер — поле, которое в словарь не идёт.
    words: Vec<svindb_core::analyze::Tokens>,
}

#[derive(Clone, Copy)]
struct WalTicket {
    last: u64,
    records: usize,
}

impl WalTicket {
    /// Номер WAL, соответствующий документу пачки.
    ///
    /// У нового атомарного batch все документы делят один номер; старые
    /// одиночные записи при replay сохраняют прежнюю последовательную схему.
    fn at(self, document: usize) -> u64 {
        if self.records <= 1 {
            self.last
        } else {
            self.last + 1 - self.records as u64 + document as u64
        }
    }

    /// Первый номер, занятый этой пачкой.
    fn first(self) -> u64 {
        self.last + 1 - self.records.max(1) as u64
    }
}

/// Очередь применения: порядок применения равен порядку номеров журнала.
///
/// ## Зачем понадобилась
///
/// Номера раздаёт `Wal::stage` под своим замком, а применяют пачки уже наперегонки:
/// [`Writer::lock_ids`] держит **только свои** полосы, поэтому две пачки с
/// непересекающимися идентификаторами не исключают друг друга ничем. Пачка с
/// бо́льшим номером могла примениться первой — и дальше рушилось сразу двое.
///
/// **Потеря подтверждённого.** `Indexer::flush` уносит в заголовок сегмента
/// `self.seq` — номер последнего *применённого* документа. Применись пачка 6
/// раньше пачки 5, и сегмент объявил бы долговечным номер 6; отметка журнала
/// уезжала на 6, `wal::trim` удалял файл вместе с записью 5, а пачка 5 после
/// этого спокойно применялась в новый буфер и отвечала клиенту «принято». Эти
/// документы не лежали больше нигде: в сегмент они не попали, из журнала их
/// вырезали, буфер жил в памяти. Падение — и подтверждённого нет.
///
/// **Неразрешимость переигрывания.** Пока порядок разорван, запись с номером
/// выше сплошного префикса *может* оказаться в сегменте, а может и не оказаться,
/// и по одному лишь диапазону номеров решить нельзя: пропустим — потеряем,
/// переиграем — получим вторую живую версию.
///
/// ## Что даёт
///
/// Инвариант в одну строку: **применение идёт строго по возрастанию номера**.
/// Отсюда `Indexer::seq` становится сплошным префиксом, а не просто последним
/// применённым, и заголовок сегмента вместе с отметкой журнала начинают
/// означать ровно то, что обещают. Отдельного поля в формате не нужно.
///
/// ## Почему это не сужает пропуск
///
/// Сериализация тут уже была: пачка из пяти тысяч различных идентификаторов
/// занимает практически все 256 полос, то есть работала общим замком. Билет
/// заменяет борьбу за 256 мьютексов одним ожиданием на условной переменной и
/// снимает произвольность порядка. Дорогие стадии (разбор, токенизация,
/// сжатие) из-под него всё равно уедут отдельной работой.
struct Applied {
    /// Сплошной префикс: всё до него включительно применено.
    upto: Mutex<u64>,
    moved: Condvar,
}

/// Место в очереди применения. Освобождается **на любом пути**, включая ошибку.
///
/// Без этого утёкший билет останавливал бы запись навсегда: следующий по номеру
/// ждал бы предшественника, которого уже нет.
struct ApplyTicket<'a> {
    at: &'a Applied,
    last: u64,
}

impl Drop for ApplyTicket<'_> {
    fn drop(&mut self) {
        let mut upto = self.at.upto.lock().unwrap_or_else(|e| e.into_inner());
        *upto = (*upto).max(self.last);
        self.at.moved.notify_all();
    }
}

impl Applied {
    fn new(from: u64) -> Applied {
        Applied { upto: Mutex::new(from), moved: Condvar::new() }
    }

    /// Ждёт, пока применится всё, что стоит в очереди раньше.
    ///
    /// Ожидание с пределом, а не вечное: зависший предшественник обязан быть
    /// назван, а не превратиться в молча вставший узел. Предел щедрый — он
    /// стережёт от дефекта, а не от медленного диска.
    fn claim(&self, t: WalTicket) -> Result<ApplyTicket<'_>, String> {
        let need = t.first().saturating_sub(1);
        let mut upto = self.upto.lock().unwrap_or_else(|e| e.into_inner());
        if *upto < need {
            let (next, timed_out) = self
                .moved
                .wait_timeout_while(upto, APPLY_WAIT, |upto| *upto < need)
                .unwrap_or_else(|e| e.into_inner());
            upto = next;
            if timed_out.timed_out() {
                return Err(format!(
                    "очередь применения встала: запись {} ждёт {}, применено до {}",
                    t.first(),
                    need,
                    *upto
                ));
            }
        }
        drop(upto);
        Ok(ApplyTicket { at: self, last: t.last })
    }

    /// Сплошной префикс — для наблюдаемости.
    fn watermark(&self) -> u64 {
        *self.upto.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Сколько ждать своей очереди применения, прежде чем назвать это застреванием.
const APPLY_WAIT: Duration = Duration::from_secs(120);

/// Насколько отметке журнала позволено отставать от принятого, в записях.
///
/// Каждая неотмеченная запись переигрывается при следующем подъёме, то есть
/// отставание — это прямо время восстановления. Сто тысяч записей это порядка
/// секунд переигрывания на обычной машине; больше — и узел после падения
/// поднимается ощутимо дольше, чем оператор готов ждать, а узнаёт он об этом
/// ровно тогда, когда узнавать поздно.
///
/// Число не подобрано под замер: это назначенный конверт эксплуатации, и
/// названо оно затем, чтобы отказ приходил заранее и с числом.
const MAX_WAL_LAG: u64 = 100_000;

/// Point read ждёт снимок в пределах того же deadline, что и lookup/source.
/// Mutex::lock не позволяет выйти по disconnect во время flush/ingest.
fn lock_mutex_guarded<'a, T>(
    mutex: &'a Mutex<T>,
    request: &svindb_core::work::Guard,
) -> Option<std::sync::MutexGuard<'a, T>> {
    loop {
        if !request.check() {
            return None;
        }
        match mutex.try_lock() {
            Ok(lock) => return Some(lock),
            Err(std::sync::TryLockError::Poisoned(_)) => panic!("буфер приёма повреждён panic"),
            Err(std::sync::TryLockError::WouldBlock) => {
                // Парковка освобождает CPU connection worker до следующей
                // проверки; короткий таймер сохраняет bounded cancellation,
                // когда владелец мьютекса не обязан посылать уведомление.
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
}

/// Взятый буфер приёма, считающий, **сколько его держали**.
///
/// ## Зачем удержание, если есть ожидание
///
/// Ожидание отвечает на вопрос «дрались ли за замок», и на упорядоченном приёме
/// оно всегда близко к нулю: спорить успевают раньше, на билете очереди. Вопрос,
/// от которого зависит потолок приёма, другой — «какую долю времени
/// исключительный участок вообще занят». Ответ даёт только удержание.
///
/// Замер на трёх миллионах: ожидание 0.008 мс на запрос, удержание 41 мс, то
/// есть 91.8% всего окна заливки. Без этого счётчика первое читалось как «замок
/// свободен», а на деле он был занят почти всегда.
struct Held<'a> {
    guard: std::sync::MutexGuard<'a, Indexer>,
    since: Option<Instant>,
    metrics: &'a metrics::Metrics,
    memory_bytes: &'a AtomicU64,
}

struct MutationAccess<'a> {
    _shared: Option<std::sync::RwLockReadGuard<'a, ()>>,
    _exclusive: Option<std::sync::RwLockWriteGuard<'a, ()>>,
}

impl std::ops::Deref for Held<'_> {
    type Target = Indexer;

    fn deref(&self) -> &Indexer {
        &self.guard
    }
}

impl std::ops::DerefMut for Held<'_> {
    fn deref_mut(&mut self) -> &mut Indexer {
        &mut self.guard
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.memory_bytes.store(self.guard.buffered_bytes() as u64, Ordering::Release);
        if let Some(since) = self.since {
            self.metrics.write_held("ingest", since.elapsed().as_secs_f64() * 1000.0);
        }
    }
}

impl Wal {
    fn open(
        dir: &str,
        seq: u64,
        group_commit_ms: u64,
        metrics: Arc<metrics::Metrics>,
    ) -> std::io::Result<Wal> {
        let (file, len) = svindb_core::wal::open_tail(dir)?;
        Ok(Wal {
            dir: dir.to_string(),
            file: Mutex::new(WalFile { handle: file, len }),
            state: Mutex::new(WalState {
                seq,
                synced: seq,
                buf: Vec::with_capacity(1 << 20),
                committing: false,
                failed: String::new(),
            }),
            done: Condvar::new(),
            group_commit_ms: AtomicU64::new(group_commit_ms),
            metrics,
        })
    }

    /// Кладёт записи в буфер и отдаёт номер последней. Ничего не ждёт.
    fn stage(&self, kind: u8, bodies: &[Vec<u8>]) -> std::io::Result<u64> {
        let mut st = self.state.lock().expect("журнал");
        if !st.failed.is_empty() {
            return Err(std::io::Error::other(st.failed.clone()));
        }
        // `http::MAX_BODY` ограничивает исходный запрос, а не закодированную
        // WAL-запись. Особенно заметно это на `BATCH`: у каждого поля и
        // значения появляется собственная длина, поэтому законный HTTP
        // пакет мог превратиться в запись больше потолка reader'а. Раньше
        // такая пачка уже ложилась в файл, а после restart становилась
        // неразбираемым хвостом. Отказ до выдачи seq сохраняет атомарность:
        // ни номер, ни буфер, ни durable состояние не меняются.
        let max_body = svindb_core::wal::MAX_RECORD_BODY.saturating_sub(9);
        if let Some((at, body)) = bodies.iter().enumerate().find(|(_, body)| body.len() > max_body)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "WAL-запись {} вида {} слишком велика: {} байт, максимум {}",
                    at,
                    kind,
                    body.len(),
                    max_body
                ),
            ));
        }
        let records = u64::try_from(bodies.len()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "число записей пачки не помещается в u64",
            )
        })?;
        if records > u64::MAX - st.seq {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                format!(
                    "нумерация WAL исчерпана: последний номер {}, пачке нужно ещё {records}; \
                     ни одна запись не добавлена",
                    st.seq
                ),
            ));
        }
        let mut buf = std::mem::take(&mut st.buf);
        for body in bodies {
            // Безопасность сложения доказана одной проверкой всей пачки выше.
            st.seq += 1;
            let seq = st.seq;
            svindb_core::wal::append(&mut buf, seq, kind, body);
        }
        st.buf = buf;
        Ok(st.seq)
    }

    /// Ждёт, пока всё до `mine` включительно переживёт `fsync`.
    fn commit(&self, mine: u64) -> std::io::Result<u64> {
        let mut st = self.state.lock().expect("журнал");
        loop {
            if !st.failed.is_empty() {
                return Err(std::io::Error::other(st.failed.clone()));
            }
            if st.synced >= mine {
                return Ok(mine);
            }
            if st.committing {
                // Чужая фиксация идёт — она может накрыть и нас.
                st = self.done.wait(st).expect("журнал");
                continue;
            }
            st.committing = true;
            // Первый фиксатор даёт соседним запросам короткое и строго
            // ограниченное окно, чтобы они попали под тот же `fsync`. Замок
            // состояния на ожидании отпущен, поэтому `stage` продолжает
            // накапливать записи. Ждущие `commit` не становятся фиксаторами:
            // признак `committing` уже стоит.
            let wait = self.group_commit_ms.load(Ordering::Relaxed);
            if wait > 0 {
                let until = Instant::now() + Duration::from_millis(wait);
                loop {
                    let now = Instant::now();
                    if now >= until {
                        break;
                    }
                    let left = until.saturating_duration_since(now);
                    let (next, _) = self.done.wait_timeout(st, left).expect("журнал");
                    st = next;
                }
            }
            let batch = std::mem::take(&mut st.buf);
            let upto = st.seq;
            let records = upto.saturating_sub(st.synced);
            drop(st);

            // Замок файла, а не состояния: пока идёт `fsync`, другие копят своё
            // и попадут в следующую пачку, а не встанут в очередь за этой.
            let mark = Instant::now();
            let out = {
                let mut f = self.file.lock().expect("файл журнала");
                // Rotate before writing the next commit. A failed rotation poisons
                // the WAL through the common error path; never grow a sealed file.
                let r = (|| -> std::io::Result<()> {
                    let mut at = 0;
                    while at < batch.len() {
                        if f.len >= svindb_core::wal::ROLL_BYTES {
                            let (handle, len) = svindb_core::wal::open_tail(&self.dir)?;
                            f.handle = handle;
                            f.len = len;
                        }
                        let mut end = at;
                        let mut length = f.len;
                        while end < batch.len() && length < svindb_core::wal::ROLL_BYTES {
                            let record = svindb_core::wal::encoded_record_len(&batch[end..])?;
                            end += record;
                            length += record as u64;
                        }
                        if length > svindb_core::wal::MAX_FILE_BYTES {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "WAL commit exceeds maximum recoverable file size",
                            ));
                        }
                        f.handle.write_all(&batch[at..end])?;
                        f.handle.sync_all()?;
                        f.len = length;
                        at = end;
                    }
                    Ok(())
                })();
                if r.is_ok() {
                    // Данные уже durable, но ожидатели ещё не получили ACK и
                    // применённый префикс не сдвинут. Падение здесь обязано
                    // привести к полному replay, а не к потере записи.
                    svindb_core::fail::point("wal.after_fsync_before_ack");
                }
                r
            };

            let mut back = self.state.lock().expect("журнал");
            match &out {
                Ok(()) => {
                    back.synced = back.synced.max(upto);
                    self.metrics.wal_commit(records, mark.elapsed().as_secs_f64() * 1000.0);
                }
                Err(e) => {
                    back.failed = format!("фиксация WAL отказала: {e}");
                    self.metrics.wal_commit_failed();
                }
            }
            back.committing = false;
            st = back;
            self.done.notify_all();
            out?;
        }
    }

    fn seq(&self) -> u64 {
        self.state.lock().expect("журнал").seq
    }

    fn set_group_commit_ms(&self, ms: u64) {
        self.group_commit_ms.store(ms, Ordering::Relaxed);
    }
}

/// Учёт неудачных перезагрузок состава.
///
/// ## Зачем он вообще нужен
///
/// Перезагрузка состава — единственный способ, которым правки становятся видны
/// поиску. Она умеет отказать по причинам, которые от индекса не зависят:
/// кончились отображения (`vm.max_map_count`), кончились дескрипторы, диск
/// отдал ошибку. Прежде отказ выражался как `if let Ok(next) = …` — то есть
/// никак: живой набор молча замирал, узел продолжал отвечать **устаревшей**
/// выдачей, и не было ни строчки в журнале, ни признака снаружи. Отличить это
/// от «ничего не менялось» было нечем.
///
/// Молчание здесь хуже отказа: приём подтверждает документы, которых поиск
/// больше никогда не увидит.
///
/// Живёт отдельно от [`Writer`], потому что перезагружает состав не только он:
/// на узле только для чтения писателя нет вовсе, а фоновый пересмотр каталога
/// работает всё равно.
struct Reloads {
    failures: AtomicU64,
    /// Последняя причина — для `/_health` и для человека.
    last: Mutex<String>,
}

impl Reloads {
    fn new() -> Reloads {
        Reloads { failures: AtomicU64::new(0), last: Mutex::new(String::new()) }
    }

    /// Отказ: считаем, запоминаем причину и говорим вслух.
    fn failed(&self, whose: &str, e: &str) {
        let n = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
        // Постоянный отказ встречается на каждом пересмотре и после каждой
        // публикации. Печатать все повторы — значит превратить одну проблему
        // индекса во вторую: на стенде за пятнадцать минут вышло 126 тысяч
        // одинаковых строк и 38 МБ журнала. Первая и степени двойки сохраняют
        // раннее предупреждение и дают масштаб/динамику, но ограничивают один
        // непрерывный эпизод логарифмическим числом строк. Точное число всё
        // время доступно в `/_health` и `/_metrics`.
        if n == 1 || n.is_power_of_two() {
            eprintln!("состав сегментов не перечитался ({whose}): {e} — отказ подряд {n}-й");
        }
        if let Ok(mut last) = self.last.lock() {
            *last = format!("{whose}: {e}");
        }
    }

    /// Удача: счётчик сбрасывается, потому что состав снова свеж.
    fn ok(&self) {
        let failures = self.failures.swap(0, Ordering::Relaxed);
        if failures != 0 {
            if let Ok(mut last) = self.last.lock() {
                last.clear();
            }
            println!("состав сегментов снова перечитывается после {failures} отказов");
        }
    }

    fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    fn last(&self) -> String {
        self.last.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

struct Writer {
    dir: String,
    /// Журнал сломался и обещаниям больше верить нельзя.
    ///
    /// Взводится один раз и не снимается: восстановиться из «часть записей не
    /// легла» нельзя, не зная, какая именно часть. Узел обязан быть перезапущен,
    /// и тогда переигрывание журнала само расскажет, что уцелело.
    poisoned: AtomicBool,
    /// Журнал. `None` — приём без обещаний durability.
    wal: Option<Wal>,
    /// Что рассказал журнал при восстановлении — для `/_health` и для человека.
    recovered: svindb_core::wal::Recovered,
    /// Тот же живой набор сегментов, по которому идёт поиск.
    live: Live,
    /// Буфер приёма. Под замком, потому что запись идёт из потоков соединений.
    ///
    /// Порядок замков во всём писателе один: **сначала `ingest`, потом
    /// `layout`**. Обратный порядок где-нибудь в одном месте — это взаимная
    /// блокировка приёма и удаления, которую видно раз в неделю под нагрузкой.
    ingest: Mutex<Indexer>,
    buffered_memory_bytes: AtomicU64,
    /// Ordinary commits share access; conditional transactions exclude all
    /// mutations from validation through WAL application.
    mutation_gate: RwLock<()>,
    /// Отметку журнала пора двигать: сегмент лёг, а отметка ещё не сдвинута.
    ///
    /// Признак, а не очередь: двигать её надо не «столько раз, сколько было
    /// сбросов», а «до последнего легшего». Поток свежести читает признак и
    /// делает работу один раз за всё накопившееся.
    checkpoint_due: AtomicBool,
    /// Запас отдельных приёмников для сборки пачки **вне** замка.
    ///
    /// Заводить их на каждый запрос нельзя: у приёмника по вектору на поле, и
    /// на широкой схеме это сотни выделений там, где нужно ноль. Запас
    /// возвращает уже прогретый — с ёмкостями прошлой пачки.
    ///
    /// Мьютекс здесь дешёвый и не тот, о котором речь: он держится ровно на
    /// снятие и возврат указателя, а не на всю укладку.
    scratch: Mutex<Vec<Indexer>>,
    /// Право менять состав сегментов. Держат приём при сбросе, удаление и
    /// обслуживание — только один из них разом.
    layout: Arc<Mutex<()>>,
    /// Докуда отметка журнала уже доведена. **Единственное** место записи.
    ///
    /// Прежде здесь стоял `AtomicU64`, и он не спасал. Отметку двигают четыре
    /// места — приём, замена, явный сброс и поток обслуживания, — каждое читает
    /// `flushed_seq` под замком буфера, а писало отметку уже без него. Сравнение
    /// и запись были разными шагами, между которыми успевал вклиниться сосед:
    /// первый прочёл сто, второй двести, второй записал двести, первый записал
    /// сто. Отметка уезжала назад.
    ///
    /// Замок, а не атомик, потому что защищать надо не число, а **последователь-
    /// ность из трёх действий**: сравнить, записать файл, обрезать журнал.
    /// Атомарным сравнением такое не закрывается — оно стережёт только первое.
    checkpoint_gate: Mutex<u64>,
    /// Очередь применения. Держит инвариант «порядок применения = порядок
    /// номеров», от которого зависят и отметка журнала, и переигрывание.
    applied: Applied,
    /// План разбора документа: строится один раз, дальше только читается.
    plan: std::sync::Arc<nested::Plan>,
    /// Число полей и признак «схема с массивами». Меняются вместе со схемой,
    /// то есть никогда после старта.
    shape: (usize, bool),
    /// Новый индекс хранит атомарный lossless source.
    ///
    /// Для него плотный `Doc::flat` запрещён: он превращает отсутствующее поле
    /// в пустую строку. Признак снимается один раз после объявления схемы,
    /// чтобы не искать скрытое поле на каждом документе.
    lossless: bool,
    /// Виды полей и признак «идёт в словарь» — копия схемы для разбора слов
    /// **вне** замка буфера. Оттуда же, откуда `plan`, и с тем же сроком жизни.
    field_kinds: Vec<svindb_core::index::Kind>,
    field_searched: Vec<bool>,
    docs: AtomicU64,
    deleted: AtomicU64,
    /// Общий с фоновым пересмотром: перезагружают состав оба.
    reloads: Arc<Reloads>,
    /// Счётчики узла. Сбросы и укрупнения случаются **здесь** и в потоке
    /// обслуживания, а не в обработчике запроса, — значит и считать их надо
    /// здесь, иначе `/_metrics` показывал бы только то, что попросили снаружи.
    metrics: Arc<metrics::Metrics>,
    /// Значение счётчика опубликованных сегментов на старте процесса.
    /// От него считается настоящий process-local segment creation rate.
    initial_landed: usize,
    /// Полосатые замки идентификаторов: одинаковый `id` сериализован от
    /// проверки CAS до применения, разные идентификаторы по-прежнему вместе
    /// попадают в group commit WAL.
    keys: [Mutex<()>; 256],
    /// Барьер видимости смешанной мутации.
    ///
    /// Одна WAL-запись гарантирует атомарность после restart, а полосы ID —
    /// отсутствие конкурирующих писателей. Этого недостаточно живому читателю:
    /// между публикацией новых версий и гашением удаляемых он мог увидеть
    /// половину mutation. Обычные чтения держат shared-сторону, `/_mutate` —
    /// exclusive-сторону на весь участок применения.
    visibility: std::sync::RwLock<()>,
    /// Чётное значение означает опубликованное состояние, нечётное — окно
    /// смешанной мутации. Поиск запоминает поколение вместе со снимком состава
    /// и перед ответом убеждается, что поколение не менялось.
    visibility_generation: AtomicU64,
    /// До скольких документов буфер приёма участвует в поиске живым сегментом.
    ///
    /// Здесь, а не в общих настройках, по той же причине, по какой здесь лежит
    /// окно group commit: спрашивают его на каждой публикации, а меняют раз в
    /// жизни узла. Ноль — живой сегмент выключен.
    live_max_docs: AtomicU32,
    /// Сколько документов лежало в буфере, когда живой сегмент собирали в
    /// последний раз. Сторож от пересборки того же самого: буфер, в который
    /// ничего не пришло, дал бы посегментно тот же сегмент за ту же цену.
    live_built_docs: AtomicU32,
    /// Сколько публикаций состава уже случилось.
    ///
    /// Читателю по идентификатору он говорит одно: между тем, как тот снял
    /// снимок, и тем, как закончил обход, состав **менялся** — значит его
    /// «не нашлось» могло быть следом гонки, а не ответом. Разбор — у
    /// [`read_ids`].
    published: AtomicU64,
    pending_writes: AtomicUsize,
    write_queue_highwater: AtomicUsize,
    /// Наименьший compaction debt, уже достигнутый этим процессом.
    ///
    /// Импортированный legacy backlog считается исходной точкой, а не новой
    /// перегрузкой: admission тормозит только рост выше достигнутого минимума.
    /// По мере compaction минимум снижается и долг естественно перестаёт быть
    /// «унаследованным».
    compaction_floor: AtomicUsize,
    compaction_pending: AtomicUsize,
    compaction_pending_bytes: AtomicU64,
    compaction_completed_sources: AtomicU64,
    compaction_completed_input_bytes: AtomicU64,
    compaction_elapsed_micros: AtomicU64,
    /// Сколько записей намеренно уступили compaction после мягкого порога.
    l0_throttled: AtomicU64,
    /// Сколько записей получили retryable backpressure после жёсткого порога.
    l0_rejected: AtomicU64,
    /// Последний WAL seq, включённый в searchable ReadView.
    visible_seq: AtomicU64,
    /// Последний WAL seq, опубликованный физическим сегментом.
    ready_seq: AtomicU64,
    /// Последний flush seq, для которого чистые страницы новых immutable
    /// сегментов уже были отданы ядру как вытесняемые.
    cache_advised_seq: AtomicU64,
    drop_flushed_cache: AtomicBool,
    /// Последний ready seq, после которого compaction debt сходил к нулю.
    settled_seq: AtomicU64,
    /// Сколько из долга обслуживание может убрать **сейчас**.
    ///
    /// Отдельно от долга, потому что долг бывает законным и вечным: пятнадцать
    /// сегментов по две трети целевого размера сливать нельзя ни в какой паре,
    /// и превышение над допустимым составом остаётся навсегда. Судить о том,
    /// сведён ли индекс, надо по этому числу, а не по долгу.
    compaction_schedulable: AtomicUsize,
    compaction_running: AtomicUsize,
    /// Почему обслуживание стоит, если долг есть, а работы нет.
    ///
    /// Без этого `pending>0, running=0` не отличить от здорового ожидания:
    /// снаружи оба выглядят одинаково, а означают разное — во втором случае
    /// узел не разберёт долг никогда.
    compaction_blocked: Mutex<svindb_core::maintain::Blocked>,
    flush_state: Mutex<FlushState>,
    flush_done: Condvar,
}

/// Делает поколение нечётным на время смешанной мутации и публикует следующее
/// чётное поколение при любом выходе, включая ошибку применения.
struct VisibilityMutation<'a> {
    generation: &'a AtomicU64,
}

impl<'a> VisibilityMutation<'a> {
    fn begin(generation: &'a AtomicU64) -> Self {
        let before = generation.fetch_add(1, Ordering::AcqRel);
        debug_assert!(before.is_multiple_of(2), "смешанные мутации сериализованы write-lock");
        VisibilityMutation { generation }
    }
}

impl Drop for VisibilityMutation<'_> {
    fn drop(&mut self) {
        let before = self.generation.fetch_add(1, Ordering::Release);
        debug_assert!(!before.is_multiple_of(2), "поколение мутации обязано быть нечётным");
    }
}

impl Writer {
    fn mutation_access(&self, conditional: bool) -> MutationAccess<'_> {
        if conditional {
            MutationAccess {
                _shared: None,
                _exclusive: Some(self.mutation_gate.write().expect("conditional mutation gate")),
            }
        } else {
            MutationAccess {
                _shared: Some(self.mutation_gate.read().expect("mutation gate")),
                _exclusive: None,
            }
        }
    }

    /// Переносит завершённые публикации из индексатора в Prometheus ровно один
    /// раз. Вызывается под `ingest`: и очередь длительностей, и её потребитель
    /// имеют того же единственного владельца.
    fn observe_flushes(&self, ix: &mut Indexer) {
        self.buffered_memory_bytes.store(ix.buffered_bytes() as u64, Ordering::Release);
        for ms in ix.take_flush_ms() {
            self.metrics.flushed(ms);
        }
    }

    /// Не слишком ли отстала отметка журнала. `Err` — приём обязан отказать.
    ///
    /// ## Зачем предел, если отметка всё равно догонит
    ///
    /// Догонит — но время подъёма считается по тому, что **не** отмечено:
    /// каждая неотмеченная запись переигрывается при следующем старте. Пока
    /// отметку двигал сам приём, отставание было ограничено само собой — ценой
    /// десятков миллисекунд у случайного запроса. Отдав работу фоновому потоку,
    /// мы сняли эту цену и вместе с ней ограничение: под потоком, который
    /// быстрее фонового тика, отставание росло бы, пока не кончится диск, а
    /// подъём после падения занимал бы часы.
    ///
    /// Поэтому предел назван явно. Отказ — `429` с числом: клиенту есть что
    /// повторить, и есть что показать оператору.
    fn wal_lag_ok(&self) -> Result<(), (u64, u64)> {
        let Some(wal) = self.wal.as_ref() else { return Ok(()) };
        let seq = wal.seq();
        let done = *self.checkpoint_gate.lock().unwrap_or_else(|e| e.into_inner());
        let lag = seq.saturating_sub(done);
        if lag > MAX_WAL_LAG {
            Err((lag, MAX_WAL_LAG))
        } else {
            Ok(())
        }
    }

    /// Пускает запрос в ограниченную очередь записи.
    fn enter_write(&self, limit: usize) -> Option<WritePermit<'_>> {
        let mut seen = self.pending_writes.load(Ordering::Acquire);
        loop {
            if seen >= limit {
                self.metrics.refused_by_write_queue();
                return None;
            }
            match self.pending_writes.compare_exchange_weak(
                seen,
                seen + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.write_queue_highwater.fetch_max(seen + 1, Ordering::Relaxed);
                    return Some(WritePermit { pending: &self.pending_writes });
                }
                Err(actual) => seen = actual,
            }
        }
    }

    /// Берёт полосы идентификаторов в одном порядке, поэтому две пачки с
    /// пересечением не могут взаимно ждать друг друга.
    fn lock_ids<'a>(&'a self, ids: &[String]) -> Vec<std::sync::MutexGuard<'a, ()>> {
        let mut stripes: Vec<usize> =
            ids.iter().map(|id| bloom::hash(id) as usize & (self.keys.len() - 1)).collect();
        stripes.sort_unstable();
        stripes.dedup();
        let started = stats::ENABLED.then(Instant::now);
        let guards = stripes
            .into_iter()
            .map(|at| self.keys[at].lock().expect("полоса идентификатора"))
            .collect();
        if let Some(started) = started {
            self.metrics.write_lock("id_stripes", started.elapsed().as_secs_f64() * 1000.0);
        }
        guards
    }

    /// Занимает место в очереди применения.
    ///
    /// Без журнала (`--no-wal`) очереди нет и быть не может: номеров не
    /// раздают, переигрывать нечего, а порядок применения ни на что не влияет.
    fn claim_apply(&self, seq: Option<WalTicket>) -> Result<Option<ApplyTicket<'_>>, String> {
        let Some(t) = seq else { return Ok(None) };
        let started = stats::ENABLED.then(Instant::now);
        let ticket = match self.applied.claim(t) {
            Ok(ticket) => ticket,
            // Очередь встала — узел больше не может обещать долговечность, и
            // молчать об этом нельзя. Приём останавливается тем же способом,
            // что и при сломанном журнале, и по той же причине: записи в
            // журнале целы, перезапуск переиграет их в верном порядке. Двигать
            // отметку через застрявшего предшественника нельзя — это ровно тот
            // разрыв порядка, ради которого очередь и заведена.
            Err(why) => {
                eprintln!("{why}; приём остановлен");
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(why);
            }
        };
        if let Some(started) = started {
            self.metrics.write_lock("apply_order", started.elapsed().as_secs_f64() * 1000.0);
        }
        Ok(Some(ticket))
    }

    /// Разбирает слова полей всей пачки, ничего не запирая.
    ///
    /// Пусто для документа с массивами: у него путь укладки другой, ячейковый,
    /// и подставлять туда плоский разбор нельзя. Пусто и для полей, которые в
    /// словарь не идут вовсе, — числовых, векторных, `noindex`.
    fn pretokenize(&self, docs: &[nested::Doc]) -> Vec<Prepared> {
        // Разбор слов считается **здесь**, а не только на пути под замком.
        //
        // Счётчик стадии стоял внутри укладки, и после выноса подготовки он
        // показывал остаток — доли процента, — а сама работа переехала туда,
        // где её никто не мерил. Судить по такому счётчику, стоит ли менять
        // представление токенов, значило бы судить по пустому месту.
        let started = stats::ENABLED.then(Instant::now);
        let (nfields, grouped) = self.shape;
        let analyzer = svindb_core::analyze::Simple;
        let mut out = Vec::with_capacity(docs.len());
        let mut scratch = svindb_core::analyze::Tokens::default();
        for doc in docs.iter() {
            // Lossless-документ остаётся разреженным. Токены корневых полей
            // по-прежнему считаются здесь, вне общего буфера, но сами значения
            // передаются ячейками: только так отсутствующее поле не становится
            // присутствующей пустой строкой.
            if self.lossless && !grouped {
                let mut per_field: Vec<svindb_core::analyze::Tokens> =
                    vec![svindb_core::analyze::Tokens::default(); nfields];
                for (field, slot, text) in &doc.cells {
                    let field = *field as usize;
                    if *slot != 0 {
                        continue;
                    }
                    let kind = self.field_kinds.get(field).copied().unwrap_or(Kind::Text);
                    if !self.field_searched.get(field).copied().unwrap_or(true)
                        || kind.dims().is_some()
                        || kind.dot()
                        || kind.numeric()
                    {
                        continue;
                    }
                    Indexer::<svindb_core::analyze::Simple>::field_tokens(
                        &analyzer,
                        kind,
                        text,
                        &mut scratch,
                    );
                    per_field[field].append(&mut scratch);
                }
                out.push(Prepared { values: None, words: per_field });
                continue;
            }
            let Some(values) = doc.flat(nfields).filter(|_| !grouped) else {
                out.push(Prepared::default());
                continue;
            };
            let mut per_field: Vec<svindb_core::analyze::Tokens> =
                vec![svindb_core::analyze::Tokens::default(); nfields];
            for (f, text) in values.iter().enumerate() {
                let kind = self.field_kinds.get(f).copied().unwrap_or(Kind::Text);
                // Ровно те поля, для которых путь под замком зовёт разбор: не
                // числовые, не векторные, не разреженные и не `noindex`.
                if !self.field_searched.get(f).copied().unwrap_or(true)
                    || kind.dims().is_some()
                    || kind.dot()
                    || kind.numeric()
                {
                    continue;
                }
                Indexer::<svindb_core::analyze::Simple>::field_tokens(
                    &analyzer,
                    kind,
                    text,
                    &mut scratch,
                );
                per_field[f] = std::mem::take(&mut scratch);
            }
            out.push(Prepared { values: Some(values), words: per_field });
        }
        if let Some(started) = started {
            self.metrics.pretokenize(started.elapsed().as_secs_f64() * 1000.0);
        }
        out
    }

    /// Берёт приёмник из запаса или заводит новый.
    fn take_scratch(&self) -> Indexer {
        let taken = self.scratch.lock().unwrap_or_else(|e| e.into_inner()).pop();
        match taken {
            Some(ix) => ix,
            // Заводить приходится редко: запас растёт до числа потоков, которые
            // правда приходят одновременно, и дальше только переиспользуется.
            None => self.ingest.lock().expect("буфер приёма").detached(),
        }
    }

    /// Возвращает приёмник в запас, не давая запасу расти без предела.
    fn give_scratch(&self, ix: Indexer) {
        let mut pool = self.scratch.lock().unwrap_or_else(|e| e.into_inner());
        // Предел — по числу одновременных пишущих: больше в запасе всё равно не
        // понадобится, а каждый держит ёмкости своей последней пачки.
        if pool.len() < 64 {
            pool.push(ix);
        }
    }

    fn lock_ingest(&self) -> Held<'_> {
        let started = stats::ENABLED.then(Instant::now);
        let guard = self.ingest.lock().expect("буфер приёма");
        if let Some(started) = started {
            self.metrics.write_lock("ingest", started.elapsed().as_secs_f64() * 1000.0);
        }
        Held {
            guard,
            since: stats::ENABLED.then(Instant::now),
            metrics: &self.metrics,
            memory_bytes: &self.buffered_memory_bytes,
        }
    }

    fn lock_ingest_guarded(&self, request: &svindb_core::work::Guard) -> Option<Held<'_>> {
        let started = stats::ENABLED.then(Instant::now);
        let guard = lock_mutex_guarded(&self.ingest, request)?;
        if let Some(started) = started {
            self.metrics.write_lock("ingest", started.elapsed().as_secs_f64() * 1000.0);
        }
        Some(Held {
            guard,
            since: stats::ENABLED.then(Instant::now),
            metrics: &self.metrics,
            memory_bytes: &self.buffered_memory_bytes,
        })
    }

    /// План разбора документа. Строится один раз на жизнь узла.
    ///
    /// ## Почему копия, а не чтение приёмника
    ///
    /// Прежде план собирался на **каждую** пачку, и ради этого брался замок
    /// буфера приёма — того самого, за который дерутся все пишущие. Довод был
    /// такой: держать копию значило бы завести второй источник правды о полях.
    /// Довод неверен: схема узла неизменна с его старта. Поля объявляются
    /// однажды при `Indexer::new`, группы — однажды в `cmd_serve` до открытия
    /// порта, и после этого их не меняет никто. Второго источника правды тут не
    /// возникает: копия делается **после** объявления и живёт ровно столько же,
    /// сколько сама схема.
    ///
    /// Цена прежнего решения — один захват общего замка на каждый запрос
    /// записи, на ровном месте.
    fn plan(&self) -> std::sync::Arc<nested::Plan> {
        std::sync::Arc::clone(&self.plan)
    }

    /// Приём без замены. Полос идентификаторов **не берёт**, и это следствие
    /// упорядоченного применения.
    ///
    /// ## Почему их можно снять
    ///
    /// Полосы заводились ради одного: «одинаковый `id` сериализован от проверки
    /// CAS до применения». У безусловного приёма проверки нет — он ничего не
    /// читает и никого не гасит, — а порядок теперь держит очередь применения:
    /// номера раздаёт журнал, применение идёт строго по ним. Взаимное
    /// расположение двух приёмов одного `id` определено их номерами, а не тем,
    /// кто раньше добрался до мьютекса.
    ///
    /// Замена, гашение и мутация полосы берут по-прежнему: там есть чтение, и
    /// между ним и применением никто не должен вклиниться.
    ///
    /// ## Что это даёт
    ///
    /// Замер на трёх миллионах: полосы держались 124 мс на запрос, а буфер
    /// приёма — 0.000 мс. То есть сериализовали именно они, и держались через
    /// кодирование журнала и его `fsync`. Теперь эти стадии у разных запросов
    /// идут одновременно, а фиксации собираются в общую пачку, как и задумано
    /// у group commit.
    fn add(&self, docs: Vec<nested::Doc>) -> Result<usize, String> {
        self.healthy()?;
        // Подготовка идёт **до** журнала и до буфера, вне обоих замков.
        //
        // Здесь разбирается плоский вид документа и слова его полей — самая
        // дорогая часть укладки после сжатия. Прежде и то и другое считалось
        // под замком буфера, то есть в один поток на весь узел; плоский вид
        // собирался при этом дважды, журналом и укладкой, с полной копией
        // текста каждого поля в каждый раз. Теперь он собирается один раз и
        // достаётся обоим.
        //
        // Слова разбирает тот же код ядра, что и путь под замком, поэтому
        // сегмент выходит тот же, каким бы путём документ ни попал в приём.
        let mut ready = self.pretokenize(&docs);
        // Пачка укладывается в отдельный приёмник — **вне всяких замков**.
        //
        // Здесь идёт всё, что стоит дорого и не требует общего состояния:
        // разбор значений, слова, частоты, позиции, колонки, нормы. Под замком
        // остаётся слияние готового куска, где на каждое **разное слово пачки**
        // приходится один поиск в общем словаре вместо одного на каждое
        // вхождение. На обычном тексте это разница в разы.
        //
        // Кусок собирается тем же кодом, что и буфер приёма (см.
        // `Indexer::detached`), поэтому сегмент выходит побайтово тот же — это
        // не обещание, а построение.
        let mut frag = self.take_scratch();
        for (k, doc) in docs.iter().enumerate() {
            match ready.get_mut(k) {
                Some(p) if p.values.is_some() => {
                    let values = p.values.take().expect("значения на месте");
                    nested::put_doc_flat(&mut frag, &doc.id, &values, Some(p.words.as_mut_slice()));
                }
                other => {
                    let pre = other.map(|p| p.words.as_mut_slice());
                    nested::put_doc_pre(&mut frag, doc, false, pre);
                }
            }
        }
        // Сперва журнал, потом буфер. Обратный порядок означал бы, что между
        // ответом «принято» и записью на диск есть окно, ради закрытия которого
        // журнал и заводился.
        let _access = self.mutation_access(false);
        let seq = self.log_docs(false, &docs, Some(&ready))?;
        // Журнал не принял — не принимаем и мы. Положить документ в буфер и
        // ответить «принято» значило бы обещать то, чего не переживёт падение.
        self.healthy()?;
        // Своя очередь — **до** буфера. Применившись раньше соседки с меньшим
        // номером, пачка увела бы отметку журнала вперёд себя самой.
        let _apply = self.claim_apply(seq)?;
        let flushed = {
            let mut ix = self.lock_ingest();
            // Номер записи — последнего документа пачки: кусок вливается
            // целиком, и сегмент, которым он кончится, унесёт именно его.
            if let Some(ticket) = seq {
                ix.seq = ticket.at(docs.len().saturating_sub(1));
            }
            self.push(&mut ix, |ix| ix.absorb(&mut frag))
        };
        self.give_scratch(frag);
        self.docs.fetch_add(docs.len() as u64, Ordering::Relaxed);
        if flushed {
            // Отметка журнала двигается **не здесь**.
            //
            // Она стоит `msync` всех живых карт, двух `fsync` и обрезки, а
            // обрезка CRC-сканирует целые файлы журнала по шестьдесят четыре
            // мегабайта. Всё это делалось в потоке запроса: клиент, которому не
            // повезло попасть на сброс, платил за уборку всего узла — десятками
            // миллисекунд.
            //
            // Работа не отменяется, а переезжает в поток свежести, который и так
            // ходит каждые сто миллисекунд. Плата за перенос — отставание
            // отметки, а с ним время подъёма; чтобы оно не стало неограниченным,
            // приём отказывает `429`, когда отставание перерастает предел.
            self.checkpoint_due.store(true, Ordering::Release);
        }
        svindb_core::fail::point("add.after_apply_before_ack");
        // Спрашиваем **после** приёма, а не только до.
        //
        // Отказ фоновой записи виден не сразу: он забирается вместе с итогом
        // предыдущего сегмента, то есть на пачке, которая вызвала следующий
        // сброс. Проверка только на входе отдавала бы этой пачке `{"added":N}`,
        // и узнавал бы клиент лишь со следующей.
        //
        // Документы при этом не потеряны: журнал их удержал, отметка не
        // двинулась, и перезапуск их переиграет. Но обещание «принято» стоит
        // отдавать вместе со знанием, что дальше приём остановлен.
        self.healthy()?;
        Ok(docs.len())
    }

    /// Кладёт документ в буфер и, если это вызвало сброс, тут же перечитывает
    /// набор сегментов. `true` — сброс случился.
    fn push(&self, ix: &mut Indexer, put: impl FnOnce(&mut Indexer)) -> bool {
        // Два наблюдения снимаются до работы и сверяются после — и оба нужны.
        //
        // `flying` ловит **передачу** буфера писателю, `segments` — **посадку**
        // ранее переданного. Случиться они могут в любой ветви ниже, в том числе
        // внутри самого `put`: гашение в буфере дожидается фоновой записи, а
        // порог по объёму отдаёт буфер молча.
        //
        // Пока посадка ловилась только там, где её ждали, ветвь «буфер ещё не
        // полон» её проглатывала: сегмент ложился, приём забирал итог внутри
        // `put`, а состав об этом не узнавал **никогда**. Индекс продолжал
        // работать по составу без единого дискового сегмента: поиск отвечал по
        // одному живому сегменту, отметка журнала стояла на нуле, а документы,
        // чья живая версия успела уехать на диск, пропадали из выдачи насовсем.
        // Прежде это подбирал фоновый пересмотр каталога — но он уступает
        // писателю, как только появляется живой сегмент, и подбирать стало
        // некому. Поймано стендом под нагрузкой.
        let flying = ix.flush_inflight();
        let segments = ix.segments();
        let landed = self.push_inner(ix, put);
        let collected = ix.segments() != segments;
        let mut late_collected = false;
        if collected && !landed {
            // Сегмент лёг и был забран внутри `put` — состав обязан подобрать
            // его сам, а не ждать, пока кто-нибудь заметит.
            self.publish(ix);
        } else if !flying && ix.flush_inflight() {
            // Буфер ушёл писателю: его живой двойник обязан войти в состав
            // сейчас же, иначе документы этого буфера не видны никому — из
            // текущего буфера их забрали, на диск ещё не положили, а прежние
            // версии уже погашены.
            late_collected = self.republish(ix, false);
        }
        landed || collected || late_collected
    }

    /// Тело [`Writer::push`] без заботы о живом двойнике.
    fn push_inner(&self, ix: &mut Indexer, put: impl FnOnce(&mut Indexer)) -> bool {
        // Спросить фонового писателя, не закончил ли он, — дёшево, и делать это
        // надо на каждом документе, а не раз в сегмент. Отметка журнала двигается
        // ровно по этому признаку: не забери мы итог сразу, журнал рос бы до
        // следующего сброса, а подъём переигрывал бы всё накопленное.
        let before = ix.segments();
        ix.poll();
        self.observe_flushes(ix);
        if ix.segments() != before {
            self.publish(ix);
            put(ix);
            self.observe_flushes(ix);
            return true;
        }
        // Осаживание: буфер не растёт сверх отведённого ему.
        //
        // Порог сброса говорит, когда буфер пора отдать писателю, но не
        // говорит, что делать, если писатель ещё занят предыдущим сегментом.
        // Прежде приём просто продолжал класть, и буфер рос, пока хватало
        // памяти: на стенде куча доходила до восемнадцати гигабайт, и ядро
        // снимало процесс на тридцать первом миллионе документов.
        //
        // Здесь приём ждёт писателя, а не обгоняет его. Клиент от этого
        // замедляется ровно настолько, насколько узел не успевает укладывать
        // на диск, — то есть ровно на столько, на сколько должен. Отказывать
        // ошибкой не нужно: ожидание и есть честный ответ «принимаю, но не
        // быстрее, чем пишу», и оно сохраняет пачку вместо того, чтобы
        // заставлять клиента слать её заново.
        if ix.over_memory_ceiling() {
            let before = ix.segments();
            ix.drain();
            self.observe_flushes(ix);
            if ix.segments() != before {
                self.publish(ix);
            }
        }
        if ix.buffered() + 1 < ix.per_segment() {
            put(ix);
            self.observe_flushes(ix);
            return false;
        }
        let before = ix.segments();
        // Замок состава здесь больше не берётся, и это обязательно, а не
        // упрощение. При фоновой записи `put` может ждать поток-писатель, а тот
        // ждёт этот самый замок ради внесения в список — держи мы его здесь,
        // вышла бы взаимная блокировка. Внесение теперь под замком у самого
        // индексатора: он один знает, когда его брать и когда отпускать.
        put(ix);
        self.observe_flushes(ix);
        if ix.segments() == before {
            // Сегмент отдан на запись, но ещё не лёг. Видимым он станет по
            // живому двойнику — его внесёт `push`.
            return false;
        }
        self.publish(ix);
        true
    }

    /// Вносит легший сегмент в состав и гасит прежние версии — под одним замком.
    ///
    /// ## Порядок внутри: сперва внести, потом гасить
    ///
    /// Обратный порядок вернул бы ровно то окно, ради закрытия которого гашение
    /// и откладывалось: между гашением старой версии и появлением новой у
    /// документа не было бы ни одной живой. Здесь наоборот — в промежутке между
    /// внесением и гашением видны **обе** версии.
    ///
    /// Обмен неравноценный и выбран сознательно. Промежуток — доли микросекунды
    /// под одним замком, без единого обращения к диску: гашение это атомарный
    /// сброс бита в уже отображённой карте. Лишний кандидат в выдаче стоит
    /// проверки, отсутствие документа — неверного решения на стороне клиента.
    /// Чтение по идентификатору не задето и в этом промежутке: оно обходит
    /// состав с конца и берёт первую живую версию, то есть новейшую.
    ///
    /// ## Почему пропущенный вызов не ломает состав
    ///
    /// [`crate::maintain::supersede_in`] гасит всё, кроме новейшей версии, и
    /// потому идемпотентен: следующая публикация того же идентификатора
    /// доберёт то, что не догасила предыдущая. Список при этом не теряется —
    /// он копится в приёмнике до тех пор, пока его не заберут.
    fn publish(&self, ix: &mut Indexer) {
        // Итог писателя забирается **до** перечитывания манифеста, и это не
        // предосторожность, а закрытие двери, через которую терялись гашения.
        //
        // Порядок у фонового писателя такой: сегмент дописывается в манифест
        // под замком состава, и **только потом** уходит `Back` с обязательством
        // погасить прежние версии. Между этими двумя событиями сегмент уже
        // виден в манифесте, а обязательство ещё лежит в канале.
        //
        // Все горячие пути опрашивают писателя до публикации — `push` и
        // `refresh_live` делают это первым делом. Все, кроме одного:
        // `delete_now` звал `publish` напрямую, тот перечитывал манифест, и
        // сегмент входил в состав **без своего обязательства**. Дальше
        // обслуживание видело свежий мелкий сегмент, брало его в окно яруса,
        // сливало и сносило каталог. Когда обязательство наконец забирали,
        // хранитель разрешался точным сравнением имён — и не находился уже
        // никогда: имена монотонны и не возвращаются.
        //
        // Итог на стенде: после одиннадцати тысяч замен одного `id` под
        // приёмом, поиском и удалением в составе оставались **две** живые
        // версии, и они переживали покой. Прежнюю должно было погасить
        // пропавшее обязательство; всё, что старше, погасили обязательства
        // прежние — успешные. Отсюда ровно две, а не десять.
        ix.poll();
        self.observe_flushes(ix);
        self.republish(ix, true);
    }

    /// Пересобирает живой сегмент из нынешнего буфера — без чтения диска.
    ///
    /// Зовётся по таймеру свежести. Именно отсюда берётся видимость записи
    /// раньше сброса на диск: буфер остаётся буфером, а его копия входит в
    /// состав сегментом.
    /// Отдаёт `true`, если за это время лёг сегмент: вызывающий обязан тогда
    /// отпустить буфер и позвать [`Writer::after_flush`] — та берёт тот же
    /// замок и отсюда её звать нельзя.
    fn refresh_live(&self, ix: &mut Indexer) -> bool {
        // Итог фоновой записи забирается **до** решения, и это не оптимизация.
        //
        // Лёгший сегмент несёт с собой список отложенных гашений, а применять
        // его можно только по составу, в котором этот сегмент уже есть.
        // Обновление живого сегмента диска не перечитывает — значит, узнав о
        // посадке, оно обязано перейти в полное перечитывание, а не сделать вид,
        // что ничего не произошло.
        let before = ix.segments();
        ix.poll();
        self.observe_flushes(ix);
        let landed = ix.segments() != before;
        // В буфер ничего не пришло и ничего не легло — пересобирать нечего.
        // Считаем по числу документов, и этого достаточно: внутри одного буфера
        // оно только растёт (замена гасит прежнюю запись, но кладёт новую), а
        // сброс обнуляет его вместе с самим буфером.
        if !refresh_live_needed(
            landed,
            ix.active_buffered(),
            self.live_built_docs.load(Ordering::Relaxed),
            self.live_max_docs.load(Ordering::Relaxed),
        ) {
            return false;
        }
        // В новом режиме refresh публикует только suffix immutable generation.
        // Physical flush не вызывается и не пересобирает весь raw buffer.
        let frozen_enabled = ix.frozen_memtables_enabled();
        if frozen_enabled {
            match ix.freeze_live() {
                Ok(()) => return self.republish(ix, landed),
                Err(error) => {
                    self.reloads.failed("публикация frozen поколения", &error.to_string());
                    return false;
                }
            }
        }
        self.republish(ix, landed) || landed
    }

    /// Приводит состав в соответствие с диском и буфером — **одним** обменом.
    ///
    /// ## Почему одним, а не тремя
    ///
    /// Событий здесь три: снять прежний живой сегмент, подобрать легшее на диск,
    /// собрать живой заново. Сделай их тремя обменами — и между первым и вторым
    /// у документа не будет ни одной живой версии, а между вторым и третьим
    /// каждый документ буфера окажется в составе дважды: и в легшем сегменте, и
    /// в живом, собранном из ещё не очищенного буфера. Читатель работает со
    /// снимком, и снимок обязан быть согласованным целиком.
    ///
    /// ## Порядок: сперва состав, потом гашение
    ///
    /// Разбор — у [`svindb_core::maintain::supersede_in`]. Коротко: обратный
    /// порядок вернул бы окно, в котором у документа нет ни одной живой версии,
    /// а этот оставляет промежуток, где видны обе, — и он в тысячи раз короче.
    fn republish(&self, ix: &mut Indexer, reread: bool) -> bool {
        let _guard = self.layout.lock().expect("состав сегментов");
        // Flush мог закончиться после внешнего poll, пока ждали layout.
        // Manifest и Back теперь публикуются под этим же lock: повторный
        // неблокирующий poll снимает twin до чтения уже опубликованного disk.
        let before = ix.segments();
        ix.poll();
        self.observe_flushes(ix);
        let reread = reread || ix.segments() != before;
        let prev = { self.live.read().expect("набор сегментов").clone() };
        // Прежний живой сегмент снимается всегда: он собран из того буфера,
        // каким тот был на прошлой публикации, и с тех пор буфер изменился —
        // либо принял ещё документы, либо целиком уехал на диск.
        let base = match prev.without_any_live() {
            Ok(base) => base,
            Err(e) => {
                self.reloads.failed("снятие живого сегмента", &e.to_string());
                return false;
            }
        };
        let mut next = if reread {
            match svindb_core::Segments::reopen(&self.dir, &base) {
                Ok(next) => next,
                Err(e) => {
                    self.reloads.failed("приём", &e.to_string());
                    return false;
                }
            }
        } else {
            base
        };

        let frozen_generations = ix.frozen_generations();
        let use_frozen = !frozen_generations.is_empty();
        // Буфер, отданный писателю, показывает свой живой сегмент — тот, что
        // собран в момент передачи. Он идёт в состав первым: его документы
        // старше тех, что лежат в наборе прямо сейчас.
        //
        // Без него всё время записи сегмента его документы невидимы никому:
        // из текущего буфера их уже забрали, на диск ещё не положили, а прежние
        // версии к тому моменту погашены. Документ пропадал бы целиком.
        let mut work: Vec<(String, Vec<String>)> = Vec::new();
        if use_frozen {
            for generation in &frozen_generations {
                match next.with_live(Arc::clone(&generation.index)) {
                    Ok(with) => next = with,
                    Err(error) => {
                        self.reloads.failed("внесение frozen поколения", &error.to_string());
                        return false;
                    }
                }
                let ids = generation.superseded.to_vec();
                if !ids.is_empty() {
                    work.push((generation.index.dir.clone(), ids));
                }
            }
            if reread {
                let mut landed = ix.take_superseded();
                landed.append(&mut work);
                work = landed;
            }
        } else if let Some(inflight) = ix.inflight_live() {
            match next.with_live(inflight) {
                Ok(with) => {
                    next = with;
                    let ids = ix.inflight_superseded_now().to_vec();
                    if !ids.is_empty() {
                        work.push((svindb_core::indexer::INFLIGHT_SEGMENT.to_string(), ids));
                    }
                }
                Err(e) => self.reloads.failed("внесение летящего сегмента", &e.to_string()),
            }
        }

        // Живой сегмент собирается, пока буфер не перерос порог. Перерос —
        // видимость возвращается к сбросу на диск; см. `live_max_docs`.
        let limit = self.live_max_docs.load(Ordering::Relaxed);
        let live = if !use_frozen && limit > 0 && ix.active_buffered() <= limit {
            match ix.build_live(LIVE_SEGMENT) {
                Ok(live) => live,
                // Отказ сборки — это отсутствие свежести, а не порча: состав
                // остаётся верным, просто буфер снова невидим до сброса. Узел
                // обязан сказать об этом, но не обязан останавливаться.
                Err(e) => {
                    self.reloads.failed("сборка живого сегмента", &e.to_string());
                    None
                }
            }
        } else {
            None
        };
        // Прежние версии тех документов, что лежат в буфере, можно гасить только
        // когда буфер виден. Не собрался живой сегмент — не гасим: иначе старая
        // версия умрёт, а новая останется невидимой, и вернётся ровно то окно,
        // ради закрытия которого гашение и откладывалось.
        //
        // Список легшего забирается **только** при перечитывании, и это не
        // мелочь. Он относится к сегменту, который лежит на диске; применённый
        // по составу, куда этот сегмент ещё не внесён, он не найдёт новой версии
        // и не погасит ничего — а список при этом окажется израсходован, и
        // прежняя версия останется живой навсегда. Ровно так и получалось:
        // обновление живого сегмента забирало чужой список и тратило его впустую.
        // Список хранится парами «хранитель — идентификаторы»: гасить прежние
        // версии можно только в пользу названного сегмента, а не «того, что
        // лежит правее в составе». Разбор — у `maintain::supersede_in`.
        // Порядок списка — от старших хранителей к младшим: побеждает последний,
        // и посадки обязаны идти раньше того, что ещё в памяти.
        if reread && !use_frozen {
            let mut landed = ix.take_superseded();
            landed.append(&mut work);
            work = landed;
        }
        if let Some(live) = live {
            match next.with_live(live) {
                Ok(with) => {
                    next = with;
                    let ids = ix.superseded_now().to_vec();
                    if !ids.is_empty() {
                        work.push((LIVE_SEGMENT.to_string(), ids));
                    }
                }
                Err(e) => self.reloads.failed("внесение живого сегмента", &e.to_string()),
            }
        }

        *self.live.write().expect("набор сегментов") = Arc::new(next);
        self.visible_seq.fetch_max(ix.seq, Ordering::Release);
        // Публикация считается при любой подмене ReadView, даже если гасить
        // прежние версии не пришлось. Иначе point-read принимал настоящий
        // append за «состав не менялся» и не выполнял защитный второй заход.
        self.published.fetch_add(1, Ordering::Release);
        // Признак свежести снимается только тем, кто правда сходил на диск.
        //
        // Пересборка живого сегмента диска не касается вовсе, и объявлять по ней
        // состав перечитанным нельзя: она идёт каждые сто миллисекунд и стёрла
        // бы отметку об отказе перезагрузки почти мгновенно. `/_health` отвечал
        // бы `ok:true` на узле, который отдаёт устаревшую выдачу, — то есть
        // балансировщик не снял бы с него трафик.
        if reread {
            self.reloads.ok();
        }
        self.live_built_docs.store(ix.active_buffered(), Ordering::Relaxed);

        if work.is_empty() {
            return reread;
        }
        // Один идентификатор — один хранитель, и это самый свежий из названных.
        //
        // В одной публикации их набирается несколько: сегмент, легший минуту
        // назад, помнит замену, которую с тех пор успели заменить снова, и его
        // список называет хранителем **его**, а живая версия лежит уже в другом
        // месте. Примени оба — и старший хранитель погасит версию младшего:
        // документ откатится на версию назад, а следующая замена не пройдёт по
        // CAS. Порядок здесь заведомо верный: `take_superseded` отдаёт посадки
        // по времени, живой сегмент дописывается последним, — поэтому побеждает
        // последнее упоминание.
        let mut newest: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut work: Vec<(String, Vec<String>)> = work
            .iter()
            .rev()
            .map(|(keeper, ids)| {
                let mine: Vec<String> =
                    ids.iter().filter(|id| newest.insert(id.as_str())).cloned().collect();
                (keeper.clone(), mine)
            })
            .filter(|(_, ids)| !ids.is_empty())
            .collect();
        work.reverse();
        if work.is_empty() {
            return reread;
        }
        let segs = { self.live.read().expect("набор сегментов").clone() };
        let refs: Vec<&svindb_core::Index> = segs.list.iter().map(|a| a.as_ref()).collect();
        // Не применённое возвращается обратно, а не теряется.
        //
        // Хранителя может не оказаться в составе: пока сегмент шёл от посадки до
        // публикации, обслуживание успело слить его с соседями. Гасить в пользу
        // исчезнувшего нельзя, а забыть — значит оставить у документа две живые
        // версии навсегда. Поэтому такие идентификаторы возвращаются в очередь и
        // применятся при следующей публикации, когда преемник снова найдётся.
        let mut again: Vec<(String, Vec<String>)> = Vec::new();
        for (keeper, ids) in work {
            match svindb_core::maintain::supersede_in(&refs, &keeper, &ids) {
                Ok(done) => {
                    self.deleted.fetch_add(done.killed as u64, Ordering::Relaxed);
                    if !done.missing.is_empty() {
                        again.push((keeper, done.missing));
                    }
                }
                // Отказ здесь — это состав, в котором у документа осталось две
                // живые версии. Молчать нельзя: выдача начнёт колебаться между
                // ними, и объяснить это будет нечем.
                Err(e) => {
                    self.reloads.failed("гашение прежних версий", &e.to_string());
                    again.push((keeper, ids));
                }
            }
        }
        if !again.is_empty() {
            ix.put_back_superseded(again);
        }
        reread
    }

    /// Пишет пачку в журнал и ждёт, пока она переживёт `fsync`.
    ///
    /// Кладёт пачку в журнал.
    ///
    /// Несколько документов получают **одну** внешнюю запись с одной CRC.
    /// Поэтому queue item, attempt и checkpoint при оборванном хвосте либо
    /// переигрываются все, либо не переигрывается ни один. Одиночный документ
    /// оставляет прежний вид записи ради совместимости существующих WAL.
    ///
    /// Вид записи зависит от того, есть ли в пачке массивы. У плоского
    /// документа запись остаётся прежней — `[id, значения по полям]`, — и
    /// журналы существующих индексов читаются как читались. Документ с
    /// массивом получает свой вид записи: дописать слоты в прежний формат
    /// значило бы, что одно и то же тело читают двумя способами в зависимости
    /// от схемы, а разойдясь однажды, они положили бы значения не в те ячейки.
    fn log_docs(
        &self,
        replacing: bool,
        docs: &[nested::Doc],
        ready: Option<&[Prepared]>,
    ) -> Result<Option<WalTicket>, String> {
        let Some(wal) = self.wal.as_ref() else { return Ok(None) };
        // Форма схемы берётся из копии по той же причине, что и план: она не
        // меняется с момента старта, а замок буфера — общий для всех пишущих.
        let (nfields, grouped) = self.shape;
        let encode_started = stats::ENABLED.then(Instant::now);
        // Вид записи решается так же, как путь укладки: у схемы с массивами он
        // всегда ячейковый. Иначе переигрывание положило бы документ коротким
        // путём и повторило бы на диске ту же поломку.
        //
        // Готовый вид берётся у подготовки, если она была: собирать его заново
        // значит копировать текст каждого поля второй раз за запрос.
        // Ссылки, а не копии: собрать вид заново — та же цена, что и не
        // переиспользовать его вовсе.
        let fallback: Vec<Option<Vec<String>>>;
        let flat: Vec<&Option<Vec<String>>> = match ready {
            Some(r) if r.len() == docs.len() => r.iter().map(|p| &p.values).collect(),
            _ => {
                fallback = docs
                    .iter()
                    .map(|d| if grouped || self.lossless { None } else { d.flat(nfields) })
                    .collect();
                fallback.iter().collect()
            }
        };
        // Lossless source физически укладывается ячейками даже у плоской
        // схемы: только этот вид сохраняет различие missing/empty и канонический
        // `_source`. Когда `ready` отсутствует (PUT/CAS/recovery), одного
        // `Doc::flat` недостаточно для выбора WAL-вида: прежний код записывал
        // плоский PUT рядом с ADD_CELLS, а после crash raw buffer получал
        // арность только у части документов и отвергал delta publication.
        let cells = (self.lossless && !grouped) || flat.iter().any(|x| x.is_none());
        let kind = match (replacing, cells) {
            (false, false) => svindb_core::wal::ADD,
            (true, false) => svindb_core::wal::PUT,
            (false, true) => svindb_core::wal::ADD_CELLS,
            (true, true) => svindb_core::wal::PUT_CELLS,
        };
        // Тело пачки собирается **одним** буфером.
        //
        // Прежде каждый документ заводил свой `Vec<u8>`, тот копировался в тело
        // пачки, а тело — ещё раз, уже в поток журнала. Три копии текста на
        // запись, все в потоке запроса. Осталась одна.
        //
        // Плоский документ кодируется по ссылкам: ни идентификатор, ни значения
        // полей не копируются в промежуточные строки.
        // Свой вектор ссылок на каждый вызов: заимствования живут ровно столько,
        // сколько документ, и вынести его наружу нельзя, не связав их сроки.
        // Выделение здесь — одно на документ и без содержимого, тогда как
        // выносилось из-под замка целое тело записи.
        fn one(cells: bool, d: &nested::Doc, f: &Option<Vec<String>>, out: &mut Vec<u8>) {
            match (cells, f) {
                (false, Some(values)) => {
                    let mut refs: Vec<&str> = Vec::with_capacity(values.len() + 1);
                    refs.push(d.id.as_str());
                    refs.extend(values.iter().map(|s| s.as_str()));
                    svindb_core::wal::encode_strings_into(out, &refs);
                }
                // Ячейковый вид собирает свои строки сам: у него на ячейку
                // приходится номер поля и номер слота, которых в исходном
                // документе как строк нет.
                _ => {
                    let parts = d.wal_parts();
                    let borrowed: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
                    svindb_core::wal::encode_strings_into(out, &borrowed);
                }
            }
        }
        let records: Vec<Vec<u8>> = if docs.len() > 1 {
            let mut body = Vec::new();
            body.push(kind);
            body.extend((docs.len() as u32).to_le_bytes());
            let mut piece = Vec::new();
            for (d, f) in docs.iter().zip(flat.iter()) {
                piece.clear();
                one(cells, d, f, &mut piece);
                body.extend((piece.len() as u32).to_le_bytes());
                body.extend_from_slice(&piece);
            }
            vec![body]
        } else {
            docs.iter()
                .zip(flat.iter())
                .map(|(d, f)| {
                    let mut out = Vec::new();
                    one(cells, d, f, &mut out);
                    out
                })
                .collect()
        };
        let outer_kind =
            if records.len() == 1 && docs.len() > 1 { svindb_core::wal::BATCH } else { kind };
        let last = match wal.stage(outer_kind, &records) {
            Ok(last) => last,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::InvalidInput {
                    return Err(format!("WAL_RECORD_TOO_LARGE: {error}"));
                }
                eprintln!("журнал исчерпал нумерацию, приём остановлен: {error}");
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(error.to_string());
            }
        };
        if let Some(encode_started) = encode_started {
            self.metrics.wal_encode(encode_started.elapsed().as_secs_f64() * 1000.0);
        }
        match wal.commit(last) {
            Ok(n) => Ok(Some(WalTicket { last: n, records: records.len() })),
            Err(e) => {
                eprintln!("журнал сломался, приём остановлен: {e}");
                self.poisoned.store(true, Ordering::SeqCst);
                Err(e.to_string())
            }
        }
    }

    fn log_mutation(
        &self,
        docs: &[nested::Doc],
        deletes: &[String],
    ) -> Result<Option<WalTicket>, String> {
        let Some(wal) = self.wal.as_ref() else { return Ok(None) };
        let (nfields, grouped) = self.shape;
        let encode_started = stats::ENABLED.then(Instant::now);
        let flat: Vec<Option<Vec<String>>> =
            docs.iter().map(|doc| doc.flat(nfields).filter(|_| !grouped)).collect();
        // Та же инварианта для атомарной mutation: runtime применяет lossless
        // документы через `replace_cells`, поэтому WAL обязан восстановить их
        // тем же cell-путём, иначе смешение PUT и PUT_CELLS ломает арность
        // raw buffer на первом recovery flush.
        let cells = (self.lossless && !grouped) || flat.iter().any(Option::is_none);
        let kind = if cells { svindb_core::wal::PUT_CELLS } else { svindb_core::wal::PUT };
        let bodies: Vec<Vec<u8>> = docs
            .iter()
            .zip(flat.iter())
            .map(|(doc, values)| {
                let parts: Vec<String> = match (cells, values) {
                    (false, Some(values)) => {
                        let mut parts = Vec::with_capacity(values.len() + 1);
                        parts.push(doc.id.clone());
                        parts.extend(values.iter().cloned());
                        parts
                    }
                    _ => doc.wal_parts(),
                };
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
                svindb_core::wal::encode_strings(&refs)
            })
            .collect();
        let body = svindb_core::wal::encode_mutation(kind, &bodies, deletes);
        let last = match wal.stage(svindb_core::wal::MUTATION, &[body]) {
            Ok(last) => last,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::InvalidInput {
                    return Err(format!("WAL_RECORD_TOO_LARGE: {error}"));
                }
                eprintln!("журнал исчерпал нумерацию, смешанная мутация остановлена: {error}");
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(error.to_string());
            }
        };
        if let Some(encode_started) = encode_started {
            self.metrics.wal_encode(encode_started.elapsed().as_secs_f64() * 1000.0);
        }
        match wal.commit(last) {
            Ok(last) => Ok(Some(WalTicket { last, records: 1 })),
            Err(error) => {
                eprintln!("журнал сломался, смешанная мутация остановлена: {error}");
                self.poisoned.store(true, Ordering::SeqCst);
                Err(error.to_string())
            }
        }
    }

    /// Можно ли ещё что-то обещать.
    fn healthy(&self) -> Result<(), String> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err("журнал сломан: приём остановлен, узел надо перезапустить — \
                        переигрывание журнала покажет, что уцелело"
                .into());
        }
        // Отказ фоновой записи сегмента — то же самое по последствию: приём уже
        // ответил «принято», а на диск не легло. Прежде ядро на этом паниковало,
        // и при `panic = "abort"` полный диск на одном сегменте убивал узел со
        // всеми соединениями. Теперь ядро останавливает приём и называет
        // причину, а узел обязан её передать: молчание здесь хуже отказа.
        if let Some(why) = self.ingest.lock().expect("буфер приёма").poisoned() {
            return Err(format!(
                "запись сегмента отказала, приём остановлен: {why}. \
                 Узел надо перезапустить — переигрывание журнала покажет, что уцелело"
            ));
        }
        // Durable ACK без поисковой видимости — половина договора. Если новый
        // состав не открылся, дальнейший приём только наращивает WAL и число
        // подтверждённых, но невидимых документов. Фоновый пересмотр каталога
        // продолжает работать независимо и сам снимет этот временный
        // backpressure после первой удачи; перезапуск не требуется.
        let stale = self.reloads.failures();
        if stale != 0 {
            return Err(format!(
                "состав сегментов устарел после {stale} неудачных перечитываний: {}; \
                 приём временно остановлен до успешного перечитывания",
                self.reloads.last()
            ));
        }
        Ok(())
    }

    /// Продвигает отметку журнала и обрезает то, что больше не нужно.
    ///
    /// Зовётся **после** сброса сегмента и сброса карт живых, и порядок здесь
    /// не косметический: гашение, применённое только в отображении, при потере
    /// питания исчезло бы вместе с уже обрезанным журналом.
    fn after_flush(&self) {
        // Отметка — по тому, что легло в сегмент, а не по тому, что принято в
        // буфер. Иначе она объявляет долговечным документ, живущий в памяти:
        // воспроизводилось пачкой из трёх при сегменте в два.
        let upto = { self.ingest.lock().expect("буфер приёма").flushed_seq };
        self.ready_seq.fetch_max(upto, Ordering::Release);
        self.checkpoint_after_sync(upto);
        maintain::ensure_initial_catalog(&self.dir);
        self.drop_flushed_page_cache(upto);
    }

    /// Наблюдение долга не зависит от разрешения выполнять compaction.
    /// Тяжёлый проход metadata выполняется вне visibility/layout locks.
    fn observe_compaction_debt(&self, cfg: maintain::Config) {
        let generation = self.visibility_generation.load(Ordering::Acquire);
        if generation & 1 != 0 {
            return;
        }
        let ready = self.ready_seq.load(Ordering::Acquire);
        let view = self.live.read().expect("набор сегментов").clone();
        let refs: Vec<&Index> = view.list.iter().map(AsRef::as_ref).collect();
        let debt = maintain::backlog(&refs, cfg);
        let bytes =
            if debt == 0 { 0 } else { refs.iter().map(|segment| segment.bytes.total).sum() };
        let Ok(_visibility) = self.visibility.try_read() else {
            return;
        };
        let Ok(_layout) = self.layout.try_lock() else {
            return;
        };
        if self.visibility_generation.load(Ordering::Acquire) != generation
            || !Arc::ptr_eq(&view, &self.live.read().expect("набор сегментов"))
        {
            return;
        }
        self.compaction_pending.store(debt, Ordering::Release);
        self.compaction_pending_bytes.store(bytes, Ordering::Release);
        self.compaction_floor.fetch_min(debt, Ordering::AcqRel);
        if debt == 0
            && self.compaction_running.load(Ordering::Acquire) == 0
            && !self.poisoned.load(Ordering::Acquire)
            && self.reloads.failures() == 0
        {
            // ready снят до оценки, а не после: новый flush не получает
            // подтверждение settlement по составу предыдущего поколения.
            self.settled_seq.fetch_max(ready, Ordering::Release);
        }
    }

    /// Освобождает clean file cache только у новых durable flush-сегментов.
    ///
    /// Писатель уже завершил fsync и публикацию, а WAL checkpoint выше либо
    /// продвинут, либо оставлен для безопасного replay. Поэтому DONTNEED не
    /// меняет durable state. `try_write` сохраняет query-first договор: при
    /// активном point/search страницы не вытесняются и flush не ждёт читателя.
    fn drop_flushed_page_cache(&self, upto: u64) {
        if !self.drop_flushed_cache.load(Ordering::Relaxed) {
            return;
        }
        let Ok(_exclusive) = query_cache_gate().try_write() else { return };
        let previous = self.cache_advised_seq.load(Ordering::Acquire);
        if upto <= previous {
            return;
        }
        let segments = self.live.read().expect("набор сегментов").clone();
        for segment in &segments.list {
            if segment.src.is_memory() || segment.seq <= previous || segment.seq > upto {
                continue;
            }
            segment.src.drop_immutable_mapping_cache();
            host::drop_immutable_segment_cache(&segment.dir);
        }
        self.cache_advised_seq.store(upto, Ordering::Release);
    }

    /// Синхронизирует карты живых и двигает WAL до уже долговечной границы.
    ///
    /// Обычный сброс берёт границу из `flushed_seq`. Восстановление знает ещё
    /// один безопасный случай: тело уже в опубликованном сегменте, а переигранные
    /// гашения станут долговечными после `sync_live`. Для него граница
    /// передаётся явно.
    fn checkpoint_after_sync(&self, upto: u64) {
        let Some(wal) = self.wal.as_ref() else { return };
        // Карта живых не легла — отметку двигать нельзя: обрезать журнал,
        // потеряв гашения, значит воскресить удалённое.
        if let Err(e) = self.sync_live() {
            eprintln!("отметка журнала не сдвинута: карта живых не легла — {e}");
            self.poisoned.store(true, Ordering::SeqCst);
            return;
        }
        if upto == 0 {
            return;
        }
        // Замок держится через **обе** записи — файла отметки и обрезки. Иначе
        // два потока успевали переставить их местами: сосед записывал двести и
        // обрезал по двести, а отставший поверх записывал сто, оставляя отметку
        // указывающей на журнал, которого уже нет.
        let mut gate = self.checkpoint_gate.lock().unwrap_or_else(|e| e.into_inner());
        if *gate >= upto {
            return;
        }
        if let Err(e) = svindb_core::wal::set_checkpoint(&self.dir, upto) {
            eprintln!("отметка журнала: {e}");
            return;
        }
        svindb_core::fail::point("checkpoint.after_write");
        *gate = upto;
        if let Err(e) = svindb_core::wal::trim(&self.dir, upto) {
            eprintln!("обрезка журнала: {e}");
        }
        svindb_core::fail::point("checkpoint.after_trim");
        let _ = wal;
    }

    /// Порядок — сначала гашение, потом добавление, и переставить его нельзя:
    /// иначе только что положенный документ найдётся сам и будет погашен.
    #[cfg(test)]
    fn put(&self, docs: Vec<nested::Doc>) -> Result<(usize, usize), String> {
        match self.put_if(docs, &[])? {
            Ok(done) => Ok(done),
            Err(_) => unreachable!("без условия конфликт невозможен"),
        }
    }

    /// Атомарно проверяет условия и заменяет всю пачку.
    ///
    /// Атомарность здесь состоит из трёх частей: условная запись исключает
    /// другие записи от проверки до применения; вся пачка одним
    /// group commit переживает `fsync`; автоматический порог сегмента не режет
    /// пачку посередине. Условия сравнивают хранимое поле текущей живой версии.
    fn put_if(
        &self,
        docs: Vec<nested::Doc>,
        conditions: &[PutCondition],
    ) -> Result<Result<(usize, usize), PutConflict>, String> {
        self.healthy()?;
        let mut prepared = self.pretokenize(&docs);
        let mut fragment = self.take_scratch();
        for (doc, ready) in docs.iter().zip(prepared.iter_mut()) {
            nested::put_doc_pre(&mut fragment, doc, true, Some(ready.words.as_mut_slice()));
        }
        let _access = self.mutation_access(!conditions.is_empty());
        let mut ids: Vec<String> = docs.iter().map(|d| d.id.clone()).collect();
        ids.extend(conditions.iter().map(|c| c.id.clone()));
        let _keys = if conditions.is_empty() { Vec::new() } else { self.lock_ids(&ids) };
        if let Some(conflict) = self.check_conditions(conditions)? {
            return Ok(Err(conflict));
        }
        let seq = self.log_docs(true, &docs, Some(&prepared))?;
        self.healthy()?;
        let _apply = self.claim_apply(seq)?;
        let (replaced, flushed) = {
            let _visibility = self.visibility.write().expect("публикация replacement");
            let _generation = VisibilityMutation::begin(&self.visibility_generation);
            let mut ix = self.lock_ingest();
            ix.begin_atomic_batch();
            if let Some(ticket) = seq {
                ix.seq = ticket.last;
            }
            let mut replaced = 0;
            let mut flushed = self.push(&mut ix, |ix| {
                replaced = ix.absorb_replacing(&mut fragment);
            });
            let before = ix.segments();
            ix.finish_atomic_batch();
            self.observe_flushes(&mut ix);
            flushed |= ix.segments() != before;
            self.deleted.fetch_add(replaced as u64, Ordering::Relaxed);
            (replaced, flushed)
        };
        self.give_scratch(fragment);
        self.docs.fetch_add(docs.len() as u64, Ordering::Relaxed);
        if flushed {
            self.after_flush();
        }
        svindb_core::fail::point("put.after_apply_before_ack");
        // Синхронный сброс мог отказать уже после применения пачки. WAL её
        // удерживает, но HTTP не должен скрывать, что узел остановил приём.
        self.healthy()?;
        Ok(Ok((docs.len(), replaced)))
    }

    /// Одна conditional mutation для canonical replacement: проверки, новые
    /// версии и гашения старых derived ID разделяют lock set и одну WAL CRC.
    fn mutate_if(
        &self,
        docs: Vec<nested::Doc>,
        deletes: &[String],
        conditions: &[PutCondition],
    ) -> Result<Result<(usize, usize, usize), PutConflict>, String> {
        self.healthy()?;
        let _access = self.mutation_access(true);
        let prepared = self.pretokenize(&docs);
        let doc_ids: std::collections::HashSet<&str> =
            docs.iter().map(|doc| doc.id.as_str()).collect();
        if let Some(id) = deletes.iter().find(|id| doc_ids.contains(id.as_str())) {
            return Err(format!("смешанная мутация одновременно заменяет и удаляет id {id:?}"));
        }
        let mut ids: Vec<String> = docs.iter().map(|doc| doc.id.clone()).collect();
        ids.extend(deletes.iter().cloned());
        ids.extend(conditions.iter().map(|condition| condition.id.clone()));
        let _keys = self.lock_ids(&ids);
        if let Some(conflict) = self.check_conditions(conditions)? {
            return Ok(Err(conflict));
        }

        let ticket = self.log_mutation(&docs, deletes)?;
        self.healthy()?;
        let _apply = self.claim_apply(ticket)?;
        // Барьер видимости берётся здесь, а не в начале, и это исправление, а не
        // перестановка ради билета. Его назначение — скрыть окно **между
        // публикацией новых версий и гашением удаляемых**, то есть участок
        // применения. Стоя выше, он держал обычные чтения ещё и на время
        // проверки CAS и всей фиксации журнала с её `fsync` — платили за это все
        // читатели узла, ничего не получая взамен. Теперь он покрывает ровно то,
        // что обязан, и не покрывает ожидания своей очереди.
        let _visibility = self.visibility.write().expect("барьер видимости mutation");
        let _generation = VisibilityMutation::begin(&self.visibility_generation);
        // Единый номер мутации не вправе попасть в заголовок сегмента после
        // одной upsert-половины: checkpoint решил бы, что delete-половина тоже
        // на диске, и мог бы обрезать её WAL. Номер ставится ниже, когда обе
        // половины применены. До этого WAL остаётся единственным обязательством
        // и при падении переигрывается целиком.
        let (replaced, flushed) = self.replace_prepared_now(&docs, None, prepared)?;
        self.docs.fetch_add(docs.len() as u64, Ordering::Relaxed);
        svindb_core::fail::point("mutation.after_replace_before_delete");
        let deleted = match self.delete_now(deletes) {
            Ok(deleted) => deleted,
            Err(error) => {
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(format!(
                    "durable смешанная мутация частично применилась в памяти: {error}; \
                     приём остановлен, restart переиграет единую WAL-запись"
                ));
            }
        };
        if let Some(ticket) = ticket {
            self.ingest.lock().expect("буфер приёма").seq = ticket.last;
        }
        svindb_core::fail::point("mutation.after_delete_before_ack");
        if flushed {
            self.after_flush();
        }
        self.healthy()?;
        Ok(Ok((docs.len(), replaced, deleted)))
    }

    /// Проверяет CAS под уже взятыми stripes идентификаторов.
    fn check_conditions(&self, conditions: &[PutCondition]) -> Result<Option<PutConflict>, String> {
        if conditions.is_empty() {
            return Ok(None);
        }
        let mut ix = self.lock_ingest();
        let before = ix.segments();
        ix.drain();
        self.observe_flushes(&mut ix);
        if ix.segments() != before {
            self.publish(&mut ix);
        }
        for condition in conditions {
            let Some(field) = ix.field_names().iter().position(|field| field == &condition.field)
            else {
                return Err(format!("условие ссылается на неизвестное поле {:?}", condition.field));
            };
            let has_source = ix
                .field_names()
                .iter()
                .position(|name| name == nested::SOURCE_FIELD)
                .is_some_and(|source| ix.field_stored(source));
            if !ix.field_stored(field) && !has_source {
                return Err(format!(
                    "условие по полю {:?} невозможно: поле объявлено nostore",
                    condition.field
                ));
            }
            let actual = self.current_value(&mut ix, &condition.id, field, &condition.field)?;
            if actual != condition.expected {
                return Ok(Some(PutConflict {
                    id: condition.id.clone(),
                    field: condition.field.clone(),
                }));
            }
        }
        Ok(None)
    }

    /// Значение последней живой версии: сперва более новый буфер, затем сегменты
    /// от новых к старым. Вызывается с `ingest`, поэтому новый документ с тем же
    /// id не вклинится; полосатый замок удерживается вызывающим.
    fn current_value(
        &self,
        ix: &mut Indexer,
        id: &str,
        field: usize,
        field_name: &str,
    ) -> Result<Option<String>, String> {
        let kind = ix.field_kinds().get(field).copied().unwrap_or(Kind::Text);
        if let Some(source_field) =
            ix.field_names().iter().position(|name| name == nested::SOURCE_FIELD)
        {
            if let Some(doc) = ix.buffered_doc(id) {
                let source = ix.buffered_at(doc, source_field);
                let projection = ix
                    .field_names()
                    .iter()
                    .position(|name| name == nested::PROJECTION_FIELD)
                    .and_then(|projection_field| ix.buffered_at(doc, projection_field))
                    .filter(|raw| !raw.is_empty())
                    .or(source);
                if let Some(raw) = projection {
                    return Ok(projection_scalar(&raw, field_name, kind));
                }
            }
        }
        if let Some(v) = ix.buffered_value(id, field) {
            return Ok(Some(v));
        }
        let segs = { self.live.read().expect("набор сегментов").clone() };
        let hash = bloom::hash(id);
        let mut found = Vec::new();
        for seg in segs.list.iter().rev() {
            found.clear();
            seg.idmap.find_all(id, hash, &mut found);
            for &doc in found.iter().rev() {
                if !seg.is_live(doc) {
                    continue;
                }
                let Some(at) = seg.fields.iter().position(|f| f.name == field_name) else {
                    return Ok(None);
                };
                if let Some(source_at) = seg.field_of(nested::SOURCE_FIELD) {
                    let source = seg.fetch(doc, source_at);
                    let projection = seg
                        .field_of(nested::PROJECTION_FIELD)
                        .and_then(|projection_at| seg.fetch(doc, projection_at))
                        .filter(|raw| !raw.is_empty())
                        .or(source);
                    if let Some(raw) = projection {
                        let raw = String::from_utf8_lossy(&raw);
                        return Ok(projection_scalar(&raw, field_name, kind));
                    }
                }
                if !seg.fields[at].stored {
                    return Err(format!(
                        "условие по полю {field_name:?} невозможно: текущая версия лежит в старом nostore-сегменте без lossless source"
                    ));
                }
                let bytes = seg.fetch(doc, at).unwrap_or_default();
                return Ok(Some(String::from_utf8_lossy(&bytes).into_owned()));
            }
        }
        Ok(None)
    }

    /// Отдельно от [`Writer::put`], потому что переигранное из журнала
    /// записывать в него заново нельзя: журнал рос бы при каждом перезапуске.
    ///
    /// Отдаёт число погашенных версий и признак «случился сброс».
    fn replace_prepared_now(
        &self,
        docs: &[nested::Doc],
        seq: Option<WalTicket>,
        mut prepared: Vec<Prepared>,
    ) -> Result<(usize, bool), String> {
        let mut ix = self.lock_ingest();
        // Прежние версии здесь **не гасятся**, и это главное в этой функции.
        //
        // Замена — два события с разной задержкой видимости: старая версия
        // перестаёт быть нужной сразу, новая становится видимой только с
        // публикацией. Гаси старую здесь — и между ними у документа не остаётся
        // ни одной живой версии; поиск отвечает пустотой на документ, который
        // никуда не девался, и тем дольше, чем реже публикация. Под потоком
        // замен документ невидим почти всегда, а не изредка.
        //
        // Поэтому гашение едет с буфером и применяется тем, кто вносит сегмент
        // в состав, — см. [`Writer::publish`] и `Indexer::superseded`. Побочно
        // это снимает с пути записи обход всех сегментов на каждую пачку.
        ix.begin_atomic_batch();
        let mut replaced = 0usize;
        // Замыкание вызывается тут же намеренно: `?` внутри обязан выйти
        // из **него**, а не из всей функции, иначе отказ середины пачки
        // проскочил бы мимо `finish_atomic_batch` и оставил приём в
        // отложенном сбросе навсегда.
        #[allow(clippy::redundant_closure_call)]
        let applied = (|| -> Result<bool, String> {
            let mut flushed = false;
            for (k, doc) in docs.iter().enumerate() {
                if let Some(ticket) = seq {
                    ix.seq = ticket.at(k);
                }
                let mut killed = 0usize;
                if self.push(&mut ix, |ix| {
                    killed =
                        nested::put_doc_pre(ix, doc, true, Some(prepared[k].words.as_mut_slice()));
                }) {
                    flushed = true;
                }
                replaced += killed;
            }
            Ok(flushed)
        })();
        let mut flushed = match applied {
            Ok(flushed) => flushed,
            Err(e) => {
                // WAL уже содержит всю пачку, но память применить её целиком
                // не смогла. Продолжать на частичном состоянии нельзя:
                // запрещаем его публикацию и останавливаем приём; restart
                // переиграет одну атомарную WAL-запись с начала.
                ix.cancel_atomic_batch();
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(format!(
                    "атомарная пачка не применилась целиком: {e}; приём остановлен, \
                     перезапуск переиграет durable WAL"
                ));
            }
        };
        let before_finish = ix.segments();
        ix.finish_atomic_batch();
        self.observe_flushes(&mut ix);
        flushed |= ix.segments() != before_finish;
        self.deleted.fetch_add(replaced as u64, Ordering::Relaxed);
        Ok((replaced, flushed))
    }

    fn delete(&self, ids: &[String]) -> Result<usize, String> {
        self.healthy()?;
        let _access = self.mutation_access(false);
        let _keys = self.lock_ids(ids);
        let mut seq = None;
        if let Some(wal) = self.wal.as_ref() {
            let parts: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
            let last = wal
                .stage(svindb_core::wal::DELETE, &[svindb_core::wal::encode_strings(&parts)])
                .map_err(|error| {
                    self.poisoned.store(true, Ordering::SeqCst);
                    format!("нумерация WAL исчерпана, удаление остановлено: {error}")
                })?;
            match wal.commit(last) {
                Ok(n) => seq = Some(WalTicket { last: n, records: 1 }),
                Err(e) => {
                    eprintln!("журнал сломался, приём остановлен: {e}");
                    self.poisoned.store(true, Ordering::SeqCst);
                }
            }
        }
        self.healthy()?;
        // Номер ставится **после** своей очереди, а не сразу за фиксацией.
        // Иначе гашение, обогнавшее соседнюю пачку с меньшим номером, увело бы
        // `seq` вперёд неё, и следующий сброс объявил бы долговечным то, что
        // ещё не применено.
        let _apply = self.claim_apply(seq)?;
        // Номер ставится **после применения**, а не перед ним.
        //
        // Перед ним он открывал окно, в котором `Indexer::seq` уже включает
        // запись гашения, а бит в карте живых ещё не проставлен. Попади в это
        // окно сброс — и номер уехал бы в заголовок сегмента, `after_flush`
        // переставил бы отметку журнала и обрезал его **по записи, которой нет
        // ни в одной карте живых**. Падение после этого воскрешало бы удалённое,
        // и клиент, получивший «удалено», не имел бы способа об этом узнать.
        //
        // Обратный порядок безобиден: отставшая отметка означает лишь, что
        // гашение переиграется при восстановлении, а оно идемпотентно.
        let out = {
            let _visibility = self.visibility.write().expect("публикация delete");
            let _generation = VisibilityMutation::begin(&self.visibility_generation);
            self.delete_now(ids)?
        };
        // Окно существует намеренно: сначала durable WAL, затем карта живых,
        // и только после неё номер применённого. Точка отказа закрепляет
        // доказательство, что падение ровно здесь не воскресит документ.
        svindb_core::fail::point("delete.after_apply_before_seq");
        if let Some(t) = seq {
            self.ingest.lock().expect("буфер приёма").seq = t.last;
        }
        Ok(out)
    }

    /// Применяет гашение, ничего не записывая в журнал.
    ///
    /// Гасит **и в сегментах, и в буфере приёма**. Второе не мелочь: принятый
    /// документ до полутора секунд лежит только в буфере, и без этого удаление
    /// свежего документа либо пропадало, либо — что хуже — откладывалось и
    /// срабатывало уже после сброса, убивая заодно и его замену.
    fn delete_now(&self, ids: &[String]) -> Result<usize, String> {
        let mut ix = self.lock_ingest();
        // HTTP-контракт считает удалённые **логические** документы, а не
        // физические версии одного id. После delta-only refresh один id может
        // одновременно жить в нескольких frozen-поколениях (все они нужны
        // поиску до следующего flush), поэтому простая сумма `kill_buffered`
        // и `delete_in.killed` стала бы завышать ответ: один `relation-old`
        // возвращался как `deleted: 2`. Физический счётчик writer при этом
        // сохраняется отдельно — он нужен для оценки мёртвых версий и
        // compaction debt.
        let mut logical = std::collections::HashSet::new();
        let mut killed: usize = 0;
        for id in ids {
            let buffered = ix.kill_buffered(id);
            killed = killed.saturating_add(buffered);
            if buffered > 0 {
                logical.insert(id.clone());
            }
        }
        // Состав перечитывается **всегда**, а не «если за этот вызов что-то
        // легло», и обе половины этого важны.
        //
        // Гашение по сегментам обязано идти по свежему набору. Сегмент мог лечь
        // и быть забран приёмом задолго до удаления — приём забирает итог на
        // каждом документе, а состав перечитывает не всегда, — и набор остаётся
        // устаревшим сколько угодно долго. Удаление шло по нему, промахивалось
        // мимо легшего сегмента и отвечало «удалено», не удалив: документ оживал
        // в выдаче, а журнал говорил, что удаление прошло.
        //
        // И живой сегмент обязан пересобраться **после** гашения в буфере: он
        // собран из буфера, каким тот был до удаления, и до следующего срока
        // свежести показывал бы только что удалённый документ живым.
        //
        // Цена — перечитывание списка и пересборка живого сегмента на удаление.
        // Это дороже прежнего, и это правильный размен: удаление, о промахе
        // которого никто не узнает, дороже любой пересборки.
        self.publish(&mut ix);
        let (segment_killed, segment_ids) = self.kill_in_segments(ids)?;
        killed += segment_killed;
        logical.extend(segment_ids);
        self.deleted.fetch_add(killed as u64, Ordering::Relaxed);
        Ok(logical.len())
    }

    /// Гасит идентификаторы в уже записанных сегментах.
    ///
    /// Зовётся с захваченным `ingest`: порядок замков во всём писателе —
    /// сначала буфер, потом состав.
    fn kill_in_segments(
        &self,
        ids: &[String],
    ) -> Result<(usize, std::collections::HashSet<String>), String> {
        let _guard = self.layout.lock().expect("состав сегментов");
        let segs = { self.live.read().expect("набор сегментов").clone() };
        let refs: Vec<&svindb_core::Index> = segs.list.iter().map(|a| a.as_ref()).collect();
        // `delete_in` одновременно запоминает физически погашенные id и
        // меняет live maps. Отдельный предварительный проход здесь был бы
        // дорогим и создавал бы ещё одно окно под layout-lock.
        maintain::delete_in(&refs, ids)
            .map(|out| (out.killed, out.logical.into_iter().collect()))
            .map_err(|e| e.to_string())
    }

    /// Перечитывает набор сегментов, чтобы правки стали видны поиску.
    ///
    /// Отказ не проглатывается: без учёта он означал бы, что узел молча отдаёт
    /// устаревшую выдачу, подтверждая при этом приём. Разбор — у [`Reloads`].
    fn reload(&self) {
        // Переоткрываются только изменившиеся сегменты: на сбросе меняется
        // ровно один, а перезагрузка идёт под замком буфера и замком состава
        // разом. Разбор — у [`Segments::reopen`].
        let prev = { self.live.read().expect("набор сегментов").clone() };
        match Segments::reopen(&self.dir, &prev) {
            Ok(next) => {
                *self.live.write().expect("набор сегментов") = Arc::new(next);
                self.reloads.ok();
            }
            Err(e) => self.reloads.failed("приём", &e.to_string()),
        }
    }

    #[cfg(test)]
    fn flush(&self) {
        {
            let mut ix = self.ingest.lock().expect("буфер приёма");
            ix.flush_now();
            self.observe_flushes(&mut ix);
            self.publish(&mut ix);
        }
        self.after_flush();
    }

    /// Запускает публикацию и ждёт не дольше заданного срока.
    ///
    /// `Ok(true)` — нужное поколение уже на диске, `Ok(false)` — работа
    /// продолжается и HTTP должен ответить `202`, `Err` — публикация отказала.
    fn request_flush(self: &Arc<Self>, wait: Duration) -> Result<bool, String> {
        let ticket = {
            let mut state = self.flush_state.lock().expect("состояние flush");
            state.requested = state.requested.checked_add(1).ok_or_else(|| {
                "нумерация запросов flush исчерпана; узел надо перезапустить до следующей публикации"
                    .to_string()
            })?;
            let ticket = state.requested;
            if !state.running {
                state.running = true;
                state.error.clear();
                let writer = Arc::clone(self);
                if let Err(e) = std::thread::Builder::new()
                    .name("svindb-flush-coordinator".into())
                    .spawn(move || writer.flush_worker())
                {
                    state.running = false;
                    state.error = format!("поток публикации не запустился: {e}");
                    return Err(state.error.clone());
                }
            }
            ticket
        };

        let deadline = Instant::now() + wait;
        let mut state = self.flush_state.lock().expect("состояние flush");
        loop {
            if state.completed >= ticket {
                return Ok(true);
            }
            if !state.error.is_empty() {
                return Err(state.error.clone());
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            let left = deadline.saturating_duration_since(now);
            let (next, timed) = self.flush_done.wait_timeout(state, left).expect("состояние flush");
            state = next;
            if timed.timed_out() && state.completed < ticket {
                return Ok(false);
            }
        }
    }

    fn flush_worker(self: Arc<Self>) {
        loop {
            let goal = self.flush_state.lock().expect("состояние flush").requested;
            let result = self.drive_one_flush();
            let mut state = self.flush_state.lock().expect("состояние flush");
            match result {
                Ok(()) => state.completed = state.completed.max(goal),
                Err(e) => {
                    state.error = e;
                    state.running = false;
                    self.flush_done.notify_all();
                    return;
                }
            }
            self.flush_done.notify_all();
            if state.requested <= state.completed {
                state.running = false;
                return;
            }
        }
    }

    /// Доводит до диска снимок буфера, не удерживая `ingest` во время работы
    /// фонового писателя. Это и отделяет bounded HTTP wait от тяжёлой публикации.
    fn drive_one_flush(&self) -> Result<(), String> {
        self.healthy()?;
        let wal_on = self.wal.is_some();
        let mut sent_without_wal = false;
        let (target, mut landed) = {
            let mut ix = self.ingest.lock().expect("буфер приёма");
            let before = ix.segments();
            ix.poll();
            self.observe_flushes(&mut ix);
            let landed = ix.segments() != before;
            // Цель — по журналу, а не по наполненности буфера.
            //
            // Пустой буфер не значит «всё опубликовано»: документы могли уже
            // уехать к писателю и лежать в сегменте, который пишется прямо
            // сейчас. Пока условие смотрело на буфер, `/_flush` в этот миг
            // отвечал `200 flushed:true`, хотя двести тысяч принятых документов
            // ещё не были видны никому. Обещание публикации нарушалось молча —
            // и ловилось только сверкой счёта на объёме.
            //
            // Номер последней принятой записи против номера последней легшей
            // отвечает на тот самый вопрос, который задаёт клиент: всё ли, что
            // мне подтвердили, уже опубликовано.
            let target = (wal_on && ix.seq > ix.flushed_seq).then_some(ix.seq);
            (target, landed)
        };
        if landed {
            {
                let mut ix = self.ingest.lock().expect("буфер приёма");
                self.publish(&mut ix);
            }
            self.after_flush();
        }

        loop {
            let (done, must_reload) = {
                let mut ix = self.ingest.lock().expect("буфер приёма");
                let before = ix.segments();
                ix.poll();
                let done = match target {
                    Some(seq) if ix.flushed_seq >= seq => true,
                    Some(_) => {
                        if !ix.flush_inflight() && ix.active_buffered() > 0 {
                            ix.flush();
                            false
                        } else {
                            // Нечему лечь — значит и ждать нечего.
                            //
                            // `flushed_seq` двигает **только легший сегмент**, а
                            // `seq` двигает всякая запись журнала, включая ту, у
                            // которой документа нет: гашение. Поэтому после
                            // `/_delete` при пустом буфере `flushed_seq` не мог
                            // догнать `seq` уже никогда, и цикл крутился с шагом
                            // 5 мс до конца жизни процесса. Наружу это выглядело
                            // так: `/_flush` навсегда отвечает `202`, а ядро
                            // занято ничем.
                            //
                            // Хуже кода ответа — то, что при этом не звался
                            // `sync_live`: правка карты живых оставалась только
                            // в кеше страниц, и обещание `/_flush` не
                            // выполнялось вовсе, хотя запрос вернулся.
                            //
                            // Вывод верен потому, что `seq` документа ставится
                            // **под тем же замком**, что и вставка в буфер: если
                            // буфер пуст и писатель не занят, ни одна поднявшая
                            // `seq` запись сегмента не ждёт. Остаток — гашения,
                            // и их долговечность — это `sync_live` ниже.
                            !ix.flush_inflight()
                        }
                    }
                    None if !wal_on => {
                        if sent_without_wal && !ix.flush_inflight() {
                            true
                        } else if !sent_without_wal && !ix.flush_inflight() {
                            if ix.active_buffered() == 0 {
                                true
                            } else {
                                ix.flush();
                                sent_without_wal = true;
                                !ix.flush_inflight()
                            }
                        } else {
                            false
                        }
                    }
                    // Буфер был пуст: flush нужен только картам живых.
                    None => true,
                };
                self.observe_flushes(&mut ix);
                (done, ix.segments() != before)
            };
            if must_reload {
                {
                    let mut ix = self.ingest.lock().expect("буфер приёма");
                    self.publish(&mut ix);
                }
                self.after_flush();
                landed = true;
            }
            self.healthy()?;
            if done {
                if !landed {
                    self.sync_live()?;
                }
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Уносит правки карт живых на диск.
    ///
    /// Не на каждое гашение, а на сброс: правка живёт в кеше страниц, и смерть
    /// процесса ей не страшна — кеш принадлежит ядру. Страшна потеря питания, и
    /// от неё защищает вот это.
    /// Сбрасывает карты живых на диск. `Err` — хотя бы одна не легла.
    fn sync_live(&self) -> Result<(), String> {
        let segs = { self.live.read().expect("набор сегментов").clone() };
        let mut bad = Vec::new();
        // Живой сегмент пропускается явно, а не потому, что синхронизация его
        // карты ничего не делает. Разница в том, что здесь записано **почему**
        // это верно: под ним нет файла, а его гашения — это гашения версий,
        // лежащих в том же буфере, который целиком держит журнал. Терять тут
        // нечего, и отметку журнала это не двигает: она идёт по легшему
        // сегменту, а живой сегмент её не касается вовсе.
        for ix in segs.list.iter().filter(|ix| !ix.src.is_memory()) {
            if let Err(e) = ix.sync_live() {
                eprintln!("сброс карты живых {}: {e}", ix.dir);
                bad.push(format!("{}: {e}", ix.dir));
            }
        }
        if bad.is_empty() {
            Ok(())
        } else {
            Err(bad.join("; "))
        }
    }
}

/// Нужно ли заново публиковать searchable-live view на тике свежести.
///
/// Когда активный буфер уже превысил `live_max_docs`, прошлый живой двойник
/// остаётся корректным снимком части этого буфера, а новый полный двойник всё
/// равно запрещён admission. Повторно снимать/собирать ReadView в таком цикле
/// не только бесполезно, но и держит layout/visibility lock на миллионах
/// пустых решений. Перечитывание после настоящего disk flush (`landed=true`)
/// обязательно: оно меняет canonical состав и должно атомарно попасть в view.
fn refresh_live_needed(landed: bool, buffered: u32, built: u32, limit: u32) -> bool {
    landed || (buffered != built && (limit == 0 || buffered <= limit))
}

/// Наступил ли срок публикации searchable-live view.
///
/// `refresh_interval_ms` — обычный тик после накопления минимальной пачки,
/// `refresh_max_interval_ms` — hard deadline от первого документа, который не
/// даёт редкому индексу остаться невидимым навсегда. Оба значения управляют
/// только видимостью памяти: физический flush проверяется отдельными порогами
/// индексатора и в этот предикат не входит.
fn refresh_due(
    elapsed_ms: u64,
    buffered: u32,
    interval_ms: u64,
    min_docs: u32,
    max_interval_ms: u64,
) -> bool {
    if buffered == 0 {
        return false;
    }
    let regular = interval_ms > 0 && elapsed_ms >= interval_ms && buffered >= min_docs.max(1);
    let hard = max_interval_ms > 0 && elapsed_ms >= max_interval_ms;
    regular || hard
}

/// Удерживает кучу распределителя в границах, выведённых из машины.
///
/// ## Зачем это понадобилось
///
/// Узел умирал от нехватки памяти на тридцати одном миллионе документов,
/// удерживая 18.7 ГБ анонимной кучи при индексе в два гигабайта на диске.
/// Разделяющие опыты сняли подозрения по очереди: слияния ни при чём
/// (`--no-maintain` даёт тот же расход), состав ни при чём (перезапуск на
/// готовом индексе из двадцати одного сегмента стоит **три мегабайта**), буфер
/// приёма ни при чём (он исправно сбрасывается, а живой набор структур не
/// превышает двух с половиной гигабайт и между пятнадцатью и тридцатью одним
/// миллионом **не меняется**).
///
/// Осталось единственное объяснение, и оно подтвердилось: память формально
/// освобождена, но ядру не возвращена. Приём идёт на работниках соединений, их
/// `cores() * 8`, и на тридцати ядрах это ровно двести сорок — предел арен
/// glibc. Каждый работник выделяет строки документа в **своей** арене, а
/// опустошает буфер один поток писателя, и куски расходятся по чужим свободным
/// спискам. Подкуча неосновной арены отдаётся ядру только целиком свободной, а
/// при такой чересполосице она не бывает свободной никогда. Потолок
/// `240 × HEAP_MAX_SIZE` — около пятнадцати гигабайт, тот самый порядок, на
/// котором узел и умер.
///
/// ## Почему предел выводится из ядер, а не из числа работников
///
/// Арены заведены против тесноты между потоками, выделяющими **одновременно**.
/// Здесь таких не двести сорок: приём сериализован общим замком буфера, а
/// запросам отведён отдельный пул по числу ядер. Работники же почти всё время
/// спят на сокете и не выделяют ничего. Поэтому предел выводится из ядер —
/// величины, отражающей настоящую одновременность, — и трогать число
/// работников не приходится: связность узла остаётся прежней.
///
/// Замер на пятнадцати миллионах документов (Linux, тридцать ядер), пик
/// анонимной кучи и скорость приёма:
///
/// | арен | пик | скорость |
/// |---|---|---|
/// | без предела | 8197 МБ | 47 479 док/с |
/// | ядра (30) | 6114 МБ | 47 241 док/с |
/// | ядра/4 (8) | **3053 МБ** | 45 607 док/с |
/// | 2 | 2127 МБ | 41 274 док/с |
///
/// Четверть ядер снимает две трети расхода за четыре процента скорости; ниже
/// начинается теснота, и за следующий гигабайт приходится платить вдесятеро
/// дороже. Отсюда `ядра / 4`, но не меньше двух.
///
/// ## Порог ухода в mmap
///
/// Второй предел прибивает порог, выше которого кусок берётся у ядра напрямую
/// и ему же возвращается при освобождении. По умолчанию порог плавающий: он
/// поднимается сам, стоит освободить крупный кусок, и после первого же сброса
/// сегмента вся лестница до тридцати двух мегабайт начинает браться из арены —
/// то есть навсегда. Прибитый порог снимал 38% расхода отдельной мерой, ценой
/// трёх процентов скорости.
///
/// ## Границы применимости
///
/// `mallopt` — это glibc. На musl его нет, на macOS и прочих распределитель
/// другой и ведёт себя иначе, поэтому вызов ограничен `target_env = "gnu"`.
/// Ошибку возврата глотаем намеренно: неприменённая подсказка распределителю —
/// не повод не запускать узел. Значение переопределяется переменной среды, но
/// умолчание обязано быть верным само по себе.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn hold_the_allocator() {
    // Из `malloc.h`: отрицательные номера — расширения glibc.
    const M_TRIM_THRESHOLD: std::ffi::c_int = -1;
    const M_MMAP_THRESHOLD: std::ffi::c_int = -3;
    const M_ARENA_MAX: std::ffi::c_int = -8;

    unsafe extern "C" {
        fn mallopt(param: std::ffi::c_int, value: std::ffi::c_int) -> std::ffi::c_int;
    }

    let arenas = std::env::var("SVINDB_MALLOC_ARENAS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or_else(|| allocator_arenas(cores(), svindb_core::mem::available()));
    // Порог держим у ста двадцати восьми килобайт: ниже — учащаются обращения
    // к ядру на кусках, которые арена отдала бы даром; выше — начинает
    // оседать та самая лестница.
    const MMAP_THRESHOLD: std::ffi::c_int = 128 * 1024;
    unsafe {
        mallopt(M_ARENA_MAX, arenas as std::ffi::c_int);
        mallopt(M_MMAP_THRESHOLD, MMAP_THRESHOLD);
        mallopt(M_TRIM_THRESHOLD, MMAP_THRESHOLD * 2);
    }
}

/// Число арен glibc выводится сразу из двух конечных ресурсов.
///
/// CPU ограничивает полезный параллелизм, а RAM — сколько освобождённых
/// подкуч распределитель вправе удерживать. Прежнее правило смотрело только на
/// ядра: контейнеру с одним гигабайтом на 30-ядерном хосте назначались семь
/// арен, и он погибал на 9.2 млн документов, хотя каждая подсистема соблюдала
/// собственную долю. Две арены на том же корпусе приняли все 10 млн без OOM.
///
/// Одна арена на два гигабайта — осторожная верхняя граница; минимум две
/// сохраняет параллелизм управляющего и рабочего потоков. На большой машине
/// прежний CPU-предел остаётся старшим, поэтому исправление не размножает арены
/// вместе с объёмом памяти.
#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
fn allocator_arenas(cpu_cores: usize, memory_bytes: Option<u64>) -> usize {
    let by_cpu = (cpu_cores / 4).max(2);
    let by_memory = memory_bytes
        .map(|bytes| usize::try_from(bytes / (2 << 30)).unwrap_or(usize::MAX).max(2))
        .unwrap_or(by_cpu);
    by_cpu.min(by_memory)
}

#[cfg(test)]
mod allocator_policy_tests {
    use super::allocator_arenas;

    #[test]
    fn a_small_cgroup_limits_arenas_even_on_a_large_host() {
        assert_eq!(allocator_arenas(30, Some(1 << 30)), 2);
        assert_eq!(allocator_arenas(30, Some(4 << 30)), 2);
        assert_eq!(allocator_arenas(30, Some(8 << 30)), 4);
        assert_eq!(allocator_arenas(30, Some(16 << 30)), 7);
    }

    #[test]
    fn cpu_and_unknown_memory_keep_the_existing_ceiling() {
        assert_eq!(allocator_arenas(2, Some(128 << 30)), 2);
        assert_eq!(allocator_arenas(8, Some(128 << 30)), 2);
        assert_eq!(allocator_arenas(32, Some(128 << 30)), 8);
        assert_eq!(allocator_arenas(30, None), 7);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn hold_the_allocator() {}

fn main() {
    // Первым делом и до единого потока: `M_ARENA_MAX` действует на арены,
    // заводимые после него, а заведённую арену уже не отобрать.
    hold_the_allocator();
    let args: Vec<String> = std::env::args().collect();
    let mutation_root = match args.get(1).map(String::as_str) {
        Some("restore") => args.get(3),
        Some("merge" | "bloom" | "pack" | "unpack") => args.get(2),
        _ => None,
    };
    if let Some(root) = mutation_root {
        let root = std::path::Path::new(root);
        if let Err(error) =
            migration::ensure_ready(root).and_then(|_| migration::ensure_writable(root))
        {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
    match args.get(1).map(String::as_str) {
        Some("index") => cmd_index(&args),
        Some(
            "migration-preflight"
            | "migration-segment"
            | "migration-encoded-segment"
            | "catalog-build",
        ) => {
            if let Err(error) = migration::command(&args) {
                eprintln!("миграция остановлена: {error}");
                std::process::exit(1);
            }
        }
        Some("inspect") => cmd_inspect(&args),
        Some("check") => cmd_check(&args),
        Some("merge") => cmd_merge(&args),
        Some("bloom") => cmd_bloom(&args),
        Some("unpack") => cmd_repack(&args, false),
        Some("pack") => cmd_repack(&args, true),
        Some("wal") => cmd_wal(&args),
        Some("serve") => cmd_serve(&args[1..]),
        Some("restore") => cmd_restore(&args),
        Some("passwd") => cmd_passwd(&args),
        Some("snapshot") => cmd_snapshot(&args),
        Some("snapshot-prune") => cmd_snapshot_prune(&args),
        Some("--version" | "-V") => println!(
            "svindb {} revision={} target={} source_date_epoch={}",
            host::version(),
            host::revision(),
            host::target(),
            host::build_epoch()
        ),
        Some("--help" | "-h") => help(),
        // Флаг, который не узнали, — это **не** просьба поднять узел.
        //
        // Пока здесь стоял один `Some(_) => cmd_serve`, всякий, кто по общему
        // рефлексу набирал `svindb --help`, получал работающую базу: она брала
        // writer-lock на каталог `data` рядом с бинарником и слушала 19300, ни
        // словом об этом не сказав. То же случалось на любой опечатке в имени
        // подкоманды. Умолчание «поднять узел» осталось только для того, ради
        // чего оно и заводилось, — для `svindb <каталог> <порт>` без слова
        // `serve`; а всё, что начинается с дефиса, теперь получает подсказку.
        Some(x) if x.starts_with('-') => {
            eprintln!("неизвестный флаг {x:?}; узел так не поднимается");
            usage()
        }
        Some(value)
            if value.starts_with('/')
                || value.starts_with("./")
                || value.starts_with("../")
                || value.contains('/')
                || std::path::Path::new(value).is_dir()
                || value.parse::<u16>().is_ok() =>
        {
            cmd_serve(&args)
        }
        Some(value) => {
            eprintln!("unknown command {value:?}; use `svindb serve <directory> [port]`");
            usage()
        }
        None => usage(),
    }
}

/// Потолок кеша распакованных кусков на один ответ.
///
/// Байтовый, а не в числе кусков: кусок бывает и в четыре килобайта, и во много
/// больше, если в нём одно длинное значение. Восемь мегабайт покрывают страницу
/// в сотню документов с запасом и ограничивают худший случай — ответ, у которого
/// каждый документ в своём куске.
const FETCH_CACHE: usize = 8 << 20;

/// Сколько миллисекунд отведено прогреву.
///
/// Две минуты — не «сколько не жалко», а порядок, за которым прогрев перестаёт
/// быть прогревом: узел уже отвечает, страницы приходят по обращению, и фоновое
/// чтение всего диска только мешает тем запросам, которые идут.
const WARM_MS: u64 = 120_000;

/// Имя живого сегмента в составе.
///
/// Одно на узел, потому что живой сегмент один: он пересобирается целиком и
/// подменяет предыдущий. Путём это имя не является намеренно — под ним нет
/// каталога, и открыть по нему нечего.
const LIVE_SEGMENT: &str = "живой-сегмент";

/// Сколько идентификаторов принимает один `/_mget`.
///
/// Столько же, сколько документов отдаёт самая глубокая страница поиска: обе
/// точки стоят обхода сегментов на документ, и разрешать одной вдесятеро больше
/// значило бы оставить дыру в том же заборе.
const MAX_MGET: usize = 10_000;

/// Разрешённый bounded fallback для очень маленького состава.
///
/// На восстановленном одно-сегментном индексе каталог может ещё не успеть
/// появиться сразу после offline restore. Полный отказ сделал бы такой индекс
/// временно нечитаемым, хотя один `idmap` дешевле подготовки каталога.
/// Ограничение намеренно маленькое: на production-составе с сотнями сегментов
/// request path никогда не превращается в линейный обход.
const POINT_SCAN_FALLBACK_MAX_SEGMENTS: usize = 8;

/// Подсказка одним текстом.
///
/// Вынесена из [`usage`], потому что читателей у неё два и ведут они себя
/// по-разному: спросивший `--help` получает её в stdout и код `0`, а
/// ошибившийся — в stderr и код `2`. Печатать подсказку в stderr тому, кто её
/// **просил**, значит ломать `svindb --help | less`.
const HELP: &str = "svindb serve   [порт]                       каталог data рядом с svindb\n\
         svindb serve   [каталог] [порт] [--bind=АДРЕС] [--warm[=eager|lazy]] [--threads=N]\n\
         svindb serve   [каталог] [--read-only] [--no-maintain] [--flush=N] [--target=N] [--no-wal]\n\
         \x20 [--allow-low-map-count]  не отказывать, когда vm.max_map_count мал\n\
         новый или multi-index каталог открывается в режиме нескольких индексов\n\
         \x20 PUT /имя создаёт индекс; /имя/_bulk и /имя/_search выбирают его\n\
         \x20 [--max-indices=N] ограничивает одновременно открытые индексы (64)\n\
         существующий одноиндексный каталог распознаётся и открывается автоматически\n\
         \x20 [--single-index] явно создаёт новый каталог прежнего формата\n\
         \x20 [--fields=поле:вес|тип,...] схема по умолчанию для нового индекса\n\
         svindb serve   [каталог] --config=ФАЙЛ   права доступа, CORS, пути снимков\n\
         svindb serve   [каталог] --tls-cert=ФАЙЛ --tls-key=ФАЙЛ   сборка с --features tls\n\
         svindb serve   --shards=host:port,...   координатор: рассылает и сливает\n\
         \x20 копии шарда через плюс: --shards=a:1+b:1,c:2\n\
         \x20 полоса ячеек через собаку: --shards=a:1@0-2048,b:2@2048-4096\n\
         \x20 без полос ячейки делятся поровну; после деления шарда — задавать\n\
         \x20 в запросе scoring=global — точные скоры двумя кругами\n\
         svindb serve   --replicas=host:port,... --term=N [--ack=one|quorum|all]\n\
         \x20 узел становится primary шарда: применяет у себя и рассылает копиям\n\
         \x20 терм обязателен — им реплика отличает нынешнего primary от смещённого\n\
         \x20 умолчание договора — quorum; недостача подтверждений отвечает 503\n\
         \x20 [--replicate-keep-mb=N] кольцо разосланного для догона (64)\n\
         \x20 [--replicate-mbps=N] полоса догона, 0 — без предела (32)\n\
         настройки индекса живут в settings.json рядом с ним; аргументы старше:\n\
         \x20 [--refresh-ms=N] [--merge-at-once=N] [--dead-fraction=Д] [--grace-ms=N]\n\
         \x20 [--merge-threads=N]  ширина слияния; 0 — четверть ядер\n\
         \x20 посмотреть и поменять на ходу: GET / PUT /_settings (нужен admin)\n\
         svindb snapshot <каталог> <приёмник> <имя> [--fast]  снять слепок\n\
         svindb restore  <архив> <каталог>   поднять слепок; через API нельзя\n\
         svindb passwd   [повторов]          хеш пароля для файла настроек\n\
         приём: POST /_bulk (index/delete), /_put (условная замена), /_delete, /_flush\n\
         svindb index   <ndjson> <каталог> [док_на_сегмент] [поле:вес|тип,...]\n\
         svindb merge   <каталог> [сегментов_за_раз] [потолок_документов]\n\
         svindb bloom   <каталог> [бит_на_документ]   пересобрать фильтры\n\
         svindb unpack  <каталог>   разложить seg.dat сегментов по файлам\n\
         svindb pack    <каталог>   собрать файлы сегментов обратно в seg.dat\n\
         svindb wal     <каталог>   что лежит в журнале приёма\n\
         svindb migration-preflight <индекс>   строгий read-only gate WAL и metadata\n\
         svindb migration-segment <индекс> <сегмент> <новый-каталог> <док-на-сегмент>\n\
         svindb migration-encoded-segment <индекс> <сегмент> <новый-каталог> <док-на-сегмент>\n\
         svindb catalog-build <индекс> <entries-в-памяти>   офлайновый производный ID catalog\n\
         svindb check   <каталог> [--full] [--json]   целость индекса, только чтение\n\
         svindb inspect <каталог> [--deep] [--json]   из чего состоит место\n\
         svindb inspect <каталог> --plan  [--json]    во что обошлась бы новая раскладка";

/// Подсказка тому, кто ошибся: в stderr и с ненулевым кодом.
fn usage() -> ! {
    eprintln!("{HELP}");
    std::process::exit(2)
}

/// Подсказка тому, кто её просил: в stdout и с нулевым кодом.
fn help() -> ! {
    println!("{HELP}");
    std::process::exit(0)
}

/// Схема каталога так, как её объявили при сборке индекса.
///
/// `None` — реестра нет, то есть каталог пуст и объявлять нечего. Похороненные
/// поля пропускаются: писать в них уже нельзя, а место в ряду значений они
/// занимали бы, сдвигая все последующие.
fn schema_of(dir: &str) -> Option<Vec<svindb_core::schema::Decl>> {
    use svindb_core::schema::Decl;
    let reg = svindb_core::schema::Registry::load(dir).ok().flatten()?;
    let live: Vec<_> = reg.fields().iter().filter(|f| f.dead.is_none()).collect();
    if live.is_empty() {
        return None;
    }
    // Группа отдаётся **путём**, а не номером: номер — дело реестра, а
    // объявление говорит о документе. Прежде группа отбрасывалась вовсе, и
    // повторное объявление на старте требовало нуля — из-за чего индекс с
    // массивом переставал подниматься.
    Some(
        live.iter()
            .map(|f| Decl {
                path: f.path.clone(),
                kind: f.kind,
                group: reg.group_path(f.group).unwrap_or_default(),
                boost: f.boost,
                store: f.store,
                positions: f.positions,
                index: f.index,
            })
            .collect(),
    )
}

/// Generates process-unique IDs with a random restart namespace.
fn new_document_id() -> Result<String, String> {
    static PREFIX: OnceLock<Result<String, String>> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let prefix = PREFIX
        .get_or_init(|| {
            let mut bytes = [0u8; 16];
            std::fs::File::open("/dev/urandom")
                .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut bytes))
                .map_err(|error| format!("cannot generate document id: {error}"))?;
            Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
        })
        .as_ref()
        .map_err(Clone::clone)?;
    let next = NEXT
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
        .map_err(|_| "document id sequence exhausted".to_string())?;
    Ok(format!("{prefix}{next:016x}"))
}

/// Переводит ES NDJSON в native envelope для сетевого координатора.
/// Локальный bulk использует bulk.rs без повторного разбора документов.
fn es_batch(body: &[u8], expected_index: Option<&str>) -> Result<Vec<u8>, String> {
    let items = esbulk::parse(body)?;
    let mut docs: Vec<serde_json::Value> = Vec::with_capacity(items.len());
    for it in items.iter() {
        if let (Some(expected), Some(actual)) = (expected_index, it.index.as_deref()) {
            if expected != actual {
                return Err(format!(
                    "документ {:?}: action metadata задаёт индекс {actual:?}, а путь — {expected:?}",
                    it.id.as_deref().unwrap_or("без _id")
                ));
            }
        }
        if let Some(why) = it.refused.as_deref() {
            return Err(format!("документ {:?}: {why}", it.id.as_deref().unwrap_or("без _id")));
        }
        if it.act == esbulk::Act::Delete {
            return Err(
                "гашение в пачке приёма пока не выполняется — шлите его отдельно на /_delete"
                    .to_string(),
            );
        }
        let Some(d) = it.body.clone() else { continue };
        let id = match it.id.as_ref() {
            Some(id) if !id.is_empty() => id.clone(),
            Some(_) => return Err("_id must not be empty".into()),
            None => new_document_id()?,
        };
        docs.push(serde_json::json!({"id": id, "_source": d}));
    }
    let wrapped = serde_json::json!({ "documents": docs });
    serde_json::to_vec(&wrapped).map_err(|e| format!("перевод пачки: {e}"))
}

/// То же для одного документа на `/{индекс}/_doc/{id}`.
fn es_one(body: &[u8], id: &str) -> Result<Vec<u8>, String> {
    if id.is_empty() {
        return Err("в пути нет идентификатора: ожидается /{индекс}/_doc/{id}".to_string());
    }
    let d: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("документ не разобрался: {e}"))?;
    if !d.is_object() {
        return Err("документ — не объект".to_string());
    }
    let wrapped = serde_json::json!({
        "documents": [{"id": id, "_source": d}]
    });
    serde_json::to_vec(&wrapped).map_err(|e| format!("перевод документа: {e}"))
}

/// Хранимое значение в JSON — по объявленному типу поля.
///
/// Число отдаётся числом, логическое — логическим, прочее строкой. Прежде всё
/// уходило строкой, включая цену и метку времени, и клиенту приходилось
/// переводить их обратно — то есть знать типы, которые движок и так знает.
///
/// Печатается **исходная запись**, а не разобранное число: перевод через `f64`
/// потерял бы точность у целых больше двух в пятьдесят третьей степени.
fn json_value(raw: &str, kind: Option<&Kind>) -> String {
    let kind = kind.copied().unwrap_or(Kind::Text);
    if raw.is_empty() {
        return json_string(raw);
    }
    if kind == Kind::Bool && (raw == "true" || raw == "false") {
        return raw.to_string();
    }
    if kind.numeric() && raw.parse::<f64>().is_ok_and(|n| n.is_finite()) {
        return raw.to_string();
    }
    json_string(raw)
}

/// Схема индекса в JSON: живые поля, их типы, признаки и массивы.
fn schema_json(dir: &str) -> Option<String> {
    let decls: Vec<_> = schema_of(dir)?.into_iter().filter(|d| !internal_field(&d.path)).collect();
    let mut groups: Vec<&str> =
        decls.iter().map(|d| d.group.as_str()).filter(|g| !g.is_empty()).collect();
    groups.sort_unstable();
    groups.dedup();
    let fields: Vec<String> = decls
        .iter()
        .map(|d| {
            format!(
                "{{\"path\":{},\"type\":{},\"boost\":{},\"store\":{},\"index\":{},\"positions\":{},\"group\":{}}}",
                json_string(&d.path),
                json_string(&d.kind.spec()),
                d.boost,
                d.store,
                d.index,
                d.positions,
                json_string(&d.group)
            )
        })
        .collect();
    Some(format!(
        "{{\"fields\":[{}],\"groups\":[{}]}}",
        fields.join(","),
        groups.iter().map(|g| json_string(g)).collect::<Vec<_>>().join(",")
    ))
}

/// Объявленная схема, разложенная по параллельным полосам: имена, веса,
/// виды, «хранить», «позиции», «искать». Индекс во всех шести один.
///
/// Имя не `Declared` — так уже зовётся структура ниже, и совпадение сбило бы
/// с толку обеих.
type Spread = (Vec<String>, Vec<f64>, Vec<Kind>, Vec<bool>, Vec<bool>, Vec<bool>);

/// Относится ли имя к внутренней атомарной версии документа.
fn internal_field(path: &str) -> bool {
    matches!(path, nested::SOURCE_FIELD | nested::PROJECTION_FIELD)
}

fn internal_decl(path: &str) -> svindb_core::schema::Decl {
    svindb_core::schema::Decl {
        path: path.to_string(),
        kind: Kind::Text,
        group: String::new(),
        boost: 1.0,
        store: true,
        positions: false,
        index: false,
    }
}

/// Дополняет рабочую схему двумя полями атомарной версии документа.
///
/// Повторный вызов безопасен: так один и тот же код обслуживает создание и
/// restart. Пользовательская схема не вправе переопределить эти имена другим
/// типом или признаками — иначе чтение source зависело бы от её настроек.
fn ensure_internal_fields(decls: &mut Vec<svindb_core::schema::Decl>) -> Result<(), String> {
    for path in [nested::SOURCE_FIELD, nested::PROJECTION_FIELD] {
        match decls.iter().find(|d| d.path == path) {
            Some(d)
                if d.kind == Kind::Text
                    && d.group.is_empty()
                    && d.store
                    && !d.index
                    && !d.positions => {}
            Some(_) => {
                return Err(format!(
                    "поле {path:?} зарезервировано форматом атомарного _source и не может быть переопределено"
                ))
            }
            None => decls.push(internal_decl(path)),
        }
    }
    Ok(())
}

/// Не дублирует пользовательские листья в новом native source-backed индексе.
///
/// Полный JSON и типизированная проекция уже живут атомарно вместе с версией
/// документа. GET, MGET, `fields`, CAS и highlight читают их оттуда. Отдельный
/// stored-файл каждого листа повторял те же байты и на Flat-10M почти удвоил
/// запись и память. Старые индексы не переписываются: функция вызывается
/// только до первого объявления новой схемы.
fn source_back_native_fields(decls: &mut [svindb_core::schema::Decl]) {
    for decl in decls.iter_mut().filter(|decl| !internal_field(&decl.path)) {
        decl.store = false;
    }
}

/// Раскладывает объявление по срезам, которых ждёт приёмник.
fn spread(decls: &[svindb_core::schema::Decl]) -> Spread {
    (
        decls.iter().map(|d| d.path.clone()).collect(),
        decls.iter().map(|d| d.boost).collect(),
        decls.iter().map(|d| d.kind).collect(),
        decls.iter().map(|d| d.store).collect(),
        decls.iter().map(|d| d.index).collect(),
        decls.iter().map(|d| d.positions).collect(),
    )
}

/// Поля по умолчанию — те же, с которыми собраны существующие индексы.
fn default_fields() -> (Vec<String>, Vec<f64>, Vec<Kind>) {
    (vec!["title".to_string(), "body".to_string()], vec![2.0, 1.0], vec![Kind::Text, Kind::Text])
}

/// Разбирает объявление полей: `title:2,body:1,цена:double,дата:date`.
///
/// После двоеточия либо число — это вес текстового поля, — либо имя типа.
/// Одно и то же место занято двумя разными вещами намеренно: у текстового поля
/// вес есть, а у числового его нет и быть не может, и обратное тоже верно.
fn parse_fields(spec: &str) -> Result<Vec<svindb_core::schema::Decl>, String> {
    use svindb_core::schema::Decl;
    let mut out: Vec<Decl> = Vec::new();
    for part in spec.split(',').filter(|p| !p.trim().is_empty()) {
        let (name, tail) = match part.split_once(':') {
            Some((n, t)) => (n.trim(), t.trim()),
            None => (part.trim(), ""),
        };
        // Метки отделяются **с конца**, по одной, пока они узнаются.
        //
        // Прежде отрезался ровно один хвост после второго двоеточия, и
        // разбор держался на удаче: `dense_vector:768` уцелел лишь потому, что
        // `768` не совпало ни с `nostore`, ни с `noindex`. А `text:slots` не
        // уцелел вовсе — тип получался `None`, хвост уходил в разбор веса, тоже
        // неудачно, и поле молча становилось текстовым с весом единица. То есть
        // документированный синтаксис давал рабочий индекс с неверными типами.
        let mut rest = tail;
        let (mut keep, mut index, mut slots) = (true, true, false);
        while let Some((head, label)) = rest.rsplit_once(':') {
            match label.trim() {
                "nostore" => keep = false,
                "noindex" => index = false,
                "slots" => slots = true,
                _ => break,
            }
            rest = head.trim();
        }
        let (boost, kind) = match Kind::parse(rest) {
            Some(k) => (1.0, k),
            None => match rest.parse::<f64>() {
                Ok(b) => (b, Kind::Text),
                Err(_) => {
                    return Err(format!(
                        "поле {name:?}: {rest:?} — это ни тип, ни вес. Типы: text, keyword, \
                         int32, int64, date, date:iso, bool, float, double, binary, \
                         geo_point, dense_vector:N, sparse_vector; метки: nostore, noindex, \
                         slots"
                    ))
                }
            },
        };
        svindb_core::schema::valid_field_path(name)?;
        // `slots` значит «поле лежит в массиве», и массив — это всё до
        // последней точки. Один уровень: массива внутри массива движок не
        // держит, и молча притвориться, что держит, нельзя.
        let group = if slots {
            match name.rsplit_once('.') {
                Some((head, _)) => head.to_string(),
                None => {
                    return Err(format!(
                        "поле {name:?}: метка slots значит «поле лежит в массиве», а в пути \
                         нет точки — назвать массив нечем"
                    ))
                }
            }
        } else {
            String::new()
        };
        let vector = kind.dims().is_some() || kind.dot();
        out.push(Decl {
            path: name.to_string(),
            kind,
            group: group.clone(),
            boost,
            store: keep && !vector,
            positions: true,
            index,
        });
        // Точка раскладывается на две числовые колонки-спутника. Рамка и
        // радиус спрашиваются отбором по ним — тем же, что и всякий диапазон, —
        // а не отдельной машинерией: заводить вторую было бы вторым способом
        // ответить на один вопрос.
        //
        // Спутники объявляются здесь, а не оставляются на загрузчика: забыть их
        // значило бы получить поле, которое хранится и отдаётся, но не
        // ищется, — молча.
        if kind == Kind::Geo {
            for part in ["lat", "lon"] {
                out.push(Decl {
                    path: format!("{name}.{part}"),
                    kind: Kind::Double,
                    group: group.clone(),
                    boost: 1.0,
                    // Содержимое спутника не хранится: оно уже лежит в самой
                    // точке, и хранить его дважды значит платить дважды.
                    store: false,
                    positions: false,
                    index: true,
                });
            }
        }
    }
    if out.is_empty() {
        let (n, b, k) = default_fields();
        return Ok(n
            .into_iter()
            .enumerate()
            .map(|(i, path)| Decl {
                path,
                kind: k[i],
                group: String::new(),
                boost: b[i],
                store: true,
                positions: true,
                index: true,
            })
            .collect());
    }
    Ok(out)
}

/// Разбирает тело `PUT /{index}`. Поддерживается родной компактный `fields` и
/// привычный Elasticsearch `mappings.properties`; неизвестная семантика
/// отвергается, а не изображается похожей.
fn parse_index_creation(
    body: &[u8],
    fallback: &str,
) -> Result<(Vec<svindb_core::schema::Decl>, svindb_core::settings::Settings), String> {
    if body.iter().all(u8::is_ascii_whitespace) {
        let mut fields = parse_fields(fallback)?;
        source_back_native_fields(&mut fields);
        ensure_internal_fields(&mut fields)?;
        return Ok((fields, svindb_core::settings::Settings::default()));
    }
    let text = std::str::from_utf8(body).map_err(|e| format!("тело создания не UTF-8: {e}"))?;
    let native = svindb_core::json::parse(text)?;
    reject_duplicate_keys(&native, "создание")?;
    let value: serde_json::Value = serde_json::from_str(&native.to_compact())
        .map_err(|e| format!("создание не преобразовано в JSON: {e}"))?;
    let source_mode = mapping::SourceMode::from_root(&value)?;
    let object = native.as_obj().ok_or("тело создания индекса должно быть объектом")?;
    for (key, _) in object {
        if !matches!(key.as_str(), "fields" | "mappings" | "settings" | "metadata" | "aliases") {
            return Err(format!(
                "ключ создания {key:?} не поддерживается; ожидаются fields, mappings, settings, metadata, aliases"
            ));
        }
    }
    let mut settings = svindb_core::settings::Settings::default();
    let mut normalizers = std::collections::HashSet::new();
    if native.get("mappings").is_some() {
        normalizers = parse_es_settings(value.get("settings"), &mut settings)?;
    } else if let Some(patch) = native.get("settings") {
        settings.patch(patch)?;
    }
    let native_fields = native.get("mappings").is_none();
    let mut fields = match (native.get("fields"), native.get("mappings")) {
        (Some(_), Some(_)) => {
            return Err("задайте либо fields, либо mappings.properties, но не оба".into())
        }
        (Some(value), None) => {
            let spec = value.as_str().ok_or("fields должен быть строкой")?;
            parse_fields(spec)?
        }
        (None, Some(_)) => {
            // Второму разборщику отдаётся каноническое дерево первого, а не
            // исходная строка. Иначе duplicate-key мог значить «первый» для
            // Settings и «последний» для serde, то есть один запрос создавал
            // бы две разные схемы в зависимости от ветви.
            let mappings = value
                .get("mappings")
                .and_then(serde_json::Value::as_object)
                .ok_or("mappings должен быть объектом")?;
            for key in mappings.keys() {
                if !matches!(key.as_str(), "properties" | "_meta" | "_source" | "dynamic") {
                    return Err(format!(
                        "mappings.{key} пока не поддерживается; SvinDB не будет молча менять семантику runtime/dynamic templates"
                    ));
                }
            }
            if let Some(dynamic) = mappings.get("dynamic") {
                let valid = dynamic.as_bool() == Some(false) || dynamic.as_str() == Some("strict");
                if !valid {
                    return Err(
                        "mappings.dynamic: поддерживаются strict и false; true/runtime требуют \
                         изменения живой схемы и не будут молча подменены source-only хранением"
                            .into(),
                    );
                }
            } else {
                return Err(
                    "mappings.dynamic отсутствует: Elasticsearch по умолчанию добавляет поля на \
                     ходу; задайте strict или false, чтобы контракт был однозначным"
                        .into(),
                );
            }
            let properties = mappings
                .get("properties")
                .and_then(serde_json::Value::as_object)
                .ok_or("mappings.properties должен быть объектом")?;
            let mut out = Vec::new();
            parse_es_properties(properties, "", None, &normalizers, &mut out)?;
            if out.is_empty() {
                return Err("mappings.properties не содержит полей".into());
            }
            out
        }
        (None, None) => parse_fields(fallback)?,
    };
    if native_fields {
        source_back_native_fields(&mut fields);
    }
    if fields.iter().any(|d| internal_field(&d.path)) {
        return Err(format!(
            "имена {:?} и {:?} зарезервированы внутренним форматом документа",
            nested::SOURCE_FIELD,
            nested::PROJECTION_FIELD
        ));
    }
    if source_mode.stores() {
        ensure_internal_fields(&mut fields)?;
    }
    Ok((fields, settings))
}

/// Настройки Elasticsearch, влияющие на компиляцию mapping.
///
/// Настройки шардов на одноузловом runtime не изображаются применёнными:
/// незнакомый ключ — отказ. Refresh переводится в настройку SvinDB, mapping
/// limits проверяются самим форматом, normalizer определяет вид keyword.
fn parse_es_settings(
    raw: Option<&serde_json::Value>,
    settings: &mut svindb_core::settings::Settings,
) -> Result<std::collections::HashSet<String>, String> {
    let mut lowercase = std::collections::HashSet::new();
    let Some(raw) = raw else { return Ok(lowercase) };
    let root = raw.as_object().ok_or("settings должен быть объектом")?;
    for key in root.keys() {
        if !matches!(key.as_str(), "index" | "analysis") {
            return Err(format!("settings.{key} пока не поддерживается"));
        }
    }
    if let Some(index) = root.get("index") {
        let index = index.as_object().ok_or("settings.index должен быть объектом")?;
        for (key, value) in index {
            match key.as_str() {
                "refresh_interval" => {
                    let text = value
                        .as_str()
                        .ok_or("settings.index.refresh_interval должен быть строкой")?;
                    settings.refresh_interval_ms = parse_es_duration_ms(text)?;
                }
                "mapping" => {
                    let mapping =
                        value.as_object().ok_or("settings.index.mapping должен быть объектом")?;
                    for (kind, limit) in mapping {
                        if kind == "source" {
                            let source = limit
                                .as_object()
                                .ok_or("settings.index.mapping.source должен быть объектом")?;
                            if source.keys().any(|key| key != "mode")
                                || source.get("mode").and_then(serde_json::Value::as_str).is_none()
                            {
                                return Err(
                                    "settings.index.mapping.source: ожидается строковый mode"
                                        .into(),
                                );
                            }
                            continue;
                        }
                        if !matches!(kind.as_str(), "total_fields" | "nested_fields") {
                            return Err(format!("settings.index.mapping.{kind} не поддерживается"));
                        }
                        let object = limit.as_object().ok_or_else(|| {
                            format!("settings.index.mapping.{kind} должен быть объектом")
                        })?;
                        if object.keys().any(|key| key != "limit")
                            || object.get("limit").and_then(serde_json::Value::as_u64).is_none()
                        {
                            return Err(format!(
                                "settings.index.mapping.{kind}: ожидается целый limit"
                            ));
                        }
                    }
                }
                other => return Err(format!("settings.index.{other} пока не поддерживается")),
            }
        }
    }
    if let Some(analysis) = root.get("analysis") {
        let analysis = analysis.as_object().ok_or("settings.analysis должен быть объектом")?;
        for key in analysis.keys() {
            if key != "normalizer" {
                return Err(format!("settings.analysis.{key} пока не поддерживается"));
            }
        }
        if let Some(normalizers) = analysis.get("normalizer") {
            let normalizers = normalizers
                .as_object()
                .ok_or("settings.analysis.normalizer должен быть объектом")?;
            for (name, definition) in normalizers {
                let definition = definition
                    .as_object()
                    .ok_or_else(|| format!("normalizer {name:?} должен быть объектом"))?;
                for key in definition.keys() {
                    if !matches!(key.as_str(), "type" | "filter" | "char_filter") {
                        return Err(format!("normalizer {name:?}: ключ {key:?} не поддерживается"));
                    }
                }
                let filters = definition
                    .get("filter")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| format!("normalizer {name:?}: нужен filter"))?;
                if filters.len() != 1 || filters[0].as_str() != Some("lowercase") {
                    return Err(format!(
                        "normalizer {name:?}: поддерживается только filter [\"lowercase\"]"
                    ));
                }
                if definition
                    .get("char_filter")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|filters| !filters.is_empty())
                {
                    return Err(format!("normalizer {name:?}: char_filter не поддержан"));
                }
                lowercase.insert(name.clone());
            }
        }
    }
    Ok(lowercase)
}

fn parse_es_duration_ms(text: &str) -> Result<u64, String> {
    let (number, multiplier) = if let Some(value) = text.strip_suffix("ms") {
        (value, 1u64)
    } else if let Some(value) = text.strip_suffix('s') {
        (value, 1_000)
    } else if let Some(value) = text.strip_suffix('m') {
        (value, 60_000)
    } else {
        return Err(format!("refresh_interval {text:?}: нужны ms, s или m"));
    };
    number
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(multiplier))
        .ok_or_else(|| format!("refresh_interval {text:?} слишком велик"))
}

fn reject_duplicate_keys(value: &svindb_core::json::Json, path: &str) -> Result<(), String> {
    match value {
        svindb_core::json::Json::Obj(fields) => {
            let mut seen = std::collections::BTreeSet::new();
            for (key, child) in fields {
                if !seen.insert(key.as_str()) {
                    return Err(format!("{path}: ключ {key:?} повторён"));
                }
                reject_duplicate_keys(child, &format!("{path}.{key}"))?;
            }
        }
        svindb_core::json::Json::Arr(items) => {
            for (at, child) in items.iter().enumerate() {
                reject_duplicate_keys(child, &format!("{path}[{at}]"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_es_properties(
    properties: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
    group: Option<&str>,
    lowercase_normalizers: &std::collections::HashSet<String>,
    out: &mut Vec<svindb_core::schema::Decl>,
) -> Result<(), String> {
    use svindb_core::schema::Decl;
    for (name, value) in properties {
        let field = if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
        svindb_core::schema::valid_field_path(&field)?;
        let spec = value.as_object().ok_or_else(|| format!("поле {field:?}: ожидается объект"))?;
        let kind_name = spec.get("type").and_then(serde_json::Value::as_str);
        let children = spec.get("properties").and_then(serde_json::Value::as_object);
        let compound = kind_name == Some("object")
            || kind_name == Some("nested")
            || (kind_name.is_none() && children.is_some());
        for key in spec.keys() {
            let supported = match key.as_str() {
                "type" | "properties" => true,
                "index" | "boost" | "store" | "ignore_above" | "normalizer" | "analyzer"
                | "format" | "fields" | "scaling_factor" | "value" => !compound,
                "enabled" => compound,
                "dynamic" => compound,
                "dims" => kind_name == Some("dense_vector"),
                "index_options" | "similarity" | "element_type" => {
                    kind_name == Some("dense_vector")
                }
                _ => false,
            };
            if !supported {
                return Err(format!(
                    "поле {field:?}: параметр {key:?} не поддерживается и не будет проигнорирован"
                ));
            }
        }
        if compound && spec.get("enabled").and_then(serde_json::Value::as_bool) == Some(false) {
            // Поддерево остаётся в lossless `_source`, но колонок для него нет.
            continue;
        }
        if let Some(dynamic) = spec.get("dynamic") {
            let valid = dynamic.as_bool() == Some(false) || dynamic.as_str() == Some("strict");
            if !valid {
                return Err(format!(
                    "поле {field:?}: dynamic поддерживает strict или false; true требует \
                     изменения живой схемы"
                ));
            }
        }
        if kind_name == Some("object") || (kind_name.is_none() && children.is_some()) {
            let children = children.ok_or_else(|| format!("объект {field:?} без properties"))?;
            parse_es_properties(children, &field, group, lowercase_normalizers, out)?;
            continue;
        }
        if kind_name == Some("nested") {
            let children = children.ok_or_else(|| format!("nested {field:?} без properties"))?;
            parse_es_properties(children, &field, Some(&field), lowercase_normalizers, out)?;
            continue;
        }
        if children.is_some() {
            return Err(format!("поле {field:?}: properties несовместим с типом {kind_name:?}"));
        }
        if let Some(store) = spec.get("store") {
            store.as_bool().ok_or_else(|| format!("поле {field:?}: store должен быть bool"))?;
        }
        if let Some(limit) = spec.get("ignore_above") {
            limit
                .as_u64()
                .filter(|limit| *limit > 0)
                .ok_or_else(|| format!("поле {field:?}: ignore_above должен быть положительным"))?;
        }
        if let Some(analyzer) = spec.get("analyzer") {
            if analyzer.as_str() != Some("standard") {
                return Err(format!(
                    "поле {field:?}: analyzer должен быть standard; другой анализатор изменил бы токены"
                ));
            }
        }
        let date_format = spec.get("format").and_then(serde_json::Value::as_str);
        if let Some(format) = date_format {
            if !matches!(
                format,
                "strict_date_optional_time||epoch_millis"
                    | "strict_date_optional_time||epoch_second"
            ) {
                return Err(format!("поле {field:?}: формат даты {format:?} не поддерживается"));
            }
        }
        let normalizer = spec.get("normalizer").and_then(serde_json::Value::as_str);
        if let Some(normalizer) = normalizer {
            if !lowercase_normalizers.contains(normalizer) {
                return Err(format!("поле {field:?}: normalizer {normalizer:?} не объявлен"));
            }
        }
        let kind = match kind_name.unwrap_or("text") {
            "text" => Kind::Text,
            "keyword" | "constant_keyword" => {
                if normalizer.is_some() {
                    Kind::Keyword
                } else {
                    Kind::KeywordExact
                }
            }
            "short" | "integer" => Kind::Int32,
            "long" => Kind::Int64,
            "unsigned_long" => Kind::Int64,
            "float" => Kind::Float,
            "double" | "scaled_float" => Kind::Double,
            "boolean" => Kind::Bool,
            "date" => {
                if date_format.is_some_and(|format| format.ends_with("epoch_second")) {
                    Kind::DateIsoSeconds
                } else {
                    Kind::DateIso
                }
            }
            "binary" => Kind::Binary,
            "geo_point" => Kind::Geo,
            "sparse_vector" => Kind::Sparse,
            "dense_vector" => {
                let dims = spec
                    .get("dims")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| format!("поле {field:?}: dense_vector требует dims"))?;
                Kind::parse(&format!("dense_vector:{dims}"))
                    .ok_or_else(|| format!("поле {field:?}: недопустимая размерность {dims}"))?
            }
            other => {
                return Err(format!(
                    "поле {field:?}: тип Elasticsearch {other:?} не поддерживается"
                ))
            }
        };
        let searched = spec
            .get("index")
            .map(|v| v.as_bool().ok_or_else(|| format!("поле {field:?}: index должен быть bool")))
            .transpose()?
            .unwrap_or(true);
        let boost = spec
            .get("boost")
            .map(|v| v.as_f64().ok_or_else(|| format!("поле {field:?}: boost должен быть числом")))
            .transpose()?
            .unwrap_or(1.0);
        let vector = kind.dims().is_some() || kind.dot();
        // Массив скаляров не меняет группу поля: движок кодирует несколько
        // значений внутри той же физической колонки/постингов. Группа нужна
        // только массиву объектов, где несколько полей обязаны делить slot.
        let own_group = group.unwrap_or("");
        out.push(Decl {
            path: field.clone(),
            kind,
            group: own_group.to_string(),
            boost,
            // `_source` теперь отдельный и lossless. Дублировать каждый leaf
            // широкой схемы больше не нужно: `store` означает именно явное
            // stored field, как у Elasticsearch.
            store: !vector
                && spec.get("store").and_then(serde_json::Value::as_bool).unwrap_or(false),
            positions: kind == Kind::Text,
            index: searched,
        });
        if let Some(subfields) = spec.get("fields") {
            let subfields = subfields
                .as_object()
                .ok_or_else(|| format!("поле {field:?}.fields должно быть объектом"))?;
            parse_es_properties(
                subfields,
                &field,
                Some(own_group).filter(|group| !group.is_empty()),
                lowercase_normalizers,
                out,
            )?;
        }
        if kind == Kind::Geo {
            for coordinate in ["lat", "lon"] {
                out.push(Decl {
                    path: format!("{field}.{coordinate}"),
                    kind: Kind::Double,
                    group: group.unwrap_or("").to_string(),
                    boost: 1.0,
                    store: false,
                    positions: false,
                    index: true,
                });
            }
        }
    }
    Ok(())
}

fn cmd_index(args: &[String]) {
    let plain: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let src = plain.get(2).unwrap_or_else(|| usage());
    let out = plain.get(3).unwrap_or_else(|| usage());
    let root = std::path::Path::new(out.as_str());
    if let Err(error) = migration::ensure_ready(root).and_then(|_| migration::ensure_writable(root))
    {
        eprintln!("{error}");
        std::process::exit(1);
    }
    let per: u32 = plain.get(4).and_then(|x| x.parse().ok()).unwrap_or(1_000_000);
    let posmin: u32 = args
        .iter()
        .find_map(|f| f.strip_prefix("--posmin="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut decls =
        parse_fields(plain.get(5).map(|s| s.as_str()).unwrap_or("")).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        });
    source_back_native_fields(&mut decls);
    ensure_internal_fields(&mut decls).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    let (fields, boosts, kinds, stored, searched, positioned) = spread(&decls);

    let _lock = manifest::WriterLock::take(out).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });
    // Реестр объявляется до первого сегмента: устойчивые номера обязаны быть
    // известны заголовку, который пишется в конце каждого сброса.
    let _ = &positioned;
    let declared = svindb_core::schema::declare_all(out, &decls).unwrap_or_else(|e| {
        eprintln!("схема индекса: {e}");
        std::process::exit(1);
    });
    let t0 = Instant::now();
    let ix = ndjson::index_file(
        src,
        out,
        &fields,
        &boosts,
        &kinds,
        &stored,
        &searched,
        per,
        posmin,
        &declared.fids,
        &declared.groups,
        &declared.group_paths,
    )
    .unwrap_or_else(|e| {
        eprintln!("индексация не удалась: {e}");
        std::process::exit(1);
    });
    println!(
        "проиндексировано {} документов, {} постингов, {} сегментов, за {:.1} с ({:.0} док/с)",
        ix.total_docs,
        ix.total_postings,
        ix.segments(),
        t0.elapsed().as_secs_f64(),
        ix.total_docs as f64 / t0.elapsed().as_secs_f64().max(1e-9)
    );
    if !ix.phases.is_empty() {
        let by: Vec<String> = ix.phases.iter().map(|(n, s)| format!("{n} {s:.1} с")).collect();
        let sum: f64 = ix.phases.iter().map(|(_, s)| s).sum();
        println!("    по фазам: {}", by.join(", "));
        // Сумма фаз больше времени по часам — не ошибка счёта, а то, ради чего
        // всё делалось: запись сегмента идёт в своём потоке, пока разбор
        // продолжается. Сказать это надо прямо, иначе строка читается как
        // противоречие.
        if sum > t0.elapsed().as_secs_f64() * 1.02 {
            println!("    сумма фаз {:.1} с больше часов: запись шла одновременно с разбором", sum);
        }
    }
}

/// Слияние сегментов до тех пор, пока политика находит что сливать.
fn cmd_merge(args: &[String]) {
    let dir = args.get(2).unwrap_or_else(|| usage());
    let policy = merge::Policy {
        at_once: args.get(3).and_then(|x| x.parse().ok()).unwrap_or(8),
        max_docs: args.get(4).and_then(|x| x.parse().ok()).unwrap_or(50_000_000),
    };
    // Замок: приём и слияние оба правят список сегментов.
    let _lock = manifest::WriterLock::take(dir).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });

    let mut round = 0usize;
    loop {
        let names: Vec<String> =
            manifest::segments(dir).iter().map(|p| manifest::name_of(p)).collect();
        if names.len() < 2 {
            break;
        }
        let loaded: Vec<Index> = match names
            .iter()
            .map(|n| index::load(&format!("{dir}/{n}")))
            .collect::<std::io::Result<Vec<_>>>()
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("сегмент не загрузился: {e}");
                std::process::exit(1);
            }
        };
        let sizes: Vec<usize> = loaded.iter().map(|s| s.docs).collect();
        let orders: Vec<Vec<u32>> = loaded.iter().map(merge::field_ids).collect();
        let Some((from, to)) = merge::plan(&sizes, &orders, policy) else { break };

        // Имя слитого не пересекается с приёмом: тот занимает `seg*`.
        let out_name = format!("mrg{:04}", next_merge_number(dir));
        let out = format!("{dir}/{out_name}");
        let docs: usize = sizes[from..to].iter().sum();
        println!(
            "заход {}: сливаю {} сегментов ({}..{}), {} документов -> {out_name}",
            round + 1,
            to - from,
            names[from],
            names[to - 1],
            docs
        );

        let t0 = Instant::now();
        let refs: Vec<&Index> = loaded[from..to].iter().collect();
        // Оценка пика печатается рядом с работой, которую она оценивает.
        //
        // Без этого её не с чем сверить: оценка решает, брать ли слияние вообще,
        // а проверить, не завышена ли она, можно только поставив рядом
        // измеренный пик процесса. Именно так и выяснилось, что на широкой схеме
        // она завышает многократно, и оттого уборка не запускалась.
        println!(
            "  оценка пика: {:.1} МиБ",
            merge::peak_estimate(&refs) as f64 / (1u64 << 20) as f64
        );
        let res = match merge::merge(&refs, &out) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("слияние не удалось: {e}");
                let _ = std::fs::remove_dir_all(&out);
                std::process::exit(1);
            }
        };
        // Сегменты держат отображения исходных файлов — освобождаем до удаления.
        drop(loaded);

        let mut next = names.clone();
        next.splice(from..to, [out_name.clone()]);
        if let Err(e) = manifest::write(dir, &next) {
            eprintln!("список сегментов не обновился: {e}");
            std::process::exit(1);
        }
        println!(
            "  готово за {:.1} с: {} документов, {} слов, {} постингов, {:.1} МБ",
            t0.elapsed().as_secs_f64(),
            res.docs,
            res.terms,
            res.postings,
            // Мебибайты, а не мегабайты: так же считают `du` и `_cat/indices`
            // у Elasticsearch. Смешение единиц однажды уже стоило неверного
            // вывода — 541.7 десятичных сравнили с 534.6 двоичных и решили, что
            // индекс вырос, хотя он был меньше.
            res.bytes as f64 / (1u64 << 20) as f64
        );
        println!(
            "    по фазам: {}",
            res.phases.iter().map(|(n, t)| format!("{n} {t:.1} с")).collect::<Vec<_>>().join(", ")
        );

        std::thread::sleep(Duration::from_secs(2));
        for name in names[from..to].iter() {
            if let Err(e) = std::fs::remove_dir_all(format!("{dir}/{name}")) {
                eprintln!("  не удалось убрать {name}: {e}");
            }
        }
        round += 1;
    }

    let left = manifest::segments(dir).len();
    if round == 0 {
        println!("сливать нечего: {left} сегментов");
    } else {
        println!("заходов {round}, сегментов осталось {left}");
    }
}

/// Следующий свободный номер для слитого сегмента.
fn next_merge_number(dir: &str) -> usize {
    std::fs::read_dir(dir)
        .map(|it| {
            it.filter_map(|e| e.ok())
                .filter_map(|e| {
                    let n = e.file_name().to_string_lossy().to_string();
                    n.strip_prefix("mrg").and_then(|d| d.parse::<usize>().ok()).map(|x| x + 1)
                })
                .max()
                .unwrap_or(0)
        })
        .unwrap_or(0)
}

/// Наибольший номер записи, вошедший в какой-либо сегмент.
///
/// Читается **заголовок**, а не сегмент целиком. Отметка лежит в `head.tsv`, и
/// прежде здесь стоял `index::load` — то есть полное открытие каждого сегмента
/// со всеми отображениями и с `walk_terms`, полным разбором словаря, ради одного
/// восьмибайтного числа, которое и так в первых строках файла.
///
/// Стоило это ровно второго открытия всего индекса на старте: `Segments::load`
/// уже прошёл по тем же сегментам строкой выше. На четырёх тысячах сегментов
/// узел поднимался вдвое дольше без единой причины.
fn segs_seq(dir: &str) -> u64 {
    // Нечитаемый список — не «ноль сегментов».
    //
    // Прежде отказ чтения превращался здесь в пустой состав, а пустой состав
    // означает `published = 0`: подъём считал, что на диске нет ничего, и
    // переигрывал журнал целиком. Тела записей, чьи сегменты преспокойно лежат
    // рядом, ложились бы **второй раз** — и у документов появлялись бы дубли,
    // молча, на ровном месте.
    //
    // Отказать нельзя (мы возвращаем число), но и соврать нельзя. Поэтому
    // нечитаемость останавливает узел: индекс, чей состав не прочёлся, не тот
    // индекс, который можно поднимать.
    let listed = manifest::segments_checked(dir).unwrap_or_else(|e| {
        eprintln!(
            "{dir}: список сегментов не прочёлся: {e}. Поднять узел на таком списке значило \
             бы переиграть журнал поверх уже лежащих сегментов и завести дубли"
        );
        std::process::exit(1);
    });
    listed
        .iter()
        .filter_map(|n| svindb_core::index::read_head(n).ok())
        .map(|h| h.seq)
        .max()
        .unwrap_or(0)
}

/// Показывает состояние журнала, не запуская сервер.
fn cmd_wal(args: &[String]) -> ! {
    let Some(dir) = args.get(2) else { usage() };
    let files = svindb_core::wal::files(dir);
    let mark = svindb_core::wal::checkpoint(dir);
    println!("каталог     {}", svindb_core::wal::dir_of(dir));
    println!("файлов      {}", files.len());
    println!("отметка     {mark}");

    let mut total = 0usize;
    let mut adds = 0usize;
    let mut puts = 0usize;
    let mut dels = 0usize;
    let mut torn = 0u64;
    let (mut lo, mut hi) = (u64::MAX, 0u64);
    for f in files.iter() {
        let Ok(bytes) = svindb_core::wal::read_all(f) else { continue };
        let s = svindb_core::wal::scan(&bytes);
        for r in s.records.iter() {
            total += 1;
            lo = lo.min(r.seq);
            hi = hi.max(r.seq);
            match r.kind {
                svindb_core::wal::ADD | svindb_core::wal::ADD_CELLS => adds += 1,
                svindb_core::wal::PUT | svindb_core::wal::PUT_CELLS => puts += 1,
                svindb_core::wal::DELETE => dels += 1,
                svindb_core::wal::BATCH => {
                    if let Some((kind, bodies)) = svindb_core::wal::decode_batch(&r.body) {
                        match kind {
                            svindb_core::wal::ADD | svindb_core::wal::ADD_CELLS => {
                                adds += bodies.len()
                            }
                            svindb_core::wal::PUT | svindb_core::wal::PUT_CELLS => {
                                puts += bodies.len()
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        torn += s.dropped;
        println!(
            "  {}: записей {}, {} байт{}",
            manifest::name_of(f),
            s.records.len(),
            bytes.len(),
            if s.torn {
                format!(", ОБОРВАН на хвосте в {} байт", s.dropped)
            } else {
                String::new()
            }
        );
    }
    if total == 0 {
        println!("\nжурнал пуст — переигрывать нечего");
        std::process::exit(0);
    }
    println!(
        "\nвсего WAL-записей {total}: приёмов {adds}, замен {puts}, \
         гашений {dels}, номера {lo}–{hi}"
    );
    let r = svindb_core::wal::for_replay(dir, |_| Ok(())).unwrap_or_else(|e| {
        eprintln!("журнал не прочитан: {e}");
        std::process::exit(1);
    });
    println!(
        "к переигрыванию {} операций (приёмов {}, замен {}, гашений {}), \
         пропущено WAL-записей как уже на диске {}",
        r.adds + r.puts + r.deletes,
        r.adds,
        r.puts,
        r.deletes,
        r.skipped
    );
    if torn > 0 {
        println!("непригодно {torn} байт хвоста — это и есть потерянное");
    } else {
        println!("оборванного хвоста нет — потеряно ничего");
    }
    std::process::exit(0)
}

/// Пересобирает фильтры Блума на месте, не трогая индекс.
///
/// ## Как фильтр меняется у упакованного сегмента
///
/// Фильтр — кусок сегмента, и у упакованного он лежит внутри `seg.dat`. Положить
/// пересобранный отдельным файлом рядом нельзя: читатель упакованного сегмента к
/// файлам рядом не обращается **никогда** и продолжил бы брать прежний кусок из
/// контейнера. Настройка оператора тогда молча не действовала бы, а `check` на
/// это не жалуется — имя `idmap.bloom` есть в описи известных.
///
/// Писать поверх `seg.dat` нельзя тем более: узел, держащий его отображённым,
/// получил бы не устаревшие данные, а `SIGBUS` на первой же странице за новым
/// концом файла. Поэтому контейнер пересобирается целиком рядом и подставляется
/// именем — [`svindb_core::container::replace`]. Прочие куски переносятся
/// потоком и со сверкой сумм, так что пересборка фильтра заодно читает весь
/// сегмент и отказывается работать поверх порчи.
///
/// ## Почему опрос идёт первым проходом
///
/// Сегмент, который не открывается, обязан остановить работу **до** первой
/// правки: индекс, где фильтры пересобраны у половины сегментов, — состояние, о
/// котором потом никто не догадается. Проход стоит открытия заголовка и одного
/// отображения на сегмент, и это дёшево против наполовину применённой настройки.
///
/// Больше опрос не обещает, и умолчать об этом нельзя: он видит ровно то, что
/// сегмент открывается. Пересборка контейнера просит вдобавок полного чтения
/// сегмента и второго его объёма свободного места — ни того, ни другого опрос
/// проверить не может, и отказ на середине обхода остаётся возможным. Поэтому
/// такой отказ говорит, к скольким сегментам настройка уже применена: у них
/// новое число бит на документ, у остальных прежнее. Повторный запуск с тем же
/// числом доводит работу до конца — фильтр собирается из карты идентификаторов
/// заново, и сколько раз его пересобирали, на итог не влияет.
///
/// ## Почему нужен замок писателя
///
/// Подмена куска подставляет новый `seg.dat` переименованием, а во временном
/// имени стоит номер процесса — то есть двум одновременным заходам ничто не
/// мешало бы довести до диска каждому свой контейнер и переименовать оба:
/// `seg.dat` остался бы от одного захода, а длина в `head.tsv` — от другого, и
/// сегмент перестал бы открываться. Тем же замком команда разводится с приёмом,
/// слиянием и записывающим узлом, который переоткрывает сегменты у себя.
fn cmd_bloom(args: &[String]) -> ! {
    let Some(dir) = args.get(2) else { usage() };
    let bits: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(bloom::DEFAULT_BITS);
    // Замок берётся до опроса: опрашивать состав, который под нами правит
    // чужой писатель, значит опрашивать не то, что будем менять.
    let _lock = manifest::WriterLock::take(dir).unwrap_or_else(|e| {
        eprintln!("{e}");
        eprintln!(
            "фильтры не пересобраны: подмена куска и чужая запись в один каталог \
                   оставили бы сегмент, у которого заголовок и контейнер от разных заходов"
        );
        std::process::exit(1);
    });
    let names = manifest::segments_checked(dir).unwrap_or_else(|e| {
        eprintln!("{dir}: список сегментов не прочёлся: {e}");
        std::process::exit(1);
    });
    if names.is_empty() {
        eprintln!("в {dir} нет сегментов");
        std::process::exit(1);
    }
    for name in names.iter() {
        if let Err(e) = svindb_core::index::load(name) {
            eprintln!("{name}: {e}");
            eprintln!(
                "фильтры не пересобраны: сегмент не открылся, а начинать работу, \
                       которую нечем закончить, значит применить настройку наполовину"
            );
            std::process::exit(1);
        }
    }
    let t0 = std::time::Instant::now();
    let mut total = 0usize;
    let mut docs = 0usize;
    let mut repacked = 0usize;
    // Номер в обходе — он же число сегментов, уже получивших новое число бит:
    // обход идёт дальше только после удачной укладки. Без этого числа отказ
    // посреди обхода не отличить от отказа до первой правки, а по каталогу
    // разницу не видно.
    for (applied, name) in names.iter().enumerate() {
        let Rebuilt { blob, packed, docs: n } = match rebuilt_filter(name, bits) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("{name}: {e}");
                partly_applied(applied, names.len(), bits);
                std::process::exit(1);
            }
        };
        total += blob.len();
        docs += n;
        let laid = if packed {
            repacked += 1;
            // Контейнер пересобирается целиком: заголовок с новой длиной и
            // сводку `replace` переписывает сам — иначе `check` объявил бы
            // свежий сегмент битым.
            svindb_core::container::replace(name, "idmap.bloom", &blob).map(|_| ())
        } else {
            svindb_core::fs::atomic_write(
                std::path::Path::new(name).join("idmap.bloom").as_path(),
                &blob,
                0o600,
            )
            // Файл сегмента подменён — значит и сводка сумм устарела. Не
            // обновить её здесь значило бы получить ложную поломку на первой же
            // проверке.
            .and_then(|()| svindb_core::sums::write(name))
        };
        if let Err(e) = laid {
            eprintln!("{name}: {e}");
            partly_applied(applied, names.len(), bits);
            std::process::exit(1);
        }
    }
    println!(
        "фильтры пересобраны: {} сегментов ({repacked} с пересборкой контейнера), {bits} бит \
         на документ, {total} байт ({:.3} на документ), {:.1} с",
        names.len(),
        total as f64 / docs.max(1) as f64,
        t0.elapsed().as_secs_f64()
    );
    std::process::exit(0)
}

/// Чем кончился прерванный обход `svindb bloom`: сколько сегментов уже с новым
/// числом бит на документ, а сколько осталось с прежним.
///
/// Строка обязательна именно потому, что по каталогу этого не видно: у фильтра
/// нет признака, по которому можно спросить, каким числом бит он собран. Отказ
/// без такой строки читался бы как «ничего не сделано» — а сделано может быть
/// всё, кроме последнего сегмента.
fn partly_applied(applied: usize, total: usize, bits: usize) {
    if applied == 0 {
        eprintln!(
            "фильтры не пересобраны: ни один из {total} сегментов не тронут, индекс в том \
             же виде, в каком был"
        );
        return;
    }
    // Числа рядом, и оба про сегменты, но означают разное: сколько уже с новым
    // числом бит и сколько ещё с прежним. Написанные подряд без слова
    // «сегментов» у второго, они читались как «у остальных 26 прежнее число» —
    // то есть как будто прежнее число бит тоже двадцать шесть.
    eprintln!(
        "настройка применена к {applied} сегментам из {total}: у этих {applied} теперь {bits} \
         бит на документ, у остальных {} сегментов — прежнее число. Уберите причину отказа и \
         повторите ту же команду: фильтр собирается из карты идентификаторов заново, поэтому \
         повтор доводит работу до конца и не портит уже пересобранное",
        total - applied
    );
}

/// Пересобранный фильтр сегмента и то, что нужно знать о сегменте дальше.
struct Rebuilt {
    blob: Vec<u8>,
    /// Куда его класть: в контейнер пересборкой или отдельным файлом.
    packed: bool,
    /// Документов в сегменте — для строки «столько байт на документ».
    docs: usize,
}

/// Собирает фильтр заново, ничего не записывая.
///
/// Сегмент открывается и закрывается **здесь**, а не вокруг подмены: пересборка
/// контейнера переименовывает `seg.dat` поверх прежнего, и держать в это время
/// его отображение незачем. Отображение старого inode переименование переживёт
/// без вреда, но читать после подмены его никто больше не должен.
fn rebuilt_filter(seg: &str, bits: usize) -> Result<Rebuilt, String> {
    let ix = svindb_core::index::load(seg).map_err(|e| e.to_string())?;
    let store = svindb_core::ids::Ids::open(&ix.src, ix.docs);
    let mut ids: Vec<String> = Vec::with_capacity(ix.docs);
    let mut walk = store.walk();
    while let Some(b) = walk.next() {
        ids.push(String::from_utf8_lossy(b).into_owned());
    }
    Ok(Rebuilt { blob: bloom::build(&ids, bits), packed: ix.src.is_packed(), docs: ix.docs })
}

/// Раскладывает контейнеры сегментов по файлам и упаковывает обратно.
///
/// ## Зачем это оператору
///
/// Оба вида сегмента читаются одинаково, но правится — руками, глазами и
/// отладкой — только рассыпанный: `xxd norms.bin` работает, `xxd seg.dat` нет.
/// Раскладка возвращает сегменту тот вид, каким он был до контейнера, и все
/// прежние средства снова работают; упаковка возвращает область отображения,
/// ради которой контейнер и заведён.
///
/// ## Почему отказ останавливает работу
///
/// Полуразобранный индекс хуже неразобранного: часть сегментов рассыпана, часть
/// упакована, и по каталогу этого не видно. Поэтому первый же отказ называет
/// сегмент и прекращает обход — доделанное осталось целым, недоделанное не
/// начато, и повторный запуск продолжит с того же места (сегмент, уже
/// находящийся в нужном виде, не трогается).
///
/// ## Почему нужен замок писателя
///
/// Перекладка меняет вид сегмента: подставляет `seg.dat` переименованием и
/// переписывает `head.tsv`. Живой узел, приём и слияние по тому же каталогу
/// заняты тем же — переоткрывают сегменты и правят состав, — и попасть им в окно
/// между переименованием и заголовком значит получить сегмент, который не
/// открывается. Договор «двух писателей по каталогу не бывает» держит тот же
/// замок, что у приёма и слияния, а не просьба к оператору так не делать.
fn cmd_repack(args: &[String], into_container: bool) -> ! {
    let Some(dir) = args.get(2) else { usage() };
    let _lock = manifest::WriterLock::take(dir).unwrap_or_else(|e| {
        eprintln!("{e}");
        eprintln!(
            "перекладка не начата: она подставляет seg.dat переименованием, и чужая \
                   запись в тот же каталог оставила бы сегмент, который не открывается"
        );
        std::process::exit(1);
    });
    let names = manifest::segments_checked(dir).unwrap_or_else(|e| {
        eprintln!("{dir}: список сегментов не прочёлся: {e}");
        std::process::exit(1);
    });
    if names.is_empty() {
        eprintln!("в {dir} нет сегментов");
        std::process::exit(1);
    }
    let (did, already) = if into_container {
        ("упакован", "уже упакован")
    } else {
        ("разложен", "уже разложен")
    };
    let t0 = Instant::now();
    let (mut done, mut skipped) = (0usize, 0usize);
    for name in names.iter() {
        let short = manifest::name_of(name);
        // Опись контейнера читается до раскладки и после упаковки — то есть
        // тогда, когда контейнер есть. Спроси её не в тот момент, и отчёт
        // сообщал бы «состав не перечислить» о совершенно здоровом сегменте.
        let before = if into_container { None } else { Some(container_shape(name)) };
        let r = if into_container {
            svindb_core::container::pack(name)
        } else {
            svindb_core::container::unpack(name)
        };
        match r {
            Ok(true) => {
                done += 1;
                let shape = before.unwrap_or_else(|| container_shape(name));
                println!("{short}: {did}, {shape}");
            }
            Ok(false) => {
                skipped += 1;
                println!("{short}: {already}");
            }
            Err(e) => {
                eprintln!("{short}: {e}");
                eprintln!(
                    "работа прекращена на этом сегменте: {done} уже {did}, остальные не тронуты"
                );
                std::process::exit(1);
            }
        }
    }
    println!(
        "{}: {done} сегментов, {skipped} и так были в нужном виде, {:.1} с",
        if into_container { "упаковано" } else { "разложено" },
        t0.elapsed().as_secs_f64()
    );
    std::process::exit(0)
}

/// Сколько кусков и байт в контейнере сегмента — словами для отчёта команды.
///
/// Нечитаемое здесь не отказ: перекладка к этому времени либо ещё не начата,
/// либо уже удалась, и строка отчёта не вправе выглядеть как её провал. Отказ,
/// если он будет, придёт от самой перекладки и со своей причиной.
fn container_shape(seg: &str) -> String {
    let path = format!("{seg}/{}", svindb_core::container::FILE);
    match svindb_core::container::Table::read(&path) {
        Ok(t) => format!(
            "кусков {}, контейнер {} байт",
            t.members().len(),
            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
        ),
        Err(e) => format!("состав не перечислить: {e}"),
    }
}

/// Проверка целостности: только чтение, без починки.
///
/// Отдельной командой, а не частью подъёма, потому что цели разные. Подъём
/// обязан отказаться на первой поломке — открыть повреждённый индекс как
/// исправный хуже, чем не открыть. Проверке нужно обратное: пройти всё и
/// перечислить, потому что человеку важна не первая причина, а все.
///
/// Код возврата — договор для скриптов: 0 всё цело, 1 есть поломки, 2 не
/// разобрали командную строку.
fn cmd_check(args: &[String]) {
    let flags: Vec<&String> = args.iter().filter(|a| a.starts_with("--")).collect();
    let plain: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let dir = plain.get(2).unwrap_or_else(|| usage());
    let depth = if flags.iter().any(|f| f.as_str() == "--full") {
        svindb_core::check::Depth::Full
    } else {
        svindb_core::check::Depth::Fast
    };
    let as_json = flags.iter().any(|f| f.as_str() == "--json");

    let t0 = Instant::now();
    let r = svindb_core::check::check(dir, depth);
    let took = t0.elapsed().as_secs_f64();

    if as_json {
        let items: Vec<serde_json::Value> = r
            .findings
            .iter()
            .map(|f| {
                serde_json::json!({
                    "level": f.level.name(),
                    "scope": f.scope,
                    "check": f.check,
                    "text": f.text,
                    "means": f.means,
                })
            })
            .collect();
        let out = serde_json::json!({
            "dir": r.dir,
            "depth": if depth == svindb_core::check::Depth::Full { "full" } else { "fast" },
            "segments": r.segments,
            "docs": r.docs,
            "broken": r.broken(),
            "notes": r.notes(),
            "ok": r.ok(),
            "took_s": (took * 1000.0).round() / 1000.0,
            "findings": items,
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
    } else {
        println!(
            "каталог    {}\nсегментов  {}\nдокументов {}\nглубина    {}",
            r.dir,
            r.segments,
            r.docs,
            if depth == svindb_core::check::Depth::Full {
                "полная"
            } else {
                "быстрая"
            }
        );
        if r.findings.is_empty() {
            println!("\nвсё цело, за {took:.3} с");
        } else {
            println!();
            for f in r.findings.iter() {
                println!("{:<8} {:<28} {}", f.level.name(), f.scope, f.text);
                println!("{:8} {:<28} → {}", "", f.check, f.means);
            }
            println!("\nполомок {}, заметок {}, за {took:.3} с", r.broken(), r.notes());
        }
    }
    // Заметка кодом возврата не считается: потерянный сегмент это повод убрать
    // место, а не повод не пускать поиск.
    std::process::exit(i32::from(!r.ok()));
}

/// Осмотр хранилища: распределения и оценки вариантов.
///
/// Отдельным флагом, а не всегда, потому что это полный проход по спискам и
/// позициям: на боевом сегменте он идёт минуты, а обычному `inspect` нужны
/// размеры файлов и поля.
fn cmd_inspect_deep(dir: &str, as_json: bool) {
    let t0 = Instant::now();
    let s = svindb_core::inspect::stats(dir).unwrap_or_else(|e| {
        eprintln!("индекс не загрузился: {e}");
        std::process::exit(1);
    });
    let took = t0.elapsed().as_secs_f64();

    if as_json {
        let out = serde_json::json!({
            "dir": s.dir,
            "segments": s.segments,
            "docs": s.docs,
            "terms": s.terms,
            "postings": s.postings,
            "positions": s.positions,
            "bytes": s.bytes(),
            "took_s": (took * 1000.0).round() / 1000.0,
            "files": s.files.iter().map(|(n, b)| serde_json::json!({"file": n, "bytes": b}))
                .collect::<Vec<_>>(),
            "numbers": s.numbers.iter().map(|n| serde_json::json!({
                "name": n.name, "value": n.value, "unit": n.unit })).collect::<Vec<_>>(),
            "histograms": s.hists.iter().map(|h| serde_json::json!({
                "name": h.name,
                "buckets": h.buckets.iter().map(|(l, n)| serde_json::json!({"bucket": l, "count": n}))
                    .collect::<Vec<_>>() })).collect::<Vec<_>>(),
            // Оценки идут вместе с оговоркой и **без** признака «лучше»: место,
            // цена разбора, задержка и память сборки — четыре разные величины.
            "estimates": s.variants.iter().map(|v| serde_json::json!({
                "variant": v.name,
                "bytes_now": v.now,
                "bytes_then": v.then,
                "delta_bytes": v.delta(),
                "delta_per_doc": v.delta() as f64 / s.docs.max(1) as f64,
                "excludes": v.note })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
        return;
    }

    println!(
        "каталог    {}\nсегментов  {}\nдокументов {}\nслов       {}\nпостингов  {}\nвхождений  {}",
        s.dir, s.segments, s.docs, s.terms, s.postings, s.positions
    );
    println!("\nвеличина                                значение");
    for n in s.numbers.iter() {
        println!("{:<38} {:>10.3} {}", n.name, n.value, n.unit);
    }
    for h in s.hists.iter() {
        let total: u64 = h.buckets.iter().map(|(_, n)| n).sum();
        println!("\n{}   (всего {total})", h.name);
        for (label, n) in h.buckets.iter().filter(|(_, n)| *n > 0) {
            let share = *n as f64 * 100.0 / total.max(1) as f64;
            let bar = "#".repeat(((share / 2.0).round() as usize).min(50));
            println!("  {label:>12} {n:>12}  {share:>5.1}%  {bar}");
        }
    }
    println!("\nоценки вариантов — модель, а не замер");
    for v in s.variants.iter() {
        let per_doc = v.delta() as f64 / s.docs.max(1) as f64;
        println!(
            "\n  {}\n    сейчас {} Б, в варианте {} Б, разница {:+} Б ({:+.3} Б/док)\n    {}",
            v.name,
            v.now,
            v.then,
            v.delta(),
            per_doc,
            v.note
        );
    }
    println!("\nосмотр за {took:.2} с");
}

/// Прикидка новой раскладки по классам частоты.
///
/// Отдельно от `--deep`, потому что отвечает на другой вопрос. Тот говорит, из
/// чего состоит место сейчас; этот — во что обошлось бы каждое слово, если бы
/// редкое слово платило по своей частоте, а не по цене длинного списка.
///
/// Ни одного байта при этом не пишется: считается по тому, что уже на диске.
fn cmd_inspect_plan(dir: &str, as_json: bool) {
    let t0 = Instant::now();
    let p = svindb_core::inspect::plan(dir).unwrap_or_else(|e| {
        eprintln!("индекс не загрузился: {e}");
        std::process::exit(1);
    });
    let took = t0.elapsed().as_secs_f64();
    let d = p.docs.max(1) as f64;
    let per = |b: u64| b as f64 / d;

    if as_json {
        let cls = |k: &svindb_core::inspect::Klass| {
            let f = |b: &svindb_core::inspect::Bytes| {
                serde_json::json!({
                    "terms": b.terms, "blocks": b.blocks, "posidx": b.posidx,
                    "postings": b.post, "positions": b.pos, "dict": b.dict,
                    "total": b.total(), "per_doc": per(b.total()),
                })
            };
            serde_json::json!({
                "df": k.name, "terms": k.terms, "postings": k.postings,
                "now": f(&k.now), "then": f(&k.then),
                "delta_per_doc": per(k.then.total()) - per(k.now.total()),
            })
        };
        let out = serde_json::json!({
            "dir": p.dir,
            "docs": p.docs,
            "terms": p.terms,
            "took_s": (took * 1000.0).round() / 1000.0,
            "classes": p.classes.iter().map(cls).collect::<Vec<_>>(),
            "short_list_codings": p.codings.iter()
                .map(|(n, b)| serde_json::json!({"coding": n, "bytes": b, "per_doc": per(*b)}))
                .collect::<Vec<_>>(),
            "posting_block_sizes": p.blocksizes.iter()
                .map(|(n, b)| serde_json::json!({"docs_per_block": n, "bytes": b, "per_doc": per(*b)}))
                .collect::<Vec<_>>(),
            "ceiling_from_level_and_tf": {
                "exact": p.tmax_exact, "lists": p.tmax_total,
                "reader_exceeds_stored": p.tmax_over, "worst_relative_excess": p.tmax_worst },
            "dictionary_without_field_byte": {
                "now": p.dict_now, "then": p.dict_nofield,
                "delta_per_doc": per(p.dict_nofield) - per(p.dict_now) },
            "excludes": "модель по месту. Цена разбора, задержка и память сборки \
                         здесь не считались: короткий список без потолка блока \
                         теряет отсечение внутри себя, а запись переменной длины \
                         читается проходом по опорной группе, а не одним обращением",
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
        return;
    }

    println!("каталог    {}\nдокументов {}\nслов       {}", p.dir, p.docs, p.terms);
    println!("\nпо классам частоты, Б/док");
    println!(
        "{:>6} {:>10} {:>11} {:>9} {:>9} {:>9}",
        "df", "слов", "постингов", "сейчас", "вариант", "разница"
    );
    for k in p.classes.iter() {
        println!(
            "{:>6} {:>10} {:>11} {:>9.2} {:>9.2} {:>+9.2}",
            k.name,
            k.terms,
            k.postings,
            per(k.now.total()),
            per(k.then.total()),
            per(k.then.total()) - per(k.now.total())
        );
    }
    let (now, then) = (p.now(), p.then());
    println!(
        "{:>6} {:>10} {:>11} {:>9.2} {:>9.2} {:>+9.2}",
        "всего",
        p.terms,
        p.classes.iter().map(|k| k.postings).sum::<u64>(),
        per(now.total()),
        per(then.total()),
        per(then.total()) - per(now.total())
    );

    println!("\nпо назначению, Б/док");
    println!("{:>14} {:>9} {:>9} {:>9}", "файл", "сейчас", "вариант", "разница");
    for (name, a, b) in [
        ("метаданные слов", now.terms, then.terms),
        ("метаданные блоков", now.blocks, then.blocks),
        ("указатель позиций", now.posidx, then.posidx),
        ("списки", now.post, then.post),
        ("позиции", now.pos, then.pos),
        ("словарь", now.dict, then.dict),
    ] {
        println!("{name:>14} {:>9.2} {:>9.2} {:>+9.2}", per(a), per(b), per(b) - per(a));
    }

    println!("\nкороткий список (2 ≤ df ≤ блок): чем кодировать номера, Б/док");
    for (name, b) in p.codings.iter() {
        println!("  {name:<24} {:>8.2}", per(*b));
    }

    println!("\nразмер блока постингов, Б/док (только место, отсечение не считалось)");
    for (n, b) in p.blocksizes.iter() {
        println!("  {n:>4} документов          {:>8.2}", per(*b));
    }

    println!(
        "\nпотолок списка из пары «уровень длины, частота»: {} из {} совпали побитово",
        p.tmax_exact, p.tmax_total
    );
    println!(
        "потолок ниже вклада, посчитанного читателем: {} из {} списков, худшее превышение {:.3e}",
        p.tmax_over, p.tmax_total, p.tmax_worst
    );
    println!(
        "словарь без байта поля на записи: {:.2} против {:.2} Б/док ({:+.2})",
        per(p.dict_nofield),
        per(p.dict_now),
        per(p.dict_nofield) - per(p.dict_now)
    );
    println!(
        "\nне учтено: цена разбора, задержка и память сборки. Короткий список без\n\
         потолка блока теряет отсечение внутри себя; запись переменной длины\n\
         читается проходом по опорной группе, а не одним обращением."
    );
    println!("\nприкидка за {took:.2} с");
}

/// Разбор потока позиций: чем его кодировать и что портит один длинный документ.
fn cmd_inspect_positions(dir: &str, as_json: bool) {
    let t0 = Instant::now();
    let p = svindb_core::inspect::positions(dir).unwrap_or_else(|e| {
        eprintln!("индекс не загрузился: {e}");
        std::process::exit(1);
    });
    let took = t0.elapsed().as_secs_f64();
    let d = p.docs.max(1) as f64;
    let per = |b: u64| b as f64 / d;

    if as_json {
        let out = serde_json::json!({
            "dir": p.dir,
            "docs": p.docs,
            "postings": p.postings,
            "position_values": p.values,
            "bytes_now": p.now,
            "bytes_lost_to_foreign_width": p.foreign,
            "took_s": (took * 1000.0).round() / 1000.0,
            "codings": p.codings.iter().map(|c| serde_json::json!({
                "coding": c.name,
                "bytes": c.bytes,
                "per_doc": per(c.bytes),
                "values_scanned_per_lookup": c.scan,
                "excludes": c.note })).collect::<Vec<_>>(),
            "width_inflation": p.inflation.buckets.iter()
                .map(|(l, n)| serde_json::json!({"times": l, "documents": n}))
                .collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
        return;
    }

    println!(
        "каталог    {}\nдокументов {}\nпостингов  {}\nзначений   {}",
        p.dir, p.docs, p.postings, p.values
    );
    println!(
        "\nсейчас {:.3} Б/док, из них {:.3} — разрядность, заданная чужим документом ({:.1}%)",
        per(p.now),
        per(p.foreign),
        p.foreign as f64 * 100.0 / p.now.max(1) as f64
    );
    println!(
        "жадная нарезка по границам документов: {} кусков, {:.1} значения на кусок",
        p.chunks,
        p.values as f64 / p.chunks.max(1) as f64
    );
    println!("\n{:<42}{:>9}{:>10}", "способ", "Б/док", "разбор");
    for c in p.codings.iter() {
        println!("  {:<40}{:>9.3}{:>10.0}", c.name, per(c.bytes), c.scan);
    }
    let total: u64 = p.inflation.buckets.iter().map(|(_, n)| n).sum();
    println!("\n{}   (всего {total})", p.inflation.name);
    for (label, n) in p.inflation.buckets.iter().filter(|(_, n)| *n > 0) {
        let share = *n as f64 * 100.0 / total.max(1) as f64;
        println!(
            "  {label:>8} {n:>12}  {share:>5.1}%  {}",
            "#".repeat(((share / 2.0) as usize).min(50))
        );
    }
    println!(
        "\n«разбор» — сколько значений надо прочесть ради одного нужного. У упаковки\n\
         постоянной ширины это единица; где длина значения зависит от значения — весь кусок.\n\
         Место и цена разбора — разные величины, и складывать их в одно число нельзя."
    );
    println!("\nразбор за {took:.2} с");
}

/// Разбор адресации потока позиций: чем указывать на блок.
fn cmd_inspect_posidx(dir: &str, as_json: bool) {
    let p = svindb_core::inspect::posidx(dir).unwrap_or_else(|e| {
        eprintln!("индекс не загрузился: {e}");
        std::process::exit(1);
    });
    let d = p.docs.max(1) as f64;
    let per = |b: u64| b as f64 / d;
    if as_json {
        let out = serde_json::json!({
            "dir": p.dir, "docs": p.docs,
            "entries_now": p.entries, "entries_if_every_block": p.entries_all,
            "entries_pointing_nowhere": p.empty,
            "bytes_now": p.now, "per_doc_now": per(p.now),
            "ways": p.ways.iter().map(|w| serde_json::json!({
                "way": w.name, "bytes": w.bytes, "per_doc": per(w.bytes),
                "values_scanned_per_lookup": w.scan, "excludes": w.note })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
        return;
    }
    println!("каталог    {}\nдокументов {}", p.dir, p.docs);
    println!(
        "\nзаписей сейчас {}, было бы у каждого блока {} — вынесение редких слов\n\
         уже забрало {:.1}% указателя, и это **не** заслуга адресации",
        p.entries,
        p.entries_all,
        (p.entries_all - p.entries) as f64 * 100.0 / p.entries_all.max(1) as f64
    );
    if p.empty > 0 {
        println!("записей, указывающих в никуда: {}", p.empty);
    }
    println!("\n{:<40}{:>9}{:>10}", "способ", "Б/док", "разбор");
    for w in p.ways.iter() {
        println!("  {:<38}{:>9.3}{:>10.0}", w.name, per(w.bytes), w.scan);
    }
    let total: u64 = p.deltas.buckets.iter().map(|(_, n)| n).sum();
    println!("\n{}   (всего {total})", p.deltas.name);
    for (label, n) in p.deltas.buckets.iter().filter(|(_, n)| *n > 0) {
        let share = *n as f64 * 100.0 / total.max(1) as f64;
        println!(
            "  {label:>12} {n:>10}  {share:>5.1}%  {}",
            "#".repeat(((share / 2.0) as usize).min(50))
        );
    }
}

/// Обменный курс порога: сколько байт снимает и во что обходится проверка.
fn cmd_inspect_verify(dir: &str, as_json: bool) {
    let t0 = Instant::now();
    let v = svindb_core::inspect::verify(dir).unwrap_or_else(|e| {
        eprintln!("разобрать не вышло: {e}");
        std::process::exit(1);
    });
    let took = t0.elapsed().as_secs_f64();
    let d = v.docs.max(1) as f64;
    let per = |b: u64| b as f64 / d;

    if as_json {
        let out = serde_json::json!({
            "dir": v.dir, "docs": v.docs, "field": v.field,
            "terms": v.terms, "position_values": v.values,
            "pos_bytes": v.pos_bytes, "pos_per_doc": per(v.pos_bytes),
            "term_meta_bytes": v.meta_bytes, "pos_idx_entries": v.idx_entries,
            "store_chunks": v.chunks,
            "verify_us_scattered": (v.scatter_us * 1000.0).round() / 1000.0,
            "verify_us_in_a_run": (v.run_us * 1000.0).round() / 1000.0,
            "took_s": (took * 1000.0).round() / 1000.0,
            "cuts": v.cuts.iter().map(|c| serde_json::json!({
                "v": c.v,
                "terms_keeping_positions": c.kept_terms,
                "values_kept": c.kept_values, "values_dropped": c.dropped_values,
                "pos_bytes_kept": c.kept_bytes, "pos_bytes_dropped": c.dropped_pos,
                "pos_per_doc_dropped": per(c.dropped_pos),
                "term_meta_dropped": c.dropped_meta,
                "pos_idx_entries_gone_stale": c.stale_idx,
                "worst_candidates": c.worst,
                "worst_ms_scattered": (c.worst_ms * 1000.0).round() / 1000.0,
                "worst_ms_in_a_run": (c.worst_run_ms * 1000.0).round() / 1000.0,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
        return;
    }

    println!("каталог    {}\nдокументов {}\nполе       {}", v.dir, v.docs, v.field);
    println!(
        "\nпозиции сейчас {:.2} Б/док, {} значений, {} слов, {} кусков содержимого",
        per(v.pos_bytes),
        v.values,
        v.terms,
        v.chunks
    );
    println!(
        "цена одной проверки: {:.2} мкс вразброс, {:.2} мкс подряд — то есть\n\
         соседство кандидатов стоит {:.1}×, и это потолок переупорядочивания",
        v.scatter_us,
        v.run_us,
        v.scatter_us / v.run_us.max(1e-9)
    );

    println!(
        "\n{:>9}{:>10}{:>11}{:>11}{:>12}{:>12}",
        "порог V", "слов", "снимется", "останется", "худшая мс", "она же подряд"
    );
    for c in v.cuts.iter() {
        println!(
            "{:>9}{:>10}{:>10.2}Б{:>10.2}Б{:>12.1}{:>12.1}",
            c.v,
            c.kept_terms,
            per(c.dropped_pos + c.dropped_meta),
            per(c.kept_bytes),
            c.worst_ms,
            c.worst_run_ms
        );
    }
    println!(
        "\n«снимется» — точная величина: у каждого слова свой кусок потока, и убрать\n\
         одно не значит переупаковать другие. «Худшая мс» — граница, а не ожидание:\n\
         столько стоила бы фраза, у которой самое редкое слово стоит ровно на пороге."
    );
    let stale: Vec<&svindb_core::inspect::Cut> =
        v.cuts.iter().filter(|c| c.stale_idx > 0).collect();
    if let Some(c) = stale.last() {
        println!(
            "\nзаписей pos.idx, теряющих смысл при V={}: {} из {} ({:.2} Б/док).\n\
             Сами они не освободятся: указатель проиндексован номером блока.",
            c.v,
            c.stale_idx,
            v.idx_entries,
            per(c.stale_idx * 4)
        );
    }
    let total: u64 = v.dfs.buckets.iter().map(|(_, n)| n).sum();
    println!("\n{}   (всего {total})", v.dfs.name);
    for (label, n) in v.dfs.buckets.iter().filter(|(_, n)| *n > 0) {
        let share = *n as f64 * 100.0 / total.max(1) as f64;
        println!(
            "  {label:>14} {n:>9}  {share:>5.1}%  {}",
            "#".repeat(((share / 2.0) as usize).min(50))
        );
    }
    println!("\nразбор занял {took:.1} с");
}

/// Во что обошлись бы списки пар и что они могли бы заменить.
fn cmd_inspect_pairs(dir: &str, as_json: bool) {
    let t0 = Instant::now();
    let p = svindb_core::inspect::pairs(dir).unwrap_or_else(|e| {
        eprintln!("разобрать не вышло: {e}");
        std::process::exit(1);
    });
    let took = t0.elapsed().as_secs_f64();
    let d = p.docs.max(1) as f64;
    let per = |b: u64| b as f64 / d;

    if as_json {
        let out = serde_json::json!({
            "dir": p.dir, "docs": p.docs, "field": p.field, "tokens": p.tokens,
            "pos_bytes": p.pos_bytes, "pos_per_doc": per(p.pos_bytes),
            "took_s": (took * 1000.0).round() / 1000.0,
            "by_term_frequency": p.levels.iter().map(|l| serde_json::json!({
                "b": l.b, "head_terms": l.head, "pairs": l.pairs, "postings": l.postings,
                "list_bytes": l.bytes, "dict_bytes": l.dict,
                "per_doc": per(l.bytes + l.dict),
                "net_per_doc": per(p.pos_bytes) - per(l.bytes + l.dict),
            })).collect::<Vec<_>>(),
            "by_verification_budget": p.budgets.iter().map(|l| serde_json::json!({
                "candidates": l.b, "pairs": l.pairs, "postings": l.postings,
                "list_bytes": l.bytes, "dict_bytes": l.dict,
                "per_doc": per(l.bytes + l.dict),
                "net_per_doc": per(p.pos_bytes) - per(l.bytes + l.dict),
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
        return;
    }

    println!("каталог    {}\nдокументов {}\nполе       {}", p.dir, p.docs, p.field);
    println!("\nслов в тексте {}, позиции сейчас {:.2} Б/док", p.tokens, per(p.pos_bytes));

    println!("\nотбор по частоте каждого слова");
    println!(
        "{:>9}{:>8}{:>12}{:>13}{:>10}{:>10}",
        "порог", "слов", "различных пар", "постингов", "Б/док", "чистыми"
    );
    for l in p.levels.iter() {
        println!(
            "{:>9}{:>8}{:>12}{:>13}{:>10.2}{:>10.2}",
            l.b,
            l.head,
            l.pairs,
            l.postings,
            per(l.bytes + l.dict),
            per(p.pos_bytes) - per(l.bytes + l.dict)
        );
    }

    println!(
        "\nотбор по бюджету проверки — пара заводится там, где `df(A)·df(B)/N`\n\
         перешло бы бюджет, то есть ровно там, где проверять было бы дорого"
    );
    println!(
        "{:>11}{:>12}{:>13}{:>10}{:>10}",
        "кандидатов", "различных пар", "постингов", "Б/док", "чистыми"
    );
    for l in p.budgets.iter() {
        println!(
            "{:>11}{:>12}{:>13}{:>10.2}{:>10.2}",
            l.b,
            l.pairs,
            l.postings,
            per(l.bytes + l.dict),
            per(p.pos_bytes) - per(l.bytes + l.dict)
        );
    }

    println!(
        "\n«Б/док» — нижняя граница: биномиальная оценка списка плюс шесть байт на\n\
         запись словаря пар. Настоящая упаковка крупнее примерно на десятую часть.\n\
         Не учтено: пара обслуживает только соседство, но не `slop`, и фразе из трёх\n\
         слов даёт якорь, а не ответ."
    );
    println!("\nразбор занял {took:.1} с");
}

/// Посегментный разрез состава: сколько мёртвых и почему сегмент не убирается.
///
/// ## Зачем отдельный вид, когда есть `/_health`
///
/// `/_health` отдаёт **сумму**: сегментов столько-то, мёртвых столько-то. Сумма
/// не отличает двух состояний, которые лечатся по-разному: «двадцать сегментов,
/// в каждом по пять процентов мёртвых» — это долг укрупнения, а «два сегмента,
/// в одном половина мёртвых и он заморожен» — это недостижимая уборка, и
/// укрупнение её никогда не разберёт. Обе картины дают одну и ту же долю
/// мёртвых по индексу.
///
/// Поэтому здесь на каждый сегмент печатается то, чем эти состояния
/// различаются: заморожен ли он, сколько памяти запросила бы его уборка и
/// сколько её отведено. Строка `нужно > отведено` у замороженного грязного
/// сегмента **и есть** диагноз.
fn cmd_inspect_segments(dir: &str, as_json: bool) {
    let segs = Segments::load(dir).unwrap_or_else(|e| {
        eprintln!("индекс не загрузился: {e}");
        std::process::exit(1);
    });
    let cfg = svindb_core::maintain::Config::default();
    let have = svindb_core::maintain::merge_budget();
    let rows: Vec<_> = segs
        .list
        .iter()
        .map(|ix| {
            let seg = svindb_core::merge::Seg::of(ix);
            let dead = ix.dead();
            let one: [&svindb_core::Index; 1] = [ix.as_ref()];
            (
                svindb_core::manifest::name_of(&ix.dir),
                ix.docs,
                dead,
                if ix.docs > 0 { dead as f64 / ix.docs as f64 } else { 0.0 },
                seg.bytes,
                seg.live(),
                seg.frozen(&cfg.tier_of()),
                svindb_core::merge::peak_estimate(&one),
                ix.seq,
            )
        })
        .collect();

    if as_json {
        let out = serde_json::json!({
            "dir": dir,
            "segments": rows.len(),
            "docs": segs.docs(),
            "merge_budget_bytes": have,
            "list": rows.iter().map(|r| serde_json::json!({
                "name": r.0, "docs": r.1, "dead": r.2, "dead_fraction": r.3,
                "bytes": r.4, "live_bytes": r.5, "frozen": r.6,
                "reclaim_needs_bytes": r.7, "reclaim_fits": r.7 <= have, "seq": r.8,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
        return;
    }
    let mib = |b: u64| b as f64 / (1u64 << 20) as f64;
    println!("каталог    {dir}");
    println!("сегментов  {}", rows.len());
    println!("бюджет слияния {:.0} МиБ", mib(have));
    println!(
        "
{:<10} {:>11} {:>11} {:>7} {:>10} {:>10} {:>8} {:>11} {:>7}",
        "сегмент",
        "документов",
        "мёртвых",
        "доля",
        "МиБ",
        "живых МиБ",
        "заморож",
        "уборке МиБ",
        "влезет"
    );
    for r in rows.iter() {
        println!(
            "{:<10} {:>11} {:>11} {:>6.1}% {:>10.0} {:>10.0} {:>8} {:>11.0} {:>7}",
            r.0,
            r.1,
            r.2,
            r.3 * 100.0,
            mib(r.4),
            mib(r.5),
            if r.6 { "да" } else { "нет" },
            mib(r.7),
            if r.7 <= have { "да" } else { "НЕТ" }
        );
    }
    let stuck: Vec<&str> =
        rows.iter().filter(|r| r.6 && r.2 > 0 && r.7 > have).map(|r| r.0.as_str()).collect();
    if !stuck.is_empty() {
        println!(
            "
замороженных грязных, чья уборка не влезает в бюджет: {} — {}",
            stuck.len(),
            stuck.join(", ")
        );
    }
}

fn cmd_inspect(args: &[String]) {
    let flags: Vec<&String> = args.iter().filter(|a| a.starts_with("--")).collect();
    let plain: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let dir = plain.get(2).unwrap_or_else(|| usage());
    let as_json = flags.iter().any(|f| f.as_str() == "--json");
    if flags.iter().any(|f| f.as_str() == "--verify") {
        return cmd_inspect_verify(dir, as_json);
    }
    if flags.iter().any(|f| f.as_str() == "--pairs") {
        return cmd_inspect_pairs(dir, as_json);
    }
    if flags.iter().any(|f| f.as_str() == "--posidx") {
        return cmd_inspect_posidx(dir, as_json);
    }
    if flags.iter().any(|f| f.as_str() == "--positions") {
        return cmd_inspect_positions(dir, as_json);
    }
    if flags.iter().any(|f| f.as_str() == "--plan") {
        return cmd_inspect_plan(dir, as_json);
    }
    if flags.iter().any(|f| f.as_str() == "--segments") {
        return cmd_inspect_segments(dir, as_json);
    }
    if flags.iter().any(|f| f.as_str() == "--deep") {
        return cmd_inspect_deep(dir, as_json);
    }
    let segs = Segments::load(dir).unwrap_or_else(|e| {
        eprintln!("индекс не загрузился: {e}");
        std::process::exit(1);
    });
    println!("каталог    {dir}");
    println!("документов {}", segs.docs());
    println!("сегментов  {}", segs.list.len());
    println!("списков    {}", segs.lists());
    if let Some(first) = segs.list.first() {
        for f in first.fields.iter() {
            if f.kind.numeric() {
                // Признак разброса — это ответ на «отсекает ли пропуск по
                // границам блока». Печатается, потому что по нему решают, нужна
                // ли полю надстройка, и решение обязано быть видимым.
                println!(
                    "поле       {} тип {} размах блока к общему {:.2e}{}",
                    f.name,
                    f.kind.name(),
                    f.spread,
                    if f.spread >= svindb_core::column::SPREAD_USELESS {
                        " (пропуск по блокам не отсекает)"
                    } else {
                        ""
                    }
                );
            } else {
                println!(
                    "поле       {} тип {} вес {} средняя длина {:.4}",
                    f.name,
                    f.kind.name(),
                    f.boost,
                    f.avgdl
                );
            }
        }
    }

    // Раскладка по файлам: без неё разговор про место идёт на ощупь.
    let mut by_name: Vec<(String, u64)> = Vec::new();
    let mut total = 0u64;
    // Пути берутся из списка сегментов: после слияния имена уже не `seg####`,
    // и перебор по номеру показывал бы пустоту.
    for path in Segments::ready(dir) {
        let Ok(entries) = std::fs::read_dir(&path) else { continue };
        for e in entries.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().to_string();
            let size = e.metadata().map(|m| m.len()).unwrap_or(0);
            total += size;
            match by_name.iter_mut().find(|(n, _)| *n == name) {
                Some((_, s)) => *s += size,
                None => by_name.push((name, size)),
            }
        }
    }
    by_name.sort_by_key(|a| std::cmp::Reverse(a.1));
    println!("\nфайл           байт          доля");
    for (name, size) in by_name.iter() {
        let share = if total == 0 { 0.0 } else { *size as f64 * 100.0 / total as f64 };
        println!("{name:<14} {size:>13}  {share:>5.1}%");
    }
    println!("{:<14} {total:>13}  100.0%", "всего");

    // Что процесс держит в своей памяти, а что живёт в страницах ядра.
    let docs: usize = segs.docs();
    let fields = segs.list.first().map_or(0, |s| s.fields.len());
    let blocks: usize = segs.list.iter().map(|s| s.blocks_count()).sum();
    let lists: usize = segs.lists();
    let norms = docs * fields;
    let live = segs.list.iter().map(|s| s.live_bytes()).sum::<usize>();
    // На блок: последний номер `u32`, потолок множителя `f32`, длина `u16`.
    let meta = blocks * svindb_core::index::BLOCK_META;
    // На список: число документов, номер первого блока, смещение его начала.
    let terms = lists * svindb_core::index::TERM_META;
    // Фильтр Блума: не нужен поиску вовсе, нужен замене и удалению. Считается
    // отдельной строкой именно поэтому — на приёме без изменений это чистая
    // плата, и видно, за что платим.
    let bloom: usize = segs.list.iter().map(|s| s.idmap.bloom_bytes()).sum();
    // Колонки значений: поиску не нужны вовсе, нужны отбору, сортировке и
    // агрегациям. Отдельной строкой именно поэтому — видно, за что платим.
    let cols: usize = segs
        .list
        .iter()
        .map(|s| {
            s.fields.iter().filter_map(|f| f.column.as_ref()).map(|c| c.bytes()).sum::<usize>()
        })
        .sum();
    let resident = norms + live + meta + terms + bloom + cols;
    println!("\nв памяти процесса          байт      на документ");
    println!("{:<20} {:>13}  {:>10.3}", "нормы", norms, norms as f64 / docs.max(1) as f64);
    println!(
        "{:<20} {:>13}  {:>10.3}",
        "метаданные блоков",
        meta,
        meta as f64 / docs.max(1) as f64
    );
    println!("{:<20} {:>13}  {:>10.3}", "списки", terms, terms as f64 / docs.max(1) as f64);
    println!("{:<20} {:>13}  {:>10.3}", "живые документы", live, live as f64 / docs.max(1) as f64);
    println!("{:<20} {:>13}  {:>10.3}", "фильтр по ид", bloom, bloom as f64 / docs.max(1) as f64);
    println!("{:<20} {:>13}  {:>10.3}", "колонки значений", cols, cols as f64 / docs.max(1) as f64);
    println!("{:<20} {:>13}  {:>10.3}", "итого", resident, resident as f64 / docs.max(1) as f64);
    println!(
        "{:<20} {:>13}  {:>10.3}   (страницы ядра, не бюджет узла)",
        "отображено",
        total.saturating_sub(resident as u64),
        total.saturating_sub(resident as u64) as f64 / docs.max(1) as f64
    );
}

/// Поднимает снимок в каталог. Только командой — через API этого нет.
///
/// Намеренно: снимок без восстановления бесполезен, но восстанавливать поверх
/// живого индекса по сети — способ потерять данные одним запросом. Тот, кто
/// поднимает слепок, стоит у машины и знает, что делает.
fn cmd_restore(args: &[String]) {
    let (Some(arc), Some(dst)) = (args.get(2), args.get(3)) else {
        eprintln!("svindb restore <архив> <каталог>");
        std::process::exit(2);
    };
    if std::path::Path::new(dst).exists()
        && std::fs::read_dir(dst).map(|d| d.count() > 0).unwrap_or(false)
    {
        eprintln!("{dst}: каталог не пуст — поднимать поверх нельзя");
        std::process::exit(2);
    }
    let t0 = std::time::Instant::now();
    match svindb_core::snapshot::restore(arc, dst) {
        Ok(info) => println!(
            "поднято: документов {}, сегментов {}, {:.1} МиБ за {:.1} с",
            info.docs,
            info.segments.len(),
            info.total_len() as f64 / (1 << 20) as f64,
            t0.elapsed().as_secs_f64()
        ),
        Err(e) => {
            eprintln!("не поднялось: {e}");
            std::process::exit(1);
        }
    }
}

/// Снимает слепок командой — тем же кодом, что и точка API.
fn cmd_snapshot(args: &[String]) {
    let (Some(dir), Some(dest), Some(name)) = (args.get(2), args.get(3), args.get(4)) else {
        eprintln!("svindb snapshot <каталог> <приёмник> <имя> [--fast] [--incremental]");
        std::process::exit(2);
    };
    let effort = if args.iter().any(|a| a == "--fast") {
        svindb_core::codec::pack::Effort::Fast
    } else {
        svindb_core::codec::pack::Effort::Thorough
    };
    let t0 = std::time::Instant::now();
    // Инкрементальный снимок переиспользует уже снятые сегменты: они
    // неизменяемы, и переснимать их нечего. Цена названа — приёмник держит
    // объекты, пока их не уберёт `snapshot-prune`.
    let inc = args.iter().any(|a| a == "--incremental");
    let made = if inc {
        svindb_core::snapshot::create_incremental(dir, dest, name, effort)
    } else {
        svindb_core::snapshot::create(dir, dest, name, effort)
    };
    match made {
        Ok(info) => println!(
            "снято: документов {}, сегментов {}, {:.1} → {:.1} МиБ ({:.1}%) за {:.1} с",
            info.docs,
            info.segments.len(),
            info.total_len() as f64 / (1 << 20) as f64,
            info.total_stored() as f64 / (1 << 20) as f64,
            info.total_stored() as f64 / info.total_len().max(1) as f64 * 100.0,
            t0.elapsed().as_secs_f64()
        ),
        Err(e) => {
            eprintln!("не снялось: {e}");
            std::process::exit(1);
        }
    }
}

/// Убирает старые снимки и осиротевшие объекты.
///
/// Число оставляемых спрашивается явно и не имеет умолчания: «сколько-нибудь»
/// снимков — это не политика хранения, а догадка о ней, и ошибка здесь стоит
/// снимков, которых уже нет.
fn cmd_snapshot_prune(args: &[String]) {
    let (Some(dest), Some(keep)) = (args.get(2), args.get(3).and_then(|s| s.parse::<usize>().ok()))
    else {
        eprintln!("svindb snapshot-prune <приёмник> <сколько оставить>");
        std::process::exit(2);
    };
    match svindb_core::snapshot::prune(dest, keep) {
        Ok((snaps, objects)) => {
            println!("убрано снимков {snaps}, освобождено объектов {objects}");
        }
        Err(e) => {
            eprintln!("уборка не прошла: {e}");
            std::process::exit(1);
        }
    }
}

/// Печатает хеш пароля для файла настроек.
///
/// Пароль читается из потока ввода, а не берётся аргументом: аргументы видно в
/// `ps` всякому, кто вошёл на машину, и они оседают в истории оболочки.
fn cmd_passwd(args: &[String]) {
    let iters: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(210_000);
    if iters < 10_000 {
        eprintln!("повторов слишком мало: {iters}");
        std::process::exit(2);
    }
    eprint!("пароль (виден при вводе — TTY мы не трогаем): ");
    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() || line.trim().is_empty() {
        eprintln!("пусто");
        std::process::exit(2);
    }
    println!("{}", svindb_core::hash::password_hash(line.trim_end_matches(['\n', '\r']), iters));
}

/// Каталог и порт у `serve`.
fn serve_location(
    plain: &[&str],
    configured_port: Option<u16>,
    default_dir: &str,
) -> Result<(String, u16, bool), String> {
    let positionals = plain.get(1..).unwrap_or_default();
    let default_port = configured_port.unwrap_or(19300);
    let port = |raw: &str| {
        raw.parse::<u16>().map_err(|_| format!("неверный порт {raw:?}: нужно число от 0 до 65535"))
    };

    match positionals {
        [] => Ok((default_dir.to_string(), default_port, true)),
        [only] if only.bytes().all(|b| b.is_ascii_digit()) => {
            Ok((default_dir.to_string(), port(only)?, true))
        }
        [dir] => Ok(((*dir).to_string(), default_port, false)),
        [dir, raw_port] => Ok(((*dir).to_string(), port(raw_port)?, false)),
        _ => Err(format!(
            "у serve не больше двух позиционных аргументов: [каталог] [порт]; получено {}",
            positionals.len()
        )),
    }
}

/// Путь по умолчанию не зависит от каталога, из которого запустили процесс:
/// бинарник можно вызывать из cron, launchd или через абсолютный путь, и он всё
/// равно откроет одно и то же хранилище рядом с собой.
fn default_data_dir() -> Result<String, String> {
    let executable =
        std::env::current_exe().map_err(|e| format!("не удалось определить путь к svindb: {e}"))?;
    data_dir_beside(&executable)
}

fn data_dir_beside(executable: &std::path::Path) -> Result<String, String> {
    let parent = executable
        .parent()
        .ok_or_else(|| format!("у исполняемого файла {} нет каталога", executable.display()))?;
    parent
        .join("data")
        .into_os_string()
        .into_string()
        .map_err(|path| format!("путь к каталогу данных не в UTF-8: {}", path.to_string_lossy()))
}

/// Режим узла. Полный цикл — умолчание, ограничения задаются обратными
/// флагами, поэтому обычный запуск пригоден и для чтения, и для записи.
fn serve_mode(flags: &[&str]) -> Result<(bool, bool), String> {
    if flags.iter().any(|f| matches!(*f, "--write" | "--maintain")) {
        return Err(
            "флаги --write и --maintain удалены: запись и фоновое обслуживание теперь включены по умолчанию; используйте --read-only или --no-maintain для ограничения узла"
                .to_string(),
        );
    }
    for flag in flags {
        validate_serve_flag(flag)?;
    }
    let writable = !flags.contains(&"--read-only");
    let maintaining = writable && !flags.contains(&"--no-maintain");
    Ok((writable, maintaining))
}

/// Проверяем все аргументы до чтения конфигурации и открытия хранилища.
/// Опечатка или негодное число не должны молча включать умолчание.
fn validate_serve_flag(flag: &str) -> Result<(), String> {
    if matches!(
        flag,
        "--read-only"
            | "--no-maintain"
            | "--multi-index"
            | "--single-index"
            | "--warm"
            | "--no-wal"
            | "--sync-flush"
            | "--allow-low-map-count"
    ) {
        return Ok(());
    }
    let Some((name, value)) = flag.split_once('=') else {
        return Err(format!("{flag}: неизвестный флаг serve или отсутствует значение ="));
    };
    let invalid = |expected: &str| format!("{flag}: ожидается {expected}");
    match name {
        "--config" | "--bind" | "--tls-cert" | "--tls-key" | "--shards" | "--replicas" => {
            if value.is_empty() {
                return Err(invalid("непустое значение"));
            }
        }
        "--fields" => {}
        "--warm" => {
            if !matches!(value, "eager" | "lazy") {
                return Err(invalid("eager или lazy"));
            }
        }
        "--ack" => {
            if svindb_core::cluster::Ack::parse(value).is_none() {
                return Err(invalid("one, quorum или all"));
            }
        }
        "--dead-fraction" => {
            let fraction = value.parse::<f64>().map_err(|_| invalid("конечная доля от 0 до 1"))?;
            if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
                return Err(invalid("конечная доля от 0 до 1"));
            }
        }
        "--posmin" => {
            value.parse::<u32>().map_err(|_| invalid("целое от 0 до 4294967295"))?;
        }
        "--threads" | "--max-indices" | "--replicate-keep-mb" | "--replicate-mbps" => {
            let number =
                value.parse::<usize>().map_err(|_| invalid("целое неотрицательное число"))?;
            if matches!(name, "--threads" | "--max-indices") && number == 0 {
                return Err(invalid("положительное целое число"));
            }
        }
        "--flush" | "--target" | "--refresh-ms" | "--merge-at-once" | "--grace-ms"
        | "--merge-threads" | "--term" => {
            value.parse::<u64>().map_err(|_| invalid("целое неотрицательное число"))?;
        }
        _ => return Err(format!("{flag}: неизвестный флаг serve")),
    }
    Ok(())
}

/// Слушает ли узел только петлю.
///
/// Признак один на два решения, и это нарочно: по нему же узел говорит, что
/// стоит открытым без прав доступа. Боевой узел — тот, до которого дотягиваются
/// снаружи; узел на петле — это стенд, отладка или запуск на ноутбуке, и
/// требовать от него того же значило бы сделать невозможной работу с индексом
/// там, где предел поднять некому.
///
/// `::1` — та же петля, что и `127.0.0.1`: прежде он сюда не попадал, и узел на
/// шестой версии протокола получал предупреждение про открытый порт, не будучи
/// открытым.
fn loopback(bind: &str) -> bool {
    matches!(bind, "127.0.0.1" | "::1" | "localhost")
}

/// Отказать или предупредить, когда областей отображения не хватит на индекс.
///
/// ## Почему не всегда отказ
///
/// Так поступает Elasticsearch: в боевом режиме он на этом месте не стартует, а
/// на стенде ограничивается записью в журнал. Боевой режим у него — узел,
/// доступный не только с петли, и признак у нас тот же, [`loopback`].
///
/// `--read-only` мягче осознанно: реплика на чтение сегментов не заводит, её
/// потребность в областях не растёт, и потолок ей грозит не в работе, а только
/// на открытии. Неоткрывшаяся реплика хуже работающей у самого потолка.
///
/// `--allow-low-map-count` — вентиль, и без него нельзя: узел в контейнере
/// делит `vm.max_map_count` с хозяином, оператору эта ручка недоступна, и без
/// вентиля такой узел стал бы незапускаемым по причине, которую он не может
/// устранить.
fn map_shortfall_for(bind: &str, read_only: bool, allow_low: bool) -> MapShortfall {
    if allow_low || read_only || loopback(bind) {
        MapShortfall::Warn
    } else {
        MapShortfall::Refuse
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServeRuntime {
    Multi,
    Single,
}

/// Выбирает runtime без эвристики по отдельным файлам индекса.
///
/// Новый каталог по умолчанию становится multi-root. Старый непустой индекс
/// остаётся одноиндексным, чтобы обновление бинарника не меняло его layout и не
/// скрывало данные. Распределённые роли пока используют прежний локальный
/// runtime явно: их topology ещё не имеет измерения имени индекса.
fn select_serve_runtime(layout: multi::LayoutKind, flags: &[&str]) -> Result<ServeRuntime, String> {
    let force_multi = flags.contains(&"--multi-index");
    let force_single = flags.contains(&"--single-index");
    if force_multi && force_single {
        return Err("--multi-index и --single-index противоречат друг другу".to_string());
    }
    let distributed =
        flags.iter().any(|flag| flag.starts_with("--shards=") || flag.starts_with("--replicas="));
    if force_single || distributed {
        if layout == multi::LayoutKind::Multi {
            return Err(
                "multi-index root нельзя открыть одноиндексным, coordinator или primary runtime: его каталоги адресуются по именам индексов"
                    .to_string(),
            );
        }
        if flags.iter().any(|flag| flag.starts_with("--max-indices=")) {
            return Err("--max-indices относится к обычному многoиндексному runtime".to_string());
        }
        return Ok(ServeRuntime::Single);
    }
    if force_multi {
        return Ok(ServeRuntime::Multi);
    }
    Ok(match layout {
        multi::LayoutKind::New | multi::LayoutKind::Multi => ServeRuntime::Multi,
        multi::LayoutKind::Legacy => ServeRuntime::Single,
    })
}

fn cmd_serve(args: &[String]) {
    // Отметка ставится здесь, а не при первом запросе: время работы узла
    // считается от подъёма, и разбор аргументов с открытием сегментов в него
    // входят — на большом индексе это заметная часть.
    host::mark_start();
    install_shutdown_signals();
    let flags: Vec<&str> =
        args.iter().map(String::as_str).filter(|a| a.starts_with("--")).collect();
    let plain: Vec<&str> =
        args.iter().map(String::as_str).filter(|a| !a.starts_with("--")).collect();
    let (writable, maintaining) = serve_mode(&flags).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    // Файл настроек читается первым, аргументы перекрывают его. Так отлаживают
    // узел, не трогая его настройки: `--port=` поверх боевого файла поднимает
    // второй экземпляр, ничего не переписав.
    let cfg = match flags.iter().find_map(|f| f.strip_prefix("--config=")) {
        Some(path) => config::load(path).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        }),
        None => config::Config::default(),
    };
    // Конфигурация workload читается до создания search pool: OnceLock нельзя
    // перенастроить после первого вызова `resources()`, а прежний порядок
    // навсегда оставлял узел на внутренних значениях admission.
    let configured_resources = resources_with(&cfg.workload);
    let source_cache = Arc::new(source_cache::Cache::new(configured_resources.source_cache_bytes));
    svindb_core::executor::install(configured_resources.cpu_max).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let _pressure_sampler = pressure::start().unwrap_or_else(|error| {
        eprintln!("не запущено измерение cgroup pressure: {error}");
        std::process::exit(1);
    });
    let default_dir = default_data_dir().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    let (dir, port, using_default_dir) = serve_location(&plain, cfg.port, &default_dir)
        .unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        });
    migration::ensure_ready(std::path::Path::new(&dir)).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    if writable {
        migration::ensure_writable(std::path::Path::new(&dir)).unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });
    }
    if using_default_dir && writable {
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| {
            eprintln!(
                "каталог данных {dir} не создаётся: {e}\n\
                 укажите доступный путь явно: svindb serve <каталог> {port}"
            );
            std::process::exit(1);
        });
    }
    if using_default_dir && !writable && !std::path::Path::new(&dir).is_dir() {
        eprintln!(
            "каталога данных {dir} нет, а --read-only запрещает создавать его; сначала запустите обычный узел или укажите существующий каталог"
        );
        std::process::exit(1);
    }
    if using_default_dir {
        println!("каталог данных: {dir}");
    }
    // Прогрев метаданных: списки остаются на диске, всё остальное затягивается
    // в кеш страниц. Разбор — у `Index::warm`.
    //
    // `--warm=lazy` — умолчание и без флага: узел поднимается сразу, страницы
    // приходят по мере обращения. `--warm` и `--warm=eager` греют заранее, и
    // теперь это делается **после** подъёма слушателя: прежде порт открывался
    // только по окончании прогрева, то есть узел с большим индексом отвечал
    // отказом соединения ровно столько, сколько длился прогрев, и снаружи это
    // было неотличимо от «не запустился».
    let warm = flags.iter().any(|f| *f == "--warm" || *f == "--warm=eager");
    // Проверяется **каждое** значение, а не «есть ли хоть одно годное».
    // Прежнее условие пропускало опечатку молча, стоило в argv оказаться ещё и
    // верному `--warm=lazy`: узел поднимался, и оператор был уверен, что задал
    // режим, которого не задал.
    if let Some(bad) = flags
        .iter()
        .find(|f| f.starts_with("--warm=") && **f != "--warm=eager" && **f != "--warm=lazy")
    {
        eprintln!("{bad}: --warm принимает только eager и lazy");
        std::process::exit(1);
    }
    // По умолчанию — по числу ядер, а не один.
    //
    // Единица означала, что межсегментного параллелизма нет **вовсе**: доля ядер
    // на запрос считается как `(ядра / занятые).clamp(1, threads)`, и при
    // `threads = 1` она всегда единица. То есть запрос обходил все сегменты
    // последовательно, сколько бы их ни было и сколько бы ядер ни стояло без
    // дела. На сотне сегментов это терпимо, на тысячах — линейно по их числу.
    //
    // Делить ядра между запросами узел уже умеет сам: `Inflight` считает
    // одновременные поиски, и доля падает по мере их появления. Значит потолок
    // здесь не про справедливость, а про то, сколько ядер узлу позволено занять
    // на один запрос, — и занимать все свои ядра для узла нормально.
    let threads: usize = flags
        .iter()
        .find_map(|f| f.strip_prefix("--threads="))
        .and_then(|v| v.parse().ok())
        .or(cfg.threads)
        .unwrap_or_else(cores);
    let no_wal = flags.contains(&"--no-wal");
    // Настройка TLS читается **до** открытия индекса.
    //
    // Отказ на негодном сертификате обязан прийти раньше, чем узел возьмёт
    // замок писателя и переиграет журнал: чинить настройку проще, когда узел не
    // успел ничего тронуть, а стоящий рядом наблюдатель не увидит «поднялся и
    // сразу упал».
    let tls = tls_from(args, &cfg);
    let layout = multi::classify(&dir).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let runtime = select_serve_runtime(layout, &flags).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(2);
    });
    // Один hook обслуживает и обычный, и многoиндексный runtime. Он ставится
    // до запуска фоновых потоков, чтобы первый же merge увидел query-first
    // политику, а не получил окно без governor.
    install_workload_hooks(&cfg.workload);
    if runtime == ServeRuntime::Multi {
        cmd_serve_multi(&flags, &dir, port, threads, writable, maintaining, warm, tls, cfg);
        return;
    }
    // Настройки индекса: файл в его каталоге, поверх — аргументы запуска.
    //
    // Аргумент старше нарочно: им чинят узел, который сам себя настроил во вред,
    // и для этого он не должен требовать правки файла в каталоге, который, может,
    // и не открывается. Записывается аргумент при этом **не**: он на один запуск,
    // писать умеет только `PUT /_settings`.
    //
    // Негодный файл — выход, а не возврат к умолчаниям: узел с чужой нарезкой
    // режет сегменты не так, как весь остальной индекс, и заметят это по памяти
    // через недели.
    let mut settings = svindb_core::settings::load(&dir).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    let flag_u64 = |name: &str| -> Option<u64> {
        flags.iter().find_map(|f| f.strip_prefix(name)).and_then(|v| v.parse().ok())
    };
    if let Some(v) = flag_u64("--flush=") {
        settings.flush_docs = v.min(u32::MAX as u64) as u32;
    }
    if let Some(v) = flag_u64("--target=") {
        settings.target_docs = v as usize;
    }
    if let Some(v) = flag_u64("--refresh-ms=") {
        settings.refresh_interval_ms = v;
    }
    if let Some(v) = flag_u64("--merge-at-once=") {
        settings.merge_at_once = v as usize;
    }
    if let Some(v) = flag_u64("--grace-ms=") {
        settings.grace_ms = v;
    }
    if let Some(v) = flag_u64("--merge-threads=") {
        settings.merge_threads = v as usize;
    }
    if let Some(v) = flags
        .iter()
        .find_map(|f| f.strip_prefix("--dead-fraction="))
        .and_then(|v| v.parse::<f64>().ok())
    {
        settings.dead_fraction = v;
    }
    if let Err(e) = settings.check() {
        eprintln!("настройки: {e}");
        std::process::exit(2);
    }
    let per_segment: u32 = settings.flush_docs;
    let posmin: u32 = flags
        .iter()
        .find_map(|f| f.strip_prefix("--posmin="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let sync_flush = flags.contains(&"--sync-flush");
    let live_settings = Arc::new(RwLock::new(settings));
    // Режим координатора: `--shards=a:1,b:2`. Своих данных у такого узла нет —
    // он рассылает запрос шардам и складывает их верхушки. Разбор — у `coord`.
    // Реплики — через `+`: `--shards=a:1+b:1,c:2` это два шарда, у первого две
    // копии. Не через `|` и не через `;`: в оболочке они без кавычек разрывают
    // команду, и узел молча поднялся бы на одном шарде — то есть отдавал бы
    // пятую часть корпуса, выглядя исправным.
    //
    // Полоса ячеек — через `@`: `--shards=a:1@0-2048,b:2@2048-4096`. Без неё
    // ячейки делятся поровну, и это верное умолчание, пока шарды не делили.
    // После деления полосы **неравны**, и равное деление адресовало бы половину
    // корпуса в шард, где её нет.
    let mut shard_ranges: Option<Vec<(u64, u64)>> = None;
    let shard_addrs: Option<Vec<Vec<String>>> =
        flags.iter().find_map(|f| f.strip_prefix("--shards=")).map(|v| {
            let mut ranges: Vec<Option<(u64, u64)>> = Vec::new();
            let addrs: Vec<Vec<String>> = v
                .split(',')
                .map(|shard| {
                    let (body, band) = match shard.split_once('@') {
                        Some((b, r)) => (b, parse_band(r)),
                        None => (shard, None),
                    };
                    ranges.push(band);
                    body.split('+')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<String>>()
                })
                .filter(|reps: &Vec<String>| !reps.is_empty())
                .collect();
            // Полосы либо у всех, либо ни у кого: половина заданных — это карта,
            // про которую нельзя сказать, покрытие она или нет.
            if ranges.iter().any(Option::is_some) {
                if ranges.iter().any(Option::is_none) {
                    eprintln!(
                        "--shards=: полоса `@lo-hi` задана не у всех шардов. \
                         Либо у всех, либо ни у кого — иначе неизвестно, покрыты ли ячейки"
                    );
                    std::process::exit(2);
                }
                shard_ranges = Some(ranges.into_iter().flatten().collect());
            }
            addrs
        });
    if let Some(r) = shard_ranges.as_ref() {
        if let Err(e) = coord::check_ranges(r) {
            eprintln!("--shards=: полосы не покрывают ячейки ровно один раз: {e}");
            std::process::exit(2);
        }
        println!(
            "полосы шардов заданы явно: {}",
            r.iter().map(|(a, b)| format!("{a}-{b}")).collect::<Vec<_>>().join(", ")
        );
    }
    // Держатели **этого** шарда: `--replicas=б:2,в:3`. Не путать с `--shards=`:
    // тот делит корпус между разными наборами документов, этот держит копии
    // одного и того же. Узел с `--replicas` — primary своего шарда.
    let replica_addrs: Vec<String> = flags
        .iter()
        .find_map(|f| f.strip_prefix("--replicas="))
        .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
        .unwrap_or_default();
    // Терм — не украшение и не версия: это то, чем реплика отличает нынешнего
    // primary от смещённого. Поэтому у `--replicas` он обязателен, а умолчания
    // у него нет: умолчание «1» означало бы, что два разных primary неотличимы.
    let term: Option<u64> =
        flags.iter().find_map(|f| f.strip_prefix("--term=")).and_then(|v| v.parse().ok());
    let ack = flags
        .iter()
        .find_map(|f| f.strip_prefix("--ack="))
        .map(|v| match svindb_core::cluster::Ack::parse(v) {
            Some(a) => a,
            None => {
                eprintln!("--ack={v}: допустимы one, quorum, all");
                std::process::exit(2);
            }
        })
        .unwrap_or(svindb_core::cluster::Ack::Quorum);
    // Сколько разосланного держать для догона вернувшейся копии и с какой
    // полосой её догонять. Умолчания: 64 МиБ кольца и 32 МиБ/с.
    //
    // Кольцо не бесконечно намеренно: копия, отсутствовавшая час, отстала
    // настолько, что пересылка записей дороже переноса сегментов, и делать её
    // значило бы выбрать заведомо худший способ. Выпавшее из кольца называется
    // отказом, а не подменяется молчанием.
    let keep_mb: usize = flags
        .iter()
        .find_map(|f| f.strip_prefix("--replicate-keep-mb="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let catchup_mbps: usize = flags
        .iter()
        .find_map(|f| f.strip_prefix("--replicate-mbps="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(32);
    if !replica_addrs.is_empty() && term.is_none() {
        eprintln!(
            "--replicas= задан без --term=. Терм — то, чем реплика отличает нынешнего \
             primary от смещённого; без него защиты от split brain нет"
        );
        std::process::exit(2);
    }
    if let Some(a) = shard_addrs.as_ref() {
        if a.is_empty() {
            eprintln!("--shards= задан, но список пуст");
            std::process::exit(2);
        }
        if let Err(error) = svindb_core::shards::checked_count(a.len()) {
            eprintln!("--shards=: {error}");
            std::process::exit(2);
        }
        let listed: Vec<String> = a.iter().map(|r| r.join("+")).collect();
        println!(
            "режим координатора: шардов {}, реплик {} ({})",
            a.len(),
            a.iter().map(Vec::len).sum::<usize>(),
            listed.join(", ")
        );
    }

    // Адрес слушателя разбирается здесь, а слушатель заводится ниже: от адреса
    // зависит, отказом или предупреждением отвечать на нехватку областей
    // отображения, а решать это надо до открытия индекса.
    //
    // Умолчание — только петля, и это не осторожность, а безопасность: у поиска
    // нет ни проверки прав, ни ограничения по адресу, и узел, поднятый на всех
    // адресах по умолчанию, отдал бы весь индекс всякому, кто дотянется до порта.
    let bind = args
        .iter()
        .find_map(|f| f.strip_prefix("--bind="))
        .map(|s| s.to_string())
        .or_else(|| cfg.bind.clone())
        .unwrap_or_else(|| "127.0.0.1".to_string());

    // Потолок отображений памяти проверяется **до** открытия индекса — то есть
    // до замка писателя и до переигрывания журнала, по тому же доводу, что и
    // негодный сертификат TLS: чинить настройку проще, когда узел ещё ничего не
    // тронул, а стоящий рядом наблюдатель не увидит «поднялся и сразу упал».
    //
    // Сама проверка живёт в `Segments::load`: только там известно и число
    // сегментов, и сколько областей занимает один. Здесь объявляется лишь то,
    // как на нехватку отвечать.
    svindb_core::mmap::set_map_shortfall(map_shortfall_for(
        &bind,
        !writable,
        flags.contains(&"--allow-low-map-count"),
    ));
    if svindb_core::mmap::max_maps().is_none() {
        // Сказать вслух и один раз. Молчание тут хуже: оператор решит, что
        // проверка прошла, а она не проводилась вовсе — предел есть и у macOS,
        // но наружу он не выставлен, и выдумывать за ядро число нельзя.
        println!(
            "предел числа областей отображения ядро не сообщает — проверка потолка не \
             проводилась; о пределе узел узнает отказом mmap на открытии сегмента"
        );
    }
    let segs = Arc::new(Segments::load(&dir).unwrap_or_else(|e| {
        eprintln!("индекс не загрузился: {e}");
        std::process::exit(1);
    }));
    println!(
        "движок: {} документов, {} сегментов, {} полей, {} списков, порт {}",
        segs.docs(),
        reported_segment_count(&segs),
        segs.list.first().map_or(0, |s| s.fields.len()),
        segs.lists(),
        port
    );
    if threads > 1 {
        println!("потоков на запрос: {threads}");
    }
    let live: Live = Arc::new(RwLock::new(segs));
    let background_stop = Arc::new(AtomicBool::new(false));
    let maintenance_lane = Arc::new(Mutex::new(()));
    // Общий на весь узел: состав перезагружают и писатель, и пересмотр каталога,
    // а `/_health` обязан показывать их отказы одинаково.
    let reloads = Arc::new(Reloads::new());
    let mut background_threads = vec![spawn_rescan(
        Arc::clone(&live),
        dir.clone(),
        Arc::clone(&reloads),
        Arc::clone(&background_stop),
    )
    .unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    })];

    let fields_flag: Option<String> =
        flags.iter().find_map(|f| f.strip_prefix("--fields=")).map(str::to_string);
    // Счётчики заводятся до писателя: их держат оба — узел для выдачи, писатель
    // для сбросов и укрупнений, которые случаются вне обработчика запроса.
    let node_metrics = Arc::new(metrics::Metrics::new());
    // Замок принадлежит runtime индекса и освобождается только после остановки
    // его фоновых потоков. Прежний `mem::forget` годился лишь пока процесс
    // держал ровно один индекс до самого exit; при горячем DELETE/повторном
    // CREATE он навсегда оставлял бы занятый дескриптор и старый flock.
    let writer_lock = if writable {
        Some(IndexLease::Write {
            _lock: manifest::WriterLock::take(&dir).unwrap_or_else(|e| {
                eprintln!("{e}");
                std::process::exit(1);
            }),
        })
    } else {
        None
    };
    let writer = if writable {
        let mapping_rules = mapping::Rules::load(&dir).unwrap_or_else(|error| {
            eprintln!("mapping индекса: {error}");
            std::process::exit(1);
        });
        // Замок взят — значит других писателей нет, и каталог вне списка может
        // быть только остатком прерванного слияния.
        let swept = manifest::sweep(&dir);
        if !swept.is_empty() {
            let bytes: u64 = swept.iter().map(|(_, b)| b).sum();
            println!(
                "убрано брошенных сегментов {}: {:.2} ГиБ",
                swept.len(),
                bytes as f64 / (1u64 << 30) as f64
            );
        }
        // Схема берётся у **индекса**, а не из умолчаний.
        //
        // Прежде здесь стояли зашитые «title, body», и приём по сети работал
        // ровно на индексах с такой схемой. Всякий другой — собранный через
        // `svindb index` со своими полями — принимал документы, отвечал
        // `{"added":N}` и клал пустоту: буфер приёма спрашивал у документа
        // «title» и «body», которых в нём нет. Заодно `declare_flat` дописывал
        // эти два имени в чужой реестр, и в схеме появлялись поля-призраки.
        //
        // Порядок такой: живые поля реестра, если он есть; иначе `--fields=`;
        // иначе умолчания. Последнее — только для пустого каталога, где искать
        // всё равно не по чему.
        let mut decls = match schema_of(&dir) {
            Some(x) => x,
            None => {
                let mut created = parse_fields(fields_flag.as_deref().unwrap_or(""))
                    .unwrap_or_else(|e| {
                        eprintln!("{e}");
                        std::process::exit(2);
                    });
                source_back_native_fields(&mut created);
                created
            }
        };
        if mapping_rules.source_mode().stores() {
            ensure_internal_fields(&mut decls).unwrap_or_else(|e| {
                eprintln!("схема индекса: {e}");
                std::process::exit(2);
            });
        }
        let (fields, boosts, kinds, stored, searched, _positioned) = spread(&decls);
        // Реестр общий с приёмом через `index`: поднятый на готовом индексе
        // узел получает те же номера, что уже стоят в его сегментах.
        let declared = svindb_core::schema::declare_all(&dir, &decls).unwrap_or_else(|e| {
            eprintln!("схема индекса: {e}");
            std::process::exit(1);
        });
        let fids = declared.fids;
        // `typed_stored`, а не `typed`: признак хранения приходит из схемы, и
        // терять его значило бы хранить содержимое поля, объявленного
        // `nostore`. Проверено — `--fields=message:text:nostore` принимался и
        // молча игнорировался: `message.sbin` занимал те же 44% сегмента.
        let mut ingest = Indexer::typed_stored(&dir, fields, boosts, kinds, stored, per_segment);
        ingest.set_searched(searched);
        ingest.set_fids(fids);
        // Группы объявляются вместе с полями, а не зашиваются нолём: без этого
        // массив объектов был бы невыразим ничем.
        if declared.group_paths.len() > 1 {
            ingest.set_groups(declared.groups, declared.group_paths);
        }
        ingest.set_posmin(posmin);

        // Восстановление идёт **до** первого ответа клиенту и под замком
        // писателя: переигрывать журнал, пока в него кто-то пишет, значит
        // получить два разных мнения о том, что в индексе.
        // Докуда состав уже долговечен. Заголовок каждого сегмента несёт номер
        // записи, которой он кончился, и максимум по ним — это ровно та граница,
        // ниже которой тело записи заведомо лежит на диске. Читается **до**
        // переигрывания: сброс посреди него добавит сегментов, и взятый после
        // максимум объявил бы долговечным то, что этим же переигрыванием и
        // положено.
        let published = segs_seq(&dir);
        let report = svindb_core::wal::for_replay_banded(&dir, published, |record, band| {
            replay(&record, band, &mut ingest, &live, &dir)
        })
        .unwrap_or_else(|e| {
            eprintln!("журнал не удалось восстановить: {e}");
            std::process::exit(1);
        });
        ingest.enable_frozen_memtables(resources().max_frozen_memtables).unwrap_or_else(|error| {
            eprintln!("frozen lifecycle не включился: {error}");
            std::process::exit(1);
        });
        // Нумерация продолжается с **отметки**, а не с того, что осталось в
        // журнале. Разница решающая: после обрезки журнал пуст, и счёт с нуля
        // выдал бы новым записям номера, которые отметка уже объявила лежащими
        // на диске, — при следующем падении они были бы пропущены как
        // переигранные. Убийство под нагрузкой поймало ровно это: из ста
        // пятидесяти обещанных документов восстанавливалось шестьдесят три.
        //
        // Номера сегментов берутся тем же максимумом: отметка их покрывает, но
        // выводить целость данных из чужой аккуратности не стоит.
        ingest.seq = svindb_core::wal::next_seq(&dir, report.to, segs_seq(&dir));
        // Приёмник создаётся с нулевым `flushed_seq`, но готовый индекс уже
        // долговечен до границы опубликованных сегментов. Без этого после чистого
        // restart `/_flush` считает, будто весь индекс ещё не ложился на диск.
        ingest.flushed_seq = published;
        // Отсюда же стартует очередь применения: всё до этого номера уже
        // применено переигрыванием, значит первая новая запись ждать не должна.
        let applied_from = ingest.seq;
        if report.adds + report.deletes + report.puts > 0 || report.dropped > 0 {
            println!(
                "журнал: переиграно приёмов {}, замен {}, гашений {}, номера {}–{}, \
                 отброшено оборванного хвоста {} байт",
                report.adds, report.puts, report.deletes, report.from, report.to, report.dropped
            );
        }

        let wal = if no_wal {
            println!("журнал выключен: «принято» не переживёт падения");
            None
        } else {
            match Wal::open(
                &dir,
                ingest.seq,
                settings.wal_group_commit_ms,
                Arc::clone(&node_metrics),
            ) {
                Ok(w) => Some(w),
                Err(e) => {
                    eprintln!(
                        "журнал не открылся: {e}\n\
                         приём с обещаниями невозможен — почините каталог {}/{} \
                         или запустите с --no-wal, если долговечность не нужна",
                        dir,
                        svindb_core::wal::DIR
                    );
                    std::process::exit(1);
                }
            }
        };

        // Замок состава заводится **до** писателя: его же получает индексатор,
        // чтобы вносить свежий сегмент в список под тем самым замком, под
        // которым состав переписывает укрупнение.
        let layout = Arc::new(Mutex::new(()));
        if sync_flush {
            ingest.share_layout(Arc::clone(&layout));
        } else {
            ingest.background(Arc::clone(&layout));
        }
        let initial_landed = ingest.segments();
        // План и форма схемы снимаются **здесь**: поля объявлены, группы
        // выставлены, переигрывание прошло, а порт ещё закрыт. Позже схема не
        // меняется, поэтому копия остаётся верной на всю жизнь узла.
        let plan = std::sync::Arc::new(nested::Plan::build_with(
            ingest.field_names(),
            ingest.field_kinds(),
            ingest.field_groups(),
            ingest.group_paths(),
            &mapping_rules,
        ));
        let shape = (ingest.field_names().len(), ingest.group_paths().len() > 1);
        let lossless = ingest.field_names().iter().any(|field| field == nested::SOURCE_FIELD);
        let field_kinds = ingest.field_kinds().to_vec();
        let field_searched = ingest.searched_flags().to_vec();
        Some(Arc::new(Writer {
            plan,
            shape,
            lossless,
            field_kinds,
            field_searched,
            poisoned: AtomicBool::new(false),
            wal,
            recovered: report,
            dir: dir.clone(),
            live: Arc::clone(&live),
            buffered_memory_bytes: AtomicU64::new(ingest.buffered_bytes() as u64),
            mutation_gate: RwLock::new(()),
            ingest: Mutex::new(ingest),
            checkpoint_due: AtomicBool::new(false),
            scratch: Mutex::new(Vec::new()),
            layout,
            checkpoint_gate: Mutex::new(svindb_core::wal::checkpoint(&dir)),
            // Очередь начинается там, где кончилось переигрывание: первая новая
            // запись получит следующий номер и не станет ждать несуществующего
            // предшественника.
            applied: Applied::new(applied_from),
            docs: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
            reloads: Arc::clone(&reloads),
            metrics: Arc::clone(&node_metrics),
            initial_landed,
            keys: std::array::from_fn(|_| Mutex::new(())),
            visibility: std::sync::RwLock::new(()),
            visibility_generation: AtomicU64::new(0),
            live_max_docs: AtomicU32::new(live_settings.read().expect("настройки").live_max_docs),
            live_built_docs: AtomicU32::new(0),
            published: AtomicU64::new(0),
            pending_writes: AtomicUsize::new(0),
            write_queue_highwater: AtomicUsize::new(0),
            compaction_floor: AtomicUsize::new(usize::MAX),
            compaction_pending: AtomicUsize::new(0),
            compaction_pending_bytes: AtomicU64::new(0),
            compaction_completed_sources: AtomicU64::new(0),
            compaction_completed_input_bytes: AtomicU64::new(0),
            compaction_elapsed_micros: AtomicU64::new(0),
            l0_throttled: AtomicU64::new(0),
            l0_rejected: AtomicU64::new(0),
            visible_seq: AtomicU64::new(published),
            ready_seq: AtomicU64::new(published),
            cache_advised_seq: AtomicU64::new(published),
            drop_flushed_cache: AtomicBool::new(settings.drop_flushed_page_cache),
            settled_seq: AtomicU64::new(0),
            compaction_schedulable: AtomicUsize::new(0),
            compaction_running: AtomicUsize::new(0),
            compaction_blocked: Mutex::new(Default::default()),
            flush_state: Mutex::new(FlushState::default()),
            flush_done: Condvar::new(),
        }))
    } else {
        None
    };
    if let Some(w) = writer.as_ref() {
        let s = *live_settings.read().expect("настройки");
        // Нулевое значение больше не означает огромный автоматический буфер:
        // это bounded WAL/recovery cap, чтобы freshness не создавала tiny
        // segments и бесконечный merge debt.
        if let Ok(mut ix) = w.ingest.lock() {
            ix.set_flush_bytes(effective_flush_bytes(&s, resources().wal_replay_bytes));
        }
        // Тот же предел, по которому состав решает про текущий буфер, отдаётся
        // и приёмнику: иначе он собирал бы живой двойник отданного буфера даже
        // тогда, когда состав уже отказался показывать буфер поиску.
        if let Ok(mut ix) = w.ingest.lock() {
            ix.set_live_max_docs(s.live_max_docs);
        }
        // Хвост, восстановленный из WAL, обязан стать поисково видимым, а весь
        // переигранный префикс — отмеченным до открытия порта.
        //
        // Переигрывание кладёт последнюю неполную пачку в mutable-буфер. Если
        // открыть порт сразу, первый запрос успевает обогнать 100-миллисекундный
        // поток свежести и не видит уже подтверждённые до падения документы.
        // В debug-сборке это окно скрывала сама медленная проверка, а release
        // стабильно терял из первого ответа ровно хвост после опубликованного
        // сегмента. Это не потеря на диске, но нарушение ready-контракта после
        // restart: живой узел не вправе отдавать заведомо неполный состав.
        //
        // Доводим хвост тем же производственным путём, что обслуживает
        // `/_flush`: он ждёт фонового писателя, публикует сегмент, синхронизирует
        // карты живости и только затем двигает checkpoint. Размер хвоста не
        // важен и отдельного порога здесь нет — readiness наступает после всей
        // обязательной recovery-работы, а не через таймер удачного размера.
        let recovered_upto = if w.wal.is_some() { segs_seq(&dir).max(w.recovered.to) } else { 0 };
        let recovered_tail = w.ingest.lock().map(|ix| ix.active_buffered() > 0).unwrap_or(false);
        if recovered_tail {
            if let Err(error) = w.drive_one_flush() {
                eprintln!(
                    "восстановленный хвост WAL не удалось опубликовать до открытия порта: {error}"
                );
                std::process::exit(1);
            }
        } else if svindb_core::wal::checkpoint(&dir) < recovered_upto {
            // Сегмент успел войти в manifest, а узел умер до checkpoint. Тела
            // записей при replay уже не клались в буфер, поэтому
            // `recovered_tail == false`. Прежний код ничего не делал и открывал
            // порт с `checkpoint=0`; тихий узел переигрывал бы тот же WAL при каждом
            // следующем restart. Здесь тела уже долговечны, а гашения доводит
            // `sync_live`, так что границу можно двинуть сейчас.
            w.checkpoint_after_sync(recovered_upto);
        }
        let recovered_checkpoint = svindb_core::wal::checkpoint(&dir);
        if recovered_checkpoint < recovered_upto {
            eprintln!(
                "восстановление WAL не завершено: checkpoint {recovered_checkpoint} \
                 отстаёт от долговечной границы {recovered_upto}"
            );
            std::process::exit(1);
        }
        let mb = w.ingest.lock().map(|ix| ix.flush_bytes() >> 20).unwrap_or(0);
        w.observe_compaction_debt(s.maintain());
        println!("приём включён, сброс по {} документов или {mb} МБ буфера", s.flush_docs);
        // Сброс по времени — **свой** поток, не поток обслуживания. Тот гасится
        // `--no-maintain`, и свежесть вместе с ним исчезала бы молча.
        if s.refresh_interval_ms > 0 {
            println!("свежесть: сброс по времени через {} мс", s.refresh_interval_ms);
        }
        background_threads.push(
            spawn_refresh(Arc::clone(w), Arc::clone(&live_settings), Arc::clone(&background_stop))
                .unwrap_or_else(|error| {
                    eprintln!("{error}");
                    std::process::exit(1);
                }),
        );
        if maintaining {
            println!("обслуживание включено, целевой сегмент {}", s.target_docs);
            background_threads.push(
                spawn_maintenance(
                    Arc::clone(w),
                    Arc::clone(&live_settings),
                    Arc::clone(&background_stop),
                    Arc::clone(&maintenance_lane),
                )
                .unwrap_or_else(|error| {
                    eprintln!("{error}");
                    std::process::exit(1);
                }),
            );
        } else {
            println!("фоновое обслуживание выключено (--no-maintain)");
        }
    } else {
        println!("режим только чтение (--read-only)");
    }

    // Адрес разобран выше, до открытия индекса: по нему решается и правило
    // отказа на нехватку областей отображения.
    let listener = TcpListener::bind((bind.as_str(), port)).unwrap_or_else(|e| {
        eprintln!("не слушается {bind}:{port}: {e}");
        std::process::exit(1);
    });
    prepare_listener_shutdown(&listener, port == 0);
    // Прогрев — уже за слушателем: порт открыт, и узел отвечает всё это время.
    //
    // Порядок внутри тоже не случаен. Сперва метаданные поиска: словари, нормы,
    // границы блоков — то, без чего не обходится **ни один** запрос. Обратное
    // отображение греется после и только там, где есть приём: поиск его не
    // касается, а места оно занимает больше, чем все метаданные поиска вместе.
    if warm {
        let live_now = Arc::clone(&live);
        let warm_thread = std::thread::Builder::new()
            .name("warm".into())
            .spawn(move || {
                let t0 = Instant::now();
                let segs = { live_now.read().expect("набор сегментов").clone() };
                // Предел объявлен, а не подобран: половина памяти машины и две
                // минуты. Половина — потому что прогрев занимает кеш **ядра**,
                // который делится со всем, что на машине есть; две минуты —
                // потому что за ними прогрев перестаёт быть прогревом и
                // становится фоновым чтением всего диска.
                let cap = svindb_core::mem::available().map_or(0, |r| (r / 2) as usize);
                let mut left = svindb_core::mmap::WarmBudget::new(cap, WARM_MS);
                let meta = segs.warm_within(&mut left);
                let ids = if writable && !left.stopped() { segs.warm_ids() } else { 0 };
                println!(
                    "прогрето: метаданных {:.2} ГБ, обратного отображения {:.2} ГБ, \
                     за {:.1} с{}",
                    meta as f64 / 1e9,
                    ids as f64 / 1e9,
                    t0.elapsed().as_secs_f64(),
                    // Усечение называется вслух: молчаливое читается как
                    // «прогрето всё», и потом холодные первые запросы объясняют
                    // себе чем угодно, кроме настоящей причины.
                    if left.stopped() {
                        format!(
                            " — оборвано пределом ({:.1} ГБ / {} с), прогрето не всё",
                            cap as f64 / 1e9,
                            WARM_MS / 1000
                        )
                    } else {
                        String::new()
                    }
                );
            })
            .expect("поток прогрева");
        background_threads.push(warm_thread);
    }
    let guard = Arc::new(auth::Guard::new(cfg.auth.clone()).unwrap_or_else(|e| {
        eprintln!("права доступа: {e}");
        std::process::exit(2);
    }));
    if guard.on() {
        println!(
            "права включены: пользователей {}, ролей {}",
            cfg.auth.users.len(),
            cfg.auth.roles.len()
        );
        // Сказать это надо вслух и каждый раз. Права закрывают чужой запрос, но
        // не чужие глаза на проводе: `Basic` — это base64, а не шифр.
        println!(
            "внимание: TLS нет — пароли и токены идут открытым текстом; \
             ставьте узел за проксировщиком или в доверенной сети"
        );
    }
    if !cfg.cors.origins.is_empty() {
        println!("CORS: источников {}", cfg.cors.origins.len());
    }
    if !loopback(&bind) && !guard.on() {
        eprintln!(
            "внимание: узел слушает {bind} — прав доступа нет, и всякий, кто дотянется \
             до порта, прочтёт весь индекс. Заведите auth в файле настроек"
        );
    }
    // Общий предел памяти запросов — доля из общего реестра, а не своё число
    // рядом. Пока каждая подсистема считала долю сама, про сумму не знал никто:
    // доли складывались как получится, и получалось больше единицы. Не измерили
    // память — не обещаем предела: ноль означает «не считать», то есть прежнее
    // поведение.
    // Реестр долей заводится один на узел, и поиск берёт из него свой счёт.
    // Пока каждая подсистема считала долю сама, про сумму не знал никто.
    let ledger = svindb_core::govern::Ledger::from_machine();
    let query_pool = Arc::new(svindb_core::budget::Pool::new(
        ledger.pool(svindb_core::govern::Class::Query).map_or(0, |p| p.max()),
    ));
    // Терм заводится до узла: primary объявляет о вступлении **до** первого
    // принятого запроса, иначе окно между подъёмом и объявлением — это окно,
    // в котором он пишет, не зная о собственном смещении.
    let terms = writer.is_some().then(|| Arc::new(replicate::Terms::open(&dir)));
    // `--replicas` без приёма — молча ничего не значит, и это худший исход:
    // оператор, поднявший узел координатором или в режиме чтения, считал бы, что
    // копии у него есть. Их нет.
    if !replica_addrs.is_empty() && terms.is_none() {
        eprintln!(
            "--replicas= задан, но узел ничего не принимает: \
             это координатор (--shards=) либо --read-only. Реплики держит тот, кто пишет"
        );
        std::process::exit(2);
    }
    let peers = match (terms.as_ref(), term) {
        (Some(t), Some(term)) if !replica_addrs.is_empty() => {
            let seen = match t.observe(term) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!(
                        "узел не может вступить в терм {term}: {}. \
                         Это не сбой запуска, а отказ разойтись с уже виденным",
                        e.say()
                    );
                    std::process::exit(2);
                }
            };
            println!(
                "primary шарда: терм {term}, реплик {}, договор {}",
                replica_addrs.len(),
                ack.name()
            );
            let _ = seen;
            let peers = Arc::new(replicate::Peers::new(
                replica_addrs.clone(),
                ack,
                term,
                0,
                Duration::from_secs(30),
                keep_mb.saturating_mul(1 << 20),
                catchup_mbps.saturating_mul(1 << 20),
            ));
            // Счёт подхватывается **у копий**, а не из своего файла: свой файл
            // хранит то, что узел принял как реплика, а не то, что разослал как
            // primary. Разбор — у `Peers::resume`.
            peers.resume();
            Some(peers)
        }
        _ => None,
    };
    let node = Arc::new(Node {
        query_pool,
        source_cache,
        dir: dir.clone(),
        live: Arc::clone(&live),
        snapshot_scope: None,
        settings_lane: Arc::new(Mutex::new(())),
        ingest_budget: None,
        guard,
        cors: cfg.cors.clone(),
        snapshot: cfg.snapshot.clone(),
        reloads: Arc::clone(&reloads),
        fanout: shard_addrs.as_ref().map(|a| match shard_ranges.clone() {
            Some(r) => coord::Fanout::with_ranges(
                a.clone(),
                r,
                Duration::from_secs(30),
                coord::DEFAULT_PERMITS,
            ),
            None => coord::Fanout::new(a.clone(), Duration::from_secs(30)),
        }),
        live_settings: Arc::clone(&live_settings),
        writer: writer.clone(),
        _writer_lock: writer_lock,
        background_stop,
        background_threads: Mutex::new(background_threads),
        metrics: Arc::clone(&node_metrics),
        // После `declare_all`, а не до: реестр на диске появляется там, и
        // снятый раньше был бы пуст на новом индексе — то есть ровно в том
        // случае, ради которого объявленная схема сюда и попала.
        declared: Declared::of(&dir).unwrap_or_else(|error| {
            eprintln!("схема индекса: {error}");
            std::process::exit(1);
        }),
        maintaining,
        terms: terms.clone(),
        repl: peers.clone(),
    });
    // Поток на **соединение**, а не на запрос, и заводятся они один раз.
    //
    // Работников больше, чем ядер, и это не противоречие: они почти всё время
    // спят на чтении из сокета, а не считают. Ядра делят между собой те
    // немногие, что заняты запросом прямо сейчас.
    let workers = (cores() * 8).clamp(16, 256);
    println!("работников на соединения: {workers}");
    let inflight = Arc::new(AtomicUsize::new(0));
    let body_limit = http::inflight_body_limit(svindb_core::mem::available());
    println!("общий бюджет тел запросов: {} МБ", body_limit / (1 << 20));
    let body_budget = Arc::new(http::BodyBudget::new(body_limit));
    // Приём вынесен из работников: медленные клиенты больше не могут занять
    // все места, на которых вообще вызывается accept. Очередь ограничена, и
    // перегруженный узел быстро отвечает 503 вместо зависания новых клиентов.
    let (connections, incoming) =
        std::sync::mpsc::sync_channel::<(TcpStream, std::net::SocketAddr)>(workers * 2);
    let incoming = Arc::new(std::sync::Mutex::new(incoming));
    let active_connections = Arc::new(ActiveConnections::default());
    let mut pool = Vec::with_capacity(workers);
    for _ in 0..workers {
        let incoming = Arc::clone(&incoming);
        let inflight = Arc::clone(&inflight);
        let body_budget = Arc::clone(&body_budget);
        let node = Arc::clone(&node);
        let active_connections = Arc::clone(&active_connections);
        let tls = tls.clone();
        pool.push(std::thread::spawn(move || loop {
            let pair = incoming.lock().ok().and_then(|rx| rx.recv().ok());
            let Some((s, peer)) = pair else { break };
            // macOS inherits O_NONBLOCK from the shutdown-aware listener.
            // Request deadlines require blocking reads, including TLS handshakes.
            if s.set_nonblocking(false).is_err() {
                continue;
            }
            let _ = s.set_nodelay(true);
            // Таймаут на запись был не задан вовсе, и это держало работника
            // навсегда: клиент, перестающий читать, оставлял поток в `write` до
            // конца жизни процесса. Достаточно нескольких таких, чтобы узел
            // перестал принимать соединения, ничего при этом не считая.
            let _ = s.set_write_timeout(Some(Duration::from_secs(60)));
            // Одна ручка на чтение и на запись. Прежде здесь стоял `try_clone`
            // и две независимые: для голого TCP разницы нет, для TLS это
            // невозможно — сеанс это счётчики и ключи в одном месте.
            // Рукопожатие TLS — в работнике, а не в цикле `accept`.
            //
            // В цикле оно остановило бы приём **всех** соединений на время
            // одного: клиенту достаточно открыть сокет и замолчать, чтобы узел
            // перестал принимать кого бы то ни было. Здесь же оно занимает
            // ровно одного работника и ограничено тем же сроком, что и запрос.
            let opened: std::io::Result<wire::Wire> = match tls.as_ref() {
                None => Ok(wire::Wire::plain(s, Duration::from_secs(60))),
                #[cfg(feature = "tls")]
                Some(cfg) => wire::accept(cfg, s, Duration::from_secs(60)),
                #[cfg(not(feature = "tls"))]
                Some(never) => match *never {},
            };
            let Ok(opened) = opened else { continue };
            let _active_connection = active_connections.register(&opened);
            let _connection_disconnect = install_connection_disconnect(&opened);
            let mut reader = std::io::BufReader::new(opened);
            // Keep-alive ограничен: один клиент не держит место бесконечно,
            // даже если каждый отдельный запрос укладывается в deadline.
            // Счётчик виден внутри: последний ответ обязан сказать клиенту,
            // что соединение закрывается. Прежде цикл просто кончался, а
            // `keep` считался только по заголовку клиента и о пределе не знал:
            // сотый ответ уходил с `Connection: keep-alive`, узел закрывал
            // сокет, и клиент получал обрыв на следующем запросе. На стенде
            // это давало ровно двадцать четыре `Broken pipe` за пятнадцать
            // миллионов документов — 3000 запросов на восемь потоков, по
            // триста семьдесят пять на соединение, три закрытия на каждом.
            // Документы при этом не терялись только потому, что загрузчик
            // повторял пачку; клиент без повторов терял бы их молча.
            const PER_CONNECTION: usize = 100;
            for served in 0..PER_CONNECTION {
                // Адаптивный срок различает два безопасных случая. Полностью
                // молчащее keep-alive-соединение получает обычный минутный
                // idle timeout, а клиент, уже приславший неполный заголовок,
                // получает короткое окно прогресса. Без этого один slowloris
                // занимал connection worker на 60 секунд и мог вытеснить
                // control-запросы. Проверка readiness делается через poll(2)
                // без изменения blocking-флага основного сокета.
                renew_connection_header_deadline(&mut reader);
                let mut req = match http::read_head_from(&mut reader) {
                    http::Incoming::Request(r) => r,
                    http::Incoming::Refused(code, why) => {
                        http::set_extra(String::new());
                        let payload = format!("{{\"error\":{}}}", json_string(why));
                        http::respond_with(reader.get_mut(), code, &payload, false);
                        break;
                    }
                    http::Incoming::RefusedBody(code, why, len) => {
                        // Без этого клиент получает Broken pipe во время
                        // отправки и не видит 413. Читается фиксированным
                        // буфером и только до жёсткого потолка `http`.
                        http::set_extra(String::new());
                        let payload = format!("{{\"error\":{}}}", json_string(why));
                        http::respond_with(reader.get_mut(), code, &payload, false);
                        reader.get_mut().renew(Duration::from_secs(60));
                        let complete = http::discard_body_from(&mut reader, len);
                        if !complete {
                            break;
                        }
                        break;
                    }
                    http::Incoming::Done => break,
                };
                // Дополнительные заголовки относятся только к одному ответу.
                // Сбрасываем их до CORS gate: если прошлый запрос вернул
                // Retry-After, он не должен протечь в следующий запрос без
                // Origin или заменить его CORS-набор.
                http::set_extra(String::new());
                // Предел соединения — такая же причина закрыться, как и
                // просьба клиента.
                let keep = !req.close && served + 1 < PER_CONNECTION;
                // Цель разбирается **один раз** и здесь: дальше её читают
                // права, учёт и выдача, и пока каждый читал её по-своему, они
                // расходились в решениях. Разбор — в [`route`], там же разбор
                // почему.
                let route = route::Route::parse(&req.method, &req.path);
                // Имя индекса в пути обязано называть **этот** индекс.
                //
                // Узел держит один индекс — так он устроен: один писатель, один
                // список сегментов, один журнал. Прежде имя из пути принималось
                // и выбрасывалось, и `/заказы/_bulk` с `/пользователи/_bulk`
                // молча писали в одно место. Клиент, разделивший данные по
                // именам, узнавал об этом не отказом, а тем, что чужие
                // документы находятся его запросом.
                //
                // Разбор контракта — в `docs/adr/0002`.
                if let Some(want) = route.index.as_deref() {
                    if let Err(msg) = index_name_ok(&node, want) {
                        http::set_extra(String::new());
                        http::respond_with(
                            reader.get_mut(),
                            404,
                            &format!("{{\"error\":{}}}", json_string(&msg)),
                            false,
                        );
                        break;
                    }
                }
                // Тело ещё не прочитано. Любой отказ закрывает соединение,
                // иначе его байты стали бы строкой следующего запроса.
                if !gate(reader.get_mut(), &req, &route, &node, false, Some(peer.ip())) {
                    break;
                }
                let _body_permit = match body_budget.reserve(req.body_len()) {
                    Ok(permit) => permit,
                    Err(full) => {
                        // Отвечаем до крупной аллокации, но тело всё равно
                        // дочитываем фиксированным буфером. Если закрыть сокет
                        // сразу, клиент, который ещё отправляет 31 МиБ, увидит
                        // Broken pipe вместо управляемого retryable 429 и не
                        // сможет отличить backpressure от упавшего узла.
                        http::set_extra("Retry-After: 1\r\n".to_string());
                        let payload = format!(
                            "{{\"error\":\"общий бюджет тел запросов исчерпан; повторите с backoff\",\
                             \"want_bytes\":{},\"used_bytes\":{},\"limit_bytes\":{},\
                             \"retryable\":true}}",
                            full.want, full.used, full.max
                        );
                        http::respond_with(reader.get_mut(), 429, &payload, false);
                        reader.get_mut().renew(Duration::from_secs(60));
                        let _ = http::discard_body_from(&mut reader, req.body_len());
                        break;
                    }
                };
                reader.get_mut().renew(Duration::from_secs(60));
                if !http::read_body_from(&mut reader, &mut req) {
                    break;
                }
                // Измерение стоит **здесь**, а не внутри обработчика: это
                // единственное место, где виден весь запрос целиком — от
                // разбора заголовка до отданного ответа. Внутри обработчика
                // видна только та часть, до которой запрос дошёл, и ранние
                // отказы (права, размыкатель, негодное тело) в счёт не попали
                // бы вовсе — то есть метрика молчала бы ровно про то, ради чего
                // её и заводят.
                let kind = metrics::Kind::of(&route, node.fanout.is_some());
                let began = Instant::now();
                http::forget_code();
                serve(
                    reader.get_mut(),
                    req,
                    &route,
                    threads,
                    &inflight,
                    keep,
                    &node,
                );
                let code = http::last_code();
                if code != 0 {
                    node.metrics.request(kind, code, began.elapsed().as_secs_f64() * 1000.0);
                }
                if !keep {
                    break;
                }
            }
        }));
    }
    loop {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            break;
        }
        let pair = match listener.accept() {
            Ok(pair) => pair,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                eprintln!("приём соединения остановлен: {error}");
                break;
            }
        };
        match connections.try_send(pair) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full((stream, _))) => {
                let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
                http::set_extra(String::new());
                // Отказ на пороге: соединение до работника не дошло, и заводить
                // ему полноценную ручку не за чем — но отвечать надо тем же
                // способом, иначе ответ разъедется с остальными.
                let mut w = wire::Wire::plain(stream, Duration::from_secs(1));
                http::respond_with(
                    &mut w,
                    503,
                    "{\"error\":\"узел занят: очередь соединений заполнена\"}",
                    false,
                );
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => break,
        }
    }
    active_connections.shutdown_all();
    // Сначала закрываем sender: работники, закончив текущий запрос, выходят из
    // `recv`. Node уничтожается только после их join, а его Drop останавливает
    // refresh/maintenance и дожидается durable фоновых границ.
    drop(connections);
    for worker in pool {
        let _ = worker.join();
    }
}

/// Поднимает обычный layout, в котором один HTTP-процесс обслуживает несколько
/// независимых локальных индексов. Для нового каталога это режим по умолчанию;
/// имя функции отличает его только от внутреннего legacy-runtime.
#[allow(clippy::too_many_arguments)]
fn cmd_serve_multi(
    flags: &[&str],
    dir: &str,
    port: u16,
    threads: usize,
    writable: bool,
    maintaining: bool,
    warm: bool,
    tls: Option<wire::Acceptor>,
    cfg: config::Config,
) {
    for incompatible in ["--shards=", "--replicas=", "--term=", "--ack="] {
        if flags.iter().any(|flag| flag.starts_with(incompatible)) {
            eprintln!(
                "{incompatible} нельзя применить к multi-index root: топология должна быть задана отдельно для каждого индекса; общий fanout смешал бы их данные"
            );
            std::process::exit(2);
        }
    }
    let bind = flags
        .iter()
        .find_map(|flag| flag.strip_prefix("--bind="))
        .map(str::to_string)
        .or(cfg.bind.clone())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let max_indices = match flags.iter().find_map(|flag| flag.strip_prefix("--max-indices=")) {
        Some(raw) => raw.parse::<usize>().ok().filter(|n| *n > 0).unwrap_or_else(|| {
            eprintln!("--max-indices={raw}: ожидается положительное целое число");
            std::process::exit(2);
        }),
        None => 64,
    };
    let posmin = flags
        .iter()
        .find_map(|flag| flag.strip_prefix("--posmin="))
        .map(|raw| {
            raw.parse::<u32>().unwrap_or_else(|_| {
                eprintln!("--posmin={raw}: ожидается целое от 0 до {}", u32::MAX);
                std::process::exit(2);
            })
        })
        .unwrap_or(0);
    let overrides = IndexOverrides::from_flags(flags).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(2);
    });
    let guard = Arc::new(auth::Guard::new(cfg.auth.clone()).unwrap_or_else(|error| {
        eprintln!("права доступа: {error}");
        std::process::exit(2);
    }));
    if !loopback(&bind) && !guard.on() {
        eprintln!(
            "внимание: многoиндексный узел слушает {bind} без прав доступа; заведите auth в файле настроек"
        );
    }
    svindb_core::mmap::set_map_shortfall(map_shortfall_for(
        &bind,
        !writable,
        flags.contains(&"--allow-low-map-count"),
    ));
    let ledger = svindb_core::govern::Ledger::from_machine();
    let query_pool = Arc::new(svindb_core::budget::Pool::new(
        ledger.pool(svindb_core::govern::Class::Query).map_or(0, |pool| pool.max()),
    ));
    let source_cache =
        Arc::new(source_cache::Cache::new(resources_with(&cfg.workload).source_cache_bytes));
    let settings_lane = Arc::new(Mutex::new(()));
    let ingest_budget = Arc::new(multi::IngestBudget::default());
    let options = LocalIndexOptions {
        writable,
        maintaining,
        no_wal: flags.contains(&"--no-wal"),
        sync_flush: flags.contains(&"--sync-flush"),
        posmin,
        fields_fallback: flags
            .iter()
            .find_map(|flag| flag.strip_prefix("--fields="))
            .unwrap_or("")
            .to_string(),
        overrides,
        guard: Arc::clone(&guard),
        cors: cfg.cors.clone(),
        snapshot: cfg.snapshot.clone(),
        query_pool,
        source_cache,
        workload: cfg.workload.clone(),
        maintenance_lane: Arc::new(Mutex::new(())),
        settings_lane,
        ingest_budget,
    };
    let root = multi::Root::open(dir, writable).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let registry =
        Arc::new(multi::Registry::open(root, options, max_indices).unwrap_or_else(|error| {
            eprintln!("многoиндексный узел не открылся: {error}");
            std::process::exit(1);
        }));
    let startup_cache_dropped = trim_startup_page_cache(&registry);
    if startup_cache_dropped > 0 {
        println!(
            "startup cache reserve: immutable pages advised={} МиБ",
            startup_cache_dropped / (1 << 20)
        );
    }
    if warm {
        // Один общий бюджет и последовательный проход. Дать каждому индексу
        // «половину RAM» означало бы при десяти индексах пообещать кешу пять
        // машин и вытеснить начало прогрева собственным концом.
        registry.start_warm().unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });
    }
    let listener = TcpListener::bind((bind.as_str(), port)).unwrap_or_else(|error| {
        eprintln!("не слушается {bind}:{port}: {error}");
        std::process::exit(1);
    });
    prepare_listener_shutdown(&listener, port == 0);
    println!(
        "многoиндексный узел: корень {dir}, индексов {}, предел {max_indices}, порт {port}",
        registry.list().len()
    );
    run_multi_listener(listener, tls, threads, registry, guard, cfg.cors);
}

/// Возвращает control/search reserve после большого открытия multi-index root.
///
/// Открытие сотен mmap-сегментов может наполнить cgroup чистыми страницами ещё
/// до первого запроса. Elasticsearch оставляет память вне heap для filesystem
/// cache, RocksDB умеет обходить page cache для фонового I/O; здесь сохраняем
/// mmap-чтение, но при фактическом нарушении reserve помечаем старые immutable
/// страницы как ненужные. До listener и запросов конкурирующих читателей нет.
fn trim_startup_page_cache(registry: &multi::Registry) -> u64 {
    let pressure = svindb_core::mem::pressure();
    let Some(limit) = pressure.limit else { return 0 };
    let reserve = svindb_core::govern::pressure_relief_reserve(limit);
    if !pressure.near_limit(reserve) {
        return 0;
    }
    let _exclusive = query_cache_gate().write().unwrap_or_else(std::sync::PoisonError::into_inner);
    registry
        .all_nodes()
        .into_iter()
        .map(|(_, node)| {
            let segments = node.live.read().expect("набор сегментов").clone();
            segments
                .list
                .iter()
                .filter(|segment| !segment.src.is_memory())
                .map(|segment| {
                    segment.src.drop_immutable_mapping_cache() as u64
                        + host::drop_immutable_segment_cache(&segment.dir)
                })
                .sum::<u64>()
        })
        .sum()
}

/// Один bounded проход emergency cache relief.
///
/// Exclusive guard берётся отдельно на каждый сегмент. Поэтому появившийся
/// между двумя квантами point/search получает shared guard и немедленно
/// останавливает дальнейшее вытеснение; запрос не ждёт обхода всего индекса.
fn relieve_runtime_page_cache(registry: &multi::Registry) -> u64 {
    if resources().active(ResourceClass::Compaction) > 0 {
        return 0;
    }
    let mut advised = 0u64;
    for (_, node) in registry.all_nodes() {
        let segments = node.live.read().expect("набор сегментов").clone();
        for segment in &segments.list {
            if segment.src.is_memory() {
                continue;
            }
            let Ok(_exclusive) = query_cache_gate().try_write() else { return advised };
            advised = advised.saturating_add(segment.src.drop_immutable_mapping_cache() as u64);
            advised = advised.saturating_add(host::drop_immutable_segment_cache(&segment.dir));
            std::thread::yield_now();
        }
    }
    advised
}

fn run_multi_listener(
    listener: TcpListener,
    tls: Option<wire::Acceptor>,
    threads: usize,
    registry: Arc<multi::Registry>,
    guard: Arc<auth::Guard>,
    cors_cfg: config::Cors,
) {
    // Реальный cgroup breaker дополняется reclamation-контроллером. Breaker
    // даёт retryable 429 сразу, контроллер в фоне возвращает clean cache и тем
    // самым снимает отказ без ручного вмешательства. Такой split повторяет
    // RocksDB write-stall/no_slowdown: request thread не выполняет тяжёлую
    // очистку сам и не держит application lock.
    let cache_registry = Arc::clone(&registry);
    let cache_pressure = std::thread::Builder::new()
        .name("svindb-cache-pressure".into())
        .spawn(move || {
            while !SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
                let pressure = svindb_core::mem::pressure();
                if let Some(limit) = pressure.limit {
                    let reserve = svindb_core::govern::pressure_relief_reserve(limit);
                    if pressure.near_limit(reserve) {
                        let _ = relieve_runtime_page_cache(&cache_registry);
                    }
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        })
        .ok();
    let workers = (cores() * 8).clamp(16, 256);
    let inflight = Arc::new(AtomicUsize::new(0));
    let body_limit = http::inflight_body_limit(svindb_core::mem::available());
    let body_budget = Arc::new(http::BodyBudget::new(body_limit));
    println!(
        "работников на соединения: {workers}, общий бюджет тел: {} МБ",
        body_limit / (1 << 20)
    );
    let (connections, incoming) =
        std::sync::mpsc::sync_channel::<(TcpStream, std::net::SocketAddr)>(workers * 2);
    let incoming = Arc::new(Mutex::new(incoming));
    let active_connections = Arc::new(ActiveConnections::default());
    let mut pool = Vec::with_capacity(workers);
    for _ in 0..workers {
        let incoming = Arc::clone(&incoming);
        let inflight = Arc::clone(&inflight);
        let body_budget = Arc::clone(&body_budget);
        let registry = Arc::clone(&registry);
        let guard = Arc::clone(&guard);
        let cors_cfg = cors_cfg.clone();
        let active_connections = Arc::clone(&active_connections);
        let tls = tls.clone();
        pool.push(std::thread::spawn(move || loop {
            let pair = incoming.lock().ok().and_then(|receiver| receiver.recv().ok());
            let Some((stream, peer)) = pair else { break };
            // Accepted sockets may inherit the listener's nonblocking mode.
            if stream.set_nonblocking(false).is_err() {
                continue;
            }
            let _ = stream.set_nodelay(true);
            let _ = stream.set_write_timeout(Some(Duration::from_secs(60)));
            let opened: std::io::Result<wire::Wire> = match tls.as_ref() {
                None => Ok(wire::Wire::plain(stream, Duration::from_secs(60))),
                #[cfg(feature = "tls")]
                Some(config) => wire::accept(config, stream, Duration::from_secs(60)),
                #[cfg(not(feature = "tls"))]
                Some(never) => match *never {},
            };
            let Ok(opened) = opened else { continue };
            let _active_connection = active_connections.register(&opened);
            let _connection_disconnect = install_connection_disconnect(&opened);
            let mut reader = std::io::BufReader::new(opened);
            const PER_CONNECTION: usize = 100;
            for served_count in 0..PER_CONNECTION {
                renew_connection_header_deadline(&mut reader);
                let mut req = match http::read_head_from(&mut reader) {
                    http::Incoming::Request(request) => request,
                    http::Incoming::Refused(code, why) => {
                        http::set_extra(String::new());
                        http::respond_with(
                            reader.get_mut(),
                            code,
                            &format!("{{\"error\":{}}}", json_string(why)),
                            false,
                        );
                        break;
                    }
                    http::Incoming::RefusedBody(code, why, len) => {
                        http::set_extra(String::new());
                        http::respond_with(
                            reader.get_mut(),
                            code,
                            &format!("{{\"error\":{}}}", json_string(why)),
                            false,
                        );
                        reader.get_mut().renew(Duration::from_secs(60));
                        let _ = http::discard_body_from(&mut reader, len);
                        break;
                    }
                    http::Incoming::Done => break,
                };
                // Сбрасываем per-request headers до gate, сохраняя CORS,
                // который gate установит для текущего Origin.
                http::set_extra(String::new());
                let keep = !req.close && served_count + 1 < PER_CONNECTION;
                let route = route::Route::parse(&req.method, &req.path);
                // Сначала права, потом существование индекса: иначе анонимный
                // клиент мог бы перебирать имена по разнице 404/401.
                if !gate_with(
                    reader.get_mut(),
                    &req,
                    &route,
                    &guard,
                    &cors_cfg,
                    false,
                    Some(peer.ip()),
                ) {
                    break;
                }
                let _body_permit = match body_budget.reserve(req.body_len()) {
                    Ok(permit) => permit,
                    Err(full) => {
                        http::set_extra("Retry-After: 1\r\n".to_string());
                        let payload = format!(
                            "{{\"error\":\"общий бюджет тел запросов исчерпан\",\"want_bytes\":{},\"used_bytes\":{},\"limit_bytes\":{},\"retryable\":true}}",
                            full.want, full.used, full.max
                        );
                        http::respond_with(reader.get_mut(), 429, &payload, false);
                        reader.get_mut().renew(Duration::from_secs(60));
                        let _ = http::discard_body_from(&mut reader, req.body_len());
                        break;
                    }
                };
                reader.get_mut().renew(Duration::from_secs(60));
                if !http::read_body_from(&mut reader, &mut req) {
                    break;
                }
                dispatch_multi(
                    reader.get_mut(),
                    req,
                    &route,
                    threads,
                    &inflight,
                    keep,
                    &registry,
                    &guard,
                );
                if !keep {
                    break;
                }
            }
        }));
    }
    loop {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            break;
        }
        let pair = match listener.accept() {
            Ok(pair) => pair,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                eprintln!("приём соединения остановлен: {error}");
                break;
            }
        };
        match connections.try_send(pair) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full((stream, _))) => {
                let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
                http::set_extra(String::new());
                let mut wire = wire::Wire::plain(stream, Duration::from_secs(1));
                http::respond_with(
                    &mut wire,
                    503,
                    "{\"error\":\"узел занят: очередь соединений заполнена\"}",
                    false,
                );
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => break,
        }
    }
    active_connections.shutdown_all();
    drop(connections);
    for worker in pool {
        let _ = worker.join();
    }
    if let Some(cache_pressure) = cache_pressure {
        let _ = cache_pressure.join();
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_multi(
    stream: &mut wire::Wire,
    req: http::Request,
    route: &route::Route,
    threads: usize,
    inflight: &AtomicUsize,
    keep: bool,
    registry: &multi::Registry,
    guard: &auth::Guard,
) {
    if refuse_route(stream, &req, route, keep) {
        return;
    }
    if serve_multi_control(stream, &req, route, registry, guard, keep) {
        return;
    }
    let Some(name) = route.index.as_deref() else {
        http::respond_with(
            stream,
            400,
            "{\"error\":\"в многoиндексном режиме укажите индекс в пути: /{index}/_search, /{index}/_bulk и т.д.\",\"type\":\"index_required_exception\"}",
            keep,
        );
        return;
    };
    if route.writes() && !registry.writable() {
        http::respond_with(
            stream,
            403,
            "{\"error\":\"узел открыт только для чтения\",\"type\":\"cluster_block_exception\"}",
            keep,
        );
        return;
    }
    if route.endpoint == route::Endpoint::Search {
        let mut expand_open = true;
        let mut expand_closed = false;
        if let Some(expand) = query_value(&route.query, "expand_wildcards") {
            let values: Vec<&str> = expand.split(',').collect();
            let invalid = values
                .iter()
                .find(|value| !matches!(**value, "open" | "closed" | "all" | "none" | "hidden"));
            if let Some(value) = invalid {
                http::respond_with(
                    stream,
                    400,
                    &format!(
                        "{{\"error\":{}}}",
                        json_string(&format!(
                            "expand_wildcards содержит неизвестное значение {value:?}"
                        ))
                    ),
                    keep,
                );
                return;
            }
            if values.contains(&"none") && values.len() != 1 {
                http::respond_with(
                    stream,
                    400,
                    "{\"error\":\"expand_wildcards=none нельзя объединять с другими значениями\"}",
                    keep,
                );
                return;
            }
            expand_open = values.contains(&"open") || values.contains(&"all");
            expand_closed = values.contains(&"closed") || values.contains(&"all");
            // SvinDB не имеет hidden-state. Значение принимается ради
            // совместимости, но не расширяет состав: скрытых индексов в
            // каталоге быть не может. `none` уже проверен как одиночное.
        }
        let ignore_unavailable = match query_bool(&route.query, "ignore_unavailable", false) {
            Ok(value) => value,
            Err(error) => {
                http::respond_with(
                    stream,
                    400,
                    &format!("{{\"error\":{}}}", json_string(&error)),
                    keep,
                );
                return;
            }
        };
        let allow_no_indices = match query_bool(&route.query, "allow_no_indices", false) {
            Ok(value) => value,
            Err(error) => {
                http::respond_with(
                    stream,
                    400,
                    &format!("{{\"error\":{}}}", json_string(&error)),
                    keep,
                );
                return;
            }
        };
        let targets = match registry.read_targets_with(
            name,
            ignore_unavailable,
            allow_no_indices,
            expand_open,
            expand_closed,
        ) {
            Ok(targets) => targets,
            Err(error) => {
                respond_catalog_error(stream, &error, keep);
                return;
            }
        };
        if targets.is_empty() {
            http::respond_with(stream, 200, "{\"took_ms\":0,\"visited\":0,\"hits\":[]}", keep);
            return;
        }
        return serve_local_multi_search(stream, req, route, &targets, threads, inflight, keep);
    }
    let target = if route.endpoint == route::Endpoint::Flush {
        registry.flush_target(name)
    } else if route.writes() {
        registry.write_target(name)
    } else if route.endpoint == route::Endpoint::Settings
        || (route.endpoint == route::Endpoint::Snapshot && req.method == "POST")
    {
        registry.one_metadata_target(name)
    } else if matches!(
        route.endpoint,
        route::Endpoint::Health
            | route::Endpoint::Metrics
            | route::Endpoint::Stats
            | route::Endpoint::Schema
            | route::Endpoint::Inspect
            | route::Endpoint::Snapshot
    ) {
        registry.one_control_target(name)
    } else {
        registry.one_read_target(name)
    };
    let target = match target {
        Ok(target) => target,
        Err(error) => {
            respond_catalog_error(stream, &error, keep);
            return;
        }
    };
    let node = target.node;
    let kind = metrics::Kind::of(route, false);
    let began = Instant::now();
    http::forget_code();
    serve(stream, req, route, threads, inflight, keep, &node);
    let code = http::last_code();
    if code != 0 {
        node.metrics.request(kind, code, began.elapsed().as_secs_f64() * 1000.0);
    }
}

#[allow(clippy::too_many_arguments)]
fn serve_local_multi_search(
    stream: &mut wire::Wire,
    req: http::Request,
    route: &route::Route,
    targets: &[multi::Target],
    threads: usize,
    inflight: &AtomicUsize,
    keep: bool,
) {
    let Some(first) = targets.first() else {
        return http::respond_with(
            stream,
            404,
            "{\"error\":\"цель поиска не раскрылась ни в один индекс\"}",
            keep,
        );
    };
    // Один общий `Segments` даёт движку настоящий глобальный top-K, sort,
    // cardinality и terms-reduce. Это не склейка готовых JSON-верхушек:
    // агрегации видят все сегменты разом и применяют те же доказательства
    // точности, что внутри одного физического индекса.
    let signature = first.node.declared.signature;
    for target in targets.iter().skip(1) {
        let other = target.node.declared.signature;
        if other != signature {
            let payload = serde_json::json!({
                "error": format!(
                    "индексы {:?} и {:?} имеют разные schema/mapping; один запрос нельзя разобрать двумя несовместимыми способами",
                    first.name, target.name
                ),
                "type": "illegal_argument_exception",
                "indices": [first.name.as_str(), target.name.as_str()],
            });
            return http::respond_with(stream, 400, &payload.to_string(), keep);
        }
    }
    // У объединённого запроса нет «главного» индекса: он обязан подчиняться
    // самому строгому ненулевому breaker среди всех физических целей. Иначе
    // перестановка имён в alias или `a,b` могла бы незаметно ослабить предел
    // памяти. Ноль сохраняет прежнюю семантику «явный предел не задан» и
    // участвует только тогда, когда ненулевого ограничения нет ни у кого.
    let memory_budget_mb = strictest_memory_budget(targets.iter().filter_map(|target| {
        target.node.live_settings.read().ok().map(|settings| settings.memory_budget_mb)
    }));
    let capture_guard = match point_guard(&req, route) {
        Ok(guard) => guard,
        Err(error) => {
            return http::respond_with(
                stream,
                400,
                &format!("{{\"error\":{}}}", json_string(&error)),
                keep,
            )
        }
    };
    let _capture_disconnect = DisconnectWatch::start(stream, Arc::clone(&capture_guard));
    let snapshot = |target: &multi::Target| {
        let (segments, live) =
            capture_pit(&target.node.live, target.node.writer.as_ref(), Some(&capture_guard))?;
        let generation = target.node.writer.as_ref().map(|writer| {
            (Arc::clone(&target.node), writer.visibility_generation.load(Ordering::Acquire))
        });
        Ok::<_, String>((segments, live, generation))
    };
    let view = if targets.len() == 1 {
        let (segments, frozen, generation) = match snapshot(&targets[0]) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return http::respond_with(
                    stream,
                    503,
                    &format!(
                        "{{\"error\":{},\"retryable\":true,\"reason\":{}}}",
                        json_string(&error),
                        json_string(if capture_guard.fired() {
                            capture_guard.reason().name()
                        } else {
                            "read_view_raced"
                        })
                    ),
                    keep,
                )
            }
        };
        let to = segments.list.iter().map(|segment| segment.docs as u64).sum();
        LocalSearchView {
            segments,
            live: frozen,
            owners: vec![SearchOwner { from: 0, to, index: targets[0].name.clone() }],
            memory_budget_mb,
            generations: generation.into_iter().collect(),
        }
    } else {
        let mut list = Vec::new();
        let mut base = Vec::new();
        let mut owners = Vec::new();
        let mut generations = Vec::new();
        let mut frozen = Vec::new();
        let mut cursor = 0u64;
        for target in targets {
            let from = cursor;
            let (segments, live, generation) = match snapshot(target) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return http::respond_with(
                        stream,
                        503,
                        &format!("{{\"error\":{},\"retryable\":true}}", json_string(&error)),
                        keep,
                    )
                }
            };
            generations.extend(generation);
            frozen.extend(live.iter().cloned());
            for segment in &segments.list {
                base.push(cursor);
                cursor = match cursor.checked_add(segment.docs as u64) {
                    Some(next) => next,
                    None => {
                        return http::respond_with(
                            stream,
                            500,
                            "{\"error\":\"общее пространство документов переполнило u64\"}",
                            keep,
                        )
                    }
                };
                list.push(Arc::clone(segment));
            }
            owners.push(SearchOwner { from, to: cursor, index: target.name.clone() });
        }
        LocalSearchView {
            segments: Arc::new(Segments::from_parts(list, base)),
            live: Arc::new(frozen),
            owners,
            memory_budget_mb,
            generations,
        }
    };
    // Снятие локального ReadView завершено. Дальше `serve_with_view` создаёт
    // единственный search guard/monitor для полного запроса; держать внешний
    // watcher до окончания выдачи означало бы двойную регистрацию одного TCP
    // соединения в общем poll-мониторе.
    drop(_capture_disconnect);
    let node = &first.node;
    let kind = metrics::Kind::of(route, false);
    let began = Instant::now();
    http::forget_code();
    serve_with_view(stream, req, route, threads, inflight, keep, node, Some(&view));
    let code = http::last_code();
    if code != 0 {
        node.metrics.request(kind, code, began.elapsed().as_secs_f64() * 1000.0);
    }
}

fn search_schema_signature(dir: &str) -> Result<[u8; 32], String> {
    let read = |name: &str| -> Result<Vec<u8>, String> {
        let path = std::path::Path::new(dir).join(name);
        match svindb_core::fs::read_regular_limited(&path, 4 << 20, false) {
            Ok(bytes) => Ok(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(format!("{}: {error}", path.display())),
        }
    };
    let schema = read(svindb_core::schema::FILE)?;
    let mapping = read(mapping::FILE)?;
    // mapping.json исторически хранит всё тело CREATE, включая settings,
    // metadata и aliases. Для поиска значим только `mappings`: различное
    // описание или refresh interval не делает две одинаковые схемы
    // несовместимыми.
    let mapping = if mapping.is_empty() {
        Vec::new()
    } else {
        let value: serde_json::Value = serde_json::from_slice(&mapping)
            .map_err(|error| format!("{dir}/{}: {error}", mapping::FILE))?;
        let source_mode = mapping::SourceMode::from_root(&value)?;
        let mut mappings = value.get("mappings").cloned().unwrap_or_else(|| serde_json::json!({}));
        if let Some(object) = mappings.as_object_mut() {
            // Обе публичные формы source mode семантически равны. Подпись alias
            // обязана сравнивать исполненный контракт, а не место, где клиент
            // записал `disabled`; иначе два совместимых индекса нельзя искать
            // одним alias.
            object.insert("_source".into(), serde_json::json!({"mode": source_mode.name()}));
        }
        serde_json::to_vec(&mappings)
            .map_err(|error| format!("{dir}/{}: {error}", mapping::FILE))?
    };
    let mut bytes = Vec::with_capacity(16 + schema.len() + mapping.len());
    bytes.extend_from_slice(&(schema.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&schema);
    bytes.extend_from_slice(&(mapping.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&mapping);
    Ok(svindb_core::hash::sha256(&bytes))
}

fn serve_multi_control(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    registry: &multi::Registry,
    guard: &auth::Guard,
    keep: bool,
) -> bool {
    if route.endpoint == route::Endpoint::Security && route.index.is_none() {
        return serve_security(stream, req, route, guard, keep);
    }
    if route.endpoint == route::Endpoint::CatAliases {
        let catalog = registry.catalog();
        let mut rows = Vec::new();
        for (alias, bindings) in catalog.aliases() {
            for binding in bindings {
                let Some(entry) = catalog.by_uuid(&binding.target) else { continue };
                rows.push(serde_json::json!({
                    "alias": alias,
                    "index": entry.name,
                    "is_write_index": binding.is_write,
                    "state": entry.state.name(),
                }));
            }
        }
        http::respond_with(
            stream,
            200,
            &serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()),
            keep,
        );
        return true;
    }
    if route.endpoint == route::Endpoint::Alias {
        match req.method.as_str() {
            "GET" | "HEAD" => {
                let catalog = registry.catalog();
                let selected_alias = (!route.rest.is_empty()).then_some(route.rest.as_str());
                let selected_index = route.index.as_deref();
                let mut out = serde_json::Map::new();
                for entry in catalog.entries() {
                    if selected_index.is_some_and(|name| name != entry.name) {
                        continue;
                    }
                    let mut aliases = serde_json::Map::new();
                    for alias in catalog.aliases_of(&entry.uuid) {
                        if selected_alias.is_some_and(|want| want != alias) {
                            continue;
                        }
                        let flag = catalog
                            .bindings(alias)
                            .and_then(|bindings| {
                                bindings.iter().find(|binding| binding.target == entry.uuid)
                            })
                            .and_then(|binding| binding.is_write);
                        let mut spec = serde_json::Map::new();
                        if let Some(flag) = flag {
                            spec.insert("is_write_index".to_string(), flag.into());
                        }
                        aliases.insert(alias.to_string(), serde_json::Value::Object(spec));
                    }
                    if !aliases.is_empty() {
                        out.insert(entry.name.clone(), serde_json::json!({"aliases": aliases}));
                    }
                }
                if out.is_empty() {
                    let error = if let Some(alias) = selected_alias {
                        catalog::CatalogError::AliasNotFound { alias: alias.to_string() }
                    } else {
                        catalog::CatalogError::IndexNotFound {
                            name: selected_index.unwrap_or_default().to_string(),
                        }
                    };
                    return respond_catalog_error(stream, &error, keep);
                }
                if req.method == "HEAD" {
                    http::respond_empty(stream, 200, keep);
                    return true;
                }
                http::respond_with(stream, 200, &serde_json::Value::Object(out).to_string(), keep);
                return true;
            }
            "POST" if route.index.is_none() => {
                if !registry.writable() {
                    http::respond_with(
                        stream,
                        403,
                        "{\"error\":\"узел открыт только для чтения\"}",
                        keep,
                    );
                    return true;
                }
                let actions = match parse_alias_actions(&req.body) {
                    Ok(actions) => actions,
                    Err(error) => return respond_catalog_error(stream, &error, keep),
                };
                return match registry.aliases(&actions) {
                    Ok(catalog) => {
                        http::respond_with(
                            stream,
                            200,
                            &serde_json::json!({
                                "acknowledged": true,
                                "generation": catalog.generation(),
                            })
                            .to_string(),
                            keep,
                        );
                        true
                    }
                    Err(error) => respond_catalog_error(stream, &error, keep),
                };
            }
            "PUT" | "DELETE" if route.index.is_some() && !route.rest.is_empty() => {
                if !registry.writable() {
                    http::respond_with(
                        stream,
                        403,
                        "{\"error\":\"узел открыт только для чтения\"}",
                        keep,
                    );
                    return true;
                }
                let index = route.index.as_deref().unwrap_or_default().to_string();
                let alias = route.rest.clone();
                let is_write = if req.method == "PUT" && !req.body.is_empty() {
                    let value = match serde_json::from_slice::<serde_json::Value>(&req.body) {
                        Ok(value) => value,
                        Err(error) => {
                            return respond_catalog_error(
                                stream,
                                &catalog::CatalogError::InvalidParameter {
                                    key: "body".to_string(),
                                    why: error.to_string(),
                                },
                                keep,
                            )
                        }
                    };
                    match value.get("is_write_index") {
                        None => None,
                        Some(flag) => match flag.as_bool() {
                            Some(flag) => Some(flag),
                            None => {
                                return respond_catalog_error(
                                    stream,
                                    &catalog::CatalogError::InvalidParameter {
                                        key: "is_write_index".to_string(),
                                        why: "нужен bool".to_string(),
                                    },
                                    keep,
                                )
                            }
                        },
                    }
                } else {
                    None
                };
                let action = if req.method == "PUT" {
                    multi::AliasAction::Add { index, alias, is_write }
                } else {
                    multi::AliasAction::Remove { index, alias, must_exist: true }
                };
                return match registry.aliases(&[action]) {
                    Ok(catalog) => {
                        http::respond_with(
                            stream,
                            200,
                            &serde_json::json!({
                                "acknowledged": true,
                                "generation": catalog.generation(),
                            })
                            .to_string(),
                            keep,
                        );
                        true
                    }
                    Err(error) => respond_catalog_error(stream, &error, keep),
                };
            }
            _ => {
                http::respond_with(
                    stream,
                    405,
                    "{\"error\":\"метод alias API не поддерживается\"}",
                    keep,
                );
                return true;
            }
        }
    }
    if route.endpoint == route::Endpoint::CatIndices {
        let tag = query_value(&route.query, "tag");
        let rows: Vec<serde_json::Value> = registry
            .list()
            .into_iter()
            .filter(|info| {
                tag.as_ref().is_none_or(|tag| info.tags.iter().any(|candidate| candidate == tag))
            })
            .map(|info| {
                serde_json::json!({
                    "health": "green",
                    "status": info.state.name(),
                    "index": info.name,
                    "uuid": info.uuid,
                    "docs.count": info.docs,
                    "docs.deleted": info.deleted,
                    "segments.count": info.segments,
                    "store.size_in_bytes": info.bytes,
                    "writable": info.writable,
                    "blocks": info.blocks.names(),
                    "description": info.description,
                    "tags": info.tags,
                    "aliases": info.aliases,
                    "write_aliases": info.write_aliases,
                })
            })
            .collect();
        http::respond_with(
            stream,
            200,
            &serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()),
            keep,
        );
        return true;
    }
    if route.endpoint == route::Endpoint::Health && route.index.is_none() {
        let nodes = registry.all_nodes();
        let catalog = registry.catalog();
        let total_indices =
            catalog.entries().iter().filter(|entry| entry.state.is_settled()).count();
        let closed_indices =
            catalog.entries().iter().filter(|entry| entry.state == catalog::State::Closed).count();
        let mut docs = 0usize;
        let mut segments = 0usize;
        let mut unhealthy = Vec::new();
        for (name, node) in &nodes {
            let segs = node.live.read().expect("набор сегментов").clone();
            let deleted: usize = segs.list.iter().map(|segment| segment.dead()).sum();
            docs = docs.saturating_add(segs.docs().saturating_sub(deleted));
            segments = segments.saturating_add(reported_segment_count(&segs));
            if node.reloads.failures() > 0
                || node.writer.as_ref().is_some_and(|writer| writer.healthy().is_err())
            {
                unhealthy.push(name.clone());
            }
        }
        let ok = unhealthy.is_empty();
        let payload = serde_json::json!({
            "ok": ok,
            "mode": "multi_index",
            "indices": total_indices,
            "open_indices": nodes.len(),
            "closed_indices": closed_indices,
            "docs": docs,
            "segments": segments,
            "unhealthy_indices": unhealthy,
        });
        http::respond_with(stream, if ok { 200 } else { 503 }, &payload.to_string(), keep);
        return true;
    }
    if route.endpoint == route::Endpoint::Stats && route.index.is_none() {
        let rows = registry.list();
        let docs: usize = rows.iter().filter_map(|info| info.docs).sum();
        let deleted: usize = rows.iter().filter_map(|info| info.deleted).sum();
        let segments: usize = rows.iter().filter_map(|info| info.segments).sum();
        let bytes: u64 = rows.iter().map(|info| info.bytes).sum();
        let indices = rows
            .into_iter()
            .map(|info| {
                (
                    info.name,
                    serde_json::json!({
                        "docs": info.docs,
                        "deleted": info.deleted,
                        "segments": info.segments,
                        "index_bytes": info.bytes,
                        "writable": info.writable,
                        "state": info.state.name(),
                        "blocks": info.blocks.names(),
                        "description": info.description,
                        "tags": info.tags,
                        "aliases": info.aliases,
                        "write_aliases": info.write_aliases,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let payload = serde_json::json!({
            "version": host::version(),
            "revision": host::revision(),
            "build_target": host::target(),
            "source_date_epoch": host::build_epoch(),
            "mode": "multi_index",
            "uptime_seconds": host::uptime_seconds(),
            "totals": {
                "indices": indices.len(),
                "docs": docs,
                "deleted": deleted,
                "segments": segments,
                "index_bytes": bytes,
            },
            "indices": indices,
        });
        http::respond_with(stream, 200, &payload.to_string(), keep);
        return true;
    }
    if route.endpoint == route::Endpoint::Metadata {
        let Some(name) = route.index.as_deref() else { return false };
        if req.method == "GET" {
            let catalog = registry.catalog();
            let Some(entry) = catalog.by_name(name) else {
                return respond_catalog_error(
                    stream,
                    &catalog::CatalogError::IndexNotFound { name: name.to_string() },
                    keep,
                );
            };
            let payload = serde_json::json!({
                "index": entry.name,
                "uuid": if entry.uuid.is_nil() { String::new() } else { entry.uuid.text() },
                "description": entry.description,
                "tags": entry.tags,
                "created_ms": entry.created_ms,
            });
            http::respond_with(stream, 200, &payload.to_string(), keep);
            return true;
        }
        if req.method != "PUT" {
            http::respond_with(stream, 405, "{\"error\":\"метод не поддерживается\"}", keep);
            return true;
        }
        if !registry.writable() {
            http::respond_with(stream, 403, "{\"error\":\"узел открыт только для чтения\"}", keep);
            return true;
        }
        let (description, tags) = match parse_index_metadata(&req.body) {
            Ok(value) => value,
            Err(error) => return respond_catalog_error(stream, &error, keep),
        };
        return match registry.update_metadata(name, description, tags) {
            Ok(catalog) => {
                http::respond_with(
                    stream,
                    200,
                    &serde_json::json!({
                        "acknowledged": true,
                        "generation": catalog.generation(),
                    })
                    .to_string(),
                    keep,
                );
                true
            }
            Err(error) => respond_catalog_error(stream, &error, keep),
        };
    }
    if route.endpoint == route::Endpoint::Blocks {
        let Some(name) = route.index.as_deref() else { return false };
        if req.method == "GET" {
            let catalog = registry.catalog();
            let Some(entry) = catalog.by_name(name) else {
                return respond_catalog_error(
                    stream,
                    &catalog::CatalogError::IndexNotFound { name: name.to_string() },
                    keep,
                );
            };
            http::respond_with(
                stream,
                200,
                &serde_json::json!({
                    "index": entry.name,
                    "blocks": entry.blocks.names(),
                })
                .to_string(),
                keep,
            );
            return true;
        }
        if !matches!(req.method.as_str(), "PUT" | "DELETE") || route.rest.is_empty() {
            http::respond_with(stream, 405, "{\"error\":\"метод не поддерживается\"}", keep);
            return true;
        }
        if !registry.writable() {
            http::respond_with(stream, 403, "{\"error\":\"узел открыт только для чтения\"}", keep);
            return true;
        }
        return match registry.change_block(name, &route.rest, req.method == "PUT") {
            Ok(catalog) => {
                let entry = catalog.by_name(name).expect("индекс остался в каталоге");
                http::respond_with(
                    stream,
                    200,
                    &serde_json::json!({
                        "acknowledged": true,
                        "blocks": entry.blocks.names(),
                        "generation": catalog.generation(),
                    })
                    .to_string(),
                    keep,
                );
                true
            }
            Err(error) => respond_catalog_error(stream, &error, keep),
        };
    }
    if matches!(route.endpoint, route::Endpoint::Open | route::Endpoint::Close) {
        let Some(name) = route.index.as_deref() else { return false };
        if !registry.writable() {
            http::respond_with(stream, 403, "{\"error\":\"узел открыт только для чтения\"}", keep);
            return true;
        }
        let changed = if route.endpoint == route::Endpoint::Open {
            registry.open_index(name)
        } else {
            registry.close(name)
        };
        return match changed {
            Ok(catalog) => {
                http::respond_with(
                    stream,
                    200,
                    &serde_json::json!({
                        "acknowledged": true,
                        "index": name,
                        "state": catalog.by_name(name).map(|entry| entry.state.name()),
                        "generation": catalog.generation(),
                    })
                    .to_string(),
                    keep,
                );
                true
            }
            Err(error) => respond_catalog_error(stream, &error, keep),
        };
    }
    if route.endpoint == route::Endpoint::Rollover {
        let Some(alias) = route.index.as_deref() else { return false };
        if !registry.writable() {
            http::respond_with(stream, 403, "{\"error\":\"узел открыт только для чтения\"}", keep);
            return true;
        }
        let request = match parse_rollover_request(
            &req.body,
            (!route.rest.is_empty()).then(|| route.rest.clone()),
        ) {
            Ok(request) => request,
            Err(error) => return respond_catalog_error(stream, &error, keep),
        };
        return match registry.rollover(alias, request) {
            Ok(outcome) => {
                let conditions: serde_json::Map<String, serde_json::Value> = outcome
                    .conditions
                    .into_iter()
                    .map(|(name, met)| (name, serde_json::Value::Bool(met)))
                    .collect();
                http::respond_with(
                    stream,
                    200,
                    &serde_json::json!({
                        "acknowledged": outcome.rolled_over,
                        "old_index": outcome.old_index,
                        "new_index": outcome.new_index,
                        "rolled_over": outcome.rolled_over,
                        "dry_run": outcome.dry_run,
                        "conditions": conditions,
                        "generation": outcome.generation,
                    })
                    .to_string(),
                    keep,
                );
                true
            }
            Err(error) => respond_catalog_error(stream, &error, keep),
        };
    }
    if route.endpoint != route::Endpoint::Index {
        return false;
    }
    let Some(name) = route.index.as_deref() else { return false };
    if let Err(error) = multi::valid_name(name) {
        http::respond_with(stream, 400, &format!("{{\"error\":{}}}", json_string(&error)), keep);
        return true;
    }
    match req.method.as_str() {
        "HEAD" => http::respond_empty(stream, if registry.exists(name) { 200 } else { 404 }, keep),
        "GET" => match registry.list().into_iter().find(|info| info.name == name) {
            Some(info) => http::respond_with(
                stream,
                200,
                &serde_json::json!({
                    "index": info.name,
                    "docs": info.docs,
                    "deleted": info.deleted,
                    "segments": info.segments,
                    "bytes": info.bytes,
                    "writable": info.writable,
                    "state": info.state.name(),
                    "blocks": info.blocks.names(),
                    "description": info.description,
                    "tags": info.tags,
                    "aliases": info.aliases,
                    "write_aliases": info.write_aliases,
                })
                .to_string(),
                keep,
            ),
            None => http::respond_with(
                stream,
                404,
                &format!(
                    "{{\"error\":{},\"type\":\"index_not_found_exception\"}}",
                    json_string(&format!("индекс {name:?} не существует"))
                ),
                keep,
            ),
        },
        "PUT" => {
            if !registry.writable() {
                http::respond_with(
                    stream,
                    403,
                    "{\"error\":\"узел открыт только для чтения\"}",
                    keep,
                );
                return true;
            }
            let (decls, settings) =
                match parse_index_creation(&req.body, registry.fallback_fields()) {
                    Ok(spec) => spec,
                    Err(error) => {
                        http::respond_with(
                            stream,
                            400,
                            &format!("{{\"error\":{}}}", json_string(&error)),
                            keep,
                        );
                        return true;
                    }
                };
            let mapping = serde_json::from_slice::<serde_json::Value>(&req.body)
                .ok()
                .filter(|value| value.get("mappings").is_some())
                .map(|_| req.body.as_slice());
            let create = match parse_create_options(&req.body) {
                Ok(create) => create,
                Err(error) => return respond_catalog_error(stream, &error, keep),
            };
            match registry.create_with_mapping(name, &decls, &settings, mapping, create) {
                Ok(_) => http::respond_with(
                    stream,
                    200,
                    &format!("{{\"acknowledged\":true,\"index\":{}}}", json_string(name)),
                    keep,
                ),
                // Код берётся у самой ошибки, а не угадывается по подстроке в
                // её русском тексте: прежняя лестница переставала работать от
                // любой переформулировки, и отказ её был невидим — клиент
                // просто получал 500 вместо 409.
                Err(error) => {
                    http::respond_with(stream, error.status(), &error.body(), keep);
                }
            }
        }
        "DELETE" => {
            if !registry.writable() {
                http::respond_with(
                    stream,
                    403,
                    "{\"error\":\"узел открыт только для чтения\"}",
                    keep,
                );
                return true;
            }
            match registry.delete(name) {
                Ok(()) => http::respond_with(
                    stream,
                    200,
                    &format!("{{\"acknowledged\":true,\"index\":{}}}", json_string(name)),
                    keep,
                ),
                Err(error) => http::respond_with(stream, error.status(), &error.body(), keep),
            }
        }
        _ => http::respond_with(stream, 405, "{\"error\":\"метод не поддерживается\"}", keep),
    }
    true
}

fn respond_catalog_error(
    stream: &mut wire::Wire,
    error: &catalog::CatalogError,
    keep: bool,
) -> bool {
    http::respond_with(stream, error.status(), &error.body(), keep);
    true
}

fn query_value(query: &str, wanted: &str) -> Option<String> {
    let raw = query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == wanted).then_some(value)
    })?;
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0usize;
    while at < bytes.len() {
        if bytes[at] == b'+' {
            out.push(b' ');
            at += 1;
            continue;
        }
        if bytes[at] == b'%' && at + 2 < bytes.len() {
            let hex = |byte: u8| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            };
            if let (Some(hi), Some(lo)) = (hex(bytes[at + 1]), hex(bytes[at + 2])) {
                out.push(hi << 4 | lo);
                at += 3;
                continue;
            }
        }
        out.push(bytes[at]);
        at += 1;
    }
    String::from_utf8(out).ok()
}

fn query_bool(query: &str, name: &str, default: bool) -> Result<bool, String> {
    match query_value(query, name).as_deref() {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(value) => Err(format!("параметр {name}={value:?}: ожидались true или false")),
    }
}

type MetadataPatch = (Option<Option<String>>, Option<Vec<String>>);

fn parse_index_metadata(body: &[u8]) -> Result<MetadataPatch, catalog::CatalogError> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| catalog::CatalogError::InvalidParameter {
            key: "body".to_string(),
            why: format!("не разобралось как JSON: {error}"),
        })?;
    let object = value.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
        key: "body".to_string(),
        why: "metadata должна быть объектом".to_string(),
    })?;
    for key in object.keys() {
        if !matches!(key.as_str(), "description" | "tags") {
            return Err(catalog::CatalogError::InvalidParameter {
                key: key.clone(),
                why: "ожидались description и tags".to_string(),
            });
        }
    }
    let description = match object.get("description") {
        None => None,
        Some(serde_json::Value::Null) => Some(None),
        Some(serde_json::Value::String(value)) => Some(Some(value.clone())),
        Some(_) => {
            return Err(catalog::CatalogError::InvalidParameter {
                key: "description".to_string(),
                why: "нужна строка либо null".to_string(),
            })
        }
    };
    let tags = match object.get("tags") {
        None => None,
        Some(serde_json::Value::Array(values)) => Some(
            values
                .iter()
                .map(|value| {
                    value.as_str().map(str::to_string).ok_or_else(|| {
                        catalog::CatalogError::InvalidParameter {
                            key: "tags".to_string(),
                            why: "каждый тег обязан быть строкой".to_string(),
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(_) => {
            return Err(catalog::CatalogError::InvalidParameter {
                key: "tags".to_string(),
                why: "нужен массив строк".to_string(),
            })
        }
    };
    Ok((description, tags))
}

fn parse_create_options(body: &[u8]) -> Result<multi::CreateOptions, catalog::CatalogError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(multi::CreateOptions::default());
    }
    let value: serde_json::Value = serde_json::from_slice(body).map_err(|error| {
        catalog::CatalogError::InvalidParameter { key: "body".to_string(), why: error.to_string() }
    })?;
    let object = value.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
        key: "body".to_string(),
        why: "создание индекса должно быть объектом".to_string(),
    })?;
    let (description, tags) = match object.get("metadata") {
        None => (None, None),
        Some(metadata) => {
            parse_index_metadata(&serde_json::to_vec(metadata).map_err(|error| {
                catalog::CatalogError::InvalidParameter {
                    key: "metadata".to_string(),
                    why: error.to_string(),
                }
            })?)?
        }
    };
    let mut aliases = Vec::new();
    if let Some(raw) = object.get("aliases") {
        let raw = raw.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
            key: "aliases".to_string(),
            why: "нужен объект alias -> параметры".to_string(),
        })?;
        for (alias, spec) in raw {
            let spec = spec.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
                key: format!("aliases.{alias}"),
                why: "параметры alias должны быть объектом".to_string(),
            })?;
            for unsupported in ["filter", "routing", "search_routing", "index_routing"] {
                if spec.contains_key(unsupported) {
                    return Err(catalog::CatalogError::NotImplementedParameter {
                        key: format!("aliases.{alias}.{unsupported}"),
                    });
                }
            }
            if let Some(key) = spec.keys().find(|key| key.as_str() != "is_write_index") {
                return Err(catalog::CatalogError::InvalidParameter {
                    key: format!("aliases.{alias}.{key}"),
                    why: "поддерживается только is_write_index".to_string(),
                });
            }
            let is_write = spec
                .get("is_write_index")
                .map(|value| {
                    value.as_bool().ok_or_else(|| catalog::CatalogError::InvalidParameter {
                        key: format!("aliases.{alias}.is_write_index"),
                        why: "нужен bool".to_string(),
                    })
                })
                .transpose()?;
            aliases.push((alias.clone(), is_write));
        }
    }
    Ok(multi::CreateOptions {
        description: description.flatten(),
        tags: tags.unwrap_or_default(),
        aliases,
    })
}

fn parse_rollover_request(
    body: &[u8],
    new_index: Option<String>,
) -> Result<multi::RolloverRequest, catalog::CatalogError> {
    let value = if body.iter().all(u8::is_ascii_whitespace) {
        serde_json::json!({})
    } else {
        serde_json::from_slice(body).map_err(|error| catalog::CatalogError::InvalidParameter {
            key: "body".to_string(),
            why: format!("не разобралось как JSON: {error}"),
        })?
    };
    let object = value.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
        key: "body".to_string(),
        why: "rollover должен быть объектом".to_string(),
    })?;
    for key in object.keys() {
        if !matches!(key.as_str(), "conditions" | "dry_run" | "settings" | "metadata") {
            return Err(if key == "mappings" {
                catalog::CatalogError::NotImplementedParameter { key: key.clone() }
            } else {
                catalog::CatalogError::InvalidParameter {
                    key: key.clone(),
                    why: "ожидались conditions, dry_run, settings и metadata".to_string(),
                }
            });
        }
    }
    let conditions = object
        .get("conditions")
        .map(|value| {
            value.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
                key: "conditions".to_string(),
                why: "нужен объект".to_string(),
            })
        })
        .transpose()?;
    if let Some(conditions) = conditions {
        if let Some(key) = conditions
            .keys()
            .find(|key| !matches!(key.as_str(), "max_docs" | "max_age" | "max_size"))
        {
            return Err(catalog::CatalogError::InvalidParameter {
                key: format!("conditions.{key}"),
                why: "поддерживаются max_docs, max_age и max_size".to_string(),
            });
        }
    }
    let max_docs = conditions
        .and_then(|conditions| conditions.get("max_docs"))
        .map(|value| {
            value.as_u64().ok_or_else(|| catalog::CatalogError::InvalidParameter {
                key: "conditions.max_docs".to_string(),
                why: "нужно неотрицательное целое".to_string(),
            })
        })
        .transpose()?;
    let max_age_ms = conditions
        .and_then(|conditions| conditions.get("max_age"))
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| catalog::CatalogError::InvalidParameter {
                    key: "conditions.max_age".to_string(),
                    why: "нужна строка вроде 30d или 12h".to_string(),
                })
                .and_then(|raw| {
                    parse_scaled_u64(
                        raw,
                        "ms",
                        &[("s", 1_000), ("m", 60_000), ("h", 3_600_000), ("d", 86_400_000)],
                    )
                })
        })
        .transpose()?;
    let max_size = conditions
        .and_then(|conditions| conditions.get("max_size"))
        .map(|value| {
            if let Some(number) = value.as_u64() {
                return Ok(number);
            }
            value
                .as_str()
                .ok_or_else(|| catalog::CatalogError::InvalidParameter {
                    key: "conditions.max_size".to_string(),
                    why: "нужно число байт либо строка вроде 500gb".to_string(),
                })
                .and_then(|raw| {
                    parse_scaled_u64(
                        raw,
                        "b",
                        &[("kb", 1 << 10), ("mb", 1 << 20), ("gb", 1 << 30), ("tb", 1u64 << 40)],
                    )
                })
        })
        .transpose()?;
    let dry_run = match object.get("dry_run") {
        None => false,
        Some(value) => value.as_bool().ok_or_else(|| catalog::CatalogError::InvalidParameter {
            key: "dry_run".to_string(),
            why: "нужен bool".to_string(),
        })?,
    };
    let settings_patch = object
        .get("settings")
        .map(|value| {
            if !value.is_object() {
                return Err(catalog::CatalogError::InvalidParameter {
                    key: "settings".to_string(),
                    why: "нужен объект".to_string(),
                });
            }
            serde_json::to_vec(value).map_err(|error| catalog::CatalogError::InvalidParameter {
                key: "settings".to_string(),
                why: error.to_string(),
            })
        })
        .transpose()?;
    let (description, tags) = match object.get("metadata") {
        None => (None, None),
        Some(value) => parse_index_metadata(&serde_json::to_vec(value).map_err(|error| {
            catalog::CatalogError::InvalidParameter {
                key: "metadata".to_string(),
                why: error.to_string(),
            }
        })?)?,
    };
    Ok(multi::RolloverRequest {
        new_index,
        max_docs,
        max_age_ms,
        max_size,
        dry_run,
        settings_patch,
        description,
        tags,
    })
}

fn parse_scaled_u64(
    raw: &str,
    bare_suffix: &str,
    units: &[(&str, u64)],
) -> Result<u64, catalog::CatalogError> {
    let lower = raw.trim().to_ascii_lowercase();
    let (number, factor) = units
        .iter()
        .copied()
        .chain(std::iter::once((bare_suffix, 1)))
        .filter_map(|(suffix, factor)| {
            lower.strip_suffix(suffix).map(|number| (suffix.len(), number, factor))
        })
        .max_by_key(|(len, _, _)| *len)
        .map(|(_, number, factor)| (number, factor))
        .ok_or_else(|| catalog::CatalogError::InvalidParameter {
            key: "condition".to_string(),
            why: format!("неизвестная единица в {raw:?}"),
        })?;
    let number =
        number.trim().parse::<u64>().map_err(|_| catalog::CatalogError::InvalidParameter {
            key: "condition".to_string(),
            why: format!("неверное число в {raw:?}"),
        })?;
    number.checked_mul(factor).ok_or_else(|| catalog::CatalogError::LimitExceeded {
        what: format!("условие {raw:?}"),
        have: usize::MAX,
        limit: usize::MAX,
    })
}

fn parse_alias_actions(body: &[u8]) -> Result<Vec<multi::AliasAction>, catalog::CatalogError> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| catalog::CatalogError::InvalidParameter {
            key: "body".to_string(),
            why: format!("не разобралось как JSON: {error}"),
        })?;
    let root = value.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
        key: "body".to_string(),
        why: "тело _aliases должно быть объектом".to_string(),
    })?;
    if root.keys().any(|key| key != "actions") {
        return Err(catalog::CatalogError::InvalidParameter {
            key: "body".to_string(),
            why: "на верхнем уровне ожидается только actions".to_string(),
        });
    }
    let rows = root.get("actions").and_then(serde_json::Value::as_array).ok_or_else(|| {
        catalog::CatalogError::InvalidParameter {
            key: "actions".to_string(),
            why: "нужен массив".to_string(),
        }
    })?;
    let mut out = Vec::new();
    for (at, row) in rows.iter().enumerate() {
        let action = row.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
            key: format!("actions[{at}]"),
            why: "действие должно быть объектом".to_string(),
        })?;
        if action.len() != 1 {
            return Err(catalog::CatalogError::InvalidParameter {
                key: format!("actions[{at}]"),
                why: "нужно ровно одно из add, remove, remove_index".to_string(),
            });
        }
        let (kind, spec) = action.iter().next().expect("один элемент");
        let spec = spec.as_object().ok_or_else(|| catalog::CatalogError::InvalidParameter {
            key: format!("actions[{at}].{kind}"),
            why: "параметры должны быть объектом".to_string(),
        })?;
        for unsupported in ["filter", "routing", "search_routing", "index_routing"] {
            if spec.contains_key(unsupported) {
                return Err(catalog::CatalogError::NotImplementedParameter {
                    key: unsupported.to_string(),
                });
            }
        }
        let allowed: &[&str] = match kind.as_str() {
            "add" => &["index", "indices", "alias", "is_write_index"],
            "remove" => &["index", "indices", "alias", "must_exist"],
            "remove_index" => &["index", "indices"],
            _ => {
                return Err(catalog::CatalogError::InvalidParameter {
                    key: kind.clone(),
                    why: "ожидались add, remove или remove_index".to_string(),
                })
            }
        };
        if let Some(key) = spec.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(catalog::CatalogError::InvalidParameter {
                key: key.clone(),
                why: format!("ключ не поддерживается у {kind}"),
            });
        }
        let mut indices = Vec::new();
        if let Some(index) = spec.get("index") {
            indices.push(
                index
                    .as_str()
                    .ok_or_else(|| catalog::CatalogError::InvalidParameter {
                        key: "index".to_string(),
                        why: "нужна строка".to_string(),
                    })?
                    .to_string(),
            );
        }
        if let Some(values) = spec.get("indices") {
            let values =
                values.as_array().ok_or_else(|| catalog::CatalogError::InvalidParameter {
                    key: "indices".to_string(),
                    why: "нужен массив строк".to_string(),
                })?;
            for value in values {
                indices.push(
                    value
                        .as_str()
                        .ok_or_else(|| catalog::CatalogError::InvalidParameter {
                            key: "indices".to_string(),
                            why: "каждый индекс обязан быть строкой".to_string(),
                        })?
                        .to_string(),
                );
            }
        }
        if indices.is_empty() {
            return Err(catalog::CatalogError::InvalidParameter {
                key: "index".to_string(),
                why: "нужен index либо indices".to_string(),
            });
        }
        let alias = || -> Result<String, catalog::CatalogError> {
            spec.get("alias").and_then(serde_json::Value::as_str).map(str::to_string).ok_or_else(
                || catalog::CatalogError::InvalidParameter {
                    key: "alias".to_string(),
                    why: "нужна строка".to_string(),
                },
            )
        };
        match kind.as_str() {
            "add" => {
                let alias = alias()?;
                let is_write = spec
                    .get("is_write_index")
                    .map(|value| {
                        value.as_bool().ok_or_else(|| catalog::CatalogError::InvalidParameter {
                            key: "is_write_index".to_string(),
                            why: "нужен bool".to_string(),
                        })
                    })
                    .transpose()?;
                out.extend(indices.into_iter().map(|index| multi::AliasAction::Add {
                    index,
                    alias: alias.clone(),
                    is_write,
                }));
            }
            "remove" => {
                let alias = alias()?;
                let must_exist = spec
                    .get("must_exist")
                    .map(|value| {
                        value.as_bool().ok_or_else(|| catalog::CatalogError::InvalidParameter {
                            key: "must_exist".to_string(),
                            why: "нужен bool".to_string(),
                        })
                    })
                    .transpose()?
                    .unwrap_or(false);
                out.extend(indices.into_iter().map(|index| multi::AliasAction::Remove {
                    index,
                    alias: alias.clone(),
                    must_exist,
                }));
            }
            "remove_index" => out
                .extend(indices.into_iter().map(|index| multi::AliasAction::RemoveIndex { index })),
            _ => unreachable!(),
        }
    }
    Ok(out)
}

/// Настройка TLS из аргументов запуска. `None` — открытым текстом.
///
/// ## Почему отказ, а не тихий откат
///
/// Узел, собранный без признака `tls`, но запущенный с `--tls-cert=`, обязан
/// **не подняться**. Молча слушать открытым текстом там, где хозяин попросил
/// шифрование, — худшее из возможного: снаружи всё выглядит работающим, а
/// пароли идут по проводу как есть, и узнать об этом можно только перехватом.
fn tls_from(args: &[String], _cfg: &config::Config) -> Option<wire::Acceptor> {
    let flag = |name: &str| args.iter().find_map(|a| a.strip_prefix(name)).map(str::to_string);
    let cert = flag("--tls-cert=");
    let key = flag("--tls-key=");
    match (cert, key) {
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            eprintln!("TLS: нужны оба — --tls-cert= и --tls-key=");
            std::process::exit(2);
        }
        #[cfg(feature = "tls")]
        (Some(cert), Some(key)) => match wire::load(&cert, &key) {
            Ok(a) => {
                println!("TLS включён: {cert}");
                Some(a)
            }
            Err(e) => {
                eprintln!("TLS: {e}");
                std::process::exit(2);
            }
        },
        #[cfg(not(feature = "tls"))]
        (Some(_), Some(_)) => {
            eprintln!(
                "TLS: узел собран без него. Пересоберите с \
                 `cargo build --release --features tls` — либо снимите --tls-cert/--tls-key \
                 и поставьте перед узлом проксировщик."
            );
            std::process::exit(2);
        }
    }
}

/// Сколько ядер у машины. Ноль от системы — берём восемь как разумную середину.
///
/// Считается один раз. Это не преждевременная бережливость: величина спрашивается
/// **на каждый запрос**, чтобы поделить ядра между запросами, а
/// `available_parallelism` под этим делает системный вызов. Шесть тысяч запросов
/// в секунду — шесть тысяч лишних вызовов.
fn cores() -> usize {
    static CORES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CORES.get_or_init(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
}

/// Решение admission по долгу L0.
///
/// До мягкой границы приём не платит ничего. Между границами задержка растёт,
/// но ограничена ста миллисекундами, чтобы один запрос не зависал за долгом.
/// На жёсткой границе запрос обязан повториться позже: продолжать создавать
/// сегменты быстрее compaction означало бы лишь отложить неизбежный collapse.
fn l0_admission(debt: usize, soft: usize, hard: usize) -> Result<Duration, ()> {
    if debt >= hard {
        return Err(());
    }
    if debt <= soft {
        return Ok(Duration::ZERO);
    }
    let millis = debt.saturating_sub(soft).saturating_mul(4).min(100) as u64;
    Ok(Duration::from_millis(millis))
}

/// Долг, созданный после запуска процесса поверх наименьшего уже достигнутого.
///
/// Большой импортированный индекс может начать с тысяч сегментов. Запретить
/// ему всякую запись до многодневной compaction означало бы сделать upgrade
/// невозможным. Унаследованный уровень grandfathered, но как только compaction
/// опускает минимум, он опускается навсегда: обратный рост снова проходит через
/// обычные soft/hard границы.
fn l0_growth(debt: usize, floor: usize) -> (usize, usize) {
    let baseline = floor.min(debt);
    (baseline, debt.saturating_sub(baseline))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
enum ResourceClass {
    Control = 0,
    Point = 1,
    Interactive = 2,
    Analytics = 3,
    Maintenance = 4,
    Write = 5,
    Compaction = 6,
}

impl ResourceClass {
    const ALL: [ResourceClass; 7] = [
        ResourceClass::Control,
        ResourceClass::Point,
        ResourceClass::Interactive,
        ResourceClass::Analytics,
        ResourceClass::Maintenance,
        ResourceClass::Write,
        ResourceClass::Compaction,
    ];

    fn name(self) -> &'static str {
        match self {
            ResourceClass::Control => "control",
            ResourceClass::Point => "point_read",
            ResourceClass::Interactive => "interactive_search",
            ResourceClass::Analytics => "analytics",
            ResourceClass::Maintenance => "maintenance_scan",
            ResourceClass::Write => "write",
            ResourceClass::Compaction => "compaction",
        }
    }

    /// Максимум ожидающих работ каждого класса.
    ///
    /// Ожидание — это состояние continuation, а не повод создавать новый
    /// системный поток. Лимит защищает память самого scheduler и делает
    /// перегрузку наблюдаемой: лишняя работа получает retryable отказ ещё до
    /// попадания в очередь.
    fn queue_limit(self) -> usize {
        match self {
            ResourceClass::Control => 32,
            ResourceClass::Point => 128,
            ResourceClass::Interactive => 64,
            ResourceClass::Analytics => 16,
            ResourceClass::Maintenance => 8,
            ResourceClass::Write => 64,
            ResourceClass::Compaction => 1,
        }
    }
}

/// Фазы единой работы data-plane запроса.
///
/// Числовое представление хранится атомарно, чтобы административный снимок
/// мог прочитать состояние без блокировки самого запроса. Переходы не меняют
/// семантику ответа: это наблюдаемая граница владения, а не второй планировщик.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum WorkPhase {
    Received = 0,
    Planned = 1,
    Queued = 2,
    Running = 3,
    Yielded = 4,
    Responding = 5,
    Finished = 6,
}

/// Причина завершения работы, отличимая от текущей фазы.
///
/// `Guard::Reason` сообщает причину отмены запроса, но не различает успешно
/// завершённую работу, bounded отказ admission и остановку узла. Это поле
/// остаётся внутри runtime-контекста: наружу попадает только безопасное имя
/// причины, без текста запроса или идентификаторов пользователя.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum WorkReason {
    Running = 0,
    Completed = 1,
    Cancelled = 2,
    Deadline = 3,
    Rejected = 4,
    Shutdown = 5,
    Error = 6,
}

impl WorkReason {
    fn name(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Deadline => "deadline",
            Self::Rejected => "rejected",
            Self::Shutdown => "shutdown",
            Self::Error => "error",
        }
    }
}

/// Контекст, общий для admission, исполнения и завершения одного запроса.
///
/// Раньше guard, query registry и resource permit создавались раздельно, и
/// отказ между ними было трудно связать с конкретной работой. Теперь один
/// контекст несёт монотонный correlation id, общий deadline/cancellation,
/// класс, фазу и требование полноты. Дочерние функции всё ещё принимают
/// `&Guard` для обратной совместимости с core API, но получают тот же объект.
struct WorkContext {
    id: u64,
    started: Instant,
    guard: Arc<svindb_core::work::Guard>,
    class: AtomicU8,
    phase: AtomicU8,
    require_complete: bool,
    /// Зарезервированная верхняя граница памяти запроса. Это значение уже
    /// прошло через общий `query_pool`; его повторно не складывают с RSS или
    /// file cache, но сохраняют для diagnostics и cleanup-проверок.
    reserved_bytes: AtomicU64,
    /// Фактически материализованные source/projection/response bytes,
    /// наблюдённые после начала исполнения. Значение может быть меньше
    /// резерва и не является вторым лимитом allocator.
    materialized_bytes: AtomicU64,
    cpu_units: AtomicUsize,
    io_units: AtomicUsize,
    reason: AtomicU8,
}

static NEXT_WORK_ID: AtomicU64 = AtomicU64::new(0);

impl WorkContext {
    fn new(
        class: ResourceClass,
        guard: Arc<svindb_core::work::Guard>,
        require_complete: bool,
    ) -> Arc<Self> {
        let id = NEXT_WORK_ID.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        Arc::new(Self {
            id,
            started: Instant::now(),
            guard,
            class: AtomicU8::new(class as u8),
            phase: AtomicU8::new(WorkPhase::Received as u8),
            require_complete,
            reserved_bytes: AtomicU64::new(0),
            materialized_bytes: AtomicU64::new(0),
            cpu_units: AtomicUsize::new(0),
            io_units: AtomicUsize::new(0),
            reason: AtomicU8::new(WorkReason::Running as u8),
        })
    }

    fn set_class(&self, class: ResourceClass) {
        self.class.store(class as u8, Ordering::Release);
    }

    fn set_phase(&self, phase: WorkPhase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    fn guard(&self) -> &svindb_core::work::Guard {
        &self.guard
    }

    fn phase(&self) -> WorkPhase {
        match self.phase.load(Ordering::Acquire) {
            0 => WorkPhase::Received,
            1 => WorkPhase::Planned,
            2 => WorkPhase::Queued,
            3 => WorkPhase::Running,
            4 => WorkPhase::Yielded,
            5 => WorkPhase::Responding,
            _ => WorkPhase::Finished,
        }
    }

    fn class(&self) -> ResourceClass {
        match self.class.load(Ordering::Acquire) {
            0 => ResourceClass::Control,
            1 => ResourceClass::Point,
            2 => ResourceClass::Interactive,
            3 => ResourceClass::Analytics,
            4 => ResourceClass::Maintenance,
            5 => ResourceClass::Write,
            _ => ResourceClass::Compaction,
        }
    }

    fn requires_complete(&self) -> bool {
        self.require_complete
    }

    fn reserve_memory(&self, bytes: usize) {
        self.reserved_bytes.store(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Release);
    }

    fn record_materialized(&self, bytes: usize) {
        self.materialized_bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::AcqRel);
    }

    fn set_units(&self, cpu: usize, io: usize) {
        self.cpu_units.store(cpu, Ordering::Release);
        self.io_units.store(io, Ordering::Release);
    }

    fn finish(&self, reason: WorkReason) {
        self.reason.store(reason as u8, Ordering::Release);
        self.phase.store(WorkPhase::Finished as u8, Ordering::Release);
    }

    fn reason(&self) -> WorkReason {
        match self.reason.load(Ordering::Acquire) {
            1 => WorkReason::Completed,
            2 => WorkReason::Cancelled,
            3 => WorkReason::Deadline,
            4 => WorkReason::Rejected,
            5 => WorkReason::Shutdown,
            6 => WorkReason::Error,
            _ => WorkReason::Running,
        }
    }

    fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes.load(Ordering::Acquire)
    }

    fn materialized_bytes(&self) -> u64 {
        self.materialized_bytes.load(Ordering::Acquire)
    }

    fn cpu_units(&self) -> usize {
        self.cpu_units.load(Ordering::Acquire)
    }

    fn io_units(&self) -> usize {
        self.io_units.load(Ordering::Acquire)
    }
}

impl Drop for WorkContext {
    fn drop(&mut self) {
        if self.reason.load(Ordering::Acquire) == WorkReason::Running as u8 {
            let reason = match self.guard.reason() {
                svindb_core::work::Reason::Cancelled => WorkReason::Cancelled,
                svindb_core::work::Reason::Deadline => WorkReason::Deadline,
                svindb_core::work::Reason::Running => WorkReason::Completed,
            };
            self.reason.store(reason as u8, Ordering::Release);
        }
        self.phase.store(WorkPhase::Finished as u8, Ordering::Release);
    }
}

struct ResourceLane {
    active: AtomicUsize,
    rejected: AtomicU64,
    waited_micros: AtomicU64,
    max: usize,
    io_units: usize,
}

struct Resources {
    lanes: [ResourceLane; 7],
    io_used: AtomicUsize,
    io_max: usize,
    cache_gate: &'static RwLock<()>,
    cpu_used: AtomicUsize,
    cpu_max: usize,
    interactive_max_units: usize,
    analytics_max_units: usize,
    latency_reserve_bytes: u64,
    wal_replay_bytes: usize,
    max_frozen_memtables: usize,
    background_io_bytes_per_second: u64,
    background_io_burst_bytes: u64,
    background_io_max_inflight: usize,
    source_cache_bytes: usize,
    /// Ожидающие data-plane работы. Это отдельный счётчик, а не часть active:
    /// analytics уступает интерактивному поиску ещё до того, как тот получил
    /// permit, иначе очередь могла бы оставаться невидимой для governor.
    waiters: [AtomicUsize; 7],
    /// Событие изменения доступности ресурса. Ожидающий запрос не держит CPU
    /// permit и не просыпается периодическим busy-sleep: владелец permit
    /// увеличивает поколение и будит continuation после освобождения.
    wake: Condvar,
    wake_generation: Mutex<u64>,
}

const MAX_RESOURCE_WAIT: Duration = Duration::from_millis(250);
const RESOURCE_POLL: Duration = Duration::from_millis(2);

/// Начальный консервативный предел полного обхода в interactive lane.
const INTERACTIVE_COUNT_DOCUMENTS: u64 = 10_000;

fn search_resource_class(
    spec: &QuerySpec,
    aggs: &[svindb_core::aggs::Agg],
    documents: Option<u64>,
) -> ResourceClass {
    if spec.k > 1_000 {
        return ResourceClass::Analytics;
    }
    if aggs.is_empty() {
        return ResourceClass::Interactive;
    }
    // Только document count: scalar field stats могут раскрывать большие
    // массивы, поэтому небольшой corpus сам по себе не ограничивает их цену.
    let count = aggs.iter().all(|a| {
        matches!(a.kind, svindb_core::aggs::Kind::Stats { field: None })
            && a.over == svindb_core::aggs::Over::Docs
    });
    let simple = spec.nested.is_none()
        && spec.phrases.is_empty()
        && !spec.translit
        && spec.words.len() + spec.must.len() + spec.must_not.len() <= 32
        && spec
            .words
            .iter()
            .chain(&spec.must)
            .chain(&spec.must_not)
            .all(|word| word.terms.iter().all(|term| !term.prefix));
    // Одно SpecWord может содержать много альтернатив. Число узлов не
    // ограничивает число dictionary probes и стоимость проверки документа.
    let terms = spec
        .words
        .iter()
        .chain(&spec.must)
        .chain(&spec.must_not)
        .try_fold(0usize, |total, word| total.checked_add(word.terms.len()));
    if count
        && simple
        && terms.is_some_and(|n| n <= 32)
        && documents.is_some_and(|n| n <= INTERACTIVE_COUNT_DOCUMENTS)
    {
        ResourceClass::Interactive
    } else {
        ResourceClass::Analytics
    }
}

/// Общий token bucket для writes при cgroup I/O stall.
struct IoWritePacer {
    next: Mutex<Instant>,
}

impl IoWritePacer {
    fn enter(&self, guard: Option<&svindb_core::work::Guard>) -> bool {
        const QUANTUM: Duration = Duration::from_millis(20);
        const MAX_WAIT: Duration = Duration::from_millis(250);
        let now = Instant::now();
        let mut next = self.next.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let slot = (*next).max(now);
        let wait = slot.saturating_duration_since(now);
        if wait > MAX_WAIT {
            return false;
        }
        *next = slot + QUANTUM;
        drop(next);
        let started = Instant::now();
        while started.elapsed() < wait {
            if guard.is_some_and(|guard| !guard.check()) {
                return false;
            }
            let left = wait.saturating_sub(started.elapsed()).min(Duration::from_millis(10));
            if left.is_zero() {
                break;
            }
            std::thread::park_timeout(left);
        }
        true
    }
}

fn io_write_pacer() -> &'static IoWritePacer {
    static PACER: std::sync::OnceLock<IoWritePacer> = std::sync::OnceLock::new();
    PACER.get_or_init(|| IoWritePacer { next: Mutex::new(Instant::now()) })
}

/// Состояние коротких maintenance-квантов при непрерывном поиске.
///
/// Полная остановка compaction на busy-узле оставила бы L0 debt без шанса
/// догнать его. Первые две секунды разрешаются 50 мс работы на 500 мс,
/// затем при непрерывном query demand — 125 мс на 500 мс. Это ограничивает
/// уступку dashboards, не оставляя merge постоянно на 10% CPU. Свежие признаки
/// тяжёлого давления имеют старшинство над любой квантой.
struct CompactionPauseState {
    grant_until: Instant,
    next_grant: Instant,
    busy_since: Option<Instant>,
}

impl CompactionPauseState {
    fn wait_at(&mut self, now: Instant, query_busy: bool, hard_pressure: bool) -> Option<Duration> {
        if query_busy {
            self.busy_since.get_or_insert(now);
        } else {
            self.busy_since = None;
            self.grant_until = now;
        }
        if hard_pressure {
            self.grant_until = now;
            return Some(Duration::from_millis(10));
        }
        if !query_busy || now < self.grant_until {
            return None;
        }
        if now >= self.next_grant {
            let sustained = self.busy_since.is_some_and(|since| {
                now.saturating_duration_since(since) >= Duration::from_secs(2)
            });
            self.grant_until = now + Duration::from_millis(if sustained { 125 } else { 50 });
            self.next_grant = now + Duration::from_millis(500);
            return None;
        }
        Some(self.next_grant.saturating_duration_since(now).min(Duration::from_millis(10)))
    }
}

#[derive(Clone, Copy, Default)]
struct CompactionHookSnapshot {
    checkpoints: u64,
    pauses: u64,
    paused_ms: u64,
    preemptions: u64,
    paced_bytes: u64,
    paced_waited_us: u64,
}

impl CompactionHookSnapshot {
    fn delta(self, before: Self) -> Self {
        Self {
            checkpoints: self.checkpoints.saturating_sub(before.checkpoints),
            pauses: self.pauses.saturating_sub(before.pauses),
            paused_ms: self.paused_ms.saturating_sub(before.paused_ms),
            preemptions: self.preemptions.saturating_sub(before.preemptions),
            paced_bytes: self.paced_bytes.saturating_sub(before.paced_bytes),
            paced_waited_us: self.paced_waited_us.saturating_sub(before.paced_waited_us),
        }
    }
}

struct QueryFirstCompactionHook {
    state: Mutex<CompactionPauseState>,
    io_state: Mutex<IoPaceState>,
    io_rate_bytes_per_second: u64,
    io_burst_bytes: u64,
    checkpoints: AtomicU64,
    pauses: AtomicU64,
    paused_ms: AtomicU64,
    preemptions: AtomicU64,
    paced_bytes: AtomicU64,
    paced_waited_us: AtomicU64,
}

struct IoPaceState {
    available_bytes: u64,
    last: Instant,
}

impl QueryFirstCompactionHook {
    fn new(io_rate_mbps: u64, io_burst_mb: u64) -> Self {
        let now = Instant::now();
        let rate = io_rate_mbps.saturating_mul(1 << 20);
        let burst = io_burst_mb.saturating_mul(1 << 20).max(rate / 4);
        Self {
            state: Mutex::new(CompactionPauseState {
                grant_until: now,
                next_grant: now,
                busy_since: None,
            }),
            io_state: Mutex::new(IoPaceState { available_bytes: burst, last: now }),
            io_rate_bytes_per_second: rate,
            io_burst_bytes: burst,
            checkpoints: AtomicU64::new(0),
            pauses: AtomicU64::new(0),
            paused_ms: AtomicU64::new(0),
            preemptions: AtomicU64::new(0),
            paced_bytes: AtomicU64::new(0),
            paced_waited_us: AtomicU64::new(0),
        }
    }

    fn query_busy() -> bool {
        resources().query_demand()
    }

    fn hard_pressure(sample: Option<pressure::Sample>, now: Instant) -> bool {
        sample.is_some_and(|sample| {
            now.saturating_duration_since(sample.at) <= Duration::from_secs(1)
                && (sample.pressure.io_severely_overloaded()
                    || (sample.pressure.cgroup_limited && sample.pressure.overloaded()))
        })
    }

    fn pace(&self, logical_bytes: u64) {
        let rate = self.io_rate_bytes_per_second;
        if rate == 0 || logical_bytes == 0 {
            return;
        }
        let now = Instant::now();
        let mut state = self.io_state.lock().unwrap_or_else(|e| e.into_inner());
        let elapsed = now.saturating_duration_since(state.last);
        let refill = ((elapsed.as_nanos().saturating_mul(u128::from(rate))) / 1_000_000_000)
            .min(u128::from(u64::MAX)) as u64;
        state.available_bytes =
            state.available_bytes.saturating_add(refill).min(self.io_burst_bytes);
        state.last = now;
        let granted = state.available_bytes.min(logical_bytes);
        state.available_bytes -= granted;
        let remaining = logical_bytes.saturating_sub(granted);
        let wait_us = if remaining == 0 {
            0
        } else {
            u128::from(remaining).saturating_mul(1_000_000).div_ceil(u128::from(rate)).min(250_000)
                as u64
        };
        let wait = Duration::from_micros(wait_us);
        if wait_us > 0 {
            // Время ожидания уже зарезервировано этим вызовом. Следующая
            // порция не получит тот же burst повторно, пока timestamp не
            // догонит момент завершения сна.
            state.last = now + wait;
            state.available_bytes = 0;
        }
        drop(state);
        self.paced_bytes.fetch_add(logical_bytes, Ordering::Relaxed);
        if !wait.is_zero() {
            self.pauses.fetch_add(1, Ordering::Relaxed);
            // Не оставляем merge в одном непрерываемом sleep до 250 мс:
            // query demand и SIGTERM должны быть замечены между короткими
            // квантами. Уже зарезервированный токен остаётся списанным, поэтому
            // досрочный выход только усиливает backoff, но не создаёт burst.
            let started = Instant::now();
            while started.elapsed() < wait {
                if self.should_stop() || Self::query_busy() {
                    break;
                }
                let left = wait.saturating_sub(started.elapsed()).min(Duration::from_millis(10));
                if left.is_zero() {
                    break;
                }
                std::thread::park_timeout(left);
            }
            self.paced_waited_us
                .fetch_add(u64::try_from(wait.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
        }
    }

    fn should_stop(&self) -> bool {
        SHUTDOWN_REQUESTED.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> CompactionHookSnapshot {
        CompactionHookSnapshot {
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
            pauses: self.pauses.load(Ordering::Relaxed),
            paused_ms: self.paused_ms.load(Ordering::Relaxed),
            preemptions: self.preemptions.load(Ordering::Relaxed),
            paced_bytes: self.paced_bytes.load(Ordering::Relaxed),
            paced_waited_us: self.paced_waited_us.load(Ordering::Relaxed),
        }
    }
}

impl svindb_core::work::CompactionHook for QueryFirstCompactionHook {
    fn checkpoint(&self, logical_bytes: u64) {
        if self.should_stop() {
            return;
        }
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
        self.pace(logical_bytes);
        if pressure::background_stalled() {
            self.pauses.fetch_add(1, Ordering::Relaxed);
            let started = Instant::now();
            while started.elapsed() < Duration::from_millis(25) {
                if self.should_stop() {
                    break;
                }
                let left = Duration::from_millis(25)
                    .saturating_sub(started.elapsed())
                    .min(Duration::from_millis(5));
                if left.is_zero() {
                    break;
                }
                std::thread::park_timeout(left);
            }
            self.paused_ms.fetch_add(
                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        let started = Instant::now();
        let mut yielded = false;
        loop {
            if self.should_stop() {
                break;
            }
            let query_busy = Self::query_busy();
            let now = Instant::now();
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let wait = state.wait_at(now, query_busy, Self::hard_pressure(pressure::latest(), now));
            drop(state);
            let Some(wait) = wait else { break };
            if !yielded && query_busy {
                self.preemptions.fetch_add(1, Ordering::Relaxed);
                yielded = true;
            }
            self.pauses.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(wait.max(Duration::from_millis(1)));
        }
        self.paused_ms.fetch_add(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn should_stop(&self) -> bool {
        SHUTDOWN_REQUESTED.load(Ordering::Acquire)
    }
}

/// Статистика hook, сгруппированная по каталогу индекса.
///
/// Сам hook общий для процесса: он не знает, какой `Writer` вызвал тик. Job
/// снимает два атомарных snapshot до/после своей работы и складывает дельту в
/// эту таблицу. Поэтому process-wide counters не маскируются под точный индекс,
/// а overlap явно остаётся в residual process I/O.
#[derive(Clone, Copy, Default)]
struct CompactionIndexStats {
    jobs: u64,
    active_ms: u64,
    paused_ms: u64,
    query_preemptions: u64,
    checkpoints: u64,
    paced_bytes: u64,
    paced_waited_us: u64,
}

static COMPACTION_INDEX_STATS: std::sync::OnceLock<Mutex<HashMap<String, CompactionIndexStats>>> =
    std::sync::OnceLock::new();
static WORKLOAD_HOOK: std::sync::OnceLock<Arc<QueryFirstCompactionHook>> =
    std::sync::OnceLock::new();

fn compaction_stats_table() -> &'static Mutex<HashMap<String, CompactionIndexStats>> {
    COMPACTION_INDEX_STATS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn hook_snapshot() -> CompactionHookSnapshot {
    WORKLOAD_HOOK.get().map_or_else(CompactionHookSnapshot::default, |hook| hook.snapshot())
}

fn record_compaction_job(dir: &str, active: Duration, hook: CompactionHookSnapshot) {
    let mut table = compaction_stats_table().lock().unwrap_or_else(|e| e.into_inner());
    // Каталог обычно содержит десятки индексов, но тестовые/ephemeral roots
    // могут создаваться часто. Ограничиваем диагностическую таблицу, чтобы
    // метрики не превратились в долгоживущую утечку по именам каталогов.
    if !table.contains_key(dir) && table.len() >= 512 {
        if let Some(oldest) = table.keys().next().cloned() {
            table.remove(&oldest);
        }
    }
    let entry = table.entry(dir.to_string()).or_default();
    entry.jobs = entry.jobs.saturating_add(1);
    entry.active_ms =
        entry.active_ms.saturating_add(u64::try_from(active.as_millis()).unwrap_or(u64::MAX));
    entry.paused_ms = entry.paused_ms.saturating_add(hook.paused_ms);
    entry.query_preemptions = entry.query_preemptions.saturating_add(hook.preemptions);
    entry.checkpoints = entry.checkpoints.saturating_add(hook.checkpoints);
    entry.paced_bytes = entry.paced_bytes.saturating_add(hook.paced_bytes);
    entry.paced_waited_us = entry.paced_waited_us.saturating_add(hook.paced_waited_us);
}

fn compaction_stats(dir: &str) -> CompactionIndexStats {
    compaction_stats_table()
        .lock()
        .ok()
        .and_then(|table| table.get(dir).copied())
        .unwrap_or_default()
}

struct ResourcePermit {
    resources: &'static Resources,
    class: ResourceClass,
    io_units: usize,
    cpu_units: usize,
    /// Пока пользовательский read жив, compaction не вправе выталкивать его
    /// source mappings из page cache.
    _cache_read: Option<std::sync::RwLockReadGuard<'static, ()>>,
}

impl ResourcePermit {
    fn cpu_units(&self) -> usize {
        self.cpu_units
    }

    fn io_units(&self) -> usize {
        self.io_units
    }
}

fn query_cache_gate() -> &'static RwLock<()> {
    static GATE: std::sync::OnceLock<RwLock<()>> = std::sync::OnceLock::new();
    GATE.get_or_init(|| RwLock::new(()))
}

impl Resources {
    fn query_demand(&self) -> bool {
        [ResourceClass::Point, ResourceClass::Interactive].iter().any(|&class| {
            self.active(class) > 0 || self.waiters[class as usize].load(Ordering::Acquire) > 0
        }) || self.active(ResourceClass::Analytics) > 0
    }
    #[cfg(test)]
    fn new(width: usize) -> Resources {
        Self::new_with(width, &config::Workload::default())
    }

    fn new_with(width: usize, workload: &config::Workload) -> Resources {
        let width = width.max(1);
        let configured_search = if workload.search_cpu_units == 0 {
            width.saturating_sub(4).max(1)
        } else {
            workload.search_cpu_units
        };
        let cpu_max = configured_search.min(width.saturating_sub(1).max(1));
        let specs = [
            (8, 0),
            // Point GET/MGET имеют собственную дешёвую очередь и обязаны
            // выдерживать acceptance concurrency 32 даже на 16 CPU. Каталог
            // ID разделяется между ReadView, поэтому удвоение CPU здесь не
            // означает 32 одновременных обхода всех сегментов.
            (width.saturating_mul(4).clamp(64, 256), 1),
            // Число работ не подменяет вычислительный бюджет. Прежний
            // width/8 сериализовал analytics на узлах с 8–15 CPU даже при
            // свободных workers: c4 увеличивало очередь, а не throughput.
            // Каждый запрос по-прежнему атомарно резервирует CPU/I/O и память;
            // жёсткие ceilings ограничивают число одновременно живых планов.
            (cpu_max.min(8), 2),
            (cpu_max.min(4), 4),
            (2, 4),
            // WAL group commit требует очереди шире числа CPU: конкурентные
            // записи складываются в один fsync, а прежний предел width/4
            // отвергал 20 из 24 законных запросов ещё до существующего
            // `max_pending_writes`. I/O unit один: физическую запись всё равно
            // сериализует group commit; backpressure остаётся у bounded write
            // queue и L0 hard limit.
            (width.saturating_mul(4).clamp(32, 256), 1),
            (1, width.max(4)),
        ];
        Resources {
            lanes: std::array::from_fn(|at| ResourceLane {
                active: AtomicUsize::new(0),
                rejected: AtomicU64::new(0),
                waited_micros: AtomicU64::new(0),
                max: specs[at].0,
                io_units: specs[at].1,
            }),
            io_used: AtomicUsize::new(0),
            io_max: width.saturating_mul(4).max(8),
            cache_gate: query_cache_gate(),
            cpu_used: AtomicUsize::new(0),
            cpu_max,
            interactive_max_units: workload.interactive_max_units.min(configured_search).max(1),
            analytics_max_units: workload.analytics_max_units.min(configured_search).max(1),
            latency_reserve_bytes: workload.latency_reserve_mb.saturating_mul(1 << 20),
            wal_replay_bytes: workload.wal_replay_mb.saturating_mul(1 << 20).min(usize::MAX as u64)
                as usize,
            max_frozen_memtables: workload.max_frozen_memtables,
            background_io_bytes_per_second: workload.background_io_mbps.saturating_mul(1 << 20),
            background_io_burst_bytes: workload.background_io_burst_mb.saturating_mul(1 << 20),
            background_io_max_inflight: workload.background_io_max_inflight,
            source_cache_bytes: workload
                .source_cache_mb
                .saturating_mul(1 << 20)
                .min(usize::MAX as u64) as usize,
            waiters: std::array::from_fn(|_| AtomicUsize::new(0)),
            wake: Condvar::new(),
            wake_generation: Mutex::new(0),
        }
    }

    fn enter(&'static self, class: ResourceClass) -> Option<ResourcePermit> {
        self.enter_now(class, true, usize::MAX)
    }

    /// Пытается взять permit без побочных отказов в метриках.
    ///
    /// Очередь вызывает этот путь много раз за время ожидания; считать каждую
    /// неудачную пробу отказом означало бы превратить одну перегрузку в сотни
    /// ложных rejection-событий. Публичный для сервера `enter` по-прежнему
    /// фиксирует немедленный отказ.
    fn enter_now(
        &'static self,
        class: ResourceClass,
        count_rejection: bool,
        requested_units: usize,
    ) -> Option<ResourcePermit> {
        let lane = &self.lanes[class as usize];
        let mut active = lane.active.load(Ordering::Acquire);
        loop {
            if active >= lane.max {
                if count_rejection {
                    lane.rejected.fetch_add(1, Ordering::Relaxed);
                }
                return None;
            }
            match lane.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(now) => active = now,
            }
        }

        let mut used = self.io_used.load(Ordering::Acquire);
        loop {
            let next = used.saturating_add(lane.io_units);
            if next > self.io_max {
                lane.active.fetch_sub(1, Ordering::AcqRel);
                if count_rejection {
                    lane.rejected.fetch_add(1, Ordering::Relaxed);
                }
                return None;
            }
            match self.io_used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(now) => used = now,
            }
        }
        let cache_read = if matches!(
            class,
            ResourceClass::Point
                | ResourceClass::Interactive
                | ResourceClass::Analytics
                | ResourceClass::Write
        ) {
            // Cache relief может выполнять kernel I/O под exclusive guard.
            // Блокирующий read() здесь обходил request deadline и удерживал
            // уже выданные lane/I/O permits на неопределённое время.
            match self.cache_gate.try_read() {
                Ok(guard) => Some(guard),
                Err(std::sync::TryLockError::Poisoned(error)) => Some(error.into_inner()),
                Err(std::sync::TryLockError::WouldBlock) => {
                    self.io_used.fetch_sub(lane.io_units, Ordering::AcqRel);
                    lane.active.fetch_sub(1, Ordering::AcqRel);
                    if count_rejection {
                        lane.rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    return None;
                }
            }
        } else {
            None
        };
        // Ширина уже начатого запроса не отзывается. Новый запрос получает
        // только ещё не выданные units, а не новую долю тех же самых CPU.
        // Резерв живёт до окончания aggregation/hydration/serialization.
        let cap = match class {
            ResourceClass::Interactive => self.interactive_max_units,
            ResourceClass::Analytics => self.analytics_max_units,
            _ => 0,
        };
        let desired = cap.min(requested_units.max(1));
        let mut cpu_used = self.cpu_used.load(Ordering::Acquire);
        let cpu_units = loop {
            let available = self.cpu_max.saturating_sub(cpu_used);
            if desired > 0 && available == 0 {
                self.io_used.fetch_sub(lane.io_units, Ordering::AcqRel);
                lane.active.fetch_sub(1, Ordering::AcqRel);
                if count_rejection {
                    lane.rejected.fetch_add(1, Ordering::Relaxed);
                }
                return None;
            }
            let granted = desired.min(available);
            if granted == 0 {
                break 0;
            }
            match self.cpu_used.compare_exchange_weak(
                cpu_used,
                cpu_used + granted,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break granted,
                Err(current) => cpu_used = current,
            }
        };
        Some(ResourcePermit {
            resources: self,
            class,
            io_units: lane.io_units,
            cpu_units,
            _cache_read: cache_read,
        })
    }

    /// Берёт permit с коротким deadline-aware ожиданием.
    ///
    /// Ожидание ограничено 250 мс даже у бессрочного запроса: очередь должна
    /// быть bounded и возвращать клиенту retryable 429 до того, как его
    /// внешний timeout превратится в необъяснимую задержку. Interactive имеет
    /// приоритет над analytics: пока интерактивная работа ожидает место, новые
    /// analytics не обходят её в `enter_now`.
    fn enter_until(
        &'static self,
        class: ResourceClass,
        guard: Option<&svindb_core::work::Guard>,
    ) -> Option<ResourcePermit> {
        self.enter_until_with_units(class, guard, usize::MAX)
    }

    fn enter_until_with_units(
        &'static self,
        class: ResourceClass,
        guard: Option<&svindb_core::work::Guard>,
        requested_units: usize,
    ) -> Option<ResourcePermit> {
        // Быстрый путь не должен занимать место в очереди: в обычном режиме
        // запрос получает permit без регистрации continuation. Исключение —
        // analytics при уже ожидающем interactive: приоритет проверяется до
        // попытки выдачи, иначе свободные analytics CPU позволили бы обойти
        // поставленный в очередь пользовательский поиск.
        let interactive_waiting = class == ResourceClass::Analytics
            && self.waiters[ResourceClass::Interactive as usize].load(Ordering::Acquire) > 0;
        if !interactive_waiting {
            if let Some(permit) = self.enter_now(class, false, requested_units) {
                return Some(permit);
            }
        }

        let waiter = &self.waiters[class as usize];
        let limit = class.queue_limit();
        let mut queued = waiter.load(Ordering::Acquire);
        loop {
            if queued >= limit {
                self.lanes[class as usize].rejected.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            match waiter.compare_exchange_weak(
                queued,
                queued + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => queued = actual,
            }
        }
        let started = Instant::now();
        let result = loop {
            if guard.is_some_and(|g| !g.check()) {
                break None;
            }
            if class == ResourceClass::Analytics
                && self.waiters[ResourceClass::Interactive as usize].load(Ordering::Acquire) > 0
            {
                if started.elapsed() >= MAX_RESOURCE_WAIT {
                    break None;
                }
                self.wait_for_change(RESOURCE_POLL);
                continue;
            }
            if let Some(permit) = self.enter_now(class, false, requested_units) {
                break Some(permit);
            }
            if started.elapsed() >= MAX_RESOURCE_WAIT {
                break None;
            }
            let remaining = MAX_RESOURCE_WAIT.saturating_sub(started.elapsed());
            let request_remaining =
                guard.and_then(svindb_core::work::Guard::remaining).unwrap_or(remaining);
            let wait = remaining.min(request_remaining).min(RESOURCE_POLL);
            if wait.is_zero() {
                break None;
            }
            self.wait_for_change(wait);
        };
        waiter.fetch_sub(1, Ordering::AcqRel);
        self.lanes[class as usize].waited_micros.fetch_add(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if result.is_none() {
            self.lanes[class as usize].rejected.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Ждёт освобождения любого ресурса или короткого таймера deadline.
    ///
    /// Поколение читается до попытки сна, поэтому уведомление между неудачной
    /// попыткой `enter_now` и входом в condvar не теряется. Таймер остаётся
    /// обязательным: при отмене запроса никто не обязан освобождать ресурс.
    fn wait_for_change(&self, timeout: Duration) {
        let mut generation = self.wake_generation.lock().unwrap_or_else(|e| e.into_inner());
        let seen = *generation;
        let (next, _) =
            self.wake.wait_timeout(generation, timeout).unwrap_or_else(|e| e.into_inner());
        generation = next;
        if *generation == seen {
            // Таймер уже проверил deadline в следующей итерации. Явное чтение
            // не даёт оптимизатору превратить ожидание в пустой блок.
            std::hint::black_box(*generation);
        }
    }

    fn notify_waiters(&self) {
        if self.waiters.iter().all(|waiters| waiters.load(Ordering::Acquire) == 0) {
            return;
        }
        let mut generation = self.wake_generation.lock().unwrap_or_else(|e| e.into_inner());
        *generation = generation.wrapping_add(1);
        self.wake.notify_all();
    }

    fn active(&self, class: ResourceClass) -> usize {
        self.lanes[class as usize].active.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> Vec<(&'static str, u64, u64, u64, u64, u64)> {
        ResourceClass::ALL
            .iter()
            .map(|&class| {
                let lane = &self.lanes[class as usize];
                (
                    class.name(),
                    lane.active.load(Ordering::Relaxed) as u64,
                    lane.max as u64,
                    lane.rejected.load(Ordering::Relaxed),
                    self.waiters[class as usize].load(Ordering::Relaxed) as u64,
                    lane.waited_micros.load(Ordering::Relaxed) / 1_000,
                )
            })
            .collect()
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        let lane = &self.resources.lanes[self.class as usize];
        if self.io_units > 0 {
            self.resources.io_used.fetch_sub(self.io_units, Ordering::AcqRel);
        }
        if self.cpu_units > 0 {
            self.resources.cpu_used.fetch_sub(self.cpu_units, Ordering::AcqRel);
        }
        lane.active.fetch_sub(1, Ordering::AcqRel);
        self.resources.notify_waiters();
    }
}

fn resources() -> &'static Resources {
    resources_with(&config::Workload::default())
}

fn resources_with(workload: &config::Workload) -> &'static Resources {
    static RESOURCES: std::sync::OnceLock<Resources> = std::sync::OnceLock::new();
    RESOURCES.get_or_init(|| Resources::new_with(cores(), workload))
}

/// Чтения, чья согласованность обеспечивается собственным versioned view.
///
/// Их нельзя дополнительно ставить за общий visibility barrier: это возвращает
/// старую взаимную блокировку refresh/mutation с data-plane и уничтожает смысл
/// короткого снимка внутри соответствующего исполнителя.
fn uses_versioned_read(endpoint: route::Endpoint) -> bool {
    matches!(endpoint, route::Endpoint::Search | route::Endpoint::DocGet | route::Endpoint::MGet)
}

/// Внутренняя ширина одного MGET с учётом уже активных point requests.
///
/// Permit ограничивает число запросов, но сам по себе не ограничивает потоки
/// `Fetcher::many`: при c32 каждый MGET раньше получал все 16 настроенных
/// потоков. Деление сохраняет параллелизм одиночного набора и не создаёт сотни
/// hydration workers, когда параллелизм уже обеспечен снаружи.
fn point_read_share(active: usize, configured: usize) -> usize {
    (configured.max(1) / active.max(1)).clamp(1, configured.clamp(1, 8))
}

fn install_workload_hooks(workload: &config::Workload) {
    let hook = WORKLOAD_HOOK.get_or_init(|| {
        Arc::new(QueryFirstCompactionHook::new(
            workload.background_io_mbps,
            workload.background_io_burst_mb,
        ))
    });
    let erased: Arc<dyn svindb_core::work::CompactionHook> = hook.clone();
    let _ = svindb_core::work::install_compaction_hook(erased);
}

struct ActiveQuery {
    id: u64,
    started: Instant,
    index: String,
    class: &'static str,
    context: Arc<WorkContext>,
}

struct QueryRegistry {
    active: Mutex<Vec<ActiveQuery>>,
}

struct QueryLease {
    registry: &'static QueryRegistry,
    id: u64,
    context: Option<Arc<WorkContext>>,
}

impl QueryRegistry {
    fn enter_context(
        &'static self,
        context: &Arc<WorkContext>,
        index: &str,
        class: ResourceClass,
    ) -> QueryLease {
        context.set_class(class);
        context.set_phase(WorkPhase::Running);
        self.active.lock().unwrap_or_else(|e| e.into_inner()).push(ActiveQuery {
            id: context.id,
            started: context.started,
            index: index.to_string(),
            class: context.class().name(),
            context: Arc::clone(context),
        });
        QueryLease { registry: self, id: context.id, context: Some(Arc::clone(context)) }
    }

    fn oldest(&self) -> Option<(u64, f64, String, &'static str)> {
        let active = self.active.lock().ok()?;
        let query = active.iter().min_by_key(|query| query.started)?;
        Some((query.id, query.started.elapsed().as_secs_f64(), query.index.clone(), query.class))
    }

    /// Полный безопасный снимок старшей работы для admin diagnostics.
    ///
    /// В ответ не попадает тело запроса: оператору достаточно понять, какой
    /// класс удерживает очередь, сколько он зарезервировал и на какой фазе
    /// находится. `Arc<WorkContext>` здесь нужен только до копирования
    /// атомарных полей; сам registry не удерживается во время исполнения.
    fn oldest_detail(&self) -> Option<WorkDiagnostic> {
        let active = self.active.lock().ok()?;
        let query = active.iter().min_by_key(|query| query.started)?;
        let context = &query.context;
        Some(WorkDiagnostic {
            id: query.id,
            age_ms: u64::try_from(query.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            index: query.index.clone(),
            class: query.class,
            phase: context.phase(),
            reason: context.reason(),
            reserved_bytes: context.reserved_bytes(),
            materialized_bytes: context.materialized_bytes(),
            cpu_units: context.cpu_units(),
            io_units: context.io_units(),
        })
    }
}

#[derive(Clone)]
struct WorkDiagnostic {
    id: u64,
    age_ms: u64,
    index: String,
    class: &'static str,
    phase: WorkPhase,
    reason: WorkReason,
    reserved_bytes: u64,
    materialized_bytes: u64,
    cpu_units: usize,
    io_units: usize,
}

fn work_phase_name(phase: WorkPhase) -> &'static str {
    match phase {
        WorkPhase::Received => "received",
        WorkPhase::Planned => "planned",
        WorkPhase::Queued => "queued",
        WorkPhase::Running => "running",
        WorkPhase::Yielded => "yielded",
        WorkPhase::Responding => "responding",
        WorkPhase::Finished => "finished",
    }
}

fn oldest_query_json() -> String {
    let Some(work) = query_registry().oldest_detail() else {
        return "null".to_string();
    };
    format!(
        "{{\"id\":{},\"age_ms\":{},\"index\":{},\"class\":{},\"phase\":{},\
         \"reason\":{},\"reserved_bytes\":{},\"materialized_bytes\":{},\
         \"cpu_units\":{},\"io_units\":{}}}",
        work.id,
        work.age_ms,
        json_string(&work.index),
        json_string(work.class),
        json_string(work_phase_name(work.phase)),
        json_string(work.reason.name()),
        work.reserved_bytes,
        work.materialized_bytes,
        work.cpu_units,
        work.io_units,
    )
}

impl Drop for QueryLease {
    fn drop(&mut self) {
        if let Some(context) = &self.context {
            let reason = match context.guard.reason() {
                svindb_core::work::Reason::Cancelled => WorkReason::Cancelled,
                svindb_core::work::Reason::Deadline => WorkReason::Deadline,
                svindb_core::work::Reason::Running => WorkReason::Completed,
            };
            context.finish(reason);
        }
        let mut active = self.registry.active.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(at) = active.iter().position(|query| query.id == self.id) {
            active.swap_remove(at);
        }
    }
}

fn query_registry() -> &'static QueryRegistry {
    static REGISTRY: std::sync::OnceLock<QueryRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| QueryRegistry { active: Mutex::new(Vec::new()) })
}

#[cfg(test)]
mod work_context_tests {
    use super::*;

    #[test]
    fn context_keeps_one_resource_snapshot_until_cleanup() {
        let guard = Arc::new(svindb_core::work::Guard::new(5_000));
        let context = WorkContext::new(ResourceClass::Interactive, guard, true);
        context.reserve_memory(12_345);
        context.record_materialized(678);
        context.set_units(8, 1);
        context.set_phase(WorkPhase::Running);

        assert_eq!(context.reserved_bytes(), 12_345);
        assert_eq!(context.materialized_bytes(), 678);
        assert_eq!(context.cpu_units(), 8);
        assert_eq!(context.io_units(), 1);
        assert_eq!(context.reason(), WorkReason::Running);

        context.finish(WorkReason::Completed);
        assert_eq!(context.phase(), WorkPhase::Finished);
        assert_eq!(context.reason(), WorkReason::Completed);
    }

    #[test]
    fn cancelled_guard_is_preserved_as_context_finish_reason() {
        let guard = Arc::new(svindb_core::work::Guard::new(0));
        let context = WorkContext::new(ResourceClass::Point, Arc::clone(&guard), true);
        guard.cancel();
        drop(context);
        assert_eq!(guard.reason(), svindb_core::work::Reason::Cancelled);
    }
}

/// Число одновременно закреплённых PIT одного процесса.
const PIT_MAX_ACTIVE: usize = 16;
/// Общая куча frozen live bitmaps. Отображения сегментов сюда не входят: они
/// уже учтены resident/cgroup admission и делятся через `Arc`.
const PIT_MAX_BITMAP_BYTES: usize = 512 << 20;
const PIT_MAX_TTL_MS: u64 = 120_000;

type FrozenLive = Arc<Vec<Arc<svindb_core::live::Snapshot>>>;
type FrozenReadView = (Arc<Segments>, FrozenLive);

struct PitSnapshot {
    id: String,
    index: String,
    segments: Arc<Segments>,
    live: FrozenLive,
    bytes: usize,
    expires: Instant,
}

#[derive(Default)]
struct PitRegistry {
    entries: Mutex<Vec<PitSnapshot>>,
}

impl PitRegistry {
    fn prune(entries: &mut Vec<PitSnapshot>) {
        let now = Instant::now();
        entries.retain(|entry| entry.expires > now);
    }

    fn token() -> Result<String, String> {
        let bytes = svindb_core::hash::try_entropy(24)
            .map_err(|error| format!("PIT id не получен от ядра: {error}"))?;
        Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect::<Vec<_>>().join(""))
    }

    fn reap_after(&'static self, id: String, ttl_ms: u64) {
        std::thread::Builder::new()
            .name("svindb-pit-expiry".into())
            .spawn(move || {
                std::thread::sleep(Duration::from_millis(ttl_ms));
                let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
                let now = Instant::now();
                entries.retain(|entry| entry.id != id || entry.expires > now);
            })
            .ok();
    }

    fn insert(
        &'static self,
        index: &str,
        segments: Arc<Segments>,
        live: FrozenLive,
        ttl_ms: u64,
    ) -> Result<String, String> {
        let bytes = live.iter().map(|snapshot| snapshot.bytes()).sum::<usize>();
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune(&mut entries);
        if entries.len() >= PIT_MAX_ACTIVE {
            return Err(format!(
                "активных PIT {} при пределе {PIT_MAX_ACTIVE}; закройте/дождитесь старого",
                entries.len()
            ));
        }
        let held = entries.iter().map(|entry| entry.bytes).sum::<usize>();
        if held.saturating_add(bytes) > PIT_MAX_BITMAP_BYTES {
            return Err(format!(
                "PIT live bitmaps просят {} байт поверх удерживаемых {held} при пределе {PIT_MAX_BITMAP_BYTES}",
                bytes
            ));
        }
        let mut id = Self::token()?;
        while entries.iter().any(|entry| entry.id == id) {
            id = Self::token()?;
        }
        entries.push(PitSnapshot {
            id: id.clone(),
            index: index.to_string(),
            segments,
            live,
            bytes,
            expires: Instant::now() + Duration::from_millis(ttl_ms),
        });
        drop(entries);
        self.reap_after(id.clone(), ttl_ms);
        Ok(id)
    }

    fn get(&'static self, id: &str, index: &str, ttl_ms: u64) -> Option<FrozenReadView> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune(&mut entries);
        let entry = entries.iter_mut().find(|entry| entry.id == id && entry.index == index)?;
        entry.expires = Instant::now() + Duration::from_millis(ttl_ms);
        let result = (Arc::clone(&entry.segments), Arc::clone(&entry.live));
        drop(entries);
        self.reap_after(id.to_string(), ttl_ms);
        Some(result)
    }

    fn state(&self) -> (usize, usize) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune(&mut entries);
        (entries.len(), entries.iter().map(|entry| entry.bytes).sum())
    }
}

fn pit_registry() -> &'static PitRegistry {
    static REGISTRY: std::sync::OnceLock<PitRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(PitRegistry::default)
}

struct PitAsk {
    id: Option<String>,
    ttl_ms: u64,
}

fn parse_pit(value: Option<&serde_json::Value>) -> Result<Option<PitAsk>, String> {
    let Some(value) = value else { return Ok(None) };
    let (id, ttl_ms) = match value {
        serde_json::Value::String(id) => (Some(id.clone()), 60_000),
        serde_json::Value::Object(object) => {
            if let Some(key) =
                object.keys().find(|key| !matches!(key.as_str(), "id" | "keep_alive_ms"))
            {
                return Err(format!("pit.{key}: неизвестный параметр"));
            }
            let id = object
                .get("id")
                .map(|value| {
                    value
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .map(str::to_string)
                        .ok_or_else(|| "pit.id: нужна непустая строка".to_string())
                })
                .transpose()?;
            let ttl = object
                .get("keep_alive_ms")
                .map(|value| {
                    value.as_u64().ok_or_else(|| "pit.keep_alive_ms: нужно целое число".to_string())
                })
                .transpose()?
                .unwrap_or(60_000);
            (id, ttl)
        }
        _ => return Err("pit: нужна строка id либо объект {id,keep_alive_ms}".to_string()),
    };
    if ttl_ms == 0 || ttl_ms > PIT_MAX_TTL_MS {
        return Err(format!("pit.keep_alive_ms = {ttl_ms}, допустимо 1..={PIT_MAX_TTL_MS}"));
    }
    Ok(Some(PitAsk { id, ttl_ms }))
}

/// Снимает PIT без длинного visibility read-lock.
///
/// Live bitmap копируется оптимистично между двумя чтениями seqlock-поколения.
/// Если update/delete попал внутрь, копия выбрасывается и проход повторяется.
/// Поэтому `_mutate` не ждёт копирования десятков мегабайт, а PIT всё равно
/// относится к одному чётному состоянию.
fn capture_pit(
    live: &Live,
    writer: Option<&Arc<Writer>>,
    guard: Option<&svindb_core::work::Guard>,
) -> Result<FrozenReadView, String> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(100) {
        if guard.is_some_and(|guard| !guard.check()) {
            return Err(format!(
                "ReadView отменён до публикации: {}",
                guard.expect("проверено").reason().name()
            ));
        }
        let before =
            writer.map_or(0, |writer| writer.visibility_generation.load(Ordering::Acquire));
        if !before.is_multiple_of(2) {
            std::thread::yield_now();
            continue;
        }
        let segments = live.read().expect("набор сегментов").clone();
        let frozen = Arc::new(
            segments.list.iter().map(|segment| segment.shared_live_snapshot()).collect::<Vec<_>>(),
        );
        if guard.is_some_and(|guard| !guard.check()) {
            return Err(format!(
                "ReadView отменён после копирования live maps: {}",
                guard.expect("проверено").reason().name()
            ));
        }
        let after = writer.map_or(0, |writer| writer.visibility_generation.load(Ordering::Acquire));
        if before == after && after.is_multiple_of(2) {
            return Ok((segments, frozen));
        }
    }
    Err("не удалось снять immutable ReadView за 100 мс: visibility непрерывно менялась; повторите запрос"
        .to_string())
}

/// Счётчик активных поисков с гарантированным уменьшением на любом выходе.
struct Inflight<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> Inflight<'a> {
    fn enter(counter: &'a AtomicUsize) -> Inflight<'a> {
        counter.fetch_add(1, Ordering::AcqRel);
        Inflight { counter }
    }
}

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Команда центральному наблюдателю TCP-сокетов.
#[cfg(unix)]
type DisconnectGuardSlot = Arc<Mutex<Option<(u64, Arc<svindb_core::work::Guard>)>>>;

#[cfg(unix)]
enum DisconnectCommand {
    Register { id: u64, transport: TcpStream, slot: DisconnectGuardSlot },
    Unregister { id: u64, done: Option<std::sync::mpsc::SyncSender<()>> },
}

/// Один poller обслуживает все занятые вычислением соединения.
///
/// Поток на каждый search/GET был скрытой частью стоимости очереди: при
/// насыщении admission служебные наблюдатели могли превысить число потоков,
/// хотя сами запросы ещё не получили CPU permit. На Unix `poll` не меняет
/// флагов исходного дескриптора и поэтому безопасен для главного blocking
/// reader, включая TLS. Регистрация bounded: если канал заполнен, запрос
/// сохраняет deadline/cancellation на остальных границах и не создаёт новый
/// thread.
#[cfg(unix)]
struct DisconnectMonitor {
    next: AtomicU64,
    tx: std::sync::mpsc::SyncSender<DisconnectCommand>,
}

#[cfg(unix)]
struct WatchedDisconnect {
    id: u64,
    monitor: &'static DisconnectMonitor,
    released: bool,
}

#[cfg(unix)]
#[repr(C)]
struct PollFd {
    fd: std::os::unix::io::RawFd,
    events: i16,
    revents: i16,
}

#[cfg(unix)]
extern "C" {
    fn poll(fds: *mut PollFd, nfds: usize, timeout: i32) -> i32;
    fn recv(fd: std::os::unix::io::RawFd, buf: *mut u8, len: usize, flags: i32) -> isize;
}

#[cfg(unix)]
const POLLIN: i16 = 0x0001;
#[cfg(unix)]
const POLLERR: i16 = 0x0008;
#[cfg(unix)]
const POLLHUP: i16 = 0x0010;
#[cfg(unix)]
const POLLNVAL: i16 = 0x0020;

#[cfg(target_os = "linux")]
const MSG_DONTWAIT: i32 = 0x40;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const MSG_DONTWAIT: i32 = 0x80;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
const MSG_DONTWAIT: i32 = 0x40;
const MSG_PEEK: i32 = 0x02;

/// Starts a header read with an idle timeout and a short timeout after the
/// first application byte. Buffered bytes already count as progress, and TLS
/// handshake records do not. No cloned-socket peek participates in the decision.
fn renew_connection_header_deadline(reader: &mut std::io::BufReader<wire::Wire>) {
    let buffered = reader.buffer();
    if !buffered.is_empty() {
        let complete = buffered.windows(4).any(|bytes| bytes == b"\r\n\r\n");
        reader.get_mut().renew(if complete {
            Duration::from_secs(60)
        } else {
            Duration::from_millis(50)
        });
        return;
    }
    reader.get_mut().renew(Duration::from_secs(60));
    reader.get_mut().begin_header(Duration::from_millis(50));
}

#[cfg(unix)]
fn disconnect_peek(transport: &TcpStream, byte: &mut [u8; 1]) -> std::io::Result<usize> {
    let result = unsafe {
        recv(
            std::os::unix::io::AsRawFd::as_raw_fd(transport),
            byte.as_mut_ptr(),
            byte.len(),
            MSG_PEEK | MSG_DONTWAIT,
        )
    };
    if result < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DisconnectSocketKey {
    local: std::net::SocketAddr,
    peer: std::net::SocketAddr,
}

#[cfg(unix)]
struct WatchedDisconnectSocket {
    id: u64,
    transport: TcpStream,
    key: DisconnectSocketKey,
    slot: DisconnectGuardSlot,
}

#[cfg(unix)]
fn disconnect_socket_key(transport: &TcpStream) -> Option<DisconnectSocketKey> {
    Some(DisconnectSocketKey {
        local: transport.local_addr().ok()?,
        peer: transport.peer_addr().ok()?,
    })
}

#[cfg(unix)]
fn upsert_disconnect_socket(
    watched: &mut Vec<WatchedDisconnectSocket>,
    item: WatchedDisconnectSocket,
) {
    // `TcpStream::try_clone` выдаёт новый числовой fd, но сохраняет одну и ту
    // же пару TCP endpoints. Сравнение только raw fd поэтому не защищает от
    // накопления сотен клонов одного keep-alive-соединения. Если endpoint не
    // удалось прочитать, watcher не регистрируется: безопаснее оставить
    // только обычный deadline guard, чем бесконечно накапливать неизвестные
    // сокеты в центральном poller.
    if let Some(slot) = watched.iter_mut().find(|old| old.key == item.key) {
        *slot = item;
        return;
    }
    watched.push(item);
}

#[cfg(unix)]
impl DisconnectMonitor {
    fn new() -> &'static Self {
        static MONITOR: std::sync::OnceLock<DisconnectMonitor> = std::sync::OnceLock::new();
        MONITOR.get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::sync_channel(8192);
            std::thread::Builder::new()
                .name("svindb-disconnect-monitor".into())
                .spawn(move || Self::run(rx))
                .expect("наблюдатель disconnect не запустился");
            DisconnectMonitor { next: AtomicU64::new(0), tx }
        })
    }

    fn run(rx: std::sync::mpsc::Receiver<DisconnectCommand>) {
        let mut watched: Vec<WatchedDisconnectSocket> = Vec::new();
        loop {
            while let Ok(command) = rx.try_recv() {
                match command {
                    DisconnectCommand::Register { id, transport, slot } => {
                        let Some(key) = disconnect_socket_key(&transport) else {
                            continue;
                        };
                        let item = WatchedDisconnectSocket { id, key, transport, slot };
                        // Один TCP-сокет может последовательно обслужить
                        // сотни HTTP-запросов. Регистрация сторожа относится
                        // к текущему guard, а не к истории запросов. Если
                        // `Unregister` предыдущего id задержался в канале,
                        // простой `push` оставлял его cloned fd в poller и
                        // давал линейный рост дескрипторов до EMFILE. Замена по
                        // идентичности socket сохраняет новый guard, а старый
                        // `TcpStream` закрывает сразу; поздний unregister
                        // проверит id и новый entry не удалит.
                        upsert_disconnect_socket(&mut watched, item);
                    }
                    DisconnectCommand::Unregister { id, done } => {
                        watched.retain(|item| item.id != id);
                        if let Some(done) = done {
                            let _ = done.send(());
                        }
                    }
                }
            }
            if watched.is_empty() {
                // Нельзя просто выбросить полученную команду: первый watcher
                // приходит именно в пустой список, и потерянная регистрация
                // превращает disconnect в обычный deadline timeout.
                if let Ok(command) = rx.recv_timeout(Duration::from_millis(100)) {
                    match command {
                        DisconnectCommand::Register { id, transport, slot } => {
                            let Some(key) = disconnect_socket_key(&transport) else {
                                continue;
                            };
                            let item = WatchedDisconnectSocket { id, key, transport, slot };
                            upsert_disconnect_socket(&mut watched, item);
                        }
                        DisconnectCommand::Unregister { done, .. } => {
                            if let Some(done) = done {
                                let _ = done.send(());
                            }
                        }
                    }
                }
                continue;
            }
            let mut fds: Vec<PollFd> = watched
                .iter()
                .map(|item| PollFd {
                    fd: std::os::unix::io::AsRawFd::as_raw_fd(&item.transport),
                    events: POLLIN,
                    revents: 0,
                })
                .collect();
            // 25 мс — верхняя граница обнаружения idle disconnect; кооперативные
            // guard-проверки добавляют только остаток текущей CPU-порции.
            let result = unsafe { poll(fds.as_mut_ptr(), fds.len(), 25) };
            if result <= 0 {
                continue;
            }
            let mut byte = [0u8; 1];
            for (item, fd) in watched.iter().zip(fds.iter()) {
                if fd.revents & (POLLIN | POLLERR | POLLHUP | POLLNVAL) == 0 {
                    continue;
                }
                match disconnect_peek(&item.transport, &mut byte) {
                    Ok(0) => {
                        if let Ok(active) = item.slot.lock() {
                            if let Some((_, guard)) = active.as_ref() {
                                guard.cancel();
                            }
                        }
                    }
                    Ok(_) => {
                        // Данные (в том числе следующий keep-alive запрос)
                        // ещё лежат в сокете. Это не disconnect; основной reader
                        // заберёт их после завершения текущей работы.
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock
                                | std::io::ErrorKind::TimedOut
                                | std::io::ErrorKind::Interrupted
                        ) => {}
                    Err(_) => {
                        if let Ok(active) = item.slot.lock() {
                            if let Some((_, guard)) = active.as_ref() {
                                guard.cancel();
                            }
                        }
                    }
                }
            }
        }
    }

    fn register_connection(
        &'static self,
        transport: TcpStream,
        slot: DisconnectGuardSlot,
    ) -> Option<WatchedDisconnect> {
        // Не помещаем уже закрытый/неполностью установленный сокет в
        // бесконечный fallback: обычный request deadline всё равно остаётся
        // действующим, а monitor должен хранить только адресуемые записи.
        disconnect_socket_key(&transport)?;
        let id = self.next.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        self.tx.try_send(DisconnectCommand::Register { id, transport, slot }).ok()?;
        Some(WatchedDisconnect { id, monitor: self, released: false })
    }
}

#[cfg(unix)]
impl Drop for WatchedDisconnect {
    fn drop(&mut self) {
        if !self.released {
            let _ =
                self.monitor.tx.try_send(DisconnectCommand::Unregister { id: self.id, done: None });
        }
    }
}

#[cfg(unix)]
impl WatchedDisconnect {
    fn release_wait(&mut self) {
        if self.released {
            return;
        }
        let (done, complete) = std::sync::mpsc::sync_channel(0);
        let sent = self
            .monitor
            .tx
            .send(DisconnectCommand::Unregister { id: self.id, done: Some(done) })
            .is_ok();
        if sent {
            let _ = complete.recv_timeout(Duration::from_millis(100));
        }
        // Даже при остановившемся monitor команда либо уже была принята и
        // подтверждена, либо его поток вскоре закроет все свои Watched entries.
        self.released = true;
    }
}

#[cfg(unix)]
struct ConnectionDisconnect {
    slot: DisconnectGuardSlot,
    _registration: WatchedDisconnect,
    next: AtomicU64,
}

#[cfg(unix)]
impl ConnectionDisconnect {
    fn start(stream: &wire::Wire) -> Option<Arc<Self>> {
        let transport = stream.transport_clone().ok()?;
        let slot = Arc::new(Mutex::new(None));
        let registration =
            DisconnectMonitor::new().register_connection(transport, Arc::clone(&slot))?;
        Some(Arc::new(Self { slot, _registration: registration, next: AtomicU64::new(0) }))
    }

    fn activate(
        self: &Arc<Self>,
        guard: Arc<svindb_core::work::Guard>,
    ) -> Option<ActiveDisconnect> {
        let id = self.next.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let mut active = self.slot.lock().ok()?;
        if active.is_some() {
            // На одном соединении сервер исполняет только один HTTP-запрос.
            // Если вложенный путь нарушит это правило, не перетираем чужой
            // guard: вызывающая сторона сохранит обычный deadline.
            return None;
        }
        *active = Some((id, guard));
        Some(ActiveDisconnect { id, owner: Arc::clone(self) })
    }

    fn deactivate(&self, id: u64) {
        if let Ok(mut active) = self.slot.lock() {
            if active.as_ref().is_some_and(|(current, _)| *current == id) {
                *active = None;
            }
        }
    }
}

#[cfg(unix)]
impl Drop for ConnectionDisconnect {
    fn drop(&mut self) {
        self._registration.release_wait();
    }
}

#[cfg(unix)]
struct ActiveDisconnect {
    id: u64,
    owner: Arc<ConnectionDisconnect>,
}

#[cfg(unix)]
impl Drop for ActiveDisconnect {
    fn drop(&mut self) {
        self.owner.deactivate(self.id);
    }
}

#[cfg(unix)]
thread_local! {
    static CURRENT_CONNECTION_DISCONNECT: std::cell::RefCell<Option<Arc<ConnectionDisconnect>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(unix)]
struct ConnectionDisconnectScope;

#[cfg(unix)]
impl ConnectionDisconnectScope {
    fn install(connection: Option<Arc<ConnectionDisconnect>>) -> Self {
        CURRENT_CONNECTION_DISCONNECT.with(|current| *current.borrow_mut() = connection);
        Self
    }
}

#[cfg(unix)]
impl Drop for ConnectionDisconnectScope {
    fn drop(&mut self) {
        CURRENT_CONNECTION_DISCONNECT.with(|current| *current.borrow_mut() = None);
    }
}

#[cfg(not(unix))]
struct ConnectionDisconnectScope;

#[cfg(not(unix))]
fn install_connection_disconnect(_stream: &wire::Wire) -> ConnectionDisconnectScope {
    ConnectionDisconnectScope
}

#[cfg(unix)]
fn install_connection_disconnect(stream: &wire::Wire) -> ConnectionDisconnectScope {
    ConnectionDisconnectScope::install(ConnectionDisconnect::start(stream))
}

/// Наблюдает за закрытием TCP, пока синхронный обработчик занят вычислением.
/// На Unix все запросы используют один `poll`-поток. На остальных платформах
/// сохраняется bounded fallback с коротким read-timeout.
struct DisconnectWatch {
    #[cfg(unix)]
    watched: Option<ActiveDisconnect>,
    #[cfg(not(unix))]
    stop: Arc<AtomicBool>,
}

impl DisconnectWatch {
    fn start(stream: &wire::Wire, guard: Arc<svindb_core::work::Guard>) -> Option<Self> {
        #[cfg(unix)]
        {
            let connection = CURRENT_CONNECTION_DISCONNECT
                .with(|current| current.borrow().as_ref().cloned())
                .or_else(|| ConnectionDisconnect::start(stream))?;
            let watched = connection.activate(guard)?;
            Some(Self { watched: Some(watched) })
        }
        #[cfg(not(unix))]
        {
            let transport = stream.transport_clone().ok()?;
            transport.set_read_timeout(Some(Duration::from_millis(25))).ok()?;
            let stop = Arc::new(AtomicBool::new(false));
            let watched = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut byte = [0u8; 1];
                while !watched.load(Ordering::Acquire) {
                    match transport.peek(&mut byte) {
                        Ok(0) => {
                            guard.cancel();
                            break;
                        }
                        Ok(_) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock
                                    | std::io::ErrorKind::TimedOut
                                    | std::io::ErrorKind::Interrupted
                            ) => {}
                        Err(_) => {
                            guard.cancel();
                            break;
                        }
                    }
                }
            });
            Some(Self { stop })
        }
    }
}

#[cfg(unix)]
impl Drop for DisconnectWatch {
    fn drop(&mut self) {
        // Явно забираем регистрацию, чтобы её Drop отправил Unregister и
        // компилятор не трактовал поле как забытое состояние.
        let _ = self.watched.take();
    }
}

#[cfg(not(unix))]
impl Drop for DisconnectWatch {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// Completes a segment publication during WAL replay and applies deferred
/// supersedes to the newly reloaded live set.
fn replay_finalize_publish(
    ingest: &mut Indexer,
    live: &Live,
    dir: &str,
    before: usize,
) -> Result<(), String> {
    if ingest.segments() == before {
        return Ok(());
    }
    let next = Segments::load(dir)
        .map_err(|e| format!("{dir}: состав не перечитался после переигрывания: {e}"))?;
    *live.write().expect("набор сегментов") = Arc::new(next);
    let work = ingest.take_superseded();
    if work.is_empty() {
        return Ok(());
    }
    let segs = { live.read().expect("набор сегментов").clone() };
    let refs: Vec<&svindb_core::Index> = segs.list.iter().map(|a| a.as_ref()).collect();
    let mut again = Vec::new();
    for (keeper, ids) in work {
        let done = maintain::supersede_in(&refs, &keeper, &ids)
            .map_err(|e| format!("переигрывание: гашение прежних версий: {e}"))?;
        if !done.missing.is_empty() {
            again.push((keeper, done.missing));
        }
    }
    ingest.put_back_superseded(again);
    Ok(())
}

#[cfg(test)]
mod replay_publication_tests {
    use super::*;

    fn deferred_batch_supersedes_are_applied(mixed: bool) {
        use svindb_core::wal;
        let dir = std::env::temp_dir()
            .join(format!("svindb-replay-boundary-{}-{mixed}", std::process::id()));
        let dir = dir.to_str().unwrap();
        let mut ingest = Indexer::typed(dir, vec!["body".into()], vec![1.0], vec![Kind::Text], 4);
        ingest.seq = 1;
        for id in 0..4 {
            ingest.add(&format!("replay-{id}"), &["old"]);
        }
        if mixed {
            ingest.add("obsolete", &["old"]);
        }
        ingest.flush_now();
        let live: Live = Arc::new(RwLock::new(Arc::new(Segments::load(dir).unwrap())));
        let before = ingest.segments();
        let bodies: Vec<_> =
            (0..4).map(|id| wal::encode_strings(&[&format!("replay-{id}"), "new"])).collect();
        let record = wal::Record {
            seq: 2,
            kind: if mixed { wal::MUTATION } else { wal::BATCH },
            body: if mixed {
                wal::encode_mutation(wal::PUT, &bodies, &["obsolete".into()])
            } else {
                wal::encode_batch(wal::PUT, &bodies)
            },
        };
        // Exactly four replacements hit the flush threshold only when the
        // outer atomic batch finishes; recursive records cannot publish it.
        replay(&record, wal::Band::EffectsOnly, &mut ingest, &live, dir).unwrap();
        assert!(ingest.segments() > before);
        assert!(ingest.take_superseded().is_empty(), "published supersedes were left pending");
        let segments = live.read().unwrap().clone();
        let mut visible = 0;
        for segment in &segments.list {
            for doc in 0..segment.docs as u32 {
                if segment.is_live(doc) {
                    visible += 1;
                    assert_eq!(segment.fetch(doc, 0).as_deref(), Some(b"new".as_slice()));
                }
            }
        }
        assert_eq!(visible, 4, "WAL replay left duplicate published versions");
        drop(segments);
        drop(live);
        drop(ingest);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn batch_boundary_publication_applies_deferred_supersedes() {
        deferred_batch_supersedes_are_applied(false);
    }

    #[test]
    fn mixed_mutation_boundary_publication_applies_deferred_supersedes() {
        deferred_batch_supersedes_are_applied(true);
    }
}

/// Пересмотр каталога: появившийся сегмент подхватывается сам.
///
/// Замок здесь не нужен ни один: восстановление идёт до первого ответа клиенту.
fn replay(
    rec: &svindb_core::wal::Record,
    band: svindb_core::wal::Band,
    ingest: &mut Indexer,
    live: &Live,
    dir: &str,
) -> Result<(), String> {
    use svindb_core::wal::Band;
    // Тело уже лежит в опубликованном сегменте — добавить его второй раз значит
    // завести документу вторую живую версию. У `ADD` разрушительной части нет
    // (в отличие от `PUT`, который ещё и гасит прежнюю), поэтому пропускается
    // вся запись целиком, и это ровно то, что делает переигрывание идемпотентным.
    //
    // `PUT`, `DELETE` и `MUTATION` в этой полосе переигрываются **полностью**:
    // их гашения правят карту живых чужого, более старого сегмента, а та
    // доходит до диска только вместе с `msync` перед отметкой. Лишнее тело
    // замены уберёт укрупнение; невыполненное гашение не убрал бы никто.
    let body_is_durable = band == Band::EffectsOnly;
    if body_is_durable && matches!(rec.kind, svindb_core::wal::ADD | svindb_core::wal::ADD_CELLS) {
        ingest.seq = rec.seq;
        return Ok(());
    }
    // У атомарной пачки одна внешняя CRC. Внутренние тела применяются под
    // запретом auto-flush, чтобы порог сегмента не сделал durable-транзакцию
    // частично опубликованной во время восстановления.
    if rec.kind == svindb_core::wal::BATCH {
        let (kind, bodies) = svindb_core::wal::decode_batch(&rec.body)
            .ok_or_else(|| format!("атомарная пачка {} не разобралась", rec.seq))?;
        if bodies.iter().any(|body| svindb_core::wal::decode_strings(body).is_none()) {
            return Err(format!("атомарная пачка {} содержит негодное тело документа", rec.seq));
        }
        let before = ingest.segments();
        ingest.begin_atomic_batch();
        let applied = (|| {
            for body in bodies {
                replay(
                    &svindb_core::wal::Record { seq: rec.seq, kind, body },
                    band,
                    ingest,
                    live,
                    dir,
                )?;
            }
            Ok::<(), String>(())
        })();
        if let Err(e) = applied {
            ingest.cancel_atomic_batch();
            return Err(e);
        }
        ingest.finish_atomic_batch();
        replay_finalize_publish(ingest, live, dir, before)?;
        return Ok(());
    }

    let kill = |ids: &[String]| -> Result<(), String> {
        let segs = { live.read().expect("набор сегментов").clone() };
        let refs: Vec<&svindb_core::Index> = segs.list.iter().map(|a| a.as_ref()).collect();
        maintain::delete_in(&refs, ids)
            .map(|_| ())
            .map_err(|e| format!("переигрывание гашения: {e}"))
    };
    if rec.kind == svindb_core::wal::MUTATION {
        let (kind, documents, deletes) = svindb_core::wal::decode_mutation(&rec.body)
            .ok_or_else(|| format!("смешанная мутация {} не разобралась", rec.seq))?;
        if documents.iter().any(|body| svindb_core::wal::decode_strings(body).is_none()) {
            return Err(format!("смешанная мутация {} содержит негодное тело документа", rec.seq));
        }
        let before = ingest.segments();
        ingest.begin_atomic_batch();
        let applied = (|| {
            for body in documents {
                replay(
                    &svindb_core::wal::Record { seq: rec.seq, kind, body },
                    band,
                    ingest,
                    live,
                    dir,
                )?;
            }
            for id in &deletes {
                ingest.kill_buffered(id);
            }
            kill(&deletes)?;
            Ok::<(), String>(())
        })();
        if let Err(error) = applied {
            ingest.cancel_atomic_batch();
            return Err(error);
        }
        ingest.seq = rec.seq;
        ingest.finish_atomic_batch();
        replay_finalize_publish(ingest, live, dir, before)?;
        return Ok(());
    }
    let parts = svindb_core::wal::decode_strings(&rec.body)
        .ok_or_else(|| format!("запись WAL {} не разобралась", rec.seq))?;
    let before = ingest.segments();
    ingest.seq = rec.seq;
    match rec.kind {
        svindb_core::wal::ADD => {
            if let Some((id, values)) = parts.split_first() {
                let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                ingest.add(id, &refs);
            }
        }
        // Прежние версии здесь не гасятся — ровно как и на живом приёме.
        //
        // Прежде переигрывание гасило сразу, и это выглядело безобидно: узел ещё
        // никого не обслуживает, окна невидимости никто не увидит. Но окно не
        // закрывается концом переигрывания. Замена остаётся в буфере, а
        // погашенная старая версия — на диске, и поднявшийся узел начинает
        // работу с документом, которого поиск не находит вовсе. Проверено:
        // после обрыва на шести заменах поиск отвечал пустотой.
        //
        // Отложенный список едет с буфером и применяется публикацией, как и
        // всюду; до неё видна прежняя версия — та самая, что была видна и до
        // обрыва.
        svindb_core::wal::PUT => {
            if let Some((id, values)) = parts.split_first() {
                let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                ingest.replace(id, &refs);
            }
        }
        // Документ с массивами. Число групп берётся у приёмника, то есть у
        // той же схемы, по которой запись и делалась: пересчитывать его из
        // тела нечем — арность и ячейки в нём лежат подряд.
        //
        // Оба вида остаются в одной ветви: их разбор одинаков, а различие —
        // только в `replacing`. Отделять `ADD_CELLS` ради полосы не нужно,
        // потому что до сюда он в полосе `EffectsOnly` уже не доходит: его
        // отсекает проверка в начале функции, вместе с обычным `ADD`.
        svindb_core::wal::ADD_CELLS | svindb_core::wal::PUT_CELLS => {
            let ngroups = ingest.group_paths().len().max(1);
            let doc = nested::Doc::from_wal_parts(&parts, ngroups).ok_or_else(|| {
                format!(
                    "переигрывание: запись {} с массивами не разобралась по нынешней \
                     схеме — в ней {ngroups} групп",
                    rec.seq
                )
            })?;
            let replacing = rec.kind == svindb_core::wal::PUT_CELLS;
            nested::put_doc(ingest, &doc, replacing);
        }
        svindb_core::wal::DELETE => {
            for id in parts.iter() {
                ingest.kill_buffered(id);
            }
            kill(&parts)?;
        }
        other => {
            return Err(format!("переигрывание: запись {} неизвестного вида {other}", rec.seq));
        }
    }
    // A flush can occur in the middle of replay; refresh the view and apply
    // every deferred supersede before the next WAL record is considered.
    replay_finalize_publish(ingest, live, dir, before)?;
    Ok(())
}

fn spawn_rescan(
    live: Live,
    dir: String,
    reloads: Arc<Reloads>,
    stop: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, String> {
    std::thread::Builder::new()
        .name("segment-rescan".into())
        .spawn(move || loop {
            std::thread::sleep(Duration::from_millis(500));
            if stop.load(Ordering::Acquire) {
                return;
            }
            let now = Segments::ready(&dir).len();
            let prev = { live.read().unwrap().clone() };
            // Большой catalog/grid строится здесь, в фоне, а не первым GET.
            // Если состав ещё пуст или каталог временно недоступен, следующая
            // итерация повторит подготовку; request path не запускает полный
            // обход сегментов и вернёт bounded `id_directory_not_ready`.
            let guard = svindb_core::work::Guard::with_cancel_flag(0, Arc::clone(&stop));
            let _ = prev.prepare_id_directory_guarded(Some(&guard));
            if !guard.check() {
                return;
            }
            // Живой сегмент в составе означает, что здесь работает писатель, и
            // подбирать за ним новое с диска — не помощь, а вред.
            //
            // Пересмотр видит только каталог. Живой сегмент он перенёс бы как есть,
            // не зная, что его содержимое ровно сейчас и легло тем самым новым
            // сегментом, — и каждый документ буфера оказался бы в выдаче дважды.
            // Проверено: две живые версии одного `id` с одинаковой revision на
            // потоке замен с частым сбросом.
            //
            // Пропуск ничего не стоит: писатель подбирает легшее сам, в
            // `Writer::republish`, где живой сегмент снимается тем же движением.
            // А узлу только для чтения — тому, ради кого пересмотр и заведён, —
            // живой сегмент взяться неоткуда: у него нет приёма.
            if prev.live_ones().next().is_some() {
                continue;
            }
            let have = prev.list.len();
            if now == have || now == 0 {
                continue;
            }
            match Segments::reopen(&dir, &prev) {
                Ok(next) => {
                    if next.list.len() != have {
                        let docs = next.docs();
                        let count = next.list.len();
                        *live.write().unwrap() = Arc::new(next);
                        println!("подхвачено сегментов: {count}, документов {docs}");
                    }
                    reloads.ok();
                }
                // Пересмотр каталога — единственный, кто перезагружает состав на
                // узле только для чтения. Проглоти он отказ, и такой узел навсегда
                // застыл бы на составе, с которым поднялся.
                Err(e) => reloads.failed("пересмотр каталога", &e.to_string()),
            }
        })
        .map_err(|e| format!("поток пересмотра каталога не создан: {e}"))
}

/// Всё, что узел знает о себе помимо индекса: кто вправе обращаться, кому
/// открыт браузерный доступ, куда позволено писать снимки и свеж ли состав.
/// Имя индекса, под которым к узлу уже обращались.
///
/// ## Почему первое увиденное, а не имя каталога
///
/// Узел держит один индекс, и **как** его зовут, ему безразлично: различать
/// нечего. Вредно не любое имя, а **два разных имени на один узел** — тогда
/// клиент думает, что развёл данные, а они лежат вместе, и узнаёт он об этом не
/// отказом, а чужими документами в своей выдаче.
///
/// Привязать имя к каталогу данных было бы проще и хуже: каталог называет
/// оператор для себя, клиент его не знает, и совпадать они не обязаны ничем.
/// Такая привязка сломала бы всякого совместимого клиента, не поймав ни одной
/// настоящей ошибки.
///
/// Поэтому имя запоминается первое увиденное, и дальше требуется только
/// постоянство. Разбор — в `docs/adr/0002`.
static SEEN_INDEX: std::sync::OnceLock<Mutex<Option<String>>> = std::sync::OnceLock::new();

/// Годится ли имя из пути этому узлу.
///
/// `_all` и `*` годятся всегда: это привычные способы сказать «во всём, что
/// есть», и для узла с одним индексом они верны буквально.
///
/// Координатор имён не проверяет: своих данных у него нет, а шарды проверят
/// сами и по своим именам, а не по его.
fn index_name_ok(node: &Node, want: &str) -> Result<(), String> {
    if node.fanout.is_some() || want == "_all" || want == "*" {
        return Ok(());
    }
    let cell = SEEN_INDEX.get_or_init(|| Mutex::new(None));
    let mut seen = cell.lock().expect("имя индекса");
    match seen.as_deref() {
        None => {
            *seen = Some(want.to_string());
            Ok(())
        }
        Some(had) if had == want => Ok(()),
        Some(had) => Err(format!(
            "этот узел держит один индекс, и к нему уже обращались как к {had:?}; \
             запрошен {want:?}. Один узел — один индекс, см. docs/adr/0002"
        )),
    }
}

/// Перекрытия настроек индекса аргументами запуска многoиндексного узла.
///
/// Хранятся значениями, а не ссылками на `argv`: новые индексы создаются уже
/// после старта процесса и должны получить ровно те же операторские пределы,
/// что индексы, найденные при подъёме.
#[derive(Clone, Copy, Default)]
struct IndexOverrides {
    flush_docs: Option<u32>,
    target_docs: Option<usize>,
    refresh_interval_ms: Option<u64>,
    merge_at_once: Option<usize>,
    grace_ms: Option<u64>,
    merge_threads: Option<usize>,
    dead_fraction: Option<f64>,
}

impl IndexOverrides {
    fn from_flags(flags: &[&str]) -> Result<IndexOverrides, String> {
        fn integer(flags: &[&str], name: &str) -> Result<Option<u64>, String> {
            let Some(raw) = flags.iter().find_map(|f| f.strip_prefix(name)) else {
                return Ok(None);
            };
            raw.parse::<u64>()
                .map(Some)
                .map_err(|_| format!("{name}{raw}: ожидается целое неотрицательное число"))
        }
        let dead_fraction = match flags.iter().find_map(|f| f.strip_prefix("--dead-fraction=")) {
            Some(raw) => Some(
                raw.parse::<f64>()
                    .map_err(|_| format!("--dead-fraction={raw}: ожидается конечная доля"))?,
            ),
            None => None,
        };
        Ok(IndexOverrides {
            flush_docs: integer(flags, "--flush=")?.map(|v| v.min(u32::MAX as u64) as u32),
            target_docs: integer(flags, "--target=")?.map(|v| v as usize),
            refresh_interval_ms: integer(flags, "--refresh-ms=")?,
            merge_at_once: integer(flags, "--merge-at-once=")?.map(|v| v as usize),
            grace_ms: integer(flags, "--grace-ms=")?,
            merge_threads: integer(flags, "--merge-threads=")?.map(|v| v as usize),
            dead_fraction,
        })
    }

    fn apply(self, settings: &mut svindb_core::settings::Settings) -> Result<(), String> {
        if let Some(v) = self.flush_docs {
            settings.flush_docs = v;
        }
        if let Some(v) = self.target_docs {
            settings.target_docs = v;
        }
        if let Some(v) = self.refresh_interval_ms {
            settings.refresh_interval_ms = v;
        }
        if let Some(v) = self.merge_at_once {
            settings.merge_at_once = v;
        }
        if let Some(v) = self.grace_ms {
            settings.grace_ms = v;
        }
        if let Some(v) = self.merge_threads {
            settings.merge_threads = v;
        }
        if let Some(v) = self.dead_fraction {
            settings.dead_fraction = v;
        }
        settings.check()
    }
}

/// Неизменяемая часть конфигурации всех локальных индексов одного процесса.
#[derive(Clone)]
struct LocalIndexOptions {
    writable: bool,
    maintaining: bool,
    no_wal: bool,
    sync_flush: bool,
    posmin: u32,
    fields_fallback: String,
    overrides: IndexOverrides,
    guard: Arc<auth::Guard>,
    cors: config::Cors,
    snapshot: config::Snapshot,
    query_pool: Arc<svindb_core::budget::Pool>,
    /// Один bounded source cache на multi-index process, а не по 256 МиБ на
    /// каждый индекс. Иначе число индексов умножило бы memory reserve.
    source_cache: Arc<source_cache::Cache>,
    /// Общий профиль governor для всех индексов multi-root.
    workload: config::Workload,
    /// Единственная полоса тяжёлого merge на весь процесс. Бюджет памяти у
    /// merge процессный, поэтому раздать его каждому индексу означало бы
    /// умножить обещанную память на число индексов.
    maintenance_lane: Arc<Mutex<()>>,
    /// CREATE/DELETE и живые PUT settings меняют один процессный бюджет и
    /// потому проходят одной последовательной полосой.
    settings_lane: Arc<Mutex<()>>,
    ingest_budget: Arc<multi::IngestBudget>,
}

/// Открывает один полностью независимый индекс внутри многoиндексного процесса.
///
/// Функция не завершает процесс: ошибка создания одного индекса должна стать
/// HTTP-ответом, а ошибка открытия при старте — одним понятным отказом всего
/// узла. Все фоновые работы запускаются лишь после успешного recovery, поэтому
/// частично открытый индекс не оставляет за собой потоков и lock-файлов.
fn open_local_index(
    name: &str,
    dir: &str,
    options: &LocalIndexOptions,
) -> Result<Arc<Node>, String> {
    migration::ensure_ready(std::path::Path::new(dir))?;
    if options.writable {
        migration::ensure_writable(std::path::Path::new(dir))?;
    }
    let writer_lock = if options.writable {
        Some(IndexLease::Write {
            _lock: manifest::WriterLock::take(dir).map_err(|e| e.to_string())?,
        })
    } else {
        Some(IndexLease::Read {
            _lock: manifest::ReaderLock::take(dir).map_err(|e| e.to_string())?,
        })
    };
    if options.writable {
        let swept = manifest::sweep(dir);
        if !swept.is_empty() {
            let bytes: u64 = swept.iter().map(|(_, bytes)| *bytes).sum();
            println!(
                "индекс {name}: убрано брошенных сегментов {}: {:.2} ГиБ",
                swept.len(),
                bytes as f64 / (1u64 << 30) as f64
            );
        }
    }

    let mut settings = svindb_core::settings::load(dir)?;
    options.overrides.apply(&mut settings).map_err(|e| format!("индекс {name}: настройки: {e}"))?;
    let live_settings = Arc::new(RwLock::new(settings));
    let segs =
        Arc::new(Segments::load(dir).map_err(|e| format!("индекс {name} не загрузился: {e}"))?);
    let live: Live = Arc::new(RwLock::new(segs));
    let reloads = Arc::new(Reloads::new());
    let node_metrics = Arc::new(metrics::Metrics::new());

    let writer = if options.writable {
        let mapping_rules = mapping::Rules::load(dir).map_err(|e| format!("индекс {name}: {e}"))?;
        let mut decls = match schema_of(dir) {
            Some(decls) => decls,
            None => {
                parse_fields(&options.fields_fallback).map_err(|e| format!("индекс {name}: {e}"))?
            }
        };
        if mapping_rules.source_mode().stores() {
            ensure_internal_fields(&mut decls).map_err(|e| format!("индекс {name}: {e}"))?;
        }
        let (fields, boosts, kinds, stored, searched, _positioned) = spread(&decls);
        let declared = svindb_core::schema::declare_all(dir, &decls)
            .map_err(|e| format!("индекс {name}: схема: {e}"))?;
        let mut ingest =
            Indexer::typed_stored(dir, fields, boosts, kinds, stored, settings.flush_docs);
        ingest.set_searched(searched);
        ingest.set_fids(declared.fids);
        if declared.group_paths.len() > 1 {
            ingest.set_groups(declared.groups, declared.group_paths);
        }
        ingest.set_posmin(options.posmin);

        let published = segs_seq(dir);
        let report = svindb_core::wal::for_replay_banded(dir, published, |record, band| {
            replay(&record, band, &mut ingest, &live, dir)
        })
        .map_err(|e| format!("индекс {name}: журнал не восстановлен: {e}"))?;
        ingest
            .enable_frozen_memtables(options.workload.max_frozen_memtables)
            .map_err(|error| format!("индекс {name}: frozen lifecycle: {error}"))?;
        ingest.seq = svindb_core::wal::next_seq(dir, report.to, segs_seq(dir));
        ingest.flushed_seq = published;
        let applied_from = ingest.seq;
        let wal = if options.no_wal {
            None
        } else {
            Some(
                Wal::open(dir, ingest.seq, settings.wal_group_commit_ms, Arc::clone(&node_metrics))
                    .map_err(|e| format!("индекс {name}: журнал не открылся: {e}"))?,
            )
        };
        let layout = Arc::new(Mutex::new(()));
        if options.sync_flush {
            ingest.share_layout(Arc::clone(&layout));
        } else {
            ingest.background(Arc::clone(&layout));
        }
        let initial_landed = ingest.segments();
        let plan = Arc::new(nested::Plan::build_with(
            ingest.field_names(),
            ingest.field_kinds(),
            ingest.field_groups(),
            ingest.group_paths(),
            &mapping_rules,
        ));
        let shape = (ingest.field_names().len(), ingest.group_paths().len() > 1);
        let lossless = ingest.field_names().iter().any(|field| field == nested::SOURCE_FIELD);
        let field_kinds = ingest.field_kinds().to_vec();
        let field_searched = ingest.searched_flags().to_vec();
        let writer = Arc::new(Writer {
            plan,
            shape,
            lossless,
            field_kinds,
            field_searched,
            poisoned: AtomicBool::new(false),
            wal,
            recovered: report,
            dir: dir.to_string(),
            live: Arc::clone(&live),
            buffered_memory_bytes: AtomicU64::new(ingest.buffered_bytes() as u64),
            mutation_gate: RwLock::new(()),
            ingest: Mutex::new(ingest),
            checkpoint_due: AtomicBool::new(false),
            scratch: Mutex::new(Vec::new()),
            layout,
            checkpoint_gate: Mutex::new(svindb_core::wal::checkpoint(dir)),
            applied: Applied::new(applied_from),
            docs: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
            reloads: Arc::clone(&reloads),
            metrics: Arc::clone(&node_metrics),
            initial_landed,
            keys: std::array::from_fn(|_| Mutex::new(())),
            visibility: std::sync::RwLock::new(()),
            visibility_generation: AtomicU64::new(0),
            live_max_docs: AtomicU32::new(settings.live_max_docs),
            live_built_docs: AtomicU32::new(0),
            published: AtomicU64::new(0),
            pending_writes: AtomicUsize::new(0),
            write_queue_highwater: AtomicUsize::new(0),
            compaction_floor: AtomicUsize::new(usize::MAX),
            compaction_pending: AtomicUsize::new(0),
            compaction_pending_bytes: AtomicU64::new(0),
            compaction_completed_sources: AtomicU64::new(0),
            compaction_completed_input_bytes: AtomicU64::new(0),
            compaction_elapsed_micros: AtomicU64::new(0),
            l0_throttled: AtomicU64::new(0),
            l0_rejected: AtomicU64::new(0),
            visible_seq: AtomicU64::new(published),
            ready_seq: AtomicU64::new(published),
            cache_advised_seq: AtomicU64::new(published),
            drop_flushed_cache: AtomicBool::new(settings.drop_flushed_page_cache),
            settled_seq: AtomicU64::new(0),
            compaction_schedulable: AtomicUsize::new(0),
            compaction_running: AtomicUsize::new(0),
            compaction_blocked: Mutex::new(Default::default()),
            flush_state: Mutex::new(FlushState::default()),
            flush_done: Condvar::new(),
        });

        writer
            .ingest
            .lock()
            .map_err(|_| format!("индекс {name}: отравлен замок приёма"))?
            .set_flush_bytes(effective_flush_bytes(&settings, resources().wal_replay_bytes));
        writer
            .ingest
            .lock()
            .map_err(|_| format!("индекс {name}: отравлен замок приёма"))?
            .set_live_max_docs(settings.live_max_docs);

        let recovered_upto =
            if writer.wal.is_some() { segs_seq(dir).max(writer.recovered.to) } else { 0 };
        let recovered_tail =
            writer.ingest.lock().map(|ix| ix.active_buffered() > 0).unwrap_or(false);
        if recovered_tail {
            writer
                .drive_one_flush()
                .map_err(|e| format!("индекс {name}: хвост WAL не опубликован: {e}"))?;
        } else if svindb_core::wal::checkpoint(dir) < recovered_upto {
            writer.checkpoint_after_sync(recovered_upto);
        }
        let checkpoint = svindb_core::wal::checkpoint(dir);
        if checkpoint < recovered_upto {
            return Err(format!(
                "индекс {name}: recovery не завершён: checkpoint {checkpoint} < {recovered_upto}"
            ));
        }
        writer.observe_compaction_debt(settings.maintain());
        Some(writer)
    } else {
        None
    };

    let stop = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    let started = (|| -> Result<(), String> {
        threads.push(spawn_rescan(
            Arc::clone(&live),
            dir.to_string(),
            Arc::clone(&reloads),
            Arc::clone(&stop),
        )?);
        if let Some(writer) = writer.as_ref() {
            threads.push(spawn_refresh(
                Arc::clone(writer),
                Arc::clone(&live_settings),
                Arc::clone(&stop),
            )?);
            if options.maintaining {
                threads.push(spawn_maintenance(
                    Arc::clone(writer),
                    Arc::clone(&live_settings),
                    Arc::clone(&stop),
                    Arc::clone(&options.maintenance_lane),
                )?);
            }
        }
        Ok(())
    })();
    if let Err(error) = started {
        stop.store(true, Ordering::Release);
        for thread in threads {
            let _ = thread.join();
        }
        return Err(format!("индекс {name}: {error}"));
    }
    let terms = writer.is_some().then(|| Arc::new(replicate::Terms::open(dir)));
    Ok(Arc::new(Node {
        query_pool: Arc::clone(&options.query_pool),
        source_cache: Arc::clone(&options.source_cache),
        dir: dir.to_string(),
        live,
        snapshot_scope: Some(multi::encoded(name)),
        settings_lane: Arc::clone(&options.settings_lane),
        ingest_budget: Some(Arc::clone(&options.ingest_budget)),
        guard: Arc::clone(&options.guard),
        cors: options.cors.clone(),
        snapshot: options.snapshot.clone(),
        reloads,
        fanout: None,
        live_settings,
        writer,
        _writer_lock: writer_lock,
        background_stop: stop,
        background_threads: Mutex::new(threads),
        metrics: node_metrics,
        declared: Declared::of(dir).map_err(|error| format!("индекс {name}: {error}"))?,
        maintaining: options.maintaining,
        terms,
        repl: None,
    }))
}

enum IndexLease {
    Write { _lock: manifest::WriterLock },
    Read { _lock: manifest::ReaderLock },
}

pub struct Node {
    /// Общий generation-aware кэш распакованного source для процесса.
    source_cache: Arc<source_cache::Cache>,
    pub dir: String,
    /// Живой состав именно этого индекса. Состав и писатель выбираются одной
    /// ссылкой на `Node`, чтобы многoиндексная маршрутизация не могла случайно
    /// соединить сегменты одного индекса с WAL другого.
    live: Live,
    /// Подкаталог приёмника снимков для многoиндексного runtime. Два индекса
    /// вправе иметь снимок с одинаковым именем, поэтому общий `dest/name`
    /// допустим только в прежнем одноиндексном режиме.
    snapshot_scope: Option<String>,
    /// В multi-runtime этот замок общий с CREATE/DELETE всех индексов: новая
    /// настройка памяти и изменение состава видят одно последовательное
    /// состояние. В одноиндексном режиме замок локален.
    settings_lane: Arc<Mutex<()>>,
    /// Общий ingest-budget есть только у multi-runtime.
    ingest_budget: Option<Arc<multi::IngestBudget>>,
    pub guard: Arc<auth::Guard>,
    pub cors: config::Cors,
    pub snapshot: config::Snapshot,
    /// Свеж ли состав сегментов. Отсюда `/_health` берёт `ok`.
    reloads: Arc<Reloads>,
    /// Адреса шардов. Непусто — узел работает координатором: своих данных у него
    /// нет, он рассылает запрос и складывает верхушки.
    fanout: Option<coord::Fanout>,
    /// Живые настройки индекса: их читают обслуживание, сброс по времени и
    /// `/_settings`, а меняет `PUT`.
    live_settings: Arc<RwLock<svindb_core::settings::Settings>>,
    /// Писатель, если приём включён. Нужен `PUT /_settings`, чтобы применить
    /// новый потолок документов на сегмент к буферу.
    writer: Option<Arc<Writer>>,
    /// Эксклюзивное право писать именно этот каталог. Не протекает намеренно:
    /// удалённый индекс обязан освободить lock, чтобы то же имя можно было
    /// создать снова без перезапуска всего процесса.
    _writer_lock: Option<IndexLease>,
    /// Общий сигнал завершения пересмотра, refresh и maintenance этого индекса.
    background_stop: Arc<AtomicBool>,
    /// Потоки принадлежат индексу, а не процессу. Это делает hot-delete
    /// настоящим закрытием runtime, а не удалением файлов из-под живого кода.
    background_threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// Что узел про себя считает. Отдаётся точкой `/_metrics`.
    metrics: Arc<metrics::Metrics>,
    /// Общий счёт памяти запросов. Предел у одного запроса был всегда, а у их
    /// суммы — нет: двести пятьдесят шесть рабочих потоков по двести пятьдесят
    /// шесть мегабайт складывались в шестьдесят четыре гигабайта законной
    /// нагрузки, при которой узел снимало ядро.
    query_pool: Arc<svindb_core::budget::Pool>,
    /// Объявленная схема, снятая на подъёме. Разбор запроса спрашивает её, а не
    /// только уже легшие сегменты; см. [`Declared`].
    declared: Declared,
    /// Терм, который узел видел, и номер последней применённой репликации.
    ///
    /// Есть у всякого узла с приёмом, а не только у реплики: primary тоже
    /// обязан помнить чужой терм — иначе, пережив рестарт после смещения, он
    /// снова счёл бы себя законным.
    terms: Option<Arc<replicate::Terms>>,
    /// Держатели этого шарда, если узел — primary. `None` — узел одинок, и
    /// подтверждение записи значит ровно то же, что и раньше.
    repl: Option<Arc<replicate::Peers>>,
    /// Идёт ли фоновое обслуживание: слияние и очистка мёртвых.
    ///
    /// Отдельно от наличия писателя: `--read-only` снимает и запись, и
    /// обслуживание, а `--no-maintain` — только второе. Клиенту, который видит
    /// растущее число сегментов, надо отличать «узел не успевает» от «узлу
    /// велели не сливать».
    maintaining: bool,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.background_stop.store(true, Ordering::Release);
        let threads =
            self.background_threads.get_mut().unwrap_or_else(|poisoned| poisoned.into_inner());
        for thread in threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// Пропускной пункт: CORS и права **до** маршрутизации.
///
/// Одна точка, а не проверка в каждой ветке `serve`. Разложенная по веткам, она
/// однажды окажется забытой в новой ветке, и заметит это не тест, а тот, кто
/// прочтёт чужой индекс.
///
/// Отдаёт `false`, если уже ответил и обслуживать запрос не нужно.
fn gate(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    node: &Node,
    keep: bool,
    peer: Option<std::net::IpAddr>,
) -> bool {
    gate_with(stream, req, route, &node.guard, &node.cors, keep, peer)
}

/// Тот же пропускной пункт для процессных точек многoиндексного узла, у
/// которых ещё нет выбранного индекса (`PUT /{index}`, `/_cat/indices`).
fn gate_with(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    guard: &auth::Guard,
    cors_cfg: &config::Cors,
    keep: bool,
    peer: Option<std::net::IpAddr>,
) -> bool {
    let origin = req.header("origin");
    // Предполётный запрос разбирается до всего прочего: у него нет ни прав, ни
    // тела, и браузер шлёт его именно затем, чтобы **узнать**, позволено ли
    // посылать настоящий. Потребовать на него пароль значило бы не пустить
    // никого: учётных данных браузер в предполётный запрос не кладёт.
    if req.method == "OPTIONS" {
        let head = cors::preflight(cors_cfg, origin, req.header("access-control-request-headers"));
        http::set_extra(head);
        http::respond_empty(stream, 204, keep);
        return false;
    }
    http::set_extra(cors::headers(cors_cfg, origin));

    if matches!(req.method.as_str(), "POST" | "PUT" | "PATCH" | "DELETE")
        && origin.is_some_and(|o| !cors::trusted_for_write(cors_cfg, o, req.header("host")))
    {
        http::respond_with(
            stream,
            403,
            "{\"error\":\"источник браузерного запроса не разрешён\",\"type\":\"security_exception\"}",
            keep,
        );
        return false;
    }
    if matches!(req.method.as_str(), "POST" | "PUT" | "PATCH" | "DELETE")
        && req.body_len() > 0
        && !req.header("content-type").is_some_and(|v| {
            v.split(';').next().is_some_and(|t| {
                let t = t.trim();
                t.eq_ignore_ascii_case("application/json")
                        // NDJSON — только там, где пачка его и ждёт. Пускать
                        // его в настройки или права незачем, а без этого
                        // послабления ни один настоящий клиент Elasticsearch
                        // до приёма не доходит вовсе: он шлёт x-ndjson.
                        || (t.eq_ignore_ascii_case("application/x-ndjson")
                            && route.endpoint == route::Endpoint::Bulk)
            })
        })
    {
        http::respond_with(
            stream,
            415,
            "{\"error\":\"тело изменяющего запроса должно иметь Content-Type: application/json\"}",
            keep,
        );
        return false;
    }

    let need = auth::needed(&req.method, route);
    match guard.check_from(req.header("authorization"), need, peer) {
        auth::Verdict::Allow(_) => true,
        auth::Verdict::Anonymous => {
            http::respond_unauthorized(
                stream,
                "{\"error\":\"нужно представиться\",\"type\":\"security_exception\"}",
                keep,
            );
            false
        }
        auth::Verdict::Forbidden { who, need } => {
            // 403, а не 401, и разница существенная: «представься» против
            // «тебе нельзя». Клиент, получивший 401, повторит с паролем;
            // получивший 403 — не повторит, и правильно сделает.
            let payload = format!(
                "{{\"error\":{},\"type\":\"security_exception\",\"required\":{}}}",
                json_string(&format!("{who}: недостаточно прав")),
                json_string(need.name())
            );
            http::respond_with(stream, 403, &payload, keep);
            false
        }
        auth::Verdict::Throttled => {
            let mut headers = cors::headers(cors_cfg, origin);
            headers.push_str("Retry-After: 10\r\n");
            http::set_extra(headers);
            http::respond_with(
                stream,
                429,
                "{\"error\":\"слишком много попыток входа\",\"type\":\"security_exception\"}",
                false,
            );
            false
        }
    }
}

/// Снимки и токены. Вынесено из `serve`, чтобы поиск и приём не разрастались
/// управляющими путями.
fn snapshot_dest(node: &Node, requested: &str, create: bool) -> Result<String, String> {
    match node.snapshot_scope.as_deref() {
        Some(scope) => node.snapshot.scoped(requested, scope, create),
        None => node.snapshot.check(requested),
    }
}

fn serve_admin(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    node: &Node,
    keep: bool,
) -> bool {
    if route.endpoint == route::Endpoint::Snapshot {
        // Хвост берётся у разобранной цели, а не отрезанием от строки: прежде
        // `/_snapshot/имя?x=1` давал имя снимка `имя?x=1` — то есть строка
        // запроса уезжала в имя файла.
        let name = route.rest.trim_start_matches('/');
        if !name.is_empty() {
            if let Err(e) = svindb_core::snapshot::valid_name(name) {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                http::respond_with(stream, 400, &payload, keep);
                return true;
            }
        }
        match req.method.as_str() {
            "GET" if name.is_empty() => {
                let dests = node.snapshot.allow.clone();
                let mut all = Vec::new();
                for d in &dests {
                    let Ok(allowed) = snapshot_dest(node, d, false) else { continue };
                    for info in svindb_core::snapshot::list(&allowed) {
                        let candidate = format!("{allowed}/{}", info.name);
                        if node.snapshot.check(&candidate).is_err() {
                            continue;
                        }
                        all.push(svindb_core::json::Json::Obj(vec![
                            ("dest".into(), svindb_core::json::Json::Str(allowed.clone())),
                            ("name".into(), svindb_core::json::Json::Str(info.name.clone())),
                            ("taken".into(), svindb_core::json::Json::Int(info.taken as i64)),
                            ("docs".into(), svindb_core::json::Json::Int(info.docs as i64)),
                            (
                                "segments".into(),
                                svindb_core::json::Json::Int(info.segments.len() as i64),
                            ),
                            (
                                "bytes".into(),
                                svindb_core::json::Json::Int(info.total_stored() as i64),
                            ),
                            (
                                "bytes_raw".into(),
                                svindb_core::json::Json::Int(info.total_len() as i64),
                            ),
                        ]));
                    }
                }
                let doc = svindb_core::json::Json::Obj(vec![(
                    "snapshots".into(),
                    svindb_core::json::Json::Arr(all),
                )]);
                http::respond_with(stream, 200, &doc.to_compact(), keep);
            }
            "GET" => {
                let mut found = None;
                for d in &node.snapshot.allow {
                    let Ok(allowed) = snapshot_dest(node, d, false) else { continue };
                    let candidate = format!("{allowed}/{name}");
                    if !std::fs::symlink_metadata(&candidate).is_ok_and(|m| m.file_type().is_dir())
                    {
                        continue;
                    }
                    if node.snapshot.check(&candidate).is_err() {
                        continue;
                    }
                    if let Some(info) = svindb_core::snapshot::read(&candidate) {
                        found = Some(info);
                        break;
                    }
                }
                match found {
                    Some(info) => {
                        http::respond_with(stream, 200, &info.to_json().to_compact(), keep)
                    }
                    None => http::respond_with(
                        stream,
                        404,
                        &format!("{{\"error\":{}}}", json_string(&format!("снимка {name} нет"))),
                        keep,
                    ),
                }
            }
            "POST" if !name.is_empty() => {
                let body = String::from_utf8_lossy(&req.body).to_string();
                let doc = if body.trim().is_empty() {
                    svindb_core::json::Json::Obj(Vec::new())
                } else {
                    match svindb_core::json::parse(&body) {
                        Ok(d) => d,
                        Err(e) => {
                            let payload = format!("{{\"error\":{}}}", json_string(&e));
                            return {
                                http::respond_with(stream, 400, &payload, keep);
                                true
                            };
                        }
                    }
                };
                // Приёмник обязан быть назван: угадывать, куда писать
                // гигабайты, узел не станет.
                let dest = doc.get("dest").and_then(|x| x.as_str()).map(String::from);
                let fast = doc.get("fast").and_then(|x| x.as_bool()).unwrap_or(false);
                let dest = match dest {
                    Some(d) => d,
                    None if node.snapshot.allow.len() == 1 => node.snapshot.allow[0].clone(),
                    None => {
                        let payload = format!(
                            "{{\"error\":{}}}",
                            json_string(
                                "не указан dest, а разрешённых приёмников не один — \
                                 выберите явно"
                            )
                        );
                        http::respond_with(stream, 400, &payload, keep);
                        return true;
                    }
                };
                let dest = match snapshot_dest(node, &dest, true) {
                    Ok(d) => d,
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        http::respond_with(stream, 403, &payload, keep);
                        return true;
                    }
                };
                let effort = if fast {
                    svindb_core::codec::pack::Effort::Fast
                } else {
                    svindb_core::codec::pack::Effort::Thorough
                };
                // Публикация перед снятием — не удобство, а условие полноты.
                //
                // Снимок собирается по списку сегментов на диске. Всё принятое
                // и ещё не сброшенное лежит в буфере приёма и в журнал; в
                // снимок оно не попадает. Прежде это было тихой потерей: узел
                // отвечал «снимок готов», а в нём не хватало последних минут
                // приёма. Теперь эта потеря ещё и заметна снаружи — живой
                // сегмент делает такие документы находимыми, и оператор видел
                // бы в поиске то, чего в снимке нет.
                //
                // Ожидание ограничено тем же `flush_wait_ms`, что и `/_flush`:
                // снимок — работа административная, но держать соединение без
                // верхней границы нельзя и ей. Не успели — отказ, а не неполный
                // снимок: снимок, о неполноте которого никто не знает, хуже
                // отсутствующего.
                if let Some(w) = node.writer.as_ref() {
                    let wait = {
                        let s = node.live_settings.read().expect("настройки");
                        Duration::from_millis(s.flush_wait_ms)
                    };
                    match w.request_flush(wait) {
                        Ok(true) => {}
                        Ok(false) => {
                            let payload = format!(
                                "{{\"error\":{}}}",
                                json_string(
                                    "публикация не успела за flush_wait_ms — снимок вышел бы                                      без последних принятых документов; повторите позже или                                      поднимите flush_wait_ms"
                                )
                            );
                            http::respond_with(stream, 503, &payload, keep);
                            return true;
                        }
                        Err(e) => {
                            let payload = format!("{{\"error\":{}}}", json_string(&e));
                            http::respond_with(stream, 503, &payload, keep);
                            return true;
                        }
                    }
                }
                let t0 = std::time::Instant::now();
                match svindb_core::snapshot::create(&node.dir, &dest, name, effort) {
                    Ok(info) => {
                        let mut doc = info.to_json();
                        if let svindb_core::json::Json::Obj(kv) = &mut doc {
                            kv.push((
                                "took_ms".into(),
                                svindb_core::json::Json::Int(t0.elapsed().as_millis() as i64),
                            ));
                            kv.push(("dest".into(), svindb_core::json::Json::Str(dest.clone())));
                        }
                        http::respond_with(stream, 201, &doc.to_compact(), keep);
                    }
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        http::respond_with(stream, 409, &payload, keep);
                    }
                }
            }
            // Удаления нет — и это не «пока не сделали». Снимок, который можно
            // снести одним запросом, не сохранность, а иллюзия.
            "DELETE" => http::respond_with(
                stream,
                405,
                "{\"error\":\"удаление снимков через API не предусмотрено; \
                 уберите каталог руками\"}",
                keep,
            ),
            _ => http::respond_with(stream, 405, "{\"error\":\"метод не тот\"}", keep),
        }
        return true;
    }

    if route.endpoint == route::Endpoint::Security {
        return serve_security(stream, req, route, &node.guard, keep);
    }
    if route.endpoint == route::Endpoint::Settings {
        return serve_settings(stream, req, node, keep);
    }
    false
}

/// Поиск по шардам: рассылка, слияние, два режима точности.
///
/// Отдельная ветвь до разбора запроса: координатор тело не разбирает вовсе, он
/// пересылает его как есть. Разбирать его дважды — здесь и на шарде — означало бы
/// два способа понять один запрос, и однажды они разойдутся.
/// Validate the coordinator contract before contacting shards. Unsupported
/// reductions must fail explicitly rather than return a plausible wrong result.
fn prepare_fanout_request(body: &[u8]) -> Result<(serde_json::Value, usize, usize), String> {
    let raw: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;
    let mut ask = match es::dialect(&raw)? {
        es::Dialect::Native => raw,
        es::Dialect::Es => es::translate(&raw)?,
    };
    only_keys(&ask, "coordinator search", QUERY_KEYS)?;
    for feature in ["aggs", "sort", "search_after", "pit"] {
        if ask.get(feature).is_some_and(|value| {
            !value.is_null()
                && value.as_array().is_none_or(|items| !items.is_empty())
                && value.as_object().is_none_or(|items| !items.is_empty())
        }) {
            return Err(format!("coordinator does not support {feature} reduction"));
        }
    }
    if ask.get("track_total_hits").is_some_and(|value| value != &serde_json::Value::Bool(false)) {
        return Err("coordinator does not support track_total_hits reduction".into());
    }
    let integer = |name: &str, default: usize| -> Result<usize, String> {
        match ask.get(name) {
            None => Ok(default),
            Some(value) => value
                .as_u64()
                .and_then(|number| usize::try_from(number).ok())
                .ok_or_else(|| format!("{name} must be a non-negative integer")),
        }
    };
    let from = integer("from", 0)?;
    let size = integer("size", integer("k", 10)?)?;
    let window = from
        .checked_add(size)
        .filter(|value| *value <= 10_000)
        .ok_or("coordinator from + size exceeds 10000")?;
    let partial = match ask.get("partial") {
        None => false,
        Some(value) => value.as_bool().ok_or("partial must be a boolean")?,
    };
    if let Some(object) = ask.as_object_mut() {
        object.insert("partial".into(), serde_json::json!(partial));
        object.insert("from".into(), serde_json::json!(0));
        object.insert("size".into(), serde_json::json!(window));
        object.insert("k".into(), serde_json::json!(window.max(1)));
    }
    Ok((ask, from, size))
}

fn serve_fanout(
    stream: &mut wire::Wire,
    req: &http::Request,
    fan: &coord::Fanout,
    keep: bool,
    metrics: &metrics::Metrics,
) {
    let (ask, from, size) = match prepare_fanout_request(&req.body) {
        Ok(request) => request,
        Err(reason) => {
            return http::respond_with(
                stream,
                400,
                &serde_json::json!({"error":{"type":"unsupported_coordinator_query",
                "reason":reason},"status":400})
                .to_string(),
                keep,
            )
        }
    };
    let body = ask.to_string();
    let k = from + size;
    let routing = ask.get("routing").and_then(|value| match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        _ => None,
    });
    let routed = routing.as_deref().map(|key| fan.owner("", Some(key)));
    // Один routed shard и есть весь участвующий корпус, поэтому его локальная
    // статистика уже глобальна и первый DFS-круг не нужен.
    let exact = routed.is_none() && ask.get("scoring").and_then(|x| x.as_str()) == Some("global");
    let allow_partial = ask.get("partial").and_then(|x| x.as_bool()).unwrap_or(false);
    let t0 = Instant::now();
    let mut scoring = if exact { "global" } else { "local" };
    let mut preliminary_bad: Vec<String> = Vec::new();
    let mut stats_incomplete = false;

    // Точный режим — два круга. Первый только собирает частоты: шард читает их у
    // уже раскрытых слов и списков не обходит.
    // Реплики выбираются **один раз на запрос** и служат обоим кругам: попади
    // они в разные копии, статистику посчитала бы одна, а искала бы по ней
    // другая — с другой точкой свежести.
    let picks = fan.pick();
    let mut send = body.clone();
    if exact {
        let mut probe = ask.clone();
        if let serde_json::Value::Object(m) = &mut probe {
            m.insert("_stats_only".into(), serde_json::Value::Bool(true));
        }
        let first = fan.fan(&picks, "/", &probe.to_string());
        let bad = first.failed(&fan.shards);
        if !bad.is_empty() && !allow_partial {
            let payload = format!(
                "{{\"error\":{}}}",
                json_string(&format!(
                    "точный режим требует всех шардов: не ответили {}",
                    bad.join("; ")
                ))
            );
            return http::respond_with(stream, 503, &payload, keep);
        }
        // Статистика не сложилась — ветви `else` нет намеренно: честнее искать с
        // местной, чем с посчитанной по неполному набору. Об этом говорится в
        // ответе.
        if let Some((total, df)) = coord::merge_stats(&first.replies) {
            let mut with = ask.clone();
            if let serde_json::Value::Object(m) = &mut with {
                m.insert(GLOBAL_STATS.into(), serde_json::json!({"total": total, "df": df}));
            }
            send = with.to_string();
        } else {
            // Точный режим без полного DFS невозможен. Запрос разрешил partial,
            // поэтому выполняем его с локальными весами, но называем и смену
            // режима, и причину. Печатать `scoring:"global"` здесь означало бы
            // выдать другой порядок за запрошенный.
            if !allow_partial {
                return http::respond_with(
                    stream,
                    503,
                    "{\"error\":\"ответ DFS-статистики не разобрался; точный режим не выполнен\",\"retryable\":true}",
                    keep,
                );
            }
            scoring = "local";
            stats_incomplete = true;
            preliminary_bad = bad;
            if preliminary_bad.is_empty() {
                preliminary_bad.push("ответ DFS-статистики не разобрался".to_string());
            }
        }
    }

    let got = match routed {
        Some(shard) => {
            let mut bodies: Vec<Option<String>> = (0..fan.shards.len()).map(|_| None).collect();
            bodies[shard] = Some(send);
            fan.fan_each(&picks, "/", &bodies)
        }
        None => fan.fan(&picks, "/", &send),
    };
    metrics.shard_fallback(got.fallbacks(&fan.shards, &picks));
    let bad = got.failed(&fan.shards);
    if !bad.is_empty() && !allow_partial {
        let payload = format!(
            "{{\"error\":{}}}",
            json_string(&format!(
                "шарды не ответили: {}. Поставьте \"partial\":true, если выдача без \
                 них всё равно нужна",
                bad.join("; ")
            ))
        );
        return http::respond_with(stream, 503, &payload, keep);
    }
    let mut all_bad = preliminary_bad;
    for failure in bad.iter() {
        if !all_bad.contains(failure) {
            all_bad.push(failure.clone());
        }
    }
    let mut shard_terminated: Option<String> = None;
    let shard_partial = got.replies.iter().filter_map(|reply| reply.as_ref().ok()).any(|body| {
        let parsed = serde_json::from_str::<serde_json::Value>(body).ok();
        let partial = parsed
            .as_ref()
            .and_then(|value| value.get("partial"))
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        if partial && shard_terminated.is_none() {
            shard_terminated = parsed
                .as_ref()
                .and_then(|value| value.get("terminated"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
        }
        partial
    });
    if shard_partial && !allow_partial {
        return http::respond_with(
            stream,
            503,
            &serde_json::json!({"error":{"type":"incomplete_search_exception",
                "reason":"a shard returned incomplete results"},"status":503,
                "retryable":true})
            .to_string(),
            keep,
        );
    }
    let (hits, visited) = coord::merge_hits(&got.replies, k);
    let items: Vec<String> = hits
        .iter()
        .skip(from)
        .take(size)
        .map(|h| {
            let mut one = h.raw.clone();
            if let serde_json::Value::Object(m) = &mut one {
                // Номер документа локален для шарда, поэтому наружу выходит
                // парой. Склеить их в одно число значило бы вернуться к одному
                // пространству номеров — то есть к потолку, ради обхода которого
                // шарды и заведены.
                m.insert("shard".into(), serde_json::json!(h.shard));
            }
            one.to_string()
        })
        .collect();
    let failed = if all_bad.is_empty() {
        String::new()
    } else {
        format!(
            ",\"failed\":[{}]",
            all_bad.iter().map(|b| json_string(b)).collect::<Vec<_>>().join(",")
        )
    };
    let incomplete = !all_bad.is_empty() || stats_incomplete || shard_partial;
    let completion = if incomplete {
        let reason = if !bad.is_empty() {
            "shard_failure".to_string()
        } else if stats_incomplete {
            "global_stats_incomplete".to_string()
        } else {
            shard_terminated.unwrap_or_else(|| "shard_partial".to_string())
        };
        format!("\"partial\":true,\"complete\":false,\"terminated\":{},", json_string(&reason))
    } else {
        "\"partial\":false,\"complete\":true,".to_string()
    };
    let payload = format!(
        "{{{completion}\"took_ms\":{:.3},\"visited\":{visited},\"shards\":{},\"scoring\":{},\
         \"hits\":[{}]{failed}}}",
        t0.elapsed().as_secs_f64() * 1000.0,
        fan.shards.len(),
        json_string(scoring),
        items.join(",")
    );
    http::respond_with(stream, 200, &payload, keep);
}

/// Ключ, которым координатор передаёт шарду **общую** статистику запроса.
///
/// Внутренний: клиент его не ставит, а координатор ставит всегда во втором круге
/// точного режима. Разбирается на пути запроса, а не здесь, потому что нужен
/// вместе с уже раскрытым `QuerySpec`.
pub const GLOBAL_STATS: &str = "_global_stats";

/// Настройки индекса: посмотреть и поменять на живом узле.
///
/// `PUT` — частичное обновление, как у Elasticsearch: прислать одну настройку и
/// не перечислять остальные. Требование прислать всё означало бы, что клиент
/// сперва читает, потом пишет, — то есть гонку на двух администраторах.
///
/// Порядок действий несущий: **сперва на диск, потом в память**. Обратный
/// оставил бы узел, который работает по настройке, не дожившей до перезапуска, —
/// и объяснить такое расхождение было бы нечем.
/// `GET /{индекс}/_doc/{id}` — один документ по идентификатору.
///
/// ## Почему это не поиск с отбором по `id`
///
/// Идентификатор — не поле схемы: он лежит в своём словаре сегмента и
/// адресуется хешем, а не списком слов. Отбора по нему у запроса нет и быть не
/// может, поэтому без этой точки клиент, у которого на руках `id`, не мог
/// прочесть документ вовсе — только записать его заново или погасить.
///
/// ## Порядок просмотра
///
/// Сперва буфер приёма, затем сегменты от новых к старым, и в каждом — с конца.
/// Тот же порядок, что у CAS в [`Writer::current_value`], и по той же причине:
/// живых версий одного идентификатора бывает несколько, а нужна последняя.
/// Читать в обратном порядке значило бы отдавать позапрошлую версию — молча.
fn serve_doc_get(
    stream: &mut wire::Wire,
    route: &route::Route,
    live: &Live,
    writer: Option<&Arc<Writer>>,
    declared: &Declared,
    guard: &svindb_core::work::Guard,
    keep: bool,
) {
    let id = route.rest.as_str();
    if id.is_empty() {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"пустой идентификатор документа\"}",
            keep,
        );
    }
    let source_only = match query_bool(&route.query, "source_only", false) {
        Ok(value) => value,
        Err(error) => {
            return http::respond_with(
                stream,
                400,
                &format!("{{\"error\":{}}}", json_string(&error)),
                keep,
            )
        }
    };
    let ids = [id.to_string()];
    let mut got = match read_ids(live, writer, declared, &ids, 1, source_only, guard) {
        Ok(got) => got,
        Err(ReadIdsError::Cancelled) => {
            return http::respond_with(
                stream,
                503,
                &format!(
                    "{{\"error\":\"point read не завершён в заданный срок\",\"retryable\":true,\"reason\":{}}}",
                    json_string(guard.reason().name())
                ),
                keep,
            )
        }
        Err(ReadIdsError::DirectoryNotReady) => {
            return http::respond_with(
                stream,
                503,
                "{\"error\":\"generation-aware ID directory ещё не готов\",\"retryable\":true,\"reason\":\"id_directory_not_ready\"}",
                keep,
            )
        }
    };
    match got.pop().flatten() {
        Some(found) => http::respond_with(stream, 200, &found.payload(id, source_only), keep),
        // `found:false` рядом с `404`, а не вместо него: код нужен клиенту,
        // который смотрит только на статус, а поле — тому, кто читает тело.
        None => http::respond_with(
            stream,
            404,
            &format!("{{\"id\":{},\"found\":false}}", json_string(id)),
            keep,
        ),
    }
}

/// Найденная версия документа: где лежит и что в ней.
struct DocHit {
    /// Общий номер документа. `None` — версия ещё в буфере приёма.
    doc: Option<u64>,
    /// Поля уже собраны в тело объекта JSON, без обрамляющих скобок.
    fields: String,
    /// Исходный JSON ровно той же версии. `None` бывает у старого сегмента и у
    /// индекса, который явно создан с отключённым `_source`.
    source: Option<String>,
}

impl DocHit {
    fn payload(&self, id: &str, source_only: bool) -> String {
        let location = match self.doc {
            Some(doc) => format!(",\"doc\":{doc}"),
            None => ",\"buffered\":true".to_string(),
        };
        let source = self.source.as_ref().map_or_else(String::new, |raw| {
            // `raw` появился только после успешного `serde_json::to_string` и
            // затем прошёл CRC store, поэтому это готовое значение JSON, а не
            // строка, которую надо экранировать ещё раз.
            format!(",\"_source\":{raw}")
        });
        let fields =
            if source_only { String::new() } else { format!(",\"fields\":{{{}}}", self.fields) };
        format!(
            "{{\"id\":{},\"_id\":{},\"found\":true{location}{fields}{source}}}",
            json_string(id),
            json_string(id),
        )
    }
}

/// Собирает значения объявленного dot-path из исходного JSON.
///
/// Массив на промежуточном уровне обходится целиком. Поэтому
/// `changes.after_text` возвращается массивом значений, но `_source` рядом
/// остаётся исходной иерархией — никакого обратного восстановления из
/// разделителя здесь нет.
fn collect_json_path(value: &serde_json::Value, parts: &[&str], out: &mut Vec<serde_json::Value>) {
    if parts.is_empty() {
        out.push(value.clone());
        return;
    }
    match value {
        serde_json::Value::Object(object) => {
            if let Some(next) = object.get(parts[0]) {
                collect_json_path(next, &parts[1..], out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_json_path(item, parts, out);
            }
        }
        _ => {}
    }
}

/// Поля ответа из точной проекции, с сохранением presence и JSON-типов.
fn fields_from_projection(raw: &str, names: &[String]) -> Option<String> {
    let fields: Vec<usize> = names
        .iter()
        .enumerate()
        .filter_map(|(field, name)| (!name.is_empty() && !internal_field(name)).then_some(field))
        .collect();
    selected_fields_from_projection(raw, names, &fields)
}

fn selected_fields_from_projection(
    raw: &str,
    names: &[String],
    selected: &[usize],
) -> Option<String> {
    let root: serde_json::Value = serde_json::from_str(raw).ok()?;
    let object = root.as_object()?;
    let mut fields = Vec::new();
    for &field in selected {
        let Some(name) = names.get(field).filter(|name| !name.is_empty() && !internal_field(name))
        else {
            continue;
        };
        // Плоская проекция с буквальным dot-key тоже законна. Сначала точное
        // имя, затем структурный путь: это сохраняет прежний native-контракт и
        // одновременно понимает обычный вложенный JSON.
        let value = match object.get(name) {
            Some(value) if !value.is_null() => Some(value.clone()),
            Some(_) => None,
            None => {
                let mut found = Vec::new();
                collect_json_path(&root, &name.split('.').collect::<Vec<_>>(), &mut found);
                found.retain(|value| !value.is_null());
                match found.len() {
                    0 => None,
                    1 => found.pop(),
                    _ => Some(serde_json::Value::Array(found)),
                }
            }
        };
        if let Some(value) = value {
            fields.push(format!("{}:{}", json_string(name), value));
        }
    }
    Some(fields.join(","))
}

/// Одно scalar-значение проекции для CAS. `None` означает именно отсутствие
/// или JSON null; присутствующая пустая строка остаётся `Some("")`.
fn projection_scalar(raw: &str, path: &str, kind: Kind) -> Option<String> {
    let root: serde_json::Value = serde_json::from_str(raw).ok()?;
    let direct = root.as_object().and_then(|object| object.get(path));
    let value = match direct {
        Some(value) => value.clone(),
        None => {
            let mut found = Vec::new();
            collect_json_path(&root, &path.split('.').collect::<Vec<_>>(), &mut found);
            if found.len() != 1 {
                return None;
            }
            found.pop()?
        }
    };
    match intake::take_cell(Some(&value), kind) {
        intake::Taken::Value(value) => Some(value),
        _ => None,
    }
}

/// Текст объявленного пути для подсветки из атомарной проекции документа.
///
/// Отдельный stored leaf у нового индекса по умолчанию не нужен: он повторял
/// `_source`. Массивы скаляров соединяются пробелом, чтобы snippet видел все
/// значения, но исходный JSON ответа при этом, разумеется, не меняется.
fn projection_text(raw: &str, path: &str) -> Option<String> {
    fn append(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Array(values) => {
                for value in values {
                    append(value, out);
                }
            }
            serde_json::Value::String(value) => out.push(value.clone()),
            serde_json::Value::Number(value) => out.push(value.to_string()),
            serde_json::Value::Bool(value) => out.push(value.to_string()),
            serde_json::Value::Null | serde_json::Value::Object(_) => {}
        }
    }

    let root: serde_json::Value = serde_json::from_str(raw).ok()?;
    let mut found = Vec::new();
    if let Some(value) = root.as_object().and_then(|object| object.get(path)) {
        found.push(value.clone());
    } else {
        collect_json_path(&root, &path.split('.').collect::<Vec<_>>(), &mut found);
    }
    let mut text = Vec::new();
    for value in &found {
        append(value, &mut text);
    }
    (!text.is_empty()).then(|| text.join(" "))
}

/// Точный source-verifier для nested-внутри-nested.
///
/// Postings каждого уровня дают bounded множество кандидатов. Здесь
/// проверяется только parent-child связь по исходному JSON; если кандидатов
/// больше бюджета, запрос получает 429, а не усечённый правдоподобный ответ.
fn source_query_matches(
    root: &serde_json::Value,
    query: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
) -> Result<bool, String> {
    eval_source_query(root, "", query, names, kinds)
}

fn eval_source_query(
    current: &serde_json::Value,
    current_path: &str,
    query: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
) -> Result<bool, String> {
    let object = query.as_object().ok_or("source nested query обязан быть объектом")?;
    let mut results = Vec::new();
    for (verb, body) in object {
        let matched = match verb.as_str() {
            "nested" => {
                let nested = body.as_object().ok_or("nested обязан быть объектом")?;
                let path = nested
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("nested.path отсутствует")?;
                let inner = nested.get("query").ok_or("nested.query отсутствует")?;
                let relative = path
                    .strip_prefix(current_path)
                    .map(|tail| tail.trim_start_matches('.'))
                    .unwrap_or(path);
                let mut elements = Vec::new();
                collect_source_elements(
                    current,
                    &relative.split('.').filter(|part| !part.is_empty()).collect::<Vec<_>>(),
                    &mut elements,
                );
                elements.into_iter().try_fold(false, |seen, element| {
                    if seen {
                        Ok(true)
                    } else {
                        eval_source_query(element, path, inner, names, kinds)
                    }
                })?
            }
            "bool" => {
                let boolean = body.as_object().ok_or("bool обязан быть объектом")?;
                let arm = |name: &str| -> Result<Vec<&serde_json::Value>, String> {
                    match boolean.get(name) {
                        None => Ok(Vec::new()),
                        Some(value) => value
                            .as_array()
                            .map(|items| items.iter().collect())
                            .ok_or_else(|| format!("bool.{name} обязан быть массивом")),
                    }
                };
                let must = arm("must")?;
                let filter = arm("filter")?;
                let must_not = arm("must_not")?;
                let should = arm("should")?;
                let mut yes = true;
                for clause in must.iter().chain(filter.iter()) {
                    yes &= eval_source_query(current, current_path, clause, names, kinds)?;
                }
                for clause in must_not {
                    yes &= !eval_source_query(current, current_path, clause, names, kinds)?;
                }
                let should_hits = should
                    .iter()
                    .map(|clause| eval_source_query(current, current_path, clause, names, kinds))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|hit| *hit)
                    .count();
                let minimum = boolean
                    .get("minimum_should_match")
                    .and_then(serde_json::Value::as_u64)
                    .map(|value| value as usize)
                    .unwrap_or_else(|| {
                        usize::from(must.is_empty() && filter.is_empty() && !should.is_empty())
                    });
                yes && should_hits >= minimum
            }
            "term" => source_term(current, current_path, body, names, kinds, false)?,
            "terms" => source_term(current, current_path, body, names, kinds, true)?,
            "match" => source_match(current, current_path, body)?,
            "range" => source_range(current, current_path, body, names, kinds)?,
            "exists" => {
                let field = body
                    .get("field")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("exists.field отсутствует")?;
                !source_field_values(current, current_path, field, names).is_empty()
            }
            "match_all" => body.as_object().is_some_and(|body| body.is_empty()),
            other => return Err(format!("source verifier не понимает query {other:?}")),
        };
        results.push(matched);
    }
    Ok(results.into_iter().all(|result| result))
}

fn collect_source_elements<'a>(
    value: &'a serde_json::Value,
    parts: &[&str],
    out: &mut Vec<&'a serde_json::Value>,
) {
    if parts.is_empty() {
        match value {
            serde_json::Value::Array(items) => out.extend(items),
            serde_json::Value::Null => {}
            other => out.push(other),
        }
        return;
    }
    match value {
        serde_json::Value::Object(object) => {
            if let Some(next) = object.get(parts[0]) {
                collect_source_elements(next, &parts[1..], out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_source_elements(item, parts, out);
            }
        }
        _ => {}
    }
}

fn source_field_values<'a>(
    current: &'a serde_json::Value,
    current_path: &str,
    field: &str,
    names: &[String],
) -> Vec<&'a serde_json::Value> {
    let relative =
        field.strip_prefix(current_path).map(|tail| tail.trim_start_matches('.')).unwrap_or(field);
    let mut out = Vec::new();
    collect_source_elements(
        current,
        &relative.split('.').filter(|part| !part.is_empty()).collect::<Vec<_>>(),
        &mut out,
    );
    if out.is_empty() {
        if let Some((parent, _)) = field.rsplit_once('.') {
            if names.iter().any(|name| name == parent) {
                let relative = parent
                    .strip_prefix(current_path)
                    .map(|tail| tail.trim_start_matches('.'))
                    .unwrap_or(parent);
                collect_source_elements(
                    current,
                    &relative.split('.').filter(|part| !part.is_empty()).collect::<Vec<_>>(),
                    &mut out,
                );
            }
        }
    }
    out.into_iter().filter(|value| !value.is_null()).collect()
}

fn source_term(
    current: &serde_json::Value,
    current_path: &str,
    body: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
    many: bool,
) -> Result<bool, String> {
    let object = body.as_object().ok_or("term/terms обязан быть объектом")?;
    let (field, wanted) = object.iter().next().ok_or("term/terms пуст")?;
    let fid = names
        .iter()
        .position(|name| name == field)
        .ok_or_else(|| format!("source verifier: поле {field:?} отсутствует в compiled mapping"))?;
    let kind = kinds.get(fid).copied().unwrap_or(Kind::Text);
    let wanted: Vec<&serde_json::Value> = if many {
        wanted.as_array().ok_or("terms требует массив")?.iter().collect()
    } else {
        vec![wanted]
    };
    for value in source_field_values(current, current_path, field, names) {
        for expected in &wanted {
            if source_scalar_equal(value, expected, kind) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn source_scalar_equal(a: &serde_json::Value, b: &serde_json::Value, kind: Kind) -> bool {
    let text = |value: &serde_json::Value| match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        _ => None,
    };
    let (Some(a), Some(b)) = (text(a), text(b)) else { return false };
    if kind.numeric() {
        return kind.parse_value(&a) == kind.parse_value(&b);
    }
    if kind.folds_case() || kind == Kind::Text {
        a.to_lowercase() == b.to_lowercase()
    } else {
        a == b
    }
}

fn source_match(
    current: &serde_json::Value,
    current_path: &str,
    body: &serde_json::Value,
) -> Result<bool, String> {
    let object = body.as_object().ok_or("match обязан быть объектом")?;
    let (field, query) = object.iter().next().ok_or("match пуст")?;
    let query = query
        .as_str()
        .or_else(|| query.get("query").and_then(serde_json::Value::as_str))
        .ok_or("match.query обязан быть строкой")?;
    let words: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
    let mut values = Vec::new();
    let relative =
        field.strip_prefix(current_path).map(|tail| tail.trim_start_matches('.')).unwrap_or(field);
    collect_source_elements(
        current,
        &relative.split('.').filter(|part| !part.is_empty()).collect::<Vec<_>>(),
        &mut values,
    );
    Ok(values.iter().filter_map(|value| value.as_str()).any(|value| {
        let value = value.to_lowercase();
        words.iter().any(|word| value.split_whitespace().any(|token| token == word))
    }))
}

fn source_range(
    current: &serde_json::Value,
    current_path: &str,
    body: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
) -> Result<bool, String> {
    let object = body.as_object().ok_or("range обязан быть объектом")?;
    let (field, bounds) = object.iter().next().ok_or("range пуст")?;
    let bounds = bounds.as_object().ok_or("range bounds обязан быть объектом")?;
    let fid = names.iter().position(|name| name == field).ok_or("range field отсутствует")?;
    let kind = kinds.get(fid).copied().unwrap_or(Kind::Int64);
    let edge = |name: &str| -> Option<i64> {
        let value = bounds.get(name)?;
        let raw = value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string());
        kind.parse_value(&raw)
    };
    Ok(source_field_values(current, current_path, field, names).into_iter().any(|value| {
        let raw = value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string());
        let Some(value) = kind.parse_value(&raw) else { return false };
        edge("gte").is_none_or(|edge| value >= edge)
            && edge("gt").is_none_or(|edge| value > edge)
            && edge("lte").is_none_or(|edge| value <= edge)
            && edge("lt").is_none_or(|edge| value < edge)
    }))
}

/// Согласованный снимок для чтения по идентификатору: буфер приёма и состав.
///
/// ## Почему это один объект, а не два обращения подряд
///
/// Версия документа живёт либо в буфере, либо в сегменте, и публикация
/// переносит её из первого во второй. Возьми читатель снимок состава до того,
/// как взял буфер, и попади публикация ровно между, — документ в снимке ещё не
/// опубликован, а в буфере уже не лежит. Ответом было бы `404` на документ,
/// который никуда не девался. Ровно та потеря, ради закрытия которой точка и
/// заводилась, только окном в микросекунды вместо refresh.
///
/// Поэтому замок буфера берётся **первым**, и состав снимается уже под ним:
/// публикация идёт под тем же замком и в этот промежуток не влезает. Порядок
/// совпадает с порядком у писателя (`ingest`, затем состав) — обратный завёл бы
/// встречное ожидание.
/// Замок при этом держится **только на снимок и пробы по буферу**, а не на
/// весь запрос. Разница видна на наборе: прежде `_mget` на тысячу
/// идентификаторов держал `ingest` всё время, пока обходил сегменты и
/// распаковывал содержимое, — то есть приём стоял ровно столько, сколько длится
/// самое дорогое чтение. Проба по буферу — это хеш и сравнение строки;
/// оставлять под замком её одну достаточно, потому что вся оговорка выше про
/// публикацию касается ровно её.
struct DocView {
    segs: Arc<Segments>,
    names: Vec<String>,
    kinds: Vec<Kind>,
    /// Что нашлось в буфере — снято под замком, дальше только читается.
    ///
    /// Ключ — **место в списке**, с которым позвали [`DocView::take`], а не сам
    /// идентификатор. Отсюда обязательство: `resolve` зовут с тем же списком.
    /// Взамен поиск по буферу становится сравнением чисел: при наборе, только
    /// что принятом и тут же прочитанном, буфер содержит все тысячу
    /// идентификаторов, и сравнение строк дало бы миллион сравнений на пустом
    /// месте.
    buffered: Vec<(usize, DocHit)>,
    threads: usize,
    /// Сколько публикаций насчитал узел к моменту снимка. `None` — узел без
    /// приёма, публиковать некому.
    mark: Option<u64>,
    source_fid: Option<usize>,
    projection_fid: Option<usize>,
    /// Источник без проекции и declared-field fallback. Это настоящий
    /// source-only point read: кроме `_source` ни один store не открывается.
    source_only: bool,
}

/// Минимальный согласованный снимок, который снимается под ingest-lock.
struct PointSnapshot {
    segs: Arc<Segments>,
    buffered: Vec<(usize, DocHit)>,
    source_fid: Option<usize>,
    projection_fid: Option<usize>,
}

impl DocView {
    /// Source-only не строит реестр проекции обходом metadata всех сегментов.
    /// Номера source/projection уже закреплены согласованным снимком.
    fn projection_schema(
        segments: &Segments,
        declared: &Declared,
        source_only: bool,
    ) -> (Vec<String>, Vec<Kind>) {
        if source_only {
            (Vec::new(), Vec::new())
        } else {
            (field_names_of(segments, declared), field_kinds_of(segments, declared))
        }
    }

    fn take(
        live: &Live,
        writer: Option<&Arc<Writer>>,
        declared: &Declared,
        ids: &[String],
        threads: usize,
        source_only: bool,
        guard: &svindb_core::work::Guard,
    ) -> Option<DocView> {
        Some(match writer {
            Some(w) => {
                // Под ingest-lock остаётся только согласованный снимок. Имена
                // и типы disk-полей вычисляются уже после освобождения замка:
                // на большом индексе это обход metadata сотен сегментов, и
                // прежний scope сериализовал независимые GET/MGET примерно
                // по две миллисекунды на каждый запрос.
                let (mark, snapshot) = {
                    let mut ix = w.lock_ingest_guarded(guard)?;
                    // Отметка снимается **под замком**, вместе со снимком:
                    // иначе она относилась бы к другому мгновению, чем состав.
                    let mark = Some(w.published.load(Ordering::Acquire));
                    let snapshot = DocView::snapshot_held(&mut ix, live, ids, source_only, guard);
                    (mark, snapshot)
                };
                let PointSnapshot { segs, buffered, source_fid, projection_fid } = snapshot;
                let (names, kinds) = Self::projection_schema(&segs, declared, source_only);
                DocView {
                    segs,
                    names,
                    kinds,
                    buffered,
                    threads,
                    mark,
                    source_fid,
                    projection_fid,
                    source_only,
                }
            }
            None => {
                let segs = live.read().expect("набор сегментов").clone();
                let (names, kinds) = Self::projection_schema(&segs, declared, source_only);
                DocView {
                    segs,
                    names,
                    kinds,
                    buffered: Vec::new(),
                    threads,
                    mark: None,
                    source_fid: declared.source_fid,
                    projection_fid: declared.projection_fid,
                    source_only,
                }
            }
        })
    }

    /// Устаревший путь снимка с уже удерживаемым замком оставлен только для
    /// совместимости старых тестов; рабочий GET/MGET использует [`DocView::take`].
    #[allow(clippy::too_many_arguments, dead_code)]
    fn held(
        ix: &mut Indexer,
        live: &Live,
        declared: &Declared,
        ids: &[String],
        threads: usize,
        mark: Option<u64>,
        source_only: bool,
        guard: &svindb_core::work::Guard,
    ) -> DocView {
        let PointSnapshot { segs, buffered, source_fid, projection_fid } =
            DocView::snapshot_held(ix, live, ids, source_only, guard);
        let (names, kinds) = Self::projection_schema(&segs, declared, source_only);
        DocView {
            segs,
            names,
            kinds,
            buffered,
            threads,
            mark,
            source_fid,
            projection_fid,
            source_only,
        }
    }

    /// Снимает только ту часть point-view, которая обязана быть согласована с
    /// ingest-буфером. Замок удерживается только до копирования буфера и
    /// `Arc`-снимка сегментов; каталог и гидратация выполняются после его
    /// освобождения.
    fn snapshot_held(
        ix: &mut Indexer,
        live: &Live,
        ids: &[String],
        source_only: bool,
        guard: &svindb_core::work::Guard,
    ) -> PointSnapshot {
        let mut buffered: Vec<(usize, DocHit)> = Vec::new();
        let segs = {
            {
                let segs = { live.read().expect("набор сегментов").clone() };
                // Имена и типы берутся у **буфера**, а не у сегментов.
                //
                // Сегментов может не быть вовсе — ровно так и выглядит первый
                // документ нового индекса, — и тогда `field_names_of` честно
                // отдаёт умолчание `title, body`. Собрав по нему выдачу, точка
                // молча теряла бы все прочие поля документа, который только что
                // приняла: проверено на схеме из пяти полей, вернулось два.
                let names = ix.field_names().to_vec();
                let kinds = ix.field_kinds().to_vec();
                let source_fid = names.iter().position(|name| name == nested::SOURCE_FIELD);
                let projection_fid = names.iter().position(|name| name == nested::PROJECTION_FIELD);
                for (at, id) in ids.iter().enumerate() {
                    if !guard.check() {
                        break;
                    }
                    let Some(doc) = ix.buffered_doc(id) else { continue };
                    let source = source_fid.and_then(|field| ix.buffered_at(doc, field));
                    let projection = (!source_only)
                        .then(|| {
                            projection_fid
                                .and_then(|field| ix.buffered_at(doc, field))
                                .filter(|raw| !raw.is_empty())
                                .or_else(|| source.clone())
                        })
                        .flatten();
                    let fields = if source_only {
                        String::new()
                    } else {
                        projection
                            .as_deref()
                            .and_then(|raw| fields_from_projection(raw, &names))
                            .unwrap_or_else(|| {
                                names
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, name)| !internal_field(name))
                                    .filter_map(|(i, name)| {
                                        let raw = ix.buffered_at(doc, i)?;
                                        Some(format!(
                                            "{}:{}",
                                            json_string(name),
                                            json_value(&raw, kinds.get(i))
                                        ))
                                    })
                                    .collect::<Vec<_>>()
                                    .join(",")
                            })
                    };
                    buffered.push((at, DocHit { doc: None, fields, source }));
                }
                segs
            }
        };
        let source_fid = ix.field_names().iter().position(|name| name == nested::SOURCE_FIELD);
        let projection_fid =
            ix.field_names().iter().position(|name| name == nested::PROJECTION_FIELD);
        PointSnapshot { segs, buffered, source_fid, projection_fid }
    }

    /// Последние живые версии всего набора: сперва буфер, затем сегменты.
    ///
    /// ## Почему набором, а не по одному
    ///
    /// Прежде каждый идентификатор обходил **все** сегменты сам. На тысяче
    /// идентификаторов и двадцати сегментах это двадцать тысяч проб, идущих в
    /// худшем возможном порядке: соседние пробы попадают в разные сегменты, то
    /// есть в разные фильтры Блума и разные карты. Здесь порядок обратный —
    /// сегмент снаружи, идентификаторы внутри: фильтр сегмента читается подряд,
    /// а множество нерешённых сжимается по мере того, как версии находятся.
    ///
    /// Ответ от перестановки не меняется: сегменты по-прежнему обходятся от
    /// новых к старым, и первый ответивший забирает идентификатор себе.
    ///
    /// Зовут **с тем же списком**, с которым брали снимок: пробы по буферу
    /// помечены местом в нём. Разбор — у поля `buffered`.
    fn resolve(
        &self,
        ids: &[String],
        guard: &svindb_core::work::Guard,
    ) -> Result<Vec<Option<DocHit>>, ReadIdsError> {
        let mut out: Vec<Option<DocHit>> = (0..ids.len()).map(|_| None).collect();
        // Где лежит найденное: место в ответе → (сегмент, местный номер).
        let mut at: Vec<(usize, usize, u32)> = Vec::new();
        let mut left: Vec<usize> = Vec::with_capacity(ids.len());
        // snapshot_held записывает позиции в порядке запроса. Один курсор
        // сохраняет дубликаты ID и убирает квадратичный поиск по буферу.
        let mut buffered = self.buffered.iter().peekable();
        for (i, _) in ids.iter().enumerate() {
            if !guard.check() {
                return Err(ReadIdsError::Cancelled);
            }
            match buffered.next_if(|(position, _)| *position == i) {
                Some((_, hit)) => {
                    out[i] = Some(DocHit {
                        doc: hit.doc,
                        fields: hit.fields.clone(),
                        source: hit.source.clone(),
                    })
                }
                None => left.push(i),
            }
        }

        // Все цели уже найдены в согласованном снимке памяти: каталог на
        // диске, в том числе его холодная инициализация, здесь не нужен.
        if left.is_empty() {
            return Ok(out);
        }

        // На действительно пустом индексе отсутствие доказуемо без каталога:
        // в составе нет ни одного ID-map, который мог бы содержать цель. Это
        // сохраняет обычный 404 для нового/пустого индекса, но не разрешает
        // скрытый полный обход непустого legacy-состава.
        if self.segs.list.iter().all(|segment| segment.idmap.is_empty()) {
            return Ok(out);
        }

        // Frozen/live memory generations — ограниченная RAM-надстройка, которая
        // не входит в дисковый каталог. Сначала разрешаем их напрямую, и лишь
        // затем решаем, нужен ли disk directory. Так удаления и только что
        // видимые документы остаются точными, а маленький индекс без первого
        // каталога не запускает обход всех дисковых сегментов.
        let mut memory_left = left;
        let mut found = Vec::new();
        for (s, segment) in self.segs.list.iter().enumerate().rev() {
            if !segment.src.is_memory() || memory_left.is_empty() {
                continue;
            }
            if !guard.check() {
                return Err(ReadIdsError::Cancelled);
            }
            memory_left.retain(|&i| {
                if !guard.check() {
                    return true;
                }
                found.clear();
                segment.idmap.find_all(&ids[i], bloom::hash(&ids[i]), &mut found);
                match found.iter().rev().find(|&&doc| segment.is_live(doc)) {
                    Some(&local) => {
                        at.push((i, s, local));
                        false
                    }
                    None => true,
                }
            });
        }
        left = memory_left;
        if left.is_empty() {
            // Все цели разрешены в памяти; disk catalog не требуется.
        } else if self.segs.list.iter().all(|segment| segment.src.is_memory()) {
            // В составе нет disk segments, поэтому оставшиеся цели доказуемо
            // отсутствуют после bounded RAM overlay-прохода.
            return if at.is_empty() { Ok(out) } else { self.hydrate(out, at, guard) };
        }

        if left.is_empty() {
            return self.hydrate(out, at, guard);
        }

        // Сразу после восстановления очень маленький состав может ещё не иметь
        // опубликованного disk-каталога. В этом специальном случае один
        // bounded проход по максимум восьми `idmap` дешевле и безопаснее
        // временного 503: стоимость ограничена константой и не зависит от
        // production-числа сегментов. Для большего состава ниже разрешён
        // только готовый generation-aware directory.
        if self.segs.list.len() <= POINT_SCAN_FALLBACK_MAX_SEGMENTS {
            let mut disk_left = left;
            let mut found = Vec::new();
            for (s, segment) in self.segs.list.iter().enumerate().rev() {
                if segment.src.is_memory() || disk_left.is_empty() {
                    continue;
                }
                if !guard.check() {
                    return Err(ReadIdsError::Cancelled);
                }
                disk_left.retain(|&i| {
                    if !guard.check() {
                        return true;
                    }
                    found.clear();
                    segment.idmap.find_all(&ids[i], bloom::hash(&ids[i]), &mut found);
                    match found.iter().rev().find(|&&doc| segment.is_live(doc)) {
                        Some(&local) => {
                            at.push((i, s, local));
                            false
                        }
                        None => true,
                    }
                });
            }
            if !guard.check() {
                return Err(ReadIdsError::Cancelled);
            }
            // Все disk-сегменты состава проверены: оставшиеся идентификаторы
            // действительно отсутствуют, поэтому обычный `found:false` точен.
            return self.hydrate(out, at, guard);
        }

        match self.segs.locate_ids_ready_guarded(ids, Some(guard)) {
            Some(known) => {
                // Каталог уже вернул полный набор мест: покрытый глобальный
                // префикс и небольшой L0-хвост. Новейшее живое место выбирается
                // в том же порядке, что прежний обратный обход сегментов.
                for i in left {
                    if !guard.check() {
                        return Err(ReadIdsError::Cancelled);
                    }
                    let Some(&(s, local)) =
                        known[i].iter().rev().find(|&&(s, local)| self.segs.list[s].is_live(local))
                    else {
                        continue;
                    };
                    at.push((i, s, local));
                }
            }
            None if guard.fired() => return Err(ReadIdsError::Cancelled),
            None => return Err(ReadIdsError::DirectoryNotReady),
        }
        self.hydrate(out, at, guard)
    }

    /// Гидратация найденных point-read адресов. Вынесена из resolve, чтобы RAM
    /// overlay и generation-aware disk directory использовали один путь и одну
    /// отмену, не дублируя декодирование source/projection.
    fn hydrate(
        &self,
        mut out: Vec<Option<DocHit>>,
        at: Vec<(usize, usize, u32)>,
        guard: &svindb_core::work::Guard,
    ) -> Result<Vec<Option<DocHit>>, ReadIdsError> {
        if at.is_empty() {
            return Ok(out);
        }

        // Гидратация — через общий сборщик: он группирует по кускам содержимого
        // и распаковывает каждый однажды. Прежде каждое поле каждого документа
        // читалось отдельным `fetch`, то есть кусок распаковывался столько раз,
        // сколько из него взяли значений.
        let docs: Vec<u64> =
            at.iter().map(|&(_, s, local)| self.segs.base[s] + u64::from(local)).collect();
        let fetcher = self.segs.fetcher(FETCH_CACHE);
        let sources = self
            .source_fid
            .map(|fid| fetcher.many_guarded(&docs, fid, self.threads, Some(guard)))
            .unwrap_or_else(|| vec![None; docs.len()]);
        let projections = if self.source_only {
            vec![None; docs.len()]
        } else {
            self.projection_fid
                .map(|fid| fetcher.many_guarded(&docs, fid, self.threads, Some(guard)))
                .unwrap_or_else(|| vec![None; docs.len()])
        };
        // Source/projection уже содержат согласованную версию всех полей.
        // Читать после них каждый declared stored field — не «запасной путь»,
        // а повторная гидратация: 63 поля превращались в 63 обращения к store
        // на каждый point GET. Отдельные поля нужны только старым документам,
        // у которых нет ни source, ни projection.
        let fallback: Vec<(usize, u64)> = docs
            .iter()
            .copied()
            .enumerate()
            .filter(|(j, _)| {
                if self.source_only {
                    return false;
                }
                let source = sources.get(*j).and_then(Option::as_ref);
                let projection =
                    projections.get(*j).and_then(Option::as_ref).filter(|raw| !raw.is_empty());
                source.is_none() && projection.is_none()
            })
            .collect();
        let fallback_docs: Vec<u64> = fallback.iter().map(|(_, doc)| *doc).collect();
        let mut cells: Vec<Vec<String>> = vec![Vec::new(); at.len()];
        if !fallback_docs.is_empty() {
            for (fid, name) in self.names.iter().enumerate() {
                if !guard.check() {
                    break;
                }
                if name.is_empty() || internal_field(name) {
                    continue;
                }
                for ((slot, _), got) in fallback.iter().zip(fetcher.many_guarded(
                    &fallback_docs,
                    fid,
                    self.threads,
                    Some(guard),
                )) {
                    if !guard.check() {
                        break;
                    }
                    let Some(bytes) = got else { continue };
                    let raw = String::from_utf8_lossy(&bytes);
                    cells[*slot].push(format!(
                        "{}:{}",
                        json_string(name),
                        json_value(&raw, self.kinds.get(fid))
                    ));
                }
            }
        }
        for (j, ((i, _, _), (fallback, doc))) in
            at.iter().copied().zip(cells.into_iter().zip(docs)).enumerate()
        {
            if !guard.check() {
                return Err(ReadIdsError::Cancelled);
            }
            let source = sources
                .get(j)
                .and_then(|raw| raw.as_ref())
                .map(|raw| String::from_utf8_lossy(raw).into_owned());
            let projection = projections
                .get(j)
                .and_then(|raw| raw.as_ref())
                .filter(|raw| !raw.is_empty())
                .map(|raw| String::from_utf8_lossy(raw).into_owned())
                .or_else(|| source.clone());
            let fields = if self.source_only {
                String::new()
            } else {
                projection
                    .as_deref()
                    .and_then(|raw| fields_from_projection(raw, &self.names))
                    .unwrap_or_else(|| fallback.join(","))
            };
            // Наружу идёт **общий** номер, тот же, что у выдачи поиска: местный
            // номер сегмента у каждого свой, и связать по нему выдачу двух точек
            // было бы нечем.
            out[i] = Some(DocHit { doc: Some(doc), fields, source });
        }
        Ok(out)
    }
}

/// Почему point-read не смог вернуть обычный набор `found`/`found:false`.
///
/// Отдельный статус каталога принципиален: legacy fallback по всем сегментам
/// был корректным, но под production-нагрузкой превращал GET/MGET в скрытый
/// O(number_of_segments) скан. Теперь непустой индекс без готового directory
/// получает bounded retryable отказ и не занимает worker до конца обхода.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadIdsError {
    Cancelled,
    DirectoryNotReady,
}

/// Читает набор через готовый generation-aware ID directory.
///
/// ## Почему одного захода мало
///
/// Снимок состава — это `Arc` на **список** сегментов. Он закрепляет список,
/// порядок и начала номеров, но **не карту живых**: гашение правит биты прямо в
/// общем отображении тех же самых сегментов, на которые смотрит снимок. А
/// публикация идёт двумя шагами — сперва новый хранитель попадает в **новый**
/// список, которого у читателя нет, и только потом гаснет старая версия.
///
/// Значит читатель со снимком, взятым между шагами, видит старую версию уже
/// мёртвой, а новую — нигде. Ответ выходит `found:false` на документ, который
/// никуда не девался: ровно та потеря, ради закрытия которой точка и заведена.
/// Замерено на 150 целях и четырёх читателях: 106 промахов из 4693 чтений.
///
/// Прежде этого не случалось потому, что замок держался на **весь** обход, и
/// публикация в него не влезала. Теперь повторяются только короткие снимки, а
/// отсутствие каталога не маскируется линейным сканированием старого состава.
///
/// ## Почему повторяются только не найденные
///
/// Найденное безопасно: живая версия нашлась в закреплённом снимке. Опасен
/// только **ненайденный** ответ — он и бывает следом гонки. Поэтому не найденные
/// перечитываются заново под замком, где состав и карта живых согласованы между
/// собой по построению, — то есть ровно с тем инвариантом, что был до Ф5.7.
///
/// Цена честная: повторный снимок берётся только там, где что-то не нашлось, и
/// только по этому остатку. Если после bounded попыток каталог/поколение не
/// согласовались, запрос получает retryable отказ вместо скрытого O(segments).
fn read_ids(
    live: &Live,
    writer: Option<&Arc<Writer>>,
    declared: &Declared,
    ids: &[String],
    threads: usize,
    source_only: bool,
    guard: &svindb_core::work::Guard,
) -> Result<Vec<Option<DocHit>>, ReadIdsError> {
    let Some(view) = DocView::take(live, writer, declared, ids, threads, source_only, guard) else {
        return Err(ReadIdsError::Cancelled);
    };
    let mut mark = view.mark;
    let mut out = view.resolve(ids, guard)?;
    let Some(w) = writer else { return Ok(out) };
    if guard.fired() {
        return Err(ReadIdsError::Cancelled);
    }
    let mut missing: Vec<usize> =
        out.iter().enumerate().filter_map(|(i, x)| x.is_none().then_some(i)).collect();
    if missing.is_empty() {
        return Ok(out);
    }
    // Состав за время обхода не менялся — значит «не нашлось» это ответ, а не
    // след гонки, и второй заход не нужен. Без этой проверки набор из тысячи
    // заведомо отсутствующих идентификаторов брал бы замок каждый раз, то есть
    // ровно то удержание, которое Ф5.7 и снимала.
    if mark == Some(w.published.load(Ordering::Acquire)) {
        return Ok(out);
    }

    // Публикация могла попасть ровно в первый проход. Сначала повторяем снимок
    // optimistic: `DocView::take` держит ingest-lock только на копирование
    // буфера и Arc ReadView, а каталог/гидратация идут уже без него. На
    // searchable-live периоде это убирает 100-мс ступень отсутствующего GET:
    // прежний код сразу держал lock на всём втором segment walk.
    //
    // Три короткие попытки bounded. Если публикации всё же меняются быстрее
    // point-read, возвращаем retryable отказ: последний полный обход под
    // ingest-lock запрещён, потому что он блокировал бы писателя и скрывал
    // линейную стоимость GET/MGET.
    for _ in 0..3 {
        if guard.fired() {
            return Err(ReadIdsError::Cancelled);
        }
        let again: Vec<String> = missing.iter().map(|&i| ids[i].clone()).collect();
        let Some(retry) =
            DocView::take(live, writer, declared, &again, threads, source_only, guard)
        else {
            return Err(ReadIdsError::Cancelled);
        };
        mark = retry.mark;
        let resolved = retry.resolve(&again, guard)?;
        let mut left = Vec::with_capacity(missing.len());
        for (original, got) in missing.into_iter().zip(resolved) {
            match got {
                Some(hit) => out[original] = Some(hit),
                None => left.push(original),
            }
        }
        missing = left;
        if missing.is_empty() || mark == Some(w.published.load(Ordering::Acquire)) {
            return Ok(out);
        }
    }

    if guard.fired() {
        return Err(ReadIdsError::Cancelled);
    }
    // Больше не берём ingest-lock ради последнего полного обхода. Если три
    // коротких поколения подряд менялись быстрее point-read, повтор безопаснее
    // принять клиентом с bounded 503: построение каталога и гидратация никогда
    // не должны ждать за писателем под его lock.
    Err(ReadIdsError::DirectoryNotReady)
}

/// `POST /_bulk` и `POST /_put` на координаторе.
fn serve_coord_write(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    fan: &coord::Fanout,
    keep: bool,
) {
    // Тело на языке Elasticsearch переводится в родное **до** раскладки, а не
    // читается вторым разборщиком: раскладке нужен `id` каждого документа, и
    // искать его двумя разными способами значит однажды разойтись.
    let translated;
    let body: &[u8] = if esbulk::looks_like_ndjson(&req.body) {
        translated = match es_batch(&req.body, route.index.as_deref()) {
            Ok(b) => b,
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                return http::respond_with(stream, 400, &payload, keep);
            }
        };
        &translated
    } else {
        &req.body
    };
    let Some(split) = coord::split_with(body, "documents", fan) else {
        let payload = format!(
            "{{\"error\":{}}}",
            json_string("тело не разобралось: ожидается {\"documents\":[{\"id\":…}]}")
        );
        return http::respond_with(stream, 400, &payload, keep);
    };
    let path = if route.endpoint == route::Endpoint::Put { "/_put" } else { "/_bulk" };
    // Запись — в primary шарда, а не в очередную копию по кругу: рассылает
    // копиям только он, и реплика, получив запись напрямую, применит её у себя
    // и не расскажет никому.
    let picks = fan.writes_to();
    let got = fan.fan_each_once(&picks, path, &split.bodies);
    let (code, payload) = coord::merge_writes(&got.replies, &split);
    http::respond_with(stream, code, &payload, keep)
}

/// `POST|PUT|DELETE /_doc/{id}` на координаторе: один шард, родное тело.
///
/// ## Почему запрос переписывается, а не пересылается как есть
///
/// Исходящий у веера один — `POST` с телом. Держать второй ради `DELETE` было
/// бы лишним каналом, а `/_doc/{id}` и так выражается через уже имеющиеся
/// точки **точно**: у шарда `POST /_doc/{id}` кончается тем же `add`, что и
/// `/_bulk`, а `DELETE /_doc/{id}` — тем же `delete`, что и `/_delete`. Перевод
/// делает `es_one` — тот самый, который вызвал бы шард, а не второй разборщик.
fn serve_coord_one(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    fan: &coord::Fanout,
    keep: bool,
) {
    let n = fan.shards.len();
    let body_routing = serde_json::from_slice::<serde_json::Value>(&req.body)
        .ok()
        .and_then(|value| value.get("peer_id").cloned())
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value),
            serde_json::Value::Number(value) => Some(value.to_string()),
            _ => None,
        });
    let mine = fan.owner(&route.rest, req.header("x-svindb-routing").or(body_routing.as_deref()));
    let (path, ask) = if req.method == "DELETE" {
        ("/_delete", serde_json::json!({ "ids": [route.rest.clone()] }).to_string())
    } else {
        match es_one(&req.body, &route.rest) {
            Ok(b) => ("/_bulk", String::from_utf8_lossy(&b).into_owned()),
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                return http::respond_with(stream, 400, &payload, keep);
            }
        }
    };
    let mut bodies: Vec<Option<String>> = (0..n).map(|_| None).collect();
    if let Some(slot) = bodies.get_mut(mine) {
        *slot = Some(ask);
    }
    let picks = fan.writes_to();
    let got = fan.fan_each_once(&picks, path, &bodies);
    let split = coord::Split {
        bodies: Vec::new(),
        places: (0..n).map(|i| if i == mine { vec![0] } else { Vec::new() }).collect(),
    };
    let (code, payload) = coord::merge_writes(&got.replies, &split);
    http::respond_with(stream, code, &payload, keep)
}

/// `POST /_delete` на координаторе: список идентификаторов по своим шардам.
fn serve_coord_delete(stream: &mut wire::Wire, body: &[u8], fan: &coord::Fanout, keep: bool) {
    let Some(split) = coord::split_with(body, "ids", fan) else {
        let payload = format!(
            "{{\"error\":{}}}",
            json_string("тело не разобралось: ожидается {\"ids\":[\"a\",\"b\"]}")
        );
        return http::respond_with(stream, 400, &payload, keep);
    };
    let picks = fan.writes_to();
    let got = fan.fan_each_once(&picks, "/_delete", &split.bodies);
    let (code, payload) = coord::merge_writes(&got.replies, &split);
    http::respond_with(stream, code, &payload, keep)
}

/// `POST /_flush` на координаторе: всем шардам, и «да» только если всем «да».
fn serve_coord_flush(stream: &mut wire::Wire, fan: &coord::Fanout, keep: bool) {
    let n = fan.shards.len();
    let bodies: Vec<Option<String>> = (0..n).map(|_| Some(String::from("{}"))).collect();
    let picks = fan.writes_to();
    let got = fan.fan_each_once(&picks, "/_flush", &bodies);
    let mut bad: Vec<String> = Vec::new();
    let mut busy = 0usize;
    for (i, r) in got.replies.iter().enumerate() {
        match r {
            Ok(b) if b.contains("\"flushed\":true") => {}
            // «Сброс уже идёт» — не отказ, и превращать его в отказ нельзя:
            // клиент повторил бы запрос, который и так выполняется. Ответ тот
            // же, что у одиночного узла в том же положении.
            Ok(b) if b.contains("\"in_progress\":true") => busy += 1,
            Ok(b) => bad.push(format!("{{\"shard\":{i},\"reply\":{}}}", json_string(b))),
            Err(e) => bad.push(format!("{{\"shard\":{i},\"reply\":{}}}", json_string(e))),
        }
    }
    if !bad.is_empty() {
        let payload = format!("{{\"flushed\":false,\"failed\":[{}]}}", bad.join(","));
        return http::respond_with(stream, 503, &payload, keep);
    }
    if busy > 0 {
        // Ровно так же, как одиночный узел: `202`, а не `200`. Разница несущая —
        // после `200` записанное видно, после `202` ещё нет.
        let payload = format!("{{\"flushed\":false,\"in_progress\":true,\"shards\":{busy}}}");
        return http::respond_with(stream, 202, &payload, keep);
    }
    http::respond_with(stream, 200, "{\"flushed\":true}", keep)
}

/// Разбирает `lo-hi` из `--shards=адрес@lo-hi`. Мусор — не полоса, а ошибка.
fn parse_band(s: &str) -> Option<(u64, u64)> {
    let (lo, hi) = s.trim().split_once('-')?;
    Some((lo.trim().parse().ok()?, hi.trim().parse().ok()?))
}

/// Договор подтверждения этого узла. У одинокого — `one`: он и есть все.
fn node_ack(node: &Node) -> svindb_core::cluster::Ack {
    node.repl.as_ref().map_or(svindb_core::cluster::Ack::One, |p| p.ack)
}

/// Рассылает держателям **уже применённое** и говорит, хватило ли подтверждений.
///
/// Возвращает `None` у одинокого узла: там подтверждение значит ровно то же,
/// что и всегда, и приписывать к ответу договор, которого нет, — врать.
///
/// Тело уходит **дословно тем же**, каким его разобрал primary, а не пересобранным
/// из разобранных документов: пересборка — это второй способ понять один запрос,
/// и однажды он разойдётся с первым молча.
fn replicate_effect(
    node: &Node,
    op: &str,
    body: Option<&[u8]>,
    deletes: &[String],
) -> Option<replicate::Acked> {
    let peers = node.repl.as_ref()?;
    let mut rec = format!("{{\"op\":{}", json_string(op));
    if let Some(b) = body {
        // Экранирование одним проходом по байтам — дешевле полного разбора,
        // а разберёт его реплика ровно тем же `parse_docs`, что и primary.
        rec.push_str(&format!(",\"body\":{}", json_string(&String::from_utf8_lossy(b))));
    }
    if !deletes.is_empty() {
        let ids: Vec<String> = deletes.iter().map(|x| json_string(x)).collect();
        rec.push_str(&format!(",\"delete\":[{}]", ids.join(",")));
    }
    rec.push('}');
    Some(peers.send(&rec))
}

/// `POST /_reshard` — сузить полосу ячеек узла.
///
/// ## Зачем эта точка вообще нужна
///
/// Деление шарда — это две половины полосы у двух узлов. Скопировать данные
/// можно снимком (Ф6.3): снял с шарда, восстановил соседом — и у обоих полный
/// корпус. Дальше каждому надо оставить **свою** половину, и вот это без
/// отдельной точки не выражается: гашение принимает список идентификаторов, а
/// здесь их миллионы и клиент их не знает.
///
/// Узел знает. Он читает свои идентификаторы, считает ячейку каждого и гасит
/// то, что вне полосы. Никакого сканирования по сети, никакого перечисления
/// корпуса клиентом.
///
/// ## Почему сначала сброс, а потом счёт
///
/// Принятое, но не опубликованное лежит в буфере приёма, а не в сегментах;
/// обход сегментов его не увидит. Не сбросив, узел оставил бы у себя чужие
/// документы — молча, и обнаружилось бы это тем, что два шарда отдают один и
/// тот же документ.
///
/// ## Повторяемость
///
/// Второй вызов с той же полосой не находит ничего: гасить уже нечего.
/// Оборвался на середине — повтор доводит до конца. Поэтому же он годится как
/// `dry_run`: тот же обход, но без гашения.
fn serve_reshard(
    stream: &mut wire::Wire,
    req: &http::Request,
    w: &Arc<Writer>,
    live: &Live,
    node: &Node,
    keep: bool,
) {
    let raw: serde_json::Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(e) => {
            let payload =
                format!("{{\"error\":{}}}", json_string(&format!("тело не разобралось: {e}")));
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    let (Some(lo), Some(hi)) =
        (raw.get("keep_lo").and_then(|x| x.as_u64()), raw.get("keep_hi").and_then(|x| x.as_u64()))
    else {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"нужны keep_lo и keep_hi — полоса ячеек [lo, hi)\"}",
            keep,
        );
    };
    let slots = svindb_core::shards::SLOTS;
    if hi <= lo || hi > slots {
        let msg = format!("полоса [{lo}, {hi}) пуста, вывернута или выходит за {slots} ячеек");
        return http::respond_with(
            stream,
            400,
            &format!("{{\"error\":{}}}", json_string(&msg)),
            keep,
        );
    }
    let dry = raw.get("dry_run").and_then(|x| x.as_bool()).unwrap_or(false);

    // Сброс **до** обхода: см. разбор выше.
    if !dry {
        let wait = node.live_settings.read().map(|s| s.flush_wait_ms).unwrap_or(5_000);
        match w.request_flush(Duration::from_millis(wait)) {
            Ok(true) => {}
            Ok(false) => {
                return http::respond_with(
                    stream,
                    202,
                    "{\"reshard\":false,\"in_progress\":true,\"error\":\"сброс не закончился; повторите\"}",
                    keep,
                )
            }
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                return http::respond_with(stream, 503, &payload, keep);
            }
        }
    }

    // Обход своих идентификаторов. Пачками, а не одним списком: корпус может не
    // поместиться в память, а гашение всё равно идёт пачками.
    const BATCH: usize = 4096;
    let segs = { live.read().expect("набор сегментов").clone() };
    let mut kept = 0usize;
    let mut doomed: Vec<String> = Vec::new();
    let mut removed = 0usize;
    let mut failed: Option<String> = None;
    for ix in segs.list.iter() {
        for doc in 0..ix.docs as u32 {
            if !ix.is_live(doc) {
                continue;
            }
            let Some(id) = ix.idmap.id_at(doc) else { continue };
            let slot = svindb_core::shards::slot_of(&id);
            if slot >= lo && slot < hi {
                kept += 1;
                continue;
            }
            doomed.push(id);
            if !dry && doomed.len() >= BATCH {
                match w.delete(&doomed) {
                    Ok(n) => removed += n,
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
                doomed.clear();
            }
        }
        if failed.is_some() {
            break;
        }
    }
    if dry {
        let payload = format!(
            "{{\"dry_run\":true,\"keep_lo\":{lo},\"keep_hi\":{hi},\"kept\":{kept},\"would_remove\":{}}}",
            doomed.len()
        );
        return http::respond_with(stream, 200, &payload, keep);
    }
    if failed.is_none() && !doomed.is_empty() {
        match w.delete(&doomed) {
            Ok(n) => removed += n,
            Err(e) => failed = Some(e),
        }
    }
    node.metrics.deleted(removed as u64);
    if let Some(e) = failed {
        // Половина погашена, половина нет — и об этом надо сказать числом.
        // Повтор доведёт до конца: обход идёт по принадлежности, а не по списку.
        let payload = format!(
            "{{\"error\":{},\"removed\":{removed},\"kept\":{kept},\"partial\":true}}",
            json_string(&e)
        );
        return http::respond_with(stream, 503, &payload, keep);
    }
    let payload =
        format!("{{\"reshard\":true,\"keep_lo\":{lo},\"keep_hi\":{hi},\"kept\":{kept},\"removed\":{removed}}}");
    http::respond_with(stream, 200, &payload, keep)
}

/// Приём записи от primary этого шарда.
///
/// ## Что здесь **не** делается и почему
///
/// * **Не разбираются условия замены.** Их рассудил primary, и второй разбор на
///   реплике дал бы двум узлам возможность рассудить одно условие по-разному —
///   расхождение, о котором оба ответили бы «принято».
/// * **Не проверяется строгость приёма.** Она отвергла бы то, что primary уже
///   принял и подтвердил клиенту; копии разошлись бы, и виноватой выглядела бы
///   реплика.
/// * **Ничего не пересылается дальше.** Реплика реплики — это уже другая
///   топология, и заводить её молча нельзя.
fn serve_replicate(
    stream: &mut wire::Wire,
    req: &http::Request,
    w: &Arc<Writer>,
    node: &Node,
    keep: bool,
) {
    let Some(terms) = node.terms.as_ref() else {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"узел не принимает репликацию: приёма нет\"}",
            keep,
        );
    };
    let raw: serde_json::Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(e) => {
            let payload = format!(
                "{{\"error\":{}}}",
                json_string(&format!("тело записи репликации не разобралось: {e}"))
            );
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    let (Some(term), Some(at)) =
        (raw.get("term").and_then(|x| x.as_u64()), raw.get("at").and_then(|x| x.as_u64()))
    else {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"запись репликации без term/at\"}",
            keep,
        );
    };
    // Зонд: `at = 0` не бывает у настоящей записи (счёт идёт с единицы), и потому
    // служит вопросом «где ты». Нужен primary при подъёме: свой исходящий номер
    // он не помнит, а начать с единицы значило бы уйти в разрыв со всеми живыми
    // копиями разом. Терм при этом проверяется — так primary узнаёт о своём
    // смещении **до** первой записи, а не после.
    if at == 0 {
        let seen = terms.seen();
        if term < seen.term {
            let msg = format!("устаревший терм {term}: узел уже видел {}", seen.term);
            let payload = format!(
                "{{\"error\":{},\"seen_term\":{},\"upto\":{}}}",
                json_string(&msg),
                seen.term,
                seen.upto
            );
            return http::respond_with(stream, 409, &payload, keep);
        }
        let payload = format!(
            "{{\"probe\":true,\"term\":{},\"upto\":{}}}",
            seen.term,
            if seen.term == term { seen.upto } else { 0 }
        );
        return http::respond_with(stream, 200, &payload, keep);
    }
    // Терм — **до** применения. Приняв сначала и спросив потом, узел успел бы
    // изменить данные по указанию того, кого уже сместили.
    // Терм — **до** применения, отметка применённого — **после**. Разбор, почему
    // это две операции, а не одна, — у `Terms::admit`.
    let fresh = match terms.admit(term, at) {
        Ok(s) => s,
        Err(e) => {
            let payload = format!(
                "{{\"error\":{},\"seen_term\":{},\"upto\":{}}}",
                json_string(&e.say()),
                terms.seen().term,
                terms.seen().upto
            );
            return http::respond_with(stream, e.code(), &payload, keep);
        }
    };
    // Повтор той же записи принимается и **не применяется**: отправитель
    // повторяет потому, что не знает, дошло ли, а второе применение `add` — это
    // сдвоенный документ, которого потом ничем не отличить от честного.
    if !fresh {
        let payload = format!("{{\"applied\":false,\"repeat\":true,\"term\":{term},\"at\":{at}}}");
        return http::respond_with(stream, 200, &payload, keep);
    }
    let op = raw.get("op").and_then(|x| x.as_str()).unwrap_or("add");
    if !matches!(op, "add" | "put" | "delete") {
        let payload = format!("{{\"error\":{}}}", json_string("неизвестная операция репликации"));
        return http::respond_with(stream, 400, &payload, keep);
    }
    let mut added = 0usize;
    let mut deleted = 0usize;
    if let Some(body) = raw.get("body").and_then(|x| x.as_str()) {
        let plan = w.plan();
        let (docs, _report) = match parse_docs(body.as_bytes(), &plan) {
            Ok(x) => x,
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                return http::respond_with(stream, 400, &payload, keep);
            }
        };
        // Реплицированная запись всегда каноническая по `id`, включая bulk.
        // И primary в режиме репликации применяет её как upsert: иначе падение
        // копии после локального применения, но до `Terms::done`, заставило бы
        // primary повторить `add` и создало вторую живую версию. `put` можно
        // безопасно повторить в любом таком окне, а состав primary и копии при
        // этом остаётся одинаковым.
        let outcome = w.put_if(docs, &[]).map(|r| r.map(|(a, _)| a));
        match outcome {
            Ok(Ok(n)) => added = n,
            Ok(Err(_)) => {
                // Условий не передавали — конфликту взяться неоткуда. Если он
                // всё же случился, молчать нельзя: это расхождение договора.
                return http::respond_with(
                    stream,
                    500,
                    "{\"error\":\"репликация получила конфликт без условий\"}",
                    keep,
                );
            }
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                return http::respond_with(stream, 503, &payload, keep);
            }
        }
    }
    if let Some(ids) = raw.get("delete").and_then(|x| x.as_array()) {
        let ids: Vec<String> = ids.iter().filter_map(|x| x.as_str().map(String::from)).collect();
        if !ids.is_empty() {
            match w.delete(&ids) {
                Ok(n) => deleted = n,
                Err(e) => {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    return http::respond_with(stream, 503, &payload, keep);
                }
            }
        }
    }
    node.metrics.added(added as u64);
    node.metrics.deleted(deleted as u64);
    node.metrics.ingest_bytes(req.body.len() as u64);
    svindb_core::fail::point("replication.after_apply_before_done");
    // Применено — теперь и только теперь номер объявляется применённым.
    if let Err(e) = terms.done(term, at) {
        let payload = format!(
            "{{\"error\":{},\"applied\":true,\"note\":\"запись применена, но отметка не легла\"}}",
            json_string(&e.say())
        );
        return http::respond_with(stream, e.code(), &payload, keep);
    }
    let payload = format!(
        "{{\"applied\":true,\"added\":{added},\"deleted\":{deleted},\"term\":{term},\"at\":{at}}}"
    );
    http::respond_with(stream, 200, &payload, keep)
}

/// `POST /_mget` на координаторе: разложить по шардам и собрать по местам.
///
/// ## Почему собирается по местам, а не по порядку ответов
///
/// Клиент сопоставляет ответ со своим списком по номеру. Шарды отвечают
/// вразнобой — кто быстрее, — и сложи мы ответы по готовности, каждый документ
/// оказался бы подписан чужим идентификатором. Поэтому места считаются при
/// раскладке и держатся до сборки.
fn serve_coord_mget(stream: &mut wire::Wire, body: &[u8], fan: &coord::Fanout, keep: bool) {
    let Some(split) = coord::split_with(body, "ids", fan) else {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"тело разбирается как {\\\"ids\\\":[\\\"a\\\",\\\"b\\\"]}\"}",
            keep,
        );
    };
    let total: usize = split.places.iter().map(|p| p.len()).sum();
    let picks = fan.pick();
    let got = fan.fan_each(&picks, "/_mget", &split.bodies);
    let payload = coord::merge_docs(&got.replies, &split, total);
    http::respond_with(stream, 200, &payload, keep)
}

/// `GET /_doc/{id}` на координаторе: routed point read либо bounded fan-out.
fn serve_coord_doc(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    fan: &coord::Fanout,
    keep: bool,
) {
    let id = route.rest.as_str();
    if id.is_empty() {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"пустой идентификатор документа\"}",
            keep,
        );
    }
    let n = fan.shards.len();
    let mut bodies: Vec<Option<String>> = (0..n).map(|_| None).collect();
    let ask = serde_json::json!({ "ids": [id] }).to_string();
    let routed = req.header("x-svindb-routing").map(|routing| fan.owner(id, Some(routing)));
    match routed {
        Some(mine) => bodies[mine] = Some(ask),
        None => bodies.iter_mut().for_each(|body| *body = Some(ask.clone())),
    }
    let picks = fan.pick();
    let got = fan.fan_each(&picks, "/_mget", &bodies);
    let mut failures = 0usize;
    let found = got
        .replies
        .iter()
        .filter_map(|reply| match reply {
            Ok(body) => {
                let found = serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|v| v.get("docs").and_then(|d| d.as_array()).cloned())
                    .and_then(|d| d.first().cloned());
                found.filter(|one| one.get("found").and_then(|f| f.as_bool()) == Some(true))
            }
            Err(_) => {
                failures += 1;
                None
            }
        })
        .next();
    match found {
        Some(one) => http::respond_with(stream, 200, &one.to_string(), keep),
        None if failures > 0 => http::respond_with(
            stream,
            503,
            &format!(
                "{{\"error\":\"{failures} shard не ответили; отсутствие документа не доказано\",\"retryable\":true}}"
            ),
            keep,
        ),
        None => http::respond_with(
            stream,
            404,
            &format!("{{\"id\":{},\"found\":false}}", json_string(id)),
            keep,
        ),
    }
}

/// `POST /_mget` — чтение набора документов по списку идентификаторов.
///
/// ## Зачем отдельная точка, если есть `GET /_doc/{id}`
///
/// Control plane читает не документ, а **набор**: пул аккаунтов, задания
/// панели, связку «очередь и её попытка». Столько же отдельных `GET` берут
/// снимок буфера и состава каждый свой, и публикация, случившаяся между ними,
/// собирает ответ из разных поколений индекса: половина набора до замены,
/// половина после. Здесь снимок один на весь список.
///
/// Это не транзакция и не читающий snapshot всего индекса: набор согласован
/// между собой на момент запроса, а параллельная запись, легшая после снимка,
/// в него не попадёт — как и у всякого читателя.
#[allow(clippy::too_many_arguments)]
fn serve_mget(
    stream: &mut wire::Wire,
    body: &[u8],
    live: &Live,
    writer: Option<&Arc<Writer>>,
    declared: &Declared,
    threads: usize,
    guard: &svindb_core::work::Guard,
    keep: bool,
) {
    let Some(ids) = parse_ids(body) else {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"тело разбирается как {\\\"ids\\\":[\\\"a\\\",\\\"b\\\"]}\"}",
            keep,
        );
    };
    if let Err(e) = no_empty_ids(&ids) {
        return http::respond_with(
            stream,
            400,
            &format!("{{\"error\":{}}}", json_string(&e)),
            keep,
        );
    }
    // Предел тот же, что у верхушки поиска: список без предела превращает одну
    // строку тела в обход всех сегментов столько раз, сколько в ней запятых.
    if ids.len() > MAX_MGET {
        return http::respond_with(
            stream,
            400,
            &format!("{{\"error\":\"идентификаторов {} при пределе {MAX_MGET}\"}}", ids.len()),
            keep,
        );
    }

    // Отсутствующие остаются в выдаче с `found:false`, а не выпадают из неё:
    // клиент сопоставляет ответ со своим списком по порядку, и молчаливый
    // пропуск сдвинул бы ему все последующие.
    let resolved = match read_ids(live, writer, declared, &ids, threads, false, guard) {
        Ok(resolved) => resolved,
        Err(ReadIdsError::Cancelled) => {
            return http::respond_with(
                stream,
                503,
                &format!(
                    "{{\"error\":\"mget не завершён в заданный срок\",\"retryable\":true,\"reason\":{}}}",
                    json_string(guard.reason().name())
                ),
                keep,
            )
        }
        Err(ReadIdsError::DirectoryNotReady) => {
            return http::respond_with(
                stream,
                503,
                "{\"error\":\"generation-aware ID directory ещё не готов\",\"retryable\":true,\"reason\":\"id_directory_not_ready\"}",
                keep,
            )
        }
    };
    let mut docs = Vec::with_capacity(ids.len());
    for (got, id) in resolved.into_iter().zip(ids.iter()) {
        if !guard.check() {
            return http::respond_with(
                stream,
                503,
                &format!(
                    "{{\"error\":\"mget serialization не завершена\",\"retryable\":true,\"reason\":{}}}",
                    json_string(guard.reason().name())
                ),
                keep,
            );
        }
        docs.push(match got {
            Some(found) => found.payload(id, false),
            None => format!("{{\"id\":{},\"found\":false}}", json_string(id)),
        });
    }
    let payload = format!("{{\"docs\":[{}]}}", docs.join(","));
    http::respond_with(stream, 200, &payload, keep)
}

/// Число логических searchable tiers для control-plane.
///
/// Несколько frozen generations — самостоятельные immutable источники для
/// matcher, но наружный счётчик `segments` исторически означает физические
/// сегменты плюс один active-memory tier. Детальное число frozen поколений
/// публикуется отдельно в metrics; смешивать его с L0 debt нельзя.
fn reported_segment_count(segs: &Segments) -> usize {
    let disk = segs.list.iter().filter(|segment| !segment.src.is_memory()).count();
    disk + usize::from(segs.list.iter().any(|segment| segment.src.is_memory()))
}

/// `GET /_stats` — чем узел является.
///
/// Отдельно от `/_health`: тот спрашивают на каждой проверке живости, и класть
/// в него обход каталога значило бы платить за размер индекса много раз в
/// секунду. Здесь же собрано то, что клиенту нужно один раз при подключении:
/// версия, режим, схема, место.
fn serve_stats(
    stream: &mut wire::Wire,
    live: &Live,
    writer: Option<&Arc<Writer>>,
    node: &Node,
    threads: usize,
    keep: bool,
) {
    let segs = { live.read().expect("набор сегментов").clone() };
    let dead: usize = segs.list.iter().map(|s| s.dead()).sum();
    // Атомарный `_source` и его необязательная проекция — детали формата,
    // а не поля пользовательской схемы. Если учитывать их здесь, один и тот
    // же mapping внезапно «шире» на два поля только из-за поддержки lossless
    // чтения, а лимиты/дашборды оператора начинают лгать.
    let fields = schema_of(&node.dir)
        .map(|decls| decls.into_iter().filter(|decl| !internal_field(&decl.path)).count())
        .unwrap_or(0);
    // Шарды выходят вложенными списками, а не плоским: внешний уровень задаёт
    // номера шардов, внутренний — равноправные реплики одного. Склеить их в
    // один список значило бы выдать три реплики одного шарда за три шарда.
    let shards: Vec<String> = node
        .fanout
        .as_ref()
        .map(|f| {
            f.shards
                .iter()
                .map(|replicas| {
                    let r: Vec<String> = replicas.iter().map(|a| json_string(a)).collect();
                    format!("[{}]", r.join(","))
                })
                .collect()
        })
        .unwrap_or_default();
    let free = host::free_bytes(&node.dir).map(|n| n.to_string()).unwrap_or_else(|| "null".into());
    let governor = resources();
    let source_cache_stats = node.source_cache.stats();
    let oldest_query = oldest_query_json();
    let workload_json = format!(
        "{{\"search_cpu_units\":{},\"interactive_max_units\":{},\"analytics_max_units\":{},\
          \"latency_reserve_bytes\":{},\"wal_replay_bytes\":{},\"max_frozen_memtables\":{},\
          \"background_io_bytes_per_second\":{},\"background_io_burst_bytes\":{},\
          \"background_io_max_inflight\":{},\"compaction_policy\":\"query_first\"}}",
        governor.cpu_max,
        governor.interactive_max_units,
        governor.analytics_max_units,
        governor.latency_reserve_bytes,
        governor.wal_replay_bytes,
        governor.max_frozen_memtables,
        governor.background_io_bytes_per_second,
        governor.background_io_burst_bytes,
        governor.background_io_max_inflight,
    );
    let workload_json = workload_json
        .strip_suffix('}')
        .map(|head| {
            format!(
                "{head},\"source_cache_capacity_bytes\":{},\"source_cache_bytes\":{},\
                 \"source_cache_entries\":{}}}",
                source_cache_stats.capacity_bytes,
                source_cache_stats.bytes,
                source_cache_stats.entries
            )
        })
        .unwrap_or(workload_json);
    // Терм и номер применённого — наружу.
    //
    // Не украшение: отставшую копию иначе не отличить от целой ничем, кроме
    // выдачи, а по выдаче разницу видит уже тот, кто получил неполный ответ.
    // Здесь она видна тому, кто следит за узлами.
    let repl = match (node.terms.as_ref(), node.repl.as_ref()) {
        (None, _) => String::from("null"),
        (Some(t), peers) => {
            let seen = t.seen();
            let role = if peers.is_some() { "primary" } else { "holder" };
            let of = peers.map_or(1, |p| p.holders());
            let ack = peers.map_or("one", |p| p.ack.name());
            format!(
                "{{\"role\":{},\"term\":{},\"applied_upto\":{},\"holders\":{of},\"ack\":{}}}",
                json_string(role),
                seen.term,
                seen.upto,
                json_string(ack)
            )
        }
    };
    let payload = format!(
        "{{\"version\":{},\"revision\":{},\"build_target\":{},\"source_date_epoch\":{},\"protocol_version\":1,\
         \"capabilities\":[\"durable_ack\",\"linearizable_mget\",\"conditional_put\",\
         \"conditional_mutate\",\"snapshot_restore\",\"lossless_document_source\",\
         \"sparse_source_fields\",\"scalar_multivalue_fields\",\"source_only_objects\",\
         \"hierarchical_nested_slots\",\"case_sensitive_keyword\",\
         \"numeric_term_queries\",\"strict_query_parameters\"],\
         \"uptime_seconds\":{:.3},\"dir\":{},\
         \"mode\":{{\"read_only\":{},\"maintain\":{},\"wal\":{},\"coordinator\":{}}},\
         \"shards\":[{}],\"threads\":{threads},\
         \"schema\":{{\"fields\":{fields},\"source_mode\":{},\"mapping_transforms\":{}}},\
         \"docs\":{},\"dead\":{dead},\"segments\":{},\
         \"disk\":{{\"index_bytes\":{},\"free_bytes\":{free}}},\
         \"replication\":{repl},\
         \"auth\":{{\"enabled\":{}}}}}",
        json_string(host::version()),
        json_string(host::revision()),
        json_string(&host::target()),
        json_string(host::build_epoch()),
        host::uptime_seconds(),
        json_string(&node.dir),
        writer.is_none(),
        node.maintaining,
        writer.is_some_and(|w| w.wal.is_some()),
        node.fanout.is_some(),
        shards.join(","),
        json_string(node.declared.mapping.source_mode().name()),
        node.declared.mapping.transformed(),
        segs.docs(),
        reported_segment_count(&segs),
        host::index_bytes(&node.dir),
        node.guard.auth.on(),
    );
    let payload = payload
        .strip_suffix('}')
        .map(|head| {
            format!(
                "{head},\"workload\":{},\"point_read\":{{\"id_directory_ready\":{}}},\
                 \"oldest_query\":{oldest_query}}}",
                workload_json,
                segs.id_directory_ready()
            )
        })
        .unwrap_or(payload);
    http::respond_with(stream, 200, &payload, keep)
}

/// `GET /_inspect` — metadata-only снимок состава.
///
/// Глубокий разбор словарей/postings/positions остаётся у `svindb inspect` как
/// явная операторская операция. HTTP-точка не декодирует содержимое сегментов:
/// под mixed workload такой «осмотр» раньше был не control-plane запросом, а
/// скрытым полноиндексным scan, способным занять ядро на минуты.
fn serve_inspect(
    stream: &mut wire::Wire,
    node: &Node,
    live: &Live,
    guard: &svindb_core::work::Guard,
    keep: bool,
) {
    let segs = live.read().expect("набор сегментов").clone();
    let mut disk_segments = 0usize;
    let mut disk_docs = 0usize;
    let mut live_segments = 0usize;
    let mut live_docs = 0usize;
    let mut dead = 0usize;
    for segment in &segs.list {
        if !guard.check() {
            return http::respond_with(
                stream,
                503,
                &format!(
                    "{{\"error\":\"inspect отменён до завершения metadata snapshot\",\
                     \"retryable\":true,\"reason\":{}}}",
                    json_string(guard.reason().name())
                ),
                keep,
            );
        }
        if segment.src.is_memory() {
            live_segments += 1;
            live_docs = live_docs.saturating_add(segment.docs);
        } else {
            disk_segments += 1;
            disk_docs = disk_docs.saturating_add(segment.docs);
            dead = dead.saturating_add(segment.dead());
        }
    }
    let payload = format!(
        "{{\"mode\":\"metadata\",\"dir\":{},\"docs\":{},\"segments\":{},\
         \"dead\":{},\"index_bytes\":{},\"free_bytes\":{},\
         \"disk\":{{\"segments\":{disk_segments},\"docs\":{disk_docs}}},\
         \"live\":{{\"segments\":{live_segments},\"docs\":{live_docs}}},\
         \"deep_inspect\":\"svindb inspect --json <dir>\"}}",
        json_string(&node.dir),
        disk_docs,
        disk_segments,
        dead,
        host::index_bytes(&node.dir),
        host::free_bytes(&node.dir).map(|n| n.to_string()).unwrap_or_else(|| "null".into()),
    );
    http::respond_with(stream, 200, &payload, keep)
}

fn serve_settings(stream: &mut wire::Wire, req: &http::Request, node: &Node, keep: bool) -> bool {
    use svindb_core::settings;
    let show = |s: &settings::Settings, applied: &str| {
        let mut doc = s.to_json();
        if let svindb_core::json::Json::Obj(kv) = &mut doc {
            kv.push(("applied".into(), svindb_core::json::Json::Str(applied.into())));
        }
        doc.to_compact()
    };
    match req.method.as_str() {
        "GET" => {
            let s = *node.live_settings.read().expect("настройки");
            http::respond_with(stream, 200, &show(&s, "все"), keep);
        }
        "PUT" | "POST" => {
            let _settings_lane = match node.settings_lane.lock() {
                Ok(guard) => guard,
                Err(_) => {
                    http::respond_with(
                        stream,
                        503,
                        "{\"error\":\"полоса изменения настроек повреждена\"}",
                        keep,
                    );
                    return true;
                }
            };
            let body = String::from_utf8_lossy(&req.body).to_string();
            let doc = match svindb_core::json::parse(&body) {
                Ok(d) => d,
                Err(e) => {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    http::respond_with(stream, 400, &payload, keep);
                    return true;
                }
            };
            let mut next = *node.live_settings.read().expect("настройки");
            let before = next;
            if let Err(e) = next.patch(&doc) {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                http::respond_with(stream, 400, &payload, keep);
                return true;
            }
            if let Some(budget) = node.ingest_budget.as_ref() {
                if let Err(e) = budget.validate(Some(&node.dir), next.flush_mb) {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    http::respond_with(stream, 409, &payload, keep);
                    return true;
                }
            }
            if let Err(e) = settings::save(&node.dir, &next) {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                http::respond_with(stream, 503, &payload, keep);
                return true;
            }
            *node.live_settings.write().expect("настройки") = next;
            if let Some(writer) = node.writer.as_ref() {
                writer.drop_flushed_cache.store(next.drop_flushed_page_cache, Ordering::Relaxed);
            }
            // Потолок документов на сегмент — единственное, что не применяется
            // немедленно: уже набранное буфером остаётся набранным. Ответ об
            // этом говорит прямо, чтобы никто не гадал, почему первый сегмент
            // после правки вышел прежнего размера.
            let mut applied = "немедленно";
            if next.flush_docs != before.flush_docs {
                if let Some(w) = node.writer.as_ref() {
                    if let Ok(mut ix) = w.ingest.lock() {
                        ix.set_per_segment(next.flush_docs);
                    }
                }
                applied = "немедленно, кроме flush_docs — он со следующего сегмента";
            }
            if next.live_max_docs != before.live_max_docs {
                if let Some(w) = node.writer.as_ref() {
                    if let Ok(mut ix) = w.ingest.lock() {
                        ix.set_live_max_docs(next.live_max_docs);
                    }
                }
            }
            // А потолок по объёму — **сразу**: он сравнивается с уже занятым, и
            // сниженный ниже набранного вызовет сброс на ближайшем документе.
            // Это и есть то, чего от него ждут, когда снижают на живом узле.
            if next.flush_mb != before.flush_mb {
                if let Some(budget) = node.ingest_budget.as_ref() {
                    if let Err(e) = budget.rebalance() {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        http::respond_with(stream, 503, &payload, keep);
                        return true;
                    }
                } else if let Some(w) = node.writer.as_ref() {
                    if let Ok(mut ix) = w.ingest.lock() {
                        ix.set_flush_bytes(effective_flush_bytes(
                            &next,
                            resources().wal_replay_bytes,
                        ));
                    }
                }
            }
            if next.live_max_docs != before.live_max_docs {
                if let Some(w) = node.writer.as_ref() {
                    w.live_max_docs.store(next.live_max_docs, Ordering::Relaxed);
                }
            }
            if next.wal_group_commit_ms != before.wal_group_commit_ms {
                if let Some(w) = node.writer.as_ref() {
                    if let Some(wal) = w.wal.as_ref() {
                        wal.set_group_commit_ms(next.wal_group_commit_ms);
                    }
                }
            }
            println!("настройки изменены: {}", show(&next, applied));
            http::respond_with(stream, 200, &show(&next, applied), keep);
        }
        _ => {
            http::respond_with(stream, 405, "{\"error\":\"у настроек есть GET и PUT\"}", keep);
        }
    }
    true
}

fn serve_security(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    guard: &auth::Guard,
    keep: bool,
) -> bool {
    // Хвост после `/_security`, уже без строки запроса.
    let path = route.rest.as_str();
    if path == "/api_key" && req.method == "POST" {
        let doc = match svindb_core::json::parse(&String::from_utf8_lossy(&req.body)) {
            Ok(d) => d,
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                http::respond_with(stream, 400, &payload, keep);
                return true;
            }
        };
        let name = doc.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let roles: Vec<String> = doc
            .get("roles")
            .and_then(|x| x.as_arr())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();
        // Срок в миллисекундах; ноль — бессрочный. Единица одна и та же для
        // выдачи и для проверки, чтобы не пересчитывать её в двух местах.
        let ttl = doc.get("expires_ms").and_then(|x| x.as_i64()).unwrap_or(0).max(0) as u64;
        match guard.issue(&name, roles, ttl) {
            Ok((key, secret)) => {
                // Секрет отдаётся **один раз**: в хранилище лёг только хеш, и
                // повторить выдачу узел не сможет при всём желании.
                let doc = svindb_core::json::Json::Obj(vec![
                    ("id".into(), svindb_core::json::Json::Str(key.id.clone())),
                    ("name".into(), svindb_core::json::Json::Str(key.name.clone())),
                    ("api_key".into(), svindb_core::json::Json::Str(secret.clone())),
                    (
                        "encoded".into(),
                        svindb_core::json::Json::Str(svindb_core::hash::b64(
                            format!("{}:{secret}", key.id).as_bytes(),
                        )),
                    ),
                    ("expires".into(), svindb_core::json::Json::Int(key.expires as i64)),
                ]);
                http::respond_with(stream, 201, &doc.to_compact(), keep);
            }
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                http::respond_with(stream, 400, &payload, keep);
            }
        }
        return true;
    }
    if path == "/api_key" && req.method == "GET" {
        let keys: Vec<svindb_core::json::Json> = guard.list().iter().map(|k| k.public()).collect();
        let doc = svindb_core::json::Json::Obj(vec![(
            "api_keys".into(),
            svindb_core::json::Json::Arr(keys),
        )]);
        http::respond_with(stream, 200, &doc.to_compact(), keep);
        return true;
    }
    if let Some(id) = path.strip_prefix("/api_key/") {
        if req.method == "DELETE" {
            // Отзыв есть, хотя удаления снимков нет, и это не противоречие:
            // снимок, который нельзя удалить, — сохранность; токен, который
            // нельзя отозвать, — дыра, которую нечем закрыть после утечки.
            return match guard.revoke(id) {
                Ok(true) => {
                    http::respond_with(stream, 200, "{\"revoked\":true}", keep);
                    true
                }
                Ok(false) => {
                    http::respond_with(stream, 404, "{\"error\":\"такого токена нет\"}", keep);
                    true
                }
                Err(e) => {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    http::respond_with(stream, 500, &payload, keep);
                    true
                }
            };
        }
    }
    http::respond_with(stream, 404, "{\"error\":\"нет такой точки\"}", keep);
    true
}

// Девять доводов — это срез состояния узла на время одного запроса. Собрать
// их в структуру значило бы завести ей время жизни поверх четырёх разных
// заимствований, и подпись стала бы длиннее, а не короче.
#[allow(clippy::too_many_arguments)]
/// Refuse unknown endpoints before any handler can interpret them as search.
fn refuse_route(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    keep: bool,
) -> bool {
    if route.endpoint != route::Endpoint::Unknown {
        return false;
    }
    let allowed = route::Route::allowed_methods(&req.path);
    let (status, kind, reason) = if allowed.is_empty() {
        (404, "resource_not_found_exception", format!("unknown endpoint {}", req.path))
    } else {
        http::append_extra(&format!("Allow: {}\r\n", allowed.join(", ")));
        (
            405,
            "method_not_allowed_exception",
            format!("method {} is not allowed for {}", req.method, req.path),
        )
    };
    let body = serde_json::json!({"error":{"type":kind,"reason":reason},"status":status});
    http::respond_with(stream, status, &body.to_string(), keep);
    true
}

fn serve(
    stream: &mut wire::Wire,
    req: http::Request,
    route: &route::Route,
    threads: usize,
    inflight: &AtomicUsize,
    keep: bool,
    node: &Node,
) {
    if refuse_route(stream, &req, route, keep) {
        return;
    }
    serve_with_view(stream, req, route, threads, inflight, keep, node, None);
}

struct SearchOwner {
    from: u64,
    to: u64,
    index: String,
}

struct LocalSearchView {
    segments: Arc<Segments>,
    live: FrozenLive,
    owners: Vec<SearchOwner>,
    memory_budget_mb: u64,
    generations: Vec<(Arc<Node>, u64)>,
}

fn strictest_memory_budget(budgets: impl IntoIterator<Item = u64>) -> u64 {
    budgets.into_iter().filter(|budget| *budget != 0).min().unwrap_or(0)
}

impl LocalSearchView {
    fn index_of(&self, doc: u64) -> Option<&str> {
        // У PIT одного физического индекса состав закреплён, а свежий view,
        // через который пришёл следующий запрос, мог уже иметь другой верхний
        // doc. Имя всё равно однозначно: один owner владеет каждым doc этого
        // PIT, независимо от текущего размера индекса.
        if self.owners.len() == 1 {
            return self.owners.first().map(|owner| owner.index.as_str());
        }
        self.owners
            .iter()
            .find(|owner| owner.from <= doc && doc < owner.to)
            .map(|owner| owner.index.as_str())
    }

    fn is_multi_target(&self) -> bool {
        self.owners.len() > 1
    }

    fn changed(&self) -> bool {
        self.generations.iter().any(|(node, read)| {
            node.writer
                .as_ref()
                .is_some_and(|writer| writer.visibility_generation.load(Ordering::Acquire) != *read)
        })
    }
}

const DEFAULT_SEARCH_TIMEOUT_MS: u64 = 30_000;
const MAX_SEARCH_TIMEOUT_MS: u64 = 300_000;

/// Единый срок поиска из тела или заголовка. Ноль выбирает серверное умолчание.
fn search_guard(
    req: &http::Request,
    request: &serde_json::Value,
) -> Result<Arc<svindb_core::work::Guard>, String> {
    let from_body = match request.get("timeout_ms") {
        None => None,
        Some(value) => Some(
            value
                .as_u64()
                .ok_or_else(|| "timeout_ms должен быть целым числом миллисекунд".to_string())?,
        ),
    };
    let from_header = match req.header("x-svindb-timeout-ms") {
        None => None,
        Some(value) => Some(
            value
                .parse::<u64>()
                .map_err(|_| "X-SvinDB-Timeout-Ms должен быть целым числом".to_string())?,
        ),
    };
    // Заголовок ставит внешний section deadline, тело — внутреннюю просьбу
    // клиента. Выбирать первое означало бы, что тело на 6000 мс молча
    // переписывает серверный резерв 5500 мс. Минимум сохраняет оба договора и
    // оставляет время на сериализацию ответа до внешнего disconnect.
    let timeout = [from_body, from_header]
        .into_iter()
        .flatten()
        .map(|ms| if ms == 0 { DEFAULT_SEARCH_TIMEOUT_MS } else { ms })
        .min()
        .unwrap_or(DEFAULT_SEARCH_TIMEOUT_MS);
    if timeout > MAX_SEARCH_TIMEOUT_MS {
        return Err(format!(
            "timeout_ms = {timeout} выше серверного потолка {MAX_SEARCH_TIMEOUT_MS}"
        ));
    }
    Ok(Arc::new(svindb_core::work::Guard::new(timeout)))
}

/// Единый срок point GET/MGET из query string или внутреннего заголовка.
///
/// У point read нет JSON-тела, поэтому он не может пользоваться
/// [`search_guard`] напрямую. Договор всё равно должен быть тем же: меньший
/// из двух сроков побеждает, ноль выбирает серверное умолчание, а
/// слишком большое значение отвергается до начала гидратации. Это важно не
/// только для latency: отключившийся клиент должен отменить ту же работу, что
/// и текстовый поиск, а не оставить `read_ids` обходить старые сегменты.
fn point_guard(
    req: &http::Request,
    route: &route::Route,
) -> Result<Arc<svindb_core::work::Guard>, String> {
    let from_query = query_value(&route.query, "timeout_ms")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| "timeout_ms должен быть целым числом миллисекунд".to_string())
        })
        .transpose()?;
    let from_header = req
        .header("x-svindb-timeout-ms")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| "X-SvinDB-Timeout-Ms должен быть целым числом".to_string())
        })
        .transpose()?;
    let timeout = [from_query, from_header]
        .into_iter()
        .flatten()
        .map(|ms| if ms == 0 { DEFAULT_SEARCH_TIMEOUT_MS } else { ms })
        .min()
        .unwrap_or(DEFAULT_SEARCH_TIMEOUT_MS);
    if timeout > MAX_SEARCH_TIMEOUT_MS {
        return Err(format!(
            "timeout_ms = {timeout} выше серверного потолка {MAX_SEARCH_TIMEOUT_MS}"
        ));
    }
    Ok(Arc::new(svindb_core::work::Guard::new(timeout)))
}

/// Нижняя граница памяти, которой узел уже владеет вне cgroup sample.
///
/// `memory.current` обычно включает эти объекты, поэтому caller берёт максимум
/// между наблюдаемым current и floor, а не складывает их. Это покрывает короткое
/// окно между публикацией frozen/PIT и обновлением sampler, не объявляя RSS,
/// file cache и mmap независимыми расходами.
fn runtime_memory_floor(
    writer: Option<&Arc<Writer>>,
    segments: &Segments,
    source_cache: Option<&source_cache::Cache>,
) -> u64 {
    // Обновляется владельцем ingest при публикации состояния. Search не
    // ждёт ни WAL/fsync, ни построения сегмента под ingest mutex.
    let ingest = writer.map_or(0, |writer| writer.buffered_memory_bytes.load(Ordering::Acquire));
    let view_memory =
        segments.list.iter().map(|segment| segment.src.memory_bytes() as u64).sum::<u64>();
    let (_, pit_bytes) = pit_registry().state();
    let source_cache_bytes = source_cache.map_or(0, |cache| cache.stats().bytes as u64);
    // Single-index frozen generations видны и в indexer, и в ReadView; берём
    // максимум, чтобы не удвоить один Arc-payload. Multi-index view, напротив,
    // содержит память всех целей, поэтому её собственная нижняя граница
    // покрывает индексы, которых нет у `writer` первой цели.
    ingest.max(view_memory).saturating_add(pit_bytes as u64).saturating_add(source_cache_bytes)
}

/// Эффективный резерв latency: конфигурация может только увеличить
/// рекомендованный минимум, но никогда не вправе занять весь cgroup и тем
/// самым отключить point/control path на маленьком тестовом узле.
fn effective_latency_reserve(limit: Option<u64>, configured: u64) -> u64 {
    match limit {
        Some(limit) if limit > 0 => {
            let configured = configured.min(limit / 2);
            svindb_core::govern::latency_reserve(limit).max(configured)
        }
        _ => configured,
    }
}

#[allow(clippy::too_many_arguments)]
fn serve_with_view(
    stream: &mut wire::Wire,
    req: http::Request,
    route: &route::Route,
    threads: usize,
    inflight: &AtomicUsize,
    keep: bool,
    node: &Node,
    search_view: Option<&LocalSearchView>,
) {
    let live = &node.live;
    let writer = node.writer.as_ref();
    let stream = &mut *stream;
    if serve_admin(stream, &req, route, node, keep) {
        return;
    }
    // Координатор: тело не разбирается вовсе, оно пересылается как есть.
    if let Some(fan) = node.fanout.as_ref() {
        if route.endpoint == route::Endpoint::Search {
            return serve_fanout(stream, &req, fan, keep, &node.metrics);
        }
        // Всё, что работает с документами, на координаторе отвергается.
        //
        // Прежде эти точки обслуживались его **собственным** каталогом: `_bulk`
        // и `_put` писали в пустой индекс координатора и отвечали `200`, а
        // `_doc` и `_mget` читали оттуда же и честно не находили ничего. То есть
        // клиент, отправивший данные не туда, узнавал об этом не отказом, а
        // пустой выдачей — когда-нибудь потом.
        //
        // Веер этого сделать не может и не притворяется: раскладывать запись по
        // шардам узел не умеет (`svindb_core::shards::route` обслуживает
        // внутрипроцессные шарды, а не сетевые), а гадать, в какой шард лёг
        // документ, — значит опрашивать все и складывать ответы, чего у точки на
        // один документ нет в договоре. Поэтому отказ, а не тишина.
        // Набор по идентификаторам координатор **раскладывает**: каждому
        // шарду уходит только его часть списка, а ответы собираются обратно в
        // порядок запроса. Разбор — у `coord::split_docs`.
        if route.endpoint == route::Endpoint::MGet {
            return serve_coord_mget(stream, &req.body, fan, keep);
        }
        // Точка на один документ адресуется тем же остатком: у неё один
        // идентификатор, и шард у него ровно один.
        if route.endpoint == route::Endpoint::DocGet && !route.rest.is_empty() {
            return serve_coord_doc(stream, &req, route, fan, keep);
        }
        // Запись раскладывается тем же остатком, что и чтение: иначе документ
        // лёг бы в один шард, а искался в другом.
        if matches!(route.endpoint, route::Endpoint::Bulk | route::Endpoint::Put) {
            return serve_coord_write(stream, &req, route, fan, keep);
        }
        // `_doc` по пути — один идентификатор, один шард; тело уходит шарду как
        // есть, и переводит его с языка Elasticsearch тот, кто его разбирает.
        if route.endpoint == route::Endpoint::Doc && !route.rest.is_empty() {
            return serve_coord_one(stream, &req, route, fan, keep);
        }
        if route.endpoint == route::Endpoint::Delete {
            return serve_coord_delete(stream, &req.body, fan, keep);
        }
        // Сброс адресуется не документу, а узлу: он идёт **всем** шардам, и
        // «сброшено» значит «сброшено у каждого».
        if route.endpoint == route::Endpoint::Flush {
            return serve_coord_flush(stream, fan, keep);
        }
        if route.endpoint == route::Endpoint::Mutate {
            // Условная замена читает и пишет за один шаг, и её условие может
            // ссылаться на документы, разъехавшиеся по разным шардам. Веер
            // такой атомарности не даёт и делать вид, что даёт, не будет.
            let payload = format!(
                "{{\"error\":{}}}",
                json_string(
                    "условная замена через координатор не обслуживается: её атомарность                      ограничена одним узлом, а документы запроса лежат в разных шардах.                      Адресуйте `_mutate` шарду напрямую"
                )
            );
            http::respond_with(stream, 400, &payload, keep);
            return;
        }
    }
    // Search ниже снимает состав и поколение под shared-lock, сразу отпускает
    // его и валидирует поколение перед ответом. Point GET/MGET здесь тоже не
    // берут общий visibility barrier: `DocView::take` снимает согласованный
    // ingest+segments snapshot, а `read_ids` повторяет только промахи при смене
    // publication generation. Внешний barrier был не защитой, а второй
    // синхронизацией: на production refresh стабильно добавлял около 190 мс даже
    // к заведомо отсутствующему ID.
    let metadata_only = matches!(
        route.endpoint,
        route::Endpoint::Health
            | route::Endpoint::Metrics
            | route::Endpoint::Stats
            | route::Endpoint::Inspect
            | route::Endpoint::Schema
            | route::Endpoint::Validate
    );
    let _control_permit = if metadata_only {
        match resources().enter(ResourceClass::Control) {
            Some(permit) => Some(permit),
            None => {
                http::set_extra("Retry-After: 1\r\n".to_string());
                return http::respond_with(
                    stream,
                    503,
                    "{\"error\":\"control queue заполнена\",\"retryable\":true,\"reason\":\"workload_queue\",\"queue\":\"control\"}",
                    keep,
                );
            }
        }
    } else {
        None
    };
    let versioned_read = uses_versioned_read(route.endpoint);
    let _visibility = if route.writes() || metadata_only || versioned_read {
        None
    } else {
        writer.map(|w| w.visibility.read().expect("барьер чтения mutation"))
    };
    if route.endpoint == route::Endpoint::Metrics {
        // Права нужны, и это осознанно. Соблазн открыть точку, как `/_health`,
        // велик — сборщику удобнее без учётной записи. Но `/_health` не выдаёт
        // ничего о данных, а метрики выдают: число документов, темп приёма,
        // долю отказов. По ним читается и размер корпуса, и то, работает ли с
        // ним кто-нибудь прямо сейчас.
        //
        // Умолчание `needed` для незнакомого пути — `Read`, и здесь оно верное:
        // отдельной строки в `auth::needed` для `/_metrics` нет намеренно.
        // Сборщику даётся учётная запись с одним `read`.
        let segs = { live.read().unwrap().clone() };
        let dead: usize = segs.list.iter().map(|s| s.dead()).sum();
        let ingest_state = writer.as_ref().and_then(|w| {
            w.ingest.lock().ok().map(|ix| {
                (
                    u64::from(ix.buffered()),
                    ix.buffered_bytes() as u64,
                    ix.segments() as u64,
                    ix.total_docs,
                    ix.total_tokens,
                    ix.total_postings,
                    ix.phases.clone(),
                    ix.twins(),
                )
            })
        });
        let pressure = svindb_core::mem::pressure();
        let process_io = host::io();
        let hook_stats = compaction_stats(&node.dir);
        let governor = resources();
        let source_cache_stats = node.source_cache.stats();
        let state = metrics::State {
            // Считается по составу, а не по буферу: живой сегмент — это то, что
            // поиск действительно видит, и спрашивать об этом надо у него.
            live_docs: segs.live_ones().map(|s| s.docs as u64).sum(),
            disk_docs: segs.list.iter().filter(|s| !s.src.is_memory()).map(|s| s.docs as u64).sum(),
            disk_dead: segs
                .list
                .iter()
                .filter(|s| !s.src.is_memory())
                .map(|s| s.dead() as u64)
                .sum(),
            docs: segs.docs() as u64,
            dead: dead as u64,
            segments: reported_segment_count(&segs) as u64,
            frozen_generations: writer.as_ref().map_or(0, |w| {
                w.ingest.lock().map(|ix| ix.frozen_generation_count()).unwrap_or(0) as u64
            }),
            frozen_memory_bytes: writer.as_ref().map_or(0, |w| {
                w.ingest.lock().map(|ix| ix.frozen_memory_bytes()).unwrap_or(0) as u64
            }),
            buffered: ingest_state.as_ref().map_or(0, |s| s.0),
            mmaps: svindb_core::mmap::live_maps() as u64,
            mmaps_limit: svindb_core::mmap::max_maps().map(|n| n as u64),
            wal_seq: writer.as_ref().and_then(|w| w.wal.as_ref()).map_or(0, |w| w.seq()),
            wal_checkpoint: writer.as_ref().map_or(0, |w| svindb_core::wal::checkpoint(&w.dir)),
            reload_failures: node.reloads.failures(),
            ingest_healthy: writer.as_ref().is_none_or(|w| w.healthy().is_ok()),
            inflight: inflight.load(Ordering::Relaxed) as u64,
            flushes: ingest_state.as_ref().map_or(0, |s| s.2),
            twins_built: ingest_state.as_ref().map_or(0, |s| s.7 .0),
            twins_skipped: ingest_state.as_ref().map_or(0, |s| s.7 .1),
            segments_created: writer
                .as_ref()
                .zip(ingest_state.as_ref())
                .map_or(0, |(w, s)| s.2.saturating_sub(w.initial_landed as u64)),
            write_queue_pending: writer
                .as_ref()
                .map_or(0, |w| w.pending_writes.load(Ordering::Relaxed) as u64),
            write_queue_highwater: writer
                .as_ref()
                .map_or(0, |w| w.write_queue_highwater.load(Ordering::Relaxed) as u64),
            compaction_pending: writer
                .as_ref()
                .map_or(0, |w| w.compaction_pending.load(Ordering::Relaxed) as u64),
            compaction_pending_bytes: writer
                .as_ref()
                .map_or(0, |w| w.compaction_pending_bytes.load(Ordering::Relaxed)),
            compaction_completed_sources: writer
                .as_ref()
                .map_or(0, |w| w.compaction_completed_sources.load(Ordering::Relaxed)),
            compaction_completed_input_bytes: writer
                .as_ref()
                .map_or(0, |w| w.compaction_completed_input_bytes.load(Ordering::Relaxed)),
            compaction_elapsed_micros: writer
                .as_ref()
                .map_or(0, |w| w.compaction_elapsed_micros.load(Ordering::Relaxed)),
            l0_throttled: writer.as_ref().map_or(0, |w| w.l0_throttled.load(Ordering::Relaxed)),
            l0_rejected: writer.as_ref().map_or(0, |w| w.l0_rejected.load(Ordering::Relaxed)),
            ack_seq: writer.as_ref().and_then(|w| w.wal.as_ref()).map_or(0, |wal| wal.seq()),
            visible_seq: writer.as_ref().map_or(0, |w| w.visible_seq.load(Ordering::Acquire)),
            ready_seq: writer.as_ref().map_or(0, |w| w.ready_seq.load(Ordering::Acquire)),
            settled_seq: writer.as_ref().map_or(0, |w| w.settled_seq.load(Ordering::Acquire)),
            memory_current: pressure.current,
            memory_limit: pressure.limit,
            memory_anon: pressure.anon,
            memory_file_cache: pressure.file_cache,
            memory_slab: pressure.slab,
            memory_events_max: pressure.events_max,
            psi_some_avg10: pressure.psi_some_avg10,
            psi_full_avg60: pressure.psi_full_avg60,
            process_read_bytes: process_io.read_bytes,
            process_write_bytes: process_io.write_bytes,
            process_rchar: process_io.rchar,
            process_wchar: process_io.wchar,
            resource_queues: governor.snapshot(),
            search_cpu_units_used: governor.cpu_used.load(Ordering::Acquire) as u64,
            search_cpu_units_limit: governor.cpu_max as u64,
            interactive_max_units: governor.interactive_max_units as u64,
            analytics_max_units: governor.analytics_max_units as u64,
            io_units_used: governor.io_used.load(Ordering::Acquire) as u64,
            io_units_limit: governor.io_max as u64,
            latency_reserve_bytes: governor.latency_reserve_bytes,
            wal_replay_bytes: governor.wal_replay_bytes as u64,
            max_frozen_memtables: governor.max_frozen_memtables as u64,
            background_io_bytes_per_second: governor.background_io_bytes_per_second,
            background_io_burst_bytes: governor.background_io_burst_bytes,
            background_io_max_inflight: governor.background_io_max_inflight as u64,
            source_cache_capacity_bytes: source_cache_stats.capacity_bytes as u64,
            source_cache_bytes: source_cache_stats.bytes as u64,
            source_cache_entries: source_cache_stats.entries as u64,
            source_cache_hits: source_cache_stats.hits,
            source_cache_misses: source_cache_stats.misses,
            source_cache_evictions: source_cache_stats.evictions,
            oldest_query_seconds: query_registry().oldest().map(|(_, age, _, _)| age),
            id_directory_ready: segs.id_directory_ready(),
            internal_memory_floor: runtime_memory_floor(writer, &segs, Some(&node.source_cache)),
            compaction_jobs: hook_stats.jobs,
            compaction_active_ms: hook_stats.active_ms,
            compaction_paused_ms: hook_stats.paused_ms,
            compaction_query_preemptions: hook_stats.query_preemptions,
            compaction_checkpoints: hook_stats.checkpoints,
            compaction_paced_bytes: hook_stats.paced_bytes,
            compaction_paced_waited_us: hook_stats.paced_waited_us,
            compaction_running: writer
                .as_ref()
                .map_or(0, |w| w.compaction_running.load(Ordering::Relaxed) as u64),
            ingest_phases: ingest_state.as_ref().map_or_else(Vec::new, |s| s.6.clone()),
            ingest_total_docs: ingest_state.as_ref().map_or(0, |s| s.3),
            ingest_total_tokens: ingest_state.as_ref().map_or(0, |s| s.4),
            ingest_total_postings: ingest_state.as_ref().map_or(0, |s| s.5),
            ingest_buffer_bytes: ingest_state.as_ref().map_or(0, |s| s.1),
            // Размер каталога считается с кешем внутри `host`: обход стоит
            // `stat` на файл, а точку метрик спрашивают чаще, чем меняется
            // состав сегментов.
            index_bytes: Some(host::index_bytes(&node.dir)),
            disk_free_bytes: host::free_bytes(&node.dir),
        };
        let body = node.metrics.render(&state);
        return http::respond_typed(stream, 200, &body, keep, "text/plain; version=0.0.4");
    }
    if route.endpoint == route::Endpoint::DocGet {
        let guard = match point_guard(&req, route) {
            Ok(guard) => guard,
            Err(error) => {
                return http::respond_with(
                    stream,
                    400,
                    &format!("{{\"error\":{}}}", json_string(&error)),
                    keep,
                )
            }
        };
        let work = WorkContext::new(ResourceClass::Point, Arc::clone(&guard), true);
        work.set_phase(WorkPhase::Planned);
        let _disconnect = DisconnectWatch::start(stream, Arc::clone(&guard));
        work.set_phase(WorkPhase::Queued);
        work.set_phase(WorkPhase::Yielded);
        let Some(point_permit) = resources().enter_until(ResourceClass::Point, Some(&guard)) else {
            http::set_extra("Retry-After: 1\r\n".to_string());
            let reason = if guard.fired() { guard.reason().name() } else { "workload_queue" };
            let status = if guard.fired() { 503 } else { 429 };
            return http::respond_with(
                stream,
                status,
                &format!(
                    "{{\"error\":\"point-read queue не смогла начать работу\",\"retryable\":true,\"reason\":{}}}",
                    json_string(reason)
                ),
                keep,
            );
        };
        work.set_units(point_permit.cpu_units(), point_permit.io_units());
        work.set_phase(WorkPhase::Running);
        serve_doc_get(stream, route, live, writer, &node.declared, work.guard(), keep);
        work.set_phase(WorkPhase::Responding);
        return;
    }
    if route.endpoint == route::Endpoint::MGet {
        let guard = match point_guard(&req, route) {
            Ok(guard) => guard,
            Err(error) => {
                return http::respond_with(
                    stream,
                    400,
                    &format!("{{\"error\":{}}}", json_string(&error)),
                    keep,
                )
            }
        };
        let work = WorkContext::new(ResourceClass::Point, Arc::clone(&guard), true);
        work.set_phase(WorkPhase::Planned);
        let _disconnect = DisconnectWatch::start(stream, Arc::clone(&guard));
        work.set_phase(WorkPhase::Queued);
        work.set_phase(WorkPhase::Yielded);
        let Some(point_permit) = resources().enter_until(ResourceClass::Point, Some(&guard)) else {
            http::set_extra("Retry-After: 1\r\n".to_string());
            let reason = if guard.fired() { guard.reason().name() } else { "workload_queue" };
            let status = if guard.fired() { 503 } else { 429 };
            return http::respond_with(
                stream,
                status,
                &format!(
                    "{{\"error\":\"point-read queue не смогла начать работу\",\"retryable\":true,\"reason\":{}}}",
                    json_string(reason)
                ),
                keep,
            );
        };
        work.set_units(point_permit.cpu_units(), point_permit.io_units());
        work.set_phase(WorkPhase::Running);
        let share = point_read_share(resources().active(ResourceClass::Point), threads);
        serve_mget(stream, &req.body, live, writer, &node.declared, share, work.guard(), keep);
        work.set_phase(WorkPhase::Responding);
        return;
    }
    if route.endpoint == route::Endpoint::Stats {
        return serve_stats(stream, live, writer, node, threads, keep);
    }
    if route.endpoint == route::Endpoint::Inspect {
        let guard = match point_guard(&req, route) {
            Ok(guard) => guard,
            Err(error) => {
                return http::respond_with(
                    stream,
                    400,
                    &format!("{{\"error\":{}}}", json_string(&error)),
                    keep,
                )
            }
        };
        let work = WorkContext::new(ResourceClass::Maintenance, Arc::clone(&guard), true);
        work.set_phase(WorkPhase::Planned);
        let _disconnect = DisconnectWatch::start(stream, Arc::clone(&guard));
        work.set_phase(WorkPhase::Running);
        return serve_inspect(stream, node, live, &guard, keep);
    }
    // Схема как она объявлена. Спросить об этом узел прежде было нечем вовсе, а
    // половина ошибок приёма — это несовпадение имени поля с объявленным.
    if route.endpoint == route::Endpoint::Schema {
        let payload = match schema_json(&node.dir) {
            Some(p) => p,
            None => "{\"fields\":[],\"groups\":[]}".to_string(),
        };
        return http::respond_with(stream, 200, &payload, keep);
    }

    // Сухой прогон приёма: тем же разбором, что и настоящий, но без записи.
    //
    // Именно **тем же**, а не его копией: разойдясь, они дали бы «проверка
    // сказала одно, приём сделал другое», и это хуже отсутствия проверки.
    if route.endpoint == route::Endpoint::Validate {
        let Some(w) = writer.as_ref() else {
            return http::respond_with(
                stream,
                400,
                "{\"error\":\"проверять нечем: узел запущен с --read-only\"}",
                keep,
            );
        };
        let schema_started = stats::ENABLED.then(Instant::now);
        let plan = w.plan();
        if let Some(schema_started) = schema_started {
            node.metrics.ingest_schema(schema_started.elapsed().as_secs_f64() * 1000.0);
        }
        let parse_started = stats::ENABLED.then(Instant::now);
        let parsed = parse_docs(&req.body, &plan);
        if let Some(parse_started) = parse_started {
            node.metrics.ingest_json(parse_started.elapsed().as_secs_f64() * 1000.0);
        }
        let payload = match parsed {
            Ok((docs, report)) => format!(
                "{{\"documents\":{},\"ok\":{}{}}}",
                docs.len(),
                report.clean(),
                report.suffix(json_string)
            ),
            Err(e) => format!("{{\"ok\":false,\"error\":{}}}", json_string(&e)),
        };
        return http::respond_with(stream, 200, &payload, keep);
    }

    if route.endpoint == route::Endpoint::Health {
        let segs = { live.read().unwrap().clone() };
        let dead: usize = segs.list.iter().map(|s| s.dead()).sum();
        // `ok` — не украшение, а признак для балансировщика. Узел, у которого
        // состав перестал перечитываться, отвечает **устаревшей** выдачей, и
        // снять с него трафик надо раньше, чем это заметят по жалобам. Признак
        // снимается сам, как только перезагрузка снова прошла: одиночный сбой
        // не должен выводить узел из строя навсегда.
        let stale = node.reloads.failures();
        let payload = format!(
            "{{\"ok\":{},\"docs\":{},\"segments\":{},\"dead\":{},\"added\":{},\"deleted\":{}}}",
            // `ok` ложно и когда приём остановлен отказом записи: узел
            // отвечает на поиск, но больше ничего не принимает, и
            // балансировщик обязан об этом узнать — он спрашивает именно
            // отсюда.
            stale == 0 && writer.as_ref().is_none_or(|w| w.healthy().is_ok()),
            segs.docs(),
            reported_segment_count(&segs),
            dead,
            writer.as_ref().map_or(0, |w| w.docs.load(Ordering::Relaxed)),
            writer.as_ref().map_or(0, |w| w.deleted.load(Ordering::Relaxed))
        );
        let payload = payload.replace(
            "}",
            &format!(
                ",\"reload_failures\":{stale},\"reload_error\":{},\
                 \"mappings\":{},\"maps_limit\":{},\
                 \"buffered\":{},\"wal\":{{\"seq\":{},\"checkpoint\":{},\"applied\":{},\
                 \"replayed_adds\":{},\"replayed_puts\":{},\"replayed_deletes\":{},\
                 \"torn_bytes\":{}}},\
                 \"write_queue\":{{\"pending\":{},\"limit\":{}}},\
                 \"query_memory\":{{\"used_bytes\":{},\"limit_bytes\":{}}},\
                 \"compaction\":{{\"pending_segments\":{},\"pending_schedulable\":{},\"running_segments\":{},\"blocked\":{}}},\
                 \"flush_in_progress\":{},\"ingest_error\":{}{}}}",
                json_string(&node.reloads.last()),
                // Число областей отображения и предел процесса. Упирается в
                // предел не память, а число сегментов, и потолок это, а не
                // замедление: индекс просто перестаёт открываться. Отказ `mmap`
                // об этом скажет, но поздно — здесь видно заранее.
                svindb_core::mmap::live_maps(),
                svindb_core::mmap::max_maps()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "null".into()),
                writer.as_ref().map_or(0, |w| w.ingest.lock().map(|ix| ix.buffered()).unwrap_or(0)),
                writer.as_ref().and_then(|w| w.wal.as_ref()).map_or(0, |w| w.seq()),
                writer.as_ref().map_or(0, |w| svindb_core::wal::checkpoint(&w.dir)),
                // Сплошной префикс применённого. Отставание от `seq` — это
                // записи, пережившие `fsync`, но ещё не легшие в буфер; растущий
                // и не сходящийся разрыв означает застрявшую очередь применения,
                // и увидеть это надо раньше, чем по истечении предела ожидания.
                writer.as_ref().map_or(0, |w| w.applied.watermark()),
                writer.as_ref().map_or(0, |w| w.recovered.adds),
                writer.as_ref().map_or(0, |w| w.recovered.puts),
                writer.as_ref().map_or(0, |w| w.recovered.deletes),
                writer.as_ref().map_or(0, |w| w.recovered.dropped),
                writer.as_ref().map_or(0, |w| w.pending_writes.load(Ordering::Relaxed)),
                node.live_settings.read().map(|s| s.max_pending_writes).unwrap_or(0),
                node.query_pool.used(),
                node.query_pool.max(),
                writer.as_ref().map_or(0, |w| w.compaction_pending.load(Ordering::Relaxed)),
                writer.as_ref().map_or(0, |w| w.compaction_schedulable.load(Ordering::Relaxed)),
                writer.as_ref().map_or(0, |w| w.compaction_running.load(Ordering::Relaxed)),
                // Причина простоя вместе с числами: без них «не влезает» —
                // это жалоба, а не сведение, по которому можно что-то решить.
                json_string(&writer.as_ref().map_or(String::new(), |w| {
                    match &*w.compaction_blocked.lock().unwrap_or_else(|e| e.into_inner()) {
                        svindb_core::maintain::Blocked::OverPeak {
                            worst,
                            need,
                            have,
                            sources,
                            kind,
                            smallest_pair_need,
                        } => format!(
                            "over_peak: {kind:?} из {sources} сегментов просит {need} Б при \
                             отведённых {have} Б, дороже всего {worst}; самая дешёвая пара \
                             просит {smallest_pair_need} Б"
                        ),
                        svindb_core::maintain::Blocked::DiskShort { free, need } => format!(
                            "disk_short: свободно {free} Б, работе нужно {need} Б"
                        ),
                        other => other.name().to_string(),
                    }
                })),
                writer
                    .as_ref()
                    .is_some_and(|w| { w.flush_state.lock().map(|s| s.running).unwrap_or(false) }),
                json_string(&writer.as_ref().and_then(|w| w.healthy().err()).unwrap_or_default()),
                // Узел-координатор своих документов не имеет, и числа выше у
                // него нули по построению. Без этого блока `/_health` выглядел
                // бы как пустой узел, которому нечего отдавать, — а он
                // обслуживает веер и вполне жив.
                //
                // `cooling` — реплики, отвечающие сейчас не первыми после
                // отказа. Выдача при этом приходит, и по ней бы никто не
                // догадался, что половина копий лежит.
                match node.fanout.as_ref() {
                    None => String::new(),
                    Some(f) => format!(
                        ",\"coordinator\":{{\"shards\":{},\"replicas\":{},\"cooling\":{}}}",
                        f.shards.len(),
                        f.replicas(),
                        f.cooling()
                    ),
                }
            ),
        );
        let payload = match writer {
            Some(writer) => {
                let (soft, hard) = node
                    .live_settings
                    .read()
                    .map(|settings| (settings.l0_soft_segments, settings.l0_hard_segments))
                    .unwrap_or((8, 24));
                let oldest = query_registry().oldest().map_or_else(
                    || "null".to_string(),
                    |(id, age, index, class)| {
                        format!(
                            "{{\"id\":{id},\"age_ms\":{:.3},\"index\":{},\"class\":{}}}",
                            age * 1000.0,
                            json_string(&index),
                            json_string(class)
                        )
                    },
                );
                let (pit_active, pit_bitmap_bytes) = pit_registry().state();
                let hook_stats = compaction_stats(&writer.dir);
                let governor = resources();
                let source_cache_stats = node.source_cache.stats();
                let l0_absolute = writer.compaction_pending.load(Ordering::Relaxed);
                let (l0_baseline, l0_effective) =
                    l0_growth(l0_absolute, writer.compaction_floor.load(Ordering::Relaxed));
                let pending_bytes = writer.compaction_pending_bytes.load(Ordering::Relaxed);
                let compacted_bytes =
                    writer.compaction_completed_input_bytes.load(Ordering::Relaxed);
                let compaction_seconds =
                    writer.compaction_elapsed_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
                let compaction_bytes_per_second = if compaction_seconds > 0.0 {
                    compacted_bytes as f64 / compaction_seconds
                } else {
                    0.0
                };
                let compaction_eta = if pending_bytes > 0 && compaction_bytes_per_second > 0.0 {
                    format!("{:.3}", pending_bytes as f64 / compaction_bytes_per_second)
                } else {
                    "null".to_string()
                };
                let suffix = format!(
                    ",\"watermarks\":{{\"ack\":{},\"visible\":{},\"ready\":{},\"settled\":{}}},\
                     \"workload\":{{\"search_cpu_units\":{},\"interactive_max_units\":{},\
                     \"analytics_max_units\":{},\"cpu_units_used\":{},\"io_units_used\":{},\
                     \"io_units_limit\":{},\"latency_reserve_bytes\":{},\"wal_replay_bytes\":{},\
                     \"max_frozen_memtables\":{}}},\
                     \"point_read\":{{\"id_directory_ready\":{}}},\
                     \"l0_admission\":{{\"soft\":{soft},\"hard\":{hard},\"absolute\":{l0_absolute},\
                     \"baseline\":{l0_baseline},\"effective\":{l0_effective},\"throttled\":{},\"rejected\":{}}},\
                     \"oldest_query\":{oldest},\"pit\":{{\"active\":{pit_active},\"bitmap_bytes\":{pit_bitmap_bytes}}},\
                     \"compaction_progress\":{{\"pending_bytes\":{pending_bytes},\"completed_input_bytes\":{compacted_bytes},\
                     \"completed_sources\":{},\"jobs\":{},\"active_seconds\":{compaction_seconds:.6},\
                     \"active_ms_total\":{},\"paused_ms_total\":{},\"query_preemptions\":{},\
                     \"checkpoints\":{},\"paced_bytes\":{},\"paced_wait_us\":{},\
                     \"bytes_per_second\":{compaction_bytes_per_second:.3},\
                     \"eta_seconds\":{compaction_eta}}}}}",
                    writer.wal.as_ref().map_or(0, Wal::seq),
                    writer.visible_seq.load(Ordering::Acquire),
                    writer.ready_seq.load(Ordering::Acquire),
                    writer.settled_seq.load(Ordering::Acquire),
                    governor.cpu_max,
                    governor.interactive_max_units,
                    governor.analytics_max_units,
                    governor.cpu_used.load(Ordering::Acquire),
                    governor.io_used.load(Ordering::Acquire),
                    governor.io_max,
                    governor.latency_reserve_bytes,
                    governor.wal_replay_bytes,
                    governor.max_frozen_memtables,
                    segs.id_directory_ready(),
                    writer.l0_throttled.load(Ordering::Relaxed),
                    writer.l0_rejected.load(Ordering::Relaxed),
                    writer.compaction_completed_sources.load(Ordering::Relaxed),
                    hook_stats.jobs,
                    hook_stats.active_ms,
                    hook_stats.paused_ms,
                    hook_stats.query_preemptions,
                    hook_stats.checkpoints,
                    hook_stats.paced_bytes,
                    hook_stats.paced_waited_us,
                );
                let suffix = suffix
                    .strip_suffix('}')
                    .map(|head| {
                        format!(
                            "{head},\"source_cache\":{{\"capacity_bytes\":{},\"bytes\":{},\
                             \"entries\":{},\"hits\":{},\"misses\":{},\"evictions\":{}}}}}",
                            source_cache_stats.capacity_bytes,
                            source_cache_stats.bytes,
                            source_cache_stats.entries,
                            source_cache_stats.hits,
                            source_cache_stats.misses,
                            source_cache_stats.evictions
                        )
                    })
                    .unwrap_or(suffix);
                payload.strip_suffix('}').map(|head| format!("{head}{suffix}")).unwrap_or(payload)
            }
            None => payload,
        };
        let build = format!(
            ",\"build\":{{\"version\":{},\"revision\":{},\"target\":{},\"source_date_epoch\":{}}}}}",
            json_string(host::version()),
            json_string(host::revision()),
            json_string(&host::target()),
            json_string(host::build_epoch()),
        );
        let payload =
            payload.strip_suffix('}').map(|head| format!("{head}{build}")).unwrap_or(payload);
        return http::respond_with(stream, 200, &payload, keep);
    }

    // По разобранной цели, а не по строке: прежде `POST /_bulk?refresh=true` и
    // `POST /мойиндекс/_bulk` сюда не попадали вовсе и уходили в поиск —
    // отвечая двумястами на запрос, который ничего не принял. Разбор в
    // [`route`], там же и разбор почему.
    // `PUT` наравне с `POST`: привычная клиенту точка на один документ
    // принимает оба, и требовать от него только `POST` значило бы уронить
    // запрос в поиск — то есть ответить двумястами, ничего не приняв.
    if matches!(req.method.as_str(), "POST" | "PUT" | "DELETE") && route.writes() {
        let Some(w) = writer.as_ref() else {
            return http::respond_with(
                stream,
                400,
                "{\"error\":\"приём выключен: узел запущен с --read-only\"}",
                keep,
            );
        };
        // Flush — управляющая операция, которая освобождает WAL/L0 и память.
        // Она не должна сначала проходить write-memory/L0 admission: под
        // давлением именно этот порядок превращал единственный способ
        // стабилизации в 429 и оставлял узел занятым крупными буферами. Для
        // flush используется отдельная control-очередь; сам тяжёлый writer
        // остаётся асинхронным и ждёт только ограниченное время ответа.
        if route.endpoint == route::Endpoint::Flush {
            let flush_guard = match point_guard(&req, route) {
                Ok(guard) => guard,
                Err(error) => {
                    return http::respond_with(
                        stream,
                        400,
                        &format!("{{\"error\":{}}}", json_string(&error)),
                        keep,
                    )
                }
            };
            let work = WorkContext::new(ResourceClass::Control, Arc::clone(&flush_guard), true);
            work.set_phase(WorkPhase::Planned);
            let _disconnect = DisconnectWatch::start(stream, Arc::clone(&flush_guard));
            work.set_phase(WorkPhase::Queued);
            let Some(_control_permit) =
                resources().enter_until(ResourceClass::Control, Some(&flush_guard))
            else {
                http::set_extra("Retry-After: 1\r\n".to_string());
                let reason = if flush_guard.fired() {
                    flush_guard.reason().name()
                } else {
                    "workload_queue"
                };
                let status = if flush_guard.fired() { 503 } else { 429 };
                return http::respond_with(
                    stream,
                    status,
                    &format!(
                        "{{\"error\":\"control queue не смогла начать flush\",\
                         \"retryable\":true,\"reason\":{}}}",
                        json_string(reason)
                    ),
                    keep,
                );
            };
            work.set_units(0, 0);
            work.set_phase(WorkPhase::Running);
            let wait = node.live_settings.read().map(|s| s.flush_wait_ms).unwrap_or(5_000);
            return match w.request_flush(Duration::from_millis(wait)) {
                Ok(true) => http::respond_with(stream, 200, "{\"flushed\":true}", keep),
                Ok(false) => http::respond_with(
                    stream,
                    202,
                    "{\"flushed\":false,\"in_progress\":true}",
                    keep,
                ),
                Err(e) => {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    http::respond_with(stream, 503, &payload, keep)
                }
            };
        }
        let write_guard = match point_guard(&req, route) {
            Ok(guard) => guard,
            Err(error) => {
                return http::respond_with(
                    stream,
                    400,
                    &format!("{{\"error\":{}}}", json_string(&error)),
                    keep,
                )
            }
        };
        let work = WorkContext::new(ResourceClass::Write, Arc::clone(&write_guard), true);
        work.set_phase(WorkPhase::Planned);
        let _disconnect = DisconnectWatch::start(stream, Arc::clone(&write_guard));
        work.set_phase(WorkPhase::Queued);
        // Краткий cache-relief/flush quantum не означает перегрузку ingest.
        // До WAL допускается та же bounded очередь до 250 мс, что у чтения;
        // L0 hard limit и max_pending_writes остаются самостоятельными gates.
        let Some(resource_write) =
            resources().enter_until(ResourceClass::Write, Some(work.guard()))
        else {
            http::set_extra("Retry-After: 1\r\n".to_string());
            return http::respond_with(
                stream,
                429,
                "{\"error\":\"write resource queue заполнена\",\"retryable\":true,\"reason\":\"workload_queue\",\"queue\":\"write\"}",
                keep,
            );
        };
        work.set_units(resource_write.cpu_units(), resource_write.io_units());
        work.set_phase(WorkPhase::Running);
        // Ограничение берётся **до** разбора тела и ожидания ingest/layout:
        // иначе уже переполненный узел продолжал бы занимать по потоку на
        // каждый новый запрос, а 429 появлялся бы только после самой дорогой
        // части. Permit живёт до любого раннего `return` этой ветви благодаря
        // RAII и потому не протекает на ошибочном JSON или конфликте CAS.
        // Приём репликации — до всего прочего: у него другой договор.
        //
        // Здесь не разбираются условия (primary их уже рассудил), не проверяется
        // строгость (она отвергла бы то, что primary уже принял, и копии разошлись
        // бы) и ничего не пересылается дальше. Проверяется одно, зато прежде
        // всего: имеет ли отправитель право писать в этот терм.
        if route.endpoint == route::Endpoint::Replicate {
            return serve_replicate(stream, &req, w, node, keep);
        }
        // Сужение полосы: узел гасит у себя всё, что по новой раскладке
        // принадлежит соседу. Разбор — у `serve_reshard`.
        if route.endpoint == route::Endpoint::Reshard {
            return serve_reshard(stream, &req, w, live, node, keep);
        }
        // Real-memory indexing pressure: как у Elasticsearch, outstanding
        // write не вправе занять reserve control/search, а как у RocksDB
        // no_slowdown, клиент получает bounded retryable отказ вместо
        // неограниченного ожидания reclaim. Репликация выше не отвергается:
        // уже подтверждённая primary-запись должна догнать копию.
        let pressure = svindb_core::mem::pressure();
        if let Some(limit) = pressure.limit {
            let latency_reserve = svindb_core::govern::latency_reserve(limit);
            let relief_reserve = svindb_core::govern::pressure_relief_reserve(limit);
            if pressure.near_limit(relief_reserve) || pressure.overloaded() {
                http::set_extra("Retry-After: 1\r\n".to_string());
                let payload = format!(
                    "{{\"error\":\"приём придержан до освобождения memory reserve\",\
                     \"retryable\":true,\"reason\":\"write_memory_pressure\",\
                     \"current_bytes\":{},\"limit_bytes\":{limit},\
                     \"latency_reserve_bytes\":{latency_reserve},\
                     \"pressure_relief_reserve_bytes\":{relief_reserve}}}",
                    pressure.current.unwrap_or(0)
                );
                return http::respond_with(stream, 429, &payload, keep);
            }
        }
        if pressure.io_severely_overloaded() {
            http::set_extra("Retry-After: 1\r\n".to_string());
            return http::respond_with(
                stream,
                429,
                "{\"error\":\"приём придержан из-за I/O stall\",\"retryable\":true,\
                 \"reason\":\"write_io_pressure\"}",
                keep,
            );
        }
        if pressure.io_overloaded() {
            // Общий, а не per-thread delay: одновременно проснувшиеся writers
            // снова создали бы тот же burst. Длинная очередь получает 429,
            // как RocksDB no_slowdown, и не занимает connection workers.
            if !io_write_pacer().enter(Some(&write_guard)) {
                http::set_extra("Retry-After: 1\r\n".to_string());
                let reason = if write_guard.fired() { "deadline" } else { "write_io_queue" };
                let status = if write_guard.fired() { 503 } else { 429 };
                return http::respond_with(
                    stream,
                    status,
                    &format!(
                        "{{\"error\":\"I/O write pacer заполнен\",\"retryable\":true,\
                         \"reason\":{}}}",
                        json_string(reason)
                    ),
                    keep,
                );
            }
        }
        let (write_limit, l0_soft, l0_hard) = node
            .live_settings
            .read()
            .map(|s| (s.max_pending_writes, s.l0_soft_segments, s.l0_hard_segments))
            .unwrap_or((64, 8, 24));
        let l0_absolute = w.compaction_pending.load(Ordering::Acquire);
        let (l0_baseline, l0_debt) =
            l0_growth(l0_absolute, w.compaction_floor.load(Ordering::Acquire));
        match l0_admission(l0_debt, l0_soft, l0_hard) {
            Err(()) => {
                w.l0_rejected.fetch_add(1, Ordering::Relaxed);
                http::set_extra("Retry-After: 1\r\n".to_string());
                let payload = format!(
                    "{{\"error\":\"L0 compaction debt достиг жёсткого предела; повторите с backoff\",\
                     \"retryable\":true,\"l0_debt\":{l0_debt},\"l0_absolute\":{l0_absolute},\
                     \"l0_baseline\":{l0_baseline},\"soft\":{l0_soft},\"hard\":{l0_hard}}}"
                );
                return http::respond_with(stream, 429, &payload, keep);
            }
            Ok(delay) if !delay.is_zero() => {
                w.l0_throttled.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                while started.elapsed() < delay {
                    if !write_guard.check() {
                        let payload = format!(
                            "{{\"error\":\"L0 backpressure отменён клиентом\",\
                             \"retryable\":true,\"reason\":{}}}",
                            json_string(write_guard.reason().name())
                        );
                        return http::respond_with(stream, 503, &payload, keep);
                    }
                    let left =
                        delay.saturating_sub(started.elapsed()).min(Duration::from_millis(10));
                    if left.is_zero() {
                        break;
                    }
                    std::thread::park_timeout(left);
                }
            }
            Ok(_) => {}
        }
        let Some(_write_permit) = w.enter_write(write_limit) else {
            return http::respond_with(
                stream,
                429,
                "{\"error\":\"очередь записи заполнена; повторите запрос с backoff\",\"retryable\":true}",
                keep,
            );
        };
        // Место проверяется **до** разбора тела и до журнала.
        //
        // `ENOSPC` посреди сброса или слияния оставляет половину работы и
        // половину места, и разобрать это некому: отметка не двинулась, буфер не
        // отдан, а узел уже не может ни принять, ни прибрать. Отказ до начала —
        // это отказ с причиной и числом, после которого узел продолжает жить и
        // отвечать на чтение.
        //
        // Нужное считается по телу запроса, а не по индексу: сжатие ужимает, но
        // обещать этого нельзя, а журнал примет тело как есть.
        if let host::Admit::Short { free, need, floor } =
            host::admit_disk(&node.dir, req.body.len() as u64)
        {
            let payload = format!(
                "{{\"error\":{},\"free_bytes\":{free},\"need_bytes\":{need},\
                 \"floor_bytes\":{floor},\"retryable\":true}}",
                json_string(
                    "на томе не осталось запаса: приём остановлен, чтение продолжает работать"
                )
            );
            return http::respond_with(stream, 507, &payload, keep);
        }
        // Отставание отметки журнала — тоже до разбора тела.
        //
        // Отметку двигает фоновый поток, и это снимает с запроса десятки
        // миллисекунд. Но у переноса есть обратная сторона: под потоком, который
        // быстрее фонового тика, неотмеченное копится, а каждая неотмеченная
        // запись переигрывается при подъёме. Без предела время восстановления
        // становится неограниченным — и узнают об этом ровно тогда, когда
        // узнавать поздно.
        if let Some(w) = node.writer.as_ref() {
            if let Err((lag, limit)) = w.wal_lag_ok() {
                let payload = format!(
                    "{{\"error\":{},\"wal_lag\":{lag},\"wal_lag_limit\":{limit},\
                     \"retryable\":true}}",
                    json_string(
                        "отметка журнала отстала: приём придержан, чтобы подъём после \
                         падения остался конечным"
                    )
                );
                return http::respond_with(stream, 429, &payload, keep);
            }
        }
        if route.endpoint == route::Endpoint::Bulk
            && esbulk::looks_like_ndjson(&req.body)
            && bulk::serve(stream, &req, route, node, keep)
        {
            return;
        }
        // Поля берутся у приёмного буфера, то есть из схемы **этого** индекса.
        //
        // Прежде тут стояли зашитые «title, body» из `default_fields`, и всякий
        // индекс с другой схемой принимал документы, отвечал `{"added":N}` и
        // клал пустоту в каждое поле. Проверено на 25 млн сообщений: поле
        // `message` не доходило до индекса вовсе, а приём об этом не сообщал.
        let schema_started = stats::ENABLED.then(Instant::now);
        let plan = w.plan();
        if let Some(schema_started) = schema_started {
            node.metrics.ingest_schema(schema_started.elapsed().as_secs_f64() * 1000.0);
        }
        // Строгость спрашивается у живых настроек, а не у аргументов запуска:
        // её меняют на ходу тем же `PUT /_settings`, что и всё остальное.
        let strict = node
            .live_settings
            .read()
            .map(|s| s.strict_ingest)
            .unwrap_or(svindb_core::settings::Strict::Batch);
        let payload = match route.endpoint {
            route::Endpoint::Flush => unreachable!("flush обработан через control admission выше"),
            // Гашение одного документа по пути: `DELETE /{индекс}/_doc/{id}`.
            route::Endpoint::Doc if req.method == "DELETE" => {
                let ids = vec![route.rest.clone()];
                if let Err(e) = no_empty_ids(&ids) {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    return http::respond_with(stream, 400, &payload, keep);
                }
                match w.delete(&ids) {
                    Ok(n) => {
                        node.metrics.deleted(n as u64);
                        match replicate_effect(node, "delete", None, &ids) {
                            None => format!("{{\"deleted\":{n}}}"),
                            Some(a) => {
                                let payload = format!(
                                    "{{\"deleted\":{n}{}}}",
                                    a.suffix(node_ack(node), json_string)
                                );
                                let code = if a.enough() { 200 } else { 503 };
                                return http::respond_with(stream, code, &payload, keep);
                            }
                        }
                    }
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        let status = if e.starts_with("WAL_RECORD_TOO_LARGE:") { 413 } else { 503 };
                        return http::respond_with(stream, status, &payload, keep);
                    }
                }
            }
            route::Endpoint::Delete => {
                let Some(ids) = parse_ids(&req.body) else {
                    let payload = format!(
                        "{{\"error\":{}}}",
                        json_string("тело не разобралось: ожидается {\"ids\":[…]}")
                    );
                    return http::respond_with(stream, 400, &payload, keep);
                };
                if let Err(e) = no_empty_ids(&ids) {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    return http::respond_with(stream, 400, &payload, keep);
                }
                match w.delete(&ids) {
                    Ok(n) => {
                        node.metrics.deleted(n as u64);
                        match replicate_effect(node, "delete", None, &ids) {
                            None => format!("{{\"deleted\":{n}}}"),
                            Some(a) => {
                                let payload = format!(
                                    "{{\"deleted\":{n}{}}}",
                                    a.suffix(node_ack(node), json_string)
                                );
                                let code = if a.enough() { 200 } else { 503 };
                                return http::respond_with(stream, code, &payload, keep);
                            }
                        }
                    }
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        let status = if e.starts_with("WAL_RECORD_TOO_LARGE:") { 413 } else { 503 };
                        return http::respond_with(stream, status, &payload, keep);
                    }
                }
            }
            route::Endpoint::Put | route::Endpoint::Mutate => {
                let conditions = match parse_put_conditions(&req.body) {
                    Ok(c) => c,
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        return http::respond_with(stream, 400, &payload, keep);
                    }
                };
                let deletes = if route.endpoint == route::Endpoint::Mutate {
                    match parse_mutation_deletes(&req.body) {
                        Ok(ids) => ids,
                        Err(e) => {
                            let payload = format!("{{\"error\":{}}}", json_string(&e));
                            return http::respond_with(stream, 400, &payload, keep);
                        }
                    }
                } else {
                    Vec::new()
                };
                if let Err(e) = no_empty_ids(&deletes) {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    return http::respond_with(stream, 400, &payload, keep);
                }
                let parse_started = stats::ENABLED.then(Instant::now);
                let parsed = parse_docs(&req.body, &plan);
                if let Some(parse_started) = parse_started {
                    node.metrics.ingest_json(parse_started.elapsed().as_secs_f64() * 1000.0);
                }
                let (docs, report) = match parsed {
                    Ok(x) => x,
                    Err(e) if e == "тело не разобралось" => {
                        let payload = format!(
                            "{{\"error\":{}}}",
                            json_string(
                                "тело не разобралось: ожидается {\"documents\":[{\"id\":…}]}"
                            )
                        );
                        return http::respond_with(stream, 400, &payload, keep);
                    }
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        return http::respond_with(stream, 400, &payload, keep);
                    }
                };
                let put_ids: Vec<String> = docs.iter().map(|d| d.id.clone()).collect();
                if let Err(e) = no_empty_ids(&put_ids) {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    return http::respond_with(stream, 400, &payload, keep);
                }
                // Строгость отвергает **всю** пачку, а не отдельные документы.
                // Наполовину принятая пачка хуже отвергнутой: повтор запроса
                // сдвоит принятое, а разобрать, что именно дошло, клиенту нечем.
                // Пустой идентификатор — общий у **всех** документов без `id`,
                // и адресовать им нельзя. `_put` и `_delete` это проверяли, а
                // приём пачкой — нет: документ с пустым `id` брался молча и
                // обнаруживался позже, когда его не удавалось ни заменить, ни
                // удалить.
                let named: Vec<String> =
                    docs.iter().filter(|d| d.has_id).map(|d| d.id.clone()).collect();
                if let Err(e) = no_empty_ids(&named) {
                    let payload = format!("{{\"error\":{}}}", json_string(&e));
                    return http::respond_with(stream, 400, &payload, keep);
                }
                if !strict.lenient() && !report.clean() {
                    let payload = format!("{{\"error\":{}}}", json_string(&report.refusal()));
                    return http::respond_with(stream, 400, &payload, keep);
                }
                let outcome = if route.endpoint == route::Endpoint::Mutate {
                    w.mutate_if(docs, &deletes, &conditions)
                } else {
                    w.put_if(docs, &conditions)
                        .map(|result| result.map(|(added, replaced)| (added, replaced, 0)))
                };
                match outcome {
                    Ok(Ok((n, replaced, deleted))) => {
                        node.metrics.added(n as u64);
                        node.metrics.deleted(deleted as u64);
                        node.metrics.ingest_bytes(req.body.len() as u64);
                        let head = format!(
                            "{{\"added\":{n},\"replaced\":{replaced},\"deleted\":{deleted},\"durable\":true{}",
                            report.suffix(json_string)
                        );
                        // Реплике уходит **исход**, а не условие: primary его уже
                        // рассудил, и второй разбор дал бы двум узлам возможность
                        // рассудить одно условие по-разному. Расхождение, о
                        // котором оба ответили бы «принято».
                        match replicate_effect(node, "put", Some(&req.body), &deletes) {
                            None => format!("{head}}}"),
                            Some(a) => {
                                let payload =
                                    format!("{head}{}}}", a.suffix(node_ack(node), json_string));
                                let code = if a.enough() { 200 } else { 503 };
                                return http::respond_with(stream, code, &payload, keep);
                            }
                        }
                    }
                    Ok(Err(conflict)) => {
                        let payload = format!(
                            "{{\"conflict\":true,\"id\":{},\"field\":{}}}",
                            json_string(&conflict.id),
                            json_string(&conflict.field)
                        );
                        return http::respond_with(stream, 409, &payload, keep);
                    }
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        return http::respond_with(stream, 503, &payload, keep);
                    }
                }
            }
            // Приём пачкой. Ветвь именована, а не оставлена умолчанием:
            // сюда попадает только то, что `Route::writes` признала пишущим, а
            // это перечисление в одном месте — и другой точки здесь взяться
            // неоткуда.
            _ => {
                // Тело в виде, каким его шлёт Elasticsearch, переводится в
                // родное **до** разбора. Второго разборщика документа при этом
                // не заводится: два способа понять один запрос однажды
                // разойдутся, и разойдутся молча.
                let translated;
                let body: &[u8] = if route.endpoint == route::Endpoint::Doc {
                    translated = match es_one(&req.body, &route.rest) {
                        Ok(b) => b,
                        Err(e) => {
                            let payload = format!("{{\"error\":{}}}", json_string(&e));
                            return http::respond_with(stream, 400, &payload, keep);
                        }
                    };
                    &translated
                } else if esbulk::looks_like_ndjson(&req.body) {
                    translated = match es_batch(&req.body, route.index.as_deref()) {
                        Ok(b) => b,
                        Err(e) => {
                            let payload = format!("{{\"error\":{}}}", json_string(&e));
                            return http::respond_with(stream, 400, &payload, keep);
                        }
                    };
                    &translated
                } else {
                    &req.body
                };
                let parse_started = stats::ENABLED.then(Instant::now);
                let parsed = parse_docs(body, &plan);
                if let Some(parse_started) = parse_started {
                    node.metrics.ingest_json(parse_started.elapsed().as_secs_f64() * 1000.0);
                }
                let (docs, report) = match parsed {
                    Ok(x) => x,
                    Err(e) if e == "тело не разобралось" => {
                        let payload = format!(
                            "{{\"error\":{}}}",
                            json_string(
                                "тело не разобралось: ожидается {\"documents\":[{\"id\":…}]}"
                            )
                        );
                        return http::respond_with(stream, 400, &payload, keep);
                    }
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        return http::respond_with(stream, 400, &payload, keep);
                    }
                };
                if !strict.lenient() && !report.clean() {
                    let payload = format!("{{\"error\":{}}}", json_string(&report.refusal()));
                    return http::respond_with(stream, 400, &payload, keep);
                }
                // `id` is a key on every node; retries must replace old versions.
                // В кластере `id` — ключ, а bulk является upsert. Это не
                // косметическая унификация: повтор записи на реплике после
                // crash между применением и отметкой должен быть идемпотентен,
                // и primary обязан иметь ту же семантику, как и одиночный узел.
                let outcome = w.put_if(docs, &[]).map(|result| result.map(|(n, _)| n));
                match outcome {
                    Ok(Ok(n)) => {
                        node.metrics.added(n as u64);
                        node.metrics.ingest_bytes(body.len() as u64);
                        let head = format!("{{\"added\":{n}{}", report.suffix(json_string));
                        let replicated_op = "put";
                        match replicate_effect(node, replicated_op, Some(body), &[]) {
                            None => format!("{head}}}"),
                            Some(a) => {
                                let payload =
                                    format!("{head}{}}}", a.suffix(node_ack(node), json_string));
                                // Недостача подтверждений — **не** `200`.
                                //
                                // Запись у primary есть, и это правда; но клиент
                                // просил `quorum`, а получил `one`, и разница
                                // ровно в том, переживёт ли запись отказ. Ответить
                                // «принято» значило бы продать ему не то, что он
                                // купил, — и узнал бы он об этом при отказе.
                                let code = if a.enough() { 200 } else { 503 };
                                return http::respond_with(stream, code, &payload, keep);
                            }
                        }
                    }
                    Ok(Err(_)) => {
                        return http::respond_with(
                            stream,
                            500,
                            "{\"error\":\"bulk-upsert получил конфликт без условий\"}",
                            keep,
                        );
                    }
                    Err(e) => {
                        let payload = format!("{{\"error\":{}}}", json_string(&e));
                        return http::respond_with(stream, 503, &payload, keep);
                    }
                }
            }
        };
        return http::respond_with(stream, 200, &payload, keep);
    }

    // Тело и dialect разбираются до ReadView: это позволяет учесть body
    // timeout в том же guard, который отменяет cold capture, а не начинать
    // отсчёт после него.
    let raw: serde_json::Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(e) => {
            let payload = format!(
                "{{\"error\":{}}}",
                json_string(&format!("тело не разобралось как JSON: {e}"))
            );
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    let date_fields = es::date_range_fields(&raw);
    let mut request = match es::dialect(&raw) {
        Ok(es::Dialect::Native) => raw,
        Ok(es::Dialect::Es) => match es::translate(&raw) {
            Ok(v) => v,
            Err(e) => {
                let payload = format!("{{\"error\":{}}}", json_string(&e));
                return http::respond_with(stream, 400, &payload, keep);
            }
        },
        Err(e) => {
            let payload = format!("{{\"error\":{}}}", json_string(&e));
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    let pit_ask = match parse_pit(request.get("pit")) {
        Ok(pit) => pit,
        Err(error) => {
            let payload = format!("{{\"error\":{}}}", json_string(&error));
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    let request_guard = match search_guard(&req, &request) {
        Ok(guard) => guard,
        Err(error) => {
            let payload = format!("{{\"error\":{}}}", json_string(&error));
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    // Даже короткое снятие ReadView может читать холодные live bitmaps. Теперь
    // body/header deadline уже принадлежит этому guard; disconnect во время
    // capture не ждёт последующего этапа планирования.
    let capture_guard = Arc::clone(&request_guard);
    let capture_disconnect = DisconnectWatch::start(stream, Arc::clone(&capture_guard));
    let (mut snap, automatic_live, read_generation) = match search_view {
        Some(view) => (Arc::clone(&view.segments), Some(Arc::clone(&view.live)), None),
        None => {
            let (held, frozen) = match capture_pit(live, writer, Some(&capture_guard)) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return http::respond_with(
                        stream,
                        503,
                        &format!(
                            "{{\"error\":{},\"retryable\":true,\"reason\":{}}}",
                            json_string(&error),
                            json_string(if capture_guard.fired() {
                                capture_guard.reason().name()
                            } else {
                                "read_view_raced"
                            })
                        ),
                        keep,
                    )
                }
            };
            (
                held,
                Some(frozen),
                writer.map(|writer| writer.visibility_generation.load(Ordering::Acquire)),
            )
        }
    };
    let mut names = field_names_of(&snap, &node.declared);
    let mut kinds = field_kinds_of(&snap, &node.declared);
    let mut view_generation = read_generation
        .and_then(|generation| writer.map(|writer| (&writer.visibility_generation, generation)));
    let mut pit_reply: Option<String> = None;
    let mut pit_live: Option<FrozenLive> = automatic_live;
    if let Some(PitAsk { id: Some(id), ttl_ms }) = pit_ask.as_ref() {
        if search_view.is_some_and(LocalSearchView::is_multi_target) {
            return http::respond_with(
                stream,
                400,
                "{\"error\":\"PIT пока открывается только для одного индекса; multi-index ReadView требует отдельного PIT каждого индекса\"}",
                keep,
            );
        }
        let Some((held, frozen)) = pit_registry().get(id, &node.dir, *ttl_ms) else {
            return http::respond_with(
                stream,
                404,
                "{\"error\":\"PIT не найден или истёк\",\"reason\":\"pit_expired\"}",
                keep,
            );
        };
        snap = held;
        pit_live = Some(frozen);
        pit_reply = Some(id.clone());
        // Изменения текущего ReadView к закреплённому PIT не относятся.
        names = field_names_of(&snap, &node.declared);
        kinds = field_kinds_of(&snap, &node.declared);
    }
    normalize_mapping_query(&mut request, &node.declared.mapping, &names, &kinds);
    drop(capture_disconnect);
    let require_complete =
        request.get("partial").and_then(|value| value.as_bool()).is_some_and(|partial| !partial);
    let work =
        WorkContext::new(ResourceClass::Interactive, Arc::clone(&request_guard), require_complete);
    work.set_phase(WorkPhase::Planned);
    let _disconnect = DisconnectWatch::start(stream, Arc::clone(&request_guard));
    // Сверка на `noindex` — **до** обеих ветвей, булевой и векторной: поле,
    // объявленное «хранить, не искать», не ищется ни тем способом, ни другим.
    if let Err(msg) = refuse_unsearchable(&request, &names, &field_searched_of(&snap)) {
        let payload = format!("{{\"error\":{}}}", json_string(&msg));
        return http::respond_with(stream, 400, &payload, keep);
    }
    // Поиск по вектору — отдельная ветка, а не разновидность булева запроса.
    //
    // У него нет ни слов, ни весов BM25, ни обхода списков: он читает коды
    // подряд и считает расстояние. Пропускать его через разбор булева запроса
    // значило бы объявить общими вещи, у которых общего только имя «поиск».
    if request.get("vector").is_some() {
        if pit_ask.is_some() {
            return http::respond_with(
                stream,
                400,
                "{\"error\":\"PIT для vector search пока не поддержан: vector scan читает отдельный live predicate\"}",
                keep,
            );
        }
        work.set_phase(WorkPhase::Queued);
        work.set_phase(WorkPhase::Yielded);
        let Some(search_permit) = resources().enter_until_with_units(
            ResourceClass::Interactive,
            Some(&request_guard),
            threads,
        ) else {
            http::set_extra("Retry-After: 1\r\n".to_string());
            let reason = if request_guard.fired() {
                request_guard.reason().name()
            } else {
                "workload_queue"
            };
            let status = if request_guard.fired() { 503 } else { 429 };
            return http::respond_with(
                stream,
                status,
                &format!(
                    "{{\"error\":\"interactive search queue не смогла начать работу\",\"retryable\":true,\"reason\":{}}}",
                    json_string(reason)
                ),
                keep,
            );
        };
        work.set_units(search_permit.cpu_units(), search_permit.io_units());
        let _query_lease =
            query_registry().enter_context(&work, &node.dir, ResourceClass::Interactive);
        // Vector и lexical расходуют один и тот же резервируемый бюджет.
        let _active = Inflight::enter(inflight);
        let share = search_permit.cpu_units;
        let schema = SearchSchema {
            names: &names,
            kinds: &kinds,
            source_fid: node.declared.source_fid,
            projection_fid: node.declared.projection_fid,
        };
        let read = KnnReadView { segments: &snap, owner: search_view, generation: view_generation };
        work.set_phase(WorkPhase::Running);
        serve_knn(stream, keep, &request, read, schema, share, work.guard());
        work.set_phase(WorkPhase::Responding);
        return;
    }
    for field in &date_fields {
        if !names
            .iter()
            .position(|name| name == field)
            .is_some_and(|at| matches!(kinds.get(at), Some(Kind::Date | Kind::DateIso)))
        {
            return http::respond_with(
                stream,
                400,
                &serde_json::json!({"error":{"type":"query_parsing_exception",
                    "reason":format!("date bound requires a date field: {field}")},"status":400})
                .to_string(),
                keep,
            );
        }
    }
    let groups = group_ids_of(&snap);
    let slot_groups = slot_groups_of(&snap);
    let mut spec = match parse_query_value(&request, &names, &kinds, &groups, &slot_groups) {
        Ok(spec) => spec,
        Err(msg) => {
            let payload = format!("{{\"error\":{}}}", json_string(&msg));
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    spec.guard = Some(Arc::clone(&request_guard));
    spec.expansion = Some(Arc::new(svindb_core::segments::ExpansionBudget::new(
        spec.max_expansions,
        spec.max_expansion_examined,
    )));
    if let Some(PitAsk { id: None, ttl_ms }) = pit_ask.as_ref() {
        if search_view.is_some_and(LocalSearchView::is_multi_target) {
            return http::respond_with(
                stream,
                400,
                "{\"error\":\"PIT пока открывается только для одного индекса\"}",
                keep,
            );
        }
        // `snap` и `pit_live` уже сняты одной seqlock-операцией выше. Второй
        // capture здесь создавал бы ненужное окно между тем ReadView, по
        // которому выполнится первая страница, и тем, который закрепит PIT.
        let held = Arc::clone(&snap);
        let frozen = match pit_live.as_ref() {
            Some(frozen) => Arc::clone(frozen),
            None => {
                return http::respond_with(
                    stream,
                    500,
                    "{\"error\":\"immutable live view для PIT не был снят\"}",
                    keep,
                )
            }
        };
        let id =
            match pit_registry().insert(&node.dir, Arc::clone(&held), Arc::clone(&frozen), *ttl_ms)
            {
                Ok(id) => id,
                Err(error) => {
                    http::set_extra("Retry-After: 1\r\n".to_string());
                    let payload = format!(
                        "{{\"error\":{},\"retryable\":true,\"reason\":\"pit_admission\"}}",
                        json_string(&error)
                    );
                    return http::respond_with(stream, 429, &payload, keep);
                }
            };
        snap = held;
        pit_live = Some(frozen);
        pit_reply = Some(id);
    }
    spec.live = pit_live;
    // Текстовый/aggregation путь читает frozen live bitmaps. Последующая
    // мутация текущего ReadView к нему не относится и не требует 503.
    view_generation = None;
    if node.declared.source_fid.is_none() {
        if let Some(&field) =
            spec.highlight.iter().find(|&&field| !node.declared.field_stored(field))
        {
            let name = names.get(field).map(String::as_str).unwrap_or("");
            let payload = format!(
                "{{\"error\":{}}}",
                json_string(&format!(
                    "highlight поля {name:?} невозможен: у индекса отключён _source, а поле не объявлено store:true"
                ))
            );
            return http::respond_with(stream, 400, &payload, keep);
        }
    }
    let aggs = match parse_aggs(&request, &names, &kinds, &groups, &slot_groups) {
        Ok(a) => a,
        Err(msg) => {
            let payload = format!("{{\"error\":{}}}", json_string(&msg));
            return http::respond_with(stream, 400, &payload, keep);
        }
    };
    if let Err(error) = snap.numeric_compatibility(&spec, &aggs) {
        return http::respond_with(
            stream,
            422,
            &format!(
                "{{\"error\":{},\"retryable\":false,\"reason\":\"index_migration_required\"}}",
                json_string(&error),
            ),
            keep,
        );
    }
    // `size: 0` с aggregation — это count/analytics без выдачи. Верхушку
    // строить всё равно не нужно: точный счёт считается отдельным проходом по
    // совпадениям, а пустой ответ не должен занимать ни CPU, ни память top-K.
    // Без aggregation оставляем прежний путь, потому что обычный search пока
    // не обязан публиковать total hits.
    let count_only =
        request.get("size").and_then(serde_json::Value::as_u64).is_some_and(|size| size == 0)
            && !aggs.is_empty();
    // Collector-ы создаются на рабочих потоках aggregation. Scalar count
    // имеет фиксированное состояние, bucket-агрегации — собственный предел;
    // умножение на ширину покрывает одновременные локальные Sink-и и остаётся
    // верхней оценкой для admission.
    let aggregation_memory_bytes =
        svindb_core::aggs::memory_hint(&aggs).saturating_mul(threads.clamp(1, 8));
    let count_documents = if aggs.is_empty()
        || search_resource_class(&spec, &aggs, Some(0)) == ResourceClass::Analytics
    {
        None
    } else {
        snap.candidate_document_bound(&spec, INTERACTIVE_COUNT_DOCUMENTS)
    };
    let resource_class = search_resource_class(&spec, &aggs, count_documents);
    work.set_class(resource_class);
    work.set_phase(WorkPhase::Queued);
    work.set_phase(WorkPhase::Yielded);
    let Some(search_permit) =
        resources().enter_until_with_units(resource_class, Some(&request_guard), threads)
    else {
        http::set_extra("Retry-After: 1\r\n".to_string());
        let reason =
            if request_guard.fired() { request_guard.reason().name() } else { "workload_queue" };
        let status = if request_guard.fired() { 503 } else { 429 };
        let payload = format!(
            "{{\"error\":\"resource queue {} не смогла начать работу\",\"retryable\":true,\"reason\":{},\"queue\":{}}}",
            resource_class.name(),
            json_string(reason),
            json_string(resource_class.name()),
        );
        return http::respond_with(stream, status, &payload, keep);
    };
    let _query_lease = query_registry().enter_context(&work, &node.dir, resource_class);
    let source_nested = request.get("source_nested");
    if source_nested.is_some() && !aggs.is_empty() {
        return http::respond_with(
            stream,
            400,
            "{\"error\":\"агрегации вместе с hierarchical nested пока отвергаются: source verifier не вправе считать coarse-кандидатов как точные\"}",
            keep,
        );
    }
    let requested_k = spec.k;
    const SOURCE_NESTED_CANDIDATES: usize = 100_000;
    let source_nested_limit = if source_nested.is_some() {
        let configured = if spec.max_verify == 0 {
            SOURCE_NESTED_CANDIDATES
        } else {
            spec.max_verify.min(SOURCE_NESTED_CANDIDATES)
        };
        let limit = configured.max(requested_k);
        spec.k = limit;
        Some(limit)
    } else {
        None
    };

    // Первый круг точного режима: шард отдаёт свою статистику и ничего не ищет.
    //
    // Отдельная точка для этого не нужна и вредна: статистика считается по уже
    // раскрытому запросу, а раскрытие — это разбор того же тела. Своя точка
    // означала бы второй разбор и второй способ разойтись с ним.
    if request.get("_stats_only").and_then(|v| v.as_bool()) == Some(true) {
        let segs = Arc::clone(&snap);
        let (total, df) = segs.stats(&spec);
        let (expansion_examined, expansion_accepted, expansion_partial) = spec
            .expansion
            .as_ref()
            .map(|budget| {
                let (examined, accepted) = budget.progress();
                (examined, accepted, budget.truncated())
            })
            .unwrap_or((0, 0, false));
        if view_generation.is_some_and(|(current, read)| current.load(Ordering::Acquire) != read) {
            return http::respond_with(
                stream,
                503,
                "{\"error\":\"ReadView изменился во время статистики; повторите запрос\",\"retryable\":true,\"reason\":\"read_view_changed\"}",
                keep,
            );
        }
        let allow_partial =
            request.get("partial").and_then(|value| value.as_bool()).unwrap_or(true);
        if expansion_partial && !allow_partial {
            return http::respond_with(
                stream,
                503,
                "{\"error\":\"prefix expansion не завершён в объявленных пределах\",\"retryable\":true,\"reason\":\"expansion_budget\"}",
                keep,
            );
        }
        let completion = if expansion_partial {
            "\"partial\":true,\"complete\":false,\"terminated\":\"expansion_budget\","
        } else {
            "\"partial\":false,\"complete\":true,"
        };
        let payload = format!(
            "{{{completion}\"total\":{total},\"df\":[{}],\"expansion\":{{\"examined\":{expansion_examined},\"accepted\":{expansion_accepted},\"examined_limit\":{},\"accepted_limit\":{}}}}}",
            df.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(","),
            spec.max_expansion_examined,
            spec.max_expansions,
        );
        return http::respond_with(stream, 200, &payload, keep);
    }
    // Второй круг: общая статистика пришла снаружи и заменяет местную.
    let global: Option<(usize, Vec<u64>)> = request.get(GLOBAL_STATS).and_then(|g| {
        let total = g.get("total")?.as_u64()? as usize;
        let df: Vec<u64> = g.get("df")?.as_array()?.iter().filter_map(|x| x.as_u64()).collect();
        Some((total, df))
    });

    let started = Instant::now();
    let segs = Arc::clone(&snap);
    // Размыкатель: отказ **до** начала работы, а не падение узла посреди неё.
    //
    // Цена запроса зависит не только от него: рабочая память растёт с числом
    // сегментов, которого спрашивающий не видит. Узел, умерший по памяти, теряет
    // все остальные запросы, а не только этот. Разбор — у `budget::Ask`.
    let source_response_bytes =
        if request.get("source").and_then(serde_json::Value::as_bool).unwrap_or(false) {
            2_048
        } else {
            0
        };
    // Число реально существующих документов — строгий верхний предел heap,
    // даже если source verifier попросил большой coarse-candidate budget.
    // Иначе три документа с max_verify=100000 резервировали сотни МиБ.
    let corpus_docs =
        segs.list.iter().fold(0usize, |sum, segment| sum.saturating_add(segment.docs));
    // Ограничивается и настоящий executor: одной меньшей оценки недостаточно,
    // поскольку Vec::with_capacity(k + 1) иначе выделит прежний большой heap.
    // Поисковые collector-ы требуют k>=1; пустой corpus всё равно не имеет jobs.
    spec.k = spec.k.min(corpus_docs.max(1));
    let query_k = if count_only { 0 } else { spec.k.saturating_add(spec.from).min(corpus_docs) };
    let ask = svindb_core::budget::Ask {
        segments: segs.list.len(),
        k: query_k,
        clauses: spec.words.len()
            + spec.must.len()
            + spec.must_not.len()
            + spec.phrases.len()
            + spec.filter.len(),
        aggs: aggs.len(),
        response_bytes: query_k.saturating_mul(
            256usize
                .saturating_add(spec.fields.len().saturating_mul(256))
                .saturating_add(spec.highlight.len().saturating_mul(512))
                .saturating_add(source_response_bytes),
        ),
    };
    let estimated_query_bytes =
        ask.bytes_with_execution(aggregation_memory_bytes, search_permit.cpu_units);
    let budget_mb = search_view.map_or_else(
        || node.live_settings.read().map(|s| s.memory_budget_mb).unwrap_or(0),
        |view| view.memory_budget_mb,
    );
    if let Err(e) = ask.check_with_execution(
        (budget_mb as usize).saturating_mul(1 << 20),
        aggregation_memory_bytes,
        search_permit.cpu_units,
    ) {
        let payload = format!("{{\"error\":{}}}", json_string(&e));
        node.metrics.refused_by_breaker();
        // 429, а не 400: запрос сам по себе законен, узлу его сейчас не поднять.
        return http::respond_with(stream, 429, &payload, keep);
    }
    // Статический budget ограничивает форму запроса, а динамический admission
    // смотрит на узел сейчас: cgroup current уже включает resident mappings,
    // page cache, активный merge, ingest и соседние point reads. Control reserve
    // вычитается из headroom и потому остаётся реально свободным, а не только
    // нарисованным отдельной долей при старте.
    let pressure = svindb_core::mem::pressure();
    let configured_latency_reserve = resources().latency_reserve_bytes;
    let latency_reserve = effective_latency_reserve(pressure.limit, configured_latency_reserve);
    if pressure.near_limit(latency_reserve) || pressure.overloaded() {
        // Source cache ускоряет повторные ответы, но никогда не важнее control
        // и point reserve. Сбрасываем только собственные immutable copies;
        // canonical source и page cache этим не затрагиваются.
        node.source_cache.clear();
    }
    // `memory.current` уже включает физически видимые mappings, frozen
    // generations и page cache. Внутренний ledger здесь служит только нижней
    // границей на случай задержки cgroup sample: берём максимум наблюдаемого
    // current и собственных retained/reserved bytes, не складывая пересекающиеся
    // RSS/file/mmap counters второй раз.
    let internal_floor = runtime_memory_floor(writer, &snap, Some(&node.source_cache));
    let accounted_current = pressure.current.unwrap_or(0).max(internal_floor);
    if pressure.overloaded() {
        let payload = format!(
            "{{\"error\":\"memory PSI указывает на latency collapse; тяжёлый запрос временно отвергнут\",\
             \"retryable\":true,\"psi_some_avg10\":{},\"psi_full_avg60\":{}}}",
            pressure.psi_some_avg10.map_or("null".to_string(), |v| format!("{v:.3}")),
            pressure.psi_full_avg60.map_or("null".to_string(), |v| format!("{v:.3}")),
        );
        node.metrics.refused_by_breaker();
        return http::respond_with(stream, 503, &payload, keep);
    }
    // Маленькие point/interactive ответы ещё можно обслужить из оставшегося
    // headroom. Analytics и большие страницы при почти полном cgroup иначе
    // запускают прямой reclaim и вытесняют рабочий page cache соседних
    // запросов, поэтому получают быстрый retryable отказ.
    if pressure.near_limit(latency_reserve)
        && (resource_class == ResourceClass::Analytics
            || spec.k > 1_000
            || spec.words.len() > 32
            || estimated_query_bytes >= 16 * 1024 * 1024)
    {
        let payload = format!(
            "{{\"error\":\"cgroup memory близок к пределу; тяжёлый запрос временно отложен\",\
             \"retryable\":true,\"reason\":\"memory_pressure\",\
             \"latency_reserve_bytes\":{},\"current_bytes\":{},\"limit_bytes\":{}}}",
            latency_reserve,
            accounted_current,
            pressure.limit.unwrap_or(0),
        );
        node.metrics.refused_by_breaker();
        return http::respond_with(stream, 503, &payload, keep);
    }
    // Для небольшого interactive запроса clean file cache фонового merge
    // является вытесняемым. Защищаем двойной latency reserve как рабочий cache;
    // остальное ядро может освободить. При PSI overload этот путь уже отсечён
    // выше, то есть reclaim thrash не маскируется словом «cache».
    let protected_file_cache = latency_reserve.saturating_mul(2);
    if let Some(headroom) = pressure
        .headroom_after_file_reclaim(latency_reserve, protected_file_cache)
        .or_else(|| pressure.headroom(latency_reserve))
    {
        let headroom = headroom
            .saturating_sub(accounted_current.saturating_sub(pressure.current.unwrap_or(0)));
        if (estimated_query_bytes as u64) > headroom {
            let payload = format!(
                "{{\"error\":\"динамический memory admission сохранил control reserve\",\
                 \"retryable\":true,\"want_bytes\":{},\"headroom_bytes\":{headroom},\
                 \"control_reserve_bytes\":{},\"latency_reserve_bytes\":{},\
                 \"current_bytes\":{},\"limit_bytes\":{}}}",
                estimated_query_bytes,
                svindb_core::govern::CONTROL_BYTES,
                latency_reserve,
                accounted_current,
                pressure.limit.unwrap_or(0),
            );
            node.metrics.refused_by_breaker();
            return http::respond_with(stream, 429, &payload, keep);
        }
    }
    // Предел у **суммы**, а не только у каждого запроса по себе.
    //
    // Место держится до конца обслуживания запроса и возвращается на любом
    // пути, включая ранний отказ: `Permit` освобождает его в `Drop`.
    let _room = match node.query_pool.reserve(estimated_query_bytes) {
        Ok(room) => room,
        Err(full) => {
            let payload = format!(
                "{{\"error\":{},\"want_bytes\":{},\"used_bytes\":{},\"limit_bytes\":{},\
                 \"retryable\":true}}",
                json_string(
                    "узел сейчас не поднимет столько памяти под запросы; повторите с backoff"
                ),
                full.want,
                full.used,
                full.max
            );
            node.metrics.refused_by_breaker();
            return http::respond_with(stream, 429, &payload, keep);
        }
    };
    work.reserve_memory(estimated_query_bytes);
    work.set_units(search_permit.cpu_units(), search_permit.io_units());
    // Счётчики обнуляются перед запросом, а не копятся: поток обслуживает одно
    // соединение, и снимок относится ровно к нему.
    // Сколько потоков дать этому запросу — решается по загрузке, а не заранее.
    //
    // Ширина закреплена permit: следующий запрос не перераздаёт CPU,
    // которые этот executor уже использует.
    let active = Inflight::enter(inflight);
    let share = search_permit.cpu_units;

    let _ = stats::take();
    let core_started = stats::ENABLED.then(Instant::now);
    let fused_document_count = !count_only && svindb_core::aggs::document_counts_only(&aggs);
    let mut fused_count = None;
    let mut total_hits: Option<(u64, bool)> = None;
    let track_total = spec.track_total_hits;
    let need_total =
        !count_only && !matches!(track_total, svindb_core::segments::TrackTotalHits::Omit);
    let mut out: Outcome = if count_only {
        // Exact aggregation ниже всё равно обходит каждый допустимый документ;
        // search_with здесь был бы бесполезным дублированием candidate pass.
        Outcome { hits: Vec::new(), visited: 0, partial: false }
    } else if fused_document_count || need_total {
        let mut count_spec = spec.clone();
        if fused_document_count {
            count_spec.track_total_hits = svindb_core::segments::TrackTotalHits::Exact;
        }
        let (out, count) = segs.search_count_with_g(
            &count_spec,
            share,
            global.as_ref().map(|(total, df)| (*total, df.as_slice())),
        );
        if fused_document_count {
            fused_count = Some(count);
        }
        if need_total {
            let (value, exact) = match track_total {
                svindb_core::segments::TrackTotalHits::Exact => (count, !out.partial),
                svindb_core::segments::TrackTotalHits::UpTo(limit) => {
                    (count.min(limit), !out.partial && count <= limit)
                }
                svindb_core::segments::TrackTotalHits::Omit => (0, false),
            };
            total_hits = Some((value, exact));
        }
        out
    } else {
        match global.as_ref() {
            Some((total, df)) => segs.search_with_g(&spec, share, Some((*total, df.as_slice()))),
            None => segs.search_with(&spec, share),
        }
    };
    drop(active);
    if let (Some(query), Some(limit)) = (source_nested, source_nested_limit) {
        if out.visited > limit as u64 {
            let payload = format!(
                "{{\"error\":{}}}",
                json_string(&format!(
                    "hierarchical nested дал {} coarse-кандидатов при бюджете {limit}; сузьте запрос или увеличьте max_verify в пределах {SOURCE_NESTED_CANDIDATES}",
                    out.visited
                ))
            );
            node.metrics.refused_by_breaker();
            return http::respond_with(stream, 429, &payload, keep);
        }
        let Some(source_fid) = node.declared.source_fid else {
            return http::respond_with(
                stream,
                400,
                "{\"error\":\"hierarchical nested требует lossless_document_source\"}",
                keep,
            );
        };
        let docs: Vec<u64> = out.hits.iter().map(|hit| hit.doc).collect();
        let sources =
            segs.fetcher(FETCH_CACHE).many_guarded(&docs, source_fid, share, Some(&request_guard));
        let mut exact = Vec::with_capacity(out.hits.len());
        for (hit, raw) in out.hits.drain(..).zip(sources) {
            if !request_guard.check() {
                break;
            }
            let Some(raw) = raw else { continue };
            let source: serde_json::Value = match serde_json::from_slice(&raw) {
                Ok(source) => source,
                Err(error) => {
                    let payload = format!(
                        "{{\"error\":{}}}",
                        json_string(&format!(
                            "lossless source документа {} повреждён: {error}",
                            hit.doc
                        ))
                    );
                    return http::respond_with(stream, 500, &payload, keep);
                }
            };
            let projection = match nested::normalize_projection(&source, &node.declared.mapping) {
                Ok(projection) => projection,
                Err(reason) => {
                    return http::respond_with(
                        stream,
                        500,
                        &serde_json::json!({"error":{"type":"source_verification_exception",
                        "reason":reason},"status":500})
                        .to_string(),
                        keep,
                    )
                }
            };
            match source_query_matches(&projection, query, &names, &kinds) {
                Ok(true) => exact.push(hit),
                Ok(false) => {}
                Err(error) => {
                    let payload = format!("{{\"error\":{}}}", json_string(&error));
                    return http::respond_with(stream, 400, &payload, keep);
                }
            }
        }
        exact.truncate(requested_k);
        out.hits = exact;
    }
    if request.get("size").and_then(serde_json::Value::as_u64) == Some(0) {
        out.hits.clear();
    }
    let core_ms = core_started.map_or(0.0, |started| started.elapsed().as_secs_f64() * 1000.0);

    let hydrate_started = stats::ENABLED.then(Instant::now);
    let mut fetcher = segs.fetcher(FETCH_CACHE);
    // Содержимое достаётся **разом на всю страницу**, а не по документу.
    //
    // Прежде здесь грелся кеш кусков (`prepare`), а сам ответ звал `get` на
    // каждое попадание. Пока страница мала и документы рядом, это верно. Но у
    // верхушки в десять тысяч документы разбросаны по всему индексу, и почти
    // каждый лежит в своём куске: кусок весит килобайты, десять тысяч кусков —
    // сотни мегабайт, а потолок кеша восемь. Кеш вытеснял прогретое, и каждый
    // кусок проходил распаковку дважды.
    //
    // Замерено на 25 млн, `k = 10 000` с выдачей поля: 580 мс, из них по профилю
    // 98% в `DecoderDict::decompress`. Разбор — у `Fetcher::many`.
    let page: Vec<u64> = out.hits.iter().skip(spec.from).map(|h| h.doc).collect();
    let mut want: Vec<usize> = spec.fields.iter().chain(spec.highlight.iter()).copied().collect();
    let want_source = request.get("source").and_then(|value| value.as_bool()).unwrap_or(false);
    if want_source {
        if let Some(fid) = node.declared.source_fid {
            want.push(fid);
        }
    }
    if !spec.fields.is_empty() || !spec.highlight.is_empty() {
        if let Some(fid) = node.declared.projection_fid {
            want.push(fid);
        }
        if let Some(fid) = node.declared.source_fid {
            want.push(fid);
        }
    }
    want.sort_unstable();
    want.dedup();
    // Поле → значения по документам страницы, в порядке страницы.
    let source_fid = node.declared.source_fid;
    let source_generation = snap.generation();
    let source_cache = Arc::clone(&node.source_cache);
    let bulk: Vec<(usize, Vec<Option<Vec<u8>>>)> = want
        .iter()
        .map(|&f| {
            let values = if source_fid == Some(f) {
                source_cache.many(source_generation, &page, Some(&request_guard), |docs, guard| {
                    fetcher.many_guarded(docs, f, share, guard)
                })
            } else {
                fetcher.many_guarded(&page, f, share, Some(&request_guard))
            };
            (f, values)
        })
        .collect();
    let (source_values, source_bytes): (usize, usize) = if stats::ENABLED {
        (
            bulk.iter().map(|(_, values)| values.iter().filter(|v| v.is_some()).count()).sum(),
            bulk.iter()
                .flat_map(|(_, values)| values.iter().filter_map(|v| v.as_ref()))
                .map(Vec::len)
                .sum(),
        )
    } else {
        (0, 0)
    };
    work.record_materialized(source_bytes);
    let value = |f: usize, at: usize| -> Option<&[u8]> {
        bulk.iter().find(|(k, _)| *k == f).and_then(|(_, v)| v.get(at)).and_then(|x| x.as_deref())
    };
    // Срез страницы: верхушка набиралась на `from + size`, наружу идёт хвост.
    let items: Vec<String> = out
        .hits
        .iter()
        .skip(spec.from)
        .take_while(|_| request_guard.check())
        .enumerate()
        .map(|(at, h)| {
            let source_json = node
                .declared
                .source_fid
                .and_then(|fid| value(fid, at))
                .and_then(|raw| std::str::from_utf8(raw).ok());
            let projection_json = node
                .declared
                .projection_fid
                .and_then(|fid| value(fid, at))
                .filter(|raw| !raw.is_empty())
                .and_then(|raw| std::str::from_utf8(raw).ok())
                .or(source_json);
            // При сортировке по полю наружу идёт **значение**, а не ранг: ранг
            // это внутренняя величина, и принимать его обратно курсором страницы
            // означало бы просить клиента знать про наш знак минус.
            // Содержимое — только если его попросили: распаковка куска на
            // документ стоит около девяти микросекунд, и платить за неё обязан
            // тот, кто её заказал.
            let fields = if spec.fields.is_empty() {
                String::new()
            } else if let Some(projected) = projection_json {
                let body = selected_fields_from_projection(projected, &names, &spec.fields)
                    .unwrap_or_default();
                format!(",\"fields\":{{{body}}}")
            } else {
                let parts: Vec<String> = spec
                    .fields
                    .iter()
                    .filter_map(|&f| {
                        if !request_guard.check() {
                            return None;
                        }
                        // Содержимое приходит байтами: строкой оно станет только
                        // здесь, и негодный UTF-8 обязан дать пустое значение, а
                        // не уронить ответ. Отсутствующая stored-копия при
                        // отключённом `_source` означает «значение недоступно»,
                        // а не ложную пустую строку.
                        let raw = value(f, at)?;
                        let text = String::from_utf8_lossy(raw).into_owned();
                        Some(format!(
                            "{}:{}",
                            json_string(&names[f]),
                            json_value(&text, kinds.get(f))
                        ))
                    })
                    .collect();
                format!(",\"fields\":{{{}}}", parts.join(","))
            };
            // Подсветка: содержимое берётся из хранилища и разбирается заново.
            // Слова запроса — те же, что ушли в поиск, а не введённые
            // пользователем: подсветить надо то, что нашлось.
            let highlight = if spec.highlight.is_empty() {
                String::new()
            } else {
                let tags = svindb_core::highlight::Tags::default();
                let parts: Vec<String> = spec
                    .highlight
                    .iter()
                    .filter_map(|&f| {
                        if !request_guard.check() {
                            return None;
                        }
                        let text = projection_json
                            .and_then(|raw| projection_text(raw, &names[f]))
                            .or_else(|| {
                                value(f, at).map(|v| String::from_utf8_lossy(v).into_owned())
                            })?;
                        // Фраза по этому полю — если она есть, подсвечивается
                        // она целиком, а не её слова порознь.
                        let ph = spec.phrases.iter().find(|p| p.fid == f);
                        let snip = match ph {
                            Some(p) => svindb_core::highlight::snippet(
                                &text,
                                &p.terms,
                                Some((&p.terms, p.slop)),
                                spec.highlight_size,
                                &tags,
                            ),
                            None => {
                                let terms: Vec<String> = spec
                                    .words
                                    .iter()
                                    .chain(spec.must.iter())
                                    .filter(|w| w.fid == f)
                                    .flat_map(|w| w.terms.iter().map(|t| t.text.clone()))
                                    .collect();
                                svindb_core::highlight::snippet(
                                    &text,
                                    &terms,
                                    None,
                                    spec.highlight_size,
                                    &tags,
                                )
                            }
                        }?;
                        Some(format!("{}:{}", json_string(&names[f]), json_string(&snip)))
                    })
                    .collect();
                if parts.is_empty() {
                    String::new()
                } else {
                    format!(",\"highlight\":{{{}}}", parts.join(","))
                }
            };
            // Идентификатор берётся у того же выборщика, что и содержимое:
            // перевод «общий номер → сегмент и местный номер» обязан быть в
            // одном месте, иначе он разойдётся сам с собой.
            let id = if spec.with_ids {
                let id = fetcher.id(h.doc).unwrap_or_default();
                format!(",\"id\":{},\"_id\":{}", json_string(&id), json_string(&id))
            } else {
                String::new()
            };
            let source = if want_source {
                source_json.map(|raw| format!(",\"_source\":{raw}")).unwrap_or_default()
            } else {
                String::new()
            };
            let index = search_view
                .and_then(|view| view.index_of(h.doc))
                .map(|name| format!(",\"_index\":{}", json_string(name)))
                .unwrap_or_default();
            if spec.sort.is_empty() {
                format!(
                    "{{\"doc\":{}{}{},\"score\":{}{}{}{}}}",
                    h.doc, id, index, h.score, fields, highlight, source
                )
            } else {
                // Наружу идут **значения**, а не нормированные ключи: нормировка
                // это внутренняя величина, и просить клиента знать про наше
                // побитовое отрицание значило бы вынести кухню в ответ.
                let vals: Vec<String> = spec
                    .sort
                    .iter()
                    .enumerate()
                    .map(|(i, s)| match svindb_core::search::denorm_key(h.keys[i], s.desc) {
                        None => "null".to_string(),
                        Some(value)
                            if matches!(kinds.get(s.fid), Some(Kind::Float | Kind::Double)) =>
                        {
                            // Debug-форма сохраняет 1.0 и -0.0 как дробные
                            // JSON numbers, включая знак нуля в курсоре.
                            format!("{:?}", svindb_core::index::float_of(value))
                        }
                        Some(value) => value.to_string(),
                    })
                    .collect();
                format!(
                    "{{\"doc\":{}{}{},\"score\":{},\"sort\":[{},{}]{}{}{}}}",
                    h.doc,
                    id,
                    index,
                    h.score,
                    vals.join(","),
                    h.doc,
                    fields,
                    highlight,
                    source
                )
            }
        })
        .collect();
    let hydrate_ms =
        hydrate_started.map_or(0.0, |started| started.elapsed().as_secs_f64() * 1000.0);
    let aggregate_started = stats::ENABLED.then(Instant::now);
    let mut agg_partial = false;
    let aggs_out = if aggs.is_empty() {
        String::new()
    } else {
        let res = if let Some(count) = fused_count {
            // Все document-count aliases получают один и тот же scalar.
            // Полнота уже принадлежит общему matcher, а не второму обходу.
            agg_partial = out.partial;
            aggs.iter()
                .map(|agg| svindb_core::aggs::Result_ {
                    name: agg.name.clone(),
                    space: agg.space,
                    value: svindb_core::aggs::Value::Stats {
                        count,
                        min: None,
                        max: None,
                        sum: 0,
                        fsum: None,
                    },
                })
                .collect()
        } else {
            let counted = segs.aggregate_with(&spec, &aggs, share);
            agg_partial = counted.partial;
            counted.values
        };
        // Переполнение корзин — отказ **запроса**, а не поле внутри ответа:
        // клиент, смотрящий на верхний уровень, поля бы не заметил и принял бы
        // ответ за полный.
        if let Some(r) =
            res.iter().find(|r| matches!(r.value, svindb_core::aggs::Value::TooMany { .. }))
        {
            let limit = match r.value {
                svindb_core::aggs::Value::TooMany { limit } => limit,
                _ => 0,
            };
            let msg = format!(
                "агрегация {:?}: корзин больше {limit} — возьмите шаг покрупнее или сузьте \
                 отбор",
                r.name
            );
            let payload = format!("{{\"error\":{}}}", json_string(&msg));
            return http::respond_with(stream, 400, &payload, keep);
        }
        format!(",\"aggs\":{}", aggs_json(&res, &names))
    };
    let aggregate_ms =
        aggregate_started.map_or(0.0, |started| started.elapsed().as_secs_f64() * 1000.0);
    // Снимок после всех collectors: иначе size:0 показывал нулевую работу,
    // а чтения агрегации терялись между ответом и следующим stats::take().
    let c = stats::take();
    if stats::ENABLED {
        node.metrics.search_stages(
            core_ms,
            hydrate_ms,
            aggregate_ms,
            source_values as u64,
            source_bytes as u64,
        );
    }
    let counters = if stats::ENABLED {
        format!(
            ",\"stats\":{{\"source_values\":{source_values},\
             \"source_bytes\":{source_bytes},\"word_next\":{},\"word_seek\":{},\
             \"sift_levels\":{},\"gather_calls\":{},\"gather_nodes\":{},\
             \"cursor_next\":{},\"cursor_seek\":{},\"cursor_fill\":{},\
             \"scored\":{},\"block_skips\":{},\"prefix_expansions\":{},\
             \"positions_read\":{},\"scan_visited\":{},\
             \"core_ms\":{core_ms:.3},\"hydrate_ms\":{hydrate_ms:.3},\"aggregate_ms\":{aggregate_ms:.3},\
             \"thread_read_bytes\":{},\"thread_write_bytes\":{}}}",
            c.word_next,
            c.word_seek,
            c.sift_levels,
            c.gather_calls,
            c.gather_nodes,
            c.cursor_next,
            c.cursor_seek,
            c.cursor_fill,
            c.scored,
            c.block_skips,
            c.prefix_expansions,
            c.positions_read,
            c.scan_visited,
            c.io_read_bytes,
            c.io_write_bytes
        )
    } else {
        String::new()
    };
    let took = started.elapsed().as_secs_f64() * 1000.0;
    // Неполнота у ответа одна на всех: и оборванная проверка по тексту, и
    // оборванный счёт означают для клиента ровно одно — этому числу нельзя
    // верить как полному. Два разных признака заставили бы его проверять оба.
    if view_generation.is_some_and(|(current, read)| current.load(Ordering::Acquire) != read) {
        return http::respond_with(
            stream,
            503,
            "{\"error\":\"ReadView изменился во время поиска; повторите запрос\",\"retryable\":true,\"reason\":\"read_view_changed\"}",
            keep,
        );
    }
    let (expansion_examined, expansion_accepted, expansion_partial) = spec
        .expansion
        .as_ref()
        .map(|budget| {
            let (examined, accepted) = budget.progress();
            (examined, accepted, budget.truncated())
        })
        .unwrap_or((0, 0, false));
    let incomplete = out.partial || agg_partial || request_guard.fired() || expansion_partial;
    let allow_partial = !work.requires_complete();
    let reason = if request_guard.fired() {
        request_guard.reason().name()
    } else if expansion_partial {
        "expansion_budget"
    } else {
        "verification_budget"
    };
    if incomplete && !allow_partial {
        let payload = format!(
            "{{\"error\":\"поиск не завершён в объявленных пределах\",\"retryable\":true,\"reason\":{}}}",
            json_string(reason)
        );
        return http::respond_with(stream, 503, &payload, keep);
    }
    let completion = if incomplete {
        format!(",\"partial\":true,\"complete\":false,\"terminated\":{}", json_string(reason))
    } else {
        ",\"partial\":false,\"complete\":true".to_string()
    };
    let total_out = total_hits.map_or_else(String::new, |(value, exact)| {
        format!(
            ",\"total\":{{\"value\":{value},\"relation\":{}}}",
            json_string(if exact { "eq" } else { "gte" })
        )
    });
    let pit_out =
        pit_reply.as_ref().map_or_else(String::new, |id| format!(",\"pit\":{}", json_string(id)));
    let expansion_out = format!(
        ",\"expansion\":{{\"examined\":{expansion_examined},\"accepted\":{expansion_accepted},\
         \"examined_limit\":{},\"accepted_limit\":{}}}",
        spec.max_expansion_examined, spec.max_expansions
    );
    let payload = format!(
        "{{\"took_ms\":{:.3},\"visited\":{}{}{},\"hits\":[{}]{}{}{}{}}}",
        took,
        out.visited,
        completion,
        total_out,
        items.join(","),
        aggs_out,
        counters,
        pit_out,
        expansion_out,
    );
    work.set_phase(WorkPhase::Responding);
    http::respond_with(stream, 200, &payload, keep);
}

/// Отвечает на поиск по плотному вектору.
///
/// `{"k":10,"vector":{"field":"v","query":[…]},"fields":["title"]}`
///
/// Наружу идёт `score` — **не** расстояние. Расстояние тем меньше, чем ближе, а
/// вес во всей остальной выдаче тем больше, чем лучше, и отдавать в одном поле
/// две противоположные шкалы нельзя. Преобразование `1/(1+d)` то же, что у
/// Elasticsearch для `l2_norm`: порядок сохраняет, в ноль не упирается,
/// сравнивать веса между запросами не даёт — как и BM25.
struct SearchSchema<'a> {
    names: &'a [String],
    kinds: &'a [Kind],
    source_fid: Option<usize>,
    projection_fid: Option<usize>,
}

struct KnnReadView<'a> {
    segments: &'a Arc<Segments>,
    owner: Option<&'a LocalSearchView>,
    generation: Option<(&'a AtomicU64, u64)>,
}

fn serve_knn(
    stream: &mut wire::Wire,
    keep: bool,
    request: &serde_json::Value,
    read: KnnReadView<'_>,
    schema: SearchSchema<'_>,
    share: usize,
    guard: &svindb_core::work::Guard,
) {
    let KnnReadView { segments: segs, owner: search_view, generation: view_generation } = read;
    let SearchSchema { names, kinds, source_fid, projection_fid } = schema;
    let started = Instant::now();
    let fail = |stream: &mut wire::Wire, msg: String| {
        let payload = format!("{{\"error\":{}}}", json_string(&msg));
        http::respond_with(stream, 400, &payload, keep);
    };
    if let Err(error) = only_keys(
        request,
        "vector query",
        &["k", "vector", "fields", "ids", "source", "timeout_ms", "partial"],
    ) {
        return fail(stream, error);
    }
    let Some(node) = request.get("vector") else {
        return fail(stream, "vector: не задан объект запроса".into());
    };
    if let Err(error) = only_keys(node, "vector", &["field", "query"]) {
        return fail(stream, error);
    }
    let f = match field_ref(node.get("field"), "vector", names) {
        Ok(f) => f,
        Err(e) => return fail(stream, e),
    };
    let Some(dims) = kinds.get(f).and_then(|k| k.dims()) else {
        return fail(
            stream,
            format!(
                "поле {:?} объявлено как {}, а не dense_vector",
                names[f],
                kinds.get(f).map(|k| k.name()).unwrap_or("?")
            ),
        );
    };
    let Some(arr) = node.get("query").and_then(|x| x.as_array()) else {
        return fail(stream, "vector: не задан query".into());
    };
    // Длина сверяется до поиска. Вектор не той длины — это ошибка запроса, а не
    // повод вернуть пустую выдачу: пустая выдача выглядит как «ничего не
    // нашлось», и отличить её от «спросили не то» клиенту нечем.
    if arr.len() != dims {
        return fail(
            stream,
            format!("поле {:?} на {dims} измерений, а в запросе {}", names[f], arr.len()),
        );
    }
    let mut query = Vec::with_capacity(dims);
    for (i, x) in arr.iter().enumerate() {
        match x.as_f64().filter(|v| v.is_finite()) {
            Some(v) => query.push(v as f32),
            None => return fail(stream, format!("vector: значение {i} не число")),
        }
    }
    let k = request.get("k").and_then(|x| x.as_u64()).unwrap_or(10).clamp(1, 10_000) as usize;

    let hits = segs.knn_with_guard(f, &query, k, share, guard);

    // Содержимое — тем же путём, что у булева запроса: кеш кусков на ответ.
    let requested: Vec<usize> = match request.get("fields") {
        Some(value) => {
            let Some(a) = value.as_array() else {
                return fail(stream, "fields: ожидается массив имён".into());
            };
            let mut out = Vec::new();
            for x in a.iter() {
                match field_ref(Some(x), "fields", names) {
                    Ok(i) => out.push(i),
                    Err(e) => return fail(stream, e),
                }
            }
            out
        }
        None => Vec::new(),
    };
    let boolean = |key: &str, default: bool| -> Result<bool, String> {
        match request.get(key) {
            None => Ok(default),
            Some(serde_json::Value::Bool(value)) => Ok(*value),
            Some(_) => Err(format!("{key}: ожидается true или false")),
        }
    };
    let want_source = match boolean("source", false) {
        Ok(value) => value,
        Err(error) => return fail(stream, error),
    };
    let with_ids = match boolean("ids", true) {
        Ok(value) => value,
        Err(error) => return fail(stream, error),
    };
    let mut want = requested.clone();
    if !requested.is_empty() {
        if let Some(field) = projection_fid {
            want.push(field);
        }
        if let Some(field) = source_fid {
            want.push(field);
        }
    }
    if want_source {
        if let Some(field) = source_fid {
            want.push(field);
        }
    }
    want.sort_unstable();
    want.dedup();
    let mut fetcher = segs.fetcher(FETCH_CACHE);
    let docs: Vec<u64> = hits.iter().map(|x| x.0).collect();
    let bulk: Vec<(usize, Vec<Option<Vec<u8>>>)> = want
        .iter()
        .map(|&field| (field, fetcher.many_guarded(&docs, field, share, Some(guard))))
        .collect();
    let value = |field: usize, at: usize| -> Option<&[u8]> {
        bulk.iter()
            .find(|(candidate, _)| *candidate == field)
            .and_then(|(_, values)| values.get(at))
            .and_then(|value| value.as_deref())
    };
    let items: Vec<String> = hits
        .iter()
        .take_while(|_| guard.check())
        .enumerate()
        .map(|(at, &(doc, dist))| {
            let source_json = source_fid
                .and_then(|field| value(field, at))
                .and_then(|raw| std::str::from_utf8(raw).ok());
            let projection_json = projection_fid
                .and_then(|field| value(field, at))
                .filter(|raw| !raw.is_empty())
                .and_then(|raw| std::str::from_utf8(raw).ok())
                .or(source_json);
            let fields = if requested.is_empty() {
                String::new()
            } else if let Some(projected) = projection_json {
                let body = selected_fields_from_projection(projected, names, &requested)
                    .unwrap_or_default();
                format!(",\"fields\":{{{body}}}")
            } else {
                let parts: Vec<String> = requested
                    .iter()
                    .filter_map(|&i| {
                        let text = value(i, at)?;
                        let raw = String::from_utf8_lossy(text);
                        Some(format!(
                            "{}:{}",
                            json_string(&names[i]),
                            json_value(&raw, kinds.get(i))
                        ))
                    })
                    .collect();
                if parts.is_empty() {
                    String::new()
                } else {
                    format!(",\"fields\":{{{}}}", parts.join(","))
                }
            };
            // Идентификатор — как и у обычного поиска: номер документа
            // внутренний, слияние его меняет, и связать выдачу с тем, что
            // клиент хранит у себя, без него нечем.
            let id = if with_ids {
                let id = fetcher.id(doc).unwrap_or_default();
                format!(",\"id\":{},\"_id\":{}", json_string(&id), json_string(&id))
            } else {
                String::new()
            };
            let source = if want_source {
                source_json.map(|raw| format!(",\"_source\":{raw}")).unwrap_or_default()
            } else {
                String::new()
            };
            let index = search_view
                .and_then(|view| view.index_of(doc))
                .map(|name| format!(",\"_index\":{}", json_string(name)))
                .unwrap_or_default();
            format!(
                "{{\"doc\":{}{}{},\"score\":{:.6},\"distance\":{:.4}{}{}}}",
                doc,
                id,
                index,
                1.0 / (1.0 + f64::from(dist).max(0.0).sqrt()),
                dist,
                fields,
                source
            )
        })
        .collect();
    if search_view.is_some_and(LocalSearchView::changed)
        || view_generation.is_some_and(|(current, read)| current.load(Ordering::Acquire) != read)
    {
        return http::respond_with(
            stream,
            503,
            "{\"error\":\"ReadView изменился во время векторного поиска; повторите запрос\",\"retryable\":true,\"reason\":\"read_view_changed\"}",
            keep,
        );
    }
    let incomplete = guard.fired();
    let allow_partial = request.get("partial").and_then(|value| value.as_bool()).unwrap_or(true);
    if incomplete && !allow_partial {
        let payload = format!(
            "{{\"error\":\"поиск не завершён в заданный срок\",\"retryable\":true,\"reason\":{}}}",
            json_string(guard.reason().name())
        );
        return http::respond_with(stream, 503, &payload, keep);
    }
    let completion = if incomplete {
        format!(
            ",\"partial\":true,\"complete\":false,\"terminated\":{}",
            json_string(guard.reason().name())
        )
    } else {
        ",\"partial\":false,\"complete\":true".to_string()
    };
    let payload = format!(
        "{{\"took_ms\":{:.3},\"visited\":{}{},\"hits\":[{}]}}",
        started.elapsed().as_secs_f64() * 1000.0,
        // Перебор полный: посещены все живые документы всех сегментов, где поле
        // есть. Число печатается честное, а не число выданных.
        segs.list
            .iter()
            .filter(|ix| ix.fields.get(f).is_some_and(|x| x.vectors.is_some()))
            .map(|ix| ix.docs as u64)
            .sum::<u64>(),
        completion,
        items.join(",")
    );
    http::respond_with(stream, 200, &payload, keep);
}

/// Разбирает тело запроса.
#[cfg(test)]
fn parse_query(
    body: &[u8],
    names: &[String],
    kinds: &[Kind],
    groups: &[(String, u32)],
    slot_groups: &[(usize, u32)],
) -> Result<QuerySpec, String> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("тело не разобралось как JSON: {e}"))?;
    parse_query_value(&v, names, kinds, groups, slot_groups)
}

fn parse_query_value(
    v: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
    groups: &[(String, u32)],
    slot_groups: &[(usize, u32)],
) -> Result<QuerySpec, String> {
    only_keys(v, "запрос", QUERY_KEYS)?;

    let k = match v.get("k") {
        None => 10usize,
        Some(x) => {
            x.as_u64().ok_or_else(|| "k должно быть целым числом".to_string())?.clamp(1, 10_000)
                as usize
        }
    };
    let max_expansions = match v.get("max_expansions") {
        None => DEFAULT_EXPANSIONS,
        Some(x) => x
            .as_u64()
            .ok_or_else(|| "max_expansions должно быть целым числом".to_string())?
            .clamp(1, 10_000) as usize,
    };
    let max_expansion_examined = match v.get("max_expansion_examined") {
        None => svindb_core::segments::DEFAULT_EXPANSION_EXAMINED,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| "max_expansion_examined должно быть целым числом".to_string())?
            .min(10_000_000) as usize,
    };

    let tree = v.get("query");
    if let Some(tree) = tree {
        only_keys(
            tree,
            "query",
            &["must", "should", "must_not", "filter", "minimum_should_match"],
        )?;
    }
    // Вложенность решается **до** плоского разбора: плоский разборщик на ветви
    // `{"bool": ...}` спотыкается о отсутствие поля, и сообщение получается про
    // поле, а не про то, что запрос надо читать иначе.
    let match_all = match v.get("match_all") {
        None => false,
        Some(serde_json::Value::Bool(value)) => *value,
        Some(_) => return Err("\"match_all\" — это true или false".into()),
    };
    let nested = match tree {
        Some(q) if has_nesting(q) => {
            // Корневой filter разобран отдельно ниже; у вложенного bool он
            // не поддерживается и остаётся ошибкой, а не теряется.
            let mut boolean = q.clone();
            boolean.as_object_mut().expect("query проверен").remove("filter");
            Some(parse_bool(&boolean, names, kinds, groups)?)
        }
        _ if match_all => Some(svindb_core::segments::SpecBool::default()),
        _ => None,
    };
    let flat = nested.is_none();
    let words = match tree.and_then(|q| q.get("should")).filter(|_| flat) {
        Some(x) => parse_clauses(x, "should", names, kinds)?,
        None => match v.get("words") {
            Some(x) => parse_clauses(x, "words", names, kinds)?,
            // Слов может не быть вовсе: запрос из одного отбора законен.
            // Проверка «искать не по чему» стоит ниже, когда разобран и отбор.
            None => Vec::new(),
        },
    };
    let arm = |name: &str| -> Option<&serde_json::Value> {
        tree.and_then(|q| q.get(name)).filter(|_| flat).or_else(|| v.get(name))
    };
    let must = match arm("must") {
        Some(x) => parse_clauses(x, "must", names, kinds)?,
        None => Vec::new(),
    };
    let must_not = match arm("must_not") {
        Some(x) => parse_clauses(x, "must_not", names, kinds)?,
        None => Vec::new(),
    };
    let filter_src = tree.filter(|q| q.get("filter").is_some()).unwrap_or(v);
    let filter = parse_filter(filter_src, names, kinds, slot_groups, groups)?;
    // Фразы: `"phrases":[{"field":1,"terms":["красный","квадрат"],"slop":0}]`.
    //
    // Слова здесь не разбираются анализатором и не раскрываются маской: фраза
    // требует порядка, а маска даёт альтернативы. Приводятся только к нижнему
    // регистру — тем же способом, что при приёме.
    let mut phrases: Vec<svindb_core::segments::SpecPhrase> = Vec::new();
    if let Some(a) = v.get("phrases").and_then(|x| x.as_array()) {
        for (i, p) in a.iter().enumerate() {
            // Поле обязательно. Прежде его отсутствие молча означало поле ноль,
            // и фраза искалась не там, где спрошено, — а какое поле нулевое,
            // клиент не знает вовсе.
            only_keys(p, &format!("phrases[{i}]"), &["field", "terms", "slop", "boost"])?;
            let field = field_ref(p.get("field"), &format!("phrases[{i}]"), names)?;
            let terms: Vec<String> = p
                .get("terms")
                .and_then(|x| x.as_array())
                .map(|t| t.iter().filter_map(|x| x.as_str()).map(|x| x.to_lowercase()).collect())
                .unwrap_or_default();
            if terms.len() < 2 {
                return Err(format!(
                    "phrases[{i}]: во фразе нужно хотя бы два слова, иначе это просто слово"
                ));
            }
            if terms.iter().any(|t| t.contains('*')) {
                return Err(format!(
                    "phrases[{i}]: маска во фразе не поддержана — маска даёт альтернативы, \
                     а фраза требует порядка"
                ));
            }
            let slop = p.get("slop").and_then(|x| x.as_u64()).unwrap_or(0);
            if slop > 1000 {
                return Err(format!("phrases[{i}]: slop {slop} — это уже не фраза"));
            }
            let slop =
                u32::try_from(slop).map_err(|_| format!("phrases[{i}]: slop слишком велик"))?;
            let boost = finite_boost(p.get("boost"), &format!("phrases[{i}].boost"))?;
            // Отказ на границе разбора, а не пустая выдача: клиенту надо
            // сказать, что фразы по такому полю не бывает, а не молча ничего
            // не найти.
            if slot_groups.iter().any(|(f, g)| *f == field && *g != 0) {
                return Err(format!(
                    "phrases[{i}]: поле {:?} лежит внутри массива — позиция там номер \
                     элемента, а не номер слова, и фразы по нему не бывает",
                    names.get(field).map(|s| s.as_str()).unwrap_or("")
                ));
            }
            phrases.push(svindb_core::segments::SpecPhrase { fid: field, terms, slop, boost });
        }
    }

    // Разреженный вектор: `"sparse":{"field":"s","query":{"кот":1.5,"пёс":0.2}}`.
    let mut words = words;
    if let Some(sp) = v.get("sparse") {
        // Вложенное дерево и разреженный вектор вместе не работают, и молчать об
        // этом нельзя: плоские слова при `nested` не читаются вовсе, и блок
        // `sparse` просто выпал бы из запроса. Выдача осталась бы
        // правдоподобной — она бы отвечала на другой вопрос.
        if nested.is_some() {
            return Err(
                "sparse и вложенное дерево нельзя задавать вместе: плоские слова при нём не читаются, и вектор молча выпал бы из запроса"
                    .into(),
            );
        }
        let f = field_ref(sp.get("field"), "sparse", names)?;
        if !kinds.get(f).map(|k| k.dot()).unwrap_or(false) {
            return Err(format!(
                "поле {:?} объявлено как {}, а не sparse_vector",
                names[f],
                kinds.get(f).map(|k| k.name()).unwrap_or("?")
            ));
        }
        let Some(q) = sp.get("query").and_then(|x| x.as_object()) else {
            return Err("sparse: не задан query как объект «измерение: вес»".into());
        };
        // Потолок веса запроса — тот, при котором сумма произведений ещё
        // представима. Проверять надо именно сумму, а не каждое слагаемое:
        // конечные веса дают бесконечный вес документа, а бесконечность в
        // ответе — это уже не JSON, и клиент падает на разборе, а не получает
        // отказ. Величина не с потолка: это ровно условие представимости.
        let mut ceiling = 0f64;
        for (dim, w) in q.iter() {
            let Some(w) = w.as_f64().filter(|x| x.is_finite()) else {
                return Err(format!("sparse: вес измерения {dim:?} не число"));
            };
            ceiling += w.abs() * svindb_core::sparse::WMAX;
            if !ceiling.is_finite() {
                return Err(
                    "sparse: веса запроса так велики, что вес документа перестаёт быть числом"
                        .into(),
                );
            }
            if w <= 0.0 {
                continue;
            }
            words.push(SpecWord {
                fid: f,
                terms: vec![svindb_core::segments::Term {
                    text: dim.to_lowercase(),
                    prefix: false,
                }],
                given: Some(w),
            });
        }
    }

    // Запрос из одного отбора законен: «всё за неделю», «всё дороже ста». Слов
    // в нём нет и веса тоже — порядок по номеру документа или по сортировке.
    // А вот запрос без слов и без отбора искать не по чему.
    if words.is_empty()
        && must.is_empty()
        && must_not.is_empty()
        && filter.is_empty()
        && nested.is_none()
        && phrases.is_empty()
    {
        return Err("запрос без единого слова и без отбора: искать не по чему".into());
    }
    // Идентификаторы: `"ids":true`. Поиск отдаёт номер документа, а номер —
    // величина внутренняя: слияние его меняет. Без перевода найденное нечем
    // связать с тем, что клиент хранит у себя.
    let with_ids = match v.get("ids") {
        // Идентификатор — часть всякого публичного hit. Внутренний `doc`
        // меняется после merge и не может быть ключом клиентского объекта.
        None => true,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(_) => return Err("\"ids\" — это true или false".into()),
    };
    // Подсветка: `"highlight":{"fields":["body"],"size":150}`.
    let mut highlight: Vec<usize> = Vec::new();
    let mut highlight_size = 150usize;
    if let Some(h) = v.get("highlight") {
        if let Some(a) = h.get("fields").and_then(|x| x.as_array()) {
            for f in a {
                let Some(name) = f.as_str() else {
                    return Err("в \"highlight.fields\" ожидались имена полей строками".into());
                };
                match names.iter().position(|n| !n.is_empty() && n == name) {
                    Some(i) => highlight.push(i),
                    None => return Err(format!("подсветка: поле {name:?} не объявлено")),
                }
            }
        }
        highlight_size =
            h.get("size").and_then(|x| x.as_u64()).unwrap_or(150).clamp(16, 4000) as usize;
    }

    // Какие поля вернуть содержимым. Имена, а не номера: номер поля — это наша
    // внутренняя нумерация, и просить клиента её знать значило бы связать его с
    // порядком объявления.
    let mut want_fields: Vec<usize> = Vec::new();
    if let Some(a) = v.get("fields").and_then(|x| x.as_array()) {
        for f in a {
            let Some(name) = f.as_str() else {
                return Err("в \"fields\" ожидались имена полей строками".into());
            };
            match names.iter().position(|n| !n.is_empty() && n == name) {
                Some(i) => want_fields.push(i),
                None => {
                    return Err(format!("поле {name:?} не объявлено: есть {}", names.join(", ")))
                }
            }
        }
    }
    let page_integer = |key: &str| -> Result<Option<usize>, String> {
        v.get(key)
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|number| usize::try_from(number).ok())
                    .ok_or_else(|| format!("{key} must be a non-negative integer"))
            })
            .transpose()
    };
    let from = page_integer("from")?.unwrap_or(0);
    let size = page_integer("size")?;
    // Верхушка набирается на **`from + size`**, а не на `size`: наружу идёт её
    // хвост, и собрать надо всё до него.
    let size = size.unwrap_or(k);
    if from.checked_add(size).is_none_or(|window| window > 10_000) {
        return Err(format!(
            "from + size = {} глубже потолка в 10 000. Глубокий обход делается \
             курсором search_after: его цена не зависит от номера страницы",
            from.saturating_add(size)
        ));
    }
    let k = (from + size).min(10_000);

    let track_total_hits = match v.get("track_total_hits") {
        None | Some(serde_json::Value::Bool(false)) => svindb_core::segments::TrackTotalHits::Omit,
        Some(serde_json::Value::Bool(true)) => svindb_core::segments::TrackTotalHits::Exact,
        Some(value) => {
            let limit = value
                .as_u64()
                .ok_or_else(|| "track_total_hits должно быть bool либо целым числом".to_string())?;
            if limit == 0 {
                svindb_core::segments::TrackTotalHits::Omit
            } else {
                svindb_core::segments::TrackTotalHits::UpTo(limit)
            }
        }
    };

    let mut sort = Vec::new();
    if let Some(a) = v.get("sort").and_then(|x| x.as_array()) {
        if a.len() > svindb_core::search::MAX_SORT {
            return Err(format!(
                "ключей сортировки {}, потолок {}: дальше сортировка перестаёт \
                 быть сортировкой",
                a.len(),
                svindb_core::search::MAX_SORT
            ));
        }
        for (i, x) in a.iter().enumerate() {
            let what = format!("sort[{i}]");
            only_keys(x, &what, &["field", "order"])?;
            let fid = field_ref(x.get("field"), &what, names)?;
            refuse_slot_misuse(fid, &what, names, slot_groups, groups, None)?;
            // Сортировать можно только по тому, у чего есть колонка значений и
            // чьи значения значат одно и то же во всех сегментах.
            //
            // У текстового поля колонки нет вовсе: все ключи получались
            // «значения нет», порядок выходил произвольным, и отказа при этом
            // не было ни одного. У ключевого колонка есть, но в ней номер слова
            // в словаре **этого** сегмента — порядок верен внутри сегмента и
            // неверен между ними, то есть неверен ровно тогда, когда индекс
            // перестал быть одним сегментом.
            let k = kinds.get(fid).copied().unwrap_or(Kind::Text);
            let name = names.get(fid).map(|s| s.as_str()).unwrap_or("");
            if k.keyword() {
                return Err(format!(
                    "{what}: поле {name:?} — ключевое слово, и в колонке у него номер слова в \
                     словаре сегмента; между сегментами такие номера значат разное, и порядок \
                     вышел бы неверным молча"
                ));
            }
            if !k.numeric() {
                return Err(format!(
                    "{what}: поле {name:?} объявлено как {} — колонки значений у него нет, \
                     сортировать нечем",
                    k.name()
                ));
            }
            sort.push(svindb_core::segments::Sort {
                fid,
                desc: x.get("order").and_then(|o| o.as_str()).unwrap_or("asc") == "desc",
            });
        }
    }
    // Курсор страницы: столько же значений, сколько ключей сортировки, и номер
    // последним. Пришедшие значения нормируются тем же правилом, что и при
    // сборе, — иначе курсор и сортировка говорили бы на разных языках.
    let after = match v.get("search_after").and_then(|x| x.as_array()) {
        None => None,
        Some(a) if a.len() == sort.len().max(1) + 1 => {
            let doc = a[a.len() - 1]
                .as_u64()
                .ok_or_else(|| "search_after: последним нужен номер документа".to_string())?;
            // Потолок сверяется и здесь: `as` за границей молча заворачивает —
            // курсор превращался в ноль, и клиент, ничего не заметив, начинал
            // листать с первой страницы заново. Негодный курсор — отказ, как и
            // номер поля вне объявленных.
            if doc > svindb_core::segments::MAX_DOCS {
                return Err(format!(
                    "search_after: номер документа {doc} больше потолка {}",
                    svindb_core::segments::MAX_DOCS
                ));
            }
            let mut keys = Vec::with_capacity(a.len() - 1);
            for (i, x) in a[..a.len() - 1].iter().enumerate() {
                keys.push(match sort.get(i) {
                    Some(s) => {
                        if x.is_null() {
                            svindb_core::search::MISSING_KEY
                        } else {
                            let raw =
                                if matches!(kinds.get(s.fid), Some(Kind::Float | Kind::Double)) {
                                    x.as_f64()
                                        .filter(|v| v.is_finite())
                                        .map(svindb_core::index::float_key)
                                } else {
                                    x.as_i64()
                                }
                                .ok_or_else(|| {
                                    format!(
                                    "search_after[{i}]: нужно число объявленного типа либо null"
                                )
                                })?;
                            svindb_core::search::norm_key(raw, s.desc)
                        }
                    }
                    None => {
                        let v = x
                            .as_f64()
                            .ok_or_else(|| format!("search_after[{i}]: нужен вес числом"))?;
                        v.to_bits() as i64 as svindb_core::search::SortKey
                    }
                });
            }
            Some((keys, doc))
        }
        Some(_) => {
            return Err(format!(
                "search_after: нужно {} значений — по одному на ключ сортировки \
                 и номер документа последним",
                sort.len().max(1) + 1
            ))
        }
    };

    let min_should = match tree
        .and_then(|q| q.get("minimum_should_match"))
        .or_else(|| v.get("minimum_should_match"))
    {
        None => 0,
        Some(value) => value.as_u64().ok_or("minimum_should_match: нужно целое число")? as usize,
    };
    let word_count = words.iter().filter(|w| w.given.is_none()).count();
    if min_should > 0 && !words.iter().all(|w| w.given.is_none()) {
        return Err(
            "minimum_should_match и sparse нельзя задавать вместе: порог считает слова, а измерения вектора словами не являются"
                .into(),
        );
    }
    if min_should > 0 && min_should > word_count && nested.is_none() {
        return Err(format!(
            "minimum_should_match = {min_should} при {} необязательных: совпасть \
             не сможет никто",
            words.len()
        ));
    }

    Ok(QuerySpec {
        fields: want_fields,
        phrases,
        highlight,
        highlight_size,
        with_ids,
        nested,
        min_should,
        words,
        must,
        must_not,
        sort,
        from,
        after,
        filter,
        k,
        track_total_hits,
        max_expansions,
        max_expansion_examined,
        translit: v.get("translit").and_then(|x| x.as_bool()).unwrap_or(false),
        // Предел проверок по тексту. Ноль в запросе значит «без предела» — так
        // же, как внутри движка; отсутствие поля значит умолчание.
        max_verify: v
            .get("max_verify")
            .and_then(|x| x.as_u64())
            .map(|v| v as usize)
            .unwrap_or(svindb_core::segments::DEFAULT_VERIFY),
        // Срок счёта агрегаций. Читается так же: ноль — без срока, отсутствие
        // поля — умолчание.
        max_agg_ms: v
            .get("max_agg_ms")
            .and_then(|x| x.as_u64())
            .unwrap_or(svindb_core::segments::DEFAULT_AGG_MS),
        guard: None,
        live: None,
        expansion: None,
    })
}

fn finite_boost(value: Option<&serde_json::Value>, what: &str) -> Result<f64, String> {
    let boost = match value {
        None => 1.0,
        Some(value) => value.as_f64().ok_or_else(|| format!("{what}: нужно число"))?,
    };
    if !boost.is_finite() || boost.abs() > 1_000_000.0 {
        return Err(format!("{what}: вес должен быть конечным и не больше 1000000 по модулю"));
    }
    Ok(boost)
}

/// Есть ли в дереве вложенные ветви — тогда плоский разбор недостаточен.
fn has_nesting(q: &serde_json::Value) -> bool {
    ["must", "should", "must_not"].iter().any(|arm| {
        q.get(*arm)
            .and_then(|x| x.as_array())
            // `slots` наравне с `bool`: условие «сошлось в одном элементе
            // массива» понимает только разбор дерева, и прежде оно было
            // достижимо лишь внутри `bool` — то есть документированная форма
            // запроса не работала вовсе, а отвечала «искать не по чему».
            .map(|a| a.iter().any(|c| c.get("bool").is_some() || c.get("slots").is_some()))
            .unwrap_or(false)
    })
}

/// Устойчивые номера полей, лежащих внутри массива.
///
/// Нужны разбору запроса: у такого поля позиция значит номер элемента, и всё,
/// что читает позиции как порядок слов, обязано на нём отказать.
/// Номер группы по устойчивому номеру поля. Ноль — поле корневое.
///
/// Не «лежит ли в массиве», а «в каком именно»: отказ обязан называть массив по
/// имени, иначе он говорит о беде, но не о том, как её исправить.
fn slot_groups_of(segs: &Segments) -> Vec<(usize, u32)> {
    let mut out: Vec<(usize, u32)> = Vec::new();
    for ix in segs.list.iter() {
        for f in ix.fields.iter() {
            if !out.iter().any(|(fid, _)| *fid == f.fid as usize) {
                out.push((f.fid as usize, f.group));
            }
        }
    }
    out
}

/// Группы индекса: имя пути — номер. Берётся по всем сегментам.
fn group_ids_of(segs: &Segments) -> Vec<(String, u32)> {
    let mut out: Vec<(String, u32)> = Vec::new();
    for ix in segs.list.iter() {
        for (g, path) in ix.group_paths.iter().enumerate() {
            if g == 0 || path.is_empty() || out.iter().any(|(_, x)| *x == g as u32) {
                continue;
            }
            // Путь читается из заголовка сегмента — оттуда, где он записан.
            // Прежде он выводился отрезанием хвоста у имени поля, а точка в
            // имени была запрещена, поэтому путь всегда получался пустым: любая
            // группа в запросе подменялась первой попавшейся, и подменялась
            // молча.
            out.push((path.clone(), g as u32));
        }
    }
    out
}

/// Отказ на ключ, которого разбор не знает.
///
/// ## Почему отказ, а не молчание
///
/// Прежде запрос читал только те ключи, что знал, и не смотрел на остальные.
/// Значит `{"filter":[{"field":"views","gte":1000}]}` — привычное написание для
/// пришедшего из Elasticsearch — не находило ни `from`, ни `to`, отбор
/// вырождался в «у поля есть значение», и ответ приходил двумястами. Опечатка в
/// имени ключа выглядела ровно как её отсутствие.
///
/// У приёма и у настроек такая защита есть и написана осознанно; у запроса её
/// просто не сделали. Третьей политики заводить незачем: отказ стоит одного
/// повтора, а неверный ответ — доверия к движку.
fn only_keys(v: &serde_json::Value, what: &str, allowed: &[&str]) -> Result<(), String> {
    let obj = v.as_object().ok_or_else(|| format!("{what}: нужен объект"))?;
    for key in obj.keys() {
        if !allowed.iter().any(|a| a == key) {
            return Err(format!(
                "{what}: ключ {key:?} неизвестен; ожидались: {}",
                allowed.join(", ")
            ));
        }
    }
    Ok(())
}

/// Ключи верхнего уровня родного запроса.
///
/// Четыре последних — не для клиента: `scoring` и `partial` кладёт координатор
/// в запрос к шарду, `_stats_only` и `_global_stats` — его же служебные. Забыть
/// их здесь значит сломать веер.
const QUERY_KEYS: &[&str] = &[
    "k",
    "size",
    "from",
    "words",
    "must",
    "must_not",
    "query",
    "filter",
    "phrases",
    "sparse",
    "vector",
    "aggs",
    "sort",
    "search_after",
    "highlight",
    "fields",
    "ids",
    "source",
    "source_nested",
    "routing",
    "pit",
    "translit",
    "max_expansions",
    "max_expansion_examined",
    "max_verify",
    "max_agg_ms",
    "timeout_ms",
    "minimum_should_match",
    "match_all",
    "track_total_hits",
    "scoring",
    "partial",
    "_stats_only",
    "_global_stats",
];

/// Отказ, когда поле внутри массива читают номером документа.
///
/// ## Почему отказ, а не догадка
///
/// Колонка группового поля адресуется **ординалом элемента**, а `Over::Docs`,
/// сортировка и отбор адресуют **номером документа**. Прочитанное так значение
/// принадлежит чужой ячейке, и ответ выходит правдоподобным: числа на месте,
/// порядок осмысленный, а данные не те. Проверено живьём — отбор по счётчику
/// реакций нашёл один документ из двух подходящих.
///
/// Вывести `over` из объявления поля было бы соблазнительно, но нельзя: это
/// молча поменяло бы смысл счёта наблюдений — документы против элементов, — а
/// два разных вопроса не вправе иметь одно написание. Elasticsearch по той же
/// причине требует писать `nested` явно.
fn refuse_slot_misuse(
    field: usize,
    what: &str,
    names: &[String],
    slot_groups: &[(usize, u32)],
    groups: &[(String, u32)],
    over: Option<u32>,
) -> Result<(), String> {
    let g = slot_groups.iter().find(|(f, _)| *f == field).map(|(_, g)| *g).unwrap_or(0);
    let name = names.get(field).map(|s| s.as_str()).unwrap_or("");
    let path_of = |g: u32| {
        groups
            .iter()
            .find(|(_, x)| *x == g)
            .map(|(p, _)| p.clone())
            .unwrap_or_else(|| format!("№{g}"))
    };
    match (g, over) {
        (0, None) => Ok(()),
        (0, Some(_)) => Err(format!(
            "{what}: поле {name:?} лежит в корне документа, а спрошено по элементам \
             массива — уберите over"
        )),
        (g, None) => Err(format!(
            "{what}: поле {name:?} лежит внутри массива {:?}: наблюдение там — элемент, а \
                 не документ. Добавьте \"over\":{:?}, а для условия «сошлось в одном \
                 элементе» — slots",
            path_of(g),
            path_of(g)
        )),
        (g, Some(o)) if o != g => Err(format!(
            "{what}: поле {name:?} лежит в массиве {:?}, а over называет {:?}",
            path_of(g),
            path_of(o)
        )),
        _ => Ok(()),
    }
}

/// Одно условие на элемент массива.
fn parse_slot_term(
    v: &serde_json::Value,
    what: &str,
    names: &[String],
    kinds: &[Kind],
) -> Result<svindb_core::segments::SpecSlotTerm, String> {
    use svindb_core::segments::{SpecRange, SpecSlotTerm};
    if let Some(r) = v.get("range") {
        only_keys(v, what, &["range"])?;
        only_keys(r, what, &["field", "from", "to"])?;
        let field = field_ref(r.get("field"), what, names)?;
        let floating = kinds.get(field).copied().unwrap_or(Kind::Text).floating();
        let edge = |name: &str, dflt: i64| -> Result<i64, String> {
            match r.get(name) {
                None => Ok(dflt),
                Some(x) => {
                    let n = x.as_f64().ok_or_else(|| format!("{what}.{name}: нужно число"))?;
                    if floating {
                        Ok(svindb_core::index::float_key(n))
                    } else {
                        x.as_i64()
                            .ok_or_else(|| format!("{what}.{name}: у целого поля целая граница"))
                    }
                }
            }
        };
        return Ok(SpecSlotTerm::Range(SpecRange {
            fid: field,
            from: edge("from", i64::MIN)?,
            to: edge("to", i64::MAX)?,
        }));
    }
    if let Some(e) = v.get("exists") {
        only_keys(v, what, &["exists"])?;
        if e.is_object() {
            only_keys(e, what, &["field"])?;
        }
        let field = field_ref(e.get("field").or(Some(e)), what, names)?;
        let kind = kinds.get(field).copied().unwrap_or(Kind::Text);
        return Ok(SpecSlotTerm::Term(SpecWord {
            fid: field,
            terms: vec![Term { text: svindb_core::indexer::presence_term(kind), prefix: false }],
            given: Some(0.0),
        }));
    }
    if v.get("term").is_some() {
        only_keys(v, what, &["term"])?;
    }
    let w = v.get("term").unwrap_or(v);
    only_keys(w, what, &["field", "terms", "value"])?;
    if w.get("terms").is_some() && w.get("value").is_some() {
        return Err(format!("{what}: задайте terms или value, не оба"));
    }
    let field = field_ref(w.get("field"), what, names)?;
    if w.get("terms").is_some_and(|terms| {
        !terms.as_array().is_some_and(|items| items.iter().all(serde_json::Value::is_string))
    }) {
        return Err(format!("{what}.terms: нужен массив строк"));
    }
    let terms = w
        .get("terms")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str())
                .map(|term| query_term(term, kinds.get(field).copied().unwrap_or(Kind::Text)))
                .collect::<Vec<Term>>()
        })
        .or_else(|| {
            w.get("value")
                .and_then(|x| x.as_str())
                .map(|term| vec![query_term(term, kinds.get(field).copied().unwrap_or(Kind::Text))])
        })
        .ok_or_else(|| format!("{what}: нужен terms или value"))?;
    if terms.is_empty() {
        return Err(format!("{what}: пустой список слов"));
    }
    Ok(SpecSlotTerm::Term(SpecWord { fid: field, terms, given: None }))
}

/// Условие «на одном элементе массива».
fn parse_slots(
    v: &serde_json::Value,
    what: &str,
    names: &[String],
    kinds: &[Kind],
    groups: &[(String, u32)],
) -> Result<svindb_core::segments::SpecSlots, String> {
    use svindb_core::segments::SpecSlots;
    only_keys(v, what, &["group", "all", "none", "min_count", "boost"])?;
    let path = v
        .get("group")
        .and_then(|x| x.as_str())
        .ok_or_else(|| format!("{what}: нужно group — путь массива"))?;
    let group = groups.iter().find(|(p, _)| p == path).map(|(_, g)| *g).ok_or_else(|| {
        format!(
            "{what}: группы {path:?} нет — объявлены {}",
            if groups.is_empty() {
                "ни одной".to_string()
            } else {
                groups.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>().join(", ")
            }
        )
    })?;
    let arm = |name: &str| -> Result<Vec<svindb_core::segments::SpecSlotTerm>, String> {
        let Some(raw) = v.get(name) else { return Ok(Vec::new()) };
        let raw = raw.as_array().ok_or_else(|| format!("{what}.{name}: нужен массив"))?;
        raw.iter()
            .enumerate()
            .map(|(i, x)| parse_slot_term(x, &format!("{what}.{name}[{i}]"), names, kinds))
            .collect()
    };
    let all = arm("all")?;
    if all.is_empty() {
        // Отрицание над пустым множеством означало бы «все элементы, кроме», а
        // «все элементы» требует знать длину массива в узле обхода. Отказ здесь
        // честнее выдуманного инварианта, который однажды разъедется молча.
        return Err(format!("{what}: нужен непустой all — из одних отрицаний условия не бывает"));
    }
    let min_count = match v.get("min_count") {
        None => 1,
        Some(value) => {
            value.as_u64().ok_or_else(|| format!("{what}.min_count: нужно целое число"))?.max(1)
        }
    };
    let min_count =
        u32::try_from(min_count).map_err(|_| format!("{what}.min_count больше {}", u32::MAX))?;
    Ok(SpecSlots {
        group,
        all,
        none: arm("none")?,
        min_count,
        boost: finite_boost(v.get("boost"), &format!("{what}.boost"))?,
    })
}

/// Агрегации запроса: `{"aggs":{"имя":{"вид":{...}}}}`.
///
/// Вид пишется так же, как в Elasticsearch, — `stats`, `histogram`, `range`,
/// `terms`, `cardinality`, — чтобы запрос переводился между движками без
/// перевода смысла. `"over"` называет путь массива: без него наблюдение это
/// документ, с ним — элемент.
fn parse_aggs(
    v: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
    groups: &[(String, u32)],
    slot_groups: &[(usize, u32)],
) -> Result<Vec<svindb_core::aggs::Agg>, String> {
    use svindb_core::aggs::{Agg, Kind as AggKind, Over};
    let Some(obj) = v.get("aggs") else { return Ok(Vec::new()) };
    let obj = obj.as_object().ok_or("aggs: нужен объект «имя → агрегация»")?;
    if obj.len() > 32 {
        return Err(format!("aggs: {} штук, потолок 32", obj.len()));
    }
    let mut out = Vec::with_capacity(obj.len());
    let mut range_budget = 0usize;
    for (name, body) in obj.iter() {
        const TYPES: &[&str] =
            &["stats", "count", "histogram", "date_histogram", "range", "terms", "cardinality"];
        only_keys(
            body,
            name,
            &[
                "over",
                "stats",
                "count",
                "histogram",
                "date_histogram",
                "range",
                "terms",
                "cardinality",
            ],
        )?;
        let types: Vec<_> = TYPES.iter().filter(|kind| body.get(**kind).is_some()).collect();
        if types.len() != 1 {
            return Err(format!("{name}: нужен ровно один вид агрегации"));
        }
        let agg_type = *types[0];
        let allowed: &[&str] = match agg_type {
            "stats" | "count" => &["field"],
            "histogram" => &["field", "interval", "offset"],
            "date_histogram" => &["field", "calendar_interval", "fixed_interval", "time_zone"],
            "range" => &["field", "ranges"],
            "terms" => &["field", "size"],
            "cardinality" => &["field", "mode"],
            _ => unreachable!(),
        };
        only_keys(&body[agg_type], &format!("{name}.{agg_type}"), allowed)?;
        if body.get("over").is_some_and(|over| !over.is_string()) {
            return Err(format!("{name}.over: нужна строка"));
        }
        let over = match body.get("over").and_then(|x| x.as_str()) {
            None => Over::Docs,
            Some(path) => {
                let g = groups
                    .iter()
                    .find(|(p, _)| p == path)
                    .map(|(_, g)| *g)
                    .ok_or_else(|| format!("{name}.over: группы {path:?} нет"))?;
                Over::Slots { group: g }
            }
        };
        let one = |k: &str| -> Option<&serde_json::Value> { body.get(k) };
        let field_of = |b: &serde_json::Value, what: &str| -> Result<usize, String> {
            field_ref(b.get("field"), what, names)
        };
        let kind = if let Some(b) = one("stats").or_else(|| one("count")) {
            let f = match b.get("field") {
                None => None,
                Some(_) => Some(field_of(b, &format!("{name}.stats"))?),
            };
            AggKind::Stats { field: f }
        } else if let Some(b) = one("date_histogram") {
            if b.get("calendar_interval").is_some() && b.get("fixed_interval").is_some() {
                return Err(format!("{name}.date_histogram: задайте один interval"));
            }
            if b.get("time_zone").is_some_and(|zone| !zone.is_string()) {
                return Err(format!("{name}.date_histogram.time_zone: нужна строка"));
            }
            if b.get("calendar_interval").is_some_and(|interval| !interval.is_string()) {
                return Err(format!("{name}.date_histogram.calendar_interval: нужна строка"));
            }
            let field = field_of(b, &format!("{name}.date_histogram"))?;
            // Часовой пояс — только постоянным смещением. Именованный требует
            // базы `tzdata`, которая живёт и меняется отдельно от программы, а
            // у ядра нет ни одной зависимости. Посчитать его «примерно» значило
            // бы сдвинуть границу суток и выдать неверный отчёт, выглядящий
            // верным.
            let tz_ms = match b.get("time_zone").and_then(|x| x.as_str()) {
                None => 0,
                Some(z) => match svindb_core::iso::parse_millis(&format!("1970-01-01T00:00:00{z}"))
                {
                    Some(ms) => -ms,
                    None => {
                        return Err(format!(
                            "{name}.date_histogram.time_zone: {z:?} — принимается только \
                             постоянное смещение вида +03:00; именованные пояса требуют базы \
                             часовых поясов, которой у движка нет"
                        ))
                    }
                },
            };
            let step = match b.get("calendar_interval").and_then(|x| x.as_str()) {
                Some(w) => {
                    let unit = svindb_core::aggs::Unit::parse(w).ok_or_else(|| {
                        format!(
                            "{name}.date_histogram.calendar_interval: {w:?} — бывает \
                             1d, 1w, 1M, 1q, 1y"
                        )
                    })?;
                    svindb_core::aggs::Step::Calendar { unit, tz_ms }
                }
                None => {
                    let ms = b
                        .get("fixed_interval")
                        .and_then(|x| x.as_i64())
                        .filter(|x| *x > 0)
                        .ok_or_else(|| {
                            format!(
                                "{name}.date_histogram: нужен calendar_interval (1d, 1w, 1M, \
                                 1q, 1y) или fixed_interval в миллисекундах"
                            )
                        })?;
                    svindb_core::aggs::Step::Int { interval: ms, offset: -tz_ms }
                }
            };
            AggKind::Histogram { field, interval: 1, offset: 0, step }
        } else if let Some(b) = one("histogram") {
            let field = field_of(b, &format!("{name}.histogram"))?;
            let floating = kinds.get(field).copied().unwrap_or(Kind::Text).floating();
            // Шаг читается **по типу поля**, а не по виду записи в JSON: то же
            // правило, что у отбора. Прежде он читался целым всегда, и по
            // дробному полю сетку нельзя было даже задать — шаг «одна десятая»
            // пришлось бы выражать в битах, а биты не равномерны.
            let interval = b
                .get("interval")
                .and_then(|x| x.as_f64())
                .filter(|x| *x > 0.0)
                .ok_or_else(|| format!("{name}.histogram: нужен interval больше нуля"))?;
            let offset = match b.get("offset") {
                None => 0.0,
                Some(value) => {
                    value.as_f64().ok_or_else(|| format!("{name}.histogram.offset: нужно число"))?
                }
            };
            if !floating && (interval.fract() != 0.0 || offset.fract() != 0.0) {
                return Err(format!("{name}.histogram: у целого поля interval и offset целые"));
            }
            let step = if floating {
                svindb_core::aggs::Step::Float { interval, offset }
            } else {
                svindb_core::aggs::Step::Int { interval: interval as i64, offset: offset as i64 }
            };
            AggKind::Histogram { field, interval: interval as i64, offset: offset as i64, step }
        } else if let Some(b) = one("range") {
            let raw = b
                .get("ranges")
                .and_then(|x| x.as_array())
                .ok_or_else(|| format!("{name}.range: нужен массив ranges"))?;
            range_budget = range_budget.saturating_add(raw.len());
            if raw.len() > svindb_core::aggs::MAX_RANGES
                || range_budget > svindb_core::aggs::MAX_RANGES
            {
                return Err(format!(
                    "{name}.range: суммарно {range_budget} диапазонов, потолок {} на запрос",
                    svindb_core::aggs::MAX_RANGES
                ));
            }
            let field = field_of(b, &format!("{name}.range"))?;
            let floating = kinds.get(field).copied().unwrap_or(Kind::Text).floating();
            // Границы приводятся по типу поля — то же правило, что у отбора.
            // Прежде они читались целыми, и `{"from":1.0}` давало `None`, то
            // есть диапазон молча накрывал всё.
            let edge = |r: &serde_json::Value, k: &str, dflt: i64| -> Result<i64, String> {
                match r.get(k) {
                    None | Some(serde_json::Value::Null) => Ok(dflt),
                    Some(x) => {
                        let n =
                            x.as_f64().ok_or_else(|| format!("{name}.range.{k}: нужно число"))?;
                        if floating {
                            Ok(svindb_core::index::float_key(n))
                        } else {
                            x.as_i64().ok_or_else(|| {
                                format!("{name}.range.{k}: у целого поля целая граница")
                            })
                        }
                    }
                }
            };
            let mut edges: Vec<(i64, Option<i64>)> = Vec::with_capacity(raw.len());
            for r in raw.iter() {
                only_keys(r, &format!("{name}.range.ranges"), &["from", "to"])?;
                let to = match r.get("to") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(_) => Some(edge(r, "to", i64::MAX)?),
                };
                edges.push((edge(r, "from", i64::MIN)?, to));
            }
            AggKind::Range { field, edges }
        } else if let Some(b) = one("terms") {
            AggKind::Terms {
                field: field_of(b, &format!("{name}.terms"))?,
                size: match b.get("size") {
                    None => 10,
                    Some(value) => value
                        .as_u64()
                        .ok_or_else(|| format!("{name}.terms.size: нужно целое число"))?
                        .clamp(1, 10_000) as usize,
                },
            }
        } else if let Some(b) = one("cardinality") {
            let field = field_of(b, &format!("{name}.cardinality"))?;
            let mode = match b.get("mode") {
                None => "hll",
                Some(value) => value
                    .as_str()
                    .ok_or_else(|| format!("{name}.cardinality.mode: нужна строка"))?,
            };
            match mode {
                "hll" => AggKind::Cardinality { field },
                "exact" => AggKind::CardinalityExact { field },
                mode => {
                    return Err(format!(
                        "{name}.cardinality.mode: {mode:?}; допустимы exact или hll"
                    ))
                }
            }
        } else {
            return Err(format!(
                "{name}: неизвестный вид агрегации — есть stats, histogram, range, terms, \
                 cardinality"
            ));
        };
        // Поле обязано иметь колонку: без неё считать нечего, и молчаливый ноль
        // был бы хуже отказа.
        let field = match kind {
            AggKind::Stats { field } => field,
            AggKind::Histogram { field, .. }
            | AggKind::Range { field, .. }
            | AggKind::Terms { field, .. }
            | AggKind::Cardinality { field }
            | AggKind::CardinalityExact { field } => Some(field),
        };
        if let Some(f) = field {
            let over_group = match over {
                Over::Slots { group } => Some(group),
                _ => None,
            };
            refuse_slot_misuse(f, name, names, slot_groups, groups, over_group)?;
            let k = kinds.get(f).copied().unwrap_or(Kind::Text);
            // По ключевому полю считать можно только то, что считает
            // **различность**: в его колонке лежат номера слов в словаре
            // ЭТОГО сегмента, и у соседнего тот же номер значит другое слово.
            // Наименьшее, наибольшее, сумма и корзины по таким номерам —
            // правдоподобная бессмыслица, и считались они молча.
            if k.keyword()
                && !matches!(
                    kind,
                    AggKind::Terms { .. }
                        | AggKind::Cardinality { .. }
                        | AggKind::CardinalityExact { .. }
                )
            {
                return Err(format!(
                    "{name}: поле {:?} — ключевое слово, и в колонке у него номер слова в \
                     словаре сегмента, а не число. Считать по нему можно terms и cardinality; \
                     для чисел объявите поле числовым",
                    names.get(f).map(|s| s.as_str()).unwrap_or("")
                ));
            }
            if !k.numeric() && !k.keyword() {
                return Err(format!(
                    "{name}: поле {:?} объявлено как {} — считать по нему нечего, \
                     колонки значений у текста нет",
                    names.get(f).map(|s| s.as_str()).unwrap_or(""),
                    k.name()
                ));
            }
        }
        let space = match field.map(|f| kinds.get(f).copied().unwrap_or(Kind::Text)) {
            Some(k) if k.floating() => svindb_core::aggs::Space::Float,
            Some(k) if k.keyword() => svindb_core::aggs::Space::Keyword,
            _ => svindb_core::aggs::Space::Int,
        };
        out.push(Agg { name: name.clone(), kind, over, space });
    }
    Ok(out)
}

/// Итог агрегаций в JSON. Ключи те же, что у Elasticsearch.
fn aggs_json(res: &[svindb_core::aggs::Result_], names: &[String]) -> String {
    use svindb_core::aggs::{Space, Value};
    let mut parts: Vec<String> = Vec::with_capacity(res.len());
    for r in res.iter() {
        // Значение покидает сегмент в том пространстве, на котором говорит
        // клиент. У дробного поля колонка хранит **ключ** — монотонное
        // преобразование бит, — и наружу он идёт числом.
        let number = |v: i64| -> String {
            if r.space == Space::Float {
                let x = svindb_core::index::float_of(v);
                // Конечность проверена на приёме, но печать JSON обязана
                // оставаться JSON при любом содержимом колонки.
                if x.is_finite() {
                    return format!("{x}");
                }
            }
            v.to_string()
        };
        let body = match &r.value {
            Value::Stats { count, min, max, sum, fsum } => {
                let total = fsum.unwrap_or(*sum as f64);
                let avg = if *count > 0 { total / *count as f64 } else { 0.0 };
                // Сумма дробного поля печатается дробной, целого — целой:
                // приводить целую сумму к `f64` значило бы терять точность на
                // числах больше двух в пятьдесят третьей.
                let sum_text = match fsum {
                    Some(f) => format!("{f}"),
                    None => sum.to_string(),
                };
                format!(
                    "{{\"count\":{count},\"min\":{},\"max\":{},\"sum\":{sum_text},\"avg\":{avg}}}",
                    min.map(number).unwrap_or_else(|| "null".into()),
                    max.map(number).unwrap_or_else(|| "null".into())
                )
            }
            Value::Buckets(b) => {
                let items: Vec<String> = b
                    .iter()
                    .map(|(k, n)| format!("{{\"key\":{},\"doc_count\":{n}}}", number(*k)))
                    .collect();
                format!("{{\"buckets\":[{}]}}", items.join(","))
            }
            Value::Terms { top, error, other } => {
                let items: Vec<String> = top
                    .iter()
                    .map(|(k, n)| {
                        // Ключ приходит уже разрешённым: ядро перевело номер
                        // словаря в строку на границе своего сегмента
                        // (`aggs::Sink::seal`).
                        //
                        // Прежде разрешал этот код, и **через нулевой сегмент**:
                        // брал `segs.list.first()` и спрашивал у его словаря.
                        // На одном сегменте это работало, на нескольких —
                        // подписывало чужим именем, потому что один и тот же
                        // номер означает у сегментов разные слова.
                        let key = match k {
                            svindb_core::aggs::TermKey::Num(v) => v.to_string(),
                            svindb_core::aggs::TermKey::Text(s) => json_string(s),
                        };
                        format!("{{\"key\":{key},\"doc_count\":{n}}}")
                    })
                    .collect();
                format!(
                    "{{\"doc_count_error_upper_bound\":{error},\"sum_other_doc_count\":{other},\
                     \"buckets\":[{}]}}",
                    items.join(",")
                )
            }
            // Переполнение до сборки ответа не доходит: запрос уже отвергнут.
            Value::TooMany { .. } => continue,
            Value::Cardinality { value } => {
                // Приближённость, о которой не сказано, — это неверный ответ.
                format!(
                    "{{\"value\":{value},\"mode\":\"hll\",\"approximate\":true,\"error\":0.0081}}"
                )
            }
            Value::ExactCardinality { value } => {
                format!("{{\"value\":{value},\"mode\":\"exact\",\"approximate\":false}}")
            }
        };
        parts.push(format!("{}:{}", json_string(&r.name), body));
    }
    let _ = names;
    format!("{{{}}}", parts.join(","))
}

fn parse_bool(
    q: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
    groups: &[(String, u32)],
) -> Result<svindb_core::segments::SpecBool, String> {
    use svindb_core::segments::{SpecBool, SpecClause};
    only_keys(q, "bool", &["must", "should", "must_not", "minimum_should_match"])?;
    let arm = |name: &str| -> Result<Vec<SpecClause>, String> {
        let Some(raw) = q.get(name) else { return Ok(Vec::new()) };
        let raw = raw.as_array().ok_or_else(|| format!("{name}: нужен массив"))?;
        let mut out = Vec::with_capacity(raw.len());
        for (i, c) in raw.iter().enumerate() {
            if let Some(inner) = c.get("bool") {
                only_keys(c, &format!("{name}[{i}]"), &["bool"])?;
                out.push(SpecClause::Bool(parse_bool(inner, names, kinds, groups)?));
                continue;
            }
            if let Some(s) = c.get("slots") {
                only_keys(c, &format!("{name}[{i}]"), &["slots"])?;
                out.push(SpecClause::Slots(parse_slots(
                    s,
                    &format!("{name}[{i}].slots"),
                    names,
                    kinds,
                    groups,
                )?));
                continue;
            }
            if c.get("term").is_some() {
                only_keys(c, &format!("{name}[{i}]"), &["term"])?;
            }
            let w = c.get("term").unwrap_or(c);
            only_keys(w, &format!("{name}[{i}]"), &["field", "terms", "boost", "term", "exists"])?;
            let field = field_ref(w.get("field"), &format!("{name}[{i}]"), names)?;
            let terms = w
                .get("terms")
                .and_then(|t| t.as_array())
                .ok_or_else(|| format!("{name}[{i}]: нужен массив terms"))?;
            if terms.iter().any(|term| !term.is_string()) {
                return Err("terms: нужен массив строк".into());
            }
            let kind = kinds.get(field).copied().unwrap_or(Kind::Text);
            let terms: Vec<Term> =
                if w.get("exists").and_then(serde_json::Value::as_bool) == Some(true) {
                    vec![Term { text: svindb_core::indexer::presence_term(kind), prefix: false }]
                } else {
                    terms
                        .iter()
                        .filter_map(|t| t.as_str())
                        // Пустая строка — законное и индексируемое keyword-
                        // значение. Выбрасывать её означало склеить empty с
                        // missing ровно на стороне запроса.
                        .filter(|t| !t.is_empty() || kind.keyword())
                        .map(|term| query_term(term, kind))
                        .collect()
                };
            if !terms.is_empty() {
                out.push(SpecClause::Term(SpecWord {
                    fid: field,
                    terms,
                    given: w.get("boost").and_then(serde_json::Value::as_f64),
                }));
            }
        }
        Ok(out)
    };
    let should = arm("should")?;
    let min_should = match q.get("minimum_should_match") {
        None => 0,
        Some(value) => {
            value.as_u64().ok_or("bool.minimum_should_match: нужно целое число")? as usize
        }
    };
    if min_should > should.len() {
        return Err(format!(
            "minimum_should_match = {min_should} при {} необязательных: совпасть \
             не сможет никто",
            should.len()
        ));
    }
    Ok(SpecBool { must: arm("must")?, should, must_not: arm("must_not")?, min_should })
}

/// Разбирает ветвь дерева или плоский массив слов.
fn parse_clauses(
    raw: &serde_json::Value,
    what: &str,
    names: &[String],
    kinds: &[Kind],
) -> Result<Vec<SpecWord>, String> {
    let raw = raw.as_array().ok_or_else(|| format!("{what}: нужен массив"))?;
    let mut out = Vec::with_capacity(raw.len());
    for (i, w) in raw.iter().enumerate() {
        // Лист дерева пишется как `{"term": {...}}`, плоская форма — сразу
        // объектом. Разница только в обёртке.
        if w.get("term").is_some() {
            only_keys(w, &format!("{what}[{i}]"), &["term"])?;
        }
        let w = w.get("term").unwrap_or(w);
        only_keys(w, &format!("{what}[{i}]"), &["field", "terms", "boost", "term", "exists"])?;
        let field = field_ref(w.get("field"), &format!("{what}[{i}]"), names)?;
        let terms = w
            .get("terms")
            .and_then(|t| t.as_array())
            .ok_or_else(|| format!("{what}[{i}]: нужен массив terms"))?;
        if terms.iter().any(|term| !term.is_string()) {
            return Err("terms: нужен массив строк".into());
        }
        let kind = kinds.get(field).copied().unwrap_or(Kind::Text);
        let terms: Vec<Term> = if w.get("exists").and_then(serde_json::Value::as_bool) == Some(true)
        {
            vec![Term { text: svindb_core::indexer::presence_term(kind), prefix: false }]
        } else {
            terms
                .iter()
                .filter_map(|t| t.as_str())
                .filter(|t| !t.is_empty() || kind.keyword())
                .map(|term| query_term(term, kind))
                .collect()
        };
        if !terms.is_empty() {
            out.push(SpecWord {
                fid: field,
                terms,
                given: w.get("boost").and_then(serde_json::Value::as_f64),
            });
        }
    }
    Ok(out)
}

/// Нормализация запроса обязана совпадать с нормализацией приёма.
fn query_term(raw: &str, kind: Kind) -> Term {
    let (text, prefix) = match raw.strip_suffix('*') {
        Some(head) => (head, true),
        None => (raw, false),
    };
    let text = if kind.numeric() {
        kind.parse_value(text).map_or_else(|| text.to_string(), |value| value.to_string())
    } else if kind.keyword() {
        svindb_core::indexer::keyword_term(text, kind.folds_case())
    } else if kind == Kind::Text {
        text.to_lowercase()
    } else {
        text.to_string()
    };
    Term { text, prefix }
}

/// Применяет числовые правила compiled mapping к запросу тем же округлением,
/// что приём. Обход общий для плоских, bool и slots-ветвей.
fn normalize_mapping_query(
    value: &mut serde_json::Value,
    rules: &mapping::Rules,
    names: &[String],
    kinds: &[Kind],
) {
    match value {
        serde_json::Value::Object(object) => {
            let field = object.get("field").and_then(serde_json::Value::as_str).map(str::to_string);
            if let Some(field) = field {
                let kind = names
                    .iter()
                    .position(|name| name == &field)
                    .and_then(|field| kinds.get(field))
                    .copied()
                    .unwrap_or(Kind::Text);
                if let Some(factor) = rules.get(&field).and_then(|rule| rule.scaling_factor) {
                    let scale = |value: &serde_json::Value| -> Option<String> {
                        let raw =
                            value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string());
                        let number = raw.parse::<f64>().ok().filter(|number| number.is_finite())?;
                        let scaled = (number * factor).round() / factor;
                        let text = scaled.to_string();
                        kind.parse_value(&text).map(|_| text)
                    };
                    if let Some(terms) = object.get_mut("terms").and_then(|v| v.as_array_mut()) {
                        for term in terms {
                            if let Some(scaled) = scale(term) {
                                *term = serde_json::Value::String(scaled);
                            }
                        }
                    }
                    for edge in ["from", "to"] {
                        if let Some(value) = object.get_mut(edge) {
                            if let Some(scaled) = scale(value) {
                                if let Ok(number) =
                                    serde_json::from_str::<serde_json::Value>(&scaled)
                                {
                                    *value = number;
                                }
                            }
                        }
                    }
                }
            }
            for child in object.values_mut() {
                normalize_mapping_query(child, rules, names, kinds);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                normalize_mapping_query(item, rules, names, kinds);
            }
        }
        _ => {}
    }
}

/// Проверяет номер поля на границе разбора.
///
/// Номер приходит от клиента и дальше уходит в `fields[номер]` и в `u8` для
/// словаря. Без проверки `256` превращалось в поле 0 при поиске по словарю, но
/// оставалось 256 при обращении к нормам — и роняло **весь процесс**: сборка
/// идёт с `panic = "abort"`, так что один запрос убивал узел вместе со всеми
/// соединениями. Отказ обязан быть ответом, а не падением.
fn field_at(n: u64, what: &str, names: &[String]) -> Result<usize, String> {
    if (n as usize) < names.len() {
        return Ok(n as usize);
    }
    Err(format!("{what}: поля {n} нет — объявлено {} ({})", names.len(), names.join(", ")))
}

/// Поле запроса: номер **или имя**.
///
/// ## Почему имя, а не только номер
///
/// Хуже того, позиция перестаёт быть устойчивой сама по себе, как только у
/// объявленного поля появляются **внутренние** колонки: диапазону нужны две,
/// точке две, адресу IPv6 две. Тогда номер, видимый снаружи, и номер колонки
/// внутри — разные числа, и совпадали они лишь пока типы были простыми.
fn field_ref(v: Option<&serde_json::Value>, what: &str, names: &[String]) -> Result<usize, String> {
    match v {
        Some(serde_json::Value::String(name)) => names
            .iter()
            .position(|n| !n.is_empty() && n == name)
            .ok_or_else(|| format!("{what}: поля {name:?} нет — объявлены {}", names.join(", "))),
        Some(x) => match x.as_u64() {
            Some(n) => field_at(n, what, names),
            None => Err(format!("{what}: field — это номер или имя поля")),
        },
        None => Err(format!("{what}: нужно field — номер или имя поля")),
    }
}

/// Имена объявленных полей по порядку.
///
/// Схема на приёме у сервера сейчас одна — умолчание; когда она станет
/// объявляемой, менять придётся только здесь.
/// Имена полей — **из сегмента**, а не из умолчаний движка.
///
/// Схема лежит в `head.tsv` каждого сегмента и попадает в него при сборке:
/// `svindb index … passage` даёт сегмент с единственным полем `passage`. Сервер
/// же спрашивал `default_fields()` — зашитые «title, body», — и на таком
/// сегменте отвечал «поле passage не объявлено: есть title, body», то есть
/// перечислял поля, которых в индексе нет, и не видел того, которое есть.
///
/// Пустой список сегментов — единственный случай, когда спросить не у кого:
/// тогда остаются умолчания, и это честно, потому что искать всё равно не по
/// чему.
#[cfg(test)]
/// Схема, с которой собраны индексы в проверках этого файла.
fn two() -> Vec<String> {
    vec!["title".to_string(), "body".to_string()]
}

/// Объявленная схема по устойчивому номеру поля.
///
/// ## Зачем она отдельно от сегментов
///
/// Схема живёт в реестре каталога, а сегменты — это то, что из неё **успело
/// лечь**. Пока не опубликован ни один сегмент, спросить у них нечего, и
/// разбор запроса брал умолчание `title, body`: фильтр по объявленному полю
/// получал `400 «поля нет»` на индексе, где поле объявлено и уже принято.
/// Ровно то состояние, в котором находится узел сразу после подъёма нового
/// индекса и всё время, пока идёт первая пачка.
///
/// ## Почему снимается один раз
///
/// Схема объявляется на подъёме и до перезапуска не меняется — на том же
/// основании, на котором [`Writer::plan`] строит разбор документа заново на
/// каждую пачку, а не держит копию. Читать же реестр с диска на каждый поиск
/// значило бы положить разбор файла в самый горячий путь узла.
#[derive(Default)]
struct Declared {
    /// Имя и тип по устойчивому номеру. `None` — номер, которого в живой схеме
    /// нет: похороненное поле или дырка в нумерации.
    by_fid: Vec<Option<(String, Kind)>>,
    /// Отдельная stored-копия пользовательского поля по устойчивому номеру.
    /// Нужна для раннего отказа highlight, когда `_source` отключён.
    stored: Vec<bool>,
    /// Внутренние поля не попадают в `by_fid`: клиент не может сослаться на
    /// них как на обычные, но гидратации нужны их устойчивые номера.
    source_fid: Option<usize>,
    projection_fid: Option<usize>,
    mapping: mapping::Rules,
    /// Persisted schema + mapping fingerprint для O(1) проверки alias search.
    signature: [u8; 32],
}

impl Declared {
    fn of(dir: &str) -> Result<Declared, String> {
        let signature = search_schema_signature(dir)?;
        let Some(reg) = svindb_core::schema::Registry::load(dir).ok().flatten() else {
            // Реестра нет — индекс ещё не объявлен вовсе. Не отказ: узел
            // поднимается и на пустом каталоге, а схема появится с первой
            // пачкой, то есть уже после перезапуска.
            return Ok(Declared { signature, ..Declared::default() });
        };
        let mut by_fid: Vec<Option<(String, Kind)>> = Vec::new();
        let mut stored = Vec::new();
        let mut source_fid = None;
        let mut projection_fid = None;
        for f in reg.live() {
            let at = f.id as usize;
            if by_fid.len() <= at {
                by_fid.resize(at + 1, None);
            }
            if stored.len() <= at {
                stored.resize(at + 1, false);
            }
            stored[at] = f.store;
            match f.path.as_str() {
                nested::SOURCE_FIELD => source_fid = Some(at),
                nested::PROJECTION_FIELD => projection_fid = Some(at),
                _ => by_fid[at] = Some((f.path.clone(), f.kind)),
            }
        }
        let mapping = mapping::Rules::load(dir)?;
        Ok(Declared { by_fid, stored, source_fid, projection_fid, mapping, signature })
    }

    fn field_stored(&self, field: usize) -> bool {
        self.stored.get(field).copied().unwrap_or(false)
    }
}

/// Имена полей **по устойчивому номеру**, а не по позиции в первом сегменте.
///
/// ## Почему объявленное, а поверх него сегментное
///
/// Объявленное отвечает за то, что у индекса вообще есть, — включая поля, из
/// которых пока не лёг ни один документ. Сегменты добавляют то, чего в живой
/// схеме уже нет: похороненное поле остаётся в старых сегментах и обязано
/// находиться по имени, пока они живы. Пустое имя после обоих проходов — номер,
/// которого в индексе нет ни в каком виде.
fn field_names_of(segs: &Segments, declared: &Declared) -> Vec<String> {
    let mut out: Vec<String> = declared
        .by_fid
        .iter()
        .map(|f| f.as_ref().map_or_else(String::new, |(name, _)| name.clone()))
        .collect();
    for ix in segs.list.iter() {
        for f in ix.fields.iter() {
            if internal_field(&f.name) {
                continue;
            }
            let at = f.fid as usize;
            if out.len() <= at {
                out.resize(at + 1, String::new());
            }
            if out[at].is_empty() {
                out[at] = f.name.clone();
            }
        }
    }
    if out.is_empty() {
        // Спросить не у кого — ни схемы, ни сегментов. Умолчания честны: искать
        // всё равно не по чему.
        return default_fields().0;
    }
    out
}

/// Типы полей по устойчивому номеру. Нужны разбору запроса: у дробного поля
/// границы отбора приводятся иначе, чем у целого, и знать это можно только по
/// схеме.
fn field_kinds_of(segs: &Segments, declared: &Declared) -> Vec<Kind> {
    let mut out: Vec<Option<Kind>> =
        declared.by_fid.iter().map(|f| f.as_ref().map(|&(_, kind)| kind)).collect();
    for ix in segs.list.iter() {
        for f in ix.fields.iter() {
            let at = f.fid as usize;
            if out.len() <= at {
                out.resize(at + 1, None);
            }
            if out[at].is_none() {
                out[at] = Some(f.kind);
            }
        }
    }
    if out.is_empty() {
        return default_fields().2;
    }
    out.into_iter().map(|k| k.unwrap_or(Kind::Text)).collect()
}

/// Ищется ли по полю, по устойчивому номеру. Поле, не встреченное ни в одном
/// сегменте, считается ищущимся: отказывать за то, чего ещё нет, не за что.
///
/// Достаточно **одного** сегмента, где поле объявлено `noindex`: искать по нему
/// всё равно нельзя, а разнобой между сегментами ловит `check`.
fn field_searched_of(segs: &Segments) -> Vec<bool> {
    let mut out: Vec<bool> = Vec::new();
    for ix in segs.list.iter() {
        for f in ix.fields.iter() {
            let at = f.fid as usize;
            if out.len() <= at {
                out.resize(at + 1, true);
            }
            out[at] &= f.searched;
        }
    }
    out
}

/// Отказ на запрос по полю, объявленному `noindex`.
///
/// ## Почему отказ, а не пустая выдача
///
/// У такого поля нет ни списка в словаре, ни колонки, так что поиск и без
/// проверки вернёт ноль попаданий. Но ноль попаданий — это ответ «в данных
/// такого нет», и получивший его пойдёт искать ошибку в данных, а не в схеме.
/// Разница обходится в дни.
///
/// ## Почему обходом тела, а не в месте разбора
///
/// `field` встречается в слове, фразе, отборе, сортировке, агрегации, векторе и
/// в каждой ветви булева дерева — тринадцать мест, и каждое новое забыло бы
/// проверку по-своему. Обход же берёт **всякое** упоминание поля в запросе, чем
/// бы оно ни было: ключ `field` в запросе всегда значит «искать по этому полю».
fn refuse_unsearchable(
    v: &serde_json::Value,
    names: &[String],
    searched: &[bool],
) -> Result<(), String> {
    if searched.iter().all(|x| *x) {
        return Ok(());
    }
    match v {
        serde_json::Value::Object(m) => {
            if let Some(f) = m.get("field") {
                // Негодную ссылку на поле здесь не разбираем: об этом скажет
                // разбор запроса, и сказать дважды разными словами хуже, чем
                // один раз своими.
                if let Ok(at) = field_ref(Some(f), "поле", names) {
                    if !searched.get(at).copied().unwrap_or(true) {
                        return Err(format!(
                            "поле {:?} объявлено noindex: содержимое хранится и возвращается, \
                             но искать, отбирать и сортировать по нему нельзя",
                            names.get(at).map(String::as_str).unwrap_or("")
                        ));
                    }
                }
            }
            for x in m.values() {
                refuse_unsearchable(x, names, searched)?;
            }
            Ok(())
        }
        serde_json::Value::Array(a) => {
            for x in a.iter() {
                refuse_unsearchable(x, names, searched)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Документы из тела `_bulk`: `{"documents":[{"id":"..","title":"..","body":".."}]}`.
/// `None` — тело не похоже на запрос приёма.
fn parse_docs(
    body: &[u8],
    plan: &nested::Plan,
) -> Result<(Vec<nested::Doc>, intake::Report), String> {
    if body.is_empty() {
        return Ok((Vec::new(), intake::Report::default()));
    }
    let v = serde_json::from_slice::<serde_json::Value>(body)
        .map_err(|_| "тело не разобралось".to_string())?;
    let list = v
        .get("documents")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "тело не разобралось".to_string())?;
    let mut out = Vec::with_capacity(list.len());
    let mut report = intake::Report::default();
    for (at, d) in list.iter().enumerate() {
        let mut doc = plan.take(d, at, &mut report)?;
        if !doc.has_id {
            doc.id = new_document_id()?;
            doc.has_id = true;
        }
        no_empty_ids(std::slice::from_ref(&doc.id))?;
        out.push(doc);
    }
    Ok((out, report))
}

/// Пустой идентификатор среди адресуемых — отказ, а не «удалить всё пустое».
///
/// Документ без `id` приём берёт: бывают потоки, которые только дописывают и
/// ничего не удаляют. Но пустой идентификатор — общий у **всех** таких
/// документов, и `_delete {"ids":[""]}` гасил их разом. Проверено: четыре
/// документа, два из которых слали с `id`, одним запросом ушли все четыре.
///
/// Адресовать неадресуемое нельзя, и попытка — это отказ.
fn no_empty_ids(ids: &[String]) -> Result<(), String> {
    if ids.iter().any(|s| s.is_empty()) {
        return Err("пустой идентификатор: он общий у всех документов без id, \
                    и адресовать им нельзя — такой документ не удалить и не заменить"
            .to_string());
    }
    Ok(())
}

/// Идентификаторы из тела `_delete`: `{"ids":["a","b"]}`. `None` — не тело.
fn parse_ids(body: &[u8]) -> Option<Vec<String>> {
    if body.is_empty() {
        return Some(Vec::new());
    }
    let v = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    let list = v.get("ids")?.as_array()?;
    // Идентификатор берётся тем же переводом, что и на приёме. Прежде тут
    // стоял отбор по «это строка», и числовой `id` молча выпадал: `_bulk`
    // число принимал и переводил в текст, а `_delete {"ids":[101]}` отвечал
    // «удалено ноль» — без ошибки и без объяснения. Два соседних разбора
    // читали одно и то же по-разному.
    let mut out = Vec::with_capacity(list.len());
    for x in list.iter() {
        // Routed mget/delete может передать `{id, peer_id}`: координатору
        // нужен peer_id для адреса, локальному shard — только сам id.
        let value = x.as_object().and_then(|object| object.get("id")).unwrap_or(x);
        let taken = intake::take_cell(Some(value), Kind::Keyword);
        if taken.is_loss() {
            return None;
        }
        out.push(taken.into_text());
    }
    Some(out)
}

fn parse_mutation_deletes(body: &[u8]) -> Result<Vec<String>, String> {
    let root: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| format!("deletes не разобрались как JSON: {error}"))?;
    let Some(raw) = root.get("deletes") else { return Ok(Vec::new()) };
    let list = raw.as_array().ok_or("deletes обязан быть массивом")?;
    let mut out = Vec::with_capacity(list.len());
    for (at, value) in list.iter().enumerate() {
        let taken = intake::take_cell(Some(value), Kind::Keyword);
        if taken.is_loss() {
            return Err(format!("deletes[{at}] обязан быть строкой, числом или bool"));
        }
        out.push(taken.into_text());
    }
    Ok(out)
}

/// Условия атомарного `/_put`.
///
/// Формат намеренно отдельный от документов: служебный ключ внутри документа
/// столкнулся бы с пользовательским полем и попал бы в отчёт о потерях схемы.
/// `null` означает «живой версии нет»; скаляр сравнивается с тем текстом,
/// который хранит объявленное поле.
fn parse_put_conditions(body: &[u8]) -> Result<Vec<PutCondition>, String> {
    let root: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| format!("условия не разобрались как JSON: {e}"))?;
    let Some(raw) = root.get("conditions") else { return Ok(Vec::new()) };
    let list = raw.as_array().ok_or("conditions обязан быть массивом")?;
    let mut out: Vec<PutCondition> = Vec::with_capacity(list.len());
    for (at, item) in list.iter().enumerate() {
        let obj =
            item.as_object().ok_or_else(|| format!("conditions[{at}] обязан быть объектом"))?;
        for key in obj.keys() {
            if !matches!(key.as_str(), "id" | "field" | "equals") {
                return Err(format!(
                    "conditions[{at}]: ключ {key:?} неизвестен; ожидались id, field, equals"
                ));
            }
        }
        let id = obj
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("conditions[{at}].id обязан быть строкой"))?
            .to_string();
        let field = obj
            .get("field")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("conditions[{at}].field обязан быть строкой"))?
            .to_string();
        if id.is_empty() || field.is_empty() {
            return Err(format!("conditions[{at}]: id и field не могут быть пустыми"));
        }
        let value =
            obj.get("equals").ok_or_else(|| format!("conditions[{at}].equals отсутствует"))?;
        let expected = match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            _ => {
                return Err(format!(
                    "conditions[{at}].equals обязан быть строкой, числом, true, false или null"
                ))
            }
        };
        if let Some(previous) = out.iter().find(|c| c.id == id && c.field == field) {
            if previous.expected != expected {
                return Err(format!(
                    "conditions[{at}] противоречит предыдущему условию для id {id:?}, поля {field:?}"
                ));
            }
            continue;
        }
        out.push(PutCondition { id, field, expected });
    }
    Ok(out)
}

/// Обслуживание в своём потоке.
///
/// Один заход за круг, а не «пока есть что делать»: между заходами приём и поиск
/// должны получить своё. Уборка внутри захода идёт первой — она возвращает
/// место, а укрупнение только перекладывает.
/// Сброс по времени: свежесть, не зависящая от скорости приёма.
///
/// ## Зачем отдельный поток
///
/// Сброс по одному лишь счётчику даёт свежесть, обратную скорости приёма:
/// `flush_docs / (документов в секунду)`. При двухстах тысячах на сегмент и
/// десяти документах в секунду это пять с половиной часов до видимости — и
/// столько же растёт журнал, потому что отметка двигается только при сбросе.
/// Медленная база получала и невидимые документы, и разбухший журнал, и долгий
/// подъём.
///
/// Поток свой, а не поток обслуживания: тот гасится `--no-maintain`, и свежесть
/// исчезала бы вместе с ним молча. Тик мельче интервала, чтобы правка настройки
/// применялась не через прежний интервал, а сразу.
/// Во сколько раз дольше свежести документ вправе ждать под плотным потоком.
///
/// Сброс по времени и сброс по счётчику ставят разные цели: первый обещает, что
/// документ станет виден скоро, второй — что сегмент выйдет нужного размера. Под
/// заливкой они спорят, и до этой поправки первый выигрывал **всегда**:
/// счётчик документов не срабатывал ни разу за сто миллионов, сегменты выходили
/// по 427 тыс. вместо миллиона, а обслуживание потом сливало их попарно — то
/// есть 87 млн документов из ста проходили сжатие дважды. На двадцати пяти
/// миллионах это 58 сбросов, 25 слияний и 142 секунды в слияниях при 298
/// секундах всей заливки.
///
/// Поправка больше не объединяет freshness с physical flush. `flush_mb=0`
/// означает автоматический WAL/recovery cap, а не «делать сегмент на каждом
/// heartbeat». Сегмент в 64 МиБ достаточно мал для bounded replay и заметно
/// крупнее прежних 256-документных сбросов на production-потоке. Документный
/// порог и L0 backpressure продолжают действовать независимо от объёмного.
const DEFAULT_PHYSICAL_FLUSH_MB: u64 = 64;

fn effective_flush_bytes(
    settings: &svindb_core::settings::Settings,
    wal_replay_bytes: usize,
) -> usize {
    let mb = if settings.flush_mb == 0 { DEFAULT_PHYSICAL_FLUSH_MB } else { settings.flush_mb };
    let configured = mb.saturating_mul(1 << 20).min(usize::MAX as u64) as usize;
    // Flush по байтам и WAL replay cap — независимые причины физической
    // публикации. Реализация имеет один порог и берёт минимум: любой из них
    // обязан сработать, иначе после crash пришлось бы переигрывать больше
    // объявленного объёма.
    configured.min(wal_replay_bytes.max(1))
}

/// Старый предикат оставлен для compatibility-тестов и операторских утилит.
/// Runtime больше не вызывает его из freshness loop: freshness не должна
/// принудительно писать физический сегмент.
#[cfg(test)]
const REFRESH_PATIENCE: u32 = 30;

/// Уступает ли сброс по времени сбросу по счётчику на этом сроке.
///
/// Отдельной функцией, потому что решение проверяемо, а поток — нет.
///
/// `came` — сколько документов пришло за истёкший срок, то есть скорость потока
/// в тех же единицах. Скорость берётся по факту, а не настройкой: настройка
/// была бы ещё одним числом, которое некому подобрать правильно.
#[cfg(test)]
fn yields_to_the_counter(buffered: u32, came: u32, cap: u32, yielded: u32) -> bool {
    // Поток стоит — ждать нечего: без этой проверки нулевая скорость означала
    // бы «успеет когда-нибудь», и сброс не случился бы никогда.
    if came == 0 {
        return false;
    }
    let patience = REFRESH_PATIENCE.saturating_sub(yielded);
    if patience == 0 {
        return false;
    }
    let left = cap.saturating_sub(buffered);
    // Успеет ли буфер набраться сам за оставшееся терпение.
    u64::from(left) <= u64::from(came) * u64::from(patience)
}

fn spawn_refresh(
    writer: Arc<Writer>,
    settings: Arc<RwLock<svindb_core::settings::Settings>>,
    stop: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, String> {
    std::thread::Builder::new()
        .name("index-refresh".into())
        .spawn(move || {
            let step = Duration::from_millis(100);
            let mut idle = Duration::ZERO;
            let mut debt_observed = Instant::now();
            loop {
                std::thread::sleep(step);
                if stop.load(Ordering::Acquire) {
                    return;
                }
                // Отложенная отметка журнала — первым делом, до всякой свежести.
                //
                // Приём её больше не двигает: она стоит `msync` живых карт, двух
                // `fsync` и обрезки со сканированием CRC целых файлов, и всё это
                // делалось в потоке запроса. Здесь та же работа делается один раз
                // за всё накопившееся и никому не мешает.
                if writer.checkpoint_due.swap(false, Ordering::AcqRel) {
                    writer.after_flush();
                }
                if debt_observed.elapsed() >= Duration::from_secs(1) {
                    let cfg = settings.read().expect("настройки").maintain();
                    writer.observe_compaction_debt(cfg);
                    debt_observed = Instant::now();
                }
                let (want, min_docs, max_interval) = {
                    let s = settings.read().expect("настройки");
                    (s.refresh_interval_ms, s.refresh_min_docs, s.refresh_max_interval_ms)
                };
                // Ноль — сборка searchable live view по времени выключена.
                if want == 0 {
                    idle = Duration::ZERO;
                    continue;
                }
                // Буфер пуст — таймер не идёт. Иначе узел без приёма молотил бы
                // сбросы вхолостую, а каждый из них это `fsync` списка сегментов.
                let buffered = writer.ingest.lock().map(|ix| ix.buffered()).unwrap_or(0);
                if buffered == 0 {
                    idle = Duration::ZERO;
                    continue;
                }
                idle += step;
                if !refresh_due(
                    idle.as_millis().min(u128::from(u64::MAX)) as u64,
                    buffered,
                    want,
                    min_docs,
                    max_interval,
                ) {
                    continue;
                }
                idle = Duration::ZERO;

                // Свежесть отделена от сброса, и это главная развязка этого потока.
                //
                // Видимым буфер становится **здесь** — пересборкой живого сегмента,
                // которая не трогает диск вовсе. На диск он уезжает ниже и по своим
                // порогам, куда более редким. Прежде эти два события были одним, и
                // потому свежесть покупалась только сегментом на каждый heartbeat.
                if writer.healthy().is_ok() {
                    // Замок отпускается до `after_flush`: она берёт его сама, а
                    // повторный захват — это остановка потока навсегда.
                    let landed = {
                        let _visibility = writer
                            .visibility
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let _generation = VisibilityMutation::begin(&writer.visibility_generation);
                        match writer.ingest.lock() {
                            Ok(mut ix) => writer.refresh_live(&mut ix),
                            Err(_) => false,
                        }
                    };
                    if landed {
                        writer.after_flush();
                    }
                }
            }
        })
        .map_err(|e| format!("поток свежести не создан: {e}"))
}

fn spawn_maintenance(
    writer: Arc<Writer>,
    settings: Arc<RwLock<svindb_core::settings::Settings>>,
    stop: Arc<AtomicBool>,
    lane: Arc<Mutex<()>>,
) -> Result<std::thread::JoinHandle<()>, String> {
    std::thread::Builder::new()
        .name("index-maintenance".into())
        .spawn(move || {
            let mut delay = Duration::from_secs(2);
            loop {
                std::thread::sleep(delay);
                if stop.load(Ordering::Acquire) {
                    return;
                }
                delay = Duration::from_secs(2);
                // Память слияния выведена из ресурса **процесса**, а не индекса.
                // Несколько индексов не вправе одновременно взять каждый всю эту
                // долю. `try_lock` важен для удаления: закрываемый индекс не ждёт
                // минуты за чужим длинным merge, а проверит stop на следующем тике.
                let _lane = match lane.try_lock() {
                    Ok(guard) => guard,
                    Err(std::sync::TryLockError::WouldBlock) => {
                        delay = Duration::from_millis(100);
                        continue;
                    }
                    Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                };
                if stop.load(Ordering::Acquire) {
                    return;
                }
                // Настройки читаются **на каждом заходе**, а не берутся при запуске:
                // иначе `PUT /_settings` применялся бы только к перезапуску.
                let cfg = {
                    let mut cfg = settings.read().expect("настройки").maintain();
                    // Свободное место — снаружи: ядро индекса про файловые системы
                    // не знает, а работа, которой некуда лечь, не должна начаться.
                    cfg.disk_free_bytes = host::disk_free(&writer.dir).unwrap_or(0);
                    cfg
                };
                // Забрать итог фонового сброса, если приём замолчал сразу после
                // передачи буфера. Без этого сегмент лежал бы записанным, а отметка
                // журнала стояла бы на месте до следующего документа — то есть,
                // возможно, до утра.
                let landed = {
                    let _visibility = writer
                        .visibility
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let _generation = VisibilityMutation::begin(&writer.visibility_generation);
                    let mut ix = writer.ingest.lock().expect("буфер приёма");
                    let before = ix.segments();
                    ix.poll();
                    writer.observe_flushes(&mut ix);
                    if ix.segments() != before {
                        writer.publish(&mut ix);
                        true
                    } else {
                        false
                    }
                };
                if landed {
                    writer.after_flush();
                }
                // Сегменты берутся уже открытые. Прежде заход открывал свои — то
                // есть каждые две секунды перечитывал весь индекс с полным разбором
                // словаря каждого сегмента, и всё это под замком состава. Узлу,
                // которому нечего укрупнять, это стоило сорока трёх тысяч
                // бесполезных перечитываний в сутки.
                //
                // Набор держится ссылкой на всё время работы: под ним живут сами
                // сегменты, а `reload` посреди слияния подменяет только список.
                // Объявлен он снаружи, а берётся внутри, потому что выбранная работа
                // ссылается на его сегменты и обязана их пережить.
                let segs;
                // Выбор — под замком и быстро. Набор берётся под тем же замком:
                // возьми мы его раньше, выбирать пришлось бы по составу, который уже
                // мог смениться, и слияние ушло бы в снесённый уборкой сегмент.
                let (picked, refresh_catalog, catalog_logical_bytes) = {
                    let _guard = writer.layout.lock().expect("состав сегментов");
                    segs = writer.live.read().expect("набор сегментов").clone();
                    let refs: Vec<&svindb_core::Index> =
                        segs.list.iter().map(|a| a.as_ref()).collect();
                    let debt = maintain::backlog(&refs, cfg);
                    writer.compaction_pending.store(debt, Ordering::Relaxed);
                    writer.compaction_floor.fetch_min(debt, Ordering::AcqRel);
                    writer.compaction_pending_bytes.store(
                        if debt == 0 {
                            0
                        } else {
                            refs.iter().map(|segment| segment.bytes.total).sum()
                        },
                        Ordering::Relaxed,
                    );
                    if debt == 0 && writer.compaction_running.load(Ordering::Acquire) == 0 {
                        let ready = writer.ready_seq.load(Ordering::Acquire);
                        writer.settled_seq.fetch_max(ready, Ordering::Release);
                    }
                    let schedulable = maintain::schedulable(&refs, cfg);
                    writer.compaction_schedulable.store(schedulable, Ordering::Relaxed);
                    let mut why = maintain::Blocked::default();
                    let out = maintain::choose_why(&writer.dir, &refs, cfg, &mut why);
                    let catalog_resources_available = matches!(
                        why,
                        maintain::Blocked::Nothing
                            | maintain::Blocked::AllFrozen
                            | maintain::Blocked::SchemaSplit
                            | maintain::Blocked::OverOffset
                            | maintain::Blocked::OverSize
                    );
                    *writer.compaction_blocked.lock().unwrap_or_else(|e| e.into_inner()) = why;
                    let refresh_catalog = catalog_resources_available
                        && matches!(&out, Ok(None))
                        && schedulable == 0
                        && maintain::catalog_refresh_needed(&writer.dir, &refs);
                    let catalog_covered =
                        refs.len().saturating_sub(maintain::CATALOG_RETAIN_TAIL).max(1);
                    let catalog_logical_bytes = refs
                        .iter()
                        .take(catalog_covered)
                        .map(|segment| segment.docs as u64)
                        .sum::<u64>()
                        .saturating_mul(16);
                    (out, refresh_catalog, catalog_logical_bytes)
                };
                if refresh_catalog {
                    let Some(_compaction_permit) = resources().enter(ResourceClass::Compaction)
                    else {
                        delay = Duration::from_millis(50);
                        continue;
                    };
                    writer.compaction_running.store(1, Ordering::Relaxed);
                    let mark = Instant::now();
                    let hook_before = hook_snapshot();
                    let io_before = host::thread_io();
                    let refreshed = maintain::refresh_catalog(&writer.dir);
                    let io = host::thread_io().delta(io_before);
                    let hook_delta = hook_snapshot().delta(hook_before);
                    record_compaction_job(&writer.dir, mark.elapsed(), hook_delta);
                    let output = std::fs::metadata(format!(
                        "{}/{}",
                        writer.dir,
                        svindb_core::catalog::FILE
                    ))
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                    writer.metrics.compaction_io(catalog_logical_bytes, output, io);
                    eprintln!(
                        "обслуживание index={} kind=id_catalog status={} input_bytes={} output_bytes={} physical_read={} physical_write={} elapsed_ms={:.3}",
                        writer.dir,
                        if refreshed { "ok" } else { "failed" },
                        catalog_logical_bytes,
                        output,
                        io.read_bytes.unwrap_or(0),
                        io.write_bytes.unwrap_or(0),
                        mark.elapsed().as_secs_f64() * 1000.0,
                    );
                    writer.compaction_running.store(0, Ordering::Relaxed);
                    if !refreshed {
                        // Повтор гигабайтной пересборки в tight loop только
                        // усугубил бы ENOSPC/PSI. Следующая попытка остаётся
                        // автоматической, но с операторски заметной паузой.
                        delay = Duration::from_secs(60);
                    }
                    continue;
                }
                let job = match picked {
                    Ok(Some(j)) => j,
                    Ok(None) => continue,
                    Err(e) => {
                        eprintln!("обслуживание: {e}");
                        writer.compaction_running.store(0, Ordering::Relaxed);
                        continue;
                    }
                };
                let Some(_compaction_permit) = resources().enter(ResourceClass::Compaction) else {
                    // Другой индекс того же процесса уже использует тяжёлую
                    // I/O-полосу. Работа не теряется: план будет выбран снова
                    // на следующем коротком тике.
                    delay = Duration::from_millis(50);
                    continue;
                };
                *writer.compaction_blocked.lock().unwrap_or_else(|e| e.into_inner()) =
                    maintain::Blocked::Nothing;
                writer.compaction_running.store(job.sources().len(), Ordering::Relaxed);
                // Слияние — **без замка**, и в этом весь смысл разделения. Прежде
                // оно шло под замком состава целиком: на миллионе документов это
                // секунды и потому незаметно, на пятигигабайтном сегменте — минуты,
                // и на эти минуты вставали приём и удаления. Пока замка нет, приём
                // дописывает сегменты, а удаления гасят документы в источниках;
                // первое публикации безразлично, второе она переносит в слитое.
                let mark = Instant::now();
                let hook_before = hook_snapshot();
                // `/proc/self/io` включает чтение одновременно работающего
                // search. Берём счётчики текущего maintenance TID, чтобы
                // read/write amplification jobs не обвиняли друг друга.
                let io_before = host::thread_io();
                let logical_input = job.input_bytes();
                let input_docs = job.input_docs();
                let job_kind = job.kind_name();
                let merged = match maintain::run_all(&job, &writer.dir) {
                    Ok(m) => m,
                    Err(e) => {
                        let io = host::thread_io().delta(io_before);
                        let hook_delta = hook_snapshot().delta(hook_before);
                        record_compaction_job(&writer.dir, mark.elapsed(), hook_delta);
                        writer.metrics.compaction_io(logical_input, 0, io);
                        eprintln!(
                            "обслуживание index={} kind={job_kind} status=failed sources={} input_docs={input_docs} input_bytes={logical_input} physical_read={} physical_write={} elapsed_ms={:.3}: {e}",
                            writer.dir,
                            job.sources().len(),
                            io.read_bytes.unwrap_or(0),
                            io.write_bytes.unwrap_or(0),
                            mark.elapsed().as_secs_f64() * 1000.0,
                        );
                        writer.compaction_running.store(0, Ordering::Relaxed);
                        continue;
                    }
                };
                let logical_output: u64 = merged.iter().map(|output| output.bytes).sum();
                // Публикация — снова под замком и снова быстро.
                //
                // Замки берутся в общем для писателя порядке: сперва буфер приёма,
                // потом состав. Обратный порядок здесь означал бы взаимную
                // блокировку с любым путём приёма, который уже держит буфер и
                // тянется к составу, — а таковы `push` и `refresh_live`, то есть
                // почти всё. Буфер нужен, чтобы перевесить обязательства гашения с
                // съеденных источников на выход.
                let done = {
                    let mut ix = writer.ingest.lock().expect("буфер приёма");
                    let _guard = writer.layout.lock().expect("состав сегментов");
                    let done = maintain::publish_all(&writer.dir, &job, &merged, cfg);
                    if done.is_ok() {
                        writer.metrics.merged(mark.elapsed().as_secs_f64() * 1000.0);
                        // Обслуживание переписало сегменты; карты живых обязаны
                        // лечь до того, как новый состав станет видимым. Не легли —
                        // приём останавливается: продолжать, зная, что гашения
                        // держатся только в памяти, нельзя.
                        if let Err(e) = writer.sync_live() {
                            eprintln!("обслуживание: карта живых не легла — {e}");
                            writer.poisoned.store(true, Ordering::SeqCst);
                        }
                        // Обязательства погасить прежние версии переезжают с
                        // съеденных источников на выход — до перезагрузки состава.
                        //
                        // Иначе они остаются привязанными к именам, которых больше
                        // нет: `supersede_in` разрешает хранителя точным
                        // сравнением, не находит и кладёт обратно под тем же
                        // мёртвым именем — круг за кругом, впустую. Разбор и цена
                        // ошибки — у [`Indexer::rename_keeper`].
                        ix.rename_keeper(job.sources(), &format!("{}/{}", writer.dir, job.out()));
                        writer.reload();
                    }
                    done
                };
                // Только после durable publish: до него output ещё может иметь
                // dirty страницы, которые DONTNEED не обязан сбрасывать. Merge
                // читает источники последовательно и пишет cold output; эти
                // чистые страницы не должны занимать весь cgroup до первой
                // пользовательской ошибки. Cache сбрасывается лишь при
                // отсутствии point/search guard, а `try_write` никогда не
                // ставит query в очередь за обслуживанием.
                let cache_dropped_bytes = match query_cache_gate().try_write() {
                    Ok(_exclusive) => job
                        .sources()
                        .iter()
                        .chain(job.outs().iter())
                        .map(|name| {
                            host::drop_immutable_segment_cache(&format!(
                                "{}/{}",
                                writer.dir, name
                            ))
                        })
                        .sum::<u64>(),
                    Err(std::sync::TryLockError::WouldBlock) => 0,
                    Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                        let _exclusive = poisoned.into_inner();
                        job.sources()
                            .iter()
                            .chain(job.outs().iter())
                            .map(|name| {
                                host::drop_immutable_segment_cache(&format!(
                                    "{}/{}",
                                    writer.dir, name
                                ))
                            })
                            .sum::<u64>()
                    }
                };
                let io = host::thread_io().delta(io_before);
                let hook_delta = hook_snapshot().delta(hook_before);
                record_compaction_job(&writer.dir, mark.elapsed(), hook_delta);
                writer.metrics.compaction_io(logical_input, logical_output, io);
                let elapsed_ms = mark.elapsed().as_secs_f64() * 1000.0;
                let amp = |physical: u64, logical: u64| {
                    if logical == 0 { 0.0 } else { physical as f64 / logical as f64 }
                };
                match done {
                    Ok(a) => {
                        writer
                            .compaction_completed_sources
                            .fetch_add(job.sources().len() as u64, Ordering::Relaxed);
                        writer
                            .compaction_completed_input_bytes
                            .fetch_add(logical_input, Ordering::Relaxed);
                        writer.compaction_elapsed_micros.fetch_add(
                            u64::try_from(mark.elapsed().as_micros()).unwrap_or(u64::MAX),
                            Ordering::Relaxed,
                        );
                        println!(
                            "обслуживание index={} kind={job_kind} status=ok action={a:?} sources={} input_docs={input_docs} input_bytes={logical_input} output_bytes={logical_output} physical_read={} physical_write={} cache_dropped_bytes={cache_dropped_bytes} read_amp={:.4} write_amp={:.4} elapsed_ms={elapsed_ms:.3}",
                            writer.dir,
                            job.sources().len(),
                            io.read_bytes.unwrap_or(0),
                            io.write_bytes.unwrap_or(0),
                            amp(io.read_bytes.unwrap_or(0), logical_input),
                            amp(io.write_bytes.unwrap_or(0), logical_output),
                        );
                        // Пока долг есть, следующий выбор идёт почти сразу. Прежняя
                        // обязательная пауза в две секунды после каждого успешного
                        // слияния делала catch-up математически медленнее рождения
                        // мелких сегментов.
                        delay = Duration::from_millis(50);
                    }
                    Err(e) => eprintln!(
                        "обслуживание index={} kind={job_kind} status=publish_failed sources={} input_bytes={logical_input} output_bytes={logical_output} physical_read={} physical_write={} elapsed_ms={elapsed_ms:.3}: {e}",
                        writer.dir,
                        job.sources().len(),
                        io.read_bytes.unwrap_or(0),
                        io.write_bytes.unwrap_or(0),
                    ),
                }
                writer.compaction_running.store(0, Ordering::Relaxed);
            }
        })
        .map_err(|e| format!("поток обслуживания не создан: {e}"))
}

/// Условия отбора: `"filter":[{"field":2,"from":100,"to":500}]`.
///
/// Границы необязательны: без `from` край открыт снизу, без `to` — сверху.
/// Значения дробных полей передаются числом и переводятся в тот же
/// порядок-сохраняющий вид, в каком лежат в колонке.
fn parse_filter(
    v: &serde_json::Value,
    names: &[String],
    kinds: &[Kind],
    slot_groups: &[(usize, u32)],
    groups: &[(String, u32)],
) -> Result<Vec<svindb_core::segments::SpecRange>, String> {
    let Some(list) = v.get("filter") else { return Ok(Vec::new()) };
    let Some(list) = list.as_array() else { return Err("filter: нужен массив".into()) };
    let mut out = Vec::with_capacity(list.len());
    for (i, c) in list.iter().enumerate() {
        only_keys(c, &format!("filter[{i}]"), &["field", "from", "to"])?;
        let field = field_ref(c.get("field"), &format!("filter[{i}]"), names)?;
        // Отбор адресует колонку номером документа, а у группового поля она
        // адресована ординалом элемента. Условие «сошлось в одном элементе»
        // выражается через `slots`, и об этом говорит текст отказа.
        refuse_slot_misuse(field, &format!("filter[{i}]"), names, slot_groups, groups, None)?;
        // Границы по ключевому полю сравнивали бы номера слов в словаре этого
        // сегмента — у соседнего те же номера значат другие слова. Отбор без
        // границ (наличие значения) остаётся: он осмыслен.
        let has_edges = c.get("from").is_some() || c.get("to").is_some();
        if has_edges && kinds.get(field).copied().unwrap_or(Kind::Text).keyword() {
            return Err(format!(
                "filter[{i}]: поле {:?} — ключевое слово, у него значение строка, а не число. \
                 Для точного совпадения возьмите terms, для наличия — отбор без границ",
                names.get(field).map(|s| s.as_str()).unwrap_or("")
            ));
        }
        // Границы приводятся **по типу поля**, а не по виду записи в JSON.
        let floating = kinds.get(field).copied().unwrap_or(Kind::Text).floating();
        let edge = |name: &str, dflt: i64| -> Result<i64, String> {
            match c.get(name) {
                None | Some(serde_json::Value::Null) => Ok(dflt),
                Some(x) => {
                    let n = x.as_f64().ok_or_else(|| format!("filter[{i}].{name}: нужно число"))?;
                    if floating {
                        Ok(svindb_core::index::float_key(n))
                    } else if let Some(v) = x.as_i64() {
                        Ok(v)
                    } else {
                        Err(format!("filter[{i}].{name}: у целого поля нужна целая граница"))
                    }
                }
            }
        };
        out.push(svindb_core::segments::SpecRange {
            fid: field,
            from: edge("from", i64::MIN)?,
            to: edge("to", i64::MAX)?,
        });
    }
    Ok(out)
}

/// Строка в виде литерала JSON — для сообщений об ошибках.
fn json_string(s: &str) -> String {
    serde_json::Value::String(s.to_string()).to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_multi_index_query_uses_the_strictest_explicit_memory_budget() {
        use super::strictest_memory_budget;

        assert_eq!(strictest_memory_budget([0, 512, 128, 256]), 128);
        assert_eq!(strictest_memory_budget([512, 0, 256]), 256);
        assert_eq!(strictest_memory_budget([0, 0]), 0);
        assert_eq!(strictest_memory_budget([64]), 64);
    }

    #[test]
    fn configured_latency_reserve_cannot_consume_a_small_cgroup() {
        use super::effective_latency_reserve;

        assert_eq!(effective_latency_reserve(Some(25 << 30), 2 << 30), 2 << 30);
        assert_eq!(effective_latency_reserve(Some(1 << 30), 2 << 30), 512 << 20);
        assert_eq!(effective_latency_reserve(Some(4 << 30), 3 << 30), 2 << 30);
        assert_eq!(effective_latency_reserve(None, 2 << 30), 2 << 30);
    }

    /// Применение идёт строго по возрастанию номера, как бы ни легли потоки.
    ///
    /// Проверяется не «не упало», а **порядок**: потоки заводятся в обратном
    /// порядке номеров и получают фору тем большую, чем номер меньше, — то есть
    /// стенд специально подталкивает их примениться наоборот. Без очереди
    /// записанный порядок был бы обратным; с ней он обязан быть возрастающим.
    #[test]
    fn applying_follows_the_numbering_no_matter_how_the_threads_fall() {
        use super::{Applied, WalTicket};
        use std::sync::{Arc, Mutex};

        let applied = Arc::new(Applied::new(0));
        let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let mut hands = Vec::new();
        // Номера раздаются заранее и целиком — ровно как их раздаёт `stage`.
        for seq in (1..=16u64).rev() {
            let (applied, order) = (Arc::clone(&applied), Arc::clone(&order));
            hands.push(std::thread::spawn(move || {
                // Фора наоборот: младший номер трогается позже старшего.
                std::thread::sleep(std::time::Duration::from_millis(seq * 4));
                let ticket =
                    applied.claim(WalTicket { last: seq, records: 1 }).expect("очередь применения");
                order.lock().expect("порядок").push(seq);
                drop(ticket);
            }));
        }
        for h in hands {
            h.join().expect("поток применения");
        }
        let seen = order.lock().expect("порядок").clone();
        assert_eq!(seen, (1..=16u64).collect::<Vec<_>>(), "применение разошлось с нумерацией");
        assert_eq!(applied.watermark(), 16);
    }

    /// Место в очереди освобождается и тогда, когда применение не состоялось.
    ///
    /// Утёкший билет останавливал бы запись навсегда: следующий по номеру ждал
    /// бы предшественника, которого уже нет. Поэтому освобождение висит на
    /// `Drop`, а не на успешном пути.
    #[test]
    fn a_dropped_place_in_the_queue_does_not_wedge_the_next_one() {
        use super::{Applied, WalTicket};

        let applied = Applied::new(0);
        // Первый берёт место и роняет его, ничего не применив.
        drop(applied.claim(WalTicket { last: 1, records: 1 }).expect("первое место"));
        assert_eq!(applied.watermark(), 1);
        // Второй обязан пройти сразу, а не ждать предела.
        let began = std::time::Instant::now();
        let ticket = applied.claim(WalTicket { last: 2, records: 1 }).expect("второе место");
        assert!(began.elapsed() < std::time::Duration::from_secs(5), "второй ждал ушедшего");
        drop(ticket);
        assert_eq!(applied.watermark(), 2);
    }

    /// Пачка занимает **диапазон** номеров, а не один: следующая ждёт весь.
    #[test]
    fn a_batch_holds_its_whole_range_of_numbers() {
        use super::{Applied, WalTicket};

        let applied = Applied::new(10);
        let batch = WalTicket { last: 14, records: 4 };
        assert_eq!(batch.first(), 11);
        let ticket = applied.claim(batch).expect("диапазон 11..=14");
        assert_eq!(applied.watermark(), 10, "место занято, но ещё не применено");
        drop(ticket);
        assert_eq!(applied.watermark(), 14, "диапазон освобождён целиком");
    }

    #[test]
    fn serve_with_one_number_uses_default_data_directory_and_that_port() {
        let (dir, port, defaulted) =
            super::serve_location(&["serve", "19300"], None, "/opt/svindb/data").expect("запуск");
        assert_eq!(dir, "/opt/svindb/data");
        assert_eq!(port, 19300);
        assert!(defaulted);

        let (dir, port, defaulted) =
            super::serve_location(&["serve", "/var/lib/svindb", "19400"], None, "/ignored")
                .expect("запуск");
        assert_eq!(dir, "/var/lib/svindb");
        assert_eq!(port, 19400);
        assert!(!defaulted);

        let (dir, port, defaulted) =
            super::serve_location(&["serve", "./19300"], Some(19500), "/ignored")
                .expect("числовой каталог");
        assert_eq!(dir, "./19300");
        assert_eq!(port, 19500);
        assert!(!defaulted);
    }

    #[test]
    fn default_data_directory_is_next_to_the_executable() {
        let dir = super::data_dir_beside(std::path::Path::new("/opt/svindb/svindb"))
            .expect("каталог данных");
        assert_eq!(dir, "/opt/svindb/data");
    }

    #[test]
    fn serve_rejects_an_invalid_port_and_extra_positionals() {
        let bad_port = super::serve_location(&["serve", "/data", "70000"], None, "/ignored")
            .expect_err("порт вне диапазона принят");
        assert!(bad_port.contains("70000"), "отказ не назвал порт: {bad_port}");

        let extra = super::serve_location(&["serve", "/data", "19300", "лишнее"], None, "/ignored")
            .expect_err("лишний аргумент принят");
        assert!(extra.contains("получено 3"), "непонятный отказ: {extra}");
    }

    #[test]
    fn serve_is_writable_and_maintained_by_default() {
        assert_eq!(super::serve_mode(&[]).expect("умолчание"), (true, true));
        assert_eq!(super::serve_mode(&["--read-only"]).expect("только чтение"), (false, false));
        assert_eq!(super::serve_mode(&["--no-maintain"]).expect("без обслуживания"), (true, false));
    }

    #[test]
    fn a_new_catalog_is_multi_by_default_and_legacy_is_detected() {
        use super::multi::LayoutKind::{Legacy, Multi, New};
        use super::ServeRuntime::{Multi as MultiRuntime, Single};

        assert_eq!(super::select_serve_runtime(New, &[]).unwrap(), MultiRuntime);
        assert_eq!(super::select_serve_runtime(Multi, &[]).unwrap(), MultiRuntime);
        assert_eq!(super::select_serve_runtime(Legacy, &[]).unwrap(), Single);
        assert_eq!(super::select_serve_runtime(New, &["--multi-index"]).unwrap(), MultiRuntime);
        assert_eq!(super::select_serve_runtime(New, &["--single-index"]).unwrap(), Single);
        assert_eq!(
            super::select_serve_runtime(New, &["--shards=127.0.0.1:19400"]).unwrap(),
            Single
        );
        assert!(super::select_serve_runtime(Multi, &["--single-index"]).is_err());
        assert!(super::select_serve_runtime(Multi, &["--replicas=127.0.0.1:19400"]).is_err());
        assert!(super::select_serve_runtime(New, &["--single-index", "--max-indices=2"]).is_err());
        assert!(super::select_serve_runtime(New, &["--single-index", "--multi-index"]).is_err());
    }

    /// Нехватка областей отображения: отказ — только боевому узлу.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Правило одно на четыре случая, и каждый из них ломается по-своему.
    /// Ошибись в первом — и узел на ноутбуке, где `vm.max_map_count` трогать
    /// некому, перестанет запускаться вовсе. Ошибись во втором — и боевой узел
    /// стартует, чтобы упасть отказом `mmap` через минуты работы, то есть ровно
    /// то, ради чего проверка и заводилась. Третий и четвёртый — вентили: без
    /// них реплика на чтение и узел в контейнере, где предел задан хозяином,
    /// становятся незапускаемыми по причине, которую они устранить не могут.
    #[test]
    fn only_a_node_reachable_from_outside_refuses_to_start_on_a_low_map_count() {
        use svindb_core::mmap::MapShortfall::{Refuse, Warn};

        // Петля — предупреждение: это стенд, отладка, ноутбук.
        for bind in ["127.0.0.1", "::1", "localhost"] {
            assert_eq!(
                super::map_shortfall_for(bind, false, false),
                Warn,
                "узел на петле {bind} отказался стартовать"
            );
        }

        // Непетлевой адрес — боевой режим, отказ.
        for bind in ["0.0.0.0", "10.0.0.7", "::"] {
            assert_eq!(
                super::map_shortfall_for(bind, false, false),
                Refuse,
                "узел на {bind} стартовал бы у самого потолка"
            );
        }

        // Только чтение — предупреждение даже вне петли: сегментов такой узел
        // не заводит, и неоткрывшаяся реплика хуже работающей у потолка.
        assert_eq!(super::map_shortfall_for("0.0.0.0", true, false), Warn);

        // Вентиль снимает отказ всегда, в том числе у боевого узла.
        assert_eq!(super::map_shortfall_for("0.0.0.0", false, true), Warn);
        assert_eq!(super::map_shortfall_for("10.0.0.7", true, true), Warn);
    }

    /// Петля опознаётся тем же признаком, что и открытость узла наружу.
    ///
    /// Признак один на два решения — предупреждение про отсутствие прав и
    /// правило отказа на нехватку областей. Разъедься они, и узел объявлял бы
    /// себя боевым в одном месте и стендом в другом.
    #[test]
    fn loopback_is_recognised_by_one_predicate() {
        assert!(super::loopback("127.0.0.1"));
        assert!(super::loopback("::1"));
        assert!(super::loopback("localhost"));
        assert!(!super::loopback("0.0.0.0"));
        assert!(!super::loopback("::"));
        assert!(!super::loopback("192.168.1.4"));
        // Похожее — не петля: подстрока не признак.
        assert!(!super::loopback("127.0.0.10"));
        assert!(!super::loopback("localhost.example.com"));
    }

    #[test]
    fn serve_rejects_removed_positive_flags() {
        for flag in ["--write", "--maintain"] {
            let error = super::serve_mode(&[flag]).expect_err("старый флаг принят");
            assert!(error.contains("удалены"), "непонятный отказ: {error}");
        }
    }

    /// Обязательные и запрещённые читаются и плоско, и из дерева.
    #[test]
    fn must_and_must_not_are_read_flat_too() {
        let two = super::two();
        let flat =
            br#"{"k":5,"must":[{"field":0,"terms":["a"]}],"must_not":[{"field":1,"terms":["b"]}]}"#;
        let s = super::parse_query(flat, &two, &[Kind::Text; 2], &[], &[])
            .expect("плоская форма отвергнута");
        assert_eq!(s.must.len(), 1, "обязательное не прочлось");
        assert_eq!(s.must_not.len(), 1, "запрещённое не прочлось");

        let tree = br#"{"k":5,"query":{"must":[{"term":{"field":0,"terms":["a"]}}],
                        "must_not":[{"term":{"field":1,"terms":["b"]}}]}}"#;
        let t =
            super::parse_query(tree, &two, &[Kind::Text; 2], &[], &[]).expect("дерево отвергнуто");
        assert_eq!((t.must.len(), t.must_not.len()), (1, 1));

        assert!(super::parse_query(br#"{"k":5}"#, &two, &[Kind::Text; 2], &[], &[]).is_err());
    }

    /// Глубокая страница набирает верхушку на `from + size`, а не на `size`.
    #[test]
    fn a_deep_page_collects_the_whole_window() {
        let two = super::two();
        let q = |from: usize, size: usize| {
            let body =
                format!(r#"{{"words":[{{"field":0,"terms":["a"]}}],"from":{from},"size":{size}}}"#);
            super::parse_query(body.as_bytes(), &two, &[Kind::Text; 2], &[], &[]).expect("запрос")
        };
        assert_eq!(q(0, 10).k, 10, "первая страница");
        assert_eq!(q(990, 10).k, 1000, "верхушка обязана накрыть и пропущенное");
        assert_eq!(q(990, 10).from, 990);
        // Потолок считается по той же сумме, а не по одному `size`.
        let deep = r#"{"words":[{"field":0,"terms":["a"]}],"from":9995,"size":10}"#;
        let e = super::parse_query(deep.as_bytes(), &two, &[Kind::Text; 2], &[], &[])
            .err()
            .expect("за потолком принят");
        assert!(e.contains("10005"), "отказ не назвал сумму: {e}");
    }

    /// Схема берётся из сегмента, а не из умолчаний движка.
    ///
    /// Поля объявляются при сборке: `svindb index … passage` даёт сегмент с
    /// единственным полем `passage`, и заголовок сегмента это несёт. Сервер же
    /// спрашивал зашитые «title, body» — и отвечал «поле passage не объявлено:
    /// есть title, body», то есть перечислял поля, которых в индексе нет.
    ///
    /// Проверяется обеими сторонами: своё поле обязано находиться, чужое —
    /// отвергаться **с перечислением настоящих**. Без второй половины тест
    /// прошёл бы и на зашитом списке.
    #[test]
    fn the_schema_comes_from_the_segment_not_from_defaults() {
        let one = vec!["passage".to_string()];
        let q = br#"{"k":3,"words":[{"field":0,"terms":["a"]}],"fields":["passage"]}"#;
        assert!(
            super::parse_query(q, &one, &vec![Kind::Text; one.len()], &[], &[]).is_ok(),
            "поле из схемы сегмента отвергнуто"
        );
        let err = super::parse_query(q, &super::two(), &[Kind::Text; 2], &[], &[])
            .err()
            .expect("чужое поле принято");
        assert!(err.contains("title, body"), "отказ не назвал настоящие поля: {err}");

        // И обратно: при схеме из двух полей номер 1 законен, а при схеме из
        // одного — нет, и отказ обязан быть ответом, а не паникой.
        let two_fields = br#"{"k":1,"words":[{"field":1,"terms":["a"]}]}"#;
        assert!(super::parse_query(two_fields, &super::two(), &[Kind::Text; 2], &[], &[]).is_ok());
        let err = super::parse_query(two_fields, &one, &vec![Kind::Text; one.len()], &[], &[])
            .err()
            .expect("номер за схемой принят");
        assert!(err.contains("поля 1 нет"), "отказ не про номер поля: {err}");
    }

    use super::*;

    #[test]
    fn parses_a_normal_query() {
        let spec = parse_query(
            br#"{"k":5,"words":[{"field":0,"terms":["dom","dom*"]}]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(spec.k, 5);
        assert_eq!(spec.words.len(), 1);
        assert_eq!(spec.words[0].fid, 0);
        assert_eq!(spec.words[0].terms.len(), 2);
        assert!(!spec.words[0].terms[0].prefix);
        assert!(spec.words[0].terms[1].prefix);
        assert_eq!(spec.words[0].terms[1].text, "dom");
    }

    /// Ровно тот случай, на котором ломался поиск подстрокой: цифры и скобки
    /// внутри слова.
    #[test]
    fn terms_with_digits_and_brackets_survive() {
        let spec = parse_query(
            br#"{"k":10,"words":[{"field":1,"terms":["t1_476493","a]b"]}]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(spec.words[0].terms[0].text, "t1_476493");
        assert_eq!(spec.words[0].terms[1].text, "a]b");
    }

    #[test]
    fn defaults_and_limits() {
        let spec = parse_query(
            br#"{"words":[{"field":0,"terms":["a"]}]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(spec.k, 10);
        assert_eq!(spec.max_expansions, DEFAULT_EXPANSIONS);

        let big = parse_query(
            br#"{"k":999999,"words":[{"field":0,"terms":["a"]}]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(big.k, 10_000, "k зажимается, а не выделяет кучу по запросу");
    }

    /// Запрос из одних пустых слов — это отказ, а не пустая выдача.
    #[test]
    fn explicit_exclusion_is_a_query_even_without_positive_terms() {
        let spec = parse_query(
            br#"{"query":{"must_not":[{"term":{"field":0,"terms":["alpha"]}}]}}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        )
        .expect("явное исключение отвергнуто как пустой запрос");
        assert_eq!(spec.must_not.len(), 1);
        assert!(spec.words.is_empty());
        assert!(spec.must.is_empty());
    }

    #[test]
    fn a_query_of_empty_terms_is_refused_not_silently_empty() {
        let err = match parse_query(
            br#"{"words":[{"field":0,"terms":["",""]}]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        ) {
            Err(e) => e,
            Ok(_) => panic!("запрос без единого слова обязан быть отвергнут"),
        };
        assert!(err.contains("без единого слова"), "получили: {err}");
    }

    /// Полный обход разрешён только явным признаком, поэтому он не превращает
    /// случайно пустое тело в дорогой запрос ко всему индексу.
    #[test]
    fn explicit_match_all_is_accepted_but_an_empty_query_is_not() {
        let spec = parse_query(
            br#"{"match_all":true,"size":25,"from":50,"ids":true}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        )
        .expect("явный match_all");
        assert!(spec.nested.is_some(), "match_all не включил полный обход");
        assert_eq!(spec.k, 75, "верхушка обязана учитывать from");
        assert_eq!(spec.from, 50);
        assert!(spec.with_ids);

        assert!(parse_query(br#"{"size":25}"#, &crate::two(), &[Kind::Text; 2], &[], &[]).is_err());
    }

    /// Негодное тело — это 400 с объяснением, а не пустая выдача.
    #[test]
    fn bad_bodies_are_rejected() {
        assert!(parse_query(b"not json", &crate::two(), &[Kind::Text; 2], &[], &[]).is_err());
        assert!(parse_query(br#"{"k":10}"#, &crate::two(), &[Kind::Text; 2], &[], &[]).is_err());
        assert!(parse_query(
            br#"{"words":[{"terms":["a"]}]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[]
        )
        .is_err());
        assert!(parse_query(
            br#"{"words":[{"field":0}]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[]
        )
        .is_err());
        assert!(parse_query(
            br#"{"k":"ten","words":[]}"#,
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[]
        )
        .is_err());
    }

    /// Пустой список и неразобранное тело — разные ответы, а не один.
    ///
    /// `{"documents":[]}` это законная просьба не делать ничего, и отказ на неё
    /// был бы враньём. А `{"docs":[…]}` с опечаткой в имени поля — негодный
    /// запрос, и молчаливый `{"added":0}` прячет опечатку от того, кто её сделал.
    /// Разбор пачки без сверки типов — для тестов, которым важен только состав.
    fn docs_of(body: &[u8], fields: &[String]) -> Option<Vec<(String, Vec<String>)>> {
        let kinds = vec![Kind::Text; fields.len()];
        let plan = nested::Plan::build(fields, &kinds, &[], &[]);
        parse_docs(body, &plan).ok().map(|(d, _)| {
            d.iter().map(|x| (x.id.clone(), x.flat(fields.len()).unwrap_or_default())).collect()
        })
    }

    #[test]
    fn an_empty_list_is_not_a_broken_body() {
        let f = crate::two();
        assert_eq!(docs_of(br#"{"documents":[]}"#, &f), Some(Vec::new()));
        assert_eq!(parse_ids(br#"{"ids":[]}"#), Some(Vec::new()));

        assert_eq!(docs_of(br#"{"docs":[{"id":"a"}]}"#, &f), None, "опечатка в имени");
        assert_eq!(docs_of(br#"{"documents":{"id":"a"}}"#, &f), None, "не массив");
        assert_eq!(docs_of(b"not json", &f), None);
        assert_eq!(parse_ids(br#"{"id":["a"]}"#), None);

        assert_eq!(docs_of(b"", &f), Some(Vec::new()));
        assert_eq!(parse_ids(b""), Some(Vec::new()));

        let docs = docs_of(br#"{"documents":[{"id":"a","title":"t","body":"b"}]}"#, &f).unwrap();
        assert_eq!(docs, vec![("a".to_string(), vec!["t".to_string(), "b".to_string()])]);
    }

    /// Числовой `id` доходит до индекса, а пустым нельзя адресовать.
    ///
    /// Прежде `id` читался через `as_str`, и `{"id":101}` превращался в пустую
    /// строку: документ становился неадресуемым, `_delete {"ids":["101"]}`
    /// удалял ноль. Офлайновый путь при этом число переводил — один и тот же
    /// корпус давал два разных индекса.
    ///
    /// Хуже было второе: пустой идентификатор общий у всех документов без `id`,
    /// и одно `_delete {"ids":[""]}` гасило их разом — проверено на четырёх
    /// документах, двое из которых слали с `id`.
    #[test]
    fn a_numeric_id_survives_and_an_empty_one_cannot_address() {
        let f = crate::two();
        let body = br#"{"documents":[{"id":101,"title":"a","body":"b"},
                                     {"id":true,"title":"c","body":"d"},
                                     {"title":"e","body":"f"}]}"#;
        let docs = docs_of(body, &f).unwrap();
        assert_eq!(docs[0].0, "101", "число обязано доходить до индекса");
        assert_eq!(docs[1].0, "true");
        assert!(!docs[2].0.is_empty(), "missing id must receive an addressable generated id");

        assert!(no_empty_ids(&["a".to_string(), "b".to_string()]).is_ok());
        let e = no_empty_ids(&["a".to_string(), String::new()]).expect_err("пустой отвергнут");
        assert!(e.contains("пустой идентификатор"), "{e}");
    }

    /// Приём читает поля **этого** индекса, а не зашитые «title, body».
    ///
    /// Прежде `parse_docs` спрашивал `default_fields()`, и всякий индекс с
    /// другой схемой принимал документы, отвечал `{"added":N}` и клал пустоту в
    /// каждое поле. Ни отказа, ни записи в журнал — только пустой индекс,
    /// который замечали по пустой выдаче.
    #[test]
    fn ingest_reads_the_schema_of_this_index() {
        let own: Vec<String> = ["message", "views", "ts"].iter().map(|s| s.to_string()).collect();
        let body =
            r#"{"documents":[{"id":"a","message":"привет","views":7,"ts":1700000000}]}"#.as_bytes();
        let docs = docs_of(body, &own).unwrap();
        assert_eq!(
            docs,
            vec![(
                "a".to_string(),
                vec!["привет".to_string(), "7".to_string(), "1700000000".to_string()]
            )],
            "поля обязаны читаться по схеме, а числа — приводиться к тексту"
        );

        // Та же посылка при зашитой схеме — ровно то, что и происходило:
        // документ принят, все поля пусты.
        let lost = docs_of(body, &crate::two()).unwrap();
        assert_eq!(lost, vec![("a".to_string(), vec![String::new(), String::new()])]);
    }

    /// Непринятое называется, а не пропадает молча.
    ///
    /// Объект, текст в числовом поле и число за разрядностью не вправе
    /// исчезнуть молча. Массив скаляров теперь законен и проверяется отдельно:
    /// прежний тест обязан не объявлять новую возможность потерей.
    #[test]
    fn what_did_not_reach_the_index_is_named() {
        let f: Vec<String> = ["текст", "число"].iter().map(|s| s.to_string()).collect();
        let kinds = vec![Kind::Text, Kind::Int64];
        let body = r#"{"documents":[
            {"id":"a","текст":"годно","число":7},
            {"id":"b","текст":["массив"],"число":"не число","опечатка":1}
        ]}"#
        .as_bytes();
        let plan = nested::Plan::build(&f, &kinds, &[], &[]);
        let (docs, r) = parse_docs(body, &plan).expect("тело разобралось");

        // Массив текста дошёл как однозначная multivalue-запись, неверное
        // число — нет.
        assert_eq!(docs[0].flat(2).unwrap(), vec!["годно".to_string(), "7".to_string()]);
        let second = docs[1].flat(2).unwrap();
        assert_eq!(svindb_core::indexer::multi_parts(&second[0]), Some(vec!["массив"]));
        assert_eq!(second[1], "");

        assert_eq!(r.skipped, 1, "массив скаляров ошибочно назван потерей");
        assert_eq!(r.skipped_docs, 1, "негодное число задело один документ");
        assert_eq!(r.bad_ids, vec!["b".to_string()], "виноватый назван не своим id");
        assert_eq!(r.unknown, vec!["опечатка".to_string()]);
        assert!(!r.clean());
        let s = r.suffix(json_string);
        assert!(s.contains("\"skipped\":1"), "{s}");
        // По `id`, а не по номеру в пачке: на десяти тысячах сообщений
        // «документ 4173» не помогает найти запись — клиенту пришлось бы
        // держать у себя соответствие номера идентификатору.
        assert!(s.contains("документ \\\"b\\\""), "виноватый не назван по id: {s}");
        assert!(!s.contains("документ 1"), "документ всё ещё зовут номером: {s}");
        assert!(s.contains("опечатка"), "{s}");

        // Чистая пачка не приписывает к ответу ничего: молчание — признак того,
        // что всё дошло, и его нельзя тратить на пустые поля.
        let clean = r#"{"documents":[{"id":"a","текст":"т"},{"id":"b","число":1}]}"#.as_bytes();
        let (_, r) = parse_docs(clean, &plan).expect("тело разобралось");
        assert!(r.clean(), "незаполненное поле — не потеря");
        assert_eq!(r.suffix(json_string), "");
    }

    /// Сброс по времени уступает счётчику ровно там, где спор настоящий.
    ///
    /// До этой поправки счётчик документов не срабатывал **ни разу** за сто
    /// миллионов: поток свежести опережал его всегда, сегменты выходили по
    /// 427 тыс. вместо миллиона, и обслуживание потом сливало их попарно — 87
    /// млн документов из ста проходили сжатие дважды. На двадцати пяти
    /// миллионах это 58 сбросов, 25 слияний и 142 секунды в слияниях при 298
    /// секундах всей заливки.
    #[test]
    fn the_timer_yields_to_the_counter_only_under_a_dense_stream() {
        let cap = 1_000_000;

        // Плотная заливка: 83 тыс. за срок. Буфер наберётся сам за двенадцать
        // сроков, терпения тридцать — уступаем, и так до самого конца.
        let mut buffered = 0u32;
        let mut yielded = 0u32;
        let mut flushes = 0;
        for _ in 0..40 {
            buffered += 83_000;
            if buffered >= cap {
                // Сработал счётчик — это и есть цель.
                buffered = 0;
                yielded = 0;
                flushes += 1;
                continue;
            }
            if yields_to_the_counter(buffered, 83_000, cap, yielded) {
                yielded += 1;
            } else {
                panic!("под плотным потоком сброс по времени обязан уступить: {buffered}");
            }
        }
        assert_eq!(flushes, 3, "три полных сегмента за сорок сроков");

        // Редкий поток: сто документов за срок. Буфер не наберётся никогда —
        // ждать нечего, свежесть обещана и обязана быть.
        assert!(!yields_to_the_counter(100, 100, cap, 0));
        assert!(!yields_to_the_counter(500_000, 100, cap, 0));

        // Поток встал совсем: то, что уже лежит, обязано стать видимым.
        assert!(!yields_to_the_counter(900_000, 0, cap, 0));

        // Терпение кончается: даже под плотным потоком ожидание ограничено
        // сверху, иначе обещание свежести превратилось бы в «когда-нибудь».
        // На последнем сроке уступаем, только если буфер и правда доберётся:
        // 83 тыс. хватает на остаток в 50 тыс. и не хватает на остаток в 900.
        assert!(yields_to_the_counter(950_000, 83_000, cap, REFRESH_PATIENCE - 1));
        assert!(!yields_to_the_counter(100_000, 83_000, cap, REFRESH_PATIENCE - 1));
        // А когда терпение исчерпано — не уступаем ни при каких числах.
        assert!(!yields_to_the_counter(999_999, 83_000, cap, REFRESH_PATIENCE));

        // Потолок уже взят — уступать нечему, сбрасываем.
        assert!(
            yields_to_the_counter(cap, 83_000, cap, 0),
            "остаток ноль — счётчик вот-вот сработает"
        );
    }

    /// Запрос по полю `noindex` — отказ, а не пустая выдача.
    ///
    /// Пустая выдача значит «в данных такого нет», и получивший её пойдёт
    /// искать ошибку в данных, а не в схеме. Разница обходится в дни.
    ///
    /// Обход берёт **всякое** упоминание поля: слово, отбор, сортировку,
    /// агрегацию, вектор, любую ветвь булева дерева. Проверка в месте разбора
    /// потребовала бы тринадцати одинаковых вставок, и четырнадцатая забылась
    /// бы.
    #[test]
    fn a_noindex_field_refuses_instead_of_answering_nothing() {
        let names: Vec<String> = ["текст", "сырое"].iter().map(|s| s.to_string()).collect();
        let searched = vec![true, false];
        let refused = |body: &str| {
            let v: serde_json::Value = serde_json::from_str(body).expect("тело");
            refuse_unsearchable(&v, &names, &searched)
        };

        for body in [
            r#"{"words":[{"field":"сырое","terms":["а"]}]}"#,
            r#"{"words":[{"field":1,"terms":["а"]}]}"#,
            r#"{"filter":[{"field":"сырое","from":1}]}"#,
            r#"{"sort":[{"field":"сырое","order":"desc"}]}"#,
            r#"{"aggs":[{"field":"сырое","kind":"count"}]}"#,
            r#"{"vector":{"field":"сырое","query":[1,2]}}"#,
            r#"{"query":{"must":[{"term":{"field":"сырое","terms":["а"]}}]}}"#,
            r#"{"query":{"should":[{"must_not":[{"term":{"field":1,"terms":["а"]}}]}]}}"#,
        ] {
            let e = refused(body).err().unwrap_or_else(|| panic!("принят: {body}"));
            assert!(e.contains("noindex"), "{body}: {e}");
        }

        // А по обычному полю — как прежде, и `fields` в выдаче не при чём:
        // `noindex` заводят ровно ради того, чтобы содержимое возвращать.
        for body in [
            r#"{"words":[{"field":"текст","terms":["а"]}]}"#,
            r#"{"words":[{"field":0,"terms":["а"]}],"fields":["сырое"]}"#,
            r#"{"words":[{"field":"текст","terms":["а"]}],"ids":true}"#,
        ] {
            assert!(refused(body).is_ok(), "{body} отвергнут зря");
        }

        // Без единого выключенного поля обход не тратится вовсе.
        assert!(refuse_unsearchable(
            &serde_json::json!({"words":[{"field":1,"terms":["а"]}]}),
            &names,
            &[true, true]
        )
        .is_ok());
    }

    /// Курсор с номером за потолком — отказ, а не молчаливое обнуление.
    ///
    /// Номер документа тридцатидвухбитный, и `as u32` заворачивал: курсор
    /// 4294967296 превращался в ноль, и клиент, ничего не заметив, начинал
    /// листать с первой страницы заново. Достижимо одним запросом, безо всяких
    /// четырёх миллиардов документов в индексе.
    #[test]
    fn a_cursor_beyond_the_ceiling_is_refused() {
        let ok = r#"{"k":5,"words":[{"field":0,"terms":["a"]}],"search_after":[1.5,7]}"#;
        assert_eq!(
            parse_query(ok.as_bytes(), &crate::two(), &[Kind::Text; 2], &[], &[])
                .unwrap()
                .after
                .unwrap()
                .1,
            7
        );

        let over =
            r#"{"k":5,"words":[{"field":0,"terms":["a"]}],"search_after":[1.5,9007199254740992]}"#;
        let e = parse_query(over.as_bytes(), &crate::two(), &[Kind::Text; 2], &[], &[])
            .err()
            .expect("курсор за потолком принят");
        assert!(e.contains("номер документа"), "получили: {e}");

        // Потолок — `2^53 − 1`, а не `u32::MAX`: общий номер стал
        // восьмибайтным, и граница теперь у формата JSON, где целое перестаёт
        // быть представимым точно. Прежний край (4 294 967 295) законен.
        let edge =
            r#"{"k":5,"words":[{"field":0,"terms":["a"]}],"search_after":[1.5,9007199254740991]}"#;
        assert_eq!(
            parse_query(edge.as_bytes(), &crate::two(), &[Kind::Text; 2], &[], &[])
                .unwrap()
                .after
                .unwrap()
                .1,
            svindb_core::segments::MAX_DOCS
        );
        let was = r#"{"k":5,"words":[{"field":0,"terms":["a"]}],"search_after":[1.5,4294967296]}"#;
        assert!(
            parse_query(was.as_bytes(), &crate::two(), &[Kind::Text; 2], &[], &[]).is_ok(),
            "прежний потолок обязан быть законным номером"
        );
    }

    fn spec(s: &str) -> Vec<svindb_core::schema::Decl> {
        parse_fields(s).expect("объявление не разобралось")
    }

    #[test]
    fn field_spec_parses() {
        let (names, boosts, _kinds, _stored, searched, _pos) = spread(&spec("title:2,body:1"));
        assert_eq!(names, vec!["title", "body"]);
        assert_eq!(boosts, vec![2.0, 1.0]);
        assert_eq!(searched, vec![true, true], "по умолчанию ищутся все");
        let (names, boosts, ..) = spread(&spec(""));
        assert_eq!(names, default_fields().0);
        assert_eq!(boosts, default_fields().1);

        // Два признака в одном месте, и они не одно и то же: `nostore` не
        // хранит, но ищет; `noindex` хранит, но не ищет.
        let (n, _, k, st, ix, _) =
            spread(&spec("a:text:nostore,b:text:noindex,c:int64:noindex,d:text"));
        assert_eq!(n, vec!["a", "b", "c", "d"]);
        assert_eq!(k, vec![Kind::Text, Kind::Text, Kind::Int64, Kind::Text]);
        assert_eq!(st, vec![false, true, true, true], "nostore не хранит");
        assert_eq!(ix, vec![true, false, false, true], "noindex не ищет");
    }

    /// Elasticsearch-mapping принимается только там, где его смысл реально
    /// исполняется. Иначе удачный CREATE оставляет клиенту правдоподобную, но
    /// ложную схему — хуже явного отказа при развёртывании.
    #[test]
    fn index_creation_never_ignores_mapping_semantics() {
        let (native, _) =
            parse_index_creation(br#"{"fields":"title:2,exact:keyword,views:int64"}"#, "")
                .expect("native source-backed схема");
        assert!(
            native.iter().filter(|decl| !internal_field(&decl.path)).all(|decl| !decl.store),
            "новый native индекс не должен дублировать листья поверх lossless source"
        );
        assert!(
            native.iter().filter(|decl| internal_field(&decl.path)).all(|decl| decl.store),
            "внутренний source обязан храниться"
        );

        let good = br#"{
            "mappings":{"dynamic":"strict","properties":{
                "message":{"type":"text","index":true,"boost":2.0,"store":false},
                "price":{"type":"double"},
                "embedding":{"type":"dense_vector","dims":384}
            }}
        }"#;
        let (decls, _) = parse_index_creation(good, "").expect("поддержанная схема");
        assert_eq!(decls.iter().filter(|decl| !internal_field(&decl.path)).count(), 3);
        assert!(decls.iter().any(|decl| decl.path == nested::SOURCE_FIELD));
        assert!(decls.iter().any(|decl| decl.path == nested::PROJECTION_FIELD));
        let embedding = decls.iter().find(|decl| decl.path == "embedding").expect("embedding");
        assert_eq!(embedding.kind, Kind::Dense(384));

        let disabled = br#"{
            "mappings":{"dynamic":"strict","_source":{"enabled":false},"properties":{
                "body":{"type":"text","index":true,"store":false},
                "label":{"type":"keyword","index":true,"store":true}
            }}
        }"#;
        let (decls, _) = parse_index_creation(disabled, "").expect("source-disabled схема");
        assert!(
            decls.iter().all(|decl| !internal_field(&decl.path)),
            "source-disabled индекс не должен получать скрытую source/projection копию"
        );
        assert!(!decls.iter().find(|decl| decl.path == "body").unwrap().store);
        assert!(decls.iter().find(|decl| decl.path == "label").unwrap().store);

        let disabled_setting = br#"{
            "settings":{"index":{"mapping":{"source":{"mode":"disabled"}}}},
            "mappings":{"dynamic":"strict","properties":{"body":{"type":"text"}}}
        }"#;
        let (decls, _) =
            parse_index_creation(disabled_setting, "").expect("source mode в settings");
        assert!(decls.iter().all(|decl| !internal_field(&decl.path)));

        for (label, body) in [
            (
                "анализатор",
                br#"{"mappings":{"dynamic":"strict","properties":{"body":{"type":"text","analyzer":"russian"}}}}"#
                    .as_slice(),
            ),
            (
                "формат даты",
                br#"{"mappings":{"dynamic":"strict","properties":{"ts":{"type":"date","format":"epoch_second"}}}}"#
                    .as_slice(),
            ),
            (
                "чужая точность числа",
                br#"{"mappings":{"dynamic":"strict","properties":{"rank":{"type":"half_float"}}}}"#.as_slice(),
            ),
            (
                "неверный тип store",
                br#"{"mappings":{"dynamic":"strict","properties":{"body":{"type":"text","store":"yes"}}}}"#.as_slice(),
            ),
            (
                "повторённый type",
                br#"{"mappings":{"dynamic":"strict","properties":{"body":{"type":"text","type":"keyword"}}}}"#
                    .as_slice(),
            ),
            (
                "synthetic source без реализации",
                br#"{"mappings":{"dynamic":"strict","_source":{"mode":"synthetic"},"properties":{"body":{"type":"text"}}}}"#
                    .as_slice(),
            ),
            (
                "source filtering без реализации",
                br#"{"mappings":{"dynamic":"strict","_source":{"enabled":false,"includes":["body"]},"properties":{"body":{"type":"text"}}}}"#
                    .as_slice(),
            ),
        ] {
            assert!(parse_index_creation(body, "").is_err(), "{label} был молча принят");
        }
    }

    #[test]
    fn alias_signature_normalizes_equivalent_source_mode_forms() {
        let base =
            std::env::temp_dir().join(format!("svindb-source-signature-{}", std::process::id()));
        let left = base.join("left");
        let right = base.join("right");
        std::fs::remove_dir_all(&base).ok();
        std::fs::create_dir_all(&left).unwrap();
        std::fs::create_dir_all(&right).unwrap();
        for dir in [&left, &right] {
            std::fs::write(dir.join(svindb_core::schema::FILE), b"same-schema").unwrap();
        }
        std::fs::write(
            left.join(mapping::FILE),
            br#"{"mappings":{"dynamic":"strict","_source":{"enabled":false},"properties":{"body":{"type":"text"}}}}"#,
        )
        .unwrap();
        std::fs::write(
            right.join(mapping::FILE),
            br#"{"settings":{"index":{"mapping":{"source":{"mode":"disabled"}}}},"mappings":{"dynamic":"strict","properties":{"body":{"type":"text"}}}}"#,
        )
        .unwrap();
        let a = search_schema_signature(left.to_str().unwrap()).unwrap();
        let b = search_schema_signature(right.to_str().unwrap()).unwrap();
        assert_eq!(a, b, "эквивалентные source modes сделали alias несовместимым");
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// Метки отделяются с конца, и размерность вектора при этом цела.
    ///
    /// Прежде отрезался ровно один хвост после второго двоеточия, и разбор
    /// держался на удаче: `dense_vector:768` уцелел лишь потому, что `768` не
    /// совпало ни с одной меткой. Стоило бы завести метку с именем из цифр — и
    /// вектор молча стал бы текстом.
    #[test]
    fn labels_are_taken_from_the_end_and_the_vector_size_survives() {
        let d = spec("v:dense_vector:768");
        assert_eq!(d[0].kind, Kind::Dense(768), "размерность съедена разбором меток");
        assert!(!d[0].store, "вектор не хранится");

        // Две метки разом — и тип по-прежнему цел.
        let d = spec("t:text:nostore:noindex");
        assert_eq!(d[0].kind, Kind::Text);
        assert!(!d[0].store && !d[0].index);

        let d = spec("v:dense_vector:8:noindex");
        assert_eq!(d[0].kind, Kind::Dense(8), "размерность съедена вместе с меткой");
        assert!(!d[0].index);
    }

    /// `slots` объявляет поле внутри массива, а не молча делает его текстом.
    ///
    /// Прежде этот синтаксис был написан в документации, но не реализован:
    /// `Kind::parse("keyword:slots")` давал `None`, хвост уходил в разбор веса,
    /// тоже неудачно, и поле становилось текстовым с весом единица. Узел
    /// поднимался без единого слова, а точное совпадение превращалось в поиск
    /// по словам.
    #[test]
    fn the_slots_label_declares_an_array_instead_of_quietly_making_text() {
        let d = spec("reactions.emoji:keyword:slots");
        assert_eq!(d[0].kind, Kind::Keyword, "тип подменён");
        assert_eq!(d[0].group, "reactions", "массив не назван");

        // Без метки то же поле остаётся корневым.
        let d = spec("reactions.emoji:keyword");
        assert_eq!(d[0].group, "", "поле без метки уехало в массив");

        // А на пути без точки метка бессмысленна — и это отказ, а не догадка.
        let e = parse_fields("plain:text:slots").expect_err("slots без точки принят");
        assert!(e.contains("нет точки"), "не тот отказ: {e}");
    }

    /// Документ, у которого в каждом массиве ровно один элемент, не уходит
    /// коротким путём.
    ///
    /// ## Чем это грозило
    ///
    /// Короткий путь укладки арности не записывает вовсе — ему нечего
    /// записывать. Смешай его с ячейковым в одном сегменте, и полоса арности
    /// окажется короче числа документов: сегмент обещает полосу, а файлов её
    /// нет, и открыть его нельзя ничем. Узел при этом поднимается, отвечает
    /// «ноль документов» и повторяет отказ пересмотра каталога.
    ///
    /// Проявляется это не на первом документе, а на первом сегменте, где
    /// сошлись оба вида, — то есть на настоящем корпусе, а не на игрушечном.
    /// Так и нашлось: на десяти миллионах.
    #[test]
    fn a_document_with_exactly_one_element_per_array_still_takes_the_cells_path() {
        let dir = std::env::temp_dir().join("svindb-writer-arity-mix");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();

        let decls = parse_fields("t:2,r.emoji:keyword:slots").expect("объявление");
        let declared = svindb_core::schema::declare_all(&dir, &decls).expect("схема");
        let (fields, boosts, kinds, stored, searched, _) = spread(&decls);
        let mut ix = Indexer::typed_stored(&dir, fields, boosts, kinds, stored, 1_000_000);
        ix.set_searched(searched);
        ix.set_fids(declared.fids);
        ix.set_groups(declared.groups, declared.group_paths);

        // Три документа подряд: два элемента, один элемент, ноль элементов.
        // Средний и есть тот, что прежде уходил коротким путём.
        let plan = nested::Plan::build(
            ix.field_names(),
            ix.field_kinds(),
            ix.field_groups(),
            ix.group_paths(),
        );
        let bodies = [
            r#"{"id":"два","t":"д","r":[{"emoji":"up"},{"emoji":"fire"}]}"#,
            r#"{"id":"один","t":"д","r":[{"emoji":"up"}]}"#,
            r#"{"id":"ноль","t":"д","r":[]}"#,
        ];
        for (at, body) in bodies.iter().enumerate() {
            let v: serde_json::Value = serde_json::from_str(body).expect("тело");
            let mut r = intake::Report::default();
            let doc = plan.take(&v, at, &mut r).expect("документ");
            // Средний документ выглядит плоским — и это ровно тот случай.
            if at == 1 {
                assert!(
                    doc.flat(2).is_some(),
                    "документ с одним элементом перестал выглядеть плоским —                      проверять нечего"
                );
            }
            nested::put_doc(&mut ix, &doc, false);
        }
        ix.flush();

        // Сегмент обязан открыться: полоса арности на месте и той длины.
        let seg = format!("{dir}/seg0000");
        let opened = svindb_core::index::load(&seg)
            .unwrap_or_else(|e| panic!("сегмент не открылся — полоса арности разошлась: {e}"));
        assert_eq!(opened.docs, 3);
        let arity: Vec<u32> = (0..3).map(|d| opened.slots[1].range(d).1).collect();
        assert_eq!(arity, vec![2, 1, 0], "арность записана не та");
        assert_eq!(svindb_core::check::check(&seg, svindb_core::check::Depth::Full).broken(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Примеры из документации скармливаются настоящим разборщикам.
    ///
    /// ## Почему тест, а не вычитка
    ///
    /// Документация уже обещала синтаксис, которого не было: `slots` в схеме не
    /// разбирался и молча делал поле текстовым, а путь с точкой ронял узел. Оба
    /// файла при этом читались людьми и правились — и всё равно разошлись с
    /// кодом. Вычитка ловит опечатку, но не ловит того, что пример перестал
    /// работать; это ловит только исполнение.
    ///
    /// Тест берёт из `docs/SCHEMAS.md` объявления полей, а из `docs/API.md` —
    /// тела запросов, и прогоняет их через `parse_fields` и разбор запроса.
    #[test]
    fn the_examples_in_the_docs_are_fed_to_the_real_parsers() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("корень дерева");

        // Объявления полей: строки вида `имя:тип[:метка]` внутри блоков `text`.
        let schemas = std::fs::read_to_string(root.join("docs/SCHEMAS.md"))
            .expect("docs/SCHEMAS.md читается");
        let mut checked = 0usize;
        let mut inside = false;
        for line in schemas.lines() {
            if line.starts_with("```") {
                inside = line.starts_with("```text");
                continue;
            }
            let t = line.trim();
            if !inside || t.is_empty() || !t.contains(':') || t.contains(' ') {
                continue;
            }
            parse_fields(t).unwrap_or_else(|e| {
                panic!("docs/SCHEMAS.md обещает объявление {t:?}, а разбор его не берёт: {e}")
            });
            checked += 1;
        }
        assert!(checked >= 4, "объявлений в документации нашлось {checked} — проверять нечего");

        // Тела запросов: блоки `json`, начинающиеся с `{"query"` или `{"k"`.
        let api = std::fs::read_to_string(root.join("docs/API.md")).expect("docs/API.md читается");
        let names: Vec<String> =
            ["reactions.emoji", "reactions.count", "reactions.hidden", "message", "views", "ts"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        let kinds =
            [Kind::Keyword, Kind::Int32, Kind::Keyword, Kind::Text, Kind::Int64, Kind::Int64];
        let groups = [("reactions".to_string(), 1u32)];
        let slot_groups = [(0usize, 1u32), (1usize, 1u32), (2usize, 1u32)];

        let mut bodies = 0usize;
        let mut block = String::new();
        let mut inside = false;
        for line in api.lines() {
            if line.starts_with("```") {
                if inside && !block.trim().is_empty() {
                    let body = block.trim().to_string();
                    // Векторный поиск разбирается своим путём (`serve_knn`), и
                    // прогонять его тела через булев разбор бессмысленно:
                    // отказ пришёл бы не от документации, а от того, что
                    // проверяющий взял не тот разборщик.
                    let boolean = !body.contains("\"vector\"") && !body.contains("\"sparse\"");
                    if boolean
                        && body.starts_with('{')
                        && (body.contains("\"query\"") || body.contains("\"k\""))
                    {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                            // Разбор обязан либо принять пример, либо отказать
                            // по причине, к самому примеру не относящейся.
                            // Молчаливое непонимание — это и есть беда, которую
                            // тест ловит.
                            if let Err(e) =
                                parse_query_value(&v, &names, &kinds, &groups, &slot_groups)
                            {
                                // Поля, которых нет в этом наборе, — не вина
                                // документации.
                                assert!(
                                    e.contains("поля") || e.contains("field"),
                                    "docs/API.md обещает запрос, который разбор не берёт: \
                                     {e}\n{body}"
                                );
                            }
                            bodies += 1;
                        }
                    }
                }
                block.clear();
                inside = line.starts_with("```json");
                continue;
            }
            if inside {
                block.push_str(line);
                block.push('\n');
            }
        }
        assert!(bodies >= 2, "тел запросов в документации нашлось {bodies} — проверять нечего");
    }

    /// Неизвестный ключ запроса — отказ, а не молчаливое «как будто его нет».
    ///
    /// Самое частое написание пришедшего из Elasticsearch — `gte` вместо
    /// `from`. Прежде разбор его не видел, отбор оставался без границ и
    /// вырождался в «у поля есть значение», а ответ приходил двумястами. То
    /// есть опечатка выглядела ровно как её отсутствие.
    #[test]
    fn an_unknown_query_key_is_named_not_quietly_dropped() {
        let names = vec!["тело".to_string(), "просмотры".to_string()];
        let kinds = [Kind::Text, Kind::Int64];
        let q = |body: &str| parse_query(body.as_bytes(), &names, &kinds, &[], &[]).map(|_| ());

        for (body, key) in [
            (r#"{"k":5,"filter":[{"field":"просмотры","gte":1000}]}"#, "gte"),
            (r#"{"k":5,"words":[{"field":"тело","terms":["а"],"boots":2}]}"#, "boots"),
            (
                r#"{"k":5,"words":[{"field":"тело","terms":["а"]}],"sort":[{"field":"просмотры","ordre":"desc"}]}"#,
                "ordre",
            ),
            (r#"{"k":5,"words":[{"field":"тело","terms":["а"]}],"trackTotal":true}"#, "trackTotal"),
        ] {
            let e = q(body).err().unwrap_or_else(|| panic!("ключ {key} проглочен: {body}"));
            assert!(e.contains(key), "отказ не назвал ключ {key}: {e}");
        }

        // Законное написание по-прежнему разбирается — иначе проверка выше
        // прошла бы и у разбора, отвергающего всякий запрос.
        q(r#"{"k":5,"filter":[{"field":"просмотры","from":1000,"to":9999}]}"#)
            .expect("законный отбор отвергнут");
        q(r#"{"k":5,"words":[{"field":"тело","terms":["а"],"boost":2}]}"#)
            .expect("вес отвергнут напрасно");

        // И служебные ключи координатора не отвергаются: забыть их значило бы
        // сломать веер, а сломался бы он только на нескольких шардах.
        q(r#"{"k":5,"words":[{"field":"тело","terms":["а"]}],"scoring":"global","partial":true,"_stats_only":true,"_global_stats":{}}"#)
            .expect("служебные ключи координатора отвергнуты");
    }

    #[test]
    fn track_total_hits_has_explicit_omit_exact_and_threshold_modes() {
        let names = vec!["тело".to_string()];
        let kinds = [Kind::Text];
        let parse = |body: &str| parse_query(body.as_bytes(), &names, &kinds, &[], &[]).unwrap();
        assert_eq!(
            parse(r#"{"words":[{"field":"тело","terms":["слово"]}]}"#).track_total_hits,
            svindb_core::segments::TrackTotalHits::Omit
        );
        assert_eq!(
            parse(r#"{"track_total_hits":false,"words":[{"field":"тело","terms":["слово"]}]}"#)
                .track_total_hits,
            svindb_core::segments::TrackTotalHits::Omit
        );
        assert_eq!(
            parse(r#"{"track_total_hits":true,"words":[{"field":"тело","terms":["слово"]}]}"#)
                .track_total_hits,
            svindb_core::segments::TrackTotalHits::Exact
        );
        assert_eq!(
            parse(r#"{"track_total_hits":1000,"words":[{"field":"тело","terms":["слово"]}]}"#)
                .track_total_hits,
            svindb_core::segments::TrackTotalHits::UpTo(1000)
        );
        for body in [
            r#"{"track_total_hits":0.5,"words":[{"field":"тело","terms":["слово"]}]}"#,
            r#"{"track_total_hits":"true","words":[{"field":"тело","terms":["слово"]}]}"#,
        ] {
            assert!(parse_query(body.as_bytes(), &names, &kinds, &[], &[]).is_err());
        }
    }

    /// По ключевому полю нельзя считать числа и нельзя сортировать.
    ///
    /// В его колонке лежит номер слова в словаре **этого** сегмента: у соседнего
    /// тот же номер значит другое слово. Наименьшее, наибольшее, сумма, корзины
    /// и порядок по таким номерам — правдоподобная бессмыслица, и считались они
    /// молча. По тексту же колонки нет вовсе: все ключи выходили «значения нет»,
    /// порядок был произвольным, отказа не было ни одного.
    #[test]
    fn numbers_and_order_are_refused_on_a_keyword_and_on_a_text_field() {
        let names = vec!["метка".to_string(), "тело".to_string(), "цена".to_string()];
        let kinds = [Kind::Keyword, Kind::Text, Kind::Int64];
        let agg = |body: &str| -> Result<Vec<svindb_core::aggs::Agg>, String> {
            let v: serde_json::Value = serde_json::from_str(body).expect("тело");
            parse_aggs(&v, &names, &kinds, &[], &[])
        };
        for (body, why) in [
            (r#"{"aggs":{"n":{"stats":{"field":"метка"}}}}"#, "ключевое слово"),
            (r#"{"aggs":{"n":{"histogram":{"field":"метка","interval":1}}}}"#, "ключевое слово"),
            (
                r#"{"aggs":{"n":{"range":{"field":"метка","ranges":[{"from":0}]}}}}"#,
                "ключевое слово",
            ),
        ] {
            let e = agg(body).expect_err("счёт по ключевому полю прошёл");
            assert!(e.contains(why), "не тот отказ: {e}");
        }
        // А `terms` и `cardinality` по нему законны: они считают различность, и
        // номера для этого годятся. Без этой половины проверка выше прошла бы и
        // у запрета, отвергающего всякий счёт по ключевому полю.
        agg(r#"{"aggs":{"n":{"terms":{"field":"метка"}}}}"#).expect("terms отвергнут напрасно");
        agg(r#"{"aggs":{"n":{"cardinality":{"field":"метка"}}}}"#)
            .expect("cardinality отвергнут напрасно");
        let exact = agg(r#"{"aggs":{"n":{"cardinality":{"field":"метка","mode":"exact"}}}}"#)
            .expect("exact cardinality отвергнут");
        assert!(matches!(exact[0].kind, svindb_core::aggs::Kind::CardinalityExact { .. }));
        assert!(
            agg(r#"{"aggs":{"n":{"cardinality":{"field":"метка","mode":"maybe"}}}}"#).is_err(),
            "неизвестный режим мощности принят"
        );
        // И по числовому полю считается всё.
        agg(r#"{"aggs":{"n":{"stats":{"field":"цена"}}}}"#).expect("счёт по числу отвергнут");

        // Сортировка: по ключевому и по тексту — отказ, по числу — проходит.
        let q = |body: &str| parse_query(body.as_bytes(), &names, &kinds, &[], &[]).map(|_| ());
        let e = q(r#"{"k":5,"words":[{"field":"тело","terms":["а"]}],"sort":[{"field":"метка"}]}"#)
            .err()
            .unwrap_or_else(|| panic!("сортировка по ключевому полю принята"));
        assert!(e.contains("словаре сегмента"), "не тот отказ: {e}");
        let e = q(r#"{"k":5,"words":[{"field":"тело","terms":["а"]}],"sort":[{"field":"тело"}]}"#)
            .err()
            .unwrap_or_else(|| panic!("сортировка по тексту принята"));
        assert!(e.contains("колонки значений"), "не тот отказ: {e}");
        q(r#"{"k":5,"words":[{"field":"тело","terms":["а"]}],"sort":[{"field":"цена"}]}"#)
            .expect("сортировка по числу отвергнута напрасно");

        // Отбор с границами по ключевому полю — отказ; без границ (наличие) —
        // законен.
        let e = q(r#"{"k":5,"filter":[{"field":"метка","from":1,"to":2}]}"#)
            .err()
            .unwrap_or_else(|| panic!("границы по ключевому полю приняты"));
        assert!(e.contains("ключевое слово"), "не тот отказ: {e}");
        q(r#"{"k":5,"filter":[{"field":"метка"}]}"#).expect("наличие отвергнуто напрасно");
    }

    /// Фраза без поля — отказ, а не молчаливое поле ноль.
    ///
    /// Прежде отсутствие `field` означало нулевое поле. Клиент при этом не
    /// знает, какое поле нулевое: порядок задаётся объявлением, а не запросом.
    /// То есть фраза искалась не там, где спрошено, и ответ был правдоподобным.
    #[test]
    fn a_phrase_without_a_field_is_refused_not_taken_for_field_zero() {
        let names = vec!["title".to_string(), "body".to_string()];
        let kinds = [Kind::Text, Kind::Text];
        let e = parse_query(
            r#"{"k":5,"phrases":[{"terms":["а","б"]}]}"#.as_bytes(),
            &names,
            &kinds,
            &[],
            &[],
        )
        .err()
        .unwrap_or_else(|| panic!("фраза без поля принята"));
        assert!(e.contains("phrases[0]") && e.contains("field"), "не тот отказ: {e}");

        // С полем — разбирается. Без этой половины проверка выше прошла бы и у
        // разбора, который отвергает всякую фразу.
        parse_query(
            r#"{"k":5,"phrases":[{"field":"body","terms":["а","б"]}]}"#.as_bytes(),
            &names,
            &kinds,
            &[],
            &[],
        )
        .map(|_| ())
        .expect("фраза с полем отвергнута напрасно");
    }

    /// Поле внутри массива, прочитанное номером документа, — отказ.
    ///
    /// Это самая опасная из поломок, которые здесь чинились: ответ выходил
    /// правдоподобным. Числа на месте, порядок осмысленный, а значения — из
    /// чужих ячеек, потому что колонка группового поля адресуется ординалом
    /// элемента. Проверено живьём: отбор по счётчику реакций находил один
    /// документ из двух подходящих.
    #[test]
    fn reading_an_array_field_by_document_number_is_refused_by_name() {
        let names = vec!["text".to_string(), "reactions.count".to_string()];
        let slot_groups = [(0usize, 0u32), (1usize, 1u32)];
        let groups = [("reactions".to_string(), 1u32)];

        // Групповое поле без `over` — отказ, и отказ называет массив и способ.
        let e = refuse_slot_misuse(1, "filter[0]", &names, &slot_groups, &groups, None)
            .expect_err("групповое поле прочлось номером документа");
        assert!(e.contains("reactions"), "отказ не назвал массив: {e}");
        assert!(e.contains("over") && e.contains("slots"), "отказ не назвал способ: {e}");

        // С правильным `over` — проходит. Без этой половины проверка выше
        // прошла бы и у запрета, отвергающего всякое групповое поле.
        refuse_slot_misuse(1, "n", &names, &slot_groups, &groups, Some(1))
            .expect("верный over отвергнут");

        // Корневое поле без `over` — проходит; с `over` — отказ: читать корневую
        // колонку ординалом так же неверно, и прежде это не проверялось ничем.
        refuse_slot_misuse(0, "n", &names, &slot_groups, &groups, None)
            .expect("корневое поле отвергнуто напрасно");
        let e = refuse_slot_misuse(0, "n", &names, &slot_groups, &groups, Some(1))
            .expect_err("over на корневом поле принят");
        assert!(e.contains("корне"), "не тот отказ: {e}");

        // `over` не той группы — тоже отказ.
        let groups2 = [("reactions".to_string(), 1u32), ("tags".to_string(), 2u32)];
        let e = refuse_slot_misuse(1, "n", &names, &slot_groups, &groups2, Some(2))
            .expect_err("over чужой группы принят");
        assert!(e.contains("tags") && e.contains("reactions"), "отказ не назвал обе: {e}");
    }

    /// Схема, прочитанная из реестра, несёт группу — иначе индекс с массивом
    /// не переживёт перезапуск.
    ///
    /// Смена группы у объявленного поля запрещена навсегда. Значит стоит
    /// `schema_of` потерять группу — и повторное объявление на старте
    /// потребует нуля, получит отказ, и узел выйдет с кодом единица. Прежде
    /// так и было: группа отбрасывалась при чтении реестра.
    #[test]
    fn the_schema_read_back_from_the_registry_keeps_its_arrays() {
        let dir = std::env::temp_dir().join("svindb-schema-of-groups");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();

        let decls =
            parse_fields("text:2,reactions.emoji:keyword:slots,reactions.count:int32:slots")
                .expect("объявление");
        svindb_core::schema::declare_all(&dir, &decls).expect("первое объявление");

        let back = schema_of(&dir).expect("реестр прочитан");
        assert_eq!(
            back.iter().map(|d| d.group.as_str()).collect::<Vec<_>>(),
            vec!["", "reactions", "reactions"],
            "группа потеряна при чтении реестра"
        );
        // И повторное объявление прочитанным тождественно первому: ровно это
        // и делает узел на каждом старте.
        svindb_core::schema::declare_all(&dir, &back).expect("повторное объявление отвергнуто");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Негодный хвост — отказ с перечнем того, что бывает.
    #[test]
    fn an_unknown_tail_is_named_not_silently_taken_for_a_boost() {
        let e = parse_fields("a:tekst").expect_err("опечатка в типе принята");
        assert!(e.contains("ни тип, ни вес"), "не тот отказ: {e}");
        // А вес по-прежнему принимается: без этого проверка выше запрещала бы
        // законное написание.
        assert_eq!(spec("a:2.5")[0].boost, 2.5);
    }

    #[test]
    fn range_aggregation_is_rejected_at_the_parser_budget() {
        let ranges = (0..=svindb_core::aggs::MAX_RANGES)
            .map(|i| format!("{{\"from\":{i},\"to\":{i}}}"))
            .collect::<Vec<_>>()
            .join(",");
        let body: serde_json::Value = serde_json::from_str(&format!(
            "{{\"aggs\":{{\"wide\":{{\"range\":{{\"field\":0,\"ranges\":[{ranges}]}}}}}}}}"
        ))
        .unwrap();
        assert!(parse_aggs(&body, &["n".into()], &[Kind::Int64], &[], &[]).is_err());
    }

    #[test]
    fn inflight_guard_balances_every_early_exit() {
        let counter = AtomicUsize::new(0);
        {
            let _first = Inflight::enter(&counter);
            assert_eq!(counter.load(Ordering::Acquire), 1);
            {
                let _second = Inflight::enter(&counter);
                assert_eq!(counter.load(Ordering::Acquire), 2);
            }
            assert_eq!(counter.load(Ordering::Acquire), 1);
        }
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }

    /// Условие CAS разбирается строго: неоднозначное тело нельзя принять как
    /// безусловную замену, иначе опечатка снимет саму защиту lease.
    #[test]
    fn put_conditions_are_strict_and_normalize_scalars() {
        let body = br#"{
            "documents":[],
            "conditions":[
                {"id":"queue","field":"revision","equals":7},
                {"id":"lease","field":"owner","equals":null},
                {"id":"active","field":"value","equals":true},
                {"id":"queue","field":"revision","equals":7}
            ]
        }"#;
        assert_eq!(
            parse_put_conditions(body).unwrap(),
            vec![
                PutCondition {
                    id: "queue".into(),
                    field: "revision".into(),
                    expected: Some("7".into()),
                },
                PutCondition { id: "lease".into(), field: "owner".into(), expected: None },
                PutCondition {
                    id: "active".into(),
                    field: "value".into(),
                    expected: Some("true".into()),
                },
            ]
        );
        assert!(parse_put_conditions(br#"{"documents":[]}"#).unwrap().is_empty());

        for bad in [
            br#"{"conditions":{}}"#.as_slice(),
            br#"{"conditions":[{"id":"x","field":"r"}]}"#.as_slice(),
            br#"{"conditions":[{"id":"x","field":"r","equals":[]}]}"#.as_slice(),
            br#"{"conditions":[{"id":"x","field":"r","equals":1,"typo":1}]}"#.as_slice(),
            br#"{"conditions":[{"id":"x","field":"r","equals":1},{"id":"x","field":"r","equals":2}]}"#.as_slice(),
        ] {
            assert!(
                parse_put_conditions(bad).is_err(),
                "неоднозначное условие принято: {}",
                String::from_utf8_lossy(bad)
            );
        }
    }
}

/// Замена документа: то, ради чего у писателя вообще есть `/_put`.
#[cfg(test)]
mod upsert_tests {
    use super::*;

    fn writer(name: &str) -> (Arc<Writer>, String) {
        let dir = std::env::temp_dir()
            .join(format!("svindb-upsert-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let live: Live = Arc::new(RwLock::new(Arc::new(Segments::load(&dir).unwrap())));
        let (fields, boosts, kinds) = default_fields();
        let (plan_fields, plan_kinds) = (fields.clone(), kinds.clone());
        // Журнала нет: здесь проверяется порядок действий, а не долговечность.
        // Сброс по трём документам — чтобы он случался посреди пачки.
        let w = Writer {
            poisoned: AtomicBool::new(false),
            wal: None,
            recovered: svindb_core::wal::Recovered::default(),
            dir: dir.clone(),
            live,
            buffered_memory_bytes: AtomicU64::new(0),
            mutation_gate: RwLock::new(()),
            ingest: Mutex::new(Indexer::typed(&dir, fields, boosts, kinds, 3)),
            checkpoint_due: AtomicBool::new(false),
            scratch: Mutex::new(Vec::new()),
            layout: Arc::new(Mutex::new(())),
            checkpoint_gate: Mutex::new(0),
            plan: std::sync::Arc::new(nested::Plan::build(
                &plan_fields,
                &plan_kinds,
                &[],
                &[String::new()],
            )),
            shape: (plan_fields.len(), false),
            lossless: false,
            field_kinds: plan_kinds.clone(),
            field_searched: vec![true; plan_fields.len()],
            applied: Applied::new(0),
            docs: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
            reloads: Arc::new(Reloads::new()),
            metrics: Arc::new(metrics::Metrics::default()),
            initial_landed: 0,
            keys: std::array::from_fn(|_| Mutex::new(())),
            visibility: std::sync::RwLock::new(()),
            visibility_generation: AtomicU64::new(0),
            live_max_docs: AtomicU32::new(svindb_core::settings::Settings::default().live_max_docs),
            live_built_docs: AtomicU32::new(0),
            published: AtomicU64::new(0),
            pending_writes: AtomicUsize::new(0),
            write_queue_highwater: AtomicUsize::new(0),
            compaction_floor: AtomicUsize::new(usize::MAX),
            compaction_pending: AtomicUsize::new(0),
            compaction_pending_bytes: AtomicU64::new(0),
            compaction_completed_sources: AtomicU64::new(0),
            compaction_completed_input_bytes: AtomicU64::new(0),
            compaction_elapsed_micros: AtomicU64::new(0),
            l0_throttled: AtomicU64::new(0),
            l0_rejected: AtomicU64::new(0),
            visible_seq: AtomicU64::new(0),
            ready_seq: AtomicU64::new(0),
            cache_advised_seq: AtomicU64::new(0),
            drop_flushed_cache: AtomicBool::new(false),
            settled_seq: AtomicU64::new(0),
            compaction_schedulable: AtomicUsize::new(0),
            compaction_running: AtomicUsize::new(0),
            compaction_blocked: Mutex::new(Default::default()),
            flush_state: Mutex::new(FlushState::default()),
            flush_done: Condvar::new(),
        };
        (Arc::new(w), dir)
    }

    #[test]
    fn unconditional_upserts_share_wal_commits_without_id_stripe_serialization() {
        let (mut writer, dir) = writer("grouped-upserts");
        let metrics = Arc::clone(&writer.metrics);
        Arc::get_mut(&mut writer).unwrap().wal =
            Some(Wal::open(&dir, 0, 100, Arc::clone(&metrics)).unwrap());
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for at in 0..8 {
                let writer = &writer;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    writer.put(vec![doc(&format!("id-{at}"), &at.to_string())]).unwrap();
                });
            }
        });
        assert_eq!(writer.applied.watermark(), 8);
        let rendered = metrics.render(&metrics::State::default());
        let commits: u64 = rendered
            .lines()
            .find_map(|line| line.strip_prefix("svindb_wal_commits_total "))
            .unwrap()
            .parse()
            .unwrap();
        assert!(commits < 8, "each batch held exclusive stripes through fsync: {commits}");
        writer.flush();
        assert_eq!(bodies(&writer).len(), 8);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn search_memory_floor_does_not_wait_for_ingest_and_updates_after_mutation() {
        let (writer, dir) = writer("memory-floor");
        writer.put(vec![doc("one", "payload")]).unwrap();
        let bytes = writer.buffered_memory_bytes.load(Ordering::Acquire);
        assert!(bytes > 0);
        let _held = writer.ingest.lock().unwrap();
        let segments = writer.live.read().unwrap().clone();
        let started = Instant::now();
        assert!(runtime_memory_floor(Some(&writer), &segments, None) >= bytes);
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(_held);
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn doc(id: &str, body: &str) -> nested::Doc {
        nested::Doc {
            id: id.to_string(),
            has_id: true,
            arity: vec![1],
            cells: vec![(0, 0, "пёс".to_string()), (1, 0, body.to_string())],
        }
    }

    /// Тела всех живых документов со словом «пёс».
    fn bodies(w: &Writer) -> Vec<String> {
        let spec = parse_query(
            r#"{"k":50,"words":[{"field":0,"terms":["пёс"]}]}"#.as_bytes(),
            &crate::two(),
            &[Kind::Text; 2],
            &[],
            &[],
        )
        .expect("запрос");
        let segs = { w.live.read().unwrap().clone() };
        segs.search_with(&spec, 1)
            .hits
            .iter()
            .map(|h| {
                String::from_utf8_lossy(&segs.fetch(h.doc, 1).unwrap_or_default()).into_owned()
            })
            .collect()
    }

    #[test]
    fn push_reports_a_flush_collected_only_inside_republish() {
        let (writer, dir) = writer("late-flush-back");
        let mut ingest = writer.ingest.lock().unwrap();
        ingest.background(writer.layout.clone());
        let landed = writer.push(&mut ingest, |indexer| {
            indexer.add("late", &["пёс", "late"]);
            indexer.seq = 1;
            indexer.flush();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !svindb_core::manifest::listing(&dir).is_some_and(|listed| !listed.is_empty()) {
                assert!(Instant::now() < deadline, "flusher не опубликовал manifest");
                std::thread::yield_now();
            }
            // Manifest уже есть, но владелец не вызывал poll/collect.
            assert!(indexer.flush_inflight());
            assert_eq!(indexer.segments(), 0);
        });
        assert!(landed, "поздняя посадка не вызовет after_flush/checkpoint");
        assert!(!ingest.flush_inflight());
        assert_eq!(ingest.flushed_seq, 1);
        drop(ingest);
        assert_eq!(bodies(&writer), ["late"]);
        writer.after_flush();
        assert_eq!(writer.ready_seq.load(Ordering::Acquire), 1);
        drop(writer);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Переполнение поколения flush не может превращать будущие запросы в
    /// уже выполненные: отказ приходит до запуска фоновой публикации.
    #[test]
    fn an_exhausted_flush_generation_is_refused() {
        let (w, dir) = writer("flush-generation");
        w.flush_state.lock().expect("состояние flush").requested = u64::MAX;
        let error =
            w.request_flush(Duration::ZERO).expect_err("поколение flush завернулось к началу");
        assert!(error.contains("исчерпана"), "не тот отказ: {error}");
        let state = w.flush_state.lock().expect("состояние flush");
        assert_eq!(state.requested, u64::MAX);
        assert!(!state.running, "при отказе всё же запущен работник");
        drop(state);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn disconnect_interrupts_point_snapshot_waiting_for_ingest() {
        let (writer, dir) = writer("point-ingest-cancel");
        let ingest = writer.ingest.lock().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let wire = wire::Wire::plain(server, Duration::from_secs(5));
        let guard = Arc::new(svindb_core::work::Guard::new(5_000));
        let _watch = DisconnectWatch::start(&wire, Arc::clone(&guard)).unwrap();
        let queues: &'static Resources = Box::leak(Box::new(Resources::new(16)));
        std::thread::scope(|scope| {
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                drop(client);
            });
            let began = Instant::now();
            {
                let _permit = queues.enter(ResourceClass::Point).unwrap();
                assert!(DocView::take(
                    &writer.live,
                    Some(&writer),
                    &Declared::default(),
                    &["id".into()],
                    1,
                    true,
                    &guard
                )
                .is_none());
            }
            assert_eq!(guard.reason(), svindb_core::work::Reason::Cancelled);
            assert!(began.elapsed() < Duration::from_millis(250));
            assert_eq!(queues.active(ResourceClass::Point), 0);
            assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
        });
        drop(ingest);
        let fresh = svindb_core::work::Guard::new(1_000);
        assert!(DocView::take(
            &writer.live,
            Some(&writer),
            &Declared::default(),
            &["id".into()],
            1,
            true,
            &fresh
        )
        .is_some());
        drop(writer);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Единственная замена нового документа не должна его потерять.
    #[test]
    fn putting_a_new_document_does_not_delete_it() {
        let (w, dir) = writer("new");
        assert_eq!(w.put(vec![doc("a", "первая")]).unwrap(), (1, 0));
        w.flush();
        assert_eq!(bodies(&w), vec!["первая"], "документ обязан пережить сброс");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Сколько бы раз ни заменяли — живой остаётся одна версия, последняя.
    ///
    /// Проверяется прямо по выдаче, а не по числу погашенного в ответе: гашение
    /// прежних версий отложено до публикации, и на момент ответа их ещё столько
    /// же, сколько было. Число в ответе говорит, сколько погашено **уже**, и
    /// инвариантом «живая одна» больше не является.
    #[test]
    fn repeated_puts_leave_exactly_one_live_version() {
        let (w, dir) = writer("repeat");
        for body in ["первая", "вторая", "третья"] {
            assert_eq!(w.put(vec![doc("a", body)]).unwrap().0, 1, "{body} не принята");
        }
        w.flush();
        assert_eq!(bodies(&w), vec!["третья"]);

        assert_eq!(w.put(vec![doc("a", "четвёртая")]).unwrap().0, 1);
        w.flush();
        assert_eq!(bodies(&w), vec!["четвёртая"]);
        // Гашение прежних версий действительно произошло, а не «нечего было
        // гасить»: счётчик считает сброшенные биты, а не намерения.
        assert!(
            w.deleted.load(Ordering::Relaxed) >= 3,
            "погашено {} прежних версий из трёх",
            w.deleted.load(Ordering::Relaxed)
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Пачка крупнее обычного порога не публикуется наполовину.
    #[test]
    fn a_batch_larger_than_the_segment_threshold_stays_whole() {
        let (w, dir) = writer("batch");
        let batch = vec![
            doc("a", "первая"),
            doc("b", "бэ"),
            doc("c", "цэ"),
            doc("a", "вторая"),
            doc("d", "дэ"),
        ];
        assert_eq!(w.put(batch).unwrap().0, 5);
        w.flush();
        let mut got = bodies(&w);
        got.sort();
        assert_eq!(got, vec!["бэ", "вторая", "дэ", "цэ"], "живых обязано быть четыре");
        assert_eq!(
            manifest::segments(&dir).len(),
            1,
            "атомарная пачка не должна разрезаться порогом на два сегмента"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Удаление документа, ещё лежащего в буфере приёма, работает сразу.
    #[test]
    fn deleting_a_buffered_document_works_at_once() {
        let (w, dir) = writer("buffered");
        w.add(vec![doc("a", "первая")]).unwrap();
        assert_eq!(w.delete(&["a".to_string()]).unwrap(), 1);
        assert_eq!(w.delete(&["a".to_string()]).unwrap(), 0, "дважды не гасится");
        assert_eq!(w.delete(&["нет-такого".to_string()]).unwrap(), 0);
        w.flush();
        assert!(bodies(&w).is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Порядок «обычный приём дважды, потом замена» законен: `_bulk` ничего не
    /// проверяет по построению, и две живые версии там — ответственность
    /// вызывающего. А вот замена обязана свести их к одной.
    #[test]
    fn a_later_put_kills_every_earlier_copy() {
        let (w, dir) = writer("bulk-then-put");
        w.add(vec![doc("a", "первая")]).unwrap();
        w.add(vec![doc("a", "вторая")]).unwrap();
        w.flush();
        assert_eq!(bodies(&w).len(), 2, "приём повторов их не сводит");

        assert_eq!(w.put(vec![doc("a", "третья")]).unwrap().0, 1);
        let before = w.deleted.load(Ordering::Relaxed);
        w.flush();
        assert_eq!(bodies(&w), vec!["третья"]);
        // Обе прежние копии погашены публикацией — по одной на копию.
        assert_eq!(
            w.deleted.load(Ordering::Relaxed) - before,
            2,
            "публикация погасила не обе прежние копии"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn conditional_put_checks_buffer_and_published_segment() {
        let (w, dir) = writer("cas");
        w.put(vec![doc("lease", "1")]).unwrap();
        let first =
            PutCondition { id: "lease".into(), field: "body".into(), expected: Some("1".into()) };
        assert_eq!(
            w.put_if(vec![doc("lease", "2")], std::slice::from_ref(&first))
                .unwrap()
                .map(|(n, _)| n),
            Ok(1)
        );
        assert_eq!(
            w.put_if(vec![doc("lease", "устаревшее")], &[first]).unwrap(),
            Err(PutConflict { id: "lease".into(), field: "body".into() })
        );
        w.flush();

        let second =
            PutCondition { id: "lease".into(), field: "body".into(), expected: Some("2".into()) };
        assert_eq!(w.put_if(vec![doc("lease", "3")], &[second]).unwrap().map(|(n, _)| n), Ok(1));
        w.flush();
        assert_eq!(bodies(&w), vec!["3"], "CAS сверился не с той версией");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn only_one_concurrent_cas_can_renew_the_same_revision() {
        let (w, dir) = writer("cas-race");
        w.put(vec![doc("lease", "0")]).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let won = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for n in 1..=8 {
                let w = Arc::clone(&w);
                let barrier = Arc::clone(&barrier);
                let won = Arc::clone(&won);
                scope.spawn(move || {
                    barrier.wait();
                    let condition = PutCondition {
                        id: "lease".into(),
                        field: "body".into(),
                        expected: Some("0".into()),
                    };
                    if matches!(
                        w.put_if(vec![doc("lease", &n.to_string())], &[condition]),
                        Ok(Ok(_))
                    ) {
                        won.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(won.load(Ordering::Relaxed), 1, "одну revision обновили несколько владельцев");
        w.flush();
        assert_eq!(bodies(&w).len(), 1, "CAS обязан оставить одну живую версию");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_full_write_queue_refuses_instead_of_waiting() {
        let (w, dir) = writer("backpressure");
        let first = w.enter_write(1).expect("первое место");
        assert!(w.enter_write(1).is_none(), "переполненная очередь приняла запрос");
        drop(first);
        assert!(w.enter_write(1).is_some(), "место не вернулось после завершения");
        assert_eq!(w.write_queue_highwater.load(Ordering::Relaxed), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Durable group commit проверяется на настоящем WAL-файле и настоящем
/// `sync_all`, а не счётчиком вокруг заглушки: смысл механизма именно в числе
/// физических фиксаций.
#[cfg(test)]
mod wal_group_commit_tests {
    use super::*;

    #[test]
    fn concurrent_requests_share_one_fsync_inside_the_window() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-wal-group-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let metrics = Arc::new(metrics::Metrics::default());
        let wal = Arc::new(Wal::open(&dir, 0, 50, Arc::clone(&metrics)).unwrap());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            let first = Arc::clone(&wal);
            scope.spawn(move || {
                let mine =
                    first.stage(svindb_core::wal::ADD, &["первый".as_bytes().to_vec()]).unwrap();
                ready_tx.send(()).unwrap();
                first.commit(mine).unwrap();
            });
            ready_rx.recv().unwrap();

            // Первый запрос уже вошёл в окно ожидания либо будет вытеснен
            // одним из этих запросов; в обоих случаях один лидер обязан
            // накрыть общим рубежом все записи.
            for n in 1..8u8 {
                let wal = Arc::clone(&wal);
                scope.spawn(move || {
                    let mine = wal.stage(svindb_core::wal::ADD, &[vec![n]]).unwrap();
                    wal.commit(mine).unwrap();
                });
            }
        });

        let out = metrics.render(&metrics::State::default());
        assert!(out.contains("svindb_wal_commits_total 1"), "{out}");
        assert!(out.contains("svindb_wal_records_total 8"), "{out}");
        assert!(out.contains("svindb_wal_batch_records_count{} 1"), "{out}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Исчерпанная нумерация не добавляет половину пачки и не заворачивается.
    #[test]
    fn an_exhausted_wal_sequence_refuses_the_whole_batch() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-wal-sequence-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let metrics = Arc::new(metrics::Metrics::default());
        let wal = Wal::open(&dir, u64::MAX - 1, 0, metrics).unwrap();
        let error = wal
            .stage(svindb_core::wal::ADD, &[vec![1], vec![2]])
            .expect_err("пачка завернула номера WAL");
        assert!(error.to_string().contains("исчерпана"), "не тот отказ: {error}");
        assert_eq!(wal.seq(), u64::MAX - 1, "отказ всё же сдвинул номер");
        assert!(
            wal.state.lock().expect("журнал").buf.is_empty(),
            "часть отвергнутой пачки осталась в буфере"
        );

        let last = wal
            .stage(svindb_core::wal::ADD, &[vec![3]])
            .expect("последний доступный номер отвергнут");
        assert_eq!(last, u64::MAX);
        assert!(wal.stage(svindb_core::wal::ADD, &[vec![4]]).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Закодированная пачка ограничивается до записи в файл.
    ///
    /// Предел HTTP относится к входному JSON и не учитывает служебные длины
    /// полей WAL. Проверяем именно границу серверного приёмника: отказ не
    /// должен выдать номер, оставить часть буфера или создать файл, который
    /// reader отвергнет после аварийного перезапуска.
    #[test]
    fn an_oversized_encoded_record_is_refused_before_sequence_allocation() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-wal-record-limit-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let metrics = Arc::new(metrics::Metrics::default());
        let wal = Wal::open(&dir, 7, 0, metrics).unwrap();
        let too_large = vec![0u8; svindb_core::wal::MAX_RECORD_BODY - 8];
        let error = wal
            .stage(svindb_core::wal::BATCH, &[too_large])
            .expect_err("запись за потолком WAL принята");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("слишком велика"), "не тот отказ: {error}");
        assert_eq!(wal.seq(), 7, "отказ сдвинул номер WAL");
        assert!(wal.state.lock().expect("журнал").buf.is_empty(), "часть пачки осталась");
        let files = svindb_core::wal::files(&dir);
        assert_eq!(files.len(), 1, "WAL должен иметь только пустой хвост");
        assert_eq!(std::fs::metadata(&files[0]).unwrap().len(), 0, "отказ записал WAL-файл");
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod field_bounds_tests {
    use super::*;

    /// Номер поля вне объявленных обязан быть отказом, а не падением.
    ///
    /// Проверяются **все** места разбора, а не одно: номер приходил четырьмя
    /// путями, и закрытый один оставил бы три. Сборка идёт с `panic = "abort"`,
    /// так что промах здесь убивает узел целиком — один запрос на весь процесс.
    #[test]
    fn a_field_number_out_of_range_is_refused_everywhere() {
        let bad = [
            r#"{"k":1,"words":[{"field":256,"terms":["a"]}]}"#,
            r#"{"k":1,"query":{"must":[{"field":256,"terms":["a"]}]}}"#,
            r#"{"k":1,"query":{"should":[{"field":0,"terms":["a"]}],
                 "must_not":[{"field":9999,"terms":["b"]}]}}"#,
            r#"{"k":1,"phrases":[{"field":7,"terms":["a","b"]}]}"#,
            r#"{"k":1,"words":[{"field":0,"terms":["a"]}],"sort":[{"field":300}]}"#,
            r#"{"k":1,"filter":[{"field":512,"from":1}]}"#,
            r#"{"k":1,"words":[{"field":18446744073709551615,"terms":["a"]}]}"#,
        ];
        for q in bad {
            let r = parse_query(q.as_bytes(), &crate::two(), &[Kind::Text; 2], &[], &[]);
            assert!(r.is_err(), "принят запрос с несуществующим полем: {q}");
            let msg = r.err().expect("отказ");
            assert!(msg.contains("поля"), "запрос {q} отвергнут не за поле: {msg}");
        }
        // А объявленные — принимаются.
        for q in [
            r#"{"k":1,"words":[{"field":0,"terms":["a"]}]}"#,
            r#"{"k":1,"words":[{"field":1,"terms":["a"]}]}"#,
        ] {
            assert!(
                parse_query(q.as_bytes(), &crate::two(), &[Kind::Text; 2], &[], &[]).is_ok(),
                "отвергнуто законное поле: {q}"
            );
        }
    }
}

/// Отказ перезагрузки состава обязан быть виден, а не проглочен.
#[cfg(test)]
mod stale_tests {
    use super::*;

    /// Счётчик считает подряд идущие отказы и сам снимается при удаче.
    ///
    /// Снятие здесь не мелочь: без него первый же разовый сбой — кончились
    /// дескрипторы на секунду — выводил бы узел из ротации навсегда.
    #[test]
    fn the_counter_rises_on_failure_and_clears_itself() {
        let r = Reloads::new();
        assert_eq!(r.failures(), 0);
        assert_eq!(r.last(), "");

        r.failed("приём", "кончились отображения");
        assert_eq!(r.failures(), 1);
        r.failed("пересмотр каталога", "нет такого файла");
        assert_eq!(r.failures(), 2, "отказы обязаны считаться подряд");
        assert!(r.last().contains("нет такого файла"), "причина не запомнена: {}", r.last());

        r.ok();
        assert_eq!(r.failures(), 0, "удача обязана снимать признак");
        assert_eq!(r.last(), "", "причина обязана стираться вместе со счётчиком");
    }

    /// Неоткрываемый индекс отмечается писателем, а не проходит молча.
    ///
    /// Прежде здесь стояло `if let Ok(next) = …`: набор молча замирал, узел
    /// продолжал отвечать устаревшей выдачей и подтверждать приём документов,
    /// которых поиск уже никогда не увидит.
    #[test]
    fn a_writer_marks_a_reload_it_could_not_do() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-stale-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let live: Live = Arc::new(RwLock::new(Arc::new(Segments::load(&dir).unwrap())));
        let (fields, boosts, kinds) = default_fields();
        let (plan_fields, plan_kinds) = (fields.clone(), kinds.clone());
        let w = Writer {
            poisoned: AtomicBool::new(false),
            wal: None,
            recovered: svindb_core::wal::Recovered::default(),
            dir: dir.clone(),
            live,
            buffered_memory_bytes: AtomicU64::new(0),
            mutation_gate: RwLock::new(()),
            ingest: Mutex::new(Indexer::typed(&dir, fields, boosts, kinds, 3)),
            checkpoint_due: AtomicBool::new(false),
            scratch: Mutex::new(Vec::new()),
            layout: Arc::new(Mutex::new(())),
            checkpoint_gate: Mutex::new(0),
            plan: std::sync::Arc::new(nested::Plan::build(
                &plan_fields,
                &plan_kinds,
                &[],
                &[String::new()],
            )),
            shape: (plan_fields.len(), false),
            lossless: false,
            field_kinds: plan_kinds.clone(),
            field_searched: vec![true; plan_fields.len()],
            applied: Applied::new(0),
            docs: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
            reloads: Arc::new(Reloads::new()),
            metrics: Arc::new(metrics::Metrics::default()),
            initial_landed: 0,
            keys: std::array::from_fn(|_| Mutex::new(())),
            visibility: std::sync::RwLock::new(()),
            visibility_generation: AtomicU64::new(0),
            live_max_docs: AtomicU32::new(svindb_core::settings::Settings::default().live_max_docs),
            live_built_docs: AtomicU32::new(0),
            published: AtomicU64::new(0),
            pending_writes: AtomicUsize::new(0),
            write_queue_highwater: AtomicUsize::new(0),
            compaction_floor: AtomicUsize::new(usize::MAX),
            compaction_pending: AtomicUsize::new(0),
            compaction_pending_bytes: AtomicU64::new(0),
            compaction_completed_sources: AtomicU64::new(0),
            compaction_completed_input_bytes: AtomicU64::new(0),
            compaction_elapsed_micros: AtomicU64::new(0),
            l0_throttled: AtomicU64::new(0),
            l0_rejected: AtomicU64::new(0),
            visible_seq: AtomicU64::new(0),
            ready_seq: AtomicU64::new(0),
            cache_advised_seq: AtomicU64::new(0),
            drop_flushed_cache: AtomicBool::new(false),
            settled_seq: AtomicU64::new(0),
            compaction_schedulable: AtomicUsize::new(0),
            compaction_running: AtomicUsize::new(0),
            compaction_blocked: Mutex::new(Default::default()),
            flush_state: Mutex::new(FlushState::default()),
            flush_done: Condvar::new(),
        };

        // Пустой каталог читается: состав пуст, но это законное состояние.
        w.reload();
        assert_eq!(w.reloads.failures(), 0, "пустой индекс — не поломка");

        // Список называет сегмент, которого нет: `Segments::load` обязан
        // отказать, а писатель — это заметить.
        manifest::write(&dir, &["seg0000".to_string()]).unwrap();
        w.reload();
        assert_eq!(w.reloads.failures(), 1, "отказ перезагрузки не замечен");
        assert!(
            w.healthy().is_err(),
            "приём продолжил бы подтверждать документы при устаревшем поисковом составе"
        );
        assert!(
            w.reloads.last().contains("seg0000"),
            "причина не называет пропавший сегмент: {}",
            w.reloads.last()
        );

        // Список починили — признак снимается сам.
        manifest::write(&dir, &[]).unwrap();
        w.reload();
        assert_eq!(w.reloads.failures(), 0, "признак не снялся после удачи");
        assert!(w.healthy().is_ok(), "успешное перечитывание не сняло временный backpressure");

        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[test]
    fn client_disconnect_cancels_server_work_within_250_ms() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let client = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (server, _) = listener.accept().expect("сервер");
        let wire = wire::Wire::plain(server, Duration::from_secs(5));
        let guard = Arc::new(svindb_core::work::Guard::new(5_000));
        let watch = DisconnectWatch::start(&wire, Arc::clone(&guard)).expect("наблюдатель");

        let began = Instant::now();
        drop(client);
        while !guard.fired() && began.elapsed() < Duration::from_millis(250) {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(guard.reason(), svindb_core::work::Reason::Cancelled);
        assert!(
            began.elapsed() < Duration::from_millis(250),
            "отмена заняла {:?}",
            began.elapsed()
        );
        drop(watch);
    }

    #[test]
    fn disconnect_watcher_keeps_the_next_keepalive_read_blocking() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let mut client = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (server, _) = listener.accept().expect("сервер");
        let mut wire = wire::Wire::plain(server, Duration::from_secs(2));
        let guard = Arc::new(svindb_core::work::Guard::new(5_000));
        let watch = DisconnectWatch::start(&wire, Arc::clone(&guard)).expect("наблюдатель");

        // Даём наблюдателю войти в `peek`, затем завершаем текущую работу.
        // Следующий запрос приходит позже: основной reader обязан дождаться
        // его, а не получить `WouldBlock` от режима клонированного дескриптора.
        std::thread::sleep(Duration::from_millis(40));
        drop(watch);
        std::thread::sleep(Duration::from_millis(40));
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            client.write_all(b"x").expect("следующий запрос");
        });
        let began = Instant::now();
        let mut byte = [0u8; 1];
        std::io::Read::read_exact(&mut wire, &mut byte).expect("keep-alive чтение");
        assert_eq!(byte, [b'x']);
        assert!(
            began.elapsed() >= Duration::from_millis(30),
            "reader не дождался отложенного запроса: {:?}",
            began.elapsed()
        );
        sender.join().expect("поток клиента");
        assert!(!guard.fired(), "живой keep-alive ошибочно отменён");
    }

    #[cfg(unix)]
    #[test]
    fn disconnect_monitor_collapses_clones_of_one_keepalive_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let client_one = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (server_one, _) = listener.accept().expect("сервер");
        let clone_one = server_one.try_clone().expect("первый clone");
        let clone_two = server_one.try_clone().expect("второй clone");
        let mut watched = Vec::new();
        upsert_disconnect_socket(
            &mut watched,
            WatchedDisconnectSocket {
                id: 1,
                key: disconnect_socket_key(&clone_one).expect("endpoint первого clone"),
                transport: clone_one,
                slot: Arc::new(Mutex::new(None)),
            },
        );
        upsert_disconnect_socket(
            &mut watched,
            WatchedDisconnectSocket {
                id: 2,
                key: disconnect_socket_key(&clone_two).expect("endpoint второго clone"),
                transport: clone_two,
                slot: Arc::new(Mutex::new(None)),
            },
        );
        assert_eq!(watched.len(), 1, "клоны одного TCP не должны занимать два watcher");
        assert_eq!(watched[0].id, 2, "должен остаться guard последней регистрации");

        let client_two = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (server_two, _) = listener.accept().expect("второй сервер");
        let clone_three = server_two.try_clone().expect("третий clone");
        upsert_disconnect_socket(
            &mut watched,
            WatchedDisconnectSocket {
                id: 3,
                key: disconnect_socket_key(&clone_three).expect("endpoint третьего clone"),
                transport: clone_three,
                slot: Arc::new(Mutex::new(None)),
            },
        );
        assert_eq!(watched.len(), 2, "разные TCP-соединения должны наблюдаться независимо");
        drop((client_one, client_two, server_one, server_two));
    }

    #[cfg(unix)]
    #[test]
    fn disconnect_peek_stays_nonblocking_after_the_reader_consumes_data() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let client = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (mut server, _) = listener.accept().expect("сервер");
        let monitor_socket = server.try_clone().expect("clone monitor");
        let mut writer = client.try_clone().expect("clone client");

        std::io::Write::write_all(&mut writer, b"x").expect("байт");
        let mut byte = [0u8; 1];
        std::io::Read::read_exact(&mut server, &mut byte).expect("основной reader");
        assert_eq!(byte, [b'x']);

        let began = Instant::now();
        let result = disconnect_peek(&monitor_socket, &mut byte);
        assert!(
            result.as_ref().is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock),
            "после чтения monitor не должен зависать или видеть старый байт: {result:?}"
        );
        assert!(began.elapsed() < Duration::from_millis(25), "peek занял {:?}", began.elapsed());
    }

    #[cfg(unix)]
    #[test]
    fn an_incomplete_header_gets_a_short_progress_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let mut client = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (server, _) = listener.accept().expect("сервер");
        let wire = wire::Wire::plain(server, Duration::from_secs(60));

        // Байты уже пришли, но завершающей пустой строки заголовка нет. Важно
        // проверить именно это состояние: полностью idle keep-alive должен
        // оставаться blocking, а начатый slowloris — быстро освобождать worker.
        std::io::Write::write_all(&mut client, b"GET / HTTP/1.1\r\n").expect("строка");
        let mut reader = std::io::BufReader::new(wire);
        renew_connection_header_deadline(&mut reader);
        let began = Instant::now();
        let incoming = http::read_head_from(&mut reader);
        match incoming {
            http::Incoming::Refused(code, why) => {
                assert_eq!((code, why), (408, "заголовки не завершились вовремя"));
            }
            http::Incoming::Done => panic!("неполный заголовок неожиданно стал EOF"),
            http::Incoming::Request(_) => panic!("неполный заголовок неожиданно принят"),
            http::Incoming::RefusedBody(code, why, len) => {
                panic!("неполный заголовок получил отказ тела {code} {why} {len}")
            }
        }
        assert!(
            began.elapsed() < Duration::from_millis(500),
            "worker удерживался после неполного заголовка {:?}",
            began.elapsed()
        );
    }

    #[cfg(unix)]
    #[test]
    fn delayed_incomplete_header_switches_to_progress_deadline_after_first_byte() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let mut client = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (server, _) = listener.accept().expect("сервер");
        let release = std::sync::mpsc::sync_channel(0);
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            std::io::Write::write_all(&mut client, b"GET / HTTP/1.1\r\n").expect("строка");
            release.1.recv().expect("разрешение закрытия");
        });

        let wire = wire::Wire::plain(server, Duration::from_millis(500));
        let mut reader = std::io::BufReader::new(wire);
        renew_connection_header_deadline(&mut reader);
        let began = Instant::now();
        let incoming = http::read_head_from(&mut reader);
        assert!(
            began.elapsed() < Duration::from_millis(250),
            "worker удерживался {:?}",
            began.elapsed()
        );
        assert!(matches!(incoming, http::Incoming::Refused(408, _)));
        release.0.send(()).expect("разрешение клиента");
        sender.join().expect("поток клиента");
    }

    #[test]
    fn buffered_incomplete_header_already_counts_as_progress() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        std::io::Write::write_all(&mut client, b"GET / HTTP/1.1\r\n").unwrap();
        let mut reader = std::io::BufReader::new(wire::Wire::plain(server, Duration::from_secs(1)));
        assert!(!std::io::BufRead::fill_buf(&mut reader).unwrap().is_empty());
        renew_connection_header_deadline(&mut reader);
        let began = Instant::now();
        assert!(matches!(http::read_head_from(&mut reader), http::Incoming::Refused(408, _)));
        assert!(began.elapsed() < Duration::from_millis(250));
    }

    #[cfg(unix)]
    #[test]
    fn delayed_complete_header_keeps_a_legal_idle_connection_working() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let mut client = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        let (server, _) = listener.accept().expect("сервер");
        let release = std::sync::mpsc::sync_channel(0);
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            std::io::Write::write_all(&mut client, b"GET / HTTP/1.1\r\nHost: test\r\n\r\n")
                .expect("полный запрос");
            release.1.recv().expect("разрешение закрытия");
        });

        let wire = wire::Wire::plain(server, Duration::from_millis(500));
        let mut reader = std::io::BufReader::new(wire);
        renew_connection_header_deadline(&mut reader);
        let incoming = http::read_head_from(&mut reader);
        assert!(matches!(incoming, http::Incoming::Request(_)));
        release.0.send(()).expect("разрешение клиента");
        sender.join().expect("поток клиента");
    }

    #[test]
    fn point_read_guard_uses_the_smallest_declared_deadline() {
        let route = route::Route::parse("GET", "/docs/_doc/42?timeout_ms=80");
        let req = http::Request {
            method: "GET".into(),
            path: "/docs/_doc/42?timeout_ms=80".into(),
            body: Vec::new(),
            close: true,
            headers: vec![("x-svindb-timeout-ms".into(), "25".into())],
            body_len: 0,
        };
        let guard = point_guard(&req, &route).expect("point deadline");
        std::thread::sleep(Duration::from_millis(35));
        assert!(!guard.check(), "point deadline не сработал");
        assert_eq!(guard.reason(), svindb_core::work::Reason::Deadline);
    }

    #[test]
    fn point_read_guard_rejects_an_unbounded_timeout_request() {
        let route = route::Route::parse("GET", "/docs/_doc/42?timeout_ms=300001");
        let req = http::Request {
            method: "GET".into(),
            path: "/docs/_doc/42?timeout_ms=300001".into(),
            body: Vec::new(),
            close: true,
            headers: Vec::new(),
            body_len: 0,
        };
        let error = point_guard(&req, &route).expect_err("deadline выше потолка принят");
        assert!(error.contains("серверного потолка"), "неверная причина: {error}");
    }

    #[test]
    fn graceful_shutdown_wakes_an_idle_keepalive_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("тестовый порт");
        let mut client = TcpStream::connect(listener.local_addr().expect("адрес")).expect("клиент");
        client.set_read_timeout(Some(Duration::from_millis(250))).expect("таймаут клиента");
        let (server, _) = listener.accept().expect("сервер");
        let wire = wire::Wire::plain(server, Duration::from_secs(60));
        let connections = Arc::new(ActiveConnections::default());
        let lease = connections.register(&wire).expect("соединение не зарегистрировано");

        let began = Instant::now();
        connections.shutdown_all();
        let mut byte = [0u8; 1];
        let read = std::io::Read::read(&mut client, &mut byte);
        assert!(matches!(read, Ok(0) | Err(_)), "shutdown оставил сокет читаемым: {read:?}");
        assert!(began.elapsed() < Duration::from_millis(250), "keep-alive не разбужен");

        drop(lease);
        assert!(connections.sockets.lock().expect("реестр").is_empty(), "lease протёк");
    }

    #[test]
    fn visibility_generation_is_odd_only_inside_publication() {
        let generation = AtomicU64::new(0);
        {
            let _publication = VisibilityMutation::begin(&generation);
            assert_eq!(generation.load(Ordering::Acquire), 1);
        }
        assert_eq!(generation.load(Ordering::Acquire), 2);
    }

    #[test]
    fn point_reads_use_their_snapshot_instead_of_visibility_barrier() {
        assert!(uses_versioned_read(route::Endpoint::Search));
        assert!(uses_versioned_read(route::Endpoint::DocGet));
        assert!(uses_versioned_read(route::Endpoint::MGet));
        assert!(!uses_versioned_read(route::Endpoint::Inspect));
        assert!(!uses_versioned_read(route::Endpoint::Bulk));
    }

    #[test]
    fn l0_admission_slows_before_it_rejects() {
        assert_eq!(l0_admission(8, 8, 24), Ok(Duration::ZERO));
        assert!(l0_admission(9, 8, 24).expect("мягкий долг отвергнут") > Duration::ZERO);
        assert!(l0_admission(23, 8, 24).is_ok());
        assert!(l0_admission(24, 8, 24).is_err());
    }

    #[test]
    fn inherited_l0_debt_is_grandfathered_but_new_growth_is_not() {
        assert_eq!(l0_growth(1_500, usize::MAX), (1_500, 0));
        assert_eq!(l0_growth(1_500, 1_500), (1_500, 0));
        assert_eq!(l0_growth(1_509, 1_500), (1_500, 9));
        // Compaction опустила минимум: прежний высокий baseline не может
        // навсегда подарить запас для нового роста.
        assert_eq!(l0_growth(1_200, 1_500), (1_200, 0));
        assert_eq!(l0_growth(1_224, 1_200), (1_200, 24));
        assert!(l0_admission(l0_growth(1_224, 1_200).1, 8, 24).is_err());
    }

    #[test]
    fn resource_queues_are_separate_and_release_their_io_units() {
        let queues: &'static Resources = Box::leak(Box::new(Resources::new(4)));
        let merge = queues.enter(ResourceClass::Compaction).expect("первая compaction");
        assert!(queues.enter(ResourceClass::Compaction).is_none(), "вторая compaction прошла");
        let control = queues.enter(ResourceClass::Control).expect("control зависит от I/O");
        drop((merge, control));
        assert!(queues.enter(ResourceClass::Compaction).is_some(), "permit не освободился");
    }

    #[test]
    fn resource_waiters_are_bounded_and_wake_without_polling() {
        let workload = config::Workload { interactive_max_units: 8, ..config::Workload::default() };
        let queues: &'static Resources = Box::leak(Box::new(Resources::new_with(16, &workload)));
        let first = queues.enter(ResourceClass::Interactive).expect("первый search");
        let second = queues.enter(ResourceClass::Interactive).expect("второй search");
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..ResourceClass::Interactive.queue_limit())
                .map(|_| {
                    scope.spawn(|| {
                        let guard = svindb_core::work::Guard::new(1_000);
                        let permit = queues
                            .enter_until(ResourceClass::Interactive, Some(&guard))
                            .expect("bounded waiter должен проснуться после release");
                        drop(permit);
                    })
                })
                .collect();
            let started = Instant::now();
            while queues.waiters[ResourceClass::Interactive as usize].load(Ordering::Acquire)
                < ResourceClass::Interactive.queue_limit()
                && started.elapsed() < Duration::from_secs(1)
            {
                std::thread::yield_now();
            }
            assert_eq!(
                queues.waiters[ResourceClass::Interactive as usize].load(Ordering::Acquire),
                ResourceClass::Interactive.queue_limit(),
                "в очередь попало больше объявленного лимита"
            );
            let guard = svindb_core::work::Guard::new(1_000);
            assert!(
                queues.enter_until(ResourceClass::Interactive, Some(&guard)).is_none(),
                "лишняя continuation прошла ограниченный scheduler"
            );
            drop((first, second));
            for handle in handles {
                handle.join().expect("ожидающий search");
            }
        });
        assert_eq!(queues.active(ResourceClass::Interactive), 0);
        assert_eq!(queues.waiters[ResourceClass::Interactive as usize].load(Ordering::Acquire), 0);
    }

    #[test]
    fn configured_workload_caps_are_used_by_the_governor() {
        let workload = config::Workload {
            search_cpu_units: 6,
            interactive_max_units: 5,
            analytics_max_units: 3,
            latency_reserve_mb: 512,
            wal_replay_mb: 32,
            max_frozen_memtables: 7,
            compaction_policy: "query_first".into(),
            background_io_mbps: 32,
            background_io_burst_mb: 4,
            background_io_max_inflight: 2,
            source_cache_mb: 256,
        };
        let queues = Resources::new_with(16, &workload);
        assert_eq!(queues.cpu_max, 6);
        assert_eq!(queues.interactive_max_units, 5);
        assert_eq!(queues.analytics_max_units, 3);
        assert_eq!(queues.latency_reserve_bytes, 512 << 20);
        assert_eq!(queues.wal_replay_bytes, 32 << 20);
        assert_eq!(queues.max_frozen_memtables, 7);
    }

    #[test]
    fn point_queue_reserves_acceptance_concurrency_above_internal_reads() {
        let queues: &'static Resources = Box::leak(Box::new(Resources::new(16)));
        let permits: Vec<_> = (0..64)
            .map(|_| queues.enter(ResourceClass::Point).expect("point concurrency 64 отвергнут"))
            .collect();
        assert!(queues.enter(ResourceClass::Point).is_none(), "point queue потеряла bound");
        drop(permits);
        assert!(queues.enter(ResourceClass::Point).is_some(), "point permits не освободились");
    }

    #[test]
    fn count_admission_requires_bounded_scan_not_only_small_state() {
        let spec = QuerySpec::default();
        let count = vec![svindb_core::aggs::Agg {
            name: "matches".into(),
            kind: svindb_core::aggs::Kind::Stats { field: None },
            over: svindb_core::aggs::Over::Docs,
            space: svindb_core::aggs::Space::Int,
        }];
        assert_eq!(search_resource_class(&spec, &count, Some(10_000)), ResourceClass::Interactive);
        assert_eq!(search_resource_class(&spec, &count, Some(10_001)), ResourceClass::Analytics);
        assert_eq!(search_resource_class(&spec, &count, None), ResourceClass::Analytics);
        let stats = vec![svindb_core::aggs::Agg {
            kind: svindb_core::aggs::Kind::Stats { field: Some(0) },
            ..count[0].clone()
        }];
        assert_eq!(search_resource_class(&spec, &stats, Some(1)), ResourceClass::Analytics);
        let expensive = QuerySpec { translit: true, ..QuerySpec::default() };
        assert_eq!(search_resource_class(&expensive, &count, Some(1)), ResourceClass::Analytics);
        let alternatives = QuerySpec {
            words: vec![SpecWord {
                fid: 0,
                terms: (0..33).map(|i| Term::parse(&format!("term{i}"))).collect(),
                given: None,
            }],
            ..QuerySpec::default()
        };
        assert_eq!(search_resource_class(&alternatives, &count, Some(1)), ResourceClass::Analytics);
    }

    #[test]
    fn cache_relief_cannot_hide_an_uncancellable_permit_wait() {
        let gate: &'static RwLock<()> = Box::leak(Box::new(RwLock::new(())));
        let queues: &'static Resources =
            Box::leak(Box::new(Resources { cache_gate: gate, ..Resources::new(16) }));
        let exclusive = gate.write().unwrap();
        let guard = svindb_core::work::Guard::new(15);
        let started = Instant::now();
        assert!(queues.enter_until(ResourceClass::Point, Some(&guard)).is_none());
        assert_eq!(guard.reason(), svindb_core::work::Reason::Deadline);
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(queues.active(ResourceClass::Point), 0);
        assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.waiters[ResourceClass::Point as usize].load(Ordering::Acquire), 0);
        assert!(queues.enter(ResourceClass::Control).is_some());
        drop(exclusive);
        assert!(queues.enter(ResourceClass::Point).is_some());
    }

    #[test]
    fn write_waits_for_transient_cache_relief_without_holding_other_permits() {
        let gate: &'static RwLock<()> = Box::leak(Box::new(RwLock::new(())));
        let queues: &'static Resources =
            Box::leak(Box::new(Resources { cache_gate: gate, ..Resources::new(16) }));
        std::thread::scope(|scope| {
            let (ready, wait) = std::sync::mpsc::channel();
            scope.spawn(move || {
                let exclusive = gate.write().unwrap();
                ready.send(()).unwrap();
                let until = Instant::now() + Duration::from_millis(100);
                while queues.waiters[ResourceClass::Write as usize].load(Ordering::Acquire) == 0 {
                    assert!(Instant::now() < until);
                    std::thread::yield_now();
                }
                std::thread::sleep(Duration::from_millis(10));
                assert_eq!(queues.active(ResourceClass::Write), 0);
                assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
                assert!(queues.enter(ResourceClass::Control).is_some());
                drop(exclusive);
            });
            wait.recv().unwrap();
            let permit = queues
                .enter_until(ResourceClass::Write, None)
                .expect("краткая занятость отвергла запись");
            drop(permit);
        });
        assert_eq!(queues.active(ResourceClass::Write), 0);
        assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.lanes[ResourceClass::Write as usize].rejected.load(Ordering::Relaxed), 0);
        assert!(
            queues.lanes[ResourceClass::Write as usize].waited_micros.load(Ordering::Relaxed) > 0
        );
    }

    #[test]
    fn resource_wait_honors_the_request_deadline() {
        let workload = config::Workload { interactive_max_units: 8, ..config::Workload::default() };
        let queues: &'static Resources = Box::leak(Box::new(Resources::new_with(16, &workload)));
        let _a = queues.enter(ResourceClass::Interactive).expect("первый поиск отвергнут");
        let _b = queues.enter(ResourceClass::Interactive).expect("второй поиск отвергнут");
        let guard = svindb_core::work::Guard::new(15);
        let began = Instant::now();
        assert!(
            queues.enter_until(ResourceClass::Interactive, Some(&guard)).is_none(),
            "очередь выдала permit после истечения срока"
        );
        assert_eq!(guard.reason(), svindb_core::work::Reason::Deadline);
        assert!(began.elapsed() < Duration::from_millis(100), "очередь ждала слишком долго");
    }

    #[test]
    fn analytics_yields_to_an_interactive_waiter() {
        // CPU для analytics остаются, но interactive lane уже заполнена.
        // Иначе отказ проверял бы исчерпание CPU, а не приоритет waiter.
        let mut resources = Resources::new(16);
        resources.cpu_max = 24;
        resources.lanes[ResourceClass::Interactive as usize].max = 3;
        let queues: &'static Resources = Box::leak(Box::new(resources));
        let _a = queues.enter(ResourceClass::Interactive).expect("первый поиск отвергнут");
        let _b = queues.enter(ResourceClass::Interactive).expect("второй поиск отвергнут");
        let _c = queues.enter(ResourceClass::Interactive).expect("третий поиск отвергнут");
        std::thread::scope(|scope| {
            let interactive_guard = svindb_core::work::Guard::new(100);
            scope.spawn(move || {
                assert!(
                    queues
                        .enter_until(ResourceClass::Interactive, Some(&interactive_guard))
                        .is_none(),
                    "ожидающий interactive неожиданно получил место"
                );
            });
            let until = Instant::now() + Duration::from_millis(50);
            while queues.waiters[ResourceClass::Interactive as usize].load(Ordering::Acquire) == 0
                && Instant::now() < until
            {
                std::thread::yield_now();
            }
            assert!(
                queues.waiters[ResourceClass::Interactive as usize].load(Ordering::Acquire) > 0,
                "interactive waiter не попал в очередь"
            );
            let analytics_guard = svindb_core::work::Guard::new(15);
            assert!(
                queues.enter_until(ResourceClass::Analytics, Some(&analytics_guard)).is_none(),
                "analytics обошла ожидающий interactive"
            );
            assert_eq!(analytics_guard.reason(), svindb_core::work::Reason::Deadline);
        });
    }

    #[test]
    fn search_concurrency_uses_cpu_budget_and_keeps_control_and_point_lanes() {
        let queues: &'static Resources = Box::leak(Box::new(Resources::new(16)));
        let permits: Vec<_> = (0..4)
            .map(|_| queues.enter_now(ResourceClass::Analytics, false, 2).expect("analytics c4"))
            .collect();
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 8);
        assert!(queues.enter_now(ResourceClass::Analytics, false, 1).is_none());
        let interactive = queues.enter_now(ResourceClass::Interactive, false, 2).unwrap();
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 9);
        assert!(queues.enter(ResourceClass::Point).is_some(), "point зависит от search lane");
        assert!(queues.enter(ResourceClass::Control).is_some(), "control зависит от search lane");
        drop((permits, interactive));
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn narrow_analytics_does_not_serialize_on_eight_cpu_nodes() {
        let config = config::Workload { search_cpu_units: 4, ..config::Workload::default() };
        let queues: &'static Resources = Box::leak(Box::new(Resources::new_with(8, &config)));
        let permits: Vec<_> = (0..4)
            .map(|_| queues.enter_now(ResourceClass::Analytics, false, 1).expect("analytics c4"))
            .collect();
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 4);
        assert!(queues.enter_now(ResourceClass::Interactive, false, 1).is_none());
        assert!(queues.enter(ResourceClass::Point).is_some());
        assert!(queues.enter(ResourceClass::Control).is_some());
        drop(permits);
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn compaction_sees_waiting_user_reads_before_they_get_permits() {
        let queues = Resources::new(16);
        assert!(!queues.query_demand());
        for class in [ResourceClass::Point, ResourceClass::Interactive] {
            queues.waiters[class as usize].store(1, Ordering::Release);
            assert_eq!(queues.active(class), 0);
            assert!(queues.query_demand(), "ожидающий read невидим для compaction");
            queues.waiters[class as usize].store(0, Ordering::Release);
            assert!(!queues.query_demand());
        }
        queues.waiters[ResourceClass::Write as usize].store(1, Ordering::Release);
        assert!(!queues.query_demand(), "ingest сам приостановил необходимую ему compaction");
    }

    #[test]
    fn search_cpu_reservations_never_reissue_running_units() {
        let workload = config::Workload { interactive_max_units: 8, ..config::Workload::default() };
        let queues: &'static Resources = Box::leak(Box::new(Resources::new_with(16, &workload)));
        let first = queues.enter(ResourceClass::Interactive).unwrap();
        let second = queues.enter(ResourceClass::Interactive).unwrap();
        assert_eq!((first.cpu_units, second.cpu_units), (8, 4));
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 12);
        let deadline = svindb_core::work::Guard::new(10);
        assert!(queues.enter_until(ResourceClass::Analytics, Some(&deadline)).is_none());
        assert_eq!(queues.active(ResourceClass::Analytics), 0);
        assert!(queues.enter(ResourceClass::Control).is_some());
        assert!(queues.enter(ResourceClass::Point).is_some());
        drop(first);
        let analytics = queues.enter(ResourceClass::Analytics).unwrap();
        assert_eq!(analytics.cpu_units, 4);
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 8);
        drop((second, analytics));
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 0);
        let narrow = queues.enter_until_with_units(ResourceClass::Interactive, None, 2).unwrap();
        assert_eq!(narrow.cpu_units, 2, "узкий план не должен резервировать весь узел");
        drop(narrow);
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn eight_interactive_clients_hold_independent_cpu_reservations() {
        let queues: &'static Resources = Box::leak(Box::new(Resources::new(10)));
        let (entered, arrivals) = std::sync::mpsc::channel();
        let release = AtomicBool::new(false);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let entered = entered.clone();
                let release = &release;
                scope.spawn(move || {
                    let permit = queues
                        .enter_until_with_units(ResourceClass::Interactive, None, 10)
                        .expect("interactive client cannot enter alongside seven peers");
                    entered.send(permit.cpu_units).unwrap();
                    while !release.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                    drop(permit);
                });
            }
            drop(entered);
            let held: Vec<_> =
                (0..8).filter_map(|_| arrivals.recv_timeout(Duration::from_secs(1)).ok()).collect();
            let active = queues.active(ResourceClass::Interactive);
            let used = queues.cpu_used.load(Ordering::Acquire);
            let control = queues.enter(ResourceClass::Control);
            let point = queues.enter(ResourceClass::Point);
            release.store(true, Ordering::Release);
            assert_eq!(held, vec![1; 8]);
            assert_eq!(active, 8);
            assert_eq!(used, 8);
            assert!(used < queues.cpu_max);
            assert!(control.is_some());
            assert!(point.is_some());
        });
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
        let analytics = queues.enter(ResourceClass::Analytics).unwrap();
        assert_eq!(analytics.cpu_units, 4, "large work retains parallel workers");
    }

    #[test]
    fn cpu_admission_is_atomic_under_competing_searches() {
        let mut resources = Resources::new(16);
        // Убираем ограничение числа запросов только в тесте: проверяется
        // именно общий CPU budget, а не предшествующий lane limit.
        resources.lanes[ResourceClass::Interactive as usize].max = 64;
        resources.io_max = 1_000;
        let queues: &'static Resources = Box::leak(Box::new(resources));
        let barrier = std::sync::Barrier::new(16);
        let completed = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let barrier = &barrier;
                let completed = &completed;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..1_000 {
                        if let Some(permit) = queues.enter_now(ResourceClass::Interactive, false, 8)
                        {
                            assert!((1..=8).contains(&permit.cpu_units));
                            let used = queues.cpu_used.load(Ordering::Acquire);
                            assert!(used >= permit.cpu_units && used <= 12, "выдано {used}");
                            std::thread::yield_now();
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert!(completed.load(Ordering::Relaxed) > 0);
        assert_eq!(queues.cpu_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.active(ResourceClass::Interactive), 0);
    }

    #[test]
    fn disconnect_cancels_a_point_waiting_for_cache_relief() {
        let gate: &'static RwLock<()> = Box::leak(Box::new(RwLock::new(())));
        let queues: &'static Resources =
            Box::leak(Box::new(Resources { cache_gate: gate, ..Resources::new(16) }));
        let _exclusive = gate.write().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let wire = wire::Wire::plain(server, Duration::from_secs(5));
        let guard = Arc::new(svindb_core::work::Guard::new(5_000));
        let _watch = DisconnectWatch::start(&wire, Arc::clone(&guard)).unwrap();
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let until = Instant::now() + Duration::from_millis(100);
                while queues.waiters[ResourceClass::Point as usize].load(Ordering::Acquire) == 0 {
                    assert!(Instant::now() < until, "запрос не вошёл в очередь");
                    std::thread::yield_now();
                }
                drop(client);
            });
            let began = Instant::now();
            assert!(queues.enter_until(ResourceClass::Point, Some(&guard)).is_none());
            assert_eq!(guard.reason(), svindb_core::work::Reason::Cancelled);
            assert!(began.elapsed() < Duration::from_millis(250));
        });
        assert_eq!(queues.active(ResourceClass::Point), 0);
        assert_eq!(queues.io_used.load(Ordering::Acquire), 0);
        assert_eq!(queues.waiters[ResourceClass::Point as usize].load(Ordering::Acquire), 0);
    }

    #[test]
    fn concurrent_mget_shares_hydration_threads() {
        assert_eq!(point_read_share(1, 16), 8);
        assert_eq!(point_read_share(2, 16), 8);
        assert_eq!(point_read_share(4, 16), 4);
        assert_eq!(point_read_share(16, 16), 1);
        assert_eq!(point_read_share(32, 16), 1);
    }

    #[test]
    fn refresh_does_not_rebuild_an_over_limit_live_buffer_on_every_tick() {
        assert!(!refresh_live_needed(false, 10, 10, 10), "пустой тик пересобрал live view");
        assert!(
            refresh_live_needed(false, 9, 8, 10),
            "изменившийся допустимый буфер не опубликован"
        );
        assert!(
            !refresh_live_needed(false, 11, 10, 10),
            "буфер сверх live_max_docs зациклил бесполезную пересборку"
        );
        assert!(refresh_live_needed(true, 11, 10, 10), "легший disk segment не вызвал reload");
        assert!(refresh_live_needed(false, 1, 0, 0), "лимит 0 должен сохранять обычную свежесть");
    }

    #[test]
    fn refresh_due_obeys_minimum_batch_and_hard_visibility_deadline() {
        assert!(!refresh_due(5_000, 0, 1_000, 1, 5_000), "пустой буфер не должен публиковаться");
        assert!(!refresh_due(1_000, 9, 1_000, 10, 5_000), "неполная пачка обошла refresh_min_docs");
        assert!(refresh_due(1_000, 10, 1_000, 10, 5_000), "полная пачка не стала видимой");
        assert!(refresh_due(5_000, 1, 1_000, 10, 5_000), "refresh_max_interval не сработал");
        assert!(!refresh_due(999, 10, 1_000, 10, 5_000), "публикация произошла раньше интервала");
    }
}

#[cfg(test)]
mod strict_dsl_regression_tests {
    use super::*;

    #[test]
    fn unsupported_query_keys_are_rejected_at_every_native_level() {
        let names = vec!["body".into()];
        for body in [
            serde_json::json!({"words":[{"field":"body","terms":["x"]}],"query":{"prefix":{"body":"x"}}}),
            serde_json::json!({"query":{"should":[{"term":{"field":"body","terms":["x"]},"ignored":1}]}}),
            serde_json::json!({"query":{"should":[{"bool":{"should":[{"field":"body","terms":["x"]}],"ignored":1}}]}}),
            serde_json::json!({"query":{"should":[{"bool":{"should":[{"field":"body","terms":["x"]}]},"ignored":1}]}}),
            serde_json::json!({"words":[{"field":"body","terms":["x",7]}]}),
            serde_json::json!({"words":[{"field":"body","terms":["x"]}],"minimum_should_match":"1"}),
            serde_json::json!({"words":[{"field":"body","terms":["x"]}],"query":[]}),
        ] {
            assert!(parse_query_value(&body, &names, &[Kind::Text], &[], &[]).is_err(), "{body}");
        }
        for term in [
            serde_json::json!({"range":{"field":"body","from":1,"ignored":2}}),
            serde_json::json!({"exists":{"field":"body","ignored":2}}),
            serde_json::json!({"term":{"field":"body","value":"x"},"ignored":2}),
            serde_json::json!({"field":"body","value":"x","ignored":2}),
        ] {
            assert!(parse_slot_term(&term, "slots.all", &names, &[Kind::Int64]).is_err(), "{term}");
        }
    }

    #[test]
    fn aggregation_options_and_nested_aggregations_are_never_ignored() {
        let names = vec!["n".into()];
        for aggregation in [
            serde_json::json!({"terms":{"field":"n","order":{"_key":"asc"}}}),
            serde_json::json!({"terms":{"field":"n","min_doc_count":2}}),
            serde_json::json!({"terms":{"field":"n"},"aggs":{"inner":{"stats":{"field":"n"}}}}),
            serde_json::json!({"stats":{"field":"n"},"terms":{"field":"n"}}),
            serde_json::json!({"stats":{"field":"n","missing":0}}),
            serde_json::json!({"range":{"field":"n","ranges":[{"from":1,"to":2,"key":"named"}]}}),
            serde_json::json!({"histogram":{"field":"n","interval":1,"offset":"x"}}),
            serde_json::json!({"date_histogram":{"field":"n","fixed_interval":1,"calendar_interval":"1d"}}),
            serde_json::json!({"cardinality":{"field":"n","mode":7}}),
            serde_json::json!({"terms":{"field":"n","size":"10"}}),
        ] {
            let body = serde_json::json!({"aggs":{"a":aggregation}});
            assert!(parse_aggs(&body, &names, &[Kind::Int64], &[], &[]).is_err(), "{body}");
        }
        let body = serde_json::json!({"aggs":{"a":{"range":{"field":"n","ranges":[{"from":1,"to":2},{"from":2}]}}}});
        let aggs = parse_aggs(&body, &names, &[Kind::Int64], &[], &[]).unwrap();
        assert!(
            matches!(&aggs[0].kind, svindb_core::aggs::Kind::Range { edges, .. } if edges == &vec![(1, Some(2)), (2, None)])
        );
    }
}

#[cfg(test)]
mod strict_serve_flags_tests {
    #[test]
    fn unknown_missing_and_boolean_values_are_rejected() {
        for flag in [
            "--conf=missing.json",
            "--config",
            "--config=",
            "--read-only=true",
            "--no-maintain=false",
            "--threads",
            "--threds=4",
            "--bind=",
            "--port=19300",
        ] {
            let error = super::serve_mode(&[flag]).unwrap_err();
            assert!(error.contains(flag), "{flag}: {error}");
        }
    }

    #[test]
    fn every_numeric_value_is_checked_even_after_a_valid_duplicate() {
        for name in [
            "--threads",
            "--max-indices",
            "--replicate-keep-mb",
            "--replicate-mbps",
            "--posmin",
            "--flush",
            "--target",
            "--refresh-ms",
            "--merge-at-once",
            "--grace-ms",
            "--merge-threads",
            "--term",
            "--dead-fraction",
        ] {
            for value in ["", "oops", "-1", "18446744073709551616"] {
                let good = format!("{name}=1");
                let bad = format!("{name}={value}");
                assert!(super::serve_mode(&[&good, &bad]).is_err(), "{bad}");
            }
        }
        for flag in [
            "--threads=0",
            "--max-indices=0",
            "--posmin=4294967296",
            "--dead-fraction=NaN",
            "--dead-fraction=inf",
            "--dead-fraction=1.1",
        ] {
            assert!(super::serve_mode(&[flag]).is_err(), "{flag}");
        }
    }

    #[test]
    fn supported_boolean_and_value_flags_remain_accepted() {
        assert_eq!(
            super::serve_mode(&[
                "--single-index",
                "--warm=eager",
                "--threads=2",
                "--config=config.json",
                "--bind=0.0.0.0",
                "--tls-cert=cert.pem",
                "--tls-key=key.pem",
                "--fields=",
                "--no-wal",
                "--sync-flush",
                "--allow-low-map-count",
                "--ack=quorum",
                "--refresh-ms=0",
                "--dead-fraction=0.5",
                "--posmin=4294967295",
            ])
            .unwrap(),
            (true, true)
        );
    }
}

#[cfg(test)]
mod compaction_quantum_tests {
    use super::*;

    #[test]
    fn only_fresh_known_severe_pressure_can_block_grants() {
        let now = Instant::now();
        let sample = pressure::Sample {
            pressure: svindb_core::mem::Pressure {
                cgroup_limited: true,
                io_psi_full_avg10: Some(2.0),
                ..svindb_core::mem::Pressure::default()
            },
            delta: pressure::Delta::default(),
            at: now,
        };
        assert!(QueryFirstCompactionHook::hard_pressure(Some(sample), now));
        assert!(!QueryFirstCompactionHook::hard_pressure(
            Some(sample),
            now + Duration::from_secs(2)
        ));
        assert!(!QueryFirstCompactionHook::hard_pressure(None, now));
        let healthy =
            pressure::Sample { pressure: svindb_core::mem::Pressure::default(), ..sample };
        assert!(!QueryFirstCompactionHook::hard_pressure(Some(healthy), now));
    }

    fn state(now: Instant) -> CompactionPauseState {
        CompactionPauseState { grant_until: now, next_grant: now, busy_since: None }
    }

    #[test]
    fn continuous_search_increases_bounded_merge_duty_after_two_seconds() {
        let start = Instant::now();
        let mut state = state(start);
        let mut grants = [0usize; 10];
        for ms in 0..5000u64 {
            if state.wait_at(start + Duration::from_millis(ms), true, false).is_none() {
                grants[ms as usize / 500] += 1;
            }
        }
        assert_eq!(&grants[..4], &[50; 4]);
        assert_eq!(&grants[4..], &[125; 6]);
    }

    #[test]
    fn hard_pressure_revokes_an_open_grant_and_idle_resets_sustained_demand() {
        let start = Instant::now();
        let mut state = state(start);
        assert!(state.wait_at(start, true, false).is_none());
        assert_eq!(
            state.wait_at(start + Duration::from_millis(1), true, true),
            Some(Duration::from_millis(10))
        );
        assert!(state.wait_at(start + Duration::from_millis(2), true, false).is_some());
        assert!(state.wait_at(start + Duration::from_secs(2), true, false).is_none());
        assert_eq!(state.grant_until, start + Duration::from_millis(2125));
        assert!(state.wait_at(start + Duration::from_secs(3), false, false).is_none());
        assert_eq!(state.busy_since, None);
        assert!(state.wait_at(start + Duration::from_secs(3), true, false).is_none());
        assert_eq!(state.grant_until, start + Duration::from_millis(3050));
        assert!(state.wait_at(start + Duration::from_secs(4), false, true).is_some());
    }
}

#[cfg(test)]
mod audit_guard_regression_tests {
    use super::*;

    #[test]
    fn rotation_failure_stops_ack_and_subsequent_staging() {
        let root =
            std::env::temp_dir().join(format!("svindb-rotation-failure-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let wal =
            Wal::open(root.to_str().unwrap(), 0, 0, Arc::new(metrics::Metrics::default())).unwrap();
        wal.file.lock().unwrap().len = svindb_core::wal::ROLL_BYTES;
        let directory = root.join(svindb_core::wal::DIR);
        std::fs::rename(&directory, root.join("held-wal")).unwrap();
        std::fs::write(&directory, b"not a directory").unwrap();
        let sequence = wal.stage(svindb_core::wal::ADD, &[vec![1]]).unwrap();
        assert!(wal.commit(sequence).is_err());
        assert_eq!(wal.state.lock().unwrap().synced, 0);
        assert!(wal.stage(svindb_core::wal::ADD, &[vec![2]]).is_err());
        assert!(wal.commit(sequence).is_err());
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_native_ids_are_distinct_and_explicit_empty_ids_fail() {
        let plan = nested::Plan::build(&[], &[], &[], &[String::new()]);
        let (documents, _) = parse_docs(br#"{"documents":[{},{}]}"#, &plan).unwrap();
        assert!(!documents[0].id.is_empty());
        assert_ne!(documents[0].id, documents[1].id);
        assert!(parse_docs(br#"{"documents":[{"id":""}]}"#, &plan).is_err());
    }

    #[test]
    fn coordinator_pages_globally_and_refuses_missing_reductions() {
        let (ask, from, size) =
            prepare_fanout_request(br#"{"match_all":true,"from":5,"size":2}"#).unwrap();
        assert_eq!((from, size), (5, 2));
        assert_eq!(ask["from"], 0);
        assert_eq!(ask["size"], 7);
        assert_eq!(ask["partial"], false);
        for body in [
            serde_json::json!({"aggs":{"n":{"count":{}}}}),
            serde_json::json!({"sort":[{"field":"n"}]}),
            serde_json::json!({"track_total_hits":true}),
        ] {
            assert!(prepare_fanout_request(body.to_string().as_bytes()).is_err());
        }
    }

    #[test]
    fn hierarchical_verifier_uses_the_same_dotted_projection_as_ingest() {
        let mapping = serde_json::json!({"mappings":{"dynamic":"strict","properties":{
            "parents":{"type":"nested","properties":{
                "children":{"type":"nested","properties":{"user.name":{"type":"keyword"}}}
            }}
        }}});
        let rules = mapping::Rules::from_bytes(&serde_json::to_vec(&mapping).unwrap()).unwrap();
        let dotted = serde_json::json!({"parents":[{"children":[{"user.name":"alice"}]}]});
        let nested = serde_json::json!({"parents":[{"children":[{"user":{"name":"alice"}}]}]});
        let query = serde_json::json!({"nested":{"path":"parents","query":{
            "nested":{"path":"parents.children","query":{
                "term":{"parents.children.user.name":"alice"}
            }}
        }}});
        for source in [&dotted, &nested] {
            let projection = nested::normalize_projection(source, &rules).unwrap();
            assert!(source_query_matches(
                &projection,
                &query,
                &["parents.children.user.name".into()],
                &[Kind::Keyword]
            )
            .unwrap());
        }
        assert_eq!(dotted["parents"][0]["children"][0]["user.name"], "alice");
    }

    #[test]
    fn zero_timeout_selects_default_and_does_not_disable_header_deadline() {
        let route = route::Route::parse("GET", "/docs/_doc/a?timeout_ms=0");
        let request = http::Request {
            method: "GET".into(),
            path: "/docs/_doc/a?timeout_ms=0".into(),
            body: Vec::new(),
            close: true,
            headers: Vec::new(),
            body_len: 0,
        };
        let remaining = point_guard(&request, &route).unwrap().remaining().unwrap();
        assert!(remaining <= Duration::from_millis(DEFAULT_SEARCH_TIMEOUT_MS));
        assert!(remaining > Duration::from_secs(29));
        let request =
            http::Request { headers: vec![("x-svindb-timeout-ms".into(), "10".into())], ..request };
        let guard = search_guard(&request, &serde_json::json!({"timeout_ms":0})).unwrap();
        assert!(guard.remaining().unwrap() <= Duration::from_millis(10));
        let request =
            http::Request { headers: vec![("x-svindb-timeout-ms".into(), "0".into())], ..request };
        assert!(search_guard(&request, &serde_json::json!({"timeout_ms":0}))
            .unwrap()
            .remaining()
            .is_some());
    }

    #[test]
    fn page_windows_reject_wrong_types_and_overflow() {
        for body in [
            serde_json::json!({"match_all":true,"size":-1}),
            serde_json::json!({"match_all":true,"from":"1"}),
            serde_json::json!({"match_all":true,"from":u64::MAX,"size":2}),
        ] {
            assert!(parse_query_value(&body, &[], &[], &[], &[]).is_err());
        }
    }
}
