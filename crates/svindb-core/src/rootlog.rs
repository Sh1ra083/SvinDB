//! Долговечный журнал корневого каталога: снимок плюс дописываемый журнал.
//!
//! ## Зачем отдельный примитив, а не ещё один JSON рядом
//!
//! Состав индексов до сих пор был выводим из листинга каталога `indices/`, и
//! этого хватало, пока состав был единственным, что нужно знать. Aliases,
//! блоки и метаданные вывести из листинга нельзя: они не имеют представления
//! на диске, кроме того, которое мы сами заведём. А переписывать один общий
//! файл целиком на каждую привязку значит платить за изменение alias тем
//! больше, чем больше архивных поколений накопилось, — то есть ровно в том
//! сценарии, ради которого всё и делается.
//!
//! Отсюда форма: редкий большой **снимок** и частый маленький **журнал**.
//! Изменение стоит одну дописанную запись и один `fsync`, независимо от того,
//! сто индексов в корне или десять тысяч.
//!
//! ## Почему кадр берётся у WAL, а не пишется заново
//!
//! [`crate::wal::append`] и [`crate::wal::scan`] уже решают ровно эту задачу:
//! длина, контрольная сумма, остановка на первой негодной записи. Своя копия
//! кадра означала бы два места, где чинится одна и та же ошибка, и они
//! однажды разойдутся. Слот `seq` здесь несёт **поколение каталога** вместо
//! номера операции — ширина и смысл «монотонно растущий счётчик» те же.
//!
//! ## Асимметрия, которую нужно держать в голове
//!
//! `snapshot.bin` пишется **только** через [`crate::fs::atomic_write`], то
//! есть через уникальный inode и `rename`. Рваным он не бывает физически.
//! Поэтому несошедшаяся сумма снимка — это порча, а не оборванная запись, и
//! она обязана останавливать запуск. Законный оборванный хвост есть только у
//! `journal.bin`.

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Имя файла снимка внутри каталога журнала.
pub const SNAPSHOT: &str = "snapshot.bin";
/// Имя файла дописываемого журнала.
pub const JOURNAL: &str = "journal.bin";

/// Опознавательные байты обоих файлов.
const MAGIC: &[u8; 8] = b"svinroot";
/// Версия раскладки самих файлов журнала.
const FORMAT: u16 = 1;
/// Длина заголовка обоих файлов.
const HEADER: usize = 32;

/// Потолок тела снимка до резервирования памяти. Испорченные восемь байт
/// длины не должны превращаться в произвольное выделение.
const MAX_SNAPSHOT: u64 = 64 * 1024 * 1024;
/// Жёсткий предел журнала до чтения в память. В норме он уплотняется на сотнях
/// килобайт; сотни мегабайт означают многократный отказ compaction или порчу.
const MAX_JOURNAL: u64 = 256 * 1024 * 1024;
/// Потолок одной записи внутри тела снимка.
const MAX_REC: usize = 16 * 1024 * 1024;

/// Журнал уплотняется, когда он перерос снимок, но не раньше этого порога:
/// у пустого корня снимок занимает десятки байт, и без нижней границы
/// уплотнение шло бы после каждой второй транзакции.
const COMPACT_FLOOR: u64 = 256 * 1024;
/// Второй повод уплотнить — много мелких записей при крошечном снимке.
const COMPACT_RECORDS: u64 = 4096;

/// Одна запись: вид и тело. Смысл вида знает вызывающий, не журнал.
///
/// Журнал намеренно не проверяет вид: он не знает домена. Неизвестный вид —
/// жёсткая ошибка, но объявляет её тот, кто умеет его разобрать. Здесь же
/// пропуск был бы потерей привязки при восстановлении, то есть ровно тем, от
/// чего журнал и заводится.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rec {
    pub kind: u8,
    pub body: Vec<u8>,
}

/// Что прочиталось при открытии.
#[derive(Debug)]
pub struct Loaded {
    /// Записи снимка — состояние на поколении `base_generation`.
    pub base: Vec<Rec>,
    /// Поколение, на котором снят снимок.
    pub base_generation: u64,
    /// Транзакции журнала строго после `base_generation`, по возрастанию.
    pub replay: Vec<(u64, Rec)>,
    /// Последнее поколение: снимка либо последней транзакции журнала.
    pub generation: u64,
    /// Сколько байт оборванного хвоста отброшено.
    pub dropped: u64,
}

/// Открытый журнал корневого каталога.
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    /// Дескриптор для дописывания. `None` в режиме только чтения.
    tail: Option<std::fs::File>,
    generation: u64,
    journal_bytes: u64,
    journal_records: u64,
    snapshot_bytes: u64,
}

fn head(generation: u64, body: &[u8]) -> [u8; HEADER] {
    let mut out = [0u8; HEADER];
    out[0..8].copy_from_slice(MAGIC);
    out[8..10].copy_from_slice(&FORMAT.to_le_bytes());
    // 10..12 — резерв, ноль.
    out[12..16].copy_from_slice(&crate::wal::crc32(body).to_le_bytes());
    out[16..24].copy_from_slice(&generation.to_le_bytes());
    out[24..32].copy_from_slice(&(body.len() as u64).to_le_bytes());
    out
}

/// Разбирает заголовок и возвращает `(поколение, длина тела, сумма тела)`.
fn parse_head(bytes: &[u8], what: &Path) -> Result<(u64, u64, u32), String> {
    if bytes.len() < HEADER {
        return Err(format!(
            "{}: {} байт, а заголовок занимает {HEADER}",
            what.display(),
            bytes.len()
        ));
    }
    if &bytes[0..8] != MAGIC {
        return Err(format!("{}: чужие опознавательные байты", what.display()));
    }
    let format = u16::from_le_bytes(bytes[8..10].try_into().expect("два байта"));
    if format != FORMAT {
        return Err(format!(
            "{}: раскладка журнала {format}, а читается {FORMAT} — файл от другой версии",
            what.display()
        ));
    }
    let reserved = u16::from_le_bytes(bytes[10..12].try_into().expect("два байта"));
    if reserved != 0 {
        // Отказ, а не игнорирование: ненулевой резерв означает поле, которого
        // эта сборка не знает, и молча потерять его смысл хуже, чем не встать.
        return Err(format!("{}: занят резерв заголовка ({reserved})", what.display()));
    }
    let crc = u32::from_le_bytes(bytes[12..16].try_into().expect("четыре байта"));
    let generation = u64::from_le_bytes(bytes[16..24].try_into().expect("восемь байт"));
    let len = u64::from_le_bytes(bytes[24..32].try_into().expect("восемь байт"));
    Ok((generation, len, crc))
}

/// Складывает записи снимка в тело: `вид u8 | длина u32 | тело`.
fn encode_base(base: &[Rec]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for rec in base {
        if rec.body.len() > MAX_REC {
            return Err(format!(
                "запись снимка вида {} занимает {} байт, потолок {MAX_REC}",
                rec.kind,
                rec.body.len()
            ));
        }
        out.reserve(5 + rec.body.len());
        out.push(rec.kind);
        out.extend((rec.body.len() as u32).to_le_bytes());
        out.extend_from_slice(&rec.body);
    }
    Ok(out)
}

/// Разбирает тело снимка целиком.
///
/// Хвостовой остаток — порча, а не оборванный хвост: снимок публикуется
/// `rename`-ом целиком, поэтому «немного лишних байт в конце» означает, что
/// файл писал не мы.
fn decode_base(body: &[u8], what: &Path) -> Result<Vec<Rec>, String> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at < body.len() {
        if at + 5 > body.len() {
            return Err(format!("{}: тело обрывается на заголовке записи", what.display()));
        }
        let kind = body[at];
        let len =
            u32::from_le_bytes(body[at + 1..at + 5].try_into().expect("четыре байта")) as usize;
        if len > MAX_REC {
            return Err(format!("{}: запись длиной {len}, потолок {MAX_REC}", what.display()));
        }
        if at + 5 + len > body.len() {
            return Err(format!("{}: запись длиной {len} не помещается в тело", what.display()));
        }
        out.push(Rec { kind, body: body[at + 5..at + 5 + len].to_vec() });
        at += 5 + len;
    }
    Ok(out)
}

/// Есть ли после первой негодной позиции ещё один полностью валидный WAL
/// frame. Тогда это не оборванный последний append, а порча в середине:
/// усечь её значило бы молча потерять уже зафиксированные поздние поколения.
fn valid_frame_after(bytes: &[u8], from: usize) -> bool {
    const FRAME_HEAD: usize = 8;
    for at in from..bytes.len().saturating_sub(FRAME_HEAD) {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().expect("четыре байта")) as usize;
        if !(9..=16 * 1024 * 1024).contains(&len) {
            continue;
        }
        let Some(end) = at.checked_add(FRAME_HEAD + len) else { continue };
        if end > bytes.len() {
            continue;
        }
        let want = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("четыре байта"));
        if crate::wal::crc32(&bytes[at + 8..end]) == want {
            return true;
        }
    }
    false
}

/// Есть ли уже журнал в этом каталоге.
pub fn exists(dir: &Path) -> bool {
    dir.join(SNAPSHOT).exists()
}

impl Log {
    /// Создаёт журнал впервые: снимок на заданном поколении и пустой журнал.
    ///
    /// Порядок обязателен: сначала durable снимок, затем durable журнал.
    /// Обратный порядок оставил бы после аварии журнал без снимка, то есть
    /// файл, про который нельзя сказать, с какого состояния он продолжает.
    pub fn create(dir: &Path, generation: u64, base: &[Rec]) -> Result<Log, String> {
        crate::fs::create_dir_all_real(dir)
            .map_err(|e| format!("каталог журнала {} не создаётся: {e}", dir.display()))?;
        let mut log = Log {
            dir: dir.to_path_buf(),
            tail: None,
            generation,
            journal_bytes: 0,
            journal_records: 0,
            snapshot_bytes: 0,
        };
        log.write_snapshot(generation, base)?;
        log.rotate_journal(generation)?;
        log.open_tail()?;
        Ok(log)
    }

    /// Открывает существующий журнал и восстанавливает состояние.
    pub fn open(dir: &Path, writable: bool) -> Result<(Log, Loaded), String> {
        let snap_path = dir.join(SNAPSHOT);
        let raw = crate::fs::read_regular_limited(&snap_path, MAX_SNAPSHOT + HEADER as u64, false)
            .map_err(|e| format!("снимок каталога {} не читается: {e}", snap_path.display()))?;
        let (base_generation, len, crc) = parse_head(&raw, &snap_path)?;
        if len > MAX_SNAPSHOT {
            return Err(format!(
                "{}: тело снимка {len} байт, потолок {MAX_SNAPSHOT}",
                snap_path.display()
            ));
        }
        let end = HEADER
            .checked_add(usize::try_from(len).map_err(|_| {
                format!("{}: длина тела не помещается в адресацию", snap_path.display())
            })?)
            .ok_or_else(|| format!("{}: длина тела переполняет размер", snap_path.display()))?;
        if raw.len() != end {
            return Err(format!(
                "{}: заявлено {len} байт тела, а файл занимает {} — снимок публикуется целиком, \
                 поэтому расхождение означает порчу, а не оборванную запись",
                snap_path.display(),
                raw.len()
            ));
        }
        let body = &raw[HEADER..end];
        if crate::wal::crc32(body) != crc {
            return Err(format!(
                "{}: контрольная сумма снимка не сошлась — каталог повреждён; \
                 пересобрать состав по листингу нельзя, это молча уничтожило бы aliases и блоки",
                snap_path.display()
            ));
        }
        let base = decode_base(body, &snap_path)?;

        let journal_path = dir.join(JOURNAL);
        let jraw = crate::fs::read_regular_limited(&journal_path, MAX_JOURNAL, false)
            .map_err(|e| format!("журнал каталога {} не читается: {e}", journal_path.display()))?;
        let (journal_generation, jlen, _) = parse_head(&jraw, &journal_path)?;
        if jlen != 0 {
            return Err(format!(
                "{}: заголовок журнала заявляет тело в {jlen} байт, а журнал дописывается",
                journal_path.display()
            ));
        }
        if journal_generation > base_generation {
            // Обратное — законное окно уплотнения. А вот журнал, начатый
            // позже снимка, означает откат снимка назад: состояние потеряно.
            return Err(format!(
                "{}: журнал начат на поколении {journal_generation}, а снимок снят на \
                 {base_generation} — снимок старше журнала, каталог повреждён",
                journal_path.display()
            ));
        }
        let scan = crate::wal::scan(&jraw[HEADER..]);
        if scan.torn && valid_frame_after(&jraw[HEADER..], scan.good as usize + 1) {
            return Err(format!(
                "{}: после негодной записи найден целый поздний кадр — это порча в середине, а не оборванный хвост; усечение потеряло бы подтверждённые поколения",
                journal_path.display()
            ));
        }

        let mut replay: Vec<(u64, Rec)> = Vec::new();
        let mut generation = base_generation;
        for rec in scan.records {
            if rec.seq <= base_generation {
                // Уже внутри снимка. Это и есть окно уплотнения: снимок лёг,
                // а журнал ещё не повернулся.
                continue;
            }
            if rec.seq != generation + 1 {
                return Err(format!(
                    "{}: поколение {} после {generation} — записи журнала непрерывны по \
                     построению, потому что следующая дописывается только после возврата \
                     fsync у предыдущей; разрыв означает порчу, а не оборванный хвост",
                    journal_path.display(),
                    rec.seq
                ));
            }
            generation = rec.seq;
            replay.push((rec.seq, Rec { kind: rec.kind, body: rec.body }));
        }

        let mut log = Log {
            dir: dir.to_path_buf(),
            tail: None,
            generation,
            journal_bytes: scan.good,
            journal_records: replay.len() as u64,
            snapshot_bytes: raw.len() as u64,
        };
        if writable {
            if scan.torn {
                // Обрезаем только при записи: читающий процесс не чинит чужой
                // файл, а нескольким читателям нечего согласовывать.
                let path = log.journal_path();
                let file = crate::fs::open_regular(&path, false, true)
                    .map_err(|e| format!("журнал {} не открывается: {e}", path.display()))?;
                file.set_len(HEADER as u64 + scan.good)
                    .and_then(|()| file.sync_all())
                    .map_err(|e| format!("журнал {} не усекается: {e}", path.display()))?;
                eprintln!(
                    "каталог: журнал {} обрезан с {} до {} байт — оборванный хвост",
                    path.display(),
                    jraw.len(),
                    HEADER as u64 + scan.good
                );
            }
            log.open_tail()?;
        }
        let loaded = Loaded { base, base_generation, replay, generation, dropped: scan.dropped };
        Ok((log, loaded))
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join(SNAPSHOT)
    }

    fn journal_path(&self) -> PathBuf {
        self.dir.join(JOURNAL)
    }

    fn open_tail(&mut self) -> Result<(), String> {
        let path = self.journal_path();
        let mut file = crate::fs::open_regular(&path, false, true)
            .map_err(|e| format!("журнал {} не открывается на запись: {e}", path.display()))?;
        let at = file
            .seek(SeekFrom::End(0))
            .map_err(|e| format!("журнал {} не перематывается в конец: {e}", path.display()))?;
        let want = HEADER as u64 + self.journal_bytes;
        if at != want {
            return Err(format!("журнал {}: конец на {at}, а годных байт {want}", path.display()));
        }
        self.tail = Some(file);
        Ok(())
    }

    fn write_snapshot(&mut self, generation: u64, base: &[Rec]) -> Result<(), String> {
        let body = encode_base(base)?;
        if body.len() as u64 > MAX_SNAPSHOT {
            return Err(format!(
                "снимок каталога занял бы {} байт, потолок {MAX_SNAPSHOT}",
                body.len()
            ));
        }
        let mut bytes = Vec::with_capacity(HEADER + body.len());
        bytes.extend_from_slice(&head(generation, &body));
        bytes.extend_from_slice(&body);
        let path = self.snapshot_path();
        crate::fs::atomic_write(&path, &bytes, 0o600)
            .map_err(|e| format!("снимок каталога {} не пишется: {e}", path.display()))?;
        self.snapshot_bytes = bytes.len() as u64;
        Ok(())
    }

    fn rotate_journal(&mut self, generation: u64) -> Result<(), String> {
        // Атомарная **замена**, а не усечение: усечённый файл на миг был бы
        // валидным-но-неполным, и авария в этот миг оставила бы журнал,
        // про который нельзя доказать, что он пуст намеренно.
        let path = self.journal_path();
        crate::fs::atomic_write(&path, &head(generation, &[]), 0o600)
            .map_err(|e| format!("журнал каталога {} не поворачивается: {e}", path.display()))?;
        self.tail = None;
        self.journal_bytes = 0;
        self.journal_records = 0;
        Ok(())
    }

    /// Последнее записанное поколение.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Дописывает одну транзакцию и синхронизирует её.
    ///
    /// Возврат означает, что транзакция переживёт отключение питания. Именно
    /// поэтому публикация состояния в памяти делается **после** возврата.
    pub fn append(&mut self, generation: u64, kind: u8, body: &[u8]) -> Result<(), String> {
        if generation != self.generation + 1 {
            return Err(format!(
                "поколение {generation} после {}: журнал принимает только следующее",
                self.generation
            ));
        }
        let Some(file) = self.tail.as_mut() else {
            return Err("каталог открыт только для чтения".to_string());
        };
        let mut frame = Vec::new();
        crate::wal::append(&mut frame, generation, kind, body);
        file.write_all(&frame)
            .and_then(|()| file.sync_all())
            .map_err(|e| format!("журнал каталога не дописывается: {e}"))?;
        crate::fail::point("rootcat.after_journal_fsync");
        self.generation = generation;
        self.journal_bytes += frame.len() as u64;
        self.journal_records += 1;
        Ok(())
    }

    /// Пора ли уплотнять.
    pub fn due_for_compaction(&self) -> bool {
        self.journal_records >= COMPACT_RECORDS
            || self.journal_bytes > self.snapshot_bytes.max(COMPACT_FLOOR)
    }

    /// Переписывает снимок и поворачивает журнал.
    ///
    /// Авария между снимком и поворотом безопасна без единого дополнительного
    /// состояния: журнал остаётся со старого поколения, но открытие пропускает
    /// записи `поколение <= поколения снимка`, и повтор становится пустым.
    pub fn compact(&mut self, base: &[Rec]) -> Result<(), String> {
        let generation = self.generation;
        self.write_snapshot(generation, base)?;
        crate::fail::point("rootcat.after_snapshot");
        self.rotate_journal(generation)?;
        crate::fail::point("rootcat.after_rotate");
        self.open_tail()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(kind: u8, body: &str) -> Rec {
        Rec { kind, body: body.as_bytes().to_vec() }
    }

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "svindb-rootlog-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn a_created_log_reads_back_its_snapshot() {
        let dir = temp("создание");
        let base = vec![rec(1, "первый"), rec(2, "второй")];
        drop(Log::create(&dir, 7, &base).expect("журнал"));
        let (log, loaded) = Log::open(&dir, true).expect("открытие");
        assert_eq!(loaded.base, base);
        assert_eq!(loaded.base_generation, 7);
        assert_eq!(loaded.generation, 7);
        assert!(loaded.replay.is_empty());
        assert_eq!(log.generation(), 7);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn appended_transactions_replay_in_order() {
        let dir = temp("повтор");
        let mut log = Log::create(&dir, 1, &[rec(1, "основа")]).expect("журнал");
        log.append(2, 0x10, b"alias").expect("дописать");
        log.append(3, 0x11, b"block").expect("дописать");
        drop(log);
        let (_, loaded) = Log::open(&dir, true).expect("открытие");
        assert_eq!(loaded.base_generation, 1);
        assert_eq!(loaded.generation, 3);
        let kinds: Vec<(u64, u8)> = loaded.replay.iter().map(|(g, r)| (*g, r.kind)).collect();
        assert_eq!(kinds, vec![(2, 0x10), (3, 0x11)]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_torn_tail_is_dropped_and_the_rest_survives() {
        let dir = temp("хвост");
        let mut log = Log::create(&dir, 1, &[]).expect("журнал");
        log.append(2, 0x10, "целая".as_bytes()).expect("дописать");
        log.append(3, 0x10, "оборванная".as_bytes()).expect("дописать");
        drop(log);
        // Отрезаем последние байты — так выглядит выключенное посреди записи питание.
        let path = dir.join(JOURNAL);
        let len = std::fs::metadata(&path).expect("размер").len();
        let file = std::fs::OpenOptions::new().write(true).open(&path).expect("файл");
        file.set_len(len - 4).expect("усечение");
        drop(file);

        let (_, loaded) = Log::open(&dir, true).expect("открытие");
        assert_eq!(loaded.generation, 2, "уцелевшая транзакция должна остаться");
        assert_eq!(loaded.replay.len(), 1);
        assert!(loaded.dropped > 0);
        // После обрезки журнал снова принимает записи с правильного поколения.
        let (mut log, _) = Log::open(&dir, true).expect("повторное открытие");
        log.append(3, 0x10, "новая".as_bytes()).expect("дописать после обрезки");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_generation_gap_is_corruption_not_a_tail() {
        let dir = temp("разрыв");
        let mut log = Log::create(&dir, 1, &[]).expect("журнал");
        log.append(2, 0x10, "первая".as_bytes()).expect("дописать");
        drop(log);
        // Дописываем кадр с поколением 9 — целый по сумме, но не следующий.
        let path = dir.join(JOURNAL);
        let mut frame = Vec::new();
        crate::wal::append(&mut frame, 9, 0x10, "из будущего".as_bytes());
        let mut file = std::fs::OpenOptions::new().append(true).open(&path).expect("файл");
        file.write_all(&frame).expect("дописать вручную");
        drop(file);

        let error = Log::open(&dir, true).expect_err("разрыв поколения обязан останавливать");
        assert!(error.contains("поколение 9"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corruption_between_two_complete_records_is_not_truncated_as_a_tail() {
        let dir = temp("порча-в-середине");
        let mut log = Log::create(&dir, 1, &[]).expect("журнал");
        log.append(2, 0x10, b"first").expect("первая");
        log.append(3, 0x10, b"second").expect("вторая");
        drop(log);
        let path = dir.join(JOURNAL);
        let mut bytes = std::fs::read(&path).expect("чтение");
        // Первый frame начинается сразу после rootlog-header. Меняем его тело,
        // не задевая длину, чтобы второй frame остался полностью валидным.
        bytes[HEADER + 8 + 9] ^= 0x40;
        std::fs::write(&path, bytes).expect("порча");
        let error = Log::open(&dir, true).expect_err("порча в середине принята за crash-tail");
        assert!(error.contains("порча в середине"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_flipped_byte_in_the_snapshot_stops_startup() {
        let dir = temp("порча");
        drop(Log::create(&dir, 3, &[rec(1, "важное")]).expect("журнал"));
        let path = dir.join(SNAPSHOT);
        let mut bytes = std::fs::read(&path).expect("чтение");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("запись");

        let error = Log::open(&dir, true).expect_err("порча снимка обязана останавливать");
        assert!(error.contains("контрольная сумма"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_foreign_file_is_refused_by_its_magic() {
        let dir = temp("чужой");
        drop(Log::create(&dir, 1, &[]).expect("журнал"));
        std::fs::write(
            dir.join(SNAPSHOT),
            "этот файл писали не мы, но длина похожа на правду!!".as_bytes(),
        )
        .expect("запись");
        let error = Log::open(&dir, true).expect_err("чужой файл обязан отвергаться");
        assert!(error.contains("опознавательные байты"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_folds_the_journal_into_the_snapshot() {
        let dir = temp("уплотнение");
        let mut log = Log::create(&dir, 1, &[rec(1, "старое")]).expect("журнал");
        log.append(2, 0x10, "изменение".as_bytes()).expect("дописать");
        log.compact(&[rec(1, "старое"), rec(2, "новое")]).expect("уплотнить");
        assert_eq!(log.generation(), 2);
        drop(log);

        let (_, loaded) = Log::open(&dir, true).expect("открытие");
        assert_eq!(loaded.base_generation, 2);
        assert_eq!(loaded.base.len(), 2);
        assert!(loaded.replay.is_empty(), "журнал обязан быть пуст после поворота");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_crash_between_snapshot_and_rotation_replays_to_nothing() {
        let dir = temp("окно");
        let mut log = Log::create(&dir, 1, &[rec(1, "основа")]).expect("журнал");
        log.append(2, 0x10, "изменение".as_bytes()).expect("дописать");
        // Снимок лёг, поворот не успел: это и есть окно между двумя durable шагами.
        log.write_snapshot(2, &[rec(1, "основа"), rec(2, "изменение")]).expect("снимок");
        drop(log);

        let (log, loaded) = Log::open(&dir, true).expect("открытие");
        assert_eq!(loaded.base_generation, 2);
        assert_eq!(loaded.base.len(), 2);
        assert!(
            loaded.replay.is_empty(),
            "запись поколения 2 уже внутри снимка и повторяться не должна"
        );
        assert_eq!(log.generation(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_journal_started_after_the_snapshot_is_refused() {
        let dir = temp("откат");
        drop(Log::create(&dir, 5, &[]).expect("журнал"));
        // Снимок «откатили» назад, а журнал остался с более позднего поколения.
        let mut log = Log {
            dir: dir.clone(),
            tail: None,
            generation: 2,
            journal_bytes: 0,
            journal_records: 0,
            snapshot_bytes: 0,
        };
        log.write_snapshot(2, &[]).expect("снимок");
        drop(log);

        let error = Log::open(&dir, true).expect_err("откат снимка обязан отвергаться");
        assert!(error.contains("старше журнала"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_read_only_open_neither_truncates_nor_appends() {
        let dir = temp("чтение");
        let mut log = Log::create(&dir, 1, &[]).expect("журнал");
        log.append(2, 0x10, "целая".as_bytes()).expect("дописать");
        drop(log);
        let path = dir.join(JOURNAL);
        let len = std::fs::metadata(&path).expect("размер").len();
        let file = std::fs::OpenOptions::new().write(true).open(&path).expect("файл");
        file.set_len(len - 3).expect("усечение");
        drop(file);

        let (mut log, loaded) = Log::open(&dir, false).expect("открытие");
        assert!(loaded.dropped > 0);
        assert_eq!(
            std::fs::metadata(&path).expect("размер").len(),
            len - 3,
            "читающий процесс не чинит чужой файл"
        );
        let error = log.append(2, 0x10, "нельзя".as_bytes()).expect_err("запись в режиме чтения");
        assert!(error.contains("только для чтения"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_refuses_a_generation_that_is_not_next() {
        let dir = temp("порядок");
        let mut log = Log::create(&dir, 1, &[]).expect("журнал");
        let error = log.append(3, 0x10, "через одно".as_bytes()).expect_err("пропуск поколения");
        assert!(error.contains("только следующее"), "{error}");
        log.append(2, 0x10, "подряд".as_bytes()).expect("дописать");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
