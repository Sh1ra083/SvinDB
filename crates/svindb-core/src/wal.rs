//! Журнал приёма: что обещано клиенту, то переживает падение.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;

/// Вид записи.
pub const ADD: u8 = 1;
/// Гашение по идентификаторам.
pub const DELETE: u8 = 2;
/// Тело то же, что у [`ADD`]: идентификатор и значения полей подряд.
pub const PUT: u8 = 3;

/// Приём документа с массивами: тело — список строк
/// `[id, арность по группам…, «поле», «слот», значение, …]`, где число групп
/// известно из схемы узла.
///
/// Отдельный вид, а не расширение [`ADD`]: прежняя запись — это `[id, значения
/// по полям]`, и дописать в неё слоты значило бы, что одно и то же тело читают
/// двумя способами в зависимости от схемы. Разойдясь однажды, они дали бы
/// переигрывание, кладущее значения не в те ячейки, — молча.
pub const ADD_CELLS: u8 = 4;

/// Замена документа с массивами. Тело — как у [`ADD_CELLS`].
pub const PUT_CELLS: u8 = 5;

/// Атомарная пачка документов.
///
/// Первый байт тела — один из `ADD`, `PUT`, `ADD_CELLS`, `PUT_CELLS`, затем
/// идут законченные тела документов. Одна внешняя CRC и одна запись нужны не
/// для экономии заголовков: оборванный WAL либо содержит всю пачку, либо не
/// содержит ни одного её документа.
pub const BATCH: u8 = 6;

/// Атомарная смешанная мутация: замены документов и гашения других ID.
/// Тело кодируется [`encode_mutation`]; одна внешняя CRC не позволяет
/// восстановлению увидеть только одну половину операции.
pub const MUTATION: u8 = 7;

/// Заголовок записи: длина тела, контрольная сумма, номер, вид.
const HEADER: usize = 4 + 4 + 8 + 1;

/// Потолок тела записи. Защита от испорченной длины: без неё битые четыре байта
/// заставили бы выделить сколько угодно памяти.
///
/// Значение публично не для того, чтобы дать внешнему коду собирать WAL, а
/// чтобы слой приёма мог отвергнуть расширившуюся после кодирования атомарную
/// пачку **до** выдачи ей номера. HTTP-предел измеряется исходным JSON и не
/// защищает этот предел: длины строк и полей в WAL добавляют заметный объём.
pub const MAX_RECORD_BODY: usize = 64 * 1024 * 1024;
const MAX_BODY: usize = MAX_RECORD_BODY;

// ---------------------------------------------------------------------------
// CRC-32, табличный. Свой, потому что в ядре зависимостей нет.
// ---------------------------------------------------------------------------

fn tables() -> &'static [[u32; 256]; 8] {
    static T: std::sync::OnceLock<[[u32; 256]; 8]> = std::sync::OnceLock::new();
    T.get_or_init(|| {
        let mut t = [[0u32; 256]; 8];
        for (i, slot) in t[0].iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            }
            *slot = c;
        }
        for n in 1..8 {
            for i in 0..256 {
                let c = t[n - 1][i];
                t[n][i] = t[0][(c & 0xff) as usize] ^ (c >> 8);
            }
        }
        t
    })
}

/// Потоковый CRC-32 для файлов, которые нельзя безопасно собирать в один Vec.
pub struct Crc32(u32);

impl Crc32 {
    pub fn new() -> Crc32 {
        Crc32(0xFFFF_FFFF)
    }

    pub fn update(&mut self, bytes: &[u8]) {
        let t = tables();
        let mut c = self.0;
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            // IEEE CRC-32, reflected; explicit little endian loads also work
            // for unaligned slices and on big endian hosts.
            let first = u32::from_le_bytes(chunk[..4].try_into().unwrap()) ^ c;
            c = t[7][(first & 0xff) as usize]
                ^ t[6][((first >> 8) & 0xff) as usize]
                ^ t[5][((first >> 16) & 0xff) as usize]
                ^ t[4][(first >> 24) as usize]
                ^ t[3][chunk[4] as usize]
                ^ t[2][chunk[5] as usize]
                ^ t[1][chunk[6] as usize]
                ^ t[0][chunk[7] as usize];
        }
        for &b in chunks.remainder() {
            c = t[0][((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
        }
        self.0 = c;
    }

    pub fn finish(&self) -> u32 {
        self.0 ^ 0xFFFF_FFFF
    }
}

impl Default for Crc32 {
    fn default() -> Self {
        Self::new()
    }
}

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = Crc32::new();
    crc.update(bytes);
    crc.finish()
}

/// Одна запись журнала.
#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub kind: u8,
    pub body: Vec<u8>,
}

/// Складывает запись в поток байт.
///
/// ## Почему без промежуточного буфера
///
/// Прежде тело собиралось в отдельный `payload` — только чтобы посчитать по нему
/// сумму, — и оттуда копировалось в поток. То есть каждая пачка приёма
/// копировалась лишний раз целиком, в потоке запроса, ради числа в четыре байта.
///
/// Сумма считается по частям, а байты пишутся сразу на место. Формат не
/// меняется ни на бит: сумма та же, порядок тот же.
/// Length of one encoded record in a staged group commit. Allows the writer
/// to rotate only between complete records without limiting group size.
pub fn encoded_record_len(bytes: &[u8]) -> std::io::Result<usize> {
    let header = bytes.get(..8).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "truncated WAL header")
    })?;
    let body = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    if !(9..=MAX_BODY).contains(&body) || body + 8 > bytes.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid WAL record length",
        ));
    }
    Ok(body + 8)
}

pub fn append(out: &mut Vec<u8>, seq: u64, kind: u8, body: &[u8]) {
    let head = seq.to_le_bytes();
    let len = 9 + body.len();
    let mut crc = Crc32::new();
    crc.update(&head);
    crc.update(&[kind]);
    crc.update(body);
    out.reserve(8 + len);
    out.extend((len as u32).to_le_bytes());
    out.extend(crc.finish().to_le_bytes());
    out.extend_from_slice(&head);
    out.push(kind);
    out.extend_from_slice(body);
}

/// Что нашлось в журнале при чтении.
pub struct Scan {
    pub records: Vec<Record>,
    /// Байт до конца последней **годной** записи. Дальше начинается мусор.
    pub good: u64,
    /// Байт хвоста, отброшенных как оборванные или испорченные.
    pub dropped: u64,
    /// Оборвался ли журнал на середине записи.
    pub torn: bool,
}

/// Читает записи подряд до первой негодной.
///
/// Негодная — это оборванная (не хватило байт) или не сошедшаяся по контрольной
/// сумме. Дальше не читаем **намеренно**: за дырой могут лежать целые с виду
/// записи, но порядок уже нарушен, и переигрывать их значило бы применить
/// позднее без более раннего.
pub fn scan(bytes: &[u8]) -> Scan {
    let mut records = Vec::new();
    let mut at = 0usize;
    loop {
        if at + HEADER > bytes.len() {
            break;
        }
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().expect("четыре байта")) as usize;
        let want = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("четыре байта"));
        if !(9..=MAX_BODY).contains(&len) || at + 8 + len > bytes.len() {
            break;
        }
        let payload = &bytes[at + 8..at + 8 + len];
        if crc32(payload) != want {
            break;
        }
        records.push(Record {
            seq: u64::from_le_bytes(payload[0..8].try_into().expect("восемь байт")),
            kind: payload[8],
            body: payload[9..].to_vec(),
        });
        at += 8 + len;
    }
    let dropped = (bytes.len() - at) as u64;
    Scan { records, good: at as u64, dropped, torn: dropped > 0 }
}

// ---------------------------------------------------------------------------
// Тело записи: строки подряд, каждая с длиной.
// ---------------------------------------------------------------------------

pub fn encode_strings(parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + parts.iter().map(|p| 4 + p.len()).sum::<usize>());
    encode_strings_into(&mut out, parts);
    out
}

/// То же, но в готовый буфер: без выделения на каждый документ.
///
/// Пачка приёма кодируется по документу, и каждый заводил свой `Vec<u8>` только
/// затем, чтобы тут же быть скопированным в общее тело пачки. Здесь буфер один
/// на пачку, а документы дописываются в него подряд.
pub fn encode_strings_into(out: &mut Vec<u8>, parts: &[&str]) {
    out.extend((parts.len() as u32).to_le_bytes());
    for p in parts {
        out.extend((p.len() as u32).to_le_bytes());
        out.extend_from_slice(p.as_bytes());
    }
}

/// `None` — тело не разбирается, то есть запись негодна, как ни считай сумму.
pub fn decode_strings(body: &[u8]) -> Option<Vec<String>> {
    if body.len() < 4 {
        return None;
    }
    let n = u32::from_le_bytes(body[0..4].try_into().ok()?) as usize;
    let mut at = 4usize;
    let mut out = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        if at + 4 > body.len() {
            return None;
        }
        let l = u32::from_le_bytes(body[at..at + 4].try_into().ok()?) as usize;
        at += 4;
        if at + l > body.len() {
            return None;
        }
        out.push(String::from_utf8_lossy(&body[at..at + l]).into_owned());
        at += l;
    }
    Some(out)
}

/// Упаковывает тела документов в одну атомарную запись WAL.
pub fn encode_batch(kind: u8, bodies: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        5usize.saturating_add(bodies.iter().map(|b| 4 + b.len()).sum::<usize>()),
    );
    out.push(kind);
    out.extend((bodies.len() as u32).to_le_bytes());
    for body in bodies {
        out.extend((body.len() as u32).to_le_bytes());
        out.extend_from_slice(body);
    }
    out
}

/// Разбирает атомарную пачку и не принимает хвост после последнего тела.
///
/// Точная граница важна: иначе испорченная длина могла бы оставить байты,
/// которые CRC признала частью записи, а прикладной разбор молча проигнорировал.
pub fn decode_batch(body: &[u8]) -> Option<(u8, Vec<Vec<u8>>)> {
    if body.len() < 5 {
        return None;
    }
    let kind = body[0];
    if !matches!(kind, ADD | PUT | ADD_CELLS | PUT_CELLS) {
        return None;
    }
    let n = u32::from_le_bytes(body[1..5].try_into().ok()?) as usize;
    // Каждому документу нужны как минимум внешняя длина и счётчик строк во
    // внутреннем encode_strings. Проверяем это до цикла, чтобы поддельный
    // счётчик не заставил долго перебирать отсутствующие элементы.
    if n > body.len().saturating_sub(5) / 8 {
        return None;
    }
    let mut at = 5usize;
    let mut out = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        if at + 4 > body.len() {
            return None;
        }
        let len = u32::from_le_bytes(body[at..at + 4].try_into().ok()?) as usize;
        at += 4;
        if at + len > body.len() {
            return None;
        }
        out.push(body[at..at + len].to_vec());
        at += len;
    }
    (at == body.len()).then_some((kind, out))
}

/// Упаковывает replacements и deletes в одну WAL-запись.
pub fn encode_mutation(kind: u8, documents: &[Vec<u8>], deletes: &[String]) -> Vec<u8> {
    assert!(matches!(kind, PUT | PUT_CELLS));
    let mut out = Vec::with_capacity(
        9usize
            .saturating_add(documents.iter().map(|body| 4 + body.len()).sum::<usize>())
            .saturating_add(deletes.iter().map(|id| 4 + id.len()).sum::<usize>()),
    );
    out.push(kind);
    out.extend((documents.len() as u32).to_le_bytes());
    for body in documents {
        out.extend((body.len() as u32).to_le_bytes());
        out.extend_from_slice(body);
    }
    out.extend((deletes.len() as u32).to_le_bytes());
    for id in deletes {
        let bytes = id.as_bytes();
        out.extend((bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

/// Разбирает смешанную мутацию и отвергает недобор либо лишний хвост.
pub fn decode_mutation(body: &[u8]) -> Option<(u8, Vec<Vec<u8>>, Vec<String>)> {
    if body.len() < 9 {
        return None;
    }
    let kind = body[0];
    if !matches!(kind, PUT | PUT_CELLS) {
        return None;
    }
    let mut at = 1usize;
    let documents_len = take_u32(body, &mut at)? as usize;
    if documents_len > body.len().saturating_sub(at) / 8 {
        return None;
    }
    let mut documents = Vec::with_capacity(documents_len.min(1024));
    for _ in 0..documents_len {
        let len = take_u32(body, &mut at)? as usize;
        let end = at.checked_add(len)?;
        documents.push(body.get(at..end)?.to_vec());
        at = end;
    }
    let deletes_len = take_u32(body, &mut at)? as usize;
    if deletes_len > body.len().saturating_sub(at) / 4 {
        return None;
    }
    let mut deletes = Vec::with_capacity(deletes_len.min(1024));
    for _ in 0..deletes_len {
        let len = take_u32(body, &mut at)? as usize;
        let end = at.checked_add(len)?;
        deletes.push(String::from_utf8(body.get(at..end)?.to_vec()).ok()?);
        at = end;
    }
    (at == body.len()).then_some((kind, documents, deletes))
}

fn take_u32(body: &[u8], at: &mut usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    let value = u32::from_le_bytes(body.get(*at..end)?.try_into().ok()?);
    *at = end;
    Some(value)
}

// ---------------------------------------------------------------------------
// Журнал на диске.
// ---------------------------------------------------------------------------

/// Имя каталога журнала внутри каталога индекса.
pub const DIR: &str = "wal";
/// Файл с номером, до которого всё уже на диске.
pub const CHECKPOINT: &str = "checkpoint";
/// Когда заводить следующий файл журнала.
pub const ROLL_BYTES: u64 = 64 * 1024 * 1024;
/// Максимальный размер файла, который восстановление принимает в память.
/// Писатель обязан ротировать до превышения: файл может перейти порог
/// `ROLL_BYTES` не больше чем на одну максимальную запись.
pub const MAX_FILE_BYTES: u64 = ROLL_BYTES + MAX_BODY as u64 + HEADER as u64;
const MAX_FILES: usize = 4096;

#[cfg(target_os = "macos")]
const O_NOFOLLOW: i32 = 0x0000_0100;
#[cfg(target_os = "linux")]
const O_NOFOLLOW: i32 = 0x0002_0000;
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
const O_NOFOLLOW: i32 = 0;

pub fn dir_of(index: &str) -> String {
    format!("{index}/{DIR}")
}

/// Читает отметку. Ноль — отметки нет, переигрывать надо всё.
pub fn checkpoint(index: &str) -> u64 {
    crate::fs::read_regular_limited(
        std::path::Path::new(&format!("{}/{}", dir_of(index), CHECKPOINT)),
        64,
        true,
    )
    .ok()
    .and_then(|s| String::from_utf8(s).ok())
    .and_then(|s| s.trim().parse().ok())
    .unwrap_or(0)
}

/// Продвигает отметку. Пишется через временный файл: половина числа хуже, чем
/// его отсутствие.
pub fn set_checkpoint(index: &str, seq: u64) -> std::io::Result<()> {
    let d = dir_of(index);
    crate::fs::create_dir_all_real(std::path::Path::new(&d))?;
    // Закрепляем имя самого каталога WAL, в том числе после неудачной попытки.
    sync_directory(std::path::Path::new(index))?;
    crate::fs::atomic_write(
        std::path::Path::new(&d).join(CHECKPOINT).as_path(),
        seq.to_string().as_bytes(),
        0o600,
    )
}

/// Файлы журнала по порядку.
pub fn files(index: &str) -> Vec<String> {
    files_checked(index).unwrap_or_default()
}

/// Потолок номера файла журнала.
///
/// Пятнадцать цифр, а не разрядность `u64`: число обязано без потерь проходить
/// через JSON, где оно становится `f64`. Достижим он не будет — при откате в 64
/// МиБ это порядка десяти в двадцать третьей степени байт, — но предел, который
/// назван и проверен, лучше предела, о который однажды спотыкаются.
pub const MAX_FILE_NUMBER: u64 = 999_999_999_999_999;

/// Каноническое имя файла журнала по его номеру.
///
/// До миллиона — шесть цифр с ведущими нулями: имена, уже лежащие на дисках,
/// остаются каноническими, и переименовывать ничего не нужно. Дальше — без
/// набивки, потому что упорядочивание у нас и так численное (`sort_by_key` по
/// разобранному номеру), а набивка до фиксированной ширины стоила бы
/// совместимости ради красоты вывода `ls`.
pub fn file_name(n: u64) -> String {
    if n < 1_000_000 {
        format!("{n:06}.log")
    } else {
        format!("{n}.log")
    }
}

/// Номер по имени файла. `None` — имя не журнальное.
///
/// Каноничность проверяется обратным преобразованием, а не длиной: иначе
/// `0999999.log` и `999999.log` разобрались бы в один номер, и два файла
/// боролись бы за одно место в порядке.
pub fn number_of(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".log")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Двадцать цифр — предел разрядности `u64`; длиннее не разбираем вовсе,
    // чтобы `parse` не работал впустую на заведомо негодном имени.
    if digits.len() > 20 {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    (n <= MAX_FILE_NUMBER && name == file_name(n)).then_some(n)
}

/// Временная копия отметки, оставленная `fs::atomic_write` после падения.
fn is_checkpoint_tmp(name: &str) -> bool {
    name.starts_with(&format!(".{CHECKPOINT}.tmp-"))
}

fn files_checked(index: &str) -> std::io::Result<Vec<String>> {
    let d = dir_of(index);
    match std::fs::symlink_metadata(&d) {
        Ok(m) if m.file_type().is_dir() => {}
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{d}: WAL обязан быть настоящим каталогом"),
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    }
    let mut out: Vec<(u64, String)> = Vec::new();
    for entry in std::fs::read_dir(&d)? {
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else { continue };
        // Не журнальные жители каталога — отметка и её временные копии от
        // `fs::atomic_write`. Всё прочее разбирается как имя файла журнала.
        if name == CHECKPOINT || is_checkpoint_tmp(&name) {
            continue;
        }
        let Some(n) = number_of(&name) else {
            // Прежде здесь стоял `continue`, и это был худший из возможных
            // ответов. Писатель после `999999.log` заводит `1000000.log` —
            // семь цифр, — а читатель с проверкой «ровно шесть» его молча
            // пропускал: переигрывание не видело записей, обрезка не видела
            // файла, а клиент уже получил «принято». Отказ громче любой потери.
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{d}/{name}: посторонний файл в каталоге журнала. Пропустить его \
                     значило бы потерять подтверждённые записи молча; уберите файл \
                     или укажите другой каталог"
                ),
            ));
        };
        if !entry.file_type()?.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{d}/{name}: WAL обязан быть обычным файлом"),
            ));
        }
        out.push((n, entry.path().to_string_lossy().to_string()));
    }
    out.sort_by_key(|x| x.0);
    if out.len() > MAX_FILES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("журнал: {} файлов, потолок {MAX_FILES}", out.len()),
        ));
    }
    Ok(out.into_iter().map(|(_, p)| p).collect())
}

/// Убирает файлы, целиком лежащие до отметки.
pub fn trim(index: &str, upto: u64) -> std::io::Result<usize> {
    let mut gone = 0usize;
    let all = files(index);
    // Последний файл — тот, в который писатель прямо сейчас дописывает, и
    // трогать его нельзя ни при какой отметке.
    //
    // На Unix удаление открытого файла не мешает записи: она продолжает идти в
    // отвязанный inode и **успевает вернуть клиенту «принято»**. Пропадает всё
    // это только при падении — то есть ровно тогда, когда журнал и нужен.
    //
    // Воспроизводилось так: приём с мелким сегментом, документы по одному.
    // Второй документ вызывал сброс, отметка догоняла конец файла, файл
    // удалялся, третий документ уходил в никуда с ответом «принято».
    let keep_tail = all.len().saturating_sub(1);
    for f in all.into_iter().take(keep_tail) {
        let Ok(bytes) = read_all(&f) else { continue };
        let s = scan(&bytes);
        match s.records.last() {
            Some(last) if last.seq <= upto && !s.torn => {
                std::fs::remove_file(&f)?;
                gone += 1;
            }
            // Пустой файл держать незачем, а вот оборванный — трогать нельзя:
            // он последний и в него ещё пишут.
            None if !s.torn => {
                std::fs::remove_file(&f)?;
                gone += 1;
            }
            _ => break,
        }
    }
    Ok(gone)
}

/// Итог восстановления — то, что нужно показать человеку после падения.
#[derive(Default, Debug)]
pub struct Recovered {
    pub adds: usize,
    pub deletes: usize,
    /// Замен. Считаются отдельно от приёмов: замена трогает то, что уже лежит,
    /// и после падения человеку важно видеть именно её число.
    pub puts: usize,
    pub from: u64,
    pub to: u64,
    /// Байт хвоста, отброшенных как оборванные.
    pub dropped: u64,
    /// Записей, лежавших до отметки и потому пропущенных.
    pub skipped: usize,
    /// Записей, чьё тело уже лежало в опубликованном сегменте, и переигрывались
    /// только их разрушительные действия. Ноль здесь — обычное дело; ненулевое
    /// значит, что падение попало в окно между публикацией и отметкой.
    pub effects_only: usize,
}

/// Что делать с записью при переигрывании.
///
/// ## Почему полос три, а не две
///
/// Отметка журнала переносится **позже** публикации сегмента: сегмент делается
/// долговечным на потоке фоновой записи, а отметку двигает уже поток запроса.
/// Между ними есть окно, и падение в него оставляет состояние, для которого
/// двух ответов мало.
///
/// Записи ниже отметки долговечны целиком — и тела, и гашения. Их не трогают.
///
/// Записи выше опубликованного не долговечны ничем: их переигрывают полностью.
///
/// А между ними лежит полоса, в которой **тело уже в сегменте, а гашение может
/// быть и не на диске**: карта живых старого сегмента правится в отображении и
/// доходит до диска только вместе с `msync`, который идёт перед отметкой. Здесь
/// нельзя ни пропустить запись целиком (воскреснет удалённое), ни переиграть её
/// целиком (появится вторая живая версия). Измерено: пачка на 2800 документов
/// после падения в это окно давала 4800 строк выдачи — ровно один сегмент
/// лишних.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Band {
    /// Долговечно целиком. Не трогать.
    Skip,
    /// Тело уже в сегменте; переиграть только гашения.
    EffectsOnly,
    /// Не долговечно ничем. Переиграть целиком.
    Full,
}

/// Потоково переигрывает записи с номером больше отметки.
///
/// В памяти находится один ограниченный файл, а не весь журнал за годы. Любая
/// ошибка чтения возвращается вызывающему: молча пропустить файл означало бы
/// подтвердить подъём индекса с дырой в обещанных данных.
pub fn for_replay(
    index: &str,
    mut apply: impl FnMut(Record) -> Result<(), String>,
) -> Result<Recovered, String> {
    // Ноль означает «опубликованного нет», то есть средней полосы тоже нет и
    // поведение в точности прежнее. Так старые вызывающие ничего не замечают.
    for_replay_banded(index, 0, |rec, _| apply(rec))
}

/// То же, но каждой записи называется её полоса.
///
/// `published` — наибольший номер, доведённый до опубликованного сегмента
/// (`max` по заголовкам состава). Его знает только вызывающий: журнал не читает
/// состав и не должен.
pub fn for_replay_banded(
    index: &str,
    published: u64,
    mut apply: impl FnMut(Record, Band) -> Result<(), String>,
) -> Result<Recovered, String> {
    let upto = checkpoint(index);
    let mut r = Recovered::default();
    let all = files_checked(index).map_err(|e| e.to_string())?;
    for f in all {
        let bytes = read_all(&f).map_err(|e| format!("{f}: {e}"))?;
        let s = scan(&bytes);
        r.dropped += s.dropped;
        for rec in s.records {
            if rec.seq <= upto {
                r.skipped += 1;
                continue;
            }
            let band = if rec.seq <= published { Band::EffectsOnly } else { Band::Full };
            if band == Band::EffectsOnly {
                r.effects_only += 1;
            }
            if r.from == 0 {
                r.from = rec.seq;
            }
            r.to = rec.seq;
            match rec.kind {
                ADD | ADD_CELLS => r.adds += 1,
                DELETE => r.deletes += 1,
                PUT | PUT_CELLS => r.puts += 1,
                BATCH => {
                    let Some((kind, bodies)) = decode_batch(&rec.body) else {
                        return Err(format!("{f}: атомарная пачка {} не разобралась", rec.seq));
                    };
                    match kind {
                        ADD | ADD_CELLS => r.adds += bodies.len(),
                        PUT | PUT_CELLS => r.puts += bodies.len(),
                        _ => unreachable!("decode_batch проверил вид"),
                    }
                }
                MUTATION => {
                    let Some((_kind, documents, deletes)) = decode_mutation(&rec.body) else {
                        return Err(format!("{f}: смешанная мутация {} не разобралась", rec.seq));
                    };
                    r.puts += documents.len();
                    r.deletes += deletes.len();
                }
                // Незнакомый вид — отказ, а не пропуск. Пропустить значит
                // потерять документ **молча**, и потерять его при
                // восстановлении после падения — там, где журнал и заводился.
                // Такое бывает при понижении версии узла, и оператор обязан
                // узнать об этом до того, как поверх недосчитанного индекса
                // что-то построят.
                other => {
                    return Err(format!(
                        "{f}: запись {} вида {other} этой сборке неизвестна — журнал писала \
                         более новая версия узла; переигрывать его нельзя, документы пропали бы \
                         молча",
                        rec.seq
                    ))
                }
            }
            apply(rec, band)?;
        }
        // За оборванным файлом читать нечего: он последний по построению.
        if s.torn {
            break;
        }
    }
    Ok(r)
}

/// Открывает последний файл журнала на дозапись, заводя новый при нужде.
pub fn open_tail(index: &str) -> std::io::Result<(File, u64)> {
    let d = dir_of(index);
    crate::fs::create_dir_all_real(std::path::Path::new(&d))?;
    // Закрепляем имя самого каталога WAL, в том числе после неудачной попытки.
    sync_directory(std::path::Path::new(index))?;
    let existing = files_checked(index)?;
    // Проверяем и чиним последний файл до решения о ротации. Иначе
    // оборванный файл выше ROLL_BYTES спрятал бы все записи нового файла:
    // восстановление останавливается на первом оборванном хвосте.
    if let Some(path) = existing.last() {
        let (file, len) = repair_tail(path)?;
        if len < ROLL_BYTES {
            sync_directory(std::path::Path::new(&d))?;
            return Ok((file, len));
        }
    }
    if existing.len() >= MAX_FILES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("журнал: {} файлов, потолок {MAX_FILES}", existing.len()),
        ));
    }
    let n = existing
        .last()
        .and_then(|p| std::path::Path::new(p).file_name())
        .and_then(|s| s.to_str())
        .and_then(number_of)
        .map_or(0, |n| n + 1);
    if n > MAX_FILE_NUMBER {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("журнал: номер файла {n} выше потолка {MAX_FILE_NUMBER}"),
        ));
    }
    let path = format!("{d}/{}", file_name(n));
    let f = OpenOptions::new()
        .read(true)
        .append(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(O_NOFOLLOW)
        .open(&path)?;
    // fsync данных при первой записи не делает имя нового файла долговечным.
    // До выдачи дескриптора закрепляем inode и его запись в каталоге.
    crate::fail::point_io("wal.before_new_file_fsync")?;
    f.sync_all()?;
    sync_directory(std::path::Path::new(&d))?;
    Ok((f, 0))
}

fn sync_directory(path: &std::path::Path) -> std::io::Result<()> {
    crate::fail::point_io("wal.before_directory_fsync")?;
    File::open(path)?.sync_all()
}

fn repair_tail(path: &str) -> std::io::Result<(File, u64)> {
    // Обрезаем мусорный хвост **до** открытия на дозапись.
    //
    // Без этого новая запись легла бы за дыру, а чтение останавливается на
    // первой негодной записи — и всё, что после неё, не увидит ни один подъём.
    // То есть документ был бы принят, записан, переживал бы падение на диске и
    // всё равно пропадал: худший вид потери, потому что он выглядит как
    // сохранность.
    let mut f = crate::fs::open_private_regular_rw(std::path::Path::new(&path), true, true)?;
    if f.metadata()?.len() > MAX_FILE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{path}: файл журнала больше {MAX_FILE_BYTES} байт"),
        ));
    }
    let mut bytes = Vec::new();
    f.read_to_end(&mut bytes)?;
    let s = scan(&bytes);
    if s.torn {
        f.set_len(s.good)?;
        f.sync_all()?;
        eprintln!("журнал: {path} обрезан с {} до {} байт — оборванный хвост", bytes.len(), s.good);
    }
    f.seek(SeekFrom::End(0))?;
    let len = f.metadata()?.len();
    Ok((f, len))
}

/// Наибольший номер записи, встреченный в журнале. Ноль — журнал пуст.
pub fn last_seq(index: &str) -> u64 {
    let mut top = 0u64;
    for f in files(index) {
        let Ok(bytes) = read_all(&f) else { continue };
        for rec in scan(&bytes).records {
            top = top.max(rec.seq);
        }
    }
    top
}

/// С какого номера продолжать нумерацию записей при подъёме.
///
/// `segments` — наибольший номер, вошедший в сегменты. Отметка его покрывает, но
/// выводить целость данных из чужой аккуратности не стоит.
pub fn next_seq(index: &str, replayed_to: u64, segments: u64) -> u64 {
    checkpoint(index).max(last_seq(index)).max(replayed_to).max(segments)
}

/// Читает файл целиком — для команды осмотра.
pub fn read_all(path: &str) -> std::io::Result<Vec<u8>> {
    crate::fs::read_regular_limited(std::path::Path::new(path), MAX_FILE_BYTES, true)
}

#[cfg(test)]
mod name_tests {
    use super::*;

    /// Имя и номер переводятся друг в друга без потерь — в том числе там, где
    /// прежний писатель обгонял собственного читателя.
    #[test]
    fn a_name_and_its_number_survive_the_round_trip() {
        for n in [
            0u64,
            1,
            9,
            10,
            99_999,
            999_998,
            999_999,
            1_000_000,
            1_000_001,
            9_999_999,
            u64::from(u32::MAX),
            MAX_FILE_NUMBER,
        ] {
            let name = file_name(n);
            assert_eq!(number_of(&name), Some(n), "имя {name} разобралось не в {n}");
        }
        // Ширина меняется ровно на миллионе, и обе стороны об этом знают.
        assert_eq!(file_name(999_999), "999999.log");
        assert_eq!(file_name(1_000_000), "1000000.log");
    }

    /// Неканоническое имя отвергается, а не разбирается «как-нибудь».
    ///
    /// Иначе `0999999.log` и `999999.log` дали бы один номер, и два файла
    /// боролись бы за одно место в порядке переигрывания.
    #[test]
    fn a_non_canonical_name_is_refused() {
        for name in [
            "0999999.log",
            "00000001.log",
            "1.log",
            "999999.LOG",
            "999999.log.tmp",
            ".log",
            "log",
            "abc.log",
            "-1.log",
            "1e6.log",
            "999999",
            "",
            "99999999999999999999999.log",
        ] {
            assert_eq!(number_of(name), None, "имя {name:?} принято, хотя не каноническое");
        }
        assert_eq!(number_of(&file_name(MAX_FILE_NUMBER + 1)), None, "потолок не сторожит");
    }

    /// Порядок численный, а не строковый: иначе после миллиона журнал
    /// переигрывался бы задом наперёд.
    #[test]
    fn the_order_is_numeric_and_not_lexical() {
        let mut names: Vec<String> = [999_998u64, 999_999, 1_000_000, 1_000_001, 2_000_000]
            .iter()
            .map(|&n| file_name(n))
            .collect();
        // Строковый порядок здесь заведомо неверен — на нём и держалась ошибка.
        let mut lexical = names.clone();
        lexical.sort();
        assert_ne!(lexical, names, "проверка выродилась: строковый порядок совпал с численным");
        names.sort_by_key(|n| number_of(n).expect("каноническое имя"));
        assert_eq!(
            names,
            vec!["999998.log", "999999.log", "1000000.log", "1000001.log", "2000000.log"]
        );
    }

    /// Отметка и её временные копии живут в том же каталоге и файлами журнала
    /// не считаются — но и отказом не становятся.
    #[test]
    fn the_checkpoint_and_its_leftovers_are_not_journal_files() {
        assert!(is_checkpoint_tmp(&format!(".{CHECKPOINT}.tmp-1234-5")));
        assert!(!is_checkpoint_tmp(CHECKPOINT));
        assert!(!is_checkpoint_tmp("000001.log"));
        assert_eq!(number_of(CHECKPOINT), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_crc32(bytes: &[u8]) -> u32 {
        let mut crc = u32::MAX;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xEDB8_8320 & 0u32.wrapping_sub(crc & 1));
            }
        }
        !crc
    }

    #[test]
    fn slicing_crc_matches_ieee_reference_and_incremental_updates() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32(b""), 0);
        let mut random = 0x1234_5678u32;
        let bytes: Vec<u8> = (0..16_400)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                random as u8
            })
            .collect();
        for len in (0..260).chain([511, 512, 513, 1023, 4095, 4096, 8193, 16_384]) {
            // Offsets deliberately exercise unaligned chunks.
            for offset in 0..8 {
                let data = &bytes[offset..offset + len];
                let expected = reference_crc32(data);
                assert_eq!(crc32(data), expected, "len={len}, offset={offset}");
                for chunk in [1, 3, 7, 8, 9, 31, 128] {
                    let mut crc = Crc32::new();
                    crc.update(&[]);
                    for part in data.chunks(chunk) {
                        crc.update(part);
                        crc.update(&[]);
                    }
                    assert_eq!(crc.finish(), expected, "len={len}, chunk={chunk}");
                    assert_eq!(crc.finish(), expected, "finish must not mutate state");
                }
            }
        }
    }

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-wal-{name}"));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    #[test]
    fn a_record_survives_the_round_trip() {
        let mut buf = Vec::new();
        append(&mut buf, 1, ADD, &encode_strings(&["док-1", "заголовок", "тело"]));
        append(&mut buf, 2, DELETE, &encode_strings(&["док-7", "док-9"]));

        let s = scan(&buf);
        assert_eq!(s.dropped, 0);
        assert!(!s.torn);
        assert_eq!(s.records.len(), 2);
        assert_eq!(s.records[0].seq, 1);
        assert_eq!(s.records[0].kind, ADD);
        assert_eq!(decode_strings(&s.records[0].body).unwrap(), vec!["док-1", "заголовок", "тело"]);
        assert_eq!(decode_strings(&s.records[1].body).unwrap(), vec!["док-7", "док-9"]);
    }

    /// У пачки одна внешняя граница CRC: оборванный хвост не оставляет первые
    /// документы «как будто закоммиченными» без последних.
    #[test]
    fn an_atomic_batch_survives_whole_or_not_at_all() {
        let docs = vec![
            encode_strings(&["queue", "1"]),
            encode_strings(&["attempt", "1"]),
            encode_strings(&["checkpoint", "1"]),
        ];
        let body = encode_batch(PUT, &docs);
        let mut wal = Vec::new();
        append(&mut wal, 7, BATCH, &body);

        let whole = scan(&wal);
        assert_eq!(whole.records.len(), 1);
        assert_eq!(decode_batch(&whole.records[0].body), Some((PUT, docs.clone())));
        for cut in 1..wal.len() {
            let torn = scan(&wal[..cut]);
            assert!(torn.records.is_empty(), "обрыв на {cut} оставил часть пачки");
            assert!(torn.torn);
        }

        let mut with_tail = body;
        with_tail.push(0);
        assert!(decode_batch(&with_tail).is_none(), "лишний хвост проглочен");
    }

    #[test]
    fn a_mixed_mutation_round_trips_without_partial_tail() {
        let documents =
            vec![encode_strings(&["relation:new", "1"]), encode_strings(&["manifest:source", "2"])];
        let deletes = vec!["relation:old".to_string(), "linkocc:old".to_string()];
        let body = encode_mutation(PUT, &documents, &deletes);
        assert_eq!(decode_mutation(&body), Some((PUT, documents.clone(), deletes.clone())));

        let mut wal = Vec::new();
        append(&mut wal, 8, MUTATION, &body);
        for cut in 1..wal.len() {
            let torn = scan(&wal[..cut]);
            assert!(torn.records.is_empty(), "обрыв на {cut} оставил часть мутации");
            assert!(torn.torn);
        }

        let mut with_tail = body;
        with_tail.push(0);
        assert!(decode_mutation(&with_tail).is_none(), "лишний хвост проглочен");
    }

    /// Оборванный хвост отсекается ровно по границе целых записей.
    ///
    /// Это главное свойство журнала: процесс, убитый посреди записи, не должен
    /// уносить с собой всё, что успело лечь до него.
    #[test]
    fn a_torn_tail_costs_only_itself() {
        let mut buf = Vec::new();
        for i in 1..=10u64 {
            append(&mut buf, i, ADD, &encode_strings(&[&format!("док-{i}")]));
        }
        let whole = buf.len();
        // Обрываем на каждой возможной длине последней записи.
        let ninth = {
            let mut b = Vec::new();
            for i in 1..=9u64 {
                append(&mut b, i, ADD, &encode_strings(&[&format!("док-{i}")]));
            }
            b.len()
        };
        for cut in ninth + 1..whole {
            let s = scan(&buf[..cut]);
            assert_eq!(s.records.len(), 9, "обрыв на {cut} из {whole}");
            assert!(s.torn, "обрыв обязан быть виден");
            assert_eq!(s.dropped, (cut - ninth) as u64);
            assert_eq!(s.records[8].seq, 9);
        }
    }

    /// Испорченный байт делает запись негодной, а не проглатывается.
    #[test]
    fn a_flipped_byte_stops_the_replay_there() {
        let mut buf = Vec::new();
        for i in 1..=5u64 {
            append(&mut buf, i, ADD, &encode_strings(&[&format!("док-{i}")]));
        }
        // Портим тело третьей записи, не трогая её длину.
        let third = 2 * (HEADER + 4 + 4 + "док-1".len());
        buf[third + HEADER + 2] ^= 0x40;

        let s = scan(&buf);
        assert_eq!(s.records.len(), 2, "две целых до порчи");
        assert!(s.torn, "за порчей читать нечего");
    }

    /// Испорченная длина не должна заставить выделять память.
    #[test]
    fn a_broken_length_is_refused_not_obeyed() {
        let mut buf = Vec::new();
        append(&mut buf, 1, ADD, &encode_strings(&["док"]));
        let good = buf.len();
        append(&mut buf, 2, ADD, &encode_strings(&["ещё"]));
        buf[good..good + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        let s = scan(&buf);
        assert_eq!(s.records.len(), 1);
        assert!(s.torn);
    }

    /// Отметка переживает круг, а половина числа не появляется.
    #[test]
    fn the_checkpoint_is_written_whole_or_not_at_all() {
        let dir = tmp("checkpoint");
        assert_eq!(checkpoint(&dir), 0, "нет отметки — переигрываем всё");
        set_checkpoint(&dir, 12345).unwrap();
        assert_eq!(checkpoint(&dir), 12345);
        assert!(!std::path::Path::new(&format!("{dir}/{DIR}/.{CHECKPOINT}.tmp")).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Переигрывается только то, что позже отметки.
    #[test]
    fn replay_starts_after_the_checkpoint() {
        let dir = tmp("replay");
        let d = dir_of(&dir);
        std::fs::create_dir_all(&d).unwrap();
        let mut buf = Vec::new();
        for i in 1..=6u64 {
            let kind = if i % 3 == 0 { DELETE } else { ADD };
            append(&mut buf, i, kind, &encode_strings(&[&format!("док-{i}")]));
        }
        std::fs::write(format!("{d}/000000.log"), &buf).unwrap();
        set_checkpoint(&dir, 4).unwrap();

        let mut recs = Vec::new();
        let r = for_replay(&dir, |rec| {
            recs.push(rec);
            Ok(())
        })
        .unwrap();
        assert_eq!(recs.len(), 2, "пятая и шестая");
        assert_eq!(r.skipped, 4);
        assert_eq!((r.from, r.to), (5, 6));
        assert_eq!((r.adds, r.deletes), (1, 1));
        assert_eq!(r.dropped, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Замена — одна запись, и она переживает круг как своя, а не как приём.
    #[test]
    fn a_replacement_is_one_record_not_two() {
        let dir = tmp("put");
        let d = dir_of(&dir);
        std::fs::create_dir_all(&d).unwrap();
        let mut buf = Vec::new();
        append(&mut buf, 1, ADD, &encode_strings(&["док-1", "заголовок", "тело"]));
        append(&mut buf, 2, PUT, &encode_strings(&["док-1", "новый", "новое тело"]));
        append(&mut buf, 3, DELETE, &encode_strings(&["док-7"]));
        std::fs::write(format!("{d}/000000.log"), &buf).unwrap();

        let mut recs = Vec::new();
        let r = for_replay(&dir, |rec| {
            recs.push(rec);
            Ok(())
        })
        .unwrap();
        assert_eq!(recs.len(), 3);
        assert_eq!((r.adds, r.puts, r.deletes), (1, 1, 1));
        assert_eq!(recs[1].kind, PUT);
        assert_eq!(
            decode_strings(&recs[1].body).unwrap(),
            vec!["док-1", "новый", "новое тело"],
            "тело замены — то же, что у приёма: ид и значения полей"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Нумерация продолжается с отметки, а не с нуля после обрезки.
    #[test]
    fn numbering_continues_from_the_checkpoint_not_from_zero() {
        let dir = tmp("continue");
        set_checkpoint(&dir, 132).unwrap();
        assert!(files(&dir).is_empty(), "журнал обрезан, файлов нет");
        assert_eq!(last_seq(&dir), 0);

        assert_eq!(next_seq(&dir, 0, 0), 132, "счёт продолжается с отметки");
        assert_eq!(next_seq(&dir, 0, 200), 200, "сегмент ушёл дальше отметки");
        assert_eq!(next_seq(&dir, 150, 0), 150, "переигранное ушло дальше отметки");

        // И наоборот: пустой каталог начинает с нуля.
        let clean = tmp("clean");
        assert_eq!(next_seq(&clean, 0, 0), 0);

        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&clean).ok();
    }

    #[test]
    fn wal_discovery_and_tail_open_never_follow_links() {
        use std::os::unix::fs::symlink;
        let dir = tmp("nofollow");
        let wal = dir_of(&dir);
        std::fs::create_dir_all(&wal).unwrap();
        let outside = format!("{dir}/outside");
        std::fs::write(&outside, b"do not touch").unwrap();
        symlink(&outside, format!("{wal}/000000.log")).unwrap();
        // Более поздний настоящий файл не должен заставить discovery молча
        // пропустить подложенный первый журнал.
        //
        // Постороннего имени здесь намеренно нет: оно отвергается раньше
        // проверки типа файла, и тест проходил бы, даже если бы обращение со
        // ссылками сломалось. Посторонние имена проверяются отдельно.
        std::fs::write(format!("{wal}/000001.log"), b"").unwrap();
        assert!(files(&dir).is_empty(), "ссылка попала в журнал");
        assert!(open_tail(&dir).is_err(), "писатель принял symlink хвоста");
        assert_eq!(std::fs::read(&outside).unwrap(), b"do not touch");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Посторонний файл — отказ, а свой собственный мусор — нет.
    ///
    /// Обе половины одинаково важны. Пропуск чужого имени скрывал бы потерю
    /// подтверждённых записей (`1000000.log` после миллионного отката читался
    /// как «не журнал»). Но отказ на **своей** же временной копии отметки,
    /// оставшейся после падения посреди `atomic_write`, запер бы узел снаружи
    /// его собственного индекса.
    #[test]
    fn a_stray_file_is_refused_while_our_own_leftovers_are_not() {
        let dir = tmp("stray");
        let wal = dir_of(&dir);
        std::fs::create_dir_all(&wal).unwrap();
        std::fs::write(format!("{wal}/000000.log"), b"").unwrap();
        set_checkpoint(&dir, 7).unwrap();
        // Мусор ровно того вида, что оставляет `fs::atomic_write`.
        std::fs::write(format!("{wal}/.{CHECKPOINT}.tmp-1234-5"), b"7").unwrap();
        assert!(files_checked(&dir).is_ok(), "свой же временный файл заперся снаружи");
        assert_eq!(checkpoint(&dir), 7);

        std::fs::write(format!("{wal}/not-a-number.log"), b"junk").unwrap();
        let refused = files_checked(&dir).expect_err("посторонний файл пропущен");
        assert!(
            format!("{refused}").contains("not-a-number.log"),
            "отказ не называет файл: {refused}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Переход через миллион: писатель заводит имя, которое читатель находит.
    ///
    /// Прежде здесь узел терял записи молча — писатель создавал семизначное имя,
    /// читатель принимал только шестизначные, и `existing.last()` навсегда
    /// оставался на `999999.log`.
    #[test]
    fn the_millionth_rollover_stays_visible_to_the_reader() {
        let dir = tmp("million");
        let wal = dir_of(&dir);
        std::fs::create_dir_all(&wal).unwrap();
        // Предыдущий файл дорос до порога отката — писатель обязан завести
        // следующий, и это ровно тот номер, на котором менялась ширина имени.
        let full = format!("{wal}/{}", file_name(999_999));
        let mut record = Vec::new();
        append(&mut record, 1, ADD, &vec![0; ROLL_BYTES as usize - HEADER]);
        assert_eq!(record.len() as u64, ROLL_BYTES);
        std::fs::write(&full, record).unwrap();

        let (_file, len) = open_tail(&dir).expect("новый файл журнала");
        assert_eq!(len, 0, "писатель дописал в переполненный файл");
        assert!(
            std::path::Path::new(&format!("{wal}/1000000.log")).exists(),
            "писатель не завёл файл за миллионом"
        );
        let seen = files_checked(&dir).expect("каталог журнала");
        assert_eq!(seen.len(), 2, "читатель не увидел оба файла: {seen:?}");
        assert!(seen[1].ends_with("1000000.log"), "порядок не численный: {seen:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn oversized_wal_is_refused_before_memory_reservation() {
        let dir = tmp("oversized");
        let wal = dir_of(&dir);
        std::fs::create_dir_all(&wal).unwrap();
        let path = format!("{wal}/000000.log");
        std::fs::File::create(&path).unwrap().set_len(MAX_FILE_BYTES + 1).unwrap();
        assert!(read_all(&path).is_err());
        assert!(for_replay(&dir, |_| Ok(())).is_err());
        assert!(open_tail(&dir).is_err(), "переполнение не должно прятаться за новым файлом");
        assert_eq!(files_checked(&dir).unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_torn_full_tail_is_repaired_before_rotation() {
        use std::io::Write;
        let dir = tmp("torn-full");
        let (mut file, _) = open_tail(&dir).unwrap();
        let mut first = Vec::new();
        append(&mut first, 1, ADD, b"first");
        file.write_all(&first).unwrap();
        file.set_len(ROLL_BYTES).unwrap();
        drop(file);

        let (mut file, len) = open_tail(&dir).unwrap();
        assert_eq!(len, first.len() as u64);
        assert_eq!(files_checked(&dir).unwrap().len(), 1);
        let mut next = Vec::new();
        append(&mut next, 2, ADD, b"next");
        file.write_all(&next).unwrap();
        file.sync_all().unwrap();
        let mut seqs = Vec::new();
        let recovered = for_replay(&dir, |record| {
            seqs.push(record.seq);
            Ok(())
        })
        .unwrap();
        assert_eq!(seqs, [1, 2]);
        assert_eq!(recovered.dropped, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn sync_failures_are_returned_and_retry_keeps_the_tail_recoverable() {
        const CHILD: &str = "SVINDB_WAL_SYNC_FAILURE_CHILD";
        if let Ok(mode) = std::env::var(CHILD) {
            use std::io::Write;
            let dir = tmp(&format!("sync-failure-{mode}"));
            let error = open_tail(&dir).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
            assert_eq!(files_checked(&dir).unwrap().len(), 1);
            let (mut file, len) = open_tail(&dir).unwrap();
            assert_eq!(len, 0);
            let mut record = Vec::new();
            append(&mut record, 1, ADD, b"after retry");
            file.write_all(&record).unwrap();
            file.sync_all().unwrap();
            assert_eq!(for_replay(&dir, |_| Ok(())).unwrap().to, 1);
            std::fs::remove_dir_all(&dir).unwrap();
            return;
        }
        for (mode, point) in [
            ("file", "wal.before_new_file_fsync=enospc,count=1"),
            ("directory", "wal.before_directory_fsync=enospc,after=1,count=1"),
        ] {
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "wal::tests::sync_failures_are_returned_and_retry_keeps_the_tail_recoverable",
                    "--nocapture",
                ])
                .env(CHILD, mode)
                .env("SVINDB_FAILPOINTS", point)
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{mode}: {}\n{}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }

    /// Обрезка убирает файлы целиком и не трогает тот, в который ещё пишут.
    #[test]
    fn trimming_removes_whole_files_only() {
        let dir = tmp("trim");
        let d = dir_of(&dir);
        std::fs::create_dir_all(&d).unwrap();
        let mut a = Vec::new();
        for i in 1..=3u64 {
            append(&mut a, i, ADD, &encode_strings(&["x"]));
        }
        let mut b = Vec::new();
        for i in 4..=6u64 {
            append(&mut b, i, ADD, &encode_strings(&["x"]));
        }
        std::fs::write(format!("{d}/000000.log"), &a).unwrap();
        std::fs::write(format!("{d}/000001.log"), &b).unwrap();

        assert_eq!(trim(&dir, 3).unwrap(), 1, "первый файл целиком до отметки");
        assert_eq!(files(&dir).len(), 1);
        assert_eq!(trim(&dir, 3).unwrap(), 0, "второй ещё нужен");

        // Последний файл не удаляется **ни при какой отметке**: в него пишет
        // писатель, и на Unix удаление открытого файла не мешает записи — она
        // идёт в отвязанный inode и успевает ответить «принято». Пропадает всё
        // это при падении, то есть ровно тогда, когда журнал и нужен.
        assert_eq!(trim(&dir, 6).unwrap(), 0, "в последний файл ещё пишут");
        assert_eq!(files(&dir).len(), 1, "последний файл обязан уцелеть");

        std::fs::write(format!("{d}/000002.log"), Vec::<u8>::new()).unwrap();
        assert_eq!(trim(&dir, 6).unwrap(), 1, "теперь можно");
        assert_eq!(files(&dir).len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
