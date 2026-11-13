//! Состав корня: физические индексы, их состояние, блоки, метаданные и aliases.
//!
//! ## Почему это отдельный слой, а не поля в `index.json`
//!
//! Пока каталогом служил листинг `indices/`, состав был выводим из диска, и
//! этого хватало. Alias вывести из листинга нельзя — у него нет иного
//! представления, кроме того, которое мы заведём сами. А как только состав
//! перестаёт быть выводимым, у него появляется **поколение**: набор привязок
//! обязан меняться целиком, иначе поиск успеет увидеть половину переключения
//! rollover.
//!
//! Отсюда форма: неизменяемый [`Catalog`] на каждое поколение и транзакция,
//! которая durable-записывается **до** публикации в память. Запрос берёт один
//! снимок и живёт на нём весь свой срок; он не может увидеть цели одного
//! поколения и блоки другого.
//!
//! ## Что здесь намеренно не хранится
//!
//! Ничего производного от содержимого индекса: числа документов, размеры,
//! сегменты. Они читаются у живого runtime. Каталог отвечает за то, что
//! **нельзя** пересчитать, потеряв.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::Node;
use svindb_core::rootlog::Rec;

// ───────────────────────────── пределы ─────────────────────────────

/// Потолок описания. Восемь килобайт — это уже не подпись, а документ.
pub const MAX_DESCRIPTION: usize = 8 * 1024;
/// Сколько тегов допускается на индекс.
pub const MAX_TAGS: usize = 64;
/// Границы длины одного тега.
pub const MAX_TAG_BYTES: usize = 128;
/// Потолок всей записи метаданных на диске.
pub const MAX_META_RECORD: usize = 16 * 1024;
/// Потолок имени alias — тот же, что у имени индекса.
pub const MAX_NAME: usize = 255;

// ─────────────────────────── виды записей ───────────────────────────

/// Индекс в теле снимка.
pub const SNAP_INDEX: u8 = 0x01;
/// Alias в теле снимка.
pub const SNAP_ALIAS: u8 = 0x02;
/// Появление индекса в каталоге.
pub const TXN_INDEX_ADD: u8 = 0x10;
/// Исчезновение индекса из каталога.
pub const TXN_INDEX_REMOVE: u8 = 0x11;
/// Смена состояния индекса.
pub const TXN_INDEX_STATE: u8 = 0x12;
/// Смена набора блоков.
pub const TXN_INDEX_BLOCKS: u8 = 0x13;
/// Смена описания и тегов.
pub const TXN_INDEX_META: u8 = 0x14;
/// Завершение CREATE: открытая запись и её aliases публикуются вместе.
pub const TXN_INDEX_PUBLISH: u8 = 0x15;
/// Атомарная правка привязок нескольких aliases и удаление индексов.
pub const TXN_ALIAS_BATCH: u8 = 0x20;
/// Rollover: новый индекс, переезд признака записи и блок на старом.
pub const TXN_ROLLOVER: u8 = 0x30;

// ───────────────────────────── UUID ─────────────────────────────

/// Устойчивое имя физического индекса, не зависящее от того, как его назвали.
///
/// Привязки alias указывают на UUID, а не на имя. Именно это делает
/// однозначными и rollover, и «удалить, потом создать заново с тем же именем»:
/// новый индекс — другой индекс, и alias не наследует его молча.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// Выпускает новый UUID версии 4 из энтропии ядра.
    ///
    /// Через [`svindb_core::hash::try_entropy`], а не `entropy`: выпуск идёт
    /// внутри `PUT /{индекс}`, то есть на обычном пути запроса, и при
    /// `panic = "abort"` паника унесла бы весь процесс со всеми остальными
    /// индексами вместо отказа одному запросу.
    pub fn mint() -> Result<Uuid, CatalogError> {
        let bytes = svindb_core::hash::try_entropy(16).map_err(|e| CatalogError::ResourceBusy {
            why: format!("случайность недоступна: {e}"),
        })?;
        let mut raw = [0u8; 16];
        raw.copy_from_slice(&bytes);
        raw[6] = (raw[6] & 0x0F) | 0x40;
        raw[8] = (raw[8] & 0x3F) | 0x80;
        Ok(Uuid(raw))
    }

    #[cfg(test)]
    pub fn from_bytes(raw: [u8; 16]) -> Uuid {
        Uuid(raw)
    }

    pub fn bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Нулевой UUID — «его нет».
    ///
    /// Так выглядит запись корня формата 1, открытого только на чтение: там
    /// UUID ещё не выданы, а выдуманный в памяти менялся бы от запуска к
    /// запуску и был бы хуже честно отсутствующего.
    pub fn is_nil(&self) -> bool {
        self.0 == [0u8; 16]
    }

    /// Каноническое представление 8-4-4-4-12 строчными шестнадцатеричными.
    pub fn text(&self) -> String {
        let mut out = String::with_capacity(36);
        for (i, b) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                out.push('-');
            }
            use std::fmt::Write as _;
            let _ = write!(&mut out, "{b:02x}");
        }
        out
    }

    /// Разбирает каноническое представление. Нужен `_metadata` и тестам.
    pub fn parse(text: &str) -> Option<Uuid> {
        let bytes = text.as_bytes();
        if bytes.len() != 36
            || [8usize, 13, 18, 23].iter().any(|&at| bytes.get(at) != Some(&b'-'))
            || bytes.iter().enumerate().any(|(at, byte)| {
                !matches!(at, 8 | 13 | 18 | 23) && !matches!(*byte, b'0'..=b'9' | b'a'..=b'f')
            })
        {
            return None;
        }
        let mut raw = [0u8; 16];
        let mut at = 0usize;
        let mut hi: Option<u8> = None;
        for ch in text.chars() {
            if ch == '-' {
                continue;
            }
            let v = ch.to_digit(16)? as u8;
            match hi {
                None => hi = Some(v),
                Some(h) => {
                    if at >= 16 {
                        return None;
                    }
                    raw[at] = (h << 4) | v;
                    at += 1;
                    hi = None;
                }
            }
        }
        (at == 16 && hi.is_none()).then_some(Uuid(raw))
    }
}

// ───────────────────────────── блоки ─────────────────────────────

/// Набор запретов на индексе.
///
/// Именно набор, а не состояние: `PUT /_block/write` добавляет бит, а не
/// заменяет набор. Пока это было «текущим блоком», второй блок молча снимал
/// первый — и администратор, поставивший `read`, обнаруживал снятым `write`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Blocks(u8);

impl Blocks {
    pub const WRITE: u8 = 0x01;
    pub const READ: u8 = 0x02;
    pub const METADATA: u8 = 0x04;
    pub const READ_ONLY_ALLOW_DELETE: u8 = 0x08;
    /// Все известные этой сборке биты. Остальные обязаны быть нулём.
    const KNOWN: u8 = 0x0F;

    pub fn none() -> Blocks {
        Blocks(0)
    }

    pub fn from_bits(bits: u8) -> Option<Blocks> {
        // Неизвестный бит — отказ, а не маскирование: он поставлен более новой
        // версией, и трактовать «запрещено» как «разрешено» нельзя.
        (bits & !Self::KNOWN == 0).then_some(Blocks(bits))
    }

    pub fn bits(&self) -> u8 {
        self.0
    }

    pub fn has(&self, mask: u8) -> bool {
        self.0 & mask != 0
    }

    pub fn with(&self, mask: u8) -> Blocks {
        Blocks(self.0 | mask)
    }

    pub fn without(&self, mask: u8) -> Blocks {
        Blocks(self.0 & !mask)
    }

    /// Имя блока из запроса в маску.
    ///
    /// `read_only` — не отдельный бит, а объединение: так «блоки складываются»
    /// остаётся правдой и для него, и снятие `write` не оставляет половину
    /// `read_only` в силе незаметно.
    pub fn mask_of(name: &str) -> Option<u8> {
        match name {
            "write" => Some(Self::WRITE),
            "read" => Some(Self::READ),
            "metadata" => Some(Self::METADATA),
            "read_only" => Some(Self::WRITE | Self::METADATA),
            "read_only_allow_delete" => Some(Self::READ_ONLY_ALLOW_DELETE | Self::WRITE),
            _ => None,
        }
    }

    /// Перечисление имён для выдачи.
    pub fn names(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.has(Self::WRITE) {
            out.push("write");
        }
        if self.has(Self::READ) {
            out.push("read");
        }
        if self.has(Self::METADATA) {
            out.push("metadata");
        }
        if self.has(Self::READ_ONLY_ALLOW_DELETE) {
            out.push("read_only_allow_delete");
        }
        out
    }
}

// ──────────────────────────── состояние ────────────────────────────

/// Состояние физического индекса.
///
/// Переходные состояния существуют не для красоты: они и есть то, что делает
/// аварию посреди операции восстановимой однозначно. Внешнему запросу они не
/// видны никогда — [`Catalog::resolve`] и обработчики отвечают по ним отказом.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    /// Runtime поднят, поиск и запись возможны с точностью до блоков.
    Open,
    /// Runtime выгружен. Поиск и запись отвечают `index_closed_exception`.
    Closed,
    /// Место под именем занято, каталог на диске ещё не проверен.
    Creating,
    /// Индекс исключён из выдачи, данные ещё не сняты.
    Deleting,
    /// Runtime поднимается, но ещё не опубликован.
    Opening,
    /// Runtime сбрасывается и выгружается.
    Closing,
}

impl State {
    fn code(self) -> u8 {
        match self {
            State::Open => 0,
            State::Closed => 1,
            State::Creating => 2,
            State::Deleting => 3,
            State::Opening => 4,
            State::Closing => 5,
        }
    }

    fn from_code(code: u8) -> Option<State> {
        Some(match code {
            0 => State::Open,
            1 => State::Closed,
            2 => State::Creating,
            3 => State::Deleting,
            4 => State::Opening,
            5 => State::Closing,
            _ => return None,
        })
    }

    /// Видно ли это состояние снаружи вообще.
    pub fn is_settled(self) -> bool {
        matches!(self, State::Open | State::Closed)
    }

    pub fn name(self) -> &'static str {
        match self {
            State::Open => "open",
            State::Closed => "close",
            State::Creating => "creating",
            State::Deleting => "deleting",
            State::Opening => "opening",
            State::Closing => "closing",
        }
    }
}

// ───────────────────────────── записи ─────────────────────────────

/// Одна запись каталога о физическом индексе.
#[derive(Clone)]
pub struct Entry {
    pub uuid: Uuid,
    pub name: String,
    pub state: State,
    pub blocks: Blocks,
    pub created_ms: u64,
    pub description: Option<String>,
    pub tags: Vec<String>,
    /// Живой runtime. `Some` только у по-настоящему открытого индекса.
    ///
    /// Закрытое поколение стоит здесь ровно столько, сколько занимают его имя и
    /// теги: ни `Node`, ни writer, ни mmap за ним не стоят.
    pub node: Option<Arc<Node>>,
}

/// Печатается всё, кроме runtime: `Node` держит сегменты, writer и mmap, и
/// его развёрнутый вид в сообщении об ошибке был бы мегабайтами шума. Важно
/// одно — поднят он или нет.
impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("uuid", &self.uuid.text())
            .field("name", &self.name)
            .field("state", &self.state)
            .field("blocks", &self.blocks.names())
            .field("created_ms", &self.created_ms)
            .field("description", &self.description)
            .field("tags", &self.tags)
            .field("loaded", &self.node.is_some())
            .finish()
    }
}

/// Привязка alias к физическому индексу.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Binding {
    pub target: Uuid,
    /// `None` — признак не задан; у единственного target это неявный writer.
    /// `Some(false)` запрещает даже это неявное направление записи.
    pub is_write: Option<bool>,
}

// ─────────────────────────── кодек тела ───────────────────────────

struct Cur<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cur<'a> {
    fn new(bytes: &'a [u8]) -> Cur<'a> {
        Cur { bytes, at: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).ok_or("длина переполнила размер")?;
        if end > self.bytes.len() {
            return Err(format!("нужно {n} байт, осталось {}", self.bytes.len() - self.at));
        }
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("четыре байта")))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("восемь байт")))
    }

    fn uuid(&mut self) -> Result<Uuid, String> {
        let mut raw = [0u8; 16];
        raw.copy_from_slice(self.take(16)?);
        Ok(Uuid(raw))
    }

    fn text(&mut self, limit: usize) -> Result<String, String> {
        let len = self.u32()? as usize;
        if len > limit {
            return Err(format!("строка длиной {len}, потолок {limit}"));
        }
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| "строка не в UTF-8".to_string())
    }

    fn opt_text(&mut self, limit: usize) -> Result<Option<String>, String> {
        // Отдельный байт присутствия: отсутствующее описание и пустое — разные
        // состояния, и склеить их значило бы потерять смысл `description:null`.
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.text(limit)?)),
            other => Err(format!("признак присутствия {other}, а бывает 0 или 1")),
        }
    }

    fn write_flag(&mut self) -> Result<Option<bool>, String> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(false)),
            2 => Ok(Some(true)),
            other => Err(format!("признак is_write_index имеет неизвестный код {other}")),
        }
    }

    fn done(&self) -> Result<(), String> {
        if self.at != self.bytes.len() {
            return Err(format!(
                "после записи осталось {} лишних байт",
                self.bytes.len() - self.at
            ));
        }
        Ok(())
    }
}

fn put_text(out: &mut Vec<u8>, text: &str) {
    out.extend((text.len() as u32).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
}

fn put_opt_text(out: &mut Vec<u8>, text: Option<&str>) {
    match text {
        None => out.push(0),
        Some(v) => {
            out.push(1);
            put_text(out, v);
        }
    }
}

fn put_write_flag(out: &mut Vec<u8>, value: Option<bool>) {
    out.push(match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    });
}

fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(entry.uuid.bytes());
    put_text(&mut out, &entry.name);
    out.push(entry.state.code());
    out.push(entry.blocks.bits());
    out.extend(entry.created_ms.to_le_bytes());
    put_opt_text(&mut out, entry.description.as_deref());
    out.extend((entry.tags.len() as u32).to_le_bytes());
    for tag in &entry.tags {
        put_text(&mut out, tag);
    }
    out
}

fn decode_entry(cur: &mut Cur) -> Result<Entry, String> {
    let uuid = cur.uuid()?;
    let name = cur.text(MAX_NAME)?;
    let state = State::from_code(cur.u8()?).ok_or("неизвестное состояние индекса")?;
    let blocks = Blocks::from_bits(cur.u8()?).ok_or("неизвестный блок индекса")?;
    let created_ms = cur.u64()?;
    let description = cur.opt_text(MAX_DESCRIPTION)?;
    let count = cur.u32()? as usize;
    if count > MAX_TAGS {
        return Err(format!("тегов {count}, потолок {MAX_TAGS}"));
    }
    let mut tags = Vec::with_capacity(count);
    for _ in 0..count {
        tags.push(cur.text(MAX_TAG_BYTES)?);
    }
    Ok(Entry { uuid, name, state, blocks, created_ms, description, tags, node: None })
}

/// Одна правка одного alias: итоговый набор привязок.
///
/// Записывается **результат**, а не «добавить/убрать». Так повтор записи
/// идемпотентен, порядок внутри пачки не важен, и восстановление не обязано
/// знать, каким был набор до неё.
#[derive(Clone)]
pub struct AliasSet {
    pub alias: String,
    pub bindings: Vec<Binding>,
}

/// Содержимое [`TXN_ALIAS_BATCH`].
#[derive(Clone, Default)]
pub struct AliasBatch {
    pub aliases: Vec<AliasSet>,
    pub removed: Vec<Uuid>,
}

impl AliasBatch {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend((self.aliases.len() as u32).to_le_bytes());
        for set in &self.aliases {
            put_text(&mut out, &set.alias);
            out.extend((set.bindings.len() as u32).to_le_bytes());
            for b in &set.bindings {
                out.extend_from_slice(b.target.bytes());
                put_write_flag(&mut out, b.is_write);
            }
        }
        out.extend((self.removed.len() as u32).to_le_bytes());
        for uuid in &self.removed {
            out.extend_from_slice(uuid.bytes());
        }
        out
    }

    fn decode(cur: &mut Cur) -> Result<AliasBatch, String> {
        let na = cur.u32()? as usize;
        let mut aliases = Vec::with_capacity(na.min(1024));
        for _ in 0..na {
            let alias = cur.text(MAX_NAME)?;
            let n = cur.u32()? as usize;
            let mut bindings = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                bindings.push(Binding { target: cur.uuid()?, is_write: cur.write_flag()? });
            }
            aliases.push(AliasSet { alias, bindings });
        }
        let nr = cur.u32()? as usize;
        let mut removed = Vec::with_capacity(nr.min(1024));
        for _ in 0..nr {
            removed.push(cur.uuid()?);
        }
        Ok(AliasBatch { aliases, removed })
    }
}

/// Содержимое [`TXN_ROLLOVER`] — всё переключение одной записью.
#[derive(Clone)]
pub struct Rollover {
    pub fresh: Entry,
    pub alias: String,
    pub bindings: Vec<Binding>,
    pub retired: Uuid,
    pub retired_blocks: Blocks,
}

/// Финальная публикация обычного CREATE с aliases из его тела.
#[derive(Clone)]
pub struct CreatePublish {
    pub entry: Entry,
    pub aliases: Vec<AliasSet>,
}

impl CreatePublish {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = encode_entry(&self.entry);
        out.extend((self.aliases.len() as u32).to_le_bytes());
        for set in &self.aliases {
            put_text(&mut out, &set.alias);
            out.extend((set.bindings.len() as u32).to_le_bytes());
            for binding in &set.bindings {
                out.extend_from_slice(binding.target.bytes());
                put_write_flag(&mut out, binding.is_write);
            }
        }
        out
    }

    fn decode(cur: &mut Cur) -> Result<CreatePublish, String> {
        let entry = decode_entry(cur)?;
        let count = cur.u32()? as usize;
        let mut aliases = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let alias = cur.text(MAX_NAME)?;
            let n = cur.u32()? as usize;
            let mut bindings = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                bindings.push(Binding { target: cur.uuid()?, is_write: cur.write_flag()? });
            }
            aliases.push(AliasSet { alias, bindings });
        }
        Ok(CreatePublish { entry, aliases })
    }
}

impl Rollover {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = encode_entry(&self.fresh);
        put_text(&mut out, &self.alias);
        out.extend((self.bindings.len() as u32).to_le_bytes());
        for b in &self.bindings {
            out.extend_from_slice(b.target.bytes());
            put_write_flag(&mut out, b.is_write);
        }
        out.extend_from_slice(self.retired.bytes());
        out.push(self.retired_blocks.bits());
        out
    }

    fn decode(cur: &mut Cur) -> Result<Rollover, String> {
        let fresh = decode_entry(cur)?;
        let alias = cur.text(MAX_NAME)?;
        let n = cur.u32()? as usize;
        let mut bindings = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            bindings.push(Binding { target: cur.uuid()?, is_write: cur.write_flag()? });
        }
        let retired = cur.uuid()?;
        let retired_blocks = Blocks::from_bits(cur.u8()?).ok_or("неизвестный блок индекса")?;
        Ok(Rollover { fresh, alias, bindings, retired, retired_blocks })
    }
}

/// Сборщики тел транзакций.
pub mod body {
    use super::*;

    pub fn index_add(entry: &Entry) -> Vec<u8> {
        encode_entry(entry)
    }

    pub fn index_remove(uuid: &Uuid) -> Vec<u8> {
        uuid.bytes().to_vec()
    }

    pub fn index_state(uuid: &Uuid, state: State) -> Vec<u8> {
        let mut out = uuid.bytes().to_vec();
        out.push(state.code());
        out
    }

    pub fn index_blocks(uuid: &Uuid, blocks: Blocks) -> Vec<u8> {
        let mut out = uuid.bytes().to_vec();
        out.push(blocks.bits());
        out
    }

    pub fn index_meta(uuid: &Uuid, description: Option<&str>, tags: &[String]) -> Vec<u8> {
        let mut out = uuid.bytes().to_vec();
        put_opt_text(&mut out, description);
        out.extend((tags.len() as u32).to_le_bytes());
        for tag in tags {
            put_text(&mut out, tag);
        }
        out
    }
}

// ───────────────────────────── каталог ─────────────────────────────

/// Неизменяемый состав корня на одном поколении.
#[derive(Clone, Default)]
pub struct Catalog {
    generation: u64,
    entries: Vec<Entry>,
    by_name: BTreeMap<String, usize>,
    by_uuid: BTreeMap<Uuid, usize>,
    aliases: BTreeMap<String, Vec<Binding>>,
}

impl Catalog {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
    }

    /// Все записи, включая переходные и закрытые.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn aliases(&self) -> &BTreeMap<String, Vec<Binding>> {
        &self.aliases
    }

    /// Привязки одного alias в устойчивом порядке UUID.
    pub fn bindings(&self, alias: &str) -> Option<&[Binding]> {
        self.aliases.get(alias).map(Vec::as_slice)
    }

    pub fn by_name(&self, name: &str) -> Option<&Entry> {
        self.by_name.get(name).and_then(|ix| self.entries.get(*ix))
    }

    pub fn by_uuid(&self, uuid: &Uuid) -> Option<&Entry> {
        self.by_uuid.get(uuid).and_then(|ix| self.entries.get(*ix))
    }

    /// Сколько индексов держат живой runtime.
    ///
    /// Именно это, а не число записей, ограничивает `--max-indices`: предел
    /// защищает память, потоки и дескрипторы процесса, а закрытое поколение не
    /// тратит ничего из этого. ADR-0003 обещал такую трактовку с самого начала.
    pub fn open_count(&self) -> usize {
        self.entries.iter().filter(|e| e.node.is_some()).count()
    }

    /// Занято ли имя — индексом или alias.
    ///
    /// Одно пространство имён обязательно: быстрый путь запроса сначала
    /// спрашивает индекс по имени и только при промахе идёт в резолвер, так что
    /// совпадение молча затенило бы alias.
    pub fn name_taken(&self, name: &str) -> bool {
        self.by_name.contains_key(name) || self.aliases.contains_key(name)
    }

    /// Текущий индекс записи alias.
    ///
    /// У alias с одной целью признак записи подразумевается — так ведёт себя
    /// Elasticsearch, и ломать это на ровном месте незачем. У alias с
    /// несколькими целями подразумевать нечего: «первый открытый» — это
    /// правдоподобная ложь, которая однажды напишет в архив.
    pub fn write_target(&self, alias: &str) -> Result<&Entry, CatalogError> {
        let bindings = self
            .aliases
            .get(alias)
            .ok_or_else(|| CatalogError::AliasNotFound { alias: alias.to_string() })?;
        let explicit: Vec<&Binding> =
            bindings.iter().filter(|b| b.is_write == Some(true)).collect();
        let chosen = match (explicit.len(), bindings.len()) {
            (1, _) => explicit[0],
            (0, 1) if bindings[0].is_write != Some(false) => &bindings[0],
            (0, _) => return Err(CatalogError::NoWriteIndex { alias: alias.to_string() }),
            _ => return Err(CatalogError::MultipleWriteIndices { alias: alias.to_string() }),
        };
        self.by_uuid(&chosen.target).ok_or_else(|| CatalogError::AliasTargetNotFound {
            alias: alias.to_string(),
            target: chosen.target.text(),
        })
    }

    /// Является ли индекс индексом записи хотя бы одного alias.
    pub fn is_write_index(&self, uuid: &Uuid) -> Option<String> {
        for (alias, bindings) in &self.aliases {
            let explicit = bindings.iter().filter(|b| b.is_write == Some(true)).count();
            let hit = match (explicit, bindings.len()) {
                (1, _) => bindings.iter().any(|b| b.is_write == Some(true) && b.target == *uuid),
                (0, 1) => bindings[0].is_write != Some(false) && bindings[0].target == *uuid,
                _ => false,
            };
            if hit {
                return Some(alias.clone());
            }
        }
        None
    }

    /// Все aliases, указывающие на индекс.
    pub fn aliases_of(&self, uuid: &Uuid) -> Vec<&str> {
        self.aliases
            .iter()
            .filter(|(_, b)| b.iter().any(|x| x.target == *uuid))
            .map(|(a, _)| a.as_str())
            .collect()
    }

    fn reindex(&mut self) {
        self.by_name.clear();
        self.by_uuid.clear();
        for (ix, entry) in self.entries.iter().enumerate() {
            self.by_name.insert(entry.name.clone(), ix);
            if !entry.uuid.is_nil() {
                self.by_uuid.insert(entry.uuid, ix);
            }
        }
    }

    fn put(&mut self, entry: Entry) {
        let existing = if entry.uuid.is_nil() {
            self.by_name.get(&entry.name)
        } else {
            self.by_uuid.get(&entry.uuid)
        };
        match existing {
            Some(ix) => {
                let ix = *ix;
                self.entries[ix] = entry;
                self.reindex();
            }
            None => {
                self.entries.push(entry);
                self.reindex();
            }
        }
    }

    fn drop_uuid(&mut self, uuid: &Uuid) {
        self.entries.retain(|e| e.uuid != *uuid);
        for bindings in self.aliases.values_mut() {
            bindings.retain(|b| b.target != *uuid);
        }
        self.aliases.retain(|_, b| !b.is_empty());
        self.reindex();
    }

    /// Привязывает поднятый runtime к записи по имени.
    ///
    /// Присутствие `Node` не durable и потому не проходит через журнал: оно
    /// описывает не состав корня, а то, что этот процесс успел открыть.
    pub fn attach(&mut self, name: &str, node: Arc<Node>) -> Result<(), String> {
        let ix = *self
            .by_name
            .get(name)
            .ok_or_else(|| format!("индекса {name:?} нет в каталоге состава"))?;
        let entry = self.entries.get_mut(ix).ok_or("повреждён указатель записи")?;
        entry.node = Some(node);
        Ok(())
    }

    /// Отвязывает runtime, возвращая его вызывающему для окончательного `drop`.
    pub fn detach(&mut self, name: &str) -> Result<Option<Arc<Node>>, String> {
        let ix = *self
            .by_name
            .get(name)
            .ok_or_else(|| format!("индекса {name:?} нет в каталоге состава"))?;
        let entry = self.entries.get_mut(ix).ok_or("повреждён указатель записи")?;
        Ok(entry.node.take())
    }

    fn entry_mut(&mut self, uuid: &Uuid) -> Result<&mut Entry, String> {
        let ix = *self.by_uuid.get(uuid).ok_or_else(|| {
            format!("запись про индекс {} не встречалась раньше ссылки на него", uuid.text())
        })?;
        self.entries.get_mut(ix).ok_or_else(|| "повреждён указатель записи".to_string())
    }

    fn set_alias(&mut self, alias: String, bindings: Vec<Binding>) {
        if bindings.is_empty() {
            self.aliases.remove(&alias);
        } else {
            self.aliases.insert(alias, bindings);
        }
    }

    fn validate_aliases(&self) -> Result<(), String> {
        for (alias, bindings) in &self.aliases {
            if self.by_name.contains_key(alias) {
                return Err(format!("имя alias {alias:?} совпадает с именем физического индекса"));
            }
            let mut seen = std::collections::BTreeSet::new();
            let mut writers = 0usize;
            for binding in bindings {
                if !seen.insert(binding.target) {
                    return Err(format!(
                        "alias {alias:?} содержит target {} дважды",
                        binding.target.text()
                    ));
                }
                if self.by_uuid(&binding.target).is_none() {
                    return Err(format!(
                        "alias {alias:?} указывает на отсутствующий target {}",
                        binding.target.text()
                    ));
                }
                writers += usize::from(binding.is_write == Some(true));
            }
            if writers > 1 {
                return Err(format!("у alias {alias:?} {writers} явных write indices"));
            }
        }
        Ok(())
    }

    /// Применяет одну запись — снимка или журнала.
    ///
    /// Неизвестный вид — жёсткая ошибка, а не пропуск, по той же причине, что и
    /// в WAL: журнал писала более новая версия узла, и молча продолжить значило
    /// бы потерять привязку, про которую эта сборка не знает.
    pub fn apply(&mut self, kind: u8, body: &[u8]) -> Result<(), String> {
        let mut cur = Cur::new(body);
        match kind {
            SNAP_INDEX | TXN_INDEX_ADD => {
                let entry = decode_entry(&mut cur)?;
                cur.done()?;
                self.put(entry);
            }
            SNAP_ALIAS => {
                let alias = cur.text(MAX_NAME)?;
                let binding = Binding { target: cur.uuid()?, is_write: cur.write_flag()? };
                cur.done()?;
                self.aliases.entry(alias).or_default().push(binding);
            }
            TXN_INDEX_REMOVE => {
                let uuid = cur.uuid()?;
                cur.done()?;
                self.drop_uuid(&uuid);
            }
            TXN_INDEX_STATE => {
                let uuid = cur.uuid()?;
                let state = State::from_code(cur.u8()?).ok_or("неизвестное состояние индекса")?;
                cur.done()?;
                self.entry_mut(&uuid)?.state = state;
            }
            TXN_INDEX_BLOCKS => {
                let uuid = cur.uuid()?;
                let blocks = Blocks::from_bits(cur.u8()?).ok_or("неизвестный блок индекса")?;
                cur.done()?;
                self.entry_mut(&uuid)?.blocks = blocks;
            }
            TXN_INDEX_META => {
                let uuid = cur.uuid()?;
                let description = cur.opt_text(MAX_DESCRIPTION)?;
                let count = cur.u32()? as usize;
                if count > MAX_TAGS {
                    return Err(format!("тегов {count}, потолок {MAX_TAGS}"));
                }
                let mut tags = Vec::with_capacity(count);
                for _ in 0..count {
                    tags.push(cur.text(MAX_TAG_BYTES)?);
                }
                cur.done()?;
                let entry = self.entry_mut(&uuid)?;
                entry.description = description;
                entry.tags = tags;
            }
            TXN_INDEX_PUBLISH => {
                let publish = CreatePublish::decode(&mut cur)?;
                cur.done()?;
                self.put(publish.entry);
                for set in publish.aliases {
                    self.set_alias(set.alias, set.bindings);
                }
            }
            TXN_ALIAS_BATCH => {
                let batch = AliasBatch::decode(&mut cur)?;
                cur.done()?;
                for set in batch.aliases {
                    self.set_alias(set.alias, set.bindings);
                }
                for uuid in batch.removed {
                    self.drop_uuid(&uuid);
                }
            }
            TXN_ROLLOVER => {
                let roll = Rollover::decode(&mut cur)?;
                cur.done()?;
                self.put(roll.fresh);
                self.set_alias(roll.alias, roll.bindings);
                self.entry_mut(&roll.retired)?.blocks = roll.retired_blocks;
            }
            other => {
                return Err(format!(
                    "запись каталога вида {other:#04x} неизвестна — журнал писала более новая \
                     версия узла; продолжить значило бы молча потерять её смысл"
                ))
            }
        }
        self.validate_aliases()
    }

    /// Раскладывает состояние в записи снимка для уплотнения.
    pub fn to_base(&self) -> Vec<Rec> {
        let mut out = Vec::with_capacity(self.entries.len() + self.aliases.len());
        for entry in &self.entries {
            out.push(Rec { kind: SNAP_INDEX, body: encode_entry(entry) });
        }
        for (alias, bindings) in &self.aliases {
            for b in bindings {
                let mut body = Vec::new();
                put_text(&mut body, alias);
                body.extend_from_slice(b.target.bytes());
                put_write_flag(&mut body, b.is_write);
                out.push(Rec { kind: SNAP_ALIAS, body });
            }
        }
        out
    }
}

// ────────────────────────── проверка метаданных ──────────────────────────

/// Приводит теги к каноническому виду и проверяет пределы.
///
/// Регистр не трогается: `Hot` и `hot` — разные теги, потому что тег
/// пользовательский, и «догадливое» приведение однажды склеит два разных.
/// Повторы убираются с сохранением порядка первого появления.
pub fn normalize_tags(raw: &[String]) -> Result<Vec<String>, CatalogError> {
    let mut out: Vec<String> = Vec::with_capacity(raw.len().min(MAX_TAGS));
    for tag in raw {
        if tag.is_empty() || tag.len() > MAX_TAG_BYTES {
            return Err(CatalogError::LimitExceeded {
                what: format!("длина тега {tag:?}"),
                have: tag.len(),
                limit: MAX_TAG_BYTES,
            });
        }
        if !out.iter().any(|x| x == tag) {
            out.push(tag.clone());
        }
    }
    if out.len() > MAX_TAGS {
        return Err(CatalogError::LimitExceeded {
            what: "число тегов".to_string(),
            have: out.len(),
            limit: MAX_TAGS,
        });
    }
    Ok(out)
}

/// Проверяет описание и итоговый размер записи метаданных.
///
/// До взятия lifecycle-замка и до единой записи на диск: превышение обязано
/// быть отказом, а не половиной применённых метаданных.
pub fn check_meta(description: Option<&str>, tags: &[String]) -> Result<(), CatalogError> {
    if let Some(text) = description {
        if text.len() > MAX_DESCRIPTION {
            return Err(CatalogError::LimitExceeded {
                what: "описание".to_string(),
                have: text.len(),
                limit: MAX_DESCRIPTION,
            });
        }
    }
    let size = body::index_meta(&Uuid::default(), description, tags).len();
    if size > MAX_META_RECORD {
        return Err(CatalogError::LimitExceeded {
            what: "запись метаданных".to_string(),
            have: size,
            limit: MAX_META_RECORD,
        });
    }
    Ok(())
}

// ───────────────────────────── ошибки ─────────────────────────────

/// Отказ каталога с уже принятым решением о коде ответа.
///
/// Заводится вместо подстрочного разбора русского текста ошибки, которым
/// `PUT /{индекс}` выбирал между 409, 429 и 500. Такой разбор работает ровно
/// до первой переформулировки сообщения, и его отказ ничем не проявляется:
/// клиент просто получает 500 вместо 409.
// Часть отказов объявлена заранее и целиком: коды и типы ES решаются один раз,
// вместе с форматом, а не дописываются по одному вместе с обработчиками.
#[allow(dead_code, reason = "варианты потребляются фазами 2-4 вместе со своими точками")]
#[derive(Debug, Clone)]
pub enum CatalogError {
    IndexNotFound {
        name: String,
    },
    IndexAlreadyExists {
        name: String,
    },
    IndexClosed {
        name: String,
    },
    AliasNotFound {
        alias: String,
    },
    AliasIsIndexName {
        name: String,
    },
    AliasTargetNotFound {
        alias: String,
        target: String,
    },
    AliasChain {
        alias: String,
    },
    MultipleWriteIndices {
        alias: String,
    },
    NoWriteIndex {
        alias: String,
    },
    WriteIndexClosePrevented {
        alias: String,
        index: String,
    },
    Blocked {
        index: String,
        block: &'static str,
    },
    AmbiguousTarget {
        expr: String,
        matched: Vec<String>,
    },
    InvalidTargetForWrite {
        expr: String,
    },
    InvalidName {
        name: String,
        why: String,
    },
    InvalidParameter {
        key: String,
        why: String,
    },
    NotImplementedParameter {
        key: String,
    },
    LimitExceeded {
        what: String,
        have: usize,
        limit: usize,
    },
    TooManyOpenIndices {
        open: usize,
        limit: usize,
    },
    /// Настройки или доля приёма не приняты процессом.
    SettingsRejected {
        why: String,
    },
    ResourceBusy {
        why: String,
    },
    Durability(String),
}

impl CatalogError {
    pub fn status(&self) -> u16 {
        match self {
            CatalogError::IndexNotFound { .. }
            | CatalogError::AliasNotFound { .. }
            | CatalogError::AliasTargetNotFound { .. } => 404,
            CatalogError::IndexAlreadyExists { .. }
            | CatalogError::WriteIndexClosePrevented { .. }
            // 409, а не 400: доля приёма зависит от того, что процесс делает
            // прямо сейчас, поэтому тот же запрос позже может пройти.
            | CatalogError::SettingsRejected { .. } => 409,
            // Elasticsearch различает закрытый индекс типом ошибки при 400;
            // write/read blocks остаются запретом доступа с 403.
            CatalogError::IndexClosed { .. } => 400,
            CatalogError::Blocked { .. } => 403,
            CatalogError::TooManyOpenIndices { .. } => 429,
            CatalogError::ResourceBusy { .. } => 503,
            CatalogError::Durability(_) => 500,
            _ => 400,
        }
    }

    pub fn es_type(&self) -> &'static str {
        match self {
            CatalogError::IndexNotFound { .. } | CatalogError::AliasTargetNotFound { .. } => {
                "index_not_found_exception"
            }
            CatalogError::AliasNotFound { .. } => "alias_not_found_exception",
            CatalogError::IndexAlreadyExists { .. } => "resource_already_exists_exception",
            CatalogError::IndexClosed { .. } => "index_closed_exception",
            CatalogError::Blocked { .. } => "cluster_block_exception",
            CatalogError::AliasIsIndexName { .. } | CatalogError::AliasChain { .. } => {
                "invalid_alias_name_exception"
            }
            CatalogError::InvalidName { .. } => "invalid_index_name_exception",
            CatalogError::LimitExceeded { .. } => "validation_exception",
            CatalogError::TooManyOpenIndices { .. } => "circuit_breaking_exception",
            CatalogError::ResourceBusy { .. } => "unavailable_shards_exception",
            CatalogError::Durability(_) => "illegal_state_exception",
            _ => "illegal_argument_exception",
        }
    }
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::IndexNotFound { name } => write!(f, "индекс {name:?} не существует"),
            CatalogError::IndexAlreadyExists { name } => write!(f, "индекс {name:?} уже существует"),
            CatalogError::IndexClosed { name } => {
                write!(f, "индекс {name:?} закрыт; откройте его через POST /{name}/_open")
            }
            CatalogError::AliasNotFound { alias } => write!(f, "alias {alias:?} не существует"),
            CatalogError::AliasIsIndexName { name } => {
                write!(f, "имя {name:?} уже занято физическим индексом: alias и индекс живут в одном пространстве имён")
            }
            CatalogError::AliasTargetNotFound { alias, target } => {
                write!(f, "alias {alias:?} указывает на индекс {target}, которого нет в каталоге")
            }
            CatalogError::AliasChain { alias } => {
                write!(f, "цель {alias:?} сама является alias: цепочки alias на alias запрещены")
            }
            CatalogError::MultipleWriteIndices { alias } => write!(
                f,
                "у alias {alias:?} больше одного индекса записи; ровно один target обязан иметь is_write_index=true"
            ),
            CatalogError::NoWriteIndex { alias } => write!(
                f,
                "у alias {alias:?} несколько целей и ни одного явного индекса записи; \
                 выбирать «первый открытый» нельзя — так пишут в архив"
            ),
            CatalogError::WriteIndexClosePrevented { alias, index } => write!(
                f,
                "индекс {index:?} — индекс записи alias {alias:?}; сначала выполните rollover \
                 либо перенесите признак записи"
            ),
            CatalogError::Blocked { index, block } => {
                write!(f, "на индексе {index:?} стоит блок {block}")
            }
            CatalogError::AmbiguousTarget { expr, matched } => write!(
                f,
                "цель {expr:?} раскрывается в {} индексов ({}), а операция допускает только один",
                matched.len(),
                matched.join(", ")
            ),
            CatalogError::InvalidTargetForWrite { expr } => write!(
                f,
                "в цель {expr:?} писать нельзя: перечисление, шаблон и _all не являются \
                 однозначной целью записи"
            ),
            CatalogError::InvalidName { name, why } => write!(f, "имя {name:?} негодно: {why}"),
            CatalogError::InvalidParameter { key, why } => write!(f, "параметр {key:?}: {why}"),
            CatalogError::NotImplementedParameter { key } => write!(
                f,
                "параметр {key:?} не реализован; запрос отвергнут, а не исполнен без него"
            ),
            CatalogError::LimitExceeded { what, have, limit } => {
                write!(f, "{what}: {have}, потолок {limit}")
            }
            CatalogError::TooManyOpenIndices { open, limit } => write!(
                f,
                "открытых индексов {open}, а безопасный предел процесса {limit}; поднимите \
                 --max-indices осознанно либо закройте ненужные поколения"
            ),
            CatalogError::SettingsRejected { why } => write!(f, "{why}"),
            CatalogError::ResourceBusy { why } => write!(f, "каталог занят: {why}"),
            CatalogError::Durability(why) => write!(f, "каталог не зафиксирован: {why}"),
        }
    }
}

impl CatalogError {
    /// Тело ответа в том же плоском виде, в каком узел отдаёт прочие ошибки.
    ///
    /// Имя индекса или alias кладётся отдельным полем, а не только внутрь
    /// текста: клиенту, который ходит по нескольким целям, разбирать русскую
    /// фразу, чтобы понять, какая из них отказала, — не работа.
    pub fn body(&self) -> String {
        let subject = match self {
            CatalogError::IndexNotFound { name }
            | CatalogError::IndexAlreadyExists { name }
            | CatalogError::IndexClosed { name }
            | CatalogError::AliasIsIndexName { name }
            | CatalogError::InvalidName { name, .. } => Some(("index", name.clone())),
            CatalogError::Blocked { index, .. } => Some(("index", index.clone())),
            CatalogError::WriteIndexClosePrevented { index, .. } => Some(("index", index.clone())),
            CatalogError::AliasNotFound { alias }
            | CatalogError::AliasChain { alias }
            | CatalogError::AliasTargetNotFound { alias, .. }
            | CatalogError::MultipleWriteIndices { alias }
            | CatalogError::NoWriteIndex { alias } => Some(("alias", alias.clone())),
            _ => None,
        };
        let mut value = serde_json::json!({
            "error": self.to_string(),
            "type": self.es_type(),
        });
        if let (Some((key, name)), Some(map)) = (subject, value.as_object_mut()) {
            map.insert(key.to_string(), serde_json::Value::String(name));
        }
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, uuid: Uuid) -> Entry {
        Entry {
            uuid,
            name: name.to_string(),
            state: State::Open,
            blocks: Blocks::none(),
            created_ms: 1_700_000_000_000,
            description: None,
            tags: Vec::new(),
            node: None,
        }
    }

    fn uuid(seed: u8) -> Uuid {
        Uuid::from_bytes([seed; 16])
    }

    #[test]
    fn a_minted_uuid_is_version_four_and_round_trips_as_text() {
        let id = Uuid::mint().expect("выпуск");
        assert_eq!(id.bytes()[6] & 0xF0, 0x40, "версия обязана быть четвёртой");
        assert_eq!(id.bytes()[8] & 0xC0, 0x80, "вариант обязан быть RFC 4122");
        let text = id.text();
        assert_eq!(text.len(), 36);
        assert_eq!(Uuid::parse(&text), Some(id));
        assert!(Uuid::parse(&text.to_uppercase()).is_none(), "не канонический регистр принят");
        assert!(Uuid::parse(&text.replace('-', "")).is_none(), "UUID без разделителей принят");
        assert_ne!(id, Uuid::mint().expect("второй"), "два выпуска не совпадают");
    }

    #[test]
    fn blocks_add_up_and_never_overwrite_each_other() {
        let mut blocks = Blocks::none();
        blocks = blocks.with(Blocks::mask_of("write").expect("маска"));
        blocks = blocks.with(Blocks::mask_of("read").expect("маска"));
        assert!(blocks.has(Blocks::WRITE) && blocks.has(Blocks::READ));
        // Снятие одного не снимает другой — ровно то, чего не давал «текущий блок».
        blocks = blocks.without(Blocks::WRITE);
        assert!(!blocks.has(Blocks::WRITE) && blocks.has(Blocks::READ));
    }

    #[test]
    fn read_only_is_write_plus_metadata() {
        let mask = Blocks::mask_of("read_only").expect("маска");
        let blocks = Blocks::none().with(mask);
        assert!(blocks.has(Blocks::WRITE) && blocks.has(Blocks::METADATA));
        assert!(!blocks.has(Blocks::READ), "read_only не запрещает чтение");
    }

    #[test]
    fn an_unknown_block_bit_is_refused_rather_than_masked() {
        assert!(Blocks::from_bits(0x0F).is_some());
        assert!(Blocks::from_bits(0x10).is_none(), "неизвестный бит обязан отвергаться");
    }

    #[test]
    fn an_unknown_record_kind_is_a_hard_error() {
        let mut cat = Catalog::default();
        let error = cat.apply(0x7F, &[]).expect_err("неизвестный вид обязан отказывать");
        assert!(error.contains("неизвестна"), "{error}");
    }

    #[test]
    fn a_catalog_round_trips_through_its_own_snapshot() {
        let mut cat = Catalog::default();
        let one = uuid(1);
        let two = uuid(2);
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("messages-000001", one))).expect("первый");
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("messages-000002", two))).expect("второй");
        let batch = AliasBatch {
            aliases: vec![AliasSet {
                alias: "all_messages".to_string(),
                bindings: vec![
                    Binding { target: one, is_write: Some(false) },
                    Binding { target: two, is_write: Some(true) },
                ],
            }],
            removed: Vec::new(),
        };
        cat.apply(TXN_ALIAS_BATCH, &batch.encode()).expect("пачка");
        cat.apply(TXN_INDEX_BLOCKS, &body::index_blocks(&one, Blocks::none().with(Blocks::WRITE)))
            .expect("блок");

        let mut restored = Catalog::default();
        for rec in cat.to_base() {
            restored.apply(rec.kind, &rec.body).expect("снимок");
        }
        assert_eq!(restored.entries().len(), 2);
        assert_eq!(restored.write_target("all_messages").expect("запись").name, "messages-000002");
        assert!(restored.by_uuid(&one).expect("старый").blocks.has(Blocks::WRITE));
        assert_eq!(restored.aliases_of(&one), vec!["all_messages"]);
    }

    #[test]
    fn a_single_target_alias_routes_writes_implicitly() {
        let mut cat = Catalog::default();
        let one = uuid(1);
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("logs-000001", one))).expect("индекс");
        let batch = AliasBatch {
            aliases: vec![AliasSet {
                alias: "logs".to_string(),
                bindings: vec![Binding { target: one, is_write: None }],
            }],
            removed: Vec::new(),
        };
        cat.apply(TXN_ALIAS_BATCH, &batch.encode()).expect("пачка");
        assert_eq!(cat.write_target("logs").expect("запись").name, "logs-000001");
    }

    #[test]
    fn explicit_false_disables_the_implicit_single_target_writer() {
        let mut cat = Catalog::default();
        let one = uuid(1);
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("logs-000001", one))).expect("индекс");
        let batch = AliasBatch {
            aliases: vec![AliasSet {
                alias: "logs".to_string(),
                bindings: vec![Binding { target: one, is_write: Some(false) }],
            }],
            removed: Vec::new(),
        };
        cat.apply(TXN_ALIAS_BATCH, &batch.encode()).expect("пачка");
        assert!(matches!(cat.write_target("logs"), Err(CatalogError::NoWriteIndex { .. })));
    }

    #[test]
    fn a_multi_target_alias_without_an_explicit_write_index_refuses() {
        let mut cat = Catalog::default();
        let (one, two) = (uuid(1), uuid(2));
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("a", one))).expect("первый");
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("b", two))).expect("второй");
        let batch = AliasBatch {
            aliases: vec![AliasSet {
                alias: "both".to_string(),
                bindings: vec![
                    Binding { target: one, is_write: Some(false) },
                    Binding { target: two, is_write: Some(false) },
                ],
            }],
            removed: Vec::new(),
        };
        cat.apply(TXN_ALIAS_BATCH, &batch.encode()).expect("пачка");
        let error = cat.write_target("both").expect_err("выбирать наугад нельзя");
        assert!(matches!(error, CatalogError::NoWriteIndex { .. }), "{error}");
        assert_eq!(error.status(), 400);
    }

    #[test]
    fn removing_an_index_takes_its_alias_bindings_with_it() {
        let mut cat = Catalog::default();
        let (one, two) = (uuid(1), uuid(2));
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("a", one))).expect("первый");
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("b", two))).expect("второй");
        let batch = AliasBatch {
            aliases: vec![AliasSet {
                alias: "both".to_string(),
                bindings: vec![
                    Binding { target: one, is_write: Some(false) },
                    Binding { target: two, is_write: Some(true) },
                ],
            }],
            removed: Vec::new(),
        };
        cat.apply(TXN_ALIAS_BATCH, &batch.encode()).expect("пачка");
        cat.apply(TXN_INDEX_REMOVE, &body::index_remove(&one)).expect("удаление");
        assert!(cat.by_uuid(&one).is_none());
        assert_eq!(cat.aliases().get("both").expect("alias").len(), 1, "висячих привязок нет");

        cat.apply(TXN_INDEX_REMOVE, &body::index_remove(&two)).expect("удаление");
        assert!(cat.aliases().get("both").is_none(), "alias без целей исчезает целиком");
    }

    #[test]
    fn rollover_switches_the_write_index_and_blocks_the_old_one_in_one_record() {
        let mut cat = Catalog::default();
        let old = uuid(1);
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("messages-000001", old))).expect("старый");
        let batch = AliasBatch {
            aliases: vec![AliasSet {
                alias: "all_messages".to_string(),
                bindings: vec![Binding { target: old, is_write: Some(true) }],
            }],
            removed: Vec::new(),
        };
        cat.apply(TXN_ALIAS_BATCH, &batch.encode()).expect("пачка");

        let fresh = uuid(2);
        let roll = Rollover {
            fresh: entry("messages-000002", fresh),
            alias: "all_messages".to_string(),
            bindings: vec![
                Binding { target: old, is_write: Some(false) },
                Binding { target: fresh, is_write: Some(true) },
            ],
            retired: old,
            retired_blocks: Blocks::none().with(Blocks::WRITE),
        };
        cat.apply(TXN_ROLLOVER, &roll.encode()).expect("rollover");

        assert_eq!(cat.write_target("all_messages").expect("запись").name, "messages-000002");
        let retired = cat.by_uuid(&old).expect("старый");
        assert!(retired.blocks.has(Blocks::WRITE), "старое поколение закрыто для записи");
        assert_eq!(retired.state, State::Open, "но остаётся в поиске");
        assert_eq!(cat.is_write_index(&fresh).as_deref(), Some("all_messages"));
        assert_eq!(cat.is_write_index(&old), None);
    }

    #[test]
    fn an_index_and_an_alias_cannot_share_a_name() {
        let mut cat = Catalog::default();
        let one = uuid(1);
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("messages", one))).expect("индекс");
        assert!(cat.name_taken("messages"));
        assert!(!cat.name_taken("all_messages"));
        let batch = AliasBatch {
            aliases: vec![AliasSet {
                alias: "all_messages".to_string(),
                bindings: vec![Binding { target: one, is_write: Some(true) }],
            }],
            removed: Vec::new(),
        };
        cat.apply(TXN_ALIAS_BATCH, &batch.encode()).expect("пачка");
        assert!(cat.name_taken("all_messages"), "alias занимает имя наравне с индексом");
    }

    #[test]
    fn tags_keep_their_case_and_lose_their_duplicates() {
        let raw =
            vec!["Hot".to_string(), "hot".to_string(), "Hot".to_string(), "тёплый".to_string()];
        let tags = normalize_tags(&raw).expect("теги");
        assert_eq!(tags, vec!["Hot", "hot", "тёплый"], "регистр значим, повтор — нет");
    }

    #[test]
    fn metadata_limits_are_refused_before_anything_is_written() {
        let long = "я".repeat(MAX_DESCRIPTION);
        let error = check_meta(Some(&long), &[]).expect_err("описание длиннее потолка");
        assert!(matches!(error, CatalogError::LimitExceeded { .. }));
        assert_eq!(error.status(), 400);

        let many: Vec<String> = (0..MAX_TAGS + 1).map(|i| format!("тег-{i}")).collect();
        let error = normalize_tags(&many).expect_err("тегов больше потолка");
        assert!(matches!(error, CatalogError::LimitExceeded { .. }));

        let big = "x".repeat(MAX_TAG_BYTES + 1);
        assert!(normalize_tags(&[big]).is_err(), "длинный тег обязан отвергаться");
    }

    #[test]
    fn a_description_of_none_and_of_empty_are_different_states() {
        let mut cat = Catalog::default();
        let one = uuid(1);
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("a", one))).expect("индекс");
        cat.apply(TXN_INDEX_META, &body::index_meta(&one, Some(""), &[])).expect("пустое");
        assert_eq!(cat.by_uuid(&one).expect("запись").description.as_deref(), Some(""));
        cat.apply(TXN_INDEX_META, &body::index_meta(&one, None, &[])).expect("отсутствие");
        assert_eq!(cat.by_uuid(&one).expect("запись").description, None);
    }

    #[test]
    fn a_truncated_record_body_is_refused() {
        let full = body::index_add(&entry("messages", uuid(1)));
        let mut cat = Catalog::default();
        assert!(cat.apply(TXN_INDEX_ADD, &full[..full.len() - 2]).is_err(), "обрезанное тело");
        let mut padded = full.clone();
        padded.push(0);
        assert!(cat.apply(TXN_INDEX_ADD, &padded).is_err(), "лишние байты в конце");
    }

    #[test]
    fn a_state_reference_to_an_unknown_index_is_refused() {
        let mut cat = Catalog::default();
        let error = cat
            .apply(TXN_INDEX_STATE, &body::index_state(&uuid(9), State::Closed))
            .expect_err("ссылка на неизвестный индекс");
        assert!(error.contains("не встречалась"), "{error}");
    }

    #[test]
    fn open_count_ignores_records_without_a_runtime() {
        let mut cat = Catalog::default();
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("a", uuid(1)))).expect("первый");
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("b", uuid(2)))).expect("второй");
        // Записи есть, runtime нет — предел процесса их не считает.
        assert_eq!(cat.entries().len(), 2);
        assert_eq!(cat.open_count(), 0);
    }

    #[test]
    fn several_read_only_v1_entries_can_share_the_absent_uuid() {
        let mut cat = Catalog::default();
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("one", Uuid::default()))).expect("первый");
        cat.apply(TXN_INDEX_ADD, &body::index_add(&entry("two", Uuid::default()))).expect("второй");
        assert_eq!(cat.entries().len(), 2);
        assert!(cat.by_name("one").is_some() && cat.by_name("two").is_some());
    }
}
