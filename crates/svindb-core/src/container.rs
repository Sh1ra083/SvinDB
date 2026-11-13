//! Контейнер сегмента: неизменяемые куски одним файлом.
//!
//! ## Зачем
//!
//! Область отображения — дефицит: у процесса есть потолок на их **число**
//! (`vm.max_map_count`, по умолчанию 65 530), и упирается в него не память, а
//! число сегментов. Замерено двадцать две области на сегмент при схеме с двумя
//! текстовыми полями — то есть узел перестаёт открываться где-то на трёх
//! тысячах сегментов, притом что памяти вдоволь. Убрать этот расход можно
//! единственным способом: сложить неизменяемые куски в один файл и отобразить
//! его **один раз**, раздав читателям куски общего отображения
//! ([`crate::mmap::Region`]).
//!
//! ## Что сюда не кладётся и почему
//!
//! * `live.bin` — единственная область **на запись** у сегмента. Она правится
//!   атомарно на живом опубликованном сегменте, её `msync` в контейнере накрыл
//!   бы гигабайты вместо килобайтов, а [`crate::mmap::Mapped::word`] требует
//!   выравнивания в восемь байт, которого кусок контейнера не обещает
//!   (невыровненный атомарный доступ на aarch64 — это `SIGBUS`, и не всегда, а
//!   при определённом числе документов: прошло бы все тесты и легло в бою).
//! * `head.tsv` — он читается текстом **до** всякого отображения и сам говорит,
//!   есть ли контейнер. Спрашивать оглавление контейнера, чтобы узнать, где
//!   лежит признак контейнера, невозможно.
//! * `sums.tsv` — пишется **после** заголовка и накрывает его самого.
//! * `<поле>.scrc` — суммы распакованных кусков намеренно не отображаются:
//!   их читают дважды в жизни сегмента, а область стоила бы на каждое хранимое
//!   поле каждого сегмента. В контейнере они лежат как обычный кусок, но
//!   читаются позиционным чтением файла — см. [`Located`].
//!
//! ## Чего здесь нет
//!
//! Правила «упаковывать только мелкие куски» (`noCFSRatio` у Lucene) здесь нет
//! намеренно. Lucene **копирует** уже записанные файлы в контейнер и потому
//! удваивает пиковое место; правило ограничивает ущерб. Мы пишем сразу в
//! контейнер — ни одного лишнего байта, — и ограничивать нечего.

use crate::mmap::{Mapped, Region};
use std::io::{Read, Seek, Write};

/// Имя файла контейнера в каталоге сегмента.
pub const FILE: &str = "seg.dat";

/// Версия **контейнера**. С [`crate::index::LAYOUT`] не связана никак: контейнер
/// ортогонален кодированию записей, и сегмент раскладки 9 бывает и упакованным,
/// и рассыпанным.
pub const FORMAT: u32 = 1;

/// Длина заголовка. Он же — смещение первого куска.
pub const HEAD: usize = 64;

/// Длина записи оглавления.
pub const ENTRY: usize = 96;

/// Сколько байт записи отведено имени.
///
/// [`crate::index::valid_field_name`] держит имя поля в 64 байтах, самое длинное
/// расширение — `.bktidx`, семь байт; итого 71. Семьдесят два — с запасом на
/// байт и ради круглого шага записи.
pub const NAME: usize = 72;

/// Длина концевика.
pub const TAIL: usize = 32;

/// Выравнивание куска.
///
/// Требование к формату ровно одно, и оно уже соблюдено: ни один кусок не
/// читается как срез `u64` или `u32` — всё идёт через `read_unaligned` и
/// `from_le_bytes`. Выравнивание здесь не про правильность, а про строку кеша:
/// кусок, начинающийся посреди строки, тянет за собой соседний. Цена — до 1.6 КБ
/// на сегмент при полусотне кусков.
pub const ALIGN: u64 = 64;

/// Опознание в начале файла.
///
/// Байты подобраны так, чтобы порча называла себя сама, а не разбиралась как
/// правдоподобные числа:
/// * `0x8F` — старший бит взведён: канал, потерявший восьмой бит, ломает первый
///   же байт;
/// * `SVIN` — видно человеку в `head -c 8`, ни с чем не спутаешь;
/// * `\r\n` — перевод строк, испорченный переносом файла как текста, виден
///   сразу;
/// * `0x1A` — DOS EOF: `type` и `cat` останавливаются здесь, а не выплёвывают
///   гигабайт двоичного мусора в терминал.
pub const MAGIC: [u8; 8] = [0x8F, b'S', b'V', b'I', b'N', b'\r', b'\n', 0x1A];

/// Опознание концевика — укороченное, тех же четырёх свойств здесь не нужно:
/// концевик не начало файла, и в терминал его никто не выводит.
pub const TAIL_MAGIC: [u8; 4] = [0x8F, b'S', b'V', b'N'];

/// Потолок числа кусков.
///
/// Не про формат, а про то, что `count` приходит с диска и по нему **резервируют
/// память**. Полсотни кусков у обычного сегмента, до нескольких тысяч у широкой
/// схемы; шестьдесят пять тысяч — потолок с запасом на два порядка, и он же
/// превращает испорченные четыре байта в отказ вместо запроса на гигабайты.
const MAX_COUNT: u64 = 65_536;

fn bad(msg: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

/// Запись оглавления: имя куска, где он лежит и что в нём.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub name: String,
    /// Смещение от начала `seg.dat`, а не от конца заголовка: так его можно
    /// отдать `mmap` и `pread` без единого сложения.
    pub off: u64,
    pub len: u64,
    /// `crc32` содержимого куска. При открытии **не** проверяется: это чтение
    /// всего сегмента. Дело `check --full`.
    pub crc: u32,
}

/// Годится ли имя куска.
///
/// Проверяется и при записи, и при чтении, и это не перестраховка: имя с
/// косой чертой в оглавлении означало бы кусок, который читатель ищет **за
/// пределами каталога сегмента**, а имя с нулевым байтом внутри — два разных
/// куска, неразличимых после дополнения нулями.
fn valid_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("пустое имя куска".to_string());
    }
    if name.len() > NAME {
        return Err(format!("имя {name:?} длиннее {NAME} байт"));
    }
    if name.contains('/') || name.contains('\0') || name == "." || name == ".." {
        return Err(format!("имя {name:?} не может быть именем файла в каталоге сегмента"));
    }
    Ok(())
}

// ─────────────────────────────── запись ───────────────────────────────

/// Пишет контейнер, принимая куски по одному.
///
/// ## Два входа и почему их два
///
/// [`put`](Self::put) — для куска, который уже целиком лежит буфером в памяти:
/// таких у нас подавляющее большинство, они собираются в `Vec<u8>` и раньше
/// уходили каждый в свой файл. [`begin`](Self::begin) — для потока: `postings.bin`
/// пишется по ходу обхода слов и в память не собирается вовсе, а после развязки
/// слияния так же пишутся `pos.bin`, `terms.bin`, `dict.blob`, `ids.bin` и `.q8`.
///
/// Собрать «все файлы, потом упаковать» было бы короче и неверно: это ровно то
/// удвоение пикового места, которое Lucene вынужден терпеть в `.cfs`, и терпеть
/// его без причины Lucene незачем.
///
/// ## Что нельзя
///
/// Открытым может быть **один** кусок. Класть два потока вперемежку контейнер не
/// умеет по построению: кусок обязан лежать непрерывно, а оглавление знает про
/// него одну пару «смещение, длина». Попытка начать второй — отказ, а не молчание.
pub struct Writer {
    sink: std::io::BufWriter<std::fs::File>,
    path: String,
    layout: u32,
    /// Сколько байт уже отдано файлу. Это и есть смещение следующей записи.
    at: u64,
    dir: Vec<Member>,
    /// Открытый кусок: имя, его начало и сумма по уже дописанному.
    open: Option<(String, u64, crate::wal::Crc32, u64)>,
}

/// Сколько держать в буфере, прежде чем отдать ядру.
const SINK_BUF: usize = 1 << 18;

/// Куда складываются куски собираемого сегмента.
///
/// ## Зачем приёмник, если писатель уже есть
///
/// Сегмент собирают дважды разными по назначению путями: обычный ложится в
/// контейнер на диске, живой остаётся в памяти и на диск может не лечь никогда —
/// за его долговечность отвечает журнал. Сборка при этом обязана быть **одна**:
/// разведи её по двум путям, и словарь, постинги, колонки и позиции
/// укладывались бы двумя дорогами, а разойдясь однажды, вторая начала бы
/// собирать сегмент, который читается неправильно. Здесь развилка ровно в том
/// месте, где она настоящая, — куда положить готовый кусок, — и ни в каком
/// другом.
///
/// Порядок и правила у обоих одни: кусок нулевой длины законен, открытым может
/// быть только один поток, закрывать приёмник с незакрытым куском нельзя.
pub enum Sink {
    /// Контейнер `seg.dat` в каталоге сегмента.
    Disk(Writer),
    /// Куски остаются в памяти. Контейнера нет: складывать их в один буфер и
    /// тут же резать его обратно на области значило бы платить копированием за
    /// оглавление, которое здесь и так есть — это ключи карты.
    Memory(MemoryPack),
}

/// Куски, собранные в памяти.
pub struct MemoryPack {
    parts: std::collections::BTreeMap<String, Vec<u8>>,
    /// Открытый поток: имя и накопленное.
    open: Option<(String, Vec<u8>)>,
}

impl Default for MemoryPack {
    fn default() -> MemoryPack {
        MemoryPack::new()
    }
}

impl MemoryPack {
    pub fn new() -> MemoryPack {
        MemoryPack { parts: std::collections::BTreeMap::new(), open: None }
    }

    /// Кладёт готовый буфер отдельным куском.
    ///
    /// Имя проверяется теми же правилами, что и у файлового приёмника: кусок,
    /// законный в памяти, обязан оставаться законным и после записи на диск —
    /// иначе расхождение выяснилось бы уже при сбросе, а не при сборке.
    pub fn put(&mut self, name: &str, bytes: &[u8]) -> std::io::Result<()> {
        valid_name(name).map_err(bad)?;
        if self.open.is_some() {
            return Err(bad(format!("кусок {name:?} кладут поверх незакрытого потока")));
        }
        if self.parts.contains_key(name) {
            return Err(bad(format!("кусок {name:?} записывают второй раз")));
        }
        self.parts.insert(name.to_string(), bytes.to_vec());
        Ok(())
    }
}

impl Sink {
    /// Приёмник, складывающий куски в контейнер каталога.
    pub fn disk(dir: &str, layout: u32) -> std::io::Result<Sink> {
        Ok(Sink::Disk(Writer::create(dir, layout)?))
    }

    /// Приёмник, оставляющий куски в памяти.
    pub fn memory() -> Sink {
        Sink::Memory(MemoryPack::new())
    }

    /// Кладёт готовый буфер отдельным куском.
    pub fn put(&mut self, name: &str, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Sink::Disk(w) => w.put(name, bytes),
            Sink::Memory(m) => m.put(name, bytes),
        }
    }

    /// Начинает потоковый кусок. Дальше — [`append`](Self::append) и
    /// [`end`](Self::end).
    pub fn begin(&mut self, name: &str) -> std::io::Result<()> {
        match self {
            Sink::Disk(w) => w.begin(name),
            Sink::Memory(m) => {
                valid_name(name).map_err(bad)?;
                if m.open.is_some() {
                    return Err(bad(format!("кусок {name:?} начат поверх незакрытого")));
                }
                if m.parts.contains_key(name) {
                    return Err(bad(format!("кусок {name:?} записывают второй раз")));
                }
                m.open = Some((name.to_string(), Vec::new()));
                Ok(())
            }
        }
    }

    /// Дописывает в открытый кусок.
    pub fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Sink::Disk(w) => w.append(bytes),
            Sink::Memory(m) => {
                let Some((_, buf)) = m.open.as_mut() else {
                    return Err(bad("дописывать нечего — кусок не начат".to_string()));
                };
                buf.extend_from_slice(bytes);
                Ok(())
            }
        }
    }

    /// Сколько байт уже легло в открытый кусок — смещение следующей записи
    /// **внутри куска**, то самое, что уходит в опоры.
    pub fn open_len(&self) -> u64 {
        match self {
            Sink::Disk(w) => w.open_len(),
            Sink::Memory(m) => m.open.as_ref().map_or(0, |(_, buf)| buf.len() as u64),
        }
    }

    /// Закрывает кусок и отдаёт его длину.
    pub fn end(&mut self) -> std::io::Result<u64> {
        match self {
            Sink::Disk(w) => w.end(),
            Sink::Memory(m) => {
                let Some((name, buf)) = m.open.take() else {
                    return Err(bad("закрывать нечего — кусок не начат".to_string()));
                };
                let len = buf.len() as u64;
                m.parts.insert(name, buf);
                Ok(len)
            }
        }
    }

    /// Записан ли уже такой кусок.
    pub fn has(&self, name: &str) -> bool {
        match self {
            Sink::Disk(w) => w.has(name),
            Sink::Memory(m) => m.parts.contains_key(name),
        }
    }

    /// Закрывает приёмник.
    ///
    /// У контейнера отдаёт его полную длину — ровно то число, которое уйдёт
    /// строкой `container` в заголовок. У памяти длины контейнера нет: `None`
    /// здесь не «неизвестно», а «контейнера не существует», и заголовок такого
    /// сегмента строки `container` не получает.
    pub fn finish(self) -> std::io::Result<(Option<u64>, Option<MemoryPack>)> {
        match self {
            Sink::Disk(w) => Ok((Some(w.finish()?), None)),
            Sink::Memory(m) => {
                if m.open.is_some() {
                    return Err(bad(
                        "приёмник закрывают с незакрытым куском — длина его никому не известна"
                            .to_string(),
                    ));
                }
                Ok((None, Some(m)))
            }
        }
    }
}

impl MemoryPack {
    /// Готовый источник поверх собранных кусков.
    pub fn source(self, name: &str) -> SegmentSource {
        SegmentSource::Memory {
            name: name.to_string(),
            parts: self.parts.into_iter().map(|(n, b)| (n, Region::of_bytes(&b))).collect(),
        }
    }

    /// Куски по именам — для того, кто собирается их записать.
    pub fn parts(&self) -> &std::collections::BTreeMap<String, Vec<u8>> {
        &self.parts
    }

    /// Сколько памяти занято уложенным. По ней меряется предел живых сегментов.
    pub fn bytes(&self) -> usize {
        self.parts.values().map(|b| b.len()).sum()
    }
}

impl Writer {
    /// Заводит `seg.dat` в каталоге сегмента.
    ///
    /// Заголовок пишется здесь **нулями** и переписывается настоящим только в
    /// [`finish`](Self::finish). Так недописанный контейнер не опознаётся ни
    /// одним читателем: `MAGIC` появляется последним движением, уже после того
    /// как все куски и оглавление легли.
    pub fn create(dir: &str, layout: u32) -> std::io::Result<Writer> {
        let path = format!("{dir}/{FILE}");
        let f = std::fs::File::create(&path)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
        Writer::over(path, layout, f)
    }

    /// Заводит контейнер под **временным** именем рядом с будущим местом.
    ///
    /// `create_new` здесь против подложенного: `O_CREAT|O_EXCL` отказывает на
    /// существующем имени, даже если это символическая ссылка, — то есть
    /// временное имя не может увести запись из каталога сегмента. Тем же приёмом
    /// пользуется [`crate::fs::atomic_write`]. Двух одновременных пересборок он
    /// не разводит и разводить не может: в имени стоит номер процесса, у чужого
    /// захода он другой, и оба имени свободны. Их разводит замок писателя
    /// ([`crate::manifest::WriterLock`]), который берут все команды перекладки.
    fn create_new_at(path: &str, layout: u32) -> std::io::Result<Writer> {
        use std::os::unix::fs::OpenOptionsExt;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
        Writer::over(path.to_string(), layout, f)
    }

    fn over(path: String, layout: u32, f: std::fs::File) -> std::io::Result<Writer> {
        let mut w = Writer {
            sink: std::io::BufWriter::with_capacity(SINK_BUF, f),
            path,
            layout,
            at: 0,
            dir: Vec::new(),
            open: None,
        };
        w.raw(&[0u8; HEAD])?;
        Ok(w)
    }

    fn raw(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.sink
            .write_all(bytes)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{}: {e}", self.path)))?;
        self.at += bytes.len() as u64;
        Ok(())
    }

    /// Добивает нулями до границы [`ALIGN`].
    fn align(&mut self) -> std::io::Result<()> {
        let slack = self.at % ALIGN;
        if slack != 0 {
            let pad = [0u8; ALIGN as usize];
            let n = (ALIGN - slack) as usize;
            self.raw(&pad[..n])?;
        }
        Ok(())
    }

    fn claim(&mut self, name: &str) -> std::io::Result<u64> {
        valid_name(name).map_err(|e| bad(format!("{}: {e}", self.path)))?;
        if self.open.is_some() {
            return Err(bad(format!(
                "{}: кусок {name:?} начат, пока не закрыт предыдущий — куски контейнера \
                 лежат непрерывно, и вперемежку их писать нечем",
                self.path
            )));
        }
        if self.dir.iter().any(|m| m.name == name) {
            return Err(bad(format!("{}: кусок {name:?} записан дважды", self.path)));
        }
        if self.dir.len() as u64 >= MAX_COUNT {
            return Err(bad(format!("{}: кусков больше {MAX_COUNT}", self.path)));
        }
        self.align()?;
        Ok(self.at)
    }

    /// Кладёт готовый буфер отдельным куском.
    ///
    /// Кусок нулевой длины — законное содержимое, а не «куска нет»: разница
    /// несущая, и на ней стоит опись `check`. Кусок, которого быть не должно, в
    /// оглавление просто не попадает.
    pub fn put(&mut self, name: &str, bytes: &[u8]) -> std::io::Result<()> {
        let off = self.claim(name)?;
        self.raw(bytes)?;
        self.dir.push(Member {
            name: name.to_string(),
            off,
            len: bytes.len() as u64,
            crc: crate::wal::crc32(bytes),
        });
        Ok(())
    }

    /// Начинает потоковый кусок. Дальше — [`append`](Self::append) и
    /// [`end`](Self::end).
    pub fn begin(&mut self, name: &str) -> std::io::Result<()> {
        let off = self.claim(name)?;
        self.open = Some((name.to_string(), off, crate::wal::Crc32::new(), 0));
        Ok(())
    }

    /// Дописывает в открытый кусок.
    pub fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let Some((_, _, crc, len)) = self.open.as_mut() else {
            return Err(bad(format!("{}: дописывать нечего — кусок не начат", self.path)));
        };
        crc.update(bytes);
        *len += bytes.len() as u64;
        self.raw(bytes)
    }

    /// Сколько байт уже легло в открытый кусок.
    ///
    /// Это и есть смещение следующей записи **внутри куска** — то самое, что
    /// уходит в опоры `terms.idx` и в записи слов. Считать его по буферу
    /// писателя нельзя: буфер опустошается, кусок нет.
    pub fn open_len(&self) -> u64 {
        self.open.as_ref().map_or(0, |(_, _, _, len)| *len)
    }

    /// Закрывает кусок и отдаёт его длину.
    pub fn end(&mut self) -> std::io::Result<u64> {
        self.end_summed().map(|(len, _)| len)
    }

    /// То же, но вместе с суммой уложенного.
    ///
    /// Сумму спрашивает перенос куска из старого контейнера в новый
    /// ([`replace`]): у него есть, с чем её сверить, — запись старого оглавления.
    /// Считать её вторым проходом по тем же байтам было бы вдвое дороже самого
    /// переноса, а писатель всё равно считает её по ходу.
    pub fn end_summed(&mut self) -> std::io::Result<(u64, u32)> {
        let Some((name, off, crc, len)) = self.open.take() else {
            return Err(bad(format!("{}: закрывать нечего — кусок не начат", self.path)));
        };
        let crc = crc.finish();
        self.dir.push(Member { name, off, len, crc });
        Ok((len, crc))
    }

    /// Записан ли уже такой кусок.
    pub fn has(&self, name: &str) -> bool {
        self.dir.iter().any(|m| m.name == name)
    }

    /// Дописывает оглавление с концевиком, переписывает заголовок и **доводит
    /// файл до диска**.
    ///
    /// Отдаёт полную длину — ровно то число, которое уйдёт строкой `container` в
    /// `head.tsv`. Заголовок вправе обещать только тот контейнер, который уже на
    /// носителе, поэтому `sync_all` здесь, а не у вызывающего.
    pub fn finish(mut self) -> std::io::Result<u64> {
        if self.open.is_some() {
            return Err(bad(format!(
                "{}: контейнер закрывают с незакрытым куском — длина его никому не известна",
                self.path
            )));
        }
        // Порядок записей — по имени. Двоичный поиск это даёт даром, но не он
        // главный: одинаковое содержимое обязано давать **побайтово одинаковый**
        // контейнер, кто бы его ни собрал и в каком бы порядке ни клал куски.
        // Иначе сверка «слитый равен собранному заново» перестаёт быть сверкой.
        self.dir.sort_by(|a, b| a.name.cmp(&b.name));
        self.align()?;
        let dir_off = self.at;
        let mut table = Vec::with_capacity(self.dir.len() * ENTRY);
        for m in self.dir.iter() {
            let mut name = [0u8; NAME];
            name[..m.name.len()].copy_from_slice(m.name.as_bytes());
            table.extend_from_slice(&name);
            table.extend_from_slice(&m.off.to_le_bytes());
            table.extend_from_slice(&m.len.to_le_bytes());
            table.extend_from_slice(&m.crc.to_le_bytes());
            table.extend_from_slice(&0u32.to_le_bytes());
        }
        let crc_dir = crate::wal::crc32(&table);
        let dir_len = table.len() as u64;
        self.raw(&table)?;

        // Концевик пишется до того, как заголовок станет настоящим, и это не
        // прихоть порядка: обрыв записи режет **хвост**, значит концевик
        // пропадает первым. Читатель, прочитавший тридцать два байта с конца и
        // не нашедший там опознания, знает, что файл оборван, — не отобразив ни
        // байта и не поверив ни одному числу заголовка.
        let total = self.at + TAIL as u64;
        let mut tail = Vec::with_capacity(TAIL);
        tail.extend_from_slice(&dir_off.to_le_bytes());
        tail.extend_from_slice(&dir_len.to_le_bytes());
        tail.extend_from_slice(&crc_dir.to_le_bytes());
        tail.extend_from_slice(&total.to_le_bytes());
        tail.extend_from_slice(&TAIL_MAGIC);
        self.raw(&tail)?;

        let mut head = Vec::with_capacity(HEAD);
        head.extend_from_slice(&MAGIC);
        head.extend_from_slice(&FORMAT.to_le_bytes());
        head.extend_from_slice(&self.layout.to_le_bytes());
        head.extend_from_slice(&(self.dir.len() as u32).to_le_bytes());
        head.extend_from_slice(&0u32.to_le_bytes());
        head.extend_from_slice(&dir_off.to_le_bytes());
        head.extend_from_slice(&dir_len.to_le_bytes());
        head.extend_from_slice(&total.to_le_bytes());
        head.extend_from_slice(&crc_dir.to_le_bytes());
        head.resize(HEAD, 0);

        let name_of =
            |e: std::io::Error| std::io::Error::new(e.kind(), format!("{}: {e}", self.path));
        self.sink.flush().map_err(name_of)?;
        let mut f = self.sink.into_inner().map_err(|e| e.into_error())?;
        f.seek(std::io::SeekFrom::Start(0)).map_err(name_of)?;
        f.write_all(&head).map_err(name_of)?;
        // Один `sync_all` на весь сегмент вместо двадцати шести — по одному на
        // файл. Разбор — у `indexer::write`: `write_all` возвращается, когда
        // байты приняты ядром, а не когда легли на носитель, и `head.tsv` вправе
        // объявить сегмент готовым только после того, как содержимое на диске.
        f.sync_all().map_err(|e| {
            std::io::Error::new(e.kind(), format!("{}: сброс на диск: {e}", self.path))
        })?;
        Ok(total)
    }
}

// ─────────────────────────────── чтение ───────────────────────────────

/// Разобранное оглавление.
pub struct Table {
    members: Vec<Member>,
    layout: u32,
}

/// Что заголовок с концевиком говорят про оглавление.
struct Frame {
    dir_off: u64,
    dir_len: u64,
    crc_dir: u32,
    count: usize,
    layout: u32,
}

/// Четыре байта по **постоянному** смещению внутри куска известной длины.
///
/// Ни один вызов не берёт смещение с диска: поля обрамления и записи оглавления
/// лежат по местам, заданным форматом. Длину куска обязан проверить вызывающий —
/// `frame` делает это одной охраной на все свои чтения, `entries` получает
/// записи ровно по `ENTRY` байт от `chunks_exact`.
fn u32_at(src: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([src[at], src[at + 1], src[at + 2], src[at + 3]])
}

/// То же на восемь байт — см. [`u32_at`].
fn u64_at(src: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&src[at..at + 8]);
    u64::from_le_bytes(b)
}

/// Разбирает обрамление: заголовок, концевик и то, где лежит оглавление.
///
/// Слои проверки идут от дешёвого к дорогому и в этом порядке намеренно: длина
/// файла, концевик, заголовок, и только потом сумма оглавления. Сумм **кусков**
/// здесь нет вовсе — это чтение всего сегмента на каждом открытии, и дело это
/// `check --full`.
fn frame(path: &str, head: &[u8], tail: &[u8], total: u64) -> std::io::Result<Frame> {
    // Одна охрана на все чтения полей ниже: смещения в них постоянные, и при
    // этих двух длинах ни одно не выходит за край. Числа с диска до индексации
    // не доходят вовсе — они только сверяются друг с другом.
    if head.len() != HEAD || tail.len() != TAIL {
        return Err(bad(format!(
            "{path}: обрамление подано на {} и {} байт вместо {HEAD} и {TAIL}",
            head.len(),
            tail.len()
        )));
    }
    if total < (HEAD + TAIL) as u64 {
        return Err(bad(format!(
            "{path}: {total} байт — короче заголовка с концевиком, контейнер оборван"
        )));
    }
    if tail[28..32] != TAIL_MAGIC {
        return Err(bad(format!(
            "{path}: концевика нет — запись оборвана (обрыв режет хвост, и концевик \
             пропадает первым)"
        )));
    }
    if head[..8] != MAGIC {
        return Err(bad(format!("{path}: это не контейнер сегмента — опознание не сошлось")));
    }
    let format = u32_at(head, 8);
    if format != FORMAT {
        return Err(bad(format!(
            "{path}: контейнер версии {format}, а читается {FORMAT} — собран другой версией"
        )));
    }
    let (dir_off, dir_len, crc_dir, want_total) =
        (u64_at(head, 24), u64_at(head, 32), u32_at(head, 48), u64_at(head, 40));
    // Заголовок и концевик обязаны сойтись. Разошлись — файл склеен из двух
    // разных, и верить нельзя ни одному из них.
    if (u64_at(tail, 0), u64_at(tail, 8), u32_at(tail, 16), u64_at(tail, 20))
        != (dir_off, dir_len, crc_dir, want_total)
    {
        return Err(bad(format!(
            "{path}: заголовок и концевик описывают разные оглавления — файл склеен \
             из двух контейнеров"
        )));
    }
    if want_total != total {
        return Err(bad(format!(
            "{path}: контейнер обещает {want_total} байт, а на диске {total}"
        )));
    }
    let count = u32_at(head, 16) as u64;
    if count > MAX_COUNT {
        return Err(bad(format!(
            "{path}: оглавление обещает {count} кусков при потолке {MAX_COUNT}"
        )));
    }
    // Умножение здесь не завернётся: потолок числа записей проверен строкой
    // выше, и `MAX_COUNT * ENTRY` — шесть мегабайт.
    if dir_len != count * ENTRY as u64 {
        return Err(bad(format!(
            "{path}: оглавление на {count} записей обязано занимать {} байт, а занимает {dir_len}",
            count * ENTRY as u64
        )));
    }
    // Смещение с длиной складываются проверенно, и это не перестраховка. Оба
    // числа пришли с диска, и подобрать их можно так, что завёрнутая сумма
    // попадёт **ровно** в законное `total - TAIL`: тогда сходятся все охраны
    // выше, а срез по этим числам роняет процесс. При `panic = "abort"` порченый
    // сегмент не отказывал бы, а убивал узел — вместе с проверкой целости,
    // которая пришла о нём доложить.
    let dir_end = dir_off.checked_add(dir_len).ok_or_else(|| {
        bad(format!("{path}: оглавление обещано на {dir_off}+{dir_len} — такого конца не бывает"))
    })?;
    if dir_off < HEAD as u64 || dir_end != total - TAIL as u64 {
        return Err(bad(format!(
            "{path}: оглавление лежит не между кусками и концевиком ({dir_off}+{dir_len} \
             при длине {total})"
        )));
    }
    // Раскладка не проверяется ничем и не может: контейнер ортогонален
    // кодированию записей, и сегмент раскладки 9 бывает и упакованным, и
    // рассыпанным. Она переносится как есть — [`replace`] обязан отдать
    // пересобранному контейнеру ту же, что стояла у прежнего.
    Ok(Frame { dir_off, dir_len, crc_dir, count: count as usize, layout: u32_at(head, 12) })
}

impl Table {
    /// Разбирает оглавление из байтов файла.
    ///
    /// `whole` — весь контейнер: заголовок, куски, оглавление, концевик.
    pub fn parse(path: &str, whole: &[u8]) -> std::io::Result<Table> {
        let total = whole.len() as u64;
        // Обрамление разбирается раньше, чем берётся хоть один срез по числам
        // файла: до его проверок ни одно из них ничего не значит. Само же
        // обрамление режется через `get` — обрезанный файл обязан отказать здесь
        // и с внятным текстом, а не паникой на границе среза.
        let head = whole.get(..HEAD);
        let tail = whole.len().checked_sub(TAIL).and_then(|at| whole.get(at..));
        let (Some(head), Some(tail)) = (head, tail) else {
            return Err(bad(format!(
                "{path}: {total} байт — короче заголовка с концевиком, контейнер оборван"
            )));
        };
        let f = frame(path, head, tail, total)?;
        // Вторая стена после `checked_add` в обрамлении: числа оглавления пришли
        // с диска, и переход в `usize` на тридцати двух битах режет их молча.
        // Срез берётся `get`, чтобы любая будущая дыра в обрамлении стала
        // отказом, а не смертью процесса.
        let table = match (usize::try_from(f.dir_off), usize::try_from(f.dir_len)) {
            (Ok(at), Ok(n)) => at.checked_add(n).and_then(|end| whole.get(at..end)),
            _ => None,
        };
        let Some(table) = table else {
            return Err(bad(format!(
                "{path}: оглавление обещано на {}+{}, а файла всего {total} байт",
                f.dir_off, f.dir_len
            )));
        };
        Table::entries(path, &f, table)
    }

    fn entries(path: &str, f: &Frame, table: &[u8]) -> std::io::Result<Table> {
        let got = crate::wal::crc32(table);
        if got != f.crc_dir {
            return Err(bad(format!(
                "{path}: сумма оглавления {got:08x} против обещанной {:08x} — \
                 оглавление испорчено",
                f.crc_dir
            )));
        }
        let (count, dir_off) = (f.count, f.dir_off);
        let mut members: Vec<Member> = Vec::with_capacity(count);
        // Записи режутся `chunks_exact`, а не смещением по номеру: он отдаёт
        // ровно `ENTRY` байт или ничего, и постоянные срезы внутри записи ниже
        // не могут выйти за её край при любом содержимом оглавления.
        for (i, e) in table.chunks_exact(ENTRY).enumerate() {
            let end = e[..NAME].iter().position(|&c| c == 0).unwrap_or(NAME);
            let name = std::str::from_utf8(&e[..end])
                .map_err(|_| bad(format!("{path}: имя куска {i} не UTF-8")))?;
            // Хвост имени обязан быть нулями. Иначе два разных имени —
            // `pos.bin\0x` и `pos.bin` — стали бы одним, и кусок читался бы под
            // чужим именем.
            if e[end..NAME].iter().any(|&c| c != 0) {
                return Err(bad(format!("{path}: имя куска {i} не дополнено нулями")));
            }
            valid_name(name).map_err(|e| bad(format!("{path}: {e}")))?;
            let off = u64_at(e, NAME);
            let len = u64_at(e, NAME + 8);
            if len > 0 && (off < HEAD as u64 || off > dir_off || dir_off - off < len) {
                return Err(bad(format!(
                    "{path}: кусок {name:?} обещан на {off}+{len}, а куски кончаются на {dir_off}"
                )));
            }
            if let Some(prev) = members.last() {
                if prev.name.as_str() >= name {
                    return Err(bad(format!(
                        "{path}: оглавление не отсортировано по имени ({:?} перед {name:?}) — \
                         двоичный поиск нашёл бы не тот кусок",
                        prev.name
                    )));
                }
            }
            members.push(Member { name: name.to_string(), off, len, crc: u32_at(e, NAME + 16) });
        }
        // Записей вышло не столько, сколько обещано. Через `parse` и `read` сюда
        // приходит ровно `dir_len` байт, а `dir_len` сверен с `count` в
        // обрамлении, — значит разойтись они могут только на обрезанном срезе, и
        // тогда часть кусков просто исчезла бы из оглавления молча.
        if members.len() != count {
            return Err(bad(format!(
                "{path}: оглавление обещает {count} записей, а вместило {}",
                members.len()
            )));
        }
        // Куски не вправе накладываться. Проверка не бесплатная — сортировка по
        // смещению, — но и не дорогая: кусков полсотни. Без неё съехавшее
        // смещение отдавало бы **соседний** кусок, то есть правдоподобный мусор
        // нужной длины вместо отказа.
        let mut order: Vec<usize> = (0..members.len()).filter(|&i| members[i].len > 0).collect();
        order.sort_by_key(|&i| members[i].off);
        for w in order.windows(2) {
            let (a, b) = (&members[w[0]], &members[w[1]]);
            // Нахлёст считается вычитанием, а не суммой `a.off + a.len`: оба
            // слагаемых пришли с диска и вправе завернуться, а разность при
            // порядке по смещению — нет. Обратный порядок сюда не приходит
            // (сортировка выше), но если бы пришёл, это тоже нахлёст.
            if b.off < a.off || b.off - a.off < a.len {
                return Err(bad(format!(
                    "{path}: куски {:?} и {:?} накладываются ({}+{} против {})",
                    a.name, b.name, a.off, a.len, b.off
                )));
            }
        }
        Ok(Table { members, layout: f.layout })
    }

    /// Читает оглавление позиционным чтением, не отображая файл.
    ///
    /// Нужно тем, кому нужны длины кусков, а не их содержимое: сводке размеров
    /// сегмента и описи `check`. Отображать ради этого нечего — оглавление это
    /// килобайты против гигабайтов кусков, — а область у ядра в дефиците, и
    /// именно ради неё контейнер и заведён. Три чтения: заголовок, концевик,
    /// оглавление.
    pub fn read(path: &str) -> std::io::Result<Table> {
        use std::io::{Read, Seek};
        // Открывается тем же строгим путём, каким контейнер отображают
        // (`Mapped::open`) и каким читают прочие файлы сегмента: без следования
        // по ссылке последнего компонента и с отказом на «не обычный файл».
        // Спрашивают оглавление и опись `check`, и перекладка — а та ходит по
        // каталогам, названным оператором, где под именем `seg.dat` может
        // оказаться что угодно.
        let mut file = crate::fs::open_regular(std::path::Path::new(path), true, false)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
        let total = file.metadata()?.len();
        if total < (HEAD + TAIL) as u64 {
            return Err(bad(format!(
                "{path}: {total} байт — короче заголовка с концевиком, контейнер оборван"
            )));
        }
        let mut head = [0u8; HEAD];
        file.read_exact(&mut head)?;
        let mut tail = [0u8; TAIL];
        file.seek(std::io::SeekFrom::Start(total - TAIL as u64))?;
        file.read_exact(&mut tail)?;
        let f = frame(path, &head, &tail, total)?;
        let mut table = vec![0u8; f.dir_len as usize];
        file.seek(std::io::SeekFrom::Start(f.dir_off))?;
        file.read_exact(&mut table)?;
        Table::entries(path, &f, &table)
    }

    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// Раскладка записей, объявленная заголовком контейнера.
    pub fn layout(&self) -> u32 {
        self.layout
    }

    /// Запись куска по имени. Двоичным поиском: оглавление отсортировано, и это
    /// проверено при разборе.
    pub fn find(&self, name: &str) -> Option<&Member> {
        let at = self.members.binary_search_by(|m| m.name.as_str().cmp(name)).ok()?;
        self.members.get(at)
    }
}

/// Откуда сегмент берёт свои куски.
///
/// Два состояния, и второе не заменяет первое: сегменты, собранные до
/// контейнера, остаются рассыпанными навсегда — неизменяемый сегмент на то и
/// неизменяемый, что переписать его значит собрать заново.
pub enum SegmentSource {
    /// Файл на кусок, как было всегда.
    Loose {
        dir: String,
        /// Отвергать ли жёсткую ссылку на чужой inode. Нужно при чтении
        /// недоверенного снимка — см. [`crate::mmap::Mapped::open_private`].
        private: bool,
    },
    /// Один файл, одно отображение, куски — его области.
    Packed { dir: String, map: std::sync::Arc<Mapped>, table: Table },
    /// Сегмент, собранный в память: файла под ним нет вовсе.
    ///
    /// ## Зачем он здесь, а не отдельным видом сегмента
    ///
    /// Разбор формата обязан быть один. Заведи память вторым видом сегмента —
    /// и словарь, постинги, колонки и позиции разбирались бы двумя дорогами,
    /// а разошедшись однажды, вторая начала бы отвечать неправдой на те же
    /// байты. Здесь же память подменяет **только** способ добыть кусок, а всё,
    /// что происходит с куском дальше, остаётся тем же кодом.
    ///
    /// ## Чем он отличается от прочих
    ///
    /// У него нет каталога, и [`dir`](Self::dir) отдаёт не путь, а имя для
    /// сообщений. Тот, кто открывает файлы по этому имени, ничего не откроет —
    /// и это правильный исход: молча прочесть чужой каталог хуже, чем отказать.
    Memory {
        /// Как звать сегмент в сообщениях. Путём не является намеренно.
        name: String,
        /// Куски по именам. Порядок устойчив: опись сегмента не должна
        /// перечислять их каждый раз по-новому.
        parts: std::collections::BTreeMap<String, Region>,
    },
}

/// Рассыпанный сегмент: файл на кусок.
pub fn loose(dir: &str) -> SegmentSource {
    SegmentSource::Loose { dir: dir.to_string(), private: false }
}

/// То же для недоверенного архива.
pub fn loose_private(dir: &str) -> SegmentSource {
    SegmentSource::Loose { dir: dir.to_string(), private: true }
}

/// Сегмент в памяти: куски заданы содержимым, а не каталогом.
///
/// Байты копируются — см. [`crate::mmap::Mapped::of_bytes`]. Пустой кусок от
/// отсутствующего отличается так же, как у прочих источников: перечисленный с
/// пустым содержимым существует и пуст, неперечисленного нет.
pub fn in_memory<I, N, B>(name: &str, pieces: I) -> SegmentSource
where
    I: IntoIterator<Item = (N, B)>,
    N: AsRef<str>,
    B: AsRef<[u8]>,
{
    SegmentSource::Memory {
        name: name.to_string(),
        parts: pieces
            .into_iter()
            .map(|(n, b)| (n.as_ref().to_string(), Region::of_bytes(b.as_ref())))
            .collect(),
    }
}

impl SegmentSource {
    /// Открывает сегмент. `packed` — длина контейнера, обещанная `head.tsv`;
    /// `None` — сегмент рассыпан.
    ///
    /// Обещание из заголовка здесь не для удобства. Признаком готовности
    /// сегмента остаётся `head.tsv`, и недописанный `seg.dat` без строки
    /// `container` просто невидим: сирота, о которой `check` скажет «лишний
    /// файл». Заведи признаком сам `seg.dat` — и на новое правило пришлось бы
    /// разом согласиться `load`, `Segments::reopen`, `check` и
    /// `manifest::valid_segment_dir`.
    pub fn of(dir: &str, packed: Option<u64>) -> std::io::Result<SegmentSource> {
        Self::of_as(dir, packed, false)
    }

    /// То же для недоверенного архива: ни один файл не может быть жёсткой
    /// ссылкой на inode за пределами каталога.
    pub fn of_private(dir: &str, packed: Option<u64>) -> std::io::Result<SegmentSource> {
        Self::of_as(dir, packed, true)
    }

    fn of_as(dir: &str, packed: Option<u64>, private: bool) -> std::io::Result<SegmentSource> {
        let Some(want) = packed else {
            return Ok(SegmentSource::Loose { dir: dir.to_string(), private });
        };
        let path = format!("{dir}/{FILE}");
        let m = if private { Mapped::open_private(&path)? } else { Mapped::open(&path)? };
        // Самая дешёвая проверка — первая: длину `Mapped::open_file` и так уже
        // спросил у `metadata`, и сверить её с числом из заголовка не стоит ни
        // одного системного вызова.
        if m.len_bytes() as u64 != want {
            return Err(bad(format!(
                "{path}: {} байт, а head.tsv обещает {want} — сегмент повреждён",
                m.len_bytes()
            )));
        }
        let table = Table::parse(&path, m.slice(0, m.len_bytes()))?;
        Ok(SegmentSource::Packed { dir: dir.to_string(), map: std::sync::Arc::new(m), table })
    }

    /// Каталог сегмента — или, у собранного в память, имя для сообщений.
    ///
    /// Различать их вызывающий обязан сам: см. [`is_memory`](Self::is_memory).
    /// Открыть по этому имени файл нельзя, и попытка не откроет чужой каталог —
    /// она отка́жет.
    pub fn dir(&self) -> &str {
        match self {
            SegmentSource::Loose { dir, .. } => dir,
            SegmentSource::Packed { dir, .. } => dir,
            SegmentSource::Memory { name, .. } => name,
        }
    }

    /// Стоит ли за сегментом файл. `true` — не стоит ничего, кроме памяти.
    pub fn is_memory(&self) -> bool {
        matches!(self, SegmentSource::Memory { .. })
    }

    /// Полезная нагрузка memory source в байтах. Это только уникальные
    /// `Region`-владельцы, не оценка RSS и не обещание, что все страницы
    /// резидентны; admission добавляет собственные таблицы reader отдельно.
    pub fn memory_bytes(&self) -> usize {
        match self {
            SegmentSource::Memory { parts, .. } => {
                parts.values().map(|part| part.len_bytes()).sum()
            }
            SegmentSource::Loose { .. } | SegmentSource::Packed { .. } => 0,
        }
    }

    pub fn is_packed(&self) -> bool {
        matches!(self, SegmentSource::Packed { .. })
    }

    /// Снимает residency общего immutable mapping упакованного сегмента.
    /// Loose-сегменты дополнительно обслуживаются файловым `fadvise` уровнем
    /// сервера; memory/live сегменты не имеют права попадать под reclaim.
    pub fn drop_immutable_mapping_cache(&self) -> usize {
        match self {
            SegmentSource::Packed { map, .. } => map.drop_immutable_cache(),
            SegmentSource::Loose { .. } | SegmentSource::Memory { .. } => 0,
        }
    }

    /// Имена кусков, если их можно перечислить, не обходя каталог.
    pub fn names(&self) -> Option<Vec<&str>> {
        match self {
            SegmentSource::Memory { parts, .. } => Some(parts.keys().map(|k| k.as_str()).collect()),
            SegmentSource::Packed { .. } | SegmentSource::Loose { .. } => None,
        }
    }

    /// Оглавление, если сегмент упакован.
    pub fn table(&self) -> Option<&Table> {
        match self {
            SegmentSource::Packed { table, .. } => Some(table),
            SegmentSource::Loose { .. } | SegmentSource::Memory { .. } => None,
        }
    }

    /// Кусок по имени; отказ — то же, чем отказал бы `Mapped::open`.
    ///
    /// У рассыпанного сегмента это **дословно** сегодняшнее поведение, вплоть до
    /// вида ошибки: файла нет — `NotFound`, нет прав — `PermissionDenied`. На
    /// этом стоит то, что старые сегменты работают без единого изменения.
    ///
    /// У упакованного отсутствующий кусок — **тот же отказ**, и это не
    /// единообразие ради единообразия. Подставлять здесь пустоту значило бы, что
    /// обязательный кусок, пропавший из оглавления, открывает сегмент как
    /// исправный: `index::load` берёт `postings.bin` именно этим вызовом, а
    /// сверить пустоту с длинами не с чем — у сегмента без документов все
    /// обещанные длины и так нули. Молчание доехало бы до запросов, отвечая
    /// «здесь такого нет» на всё подряд.
    ///
    /// Кому «куска нет» — законный ответ (`pos.bin`, `pos.idx`, `g*.aidx`, когда
    /// писать было нечего), тот спрашивает [`try_open`](Self::try_open).
    pub fn open(&self, name: &str) -> std::io::Result<Region> {
        match self {
            SegmentSource::Loose { dir, private } => {
                let path = format!("{dir}/{name}");
                if *private {
                    Region::of_private_file(&path)
                } else {
                    Region::of_file(&path)
                }
            }
            SegmentSource::Packed { dir, map, table } => match table.find(name) {
                Some(m) => Ok(Region::part(map, m.off as usize, m.len as usize)),
                // Тот же вид ошибки, каким отказал бы рассыпанный: на этом стоит
                // то, что вызывающему всё равно, как лежит сегмент.
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("{dir}/{FILE}: куска {name:?} нет в оглавлении контейнера"),
                )),
            },
            // Тот же вид ошибки, каким отказали бы файловые: вызывающему всё
            // равно, как лежит сегмент, — и обязано быть всё равно.
            SegmentSource::Memory { name: seg, parts } => match parts.get(name) {
                Some(r) => Ok(r.clone()),
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("{seg}: куска {name:?} нет у сегмента в памяти"),
                )),
            },
        }
    }

    /// Кусок по имени; `None` — куска нет.
    ///
    /// «Куска нет» и «кусок есть, но пустой» — разные вещи, и обе в ходу: колонка
    /// поля, которого в сегменте не оказалось, не пишется вовсе, а пустой файл
    /// той же колонки означал бы поле без значений.
    pub fn try_open(&self, name: &str) -> Option<Region> {
        match self {
            SegmentSource::Loose { .. } => self.open(name).ok(),
            SegmentSource::Packed { map, table, .. } => {
                let m = table.find(name)?;
                Some(Region::part(map, m.off as usize, m.len as usize))
            }
            SegmentSource::Memory { parts, .. } => parts.get(name).cloned(),
        }
    }

    /// Есть ли такой кусок.
    pub fn has(&self, name: &str) -> bool {
        match self {
            SegmentSource::Loose { dir, .. } => std::fs::symlink_metadata(format!("{dir}/{name}"))
                .is_ok_and(|m| m.file_type().is_file()),
            SegmentSource::Packed { table, .. } => table.find(name).is_some(),
            SegmentSource::Memory { parts, .. } => parts.contains_key(name),
        }
    }

    /// Длина куска. `None` — куска нет.
    pub fn len_of(&self, name: &str) -> Option<u64> {
        match self {
            SegmentSource::Loose { dir, .. } => std::fs::symlink_metadata(format!("{dir}/{name}"))
                .ok()
                .filter(|m| m.file_type().is_file())
                .map(|m| m.len()),
            SegmentSource::Packed { table, .. } => table.find(name).map(|m| m.len),
            SegmentSource::Memory { parts, .. } => parts.get(name).map(|r| r.len_bytes() as u64),
        }
    }

    /// Где лежит кусок, который читают позиционно, а не отображением.
    ///
    /// Такой у нас один — `<поле>.scrc`: суммы распакованных кусков читают
    /// дважды в жизни сегмента, и держать ради них область у ядра значило бы
    /// платить самым дефицитным за самое редкое.
    pub fn located(&self, name: &str) -> Option<Located> {
        match self {
            SegmentSource::Loose { dir, .. } => {
                let path = format!("{dir}/{name}");
                let len = std::fs::symlink_metadata(&path)
                    .ok()
                    .filter(|m| m.file_type().is_file())
                    .map(|m| m.len())?;
                Some(Located { len, place: Place::File { path, at: 0 } })
            }
            SegmentSource::Packed { dir, table, .. } => {
                let m = table.find(name)?;
                Some(Located {
                    len: m.len,
                    place: Place::File { path: format!("{dir}/{FILE}"), at: m.off },
                })
            }
            SegmentSource::Memory { parts, .. } => {
                let r = parts.get(name)?;
                Some(Located { len: r.len_bytes() as u64, place: Place::Memory(r.clone()) })
            }
        }
    }
}

// ──────────────────────── перекладка сегмента ────────────────────────

/// Сколько байт переносится за один заход.
///
/// Величина постоянная, и это здесь главное: сегмент доходит до четырёх
/// гигабайт, перекладка идёт на пути оператора, а сборка — с `panic = "abort"`,
/// где выделение по размеру куска не ошибка, а смерть процесса. Четверть
/// мегабайта — та же мера, что у [`crate::sums`], и по той же причине.
const COPY: usize = 256 * 1024;

/// Текст заголовка сегмента.
///
/// Тем же путём, каким его читает [`crate::index::read_head`]: без следования по
/// ссылке последнего компонента и с потолком длины. Перекладка ходит по чужим
/// каталогам, и открывать по имени что попало ей нельзя тем более.
fn head_text(dir: &str) -> std::io::Result<String> {
    let path = format!("{dir}/head.tsv");
    let bytes =
        crate::fs::read_regular_limited(std::path::Path::new(&path), 16 * 1024 * 1024, false)?;
    String::from_utf8(bytes).map_err(|e| bad(format!("{path}: не UTF-8: {e}")))
}

/// Что заголовок обещает про контейнер. `None` — сегмент рассыпан.
///
/// Правило то же, что в [`crate::index::read_head`], и негодная строка здесь
/// точно так же отказ, а не «рассыпан»: приняв её за россыпь, перекладка
/// упаковала бы поверх живого контейнера его же разложенные куски. Держать
/// правило в двух местах приходится потому, что перекладке нужен не только
/// разбор строки, но и её место в тексте — переписывать заголовок построчно
/// иначе нечем.
fn promise(path: &str, text: &str) -> std::io::Result<Option<u64>> {
    let mut found = None;
    for line in text.lines().filter(|l| l.starts_with("container\t")) {
        if found.is_some() {
            return Err(bad(format!(
                "{path}: строка container названа дважды — какой длины ждать от контейнера, \
                 из такого заголовка не следует"
            )));
        }
        let p: Vec<&str> = line.split('\t').collect();
        let len = p.get(2).and_then(|x| x.parse::<u64>().ok());
        match (p.get(1).copied() == Some(FILE), len) {
            (true, Some(len)) => found = Some(len),
            _ => {
                return Err(bad(format!(
                    "{path}: строка container обязана быть \"{FILE}\" с длиной, а не {:?}",
                    p.get(1..).unwrap_or(&[])
                )))
            }
        }
    }
    Ok(found)
}

/// Переписывает заголовок с новой строкой `container`: `None` — убрать её,
/// `Some` — поставить или поправить длину.
///
/// Это **точка перехода** у упаковки и у раскладки: вид сегмента объявляет
/// заголовок, и меняется он одним атомарным движением. Строка ставится первой —
/// туда же её кладут приём и слияние, и одинаковый заголовок у одинакового
/// сегмента дороже, чем сохранённое место старой строки.
fn rewrite_head(dir: &str, text: &str, promised: Option<u64>) -> std::io::Result<()> {
    let line = promised.map(|len| format!("container\t{FILE}\t{len}\n"));
    let mut out = String::with_capacity(text.len() + 32);
    let had = text.lines().any(|l| l.starts_with("container\t"));
    if !had {
        if let Some(l) = line.as_deref() {
            out.push_str(l);
        }
    }
    for l in text.lines() {
        if l.starts_with("container\t") {
            if let Some(fresh) = line.as_deref() {
                out.push_str(fresh);
            }
        } else {
            out.push_str(l);
            out.push('\n');
        }
    }
    crate::fs::atomic_write(std::path::Path::new(&format!("{dir}/head.tsv")), out.as_bytes(), 0o600)
}

/// Доводит до диска **имя**, а не содержимое: переименование живёт в каталоге, и
/// без этого свежее имя переживёт потерю питания не обязательно.
fn sync_dir(dir: &str) -> std::io::Result<()> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| std::io::Error::new(e.kind(), format!("{dir}: сброс каталога: {e}")))
}

/// Заводит контейнер под свободным временным именем рядом с сегментом.
///
/// Имя занято — пробуется следующее: под тем же номером процесса могла остаться
/// сирота от прошлого обрыва, и вставать на неё нельзя. Чужой заход сюда не
/// придёт — номер в имени у него другой, — а одновременные заходы разводит замок
/// писателя ([`crate::manifest::WriterLock`]), а не `create_new`.
fn beside(dir: &str, layout: u32) -> std::io::Result<(Writer, String)> {
    let mut last = None;
    for n in 0..32u32 {
        let path = format!("{dir}/.{FILE}.tmp-{}-{n}", std::process::id());
        match Writer::create_new_at(&path, layout) {
            Ok(w) => return Ok((w, path)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| bad(format!("{dir}: не выбрано временное имя для контейнера"))))
}

/// Переносит поток в начатый кусок, не собирая его в память.
fn pour(w: &mut Writer, src: &mut impl Read, path: &str, buf: &mut [u8]) -> std::io::Result<()> {
    loop {
        let n = match src.read(buf) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            // Прерванное сигналом чтение — не конец файла и не порча: повторяем.
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(std::io::Error::new(e.kind(), format!("{path}: {e}"))),
        };
        let Some(part) = buf.get(..n) else {
            return Err(bad(format!("{path}: чтение отдало {n} байт при буфере {}", buf.len())));
        };
        w.append(part)?;
    }
}

/// Длина контейнера, взятая тем же строгим путём, каким его потом читают.
///
/// `metadata` здесь не годится: она идёт **по ссылке**, и длина досталась бы от
/// того, на что ссылка указывает, тогда как `Table::read` и `refill` открывают
/// файл с `O_NOFOLLOW` и прочли бы другое. Сверка «на диске столько, сколько
/// обещает заголовок» на таком расхождении перестала бы что-либо значить.
fn container_len(path: &str) -> std::io::Result<u64> {
    let f = crate::fs::open_regular(std::path::Path::new(path), true, false)
        .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
    let m = f.metadata().map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
    Ok(m.len())
}

/// Длина и сумма файла, посчитанные одним проходом постоянным буфером.
///
/// `None` — файла нет. Это не «файл не прочёлся»: перекладке первое говорит, что
/// работа уже сделана, а второе — что про этот файл не известно ничего, и слить
/// их значило бы принять отказ ввода-вывода за доделанный шаг.
fn file_sum(path: &str, buf: &mut [u8]) -> std::io::Result<Option<(u64, u32)>> {
    let mut f = match crate::fs::open_regular(std::path::Path::new(path), true, false) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(std::io::Error::new(e.kind(), format!("{path}: {e}"))),
    };
    let mut crc = crate::wal::Crc32::new();
    let mut len: u64 = 0;
    loop {
        let n = match f.read(buf) {
            Ok(0) => break,
            Ok(n) => n,
            // Прерванное сигналом чтение — не конец файла и не порча: повторяем.
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(std::io::Error::new(e.kind(), format!("{path}: {e}"))),
        };
        let Some(part) = buf.get(..n) else {
            return Err(bad(format!("{path}: чтение отдало {n} байт при буфере {}", buf.len())));
        };
        crc.update(part);
        len = len
            .checked_add(n as u64)
            .ok_or_else(|| bad(format!("{path}: прочитанное не помещается в счётчик")))?;
    }
    Ok(Some((len, crc.finish())))
}

/// Собирает новый контейнер из старого, подменив один кусок.
fn refill(
    mut w: Writer,
    path: &str,
    table: &Table,
    piece: &str,
    body: &[u8],
) -> std::io::Result<u64> {
    // Тем же строгим путём, каким открывают куски рассыпанного сегмента ([`fill`])
    // и сам контейнер ([`Table::read`]): пересборка ходит по каталогам, названным
    // оператором, и читать по имени что попало ей нельзя тем более.
    let mut src = crate::fs::open_regular(std::path::Path::new(path), true, false)
        .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
    let mut buf = vec![0u8; COPY];
    // Порядок переноса — по смещению, а не по имени: проход по старому файлу
    // один, и головке незачем прыгать между кусками. На новый контейнер порядок
    // не влияет — оглавление писатель сортирует сам.
    let mut order: Vec<&Member> = table.members().iter().collect();
    order.sort_by_key(|m| m.off);
    for m in order {
        if m.name == piece {
            w.put(piece, body)?;
            continue;
        }
        src.seek(std::io::SeekFrom::Start(m.off))
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
        w.begin(&m.name)?;
        pour(&mut w, &mut Read::by_ref(&mut src).take(m.len), path, &mut buf)?;
        let (len, crc) = w.end_summed()?;
        // Сумма прочего куска берётся из старого оглавления — и **сверяется** с
        // тем, что перенеслось. Выбор тут не между «взять» и «пересчитать»:
        // сверять обязательно, иначе подмена одного куска станет способом
        // узаконить порчу всех остальных — старая сумма ушла бы в новое
        // оглавление, накрыв собой уже испорченные байты, и `check --full`
        // остался бы без единственного свидетеля укладки. А раз байты всё равно
        // проходят через руки, сумма по ним считается тем же движением, что и
        // перенос, и стоит она нуля лишних чтений. Записывается посчитанная —
        // после сверки она и есть старая.
        if (len, crc) != (m.len, m.crc) {
            return Err(bad(format!(
                "{path}: кусок {:?} перенёсся как {len} байт с суммой {crc:08x}, а оглавление \
                 обещало {} и {:08x}; пересобирать контейнер поверх порчи нельзя — новая \
                 сумма объявила бы её законной",
                m.name, m.len, m.crc
            )));
        }
    }
    w.finish()
}

/// Пересобирает контейнер сегмента, подменив в нём один кусок.
///
/// Отдаёт длину нового контейнера. Кусок вправе оказаться и длиннее прежнего, и
/// короче: на его место новый не встаёт, контейнер собирается заново целиком.
///
/// ## Зачем это нужно
///
/// Кусок неизменяем, а сегмент — нет: `svindb bloom` меняет число бит на
/// документ, и `idmap.bloom` обязан смениться. У упакованного сегмента положить
/// его отдельным файлом рядом нельзя — читатель контейнера к файлам рядом не
/// обращается никогда, — а писать поверх `seg.dat` нельзя тем более: узел,
/// держащий его отображённым, получил бы `SIGBUS` на первой же странице за новым
/// концом файла. Остаётся собрать новый контейнер рядом и подставить его именем.
///
/// ## Что происходит при обрыве
///
/// Новый контейнер собирается под временным именем и доводится до диска
/// (`finish` делает `sync_all` сам). До переименования на месте сегмента лежит
/// прежний, целый; временное имя остаётся сиротой, о которой `check` скажет
/// «лишний файл», а упаковка — «остаток прерванной записи, уберите». Половины
/// нового контейнера под именем `seg.dat` не бывает ни мгновения.
///
/// Остаётся одно окно, и умолчать о нём нельзя: между переименованием и записью
/// заголовка. Двух файлов одним движением файловая система не меняет, а
/// `head.tsv` обещает длину контейнера, и она изменилась. Обрыв здесь оставляет
/// сегмент, у которого `seg.dat` — **новый и целый**, а заголовок называет
/// прежнюю длину; открытие такого отказывает внятно («N байт, а head.tsv обещает
/// M»), и чинится он `svindb unpack` с последующим `svindb pack`: раскладка
/// смотрит на сам контейнер, а не на обещание заголовка, и сверяет по дороге
/// сумму каждого куска. Порядок выбран так намеренно: после переименования на
/// диске лежит то, ради чего команду звали, а отстают от него только выводимые
/// заново числа — длина в заголовке и сводка.
///
/// ## Почему заголовок и сводка обязательны
///
/// `head.tsv` — признак вида сегмента и его длины, `sums.tsv` — опись, по
/// которой `check --full` сверяет каждый кусок. Не обнови их, и свежий сегмент
/// объявят битым: заголовок — при первом же открытии, сводка — на каждом куске
/// разом, потому что смещения в новом контейнере другие.
pub fn replace(dir: &str, piece: &str, body: &[u8]) -> std::io::Result<u64> {
    let head_path = format!("{dir}/head.tsv");
    let text = head_text(dir)?;
    let Some(promised) = promise(&head_path, &text)? else {
        return Err(bad(format!(
            "{dir}: сегмент рассыпан — кусок {piece:?} лежит отдельным файлом, и подменяют \
             такой файлом, а не пересборкой контейнера"
        )));
    };
    let path = format!("{dir}/{FILE}");
    let on_disk = container_len(&path)?;
    // Расхождение с заголовком — это оборванная прошлая подмена, и пересобирать
    // поверх неё нельзя: свежий заголовок написал бы новую длину и стёр
    // единственный след того, что сегмент недоделан. Отказ называет обе длины,
    // а чинится это раскладкой с последующей упаковкой.
    if on_disk != promised {
        return Err(bad(format!(
            "{path}: {on_disk} байт, а head.tsv обещает {promised} — подмена куска здесь \
             замела бы след оборванной прошлой; разложите сегмент и упакуйте заново"
        )));
    }
    let table = Table::read(&path)?;
    if table.find(piece).is_none() {
        return Err(bad(format!(
            "{path}: куска {piece:?} в оглавлении нет — подменять нечего, а завести его \
             подменой значило бы получить сегмент состава, которого никто не заказывал"
        )));
    }
    let (w, tmp) = beside(dir, table.layout())?;
    let total = match refill(w, &path, &table, piece, body) {
        Ok(total) => total,
        Err(e) => {
            // Недособранный контейнер под временным именем не оставляют: он не
            // сирота, о которой стоит доложить, а мусор известного нам
            // происхождения.
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
    };
    if let Err(e) = std::fs::rename(&tmp, &path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(std::io::Error::new(e.kind(), format!("{tmp} → {path}: {e}")));
    }
    // Дальше идут два шага **за** точкой невозврата: контейнер уже подменён
    // именем, а заголовок ещё называет прежнюю длину — в этом окне сегмент не
    // открывается вовсе. Голая ошибка ввода-вывода здесь оставила бы оператора с
    // «нет места на устройстве» и без единого слова о том, что индекс стоит и чем
    // его поднять. Отказ сводки ниже был обёрнут внятно с самого начала, а самый
    // опасный из шагов — нет; теперь оба говорят, в каком состоянии сегмент.
    let stuck = |e: std::io::Error, what: &str| {
        std::io::Error::new(
            e.kind(),
            format!(
                "{dir}: кусок {piece:?} подменён, но {what}: {e}; сегмент в этом виде не \
                 открывается — разложите его (svindb unpack) и упакуйте заново (svindb pack), \
                 раскладка смотрит на сам контейнер, а не на обещание заголовка"
            ),
        )
    };
    sync_dir(dir).map_err(|e| stuck(e, "имя не доведено до диска"))?;
    match head_fault() {
        Some(e) => Err(e),
        None => rewrite_head(dir, &text, Some(total)),
    }
    .map_err(|e| stuck(e, "заголовок называет прежнюю длину"))?;
    // Отказ сводки не гасится — по тем же причинам, что и у раскладки: сегмент
    // уже подменён, и опись, называющая прежние куски, объявит его битым целиком.
    // Сегмент при этом цел и открывается: слова отказа обязаны отличаться от
    // слов про незаписанный заголовок, иначе оператор станет чинить работающее.
    crate::sums::write(dir).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!(
                "{dir}: кусок {piece:?} подменён, сегмент цел, но сводка не сошлась: {e}; \
                 снимет это перекладка туда и обратно (svindb unpack, затем svindb pack) — \
                 раскладка сверяет куски с оглавлением и пишет сводку заново"
            ),
        )
    })?;
    Ok(total)
}

/// Имена кусков, лежащих в каталоге сегмента россыпью.
///
/// Три имени каталога в контейнер не идут никогда — `head.tsv`, `live.bin` и
/// `sums.tsv`, — и почему, сказано в начале модуля. Сам `seg.dat`
/// тоже пропускается: если он там есть у рассыпанного сегмента, это сирота от
/// оборванной упаковки, и место ей — под переименованием, а не внутри нового
/// контейнера.
///
/// Всё прочее упаковывается, но не всё подряд: не обычный файл, имя не в UTF-8,
/// имя с точки (остаток прерванной атомарной записи) или имя, которого не примет
/// оглавление, — отказ с именем. Молча пропустить значило бы упаковать сегмент
/// **без куска**, а молча взять — впечь в контейнер навсегда либо чужой файл по
/// ссылке, либо половину чьей-то записи.
fn loose_pieces(dir: &str) -> std::io::Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    let it =
        std::fs::read_dir(dir).map_err(|e| std::io::Error::new(e.kind(), format!("{dir}: {e}")))?;
    for e in it {
        let e = e.map_err(|e| std::io::Error::new(e.kind(), format!("{dir}: {e}")))?;
        let raw = e.file_name();
        let Some(name) = raw.to_str().map(|x| x.to_string()) else {
            return Err(bad(format!("{dir}: имя {raw:?} не в UTF-8 — куском его не назвать")));
        };
        if name == "head.tsv"
            || name == crate::live::FILE
            || name == crate::sums::FILE
            || name == FILE
        {
            continue;
        }
        let kind = std::fs::symlink_metadata(e.path())
            .map_err(|e| std::io::Error::new(e.kind(), format!("{dir}/{name}: {e}")))?;
        if !kind.file_type().is_file() {
            return Err(bad(format!(
                "{dir}/{name}: не обычный файл — упаковка не пойдёт по ссылке и не станет \
                 гадать, чей это каталог; уберите его и повторите"
            )));
        }
        if name.starts_with('.') {
            return Err(bad(format!(
                "{dir}/{name}: остаток прерванной записи — упаковать его значило бы запечь \
                 половину чужого куска в контейнер навсегда; уберите его и повторите"
            )));
        }
        valid_name(&name).map_err(|e| bad(format!("{dir}: {e}")))?;
        out.push(name);
    }
    // По имени: тот же сегмент обязан паковаться одинаково, кем бы порядок ни
    // отдал `read_dir`.
    out.sort();
    Ok(out)
}

/// Что сводка обещает про куски, лежащие россыпью: имя, длина, сумма.
///
/// `None` — сводки нет вовсе. Сверять тогда не с чем, и упаковка идёт без
/// сверки: сегменты бывают собраны до появления сводки, а снятие её руками — тот
/// самый явный отказ от свидетеля, о котором говорит [`fill`]. Про саму пропажу
/// сводки докладывает `check` отдельной находкой; выдавать её за порчу куска
/// упаковка не вправе.
///
/// Из сводки берутся только строки про файлы каталога: строки про куски
/// контейнера (`seg.dat/имя`) описывают другую раскладку того же сегмента, а
/// `head.tsv` и сам `seg.dat` перекладка переписывает своими руками — их сумма
/// обязана разойтись, иначе точки перехода не было бы.
/// Подставной отказ переписи заголовка — **только** в проверочной сборке.
///
/// ## Зачем понадобился впрыск
///
/// Шаг, который он подменяет, лежит **за** точкой невозврата: контейнер уже
/// подменён именем, а заголовок ещё называет прежнюю длину. Отказ там обязан
/// сказать оператору, что индекс стоит и чем его поднять, — но отказать
/// `rewrite_head` может только от настоящей беды с носителем (нет места,
/// ошибка ввода-вывода), и снаружи такого не подстроить: каталог, подложенный
/// вместо `head.tsv`, отвергается раньше, ещё при чтении заголовка, то есть до
/// точки невозврата.
///
/// Без впрыска слова отказа остались бы написанными, но не доказанными: снять
/// обёртку — и ни один прогон этого не заметил бы. А необъяснённое поведение
/// на самом опасном шаге хуже, чем десять строк проверочного кода.
///
/// В выпуске это `None` без состояния: ветвь исчезает при сборке.
#[cfg(not(test))]
fn head_fault() -> Option<std::io::Error> {
    None
}

#[cfg(test)]
thread_local! {
    /// Одноразовый: срабатывает на ближайшей переписи и сам себя снимает.
    /// Потоко-местный — проверки идут параллельно в одном процессе, и общий
    /// переключатель ломал бы соседям заведомо исправную работу.
    static HEAD_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn head_fault() -> Option<std::io::Error> {
    HEAD_FAULT
        .with(|c| c.replace(false))
        .then(|| std::io::Error::new(std::io::ErrorKind::StorageFull, "нет места на устройстве"))
}

fn loose_witness(dir: &str) -> Option<std::collections::BTreeMap<String, (u64, u32)>> {
    let sums = crate::sums::read(dir)?;
    Some(
        sums.into_iter()
            .filter(|s| crate::sums::piece_of(&s.name).is_none())
            .filter(|s| s.name != FILE && s.name != "head.tsv")
            .map(|s| (s.name, (s.len, s.crc)))
            .collect(),
    )
}

/// Собирает контейнер из файлов каталога, сверяя каждый кусок со сводкой.
///
/// ## Почему сверка обязательна и здесь
///
/// Сумма из сводки — единственная запись про рассыпанный кусок, посчитанная не
/// сейчас. Упакуй не сверив, и один перевёрнутый бит в `norms.bin` получил бы
/// свежую сумму сразу в двух местах — в оглавлении нового контейнера и в
/// переписанной сводке, — то есть порча стала бы законным содержимым, а
/// `check --full` остался бы доволен навсегда. Ровно от этого исхода стоит сверка
/// в [`refill`] и отказ [`unpack`] раскладывать испорченное; упаковка — третья
/// сторона того же треугольника, и вести себя иначе ей нельзя.
///
/// ## Как отличить законную правку от порчи
///
/// Правка руками — законная цель раскладки, и запретить упаковку правленого
/// нельзя. Но и решать за оператора, что расхождение задумано, упаковка не
/// вправе: со стороны байт порча и правка выглядят одинаково. Поэтому решает
/// оператор, и решает **явно**: сводка — обычный текст, строка на кусок, и
/// снятая из неё строка означает «этому куску свидетеля больше нет». Тогда
/// упаковка кладёт кусок и считает ему новую сумму, а всем прочим — сверяет
/// прежнюю. Так свидетель гибнет только от руки человека, назвавшего кусок, и
/// никогда молча.
///
/// Сумма считается тем же движением, что и перенос, — писатель считает её по
/// ходу ([`Writer::end_summed`]), — и стоит сверка нуля лишних чтений.
fn fill(
    mut w: Writer,
    dir: &str,
    names: &[String],
    witness: Option<&std::collections::BTreeMap<String, (u64, u32)>>,
) -> std::io::Result<u64> {
    let mut buf = vec![0u8; COPY];
    for name in names {
        let path = format!("{dir}/{name}");
        // Открывается тем же строгим путём, каким читаются прочие файлы
        // сегмента: упаковка не вправе втянуть в контейнер файл из-за ссылки.
        let mut f = crate::fs::open_regular(std::path::Path::new(&path), true, false)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
        w.begin(name)?;
        pour(&mut w, &mut f, &path, &mut buf)?;
        let (len, crc) = w.end_summed()?;
        if let Some((want_len, want_crc)) = witness.and_then(|m| m.get(name)) {
            if (len, crc) != (*want_len, *want_crc) {
                return Err(bad(format!(
                    "{path}: кусок разошёлся со сводкой — {len} байт с суммой {crc:08x} против \
                     {want_len} и {want_crc:08x}; упаковать его значило бы выдать ему новую \
                     сумму и в оглавлении, и в сводке, то есть объявить расхождение законным \
                     навсегда. Если правка задумана — снимите строку {name:?} из \
                     {}/{} и повторите: без свидетеля упаковка кусок примет",
                    dir,
                    crate::sums::FILE
                )));
            }
        }
    }
    w.finish()
}

/// Отстала ли сводка от нынешнего вида сегмента.
///
/// Отвечает по **именам и длинам**, не читая ни байта содержимого, и это не
/// экономия ради экономии: сверка содержимого — чтение всего сегмента, до
/// четырёх гигабайт, и платить ею за ответ «работы нет» пришлось бы на каждом
/// заходе перекладки по уже переложенному индексу.
///
/// Именами обрыв между переписью заголовка и переписью сводки виден и так: у
/// упакованного сегмента сводка от прежней россыпи называет куски, которых в
/// каталоге больше нет, и молчит про `seg.dat` с его кусками; у разложенного —
/// наоборот. Расхождение же одних сумм при сошедшихся именах и длинах — не
/// отставание, а порча, и лечить её перепиской сводки нельзя; про неё говорит
/// `check --full`, а не перекладка.
///
/// Имена с точки в счёт не идут ни с той, ни с другой стороны. Спрашивается здесь
/// не «всё ли в сводке верно», а «доделана ли перекладка», а остаток прерванной
/// записи (`.seg.dat.tmp-*` от оборванной подмены) перекладка не кладёт и не
/// сносит: [`loose_pieces`] отказывается звать его куском. Считай его — и
/// упаковка на уже упакованном сегменте молча переписывала бы сводку, вписывая
/// сироту в опись как законную часть сегмента и снимая с неё заметку `check`.
fn summary_lags(dir: &str) -> std::io::Result<bool> {
    // Сводки нет — отставать нечему: её отсутствие само по себе находка `check`,
    // и заводить её перекладке не по чину.
    let Some(want) = crate::sums::read(dir) else { return Ok(false) };
    let mut have: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let it =
        std::fs::read_dir(dir).map_err(|e| std::io::Error::new(e.kind(), format!("{dir}: {e}")))?;
    for e in it {
        let e = e.map_err(|e| std::io::Error::new(e.kind(), format!("{dir}: {e}")))?;
        let name = e.file_name().to_string_lossy().to_string();
        // Тех же двух сводка не называет никогда: себя саму и карту живых, у
        // которой сумма живёт до первого гашения.
        if name == crate::sums::FILE || name == crate::live::FILE || name.starts_with('.') {
            continue;
        }
        let m = std::fs::symlink_metadata(e.path())
            .map_err(|e| std::io::Error::new(e.kind(), format!("{dir}/{name}: {e}")))?;
        if !m.file_type().is_file() {
            continue;
        }
        have.insert(name, m.len());
    }
    if have.contains_key(FILE) {
        for m in Table::read(&format!("{dir}/{FILE}"))?.members() {
            have.insert(format!("{FILE}{}{}", crate::sums::INSIDE, m.name), m.len);
        }
    }
    let named: Vec<&crate::sums::Sum> = want.iter().filter(|s| !s.name.starts_with('.')).collect();
    if named.len() != have.len() {
        return Ok(true);
    }
    Ok(named.iter().any(|s| have.get(&s.name) != Some(&s.len)))
}

/// Сверяет то, что лежит сейчас, с записями сводки про **другую** раскладку.
///
/// Перекладка меняет только имя, под которым лежат те же байты: файл `norms.bin`
/// и строка `seg.dat/norms.bin` описывают одно содержимое. Значит сводка,
/// отставшая ровно на одну перекладку, — свидетель ничуть не хуже свежей, и
/// терять его при переписи нельзя. Без этой сверки обрыв после точки перехода
/// оставался бы окном, в котором порча получает свежую сумму молча: сводка от
/// прежней раскладки не находит нынешних имён — и переписанная сводка объявила бы
/// законным всё, что лежит.
///
/// Куда смотреть, решает нынешний вид сегмента, а не сводка: у рассыпанного
/// прежним строкам про куски отвечают файлы, у упакованного прежним строкам про
/// файлы — куски. Пропущенное имя — не находка: сводка вправе описывать состав,
/// которого больше нет, ровно это и лечит перепись.
fn cross_check(dir: &str) -> std::io::Result<()> {
    let Some(want) = crate::sums::read(dir) else { return Ok(()) };
    let path = format!("{dir}/{FILE}");
    let table = match Table::read(&path) {
        Ok(t) => Some(t),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    let mut buf = vec![0u8; COPY];
    for s in want.iter() {
        match (crate::sums::piece_of(&s.name), table.as_ref()) {
            (Some(piece), None) => {
                let file = format!("{dir}/{piece}");
                let Some((len, crc)) = file_sum(&file, &mut buf)? else { continue };
                if (len, crc) != (s.len, s.crc) {
                    return Err(bad(format!(
                        "{file}: {len} байт с суммой {crc:08x} против {} и {:08x}, записанных \
                         сводкой про тот же кусок внутри {FILE}; байты от перекладки не \
                         меняются, значит это порча — доделать перекладку значило бы выдать ей \
                         свежую сумму",
                        s.len, s.crc
                    )));
                }
            }
            (None, Some(t)) if s.name != FILE && s.name != "head.tsv" => {
                let Some(m) = t.find(&s.name) else { continue };
                if (m.len, m.crc) != (s.len, s.crc) {
                    return Err(bad(format!(
                        "{path}: кусок {:?} уложен как {} байт с суммой {:08x}, а сводка \
                         записала про тот же кусок файлом {} и {:08x}; байты от перекладки не \
                         меняются, значит это порча",
                        s.name, m.len, m.crc, s.len, s.crc
                    )));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Доводит сводку до нынешнего вида сегмента, если она от него отстала.
///
/// `true` — сводка переписана. Это и есть штатный выход из окна между переписью
/// `head.tsv` и переписью `sums.tsv`: сегмент в нём цел, но описан как прежний,
/// и `check` объявляет здоровый сегмент поломанным. Пока перекладка отвечала на
/// повторный заход «уже в нужном виде» и сводки не касалась, выхода из этого
/// окна не было вовсе — только перекладка в обратную сторону и назад.
fn catch_up_summary(dir: &str, what: &str) -> std::io::Result<bool> {
    // Сперва дешёвый вопрос — по именам и длинам, — и только за ним сверка
    // содержимого: она читает весь сегмент, и платить ею за ответ «работы нет»
    // пришлось бы на каждом заходе перекладки по уже переложенному индексу.
    if !summary_lags(dir)? {
        return Ok(false);
    }
    cross_check(dir)?;
    crate::sums::write(dir).map_err(|e| {
        std::io::Error::new(e.kind(), format!("{dir}: сегмент {what}, но сводка не сошлась: {e}"))
    })?;
    Ok(true)
}

/// Доделывает упаковку, оборванную **после** точки перехода.
///
/// Заголовок уже называет контейнер, а разложенные файлы ещё лежат рядом и
/// сводка ещё описывает их. Сносится при этом только то, что доказано лишним:
/// файл, чьё имя названо оглавлением и чьи байты совпали с уложенным куском до
/// длины и суммы. Файл с тем же именем и другим содержимым — не остаток
/// упаковки: это либо правка, которую упаковка не переносила, либо порча, и
/// снести его значило бы уничтожить единственный экземпляр. О таком — отказ с
/// именем.
///
/// Всё прочее, что лежит рядом с контейнером, упаковка не трогает: имён кусков
/// оглавление за ним не признаёт, а гадать, чей это файл, — не её дело. Про него
/// докладывает `check` заметкой «лишний файл».
fn finish_pack(dir: &str, promised: u64) -> std::io::Result<bool> {
    let path = format!("{dir}/{FILE}");
    // Заголовок обязан сойтись с файлом, и здесь тем же отказом, что у [`replace`]:
    // это след оборванной подмены куска, а не оборванной упаковки. Доделывать
    // сводку поверх него значило бы объявить работу удавшейся и отчитаться
    // словом «упакован» о сегменте, который в этом виде не открывается вовсе.
    let on_disk = container_len(&path)?;
    if on_disk != promised {
        return Err(bad(format!(
            "{path}: {on_disk} байт, а head.tsv обещает {promised} — так остаётся оборванная \
             подмена куска, и упаковке её не доделать; разложите сегмент (svindb unpack) и \
             упакуйте заново"
        )));
    }
    let table = Table::read(&path)?;
    let mut buf = vec![0u8; COPY];
    let mut left = 0usize;
    for m in table.members() {
        let file = format!("{dir}/{}", m.name);
        let Some((len, crc)) = file_sum(&file, &mut buf)? else { continue };
        if (len, crc) != (m.len, m.crc) {
            return Err(bad(format!(
                "{file}: рядом с контейнером лежит файл того же имени, но другого содержимого \
                 — {len} байт с суммой {crc:08x} против {} и {:08x} в оглавлении; остатком \
                 упаковки он быть не может, и снести его упаковка не станет: другого \
                 экземпляра этих байт нет",
                m.len, m.crc
            )));
        }
        std::fs::remove_file(&file)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{file}: {e}")))?;
        left += 1;
    }
    if left > 0 {
        sync_dir(dir)?;
    }
    let fresh = catch_up_summary(dir, "упакован")?;
    Ok(left > 0 || fresh)
}

/// Доделывает раскладку, оборванную **после** точки перехода.
///
/// Заголовок уже называет россыпь, а контейнер ещё лежит рядом и сводка ещё
/// описывает его. Сносится он только после того, как **каждый** его кусок нашёлся
/// отдельным файлом той же длины и той же суммы: до этого контейнер — единственный
/// экземпляр содержимого, и снести его значило бы потерять сегмент, а не убрать
/// сироту. Сверяются файлы с записями оглавления, а не с самим контейнером:
/// оглавление и есть то обещание, ради которого раскладка вообще считает суммы.
fn finish_unpack(dir: &str) -> std::io::Result<bool> {
    let path = format!("{dir}/{FILE}");
    let table = match Table::read(&path) {
        Ok(t) => Some(t),
        // Контейнера нет — раскладка доведена до конца, осталась разве что сводка.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        // А здесь он есть, но не читается, и молчать нельзя: сирота это или
        // единственный экземпляр кусков — из нечитаемого файла не следует.
        Err(e) => {
            return Err(std::io::Error::new(
                e.kind(),
                format!(
                    "{e}; рядом с рассыпанным сегментом лежит {FILE}, и звать его сиротой \
                     раскладка не станет, пока не прочтёт: уберите его сами, если он лишний"
                ),
            ))
        }
    };
    let mut gone = false;
    if let Some(table) = table {
        let mut buf = vec![0u8; COPY];
        for m in table.members() {
            let file = format!("{dir}/{}", m.name);
            let Some((len, crc)) = file_sum(&file, &mut buf)? else {
                return Err(bad(format!(
                    "{file}: куска {:?} нет отдельным файлом, а контейнер рядом его обещает — \
                     снести контейнер значило бы снести единственный экземпляр; разложите \
                     сегмент заново, вернув строку container в head.tsv",
                    m.name
                )));
            };
            if (len, crc) != (m.len, m.crc) {
                return Err(bad(format!(
                    "{file}: {len} байт с суммой {crc:08x} против {} и {:08x} в оглавлении \
                     контейнера рядом; который из двух экземпляров верен, раскладке не \
                     известно, и снести контейнер она не станет",
                    m.len, m.crc
                )));
            }
        }
        std::fs::remove_file(&path)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
        sync_dir(dir)?;
        gone = true;
    }
    let fresh = catch_up_summary(dir, "разложен")?;
    Ok(gone || fresh)
}

/// Складывает рассыпанный сегмент в контейнер. `false` — работы не было.
///
/// Обратное [`unpack`]: то, что раскладка вернула в отдельные файлы, упаковка
/// кладёт обратно одним `seg.dat`. Порядок кусков в нём другой — по имени, а не
/// тот, в каком их писала сборка ради упреждающего чтения метаданных, — а
/// оглавление то же самое: имена, длины и суммы совпадают до байта, и на этом
/// стоит то, что раскладка с упаковкой ничего не меняют. Держится это сверкой со
/// сводкой ([`fill`]), а не обещанием: упаковка, тихо выдающая новые суммы, была
/// бы способом объявить порчу законной.
///
/// ## Обрыв на любом шаге оставляет целый сегмент
///
/// Признак вида — `head.tsv`, и меняется он одним атомарным движением. До него
/// сегмент рассыпан и цел (свежий `seg.dat` — сирота, о которой `check` скажет
/// «лишний файл»), после — упакован и цел. Половины ни в одну сторону не бывает,
/// потому что оба вида сегмента лежат рядом целиком.
///
/// Оборванное **после** точки перехода доделывает повторный заход той же
/// команды — [`finish_pack`]: сносит разложенные файлы, доказав их совпадение с
/// кусками, и доводит сводку. Отвечать на такой заход «уже упакован» и уходить
/// значило бы оставить оператора с сегментом, который цел, а `check` называет
/// поломанным, и без штатного способа это снять.
pub fn pack(dir: &str) -> std::io::Result<bool> {
    let head_path = format!("{dir}/head.tsv");
    let text = head_text(dir)?;
    if let Some(promised) = promise(&head_path, &text)? {
        return finish_pack(dir, promised);
    }
    // Раскладка записей берётся у заголовка: контейнер её только переносит и
    // ничего о ней не решает. Нет строки — сегмент собран до неё, и ноль здесь
    // означает ровно то же, что означал бы у читателя заголовка.
    let layout = text
        .lines()
        .find_map(|l| l.strip_prefix("layout\t"))
        .and_then(|x| x.trim().parse::<u32>().ok())
        .unwrap_or(0);
    let names = loose_pieces(dir)?;
    if names.is_empty() {
        return Err(bad(format!(
            "{dir}: рядом с заголовком нет ни одного куска — упаковать пустоту значило бы \
             выдать за сегмент контейнер без содержимого"
        )));
    }
    let witness = loose_witness(dir);
    // Названный сводкой, но пропавший из каталога кусок — не повод паковать
    // остальные: читатель контейнера рядом с ним не смотрит никогда, и о пропаже
    // узнать было бы неоткуда. Отказ до первого записанного байта.
    if let Some(w) = witness.as_ref() {
        for name in w.keys() {
            if !names.iter().any(|n| n == name) {
                return Err(bad(format!(
                    "{dir}/{name}: сводка называет этот кусок, а в каталоге его нет — упаковать \
                     сегмент без куска значило бы потерять его молча; верните файл или снимите \
                     строку {name:?} из {}, если куска не должно быть",
                    crate::sums::FILE
                )));
            }
        }
    }
    let path = format!("{dir}/{FILE}");
    let (w, tmp) = beside(dir, layout)?;
    let total = match fill(w, dir, &names, witness.as_ref()) {
        Ok(total) => total,
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
    };
    if let Err(e) = std::fs::rename(&tmp, &path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(std::io::Error::new(e.kind(), format!("{tmp} → {path}: {e}")));
    }
    sync_dir(dir)?;
    // Точка перехода: до этой строки сегмент рассыпан, после — упакован.
    rewrite_head(dir, &text, Some(total))?;
    // Ниже точки перехода сегмент уже упакован и цел, а недоделаны только
    // выводимые заново вещи: снос разложенных файлов и сводка. Голая ошибка
    // ввода-вывода здесь оставила бы оператора с «нет места на устройстве» рядом
    // со словами обхода «остальные не тронуты» — про сегмент, который на самом
    // деле упакован. Оба отказа поэтому говорят, в каком состоянии сегмент и чем
    // это доделывается.
    let half = |e: std::io::Error, what: &str| {
        std::io::Error::new(
            e.kind(),
            format!(
                "{dir}: сегмент упакован и цел, но {what}: {e}; повторите упаковку — она \
                 доделает начатое, сверив каждый оставшийся файл с уложенным куском"
            ),
        )
    };
    for name in names.iter() {
        let file = format!("{dir}/{name}");
        std::fs::remove_file(&file)
            .map_err(|e| half(e, &format!("разложенный {name:?} не снесён")))?;
    }
    sync_dir(dir).map_err(|e| half(e, "снос не доведён до диска"))?;
    // Отказ сводки не гасится: см. [`unpack`] — опись, называющая уже снесённые
    // файлы, объявит целый сегмент уничтоженным.
    crate::sums::write(dir).map_err(|e| half(e, "сводка не сошлась"))?;
    Ok(true)
}

/// Раскладывает контейнер обратно по файлам и снимает признак.
///
/// ## Зачем это нужно
///
/// Оба вида сегмента читаются одинаково, но правится — руками, глазами и
/// отладкой — только рассыпанный: `xxd norms.bin` работает, `xxd seg.dat` нет.
/// Разложить сегмент значит получить ровно тот вид, каким он был до контейнера,
/// и все средства, что были до, снова работают. Вход у оператора — `svindb
/// unpack <каталог>`, обратно — `svindb pack <каталог>` ([`pack`]).
///
/// Порядок тот же, что у записи, и по той же причине: сперва файлы, потом
/// заголовок, и только потом сносится контейнер. Оборвись это посередине —
/// сегмент останется упакованным и целым, потому что признаком остаётся
/// `head.tsv`, а он до последнего называет `seg.dat`.
///
/// Смотрит раскладка на сам контейнер, а не на обещанную заголовком длину, и это
/// не небрежность: контейнер описывает себя сам — опознание, концевик, полная
/// длина, сумма оглавления, — и сверяется с собой при разборе. Благодаря этому
/// раскладка чинит и сегмент с оборванной подменой куска ([`replace`]), у
/// которого заголовок отстал от файла на одно число.
///
/// ## Почему суммы сверяются именно здесь
///
/// Сумма из оглавления — единственная запись, посчитанная писателем в момент
/// укладки куска. После раскладки её больше нет нигде: новая `sums.tsv` считается
/// по тому, что уже лежит в каталоге. Разложить не сверив значит превратить порчу
/// в законное содержимое одним вызовом — и лишить `check --full` последнего
/// свидетеля. Содержимое в этот момент и так в руках, платить за сверку нечем.
///
/// Оборванное **после** точки перехода доделывает повторный заход той же команды
/// — [`finish_unpack`], — по той же причине, по какой это делает [`pack`]: иначе
/// из окна между заголовком и сводкой нет штатного выхода, а `check` в нём
/// объявляет здоровый сегмент поломанным.
pub fn unpack(dir: &str) -> std::io::Result<bool> {
    let head_path = format!("{dir}/head.tsv");
    let text = head_text(dir)?;
    if promise(&head_path, &text)?.is_none() {
        return finish_unpack(dir);
    }
    let path = format!("{dir}/{FILE}");
    let map = Mapped::open(&path)?;
    let table = Table::parse(&path, map.slice(0, map.len_bytes()))?;
    for m in table.members() {
        let body = map.slice(m.off as usize, m.len as usize);
        if body.len() as u64 != m.len {
            return Err(bad(format!("{path}: кусок {:?} короче обещанного", m.name)));
        }
        let got = crate::wal::crc32(body);
        if got != m.crc {
            return Err(bad(format!(
                "{path}: кусок {:?} — сумма {got:08x} против обещанной {:08x}; \
                 раскладывать испорченное нельзя, после раскладки эта сумма пропадёт",
                m.name, m.crc
            )));
        }
        crate::fs::atomic_write(std::path::Path::new(&format!("{dir}/{}", m.name)), body, 0o600)?;
    }
    drop(map);
    rewrite_head(dir, &text, None)?;
    // Ниже точки перехода сегмент уже разложен и цел, недоделаны только снос
    // контейнера и сводка. Отказ обязан это сказать: иначе оператор читает
    // «работа прекращена на этом сегменте» про сегмент, который разложен.
    let half = |e: std::io::Error, what: &str| {
        std::io::Error::new(
            e.kind(),
            format!(
                "{dir}: сегмент разложен и цел, но {what}: {e}; повторите раскладку — она \
                 доделает начатое, сверив каждый кусок контейнера с разложенным файлом"
            ),
        )
    };
    std::fs::remove_file(&path).map_err(|e| half(e, &format!("{FILE} не снесён")))?;
    // Отказ сводки не гасится. Прежняя `sums.tsv` называет `seg.dat` и все его
    // куски, а их уже нет: не переписав её, мы оставляем целый сегмент с описью,
    // по которой `check` насчитает поломку на каждый кусок и лишний файл на
    // каждый разложенный. Отчёт про уничтоженный сегмент вместо целого — хуже,
    // чем честный отказ здесь.
    crate::sums::write(dir).map_err(|e| half(e, "сводка не сошлась"))?;
    Ok(true)
}

/// Где лежит кусок, который читают позиционно.
#[derive(Clone)]
enum Place {
    /// В файле: путь и смещение начала. У рассыпанного сегмента смещение нулевое.
    File { path: String, at: u64 },
    /// В памяти — у сегмента, собранного без файла под ним.
    Memory(Region),
}

/// Кусок, названный местом, а не отображением.
///
/// Место закрыто намеренно: снаружи у куска есть длина и чтение по смещению, а
/// откуда именно он читается — дело источника. Пока путь торчал наружу, всякий
/// новый вид источника обязан был выдумать себе путь, которого у него нет.
#[derive(Clone)]
pub struct Located {
    place: Place,
    pub len: u64,
}

impl Located {
    /// Читает `n` байт со смещения внутри куска. За границей куска — `None`, а
    /// не чтение соседа: в контейнере за концом куска лежит **другой** кусок.
    pub fn read_at(&self, off: u64, out: &mut [u8]) -> Option<()> {
        let n = out.len() as u64;
        if off > self.len || self.len - off < n {
            return None;
        }
        match &self.place {
            Place::File { path, at } => {
                // Начало куска пришло из оглавления, то есть с диска:
                // складывать его со смещением внутри куска без проверки нельзя,
                // иначе завёрнутая сумма читала бы чужое место в файле вместо
                // отказа.
                let at = at.checked_add(off)?;
                let mut f = std::fs::File::open(path).ok()?;
                std::io::Seek::seek(&mut f, std::io::SeekFrom::Start(at)).ok()?;
                std::io::Read::read_exact(&mut f, out).ok()?;
                Some(())
            }
            // Границы уже проверены выше по `len`, а сам срез обрезает по своей
            // длине: короткого чтения здесь быть не может, но если бы стало —
            // это отказ, а не молча недочитанный буфер.
            Place::Memory(r) => {
                let src = r.slice(off as usize, out.len());
                if src.len() != out.len() {
                    return None;
                }
                out.copy_from_slice(src);
                Some(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> String {
        let d =
            std::env::temp_dir().join(format!("svindb-container-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        d.to_string_lossy().to_string()
    }

    /// Заголовок сегмента, называющий контейнер той длины, какой он вышел.
    ///
    /// Строка та же, что пишут приём и слияние: имя и длина. Без имени её не
    /// принял бы ни один читатель заголовка, а стенд, пишущий заголовок, которого
    /// в бою не бывает, проверял бы не то, что работает.
    fn head_naming(total: u64) -> String {
        format!("container\t{FILE}\t{total}\n{}", HEAD_TAIL)
    }

    /// Остаток заголовка: меньшее, что примет [`crate::index::read_head`].
    const HEAD_TAIL: &str = "docs\t1\nlayout\t9\nfield\tbody\t1\t1\ttext\n";

    fn built(d: &str, pieces: &[(&str, Vec<u8>)]) -> u64 {
        let mut w = Writer::create(d, 9).expect("контейнер");
        for (name, bytes) in pieces {
            w.put(name, bytes).expect("кусок");
        }
        w.finish().expect("закрытие")
    }

    /// Предел числа кусков проверяется на обеих соседних величинах.
    ///
    /// Наполнять настоящий контейнер шестьюдесятью пятью тысячами файлов
    /// бессмысленно и медленно; здесь прямо подготавливается уже проверенное
    /// внутреннее оглавление писателя. Проверяется именно граничная операция:
    /// последний законный кусок принимается, следующий отвергается до записи.
    #[test]
    fn the_piece_count_limit_is_exact_on_the_writer() {
        let d = dir("piece-limit");
        let mut writer = Writer::create(&d, 9).expect("контейнер");
        let placeholder =
            Member { name: "already.bin".to_string(), off: HEAD as u64, len: 0, crc: 0 };
        writer.dir = vec![placeholder; MAX_COUNT as usize - 1];
        writer.put("last.bin", b"").expect("последний законный кусок отвергнут");
        assert_eq!(writer.dir.len() as u64, MAX_COUNT);
        let error = writer.put("overflow.bin", b"").expect_err("кусок сверх предела принят");
        assert!(
            error.to_string().contains(&MAX_COUNT.to_string()),
            "отказ не называет предел: {error}"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Число кусков с диска проверяется до резервирования оглавления.
    #[test]
    fn a_piece_count_above_the_limit_is_refused_by_the_reader() {
        let count = MAX_COUNT as u32 + 1;
        let total = (HEAD + TAIL) as u64;
        let dir_off = HEAD as u64;
        let dir_len = count as u64 * ENTRY as u64;
        let mut head = Vec::with_capacity(HEAD);
        head.extend_from_slice(&MAGIC);
        head.extend_from_slice(&FORMAT.to_le_bytes());
        head.extend_from_slice(&9u32.to_le_bytes());
        head.extend_from_slice(&count.to_le_bytes());
        head.extend_from_slice(&0u32.to_le_bytes());
        head.extend_from_slice(&dir_off.to_le_bytes());
        head.extend_from_slice(&dir_len.to_le_bytes());
        head.extend_from_slice(&total.to_le_bytes());
        head.extend_from_slice(&0u32.to_le_bytes());
        head.resize(HEAD, 0);
        let mut tail = Vec::with_capacity(TAIL);
        tail.extend_from_slice(&dir_off.to_le_bytes());
        tail.extend_from_slice(&dir_len.to_le_bytes());
        tail.extend_from_slice(&0u32.to_le_bytes());
        tail.extend_from_slice(&total.to_le_bytes());
        tail.extend_from_slice(&TAIL_MAGIC);

        let error = frame("сверх-предела", &head, &tail, total)
            .err()
            .expect("читатель принял число кусков сверх предела");
        assert!(
            error.to_string().contains(&MAX_COUNT.to_string()),
            "отказ не называет предел: {error}"
        );
    }

    #[test]
    fn every_piece_comes_back_byte_for_byte() {
        let d = dir("round");
        let pieces: Vec<(&str, Vec<u8>)> = vec![
            ("postings.bin", (0..5000u32).map(|x| x as u8).collect()),
            ("norms.bin", vec![7u8; 33]),
            ("dict.blob", Vec::new()),
            ("terms.idx", vec![1, 2, 3]),
        ];
        let total = built(&d, &pieces);
        let src = SegmentSource::of(&d, Some(total)).expect("открытие");
        assert!(src.is_packed());
        for (name, want) in pieces.iter() {
            let r = src.try_open(name).unwrap_or_else(|| panic!("куска {name} нет"));
            assert_eq!(r.slice(0, r.len_bytes()), &want[..], "кусок {name} прочитан не так");
            assert_eq!(r.len_bytes(), want.len(), "длина куска {name}");
        }
        // Куска, которого не клали, нет — и это отличается от пустого куска.
        assert!(src.try_open("pos.bin").is_none(), "несуществующий кусок нашёлся");
        assert!(src.try_open("dict.blob").is_some(), "пустой кусок пропал");
        assert!(src.try_open("dict.blob").expect("пустой кусок").is_empty());
    }

    /// «Куска нет», «кусок пуст» и «кусок не прочитался» — три разных исхода, и
    /// оба вида сегмента обязаны различать их одинаково.
    ///
    /// Пока упакованный отдавал на пропавший кусок пустоту, обязательный кусок
    /// переставал быть отказом на открытии: `index::load` берёт `postings.bin`
    /// через `open`, у рассыпанного получал `NotFound` и вставал, а у
    /// упакованного шёл дальше с пустыми списками. Сверить пустоту с длинами
    /// нечем — у сегмента без документов все обещанные длины нули, — и порча
    /// доезжала до запросов.
    #[test]
    fn a_missing_piece_is_the_same_refusal_whichever_way_the_segment_lies() {
        let l = dir("missing-loose");
        std::fs::write(format!("{l}/norms.bin"), [1u8, 2, 3]).expect("файл");
        std::fs::write(format!("{l}/dict.blob"), []).expect("файл");
        let loose_src = loose(&l);

        let p = dir("missing-packed");
        let total = built(&p, &[("norms.bin", vec![1u8, 2, 3]), ("dict.blob", Vec::new())]);
        let packed = SegmentSource::of(&p, Some(total)).expect("открытие");

        for src in [&loose_src, &packed] {
            let kind =
                if src.is_packed() { "упакованный" } else { "рассыпанный" };
            // Куска нет — отказ, и вид его один на оба сегмента.
            let e = src
                .open("postings.bin")
                .err()
                .unwrap_or_else(|| panic!("{kind}: пропавший обязательный кусок принят"));
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound, "{kind}: не тот отказ");
            assert!(src.try_open("postings.bin").is_none(), "{kind}: пропавший кусок нашёлся");
            assert!(!src.has("postings.bin"), "{kind}: пропавший кусок числится");
            assert_eq!(src.len_of("postings.bin"), None, "{kind}: у пропавшего куска есть длина");

            // Кусок пуст — это содержимое, а не пропажа, и отказом быть не вправе.
            assert!(src.open("dict.blob").expect("пустой кусок отвергнут").is_empty(), "{kind}");
            assert_eq!(src.len_of("dict.blob"), Some(0), "{kind}: пустой кусок потерял длину");
            assert_eq!(src.open("norms.bin").expect("кусок").len_bytes(), 3, "{kind}");
        }
    }

    /// Ради чего всё и затевалось: сколько бы кусков ни было, отображение одно.
    ///
    /// Считается не по [`crate::mmap::live_maps`] — тот общий на процесс, и в
    /// многопоточном прогоне соседние тесты его и растят, и роняют, — а по числу
    /// держателей у самого куска: ответ точен и ни от кого не зависит.
    #[test]
    fn one_mapping_serves_every_piece() {
        const N: usize = 64;
        let names: Vec<String> = (0..N).map(|i| format!("f{i:03}.col")).collect();
        let pieces: Vec<(&str, Vec<u8>)> =
            names.iter().map(|n| (n.as_str(), vec![7u8; 100])).collect();

        // Рассыпанный: файл на кусок, отображение на файл — держатель у каждого
        // куска свой и единственный.
        let loose_dir = dir("maps-loose");
        for (n, b) in pieces.iter() {
            std::fs::write(format!("{loose_dir}/{n}"), b).expect("файл");
        }
        let src = loose(&loose_dir);
        let held: Vec<Region> = names.iter().map(|n| src.open(n).expect("кусок")).collect();
        for (i, r) in held.iter().enumerate() {
            assert_eq!(r.owners(), 1, "рассыпанный кусок {i} с кем-то делит отображение");
        }
        drop(held);
        drop(src);

        // Упакованный: держатель один на все куски плюс сам источник.
        let d = dir("maps");
        let total = built(&d, &pieces);
        let src = SegmentSource::of(&d, Some(total)).expect("открытие");
        let held: Vec<Region> = names.iter().map(|n| src.open(n).expect("кусок")).collect();
        for (i, r) in held.iter().enumerate() {
            assert_eq!(r.owners(), N + 1, "у куска {i} оказалось не одно отображение на всех");
            assert_eq!(r.len_bytes(), 100, "кусок {i} прочитан не той длины");
            assert!(r.is_mapped(), "за куском {i} нет памяти");
        }
    }

    #[test]
    fn pieces_are_aligned_and_the_directory_is_sorted() {
        let d = dir("align");
        let total =
            built(&d, &[("z.bin", vec![1; 3]), ("a.bin", vec![2; 65]), ("m.bin", vec![3; 1])]);
        let t = Table::read(&format!("{d}/{FILE}")).expect("оглавление");
        let names: Vec<&str> = t.members().iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["a.bin", "m.bin", "z.bin"], "оглавление не по имени");
        for m in t.members() {
            assert_eq!(m.off % ALIGN, 0, "кусок {} не выровнен: {}", m.name, m.off);
        }
        assert_eq!(t.members().len(), 3);
        assert_eq!(total, std::fs::metadata(format!("{d}/{FILE}")).expect("файл").len());
    }

    #[test]
    fn the_same_content_gives_the_same_bytes_whatever_the_order() {
        let a = dir("order-a");
        let b = dir("order-b");
        let pieces: Vec<(&str, Vec<u8>)> =
            vec![("one.bin", vec![1; 70]), ("two.bin", vec![2; 130]), ("three.bin", vec![3; 5])];
        built(&a, &pieces);
        let mut rev = pieces.clone();
        rev.reverse();
        built(&b, &rev);
        let ba = std::fs::read(format!("{a}/{FILE}")).expect("а");
        let bb = std::fs::read(format!("{b}/{FILE}")).expect("б");
        // Куски лягут в разном порядке — это законно, — а вот оглавление и суммы
        // обязаны совпасть: на этом стоит сверка «слитый равен собранному».
        let ta = Table::parse("а", &ba).expect("а");
        let tb = Table::parse("б", &bb).expect("б");
        let sums = |t: &Table| -> Vec<(String, u64, u32)> {
            t.members().iter().map(|m| (m.name.clone(), m.len, m.crc)).collect()
        };
        assert_eq!(sums(&ta), sums(&tb), "порядок записи изменил оглавление");
    }

    #[test]
    fn a_stream_and_a_buffer_write_the_same_piece() {
        let a = dir("stream-a");
        let b = dir("stream-b");
        let whole: Vec<u8> = (0..10_000u32).map(|x| (x * 7) as u8).collect();
        built(&a, &[("postings.bin", whole.clone())]);
        let mut w = Writer::create(&b, 9).expect("контейнер");
        w.begin("postings.bin").expect("начало");
        for part in whole.chunks(311) {
            w.append(part).expect("кусок потока");
        }
        assert_eq!(w.open_len(), whole.len() as u64, "длина открытого куска не считается");
        assert_eq!(w.end().expect("конец"), whole.len() as u64);
        w.finish().expect("закрытие");
        assert_eq!(
            std::fs::read(format!("{a}/{FILE}")).expect("а"),
            std::fs::read(format!("{b}/{FILE}")).expect("б"),
            "поток и буфер дали разные контейнеры"
        );
    }

    #[test]
    fn two_streams_at_once_are_refused_not_mixed() {
        let d = dir("two-streams");
        let mut w = Writer::create(&d, 9).expect("контейнер");
        w.begin("postings.bin").expect("начало");
        w.append(b"aaa").expect("кусок");
        let e = w.begin("pos.bin").expect_err("второй поток принят");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let e = w.put("pos.bin", b"bbb").expect_err("буфер поверх потока принят");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn an_unclosed_stream_is_refused_at_the_end() {
        let d = dir("unclosed");
        let mut w = Writer::create(&d, 9).expect("контейнер");
        w.begin("postings.bin").expect("начало");
        w.append(b"aaa").expect("кусок");
        assert!(w.finish().is_err(), "незакрытый кусок принят");
    }

    #[test]
    fn the_same_name_twice_is_refused() {
        let d = dir("dup");
        let mut w = Writer::create(&d, 9).expect("контейнер");
        w.put("a.bin", b"1").expect("первый");
        assert!(w.put("a.bin", b"2").is_err(), "второй под тем же именем принят");

        let mut memory = Sink::memory();
        memory.put("a.bin", b"1").expect("первый в памяти");
        assert!(memory.put("a.bin", b"2").is_err(), "память перезаписала кусок под тем же именем");
        assert!(memory.begin("a.bin").is_err(), "память начала поток поверх готового куска");
    }

    #[test]
    fn a_name_that_leaves_the_segment_directory_is_refused() {
        let d = dir("escape");
        let mut w = Writer::create(&d, 9).expect("контейнер");
        for name in ["../head.tsv", "a/b", "", ".", "\u{0}x"] {
            assert!(w.put(name, b"x").is_err(), "имя {name:?} принято");
            assert!(!w.has(name), "имя {name:?} попало в оглавление");
        }
        // Годное имя той же длины принимается: иначе проверка выше доказывала
        // бы только то, что писатель отвергает всё подряд.
        w.put("head.tsv.x", b"x").expect("годное имя отвергнуто");
    }

    /// Обрыв записи режет хвост: концевик пропадает первым, и это обязано
    /// узнаваться по тридцати двум байтам, а не по разбору всего файла.
    #[test]
    fn a_truncated_container_is_refused_at_every_cut() {
        let d = dir("cut");
        let total = built(&d, &[("postings.bin", vec![9; 700]), ("norms.bin", vec![8; 40])]);
        let whole = std::fs::read(format!("{d}/{FILE}")).expect("файл");
        assert_eq!(whole.len() as u64, total);
        for cut in [1usize, 8, 63, HEAD, 200, whole.len() - TAIL - 1, whole.len() - 1] {
            let e = Table::parse("обрез", &whole[..cut])
                .err()
                .unwrap_or_else(|| panic!("обрез до {cut} принят"));
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "обрез до {cut}");
        }
        // Целый — принимается: иначе проверка выше доказывала бы только то, что
        // разбор всегда отказывает.
        Table::parse("целый", &whole).expect("целый контейнер отвергнут");
    }

    /// Фальсификация каждой охраны разбора по отдельности.
    #[test]
    fn every_field_of_the_header_is_actually_checked() {
        let d = dir("forge");
        built(&d, &[("postings.bin", vec![9; 700]), ("norms.bin", vec![8; 40])]);
        let good = std::fs::read(format!("{d}/{FILE}")).expect("файл");
        let dir_off = u64::from_le_bytes(good[24..32].try_into().expect("восемь"));

        // Порча заголовка: как называется случай и что он ломает в буфере.
        type Case = (&'static str, Box<dyn Fn(&mut Vec<u8>)>);
        let mut cases: Vec<Case> = Vec::new();
        cases.push(("опознание", Box::new(|b: &mut Vec<u8>| b[0] ^= 0xFF)));
        cases.push(("версия", Box::new(|b: &mut Vec<u8>| b[8] = 2)));
        cases.push(("число записей", Box::new(|b: &mut Vec<u8>| b[16] = 9)));
        cases.push(("смещение оглавления", Box::new(|b: &mut Vec<u8>| b[24] ^= 0x40)));
        cases.push(("длина оглавления", Box::new(|b: &mut Vec<u8>| b[32] ^= 0x40)));
        cases.push(("полная длина", Box::new(|b: &mut Vec<u8>| b[40] ^= 0x40)));
        cases.push(("сумма оглавления", Box::new(|b: &mut Vec<u8>| b[48] ^= 0x01)));
        cases.push((
            "опознание концевика",
            Box::new(|b: &mut Vec<u8>| {
                let n = b.len();
                b[n - 1] ^= 0xFF;
            }),
        ));
        cases.push((
            "концевик против заголовка",
            Box::new(|b: &mut Vec<u8>| {
                let n = b.len();
                b[n - 32] ^= 0x40;
            }),
        ));
        let off = dir_off as usize;
        cases.push(("имя куска в оглавлении", Box::new(move |b: &mut Vec<u8>| b[off] = b'z')));
        cases.push(("смещение куска", Box::new(move |b: &mut Vec<u8>| b[off + NAME] ^= 0x40)));
        cases.push(("длина куска", Box::new(move |b: &mut Vec<u8>| b[off + NAME + 8] ^= 0x40)));
        cases.push((
            "порядок записей",
            Box::new(move |b: &mut Vec<u8>| {
                // Переставить две записи местами — сумма оглавления это заметит,
                // поэтому она пересчитывается: проверяется именно порядок.
                let (a, z) = (off, off + ENTRY);
                let mut one = b[a..a + ENTRY].to_vec();
                let two = b[z..z + ENTRY].to_vec();
                b[a..a + ENTRY].copy_from_slice(&two);
                b[z..z + ENTRY].copy_from_slice(&one);
                one.clear();
                let dir_len = u64::from_le_bytes(b[32..40].try_into().expect("восемь")) as usize;
                let crc = crate::wal::crc32(&b[off..off + dir_len]);
                b[48..52].copy_from_slice(&crc.to_le_bytes());
                let n = b.len();
                b[n - 16..n - 12].copy_from_slice(&crc.to_le_bytes());
            }),
        ));

        for (what, forge) in cases {
            let mut b = good.clone();
            forge(&mut b);
            assert_ne!(b, good, "подделка {what} ничего не изменила");
            let e = Table::parse("подделка", &b)
                .err()
                .unwrap_or_else(|| panic!("подделка {what} принята"));
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "подделка {what}");
        }
        Table::parse("целый", &good).expect("целый контейнер отвергнут");
    }

    /// Числа оглавления приходят с диска, и складывать их без проверки нельзя.
    ///
    /// Пара «смещение, длина» подбирается так, что в обёртке `dir_off + dir_len`
    /// даёт ровно законное `total - TAIL`: обрамление не находит ни одного
    /// расхождения, а срез по этим числам роняет процесс. При `panic = "abort"`
    /// это не отказ на порченом сегменте, а смерть узла — вместе с самой
    /// проверкой целости, которая пришла о нём доложить.
    ///
    /// Соседний `every_field_of_the_header_is_actually_checked` этого не ловит:
    /// он правит смещение на малую величину и попадает в законный отказ.
    #[test]
    fn a_wrapped_directory_offset_is_refused_and_not_a_crash() {
        const TOTAL: u64 = 4096;
        let count: u32 = 100;
        let dir_len = count as u64 * ENTRY as u64;
        let dir_off = (TOTAL - TAIL as u64).wrapping_sub(dir_len);
        assert_eq!(
            dir_off.wrapping_add(dir_len),
            TOTAL - TAIL as u64,
            "подделка не завернулась в законный конец кусков — тест ни о чём"
        );
        assert!(dir_off > TOTAL, "смещение обязано быть завёрнутым, иначе отказ законный");

        let mut whole = vec![0u8; TOTAL as usize];
        let mut head = Vec::with_capacity(HEAD);
        head.extend_from_slice(&MAGIC);
        head.extend_from_slice(&FORMAT.to_le_bytes());
        head.extend_from_slice(&9u32.to_le_bytes());
        head.extend_from_slice(&count.to_le_bytes());
        head.extend_from_slice(&0u32.to_le_bytes());
        head.extend_from_slice(&dir_off.to_le_bytes());
        head.extend_from_slice(&dir_len.to_le_bytes());
        head.extend_from_slice(&TOTAL.to_le_bytes());
        head.extend_from_slice(&0u32.to_le_bytes());
        head.resize(HEAD, 0);
        whole[..HEAD].copy_from_slice(&head);

        // Концевик обязан говорить то же, что заголовок: обе копии рядом и
        // портятся одним событием, и охрана «склеен из двух» тут не сработает.
        let mut tail = Vec::with_capacity(TAIL);
        tail.extend_from_slice(&dir_off.to_le_bytes());
        tail.extend_from_slice(&dir_len.to_le_bytes());
        tail.extend_from_slice(&0u32.to_le_bytes());
        tail.extend_from_slice(&TOTAL.to_le_bytes());
        tail.extend_from_slice(&TAIL_MAGIC);
        let at = whole.len() - TAIL;
        whole[at..].copy_from_slice(&tail);

        let e = Table::parse("завёрнутое", &whole)
            .err()
            .expect("завёрнутое смещение оглавления принято разбором");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);

        // Тот же файл на диске: позиционное чтение и открытие сегмента обязаны
        // отказать тем же исходом. Два читателя одного формата не вправе
        // смотреть на одну порчу по-разному — расхождение само по себе дефект.
        let d = dir("wrap");
        let path = format!("{d}/{FILE}");
        std::fs::write(&path, &whole).expect("файл");
        let e = Table::read(&path).err().expect("завёрнутое смещение принято чтением");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "чтение отказало не как разбор");
        let e = SegmentSource::of(&d, Some(TOTAL))
            .err()
            .expect("завёрнутое смещение принято открытием сегмента");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_length_that_disagrees_with_the_head_is_refused() {
        let d = dir("promise");
        let total = built(&d, &[("postings.bin", vec![9; 700])]);
        SegmentSource::of(&d, Some(total)).expect("честное обещание");
        for wrong in [total - 1, total + 1] {
            let e = SegmentSource::of(&d, Some(wrong))
                .err()
                .unwrap_or_else(|| panic!("обещание {wrong} принято"));
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn overlapping_pieces_are_refused() {
        let d = dir("overlap");
        built(&d, &[("a.bin", vec![1; 200]), ("b.bin", vec![2; 200])]);
        let mut b = std::fs::read(format!("{d}/{FILE}")).expect("файл");
        let off = u64::from_le_bytes(b[24..32].try_into().expect("восемь")) as usize;
        // Вторая запись начинается там же, где первая: обе описывают один и тот
        // же кусок, и один из них читался бы под чужим именем.
        let first_off = b[off + NAME..off + NAME + 8].to_vec();
        b[off + ENTRY + NAME..off + ENTRY + NAME + 8].copy_from_slice(&first_off);
        let dir_len = u64::from_le_bytes(b[32..40].try_into().expect("восемь")) as usize;
        let crc = crate::wal::crc32(&b[off..off + dir_len]);
        b[48..52].copy_from_slice(&crc.to_le_bytes());
        let n = b.len();
        b[n - 16..n - 12].copy_from_slice(&crc.to_le_bytes());
        assert!(Table::parse("нахлёст", &b).is_err(), "нахлёст принят");
    }

    /// Раскладка обязана сверить суммы, потому что после неё их не останется.
    ///
    /// Сумма из оглавления посчитана писателем в момент укладки куска; новая
    /// `sums.tsv` считается уже по разложенным файлам. Разложить не сверив значит
    /// переписать порчу набело и объявить её законным содержимым.
    #[test]
    fn unpacking_refuses_a_piece_whose_sum_does_not_match() {
        let d = dir("unpack-crc");
        let total = built(&d, &[("norms.bin", vec![4u8; 300]), ("terms.idx", vec![5u8; 40])]);
        std::fs::write(format!("{d}/head.tsv"), head_naming(total)).expect("заголовок");
        let good = std::fs::read(format!("{d}/{FILE}")).expect("файл");

        // Портится тело куска, а не оглавление: длина цела, сумма оглавления
        // сходится, разбор доволен — доложить о порче может только сумма куска.
        let at = Table::parse("целый", &good).expect("целый").find("norms.bin").expect("кусок").off
            as usize;
        let mut broken = good.clone();
        broken[at] ^= 0x20;
        Table::parse("порченый", &broken).expect("порча тела не касается разбора оглавления");
        std::fs::write(format!("{d}/{FILE}"), &broken).expect("файл");

        let e = unpack(&d).expect_err("порченый кусок разложен");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            !std::path::Path::new(&format!("{d}/norms.bin")).exists(),
            "испорченный кусок всё-таки лёг отдельным файлом"
        );
        // Оборванная раскладка обязана оставить сегмент упакованным и целым:
        // признак — `head.tsv`, и он до последнего называет контейнер.
        assert!(std::path::Path::new(&format!("{d}/{FILE}")).exists(), "контейнер снесён");

        // Целый — раскладывается: иначе проверка выше доказывала бы только то,
        // что раскладка всегда отказывает.
        std::fs::write(format!("{d}/{FILE}"), &good).expect("файл");
        assert!(unpack(&d).expect("целый сегмент не разложился"));
        assert_eq!(std::fs::read(format!("{d}/norms.bin")).expect("кусок"), vec![4u8; 300]);
        assert!(!std::path::Path::new(&format!("{d}/{FILE}")).exists(), "контейнер остался");
    }

    /// Сводка после раскладки — не украшение.
    ///
    /// Прежняя `sums.tsv` называет `seg.dat` и все его куски, а их уже нет.
    /// Погаси её отказ — и `check` доложит про целый сегмент как про
    /// уничтоженный: поломка на каждый кусок и лишний файл на каждый
    /// разложенный.
    #[test]
    fn unpacking_that_cannot_rewrite_the_summary_says_so() {
        let d = dir("unpack-sums");
        let total = built(&d, &[("norms.bin", vec![4u8; 300])]);
        std::fs::write(format!("{d}/head.tsv"), head_naming(total)).expect("заголовок");
        // Сводке некуда лечь: на её месте каталог. Подмена искусственная, отказ
        // записи — настоящий и тот же самый, каким он был бы от полного диска.
        std::fs::create_dir(format!("{d}/{}", crate::sums::FILE)).expect("подмена сводки");

        let e = unpack(&d).expect_err("несошедшаяся сводка сошла с рук");
        assert!(e.to_string().contains("сводка"), "отказ не назвал, что не сошлось: {e}");
        // Куски всё-таки разложены: отказ про сводку, а не про раскладку, и
        // текст обязан различать это для того, кто чинит сегмент руками.
        assert_eq!(std::fs::read(format!("{d}/norms.bin")).expect("кусок"), vec![4u8; 300]);
    }

    // ─────────────────── подмена куска и перекладка ───────────────────

    /// Готовый упакованный сегмент со сводкой: заголовок, контейнер, `sums.tsv`.
    fn packed(name: &str, pieces: &[(&str, Vec<u8>)]) -> String {
        let d = dir(name);
        let total = built(&d, pieces);
        std::fs::write(format!("{d}/head.tsv"), head_naming(total)).expect("заголовок");
        crate::sums::write(&d).expect("сводка");
        d
    }

    /// Имена того, что со сводкой не сошлось. Пусто — сошлось всё.
    fn unsummed(d: &str) -> Vec<String> {
        crate::sums::verify(d).expect("сводки нет вовсе").into_iter().map(|(n, _)| n).collect()
    }

    /// Опись контейнера: имя, длина, сумма — по каждому куску.
    fn listing(d: &str) -> Vec<(String, u64, u32)> {
        Table::read(&format!("{d}/{FILE}"))
            .expect("оглавление")
            .members()
            .iter()
            .map(|m| (m.name.clone(), m.len, m.crc))
            .collect()
    }

    /// Имена всего, что лежит в каталоге сегмента.
    fn names_in(d: &str) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(d)
            .expect("каталог")
            .map(|e| e.expect("запись").file_name().to_string_lossy().to_string())
            .collect();
        v.sort();
        v
    }

    /// Тело куска упакованного сегмента.
    fn body_of(d: &str, name: &str) -> Vec<u8> {
        let total = std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len();
        let src = SegmentSource::of(d, Some(total)).expect("открытие");
        let r = src.open(name).unwrap_or_else(|e| panic!("кусок {name}: {e}"));
        r.slice(0, r.len_bytes()).to_vec()
    }

    /// Подмена обязана работать в обе стороны по длине и не тронуть прочее.
    ///
    /// Длина здесь несущая: пересобранный фильтр Блума бывает и больше прежнего,
    /// и меньше, на место старого куска он не встаёт, и весь смысл пересборки
    /// контейнера в том, что вставать никуда и не надо.
    #[test]
    fn replacing_a_piece_works_whichever_way_its_length_goes() {
        for (what, fresh) in [("длиннее", vec![7u8; 5000]), ("короче", vec![7u8; 3])] {
            let d = packed(
                &format!("replace-{}", if fresh.len() > 100 { "grow" } else { "shrink" }),
                &[
                    ("postings.bin", (0..4000u32).map(|x| x as u8).collect()),
                    ("idmap.bloom", vec![1u8; 512]),
                    ("norms.bin", vec![2u8; 65]),
                    ("dict.blob", Vec::new()),
                ],
            );
            let before: Vec<(String, u64, u32)> =
                listing(&d).into_iter().filter(|(n, _, _)| n != "idmap.bloom").collect();

            let total = replace(&d, "idmap.bloom", &fresh)
                .unwrap_or_else(|e| panic!("подмена ({what}): {e}"));

            // Прочие куски — байт в байт, вместе с их суммами.
            let after: Vec<(String, u64, u32)> =
                listing(&d).into_iter().filter(|(n, _, _)| n != "idmap.bloom").collect();
            assert_eq!(after, before, "подмена ({what}) тронула соседние куски");
            for (n, _, _) in before.iter() {
                assert!(!body_of(&d, n).is_empty() || n == "dict.blob", "кусок {n} опустел");
            }
            assert_eq!(body_of(&d, "idmap.bloom"), fresh, "подменённый кусок не тот ({what})");
            assert_eq!(body_of(&d, "norms.bin"), vec![2u8; 65], "сосед испорчен ({what})");
            assert!(body_of(&d, "dict.blob").is_empty(), "пустой кусок пропал ({what})");

            // Заголовок обещает новую длину, и сегмент по ней открывается: без
            // этого `check` объявил бы свежий сегмент битым на первом же взгляде.
            let head = crate::index::read_head(&d).expect("заголовок");
            assert_eq!(head.container, Some(total), "заголовок обещает не ту длину ({what})");
            assert_eq!(
                std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len(),
                total,
                "контейнер не той длины ({what})"
            );
            assert!(SegmentSource::of(&d, Some(total)).is_ok(), "сегмент не открылся ({what})");
            // И сводка сошлась: опись обязана называть новые куски, а не прежние.
            assert!(unsummed(&d).is_empty(), "сводка не переписана ({what}): {:?}", unsummed(&d));
            // Временных имён не осталось.
            assert_eq!(
                names_in(&d),
                vec!["head.tsv".to_string(), FILE.to_string(), crate::sums::FILE.to_string()]
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>(),
                "после подмены ({what}) в каталоге лишнее"
            );
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Подмена не вправе стать способом узаконить порчу соседнего куска.
    ///
    /// Суммы прочих кусков переезжают в новое оглавление из старого. Перенеси их
    /// не сверив — и один испорченный байт в `postings.bin` получил бы свежую
    /// запись, объявляющую его законным: `check --full` сверяет куски со сводкой,
    /// а сводка считается уже по подменённому контейнеру. Свидетель укладки
    /// пропал бы навсегда, причём молча.
    #[test]
    fn replacing_a_piece_refuses_to_carry_a_corrupted_neighbour() {
        let d = packed(
            "replace-rot",
            &[("postings.bin", vec![3u8; 900]), ("idmap.bloom", vec![1u8; 64])],
        );
        let path = format!("{d}/{FILE}");
        let good = std::fs::read(&path).expect("контейнер");
        // Портится тело соседа: длина цела, оглавление сходится, разбор доволен —
        // доложить о порче может только сумма при переносе.
        let at =
            Table::parse("целый", &good).expect("целый").find("postings.bin").expect("кусок").off
                as usize;
        let mut broken = good.clone();
        broken[at] ^= 0x40;
        Table::parse("порченый", &broken).expect("порча тела не касается оглавления");
        std::fs::write(&path, &broken).expect("файл");

        let e = replace(&d, "idmap.bloom", &[9u8; 64]).expect_err("порча перенесена");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("postings.bin"), "отказ не назвал кусок: {e}");
        // Сегмент остался прежним — отказ приходит до подмены имени.
        assert_eq!(std::fs::read(&path).expect("контейнер"), broken, "контейнер подменён");
        assert_eq!(
            names_in(&d).iter().filter(|n| n.starts_with('.')).count(),
            0,
            "недособранный контейнер остался под временным именем"
        );

        // Целый сосед переносится: иначе проверка выше доказывала бы только то,
        // что подмена всегда отказывает.
        std::fs::write(&path, &good).expect("файл");
        replace(&d, "idmap.bloom", &[9u8; 64]).expect("целый сегмент не принял подмену");
    }

    /// За точкой невозврата отказ обязан сказать, в каком состоянии сегмент.
    ///
    /// ## Почему этих трёх проверок не хватало без четвёртой
    ///
    /// Два шага подмены идут после того, как имя контейнера уже подменено:
    /// заголовок и сводка. После первого сегмент не открывается вовсе, после
    /// второго — открывается и цел. Голая ошибка ввода-вывода одинакова в обоих
    /// случаях, и оператор по ней не отличит стоящий индекс от работающего.
    /// Поэтому проверяется не только то, что отказ пришёл, но и то, что слова
    /// **разные**: иначе обёртки можно было бы снять или свести в одну, и ни
    /// один прогон этого не заметил бы.
    #[test]
    fn a_refusal_past_the_point_of_no_return_says_whether_the_segment_still_opens() {
        // Перепись заголовка отказывает подставным «нет места» — разбор,
        // почему снаружи этого шага не достать, в [`head_fault`].
        // Кусок нарочно другой длины: при равной длине заголовок называл бы
        // верное число и после подмены, и «заголовок отстал» проверить было бы
        // нечем — проверка доказывала бы сама себя.
        let d = packed("replace-stuck", &[("idmap.bloom", vec![1u8; 64])]);
        HEAD_FAULT.with(|c| c.set(true));
        let e = replace(&d, "idmap.bloom", &[9u8; 200])
            .expect_err("незаписанный заголовок сошёл с рук");
        let says = e.to_string();
        assert!(says.contains("не открывается"), "отказ не сказал, что сегмент стоит: {says}");
        assert!(
            says.contains("unpack") && says.contains("pack"),
            "отказ не назвал, чем сегмент поднять: {says}"
        );
        // И сегмент действительно стоит: контейнер подменён, заголовок отстал.
        // Без этого проверка доказывала бы только текст, а не состояние.
        let on_disk = std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len();
        let text = std::fs::read_to_string(format!("{d}/head.tsv")).expect("заголовок");
        assert!(
            !text.contains(&on_disk.to_string()),
            "заголовок всё-таки переписан — точка невозврата не пройдена, и проверять нечего"
        );
        // Обещанный ремонт работает: раскладка смотрит на сам контейнер.
        unpack(&d).expect("ремонт раскладкой");
        pack(&d).expect("ремонт упаковкой");
        assert!(crate::sums::verify(&d).expect("сводка").is_empty(), "ремонт не сошёлся");

        // Сводка — шаг ниже, и там сегмент уже цел. Слова обязаны отличаться,
        // иначе оператор станет чинить работающее.
        let d = packed("replace-half", &[("idmap.bloom", vec![1u8; 64])]);
        std::fs::remove_file(format!("{d}/{}", crate::sums::FILE)).expect("сводка");
        std::fs::create_dir(format!("{d}/{}", crate::sums::FILE)).expect("подмена сводки");
        let e =
            replace(&d, "idmap.bloom", &[9u8; 64]).expect_err("несошедшаяся сводка сошла с рук");
        let says = e.to_string();
        assert!(says.contains("цел"), "отказ не сказал, что сегмент цел: {says}");
        assert!(
            !says.contains("не открывается"),
            "отказ про сводку пугает мёртвым сегментом, а сегмент живой: {says}"
        );
        // И он действительно жив: кусок подменён, контейнер читается.
        let t = Table::read(&format!("{d}/{FILE}")).expect("контейнер после подмены");
        assert_eq!(t.find("idmap.bloom").map(|m| m.len), Some(64));
    }

    /// У упаковки точка перехода своя, и слова за ней — тоже свои.
    #[test]
    fn packing_that_cannot_rewrite_the_summary_says_the_segment_is_already_packed() {
        let d = packed("pack-half", &[("norms.bin", vec![7u8; 200])]);
        unpack(&d).expect("раскладка");
        // Сводке некуда лечь. Свидетеля для сверки при упаковке это тоже
        // лишает — и то и другое законно: про пропажу сводки докладывает
        // `check`, а упаковка выдавать её за порчу куска не вправе.
        std::fs::remove_file(format!("{d}/{}", crate::sums::FILE)).expect("сводка");
        std::fs::create_dir(format!("{d}/{}", crate::sums::FILE)).expect("подмена сводки");

        let e = pack(&d).expect_err("несошедшаяся сводка сошла с рук");
        let says = e.to_string();
        assert!(
            says.contains("упакован и цел"),
            "отказ не сказал, что сегмент уже упакован: {says}"
        );
        assert!(
            says.contains("повторите упаковку"),
            "отказ не назвал, чем работа доделывается: {says}"
        );
        // Сегмент и правда упакован: россыпи не осталось, кусок на месте.
        assert!(
            !std::path::Path::new(&format!("{d}/norms.bin")).exists(),
            "разложенный файл остался, а отказ обещал упакованный сегмент"
        );
        let t = Table::read(&format!("{d}/{FILE}")).expect("контейнер после упаковки");
        assert_eq!(t.find("norms.bin").map(|m| m.len), Some(200));
    }

    /// Три разных исхода, и путать их нельзя: куска нет, сегмент рассыпан,
    /// заголовок разошёлся с файлом.
    #[test]
    fn replacing_names_exactly_what_went_wrong() {
        let d = packed("replace-refusals", &[("norms.bin", vec![1u8; 100])]);
        let e = replace(&d, "idmap.bloom", b"x").expect_err("несуществующий кусок подменён");
        assert!(e.to_string().contains("оглавлении нет"), "не тот отказ: {e}");

        // Заголовок разошёлся с файлом — след оборванной прошлой подмены, и
        // заметать его свежим заголовком нельзя.
        let text = std::fs::read_to_string(format!("{d}/head.tsv")).expect("заголовок");
        let total = std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len();
        std::fs::write(
            format!("{d}/head.tsv"),
            text.replace(&format!("\t{total}\n"), &format!("\t{}\n", total + 1)),
        )
        .expect("заголовок");
        let e = replace(&d, "norms.bin", b"x").expect_err("расхождение длин сошло с рук");
        assert!(e.to_string().contains("обещает"), "не тот отказ: {e}");
        std::fs::write(format!("{d}/head.tsv"), &text).expect("заголовок");

        // Рассыпанному сегменту подменяют файл, а не кусок, и молчать об этом
        // нельзя: иначе вызывающий решил бы, что подмена состоялась.
        let l = dir("replace-loose");
        std::fs::write(format!("{l}/head.tsv"), HEAD_TAIL).expect("заголовок");
        std::fs::write(format!("{l}/norms.bin"), [1u8; 4]).expect("файл");
        let e = replace(&l, "norms.bin", b"x").expect_err("рассыпанный принял подмену");
        assert!(e.to_string().contains("рассыпан"), "не тот отказ: {e}");
        assert_eq!(std::fs::read(format!("{l}/norms.bin")).expect("файл"), [1u8; 4]);
    }

    /// Раскладка и упаковка обязаны вернуть тот же самый состав.
    ///
    /// Порядок кусков в файле после упаковки другой — по имени, а не тот, в каком
    /// их писала сборка, — а оглавление обязано совпасть до байта: имена, длины,
    /// суммы. На этом стоит обещание, что перекладка ничего не меняет.
    #[test]
    fn unpacking_and_packing_give_back_the_very_same_pieces() {
        let d = packed(
            "roundtrip",
            &[
                ("postings.bin", (0..7000u32).map(|x| (x * 3) as u8).collect()),
                ("norms.bin", vec![2u8; 65]),
                ("dict.blob", Vec::new()),
                ("terms.idx", vec![5u8; 1]),
            ],
        );
        std::fs::write(format!("{d}/{}", crate::live::FILE), [0xFFu8; 8]).expect("карта живых");
        crate::sums::write(&d).expect("сводка");
        let before = listing(&d);

        assert!(unpack(&d).expect("раскладка"), "сегмент не был упакован");
        assert!(!std::path::Path::new(&format!("{d}/{FILE}")).exists(), "контейнер остался");
        assert!(crate::index::read_head(&d).expect("заголовок").container.is_none());
        assert!(unsummed(&d).is_empty(), "сводка россыпи не сошлась: {:?}", unsummed(&d));
        // Раскладка второй раз — не работа: сегмент уже в нужном виде.
        assert!(!unpack(&d).expect("повторная раскладка"), "разложенный разложился снова");

        assert!(pack(&d).expect("упаковка"), "сегмент не упаковался");
        assert_eq!(listing(&d), before, "перекладка изменила состав кусков");
        assert_eq!(
            names_in(&d),
            vec![
                FILE.to_string(),
                "head.tsv".to_string(),
                crate::live::FILE.to_string(),
                crate::sums::FILE.to_string()
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
            "разложенные файлы остались рядом с контейнером"
        );
        let head = crate::index::read_head(&d).expect("заголовок");
        let total = std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len();
        assert_eq!(head.container, Some(total), "заголовок не назвал новую длину");
        assert_eq!(head.layout, 9, "раскладка записей потерялась");
        assert_eq!(
            Table::read(&format!("{d}/{FILE}")).expect("оглавление").layout(),
            9,
            "контейнер собран с чужой раскладкой"
        );
        assert!(unsummed(&d).is_empty(), "сводка контейнера не сошлась: {:?}", unsummed(&d));
        assert!(SegmentSource::of(&d, Some(total)).is_ok(), "упакованный не открылся");
        // Упаковка второй раз — не работа.
        assert!(!pack(&d).expect("повторная упаковка"), "упакованный упаковался снова");
    }

    /// Упаковка не гадает: всё, чему в контейнере не место, названо отказом.
    ///
    /// Молча пропустить значило бы упаковать сегмент **без куска** — и узнать об
    /// этом было бы неоткуда, потому что читатель контейнера рядом с ним не
    /// смотрит. Молча взять — впечь в контейнер навсегда либо чужой файл по
    /// ссылке, либо половину прерванной записи.
    ///
    /// Сверяется вид отказа, а не только его наличие, и это не придирчивость.
    /// Ссылку не пропустило бы и открытие куска — `open_regular` идёт с
    /// `O_NOFOLLOW`, — но уже посреди сборки и словами `FilesystemLoop`, из
    /// которых оператору не следует ни что случилось, ни что делать. Опись
    /// каталога проверяется **до** первого записанного байта и отказывает
    /// внятно.
    #[test]
    fn packing_refuses_everything_it_cannot_honestly_call_a_piece() {
        let outside = dir("pack-outside");
        let target = format!("{outside}/чужое.bin");
        std::fs::write(&target, [8u8; 16]).expect("чужой файл");

        for (what, put) in [
            (
                "ссылка",
                Box::new(move |d: &str| {
                    // Цель ссылки обязана существовать, и проверяется это здесь же:
                    // висячая ссылка воспроизводила бы совсем другой случай — «файла
                    // нет», — а доказывать надо, что упаковка не втянет **лежащий
                    // рядом чужой файл**. Звать `dir` второй раз тем и опасно, что он
                    // сносит каталог: цель ссылки пропала бы строкой ниже её
                    // создания, и заявленный случай испарился бы молча.
                    assert!(
                        std::fs::symlink_metadata(&target).is_ok_and(|m| m.file_type().is_file()),
                        "цель ссылки не создана — случай не воспроизводится"
                    );
                    std::os::unix::fs::symlink(&target, format!("{d}/norms.bin")).expect("ссылка");
                }) as Box<dyn Fn(&str)>,
            ),
            (
                "остаток записи",
                Box::new(|d: &str| {
                    std::fs::write(format!("{d}/.norms.bin.tmp-1-2"), [0u8; 4]).expect("остаток");
                }),
            ),
            (
                "каталог",
                Box::new(|d: &str| {
                    std::fs::create_dir(format!("{d}/сюрприз")).expect("каталог");
                }),
            ),
        ] {
            let d = dir("pack-refuse");
            std::fs::write(format!("{d}/head.tsv"), HEAD_TAIL).expect("заголовок");
            std::fs::write(format!("{d}/terms.bin"), [1u8; 32]).expect("кусок");
            put(&d);
            let e = pack(&d).err().unwrap_or_else(|| panic!("{what}: упаковано молча"));
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "{what}");
            assert!(
                !std::path::Path::new(&format!("{d}/{FILE}")).exists(),
                "{what}: контейнер всё-таки собран"
            );
            assert!(
                crate::index::read_head(&d).expect("заголовок").container.is_none(),
                "{what}: сегмент объявлен упакованным"
            );
            std::fs::remove_dir_all(&d).ok();
        }

        // Тот же каталог без сюрпризов упаковывается: иначе проверки выше
        // доказывали бы только то, что упаковка всегда отказывает.
        let d = dir("pack-refuse");
        std::fs::write(format!("{d}/head.tsv"), HEAD_TAIL).expect("заголовок");
        std::fs::write(format!("{d}/terms.bin"), [1u8; 32]).expect("кусок");
        assert!(pack(&d).expect("чистый каталог не упаковался"));
        std::fs::remove_dir_all(&outside).ok();
    }

    /// Путь сводки в каталоге сегмента — одной строкой, чтобы не повторять его.
    fn sums_path(d: &str) -> String {
        format!("{d}/{}", crate::sums::FILE)
    }

    /// Упаковка не вправе выдать порче свежую сумму.
    ///
    /// Случай целиком: упакованный сегмент разложили, перевернули бит в
    /// `norms.bin` — и сводка эту порчу называет. Упакуй как есть, и новая сумма
    /// ляжет сразу в двух местах: в оглавлении нового контейнера и в переписанной
    /// сводке. `check --full` после этого доволен навсегда, а единственный
    /// свидетель укладки уничтожен молча — ровно тот исход, ради предотвращения
    /// которого сверяет суммы [`refill`] и отказывается раскладывать испорченное
    /// [`unpack`].
    ///
    /// Вторая половина теста не менее важная: правка руками — законная цель
    /// раскладки, и упаковать правленое обязано быть можно. Явный отказ от
    /// свидетеля — снятая из сводки строка — упаковку не останавливает.
    #[test]
    fn packing_refuses_a_piece_that_diverges_from_the_summary() {
        let d = packed("pack-rot", &[("norms.bin", vec![4u8; 300]), ("terms.idx", vec![5u8; 40])]);
        assert!(unpack(&d).expect("раскладка"), "сегмент не был упакован");
        assert!(unsummed(&d).is_empty(), "стенд начал с несошедшейся сводки");

        let file = format!("{d}/norms.bin");
        let mut bent = std::fs::read(&file).expect("кусок");
        bent[7] ^= 0x20;
        std::fs::write(&file, &bent).expect("порча");
        // Порчу видно сводке — иначе упаковке нечем было бы её заметить, и тест
        // проверял бы не сверку, а собственный стенд.
        assert_eq!(unsummed(&d), vec!["norms.bin".to_string()], "сводка порчу не назвала");
        let witness = std::fs::read_to_string(sums_path(&d)).expect("сводка");

        let e = pack(&d).expect_err("порча упакована набело");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("norms.bin"), "отказ не назвал кусок: {e}");
        // Свидетель на месте, сегмент рассыпан, контейнера нет: отказ приходит до
        // точки перехода, и чинить после него нечего.
        assert_eq!(
            std::fs::read_to_string(sums_path(&d)).expect("сводка"),
            witness,
            "сводка переписана вопреки отказу"
        );
        assert!(!std::path::Path::new(&format!("{d}/{FILE}")).exists(), "контейнер собран");
        assert!(
            crate::index::read_head(&d).expect("заголовок").container.is_none(),
            "сегмент объявлен упакованным"
        );
        assert_eq!(
            names_in(&d).iter().filter(|n| n.starts_with('.')).count(),
            0,
            "недособранный контейнер остался под временным именем"
        );

        // Сняли строку про кусок — сказали «свидетеля этому куску больше нет».
        // Прочие куски по-прежнему сверяются, и упаковка идёт.
        let thinner: String = witness
            .lines()
            .filter(|l| !l.starts_with("norms.bin\t"))
            .map(|l| format!("{l}\n"))
            .collect();
        assert_ne!(thinner, witness, "строка про кусок не снята — вторая половина ни о чём");
        std::fs::write(sums_path(&d), &thinner).expect("сводка");
        assert!(pack(&d).expect("правленое не упаковалось"), "упаковка не сочла это работой");
        assert_eq!(body_of(&d, "norms.bin"), bent, "упаковано не то, что лежало");
        assert!(unsummed(&d).is_empty(), "сводка не переписана: {:?}", unsummed(&d));
    }

    /// Кусок, названный сводкой и пропавший из каталога, — отказ, а не упаковка
    /// остального.
    ///
    /// Молча упаковать значило бы потерять его навсегда и без следа: читатель
    /// контейнера рядом с ним не смотрит никогда, а свежая сводка про пропавший
    /// кусок больше не помянет.
    #[test]
    fn packing_refuses_a_segment_that_lost_a_piece_the_summary_names() {
        let d = packed("pack-lost", &[("norms.bin", vec![4u8; 300]), ("terms.idx", vec![5u8; 40])]);
        assert!(unpack(&d).expect("раскладка"));
        std::fs::remove_file(format!("{d}/terms.idx")).expect("пропажа");

        let e = pack(&d).expect_err("сегмент упакован без куска");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("terms.idx"), "отказ не назвал кусок: {e}");
        assert!(!std::path::Path::new(&format!("{d}/{FILE}")).exists(), "контейнер собран");

        // Кусок вернули — упаковка идёт: иначе проверка выше доказывала бы только
        // то, что упаковка всегда отказывает.
        std::fs::write(format!("{d}/terms.idx"), vec![5u8; 40]).expect("возврат");
        assert!(pack(&d).expect("возвращённый кусок не упаковался"));
    }

    /// Окно между переписью `head.tsv` и переписью `sums.tsv` чинится повторным
    /// запуском той же команды.
    ///
    /// Обрыв здесь оставляет сегмент целым, но описанным как прежний: `check`
    /// объявляет здоровый сегмент **поломанным** (пропавшие имена сводки идут в
    /// поломку, а не в заметку). Пока перекладка отвечала на повторный заход «уже
    /// в нужном виде» и сводки не касалась, штатного выхода из этого окна не было
    /// вовсе — только перекладка в обратную сторону и назад.
    #[test]
    fn a_summary_left_behind_by_an_interrupted_repack_is_caught_up_by_running_again() {
        let d = packed("catchup", &[("norms.bin", vec![4u8; 300]), ("terms.idx", vec![5u8; 40])]);

        // Упаковка: сводка осталась от прежней россыпи.
        assert!(unpack(&d).expect("раскладка"));
        let loose_sums = std::fs::read_to_string(sums_path(&d)).expect("сводка россыпи");
        assert!(pack(&d).expect("упаковка"));
        std::fs::write(sums_path(&d), &loose_sums).expect("обрыв перед сводкой");
        assert!(!unsummed(&d).is_empty(), "стенд не воспроизвёл окно");
        assert!(pack(&d).expect("повторный заход отказал"), "упаковка сказала «уже упакован»");
        assert!(unsummed(&d).is_empty(), "сводка так и от россыпи: {:?}", unsummed(&d));
        assert!(!pack(&d).expect("третий заход"), "доделанное доделалось снова");
        // И сегмент от лечения не изменился: чинилась сводка, а не содержимое.
        assert_eq!(body_of(&d, "norms.bin"), vec![4u8; 300], "содержимое куска тронуто");

        // Раскладка: сводка осталась от контейнера.
        let packed_sums = std::fs::read_to_string(sums_path(&d)).expect("сводка контейнера");
        assert!(unpack(&d).expect("раскладка"));
        std::fs::write(sums_path(&d), &packed_sums).expect("обрыв перед сводкой");
        assert!(!unsummed(&d).is_empty(), "стенд не воспроизвёл окно");
        assert!(unpack(&d).expect("повторный заход отказал"), "раскладка сказала «уже разложен»");
        assert!(unsummed(&d).is_empty(), "сводка так и от контейнера: {:?}", unsummed(&d));
        assert!(!unpack(&d).expect("третий заход"), "доделанное доделалось снова");
        assert_eq!(std::fs::read(format!("{d}/norms.bin")).expect("кусок"), vec![4u8; 300]);
    }

    /// Доделывая перекладку, сводку переписывают только поверх сошедшегося.
    ///
    /// Сводка, отставшая на одну перекладку, — свидетель ничуть не хуже свежей:
    /// байты от перекладки не меняются, меняется только имя, под которым они
    /// лежат. Строка `seg.dat/norms.bin` про разложенный `norms.bin` говорит то
    /// же, что сказала бы строка `norms.bin`. Не сверься с ней — и окно после
    /// точки перехода стало бы вторым входом для той же беды: порча получает
    /// свежую сумму молча.
    #[test]
    fn catching_up_the_summary_refuses_to_do_it_over_corruption() {
        let d = packed("catchup-rot", &[("norms.bin", vec![4u8; 300])]);
        let packed_sums = std::fs::read_to_string(sums_path(&d)).expect("сводка контейнера");
        assert!(unpack(&d).expect("раскладка"));
        // Обрыв перед переписью сводки — и порча уже разложенного куска.
        std::fs::write(sums_path(&d), &packed_sums).expect("обрыв перед сводкой");
        let file = format!("{d}/norms.bin");
        let mut bent = std::fs::read(&file).expect("кусок");
        bent[11] ^= 0x08;
        std::fs::write(&file, &bent).expect("порча");

        let e = unpack(&d).expect_err("порча получила свежую сумму");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("norms.bin"), "отказ не назвал кусок: {e}");
        assert_eq!(
            std::fs::read_to_string(sums_path(&d)).expect("сводка"),
            packed_sums,
            "сводка переписана вопреки отказу"
        );

        // Тот же обрыв без порчи доделывается: иначе проверка выше доказывала бы
        // только то, что доделка всегда отказывает.
        std::fs::write(&file, vec![4u8; 300]).expect("возврат");
        assert!(unpack(&d).expect("целое не доделалось"));
        assert!(unsummed(&d).is_empty(), "сводка не доведена: {:?}", unsummed(&d));
    }

    /// Разложенный файл рядом с контейнером снимает повторная упаковка — но
    /// только доказав, что он и есть уложенный кусок.
    ///
    /// Файл того же имени с другим содержимым остатком упаковки быть не может:
    /// это либо правка, которую упаковка не переносила, либо порча. Снести его
    /// значило бы уничтожить единственный экземпляр этих байт, и потому — отказ с
    /// именем.
    #[test]
    fn a_leftover_loose_file_is_removed_only_once_proved_to_be_the_piece() {
        let d =
            packed("catchup-left", &[("norms.bin", vec![4u8; 300]), ("terms.idx", vec![5u8; 40])]);
        std::fs::write(format!("{d}/norms.bin"), vec![9u8; 300]).expect("подкидыш");
        let e = pack(&d).expect_err("чужой файл снесён молча");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("norms.bin"), "отказ не назвал файл: {e}");
        assert_eq!(
            std::fs::read(format!("{d}/norms.bin")).expect("файл"),
            vec![9u8; 300],
            "файл всё-таки снесён"
        );

        // Он же байт в байт как кусок: вот это остаток, и его снимают.
        std::fs::write(format!("{d}/norms.bin"), body_of(&d, "norms.bin")).expect("остаток");
        assert!(pack(&d).expect("остаток не снят"), "упаковка не сочла это работой");
        assert!(
            !std::path::Path::new(&format!("{d}/norms.bin")).exists(),
            "остаток остался рядом с контейнером"
        );
        assert!(unsummed(&d).is_empty(), "сводка не сошлась: {:?}", unsummed(&d));
        assert_eq!(body_of(&d, "norms.bin"), vec![4u8; 300], "кусок тронут");
    }

    /// Контейнер, оставшийся рядом с разложенными файлами, снимает повторная
    /// раскладка — но только доказав, что каждый кусок разложен.
    ///
    /// Пока это не доказано, контейнер — единственный экземпляр содержимого:
    /// снести его значило бы потерять сегмент, а не убрать сироту.
    #[test]
    fn a_leftover_container_is_removed_only_once_every_piece_is_proved_unpacked() {
        let d = packed(
            "catchup-orphan",
            &[("norms.bin", vec![4u8; 300]), ("terms.idx", vec![5u8; 40])],
        );
        let keep = std::fs::read(format!("{d}/{FILE}")).expect("контейнер");
        assert!(unpack(&d).expect("раскладка"));
        let path = format!("{d}/{FILE}");

        for (what, instead) in
            [("куска нет отдельным файлом", None), ("кусок разложен другим", Some(vec![6u8; 40]))]
        {
            std::fs::write(&path, &keep).expect("сирота");
            std::fs::remove_file(format!("{d}/terms.idx")).ok();
            if let Some(body) = instead {
                std::fs::write(format!("{d}/terms.idx"), body).expect("правка");
            }
            let e = unpack(&d).err().unwrap_or_else(|| panic!("{what}: контейнер снесён"));
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "{what}");
            assert!(e.to_string().contains("terms.idx"), "{what}: отказ не назвал кусок: {e}");
            assert_eq!(std::fs::read(&path).expect("контейнер"), keep, "{what}: контейнер тронут");
        }

        // Всё разложено байт в байт — сироту снимают.
        std::fs::write(format!("{d}/terms.idx"), vec![5u8; 40]).expect("возврат");
        assert!(unpack(&d).expect("сирота не снята"), "раскладка не сочла это работой");
        assert!(!std::path::Path::new(&path).exists(), "контейнер остался");
        assert!(unsummed(&d).is_empty(), "сводка не сошлась: {:?}", unsummed(&d));
        assert_eq!(std::fs::read(format!("{d}/norms.bin")).expect("кусок"), vec![4u8; 300]);
    }

    /// Сирота под временным именем — не повод объявить упаковку работой.
    ///
    /// Остаток прерванной подмены (`.seg.dat.tmp-*`) перекладка не кладёт и не
    /// сносит: [`loose_pieces`] отказывается звать его куском. Считай его в
    /// вопросе «доделана ли перекладка» — и повторная упаковка молча вписала бы
    /// сироту в сводку как законную часть сегмента, сняв с неё заметку `check` и
    /// отчитавшись словом «упакован» о работе, которой не было.
    #[test]
    fn an_orphan_under_a_temporary_name_is_not_mistaken_for_unfinished_work() {
        let d = packed("orphan-tmp", &[("norms.bin", vec![2u8; 96])]);
        let orphan = format!("{d}/.{FILE}.tmp-777-0");
        std::fs::write(&orphan, b"aaaa").expect("сирота");
        let witness = std::fs::read_to_string(sums_path(&d)).expect("сводка");

        assert!(!pack(&d).expect("упаковка отказала"), "сирота принята за недоделанную работу");
        assert_eq!(
            std::fs::read_to_string(sums_path(&d)).expect("сводка"),
            witness,
            "сирота вписана в сводку как часть сегмента"
        );
        assert!(std::path::Path::new(&orphan).exists(), "сирота снесена упаковкой");
    }

    /// Оборванную подмену куска упаковка не доделывает, а называет.
    ///
    /// Заголовок отстал от контейнера на одно число — сегмент в этом виде не
    /// открывается вовсе. Доведи упаковка сводку до нынешнего контейнера и скажи
    /// «упакован», и отчёт объявил бы работу удавшейся ровно там, где индекс
    /// стоит. Чинится это раскладкой с обратной упаковкой: раскладка смотрит на
    /// сам контейнер, а не на обещание заголовка.
    #[test]
    fn packing_does_not_finish_over_an_interrupted_piece_swap() {
        let d = packed("finish-window", &[("norms.bin", vec![3u8; 128])]);
        let total = std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len();
        let good = std::fs::read_to_string(format!("{d}/head.tsv")).expect("заголовок");
        std::fs::write(
            format!("{d}/head.tsv"),
            good.replace(&format!("\t{total}\n"), &format!("\t{}\n", total - 64)),
        )
        .expect("заголовок");

        let e = pack(&d).expect_err("упаковка доделала оборванную подмену");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("обещает"), "отказ не назвал расхождение: {e}");
        // И обещанный путь ремонта работает: раскладка смотрит на сам контейнер.
        assert!(unpack(&d).expect("раскладка не подняла сегмент"));
        assert!(pack(&d).expect("обратная упаковка"));
        assert!(unsummed(&d).is_empty(), "сводка не сошлась: {:?}", unsummed(&d));
        assert_eq!(body_of(&d, "norms.bin"), vec![3u8; 128], "содержимое куска изменилось");
    }

    /// Контейнер на пути пересборки открывают тем же строгим путём, каким его
    /// читают все прочие.
    ///
    /// Рядом в этом же модуле [`head_text`] и [`fill`] ужесточены ровно от этого и
    /// с тем же обоснованием: перекладка ходит по каталогам, названным оператором,
    /// и открывать по имени что попало ей нельзя тем более. Наружу пересборка при
    /// этом не пишет — переименование заменяет саму ссылку, — но прочитанное по
    /// ссылке уехало бы в новый контейнер как содержимое сегмента, а длина,
    /// снятая с чужого файла, сверялась бы с заголовком этого.
    #[test]
    fn the_container_is_never_opened_through_a_symlink() {
        let real = packed("nofollow-real", &[("norms.bin", vec![7u8; 200])]);
        let d = dir("nofollow");
        // Длина в заголовке нарочно не та, что у цели ссылки: сверка длин, снятых
        // по ссылке, прошла бы незамеченной, и отказ пришёл бы позже и не о том.
        std::fs::write(format!("{d}/head.tsv"), head_naming(1)).expect("заголовок");
        std::os::unix::fs::symlink(format!("{real}/{FILE}"), format!("{d}/{FILE}"))
            .expect("ссылка");

        Table::read(&format!("{d}/{FILE}")).err().expect("оглавление прочитано по ссылке");
        let e = replace(&d, "norms.bin", b"x").expect_err("подмена пошла по ссылке");
        assert!(
            !e.to_string().contains("обещает"),
            "длина снята с цели ссылки, а не с файла, который потом прочтут: {e}"
        );
        // Открытие сегмента и раскладка отказывают тем же: два читателя одного
        // формата не вправе смотреть на подмену по-разному.
        assert!(SegmentSource::of(&d, Some(1)).is_err(), "сегмент открыт по ссылке");
        assert!(unpack(&d).is_err(), "раскладка пошла по ссылке");
        // Цель ссылки не тронута, и читается она по своему имени: иначе проверки
        // выше доказывали бы только то, что чтение всегда отказывает.
        Table::read(&format!("{real}/{FILE}")).expect("настоящий контейнер отвергнут");
        std::fs::remove_dir_all(&real).ok();
    }

    /// Заголовок, назвавший контейнер дважды, — отказ на всех трёх путях
    /// перекладки.
    ///
    /// Длина контейнера — то, чем открытие сегмента отличает целый файл от
    /// оборванной подмены куска. Прими перекладка первую строку, а читатель
    /// заголовка последнюю (или наоборот), и они разошлись бы в том, что считать
    /// сегментом: один сверяет одну длину, другой пишет другую. Из такого
    /// заголовка длина не следует, и молчаливый выбор одной из строк — худшее из
    /// решений.
    #[test]
    fn a_head_that_names_the_container_twice_is_refused() {
        let d = packed("promise-twice", &[("norms.bin", vec![1u8; 100])]);
        let total = std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len();
        let good = std::fs::read_to_string(format!("{d}/head.tsv")).expect("заголовок");
        // Вторая строка называет ту же длину: расхождение длин здесь ни при чём,
        // отказывать обязано само удвоение.
        std::fs::write(format!("{d}/head.tsv"), format!("{good}container\t{FILE}\t{total}\n"))
            .expect("заголовок");

        let cases: Vec<(&str, Option<std::io::Error>)> = vec![
            ("подмена", replace(&d, "norms.bin", b"x").err()),
            ("раскладка", unpack(&d).err()),
            ("упаковка", pack(&d).err()),
        ];
        for (what, e) in cases {
            let e = e.unwrap_or_else(|| panic!("{what}: удвоенная строка container принята"));
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "{what}");
            assert!(e.to_string().contains("дважды"), "{what}: не тот отказ: {e}");
        }
        // Сегмент не тронут: отказ приходит до первого движения.
        assert_eq!(
            std::fs::metadata(format!("{d}/{FILE}")).expect("контейнер").len(),
            total,
            "контейнер тронут вопреки отказу"
        );

        // Одна строка — работа идёт: иначе проверки выше доказывали бы только то,
        // что перекладка всегда отказывает.
        std::fs::write(format!("{d}/head.tsv"), &good).expect("заголовок");
        assert!(unpack(&d).expect("одна строка container отвергнута"));
    }

    /// Ради чего вся перекладка: индекс отвечает то же самое.
    ///
    /// Сверяются не размеры и не суммы, а **выдача**: документы и их скоры. Опись
    /// кусков может сойтись и у сегмента, который перестал искать, — например,
    /// если кусок уехал в контейнер под чужим именем.
    #[test]
    fn a_real_index_answers_the_same_after_a_round_trip() {
        use crate::search::{QueryWord, Request};

        let d = dir("roundtrip-search");
        let mut w = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 1_000_000);
        for i in 0..200u32 {
            w.add(&format!("d{i}"), &[&format!("дом у дороги {i} общее слово")]);
        }
        w.flush();
        let seg = format!("{d}/seg0000");

        let answers = |seg: &str| -> Vec<(u32, String, u64)> {
            let ix = crate::index::load(seg).expect("сегмент не открылся");
            let list = ix.dict.lookup(0, "общее").expect("слово есть");
            let req = Request {
                mode: crate::search::MatchMode::AnyTerm,
                skipped: false,
                live: None,
                words: vec![QueryWord::new(0, vec![list])],
                k: 20,
                filter: Vec::new(),
                must: Vec::new(),
                must_not: Vec::new(),
                tree: Default::default(),
                sort: Vec::new(),
                floor_key: crate::search::KeyFloor::None,
                doc_base: 0,
                budget: None,
                guard: None,
                score: Vec::new(),
                after: None,
            };
            let (hits, _) = crate::search::search(&ix, &req, 0.0);
            let ids = crate::ids::Ids::open(&ix.src, ix.docs);
            hits.into_iter()
                .map(|h| (h.doc, ids.get(h.doc as usize).unwrap_or_default(), h.score.to_bits()))
                .collect()
        };

        let before = answers(&seg);
        assert!(before.len() >= 20, "стенд ничего не нашёл — сверять нечего");
        assert!(
            crate::index::load(&seg).expect("сегмент").src.is_packed(),
            "сборка без контейнера"
        );

        assert!(unpack(&seg).expect("раскладка"));
        assert!(!crate::index::load(&seg).expect("сегмент").src.is_packed(), "остался упакованным");
        assert_eq!(answers(&seg), before, "россыпь отвечает не то, что контейнер");

        assert!(pack(&seg).expect("упаковка"));
        assert!(crate::index::load(&seg).expect("сегмент").src.is_packed(), "не упаковался");
        assert_eq!(answers(&seg), before, "после перекладки выдача изменилась");
        assert!(unsummed(&seg).is_empty(), "сводка не сошлась: {:?}", unsummed(&seg));
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn a_loose_segment_behaves_exactly_as_before() {
        let d = dir("loose");
        std::fs::write(format!("{d}/norms.bin"), [1u8, 2, 3]).expect("файл");
        std::fs::write(format!("{d}/empty.bin"), []).expect("файл");
        let src = loose(&d);
        assert!(!src.is_packed());
        assert_eq!(src.open("norms.bin").expect("есть").len_bytes(), 3);
        assert_eq!(src.try_open("empty.bin").expect("есть").len_bytes(), 0);
        assert!(src.try_open("pos.bin").is_none());
        assert_eq!(
            src.open("pos.bin").err().expect("отсутствующий файл принят").kind(),
            std::io::ErrorKind::NotFound
        );
    }

    /// Сегмент в памяти отвечает на все вопросы источника так же, как лежащий
    /// на диске. Сверяется не «работает», а **совпадение** с файловым источником
    /// на тех же кусках: разойдись они, разбор формата пошёл бы двумя дорогами.
    #[test]
    fn a_segment_in_memory_answers_exactly_as_one_on_disk() {
        let d = dir("in-memory");
        let pieces: Vec<(&str, Vec<u8>)> = vec![
            ("postings.bin", (0..5000u32).map(|x| x as u8).collect()),
            ("norms.bin", vec![7u8; 33]),
            // Пустой кусок положен намеренно: он существует и пуст, и это не то
            // же самое, что отсутствующий.
            ("dict.blob", Vec::new()),
            ("body.scrc", vec![1, 2, 3, 4, 9, 9, 9, 9]),
        ];
        let total = built(&d, &pieces);
        let disk = SegmentSource::of(&d, Some(total)).expect("открытие");
        let memory = in_memory("память/seg0001", pieces.iter().map(|(n, b)| (*n, b.clone())));
        assert!(memory.is_memory() && !disk.is_memory());

        for (name, want) in pieces.iter() {
            let from_disk = disk.open(name).expect("кусок с диска");
            let from_memory = memory.open(name).expect("кусок из памяти");
            assert_eq!(
                from_memory.slice(0, from_memory.len_bytes()),
                from_disk.slice(0, from_disk.len_bytes()),
                "кусок {name} из памяти и с диска разошлись"
            );
            assert_eq!(from_memory.slice(0, from_memory.len_bytes()), &want[..], "кусок {name}");
            assert_eq!(memory.len_of(name), disk.len_of(name), "длина куска {name}");
            assert!(memory.has(name) && disk.has(name), "кусок {name} потерялся");
        }

        // Отсутствующий кусок отказывает тем же видом ошибки: вызывающему всё
        // равно, как лежит сегмент, — и обязано быть всё равно.
        assert!(memory.try_open("нет.bin").is_none());
        assert!(!memory.has("нет.bin"));
        assert_eq!(memory.len_of("нет.bin"), None);
        // `map` перед `unwrap_err` — чтобы сравнивать вид отказа, не требуя от
        // куска уметь печататься.
        assert_eq!(
            memory.open("нет.bin").map(|_| ()).unwrap_err().kind(),
            disk.open("нет.bin").map(|_| ()).unwrap_err().kind(),
            "отказы разошлись видом"
        );

        // Позиционное чтение — то же и с теми же границами, хотя файла под ним
        // нет вовсе.
        let a = memory.located("body.scrc").expect("кусок");
        let mut buf = [0u8; 4];
        a.read_at(0, &mut buf).expect("чтение");
        assert_eq!(buf, [1, 2, 3, 4]);
        a.read_at(4, &mut buf).expect("чтение");
        assert_eq!(buf, [9, 9, 9, 9]);
        assert!(a.read_at(5, &mut buf).is_none(), "чтение вышло за кусок");
        assert!(memory.located("нет.scrc").is_none());

        // Память не занимает у ядра области отображения: потолок процесса на их
        // число — то, ради чего сегменты и складывают в контейнер.
        assert_eq!(memory.names().expect("перечислимы").len(), pieces.len());
        assert!(disk.names().is_none(), "у файлового источника перечня быть не должно");
    }

    #[test]
    fn a_positioned_read_stops_at_the_edge_of_its_piece() {
        let d = dir("located");
        let total = built(&d, &[("a.scrc", vec![1, 2, 3, 4]), ("b.scrc", vec![9, 9, 9, 9])]);
        let src = SegmentSource::of(&d, Some(total)).expect("открытие");
        let a = src.located("a.scrc").expect("кусок");
        let mut buf = [0u8; 4];
        a.read_at(0, &mut buf).expect("чтение");
        assert_eq!(buf, [1, 2, 3, 4]);
        // За концом куска лежит соседний, и отдавать его вместо отказа нельзя.
        assert!(a.read_at(1, &mut buf).is_none(), "чтение вышло за кусок");
        assert!(a.read_at(4, &mut buf).is_none(), "чтение вышло за кусок");
        assert!(src.located("c.scrc").is_none());
    }
}
