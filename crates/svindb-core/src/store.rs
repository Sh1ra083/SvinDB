//! Хранилище документов: вернуть содержимое, а не номер.

use crate::codec::pack::{DecoderDict, Effort, EncoderDict, Scratch};
use crate::mmap::Region;

/// Кусок до сжатия. Набирается значениями, пока не наберётся столько.
///
/// ## Откуда шестнадцать килобайт
pub const BLOCK: usize = 16384;
/// Байт на засечку: смещение, сжатая длина, длина до сжатия, первый номер.
pub const MARK: usize = 16;
pub const DICT_MAX: usize = 1 << 20;
/// Пол словаря: мельче он уже ничего не описывает.
pub const DICT_MIN: usize = 32 << 10;
/// Какую долю колонки занимает словарь.
pub const DICT_SHARE: usize = 256;

/// Наименьшее число кусков, при котором источник стоит **переносить**.
///
/// ## Сторож против россыпи
///
/// Перенос кладёт в `.sdic` словарь **каждого** источника. Слей восемь
/// сегментов, из которых семь по сотне документов, — и семь словарей лягут
/// рядом с данными, которые описывают, и перевесят их. Восемь кусков это сто
/// двадцать восемь килобайт до сжатия: мельче — россыпь, и её дешевле
/// распаковать и дописать в соседний отрезок обычным сжатием.
pub const CARRY_MIN_BLOCKS: usize = 8;

/// Действующий сторож против россыпи: наименьшее число кусков и наименьший
/// словарь, при которых источник стоит переносить.
///
/// В выпуске это [`CARRY_MIN_BLOCKS`] и [`DICT_MIN`] и ничего больше — вызов
/// подставляется целиком и от пары постоянных не отличается ни байтом.
///
/// ## Зачем шов
///
/// Настоящий порог — колонка за восемь мегабайт: мельче неё [`dict_size`]
/// упирается в пол, и словарь достаётся колонке по полу, а не по размеру.
/// Проверить на таком входе можно ровно одно — что порог действует; всё
/// остальное (перенумерация, погашенные, распущенные отрезки, неудобные формы)
/// пришлось бы каждый раз оплачивать десятью мегабайтами корпуса на источник.
/// Опущенный порог даёт то же самое на входе в сотню килобайт.
///
/// Ячейка потоко-местная, а не общая: проверки идут параллельно, и опущенный
/// одной порог перенёс бы то, что соседняя ждёт пересобранным. Тот же приём и по
/// той же причине уже стоит у [`crate::dict::offset_ceiling`].
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn carry_floor() -> (usize, usize) {
    (CARRY_MIN_BLOCKS, DICT_MIN)
}

#[cfg(test)]
thread_local! {
    static CARRY_FLOOR: std::cell::Cell<(usize, usize)> =
        const { std::cell::Cell::new((CARRY_MIN_BLOCKS, DICT_MIN)) };
}

#[cfg(test)]
pub(crate) fn carry_floor() -> (usize, usize) {
    CARRY_FLOOR.with(|c| c.get())
}

/// Опущенный порог держится, пока жив ответ [`lower_carry_floor`].
///
/// Возврат прежнего значения обязателен: libtest переиспользует потоки между
/// проверками, и оставленный на полу порог перенёс бы хранимое там, где
/// следующая проверка ждёт пересборки.
#[cfg(test)]
pub(crate) struct LoweredFloor((usize, usize));

#[cfg(test)]
impl Drop for LoweredFloor {
    fn drop(&mut self) {
        CARRY_FLOOR.with(|c| c.set(self.0));
    }
}

/// Опускает сторож против россыпи на этом потоке. Только для проверок.
#[cfg(test)]
#[must_use = "порог вернётся на место тут же, если ответ уронить"]
pub(crate) fn lower_carry_floor(blocks: usize, dict: usize) -> LoweredFloor {
    LoweredFloor(CARRY_FLOOR.with(|c| c.replace((blocks, dict))))
}

/// Сколько словарей у поля терпимо.
///
/// Инвариант «словарь — один непрерывный отрезок кусков» переживает любое число
/// слияний, но число словарей при этом растёт: восемь источников по `K` дают
/// `8K`. Потолок нужен не поиску — тот ходит двоичным поиском, — а открытию:
/// каждая запись сверяется на строение, а таблица распаковки строится лениво.
pub const DICT_COUNT_MAX: usize = 256;

/// До скольких словарей коалесцируется таблица, когда потолок пробит.
///
/// Не до `DICT_COUNT_MAX`, а вдвое ниже: иначе следующее же слияние пробило бы
/// потолок снова, и коалесцирование шло бы на каждом ярусе.
pub const DICT_COUNT_KEEP: usize = 128;

/// Пол потолка суммарного объёма словарей поля.
pub const DICT_TOTAL_FLOOR: usize = 4 << 20;

/// Действующий потолок таблицы словарей: сколько их терпимо и до скольких
/// коалесцировать. В выпуске — [`DICT_COUNT_MAX`] и [`DICT_COUNT_KEEP`].
///
/// Шов той же природы, что у [`carry_floor`]: собрать двести пятьдесят семь
/// отрезков настоящими сегментами — это двести пятьдесят семь источников
/// слияния, а роспуск отрезка проверяется на четырёх.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn dict_counts() -> (usize, usize) {
    (DICT_COUNT_MAX, DICT_COUNT_KEEP)
}

#[cfg(test)]
thread_local! {
    static DICT_COUNTS: std::cell::Cell<(usize, usize)> =
        const { std::cell::Cell::new((DICT_COUNT_MAX, DICT_COUNT_KEEP)) };
}

#[cfg(test)]
pub(crate) fn dict_counts() -> (usize, usize) {
    DICT_COUNTS.with(|c| c.get())
}

/// Опущенный потолок держится, пока жив ответ [`lower_dict_counts`].
#[cfg(test)]
pub(crate) struct LoweredCounts((usize, usize));

#[cfg(test)]
impl Drop for LoweredCounts {
    fn drop(&mut self) {
        DICT_COUNTS.with(|c| c.set(self.0));
    }
}

/// Опускает потолок таблицы словарей на этом потоке. Только для проверок.
#[cfg(test)]
#[must_use = "потолок вернётся на место тут же, если ответ уронить"]
pub(crate) fn lower_dict_counts(max: usize, keep: usize) -> LoweredCounts {
    LoweredCounts(DICT_COUNTS.with(|c| c.replace((max, keep))))
}

/// Сколько байт берётся от одного значения в выборку под словарь.
///
/// Выборка набирается прореживанием, и брать значение целиком нельзя: одно
/// длинное заняло бы её всю. Кусок в четверть килобайта описывает значение и
/// оставляет место остальным.
const PIECE: usize = 256;

/// Размер словаря под колонку такого объёма.
pub fn dict_size(column_bytes: usize) -> usize {
    (column_bytes / DICT_SHARE).clamp(DICT_MIN, DICT_MAX)
}

/// Опознавательное слово таблицы словарей: `SDIX` от младшего байта.
pub const SDIX_MAGIC: u32 = 0x5849_4453;
/// Версия таблицы словарей.
pub const SDIX_VERSION: u32 = 1;
/// Заголовок таблицы: опознание, версия, число словарей, число кусков.
pub const SDIX_HEAD: usize = 16;
/// Байт на запись таблицы: смещение записи в `.sdic`, её длина, первый кусок.
pub const SDIX_ENTRY: usize = 12;

/// Один словарь в таблице: где лежит его запись и с какого куска он владеет.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DictSpan {
    /// Смещение записи словаря в `<поле>.sdic`.
    pub at: u32,
    /// Длина записи.
    pub len: u32,
    /// Первый кусок, которым владеет словарь.
    pub first: u32,
}

/// Выкладывает таблицу словарей `<поле>.sdix`.
///
/// ## Зачем файл и почему номера словаря нет в засечке
///
/// Укрупнение переносит куски `.sbin` **байт в байт**, а словари источников
/// складывает в `.sdic` подряд: пересжатие хранимого — самая дорогая часть
/// заливки (замерено 93.2 с против 54.2 с на десяти миллионах, когда слияний не
/// стало вовсе), и платить её на каждом ярусе укрупнения нельзя.
///
/// Раз источники выкладываются подряд, куски одного словаря образуют
/// **непрерывный отрезок** — и этого довольно, чтобы найти словарь двоичным
/// поиском по первым кускам. Инвариант «словарь владеет одним отрезком»
/// переживает любое число слияний, поэтому номер словаря в засечку не кладётся:
/// засечка осталась шестнадцатибайтной. Двадцать байт стоили бы 2.5 МБ на сто
/// миллионов документов и, что хуже, развернули бы треть засечек через границу
/// шестидесятичетырёхбайтной линии кеша при двоичном поиске.
///
/// Файл заводится только при `K > 1`: сегмент с одним словарём остаётся
/// побайтово таким же, каким был до появления таблицы.
pub fn build_sdix(spans: &[DictSpan], blocks: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(SDIX_HEAD + SDIX_ENTRY * spans.len() + 4);
    out.extend(SDIX_MAGIC.to_le_bytes());
    out.extend(SDIX_VERSION.to_le_bytes());
    out.extend((spans.len() as u32).to_le_bytes());
    out.extend(blocks.to_le_bytes());
    for s in spans {
        out.extend(s.at.to_le_bytes());
        out.extend(s.len.to_le_bytes());
        out.extend(s.first.to_le_bytes());
    }
    let crc = crate::wal::crc32(&out);
    out.extend(crc.to_le_bytes());
    out
}

/// Хранилище одного поля одного сегмента.
pub struct Store {
    data: Region,
    marks: Region,
    /// Отображение `<поле>.sdic` — **одно** на все словари, сколько бы их ни
    /// было. Записи берутся из него срезами: страницами распоряжается ядро, и
    /// копия удвоила бы то, что и так есть, а отдельное отображение на словарь
    /// умножило бы число областей у ядра на число слитых источников.
    sdic: Region,
    /// Запись каждого словаря в `.sdic`: смещение и длина.
    spans: Vec<(usize, usize)>,
    /// Первый кусок каждого словаря. Строго возрастает, `firsts[0] == 0`.
    firsts: Vec<usize>,
    /// Распаковщики, каждый строится при первом обращении именно к нему.
    ///
    /// ## Почему лениво и почему `OnceLock`
    ///
    /// Таблицы распаковки — по тридцать два килобайта на словарь. Шестьдесят
    /// восемь словарей это 2.2 МБ кучи на поле, а на индексе из двух десятков
    /// сегментов — восемьдесят семь мегабайт, притом что точечная выдача трогает
    /// **один** словарь. `OnceLock` здесь не украшение: `Store` читают из
    /// `thread::scope` в `Fetcher::extract`, и он обязан оставаться `Sync` —
    /// `RefCell` или `Cell` этого не дают.
    ///
    /// Только распаковка. Состояния упаковщика — цепочек по словарю — у
    /// поискового узла нет и быть не может: такого типа он не строит.
    decs: Vec<std::sync::OnceLock<DecoderDict>>,
    /// Путь к суммам распакованных кусков и их ожидаемая длина.
    ///
    /// Путь, а не отображение, и это счёт, а не вкус. Суммы читают ровно двое —
    /// `check --full` и слияние, когда переносит кусок. Поисковый узел не
    /// смотрит на них никогда: сверять их на выдаче значило бы распаковывать
    /// кусок ради проверки того, что он распаковался.
    ///
    /// Отображение же занимает область ядра **на каждое хранимое поле каждого
    /// сегмента**, а их число и есть то, во что упирается масштаб: при потолке
    /// `vm.max_map_count` = 65 530 и девятнадцати областях на сегмент индекс
    /// перестаёт открываться примерно на трёх с половиной тысячах сегментов.
    /// Держать область ради файла, который читают дважды в жизни сегмента, —
    /// платить самым дефицитным за самое редкое.
    ///
    /// `None` — файла нет: так собраны все сегменты до его появления.
    crcs: Option<(crate::container::Located, usize)>,
}

impl Store {
    /// Открывает хранилище поля.
    ///
    /// Недоверенный архив открывается тем же вызовом: жёсткую ссылку на чужой
    /// inode отвергает **источник** ([`crate::container::loose_private`]), а не
    /// второй набор путей здесь. Прежде это была отдельная `open_private` со
    /// своими вызовами открытия, и разъехаться два набора могли на любой правке
    /// — достаточно было добавить файл в один и забыть про другой.
    pub fn open(src: &crate::container::SegmentSource, field: &str) -> Option<Store> {
        let data = src.try_open(&format!("{field}.sbin"))?;
        let marks = src.try_open(&format!("{field}.smrk"))?;
        if marks.len_bytes() % MARK != 0 {
            return None;
        }
        let blocks = marks.len_bytes() / MARK;
        // `.sdic` берётся куском целиком, а не по записи на словарь, и это не
        // недосмотр: записи адресуются внутри него парой «смещение и длина», и
        // распаковщик каждой держит свой подкусок того же отображения. Держатель
        // при этом один на все словари поля, сколько бы источников ни слилось.
        let sdic = src.try_open(&format!("{field}.sdic"))?;

        // Таблицы нет — словарь один, и он занимает `.sdic` целиком. Так лежат
        // все сегменты, собранные до появления таблицы, и никакой миграции им не
        // нужно: ветка здесь ровно одна.
        //
        // «Нет» — это именно отсутствие файла, а не пустой файл: пустой разберётся
        // как таблица без опознавательного слова и будет отвергнут. Слить эти два
        // случая значило бы принять обрезанную до нуля таблицу за её отсутствие.
        let table = src.try_open(&format!("{field}.sdix"));
        let (spans, firsts) = match table {
            Some(m) => read_sdix(&m, sdic.len_bytes(), blocks)?,
            None => (vec![(0usize, sdic.len_bytes())], vec![0usize]),
        };

        // Строение каждой записи сверяется **на открытии**, а таблицы
        // распаковки строятся лениво. Иначе испорченный словарь перестал бы быть
        // отказом на открытии и стал бы пустой выдачей на каждом документе —
        // ровно тем молчанием, от которого заведена проверка.
        for (at, len) in spans.iter() {
            let rec = sdic.slice(*at, *len);
            if rec.len() != *len || !crate::codec::pack::record_ok(rec) {
                return None;
            }
        }

        // Сумм может не быть вовсе, но если они есть — их обязано быть ровно по
        // одной на кусок. Короткий файл читался бы нулями, то есть объявлял бы
        // сошедшимся то, чего не сверял. Длина сверяется здесь, при открытии, —
        // отображать ради этого нечего, хватает `metadata`.
        let crcs = match src.located(&format!("{field}.scrc")) {
            None => None,
            Some(at) if at.len == 0 => None,
            Some(at) if at.len as usize == blocks * 4 => Some((at, blocks)),
            Some(_) => return None,
        };

        let decs = spans.iter().map(|_| std::sync::OnceLock::new()).collect();
        Some(Store { data, marks, sdic, spans, firsts, decs, crcs })
    }

    pub fn blocks(&self) -> usize {
        self.marks.len_bytes() / MARK
    }

    /// Сколько словарей у хранилища. Больше одного — только после укрупнения.
    pub fn dicts(&self) -> usize {
        self.spans.len()
    }

    /// `crc32` распакованного куска. `None` — сумм нет или куска нет.
    ///
    /// Сверяется не на выдаче, а в `check --full`: сводка `sums.tsv` доказывает,
    /// что байты `.sbin` целы, но не то, что они распаковываются в то же самое.
    /// Риск переноса кусков между сегментами ровно такой — перепутанный словарь
    /// или съехавшая засечка дают целый файл и другое содержимое.
    /// Читается с диска, а не из отображения: см. [`Store::crcs`]. Четыре байта
    /// с известного смещения — одно чтение, и только там, где сумма нужна.
    pub fn chunk_crc(&self, i: usize) -> Option<u32> {
        let (at, blocks) = self.crcs.as_ref()?;
        if i >= *blocks {
            return None;
        }
        let mut buf = [0u8; 4];
        at.read_at((i * 4) as u64, &mut buf)?;
        Some(u32::from_le_bytes(buf))
    }

    /// Есть ли у хранилища суммы распакованных кусков.
    pub fn has_crcs(&self) -> bool {
        self.crcs.is_some()
    }

    /// Первый кусок отрезка словаря `k`.
    pub fn dict_first(&self, k: usize) -> Option<usize> {
        self.firsts.get(k).copied()
    }

    /// Запись словаря `k` — байты `.sdic` **как есть**.
    ///
    /// Срезом отображения, а не копией: перенос кладёт эту запись в `.sdic`
    /// нового сегмента дословно, и куча ей на пути незачем.
    pub fn dict_record(&self, k: usize) -> Option<&[u8]> {
        let (at, len) = *self.spans.get(k)?;
        let rec = self.sdic.slice(at, len);
        (rec.len() == len).then_some(rec)
    }

    /// Сколько в записи словаря `k` собственно словаря — без таблиц кодов.
    ///
    /// Нужно сторожу против россыпи: [`dict_size`] упирается в пол `DICT_MIN`
    /// на всякой колонке мельче восьми мегабайт, и словарь такой колонке достался
    /// не по размеру, а по полу.
    pub fn dict_bytes(&self, k: usize) -> Option<usize> {
        let rec = self.dict_record(k)?;
        let n = u32::from_le_bytes(rec.get(..4)?.try_into().ok()?) as usize;
        (rec.len() >= 4 + n).then_some(n)
    }

    /// Засечка куска: смещение, сжатая длина, длина до сжатия, первый номер.
    pub fn mark_at(&self, i: usize) -> Option<(usize, usize, usize, u32)> {
        (i < self.blocks()).then(|| self.mark(i))
    }

    /// Сжатое тело куска — байты `.sbin` **как есть**.
    ///
    /// То, что перенос копирует, ничего не разбирая. `None` — засечка обещает
    /// больше байт, чем в файле есть.
    pub fn chunk_body(&self, i: usize) -> Option<&[u8]> {
        let (off, len, _, _) = self.mark_at(i)?;
        let body = self.data.slice(off, len);
        (body.len() == len).then_some(body)
    }

    /// Словарь, которым сжат кусок.
    ///
    /// Двоичный поиск по первым кускам: словарь владеет непрерывным отрезком, и
    /// это инвариант построения слияния, а не догадка. `firsts[0] == 0` сверено
    /// на открытии, поэтому отрезок находится у любого куска.
    fn dict_of(&self, block: usize) -> Option<&DecoderDict> {
        let k = self.firsts.partition_point(|&f| f <= block).checked_sub(1)?;
        let cell = self.decs.get(k)?;
        if let Some(d) = cell.get() {
            return Some(d);
        }
        let (at, len) = *self.spans.get(k)?;
        let made = DecoderDict::open_at(&self.sdic, at, len)?;
        // Отказ `set` означает, что сосед успел первым, — тогда берётся его.
        let _ = cell.set(made);
        cell.get()
    }

    fn mark(&self, i: usize) -> (usize, usize, usize, u32) {
        let m = self.marks.slice(i * MARK, MARK);
        let at = |o: usize| u32::from_le_bytes(m[o..o + 4].try_into().expect("четыре байта"));
        (at(0) as usize, at(4) as usize, at(8) as usize, at(12))
    }

    /// Сколько значений в куске и с какого номера он начинается.
    ///
    /// Нужно проверке целости, и только ей. Двоичный поиск куска держится на
    /// возрастании поля «первый номер», но одного возрастания мало: уменьшить
    /// его на восемь можно так, что порядок уцелеет, все проверки по
    /// метаданным пройдут, а `get` начнёт отдавать содержимое **соседнего**
    /// документа — без отказа, без паники и без единого признака.
    ///
    /// Поймать это можно только тем, что здесь: числом значений внутри куска
    /// против разности первых номеров. Стоит это распаковки каждого куска,
    /// поэтому в быстрой проверке его нет.
    pub fn chunk(&self, i: usize) -> Option<(u32, usize)> {
        let (first, n, _) = self.chunk_at(i)?;
        Some((first, n))
    }

    /// То же, что [`Store::chunk`], но отдаёт и сами распакованные байты.
    ///
    /// Отдельно потому, что сумму распакованного считают по ним же: сверять её
    /// вторым проходом значило бы распаковать весь сегмент дважды.
    pub fn chunk_at(&self, i: usize) -> Option<(u32, usize, Vec<u8>)> {
        if i >= self.blocks() {
            return None;
        }
        let first = self.mark(i).3;
        let chunk = self.chunk_bytes(i)?;
        let mut at = 0usize;
        let mut n = 0usize;
        while at + 4 <= chunk.len() {
            let l = u32::from_le_bytes(chunk[at..at + 4].try_into().ok()?) as usize;
            at += 4 + l;
            if at > chunk.len() {
                return None;
            }
            n += 1;
        }
        (at == chunk.len()).then_some((first, n, chunk))
    }

    /// Последовательный проход: кусок распаковывается один раз.
    pub fn walk(&self) -> StoreWalk<'_> {
        StoreWalk { store: self, chunk: 0, buf: Vec::new(), at: 0, doc: 0, broken: false }
    }

    /// Номер куска, в котором лежит документ, и его границы в файле.
    ///
    /// Отдаётся отдельно от значения, потому что кусок — единица распаковки, а
    /// значение — нет. Кто отдаёт десять документов подряд, распакует кусок
    /// однажды, если знает его номер; `get` этого не знает и распаковывает
    /// заново на каждый документ.
    pub fn locate(&self, doc: u32) -> Option<(usize, u32)> {
        let n = self.blocks();
        if n == 0 {
            return None;
        }
        let (mut lo, mut hi) = (0usize, n);
        while lo + 1 < hi {
            let mid = (lo + hi) / 2;
            if self.mark(mid).3 <= doc {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let first = self.mark(lo).3;
        (doc >= first).then_some((lo, first))
    }

    /// Распаковывает кусок целиком. `None` — кусок негоден.
    pub fn chunk_bytes(&self, i: usize) -> Option<Vec<u8>> {
        if i >= self.blocks() {
            return None;
        }
        let (off, len, raw, _) = self.mark(i);
        let bytes = self.data.slice(off, len);
        (bytes.len() == len).then(|| self.dict_of(i)?.decompress(bytes, raw))?
    }

    /// Значение документа. `None` — номера нет или кусок негоден.
    ///
    /// Негодный кусок отдаётся отказом, а не мусором: файл мог обрваться при
    /// падении, байт мог испортиться на диске, и выдать в таком случае мусор как
    /// содержимое значило бы врать тем убедительнее, чем незаметнее порча.
    pub fn get(&self, doc: u32) -> Option<Vec<u8>> {
        let n = self.blocks();
        if n == 0 {
            return None;
        }
        // Двоичный поиск куска: засечки идут по возрастанию первого номера.
        let (mut lo, mut hi) = (0usize, n);
        while lo + 1 < hi {
            let mid = (lo + hi) / 2;
            if self.mark(mid).3 <= doc {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let (off, len, raw, first) = self.mark(lo);
        if doc < first {
            return None;
        }
        let bytes = self.data.slice(off, len);
        if bytes.len() != len {
            return None;
        }
        let chunk = self.dict_of(lo)?.decompress(bytes, raw)?;
        value_at(&chunk, (doc - first) as usize)
    }
}

/// Разобранная таблица словарей: записи в `.sdic` (смещение и длина) и первые
/// куски их отрезков. Две полосы, а не список пар: первые куски перебираются
/// двоичным поиском на каждой выдаче, и держать их вплотную стоит того.
type DictTable = (Vec<(usize, usize)>, Vec<usize>);

/// Читает таблицу словарей: записи в `.sdic` и первые куски.
///
/// ## Почему проверок так много
///
/// Это новая поверхность молчаливой порчи, и молчаливой — в самом буквальном
/// смысле. Записи в `.sdic` самоописательны, а разбор идёт от начала среза:
/// чужое смещение даёт не отказ, а правдоподобную запись, которой распакуются
/// куски **другого** словаря. Чаще это отказ, но иногда — мусор нужной длины,
/// то есть содержимое документа, которого никогда не было. Ни размер файлов, ни
/// их контрольные суммы этого не видят: всё цело, просто читается не там.
///
/// Поэтому здесь сверяется всё, что вообще выразимо: опознание, версия, сумма по
/// заголовку и записям, число кусков против `.smrk`, возрастание первых кусков
/// от нуля, и покрытие `.sdic` записями встык и без нахлёста.
fn read_sdix(map: &Region, sdic_len: usize, blocks: usize) -> Option<DictTable> {
    if map.u32_at(0) != SDIX_MAGIC || map.u32_at(4) != SDIX_VERSION {
        return None;
    }
    let k = map.u32_at(8) as usize;
    if k == 0 {
        return None;
    }
    // Число словарей приходит с диска, и умножение на нём — не наша величина:
    // в выпуске с `panic = "abort"` переполнение убило бы узел на открытии.
    let want = SDIX_HEAD.checked_add(SDIX_ENTRY.checked_mul(k)?)?.checked_add(4)?;
    if map.len_bytes() != want {
        return None;
    }
    let body = map.slice(0, want - 4);
    if body.len() != want - 4 || crate::wal::crc32(body) != map.u32_at(want - 4) {
        return None;
    }
    // Число кусков в таблице против длины засечек: разошлись — таблица не от
    // этого хранилища, и отрезки кусков указывают в пустоту.
    if map.u32_at(12) as usize != blocks {
        return None;
    }

    let mut spans = Vec::with_capacity(k);
    let mut firsts = Vec::with_capacity(k);
    let mut edge = 0usize;
    let mut prev: Option<usize> = None;
    for i in 0..k {
        let o = SDIX_HEAD + i * SDIX_ENTRY;
        let at = map.u32_at(o) as usize;
        let len = map.u32_at(o + 4) as usize;
        let first = map.u32_at(o + 8) as usize;
        if at != edge {
            return None;
        }
        edge = at.checked_add(len)?;
        match prev {
            None if first != 0 => return None,
            Some(p) if first <= p => return None,
            _ => {}
        }
        if first >= blocks {
            return None;
        }
        prev = Some(first);
        spans.push((at, len));
        firsts.push(first);
    }
    (edge == sdic_len).then_some((spans, firsts))
}

/// Последовательный проход по хранилищу: кусок распаковывается **однажды**.
///
/// ## Зачем
///
/// [`Store::get`] ищет кусок двоичным поиском и распаковывает его целиком ради
/// одного значения. Для точечной выдачи это верно, для прохода — нет: слияние
/// звало `get` на каждый документ и распаковывало один и тот же кусок столько
/// раз, сколько в нём документов. На четырёх килобайтах это десятки раз.
///
/// Здесь кусок распаковывается один раз, а значения отдаются **срезами** в него.
/// Срез живёт до следующего шага — этого довольно всем, кто переносит значение
/// дальше, и никому не приходится заводить `Vec` на документ.
///
/// ## Договор при порче
///
/// Проход останавливается на первом же куске, который не распаковался или чьи
/// длины не сошлись с его границей, и сообщает об этом [`StoreWalk::broken`].
/// Молча оборваться он не имеет права: для того, кто переносит документы, «конец
/// данных» и «дальше нечитаемо» — разные вещи, и путать их значит потерять
/// половину поля, отчитавшись об успехе.
pub struct StoreWalk<'a> {
    store: &'a Store,
    /// Номер следующего куска.
    chunk: usize,
    /// Распакованный кусок и место в нём.
    buf: Vec<u8>,
    at: usize,
    /// Номер следующего документа.
    doc: u32,
    broken: bool,
}

#[allow(clippy::should_implement_trait)]
impl<'a> StoreWalk<'a> {
    /// Следующий документ: его номер и содержимое срезом.
    ///
    /// `None` — либо всё пройдено, либо дальше порча; отличает их
    /// [`StoreWalk::broken`].
    pub fn next(&mut self) -> Option<(u32, &[u8])> {
        while self.at >= self.buf.len() {
            if self.broken || self.chunk >= self.store.blocks() {
                return None;
            }
            let (off, len, raw, first) = self.store.mark(self.chunk);
            let bytes = self.store.data.slice(off, len);
            if bytes.len() != len {
                self.broken = true;
                return None;
            }
            let Some(next) = self.store.dict_of(self.chunk).and_then(|d| d.decompress(bytes, raw))
            else {
                self.broken = true;
                return None;
            };
            self.buf = next;
            self.at = 0;
            self.doc = first;
            self.chunk += 1;
        }
        let n = u32::from_le_bytes(self.buf.get(self.at..self.at + 4)?.try_into().ok()?) as usize;
        let from = self.at + 4;
        if from + n > self.buf.len() {
            self.broken = true;
            return None;
        }
        self.at = from + n;
        let doc = self.doc;
        self.doc += 1;
        Some((doc, &self.buf[from..from + n]))
    }

    /// Оборвался ли проход на порче, а не на конце данных.
    pub fn broken(&self) -> bool {
        self.broken
    }
}

/// Значение по порядковому номеру внутри уже распакованного куска — срезом.
///
/// Отдельно от [`value_at`], потому что копия нужна не всем: кто отдаёт
/// содержимое в ответ, обходится ссылкой в чужой буфер.
pub fn slice_at(chunk: &[u8], skip: usize) -> Option<&[u8]> {
    let mut at = 0usize;
    for _ in 0..skip {
        let l = u32::from_le_bytes(chunk.get(at..at + 4)?.try_into().ok()?) as usize;
        at += 4 + l;
    }
    let l = u32::from_le_bytes(chunk.get(at..at + 4)?.try_into().ok()?) as usize;
    chunk.get(at + 4..at + 4 + l)
}

/// Достаёт значение по порядковому номеру внутри распакованного куска.
fn value_at(chunk: &[u8], skip: usize) -> Option<Vec<u8>> {
    let mut at = 0usize;
    for _ in 0..skip {
        let l = u32::from_le_bytes(chunk.get(at..at + 4)?.try_into().ok()?) as usize;
        at += 4 + l;
    }
    let l = u32::from_le_bytes(chunk.get(at..at + 4)?.try_into().ok()?) as usize;
    chunk.get(at + 4..at + 4 + l).map(|s| s.to_vec())
}

/// Собранное хранилище: файлы, готовые лечь на диск.
pub struct Built {
    pub data: Vec<u8>,
    pub marks: Vec<u8>,
    pub dict: Vec<u8>,
    /// `crc32` **распакованных** кусков, по четыре байта на кусок.
    ///
    /// ## Зачем отдельная сумма, если есть `sums.tsv`
    ///
    /// Та доказывает, что байты `.sbin` целы. Она не доказывает, что они
    /// распаковываются в то же самое, — а укрупнение будет переносить куски
    /// между сегментами, и риск там ровно этот: перепутанный словарь или
    /// съехавшая на запись таблица дают целый файл и **другое** содержимое.
    ///
    /// При переносе сумма копируется дословно вместе с куском, и распаковывать
    /// его для этого не нужно; при пересжатии — пересчитывается. Стоит она 2.5 МБ
    /// на сто миллионов документов, то есть три сотых процента сегмента.
    pub crcs: Vec<u8>,
}

/// Собирает хранилище поля.
///
/// `values` **толкает** значения в переданный приёмник, а не отдаётся
/// итератором: только так значение можно передать срезом. Итератор обязан
/// владеть тем, что отдаёт, — то есть заводить `Vec` на каждый документ, — а
/// слиянию нужно ровно обратное: пройти чужое хранилище, ничего не копируя.
///
/// Здесь первый проход набирает выборку **прореживанием**: куски берутся подряд,
/// пока не наберётся потолок, а когда наберётся — каждый второй выбрасывается и
/// шаг удваивается. Так выборка остаётся растянутой на всю колонку при любой её
/// длине, а длину знать не нужно. Прореживание детерминированное: тот же вход
/// даёт ту же выборку, иначе сегмент перестал бы собираться побайтово одинаково.
///
/// Держать значения в памяти между проходами нельзя: при слиянии это гигабайт
/// на сегмент, а источники всё равно отображены и второй проход по ним дёшев.
///
/// `Ok(None)` — значений нет вовсе, и хранилище не нужно. `Err` — вход не влезает
/// в раскладку; см. [`fits_mark`].
///
/// `sbin` — куда ляжет вывод; идёт только в текст отказа по потолку.
pub fn build<F>(values: F, effort: Effort, sbin: &str) -> std::io::Result<Option<Built>>
where
    F: Fn(&mut dyn FnMut(&[u8])),
{
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    build_with(values, effort, threads, sbin)
}

/// Влезает ли засечка куска в четыре байта.
///
/// ## Почему это самый ранний обрыв сегмента
///
/// Засечка в `<поле>.smrk` — это начало куска внутри `<поле>.sbin`, четыре
/// байта. Хранимое содержимое — самый крупный файл сегмента: при семидесяти трёх
/// байтах на документ и доле в шестьдесят процентов `.sbin` упирается в четыре
/// гигабайта около девяноста семи миллионов документов, то есть **раньше**
/// словаря, идентификаторов и списков.
///
/// Заворот засечки ничего не роняет: `get` находит кусок двоичным поиском по
/// засечкам, попадает не в тот и возвращает **чужой документ**. Ни одна сверка
/// размеров этого не видит — файл целый, кратность соблюдена.
///
/// Отказ, а не паника: выпуск идёт с `panic = "abort"`, и `assert!` здесь убил бы
/// узел на приёме вместо того, чтобы отказать одному сбросу.
fn fits_mark(len: usize, sbin: &str, at: usize) -> std::io::Result<()> {
    let ceiling = crate::dict::offset_ceiling();
    if len as u64 <= ceiling {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{sbin}: хранимое содержимое перевалило за четыре гигабайта на {at}-м куске \
             ({len} байт при потолке {ceiling}) — засечка рядом с ним тридцатидвухбитная и \
             завернулась бы, а get отдавал бы чужой документ; уменьшите flush_docs \
             у приёма или target_docs у укрупнения"
        ),
    ))
}

// Сколько потоков попросили у последней сборки на **этом** потоке.
//
// Только для проверок: ими доказывается, что слияние и правда сужает себе
// ширину, а не зовёт сборку с числом ядер. Счётчик потоковый, а не общий, затем,
// что проверки идут параллельно и общий показывал бы чужую сборку.
#[cfg(test)]
thread_local! {
    pub(crate) static LAST_THREADS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// То же с заданным числом потоков. Отдельно — ради проверки: вывод обязан быть
/// **побайтово** одним и тем же при любом их числе, иначе сегмент начинает
/// зависеть от машины, на которой собран.
pub fn build_with<F>(
    values: F,
    effort: Effort,
    threads: usize,
    sbin: &str,
) -> std::io::Result<Option<Built>>
where
    F: Fn(&mut dyn FnMut(&[u8])),
{
    let threads = threads.max(1);
    #[cfg(test)]
    LAST_THREADS.with(|c| c.set(threads));
    let t0 = std::time::Instant::now();
    // Первый проход: объём, выборка под словарь и обучающие куски — всё сразу.
    let mut pieces: Vec<Vec<u8>> = Vec::new();
    let mut parts: Vec<Vec<u8>> = Vec::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut total = 0usize;
    let mut docs = 0usize;
    // Сколько значений пропускать между взятыми и сколько уже пропущено.
    let mut stride = 1usize;
    let mut since = 0usize;
    // Потолок выборки в кусках. Настоящий размер словаря станет известен в конце
    // прохода, и лишнее просто отрежется.
    let cap = DICT_MAX / PIECE;

    values(&mut |v: &[u8]| {
        total += v.len() + 4;
        docs += 1;
        if since == 0 {
            let take = v.len().min(PIECE);
            if take > 0 {
                pieces.push(v[..take].to_vec());
            }
            if pieces.len() >= cap {
                let mut keep = Vec::with_capacity(pieces.len() / 2 + 1);
                for (i, p) in pieces.drain(..).enumerate() {
                    if i % 2 == 0 {
                        keep.push(p);
                    }
                }
                pieces = keep;
                stride *= 2;
            }
        }
        since = (since + 1) % stride;
        // Обучающие куски — те же данные, но в форме куска: таблицы обучаются на
        // том, что ими потом кодируется, а не на голом тексте.
        if parts.len() < 512 {
            buf.extend((v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
            if buf.len() >= BLOCK {
                parts.push(std::mem::take(&mut buf));
            }
        }
    });
    if docs == 0 {
        return Ok(None);
    }
    if !buf.is_empty() && parts.is_empty() {
        parts.push(std::mem::take(&mut buf));
    }

    // Теперь объём известен — берём из выборки ровно столько, сколько нужно.
    let want = dict_size(total).min(total);
    let mut sample: Vec<u8> = Vec::with_capacity(want);
    for p in pieces.iter() {
        if sample.len() >= want {
            break;
        }
        let take = (want - sample.len()).min(p.len());
        sample.extend_from_slice(&p[..take]);
    }
    let t_pass1 = t0.elapsed().as_secs_f64();
    let t_scan = std::time::Instant::now();
    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    // Необучившийся словарь — не отказ: значений слишком мало, чтобы было чему
    // учиться, и хранилище такому полю просто не нужно.
    let Some(dict) = EncoderDict::train_with(sample, &refs, effort) else {
        return Ok(None);
    };
    // Обучающее сырьё больше не нужно — отпускаем его здесь, а не в конце.
    //
    // `parts` держит до пятисот двенадцати кусков по шестнадцать килобайт, то
    // есть до восьми мегабайт, `pieces` — ещё до мегабайта мелкими выделениями.
    // Дальше идёт второй проход, в котором живут разом сжатая колонка, пачка
    // сырых кусков и та же пачка сжатой; лишние девять мегабайт под ними просто
    // лежат. Ни на выход, ни на словарь их освобождение не влияет: словарь уже
    // обучен и владеет своей копией.
    drop(refs);
    drop(parts);
    drop(pieces);
    let t_train = t_scan.elapsed().as_secs_f64();

    // Второй проход: куски.
    //
    // Куски копятся пачкой и жмутся разом, а кладутся **по порядку**: порядок
    // задаёт номера документов, и переставить их значило бы поменять индекс.
    // Пачка мала — по нескольку на поток, — поэтому память не растёт с колонкой.
    // Пачка крупная не случайно: на каждой поднимаются потоки заново, и при
    // сорока кусках их подъём съедал больше, чем сама работа.
    //
    // ## Почему предел байтами, а не числом кусков
    //
    // Прежде предел был `threads * 64` кусков, и пояснение обещало «около двух с
    // половиной мегабайт». Обещание держалось только при четырёх потоках: кусок
    // это `BLOCK` и больше, и на восьми ядрах пачка выходила восемь-десять
    // мегабайт сырья — плюс столько же сжатых копий рядом. То есть память
    // всё-таки росла, только не с колонкой, а с шириной машины, и на узле с
    // тридцатью двумя ядрами слияние платило за это вчетверо против замеренного.
    //
    // Байтовый предел возвращает обещанное: расход постоянен при любой ширине.
    // Потоки от этого не простаивают — меняется длина очереди перед ними, а не
    // число работников: при восьми потоках на каждого по-прежнему приходится
    // два десятка кусков за подъём.
    //
    // Счёт остаётся вторым пределом на случай мелких значений: пачка из
    // множества крошечных кусков не займёт много памяти, но подъём потоков
    // окупит вряд ли.
    const BATCH_BYTES: usize = 2 * 1024 * 1024 + 512 * 1024;
    let batch = threads * 64;
    let mut data = Vec::new();
    let mut marks = Vec::new();
    let mut crcs = Vec::new();
    let mut chunk: Vec<u8> = Vec::new();
    let mut first = 0u32;
    let mut doc = 0u32;
    // Накопленные куски и номер первого документа каждого.
    let mut pend: Vec<(Vec<u8>, u32, u32)> = Vec::new();
    // Сколько сырья лежит в пачке: предел байтовый, см. `BATCH_BYTES`.
    let mut pend_bytes = 0usize;
    // Сколько кусков уже уложено: отказ по потолку обязан назвать место, а не
    // только размер.
    let mut made = 0usize;

    let flush = |pend: &mut Vec<(Vec<u8>, u32, u32)>,
                 data: &mut Vec<u8>,
                 marks: &mut Vec<u8>,
                 crcs: &mut Vec<u8>,
                 made: &mut usize|
     -> std::io::Result<()> {
        if pend.is_empty() {
            return Ok(());
        }
        let packed: Vec<Vec<u8>> = if pend.len() == 1 || threads == 1 {
            let mut sc = Scratch::default();
            pend.iter().map(|(c, _, _)| dict.compress(c, effort, &mut sc)).collect()
        } else {
            let dict = &dict;
            let src = &*pend;
            std::thread::scope(|scope| {
                let hands: Vec<_> = (0..threads)
                    .map(|t| {
                        scope.spawn(move || {
                            let mut sc = Scratch::default();
                            src.iter()
                                .enumerate()
                                .filter(|(i, _)| i % threads == t)
                                .map(|(i, (c, _, _))| (i, dict.compress(c, effort, &mut sc)))
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect();
                let mut out: Vec<Vec<u8>> = vec![Vec::new(); src.len()];
                for h in hands {
                    for (i, v) in h.join().expect("поток сжатия") {
                        out[i] = v;
                    }
                }
                out
            })
        };
        for ((raw, from, _), body) in pend.iter().zip(packed.iter()) {
            *made += 1;
            // Отказ **до** записи усечённой засечки, а не после: записанная
            // засечка неотличима от настоящей, файл остаётся правильного размера
            // и кратности, и разойтись обнаружится только чужим документом в
            // выдаче.
            fits_mark(data.len(), sbin, *made)?;
            marks.extend((data.len() as u32).to_le_bytes());
            marks.extend((body.len() as u32).to_le_bytes());
            marks.extend((raw.len() as u32).to_le_bytes());
            marks.extend(from.to_le_bytes());
            data.extend_from_slice(body);
            // Сумма считается по **распакованному**: сжатое накрывает `sums.tsv`,
            // а вот того, что оно распакуется в то же самое, не проверяет никто.
            crcs.extend(crate::wal::crc32(raw).to_le_bytes());
        }
        pend.clear();
        Ok(())
    };

    // Отказ наружу из толкающего обхода вынести нечем: приёмник обязан вернуть
    // `()`, а `values` о наших ошибках ничего не знает. Поэтому отказ
    // запоминается, обход после него перестаёт копить, и `Err` уходит вызывающему
    // ниже. Молча продолжать нельзя: следующие засечки лягут уже завёрнутыми.
    let mut failed: Option<std::io::Error> = None;
    values(&mut |v: &[u8]| {
        if failed.is_some() {
            return;
        }
        chunk.extend((v.len() as u32).to_le_bytes());
        chunk.extend_from_slice(v);
        doc += 1;
        if chunk.len() >= BLOCK {
            pend_bytes += chunk.len();
            pend.push((std::mem::take(&mut chunk), first, doc));
            first = doc;
            if pend_bytes >= BATCH_BYTES || pend.len() >= batch {
                if let Err(e) = flush(&mut pend, &mut data, &mut marks, &mut crcs, &mut made) {
                    failed = Some(e);
                }
                pend_bytes = 0;
            }
        }
    });
    if let Some(e) = failed {
        return Err(e);
    }
    if !chunk.is_empty() {
        pend.push((std::mem::take(&mut chunk), first, doc));
    }
    flush(&mut pend, &mut data, &mut marks, &mut crcs, &mut made)?;

    if std::env::var_os("SVINDB_STORE_PHASES").is_some() {
        eprintln!(
            "      хранилище: проход 1 {t_pass1:.1} с, обучение {t_train:.1} с, проход 2 {:.1} с",
            t0.elapsed().as_secs_f64() - t_pass1 - t_train
        );
    }
    Ok(Some(Built { data, marks, dict: dict.to_bytes(), crcs }))
}

/// Пишет файл хранилища и **доводит его до диска**.
///
/// `write_all` возвращается, когда байты приняты ядром, а не когда легли на
/// носитель, а признаком готовности сегмента служит порядок записи файлов.
fn put(dir: &str, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = std::fs::File::create(format!("{dir}/{name}"))?;
    std::io::Write::write_all(&mut f, bytes)?;
    f.sync_all()
}

fn bent(msg: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

/// Что вышло из переноса.
#[derive(Debug)]
pub struct Made {
    /// Сколько документов накрыто. Сверяется вызывающим с числом документов
    /// слитого сегмента: разойдись они — значения съехали бы на чужие номера.
    pub docs: u32,
    pub blocks: usize,
    pub dicts: usize,
    /// Сколько кусков пришлось **сжать**. Перенесённые байт в байт сюда не
    /// попадают, и на чистом слиянии здесь ноль.
    pub packed: usize,
}

/// Один отрезок: словарь и то, что нужно, чтобы им пересжимать.
struct Run {
    /// Запись словаря — она же ляжет в `.sdic` дословно.
    record: Vec<u8>,
    /// Упаковщик по этой записи. Поднимается **лениво**: перенос байт в байт
    /// его не трогает вовсе, а стоит он цепочек по всему словарю.
    enc: Option<EncoderDict>,
    /// Попал ли словарь в таблицу. Попадает при первом же своём куске: отрезок
    /// без кусков нарушил бы строгое возрастание первых кусков в `.sdix`, а
    /// отрезок, у которого все документы погашены, — обычное дело.
    placed: bool,
}

/// Сборка хранилища **переносом**: куски источников ложатся байт в байт.
///
/// ## Зачем отдельно от [`build`]
///
/// [`build`] пересжимает всё, что ему дали, и копит `.sbin` в `Vec`. Для приёма
/// это верно: колонка там строится впервые, а сегмент режется по `flush_docs`.
/// Для укрупнения — нет, и по двум разным причинам.
///
/// Первая: пересжатие хранимого — самая дорогая часть заливки, а куски, в
/// которых ни один документ не погашен, уже сжаты и **не меняются**. Меняется
/// только номер первого документа, а он живёт в засечке, не в теле: номера
/// документов внутри куска не хранятся вовсе — тело это «длина|байты» подряд.
/// Значит перенумерация это переписать засечку, а тело скопировать.
///
/// Вторая: `Vec` под `.sbin` — это пик кучи ровно в размер самого крупного файла
/// сегмента. На ста миллионах документов замерено 1491 МБ RSS. Здесь `.sbin`
/// течёт в файл через [`std::io::BufWriter`] и на куче не задерживается; в
/// памяти остаются только засечки, суммы и словари — то, что меряется десятками
/// байт на кусок, а не килобайтами.
///
/// ## Договор
///
/// Куски ложатся **в том порядке, в каком их дали**, и номера документов идут
/// подряд от нуля: порядок задаёт нумерацию, и переставить его значило бы
/// поменять индекс. Отрезок словаря обязан быть непрерывным — на этом стоит
/// поиск словаря по номеру куска, — поэтому [`Transfer::open_dict`] закрывает
/// недобранный кусок прежнего словаря, а не оставляет его висеть.
pub struct Transfer {
    dir: String,
    stem: String,
    sbin: std::io::BufWriter<std::fs::File>,
    /// Сколько байт уже ушло в `.sbin`; оно же — смещение следующего куска.
    at: usize,
    marks: Vec<u8>,
    crcs: Vec<u8>,
    sdic: Vec<u8>,
    spans: Vec<DictSpan>,
    blocks: usize,
    /// Номер следующего документа выхода.
    doc: u32,
    run: Option<Run>,
    /// Недобранный кусок и сколько в нём значений.
    pend: Vec<u8>,
    pend_docs: u32,
    effort: Effort,
    scratch: Scratch,
    packed: usize,
}

impl Transfer {
    /// Заводит перенос в каталог `dir` под именем поля `stem`.
    pub fn create(dir: &str, stem: &str, effort: Effort) -> std::io::Result<Transfer> {
        let f = std::fs::File::create(format!("{dir}/{stem}.sbin"))?;
        Ok(Transfer {
            dir: dir.to_string(),
            stem: stem.to_string(),
            // Буфер крупный не для скорости записи, а против дробления: тела
            // кусков идут по нескольку килобайт, и восьмикилобайтный буфер по
            // умолчанию превращал бы каждый кусок в отдельный вызов ядра.
            sbin: std::io::BufWriter::with_capacity(1 << 20, f),
            at: 0,
            marks: Vec::new(),
            crcs: Vec::new(),
            sdic: Vec::new(),
            spans: Vec::new(),
            blocks: 0,
            doc: 0,
            run: None,
            pend: Vec::new(),
            pend_docs: 0,
            effort,
            scratch: Scratch::default(),
            packed: 0,
        })
    }

    /// Принимает словарь источника: с него начинается новый отрезок кусков.
    ///
    /// Запись копируется, а не берётся ссылкой: она переживёт отображение
    /// источника, если тот закроют раньше, чем допишется вывод.
    pub fn open_dict(&mut self, record: &[u8]) -> std::io::Result<()> {
        // Недобранный кусок принадлежит **прежнему** словарю: оставь его
        // висеть — и он лёг бы уже за границей отрезка, то есть распаковывался
        // бы чужим словарём.
        self.seal()?;
        if !crate::codec::pack::record_ok(record) {
            return Err(bent(format!(
                "{}/{}.sdic: запись словаря не разбирается — переносить куски нечем",
                self.dir, self.stem
            )));
        }
        self.run = Some(Run { record: record.to_vec(), enc: None, placed: false });
        Ok(())
    }

    /// Переносит кусок **байт в байт**: тело копируется, засечка переписывается
    /// на новое смещение и новый первый номер, сумма распакованного копируется
    /// дословно.
    ///
    /// `raw_len` — длина до сжатия из засечки источника, `docs` — сколько в куске
    /// значений, `crc` — сумма распакованного оттуда же. Ни одного из трёх
    /// перенос не пересчитывает: пересчёт означал бы распаковку, то есть ровно
    /// ту работу, которой перенос и избегает.
    pub fn carry(
        &mut self,
        body: &[u8],
        raw_len: usize,
        docs: u32,
        crc: u32,
    ) -> std::io::Result<()> {
        self.seal()?;
        self.emit(body, raw_len, docs, crc)
    }

    /// Пересжимает кусок **тем же** словарём отрезка.
    ///
    /// Для куска, из которого выпали погашенные: перенести его как есть нельзя,
    /// а собрать другим словарём — тем более, иначе он вывалится из отрезка
    /// своего словаря, и поиск словаря по номеру куска перестанет работать.
    ///
    /// `raw` — уже отобранное тело куска в той же раскладке «длина|байты».
    /// Пустое означает, что погашены все: кусок просто исчезает, и непрерывность
    /// отрезка от этого не страдает.
    pub fn recode(&mut self, raw: &[u8], docs: u32) -> std::io::Result<()> {
        self.seal()?;
        if raw.is_empty() {
            return Ok(());
        }
        self.pack(raw, docs)
    }

    /// Дописывает значение обычным сжатием — словарём **текущего** отрезка.
    ///
    /// Так уходят источники, не прошедшие сторож против россыпи, поля, которых
    /// в источнике нет вовсе, и числовые поля без хранилища.
    pub fn push(&mut self, value: &[u8]) -> std::io::Result<()> {
        self.pend.extend((value.len() as u32).to_le_bytes());
        self.pend.extend_from_slice(value);
        self.pend_docs += 1;
        // Граница куска ровно та же, что у `build`: иначе одна и та же колонка
        // резалась бы на куски по-разному в зависимости от пути.
        if self.pend.len() >= BLOCK {
            self.seal()?;
        }
        Ok(())
    }

    /// Закрывает недобранный кусок, если он есть.
    fn seal(&mut self) -> std::io::Result<()> {
        if self.pend.is_empty() {
            return Ok(());
        }
        let raw = std::mem::take(&mut self.pend);
        let docs = std::mem::take(&mut self.pend_docs);
        self.pack(&raw, docs)?;
        // Буфер возвращается на место: он переживает кусок и не заводится
        // заново на каждом из сотен тысяч.
        self.pend = raw;
        self.pend.clear();
        Ok(())
    }

    /// Сжимает тело словарём текущего отрезка и укладывает как кусок.
    fn pack(&mut self, raw: &[u8], docs: u32) -> std::io::Result<()> {
        let Some(mut run) = self.run.take() else {
            return Err(bent(format!(
                "{}/{}.sbin: кусок некому сжать — отрезок словаря не открыт",
                self.dir, self.stem
            )));
        };
        if run.enc.is_none() {
            // Подъём упаковщика по уже выложенной записи: обучать нечему, длины
            // кодов лежат в ней, а коды из длин восстанавливаются однозначно.
            let Some(enc) = EncoderDict::adopt(&run.record) else {
                return Err(bent(format!(
                    "{}/{}.sdic: по записи словаря не поднимается упаковщик",
                    self.dir, self.stem
                )));
            };
            run.enc = Some(enc);
        }
        let body = match run.enc.as_ref() {
            Some(enc) => enc.compress(raw, self.effort, &mut self.scratch),
            None => return Err(bent("упаковщик пропал сразу после подъёма".to_string())),
        };
        self.run = Some(run);
        self.packed += 1;
        let crc = crate::wal::crc32(raw);
        self.emit(&body, raw.len(), docs, crc)
    }

    /// Кладёт готовое тело куска: засечка, байты, сумма, номера.
    fn emit(&mut self, body: &[u8], raw_len: usize, docs: u32, crc: u32) -> std::io::Result<()> {
        self.place()?;
        let sbin = format!("{}/{}.sbin", self.dir, self.stem);
        // Отказ **до** записи усечённой засечки: записанная неотличима от
        // настоящей, и разойтись обнаружилось бы только чужим документом.
        fits_mark(self.at, &sbin, self.blocks + 1)?;
        let ceiling = crate::dict::offset_ceiling();
        if body.len() as u64 > ceiling || raw_len as u64 > ceiling {
            return Err(bent(format!(
                "{sbin}: кусок {} не влезает в засечку ({} байт сжатого, {raw_len} до сжатия)",
                self.blocks + 1,
                body.len()
            )));
        }
        self.marks.extend((self.at as u32).to_le_bytes());
        self.marks.extend((body.len() as u32).to_le_bytes());
        self.marks.extend((raw_len as u32).to_le_bytes());
        self.marks.extend(self.doc.to_le_bytes());
        std::io::Write::write_all(&mut self.sbin, body)?;
        self.at += body.len();
        self.crcs.extend(crc.to_le_bytes());
        let Some(next) = self.doc.checked_add(docs) else {
            return Err(bent(format!("{sbin}: номера документов перевалили за четыре миллиарда")));
        };
        self.doc = next;
        self.blocks += 1;
        Ok(())
    }

    /// Вносит словарь текущего отрезка в таблицу — при **первом** его куске.
    fn place(&mut self) -> std::io::Result<()> {
        let Some(run) = self.run.as_mut() else {
            return Err(bent(format!(
                "{}/{}.sbin: кусок без словаря — распаковать его будет нечем",
                self.dir, self.stem
            )));
        };
        if run.placed {
            return Ok(());
        }
        let at = self.sdic.len();
        let len = run.record.len();
        if (at + len) as u64 > crate::dict::offset_ceiling() {
            return Err(bent(format!(
                "{}/{}.sdic: словари поля перевалили за четыре гигабайта",
                self.dir, self.stem
            )));
        }
        self.spans.push(DictSpan { at: at as u32, len: len as u32, first: self.blocks as u32 });
        self.sdic.extend_from_slice(&run.record);
        run.placed = true;
        Ok(())
    }

    /// Дописывает остаток и выкладывает файлы поля.
    ///
    /// Порядок здесь тот же, что у приёма: сначала содержимое, потом всё, что на
    /// него ссылается. `.sdix` пишется **только при двух и более** словарях —
    /// сегмент с одним обязан остаться побайтово таким же, каким был до
    /// появления таблицы.
    pub fn finish(mut self) -> std::io::Result<Made> {
        self.seal()?;
        std::io::Write::flush(&mut self.sbin)?;
        let file = self.sbin.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(file);

        let (dir, stem) = (self.dir.clone(), self.stem.clone());
        if self.blocks == 0 {
            // Значений не было вовсе — хранилища не нужно. Оставить пустой
            // `.sbin` значило бы завести поле, которое отвечает пустотой на всё.
            for ext in ["sbin", "smrk", "sdic", "scrc", "sdix"] {
                std::fs::remove_file(format!("{dir}/{stem}.{ext}")).ok();
            }
            // Число документов сообщается **как есть**, а не нулём: кусков без
            // значений не бывает, и разойдись эти два числа — пусть об этом
            // споткнётся сверка вызывающего, а не наш успокоительный ноль.
            return Ok(Made { docs: self.doc, blocks: 0, dicts: 0, packed: self.packed });
        }
        put(&dir, &format!("{stem}.smrk"), &self.marks)?;
        put(&dir, &format!("{stem}.sdic"), &self.sdic)?;
        put(&dir, &format!("{stem}.scrc"), &self.crcs)?;
        if self.spans.len() > 1 {
            put(&dir, &format!("{stem}.sdix"), &build_sdix(&self.spans, self.blocks as u32))?;
        } else {
            std::fs::remove_file(format!("{dir}/{stem}.sdix")).ok();
        }
        Ok(Made {
            docs: self.doc,
            blocks: self.blocks,
            dicts: self.spans.len(),
            packed: self.packed,
        })
    }
}

/// Надо ли распустить часть отрезков, чтобы таблица словарей не разрослась.
///
/// ## Правило
///
/// Словарей больше [`DICT_COUNT_MAX`] **или** их суммарный объём больше
/// `max(DICT_TOTAL_FLOOR, колонка / DICT_SHARE)` — распускаются соседние отрезки
/// с наименьшими колонками, пока их не останется [`DICT_COUNT_KEEP`].
///
/// Роспуск означает, что куски отрезка распаковываются и дописываются к
/// **соседнему** словарю обычным сжатием. Соседнему — потому что инвариант
/// «словарь владеет одним непрерывным отрезком кусков» обязан уцелеть: он и есть
/// то, на чём стоит поиск словаря двоичным поиском по первым кускам.
///
/// Порог по объёму сам по себе отрезков не распускает: пока их меньше
/// [`DICT_COUNT_KEEP`], распускать нечего — вычёркивание единственного соседа
/// стоило бы пересжатия целой колонки ради нескольких десятков килобайт.
///
/// Отдельной функцией, потому что решение принимается **до** первого байта
/// вывода и проверяется само по себе: собрать в тесте триста отрезков дешевле
/// числами, чем сегментами.
pub(crate) fn coalesce(volumes: &[u64], dicts: &[u64], column: u64) -> Vec<bool> {
    let mut keep = vec![true; volumes.len()];
    let total: u64 = dicts.iter().sum();
    let cap = (column / DICT_SHARE as u64).max(DICT_TOTAL_FLOOR as u64);
    let (most, target) = dict_counts();
    let over = volumes.len() > most || total > cap;
    if !over || volumes.len() <= target {
        return keep;
    }
    // Роспуск идёт от наименьшей колонки: пересжать придётся именно её, и
    // платить за это надо там, где дешевле. Порядок при равенстве — по номеру
    // отрезка, иначе вывод зависел бы от того, как сортировка расставит равных.
    let mut order: Vec<usize> = (0..volumes.len()).collect();
    order.sort_by_key(|&i| (volumes[i], i));
    let mut left = volumes.len();
    for i in order {
        if left <= target {
            break;
        }
        keep[i] = false;
        left -= 1;
    }
    keep
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-store-{name}"));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        d.to_string_lossy().to_string()
    }

    fn put(d: &str, field: &str, b: &Built) {
        std::fs::write(format!("{d}/{field}.sbin"), &b.data).expect("sbin");
        std::fs::write(format!("{d}/{field}.smrk"), &b.marks).expect("smrk");
        std::fs::write(format!("{d}/{field}.sdic"), &b.dict).expect("sdic");
    }

    fn corpus(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| {
                format!(
                    "товар номер {i} категория электроника цена {} руб описание подробное и длинное",
                    i * 7
                )
            })
            .collect()
    }

    /// Документ возвращается **байт в байт** тем, каким пришёл, — на каждом
    /// номере, а не на выборке. Ошибка в проходе по длинам вернула бы соседа, и
    /// поймать её можно только полным перебором.
    #[test]
    fn every_document_comes_back_byte_for_byte() {
        let d = dir("exact");
        let vals = corpus(5000);
        let b = build(
            |emit| vals.iter().for_each(|s| emit(s.as_bytes())),
            Effort::Fast,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано");
        put(&d, "body", &b);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        for (i, want) in vals.iter().enumerate() {
            let got = s.get(i as u32).expect("значение");
            assert_eq!(got, want.as_bytes(), "документ {i} вернулся не тем");
        }
        assert!(s.get(vals.len() as u32).is_none(), "номер за концом отдал значение");
    }

    /// Пустые значения, UTF-8 вне латиницы и значение длиннее куска — не край, а
    /// обычные данные.
    #[test]
    fn odd_values_survive() {
        let d = dir("odd");
        let mut vals: Vec<String> = corpus(200);
        vals.push(String::new());
        vals.push("ёжик 日本語 emoji 🐖 смешанное".repeat(3));
        vals.push("x".repeat(BLOCK * 3));
        vals.push(String::new());
        vals.extend(corpus(200));
        let b = build(
            |emit| vals.iter().for_each(|s| emit(s.as_bytes())),
            Effort::Thorough,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано");
        put(&d, "body", &b);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        for (i, want) in vals.iter().enumerate() {
            assert_eq!(s.get(i as u32).expect("значение"), want.as_bytes(), "документ {i}");
        }
    }

    /// Одно значение на всё поле: куска меньше порога, словаря почти нет.
    #[test]
    fn a_single_value_is_not_an_edge_case() {
        let d = dir("single");
        let vals = ["одно".to_string()];
        let b = build(
            |emit| vals.iter().for_each(|s| emit(s.as_bytes())),
            Effort::Fast,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано");
        put(&d, "body", &b);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        assert_eq!(s.get(0).expect("значение"), b"\xd0\xbe\xd0\xb4\xd0\xbd\xd0\xbe");
        assert!(s.get(1).is_none());
    }

    /// Пустая колонка не даёт хранилища и не притворяется, что даёт.
    #[test]
    fn an_empty_column_makes_no_store() {
        assert!(build(|_emit| {}, Effort::Fast, "проверка/body.sbin").expect("влезло").is_none());
    }

    /// Испорченный кусок ловится отказом, а не отдаётся как содержимое.
    #[test]
    fn a_broken_block_is_refused_not_returned() {
        let d = dir("broken");
        let vals = corpus(2000);
        let mut b = build(
            |emit| vals.iter().for_each(|s| emit(s.as_bytes())),
            Effort::Fast,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано");
        // Портим середину данных: часть кусков обязана отказать, и ни один —
        // не вернуть чужое содержимое.
        let mid = b.data.len() / 2;
        for k in 0..64 {
            b.data[mid + k] ^= 0xff;
        }
        put(&d, "body", &b);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let mut refused = 0;
        for (i, want) in vals.iter().enumerate() {
            match s.get(i as u32) {
                None => refused += 1,
                Some(got) => assert_eq!(got, want.as_bytes(), "документ {i} вернулся чужим"),
            }
        }
        assert!(refused > 0, "порча не замечена ни на одном документе");
    }

    /// Сегмент без хранилища открывается, а не падает.
    #[test]
    fn a_segment_without_a_store_opens() {
        let d = dir("none");
        assert!(Store::open(&crate::container::loose(&d), "body").is_none());
    }

    /// Потолок засечки хранилища: `u32::MAX` ещё влезает, на байт больше — нет.
    ///
    /// ## Что ломалось бы без отказа
    ///
    /// Засечка легла бы завёрнутой, и `get` нашёл бы двоичным поиском **не тот**
    /// кусок: содержимое чужого документа вернулось бы как своё. Ни размер, ни
    /// кратность засечек, ни контрольная сумма этого не замечают — файл целый,
    /// просто читается не там. Хранимое содержимое при этом упирается в потолок
    /// **первым** из всех файлов сегмента: оно самое крупное.
    ///
    /// Четыре гигабайта в тесте не собрать, поэтому проверяется само правило и
    /// его края: сдвиг границы на единицу вниз отказал бы сегментам, которые
    /// открываются, вверх — пропустил бы заворот.
    #[test]
    fn the_store_mark_ceiling_is_inclusive_of_u32_max() {
        assert!(fits_mark(0, "seg0007/body.sbin", 1).is_ok(), "пустое хранилище влезает");
        assert!(
            fits_mark(crate::dict::OFFSET_CEILING as usize, "seg0007/body.sbin", 9).is_ok(),
            "потолок обязан быть самим u32::MAX"
        );

        let e = fits_mark(crate::dict::OFFSET_CEILING as usize + 1, "seg0007/body.sbin", 10)
            .expect_err("на байт за потолком обязан быть отказ");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("seg0007/body.sbin"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("4294967296"), "отказ не назвал достигнутое: {msg}");
        assert!(msg.contains("4294967295"), "отказ не назвал потолок: {msg}");
        assert!(msg.contains("10-м куске"), "отказ не назвал место: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход приёму: {msg}");
        assert!(msg.contains("target_docs"), "отказ не подсказал выход укрупнению: {msg}");
    }

    /// Охрана стоит **на пути записи**, а не рядом с ним.
    ///
    /// ## Чем это отличается от проверки выше
    ///
    /// Выше проверен предикат: точные числа, оба края. Предикат, которого никто
    /// не зовёт, ведёт себя так же — снятие строки `fits_mark(...)` из
    /// `build_with` не роняло ни одной проверки, и охрана оставалась
    /// украшением. Здесь потолок опущен до сотни байт, колонка обычная и
    /// маленькая, и сборка обязана отказать до того, как усечённая засечка ляжет
    /// в `marks`.
    ///
    /// Один поток берётся не ради скорости: при одном потоке пачка складывается
    /// целиком и все засечки пишутся подряд, так что проверка меряет саму
    /// охрану, а не порядок подъёма потоков сжатия.
    #[test]
    fn the_chunk_mark_guard_stands_on_the_write_path() {
        // Кусок закрывается на шестнадцати килобайтах; двух тысяч значений
        // хватает на добрый десяток кусков, то есть на десяток засечек.
        let vals = corpus(2000);
        let feed = |emit: &mut dyn FnMut(&[u8])| vals.iter().for_each(|s| emit(s.as_bytes()));

        // При настоящем потолке та же колонка обязана собираться.
        let ok = build_with(feed, Effort::Fast, 1, "seg0007/body.sbin")
            .expect("обычная колонка собирается")
            .expect("хранилище есть");
        assert!(ok.marks.len() / MARK >= 2, "нужно больше одной засечки, иначе охрана ни при чём");
        let packed = ok.data.len();
        assert!(packed > 100, "сжатое обязано перерасти опущенный потолок: {packed}");

        let _low = crate::dict::lower_offset_ceiling(100);
        let e = match build_with(feed, Effort::Fast, 1, "seg0007/body.sbin") {
            Ok(_) => panic!("охрана не на пути записи: засечка за потолком легла в файл"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("seg0007/body.sbin"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("при потолке 100"), "отказ померил не тем потолком: {msg}");
        assert!(msg.contains("куске"), "отказ не назвал место: {msg}");
    }

    /// Обычная колонка отказа не даёт, а пустая — не отказ, а «нечего хранить».
    ///
    /// Разница несущая: `build` теперь возвращает `Result<Option<_>>`, и слить
    /// «значений нет» с «не влезло» значило бы потерять поле молча — ровно то,
    /// от чего заводился отказ.
    #[test]
    fn an_empty_column_is_not_a_refusal() {
        let vals = corpus(2000);
        let built = build(
            |emit| vals.iter().for_each(|s| emit(s.as_bytes())),
            Effort::Fast,
            "seg0007/body.sbin",
        )
        .expect("обычная колонка обязана собраться");
        assert!(built.is_some(), "у колонки со значениями обязано быть хранилище");

        let empty = build(|_emit| {}, Effort::Fast, "seg0007/body.sbin")
            .expect("пустая колонка — не отказ");
        assert!(empty.is_none(), "пустая колонка не заводит хранилища");
    }
}

/// Проход и кеш: то, ради чего они заведены, — не «работает», а «однажды».
#[cfg(test)]
mod walking {
    use super::*;

    fn corpus(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("документ {i} с текстом про товары, цены и доставку по городу"))
            .collect()
    }

    fn built(name: &str, vals: &[String]) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-walk-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        let b = build(
            |emit| vals.iter().for_each(|s| emit(s.as_bytes())),
            Effort::Fast,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано");
        std::fs::write(format!("{d}/body.sbin"), &b.data).expect("sbin");
        std::fs::write(format!("{d}/body.smrk"), &b.marks).expect("smrk");
        std::fs::write(format!("{d}/body.sdic"), &b.dict).expect("sdic");
        d
    }

    /// Проход отдаёт ровно то же, что выборка по номеру, и в том же порядке.
    #[test]
    fn walking_gives_exactly_what_asking_gives() {
        let vals = corpus(5000);
        let d = built("walkinggives", &vals);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let mut w = s.walk();
        let mut n = 0usize;
        while let Some((doc, got)) = w.next() {
            assert_eq!(doc as usize, n, "номер документа поехал");
            assert_eq!(got, vals[n].as_bytes(), "документ {n} прошёл не тем");
            n += 1;
        }
        assert!(!w.broken(), "проход оборвался на порче");
        assert_eq!(n, vals.len(), "прошло {n} документов из {}", vals.len());
        std::fs::remove_dir_all(&d).ok();
    }

    /// Проход по всему хранилищу распаковывает каждый кусок **однажды**.
    #[test]
    fn every_chunk_is_unpacked_once() {
        let vals = corpus(5000);
        let d = built("everyonce", &vals);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let chunks = s.blocks();
        assert!(chunks > 20, "кусков всего {chunks} — сравнивать не с чем");
        let mut w = s.walk();
        let mut n = 0usize;
        while w.next().is_some() {
            n += 1;
        }
        // Проход распаковал ровно число кусков; выборка по номеру распаковала бы
        // по одному разу на документ.
        assert_eq!(n, vals.len());
        assert!(
            vals.len() / chunks >= 10,
            "на кусок приходится {} документов — выигрыш не проверить",
            vals.len() / chunks
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Порча обрывает проход **с признаком**, а не тихим концом данных.
    ///
    /// Разница решающая для того, кто переносит документы: «дальше ничего нет» и
    /// «дальше нечитаемо» — разные вещи, и спутать их значит потерять половину
    /// поля, отчитавшись об успехе.
    #[test]
    fn corruption_stops_the_walk_and_says_so() {
        let vals = corpus(5000);
        let d = built("corruptionso", &vals);
        let path = format!("{d}/body.sbin");
        let mut raw = std::fs::read(&path).expect("sbin");
        let mid = raw.len() / 2;
        for k in 0..128 {
            raw[mid + k] ^= 0xff;
        }
        std::fs::write(&path, &raw).expect("запись");
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let mut w = s.walk();
        let mut n = 0usize;
        while let Some((doc, got)) = w.next() {
            assert_eq!(got, vals[doc as usize].as_bytes(), "документ {doc} прошёл чужим");
            n += 1;
        }
        assert!(w.broken(), "порча прошла как конец данных");
        assert!(n < vals.len(), "порча не помешала пройти всё");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Испорченный словарь — отказ на открытии, а не мусор на отдаче.
    #[test]
    fn a_broken_dictionary_refuses_to_open() {
        let vals = corpus(500);
        let d = built("aopen", &vals);
        let path = format!("{d}/body.sdic");
        let whole = std::fs::read(&path).expect("sdic");
        // Длина словаря в заголовке — за концом файла.
        let mut bent = whole.clone();
        bent[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&path, &bent).expect("запись");
        assert!(
            Store::open(&crate::container::loose(&d), "body").is_none(),
            "негодная длина словаря принята"
        );
        // Обрыв на любой длине — тоже отказ, а не половина таблиц.
        for cut in [0usize, 4, whole.len() / 2, whole.len() - 1] {
            std::fs::write(&path, &whole[..cut]).expect("запись");
            assert!(
                Store::open(&crate::container::loose(&d), "body").is_none(),
                "словарь, обрезанный до {cut}, принят"
            );
        }
        std::fs::write(&path, &whole).expect("возврат");
        assert!(
            Store::open(&crate::container::loose(&d), "body").is_some(),
            "возвращённый словарь не открылся"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Испорченная засечка не отдаёт содержимое соседнего документа.
    #[test]
    fn a_bent_mark_refuses_rather_than_lies() {
        let vals = corpus(2000);
        let d = built("alies", &vals);
        let path = format!("{d}/body.smrk");
        let mut raw = std::fs::read(&path).expect("smrk");
        // Первый номер второго куска — на восемь меньше: порядок цел, смысл нет.
        let at = MARK + 12;
        let was = u32::from_le_bytes(raw[at..at + 4].try_into().expect("четыре байта"));
        raw[at..at + 4].copy_from_slice(&(was - 8).to_le_bytes());
        std::fs::write(&path, &raw).expect("запись");
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let mut wrong = 0;
        for (i, want) in vals.iter().enumerate() {
            if let Some(got) = s.get(i as u32) {
                if got != want.as_bytes() {
                    wrong += 1;
                }
            }
        }
        // Врать оно всё-таки может — на то и заведена проверка целостности,
        // которая ловит это числом значений в куске. Тест закрепляет, что
        // сдвинутая засечка **видна**: без него правка проверки прошла бы молча.
        assert!(wrong > 0, "сдвинутая засечка ничего не изменила — проверять нечего");
        let bad =
            crate::store::Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let mut first = 0u32;
        let mut caught = false;
        for i in 0..bad.blocks() {
            match bad.chunk(i) {
                Some((f, n)) => {
                    if f != first {
                        caught = true;
                    }
                    first = f + n as u32;
                }
                None => caught = true,
            }
        }
        assert!(caught, "сдвиг засечки не пойман разбором кусков");
        std::fs::remove_dir_all(&d).ok();
    }
}

/// Сжатие в несколько потоков обязано давать **те же байты**.
#[cfg(test)]
mod parallel {
    use super::*;

    /// Число потоков не меняет ни одного байта вывода.
    #[test]
    fn the_number_of_threads_changes_nothing() {
        let vals: Vec<String> = (0..30_000)
            .map(|i| {
                format!("документ {i} про товары, цены и доставку; описание номер {}", i % 977)
            })
            .collect();
        let emit = |e: &mut dyn FnMut(&[u8])| vals.iter().for_each(|s| e(s.as_bytes()));
        let one = build_with(emit, Effort::Thorough, 1, "проверка/body.sbin")
            .expect("влезло")
            .expect("собрано");
        assert!(one.marks.len() / MARK > 64, "кусков {} — пачка одна", one.marks.len() / MARK);
        for threads in [2usize, 3, 8, 64] {
            let many = build_with(emit, Effort::Thorough, threads, "проверка/body.sbin")
                .expect("влезло")
                .expect("собрано");
            assert_eq!(many.data, one.data, "при {threads} потоках данные другие");
            assert_eq!(many.marks, one.marks, "при {threads} потоках засечки другие");
            assert_eq!(many.dict, one.dict, "при {threads} потоках словарь другой");
        }
    }

    #[test]
    fn what_many_threads_wrote_one_reader_gets_back() {
        let vals: Vec<String> =
            (0..20_000).map(|i| format!("значение {i} с хвостом {}", i % 313)).collect();
        let d = std::env::temp_dir()
            .join(format!("svindb-par-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        let b = build_with(
            |e: &mut dyn FnMut(&[u8])| vals.iter().for_each(|s| e(s.as_bytes())),
            Effort::Fast,
            8,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано");
        std::fs::write(format!("{d}/body.sbin"), &b.data).expect("sbin");
        std::fs::write(format!("{d}/body.smrk"), &b.marks).expect("smrk");
        std::fs::write(format!("{d}/body.sdic"), &b.dict).expect("sdic");
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        for (i, want) in vals.iter().enumerate() {
            assert_eq!(s.get(i as u32).as_deref(), Some(want.as_bytes()), "документ {i}");
        }
        std::fs::remove_dir_all(&d).ok();
    }
}

/// Таблица словарей: то, ради чего укрупнение перестанет пересжимать хранимое.
#[cfg(test)]
mod dictionary_table {
    use super::*;

    fn dir(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-sdix-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        d.to_string_lossy().to_string()
    }

    fn make(vals: &[String], effort: Effort) -> Built {
        build_with(
            |e: &mut dyn FnMut(&[u8])| vals.iter().for_each(|s| e(s.as_bytes())),
            effort,
            1,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано")
    }

    /// Корпус, который жмётся: у каждого источника слияния он **свой**, иначе
    /// словари вышли бы одинаковыми и подмена одного другим ничего не изменила бы.
    fn corpus(tag: &str, n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("{tag} запись {i} свойство {} значение {}", i % 29, i * 31))
            .collect()
    }

    /// Шум: сжатию не за что зацепиться, и кусок ложится в файл как есть.
    ///
    /// Нужен там, где порча обязана **пережить** распаковку: у сжатого куска
    /// перевёрнутый байт почти всегда рвёт поток, и тогда меряется отказ
    /// распаковщика, а не сумма распакованного. Байты берутся все двести
    /// пятьдесят шесть, а не печатные: на суженном алфавите Хаффман выигрывает
    /// четверть, и кусок всё-таки уходит в сжатие.
    fn noise(tag: u64, n: usize, len: usize) -> Vec<Vec<u8>> {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ tag;
        (0..n)
            .map(|_| {
                (0..len)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        seed as u8
                    })
                    .collect()
            })
            .collect()
    }

    fn make_bytes(vals: &[Vec<u8>]) -> Built {
        build_with(
            |e: &mut dyn FnMut(&[u8])| vals.iter().for_each(|v| e(v)),
            Effort::Fast,
            1,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано")
    }

    /// Складывает два собранных хранилища в одно — ровно так, как это сделает
    /// укрупнение: куски подряд, словари подряд, таблица поверх.
    fn graft(d: &str, a: &Built, adocs: u32, b: &Built, table: bool) {
        let mut data = a.data.clone();
        data.extend_from_slice(&b.data);
        let mut marks = a.marks.clone();
        for i in 0..b.marks.len() / MARK {
            let m = &b.marks[i * MARK..(i + 1) * MARK];
            let at = |o: usize| u32::from_le_bytes(m[o..o + 4].try_into().expect("четыре байта"));
            marks.extend((at(0) + a.data.len() as u32).to_le_bytes());
            marks.extend(at(4).to_le_bytes());
            marks.extend(at(8).to_le_bytes());
            marks.extend((at(12) + adocs).to_le_bytes());
        }
        let mut sdic = a.dict.clone();
        sdic.extend_from_slice(&b.dict);
        let mut crcs = a.crcs.clone();
        crcs.extend_from_slice(&b.crcs);
        std::fs::write(format!("{d}/body.sbin"), &data).expect("sbin");
        std::fs::write(format!("{d}/body.smrk"), &marks).expect("smrk");
        std::fs::write(format!("{d}/body.sdic"), &sdic).expect("sdic");
        std::fs::write(format!("{d}/body.scrc"), &crcs).expect("scrc");
        std::fs::remove_file(format!("{d}/body.sdix")).ok();
        if table {
            let spans = [
                DictSpan { at: 0, len: a.dict.len() as u32, first: 0 },
                DictSpan {
                    at: a.dict.len() as u32,
                    len: b.dict.len() as u32,
                    first: (a.marks.len() / MARK) as u32,
                },
            ];
            let blocks = (marks.len() / MARK) as u32;
            std::fs::write(format!("{d}/body.sdix"), build_sdix(&spans, blocks)).expect("sdix");
        }
    }

    /// При одном словаре выбранный encoder остаётся побайтово стабильным.
    ///
    /// ## Почему это критерий приёмки, а не вкусовщина
    ///
    /// Таблица словарей заводится ради укрупнения, а платить за неё пересборкой
    /// уже собранного нельзя: сегментов у живого узла тысячи, и переписать их —
    /// это та самая заливка, которую и экономят. Значит сегмент с одним словарём
    /// обязан остаться прежним побайтово.
    ///
    /// Числа здесь сняты с принятой стратегии encoder на том же корпусе и при
    /// одном потоке. Fast-эталон версии 1.2.0 намеренно обновлён вместе с
    /// обучением Huffman тем же Fast-разбором, которым затем пишутся куски:
    /// это меняет только выбранные коды нового сегмента, а не self-describing
    /// формат записи и не чтение прежних словарей. Отдельные legacy-fixtures
    /// проверяют именно открытие старых байтов.
    #[test]
    fn the_single_dictionary_format_did_not_move_a_byte() {
        let vals: Vec<String> = (0..7000)
            .map(|i| format!("товар {i} категория {} цена {} руб описание", i % 37, i * 13))
            .collect();
        // (усилие, длина и сумма .sbin, длина и сумма .smrk, длина и сумма .sdic)
        let golden: [(Effort, usize, u32, usize, u32, usize, u32); 2] = [
            (Effort::Fast, 60004, 0xc169_568d, 544, 0xe3d9_ba5b, 33117, 0x0145_e1c0),
            (Effort::Thorough, 54635, 0xb513_03c0, 544, 0x1dc3_d7fd, 33117, 0xdd08_b07e),
        ];
        for (eff, dn, dc, mn, mc, sn, sc) in golden {
            let b = make(&vals, eff);
            assert_eq!(
                (b.data.len(), crate::wal::crc32(&b.data)),
                (dn, dc),
                "{eff:?}: .sbin другой"
            );
            assert_eq!(
                (b.marks.len(), crate::wal::crc32(&b.marks)),
                (mn, mc),
                "{eff:?}: .smrk другой"
            );
            assert_eq!(
                (b.dict.len(), crate::wal::crc32(&b.dict)),
                (sn, sc),
                "{eff:?}: .sdic другой"
            );
            // Сумм на кусок ровно столько же, сколько засечек.
            assert_eq!(b.crcs.len(), b.marks.len() / MARK * 4, "{eff:?}: сумм не по куску");
        }

        // И читается такой сегмент так же, как читался: ни таблицы, ни сумм
        // рядом нет — это в точности то, что лежит на диске у уже собранного.
        let d = dir("k1");
        let b = make(&vals, Effort::Fast);
        std::fs::write(format!("{d}/body.sbin"), &b.data).expect("sbin");
        std::fs::write(format!("{d}/body.smrk"), &b.marks).expect("smrk");
        std::fs::write(format!("{d}/body.sdic"), &b.dict).expect("sdic");
        let s = Store::open(&crate::container::loose(&d), "body")
            .expect("сегмент без таблицы обязан открыться");
        assert_eq!(s.dicts(), 1, "без таблицы словарь ровно один");
        assert!(!s.has_crcs(), "сумм нет — и притворяться, что есть, нечем");
        for (i, want) in vals.iter().enumerate() {
            assert_eq!(s.get(i as u32).as_deref(), Some(want.as_bytes()), "документ {i}");
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Два словаря, каждый со своим отрезком кусков: куски перенесены как есть.
    ///
    /// Это и есть то, ради чего заводилась таблица. Проверка фальсифицируема
    /// прямо в теле: те же байты **без** `.sdix` читаться не должны вовсе, а
    /// таблица, законная по строению, но с переставленными словарями, обязана
    /// расходиться по суммам — на то они и заведены.
    #[test]
    fn two_dictionaries_each_own_their_run_of_chunks() {
        let d = dir("two");
        let a = corpus("альфа", 3000);
        let z = corpus("зета", 2500);
        let ba = make(&a, Effort::Fast);
        let bz = make(&z, Effort::Fast);
        assert_ne!(ba.dict, bz.dict, "словари вышли одинаковыми — подменять нечего");

        graft(&d, &ba, a.len() as u32, &bz, true);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        assert_eq!(s.dicts(), 2, "таблица обещала два словаря");
        assert_eq!(s.blocks(), (ba.marks.len() + bz.marks.len()) / MARK);
        let all: Vec<&String> = a.iter().chain(z.iter()).collect();
        for (i, want) in all.iter().enumerate() {
            assert_eq!(s.get(i as u32).as_deref(), Some(want.as_bytes()), "документ {i}");
        }
        // И проход отдаёт то же самое, что выборка по номеру: у прохода свой
        // путь к словарю, и разъехаться они могут независимо.
        let mut w = s.walk();
        let mut n = 0usize;
        while let Some((doc, got)) = w.next() {
            assert_eq!(doc as usize, n, "номер поехал");
            assert_eq!(got, all[n].as_bytes(), "проход отдал документ {n} не тем");
            n += 1;
        }
        assert!(!w.broken(), "проход оборвался на порче");
        assert_eq!(n, all.len());
        // Суммы распакованного перенеслись вместе с кусками и сходятся.
        for i in 0..s.blocks() {
            let (_, _, raw) = s.chunk_at(i).expect("кусок читается");
            assert_eq!(s.chunk_crc(i), Some(crate::wal::crc32(&raw)), "сумма куска {i}");
        }
        drop(s);

        // Фальсификация первая: те же байты без таблицы. Один словарь на всё
        // хранилище — и он обязан быть отвергнут, а не принят за подходящий:
        // запись кончается задолго до конца `.sdic`, и это видно на открытии.
        std::fs::remove_file(format!("{d}/body.sdix")).expect("таблица снята");
        assert!(
            Store::open(&crate::container::loose(&d), "body").is_none(),
            "склеенные словари без таблицы приняты как один — значит таблица ничего не решает"
        );

        // Фальсификация вторая, и она про то, чего таблица поймать **не может**.
        // Словари меняются местами в самом `.sdic`, а таблица остаётся законной:
        // покрытие встык, первые куски возрастают от нуля. Каждый отрезок кусков
        // при этом достаётся чужому словарю. Ловит это только сумма
        // распакованного — на то она и заведена.
        let mut swapped = bz.dict.clone();
        swapped.extend_from_slice(&ba.dict);
        std::fs::write(format!("{d}/body.sdic"), &swapped).expect("sdic");
        let spans = [
            DictSpan { at: 0, len: bz.dict.len() as u32, first: 0 },
            DictSpan {
                at: bz.dict.len() as u32,
                len: ba.dict.len() as u32,
                first: (ba.marks.len() / MARK) as u32,
            },
        ];
        std::fs::write(
            format!("{d}/body.sdix"),
            build_sdix(&spans, ((ba.marks.len() + bz.marks.len()) / MARK) as u32),
        )
        .expect("sdix");
        let wrong = Store::open(&crate::container::loose(&d), "body")
            .expect("законная таблица обязана открыться");
        let sound = (0..wrong.blocks())
            .filter(|i| {
                wrong
                    .chunk_at(*i)
                    .is_some_and(|(_, _, raw)| wrong.chunk_crc(*i) == Some(crate::wal::crc32(&raw)))
            })
            .count();
        assert!(
            sound < wrong.blocks(),
            "подмена словаря прошла все проверки и сошлась по суммам — ловить её нечем"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Гнутая таблица — отказ на открытии, а не чужой документ в выдаче.
    ///
    /// Каждая порча здесь **пересчитывает** сумму таблицы: иначе всё ловила бы
    /// одна строка сверки суммы, а правила покрытия и возрастания остались бы
    /// непроверенными. Отдельно проверяется и сама сумма — на порче, которая
    /// пересчёта не получила.
    #[test]
    fn a_bent_dictionary_table_refuses_to_open() {
        let d = dir("bent");
        let a = corpus("альфа", 3000);
        let z = corpus("зета", 2500);
        let ba = make(&a, Effort::Fast);
        let bz = make(&z, Effort::Fast);
        graft(&d, &ba, a.len() as u32, &bz, true);
        let path = format!("{d}/body.sdix");
        let whole = std::fs::read(&path).expect("таблица");
        assert!(
            Store::open(&crate::container::loose(&d), "body").is_some(),
            "целая таблица обязана открываться"
        );

        // Пересчёт суммы после порчи: проверяются правила, а не сумма.
        let reseal = |mut b: Vec<u8>| {
            let n = b.len() - 4;
            let crc = crate::wal::crc32(&b[..n]);
            b[n..].copy_from_slice(&crc.to_le_bytes());
            b
        };
        let put = |b: &[u8]| std::fs::write(&path, b).expect("запись");
        let at_entry = |i: usize, o: usize| SDIX_HEAD + i * SDIX_ENTRY + o;
        let poke = |b: &[u8], off: usize, v: u32| {
            let mut x = b.to_vec();
            x[off..off + 4].copy_from_slice(&v.to_le_bytes());
            x
        };
        let read32 = |b: &[u8], off: usize| {
            u32::from_le_bytes(b[off..off + 4].try_into().expect("четыре байта"))
        };

        let first0 = read32(&whole, at_entry(0, 8));
        let first1 = read32(&whole, at_entry(1, 8));
        assert_eq!(first0, 0, "первый кусок первого словаря обязан быть нулевым");
        assert!(first1 > 0);

        let bends: Vec<(&str, Vec<u8>)> = vec![
            // Переставленные первые куски: порядок сломан, числа правдоподобны.
            (
                "первые куски переставлены",
                reseal({
                    let x = poke(&whole, at_entry(0, 8), first1);
                    poke(&x, at_entry(1, 8), first0)
                }),
            ),
            // Первый словарь начинается не с нулевого куска — начало колонки
            // осталось бы вовсе без словаря.
            ("первый кусок не нулевой", reseal(poke(&whole, at_entry(0, 8), 1))),
            // Отрезок за числом кусков: словарь, которому нечем владеть.
            ("первый кусок за концом", reseal(poke(&whole, at_entry(1, 8), u32::MAX / 2))),
            // Чужое смещение записи: разбор от него дал бы правдоподобную запись.
            ("смещение записи съехало", reseal(poke(&whole, at_entry(1, 0), 1))),
            // Длина записи, не доводящая до конца `.sdic`.
            (
                "записи не покрывают словарь",
                reseal(poke(&whole, at_entry(1, 4), read32(&whole, at_entry(1, 4)) - 1)),
            ),
            // Одна и та же запись названа дважды. Покрытие **в сумме** сходится,
            // первые куски возрастают от нуля, каждая запись сама по себе годна —
            // и всё же средний отрезок кусков достаётся чужому словарю. Ловится
            // это только требованием идти встык: ни одна запись не начинается
            // там, где кончилась не предыдущая.
            (
                "запись названа дважды",
                build_sdix(
                    &[
                        DictSpan { at: 0, len: ba.dict.len() as u32, first: 0 },
                        DictSpan { at: 0, len: ba.dict.len() as u32, first: 1 },
                        DictSpan {
                            at: ba.dict.len() as u32,
                            len: bz.dict.len() as u32,
                            first: (ba.marks.len() / MARK) as u32,
                        },
                    ],
                    ((ba.marks.len() + bz.marks.len()) / MARK) as u32,
                ),
            ),
            // Число кусков не то, что в `.smrk`.
            ("число кусков не сошлось", reseal(poke(&whole, 12, 7))),
            ("опознание не то", reseal(poke(&whole, 0, 0x4b4f_4f4c))),
            ("версия из будущего", reseal(poke(&whole, 4, 2))),
            // Число словарей не то, что в длине файла.
            ("число словарей не сошлось", reseal(poke(&whole, 8, 3))),
            // А это — без пересчёта: ловится именно суммой.
            ("сумма не сошлась", poke(&whole, at_entry(1, 8), first1 + 1)),
            ("таблица обрезана", whole[..whole.len() - 1].to_vec()),
            ("таблица пуста", Vec::new()),
        ];
        for (why, bent) in bends {
            put(&bent);
            assert!(
                Store::open(&crate::container::loose(&d), "body").is_none(),
                "таблица принята: {why}"
            );
        }
        put(&whole);
        assert!(
            Store::open(&crate::container::loose(&d), "body").is_some(),
            "возвращённая таблица не открылась"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Подменённый байт в `.sbin`, который **всё-таки распаковался**, ловится
    /// суммой распакованного — и ничем больше.
    ///
    /// ## Что здесь ловится
    ///
    /// Сводка `sums.tsv` доказывает, что байты `.sbin` те же, какими легли. Она
    /// ничего не говорит о том, что они распаковываются в то же самое, — а
    /// укрупнение будет переносить куски между сегментами, и там сводка новая.
    /// Кусок, легший в файл как есть (шум сжатию не поддаётся), при перевёрнутом
    /// байте распаковывается **безупречно**: длина сошлась, число значений
    /// сошлось, разбор по длинам сошёлся. Не сходится только содержимое.
    #[test]
    fn a_flipped_byte_that_still_unpacks_is_caught_by_the_chunk_sum() {
        let d = dir("scrc");
        // Кусок закрывается на шестнадцати килобайтах, то есть на семьдесят
        // четвёртом значении по двести двадцать четыре байта. Ровно пять таких
        // кусков, а хвостовым остаётся одно короткое значение — вот оно-то и
        // ляжет в файл как есть: выборка под словарь до него не дотянулась, а
        // короче собственного заголовка не сожмёшь.
        let mut vals = noise(7, 370, 220);
        vals.extend(noise(11, 1, 64));
        let b = make_bytes(&vals);
        std::fs::write(format!("{d}/body.sbin"), &b.data).expect("sbin");
        std::fs::write(format!("{d}/body.smrk"), &b.marks).expect("smrk");
        std::fs::write(format!("{d}/body.sdic"), &b.dict).expect("sdic");
        std::fs::write(format!("{d}/body.scrc"), &b.crcs).expect("scrc");

        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        assert!(s.has_crcs(), "суммы обязаны быть");
        assert!(s.blocks() > 1, "кусков всего {} — портить нечего", s.blocks());
        // Кусок, легший как есть: первый байт тела — признак «без сжатия».
        let victim = (0..s.blocks())
            .find(|i| {
                let (off, _, _, _) = s.mark(*i);
                b.data[off] == 0
            })
            .expect("шум обязан лечь хотя бы одним куском без сжатия");
        let (off, len, _, _) = s.mark(victim);
        for i in 0..s.blocks() {
            let (_, _, raw) = s.chunk_at(i).expect("кусок читается");
            assert_eq!(s.chunk_crc(i), Some(crate::wal::crc32(&raw)), "сумма куска {i}");
        }
        let whole = s.chunk_at(victim).expect("кусок читается");
        drop(s);

        // Портим байт в середине тела — не признак и не длину значения.
        let mut data = b.data.clone();
        data[off + len / 2] ^= 0x20;
        std::fs::write(format!("{d}/body.sbin"), &data).expect("sbin");

        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let (first, n, raw) = s.chunk_at(victim).expect("кусок обязан распаковаться как ни в чём");
        // Разбор обязан остаться целым, иначе меряется отказ распаковщика, а не
        // сумма: числа значений и первого номера сравниваются с теми же до порчи.
        assert_eq!((first, n), (whole.0, whole.1), "порча сломала разбор — меряется не то");
        assert_ne!(raw, whole.2, "перевёрнутый байт не дошёл до распакованного");
        assert_ne!(
            s.chunk_crc(victim),
            Some(crate::wal::crc32(&raw)),
            "перевёрнутый байт не сдвинул сумму распакованного"
        );
        // И выдача при этом врёт: без суммы поймать это было бы нечем.
        let lying = (0..vals.len())
            .filter(|i| s.get(*i as u32).as_deref() != Some(vals[*i].as_slice()))
            .count();
        assert!(lying > 0, "порча не изменила выдачи — проверять нечего");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Отображение `.sdic` одно на все словари, а таблицы распаковки строятся по
    /// одной — той, которую и правда спросили.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Наивные `K` словарей — это `K` вызовов `mmap` на **один** файл, то есть
    /// `K` областей у ядра при потолке `vm.max_map_count` в 65530 и двух десятках
    /// областей на сегмент и без того. И `K` таблиц распаковки по тридцать два
    /// килобайта: шестьдесят восемь словарей это 2.2 МБ на поле, на двух десятках
    /// сегментов — восемьдесят семь мегабайт кучи ради выдачи одного документа.
    ///
    /// Счётчик областей у процесса общий, и тесты идут вперемешку — сравнивать по
    /// нему нечего. Зато число ссылок на отображение и число построенных таблиц
    /// не зависят ни от кого: они и проверяются.
    #[test]
    fn one_mapping_serves_every_dictionary_and_only_the_asked_one_is_built() {
        let d = dir("lazy");
        let a = corpus("альфа", 3000);
        let z = corpus("зета", 2500);
        let ba = make(&a, Effort::Fast);
        let bz = make(&z, Effort::Fast);
        graft(&d, &ba, a.len() as u32, &bz, true);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        assert_eq!(s.dicts(), 2);

        let built = |s: &Store| s.decs.iter().filter(|c| c.get().is_some()).count();
        assert_eq!(built(&s), 0, "словарь построен до того, как его спросили");
        assert_eq!(s.sdic.owners(), 1, "отображение уже разошлось");

        // Точечная выдача трогает ровно один словарь.
        assert_eq!(s.get(0).as_deref(), Some(a[0].as_bytes()));
        assert_eq!(built(&s), 1, "выдача одного документа построила лишние словари");

        // Документ из второй половины поднимает второй — и **тот же** файл.
        assert_eq!(s.get(a.len() as u32).as_deref(), Some(z[0].as_bytes()));
        assert_eq!(built(&s), 2);
        assert_eq!(s.sdic.owners(), 3, "у словарей оказалось не одно отображение на всех");
        std::fs::remove_dir_all(&d).ok();
    }

    /// `Store` читают из нескольких потоков разом — и ленивые словари этому не
    /// мешают.
    ///
    /// Выдача поднимается из `thread::scope` в `Fetcher::extract`, поэтому
    /// `Store` обязан оставаться `Sync`. Проверка не украшение: `RefCell` или
    /// `Cell` на месте `OnceLock` дали бы ровно ту же выдачу в один поток и
    /// перестали бы собираться здесь.
    #[test]
    fn many_threads_read_one_store_at_once() {
        let d = dir("shared");
        let a = corpus("альфа", 3000);
        let z = corpus("зета", 2500);
        let ba = make(&a, Effort::Fast);
        let bz = make(&z, Effort::Fast);
        graft(&d, &ba, a.len() as u32, &bz, true);
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let all: Vec<&String> = a.iter().chain(z.iter()).collect();
        std::thread::scope(|scope| {
            for t in 0..8usize {
                let s = &s;
                let all = &all;
                scope.spawn(move || {
                    for i in (t..all.len()).step_by(8) {
                        assert_eq!(
                            s.get(i as u32).as_deref(),
                            Some(all[i].as_bytes()),
                            "документ {i} в потоке {t}"
                        );
                    }
                });
            }
        });
        std::fs::remove_dir_all(&d).ok();
    }
}

/// Перенос: то, ради чего укрупнение перестало пересжимать хранимое.
#[cfg(test)]
mod transfer {
    use super::*;

    fn dir(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-carry-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        d.to_string_lossy().to_string()
    }

    fn corpus(tag: &str, n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("{tag} запись {i} свойство {} значение {}", i % 29, i * 31))
            .collect()
    }

    fn make(vals: &[String]) -> Built {
        build_with(
            |e: &mut dyn FnMut(&[u8])| vals.iter().for_each(|s| e(s.as_bytes())),
            Effort::Fast,
            1,
            "проверка/body.sbin",
        )
        .expect("влезло")
        .expect("собрано")
    }

    fn read(d: &str, ext: &str) -> Vec<u8> {
        std::fs::read(format!("{d}/body.{ext}")).unwrap_or_default()
    }

    /// Дописывание значений даёт **те же байты**, что сборка с нуля.
    ///
    /// ## Почему это главное утверждение о `push`
    ///
    /// Перенос дописывает значения там, где переносить нечего: у источника,
    /// не прошедшего сторож против россыпи, у поля, которого в источнике нет, у
    /// распущенного отрезка. Разойдись раскладка `push` с раскладкой [`build`]
    /// хоть в границе куска, хоть в порядке полей засечки — и одна и та же
    /// колонка резалась бы на куски по-разному в зависимости от пути, которым
    /// сегмент собрался. Сравнение «читается обратно» этого не ловило бы: своё
    /// читается своим безупречно.
    ///
    /// Словарь здесь взят у самой сборки: доказывается раскладка, а не обучение.
    ///
    /// Значения здесь **ровной длины**, и не для красоты: при пятистах восьми
    /// байтах запись занимает ровно пятьсот двенадцать, и кусок набирается точно
    /// до `BLOCK`, ни байтом больше. Только на таком входе видно, закрывается
    /// кусок по «набралось» или по «переполнилось»: на обычных значениях длина
    /// перескакивает границу, и разница в один байт остаётся незаметной.
    #[test]
    fn pushing_lays_down_the_same_bytes_as_building() {
        let mut vals = corpus("альфа", 4000);
        vals.extend((0..300).map(|i| format!("{i:>508}").replace(' ', "я")[..508].to_string()));
        let b = make(&vals);
        let d = dir("push");
        let mut tr = Transfer::create(&d, "body", Effort::Fast).expect("завёлся");
        tr.open_dict(&b.dict).expect("словарь принят");
        for v in vals.iter() {
            tr.push(v.as_bytes()).expect("значение");
        }
        let made = tr.finish().expect("дописано");

        assert_eq!(made.docs as usize, vals.len(), "накрыто не столько документов");
        assert_eq!(made.blocks, b.marks.len() / MARK, "кусков вышло другое число");
        assert_eq!(made.dicts, 1);
        assert_eq!(read(&d, "sbin"), b.data, "тела кусков разошлись со сборкой");
        assert_eq!(read(&d, "smrk"), b.marks, "засечки разошлись со сборкой");
        assert_eq!(read(&d, "sdic"), b.dict, "словарь лёг не тем");
        assert_eq!(read(&d, "scrc"), b.crcs, "суммы распакованного разошлись");
        assert!(
            std::fs::metadata(format!("{d}/body.sdix")).is_err(),
            "при одном словаре таблицы быть не должно — иначе формат сдвинулся"
        );

        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        for (i, want) in vals.iter().enumerate() {
            assert_eq!(s.get(i as u32).as_deref(), Some(want.as_bytes()), "документ {i}");
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Перенос копирует тело **дословно** и переписывает одну засечку.
    ///
    /// ## Что здесь доказывается
    ///
    /// Что номера документов в теле куска не хранятся вовсе: перенумерация — это
    /// переписать первый номер в засечке, а тело скопировать. Поэтому вывод
    /// обязан быть побайтовой склейкой тел источников, а компрессор не позван ни
    /// разу.
    ///
    /// Сравнение «документы читаются» этого бы не поймало: пересжатие теми же
    /// словарями дало бы ту же выдачу и совсем другие байты.
    #[test]
    fn carrying_copies_the_body_and_rewrites_only_the_mark() {
        let a = corpus("альфа", 4000);
        let z = corpus("зета", 3000);
        let (ba, bz) = (make(&a), make(&z));
        assert_ne!(ba.dict, bz.dict, "словари вышли одинаковыми — переносить нечего различать");

        let da = dir("carrya");
        let dz = dir("carryz");
        for (d, b) in [(&da, &ba), (&dz, &bz)] {
            std::fs::write(format!("{d}/body.sbin"), &b.data).expect("sbin");
            std::fs::write(format!("{d}/body.smrk"), &b.marks).expect("smrk");
            std::fs::write(format!("{d}/body.sdic"), &b.dict).expect("sdic");
            std::fs::write(format!("{d}/body.scrc"), &b.crcs).expect("scrc");
        }
        let sa = Store::open(&crate::container::loose(&da), "body").expect("первое");
        let sz = Store::open(&crate::container::loose(&dz), "body").expect("второе");

        let out = dir("carryout");
        let mut tr = Transfer::create(&out, "body", Effort::Fast).expect("завёлся");
        for st in [&sa, &sz] {
            tr.open_dict(st.dict_record(0).expect("запись")).expect("словарь принят");
            for c in 0..st.blocks() {
                let (_, _, raw, first) = st.mark_at(c).expect("засечка");
                let next = if c + 1 < st.blocks() {
                    st.mark_at(c + 1).expect("засечка").3
                } else {
                    first + st.chunk(c).expect("кусок").1 as u32
                };
                let body = st.chunk_body(c).expect("тело");
                let crc = st.chunk_crc(c).expect("сумма");
                tr.carry(body, raw, next - first, crc).expect("перенесено");
            }
        }
        let made = tr.finish().expect("дописано");

        assert_eq!(made.packed, 0, "перенос звал компрессор {} раз", made.packed);
        assert_eq!(made.dicts, 2);
        assert_eq!(made.docs as usize, a.len() + z.len());

        let mut want = ba.data.clone();
        want.extend_from_slice(&bz.data);
        assert_eq!(read(&out, "sbin"), want, "тела кусков не склеились байт в байт");
        let mut wdic = ba.dict.clone();
        wdic.extend_from_slice(&bz.dict);
        assert_eq!(read(&out, "sdic"), wdic, "словари не легли подряд");
        let mut wcrc = ba.crcs.clone();
        wcrc.extend_from_slice(&bz.crcs);
        assert_eq!(read(&out, "scrc"), wcrc, "суммы не скопированы дословно");
        assert!(
            std::fs::metadata(format!("{out}/body.sdix")).is_ok(),
            "таблицы нет при двух словарях"
        );

        // И читается это как одно хранилище, с новыми номерами.
        let s = Store::open(&crate::container::loose(&out), "body").expect("открылось");
        let all: Vec<&String> = a.iter().chain(z.iter()).collect();
        for (i, w) in all.iter().enumerate() {
            assert_eq!(s.get(i as u32).as_deref(), Some(w.as_bytes()), "документ {i}");
        }
        for i in 0..s.blocks() {
            let (_, _, raw) = s.chunk_at(i).expect("кусок");
            assert_eq!(s.chunk_crc(i), Some(crate::wal::crc32(&raw)), "сумма куска {i}");
        }
        for d in [da, dz, out] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Смена словаря закрывает недобранный кусок.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Кусок принадлежит ровно одному словарю. Оставь `open_dict` недобранный
    /// кусок висеть — и он лёг бы **после** первого куска нового отрезка, то
    /// есть в чужом отрезке, и распаковывался бы чужим словарём. Ни таблица, ни
    /// её проверки этого не видят: покрытие встык сходится, первые куски
    /// возрастают. Видна только выдача — и только если словари достаточно
    /// разные.
    #[test]
    fn opening_a_dictionary_seals_the_chunk_of_the_previous_one() {
        let a = corpus("альфа", 4000);
        let z = corpus("зета", 3000);
        let (ba, bz) = (make(&a), make(&z));
        let d = dir("seal");
        let mut tr = Transfer::create(&d, "body", Effort::Fast).expect("завёлся");
        tr.open_dict(&ba.dict).expect("первый словарь");
        // Ровно столько, чтобы кусок остался недобранным.
        for v in a.iter().take(30) {
            tr.push(v.as_bytes()).expect("значение");
        }
        tr.open_dict(&bz.dict).expect("второй словарь");
        for v in z.iter().take(300) {
            tr.push(v.as_bytes()).expect("значение");
        }
        let made = tr.finish().expect("дописано");
        assert_eq!(made.dicts, 2, "оба словаря обязаны попасть в таблицу");
        assert_eq!(made.docs, 330);

        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        let all: Vec<&String> = a.iter().take(30).chain(z.iter().take(300)).collect();
        for (i, w) in all.iter().enumerate() {
            assert_eq!(s.get(i as u32).as_deref(), Some(w.as_bytes()), "документ {i}");
        }
        // Отрезок второго словаря начинается не с нулевого куска — значит
        // недобранный кусок первого и правда закрылся до смены.
        assert_eq!(s.dict_first(0), Some(0));
        assert!(s.dict_first(1).is_some_and(|f| f > 0), "второй словарь забрал первый кусок");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Отрезок, у которого не оказалось ни одного куска, в таблицу не попадает.
    ///
    /// Так выглядит источник, все документы которого погашены. Попади его
    /// словарь в таблицу — первые куски перестали бы строго возрастать, и
    /// `.sdix` был бы отвергнут на открытии: поле осталось бы без содержимого.
    #[test]
    fn a_run_without_chunks_never_reaches_the_table() {
        let a = corpus("альфа", 2000);
        let z = corpus("зета", 2000);
        let (ba, bz) = (make(&a), make(&z));
        let d = dir("emptyrun");
        let mut tr = Transfer::create(&d, "body", Effort::Fast).expect("завёлся");
        tr.open_dict(&ba.dict).expect("первый словарь");
        tr.recode(&[], 0).expect("кусок, где погашены все");
        tr.open_dict(&bz.dict).expect("второй словарь");
        for v in z.iter() {
            tr.push(v.as_bytes()).expect("значение");
        }
        let made = tr.finish().expect("дописано");
        assert_eq!(made.dicts, 1, "словарь без кусков попал в таблицу");
        assert!(
            std::fs::metadata(format!("{d}/body.sdix")).is_err(),
            "таблица заведена при одном словаре"
        );
        let s = Store::open(&crate::container::loose(&d), "body").expect("открылось");
        for (i, w) in z.iter().enumerate() {
            assert_eq!(s.get(i as u32).as_deref(), Some(w.as_bytes()), "документ {i}");
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Значений не было вовсе — файлов тоже не остаётся.
    #[test]
    fn a_transfer_without_a_single_value_leaves_nothing_behind() {
        let d = dir("nothing");
        let b = make(&corpus("альфа", 100));
        let mut tr = Transfer::create(&d, "body", Effort::Fast).expect("завёлся");
        tr.open_dict(&b.dict).expect("словарь");
        let made = tr.finish().expect("дописано");
        assert_eq!((made.docs, made.blocks), (0, 0));
        for ext in ["sbin", "smrk", "sdic", "scrc", "sdix"] {
            assert!(
                std::fs::metadata(format!("{d}/body.{ext}")).is_err(),
                "остался {ext} у хранилища без значений"
            );
        }
        assert!(Store::open(&crate::container::loose(&d), "body").is_none());
        std::fs::remove_dir_all(&d).ok();
    }

    /// Кусок без открытого словаря — отказ, а не молчаливая пропажа.
    #[test]
    fn a_chunk_without_a_dictionary_is_refused() {
        let d = dir("nodict");
        let mut tr = Transfer::create(&d, "body", Effort::Fast).expect("завёлся");
        tr.push("значение".as_bytes()).expect("копится без словаря — беды нет");
        let e = tr.finish().expect_err("сжать нечем — обязан быть отказ");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("словар"), "отказ не про словарь: {e}");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Негодная запись словаря не принимается вовсе.
    #[test]
    fn a_bent_dictionary_record_is_not_accepted() {
        let d = dir("bentrec");
        let b = make(&corpus("альфа", 2000));
        let mut tr = Transfer::create(&d, "body", Effort::Fast).expect("завёлся");
        for cut in [0usize, 3, b.dict.len() / 2, b.dict.len() - 1] {
            assert!(tr.open_dict(&b.dict[..cut]).is_err(), "запись, обрезанная до {cut}, принята");
        }
        let mut tail = b.dict.clone();
        tail.push(0);
        assert!(tr.open_dict(&tail).is_err(), "запись с хвостом принята");
        assert!(tr.open_dict(&b.dict).is_ok(), "целая запись обязана приниматься");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Правило пересборки таблицы словарей: числа, а не сегменты.
    ///
    /// Собрать три сотни отрезков настоящими сегментами — это три сотни
    /// источников слияния; правило же выражается целиком в числах, и проверяется
    /// ими. Каждый случай фальсифицируем: сдвинь порог на единицу — и он падает.
    #[test]
    fn the_dictionary_table_is_rebuilt_only_when_it_has_grown() {
        let small = 40u64 << 10;
        let column = 1u64 << 30;

        // Обычное укрупнение: восемь отрезков, словари мелкие — не трогаем.
        let keep = coalesce(&[1u64 << 20; 8], &[small; 8], column);
        assert!(keep.iter().all(|k| *k), "нетронутую таблицу всё-таки пересобрали");

        // Ровно на потолке числа словарей — ещё терпимо.
        let keep = coalesce(&vec![1u64 << 20; DICT_COUNT_MAX], &vec![64; DICT_COUNT_MAX], column);
        assert_eq!(
            keep.iter().filter(|k| **k).count(),
            DICT_COUNT_MAX,
            "потолок сработал на нём самом"
        );

        // На один сверх потолка — падаем ровно до `DICT_COUNT_KEEP`.
        let n = DICT_COUNT_MAX + 1;
        let volumes: Vec<u64> = (0..n).map(|i| (i as u64 + 1) * 1024).collect();
        let keep = coalesce(&volumes, &vec![64; n], column);
        assert_eq!(keep.iter().filter(|k| **k).count(), DICT_COUNT_KEEP);
        // Распущены **наименьшие**: пересжать придётся именно их.
        assert!(keep[n - DICT_COUNT_KEEP..].iter().all(|k| *k), "распустили не наименьшие");
        assert!(keep[..n - DICT_COUNT_KEEP].iter().all(|k| !*k), "оставили лишних");

        // Порог по объёму. Сумма словарей за `DICT_TOTAL_FLOOR` при мелкой
        // колонке — пересобираем; на байт меньше — нет.
        const N: usize = 200;
        let heavy = DICT_TOTAL_FLOOR as u64 / N as u64 + 1;
        let keep = coalesce(&[1u64 << 20; N], &[heavy; N], 1 << 20);
        assert_eq!(
            keep.iter().filter(|k| **k).count(),
            DICT_COUNT_KEEP,
            "объём словарей не сработал"
        );
        let light = DICT_TOTAL_FLOOR as u64 / N as u64 - 1;
        let keep = coalesce(&[1u64 << 20; N], &[light; N], 1 << 20);
        assert!(keep.iter().all(|k| *k), "под порогом объёма таблицу всё-таки пересобрали");

        // Крупная колонка поднимает потолок объёма над полом: та же сумма
        // словарей при колонке в четыре гигабайта — уже не повод.
        let keep = coalesce(&[1u64 << 20; N], &[heavy; N], 4u64 << 30);
        assert!(keep.iter().all(|k| *k), "потолок объёма не растёт вместе с колонкой");

        // Мало отрезков — распускать нечего, каким бы ни был объём словарей.
        let keep = coalesce(&[1u64 << 20; 4], &[4u64 << 20; 4], 1 << 20);
        assert!(keep.iter().all(|k| *k), "распустили соседа при четырёх отрезках");
    }
}
