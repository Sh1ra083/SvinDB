//! Словарь терминов: слово — в номер списка.

use crate::mmap::Region;

/// Через сколько слов ставится опора.
pub const ANCHOR: usize = 64;

/// Сколько форм берётся из маски по умолчанию. Столько же берёт Elasticsearch.
pub const DEFAULT_EXPANSIONS: usize = 50;

/// Потолок смещения внутри сегмента: опоры везде тридцатидвухбитные.
///
/// Потолок — сам `u32::MAX`, а не «на единицу меньше»: такое смещение ещё
/// представимо четырьмя байтами, и читатель его принимает. Сдвиг границы на
/// единицу отказал бы сегментам, которые открываются.
pub const OFFSET_CEILING: u64 = u32::MAX as u64;

/// Действующий потолок смещения: то, с чем сравнивается **всякая** охрана.
///
/// В выпуске это сам [`OFFSET_CEILING`] и ничего больше — вызов подставляется
/// целиком, и от постоянной он не отличается ни байтом.
///
/// ## Зачем шов
///
/// Охраны потолка проверялись точными числами — и только сами предикаты. А цена
/// ошибки лежит не в предикате, а в **проводке**: предикат обязан стоять между
/// входом и файлом. Снятие трёх охран разом оставляло весь набор зелёным —
/// значит проводку не проверял никто. Настоящим входом её не проверить: это
/// четыре гигабайта на каждую охрану. Опущенный потолок даёт то же самое на
/// входе в сотню байт: обычный маленький вход, и запись обязана отказать.
///
/// Ячейка потоко-местная, а не общая: проверки идут параллельно, и опущенный
/// одной потолок отказал бы записи в соседней. Тот же приём и по той же причине
/// уже стоит у `merge::ORDER_CALLS`.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn offset_ceiling() -> u64 {
    OFFSET_CEILING
}

#[cfg(test)]
thread_local! {
    static CEILING: std::cell::Cell<u64> = const { std::cell::Cell::new(OFFSET_CEILING) };
}

#[cfg(test)]
pub(crate) fn offset_ceiling() -> u64 {
    CEILING.with(|c| c.get())
}

/// Опущенный потолок держится, пока жив ответ [`lower_offset_ceiling`].
///
/// Возврат прежнего значения обязателен: libtest переиспользует потоки между
/// проверками, и оставленный на полу потолок отказал бы следующей — отказом,
/// которого она не ждёт и объяснить не сможет.
#[cfg(test)]
pub(crate) struct LoweredCeiling(u64);

#[cfg(test)]
impl Drop for LoweredCeiling {
    fn drop(&mut self) {
        CEILING.with(|c| c.set(self.0));
    }
}

/// Опускает потолок смещения на этом потоке. Только для проверок.
#[cfg(test)]
#[must_use = "потолок вернётся на место тут же, если ответ уронить"]
pub(crate) fn lower_offset_ceiling(to: u64) -> LoweredCeiling {
    LoweredCeiling(CEILING.with(|c| c.replace(to)))
}

/// Влезает ли очередная опора словаря в четыре байта.
///
/// ## Почему проверяется, а не подразумевается
///
/// Опора в `dict.idx` — смещение внутри `dict.blob`, и лежит оно в четырёх
/// байтах. Заворот такой опоры не роняет ничего и не портит ни одного
/// заголовка: она просто начинает указывать в середину чужого слова. Двоичный
/// поиск сходится не туда, и слово либо не находится вовсе, либо находится
/// чужое — молча, прямо на выдаче. Из пяти тридцатидвухбитных смещений сегмента
/// это самое тихое: `walk_terms` ловит только заворот `terms.bin`.
///
/// Отказ, а не паника: выпуск собирается с `panic = "abort"`, и паника на пути
/// приёма убила бы узел вместо того, чтобы отказать одному сбросу.
fn fits_anchor(len: usize, blob: &str, at: usize) -> std::io::Result<()> {
    let ceiling = offset_ceiling();
    if len as u64 <= ceiling {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{blob}: словарь перевалил за четыре гигабайта на {at}-м слове \
             ({len} байт при потолке {ceiling}) — опора рядом с ним \
             тридцатидвухбитная и завернулась бы на чужое слово; \
             уменьшите flush_docs у приёма или target_docs у укрупнения"
        ),
    ))
}

/// Кодирует отсортированные пары `(поле, слово)` во фронт-кодированный словарь.
///
/// `blob` — куда ляжет вывод; нужен только затем, чтобы отказ по потолку назвал
/// файл. Один и тот же кодировщик собирает и `dict.blob`, и `idmap.blob`, и без
/// имени оператор не понял бы, какой из них упёрся.
///
/// ## Почему номер поля лежит не в каждой записи
///
/// Ключи упорядочены по полю, а потом по слову, — значит поле меняется не чаще,
/// чем число полей, на весь словарь. Байт на каждую запись хранил величину,
/// которая у соседей совпадает почти всегда: 2.06 Б/док на осевшем сегменте и
/// 4.40 на приёмном, где слов на документ вдвое больше.
///
/// Признак смены уезжает младшим битом в длину общего начала — та почти всегда
/// меньше шестидесяти четырёх, и varint от неё остаётся однобайтовым. На опоре
/// признак стоит всегда: двоичный поиск сравнивает опорные ключи между собой, и
/// поле у них обязано читаться без оглядки на соседей.
pub fn encode(keys: &[(u32, String)], blob: &str) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let mut out: Vec<u8> = Vec::with_capacity(keys.len() * 4);
    let mut coder = Coder::new(keys.len());
    for (fld, term) in keys.iter() {
        coder.push(*fld, term.as_bytes(), 0, &mut out, blob)?;
    }
    Ok((out, coder.anchors))
}

/// Кодировщик словаря: одна пара за раз, поверх любого приёмника.
///
/// ## Почему он один на два пути
///
/// Словарь пишут двое. Приём держит слова сброса в памяти и так, и кодирует их
/// одним куском ([`encode`]). Слияние — нет: его словарь это объединение
/// словарей всех источников, и на укрупнении в десятки миллионов документов
/// сами ключи занимали больше, чем готовый `dict.blob`. Кодировщику при этом
/// дальше **одного** предыдущего слова смотреть не на что, поэтому у слияния
/// поток ([`Writer`]).
///
/// Кодирование обязано остаться ровно одним. Разойдись эти два хоть на байт — и
/// словарь слитого сегмента разбирался бы не тем же кодом, что приёмного:
/// двоичный поиск сошёлся бы не туда, а слово нашлось бы чужое. Молча, прямо на
/// выдаче.
struct Coder {
    /// Предыдущее слово: от него считается общее начало.
    prev: Vec<u8>,
    /// Поле предыдущего слова. `u32::MAX` — слов ещё не было.
    prev_field: u32,
    /// Сколько пар уже закодировано — по нему ставятся опоры.
    n: usize,
    anchors: Vec<u8>,
}

impl Coder {
    fn new(hint: usize) -> Coder {
        Coder {
            prev: Vec::with_capacity(64),
            prev_field: u32::MAX,
            n: 0,
            anchors: Vec::with_capacity(hint.div_ceil(ANCHOR) * 4),
        }
    }

    /// Дописывает пару `(поле, слово)` в `buf`.
    ///
    /// `base` — сколько байт словаря уже ушло из `buf` наружу: опора это
    /// смещение **в файле**, а не в буфере, и буфер опустошается, а смещение
    /// нет.
    fn push(
        &mut self,
        fld: u32,
        term: &[u8],
        base: usize,
        buf: &mut Vec<u8>,
        blob: &str,
    ) -> std::io::Result<()> {
        let at = base + buf.len();
        let anchor = self.n.is_multiple_of(ANCHOR);
        if anchor {
            // Отказ **до** записи усечённой опоры, а не после: записанная опора
            // уже неотличима от настоящей, и починить её потом нечем.
            fits_anchor(at, blob, self.n + 1)?;
            self.anchors.extend((at as u32).to_le_bytes());
        }
        // Общее начало считается только внутри одного поля: у соседей из
        // разных полей общего начала нет по определению порядка.
        let changed = anchor || fld != self.prev_field;
        let common = if changed {
            0
        } else {
            term.iter().zip(self.prev.iter()).take_while(|(a, b)| a == b).count()
        };
        varint(buf, ((common as u64) << 1) | u64::from(changed));
        varint(buf, (term.len() - common) as u64);
        if changed {
            // Номер поля varint, а не байт. Байт держал потолок в 256 полей —
            // на плоской схеме незаметно, на вложенной упирается: у ES
            // умолчание тысяча. Места это не стоит: номер пишется только при
            // смене поля, то есть не чаще, чем полей во всём словаре, а у
            // номеров до 128 varint и байт побайтово совпадают.
            varint(buf, u64::from(fld));
        }
        buf.extend_from_slice(&term[common..]);
        self.prev.clear();
        self.prev.extend_from_slice(term);
        self.prev_field = fld;
        self.n += 1;
        Ok(())
    }
}

/// Потоковая запись `dict.blob`.
///
/// Слияние прежде держало слитый словарь в памяти **дважды**: сырые ключи
/// обхода и их же копию строками для [`encode`], — и только потом получало
/// третьим сам блоб. Ключи при этом приходят по возрастанию и ни один из них
/// после записи не перечитывается.
///
/// Опоры остаются в памяти: их четыре байта на шестьдесят четыре слова, и
/// отдать их надо одним куском в `dict.idx`.
pub struct Writer {
    sink: std::io::BufWriter<std::fs::File>,
    path: String,
    /// Черновик **одной** записи. Пуст на входе в `push` и на выходе из него:
    /// иначе смещение опоры пришлось бы складывать из двух слагаемых, а
    /// сложение это и есть то место, где такие вещи разъезжаются.
    rec: Vec<u8>,
    /// Длина файла. Опоры считаются от неё и ни от чего больше.
    len: usize,
    coder: Coder,
}

/// Размер буфера записи. Собирать больше незачем: он не для смещений, а лишь
/// затем, чтобы не звать ядро на каждое слово.
const FLUSH: usize = 1 << 18;

impl Writer {
    pub fn create(path: &str) -> std::io::Result<Writer> {
        Ok(Writer {
            sink: std::io::BufWriter::with_capacity(FLUSH, std::fs::File::create(path)?),
            path: path.to_string(),
            rec: Vec::with_capacity(256),
            len: 0,
            coder: Coder::new(0),
        })
    }

    /// Дописывает пару. Байты, а не строка: обход словарей отдаёт срез, и
    /// заводить под каждое слово `String` незачем.
    pub fn push(&mut self, fld: u32, term: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        self.coder.push(fld, term, self.len, &mut self.rec, &self.path)?;
        self.sink.write_all(&self.rec)?;
        self.len += self.rec.len();
        self.rec.clear();
        Ok(())
    }

    /// Дописывает хвост буфера, **доводит файл до диска** и отдаёт опоры.
    ///
    /// Довод тот же, что у `merge::write`: `write_all` возвращается, когда байты
    /// приняты ядром, а не когда легли на носитель, и порядок «заголовок
    /// последним» без этого ничего не обещает.
    pub fn finish(mut self) -> std::io::Result<Vec<u8>> {
        use std::io::Write;
        self.sink.flush()?;
        self.sink.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        Ok(self.coder.anchors)
    }
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline(always)]
fn read_varint(src: &[u8], at: &mut usize) -> usize {
    let mut v = 0usize;
    let mut shift = 0u32;
    loop {
        let Some(&b) = src.get(*at) else { return v };
        *at += 1;
        v |= ((b & 0x7f) as usize) << shift;
        if b < 0x80 {
            return v;
        }
        shift += 7;
    }
}

/// Одиннадцать байт на запись — раскладка словаря до фронт-кодирования.
const FLAT_ENTRY: usize = 11;

pub struct Dict {
    blob: Region,
    anchors: Region,
    len: usize,
    /// Седьмая раскладка: номер поля пишется только при смене.
    slim: bool,
    /// Восьмая: номер поля varint, и потолка в 256 полей больше нет.
    wide: bool,
    /// Держится ради уже собранных индексов. Опознаётся по длине `dict.idx`:
    /// плоский занимает ровно одиннадцать байт на слово, фронт-кодированный —
    /// четыре на опору, то есть в сто семьдесят раз меньше. Спутать нельзя.
    flat: bool,
}

/// Проход по группе опоры: восстанавливает слова подряд.
///
/// Всякий проход начинается с опоры — и в поиске, и в маске, и в переборе. Это
/// не удобство, а условие: номер поля пишется только при смене, а на опоре он
/// стоит всегда, так что к первому же слову он уже известен.
struct Walk<'a> {
    src: &'a [u8],
    at: usize,
    word: Vec<u8>,
    field: u32,
    /// Седьмая раскладка: номер поля только при смене. Шестая — в каждой записи.
    slim: bool,
    /// Восьмая: номер поля varint. До неё — ровно байт, и полей не больше 256.
    wide: bool,
}

impl<'a> Walk<'a> {
    fn new(src: &'a [u8], at: usize, slim: bool, wide: bool) -> Walk<'a> {
        Walk { src, at, word: Vec::with_capacity(48), field: 0, slim, wide }
    }

    /// Читает следующее слово. `false` — записи кончились.
    fn step(&mut self) -> bool {
        if self.at >= self.src.len() {
            return false;
        }
        let head = read_varint(self.src, &mut self.at);
        let (common, changed) = if self.slim { (head >> 1, head & 1 == 1) } else { (head, true) };
        let tail = read_varint(self.src, &mut self.at);
        if changed {
            if self.wide {
                if self.at >= self.src.len() {
                    return false;
                }
                self.field = read_varint(self.src, &mut self.at) as u32;
            } else {
                let Some(&fld) = self.src.get(self.at) else { return false };
                self.at += 1;
                self.field = u32::from(fld);
            }
        }
        self.word.truncate(common.min(self.word.len()));
        let end = (self.at + tail).min(self.src.len());
        self.word.extend_from_slice(&self.src[self.at..end]);
        self.at = end;
        true
    }
}

impl Dict {
    pub fn open(
        src: &crate::container::SegmentSource,
        terms: usize,
        layout: u32,
    ) -> std::io::Result<Dict> {
        Ok(Self::open_named(src, "dict", terms, layout))
    }

    /// Тот же словарь под другим именем файлов.
    ///
    /// Отсортированные строки с двоичным поиском нужны не только терминам:
    /// обратное отображение «идентификатор → номер» — та же задача, и второй
    /// реализации ей не нужно.
    pub fn open_named(
        src: &crate::container::SegmentSource,
        name: &str,
        terms: usize,
        layout: u32,
    ) -> Dict {
        let slim = layout >= 7;
        let wide = layout >= 8;
        let blob = src.try_open(&format!("{name}.blob")).unwrap_or_else(Region::empty);
        let anchors = src.try_open(&format!("{name}.idx")).unwrap_or_else(Region::empty);
        if terms > 0 && anchors.len_bytes() == terms * FLAT_ENTRY {
            return Dict { blob, anchors, len: terms, slim, wide, flat: true };
        }
        // Число слов считается один раз при загрузке проходом по последней
        // группе: хранить его отдельно незачем.
        let groups = anchors.len_bytes() / 4;
        let len = if groups == 0 {
            0
        } else {
            let mut w = Walk::new(
                blob.slice(0, blob.len_bytes()),
                Self::anchor_at(&anchors, groups - 1),
                slim,
                wide,
            );
            let mut n = 0usize;
            while n < ANCHOR && w.step() {
                n += 1;
            }
            (groups - 1) * ANCHOR + n
        };
        Dict { blob, anchors, len, slim, wide, flat: false }
    }

    /// Запись плоского словаря: поле, слово, номер списка.
    fn flat_at(&self, i: usize) -> (u32, &[u8], u32) {
        let e = self.anchors.slice(i * FLAT_ENTRY, FLAT_ENTRY);
        if e.len() < FLAT_ENTRY {
            return (u32::MAX, &[], 0);
        }
        let off = u32::from_le_bytes([e[0], e[1], e[2], e[3]]) as usize;
        let len = u16::from_le_bytes([e[4], e[5]]) as usize;
        (u32::from(e[6]), self.blob.slice(off, len), u32::from_le_bytes([e[7], e[8], e[9], e[10]]))
    }

    fn flat_lower_bound(&self, fld: u32, term: &[u8]) -> usize {
        let (mut lo, mut hi) = (0usize, self.len);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (f, t, _) = self.flat_at(mid);
            if (f, t) < (fld, term) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn anchor_at(anchors: &Region, i: usize) -> usize {
        let e = anchors.slice(i * 4, 4);
        if e.len() < 4 {
            return 0;
        }
        u32::from_le_bytes([e[0], e[1], e[2], e[3]]) as usize
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// Затягивает словарь в кеш страниц. Он маленький и нужен каждому запросу.
    pub fn warm(&self) -> usize {
        self.blob.warm() + self.anchors.warm()
    }

    /// Сколько байт он занимает на диске — для бюджета прогрева.
    pub fn bytes(&self) -> usize {
        self.blob.len_bytes() + self.anchors.len_bytes()
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn groups(&self) -> usize {
        self.anchors.len_bytes() / 4
    }

    /// Опорное слово группы `g` — оно записано целиком.
    fn anchor_key(&self, g: usize) -> (u32, Vec<u8>) {
        let src = self.blob.slice(0, self.blob.len_bytes());
        let mut w = Walk::new(src, Self::anchor_at(&self.anchors, g), self.slim, self.wide);
        if !w.step() {
            return (u32::MAX, Vec::new());
        }
        (w.field, w.word)
    }

    /// Группа, с которой может начинаться `(fld, term)`.
    fn group_of(&self, fld: u32, term: &[u8]) -> usize {
        let (mut lo, mut hi) = (0usize, self.groups());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (f, t) = self.anchor_key(mid);
            if (f, t.as_slice()) < (fld, term) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // Слово может лежать в хвосте предыдущей группы.
        lo.saturating_sub(1)
    }

    /// Номер списка по точному слову.
    pub fn lookup(&self, fld: u32, term: &str) -> Option<u32> {
        let needle = term.as_bytes();
        if self.flat {
            let i = self.flat_lower_bound(fld, needle);
            if i >= self.len {
                return None;
            }
            let (f, t, list) = self.flat_at(i);
            return if f == fld && t == needle { Some(list) } else { None };
        }
        let g = self.group_of(fld, needle);
        let src = self.blob.slice(0, self.blob.len_bytes());
        let mut w = Walk::new(src, Self::anchor_at(&self.anchors, g), self.slim, self.wide);
        let mut i = g * ANCHOR;
        // Группа плюс запас: искомое слово могло оказаться первым в следующей.
        while i < self.len && w.step() {
            match (w.field, w.word.as_slice()).cmp(&(fld, needle)) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Equal => return Some(i as u32),
                std::cmp::Ordering::Greater => return None,
            }
        }
        None
    }

    /// Номер первого списка с этим словом и сколько их лежит подряд.
    ///
    /// Словарь терминов повторов не знает — слово в нём одно, — а вот обратное
    /// отображение знает: один идентификатор стоит у стольких документов,
    /// сколько раз приём его положил. `lookup` отдаёт первый из них, и гашение
    /// по нему упирается в уже мёртвую копию, оставляя живую.
    ///
    /// Отдельным ходом, а не общим с `lookup`, намеренно: чтобы досчитать
    /// повторы, надо шагнуть за найденное слово, и на словаре терминов этот шаг
    /// оплачивался бы каждым словом каждого запроса ради случая, которого там не
    /// бывает.
    pub fn lookup_range(&self, fld: u32, term: &str) -> Option<(u32, u32)> {
        let needle = term.as_bytes();
        if self.flat {
            let at = self.flat_lower_bound(fld, needle);
            let mut n = 0usize;
            while at + n < self.len {
                let (f, t, _) = self.flat_at(at + n);
                if f != fld || t != needle {
                    break;
                }
                n += 1;
            }
            return (n > 0).then_some((at as u32, n as u32));
        }
        let g = self.group_of(fld, needle);
        let src = self.blob.slice(0, self.blob.len_bytes());
        let mut w = Walk::new(src, Self::anchor_at(&self.anchors, g), self.slim, self.wide);
        let mut i = g * ANCHOR;
        let mut first = None;
        let mut n = 0u32;
        while i < self.len && w.step() {
            match (w.field, w.word.as_slice()).cmp(&(fld, needle)) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Equal => {
                    first.get_or_insert(i as u32);
                    n += 1;
                    i += 1;
                }
                std::cmp::Ordering::Greater => break,
            }
        }
        first.map(|f| (f, n))
    }

    /// Последовательный проход по словарю.
    ///
    /// Отдельно от `term_at`, потому что тот у фронт-кодированного словаря
    /// каждый раз идёт от опоры: перебрать так весь словарь значит пройти
    /// каждую группу по шестьдесят четыре раза. Слиянию сегментов нужен именно
    /// проход, и он обязан быть одним.
    pub fn walk(&self) -> DictWalk<'_> {
        DictWalk {
            dict: self,
            walk: Walk::new(self.blob.slice(0, self.blob.len_bytes()), 0, self.slim, self.wide),
            at: 0,
        }
    }

    /// Слово по его порядковому номеру. Для сверки и разбора.
    pub fn term_at(&self, i: usize) -> (u32, String) {
        if self.flat {
            let (f, t, _) = self.flat_at(i);
            return (f, String::from_utf8_lossy(t).to_string());
        }
        let src = self.blob.slice(0, self.blob.len_bytes());
        let g = i / ANCHOR;
        let mut w = Walk::new(src, Self::anchor_at(&self.anchors, g), self.slim, self.wide);
        for _ in 0..=(i % ANCHOR) {
            if !w.step() {
                return (u32::MAX, String::new());
            }
        }
        (w.field, String::from_utf8_lossy(&w.word).to_string())
    }

    /// Устойчивый хеш слова по номеру списка без выделения строки.
    ///
    /// Номер списка действителен только внутри одного сегмента. Агрегация
    /// мощности складывает HLL-регистры разных сегментов, поэтому подавать ей
    /// сам номер нельзя: нулевой список одного сегмента может означать «алый»,
    /// а нулевой другого — «синий». Хеш текста одинаков во всех сегментах и не
    /// требует строить глобальные ординалы.
    pub fn hash_at(&self, i: usize) -> Option<(u32, u64)> {
        if i >= self.len {
            return None;
        }
        if self.flat {
            let (field, term, _) = self.flat_at(i);
            return Some((field, crate::bloom::hash64(term)));
        }
        let src = self.blob.slice(0, self.blob.len_bytes());
        let group = i / ANCHOR;
        let mut walk = Walk::new(src, Self::anchor_at(&self.anchors, group), self.slim, self.wide);
        for _ in 0..=(i % ANCHOR) {
            if !walk.step() {
                return None;
            }
        }
        Some((walk.field, crate::bloom::hash64(&walk.word)))
    }

    /// Хеши выбранных ординалов в порядке возрастания.
    ///
    /// Для фронт-кодированного словаря каждая затронутая группа читается одним
    /// последовательным проходом от опоры. Это сохраняет стоимость одного
    /// разбора на группу вместо повторного восстановления общего начала для
    /// каждого ординала. Ответ выровнен с `ordinals`; отсутствующий ординал
    /// обозначается `None`.
    pub fn hashes_at_sorted(&self, ordinals: &[usize]) -> Vec<Option<(u32, u64)>> {
        self.hashes_at_sorted_guarded(ordinals, None)
    }

    /// Диапазон local ordinals одного поля. Словарь упорядочен по
    /// (field, term), поэтому достаточно двух двоичных поисков, без обхода
    /// всех слов или декодирования колонок документов.
    pub fn field_ordinals(&self, field: u32) -> std::ops::Range<usize> {
        self.field_ordinals_guarded(field, None).expect("без guard отмена невозможна")
    }

    pub fn field_ordinals_guarded(
        &self,
        field: u32,
        guard: Option<&crate::work::Guard>,
    ) -> Option<std::ops::Range<usize>> {
        let bound = |after: bool| {
            let (mut lo, mut hi) = (0, self.len);
            while lo < hi {
                if guard.is_some_and(|g| !g.check()) {
                    return None;
                }
                let mid = lo + (hi - lo) / 2;
                let seen = self.term_at(mid).0;
                if seen < field || (after && seen == field) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            Some(lo)
        };
        Some(bound(false)?..bound(true)?)
    }

    pub fn hashes_at_sorted_guarded(
        &self,
        ordinals: &[usize],
        guard: Option<&crate::work::Guard>,
    ) -> Vec<Option<(u32, u64)>> {
        self.hashes_at_sorted_while(ordinals, || guard.is_none_or(|g| g.check()))
    }

    /// Единая граница порции для production guard и детерминированной
    /// проверки отмены уже начатого прохода.
    fn hashes_at_sorted_while(
        &self,
        ordinals: &[usize],
        mut proceed: impl FnMut() -> bool,
    ) -> Vec<Option<(u32, u64)>> {
        let mut out = vec![None; ordinals.len()];
        if self.flat {
            for (slot, &ordinal) in ordinals.iter().enumerate() {
                if slot.is_multiple_of(64) && !proceed() {
                    break;
                }
                if ordinal < self.len {
                    let (field, term, _) = self.flat_at(ordinal);
                    out[slot] = Some((field, crate::bloom::hash64(term)));
                }
            }
            return out;
        }
        let src = self.blob.slice(0, self.blob.len_bytes());
        let mut cursor = 0usize;
        while cursor < ordinals.len() {
            if !proceed() {
                break;
            }
            if ordinals[cursor] >= self.len {
                break;
            }
            let group = ordinals[cursor] / ANCHOR;
            let mut walk =
                Walk::new(src, Self::anchor_at(&self.anchors, group), self.slim, self.wide);
            let end = ordinals[cursor..]
                .iter()
                .position(|&ordinal| ordinal / ANCHOR != group)
                .map_or(ordinals.len(), |offset| cursor + offset);
            let mut next_ordinal = group * ANCHOR;
            for target in cursor..end {
                let wanted = ordinals[target];
                if wanted >= self.len {
                    break;
                }
                if target > cursor && wanted == ordinals[target - 1] {
                    out[target] = out[target - 1];
                    continue;
                }
                // После step курсор указывает на следующий ordinal; word —
                // только что прочитанное слово. Эти два состояния нельзя
                // смешивать: иначе соседние цели получают чужой hash.
                while next_ordinal <= wanted {
                    if !walk.step() {
                        return out;
                    }
                    next_ordinal += 1;
                }
                out[target] = Some((walk.field, crate::bloom::hash64(&walk.word)));
            }
            cursor = end;
        }
        out
    }

    /// Списки всех слов поля с данным началом — не больше `cap`.
    ///
    /// Ровно то, чего не умела хеш-таблица: переход к началу диапазона и проход
    /// вперёд. Фронт-кодированию этот запрос подходит лучше всего — проход
    /// вперёд и есть его естественный ход.
    pub fn by_prefix(&self, fld: u32, prefix: &str, cap: usize) -> Vec<u32> {
        self.by_prefix_terms(fld, prefix, cap).into_iter().map(|(_, list)| list).collect()
    }

    /// Слова и списки одного prefix-диапазона — для глобального expansion
    /// budget. Обычному поиску строки не нужны, поэтому прежний [`by_prefix`]
    /// оставляет только номера; глобальному бюджету строка нужна, чтобы одна и
    /// та же форма в разных сегментах считалась принятой один раз.
    pub fn by_prefix_terms(&self, fld: u32, prefix: &str, cap: usize) -> Vec<(String, u32)> {
        let mut out = Vec::new();
        if cap == 0 {
            return out;
        }
        let needle = prefix.as_bytes();
        if self.flat {
            let mut i = self.flat_lower_bound(fld, needle);
            while i < self.len && out.len() < cap {
                let (f, t, list) = self.flat_at(i);
                if f != fld || !t.starts_with(needle) {
                    break;
                }
                out.push((String::from_utf8_lossy(t).into_owned(), list));
                i += 1;
            }
            return out;
        }
        let g = self.group_of(fld, needle);
        let src = self.blob.slice(0, self.blob.len_bytes());
        let mut w = Walk::new(src, Self::anchor_at(&self.anchors, g), self.slim, self.wide);
        let mut i = g * ANCHOR;

        while i < self.len && w.step() {
            let key = (w.field, w.word.as_slice());
            if key < (fld, needle) {
                i += 1;
                continue;
            }
            if w.field != fld || !w.word.starts_with(needle) {
                break;
            }
            out.push((String::from_utf8_lossy(&w.word).into_owned(), i as u32));
            if out.len() >= cap {
                break;
            }
            i += 1;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn build(name: &str, keys: &[(u32, String)]) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-dict-{name}"));
        std::fs::create_dir_all(&dir).unwrap();
        let (blob, anchors) = encode(keys, "проверка/dict.blob").expect("влезло");
        std::fs::File::create(dir.join("dict.blob")).unwrap().write_all(&blob).unwrap();
        std::fs::File::create(dir.join("dict.idx")).unwrap().write_all(&anchors).unwrap();
        dir.to_string_lossy().to_string()
    }

    fn sorted(pairs: &[(u32, &str)]) -> Vec<(u32, String)> {
        let mut v: Vec<(u32, String)> = pairs.iter().map(|(f, t)| (*f, t.to_string())).collect();
        v.sort();
        v
    }

    fn sample() -> Vec<(u32, String)> {
        sorted(&[
            (0, "дом"),
            (0, "домашний"),
            (0, "домик"),
            (0, "дорога"),
            (1, "дом"),
            (1, "домна"),
            (1, "ель"),
        ])
    }

    #[test]
    fn finds_exact_terms() {
        let keys = sample();
        let dir = build("exact", &keys);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();
        assert_eq!(d.len(), keys.len());
        for (i, (fld, term)) in keys.iter().enumerate() {
            assert_eq!(d.lookup(*fld, term), Some(i as u32), "{fld}:{term}");
        }
        assert!(d.lookup(1, "дорога").is_none());
        assert!(d.lookup(0, "ель").is_none());
        assert!(d.lookup(0, "домо").is_none());
        assert!(d.lookup(0, "а").is_none());
        assert!(d.lookup(9, "дом").is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn prefix_stays_inside_one_field() {
        let keys = sample();
        let dir = build("prefix", &keys);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();

        let want: Vec<u32> = keys
            .iter()
            .enumerate()
            .filter(|(_, (f, t))| *f == 0 && t.starts_with("дом"))
            .map(|(i, _)| i as u32)
            .collect();
        assert_eq!(d.by_prefix(0, "дом", DEFAULT_EXPANSIONS), want);
        assert_eq!(d.by_prefix(1, "дом", DEFAULT_EXPANSIONS).len(), 2);
        assert!(d.by_prefix(0, "щ", DEFAULT_EXPANSIONS).is_empty());
        assert_eq!(d.by_prefix(0, "дом", 2).len(), 2);
        assert_eq!(d.by_prefix(0, "дом", 0).len(), 0);
        assert_eq!(d.by_prefix(0, "", DEFAULT_EXPANSIONS).len(), 4);
        assert_eq!(d.by_prefix(1, "", DEFAULT_EXPANSIONS).len(), 3);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Словарь длиннее одной опорной группы — там и живут все краевые случаи.
    #[test]
    fn spans_many_anchor_groups() {
        let mut pairs: Vec<(u32, String)> = Vec::new();
        for i in 0..500 {
            pairs.push((0, format!("слово{i:04}")));
            pairs.push((1, format!("термин{i:04}")));
        }
        pairs.sort();
        let dir = build("groups", &pairs);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();
        assert_eq!(d.len(), pairs.len());
        for (i, (fld, term)) in pairs.iter().enumerate() {
            assert_eq!(d.lookup(*fld, term), Some(i as u32), "слово {i}");
        }
        // Маска на границе групп обязана собрать все формы, а не полгруппы.
        let got = d.by_prefix(0, "слово00", 1000);
        assert_eq!(got.len(), 100, "слово0000..слово0099");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Слова без общего начала и слова, где одно — начало другого.
    #[test]
    fn awkward_neighbours() {
        let keys = sorted(&[(0, "a"), (0, "ab"), (0, "abc"), (0, "b"), (0, "ba"), (0, "z")]);
        let dir = build("awkward", &keys);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();
        for (i, (fld, term)) in keys.iter().enumerate() {
            assert_eq!(d.lookup(*fld, term), Some(i as u32), "{term}");
        }
        assert_eq!(d.by_prefix(0, "a", 10).len(), 3);
        assert_eq!(d.by_prefix(0, "ab", 10).len(), 2);
        assert_eq!(d.by_prefix(0, "b", 10).len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_dictionary() {
        let dir = build("empty", &[]);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();
        assert!(d.is_empty());
        assert_eq!(d.field_ordinals(0), 0..0);
        assert!(d.lookup(0, "что-нибудь").is_none());
        assert!(d.by_prefix(0, "ч", 10).is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn ordinal_range_handles_a_missing_middle_field_and_cancel() {
        let dir = build("field-range-gap", &[(0, "a".into()), (2, "b".into())]);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();
        assert_eq!(d.field_ordinals(0), 0..1);
        assert_eq!(d.field_ordinals(1), 1..1);
        assert_eq!(d.field_ordinals(2), 1..2);
        let guard = crate::work::Guard::new(0);
        guard.cancel();
        assert!(d.field_ordinals_guarded(1, Some(&guard)).is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Пакетный проход обязан совпадать с независимым чтением каждого
    /// ординала, включая повторы, смену поля и отсутствующие номера.
    #[test]
    fn sorted_hashes_match_individual_lookup_across_anchor_groups() {
        let mut keys: Vec<(u32, String)> =
            (0..700u32).flat_map(|i| [(0, format!("a{i:04}")), (1, format!("b{i:04}"))]).collect();
        keys.sort();
        let dir = build("sorted-hashes", &keys);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();
        let mut ordinals = vec![0, 63, 64, 65, 127, 128, 699, 700, usize::MAX, 128];
        ordinals.sort_unstable();
        let got = d.hashes_at_sorted(&ordinals);
        let want: Vec<_> = ordinals.iter().map(|&i| d.hash_at(i)).collect();
        assert_eq!(got, want, "пакетный хеш разошёлся с одиночным чтением");
        assert_eq!(d.field_ordinals(0), 0..700);
        assert_eq!(d.field_ordinals(1), 700..1400);
        assert_eq!(d.field_ordinals(2), 1400..1400);
        assert_eq!(d.field_ordinals(u32::MAX), 1400..1400);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отмена должна обрываться внутри реального прохода по словарю, оставляя
    /// хвост невыданным, а не проверяться только до входа в функцию.
    #[test]
    fn sorted_hashes_guard_stops_an_active_dictionary_chunk() {
        let keys: Vec<(u32, String)> = (0..512u32).map(|i| (0, format!("term{i:07}"))).collect();
        let dir = build("sorted-hashes-cancel", &keys);
        let d = Dict::open(&crate::container::loose(&dir), 0, crate::index::LAYOUT).unwrap();
        let ordinals: Vec<usize> = (0..d.len()).collect();
        let guard = crate::work::Guard::new(0);
        let mut chunks = 0;
        let got = d.hashes_at_sorted_while(&ordinals, || {
            chunks += 1;
            if chunks == 4 {
                guard.cancel();
            }
            guard.check()
        });
        let present = got.iter().filter(|v| v.is_some()).count();
        assert!(
            present > 0 && present < ordinals.len(),
            "проход не был прерван внутри чанка: {present}"
        );
        assert!(guard.fired(), "сторож не зафиксировал отмену");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Фронт-кодирование обязано действительно экономить, а не просто работать.
    #[test]
    fn front_coding_actually_saves() {
        let keys: Vec<(u32, String)> =
            (0..5_000).map(|i| (0u32, format!("префикс_общий_{i:07}"))).collect();
        let plain: usize = keys.iter().map(|(_, t)| t.len() + 11).sum();
        let (blob, anchors) = encode(&keys, "проверка/dict.blob").expect("влезло");
        let coded = blob.len() + anchors.len();
        assert!(coded * 4 < plain, "фронт-кодирование {coded} против {plain}");
    }

    /// Потолок опоры словаря: `u32::MAX` ещё влезает, на байт больше — уже нет.
    ///
    /// ## Что ломалось бы без отказа
    ///
    /// `encode` клал бы `blob.len() as u32` завёрнутым, и опора в `dict.idx`
    /// указывала бы в середину чужого слова того же файла. Двоичный поиск после
    /// этого сходится не туда: слово либо перестаёт находиться, либо находится
    /// чужое — без отказа, без паники и без единого признака порчи. Ни один
    /// размер, ни одна контрольная сумма этого не видят.
    ///
    /// Словарь в четыре гигабайта в тесте не собрать, поэтому проверяется само
    /// правило и его края. Край несущий: `u32::MAX` — законное смещение, и
    /// читатель его принимает; сдвинь границу на единицу вниз — приём начнёт
    /// отказывать сегментам, которые открываются.
    #[test]
    fn the_dictionary_anchor_ceiling_is_inclusive_of_u32_max() {
        assert!(fits_anchor(0, "seg0007/dict.blob", 1).is_ok(), "пустой словарь влезает");
        assert!(
            fits_anchor(OFFSET_CEILING as usize, "seg0007/dict.blob", 64).is_ok(),
            "потолок обязан быть самим u32::MAX"
        );

        let e = fits_anchor(OFFSET_CEILING as usize + 1, "seg0007/dict.blob", 65)
            .expect_err("на байт за потолком обязан быть отказ");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        // Отказ обязан назвать файл, достигнутый размер, потолок и выход: без
        // этого оператор не поймёт, что именно уменьшать.
        assert!(msg.contains("seg0007/dict.blob"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("4294967296"), "отказ не назвал достигнутое: {msg}");
        assert!(msg.contains("4294967295"), "отказ не назвал потолок: {msg}");
        assert!(msg.contains("65-м слове"), "отказ не назвал место: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход приёму: {msg}");
        assert!(msg.contains("target_docs"), "отказ не подсказал выход укрупнению: {msg}");
    }

    /// Охрана стоит **на пути записи**, а не рядом с ним.
    ///
    /// ## Чем это отличается от проверки выше
    ///
    /// Выше проверен предикат: точные числа, оба края. Но предикат, который
    /// никто не зовёт, ведёт себя ровно так же — и снятие строки
    /// `fits_anchor(...)` из `encode` не роняло ни одной проверки. Здесь
    /// проверяется именно вызов: потолок опущен до сотни байт, вход обычный и
    /// маленький, и `encode` обязан отказать до того, как усечённая опора ляжет
    /// в `anchors`. Сними охрану — словарь соберётся, и проверка упадёт.
    #[test]
    fn the_anchor_guard_stands_on_the_write_path() {
        // Три опоры: первая приходится на пустой ещё `blob` и влезает при любом
        // потолке, а вот вторая и третья — уже нет.
        let mut keys: Vec<(u32, String)> =
            (0..3 * ANCHOR).map(|i| (0u32, format!("слово_{i:07}"))).collect();
        keys.sort();

        // При настоящем потолке тот же вход обязан собираться: иначе проверка
        // ловила бы не охрану, а любую другую поломку кодировщика.
        let (blob, _) = encode(&keys, "seg0007/dict.blob").expect("обычный словарь собирается");
        assert!(blob.len() > 100, "вход обязан перерасти опущенный потолок: {}", blob.len());

        let _low = lower_offset_ceiling(100);
        // Разбор `match`, а не `expect_err`: тот при удаче вывалил бы в отчёт
        // весь словарь целиком, и настоящую причину пришлось бы искать глазами.
        let e = match encode(&keys, "seg0007/dict.blob") {
            Ok(_) => panic!("охрана не на пути записи: опора за потолком легла в словарь"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("seg0007/dict.blob"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("при потолке 100"), "отказ померил не тем потолком: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход: {msg}");
    }

    /// Опущенный потолок возвращается на место, а не течёт в соседнюю проверку.
    ///
    /// libtest переиспользует потоки, и оставленный на полу потолок отказал бы
    /// следующей проверке отказом, которого она не ждёт. Порядок проверок при
    /// этом не задан, поэтому течь ловится здесь, а не тем, что где-то упало.
    #[test]
    fn a_lowered_ceiling_is_put_back() {
        assert_eq!(offset_ceiling(), OFFSET_CEILING, "потолок начинается настоящим");
        {
            let _low = lower_offset_ceiling(100);
            assert_eq!(offset_ceiling(), 100, "потолок не опустился");
            let _deeper = lower_offset_ceiling(7);
            assert_eq!(offset_ceiling(), 7, "вложенное опускание не сработало");
        }
        assert_eq!(offset_ceiling(), OFFSET_CEILING, "потолок остался опущенным");
    }

    /// Поток и накопление дают **один и тот же** словарь, байт в байт.
    ///
    /// ## Почему это несущая проверка
    ///
    /// После правки словарь слитого сегмента пишет [`Writer`], а словарь
    /// приёмного — по-прежнему [`encode`]. Читатель у них один. Разойдись они
    /// хоть на байт — двоичный поиск по слитому сегменту сходился бы не туда, и
    /// не отказом, а чужим словом на выдаче.
    ///
    /// Вход нарочно **длиннее буфера записи**, и не для красоты: опора — это
    /// смещение в файле, а буфер за время сборки опустошается много раз. Пока
    /// словарь меньше буфера, «смещение в файле» и «смещение в буфере» — одно и
    /// то же число, и подмена одного другим проходит незамеченной.
    #[test]
    fn the_streaming_writer_matches_the_accumulating_encoder() {
        let mut keys: Vec<(u32, String)> = Vec::new();
        // Три поля вперемешку, длинные общие начала, слова разной длины —
        // задеты все ветви кодирования: смена поля, опора, общее начало.
        for i in 0..60_000u32 {
            // Общее начало длинное — фронт-кодирование работает в полную силу.
            keys.push((0, format!("слово_общее_начало_{i:09}")));
            keys.push((1, format!("t{i}")));
            // А здесь его почти нет: цифры перевёрнуты, и соседи расходятся с
            // первого же байта. Без такой семьи словарь ужимается втрое и до
            // сброса буфера дело не доходит.
            let back: String = format!("{i:07}").chars().rev().collect();
            keys.push((7, format!("{back}-ключ-подлиннее-{i}")));
        }
        keys.sort();
        let (want_blob, want_anchors) = encode(&keys, "проверка/dict.blob").expect("влезло");
        assert!(
            want_blob.len() > FLUSH * 2,
            "вход обязан перерасти буфер записи, иначе смещение в файле и смещение \
             в буфере — одно и то же число: {}",
            want_blob.len()
        );

        let dir = std::env::temp_dir().join("svindb-dict-stream");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dict.blob").to_string_lossy().to_string();
        let mut w = Writer::create(&path).expect("файл заведён");
        for (f, t) in keys.iter() {
            w.push(*f, t.as_bytes()).expect("слово легло");
        }
        let got_anchors = w.finish().expect("поток закрыт");
        let got_blob = std::fs::read(&path).expect("словарь прочитан");

        assert_eq!(got_blob.len(), want_blob.len(), "длина словаря разошлась");
        assert_eq!(got_blob, want_blob, "содержимое словаря разошлось");
        assert_eq!(got_anchors, want_anchors, "опоры разошлись");

        // И читается он тем же разбором: побайтовое равенство это доказывает,
        // но поиск проверяет ещё и то, что равны они не по случайности.
        std::fs::write(dir.join("dict.idx"), &got_anchors).unwrap();
        let d = Dict::open(
            &crate::container::loose(&dir.to_string_lossy()),
            keys.len(),
            crate::index::LAYOUT,
        )
        .expect("словарь открылся");
        for at in [0usize, 1, 63, 64, keys.len() / 2, keys.len() - 1] {
            let (f, t) = &keys[at];
            assert_eq!(d.lookup(*f, t), Some(at as u32), "поле {f}, слово {t}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Охрана потолка стоит на пути **потоковой** записи, а не только у `encode`.
    ///
    /// Проверка выше ловит охрану у накопительного пути. У потока она своя — он
    /// считает смещение от начала файла, а не от длины буфера, — и вычеркнуть её
    /// можно отдельно. Тогда завёрнутая опора легла бы в `dict.idx` слитого
    /// сегмента, и словарь читался бы вразнос.
    #[test]
    fn the_streaming_anchor_guard_stands_on_the_write_path() {
        let dir = std::env::temp_dir().join("svindb-dict-stream-guard");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dict.blob").to_string_lossy().to_string();
        let keys: Vec<(u32, String)> =
            (0..3 * ANCHOR).map(|i| (0u32, format!("слово_{i:07}"))).collect();

        // Сперва при настоящем потолке: иначе проверку закрыл бы любой другой
        // отказ записи.
        let mut ok = Writer::create(&path).expect("файл заведён");
        for (f, t) in keys.iter() {
            ok.push(*f, t.as_bytes()).expect("слово легло");
        }
        ok.finish().expect("обычный словарь пишется потоком");
        assert!(
            std::fs::metadata(&path).expect("словарь").len() > 100,
            "вход обязан перерасти опущенный потолок"
        );

        let _low = lower_offset_ceiling(100);
        let mut w = Writer::create(&path).expect("файл заведён");
        let e = match keys.iter().try_for_each(|(f, t)| w.push(*f, t.as_bytes())) {
            Ok(()) => panic!("охрана не на пути записи: опора за потолком легла в поток"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("dict.blob"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("при потолке 100"), "отказ померил не тем потолком: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход: {msg}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Обычный словарь отказа не даёт: правило не должно мешать работе.
    #[test]
    fn an_ordinary_dictionary_is_never_refused() {
        let keys: Vec<(u32, String)> =
            (0..5_000).map(|i| (i % 3, format!("слово_{i:07}"))).collect();
        let mut keys = keys;
        keys.sort();
        let (blob, anchors) =
            encode(&keys, "seg0007/dict.blob").expect("обычный словарь обязан собраться");
        assert_eq!(anchors.len(), keys.len().div_ceil(ANCHOR) * 4, "опор не столько");
        assert!(!blob.is_empty());
    }
}

#[cfg(test)]
mod tests_real_shape {
    use super::*;
    use std::io::Write;

    /// Два поля и словарь того же вида, что даёт настоящий корпус.
    ///
    /// Здесь важна не длина, а форма: слова обоих полей вперемешку по общему
    /// порядку, длинные общие начала, границы полей внутри опорных групп.
    #[test]
    fn two_fields_with_shared_prefixes() {
        let mut keys: Vec<(u32, String)> = Vec::new();
        for i in 0..3_000u32 {
            keys.push((0, format!("t0_{i}")));
            keys.push((1, format!("t1_{i}")));
        }
        keys.sort();

        let dir = std::env::temp_dir().join("svindb-dict-realshape");
        std::fs::create_dir_all(&dir).unwrap();
        let (blob, anchors) = encode(&keys, "проверка/dict.blob").expect("влезло");
        std::fs::File::create(dir.join("dict.blob")).unwrap().write_all(&blob).unwrap();
        std::fs::File::create(dir.join("dict.idx")).unwrap().write_all(&anchors).unwrap();
        let path = dir.to_string_lossy().to_string();

        let d = Dict::open(&crate::container::loose(&path), 0, crate::index::LAYOUT).unwrap();
        assert_eq!(d.len(), keys.len(), "число слов");
        for (i, (fld, term)) in keys.iter().enumerate() {
            assert_eq!(d.lookup(*fld, term), Some(i as u32), "{fld}:{term} на месте {i}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Проход по словарю от начала: поле, слово, номер списка.
///
/// Не `Iterator`: слово отдаётся ссылкой на буфер, который переиспользуется, а
/// выдавать ссылку на своё нутро стандартный трейт не позволяет. Заводить на
/// каждое из полумиллиона слов по строке ради формальности незачем.
pub struct DictWalk<'a> {
    dict: &'a Dict,
    walk: Walk<'a>,
    at: usize,
}

impl<'a> DictWalk<'a> {
    /// Следующее слово. `None` — словарь кончился.
    // Не `Iterator`: обход умеет прыгать (`seek`) и отдаёт заимствование,
    // связанное с самим обходчиком. Ни то ни другое в `Iterator` не
    // выражается, а притворяться им — обещать `map`/`collect`, которые
    // на прыжках дали бы неверный ответ.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(u32, &[u8], u32)> {
        if self.at >= self.dict.len {
            return None;
        }
        if self.dict.flat {
            let (f, t, list) = self.dict.flat_at(self.at);
            self.at += 1;
            return Some((f, t, list));
        }
        if !self.walk.step() {
            return None;
        }
        let list = self.at as u32;
        self.at += 1;
        Some((self.walk.field, &self.walk.word, list))
    }
}
