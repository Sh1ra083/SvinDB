//! Упаковщик хранилища: LZ77 с большим окном плюс Хаффман на обученных
//! таблицах.

use super::huff::{self, BitReader, BitWriter, Decoder};

/// Окно: насколько далеко назад достаёт ссылка.
pub const WINDOW: usize = 1 << 20;
/// Короче этого ссылка занимает больше, чем экономит.
pub const MIN_MATCH: usize = 4;
/// Наибольшее совпадение. Длинные совпадения в словарь — то, ради чего он и
/// нужен, поэтому потолок в 258 байт у DEFLATE здесь не годится.
pub const MAX_MATCH: usize = 2050;

/// Символ конца куска.
const END: u16 = 256;
/// Литералов 256, конец 1, длин 40.
pub const NLIT: usize = 297;
/// Расстояний: 4 коротких плюс восемнадцать пар — до 2²⁰.
pub const NDIST: usize = 40;

const HASH_BITS: u32 = 18;

/// Ширина таблицы цепочек **куска**, а не словаря.
const SRC_HASH_BITS: u32 = 14;

/// База и число добавочных бит для кода длины.
///
/// Первые восемь длин кодируются точно, дальше — четвёрками с удвоением шага.
/// Тот же порядок, что в RFC 1951, продолженный за 258 байт.
#[inline]
fn len_code(code: usize) -> (usize, u32) {
    if code < 8 {
        return (MIN_MATCH + code, 0);
    }
    let g = (code - 8) / 4 + 1;
    let j = (code - 8) % 4;
    // База группы: 11, 19, 35, 67, … — каждая следующая на 4·2^g дальше.
    let mut base = MIN_MATCH + 8;
    for k in 1..g {
        base += 4 << k;
    }
    (base + (j << g), g as u32)
}

/// Обратно: длина в код и добавочные биты.
#[inline]
fn len_of(len: usize) -> (usize, u32, u32) {
    debug_assert!((MIN_MATCH..=MAX_MATCH).contains(&len));
    if len < MIN_MATCH + 8 {
        return (len - MIN_MATCH, 0, 0);
    }
    let mut base = MIN_MATCH + 8;
    let mut g = 1usize;
    loop {
        let span = 4 << g;
        if len < base + span || g >= 8 {
            let j = (len - base) >> g;
            let j = j.min(3);
            let code = 8 + (g - 1) * 4 + j;
            let (b, e) = len_code(code);
            return (code, (len - b) as u32, e);
        }
        base += span;
        g += 1;
    }
}

/// База и число добавочных бит для кода расстояния.
#[inline]
fn dist_code(code: usize) -> (usize, u32) {
    if code < 4 {
        return (1 + code, 0);
    }
    let g = (code - 4) / 2 + 1;
    let j = (code - 4) % 2;
    (1 + (1 << (g + 1)) + (j << g), g as u32)
}

#[inline]
fn dist_of(dist: usize) -> (usize, u32, u32) {
    debug_assert!((1..=WINDOW).contains(&dist));
    if dist <= 4 {
        return (dist - 1, 0, 0);
    }
    let mut g = 1usize;
    while (1 << (g + 2)) < dist {
        g += 1;
    }
    let base = 1 + (1 << (g + 1));
    let j = (dist - base) >> g;
    let code = 4 + (g - 1) * 2 + j.min(1);
    let (b, e) = dist_code(code);
    (code, (dist - b) as u32, e)
}

#[inline(always)]
fn hash4(v: u32) -> usize {
    (v.wrapping_mul(2_654_435_761) >> (32 - HASH_BITS)) as usize
}

/// Тот же хеш, но для таблицы куска: берётся меньше старших бит.
#[inline(always)]
fn hash_src(v: u32) -> usize {
    (v.wrapping_mul(2_654_435_761) >> (32 - SRC_HASH_BITS)) as usize
}

#[inline(always)]
fn read4(s: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([s[at], s[at + 1], s[at + 2], s[at + 3]])
}

/// Насколько тщательно искать совпадения.
///
/// Приём пишет быстро — от этого зависит свежесть. Укрупнение бывает раз в жизни
/// документа, и там можно платить процессором: выигранный байт остаётся
/// выигранным навсегда, а для SSD со ста перезаписями непереписанный байт стоит
/// дороже потраченного такта.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Effort {
    /// Приём: короткие цепочки, без оглядки.
    Fast,
    /// Укрупнение: длинные цепочки и ленивое совпадение.
    Thorough,
}

impl Effort {
    fn chain(self) -> usize {
        match self {
            Effort::Fast => 24,
            Effort::Thorough => 512,
        }
    }
    fn lazy(self) -> bool {
        self == Effort::Thorough
    }
}

/// Откуда взяты байты словаря.
///
/// Два случая, и они разной природы. У поиска словарь лежит в **отображённом**
/// файле: копировать его на кучу незачем — страницами распоряжается ядро, а
/// копия удваивает то, что и так есть. У сборки словарь ещё только строится, и
/// своих байтов ему не миновать.
///
/// ## Почему отображение под `Arc`, а не своё у каждого словаря
///
/// В одном `<поле>.sdic` записей теперь бывает много — по одной на источник
/// слияния. Держи каждая своё `Mapped`, и `K` словарей означали бы `K` вызовов
/// `mmap` на **один и тот же** файл, то есть `K` областей отображения у ядра.
/// Сегмент и без того стоит два десятка областей при потолке `vm.max_map_count`
/// в 65530, и умножать это на число слитых источников значит упереться в потолок
/// не на тысячах сегментов, а на сотнях. Общая ссылка снимает вопрос целиком:
/// область одна, сколько бы записей в ней ни лежало.
enum Bytes {
    /// Кусок отображённого файла. На куче не лежит ничего.
    Mapped(crate::mmap::Region),
    /// Свои байты — там, где словарь порождается, а не читается.
    Owned(Vec<u8>),
}

impl Bytes {
    #[inline(always)]
    fn get(&self) -> &[u8] {
        match self {
            Bytes::Mapped(r) => r.slice(0, r.len_bytes()),
            Bytes::Owned(v) => v,
        }
    }
}

/// Что нужно **распаковке**: байты словаря и две таблицы декодирования.
///
/// ## Почему отдельно от упаковки
///
/// Разделение здесь не украшение раскладки, а ровно то, что убирает эту память:
/// поисковый узел больше не может построить состояние упаковщика, потому что
/// такого типа у него нет.
pub struct DecoderDict {
    bytes: Bytes,
    lit_dec: Decoder,
    dist_dec: Decoder,
}

/// Что нужно **упаковке**: всё вышеперечисленное плюс цепочки и коды.
///
/// Живёт только в сборке и слиянии — там, где словарь порождается. Владеет
/// распаковщиком, а не наследует его: упаковщику распаковка нужна для обучения,
/// а обратное неверно.
pub struct EncoderDict {
    dec: DecoderDict,
    /// Цепочки совпадений по словарю: голова корзины и предыдущее вхождение.
    head: Vec<u32>,
    prev: Vec<u32>,
    lit_code: Vec<u16>,
    dist_code: Vec<u16>,
    /// Длины кодов: нужны и записи, и выкладке словаря на диск.
    lit_len: Vec<u8>,
    dist_len: Vec<u8>,
}

impl DecoderDict {
    /// Словарь из отображённого куска целиком. Байты не копируются.
    pub fn open(map: crate::mmap::Region) -> Option<DecoderDict> {
        let len = map.len_bytes();
        Self::open_at(&map, 0, len)
    }

    /// Словарь из **записи** внутри отображённого файла.
    ///
    /// Записи в `<поле>.sdic` лежат подряд, и какая где — говорит таблица
    /// словарей `<поле>.sdix`. Кусок передаётся ссылкой и делится дальше
    /// [`Region::sub`](crate::mmap::Region::sub): у всех записей одного поля
    /// отображение обязано быть одним, иначе число областей у ядра умножается на
    /// число словарей.
    ///
    /// Запись обязана кончиться ровно там, где обещано. Без этой сверки чужое
    /// смещение в таблице читалось бы как правдоподобная запись — разбор идёт от
    /// начала среза и сам по себе о конце ничего не знает.
    pub fn open_at(map: &crate::mmap::Region, at: usize, len: usize) -> Option<DecoderDict> {
        let rec = map.slice(at, len);
        if rec.len() != len {
            return None;
        }
        let r = split(rec)?;
        if r.end != len {
            return None;
        }
        Some(DecoderDict {
            lit_dec: Decoder::new(&r.lit_len)?,
            dist_dec: Decoder::new(&r.dist_len)?,
            bytes: Bytes::Mapped(map.sub(at + r.at, r.len)),
        })
    }

    /// Словарь из своих байтов. Нужен там, где отображать нечего.
    pub fn from_bytes(src: &[u8]) -> Option<DecoderDict> {
        let r = split(src)?;
        Some(DecoderDict {
            lit_dec: Decoder::new(&r.lit_len)?,
            dist_dec: Decoder::new(&r.dist_len)?,
            bytes: Bytes::Owned(src.get(r.at..r.at + r.len)?.to_vec()),
        })
    }

    pub fn len(&self) -> usize {
        self.bytes.get().len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.get().is_empty()
    }
}

/// Разобранная запись словаря.
///
/// Границы, а не срез: вызывающий сам решает, копировать байты или оставить их в
/// отображении. Конец записи назван отдельно — записей в файле бывает несколько
/// подряд, и «где кончилась эта» перестало быть тем же, что «где кончился файл».
struct Record {
    /// Смещение байтов словаря внутри записи.
    at: usize,
    len: usize,
    /// Где запись кончается — то есть где начинается следующая.
    end: usize,
    lit_len: Vec<u8>,
    dist_len: Vec<u8>,
}

/// Годится ли срез как **целая** запись словаря: разбирается и кончается ровно
/// на своей границе.
///
/// Строение записей сверяется отдельно от построения таблиц распаковки: таблицы
/// стоят по тридцать два килобайта и строятся лениво, а сверить надо все записи
/// сразу — иначе испорченный словарь перестал бы быть отказом на открытии и стал
/// бы пустой выдачей на каждом документе.
pub fn record_ok(src: &[u8]) -> bool {
    split(src).is_some_and(|r| r.end == src.len())
}

/// Разбирает выложенный словарь: где лежат байты и каковы длины кодов.
fn split(src: &[u8]) -> Option<Record> {
    let mut at = 0usize;
    let u32_at = |at: &mut usize| -> Option<usize> {
        let v = u32::from_le_bytes(src.get(*at..*at + 4)?.try_into().ok()?) as usize;
        *at += 4;
        Some(v)
    };
    let n = u32_at(&mut at)?;
    // Границы проверяются до срезов: длина с диска, и негодная означала бы
    // чтение за концом отображения.
    if src.len() < at + n {
        return None;
    }
    let (bytes_at, bytes_len) = (at, n);
    at += n;
    let ln = u32_at(&mut at)?;
    let lit_len = src.get(at..at + ln)?.to_vec();
    at += ln;
    let dn = u32_at(&mut at)?;
    let dist_len = src.get(at..at + dn)?.to_vec();
    at += dn;
    if lit_len.len() != NLIT || dist_len.len() != NDIST {
        return None;
    }
    Some(Record { at: bytes_at, len: bytes_len, end: at, lit_len, dist_len })
}

impl DecoderDict {
    /// Сколько резервировать под распаковку куска.
    ///
    /// Не `expect`, потому что `expect` приходит из `<поле>.smrk`, то есть с
    /// диска, и резерв делается **до** разбора первого бита. Контрольных сумм у
    /// файлов сегмента нет, поэтому испорченные четыре байта — это запрос
    /// памяти на сколько угодно: `0xFFFFFFFF` даёт почти четыре гигабайта на
    /// каждую выдачу любого документа этого куска. Под `ulimit -v` тот же
    /// запрос — отказ аллокатора, то есть `abort` всего узла вместо `None` на
    /// одном куске.
    ///
    /// Больше `MAX_MATCH` байт на бит входа распаковка выдать не может — это
    /// граница по построению кодека, а не догадка. На годных данных она в тысячи
    /// раз выше настоящей степени сжатия, поэтому здесь всегда берётся `expect`
    /// и лишних перевыделений не появляется.
    fn reserve(expect: usize, body: usize) -> usize {
        expect.min(body.saturating_mul(8 * MAX_MATCH))
    }

    /// Распаковывает кусок. `None` — поток негодный.
    ///
    /// Негодный поток это обычное дело: файл мог обрваться при падении, байт мог
    /// испортиться на диске. Отдать в таком случае мусор как содержимое
    /// документа значило бы врать тем убедительнее, чем незаметнее порча.
    pub fn decompress(&self, src: &[u8], expect: usize) -> Option<Vec<u8>> {
        let kind = *src.first()?;
        let body = &src[1..];
        if kind == 0 {
            return (body.len() == expect).then(|| body.to_vec());
        }
        if kind != 1 {
            return None;
        }

        let dict = self.bytes.get();
        let dl = dict.len();
        let mut out = Vec::with_capacity(Self::reserve(expect, body.len()));
        let mut r = BitReader::new(body);

        loop {
            if r.overrun() {
                return None;
            }
            let (sym, n) = self.lit_dec.lookup(r.peek(huff::MAX_BITS))?;
            r.skip(n);
            if sym == END {
                break;
            }
            if sym < END {
                out.push(sym as u8);
                if out.len() > expect {
                    return None;
                }
                continue;
            }

            let lc = usize::from(sym) - 257;
            if lc >= NLIT - 257 {
                return None;
            }
            let (lbase, lextra) = len_code(lc);
            let len = lbase + r.take(lextra) as usize;
            if !(MIN_MATCH..=MAX_MATCH).contains(&len) || out.len() + len > expect {
                return None;
            }

            let (dsym, dn) = self.dist_dec.lookup(r.peek(huff::MAX_BITS))?;
            r.skip(dn);
            let dc = usize::from(dsym);
            if dc >= NDIST {
                return None;
            }
            let (dbase, dextra) = dist_code(dc);
            let dist = dbase + r.take(dextra) as usize;

            let abs = dl + out.len();
            if dist == 0 || dist > abs || dist > WINDOW {
                return None;
            }
            let from = abs - dist;
            // Копирование **по байту**: совпадение может перекрываться с собой —
            // так кодируется повтор, — и может начинаться в словаре, а кончаться
            // в выводе. Копирование срезом испортило бы оба случая молча.
            for from in from..from + len {
                let b = if from < dl { dict[from] } else { out[from - dl] };
                out.push(b);
            }
        }

        (out.len() == expect).then_some(out)
    }
}

impl EncoderDict {
    /// Собирает словарь из байтов и обученных частот.
    ///
    /// `None` — частоты негодные: пустой алфавит длин или расстояний означал бы,
    /// что обучение не нашло ни одного совпадения, и такой словарь молча
    /// испортил бы всё, что им упакуют.
    pub fn new(bytes: Vec<u8>, lit_freq: &[u32], dist_freq: &[u32]) -> Option<EncoderDict> {
        let lit_len = huff::lengths(lit_freq);
        let dist_len = huff::lengths(dist_freq);
        let lit_dec = Decoder::new(&lit_len)?;
        let dist_dec = Decoder::new(&dist_len)?;
        let lit_code = huff::codes(&lit_len);
        let dist_code = huff::codes(&dist_len);
        let (head, prev) = index(&bytes);
        Some(EncoderDict {
            dec: DecoderDict { bytes: Bytes::Owned(bytes), lit_dec, dist_dec },
            head,
            prev,
            lit_len,
            dist_len,
            lit_code,
            dist_code,
        })
    }

    /// Упаковщик по **уже выложенной** записи словаря.
    ///
    /// ## Зачем
    ///
    /// Укрупнение переносит куски `.sbin` байт в байт вместе со словарём
    /// источника — пересжимать их нечем и незачем. Но кусок, из которого выпали
    /// погашенные документы, перенести как есть нельзя: его надо собрать заново.
    /// Собрать его **другим** словарём тоже нельзя — тогда он перестал бы
    /// принадлежать отрезку кусков своего словаря, а на непрерывности этого
    /// отрезка стоит весь поиск словаря по номеру куска.
    ///
    /// Отсюда и этот конструктор: он поднимает упаковщик по записи, которая уже
    /// лежит в `<поле>.sdic`, и пересжатый им кусок остаётся у своего словаря.
    ///
    /// Обучать заново нечему: длины кодов лежат в записи, а коды из длин
    /// восстанавливаются однозначно — потому на диске и хранятся именно длины.
    /// Дороже всего здесь цепочки совпадений по словарю, и они же — то, чего у
    /// распаковщика нет и быть не может.
    ///
    /// `None` — запись негодна или кончается не там, где кончается срез.
    pub fn adopt(record: &[u8]) -> Option<EncoderDict> {
        let r = split(record)?;
        if r.end != record.len() {
            return None;
        }
        let lit_dec = Decoder::new(&r.lit_len)?;
        let dist_dec = Decoder::new(&r.dist_len)?;
        let lit_code = huff::codes(&r.lit_len);
        let dist_code = huff::codes(&r.dist_len);
        let bytes = record.get(r.at..r.at + r.len)?.to_vec();
        let (head, prev) = index(&bytes);
        Some(EncoderDict {
            dec: DecoderDict { bytes: Bytes::Owned(bytes), lit_dec, dist_dec },
            head,
            prev,
            lit_len: r.lit_len,
            dist_len: r.dist_len,
            lit_code,
            dist_code,
        })
    }

    /// Словарь, обученный на выборке из колонки.
    ///
    /// Два прохода: первый разбирает выборку и считает, чего в ней сколько,
    /// второй строит по этому таблицы. Обучать надо на **другой** части колонки,
    /// чем потом меряешь, иначе меряется память словаря, а не сжатие, — но это
    /// забота вызывающего, здесь про это только сказано.
    pub fn train(bytes: Vec<u8>, sample: &[&[u8]]) -> Option<EncoderDict> {
        Self::train_with(bytes, sample, Effort::Thorough)
    }

    /// То же обучение в режиме, которым затем будут кодироваться куски.
    ///
    /// Ingest использует `Fast`, укрупнение — `Thorough`; частоты Хаффмана
    /// должны описывать именно тот поток лексем, который породит выбранный
    /// разбор, а не всегда самый дорогой из двух.
    pub fn train_with(bytes: Vec<u8>, sample: &[&[u8]], effort: Effort) -> Option<EncoderDict> {
        let (head, prev) = index(&bytes);
        let mut lit_freq = vec![0u32; NLIT];
        let mut dist_freq = vec![0u32; NDIST];
        let probe = EncoderDict {
            dec: DecoderDict {
                bytes: Bytes::Owned(bytes),
                lit_dec: Decoder::new(&[1u8, 1]).expect("два символа"),
                dist_dec: Decoder::new(&[1u8, 1]).expect("два символа"),
            },
            head,
            prev,
            lit_len: Vec::new(),
            dist_len: Vec::new(),
            lit_code: Vec::new(),
            dist_code: Vec::new(),
        };
        let mut scratch = Scratch::default();
        for part in sample {
            probe.parse(part, effort, &mut scratch, &mut |sym, dsym| {
                lit_freq[sym as usize] += 1;
                if let Some(d) = dsym {
                    dist_freq[d as usize] += 1;
                }
            });
            lit_freq[END as usize] += 1;
        }
        // Каждому символу — хотя бы единица.
        //
        // Символ с нулевой частотой не получает кода вовсе, и записать его потом
        // нечем: он ушёл бы в поток как ноль бит, а распаковщик прочитал бы на
        // его месте следующий. Выборка не обязана содержать все длины, все
        // расстояния и все байты — а данные их содержат.
        for f in lit_freq.iter_mut() {
            *f = (*f).max(1);
        }
        for f in dist_freq.iter_mut() {
            *f = (*f).max(1);
        }
        let Bytes::Owned(raw) = probe.dec.bytes else { return None };
        EncoderDict::new(raw, &lit_freq, &dist_freq)
    }

    pub fn len(&self) -> usize {
        self.dec.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dec.is_empty()
    }

    /// Распаковщик этого словаря. Нужен кругу «сжали — разжали» при сборке.
    pub fn decoder(&self) -> &DecoderDict {
        &self.dec
    }

    /// Кладёт словарь на диск: байты и длины кодов.
    ///
    /// Хранятся именно длины, а не сами коды: коды из длин восстанавливаются
    /// однозначно, а вот наоборот — нет, и хранить их значило бы держать на
    /// диске две согласованные копии одного.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.dec.len() + NLIT + NDIST + 12);
        out.extend((self.dec.bytes.get().len() as u32).to_le_bytes());
        out.extend_from_slice(self.dec.bytes.get());
        out.extend((self.lit_len.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.lit_len);
        out.extend((self.dist_len.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.dist_len);
        out
    }

    /// Разбирает кусок и отдаёт только символы — для обучения таблиц.
    ///
    /// Обёртка над [`Dict::walk`], а не вторая реализация: разбирать при
    /// обучении и при записи по-разному значило бы обучить таблицы не тому, что
    /// ими потом кодируется.
    fn parse(
        &self,
        src: &[u8],
        effort: Effort,
        sc: &mut Scratch,
        emit: &mut dyn FnMut(u16, Option<u16>),
    ) {
        self.walk(src, effort, sc, &mut |t| match t {
            Tok::Lit(b) => emit(u16::from(b), None),
            Tok::Match { len, dist } => {
                let (lc, _, _) = len_of(len);
                let (dc, _, _) = dist_of(dist);
                emit((257 + lc) as u16, Some(dc as u16));
            }
        });
    }

    /// Сжимает кусок. Ответ всегда распаковывается обратно, каким бы вход ни был.
    ///
    /// Лексемы пишутся **сразу**, а не собираются в вектор: он жил ровно до
    /// конца разбора одного куска и на настоящей колонке заводился заново
    /// тысячи раз. Обучению вектор тоже не нужен — оно считает частоты на лету.
    pub fn compress(&self, src: &[u8], effort: Effort, sc: &mut Scratch) -> Vec<u8> {
        let mut w = BitWriter::new();
        let (lit_code, lit_len) = (&self.lit_code, &self.lit_len);
        let (dist_code, dist_len) = (&self.dist_code, &self.dist_len);
        self.walk(src, effort, sc, &mut |t| match t {
            Tok::Lit(b) => {
                let s = usize::from(b);
                w.put(u32::from(lit_code[s]), u32::from(lit_len[s]));
            }
            Tok::Match { len, dist } => {
                let (lc, lx, lb) = len_of(len);
                let s = 257 + lc;
                w.put(u32::from(lit_code[s]), u32::from(lit_len[s]));
                w.put(lx, lb);
                let (dc, dx, db) = dist_of(dist);
                w.put(u32::from(dist_code[dc]), u32::from(dist_len[dc]));
                w.put(dx, db);
            }
        });
        w.put(u32::from(self.lit_code[END as usize]), u32::from(self.lit_len[END as usize]));
        let body = w.finish();

        if body.len() >= src.len() {
            let mut out = Vec::with_capacity(src.len() + 1);
            out.push(0u8);
            out.extend_from_slice(src);
            out
        } else {
            let mut out = Vec::with_capacity(body.len() + 1);
            out.push(1u8);
            out.extend_from_slice(&body);
            out
        }
    }
}

/// Лексема разбора.
enum Tok {
    Lit(u8),
    Match { len: usize, dist: usize },
}

impl EncoderDict {
    /// Единственный настоящий разбор. Обучение и запись зовут его же: разбирать
    /// двумя разными способами значило бы обучить таблицы не тому, что ими
    /// потом кодируется.
    fn walk(&self, src: &[u8], effort: Effort, sc: &mut Scratch, emit: &mut dyn FnMut(Tok)) {
        let dict = self.dec.bytes.get();
        sc.begin(src.len());
        let limit = src.len();
        let mut at = 0usize;

        while at < limit {
            let (mlen, mdist) = self.best(src, at, limit, sc, dict, effort);
            let mut take_lit = mlen < MIN_MATCH;

            if !take_lit && effort.lazy() && at + 1 < limit {
                sc.put(src, at, limit);
                let (nlen, ndist) = self.best(src, at + 1, limit, sc, dict, effort);
                if nlen > mlen {
                    emit(Tok::Lit(src[at]));
                    at += 1;
                    if nlen >= MIN_MATCH {
                        emit(Tok::Match { len: nlen, dist: ndist });
                        for k in 0..nlen {
                            sc.put(src, at + k, limit);
                        }
                        at += nlen;
                    }
                    continue;
                }
                take_lit = false;
            }

            if take_lit {
                emit(Tok::Lit(src[at]));
                sc.put(src, at, limit);
                at += 1;
            } else {
                emit(Tok::Match { len: mlen, dist: mdist });
                for k in 0..mlen {
                    sc.put(src, at + k, limit);
                }
                at += mlen;
            }
        }
    }

    fn best(
        &self,
        src: &[u8],
        at: usize,
        limit: usize,
        sc: &Scratch,
        dict: &[u8],
        effort: Effort,
    ) -> (usize, usize) {
        if at + MIN_MATCH > limit {
            return (0, 0);
        }
        let dl = dict.len();
        let h = hash4(read4(src, at));
        let cap = (limit - at).min(MAX_MATCH);
        let (mut blen, mut bdist) = (0usize, 0usize);

        let mut p = sc.head_of(read4(src, at));
        let mut steps = effort.chain();
        while p != 0 && steps > 0 {
            let cand = p - 1;
            if cand >= at {
                break;
            }
            let d = at - cand;
            if d > WINDOW {
                break;
            }
            let mut l = 0usize;
            while l < cap && src[cand + l] == src[at + l] {
                l += 1;
            }
            if l >= MIN_MATCH && l > blen {
                blen = l;
                bdist = d;
                if l == cap {
                    return (blen, bdist);
                }
            }
            p = sc.prev[cand] as usize;
            steps -= 1;
        }

        let mut p = self.head[h] as usize;
        let mut steps = effort.chain();
        while p != 0 && steps > 0 {
            let cand = p - 1;
            let d = dl - cand + at;
            if d > WINDOW {
                break;
            }
            let mut l = 0usize;
            while l < cap && cand + l < dl && dict[cand + l] == src[at + l] {
                l += 1;
            }
            if l >= MIN_MATCH && l > blen {
                blen = l;
                bdist = d;
                if l == cap {
                    break;
                }
            }
            p = self.prev[cand] as usize;
            steps -= 1;
        }
        (blen, bdist)
    }
}

/// Переиспользуемое состояние разбора одного куска.
///
/// ## Зачем
///
/// Здесь таблица заводится один раз на всю сборку и не обнуляется вовсе. Вместо
/// обнуления у каждой корзины стоит **отметка поколения**: корзина считается
/// пустой, если её отметка не совпадает с текущей. Смена куска — увеличение
/// счётчика на единицу.
#[derive(Default)]
pub struct Scratch {
    head: Vec<u32>,
    stamp: Vec<u32>,
    prev: Vec<u32>,
    now: u32,
}

impl Scratch {
    /// Готовит состояние под кусок длиной `n`. Ничего не обнуляет.
    fn begin(&mut self, n: usize) {
        if self.head.is_empty() {
            self.head = vec![0u32; 1 << SRC_HASH_BITS];
            self.stamp = vec![0u32; 1 << SRC_HASH_BITS];
        }
        if self.prev.len() < n + 1 {
            self.prev.resize(n + 1, 0);
        }
        // Переполнение счётчика поколений — раз в четыре миллиарда кусков, и
        // тогда отметки надо погасить: иначе старая корзина сойдёт за свежую.
        self.now = match self.now.checked_add(1) {
            Some(v) => v,
            None => {
                self.stamp.fill(0);
                1
            }
        };
    }

    #[inline(always)]
    fn head_of(&self, v: u32) -> usize {
        let h = hash_src(v);
        if self.stamp[h] == self.now {
            self.head[h] as usize
        } else {
            0
        }
    }

    #[inline(always)]
    fn put(&mut self, src: &[u8], at: usize, limit: usize) {
        if at + MIN_MATCH <= limit {
            let h = hash_src(read4(src, at));
            self.prev[at] = if self.stamp[h] == self.now { self.head[h] } else { 0 };
            self.head[h] = at as u32 + 1;
            self.stamp[h] = self.now;
        }
    }
}

/// Строит цепочки позиций по словарю: хеш четырёх байт — последняя позиция,
/// дальше по `prev`.
fn index(bytes: &[u8]) -> (Vec<u32>, Vec<u32>) {
    let mut head = vec![0u32; 1 << HASH_BITS];
    let mut prev = vec![0u32; bytes.len() + 1];
    if bytes.len() >= MIN_MATCH {
        #[allow(clippy::needless_range_loop)] // индекс сам участвует в вычислении
        for at in 0..=bytes.len() - MIN_MATCH {
            let h = hash4(read4(bytes, at));
            prev[at] = head[h];
            head[h] = at as u32 + 1;
        }
    }
    (head, prev)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict_of(sample: &[&[u8]]) -> EncoderDict {
        let mut bytes = Vec::new();
        for s in sample {
            bytes.extend_from_slice(s);
        }
        EncoderDict::train(bytes, sample).expect("словарь обучился")
    }

    fn round(d: &EncoderDict, data: &[u8], effort: Effort) {
        let packed = d.compress(data, effort, &mut Scratch::default());
        let back = d.decoder().decompress(&packed, data.len());
        assert_eq!(
            back.as_deref(),
            Some(data),
            "круг не сошёлся на {} байтах при {effort:?}",
            data.len()
        );
    }

    fn text(n: usize) -> Vec<u8> {
        let mut s = String::new();
        for i in 0..n {
            s.push_str(&format!(
                "товар номер {i} категория электроника цена {} руб описание длинное\n",
                i * 7
            ));
        }
        s.into_bytes()
    }

    /// Испорченная засечка не должна требовать памяти.
    ///
    /// «Длина до сжатия» лежит в `<поле>.smrk` четырьмя байтами без контрольной
    /// суммы, и резерв под распаковку делался **по ней** — до разбора первого
    /// бита. Один перевёрнутый старший байт превращал выдачу документа в запрос
    /// почти четырёх гигабайт: под `ulimit -v` это не `None` на одном куске, а
    /// `abort` всего узла.
    #[test]
    fn a_broken_mark_cannot_ask_for_memory() {
        let seed = text(40);
        let d = dict_of(&[seed.as_slice()]);
        let data = text(60);
        let packed = d.compress(&data, Effort::Thorough, &mut Scratch::default());

        // Резерв берётся по входу, а не по обещанию засечки.
        assert!(
            DecoderDict::reserve(u32::MAX as usize, packed.len()) < u32::MAX as usize,
            "резерв по испорченной засечке остался её заложником"
        );
        // А на годных числах — ровно то, что просили: `min` берёт `expect`, и
        // лишних перевыделений не появляется.
        assert_eq!(DecoderDict::reserve(data.len(), packed.len()), data.len());
        assert_eq!(DecoderDict::reserve(4096, 1500), 4096);

        // Отказ при этом остался отказом: враньё засечки ловится по выходу.
        assert!(d.decoder().decompress(&packed, u32::MAX as usize).is_none());
        assert!(d.decoder().decompress(&packed, data.len() + 1).is_none());
        assert_eq!(d.decoder().decompress(&packed, data.len()).as_deref(), Some(data.as_slice()));
    }

    /// Коды длин и расстояний обязаны быть обратимы на **каждом** значении, а не
    /// на выборке: ошибка в одной группе даёт неверную длину, а не падение.
    #[test]
    fn every_length_and_distance_round_trips_through_its_code() {
        for len in MIN_MATCH..=MAX_MATCH {
            let (c, extra, bits) = len_of(len);
            assert!(c < NLIT - 257, "код длины {c} вне алфавита");
            let (base, b) = len_code(c);
            assert_eq!(b, bits, "число добавочных бит разошлось на длине {len}");
            assert_eq!(base + extra as usize, len, "длина {len} восстановилась неверно");
            assert!(extra < (1 << bits) || bits == 0, "добавочное не влезает");
        }
        for dist in 1..=WINDOW {
            let (c, extra, bits) = dist_of(dist);
            assert!(c < NDIST, "код расстояния {c} вне алфавита на {dist}");
            let (base, b) = dist_code(c);
            assert_eq!(b, bits, "число добавочных бит разошлось на расстоянии {dist}");
            assert_eq!(base + extra as usize, dist, "расстояние {dist} восстановилось неверно");
        }
    }

    #[test]
    fn every_shape_survives_the_round_trip() {
        let sample = text(200);
        let parts: Vec<&[u8]> = sample.chunks(4096).collect();
        let d = dict_of(&parts);

        for effort in [Effort::Fast, Effort::Thorough] {
            round(&d, b"", effort);
            round(&d, b"a", effort);
            round(&d, b"abc", effort);
            round(&d, &[0u8; 100], effort);
            round(&d, b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", effort);
            round(&d, "русский текст с повторами русский текст с повторами".as_bytes(), effort);
            round(&d, &text(50), effort);
            // Совпадение длиннее потолка: обязано разбиться на несколько.
            round(&d, &vec![b'x'; MAX_MATCH * 3], effort);
            // Шум: сжатию не за что зацепиться, кусок обязан лечь как есть.
            let mut seed = 12345u64;
            let noise: Vec<u8> = (0..5000)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed as u8
                })
                .collect();
            round(&d, &noise, effort);
        }
    }

    /// Словарь обязан помогать, иначе он не нужен.
    #[test]
    fn the_dictionary_actually_shrinks_what_it_was_trained_on() {
        let sample = text(400);
        let parts: Vec<&[u8]> = sample.chunks(4096).collect();
        let with = dict_of(&parts);
        let without = {
            let mut freq = vec![1u32; NLIT];
            let mut df = vec![1u32; NDIST];
            freq[END as usize] = 1;
            df[0] = 1;
            EncoderDict::new(Vec::new(), &freq, &df).expect("словарь без байтов")
        };
        // Кусок из той же колонки, но не из выборки.
        let fresh = text(430);
        let fresh = &fresh[fresh.len() - 4096..];
        let a = with.compress(fresh, Effort::Fast, &mut Scratch::default()).len();
        let b = without.compress(fresh, Effort::Fast, &mut Scratch::default()).len();
        assert!(a * 10 < b * 9, "словарь не дал и десятой части: {a} против {b}");
        round(&with, fresh, Effort::Fast);
        round(&without, fresh, Effort::Fast);
    }

    #[test]
    fn thorough_is_never_worse_than_fast() {
        let sample = text(300);
        let parts: Vec<&[u8]> = sample.chunks(4096).collect();
        let d = dict_of(&parts);
        let fresh = text(320);
        for chunk in fresh.chunks(4096).take(8) {
            let f = d.compress(chunk, Effort::Fast, &mut Scratch::default()).len();
            let t = d.compress(chunk, Effort::Thorough, &mut Scratch::default()).len();
            assert!(t <= f, "тщательный вышел хуже быстрого: {t} против {f}");
            round(&d, chunk, Effort::Thorough);
        }
    }

    /// Испорченный поток отвергается, а не отдаётся как содержимое.
    #[test]
    fn a_broken_stream_is_refused_not_returned() {
        let sample = text(200);
        let parts: Vec<&[u8]> = sample.chunks(4096).collect();
        let d = dict_of(&parts);
        let data = &text(210)[..3000];
        let packed = d.compress(data, Effort::Thorough, &mut Scratch::default());

        for cut in 0..packed.len() {
            if let Some(back) = d.decoder().decompress(&packed[..cut], data.len()) {
                assert_eq!(back, data, "обрыв на {cut} дал не то содержимое");
            }
        }
        for at in (0..packed.len()).step_by(7) {
            let mut bad = packed.clone();
            bad[at] ^= 0xff;
            if let Some(back) = d.decoder().decompress(&bad, data.len()) {
                assert_eq!(back.len(), data.len(), "порча на {at} дала не ту длину");
            }
        }
    }

    /// Словарь ходит на диск и обратно без потерь.
    #[test]
    fn a_dictionary_survives_the_disk() {
        let sample = text(200);
        let parts: Vec<&[u8]> = sample.chunks(4096).collect();
        let d = dict_of(&parts);
        let back = DecoderDict::from_bytes(&d.to_bytes()).expect("словарь прочитался");
        let data = &text(230)[..4000];
        let a = d.compress(data, Effort::Thorough, &mut Scratch::default());
        // Словарь с диска умеет ровно то, что нужно поиску, — распаковывать. И
        // это главное утверждение после разделения: упакованное **упаковщиком**
        // читается **распаковщиком**, собранным из выложенных байт.
        assert_eq!(back.decompress(&a, data.len()).as_deref(), Some(data));
        assert_eq!(back.len(), d.len(), "длина словаря после диска другая");

        // Обрыв файла словаря обязан быть замечен.
        let bytes = d.to_bytes();
        for cut in [0usize, 3, bytes.len() / 2, bytes.len() - 1] {
            assert!(DecoderDict::from_bytes(&bytes[..cut]).is_none(), "обрыв на {cut} принят");
        }
    }
}

/// Записи словарей внутри одного файла: то, на чём стоит перенос кусков.
#[cfg(test)]
mod records {
    use super::*;

    fn text(tag: &str, n: usize) -> Vec<u8> {
        let mut s = String::new();
        for i in 0..n {
            s.push_str(&format!("{tag} строка {i} свойство {} значение {}\n", i % 23, i * 11));
        }
        s.into_bytes()
    }

    fn dict_of(seed: &[u8]) -> EncoderDict {
        let parts: Vec<&[u8]> = seed.chunks(4096).collect();
        EncoderDict::train(seed.to_vec(), &parts).expect("словарь обучился")
    }

    /// Упаковщик, поднятый по выложенной записи, жмёт **теми же байтами**.
    ///
    /// ## Зачем это нужно и что ломалось бы без проверки
    ///
    /// Укрупнение переносит куски `.sbin` как есть, но кусок, из которого выпали
    /// погашенные документы, приходится собирать заново — и обязательно словарём
    /// **своего** источника, иначе он вывалится из отрезка кусков этого словаря.
    /// Словаря того источника в живом виде уже нет: есть только запись в
    /// `.sdic`. Значит поднятый по ней упаковщик обязан быть неотличим от
    /// исходного — не «сжимать не хуже», а давать **те же байты**.
    ///
    /// Сравнение «распаковалось обратно» этого не ловило бы: коды, построенные по
    /// другим длинам, тоже распаковываются своим же распаковщиком безупречно, а
    /// перенос кусков после такого пересжатия молча испортил бы отрезок.
    #[test]
    fn an_adopted_dictionary_packs_byte_for_byte_like_its_source() {
        let d = dict_of(&text("альфа", 300));
        let rec = d.to_bytes();
        let a = EncoderDict::adopt(&rec).expect("запись принята");

        assert_eq!(a.len(), d.len(), "длина словаря после подъёма другая");
        assert_eq!(a.to_bytes(), rec, "выложенная запись после подъёма другая");

        let cases: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"a".to_vec(),
            text("альфа", 50),
            text("зета", 60),
            vec![b'x'; MAX_MATCH * 3],
        ];
        for effort in [Effort::Fast, Effort::Thorough] {
            for data in cases.iter() {
                let want = d.compress(data, effort, &mut Scratch::default());
                let got = a.compress(data, effort, &mut Scratch::default());
                assert_eq!(got, want, "поднятый словарь сжал иначе при {effort:?}");
                // И то, что он сжал, читается распаковщиком исходного: именно на
                // этом стоит перенос — кусок остаётся у своего словаря.
                assert_eq!(
                    d.decoder().decompress(&got, data.len()).as_deref(),
                    Some(data.as_slice()),
                    "исходный распаковщик не прочёл пересжатое при {effort:?}"
                );
            }
        }
    }

    /// Запись, кончающаяся не там, где кончается срез, не поднимается.
    ///
    /// Разбор идёт от начала и сам по себе о конце ничего не знает: чужое
    /// смещение или лишний хвост дали бы правдоподобный словарь, которым потом
    /// распаковались бы чужие куски.
    #[test]
    fn a_record_that_does_not_end_where_promised_is_refused() {
        let d = dict_of(&text("альфа", 200));
        let rec = d.to_bytes();
        assert!(EncoderDict::adopt(&rec).is_some(), "целая запись обязана подниматься");

        let mut longer = rec.clone();
        longer.push(0);
        assert!(EncoderDict::adopt(&longer).is_none(), "запись с хвостом принята");

        for cut in [0usize, 3, rec.len() / 2, rec.len() - 1] {
            assert!(EncoderDict::adopt(&rec[..cut]).is_none(), "обрыв на {cut} принят");
        }
        // И то же для распаковщика: две записи подряд — не одна длинная.
        let mut two = rec.clone();
        two.extend_from_slice(&dict_of(&text("зета", 210)).to_bytes());
        assert!(
            DecoderDict::from_bytes(&two).is_some(),
            "разбор от нуля склейку всё-таки принимает — потому и нужна таблица"
        );
    }

    /// Записи в одном файле читаются каждая по своим границам, а отображение у
    /// них **одно**.
    ///
    /// Отдельное отображение на словарь означало бы `K` областей у ядра на один
    /// файл — при потолке `vm.max_map_count` в 65530 и двух десятках областей на
    /// сегмент это упёрлось бы не на тысячах сегментов, а на сотнях.
    #[test]
    fn every_record_is_read_by_its_own_bounds_from_one_mapping() {
        let a = dict_of(&text("альфа", 300));
        let z = dict_of(&text("зета", 320));
        let (ra, rz) = (a.to_bytes(), z.to_bytes());
        assert_ne!(ra, rz, "словари вышли одинаковыми — подменять нечего");
        let mut whole = ra.clone();
        whole.extend_from_slice(&rz);

        let path = std::env::temp_dir()
            .join(format!("svindb-pack-records-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::write(&path, &whole).expect("файл");
        let map = crate::mmap::Region::of_file(&path).expect("отображение");

        let da = DecoderDict::open_at(&map, 0, ra.len()).expect("первая");
        let dz = DecoderDict::open_at(&map, ra.len(), rz.len()).expect("вторая");
        assert_eq!(map.owners(), 3, "у записей оказалось не одно отображение на всех");
        assert_eq!(da.len(), a.len());
        assert_eq!(dz.len(), z.len());

        let data = text("альфа", 40);
        let packed = a.compress(&data, Effort::Fast, &mut Scratch::default());
        assert_eq!(da.decompress(&packed, data.len()).as_deref(), Some(data.as_slice()));
        // Чужой словарь того же файла обязан не прочесть — иначе таблица
        // словарей ничего не решала бы.
        assert_ne!(
            dz.decompress(&packed, data.len()).as_deref(),
            Some(data.as_slice()),
            "чужой словарь распаковал верно — значит словари неразличимы"
        );

        // Границы: на байт мимо — отказ, а не правдоподобная запись.
        assert!(DecoderDict::open_at(&map, 0, ra.len() - 1).is_none());
        assert!(DecoderDict::open_at(&map, 0, ra.len() + 1).is_none());
        assert!(DecoderDict::open_at(&map, 1, ra.len()).is_none());
        assert!(DecoderDict::open_at(&map, ra.len(), rz.len() + 1).is_none());
        assert!(DecoderDict::open_at(&map, whole.len(), 8).is_none());

        drop(da);
        drop(dz);
        std::fs::remove_file(&path).ok();
    }
}
