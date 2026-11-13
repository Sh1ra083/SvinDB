//! Колонка значений: число на документ, отобранное в память.

use crate::mmap::Region;

/// Документов в блоке. То же число, что у постингов.
pub const BLOCK: usize = 128;

/// Байт метаданных на блок: наименьшее значение, наибольшее и смещение данных.
pub const META: usize = 20;

/// Порядок-сохраняющее превращение знакового в беззнаковое.
#[inline(always)]
pub fn ord(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

#[inline(always)]
pub fn unord(u: u64) -> i64 {
    (u ^ (1u64 << 63)) as i64
}

/// Ширина в битах, которой хватает на значение.
#[inline(always)]
fn width_of(max: u64) -> u32 {
    64 - max.leading_zeros()
}

fn pack(out: &mut Vec<u8>, vals: &[u64], width: u32) {
    if width == 0 {
        return;
    }
    let mut acc: u128 = 0;
    let mut bits = 0u32;
    for &v in vals {
        acc |= (v as u128) << bits;
        bits += width;
        while bits >= 8 {
            out.push((acc & 0xff) as u8);
            acc >>= 8;
            bits -= 8;
        }
    }
    if bits > 0 {
        out.push((acc & 0xff) as u8);
    }
}

/// Одно значение по номеру внутри блока, без распаковки блока.
///
/// ## Почему шестнадцать байт берутся одним словом
///
/// Раньше здесь стоял цикл на шестнадцать `src.get(byte + j)` — шестнадцать
/// проверок границы и шестнадцать сдвигов ради одного числа. Замерено, что
/// чтение значения стоит около 4,45 наносекунды на документ и что это
/// **доминирующий член** и в агрегации, и в отборе: голый обход корпуса стоит
/// 4,5 нс на документ, а тот же обход с двумя условиями — 13,4, то есть по
/// одному чтению на условие.
///
/// Ширина значения не превышает шестидесяти четырёх разрядов, а сдвиг внутри
/// байта — семи: больше девяти байт прочитать неоткуда, и шестнадцать берутся
/// с запасом. Когда они есть — это одно невыровненное слово. Побайтно
/// собирается только хвост файла, где шестнадцати байт уже нет.
///
/// Ответ обязан совпадать с прежним побайтовым до бита: обе ветви складывают
/// те же байты в том же порядке, и это проверяется
/// [`tests::both_ways_of_reading_a_value_agree`].
#[inline(always)]
fn value_at(src: &[u8], width: u32, i: usize) -> u64 {
    if width == 0 {
        return 0;
    }
    let bit = i * width as usize;
    let byte = bit >> 3;
    let shift = (bit & 7) as u32;
    let w: u128 = match src.get(byte..byte + 16) {
        Some(s) => u128::from_le_bytes(s.try_into().expect("ровно шестнадцать байт")),
        None => value_tail(src, byte),
    };
    let mask: u128 = if width >= 128 { u128::MAX } else { (1u128 << width) - 1 };
    ((w >> shift) & mask) as u64
}

/// Хвост файла: шестнадцати байт нет, собирается тем, что осталось.
///
/// Вынесено из [`value_at`] намеренно — `#[cold]` держит редкую ветвь подальше
/// от горячего пути, чтобы она не мешала встраиванию.
#[cold]
fn value_tail(src: &[u8], byte: usize) -> u128 {
    let mut w: u128 = 0;
    for j in 0..16usize {
        if let Some(&b) = src.get(byte + j) {
            w |= (b as u128) << (8 * j);
        }
    }
    w
}

/// Собирает колонку. Отдаёт значения и метаданные блоков отдельными файлами.
///
/// Документ без значения обязан иметь `false` в `has`; что лежит в `vals` на его
/// месте, значения не имеет — оно не будет прочитано. Сюда удобно класть
/// наименьшее значение блока, чтобы не задирать ширину.
pub fn encode(vals: &[i64], has: &[bool]) -> (Vec<u8>, Vec<u8>) {
    let nblocks = vals.len().div_ceil(BLOCK);
    let mut data = Vec::with_capacity(vals.len());
    let mut meta = Vec::with_capacity(nblocks * META);
    let mut buf: Vec<u64> = Vec::with_capacity(BLOCK);

    for (bi, chunk) in vals.chunks(BLOCK).enumerate() {
        let from = bi * BLOCK;
        let at = data.len() as u32;
        // Границы считаются только по документам со значением: отсутствующее
        // значение не должно раздвигать границы блока и мешать пропуску.
        let mut lo = i64::MAX;
        let mut hi = i64::MIN;
        for (i, &v) in chunk.iter().enumerate() {
            if has[from + i] {
                lo = lo.min(v);
                hi = hi.max(v);
            }
        }
        if lo > hi {
            // Ни одного значения в блоке. Границы делаем пустыми так, чтобы ни
            // один диапазон их не пересёк.
            lo = i64::MAX;
            hi = i64::MIN;
        }
        let base = if lo == i64::MAX { 0 } else { ord(lo) };
        buf.clear();
        for (i, &v) in chunk.iter().enumerate() {
            buf.push(if has[from + i] { ord(v) - base } else { 0 });
        }
        let width = width_of(buf.iter().copied().max().unwrap_or(0));
        meta.extend(lo.to_le_bytes());
        meta.extend(hi.to_le_bytes());
        // Смещение **байта ширины**, а не данных: по нему читается и то и другое.
        meta.extend(at.to_le_bytes());
        data.push(width as u8);
        pack(&mut data, &buf, width);
    }
    (data, meta)
}

/// Байт метаданных разреженного блока: те же двадцать, что у плотного, плюс
/// номер последнего значения блока.
///
/// Первые двадцать байт **побайтово совпадают** с плотными: границы блока и
/// смещение данных лежат там же. Поэтому пропуск блока по границам, признак
/// разброса и обход по значению работают над обеими раскладками одним кодом —
/// разреженная запись это плотная плюс то, что заменяет арифметику поиском.
pub const SMETA: usize = 24;

/// Собирает разреженную колонку из пар «номер, значение» по возрастанию номера.
pub fn encode_sparse(present: &[(u32, i64)]) -> (Vec<u8>, Vec<u8>) {
    let nblocks = present.len().div_ceil(BLOCK);
    let mut data = Vec::with_capacity(present.len() * 2);
    let mut meta = Vec::with_capacity(nblocks * SMETA);
    let mut deltas: Vec<u64> = Vec::with_capacity(BLOCK);
    let mut vals: Vec<u64> = Vec::with_capacity(BLOCK);
    let mut prev = 0u32;
    for chunk in present.chunks(BLOCK) {
        let at = data.len() as u32;
        let lo = chunk.iter().map(|(_, v)| *v).min().unwrap_or(i64::MAX);
        let hi = chunk.iter().map(|(_, v)| *v).max().unwrap_or(i64::MIN);
        let base = ord(lo);
        deltas.clear();
        vals.clear();
        for (u, v) in chunk.iter() {
            deltas.push((*u - prev) as u64);
            prev = *u;
            vals.push(ord(*v) - base);
        }
        let dw = width_of(deltas.iter().copied().max().unwrap_or(0));
        let vw = width_of(vals.iter().copied().max().unwrap_or(0));
        meta.extend(lo.to_le_bytes());
        meta.extend(hi.to_le_bytes());
        meta.extend(at.to_le_bytes());
        meta.extend(chunk.last().map(|(u, _)| *u).unwrap_or(0).to_le_bytes());
        data.push(dw as u8);
        pack(&mut data, &deltas, dw);
        data.push(vw as u8);
        pack(&mut data, &vals, vw);
    }
    (data, meta)
}

/// Во что обойдётся колонка в каждой раскладке, в байтах.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cost {
    pub dense: u64,
    pub sparse: u64,
}

/// Во сколько раз разреженная обязана быть дешевле, чтобы за неё стоило
/// платить поиском вместо арифметики.
pub const SPARSE_MARGIN: u64 = 2;

/// Считает обе цены одним проходом по значениям.
pub fn measure(units: usize, present: &[(u32, i64)]) -> Cost {
    // Плотная: метаданные блока плюс упакованные значения плюс биты присутствия.
    let nblocks = units.div_ceil(BLOCK);
    let mut dense = (nblocks * META) as u64 + units.div_ceil(64) as u64 * 8;
    {
        // Ширина считается по блокам: у пустого блока она нулевая, и такой блок
        // стоит один байт. Считать по общему размаху значило бы завысить цену
        // разреженного поля и никогда его не выбрать.
        let mut at = 0usize;
        for b in 0..nblocks {
            let from = (b * BLOCK) as u32;
            let to = from + BLOCK as u32;
            let mut lo = i64::MAX;
            let mut hi = i64::MIN;
            while at < present.len() && present[at].0 < to {
                if present[at].0 >= from {
                    lo = lo.min(present[at].1);
                    hi = hi.max(present[at].1);
                }
                at += 1;
            }
            let w = if lo > hi { 0 } else { width_of(ord(hi) - ord(lo)) };
            let n = BLOCK.min(units - b * BLOCK);
            dense += 1 + (n * w as usize).div_ceil(8) as u64;
        }
    }
    // Разреженная: те же границы плюс номер последнего, приращения и значения.
    let sblocks = present.len().div_ceil(BLOCK);
    let mut sparse = (sblocks * SMETA) as u64;
    let mut prev = 0u32;
    for chunk in present.chunks(BLOCK) {
        let lo = chunk.iter().map(|(_, v)| *v).min().unwrap_or(0);
        let hi = chunk.iter().map(|(_, v)| *v).max().unwrap_or(0);
        let mut dmax = 0u64;
        for (u, _) in chunk.iter() {
            dmax = dmax.max((*u - prev) as u64);
            prev = *u;
        }
        let dw = width_of(dmax);
        let vw = width_of(ord(hi) - ord(lo));
        sparse += 2
            + (chunk.len() * dw as usize).div_ceil(8) as u64
            + (chunk.len() * vw as usize).div_ceil(8) as u64;
    }
    Cost { dense, sparse }
}

/// Какую раскладку выбрать этим данным.
///
/// Точка, где разреженная становится вдвое дешевле, зависит от ширины значения
/// втрое: у поля-константы это плотность около 0.16, у 32-битного — около 0.46.
/// Константа «выбираем разреженную ниже 0.4» ошиблась бы на поле-константе в два
/// с половиной раза, а поля-константы в разреженных схемах как раз частые:
/// признак, тип, версия.
pub fn choose(units: usize, present: &[(u32, i64)]) -> Layout {
    if present.is_empty() {
        return Layout::None;
    }
    let c = measure(units, present);
    if c.sparse.saturating_mul(SPARSE_MARGIN) <= c.dense {
        Layout::Sparse
    } else {
        Layout::Dense
    }
}

/// Бит присутствия на документ.
pub fn encode_presence(has: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; has.len().div_ceil(64) * 8];
    for (d, &y) in has.iter().enumerate() {
        if y {
            out[d >> 3] |= 1u8 << (d & 7);
        }
    }
    out
}

/// Как поле уложено в этом сегменте.
///
/// Раскладка выбирается **по данным на сбросе**, а не объявляется в схеме.
/// Память о текущем блоке для прохода подряд. См. [`Column::get_with`].
#[derive(Clone, Copy)]
pub struct BlockCache {
    block: usize,
    start: usize,
    width: u32,
    base: u64,
}

impl Default for BlockCache {
    fn default() -> BlockCache {
        BlockCache { block: usize::MAX, start: 0, width: 0, base: 0 }
    }
}

/// Прецедент тот же, что у корзин: сегмент считает признак сам и решает сам —
/// догадываться о том, как поле будут спрашивать, не требуется.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Layout {
    /// Ни одного значения в сегменте. Файлов нет вовсе.
    None,
    /// Значение адресуется арифметикой: блок `doc/128`, слот `doc%128`.
    #[default]
    Dense,
    /// Только присутствующие значения, по возрастанию номера.
    ///
    /// Плотная раскладка платит за **каждый** документ, есть у него значение
    /// или нет: 0.156 байта метаданных блока и 0.125 бита присутствия. На
    /// поле, которое есть у одного документа из ста, это в сто раз дороже
    /// самих данных. Здесь платится только за то, что есть, а номер хранится
    /// приращением.
    ///
    /// Плата за это одна: адресация меняется с арифметики на двоичный поиск по
    /// номеру последнего значения блока.
    Sparse,
}

impl Layout {
    pub fn name(self) -> &'static str {
        match self {
            Layout::None => "none",
            Layout::Dense => "dense",
            Layout::Sparse => "sparse",
        }
    }

    pub fn parse(s: &str) -> Option<Layout> {
        match s {
            "none" => Some(Layout::None),
            "dense" => Some(Layout::Dense),
            "sparse" => Some(Layout::Sparse),
            _ => None,
        }
    }
}

/// Как хранится признак наличия значения.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Presence {
    /// Бит на документ в `.has`.
    #[default]
    Bits,
    /// Значение есть у **каждого** документа — файла нет.
    All,
}

impl Presence {
    pub fn name(self) -> &'static str {
        match self {
            Presence::Bits => "bits",
            Presence::All => "all",
        }
    }

    pub fn parse(s: &str) -> Option<Presence> {
        match s {
            "bits" => Some(Presence::Bits),
            "all" => Some(Presence::All),
            _ => None,
        }
    }
}

/// Отображённая колонка.
pub struct Column {
    docs: usize,
    /// Надстройка для полей, где пропуск по границам блока не отсекает.
    pub buckets: Option<Buckets>,
    data: Region,
    meta: Region,
    /// `None` — значение есть у каждого документа, файла нет.
    has: Option<Region>,
    layout: Layout,
    /// Сколько значений в колонке. У плотной равно числу единиц.
    values: usize,
}

impl Column {
    /// Плотная полоса внутри другого typed-container. Проверяет все границы
    /// блоков до передачи mmap горячему reader; присутствуют все значения.
    pub(crate) fn from_dense_parts(data: Region, meta: Region, values: usize) -> Option<Self> {
        let blocks = values.div_ceil(BLOCK);
        if values > u32::MAX as usize || meta.len_bytes() != blocks.checked_mul(META)? {
            return None;
        }
        let mut expected = 0usize;
        for block in 0..blocks {
            let start = meta.u32_at(block * META + 16) as usize;
            if start != expected || start >= data.len_bytes() {
                return None;
            }
            let width = data.u8_at(start) as usize;
            let lo = meta.u64_at(block * META) as i64;
            let hi = meta.u64_at(block * META + 8) as i64;
            if width > 64 || lo > hi {
                return None;
            }
            let count = (values - block * BLOCK).min(BLOCK);
            expected = start.checked_add(1 + count.checked_mul(width)?.div_ceil(8))?;
        }
        if expected != data.len_bytes() {
            return None;
        }
        Some(Self {
            docs: values,
            data,
            meta,
            has: None,
            buckets: None,
            layout: Layout::Dense,
            values,
        })
    }

    /// Открывает колонку поля. `None` — файлов нет.
    ///
    /// `presence` приходит из заголовка сегмента, а не выводится из наличия
    /// файла. Разница несущая: «файла нет, потому что он не нужен» и «файл
    /// потерялся» — разные состояния, и второе обязано быть поломкой, а не
    /// молчаливым «значение есть у всех».
    pub fn open(
        src: &crate::container::SegmentSource,
        field: &str,
        docs: usize,
        presence: Presence,
    ) -> Option<Column> {
        Column::open_with(src, field, docs, presence, Layout::Dense, docs)
    }

    /// То же, но с объявленной раскладкой и числом значений.
    ///
    /// Оба берутся из заголовка сегмента, а не выводятся: «файла нет, потому
    /// что он не нужен» и «файл потерялся» обязаны различаться.
    pub fn open_with(
        src: &crate::container::SegmentSource,
        field: &str,
        docs: usize,
        presence: Presence,
        layout: Layout,
        values: usize,
    ) -> Option<Column> {
        if layout == Layout::Sparse {
            let data = src.try_open(&format!("{field}.scol"))?;
            let meta = src.try_open(&format!("{field}.smeta"))?;
            if meta.len_bytes() < values.div_ceil(BLOCK) * SMETA {
                return None;
            }
            let buckets = Buckets::open(src, field, docs);
            return Some(Column { docs, data, meta, has: None, buckets, layout, values });
        }
        let data = src.try_open(&format!("{field}.col"))?;
        let meta = src.try_open(&format!("{field}.cmeta"))?;
        let has = match presence {
            Presence::All => None,
            Presence::Bits => Some(src.try_open(&format!("{field}.has"))?),
        };
        let nblocks = docs.div_ceil(BLOCK);
        if meta.len_bytes() < nblocks * META {
            return None;
        }
        let buckets = Buckets::open(src, field, docs);
        Some(Column { docs, data, meta, has, buckets, layout: Layout::Dense, values: docs })
    }

    /// Сколько блоков у колонки: у плотной по единицам, у разреженной по
    /// значениям.
    #[inline(always)]
    fn nblocks(&self) -> usize {
        match self.layout {
            Layout::Sparse => self.values.div_ceil(BLOCK),
            _ => self.docs.div_ceil(BLOCK),
        }
    }

    #[inline(always)]
    fn stride(&self) -> usize {
        match self.layout {
            Layout::Sparse => SMETA,
            _ => META,
        }
    }

    /// Значение разреженной колонки: двоичный поиск по номеру последнего в
    /// блоке, затем проход по приращениям внутри блока.
    fn sparse_get(&self, unit: u32) -> Option<i64> {
        let n = self.nblocks();
        if n == 0 {
            return None;
        }
        let last_of = |b: usize| self.meta.u32_at(b * SMETA + 20);
        let (mut lo, mut hi) = (0usize, n - 1);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if last_of(mid) < unit {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if last_of(lo) < unit {
            return None;
        }
        let at = self.meta.u32_at(lo * SMETA + 16) as usize;
        let base = ord(self.block_min(lo));
        let count = BLOCK.min(self.values - lo * BLOCK);
        let dw = self.data.u8_at(at) as u32;
        let dstart = at + 1;
        let dlen = (count * dw as usize).div_ceil(8);
        let vw = self.data.u8_at(dstart + dlen) as u32;
        let vstart = dstart + dlen + 1;
        let dsrc = self.data.slice(dstart, dlen + 16);
        let mut cur = if lo == 0 { 0u32 } else { last_of(lo - 1) };
        for i in 0..count {
            cur += value_at(dsrc, dw, i) as u32;
            if cur == unit {
                let vsrc = self.data.slice(vstart, (count * vw as usize).div_ceil(8) + 16);
                return Some(unord(base + value_at(vsrc, vw, i)));
            }
            if cur > unit {
                return None;
            }
        }
        None
    }

    /// Номер последнего документа блока разреженной колонки.
    #[inline]
    fn sparse_last(&self, b: usize) -> u32 {
        self.meta.u32_at(b * SMETA + 20)
    }

    /// Пары «номер документа, значение» одного блока разреженной колонки.
    ///
    /// Тот же разбор, что у [`Column::sparse_get`], только целиком: там ищут
    /// один номер и бросают блок, здесь блок нужен весь. Держать два разбора
    /// одного формата нельзя — разойдутся молча, — поэтому точечное чтение
    /// остаётся точечным, а здесь идёт честный проход по приращениям.
    fn sparse_block(&self, b: usize, out: &mut Vec<(u32, i64)>) {
        out.clear();
        if b >= self.nblocks() {
            return;
        }
        let at = self.meta.u32_at(b * SMETA + 16) as usize;
        let base = ord(self.block_min(b));
        let count = BLOCK.min(self.values - b * BLOCK);
        let dw = self.data.u8_at(at) as u32;
        let dstart = at + 1;
        let dlen = (count * dw as usize).div_ceil(8);
        let vw = self.data.u8_at(dstart + dlen) as u32;
        let vstart = dstart + dlen + 1;
        let dsrc = self.data.slice(dstart, dlen + 16);
        let vsrc = self.data.slice(vstart, (count * vw as usize).div_ceil(8) + 16);
        // Приращения номеров идут от последнего номера **предыдущего** блока —
        // так их и записал кодировщик.
        let mut cur = if b == 0 { 0u32 } else { self.sparse_last(b - 1) };
        for i in 0..count {
            cur = cur.wrapping_add(value_at(dsrc, dw, i) as u32);
            out.push((cur, unord(base + value_at(vsrc, vw, i))));
        }
    }

    /// Гасит биты документов разреженной колонки со значением в `[lo, hi]`.
    ///
    /// Обход идёт по блокам значений, а не по номерам документов: это и есть
    /// единственный порядок, в котором разреженная колонка вообще хранится.
    /// Блоки упорядочены по номеру документа, поэтому биты ложатся по
    /// возрастанию — как и требует потребитель порций.
    ///
    /// Отсечений два, и оба нужны: блок мимо диапазона **значений** не
    /// читается вовсе, и блок, чьи документы целиком вне окна, — тоже. Первое
    /// экономит распаковку, второе — обход на каждом окне: без него окно из
    /// шестидесяти четырёх тысяч документов разбирало бы все блоки сегмента.
    fn sparse_select_window(
        &self,
        lo: i64,
        hi: i64,
        docs: std::ops::Range<usize>,
        base: u32,
        out: &mut [u64],
    ) -> usize {
        let n = self.nblocks();
        if n == 0 {
            return 0;
        }
        // Первый блок, чей последний номер дошёл до начала окна.
        let (mut first, mut last) = (0usize, n - 1);
        while first < last {
            let mid = (first + last) / 2;
            if (self.sparse_last(mid) as usize) < docs.start {
                first = mid + 1;
            } else {
                last = mid;
            }
        }
        let mut found = 0usize;
        let mut pairs: Vec<(u32, i64)> = Vec::with_capacity(BLOCK);
        for b in first..n {
            // Блок начинается за окном — и все следующие тоже: номера растут.
            let starts_at = if b == 0 { 0 } else { self.sparse_last(b - 1) as usize + 1 };
            if starts_at >= docs.end {
                break;
            }
            let (bmin, bmax) = (self.block_min(b), self.block_max(b));
            if bmin > bmax || bmax < lo || bmin > hi {
                continue;
            }
            self.sparse_block(b, &mut pairs);
            for &(d, v) in pairs.iter() {
                let at = d as usize;
                if at < docs.start || at >= docs.end || v < lo || v > hi {
                    continue;
                }
                let g = base as usize + (at - docs.start);
                out[g >> 6] |= 1u64 << (g & 63);
                found += 1;
            }
        }
        found
    }

    pub fn docs(&self) -> usize {
        self.docs
    }

    pub fn bytes(&self) -> usize {
        self.data.len_bytes()
            + self.meta.len_bytes()
            + self.has.as_ref().map_or(0, |h| h.len_bytes())
            + self.buckets.as_ref().map_or(0, |b| b.bytes())
    }

    pub fn warm(&self) -> usize {
        self.data.warm()
            + self.meta.warm()
            + self.has.as_ref().map_or(0, |h| h.warm())
            + self.buckets.as_ref().map_or(0, |b| b.warm())
    }

    /// Оценка числа кандидатов в диапазоне — по ней выбирается ведущий обхода.
    ///
    /// ## Что было и что измерено
    ///
    /// Прежде блок, задетый диапазоном, засчитывался целиком: оценка равнялась
    /// «задетых блоков × сто двадцать восемь». Довод был такой: оценка завышена,
    /// а завышена она в безопасную сторону, потому что недооценка увела бы обход
    /// туда, где кандидатов на самом деле больше.
    ///
    /// Довод верен, а мера — нет. Замерено на колонке в сто тысяч документов:
    ///
    /// | колонка | диапазон | правда | было | завышение |
    /// |---|---|---:|---:|---:|
    /// | плотная растущая | 1% | 1 000 | 1 024 | 1.0× |
    /// | плотная случайная (вёдра) | 1% | 1 000 | 1 001 | 1.0× |
    /// | **разреженная случайная** | 1% | 10 | 768 | **77×** |
    /// | **разреженная случайная** | 20% | 201 | 1 024 | **5.1×** |
    ///
    /// У плотной колонки промаха нет: там, где границы блоков не отсекают,
    /// движок строит вёдра, и оценка идёт по ним. Разреженной вёдра не строят
    /// никогда — блоков у неё и так во столько раз меньше, во сколько поле реже,
    /// — и у неё-то каждый блок и оказывается задет. Оценка вырождается в «все
    /// значения колонки», и отбор проигрывает выбор ведущего любому слову,
    /// которое встречается реже, чем поле заполнено. То есть отбор,
    /// отбирающий сотую часть, обход не ведёт.
    ///
    /// ## Что теперь
    ///
    /// Задетый блок засчитывается **долей**: во сколько диапазон перекрывает его
    /// границы, столько от него и берётся. Это уже не мажоранта, а ожидание — и
    /// цена ошибки здесь несимметрична не так, как казалось: промах вниз стоит
    /// одного лишнего обхода отбором вместо обхода словом, промах вверх стоит
    /// полного просмотра списка частого слова. Второе дороже.
    ///
    /// Доля берётся не меньше единицы: блок задет — значит хотя бы один
    /// кандидат в нём предполагается, иначе узкий диапазон дал бы ноль и
    /// «ведущего нет» стало бы неотличимо от «ведущий отбирает всё».
    pub fn estimate(&self, lo: i64, hi: i64) -> usize {
        if let Some(b) = self.buckets.as_ref() {
            return b.estimate(lo, hi);
        }
        let nblocks = self.nblocks();
        let mut hit = 0usize;
        for b in 0..nblocks {
            hit += share_of(self.block_min(b), self.block_max(b), lo, hi, BLOCK);
        }
        hit.min(self.docs)
    }

    /// Есть ли у документа значение.
    #[inline(always)]
    pub fn has(&self, doc: u32) -> bool {
        if self.layout == Layout::Sparse {
            return self.sparse_get(doc).is_some();
        }
        match self.has.as_ref() {
            // Файла нет — значение есть у каждого; проверять нечего.
            None => (doc as usize) < self.docs,
            Some(h) => h.u8_at(doc as usize >> 3) >> (doc & 7) & 1 == 1,
        }
    }

    /// Читатель с памятью о блоке — для прохода подряд.
    ///
    /// ## Зачем он, если есть `get`
    ///
    /// `get` каждый раз заново читает заголовок блока: смещение, ширину,
    /// наименьшее значение — и строит срез до конца файла. На блок из ста
    /// двадцати восьми документов это сто двадцать восемь повторов одной и той
    /// же работы.
    ///
    /// Читатель помнит текущий блок и переучитывает заголовок только при
    /// переходе в следующий. Порядок обращений произвольный: попадание не в
    /// свой блок просто перечитывает заголовок, то есть вырождается в `get`.
    #[inline]
    pub fn get_with(&self, c: &mut BlockCache, doc: u32) -> Option<i64> {
        if self.layout == Layout::Sparse {
            return self.sparse_get(doc);
        }
        if doc as usize >= self.docs || !self.has(doc) {
            return None;
        }
        let b = doc as usize / BLOCK;
        if b != c.block {
            let (start, width) = self.block_at(b);
            let lo = self.block_min(b);
            c.block = b;
            c.start = start;
            c.width = width;
            c.base = if lo == i64::MAX { 0 } else { ord(lo) };
        }
        let raw = value_at(
            self.data.slice(c.start, self.data.len_bytes() - c.start),
            c.width,
            doc as usize % BLOCK,
        );
        Some(unord(c.base.wrapping_add(raw)))
    }

    /// Значение документа. `None` — значения нет.
    #[inline(always)]
    pub fn get(&self, doc: u32) -> Option<i64> {
        if self.layout == Layout::Sparse {
            return self.sparse_get(doc);
        }
        if doc as usize >= self.docs || !self.has(doc) {
            return None;
        }
        let b = doc as usize / BLOCK;
        let (start, width) = self.block_at(b);
        let lo = self.block_min(b);
        let base = if lo == i64::MAX { 0 } else { ord(lo) };
        let raw = value_at(
            self.data.slice(start, self.data.len_bytes() - start),
            width,
            doc as usize % BLOCK,
        );
        Some(unord(base.wrapping_add(raw)))
    }

    #[inline(always)]
    fn block_min(&self, b: usize) -> i64 {
        self.meta.u64_at(b * self.stride()) as i64
    }

    #[inline(always)]
    fn block_max(&self, b: usize) -> i64 {
        self.meta.u64_at(b * self.stride() + 8) as i64
    }

    /// Смещение байта ширины блока и сама ширина.
    #[inline(always)]
    fn block_at(&self, b: usize) -> (usize, u32) {
        let at = self.meta.u32_at(b * META + 16) as usize;
        (at + 1, self.data.u8_at(at) as u32)
    }

    /// Гасит в `out` биты документов со значением в `[lo, hi]`.
    ///
    /// `base` — смещение номеров документов сегмента в общем пространстве.
    /// Отдаёт число найденных.
    pub fn select(&self, lo: i64, hi: i64, base: u32, out: &mut [u64]) -> usize {
        self.select_window(lo, hi, 0..self.docs, base, out)
    }

    /// То же по окну номеров: биты ложатся от начала окна, а не от начала
    /// сегмента.
    ///
    /// ## Зачем окно
    ///
    /// Без него отбор по диапазону выделял битовую карту на **весь** сегмент и
    /// собирал в память всех подошедших разом. На сегменте в пятьдесят миллионов
    /// документов это шесть мегабайт карты и до двухсот мегабайт номеров — на
    /// каждый сегмент и на каждый запрос, независимо от того, сколько документов
    /// нужно клиенту. Запрос вида «всё, у чего есть цена» стоил бы столько же,
    /// сколько весь индекс.
    ///
    /// Окно снимает и то и другое: и карта, и список кандидатов становятся
    /// величиной с окно, а не с сегментом. Порядок при этом сохраняется —
    /// окна идут по возрастанию номеров, и курсоры дерева условий, которые
    /// назад не ходят, этого не замечают.
    pub fn select_window(
        &self,
        lo: i64,
        hi: i64,
        docs: std::ops::Range<usize>,
        base: u32,
        out: &mut [u64],
    ) -> usize {
        let end = docs.end.min(self.docs);
        if docs.start >= end {
            return 0;
        }
        // У разреженной колонки блок — это блок **значений**, а не документов.
        //
        // Ниже блоки перебираются как отрезки номеров документов: `b`-й блок
        // считается покрывающим документы `b·BLOCK .. b·BLOCK+BLOCK`. У плотной
        // колонки это правда по построению, у разреженной — нет: там в блоке
        // лежат сто двадцать восемь **присутствующих** значений, чьи номера
        // разбросаны по всему сегменту.
        //
        // Пока это различие не соблюдалось, отбор по диапазону на разреженном
        // поле отдавал **ноль** документов вместо всех подходящих: границы
        // значений сверялись с одними блоками, а номера брались из других.
        // Молча — «не нашлось» выглядит как законный ответ.
        if self.layout == Layout::Sparse {
            return self.sparse_select_window(lo, hi, docs.start..end, base, out);
        }
        // Блоки, накрывающие окно: метаданные блока по-прежнему отсекают те, чьи
        // значения мимо диапазона, — окно сужает обход, а не заменяет отсечение.
        let first = docs.start / BLOCK;
        let last = (end - 1) / BLOCK;
        let mut found = 0usize;
        let mut cache = BlockCache::default();
        for b in first..=last {
            let (bmin, bmax) = (self.block_min(b), self.block_max(b));
            // Блок целиком мимо — не читается вовсе. Ради этого метаданные и есть.
            if bmax < lo || bmin > hi || bmin > bmax {
                continue;
            }
            let from = (b * BLOCK).max(docs.start);
            let to = (b * BLOCK + BLOCK).min(end);
            let covered = bmin >= lo && bmax <= hi;
            for doc in from..to {
                let d = doc as u32;
                // Границы доказывают, что каждое присутствующее значение
                // блока подходит. Missing всё равно проверяется по bitmap.
                let matches = if covered {
                    self.has(d)
                } else {
                    self.get_with(&mut cache, d).is_some_and(|v| lo <= v && v <= hi)
                };
                if matches {
                    let g = base as usize + (doc - docs.start);
                    out[g >> 6] |= 1u64 << (g & 63);
                    found += 1;
                }
            }
        }
        found
    }

    /// Документы в порядке значения, порциями, с границей оставшегося.
    ///
    /// Отдаёт очередную порцию документов и **лучшее из возможного** среди тех,
    /// что ещё не отданы. По этой границе обход и обрывается: когда она хуже
    /// k-го собранного, дальше идти незачем.
    ///
    /// Ничего нового для этого не хранится. У поля с корзинами порядок задают
    /// сами корзины — `bucket_of` монотонна по значению. У поля без корзин
    /// (а это ровно те поля, где значение растёт вместе с номером документа)
    /// порядок задают границы блоков из `.cmeta`.
    pub fn in_value_order(&self, desc: bool) -> ValueWalk<'_> {
        // Bound относится только к присутствующим числам, missing здесь нет.
        // Поэтому нормированный ключ заведомо помещается в i64: metadata
        // каждого блока не нужно расширять ради отдельного missing.
        let edge =
            |lo: i64, hi: i64| crate::search::norm_key(if desc { hi } else { lo }, desc) as i64;
        let mut order: Vec<(i64, usize)> = match self.buckets.as_ref() {
            // Корзины уже упорядочены по значению: `bucket_of` монотонна.
            Some(b) => (0..b.count())
                .filter(|&k| b.len_of(k) > 0)
                .map(|k| {
                    let (lo, hi) = b.edges_of(k);
                    (edge(lo, hi), k)
                })
                .collect(),
            None => (0..self.docs.div_ceil(BLOCK))
                .filter(|&i| self.block_min(i) <= self.block_max(i))
                .map(|i| (edge(self.block_min(i), self.block_max(i)), i))
                .collect(),
        };
        // Лучшее вперёд — то есть по убыванию нормированной границы.
        order.sort_unstable_by_key(|a| std::cmp::Reverse(a.0));
        ValueWalk { col: self, order, at: 0 }
    }

    /// Во сколько раз средний размах блока меньше общего.
    pub fn spread(&self) -> f64 {
        spread_of((0..self.docs.div_ceil(BLOCK)).map(|b| (self.block_min(b), self.block_max(b))))
    }
}

/// Обход документов в порядке значения — порция за порцией.
pub struct ValueWalk<'a> {
    col: &'a Column,
    /// Куски (корзины или блоки) в порядке от лучшего к худшему.
    order: Vec<(i64, usize)>,
    at: usize,
}

impl<'a> ValueWalk<'a> {
    /// Сколько кусков всего — по нему прикидывается размер пачки.
    pub fn chunks(&self) -> usize {
        self.order.len()
    }

    /// Лучшая **нормированная** граница среди ещё не отданных кусков.
    ///
    /// Сравнима напрямую с первым ключом верхушки. `None` — отдано всё.
    pub fn bound(&self) -> Option<i64> {
        self.order.get(self.at).map(|&(v, _)| v)
    }

    /// Следующая порция документов. Пусто — обход кончился.
    ///
    /// Внутри порции документы **не** упорядочены по значению: кусок отдаётся
    /// целиком, а досортировку делает верхушка. Досортировывать здесь значило
    /// бы сортировать то, что верхушка всё равно отбросит.
    pub fn next(&mut self, out: &mut Vec<u32>) -> bool {
        out.clear();
        let Some(&(_, i)) = self.order.get(self.at) else { return false };
        self.at += 1;
        match self.col.buckets.as_ref() {
            Some(b) => b.docs(i, out),
            None => {
                let from = i * BLOCK;
                let to = (from + BLOCK).min(self.col.docs);
                for d in from..to {
                    if self.col.has(d as u32) {
                        out.push(d as u32);
                    }
                }
            }
        }
        true
    }
}

/// Порог признака, за которым пропуск по границам блока перестаёт работать.
pub const SPREAD_USELESS: f64 = 0.01;

// ---------------------------------------------------------------------------
// Корзины значений: надстройка для полей, где пропуск по блокам не отсекает.
//
// У поля, растущего вместе с порядком приёма, диапазон в один процент задевает
// один процент блоков, и надстройка не нужна вовсе. У поля со случайными
// значениями — все сто, и без надстройки любой отбор вырождается в полный
// проход колонки. Разница между этими случаями пять порядков (`spread`), и по
// ней движок решает сам, строить корзины или нет.
//
// Устройство: значение огрубляется до номера корзины, у корзины лежит список
// её документов приращениями. Диапазон — списки целиком попавших корзин плюс
// уточнение по колонке на двух краевых.
// ---------------------------------------------------------------------------

pub const BUCKETS: usize = 16_384;

/// Байт на корзину в указателе: смещение и число документов.
const BKT_META: usize = 8;

/// Сколько корзин завести полю с такими данными.
///
/// ## Почему это не постоянная
///
/// Ограничений два, и оба выведены из данных, а не из вкуса:
///
/// * **размах**. Корзина мельче одного значения ничего не разделяет: у поля со
///   значениями от нуля до четырёх больше пяти корзин не бывает, сколько их ни
///   заводи. Это же покрывает логическое поле, признак, тип и версию — всё, что
///   в разреженных схемах встречается чаще всего.
/// * **число значений**. Около четырёх документов на корзину; дальше указатель
///   растёт, а отсечение нет.
pub fn bucket_count(lo: i64, hi: i64, values: usize) -> usize {
    if values == 0 || lo > hi {
        return 1;
    }
    let by_range = (ord(hi) - ord(lo)).saturating_add(1).min(BUCKETS as u64) as usize;
    let by_values = (values / 4).max(1);
    by_range.min(by_values).clamp(1, BUCKETS)
}

/// Номер корзины по значению.
#[inline(always)]
fn bucket_of(v: i64, base: u64, span: u64, count: usize) -> usize {
    let u = ord(v);
    if u <= base {
        return 0;
    }
    (((u - base) / span) as usize).min(count - 1)
}

/// Собирает корзины. Отдаёт списки и указатель на них.
pub fn encode_buckets(vals: &[i64], has: &[bool]) -> (Vec<u8>, Vec<u8>) {
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    for (i, &v) in vals.iter().enumerate() {
        if has[i] {
            lo = lo.min(v);
            hi = hi.max(v);
        }
    }
    let values = has.iter().filter(|&&y| y).count();
    let count = bucket_count(lo, hi, values);
    // Шаг считается насыщением, а не сложением.
    //
    // При размахе во весь `i64` и одном ведре `(ord(hi) − ord(lo)) / 1 + 1` —
    // это `u64::MAX + 1`. В отладочной сборке здесь была паника, в выпускной
    // ноль, и следующий же `bucket_of` делил на ноль — то есть узел умирал на
    // сбросе сегмента, а выпускной профиль объявлен `panic = "abort"`.
    // Достаточно было поля, заполненного меньше чем у восьми документов и
    // несущего оба края разрядной сетки.
    let (base, span) = if lo > hi {
        (0u64, 1u64)
    } else {
        (ord(lo), ((ord(hi) - ord(lo)) / count as u64).saturating_add(1))
    };

    let mut lists: Vec<Vec<u32>> = vec![Vec::new(); count];
    for (doc, &v) in vals.iter().enumerate() {
        if has[doc] {
            lists[bucket_of(v, base, span, count)].push(doc as u32);
        }
    }

    let mut data = Vec::new();
    let mut idx = Vec::with_capacity(16 + count * BKT_META);
    idx.extend(base.to_le_bytes());
    idx.extend(span.to_le_bytes());
    let mut deltas: Vec<u32> = Vec::with_capacity(crate::bits::BLOCK);
    for l in lists.iter() {
        idx.extend((data.len() as u32).to_le_bytes());
        idx.extend((l.len() as u32).to_le_bytes());
        let mut prev = 0u32;
        for chunk in l.chunks(crate::bits::BLOCK) {
            deltas.clear();
            for &d in chunk {
                deltas.push(d - prev);
                prev = d;
            }
            let mut bp = 0u64;
            crate::bits::pack_values(&mut data, &deltas, &mut bp);
        }
    }
    (data, idx)
}

/// Отображённые корзины.
pub struct Buckets {
    data: Region,
    idx: Region,
    base: u64,
    span: u64,
    count: usize,
}

impl Buckets {
    pub fn open(
        src: &crate::container::SegmentSource,
        field: &str,
        docs: usize,
    ) -> Option<Buckets> {
        let data = src.try_open(&format!("{field}.bkt"))?;
        let idx = src.try_open(&format!("{field}.bktidx"))?;
        if idx.len_bytes() < 16 + BKT_META || (idx.len_bytes() - 16) % BKT_META != 0 {
            return None;
        }
        let count = (idx.len_bytes() - 16) / BKT_META;
        if count == 0 || count > BUCKETS {
            return None;
        }
        // Указатель целиком недоверенный: каждая запись проверяется до того,
        // как попадёт на горячий путь обхода. Особенно важны `n <= docs` и
        // фактическая длина упакованных блоков — иначе четыре байта метаданных
        // превращались в миллиарды итераций по нулевому отображению.
        let mut expected = 0usize;
        let mut total = 0usize;
        for b in 0..count {
            let meta = 16 + b * BKT_META;
            let off = idx.u32_at(meta) as usize;
            let n = idx.u32_at(meta + 4) as usize;
            total = total.checked_add(n)?;
            if off != expected || n > docs || total > docs {
                return None;
            }
            let mut at = off;
            let mut left = n;
            let mut prev = 0u32;
            let mut seen = 0usize;
            while left > 0 {
                let take = left.min(crate::bits::BLOCK);
                if at >= data.len_bytes() {
                    return None;
                }
                let width = crate::bits::width_of(data.slice(at, 1));
                if width > 32 {
                    return None;
                }
                let packed = crate::bits::packed_len(take, width);
                let end = at.checked_add(packed)?;
                if end > data.len_bytes() {
                    return None;
                }
                let src = data.slice(at + 1, crate::bits::window_len(take, width));
                for i in 0..take {
                    let delta = crate::bits::value_at(src, width, i);
                    if seen > 0 && delta == 0 {
                        return None;
                    }
                    prev = prev.checked_add(delta)?;
                    if prev as usize >= docs {
                        return None;
                    }
                    seen += 1;
                }
                at = end;
                left -= take;
            }
            expected = at;
        }
        if expected != data.len_bytes() {
            return None;
        }
        let base = idx.u64_at(0);
        let span = idx.u64_at(8).max(1);
        Some(Buckets { data, idx, base, span, count })
    }

    /// Сколько корзин у этого поля в этом сегменте.
    pub fn count(&self) -> usize {
        self.count
    }

    pub fn bytes(&self) -> usize {
        self.data.len_bytes() + self.idx.len_bytes()
    }

    pub fn warm(&self) -> usize {
        self.data.warm() + self.idx.warm()
    }

    #[inline]
    fn at(&self, b: usize) -> (usize, usize) {
        if b >= self.count {
            return (0, 0);
        }
        let off = 16 + b * BKT_META;
        (self.idx.u32_at(off) as usize, self.idx.u32_at(off + 4) as usize)
    }

    /// Какие корзины накрывает диапазон и сколько в них документов.
    ///
    /// Второе — оценка числа кандидатов, по которой решается, кому вести обход.
    /// Она завышена ровно на краевые корзины, и завышена **в правильную
    /// сторону**: недооценка увела бы обход туда, где кандидатов больше.
    pub fn span_of(&self, lo: i64, hi: i64) -> (usize, usize, usize) {
        let first = bucket_of(lo, self.base, self.span, self.count);
        let last = bucket_of(hi, self.base, self.span, self.count);
        let mut n = 0usize;
        for b in first..=last {
            n += self.at(b).1;
        }
        (first, last, n)
    }

    /// Оценка числа кандидатов в диапазоне.
    ///
    /// Отдельно от [`Buckets::span_of`], потому что вопросы разные. `span_of`
    /// отвечает «сколько документов придётся перебрать, если вести корзинами» —
    /// там нужны целые корзины, включая краевые целиком. Здесь вопрос «сколько
    /// из них подойдёт», и краевая корзина засчитывается долей.
    ///
    /// Разница видна на пустом ответе. `bucket_of` прижимает номер к краю, и
    /// диапазон, лежащий целиком выше всех значений, попадал в последнюю
    /// корзину и оценивался её размером. Замерено: у поля из двух значений
    /// диапазон `[40000, 60000]` при нуле подходящих оценивался в пятьдесят
    /// тысяч — то есть самым дорогим из возможных, и обход уходил вести словом
    /// ровно там, где отбор не даёт ни одного документа.
    pub fn estimate(&self, lo: i64, hi: i64) -> usize {
        let (first, last, _) = self.span_of(lo, hi);
        let mut n = 0usize;
        for b in first..=last {
            let (blo, bhi) = self.edges_of(b);
            n += share_of(blo, bhi, lo, hi, self.at(b).1);
        }
        n
    }

    /// Сколько документов в корзине.
    pub fn len_of(&self, b: usize) -> usize {
        self.at(b).1
    }

    /// Края диапазона корзины: наименьшее и наибольшее возможное значение.
    pub fn edges_of(&self, b: usize) -> (i64, i64) {
        let lo = self.base.saturating_add((b as u64).saturating_mul(self.span));
        let hi =
            self.base.saturating_add((b as u64 + 1).saturating_mul(self.span)).saturating_sub(1);
        (unord(lo), unord(hi))
    }

    /// Что ведро **может держать** — с учётом прижатия по краям.
    ///
    /// ## Чем это отличается от [`Buckets::edges_of`]
    ///
    /// `edges_of` говорит, где ведро лежит на **сетке**: начало плюс столько-то
    /// шагов. Этого довольно, чтобы упорядочить вёдра по значению и прикинуть,
    /// сколько в них попадёт. Но это не то же, что «какие значения в нём
    /// лежат»: [`bucket_of`] **прижимает** — всё ниже начала сетки идёт в
    /// нулевое ведро, всё выше конца в последнее. У крайних вёдер настоящие
    /// границы поэтому открыты.
    ///
    /// ## Почему разница перестала быть безобидной
    ///
    /// Пока края служили порядку и оценке, промах стоил лишнего чтения. Счёт по
    /// заголовкам (Ф5.4) сделал из края **обещание**: ведро, чьи края лежат
    /// внутри отбора, засчитывается целиком, не читая ни одного значения.
    ///
    /// И край врал. Верхний строится как `начало + (b+1)·шаг − 1` в
    /// беззнаковом пространстве; у колонки, чей максимум равен ровно
    /// `i64::MAX`, эта сумма упирается в потолок `u64`, насыщается — и
    /// объявленный край выходит на единицу **ниже** значения, которое ведро
    /// держит. Проверено: документ со значением `i64::MAX` засчитывался отбором
    /// `[i64::MIN, i64::MAX − 1]`, который его исключает.
    ///
    /// Арифметика, которой это разбиралось на бумаге, была верна в целых
    /// числах и неверна в `u64`: `ord(hi) + 1` не всегда представимо.
    pub fn covers(&self, b: usize) -> (i64, i64) {
        let (lo, hi) = self.edges_of(b);
        (if b == 0 { i64::MIN } else { lo }, if b + 1 >= self.count { i64::MAX } else { hi })
    }

    /// Документы корзины по возрастанию номера.
    pub fn docs(&self, b: usize, out: &mut Vec<u32>) {
        out.clear();
        let (off, n) = self.at(b);
        if n == 0 {
            return;
        }
        let mut prev = 0u32;
        let mut at = off;
        let mut left = n;
        while left > 0 {
            let take = left.min(crate::bits::BLOCK);
            let width = crate::bits::width_of(self.data.slice(at, 1));
            let src = self.data.slice(at + 1, crate::bits::window_len(take, width));
            for i in 0..take {
                prev += crate::bits::value_at(src, width, i);
                out.push(prev);
            }
            at += crate::bits::packed_len(take, width);
            left -= take;
        }
    }
}

/// Сколько из `pop` документов куска попадает в диапазон — по доле перекрытия.
///
/// Кусок (блок или ведро) держит `pop` документов, чьи значения лежат между
/// `bmin` и `bmax`. Про их распределение внутри этих границ не известно ничего,
/// и ровное — единственное предположение, которое не требует хранить лишнего.
///
/// Перекрытия нет — ноль, и это важнее, чем кажется: диапазон, целиком лежащий
/// мимо всех значений, обязан оцениваться нулём, иначе он выглядит самым
/// дорогим и обход уходит вести словом. Перекрытие есть — не меньше единицы,
/// иначе узкий диапазон дал бы ноль, и «кандидатов нет» стало бы неотличимо от
/// «ведущего выбрать не по чему».
#[inline]
fn share_of(bmin: i64, bmax: i64, lo: i64, hi: i64, pop: usize) -> usize {
    if pop == 0 || bmin > bmax || bmax < lo || bmin > hi {
        return 0;
    }
    // Ширины считаются в `i128`: у куска с границами от `i64::MIN` до
    // `i64::MAX` разность не помещается в `i64`, и вычитание переполнилось бы.
    let span = (bmax as i128 - bmin as i128) + 1;
    if span <= 1 {
        return pop;
    }
    let cut = (hi.min(bmax) as i128 - lo.max(bmin) as i128) + 1;
    if cut <= 0 {
        return 0;
    }
    ((pop as i128 * cut / span) as usize).clamp(1, pop)
}

/// Тот же признак по готовым границам блоков — нужен на сбросе, до отображения.
pub fn spread_of(blocks: impl Iterator<Item = (i64, i64)>) -> f64 {
    let mut gmin = i64::MAX;
    let mut gmax = i64::MIN;
    let mut sum = 0f64;
    let mut n = 0usize;
    for (lo, hi) in blocks {
        if lo > hi {
            continue;
        }
        gmin = gmin.min(lo);
        gmax = gmax.max(hi);
        sum += (hi as i128 - lo as i128) as f64;
        n += 1;
    }
    if n == 0 || gmin > gmax {
        return 1.0;
    }
    let global = ((gmax as i128 - gmin as i128) as f64).max(1.0);
    sum / n as f64 / global
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn build(name: &str, vals: &[i64], has: &[bool]) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-column-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (data, meta) = encode(vals, has);
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("p.col", &data);
        w("p.cmeta", &meta);
        w("p.has", &encode_presence(has));
        dir.to_string_lossy().to_string()
    }

    #[test]
    fn every_value_survives_the_round_trip() {
        let vals: Vec<i64> = vec![
            0,
            1,
            -1,
            i64::MIN,
            i64::MAX,
            i64::MIN + 1,
            i64::MAX - 1,
            -1_000_000_000_000,
            1_700_000_000_000,
        ];
        let has = vec![true; vals.len()];
        let dir = build("roundtrip", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", vals.len(), Presence::Bits)
            .expect("колонка открылась");
        for (d, &v) in vals.iter().enumerate() {
            assert_eq!(c.get(d as u32), Some(v), "документ {d}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Больше одного блока: границы и ширины у блоков свои.
    #[test]
    fn many_blocks_keep_their_own_width() {
        let n = BLOCK * 5 + 7;
        let vals: Vec<i64> = (0..n).map(|i| (i as i64) * 1_000_003 - 500).collect();
        let has = vec![true; n];
        let dir = build("blocks", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");
        for (d, &v) in vals.iter().enumerate() {
            assert_eq!(c.get(d as u32), Some(v), "документ {d}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Документ без значения не попадает ни в один диапазон.
    #[test]
    fn a_document_without_a_value_matches_no_range() {
        let n = 300;
        let vals: Vec<i64> = (0..n).map(|i| i as i64).collect();
        let has: Vec<bool> = (0..n).map(|i| i % 3 != 0).collect();
        let dir = build("missing", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");

        #[allow(clippy::needless_range_loop)] // индекс сам участвует в вычислении
        for d in 0..n {
            assert_eq!(c.has(d as u32), has[d]);
            assert_eq!(c.get(d as u32).is_some(), has[d]);
        }
        let mut bits = vec![0u64; n.div_ceil(64)];
        let found = c.select(i64::MIN, i64::MAX, 0, &mut bits);
        assert_eq!(found, has.iter().filter(|&&x| x).count(), "взяты только имеющие значение");
        for d in 0..n {
            let live = bits[d >> 6] >> (d & 63) & 1 == 1;
            assert_eq!(live, has[d], "документ {d}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отбор обязан совпасть с честным перебором на всех границах.
    #[test]
    fn selection_matches_the_honest_walk() {
        let n = BLOCK * 7 + 13;
        let mut seed = 12345u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let vals: Vec<i64> = (0..n).map(|_| (next() % 1000) as i64 - 500).collect();
        let has: Vec<bool> = (0..n).map(|_| next() % 5 != 0).collect();
        let dir = build("select", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");

        for (lo, hi) in [(-500i64, 499i64), (0, 0), (-1, 1), (100, 200), (499, 499), (600, 700)] {
            let mut bits = vec![0u64; n.div_ceil(64)];
            let found = c.select(lo, hi, 0, &mut bits);
            let want =
                (0..n).filter(|&d| has[d] && vals[d] >= lo && vals[d] <= hi).collect::<Vec<_>>();
            assert_eq!(found, want.len(), "диапазон [{lo}, {hi}]");
            for d in want {
                assert!(bits[d >> 6] >> (d & 63) & 1 == 1, "документ {d} в [{lo}, {hi}]");
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Метаданные блока лежат в файле целиком, включая смещение.
    ///
    /// Проверка ровно того инварианта, ради которого смещение туда и переехало:
    /// открытие сегмента не должно ничего копить в памяти процесса.
    #[test]
    fn block_metadata_lives_in_the_file_not_in_memory() {
        let n = BLOCK * 4 + 5;
        // Ширины у блоков разные — если бы смещение выводилось сложением, любая
        // ошибка в этом сложении здесь бы и вылезла.
        let vals: Vec<i64> = (0..n)
            .map(|i| match i / BLOCK {
                0 => 0,
                1 => i as i64,
                2 => i as i64 * 1_000_000,
                _ => i as i64 * 1_000_000_000_000,
            })
            .collect();
        let has = vec![true; n];
        let dir = build("meta", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");
        assert_eq!(
            std::fs::metadata(format!("{dir}/p.cmeta")).unwrap().len() as usize,
            n.div_ceil(BLOCK) * META
        );
        for (d, &v) in vals.iter().enumerate() {
            assert_eq!(c.get(d as u32), Some(v), "документ {d}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Признак различает те самые два случая, ради которых заведён.
    ///
    /// Блоков берётся столько, сколько их в настоящем сегменте: у идеально
    /// растущего поля признак равен примерно единице на число блоков, и на
    /// сотне блоков он вырождается в сам порог. Целевой сегмент в миллион
    /// документов — это 7813 блоков, то есть 1.3·10⁻⁴, с запасом ниже порога.
    #[test]
    fn spread_tells_the_two_cases_apart() {
        let n = BLOCK * 8000;
        // Растущее вместе с номером — метка времени.
        let rising: Vec<(i64, i64)> = (0..n / BLOCK)
            .map(|b| ((b * BLOCK) as i64 * 1000, ((b + 1) * BLOCK) as i64 * 1000))
            .collect();
        // Случайное — каждый блок накрывает почти весь размах.
        let random: Vec<(i64, i64)> = (0..n / BLOCK).map(|_| (0i64, 1_000_000i64)).collect();

        let a = spread_of(rising.into_iter());
        let b = spread_of(random.into_iter());
        assert!(a < SPREAD_USELESS, "растущее поле: признак {a:e} обязан быть мал");
        assert!(b >= SPREAD_USELESS, "случайное поле: признак {b:e} обязан быть велик");
        assert!(b / a > 100.0, "признак обязан различать случаи, а не колебаться");
    }

    /// Блок, где ни у одного документа нет значения, не пересекает ничего.
    #[test]
    fn an_empty_block_intersects_nothing() {
        let n = BLOCK * 2;
        let vals = vec![7i64; n];
        // Второй блок целиком без значений.
        let has: Vec<bool> = (0..n).map(|d| d < BLOCK).collect();
        let dir = build("empty", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");
        let mut bits = vec![0u64; n.div_ceil(64)];
        assert_eq!(c.select(i64::MIN, i64::MAX, 0, &mut bits), BLOCK);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Быстрое чтение словом и побайтовое дают один и тот же бит.
    ///
    /// ## Почему это отдельный тест
    ///
    /// [`value_at`] читает шестнадцать байт одним невыровненным словом, а на
    /// хвосте файла — по байту. Две ветви, один ответ: разойдись они, колонка
    /// отдавала бы **правдоподобные** числа, отличающиеся от записанных, и ни
    /// отказа, ни строки в журнале при этом не будет. Поймать такое можно
    /// только сравнением ветвей между собой.
    ///
    /// Перебираются все ширины от нуля до шестидесяти четырёх и все позиции в
    /// блоке; хвост берётся усечением того же среза, чтобы обе ветви читали
    /// заведомо одни и те же байты.
    #[test]
    fn both_ways_of_reading_a_value_agree() {
        // Достаточно, чтобы шестнадцать байт нашлись для любой позиции блока:
        // 128 значений по 64 разряда — это 1024 байта, плюс запас на слово.
        let mut src = vec![0u8; 128 * 8 + 32];
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for b in src.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = (x >> 24) as u8;
        }
        let mut checked = 0usize;
        for width in 0..=64u32 {
            for i in 0..BLOCK {
                let bit = i * width as usize;
                let byte = bit >> 3;
                if byte + 16 > src.len() {
                    continue;
                }
                let fast = value_at(&src, width, i);
                // Тот же ответ, но заведомо через хвостовую ветвь: срез
                // обрезается так, что шестнадцати байт уже нет.
                let cut = (byte + 16).min(src.len()) - 1;
                let slow = {
                    let shift = (bit & 7) as u32;
                    let w = value_tail(&src[..cut], byte);
                    if width == 0 {
                        0
                    } else {
                        let mask: u128 =
                            if width >= 128 { u128::MAX } else { (1u128 << width) - 1 };
                        ((w >> shift) & mask) as u64
                    }
                };
                assert_eq!(
                    fast, slow,
                    "ширина {width}, позиция {i}: словом {fast}, побайтно {slow}"
                );
                checked += 1;
            }
        }
        assert!(checked > 4000, "проверено всего {checked} сочетаний");
    }
}

#[cfg(test)]
mod bucket_tests {
    use super::*;
    use std::io::Write;

    fn build(name: &str, vals: &[i64], has: &[bool]) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-bkt-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (data, meta) = encode(vals, has);
        let (bkt, bidx) = encode_buckets(vals, has);
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("p.col", &data);
        w("p.cmeta", &meta);
        w("p.has", &encode_presence(has));
        w("p.bkt", &bkt);
        w("p.bktidx", &bidx);
        dir.to_string_lossy().to_string()
    }

    #[test]
    fn untrusted_bucket_counts_are_rejected_before_the_decode_loop() {
        let vals: Vec<i64> = (0..32).collect();
        let has = vec![true; vals.len()];
        let dir = build("bad-count", &vals, &has);
        let mut idx = Vec::new();
        idx.extend(0u64.to_le_bytes());
        idx.extend(1u64.to_le_bytes());
        idx.extend(0u32.to_le_bytes());
        idx.extend(u32::MAX.to_le_bytes());
        std::fs::write(format!("{dir}/p.bktidx"), idx).unwrap();
        let column =
            Column::open(&crate::container::loose(&dir), "p", vals.len(), Presence::Bits).unwrap();
        assert!(column.buckets.is_none(), "повреждённый указатель дошёл до обхода");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn covered_blocks_and_partial_windows_match_raw_values_with_missing() {
        let n = BLOCK * 4 + 37;
        let vals: Vec<i64> = (0..n)
            .map(|i| match i % 131 {
                0 => i64::MIN,
                1 => i64::MAX,
                _ => (i % 97) as i64 - 48,
            })
            .collect();
        for all in [false, true] {
            let has: Vec<bool> =
                (0..n).map(|i| all || (i % 5 != 0 && !(BLOCK..2 * BLOCK).contains(&i))).collect();
            let dir = build(&format!("covered-{all}"), &vals, &has);
            let column = Column::open(
                &crate::container::loose(&dir),
                "p",
                n,
                if all { Presence::All } else { Presence::Bits },
            )
            .unwrap();
            for (lo, hi) in [
                (i64::MIN, i64::MAX),
                (-100, 100),
                (-3, 9),
                (i64::MAX, i64::MAX),
                (i64::MIN, i64::MIN),
            ] {
                for (start, end) in
                    [(0, n), (17, 129), (BLOCK + 3, 2 * BLOCK), (2 * BLOCK + 7, n + 15)]
                {
                    let base = 11;
                    let mut bits = vec![0u64; (base + end - start).div_ceil(64)];
                    let count = column.select_window(lo, hi, start..end, base as u32, &mut bits);
                    let mut expected = 0;
                    for offset in 0..(end - start) {
                        let doc = start + offset;
                        let matches = doc < n && has[doc] && lo <= vals[doc] && vals[doc] <= hi;
                        expected += usize::from(matches);
                        assert_eq!(
                            (bits[(base + offset) >> 6] >> ((base + offset) & 63)) & 1,
                            u64::from(matches),
                            "doc={doc}, range={lo}..{hi}, all={all}"
                        );
                    }
                    assert_eq!(count, expected);
                    assert_eq!(bits[0] & ((1 << base) - 1), 0);
                }
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    /// Обход окнами отдаёт ровно то же, что обход целиком.
    ///
    /// На этом стоит вся выгода окна: отбор перестал выделять память под весь
    /// сегмент, и если бы разбиение хоть где-то теряло документ на границе, это
    /// была бы не экономия, а тихая потеря выдачи. Границы проверяются нарочно
    /// несовпадающие с блоком и со словом карты.
    #[test]
    fn windows_select_exactly_what_a_whole_pass_selects() {
        let n = BLOCK * 3 + 37;
        let vals: Vec<i64> = (0..n).map(|i| (i as i64 * 7) % 500).collect();
        let has = vec![true; vals.len()];
        let d = build("окно", &vals, &has);
        let c = Column::open(&crate::container::loose(&d), "p", vals.len(), Presence::Bits)
            .expect("колонка");

        for (lo, hi) in [(i64::MIN, i64::MAX), (0, 0), (100, 300), (499, 499), (600, 700)] {
            let mut whole = vec![0u64; n.div_ceil(64)];
            let want = c.select(lo, hi, 0, &mut whole);

            for step in [1usize, 64, 100, BLOCK, BLOCK + 1, n] {
                let mut got = 0usize;
                let mut seen: Vec<u32> = Vec::new();
                let mut at = 0usize;
                while at < n {
                    let end = (at + step).min(n);
                    let mut bits = vec![0u64; step.div_ceil(64)];
                    got += c.select_window(lo, hi, at..end, 0, &mut bits);
                    for (w, word) in bits.iter().enumerate() {
                        let mut x = *word;
                        while x != 0 {
                            let b = x.trailing_zeros() as usize;
                            seen.push((at + w * 64 + b) as u32);
                            x &= x - 1;
                        }
                    }
                    at = end;
                }
                assert_eq!(got, want, "диапазон {lo}..{hi}, окно {step}: число разошлось");
                let mut want_docs: Vec<u32> = Vec::new();
                for (w, word) in whole.iter().enumerate() {
                    let mut x = *word;
                    while x != 0 {
                        let b = x.trailing_zeros() as usize;
                        want_docs.push((w * 64 + b) as u32);
                        x &= x - 1;
                    }
                }
                assert_eq!(
                    seen, want_docs,
                    "диапазон {lo}..{hi}, окно {step}: документы разошлись"
                );
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Корзины обязаны отдать ровно тех же, что честный перебор колонки.
    ///
    /// Проверяются края: границы корзин, диапазон шире всего поля, диапазон
    /// целиком мимо, и одиночное значение.
    #[test]
    fn buckets_return_exactly_what_an_honest_walk_returns() {
        let n = 20_000;
        let mut seed = 99u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let vals: Vec<i64> = (0..n).map(|_| (next() % 1_000_000) as i64 - 500_000).collect();
        let has: Vec<bool> = (0..n).map(|_| next() % 7 != 0).collect();
        let dir = build("walk", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");
        let b = c.buckets.as_ref().expect("корзины на месте");

        let mut docs = Vec::new();
        for (lo, hi) in [
            (-500_000i64, 499_999i64),
            (i64::MIN, i64::MAX),
            (0, 0),
            (-1000, 1000),
            (499_999, 499_999),
            (600_000, 700_000),
            (-600_000, -550_000),
        ] {
            let want: Vec<u32> = (0..n)
                .filter(|&d| has[d] && vals[d] >= lo && vals[d] <= hi)
                .map(|d| d as u32)
                .collect();

            let (first, last, est) = b.span_of(lo, hi);
            let mut got: Vec<u32> = Vec::new();
            for k in first..=last {
                b.docs(k, &mut docs);
                for &d in docs.iter() {
                    let v = c.get(d).expect("документ корзины обязан иметь значение");
                    if v >= lo && v <= hi {
                        got.push(d);
                    }
                }
            }
            got.sort_unstable();
            assert_eq!(got, want, "диапазон [{lo}, {hi}]");
            assert!(est >= want.len(), "оценка {est} обязана быть не меньше {}", want.len());
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Документ без значения не попадает ни в одну корзину.
    #[test]
    fn a_document_without_a_value_is_in_no_bucket() {
        let n = 1000;
        let vals: Vec<i64> = (0..n).map(|i| i as i64).collect();
        let has: Vec<bool> = (0..n).map(|i| i % 4 != 0).collect();
        let dir = build("missing", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");
        let b = c.buckets.as_ref().expect("корзины на месте");

        let mut seen = 0usize;
        let mut docs = Vec::new();
        for k in 0..BUCKETS {
            b.docs(k, &mut docs);
            for &d in docs.iter() {
                assert!(has[d as usize], "документ {d} без значения попал в корзину");
                seen += 1;
            }
        }
        assert_eq!(seen, has.iter().filter(|&&x| x).count());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Полю с малым размахом достаётся корзина на значение, а не шестнадцать
    /// тысяч.
    #[test]
    fn a_narrow_field_gets_a_bucket_per_value() {
        let n = 4000;
        // Логическое поле и поле с пятью градациями — то, что в разреженных
        // схемах встречается чаще всего.
        for (name, span, want) in [("bool", 2i64, 2usize), ("rating", 5, 5)] {
            let vals: Vec<i64> = (0..n).map(|i| (i as i64) % span).collect();
            let has = vec![true; n];
            let dir = build(name, &vals, &has);
            let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
                .expect("колонка открылась");
            let b = c.buckets.as_ref().expect("корзины на месте");
            assert_eq!(b.count(), want, "{name}: корзин по числу различных значений");

            let idx = std::fs::metadata(format!("{dir}/p.bktidx")).unwrap().len() as usize;
            assert_eq!(idx, 16 + want * BKT_META, "{name}: указатель по числу корзин");
            assert!(idx < 16 + BUCKETS * BKT_META, "{name}: и он меньше прежнего");

            // Ответ обязан совпасть с честным перебором на каждом значении.
            let mut docs = Vec::new();
            for v in 0..span {
                let (first, last, _) = b.span_of(v, v);
                let mut got: Vec<u32> = Vec::new();
                for k in first..=last {
                    b.docs(k, &mut docs);
                    got.extend(docs.iter().filter(|&&d| c.get(d) == Some(v)));
                }
                got.sort_unstable();
                let want: Vec<u32> = (0..n as u32).filter(|&d| c.get(d) == Some(v)).collect();
                assert_eq!(got, want, "{name}: значение {v}");
            }
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Число корзин выводится из длины файла, и это единственное, что делает
    /// формат совместимым: у старого файла `(len − 16) / 8` обязано дать ровно
    /// `BUCKETS`, а выдача — совпасть с честным перебором.
    #[test]
    fn a_pointer_written_by_the_old_version_still_reads() {
        fn encode_v1(vals: &[i64], has: &[bool]) -> (Vec<u8>, Vec<u8>) {
            let mut lo = i64::MAX;
            let mut hi = i64::MIN;
            for (i, &v) in vals.iter().enumerate() {
                if has[i] {
                    lo = lo.min(v);
                    hi = hi.max(v);
                }
            }
            let (base, span) = if lo > hi {
                (0u64, 1u64)
            } else {
                (ord(lo), (ord(hi) - ord(lo)) / BUCKETS as u64 + 1)
            };
            let mut lists: Vec<Vec<u32>> = vec![Vec::new(); BUCKETS];
            for (doc, &v) in vals.iter().enumerate() {
                if has[doc] {
                    let u = ord(v);
                    let b =
                        if u <= base { 0 } else { (((u - base) / span) as usize).min(BUCKETS - 1) };
                    lists[b].push(doc as u32);
                }
            }
            let mut data = Vec::new();
            let mut idx = Vec::with_capacity(16 + BUCKETS * BKT_META);
            idx.extend(base.to_le_bytes());
            idx.extend(span.to_le_bytes());
            let mut deltas: Vec<u32> = Vec::with_capacity(crate::bits::BLOCK);
            for l in lists.iter() {
                idx.extend((data.len() as u32).to_le_bytes());
                idx.extend((l.len() as u32).to_le_bytes());
                let mut prev = 0u32;
                for chunk in l.chunks(crate::bits::BLOCK) {
                    deltas.clear();
                    for &d in chunk {
                        deltas.push(d - prev);
                        prev = d;
                    }
                    let mut bp = 0u64;
                    crate::bits::pack_values(&mut data, &deltas, &mut bp);
                }
            }
            (data, idx)
        }

        let n = 3000;
        let vals: Vec<i64> = (0..n).map(|i| (i as i64 * 7919) % 100_000).collect();
        let has: Vec<bool> = (0..n).map(|i| i % 5 != 0).collect();

        let dir = std::env::temp_dir().join("svindb-bkt-v1");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (data, meta) = encode(&vals, &has);
        let (bkt, bidx) = encode_v1(&vals, &has);
        assert_eq!(bidx.len(), 16 + BUCKETS * BKT_META, "старый указатель фиксированной длины");
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("p.col", &data);
        w("p.cmeta", &meta);
        w("p.has", &encode_presence(&has));
        w("p.bkt", &bkt);
        w("p.bktidx", &bidx);

        let dir = dir.to_string_lossy().to_string();
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");
        let b = c.buckets.as_ref().expect("корзины на месте");
        assert_eq!(b.count(), BUCKETS, "число корзин выведено из длины");

        let (lo, hi) = (20_000i64, 60_000i64);
        let (first, last, est) = b.span_of(lo, hi);
        let mut docs = Vec::new();
        let mut got: Vec<u32> = Vec::new();
        for k in first..=last {
            b.docs(k, &mut docs);
            got.extend(docs.iter().filter(|&&d| matches!(c.get(d), Some(v) if v >= lo && v <= hi)));
        }
        got.sort_unstable();
        let want: Vec<u32> =
            (0..n as u32).filter(|&d| matches!(c.get(d), Some(v) if v >= lo && v <= hi)).collect();
        assert_eq!(got, want, "выдача старого указателя");
        assert!(est >= want.len(), "оценка не занижает");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Оценка по корзинам не должна быть меньше правды.
    ///
    /// Занижение увело бы обход туда, где кандидатов на самом деле больше, —
    /// то есть выбор ведущего стал бы неверным именно там, где важен.
    #[test]
    fn the_estimate_never_undershoots() {
        let n = 5000;
        let vals: Vec<i64> = (0..n).map(|i| (i as i64 * 7919) % 100_000).collect();
        let has = vec![true; n];
        let dir = build("estimate", &vals, &has);
        let c = Column::open(&crate::container::loose(&dir), "p", n, Presence::Bits)
            .expect("колонка открылась");
        for (lo, hi) in [(0i64, 100i64), (0, 50_000), (99_000, 100_000), (0, 99_999)] {
            let want = (0..n).filter(|&d| vals[d] >= lo && vals[d] <= hi).count();
            assert!(c.estimate(lo, hi) >= want, "диапазон [{lo}, {hi}]");
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod walk_tests {
    use super::*;
    use std::io::Write;

    /// Обход в порядке значения обязан отдавать документы по убыванию, а
    /// граница — не отставать от отданного.
    ///
    /// Если граница врёт в большую сторону, обрыв не сработает никогда: он ждёт
    /// «оставшееся строго хуже k-го», а получает «оставшееся не хуже».
    #[test]
    fn the_walk_bound_never_promises_more_than_is_left() {
        let n = 20_000;
        let mut seed = 31u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let vals: Vec<i64> = (0..n).map(|_| (next() % 1_000_000) as i64).collect();
        let has = vec![true; n];

        let dir = std::env::temp_dir().join("svindb-walk");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (data, meta) = encode(&vals, &has);
        let (bkt, bidx) = encode_buckets(&vals, &has);
        let w = |nm: &str, b: &[u8]| {
            std::fs::File::create(dir.join(nm)).unwrap().write_all(b).unwrap();
        };
        w("p.col", &data);
        w("p.cmeta", &meta);
        w("p.has", &encode_presence(&has));
        w("p.bkt", &bkt);
        w("p.bktidx", &bidx);
        let d = dir.to_string_lossy().to_string();
        let c = Column::open(&crate::container::loose(&d), "p", n, Presence::Bits)
            .expect("колонка открылась");
        assert!(c.buckets.is_some(), "у случайного поля обязаны быть корзины");

        let mut walk = c.in_value_order(true);
        let mut docs = Vec::new();
        let mut seen = 0usize;
        let mut worst_so_far = i64::MAX;
        while walk.next(&mut docs) {
            for &doc in docs.iter() {
                let v = c.get(doc).expect("документ корзины имеет значение");
                worst_so_far = worst_so_far.min(v);
                seen += 1;
            }
            if let Some(b) = walk.bound() {
                assert!(
                    b <= worst_so_far,
                    "граница {b} обещает больше, чем осталось: уже отдано вплоть до {worst_so_far}"
                );
            }
        }
        assert_eq!(seen, n, "обход обязан отдать все документы");
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod estimate_tests {
    use super::*;
    use std::io::Write;

    /// Колонка на диске из готовых значений, с вёдрами по тому же правилу, по
    /// какому их строит приём: там, где границы блоков не отсекают.
    fn build(name: &str, vals: &[i64], has: &[bool]) -> (String, bool) {
        let dir = std::env::temp_dir().join(format!("svindb-estimate-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (data, meta) = encode(vals, has);
        let spread = spread_of(meta.chunks(META).map(|c| {
            let lo = i64::from_le_bytes(c[0..8].try_into().unwrap());
            let hi = i64::from_le_bytes(c[8..16].try_into().unwrap());
            (lo, hi)
        }));
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("p.col", &data);
        w("p.cmeta", &meta);
        w("p.has", &encode_presence(has));
        let with = spread >= SPREAD_USELESS;
        if with {
            let (bkt, bidx) = encode_buckets(vals, has);
            w("p.bkt", &bkt);
            w("p.bktidx", &bidx);
        }
        (dir.to_string_lossy().to_string(), with)
    }

    /// Оценка кандидатов держится около правды на всех раскладках колонки.
    ///
    /// ## Что тут проверяется и почему числом
    ///
    /// По этой оценке выбирается ведущий обхода: отбор ведёт, только если
    /// обещает меньше кандидатов, чем самое редкое слово. Завышение поэтому не
    /// «немного неточно», а прямо меняет план запроса — и меняет молча.
    ///
    /// Замерено на ста тысячах документов до правки:
    ///
    /// | колонка | диапазон | правда | было | стало |
    /// |---|---|---:|---:|---:|
    /// | плотная растущая | 10% | 10 000 | 10 112 | 10 000 |
    /// | плотная случайная | 20% | 20 001 | 20 006 | 20 001 |
    /// | разреженная случайная | 1% | 10 | **768** | 6 |
    /// | разреженная случайная | 20% | 201 | **1 024** | 203 |
    /// | два значения, диапазон мимо | — | 0 | **50 000** | 0 |
    ///
    /// Две выделенные строки и есть то, ради чего правка: разреженной колонке
    /// вёдра не строят никогда, и у неё каждый блок оказывался задет.
    #[test]
    fn the_estimate_stays_near_the_truth_on_every_shape() {
        const N: usize = 100_000;
        let shapes: Vec<(&str, Vec<i64>)> = vec![
            ("растущее", (0..N).map(|i| i as i64).collect()),
            (
                "случайное",
                (0..N).map(|i| ((i as u64).wrapping_mul(2_654_435_761) % 100_000) as i64).collect(),
            ),
            ("почти растущее", (0..N).map(|i| i as i64 + ((i % 7) as i64) * 3).collect()),
        ];
        let has = vec![true; N];
        for (name, vals) in shapes {
            let (dir, _) = build(name, &vals, &has);
            let c = Column::open(&crate::container::loose(&dir), "p", N, Presence::Bits)
                .expect("колонка открылась");
            for (lo, hi) in [(0i64, 999i64), (0, 9_999), (40_000, 60_000)] {
                let truth = vals.iter().filter(|&&v| v >= lo && v <= hi).count();
                let est = c.estimate(lo, hi);
                let off = est as f64 / truth.max(1) as f64;
                assert!(
                    (0.5..=1.5).contains(&off),
                    "{name}: [{lo},{hi}] правда {truth}, оценка {est} — промах в {off:.1} раз"
                );
            }
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Разреженная колонка: у неё вёдер нет, и промах был самым большим.
    #[test]
    fn a_sparse_column_is_estimated_by_overlap_not_by_blocks() {
        const N: usize = 100_000;
        // Значение у одного документа из ста, значения не упорядочены.
        let mut sparse: Vec<(u32, i64)> = Vec::new();
        for i in (0..N).step_by(100) {
            let at = (i / 100) as u64;
            sparse.push((i as u32, (at.wrapping_mul(2_654_435_761) % 1000) as i64));
        }
        let dir = std::env::temp_dir().join("svindb-estimate-sparse");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (sdata, smeta) = encode_sparse(&sparse);
        std::fs::File::create(dir.join("p.scol")).unwrap().write_all(&sdata).unwrap();
        std::fs::File::create(dir.join("p.smeta")).unwrap().write_all(&smeta).unwrap();
        let dirs = dir.to_string_lossy().to_string();
        let c = Column::open_with(
            &crate::container::loose(&dirs),
            "p",
            N,
            Presence::Bits,
            Layout::Sparse,
            sparse.len(),
        )
        .expect("колонка открылась");

        // Узкий диапазон: десять значений из тысячи. Прежде оценка давала 768 —
        // то есть отбор, отбирающий сотую часть, обход не вёл никогда.
        let truth = sparse.iter().filter(|(_, v)| (0..=9).contains(v)).count();
        let est = c.estimate(0, 9);
        assert_eq!(truth, 10, "корпус не тот");
        assert!(est <= 40, "оценка {est} при десяти подошедших — отбор снова не поведёт");
        std::fs::remove_dir_all(&dirs).ok();
    }

    /// Разреженная колонка отдаёт по диапазону ровно тех, у кого значение в нём.
    ///
    /// ## Что это ловит
    ///
    /// `select_window` перебирала блоки как отрезки **номеров документов**: `b`-й
    /// блок считался покрывающим `b·BLOCK .. b·BLOCK+BLOCK`. У плотной колонки
    /// это правда по построению; у разреженной в блоке лежат сто двадцать восемь
    /// **присутствующих значений**, чьи номера разбросаны по всему сегменту.
    /// Границы значений сверялись с одними блоками, а номера брались из других, и
    /// отбор отдавал ноль документов вместо всех подходящих — молча, потому что
    /// «не нашлось» выглядит законным ответом.
    ///
    /// Сверка идёт с честным перебором по всем документам, а не с набором
    /// частных случаев: у оптимизации одно обязательство — та же выдача.
    #[test]
    fn a_sparse_column_selects_exactly_the_documents_in_range() {
        const N: usize = 20_000;
        // Значение у одного документа из ста — то есть блоков значений мало, а
        // номера разбросаны по всем двадцати тысячам. Значения нарочно не
        // совпадают с номерами: иначе «номер в окне» и «значение в диапазоне»
        // случайно означали бы одно и то же, и промах спрятался бы.
        let mut sparse: Vec<(u32, i64)> = Vec::new();
        for i in (0..N).step_by(100) {
            let at = (i / 100) as u64;
            sparse.push((i as u32, (at.wrapping_mul(2_654_435_761) % 10_000) as i64));
        }
        let dir = std::env::temp_dir().join("svindb-estimate-sparse-select");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (sdata, smeta) = encode_sparse(&sparse);
        std::fs::File::create(dir.join("p.scol")).unwrap().write_all(&sdata).unwrap();
        std::fs::File::create(dir.join("p.smeta")).unwrap().write_all(&smeta).unwrap();
        let dirs = dir.to_string_lossy().to_string();
        let c = Column::open_with(
            &crate::container::loose(&dirs),
            "p",
            N,
            Presence::Bits,
            Layout::Sparse,
            sparse.len(),
        )
        .expect("колонка открылась");

        // Окна разной ширины, включая такое, что меньше блока значений, и
        // такое, что накрывает сегмент целиком.
        for win in [64usize, 1000, 8192, N] {
            for (lo, hi) in [(0i64, 9), (0, 999), (2_000, 3_000), (0, 10_000), (99_999, 100_000)] {
                let mut got: Vec<u32> = Vec::new();
                let mut bits = vec![0u64; win.div_ceil(64)];
                let mut at = 0usize;
                while at < N {
                    let end = (at + win).min(N);
                    bits.iter_mut().for_each(|w| *w = 0);
                    let found = c.select_window(lo, hi, at..end, 0, &mut bits);
                    let mut seen = 0usize;
                    for (w, word) in bits.iter().enumerate() {
                        let mut x = *word;
                        while x != 0 {
                            let b = x.trailing_zeros() as usize;
                            got.push((at + w * 64 + b) as u32);
                            seen += 1;
                            x &= x - 1;
                        }
                    }
                    assert_eq!(seen, found, "окно {at}..{end}: биты и счётчик разошлись");
                    at = end;
                }
                let want: Vec<u32> =
                    sparse.iter().filter(|(_, v)| *v >= lo && *v <= hi).map(|(d, _)| *d).collect();
                assert_eq!(
                    got,
                    want,
                    "окно {win}, диапазон [{lo},{hi}]: отобрано {} против {} честных",
                    got.len(),
                    want.len()
                );
            }
        }
        std::fs::remove_dir_all(&dirs).ok();
    }

    /// Края ведра ограничивают всё, что оно держит, — включая края сетки.
    ///
    /// Тонкость в `u64`: верхний край считается как `начало + (b+1)·шаг − 1`, и
    /// у колонки с максимумом ровно `i64::MAX` эта сумма упирается в потолок.
    /// Насыщение уводит объявленный край **ниже** значения, которое ведро
    /// держит, а счёт по заголовкам принимает край за обещание.
    #[test]
    fn bucket_bounds_hold_every_value_they_carry() {
        // Значения по всему размаху, максимум ровно на краю сетки.
        let mut vals: Vec<i64> =
            (0..400).map(|i| (i as i64 - 200) * 4_000_000_000_000_000).collect();
        vals.push(i64::MAX);
        vals.push(i64::MIN);
        let has = vec![true; vals.len()];
        let (dir, with) = build("bounds", &vals, &has);
        assert!(with, "у такого размаха обязаны быть вёдра");
        let c = Column::open(&crate::container::loose(&dir), "p", vals.len(), Presence::Bits)
            .expect("колонка открылась");
        let b = c.buckets.as_ref().expect("вёдра");
        let mut docs: Vec<u32> = Vec::new();
        for k in 0..b.count() {
            b.docs(k, &mut docs);
            let (lo, hi) = b.covers(k);
            for &d in docs.iter() {
                let v = c.get(d).expect("значение есть");
                assert!(
                    v >= lo && v <= hi,
                    "ведро {k} объявило [{lo}, {hi}], а документ {d} несёт {v}"
                );
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Одно ведро на весь размах `i64` не роняет приём и держит всё.
    ///
    /// Шаг считался сложением, а не насыщением: `u64::MAX + 1` давал панику в
    /// отладке и ноль в выпуске, а следом деление на ноль. Достаточно поля,
    /// заполненного меньше чем у восьми документов и несущего оба края сетки.
    #[test]
    fn a_single_bucket_over_the_whole_range_survives() {
        let vals: Vec<i64> = vec![i64::MIN, i64::MAX, 0, 5, -7, 100];
        let has = vec![true; vals.len()];
        let (dir, with) = build("whole", &vals, &has);
        assert!(with, "у такого размаха обязаны быть вёдра");
        let c = Column::open(&crate::container::loose(&dir), "p", vals.len(), Presence::Bits)
            .expect("колонка открылась");
        let b = c.buckets.as_ref().expect("вёдра");
        let mut docs: Vec<u32> = Vec::new();
        let mut seen = 0usize;
        for k in 0..b.count() {
            b.docs(k, &mut docs);
            let (lo, hi) = b.covers(k);
            for &d in docs.iter() {
                let v = c.get(d).expect("значение есть");
                assert!(v >= lo && v <= hi, "ведро {k}: [{lo}, {hi}] против {v}");
                seen += 1;
            }
        }
        assert_eq!(seen, vals.len(), "вёдра растеряли документы");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Диапазон мимо всех значений оценивается нулём, а не краевой корзиной.
    #[test]
    fn a_range_past_every_value_costs_nothing() {
        const N: usize = 10_000;
        let vals: Vec<i64> = (0..N).map(|i| (i % 2) as i64).collect();
        let has = vec![true; N];
        let (dir, with) = build("outside", &vals, &has);
        assert!(with, "у поля из двух значений обязаны быть вёдра");
        let c = Column::open(&crate::container::loose(&dir), "p", N, Presence::Bits)
            .expect("колонка открылась");
        assert_eq!(c.estimate(0, 1), N, "все значения обязаны попасть");
        assert_eq!(c.estimate(40_000, 60_000), 0, "диапазон мимо всех значений оценён не нулём");
        assert_eq!(c.estimate(-100, -1), 0, "диапазон ниже всех значений оценён не нулём");
        std::fs::remove_dir_all(&dir).ok();
    }
}
