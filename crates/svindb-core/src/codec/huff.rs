//! Канонический Хаффман: длины кодов, коды, таблица распаковки.

/// Наибольшая длина кода. Таблица распаковки — `1 << MAX_BITS` записей.
pub const MAX_BITS: u32 = 12;

/// Длины кодов по частотам. Ноль — символ не встречается и кода не получает.
///
/// Возвращает пустой вектор, если не встречается ни один символ: пустому
/// алфавиту код не нужен, и вызывающий обязан это учесть, а не получить
/// таблицу из нулей, которая выглядит рабочей.
pub fn lengths(freq: &[u32]) -> Vec<u8> {
    let used = freq.iter().filter(|&&f| f > 0).count();
    if used == 0 {
        return Vec::new();
    }
    if used == 1 {
        // Один символ. Дерева нет, но код нужен: иначе поток из одного
        // повторяющегося символа не закодировать вовсе.
        let mut out = vec![0u8; freq.len()];
        out[freq.iter().position(|&f| f > 0).expect("символ найден")] = 1;
        return out;
    }

    let mut shift = 0u32;
    loop {
        let scaled: Vec<u32> =
            freq.iter().map(|&f| if f == 0 { 0 } else { (f >> shift).max(1) }).collect();
        let lens = build(&scaled);
        if lens.iter().all(|&l| u32::from(l) <= MAX_BITS) {
            return lens;
        }
        shift += 1;
        // Когда все частоты станут единицами, дерево будет уравновешенным
        // глубины ceil(log2(алфавит)) — для трёх сотен символов это девять,
        // то есть цикл заведомо кончается.
    }
}

/// Длины кодов без ограничения: обычное дерево Хаффмана.
fn build(freq: &[u32]) -> Vec<u8> {
    let n = freq.len();
    // Узлы: листья 0..n, внутренние n..2n−1.
    let mut weight = vec![0u64; 2 * n];
    let mut parent = vec![usize::MAX; 2 * n];
    let mut live: Vec<usize> = Vec::with_capacity(n);
    for (i, &f) in freq.iter().enumerate() {
        if f > 0 {
            weight[i] = u64::from(f);
            live.push(i);
        }
    }
    // По убыванию веса: наименьшие окажутся в конце и снимаются с конца.
    live.sort_unstable_by(|&a, &b| weight[b].cmp(&weight[a]).then(b.cmp(&a)));

    // Внутренние узлы рождаются по неубыванию веса, поэтому вторая очередь —
    // обычная, и наименьший всегда в голове одной из двух.
    let mut inner: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    let mut next = n;

    let take = |live: &mut Vec<usize>,
                inner: &mut std::collections::VecDeque<usize>,
                weight: &[u64]|
     -> usize {
        match (live.last(), inner.front()) {
            (Some(&l), Some(&i)) => {
                if weight[l] <= weight[i] {
                    live.pop().expect("непустой")
                } else {
                    inner.pop_front().expect("непустой")
                }
            }
            (Some(_), None) => live.pop().expect("непустой"),
            (None, Some(_)) => inner.pop_front().expect("непустой"),
            (None, None) => unreachable!("узлы кончились раньше корня"),
        }
    };

    while live.len() + inner.len() > 1 {
        let a = take(&mut live, &mut inner, &weight);
        let b = take(&mut live, &mut inner, &weight);
        weight[next] = weight[a] + weight[b];
        parent[a] = next;
        parent[b] = next;
        inner.push_back(next);
        next += 1;
    }

    let mut out = vec![0u8; n];
    for (i, len) in out.iter_mut().enumerate() {
        if freq[i] == 0 {
            continue;
        }
        let mut d = 0u32;
        let mut at = i;
        while parent[at] != usize::MAX {
            at = parent[at];
            d += 1;
        }
        *len = d.min(255) as u8;
    }
    out
}

/// Канонические коды по длинам, уже перевёрнутые под запись младшим битом
/// вперёд.
///
/// Перевёрнутые — потому что весь остальной поток пишется младшим битом вперёд,
/// и распаковщик тогда читает `MAX_BITS` бит одним куском и идёт прямо в
/// таблицу. Хранить коды старшим битом вперёд, как в описании канонического
/// кода, значило бы переворачивать их на каждом символе.
pub fn codes(lens: &[u8]) -> Vec<u16> {
    let mut count = [0u16; MAX_BITS as usize + 1];
    for &l in lens {
        if l > 0 {
            count[l as usize] += 1;
        }
    }
    let mut next = [0u16; MAX_BITS as usize + 1];
    let mut code = 0u16;
    for l in 1..=MAX_BITS as usize {
        code = (code + count[l - 1]) << 1;
        next[l] = code;
    }
    let mut out = vec![0u16; lens.len()];
    for (i, &l) in lens.iter().enumerate() {
        if l == 0 {
            continue;
        }
        let c = next[l as usize];
        next[l as usize] += 1;
        out[i] = reverse(c, u32::from(l));
    }
    out
}

/// Переворачивает младшие `n` бит.
fn reverse(v: u16, n: u32) -> u16 {
    let mut out = 0u16;
    for i in 0..n {
        out |= ((v >> (n - 1 - i)) & 1) << i;
    }
    out
}

/// Таблица распаковки: `MAX_BITS` бит входа — символ и сколько бит съесть.
///
/// ## Таблица делится между одинаковыми словарями
///
/// Таблица занимает `1 << MAX_BITS` записей по четыре байта — **шестнадцать
/// килобайт**, и её держит каждый распаковщик. Словарь хранимого поля держит
/// два: литералы и расстояния. Значит тридцать два килобайта на хранимое поле на
/// сегмент — на четырёх тысячах сегментов с десятью полями это **1.28 гигабайта
/// кучи**, и это не кеш страниц, а настоящая память процесса, которую ядру не
/// отдать.
///
/// Общей на всех таблица быть не может: она строится из длин кодов, а те
/// приходят из словаря, обученного на **этом** сегменте. Но у сегментов одного
/// корпуса длины совпадают часто — статистика та же, — и тогда таблицы
/// побайтово одинаковы.
///
/// Поэтому таблица делится по содержимому: ключ — сами длины кодов, значение —
/// уже построенная таблица. Совпали длины, значит совпадёт и таблица, и второй
/// раз её строить незачем.
///
/// В кеше лежат **слабые** ссылки: иначе таблица закрытого сегмента жила бы до
/// конца процесса, и вместо экономии вышла бы утечка, растущая с числом
/// открытий.
#[derive(Clone)]
pub struct Decoder {
    /// `Arc<[u32]>` — не `Arc<Vec<u32>>`: у среза указатель уже толстый, и
    /// обращение стоит столько же, сколько у `Vec`. Лишнего прыжка в горячем
    /// цикле распаковки не появляется.
    table: std::sync::Arc<[u32]>,
}

/// Таблица разбора, поделённая между читателями. Слабая ссылка, чтобы
/// последний ушедший читатель освободил её сам.
type SharedTables = std::collections::HashMap<Box<[u8]>, std::sync::Weak<[u32]>>;

/// Уже построенные таблицы по длинам кодов, слабыми ссылками.
static SHARED: std::sync::Mutex<Option<SharedTables>> = std::sync::Mutex::new(None);

/// Отдаёт таблицу для этих длин, построив её при первой надобности.
fn shared_table(lens: &[u8], build: impl FnOnce() -> Vec<u32>) -> std::sync::Arc<[u32]> {
    let Ok(mut guard) = SHARED.lock() else {
        // Замок отравлен — считаем сами. Отказывать из-за учёта нельзя: он
        // экономит память, а не обеспечивает правильность.
        return build().into();
    };
    let map = guard.get_or_insert_with(std::collections::HashMap::new);
    if let Some(got) = map.get(lens).and_then(|w| w.upgrade()) {
        return got;
    }
    let made: std::sync::Arc<[u32]> = build().into();
    map.insert(lens.into(), std::sync::Arc::downgrade(&made));
    // Мёртвые ссылки убираются здесь же, на промахе: отдельный обход был бы
    // работой без повода, а промахи и есть тот повод.
    if map.len() > 64 {
        map.retain(|_, w| w.strong_count() > 0);
    }
    made
}

/// Сколько разных таблиц распаковки живо в процессе. Для замеров и `/_health`.
pub fn shared_tables() -> usize {
    SHARED
        .lock()
        .ok()
        .and_then(|g| g.as_ref().map(|m| m.values().filter(|w| w.strong_count() > 0).count()))
        .unwrap_or(0)
}

/// Проверяет присутствие именно заданного ключа в кеше.
///
/// Общий счётчик нельзя использовать в параллельном тесте отказа: пока тест
/// проверяет негодные длины, другой тест вправе открыть собственную годную
/// таблицу. Проверка конкретного ключа отделяет эти независимые события.
#[cfg(test)]
fn shared_contains(lens: &[u8]) -> bool {
    SHARED.lock().ok().and_then(|g| g.as_ref().map(|m| m.contains_key(lens))).unwrap_or(false)
}

/// Проверяет, что для заданных длин сейчас существует живая таблица.
#[cfg(test)]
fn shared_is_live(lens: &[u8]) -> bool {
    SHARED
        .lock()
        .ok()
        .and_then(|g| g.as_ref().and_then(|m| m.get(lens)).map(|table| table.strong_count() > 0))
        .unwrap_or(false)
}

impl Decoder {
    /// `None` — длины негодные: код длиннее [`MAX_BITS`] или дерево неполное.
    ///
    /// Неполное дерево отвергается намеренно. Оно означало бы, что часть
    /// битовых последовательностей не отображается ни в какой символ, и
    /// испорченный поток распаковался бы в мусор вместо отказа.
    pub fn new(lens: &[u8]) -> Option<Decoder> {
        if lens.is_empty() {
            return None;
        }
        // Проверка **до** построения кодов, а не после: длины приходят с диска и
        // могут быть испорчены, а `codes` по длине больше потолка вышла бы за
        // границу таблицы. Порча обязана давать отказ, а не падение.
        if lens.iter().any(|&l| u32::from(l) > MAX_BITS) {
            return None;
        }
        // Проверка Крафта — **до** разделения таблицы: негодные длины не должны
        // ни строить таблицу, ни попадать в кеш под своим ключом.
        let codes = codes(lens);
        let mut kraft = 0u64;
        for &l in lens.iter() {
            if l != 0 {
                kraft += 1u64 << (MAX_BITS - u32::from(l));
            }
        }
        // Единственное исключение — алфавит из одного символа: его код занимает
        // половину пространства, и вторая половина остаётся пустой.
        let single = lens.iter().filter(|&&l| l > 0).count() == 1;
        if kraft != 1u64 << MAX_BITS && !(single && kraft == 1u64 << (MAX_BITS - 1)) {
            return None;
        }
        let table = shared_table(lens, || {
            let mut table = vec![u32::MAX; 1 << MAX_BITS];
            for (sym, (&l, &c)) in lens.iter().zip(codes.iter()).enumerate() {
                if l == 0 {
                    continue;
                }
                let step = 1usize << l;
                let mut at = c as usize;
                while at < table.len() {
                    table[at] = ((sym as u32) << 8) | u32::from(l);
                    at += step;
                }
            }
            table
        });
        Some(Decoder { table })
    }

    /// Символ и длина его кода по `MAX_BITS` битам входа.
    #[inline(always)]
    pub fn lookup(&self, bits: u32) -> Option<(u16, u32)> {
        let e = self.table[(bits & ((1 << MAX_BITS) - 1)) as usize];
        if e == u32::MAX {
            return None;
        }
        Some(((e >> 8) as u16, e & 0xff))
    }
}

/// Пишет биты младшим вперёд.
pub struct BitWriter {
    pub out: Vec<u8>,
    acc: u64,
    bits: u32,
}

impl BitWriter {
    pub fn new() -> BitWriter {
        BitWriter { out: Vec::new(), acc: 0, bits: 0 }
    }

    #[inline(always)]
    pub fn put(&mut self, value: u32, n: u32) {
        debug_assert!(n <= 32 && (n == 32 || value < (1 << n)), "значение шире {n} бит");
        self.acc |= u64::from(value) << self.bits;
        self.bits += n;
        while self.bits >= 8 {
            self.out.push(self.acc as u8);
            self.acc >>= 8;
            self.bits -= 8;
        }
    }

    /// Дописывает хвост нулями до байта.
    pub fn finish(mut self) -> Vec<u8> {
        if self.bits > 0 {
            self.out.push(self.acc as u8);
        }
        self.out
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Читает биты младшим вперёд.
///
/// За концом входа отдаёт нули, а не отказ: отказ обязан прийти от того, кто
/// понимает, чего ждал — неизвестного символа или оборванной лексемы. Иначе
/// пришлось бы проверять конец на каждом бите.
pub struct BitReader<'a> {
    src: &'a [u8],
    at: usize,
    acc: u64,
    bits: u32,
}

impl<'a> BitReader<'a> {
    pub fn new(src: &'a [u8]) -> BitReader<'a> {
        BitReader { src, at: 0, acc: 0, bits: 0 }
    }

    #[inline(always)]
    fn fill(&mut self) {
        while self.bits <= 56 {
            let b = if self.at < self.src.len() { self.src[self.at] } else { 0 };
            self.acc |= u64::from(b) << self.bits;
            self.at += 1;
            self.bits += 8;
        }
    }

    #[inline(always)]
    pub fn peek(&mut self, n: u32) -> u32 {
        self.fill();
        (self.acc & ((1u64 << n) - 1)) as u32
    }

    #[inline(always)]
    pub fn skip(&mut self, n: u32) {
        self.acc >>= n;
        self.bits -= n;
    }

    #[inline(always)]
    pub fn take(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        let v = self.peek(n);
        self.skip(n);
        v
    }

    pub fn overrun(&self) -> bool {
        self.at > self.src.len() + 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Длины обязаны укладываться в потолок на любых частотах, включая злые.
    #[test]
    fn lengths_never_exceed_the_cap() {
        let cases: Vec<Vec<u32>> = vec![
            vec![1; 300],
            (1..=300u32).collect(),
            // Частоты Фибоначчи — худший случай для Хаффмана: дерево вырождается
            // в цепочку, и без ограничения длина равнялась бы числу символов.
            {
                let mut f = vec![1u32, 1];
                for i in 2..40 {
                    let v = f[i - 1].saturating_add(f[i - 2]);
                    f.push(v);
                }
                f
            },
            vec![1_000_000, 1, 1, 1],
        ];
        for freq in cases {
            let lens = lengths(&freq);
            assert!(
                lens.iter().all(|&l| u32::from(l) <= MAX_BITS),
                "длина вышла за {MAX_BITS}: {:?}",
                lens.iter().max()
            );
            assert!(Decoder::new(&lens).is_some(), "таблица не построилась");
        }
    }

    /// Круг по всем символам алфавита, а не по паре наугад.
    #[test]
    fn every_symbol_survives_the_round_trip() {
        let mut freq = vec![0u32; 300];
        for (i, f) in freq.iter_mut().enumerate() {
            *f = (i as u32 % 17) + 1;
        }
        let lens = lengths(&freq);
        let cs = codes(&lens);
        let dec = Decoder::new(&lens).expect("таблица");

        let mut w = BitWriter::new();
        for (s, (&l, &c)) in lens.iter().zip(cs.iter()).enumerate() {
            if l > 0 {
                w.put(u32::from(c), u32::from(l));
            }
            let _ = s;
        }
        let bytes = w.finish();

        let mut r = BitReader::new(&bytes);
        for (s, &l) in lens.iter().enumerate() {
            if l == 0 {
                continue;
            }
            let (sym, n) = dec.lookup(r.peek(MAX_BITS)).expect("символ");
            assert_eq!(sym as usize, s, "символ разошёлся");
            assert_eq!(n, u32::from(l), "длина кода разошлась");
            r.skip(n);
        }
    }

    /// Алфавит из одного символа — не край, а обычный случай: колонка, где все
    /// значения одинаковы, даёт ровно его.
    #[test]
    fn an_alphabet_of_one_still_codes() {
        let mut freq = vec![0u32; 50];
        freq[7] = 1000;
        let lens = lengths(&freq);
        assert_eq!(lens[7], 1, "единственному символу нужен код в один бит");
        let dec = Decoder::new(&lens).expect("таблица на один символ");
        let cs = codes(&lens);
        let mut w = BitWriter::new();
        for _ in 0..100 {
            w.put(u32::from(cs[7]), 1);
        }
        let bytes = w.finish();
        let mut r = BitReader::new(&bytes);
        for _ in 0..100 {
            let (sym, n) = dec.lookup(r.peek(MAX_BITS)).expect("символ");
            assert_eq!(sym, 7);
            r.skip(n);
        }
    }

    /// Неполное дерево обязано быть отвергнуто, а не принято молча.
    #[test]
    fn an_incomplete_tree_is_refused() {
        // Два символа по три бита: покрыто 2/8 пространства.
        let lens = vec![3u8, 3, 0, 0];
        assert!(Decoder::new(&lens).is_none(), "неполное дерево принято");
        // И слишком длинный код тоже.
        let mut long = vec![0u8; 4];
        long[0] = (MAX_BITS + 1) as u8;
        assert!(Decoder::new(&long).is_none(), "код длиннее потолка принят");
    }

    /// Биты идут туда и обратно на любой ширине, включая нулевую и полную.
    #[test]
    fn bits_survive_any_width() {
        let mut seed = 0x1234_5678u64;
        let mut vals = Vec::new();
        let mut w = BitWriter::new();
        for _ in 0..2000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let n = (seed % 25) as u32;
            let v = if n == 0 { 0 } else { (seed >> 32) as u32 & ((1 << n) - 1) };
            w.put(v, n);
            vals.push((v, n));
        }
        let bytes = w.finish();
        let mut r = BitReader::new(&bytes);
        for (v, n) in vals {
            assert_eq!(r.take(n), v, "разошлось на ширине {n}");
        }
    }
}

/// Разделение таблиц распаковки по содержимому.
#[cfg(test)]
mod shared_tests {
    use super::*;

    /// Годные длины кодов длиной 288: один символ на два бита и хвост нулей не
    /// пройдёт проверку Крафта, поэтому строится полное двоичное дерево.
    fn lens(n: usize) -> Vec<u8> {
        // Полное дерево из `n` листьев одинаковой глубины: `n` обязано быть
        // степенью двойки, тогда сумма `2^-l` равна единице ровно.
        let depth = n.trailing_zeros() as u8;
        vec![depth; n]
    }

    /// Одинаковые длины дают **одну и ту же** таблицу, а не две одинаковых.
    ///
    /// Проверяется тождество памяти, а не равенство содержимого: содержимое
    /// совпало бы и у двух отдельных таблиц, и тогда тест молчал бы о том, ради
    /// чего написан. Шестнадцать килобайт на таблицу, две на хранимое поле, —
    /// на тысячах сегментов это гигабайты кучи.
    #[test]
    fn the_same_lengths_share_one_table() {
        let l = lens(256);
        let a = Decoder::new(&l).expect("годные длины");
        let b = Decoder::new(&l).expect("годные длины");
        assert!(
            std::sync::Arc::ptr_eq(&a.table, &b.table),
            "таблица построена дважды при одинаковых длинах"
        );
        // И она работает: любой вход даёт символ с длиной кода.
        let (_, bits) = a.lookup(0).expect("нулевой вход разобрался");
        assert_eq!(bits, 8, "длина кода не та");
    }

    /// Разные длины — разные таблицы.
    #[test]
    fn different_lengths_get_different_tables() {
        let a = Decoder::new(&lens(256)).expect("годные");
        let b = Decoder::new(&lens(512)).expect("годные");
        assert!(!std::sync::Arc::ptr_eq(&a.table, &b.table), "разные длины слились в одну таблицу");
    }

    /// Закрытый сегмент забирает таблицу с собой: в кеше слабые ссылки.
    ///
    /// Без этого таблица жила бы до конца процесса, и вместо экономии вышла бы
    /// утечка, растущая с числом открытий сегментов.
    #[test]
    fn a_table_dies_with_its_last_user() {
        // Своя форма длин, чтобы не пересечься с соседними тестами.
        let l = lens(1024);
        {
            let _a = Decoder::new(&l).expect("годные");
            assert!(shared_is_live(&l), "таблица не попала в учёт");
        }
        // Ссылок не осталось — таблица освободилась. Учёт считает только живые,
        // поэтому мёртвая запись в счёт уже не идёт, даже если ключ ещё лежит.
        assert!(!shared_is_live(&l), "таблица пережила своего последнего хозяина");
    }

    /// Негодные длины не строят таблицу и не попадают в кеш.
    #[test]
    fn broken_lengths_are_refused_before_the_cache() {
        // Крафт не сходится: два символа по одному биту плюс третий.
        let broken_kraft = [1, 1, 1];
        assert!(Decoder::new(&broken_kraft).is_none(), "негодные длины приняты");
        // Длина за потолком.
        let too_long = [MAX_BITS as u8 + 1, 1];
        assert!(Decoder::new(&too_long).is_none(), "длина за потолком принята");
        assert!(!shared_contains(&broken_kraft), "негодные длины попали в кеш");
        assert!(!shared_contains(&too_long), "длина за потолком попала в кеш");
    }
}
