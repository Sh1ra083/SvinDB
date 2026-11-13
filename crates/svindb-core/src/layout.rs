//! Компактная раскладка метаданных слов и списков документов.

use crate::mmap::Region;

/// Через сколько слов ставится опора. То же число, что у словаря, и не случайно:
/// поиск слова уже проходит его группу, и группы обоих файлов идут в ногу.
pub const ANCHOR: usize = crate::dict::ANCHOR;

/// Байт на опорную запись: четыре накопленных числа.
pub const RESTART: usize = 16;

/// Все частоты списка равны единице.
pub const TF1: u64 = 1;
/// У поля есть позиции.
pub const POS: u64 = 2;
/// Единственное вхождение и его позиция лежат в самой записи.
pub const INLINE: u64 = 4;
/// На сколько сдвинута частота списка в заголовке.
pub const SHIFT: u32 = 3;

/// Сколько блоков заводит список из `df` документов.
///
/// Короткий список не заводит ни одного: он целиком помещается в один блок, а
/// метаданные такого блока — точная копия метаданных слова.
#[inline(always)]
pub fn blocks_of(df: u32) -> u32 {
    if df as usize > crate::bits::BLOCK {
        (df as usize).div_ceil(crate::bits::BLOCK) as u32
    } else {
        0
    }
}

/// Что лежит в записи слова.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Rec {
    pub df: u32,
    /// Все частоты списка единичны — тогда полосы частот нет вовсе.
    pub tf_all_one: bool,
    /// Поле хранит позиции.
    pub positioned: bool,
    /// `df == 1`: номер документа.
    pub doc: u32,
    /// `df == 1`: частота.
    pub tf: u32,
    /// `df == 1` и вхождение одно: его позиция.
    pub inline_pos: Option<u32>,
    /// `df >= 2`: уровень длины, дающий потолок списка.
    pub qmax: u8,
    /// `df >= 2`: частота, дающая потолок списка.
    pub tfmax: u32,
    /// Длина куска в `postings.bin`. У одиночного слова ноль.
    pub post_len: u32,
    /// Длина куска в `pos.bin`. У одиночного слова с одним вхождением ноль.
    pub pos_len: u32,
}

/// Кладёт запись в конец буфера.
pub fn encode(out: &mut Vec<u8>, r: &Rec) {
    let mut flags = 0u64;
    if r.tf_all_one {
        flags |= TF1;
    }
    if r.positioned {
        flags |= POS;
    }
    if r.inline_pos.is_some() {
        flags |= INLINE;
    }
    varint(out, (u64::from(r.df) << SHIFT) | flags);
    if r.df == 1 {
        varint(out, u64::from(r.doc));
        if !r.tf_all_one {
            // Частота одиночного слова больше единицы — иначе стоял бы TF1.
            varint(out, u64::from(r.tf) - 2);
        }
        if r.positioned {
            match r.inline_pos {
                Some(p) => varint(out, u64::from(p)),
                None => varint(out, u64::from(r.pos_len)),
            }
        }
        return;
    }
    out.push(r.qmax);
    if !r.tf_all_one {
        // Частота потолка не меньше единицы: лучшей парой может оказаться и
        // короткий документ с одним вхождением.
        varint(out, u64::from(r.tfmax) - 1);
    }
    varint(out, u64::from(r.post_len));
    if r.positioned {
        varint(out, u64::from(r.pos_len));
    }
}

/// Читает запись, продвигая `at`. `None` — байты кончились или запись негодная.
///
/// Пустая частота (`df = 0`) — не «слова здесь нет», а порча: слово попадает в
/// словарь только тогда, когда оно у кого-то встретилось. Возвращать её как
/// обычную запись значило бы превратить повреждение в пустой список, то есть в
/// молчаливо неверную выдачу.
pub fn decode(src: &[u8], at: &mut usize) -> Option<Rec> {
    let h = read_varint(src, at)?;
    let df = u32::try_from(h >> SHIFT).ok()?;
    if df == 0 {
        return None;
    }
    let mut r = Rec { df, tf_all_one: h & TF1 != 0, positioned: h & POS != 0, ..Rec::default() };
    let inline = h & INLINE != 0;
    if df == 1 {
        r.doc = u32::try_from(read_varint(src, at)?).ok()?;
        r.tf = if r.tf_all_one { 1 } else { u32::try_from(read_varint(src, at)?).ok()? + 2 };
        if r.positioned {
            let v = u32::try_from(read_varint(src, at)?).ok()?;
            if inline {
                r.inline_pos = Some(v);
            } else {
                r.pos_len = v;
            }
        }
        // Значок «позиция внутри» при частоте больше единицы — противоречие:
        // вхождений несколько, а место есть под одно.
        if inline && r.tf != 1 {
            return None;
        }
        return Some(r);
    }
    r.qmax = *src.get(*at)?;
    *at += 1;
    r.tfmax = if r.tf_all_one { 1 } else { u32::try_from(read_varint(src, at)?).ok()? + 1 };
    r.post_len = u32::try_from(read_varint(src, at)?).ok()?;
    if r.positioned {
        r.pos_len = u32::try_from(read_varint(src, at)?).ok()?;
    }
    // Список из двух и более документов обязан занимать место в списках.
    if r.post_len == 0 {
        return None;
    }
    Some(r)
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline(always)]
fn read_varint(src: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let &b = src.get(*at)?;
        *at += 1;
        v |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return Some(v);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// Запись слова вместе с накопленными смещениями.
#[derive(Clone, Copy, Debug, Default)]
pub struct Located {
    pub rec: Rec,
    /// Смещение куска списка в `postings.bin`.
    pub post: usize,
    /// Смещение куска позиций в `pos.bin`.
    pub pos: usize,
    /// Номер первого блока в `blocks.bin`. Осмыслен только у длинного списка.
    pub block: usize,
    /// Сколько байт заняла сама запись. Нужно разбору и проверке: по нему видно,
    /// во что обошлось слово, не пересобирая запись заново.
    pub len: u32,
}

/// Метаданные слов седьмой раскладки.
pub struct Terms {
    bin: Region,
    idx: Region,
    len: usize,
}

impl Terms {
    pub fn open(src: &crate::container::SegmentSource, terms: usize) -> std::io::Result<Terms> {
        let bin = src.open("terms.bin")?;
        let idx = src.open("terms.idx")?;
        Ok(Terms { bin, idx, len: terms })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn bytes(&self) -> usize {
        self.bin.len_bytes()
    }

    pub fn idx_bytes(&self) -> usize {
        self.idx.len_bytes()
    }

    /// Сколько опорных групп обязано быть при таком числе слов.
    pub fn groups(terms: usize) -> usize {
        terms.div_ceil(ANCHOR)
    }

    pub fn warm(&self) -> usize {
        self.bin.warm() + self.idx.warm()
    }

    /// Опорная запись группы: смещения в трёх файлах и номер первого блока.
    fn restart(&self, g: usize) -> Located {
        let at = g * RESTART;
        Located {
            rec: Rec::default(),
            post: self.idx.u32_at(at + 4) as usize,
            pos: self.idx.u32_at(at + 8) as usize,
            block: self.idx.u32_at(at + 12) as usize,
            len: 0,
        }
    }

    /// Проход по группе, начиная с её опоры.
    pub fn walk(&self, g: usize) -> Walk<'_> {
        let head = self.restart(g);
        Walk {
            src: self.bin.slice(0, self.bin.len_bytes()),
            at: self.idx.u32_at(g * RESTART) as usize,
            post: head.post,
            pos: head.pos,
            block: head.block,
            left: (self.len - (g * ANCHOR).min(self.len)).min(ANCHOR),
        }
    }

    /// Слово по номеру. `None` — номера нет или запись не разобралась.
    ///
    /// Проходом по группе, а не обращением по смещению: запись переменной длины,
    /// и таблицы смещений на каждое слово стоили бы ровно того, ради чего всё
    /// затевалось. Группа лежит подряд и укладывается в считанные строки кеша.
    pub fn at(&self, i: usize) -> Option<Located> {
        if i >= self.len {
            return None;
        }
        let g = i / ANCHOR;
        let mut w = self.walk(g);
        let mut out = None;
        for _ in 0..=(i % ANCHOR) {
            out = w.next();
            out?;
        }
        out
    }
}

/// Проход по опорной группе: записи подряд с накоплением смещений.
pub struct Walk<'a> {
    src: &'a [u8],
    at: usize,
    post: usize,
    pos: usize,
    block: usize,
    left: usize,
}

#[allow(clippy::should_implement_trait)]
impl Walk<'_> {
    /// Следующее слово группы. `None` — группа кончилась или запись негодная.
    pub fn next(&mut self) -> Option<Located> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let was = self.at;
        let rec = decode(self.src, &mut self.at)?;
        let out = Located {
            rec,
            post: self.post,
            pos: self.pos,
            block: self.block,
            len: (self.at - was) as u32,
        };
        self.post += rec.post_len as usize;
        self.pos += rec.pos_len as usize;
        self.block += blocks_of(rec.df) as usize;
        Some(out)
    }

    /// Где проход стоит сейчас: смещение в `terms.bin` и три накопленных числа.
    ///
    /// Нужно проверке целостности: конец последней группы обязан совпасть с
    /// длинами файлов, и это единственный способ убедиться, что записи не
    /// разъехались.
    pub fn here(&self) -> (usize, usize, usize, usize) {
        (self.at, self.post, self.pos, self.block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(r: Rec) {
        let mut buf = Vec::new();
        encode(&mut buf, &r);
        let mut at = 0usize;
        let got = decode(&buf, &mut at).unwrap_or_else(|| panic!("не разобралось: {r:?}"));
        assert_eq!(got, r, "разошлось на {} байтах", buf.len());
        assert_eq!(at, buf.len(), "разбор не дочитал запись до конца: {r:?}");
    }

    #[test]
    fn a_singleton_fits_in_a_handful_of_bytes() {
        let r = Rec {
            df: 1,
            tf_all_one: true,
            positioned: true,
            doc: 197_314,
            tf: 1,
            inline_pos: Some(41),
            ..Rec::default()
        };
        roundtrip(r);
        let mut buf = Vec::new();
        encode(&mut buf, &r);
        // Заголовок, номер документа и позиция — и больше ничего.
        assert_eq!(buf.len(), 5, "одиночное слово заняло {} байт", buf.len());
    }

    /// Все три класса и все сочетания значков.
    #[test]
    fn every_shape_survives_the_roundtrip() {
        for &df in &[1u32, 2, 3, 127, 128, 129, 5000, 1_000_000] {
            for &tf1 in &[true, false] {
                for &positioned in &[true, false] {
                    let single = df == 1;
                    let inline = single && tf1 && positioned;
                    roundtrip(Rec {
                        df,
                        tf_all_one: tf1,
                        positioned,
                        doc: if single { 123_456 } else { 0 },
                        tf: if single {
                            if tf1 {
                                1
                            } else {
                                9
                            }
                        } else {
                            0
                        },
                        inline_pos: if inline { Some(7) } else { None },
                        qmax: if single { 0 } else { 200 },
                        tfmax: if single {
                            0
                        } else if tf1 {
                            1
                        } else {
                            17
                        },
                        post_len: if single { 0 } else { 4096 },
                        pos_len: if positioned && !inline { 321 } else { 0 },
                    });
                }
            }
        }
    }

    /// Порча не имеет права стать пустым списком.
    ///
    /// Это условие важнее размера. Обрезанная или испорченная запись читается
    /// нулями, а `df = 0` в шестой раскладке значило «такого слова здесь нет» —
    /// то есть повреждение выглядело как исправная работа с другой выдачей.
    #[test]
    fn a_broken_record_is_refused_not_read_as_an_empty_list() {
        let r = Rec {
            df: 300,
            tf_all_one: false,
            positioned: true,
            qmax: 12,
            tfmax: 4,
            post_len: 900,
            pos_len: 1200,
            ..Rec::default()
        };
        let mut buf = Vec::new();
        encode(&mut buf, &r);

        // Обрезка по любой длине: разбор обязан отказаться, а не додумать.
        for cut in 0..buf.len() {
            let mut at = 0usize;
            assert!(
                decode(&buf[..cut], &mut at).is_none(),
                "запись, обрезанная до {cut} байт из {}, разобралась",
                buf.len()
            );
        }
        // Нули вместо записи — тоже отказ.
        let mut at = 0usize;
        assert!(decode(&[0u8; 16], &mut at).is_none(), "нулевая запись принята");
        // И пустой кусок в списках у непустого слова.
        let mut bad = Vec::new();
        encode(&mut bad, &Rec { post_len: 0, ..r });
        let mut at = 0usize;
        assert!(decode(&bad, &mut at).is_none(), "список без места в файле принят");
    }

    /// Короткий список блоков не заводит, длинный заводит.
    #[test]
    fn only_a_long_list_owns_blocks() {
        assert_eq!(blocks_of(1), 0);
        assert_eq!(blocks_of(crate::bits::BLOCK as u32), 0);
        assert_eq!(blocks_of(crate::bits::BLOCK as u32 + 1), 2);
        assert_eq!(blocks_of(crate::bits::BLOCK as u32 * 3), 3);
    }
}

/// Сверка раскладки с тем, что в неё положили.
///
/// Проверяется не «читается без паники», а **совпадение с перебором**: списки,
/// частоты и позиции восстанавливаются из самого корпуса, безо всякого знания о
/// формате, и сравниваются с тем, что отдаёт движок. Раскладка, которая делит
/// слова на классы, обязана давать один и тот же ответ во всех трёх.
#[cfg(test)]
mod against_the_corpus {
    use crate::bits::BLOCK;
    use std::collections::HashMap;

    /// Корпус, в котором есть все три класса слов сразу.
    ///
    /// Без этого проверка ничего не стоит: классы отличаются кодом, и корпус,
    /// где все слова редкие, оставит блочный путь нетронутым.
    fn corpus() -> Vec<(String, String)> {
        let mut out = Vec::new();
        for i in 0..500u32 {
            let mut body = String::new();
            // Слово у всех — длинный список, четыре блока.
            body.push_str("всюду ");
            // Слово у каждого десятого — короткий список.
            if i % 10 == 0 {
                body.push_str("редко ");
            }
            // Слово ровно у двух — короткий список на границе снизу.
            if i == 3 || i == 400 {
                body.push_str("пара ");
            }
            // Слово ровно у одного и один раз — одиночное со встроенной
            // позицией.
            body.push_str(&format!("свой{i} "));
            // Слово ровно у одного, но трижды — одиночное с куском позиций.
            if i == 42 {
                body.push_str("трижды трижды трижды ");
            }
            // Повторы у частого слова, чтобы полоса частот была не из одних
            // единиц и длинный список её писал.
            if i % 7 == 0 {
                body.push_str("всюду всюду ");
            }
            out.push((format!("d{i:04}"), body.trim().to_string()));
        }
        out
    }

    fn build(name: &str) -> (String, Vec<(String, String)>) {
        let d = std::env::temp_dir()
            .join(format!("svindb-layout7-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let docs = corpus();
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 1_000_000);
        for (id, body) in docs.iter() {
            ix.add(id, &[body]);
        }
        ix.flush();
        (d, docs)
    }

    /// Что в корпусе на самом деле: слово → документ → позиции.
    fn expected(docs: &[(String, String)]) -> HashMap<String, Vec<(u32, Vec<u32>)>> {
        let mut out: HashMap<String, Vec<(u32, Vec<u32>)>> = HashMap::new();
        for (doc, (_, body)) in docs.iter().enumerate() {
            let mut here: HashMap<&str, Vec<u32>> = HashMap::new();
            for (at, w) in body.split_whitespace().enumerate() {
                here.entry(w).or_default().push(at as u32);
            }
            for (w, poss) in here {
                out.entry(w.to_string()).or_default().push((doc as u32, poss));
            }
        }
        for v in out.values_mut() {
            v.sort_by_key(|(d, _)| *d);
        }
        out
    }

    /// Номера, частоты и позиции обязаны совпасть с корпусом во всех классах.
    #[test]
    fn every_class_gives_back_exactly_what_went_in() {
        let (d, docs) = build("readback");
        let ix = crate::index::load(&format!("{d}/seg0000")).expect("сегмент");
        let want = expected(&docs);

        // Классы обязаны быть представлены все: иначе проверка проходит, ничего
        // не проверив.
        let (mut pulsed, mut short, mut long) = (0, 0, 0);
        let mut poss = Vec::new();
        for (word, rows) in want.iter() {
            let list =
                ix.dict.lookup(0, word).unwrap_or_else(|| panic!("слова {word} нет в словаре"));
            let t = ix.term(list as usize, 0);
            assert_eq!(t.df as usize, rows.len(), "df у слова {word}");
            match (t.one.is_some(), t.meta) {
                (true, _) => pulsed += 1,
                (false, false) => short += 1,
                (false, true) => long += 1,
            }

            let mut c = ix.cursor(list, 0, 1.0);
            for (doc, want_poss) in rows.iter() {
                assert_eq!(c.cur(), *doc, "слово {word}: номер документа");
                assert_eq!(
                    c.tf() as usize,
                    want_poss.len(),
                    "слово {word}, документ {doc}: частота"
                );
                c.positions(&mut poss);
                assert_eq!(&poss, want_poss, "слово {word}, документ {doc}: позиции");
                c.next();
            }
            assert_eq!(c.cur(), u32::MAX, "слово {word}: список длиннее корпуса");
        }
        assert!(pulsed > 400, "одиночных слов {pulsed} — класс не покрыт");
        assert!(short >= 2, "коротких списков {short} — класс не покрыт");
        assert!(long >= 1, "длинных списков {long} — класс не покрыт");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Потолок обязан быть потолком — и обязан достигаться.
    ///
    /// Два утверждения, и второе не менее важно первого. Потолок ниже вклада
    /// теряет документы молча; потолок выше достижимого просто ослабляет
    /// отсечение, но если он **точно** равен наибольшему вкладу, значит
    /// восстановление из пары «уровень длины, частота» ничего не потеряло.
    #[test]
    fn the_ceiling_is_reached_and_never_exceeded() {
        let (d, _) = build("ceiling");
        let ix = crate::index::load(&format!("{d}/seg0000")).expect("сегмент");
        for list in 0..ix.nterms {
            let (fld, word) = ix.dict.term_at(list);
            let fld = fld as usize;
            let t = ix.term(list, fld);
            let mut c = ix.cursor(list as u32, fld, 1.0);
            let mut top = 0f64;
            while c.cur() != u32::MAX {
                let v = ix.sat(fld, c.cur(), c.tf());
                assert!(
                    v <= t.max,
                    "слово {word}: вклад {v} выше потолка {} — отсечение вправе его выбросить",
                    t.max
                );
                top = top.max(v);
                c.next();
            }
            assert_eq!(
                top.to_bits(),
                t.max.to_bits(),
                "слово {word}: потолок {} не достигается, наибольший вклад {top}",
                t.max
            );
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Потолок блока тоже обязан накрывать свой блок.
    #[test]
    fn a_block_ceiling_covers_its_own_block() {
        let (d, _) = build("blockmax");
        let ix = crate::index::load(&format!("{d}/seg0000")).expect("сегмент");
        let mut off = Vec::new();
        let mut docs = vec![0u32; BLOCK];
        let mut tfs = vec![0u32; BLOCK];
        let mut checked = 0;
        for list in 0..ix.nterms {
            let (fld, word) = ix.dict.term_at(list);
            let fld = fld as usize;
            let t = ix.term(list, fld);
            ix.block_offsets(&t, &mut off);
            for (b, &at) in off.iter().enumerate() {
                let bound = f64::from(f32::from_bits(ix.block_max_bits(&t, b)));
                let n = ix.read_block_tf(&t, b, at, &mut docs, Some(&mut tfs));
                for k in 0..n {
                    let v = ix.sat(fld, docs[k], tfs[k]);
                    assert!(
                        v <= bound,
                        "слово {word}, блок {b}: вклад {v} выше потолка блока {bound}"
                    );
                }
                checked += n;
            }
        }
        assert!(checked > 1000, "проверено всего {checked} постингов");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Выдача обязана совпасть с перебором по тем же формулам.
    ///
    /// Перебор считает вклад по корпусу, ничего не зная о раскладке. Совпадение
    /// с ним — единственное утверждение, ради которого всё остальное и делалось:
    /// формат сменился, ответ не изменился.
    #[test]
    fn the_answer_matches_a_brute_force_scorer() {
        let (d, docs) = build("bruteforce");
        let ix = crate::index::load(&format!("{d}/seg0000")).expect("сегмент");
        let want = expected(&docs);

        for word in ["всюду", "редко", "пара", "свой7", "трижды"] {
            let list = ix.dict.lookup(0, word).expect(word);
            let rows = &want[word];
            let w = ix.weight(0, rows.len() as u32);
            let mut brute: Vec<(u32, f64)> = rows
                .iter()
                .map(|(doc, poss)| (*doc, w * ix.sat(0, *doc, poss.len() as u32)))
                .collect();
            // Тот же порядок, что у выдачи: по убыванию веса, при равных — по
            // возрастанию номера.
            brute.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            brute.truncate(10);

            let req = crate::search::Request {
                mode: crate::search::MatchMode::AnyTerm,
                skipped: false,
                live: None,
                words: vec![crate::search::QueryWord::new(0, vec![list])],
                k: 10,
                filter: Vec::new(),
                must: Vec::new(),
                must_not: Vec::new(),
                tree: crate::search::BoolReq::default(),
                sort: Vec::new(),
                floor_key: crate::search::KeyFloor::None,
                after: None,
                doc_base: 0,
                budget: None,
                guard: None,
                score: Vec::new(),
            };
            let (hits, _) = crate::search::search(&ix, &req, 0.0);
            let got: Vec<(u32, f64)> = hits.iter().map(|h| (h.doc, h.score)).collect();
            assert_eq!(got.len(), brute.len(), "слово {word}: число попаданий");
            for (i, ((gd, gs), (bd, bs))) in got.iter().zip(brute.iter()).enumerate() {
                assert_eq!(gd, bd, "слово {word}, место {i}: номер документа");
                assert_eq!(gs.to_bits(), bs.to_bits(), "слово {word}, место {i}: вес");
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }
}
