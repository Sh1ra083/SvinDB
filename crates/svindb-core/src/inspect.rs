//! Осмотр хранилища: что там на самом деле лежит и по чему распределено.

use crate::index::{Index, BLOCK_META};

pub struct Number {
    pub name: String,
    pub value: f64,
    pub unit: &'static str,
}

/// Распределение по корзинам.
pub struct Hist {
    pub name: String,
    /// Подпись корзины и сколько в ней.
    pub buckets: Vec<(String, u64)>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl Hist {
    fn total(&self) -> u64 {
        self.buckets.iter().map(|(_, n)| n).sum()
    }
}

/// Оценка варианта раскладки.
pub struct Variant {
    pub name: String,
    /// Байт сейчас — в той части, которой вариант касается.
    pub now: u64,
    pub then: u64,
    /// Чего оценка не учитывает. Обязательное поле: без него число врёт.
    pub note: &'static str,
}

impl Variant {
    pub fn delta(&self) -> i64 {
        self.then as i64 - self.now as i64
    }
}

/// Что нашлось при осмотре.
pub struct Stats {
    pub dir: String,
    pub segments: usize,
    pub docs: u64,
    pub terms: u64,
    pub postings: u64,
    pub positions: u64,
    /// Байт по имени файла, сложено по всем сегментам.
    pub files: Vec<(String, u64)>,
    pub numbers: Vec<Number>,
    pub hists: Vec<Hist>,
    pub variants: Vec<Variant>,
}

impl Stats {
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|(_, b)| b).sum()
    }

    /// Доля слов, встреченных ровно в одном документе, в процентах.
    pub fn singleton_share(&self) -> f64 {
        self.numbers
            .iter()
            .find(|n| n.name == "доля слов, встреченных однажды")
            .map_or(0.0, |n| n.value)
    }
}

/// Копилка корзин с подписями.
struct Buckets {
    edges: Vec<u64>,
    counts: Vec<u64>,
}

impl Buckets {
    /// Корзины по границам: `[1, 2, 5, 17, …]` даёт `1`, `2–4`, `5–16`, …
    fn new(edges: &[u64]) -> Buckets {
        Buckets { edges: edges.to_vec(), counts: vec![0; edges.len()] }
    }

    fn add(&mut self, v: u64) {
        let i = self.edges.partition_point(|&e| e <= v).saturating_sub(1);
        self.counts[i] += 1;
    }

    fn into_hist(self, name: &str) -> Hist {
        let mut buckets = Vec::with_capacity(self.counts.len());
        for (i, n) in self.counts.iter().enumerate() {
            let lo = self.edges[i];
            let label = match self.edges.get(i + 1) {
                Some(&next) if next == lo + 1 => format!("{lo}"),
                Some(&next) => format!("{lo}–{}", next - 1),
                None => format!("{lo}+"),
            };
            buckets.push((label, *n));
        }
        Hist { name: name.to_string(), buckets }
    }
}

/// Степенные корзины: 1, 2, 4, 8, … — для разностей и ширин.
fn powers(upto: u32) -> Vec<u64> {
    let mut out = vec![0u64, 1];
    let mut v = 2u64;
    while v <= upto as u64 {
        out.push(v);
        v *= 2;
    }
    out
}

/// Сколько байт займут `n` значений шириной `width` в нынешней упаковке.
fn packed(n: usize, width: u32) -> u64 {
    crate::bits::packed_len(n, width) as u64
}

/// Ширина, нужная для наибольшего из значений.
fn width_for(max: u32, min: u32) -> u32 {
    (32 - max.leading_zeros()).max(min)
}

/// Сырьё, собранное за один проход по сегменту.
#[derive(Default)]
struct Raw {
    docs: u64,
    terms: u64,
    postings: u64,
    positions: u64,
    /// Списки с одним документом: сколько их и сколько занимают.
    singletons: u64,
    singleton_post_bytes: u64,
    singleton_pos_bytes: u64,
    /// Блоки, где все частоты единичны.
    all_one_blocks: u64,
    blocks: u64,
    /// Байты списков и позиций по коротким спискам (df ниже порога).
    short_blocks: [u64; SHORT.len()],
    /// Списки в один блок и из них — те, у кого потолок блока побитово равен
    /// потолку списка.
    one_block_lists: u64,
    one_block_same_max: u64,
    /// Нынешний объём позиций и он же при разной нарезке.
    pos_now: u64,
    pos_by_group: [u64; GROUPS.len()],
    pos_gap1: u64,
    /// Разности указателя блока позиций — для оценки якорного pos.idx.
    posidx_deltas: Vec<u32>,
}

/// Пороги короткого списка, для которых считается цена метаданных блоков.
const SHORT: [u32; 3] = [2, 128, 256];
/// Размеры блока позиций в **значениях**, а не в документах.
const GROUPS: [usize; 3] = [64, 128, 256];

/// Осматривает индекс. Работает по исправному: целость — забота `check`.
pub fn stats(dir: &str) -> std::io::Result<Stats> {
    let segs = crate::segments::Segments::load(dir)?;
    let mut raw = Raw::default();
    let mut df = Buckets::new(&[1, 2, 5, 17, 129, 257]);
    let mut tf = Buckets::new(&powers(1 << 20));
    let mut dgap = Buckets::new(&powers(1 << 20));
    let mut pgap = Buckets::new(&powers(1 << 20));
    let mut dwidth = Buckets::new(&(0u64..=32).collect::<Vec<_>>());
    let mut pwidth = Buckets::new(&(0u64..=32).collect::<Vec<_>>());
    let mut per_block = Buckets::new(&powers(1 << 16));

    for ix in segs.list.iter() {
        segment(
            ix,
            &mut raw,
            &mut df,
            &mut tf,
            &mut dgap,
            &mut pgap,
            &mut dwidth,
            &mut pwidth,
            &mut per_block,
        );
    }

    let files = files_of(dir);
    let total: u64 = files.iter().map(|(_, b)| b).sum();
    let d = raw.docs.max(1) as f64;
    let p = raw.postings.max(1) as f64;
    let t = raw.terms.max(1) as f64;

    let byname = |n: &str| files.iter().find(|(f, _)| f == n).map_or(0, |(_, b)| *b);
    let mut numbers = vec![
        Number {
            name: "всего на документ".into(), value: total as f64 / d, unit: "Б/док"
        },
        Number { name: "слов на документ".into(), value: t / d, unit: "" },
        Number { name: "постингов на документ".into(), value: p / d, unit: "" },
        Number {
            name: "вхождений на постинг".into(), value: raw.positions as f64 / p, unit: ""
        },
        Number {
            name: "списки на постинг".into(),
            value: byname("postings.bin") as f64 / p,
            unit: "Б",
        },
        Number {
            name: "метаданные блока на постинг".into(),
            value: byname("blocks.bin") as f64 / p,
            unit: "Б",
        },
        Number {
            name: "словарь на слово".into(),
            value: byname("dict.blob") as f64 / t,
            unit: "Б",
        },
        Number {
            name: "метаданные слова".into(),
            value: byname("terms.bin") as f64 / t,
            unit: "Б",
        },
        Number {
            name: "позиции на вхождение".into(),
            value: byname("pos.bin") as f64 / raw.positions.max(1) as f64,
            unit: "Б",
        },
        Number {
            name: "доля слов, встреченных однажды".into(),
            value: raw.singletons as f64 * 100.0 / t,
            unit: "%",
        },
        Number {
            name: "доля блоков с единичными частотами".into(),
            value: raw.all_one_blocks as f64 * 100.0 / raw.blocks.max(1) as f64,
            unit: "%",
        },
    ];
    for (name, bytes) in files.iter() {
        numbers.push(Number {
            name: format!("файл {name}"),
            value: *bytes as f64 / d,
            unit: "Б/док",
        });
    }

    let hists = vec![
        df.into_hist("документов у слова"),
        tf.into_hist("частота слова в документе"),
        dgap.into_hist("разность номеров документов"),
        pgap.into_hist("разность позиций"),
        dwidth.into_hist("ширина полосы номеров, бит"),
        pwidth.into_hist("ширина полосы позиций, бит"),
        per_block.into_hist("значений позиций на блок"),
    ];

    let variants = variants(&raw, &files);
    Ok(Stats {
        dir: dir.into(),
        segments: segs.list.len(),
        docs: raw.docs,
        terms: raw.terms,
        postings: raw.postings,
        positions: raw.positions,
        files,
        numbers,
        hists,
        variants,
    })
}

#[allow(clippy::too_many_arguments)]
fn segment(
    ix: &Index,
    raw: &mut Raw,
    df: &mut Buckets,
    tf: &mut Buckets,
    dgap: &mut Buckets,
    pgap: &mut Buckets,
    dwidth: &mut Buckets,
    pwidth: &mut Buckets,
    per_block: &mut Buckets,
) {
    raw.docs += ix.docs as u64;
    let nterms = ix.dict.len();
    raw.terms += nterms as u64;

    let nblocks = ix.blocks_count();
    let pos_len = ix.pos_len();
    let mut off = Vec::new();
    let mut docs = vec![0u32; crate::bits::BLOCK];
    let mut tfs = vec![0u32; crate::bits::BLOCK];
    let mut poss = Vec::new();
    // Поток позиций списка: по нему считаются варианты нарезки.
    let mut stream: Vec<u32> = Vec::new();
    // Приращение это или первая позиция документа. Разница решающая для `gap-1`:
    // вычесть единицу можно из приращения — оно не меньше единицы, — а первая
    // позиция бывает нулём, и вычитание из неё портит поток.
    let mut is_gap: Vec<bool> = Vec::new();

    let mut prev_idx = 0usize;
    for list in 0..nterms {
        let n = ix.t_df(list);
        raw.postings += n as u64;
        df.add(n as u64);

        let field = ix.dict.term_at(list).0 as usize;
        let t = ix.term(list, field);
        ix.block_offsets(&t, &mut off);
        raw.blocks += off.len() as u64;

        // Указатель блока позиций. Разности нужны дважды: для оценки якорного
        // pos.idx и для **точного** объёма позиций блока — он равен расстоянию
        // до следующего указателя. Брать его моделью, когда он лежит в файле,
        // значило бы сравнивать варианты с собственной прикидкой, а не с тем,
        // что есть на диске.
        //
        // У седьмой раскладки указателя нет там, где блок один: длина куска
        // позиций лежит в самой записи слова, и брать её надо оттуда.
        let mut here_now = 0u64;
        if t.meta {
            for b in 0..off.len() {
                let blk = t.first + b;
                let at = ix.p_at(blk);
                raw.posidx_deltas.push(at.saturating_sub(prev_idx) as u32);
                prev_idx = at;
                let end = if blk + 1 < nblocks { ix.p_at(blk + 1) } else { pos_len };
                here_now += end.saturating_sub(at) as u64;
            }
        }
        if ix.layout >= 7 {
            // Хвост `pos.bin` за последним длинным списком принадлежит коротким,
            // и приписывать его последнему блоку значило бы отдать одному слову
            // половину файла. Длина куска лежит в записи слова — берём её.
            here_now = u64::from(t.pos_len);
        }
        raw.pos_now += here_now;

        stream.clear();
        is_gap.clear();
        let mut prev_doc = 0u32;
        let mut c = ix.cursor(list as u32, field, 1.0);
        for (b, &at) in off.iter().enumerate() {
            let got = ix.read_block_tf(&t, b, at, &mut docs, Some(&mut tfs));
            let mut ones = true;
            let mut max_gap = 0u32;
            for k in 0..got {
                let gap = docs[k] - prev_doc;
                prev_doc = docs[k];
                max_gap = max_gap.max(gap);
                dgap.add(gap as u64);
                let f = tfs[k].max(1);
                tf.add(f as u64);
                if f > 1 {
                    ones = false;
                }
            }
            if ones {
                raw.all_one_blocks += 1;
            }
            dwidth.add(width_for(max_gap, 1) as u64);
            let _ = b;
        }

        // Позиции — курсором: он единственный знает, где они лежат.
        let mut in_list = 0u64;
        let mut max_pos_val = 0u32;
        while c.cur() != u32::MAX {
            let got = c.positions(&mut poss);
            if got > 0 {
                let mut last = 0u32;
                for (k, &v) in poss.iter().enumerate() {
                    // Первая позиция документа лежит как есть, следующие —
                    // приращениями. Считаем то, что действительно записано.
                    let stored = if k == 0 { v } else { v - last };
                    last = v;
                    if k > 0 {
                        pgap.add(stored as u64);
                    }
                    max_pos_val = max_pos_val.max(stored);
                    stream.push(stored);
                    is_gap.push(k > 0);
                }
                in_list += got as u64;
            }
            c.next();
        }
        raw.positions += in_list;
        if !stream.is_empty() {
            pwidth.add(width_for(max_pos_val, 0) as u64);
            per_block.add(stream.len() as u64);
            tally_positions(raw, &stream, &is_gap);
        }

        // Списки из одного документа: их доля — главный вопрос к новой раскладке.
        if n == 1 {
            raw.singletons += 1;
            let bytes: u64 = if t.meta {
                off.iter().enumerate().map(|(b, _)| ix.b_len(t.first + b) as u64).sum()
            } else {
                u64::from(t.post_len)
            };
            raw.singleton_post_bytes += bytes;
            raw.singleton_pos_bytes += here_now;
        }
        for (i, &edge) in SHORT.iter().enumerate() {
            if n < edge {
                raw.short_blocks[i] += off.len() as u64;
            }
        }

        // Список в один блок: потолок блока обязан совпасть с потолком списка —
        // максимум по одному блоку и есть максимум по списку. Проверяется
        // побитово, а не с допуском: если это верно, четыре байта на блок у
        // таких списков хранятся впустую.
        //
        // Вопрос стоит только к шестой раскладке. В седьмой такой блок своих
        // метаданных не заводит вовсе — ровно потому, что это тождество здесь и
        // подтвердилось.
        if off.len() == 1 && t.meta {
            raw.one_block_lists += 1;
            if f64::from(ix.b_max(t.first)).to_bits() == t.max.to_bits() {
                raw.one_block_same_max += 1;
            }
        }
    }
}

/// Байт под поток значений, нарезанный кусками по `group`.
fn packed_stream(vals: &[u32], group: usize) -> u64 {
    let mut out = 0u64;
    for chunk in vals.chunks(group) {
        let mx = chunk.iter().copied().max().unwrap_or(0);
        out += packed(chunk.len(), width_for(mx, 0));
    }
    out
}

/// Считает поток позиций списка во всех рассматриваемых нарезках.
fn tally_positions(raw: &mut Raw, stream: &[u32], is_gap: &[bool]) {
    for (i, &g) in GROUPS.iter().enumerate() {
        raw.pos_by_group[i] += packed_stream(stream, g);
    }
    // `gap-1`: минимальная разность между вхождениями одного слова равна
    // единице, значит единицу можно не хранить. Первой позиции документа это не
    // касается — она лежит как есть и бывает нулём.
    //
    // Вычитать из всего потока подряд — ошибка, и не безобидная: она завышает
    // выигрыш в сорок раз. Ширина у блока одна на всех, и делят её первые
    // позиции с приращениями; у подавляющего большинства блоков максимум задан
    // именно первой позицией, уменьшить которую нельзя, — значит ширина не
    // меняется, и не меняется ничего.
    let shrunk: Vec<u32> = stream
        .iter()
        .zip(is_gap.iter())
        .map(|(&v, &g)| if g { v.saturating_sub(1) } else { v })
        .collect();
    raw.pos_gap1 += packed_stream(&shrunk, GROUPS[1]);
}

/// Оценки вариантов. Каждая — модель, и у каждой сказано, чего она не считает.
fn variants(raw: &Raw, files: &[(String, u64)]) -> Vec<Variant> {
    let byname = |n: &str| files.iter().find(|(f, _)| f == n).map_or(0, |(_, b)| *b);
    let mut out = Vec::new();

    // Слово, встреченное однажды, платит полную цену длинного списка:
    // метаданные блока, указатель позиций и обрамление постингов.
    let now = raw.singletons * (BLOCK_META + 4) as u64
        + raw.singleton_post_bytes
        + raw.singleton_pos_bytes;
    // В новой раскладке номер документа переезжает в метаданные слова, блока и
    // указателя не заводится вовсе. Позиция одиночного вхождения тоже уходит
    // внутрь; при нескольких вхождениях поток остаётся.
    let then = raw.singletons * 4;
    out.push(Variant {
        name: "номер документа внутрь метаданных слова (df = 1)".into(),
        now,
        then,
        note: "не учтено: метаданные слова вырастут на признак и на сам номер; \
               разбор такого слова станет другим ходом, и его цена здесь не считалась",
    });

    for (i, &edge) in SHORT.iter().enumerate() {
        out.push(Variant {
            name: format!("короткие списки без метаданных блока (df < {edge})"),
            now: raw.short_blocks[i] * BLOCK_META as u64,
            then: 0,
            note: "не учтено: без потолка блока отсечение по весу для этих списков \
                   перестанет работать, то есть это размен места на скорость, а не выигрыш",
        });
    }

    // Список в один блок хранит потолок дважды и длину — выводимую.
    //
    // Это самая дешёвая из найденных правок: она не трогает ни `postings.bin`,
    // ни `pos.bin`, ни чтение списка — только метаданные блока, которые у
    // односегментного списка повторяют метаданные слова.
    if raw.one_block_lists > 0 {
        let same = raw.one_block_same_max == raw.one_block_lists;
        out.push(Variant {
            name: format!(
                "у списка в один блок не хранить потолок и длину ({} из {} совпали побитово)",
                raw.one_block_same_max, raw.one_block_lists
            ),
            now: raw.one_block_lists * BLOCK_META as u64,
            // Остаётся последний номер документа: он у блока свой.
            then: if same {
                raw.one_block_lists * 4
            } else {
                raw.one_block_lists * BLOCK_META as u64
            },
            note: "не учтено: длина блока выводится из соседних смещений списков, \
                   значит чтение перестанет быть одним обращением; \
                   и это верно только там, где потолки совпали побитово",
        });
    }

    for (i, &g) in GROUPS.iter().enumerate() {
        out.push(Variant {
            name: format!("позиции блоками по {g} значений"),
            now: raw.pos_now,
            then: raw.pos_by_group[i],
            note: "не учтено: мелкий блок это больше байтов ширины и больше обращений \
                   при чтении фразы; цена разбора здесь не считалась",
        });
    }

    out.push(Variant {
        name: "gap-1 у разностей позиций".into(),
        now: raw.pos_by_group[1],
        then: raw.pos_gap1,
        note: "не учтено: вычитание единицы на каждое значение при чтении; \
               оценка снята при нарезке по 128 значений",
    });

    // Указатель блока позиций сейчас абсолютный `u32`. Разности у него мелкие,
    // и по ним видно, сколько дал бы якорный вид.
    let n = raw.posidx_deltas.len();
    let anchored = packed_stream(&raw.posidx_deltas, 64) + (n as u64 / 64) * 4;
    out.push(Variant {
        name: "pos.idx разностями с якорем каждые 64".into(),
        now: byname("pos.idx"),
        then: anchored,
        note: "не учтено: чтение указателя перестанет быть одним обращением — \
               понадобится якорь и разбор до нужного места внутри группы",
    });

    out
}

// ───────────────────────── прикидка новой раскладки ─────────────────────────
//
// Всё, что ниже, — **модель**, посчитанная по настоящему индексу: для каждого
// слова берётся то, что оно занимает сейчас, и то, что заняло бы в
// рассматриваемом представлении. Ни одного байта при этом не пишется.
//
// Мера везде одна — байт на документ по классам частоты. Класс здесь главный:
// вся мысль новой раскладки в том, что редкое слово не должно платить цену
// длинного списка, и увидеть это можно только с разбивкой.

/// Байты по назначению. Одна запись — один класс частоты либо весь сегмент.
#[derive(Default, Clone, Copy)]
pub struct Bytes {
    pub terms: u64,
    pub blocks: u64,
    pub posidx: u64,
    pub post: u64,
    pub pos: u64,
    pub dict: u64,
}

impl Bytes {
    pub fn total(&self) -> u64 {
        self.terms + self.blocks + self.posidx + self.post + self.pos + self.dict
    }

    fn add(&mut self, o: &Bytes) {
        self.terms += o.terms;
        self.blocks += o.blocks;
        self.posidx += o.posidx;
        self.post += o.post;
        self.pos += o.pos;
        self.dict += o.dict;
    }
}

/// Класс частоты: сколько в нём слов и во что они обходятся.
pub struct Klass {
    pub name: String,
    pub terms: u64,
    pub postings: u64,
    pub now: Bytes,
    pub then: Bytes,
}

/// Что показала прикидка.
pub struct Plan {
    pub dir: String,
    pub docs: u64,
    pub terms: u64,
    pub classes: Vec<Klass>,
    /// Способы кодирования номеров короткого списка: имя → байт по всем таким.
    pub codings: Vec<(String, u64)>,
    /// Размер блока постингов в документах → байт в `postings.bin`.
    pub blocksizes: Vec<(usize, u64)>,
    /// Списки, у которых потолок **точно** восстановился из пары «уровень длины,
    /// частота», и сколько их всего. Если сходится всё — потолок можно хранить
    /// байтом, ничего не квантуя.
    pub tmax_exact: u64,
    pub tmax_total: u64,
    /// Списки, где вклад, посчитанный **читателем**, выходит за хранимый потолок.
    ///
    /// Величина не про место, а про правильность: потолок, который меньше
    /// настоящего вклада, — не потолок, и отсечение по нему теряет документы
    /// молча. Считается той же средней длиной, какой пользуется поиск.
    pub tmax_over: u64,
    /// Насколько именно выходит — наибольшее относительное превышение.
    pub tmax_worst: f64,
    /// Словарь без байта поля на каждой записи.
    pub dict_now: u64,
    pub dict_nofield: u64,
}

impl Plan {
    pub fn now(&self) -> Bytes {
        let mut b = Bytes::default();
        for k in self.classes.iter() {
            b.add(&k.now);
        }
        b
    }

    pub fn then(&self) -> Bytes {
        let mut b = Bytes::default();
        for k in self.classes.iter() {
            b.add(&k.then);
        }
        b
    }
}

/// Байт под varint.
fn vlen(v: u64) -> usize {
    let mut n = 1;
    let mut v = v >> 7;
    while v > 0 {
        n += 1;
        v >>= 7;
    }
    n
}

/// Границы классов частоты. Те же, что просил разбор.
const KLASSES: [(u32, u32); 6] = [(1, 1), (2, 4), (5, 16), (17, 128), (129, 256), (257, u32::MAX)];

fn klass_of(df: u32) -> usize {
    KLASSES.iter().position(|&(lo, hi)| df >= lo && df <= hi).unwrap_or(KLASSES.len() - 1)
}

fn klass_name(i: usize) -> String {
    let (lo, hi) = KLASSES[i];
    if hi == u32::MAX {
        format!("{lo}+")
    } else if lo == hi {
        format!("{lo}")
    } else {
        format!("{lo}–{hi}")
    }
}

/// Байт под поток номеров в разных представлениях.
mod coding {
    /// Нынешнее: байт ширины и `n` значений подряд.
    pub fn bitpack(vals: &[u32]) -> u64 {
        let mx = vals.iter().copied().max().unwrap_or(0);
        let w = (32 - mx.leading_zeros()).max(1) as usize;
        1 + (vals.len() * w).div_ceil(8) as u64
    }

    /// Байт на значение по семь бит.
    pub fn vbyte(vals: &[u32]) -> u64 {
        vals.iter().map(|&v| super::vlen(v as u64) as u64).sum()
    }

    /// Длина значения тут кратна байту, а не биту: за это платится место, а
    /// взамен разбор идёт без сдвигов, таблицей перестановки.
    pub fn stream_vbyte(vals: &[u32]) -> u64 {
        let mut n = vals.len().div_ceil(4) as u64;
        for &v in vals {
            n += match v {
                0..=0xff => 1,
                0x100..=0xffff => 2,
                0x1_0000..=0xff_ffff => 3,
                _ => 4,
            };
        }
        n
    }

    /// FOR: вычесть наименьшее и упаковать. Байт ширины плюс само наименьшее.
    pub fn frame(vals: &[u32]) -> u64 {
        let mn = vals.iter().copied().min().unwrap_or(0);
        let mx = vals.iter().copied().max().unwrap_or(0);
        let w = (32 - (mx - mn).leading_zeros()).max(1) as usize;
        1 + super::vlen(mn as u64) as u64 + (vals.len() * w).div_ceil(8) as u64
    }

    /// PFor: ширина по девяноста процентам значений, остальные — исключениями.
    ///
    /// Исключение стоит байт места внутри блока и четыре байта значения в
    /// хвосте. Ширина перебирается вся: выбирается та, что даёт меньше байт.
    pub fn pfor(vals: &[u32]) -> u64 {
        let mut best = u64::MAX;
        for w in 1..=32u32 {
            let cap = if w >= 32 { u32::MAX } else { (1u32 << w) - 1 };
            let ex = vals.iter().filter(|&&v| v > cap).count() as u64;
            let n = 2 + (vals.len() * w as usize).div_ceil(8) as u64 + ex * 5;
            best = best.min(n);
        }
        best
    }

    /// Элиас-Фано по возрастающей последовательности номеров.
    ///
    /// Считается по самим номерам, а не по разностям: в этом весь приём —
    /// старшие биты идут унарно и занимают два бита на значение независимо от
    /// вселенной, младшие лежат как есть.
    pub fn elias_fano(docs: &[u32]) -> u64 {
        let n = docs.len() as u64;
        if n == 0 {
            return 0;
        }
        let u = docs.last().copied().unwrap_or(0) as u64 + 1;
        let low = (u / n).max(1).ilog2() as u64;
        // Верхняя часть: `n` единиц и не больше `n` нулей — два бита на значение.
        let bits = n * low + 2 * n;
        1 + bits.div_ceil(8)
    }
}

/// Прикидывает новую раскладку по настоящему индексу. Ничего не пишет.
pub fn plan(dir: &str) -> std::io::Result<Plan> {
    let segs = crate::segments::Segments::load(dir)?;
    let mut classes: Vec<Klass> = (0..KLASSES.len())
        .map(|i| Klass {
            name: klass_name(i),
            terms: 0,
            postings: 0,
            now: Bytes::default(),
            then: Bytes::default(),
        })
        .collect();
    let mut codings: Vec<(String, u64)> = [
        "побитово, как сейчас",
        "VByte",
        "StreamVByte",
        "FOR",
        "PFor с исключениями",
        "Элиас-Фано",
    ]
    .iter()
    .map(|n| (n.to_string(), 0u64))
    .collect();
    let mut blocksizes: Vec<(usize, u64)> = vec![(64, 0), (128, 0), (256, 0)];
    let mut docs = 0u64;
    let mut terms = 0u64;
    let mut ceil = Ceiling::default();
    let mut dict_now = 0u64;
    let mut dict_nofield = 0u64;

    for ix in segs.list.iter() {
        docs += ix.docs as u64;
        terms += ix.dict.len() as u64;
        plan_segment(
            ix,
            &mut classes,
            &mut codings,
            &mut blocksizes,
            &mut ceil,
            &mut dict_now,
            &mut dict_nofield,
        );
    }

    Ok(Plan {
        dir: dir.into(),
        docs,
        terms,
        classes,
        codings,
        blocksizes,
        tmax_exact: ceil.exact,
        tmax_total: ceil.total,
        tmax_over: ceil.over,
        tmax_worst: ceil.worst,
        dict_now,
        dict_nofield,
    })
}

/// Сверка хранимого потолка списка с тем, что посчитает читатель.
#[derive(Default)]
struct Ceiling {
    total: u64,
    exact: u64,
    over: u64,
    worst: f64,
}

#[allow(clippy::too_many_arguments)]
fn plan_segment(
    ix: &Index,
    classes: &mut [Klass],
    codings: &mut [(String, u64)],
    blocksizes: &mut [(usize, u64)],
    ceil: &mut Ceiling,
    dict_now: &mut u64,
    dict_nofield: &mut u64,
) {
    let nterms = ix.dict.len();
    let nblocks = ix.blocks_count();
    let pos_len = ix.pos_len();
    let mut off = Vec::new();
    let mut dbuf = vec![0u32; crate::bits::BLOCK];
    let mut fbuf = vec![0u32; crate::bits::BLOCK];
    let mut poss = Vec::new();

    // Словарь идёт тем же порядком, что и списки: номер слова и есть номер
    // списка. Идём по нему проходом, а не обращением по номеру, — обращение у
    // фронт-кодированного словаря каждый раз начинает от опоры.
    let mut w = ix.dict.walk();
    let mut prev_term: Vec<u8> = Vec::new();
    let mut cur_term: Vec<u8> = Vec::new();
    let mut prev_field = u32::MAX;
    let mut alldocs: Vec<u32> = Vec::new();
    let mut alltfs: Vec<u32> = Vec::new();

    let mut i = 0usize;
    while let Some((fld, bytes, _)) = w.next() {
        if i >= nterms {
            break;
        }
        cur_term.clear();
        cur_term.extend_from_slice(bytes);
        let list = i;
        i += 1;

        // ── словарь: запись сейчас и она же без байта поля на каждой записи ──
        let changed = fld != prev_field || list.is_multiple_of(crate::dict::ANCHOR);
        let common = if changed {
            0
        } else {
            cur_term.iter().zip(prev_term.iter()).take_while(|(a, b)| a == b).count()
        };
        let tail = cur_term.len() - common;
        // Без байта поля: младший бит длины совпадения говорит, что поле
        // сменилось. Байт поля пишется только тогда — и на опоре, где он нужен
        // двоичному поиску.
        let then_dict = vlen(((common as u64) << 1) | u64::from(changed))
            + vlen(tail as u64)
            + tail
            + usize::from(changed);
        // «Сейчас» берётся по той раскладке, что на диске, а не по одной
        // выбранной. Иначе прикидка, запущенная на новом сегменте, показала бы
        // выигрыш, который уже взят.
        let here_dict = if ix.layout >= 7 {
            then_dict
        } else {
            vlen(common as u64) + vlen(tail as u64) + 1 + tail
        };
        *dict_now += here_dict as u64;
        *dict_nofield += then_dict as u64;
        std::mem::swap(&mut prev_term, &mut cur_term);
        prev_field = fld;

        let field = fld as usize;
        let t = ix.term(list, field);
        let df = t.df;
        let positioned = ix.fields.get(field).map(|f| f.positioned).unwrap_or(false);
        ix.block_offsets(&t, &mut off);

        // ── что занимает сейчас ──
        let meta_blocks = if t.meta { off.len() } else { 0 };
        let mut here = Bytes {
            terms: u64::from(t.rec_len)
                + if ix.layout >= 7 { 16 / crate::dict::ANCHOR as u64 } else { 0 },
            blocks: (meta_blocks * BLOCK_META) as u64,
            posidx: (meta_blocks * 4) as u64,
            post: 0,
            pos: 0,
            dict: here_dict as u64,
        };
        if ix.layout >= 7 {
            here.post = u64::from(t.post_len);
            here.pos = u64::from(t.pos_len);
        } else {
            for b in 0..off.len() {
                let blk = t.first + b;
                here.post += ix.b_len(blk) as u64;
                let end = if blk + 1 < nblocks { ix.p_at(blk + 1) } else { pos_len };
                here.pos += end.saturating_sub(ix.p_at(blk)) as u64;
            }
        }

        // ── содержимое списка: номера, частоты, позиции ──
        alldocs.clear();
        alltfs.clear();
        for (b, &at) in off.iter().enumerate() {
            let got = ix.read_block_tf(&t, b, at, &mut dbuf, Some(&mut fbuf));
            alldocs.extend_from_slice(&dbuf[..got]);
            alltfs.extend_from_slice(&fbuf[..got]);
        }
        // Признак ставится только короткому списку: у длинного он требует знать
        // все частоты до первого блока, а слияние идёт потоком. См. `write_run`.
        let tf_all_one =
            (df as usize) <= crate::bits::BLOCK && alltfs.iter().all(|&t| t.max(1) == 1);

        // Потолок списка: пара «уровень длины, частота», дающая максимум. Если
        // потолок из неё восстанавливается точно, хранить четыре байта `f32` не
        // нужно — довольно байта уровня.
        let mut best = (0f64, 0u8, 1u32);
        for (k, &doc) in alldocs.iter().enumerate() {
            let q = ix.level(field, doc);
            let tf = alltfs[k].max(1);
            let v = ix.sat(field, doc, tf);
            if v > best.0 {
                best = (v, q, tf);
            }
        }
        if df > 0 {
            let stored = t.max;
            ceil.total += 1;
            if (best.0 as f32).to_bits() == (stored as f32).to_bits() {
                ceil.exact += 1;
            }
            // Тот же вклад, что посчитает поиск: `sat` по средней длине из
            // заголовка. Выйти за хранимый потолок он не имеет права.
            if best.0 > stored {
                ceil.over += 1;
                ceil.worst = ceil.worst.max((best.0 - stored) / stored.max(f64::MIN_POSITIVE));
            }
        }

        // Первая позиция первого документа — её одиночное слово унесёт внутрь
        // метаданных, если вхождение всего одно.
        let mut first_pos = 0u32;
        if df == 1 && positioned {
            let c = ix.cursor(list as u32, field, 1.0);
            if c.positions(&mut poss) == 1 {
                first_pos = poss[0];
            }
        }

        // ── что заняло бы в новой раскладке ──
        let word = Word {
            df,
            docs: &alldocs,
            tfs: &alltfs,
            tf_all_one,
            positioned,
            first_pos,
            pos_bytes: here.pos,
            dict: then_dict as u64,
            tfmax: best.2,
        };
        let then = then_of(&word, crate::bits::BLOCK);

        let k = klass_of(df);
        classes[k].terms += 1;
        classes[k].postings += df as u64;
        classes[k].now.add(&here);
        classes[k].then.add(&then);

        // ── способы кодирования короткого списка ──
        //
        // Считаются только по спискам, которые целиком лежат в одном блоке:
        // именно им предлагается своё представление, и сравнивать надо их.
        if df >= 2 && (df as usize) <= crate::bits::BLOCK {
            let mut prev = 0u32;
            let deltas: Vec<u32> = alldocs
                .iter()
                .map(|&d| {
                    let g = d - prev;
                    prev = d;
                    g
                })
                .collect();
            codings[0].1 += coding::bitpack(&deltas);
            codings[1].1 += coding::vbyte(&deltas);
            codings[2].1 += coding::stream_vbyte(&deltas);
            codings[3].1 += coding::frame(&deltas);
            codings[4].1 += coding::pfor(&deltas);
            codings[5].1 += coding::elias_fano(&alldocs);
        }

        // ── размер блока постингов ──
        //
        // Считается **вся** раскладка при каждом размере, а не один
        // `postings.bin`. Размер блока — это ещё и граница между коротким
        // списком и длинным: мельче блок — плотнее приращения, но больше слов
        // переходит в длинный класс и заводит себе метаданные блоков и
        // указатель позиций. По одному файлу выигрывает 64, по всей раскладке
        // ответ может быть другим, и ради этого счёт и переделан.
        for (size, acc) in blocksizes.iter_mut() {
            *acc += then_of(&word, *size).total();
        }
    }
}

/// Слово со всем, что нужно, чтобы прикинуть его цену.
struct Word<'a> {
    df: u32,
    docs: &'a [u32],
    tfs: &'a [u32],
    tf_all_one: bool,
    positioned: bool,
    /// Позиция единственного вхождения — её унесёт внутрь метаданных слова.
    first_pos: u32,
    /// Сколько позиции слова занимают сейчас. В новой раскладке столько же:
    /// сам поток позиций не меняется, меняется только то, как его находят.
    pos_bytes: u64,
    dict: u64,
    /// Частота, дающая потолок списка.
    tfmax: u32,
}

/// Во что обошлось бы слово при блоке в `block` документов.
///
/// `block` здесь двойного назначения: и размер куска в `postings.bin`, и
/// граница класса. Список, целиком помещающийся в один блок, метаданных блока
/// не заводит — у него потолок блока и есть потолок списка, а пропускать внутри
/// одного блока нечего.
fn then_of(w: &Word, block: usize) -> Bytes {
    let mut then = Bytes { dict: w.dict, ..Bytes::default() };
    // Опорная запись на каждые ANCHOR слов: смещения в трёх файлах и номер
    // первого блока. Раскладывается по словам поровну.
    then.terms = 16 / crate::dict::ANCHOR as u64;
    then.terms += vlen(u64::from(w.df) << crate::layout::SHIFT) as u64;

    if w.df == 1 {
        let tf = w.tfs.first().copied().unwrap_or(1).max(1);
        then.terms += vlen(u64::from(w.docs[0])) as u64;
        if !w.tf_all_one {
            then.terms += vlen(u64::from(tf) - 2) as u64;
        }
        if w.positioned {
            if tf == 1 {
                // Единственное вхождение уезжает в метаданные слова целиком, и
                // в `pos.bin` от такого слова не остаётся ничего.
                then.terms += vlen(u64::from(w.first_pos)) as u64;
            } else {
                then.terms += vlen(w.pos_bytes) as u64;
                then.pos = w.pos_bytes;
            }
        }
        // Ни блока, ни указателя, ни куска в списках: всё внутри слова.
        return then;
    }

    // Потолок — байтом уровня длины плюс частота, когда та не единица.
    then.terms += 1;
    if !w.tf_all_one {
        then.terms += vlen(u64::from(w.tfmax) - 1) as u64;
    }

    // Кусок списка: номера побитово, частоты — только если они не все единичны.
    // Это и есть отказ от байта ширины на самый частый случай.
    //
    // Счёт приращений сквозной по списку, а не по блоку: первое значение блока
    // отсчитывается от последнего номера предыдущего. Обнулять его на каждом
    // блоке — не мелкая неточность: тогда первое приращение равно самому номеру
    // документа, а ширина в блоке одна на всех, и модель завышает длинные
    // списки на треть.
    let mut post = 0u64;
    let mut prev = 0u32;
    for chunk in w.docs.chunks(block) {
        let deltas: Vec<u32> = chunk
            .iter()
            .map(|&d| {
                let g = d - prev;
                prev = d;
                g
            })
            .collect();
        post += coding::bitpack(&deltas);
    }
    if !w.tf_all_one {
        for chunk in w.tfs.chunks(block) {
            let vals: Vec<u32> = chunk.iter().map(|&t| t.max(1) - 1).collect();
            let mx = vals.iter().copied().max().unwrap_or(0);
            let bw = (32 - mx.leading_zeros()) as usize;
            post += 1 + (vals.len() * bw).div_ceil(8) as u64;
        }
    }
    then.post = post;
    then.terms += vlen(post) as u64;
    if w.positioned {
        then.terms += vlen(w.pos_bytes) as u64;
        then.pos = w.pos_bytes;
    }

    // Метаданные блока и указатель позиций остаются только длинным спискам.
    if w.df as usize > block {
        let nblocks = (w.df as usize).div_ceil(block) as u64;
        then.blocks = nblocks * BLOCK_META as u64;
        then.posidx = nblocks * 4;
    }
    then
}

// ─────────────────────── поток позиций: чем его кодировать ───────────────────
//
// Позиции упакованы **вместе с блоком постингов**: одна ширина на все значения
// всех ста двадцати восьми документов блока. Отсюда вопрос, ради которого всё
// ниже и написано: задаёт ли один длинный документ разрядность остальным и
// сколько это стоит.

/// Способ кодирования потока позиций.
pub struct Coding {
    pub name: String,
    pub bytes: u64,
    /// Сколько значений придётся разобрать, чтобы прочесть **одно** нужное.
    ///
    /// Вторая ось размена, и без неё таблица размеров врёт. У упаковки
    /// постоянной ширины любое значение достаётся сдвигом — единица. Там, где
    /// длина значения зависит от значения, приходится идти от начала куска.
    pub scan: f64,
    pub note: &'static str,
}

/// Что показал разбор потока позиций.
pub struct PosPlan {
    pub dir: String,
    pub docs: u64,
    pub postings: u64,
    pub values: u64,
    /// Байт под позиции сейчас — по записям слов, а не по размеру файла.
    pub now: u64,
    /// Байт, ушедших на разрядность, заданную **чужим** документом.
    pub foreign: u64,
    /// Сколько кусков даёт жадная нарезка и сколько в них значений.
    pub chunks: u64,
    pub codings: Vec<Coding>,
    /// Во сколько раз ширина куска шире, чем нужно самому документу.
    pub inflation: Hist,
}

/// Размеры куска позиций, которые сравниваются.
const POSBLOCK: [usize; 6] = [8, 16, 32, 64, 128, 256];

/// Байт под varint.
fn vb(v: u32) -> u64 {
    vlen(u64::from(v)) as u64
}

/// Способы: имя, сколько значений надо разобрать ради одного, оговорка.
const CODINGS: [(&str, f64, &str); 17] = [
    (
        "побитово, 8 значений",
        1.0,
        "не учтено: байт ширины на восемь значений — это бит на значение",
    ),
    ("побитово, 16 значений", 1.0, "не учтено: заголовков вдесятеро больше, чем сейчас"),
    (
        "побитово, 32 значения",
        1.0,
        "не учтено: вчетверо больше заголовков и обращений при обходе подряд",
    ),
    ("побитово, 64 значения", 1.0, "не учтено: вдвое больше заголовков"),
    ("побитово, 128 значений", 1.0, "не учтено: ничего сверх места — это раскладка Lucene"),
    ("побитово, 256 значений", 1.0, "не учтено: разрядность задаётся вдвое большим числом соседей"),
    (
        "PFor, 128, с исключениями",
        64.0,
        "не учтено: исключение — ветка на значение, и своё значение без прохода не найти",
    ),
    (
        "VByte по всему потоку",
        64.0,
        "не учтено: произвольного доступа нет вовсе, только проход от начала",
    ),
    ("StreamVByte по всему потоку", 64.0, "не учтено: то же, плюс таблица перестановок"),
    ("побитово 128, короткий хвост VByte", 16.0, "не учтено: два разборщика вместо одного"),
    ("побитово 128 и gap−1", 1.0, "не учтено: вычитание единицы на каждое значение при чтении"),
    (
        "раздельно: первые и приращения, по 128",
        1.0,
        "не учтено: два потока — два указателя и два номера на документ",
    ),
    (
        "по границам документов, порог 2 бита",
        1.0,
        "заголовок учтён; не учтён проход по кускам при произвольном доступе",
    ),
    ("по границам документов, порог 4 бита", 1.0, "то же"),
    (
        "по границам документов, порог 8 бит",
        1.0,
        "то же; порог равен заголовку — решение «местно честное»",
    ),
    (
        "по совпадению разрядности, без порога",
        1.0,
        "кусок это ряд документов одной ширины; параметра нет вовсе",
    ),
    (
        "нижняя граница: своя ширина у документа",
        1.0,
        "недостижима: заголовков нет вовсе, делить разрядность не с кем",
    ),
];

/// Разбирает поток позиций. Работает по исправному индексу.
pub fn positions(dir: &str) -> std::io::Result<PosPlan> {
    let segs = crate::segments::Segments::load(dir)?;
    let mut p = PosPlan {
        dir: dir.into(),
        docs: 0,
        postings: 0,
        values: 0,
        now: 0,
        foreign: 0,
        chunks: 0,
        codings: Vec::new(),
        inflation: Hist { name: String::new(), buckets: Vec::new() },
    };
    let mut acc = vec![0u64; CODINGS.len()];
    let mut infl = Buckets::new(&[1, 2, 3, 5, 9, 17]);
    let (mut floor_bits, mut block_bits) = (0u64, 0u64);

    let mut stream: Vec<u32> = Vec::new();
    let mut starts: Vec<usize> = Vec::new();
    let mut poss = Vec::new();

    for ix in segs.list.iter() {
        p.docs += ix.docs as u64;
        for list in 0..ix.dict.len() {
            let fld = ix.dict.term_at(list).0 as usize;
            let t = ix.term(list, fld);
            p.postings += u64::from(t.df);
            p.now += u64::from(t.pos_len) + u64::from(t.one.and_then(|r| r.inline_pos).is_some());

            stream.clear();
            starts.clear();
            let mut c = ix.cursor(list as u32, fld, 1.0);
            while c.cur() != u32::MAX {
                if c.positions(&mut poss) > 0 {
                    starts.push(stream.len());
                    let mut last = 0u32;
                    for (k, &v) in poss.iter().enumerate() {
                        stream.push(if k == 0 { v } else { v - last });
                        last = v;
                    }
                }
                c.next();
            }
            if stream.is_empty() {
                continue;
            }
            p.values += stream.len() as u64;
            starts.push(stream.len());

            // Разрядность, нужная самому документу, против той, что досталась
            // ему от соседей по куску.
            let whole = width_for(stream.iter().copied().max().unwrap_or(0), 0);
            for w in starts.windows(2) {
                let mine = width_for(stream[w[0]..w[1]].iter().copied().max().unwrap_or(0), 0);
                floor_bits += ((w[1] - w[0]) as u64) * u64::from(mine);
                block_bits += ((w[1] - w[0]) as u64) * u64::from(whole);
                infl.add(u64::from(whole.max(1) / mine.max(1)));
            }
            p.chunks += tally_codings(&mut acc, &stream, &starts);
        }
    }

    p.foreign = block_bits.saturating_sub(floor_bits).div_ceil(8);
    p.inflation = infl.into_hist("во сколько раз ширина куска шире нужной документу");
    p.codings = CODINGS
        .iter()
        .zip(acc.iter())
        .map(|(&(name, scan, note), &bytes)| Coding { name: name.into(), bytes, scan, note })
        .collect();
    Ok(p)
}

/// Считает поток списка во всех способах сразу.
fn tally_codings(acc: &mut [u64], stream: &[u32], starts: &[usize]) -> u64 {
    for (i, &n) in POSBLOCK.iter().enumerate() {
        acc[i] += packed_stream(stream, n);
    }
    let n = POSBLOCK.len();
    for chunk in stream.chunks(128) {
        acc[n] += pfor_len(chunk);
    }
    acc[n + 1] += stream.iter().map(|&v| vb(v)).sum::<u64>();
    acc[n + 2] += stream_vbyte_len(stream);
    // Хвостовой кусок почти всегда неполный: у большинства списков значений
    // единицы, и байт ширины на них — половина записи.
    for chunk in stream.chunks(128) {
        acc[n + 3] += if chunk.len() >= 16 {
            packed(chunk.len(), width_for(chunk.iter().copied().max().unwrap_or(0), 0))
        } else {
            chunk.iter().map(|&v| vb(v)).sum::<u64>()
        };
    }
    // gap−1: единицу можно не хранить только у приращения. Первая позиция
    // документа лежит абсолютной и бывает нулём.
    let mut shrunk: Vec<u32> = stream.to_vec();
    for w in starts.windows(2) {
        for v in shrunk[w[0] + 1..w[1]].iter_mut() {
            *v -= 1;
        }
    }
    acc[n + 4] += packed_stream(&shrunk, 128);
    // Раздельные потоки: абсолютные первые растут с длиной документа,
    // приращения — нет. Вместе они делят одну ширину, порознь — каждый свою.
    let mut firsts: Vec<u32> = Vec::with_capacity(starts.len());
    let mut gaps: Vec<u32> = Vec::with_capacity(stream.len());
    for w in starts.windows(2) {
        firsts.push(stream[w[0]]);
        gaps.extend_from_slice(&stream[w[0] + 1..w[1]]);
    }
    acc[n + 5] += packed_stream(&firsts, 128) + packed_stream(&gaps, 128);

    // Нарезка по **границам документов**, жадно.
    //
    // Ни один размер куска в значениях не годится: длинный документ приносит
    // сразу много широких значений, и куски вокруг него всё равно платят. Резать
    // надо там, где меняется распределение, а меняется оно на границе документа.
    //
    // Правило: пока добавить документ в текущий кусок дешевле, чем закрыть кусок
    // и начать новый, — добавляем. Порог — во что обходится новый заголовок;
    // он же решает, насколько охотно нарезка дробит поток, поэтому меряется
    // тремя значениями, а не берётся с потолка.
    let mut chunks = 0u64;
    for (v, thr) in [2u64, 4, 8].iter().enumerate() {
        let (mut cur_n, mut cur_w, mut bits, mut heads) = (0u64, 0u32, 0u64, 0u64);
        for w in starts.windows(2) {
            let k = (w[1] - w[0]) as u64;
            let wd = width_for(stream[w[0]..w[1]].iter().copied().max().unwrap_or(0), 0);
            let merged = (cur_n + k) * u64::from(cur_w.max(wd));
            let split = cur_n * u64::from(cur_w) + thr + k * u64::from(wd);
            if cur_n > 0 && split < merged {
                bits += cur_n * u64::from(cur_w);
                heads += 1 + if cur_n <= 7 { 0 } else { vlen(cur_n) as u64 };
                cur_n = k;
                cur_w = wd;
            } else {
                cur_w = cur_w.max(wd);
                cur_n += k;
            }
        }
        bits += cur_n * u64::from(cur_w);
        heads += 1 + if cur_n <= 7 { 0 } else { vlen(cur_n) as u64 };
        acc[n + 6 + v] += bits.div_ceil(8) + heads;
        if v == 2 {
            chunks = heads;
        }
    }
    // Без порога вовсе: кусок — ряд документов одной разрядности. Параметра
    // нет, значит и подбирать нечего; зато кусков больше всего.
    {
        let (mut cur_n, mut cur_w, mut bits, mut heads) = (0u64, 0u32, 0u64, 0u64);
        for w in starts.windows(2) {
            let k = (w[1] - w[0]) as u64;
            let wd = width_for(stream[w[0]..w[1]].iter().copied().max().unwrap_or(0), 0);
            if cur_n > 0 && wd != cur_w {
                bits += cur_n * u64::from(cur_w);
                heads += 1 + if cur_n <= 7 { 0 } else { vlen(cur_n) as u64 };
                cur_n = 0;
            }
            cur_w = wd;
            cur_n += k;
        }
        bits += cur_n * u64::from(cur_w);
        heads += 1 + if cur_n <= 7 { 0 } else { vlen(cur_n) as u64 };
        acc[n + 9] += bits.div_ceil(8) + heads;
    }

    let mut floor = 0u64;
    for w in starts.windows(2) {
        let mine = width_for(stream[w[0]..w[1]].iter().copied().max().unwrap_or(0), 0);
        floor += ((w[1] - w[0]) as u64) * u64::from(mine);
    }
    acc[n + 10] += floor.div_ceil(8);
    chunks
}

/// PFor: перебирает ширину и берёт ту, где место с исключениями наименьшее.
///
/// Исключение стоит байт места внутри куска и varint значения в хвосте.
fn pfor_len(vals: &[u32]) -> u64 {
    let mut best = u64::MAX;
    for w in 0..=32u32 {
        let cap = if w >= 32 { u32::MAX } else { (1u32 << w) - 1 };
        let extra: u64 = vals.iter().filter(|&&v| v > cap).map(|&v| 1 + vb(v)).sum();
        best = best.min(2 + (vals.len() * w as usize).div_ceil(8) as u64 + extra);
    }
    best
}

fn stream_vbyte_len(vals: &[u32]) -> u64 {
    let mut n = vals.len().div_ceil(4) as u64;
    for &v in vals {
        n += match v {
            0..=0xff => 1,
            0x100..=0xffff => 2,
            0x1_0000..=0xff_ffff => 3,
            _ => 4,
        };
    }
    n
}

// ───────────────── указатель на позиции: чем адресовать блоки ────────────────

/// Способ адресации потока позиций.
pub struct Addr {
    pub name: String,
    pub bytes: u64,
    /// Сколько значений надо прочесть, чтобы достать одно. Единица — прямое
    /// обращение; больше — проход внутри опорной группы.
    pub scan: f64,
    pub note: &'static str,
}

/// Что показал разбор `pos.idx`.
pub struct IdxPlan {
    pub dir: String,
    pub docs: u64,
    pub entries: u64,
    pub entries_all: u64,
    /// Записей, указывающих в никуда: у поля нет позиций, и разность нулевая.
    pub empty: u64,
    pub now: u64,
    pub ways: Vec<Addr>,
    pub deltas: Hist,
}

/// Разбирает адресацию позиций.
pub fn posidx(dir: &str) -> std::io::Result<IdxPlan> {
    let segs = crate::segments::Segments::load(dir)?;
    let mut p = IdxPlan {
        dir: dir.into(),
        docs: 0,
        entries: 0,
        entries_all: 0,
        empty: 0,
        now: 0,
        ways: Vec::new(),
        deltas: Hist { name: String::new(), buckets: Vec::new() },
    };
    let mut hist = Buckets::new(&powers(1 << 22));
    let mut all: Vec<u64> = Vec::new();

    for ix in segs.list.iter() {
        p.docs += ix.docs as u64;
        let n = ix.blocks_count();
        let pos_len = ix.pos_len();
        p.entries += n as u64;
        p.now += (n * 4) as u64;
        for list in 0..ix.dict.len() {
            let fld = ix.dict.term_at(list).0 as usize;
            p.entries_all += ix.term(list, fld).blocks as u64;
        }
        let base = all.len();
        for b in 0..n {
            let at = ix.p_at(b);
            let end = if b + 1 < n { ix.p_at(b + 1) } else { pos_len };
            let d = end.saturating_sub(at) as u64;
            if d == 0 {
                p.empty += 1;
            }
            hist.add(d);
            all.push(at as u64);
        }
        let _ = base;
    }
    p.deltas = hist.into_hist("байт позиций на блок длинного списка");
    p.ways = ways_of(&all, p.entries);
    Ok(p)
}

/// Считает все рассматриваемые способы по одному ряду смещений.
fn ways_of(off: &[u64], n: u64) -> Vec<Addr> {
    let mut out = Vec::new();
    let deltas: Vec<u64> =
        off.windows(2).map(|w| w[1].saturating_sub(w[0])).chain(std::iter::once(0)).collect();
    out.push(Addr {
        name: "сейчас: абсолютный u32 на блок".into(),
        bytes: n * 4,
        scan: 1.0,
        note: "прямое обращение по номеру блока, без разбора",
    });

    // Длина вместо смещения: курсор и так проходит блоки один за другим, и
    // накопить сумму ему ничего не стоит. Побег нужен там, где длина не влезла.
    let over = deltas.iter().filter(|&&d| d >= 0xffff).count() as u64;
    out.push(Addr {
        name: "длина u16 с побегом вместо смещения".into(),
        bytes: n * 2 + over * 4,
        scan: 1.0,
        note: "прямого обращения нет: длина осмысленна только накопленной от начала списка",
    });

    // Якорь плюс побитовые разности внутри группы.
    for g in [32usize, 64, 128] {
        let mut bytes = 0u64;
        for chunk in deltas.chunks(g) {
            let mx = chunk.iter().copied().max().unwrap_or(0);
            let w = 64 - mx.leading_zeros();
            bytes += 4 + 1 + ((chunk.len() as u64 * u64::from(w)).div_ceil(8));
        }
        out.push(Addr {
            name: format!("якорь каждые {g} и побитовые разности"),
            bytes,
            scan: (g / 2) as f64,
            note:
                "обращение это якорь плюс проход по группе: чем крупнее группа, тем длиннее проход",
        });
    }

    // Direct Monotonic: на группу — начало, средний шаг и ширина отклонения.
    for g in [64usize, 128] {
        let mut bytes = 0u64;
        for chunk in off.chunks(g) {
            if chunk.is_empty() {
                continue;
            }
            let first = chunk[0];
            let last = *chunk.last().unwrap_or(&first);
            let step = (last - first) as f64 / (chunk.len().max(1) as f64);
            let mut mx = 0u64;
            for (i, &v) in chunk.iter().enumerate() {
                let pred = first + (step * i as f64) as u64;
                mx = mx.max(v.abs_diff(pred));
            }
            let w = 64 - (mx * 2 + 1).leading_zeros();
            bytes += 4 + 4 + 1 + ((chunk.len() as u64 * u64::from(w)).div_ceil(8));
        }
        out.push(Addr {
            name: format!("Direct Monotonic, группа {g}"),
            bytes,
            scan: 1.0,
            note: "прямое обращение остаётся: отклонение от прямой лежит по номеру внутри группы",
        });
    }

    // Элиас-Фано по всему ряду: старшие биты унарно, младшие как есть.
    let u = off.last().copied().unwrap_or(1).max(1);
    let low = (u / n.max(1)).max(1).ilog2() as u64;
    let ef_bits = n * low + 2 * n;
    out.push(Addr {
        name: "Элиас-Фано по всему ряду".into(),
        // Надстройка выбора добавляет к старшим битам около четверти.
        bytes: (ef_bits + (2 * n) / 4).div_ceil(8),
        scan: 8.0,
        note: "обращение требует выбора k-й единицы в унарном ряду — это надстройка и проход по слову",
    });

    // Partitioned EF: ряд делится на части, у каждой своя вселенная.
    for g in [128usize, 1024] {
        let mut bytes = 0u64;
        for chunk in off.chunks(g) {
            let (Some(&a), Some(&b)) = (chunk.first(), chunk.last()) else { continue };
            let k = chunk.len() as u64;
            let uu = (b - a).max(1);
            let low = (uu / k).max(1).ilog2() as u64;
            bytes += 4 + (k * low + 2 * k + (2 * k) / 4).div_ceil(8);
        }
        out.push(Addr {
            name: format!("Partitioned Elias-Fano, часть {g}"),
            bytes,
            scan: 8.0,
            note: "то же плюс поиск нужной части; выигрывает там, где плотность рядом неровная",
        });
    }
    out
}

/// Приписывает байты имени, складывая с уже накопленным.
fn bump(by: &mut Vec<(String, u64)>, name: String, bytes: u64) {
    match by.iter_mut().find(|(n, _)| *n == name) {
        Some((_, s)) => *s += bytes,
        None => by.push((name, bytes)),
    }
}

/// Как называется строка с обрамлением контейнера.
///
/// Имя нарочно не совпадает ни с одним куском и ни с одним файлом: по нему
/// ничего не ищут, его читают глазами. Собирается из имени контейнера, а не
/// пишется словом: два места, называющие один файл, рано или поздно расходятся.
fn frame_name() -> String {
    format!("{}: заголовок, оглавление, выравнивание", crate::container::FILE)
}

/// Байты по имени файла, сложенные по всем сегментам.
///
/// ## Почему у упакованного сегмента имена берутся из оглавления
///
/// Весь осмотр стоит на этой разбивке: по ней считаются «байт на постинг»,
/// «словарь на слово», «позиции на вхождение» и все оценки вариантов раскладки —
/// то есть числа, по которым принимаются решения о формате. У упакованного
/// сегмента в каталоге лежат четыре файла, и обход каталога отдал бы один ком
/// `seg.dat` в гигабайты и ноль напротив каждого потока. Ноль в знаменателе не
/// отказывает — он делает оценку правдоподобной и неверной, а по ней потом
/// переписывают раскладку.
///
/// ## Куда девается обрамление
///
/// Заголовок, оглавление, концевик и выравнивание кусков — байты настоящие, они
/// занимают место на диске. Сложи только куски, и сумма разойдётся с тем, что
/// показывает `du`, а «всего на документ» станет заниженным. Поэтому разница
/// названа отдельной строкой: сумма по всем именам обязана равняться длине
/// файлов сегмента, и ни один байт не пропадает и не считается дважды.
///
/// Оглавление не прочлось — сегмент остаётся одной строкой `seg.dat` во всю
/// длину. Разбивка при этом врёт, но врёт **видимо**: пустые строки напротив
/// потоков читались бы как «этих файлов нет», а гигабайт под собственным именем
/// не спутать ни с чем. Сказать об этом внятно — дело `check`, не осмотра.
fn files_of(dir: &str) -> Vec<(String, u64)> {
    let mut by: Vec<(String, u64)> = Vec::new();
    for path in crate::segments::Segments::ready(dir) {
        let Ok(entries) = std::fs::read_dir(&path) else { continue };
        for e in entries.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().to_string();
            let size = e.metadata().map(|m| m.len()).unwrap_or(0);
            if name != crate::container::FILE {
                bump(&mut by, name, size);
                continue;
            }
            let Ok(table) = crate::container::Table::read(&format!("{path}/{name}")) else {
                bump(&mut by, name, size);
                continue;
            };
            let mut inside = 0u64;
            for m in table.members() {
                bump(&mut by, m.name.clone(), m.len);
                inside += m.len;
            }
            bump(&mut by, frame_name(), size.saturating_sub(inside));
        }
    }
    by.sort_by_key(|(_, b)| std::cmp::Reverse(*b));
    by
}

// ---------------------------------------------------------------------------
// Порог, за которым позиции перестают храниться.
//
// Позиции обслуживают ровно один класс запросов — фразы и близость. Скоринг их
// не читает вовсе, подсветка и так разбирает текст заново. Значит их можно не
// хранить там, где порядок слов дешевле **получить** из сохранённого текста.
//
// Правило: позиции хранятся тогда и только тогда, когда `df(слово) > V`. Тогда
// любая фраза попадает ровно в один из двух случаев — либо все её слова выше
// порога и работает сегодняшний позиционный путь, либо хотя бы одно ниже, и
// тогда кандидатов не больше его `df`, то есть не больше `V`. Граница здесь
// устройственная, а не эвристическая: третьего случая нет.
//
// Весь вопрос в обменном курсе — сколько байт снимает порог и во что обходится
// то, что он оставил проверке. Обе величины меряются здесь, а не выводятся.
// ---------------------------------------------------------------------------

/// Пороги `df`, которые сравниваются.
///
/// Начинается с размера блока: ниже него список и так живёт одним куском, и
/// отдельный класс из него делать нечего. Дальше по порядкам величины.
const CUTS: [u32; 8] = [128, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000];

/// Что слово платит за свои позиции.
#[derive(Clone, Copy, Default)]
struct Toll {
    df: u32,
    values: u64,
    /// Байт в `pos.bin`.
    pos: u64,
    /// Байт в `terms.bin`, которые есть только из-за позиций: длина куска либо
    /// сама позиция единственного вхождения.
    meta: u64,
    /// Записей `pos.idx`, которые перестанут значить что-либо.
    idx: u64,
}

/// Что снимает порог и что после него остаётся проверке.
pub struct Cut {
    pub v: u32,
    /// Слов, сохранивших позиции.
    pub kept_terms: u64,
    pub kept_values: u64,
    pub kept_bytes: u64,
    pub dropped_terms: u64,
    pub dropped_values: u64,
    /// Байт `pos.bin`, которые уходят. Величина **точная**: у каждого слова свой
    /// кусок потока, и убрать одно не значит переупаковать другие.
    pub dropped_pos: u64,
    /// Байт `terms.bin`, уходящих вместе с ними.
    pub dropped_meta: u64,
    /// Записей `pos.idx`, теряющих смысл. Не освобождаются сами: указатель
    /// проиндексован **номером блока**, и разредить его — отдельная правка.
    pub stale_idx: u64,
    /// Худшее число кандидатов у фразы при этом пороге. Оно же сам порог.
    pub worst: u64,
    /// Во что худшая фраза обойдётся вразброс и когда кандидаты идут подряд.
    pub worst_ms: f64,
    pub worst_run_ms: f64,
}

/// Что показал разбор порогов.
pub struct Verify {
    pub dir: String,
    pub docs: u64,
    /// Поле, по которому мерилось: самое крупное текстовое с позициями.
    pub field: String,
    pub terms: u64,
    pub values: u64,
    /// Байт под позиции сейчас — по записям слов, а не по размеру файла.
    pub pos_bytes: u64,
    pub meta_bytes: u64,
    pub idx_entries: u64,
    pub chunks: u64,
    /// Мкс на одного кандидата вразброс: своя распаковка на каждого.
    pub scatter_us: f64,
    /// Мкс на одного кандидата подряд: кусок делится с соседями.
    pub run_us: f64,
    pub cuts: Vec<Cut>,
    pub dfs: Hist,
}

/// Разбирает пороги. Работает по исправному индексу.
pub fn verify(dir: &str) -> std::io::Result<Verify> {
    let segs = crate::segments::Segments::load(dir)?;

    let field = text_field(&segs).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "нет текстового поля, у которого есть и позиции, и содержимое: проверять нечем",
        )
    })?;

    let mut v = Verify {
        dir: dir.into(),
        docs: 0,
        field: segs.list[0].fields[field].name.clone(),
        terms: 0,
        values: 0,
        pos_bytes: 0,
        meta_bytes: 0,
        idx_entries: 0,
        chunks: 0,
        scatter_us: 0.0,
        run_us: 0.0,
        cuts: Vec::new(),
        dfs: Hist { name: String::new(), buckets: Vec::new() },
    };
    let mut tolls: Vec<Toll> = Vec::new();
    let mut dfs = Buckets::new(&[1, 2, 129, 1_001, 10_001, 100_001]);
    // Два самых частых слова поля — ими и меряется цена проверки: слово, которого
    // в тексте нет, разбор проходит вхолостую и цена выходит заниженной.
    let mut top: Vec<(u32, String)> = Vec::new();

    for ix in segs.list.iter() {
        v.docs += ix.docs as u64;
        v.chunks += ix.fields[field].store.as_ref().map_or(0, |s| s.blocks() as u64);
        for list in 0..ix.dict.len() {
            let (fld, term) = ix.dict.term_at(list);
            if fld as usize != field {
                continue;
            }
            let t = ix.term(list, field);
            if t.df == 0 {
                continue;
            }
            v.terms += 1;
            dfs.add(u64::from(t.df));

            // Позиций у слова столько, сколько всего вхождений: обход по
            // постингам, без чтения самих позиций.
            let mut values = 0u64;
            let mut c = ix.cursor(list as u32, field, 1.0);
            while c.cur() != u32::MAX {
                values += u64::from(c.tf());
                c.next();
            }

            let inline = t.one.and_then(|r| r.inline_pos);
            let meta = match inline {
                Some(p) => vlen(u64::from(p)) as u64,
                None if t.pos_len > 0 => vlen(u64::from(t.pos_len)) as u64,
                None => 0,
            };
            let w = Toll {
                df: t.df,
                values,
                pos: u64::from(t.pos_len),
                meta,
                idx: if t.meta { t.blocks as u64 } else { 0 },
            };
            v.values += values;
            v.pos_bytes += w.pos;
            v.meta_bytes += w.meta;
            v.idx_entries += w.idx;
            tolls.push(w);

            if top.len() < 2 || top.iter().any(|(d, _)| *d < t.df) {
                top.push((t.df, term.to_string()));
                top.sort_by_key(|(d, _)| std::cmp::Reverse(*d));
                top.truncate(2);
            }
        }
    }

    // Цена проверки — на самом крупном сегменте: цена куска от числа сегментов
    // не зависит, а разброс кандидатов на мелком не успевает проявиться.
    let big =
        segs.list.iter().max_by_key(|ix| ix.docs).expect("сегмент есть: иначе поле бы не нашлось");
    let probe_words: Vec<String> = top.iter().map(|(_, t)| t.clone()).collect();
    if let Some(store) = big.fields[field].store.as_ref() {
        let docs = big.docs as u32;
        const PROBES: u32 = 4096;
        let stride = (docs / PROBES).max(2);
        v.scatter_us = probe(store, docs, &probe_words, stride, PROBES);
        v.run_us = probe(store, docs, &probe_words, 1, PROBES);
    }

    v.dfs = dfs.into_hist("документов у слова");
    v.cuts = CUTS
        .iter()
        .map(|&cut| {
            let mut c = Cut {
                v: cut,
                kept_terms: 0,
                kept_values: 0,
                kept_bytes: 0,
                dropped_terms: 0,
                dropped_values: 0,
                dropped_pos: 0,
                dropped_meta: 0,
                stale_idx: 0,
                worst: u64::from(cut),
                worst_ms: u64::from(cut) as f64 * v.scatter_us / 1000.0,
                worst_run_ms: u64::from(cut) as f64 * v.run_us / 1000.0,
            };
            for w in tolls.iter() {
                if w.df > cut {
                    c.kept_terms += 1;
                    c.kept_values += w.values;
                    c.kept_bytes += w.pos;
                } else {
                    c.dropped_terms += 1;
                    c.dropped_values += w.values;
                    c.dropped_pos += w.pos;
                    c.dropped_meta += w.meta;
                    c.stale_idx += w.idx;
                }
            }
            c
        })
        .collect();
    Ok(v)
}

/// Цена одной проверки: достать текст поля и найти в нём слова.
///
/// `stride` — через сколько документов берётся следующий кандидат. Единица —
/// подряд, и тогда распаковка куска делится с соседями; крупный шаг — вразброс,
/// и тогда на каждого кандидата приходится своя. Разница между ними и есть то,
/// что могло бы дать переупорядочивание документов при слиянии.
///
/// Кусок запоминается **один**, последний. Кандидаты идут по возрастанию номера
/// документа, значит помнить больше незачем: назад обход не пойдёт.
fn probe(store: &crate::store::Store, docs: u32, words: &[String], stride: u32, n: u32) -> f64 {
    if words.is_empty() || docs == 0 {
        return 0.0;
    }
    let mut out = vec![Vec::new(); words.len()];
    let mut buf = String::new();
    let mut taken = 0u32;
    let mut secs = 0.0;
    for round in 0..2 {
        let mut held: Option<(usize, Vec<u8>)> = None;
        let mut doc = 0u32;
        taken = 0;
        let t0 = std::time::Instant::now();
        while taken < n && doc < docs {
            if let Some((chunk, first)) = store.locate(doc) {
                if held.as_ref().map(|(c, _)| *c) != Some(chunk) {
                    held = store.chunk_bytes(chunk).map(|raw| (chunk, raw));
                }
                if let Some(text) = held
                    .as_ref()
                    .and_then(|(_, raw)| crate::store::slice_at(raw, (doc - first) as usize))
                    .and_then(|b| std::str::from_utf8(b).ok())
                {
                    crate::highlight::locate(text, words, &mut out, &mut buf);
                    let lists: Vec<&[u32]> = out.iter().map(|l| l.as_slice()).collect();
                    std::hint::black_box(crate::phrase::count(&lists, 0));
                }
            }
            taken += 1;
            doc = doc.saturating_add(stride);
        }
        if round == 1 {
            secs = t0.elapsed().as_secs_f64();
        }
    }
    secs * 1e6 / f64::from(taken.max(1))
}

// ---------------------------------------------------------------------------
// Пара слов отдельным списком.
//
// Порог `V` ограничивает проверку сверху числом кандидатов, но кандидатов на
// самом деле не `min(df)`, а `|A ∩ B|` — и велико пересечение только тогда,
// когда **оба** слова частые. Отсюда следующий ход: завести список у самой пары
// там, где её слова частые оба. Тогда дорогая фраза отвечается точно и без
// проверки вовсе, а проверке остаётся только то, что и так дёшево.
//
// Всё держится на одном числе, которого никто не знает: сколько таких пар
// **действительно встречается подряд**. Полный индекс биграмм отвергается по
// порядку величины — различных пар в тексте почти столько же, сколько слов, — но
// пары из двух частых слов это другое множество, и его размер надо измерить, а
// не оценить.
// ---------------------------------------------------------------------------

/// Пороги частоты слова, при которых пара считается достойной своего списка.
const PAIRS: [u32; 5] = [1_000, 5_000, 20_000, 50_000, 100_000];

/// Бюджеты проверки в кандидатах, под которые подбирается набор пар.
///
/// Правило отбора здесь другое и оно **верное**: пара нужна не тогда, когда оба
/// её слова частые, а тогда, когда проверка по ней не уложилась бы в бюджет.
/// Ожидаемое пересечение двух списков — `df(A)·df(B)/N`, значит пара нужна при
/// `df(A)·df(B) > бюджет·N`. Отбор по каждому слову порознь пропускает худший
/// случай: редкое слово рядом с «the» даёт десятки тысяч кандидатов, а по
/// порогу «оба выше» такая пара не заводится вовсе.
const BUDGETS: [u32; 5] = [250, 500, 1_000, 2_000, 5_000];

/// Что дал бы порог, если завести списки пар начиная с него.
pub struct Level {
    pub b: u32,
    /// Слов выше порога.
    pub head: u64,
    /// Различных пар, встретившихся подряд, у которых оба слова выше порога.
    pub pairs: u64,
    /// Постингов у этих пар: пара и документ, посчитанные однажды.
    pub postings: u64,
    /// Байт под списки пар. Нижняя граница по биномиальной оценке.
    pub bytes: u64,
    /// Байт под словарь пар.
    pub dict: u64,
}

/// Что показал разбор пар.
pub struct PairPlan {
    pub dir: String,
    pub docs: u64,
    pub field: String,
    pub tokens: u64,
    /// Байт под позиции сейчас — то, что списки пар могли бы заменить.
    pub pos_bytes: u64,
    /// Отбор по частоте каждого слова: сколько пар и чего они стоят.
    pub levels: Vec<Level>,
    /// Отбор по бюджету проверки — тот, по которому и надо решать.
    pub budgets: Vec<Level>,
}

/// Разбирает пары. Читает **содержимое**, поэтому идёт минуты, а не секунды.
pub fn pairs(dir: &str) -> std::io::Result<PairPlan> {
    use std::collections::HashMap;

    let segs = crate::segments::Segments::load(dir)?;
    let field = text_field(&segs).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "нет текстового поля, у которого есть и позиции, и содержимое",
        )
    })?;
    let base = PAIRS[0];

    let mut p = PairPlan {
        dir: dir.into(),
        docs: 0,
        field: segs.list[0].fields[field].name.clone(),
        tokens: 0,
        pos_bytes: 0,
        levels: PAIRS
            .iter()
            .map(|&b| Level { b, head: 0, pairs: 0, postings: 0, bytes: 0, dict: 0 })
            .collect(),
        budgets: BUDGETS
            .iter()
            .map(|&b| Level { b, head: 0, pairs: 0, postings: 0, bytes: 0, dict: 0 })
            .collect(),
    };

    for ix in segs.list.iter() {
        p.docs += ix.docs as u64;
        let n = ix.docs as u64;

        // Где кончается голова. Порог по частоте каждого слова — не единственное
        // условие: по бюджету годится и слово в четыре сотни документов, если
        // рядом с ним стоит самое частое слово корпуса. Поэтому нижняя граница
        // выводится из наибольшей частоты, а не назначается.
        let mut top_df = 1u64;
        for list in 0..ix.dict.len() {
            if ix.dict.term_at(list).0 as usize == field {
                top_df = top_df.max(u64::from(ix.t_df(list)));
            }
        }
        let floor = (u64::from(BUDGETS[0]) * n / top_df).min(u64::from(base)) as u32;

        // Голова сегмента: слово выше нижней границы получает номер, остальные
        // не получают ничего. Словарь на десятки тысяч слов ищется наносекунды,
        // а общий — сотни: искать пришлось бы сто миллионов раз.
        let mut head: HashMap<String, u32> = HashMap::new();
        let mut dfs: Vec<u32> = Vec::new();
        for list in 0..ix.dict.len() {
            let (fld, term) = ix.dict.term_at(list);
            if fld as usize != field {
                continue;
            }
            let t = ix.term(list, field);
            p.pos_bytes += u64::from(t.pos_len);
            if t.df > floor {
                head.insert(term, dfs.len() as u32);
                dfs.push(t.df);
            }
        }

        // Пара → сколько документов её содержат, наименьшая частота её слов и
        // произведение частот. Оба отбора обслуживаются одним проходом: пара,
        // годная при высоком пороге, годна и при низком.
        let mut seen: HashMap<u64, (u32, u32, u64)> = HashMap::new();
        let mut here: Vec<u64> = Vec::new();
        let mut toks: Vec<u32> = Vec::new();
        let mut buf = String::new();

        let Some(store) = ix.fields[field].store.as_ref() else { continue };
        let mut walk = store.walk();
        while let Some((_, raw)) = walk.next() {
            let Ok(text) = std::str::from_utf8(raw) else { continue };
            // Номера слов документа по порядку: не голова — u32::MAX.
            toks.clear();
            buf.clear();
            for c in text.chars() {
                if crate::analyze::is_word_char(c) {
                    buf.extend(c.to_lowercase());
                } else if !buf.is_empty() {
                    toks.push(head.get(buf.as_str()).copied().unwrap_or(u32::MAX));
                    buf.clear();
                }
            }
            if !buf.is_empty() {
                toks.push(head.get(buf.as_str()).copied().unwrap_or(u32::MAX));
            }
            p.tokens += toks.len() as u64;

            // Пара засчитывается документу однажды: список пары — это список
            // документов, а не вхождений.
            here.clear();
            for w in toks.windows(2) {
                if w[0] != u32::MAX && w[1] != u32::MAX {
                    here.push(u64::from(w[0]) << 32 | u64::from(w[1]));
                }
            }
            here.sort_unstable();
            here.dedup();
            for &key in here.iter() {
                let (a, b) = (dfs[(key >> 32) as usize], dfs[(key & 0xFFFF_FFFF) as usize]);
                let (lo, prod) = (a.min(b), u64::from(a) * u64::from(b));
                if lo <= base && prod <= u64::from(BUDGETS[0]) * n {
                    continue;
                }
                let e = seen.entry(key).or_insert((0, lo, prod));
                e.0 += 1;
            }
        }
        if walk.broken() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "содержимое поля читается не до конца: считать по нему нечего",
            ));
        }

        for lvl in p.levels.iter_mut() {
            lvl.head += dfs.iter().filter(|&&d| d > lvl.b).count() as u64;
            for &(docs, lo, _) in seen.values() {
                if lo <= lvl.b {
                    continue;
                }
                lvl.pairs += 1;
                lvl.postings += u64::from(docs);
                lvl.bytes += binomial(u64::from(docs), n);
                // Словарь пар — два номера слова, а не две строки. Первый идёт
                // разностью по возрастанию, второй целиком.
                lvl.dict += 6;
            }
        }
        for lvl in p.budgets.iter_mut() {
            let need = u64::from(lvl.b) * n;
            lvl.head += dfs.iter().filter(|&&d| u64::from(d) * u64::from(d) > need).count() as u64;
            for &(docs, _, prod) in seen.values() {
                if prod <= need {
                    continue;
                }
                lvl.pairs += 1;
                lvl.postings += u64::from(docs);
                lvl.bytes += binomial(u64::from(docs), n);
                lvl.dict += 6;
            }
        }
    }
    Ok(p)
}

/// Сколько байт занимает список из `df` документов среди `n` по нижней границе.
///
/// Биномиальная оценка `log2 C(n, df)`: меньше не бывает ни у какой упаковки,
/// потому что столько в списке сведений. Настоящая упаковка крупнее примерно на
/// десятую часть, и это оговорено там, где число печатается.
///
/// Через двоичную энтропию, а не через `df·log2(n/df)`. Второе верно только при
/// `df ≪ n` и на плотном списке становится не нижней границей, а завышением:
/// половина документов дала бы им 2.44 бита на документ вместо ровно одного.
/// Списки пар как раз плотные — «of the» стоит в трети документов, — так что
/// приближение для редких здесь испортило бы именно то, ради чего считается.
fn binomial(df: u64, n: u64) -> u64 {
    if df == 0 || df >= n {
        return 0;
    }
    let (k, n) = (df as f64, n as f64);
    let p = k / n;
    let h = -p * p.log2() - (1.0 - p) * (1.0 - p).log2();
    let bits = n * h - 0.5 * (2.0 * std::f64::consts::PI * k * (1.0 - p)).log2();
    bits.max(1.0).ceil() as u64 / 8
}

/// Самое крупное текстовое поле, у которого есть и позиции, и содержимое.
fn text_field(segs: &crate::segments::Segments) -> Option<usize> {
    let mut field = None;
    let mut widest = 0u64;
    for ix in segs.list.iter() {
        for (f, fl) in ix.fields.iter().enumerate() {
            if fl.kind != crate::index::Kind::Text || !fl.positioned {
                continue;
            }
            let n = fl.store.as_ref().map_or(0, |s| s.blocks() as u64);
            if n > widest {
                widest = n;
                field = Some(f);
            }
        }
    }
    field
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-inspect-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(
            &d,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            400,
        );
        for i in 0..800u32 {
            // Слова с разной частотой: «общее» у всех, «редкое{i}» у одного.
            let body = format!("общее слово{} редкое{i} тело{}", i % 9, i % 61);
            ix.add(&format!("d{i:04}"), &[&format!("загол{}", i % 23), &body]);
        }
        ix.flush();
        d
    }

    /// Счётчики осмотра обязаны сойтись с тем, что действительно в сегменте.
    ///
    /// Метрика, посчитанная неверно, хуже отсутствующей: по ней принимают
    /// решения о формате. Поэтому сверяется не «выводится без паники», а числа.
    #[test]
    fn the_counts_add_up() {
        let d = corpus("counts");
        let s = stats(&d).expect("осмотр");
        let segs = crate::segments::Segments::load(&d).expect("индекс");

        assert_eq!(s.docs, 800, "документов");
        assert_eq!(s.segments, 2);
        let terms: u64 = segs.list.iter().map(|ix| ix.dict.len() as u64).sum();
        assert_eq!(s.terms, terms, "слов");

        // Постинги — сумма df по всем спискам всех сегментов.
        let postings: u64 = segs
            .list
            .iter()
            .map(|ix| (0..ix.dict.len()).map(|l| ix.t_df(l) as u64).sum::<u64>())
            .sum();
        assert_eq!(s.postings, postings, "постингов");

        // «Редкое{i}» встречается ровно однажды, значит слов-одиночек не меньше
        // числа документов.
        assert!(s.singleton_share() >= 50.0, "доля одиночек {}", s.singleton_share());
        std::fs::remove_dir_all(&d).ok();
    }

    /// Гистограммы обязаны покрывать ровно то, что посчитано.
    ///
    /// Корзина, в которую ничего не попало, и корзина, потерявшая половину, по
    /// виду неразличимы — сходится только сумма.
    #[test]
    fn every_histogram_covers_everything_it_counted() {
        let d = corpus("hist");
        let s = stats(&d).expect("осмотр");
        let by = |n: &str| s.hists.iter().find(|h| h.name == n).expect(n);

        assert_eq!(by("документов у слова").total(), s.terms, "по списку на слово");
        assert_eq!(by("частота слова в документе").total(), s.postings, "по частоте на постинг");
        assert_eq!(by("разность номеров документов").total(), s.postings, "по разности на постинг");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Порог обязан делить слова надвое, ничего не теряя и не задваивая.
    ///
    /// Таблица порогов — единственное, по чему принимается решение снимать
    /// позиции. Сойдись она не с тем, что в сегменте, и решение принималось бы
    /// по числу, которое никто не проверял.
    #[test]
    fn every_cut_splits_the_words_and_loses_none() {
        let d = corpus("verify");
        let v = verify(&d).expect("разбор порогов");
        assert!(v.terms > 0 && v.pos_bytes > 0, "поле нашлось, но пустое");
        assert!(!v.cuts.is_empty());

        let mut before = 0u64;
        for c in v.cuts.iter() {
            assert_eq!(c.kept_terms + c.dropped_terms, v.terms, "слов при V={}", c.v);
            assert_eq!(c.kept_bytes + c.dropped_pos, v.pos_bytes, "байт позиций при V={}", c.v);
            assert_eq!(c.kept_values + c.dropped_values, v.values, "значений при V={}", c.v);
            // Порог выше снимает не меньше: слова, снятые низким, снимет и он.
            assert!(c.dropped_pos >= before, "порог {} снял меньше предыдущего", c.v);
            before = c.dropped_pos;
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Пары обязаны быть теми, что в тексте действительно стоят рядом.
    ///
    /// Числа пар решают, заводить ли им списки, а ошибиться здесь легко в обе
    /// стороны: посчитать пару, которой нет, или потерять ту, что есть. Поэтому
    /// корпус здесь такой, где ответ известен наизусть.
    #[test]
    fn pairs_are_the_ones_that_actually_stand_next_to_each_other() {
        let d = std::env::temp_dir()
            .join(format!("svindb-inspect-pairs-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 4000);
        // Четыре слова у **каждого** документа и всегда в одном порядке: пар
        // подряд ровно три. Плюс слово, которое встречается у десятка — оно в
        // голову не попадает, и пар не даёт.
        for i in 0..2000u32 {
            let tail = if i < 10 { " редкое" } else { "" };
            ix.add(&format!("d{i:04}"), &[&format!("красный квадрат синий круг{tail}")]);
        }
        ix.flush();

        let p = pairs(&d).expect("разбор пар");
        assert_eq!(p.docs, 2000);
        assert_eq!(p.tokens, 4 * 2000 + 10, "слов в тексте");
        for l in p.budgets.iter() {
            // Порог годности: 2000·2000 = 4 млн против бюджет·2000.
            let want = if u64::from(l.b) * 2000 < 4_000_000 { 3 } else { 0 };
            assert_eq!(l.pairs, want, "пар при бюджете {}", l.b);
            assert_eq!(l.postings, want * 2000, "постингов при бюджете {}", l.b);
        }
        // «Редкое» стоит рядом с «круг» у десяти документов, но само редкое —
        // пары с ним быть не должно ни при одном отборе.
        let most = p.budgets.iter().map(|l| l.pairs).max().unwrap_or(0);
        assert_eq!(most, 3, "нашлась пара, которой в голове быть не может");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Нижняя граница списка обязана быть границей, а не выдумкой.
    ///
    /// Ловушка здесь настоящая и уже сработала: приближение `df·log2(n/df)`
    /// верно только для редких списков, а на списке в половину корпуса даёт
    /// 2.44 бита на документ вместо одного — то есть перестаёт быть нижней
    /// границей ровно там, где считаются пары.
    #[test]
    fn the_binomial_floor_stays_below_what_packing_costs() {
        // Половина документов — это бит на документ **корпуса**, то есть два
        // бита на постинг: выбрать миллион из двух стоит ровно два миллиона бит.
        let half = binomial(1_000_000, 2_000_000);
        assert!(half <= 250_000, "плотный список оценён в {half} Б вместо 250 000");
        assert!(half >= 249_000, "оценка ушла ниже энтропии: {half} Б");

        // Треть — двоичная энтропия 0.9183 бита на документ.
        let third = binomial(666_666, 2_000_000) * 8;
        let want = (2_000_000.0 * 0.918_295_8f64) as u64;
        assert!(third <= want && third + want / 100 >= want, "треть: {third} против {want}");

        // Редкий список: около `log2(n/df)` бит на документ, и приближение для
        // редких здесь сходится с точной формулой.
        let df = 1_000u64;
        let got = binomial(df, 2_000_000) * 8;
        let want = (df as f64 * (2_000_000f64 / df as f64).log2()) as u64;
        assert!(got >= want, "оценка {got} ниже энтропии {want}");
        assert!(got <= want * 5 / 4, "оценка {got} много выше энтропии {want}");
        assert_eq!(binomial(0, 100), 0);
    }

    /// Осмотр обязан видеть состав упакованного сегмента, а не один ком.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Разбивка по именам — знаменатель всего осмотра: «списки на постинг»,
    /// «словарь на слово», «метаданные слова», «позиции на вхождение» и каждая
    /// оценка варианта раскладки берут байты именно оттуда. У упакованного
    /// сегмента в каталоге лежат четыре файла, и обход каталога отдаёт один
    /// `seg.dat` в гигабайты и ноль напротив каждого потока. Ноль не отказывает:
    /// он делает оценки правдоподобными и неверными, а по ним переписывают
    /// формат.
    ///
    /// Отдельно сверяется сумма: разбивка обязана сойтись с тем, что сегменты
    /// занимают на диске. Иначе куски считались бы дважды — сами по себе и
    /// внутри `seg.dat`, — либо обрамление контейнера пропадало бы, и «всего на
    /// документ» врало бы в обе стороны.
    #[test]
    fn a_packed_segment_shows_its_pieces_and_loses_not_a_byte() {
        let d = corpus("packed");
        let segs = crate::segments::Segments::ready(&d);
        assert_eq!(segs.len(), 2, "сегментов не два");
        let mut on_disk = 0u64;
        for s in segs.iter() {
            assert!(
                std::path::Path::new(&format!("{s}/{}", crate::container::FILE)).exists(),
                "сегмент {s} не упакован — проверять нечего"
            );
            for e in std::fs::read_dir(s).expect("каталог сегмента").filter_map(|e| e.ok())
            {
                on_disk += e.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }

        let files = files_of(&d);
        let byname = |n: &str| files.iter().find(|(f, _)| f == n).map_or(0, |(_, b)| *b);
        for n in ["postings.bin", "terms.bin", "dict.blob", "norms.bin", "ids.bin", "pos.bin"] {
            assert!(byname(n) > 0, "поток {n} не назван: {:?}", files);
        }
        assert_eq!(byname(crate::container::FILE), 0, "сегмент показан одним комом");
        assert!(byname(&frame_name()) > 0, "обрамление контейнера не названо");
        assert_eq!(
            files.iter().map(|(_, b)| *b).sum::<u64>(),
            on_disk,
            "разбивка разошлась с тем, что лежит на диске: {:?}",
            files
        );

        // И то, ради чего разбивка нужна: числа осмотра перестают быть нулями.
        let s = stats(&d).expect("осмотр");
        let num = |n: &str| s.numbers.iter().find(|x| x.name == n).map_or(0.0, |x| x.value);
        for n in ["списки на постинг", "словарь на слово", "метаданные слова"]
        {
            assert!(num(n) > 0.0, "{n} посчитано нулём");
        }
        assert_eq!(s.bytes(), on_disk, "всего байт посчитано мимо диска");
        std::fs::remove_dir_all(&d).ok();
    }

    /// У каждой оценки обязана быть оговорка, чего она не учитывает.
    ///
    /// Это не стиль, а договор: число без оговорки читается как «вариант лучше»,
    /// а место, цена разбора, задержка и память сборки — четыре разные величины.
    #[test]
    fn no_estimate_goes_out_without_its_caveat() {
        let d = corpus("variants");
        let s = stats(&d).expect("осмотр");
        assert!(!s.variants.is_empty());
        for v in s.variants.iter() {
            assert!(v.note.contains("не учтено"), "{}: оговорки нет", v.name);
            assert!(v.now > 0 || v.then > 0, "{}: обе величины нулевые", v.name);
        }
        std::fs::remove_dir_all(&d).ok();
    }
}
