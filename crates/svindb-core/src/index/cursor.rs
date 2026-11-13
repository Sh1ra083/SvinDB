//! Курсор по одному списку с прыжками через блоки.

use super::{Index, Term};
use crate::bits::{self, BLOCK};
use crate::stats;

pub struct Cursor<'a> {
    ix: &'a Index,
    /// Таблицы общей статистики; `None` — считать местными.
    sc: Option<crate::index::FieldScore>,
    /// Множитель, приводящий сохранённые потолки к общей средней.
    lift: f64,
    pub fld: usize,
    pub w: f64,
    /// Потолок вклада списка целиком. Считается один раз при создании: он
    /// зависит только от веса и средней длины поля, а спрашивают его на **каждой**
    /// итерации обхода по каждому курсору — это самый горячий вызов в поиске.
    bound: f64,
    t: Term,
    df: u32,
    nblocks: usize,
    block: usize,
    /// Смещение текущего блока в файле списков.
    ///
    /// Накапливается по длинам пройденных блоков, а не читается: блоки списка
    /// лежат подряд, и курсор всё равно проходит их один за другим — и в шаге, и
    /// в прыжке. Так метаданные блока обходятся в десять байт вместо двенадцати.
    off: usize,
    /// Где в блоке начинаются упакованные частоты (уже за байтом ширины).
    fdata_at: usize,
    buf: [u32; BLOCK],
    /// Ширина упаковки частот текущего блока.
    fwidth: u32,
    have: usize,
    pos: usize,
    pub doc: u32,
    done: bool,
    /// Где курсор остановился в потоке позиций прошлый раз.
    ///
    /// Нужно ради одного: **обход подряд не должен быть квадратичным**. Чтобы
    /// прочесть позиции документа, надо знать порядковый номер его первого
    /// значения, а это сумма частот всех предыдущих документов блока. Считать её
    /// заново на каждом документе — сто двадцать восемь сложений на сто двадцать
    /// восемь документов, то есть восемь тысяч на блок; ровно так и делало
    /// слияние, которое читает позиции у **каждого** постинга.
    seq: std::cell::Cell<Seq>,
}

/// Память о том, где кончился прошлый разбор позиций.
#[derive(Clone, Copy, Default)]
struct Seq {
    /// Номер блока постингов, к которому память относится. `usize::MAX` — пусто.
    block: usize,
    /// До какого документа блока досчитана сумма частот и чему она равна.
    at: usize,
    ord: usize,
    /// Смещение куска позиций, в котором лежит значение с этим номером, и номер
    /// первого значения того куска.
    chunk: usize,
    first: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(
        ix: &'a Index,
        list: u32,
        fld: usize,
        w: f64,
        sc: Option<crate::index::FieldScore>,
    ) -> Cursor<'a> {
        let t = ix.term(list as usize, fld);
        // Потолки сегмента посчитаны по его собственной средней длине. Считая
        // вклад по общей, их обязательно поднять — иначе настоящий вклад
        // окажется выше потолка, и отсечение выбросит документ молча.
        let lift = sc.as_ref().map_or(1.0, |s| s.lift);
        let mut c = Cursor {
            ix,
            fld,
            w,
            // Потолок списка берётся из самого списка, а не из средней длины
            // поля. При `tf ≡ 1` лучший документ списка это самый короткий, и он
            // один на всё поле; с частотами лучший зависит от того, какие
            // частоты в этом списке встретились, и записан при индексации.
            bound: w * lift * t.max,
            lift,
            sc,
            df: t.df,
            nblocks: t.blocks,
            block: 0,
            off: t.post,
            t,
            fdata_at: 0,
            buf: [0; BLOCK],
            fwidth: 0,
            have: 0,
            pos: 0,
            doc: 0,
            done: false,
            seq: std::cell::Cell::new(Seq { block: usize::MAX, ..Seq::default() }),
        };
        c.fill();
        c.step();
        c
    }

    /// Распаковка текущего блока.
    fn fill(&mut self) {
        stats::cursor_fill(1);
        let n = (self.df as usize).saturating_sub(self.block * BLOCK).min(BLOCK);
        if n == 0 || self.block >= self.nblocks {
            self.have = 0;
            return;
        }
        // Одиночное слово: номер документа лежит в записи слова, файла списков у
        // него нет вовсе.
        if let Some(r) = self.t.one {
            self.buf[0] = r.doc;
            self.have = 1;
            self.pos = 0;
            return;
        }
        let width = bits::width_of(self.ix.post.slice(self.off, 1));
        let src = self.ix.post.slice(self.off + 1, bits::window_len(n, width));
        // Первый блок отсчитывается от нуля, каждый следующий — от последнего
        // номера предыдущего: дельты в файле сквозные по списку, а не по блоку.
        let base = if self.block == 0 { 0 } else { self.ix.b_last(self.t.first + self.block - 1) };
        bits::unpack_deltas(src, n, width, base, &mut self.buf);
        self.have = n;
        self.pos = 0;
        // `packed_len` уже включает байт ширины.
        let fat = self.off + bits::packed_len(n, width);
        if self.t.tf_all_one {
            // Полосы частот нет: список из одних единиц её не пишет.
            self.fwidth = 0;
            self.fdata_at = fat;
        } else {
            self.fwidth = bits::width_of(self.ix.post.slice(fat, 1));
            self.fdata_at = fat + 1;
        }
    }

    /// Частота текущего документа.
    #[inline(always)]
    pub fn tf(&self) -> u32 {
        if let Some(r) = self.t.one {
            return r.tf;
        }
        if self.t.tf_all_one {
            return 1;
        }
        let i = self.pos.saturating_sub(1);
        let src = self.ix.post.slice(self.fdata_at, bits::window_len(self.have, self.fwidth));
        bits::value_at(src, self.fwidth, i) + 1
    }

    /// Где в `pos.bin` лежат позиции текущего блока.
    ///
    /// У длинного списка это указатель из `pos.idx`, у короткого — смещение из
    /// записи слова: блок у него один, и отдельного указателя ему не нужно.
    #[inline]
    fn pos_at(&self) -> usize {
        if self.t.meta {
            self.ix.p_at(self.t.first + self.block)
        } else {
            self.t.pos
        }
    }

    /// Позиции текущего документа. Отдаёт, сколько их положил.
    ///
    /// Читаются только когда спросили: обычный обход `pos.bin` не трогает вовсе,
    /// и страницы позиций не попадают в кеш там, где фраз нет.
    ///
    /// Смещение внутри блока — сумма частот всех документов до текущего. Она
    /// считается **здесь**, а не поддерживается курсором на ходу: поддерживать
    /// значило бы читать частоту на каждом шаге, то есть платить за фразы на
    /// каждом запросе. Сто двадцать семь дешёвых чтений на документ фразы
    /// дешевле, чем одно лишнее чтение на постинг обычного запроса.
    pub fn positions(&self, out: &mut Vec<u32>) -> usize {
        out.clear();
        let keep = self.ix.fields.get(self.fld).map(|f| f.positioned).unwrap_or(false)
            && self.t.positioned;
        if !keep || !self.ix.has_positions() || self.have == 0 {
            return 0;
        }
        // Единственное вхождение одиночного слова лежит в самой записи: ради
        // него `pos.bin` не заводится.
        if let Some(r) = self.t.one {
            if let Some(p) = r.inline_pos {
                out.push(p);
                crate::stats::positions_read(1);
                return 1;
            }
            if r.pos_len == 0 {
                return 0;
            }
        } else if !self.t.meta && self.t.pos_len == 0 {
            // Короткий список без своего куска позиций. Читать по смещению
            // нечего: там уже начинается чужой кусок.
            return 0;
        }
        let i = self.pos.saturating_sub(1);
        // Сколько значений пропустить до текущего документа и сколько взять.
        // У списка из одних единиц это `i` и `1` — считать нечего.
        // Память относится к одному блоку постингов и вместе с ним теряет смысл:
        // у следующего блока свой прогон позиций, и продолжать счёт в нём
        // нельзя. Сброс стоит здесь, до всякого разбора, а не в одной из веток
        // ниже — там он держался бы на негласном условии «курсор не ходит
        // назад, а прогоны идут по возрастанию». Условие верное, но нигде не
        // записанное, а такие однажды перестают выполняться молча.
        let mut memo = self.seq.get();
        if memo.block != self.block {
            memo = Seq { block: self.block, ..Seq::default() };
        }
        let (skip, tf) = if self.t.tf_all_one {
            (i, 1usize)
        } else if let Some(r) = self.t.one {
            (0usize, r.tf as usize)
        } else {
            let fsrc = self.ix.post.slice(self.fdata_at, bits::window_len(self.have, self.fwidth));
            // Продолжаем счёт с места, где остановились, если идём вперёд по
            // тому же блоку. Иначе считаем от начала.
            let (mut skip, from) =
                if memo.at <= i { (memo.ord, memo.at) } else { (0usize, 0usize) };
            for j in from..i {
                skip += (bits::value_at(fsrc, self.fwidth, j) + 1) as usize;
            }
            (skip, (bits::value_at(fsrc, self.fwidth, i) + 1) as usize)
        };
        memo.block = self.block;
        memo.at = i;
        memo.ord = skip;

        let at = self.pos_at();
        // Ноль означает «весь прогон одним куском» — так лежат сегменты,
        // собранные до появления этого числа.
        let step = self.ix.posblock;
        let (mut coff, mut cfirst) = if step == 0 {
            (at, 0usize)
        } else {
            let (mut o, mut f) = if memo.chunk >= at && memo.first <= skip {
                (memo.chunk, memo.first)
            } else {
                (at, 0usize)
            };
            while f + step <= skip {
                let w = bits::width_of(self.ix.pos_slice(o, 1));
                o += 1 + (step * w as usize).div_ceil(8);
                f += step;
            }
            (o, f)
        };
        memo.chunk = coff;
        memo.first = cfirst;
        self.seq.set(memo);

        let span = if step == 0 { skip + tf } else { step };
        let mut width = bits::width_of(self.ix.pos_slice(coff, 1));
        let mut src = self.ix.pos_slice(coff + 1, bits::window_len(span, width));
        let mut last = 0u32;
        for k in 0..tf {
            let ord = skip + k;
            // Вхождения документа могут пересечь границу куска: куски режутся по
            // числу значений, а не по документам.
            if step > 0 && ord >= cfirst + step {
                coff += 1 + (step * width as usize).div_ceil(8);
                cfirst += step;
                width = bits::width_of(self.ix.pos_slice(coff, 1));
                src = self.ix.pos_slice(coff + 1, bits::window_len(step, width));
            }
            let v = bits::value_at(src, width, ord - cfirst);
            last = if k == 0 { v } else { last.saturating_add(v) };
            out.push(last);
        }
        crate::stats::positions_read(tf as u64);
        tf
    }

    /// Переходит к следующему блоку, продвигая смещение на его длину.
    #[inline(always)]
    fn advance_block(&mut self) {
        self.off += self.ix.b_len(self.t.first + self.block);
        self.block += 1;
    }

    fn step(&mut self) -> bool {
        loop {
            if self.pos >= self.have {
                if (self.block + 1) * BLOCK >= self.df as usize {
                    self.done = true;
                    return false;
                }
                self.advance_block();
                self.fill();
                if self.have == 0 {
                    self.done = true;
                    return false;
                }
            }
            let d = self.buf[self.pos];
            self.pos += 1;
            // Условие `block == 0 && pos == 1` пропускает документ ноль: он
            // законный номер, но `doc` до первого шага тоже ноль.
            if d > self.doc || (self.block == 0 && self.pos == 1) {
                self.doc = d;
                return true;
            }
        }
    }

    // Не `Iterator`: обход умеет прыгать (`seek`) и отдаёт заимствование,
    // связанное с самим обходчиком. Ни то ни другое в `Iterator` не
    // выражается, а притворяться им — обещать `map`/`collect`, которые
    // на прыжках дали бы неверный ответ.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        stats::cursor_next(1);
        self.step()
    }

    /// Прыжок вперёд: блоки, чей последний номер меньше цели, пропускаются
    /// целиком — ни одного байта списка не читается.
    ///
    /// Пропускать нечего там, где блок один: у короткого списка и у одиночного
    /// слова прыжок вырождается в шаг, и `blocks.bin` не спрашивается.
    pub fn seek(&mut self, target: u32) -> bool {
        stats::cursor_seek(1);
        if self.t.meta
            && self.block < self.nblocks
            && self.ix.b_last(self.t.first + self.block) < target
        {
            while self.block < self.nblocks && self.ix.b_last(self.t.first + self.block) < target {
                self.advance_block();
                if self.block >= self.nblocks || self.block * BLOCK >= self.df as usize {
                    self.done = true;
                    return false;
                }
            }
            // Дельты в блоке отсчитываются от последнего номера предыдущего.
            self.doc = self.ix.b_last(self.t.first + self.block - 1);
            self.fill();
        }
        while self.doc < target {
            if !self.step() {
                return false;
            }
        }
        true
    }

    /// Текущий документ; `u32::MAX` означает, что список кончился.
    #[inline(always)]
    pub fn cur(&self) -> u32 {
        if self.done {
            u32::MAX
        } else {
            self.doc
        }
    }

    #[inline(always)]
    pub fn score(&self) -> f64 {
        self.w * self.ix.sat_in(self.fld, self.doc, self.tf(), self.sc.as_ref())
    }

    /// Сколько документов в списке.
    #[inline(always)]
    pub fn df(&self) -> u32 {
        self.df
    }

    /// Потолок вклада списка целиком — по нему выбирается опорный список.
    #[inline(always)]
    pub fn list_bound(&self) -> f64 {
        self.bound
    }

    /// Потолок вклада текущего блока.
    ///
    /// Применим только когда курсоры сведены на один документ: тогда их блоки
    /// его действительно содержат. Иначе величина относится к чужому блоку.
    ///
    /// У списка в один блок потолок блока — это потолок списка. Не приближение:
    /// максимум по единственному блоку и есть максимум по всему списку.
    #[inline(always)]
    pub fn block_bound(&self) -> f64 {
        if self.block >= self.nblocks {
            return 0.0;
        }
        if self.t.meta {
            self.w * self.lift * f64::from(self.ix.b_max(self.t.first + self.block))
        } else {
            self.bound
        }
    }

    /// Значений в куске позиций у этого сегмента. Ноль — прогон одним куском.
    ///
    /// Отдаётся наружу разбору и проверке: по нему они знают, как нарезан поток,
    /// не заглядывая в заголовок сегмента.
    #[inline(always)]
    pub fn posblock(&self) -> usize {
        self.ix.posblock
    }

    /// Последний документ текущего блока — граница, до которой действует
    /// [`block_bound`]. За ней потолок относится уже к другому блоку.
    ///
    /// У короткого списка он берётся из распакованного буфера, а не из
    /// `blocks.bin`: блок и так распакован целиком, и хранить последний номер
    /// отдельно значило бы платить четыре байта за то, что уже прочитано.
    #[inline(always)]
    pub fn block_last(&self) -> u32 {
        if self.done || self.block >= self.nblocks {
            return u32::MAX;
        }
        if self.t.meta {
            self.ix.b_last(self.t.first + self.block)
        } else if self.have > 0 {
            self.buf[self.have - 1]
        } else {
            u32::MAX
        }
    }
}

/// Позиции: что записали, то и прочли — при любой нарезке потока.
///
/// ## Почему оракул, а не набор случаев
///
/// Позиции нельзя проверить «глазами по одному примеру»: их правильность —
/// свойство **всего** корпуса сразу, и ломается она там, где кусок кончился
/// посреди документа, где документ длиннее любого куска, где вхождений больше,
/// чем вмещает байт частоты. Поэтому здесь строится независимый ответ прямо из
/// текста: слово → документ → его позиции. Ни одна строка этого ответа не знает
/// ни про раскладку, ни про размер куска.
#[cfg(test)]
mod position_oracle {
    use std::collections::HashMap;

    /// Ожидаемое: слово → (номер документа, позиции), по самому тексту.
    ///
    /// Разбор — тот же, что у приёма: сравнивать надо с тем, что в индекс
    /// клали, а не с собственным представлением о том, как текст делится.
    fn oracle(docs: &[(String, String)]) -> HashMap<String, Vec<(u32, Vec<u32>)>> {
        let a = crate::analyze::Simple;
        let mut out: HashMap<String, Vec<(u32, Vec<u32>)>> = HashMap::new();
        let mut toks = Vec::new();
        for (doc, (_, body)) in docs.iter().enumerate() {
            toks.clear();
            crate::analyze::Analyzer::tokenize(&a, body, &mut toks);
            let mut here: HashMap<&str, Vec<u32>> = HashMap::new();
            for (at, w) in toks.iter().enumerate() {
                here.entry(w.as_str()).or_default().push(at as u32);
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

    /// Корпус, в котором собрано всё, чем позиции ломаются.
    fn corpus() -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        // Обычные короткие документы: их большинство, и они задают фон.
        for i in 0..400u32 {
            out.push((format!("d{i:04}"), format!("общее слово{i} хвост{}", i % 7)));
        }
        // Повторяющиеся слова подряд и вперемешку — перекрывающиеся вхождения.
        out.push(("rep".into(), "эхо эхо эхо да эхо эхо".into()));
        // Документ длиннее любого куска позиций, с одним словом через равные
        // промежутки: позиции пересекают границы кусков много раз.
        let mut long = String::new();
        for i in 0..5_000 {
            long.push_str(if i % 5 == 0 { "маяк " } else { "вода " });
        }
        out.push(("long".into(), long));
        // Вхождений больше двухсот пятидесяти пяти: частота не вмещается в байт,
        // и позиции обязаны уцелеть все.
        let mut many = String::new();
        for i in 0..900 {
            many.push_str(if i % 2 == 0 { "часто " } else { "редко " });
        }
        out.push(("many".into(), many));
        // Кириллица, китайский, эмодзи и разметка вперемешку.
        out.push((
            "mix".into(),
            "Привет мир 世界 你好 世界 🎉 party 🎉 <b>жирный</b> &amp; [[ссылка|текст]] мир".into(),
        ));
        // Документ, где всё слово одно и то же: позиции идут подряд без пропусков.
        out.push(("same".into(), "раз раз раз раз раз раз раз раз".into()));
        out
    }

    fn build(name: &str, docs: &[(String, String)]) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-pos-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 1_000_000);
        for (id, body) in docs.iter() {
            ix.add(id, &[body]);
        }
        ix.flush();
        d
    }

    /// Сверяет весь индекс с оракулом. Отдаёт, сколько слов проверено.
    fn verify(dir: &str, want: &HashMap<String, Vec<(u32, Vec<u32>)>>) -> usize {
        let segs = crate::segments::Segments::load(dir).expect("индекс");
        // Позиции собираются по всем сегментам сразу: номера сквозные.
        let mut got: HashMap<String, Vec<(u32, Vec<u32>)>> = HashMap::new();
        let mut base = 0u32;
        let mut poss = Vec::new();
        for ix in segs.list.iter() {
            for list in 0..ix.dict.len() {
                let (fld, word) = ix.dict.term_at(list);
                let mut c = ix.cursor(list as u32, fld as usize, 1.0);
                let rows = got.entry(word).or_default();
                while c.cur() != u32::MAX {
                    let n = c.positions(&mut poss);
                    assert_eq!(n, c.tf() as usize, "частота и число позиций разошлись");
                    rows.push((base + c.cur(), poss.clone()));
                    c.next();
                }
            }
            base += ix.docs as u32;
        }
        for (word, rows) in want.iter() {
            let mine = got.get(word).unwrap_or_else(|| panic!("слова {word} нет в индексе"));
            assert_eq!(mine, rows, "слово {word}");
        }
        want.len()
    }

    /// Оракул сходится: каждое слово, каждый документ, каждая позиция.
    #[test]
    fn every_position_matches_the_text_it_came_from() {
        let docs = corpus();
        let want = oracle(&docs);
        let d = build("oracle", &docs);
        let n = verify(&d, &want);
        assert!(n > 400, "проверено всего {n} слов");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Прыжки не по порядку дают то же, что проход подряд.
    ///
    /// Курсор помнит, где остановился в потоке позиций, — иначе обход подряд был
    /// бы квадратичным. Память относится к одному блоку постингов, и вопрос,
    /// ради которого написан этот тест, ровно один: не переживает ли она смену
    /// блока. Проход подряд такого не поймает — там смена блока всегда двигает
    /// прогон вперёд, и негодная память отсеивается сама. Ловится это прыжками.
    #[test]
    fn seeking_out_of_order_reads_the_same_positions() {
        let docs = corpus();
        let want = oracle(&docs);
        let d = build("seek", &docs);
        let segs = crate::segments::Segments::load(&d).expect("индекс");
        let ix = &segs.list[0];
        // Слово во всех коротких документах: список длиннее блока, значит блоков
        // несколько, и частоты у всех единичны.
        let list = ix.dict.lookup(0, "общее").expect("общее");
        let rows = &want["общее"];
        assert!(rows.len() > crate::bits::BLOCK, "список короче блока — прыгать не через что");

        let mut poss = Vec::new();
        // Порядок обхода нарочно рваный, но всегда вперёд: назад курсор не ходит.
        for step in [251usize, 97, 41, 7, 1] {
            let mut c = ix.cursor(list, 0, 1.0);
            let mut k = 0usize;
            while k < rows.len() {
                let (doc, ref wpos) = rows[k];
                if !c.seek(doc) {
                    break;
                }
                assert_eq!(c.cur(), doc, "шаг {step}: прыжок промахнулся");
                c.positions(&mut poss);
                assert_eq!(&poss, wpos, "шаг {step}, документ {doc}");
                k += step;
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Вхождения после двести пятьдесят пятого не теряются.
    #[test]
    fn occurrences_past_the_byte_are_all_there() {
        let docs = corpus();
        let want = oracle(&docs);
        let d = build("past255", &docs);
        let segs = crate::segments::Segments::load(&d).expect("индекс");
        let ix = &segs.list[0];
        let list = ix.dict.lookup(0, "часто").expect("часто");
        let c = ix.cursor(list, 0, 1.0);
        let mut poss = Vec::new();
        let n = c.positions(&mut poss);
        assert_eq!(n, 450, "вхождений прочлось {n}");
        assert_eq!(&poss, &want["часто"][0].1, "позиции после 255-го");
        // И самая последняя обязана быть на месте, а не обрезаться байтом.
        assert_eq!(poss.last().copied(), Some(898));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Фраза, slop и перекрывающиеся вхождения — по позициям из индекса.
    #[test]
    fn phrases_hold_across_chunk_boundaries() {
        let docs = corpus();
        let d = build("phrase", &docs);
        let segs = crate::segments::Segments::load(&d).expect("индекс");
        let ix = &segs.list[0];
        let get = |w: &str, doc: u32| -> Vec<u32> {
            let list = ix.dict.lookup(0, w).unwrap_or_else(|| panic!("{w}"));
            let mut c = ix.cursor(list, 0, 1.0);
            let mut out = Vec::new();
            while c.cur() != u32::MAX {
                if c.cur() == doc {
                    c.positions(&mut out);
                    return out;
                }
                c.next();
            }
            Vec::new()
        };
        let doc_of = |id: &str| ix.idmap.find(id).expect(id);

        // Перекрывающиеся вхождения: «эхо эхо эхо да эхо эхо».
        let rep = doc_of("rep");
        let echo = get("эхо", rep);
        assert_eq!(echo, vec![0, 1, 2, 4, 5]);
        assert_eq!(crate::phrase::count(&[&echo, &echo], 0), 3, "пары подряд");
        // «эхо эхо эхо да эхо эхо»: тройка подряд ровно одна, пар — три.
        assert_eq!(crate::phrase::count(&[&echo, &echo, &echo], 0), 1, "тройки подряд");

        // Точная фраза через границу куска: «маяк вода» в длинном документе.
        let long = doc_of("long");
        let (m, w) = (get("маяк", long), get("вода", long));
        assert_eq!(m.len(), 1000, "вхождений маяка");
        assert_eq!(w.len(), 4000, "вхождений воды");
        assert_eq!(crate::phrase::count(&[&m, &w], 0), 1000, "«маяк вода» подряд");
        // Обратный порядок: «вода маяк» тоже встречается — перед каждым маяком,
        // кроме самого первого, стоит вода. Девятьсот девяносто девять раз.
        assert_eq!(crate::phrase::count(&[&w, &m], 0), 999, "«вода маяк» подряд");
        // Фраза с пропуском: «маяк ... маяк» через четыре слова — только со slop.
        assert_eq!(crate::phrase::count(&[&m, &m], 0), 0, "два маяка подряд не стоят");
        assert_eq!(crate::phrase::count(&[&m, &m], 3), 0, "и через три тоже");
        assert_eq!(crate::phrase::count(&[&m, &m], 4), 999, "а через четыре — каждый раз");

        // Разметка и не-латиница: «世界» встречается дважды, «мир» дважды.
        let mix = doc_of("mix");
        assert_eq!(get("世界", mix).len(), 2);
        assert_eq!(get("мир", mix).len(), 2);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Слияние и повторное открытие ничего не меняют.
    ///
    /// Проверяется на **приёмной** нарезке: там позиции лежат в восьми сегментах
    /// и переносятся слиянием по одному постингу, то есть по тому самому пути,
    /// где память курсора и заменила квадратичный пересчёт.
    #[test]
    fn merge_and_reopen_change_nothing() {
        let docs = corpus();
        let want = oracle(&docs);
        let d = std::env::temp_dir()
            .join(format!("svindb-pos-merge-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 64);
        for (id, body) in docs.iter() {
            ix.add(id, &[body]);
        }
        ix.flush();
        let before = crate::segments::Segments::load(&d).expect("индекс").list.len();
        assert!(before > 4, "сегментов всего {before} — слияние нечему проверять");
        verify(&d, &want);

        let sources = crate::segments::Segments::load(&d).expect("индекс");
        let refs: Vec<&crate::index::Index> = sources.list.iter().map(|s| &**s).collect();
        crate::merge::merge(&refs, &format!("{d}/mrg0000")).expect("слияние");
        drop(sources);
        // Список сегментов подменяется на один слитый, и индекс открывается
        // заново — тем же путём, каким это делает обслуживание.
        crate::manifest::write(&d, &["mrg0000".to_string()]).expect("список");
        for s in std::fs::read_dir(&d).expect("каталог").filter_map(|e| e.ok()) {
            let n = s.file_name().to_string_lossy().to_string();
            if n.starts_with("seg") {
                std::fs::remove_dir_all(s.path()).ok();
            }
        }
        verify(&d, &want);
        std::fs::remove_dir_all(&d).ok();
    }
}
