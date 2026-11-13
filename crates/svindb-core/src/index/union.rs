//! Курсор по **объединению** списков — одно слово запроса целиком.

use super::Cursor;
use crate::stats;

/// С этого числа форм куча начинает окупать свою косвенность.
const HEAP_FROM: usize = 8;

pub struct Union<'a> {
    subs: Vec<Cursor<'a>>,
    /// Номера текущих документов подкурсоров, по одному на форму.
    ///
    /// Копия того, что и так лежит в курсорах, — но лежащая подряд. Разбор в
    /// заголовке модуля.
    keys: Vec<u32>,
    /// Куча-минимум из номеров подкурсоров, ключ — текущий документ.
    /// Пуста, когда форм мало и обход идёт прямым перебором.
    heap: Vec<u32>,
    /// Текущий документ при прямом переборе.
    doc: u32,
    /// Позиции в куче, стоящие на текущем документе. Переиспользуется.
    at: Vec<u32>,
    /// Для какого документа собран `at`. Отсечение спрашивает потолок блока и
    /// скор подряд по одному и тому же документу — второй раз собирать незачем.
    at_doc: u32,
    /// Ближайшая форма, обогнавшая текущий документ. Граница, за которую
    /// потолок текущих блоков уже не отвечает.
    frontier: u32,
    /// Стек обхода кучи. Поле, чтобы не выделять память на каждом документе.
    stack: Vec<u32>,
}

impl<'a> Union<'a> {
    pub fn new(subs: Vec<Cursor<'a>>) -> Union<'a> {
        let n = subs.len();
        let keys: Vec<u32> = subs.iter().map(|c| c.cur()).collect();
        if n < HEAP_FROM {
            let doc = keys.iter().copied().min().unwrap_or(u32::MAX);
            return Union {
                subs,
                keys,
                heap: Vec::new(),
                doc,
                at: Vec::new(),
                frontier: u32::MAX,
                at_doc: u32::MAX,
                stack: Vec::new(),
            };
        }
        let mut u = Union {
            subs,
            keys,
            heap: (0..n as u32).collect(),
            doc: 0,
            at: Vec::with_capacity(n),
            at_doc: u32::MAX,
            frontier: u32::MAX,
            stack: Vec::with_capacity(64),
        };
        // Построение кучи снизу вверх: `n` действий, а не `n log n`.
        for i in (0..n / 2).rev() {
            u.sift_down(i);
        }
        u
    }

    /// Двигает подкурсор и тут же поправляет его номер.
    ///
    /// Единственное место, где курсор сдвигается, — чтобы номер не мог отстать.
    #[inline(always)]
    fn advance(&mut self, i: usize, target: Option<u32>) {
        match target {
            Some(t) => self.subs[i].seek(t),
            None => self.subs[i].next(),
        };
        self.keys[i] = self.subs[i].cur();
    }

    #[inline(always)]
    fn heaped(&self) -> bool {
        !self.heap.is_empty()
    }

    #[inline(always)]
    fn key(&self, pos: usize) -> u32 {
        let i = self.heap[pos] as usize;
        debug_assert_eq!(self.keys[i], self.subs[i].cur(), "номер разошёлся с курсором");
        self.keys[i]
    }

    fn sift_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        let mut levels = 0u64;
        loop {
            levels += 1;
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut m = i;
            if l < n && self.key(l) < self.key(m) {
                m = l;
            }
            if r < n && self.key(r) < self.key(m) {
                m = r;
            }
            if m == i {
                stats::sift_levels(levels);
                return;
            }
            self.heap.swap(i, m);
            i = m;
        }
    }

    /// Ближайшая форма впереди, если на документе стоит **только корень**.
    ///
    /// В куче-минимуме узлы, равные минимуму, связны от корня, поэтому хватает
    /// взглянуть на двух его детей: оба строго больше — значит кроме корня на
    /// документе никого нет, и ответ на все вопросы даёт один подкурсор. Два
    /// обращения вместо обхода области.
    ///
    /// Случай этот не редкий, а почти единственный: формы одного слова редко
    /// совпадают на одном документе. Счётчики показали, что полный обход
    /// вызывался **на каждом шаге** и находил ровно один узел — то есть вся его
    /// работа была впустую.
    #[inline(always)]
    fn frontier_if_sole(&self) -> Option<u32> {
        let doc = self.key(0);
        let a = if self.heap.len() > 1 { self.key(1) } else { u32::MAX };
        if a == doc {
            return None;
        }
        let b = if self.heap.len() > 2 { self.key(2) } else { u32::MAX };
        if b == doc {
            return None;
        }
        Some(a.min(b))
    }

    /// Позиции подкурсоров кучи, стоящих на текущем документе.
    ///
    /// В куче-минимуме все узлы, равные минимуму, образуют связную область от
    /// корня, поэтому обход останавливается сразу за её границей и стоит
    /// столько, сколько таких узлов, а не сколько всего форм.
    fn gather(&mut self) {
        let doc = self.cur();
        if doc == self.at_doc {
            return;
        }
        stats::gather_calls(1);
        self.at.clear();
        self.at_doc = doc;
        self.frontier = u32::MAX;
        if doc == u32::MAX {
            return;
        }
        // Стек обхода — поле, а не массив на стеке: предел раскрытия задаётся
        // запросом, и фиксированный размер означал бы молча потерянную форму,
        // то есть заниженный скор без единого признака ошибки.
        self.stack.clear();
        self.stack.push(0);
        while let Some(pos) = self.stack.pop() {
            let pos = pos as usize;
            if pos >= self.heap.len() {
                continue;
            }
            let k = self.key(pos);
            if k != doc {
                // Граница области минимума. Всё, что глубже, не меньше этого
                // узла, поэтому ближайшая обогнавшая форма — минимум по таким
                // границам, и обход дальше не идёт.
                if k < self.frontier {
                    self.frontier = k;
                }
                continue;
            }
            self.at.push(pos as u32);
            stats::gather_nodes(1);
            for child in [2 * pos + 1, 2 * pos + 2] {
                if child < self.heap.len() {
                    self.stack.push(child as u32);
                }
            }
        }
    }

    #[inline(always)]
    /// Сколько документов во всех формах слова вместе.
    ///
    /// Оценка сверху: одна и та же пара документ-слово может попасть в
    /// несколько форм маски. Завышение здесь безвредно — по этой величине
    /// выбирается ведущий обхода, и завышение уводит выбор в сторону
    /// осторожности.
    pub fn df(&self) -> u32 {
        self.subs.iter().map(|c| c.df()).sum()
    }

    pub fn cur(&self) -> u32 {
        if self.heaped() {
            self.key(0)
        } else {
            self.doc
        }
    }

    /// Списков под словом. Одно слово может быть раскрыто в десятки форм.
    pub fn arity(&self) -> usize {
        self.subs.len()
    }

    // Не `Iterator`: обход умеет прыгать (`seek`) и отдаёт заимствование,
    // связанное с самим обходчиком. Ни то ни другое в `Iterator` не
    // выражается, а притворяться им — обещать `map`/`collect`, которые
    // на прыжках дали бы неверный ответ.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        stats::word_next(1);
        if !self.heaped() {
            let at = self.doc;
            for i in 0..self.subs.len() {
                if self.keys[i] == at {
                    self.advance(i, None);
                }
            }
            self.doc = self.keys.iter().copied().min().unwrap_or(u32::MAX);
            return self.doc != u32::MAX;
        }
        let doc = self.cur();
        if doc == u32::MAX {
            return false;
        }
        // Пока в корне тот же документ — сдвигать корень. Каждый сдвиг стоит
        // `log n`, а повторов ровно столько, сколько форм стояло на документе.
        while self.key(0) == doc {
            let i = self.heap[0] as usize;
            self.advance(i, None);
            self.sift_down(0);
        }
        self.cur() != u32::MAX
    }

    pub fn seek(&mut self, target: u32) -> bool {
        stats::word_seek(1);
        if !self.heaped() {
            if self.doc >= target {
                return self.doc != u32::MAX;
            }
            for i in 0..self.subs.len() {
                if self.keys[i] < target {
                    self.advance(i, Some(target));
                }
            }
            self.doc = self.keys.iter().copied().min().unwrap_or(u32::MAX);
            return self.doc != u32::MAX;
        }
        while self.key(0) < target {
            let i = self.heap[0] as usize;
            self.advance(i, Some(target));
            self.sift_down(0);
        }
        self.cur() != u32::MAX
    }

    /// Вклад слова берётся один раз — по лучшей из совпавших форм.
    pub fn score(&mut self) -> f64 {
        if self.heaped() && self.frontier_if_sole().is_some() {
            return self.subs[self.heap[0] as usize].score();
        }
        if !self.heaped() {
            let at = self.doc;
            return self
                .subs
                .iter()
                .enumerate()
                .filter(|(i, _)| self.keys[*i] == at)
                .map(|(_, c)| c.score())
                .fold(0.0, f64::max);
        }
        self.gather();
        let mut best = 0.0f64;
        for &pos in self.at.iter() {
            let s = self.subs[self.heap[pos as usize] as usize].score();
            if s > best {
                best = s;
            }
        }
        best
    }

    /// Потолок вклада слова: по лучшей из форм, а не по сумме.
    ///
    /// Сумма была бы неверна вдвойне — вклад берётся один раз, значит и потолок
    /// один. Завышенный потолок не теряет документы, но и не отсекает: опорный
    /// список выбирался бы слишком рано, и обход просматривал бы всё подряд.
    pub fn list_bound(&self) -> f64 {
        self.subs.iter().map(|c| c.list_bound()).fold(0.0, f64::max)
    }

    /// Докуда можно прыгнуть, если прыжок вообще имеет смысл.
    ///
    /// `Some(граница)` — только когда её задаёт **конец блока**, то есть когда
    /// прыжок действительно перешагивает остаток блока. `None` — когда границу
    /// задаёт ближайшая форма впереди: туда обход и так придёт следующим шагом,
    /// и прыгать значит идти той же дорогой, но дороже.
    pub fn skip_limit(&mut self) -> Option<u32> {
        let doc = self.cur();
        if self.heaped() {
            let frontier = self.frontier_if_sole()?;
            let last = self.subs[self.heap[0] as usize].block_last();
            return if last < frontier { Some(last) } else { None };
        }
        // Мало форм — перебор дешевле любой хитрости.
        let mut block_edge = u32::MAX;
        let mut ahead = u32::MAX;
        for (i, c) in self.subs.iter().enumerate() {
            if self.keys[i] == doc {
                block_edge = block_edge.min(c.block_last());
            } else {
                ahead = ahead.min(self.keys[i]);
            }
        }
        if block_edge < ahead {
            Some(block_edge)
        } else {
            None
        }
    }

    pub fn block_bound(&mut self) -> f64 {
        if self.heaped() && self.frontier_if_sole().is_some() {
            return self.subs[self.heap[0] as usize].block_bound();
        }
        if !self.heaped() {
            let at = self.doc;
            return self
                .subs
                .iter()
                .enumerate()
                .filter(|(i, _)| self.keys[*i] == at)
                .map(|(_, c)| c.block_bound())
                .fold(0.0, f64::max);
        }
        self.gather();
        let mut best = 0.0f64;
        for &pos in self.at.iter() {
            let s = self.subs[self.heap[pos as usize] as usize].block_bound();
            if s > best {
                best = s;
            }
        }
        best
    }
}

impl<'a> Union<'a> {
    /// Позиции текущего документа по **всем** формам слова, слитые и без
    /// повторов.
    ///
    /// Фраза с объединением не работает намеренно: «порядок альтернатив» —
    /// бессмыслица, и об этом сказано у [`crate::search::QueryPhrase`]. Со
    /// слотами иначе: слот, где встретилась любая форма слова, — это слот, где
    /// слово встретилось. Утверждение осмысленно, поэтому слияние здесь
    /// законно, а во фразе нет.
    pub fn positions(&self, out: &mut Vec<u32>) -> usize {
        out.clear();
        let mut buf: Vec<u32> = Vec::new();
        for c in self.subs.iter() {
            if c.cur() != self.cur() {
                continue;
            }
            c.positions(&mut buf);
            out.extend_from_slice(&buf);
        }
        out.sort_unstable();
        out.dedup();
        out.len()
    }
}
