//! Набор сегментов: поиск по каждому и слияние верхушек.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::dict::DEFAULT_EXPANSIONS;
use crate::index::{self, Index};
use crate::search::{self, QueryWord, Request};

/// Слово запроса как его прислали: текст и признак маски.
///
/// Разрешение в номера списков делается **на каждом сегменте отдельно**:
/// словари у сегментов свои, и одно и то же слово имеет в них разные номера.
#[derive(Clone, Debug)]
pub struct Term {
    pub text: String,
    /// Слово с `*` на конце — маска, раскрывается по словарю проходом вперёд.
    pub prefix: bool,
}

impl Term {
    /// Разбирает слово: хвостовая звёздочка означает маску.
    pub fn parse(raw: &str) -> Term {
        match raw.strip_suffix('*') {
            Some(head) => Term { text: head.to_string(), prefix: true },
            None => Term { text: raw.to_string(), prefix: false },
        }
    }
}

/// Слово запроса по конкретному полю. Формы внутри — альтернативы.
#[derive(Clone, Debug)]
pub struct SpecWord {
    /// **Устойчивый** номер поля, а не местный номер сегмента.
    ///
    /// Разница несущая. Местный номер годится ровно для одного сегмента;
    /// применённый к другому, он читает чужое поле — не отказом, а другой
    /// выдачей. Перевод в местный делает [`QuerySpec::resolve`], у каждого
    /// сегмента свой.
    pub fid: usize,
    pub terms: Vec<Term>,
    pub given: Option<f64>,
}

#[derive(Clone, Debug)]
/// Условие отбора по значению поля — **разрешённое**, с местным номером.
///
/// Отбор не участвует в ранжировании: он отсекает, а не взвешивает. Несколько
/// условий соединяются по «и» — документ обязан удовлетворять всем.
pub struct Range {
    /// Местный номер поля в **этом** сегменте. [`NO_FIELD`] — поля здесь нет.
    pub field: usize,
    /// Границы включительно. `i64::MIN`/`i64::MAX` — открытый край.
    pub from: i64,
    pub to: i64,
}

/// То же условие в запросе, до перевода в местные номера.
///
/// Отдельный тип, а не тот же самый, потому что смысл числа разный: одна
/// структура на оба смысла — это приглашение перепутать местный номер с
/// устойчивым там, где компилятор смолчит.
#[derive(Clone, Debug)]
pub struct SpecRange {
    /// Устойчивый номер поля.
    pub fid: usize,
    pub from: i64,
    pub to: i64,
}

/// Поля нет в этом сегменте.
pub const NO_FIELD: usize = usize::MAX;

/// Общий на запрос бюджет раскрытия prefix-форм.
///
/// `accepted` считает уникальную пару «устойчивое поле, раскрытое слово» по
/// всему ReadView, а `examined` — фактически прочитанные записи словарей. Это
/// две разные цены: одна форма встречается в тысяче сегментов и принимается
/// один раз, но её наличие приходится увидеть в каждом из них.
pub struct ExpansionBudget {
    accepted_limit: usize,
    examined_limit: usize,
    accepted: AtomicUsize,
    examined: AtomicUsize,
    truncated: AtomicBool,
    terms: Mutex<HashMap<(usize, String), HashSet<String>>>,
}

impl ExpansionBudget {
    pub fn new(accepted_limit: usize, examined_limit: usize) -> ExpansionBudget {
        ExpansionBudget {
            accepted_limit,
            examined_limit,
            accepted: AtomicUsize::new(0),
            examined: AtomicUsize::new(0),
            truncated: AtomicBool::new(false),
            terms: Mutex::new(HashMap::new()),
        }
    }

    fn accepted_for(&self, fid: usize, prefix: &str) -> Vec<String> {
        let mut terms: Vec<String> = self
            .terms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(fid, prefix.to_string()))
            .map(|terms| terms.iter().cloned().collect())
            .unwrap_or_default();
        // HashSet нужен для дедупликации общего бюджета, но его порядок
        // зависит от случайного seed процесса. Сортировка перед lookup делает
        // повторный ReadView и A/B-прогон воспроизводимыми, не меняя множества
        // разрешённых терминов и их exact semantics.
        terms.sort_unstable();
        terms
    }

    /// Учитывает одну физически рассмотренную форму и решает, разрешена ли
    /// она. Уже принятая форма остаётся разрешённой даже на самом краю бюджета:
    /// иначе один сегмент нашёл бы её, а следующий потерял — результат зависел
    /// бы от нарезки корпуса.
    fn consider(&self, fid: usize, prefix: &str, term: &str) -> bool {
        let seen = self.examined.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let mut terms = self.terms.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let accepted = terms.entry((fid, prefix.to_string())).or_default();
        if accepted.contains(term) {
            if self.examined_limit > 0 && seen > self.examined_limit {
                self.truncated.store(true, Ordering::Relaxed);
            }
            return true;
        }
        if (self.examined_limit > 0 && seen > self.examined_limit)
            || self.accepted.load(Ordering::Relaxed) >= self.accepted_limit
        {
            self.truncated.store(true, Ordering::Relaxed);
            return false;
        }
        accepted.insert(term.to_string());
        self.accepted.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn remaining_examined(&self) -> usize {
        if self.examined_limit == 0 {
            usize::MAX
        } else {
            self.examined_limit.saturating_sub(self.examined.load(Ordering::Relaxed))
        }
    }

    fn saturated(&self) -> bool {
        self.accepted.load(Ordering::Relaxed) >= self.accepted_limit
            || self.remaining_examined() == 0
    }

    pub fn truncated(&self) -> bool {
        self.truncated.load(Ordering::Relaxed)
    }

    pub fn progress(&self) -> (usize, usize) {
        (self.examined.load(Ordering::Relaxed), self.accepted.load(Ordering::Relaxed))
    }
}

/// Раскрывает слово по словарю сегмента.
///
/// ## Транслитерация и раскладка
///
/// `translit` добавляет к слову его прочтения: `ghbdtn` читается как `привет`
/// по позиции клавиши, `klaviatura` — как `клавиатура` по звучанию. Прочтений у
/// латинской записи много, и **выбирает между ними словарь сегмента**: список
/// заводится только тому кандидату, который в индексе действительно есть.
/// Отбор здесь обязателен, а не осторожность: без него `son` притащил бы `сон`,
/// а `rot` — `рот`, и точность посыпалась бы на каждом английском запросе.
///
/// Прочтения — **альтернативы** слову, а не отдельные слова: документ, где
/// нашлось любое из них, совпал один раз. Ровно так же устроена маска, и это не
/// совпадение, а то же самое утверждение о запросе.
///
/// По умолчанию выключено. На английском корпусе транслитерация — чистый шум, и
/// платить за неё обязан тот, кто её попросил.
fn resolve_word(
    ix: &Index,
    w: &SpecWord,
    max_expansions: usize,
    translit: bool,
    guard: Option<&crate::work::Guard>,
    expansion: Option<&ExpansionBudget>,
) -> search::QueryWord {
    let mut lists = Vec::new();
    // Поля нет в этом сегменте — слово не совпадает ни с чем, и это верный
    // ответ, а не потеря: сегмент собран до того, как поле объявили. Слово всё
    // равно кладётся, с пустым списком, — по позиции в векторе слова
    // сопоставляются между сегментами ради общей частоты.
    let Some(field) = ix.field_by_id(w.fid as u32) else {
        return search::QueryWord::new(NO_FIELD, lists);
    };
    let literal = w.given.is_some();
    for t in w.terms.iter() {
        if guard.is_some_and(|g| !g.check()) {
            break;
        }
        // Отрицательный Bloom для буквального терма позволяет не открывать
        // словарь. Prefix/translit требуют других форм, поэтому здесь
        // применимы только при явно буквальном весе или отключённой функции.
        if (!t.prefix || literal)
            && (!translit || literal)
            && ix.term_filter.as_ref().is_some_and(|filter| !filter.maybe_has_term(&t.text))
        {
            continue;
        }
        if t.prefix && !literal {
            if let Some(expansion) = expansion {
                // Уже принятые глобальные формы находятся точными lookup: как
                // только accepted budget заполнен, prefix walk на остальных
                // сегментах больше не нужен для результата.
                let accepted = expansion.accepted_for(w.fid, &t.text);
                for term in accepted.iter() {
                    if let Some(list) = ix.dict.lookup(field as u32, term) {
                        if !lists.contains(&list) {
                            lists.push(list);
                        }
                    }
                }
                if expansion.saturated() {
                    // Бюджет заполнен. Сам факт непройденного следующего
                    // сегмента означает, что полноту новых форм доказать уже
                    // нельзя; partial называется явно в ответе.
                    expansion.truncated.store(true, Ordering::Relaxed);
                    continue;
                }
                let cap = expansion.remaining_examined().min(10_000);
                let expanded = ix.dict.by_prefix_terms(field as u32, &t.text, cap);
                crate::stats::prefix_expansions(expanded.len() as u64);
                for (term, list) in expanded {
                    if expansion.consider(w.fid, &t.text, &term) {
                        if !lists.contains(&list) {
                            lists.push(list);
                        }
                    } else {
                        break;
                    }
                }
                if expansion.saturated() {
                    // На точной границе нельзя доказать, что следующей формы
                    // нет, не переступив examined budget. Консервативный
                    // partial честнее ложного `complete:true`.
                    expansion.truncated.store(true, Ordering::Relaxed);
                }
            } else {
                let expanded = ix.dict.by_prefix(field as u32, &t.text, max_expansions);
                crate::stats::prefix_expansions(expanded.len() as u64);
                lists.extend(expanded);
            }
            continue;
        }
        if let Some(l) = ix.dict.lookup(field as u32, &t.text) {
            lists.push(l);
        }
        if !translit || literal {
            continue;
        }
        for cand in crate::translit::readings(&t.text, max_expansions) {
            if guard.is_some_and(|g| !g.check()) {
                break;
            }
            if lists.len() >= max_expansions {
                break;
            }
            if let Some(l) = ix.dict.lookup(field as u32, &cand) {
                // Один список дважды — это не альтернатива, а двойной вес.
                if !lists.contains(&l) {
                    lists.push(l);
                }
            }
        }
    }
    let mut out = search::QueryWord::new(field, lists);
    out.weight = w.given;
    out
}

/// Слова фразы в номера списков этого сегмента.
///
/// Слово, которого в сегменте нет, оставляет пустой список — и узел фразы на
/// этом отступает целиком. Это верно: фраза без одного слова не фраза.
fn resolve_phrase(
    ix: &Index,
    p: &SpecPhrase,
    guard: Option<&crate::work::Guard>,
) -> search::QueryPhrase {
    // Фраза по полю внутри массива не имеет смысла и потому отвергается.
    //
    // Позиция там — номер элемента, а не номер слова: все слова одного элемента
    // стоят на одном месте. Фраза сравнила бы номера элементов как порядок слов
    // и ответила бы правдоподобно и неверно. Пустая фраза не совпадает ни с
    // чем, и это честный ответ — в отличие от выдуманного.
    if let Some(f) = ix.field_by_id(p.fid as u32) {
        if ix.fields[f].group != crate::slots::ROOT {
            let words =
                p.terms.iter().map(|_| search::QueryWord::new(NO_FIELD, Vec::new())).collect();
            return search::QueryPhrase { field: NO_FIELD, words, slop: p.slop, boost: p.boost };
        }
    }
    // Поля здесь нет — фраза не совпадает ни с чем. Слова кладутся пустыми,
    // чтобы длина фразы осталась той же: по ней считается допустимый разрыв.
    let Some(field) = ix.field_by_id(p.fid as u32) else {
        let words = p.terms.iter().map(|_| search::QueryWord::new(NO_FIELD, Vec::new())).collect();
        return search::QueryPhrase { field: NO_FIELD, words, slop: p.slop, boost: p.boost };
    };
    let words = p
        .terms
        .iter()
        .map(|t| {
            let lists = guard
                .is_none_or(crate::work::Guard::check)
                .then(|| ix.dict.lookup(field as u32, t))
                .flatten()
                .into_iter()
                .collect();
            search::QueryWord::new(field, lists)
        })
        .collect();
    search::QueryPhrase { field, words, slop: p.slop, boost: p.boost }
}

fn resolve_bool(
    ix: &Index,
    b: &SpecBool,
    max_expansions: usize,
    translit: bool,
    guard: Option<&crate::work::Guard>,
    expansion: Option<&ExpansionBudget>,
) -> search::BoolReq {
    let arm = |src: &[SpecClause]| -> Vec<search::Clause> {
        src.iter()
            .map(|c| match c {
                SpecClause::Term(w) => search::Clause::Word(resolve_word(
                    ix,
                    w,
                    max_expansions,
                    translit,
                    guard,
                    expansion,
                )),
                SpecClause::Phrase(p) => search::Clause::Phrase(resolve_phrase(ix, p, guard)),
                SpecClause::Bool(inner) => search::Clause::Bool(resolve_bool(
                    ix,
                    inner,
                    max_expansions,
                    translit,
                    guard,
                    expansion,
                )),
                SpecClause::Slots(s) => search::Clause::Slots(search::SlotReq {
                    group: s.group,
                    all: s
                        .all
                        .iter()
                        .map(|x| {
                            resolve_slot_term(ix, x, max_expansions, translit, guard, expansion)
                        })
                        .collect(),
                    none: s
                        .none
                        .iter()
                        .map(|x| {
                            resolve_slot_term(ix, x, max_expansions, translit, guard, expansion)
                        })
                        .collect(),
                    min_count: s.min_count,
                    boost: s.boost,
                }),
            })
            .collect()
    };
    search::BoolReq {
        must: arm(&b.must),
        should: arm(&b.should),
        must_not: arm(&b.must_not),
        min_should: b.min_should,
    }
}

/// Потолок общего номера документа.
///
/// ## Почему именно столько
///
/// Общий номер — восемь байт, но потолок не `u64::MAX`, а `2^53 − 1`. Это
/// граница, за которой целое перестаёт быть представимым в `f64` **точно**, а
/// значит и в JSON: там числа — двойной точности, и `9007199254740993` придёт
/// клиенту как `9007199254740992`. Молча. Отдать номер, который клиент прочтёт
/// как другой, хуже, чем отказать.
///
/// Девять квадриллионов — это в девяносто тысяч раз больше ста миллиардов, так
/// что практического ограничения здесь нет; есть честная граница формата.
///
/// ## Что изменилось и почему это не ломает поиск
///
/// Прежде общий номер был четырёхбайтным, и потолок приходился на 4.29 млрд
/// документов **на индекс**. Расширен только он. Номер **внутри сегмента**
/// остался четырёхбайтным, и это не недоделка: там значение `u32::MAX` занято
/// курсором под «список кончился», а сегмент по построению не бывает больше
/// пятидесяти миллионов документов (`settings::MAX_TARGET_DOCS`). Горячий путь
/// обхода списков работает с местными номерами и не подорожал ни на байт.
pub const MAX_DOCS: u64 = (1u64 << 53) - 1;

/// Начала сегментов в общем пространстве номеров. `Err` — оно кончилось.
///
/// Отказ на открытии — в том же духе, что «пропавший сегмент из списка — отказ,
/// а не тихий пропуск»: узел не поднимается и говорит, почему. Прежде сумма
/// набиралась `saturating_add`, и у восемьдесят шестого сегмента по 50 млн
/// документов она упиралась в потолок: хвост его документов получал номера
/// **ранних**. Наружу это выходило не ошибкой, а чужим содержимым.
pub fn bases(sizes: &[usize]) -> Result<Vec<u64>, String> {
    let mut out = Vec::with_capacity(sizes.len());
    let mut off = 0u64;
    for (i, &n) in sizes.iter().enumerate() {
        out.push(off);
        off = off.saturating_add(n as u64);
        if off > MAX_DOCS {
            return Err(format!(
                "номера документов кончились на сегменте {i}: с ним их {off} при потолке \
                 {MAX_DOCS} — за этой границей целое не представимо в JSON точно, и номер \
                 пришёл бы клиенту другим"
            ));
        }
    }
    Ok(out)
}

/// Все слова запроса подряд, в порядке, одинаковом для любого сегмента.
///
/// Порядок здесь — это способ сопоставления: слово номер `j` в одном сегменте
/// обязано быть тем же словом запроса, что и `j` в другом. Держится это на том,
/// что раскрытие идёт по одному и тому же запросу и кладёт слово даже с пустым
/// списком.
fn words_mut(req: &mut Request) -> Vec<&mut QueryWord> {
    fn clauses<'a>(cs: &'a mut [search::Clause], out: &mut Vec<&'a mut QueryWord>) {
        for c in cs.iter_mut() {
            match c {
                search::Clause::Word(w) => out.push(w),
                search::Clause::Phrase(p) => out.extend(p.words.iter_mut()),
                search::Clause::Slots(s) => {
                    out.extend(s.all.iter_mut().filter_map(|x| match x {
                        search::SlotTerm::Word(w) => Some(w),
                        _ => None,
                    }));
                    out.extend(s.none.iter_mut().filter_map(|x| match x {
                        search::SlotTerm::Word(w) => Some(w),
                        _ => None,
                    }));
                }
                search::Clause::Bool(b) => {
                    clauses(&mut b.must, out);
                    clauses(&mut b.should, out);
                    clauses(&mut b.must_not, out);
                }
            }
        }
    }
    let mut out: Vec<&mut QueryWord> = Vec::new();
    out.extend(req.words.iter_mut());
    out.extend(req.must.iter_mut());
    out.extend(req.must_not.iter_mut());
    clauses(&mut req.tree.must, &mut out);
    clauses(&mut req.tree.should, &mut out);
    clauses(&mut req.tree.must_not, &mut out);
    out
}

/// Лучшее вперёд: по ключам сортировки, при равенстве — меньший номер.
fn best_first(a: &Hit, b: &Hit) -> std::cmp::Ordering {
    if a.nkeys == 0 {
        return b.score.total_cmp(&a.score).then(a.doc.cmp(&b.doc));
    }
    for i in 0..a.nkeys as usize {
        if a.keys[i] != b.keys[i] {
            return b.keys[i].cmp(&a.keys[i]);
        }
    }
    a.doc.cmp(&b.doc)
}

/// Один ключ сортировки.
#[derive(Clone)]
pub struct Sort {
    /// Устойчивый номер поля — см. [`SpecWord::fid`].
    pub fid: usize,
    pub desc: bool,
}

/// Ветвь дерева условий в запросе, до раскрытия по словарю.
#[derive(Clone, Default)]
pub struct SpecBool {
    pub must: Vec<SpecClause>,
    pub should: Vec<SpecClause>,
    pub must_not: Vec<SpecClause>,
    pub min_should: usize,
}

/// Лист или вложенная ветвь.
#[derive(Clone)]
pub enum SpecClause {
    Term(SpecWord),
    Phrase(SpecPhrase),
    Bool(SpecBool),
    Slots(SpecSlots),
}

/// Условия на один элемент массива — до перевода в местные номера сегмента.
#[derive(Clone)]
pub struct SpecSlots {
    /// Номер группы: путь массива. Общий для всего индекса, как и `fid`.
    pub group: u32,
    pub all: Vec<SpecSlotTerm>,
    pub none: Vec<SpecSlotTerm>,
    pub min_count: u32,
    pub boost: f64,
}

/// Одно условие на элемент.
#[derive(Clone)]
pub enum SpecSlotTerm {
    Term(SpecWord),
    Range(SpecRange),
    Exists { fid: usize },
}

fn resolve_slot_term(
    ix: &Index,
    t: &SpecSlotTerm,
    max_expansions: usize,
    translit: bool,
    guard: Option<&crate::work::Guard>,
    expansion: Option<&ExpansionBudget>,
) -> search::SlotTerm {
    match t {
        SpecSlotTerm::Term(w) => {
            search::SlotTerm::Word(resolve_word(ix, w, max_expansions, translit, guard, expansion))
        }
        SpecSlotTerm::Range(r) => search::SlotTerm::Range {
            field: ix.field_by_id(r.fid as u32).unwrap_or(NO_FIELD),
            from: r.from,
            to: r.to,
        },
        SpecSlotTerm::Exists { fid } => {
            search::SlotTerm::Exists { field: ix.field_by_id(*fid as u32).unwrap_or(NO_FIELD) }
        }
    }
}

/// Фраза запроса: слова по порядку и допустимый разрыв.
#[derive(Clone)]
pub struct SpecPhrase {
    /// Устойчивый номер поля — см. [`SpecWord::fid`].
    pub fid: usize,
    /// Слова как они написаны. Маска здесь не раскрывается: фраза требует
    /// порядка, а маска даёт альтернативы, и «порядок альтернатив» пришлось бы
    /// придумывать.
    pub terms: Vec<String>,
    pub slop: u32,
    pub boost: f64,
}

#[derive(Clone)]
pub struct QuerySpec {
    /// Необязательные слова: влияют на вес, но не обязаны совпасть.
    pub words: Vec<SpecWord>,
    /// Обязательные: документ без любого из них не совпал вовсе.
    pub must: Vec<SpecWord>,
    /// Запрещённые: документ с любым из них выбывает.
    pub must_not: Vec<SpecWord>,
    /// Вложенные условия. Пусто — запрос описан плоскими списками выше.
    pub nested: Option<SpecBool>,
    /// Сколько необязательных обязано совпасть.
    pub min_should: usize,
    /// Ключи сортировки по порядку. Второй решает ничьи первого.
    pub sort: Vec<Sort>,
    /// Сколько верхних пропустить.
    pub from: usize,
    /// Ключи последнего показанного: продолжать строго после него.
    pub after: Option<(Vec<search::SortKey>, u64)>,
    /// Условия отбора. Пусто — отбора нет.
    pub filter: Vec<SpecRange>,
    pub k: usize,
    /// Требуемая полнота общего количества совпадений. `Omit` сохраняет
    /// прежний быстрый договор без total; `UpTo` допускает bounded count.
    pub track_total_hits: TrackTotalHits,
    /// Потолок раскрытия маски. Столько же берёт Elasticsearch по умолчанию.
    pub max_expansions: usize,
    /// Сколько словарных записей разрешено физически рассмотреть при раскрытии
    /// масок по всему ReadView. Ноль — без предела.
    pub max_expansion_examined: usize,
    /// Какие поля подсветить и каким куском. Пусто — не подсвечивать.
    ///
    /// Подсветка идёт разбором содержимого заново, а не по хранимым смещениям:
    /// хранилище у нас есть, и смещения можно получить вместо того, чтобы
    /// платить за них диском на каждом вхождении каждого слова.
    pub highlight: Vec<usize>,
    /// Длина куска подсветки в байтах.
    pub highlight_size: usize,
    /// Фразы верхнего уровня. Обязательны: документ без фразы не совпал.
    pub phrases: Vec<SpecPhrase>,
    pub fields: Vec<usize>,
    /// Отдавать ли идентификатор документа рядом с его номером.
    ///
    /// Не всегда: чтение идентификатора — это обращение в отдельный файл на
    /// каждое попадание, и платить за него там, где он не нужен, не за что.
    pub with_ids: bool,
    /// Искать ли слово ещё и по его прочтениям: раскладка и транслитерация.
    pub translit: bool,
    /// Сколько документов позволено прочесть ради порядка слов во фразе.
    ///
    /// Ноль — без предела. Умолчание — [`DEFAULT_VERIFY`], и оно щедрое
    /// намеренно: это не ручка настройки, а сторож. Разбор — у
    /// [`crate::search::Budget`].
    pub max_verify: usize,
    /// Сколько миллисекунд позволено считать агрегации. `0` — без срока.
    ///
    /// Не ручка производительности, а сторож: работа счёта равна числу
    /// совпадений, а не размеру ответа, и без срока `count` без слов занимает
    /// поток соединения ровно настолько, насколько велик корпус. Разбор — у
    /// [`crate::aggs::Guard`].
    pub max_agg_ms: u64,
    /// Общий deadline/cancellation guard search, aggregation и hydration.
    pub guard: Option<Arc<crate::work::Guard>>,
    /// По сегменту — immutable live bitmap PIT. `None` означает обычный
    /// последний ReadView.
    pub live: Option<Arc<Vec<Arc<crate::live::Snapshot>>>>,
    /// Общий отчёт prefix expansion для всех сегментов запроса.
    pub expansion: Option<Arc<ExpansionBudget>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TrackTotalHits {
    #[default]
    Omit,
    Exact,
    UpTo(u64),
}

/// Умолчание предела проверок по тексту.
pub const DEFAULT_VERIFY: usize = 100_000;
pub const DEFAULT_EXPANSION_EXAMINED: usize = 10_000;

/// Умолчание срока счёта агрегаций.
///
/// Десять секунд — это не «сколько не жалко», а порядок, за которым запрос
/// перестаёт быть запросом. Полный счёт по ста миллионам документов при
/// измеренных сотнях наносекунд на документ укладывается; всё, что не
/// укладывается, оператор увидит по признаку неполноты, а не по занятому ядру.
pub const DEFAULT_AGG_MS: u64 = 10_000;

impl Default for QuerySpec {
    fn default() -> Self {
        QuerySpec {
            words: Vec::new(),
            must: Vec::new(),
            must_not: Vec::new(),
            nested: None,
            min_should: 0,
            sort: Vec::new(),
            from: 0,
            after: None,
            filter: Vec::new(),
            k: 10,
            track_total_hits: TrackTotalHits::default(),
            max_expansions: DEFAULT_EXPANSIONS,
            max_expansion_examined: DEFAULT_EXPANSION_EXAMINED,
            fields: Vec::new(),
            phrases: Vec::new(),
            highlight: Vec::new(),
            highlight_size: 150,
            with_ids: false,
            translit: false,
            max_verify: DEFAULT_VERIFY,
            max_agg_ms: DEFAULT_AGG_MS,
            guard: None,
            live: None,
            expansion: None,
        }
    }
}

impl QuerySpec {
    /// План отвергнутого сегмента с сохранением corpus-статистики.
    ///
    /// Он нужен сегменту, который глобальный каталог терминов уже отверг.
    /// Для плоского OR все literal-слова доказанно отсутствуют и lookup не
    /// нужен. В boolean-дереве отдельные слова могут встречаться даже при
    /// невозможной конъюнкции: их `df` разрешается обычным путём. `skipped`
    /// запрещает исполнение, но не исключает сегмент из общей BM25-статистики.
    fn resolve_skipped(&self, ix: &Index, expansion: Option<&ExpansionBudget>) -> Request {
        // Невозможность всей конъюнкции не означает df=0 для каждого слова.
        // Сохраняем полную структуру дерева и corpus statistics; skipped
        // запрещает только исполнение. resolve_word использует Bloom, поэтому
        // заведомо отсутствующие literal-слова не требуют lookup словаря.
        if Segments::boolean(self) || !self.filter.is_empty() {
            let mut request = self.resolve_with_expansion(ix, expansion);
            request.skipped = true;
            return request;
        }
        // Пустые local lists нужны не только для плоского OR. После безопасного
        // intersect масок `must` сегмент может быть пропущен ещё до разрешения
        // словаря. Структура всех верхнеуровневых слов при этом обязана
        // сохраниться для общей `df`: иначе skipped-сегмент сдвинет позиции и
        // соседнее слово получит чужой вес.
        let blank = |word: &SpecWord| {
            let field = ix.field_by_id(word.fid as u32).unwrap_or(NO_FIELD);
            let mut resolved = QueryWord::new(field, Vec::new());
            resolved.weight = word.given;
            resolved
        };
        let words = self.words.iter().map(blank).collect();
        let must = self.must.iter().map(blank).collect();
        let must_not = self.must_not.iter().map(blank).collect();
        Request {
            mode: search::MatchMode::AnyTerm,
            skipped: true,
            live: None,
            tree: search::BoolReq::default(),
            words,
            must,
            must_not,
            sort: Vec::new(),
            floor_key: crate::search::KeyFloor::None,
            after: self.after.clone(),
            doc_base: 0,
            budget: None,
            guard: self.guard.clone(),
            score: Vec::new(),
            filter: Vec::new(),
            k: self.k + self.from,
        }
    }

    /// Переводит слова запроса в номера списков этого сегмента.
    pub fn resolve(&self, ix: &Index) -> Request {
        self.resolve_with_expansion(ix, None)
    }

    /// Исторический договор aggregate без query — весь corpus. Это решение
    /// принимается по исходному запросу до dictionary lookup: отсутствующий
    /// literal остаётся MatchNone, а не превращается в MatchAll.
    fn resolve_for_aggregation(&self, ix: &Index) -> Request {
        let mut request = self.resolve(ix);
        if !Segments::boolean(self) && self.words.is_empty() {
            request.mode = search::MatchMode::Boolean;
        }
        request
    }

    fn resolve_with_expansion(&self, ix: &Index, expansion: Option<&ExpansionBudget>) -> Request {
        // Слово кладётся даже с пустым списком, и это несущее свойство, а не
        // небрежность: по позиции в этом векторе слова сопоставляются **между
        // сегментами**, чтобы сложить общую частоту документов. Пропуская
        // отсутствующие, мы сдвинули бы позиции, и слову достался бы чужой вес.
        // Обход пустое слово всё равно отбрасывает — по df = 0.
        let words: Vec<QueryWord> = self
            .words
            .iter()
            .map(|w| {
                resolve_word(
                    ix,
                    w,
                    self.max_expansions,
                    self.translit,
                    self.guard.as_deref(),
                    expansion,
                )
            })
            .collect();
        let resolve_all = |src: &[SpecWord]| -> Vec<QueryWord> {
            src.iter()
                .map(|w| {
                    resolve_word(
                        ix,
                        w,
                        self.max_expansions,
                        self.translit,
                        self.guard.as_deref(),
                        expansion,
                    )
                })
                .collect()
        };
        if !Segments::boolean(self) {
            return Request {
                mode: search::MatchMode::AnyTerm,
                skipped: false,
                live: None,
                tree: search::BoolReq::default(),
                words,
                must: Vec::new(),
                must_not: Vec::new(),
                sort: Vec::new(),
                floor_key: crate::search::KeyFloor::None,
                after: self.after.clone(),
                doc_base: 0,
                budget: None,
                guard: self.guard.clone(),
                score: Vec::new(),
                filter: Vec::new(),
                k: self.k + self.from,
            };
        }
        let tree = match self.nested.as_ref() {
            Some(b) => resolve_bool(
                ix,
                b,
                self.max_expansions,
                self.translit,
                self.guard.as_deref(),
                expansion,
            ),
            None => search::BoolReq {
                // Фразы верхнего уровня стоят рядом с обязательными словами:
                // документ, где фразы нет, не совпал.
                must: self
                    .must
                    .iter()
                    .map(|w| {
                        search::Clause::Word(resolve_word(
                            ix,
                            w,
                            self.max_expansions,
                            self.translit,
                            self.guard.as_deref(),
                            expansion,
                        ))
                    })
                    .chain(self.phrases.iter().map(|p| {
                        search::Clause::Phrase(resolve_phrase(ix, p, self.guard.as_deref()))
                    }))
                    .collect(),
                should: self
                    .words
                    .iter()
                    .map(|w| {
                        search::Clause::Word(resolve_word(
                            ix,
                            w,
                            self.max_expansions,
                            self.translit,
                            self.guard.as_deref(),
                            expansion,
                        ))
                    })
                    .collect(),
                must_not: self
                    .must_not
                    .iter()
                    .map(|w| {
                        search::Clause::Word(resolve_word(
                            ix,
                            w,
                            self.max_expansions,
                            self.translit,
                            self.guard.as_deref(),
                            expansion,
                        ))
                    })
                    .collect(),
                min_should: self.min_should,
            },
        };
        Request {
            mode: search::MatchMode::Boolean,
            skipped: false,
            live: None,
            tree,
            words,
            must: resolve_all(&self.must),
            must_not: resolve_all(&self.must_not),
            // Поля нет в сегменте — ключ у всех его документов «отсутствует»,
            // и они уходят в конец при любом направлении.
            sort: self
                .sort
                .iter()
                .map(|s| (ix.field_by_id(s.fid as u32).unwrap_or(NO_FIELD), s.desc))
                .collect(),
            floor_key: crate::search::KeyFloor::None,
            after: self.after.clone(),
            doc_base: 0,
            budget: None,
            guard: self.guard.clone(),
            score: Vec::new(),
            k: self.k + self.from,
            filter: self
                .filter
                .iter()
                // Отбор по полю, которого в сегменте нет, не совпадает ни с
                // чем: `alive` спрашивает колонку и не находит её.
                .map(|r| Range {
                    field: ix.field_by_id(r.fid as u32).unwrap_or(NO_FIELD),
                    from: r.from,
                    to: r.to,
                })
                .collect(),
        }
    }
}

/// С какого числа постингов запрос стоит делить между потоками.
///
/// Единственная мера, по которой это решается. Прежде рядом стоял второй порог —
/// «делить независимо от постингов, если сегментов больше, чем потоков», — и он
/// был перевёрнут: чем больше сегментов, тем больше потоков поднимается на тот же
/// запрос, и тем заметнее их цена. Замерено на двухстах сегментах: редкое слово
/// в потоках выходило **медленнее** последовательного, а порог по числу
/// сегментов ровно этот случай и пропускал.
const PARALLEL_POSTINGS: u64 = 50_000;

/// Найденный документ в общем пространстве номеров.
pub struct Hit {
    /// Номер в общем пространстве: номер внутри сегмента плюс его база.
    ///
    /// Восемь байт, а не четыре: четырёх хватало на 4.29 млрд документов в
    /// индексе, и это был потолок самого индекса. Разбор — у [`MAX_DOCS`].
    pub doc: u64,
    pub score: f64,
    /// Ключи сортировки — их же принимает курсор страницы.
    pub keys: search::Keys,
    pub nkeys: u8,
    /// Сегмент и номер внутри него — по ним достаётся идентификатор.
    pub segment: usize,
    pub local: u32,
}

/// Считает по метаданным вёдер, не читая документов. `false` — не вышло.
///
/// ## Что тут происходит
///
/// У внутреннего ведра колонки в заголовке уже записано, сколько в нём
/// документов. Ведро, целиком лежащее внутри условия отбора, целиком в него и
/// попадает: читать его документы незачем, их вклад известен числом. Работа
/// становится `O(вёдер)` вместо `O(документов)` — на десяти миллионах с отбором
/// на десятую часть это тысяча заголовков вместо миллиона чтений колонки.
///
/// Краевые вёдра — их не больше двух — читаются по документам, как раньше.
///
/// ## Почему условий так много
///
/// Каждое закрывает случай, в котором число из заголовка перестало бы означать
/// то, что нужно:
///
/// * **Ни одного слова.** Иначе кандидатов задаёт список постингов, а ведро о
///   нём ничего не знает.
/// * **Ровно одно условие отбора, и по тому же полю.** Второе условие отсеяло
///   бы часть документов ведра, а какую — по заголовку не видно.
/// * **Ни одного мёртвого документа.** Мёртвые лежат в ведре наравне с живыми:
///   ведро строится при сборке сегмента и о позднейших удалениях не знает.
/// * **Наблюдение — документ.** У массива наблюдений столько, сколько
///   элементов, и число документов ведра тут не при чём.
/// * **У поля есть вёдра.** Без них и заголовков нет.
///
/// Ни одно из них не «на всякий случай»: нарушь любое — и ответ разойдётся с
/// обходом молча, потому что оба выглядят одинаково правдоподобно.
/// Может ли счёт ответить по заголовкам вёдер, не открывая документы.
///
/// ## Почему отдельной функцией
///
/// Ответ нужен в двух местах: самому [`count_by_buckets`] и планировщику
/// работ, который обязан **не делить** такой сегмент на полосы. Заголовок ведра
/// говорит про весь сегмент, а не про полосу, поэтому делёный сегмент этим
/// путём воспользоваться не может.
///
/// Разъехались бы эти два условия — и планировщик отобрал бы у счёта его
/// быстрый путь молча. Замерено, чем это пахнет: отбор по диапазону со счётом
/// отвечает за 0,006 миллисекунды, а тот же запрос обходом — за 0,59, то есть в
/// сто раз дольше, и ответ при этом верный. Такое расхождение не ловится ничем,
/// кроме замера, поэтому условие живёт в одном месте.
fn buckets_can_answer(
    ix: &Index,
    req: &search::Request,
    aggs: &[crate::aggs::Agg],
    at: &[usize],
) -> bool {
    if !req.words.is_empty()
        || !req.must.is_empty()
        || !req.must_not.is_empty()
        || !req.tree.must.is_empty()
        || !req.tree.should.is_empty()
        || !req.tree.must_not.is_empty()
        || req.filter.len() != 1
        || !req.live.as_ref().map_or_else(|| ix.all_live(), |live| live.all_live())
        || aggs.is_empty()
    {
        return false;
    }
    let r = &req.filter[0];
    if ix.fields.get(r.field).is_some_and(|f| f.multi_column.is_some()) {
        return false;
    }
    let Some(col) = ix.fields.get(r.field).and_then(|f| f.column.as_ref()) else { return false };
    if col.buckets.is_none() {
        return false;
    }
    // Счёт идёт по полю отбора, значит и агрегация обязана смотреть на него же:
    // по чужому полю заголовок ведра не говорит ничего. Сравниваются
    // **местные** позиции: слева переведённая, справа та, что уже перевёл
    // `QuerySpec::resolve`.
    aggs.iter().zip(at.iter()).all(|(agg, &local)| {
        agg.over == crate::aggs::Over::Docs
            && match agg.kind {
                crate::aggs::Kind::Stats { field: None } => true,
                crate::aggs::Kind::Histogram { .. } | crate::aggs::Kind::Range { .. } => {
                    local == r.field
                }
                _ => false,
            }
    })
}

fn count_by_summaries(
    ix: &Index,
    req: &search::Request,
    aggs: &[crate::aggs::Agg],
    at: &[usize],
    sink: &mut crate::aggs::Sink,
    guard: &crate::aggs::Guard,
    tick: &mut u32,
) -> bool {
    if crate::aggs::document_counts_only(aggs) {
        if let Some(count) = search::exact_metadata_count(ix, req) {
            return sink.bulk(aggs, 0, 0, count);
        }
    }
    if !buckets_can_answer(ix, req, aggs, at) {
        return false;
    }
    let r = &req.filter[0];
    let Some(col) = ix.fields.get(r.field).and_then(|f| f.column.as_ref()) else { return false };
    let Some(b) = col.buckets.as_ref() else { return false };
    // Счёт идёт по полю отбора, значит и агрегация обязана смотреть на него же:
    // по чужому полю заголовок ведра не говорит ничего.
    // Сравниваются **местные** позиции: слева переведённая, справа та, что уже
    // перевёл `QuerySpec::resolve`. Прежде слева стоял устойчивый номер поля из
    // самой агрегации — два разных пространства сравнивались как одно, и при
    // случайном совпадении чисел счёт отвечал по чужой колонке, не прочитав ни
    // одного документа.
    let (first, last, _) = b.span_of(r.from, r.to);
    let mut docs: Vec<u32> = Vec::new();
    for k in first..=last {
        let n = b.len_of(k) as u64;
        if n == 0 {
            continue;
        }
        // Спрашивается «что ведро может держать», а не «где оно лежит на
        // сетке»: у крайних вёдер это разные вещи, и здесь край — обещание.
        let (lo, hi) = b.covers(k);
        if lo >= r.from && hi <= r.to && sink.bulk(aggs, lo, hi, n) {
            continue;
        }
        // Краевое ведро или неподходящая агрегация — по документам, как всегда.
        b.docs(k, &mut docs);
        for &doc in docs.iter() {
            // Считается здесь же: счётчик обязан означать «документов
            // прочитано», иначе быстрый путь выглядел бы бесплатным ровно там,
            // где он платит.
            crate::stats::scan_visited(1);
            match col.get(doc) {
                Some(v) if v >= r.from && v <= r.to => {
                    sink.accept_guarded(ix, aggs, at, doc, Some(guard))
                }
                _ => continue,
            }
            if !guard.go(tick) {
                return true;
            }
        }
    }
    true
}

/// Что один поток насчитал: свой сводный приёмник и дробные суммы по сегментам.
///
/// Дробные суммы едут отдельно и с номером сегмента, потому что складывать их
/// вправе только тот, кто видит все, — и только по возрастанию номера.
type Lane = (Option<crate::aggs::Sink>, Vec<(usize, Vec<(f64, f64)>)>);

/// Единица работы счёта: сегмент целиком либо его полоса.
///
/// `range` — `None` у неделёного сегмента. Тогда работа идёт прежним путём и
/// вправе ответить по заголовкам вёдер сразу за весь сегмент; полоса такого
/// права не имеет, потому что заголовок ведра говорит про весь сегмент, а не
/// про её кусок.
struct Job {
    at: usize,
    range: Option<(u32, u32)>,
}

/// Меньше этого числа документов полосу заводить незачем: поток обойдётся
/// дороже работы, которую ему дадут.
const MIN_LANE_DOCS: usize = 32_768;

/// Итог счёта: значения и признак неполноты.
pub struct AggOutcome {
    pub values: Vec<crate::aggs::Result_>,
    /// Обход оборван сторожем: срок вышел или запрос отменён.
    ///
    /// Отдельное поле, а не молчание, — по той же причине, что у
    /// [`Outcome::partial`]: неполная сумма, выданная как полная, это не
    /// «немного не то число», а неверный ответ без признака.
    pub partial: bool,
}

pub struct Outcome {
    pub hits: Vec<Hit>,
    pub visited: u64,
    /// Ответ неполон: предел проверок по тексту исчерпан.
    ///
    /// Отдельное поле, а не молчание. Неполная выдача, выданная как полная, —
    /// худший вид ошибки: по ней принимают решения, и признака у неё нет.
    pub partial: bool,
}

pub struct Segments {
    pub list: Vec<Arc<Index>>,
    /// Смещение номеров документов сегмента в общем пространстве.
    pub base: Vec<u64>,
    /// Ленивый глобальный каталог «слово -> возможные сегменты».
    ///
    /// Каталог принадлежит неизменяемому `Segments`, то есть конкретному
    /// ReadView. Публикация flush/merge создаёт другой объект и тем самым
    /// атомарно меняет поколение каталога вместе с набором сегментов.
    term_directory: Mutex<TermDirectory>,
    /// Уникальная identity immutable ReadView в пределах процесса.
    ///
    /// Указатель `Arc<Segments>` нельзя использовать как поколение кэшей:
    /// allocator вправе переиспользовать адрес после последнего читателя.
    /// Монотонный номер остаётся уникальным даже после такого переиспользования
    /// и не требует записи в disk format.
    generation: u64,
    /// Открытый глобальный ID directory совместимого дискового префикса.
    ///
    /// Cache разделяется между append/live поколениями: они меняют ReadView,
    /// но каталог продолжает адресовать тот же неизменяемый префикс. Неудачное
    /// открытие не кешируется: обслуживание может опубликовать исправленный
    /// каталог без изменения самого состава сегментов.
    id_directory: Arc<IdDirectoryCell>,
}

/// Ready-path не берёт mutex; отдельный строитель допускает отменяемое
/// ожидание без неограниченного OnceLock::get_or_init. Единовременно строится
/// одна решётка, поэтому c32 не выделяет 32 одинаковых memory buffers.
#[derive(Default)]
struct IdDirectoryCell {
    value: OnceLock<crate::maintain::IdDirectory>,
    building: Mutex<bool>,
    wake: std::sync::Condvar,
}

impl IdDirectoryCell {
    fn get(&self) -> Option<&crate::maintain::IdDirectory> {
        self.value.get()
    }

    fn open(
        &self,
        sources: &[&Index],
        guard: Option<&crate::work::Guard>,
    ) -> Option<&crate::maintain::IdDirectory> {
        loop {
            if guard.is_some_and(|g| !g.check()) {
                return None;
            }
            if let Some(value) = self.get() {
                return Some(value);
            }
            let mut building = match self.building.try_lock() {
                Ok(lock) => lock,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    // Чужой builder держит сам state-lock. Не блокируем worker
                    // на нём: паркуем continuation коротким таймером и снова
                    // проверяем guard. Обычный builder отпускает lock на время
                    // тяжёлого открытия и будит нас через condvar.
                    let wait = guard
                        .and_then(crate::work::Guard::remaining)
                        .map_or(std::time::Duration::from_millis(1), |left| {
                            left.min(std::time::Duration::from_millis(1))
                        });
                    if wait.is_zero() {
                        return None;
                    }
                    std::thread::park_timeout(wait);
                    continue;
                }
            };
            if *building {
                // Ожидающий GET хранится как состояние condvar, а не как
                // периодически спящий worker. Таймер остаётся коротким, чтобы
                // отменённый guard не зависел от того, когда builder завершится.
                let wait = guard
                    .and_then(crate::work::Guard::remaining)
                    .map_or(std::time::Duration::from_millis(1), |left| {
                        left.min(std::time::Duration::from_millis(1))
                    });
                if wait.is_zero() {
                    return None;
                }
                let (next, _) =
                    self.wake.wait_timeout(building, wait).unwrap_or_else(|e| e.into_inner());
                building = next;
                continue;
            }
            *building = true;
            drop(building);
            if let Some(value) = self.get() {
                let mut building = self.building.lock().unwrap_or_else(|e| e.into_inner());
                *building = false;
                self.wake.notify_all();
                return Some(value);
            }
            let value = crate::maintain::IdDirectory::open_guarded(sources, guard);
            let cancelled = guard.is_some_and(|g| !g.check());
            let mut building = self.building.lock().unwrap_or_else(|e| e.into_inner());
            *building = false;
            if !cancelled {
                // Ни отсутствие/старое поколение, ни отмена не могут навсегда
                // запретить повторную фоновую подготовку этого же ReadView.
                if let Some(value) = value {
                    let _ = self.value.set(value);
                }
            }
            self.wake.notify_all();
            if cancelled {
                return None;
            }
            return self.get();
        }
    }
}

/// Сколько разных слов одного поколения держать в ленивом каталоге.
///
/// Запись мала (бит на сегмент), но запросный текст недоверенный, поэтому
/// неограниченный `HashMap` превратился бы в управляемую клиентом утечку.
const TERM_DIRECTORY_CAPACITY: usize = 4096;

#[derive(Default)]
struct TermDirectory {
    entries: HashMap<String, Arc<Vec<u64>>>,
    order: VecDeque<String>,
}

/// Заводит общий на запрос счётчик проверок и раздаёт его сегментам.
///
/// Общий, а не посегментный: бюджет принадлежит запросу. Раздай его каждому
/// сегменту порознь — пятьдесят шардов потратили бы пятьдесят бюджетов, и
/// объявленный предел перестал бы что-либо ограничивать.
fn share_budget(spec: &QuerySpec, reqs: &mut [search::Request]) -> Option<Arc<search::Budget>> {
    if spec.max_verify == 0 {
        return None;
    }
    let b = Arc::new(search::Budget::new(spec.max_verify as u64));
    for r in reqs.iter_mut() {
        r.budget = Some(Arc::clone(&b));
    }
    Some(b)
}

/// Кончился ли бюджет за этот запрос.
fn capped(b: &Option<Arc<search::Budget>>) -> bool {
    b.as_ref().is_some_and(|b| b.capped())
}

/// Остановлен ли запрос общим сроком или явной отменой клиента.
fn interrupted(spec: &QuerySpec) -> bool {
    spec.guard.as_ref().is_some_and(|g| g.fired())
}

/// Разница схем словами, годными для человека.
fn same_schema(a: &Index, b: &Index) -> Result<(), String> {
    // Сверка идёт по **устойчивому номеру**, а не по позиции.
    for f in a.fields.iter() {
        let Some(j) = b.field_by_id(f.fid) else { continue };
        let g = &b.fields[j];
        // Группа сравнивается наравне с типом, и это не педантизм: колонка
        // группового поля адресуется ординалом элемента, а корневого — номером
        // документа. Сегмент, у которого поле переехало в другую группу,
        // отвечал бы на тот же запрос значениями из чужих ячеек — и открывался
        // бы рядом с правильным молча.
        if f.name != g.name
            || f.kind != g.kind
            || f.stored != g.stored
            || f.positioned != g.positioned
            || f.searched != g.searched
            || f.group != g.group
        {
            let shape = |x: &crate::index::Field| {
                format!(
                    "{}:{}{}{}{}{}",
                    x.name,
                    x.kind.spec(),
                    if x.stored { "" } else { ":nostore" },
                    if x.searched { "" } else { ":noindex" },
                    if x.positioned { "" } else { ":nopos" },
                    if x.group == 0 { String::new() } else { format!(":группа{}", x.group) }
                )
            };
            return Err(format!("поле {}: {} против {}", f.fid, shape(f), shape(g)));
        }
    }
    // Пути групп — тоже часть схемы. Один и тот же номер, означающий в двух
    // сегментах разные массивы, — это два разных индекса в одном каталоге.
    for (g, (pa, pb)) in a.group_paths.iter().zip(b.group_paths.iter()).enumerate() {
        if pa != pb && !pa.is_empty() && !pb.is_empty() {
            return Err(format!("группа {g}: путь {pa:?} против {pb:?}"));
        }
    }
    Ok(())
}

/// Сколько областей отображения занимает один сегмент, если это уже замерено.
///
/// Ноль — ещё не замерено. Замер делается один раз на процесс, и вот почему.
///
/// Разность [`crate::mmap::live_maps`] верна только тогда, когда между двумя её
/// чтениями никто другой отображений не заводит. На старте узла это так: набор
/// открывается до того, как заведены пересмотр каталога и писатель. А `load`
/// зовётся и потом — например посреди переигрывания журнала, когда сброс завёл
/// новый сегмент, — и там рядом уже работает пересмотр. Замерь мы снова, чужая
/// область попала бы в наш `per_seg` и раздула бы потребность на пустом месте,
/// вплоть до ложного отказа.
///
/// Переизмерять и не нужно: [`Segments::load`] сам требует одинаковых схем у
/// всех сегментов, а число файлов сегмента определяется схемой.
static MAPS_PER_SEGMENT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Следующий номер immutable ReadView. Перезапуск очищает process-local cache,
/// поэтому сохранять его в manifest не нужно.
static NEXT_SEGMENTS_GENERATION: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Сказано ли уже про нехватку областей.
///
/// Предупреждение — про устройство узла, а не про конкретное открытие, и второй
/// раз оно ничего не добавляет. А `load` зовётся на каждом сбросе, случившемся
/// посреди переигрывания журнала: без этого признака узел с длинным журналом
/// залил бы одним и тем же абзацем весь вывод старта.
static SHORTFALL_TOLD: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Запоминает первый непустой замер и отдаёт запомненное всем последующим.
///
/// Нулевой замер не запоминается и за истину не выдаётся: он означает, что у
/// сегмента не оказалось ни одного непустого файла, то есть мерить было нечего.
/// Тогда в ход идёт пол — заниженная, но честная оценка; запомнить ноль значило
/// бы навсегда объявить, что сегменты областей не занимают.
///
/// Ячейка передаётся, а не берётся из статики, ровно затем, чтобы правило
/// проверялось само по себе: статика одна на процесс, и тест, зависящий от
/// того, кто её тронул первым, проверял бы порядок запуска, а не правило.
fn remember_per_segment(cell: &std::sync::atomic::AtomicUsize, measured: usize) -> usize {
    let known = cell.load(std::sync::atomic::Ordering::Relaxed);
    if known != 0 {
        return known;
    }
    if measured == 0 {
        return crate::mmap::MIN_MAPS_PER_SEGMENT;
    }
    let per = measured.max(crate::mmap::MIN_MAPS_PER_SEGMENT);
    cell.store(per, std::sync::atomic::Ordering::Relaxed);
    per
}

/// Чем отвечать на посчитанную потребность в областях отображения.
///
/// `Err` — отказ, `Ok(Some(текст))` — предупреждение, `Ok(None)` — молчать,
/// потому что всё помещается.
///
/// ## Зачем проверять заранее
///
/// У процесса есть потолок на **число** областей — на Linux это
/// `vm.max_map_count`, по умолчанию 65530. Сегмент занимает области, значит
/// потолок — это стена по числу сегментов, а не по памяти. За ней индекс просто
/// перестаёт открываться, и узнают об этом отказом `mmap` посреди открытия: узел
/// работал минуты, прочитал половину каталогов и умер с «cannot allocate memory»
/// при вдоволь свободной памяти.
///
/// Где стоит стена, зависит от того, упакован сегмент или рассыпан, и разница
/// тут в порядок. Замерено (счётчик [`crate::mmap::live_maps`] до и после
/// `Segments::load`, три сегмента): упакованный стоит **двух** областей —
/// `seg.dat` и `live.bin`, — и не зависит ни от схемы, ни от того, был запрос
/// или нет. Рассыпанный, собранный до контейнера, стоит семнадцати при двух
/// текстовых полях и тридцати при пяти полях с вектором: файл на кусок. Отсюда
/// и стена: у упакованного индекса она примерно там же, где у рассыпанного была
/// бы при вдесятеро меньшем числе сегментов.
///
/// Здесь то же самое известно **до** открытия и сказано числами, которые можно
/// выполнить. Так же поступает Elasticsearch: он на этом месте требует не меньше
/// 262 144 и в боевом режиме не стартует вовсе.
///
/// ## Почему не всегда отказ
///
/// Решает не ядро, а тот, кто индекс открывает: правило приходит извне, через
/// [`crate::mmap::set_map_shortfall`]. Инструментам (`svindb merge`,
/// `svindb check`) отказывать нельзя — слияние и есть тот рычаг, которым число
/// сегментов уменьшают, и отказ отнял бы у упёршегося индекса единственную
/// починку.
///
/// ## Почему оценка по полу не предупреждает
///
/// Пол — две области на сегмент, и это ровно столько, сколько стоит
/// **упакованный** сегмент: для него оценка по полу не занижена вовсе.
/// А вот у рассыпанного, собранного до контейнера, областей замерено
/// семнадцать-тридцать, и совет «поднимите предел до N», посчитанный по полу,
/// вышел бы для него на порядок меньше нужного — то есть хуже молчания.
///
/// Какой из двух видов сегментов в составе, здесь ещё неизвестно: пол считается
/// **до** открытия первого каталога, а признак контейнера лежит в `head.tsv`
/// каждого сегмента, и набор бывает смешанным. Поэтому решение осталось
/// прежним: отказать по полу можно (если не лезет даже он, не полезет ничто), а
/// предупреждать нечем — точный замер придёт следом, после первого сегмента, и
/// назовёт верное число для того вида, который в составе и правда лежит.
fn map_verdict(
    budget: &crate::mmap::MapBudget,
    rule: crate::mmap::MapShortfall,
) -> std::io::Result<Option<String>> {
    // Пик — отказ **всегда**, петля там или не петля, и раньше всего прочего.
    //
    // Первая проверка мягкая нарочно: она смотрит на потребность нового состава
    // без своих уже стоящих областей, иначе переоткрытие посреди переигрывания
    // журнала объявляло бы, что индексу нужно вдвое, и валило узел, который
    // минуту назад поднялся на этом же пределе.
    //
    // Но мягкости мало. У упакованного сегмента обе области открываются вслух:
    // `seg.dat` — через `SegmentSource::of`, `live.bin` — через `Live::open`, и
    // отказ любой из них выходит из `index::load` ошибкой. А у рассыпанного
    // сегмента кусок — это отдельный файл, и `try_open` для него сводится к
    // `open(...).ok()`: три десятка мест (колонка, фильтр, хранилище поля,
    // словарь, отпечаток идентификаторов) превращают неоткрывшийся файл в
    // «этого поля нет». Значит у рассыпанного набора отказ по пределу областей
    // не громкий, а молчаливый, и неполная выдача после него неотличима от
    // честной. Против этого и стоит второй порог: пик сверяется с **жёстким**
    // пределом ядра, а не с рабочим потолком, потому что за жёстким `mmap`
    // откажет прямо на этом открытии.
    if budget.peaks_over() {
        return Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, budget.explain()));
    }
    if budget.fits() {
        return Ok(None);
    }
    if rule == crate::mmap::MapShortfall::Refuse {
        return Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, budget.explain()));
    }
    if !budget.exact {
        return Ok(None);
    }
    Ok(Some(budget.explain()))
}

#[cfg(test)]
mod peak_tests {
    use crate::mmap::{map_budget_with, MapShortfall};

    /// Пик отказывает даже там, где мягкая проверка молчит.
    ///
    /// Мягкая смотрит на новый состав без уже стоящих своих областей — иначе
    /// переоткрытие валило бы работающий узел. Но её одной мало: у рассыпанного
    /// сегмента кусок открывается своим файлом, и три десятка мест глотают
    /// отказ через `try_open`, то есть через `open(...).ok()`. Нехватка областей
    /// у них выглядит как «поля нет». Неполная выдача, неотличимая от честной, —
    /// худшее, чем может кончиться нехватка.
    #[test]
    fn the_peak_refuses_where_the_soft_check_stays_silent() {
        // Предел 1000, рабочий потолок 900. Новому составу нужно 800 — мягкая
        // проверка довольна. Но своих уже стоит 500, и ядру предъявят 1300.
        let b = map_budget_with(400, 2, Some(0), 1000, 500);
        assert!(b.fits(), "мягкая проверка обязана быть довольна: {} <= {}", b.need, b.budget);
        assert!(b.peaks_over(), "пик {} обязан перевалить за предел {}", b.peak(), b.limit);
        // И отказ приходит даже на петле, где нехватка иначе была бы заметкой.
        let v = super::map_verdict(&b, MapShortfall::Warn);
        assert!(v.is_err(), "пик обязан отказывать и в мягком режиме");

        // А там, где пик умещается, мягкий режим остаётся мягким.
        let ok = map_budget_with(400, 2, Some(0), 4000, 500);
        assert!(!ok.peaks_over());
        assert!(super::map_verdict(&ok, MapShortfall::Warn).is_ok());
    }

    /// Пик считается от своих областей, а не от базы: их складывают, не путают.
    #[test]
    fn the_peak_counts_our_own_regions_on_top_of_the_need() {
        let b = map_budget_with(100, 20, Some(300), 65_530, 1_700);
        assert_eq!(b.need, 300 + 2_000);
        assert_eq!(b.peak(), 300 + 2_000 + 1_700);
    }
}

/// Хватит ли процессу областей отображения на весь индекс.
fn map_gate(budget: crate::mmap::MapBudget) -> std::io::Result<()> {
    if let Some(msg) = map_verdict(&budget, crate::mmap::map_shortfall())? {
        if !SHORTFALL_TOLD.swap(true, std::sync::atomic::Ordering::Relaxed) {
            eprintln!("внимание: {msg}");
        }
    }
    Ok(())
}

impl Segments {
    /// Собирает неизменяемое поколение из уже открытых сегментов.
    pub fn from_parts(list: Vec<Arc<Index>>, base: Vec<u64>) -> Segments {
        Segments {
            list,
            base,
            term_directory: Mutex::new(TermDirectory::default()),
            generation: NEXT_SEGMENTS_GENERATION
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .wrapping_add(1),
            id_directory: Arc::new(IdDirectoryCell::default()),
        }
    }

    /// Собирает следующее поколение, сохраняя ID cache только для append/live
    /// перехода. Сравниваются дисковые сегменты: memory-сегмент не входит в
    /// `catalog.bin` и может свободно появляться или исчезать в хвосте.
    fn from_parts_reusing(list: Vec<Arc<Index>>, base: Vec<u64>, previous: &Segments) -> Segments {
        let previous_disk: Vec<(u64, i64)> = previous
            .list
            .iter()
            .filter(|index| !index.src.is_memory())
            .map(|index| index.head_id)
            .collect();
        let next_disk: Vec<(u64, i64)> =
            list.iter().filter(|index| !index.src.is_memory()).map(|index| index.head_id).collect();
        let append_compatible = next_disk.starts_with(&previous_disk);
        let sources: Vec<&Index> = list.iter().map(Arc::as_ref).collect();
        let reusable = match previous.id_directory.get() {
            // Пока directory не открыт, безопасно разделять будущую
            // инициализацию только с append-составом: неизвестно, какой
            // префикс покроет файл к моменту первого point read.
            None => append_compatible,
            // Открытый directory сам знает точный покрытый префикс. Замена
            // L0-хвоста compaction не обязана начинаться со всего прежнего
            // списка сегментов и не должна инвалидировать этот префикс.
            Some(directory) => directory.reusable(&sources),
        };
        Segments {
            list,
            base,
            term_directory: Mutex::new(TermDirectory::default()),
            generation: NEXT_SEGMENTS_GENERATION
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .wrapping_add(1),
            id_directory: if reusable {
                Arc::clone(&previous.id_directory)
            } else {
                Arc::new(IdDirectoryCell::default())
            },
        }
    }

    /// Уникальная identity immutable состава для generation-aware кэшей.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Generation-aware места идентификаторов с одним открытием глобального
    /// каталога на immutable ReadView.
    pub fn locate_ids(&self, ids: &[String]) -> Option<Vec<Vec<(usize, u32)>>> {
        self.locate_ids_guarded(ids, None)
    }

    /// Пытается подготовить ID directory вне request path.
    ///
    /// Построение resident grid может читать холодный `catalog.bin`, поэтому
    /// его нельзя запускать лениво из GET. Метод вызывается фоновым rescan
    /// потоком после публикации состава; повторная подготовка уже готового
    /// поколения ничего не делает и не переоткрывает mmap.
    pub fn prepare_id_directory(&self) -> bool {
        self.prepare_id_directory_guarded(None)
    }

    /// Фоновая подготовка с отменой при остановке runtime индекса.
    pub fn prepare_id_directory_guarded(&self, guard: Option<&crate::work::Guard>) -> bool {
        if guard.is_some_and(|g| !g.check()) {
            return false;
        }
        if self.id_directory.get().is_some() {
            return true;
        }
        let sources: Vec<&Index> = self.list.iter().map(Arc::as_ref).collect();
        self.id_directory.open(&sources, guard).is_some()
    }

    /// Готов ли быстрый point-read lookup для этого immutable ReadView.
    pub fn id_directory_ready(&self) -> bool {
        self.id_directory.get().is_some()
    }

    /// Отменяемая версия [`Segments::locate_ids`].
    ///
    /// Сигнал отмены передаётся дальше в ID-directory, чтобы закрытый клиент
    /// не запускал после этого полный fallback по всем сегментам.
    pub fn locate_ids_guarded(
        &self,
        ids: &[String],
        guard: Option<&crate::work::Guard>,
    ) -> Option<Vec<Vec<(usize, u32)>>> {
        if guard.is_some_and(|g| !g.check()) {
            return None;
        }
        let sources: Vec<&Index> = self.list.iter().map(Arc::as_ref).collect();
        let directory = self.id_directory.open(&sources, guard)?;
        self.locate_in_directory(directory, &sources, ids, guard)
    }

    /// HTTP point-read использует только готовый каталог: не строит grid и
    /// не ждёт builder. Временная неготовность означает bounded retryable отказ.
    pub fn locate_ids_ready_guarded(
        &self,
        ids: &[String],
        guard: Option<&crate::work::Guard>,
    ) -> Option<Vec<Vec<(usize, u32)>>> {
        if guard.is_some_and(|g| !g.check()) {
            return None;
        }
        let directory = self.id_directory.get()?;
        let sources: Vec<&Index> = self.list.iter().map(Arc::as_ref).collect();
        self.locate_in_directory(directory, &sources, ids, guard)
    }

    fn locate_in_directory(
        &self,
        directory: &crate::maintain::IdDirectory,
        sources: &[&Index],
        ids: &[String],
        guard: Option<&crate::work::Guard>,
    ) -> Option<Vec<Vec<(usize, u32)>>> {
        // Старый запрос может удерживать короткое поколение, пока более новый
        // запрос первым инициализировал общий cache по длинному append-составу.
        // В таком случае точный fallback безопаснее обращения за границу
        // старого списка сегментов.
        if !directory.compatible(sources) {
            return None;
        }
        let mut hashes = Vec::with_capacity(ids.len());
        for id in ids {
            if guard.is_some_and(|g| !g.check()) {
                return None;
            }
            hashes.push(crate::bloom::hash(id));
        }
        directory.locate_guarded(sources, ids, &hashes, guard)
    }

    /// Возможные сегменты для одного буквального слова.
    ///
    /// Бит взведён и при ложном срабатывании фильтра, и когда фильтра у старого
    /// сегмента нет. Поэтому каталог способен только оставить лишнюю работу,
    /// но никогда не выбросить настоящий результат.
    fn term_segments(
        &self,
        term: &str,
        guard: Option<&crate::work::Guard>,
    ) -> Option<Arc<Vec<u64>>> {
        if let Some(found) = self
            .term_directory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(term)
            .cloned()
        {
            return Some(found);
        }

        let mut bits = vec![0u64; self.list.len().div_ceil(64)];
        for (i, ix) in self.list.iter().enumerate() {
            if i & 63 == 0 && guard.is_some_and(|g| !g.check()) {
                // Неполный отрицательный каталог кэшировать нельзя: нули в
                // непройденном хвосте выглядели бы как точное отсутствие.
                return None;
            }
            let possible = ix.term_filter.as_ref().is_none_or(|filter| filter.maybe_has_term(term));
            if possible {
                bits[i >> 6] |= 1u64 << (i & 63);
            }
        }
        let bits = Arc::new(bits);
        let mut directory =
            self.term_directory.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(found) = directory.entries.get(term) {
            return Some(Arc::clone(found));
        }
        while directory.entries.len() >= TERM_DIRECTORY_CAPACITY {
            let Some(oldest) = directory.order.pop_front() else { break };
            directory.entries.remove(&oldest);
        }
        directory.order.push_back(term.to_string());
        directory.entries.insert(term.to_string(), Arc::clone(&bits));
        Some(bits)
    }

    /// Объединённый набор сегментов для обычного запроса по набору полей.
    ///
    /// Literal-альтернативы объединяются, обязательные ветви пересекаются;
    /// boolean-tree учитывает minimum_should_match. Неизвестные phrase/slot,
    /// prefix и transliteration сохраняют все возможные сегменты. Отрицание
    /// не отбрасывает сегмент по присутствию запрещённого слова.
    fn term_candidates(&self, spec: &QuerySpec) -> Option<Vec<u64>> {
        let mut candidates = self.literal_candidates(spec);
        for range in &spec.filter {
            if spec.guard.as_ref().is_some_and(|guard| !guard.check()) {
                return None;
            }
            if range.from != range.to {
                continue;
            }
            let Ok(fid) = u32::try_from(range.fid) else {
                continue;
            };
            // Layout 12 гарантирует numeric exact postings, включая arrays.
            // Неизвестный/legacy профиль остаётся на полном пути.
            let supported = |index: &Index| {
                index.layout >= 12
                    && index
                        .field_by_id(fid)
                        .and_then(|field| index.fields.get(field))
                        .is_some_and(|field| field.kind.numeric() && field.searched)
            };
            if !self.list.iter().any(|index| supported(index)) {
                continue;
            }
            let possible = self.term_segments(&range.from.to_string(), spec.guard.as_deref())?;
            let mask =
                candidates.get_or_insert_with(|| vec![u64::MAX; self.list.len().div_ceil(64)]);
            for (at, index) in self.list.iter().enumerate() {
                if at & 63 == 0 && spec.guard.as_ref().is_some_and(|guard| !guard.check()) {
                    return None;
                }
                if supported(index) && !Self::candidate(Some(&possible), at) {
                    mask[at >> 6] &= !(1u64 << (at & 63));
                }
            }
        }
        candidates
    }

    fn literal_candidates(&self, spec: &QuerySpec) -> Option<Vec<u64>> {
        // Фразы верхнего уровня пока не участвуют в доказательстве.
        if !spec.phrases.is_empty() {
            return None;
        }
        if spec.nested.is_none() && spec.words.is_empty() && spec.must.is_empty() {
            return None;
        }
        let words = self.list.len().div_ceil(64);

        // Один SpecWord может содержать несколько альтернатив (маска,
        // транслитерация, нормализация). Сегмент подходит, если в нём есть
        // хотя бы одна альтернатива; prefix без исходного literal остаётся
        // консервативным и выключает маску целиком.
        let word_mask = |word: &SpecWord| -> Option<Vec<u64>> {
            if spec.translit && word.given.is_none() {
                return None;
            }
            let mut unique: HashSet<&str> = HashSet::new();
            for term in word.terms.iter() {
                if term.prefix && word.given.is_none() {
                    return None;
                }
                unique.insert(term.text.as_str());
            }
            if unique.is_empty() {
                return Some(vec![0; words]);
            }
            let mut mask = vec![0u64; words];
            for term in unique {
                let possible = self.term_segments(term, spec.guard.as_deref())?;
                for (out, &bits) in mask.iter_mut().zip(possible.iter()) {
                    *out |= bits;
                }
            }
            Some(mask)
        };

        // Маска означает только возможность совпадения в сегменте.
        // Неизвестный лист сохраняет все биты; отрицательные листья не
        // отсекают сегмент по наличию слова в каком-то другом документе.
        fn boolean_mask(
            branch: &SpecBool,
            width: usize,
            word_mask: &dyn Fn(&SpecWord) -> Option<Vec<u64>>,
            guard: Option<&crate::work::Guard>,
        ) -> Option<Vec<u64>> {
            let clause_mask = |clause: &SpecClause| match clause {
                SpecClause::Term(word) => word_mask(word),
                SpecClause::Bool(inner) => boolean_mask(inner, width, word_mask, guard),
                SpecClause::Phrase(_) | SpecClause::Slots(_) => None,
            };
            let mut bits = vec![u64::MAX; width];
            for clause in &branch.must {
                if guard.is_some_and(|g| !g.check()) {
                    return None;
                }
                if let Some(mask) = clause_mask(clause) {
                    for (out, part) in bits.iter_mut().zip(mask) {
                        *out &= part;
                    }
                }
            }
            let minimum = if branch.min_should > 0 {
                branch.min_should
            } else {
                usize::from(branch.must.is_empty() && !branch.should.is_empty())
            };
            if minimum > branch.should.len() {
                return Some(vec![0; width]);
            }
            if minimum > 0 {
                let mut masks = Vec::with_capacity(branch.should.len());
                for clause in &branch.should {
                    if guard.is_some_and(|g| !g.check()) {
                        return None;
                    }
                    masks.push(clause_mask(clause));
                }
                for (block, out) in bits.iter_mut().enumerate() {
                    if guard.is_some_and(|g| !g.check()) {
                        return None;
                    }
                    if minimum == 1 {
                        *out &= masks.iter().fold(0, |union, candidate| {
                            union | candidate.as_ref().map_or(u64::MAX, |c| c[block])
                        });
                        continue;
                    }
                    if minimum == masks.len() {
                        *out &= masks.iter().fold(u64::MAX, |intersection, candidate| {
                            intersection & candidate.as_ref().map_or(u64::MAX, |c| c[block])
                        });
                        continue;
                    }
                    let mut accepted = 0u64;
                    for bit in 0..64 {
                        let mask = 1u64 << bit;
                        if *out & mask != 0
                            && masks
                                .iter()
                                .filter(|candidate| {
                                    candidate.as_ref().is_none_or(|c| c[block] & mask != 0)
                                })
                                .take(minimum)
                                .count()
                                >= minimum
                        {
                            accepted |= mask;
                        }
                    }
                    *out &= accepted;
                }
            }
            Some(bits)
        }
        if let Some(branch) = &spec.nested {
            return boolean_mask(branch, words, &word_mask, spec.guard.as_deref());
        }

        // Плоский OR: сегмент подходит, если в нём возможно хотя бы одно
        // необязательное слово. Для `must` сначала ставим все биты и затем
        // пересекаем их: каждый обязательный literal обязателен одновременно.
        // `must_not` здесь намеренно не участвует — наличие запрещённого слова
        // не доказывает отсутствие документа без него.
        let mut candidates =
            if spec.must.is_empty() { vec![0u64; words] } else { vec![u64::MAX; words] };
        if !spec.must.is_empty() {
            for word in spec.must.iter() {
                let mask = word_mask(word)?;
                for (out, bits) in candidates.iter_mut().zip(mask.iter()) {
                    *out &= *bits;
                }
            }
        } else {
            for word in spec.words.iter() {
                let mask = word_mask(word)?;
                for (out, bits) in candidates.iter_mut().zip(mask.iter()) {
                    *out |= *bits;
                }
            }
        }
        // Нулевая маска — полноценное доказательство отсутствия во всех
        // сегментах, а не повод отключить оптимизацию: `None` означает
        // «каталог неприменим» и заставил бы открыть каждый словарь.
        Some(candidates)
    }

    #[inline]
    fn candidate(bits: Option<&[u64]>, segment: usize) -> bool {
        bits.is_none_or(|bits| {
            bits.get(segment >> 6).is_some_and(|word| word & (1u64 << (segment & 63)) != 0)
        })
    }

    /// Верхняя граница документов в возможных сегментах для admission.
    ///
    /// Читаются только term filters и размеры сегментов. Это не оценка числа
    /// совпадений: корреляции фильтров и слов не предполагаются. После ceiling
    /// дальнейшее уточнение не нужно; None означает отменённую разведку.
    pub fn candidate_document_bound(&self, spec: &QuerySpec, ceiling: u64) -> Option<u64> {
        let masks = self.term_candidates(spec);
        let mut documents = 0u64;
        for (i, segment) in self.list.iter().enumerate() {
            if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                return None;
            }
            if Self::candidate(masks.as_deref(), i) && Self::range_can_match(segment, spec) {
                documents = documents.saturating_add(segment.docs as u64);
                if documents > ceiling {
                    return Some(documents);
                }
            }
        }
        Some(documents)
    }

    /// Содержимое поля по **общему** номеру документа.
    ///
    /// Общий номер — это номер внутри сегмента плюс смещение сегмента; ровно в
    /// таком виде он и уходит в выдачу. Разбирать его обратно обязаны мы, а не
    /// клиент: смещения сегментов меняются при укрупнении, и знать про них
    /// снаружи нельзя.
    pub fn fetch(&self, doc: u64, field: usize) -> Option<Vec<u8>> {
        let i = self.base.partition_point(|&b| b <= doc).checked_sub(1)?;
        self.list[i].fetch((doc - self.base[i]) as u32, field)
    }

    /// Отдача содержимого с кешом кусков **на один запрос**.
    pub fn fetcher(&self, limit: usize) -> Fetcher<'_> {
        Fetcher {
            segs: self,
            held: Vec::new(),
            bytes: 0,
            limit,
            misses: 0,
            spare: Vec::new(),
            ids: Vec::new(),
        }
    }

    /// Ближайшие `k` документов по плотному вектору, по всем сегментам.
    ///
    /// Возвращает **общие** номера и квадрат расстояния. Квадрат, а не корень:
    /// порядок тот же, а корень — лишний вызов на каждый документ. Наружу он
    /// превращается в вес там, где ответ собирается.
    ///
    /// ## Почему сегменты обходятся подряд, а не сливаются кучей
    ///
    /// Верхушка у каждого своя, на `k` мест, и в конце они сходятся сортировкой
    /// `k·сегментов` записей. Общая куча с порогом дала бы отсечку получше, но
    /// потребовала бы синхронизации между сегментами — а сегментов обычно
    /// единицы, и выигрыш утонул бы в ней.
    pub fn knn(&self, field: usize, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        self.knn_with(field, query, k, 1)
    }

    /// То же, в несколько потоков.
    ///
    /// ## Почему это делится, а обход графа — нет
    ///
    /// Работа здесь — сплошной проход по отрезку номеров, и делится он делением
    /// отрезка. Ни порога между кусками, ни общего состояния, ни порядка:
    /// каждый поток набирает свою верхушку на `k` мест, в конце они сходятся
    /// сортировкой `k·потоков` записей. Обход графа так поделить нельзя —
    /// соседство ведёт куда угодно, и потоки принялись бы за одну и ту же
    /// область.
    ///
    /// Делится **весь запрос целиком**, а не по сегментам: сегмент бывает один,
    /// и деление по сегментам оставило бы девять ядер без работы.
    ///
    /// Всё, что сверх насыщения, покупает нули, а отнимает ядра у соседних
    /// запросов, где они дают настоящий выигрыш: восемь одновременных запросов
    /// по одному потоку дают 330 запросов в секунду против 120 у одного.
    pub fn knn_with(
        &self,
        field: usize,
        query: &[f32],
        k: usize,
        threads: usize,
    ) -> Vec<(u64, f32)> {
        self.knn_approx_guarded(field, query, k, threads, false, 0, None)
    }

    /// Точный векторный поиск с общим deadline/cancellation guard.
    pub fn knn_with_guard(
        &self,
        field: usize,
        query: &[f32],
        k: usize,
        threads: usize,
        guard: &crate::work::Guard,
    ) -> Vec<(u64, f32)> {
        self.knn_approx_guarded(field, query, k, threads, false, 0, Some(guard))
    }

    /// То же с приближением по спискам.
    ///
    /// ## Что означает каждый довод
    ///
    /// * `exact` — просить точный перебор. Тогда списки не спрашиваются вовсе,
    ///   и ответ в точности тот же, что был до всякого приближения.
    /// * `nprobe` — сколько списков смотреть. Ноль означает умолчание, взятое
    ///   из замера полноты (см. [`crate::vector::default_nprobe`]).
    ///
    /// ## Почему смешивать сегменты с разбиением и без него безопасно
    ///
    /// Каждый сегмент отдаёт **настоящие** расстояния до своих документов —
    /// разбиение решает лишь, сколько документов он посмотрел. Верхушки
    /// сходятся сравнением расстояний, и сегмент без разбиения просто отдаёт
    /// свою честно и целиком. Ни пересчёта, ни поправок это не требует.
    ///
    /// Так же выглядит и постепенный переход: старые сегменты живут без
    /// разбиения, новые с ним, и никакой отдельной миграции не нужно.
    pub fn knn_approx(
        &self,
        field: usize,
        query: &[f32],
        k: usize,
        threads: usize,
        exact: bool,
        nprobe: usize,
    ) -> Vec<(u64, f32)> {
        self.knn_approx_guarded(field, query, k, threads, exact, nprobe, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn knn_approx_guarded(
        &self,
        field: usize,
        query: &[f32],
        k: usize,
        threads: usize,
        exact: bool,
        nprobe: usize,
        guard: Option<&crate::work::Guard>,
    ) -> Vec<(u64, f32)> {
        if k == 0 {
            return Vec::new();
        }
        // Куски по всем сегментам разом: сегмент на миллион и сегмент на тысячу
        // не должны получить по потоку каждый.
        let mut parts: Vec<(usize, u32, u32)> = Vec::new();
        for (i, ix) in self.list.iter().enumerate() {
            if guard.is_some_and(|g| !g.check()) {
                break;
            }
            let Some(vs) = ix.fields.get(field).and_then(|f| f.vectors.as_ref()) else {
                continue;
            };
            let docs = vs.docs() as u32;
            const MIN_CHUNK_BYTES: u32 = 1 << 20;
            let per_doc = vs.dims().max(1) as u32;
            let cap = threads.min(crate::vector::split_for(threads)).max(1);
            let want = (cap as u32).min(docs / (MIN_CHUNK_BYTES / per_doc).max(1) + 1);
            let step = docs.div_ceil(want.max(1));
            let mut at = 0u32;
            while at < docs {
                parts.push((i, at, (at + step).min(docs)));
                at += step;
            }
        }
        // Списки выбираются **один раз на сегмент**, а не на кусок: они от
        // деления на потоки не зависят, а стоят прохода по центроидам.
        let probes: Vec<Vec<u16>> = self
            .list
            .iter()
            .map(|ix| {
                if guard.is_some_and(|g| !g.check()) {
                    return Vec::new();
                }
                if exact {
                    return Vec::new();
                }
                let Some(vs) = ix.fields.get(field).and_then(|f| f.vectors.as_ref()) else {
                    return Vec::new();
                };
                let want = if nprobe == 0 {
                    crate::vector::default_nprobe(vs.ncentroids())
                } else {
                    nprobe
                };
                vs.probes(query, want)
            })
            .collect();
        let each = |&(i, from, to): &(usize, u32, u32)| -> Vec<(u64, f32)> {
            let ix = &self.list[i];
            let vs = ix.fields[field].vectors.as_ref().expect("куски есть только у поля с кодами");
            let base = self.base[i];
            vs.top_probed_guarded(query, k, &|d| ix.is_live(d), from, to, &probes[i], guard)
                .into_iter()
                .map(|(d, s)| (d as u64 + base, s))
                .collect()
        };
        let scanned: u64 = parts.iter().map(|p| u64::from(p.2 - p.1)).sum();
        let started = std::time::Instant::now();
        // Тот же постоянный backend, что и у lexical search. Временный
        // scoped-пул здесь был скрытой второй шириной: каждый vector-запрос
        // поднимал свои threads поверх уже выданного governor permit.
        let mut out: Vec<(u64, f32)> = if parts.is_empty() {
            Vec::new()
        } else {
            let parts_ref = &parts;
            crate::executor::map(parts.len(), |at| each(&parts_ref[at]))
                .into_iter()
                .flatten()
                .collect()
        };
        // Настройка деления учится на настоящих запросах — здесь она узнаёт,
        // чем кончился этот. Время берётся до сведения верхушек: сводится
        // `k·кусков` записей, и к скорости перебора это отношения не имеет.
        crate::vector::record(parts.len(), scanned, started.elapsed().as_secs_f64());
        // Ничьи разрешаются меньшим общим номером — как и в булевой выдаче.
        out.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        out.truncate(k);
        out
    }

    /// Номер поля по имени. Поля одинаковы во всех сегментах по построению.
    pub fn field_of(&self, name: &str) -> Option<usize> {
        self.list.first()?.field_of(name)
    }
    /// Состав индекса: список сегментов, если он есть, иначе обход каталога.
    pub fn ready(dir: &str) -> Vec<String> {
        crate::manifest::segments(dir)
    }

    /// Загружает индекс по списку сегментов.
    pub fn load(dir: &str) -> std::io::Result<Segments> {
        let mut dirs = crate::manifest::segments_checked(dir).map_err(std::io::Error::other)?;
        if dirs.is_empty() {
            // Каталог сам может быть сегментом — так его открывают стенды.
            if std::path::Path::new(&format!("{dir}/head.tsv")).exists() {
                dirs.push(dir.to_string());
            } else {
                // Ни сегментов, ни заголовка: индекс пуст, и это не ошибка.
                return Ok(Segments::from_parts(Vec::new(), Vec::new()));
            }
        }

        // Потолок отображений — двумя ступенями, и обе нужны.
        //
        // Предел знает только ядро Linux; там, где его неоткуда взять, проверки
        // нет вовсе, и выдумывать число нельзя — сказать об этом обязан тот, кто
        // индекс открывает.
        let limit = crate::mmap::max_maps();
        // База — расход **вне** индекса, и оба слова тут по делу.
        //
        // Читается один раз, **до открытия**: прочитай после первого сегмента — и
        // области этого сегмента вошли бы и в базу, и в `сегментов × per_seg`, то
        // есть посчитались бы дважды.
        //
        // И вычитается своё: `/proc/self/maps` считает все области процесса,
        // включая наши, а второе слагаемое — это как раз те, что откроются
        // сейчас. На первом открытии своих нет и разницы никакой; а на повторном
        // (сброс посреди переигрывания журнала) прежний набор ещё жив, и голая
        // база сложила бы два поколения одного индекса — то есть отказала бы
        // узлу, поднявшемуся на этом же пределе минуту назад. Довод целиком, с
        // ценой обмена, — у [`crate::mmap::maps_outside_index`].
        //
        // Замер приходится на тот миг, когда работников на соединения ещё нет, а
        // каждый поток — это стек и защитная страница под ним, то есть база
        // подрастёт после старта на пару сотен. Это осознанно оставлено запасу в
        // десятую долю предела: сотни против почти шести тысяч при умолчании
        // 65530. Ждать же ради точности, пока поднимутся работники, нельзя —
        // отказ обязан прийти до замка писателя и переигрывания журнала.
        // Имя своё, не `base`: `base` в конце этой же функции — начала номеров
        // документов, и одно слово на две разные величины читалось бы как
        // ошибка ровно там, где её быть не должно.
        let base_maps = crate::mmap::maps_outside_index(
            limit.and_then(|_| crate::mmap::process_maps()),
            crate::mmap::live_maps(),
        );
        if let Some(limit) = limit {
            // Ступень первая: пол в две области на сегмент. Если уже он не лезет,
            // отказ верен заведомо — и приходит до того, как открыт хоть один
            // каталог, а их могут быть десятки тысяч.
            map_gate(crate::mmap::map_budget_floor(dirs.len(), base_maps, limit))?;
        }

        let mut list: Vec<Arc<Index>> = Vec::with_capacity(dirs.len());
        // Собирается число документов на сегмент, а начала считаются потом:
        // накопление на месте пришлось бы обрывать посреди открытия.
        let mut sizes: Vec<usize> = Vec::with_capacity(dirs.len());
        for (i, d) in dirs.iter().enumerate() {
            // Замер нужен только на первом сегменте — остальные считаются по
            // нему, потому что схема у всех одна.
            let held = if i == 0 { crate::mmap::live_maps() } else { 0 };
            let ix = index::load(d)?;
            // Схемы сегментов обязаны совпадать, и проверять это надо здесь.
            //
            // Отказ громкий и на открытии, а не тихая подмена на выдаче:
            // подмену заметят через недели, и к тому времени поверх неё уже
            // будет построено.
            if let Some(first) = list.first() {
                if let Err(e) = same_schema(first, &ix) {
                    return Err(std::io::Error::other(format!(
                        "{d}: схема сегмента не совпадает с {} — {e}",
                        first.dir
                    )));
                }
            }
            sizes.push(ix.docs);
            list.push(Arc::new(ix));
            // Ступень вторая: точное число областей на сегмент — разность до и
            // после открытия первого. Решение принимается здесь, **до** второго
            // сегмента: дальше открывать нечего, если весь набор всё равно не
            // поместится. Одинаковость схем у всех сегментов гарантирует эта же
            // функция парой строк выше, поэтому первый сегмент представителен.
            if i == 0 {
                if let Some(limit) = limit {
                    let per = remember_per_segment(
                        &MAPS_PER_SEGMENT,
                        crate::mmap::live_maps().saturating_sub(held),
                    );
                    map_gate(crate::mmap::map_budget_with(
                        dirs.len(),
                        per,
                        base_maps,
                        limit,
                        crate::mmap::live_maps(),
                    ))?;
                }
            }
        }
        let base = bases(&sizes).map_err(std::io::Error::other)?;
        Ok(Segments::from_parts(list, base))
    }

    /// Сходятся ли схемы двух сегментов.
    ///
    /// Сравниваются имя, тип и признаки хранения — всё, от чего зависит, как
    /// читать поле. Вес и средняя длина не сравниваются: они считаются по
    /// содержимому сегмента и обязаны отличаться.
    #[allow(dead_code)]
    fn schema_of(ix: &Index) -> Vec<(String, &'static str, bool, bool)> {
        ix.fields.iter().map(|f| (f.name.clone(), f.kind.name(), f.stored, f.positioned)).collect()
    }

    /// Перечитывает состав, **переоткрывая только то, что изменилось**.
    ///
    /// ## Зачем
    ///
    /// [`Segments::load`] открывает каждый сегмент заново, а открытие — это не
    /// пара отображений: у раскладки 7 и выше оно гонит `walk_terms`, полный
    /// разбор всех записей словаря сегмента. На сотне сегментов это терпимо, на
    /// четырёх тысячах — десятки секунд.
    ///
    /// Платит за это не фоновая задача, а приём: перезагрузка идёт на **каждом**
    /// сбросе, под замком буфера и замком состава разом. Всё это время стоят
    /// приём, удаления, фоновый писатель — и `/_health`, потому что он тоже
    /// берёт замок буфера. Балансировщик снимает узел с трафика ровно в тот
    /// момент, когда тот занят обычной работой.
    ///
    /// При этом менять на сбросе нечего: появился один новый сегмент, остальные
    /// те же самые. Сегмент неизменяем — значит уже открытый годится как есть.
    ///
    /// ## Почему не по списку имён от вызывающего
    ///
    /// Прежняя редакция брала список изменившихся имён. Это требовало от каждого
    /// вызывающего знать, что он поменял, и любая забытая правка означала бы
    /// молча устаревший сегмент в выдаче. Здесь состав сверяется с диском, и
    /// знать вызывающему ничего не нужно.
    ///
    /// ## Чем опознаётся «тот же самый»
    ///
    /// Не именем каталога: каталог могли снести и завести заново под тем же
    /// именем. Старые отображения после этого продолжают показывать содержимое
    /// отвязанных inode, то есть поиск отвечал бы из сегмента, которого на диске
    /// уже нет. Поэтому сверяется [`crate::index::head_ident`] — узел и время
    /// правки заголовка, который пишется последним и больше не меняется.
    ///
    /// Карта живых под это не попадает и не должна: она правится на месте в
    /// разделяемом отображении, и уже открытый сегмент видит гашения сам.
    pub fn reopen(dir: &str, prev: &Segments) -> std::io::Result<Segments> {
        let mut dirs = crate::manifest::segments_checked(dir).map_err(std::io::Error::other)?;
        // Пустой список сегментов — это «на диске ничего нет», а не «ничего нет
        // вовсе»: живой сегмент лежит в памяти и в списке не значится никогда.
        //
        // Здесь стоял ранний возврат пустого состава, и он молча выбрасывал
        // живой сегмент — но только в одном случае: когда уборка забрала
        // последний дисковый сегмент, потому что все его документы оказались
        // погашены. То есть ровно на том узле, где всё принятое лежит в буфере,
        // и терять его больнее всего. Поймано тестом на перезапуск.
        if dirs.is_empty() && std::path::Path::new(&format!("{dir}/head.tsv")).exists() {
            dirs.push(dir.to_string());
        }
        let open: std::collections::HashMap<&str, &Arc<Index>> =
            prev.list.iter().map(|ix| (ix.dir.as_str(), ix)).collect();

        let mut list: Vec<Arc<Index>> = Vec::with_capacity(dirs.len());
        let mut sizes: Vec<usize> = Vec::with_capacity(dirs.len());
        for d in dirs.iter() {
            let ix = match open.get(d.as_str()) {
                // Опознание сверяется **до** переиспользования: несовпадение
                // означает тёзку, а не тот же сегмент.
                Some(old) if old.head_id == index::head_ident(d) && old.head_id != (0, 0) => {
                    Arc::clone(old)
                }
                _ => Arc::new(index::load(d)?),
            };
            if let Some(first) = list.first() {
                if let Err(e) = same_schema(first, &ix) {
                    return Err(std::io::Error::other(format!(
                        "{d}: схема сегмента не совпадает с {} — {e}",
                        first.dir
                    )));
                }
            }
            sizes.push(ix.docs);
            list.push(ix);
        }
        // Живые сегменты переносятся из прежнего набора как есть.
        //
        // Открыть их заново неоткуда: каталога у них нет, в списке сегментов их
        // нет тоже — они существуют ровно столько, сколько их держат. Забудь их
        // здесь, и всё принятое после последнего сброса исчезло бы из выдачи на
        // ближайшей перезагрузке состава — молча и целиком.
        //
        // И всегда в хвосте: состав идёт от старых к новым, а живой сегмент —
        // это самое свежее, что есть у узла. Читатель, ищущий последнюю версию
        // документа, обходит состав с конца, и живой обязан попасться ему
        // первым.
        for ix in prev.list.iter().filter(|ix| ix.src.is_memory()) {
            if let Some(first) = list.first() {
                if let Err(e) = same_schema(first, ix) {
                    return Err(std::io::Error::other(format!(
                        "{}: схема живого сегмента не совпадает с {} — {e}",
                        ix.dir, first.dir
                    )));
                }
            }
            sizes.push(ix.docs);
            list.push(Arc::clone(ix));
        }
        // Начала пересчитываются **всегда**, а не переносятся из прежнего
        // набора: состав мог измениться и по числу, и по порядку, а начала
        // выводятся из обоих. Перенос был бы тем самым молчаливым сдвигом
        // номеров, от которого стоит `segments_checked`.
        let base = bases(&sizes).map_err(std::io::Error::other)?;
        Ok(Segments::from_parts_reusing(list, base, prev))
    }

    /// Тот же состав с добавленным живым сегментом — тем, что собран в память.
    ///
    /// ## Почему в хвосте и почему это важно
    ///
    /// Состав упорядочен от старых сегментов к новым, и на этом порядке стоит
    /// чтение по идентификатору: оно обходит сегменты **с конца** и берёт первую
    /// живую версию. Поставь живой сегмент в начало — и читатель, у которого
    /// документ есть и в старом сегменте, и в живом, брал бы старую версию.
    ///
    /// ## Почему новый набор, а не правка на месте
    ///
    /// Читатели работают со снимком состава и не блокируются. Набор поэтому не
    /// меняется, а заменяется целиком: тот, кто уже начал запрос, доводит его по
    /// прежнему снимку до конца.
    pub fn with_live(&self, ix: Arc<Index>) -> std::io::Result<Segments> {
        if !ix.src.is_memory() {
            return Err(std::io::Error::other(format!(
                "{}: в состав добавляют как живой сегмент, а под ним файл",
                ix.dir
            )));
        }
        if let Some(first) = self.list.first() {
            if let Err(e) = same_schema(first, &ix) {
                return Err(std::io::Error::other(format!(
                    "{}: схема живого сегмента не совпадает с {} — {e}",
                    ix.dir, first.dir
                )));
            }
        }
        let mut list = self.list.clone();
        list.push(ix);
        let sizes: Vec<usize> = list.iter().map(|s| s.docs).collect();
        let base = bases(&sizes).map_err(std::io::Error::other)?;
        Ok(Segments::from_parts_reusing(list, base, self))
    }

    /// Тот же состав без названного живого сегмента.
    ///
    /// Зовётся, когда живой сегмент лёг на диск и его место занял обычный:
    /// оставить оба значило бы показать каждый его документ дважды.
    pub fn without_live(&self, name: &str) -> std::io::Result<Segments> {
        let list: Vec<Arc<Index>> = self
            .list
            .iter()
            .filter(|ix| !(ix.src.is_memory() && ix.dir == name))
            .cloned()
            .collect();
        let sizes: Vec<usize> = list.iter().map(|s| s.docs).collect();
        let base = bases(&sizes).map_err(std::io::Error::other)?;
        Ok(Segments::from_parts_reusing(list, base, self))
    }

    /// Тот же состав без единого живого сегмента.
    ///
    /// Зовётся публикацией: она снимает все живые разом и собирает их заново,
    /// потому что за время с прошлого раза мог смениться любой из них.
    pub fn without_any_live(&self) -> std::io::Result<Segments> {
        let list: Vec<Arc<Index>> =
            self.list.iter().filter(|ix| !ix.src.is_memory()).cloned().collect();
        let sizes: Vec<usize> = list.iter().map(|s| s.docs).collect();
        let base = bases(&sizes).map_err(std::io::Error::other)?;
        Ok(Segments::from_parts_reusing(list, base, self))
    }

    /// Сегменты, собранные в память, в порядке появления.
    ///
    /// Опознаются по источнику, а не по признаку рядом с ним: признак был бы
    /// вторым местом, где записано одно и то же, и однажды разошёлся бы с тем,
    /// чем сегмент является.
    pub fn live_ones(&self) -> impl Iterator<Item = &Arc<Index>> {
        self.list.iter().filter(|ix| ix.src.is_memory())
    }

    /// Затягивает метаданные всех сегментов в кеш страниц.
    pub fn warm(&self) -> usize {
        self.warm_within(&mut crate::mmap::WarmBudget::unlimited())
    }

    /// То же под общим на весь набор бюджетом.
    ///
    /// Общим, а не посегментным: бюджет принадлежит узлу. Раздай его каждому
    /// сегменту порознь — и сто сегментов истратили бы сто бюджетов, то есть
    /// предел перестал бы что-либо ограничивать.
    pub fn warm_within(&self, left: &mut crate::mmap::WarmBudget) -> usize {
        let mut done = 0usize;
        for s in self.list.iter() {
            done += s.warm_within(left);
            if left.stopped() {
                break;
            }
        }
        done
    }

    /// То же для обратного отображения — нужно только пишущему узлу.
    pub fn warm_ids(&self) -> usize {
        self.list.iter().map(|s| s.warm_ids()).sum()
    }

    pub fn docs(&self) -> usize {
        self.list.iter().map(|s| s.docs).sum()
    }

    pub fn lists(&self) -> usize {
        self.list.iter().map(|s| s.nterms).sum()
    }

    /// Ищет по всем сегментам в несколько потоков.
    ///
    /// ## Что делят потоки
    ///
    /// Сегмент неизменяем, поэтому делить его между читателями нечего: каждый
    /// поток берёт свои сегменты и работает по ним, ничего не блокируя. Общим
    /// остаётся ровно одно — **порог**, и он же главное, ради чего это делается.
    ///
    /// ## Почему выдача не плывёт
    fn boolean(spec: &QuerySpec) -> bool {
        spec.nested.is_some()
            || spec.min_should > 0
            || !spec.must.is_empty()
            || !spec.must_not.is_empty()
            || !spec.sort.is_empty()
            // Отбор тоже уводит с пути MaxScore: он может повести обход сам, а
            // при запросе без слов вести больше некому.
            || !spec.filter.is_empty()
            // Фраза — тоже: у неё проверка после пересечения, а быстрый путь
            // MaxScore проверок не знает и вернул бы документы, где слова есть,
            // но стоят порознь.
            || !spec.phrases.is_empty()
            // И курсор страницы: проверка «строго после показанного» живёт в
            // булевом пути, а быстрый путь по весу её не знает вовсе — и молча
            // отдавал ту же первую страницу снова.
            || spec.after.is_some()
    }

    /// Раскрывает запрос по каждому сегменту и проставляет **общий** вес слова.
    ///
    /// Обратная частота обязана считаться по всему индексу, а не по сегменту.
    /// Верхушки сегментов складываются в один список и сортируются по сырому
    /// весу — значит числа из разных сегментов сравниваются между собой. Пока
    /// каждое посчитано по своим `N` и `df`, вес перестаёт быть функцией
    /// документа и запроса и становится функцией того, как приём нарезал корпус.
    ///
    /// Так же устроен и Lucene: `docFreq` собирается по всем листам до создания
    /// скорера, и внутри индекса обратная частота одна. Посегментной она там не
    /// бывает — расхождение по шардам это уровень выше, и на него есть
    /// `dfs_query_then_fetch`.
    ///
    /// Цена: словарных раскрытий не прибавляется, они переезжают сюда из цикла.
    /// Прибавляется чтение `df` на пару «сегмент × список» — те же строки кеша,
    /// которые всё равно прочтёт курсор.
    /// Сколько документов в этом наборе и какова частота каждого слова запроса.
    ///
    /// Нужно тому, кто складывает несколько наборов в один ответ: обратная
    /// частота обязана считаться по **всему** тому, что участвует в запросе, а не
    /// по части. Разбор — у [`crate::shards`].
    pub fn stats(&self, spec: &QuerySpec) -> (usize, Vec<u64>) {
        let total: usize = self.list.iter().map(|ix| ix.docs).sum();
        let mut df: Vec<u64> = Vec::new();
        let candidates = self.term_candidates(spec);
        let expansion = spec.expansion.clone().unwrap_or_else(|| {
            Arc::new(ExpansionBudget::new(spec.max_expansions, spec.max_expansion_examined))
        });
        for (i, ix) in self.list.iter().enumerate() {
            if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                break;
            }
            let mut req = if Self::candidate(candidates.as_deref(), i) {
                spec.resolve_with_expansion(ix, Some(&expansion))
            } else {
                spec.resolve_skipped(ix, Some(&expansion))
            };
            let words = words_mut(&mut req);
            if i == 0 {
                df = vec![0; words.len()];
            } else if words.len() != df.len() {
                return (total, Vec::new());
            }
            for (j, w) in words.iter().enumerate() {
                df[j] += w.lists.iter().map(|&l| ix.t_df(l as usize) as u64).sum::<u64>();
            }
        }
        (total, df)
    }

    /// То же, но общую статистику можно задать снаружи.
    ///
    /// `global` — число документов и частоты слов по **всем** участникам запроса.
    /// Задаётся, когда наборов несколько (шарды): без этого каждый посчитал бы
    /// вес по своим `N` и `df`, и сравнивать их между собой было бы нельзя — вес
    /// перестал бы быть функцией документа и запроса.
    pub(crate) fn plan_with(
        &self,
        spec: &QuerySpec,
        global: Option<(usize, &[u64])>,
    ) -> Vec<Request> {
        let mut reqs: Vec<Request> = Vec::with_capacity(self.list.len());
        let candidates = self.term_candidates(spec);
        let expansion = spec.expansion.clone().unwrap_or_else(|| {
            Arc::new(ExpansionBudget::new(spec.max_expansions, spec.max_expansion_examined))
        });
        for (i, ix) in self.list.iter().enumerate() {
            let mut request = if Self::candidate(candidates.as_deref(), i) {
                spec.resolve_with_expansion(ix, Some(&expansion))
            } else {
                spec.resolve_skipped(ix, Some(&expansion))
            };
            request.live = spec.live.as_ref().and_then(|maps| maps.get(i)).cloned();
            reqs.push(request);
        }
        // Один сегмент — местная статистика и есть общая, считать нечего. Но
        // только если статистику не задали снаружи: у шарда из одного сегмента
        // местная частота всё равно не общая.
        if reqs.len() < 2 && global.is_none() {
            return reqs;
        }
        if let Some((total, df)) = global {
            if df.is_empty() {
                return reqs;
            }
            for (i, req) in reqs.iter_mut().enumerate() {
                let words = words_mut(req);
                if words.len() != df.len() {
                    return reqs;
                }
                for (j, w) in words.into_iter().enumerate() {
                    if df[j] == 0 || w.weight.is_some() {
                        continue;
                    }
                    let boost = self.list[i].fields.get(w.field).map_or(1.0, |f| f.boost);
                    w.weight = Some(
                        boost
                            * crate::index::idf_of(total as u64, df[j])
                            * (crate::norms::K1 + 1.0),
                    );
                }
            }
            return reqs;
        }
        let total: usize = self.list.iter().map(|ix| ix.docs).sum();
        let field_docs = self.field_scoring_populations();

        let mut df: Vec<u64> = Vec::new();
        for i in 0..reqs.len() {
            if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                return reqs;
            }
            let req = &mut reqs[i];
            let words = words_mut(req);
            if i == 0 {
                df = vec![0; words.len()];
            } else if words.len() != df.len() {
                // Раскрытие дало разное число слов — сопоставлять по позиции
                // больше нельзя. Такого быть не должно: раскрытие идёт по одному
                // и тому же запросу и кладёт слово даже с пустым списком. Но
                // если случилось, честнее оставить местный вес, чем раздать
                // слову чужой.
                return reqs;
            }
            for (j, w) in words.iter().enumerate() {
                df[j] += w.lists.iter().map(|&l| self.list[i].t_df(l as usize) as u64).sum::<u64>();
            }
        }

        for i in 0..reqs.len() {
            if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                return reqs;
            }
            let req = &mut reqs[i];
            for (j, w) in words_mut(req).into_iter().enumerate() {
                // Слово, которого нет во всём индексе, веса не получает: пусть
                // обход отбросит его по местной df, как отбрасывал всегда.
                if df[j] == 0 {
                    continue;
                }
                if w.weight.is_some() {
                    continue;
                }
                let field = self.list[i].fields.get(w.field);
                let boost = field.map_or(1.0, |f| f.boost);
                let docs = field
                    .and_then(|f| field_docs.get(&f.fid).copied().flatten())
                    .unwrap_or(total as u64);
                w.weight =
                    Some(boost * crate::index::idf_of(docs, df[j]) * (crate::norms::K1 + 1.0));
            }
        }
        self.set_lengths(&mut reqs);
        reqs
    }

    /// Поле, отсутствующее в сегменте, добавляет ноль. Неизвестная legacy
    /// статистика не заменяется нулём: весь fid остаётся на прежнем профиле
    /// до публикации проверенных metadata для всех содержащих его сегментов.
    fn field_scoring_populations(&self) -> HashMap<u32, Option<u64>> {
        let mut totals = HashMap::new();
        for index in &self.list {
            for field in &index.fields {
                let total = totals.entry(field.fid).or_insert(Some(0u64));
                *total =
                    total.zip(field.scoring_docs).map(|(left, right)| left.saturating_add(right));
            }
        }
        totals
    }

    /// Раздаёт запросам таблицы нормировки по **общей** средней длине.
    ///
    /// ## Зачем
    ///
    /// Важность слова уже общая: `N` и `df` складываются по всему составу. А
    /// нормировка на длину оставалась посегментной — таблицы строились из
    /// средней длины **своего** сегмента. Значит один и тот же по составу
    /// индекс, нарезанный иначе, давал другие скоры и другую верхушку. Измерено
    /// на десяти миллионах: у раскладок в 19 и 23 сегмента совпадали 7
    /// идентификаторов из 10.
    ///
    /// ## Как
    ///
    /// Средняя по составу выходит из целых сумм длин, которые сегменты хранят в
    /// заголовке (см. [`crate::norms::FIXED`]): суммы складываются точно и в
    /// любом порядке, поэтому она не зависит от истории нарезки. Таблицы по ней
    /// одни на все сегменты и раздаются ссылкой.
    ///
    /// Сохранённые потолки при этом остаются посегментными — они лежат на
    /// диске. Каждому сегменту считается свой множитель подъёма, приводящий их
    /// к общей средней; без него отсечение выбрасывало бы документы, чей
    /// настоящий вклад выше сохранённого потолка. Разбор — у
    /// [`crate::norms::lift_of`].
    fn set_lengths(&self, reqs: &mut [Request]) {
        if self.list.len() < 2 {
            return;
        }
        // Таблицы строятся один раз на **среднюю**, а не на сегмент: у полей с
        // совпавшей средней они те же, и копировать их по числу сегментов
        // значило бы платить четыре килобайта за каждую копию.
        // Пара таблиц: множитель длины по уровню и знаменатель насыщения.
        type ByLevel = std::sync::Arc<[f64; 256]>;
        /// Средняя длина в битах и построенная по ней пара таблиц.
        type Built = (u64, ByLevel, ByLevel);
        /// То же для поля, но со средней как числом — её ещё считать подъёмом.
        type ForField = (f64, ByLevel, ByLevel);
        let mut tables: Vec<Built> = Vec::new();
        let field_docs = self.field_scoring_populations();
        // Номер колонки местный: body может быть первым в одном сегменте
        // и вторым в другом. Группируем по устойчивому fid, без разреженного
        // массива размером с максимальный идентификатор поля.
        let mut totals = std::collections::BTreeMap::<u32, (u64, u64, bool)>::new();
        for ix in &self.list {
            for field in &ix.fields {
                let entry = totals.entry(field.fid).or_default();
                entry.0 = entry.0.saturating_add(field.len_fixed);
                entry.1 = entry.1.saturating_add(ix.docs as u64);
                entry.2 |= field.kind.dot();
            }
        }
        let mut per_field = HashMap::<u32, ForField>::new();
        for (fid, (fixed, docs, dot)) in totals {
            let docs = field_docs.get(&fid).copied().flatten().unwrap_or(docs);
            // Разреженный вектор длиной не нормируется вовсе — таблицы ему ни к
            // чему, и подъём для него равен единице.
            if docs == 0 || dot {
                continue;
            }
            let e = crate::norms::avgdl_of(fixed, docs);
            let key = e.to_bits();
            let pair = match tables.iter().find(|(k, _, _)| *k == key) {
                Some((_, nu, d)) => (nu.clone(), d.clone()),
                None => {
                    let built = crate::index::FieldScore::tables(e);
                    tables.push((key, built.0.clone(), built.1.clone()));
                    built
                }
            };
            per_field.insert(fid, (e, pair.0, pair.1));
        }
        for (i, req) in reqs.iter_mut().enumerate() {
            let mut score: Vec<Option<crate::index::FieldScore>> =
                vec![None; self.list[i].fields.len()];
            for (f, slot) in self.list[i].fields.iter().zip(&mut score) {
                let Some((e, nu, d)) = per_field.get(&f.fid) else { continue };
                *slot = Some(crate::index::FieldScore {
                    nu: nu.clone(),
                    d: d.clone(),
                    lift: crate::norms::lift_of(f.avgdl, *e, f.lmax),
                });
            }
            req.score = score;
        }
    }

    pub fn search_with(&self, spec: &QuerySpec, threads: usize) -> Outcome {
        self.search_with_g(spec, threads, None)
    }

    /// Совместимые top-K и document count по одному плану. Exact учитывает
    /// и неконкурентные документы; UpTo считает до общего `limit + 1`, после
    /// чего верхушка использует обычное безопасное отсечение. Search-after
    /// ограничивает только hits, а не множество документов для count.
    pub fn search_count_with_g(
        &self,
        spec: &QuerySpec,
        threads: usize,
        global: Option<(usize, &[u64])>,
    ) -> (Outcome, u64) {
        // Исторически пустой flat search не возвращает hits, но aggregate без
        // query считает весь корпус. Сохраняем оба договора в одном проходе.
        let empty_search = !Self::boolean(spec) && spec.words.is_empty();
        let owned = (spec.guard.is_none() || empty_search).then(|| QuerySpec {
            guard: Some(
                spec.guard
                    .clone()
                    .unwrap_or_else(|| Arc::new(crate::work::Guard::new(spec.max_agg_ms))),
            ),
            nested: if empty_search { Some(SpecBool::default()) } else { spec.nested.clone() },
            k: if empty_search { 0 } else { spec.k },
            ..spec.clone()
        });
        let spec = owned.as_ref().unwrap_or(spec);
        let guard = spec.guard.as_ref().expect("единый guard collectors");
        if spec.max_agg_ms > 0 {
            guard.tighten(std::time::Duration::from_millis(spec.max_agg_ms));
        }
        if !guard.check() {
            return (Outcome { hits: Vec::new(), visited: 0, partial: true }, 0);
        }
        let mut requests = self.plan_with(spec, global);
        let budget = share_budget(spec, &mut requests);
        for (at, request) in requests.iter_mut().enumerate() {
            request.doc_base = self.base[at];
        }
        let workers = threads.max(1).min(self.list.len().max(1));
        let next = std::sync::atomic::AtomicUsize::new(0);
        // Один sentinel сверх порога отличает точное равенство от `gte`.
        // Общий остаток не умножает полный count на число сегментов/потоков.
        let remaining = match spec.track_total_hits {
            TrackTotalHits::UpTo(limit) => {
                Some(std::sync::atomic::AtomicU64::new(limit.saturating_add(1)))
            }
            TrackTotalHits::Exact | TrackTotalHits::Omit => None,
        };
        let lane = || {
            let mut hits = Vec::new();
            let mut count = 0u64;
            let mut visited = 0u64;
            let mut floor = if Self::boolean(spec) { f64::NEG_INFINITY } else { 0.0 };
            loop {
                if !guard.check() {
                    break;
                }
                let at = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(ix) = self.list.get(at) else { break };
                if !Self::range_can_match(ix, spec) {
                    continue;
                }
                let (found, matched, examined) = search::search_count_budgeted_above(
                    ix,
                    &requests[at],
                    floor,
                    remaining.as_ref(),
                );
                count += matched;
                visited += examined;
                hits.extend(found.into_iter().map(|hit| Hit {
                    doc: self.base[at] + u64::from(hit.doc),
                    score: hit.score,
                    keys: hit.keys,
                    nkeys: hit.nkeys,
                    segment: at,
                    local: hit.doc,
                }));
                hits.sort_by(best_first);
                hits.truncate(spec.k);
                if spec.k > 0 && hits.len() == spec.k {
                    floor = hits[spec.k - 1].score;
                }
            }
            (hits, count, visited)
        };
        let parts =
            if workers == 1 { vec![lane()] } else { crate::executor::map(workers, |_| lane()) };
        let mut hits = Vec::new();
        let mut count = 0;
        let mut visited = 0;
        for (part, matches, examined) in parts {
            hits.extend(part);
            hits.sort_by(best_first);
            hits.truncate(spec.k);
            count += matches;
            visited += examined;
        }
        (Outcome { hits, visited, partial: capped(&budget) || interrupted(spec) }, count)
    }

    /// То же, но с общей статистикой снаружи. См. [`Segments::plan_with`].
    pub fn search_with_g(
        &self,
        spec: &QuerySpec,
        threads: usize,
        global: Option<(usize, &[u64])>,
    ) -> Outcome {
        if spec.guard.as_ref().is_some_and(|g| !g.check()) {
            return Outcome { hits: Vec::new(), visited: 0, partial: true };
        }
        // Булев обход по сегментам не делится: порог между ними при сортировке
        // по полю не значит того же, что при сортировке по весу, и делить его
        // между потоками значило бы отсекать по величине, которая ещё не
        // установилась. Сперва правильно, потом быстро.
        if threads <= 1 || self.list.is_empty() {
            return self.search_g(spec, global);
        }
        // Дешёвый запрос не стоит потоков — и это верно при **любом** числе
        // сегментов.
        //
        // Прежде сторож стоял под условием `сегментов < потоков`, то есть
        // проверял дешевизну только там, где сегментов мало. Условие
        // перевёрнутое: чем больше сегментов, тем больше потоков поднимается на
        // тот же запрос, и тем заметнее их цена по сравнению с его работой.
        //
        // Замерено на двухстах сегментах при десяти потоках: редкое слово
        // 0.161 мс последовательно против 0.208 мс в потоках — **медленнее**,
        // потому что сама работа занимает меньше, чем подъём потоков. Сторож при
        // этом не срабатывал: двести сегментов не меньше десяти потоков.
        //
        // Разведка стоит одного раскрытия запроса по нулевому сегменту и чтения
        // частот его списков — против подъёма потоков это даром.
        // Многословный запрос по сегментам не делится, сколько бы постингов в
        // нём ни было.
        //
        // У одного слова отсекать нечем: порог поднимается только собственными
        // документами этого же списка, и пройти его приходится почти целиком —
        // вот там потоки и окупаются вчетверо.
        //
        // ## Почему многословный запрос больше не уводится в один поток
        //
        // Здесь стояло отдельное правило: «многословный запрос по сегментам не
        // делится, сколько бы постингов в нём ни было». Довод был такой: у
        // запроса из нескольких слов порог верхушки поднимается после первых же
        // документов, MaxScore перепрыгивает целые блоки — замерено 46
        // просмотренных документов там, где списки содержат миллионы, — и подъём
        // десяти потоков стоит дороже такого запроса целиком.
        //
        // Довод верен, но правило было шире довода. Он относится к запросу, где
        // есть **редкое** слово: оно и поднимает порог. Там же, где частые все,
        // отсекать нечем ровно по той же причине, что и у одного слова, и работа
        // настоящая. Правило же смотрело на число слов и уводило в один поток и
        // тех и других.
        //
        // Проверено на 25 млн реальных сообщений в 31 сегменте, лучшее из
        // пятнадцати после прогрева:
        //
        // | запрос | правило было | правила нет |
        // |---|---:|---:|
        // | два частых («https», «не») | 12.25 мс | **4.29 мс** |
        // | два слова (оба редких) | 0.140 мс | 0.177 мс |
        // | три слова (все редкие) | 0.181 мс | 0.204 мс |
        //
        // У редких слов число просмотренных документов не изменилось — 256 и
        // 179, — то есть в один поток их увёл сторож по постингам, как и должен
        // был. Разница в сотые доли миллисекунды у них — дрейф замера: столько
        // же набежало и на однословном запросе, которого правило не касалось
        // вовсе.
        //
        // Отсюда: решает **одна** мера — объём постингов. Второй меры не нужно.
        let masked = spec.words.iter().flat_map(|w| w.terms.iter()).any(|t| t.prefix);
        if !masked {
            let probe = spec.resolve(&self.list[0]);
            // Обязательные ветви входят в оценку наравне со словами: у запроса
            // без единого слова, но с двумя `must`, работа не нулевая, а прежняя
            // оценка по одним `words` давала ноль и уводила его в один поток.
            let per_segment: u64 = probe
                .words
                .iter()
                .chain(probe.must.iter())
                .chain(probe.must_not.iter())
                .flat_map(|w| w.lists.iter())
                .map(|&l| self.list[0].t_df(l as usize) as u64)
                .sum();
            if per_segment * (self.list.len() as u64) < PARALLEL_POSTINGS {
                return self.search_g(spec, global);
            }
        }
        // Один сегмент делить между собой нечего — делятся блоки внутри него.
        //
        // **Только быстрый путь по весу.** `search::search_with` смотрит на
        // `req.words` и ни на что больше: обязательные, запрещённые, отбор,
        // фразы и вложенные ветви до него не доходят. Булев запрос, попавший
        // сюда, отвечал не «медленно», а **неверно** — молча, без отказа.
        //
        // Как это выглядело: `must` с частым словом на слитом в один сегмент
        // индексе возвращал пустую выдачу. С редким — работал, потому что
        // дешёвый запрос уходил в `search_g` сторожем выше. То есть один и тот
        // же запрос к одним и тем же данным отвечал по-разному в зависимости от
        // частоты слова и от того, сливали индекс или нет.
        //
        // Отсюда порядок: сперва правильный обход, а внутрисегментное деление
        // булева обхода — отдельная работа, у [`search::search_boolean_with`].
        if threads > 1 && self.list.len() == 1 && !Self::boolean(spec) {
            let ix = &self.list[0];
            // Сегмент один — местная статистика и есть общая. Но только если её
            // не задали снаружи: у шарда из одного сегмента общая частота
            // считается по **всем** шардам, и местная ей не равна.
            let mut reqs = self.plan_with(spec, global);
            let budget = share_budget(spec, &mut reqs);
            let (found, visited) = search::search_with(ix, &reqs[0], 0.0, threads);
            let mut hits: Vec<Hit> = found
                .into_iter()
                .map(|h| Hit {
                    doc: h.doc as u64,
                    score: h.score,
                    keys: h.keys,
                    nkeys: h.nkeys,
                    segment: 0,
                    local: h.doc,
                })
                .collect();
            // Расхождение стоило стократного ответа: на слитом в один сегмент
            // индексе `from = 990, size = 10` отдавал тысячу записей вместо
            // десяти, а на том же индексе из нескольких сегментов — десять.
            // Один и тот же запрос к одним и тем же данным давал разный ответ в
            // зависимости от того, сливали индекс или нет.
            hits.truncate(spec.k);
            return Outcome { hits, visited, partial: capped(&budget) || interrupted(spec) };
        }
        if threads <= 1 || self.list.len() <= 1 {
            return self.search_g(spec, global);
        }
        let k = spec.k;
        // Общий порог по весу — **несущий**, а не «сколько-нибудь».
        //
        // Он хранится битами `f64` и накапливается через `fetch_max`, а порядок
        // битов совпадает с порядком чисел только у неотрицательных. Отсюда
        // прижатие к нулю при записи — и отсюда же был дефект: последовательный
        // обход заводит порог булева запроса минус бесконечностью
        // (`search_g`), потому что вес бывает отрицательным — множитель фразы
        // задаётся запросом и знака не ограничивает. Здесь порог начинался с
        // нуля, и `passes` отбрасывал по нему **все** попадания с отрицательным
        // весом.
        //
        // То есть один и тот же запрос к одним и тем же данным отдавал выдачу
        // или пустоту в зависимости от числа рабочих потоков узла, которое
        // клиент не задаёт вовсе.
        //
        // Отрицательные веса из общего порога поэтому исключены целиком: пока
        // они возможны, делить его нечем — `fetch_max` по битам для них
        // означает не то. Обход при этом остаётся верным, просто отсекает
        // меньше.
        let signed = Self::boolean(spec);
        let floor = AtomicU64::new(0);
        let workers = threads.min(self.list.len());

        let mut reqs = self.plan_with(spec, global);
        let budget = share_budget(spec, &mut reqs);
        // Порог сортировки — общая ячейка на все потоки.
        //
        // Прежде запрос с сортировкой в потоки не пускали вовсе: порог
        // передавался по цепочке сегментов, а цепочки у потоков нет. Довод был
        // верен для **числа внутри запроса** и только для него — сама величина
        // делится ничем не хуже порога по весу, потому что это необходимое
        // условие, а не точный порог: `k`-й у потока не хуже общего `k`-го,
        // общий набор шире. Разбор — у [`crate::search::KeyFloor`].
        let key_floor = (!spec.sort.is_empty())
            .then(|| std::sync::Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)));
        // Начало сегмента проставляется **до** раздачи потокам: оно от порядка
        // обхода не зависит, а внутри области запросы уже только читаются.
        for (i, req) in reqs.iter_mut().enumerate() {
            req.doc_base = self.base[i];
            if let Some(f) = key_floor.as_ref() {
                req.floor_key = crate::search::KeyFloor::Shared(std::sync::Arc::clone(f));
            }
        }
        let boolean = Self::boolean(spec);
        let reqs = &reqs;
        let key_floor = key_floor.as_deref();
        let parts: Vec<(Vec<Hit>, u64)> = crate::executor::map(workers, |w| {
            let mut hits: Vec<Hit> = Vec::new();
            let mut visited = 0u64;
            // Сегменты раздаются через один: соседние обычно
            // одинаковы по размеру, и такая раздача ровнее любой
            // нарезки на куски.
            for i in (w..self.list.len()).step_by(workers) {
                if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                    break;
                }
                let ix = &self.list[i];
                // То же правило, что и в последовательном обходе:
                // сегмент вне отбора не открывается. Правило одно на
                // оба пути — разойдись они, поток и очередь давали бы
                // разную выдачу на одном запросе.
                if !Self::range_can_match(ix, spec) {
                    continue;
                }
                // И сегмент, который заведомо не даст лучшего, чем
                // уже найденный k-й. Порог берётся из общей ячейки,
                // то есть учитывает и то, что нашли соседние потоки.
                let seen_key = key_floor.map(|f| f.load(Ordering::Relaxed));
                if !Self::sort_can_beat(
                    ix,
                    spec,
                    seen_key.filter(|&v| v != i64::MIN).map(search::SortKey::from),
                ) {
                    continue;
                }
                let seen_floor = if signed {
                    f64::NEG_INFINITY
                } else {
                    f64::from_bits(floor.load(Ordering::Relaxed))
                };
                let (found, seen) = if boolean {
                    search::search_boolean(ix, &reqs[i], seen_floor)
                } else {
                    search::search(ix, &reqs[i], seen_floor)
                };
                visited += seen;
                for h in found {
                    hits.push(Hit {
                        doc: h.doc as u64 + self.base[i],
                        score: h.score,
                        keys: h.keys,
                        nkeys: h.nkeys,
                        segment: i,
                        local: h.doc,
                    });
                }
                hits.sort_by(best_first);
                hits.truncate(k);
                if hits.len() >= k {
                    if !signed {
                        floor.fetch_max(hits[k - 1].score.max(0.0).to_bits(), Ordering::Relaxed);
                    }
                    if let (Some(f), true) = (key_floor, hits[k - 1].nkeys > 0) {
                        if let Ok(key) = i64::try_from(hits[k - 1].keys[0]) {
                            f.fetch_max(key, Ordering::Relaxed);
                        }
                    }
                }
            }
            (hits, visited)
        });

        let mut hits: Vec<Hit> = Vec::new();
        let mut visited = 0u64;
        for (part, seen) in parts {
            hits.extend(part);
            visited += seen;
        }
        // Слияние идёт **тем же** сравнивателем, что и внутри потока. Здесь
        // стоял порядок по весу — верный ровно до тех пор, пока запросы с
        // сортировкой сюда не доходили: они уводились в один поток. Оставь его —
        // и сортировка по полю молча превратилась бы в сортировку по весу.
        hits.sort_by(best_first);
        hits.truncate(k);
        Outcome { hits, visited, partial: capped(&budget) || interrupted(spec) }
    }

    /// Ищет по всем сегментам и сливает верхушки.
    ///
    /// Порог переносится от сегмента к сегменту: каждый следующий отсекает по
    /// уже набранной верхушке, а не начинает с нуля. Это не приближение, а то
    /// же рассуждение, что и внутри сегмента: документ, не побивающий текущего
    /// K-го места, в выдачу не попадёт, из какого бы сегмента он ни пришёл.
    pub fn search(&self, spec: &QuerySpec) -> Outcome {
        self.search_g(spec, None)
    }

    /// То же, но с общей статистикой снаружи. См. [`Segments::plan_with`].
    /// Может ли этот сегмент дать хоть один документ под отбор запроса.
    ///
    /// ## Зачем
    ///
    /// Замерено: сегмент стоит запросу от трёх микросекунд (слово) до
    /// пятидесяти восьми (сортировка по числу) **до всякой полезной работы**.
    /// При тысяче сегментов это три и пятьдесят восемь миллисекунд, при ста
    /// тысячах — триста миллисекунд и почти шесть секунд. На десятках
    /// миллиардов документов решает не объём данных, а число сегментов, и
    /// единственный способ не платить за сегмент — его не открывать.
    ///
    /// ## Почему это не приблизительно
    ///
    /// Границы `[lo, hi]` записаны при сборке по всем присутствующим значениям
    /// поля. Документ со значением вне них в сегменте отсутствует **как факт**,
    /// а не с какой-то вероятностью. Границ нет — сегмент читается: «не знаем»
    /// и «не подходит» обязаны различаться, иначе старые сегменты, у которых
    /// этих столбцов в заголовке нет, начали бы молча пропускаться.
    fn range_can_match(ix: &Index, spec: &QuerySpec) -> bool {
        spec.filter.iter().all(|r| {
            let Some(at) = ix.field_by_id(r.fid as u32) else {
                // Поля в этом сегменте нет вовсе — значит нет и документа,
                // проходящего отбор по нему. Пропускать здесь **нельзя**
                // возвращать `true`: сегмент, где поле не объявлено, отбору по
                // нему не удовлетворяет никто. Но и решать это здесь не наше
                // дело — так же считает и сам обход, а раздваивать правило хуже.
                return true;
            };
            match ix.fields[at].bounds {
                Some((lo, hi)) => r.from <= hi && lo <= r.to,
                None => true,
            }
        })
    }

    /// Может ли сегмент дать документ лучше уже набранного k-го при сортировке.
    ///
    /// Отдельно от [`Segments::range_can_match`], потому что условие другое:
    /// там сегмент отсекается **свойством запроса**, здесь — тем, что успели
    /// найти раньше. Порог поэтому приходит снаружи: на последовательном обходе
    /// по цепочке сегментов, на потоках — из общей ячейки, куда его складывают
    /// все. Разбор — у [`crate::search::KeyFloor`].
    ///
    /// Замерено: сортировка по числу стоит 58 микросекунд на сегмент, в
    /// двадцать раз дороже словесного запроса. На дописываемом корпусе,
    /// отсортированном по времени убыванием, первые же сегменты набирают всю
    /// верхушку, и остальные читать незачем.
    ///
    /// Смотрит только на **первый** ключ сортировки: по нему и передаётся порог
    /// между сегментами (`floor_key`). Второй и следующие решают лишь
    /// равенство первого, и на «может ли сегмент дать лучшее» не влияют.
    fn sort_can_beat(ix: &Index, spec: &QuerySpec, floor_key: Option<search::SortKey>) -> bool {
        let (Some(fk), Some(s)) = (floor_key, spec.sort.first()) else {
            return true;
        };
        let Some(at) = ix.field_by_id(s.fid as u32) else { return true };
        let Some((lo, hi)) = ix.fields[at].bounds else { return true };
        // Документ без значения получает `MISSING_KEY` и идёт в конец; если
        // такой в сегменте есть, лучшего он всё равно не даст, и на потолок не
        // влияет. А вот значения есть не у всех — потолок берётся по границам.
        let best = crate::search::norm_key(if s.desc { hi } else { lo }, s.desc);
        best >= fk
    }

    /// В каком порядке обходить сегменты.
    ///
    /// ## Почему порядок вообще важен
    ///
    /// Пропуск по потолку (`sort_can_beat`) работает только тогда, когда
    /// хороший порог набран **рано**. При обходе по номеру этого не случается:
    /// сегменты пишутся от старых к новым, а спрашивают обычно новое сверху —
    /// то есть первым читается самый безнадёжный сегмент, порог получается
    /// худшим из возможных, и обойти его может каждый следующий.
    ///
    /// Замерено на двух миллионах в сорока сегментах: сортировка по убыванию
    /// времени не пропустила **ни одного** сегмента, просмотрев 16 103
    /// документа вместо десяти нужных.
    ///
    /// Порядок обхода на выдачу не влияет: верхушка всё равно сводится и
    /// сортируется в конце. Влияет он только на то, сколько сегментов вообще
    /// придётся открыть.
    fn walk_order(&self, spec: &QuerySpec) -> Vec<usize> {
        let plain = || (0..self.list.len()).collect::<Vec<_>>();
        let Some(s) = spec.sort.first() else { return plain() };
        // Ключ сегмента — лучшее, что он может дать. Не знаем границ хотя бы у
        // одного — обходим по номеру: ставить сегмент с неизвестным потолком
        // в конец значило бы задвинуть, возможно, самый нужный.
        let mut keyed: Vec<(search::SortKey, usize)> = Vec::with_capacity(self.list.len());
        for (i, ix) in self.list.iter().enumerate() {
            let at = ix.field_by_id(s.fid as u32);
            let Some((lo, hi)) = at.and_then(|at| ix.fields[at].bounds) else {
                return plain();
            };
            keyed.push((crate::search::norm_key(if s.desc { hi } else { lo }, s.desc), i));
        }
        // Лучший потолок первым — он и задаст порог остальным.
        keyed.sort_by_key(|a| std::cmp::Reverse(a.0));
        keyed.into_iter().map(|(_, i)| i).collect()
    }

    pub fn search_g(&self, spec: &QuerySpec, global: Option<(usize, &[u64])>) -> Outcome {
        let k = spec.k;
        let mut hits: Vec<Hit> = Vec::new();
        let mut visited = 0u64;
        let mut floor = 0.0f64;
        let mut floor_key: Option<search::SortKey> = None;

        // Порог между сегментами при сортировке по полю начинается не с нуля:
        // ранг там может быть отрицательным (сортировка по возрастанию — это
        // значение со знаком минус), и ноль отсёк бы половину выдачи.
        if Self::boolean(spec) {
            floor = f64::NEG_INFINITY;
        }
        // Раскрытие и общая статистика — один раз на запрос, до обхода.
        let mut reqs = self.plan_with(spec, global);
        let budget = share_budget(spec, &mut reqs);
        for i in self.walk_order(spec) {
            if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                break;
            }
            let ix = &self.list[i];
            // Сегмент, чей диапазон не пересекается с отбором, не открывается.
            if !Self::range_can_match(ix, spec) {
                continue;
            }
            // И сегмент, который заведомо не даст лучшего, чем уже найденный
            // k-й, — тоже.
            if !Self::sort_can_beat(ix, spec, floor_key) {
                continue;
            }
            let req = &mut reqs[i];
            req.floor_key = match floor_key {
                Some(v) => crate::search::KeyFloor::Fixed(v),
                None => crate::search::KeyFloor::None,
            };
            req.doc_base = self.base[i];
            let (found, seen) = if Self::boolean(spec) {
                search::search_boolean(ix, req, floor)
            } else {
                search::search(ix, req, floor)
            };
            visited += seen;
            for h in found {
                hits.push(Hit {
                    doc: h.doc as u64 + self.base[i],
                    score: h.score,
                    keys: h.keys,
                    nkeys: h.nkeys,
                    segment: i,
                    local: h.doc,
                });
            }
            hits.sort_by(best_first);
            hits.truncate(k);
            if hits.len() >= k {
                floor = hits[k - 1].score;
                // При сортировке порог по весу ничего не значит; между
                // сегментами передаётся первый ключ k-го. Это не точный порог, а
                // верное необходимое условие: документ, чей первый ключ строго
                // хуже, не победит никогда, каковы бы ни были остальные.
                floor_key = (hits[k - 1].nkeys > 0).then(|| hits[k - 1].keys[0]);
            }
        }
        Outcome { hits, visited, partial: capped(&budget) || interrupted(spec) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;

    /// Все bounds пересекаются; отсутствие точного значения доказывается
    /// только словарным фильтром. Отсечённые сегменты всё ещё участвуют в IDF.
    #[test]
    fn numeric_equality_masks_preserve_global_scores_and_legacy_fallback() {
        let dir = std::env::temp_dir().join(format!("svindb-numeric-masks-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut indexer = Indexer::typed(
            &dir,
            vec!["body".into(), "peer".into()],
            vec![1.0; 2],
            vec![crate::index::Kind::Text, crate::index::Kind::Int64],
            3,
        );
        for segment in 0..8 {
            for (offset, value) in [0, if segment == 3 { 42 } else { 7 }, 100].iter().enumerate() {
                indexer.add(&format!("d{segment}-{offset}"), &["общее слово", &value.to_string()]);
            }
        }
        indexer.flush_now();
        drop(indexer);
        let optimized = Segments::load(&dir).expect("состав");
        let mut baseline = Segments::load(&dir).expect("полный путь");
        for segment in &mut baseline.list {
            Arc::get_mut(segment).expect("отдельный reader").term_filter = None;
        }
        let spec = QuerySpec {
            words: vec![SpecWord { fid: 0, terms: vec![Term::parse("общее")], given: None }],
            filter: vec![SpecRange { fid: 1, from: 42, to: 42 }],
            k: 10,
            ..QuerySpec::default()
        };
        assert!(optimized.list.iter().all(|index| Segments::range_can_match(index, &spec)));
        let plan = optimized.plan_with(&spec, None);
        assert!(plan.iter().any(|request| request.skipped), "точный фильтр не отсёк работу");
        let actual = optimized.search(&spec);
        let expected = baseline.search(&spec);
        assert_eq!(actual.hits.len(), 1);
        assert_eq!(actual.hits.len(), expected.hits.len());
        for (actual, expected) in actual.hits.iter().zip(&expected.hits) {
            assert_eq!(actual.doc, expected.doc);
            assert_eq!(
                actual.score.to_bits(),
                expected.score.to_bits(),
                "изменился глобальный IDF"
            );
        }
        let mut legacy = Segments::load(&dir).expect("legacy metadata");
        for segment in &mut legacy.list {
            Arc::get_mut(segment).expect("отдельный reader").layout = 11;
        }
        assert!(legacy.plan_with(&spec, None).iter().all(|request| !request.skipped));
        std::fs::remove_dir_all(&dir).expect("убрать тестовый корпус");
    }

    /// Одно редкое слово по множеству полей не раскрывается отдельно в каждом
    /// словаре каждого сегмента.
    #[test]
    fn fused_field_set_uses_one_generation_aware_term_directory_entry() {
        let dir = std::env::temp_dir().join("svindb-fused-term-directory");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("каталог");
        let dir = dir.to_string_lossy().to_string();
        let fields: Vec<String> = (0..63).map(|field| format!("поле{field}")).collect();
        let mut indexer = Indexer::new(&dir, fields, vec![1.0; 63], 1);
        for segment in 0..4 {
            let values: Vec<String> =
                (0..63).map(|field| format!("обычное{field} сегмент{segment}")).collect();
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            indexer.add(&format!("док{segment}"), &refs);
        }
        indexer.flush_now();

        let segments = Segments::load(&dir).expect("состав");
        assert_eq!(segments.list.len(), 4, "проверке нужны четыре поколения сброса");
        assert!(
            segments.list.iter().all(|segment| segment.term_filter.is_some()),
            "новый сегмент опубликован без фильтра слов"
        );
        // Выбираем гарантированно отрицательную пробу, не полагаясь на
        // вероятность ложного срабатывания Bloom-фильтра.
        let absent = (0..10_000)
            .map(|probe| format!("редкость{probe}"))
            .find(|term| {
                segments.list.iter().all(|segment| {
                    !segment.term_filter.as_ref().expect("фильтр").maybe_has_term(term)
                })
            })
            .expect("не нашлось отрицательной пробы");
        let spec = QuerySpec {
            words: (0..63)
                .map(|fid| SpecWord {
                    fid,
                    terms: vec![Term { text: absent.clone(), prefix: false }],
                    given: None,
                })
                .collect(),
            k: 10,
            ..QuerySpec::default()
        };

        let plan = segments.plan_with(&spec, None);
        assert!(plan.iter().all(|request| request.skipped), "редкий запрос дошёл до словарей");
        assert_eq!(
            segments
                .term_directory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .len(),
            1,
            "одно слово по 63 полям размножилось в каталоге"
        );
        let outcome = segments.search(&spec);
        assert!(outcome.hits.is_empty());
        assert_eq!(outcome.visited, 0, "отвергнутый сегмент начал обход документов");

        // Новая ReadView не наследует ответы старого поколения: публикация
        // сегмента не может оставить битовую карту прежнего состава.
        let reopened = Segments::load(&dir).expect("новое поколение");
        assert!(
            reopened
                .term_directory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .is_empty(),
            "новое поколение унаследовало каталог старого ReadView"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Обязательное слово, которого нет ни в одном сегменте, отбрасывает весь
    /// состав до открытия словарей. Это отдельный контракт от плоского OR выше:
    /// пересечение масок не должно потерять обязательность и превратить
    /// `must(missing) AND must(existing)` в обычный обход existing.
    #[test]
    fn boolean_masks_match_a_view_without_term_filters() {
        let dir = std::env::temp_dir().join(format!("svindb-boolean-masks-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut indexer = Indexer::new(&dir, vec!["body".into()], vec![1.0], 3);
        for i in 0..30 {
            indexer.add(&format!("d{i}"), &[&format!("word{} word{}", i % 5, (i / 3) % 5)]);
        }
        indexer.flush_now();
        drop(indexer);
        let optimized = Segments::load(&dir).expect("состав");
        let mut baseline = Segments::load(&dir).expect("эталон");
        for segment in &mut baseline.list {
            Arc::get_mut(segment).expect("независимый сегмент").term_filter = None;
        }
        let leaf = |n: usize| {
            SpecClause::Term(SpecWord {
                fid: 0,
                terms: vec![Term::parse(&format!("word{n}"))],
                given: None,
            })
        };
        let aggs = [crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        for seed in 0..96 {
            let alternative = match seed % 4 {
                0 => SpecClause::Term(SpecWord {
                    fid: 0,
                    terms: vec![Term::parse("word*")],
                    given: None,
                }),
                1 => SpecClause::Phrase(SpecPhrase {
                    fid: 0,
                    terms: vec!["word0".into(), "word1".into()],
                    slop: 0,
                    boost: 1.0,
                }),
                _ => leaf((seed / 3) % 7),
            };
            let inner = SpecClause::Bool(SpecBool {
                should: vec![leaf(seed % 7), alternative],
                min_should: 1 + seed % 2,
                ..SpecBool::default()
            });
            let spec = QuerySpec {
                nested: Some(SpecBool {
                    must: if seed % 3 == 0 { vec![inner] } else { Vec::new() },
                    should: vec![leaf((seed / 2) % 7), leaf((seed / 5) % 7)],
                    must_not: vec![leaf((seed / 7) % 7)],
                    min_should: seed % 3,
                }),
                k: 30,
                ..QuerySpec::default()
            };
            let actual = optimized.search(&spec);
            let expected = baseline.search(&spec);
            assert_eq!(actual.hits.len(), expected.hits.len(), "seed={seed}");
            for (a, b) in actual.hits.iter().zip(&expected.hits) {
                assert_eq!(a.doc, b.doc, "seed={seed}");
                assert_eq!(a.score.to_bits(), b.score.to_bits(), "seed={seed}");
            }
            assert_eq!(
                optimized.aggregate(&spec, &aggs),
                baseline.aggregate(&spec, &aggs),
                "count seed={seed}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn keyword_values_keep_their_identity_when_merge_changes_dictionary_ordinals() {
        let root =
            std::env::temp_dir().join(format!("svindb-keyword-ordinal-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        let mut writer = Indexer::typed(
            &root,
            vec!["tag".into()],
            vec![1.0],
            vec![crate::index::Kind::KeywordExact],
            1,
        );
        writer.add("a", &["red"]);
        writer.add("b", &["blue"]);
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&root).unwrap();
        let aggs = [crate::aggs::Agg {
            name: "tags".into(),
            kind: crate::aggs::Kind::Terms { field: 0, size: 10 },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Keyword,
        }];
        let expected = segments.aggregate(&QuerySpec::default(), &aggs);
        let refs: Vec<_> = segments.list.iter().map(Arc::as_ref).collect();
        let output = format!("{root}/merged");
        crate::merge::merge(&refs, &output).unwrap();
        let merged =
            Segments::from_parts(vec![Arc::new(crate::index::load(&output).unwrap())], vec![0]);
        assert_eq!(
            merged.aggregate(&QuerySpec::default(), &aggs),
            expected,
            "local ordinal из исходного словаря выдан за ordinal слитого"
        );
        drop((segments, merged));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sparse_text_scoring_ignores_documents_without_tokens_before_and_after_merge() {
        let root = std::env::temp_dir().join(format!("svindb-sparse-score-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        let mut writer =
            Indexer::new(&root, vec!["body".into(), "other".into()], vec![1.0, 1.0], 2);
        for (id, body, other) in [
            ("a", "alpha", "x"),
            ("b", "alpha beta", "y"),
            ("c", "", "z"),
            ("d", "", ""),
            ("e", "", "z"),
        ] {
            writer.add(id, &[body, other]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&root).unwrap();
        assert_eq!(segments.field_scoring_populations().get(&0), Some(&Some(2)));
        let query = spec(0, &["alpha"], 10);
        let expected = (1.0f64 + 0.5 / 2.5).ln() * 2.2 / (1.0 + 1.2 * (0.25 + 0.75 / 1.5));
        let check = |segs: &Segments| {
            let found = segs.search(&query);
            assert_eq!(ids(segs, &found), ["a", "b"]);
            assert!(
                (found.hits[0].score - expected).abs() < 1e-12,
                "field N=2, avgdl=1.5: {} вместо {expected}",
                found.hits[0].score
            );
        };
        check(&segments);
        let refs: Vec<_> = segments.list.iter().map(Arc::as_ref).collect();
        let output = format!("{root}/merged");
        crate::merge::merge(&refs, &output).unwrap();
        let merged =
            Segments::from_parts(vec![Arc::new(crate::index::load(&output).unwrap())], vec![0]);
        check(&merged);
        drop((segments, merged));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corpus_scores_use_stable_field_ids_across_layouts() {
        let root = std::env::temp_dir().join(format!("svindb-field-scores-{}", std::process::id()));
        let build = |name: &str, permuted: bool| {
            let dir = root.join(name);
            for segment in 0..2 {
                let path = dir.join(format!("part{segment}"));
                let path = path.to_string_lossy().to_string();
                let swapped = permuted && segment == 1;
                let (names, boosts, fids) = if swapped {
                    (vec!["extra".into(), "body".into()], vec![1.0, 4.0], vec![9, 3])
                } else {
                    (vec!["body".into(), "extra".into()], vec![4.0, 1.0], vec![3, 9])
                };
                let mut ix = Indexer::new(&path, names, boosts, 1_000_000);
                ix.set_fids(fids);
                for n in 0..3 {
                    let body = "alpha ".repeat(n + segment + 1);
                    let extra = "beta ".repeat(20 + n * 10);
                    let fields = if swapped { [&*extra, &*body] } else { [&*body, &*extra] };
                    ix.add(&format!("{segment}/{n}"), &fields);
                }
                ix.flush_now();
                drop(ix);
                std::fs::rename(
                    dir.join(format!("part{segment}/seg0000")),
                    dir.join(format!("seg{segment:04}")),
                )
                .unwrap();
            }
            crate::manifest::write(&dir.to_string_lossy(), &["seg0000".into(), "seg0001".into()])
                .unwrap();
            Segments::load(&dir.to_string_lossy()).unwrap()
        };
        let canonical = build("canonical", false);
        let permuted = build("permuted", true);
        let query = QuerySpec {
            words: vec![SpecWord { fid: 3, terms: vec![Term::parse("alpha")], given: None }],
            k: 10,
            ..QuerySpec::default()
        };
        for global in [None, Some((6usize, vec![6u64]))] {
            let stats = global.as_ref().map(|(n, df)| (*n, df.as_slice()));
            let expected = canonical.search_with_g(&query, 1, stats);
            let actual = permuted.search_with_g(&query, 1, stats);
            assert_eq!(actual.hits.len(), 6);
            for (a, b) in actual.hits.iter().zip(&expected.hits) {
                assert_eq!(a.doc, b.doc, "layout изменил порядок при global={global:?}");
                assert_eq!(
                    a.score.to_bits(),
                    b.score.to_bits(),
                    "layout изменил score при global={global:?}"
                );
            }
        }
        drop((canonical, permuted));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn required_mask_preserves_corpus_scores_from_nonmatching_segments() {
        let dir = std::env::temp_dir().join(format!("svindb-must-score-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut indexer = Indexer::new(&dir, vec!["body".into()], vec![1.0], 2);
        for (id, text) in [("a", "alpha beta"), ("b", "alpha beta"), ("c", "alpha"), ("d", "alpha")]
        {
            indexer.add(id, &[text]);
        }
        indexer.flush_now();
        drop(indexer);
        let segments = Segments::load(&dir).expect("состав");
        let word = |text: &str| SpecWord { fid: 0, terms: vec![Term::parse(text)], given: None };
        let spec = QuerySpec { must: vec![word("alpha"), word("beta")], ..QuerySpec::default() };
        let full = QuerySpec {
            nested: Some(SpecBool {
                must: vec![SpecClause::Term(word("alpha")), SpecClause::Term(word("beta"))],
                ..SpecBool::default()
            }),
            ..QuerySpec::default()
        };
        let got = segments.search(&spec);
        let expected = segments.search(&full);
        assert_eq!(got.hits.len(), expected.hits.len());
        assert!(!got.hits.is_empty());
        for (a, b) in got.hits.iter().zip(&expected.hits) {
            assert_eq!(a.doc, b.doc);
            assert!(
                (a.score - b.score).abs() < 1e-12,
                "pruning изменил BM25: {} != {}",
                a.score,
                b.score
            );
        }
        let excluded = QuerySpec { must_not: vec![word("beta")], ..QuerySpec::default() };
        assert_eq!(
            segments.search(&excluded).hits.iter().map(|h| h.doc).collect::<Vec<_>>(),
            vec![2, 3],
            "отрицательный запрос потерял документы без положительного leading term"
        );
        let first = QuerySpec { k: 1, must_not: vec![word("beta")], ..QuerySpec::default() };
        let first_hit = segments.search(&first);
        assert_eq!(first_hit.hits[0].doc, 2);
        assert_eq!(first_hit.visited, 1, "равные последующие scores не требуют полного обхода");
        let count = [crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        assert!(matches!(
            segments.aggregate(&first, &count)[0].value,
            crate::aggs::Value::Stats { count: 2, .. }
        ));
        let absent_should = QuerySpec {
            nested: Some(SpecBool {
                should: vec![SpecClause::Term(word("missing"))],
                ..SpecBool::default()
            }),
            ..QuerySpec::default()
        };
        assert!(segments.search(&absent_should).hits.is_empty(), "should(missing) стал match_all");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn required_literal_mask_skips_every_segment_without_losing_df_shape() {
        let dir = std::env::temp_dir().join("svindb-required-term-directory");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("каталог");
        let dir = dir.to_string_lossy().to_string();
        let mut indexer = Indexer::new(&dir, vec!["текст".into()], vec![1.0], 1);
        for segment in 0..4 {
            indexer.add(&format!("док{segment}"), &[&format!("обычное{segment}")]);
        }
        indexer.flush_now();

        let segments = Segments::load(&dir).expect("состав");
        let spec = QuerySpec {
            must: vec![
                SpecWord {
                    fid: 0,
                    terms: vec![Term::parse("обязательное-слово-которого-нет")],
                    given: None,
                },
                SpecWord { fid: 0, terms: vec![Term::parse("обычное0")], given: None },
            ],
            k: 10,
            ..QuerySpec::default()
        };
        let plan = segments.plan_with(&spec, None);
        assert!(plan.iter().all(|request| request.skipped), "невозможный must дошёл до словарей");
        let (total, df) = segments.stats(&spec);
        assert_eq!(total, 4);
        assert_eq!(df, vec![0, 1, 0, 1], "skipped-сегмент потерял corpus df или структуру дерева");
        let outcome = segments.search(&spec);
        assert!(outcome.hits.is_empty(), "невозможный must дал выдачу");
        assert_eq!(outcome.visited, 0, "невозможный must начал обход документов");
        let count = [crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        let counted =
            segments.aggregate_guarded(&spec, &count, 4, &Arc::new(crate::work::Guard::new(1_000)));
        assert!(!counted.partial, "каталог обязательных слов неожиданно оборвал count");
        assert!(matches!(
            counted.values.first().map(|result| &result.value),
            Some(crate::aggs::Value::Stats { count: 0, .. })
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Пропуск сегмента по границам не теряет ни одного документа.
    ///
    /// Сверкой с полным обходом, а не набором частных случаев: пропуск —
    /// оптимизация, и единственное её обязательство в том, что выдача не
    /// изменилась. Ошибись граница на единицу — и документ пропадёт молча,
    /// потому что «не нашлось» выглядит как законный ответ.
    #[test]
    fn skipping_by_bounds_returns_exactly_what_the_full_walk_returns() {
        let dir = std::env::temp_dir().join("svindb-seg-bounds");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        // Сегменты по сто документов, значения времени растут вместе с номером
        // — так выглядит дописываемый корпус, ради которого пропуск и заведён.
        let mut ix = Indexer::typed(
            &dir,
            vec!["тело".into(), "время".into()],
            vec![1.0, 1.0],
            vec![crate::index::Kind::Text, crate::index::Kind::Int64],
            100,
        );
        for i in 0..1000u32 {
            ix.add(
                &format!("d{i}"),
                &["общее слово", &(1_700_000_000 + i as i64 * 60).to_string()],
            );
        }
        ix.flush_now();
        let segs = Segments::load(&dir).expect("состав");
        assert!(segs.list.len() >= 10, "нужно много сегментов: {}", segs.list.len());

        // Границы записались и различаются между сегментами — иначе проверка
        // проверяла бы пропуск, которого не происходит.
        let bounds: Vec<_> = segs
            .list
            .iter()
            .map(|s| s.fields.iter().find(|f| f.name == "время").expect("поле").bounds)
            .collect();
        assert!(bounds.iter().all(Option::is_some), "границы не записались: {bounds:?}");
        assert!(bounds[0] != bounds[1], "границы одинаковы — корпус не тот");

        let ask = |from: i64, to: i64| QuerySpec {
            words: vec![SpecWord { given: None, fid: 0, terms: vec![Term::parse("слово")] }],
            filter: vec![SpecRange { fid: 1, from, to }],
            k: 50,
            ..QuerySpec::default()
        };
        for (from, to) in [
            (0i64, i64::MAX),                                     // всё
            (1_700_000_000, 1_700_000_000),                       // ровно первый документ
            (1_700_000_000 + 500 * 60, i64::MAX),                 // хвост
            (1_700_000_000 + 250 * 60, 1_700_000_000 + 260 * 60), // середина
            (1, 2),                                               // ничего
        ] {
            let spec = ask(from, to);
            let with = segs.search(&spec);
            let ids: Vec<u64> = with.hits.iter().map(|h| h.doc).collect();
            // Тот же счёт вручную: сколько документов и правда попадает.
            let want = (0..1000u64)
                .filter(|i| {
                    let v = 1_700_000_000 + *i as i64 * 60;
                    v >= from && v <= to
                })
                .count()
                .min(50);
            assert_eq!(ids.len(), want, "отбор [{from}, {to}] отдал не то число");
            // Поток и очередь обязаны совпасть: правило пропуска у них одно, и
            // разойдись они — один и тот же запрос давал бы разную выдачу в
            // зависимости от того, сколько ядер оказалось свободно.
            let mut threaded: Vec<u64> =
                self_search(&segs, &spec, 8).hits.iter().map(|h| h.doc).collect();
            threaded.sort_unstable();
            let mut one = ids.clone();
            one.sort_unstable();
            assert_eq!(one, threaded, "поток и очередь разошлись на отборе [{from}, {to}]");
        }
        // Сортировка: пропуск по потолку обязан дать ровно ту же верхушку, что
        // и полный обход. Здесь ошибка была бы самой коварной — выдача не
        // пустеет, а тихо теряет лучшие документы из дальних сегментов.
        for desc in [true, false] {
            let sorted = QuerySpec {
                words: vec![SpecWord {
                    given: None, fid: 0, terms: vec![Term::parse("слово")]
                }],
                sort: vec![Sort { fid: 1, desc }],
                k: 20,
                ..QuerySpec::default()
            };
            let got: Vec<u64> = segs.search(&sorted).hits.iter().map(|h| h.doc).collect();
            // Ожидаемое считается независимо: при убывании — двадцать последних
            // документов, при возрастании — двадцать первых.
            let want: Vec<u64> =
                if desc { (980..1000u64).rev().collect() } else { (0..20u64).collect() };
            assert_eq!(got, want, "сортировка desc={desc} потеряла документы");
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Поиск в заданное число потоков — короткая запись для проверок.
    fn self_search(segs: &Segments, spec: &QuerySpec, threads: usize) -> Outcome {
        segs.search_with_g(spec, threads, None)
    }

    /// Строит корпус и отдаёт каталог. `per_seg` задаёт разбиение на сегменты.
    fn corpus(name: &str, per_seg: u32) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-seg-{name}"));
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix =
            Indexer::new(&dir, vec!["title".into(), "body".into()], vec![2.0, 1.0], per_seg);
        // «редкое» встречается один раз, «общее» — у всех: так проверяется и
        // отбор по частоте, и перенос порога между сегментами.
        for i in 0..40u32 {
            let title =
                if i == 7 { "редкое слово".to_string() } else { format!("док {i}") };
            let body = format!("общее тело номер {i} домик{}", i % 3);
            ix.add(&format!("d{i}"), &[&title, &body]);
        }
        ix.flush();
        dir
    }

    fn spec(field: usize, terms: &[&str], k: usize) -> QuerySpec {
        QuerySpec {
            words: vec![SpecWord {
                given: None,
                fid: field,
                terms: terms.iter().map(|t| Term::parse(t)).collect(),
            }],
            k,
            ..QuerySpec::default()
        }
    }

    /// Идентификаторы найденных документов — по сегменту и номеру внутри него.
    ///
    /// Источник берётся у самого сегмента, а не собирается заново по имени
    /// каталога. У упакованного сегмента `ids.bin` отдельным файлом не лежит, и
    /// поиск файлом отдал бы пустую полосу вместо содержимого: сравнение двух
    /// пустых списков прошло бы, ничего не проверив.
    fn ids(segs: &Segments, o: &Outcome) -> Vec<String> {
        o.hits
            .iter()
            .map(|h| {
                let ix = &segs.list[h.segment];
                crate::ids::Ids::open(&ix.src, ix.docs).get(h.local as usize).unwrap_or_default()
            })
            .collect()
    }

    /// Селективный запрос находит тот же документ при любом разбиении.
    ///
    /// Это и есть условие модели сегментов: как корпус нарезан — дело приёма, а
    /// не выдачи. Проверяется на запросе без ничьих; про запросы с ничьими —
    /// следующий тест, там разбиение видно, и это не дефект.
    #[test]
    fn selective_query_is_independent_of_segmentation() {
        let one = corpus("one", 1000);
        let many = corpus("many", 7);
        let a = Segments::load(&one).unwrap();
        let b = Segments::load(&many).unwrap();
        assert_eq!(a.list.len(), 1);
        assert_eq!(b.list.len(), 6, "40 документов по 7 на сегмент");
        assert_eq!(a.docs(), b.docs());

        for q in [spec(0, &["редкое"], 10), spec(1, &["домик0"], 100)] {
            let ha = a.search(&q);
            let hb = b.search(&q);
            let mut ia = ids(&a, &ha);
            let mut ib = ids(&b, &hb);
            assert!(!ia.is_empty(), "выдача пуста: сравнивать нечего");
            assert!(ia.iter().all(|s| !s.is_empty()), "идентификаторы не прочлись: {ia:?}");
            ia.sort();
            ib.sort();
            assert_eq!(ia, ib, "состав выдачи");
        }
        std::fs::remove_dir_all(&one).ok();
        std::fs::remove_dir_all(&many).ok();
    }

    /// Общее пространство номеров кончается отказом, а не заворачивается.
    #[test]
    fn the_document_numbering_runs_out_loudly() {
        // Прежний потолок — четыре с четвертью миллиарда — больше не потолок.
        // Сто миллиардов документов сегментами по пределу настроек: две тысячи
        // сегментов, и это законная опись.
        assert!(bases(&[50_000_000; 2_000]).is_ok(), "сто миллиардов обязаны влезать");

        // Границы вплотную: ровно потолок — можно, потолок плюс один — нельзя.
        assert!(bases(&[MAX_DOCS as usize]).is_ok());
        assert!(bases(&[MAX_DOCS as usize, 1]).is_err());
        let e = bases(&[MAX_DOCS as usize, 1]).expect_err("за потолком отказ");
        assert!(e.contains('1'), "отказ обязан назвать сегмент: {e}");

        // Сложение не переполняется даже на заведомо невозможных размерах:
        // отказ, а не оборот через ноль.
        assert!(bases(&[usize::MAX, usize::MAX]).is_err());
        assert_eq!(bases(&[3, 5, 7]).unwrap(), vec![0, 3, 8]);
        assert!(bases(&[]).unwrap().is_empty());
    }

    /// Булев запрос в один сегмент и в потоках отвечает одинаково.
    ///
    /// Это не про скорость. Внутрисегментное деление знает только `words`;
    /// обязательные, запрещённые, отбор и фразы до него не доходят. Булев
    /// запрос, ушедший туда, отвечал **пустотой** — молча, без отказа.
    ///
    /// Ловится это только тремя условиями сразу: один сегмент, больше одного
    /// потока и запрос дороже сторожа `PARALLEL_POSTINGS`. Прежние булевы тесты
    /// брали корпус на сорок документов и ни одного из трёх не выполняли.
    #[test]
    fn a_boolean_query_survives_one_big_segment() {
        let dir = std::env::temp_dir().join("svindb-seg-bool-one").to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        // Один сегмент, и списки заведомо длиннее сторожа: «общее» у всех.
        let n = (PARALLEL_POSTINGS as u32) + 5_000;
        let mut ix = Indexer::new(&dir, vec!["title".into(), "body".into()], vec![2.0, 1.0], n * 2);
        for i in 0..n {
            let body = if i % 3 == 0 { "общее метка" } else { "общее" };
            ix.add(&format!("d{i}"), &[&format!("док {i}"), body]);
        }
        ix.flush();
        let segs = Segments::load(&dir).unwrap();
        assert_eq!(segs.list.len(), 1, "нужен ровно один сегмент");

        let base = segs.search_with(&spec(1, &["общее"], 10), 1).hits.len();
        assert_eq!(base, 10, "обычный запрос обязан находиться");

        // Каждая булева ветвь порознь: у каждой свой способ потеряться.
        let mut q_must = spec(1, &["общее"], 10);
        q_must.must = std::mem::take(&mut q_must.words);
        let mut q_not = spec(1, &["общее"], 10);
        q_not.must_not =
            vec![SpecWord { given: None, fid: 1, terms: vec![Term::parse("метка")] }];
        for (name, q) in [("must", &q_must), ("must_not", &q_not)] {
            let one = segs.search_with(q, 1);
            let many = segs.search_with(q, 8);
            assert!(!one.hits.is_empty(), "{name}: последовательно пусто — сломан сам обход");
            assert_eq!(
                many.hits.len(),
                one.hits.len(),
                "{name}: в потоках выдача разошлась с последовательной"
            );
            for (a, b) in many.hits.iter().zip(one.hits.iter()) {
                assert_eq!(a.doc, b.doc, "{name}: порядок разошёлся");
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Нумерация за прежним потолком работает целиком, а не только в `bases`.
    ///
    /// Расширение номера до восьми байт легко сделать наполовину: расширить тип
    /// и оставить где-нибудь `as u32`. Такое усечение не падает — оно молча
    /// отдаёт **чужой документ**, и заметить это на корпусе, который в четыре
    /// миллиарда влезает, невозможно. Поэтому здесь берётся настоящий индекс, а
    /// его нумерация сдвигается за старый потолок вручную: поиск, выдача
    /// содержимого и курсор страницы обязаны согласиться друг с другом.
    #[test]
    fn numbering_past_the_old_ceiling_works_end_to_end() {
        let dir = corpus("past-u32", 7);
        let mut segs = Segments::load(&dir).unwrap();
        assert!(segs.list.len() > 1, "нужен многосегментный индекс");

        // Сдвиг ровно на прежний потолок: всякий уцелевший `as u32` обрежет
        // общий номер обратно к местному, и это будет видно.
        const SHIFT: u64 = u32::MAX as u64 + 1;
        let plain = segs.base.clone();
        for b in segs.base.iter_mut() {
            *b += SHIFT;
        }

        let q = spec(1, &["общее"], 40);
        let out = segs.search(&q);
        assert_eq!(out.hits.len(), 40, "найтись обязаны все сорок");
        for h in out.hits.iter() {
            assert!(h.doc >= SHIFT, "номер обрезан до четырёх байт: {}", h.doc);
            assert_eq!(
                h.doc,
                segs.base[h.segment] + h.local as u64,
                "общий номер разошёлся с парой «сегмент, местный»"
            );
        }

        // Содержимое достаётся по общему номеру — тем же, что вернул поиск.
        let want: Vec<String> = {
            let mut f = segs.fetcher(8);
            out.hits
                .iter()
                .map(|h| String::from_utf8_lossy(f.get(h.doc, 1).unwrap()).into_owned())
                .collect()
        };
        // Сдвиг обязан быть ровно переносом: без него — те же тела в том же
        // порядке. Это отделяет «номер поехал» от «поиск поехал».
        segs.base = plain;
        let mut f = segs.fetcher(8);
        for (h, body) in out.hits.iter().zip(want.iter()) {
            let at = segs.base[h.segment] + h.local as u64;
            assert_eq!(String::from_utf8_lossy(f.get(at, 1).unwrap()), *body);
        }
    }

    /// Скор — свойство корпуса, а не того, как приём его нарезал.
    ///
    /// ## Почему соседний тест этого не ловил
    ///
    /// В его корпусе тело каждого документа ровно пятисловное, поэтому средняя
    /// длина у любой раскладки одна и та же, и проверять там нечего. Здесь
    /// длины расходятся на два порядка, а документы **кладутся по возрастанию
    /// длины** — так посегментные средние расходятся сильнее всего: при семи
    /// сегментах в первый попадают только короткие, в последний только длинные.
    /// При случайном порядке средние сходятся, и проверка снова слепнет.
    ///
    /// ## Что сверяется
    ///
    /// Идентификаторы, порядок и вес. Порядок берётся по внешнему
    /// идентификатору, а не по номеру документа: номер зависит от раскладки, и
    /// сравнивать по нему значило бы сверять раскладку саму с собой.
    #[test]
    fn the_score_belongs_to_the_corpus_not_to_its_slicing() {
        fn build(name: &str, per_seg: u32) -> String {
            let dir = std::env::temp_dir()
                .join(format!("svindb-layout-{name}-{}", std::process::id()))
                .to_string_lossy()
                .to_string();
            std::fs::remove_dir_all(&dir).ok();
            let mut ix =
                Indexer::new(&dir, vec!["title".into(), "body".into()], vec![2.0, 1.0], per_seg);
            // Длины от одного слова до трёхсот, по возрастанию.
            for i in 0..400u32 {
                let len = 1 + (i as usize * 3) / 4;
                let mut body = String::new();
                for k in 0..len {
                    body.push_str(if k % 7 == 0 { "общее " } else { "шум " });
                }
                if i % 37 == 0 {
                    body.push_str("редкое ");
                }
                ix.add(&format!("d{i:04}"), &[&format!("док {i}"), &body]);
            }
            ix.flush();
            dir
        }

        // Раскладки: одним сегментом, парами, семью, девятнадцатью и
        // двадцатью тремя документами на сегмент.
        let layouts: Vec<(u32, String)> = [1000u32, 2, 7, 19, 23]
            .iter()
            .map(|&per| (per, build(&format!("per{per}"), per)))
            .collect();
        let opened: Vec<(u32, Segments)> =
            layouts.iter().map(|(per, d)| (*per, Segments::load(d).unwrap())).collect();
        assert_eq!(opened[0].1.list.len(), 1, "первая раскладка обязана быть односегментной");
        assert!(opened.last().unwrap().1.list.len() > 10, "последняя обязана дробиться");

        // Ответ каждой раскладки: (внешний идентификатор, вес), упорядоченный
        // независимо от раскладки.
        let answer = |segs: &Segments, word: &str| -> Vec<(String, f64)> {
            let q = spec(1, &[word], 25);
            let out = segs.search(&q);
            let mut v: Vec<(String, f64)> =
                ids(segs, &out).into_iter().zip(out.hits.iter().map(|h| h.score)).collect();
            v.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            v
        };

        for word in ["общее", "редкое", "шум"] {
            let base = answer(&opened[0].1, word);
            assert!(!base.is_empty(), "слово {word:?} не нашлось вовсе");
            for (per, segs) in opened.iter().skip(1) {
                let got = answer(segs, word);
                let names = |v: &[(String, f64)]| -> Vec<String> {
                    v.iter().map(|(s, _)| s.clone()).collect()
                };
                assert_eq!(
                    names(&got),
                    names(&base),
                    "слово {word:?}: при {per} документах на сегмент выдача другая"
                );
                for ((ia, sa), (ib, sb)) in got.iter().zip(base.iter()) {
                    assert!(
                        (sa - sb).abs() < 1e-9,
                        "слово {word:?}, документ {ia}/{ib}: вес {sa} против {sb} \
                         при {per} документах на сегмент"
                    );
                }
            }
        }
        for (_, d) in layouts.iter() {
            std::fs::remove_dir_all(d).ok();
        }
    }

    /// Быстрый путь одного слова не теряет документы на разнодлинном составе.
    ///
    /// ## Чем этот тест отличается от соседних
    ///
    /// Соседние сверяют раскладки, у которых сегменты похожи по длинам, и до
    /// дефекта не доходят. Здесь состав нарочно разнороден: сначала сегмент из
    /// однословных документов, потом сегмент из документов по три сотни слов.
    /// Общая средняя длина при этом много выше собственной средней первого
    /// сегмента, а потолок блока записан при сборке по **его** средней. Значит
    /// настоящий вклад его документов выше записанного потолка, и разницу
    /// покрывает множитель подъёма.
    ///
    /// Без подъёма сравнение с порогом срабатывает раньше времени, и `break`
    /// закрывает остаток списка вместе с документами, которым место в верхушке.
    /// Пропажа молчаливая: ответ приходит успешным, просто короче правды.
    ///
    /// Эталон — тот же корпус одним сегментом: у него общая средняя совпадает с
    /// собственной, подъём равен единице, и терять ему нечего.
    #[test]
    fn the_single_word_fast_path_keeps_what_the_short_segment_deserves() {
        fn build(name: &str, split: bool) -> String {
            let dir = std::env::temp_dir()
                .join(format!("svindb-lift-{name}-{}", std::process::id()))
                .to_string_lossy()
                .to_string();
            std::fs::remove_dir_all(&dir).ok();
            let mut ix = Indexer::new(&dir, vec!["body".into()], vec![1.0], u32::MAX);
            // Восемь блоков постингов по сто двадцать восемь документов: без
            // нескольких блоков отсечение по потолку блока не срабатывает
            // вовсе, и проверять было бы нечего.
            //
            // Длины все разные и перемешаны множителем, взаимно простым с
            // числом документов: короткие — то есть лучшие — рассыпаны по всем
            // блокам, а не собраны в первом. Тогда порог поднимается на первом
            // блоке, а лучшие документы остаются в остальных, и отсечение
            // обязано их всё-таки прочитать.
            const N: u32 = 1024;
            for j in 0..N {
                let len = 1 + (j as u64 * 401) % N as u64;
                let mut body = String::from("цель");
                for _ in 1..len {
                    body.push_str(" шум");
                }
                ix.add(&format!("к{j:04}"), &[&body]);
            }
            if split {
                ix.flush();
            }
            // Длинные документы поднимают общую среднюю длину. Из-за них
            // собственная средняя первого сегмента оказывается много ниже
            // общей, а его потолки блоков — ниже настоящего вклада.
            let mut long = String::from("цель");
            for _ in 0..4000 {
                long.push_str(" прочее");
            }
            for i in 0..200u32 {
                ix.add(&format!("д{i:04}"), &[&long]);
            }
            ix.flush_now();
            dir
        }

        let one = build("one", false);
        let two = build("two", true);
        let a = Segments::load(&one).unwrap();
        let b = Segments::load(&two).unwrap();
        assert_eq!(a.list.len(), 1, "эталон обязан быть односегментным");
        assert_eq!(b.list.len(), 2, "проверяемый состав обязан быть двухсегментным");

        for k in [10usize, 50, 100] {
            let q = spec(0, &["цель"], k);
            let want = ids(&a, &a.search(&q));
            let got = ids(&b, &b.search(&q));
            assert_eq!(want.len(), k, "эталон не набрал верхушку при k={k}");
            assert_eq!(
                got, want,
                "k={k}: разнодлинный состав отдал другую верхушку — \
                 быстрый путь одного слова потерял документы"
            );
        }
        std::fs::remove_dir_all(&one).ok();
        std::fs::remove_dir_all(&two).ok();
    }

    /// Вес слова не зависит от того, как приём нарезал корпус.
    #[test]
    fn ranking_does_not_depend_on_segmentation() {
        let one = corpus("idf-one", 1000);
        let many = corpus("idf-many", 7);
        let a = Segments::load(&one).unwrap();
        let b = Segments::load(&many).unwrap();
        assert_eq!(a.list.len(), 1);
        assert!(b.list.len() > 1, "второй корпус обязан разбиться на сегменты");

        let q = spec(1, &["общее"], 10);
        let ha = a.search(&q);
        let hb = b.search(&q);
        let (ia, ib) = (ids(&a, &ha), ids(&b, &hb));
        assert_eq!(ia.len(), 10, "десятка обязана набраться: {ia:?}");
        assert!(ia.iter().all(|s| !s.is_empty()), "идентификаторы не прочлись: {ia:?}");
        assert_eq!(ia, ib, "состав и порядок десятки");
        for (x, y) in ha.hits.iter().zip(hb.hits.iter()) {
            assert!(
                (x.score - y.score).abs() < 1e-12,
                "вес одного и того же слова разошёлся: {} против {}",
                x.score,
                y.score
            );
        }
        std::fs::remove_dir_all(&one).ok();
        std::fs::remove_dir_all(&many).ok();
    }

    /// Порог переносится между сегментами и не режет верхушку.
    #[test]
    fn shared_floor_keeps_the_top_full() {
        let dir = corpus("floor", 7);
        let s = Segments::load(&dir).unwrap();
        let out = s.search(&spec(1, &["общее"], 10));
        assert_eq!(out.hits.len(), 10, "верхушка обязана набраться целиком");
        for w in out.hits.windows(2) {
            assert!(w[0].score >= w[1].score, "выдача не по убыванию скора");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Маска раскрывается по словарю и находит все формы.
    #[test]
    fn prefix_expands() {
        let dir = corpus("prefix", 9);
        let s = Segments::load(&dir).unwrap();
        let all = s.search(&spec(1, &["домик*"], 100));
        let exact = s.search(&spec(1, &["домик0"], 100));
        assert_eq!(all.hits.len(), 40, "маска берёт все три формы");
        assert!(exact.hits.len() < all.hits.len());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отсутствующее слово даёт пустую выдачу, а не ошибку.
    #[test]
    fn missing_term_is_empty() {
        let dir = corpus("missing", 9);
        let s = Segments::load(&dir).unwrap();
        assert!(s.search(&spec(1, &["такогонет"], 10)).hits.is_empty());
        assert!(s.search(&spec(1, &["такогонет*"], 10)).hits.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod filter_tests {
    use super::*;
    use crate::index::Kind;

    /// Отбор обязан дать ровно то же, что поиск без него плюс ручная проверка.
    ///
    /// Это не проверка колонки — её проверяет `column.rs`. Это проверка того,
    /// что условие доехало от запроса до обхода и применилось к тем документам,
    /// к которым относится: поле, номер и сегмент легко перепутать местами, и
    /// ни одна из перестановок не уронит ни один другой тест.
    #[test]
    fn filtering_keeps_exactly_what_an_honest_check_keeps() {
        let dir = std::env::temp_dir().join("svindb-filter-search");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();

        let mut ix = crate::indexer::Indexer::typed(
            &d,
            vec!["body".into(), "цена".into()],
            vec![1.0, 1.0],
            vec![Kind::Text, Kind::Int64],
            60,
        );
        for i in 0..200i64 {
            // Каждый десятый без цены — он не обязан попасть ни в один диапазон.
            let price = if i % 10 == 0 { String::new() } else { format!("{}", i * 7 % 300) };
            ix.add(&format!("д{i}"), &["общее слово", &price]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let segs = Segments::load(&d).unwrap();

        let spec = |from: i64, to: i64| QuerySpec {
            words: vec![SpecWord {
                given: None,
                fid: 0,
                terms: vec![Term { text: "общее".into(), prefix: false }],
            }],
            filter: vec![SpecRange { fid: 1, from, to }],
            must: Vec::new(),
            must_not: Vec::new(),
            nested: None,
            min_should: 0,
            sort: Vec::new(),
            from: 0,
            after: None,
            k: 500,
            track_total_hits: TrackTotalHits::default(),
            max_expansions: DEFAULT_EXPANSIONS,
            max_expansion_examined: DEFAULT_EXPANSION_EXAMINED,
            fields: Vec::new(),
            phrases: Vec::new(),
            highlight: Vec::new(),
            highlight_size: 150,
            with_ids: false,
            translit: false,
            max_verify: DEFAULT_VERIFY,
            max_agg_ms: DEFAULT_AGG_MS,
            guard: None,
            live: None,
            expansion: None,
        };

        for (from, to) in [(0i64, 299i64), (0, 0), (50, 100), (299, 299), (1000, 2000)] {
            let got = segs.search(&spec(from, to));
            // Честная проверка: у скольких документов цена есть и в диапазоне.
            let want = (0..200i64)
                .filter(|i| i % 10 != 0)
                .filter(|i| {
                    let p = i * 7 % 300;
                    p >= from && p <= to
                })
                .count();
            assert_eq!(got.hits.len(), want, "диапазон [{from}, {to}]");
        }

        // Без условия находятся все двести.
        let all = QuerySpec {
            words: vec![SpecWord {
                given: None,
                fid: 0,
                terms: vec![Term { text: "общее".into(), prefix: false }],
            }],
            filter: Vec::new(),
            must: Vec::new(),
            must_not: Vec::new(),
            nested: None,
            min_should: 0,
            sort: Vec::new(),
            from: 0,
            after: None,
            k: 500,
            track_total_hits: TrackTotalHits::default(),
            max_expansions: DEFAULT_EXPANSIONS,
            max_expansion_examined: DEFAULT_EXPANSION_EXAMINED,
            fields: Vec::new(),
            phrases: Vec::new(),
            highlight: Vec::new(),
            highlight_size: 150,
            with_ids: false,
            translit: false,
            max_verify: DEFAULT_VERIFY,
            max_agg_ms: DEFAULT_AGG_MS,
            guard: None,
            live: None,
            expansion: None,
        };
        assert_eq!(segs.search(&all).hits.len(), 200);

        // Пустая булева ветвь — внутреннее представление явного match_all.
        // Она перечисляет живые документы, а не проваливается в «нет слов».
        let match_all =
            QuerySpec { nested: Some(SpecBool::default()), k: 25, ..QuerySpec::default() };
        let got = segs.search(&match_all);
        assert_eq!(got.hits.len(), 25);
        assert_eq!(got.hits[0].doc, 0);
        assert_eq!(got.hits[24].doc, 24);

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod language_tests {
    use super::*;
    use crate::index::Kind;

    fn build(name: &str) -> Segments {
        let dir = std::env::temp_dir().join(format!("svindb-lang-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();
        let mut ix = crate::indexer::Indexer::typed(
            &d,
            vec!["body".into(), "ts".into()],
            vec![1.0, 1.0],
            vec![Kind::Text, Kind::Int64],
            40,
        );
        for i in 0..300i64 {
            let mut w = vec!["общее".to_string()];
            if i % 3 == 0 {
                w.push("красный".into());
            }
            if i % 5 == 0 {
                w.push("синий".into());
            }
            ix.add(&format!("д{i}"), &[&w.join(" "), &format!("{}", 1000 - i)]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        Segments::load(&d).unwrap()
    }

    fn word(t: &str) -> SpecWord {
        SpecWord { fid: 0, given: None, terms: vec![Term { text: t.into(), prefix: false }] }
    }

    fn spec(must: Vec<SpecWord>, should: Vec<SpecWord>, not: Vec<SpecWord>) -> QuerySpec {
        QuerySpec {
            words: should,
            must,
            must_not: not,
            nested: None,
            min_should: 0,
            sort: Vec::new(),
            from: 0,
            after: None,
            filter: Vec::new(),
            k: 500,
            track_total_hits: TrackTotalHits::default(),
            max_expansions: DEFAULT_EXPANSIONS,
            max_expansion_examined: DEFAULT_EXPANSION_EXAMINED,
            fields: Vec::new(),
            phrases: Vec::new(),
            highlight: Vec::new(),
            highlight_size: 150,
            with_ids: false,
            translit: false,
            max_verify: DEFAULT_VERIFY,
            max_agg_ms: DEFAULT_AGG_MS,
            guard: None,
            live: None,
            expansion: None,
        }
    }

    /// «И», «или» и «не» обязаны дать ровно то же, что честный перебор.
    #[test]
    fn boolean_matches_an_honest_enumeration() {
        let segs = build("bool");
        let want = |f: &dyn Fn(i64) -> bool| (0..300i64).filter(|&i| f(i)).count();

        assert_eq!(
            segs.search(&spec(vec![], vec![word("красный")], vec![])).hits.len(),
            want(&|i| i % 3 == 0),
            "или"
        );
        assert_eq!(
            segs.search(&spec(vec![word("красный"), word("синий")], vec![], vec![])).hits.len(),
            want(&|i| i % 15 == 0),
            "и"
        );
        assert_eq!(
            segs.search(&spec(vec![word("красный")], vec![], vec![word("синий")])).hits.len(),
            want(&|i| i % 3 == 0 && i % 5 != 0),
            "и не"
        );
        // Обязательного слова нет в словаре — совпасть не может никто.
        assert_eq!(segs.search(&spec(vec![word("нетакого")], vec![], vec![])).hits.len(), 0);
    }

    /// Сортировка по полю совпадает с сортировкой собранного вручную списка.
    #[test]
    fn sorting_matches_a_hand_sorted_list() {
        let segs = build("sort");
        for desc in [false, true] {
            let mut s = spec(vec![word("красный")], vec![], vec![]);
            s.sort = vec![Sort { fid: 1, desc }];
            s.k = 5;
            let got: Vec<i64> = segs
                .search(&s)
                .hits
                .iter()
                .map(|h| search::denorm_key(h.keys[0], desc).expect("число присутствует"))
                .collect();

            let mut want: Vec<i64> = (0..300i64).filter(|i| i % 3 == 0).map(|i| 1000 - i).collect();
            want.sort();
            if desc {
                want.reverse();
            }
            want.truncate(5);
            assert_eq!(got, want, "сортировка desc={desc}");
        }
    }

    /// Курсор страницы отдаёт ровно то же, что срез той же глубины.
    #[test]
    fn the_cursor_walks_the_same_list_as_paging() {
        let segs = build("after");
        let page = |after: Option<(Vec<search::SortKey>, u64)>| {
            let mut s = spec(vec![word("красный")], vec![], vec![]);
            s.sort = vec![Sort { fid: 1, desc: true }];
            s.k = 7;
            s.after = after;
            segs.search(&s).hits.iter().map(|h| (h.keys[0], h.doc)).collect::<Vec<_>>()
        };
        let first = page(None);
        let last = *first.last().expect("страница не пуста");
        let second = page(Some((vec![last.0], last.1)));

        let mut whole = spec(vec![word("красный")], vec![], vec![]);
        whole.sort = vec![Sort { fid: 1, desc: true }];
        whole.k = 14;
        let all: Vec<(search::SortKey, u64)> =
            segs.search(&whole).hits.iter().map(|h| (h.keys[0], h.doc)).collect();

        assert_eq!(first, all[..7], "первая страница");
        assert_eq!(second, all[7..14], "вторая страница через курсор");
    }
}

#[cfg(test)]
mod filter_lead_tests {
    use super::*;
    use crate::index::Kind;

    /// Корпус: слово у всех, «редкое» у каждого сотого, цена случайна.
    fn build(name: &str) -> (Segments, Vec<i64>) {
        let dir = std::env::temp_dir().join(format!("svindb-lead-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();
        let mut ix = crate::indexer::Indexer::typed(
            &d,
            vec!["body".into(), "цена".into()],
            vec![1.0, 1.0],
            vec![Kind::Text, Kind::Int64],
            700,
        );
        let mut seed = 4242u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut prices = Vec::with_capacity(2000);
        for i in 0..2000 {
            let p = (next() % 10_000) as i64;
            prices.push(p);
            let mut w = vec!["общее".to_string()];
            if i % 100 == 0 {
                w.push("редкое".into());
            }
            ix.add(&format!("д{i}"), &[&w.join(" "), &p.to_string()]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        (Segments::load(&d).unwrap(), prices)
    }

    fn spec(words: Vec<SpecWord>, from: i64, to: i64) -> QuerySpec {
        QuerySpec {
            words,
            must: Vec::new(),
            must_not: Vec::new(),
            nested: None,
            min_should: 0,
            sort: Vec::new(),
            from: 0,
            after: None,
            filter: vec![SpecRange { fid: 1, from, to }],
            k: 3000,
            track_total_hits: TrackTotalHits::default(),
            max_expansions: DEFAULT_EXPANSIONS,
            max_expansion_examined: DEFAULT_EXPANSION_EXAMINED,
            fields: Vec::new(),
            phrases: Vec::new(),
            highlight: Vec::new(),
            highlight_size: 150,
            with_ids: false,
            translit: false,
            max_verify: DEFAULT_VERIFY,
            max_agg_ms: DEFAULT_AGG_MS,
            guard: None,
            live: None,
            expansion: None,
        }
    }

    fn word(t: &str) -> SpecWord {
        SpecWord { fid: 0, given: None, terms: vec![Term { text: t.into(), prefix: false }] }
    }

    /// Кто бы ни вёл обход, выдача обязана быть той же.
    ///
    /// Это главное свойство: выбор ведущего — вопрос скорости, и если он
    /// меняет **ответ**, то он неверен. Диапазоны берутся и узкие, и широкие,
    /// чтобы ведущим успели побывать и отбор, и слово.
    #[test]
    fn the_answer_does_not_depend_on_who_leads() {
        let (segs, prices) = build("same");
        for (from, to) in
            [(0i64, 50i64), (0, 1000), (0, 5000), (0, 9999), (5000, 5000), (20_000, 30_000)]
        {
            let want = prices.iter().filter(|&&p| p >= from && p <= to).count();
            assert_eq!(
                segs.search(&spec(vec![word("общее")], from, to)).hits.len(),
                want,
                "слово у всех, диапазон [{from}, {to}]"
            );
            // То же без слов вовсе: отбор ведёт по построению.
            assert_eq!(
                segs.search(&spec(Vec::new(), from, to)).hits.len(),
                want,
                "без слов, диапазон [{from}, {to}]"
            );
            // И с редким словом, где ведущим станет слово.
            let want_rare = (0..2000usize)
                .filter(|i| i % 100 == 0 && prices[*i] >= from && prices[*i] <= to)
                .count();
            assert_eq!(
                segs.search(&spec(vec![word("редкое")], from, to)).hits.len(),
                want_rare,
                "редкое слово, диапазон [{from}, {to}]"
            );
        }
    }

    /// Узкий отбор обязан вести: просмотрено столько, сколько в отборе.
    ///
    /// Без этого широкое слово просматривало бы весь свой список и отбрасывало
    /// почти всё просмотренное — ровно то, ради чего ведущий и выбирается.
    #[test]
    fn a_narrow_filter_takes_the_lead_from_a_wide_word() {
        let (segs, prices) = build("lead");
        let narrow = segs.search(&spec(vec![word("общее")], 0, 50));
        let in_range = prices.iter().filter(|&&p| p <= 50).count();
        assert!(
            narrow.visited <= (in_range as u64) * 2,
            "узкий отбор обязан вести: просмотрено {} при {in_range} в отборе",
            narrow.visited
        );

        // А широкий — уступить слову, и просмотр не должен взлететь.
        let wide = segs.search(&spec(vec![word("редкое")], 0, 9999));
        assert!(wide.visited <= 30, "редкое слово ведёт само: просмотрено {}", wide.visited);
    }
}

#[cfg(test)]
mod nesting_tests {
    use super::*;

    /// Корпус, где у каждого документа свой набор из трёх признаков.
    ///
    /// Свой каталог на каждый тест: тесты гоняются параллельно, а список
    /// сегментов один на каталог — общий каталог уже давал гонку.
    fn build(name: &str) -> Segments {
        let dir = std::env::temp_dir().join(format!("svindb-nesting-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 200);
        for i in 0..800usize {
            let mut w = vec!["общее".to_string()];
            if i & 1 != 0 {
                w.push("москва".into());
            }
            if i & 2 != 0 {
                w.push("квартира".into());
            }
            if i & 4 != 0 {
                w.push("дом".into());
            }
            if i & 8 != 0 {
                w.push("аренда".into());
            }
            ix.add(&format!("д{i}"), &[&w.join(" ")]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        Segments::load(&d).unwrap()
    }

    fn t(w: &str) -> SpecClause {
        SpecClause::Term(SpecWord {
            given: None,
            fid: 0,
            terms: vec![Term { text: w.into(), prefix: false }],
        })
    }

    fn run(segs: &Segments, root: SpecBool) -> usize {
        let spec = QuerySpec { nested: Some(root), k: 1000, ..QuerySpec::default() };
        segs.search(&spec).hits.len()
    }

    /// «Москва И (квартира ИЛИ дом) И НЕ аренда» — то, что плоскими списками
    /// выразить нельзя вовсе: «или» внутри «и» им негде поставить.
    #[test]
    fn or_inside_and_is_expressible_and_correct() {
        let segs = build("orand");
        let got = run(
            &segs,
            SpecBool {
                must: vec![
                    t("москва"),
                    SpecClause::Bool(SpecBool {
                        should: vec![t("квартира"), t("дом")],
                        ..Default::default()
                    }),
                ],
                must_not: vec![t("аренда")],
                ..Default::default()
            },
        );
        let want = (0..800usize)
            .filter(|i| i & 1 != 0 && (i & 2 != 0 || i & 4 != 0) && i & 8 == 0)
            .count();
        assert_eq!(got, want);
    }

    /// Порог совпадений: «хотя бы два признака из трёх».
    #[test]
    fn minimum_should_match_counts_what_it_says() {
        let segs = build("minshould");
        for need in 1..=3usize {
            let got = run(
                &segs,
                SpecBool {
                    should: vec![t("москва"), t("квартира"), t("дом")],
                    min_should: need,
                    ..Default::default()
                },
            );
            let want = (0..800usize)
                .filter(|i| {
                    let hits =
                        (i & 1 != 0) as usize + (i & 2 != 0) as usize + (i & 4 != 0) as usize;
                    hits >= need
                })
                .count();
            assert_eq!(got, want, "порог {need}");
        }
    }

    /// Вложенность глубже одного уровня не должна ломаться.
    #[test]
    fn nesting_survives_more_than_one_level() {
        let segs = build("deep");
        let got = run(
            &segs,
            SpecBool {
                must: vec![SpecClause::Bool(SpecBool {
                    must: vec![t("общее")],
                    should: vec![
                        SpecClause::Bool(SpecBool {
                            must: vec![t("москва"), t("квартира")],
                            ..Default::default()
                        }),
                        t("дом"),
                    ],
                    min_should: 1,
                    ..Default::default()
                })],
                ..Default::default()
            },
        );
        let want = (0..800usize).filter(|i| (i & 1 != 0 && i & 2 != 0) || i & 4 != 0).count();
        assert_eq!(got, want);
    }
}

#[cfg(test)]
mod early_stop_tests {
    use super::*;
    use crate::index::Kind;

    /// Два числовых поля: одно растёт с приёмом, другое случайно. Первое идёт
    /// без корзин (границы блоков отсекают), второе с корзинами.
    fn build(name: &str) -> (Segments, Vec<(i64, i64)>) {
        let dir = std::env::temp_dir().join(format!("svindb-stop-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();
        let mut ix = crate::indexer::Indexer::typed(
            &d,
            vec!["body".into(), "дата".into(), "цена".into()],
            vec![1.0, 1.0, 1.0],
            vec![Kind::Text, Kind::Date, Kind::Int64],
            600,
        );
        let mut seed = 777u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut vals = Vec::with_capacity(3000);
        let mut t = 1_700_000_000_000i64;
        for i in 0..3000 {
            t += (next() % 1000) as i64;
            // Цена нарочно с малым числом значений: ничьих первого ключа много,
            // и добор вторым ключом обязан работать.
            let p = (next() % 40) as i64;
            vals.push((t, p));
            let mut w = vec!["общее".to_string()];
            if i % 7 == 0 {
                w.push("редкое".into());
            }
            ix.add(&format!("д{i}"), &[&w.join(" "), &t.to_string(), &p.to_string()]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        (Segments::load(&d).unwrap(), vals)
    }

    fn query(word: &str, sort: Vec<Sort>, k: usize) -> QuerySpec {
        let mut s = QuerySpec {
            words: vec![SpecWord {
                given: None,
                fid: 0,
                terms: vec![Term { text: word.into(), prefix: false }],
            }],
            sort,
            ..QuerySpec::default()
        };
        s.k = k;
        s
    }

    /// Ответ обязан быть тем же, каким бы путём обход ни пошёл.
    ///
    /// Обрыв — вопрос скорости. Если он меняет выдачу, он неверен, и проверять
    /// это надо на обоих полях: у одного корзины есть, у другого нет, и порядок
    /// обхода они задают по-разному.
    #[test]
    fn early_stop_returns_exactly_what_a_full_walk_returns() {
        let (segs, vals) = build("same");
        for (field, name) in [(1usize, "дата"), (2, "цена")] {
            for desc in [false, true] {
                for word in ["общее", "редкое"] {
                    for k in [1usize, 10, 100] {
                        let got: Vec<u64> = segs
                            .search(&query(word, vec![Sort { fid: field, desc }], k))
                            .hits
                            .iter()
                            .map(|h| h.doc)
                            .collect();

                        // Честный перебор: те же документы, тот же порядок.
                        let mut want: Vec<u64> =
                            (0..3000u64).filter(|&d| word == "общее" || d % 7 == 0).collect();
                        want.sort_by(|&a, &b| {
                            let (va, vb) = if field == 1 {
                                (vals[a as usize].0, vals[b as usize].0)
                            } else {
                                (vals[a as usize].1, vals[b as usize].1)
                            };
                            let ord = if desc { vb.cmp(&va) } else { va.cmp(&vb) };
                            ord.then(a.cmp(&b))
                        });
                        want.truncate(k);
                        assert_eq!(got, want, "поле {name}, desc={desc}, {word}, k={k}");
                    }
                }
            }
        }
    }

    /// Второй ключ решает ничьи первого.
    ///
    /// У цены сорок значений на три тысячи документов — ничьих много, и без
    /// добора порядок внутри них был бы по номеру, а не по дате.
    #[test]
    fn the_second_key_breaks_ties_of_the_first() {
        let (segs, vals) = build("ties");
        let got: Vec<u64> = segs
            .search(&query(
                "общее",
                vec![Sort { fid: 2, desc: false }, Sort { fid: 1, desc: true }],
                50,
            ))
            .hits
            .iter()
            .map(|h| h.doc)
            .collect();

        let mut want: Vec<u64> = (0..3000u64).collect();
        want.sort_by(|&a, &b| {
            vals[a as usize]
                .1
                .cmp(&vals[b as usize].1)
                .then(vals[b as usize].0.cmp(&vals[a as usize].0))
                .then(a.cmp(&b))
        });
        want.truncate(50);
        assert_eq!(got, want);
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    fn dir(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-schema-{name}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("каталог");
        d.to_string_lossy().to_string()
    }

    /// Сегмент с чужой схемой ловится на открытии, а не отдаёт содержимое под
    /// чужим именем.
    #[test]
    fn a_segment_with_a_foreign_schema_is_refused_loudly() {
        let d = dir("mixed");
        let mut a = crate::indexer::Indexer::new(&d, vec!["name".into()], vec![1.0], 100);
        for i in 0..10 {
            a.add(&format!("a{i}"), &[&format!("значение{i}")]);
        }
        a.flush();

        // Второй сегмент с другим именем поля — как если бы сервер поднялся с
        // умолчанием поверх чужого индекса.
        let mut b = crate::indexer::Indexer::new(&d, vec!["title".into()], vec![1.0], 100);
        for i in 0..10 {
            b.add(&format!("b{i}"), &[&format!("другое{i}")]);
        }
        b.flush();

        let e = Segments::load(&d).err().expect("схемы разные — открытие обязано отказать");
        let msg = e.to_string();
        assert!(msg.contains("схема"), "отказ не про схему: {msg}");
        assert!(msg.contains("name") && msg.contains("title"), "отказ не называет поля: {msg}");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// Совпадающие схемы открываются без единого слова.
    #[test]
    fn matching_schemas_open_quietly() {
        let d = dir("same");
        for part in 0..2 {
            let mut ix = crate::indexer::Indexer::new(
                &d,
                vec!["title".into(), "body".into()],
                vec![2.0, 1.0],
                100,
            );
            for i in 0..10 {
                ix.add(&format!("{part}-{i}"), &[&format!("з{i}"), "тело"]);
            }
            ix.flush();
        }
        let segs = Segments::load(&d).expect("одинаковые схемы обязаны открыться");
        assert_eq!(segs.list.len(), 2);
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// Кеш распакованных кусков на время одного запроса.
///
/// ## Почему на запрос, а не на процесс
///
/// Кеш, переживающий запрос, — это удержанная память, размер которой задаёт не
/// тот, кто её оплачивает. Ответ на сто документов из ста разных кусков удержал
/// бы четыреста килобайт до следующей уборки, и на тысяче одновременных
/// запросов это уже полгигабайта. Здесь кеш умирает вместе с ответом, и потолок
/// у него **байтовый**, а не в числе кусков: кусок бывает и в четыре килобайта,
/// и во много больше, если в нём одно длинное значение.
///
/// ## Чего он не делает
///
/// Не заменяет кеш страниц. Сжатые куски и так лежат в отображении, и ядро
/// решает, держать их или нет. Здесь кешируется **распакованное** — то, чего в
/// отображении нет и что стоит работы, а не чтения.
pub struct Fetcher<'a> {
    segs: &'a Segments,
    /// Ключ: сегмент, поле, номер куска. И сам кусок.
    held: Vec<((usize, usize, usize), Vec<u8>)>,
    bytes: usize,
    limit: usize,
    /// Сколько раз кусок пришлось распаковать. Мера, а не украшение: по ней
    /// видно, сработала группировка или нет.
    misses: usize,
    /// Значение, восстановленное из колонки. Живёт до следующего такого же.
    spare: Vec<u8>,
    /// Открытые хранилища идентификаторов по номеру сегмента.
    ///
    /// Лениво и только по просьбе. `ids.bin` — второй по величине кусок сегмента
    /// (91 МБ на 25 млн документов), а поиску идентификаторы не нужны вовсе: они
    /// нужны ответу, и только когда их попросили.
    ///
    /// Область отображения ленивость больше не экономит, и это стоит сказать
    /// прямо, чтобы следующий читатель не берёг то, что уже бесплатно: у
    /// упакованного сегмента `ids.bin` — кусок общего `seg.dat`, и его открытие
    /// заводит [`crate::mmap::Region`] внутри уже стоящего отображения, не
    /// прибавляя к счёту ядра ничего. Экономится разбор и опоры, а не области. У
    /// рассыпанного сегмента, собранного до контейнера, это по-прежнему
    /// отдельный файл и отдельная область — там прежний довод в силе.
    ids: Vec<(usize, crate::ids::Ids)>,
}

impl Fetcher<'_> {
    /// Идентификатор документа — тот, под которым его положили.
    ///
    /// Поиск отдаёт **номер** документа, а номер — величина внутренняя: он
    /// меняется при слиянии, и держать его у себя клиенту нельзя. Без этого
    /// перевода найденное невозможно связать с тем, что клиент хранит у себя, —
    /// разве что заведя копию своего идентификатора отдельным полем.
    pub fn id(&mut self, doc: u64) -> Option<String> {
        let i = self.segs.base.partition_point(|&b| b <= doc).checked_sub(1)?;
        let local = (doc - self.segs.base[i]) as usize;
        let at = match self.ids.iter().position(|(k, _)| *k == i) {
            Some(at) => at,
            None => {
                let ix = self.segs.list.get(i)?;
                self.ids.push((i, crate::ids::Ids::open(&ix.src, ix.docs)));
                self.ids.len() - 1
            }
        };
        self.ids[at].1.get(local)
    }

    /// Содержимое поля документа срезом в кеш.
    ///
    /// Поле зовётся **устойчивым** номером: документ живёт в каком-то одном
    /// сегменте, и местный номер поля у каждого свой. Перевод здесь — в одном
    /// месте, а не у каждого вызывающего.
    pub fn get(&mut self, doc: u64, fid: usize) -> Option<&[u8]> {
        let i = self.segs.base.partition_point(|&b| b <= doc).checked_sub(1)?;
        let local = doc - self.segs.base[i];
        let field = self.segs.list[i].field_by_id(fid as u32)?;
        let f = self.segs.list[i].fields.get(field)?;
        let Some(store) = f.store.as_ref() else {
            // Числовое поле копии текстом не держит — значение печатается из
            // колонки. Кеш кусков тут ни при чём, но отдать надо срезом, как и
            // всем остальным, поэтому строка кладётся в свой буфер.
            if !(f.stored && f.kind.numeric()) {
                return None;
            }
            self.spare = self.segs.list[i].fetch(local as u32, field)?;
            return Some(&self.spare);
        };
        let (chunk, first) = store.locate(local as u32)?;
        let key = (i, field, chunk);
        let at = match self.held.iter().position(|(k, _)| *k == key) {
            Some(at) => at,
            None => {
                let raw = store.chunk_bytes(chunk)?;
                self.bytes += raw.len();
                self.misses += 1;
                self.held.push((key, raw));
                // Потолок держится выбрасыванием самого старого: обращения идут
                // по возрастанию номера документа, и старое нужно реже всего.
                while self.bytes > self.limit && self.held.len() > 1 {
                    let (_, v) = self.held.remove(0);
                    self.bytes -= v.len();
                }
                self.held.len() - 1
            }
        };
        crate::store::slice_at(&self.held[at].1, (local - first as u64) as usize)
    }

    /// Значения поля сразу для многих документов: каждый кусок — один раз.
    ///
    /// ## Зачем отдельно от `get`
    ///
    /// `get` кладёт **распакованный кусок** в кеш и отдаёт срез из него. Пока
    /// документов немного и они рядом, это верно: соседи по куску достаются
    /// даром. Но верхушка из десяти тысяч — это десять тысяч документов,
    /// разбросанных по всему индексу, и почти каждый лежит в своём куске.
    ///
    /// Тогда кеш перестаёт быть кешем. Кусок весит килобайты, десять тысяч
    /// кусков — сотни мегабайт, а потолок кеша восемь. `prepare` грел, кеш
    /// вытеснял, `get` распаковывал заново — то есть каждый кусок проходил
    /// распаковку **дважды**. Замерено на 25 млн: `k = 10 000` с выдачей поля
    /// стоил 580 мс, из них по профилю 98% в `DecoderDict::decompress`.
    ///
    /// Здесь наружу отдаётся не кусок, а **значение**. Значение — десятки байт
    /// против килобайтов куска, и держать их все дешевле, чем один кусок. Куски
    /// обходятся по порядку, каждый распаковывается однажды и тут же
    /// забывается.
    ///
    /// ## Почему по потокам
    ///
    /// Распаковка куска ни от чего не зависит. Работа делится по кускам, а не
    /// по документам: документы одного куска обязаны достаться из одной
    /// распаковки, иначе деление отменило бы весь выигрыш.
    ///
    /// Порядок ответа — порядок `docs`, независимо от того, как легли потоки.
    pub fn many(&self, docs: &[u64], fid: usize, threads: usize) -> Vec<Option<Vec<u8>>> {
        self.many_guarded(docs, fid, threads, None)
    }

    /// То же с cooperative cancellation между документами и сжатыми кусками.
    pub fn many_guarded(
        &self,
        docs: &[u64],
        fid: usize,
        threads: usize,
        guard: Option<&crate::work::Guard>,
    ) -> Vec<Option<Vec<u8>>> {
        let mut out: Vec<Option<Vec<u8>>> = vec![None; docs.len()];
        if docs.is_empty() {
            return out;
        }
        // Что откуда брать: сегмент, поле, кусок, первый номер куска, местный
        // номер и место в ответе.
        let mut work: Vec<(usize, usize, usize, u32, u32, usize)> = Vec::with_capacity(docs.len());
        // Числовое поле копии текстом не держит — его значение печатается из
        // колонки, и куска у него нет. Такие идут отдельным списком.
        let mut plain: Vec<(usize, usize, u32, usize)> = Vec::new();
        for (at, &d) in docs.iter().enumerate() {
            if guard.is_some_and(|g| !g.check()) {
                return out;
            }
            let Some(i) = self.segs.base.partition_point(|&b| b <= d).checked_sub(1) else {
                continue;
            };
            let local = (d - self.segs.base[i]) as u32;
            let Some(field) = self.segs.list[i].field_by_id(fid as u32) else { continue };
            let Some(f) = self.segs.list[i].fields.get(field) else { continue };
            match f.store.as_ref() {
                Some(store) => {
                    if let Some((chunk, first)) = store.locate(local) {
                        work.push((i, field, chunk, first, local, at));
                    }
                }
                None => {
                    if f.stored && f.kind.numeric() {
                        plain.push((i, field, local, at));
                    }
                }
            }
        }
        for (i, field, local, at) in plain {
            if guard.is_some_and(|g| !g.check()) {
                return out;
            }
            out[at] = self.segs.list[i].fetch(local, field);
        }
        if work.is_empty() {
            return out;
        }
        work.sort_unstable();

        // Границы кусков в отсортированном списке: одна распаковка на границу.
        let mut edges: Vec<usize> = vec![0];
        for w in 1..work.len() {
            if guard.is_some_and(|g| !g.check()) {
                return out;
            }
            let (a, b, c, ..) = work[w];
            let (pa, pb, pc, ..) = work[w - 1];
            if (a, b, c) != (pa, pb, pc) {
                edges.push(w);
            }
        }
        edges.push(work.len());
        let groups = edges.len() - 1;
        let threads = threads.clamp(1, groups).min(64);

        let segs = self.segs;
        let work = &work;
        let edges = &edges;
        // Гидрация не должна заводить отдельный scoped-пул на каждое поле и
        // каждый запрос. Общий executor уже ограничен WorkloadGovernor; здесь
        // остаются только его lane-continuations и локальные результаты.
        let parts: Vec<Vec<(usize, Option<Vec<u8>>)>> = crate::executor::map(threads, |w| {
            Self::extract(segs, work, edges, w, groups, threads, guard)
        });
        for part in parts {
            for (at, v) in part {
                out[at] = v;
            }
        }
        out
    }

    /// Куски с `from`-го по шагу `step`: распаковать и раздать значения.
    fn extract(
        segs: &Segments,
        work: &[(usize, usize, usize, u32, u32, usize)],
        edges: &[usize],
        from: usize,
        groups: usize,
        step: usize,
        guard: Option<&crate::work::Guard>,
    ) -> Vec<(usize, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        for g in (from..groups).step_by(step) {
            if guard.is_some_and(|guard| !guard.check()) {
                break;
            }
            let (lo, hi) = (edges[g], edges[g + 1]);
            let (i, field, chunk, first, ..) = work[lo];
            let raw = segs
                .list
                .get(i)
                .and_then(|ix| ix.fields.get(field))
                .and_then(|f| f.store.as_ref())
                .and_then(|s| s.chunk_bytes(chunk));
            let Some(raw) = raw else {
                out.extend(work[lo..hi].iter().map(|w| (w.5, None)));
                continue;
            };
            for w in work[lo..hi].iter() {
                let skip = w.4.saturating_sub(first) as usize;
                out.push((w.5, crate::store::slice_at(&raw, skip).map(<[u8]>::to_vec)));
            }
        }
        out
    }

    /// Сколько кусков распаковано за этот запрос.
    pub fn misses(&self) -> usize {
        self.misses
    }

    /// Готовит куски под список документов, сгруппировав их по кускам.
    ///
    /// Обращения идут в порядке **кусков**, а не документов: так соседние
    /// документы одного куска распаковывают его однажды, а чтение с диска идёт
    /// вперёд, а не вразброс.
    /// Поле зовётся устойчивым номером — как и у [`Fetcher::get`].
    pub fn prepare(&mut self, docs: &[u64], fid: usize) {
        let mut want: Vec<(usize, usize, u64)> = Vec::with_capacity(docs.len());
        for &d in docs {
            let Some(i) = self.segs.base.partition_point(|&b| b <= d).checked_sub(1) else {
                continue;
            };
            let local = d - self.segs.base[i];
            let Some(store) = self.segs.list[i]
                .field_by_id(fid as u32)
                .and_then(|f| self.segs.list[i].fields.get(f))
                .and_then(|f| f.store.as_ref())
            else {
                continue;
            };
            if let Some((chunk, _)) = store.locate(local as u32) {
                want.push((i, chunk, d));
            }
        }
        want.sort_unstable();
        want.dedup_by_key(|(i, c, _)| (*i, *c));
        for (_, _, d) in want {
            self.get(d, fid);
        }
    }
}

/// Одна и та же выдача при позициях на диске и при проверке по тексту.
///
/// ## Почему оракул именно такой
///
/// Проверка по тексту — это второй способ ответить на тот же вопрос, и ценность
/// его ровно в том, что он обязан давать **тот же** ответ. Значит и проверять
/// надо не «работает ли», а совпадение: тот же состав, тот же порядок, те же
/// веса, то же число вхождений. Оракул здесь дешёвый именно потому, что профиль
/// с полными позициями никуда не делся — можно собрать корпус дважды и сравнить.
///
/// Расхождение в весе означало бы, что фраза посчитала другое число вхождений;
/// расхождение в составе — что разбор при проверке разошёлся с разбором приёма.
/// Оба молчаливы, и заметить их иначе нечем.
#[cfg(test)]
mod adaptive_tests {
    use super::*;

    /// Тексты со всем, на чём разбор способен разойтись.
    fn texts() -> Vec<String> {
        let mut out = Vec::new();
        for i in 0..1200u32 {
            let t = match i % 8 {
                0 => format!("красный квадрат синий круг общее слово{}", i % 17),
                1 => format!("синий квадрат красный круг общее ещё{}", i % 11),
                // Перекрывающиеся вхождения: «а а а» при фразе «а а» даёт два.
                2 => format!("общее эхо эхо эхо конец{}", i % 5),
                // Кириллица, китайский, эмодзи и разметка в одном документе.
                3 => format!("общее ёжик 日本語 текст 🐖 [[ссылка|Текст]] t1_{i}"),
                // Длинный документ: позиции пересекают границы кусков.
                4 => {
                    let mut s = String::from("общее ");
                    for k in 0..900 {
                        s.push_str(if k % 90 == 0 {
                            "красный квадрат "
                        } else {
                            "шум "
                        });
                    }
                    s
                }
                // Вхождений больше, чем вмещает байт частоты.
                5 => {
                    let mut s = String::from("общее ");
                    for _ in 0..300 {
                        s.push_str("повтор ");
                    }
                    s
                }
                6 => format!("общее красный ЖЁЛТЫЙ квадрат круг {i}"),
                _ => format!("общее квадрат красный {} конец", i % 23),
            };
            out.push(t);
        }
        out
    }

    fn build(name: &str, posmin: u32) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-adaptive-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 400);
        ix.set_posmin(posmin);
        for (i, t) in texts().iter().enumerate() {
            ix.add(&format!("d{i:05}"), &[t]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        d
    }

    fn ask(dir: &str, terms: &[&str], slop: u32) -> Vec<(u64, f64)> {
        let segs = Segments::load(dir).expect("индекс");
        let spec = QuerySpec {
            words: Vec::new(),
            filter: Vec::new(),
            must: Vec::new(),
            must_not: Vec::new(),
            nested: None,
            min_should: 0,
            sort: Vec::new(),
            from: 0,
            after: None,
            k: 2000,
            track_total_hits: TrackTotalHits::default(),
            max_expansions: DEFAULT_EXPANSIONS,
            max_expansion_examined: DEFAULT_EXPANSION_EXAMINED,
            fields: Vec::new(),
            phrases: vec![SpecPhrase {
                fid: 0,
                terms: terms.iter().map(|t| (*t).to_string()).collect(),
                slop,
                boost: 1.0,
            }],
            highlight: Vec::new(),
            highlight_size: 150,
            with_ids: false,
            translit: false,
            max_verify: DEFAULT_VERIFY,
            max_agg_ms: DEFAULT_AGG_MS,
            guard: None,
            live: None,
            expansion: None,
        };
        let out = segs.search(&spec);
        out.hits
            .iter()
            // Сравнивать можно по номеру: оба индекса собраны из одного входа
            // в одном порядке, значит и нумерация у них одна.
            .map(|h| (h.doc, h.score))
            .collect()
    }

    /// Выдача обязана совпасть до последнего документа и последнего веса.
    #[test]
    fn text_verification_answers_exactly_what_positions_answer() {
        let full = build("full", 0);
        // Порог выше частоты «общего», то есть выше всех: позиций не остаётся ни
        // у одного слова, и каждая фраза идёт через текст.
        let text = build("text", 2000);

        let a = crate::index::read_head(&format!("{full}/seg0000")).expect("заголовок");
        let b = crate::index::read_head(&format!("{text}/seg0000")).expect("заголовок");
        assert_eq!(a.posmin, 0, "у полного профиля граница обязана быть нулевой");
        assert!(b.posmin > 0, "порог не сработал: позиции остались у всех");
        // Длина потока позиций берётся из оглавления контейнера, а не у
        // файловой системы: отдельного `pos.bin` на диске больше нет, и
        // `metadata` по такому имени сравнила бы нуль с нулём — то есть не
        // проверила бы ничего.
        let pos_len = |d: &str, head: &crate::index::Head| -> Option<u64> {
            let seg = format!("{d}/seg0000");
            let src = crate::container::SegmentSource::of(&seg, head.container)
                .expect("источник сегмента");
            src.len_of("pos.bin")
        };
        let pos_full = pos_len(&full, &a);
        let pos_text = pos_len(&text, &b);
        // Сравнение обязано быть между двумя сегментами, иначе оно ничего не
        // значит: нуль сам по себе получился бы и от промаха по имени куска.
        // Поэтому у полного профиля поток проверяется на заметный размер тем же
        // самым поиском по оглавлению.
        assert!(
            pos_full.is_some_and(|n| n > 4096),
            "у полного профиля поток позиций обязан быть заметным: {pos_full:?}"
        );
        // «Куска нет» и «кусок есть, но пустой» — разные вещи, однако для потока
        // позиций обе означают одно: не записано ни байта. Приём просто не
        // заводит `pos.bin`, когда писать нечего.
        assert_eq!(
            pos_text.unwrap_or(0),
            0,
            "при проверке текстом позиции всё равно записаны: {pos_text:?} против {pos_full:?}"
        );

        let cases: &[(&[&str], u32)] = &[
            (&["красный", "квадрат"], 0),
            (&["квадрат", "красный"], 0),
            (&["красный", "квадрат"], 3),
            (&["синий", "круг"], 0),
            (&["эхо", "эхо"], 0),
            (&["повтор", "повтор"], 0),
            (&["красный", "жёлтый", "квадрат"], 0),
            (&["красный", "квадрат"], 4),
            (&["日本語", "текст"], 0),
            (&["ссылка", "текст"], 0),
            (&["общее", "квадрат"], 2),
            (&["нет", "такого"], 0),
        ];
        for (terms, slop) in cases {
            let x = ask(&full, terms, *slop);
            let y = ask(&text, terms, *slop);
            assert_eq!(
                x.len(),
                y.len(),
                "{terms:?} slop {slop}: попаданий {} против {}",
                x.len(),
                y.len()
            );
            for (i, ((ia, sa), (ib, sb))) in x.iter().zip(y.iter()).enumerate() {
                assert_eq!(ia, ib, "{terms:?} slop {slop}: место {i} — {ia} против {ib}");
                assert!(
                    (sa - sb).abs() < 1e-12,
                    "{terms:?} slop {slop}: вес {ia} — {sa} против {sb}"
                );
            }
        }
        std::fs::remove_dir_all(&full).ok();
        std::fs::remove_dir_all(&text).ok();
    }

    #[test]
    fn merging_never_invents_positions_and_says_so() {
        let d = build("merge", 200);
        let names = crate::manifest::segments(&d);
        assert!(names.len() >= 2, "нужно несколько сегментов: {}", names.len());

        let before = ask(&d, &["красный", "квадрат"], 0);
        let segs = Segments::load(&d).expect("индекс");
        let refs: Vec<&crate::index::Index> = segs.list.iter().map(|a| a.as_ref()).collect();
        let out = format!("{d}/mrg0000");
        crate::merge::merge(&refs, &out).expect("слияние");
        crate::manifest::write(&d, &["mrg0000".to_string()]).unwrap();

        let head = crate::index::read_head(&out).expect("заголовок");
        let src = crate::index::read_head(&format!("{d}/seg0000")).expect("заголовок");
        assert!(
            head.posmin >= src.posmin,
            "после слияния граница улучшилась сама собой: {} против {}",
            head.posmin,
            src.posmin
        );

        let after = ask(&d, &["красный", "квадрат"], 0);
        assert_eq!(before.len(), after.len(), "слияние потеряло попадания");
        for ((ia, _), (ib, _)) in before.iter().zip(after.iter()) {
            assert_eq!(ia, ib, "слияние переставило выдачу");
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Предел проверок обязан и ограничивать работу, и признаваться в этом.
    ///
    /// Два утверждения, и второе важнее. Ограничение без признака — это тихо
    /// урезанная выдача, выданная как полная; по ней принимают решения, и
    /// отличить «больше нет» от «дальше не смотрели» клиенту нечем.
    #[test]
    fn the_budget_bounds_the_work_and_admits_it() {
        let d = build("budget", 2000);
        let segs = Segments::load(&d).expect("индекс");
        let ask_with = |limit: usize| {
            let mut spec = QuerySpec {
                k: 2000,
                phrases: vec![SpecPhrase {
                    fid: 0,
                    terms: vec!["красный".into(), "квадрат".into()],
                    slop: 0,
                    boost: 1.0,
                }],
                ..QuerySpec::default()
            };
            spec.max_verify = limit;
            segs.search(&spec)
        };

        let full = ask_with(0);
        assert!(!full.partial, "без предела ответ обязан быть полным");
        assert!(full.hits.len() > 20, "нашлось {} — мерить нечего", full.hits.len());

        // Предела хватает на десяток проверок — значит и попаданий не больше.
        let cut = ask_with(10);
        assert!(cut.partial, "предел исчерпан, а ответ выдан как полный");
        assert!(cut.hits.len() <= 10, "предел в десять проверок дал {} попаданий", cut.hits.len());

        // Предел, до которого дело не дошло, признаком не помечается.
        let wide = ask_with(1_000_000);
        assert!(!wide.partial, "щедрый предел объявлен исчерпанным");
        assert_eq!(wide.hits.len(), full.hits.len(), "щедрый предел урезал выдачу");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Отсутствие позиций не имеет права стать пустым ответом.
    #[test]
    fn a_field_without_positions_still_answers_phrases() {
        let d = std::env::temp_dir()
            .join(format!("svindb-adaptive-nopos-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::typed_positioned(
            &d,
            vec!["body".into()],
            vec![1.0],
            vec![crate::index::Kind::Text],
            vec![true],
            vec![false],
            4000,
        );
        for i in 0..300u32 {
            let t = if i % 3 == 0 {
                "красный квадрат синий"
            } else {
                "квадрат красный синий"
            };
            ix.add(&format!("d{i:04}"), &[t]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let got = ask(&d, &["красный", "квадрат"], 0);
        assert_eq!(got.len(), 100, "фраза по полю без позиций не нашлась");
        let back = ask(&d, &["квадрат", "красный"], 0);
        assert_eq!(back.len(), 200, "обратный порядок нашёлся не там");
        std::fs::remove_dir_all(&d).ok();
    }
}

/// Транслитерация и раскладка на настоящем индексе.
///
/// ## Что здесь проверяется, а что нет
///
/// Порождение прочтений проверено отдельно, в [`crate::translit`]. Здесь другое:
/// что **выбор** между ними делает словарь. Порождение отдаёт десятки вариантов,
/// из которых в языке есть один-два, и весь смысл устройства в том, что список
/// заводится только существующим.
///
/// Поэтому и корпус здесь такой: рядом со словами, которые надо найти, лежат
/// слова, в которые ошибочное прочтение могло бы попасть.
#[cfg(test)]
mod translit_tests {
    use super::*;

    /// Свой каталог каждому тесту: параллельные тесты не должны стирать данные
    /// друг друга.
    fn corpus(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-translit-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 4000);
        let docs = [
            "клавиатура и мышь на столе",
            "телевизор показывает новости",
            "привет мир как дела",
            "москва столица россии",
            "яндекс ищет по сайтам",
            "компьютер стоит в углу",
            "статья вышла в журнале",
            // Слова-ловушки: правдоподобное прочтение могло бы попасть в них.
            "сон приснился ночью",
            "рот открыт широко",
            "the son of a computer",
        ];
        for (i, t) in docs.iter().enumerate() {
            ix.add(&format!("d{i}"), &[t]);
        }
        ix.flush();
        crate::manifest::write(
            &d,
            &crate::manifest::segments(&d)
                .iter()
                .map(|p| crate::manifest::name_of(p))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        d
    }

    fn ask(dir: &str, word: &str, translit: bool) -> Vec<u64> {
        let segs = Segments::load(dir).expect("индекс");
        let spec = QuerySpec {
            words: vec![SpecWord {
                given: None,
                fid: 0,
                terms: vec![Term { text: word.into(), prefix: false }],
            }],
            k: 50,
            translit,
            ..QuerySpec::default()
        };
        let mut out: Vec<u64> = segs.search(&spec).hits.iter().map(|h| h.doc).collect();
        out.sort_unstable();
        out
    }

    /// Латиница находит русское слово — и по звучанию, и по клавише.
    #[test]
    fn latin_finds_the_russian_word() {
        let d = corpus("latin28");
        for (typed, doc) in [
            ("klaviatura", 0u32),
            ("televizor", 1),
            ("privet", 2),
            ("moskva", 3),
            ("yandex", 4),
            ("statya", 6),
            // Раскладка: `привет` вслепую.
            ("ghbdtn", 2),
            ("vjcrdf", 3),
        ] {
            assert_eq!(ask(&d, typed, true), vec![doc as u64], "{typed} не нашло документ {doc}");
            // Без признака — не находит ничего, и это умолчание.
            assert!(ask(&d, typed, false).is_empty(), "{typed} нашлось без признака");
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Кириллица находит латинское написание — обратный ход.
    #[test]
    fn cyrillic_finds_the_latin_spelling() {
        let d = corpus("cyrillic33");
        // `сон` пишется `son`, и такое слово в корпусе есть.
        let got = ask(&d, "сон", true);
        assert!(got.contains(&7), "своё слово потерялось: {got:?}");
        assert!(got.contains(&9), "латинское написание не найдено: {got:?}");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Прочтение, которого в словаре нет, списка не заводит.
    ///
    /// Это и есть отбор. `klaviatura` порождает десятки вариантов —
    /// `клавиатура`, `клавиатурь`, `клавиацура`, `слявиатура` и прочие, — и если
    /// бы искались все, выдача набралась бы из чего попало.
    #[test]
    fn a_reading_the_dictionary_does_not_know_asks_for_nothing() {
        let d = corpus("a55");
        let segs = Segments::load(&d).expect("индекс");
        let ix = &segs.list[0];
        // `televizor` порождает 25 прочтений — `телевизор`, `телевизорь`,
        // `телевысор`, `телевизё…` и прочие, — из которых в словаре есть одно.
        let many = crate::translit::readings("televizor", crate::translit::BRANCHES);
        assert!(many.len() > 20, "прочтений {} — проверять нечего", many.len());

        let w = SpecWord {
            given: None,
            fid: 0,
            terms: vec![Term { text: "televizor".into(), prefix: false }],
        };
        let resolved = resolve_word(ix, &w, DEFAULT_EXPANSIONS, true, None, None);
        assert_eq!(
            resolved.lists.len(),
            1,
            "из {} прочтений заведено {} списков вместо одного — отбор словарём не сработал",
            many.len(),
            resolved.lists.len()
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Один список не заводится дважды: альтернатива — не двойной вес.
    #[test]
    fn the_same_list_is_never_added_twice() {
        let d = corpus("the34");
        let segs = Segments::load(&d).expect("индекс");
        let ix = &segs.list[0];
        // `сон` ищется и сам по себе, и как прочтение — список обязан быть один.
        let w = SpecWord {
            given: None,
            fid: 0,
            terms: vec![Term { text: "сон".into(), prefix: false }],
        };
        let resolved = resolve_word(ix, &w, DEFAULT_EXPANSIONS, true, None, None);
        let mut uniq = resolved.lists.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(uniq.len(), resolved.lists.len(), "список задвоился: {:?}", resolved.lists);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Английский запрос по английскому корпусу от признака не портится.
    ///
    /// Самый важный случай для точности: `son` и `computer` обязаны находить
    /// свои документы, а не подменяться русскими прочтениями.
    #[test]
    fn an_english_query_still_finds_english() {
        let d = corpus("an36");
        let got = ask(&d, "computer", true);
        assert!(got.contains(&9), "английское слово потерялось: {got:?}");
        let son = ask(&d, "son", true);
        assert!(son.contains(&9), "английское слово потерялось: {son:?}");
        // `son` читается и как `сон` — документ 7 находится вдобавок, а не вместо.
        assert!(son.contains(&7), "прочтение не сработало: {son:?}");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Сегменты с **разным составом полей** отвечают на один номер одинаково.
    ///
    /// Здесь `price` стоит в одном сегменте вторым, в другом третьим, а
    /// устойчивый номер у него один. Отбор обязан вернуть ровно тех, у кого
    /// цена в диапазоне, из **обоих**.
    #[test]
    fn segments_with_different_field_layouts_agree_on_a_field() {
        use crate::index::Kind;
        use crate::indexer::Indexer;
        let dir = std::env::temp_dir().join("svindb-seg-fids");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();

        // Сегмент A: title, price. Номера 0 и 1.
        {
            let mut ix = Indexer::typed(
                &format!("{dir}/a"),
                vec!["title".into(), "price".into()],
                vec![1.0, 1.0],
                vec![Kind::Text, Kind::Int32],
                1_000_000,
            );
            ix.set_fids(vec![0, 1]);
            for i in 0..20u32 {
                ix.add(&format!("a{i}"), &[&format!("товар {i}"), &(i * 10).to_string()]);
            }
            ix.flush_now();
        }
        {
            let mut ix = Indexer::typed(
                &format!("{dir}/b"),
                vec!["title".into(), "extra".into(), "price".into()],
                vec![1.0, 1.0, 1.0],
                vec![Kind::Text, Kind::Int32, Kind::Int32],
                1_000_000,
            );
            ix.set_fids(vec![0, 2, 1]);
            for i in 0..20u32 {
                // `extra` намеренно противоположно цене: перепутай движок
                // колонки — и отбор вернёт ровно обратное множество.
                ix.add(
                    &format!("b{i}"),
                    &[&format!("товар {i}"), &(1000 - i * 10).to_string(), &(i * 10).to_string()],
                );
            }
            ix.flush_now();
        }
        // Каждый индексатор кладёт свой сегмент в свой каталог; составляем из
        // них один индекс.
        std::fs::rename(format!("{dir}/a/seg0000"), format!("{dir}/seg0000")).unwrap();
        std::fs::rename(format!("{dir}/b/seg0000"), format!("{dir}/seg0001")).unwrap();
        std::fs::remove_dir_all(format!("{dir}/a")).ok();
        std::fs::remove_dir_all(format!("{dir}/b")).ok();
        crate::manifest::write(&dir, &["seg0000".into(), "seg0001".into()]).unwrap();

        let segs = Segments::load(&dir).unwrap();
        assert_eq!(segs.list.len(), 2, "оба сегмента открылись");
        // Один и тот же путь — разные местные номера.
        assert_eq!(segs.list[0].field_by_id(1), Some(1), "в A цена вторая");
        assert_eq!(segs.list[1].field_by_id(1), Some(2), "в B цена третья");

        // Отбор по устойчивому номеру: цена от 50 до 100 включительно.
        let spec = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: 50, to: 100 }],
            k: 100,
            ..QuerySpec::default()
        };
        let out = segs.search(&spec);
        let mut got: Vec<String> = out
            .hits
            .iter()
            .map(|h| {
                // Полоса идентификаторов достаётся у открытого сегмента: у
                // упакованного отдельного `ids.bin` на диске нет.
                let ix = &segs.list[h.segment];
                crate::ids::Ids::open(&ix.src, ix.docs).get(h.local as usize).unwrap_or_default()
            })
            .collect();
        got.sort();
        let want: Vec<String> =
            (5..=10u32).flat_map(|i| [format!("a{i}"), format!("b{i}")]).collect();
        let mut want = want;
        want.sort();
        assert_eq!(got, want, "отбор прочёл в одном из сегментов чужую колонку");

        // И поле, которого в первом сегменте нет вовсе, не находит там ничего,
        // а во втором работает.
        let only_b = QuerySpec {
            filter: vec![SpecRange { fid: 2, from: 900, to: 1000 }],
            k: 100,
            ..QuerySpec::default()
        };
        let out = segs.search(&only_b);
        assert!(
            out.hits.iter().all(|h| h.segment == 1),
            "поле есть только во втором сегменте, а нашлось и в первом"
        );
        assert_eq!(out.hits.len(), 11, "1000..900 с шагом 10 — одиннадцать документов");
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Соединение по элементу массива: то, ради чего заведено слот-пространство.
#[cfg(test)]
mod slot_join_tests {
    use super::*;
    use crate::index::Kind;
    use crate::indexer::{Cell, Indexer};

    /// Корпус реакций: у каждого документа три элемента с известными числами.
    fn corpus(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-join-{name}"));
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "emoji".into(), "count".into()],
            vec![1.0, 1.0, 1.0],
            vec![Kind::Text, Kind::Keyword, Kind::Int32],
            1_000_000,
        );
        ix.set_groups(vec![0, 1, 1], vec![String::new(), "reactions".into()]);
        // Документ d: 👍 со счётчиком d, 🔥 со счётчиком 100−d, 👏 со счётчиком 7.
        for d in 0..200i64 {
            ix.add_cells(
                &format!("d{d}"),
                &[1, 3],
                &[
                    Cell { field: 0, slot: 0, text: "документ" },
                    Cell { field: 1, slot: 0, text: "up" },
                    Cell { field: 2, slot: 0, text: &d.to_string() },
                    Cell { field: 1, slot: 1, text: "fire" },
                    Cell { field: 2, slot: 1, text: &(100 - d).to_string() },
                    Cell { field: 1, slot: 2, text: "clap" },
                    Cell { field: 2, slot: 2, text: "7" },
                ],
            );
        }
        ix.flush_now();
        std::fs::rename(format!("{dir}/seg0000"), format!("{dir}/seg0000x")).unwrap();
        std::fs::rename(format!("{dir}/seg0000x"), format!("{dir}/seg0000")).unwrap();
        crate::manifest::write(&dir, &["seg0000".into()]).unwrap();
        dir
    }

    fn ask(dir: &str, emoji: &str, from: i64, to: i64) -> Vec<u64> {
        let segs = Segments::load(dir).expect("индекс открылся");
        let spec = QuerySpec {
            nested: Some(SpecBool {
                must: vec![SpecClause::Slots(SpecSlots {
                    group: 1,
                    all: vec![
                        SpecSlotTerm::Term(SpecWord {
                            fid: 1,
                            terms: vec![Term::parse(&crate::indexer::keyword_term(emoji, true))],
                            given: None,
                        }),
                        SpecSlotTerm::Range(SpecRange { fid: 2, from, to }),
                    ],
                    none: Vec::new(),
                    min_count: 1,
                    boost: 1.0,
                })],
                ..SpecBool::default()
            }),
            k: 1000,
            ..QuerySpec::default()
        };
        let mut got: Vec<u64> = segs.search(&spec).hits.iter().map(|h| h.local as u64).collect();
        got.sort_unstable();
        got
    }

    /// Условия обязаны сойтись на **одном** элементе, а не где угодно.
    ///
    /// Это тот самый случай, который плоское раскрытие массива путает. У
    /// документа `d` счётчик 👍 равен `d`, а 🔥 — `100−d`. Запрос «👍 со
    /// счётчиком ровно 90» обязан дать один документ, а не сто.
    #[test]
    fn two_conditions_must_meet_on_the_same_element() {
        let dir = corpus("pair");

        // 👍 со счётчиком ровно 90 — это документ 90 и только он.
        assert_eq!(ask(&dir, "up", 90, 90), vec![90]);
        // 🔥 со счётчиком ровно 90 — это документ 10.
        assert_eq!(ask(&dir, "fire", 90, 90), vec![10]);
        // 👏 всегда со счётчиком 7 — либо все, либо никто.
        assert_eq!(ask(&dir, "clap", 7, 7).len(), 200);
        assert!(ask(&dir, "clap", 8, 8).is_empty());

        // Плоское раскрытие ответило бы иначе: у документа 10 есть и 👍, и
        // счётчик 90 — но у разных элементов.
        let flat_would_match = ask(&dir, "up", 90, 90).contains(&10);
        assert!(!flat_would_match, "условия сошлись на разных элементах");

        // Диапазон: 👍 со счётчиком не меньше 150 — документы 150..199.
        assert_eq!(ask(&dir, "up", 150, i64::MAX).len(), 50);
        // А по 🔥 счётчик равен 100−d и выше сотни не бывает.
        assert!(ask(&dir, "fire", 101, i64::MAX).is_empty());
        // Не меньше 195 по 👍 — только документы 195..199.
        assert_eq!(ask(&dir, "up", 195, i64::MAX), vec![195, 196, 197, 198, 199]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отрицание внутри элемента и требование двух совпавших элементов.
    #[test]
    fn negation_and_min_count_work_on_elements() {
        let dir = corpus("neg");
        let segs = Segments::load(&dir).expect("индекс");

        // Рядом со слот-условием стоит обычное слово: обход ведёт оно.
        // Условие из одних диапазонов вести обход не может — у него нет списка,
        // по которому идти, — и это то же самое ограничение, по которому
        // запрос без слов и без отбора отвергается разбором.
        let run = |all: Vec<SpecSlotTerm>, none: Vec<SpecSlotTerm>, min: u32| -> usize {
            let spec = QuerySpec {
                nested: Some(SpecBool {
                    must: vec![
                        SpecClause::Term(SpecWord {
                            fid: 0,
                            terms: vec![Term::parse("документ")],
                            given: None,
                        }),
                        SpecClause::Slots(SpecSlots {
                            group: 1,
                            all,
                            none,
                            min_count: min,
                            boost: 1.0,
                        }),
                    ],
                    ..SpecBool::default()
                }),
                k: 1000,
                ..QuerySpec::default()
            };
            segs.search(&spec).hits.len()
        };

        // Элемент со счётчиком 7 есть у всех (это 👏).
        assert_eq!(
            run(vec![SpecSlotTerm::Range(SpecRange { fid: 2, from: 7, to: 7 })], vec![], 1),
            200
        );
        // Но если запретить 👏, элемента со счётчиком 7 не останется — кроме
        // документов 7 и 93, где семёрка выпадает на 👍 и 🔥.
        let left = run(
            vec![SpecSlotTerm::Range(SpecRange { fid: 2, from: 7, to: 7 })],
            vec![SpecSlotTerm::Term(SpecWord {
                fid: 1,
                terms: vec![Term::parse(&crate::indexer::keyword_term("clap", true))],
                given: None,
            })],
            1,
        );
        assert_eq!(left, 2, "остались только те, где семёрка не у 👏");

        // Двух элементов со счётчиком 7 не бывает нигде, кроме документа 7 и 93?
        // У документа 7: 👍=7, 👏=7 — это два. У 93: 🔥=7, 👏=7 — тоже два.
        assert_eq!(
            run(vec![SpecSlotTerm::Range(SpecRange { fid: 2, from: 7, to: 7 })], vec![], 2),
            2,
            "два элемента со счётчиком 7"
        );
        // Наличие значения: счётчик есть у всех трёх элементов каждого документа.
        assert_eq!(run(vec![SpecSlotTerm::Exists { fid: 2 }], vec![], 3), 200);
        assert_eq!(run(vec![SpecSlotTerm::Exists { fid: 2 }], vec![], 4), 0);
        std::fs::remove_dir_all(&dir).ok();
    }
}

impl Segments {
    /// Считает агрегации по всем совпадениям запроса.
    ///
    /// Отдельным проходом, а не поверх верхушки: верхушка отсекает, агрегация
    /// обязана видеть каждое совпадение. Сегменты считаются по отдельности и
    /// сливаются — так же, как складывались бы шарды.
    pub fn aggregate(
        &self,
        spec: &QuerySpec,
        aggs: &[crate::aggs::Agg],
    ) -> Vec<crate::aggs::Result_> {
        self.aggregate_with(spec, aggs, 1).values
    }

    /// Проверяет совместимость numeric operators со старым scalar storage.
    /// Полный `_source` можно экспортировать и без этой capability; молча
    /// выдать неверный range/count вместо migration refusal нельзя.
    pub fn numeric_compatibility(
        &self,
        spec: &QuerySpec,
        aggs: &[crate::aggs::Agg],
    ) -> Result<(), String> {
        fn nested(tree: &SpecBool, fields: &mut HashSet<usize>) {
            for clause in tree.must.iter().chain(&tree.should).chain(&tree.must_not) {
                match clause {
                    SpecClause::Bool(tree) => nested(tree, fields),
                    SpecClause::Slots(slots) => {
                        for term in slots.all.iter().chain(&slots.none) {
                            match term {
                                SpecSlotTerm::Range(range) => {
                                    fields.insert(range.fid);
                                }
                                SpecSlotTerm::Exists { fid } => {
                                    fields.insert(*fid);
                                }
                                SpecSlotTerm::Term(_) => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        let mut fields: HashSet<usize> =
            spec.filter.iter().map(|r| r.fid).chain(spec.sort.iter().map(|s| s.fid)).collect();
        if let Some(tree) = &spec.nested {
            nested(tree, &mut fields);
        }
        for ix in &self.list {
            if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                break;
            }
            if ix.layout >= 12 {
                continue;
            }
            let local = fields
                .iter()
                .filter_map(|&fid| ix.field_by_id(fid as u32))
                .chain(crate::aggs::fields_in(ix, aggs));
            for at in local {
                if let Some(field) = ix.fields.get(at).filter(|field| field.legacy_keyword_values) {
                    return Err(format!("поле {} содержит legacy keyword storage; doc values требуют migration из authoritative source в layout 12", field.name));
                }
                if let Some(field) = ix.fields.get(at).filter(|field| field.legacy_multivalue) {
                    return Err(format!("поле {} содержит legacy numeric storage; range/sort/aggregation требуют migration из authoritative source в layout 10", field.name));
                }
            }
        }
        Ok(())
    }

    /// То же в несколько потоков — и **побитово** тем же ответом.
    ///
    /// ## Что делится и что не делится
    ///
    /// Делятся сегменты: поток берёт каждый `workers`-й, как и в поиске.
    /// Внутри сегмента ничего не делится — счёт обязан увидеть каждое
    /// совпадение, и делить между потоками тут нечего, кроме самих сегментов.
    ///
    /// ## Почему ответ обязан совпасть побитово, а не примерно
    ///
    /// Число потоков — свойство узла, а не запроса. Ответ, зависящий от него,
    /// означал бы, что один и тот же запрос к одним и тем же данным даёт разные
    /// суммы на машинах разной ширины, и объяснить это клиенту нечем.
    ///
    /// Всё, кроме дробной суммы, складывается в любом порядке и даёт то же
    /// число. Дробная сумма выносится из каждого сегмента отдельно и
    /// складывается по возрастанию номера сегмента — ровно так, как сложил бы
    /// её один поток. Разбор — у [`crate::aggs::Sink::take_floats`].
    ///
    /// ## Оговорка про неполный ответ
    ///
    /// Побитовое равенство обещано **завершившемуся** обходу. Обход,
    /// оборванный сторожем по сроку, помечен `partial`, и воспроизводимым он не
    /// объявляется: сколько успели, столько и успели.
    pub fn aggregate_with(
        &self,
        spec: &QuerySpec,
        aggs: &[crate::aggs::Agg],
        threads: usize,
    ) -> AggOutcome {
        if let Some(guard) = spec.guard.as_ref() {
            if spec.max_agg_ms > 0 {
                guard.tighten(std::time::Duration::from_millis(spec.max_agg_ms));
            }
            return self.aggregate_guarded(spec, aggs, threads, guard);
        }
        self.aggregate_guarded(
            spec,
            aggs,
            threads,
            &Arc::new(crate::aggs::Guard::new(spec.max_agg_ms)),
        )
    }

    /// То же со сторожем снаружи.
    ///
    /// Срок узел выводит из запроса сам, а отмену — нет: причина отмены живёт
    /// не в запросе. Клиент закрыл соединение, узел гасится, оператор снял
    /// запрос — всё это известно тому, кто держит сторож, и никак не выводится
    /// из `spec`. Поэтому сторож можно принести свой.
    /// Раскладывает счёт по работам: сегменты, а при нехватке — их полосы.
    ///
    /// ## Когда сегмент делится
    ///
    /// Только когда деление и безопасно, и нужно. Все четыре условия
    /// обязательны, и каждое стоит за настоящим расхождением:
    ///
    /// * **Сегментов меньше, чем потоков.** Иначе делить незачем: потоки и так
    ///   заняты, а лишние работы только добавят слияний приёмников.
    /// * **Ни одной дробной агрегации.** Дробная сумма складывается в
    ///   фиксированном порядке ради побитового равенства с однопоточным
    ///   ответом, а сложение с плавающей точкой неассоциативно: разбей сегмент
    ///   — и сумма сойдётся с прежней не всегда. Целые суммы, счётчики,
    ///   минимум, максимум, корзины и регистры мощности складываются в любом
    ///   порядке и дают то же число.
    /// * **Кандидатов поставляет перебор.** Курсор списка идёт вперёд, и
    ///   отдать его половину другому потоку нельзя, не научив переходить к
    ///   нужному номеру. См. [`search::can_split`].
    /// * **Полоса не мельче [`MIN_LANE_DOCS`].** Поток дороже работы, которую
    ///   ему дадут.
    ///
    /// Не выполнено хоть одно — сегмент идёт целиком, то есть ровно так, как
    /// шёл до появления полос.
    fn plan_jobs(
        &self,
        mine: &[usize],
        spec: &QuerySpec,
        aggs: &[crate::aggs::Agg],
        threads: usize,
    ) -> Vec<Job> {
        let whole = || mine.iter().map(|&at| Job { at, range: None }).collect::<Vec<Job>>();
        // Пустой состав — раньше любых расчётов полос. Отбор,
        // не подошедший ни к одному сегменту, — обычное дело, а не край:
        // заголовки полей прямо говорят, что подходящих документов нет.
        if mine.is_empty() || threads <= 1 {
            return whole();
        }
        if aggs.iter().any(|a| matches!(a.space, crate::aggs::Space::Float)) {
            return whole();
        }
        // Дешёвая проверка по запросу до разрешения словаря: со словами
        // делить нельзя, а без слов разрешение почти ничего не стоит.
        let tree_empty = spec
            .nested
            .as_ref()
            .is_none_or(|t| t.must.is_empty() && t.should.is_empty() && t.must_not.is_empty());
        if !spec.words.is_empty()
            || !spec.must.is_empty()
            || !spec.must_not.is_empty()
            || !spec.phrases.is_empty()
            || !tree_empty
        {
            return whole();
        }

        let total_docs =
            mine.iter().fold(0usize, |sum, &at| sum.saturating_add(self.list[at].docs));
        let target_docs = total_docs.div_ceil(threads).max(MIN_LANE_DOCS);
        let mut jobs: Vec<Job> = Vec::with_capacity(mine.len());
        for &at in mine.iter() {
            if spec.guard.as_ref().is_some_and(|g| !g.check()) {
                break;
            }
            let ix = &self.list[at];
            let mut req = spec.resolve_for_aggregation(ix);
            req.live = spec.live.as_ref().and_then(|maps| maps.get(at)).cloned();
            let cols = crate::aggs::fields_in(ix, aggs);
            let lanes =
                (ix.docs.div_ceil(target_docs)).min(threads).min(ix.docs / MIN_LANE_DOCS).max(1);
            // Сегмент, на который ответят заголовки вёдер, делить нельзя: у
            // полосы этого права нет, и деление отняло бы стократно более
            // быстрый путь.
            if lanes <= 1
                || buckets_can_answer(ix, &req, aggs, &cols)
                || (crate::aggs::document_counts_only(aggs)
                    && search::exact_metadata_count(ix, &req).is_some())
                || !search::can_split(ix, &req)
            {
                jobs.push(Job { at, range: None });
                continue;
            }
            let docs = ix.docs as u32;
            let step = docs.div_ceil(lanes as u32).max(1);
            let mut lo = 0u32;
            while lo < docs {
                let hi = lo.saturating_add(step).min(docs);
                jobs.push(Job { at, range: Some((lo, hi)) });
                lo = hi;
            }
        }
        jobs
    }

    pub fn aggregate_guarded(
        &self,
        spec: &QuerySpec,
        aggs: &[crate::aggs::Agg],
        threads: usize,
        guard: &Arc<crate::aggs::Guard>,
    ) -> AggOutcome {
        // Явно переданный guard является владельцем всей операции, включая
        // planning и scanner без совпадений. Проверять его только в Sink
        // недостаточно: при нуле hits callback вообще не вызывается.
        // HTTP уже передаёт ту же Arc, поэтому его AST не копируется.
        let overridden = (!spec.guard.as_ref().is_some_and(|old| Arc::ptr_eq(old, guard)))
            .then(|| QuerySpec { guard: Some(Arc::clone(guard)), ..spec.clone() });
        let spec = overridden.as_ref().unwrap_or(spec);
        // Сегмент вне отбора не открывается — то же правило, что в поиске.
        //
        // Правило было, а звали его только оттуда: агрегация по каждому
        // сегменту раскрывала запрос и заводила приёмник, даже когда границы
        // поля в заголовке прямо говорят, что подходящих документов здесь нет
        // как факта. На сотне сегментов это сотня открытий ради нуля.
        // Для плоских literal `must`/OR generation-aware term directory
        // доказывает отсутствие совпадений ещё до `resolve` и создания
        // приёмника. Раньше этот быстрый путь использовал только поиск, а
        // aggregation повторно открывала словари всех сегментов — именно тот
        // `63 × N` профиль, который делает редкий многополевой запрос дорогим.
        // Консервативные формы (nested, phrase, prefix, transliteration) дают
        // `None` и остаются на полном точном пути.
        let candidates = self.term_candidates(spec);
        let mut mine: Vec<usize> = Vec::new();
        for i in 0..self.list.len() {
            if !guard.check() {
                break;
            }
            if !Self::candidate(candidates.as_deref(), i) {
                continue;
            }
            if Self::range_can_match(&self.list[i], spec) {
                mine.push(i);
            }
        }

        // Работы: сегмент целиком либо его полоса.
        //
        // Прежде делились только сегменты, и на слитом индексе это означало
        // один поток из шестнадцати — замерено приростом процессорного времени
        // к настенному: ровно 1,00 ядра. Комментарий выше объяснял это тем, что
        // «делить внутри сегмента нечего», но делить есть что: полоса видит все
        // свои совпадения, деление идёт по номерам документов, а не по выборке.
        let jobs = self.plan_jobs(&mine, spec, aggs, threads);
        let workers = threads.max(1).min(jobs.len().max(1));

        // Дробные суммы: по строке на работу, по столбцу на агрегацию.
        // Раскладываются по своему месту, а не дописываются в общий список, —
        // иначе порядок зависел бы от того, какой поток успел раньше.
        let mut floats: Vec<Vec<(f64, f64)>> = vec![Vec::new(); jobs.len()];

        // Счётчик сверок — на **поток**, а не на сегмент. Заведи его внутри
        // сегмента, и узел с сотней сегментов по двести документов не сверился
        // бы с часами ни разу: каждый сегмент начинал бы счёт заново и до шага
        // сверки не доходил. Сторож при этом выглядел бы работающим.
        let one = |at: usize, floats: &mut Vec<(f64, f64)>, tick: &mut u32| -> crate::aggs::Sink {
            let job = &jobs[at];
            let ix = &self.list[job.at];
            let mut req = spec.resolve_for_aggregation(ix);
            req.live = spec.live.as_ref().and_then(|maps| maps.get(job.at)).cloned();
            // Поля агрегаций переводятся в местные позиции ровно здесь и один
            // раз на сегмент — так же, как их переводит `resolve` для отбора и
            // сортировки. Разбор — у [`crate::aggs::fields_in`].
            let cols = crate::aggs::fields_in(ix, aggs);
            let mut sink = crate::aggs::Sink::for_index(ix, aggs, &cols, Some(guard));
            // Счёт по заголовкам вёдер отвечает за **весь** сегмент сразу и
            // потому применим только к неделёной работе.
            if job.range.is_none()
                && count_by_summaries(ix, &req, aggs, &cols, &mut sink, guard, tick)
            {
                sink.seal_guarded(ix, aggs, &cols, Some(guard));
                *floats = sink.take_floats();
                return sink;
            }
            match job.range {
                None => search::for_each_match(ix, &req, &mut |doc| {
                    sink.accept_guarded(ix, aggs, &cols, doc, Some(guard));
                    guard.go(tick)
                }),
                Some((lo, hi)) => search::for_each_match_in(ix, &req, lo, hi, &mut |doc| {
                    sink.accept_guarded(ix, aggs, &cols, doc, Some(guard));
                    guard.go(tick)
                }),
            }
            // Закрыть счёт по сегменту **до** слияния: номера словаря
            // действительны только внутри своего сегмента, см. `aggs::TermKey`.
            sink.seal_guarded(ix, aggs, &cols, Some(guard));
            *floats = sink.take_floats();
            sink
        };

        let parts: Vec<Lane> = if workers <= 1 || jobs.len() <= 1 {
            let mut acc: Option<crate::aggs::Sink> = None;
            let mut rows: Vec<(usize, Vec<(f64, f64)>)> = Vec::new();
            let mut tick = 0u32;
            for at in 0..jobs.len() {
                // Сработавший сторож закрывает и следующие сегменты. Без
                // этой строки каждый из них успевал принять по документу,
                // прежде чем спросить сторожа, — на сотне сегментов сотня
                // документов сверх объявленного предела.
                if guard.fired() {
                    break;
                }
                let mut f: Vec<(f64, f64)> = Vec::new();
                let sink = one(at, &mut f, &mut tick);
                rows.push((at, f));
                match acc.as_mut() {
                    Some(t) => t.merge(sink),
                    None => acc = Some(sink),
                }
            }
            vec![(acc, rows)]
        } else {
            let one = &one;
            // Сегменты после compaction резко отличаются по размеру. Раздача
            // через один могла отдать 80% документов одному worker. Индексы
            // jobs сохраняются для детерминированной редукции float-сумм.
            let cost = |at: usize| {
                jobs[at].range.map_or(self.list[jobs[at].at].docs, |(lo, hi)| (hi - lo) as usize)
            };
            let mut order: Vec<usize> = (0..jobs.len()).collect();
            order.sort_unstable_by_key(|&at| (std::cmp::Reverse(cost(at)), at));
            let mut assignments = vec![Vec::new(); workers];
            let mut loads = vec![0usize; workers];
            for at in order {
                let worker = (0..workers).min_by_key(|&w| (loads[w], w)).unwrap();
                assignments[worker].push(at);
                loads[worker] = loads[worker].saturating_add(cost(at));
            }
            crate::executor::map(workers, |w| {
                let mut rows: Vec<(usize, Vec<(f64, f64)>)> = Vec::new();
                let mut acc: Option<crate::aggs::Sink> = None;
                let mut tick = 0u32;
                for &at in &assignments[w] {
                    if guard.fired() {
                        break;
                    }
                    let mut f: Vec<(f64, f64)> = Vec::new();
                    let sink = one(at, &mut f, &mut tick);
                    rows.push((at, f));
                    match acc.as_mut() {
                        Some(t) => t.merge(sink),
                        None => acc = Some(sink),
                    }
                }
                (acc, rows)
            })
        };

        let mut total: Option<crate::aggs::Sink> = None;
        for (acc, rows) in parts {
            for (at, f) in rows {
                floats[at] = f;
            }
            let Some(s) = acc else { continue };
            match total.as_mut() {
                Some(t) => t.merge(s),
                None => total = Some(s),
            }
        }
        let mut total = total.unwrap_or_else(|| crate::aggs::Sink::new(aggs));

        // Дробные суммы складываются здесь и только здесь — по возрастанию
        // номера сегмента. Сложение простое, без компенсации: один поток сложил
        // бы их ровно так же, а любое другое правило дало бы другое число.
        // Первая строка — начало, а не слагаемое к нулю. Разница видна только на
        // отрицательном нуле (`0.0 + (-0.0)` даёт `+0.0`), но правило «побитово
        // как в один поток» на «только на отрицательном нуле» не делится.
        let mut sums: Vec<(f64, f64)> = vec![(0.0, 0.0); total.len()];
        for (n, row) in floats.iter().enumerate() {
            if n == 0 {
                sums.iter_mut().zip(row.iter()).for_each(|(out, &v)| *out = v);
                continue;
            }
            for (out, &(s, c)) in sums.iter_mut().zip(row.iter()) {
                out.0 += s;
                out.1 += c;
            }
        }
        total.set_floats(&sums);
        AggOutcome { values: total.finish(aggs), partial: guard.fired() }
    }
}

/// Агрегации против честного пересчёта по тому же корпусу.
#[cfg(test)]
mod agg_tests {
    use super::*;
    use crate::aggs::{Agg, Kind as AggKind, Over, Value};
    use crate::index::Kind;
    use crate::indexer::{Cell, Indexer};

    /// Своё имя на каждый тест: тесты идут параллельно, а `remove_dir_all` в
    /// общем каталоге под другим тестом даёт разъезд, который потом ищут часами
    /// и списывают на «мигает».
    fn corpus(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-aggs-{name}"));
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "price".into(), "emoji".into(), "count".into()],
            vec![1.0; 4],
            vec![Kind::Text, Kind::Int32, Kind::Keyword, Kind::Int32],
            250,
        );
        ix.set_groups(vec![0, 0, 1, 1], vec![String::new(), "reactions".into()]);
        for d in 0..1000i64 {
            // Две реакции: счётчики d и 2d.
            ix.add_cells(
                &format!("d{d}"),
                &[1, 2],
                &[
                    Cell { field: 0, slot: 0, text: "документ" },
                    Cell { field: 1, slot: 0, text: &(d % 100).to_string() },
                    Cell { field: 2, slot: 0, text: "up" },
                    Cell { field: 3, slot: 0, text: &d.to_string() },
                    Cell { field: 2, slot: 1, text: "fire" },
                    Cell { field: 3, slot: 1, text: &(2 * d).to_string() },
                ],
            );
        }
        ix.flush_now();
        dir
    }

    fn all() -> QuerySpec {
        QuerySpec {
            words: vec![SpecWord {
                fid: 0, terms: vec![Term::parse("документ")], given: None
            }],
            k: 1,
            ..QuerySpec::default()
        }
    }

    /// Корпус, на котором порядок сложения **виден**.
    ///
    /// ## Почему нельзя взять просто дробное поле
    ///
    /// Сложение `f64` теряет ассоциативность только когда слагаемые разной
    /// величины: пока они одного порядка, `(a+b)+c` и `a+(b+c)` совпадают
    /// побитово, и тест, собранный из таких, зелен при любой ошибке порядка.
    /// Это самый обидный вид пустого теста: он не сравнивает ноль с нулём, он
    /// сравнивает два числа, которые обязаны были разойтись, а не разошлись.
    ///
    /// ## Как он устроен
    ///
    /// В каждом сегменте один документ несёт значение, остальные — ноль. Все
    /// несущие дают единицу, кроме несущего **второго** сегмента: он даёт
    /// `1e16`. Шаг разрядной сетки там равен двум, поэтому единица, прибавленная
    /// к нему, пропадает, а сложенные между собой единицы — уцелевают. Значит
    /// ответ зависит от того, что к чему прибавили.
    ///
    /// Второго, а не первого — и это не мелочь. Естественная неверная сборка
    /// складывает сегменты полосами по потокам, а нулевой сегмент стоит первым
    /// в любой полосе: положи большое туда, и неверная сборка дала бы то же
    /// число, а тест был бы зелёным на сломанном коде. Проверено перебором всех
    /// шестнадцати мест: полоса ставит большое иначе на местах 1, 3, 8 и 12.
    ///
    /// Оговорка, которую стоит сказать вслух: «правильный» здесь — менее точный
    /// ответ. Точность внутри сегмента бережёт компенсация Неймана; между
    /// сегментами выбрана **воспроизводимость**, потому что число потоков — не
    /// свойство запроса.
    fn floats(name: &str, per: u32) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-aggs-{name}"));
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "вес".into()],
            vec![1.0; 2],
            vec![Kind::Text, Kind::Double],
            per,
        );
        for d in 0..DOCS {
            let v: f64 = if d == per {
                1.0e16
            } else if d % per == 0 {
                1.0
            } else {
                0.0
            };
            ix.add_cells(
                &format!("d{d}"),
                &[1],
                &[
                    Cell { field: 0, slot: 0, text: "документ" },
                    Cell { field: 1, slot: 0, text: &format!("{v}") },
                ],
            );
        }
        ix.flush_now();
        dir
    }

    /// Документов в дробном корпусе: степень двойки, чтобы сегменты по 128
    /// вышли ровно шестнадцатью.
    const DOCS: u32 = 2048;

    /// Сумма при `n` потоках побитово равна сумме при одном.
    ///
    /// ## Почему побитово, а не «с допуском»
    ///
    /// Число потоков — свойство узла, а не запроса. Ответ, зависящий от него,
    /// означал бы, что один и тот же запрос к одним и тем же данным даёт разные
    /// суммы на машинах разной ширины и в разное время суток — потому что доля
    /// потоков считается по загрузке. Объяснить это клиенту нечем, а допуск
    /// лишь прячет расхождение до того дня, когда оно вылезет за него.
    /// Делёный сегмент отвечает ровно то же, что неделёный.
    ///
    /// ## Почему одного сегмента мало для спокойствия
    ///
    /// До появления полос счёт делил только сегменты, и слитый индекс считался
    /// одним потоком из шестнадцати. Полосы это чинят, но вводят новый класс
    /// расхождений: полоса, потерявшая свой край, отдаёт **правдоподобный**
    /// ответ — меньше на несколько документов, без отказа и без строки в
    /// журнале. Поймать такое можно только сравнением с однопоточным ответом.
    ///
    /// Проверяются все виды сразу: счёт, статистика, корзины, верхушка и
    /// мощность. Дробных здесь нет намеренно — их планировщик делить
    /// отказывается, и проверяет это соседний тест.
    ///
    /// **Тест обязан убедиться, что деление вообще случилось.** Корпус меньше
    /// порога полосы не делится, и тогда сравнение однопоточного ответа с
    /// однопоточным же ничего не значит.
    #[test]
    fn a_split_segment_answers_exactly_as_an_unsplit_one() {
        const N: i64 = 70_000;
        let dir = std::env::temp_dir()
            .join(format!("svindb-aggs-split-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "price".into(), "kw".into()],
            vec![1.0; 3],
            vec![Kind::Text, Kind::Int64, Kind::Keyword],
            (N as u32) + 10,
        );
        for d in 0..N {
            let price = (d % 997).to_string();
            let kw = format!("k{}", d % 31);
            ix.add_cells(
                &format!("d{d:07}"),
                &[1, 1, 1],
                &[
                    Cell { field: 0, slot: 0, text: "документ" },
                    Cell { field: 1, slot: 0, text: &price },
                    Cell { field: 2, slot: 0, text: &kw },
                ],
            );
        }
        ix.flush_now();
        drop(ix);

        let segs = Segments::load(&dir).expect("индекс");
        assert_eq!(segs.list.len(), 1, "корпус обязан лечь одним сегментом");

        let aggs = [
            Agg {
                name: "n".into(),
                kind: AggKind::Stats { field: None },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
            Agg {
                name: "s".into(),
                kind: AggKind::Stats { field: Some(1) },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
            Agg {
                name: "h".into(),
                kind: AggKind::Histogram {
                    field: 1,
                    interval: 100,
                    offset: 0,
                    step: crate::aggs::Step::Int { interval: 100, offset: 0 },
                },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
            Agg {
                name: "t".into(),
                kind: AggKind::Terms { field: 2, size: 10 },
                over: Over::Docs,
                space: crate::aggs::Space::Keyword,
            },
            Agg {
                name: "c".into(),
                kind: AggKind::Cardinality { field: 1 },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
        ];
        let spec = QuerySpec { nested: Some(SpecBool::default()), k: 1, ..QuerySpec::default() };

        // Деление обязано состояться, иначе сравнивать нечего.
        let mine: Vec<usize> = (0..segs.list.len()).collect();
        let jobs = segs.plan_jobs(&mine, &spec, &aggs, 8);
        assert!(
            jobs.len() > 1,
            "сегмент не разделился на полосы — тест сравнил бы один поток сам с собой"
        );
        assert!(jobs.iter().all(|j| j.range.is_some()), "часть работ осталась целыми сегментами");

        let one = segs.aggregate_with(&spec, &aggs, 1);
        let Value::Stats { count, .. } = one.values[0].value else { panic!("не счёт") };
        assert_eq!(count, N as u64, "однопоточный счёт потерял документы");

        for threads in [2usize, 3, 4, 8, 16] {
            let many = segs.aggregate_with(&spec, &aggs, threads);
            assert_eq!(
                many.values, one.values,
                "при {threads} потоках ответ разошёлся с однопоточным"
            );
        }

        let stopped = Arc::new(crate::aggs::Guard::new(0));
        stopped.cancel();
        let cut = segs.aggregate_guarded(&spec, &aggs, 8, &stopped);
        assert!(cut.partial, "параллельный обход скрыл отмену");
        let Value::Stats { count: cut_count, .. } = cut.values[0].value else {
            panic!("первая агрегация перестала быть счётом")
        };
        assert!(cut_count < N as u64, "отменённые полосы досчитали весь сегмент");

        // Запрещённое слово требует словарного курсора и потому обязано
        // оставить сегмент целым. Проверяется сам план, а не только равенство
        // ответа: полный перебор случайно тоже мог бы дать верный результат и
        // скрыть нарушение ограничения.
        let with_not = QuerySpec {
            must_not: vec![SpecWord {
                given: None,
                fid: 0,
                terms: vec![Term { text: "запрещено".into(), prefix: false }],
            }],
            nested: Some(SpecBool::default()),
            k: 1,
            ..QuerySpec::default()
        };
        let not_jobs = segs.plan_jobs(&mine, &with_not, &aggs, 8);
        assert_eq!(not_jobs.len(), 1, "must_not ошибочно разделил сегмент");
        assert!(not_jobs[0].range.is_none());

        // Узкий диапазон ведёт обход собственной колонкой. Полосы по номерам
        // документов с ним не смешиваются, пока у cursor нет seek к границе.
        let selective = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: 0, to: 0 }],
            nested: Some(SpecBool::default()),
            k: 1,
            ..QuerySpec::default()
        };
        let filter_jobs = segs.plan_jobs(&mine, &selective, &aggs, 8);
        assert_eq!(filter_jobs.len(), 1, "ведущий диапазон ошибочно разделил сегмент");
        assert!(filter_jobs[0].range.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отбор, не подошедший ни к одному сегменту, не роняет узел.
    ///
    /// ## Чем это обернулось
    ///
    /// Планировщик работ делил число потоков на число подошедших сегментов, а
    /// сторож проверял только «сегментов не меньше, чем потоков». При нуле
    /// сегментов это ложно, и следом шло деление на ноль. В выпускной сборке
    /// с `panic = "abort"` паника уносит **весь узел**: снаружи это выглядело
    /// как «узел не ответил на сброс», и найти причину удалось только по его
    /// собственному журналу.
    ///
    /// Пустой состав — обычное дело, а не край: границы поля в заголовке
    /// сегмента прямо говорят, что подходящих документов здесь нет, и счёт
    /// обязан ответить нулём, не открывая ничего.
    #[test]
    fn a_filter_matching_no_segment_does_not_divide_by_zero() {
        let dir = corpus("nosegments");
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() > 1, "нужно несколько сегментов");

        // Цены лежат в [0, 99]; этот диапазон не накрывает ни одного сегмента.
        let spec = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: 1_000_000, to: 2_000_000 }],
            k: 1,
            ..QuerySpec::default()
        };
        let aggs = [Agg {
            name: "n".into(),
            kind: AggKind::Stats { field: None },
            over: Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        for threads in [1usize, 2, 4, 8, 16] {
            let got = segs.aggregate_with(&spec, &aggs, threads);
            assert_eq!(
                got.values[0].value,
                Value::Stats { count: 0, min: None, max: None, sum: 0, fsum: None },
                "при {threads} потоках пустой отбор ответил не нулём"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Много маленьких сегментов не оправдывает сериализацию большого.
    #[test]
    fn skewed_segments_split_even_when_segments_outnumber_workers() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-aggs-skew-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        let mut indexer = Indexer::typed(
            &dir,
            vec!["value".into(), "tag".into()],
            vec![1.0; 2],
            vec![Kind::Int64, Kind::Keyword],
            1_000_000,
        );
        let mut n = 0u64;
        for part in 0..9 {
            let len = if part == 0 { 196_608 } else { 1024 };
            for _ in 0..len {
                let value = (n % 997).to_string();
                let tag = format!("k{}", n % 31);
                indexer.add(&format!("d{n}"), &[&value, &tag]);
                n += 1;
            }
            indexer.flush_now();
        }
        drop(indexer);
        let segs = Segments::load(&dir).unwrap();
        assert_eq!(segs.list.len(), 9);
        let spec = QuerySpec { nested: Some(SpecBool::default()), ..QuerySpec::default() };
        let aggs = [
            Agg {
                name: "count".into(),
                kind: AggKind::Stats { field: None },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
            Agg {
                name: "stats".into(),
                kind: AggKind::Stats { field: Some(0) },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
            Agg {
                name: "hll".into(),
                kind: AggKind::Cardinality { field: 1 },
                over: Over::Docs,
                space: crate::aggs::Space::Keyword,
            },
            Agg {
                name: "terms".into(),
                kind: AggKind::Terms { field: 1, size: 10 },
                over: Over::Docs,
                space: crate::aggs::Space::Keyword,
            },
        ];
        let mine: Vec<_> = (0..segs.list.len()).collect();
        let largest = *mine.iter().max_by_key(|&&i| segs.list[i].docs).unwrap();
        let reference = segs.aggregate_with(&spec, &aggs, 1);
        assert!(!reference.partial);
        let Value::Stats { count, .. } = reference.values[0].value else { panic!("не count") };
        assert_eq!(count, n);
        for workers in [2, 4] {
            let jobs = segs.plan_jobs(&mine, &spec, &aggs, workers);
            let mut ranges: Vec<_> = jobs
                .iter()
                .filter(|j| j.at == largest)
                .map(|j| j.range.expect("крупный сегмент не разделён"))
                .collect();
            ranges.sort_unstable();
            assert!(ranges.len() >= 2);
            assert_eq!(ranges[0].0, 0);
            assert_eq!(ranges.last().unwrap().1 as usize, segs.list[largest].docs);
            for adjacent in ranges.windows(2) {
                assert_eq!(adjacent[0].1, adjacent[1].0);
            }
            let parallel = segs.aggregate_with(&spec, &aggs, workers);
            assert!(!parallel.partial);
            assert_eq!(parallel.values, reference.values);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Дробную агрегацию планировщик делить отказывается.
    ///
    /// Сложение с плавающей точкой неассоциативно, а договор обещает побитовое
    /// равенство с однопоточным ответом. Обещание держится не осторожностью на
    /// месте, а тем, что такой сегмент не делится вовсе.
    #[test]
    fn a_float_aggregation_is_never_split() {
        let dir = floats("nosplit", 1 << 20);
        let segs = Segments::load(&dir).expect("индекс");
        assert_eq!(segs.list.len(), 1, "нужен один сегмент, вышло {}", segs.list.len());

        let aggs = [Agg {
            name: "s".into(),
            kind: AggKind::Stats { field: Some(1) },
            over: Over::Docs,
            space: crate::aggs::Space::Float,
        }];
        let spec = QuerySpec { nested: Some(SpecBool::default()), k: 1, ..QuerySpec::default() };
        let mine: Vec<usize> = (0..segs.list.len()).collect();
        let jobs = segs.plan_jobs(&mine, &spec, &aggs, 16);
        assert_eq!(jobs.len(), 1, "дробная агрегация разделена на {} работ", jobs.len());
        assert!(jobs[0].range.is_none(), "дробная агрегация получила полосу");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_sum_does_not_depend_on_how_many_threads_counted_it() {
        let dir = floats("threads", 128);
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() >= 8, "нужно много сегментов, вышло {}", segs.list.len());

        let aggs = [Agg {
            name: "s".into(),
            kind: AggKind::Stats { field: Some(1) },
            over: Over::Docs,
            space: crate::aggs::Space::Float,
        }];
        let one = segs.aggregate_with(&all(), &aggs, 1);
        let Value::Stats { fsum: Some(want), count, .. } = one.values[0].value else {
            panic!("не дробная статистика: {:?}", one.values[0].value)
        };
        assert_eq!(count, u64::from(DOCS), "посчитаны не все документы");
        assert!(want.is_finite() && want > 0.0, "сумма {want} бессмысленна");

        for threads in [2usize, 3, 4, 8, 16] {
            let many = segs.aggregate_with(&all(), &aggs, threads);
            let Value::Stats { fsum: Some(got), .. } = many.values[0].value else {
                panic!("не дробная статистика при {threads} потоках")
            };
            assert_eq!(
                got.to_bits(),
                want.to_bits(),
                "при {threads} потоках сумма {got:e} против {want:e} при одном — \
                 разошлась на {} единиц последнего разряда",
                (got.to_bits() as i64 - want.to_bits() as i64).abs()
            );
            assert_eq!(many.values, one.values, "разошлось не только в сумме");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отмена обрывает счёт и **говорит** об этом.
    ///
    /// Молчаливая неполнота хуже отказа: сумма, выданная как полная, выглядит
    /// ответом, и по ней принимают решения.
    ///
    /// Проверяется отменой, а не сроком: срок обязан истечь по часам, и тест на
    /// нём мерил бы скорость машины, а не работу сторожа. Часы проверены
    /// отдельно, у самого сторожа.
    #[test]
    fn a_cancelled_count_stops_and_says_so() {
        // Корпуса нужно больше шага сверки: сторож обязан не мешать там, где
        // работы на глаз, и обязан сработать там, где её много. Здесь две
        // тысячи документов при шаге в тысячу — то есть сверки будут, и
        // сегментов при этом восемь, чтобы счётчик заодно проверил, что он
        // живёт на потоке, а не заводится заново на каждом сегменте.
        let dir = floats("guard", 250);
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() >= 8, "нужно много мелких сегментов");
        let aggs = [Agg {
            name: "s".into(),
            kind: AggKind::Stats { field: Some(1) },
            over: Over::Docs,
            space: crate::aggs::Space::Float,
        }];

        let stopped = Arc::new(crate::aggs::Guard::new(0));
        stopped.cancel();
        let cut = segs.aggregate_guarded(&all(), &aggs, 1, &stopped);
        assert!(cut.partial, "сторож не сработал и не признался");
        let Value::Stats { count: got, .. } = cut.values[0].value else {
            panic!("не статистика")
        };
        assert!(
            u32::try_from(got).unwrap_or(u32::MAX) < DOCS,
            "обход дошёл до конца: {got} документов"
        );
        assert!(
            got <= u64::from(crate::aggs::Guard::STRIDE),
            "обход прошёл {got} документов при шаге сверки {} — значит счётчик \
             заводится заново на каждом сегменте",
            crate::aggs::Guard::STRIDE
        );

        // А без отмены и без срока — полный ответ и никакого признака неполноты.
        let patient = Arc::new(crate::aggs::Guard::new(0));
        let full = segs.aggregate_guarded(&all(), &aggs, 1, &patient);
        assert!(!full.partial, "полный ответ помечен неполным");
        let Value::Stats { count: all_docs, .. } = full.values[0].value else {
            panic!("не статистика")
        };
        assert_eq!(all_docs, u64::from(DOCS), "без сторожа обязан посчитать всё");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Корпус, где у сегментов **разные** наборы ключевых слов.
    ///
    /// Соседний `corpus` кладёт во все документы одни и те же два значения, и
    /// от этого ординалы словаря у сегментов совпадают случайно. Пока они
    /// совпадают, слияние верхушки по ординалу выглядит верным. Здесь набор
    /// меняется от сегмента к сегменту — и совпадение пропадает.
    fn split_keywords(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-aggs-{name}"));
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "цвет".into()],
            vec![1.0; 2],
            vec![Kind::Text, Kind::Keyword],
            100,
        );
        // Три сегмента по сто документов, и в каждом свой набор цветов.
        // Первый: только «синий». Второй: «алый» и «синий». Третий: «алый».
        for d in 0..300i64 {
            let colour = match d / 100 {
                0 => "синий",
                1 => {
                    if d % 2 == 0 {
                        "алый"
                    } else {
                        "синий"
                    }
                }
                _ => "алый",
            };
            ix.add_cells(
                &format!("d{d}"),
                &[1],
                &[
                    Cell { field: 0, slot: 0, text: "документ" },
                    Cell { field: 1, slot: 0, text: colour },
                ],
            );
        }
        ix.flush_now();
        dir
    }

    /// Верхушка по ключевому слову складывается по значению, а не по ординалу.
    ///
    /// Ординал — это позиция слова в словаре **своего** сегмента, и у разных
    /// сегментов она означает разные строки. Слияние по нему складывало счётчики
    /// несвязанных значений, а печать разрешала ключ через словарь нулевого
    /// сегмента, то есть подписывала сумму чужим именем.
    ///
    /// Здесь «синий» встречается 150 раз (100 в первом сегменте и 50 во втором),
    /// «алый» — тоже 150 (50 во втором и 100 в третьем). Ординалы у них в разных
    /// сегментах разные: в первом словарь состоит из одного «синий», в третьем —
    /// из одного «алый», и оба получают номер ноль.
    #[test]
    fn a_keyword_top_merges_by_value_not_by_ordinal() {
        let dir = split_keywords("split-kw");
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() >= 3, "нужно хотя бы три сегмента, вышло {}", segs.list.len());

        let got = segs.aggregate(
            &all(),
            &[Agg {
                name: "цвета".into(),
                kind: AggKind::Terms { field: 1, size: 10 },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            }],
        );
        let Value::Terms { top, .. } = &got[0].value else { panic!("не верхушка") };

        // Проверяются счётчики, а не подписи: они не зависят от того, чем
        // представлен ключ, и промах виден на них одинаково. При слиянии по
        // ординалу выходит 250 и 50 — «синий» первого сегмента складывается с
        // «алым» второго и третьего, потому что у всех троих номер один.
        let mut counts: Vec<u64> = top.iter().map(|(_, c)| *c).collect();
        counts.sort_unstable();
        assert_eq!(
            counts,
            vec![150u64, 150u64],
            "счётчики сложились по ординалу, а не по значению: {top:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Мощность keyword-поля считается по тексту, а не по местному ordinal.
    ///
    /// В первом и третьем сегментах словарь состоит из одного слова, поэтому
    /// и «синий», и «алый» имеют ordinal 0. Прежний код подавал этот ноль в HLL
    /// и отвечал `1`, хотя различных значений два.
    #[test]
    fn keyword_cardinality_merges_by_value_not_by_ordinal() {
        let dir = split_keywords("split-kw-cardinality");
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() >= 3, "нужно хотя бы три сегмента");

        let got = segs.aggregate(
            &all(),
            &[Agg {
                name: "мощность".into(),
                kind: AggKind::Cardinality { field: 1 },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            }],
        );
        let Value::Cardinality { value } = got[0].value else { panic!("не мощность") };
        assert_eq!(value, 2, "различных цветов два, оценка вернула {value}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Запрос без слов берёт кандидатов у отбора, а не перебирает сегмент.
    ///
    /// ## Что здесь доказывается
    ///
    /// Два утверждения сразу, и порознь ни одно не годится.
    ///
    /// **Ответ тот же.** Отбор называет кандидатов приблизительно — корзина
    /// шире условия, — и уточнять их обязан обход. Сверка идёт с пересчётом по
    /// корпусу, который известен наперёд: цена документа `d` равна `d % 100`,
    /// значит в `[10, 19]` попадает ровно сто документов из тысячи.
    ///
    /// **Работы меньше.** Прежде обход шёл по `0..ix.docs` и предъявлял
    /// проверке каждый документ сегмента — тысячу, чтобы отобрать сотню. Это и
    /// есть измеренные 3.89 QPS у `count`. Счётчик `scan_visited` меряет ровно
    /// то, что изменилось: сколько кандидатов дошло до проверки.
    #[test]
    fn a_wordless_filter_does_not_walk_the_whole_segment() {
        let dir = corpus("wordless");
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() > 1, "корпус обязан лечь в несколько сегментов");
        let total: u64 = segs.list.iter().map(|ix| ix.docs as u64).sum();

        let spec = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: 10, to: 19 }],
            k: 1,
            ..QuerySpec::default()
        };
        let stat = [Agg {
            name: "s".into(),
            kind: AggKind::Stats { field: Some(1) },
            over: Over::Docs,
            space: crate::aggs::Space::Int,
        }];

        let _ = crate::stats::take();
        let got = segs.aggregate(&spec, &stat);
        let seen = crate::stats::take().scan_visited;

        let Value::Stats { count, min, max, sum, .. } = got[0].value else {
            panic!("не статистика: {:?}", got[0].value)
        };
        assert_eq!(count, 100, "в диапазоне ровно сто документов");
        assert_eq!((min, max), (Some(10), Some(19)), "границы диапазона");
        assert_eq!(sum, (10..20i128).map(|v| v * 10).sum::<i128>(), "сумма по диапазону");

        if crate::stats::ENABLED {
            assert!(
                seen < total / 2,
                "обход предъявил {seen} документов из {total} — отбор не повёл, \
                 значит кандидаты снова берутся перебором"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Счёт по заголовкам вёдер даёт то же, что обход, и почти не читает.
    ///
    /// ## Как устроен оракул
    ///
    /// Тот же запрос с добавленным словом, которое есть у **каждого**
    /// документа. Множество совпадений от этого не меняется ни на документ, а
    /// быстрый путь выключается: у него первое условие — «ни одного слова».
    /// То есть сравниваются два способа посчитать одно и то же, а не два разных
    /// вопроса.
    #[test]
    fn counting_by_bucket_headers_agrees_with_the_walk() {
        let dir = corpus("bulk");
        let segs = Segments::load(&dir).expect("индекс");

        let aggs = [
            Agg {
                name: "n".into(),
                kind: AggKind::Stats { field: None },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
            Agg {
                name: "h".into(),
                kind: AggKind::Histogram {
                    field: 1,
                    interval: 5,
                    offset: 0,
                    step: crate::aggs::Step::Int { interval: 5, offset: 0 },
                },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
            Agg {
                name: "r".into(),
                kind: AggKind::Range { field: 1, edges: vec![(10, Some(15)), (15, Some(20))] },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            },
        ];

        let fast = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: 10, to: 19 }],
            k: 1,
            ..QuerySpec::default()
        };
        let walk = QuerySpec {
            words: vec![SpecWord {
                fid: 0, terms: vec![Term::parse("документ")], given: None
            }],
            filter: vec![SpecRange { fid: 1, from: 10, to: 19 }],
            k: 1,
            ..QuerySpec::default()
        };

        let _ = crate::stats::take();
        let by_headers = segs.aggregate(&fast, &aggs);
        let read_fast = crate::stats::take().scan_visited;
        let by_walk = segs.aggregate(&walk, &aggs);
        let read_walk = crate::stats::take().scan_visited;

        assert_eq!(by_headers, by_walk, "заголовки вёдер разошлись с обходом");
        // И числа те, которые известны наперёд: цена документа `d` равна
        // `d % 100`, значит в `[10, 19]` сто документов, по пятьдесят на каждую
        // половину.
        assert_eq!(
            by_headers[0].value,
            Value::Stats { count: 100, min: None, max: None, sum: 0, fsum: None }
        );
        assert_eq!(by_headers[1].value, Value::Buckets(vec![(10, 50), (15, 50)]));
        assert_eq!(by_headers[2].value, Value::Buckets(vec![(0, 50), (1, 50)]));

        if crate::stats::ENABLED {
            // Мерка — не «меньше, чем обходом со словом» (это дал бы и обычный
            // отбор), а «меньше четверти подошедших»: заголовок обязан заменить
            // чтение, а не сократить его. Замерено: двадцать документов краевых
            // вёдер против ста подошедших; без счёта по заголовкам — ровно сто.
            assert_eq!(read_walk, 1000, "обход со словом обязан пройти весь корпус");
            assert!(
                read_fast * 4 < 100,
                "по заголовкам прочитано {read_fast} документов из ста подошедших — \
                 значит вёдра не считались, а перебирались"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Документ на самом краю разрядной сетки не засчитывается отбором, который
    /// его исключает.
    ///
    /// ## Почему именно край
    ///
    /// Счёт по заголовкам принимает край ведра за обещание: ведро, чьи края
    /// лежат внутри отбора, засчитывается целиком, не читая ни одного значения.
    /// Верхний край строится сложением в беззнаковом пространстве, и у колонки
    /// с максимумом ровно `i64::MAX` эта сумма упирается в потолок `u64`.
    /// Насыщение уводило объявленный край на единицу **ниже** значения, которое
    /// ведро держит.
    ///
    /// Разбор на бумаге этого не поймал: арифметика верна в целых числах и
    /// неверна в `u64` — `ord(hi) + 1` не всегда представимо. Поймал перебор
    /// значений у самого края.
    #[test]
    fn a_document_at_the_edge_of_the_range_is_not_counted_by_a_filter_that_excludes_it() {
        let dir = std::env::temp_dir().join("svindb-aggs-edge");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "v".into()],
            vec![1.0; 2],
            vec![Kind::Text, Kind::Int64],
            100_000,
        );
        // Размах по всей сетке, и ровно один документ на самом верхнем краю.
        for d in 0..400i64 {
            let v = (d - 200) * 4_000_000_000_000_000;
            ix.add_cells(
                &format!("d{d}"),
                &[1, 1],
                &[
                    Cell { field: 0, slot: 0, text: "документ" },
                    Cell { field: 1, slot: 0, text: &v.to_string() },
                ],
            );
        }
        ix.add_cells(
            "край",
            &[1, 1],
            &[
                Cell { field: 0, slot: 0, text: "документ" },
                Cell { field: 1, slot: 0, text: &i64::MAX.to_string() },
            ],
        );
        ix.flush_now();
        drop(ix);

        let segs = Segments::load(&dir).expect("индекс");
        let count = [Agg {
            name: "n".into(),
            kind: AggKind::Stats { field: None },
            over: Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        // Отбор исключает ровно один документ — тот, что на краю.
        let spec = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: i64::MIN, to: i64::MAX - 1 }],
            k: 1,
            ..QuerySpec::default()
        };
        let got = segs.aggregate(&spec, &count);
        let Value::Stats { count: n, .. } = got[0].value else {
            panic!("не статистика")
        };
        assert_eq!(n, 400, "документ с края засчитан отбором, который его исключает");

        // А отбор, который его включает, обязан его увидеть.
        let all = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: i64::MIN, to: i64::MAX }],
            k: 1,
            ..QuerySpec::default()
        };
        let every = segs.aggregate(&all, &count);
        let Value::Stats { count: m, .. } = every[0].value else {
            panic!("не статистика")
        };
        assert_eq!(m, 401, "документ с края потерялся");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Удалённый документ не считается по заголовку ведра.
    ///
    /// ## Почему это отдельный тест
    ///
    /// Ведро строится при сборке сегмента и о позднейших удалениях не знает:
    /// мёртвый документ лежит в нём наравне с живым, и его число сидит в
    /// заголовке. Промах здесь не заметен ничем — счёт просто больше на число
    /// удалённых, и выглядит он совершенно правдоподобно.
    #[test]
    fn a_deleted_document_is_not_counted_by_a_header() {
        let dir = std::env::temp_dir().join("svindb-aggs-dead");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "цена".into()],
            vec![1.0; 2],
            vec![Kind::Text, Kind::Int32],
            10_000,
        );
        for d in 0..1000i64 {
            ix.add_cells(
                &format!("d{d}"),
                &[1],
                &[
                    Cell { field: 0, slot: 0, text: "документ" },
                    Cell { field: 1, slot: 0, text: &(d % 100).to_string() },
                ],
            );
        }
        ix.flush_now();
        drop(ix);

        let segs = Segments::load(&dir).expect("индекс");
        assert_eq!(segs.list.len(), 1, "корпус обязан лечь одним сегментом");
        // Убить половину подошедших: цены 10..14 — это пятьдесят документов.
        // Гашением по номеру, а не через приём: тут проверяется не удаление, а
        // то, что счёт по заголовкам о нём знает.
        let mut killed = 0usize;
        for d in 0..1000u32 {
            if (10..15).contains(&(i64::from(d) % 100)) {
                assert!(segs.list[0].kill(d), "документ {d} уже мёртв");
                killed += 1;
            }
        }
        assert_eq!(killed, 50, "убито не пятьдесят");
        assert!(!segs.list[0].all_live(), "гашение не отметилось");
        let aggs = [Agg {
            name: "n".into(),
            kind: AggKind::Stats { field: None },
            over: Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        let spec = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: 10, to: 19 }],
            k: 1,
            ..QuerySpec::default()
        };
        let got = segs.aggregate(&spec, &aggs);
        assert_eq!(
            got[0].value,
            Value::Stats { count: 50, min: None, max: None, sum: 0, fsum: None },
            "мёртвые посчитаны вместе с живыми"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отбор по редко заполненному полю находит своих — и в счёте, и в поиске.
    ///
    /// ## Почему это отдельный тест, а не строка в соседнем
    ///
    /// Поле, заполненное у сотой части документов, ложится в сегмент **другой
    /// раскладкой** — разреженной. У неё блок держит сто двадцать восемь
    /// присутствующих значений, а не сто двадцать восемь подряд идущих
    /// документов, и всякий обход, считающий иначе, отдаёт ноль.
    ///
    /// Ноль здесь особенно опасен: он выглядит законным ответом. Ни отказа, ни
    /// строки в журнале — просто «по вашему условию ничего нет», и проверить это
    /// можно только пересчётом, который известен наперёд.
    ///
    /// Проверяются оба пути сразу. Они расходились: счёт до этой фазы шёл полным
    /// перебором сегмента и отвечал верно, поиск брал кандидатов у отбора и
    /// отвечал нулём. Стоило счёту перейти на общий с поиском генератор — и
    /// верным перестал быть и он.
    #[test]
    fn a_filter_on_a_rarely_filled_field_finds_its_documents() {
        const N: u32 = 20_000;
        let dir = std::env::temp_dir().join("svindb-aggs-sparse");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "цена".into()],
            vec![1.0; 2],
            vec![Kind::Text, Kind::Int64],
            N + 10,
        );
        for d in 0..N {
            let v = d.to_string();
            let mut cells = vec![Cell { field: 0, slot: 0, text: "документ" }];
            // Значение у одного из ста: этого хватает, чтобы приём выбрал
            // разреженную раскладку.
            if d % 100 == 0 {
                cells.push(Cell { field: 1, slot: 0, text: &v });
            }
            ix.add_cells(&format!("d{d}"), &[1, 1], &cells);
        }
        ix.flush_now();
        drop(ix);

        let head = crate::index::read_head(&format!("{dir}/seg0000")).expect("заголовок");
        let f = head.fields.iter().find(|f| f.name == "цена").expect("поле объявлено");
        assert_eq!(
            f.layout,
            crate::column::Layout::Sparse,
            "поле легло не разреженной раскладкой — проверять нечего"
        );

        let segs = Segments::load(&dir).expect("индекс");
        let spec = QuerySpec {
            filter: vec![SpecRange { fid: 1, from: 1000, to: 2000 }],
            k: 100,
            ..QuerySpec::default()
        };
        // Значение равно номеру, и заполнен каждый сотый: в [1000, 2000] их
        // одиннадцать — 1000, 1100, … 2000.
        let want = (1000..=2000u32).filter(|d| d % 100 == 0).count() as u64;
        assert_eq!(want, 11, "пересчёт корпуса не тот");

        let count = [Agg {
            name: "n".into(),
            kind: AggKind::Stats { field: None },
            over: Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        let got = segs.aggregate(&spec, &count);
        let Value::Stats { count: n, .. } = got[0].value else {
            panic!("не статистика")
        };
        assert_eq!(n, want, "счёт по разреженному полю потерял документы");

        let hits = segs.search_with(&spec, 1);
        assert_eq!(hits.hits.len() as u64, want, "поиск по разреженному полю потерял документы");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Обязательное слово, которого нет в сегменте, обнуляет и count вместе с
    /// фильтром.
    ///
    /// Это не просто проверка пустой выдачи. До исправления путь агрегации мог
    /// потерять факт, что `must` не разрешился в posting list, и перейти к
    /// обходу кандидатов фильтра как к `match_all`. Тогда ответ содержал
    /// `hits=0`, но положительный `count` и `complete=true` — самый опасный
    /// вариант ошибки, потому что клиент принимал его за точный результат.
    #[test]
    fn an_unresolved_required_term_makes_filtered_count_zero() {
        let dir = corpus("required-missing-filter");
        let segs = Segments::load(&dir).expect("индекс");
        let missing = QuerySpec {
            // Важно иметь второй, существующий курсор. Именно при таком
            // сочетании прежний `scanned(None)` ошибочно принимал документы
            // второго условия, хотя вся ветвь must уже была невыполнима.
            must: vec![
                SpecWord {
                    fid: 0, terms: vec![Term::parse("терм-которого-нет")], given: None
                },
                SpecWord { fid: 0, terms: vec![Term::parse("документ")], given: None },
            ],
            k: 10,
            ..QuerySpec::default()
        };
        let count = [Agg {
            name: "n".into(),
            kind: AggKind::Stats { field: None },
            over: Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        let result = segs.aggregate(&missing, &count);
        assert_eq!(
            result[0].value,
            Value::Stats { count: 0, min: None, max: None, sum: 0, fsum: None },
            "невыполнимый must не должен считаться по одному filter"
        );
        let hits = segs.search_with(&missing, 1);
        assert!(hits.hits.is_empty(), "невыполнимый must вернул документы");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Считалки по документам совпадают с честным пересчётом.
    #[test]
    fn document_aggregations_match_an_honest_count() {
        let dir = corpus("docs");
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() > 1, "корпус обязан лечь в несколько сегментов");

        let got = segs.aggregate(
            &all(),
            &[
                Agg {
                    name: "s".into(),
                    kind: AggKind::Stats { field: Some(1) },
                    over: Over::Docs,
                    space: crate::aggs::Space::Int,
                },
                Agg {
                    name: "h".into(),
                    kind: AggKind::Histogram {
                        field: 1,
                        interval: 10,
                        offset: 0,
                        step: crate::aggs::Step::Int { interval: 10, offset: 0 },
                    },
                    over: Over::Docs,
                    space: crate::aggs::Space::Int,
                },
                Agg {
                    name: "c".into(),
                    kind: AggKind::Cardinality { field: 1 },
                    over: Over::Docs,
                    space: crate::aggs::Space::Int,
                },
            ],
        );

        // Цена — это `d % 100` по тысяче документов: каждое значение по десять раз.
        let want_sum: i128 = (0..1000i64).map(|d| (d % 100) as i128).sum();
        assert_eq!(
            got[0].value,
            Value::Stats { count: 1000, min: Some(0), max: Some(99), sum: want_sum, fsum: None }
        );
        let Value::Buckets(b) = &got[1].value else { panic!("не корзины") };
        assert_eq!(b.len(), 10, "десять корзин по десять значений");
        for (key, n) in b.iter() {
            assert_eq!(*n, 100, "корзина {key}");
        }
        let Value::Cardinality { value } = got[2].value else { panic!("не мощность") };
        assert!((value as i64 - 100).abs() < 10, "различных цен 100, оценка {value}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Считалки по элементам массива: наблюдение — элемент, а не документ.
    #[test]
    fn element_aggregations_count_elements() {
        let dir = corpus("slots");
        let segs = Segments::load(&dir).expect("индекс");
        let got = segs.aggregate(
            &all(),
            &[
                Agg {
                    name: "n".into(),
                    kind: AggKind::Stats { field: None },
                    over: Over::Slots { group: 1 },
                    space: crate::aggs::Space::Int,
                },
                Agg {
                    name: "sum".into(),
                    kind: AggKind::Stats { field: Some(3) },
                    over: Over::Slots { group: 1 },
                    space: crate::aggs::Space::Int,
                },
                Agg {
                    name: "top".into(),
                    kind: AggKind::Terms { field: 2, size: 5 },
                    over: Over::Slots { group: 1 },
                    space: crate::aggs::Space::Int,
                },
            ],
        );
        // Элементов вдвое больше, чем документов.
        let Value::Stats { count, .. } = got[0].value else { panic!("не считалка") };
        assert_eq!(count, 2000, "наблюдение — элемент, а не документ");
        // Сумма счётчиков: d + 2d по всем документам.
        let want: i128 = (0..1000i128).map(|d| d + 2 * d).sum();
        let Value::Stats { sum, count, min, max, .. } = got[1].value else {
            panic!("не считалка")
        };
        assert_eq!((sum, count, min, max), (want, 2000, Some(0), Some(1998)));
        // Верхушка эмодзи: два значения по тысяче.
        let Value::Terms { top, other, .. } = &got[2].value else {
            panic!("не верхушка")
        };
        assert_eq!(top.len(), 2, "два различных эмодзи");
        assert!(top.iter().all(|(_, n)| *n == 1000), "каждый по тысяче: {top:?}");
        assert_eq!(*other, 0, "за верхушкой ничего не осталось");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отбор сужает и агрегацию тоже.
    #[test]
    fn a_filter_narrows_the_aggregation_too() {
        let dir = corpus("filter");
        let segs = Segments::load(&dir).expect("индекс");
        let mut spec = all();
        spec.filter = vec![SpecRange { fid: 1, from: 0, to: 9 }];
        let got = segs.aggregate(
            &spec,
            &[Agg {
                name: "s".into(),
                kind: AggKind::Stats { field: Some(1) },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            }],
        );
        let Value::Stats { count, min, max, .. } = got[0].value else {
            panic!("не считалка")
        };
        assert_eq!((count, min, max), (100, Some(0), Some(9)), "отбор до агрегации не дошёл");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отсутствующее слово означает ноль и для агрегации, а не весь сегмент.
    ///
    /// Верхушечный поиск возвращал пусто, но полный обход видел, что ни одного
    /// курсора не построено, и принимал это за запрос без слов. Поэтому один
    /// ответ одновременно содержал `hits: []` и `count: все документы`.
    #[test]
    fn a_missing_word_does_not_turn_an_aggregation_into_match_all() {
        let dir = corpus("missing-word");
        let segs = Segments::load(&dir).expect("индекс");
        let spec = QuerySpec {
            words: vec![SpecWord {
                fid: 0,
                terms: vec![Term::parse("такого-слова-в-корпусе-нет")],
                given: None,
            }],
            ..QuerySpec::default()
        };
        let got = segs.aggregate(
            &spec,
            &[Agg {
                name: "n".into(),
                kind: AggKind::Stats { field: None },
                over: Over::Docs,
                space: crate::aggs::Space::Int,
            }],
        );
        let Value::Stats { count, .. } = got[0].value else { panic!("не считалка") };
        assert_eq!(count, 0, "отсутствующее слово превратилось в match_all");
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Инкрементальная перезагрузка состава: что переоткрывается, а что нет.
#[cfg(test)]
mod schema_clash_tests {
    use super::*;
    use crate::index::Kind;
    use crate::indexer::{Cell, Indexer};

    /// Собирает сегмент, у которого `emoji` лежит либо в корне, либо в массиве,
    /// и кладёт его в каталог индекса под именем `segNNNN`.
    fn seg(root: &str, name: &str, no: usize, grouped: bool) -> String {
        let build = format!("{root}/{name}-build");
        std::fs::remove_dir_all(&build).ok();
        let mut ix = Indexer::typed(
            &build,
            vec!["text".into(), "emoji".into()],
            vec![1.0; 2],
            vec![Kind::Text, Kind::Keyword],
            1_000_000,
        );
        if grouped {
            ix.set_groups(vec![0, 1], vec![String::new(), "reactions".into()]);
        }
        for d in 0..5i64 {
            let cells = vec![
                Cell { field: 0, slot: 0, text: "документ" },
                Cell { field: 1, slot: 0, text: "up" },
            ];
            ix.add_cells(&format!("{name}{d}"), &[1, 1], &cells);
        }
        ix.flush_now();
        let out = format!("{root}/seg{no:04}");
        std::fs::rename(format!("{build}/seg0000"), &out).unwrap();
        std::fs::remove_dir_all(&build).ok();
        out
    }

    /// Каталог индекса из двух собранных сегментов.
    fn index_of(root: &str, names: &[&str]) -> String {
        crate::manifest::write(root, &names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
            .expect("опись сегментов");
        root.to_string()
    }

    /// Одно поле в разных группах — это два разных индекса в одном каталоге.
    ///
    /// Колонка группового поля адресуется ординалом элемента, а корневого —
    /// номером документа. Открой такие сегменты рядом — и отбор, сортировка и
    /// агрегация по этому полю читали бы в одном из них чужие ячейки.
    ///
    /// Группа подменяется **в заголовке**, а не выбором при сборке, и это не
    /// уловка ради теста. Собранные приёмом сегменты различием одной лишь
    /// группы не бывают: поле, попавшее в массив, неизбежно становится
    /// позиционным, и отказ пришёл бы от позиций, а проверка группы осталась бы
    /// пустой. А вот заголовок, правленный руками или собранный иной версией
    /// схемы, даёт ровно этот случай — и он обязан быть отвергнут.
    #[test]
    fn a_field_that_moved_into_another_group_does_not_open_beside_the_old_one() {
        let root = std::env::temp_dir().join("svindb-schema-clash-group");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        seg(&root, "a", 0, true);
        let b = seg(&root, "b", 1, true);
        // У поля `emoji` номер группы — тринадцатый столбец строки `field`.
        // Переставляем его на ноль, не трогая ни одного другого столбца.
        let head = format!("{b}/head.tsv");
        let text = std::fs::read_to_string(&head).unwrap();
        let fixed: String = text
            .lines()
            .map(|line| {
                let mut c: Vec<&str> = line.split('\t').collect();
                if c.first() == Some(&"field") && c.get(1) == Some(&"emoji") {
                    c[13] = "0";
                }
                c.join("\t")
            })
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&head, format!("{fixed}\n")).unwrap();
        assert_eq!(
            crate::index::load(&b).unwrap().fields[1].group,
            0,
            "подмена не доехала — проверять нечего"
        );

        let dir = index_of(&root, &["seg0000", "seg0001"]);
        let says = match Segments::load(&dir) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("разные группы сошли с рук"),
        };
        assert!(says.contains("не совпадает"), "не тот отказ: {says}");
        assert!(
            says.contains("группа1") && says.contains("emoji"),
            "отказ не назвал поле и его группу: {says}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Негодный путь группы в заголовке — отказ на открытии.
    ///
    /// Прежде путь проверялся только на управляющие символы и длину до 512
    /// байт, то есть заголовок принимал любую строку. Запрос по такой группе
    /// адресовался бы к тому, чего в документе нет и быть не может.
    #[test]
    fn a_group_path_that_is_not_a_path_is_refused_on_open() {
        let root = std::env::temp_dir().join("svindb-bad-group-path");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();
        let d = seg(&root, "a", 0, true);

        // Сперва убеждаемся, что сегмент исправен: иначе проверка ниже
        // доказывала бы лишь то, что он не открывается вовсе.
        crate::index::load(&d).expect("исходный сегмент обязан открываться");

        let head = format!("{d}/head.tsv");
        let text = std::fs::read_to_string(&head).unwrap();
        for bad in ["не путь!", "a..b", ".hidden"] {
            std::fs::write(&head, text.replace("reactions", bad)).unwrap();
            let says = match crate::index::load(&d) {
                Err(e) => e.to_string(),
                Ok(_) => panic!("путь {bad:?} принят"),
            };
            assert!(says.contains("путь группы"), "путь {bad:?}: не тот отказ: {says}");
        }
        std::fs::remove_dir_all(&root).ok();
    }

    /// У корневой группы пути не бывает: корень — это сам документ.
    #[test]
    fn the_root_group_may_not_carry_a_path() {
        let root = std::env::temp_dir().join("svindb-root-group-path");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();
        let d = seg(&root, "a", 0, true);

        let head = format!("{d}/head.tsv");
        let text = std::fs::read_to_string(&head).unwrap();
        let fixed: String = text
            .lines()
            .map(|line| {
                let mut c: Vec<&str> = line.split('\t').collect();
                if c.first() == Some(&"group") && c.get(1) == Some(&"0") {
                    c[2] = "корень";
                }
                c.join("\t")
            })
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&head, format!("{fixed}\n")).unwrap();

        let says = match crate::index::load(&d) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("путь у корневой группы принят"),
        };
        assert!(says.contains("корневой группы"), "не тот отказ: {says}");
        std::fs::remove_dir_all(&root).ok();
    }

    /// Один номер группы, означающий разные массивы, — тоже отказ.
    #[test]
    fn one_group_number_meaning_two_different_arrays_does_not_open() {
        let root = std::env::temp_dir().join("svindb-schema-clash-path");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let a = seg(&root, "a", 0, true);
        let b = seg(&root, "b", 1, true);
        let _ = &a;
        let head = format!("{b}/head.tsv");
        let text = std::fs::read_to_string(&head).unwrap().replace("reactions", "comments");
        std::fs::write(&head, text).unwrap();
        assert_eq!(
            crate::index::load(&b).unwrap().group_paths[1],
            "comments",
            "подмена не доехала — проверять нечего"
        );

        let dir = index_of(&root, &["seg0000", "seg0001"]);
        let says = match Segments::load(&dir) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("разные пути одного номера сошли с рук"),
        };
        assert!(
            says.contains("reactions") && says.contains("comments"),
            "отказ не назвал оба пути: {says}"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}

#[cfg(test)]
mod reopen_tests {
    use super::*;
    use crate::indexer::Indexer;

    /// Индекс из трёх сегментов по сто документов.
    fn build(name: &str) -> String {
        let dir = std::env::temp_dir()
            .join(format!("svindb-reopen-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::new(&dir, vec!["body".into()], vec![1.0], 100);
        for i in 0..300 {
            ix.add(&format!("d{i}"), &[&format!("общее слово{}", i % 7)]);
        }
        ix.flush_now();
        dir
    }

    /// Неизменившийся сегмент не открывается заново, а берётся тот же самый.
    ///
    /// Проверяется тождество указателя, а не равенство содержимого: содержимое
    /// совпало бы и после честного переоткрытия, и тогда тест молчал бы о том,
    /// ради чего написан. Переоткрытие сегмента — это полный разбор его словаря
    /// (`walk_terms`), и на тысячах сегментов оно и есть та задержка, которую
    /// видно на приёме и на `/_health`.
    #[test]
    fn an_unchanged_segment_is_reused_not_reopened() {
        let dir = build("reuse");
        let first = Segments::load(&dir).expect("индекс");
        assert!(first.list.len() >= 3, "нужно хотя бы три сегмента");

        let again = Segments::reopen(&dir, &first).expect("перезагрузка");
        assert_eq!(again.list.len(), first.list.len());
        for (a, b) in first.list.iter().zip(again.list.iter()) {
            assert!(Arc::ptr_eq(a, b), "сегмент {} открыт заново, хотя не менялся", a.dir);
        }
        assert_eq!(again.base, first.base, "начала обязаны совпасть");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Появившийся сегмент открывается, прежние — нет, начала пересчитываются.
    #[test]
    fn a_new_segment_is_opened_and_the_bases_are_recomputed() {
        let dir = build("grow");
        let first = Segments::load(&dir).expect("индекс");
        let was = first.list.len();
        let docs_before = first.docs();

        // Ещё сто документов — это ещё один сегмент.
        let mut ix = Indexer::new(&dir, vec!["body".into()], vec![1.0], 100);
        for i in 300..400 {
            ix.add(&format!("d{i}"), &[&format!("общее слово{}", i % 7)]);
        }
        ix.flush_now();
        drop(ix);

        let next = Segments::reopen(&dir, &first).expect("перезагрузка");
        assert_eq!(next.list.len(), was + 1, "новый сегмент не подхвачен");
        assert_eq!(next.docs(), docs_before + 100, "документы не сошлись");

        // Прежние — те же самые объекты.
        let reused =
            next.list.iter().filter(|b| first.list.iter().any(|a| Arc::ptr_eq(a, b))).count();
        assert_eq!(reused, was, "прежние сегменты открыты заново");

        // Начала обязаны быть пересчитаны по новому составу, а не перенесены.
        let mut acc = 0u64;
        for (i, ix) in next.list.iter().enumerate() {
            assert_eq!(next.base[i], acc, "начало сегмента {i} неверно");
            acc += ix.docs as u64;
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Compaction вправе заменить L0-хвост, не трогая префикс глобального
    /// каталога. Такая замена не должна mmap-ить тот же каталог заново.
    #[test]
    fn replacing_only_the_tail_keeps_the_open_id_directory() {
        let dir = build("id-tail");
        let first = Segments::load(&dir).expect("индекс");
        assert!(first.list.len() >= 3, "нужно хотя бы три сегмента");
        let prefix = vec![first.list[0].as_ref()];
        std::fs::write(format!("{dir}/{}", crate::catalog::FILE), crate::catalog::build(&prefix))
            .expect("каталог записан");

        let _ = crate::stats::take();
        let old = first.locate_ids(&["d10".to_string()]).expect("каталог открылся");
        assert!(!old[0].is_empty());

        // Средний хвостовой сегмент исчез, последний остался: новый состав не
        // начинается со всего прежнего, но покрытый первый сегмент тот же.
        let list = vec![Arc::clone(&first.list[0]), Arc::clone(&first.list[2])];
        let base = vec![0, first.list[0].docs as u64];
        let next = Segments::from_parts_reusing(list, base, &first);
        let tail = next.locate_ids(&["d250".to_string()]).expect("общий каталог потерян");
        assert!(!tail[0].is_empty(), "точный хвост не нашёл документ");

        if crate::stats::ENABLED {
            let counters = crate::stats::take();
            assert_eq!(counters.catalog_opened, 1, "замена хвоста переоткрыла каталог");
            assert_eq!(counters.catalog_used, 2);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отменённый point-read не должен получить частичный ответ каталога и
    /// затем незаметно перейти на полный обход сегментов.
    #[test]
    fn cancelled_id_directory_lookup_returns_before_catalog_work() {
        let dir = build("id-cancelled");
        let segments = Segments::load(&dir).expect("индекс");
        let guard = crate::work::Guard::new(0);
        guard.cancel();
        assert!(
            segments.locate_ids_guarded(&["d10".to_string()], Some(&guard)).is_none(),
            "отменённый lookup вернул места"
        );
        assert_eq!(guard.reason(), crate::work::Reason::Cancelled);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unavailable_directory_can_be_published_without_new_read_view() {
        let dir = build("id-late-publication");
        let segments = Segments::load(&dir).unwrap();
        let path = format!("{dir}/{}", crate::catalog::FILE);
        // Повреждённый/старый файл не должен навсегда отравлять OnceLock.
        std::fs::write(&path, "старый каталог").unwrap();
        assert!(!segments.prepare_id_directory());
        assert!(!segments.id_directory_ready());
        let sources: Vec<_> = segments.list.iter().map(Arc::as_ref).collect();
        let pending = format!("{path}.test-pending");
        std::fs::write(&pending, crate::catalog::build(&sources)).unwrap();
        std::fs::rename(&pending, &path).unwrap();
        let generation = segments.generation();
        assert!(segments.prepare_id_directory());
        assert!(segments.id_directory_ready());
        assert_eq!(generation, segments.generation());
        let found =
            segments.locate_ids_ready_guarded(&["d10".into(), "missing".into()], None).unwrap();
        assert!(!found[0].is_empty());
        assert!(found[1].is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ready_lookup_never_waits_for_builder_or_opens_catalog() {
        let dir = build("id-ready-only");
        let segments = Segments::load(&dir).unwrap();
        let sources: Vec<_> = segments.list.iter().map(Arc::as_ref).collect();
        std::fs::write(format!("{dir}/{}", crate::catalog::FILE), crate::catalog::build(&sources))
            .unwrap();
        let exclusive = segments.id_directory.building.lock().unwrap();
        let guard = crate::work::Guard::new(1000);
        let started = std::time::Instant::now();
        assert!(segments.locate_ids_ready_guarded(&["d10".into()], Some(&guard)).is_none());
        assert!(started.elapsed() < std::time::Duration::from_millis(250));
        assert!(!guard.fired());
        assert!(!segments.id_directory_ready());
        drop(exclusive);
        assert!(segments.prepare_id_directory_guarded(Some(&guard)));
        assert!(segments.locate_ids_ready_guarded(&["d10".into()], Some(&guard)).is_some());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn id_directory_wait_is_bounded_and_does_not_publish_cancelled_state() {
        let dir = build("id-wait-cancel");
        let segments = Segments::load(&dir).expect("индекс");
        let sources: Vec<_> = segments.list.iter().map(Arc::as_ref).collect();
        std::fs::write(format!("{dir}/{}", crate::catalog::FILE), crate::catalog::build(&sources))
            .unwrap();
        let exclusive = segments.id_directory.building.lock().unwrap();
        let guard = crate::work::Guard::new(20);
        let start = std::time::Instant::now();
        assert!(segments.locate_ids_guarded(&["d10".into()], Some(&guard)).is_none());
        assert!(start.elapsed() < std::time::Duration::from_millis(250));
        assert_eq!(guard.reason(), crate::work::Reason::Deadline);
        assert!(segments.id_directory.get().is_none(), "отмена закеширована как отсутствие файла");
        drop(exclusive);
        let found = segments.locate_ids(&["d10".into()]).expect("повторное открытие");
        assert!(!found[0].is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Каталог, снесённый и заведённый заново под тем же именем, не
    /// переиспользуется.
    ///
    /// Имени для опознания мало. Отображения снесённого каталога продолжают
    /// показывать содержимое отвязанных inode, поэтому переиспользование по
    /// имени означало бы выдачу из сегмента, которого на диске больше нет, — и
    /// ни одна проверка целости этого бы не увидела.
    #[test]
    fn a_directory_replaced_under_the_same_name_is_not_reused() {
        let dir = build("tezka");
        let first = Segments::load(&dir).expect("индекс");
        let victim = first.list[0].dir.clone();
        let docs_before = first.list[0].docs;

        // Тот же путь, другое содержимое: сто документов вместо ста, но другие,
        // и заголовок написан заново.
        std::fs::remove_dir_all(&victim).unwrap();
        let one = Indexer::new(&victim, vec!["body".into()], vec![1.0], 1_000_000);
        let mut one = one;
        for i in 0..7 {
            one.add(&format!("иной{i}"), &["другое"]);
        }
        one.flush_now();
        drop(one);
        // Индексатор кладёт сегмент внутрь каталога — поднимем его наверх, чтобы
        // путь остался тем же самым каталогом сегмента.
        let inner = format!("{victim}/seg0000");
        if std::path::Path::new(&inner).exists() {
            for e in std::fs::read_dir(&inner).unwrap().filter_map(|e| e.ok()) {
                let to = format!("{victim}/{}", e.file_name().to_string_lossy());
                std::fs::rename(e.path(), to).unwrap();
            }
            std::fs::remove_dir_all(&inner).ok();
            std::fs::remove_file(format!("{victim}/{}", crate::manifest::FILE)).ok();
        }

        let next = Segments::reopen(&dir, &first).expect("перезагрузка");
        assert!(
            !Arc::ptr_eq(&first.list[0], &next.list[0]),
            "тёзка переиспользован как тот же сегмент"
        );
        assert_ne!(
            next.list[0].docs, docs_before,
            "переоткрытый сегмент отдал прежнее число документов"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Параллельный обход сегментов: та же выдача и без потерь на дешёвом запросе.
#[cfg(test)]
mod parallel_tests {
    use super::*;
    use crate::indexer::Indexer;

    fn build(name: &str) -> (String, Segments) {
        let dir = std::env::temp_dir()
            .join(format!("svindb-par-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::new(&dir, vec!["body".into()], vec![1.0], 200);
        for i in 0..4000 {
            ix.add(&format!("d{i}"), &[&format!("общее слово{} редкое{}", i % 13, i)]);
        }
        ix.flush_now();
        drop(ix);
        let segs = Segments::load(&dir).expect("индекс");
        (dir, segs)
    }

    fn one(word: &str, k: usize) -> QuerySpec {
        QuerySpec {
            words: vec![SpecWord { fid: 0, terms: vec![Term::parse(word)], given: None }],
            k,
            ..QuerySpec::default()
        }
    }

    /// Выдача в потоках совпадает с последовательной **дословно**.
    ///
    /// Сверяются номера и веса до бита, а не число попаданий: слепок, где
    /// половина верхушки уехала на соседнюю позицию, по числу неотличим от
    /// здорового.
    #[test]
    fn threads_do_not_change_the_answer() {
        let (dir, segs) = build("same");
        assert!(segs.list.len() >= 10, "нужно много сегментов");
        let same = |name: &str, spec: &QuerySpec| {
            let a: Vec<(u64, u64)> =
                segs.search_with(spec, 1).hits.iter().map(|h| (h.doc, h.score.to_bits())).collect();
            let b: Vec<(u64, u64)> =
                segs.search_with(spec, 8).hits.iter().map(|h| (h.doc, h.score.to_bits())).collect();
            assert!(!a.is_empty(), "{name}: ничего не нашло — проверка вхолостую");
            assert_eq!(a, b, "{name}: выдача в потоках разошлась");
        };
        for (word, k) in [("общее", 10), ("общее", 500), ("слово3", 10), ("редкое7", 5)]
        {
            same(&format!("слово {word}, k={k}"), &one(word, k));
        }

        // Булевы: с этого коммита они тоже делятся между ядрами, и здесь
        // проверяется, что деление ничего не изменило. Порог у булева обхода
        // начинается с минус бесконечности, а общий порог потоков — с ноля, и
        // если бы какой-то из этих запросов отдавал попадание с нулевым весом,
        // расхождение вылезло бы ровно тут.
        let word = |w: &str| SpecWord { fid: 0, terms: vec![Term::parse(w)], given: None };
        same("must", &QuerySpec { must: vec![word("общее")], k: 10, ..QuerySpec::default() });
        same(
            "must + must_not",
            &QuerySpec {
                words: vec![word("общее")],
                must_not: vec![word("слово3")],
                k: 10,
                ..QuerySpec::default()
            },
        );
        same(
            "два must",
            &QuerySpec {
                must: vec![word("общее"), word("слово5")], k: 25, ..QuerySpec::default()
            },
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Отрицательный вес переживает деление на потоки.
    ///
    /// ## Откуда берётся отрицательный вес
    ///
    /// Множитель фразы задаётся запросом, и знак у него не ограничен: проверка
    /// требует лишь конечности и модуля не больше миллиона. Запрос «хочу это
    /// слово, но документы вот с этой фразой — в конец» так и пишется.
    ///
    /// ## Что ломалось
    ///
    /// Общий порог потоков хранится битами `f64` и накапливается `fetch_max`, а
    /// порядок битов совпадает с порядком чисел только у неотрицательных —
    /// поэтому при записи он прижимался к нулю. Последовательный обход заводит
    /// порог булева запроса минус бесконечностью, а параллельный начинал с
    /// нуля, и `passes` отбрасывал по нему всё с отрицательным весом.
    ///
    /// Один и тот же запрос к одним и тем же данным отдавал выдачу или пустоту
    /// в зависимости от числа рабочих потоков — величины, которой клиент не
    /// задаёт и не видит.
    #[test]
    fn a_negative_weight_survives_the_threads() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-neg-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        // Корпуса нужно больше порога, за которым запрос делится между потоками.
        const DOCS: usize = 60_000;
        let mut ix = Indexer::new(&dir, vec!["body".into()], vec![1.0], 2000);
        for i in 0..DOCS {
            let text = if i.is_multiple_of(10) {
                format!("общее красный шар номер{i}")
            } else {
                format!("общее прочее номер{i}")
            };
            ix.add(&format!("d{i}"), &[&text]);
        }
        ix.flush_now();
        drop(ix);
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() >= 10, "нужно много сегментов");

        let spec = || QuerySpec {
            must: vec![SpecWord { fid: 0, terms: vec![Term::parse("общее")], given: None }],
            phrases: vec![SpecPhrase {
                fid: 0,
                boost: -1000.0,
                terms: vec!["красный".to_string(), "шар".to_string()],
                slop: 0,
            }],
            k: 10,
            ..QuerySpec::default()
        };
        let one = segs.search_with(&spec(), 1).hits.len();
        assert!(one > 0, "в один поток ничего не нашлось — проверка вхолостую");
        for threads in [2usize, 4, 8] {
            assert_eq!(
                segs.search_with(&spec(), threads).hits.len(),
                one,
                "при {threads} потоках отрицательный вес выпал из выдачи"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Дешёвый запрос уходит на последовательный путь при любом числе сегментов.
    ///
    /// Прежний сторож проверял дешевизну только когда сегментов **меньше**, чем
    /// потоков, — то есть ровно там, где потоков и так поднимается мало. На
    /// многих сегментах редкое слово уходило в потоки и выходило медленнее
    /// последовательного.
    ///
    /// Проверяется не время, а решение: у последовательного пути `visited`
    /// считается по всем сегментам подряд, и совпадение с параллельным вызовом
    /// означает, что параллельный до потоков не дошёл.
    #[test]
    fn a_cheap_query_stays_sequential_however_many_segments() {
        let (dir, segs) = build("cheap");
        // Слово ровно из одного документа: постингов на порядки меньше порога.
        let spec = one("редкое42", 10);
        let seq = segs.search_with(&spec, 1);
        let par = segs.search_with(&spec, 8);
        assert_eq!(seq.visited, par.visited, "дешёвый запрос ушёл в потоки");
        assert_eq!(seq.hits.len(), par.hits.len());
        assert!(!seq.hits.is_empty(), "ничего не нашлось — проверка вхолостую");

        // А дорогой — доходит: у частого слова постингов выше порога.
        let heavy = one("общее", 500);
        assert!(
            segs.search_with(&heavy, 8).hits.len() == segs.search_with(&heavy, 1).hits.len(),
            "дорогой запрос дал разное число попаданий"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Сортировка и курсор страницы в потоках дают ту же выдачу, что в одном.
    ///
    /// ## Что здесь изменилось
    ///
    /// Оба класса запросов уводились в один поток. Довод был назван, и для
    /// курсора страницы он неверен: «строго после показанного» — чистый
    /// предикат по весу, ключам и номеру документа в общем пространстве, а
    /// номер проставляется до раздачи потокам. Порядка обхода ему не нужно.
    ///
    /// Для сортировки довод был верен наполовину: порог передавался **числом
    /// внутри запроса**, а запрос делится между потоками по ссылке. Делится
    /// сама величина: `k`-й у потока не хуже общего `k`-го, потому что общий
    /// набор шире, и `fetch_max` накапливает верную нижнюю границу.
    ///
    /// ## Почему сверка побитовая и с порядком
    ///
    /// Сортировка ставит порядок; слепок, где две соседние позиции поменялись
    /// местами, по числу попаданий неотличим от здорового. И слияние верхушек
    /// потоков шло **по весу** — при сортировке по полю это молча дало бы
    /// сортировку по весу, а веса у сортированных запросов ещё и совпадают
    /// целыми группами.
    #[test]
    fn sorting_and_paging_survive_the_threads() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-par-sort-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        // Корпуса нужно больше порога, за которым запрос вообще делится между
        // потоками: `PARALLEL_POSTINGS` постингов. На корпусе в четыре тысячи
        // документов сторож уводил запрос в один поток, и тест был зелёным при
        // любой ошибке деления — поймано контрэкспериментом, а не мыслью.
        const DOCS: i64 = 60_000;
        let mut ix = Indexer::typed(
            &dir,
            vec!["body".into(), "цена".into()],
            vec![1.0, 1.0],
            vec![crate::index::Kind::Text, crate::index::Kind::Int32],
            2000,
        );
        for i in 0..DOCS {
            // Цена не совпадает с порядком приёма: иначе сегменты легли бы
            // ровными отрезками значений, и порог отсекал бы их даже при
            // неверной передаче — тест бы этого не заметил.
            let price = (i * 7919) % 50_000;
            ix.add(&format!("d{i}"), &[&format!("общее слово{}", i % 13), &price.to_string()]);
        }
        ix.flush_now();
        drop(ix);
        let segs = Segments::load(&dir).expect("индекс");
        assert!(segs.list.len() >= 10, "нужно много сегментов");
        assert!(
            (DOCS as u64) >= PARALLEL_POSTINGS,
            "корпус ниже порога деления — проверка вхолостую"
        );

        let shot = |spec: &QuerySpec, threads: usize| -> Vec<(u64, u64, search::SortKey)> {
            segs.search_with(spec, threads)
                .hits
                .iter()
                .map(|h| (h.doc, h.score.to_bits(), h.keys[0]))
                .collect()
        };
        let word = |w: &str| SpecWord { fid: 0, terms: vec![Term::parse(w)], given: None };

        for (name, desc, k) in [("по убыванию", true, 10), ("по возрастанию", false, 25)]
        {
            let spec = QuerySpec {
                words: vec![word("общее")],
                sort: vec![Sort { fid: 1, desc }],
                k,
                ..QuerySpec::default()
            };
            let one = shot(&spec, 1);
            assert_eq!(one.len(), k, "{name}: верхушка не набралась");
            for threads in [2usize, 4, 8, 16] {
                assert_eq!(shot(&spec, threads), one, "{name}: {threads} потоков дали другое");
            }

            // И вторая страница — по курсору с первой.
            let last = one.last().copied().expect("верхушка непуста");
            let paged = QuerySpec {
                words: vec![word("общее")],
                sort: vec![Sort { fid: 1, desc }],
                after: Some((vec![last.2], last.0)),
                k,
                ..QuerySpec::default()
            };
            let page_one = shot(&paged, 1);
            assert_eq!(page_one.len(), k, "{name}: вторая страница не набралась");
            assert!(
                page_one.iter().all(|x| !one.contains(x)),
                "{name}: вторая страница повторяет первую"
            );
            for threads in [2usize, 4, 8, 16] {
                assert_eq!(
                    shot(&paged, threads),
                    page_one,
                    "{name}: страница в {threads} потоках разошлась"
                );
            }
        }

        // Курсор страницы без сортировки — по весу.
        let plain = QuerySpec { words: vec![word("общее")], k: 20, ..QuerySpec::default() };
        let first = shot(&plain, 1);
        let tail = first.last().copied().expect("верхушка непуста");
        let next = QuerySpec {
            words: vec![word("общее")],
            after: Some((vec![tail.1 as i64 as search::SortKey], tail.0)),
            k: 20,
            ..QuerySpec::default()
        };
        let seq = shot(&next, 1);
        assert!(!seq.is_empty(), "вторая страница по весу пуста — проверка вхолостую");
        for threads in [2usize, 4, 8, 16] {
            assert_eq!(shot(&next, threads), seq, "страница по весу в {threads} потоках разошлась");
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Потолок отображений на открытии набора: правило отказа и замер на сегмент.
#[cfg(test)]
mod map_gate_tests {
    use super::*;
    use crate::mmap::{map_budget, map_budget_floor, MapShortfall};

    /// Нехватка областей у боевого узла — отказ, и отказ говорит, что делать.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Без отказа узел открывает сегменты по одному и умирает на середине с
    /// «cannot allocate memory» — при вдоволь свободной памяти, после минут
    /// работы, и без единого слова о том, что упёрся он в число областей.
    /// Отказ обязан быть **`Result`**, а не паникой: выпуск собирается с
    /// `panic = "abort"`, и паника на пути открытия убила бы узел, не дав ему
    /// напечатать ни строки объяснения.
    #[test]
    fn a_refusing_node_returns_the_shortfall_as_an_error_with_instructions() {
        // 140 000 сегментов по 20 областей при пределе 65 530.
        let over = map_budget(140_000, 20, Some(384), 65_530);
        let e = map_verdict(&over, MapShortfall::Refuse)
            .expect_err("нехватка областей пропущена как достаток");
        assert_eq!(e.kind(), std::io::ErrorKind::OutOfMemory);
        let msg = e.to_string();
        assert!(
            msg.contains(&format!("sysctl -w vm.max_map_count={}", over.advised_limit())),
            "отказ не сказал, что делать: {msg}"
        );
        assert!(msg.contains("--target="), "отказ не назвал второй рычаг: {msg}");

        // Помещающийся индекс не отказывает и не предупреждает даже боевому.
        let ok = map_budget(100, 20, Some(384), 65_530);
        assert!(ok.fits());
        assert_eq!(
            map_verdict(&ok, MapShortfall::Refuse).expect("годный набор отвергнут"),
            None,
            "по годному набору сказано лишнее"
        );
    }

    /// Мягкое правило предупреждает — но только в своей полосе.
    ///
    /// ## Где эта полоса и почему она не бесконечна
    ///
    /// Мягкость нужна там, где нехватка **будущая**: потребность перевалила за
    /// рабочий потолок (девять десятых предела), но в сам предел ещё лезет.
    /// Индекс откроется, а вот сбросы и слияния однажды упрутся — об этом и
    /// говорит предупреждение.
    ///
    /// Выше жёсткого предела мягкости нет и быть не может: там `mmap` откажет
    /// **на этом самом открытии**, а у рассыпанного сегмента такой отказ
    /// молчаливый — три десятка мест зовут `try_open`, то есть `open(...).ok()`,
    /// и неоткрывшийся файл колонки становится «поля нет». Предупредить и
    /// пропустить значило бы отдать неполную выдачу, неотличимую от честной.
    #[test]
    fn a_warning_node_opens_the_same_index_and_says_the_same_numbers() {
        // Полоса мягкости: 60 000 больше потолка 58 977, но меньше предела.
        let over = map_budget(2_982, 20, Some(360), 65_530);
        assert!(!over.fits(), "набор обязан быть выше рабочего потолка");
        assert!(!over.peaks_over(), "и при этом ниже жёсткого предела: {}", over.need);
        let told = map_verdict(&over, MapShortfall::Warn)
            .expect("мягкое правило отказало в своей полосе")
            .expect("мягкое правило смолчало о нехватке");
        assert_eq!(told, over.explain(), "предупреждение говорит не то же, что отказ");
        // Тот же набор боевому узлу — отказ, теми же словами.
        assert!(map_verdict(&over, MapShortfall::Refuse).is_err());

        // А вот вдесятеро больший набор мягким уже не бывает.
        let way_over = map_budget(140_000, 20, Some(384), 65_530);
        assert!(way_over.peaks_over());
        assert!(
            map_verdict(&way_over, MapShortfall::Warn).is_err(),
            "выше жёсткого предела мягкости нет: mmap откажет на этом же открытии"
        );
    }

    /// Оценка по полу отказывает, но не предупреждает.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Пол — две области на сегмент. Упакованному сегменту столько и нужно, а
    /// рассыпанному, собранному до контейнера, замерено семнадцать-тридцать —
    /// смотря сколько у схемы полей. Какой вид сегментов в составе, до открытия
    /// первого каталога неизвестно, и набор бывает смешанным; предупреждение по
    /// полу назвало бы такому набору предел на порядок меньше нужного —
    /// оператор поднял бы `vm.max_map_count` до посоветованного, перезапустил
    /// узел и получил бы ровно ту же нехватку. Отказать по полу, наоборот, можно
    /// и нужно: если не лезет даже он, точная потребность тем более не влезет, и
    /// узнать это надо до открытия десятков тысяч каталогов.
    #[test]
    fn the_floor_estimate_refuses_but_never_warns() {
        let floor = map_budget_floor(40_000, Some(300), 65_530);
        assert!(!floor.fits(), "80 300 областей объявлены влезающими в 58 977");
        assert!(
            map_verdict(&floor, MapShortfall::Refuse).is_err(),
            "заведомо верный отказ не случился"
        );
        // Предупреждения по полу нет ни при каком правиле — но здесь набор
        // ещё и выше жёсткого предела, поэтому мягкое правило отказывает, а не
        // молчит. Предупреждения с заниженным числом не выдано и тут.
        assert!(
            map_verdict(&floor, MapShortfall::Warn).is_err(),
            "80 300 областей при пределе 65 530 — отказ, а не заметка"
        );
        // А там, где пол не перевалил за предел, мягкое правило именно молчит:
        // советовать по заниженной оценке хуже, чем не советовать.
        let quiet = map_budget_floor(30_000, Some(300), 65_530);
        assert!(!quiet.fits() && !quiet.peaks_over());
        assert_eq!(
            map_verdict(&quiet, MapShortfall::Warn).expect("пол отказал в своей полосе"),
            None,
            "по полу выдано предупреждение с заниженным числом"
        );

        // А точный замер того же набора называет число побольше — и отказывает
        // при любом правиле, потому что 880 300 областей выше самогó предела.
        let exact = map_budget(40_000, 22, Some(300), 65_530);
        let told = map_verdict(&exact, MapShortfall::Warn)
            .expect_err("точный замер вдесятеро выше предела обязан отказать")
            .to_string();
        assert!(told.contains("880300"), "точная потребность не названа: {told}");
        assert!(exact.advised_limit() > floor.advised_limit());
    }

    /// Повторное открытие не отказывает узлу, который уже поднялся.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// [`Segments::load`] зовётся не только на старте: сброс посреди переигрывания
    /// журнала перечитывает состав, и прежний набор в этот миг ещё жив — его
    /// сменят, когда новый соберётся. Все его области стоят в `/proc/self/maps`, и
    /// база, взятая оттуда голым числом, посчитала бы тот же индекс второй раз.
    ///
    /// Проверяется не арифметика (она своя, в `mmap`), а её последствие: тот же
    /// набор, что открылся при строгом правиле, обязан открыться и во второй раз.
    /// Иначе узел, поднявшийся минуту назад, умирает на первом же сбросе — а
    /// переигрывание журнала на отказ состава отвечает выходом из процесса.
    #[test]
    fn a_second_open_in_the_same_process_is_not_refused() {
        // 2000 сегментов по 20 областей: 40 000 своих плюс 400 вне индекса при
        // рабочем потолке 58 977. Помещается — но ровно до удвоения.
        const SEGMENTS: usize = 2000;
        const PER_SEG: usize = 20;
        const OUTSIDE: usize = 400;
        let ours = SEGMENTS * PER_SEG;

        let start = map_budget(
            SEGMENTS,
            PER_SEG,
            crate::mmap::maps_outside_index(Some(OUTSIDE), 0),
            65_530,
        );
        assert_eq!(
            map_verdict(&start, MapShortfall::Refuse).expect("узел не поднялся"),
            None,
            "узлу отказано уже на старте — стенд не о том"
        );

        // Тот же индекс, тот же процесс, набор ещё жив.
        let again = map_budget(
            SEGMENTS,
            PER_SEG,
            crate::mmap::maps_outside_index(Some(OUTSIDE + ours), ours),
            65_530,
        );
        assert_eq!(
            map_verdict(&again, MapShortfall::Refuse)
                .unwrap_or_else(|e| panic!("отказ работающему узлу на повторном открытии: {e}")),
            None,
            "по повторному открытию сказано лишнее — свои области посчитаны дважды"
        );
    }

    /// Число областей на сегмент замеряется один раз и потом не переспрашивается.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Замер — разность живого счётчика вокруг открытия сегмента, и она верна
    /// лишь тогда, когда рядом никто отображений не заводит. На старте это так, а
    /// при повторном `load` (например посреди переигрывания журнала) рядом уже
    /// работает пересмотр каталога, и чужая область попала бы в наш `per_seg`.
    /// У упакованного сегмента их всего две, и одна чужая — это уже расход в
    /// полтора раза против настоящего; потребность же считается умножением на
    /// число сегментов, так что на четырёх тысячах промах в одну область — это
    /// четыре тысячи областей и ложный отказ.
    #[test]
    fn the_maps_per_segment_measurement_is_taken_once() {
        let cell = std::sync::atomic::AtomicUsize::new(0);
        assert_eq!(remember_per_segment(&cell, 22), 22, "первый замер не принят");
        assert_eq!(
            remember_per_segment(&cell, 900),
            22,
            "второй замер перебил первый — чужие области попали в расчёт"
        );

        // Нулевой замер не запоминается: он означает «мерить было нечего», а
        // запомненный ноль объявил бы, что сегменты областей не занимают.
        let empty = std::sync::atomic::AtomicUsize::new(0);
        assert_eq!(remember_per_segment(&empty, 0), crate::mmap::MIN_MAPS_PER_SEGMENT);
        assert_eq!(empty.load(std::sync::atomic::Ordering::Relaxed), 0, "ноль запомнен");
        assert_eq!(remember_per_segment(&empty, 22), 22, "после нуля замер не принялся");

        // Ниже пола замер не опускается: сегмент держит хотя бы контейнер и
        // карту живых, и заниженный расход занизил бы потребность на всём наборе.
        let low = std::sync::atomic::AtomicUsize::new(0);
        assert_eq!(remember_per_segment(&low, 1), crate::mmap::MIN_MAPS_PER_SEGMENT);
    }

    /// Обычный индекс открывается и при самом строгом правиле.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Это отрицательный случай, и он важнее положительного: проверка потолка
    /// стоит на пути открытия **любого** набора, и ошибка в ней ломает не редкий
    /// узел на четырёх тысячах сегментов, а все узлы разом. Сравнение не в ту
    /// сторону, потерянное насыщение, базовый расход, посчитанный дважды (после
    /// открытия первого сегмента его области уже стоят в `/proc/self/maps`), —
    /// любое из этого превращает два сегмента в отказ на пустом месте.
    ///
    /// На ядре, которое предела не сообщает, проверка не проводится вовсе, и
    /// тогда тест сторожит второе: что путь замера не роняет открытие.
    #[test]
    fn an_ordinary_index_opens_under_the_strictest_rule() {
        let d = std::env::temp_dir().join("svindb-map-gate-open");
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("каталог");
        let d = d.to_string_lossy().to_string();
        for part in 0..2 {
            let mut ix = crate::indexer::Indexer::new(
                &d,
                vec!["title".into(), "body".into()],
                vec![2.0, 1.0],
                100,
            );
            for i in 0..10 {
                ix.add(&format!("{part}-{i}"), &[&format!("з{i}"), "тело"]);
            }
            ix.flush();
        }

        let was = crate::mmap::map_shortfall();
        crate::mmap::set_map_shortfall(crate::mmap::MapShortfall::Refuse);
        let opened = Segments::load(&d);
        crate::mmap::set_map_shortfall(was);
        let segs = opened.expect("два сегмента объявлены не влезающими в предел");
        assert_eq!(segs.list.len(), 2);
        assert_eq!(segs.docs(), 20);

        // Там, где предел известен, замер обязан был случиться и быть не ниже
        // пола: нулевой замер означал бы, что потребность считается по воздуху.
        if crate::mmap::max_maps().is_some() {
            let per = MAPS_PER_SEGMENT.load(std::sync::atomic::Ordering::Relaxed);
            assert!(
                per >= crate::mmap::MIN_MAPS_PER_SEGMENT,
                "число областей на сегмент не замерено: {per}"
            );
        }
        let _ = std::fs::remove_dir_all(&d);
    }
}
