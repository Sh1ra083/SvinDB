//! Поиск верхушки: BM25F по нескольким полям с отсечением по блокам.

use crate::index::{Cursor, Index, Union};
use crate::stats;
use std::sync::atomic::{AtomicU64, Ordering};

/// Потолок числа ключей сортировки.
pub const MAX_SORT: usize = 4;

/// Ключи сортировки документа, уже приведённые к «больше значит лучше».
pub type SortKey = i128;
pub type Keys = [SortKey; MAX_SORT];

pub struct Hit {
    pub doc: u32,
    pub score: f64,
    /// Ключи сортировки. При сортировке по весу не используются.
    pub keys: Keys,
    /// Сколько ключей занято.
    pub nkeys: u8,
}

/// Приводит значение к виду, в котором **больше всегда значит лучше**.
#[inline(always)]
pub fn norm_key(v: i64, desc: bool) -> SortKey {
    if desc {
        v as SortKey
    } else {
        (!v) as SortKey
    }
}

/// Обратное к [`norm_key`] — для отдачи наружу.
#[inline(always)]
pub fn denorm_key(k: SortKey, desc: bool) -> Option<i64> {
    let value = i64::try_from(k).ok()?;
    Some(if desc { value } else { !value })
}

/// Ключ документа, у которого значения нет.
///
/// Отдельное от всей шкалы Int64 значение: MIN/MAX остаются настоящими
/// числами и никогда не становятся missing. Формат колонок не расширяется:
/// дополнительный бит нужен только ключу collector и внутреннему курсору.
pub const MISSING_KEY: SortKey = i128::MIN;

/// Ветвь дерева условий.
///
/// Вложенность нужна не для полноты, а потому что без неё нельзя выразить самое
/// обычное: «москва И (квартира ИЛИ дом) И НЕ аренда». Плоские списки такое
/// разложить не могут — «или» внутри «и» им негде поставить.
#[derive(Default)]
pub struct BoolReq {
    pub must: Vec<Clause>,
    pub should: Vec<Clause>,
    pub must_not: Vec<Clause>,
    /// Сколько необязательных обязано совпасть.
    ///
    /// Ноль означает обычное правило: если обязательных нет, хотя бы одно
    /// необязательное. Отдельное число нужно там, где «хотя бы одно» слишком
    /// мягко, а «все» слишком жёстко — «из пяти признаков товара совпали три».
    pub min_should: usize,
}

/// Лист дерева или вложенная ветвь.
pub enum Clause {
    Word(QueryWord),
    Phrase(QueryPhrase),
    Bool(BoolReq),
    /// Условия, обязанные выполниться на **одном и том же** элементе массива.
    Slots(SlotReq),
}

/// Условие по элементу массива.
pub struct SlotReq {
    /// Номер группы — пути массива.
    pub group: u32,
    /// Положительные условия: пересечение их слотов и есть ответ.
    ///
    /// Хотя бы одно обязательно. Отрицание над пустым множеством означало бы
    /// «все слоты, кроме», а «все слоты» требует знать длину массива в узле —
    /// то есть завести инвариант, который однажды разъедется молча. Пересечение
    /// же всегда начинается с конечного множества, и `none` его только режет.
    pub all: Vec<SlotTerm>,
    pub none: Vec<SlotTerm>,
    /// Сколько элементов обязано совпасть. Ноль читается как единица.
    pub min_count: u32,
    /// Вклад в вес документа. Ноль — узел работает отбором.
    pub boost: f64,
}

/// Условие на один элемент.
pub enum SlotTerm {
    /// Слово в поле-массиве: слоты — его позиции в документе.
    Word(QueryWord),
    /// Диапазон по колонке поля-массива. Границы включительно.
    Range { field: usize, from: i64, to: i64 },
    /// Слоты, где у поля вообще есть значение.
    Exists { field: usize },
}

/// Фраза: слова по порядку, `slop` разрешает лишние между ними.
///
/// Прочтений у слова фразы нет: маска внутри фразы не поддержана намеренно —
/// раскрытие маски даёт альтернативы, а фраза требует порядка, и что значит
/// «порядок альтернатив», решать пришлось бы гаданием.
pub struct QueryPhrase {
    pub field: usize,
    /// Множитель веса фразы. Разбирался и терялся: фраза с `boost` 1 и 100
    /// весила одинаково.
    pub boost: f64,
    /// По слову на позицию фразы. Повтор слова — просто повторённый номер.
    pub words: Vec<QueryWord>,
    pub slop: u32,
}

/// Слово запроса: прочтения (маска, опечатка, леммы) — альтернативы, поэтому
/// документ, содержащий любое из них, совпал со словом **один раз**.
pub struct QueryWord {
    pub field: usize,
    pub lists: Vec<u32>,
    /// Вес слова, посчитанный по **всему** индексу. `None` — считать по сегменту.
    ///
    /// Обратная частота обязана быть общей, иначе вес перестаёт быть функцией
    /// документа и запроса и становится функцией того, как приём нарезал корпус.
    /// Верхушки сегментов складываются в один список и сортируются по сырому
    /// весу — значит числа из разных сегментов сравниваются между собой, а
    /// посчитаны они по разным `N` и разным `df`.
    ///
    /// `None` остаётся для одиночного сегмента и для тех, кто зовёт обход
    /// напрямую: там местная статистика и есть общая.
    pub weight: Option<f64>,
}

impl QueryWord {
    pub fn new(field: usize, lists: Vec<u32>) -> QueryWord {
        QueryWord { field, lists, weight: None }
    }

    /// Вес слова: общий, если он посчитан, иначе местный.
    ///
    /// `df` передаётся вызывающим, потому что считается он по-разному: обычный
    /// обход суммирует по всем формам слова, фраза берёт одну.
    #[inline]
    pub fn weight(&self, ix: &Index, df: u32) -> f64 {
        self.weight.unwrap_or_else(|| ix.weight(self.field, df))
    }
}

/// Сколько документов запросу позволено прочесть ради порядка слов.
///
/// ## Зачем он вообще нужен
///
/// Поэтому счётчик, а не оценка заранее. Оценка сверху — `df` самого редкого
/// слова — слишком груба: у двух слов по пятьдесят тысяч документов она даёт
/// пятьдесят тысяч, а пересечение их — тысяча с небольшим, и отказ был бы
/// отказом там, где работы на десять миллисекунд.
///
/// Исчерпание не притворяется отсутствием совпадений: [`Budget::capped`]
/// поднимается, и ответ говорит, что он неполон. Молчаливая неполнота хуже
/// отказа — по ней принимают решения.
#[derive(Debug)]
pub struct Budget {
    left: std::sync::atomic::AtomicI64,
    capped: std::sync::atomic::AtomicBool,
}

impl Budget {
    pub fn new(n: u64) -> Budget {
        Budget {
            left: std::sync::atomic::AtomicI64::new(n as i64),
            capped: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Берёт одну проверку. `false` — брать больше нечего.
    ///
    /// Общий на весь запрос, включая параллельный обход сегментов: бюджет
    /// принадлежит запросу, а не сегменту, иначе пятьдесят шардов потратили бы
    /// пятьдесят бюджетов.
    #[inline]
    pub fn take(&self) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        if self.left.fetch_sub(1, Relaxed) > 0 {
            return true;
        }
        self.capped.store(true, Relaxed);
        false
    }

    /// Кончился ли бюджет хоть раз за запрос.
    pub fn capped(&self) -> bool {
        self.capped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Порог первого ключа сортировки — то, чем сегменты отсекают друг друга.
///
/// ## Почему это перечисление, а не число
///
/// Порог передаётся между сегментами: сегмент, который заведомо не даст лучшего,
/// чем уже найденный `k`-й, не открывается вовсе. Пока обход шёл по одному
/// сегменту за раз, число годилось — оно просто переписывалось перед каждым
/// следующим.
///
/// Раздать сегменты потокам с числом внутри общего запроса нельзя: запрос
/// делится между потоками по ссылке, а порог обязан расти. Отсюда третий вид —
/// общая ячейка. Читается она так же, как число, и стоит того же: обычная
/// нагрузка без упорядочения.
///
/// ## Почему общий порог остаётся верным
///
/// Между сегментами передаётся **необходимое условие**, а не точный порог: у
/// каждого потока свой `k`-й, и он не хуже общего `k`-го, потому что общий
/// набор шире. Значит наибольший из потоковых порогов — всё ещё верная нижняя
/// граница, и `fetch_max` её и накапливает. Ровно тот же довод, по которому
/// потоки делят порог по весу.
#[derive(Default, Debug)]
pub enum KeyFloor {
    /// Порога нет: верхушка ещё не набралась либо сортировки нет вовсе.
    #[default]
    None,
    /// Порог, переданный по цепочке сегментов, — обход последовательный.
    Fixed(SortKey),
    /// Общий порог: сегменты идут в несколько потоков.
    Shared(std::sync::Arc<std::sync::atomic::AtomicI64>),
}

impl KeyFloor {
    /// Нынешнее значение. `None` — отсекать пока нечем.
    ///
    /// `i64::MIN` в общей ячейке означает «порога ещё нет». Это не отнимает
    /// значений: для MIN/missing общий bound остаётся консервативно выключен.
    /// Сам collector различает их по расширенному SortKey.
    #[inline(always)]
    pub fn get(&self) -> Option<SortKey> {
        match self {
            KeyFloor::None => None,
            KeyFloor::Fixed(v) => Some(*v),
            KeyFloor::Shared(a) => {
                let v = a.load(Ordering::Relaxed);
                (v != i64::MIN).then_some(v as SortKey)
            }
        }
    }
}

#[cfg(test)]
mod key_floor_tests {
    use super::*;

    #[test]
    fn missing_never_aliases_any_int64_sort_value() {
        for desc in [false, true] {
            for value in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
                let key = norm_key(value, desc);
                assert!(key > MISSING_KEY, "missing совпал с {value}, desc={desc}");
                assert_eq!(denorm_key(key, desc), Some(value));
            }
            assert_eq!(denorm_key(MISSING_KEY, desc), None);
        }
    }

    /// Три вида порога читаются одинаково, и «порога нет» отличимо от значения.
    ///
    /// `i64::MIN` выключает только shared pruning. Collector при этом
    /// различает минимальное число и missing; фиксированный порог вправе
    /// передавать оба значения без потери информации.
    #[test]
    fn the_three_kinds_of_floor_read_the_same_way() {
        assert_eq!(KeyFloor::None.get(), None);
        assert_eq!(KeyFloor::Fixed(7).get(), Some(7));
        assert_eq!(KeyFloor::Fixed(MISSING_KEY).get(), Some(MISSING_KEY), "число есть число");

        let cell = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN));
        let f = KeyFloor::Shared(std::sync::Arc::clone(&cell));
        assert_eq!(f.get(), None, "пустая ячейка обязана читаться как «порога нет»");
        cell.fetch_max(-100, Ordering::Relaxed);
        assert_eq!(f.get(), Some(-100), "отрицательный порог — обычное дело при возрастании");
        cell.fetch_max(-500, Ordering::Relaxed);
        assert_eq!(f.get(), Some(-100), "порог обязан только расти");
        cell.fetch_max(42, Ordering::Relaxed);
        assert_eq!(f.get(), Some(42));
        // И порог, равный наименьшему ключу, никого не отсекает: `passes`
        // отвергает по строгому «меньше», а меньше наименьшего ключа не бывает.
        // Проверяется самим `passes`, а не сравнением на месте: сравнение
        // компилятор считает сам и от подмены правила в `passes` не стережёт.
        let req = Request {
            mode: MatchMode::Boolean,
            skipped: false,
            live: None,
            words: Vec::new(),
            k: 1,
            filter: Vec::new(),
            must: Vec::new(),
            must_not: Vec::new(),
            tree: BoolReq::default(),
            sort: vec![(0, true)],
            floor_key: KeyFloor::Fixed(MISSING_KEY),
            after: None,
            doc_base: 0,
            budget: None,
            guard: None,
            score: Vec::new(),
        };
        let keys: Keys = [MISSING_KEY; MAX_SORT];
        assert!(
            passes(&req, 0.0, &keys, 1, 0, f64::NEG_INFINITY),
            "порог из наименьшего ключа отсёк документ без значения"
        );
    }
}

/// Способ сопоставления выбирается до разрешения словаря. Пустой AnyTerm
/// означает отсутствие совпадений, пустое Boolean-дерево — MatchAll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchMode {
    AnyTerm,
    Boolean,
}

pub struct Request {
    pub mode: MatchMode,
    /// Планировщик доказал по неизменяемому каталогу терминов, что сегмент не
    /// содержит ни одной буквальной формы запроса. Обход обязан вернуть пусто,
    /// не открывая списки и колонки.
    pub skipped: bool,
    /// Замороженная видимость PIT для этого сегмента. Без PIT поиск читает
    /// живую карту сегмента, как прежде.
    pub live: Option<std::sync::Arc<crate::live::Snapshot>>,
    pub words: Vec<QueryWord>,
    pub k: usize,
    /// Отбор по значению поля. Пусто — без отбора.
    pub filter: Vec<crate::segments::Range>,
    /// Обязательные слова: документ без любого из них не совпал вовсе.
    pub must: Vec<QueryWord>,
    /// Запрещённые слова: документ с любым из них выбывает.
    pub must_not: Vec<QueryWord>,
    /// Дерево условий. Плоские `words`/`must`/`must_not` — его частный случай.
    pub tree: BoolReq,
    /// Поля сортировки и направления. Пусто — по весу.
    pub sort: Vec<(usize, bool)>,
    /// Не точный порог, а **верное** необходимое условие: документ, чей первый
    /// ключ строго хуже, не победит никогда, каковы бы ни были остальные.
    /// Точный порог потребовал бы таскать между сегментами весь k-й документ.
    pub floor_key: KeyFloor,
    /// Ключи последнего показанного документа и его номер.
    pub after: Option<(Vec<SortKey>, u64)>,
    /// Смещение номеров этого сегмента в общей нумерации.
    pub doc_base: u64,
    /// Общий на весь запрос счётчик проверок по тексту. `None` — без предела.
    pub budget: Option<std::sync::Arc<Budget>>,
    /// Общий deadline/cancellation guard всех стадий запроса.
    pub guard: Option<std::sync::Arc<crate::work::Guard>>,
    /// Как считать вклад по общей статистике состава — по локальному номеру
    /// поля. Пусто — считать местными таблицами сегмента, как считалось всегда.
    ///
    /// Лежит в запросе, а не в сегменте, потому что зависит от **состава**:
    /// один и тот же сегмент в разных наборах даёт разную общую среднюю.
    pub score: Vec<Option<crate::index::FieldScore>>,
}

impl Request {
    #[inline(always)]
    fn is_live(&self, ix: &Index, doc: u32) -> bool {
        self.live.as_ref().map_or_else(|| ix.is_live(doc), |live| live.is_live(doc))
    }
    /// Таблицы для поля, если запрос считает по общей статистике.
    #[inline(always)]
    pub fn score_at(&self, fld: usize) -> Option<&crate::index::FieldScore> {
        self.score.get(fld).and_then(|x| x.as_ref())
    }

    #[inline]
    fn keep_going(&self, tick: &mut crate::work::ProgressTicker<'_>) -> bool {
        tick.go()
    }

    fn ticker(&self) -> crate::work::ProgressTicker<'_> {
        crate::work::ProgressTicker::new(self.guard.as_deref())
    }

    #[inline]
    fn running(&self) -> bool {
        self.guard.as_ref().is_none_or(|guard| guard.check())
    }
}

/// Порог, за которым поддержание порядка вставкой выгоднее пересортировки.
const MANY: usize = 16;

/// Запас, на который увеличивается потолок при решении о пропуске.
///
/// Точность `f32` около 1.2·10⁻⁷; миллионная доля перекрывает её на порядок и
/// при этом не даёт заметной лишней работы. Разбор — в заголовке модуля.
const SLACK: f64 = 1.0 + 1e-6;

/// Пропуск блока имеет смысл только когда он перешагивает **остаток блока**.
///
/// У обычного слова так всегда: границу задаёт конец блока, и прыжок сразу на
/// сотню документов. У маски всё иначе — полсотни форм разбросаны, ближайшая
/// почти всегда ближе конца блока, и граница упирается в неё. Прыжок туда ведёт
/// в ту же точку, куда привёл бы обычный шаг, только дороже: щупает границы
/// блоков и лишний раз спрашивает кучу.
///
/// Ищет верхушку в одном сегменте.
///
/// Возвращает верхушку по убыванию скора и число просмотренных документов.
pub fn search(ix: &Index, req: &Request, floor: f64) -> (Vec<Hit>, u64) {
    search_with(ix, req, floor, 1)
}

/// То же, но обход одного сегмента делится между потоками.
///
/// ## Что именно делится
///
/// Блоки списка. Они уже упорядочены по убыванию потолка, то есть по
/// полезности, и это готовая очередь работ: поток берёт следующий блок, смотрит,
/// дотягивает ли его потолок до порога, и либо обрабатывает, либо объявляет, что
/// дальше смотреть нечего — за ним потолки только ниже.
///
/// Очередь разбирается **порциями**: доставать по одному значит сериализовать
/// саму кучу, а порция в несколько блоков на поток эту цену размазывает.
///
/// ## Почему это не то же, что делить сегмент пополам
pub fn search_with(ix: &Index, req: &Request, floor: f64, threads: usize) -> (Vec<Hit>, u64) {
    if req.k == 0 || req.skipped {
        return (Vec::new(), 0);
    }
    if req.mode == MatchMode::Boolean {
        return search_boolean(ix, req, floor);
    }
    // Пригодные слова: те, у которых нашлась хоть одна непустая форма.
    let mut words: Vec<(usize, Vec<u32>, f64)> = Vec::with_capacity(req.words.len());
    let mut work_tick = req.ticker();
    for wq in req.words.iter() {
        if !req.keep_going(&mut work_tick) {
            return (Vec::new(), 0);
        }
        let df: u32 = wq.lists.iter().map(|&l| ix.t_df(l as usize)).sum();
        if df == 0 {
            continue;
        }
        let lists: Vec<u32> =
            wq.lists.iter().copied().filter(|&l| ix.t_df(l as usize) > 0).collect();
        if !lists.is_empty() {
            words.push((wq.field, lists, wq.weight(ix, df)));
        }
    }
    if words.is_empty() {
        return (Vec::new(), 0);
    }

    // Одно слово — самый частый запрос, и весь механизм многословного обхода
    // ему не нужен. Разбор — у `single`.
    if words.len() == 1 {
        let (field, lists, w) = words.pop().expect("одно слово");
        if threads > 1 {
            return single_parallel(ix, req, floor, field, &lists, w, threads);
        }
        return single(ix, req, floor, field, &lists, w);
    }

    // Один курсор на **слово** запроса, а не на список: раскрытые формы это
    // альтернативы одного слова, и вклад берётся один раз. Разбор — у `Union`.
    let mut cur: Vec<Union> = Vec::new();
    for (field, lists, w) in words {
        let sc = req.score_at(field).cloned();
        let subs: Vec<Cursor> =
            lists.iter().map(|&l| ix.cursor_in(l, field, w, sc.as_ref())).collect();
        cur.push(Union::new(subs));
    }

    let mut heap: Vec<Hit> = Vec::with_capacity(req.k + 1);
    let mut visited = 0u64;
    let mut order: Vec<usize> = (0..cur.len()).collect();
    order.sort_by_key(|&i| cur[i].cur());
    // Сумма потолков всех слов. Из неё вычитается накопленная до опорного, и
    // остаётся потолок хвоста — он нужен, чтобы пропускать блоки целиком.
    let total_bound: f64 = cur.iter().map(|c| c.list_bound()).sum();

    let mut moved = false;
    loop {
        if !req.keep_going(&mut work_tick) {
            break;
        }
        if moved {
            reorder(&mut order, &cur);
            moved = false;
        }
        if cur[order[0]].cur() == u32::MAX {
            break;
        }
        let theta = if heap.len() < req.k { floor } else { heap[0].score.max(floor) };

        // Опорное слово: первое, до которого накопленных потолков хватает,
        // чтобы побить порог.
        let mut acc = 0.0;
        let mut pivot = usize::MAX;
        for (p, &i) in order.iter().enumerate() {
            acc += cur[i].list_bound();
            if acc > theta {
                pivot = p;
                break;
            }
        }
        if pivot == usize::MAX {
            // Даже все слова вместе не побьют порог — дальше смотреть нечего.
            break;
        }
        let doc = cur[order[pivot]].cur();

        if cur[order[0]].cur() != doc {
            // Курсоры до опорного отстали: подтянуть их к опорному документу.
            // Индекс адресует не один массив, а несколько параллельных, и
            // сам участвует в вычислении. `zip` здесь потребовал бы
            // одновременных заимствований, которых у этого кода нет.
            #[allow(clippy::needless_range_loop)]
            for p in 0..pivot {
                let i = order[p];
                cur[i].seek(doc);
                moved = true;
            }
            continue;
        }

        // Теперь блоки курсоров содержат `doc`, и блочные потолки применимы.
        //
        // Считаются два потолка сразу. `bs` — для самого `doc`: сумма блочных
        // потолков всех слов, стоящих на нём. `ess` — то же, но только по
        // существенным словам (до опорного включительно); вместе с потолками
        // хвоста он даёт границу для **всего остатка блока**, а не одного
        // документа.
        let mut bs = 0.0;
        let mut ess = 0.0;
        for (p, &i) in order.iter().enumerate() {
            if cur[i].cur() == doc {
                let b = cur[i].block_bound();
                bs += b;
                if p <= pivot {
                    ess += b;
                }
            }
        }
        let alive = alive(ix, req, doc);
        if bs * SLACK < theta || !alive {
            // Пропуск блока целиком.
            //
            // Документ, побивающий порог, обязан встретиться хотя бы в одном
            // существенном слове — иначе он не набирает и суммы их потолков.
            // Пока такой документ не вышел за конец текущих блоков, его вклад
            // от существенных слов не больше `ess`, а от остальных — не больше
            // их потолков списка. Значит на всём этом отрезке порог не побить
            // никому, и его можно перешагнуть одним прыжком вместо ста
            // двадцати восьми шагов.
            //
            // Проверка `alive` тут ни при чём: она про один документ, а не про
            // отрезок, поэтому прыжок делается только по потолку.
            if bs * SLACK < theta && (ess + (total_bound - acc)) * SLACK < theta {
                // Прыжок делается, только если он имеет смысл у **каждого**
                // существенного слова: одного отказа хватает, чтобы отрезок не
                // был общим.
                let mut limit = Some(u32::MAX);
                for p in 0..=pivot {
                    limit = match (limit, cur[order[p]].skip_limit()) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        _ => None,
                    };
                }
                if let Some(limit) = limit.filter(|&l| l != u32::MAX && l >= doc) {
                    stats::block_skips(1);
                    for p in 0..=pivot {
                        cur[order[p]].seek(limit + 1);
                        moved = true;
                    }
                    continue;
                }
            }
            for &i in order.iter() {
                if cur[i].cur() == doc {
                    cur[i].next();
                    moved = true;
                }
            }
            continue;
        }

        let mut score = 0.0;
        for &i in order.iter() {
            if cur[i].cur() == doc {
                score += cur[i].score();
            }
        }
        visited += 1;
        stats::scored(1);
        if score <= floor && heap.len() >= req.k {
            // Курсоры сдвинулись — значит порядок обязан быть пересобран.
            //
            // С устаревшим порядком это перестаёт быть верным. Существенное
            // слово может оказаться позади опорного, и тогда подтягивание
            // несущественных курсоров перепрыгивает документ, который в этом
            // существенном слове есть. Документ потом оценивается — но уже без
            // вклада перепрыгнутых слов, то есть весом ниже настоящего, и
            // выпадает из верхушки.
            //
            // На случайных корпусах это давало 102 расхождения с полным
            // перебором на 480 многословных запросов, и терялся при этом
            // **первый** документ выдачи. Проверка — в
            // `random_multiword_matches_exhaustive`.
            for &i in order.iter() {
                if cur[i].cur() == doc {
                    cur[i].next();
                    moved = true;
                }
            }
            continue;
        }
        offer(&mut heap, req.k, doc, score, false);
        for &i in order.iter() {
            if cur[i].cur() == doc {
                cur[i].next();
                moved = true;
            }
        }
    }

    heap.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.doc.cmp(&b.doc)));
    (heap, visited)
}

/// Обход для запроса из одного слова.
///
/// ## Почему блоки идут по убыванию потолка, а не по порядку
///
/// Скор равен `вес · ν(длина)`, то есть определяется только документом.
/// Значит лучшие документы списка — те, что лежат в блоках с наибольшим
/// потолком, и идти к ним надо сразу, а не через все остальные.
///
/// По убыванию потолка порог поднимается сразу, а первый же блок, не дотянувший
/// до него, закрывает весь остаток списка: дальше потолки только ниже.
///
/// Выдача от порядка обхода не зависит — это обеспечено правилом вставки, где
/// при равных скорах побеждает меньший номер документа.
///
/// ## Почему списки идут по очереди, а не объединением
///
/// Все формы одного слова получают **один и тот же вес**: он считается по
/// сумме их частот и раздаётся всем. А вклад документа равен `вес · ν(длина)`.
/// Значит скор **не зависит от того, какая форма совпала** — он одинаков для
/// всех, и максимум, который объединение честно вычисляет обходом кучи, известен
/// заранее.
///
/// Отсюда: сливать формы незачем. Достаточно пройти их списки по очереди,
/// складывая в общую верхушку с общим порогом. Шаг становится «сдвинуть курсор,
/// взять норму, сравнить с порогом» вместо просеивания кучи и разбора, кто на
/// каком документе стоит.
///
/// Выигрывает от этого и отсечение. У объединения потолок блока — максимум по
/// формам, стоящим на документе, то есть самый слабый из возможных; у отдельного
/// списка потолок свой, и блок пропускается целиком. Порог к этому моменту уже
/// набран предыдущими формами.
///
/// ## Чего это стоит
///
/// Документ, встретившийся в нескольких формах, будет оценён несколько раз —
/// поэтому `просмотрено` у маски растёт. Работы это почти не добавляет (оценка
/// это обращение в таблицу и умножение), а в верхушку такой документ попадёт
/// один раз: `offer` проверяет, нет ли его там уже.
fn single(
    ix: &Index,
    req: &Request,
    floor: f64,
    field: usize,
    lists: &[u32],
    w: f64,
) -> (Vec<Hit>, u64) {
    let mut heap: Vec<Hit> = Vec::with_capacity(req.k + 1);
    let mut visited = 0u64;
    let dedup = lists.len() > 1;
    // Подъём потолка — тот же, что у курсора ([`crate::index::cursor`]), и он
    // здесь обязателен, а не желателен.
    //
    // Потолок блока записан при сборке сегмента и посчитан по **его**
    // средней длине поля. Скор же считается по общим таблицам всего состава
    // (`sat_in` + `Segments::set_lengths`), то есть по другой средней. Если
    // общая средняя больше собственной, настоящий вклад документа выше
    // записанного потолка, и множитель `lift` ровно эту разницу и покрывает.
    //
    // Без него сравнение `bound * SLACK < theta` срабатывает раньше времени, и
    // `break` закрывает остаток списка вместе с документами, которым место в
    // верхушке. Пропажа молчаливая: ответ приходит успешным, просто короче
    // правды, и тем чаще, чем сильнее сегменты различаются длинами.
    let lift = req.score_at(field).map_or(1.0, |s| s.lift);
    let mut offsets: Vec<usize> = Vec::new();
    let mut order: Vec<(u32, u32)> = Vec::new();
    let mut buf = [0u32; crate::bits::BLOCK];
    let mut tfs = [0u32; crate::bits::BLOCK];
    let mut work_tick = req.ticker();

    'lists: for &list in lists.iter() {
        if !req.keep_going(&mut work_tick) {
            break;
        }
        let t = ix.term(list as usize, field);
        let nblocks = t.blocks;
        if nblocks == 0 {
            continue;
        }
        ix.block_offsets(&t, &mut offsets);

        order.clear();
        order.reserve(nblocks);
        for b in 0..nblocks {
            order.push((ix.block_max_bits(&t, b), b as u32));
        }
        // Куча строится за `n`, а разбирается ровно на столько блоков, сколько
        // придётся посмотреть, — обычно единицы из десятков тысяч.
        let mut heap_blocks = std::collections::BinaryHeap::from(std::mem::take(&mut order));

        while let Some((bits, blk)) = heap_blocks.pop() {
            if !req.keep_going(&mut work_tick) {
                break 'lists;
            }
            let theta = if heap.len() < req.k { floor } else { heap[0].score.max(floor) };
            let bound = w * lift * f32::from_bits(bits) as f64;
            // Блоки идут по убыванию потолка, поэтому первый же не дотянувший
            // до порога закрывает и весь остаток списка.
            if bound * SLACK < theta {
                break;
            }
            stats::word_next(1);
            let blk = blk as usize;
            let n = ix.read_block_tf(&t, blk, offsets[blk], &mut buf, Some(&mut tfs));
            for (i, &doc) in buf.iter().take(n).enumerate() {
                if !req.keep_going(&mut work_tick) {
                    break 'lists;
                }
                let alive = alive(ix, req, doc);
                if !alive {
                    continue;
                }
                let score = w * ix.sat_in(field, doc, tfs[i], req.score_at(field));
                visited += 1;
                stats::scored(1);
                offer(&mut heap, req.k, doc, score, dedup);
            }
        }
        order = heap_blocks.into_vec();
    }

    heap.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.doc.cmp(&b.doc)));
    (heap, visited)
}

/// Предлагает документ в верхушку.
///
/// Порядок внутри кучи — от худшего к лучшему, а при равных скорах худшим
/// считается **больший** номер документа. Из-за этого выдача не зависит от того,
/// в каком порядке документы попались: при ничьей остаётся меньший номер, ровно
/// как и при обходе по возрастанию номеров. Без этого правила обход по спискам и
/// обход объединением расходились бы на ничьих, которых при близких длинах
/// документов много.
#[inline]
fn offer(heap: &mut Vec<Hit>, k: usize, doc: u32, score: f64, dedup: bool) {
    offer_key(heap, k, doc, score, [0; MAX_SORT], 0, dedup)
}

/// То же с ключом сортировки.
///
/// Оговорка: ранг из `i64` в `f64` теряет точность выше 2⁵³. Для дат, цен и
/// счётчиков это недостижимо; для значений у самого края разрядной сетки два
/// соседних могут сравняться, и тогда порядок решит номер документа.
#[inline]
fn offer_key(
    heap: &mut Vec<Hit>,
    k: usize,
    doc: u32,
    score: f64,
    keys: Keys,
    nkeys: u8,
    dedup: bool,
) {
    let cand = Hit { doc, score, keys, nkeys };
    // Повтор — только когда он вообще возможен: у слова с несколькими формами.
    //
    // Берётся лучшая. Не сумма: формы это **альтернативные прочтения** одного
    // слова запроса, а не разные слова, и документ совпал со словом один раз.
    if dedup {
        if let Some(i) = heap.iter().position(|h| h.doc == doc) {
            if worse(&heap[i], &cand) {
                heap[i] = cand;
                sift_down(heap, i);
            }
            return;
        }
    }
    let qualifies = heap.len() < k || worse(&heap[0], &cand);
    if !qualifies {
        return;
    }
    if heap.len() < k {
        heap.push(cand);
        let last = heap.len() - 1;
        sift_up(heap, last);
    } else {
        heap[0] = cand;
        sift_down(heap, 0);
    }
}

/// Хуже ли `a`, чем `b`.
///
/// Один сравниватель на все случаи: ключи уже приведены к «больше значит лучше»
/// (см. [`norm_key`]), поэтому направление сортировки здесь не участвует вовсе.
/// Без ключей сравнивается вес. При полном равенстве побеждает меньший номер —
/// то самое правило, на котором стоит независимость выдачи от порядка обхода.
#[inline(always)]
fn worse(a: &Hit, b: &Hit) -> bool {
    if a.nkeys == 0 {
        return a.score < b.score || (a.score == b.score && a.doc > b.doc);
    }
    for i in 0..a.nkeys as usize {
        if a.keys[i] != b.keys[i] {
            return a.keys[i] < b.keys[i];
        }
    }
    a.doc > b.doc
}

/// Куча худшим вверх: в корне тот, кого вытеснят первым.
#[inline]
fn sift_up(heap: &mut [Hit], mut i: usize) {
    while i > 0 {
        let p = (i - 1) / 2;
        if worse(&heap[i], &heap[p]) {
            heap.swap(i, p);
            i = p;
        } else {
            break;
        }
    }
}

#[inline]
fn sift_down(heap: &mut [Hit], mut i: usize) {
    loop {
        let (l, r) = (2 * i + 1, 2 * i + 2);
        let mut worst = i;
        if l < heap.len() && worse(&heap[l], &heap[worst]) {
            worst = l;
        }
        if r < heap.len() && worse(&heap[r], &heap[worst]) {
            worst = r;
        }
        if worst == i {
            return;
        }
        heap.swap(i, worst);
        i = worst;
    }
}

fn worst_first(a: &Hit, b: &Hit) -> std::cmp::Ordering {
    a.score.total_cmp(&b.score).then(b.doc.cmp(&a.doc))
}

/// Восстанавливает порядок курсоров по номеру документа.
#[inline]
fn reorder(order: &mut [usize], cur: &[Union]) {
    if order.len() <= MANY {
        order.sort_by_key(|&i| cur[i].cur());
        return;
    }
    for j in 1..order.len() {
        let x = order[j];
        let key = cur[x].cur();
        let mut k = j;
        while k > 0 && cur[order[k - 1]].cur() > key {
            order[k] = order[k - 1];
            k -= 1;
        }
        order[k] = x;
    }
}

/// Полный перебор без отсечения — эталон для сверки.
///
/// Медленный по построению: обходит все документы всех списков и считает скор
/// каждому. Нужен затем, что отсечение — это утверждение «этот документ не может
/// попасть в верхушку», и проверять такое утверждение можно только тем, кто
/// ничего не отсекает.
/// Булев перебор без единого отсечения: дерево спрашивается у **каждого**
/// документа сегмента. Эталон для [`search_boolean`], и только для тестов.
///
/// Здесь нет ни ведущего слова, ни блоков, ни порога — то есть нет ровно тех
/// трёх механизмов, ошибку в которых он и должен ловить. Эталон, повторяющий
/// устройство проверяемого, ничего не проверяет.
#[cfg(test)]
pub fn boolean_exhaustive(ix: &Index, req: &Request) -> Vec<Hit> {
    let Some(mut root) = bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref())
    else {
        return Vec::new();
    };
    let mut hits: Vec<Hit> = Vec::new();
    let mut work_tick = req.ticker();
    for doc in 0..ix.docs as u32 {
        if !req.keep_going(&mut work_tick) {
            break;
        }
        if !alive(ix, req, doc) {
            continue;
        }
        // Ветви идут вперёд по номеру, поэтому дерево пересобирается на каждом
        // документе: перебор дорог по построению, зато не зависит от порядка.
        let Some(mut fresh) =
            bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref())
        else {
            break;
        };
        std::mem::swap(&mut root, &mut fresh);
        if let Some(score) = root.eval(doc) {
            hits.push(Hit { doc, score, keys: [0; MAX_SORT], nkeys: 0 });
        }
    }
    hits
}

pub fn search_exhaustive(ix: &Index, req: &Request) -> Vec<Hit> {
    if req.skipped {
        return Vec::new();
    }
    let mut cur: Vec<Union> = Vec::new();
    let mut work_tick = req.ticker();
    for wq in req.words.iter() {
        if !req.keep_going(&mut work_tick) {
            return Vec::new();
        }
        let df: u32 = wq.lists.iter().map(|&l| ix.t_df(l as usize)).sum();
        if df == 0 {
            continue;
        }
        let w = wq.weight(ix, df);
        let subs: Vec<Cursor> = wq
            .lists
            .iter()
            .filter(|&&l| ix.t_df(l as usize) > 0)
            .map(|&l| ix.cursor_in(l, wq.field, w, req.score_at(wq.field)))
            .collect();
        if !subs.is_empty() {
            cur.push(Union::new(subs));
        }
    }

    let mut hits: Vec<Hit> = Vec::new();
    loop {
        if !req.keep_going(&mut work_tick) {
            break;
        }
        let doc = cur.iter().map(|c| c.cur()).min().unwrap_or(u32::MAX);
        if doc == u32::MAX {
            break;
        }
        let alive = alive(ix, req, doc);
        if alive {
            let score: f64 = cur.iter_mut().filter(|c| c.cur() == doc).map(|c| c.score()).sum();
            hits.push(Hit { doc, score, keys: [0; MAX_SORT], nkeys: 0 });
        }
        for c in cur.iter_mut() {
            if c.cur() == doc {
                c.next();
            }
        }
    }
    hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.doc.cmp(&b.doc)));
    hits.truncate(req.k);
    hits
}

/// Виден ли документ полному обходу: жив, прошёл отбор и дерево условий.
///
/// Отдельной функцией потому, что кандидатов поставляют два источника — отбор
/// и перебор, — а решение по документу обязано быть одно на оба.
#[inline(always)]
fn scanned_prepared(
    ix: &Index,
    req: &Request,
    root: &mut Option<BoolNode>,
    doc: u32,
    filters: &mut ScanFilters<'_>,
) -> bool {
    scanned_score_prepared(ix, req, root, doc, false, filters).is_some()
}

#[inline(always)]
fn scanned_score_prepared(
    ix: &Index,
    req: &Request,
    root: &mut Option<BoolNode>,
    doc: u32,
    score: bool,
    filters: &mut ScanFilters<'_>,
) -> Option<f64> {
    stats::scan_visited(1);
    if !req.is_live(ix, doc) || !filters.allows(doc, req.guard.as_deref()) {
        return None;
    }
    if req.mode == MatchMode::AnyTerm {
        return Some(0.0);
    }
    // `None` от bind означает невозможную обязательную ветвь, а не MatchAll:
    // иначе must(missing) AND must(existing) ошибочно посчитает existing.
    match root.as_mut() {
        Some(r) if score => r.eval(doc),
        Some(r) => r.matches(doc).then_some(0.0),
        None => None,
    }
}

/// Обходит **все** совпадения запроса, ничего не отсекая.
///
/// Верхушка стоит «идти, пока не отсечётся»: порог поднимается после каждого
/// документа и позволяет пропускать блоки не читая. Агрегация обязана увидеть
/// каждое совпадение, иначе сумма перестаёт быть суммой. Поэтому здесь отдельный
/// обход, а не ветка внутри отсечения: смешай их — и однажды порог поднимут там,
/// где считают.
///
/// Кандидаты берутся из объединения слов, если слова есть; иначе — у отбора, а
/// когда и он не берётся вести — перебором, потому что взять их больше неоткуда.
/// Отбор по колонке при этом работает как обычно, через `alive`.
///
/// ## Почему приёмник отвечает, а не просто принимает
///
/// `false` обрывает обход немедленно. Без этого счёт был работой без предела:
/// `count` без слов на десяти миллионах документов шёл до конца, сколько бы
/// времени это ни заняло, и прекратить его было нельзя ничем — ни сроком, ни
/// отменой, ни уходом клиента. Кто именно решает прекратить, знает вызывающий;
/// здесь известно только, что его решение обязано исполниться сразу.
/// Делится ли обход сегмента по диапазону документов.
///
/// ## Что должно совпасть
///
/// Кандидатов обязан поставлять **перебор**, а не курсор списка: курсор идёт
/// вперёд и делить его между потоками нельзя, не научив переходить к нужному
/// номеру. То же и с ведущим условием отбора — оно выдаёт кандидатов пачками
/// в своём порядке.
///
/// Дерево обязано быть пустым целиком, включая `must_not`: слово в любой его
/// ветви заводит курсор, а курсор снова означает последовательность.
///
/// Проверяется до обхода и стоит перебора условий отбора — [`filter_lead`]
/// спрашивает у колонки оценку, не читая документов.
pub fn can_split(ix: &Index, req: &Request) -> bool {
    req.words.is_empty()
        && req.must.is_empty()
        && req.tree.must.is_empty()
        && req.tree.should.is_empty()
        && req.tree.must_not.is_empty()
        && filter_lead(ix, req, ix.docs).is_none()
}

/// Перебор совпадений в диапазоне `[lo, hi)`.
///
/// Зовётся только когда [`can_split`] согласился: остальные источники
/// кандидатов диапазоном не режутся. Полоса видит все свои совпадения — деление
/// идёт по номерам документов, а не по выборке, и ни одно не теряется.
pub(crate) fn for_each_match_in(
    ix: &Index,
    req: &Request,
    lo: u32,
    hi: u32,
    on: &mut dyn FnMut(u32) -> bool,
) {
    debug_assert!(can_split(ix, req), "диапазонный обход вызван для неделимого запроса");
    debug_assert!(lo <= hi, "обратный диапазон документов {lo}..{hi}");
    if req.mode == MatchMode::AnyTerm {
        return;
    }
    let mut root = bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref());
    if req.skipped || root.is_none() {
        return;
    }
    let hi = hi.min(ix.docs as u32);
    let mut work_tick = req.ticker();
    let mut filters = ScanFilters::new(ix, req);
    for doc in lo..hi {
        if !req.keep_going(&mut work_tick) {
            return;
        }
        if scanned_prepared(ix, req, &mut root, doc, &mut filters) && !on(doc) {
            return;
        }
    }
}

pub fn for_each_match(ix: &Index, req: &Request, on: &mut dyn FnMut(u32) -> bool) {
    for_each_scored_match(ix, req, false, &mut 0, &mut |doc, _| on(doc));
}

/// Один matcher для exhaustive collectors. Score вычисляется только когда
/// его требует top-K; count сам по себе не платит за BM25.
fn for_each_scored_match(
    ix: &Index,
    req: &Request,
    score: bool,
    examined: &mut u64,
    on: &mut dyn FnMut(u32, f64) -> bool,
) {
    let mut filters = ScanFilters::new(ix, req);
    let mut root = if req.mode == MatchMode::Boolean {
        bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref())
    } else {
        None
    };
    if req.skipped || (req.mode == MatchMode::Boolean && root.is_none()) {
        return;
    }
    let mut cur: Vec<Union> = Vec::new();
    let mut work_tick = req.ticker();
    let mut mandatory = Vec::new();
    if req.mode == MatchMode::Boolean {
        required_words(&req.tree, &mut mandatory);
    }
    let rarest = mandatory
        .into_iter()
        .min_by_key(|w| w.lists.iter().map(|&list| ix.t_df(list as usize) as u64).sum::<u64>());
    let leading = if let Some(word) = rarest {
        vec![word]
    } else {
        let mut words = Vec::new();
        // OR с отрицательной альтернативой способен совпасть без любого
        // положительного слова. Его нельзя ограничивать union postings.
        if req.mode == MatchMode::AnyTerm {
            words.extend(&req.words);
        } else if positive_cover(&req.tree) {
            all_words(&req.tree, &mut words);
        }
        words
    };
    for wq in leading {
        if !req.keep_going(&mut work_tick) {
            return;
        }
        let df: u32 = wq.lists.iter().map(|&l| ix.t_df(l as usize)).sum();
        if df == 0 {
            continue;
        }
        let w = wq.weight(ix, df);
        let subs: Vec<Cursor> = wq
            .lists
            .iter()
            .filter(|&&l| ix.t_df(l as usize) > 0)
            .map(|&l| ix.cursor_in(l, wq.field, w, req.score_at(wq.field)))
            .collect();
        if !subs.is_empty() {
            cur.push(Union::new(subs));
        }
    }
    if cur.is_empty() {
        // AnyTerm без postings — MatchNone, а Boolean без обязательного
        // положительного покрытия может содержать MatchAll/NOT-ветви.
        if req.mode == MatchMode::AnyTerm {
            return;
        }
        // Слов, ведущих обход, нет. Кандидатов поставляет отбор, если он
        // обещает меньше целого сегмента, — иначе перебор, потому что взять их
        // больше неоткуда.
        //
        // Прежде здесь всегда шёл перебор `0..ix.docs`. Это и есть цена
        // измеренных 3.89 QPS у `count` и 3.05 у `range`: запрос «сколько за
        // неделю» читал живую карту и вычислял дерево на каждом из десяти
        // миллионов документов, чтобы отбор тут же отбросил почти всё
        // просмотренное. Отбор при этом умел называть своих кандидатов сам —
        // просто его об этом не спрашивали.
        match filter_lead(ix, req, ix.docs) {
            Some(i) => {
                each_candidate(ix, &req.filter[i], req.guard.as_deref(), &mut |candidates| {
                    for &doc in candidates {
                        if !req.keep_going(&mut work_tick) {
                            return false;
                        }
                        *examined += 1;
                        if let Some(score) =
                            scanned_score_prepared(ix, req, &mut root, doc, score, &mut filters)
                        {
                            if !on(doc, score) {
                                return false;
                            }
                        }
                    }
                    true
                });
            }
            None => {
                for doc in 0..ix.docs as u32 {
                    if !req.keep_going(&mut work_tick) {
                        return;
                    }
                    *examined += 1;
                    if let Some(score) =
                        scanned_score_prepared(ix, req, &mut root, doc, score, &mut filters)
                    {
                        if !on(doc, score) {
                            return;
                        }
                    }
                }
            }
        }
        return;
    }
    loop {
        if !req.keep_going(&mut work_tick) {
            return;
        }
        let doc = cur.iter().map(|c| c.cur()).min().unwrap_or(u32::MAX);
        if doc == u32::MAX {
            break;
        }
        *examined += 1;
        if let Some(mut matched_score) =
            scanned_score_prepared(ix, req, &mut root, doc, score, &mut filters)
        {
            if score && req.mode == MatchMode::AnyTerm {
                matched_score = cur.iter_mut().filter(|c| c.cur() == doc).map(|c| c.score()).sum();
            }
            if !on(doc, matched_score) {
                return;
            }
        }
        for c in cur.iter_mut() {
            if c.cur() == doc {
                c.next();
            }
        }
    }
}

/// Exact count и верхушка получают один поток совпадений. Порог heap не
/// отсекает matcher: иначе count стал бы числом только конкурентных hits.
/// search_after ограничивает выдачу, но не полное число совпадений запроса.
pub fn search_count(ix: &Index, req: &Request) -> (Vec<Hit>, u64, u64) {
    let floor = if req.mode == MatchMode::Boolean { f64::NEG_INFINITY } else { 0.0 };
    search_count_above(ix, req, floor)
}

pub(crate) fn search_count_above(ix: &Index, req: &Request, floor: f64) -> (Vec<Hit>, u64, u64) {
    search_count_budgeted_above(ix, req, floor, None)
}

/// Общий остаток `track_total_hits`: он ограничивает только счёт, а верхушка
/// после его исчерпания использует обычный search с безопасным отсечением.
pub(crate) fn search_count_budgeted_above(
    ix: &Index,
    req: &Request,
    floor: f64,
    remaining: Option<&std::sync::atomic::AtomicU64>,
) -> (Vec<Hit>, u64, u64) {
    use std::sync::atomic::Ordering;
    let top = || match req.mode {
        MatchMode::AnyTerm => search(ix, req, floor),
        MatchMode::Boolean => search_boolean(ix, req, floor),
    };
    if remaining.is_some_and(|remaining| remaining.load(Ordering::Relaxed) == 0) {
        let (hits, examined) = if req.k == 0 { (Vec::new(), 0) } else { top() };
        return (hits, 0, examined);
    }
    if let Some((count, scanned)) = range_summary_count(ix, req)
        .or_else(|| exact_metadata_count(ix, req).map(|count| (count, 0)))
    {
        let empty = count == 0;
        let count = remaining.map_or(count, |remaining| {
            remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    Some(left.saturating_sub(count))
                })
                .expect("обновление остатка всегда разрешено")
                .min(count)
        });
        if req.k == 0 || empty {
            return (Vec::new(), count, scanned);
        }
        let (hits, examined) = match req.mode {
            MatchMode::AnyTerm => search(ix, req, floor),
            MatchMode::Boolean => search_boolean(ix, req, floor),
        };
        return (hits, count, examined.saturating_add(scanned));
    }
    let mut heap = Vec::with_capacity(req.k.saturating_add(1));
    let mut count = 0u64;
    let mut examined = 0u64;
    let mut capped = false;
    for_each_scored_match(ix, req, req.k > 0, &mut examined, &mut |doc, score| {
        if let Some(remaining) = remaining {
            match remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| left.checked_sub(1))
            {
                Ok(left) => {
                    count += 1;
                    if left == 1 {
                        capped = true;
                        return false;
                    }
                }
                Err(_) => {
                    capped = true;
                    return false;
                }
            }
        } else {
            count += 1;
        }
        if req.k > 0 {
            let (keys, nkeys) = keys_of(ix, req, doc);
            if (nkeys > 0 || score >= floor) && after_allows(req, score, &keys, nkeys, doc) {
                offer_key(&mut heap, req.k, doc, score, keys, nkeys, false);
            }
        }
        true
    });
    if capped {
        // Предыдущий проход мог остановиться до лучшего документа. Полный
        // top-K пересчитывается независимо от порога totals и search_after.
        let (hits, searched) = if req.k == 0 { (Vec::new(), 0) } else { top() };
        return (hits, count, examined.saturating_add(searched));
    }
    heap.sort_by(|a, b| worst_first(b, a));
    (heap, count, examined)
}

/// Заголовки numeric-колонки и presence bitmap позволяют считать документы
/// только для scalar-поля и полностью живого снимка. Массивы и tombstones
/// идут matcher-ом.
pub(crate) fn can_count_range_summaries(ix: &Index, req: &Request) -> bool {
    req.mode == MatchMode::Boolean
        && !req.skipped
        && req.words.is_empty()
        && req.must.is_empty()
        && req.must_not.is_empty()
        && req.tree.must.is_empty()
        && req.tree.should.is_empty()
        && req.tree.must_not.is_empty()
        && req.tree.min_should == 0
        && req.filter.len() == 1
        && req.live.as_ref().is_some_and(|live| live.docs() == ix.docs && live.all_live())
        && ix.fields.get(req.filter[0].field).is_some_and(|field| {
            field.multi_column.is_none()
                && field.column.as_ref().is_some_and(|column| column.docs() == ix.docs)
        })
}

/// Выбирает блоки колонки или value buckets по числу краевых значений.
/// Полностью покрытые области не декодируются; края читают значения.
/// Второе число — реально проверенные документы, а не число совпадений.
pub(crate) fn range_summary_count(ix: &Index, req: &Request) -> Option<(u64, u64)> {
    if !can_count_range_summaries(ix, req) || !req.running() {
        return None;
    }
    let range = &req.filter[0];
    if range.from > range.to {
        return Some((0, 0));
    }
    let column = ix.fields[range.field].column.as_ref()?;
    let Some(buckets) = column.buckets.as_ref() else {
        return column.count_range_blocks(range.from, range.to, usize::MAX, req.guard.as_deref());
    };
    let (first, last, _) = buckets.span_of(range.from, range.to);
    let bucket_scan = (first..=last)
        .filter(|&bucket| {
            let (lo, hi) = buckets.covers(bucket);
            lo < range.from || hi > range.to
        })
        .map(|bucket| buckets.len_of(bucket))
        .sum();
    if let Some(count) =
        column.count_range_blocks(range.from, range.to, bucket_scan, req.guard.as_deref())
    {
        return Some(count);
    }
    let (mut count, mut scanned) = (0, 0);
    let mut docs = Vec::new();
    let mut ticker = req.ticker();
    for bucket in first..=last {
        if !req.running() {
            break;
        }
        let (lo, hi) = buckets.covers(bucket);
        if lo >= range.from && hi <= range.to {
            count += buckets.len_of(bucket) as u64;
            continue;
        }
        buckets.docs(bucket, &mut docs);
        for &doc in &docs {
            if !req.keep_going(&mut ticker) {
                return Some((count, scanned));
            }
            scanned += 1;
            crate::stats::scan_visited(1);
            if column.get(doc).is_some_and(|v| v >= range.from && v <= range.to) {
                count += 1;
            }
        }
    }
    Some((count, scanned))
}

/// DF без deletes либо полное покрытие документов позволяют exact count
/// по immutable visibility. Для MatchAll и DF==docs достаточно live cardinality
/// даже после deletes; для неполного покрытия нужен полный matcher.
pub(crate) fn exact_metadata_count(ix: &Index, req: &Request) -> Option<u64> {
    if req.skipped
        || !req.running()
        || !req.filter.is_empty()
        || !req.live.as_ref().is_some_and(|live| live.docs() == ix.docs)
    {
        return None;
    }
    let live = req.live.as_ref()?;
    let visible = live.docs().saturating_sub(live.dead()) as u64;
    let word = match req.mode {
        MatchMode::AnyTerm => match req.words.as_slice() {
            [word] => word,
            _ => return None,
        },
        MatchMode::Boolean => {
            if !req.tree.must_not.is_empty() || req.tree.min_should > req.tree.should.len() {
                return None;
            }
            match (req.tree.must.as_slice(), req.tree.should.as_slice()) {
                ([], []) => return Some(visible),
                ([Clause::Word(word)], []) => word,
                ([], [Clause::Word(word)]) if req.tree.min_should <= 1 => word,
                _ => return None,
            }
        }
    };
    let [list] = word.lists.as_slice() else { return None };
    let df = ix.term(*list as usize, word.field).df as u64;
    if df == ix.docs as u64 {
        Some(visible)
    } else if live.all_live() {
        Some(df)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use crate::segments::{QuerySpec, Segments, SpecWord, Term};

    #[test]
    fn zero_top_k_skips_top_collectors_and_preserves_exact_counts() {
        use crate::segments::{Sort, SpecBool, SpecRange, TrackTotalHits};
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("svindb-zero-top-{}", std::process::id()));
        let dir = dir.to_str().unwrap();
        let mut writer = Indexer::typed(
            dir,
            vec!["body".into(), "rank".into()],
            vec![1.0; 2],
            vec![crate::index::Kind::Text, crate::index::Kind::Int64],
            2,
        );
        for doc in 0..6 {
            writer.add(&doc.to_string(), &["common", &doc.to_string()]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(dir).unwrap();
        assert!(segments.list.len() > 1);
        for shape in 0..3 {
            let mut spec = QuerySpec {
                k: 0,
                track_total_hits: TrackTotalHits::Exact,
                live: Some(Arc::new(
                    segments.list.iter().map(|ix| Arc::new(ix.live_copy())).collect(),
                )),
                ..QuerySpec::default()
            };
            match shape {
                0 => spec.nested = Some(SpecBool::default()),
                1 => {
                    spec.words =
                        vec![SpecWord { fid: 0, terms: vec![Term::parse("common")], given: None }]
                }
                _ => {
                    spec.filter = vec![SpecRange { fid: 1, from: 2, to: 5 }];
                    spec.sort = vec![Sort { fid: 1, desc: true }];
                }
            }
            for width in [1, 4] {
                let top = segments.search_with(&spec, width);
                assert!(top.hits.is_empty());
                assert_eq!(top.visited, 0);
                assert!(!top.partial);
                let (counted, count) = segments.search_count_with_g(&spec, width, None);
                assert!(counted.hits.is_empty());
                assert!(!counted.partial);
                assert_eq!(count, if shape == 2 { 4 } else { 6 });
            }
            assert!(segments.search_g(&spec, None).hits.is_empty());
            for (ix, req) in segments.list.iter().zip(segments.plan_with(&spec, None)) {
                assert_eq!(search_with(ix, &req, 0.0, 4).1, 0);
                assert!(search_boolean(ix, &req, f64::NEG_INFINITY).0.is_empty());
            }
        }
        drop(segments);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn range_summary_count_matches_scan_and_respects_visibility_and_threshold() {
        use crate::segments::{SpecBool, SpecRange};
        use std::sync::{atomic::AtomicU64, Arc};
        let dir = std::env::temp_dir().join(format!("svindb-range-summary-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer = Indexer::typed(
            &dir,
            vec!["n".into()],
            vec![1.0],
            vec![crate::index::Kind::Int64],
            20_000,
        );
        for doc in 0..10_000 {
            // Missing scalar values must not enter a completely covered bucket.
            let value = if doc % 11 == 0 { String::new() } else { doc.to_string() };
            writer.add(&doc.to_string(), &[&value]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let ix = &segments.list[0];
        let spec = QuerySpec {
            nested: Some(SpecBool::default()),
            k: 0,
            live: Some(Arc::new(vec![Arc::new(ix.live_copy())])),
            filter: vec![SpecRange { fid: 0, from: 1000, to: 8000 }],
            ..QuerySpec::default()
        };
        let mut req = segments.plan_with(&spec, None).remove(0);
        let (fast, scanned) = range_summary_count(ix, &req).expect("scalar summary");
        let expected = (1000..=8000).filter(|doc| doc % 11 != 0).count() as u64;
        assert_eq!(fast, expected);
        assert!(scanned < expected / 4, "covered buckets decoded {scanned} documents");
        let cap = AtomicU64::new(101);
        assert_eq!(search_count_budgeted_above(ix, &req, 0.0, Some(&cap)).1, 101);
        assert_eq!(cap.load(std::sync::atomic::Ordering::Relaxed), 0);
        req.k = 7;
        let (fast_hits, fast_count, _) = search_count(ix, &req);
        req.live = None;
        assert!(range_summary_count(ix, &req).is_none());
        let (scan_hits, scan_count, _) = search_count(ix, &req);
        assert_eq!(fast_count, expected);
        assert_eq!(scan_count, expected);
        assert_eq!(
            fast_hits.iter().map(|hit| hit.doc).collect::<Vec<_>>(),
            scan_hits.iter().map(|hit| hit.doc).collect::<Vec<_>>()
        );
        req.k = 0;
        assert!(ix.kill(1000));
        req.live = Some(Arc::new(ix.live_copy()));
        assert!(range_summary_count(ix, &req).is_none());
        assert_eq!(search_count(ix, &req).1, expected - 1);
        req.live = spec.live.as_ref().map(|maps| Arc::clone(&maps[0]));
        assert_eq!(search_count(ix, &req).1, expected, "frozen visibility");
        req.filter[0].from = i64::MIN;
        req.filter[0].to = i64::MAX;
        assert_eq!(range_summary_count(ix, &req), Some((9090, 0)));
        req.tree.min_should = 1;
        assert!(range_summary_count(ix, &req).is_none());
        assert_eq!(search_count(ix, &req).1, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn range_summary_short_integers_count_covered_column_blocks() {
        use crate::segments::{SpecBool, SpecRange};
        use std::sync::Arc;
        let dir =
            std::env::temp_dir().join(format!("svindb-range-shortints-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer = Indexer::typed(
            &dir,
            vec!["n".into()],
            vec![1.0],
            vec![crate::index::Kind::Int64],
            100_001,
        );
        for doc in 0..100_000 {
            writer.add(&doc.to_string(), &[&doc.to_string()]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let ix = &segments.list[0];
        let spec = QuerySpec {
            nested: Some(SpecBool::default()),
            k: 0,
            live: Some(Arc::new(vec![Arc::new(ix.live_copy())])),
            filter: vec![SpecRange { fid: 0, from: 25_000, to: 75_000 }],
            ..QuerySpec::default()
        };
        let req = segments.plan_with(&spec, None).remove(0);
        let (count, scanned) = range_summary_count(ix, &req).unwrap();
        assert_eq!(count, 50_001);
        assert!(
            scanned <= 2 * crate::column::BLOCK as u64,
            "short integer range decoded {scanned} values instead of edge blocks"
        );
        assert_eq!(search_count(ix, &req).1, count);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn range_summary_respects_extreme_bucket_edges_and_cancellation() {
        use crate::segments::{SpecBool, SpecRange};
        use std::sync::Arc;
        let dir =
            std::env::temp_dir().join(format!("svindb-range-extremes-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer =
            Indexer::typed(&dir, vec!["n".into()], vec![1.0], vec![crate::index::Kind::Int64], 10);
        for (i, value) in [i64::MIN, 0, i64::MAX].iter().enumerate() {
            writer.add(&i.to_string(), &[&value.to_string()]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let ix = &segments.list[0];
        let spec = QuerySpec {
            nested: Some(SpecBool::default()),
            k: 0,
            live: Some(Arc::new(vec![Arc::new(ix.live_copy())])),
            filter: vec![SpecRange { fid: 0, from: i64::MIN, to: i64::MAX - 1 }],
            ..QuerySpec::default()
        };
        let mut req = segments.plan_with(&spec, None).remove(0);
        assert_eq!(range_summary_count(ix, &req).unwrap().0, 2);
        req.filter[0].from = i64::MIN + 1;
        req.filter[0].to = i64::MAX;
        assert_eq!(range_summary_count(ix, &req).unwrap().0, 2);
        let guard = Arc::new(crate::work::Guard::new(0));
        guard.cancel();
        req.guard = Some(guard);
        assert!(range_summary_count(ix, &req).is_none());
        assert_eq!(search_count(ix, &req).1, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn range_summary_rejects_multivalues_and_counts_documents_once() {
        use crate::{
            indexer::Cell,
            segments::{SpecBool, SpecRange},
        };
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("svindb-range-multi-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer =
            Indexer::typed(&dir, vec!["n".into()], vec![1.0], vec![crate::index::Kind::Int64], 10);
        let values = crate::indexer::encode_multi(&["3".into(), "4".into(), "4".into()]);
        writer.add_cells("a", &[1], &[Cell { field: 0, slot: 0, text: &values }]);
        writer.add_cells("b", &[0], &[]);
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let ix = &segments.list[0];
        let spec = QuerySpec {
            nested: Some(SpecBool::default()),
            k: 0,
            live: Some(Arc::new(vec![Arc::new(ix.live_copy())])),
            filter: vec![SpecRange { fid: 0, from: 2, to: 5 }],
            ..QuerySpec::default()
        };
        let req = segments.plan_with(&spec, None).remove(0);
        assert!(ix.fields[0].multi_column.is_some(), "must exercise actual multivalue column");
        assert!(range_summary_count(ix, &req).is_none());
        assert_eq!(search_count(ix, &req).1, 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn metadata_count_requires_immutable_visibility_and_proven_coverage() {
        use std::sync::Arc;
        const DOCS: u64 = 70_000;
        let dir =
            std::env::temp_dir().join(format!("svindb-count-metadata-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer = Indexer::new(
            &dir,
            vec!["body".into(), "narrow".into()],
            vec![1.0, 1.0],
            DOCS as u32 + 1,
        );
        for doc in 0..DOCS {
            writer.add(&doc.to_string(), &["common", if doc % 2 == 0 { "rare" } else { "" }]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let ix = &segments.list[0];
        let spec = QuerySpec {
            words: vec![SpecWord { fid: 0, terms: vec![Term::parse("common")], given: None }],
            k: 7,
            ..QuerySpec::default()
        };
        let mut req = segments.plan_with(&spec, None).remove(0);
        assert_eq!(exact_metadata_count(ix, &req), None, "mutable all_live не является snapshot");
        req.live = Some(Arc::new(ix.live_copy()));
        assert_eq!(exact_metadata_count(ix, &req), Some(DOCS));
        let frozen = Arc::clone(req.live.as_ref().unwrap());
        assert!(ix.kill(0));
        let (old_hits, old_count, _) = search_count(ix, &req);
        assert_eq!(old_count, DOCS, "PIT должен сохранить старую видимость");
        assert_eq!(old_hits[0].doc, 0);
        req.live = Some(Arc::new(ix.live_copy()));
        assert_eq!(
            exact_metadata_count(ix, &req),
            Some(DOCS - 1),
            "полное покрытие допускает live cardinality"
        );
        let partial_coverage = QuerySpec {
            words: vec![SpecWord { fid: 1, terms: vec![Term::parse("rare")], given: None }],
            live: Some(Arc::new(vec![Arc::new(ix.live_copy())])),
            ..QuerySpec::default()
        };
        let partial_req = segments.plan_with(&partial_coverage, None).remove(0);
        assert_eq!(
            exact_metadata_count(ix, &partial_req),
            None,
            "deletes вне/внутри неполного posting нельзя угадать по DF"
        );
        let (new_hits, new_count, _) = search_count(ix, &req);
        assert_eq!(new_count, DOCS - 1);
        assert_eq!(new_hits[0].doc, 1);
        req.live = Some(Arc::new(crate::live::Snapshot::all(ix.docs)));
        req.mode = MatchMode::Boolean;
        req.tree = BoolReq { min_should: 1, ..BoolReq::default() };
        assert_eq!(exact_metadata_count(ix, &req), None);
        assert_eq!(search_count(ix, &req).1, 0, "невозможный minimum не является MatchAll");
        // Большой match-all обычно делится на lanes. Summary-план не должен
        // терять O(1) ответ только из-за предоставленных восьми workers.
        let all = QuerySpec {
            nested: Some(crate::segments::SpecBool::default()),
            live: Some(Arc::new(vec![frozen])),
            ..QuerySpec::default()
        };
        let aggs = [crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        let _ = stats::take();
        let counted = segments.aggregate_with(&all, &aggs, 8);
        assert!(!counted.partial);
        assert!(matches!(counted.values[0].value, crate::aggs::Value::Stats { count: DOCS, .. }));
        if stats::ENABLED {
            assert_eq!(stats::take().scan_visited, 0);
        }
        let current = QuerySpec { live: Some(Arc::new(vec![Arc::new(ix.live_copy())])), ..all };
        let counted = segments.aggregate_with(&current, &aggs, 8);
        assert!(!counted.partial);
        assert!(
            matches!(counted.values[0].value, crate::aggs::Value::Stats { count, .. } if count == DOCS - 1)
        );
        if stats::ENABLED {
            assert_eq!(stats::take().scan_visited, 0, "MatchAll после delete сканирует postings");
        }
        drop(segments);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn slot_set_operations_match_independent_membership_and_stop_mid_filter() {
        for seed in 0..64u32 {
            let original: Vec<_> = (0..500).filter(|v| (v * 17 + seed) % 7 < 4).collect();
            let other: Vec<_> = (0..500).filter(|v| (v * 13 + seed) % 11 < 5).collect();
            let mut difference = original.clone();
            assert!(subtract_slots(&mut difference, &other, None));
            assert_eq!(
                difference,
                original.iter().copied().filter(|v| !other.contains(v)).collect::<Vec<_>>()
            );
            let mut common = original.clone();
            assert!(intersect(&mut common, &other, None));
            assert_eq!(
                common,
                original.iter().copied().filter(|v| other.contains(v)).collect::<Vec<_>>()
            );
        }
        let guard = crate::work::Guard::new(0);
        let mut slots: Vec<_> = (0..100_000).collect();
        let mut examined = 0;
        assert!(!retain_slots(&mut slots, Some(&guard), |_| {
            examined += 1;
            if examined == 17 {
                guard.cancel();
            }
            true
        }));
        assert_eq!(examined, 17, "отмена не остановила внутренний обход слотов");
        assert!(slots.is_empty(), "частичный результат выглядит как полный slot set");
    }

    #[test]
    fn fused_top_and_exact_count_preserve_boolean_sort_and_cursor_contracts() {
        use crate::segments::{Sort, SpecBool, SpecClause};
        let dir = std::env::temp_dir().join(format!("svindb-fused-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer = Indexer::typed(
            &dir,
            vec!["body".into(), "rank".into()],
            vec![1.0, 1.0],
            vec![crate::index::Kind::Text, crate::index::Kind::Int64],
            17,
        );
        for doc in 0..127 {
            let text = format!(
                "common {} {} {}",
                if doc % 2 == 0 { "alpha" } else { "" },
                if doc % 3 == 0 { "beta" } else { "" },
                if doc % 11 == 0 { "rare" } else { "" }
            );
            let rank = if doc % 5 == 0 { String::new() } else { (doc - 60).to_string() };
            writer.add(&doc.to_string(), &[&text, &rank]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let word = |text| SpecWord { fid: 0, terms: vec![Term::parse(text)], given: None };
        for scenario in 0..7 {
            let mut spec = QuerySpec { k: 7, ..QuerySpec::default() };
            let expected = (0..127)
                .filter(|doc| match scenario {
                    0 => doc % 2 == 0 || doc % 11 == 0,
                    1 => doc % 11 == 0,
                    2 => doc % 2 == 0 || doc % 3 != 0,
                    3 => doc % 3 != 0,
                    4 => true,
                    6 => (50..=70).contains(doc) && doc % 5 != 0,
                    _ => false,
                })
                .count() as u64;
            match scenario {
                0 => spec.words = vec![word("alpha"), word("rare")],
                1 => spec.must = vec![word("common"), word("rare")],
                2 => {
                    spec.nested = Some(SpecBool {
                        should: vec![
                            SpecClause::Term(word("alpha")),
                            SpecClause::Bool(SpecBool {
                                must_not: vec![SpecClause::Term(word("beta"))],
                                ..SpecBool::default()
                            }),
                        ],
                        min_should: 1,
                        ..SpecBool::default()
                    })
                }
                3 => spec.must_not = vec![word("beta")],
                4 => spec.nested = Some(SpecBool::default()),
                6 => {
                    spec.words = vec![word("common")];
                    spec.filter = vec![crate::segments::SpecRange { fid: 1, from: -10, to: 10 }];
                }
                _ => spec.must = vec![word("missing")],
            }
            for order in 0..3 {
                spec.sort =
                    if order == 0 { Vec::new() } else { vec![Sort { fid: 1, desc: order == 2 }] };
                spec.after = None;
                for page in 0..2 {
                    let separate = segments.search_with(&spec, 1);
                    for width in [1, 4] {
                        let (fused, count) = segments.search_count_with_g(&spec, width, None);
                        assert!(!fused.partial);
                        assert_eq!(
                            count, expected,
                            "scenario={scenario} order={order} page={page}"
                        );
                        assert_eq!(fused.hits.len(), separate.hits.len());
                        for (a, b) in fused.hits.iter().zip(&separate.hits) {
                            assert_eq!((a.doc, a.keys, a.nkeys), (b.doc, b.keys, b.nkeys));
                            assert!((a.score - b.score).abs() < 1e-8);
                        }
                        for limit in [0, 2, expected, expected + 1] {
                            let limited = QuerySpec {
                                track_total_hits: crate::segments::TrackTotalHits::UpTo(limit),
                                ..spec.clone()
                            };
                            let (out, count) = segments.search_count_with_g(&limited, width, None);
                            assert!(!out.partial);
                            assert_eq!(count, expected.min(limit + 1));
                            assert_eq!(out.hits.len(), separate.hits.len());
                            for (a, b) in out.hits.iter().zip(&separate.hits) {
                                assert_eq!((a.doc, a.keys, a.nkeys), (b.doc, b.keys, b.nkeys));
                                assert!((a.score - b.score).abs() < 1e-8);
                            }
                        }
                    }
                    let Some(last) = separate.hits.last() else { break };
                    if order == 0 {
                        break;
                    }
                    spec.after = Some((last.keys[..last.nkeys as usize].to_vec(), last.doc));
                }
            }
        }
        drop(segments);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Диагностический A/B CPU-проходов, не HTTP/production latency gate.
    #[test]
    #[ignore = "нагрузочный эксперимент выполняется только на Linux build host"]
    fn fused_collector_microbenchmark() {
        let dir = std::env::temp_dir().join(format!("svindb-fused-bench-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer = Indexer::new(&dir, vec!["body".into()], vec![1.0], 20_000);
        for doc in 0..200_000 {
            writer
                .add(&doc.to_string(), &[if doc % 1_000 == 0 { "common rare" } else { "common" }]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let word = |text| SpecWord { fid: 0, terms: vec![Term::parse(text)], given: None };
        let aggs = [crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        for (name, mut spec, expected) in [
            ("common", QuerySpec { words: vec![word("common")], ..QuerySpec::default() }, 200_000),
            ("rare", QuerySpec { words: vec![word("rare")], ..QuerySpec::default() }, 200),
            (
                "must",
                QuerySpec { must: vec![word("common"), word("rare")], ..QuerySpec::default() },
                200,
            ),
        ] {
            spec.live = Some(std::sync::Arc::new(
                segments.list.iter().map(|ix| std::sync::Arc::new(ix.live_copy())).collect(),
            ));
            let mut separate_us = Vec::new();
            let mut fused_us = Vec::new();
            for trial in 0..24 {
                // Порядок меняется; первые четыре пары — прогрев, не выборка.
                for path in [trial % 2, 1 - trial % 2] {
                    // Одинаковый cancellation/deadline context для обоих
                    // путей: отсутствие guard у отдельного top-K иначе
                    // сравнивало разный runtime, а не только collectors.
                    spec.guard = Some(std::sync::Arc::new(crate::work::Guard::new(10_000)));
                    let start = std::time::Instant::now();
                    let hits = if path == 0 {
                        let hits = segments.search_with(&spec, 1);
                        let count = segments.aggregate(&spec, &aggs);
                        assert!(
                            matches!(count[0].value, crate::aggs::Value::Stats { count, .. } if count == expected)
                        );
                        hits
                    } else {
                        let (hits, count) = segments.search_count_with_g(&spec, 1, None);
                        assert_eq!(count, expected);
                        hits
                    };
                    assert!(!hits.partial);
                    std::hint::black_box(hits);
                    if trial >= 4 {
                        if path == 0 {
                            separate_us.push(start.elapsed().as_micros());
                        } else {
                            fused_us.push(start.elapsed().as_micros());
                        }
                    }
                }
            }
            separate_us.sort_unstable();
            fused_us.sort_unstable();
            eprintln!(
                "fused_ab name={name} observations=20 separate_median_us={} fused_median_us={}",
                separate_us[10], fused_us[10]
            );
        }
        drop(segments);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn external_aggregation_guard_cancels_a_running_zero_match_scan() {
        use std::sync::Arc;
        const DOCS: usize = 250_000;
        let root =
            std::env::temp_dir().join(format!("svindb-zero-count-cancel-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        let mut writer = Indexer::typed_stored(
            &root,
            vec!["body".into()],
            vec![1.0],
            vec![crate::index::Kind::Text],
            vec![false],
            DOCS as u32,
        );
        for doc in 0..DOCS {
            writer.add(&doc.to_string(), &["common"]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&root).unwrap();
        let spec = QuerySpec {
            must_not: vec![SpecWord { fid: 0, terms: vec![Term::parse("common")], given: None }],
            ..QuerySpec::default()
        };
        let aggs = [crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        let guard = Arc::new(crate::work::Guard::new(0));
        std::thread::scope(|scope| {
            let signal = Arc::clone(&guard);
            let cancel = scope.spawn(move || {
                let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
                while signal.progress().0 < 64 {
                    assert!(
                        std::time::Instant::now() < until,
                        "внешний guard не попал в выполняющийся scanner"
                    );
                    std::thread::yield_now();
                }
                let at = std::time::Instant::now();
                signal.cancel();
                at
            });
            let result = segments.aggregate_guarded(&spec, &aggs, 1, &guard);
            let stopped_at = std::time::Instant::now();
            let cancelled_at = cancel.join().unwrap();
            assert!(result.partial, "scanner скрыл отмену");
            assert!(guard.progress().0 < DOCS as u64, "после отмены был обойдён весь corpus");
            assert!(
                stopped_at.saturating_duration_since(cancelled_at)
                    < std::time::Duration::from_millis(250)
            );
        });
        drop(segments);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_negative_or_branch_does_not_require_a_positive_posting() {
        use crate::segments::{SpecBool, SpecClause};
        let root = std::env::temp_dir().join(format!("svindb-negative-or-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        let mut writer = Indexer::new(&root, vec!["body".into()], vec![1.0], 100);
        for (id, text) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
            writer.add(id, &[text]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&root).unwrap();
        let word = |text| {
            SpecClause::Term(SpecWord { fid: 0, terms: vec![Term::parse(text)], given: None })
        };
        let query = QuerySpec {
            nested: Some(SpecBool {
                should: vec![
                    word("alpha"),
                    SpecClause::Bool(SpecBool {
                        must_not: vec![word("beta")],
                        ..SpecBool::default()
                    }),
                ],
                min_should: 1,
                ..SpecBool::default()
            }),
            ..QuerySpec::default()
        };
        let got = segments.search(&query);
        assert_eq!(
            got.hits.iter().map(|h| h.doc).collect::<Vec<_>>(),
            [0, 2],
            "отрицательная OR-ветвь потеряла gamma"
        );
        drop(segments);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn aggregation_uses_the_rarest_required_posting_stream() {
        let root = std::env::temp_dir().join(format!("svindb-count-lead-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        let mut writer = Indexer::new(&root, vec!["body".into()], vec![1.0], 25_000);
        for doc in 0..20_000 {
            writer
                .add(&doc.to_string(), &[if doc % 1_000 == 0 { "common rare" } else { "common" }]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&root).unwrap();
        let word = |text| SpecWord { fid: 0, terms: vec![Term::parse(text)], given: None };
        let query = QuerySpec { must: vec![word("common"), word("rare")], ..QuerySpec::default() };
        let count = [crate::aggs::Agg {
            name: "count".into(),
            kind: crate::aggs::Kind::Stats { field: None },
            over: crate::aggs::Over::Docs,
            space: crate::aggs::Space::Int,
        }];
        let _ = stats::take();
        let start = std::time::Instant::now();
        let got = segments.aggregate(&query, &count);
        let elapsed = start.elapsed();
        let counters = stats::take();
        assert!(matches!(got[0].value, crate::aggs::Value::Stats { count: 20, .. }));
        eprintln!(
            "exact_count=20 scan_visited={} elapsed_us={}",
            counters.scan_visited,
            elapsed.as_micros()
        );
        if stats::ENABLED {
            assert_eq!(
                counters.scan_visited, 20,
                "count обошёл объединение вместо обязательного редкого списка"
            );
        }
        drop(segments);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn threshold_count_stops_scanning_and_metadata_keeps_hits_after_budget_exhaustion() {
        use crate::segments::{SpecBool, TrackTotalHits};
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("svindb-threshold-{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let mut writer = Indexer::new(&dir, vec!["body".into()], vec![1.0], 100);
        for doc in 0..1000 {
            writer.add(&doc.to_string(), &["common"]);
        }
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&dir).unwrap();
        let mut spec = QuerySpec {
            nested: Some(SpecBool::default()),
            track_total_hits: TrackTotalHits::UpTo(10),
            k: 0,
            ..QuerySpec::default()
        };
        for width in [1, 4] {
            let (limited, count) = segments.search_count_with_g(&spec, width, None);
            assert!(!limited.partial);
            assert_eq!(count, 11);
            assert!(limited.visited < 100, "threshold count прошёл {} документов", limited.visited);
        }
        spec.track_total_hits = TrackTotalHits::Exact;
        let (exact, count) = segments.search_count_with_g(&spec, 4, None);
        assert!(!exact.partial);
        assert_eq!(count, 1000);
        assert!(exact.visited >= 1000);

        // Immutable live включает metadata path. Нулевой остаток не означает,
        // что сегмент пуст: поздние сегменты по-прежнему отдают свои hits.
        let mut req = segments.plan_with(&spec, None).remove(0);
        req.k = 3;
        req.live = Some(Arc::new(segments.list[0].live_copy()));
        let remaining = std::sync::atomic::AtomicU64::new(0);
        let (hits, count, _) = search_count_budgeted_above(
            &segments.list[0],
            &req,
            f64::NEG_INFINITY,
            Some(&remaining),
        );
        assert_eq!(count, 0);
        assert_eq!(hits.len(), 3);
        drop(segments);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Корпус с разбросом длин: при одинаковых длинах ничьи скрывают ошибки
    /// отсечения, потому что порог упирается в них и дальше не растёт.
    fn corpus(name: &str) -> String {
        // Своё имя на каждый тест: тесты идут параллельно, а `remove_dir_all` в
        // одном каталоге под другим тестом даёт разъезд, который потом ищут
        // часами и списывают на «мигает».
        let dir = std::env::temp_dir().join(format!("svindb-search-{name}"));
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::new(&dir, vec!["title".into(), "body".into()], vec![2.0, 1.0], 300);
        for i in 0..1500u32 {
            let title = format!("док{} загол{}", i % 97, i % 13);
            // Длина тела гуляет от трёх слов до полусотни.
            let extra = " хвост".repeat((i % 47) as usize);
            let body = format!("общее слово{} форма{} тело{}{}", i % 7, i % 211, i % 53, extra);
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

    /// Отсечение обязано давать то же, что полный перебор.
    ///
    /// Это единственная проверка, которой можно верить: отсечение — утверждение
    /// «этот документ не может попасть в верхушку», и проверять его можно только
    /// тем, кто ничего не отсекает.
    #[test]
    fn pruned_search_matches_exhaustive() {
        let dir = corpus("exhaustive");
        let segs = Segments::load(&dir).unwrap();
        assert!(segs.list.len() > 1, "корпус обязан разбиться на сегменты");

        let queries = vec![
            spec(1, &["общее"], 10),
            spec(1, &["слово3"], 10),
            spec(1, &["форма17"], 5),
            // Маски: раскрытие в десятки форм — ради них всё и затевалось.
            spec(1, &["форма*"], 10),
            spec(1, &["слово*"], 10),
            spec(1, &["тело*"], 20),
            spec(0, &["док*"], 10),
            // Многословные — другой ход обхода.
            QuerySpec {
                words: vec![
                    SpecWord { given: None, fid: 1, terms: vec![Term::parse("общее")] },
                    SpecWord { given: None, fid: 0, terms: vec![Term::parse("док*")] },
                ],
                k: 10,
                ..QuerySpec::default()
            },
            QuerySpec {
                words: vec![
                    SpecWord { given: None, fid: 1, terms: vec![Term::parse("слово*")] },
                    SpecWord { given: None, fid: 1, terms: vec![Term::parse("хвост")] },
                    SpecWord { given: None, fid: 0, terms: vec![Term::parse("загол1")] },
                ],
                k: 10,
                ..QuerySpec::default()
            },
        ];

        for (n, q) in queries.iter().enumerate() {
            for (s, ix) in segs.list.iter().enumerate() {
                let req = q.resolve(ix);
                let (fast, _) = search(ix, &req, 0.0);
                let slow = search_exhaustive(ix, &req);
                let show = |h: &[Hit]| -> String {
                    h.iter()
                        .map(|x| format!("{}:{:.6}", x.doc, x.score))
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                assert_eq!(
                    fast.len(),
                    slow.len(),
                    "запрос {n}, сегмент {s}: {} против {}",
                    show(&fast),
                    show(&slow)
                );
                for (i, (a, b)) in fast.iter().zip(slow.iter()).enumerate() {
                    assert!(
                        (a.score - b.score).abs() < 1e-12,
                        "запрос {n}, сегмент {s}, место {i}: скор {} против {}",
                        a.score,
                        b.score
                    );
                    assert_eq!(
                        a.doc, b.doc,
                        "запрос {n}, сегмент {s}, место {i}: документ {} против {}\n  отсечение: {}\n  перебор:   {}",
                        a.doc,
                        b.doc,
                        show(&fast),
                        show(&slow)
                    );
                }
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Порог, перенесённый между сегментами, не имеет права резать выдачу.
    #[test]
    fn shared_floor_does_not_lose_documents() {
        let dir = corpus("floor");
        let segs = Segments::load(&dir).unwrap();
        for q in [spec(1, &["форма*"], 10), spec(1, &["общее"], 10), spec(0, &["док*"], 5)]
        {
            let out = segs.search(&q);
            let reqs = segs.plan_with(&q, None);
            // Эталон собирается парами «общий номер, вес»: `search::Hit` держит
            // номер **внутри сегмента**, а сверяется общий, и он восьмибайтный.
            let mut all: Vec<(u64, f64)> = Vec::new();
            for (i, ix) in segs.list.iter().enumerate() {
                for h in search_exhaustive(ix, &reqs[i]) {
                    all.push((h.doc as u64 + segs.base[i], h.score));
                }
            }
            all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            all.truncate(q.k);
            assert_eq!(out.hits.len(), all.len());
            for (a, b) in out.hits.iter().zip(all.iter()) {
                assert!((a.score - b.1).abs() < 1e-12, "скор {} против {}", a.score, b.1);
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Случайные корпуса и случайные многословные запросы против перебора.
    ///
    /// Отдельно от `pruned_search_matches_exhaustive`, и это не дублирование.
    /// Тот сверяет один сегмент с порогом ноль, а на нуле ветка отсечения по
    /// перенесённому порогу не срабатывает вовсе: вес BM25 положителен. Ветка
    /// живёт только в многосегментном запросе, где порог приходит от соседа.
    ///
    /// Почему случайные, а не подобранные руками. Отсечение теряет документ не
    /// на любых данных, а на совпадении трёх обстоятельств: порог должен
    /// сработать, порядок курсоров — разъехаться, и документ между отставшим и
    /// опорным должен встретиться сразу в существенном и в несущественном
    /// слове. Подбирать такое руками — гадание; двенадцати случайных корпусов
    /// хватает, чтобы это встретилось само.
    #[test]
    fn random_multiword_matches_exhaustive() {
        // Свой генератор, а не зависимость: xorshift64 хватает, а повторяемость
        // здесь важнее качества — упавшее семя должно падать снова.
        fn rng(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }

        let mut diverged: Vec<String> = Vec::new();
        for seed in 1..=12u64 {
            let mut st = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let dir = std::env::temp_dir()
                .join(format!("svindb-search-rnd-{}-{seed}", std::process::id()));
            let dir = dir.to_string_lossy().to_string();
            std::fs::remove_dir_all(&dir).ok();

            // Мелкие сегменты нарочно: порог переносится между ними, и чем их
            // больше, тем чаще он успевает подрасти до срабатывания ветки.
            let per = 40 + (rng(&mut st) % 120) as u32;
            let mut ix = Indexer::new(&dir, vec!["a".into(), "b".into()], vec![2.0, 1.0], per);
            let docs = 300 + (rng(&mut st) % 500) as u32;
            for i in 0..docs {
                let mut title = String::new();
                let mut body = String::new();
                for _ in 0..1 + rng(&mut st) % 4 {
                    title.push_str(&format!("t{} ", rng(&mut st) % 20));
                }
                // Длина тела гуляет от одного слова до тридцати: при одинаковых
                // длинах ничьи по весу прячут ошибки отсечения.
                for _ in 0..1 + rng(&mut st) % 30 {
                    body.push_str(&format!("w{} ", rng(&mut st) % 40));
                }
                ix.add(&format!("d{i}"), &[&title, &body]);
            }
            ix.flush();

            let segs = Segments::load(&dir).unwrap();
            for round in 0..8 {
                let nw = 2 + (rng(&mut st) % 3) as usize;
                let words: Vec<SpecWord> = (0..nw)
                    .map(|_| {
                        let field = (rng(&mut st) % 2) as usize;
                        let t = if field == 0 {
                            format!("t{}", rng(&mut st) % 20)
                        } else {
                            format!("w{}", rng(&mut st) % 40)
                        };
                        SpecWord { given: None, fid: field, terms: vec![Term::parse(&t)] }
                    })
                    .collect();
                let q = QuerySpec {
                    words,
                    k: 1 + (rng(&mut st) % 12) as usize,
                    ..QuerySpec::default()
                };

                let out = segs.search(&q);
                // Эталон берёт те же раскрытия и тот же общий вес слова, что и
                // проверяемое: иначе он сверял бы две разные функции веса.
                let reqs = segs.plan_with(&q, None);
                let mut all: Vec<(u64, f64)> = Vec::new();
                for (i, sg) in segs.list.iter().enumerate() {
                    for h in search_exhaustive(sg, &reqs[i]) {
                        all.push((h.doc as u64 + segs.base[i], h.score));
                    }
                }
                all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                all.truncate(q.k);

                // Сверяются и номера, и вес: потерянный документ виден по
                // номерам, недосчитанный вклад слова — только по весу.
                let show = |h: &[(u64, f64)]| -> String {
                    h.iter().map(|(d, s)| format!("{d}:{s:.6}")).collect::<Vec<_>>().join(" ")
                };
                let fast: Vec<(u64, f64)> = out.hits.iter().map(|x| (x.doc, x.score)).collect();
                let slow: Vec<(u64, f64)> = all.clone();
                // Сверяются три вещи: тот же набор документов, те же веса по
                // позициям и невозрастающий порядок у проверяемого.
                //
                // Именно набор, а не последовательность, и это **не** ослабление
                // проверки. С общей нормировкой по длине вклады документов из
                // разных сегментов стали точно сравнимы, и ничьи по весу из
                // редкости превратились в обычное дело. Порядок внутри ничьей
                // решает то, в каком порядке путь складывает вклады слов, —
                // отсечение идёт по опорному списку, перебор по порядку слов,
                // а сложение f64 не ассоциативно. Обещать одинаковый порядок
                // равных здесь нельзя, и требовать его значит проверять
                // разрядность сложения, а не отсечение.
                //
                // Что проверка ловит по-прежнему: потерянный документ (набор
                // разойдётся), недосчитанный вклад слова (веса разойдутся) и
                // сломанный порядок выдачи (последовательность весов перестанет
                // убывать).
                let key = |h: &[(u64, f64)]| -> Vec<u64> {
                    let mut v: Vec<u64> = h.iter().map(|(d, _)| *d).collect();
                    v.sort_unstable();
                    v
                };
                let ordered = fast.windows(2).all(|w| w[0].1 >= w[1].1);
                let same = fast.len() == slow.len()
                    && key(&fast) == key(&slow)
                    && ordered
                    && fast.iter().zip(slow.iter()).all(|(a, b)| (a.1 - b.1).abs() < 1e-12);
                if !same {
                    diverged.push(format!(
                        "семя {seed}, запрос {round}:\n  отсечение: {}\n  перебор:   {}",
                        show(&fast),
                        show(&slow)
                    ));
                }
            }
            std::fs::remove_dir_all(&dir).ok();
        }
        assert!(
            diverged.is_empty(),
            "отсечение разошлось с перебором в {} запросах:\n{}",
            diverged.len(),
            diverged.join("\n")
        );
    }

    /// Булево отсечение по блокам совпадает с полным перебором.
    ///
    /// Отсечение выбрасывает блоки не глядя внутрь — по одной оценке сверху.
    /// Ошибка в этой оценке не роняет ничего и не пишет в журнал: выдача просто
    /// становится короче или беднее, и заметить это можно единственным
    /// способом — сверив с перебором на многих случайных запросах.
    ///
    /// Случайность нарочно доходит до краёв: `k = 1` (порог поднимается с
    /// первого же документа), слова от очень частых до единичных, длины
    /// документов от одного слова до тридцати. При одинаковых длинах веса
    /// вырождаются в ничьи, а ничьи прячут ровно такие ошибки.
    #[test]
    fn boolean_pruning_matches_exhaustive() {
        fn rng(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }

        let mut diverged: Vec<String> = Vec::new();
        for seed in 1..=10u64 {
            let mut st = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let dir =
                std::env::temp_dir().join(format!("svindb-bool-rnd-{}-{seed}", std::process::id()));
            let dir = dir.to_string_lossy().to_string();
            std::fs::remove_dir_all(&dir).ok();

            let docs = 400 + (rng(&mut st) % 400) as u32;
            // Один сегмент: отсечение живёт внутри сегмента, и разбиение только
            // размыло бы его порогом, который переносится между ними.
            let mut ix = Indexer::new(&dir, vec!["a".into(), "b".into()], vec![2.0, 1.0], docs * 2);
            for i in 0..docs {
                let mut title = String::new();
                let mut body = String::new();
                for _ in 0..1 + rng(&mut st) % 4 {
                    title.push_str(&format!("t{} ", rng(&mut st) % 12));
                }
                for _ in 0..1 + rng(&mut st) % 30 {
                    // Половина словаря — частые слова, половина — редкие: у
                    // первых потолки блоков ровные, у вторых рваные, и
                    // отсечение ведёт себя по-разному.
                    let r = rng(&mut st);
                    let w = if r.is_multiple_of(2) { r % 6 } else { r % 60 };
                    body.push_str(&format!("w{w} "));
                }
                ix.add(&format!("d{i}"), &[&title, &body]);
            }
            ix.flush();

            let segs = Segments::load(&dir).unwrap();
            assert_eq!(segs.list.len(), 1, "нужен один сегмент");
            let word = |st: &mut u64| -> SpecWord {
                let field = (rng(st) % 2) as usize;
                let t = if field == 0 {
                    format!("t{}", rng(st) % 12)
                } else {
                    let r = rng(st);
                    format!("w{}", if r.is_multiple_of(2) { r % 6 } else { r % 60 })
                };
                SpecWord { given: None, fid: field, terms: vec![Term::parse(&t)] }
            };

            for round in 0..24 {
                let q = QuerySpec {
                    must: (0..1 + rng(&mut st) % 2).map(|_| word(&mut st)).collect(),
                    words: (0..rng(&mut st) % 3).map(|_| word(&mut st)).collect(),
                    must_not: (0..rng(&mut st) % 2).map(|_| word(&mut st)).collect(),
                    k: 1 + (rng(&mut st) % 10) as usize,
                    ..QuerySpec::default()
                };
                let reqs = segs.plan_with(&q, None);
                let mut all: Vec<(u32, f64)> = boolean_exhaustive(&segs.list[0], &reqs[0])
                    .into_iter()
                    .map(|h| (h.doc, h.score))
                    .collect();
                all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                all.truncate(q.k);

                let out = segs.search(&q);
                let fast: Vec<(u32, f64)> =
                    out.hits.iter().map(|x| (x.doc as u32, x.score)).collect();
                let same = fast.len() == all.len()
                    && fast
                        .iter()
                        .zip(all.iter())
                        .all(|(a, b)| a.0 == b.0 && (a.1 - b.1).abs() < 1e-12);
                if !same {
                    let show = |h: &[(u32, f64)]| {
                        h.iter().map(|(d, s)| format!("{d}:{s:.6}")).collect::<Vec<_>>().join(" ")
                    };
                    diverged.push(format!(
                        "семя {seed}, запрос {round} (k={}, must={}, should={}, not={}):\n  \
                         отсечение: {}\n  перебор:   {}",
                        q.k,
                        q.must.len(),
                        q.words.len(),
                        q.must_not.len(),
                        show(&fast),
                        show(&all)
                    ));
                }
            }
            std::fs::remove_dir_all(&dir).ok();
        }
        assert!(
            diverged.is_empty(),
            "булево отсечение разошлось с перебором в {} запросах:\n{}",
            diverged.len(),
            diverged.join("\n")
        );
    }

    /// Обход, который ведёт отбор, отсекает по потолкам блоков — и не врёт.
    ///
    /// ## Что проверяется
    ///
    /// Два утверждения, и порознь ни одно не годится.
    ///
    /// **Выдача та же.** Отсечение выбрасывает документы не глядя внутрь, по
    /// одной оценке сверху. Ошибка в этой оценке ничего не роняет и никуда не
    /// пишет: выдача просто становится короче, и заметить это можно только
    /// сверкой с полным перебором.
    ///
    /// **Работы меньше.** Мерка — `scored`, число документов, доведённых до
    /// вычисления дерева. Прежде обход отбором доводил до него **каждого**
    /// своего кандидата: выбор ведущего был не выбором между двумя одинаково
    /// умелыми обходами, а разменом отсечения на узость.
    ///
    /// Замерено на этом корпусе: **100% совпадений без отсечения против 77% с
    /// ним** в худшем из двенадцати запросов. Немного — и так и должно быть на
    /// частом слове с широким отбором: отсекать там нечем почти нигде. Порог
    /// теста поставлен по этому числу, а не по красивому: он стережёт возврат к
    /// «отсечения нет вовсе», а не обещает больше, чем измерено.
    #[test]
    fn a_filter_led_walk_prunes_by_block_ceilings() {
        fn rng(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }
        let dir = std::env::temp_dir()
            .join(format!("svindb-filter-prune-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();

        const DOCS: u32 = 20_000;
        let mut ix = Indexer::typed(
            &dir,
            vec!["тело".into(), "цена".into()],
            vec![1.0, 1.0],
            vec![crate::index::Kind::Text, crate::index::Kind::Int32],
            DOCS * 2,
        );
        let mut st = 0x5DEECE66Du64 | 1;
        for i in 0..DOCS {
            // Слова: половина словаря частая, половина редкая, длины разные —
            // при одинаковых длинах веса вырождаются в ничьи, а ничьи прячут
            // ровно такие ошибки.
            let mut body = String::new();
            for _ in 0..1 + rng(&mut st) % 20 {
                // Частые и редкие берутся из **разных** отрезков словаря:
                // `r % 4` у чётного `r` даёт только чётные номера, и половина
                // «частых» слов оказалась бы редкой. Поймано тестом, который
                // из-за этого не находил, что отсекать.
                let r = rng(&mut st);
                let w = if r.is_multiple_of(2) { (r >> 1) % 4 } else { 4 + (r >> 1) % 200 };
                body.push_str(&format!("w{w} "));
            }
            // Цена не упорядочена по номеру документа — иначе отбор выродился бы
            // в непрерывный отрезок номеров, а это не тот случай, который надо
            // проверять.
            let price = (rng(&mut st) % 10_000) as i64;
            ix.add(&format!("d{i}"), &[&body, &price.to_string()]);
        }
        ix.flush();

        let segs = Segments::load(&dir).expect("индекс");
        assert_eq!(segs.list.len(), 1, "нужен один сегмент");

        let mut worst_share = 0.0f64;
        for round in 0..12u64 {
            // Отбор на пятую часть корпуса плюс частое обязательное слово: отбор
            // ведёт (кандидатов у него меньше), а отсекать есть чем.
            let from = (round * 500) as i64;
            let q = QuerySpec {
                must: vec![SpecWord {
                    given: None,
                    fid: 0,
                    terms: vec![Term::parse(&format!("w{}", round % 4))],
                }],
                filter: vec![crate::segments::SpecRange { fid: 1, from, to: from + 2_000 }],
                k: 10,
                ..QuerySpec::default()
            };
            let reqs = segs.plan_with(&q, None);
            let mut all: Vec<(u32, f64)> = boolean_exhaustive(&segs.list[0], &reqs[0])
                .into_iter()
                .map(|h| (h.doc, h.score))
                .collect();
            all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            let matched = all.len();
            all.truncate(q.k);

            let _ = crate::stats::take();
            let out = segs.search(&q);
            let scored = crate::stats::take().scored;
            let fast: Vec<(u32, f64)> = out.hits.iter().map(|x| (x.doc as u32, x.score)).collect();
            assert_eq!(fast.len(), all.len(), "запрос {round}: длина выдачи");
            for (a, b) in fast.iter().zip(all.iter()) {
                assert_eq!(a.0, b.0, "запрос {round}: другой документ");
                assert!((a.1 - b.1).abs() < 1e-12, "запрос {round}: другой вес");
            }
            assert!(matched > 200, "запрос {round}: совпадений всего {matched}, отсекать нечего");
            worst_share = worst_share.max(scored as f64 / matched as f64);
        }
        if crate::stats::ENABLED {
            assert!(
                worst_share < 0.9,
                "до дерева доведено {:.0}% совпадений — отсечения нет",
                worst_share * 100.0
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Однословный обход в несколько потоков.
///
/// Общего состояния два. **Очередь блоков** под замком: поток берёт следующий и
/// сразу отпускает, а обрабатывает уже без него. **Порог** атомарный: без него
/// потоки не помогали бы друг другу отсекать, и выигрыш свёлся бы к числу ядер.
#[allow(clippy::too_many_arguments)]
fn single_parallel(
    ix: &Index,
    req: &Request,
    floor: f64,
    field: usize,
    lists: &[u32],
    w: f64,
    threads: usize,
) -> (Vec<Hit>, u64) {
    let theta = AtomicU64::new(floor.max(0.0).to_bits());
    let dedup = lists.len() > 1;
    // Тот же подъём потолка, что и в однопоточном пути и у курсора. Разбор — в
    // [`single`]: без него отсечение молча теряет документы на составе, чьи
    // сегменты различаются средней длиной поля.
    let lift = req.score_at(field).map_or(1.0, |s| s.lift);

    /// Что осталось обойти: текущий список и его блоки по убыванию потолка.
    struct Queue {
        at: usize,
        list: crate::index::Term,
        blocks: std::collections::BinaryHeap<(u32, u32)>,
        offsets: Vec<usize>,
    }

    let load = |q: &mut Queue, ix: &Index| {
        q.list = ix.term(lists[q.at] as usize, field);
        let n = q.list.blocks;
        ix.block_offsets(&q.list, &mut q.offsets);
        let mut order: Vec<(u32, u32)> = Vec::with_capacity(n);
        for b in 0..n {
            order.push((ix.block_max_bits(&q.list, b), b as u32));
        }
        q.blocks = std::collections::BinaryHeap::from(order);
    };

    let mut start = Queue {
        at: 0,
        list: crate::index::Term::default(),
        blocks: Default::default(),
        offsets: Vec::new(),
    };
    if lists.is_empty() {
        return (Vec::new(), 0);
    }
    load(&mut start, ix);
    let queue = std::sync::Mutex::new(start);

    let parts: Vec<(Vec<Hit>, u64)> = crate::executor::map(threads, |_| {
        let mut local: Vec<Hit> = Vec::with_capacity(req.k + 1);
        let mut visited = 0u64;
        let mut buf = [0u32; crate::bits::BLOCK];
        let mut tfs = [0u32; crate::bits::BLOCK];
        let mut work_tick = req.ticker();
        loop {
            if !req.keep_going(&mut work_tick) {
                break;
            }
            // Под замком — только выбор следующего блока.
            let job = {
                let mut q = queue.lock().expect("очередь блоков");
                loop {
                    if q.at >= lists.len() {
                        break None;
                    }
                    let cut = f64::from_bits(theta.load(Ordering::Relaxed));
                    let take = match q.blocks.peek() {
                        Some(&(bits, _)) => w * lift * f32::from_bits(bits) as f64 * SLACK >= cut,
                        None => false,
                    };
                    if take {
                        let (_, blk) = q.blocks.pop().expect("вершина есть");
                        let blk = blk as usize;
                        break Some((q.list, blk, q.offsets[blk]));
                    }
                    // Список исчерпан или отсечён целиком: дальше
                    // потолки только ниже.
                    q.at += 1;
                    if q.at < lists.len() {
                        load(&mut q, ix);
                    }
                }
            };
            let Some((list, blk, off)) = job else { break };

            stats::word_next(1);
            let n = ix.read_block_tf(&list, blk, off, &mut buf, Some(&mut tfs));
            for (i, &doc) in buf.iter().take(n).enumerate() {
                if !req.keep_going(&mut work_tick) {
                    break;
                }
                if !alive(ix, req, doc) {
                    continue;
                }
                let score = w * ix.sat_in(field, doc, tfs[i], req.score_at(field));
                visited += 1;
                stats::scored(1);
                // Тот же признак, что в последовательном пути.
                offer(&mut local, req.k, doc, score, dedup);
            }
            if local.len() >= req.k {
                theta.fetch_max(local[0].score.max(0.0).to_bits(), Ordering::Relaxed);
            }
        }
        (local, visited)
    });

    let mut heap: Vec<Hit> = Vec::with_capacity(req.k + 1);
    let mut visited = 0u64;
    for (part, seen) in parts {
        visited += seen;
        for h in part {
            offer(&mut heap, req.k, h.doc, h.score, dedup);
        }
    }
    heap.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.doc.cmp(&b.doc)));
    (heap, visited)
}

struct FilterBlocks {
    owner: std::sync::Weak<crate::container::SegmentSource>,
    columns: Vec<(usize, crate::column::BlockCache)>,
}

thread_local! {
    /// Память о блоке колонки для каждого условия отбора — на поток.
    ///
    /// ## Почему потокоместная, а не в запросе
    ///
    /// `alive` зовётся из десяти мест и получает `&Request`, общий для всех
    /// потоков обхода. Сложить память туда нельзя: она изменяемая, а запрос
    /// делится между потоками. Протягивать её десятью подписями значило бы
    /// переписать половину обхода ради одной величины.
    ///
    /// Поток обходит документы **по возрастанию**, и в блоке их сто двадцать
    /// восемь: попадание в тот же блок почти всегда.
    ///
    /// Ключ — **сегмент и поле**, а не одно поле. Смещения блока принадлежат
    /// колонке конкретного сегмента; перейди обход к следующему, и та же память
    /// отдала бы значения по чужим байтам. Молча — и это поймала не мысль, а
    ///дифференциальная проверка отбора.
    // Weak не удерживает mmap/source, но сохраняет уникальность allocation
    // identity до смены владельца кеша. Адрес самого Index может переиспользоваться.
    static FILTER_BLOCKS: std::cell::RefCell<FilterBlocks> =
        const { std::cell::RefCell::new(FilterBlocks { owner: std::sync::Weak::new(), columns: Vec::new() }) };
}

#[cfg(test)]
mod filter_cache_identity_tests {
    use super::*;

    #[test]
    fn replacing_an_index_at_the_same_address_never_reuses_old_numeric_values() {
        let root = std::env::temp_dir().join(format!("svindb-filter-owner-{}", std::process::id()));
        let build = |name: &str, value: &str| {
            let path = root.join(name);
            let mut writer = crate::Indexer::typed_stored(
                &path.to_string_lossy(),
                vec!["n".into()],
                vec![1.0],
                vec![crate::index::Kind::Int64],
                vec![false],
                100,
            );
            writer.add("a", &[value]);
            writer.flush_now();
            drop(writer);
            crate::index::load(&path.join("seg0000").to_string_lossy()).unwrap()
        };
        let mut index = Box::new(build("first", "1"));
        let request = Request {
            mode: MatchMode::Boolean,
            skipped: false,
            live: None,
            words: Vec::new(),
            k: 1,
            filter: vec![crate::segments::Range { field: 0, from: 1, to: 1 }],
            must: Vec::new(),
            must_not: Vec::new(),
            tree: BoolReq::default(),
            sort: Vec::new(),
            floor_key: KeyFloor::None,
            after: None,
            doc_base: 0,
            budget: None,
            guard: None,
            score: Vec::new(),
        };
        assert!(alive(&index, &request, 0));
        let old_source = std::sync::Arc::downgrade(&index.src);
        let address = &*index as *const Index;
        *index = build("second", "2");
        assert!(old_source.upgrade().is_none(), "кеш удерживает старые mappings");
        assert_eq!(address, &*index as *const Index);
        assert!(!alive(&index, &request, 0), "число 2 прочитано как 1 из старого кеша");
        drop(index);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Виден ли документ поиску: жив и прошёл отбор.
///
/// Условия соединяются по «и»: документ обязан удовлетворять всем. Поле без
/// колонки не удовлетворяет никакому условию.
#[inline(always)]
fn alive(ix: &Index, req: &Request, doc: u32) -> bool {
    alive_except(ix, req, doc, usize::MAX)
}

/// Привязка к immutable сегменту на весь обход. Нет TLS lookup, сравнения
/// владельца и повторной проверки схемы для каждого документа. Кандидаты
/// bucket-пути всё ещё уточняются всеми фильтрами, включая ведущий.
struct ScanFilters<'a> {
    filters: Vec<FilterEntry<'a>>,
}

struct FilterEntry<'a> {
    field: Option<&'a crate::index::Field>,
    cache: crate::column::BlockCache,
    from: i64,
    to: i64,
}

impl<'a> ScanFilters<'a> {
    fn new(ix: &'a Index, req: &Request) -> Self {
        Self {
            filters: req
                .filter
                .iter()
                .map(|r| FilterEntry {
                    field: ix.fields.get(r.field),
                    cache: crate::column::BlockCache::default(),
                    from: r.from,
                    to: r.to,
                })
                .collect(),
        }
    }

    #[inline(always)]
    fn allows(&mut self, doc: u32, guard: Option<&crate::work::Guard>) -> bool {
        self.filters.iter_mut().all(|entry| {
            entry.field.is_some_and(|f| {
                f.in_numeric_range(&mut entry.cache, doc, entry.from, entry.to, guard)
            })
        })
    }
}

#[cfg(test)]
mod prepared_filter_tests {
    use super::*;

    #[test]
    fn prepared_filters_match_legacy_alive_for_full_and_narrow_ranges() {
        let root =
            std::env::temp_dir().join(format!("svindb-prepared-filter-{}", std::process::id()));
        for segment in 0..2 {
            let path = root.join(segment.to_string());
            let mut writer = crate::Indexer::typed_stored(
                &path.to_string_lossy(),
                vec!["n".into(), "many".into()],
                vec![1.0; 2],
                vec![crate::index::Kind::Int64; 2],
                vec![false; 2],
                1000,
            );
            for doc in 0..260 {
                let value = if doc % 7 == 0 {
                    String::new()
                } else {
                    (doc as i64 - 128 + segment * 1000).to_string()
                };
                let many = if doc % 11 == 0 {
                    String::new()
                } else {
                    crate::indexer::encode_multi(&[value.clone(), "2".into(), "2".into()])
                };
                writer.add(&format!("d{doc}"), &[&value, &many]);
            }
            writer.flush_now();
            drop(writer);
            let index = crate::index::load(&path.join("seg0000").to_string_lossy()).unwrap();
            assert!(index.kill(3));
            for field in [0, 1, crate::segments::NO_FIELD] {
                for (from, to) in [(i64::MIN, i64::MAX), (-5, 3), (2, 2), (4, -4)] {
                    for frozen in [false, true] {
                        let request = Request {
                            mode: MatchMode::Boolean,
                            skipped: false,
                            live: frozen.then(|| {
                                std::sync::Arc::new(crate::live::Snapshot::all(index.docs))
                            }),
                            words: Vec::new(),
                            k: 1,
                            filter: vec![
                                crate::segments::Range { field, from, to },
                                crate::segments::Range { field: 0, from: i64::MIN, to: i64::MAX },
                            ],
                            must: Vec::new(),
                            must_not: Vec::new(),
                            tree: BoolReq::default(),
                            sort: Vec::new(),
                            floor_key: KeyFloor::None,
                            after: None,
                            doc_base: 0,
                            budget: None,
                            guard: None,
                            score: Vec::new(),
                        };
                        let mut prepared = ScanFilters::new(&index, &request);
                        let legacy: Vec<bool> = (0..index.docs as u32)
                            .map(|doc| alive(&index, &request, doc))
                            .collect();
                        let optimized: Vec<bool> = (0..index.docs as u32)
                            .map(|doc| request.is_live(&index, doc) && prepared.allows(doc, None))
                            .collect();
                        assert_eq!(optimized, legacy);
                        let mut matcher = bind(&index, &request.tree, None, &[], None);
                        let _ = stats::take();
                        for doc in 0..index.docs as u32 {
                            assert_eq!(
                                scanned_prepared(
                                    &index,
                                    &request,
                                    &mut matcher,
                                    doc,
                                    &mut prepared
                                ),
                                legacy[doc as usize]
                            );
                        }
                        #[cfg(feature = "stats")]
                        assert_eq!(stats::take().scan_visited, index.docs as u64);
                    }
                }
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// То же, но условие с номером `skip` не проверяется.
///
/// Нужно ровно одному вызывающему — обходу, который ведёт отбор. Он уже проверил
/// диапазон своего ведущего условия, потому что корзина накрывает его неточно и
/// уточнять приходится в любом случае. Спросить то же самое ещё раз значило бы
/// прочесть колонку дважды на каждом кандидате.
///
/// `usize::MAX` — не пропускать ничего.
#[inline(always)]
fn alive_except(ix: &Index, req: &Request, doc: u32, skip: usize) -> bool {
    if !req.is_live(ix, doc) {
        return false;
    }
    if req.filter.is_empty() {
        return true;
    }
    let seg = std::sync::Arc::as_ptr(&ix.src);
    FILTER_BLOCKS.with(|cell| {
        let mut held = cell.borrow_mut();
        let FilterBlocks { owner, columns: cache } = &mut *held;
        if owner.as_ptr() != seg
            || cache.len() != req.filter.len()
            || cache.iter().zip(req.filter.iter()).any(|((f, _), r)| *f != r.field)
        {
            *owner = std::sync::Arc::downgrade(&ix.src);
            cache.clear();
            cache
                .extend(req.filter.iter().map(|r| (r.field, crate::column::BlockCache::default())));
        }
        for (i, (r, (_, c))) in req.filter.iter().zip(cache.iter_mut()).enumerate() {
            if i == skip {
                continue;
            }
            let ok = ix
                .fields
                .get(r.field)
                .is_some_and(|f| f.in_numeric_range(c, doc, r.from, r.to, req.guard.as_deref()));
            if !ok {
                return false;
            }
        }
        true
    })
}

/// Обход с обязательными и запрещёнными словами, сортировкой и курсором.
///
/// MaxScore отсекает по весу: он пропускает блок, чей потолок не дотягивает до
/// порога верхушки. Обязательное слово и сортировка по полю обе этот довод
/// ломают — первое потому, что документ с высоким весом может не содержать
/// обязательного, второе потому, что вес вообще перестаёт задавать порядок.
/// Втащить их во внутренности MaxScore значило бы сделать вид, что отсечение
/// работает, когда оно неверно.
///
/// ## Кто ведёт обход
///
/// Самое **редкое** обязательное слово. Оно задаёт всех возможных кандидатов:
/// документ без него совпасть не может. Остальные обязательные подтягиваются
/// прыжком, запрещённые — тоже прыжком, необязательные добавляют вес.
///
/// Обратный порядок — вести по частому и проверять редкое — означал бы пройти
/// весь список частого слова ради сотни совпадений.
pub fn search_boolean(ix: &Index, req: &Request, floor: f64) -> (Vec<Hit>, u64) {
    if req.k == 0 || req.skipped {
        return (Vec::new(), 0);
    }
    if !req.running() {
        return (Vec::new(), 0);
    }
    // Ветвь, которая не может совпасть ни с чем, обнуляет сегмент целиком.
    let Some(mut root) = bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref())
    else {
        return (Vec::new(), 0);
    };

    // Кандидатов поставляет отдельный поток. Вести обход самим деревом значило
    // бы уметь прыгать по нему целиком; вычислять его на готовом кандидате
    // умеет любая ветвь без исключений — отсюда и разделение.
    let mut required: Vec<&QueryWord> = Vec::new();
    required_words(&req.tree, &mut required);
    let by_words: usize = if !positive_cover(&req.tree) {
        usize::MAX
    } else {
        required
            .iter()
            .map(|w| w.lists.iter().map(|&l| ix.t_df(l as usize) as usize).sum::<usize>())
            .filter(|&df| df > 0)
            .min()
            .unwrap_or_else(|| {
                let mut all: Vec<&QueryWord> = Vec::new();
                all_words(&req.tree, &mut all);
                let n: usize = all
                    .iter()
                    .map(|w| w.lists.iter().map(|&l| ix.t_df(l as usize) as usize).sum::<usize>())
                    .sum();
                if n == 0 {
                    usize::MAX
                } else {
                    n
                }
            })
    };

    // Отбор тоже может вести — см. [`filter_lead`].
    if let Some(i) = filter_lead(ix, req, by_words) {
        return by_filter(ix, req, floor, i, &mut root);
    }

    // Сортировка по полю: идти ли в порядке значения с обрывом или перебрать
    // все совпадения. Полный обход стоит примерно числа совпадений; обход в
    // порядке значения — примерно `k · документов / совпадений`, потому что
    // идти придётся, пока не наберётся `k` подходящих.
    //
    // Тот же довод, по которому выбирается ведущий между словом и отбором:
    // широкому запросу выгоден обрыв, узкому — полный перебор своих совпадений.
    if !req.sort.is_empty() && by_words != usize::MAX && ix.docs > 0 {
        // Отбор в эту оценку не входит, и вставить его как «не больше самого
        // узкого условия» **нельзя** — такая поправка не сработала бы никогда.
        //
        // Доказательство короткое. Досюда доходят только те запросы, у которых
        // `filter_lead` выше вернул `None`, а это значит, что оценка каждого
        // условия отбора **не меньше** `by_words`. Значит `min(by_words, оценка)`
        // равен `by_words`, то есть тому же, что и без поправки. Такая правка
        // выглядела бы исправлением и не меняла ни одного плана.
        //
        // Настоящая поправка требует оценивать **пересечение** теснее минимума
        // — скажем, `by_words · оценка / docs` в предположении независимости, —
        // а это замена одной непроверенной модели на другую. Без замера такое не
        // ставится: правило кампании одно на все правки.
        let matches = by_words.max(1);
        let by_order = req.k.saturating_mul(ix.docs) / matches;
        if by_order < matches {
            return in_sort_order(ix, req, floor, matches);
        }
    }
    if by_words == usize::MAX {
        // Слов нет, отбор вести не берётся — значит ведёт сортировка, если она
        // задана. Пустое дерево при этом является явным match_all: оно
        // отличается от пустого запроса тем, что сервер создаёт для него
        // булеву ветвь намеренно. Обходим живые документы по номеру, сохраняя
        // общий контракт курсора и верхушки.
        if !req.sort.is_empty() {
            return in_sort_order(ix, req, floor, ix.docs);
        }
        let mut heap: Vec<Hit> = Vec::with_capacity(req.k + 1);
        let mut visited = 0u64;
        let mut work_tick = req.ticker();
        let constant_score = req.tree.must.is_empty() && req.tree.should.is_empty();
        for doc in 0..ix.docs as u32 {
            if !req.keep_going(&mut work_tick) {
                break;
            }
            if !alive(ix, req, doc) {
                continue;
            }
            let Some(score) = root.eval(doc) else { continue };
            let keys = [0; MAX_SORT];
            if passes(req, score, &keys, 0, doc, floor) {
                offer_key(&mut heap, req.k, doc, score, keys, 0, false);
            }
            visited += 1;
            // Match-all и чистое отрицание имеют одинаковый score=0.
            // Обход идёт по возрастанию doc, поэтому после k принятых
            // документов следующие не улучшают ни score, ни tie-break.
            // Exact count живёт в отдельном collector и здесь не обрывается.
            if constant_score && heap.len() >= req.k {
                break;
            }
        }
        heap.sort_by(|a, b| worst_first(b, a));
        return (heap, visited);
    }

    // Ведущий поток: самое редкое обязательное слово, а если обязательных нет —
    // объединение всех слов ветви.
    let mut lead: Vec<Union> = Vec::new();
    let rarest = required
        .iter()
        .filter(|w| w.lists.iter().any(|&l| ix.t_df(l as usize) > 0))
        .min_by_key(|w| w.lists.iter().map(|&l| ix.t_df(l as usize) as usize).sum::<usize>());
    match rarest {
        Some(w) => lead.extend(union_of(ix, w, &req.score)),
        None => {
            let mut all: Vec<&QueryWord> = Vec::new();
            all_words(&req.tree, &mut all);
            lead.extend(all.iter().filter_map(|w| union_of(ix, w, &req.score)));
        }
    }
    if lead.is_empty() {
        return (Vec::new(), 0);
    }

    let mut heap: Vec<Hit> = Vec::with_capacity(req.k + 1);
    let mut visited = 0u64;

    // Отсечение по блокам ведущего — то, чего у булева обхода не было вовсе.
    //
    // Быстрый путь по весу перепрыгивает блоки с первого дня, а здесь каждый
    // документ ведущего слова доходил до вычисления дерева. На частом слове это
    // и есть вся разница: `must` с «не» на 25 млн сообщений просматривал три с
    // половиной миллиона документов ради верхушки из десяти.
    //
    // Оценка сверху: потолок **блока** ведущего плюс потолки **списков** всех
    // прочих ветвей. Если она не дотягивает до порога верхушки, ни один документ
    // блока в выдачу не попадёт, и блок перешагивается целиком.
    //
    // Условия, при которых это законно, и почему каждое:
    //
    // * **Ведущий один и он — обязательное слово.** Тогда его вклад вычитается
    //   из общей суммы ровно один раз. Когда обязательных нет, ведущим служит
    //   объединение всех слов ветви, и вычитать было бы нечего.
    // * **Нет сортировки по полю.** При ней порядок задаёт значение, а не вес, и
    //   порог по весу перестаёт что-либо говорить о ещё не пройденном.
    // * **Все потолки конечны.** Фраза и соединение своего пока не сообщают.
    //
    // Курсор страницы (`after`) отсечению не мешает: он отбрасывает документы с
    // весом **выше** показанного, то есть только сужает выдачу сверху, а порог
    // отсекает снизу.
    let rest = match rarest {
        Some(_) if req.sort.is_empty() && lead.len() == 1 && req.k > 0 => {
            let total = bool_ceiling(&root);
            let mine = lead[0].list_bound();
            (total.is_finite() && mine.is_finite()).then(|| (total - mine).max(0.0))
        }
        _ => None,
    };

    // Окно блочной оценки: пока документ внутри него, оценка неизменна.
    let multi_must = root.must.len() > 1;
    let mut window_end = 0u32;
    let mut window_sum = f64::INFINITY;
    let mut work_tick = req.ticker();
    loop {
        if !req.keep_going(&mut work_tick) {
            break;
        }
        let doc = lead.iter().map(|c| c.cur()).min().unwrap_or(u32::MAX);
        if doc == u32::MAX {
            break;
        }
        if let Some(rest) = rest {
            // Порог — больший из двух: своей верхушки и пришедшего снаружи.
            //
            // Пришедший снаружи годится **сразу**, ещё до того как своя верхушка
            // наберётся: `passes` и так отвергает документ легче него, значит
            // блок, чей потолок его не достаёт, не даст ни одного попадания.
            //
            // Разница здесь не косметическая. Сегментов на ста миллионах около
            // ста тридцати, и каждый набирал свою десятку с нуля: замерено
            // 1 650 762 просмотренных документа на `must` с `must_not` там, где
            // тот же запрос к одному сегменту просматривает двадцать тысяч.
            // Порог, поднятый первым же сегментом, отсекает остальные с первого
            // документа.
            let cut = if heap.len() >= req.k { heap[0].score.max(floor) } else { floor };
            // Сперва оценка в окне документа: у всех обязательных берутся
            // границы блоков, а не списков. Прежде так считался только ведущий,
            // и на пересечении частых слов отсечение не срабатывало ни разу —
            // 9.65 млн просмотренных против 895 у того же слова в одиночку.
            //
            // Прыжок делается за конец **общего** окна: это наименьший из концов
            // блоков обязательных, и внутри него ни один документ не может
            // набрать больше `sum` — у каждого обязательного слова вклад не
            // превышает границы его блока, а блок на всём окне тот же.
            // Оценка считается раз на окно, а не раз на документ.
            //
            // Первая мера стоила дороже, чем экономила: `conj_window` ставит
            // все обязательные курсоры на документ, и на запросах, где
            // отсечение не срабатывает, это чистые накладные — `must` трёх
            // частых слов вырос со 126.6 до 160.0 мс, `must` с «не» — с 6.0 до
            // 9.1. Внутри окна оценка неизменна по построению (у каждого
            // обязательного слова блок тот же), значит пересчитывать её на
            // каждом документе незачем.
            // Оконная оценка нужна только там, где обязательных слов больше
            // одного. При единственном она в точности равна прежней — потолок
            // блока ведущего плюс списочные остальных, — но считается дороже:
            // лишний `seek` и `skip_limit` по копии в дереве. Замерено:
            // `must` с «не» 6.0 -> 8.4 мс, отбор по числу 5.9 -> 8.2. Возврат к
            // прежнему пути для этого случая снимает просадку целиком.
            if cut > f64::NEG_INFINITY && multi_must && doc > window_end {
                match conj_window(&mut root, doc) {
                    Some((sum, end)) => {
                        window_sum = sum;
                        window_end = end;
                    }
                    // Оценку не вывести — больше не пробуем: причина в форме
                    // запроса (фраза, соединение), а она не меняется.
                    None => {
                        window_sum = f64::INFINITY;
                        window_end = u32::MAX;
                    }
                }
            }
            if cut > f64::NEG_INFINITY && multi_must && window_end < u32::MAX {
                {
                    let (sum, end) = (window_sum, window_end);
                    if sum * SLACK < cut && end >= doc {
                        lead[0].seek(end + 1);
                        continue;
                    }
                }
            }
            if cut > f64::NEG_INFINITY && (lead[0].block_bound() + rest) * SLACK < cut {
                // `skip_limit` отвечает `Some` лишь когда прыжок и правда
                // перешагивает остаток блока; иначе идти обычным шагом
                // дешевле, чем прыгать той же дорогой.
                if let Some(last) = lead[0].skip_limit() {
                    if last < u32::MAX {
                        lead[0].seek(last + 1);
                        continue;
                    }
                    break;
                }
            }
        }
        if alive(ix, req, doc) {
            match root.eval(doc) {
                Some(score) => {
                    visited += 1;
                    stats::scored(1);
                    let (keys, nkeys) = keys_of(ix, req, doc);
                    if passes(req, score, &keys, nkeys, doc, floor) {
                        offer_key(&mut heap, req.k, doc, score, keys, nkeys, true);
                    }
                }
                None => {
                    // Не совпало — спросить обязательные, докуда они ушли, и
                    // перенести туда ведущего целиком. Разбор — у `conj_floor`.
                    let to = root.conj_floor();
                    if to == u32::MAX {
                        break;
                    }
                    if to > doc {
                        for c in lead.iter_mut() {
                            c.seek(to);
                        }
                        continue;
                    }
                }
            }
        }
        for c in lead.iter_mut() {
            if c.cur() == doc {
                c.next();
            }
        }
    }

    heap.sort_by(|a, b| worst_first(b, a));
    (heap, visited)
}

/// Прошёл ли документ курсор страницы: строго после показанного.
///
/// Строго — то есть либо лучше по ключам, либо равен по всем и с большим
/// номером. Правило то же, что у сравнивателя верхушки, и это не совпадение:
/// курсор страницы обязан говорить на языке сортировки, иначе страницы
/// разъедутся на ничьих.
#[inline]
fn after_allows(req: &Request, score: f64, keys: &Keys, nkeys: u8, doc: u32) -> bool {
    let Some((after_keys, last)) = req.after.as_ref() else { return true };
    let doc = (doc as u64).saturating_add(req.doc_base);
    if nkeys == 0 {
        let bound = after_keys.first().map(|&b| f64::from_bits(b as u64)).unwrap_or(f64::MAX);
        return score < bound || (score == bound && doc > *last);
    }
    // Индекс адресует не один массив, а несколько параллельных, и
    // сам участвует в вычислении. `zip` здесь потребовал бы
    // одновременных заимствований, которых у этого кода нет.
    #[allow(clippy::needless_range_loop)]
    for i in 0..nkeys as usize {
        let b = after_keys.get(i).copied().unwrap_or(MISSING_KEY);
        if keys[i] != b {
            return keys[i] < b;
        }
    }
    doc > *last
}

/// Обход, который ведёт отбор, а не слово.
///
/// Здесь же оказывается и запрос **без слов** — «всё за неделю», «всё дороже
/// ста». Вес брать неоткуда, и его нет: порядок по номеру документа, а если
/// задана сортировка — по ней.
fn by_filter(
    ix: &Index,
    req: &Request,
    floor: f64,
    lead: usize,
    root: &mut BoolNode,
) -> (Vec<Hit>, u64) {
    let r = &req.filter[lead];
    let Some(field) = ix.fields.get(r.field) else {
        return (Vec::new(), 0);
    };
    let mut filter_cache = crate::column::BlockCache::default();

    let mut heap: Vec<Hit> = Vec::with_capacity(req.k + 1);
    let mut visited = 0u64;

    // Отсечение по потолкам блоков — то, чего у обхода отбором не было вовсе.
    //
    // Словесный обход перепрыгивает блоки, чей потолок не дотягивает до порога
    // верхушки; обход отбором доводил до вычисления дерева **каждого** своего
    // кандидата. То есть выбор ведущего был не выбором между двумя одинаково
    // умелыми обходами, а разменом отсечения на узость: отбор, чуть более
    // избирательный, чем слово, забирал обход и тут же терял всё, что слово
    // умело пропускать.
    //
    // Условия те же, что у словесного пути, и по тем же причинам:
    //
    // * **Нет сортировки по полю.** При ней порядок задаёт значение, а не вес.
    // * **Верхушка непуста** (`k > 0`) — иначе порогу неоткуда взяться.
    // * **Оценка выводится.** `conj_window` отвечает `None` на фразах и
    //   соединениях, и тогда отсечения просто нет, как раньше.
    let skipping = req.sort.is_empty() && req.k > 0;
    let mut window_end = 0u32;
    let mut window_sum = f64::INFINITY;
    let mut give_up = false;
    let mut work_tick = req.ticker();

    each_candidate(ix, r, req.guard.as_deref(), &mut |candidates| {
        for &doc in candidates {
            if !req.keep_going(&mut work_tick) {
                return false;
            }
            if skipping && !give_up {
                let cut = if heap.len() >= req.k { heap[0].score.max(floor) } else { floor };
                if cut > f64::NEG_INFINITY {
                    if doc > window_end {
                        match conj_window(root, doc) {
                            Some((sum, end)) => {
                                window_sum = sum;
                                window_end = end;
                            }
                            // Оценку не вывести — больше не пробуем: причина в
                            // форме запроса, а она за обход не меняется.
                            None => give_up = true,
                        }
                    }
                    // Внутри окна оценка неизменна по построению, и ни один его
                    // документ не может набрать больше `window_sum`. Значит все
                    // кандидаты до конца окна пропускаются без чтения колонки и
                    // без вычисления дерева.
                    //
                    // Конец окна обязан быть **известен**. `conj_window`
                    // оставляет `u32::MAX`, когда ни одно обязательное слово не
                    // сообщило границы блока, и принять это за «окно до конца
                    // сегмента» значило бы распространить оценку одного блока на
                    // всё оставшееся. Словесный обход стережётся тем же
                    // условием — и по той же причине.
                    if !give_up
                        && window_end < u32::MAX
                        && doc <= window_end
                        && window_sum * SLACK < cut
                    {
                        continue;
                    }
                }
            }
            // Ведущая корзина накрывает диапазон не в точности — уточняем.
            if !field.in_numeric_range(&mut filter_cache, doc, r.from, r.to, req.guard.as_deref()) {
                continue;
            }
            // Диапазон ведущего проверен строкой выше — второй раз его не
            // спрашиваем. Прежде спрашивали: `alive` обходит **все** условия
            // отбора, включая то, которое и привело сюда этого кандидата, и на
            // каждом документе выходило лишнее чтение колонки.
            if !alive_except(ix, req, doc, lead) {
                continue;
            }
            let Some(score) = root.eval(doc) else { continue };
            visited += 1;
            stats::scored(1);
            let (keys, nkeys) = keys_of(ix, req, doc);
            if passes(req, score, &keys, nkeys, doc, floor) {
                offer_key(&mut heap, req.k, doc, score, keys, nkeys, true);
            }
        }
        true
    });

    heap.sort_by(|a, b| worst_first(b, a));
    (heap, visited)
}

/// Окно кандидатов: восемь килобайт карты и не больше четверти мегабайта
/// номеров, сколько бы документов ни было в сегменте.
const WINDOW: usize = 1 << 16;

/// Докуда корзины дешевле обхода окнами.
///
/// Корзины хороши, когда диапазон узкий: они сразу называют нужные документы,
/// не трогая остальных. Когда он широкий, они называют почти весь сегмент — и
/// тогда их выигрыш пропадает, а расход остаётся: все эти номера ложатся в
/// память и сортируются целиком. Отсюда порог: выше него обход окнами делает ту
/// же работу за постоянную память.
const BY_BUCKETS: usize = WINDOW * 4;

/// Кандидаты одного условия отбора — порциями, по возрастанию номера.
///
/// ## Почему это отдельная функция, а не тело `by_filter`
///
/// Потребителей два, а умел это один. Верхушечный обход брал кандидатов из
/// корзин или окном по колонке; полный обход агрегаций не умел ничего и
/// перебирал `0..ix.docs` — то есть «сколько всего за неделю» на десяти
/// миллионах документов читало живую карту и вычисляло дерево десять миллионов
/// раз, чтобы отбор следующей же строкой отбросил девяносто девять сотых
/// просмотренного. Генератор один на оба **обязательно**: разойдись они, и один
/// и тот же отбор давал бы в счёте и в выдаче разные множества.
///
/// ## Что обещает и чего не обещает
///
/// Номера идут по возрастанию — и внутри порции, и от порции к порции. Порция
/// ограничена окном, поэтому память не растёт с размером сегмента: прежде отбор
/// собирал номера всех подошедших разом, а на пятидесяти миллионах документов
/// это шесть мегабайт карты и до двухсот мегабайт номеров на каждый сегмент и
/// на каждый запрос — запрос «всё, у чего есть цена» стоил бы столько же,
/// сколько весь индекс. Порядок при этом сохраняется, и курсоры дерева условий,
/// которые назад не ходят, границы порций не замечают.
///
/// Диапазон накрывается **неточно**, когда ведут корзины: корзина шире условия,
/// и в порцию попадают соседи. Уточнить обязан потребитель.
fn each_candidate(
    ix: &Index,
    r: &crate::segments::Range,
    guard: Option<&crate::work::Guard>,
    on: &mut dyn FnMut(&[u32]) -> bool,
) {
    let Some(field) = ix.fields.get(r.field) else { return };
    let col = field.column.as_ref();
    if col.is_none() && field.multi_column.is_none() {
        return;
    }
    let by_buckets = col
        .and_then(|c| c.buckets.as_ref())
        .filter(|b| field.multi_column.is_none() && b.span_of(r.from, r.to).2 <= BY_BUCKETS);
    let mut candidates: Vec<u32> = Vec::new();
    match by_buckets {
        Some(b) => {
            // Корзина отдаёт свои документы по возрастанию, но соседние корзины
            // между собой не упорядочены — поэтому накрытые собираются и
            // сортируются один раз.
            let (first, last, _) = b.span_of(r.from, r.to);
            let mut docs: Vec<u32> = Vec::new();
            for k in first..=last {
                if guard.is_some_and(|g| !g.check()) {
                    return;
                }
                b.docs(k, &mut docs);
                candidates.extend_from_slice(&docs);
            }
            candidates.sort_unstable();
            let _ = on(&candidates);
        }
        None => {
            let mut bits = vec![0u64; WINDOW / 64];
            let mut at = 0usize;
            while at < ix.docs {
                if guard.is_some_and(|g| !g.check()) {
                    return;
                }
                let end = (at + WINDOW).min(ix.docs);
                bits.iter_mut().for_each(|w| *w = 0);
                let mut found =
                    col.map_or(0, |c| c.select_window(r.from, r.to, at..end, 0, &mut bits));
                if let Some(multi) = &field.multi_column {
                    found += multi.select_window(r.from, r.to, at..end, &mut bits, guard);
                }
                if found > 0 {
                    candidates.clear();
                    for (w, word) in bits.iter().enumerate() {
                        let mut x = *word;
                        while x != 0 {
                            let b = x.trailing_zeros() as usize;
                            candidates.push((at + w * 64 + b) as u32);
                            x &= x - 1;
                        }
                    }
                    if !on(&candidates) {
                        return;
                    }
                }
                at = end;
            }
        }
    }
}

/// Кто из условий отбора ведёт обход — тот, кто обещает меньше кандидатов.
///
/// `ceiling` — сколько кандидатов обещает нынешний ведущий: самое редкое слово,
/// а если слов нет, то весь сегмент. Условие берётся в ведущие только если
/// обещает **строго меньше**: при равенстве менять ведущего не на что.
///
/// Довод тот же, что при выборе среди обязательных слов, — у кого кандидатов
/// меньше, — только шире: широкое слово с узким отбором иначе просматривает всё
/// и отбрасывает почти всё просмотренное.
fn filter_lead(ix: &Index, req: &Request, ceiling: usize) -> Option<usize> {
    let mut best = ceiling;
    let mut lead = None;
    for (i, r) in req.filter.iter().enumerate() {
        if let Some(field) = ix.fields.get(r.field) {
            let est = field
                .column
                .as_ref()
                .map_or(0, |c| c.estimate(r.from, r.to))
                .saturating_add(field.multi_column.as_ref().map_or(0, |m| m.entries()));
            if est < best {
                best = est;
                lead = Some(i);
            }
        }
    }
    lead
}

/// Ключи сортировки документа, уже нормированные.
#[inline]
fn keys_of(ix: &Index, req: &Request, doc: u32) -> (Keys, u8) {
    let mut keys = [0; MAX_SORT];
    for (i, &(f, desc)) in req.sort.iter().take(MAX_SORT).enumerate() {
        keys[i] = match ix
            .fields
            .get(f)
            .and_then(|field| field.sort_value(doc, desc, req.guard.as_deref()))
        {
            // Документ без значения идёт в конец при любом направлении:
            // выбросить его значило бы подменить отбор сортировкой.
            None => MISSING_KEY,
            Some(v) => norm_key(v, desc),
        };
    }
    (keys, req.sort.len().min(MAX_SORT) as u8)
}

#[inline]
fn passes(req: &Request, score: f64, keys: &Keys, nkeys: u8, doc: u32, floor: f64) -> bool {
    if nkeys == 0 {
        if score < floor {
            return false;
        }
    } else if let Some(f) = req.floor_key.get() {
        if keys[0] < f {
            return false;
        }
    }
    after_allows(req, score, keys, nkeys, doc)
}

// ---------------------------------------------------------------------------
// Дерево условий во время обхода.
//
// Дерево вычисляется **на документ**: у каждой ветви спрашивается, совпала ли
// она и с каким весом. Кандидатов при этом поставляет отдельный поток — самое
// редкое обязательное слово или отбор, — и это разделение намеренное: вести
// обход деревом значило бы уметь прыгать по нему целиком, а вычислять его на
// готовом кандидате умеет любая ветвь без исключений.
// ---------------------------------------------------------------------------

enum Node<'a> {
    Word(Union<'a>),
    Phrase(PhraseNode<'a>),
    Bool(BoolNode<'a>),
    Slots(SlotNode<'a>),
}

/// Узел соединения по элементу массива.
///
/// Порядок работы тот же, что у фразы, и по той же причине: сперва дешёвое
/// приближение курсорами, потом проверка. Слова обязаны быть в документе —
/// иначе элемента с ними нет тем более, — и прыжки курсоров отсекают почти всё
/// до того, как дело дойдёт до слотов.
struct SlotNode<'a> {
    ix: &'a Index,
    group: u32,
    /// Словесные условия: объединение форм и поле, из которого читать позиции.
    words: Vec<Union<'a>>,
    /// Диапазоны по колонкам: поле, нижняя и верхняя граница.
    ///
    /// `Exists` выражается диапазоном во всю шкалу: колонка отвечает `None` там,
    /// где значения нет, и отдельная ветвь для этого не нужна.
    ranges: Vec<(usize, i64, i64)>,
    not_words: Vec<Union<'a>>,
    not_ranges: Vec<(usize, i64, i64)>,
    /// Пересечение слотов и буфер очередного списка. Живут в узле: проверок
    /// ровно столько, сколько кандидатов, и выделять память на каждую значило
    /// бы платить за неё на каждом документе.
    acc: Vec<u32>,
    buf: Vec<u32>,
    min_count: u32,
    boost: f64,
    budget: Option<std::sync::Arc<Budget>>,
    guard: Option<std::sync::Arc<crate::work::Guard>>,
}

/// Откуда фраза берёт порядок слов.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Order {
    /// Из `pos.bin`: у всех слов фразы позиции есть.
    Stored,
    /// Из текста документа: хотя бы у одного слова позиций нет.
    Text,
}

/// Фраза в обходе: пересечение слов как приближение, порядок как проверка.
///
/// Порядок именно такой, и он важен для скорости. Пересечение слов идёт по
/// обычным курсорам, то есть с отсечением по блокам; порядок проверяется только
/// у тех документов, которые пересечение пропустило. Проверять сразу значило бы
/// платить за это у каждого документа, где есть хоть одно слово фразы.
struct PhraseNode<'a> {
    ix: &'a Index,
    subs: Vec<Cursor<'a>>,
    /// Позиции слов текущего документа. Держится здесь, чтобы не заводить
    /// вектор на каждую проверку: проверок столько же, сколько кандидатов.
    poss: Vec<Vec<u32>>,
    slop: u32,
    field: usize,
    /// Таблицы общей статистики поля фразы; `None` — считать местными.
    sc: Option<crate::index::FieldScore>,
    /// Вес фразы — сумма весов её слов, как у Lucene. Фраза из редких слов
    /// сообщает о документе больше, чем из частых, и сумма это отражает.
    weight: f64,
    order: Order,
    /// Слова фразы так, как они лежат в словаре сегмента.
    words: Vec<String>,
    /// Содержимое поля — им и проверяется порядок при [`Order::Text`].
    store: Option<&'a crate::store::Store>,
    /// Последний распакованный кусок содержимого, и только он.
    chunk: Option<(usize, Vec<u8>)>,
    buf: String,
    /// Сколько проверок по тексту ещё позволено. `None` — без предела.
    budget: Option<std::sync::Arc<Budget>>,
    guard: Option<std::sync::Arc<crate::work::Guard>>,
}

struct BoolNode<'a> {
    must: Vec<Node<'a>>,
    should: Vec<Node<'a>>,
    not: Vec<Node<'a>>,
    min: usize,
}

/// Строит ветвь. `None` — ветвь не может совпасть ни с чем в этом сегменте.
fn bind<'a>(
    ix: &'a Index,
    b: &BoolReq,
    budget: Option<&std::sync::Arc<Budget>>,
    sc: &[Option<crate::index::FieldScore>],
    guard: Option<&std::sync::Arc<crate::work::Guard>>,
) -> Option<BoolNode<'a>> {
    if guard.is_some_and(|g| !g.check()) {
        return None;
    }
    let mut must = Vec::with_capacity(b.must.len());
    for c in b.must.iter() {
        // Обязательное, которого нет в сегменте, обнуляет всю ветвь.
        must.push(bind_clause(ix, c, budget, sc, guard)?);
    }
    let should: Vec<Node> =
        b.should.iter().filter_map(|c| bind_clause(ix, c, budget, sc, guard)).collect();
    let not: Vec<Node> =
        b.must_not.iter().filter_map(|c| bind_clause(ix, c, budget, sc, guard)).collect();
    // Default minimum определяется исходным деревом, до удаления ветвей,
    // отсутствующих в сегменте. Иначе should(missing) становился match_all.
    let minimum = if b.min_should > 0 {
        b.min_should
    } else if b.must.is_empty() && !b.should.is_empty() {
        1
    } else {
        0
    };
    if should.len() < minimum {
        return None;
    }
    // Пустая ветвь — это «совпадает всё», и это законно: так выглядит запрос
    // из одного отбора. Отвергать её значило бы отвергать «всё за неделю».
    Some(BoolNode { must, should, not, min: minimum })
}

fn bind_clause<'a>(
    ix: &'a Index,
    c: &Clause,
    budget: Option<&std::sync::Arc<Budget>>,
    sc: &[Option<crate::index::FieldScore>],
    guard: Option<&std::sync::Arc<crate::work::Guard>>,
) -> Option<Node<'a>> {
    if guard.is_some_and(|g| !g.check()) {
        return None;
    }
    match c {
        Clause::Word(w) => union_of(ix, w, sc).map(Node::Word),
        Clause::Phrase(p) => {
            phrase_of(ix, p, budget, sc.get(p.field).and_then(|x| x.as_ref()), guard)
                .map(Node::Phrase)
        }
        Clause::Bool(b) => bind(ix, b, budget, sc, guard).map(Node::Bool),
        Clause::Slots(s) => slots_of(ix, s, budget, sc, guard).map(Node::Slots),
    }
}

/// Узел соединения по элементу. `None` — группы нет в сегменте либо у одного из
/// обязательных условий нет ни единого списка: элемента с ним не бывает.
fn slots_of<'a>(
    ix: &'a Index,
    s: &SlotReq,
    budget: Option<&std::sync::Arc<Budget>>,
    sc: &[Option<crate::index::FieldScore>],
    guard: Option<&std::sync::Arc<crate::work::Guard>>,
) -> Option<SlotNode<'a>> {
    if s.group as usize >= ix.slots.len() || s.all.is_empty() {
        return None;
    }
    let mut words = Vec::new();
    let mut ranges = Vec::new();
    for term in s.all.iter() {
        match term {
            SlotTerm::Word(w) => words.push(union_of(ix, w, sc)?),
            SlotTerm::Range { field, from, to } => ranges.push((*field, *from, *to)),
            // Наличие значения — диапазон во всю шкалу: колонка отвечает `None`
            // там, где значения нет, и отдельной ветви для этого не нужно.
            SlotTerm::Exists { field } => ranges.push((*field, i64::MIN, i64::MAX)),
        }
    }
    let mut not_words = Vec::new();
    let mut not_ranges = Vec::new();
    for term in s.none.iter() {
        match term {
            // Запрещённого слова в сегменте нет вовсе — запрещать нечего.
            SlotTerm::Word(w) => {
                if let Some(u) = union_of(ix, w, sc) {
                    not_words.push(u);
                }
            }
            SlotTerm::Range { field, from, to } => not_ranges.push((*field, *from, *to)),
            SlotTerm::Exists { field } => not_ranges.push((*field, i64::MIN, i64::MAX)),
        }
    }
    Some(SlotNode {
        ix,
        group: s.group,
        words,
        ranges,
        not_words,
        not_ranges,
        acc: Vec::new(),
        buf: Vec::new(),
        min_count: s.min_count.max(1),
        boost: s.boost,
        budget: budget.cloned(),
        guard: guard.cloned(),
    })
}

impl SlotNode<'_> {
    /// Слоты документа, попавшие под диапазон поля.
    fn mark(&self, field: usize, from: u32, n: u32, lo: i64, hi: i64, out: &mut Vec<u32>) {
        out.clear();
        let Some(field) = self.ix.fields.get(field) else {
            return;
        };
        let mut cache = crate::column::BlockCache::default();
        for slot in 0..n {
            if self.guard.as_ref().is_some_and(|g| !g.check()) {
                out.clear();
                return;
            }
            if field.in_numeric_range(&mut cache, from + slot, lo, hi, self.guard.as_deref()) {
                out.push(slot);
            }
        }
    }

    fn eval(&mut self, doc: u32) -> Option<f64> {
        // Бюджет спрашивается до работы: исчерпан — документ не совпал, и
        // ответ об этом скажет. Тот же порядок, что у фразы.
        if self.guard.as_ref().is_some_and(|g| !g.check()) {
            return None;
        }
        if let Some(b) = self.budget.as_ref() {
            if !b.take() {
                return None;
            }
        }
        let (from, n) = self.ix.slots[self.group as usize].range(doc);
        if n == 0 {
            return None;
        }
        // Приближение: каждое словесное условие обязано быть в документе.
        for u in self.words.iter_mut() {
            u.seek(doc);
            if u.cur() != doc {
                return None;
            }
        }
        // Начало пересечения — слоты первого условия.
        self.acc.clear();
        let mut score = 0.0f64;
        let mut started = false;
        for i in 0..self.words.len() {
            if self.guard.as_ref().is_some_and(|g| !g.check()) {
                return None;
            }
            self.words[i].positions(&mut self.buf);
            score += self.words[i].score();
            if started {
                if !intersect(&mut self.acc, &self.buf, self.guard.as_deref()) {
                    return None;
                }
            } else {
                self.acc.clear();
                self.acc.extend_from_slice(&self.buf);
                started = true;
            }
            if self.acc.is_empty() {
                return None;
            }
        }
        for i in 0..self.ranges.len() {
            let (f, lo, hi) = self.ranges[i];
            if started {
                // Диапазон проверяется **по уцелевшим слотам**, а не строит
                // своё множество: после пересечения их единицы, а значений у
                // документа может быть шестнадцать.
                let (ix, group) = (self.ix, self.group as usize);
                let field = ix.fields.get(f);
                let _ = group;
                {
                    let field = field?;
                    let mut cache = crate::column::BlockCache::default();
                    if !retain_slots(&mut self.acc, self.guard.as_deref(), |s| {
                        field.in_numeric_range(&mut cache, from + s, lo, hi, self.guard.as_deref())
                    }) {
                        return None;
                    }
                }
            } else {
                let mut buf = std::mem::take(&mut self.buf);
                self.mark(f, from, n, lo, hi, &mut buf);
                self.acc.clear();
                self.acc.extend_from_slice(&buf);
                self.buf = buf;
                started = true;
            }
            if self.acc.is_empty() {
                return None;
            }
        }
        // Запрещённые: слоты, попавшие под них, из пересечения выбывают.
        for i in 0..self.not_words.len() {
            self.not_words[i].seek(doc);
            if self.not_words[i].cur() != doc {
                continue;
            }
            self.not_words[i].positions(&mut self.buf);
            if !subtract_slots(&mut self.acc, &self.buf, self.guard.as_deref()) {
                return None;
            }
            if self.acc.is_empty() {
                return None;
            }
        }
        for i in 0..self.not_ranges.len() {
            let (f, lo, hi) = self.not_ranges[i];
            if let Some(field) = self.ix.fields.get(f) {
                let mut cache = crate::column::BlockCache::default();
                if !retain_slots(&mut self.acc, self.guard.as_deref(), |s| {
                    !field.in_numeric_range(&mut cache, from + s, lo, hi, self.guard.as_deref())
                }) {
                    return None;
                }
            }
            if self.acc.is_empty() {
                return None;
            }
        }
        if (self.acc.len() as u32) < self.min_count {
            return None;
        }
        if self.guard.as_ref().is_some_and(|g| !g.check()) {
            return None;
        }
        Some(score * self.boost)
    }
}

/// Пересечение двух возрастающих списков слотов, на месте.
fn intersect(acc: &mut Vec<u32>, other: &[u32], guard: Option<&crate::work::Guard>) -> bool {
    let mut w = 0usize;
    let mut tick = 0;
    let (mut i, mut j) = (0usize, 0usize);
    while i < acc.len() && j < other.len() {
        if guard.is_some_and(|g| !g.go(&mut tick)) {
            acc.clear();
            return false;
        }
        match acc[i].cmp(&other[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                acc[w] = acc[i];
                w += 1;
                i += 1;
                j += 1;
            }
        }
    }
    acc.truncate(w);
    true
}

fn retain_slots(
    acc: &mut Vec<u32>,
    guard: Option<&crate::work::Guard>,
    mut keep: impl FnMut(u32) -> bool,
) -> bool {
    let mut out = 0;
    let mut tick = 0;
    for at in 0..acc.len() {
        if guard.is_some_and(|g| !g.go(&mut tick)) {
            acc.clear();
            return false;
        }
        if keep(acc[at]) {
            acc[out] = acc[at];
            out += 1;
        }
    }
    acc.truncate(out);
    if guard.is_some_and(|g| !g.check()) {
        acc.clear();
        return false;
    }
    true
}

/// Оба списка возрастают. Старый retain(!contains) стоил O(n*m) и мог
/// надолго зависнуть на одном документе с большим reference array.
fn subtract_slots(acc: &mut Vec<u32>, other: &[u32], guard: Option<&crate::work::Guard>) -> bool {
    let mut cursor = 0;
    let mut tick = 0;
    retain_slots(acc, guard, |value| {
        while cursor < other.len() && other[cursor] < value {
            if guard.is_some_and(|g| !g.go(&mut tick)) {
                return false;
            }
            cursor += 1;
        }
        other.get(cursor) != Some(&value)
    })
}

/// Узел фразы. `None` — хотя бы одного слова в сегменте нет, значит и фразы.
///
/// ## Что делать, когда позиций у слова нет
///
/// Теперь отсутствие позиций — не отказ, а **другой источник порядка**. Он
/// требует одного: содержимое поля должно храниться. Если и его нет, порядок
/// слов взять действительно неоткуда, и вот тогда отказ честен.
fn phrase_of<'a>(
    ix: &'a Index,
    p: &QueryPhrase,
    budget: Option<&std::sync::Arc<Budget>>,
    sc: Option<&crate::index::FieldScore>,
    guard: Option<&std::sync::Arc<crate::work::Guard>>,
) -> Option<PhraseNode<'a>> {
    if p.words.len() < 2 {
        return None;
    }
    let fld = ix.fields.get(p.field)?;
    let mut subs = Vec::with_capacity(p.words.len());
    let mut words = Vec::with_capacity(p.words.len());
    let mut weight = 0.0f64;
    // Порядок берётся из позиций только тогда, когда они есть у **каждого**
    // слова фразы: одно слово без них делает позиционный путь неполным.
    let mut stored = fld.positioned && ix.has_positions();
    for w in p.words.iter() {
        if guard.is_some_and(|g| !g.check()) {
            return None;
        }
        let &list = w.lists.first()?;
        let t = ix.term(list as usize, w.field);
        if t.df == 0 {
            return None;
        }
        stored &= t.positioned;
        let wt = w.weight(ix, t.df);
        weight += wt;
        words.push(ix.dict.term_at(list as usize).1);
        subs.push(ix.cursor_in(list, w.field, wt, sc));
    }
    let store = fld.store.as_ref();
    if !stored && (store.is_none() || fld.kind != crate::index::Kind::Text) {
        return None;
    }
    let n = subs.len();
    Some(PhraseNode {
        ix,
        subs,
        poss: vec![Vec::new(); n],
        slop: p.slop,
        field: p.field,
        sc: sc.cloned(),
        weight: weight * p.boost,
        order: if stored { Order::Stored } else { Order::Text },
        words,
        store,
        chunk: None,
        buf: String::new(),
        // Бюджет нужен только тому, кто читает текст: позиционный путь
        // ограничен сам собой.
        budget: if stored { None } else { budget.cloned() },
        guard: guard.cloned(),
    })
}

impl<'a> PhraseNode<'a> {
    fn eval(&mut self, doc: u32) -> Option<f64> {
        if self.guard.as_ref().is_some_and(|g| !g.check()) {
            return None;
        }
        // Приближение: все слова обязаны быть в документе.
        for c in self.subs.iter_mut() {
            c.seek(doc);
            if c.cur() != doc {
                return None;
            }
        }
        // Проверка: и стоять по порядку.
        match self.order {
            Order::Stored => {
                for (i, c) in self.subs.iter().enumerate() {
                    if self.guard.as_ref().is_some_and(|g| !g.check()) {
                        return None;
                    }
                    c.positions(&mut self.poss[i]);
                }
            }
            // Позиций нет — порядок читается из текста. Не нашлось текста, а
            // должен был, — документ не совпал: врать «совпал» тем убедительнее,
            // чем незаметнее пропажа, здесь нельзя.
            Order::Text => self.order_from_text(doc)?,
        }
        let lists: Vec<&[u32]> = self.poss.iter().map(|v| v.as_slice()).collect();
        let hits = crate::phrase::count_guarded(&lists, self.slop, self.guard.as_deref())?;
        if hits == 0 {
            return None;
        }
        // Частота фразы играет ту же роль, что частота слова: три вхождения
        // «красный квадрат» говорят о документе больше одного, но не втрое
        // больше — насыщение то же самое.
        Some(self.weight * self.ix.sat_in(self.field, doc, hits, self.sc.as_ref()))
    }

    /// Позиции слов фразы, взятые из текста документа.
    ///
    /// `None` — текста нет: кусок не распаковался, номер за концом хранилища
    /// или байты не строка. Все три случая означают одно — сказать про порядок
    /// слов нечего, — и документ не совпадает.
    ///
    /// Разбор здесь тот же, каким шёл приём (`highlight::locate` стоит на
    /// `analyze::is_word_char`), поэтому номера слов выходят те самые, что
    /// лежали бы в `pos.bin`. Это не совпадение, а условие: разойдись они —
    /// фраза нашлась бы там, где её нет.
    fn order_from_text(&mut self, doc: u32) -> Option<()> {
        // Поля разбираются порознь: кусок нужен по чтению, а буферы по записи,
        // и через `self` целиком это одолжить нельзя.
        let Self { store, chunk, words, poss, buf, budget, guard, .. } = self;
        if guard.as_ref().is_some_and(|g| !g.check()) {
            return None;
        }
        // Бюджет спрашивается **до** работы: исчерпан — документ не совпал, и
        // ответ об этом скажет.
        if let Some(b) = budget.as_ref() {
            if !b.take() {
                return None;
            }
        }
        let store = (*store)?;
        let (at, first) = store.locate(doc)?;
        if chunk.as_ref().map(|(c, _)| *c) != Some(at) {
            *chunk = store.chunk_bytes(at).map(|raw| (at, raw));
        }
        let (_, raw) = chunk.as_ref()?;
        let text = crate::store::slice_at(raw, (doc - first) as usize)
            .and_then(|b| std::str::from_utf8(b).ok())?;
        crate::highlight::locate_guarded(text, words, poss, buf, guard.as_deref())?;
        Some(())
    }
}

/// Объединение по всем прочтениям слова. `None` — ни одного непустого списка.
fn union_of<'a>(
    ix: &'a Index,
    w: &QueryWord,
    sc: &[Option<crate::index::FieldScore>],
) -> Option<Union<'a>> {
    let df: u32 = w.lists.iter().map(|&l| ix.t_df(l as usize)).sum();
    if df == 0 {
        return None;
    }
    let weight = w.weight(ix, df);
    let subs: Vec<Cursor> = w
        .lists
        .iter()
        .filter(|&&l| ix.t_df(l as usize) > 0)
        .map(|&l| ix.cursor_in(l, w.field, weight, sc.get(w.field).and_then(|x| x.as_ref())))
        .collect();
    (!subs.is_empty()).then(|| Union::new(subs))
}

impl<'a> Node<'a> {
    fn matches(&mut self, doc: u32) -> bool {
        match self {
            Node::Word(word) => {
                word.seek(doc);
                word.cur() == doc
            }
            Node::Bool(boolean) => boolean.matches(doc),
            // Позиционный/source verifier остаётся общим до разделения его
            // собственного scoring: здесь нельзя ослаблять проверку фразы.
            Node::Phrase(phrase) => phrase.eval(doc).is_some(),
            Node::Slots(slots) => slots.eval(doc).is_some(),
        }
    }

    /// Совпал ли документ и с каким весом. `None` — не совпал.
    fn eval(&mut self, doc: u32) -> Option<f64> {
        match self {
            Node::Word(u) => {
                u.seek(doc);
                (u.cur() == doc).then(|| u.score())
            }
            Node::Phrase(p) => p.eval(doc),
            Node::Bool(b) => b.eval(doc),
            Node::Slots(s) => s.eval(doc),
        }
    }
}

impl<'a> BoolNode<'a> {
    fn matches(&mut self, doc: u32) -> bool {
        if self.must.iter_mut().any(|c| !c.matches(doc))
            || self.not.iter_mut().any(|c| c.matches(doc))
        {
            return false;
        }
        let need = if self.min > 0 {
            self.min
        } else if self.must.is_empty() && !self.should.is_empty() {
            1
        } else {
            0
        };
        if need == 0 {
            return true;
        }
        let mut matched = 0;
        for clause in &mut self.should {
            matched += usize::from(clause.matches(doc));
            if matched >= need {
                return true;
            }
        }
        false
    }

    /// Куда можно прыгнуть после неудачной проверки.
    ///
    /// Документ, которого нет хотя бы в одном **обязательном** списке, не
    /// совпадёт — и вместе с ним не совпадёт ничего до того места, куда этот
    /// список ушёл при поиске. Значит ведущего можно двигать не на один шаг, а
    /// сразу туда.
    ///
    /// Это обычный «прыжок по конъюнкции» (Lucene зовёт его `ConjunctionDISI`).
    /// Без него обход платил длиной **ведущего** списка, хотя ответ определяется
    /// самым коротким из обязательных: пересечение «https» и «не» на 25 млн
    /// сообщений — это два миллиона шагов ради верхушки из десяти.
    ///
    /// Запрещённые ветви сюда не входят намеренно. Совпавший запрет отклоняет
    /// **этот** документ и ничего не говорит о следующем; вернуть по нему
    /// границу значило бы выбросить годные документы между ними. Проверять это
    /// не нужно: `eval` спрашивает обязательные первыми и на неудаче выходит
    /// сразу, поэтому при отказе из-за запрета все обязательные стоят ровно на
    /// текущем документе, и граница получается равной ему — то есть прыжка нет.
    ///
    /// Ноль — «границы нет», и обход делает обычный шаг.
    fn conj_floor(&self) -> u32 {
        let mut at = 0u32;
        for c in self.must.iter() {
            match c {
                Node::Word(u) => at = at.max(u.cur()),
                Node::Bool(b) => at = at.max(b.conj_floor()),
                // У фразы и соединения курсоры внутри, и куда они ушли, отсюда
                // не видно. Молчание здесь безопасно: обход просто идёт шагом.
                Node::Phrase(_) | Node::Slots(_) => {}
            }
        }
        at
    }
}

/// Наибольший вес, который ветвь может дать документу, — любому.
///
/// `f64::INFINITY` значит «не берусь оценить». Отсечение тогда не срабатывает, и
/// обход идёт как раньше: потерянная скорость, а не потерянные документы.
/// Направление ошибки здесь несимметрично, и потому неизвестное — именно
/// бесконечность. Завышенный потолок стоит просмотра; заниженный выбрасывает
/// документ, который был бы в выдаче.
fn ceiling(n: &Node) -> f64 {
    match n {
        Node::Word(u) => u.list_bound(),
        Node::Bool(b) => bool_ceiling(b),
        // У фразы вес — сумма весов её слов, у соединения — свой множитель.
        // Оба выводимы, но не проверены, а непроверенный потолок молча теряет
        // выдачу. Пока — бесконечность.
        Node::Phrase(_) | Node::Slots(_) => f64::INFINITY,
    }
}

/// Потолок ветви: обязательные складываются, необязательные — тоже.
///
/// Необязательные складываются целиком, хотя совпасть могут не все: это
/// **верхняя** оценка, а больше от неё ничего не требуется. Запрещённые не
/// участвуют — они только убирают документы, веса не добавляя.
/// Потолок ветви **в окне текущего документа** и конец этого окна.
///
/// ## Зачем понадобился
///
/// Отсечение по блокам работало только у ведущего слова: остаток считался как
/// `bool_ceiling(root) - lead.list_bound()`, то есть у всех прочих обязательных
/// брался потолок **списка по всему корпусу**. На частом слове такой потолок
/// велик и постоянен, поэтому сумма почти никогда не опускалась ниже порога, и
/// перескок не делался ни разу.
///
/// Замерено на ста миллионах: одиночное частое слово при `k = 1` просматривает
/// **895** документов — отсечение по ведущему работает превосходно. Пересечение
/// двух тех же слов — **9 652 963**. Разница в четыре порядка, и вся она здесь.
///
/// ## Что считается
///
/// У обязательных слов — граница **блока**, в который попал документ: она
/// говорит, чего стоит лучший документ этого блока, а не лучший во всём
/// корпусе. У необязательных остаётся списочная: ставить их на документ
/// незачем — они могут не совпасть вовсе, и их вклад мы обязаны считать сверху.
///
/// Конец окна — наименьший из концов блоков: дальше него хоть у кого-то из
/// обязательных начинается новый блок с другой границей, и оценка перестаёт
/// быть верной.
///
/// ## Когда отказывает
///
/// `None` означает «оценку не вывести, отсекать нельзя». Так отвечают фраза и
/// соединение: их вес выводим, но не проверен, а непроверенный потолок **молча
/// теряет выдачу**. Та же причина, по которой [`ceiling`] отдаёт для них
/// бесконечность.
fn conj_window(b: &mut BoolNode<'_>, doc: u32) -> Option<(f64, u32)> {
    let mut sum = 0.0f64;
    let mut end = u32::MAX;
    for n in b.must.iter_mut() {
        match n {
            Node::Word(u) => {
                // Ставим на документ: повторное позиционирование ничего не
                // стоит, а `eval` ниже поставил бы туда же.
                u.seek(doc);
                let bound = u.block_bound();
                if !bound.is_finite() {
                    return None;
                }
                sum += bound;
                if let Some(last) = u.skip_limit() {
                    end = end.min(last);
                }
            }
            Node::Bool(inner) => {
                let (s, e) = conj_window(inner, doc)?;
                sum += s;
                end = end.min(e);
            }
            Node::Phrase(_) | Node::Slots(_) => return None,
        }
    }
    for n in b.should.iter() {
        let c = ceiling(n);
        if !c.is_finite() {
            return None;
        }
        sum += c;
    }
    Some((sum, end))
}

fn bool_ceiling(b: &BoolNode) -> f64 {
    b.must.iter().chain(b.should.iter()).map(ceiling).sum()
}

impl<'a> BoolNode<'a> {
    fn eval(&mut self, doc: u32) -> Option<f64> {
        let mut score = 0.0f64;
        for c in self.must.iter_mut() {
            score += c.eval(doc)?;
        }
        for c in self.not.iter_mut() {
            if c.eval(doc).is_some() {
                return None;
            }
        }
        let mut hits = 0usize;
        for c in self.should.iter_mut() {
            if let Some(s) = c.eval(doc) {
                score += s;
                hits += 1;
            }
        }
        // Порог совпадений: заданный явно, иначе обычное правило — при
        // отсутствии обязательных хотя бы одно необязательное.
        let need = if self.min > 0 {
            self.min
        } else if self.must.is_empty() && !self.should.is_empty() {
            1
        } else {
            0
        };
        (hits >= need).then_some(score)
    }
}

/// Слова, без которых документ совпасть не может, с их размером списка.
///
/// Это кандидаты в ведущие: обход можно вести только по тому, что обязано
/// присутствовать. Спуск идёт через `must` и через `should` с порогом, равным
/// числу ветвей, — там «хотя бы столько» вырождается в «все».
fn positive_cover(b: &BoolReq) -> bool {
    fn covered(clause: &Clause) -> bool {
        match clause {
            Clause::Word(_) | Clause::Phrase(_) => true,
            Clause::Bool(boolean) => positive_cover(boolean),
            Clause::Slots(slots) => slots.all.iter().any(|term| matches!(term, SlotTerm::Word(_))),
        }
    }
    if b.must.iter().any(covered) {
        return true;
    }
    let need = if b.min_should > 0 {
        b.min_should
    } else if b.must.is_empty() && !b.should.is_empty() {
        1
    } else {
        0
    };
    need > b.should.iter().filter(|clause| !covered(clause)).count()
}

fn required_words<'a>(b: &'a BoolReq, out: &mut Vec<&'a QueryWord>) {
    for c in b.must.iter() {
        match c {
            Clause::Word(w) => out.push(w),
            // Фраза требует **все** свои слова, значит каждое из них
            // обязательно, и ведущим станет самое редкое.
            Clause::Phrase(p) => out.extend(p.words.iter()),
            Clause::Bool(inner) => required_words(inner, out),
            // Слово соединения обязательно так же, как слово фразы: документа
            // без него не бывает, значит и элемента тоже.
            Clause::Slots(s) => out.extend(s.all.iter().filter_map(|x| match x {
                SlotTerm::Word(w) => Some(w),
                _ => None,
            })),
        }
    }
    if b.min_should > 0 && b.min_should == b.should.len() {
        for c in b.should.iter() {
            match c {
                Clause::Word(w) => out.push(w),
                Clause::Phrase(p) => out.extend(p.words.iter()),
                Clause::Bool(inner) => required_words(inner, out),
                Clause::Slots(s) => out.extend(s.all.iter().filter_map(|x| match x {
                    SlotTerm::Word(w) => Some(w),
                    _ => None,
                })),
            }
        }
    }
}

/// Все слова-листья ветви — из них берутся кандидаты, когда обязательных нет.
fn all_words<'a>(b: &'a BoolReq, out: &mut Vec<&'a QueryWord>) {
    for c in b.must.iter().chain(b.should.iter()) {
        match c {
            Clause::Word(w) => out.push(w),
            // Слова фразы идут наравне с обычными: ведущим станет самое редкое
            // из них, а это ровно то, что нужно — фраза требует их все.
            Clause::Phrase(p) => out.extend(p.words.iter()),
            Clause::Bool(inner) => all_words(inner, out),
            Clause::Slots(s) => out.extend(s.all.iter().filter_map(|x| match x {
                SlotTerm::Word(w) => Some(w),
                _ => None,
            })),
        }
    }
}

/// Обход в порядке сортировки с обрывом на k-м документе.
///
/// Сортировка по значению поля отменяет отсечение по весу: порог верхушки
/// перестаёт что-либо говорить о блоках. Взамен появляется другой порог — по
/// самому значению, — и он работает, если идти в порядке значения.
///
/// Ничего нового для этого не хранится: у поля с корзинами порядок задают
/// корзины, у поля без корзин — границы блоков колонки. Второе не случайность:
/// корзины строятся ровно там, где границы блоков бесполезны, и наоборот.
fn in_sort_order(ix: &Index, req: &Request, floor: f64, matches: usize) -> (Vec<Hit>, u64) {
    let (field, desc) = req.sort[0];
    if ix.fields.get(field).is_none_or(|f| f.multi_column.is_some() || f.column.is_none()) {
        let Some(mut root) =
            bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref())
        else {
            return (Vec::new(), 0);
        };
        let mut heap = Vec::with_capacity(req.k + 1);
        let mut visited = 0;
        let mut tick = req.ticker();
        for doc in 0..ix.docs as u32 {
            if !req.keep_going(&mut tick) {
                break;
            }
            if !alive(ix, req, doc) {
                continue;
            }
            let Some(score) = root.eval(doc) else { continue };
            visited += 1;
            let (keys, nkeys) = keys_of(ix, req, doc);
            if passes(req, score, &keys, nkeys, doc, floor) {
                offer_key(&mut heap, req.k, doc, score, keys, nkeys, false);
            }
        }
        heap.sort_by(|a, b| worst_first(b, a));
        return (heap, visited);
    }
    let Some(col) = ix.fields.get(field).and_then(|f| f.column.as_ref()) else {
        return (Vec::new(), 0);
    };

    let mut heap: Vec<Hit> = Vec::with_capacity(req.k + 1);
    let mut visited = 0u64;
    let mut walk = col.in_value_order(desc);
    let mut docs: Vec<u32> = Vec::new();

    // Куски берутся пачками, и пачка растёт вдвое, пока верхушка не наберётся.
    let need = req.k.saturating_mul(ix.docs) / matches.max(1);
    let per_chunk = (ix.docs / walk.chunks().max(1)).max(1);
    let mut batch = (need / per_chunk).clamp(1, 1 << 12);
    let mut chunk: Vec<u32> = Vec::new();
    let mut work_tick = req.ticker();
    loop {
        if !req.keep_going(&mut work_tick) {
            break;
        }
        docs.clear();
        let mut taken = 0usize;
        while taken < batch && walk.next(&mut chunk) {
            if !req.keep_going(&mut work_tick) {
                break;
            }
            docs.extend_from_slice(&chunk);
            taken += 1;
        }
        if docs.is_empty() {
            break;
        }
        batch = (batch * 2).min(1 << 14);
        // Внутри пачки — по возрастанию номера: этого курсоры требуют.
        docs.sort_unstable();
        let Some(mut node) =
            bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref())
        else {
            break;
        };
        for &doc in docs.iter() {
            if !req.keep_going(&mut work_tick) {
                break;
            }
            if !alive(ix, req, doc) {
                continue;
            }
            let Some(score) = node.eval(doc) else { continue };
            visited += 1;
            stats::scored(1);
            let (keys, nkeys) = keys_of(ix, req, doc);
            if passes(req, score, &keys, nkeys, doc, floor) {
                offer_key(&mut heap, req.k, doc, score, keys, nkeys, true);
            }
        }
        // Обрыв: лучшее из возможного оставшегося строго хуже того, что уже
        // не пройдёт. Порогов два, и оба обязательны.
        //
        // Первый — k-й собранный, когда верхушка полна.
        let cut = match (heap.len() >= req.k, req.floor_key.get()) {
            (true, Some(f)) => Some(heap[0].keys[0].max(f)),
            (true, None) => Some(heap[0].keys[0]),
            (false, f) => f,
        };
        match walk.bound() {
            None => break,
            Some(b) => {
                if let Some(t) = cut {
                    if (b as SortKey) < t {
                        break;
                    }
                }
            }
        }
    }

    // ValueWalk перечисляет только присутствующие значения. Если верхушка
    // ещё не заполнена (либо её граница сама missing), обязаны рассмотреть
    // документы без первого ключа: сортировка не является exists-фильтром.
    if heap.len() < req.k || heap.first().is_some_and(|h| h.keys[0] == MISSING_KEY) {
        if let Some(mut node) =
            bind(ix, &req.tree, req.budget.as_ref(), &req.score, req.guard.as_ref())
        {
            for doc in 0..ix.docs as u32 {
                if !req.keep_going(&mut work_tick) {
                    break;
                }
                if col.has(doc) || !alive(ix, req, doc) {
                    continue;
                }
                let Some(score) = node.eval(doc) else { continue };
                visited += 1;
                let (keys, nkeys) = keys_of(ix, req, doc);
                if passes(req, score, &keys, nkeys, doc, floor) {
                    offer_key(&mut heap, req.k, doc, score, keys, nkeys, false);
                }
            }
        }
    }
    heap.sort_by(|a, b| worst_first(b, a));
    (heap, visited)
}

#[cfg(test)]
mod multiform_tests {
    use crate::index;

    /// Документ, совпавший несколькими формами слова, получает **лучший** вес,
    /// а не тот, что подвернулся первым.
    ///
    /// Сверка идёт с обычным объединяющим курсором: он про формы знает и
    /// считает правильно по построению. Расхождение двух путей на одном и том
    /// же индексе и есть дефект.
    #[test]
    fn a_document_matching_several_forms_keeps_the_best_score() {
        let dir = std::env::temp_dir().join("svindb-multiform");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();

        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 100_000);
        // «домик» встречается в d0 один раз, «домовой» — трижды. Обе формы
        // раскрываются маской «дом*», и вес обязан взяться по лучшей.
        ix.add("d0", &["домик домовой домовой домовой хвост"]);
        ix.add("d1", &["домик домик домик домик прочее"]);
        for i in 2..60 {
            ix.add(&format!("f{i}"), &[&format!("прочее слово{i}")]);
        }
        ix.flush();

        let seg = format!("{d}/seg0000");
        let loaded = index::load(&seg).unwrap();
        let lists: Vec<u32> =
            ["домик", "домовой"].iter().filter_map(|t| loaded.dict.lookup(0, t)).collect();
        assert_eq!(lists.len(), 2, "обе формы обязаны быть в словаре");

        // Быстрый путь: одно слово, две формы.
        let plain = |mode, words, tree| crate::search::Request {
            mode,
            skipped: false,
            live: None,
            words,
            k: 10,
            filter: Vec::new(),
            must: Vec::new(),
            must_not: Vec::new(),
            tree,
            sort: Vec::new(),
            floor_key: crate::search::KeyFloor::None,
            after: None,
            doc_base: 0,
            budget: None,
            guard: None,
            score: Vec::new(),
        };
        let fast = plain(
            crate::search::MatchMode::AnyTerm,
            vec![crate::search::QueryWord::new(0, lists.clone())],
            crate::search::BoolReq::default(),
        );
        let (hits, _) = crate::search::search(&loaded, &fast, 0.0);

        // Эталон: то же слово, но через дерево — там объединяющий курсор.
        let tree = plain(
            crate::search::MatchMode::Boolean,
            Vec::new(),
            crate::search::BoolReq {
                must: Vec::new(),
                should: vec![crate::search::Clause::Word(crate::search::QueryWord::new(
                    0,
                    lists.clone(),
                ))],
                must_not: Vec::new(),
                min_should: 0,
            },
        );
        let (want, _) = crate::search::search_boolean(&loaded, &tree, 0.0);

        let got: Vec<(u32, f64)> = hits.iter().map(|h| (h.doc, h.score)).collect();
        let exp: Vec<(u32, f64)> = want.iter().map(|h| (h.doc, h.score)).collect();
        assert_eq!(got.len(), exp.len(), "быстрый путь нашёл другое число документов");
        for (a, b) in got.iter().zip(exp.iter()) {
            assert_eq!(a.0, b.0, "порядок разошёлся: {got:?} против {exp:?}");
            assert!(
                (a.1 - b.1).abs() < 1e-9,
                "вес документа {} разошёлся: {} против {}",
                a.0,
                a.1,
                b.1
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
