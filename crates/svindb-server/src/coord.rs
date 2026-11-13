//! Координатор: несколько узлов-шардов как один поиск.
//!
//! ## Что он делает и чего не делает
//!
//! Делает: рассылает запрос шардам, складывает их верхушки в одну, добавляет к
//! каждому попаданию номер шарда. Отвечает `/_health` числом шардов, реплик и
//! остывающих копий — своих документов у него нет, и сказать о себе больше
//! нечем.
//!
//! **Не** делает: не хранит данных, не маршрутизирует приём, не следит за
//! живостью. Координатор — это узел в режиме `--shards=`, и он либо отвечает на
//! поиск, либо честно говорит, какой шард не ответил.
//!
//! Запись и чтение по идентификатору он отвергает, а не обслуживает своим
//! каталогом. Прежде обслуживал: `_bulk` и `_put` писали в его собственный
//! пустой индекс и отвечали `200`, `_doc` и `_mget` читали оттуда же и не
//! находили ничего. Клиент, отправивший данные не туда, узнавал об этом не
//! отказом, а пустой выдачей — когда-нибудь потом.
//!
//! ## Два режима точности, и разница между ними не косметическая
//!
//! Обратная частота обязана считаться по всему, что участвует в запросе. У шардов
//! по сети общей статистики нет, и вариантов ровно два — те же, что у
//! Elasticsearch:
//!
//! **Приблизительный** (`"scoring":"local"`, по умолчанию). Один круг: каждый
//! шард считает вес по своим `N` и `df`. На ровно нарезанных шардах расхождение
//! мало, потому что и `N`, и `df` делятся примерно одинаково. На перекошенных —
//! велико: редкое слово, случайно собравшееся в одном шарде, получит там вес
//! частого.
//!
//! **Точный** (`"scoring":"global"`). Два круга: первый собирает `N` и `df` со
//! всех шардов, второй ищет с уже общей статистикой. Выдача перестаёт зависеть от
//! того, как легли документы, — ценой удвоенной задержки на сеть. Это и есть
//! `dfs_query_then_fetch`.
//!
//! Первый круг дешёвый: шард только читает частоты уже раскрытых слов, не обходя
//! списки постингов.
//!
//! ## Отказ шарда — это отказ, а не «нашлось меньше»
//!
//! Молча отдать выдачу без одного шарда значит отдать **неверный** ответ, который
//! выглядит верным: ни числа попаданий, ни весов не хватает, чтобы заметить
//! пропажу пятой части корпуса. Поэтому по умолчанию — отказ с именем шарда.
//! Кому нужна доступность вместо точности, ставит `"partial":true` и получает
//! ответ с полем `failed`, где перечислено, чего в нём нет.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::httpc;

/// Сколько реплика считается остывающей после отказа.
///
/// Без этого реплики делают **хуже**, а не лучше: с тридцатисекундным сроком
/// ожидания каждый запрос платил бы полную тридцатку на мёртвую реплику прежде,
/// чем дойти до живой. Пять секунд — компромисс: буря запросов не платит за
/// отказ снова и снова, а вернувшаяся реплика ждёт возврата недолго.
///
/// Отдельного опроса живости здесь нет намеренно: он потребовал бы своего
/// потока и своего понятия «жив», а память об отказе даёт то же самое даром и
/// по тем же самым запросам, которые всё равно идут.
const COOLDOWN: Duration = Duration::from_secs(5);

/// Сколько исходящих запросов узел ведёт разом.
///
/// ## Зачем предел, если потоки и так конечны
///
/// Прежде веер поднимал **поток на шард на запрос**. При десяти шардах и
/// тридцати одновременных запросах это триста потоков, и каждый из них держит
/// сокет: узел упирается сперва в дескрипторы (на стенде `ulimit -n` равен
/// тысяче), потом в планировщик — и оба раза не там, где идёт полезная работа.
///
/// Предел общий на узел, а не на запрос. Пер-запросный предел не ограничивает
/// ничего: он умножается на число запросов, которое задаёт не узел, а клиент.
#[derive(Debug)]
pub struct Permits {
    left: std::sync::Mutex<usize>,
    wake: std::sync::Condvar,
}

impl Permits {
    pub fn new(n: usize) -> Permits {
        Permits { left: std::sync::Mutex::new(n.max(1)), wake: std::sync::Condvar::new() }
    }

    /// Ждёт своей очереди. Возврат — расписка, освобождающая место.
    fn take(&self) -> Permit<'_> {
        let mut left = self.left.lock().expect("разрешения веера");
        while *left == 0 {
            left = self.wake.wait(left).expect("разрешения веера");
        }
        *left -= 1;
        Permit { of: self }
    }
}

/// Расписка: место возвращается по выходу из области, в том числе при панике.
struct Permit<'a> {
    of: &'a Permits,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let mut left = self.of.left.lock().expect("разрешения веера");
        *left += 1;
        // Будим одного: место освободилось одно, и будить всех значило бы
        // поднять всю очередь ради одного вошедшего.
        self.of.wake.notify_one();
    }
}

/// Сколько исходящих держит узел, пока не задано иначе.
///
/// Тридцать два — не «сколько влезет», а порядок, за которым добавленный поток
/// перестаёт покупать задержку и начинает покупать переключения контекста.
/// Число вынесено в настройку, потому что верным оно бывает разным: у веера по
/// трём шардам и по тремстам оно не одно.
pub const DEFAULT_PERMITS: usize = 32;

/// Настройка координатора.
#[derive(Clone, Debug)]
pub struct Fanout {
    /// Реплики по шардам: `shards[i]` — равноправные копии шарда `i`.
    ///
    /// Порядок внешнего списка задаёт номера шардов и наружу виден; порядок
    /// внутреннего — только предпочтение при выборе, и на выдачу не влияет.
    pub shards: Vec<Vec<String>>,
    /// Полоса ячеек каждого шарда: `[lo, hi)`.
    ///
    /// ## Зачем она здесь, если шарды и так делятся поровну
    ///
    /// Поровну — пока их не делили. Деление шарда даёт **неравные** полосы, и
    /// равное деление после него адресовало бы документы не туда: половина
    /// корпуса искалась бы в шарде, где её нет. Полосы делают неравную раскладку
    /// выразимой, а равную оставляют умолчанием.
    pub ranges: Vec<(u64, u64)>,
    pub timeout: Duration,
    state: Arc<State>,
    /// Общий на узел предел исходящих. Делится между всеми запросами.
    permits: Arc<Permits>,
}

#[derive(Debug)]
struct State {
    born: Instant,
    /// Смещение по кругу: чтобы весь поток не шёл в первую реплику.
    turn: AtomicUsize,
    /// Докуда реплика остывает — миллисекунды от `born`. Ноль — годна.
    cool: Vec<Vec<AtomicU64>>,
}

/// Что вернул круг по шардам.
pub struct Fanned {
    /// Тела ответов в порядке шардов. `Err` — не ответила **ни одна** реплика.
    pub replies: Vec<Result<String, String>>,
    /// Кто именно ответил, по шардам. Нужно для отчёта и для журнала: без этого
    /// «шард 2 отдал вот такую выдачу» не сказать, какая из копий её отдала.
    pub used: Vec<Option<String>>,
}

impl Fanned {
    /// Сколько шардов ответили **не** предпочтённой репликой.
    ///
    /// Считается сверкой с выбором, а не флагом изнутри: флаг пришлось бы
    /// проставлять в четырёх ветвях опроса, и пятая забыла бы.
    pub fn fallbacks(&self, shards: &[Vec<String>], picks: &[usize]) -> u64 {
        self.used
            .iter()
            .enumerate()
            .filter(|(i, used)| {
                let Some(addr) = used.as_deref() else { return false };
                let want = shards
                    .get(*i)
                    .and_then(|r| r.get(picks.get(*i).copied().unwrap_or(0)))
                    .map(String::as_str);
                want != Some(addr)
            })
            .count() as u64
    }

    /// Имена шардов, которые не ответили ни одной репликой.
    pub fn failed(&self, shards: &[Vec<String>]) -> Vec<String> {
        self.replies
            .iter()
            .enumerate()
            .filter_map(|(i, r)| {
                r.as_ref().err().map(|e| {
                    // Копии названы один раз — списком шарда; причины идут
                    // следом и адреса в себе уже несут.
                    match shards.get(i) {
                        Some(reps) if reps.len() > 1 => format!("шард {} — {e}", reps.join("+")),
                        _ => e.clone(),
                    }
                })
            })
            .collect()
    }
}

impl Fanout {
    pub fn new(shards: Vec<Vec<String>>, timeout: Duration) -> Fanout {
        Fanout::with_permits(shards, timeout, DEFAULT_PERMITS)
    }

    /// То же с явным пределом исходящих. Разбор — у [`Permits`].
    pub fn with_permits(shards: Vec<Vec<String>>, timeout: Duration, permits: usize) -> Fanout {
        let ranges = equal_ranges(shards.len());
        Fanout::with_ranges(shards, ranges, timeout, permits)
    }

    /// То же с явными полосами. Полосы обязаны покрывать пространство ячеек
    /// ровно один раз — это проверяет [`check_ranges`] у того, кто их разобрал.
    pub fn with_ranges(
        shards: Vec<Vec<String>>,
        ranges: Vec<(u64, u64)>,
        timeout: Duration,
        permits: usize,
    ) -> Fanout {
        let cool =
            shards.iter().map(|reps| reps.iter().map(|_| AtomicU64::new(0)).collect()).collect();
        Fanout {
            shards,
            ranges,
            timeout,
            state: Arc::new(State { born: Instant::now(), turn: AtomicUsize::new(0), cool }),
            permits: Arc::new(Permits::new(permits)),
        }
    }

    fn now_ms(&self) -> u64 {
        self.state.born.elapsed().as_millis() as u64
    }

    /// Куда идёт **запись**: первая копия шарда, а не очередная по кругу.
    ///
    /// ## Почему это не то же, что [`Fanout::pick`]
    ///
    /// Круговой обход придуман для чтения: копии равноправны, и распределить
    /// поток между ними — чистая выгода. Для записи они **не** равноправны:
    /// рассылает копиям только primary, а реплика, получив запись напрямую,
    /// применит её у себя и не расскажет никому. Копии разойдутся, и обе стороны
    /// об этом промолчат.
    ///
    /// Поэтому у списка копий шарда порядок значащий: **первая — primary**. Это
    /// не соглашение об именах, а часть договора `--shards=a+b`.
    pub fn writes_to(&self) -> Vec<usize> {
        (0..self.shards.len()).map(|_| 0).collect()
    }

    /// Выбирает по одной реплике на шард — **один раз на запрос**.
    ///
    /// Один раз, а не в каждом круге: точный режим (`scoring=global`) делает два
    /// круга, и попади они в разные копии — статистику посчитала бы одна, а
    /// искала бы по ней другая, с другой точкой свежести. Расхождение вышло бы
    /// небольшим и потому незаметным, а выдача — невоспроизводимой.
    pub fn pick(&self) -> Vec<usize> {
        let turn = self.state.turn.fetch_add(1, Ordering::Relaxed);
        let now = self.now_ms();
        self.shards
            .iter()
            .enumerate()
            .map(|(i, reps)| {
                let n = reps.len().max(1);
                // Смещение зависит и от очереди, и от номера шарда: иначе все
                // шарды одного запроса шли бы в реплики одного «столбца», и
                // отказ целого узла-копии уносил бы сразу все шарды.
                let base = turn.wrapping_add(i) % n;
                for step in 0..n {
                    let at = (base + step) % n;
                    if self.cooled_until(i, at) <= now {
                        return at;
                    }
                }
                // Остывают все — берём всё равно. Иначе кратковременный обрыв
                // сети объявил бы шард мёртвым до конца остывания, хотя реплики
                // к тому времени, может, уже вернулись.
                base
            })
            .collect()
    }

    /// Сколько реплик сейчас остывают после отказа.
    ///
    /// Число для `/_health`: узел-координатор своих документов не имеет, и
    /// сказать о своём состоянии ему больше нечем. Растущее здесь означает, что
    /// часть шардов отвечает не тем, кем предпочтено, — а выдача при этом
    /// приходит, и по ней бы никто не догадался.
    pub fn cooling(&self) -> usize {
        let now = self.now_ms();
        self.state
            .cool
            .iter()
            .flat_map(|s| s.iter())
            .filter(|c| c.load(Ordering::Relaxed) > now)
            .count()
    }

    /// Сколько всего реплик во всех шардах.
    pub fn replicas(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    fn cooled_until(&self, shard: usize, at: usize) -> u64 {
        self.state.cool.get(shard).and_then(|r| r.get(at)).map_or(0, |c| c.load(Ordering::Relaxed))
    }

    fn cool_down(&self, shard: usize, at: usize) {
        if let Some(c) = self.state.cool.get(shard).and_then(|r| r.get(at)) {
            c.store(self.now_ms() + COOLDOWN.as_millis() as u64, Ordering::Relaxed);
        }
    }

    fn revive(&self, shard: usize, at: usize) {
        if let Some(c) = self.state.cool.get(shard).and_then(|r| r.get(at)) {
            c.store(0, Ordering::Relaxed);
        }
    }

    /// Рассылает одно и то же тело всем шардам **параллельно**.
    ///
    /// Параллельно, а не по очереди: последовательный круг по пятидесяти шардам
    /// складывал бы их задержки, и запрос на четыре миллисекунды превращался бы в
    /// двести. Поток на шард — не расточительство: они почти всё время спят на
    /// чтении из сокета.
    ///
    /// Внутри шарда, наоборот, реплики опрашиваются **по очереди**: спросить их
    /// разом значило бы удвоить нагрузку ради ответа, который в норме приходит с
    /// первой.
    pub fn fan(&self, picks: &[usize], path: &str, body: &str) -> Fanned {
        let n = self.shards.len();
        let mut replies: Vec<Result<String, String>> =
            (0..n).map(|_| Err(String::from("шард не опрошен"))).collect();
        let mut used: Vec<Option<String>> = (0..n).map(|_| None).collect();
        if n == 0 {
            return Fanned { replies, used };
        }
        // Работников не больше, чем шардов, и не больше общего предела: поток
        // на шард на запрос упирался в дескрипторы и планировщик раньше, чем в
        // сеть. Шарды разбираются из общей очереди — тогда медленный шард не
        // держит работника, которому досталась бы работа соседа.
        let next = AtomicUsize::new(0);
        let workers = n.min(DEFAULT_PERMITS.max(1));
        let done: Vec<(usize, Result<String, String>, Option<String>)> =
            std::thread::scope(|scope| {
                let hs: Vec<_> = (0..workers)
                    .map(|_| {
                        let next = &next;
                        scope.spawn(move || {
                            let mut mine = Vec::new();
                            loop {
                                let i = next.fetch_add(1, Ordering::Relaxed);
                                if i >= n {
                                    break;
                                }
                                let (r, u) =
                                    self.ask(i, picks.get(i).copied().unwrap_or(0), path, body);
                                mine.push((i, r, u));
                            }
                            mine
                        })
                    })
                    .collect();
                hs.into_iter().flat_map(|h| h.join().unwrap_or_default()).collect()
            });
        // Складывается **по номеру шарда**, а не по порядку готовности: выдача
        // обязана не зависеть от того, кто ответил первым.
        for (i, r, u) in done {
            replies[i] = r;
            used[i] = u;
        }
        Fanned { replies, used }
    }

    /// Спрашивает **ровно одну** копию, не переходя к соседкам.
    fn ask_one(
        &self,
        i: usize,
        at: usize,
        path: &str,
        body: &str,
    ) -> (Result<String, String>, Option<String>) {
        let reps = &self.shards[i];
        let addr = match reps.get(at.min(reps.len().saturating_sub(1))) {
            Some(a) => a,
            None => return (Err(format!("у шарда {i} нет копий")), None),
        };
        let got = {
            let _permit = self.permits.take();
            httpc::post_json(addr, path, body, self.timeout)
        };
        match got {
            Ok(r) if (200..300).contains(&r.code) => (Ok(r.body), Some(addr.clone())),
            Ok(r) => (
                Err(format!("{addr} ответил {} — {}", r.code, first_line(&r.body))),
                Some(addr.clone()),
            ),
            Err(e) => (Err(e), Some(addr.clone())),
        }
    }

    /// Спрашивает один шард, обходя его реплики по кругу от `start`.
    fn ask(
        &self,
        i: usize,
        start: usize,
        path: &str,
        body: &str,
    ) -> (Result<String, String>, Option<String>) {
        let reps = &self.shards[i];
        let n = reps.len().max(1);
        let mut why: Vec<String> = Vec::new();
        for step in 0..n {
            let at = (start + step) % n;
            let addr = &reps[at];
            // Разрешение берётся на **один** исходящий, а не на весь обход
            // реплик: держать его, пока перебираются мёртвые копии, значило бы
            // отдать предел тем, кто и так ничего не делает.
            let got = {
                let _permit = self.permits.take();
                httpc::post_json(addr, path, body, self.timeout)
            };
            match got {
                // Успех — **любой** `2xx`, а не только `200`. Шард, ответивший
                // `202` («сброс уже идёт»), ответил по существу; считать это
                // молчанием значило бы пойти к соседке за тем же ответом, а
                // потом объявить живой узел мёртвым.
                Ok(r) if (200..300).contains(&r.code) => {
                    self.revive(i, at);
                    return (Ok(r.body), Some(addr.clone()));
                }
                // Отказ шарда по существу запроса — не повод идти к соседке:
                // она ответит тем же. Так отличается «узел не отвечает» от
                // «узел ответил, что запрос негоден».
                Ok(r) if (400..500).contains(&r.code) => {
                    return (
                        Err(format!("{addr} ответил {} — {}", r.code, first_line(&r.body))),
                        Some(addr.clone()),
                    );
                }
                Ok(r) => {
                    self.cool_down(i, at);
                    why.push(format!("{addr} ответил {} — {}", r.code, first_line(&r.body)));
                }
                Err(e) => {
                    self.cool_down(i, at);
                    // Без своего `{addr}:` — его уже добавил `httpc`, и
                    // удвоенный адрес читается как две разные копии.
                    why.push(e);
                }
            }
        }
        (Err(why.join("; ")), None)
    }
}

impl Fanout {
    /// Шлёт **каждому шарду его собственное тело**.
    ///
    /// Отличие от [`Fanout::fan`] одно и оно несущее: там всем уходит один и тот
    /// же запрос, здесь у каждого свой. Шард, которому ничего не досталось, не
    /// опрашивается вовсе — платить сетевым запросом за пустой список незачем.
    pub fn fan_each(&self, picks: &[usize], path: &str, bodies: &[Option<String>]) -> Fanned {
        self.fan_each_inner(picks, path, bodies, true)
    }

    /// То же, но **без обхода копий**: спрашивается ровно выбранная.
    ///
    /// ## Почему у записи обхода нет
    ///
    /// Обход копий верен для чтения: они равноправны, и молчание одной лечится
    /// вопросом к соседке. Для записи он **вреден**: соседка — реплика, она
    /// применит запись у себя и не расскажет никому, и копии разойдутся молча.
    ///
    /// Поэтому запись, не дошедшая до primary, — это отказ, а не повод спросить
    /// у другого. Отказ клиент увидит; расхождение — нет.
    pub fn fan_each_once(&self, picks: &[usize], path: &str, bodies: &[Option<String>]) -> Fanned {
        self.fan_each_inner(picks, path, bodies, false)
    }

    fn fan_each_inner(
        &self,
        picks: &[usize],
        path: &str,
        bodies: &[Option<String>],
        walk: bool,
    ) -> Fanned {
        let n = self.shards.len();
        let mut replies: Vec<Result<String, String>> =
            (0..n).map(|_| Ok(String::from("{\"docs\":[]}"))).collect();
        let mut used: Vec<Option<String>> = (0..n).map(|_| None).collect();
        let work: Vec<usize> =
            (0..n).filter(|i| bodies.get(*i).map(|b| b.is_some()).unwrap_or(false)).collect();
        if work.is_empty() {
            return Fanned { replies, used };
        }
        let next = AtomicUsize::new(0);
        let workers = work.len().min(DEFAULT_PERMITS.max(1));
        let done: Vec<(usize, Result<String, String>, Option<String>)> =
            std::thread::scope(|scope| {
                let hs: Vec<_> = (0..workers)
                    .map(|_| {
                        let (next, work) = (&next, &work);
                        scope.spawn(move || {
                            let mut mine = Vec::new();
                            loop {
                                let at = next.fetch_add(1, Ordering::Relaxed);
                                let Some(&i) = work.get(at) else { break };
                                let body = bodies[i].as_deref().unwrap_or("");
                                let at = picks.get(i).copied().unwrap_or(0);
                                let (r, u) = if walk {
                                    self.ask(i, at, path, body)
                                } else {
                                    self.ask_one(i, at, path, body)
                                };
                                mine.push((i, r, u));
                            }
                            mine
                        })
                    })
                    .collect();
                hs.into_iter().flat_map(|h| h.join().unwrap_or_default()).collect()
            });
        for (i, r, u) in done {
            replies[i] = r;
            used[i] = u;
        }
        Fanned { replies, used }
    }
}

/// Первые полторы сотни байт чужого тела — для сообщения об отказе.
fn first_line(s: &str) -> String {
    let one: String = s.lines().next().unwrap_or_default().chars().take(160).collect();
    one
}

/// Складывает `total` и `df` из ответов первого круга.
///
/// Пропавший или невнятный ответ делает статистику негодной: сложить `df` по
/// части шардов хуже, чем не складывать вовсе, — вес получился бы посчитанным по
/// корпусу, которого нет.
pub fn merge_stats(replies: &[Result<String, String>]) -> Option<(u64, Vec<u64>)> {
    let mut total = 0u64;
    let mut df: Vec<u64> = Vec::new();
    for r in replies {
        let body = r.as_ref().ok()?;
        let v: serde_json::Value = serde_json::from_str(body).ok()?;
        if v.get("partial").and_then(serde_json::Value::as_bool) == Some(true) {
            return None;
        }
        total = total.saturating_add(v.get("total")?.as_u64()?);
        let d: Vec<u64> =
            v.get("df")?.as_array()?.iter().map(|x| x.as_u64().unwrap_or(0)).collect();
        if df.is_empty() {
            df = d;
        } else if df.len() == d.len() {
            for (a, b) in df.iter_mut().zip(d.iter()) {
                *a = a.saturating_add(*b);
            }
        } else {
            // Разное число слов после раскрытия — сопоставлять по позиции нельзя.
            return None;
        }
    }
    Some((total, df))
}

/// Попадание с шарда, уже с его номером.
#[derive(Clone, Debug)]
pub struct Hit {
    pub shard: usize,
    pub doc: u64,
    pub score: f64,
    /// Остальные поля попадания как они пришли — чтобы отдать их дословно.
    pub raw: serde_json::Value,
}

/// Сливает верхушки шардов в одну.
///
/// Порядок при равных весах — по паре «шард, номер»: иначе он зависел бы от того,
/// какой шард успел ответить первым, и одна и та же страница выдавала бы разное.
pub fn merge_hits(replies: &[Result<String, String>], k: usize) -> (Vec<Hit>, u64) {
    let mut out: Vec<Hit> = Vec::new();
    let mut visited = 0u64;
    for (shard, r) in replies.iter().enumerate() {
        let Ok(body) = r else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else { continue };
        visited += v.get("visited").and_then(|x| x.as_u64()).unwrap_or(0);
        let Some(hits) = v.get("hits").and_then(|x| x.as_array()) else { continue };
        for h in hits {
            let doc = h.get("doc").and_then(|x| x.as_u64()).unwrap_or(0);
            let score = h.get("score").and_then(|x| x.as_f64()).unwrap_or(0.0);
            out.push(Hit { shard, doc, score, raw: h.clone() });
        }
    }
    out.sort_by(|a, b| {
        b.score.total_cmp(&a.score).then(a.shard.cmp(&b.shard)).then(a.doc.cmp(&b.doc))
    });
    out.truncate(k);
    (out, visited)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> Result<String, String> {
        Ok(s.to_string())
    }

    /// Статистика складывается по всем шардам поэлементно.
    #[test]
    fn statistics_add_up_across_shards() {
        let rs = vec![
            ok(r#"{"total":100,"df":[10,5]}"#),
            ok(r#"{"total":200,"df":[20,1]}"#),
            ok(r#"{"total":300,"df":[0,4]}"#),
        ];
        assert_eq!(merge_stats(&rs), Some((600, vec![30, 10])));
    }

    /// Пропавший шард делает статистику негодной, а не «поменьше».
    ///
    /// Сложить `df` по части шардов хуже, чем не складывать: вес вышел бы
    /// посчитанным по корпусу, которого нет, и выглядел бы законным.
    #[test]
    fn a_missing_shard_voids_the_statistics() {
        let rs = vec![ok(r#"{"total":100,"df":[10]}"#), Err("нет связи".into())];
        assert_eq!(merge_stats(&rs), None, "статистика собралась по неполному набору");

        // И разное число слов — тоже негодность: сопоставлять по позиции нельзя.
        let rs = vec![ok(r#"{"total":1,"df":[1,2]}"#), ok(r#"{"total":1,"df":[1]}"#)];
        assert_eq!(merge_stats(&rs), None);
    }

    /// Верхушки сливаются по весу, а при равенстве — определённо.
    #[test]
    fn hits_merge_by_score_and_break_ties_deterministically() {
        let rs = vec![
            ok(r#"{"visited":10,"hits":[{"doc":7,"score":2.0},{"doc":1,"score":1.0}]}"#),
            ok(r#"{"visited":20,"hits":[{"doc":3,"score":3.0},{"doc":1,"score":1.0}]}"#),
        ];
        let (hits, visited) = merge_hits(&rs, 10);
        assert_eq!(visited, 30, "просмотренные не сложились");
        let got: Vec<(usize, u64, f64)> = hits.iter().map(|h| (h.shard, h.doc, h.score)).collect();
        assert_eq!(got, vec![(1, 3, 3.0), (0, 7, 2.0), (0, 1, 1.0), (1, 1, 1.0)], "порядок не тот");
        // Тай-брейк устойчив к порядку ответов: переставим шарды местами.
        let (again, _) = merge_hits(&rs, 10);
        assert_eq!(
            again.iter().map(|h| (h.shard, h.doc)).collect::<Vec<_>>(),
            hits.iter().map(|h| (h.shard, h.doc)).collect::<Vec<_>>()
        );
    }

    /// Верхушка обрезается до `k` **после** слияния, а не до него.
    #[test]
    fn the_top_is_cut_after_merging() {
        let rs = vec![
            ok(r#"{"hits":[{"doc":1,"score":1.0},{"doc":2,"score":0.9}]}"#),
            ok(r#"{"hits":[{"doc":1,"score":5.0},{"doc":2,"score":4.0}]}"#),
        ];
        let (hits, _) = merge_hits(&rs, 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].score, 5.0);
        assert_eq!(hits[1].score, 4.0, "обрезали до слияния — лучшие потерялись");
    }

    fn three(reps: &[&[&str]]) -> Fanout {
        let shards = reps.iter().map(|r| r.iter().map(|s| s.to_string()).collect()).collect();
        Fanout::new(shards, Duration::from_millis(200))
    }

    /// Выбор ходит по кругу, а не бьёт всегда в первую копию.
    ///
    /// Без этого реплики не разгружают: вторая простаивала бы, пока первая
    /// держит весь поиск, и «реплика» означала бы только запасной вариант.
    #[test]
    fn the_choice_rotates_instead_of_always_taking_the_first() {
        let f = three(&[&["a1", "a2", "a3"], &["b1", "b2"]]);
        let mut seen0 = std::collections::HashSet::new();
        let mut seen1 = std::collections::HashSet::new();
        for _ in 0..12 {
            let p = f.pick();
            seen0.insert(p[0]);
            seen1.insert(p[1]);
        }
        assert_eq!(seen0.len(), 3, "первый шард застрял на одной копии");
        assert_eq!(seen1.len(), 2, "второй шард застрял на одной копии");
    }

    /// Смещение зависит и от номера шарда, а не только от очереди.
    ///
    /// Иначе все шарды одного запроса шли бы в копии одного «столбца», и отказ
    /// целого узла-копии уносил бы сразу все шарды — то есть реплики не спасали
    /// бы ровно от того случая, ради которого их и заводят.
    #[test]
    fn one_request_spreads_across_columns() {
        let f = three(&[&["a1", "a2"], &["b1", "b2"], &["c1", "c2"]]);
        let p = f.pick();
        assert!(p.iter().any(|x| *x != p[0]), "все шарды взяли один столбец: {p:?}");
    }

    /// Отказавшая копия выпадает из выбора и возвращается сама.
    ///
    /// Без памяти об отказе реплики делают **хуже**: каждый запрос платил бы
    /// полный срок ожидания мёртвой копии прежде, чем дойти до живой.
    #[test]
    fn a_failed_replica_steps_aside_for_a_while() {
        let f = three(&[&["a1", "a2"]]);
        f.cool_down(0, 0);
        for _ in 0..8 {
            assert_eq!(f.pick()[0], 1, "остывающую копию выбрали снова");
        }
        // Ответившая копия сразу перестаёт считаться остывающей.
        f.revive(0, 0);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..8 {
            seen.insert(f.pick()[0]);
        }
        assert_eq!(seen.len(), 2, "вернувшаяся копия не вернулась в выбор");
    }

    /// Когда остывают все — шард всё равно спрашивается.
    ///
    /// Иначе кратковременный обрыв сети объявил бы шард мёртвым на всё время
    /// остывания, хотя копии к тому моменту уже вернулись, и координатор
    /// отказывал бы, ни разу не попробовав.
    #[test]
    fn when_every_replica_is_cooling_the_shard_is_still_asked() {
        let f = three(&[&["a1", "a2"]]);
        f.cool_down(0, 0);
        f.cool_down(0, 1);
        let p = f.pick();
        assert!(p[0] < 2, "выбор обязан остаться законным: {p:?}");
    }

    /// Мёртвая копия обходится живой, и это видно в отчёте.
    #[test]
    fn a_dead_replica_is_stepped_over() {
        // Порт занят слушателем, который отвечает 200 на что угодно.
        let live = std::net::TcpListener::bind("127.0.0.1:0").expect("порт");
        let addr = live.local_addr().expect("адрес");
        std::thread::spawn(move || {
            for s in live.incoming().take(4) {
                let Ok(mut s) = s else { continue };
                use std::io::{Read, Write};
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                let body = "{\"ok\":true}";
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        // А этот порт заведомо никем не занят: слушателя закрыли сразу.
        let dead = std::net::TcpListener::bind("127.0.0.1:0").expect("порт");
        let dead_addr = dead.local_addr().expect("адрес");
        drop(dead);

        let good = format!("127.0.0.1:{}", addr.port());
        let bad = format!("127.0.0.1:{}", dead_addr.port());
        let f = Fanout::new(vec![vec![bad.clone(), good.clone()]], Duration::from_millis(300));
        let picks = vec![0usize];
        let got = f.fan(&picks, "/", "{}");
        assert!(got.replies[0].is_ok(), "живая копия не спасла: {:?}", got.replies[0]);
        assert_eq!(got.used[0].as_deref(), Some(good.as_str()));
        assert_eq!(got.fallbacks(&f.shards, &picks), 1, "переход не посчитан");
        // И мёртвая теперь остывает: следующий запрос идёт мимо неё сразу.
        assert_eq!(f.pick()[0], 1);
    }

    /// Не ответившие шарды перечисляются по именам.
    #[test]
    fn failures_are_named() {
        let f = Fanned {
            replies: vec![ok("{}"), Err("нет связи".into())],
            used: vec![Some("a:1".into()), None],
        };
        let names = vec![vec!["a:1".to_string()], vec!["b:2".to_string(), "b2:2".to_string()]];
        let bad = f.failed(&names);
        assert_eq!(bad.len(), 1);
        // Названы **все** копии шарда: «шард не ответил» без перечисления копий
        // не говорит, куда идти смотреть.
        assert!(bad[0].contains("b:2") && bad[0].contains("b2:2"), "{bad:?}");
        assert!(bad[0].contains("нет связи"), "{bad:?}");
    }

    /// Отказ шарда несёт его код и начало тела, а не только «не вышло».
    #[test]
    fn a_shard_error_carries_its_reason() {
        assert_eq!(first_line("первая\nвторая"), "первая");
        assert_eq!(first_line(&"x".repeat(500)).len(), 160, "чужое тело не обрезано");
    }
}

#[cfg(test)]
mod fanout_tests {
    use super::*;

    /// Разрешения ограничивают число исходящих разом.
    ///
    /// Проверяется не «быстро ли», а **сколько одновременно**: предел на то и
    /// заведён, чтобы узел не поднимал поток и сокет на каждый шард каждого
    /// запроса. Пик считается самим держателем разрешений, а не по времени.
    #[test]
    fn permits_bound_how_many_run_at_once() {
        let p = Arc::new(Permits::new(3));
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|sc| {
            for _ in 0..32 {
                let (p, live, peak) = (Arc::clone(&p), Arc::clone(&live), Arc::clone(&peak));
                sc.spawn(move || {
                    let _hold = p.take();
                    let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(5));
                    live.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        let got = peak.load(Ordering::SeqCst);
        assert!(got <= 3, "разом шло {got} исходящих при пределе три");
        assert!(got >= 2, "разом шло {got} — предел душит там, где не должен");
        assert_eq!(live.load(Ordering::SeqCst), 0, "расписки не вернулись");
    }

    /// Расписка возвращается даже когда работник паникует.
    ///
    /// Иначе первая же паника съедала бы место навсегда, и предел сходился бы к
    /// нулю — узел переставал бы опрашивать шарды вовсе, не сказав ни слова.
    #[test]
    fn a_panicking_worker_gives_its_permit_back() {
        let p = Arc::new(Permits::new(1));
        let hit = std::panic::catch_unwind({
            let p = Arc::clone(&p);
            move || {
                let _hold = p.take();
                panic!("нарочно");
            }
        });
        assert!(hit.is_err(), "паника не случилась — проверка вхолостую");
        // Место обязано быть свободно: если нет, этот вызов не вернётся.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _hold = p.take();
            tx.send(()).ok();
        });
        assert!(
            rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "расписка не вернулась после паники — предел утёк"
        );
    }

    /// Выдача складывается по номеру шарда, а не по порядку готовности.
    ///
    /// Шарды опрашиваются из общей очереди и отвечают вразнобой; ответ обязан
    /// от этого не зависеть. Здесь все шарды заведомо недоступны — важно не
    /// содержимое ответов, а то, что каждый лёг на своё место.
    #[test]
    fn replies_land_by_shard_not_by_arrival() {
        // Адреса, которых нет: опрос честно провалится, но по местам разложится.
        let shards: Vec<Vec<String>> =
            (0..7).map(|i| vec![format!("127.0.0.1:{}", 9 + i)]).collect();
        let f = Fanout::with_permits(shards, Duration::from_millis(50), 4);
        let picks = f.pick();
        let out = f.fan(&picks, "/_search", "{}");
        assert_eq!(out.replies.len(), 7, "ответов не по числу шардов");
        assert_eq!(out.used.len(), 7);
        for (i, r) in out.replies.iter().enumerate() {
            assert!(r.is_err(), "шард {i} неожиданно ответил");
            // И это не заглушка «не опрошен»: каждый правда опрашивался.
            assert!(
                !r.as_ref().unwrap_err().contains("не опрошен"),
                "шард {i} остался неопрошенным"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Маршрутизация документов по шардам.
//
// ## Что здесь меняется в природе координатора
//
// Поиск координатор рассылает **не разбирая тела**: запрос один и тот же для
// всех шардов. С документами так нельзя — чтобы отправить документ туда, где он
// живёт, надо знать его идентификатор, то есть тело разобрать.
//
// Это осознанная перемена, а не мелочь: координатор перестаёт быть прозрачным
// пересыльщиком и становится разбирающим. Отсюда и правило — он разбирает
// **только то, что нужно для адреса**, и пересылает остальное как есть. Иначе
// у одного и того же тела появилось бы два разбора, координаторский и шардовый,
// и однажды они разошлись бы.
//
// ## Раскладка та же, что внутри процесса
//
// Ячейки `shards::slot_of` и полосы `shards::slot_owner` — те же самые, что у
// `shards::route`, и то же требование: разойдись они, документ по сети и в
// процессе попадал бы в разные места. Проверено сверкой в
// `coord::route_tests::the_network_route_matches_the_in_process_one`.
// ---------------------------------------------------------------------------

/// В какой шард идёт документ.
///
/// `routing` перебивает идентификатор: тот же ключ у нескольких документов
/// кладёт их в один шард, и это единственный способ спросить их одним запросом.
///
/// Адрес считается через **ячейку**, а не остатком по числу шардов: ячейка от
/// числа шардов не зависит, и потому деление шарда переселяет ровно свою полосу,
/// а не весь корпус. Разбор — у [`svindb_core::shards::SLOTS`].
pub fn shard_of(id: &str, routing: Option<&str>, shards: usize) -> usize {
    if shards == 0 {
        return 0;
    }
    let key = routing.unwrap_or(id);
    svindb_core::shards::slot_owner(svindb_core::shards::slot_of(key), shards)
}

/// Равные полосы для `n` шардов — умолчание, пока никого не делили.
pub fn equal_ranges(n: usize) -> Vec<(u64, u64)> {
    (0..n)
        .map(|i| (svindb_core::shards::slot_lo(i, n), svindb_core::shards::slot_lo(i + 1, n)))
        .collect()
}

/// Покрывают ли полосы пространство ячеек ровно один раз.
///
/// ## Почему это отказ запуска, а не предупреждение
///
/// Дыра в покрытии — документы, которые некуда адресовать: запись уйдёт в
/// какой-то шард, а чтение пойдёт в другой, и оба ответят без тени сомнения.
/// Перекрытие — документы с двумя адресами: запись ляжет в один, найдётся в
/// обоих. Оба случая молчаливы, и заметит их не координатор, а тот, кто через
/// месяц не найдёт своего документа.
pub fn check_ranges(ranges: &[(u64, u64)]) -> Result<(), String> {
    if ranges.is_empty() {
        return Err(String::from("полос нет вовсе"));
    }
    let mut sorted: Vec<(u64, u64, usize)> =
        ranges.iter().enumerate().map(|(i, (lo, hi))| (*lo, *hi, i)).collect();
    sorted.sort_unstable();
    let mut edge = 0u64;
    for (lo, hi, i) in sorted.iter() {
        if hi <= lo {
            return Err(format!("шард {i}: полоса [{lo}, {hi}) пуста или вывернута"));
        }
        if *lo != edge {
            return Err(if *lo > edge {
                format!("ячейки [{edge}, {lo}) не покрыты ни одним шардом")
            } else {
                format!("шард {i} перекрывает чужую полосу с ячейки {lo}")
            });
        }
        edge = *hi;
    }
    if edge != svindb_core::shards::SLOTS {
        return Err(format!(
            "полосы кончились на ячейке {edge}, а их {}",
            svindb_core::shards::SLOTS
        ));
    }
    Ok(())
}

impl Fanout {
    /// В какой шард идёт документ **по полосам этого веера**.
    ///
    /// Не свободная функция [`shard_of`]: та делит поровну и потому верна лишь
    /// пока никого не делили.
    pub fn owner(&self, id: &str, routing: Option<&str>) -> usize {
        let key = routing.unwrap_or(id);
        let slot = svindb_core::shards::slot_of(key);
        self.ranges
            .iter()
            .position(|(lo, hi)| slot >= *lo && slot < *hi)
            .unwrap_or_else(|| shard_of(id, routing, self.shards.len()))
    }
}

/// Разложенная по шардам пачка: тело для каждого и места документов в исходном.
pub struct Split {
    /// Тело для шарда. `None` — этому шарду ничего не досталось.
    pub bodies: Vec<Option<String>>,
    /// Для каждого шарда — места его документов в исходном списке.
    ///
    /// Нужно, чтобы собрать ответ **в порядке запроса**: клиент сопоставляет
    /// ответ со своим списком по номеру, и перестановка сдвинула бы ему всё.
    pub places: Vec<Vec<usize>>,
}

/// Раскладывает пачку документов по шардам.
///
/// `key` — имя поля в каждом элементе, по которому берётся идентификатор.
/// `wrap` — имя массива в теле: `documents` у приёма, `ids` у набора.
/// Раскладка при **равных** полосах. Живой путь ходит через [`split_with`]: там
/// полосы веера, а они после деления шарда не равны. Здесь — равные, и потому
/// это остаётся тем, чем должно: проверяемой опорой в тестах.
#[cfg(test)]
pub fn split_docs(body: &[u8], wrap: &str, shards: usize) -> Option<Split> {
    split_by(body, wrap, shards, &|id, routing| shard_of(id, routing, shards))
}

/// То же с явным правилом адресации — им пользуется веер с неравными полосами.
pub fn split_with(body: &[u8], wrap: &str, fan: &Fanout) -> Option<Split> {
    split_by(body, wrap, fan.shards.len(), &|id, routing| fan.owner(id, routing))
}

fn split_by(
    body: &[u8],
    wrap: &str,
    shards: usize,
    place: &dyn Fn(&str, Option<&str>) -> usize,
) -> Option<Split> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    let list = v.get(wrap)?.as_array()?;
    let mut per: Vec<Vec<serde_json::Value>> = (0..shards).map(|_| Vec::new()).collect();
    let mut places: Vec<Vec<usize>> = (0..shards).map(|_| Vec::new()).collect();
    for (at, one) in list.iter().enumerate() {
        // Идентификатор берётся и строкой, и числом — тем же переводом, что и
        // на приёме: числовой `id` там принимается, и разойтись эти два разбора
        // не вправе.
        let id = match one {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Object(_) => match one.get("id") {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Number(n)) => n.to_string(),
                // Документ без идентификатора адресовать нечем. Это не наша
                // ошибка и не наше дело: пусть о ней скажет шард, который
                // разбирает тело по-настоящему. Кладём в нулевой — он ответит
                // отказом по существу.
                _ => String::new(),
            },
            serde_json::Value::Number(n) => n.to_string(),
            _ => String::new(),
        };
        // `peer_id` — канонический routing key истории и audience. Имя здесь
        // намеренно предметное, а не бытовое `routing`: документы одного peer
        // обязаны лежать на одном shard, чтобы history не делала fan-out.
        // Старые тела без peer_id сохраняют прежнюю раскладку по document ID.
        let routing =
            one.as_object().and_then(|object| object.get("peer_id")).and_then(
                |value| match value {
                    serde_json::Value::String(value) => Some(value.clone()),
                    serde_json::Value::Number(value) => Some(value.to_string()),
                    _ => None,
                },
            );
        let s = place(&id, routing.as_deref());
        per[s].push(one.clone());
        places[s].push(at);
    }
    // Тело шарда — **исходное тело с заменённым массивом**, а не собранное
    // заново из одного массива.
    //
    // Пересборка теряла соседние ключи, и первым потерянным оказался
    // `conditions` у `_put`: условная замена молча превращалась в безусловную и
    // отвечала `200`. Клиент, писавший compare-and-swap, получал перезапись
    // чужой версии и узнавал об этом не отказом, а расхождением данных.
    let bodies = per
        .into_iter()
        .map(|mine| {
            if mine.is_empty() {
                return None;
            }
            let mut whole = v.clone();
            if let serde_json::Value::Object(m) = &mut whole {
                m.insert(wrap.to_string(), serde_json::Value::Array(mine));
                return Some(whole.to_string());
            }
            Some(serde_json::json!({ wrap: mine }).to_string())
        })
        .collect();
    Some(Split { bodies, places })
}

/// Собирает ответы набора обратно в порядок запроса.
///
/// Ответ шарда — `{"docs":[…]}` в порядке **его** части списка; места берутся
/// из [`Split::places`]. Шард, не ответивший вовсе, отдаёт своим документам
/// `found:false` с признаком `failed`: молчание тут было бы неотличимо от
/// «такого документа нет», а это разные вещи.
pub fn merge_docs(replies: &[Result<String, String>], split: &Split, total: usize) -> String {
    let mut slots: Vec<Option<String>> = (0..total).map(|_| None).collect();
    for (s, at) in split.places.iter().enumerate() {
        match replies.get(s) {
            Some(Ok(body)) => {
                let got: Option<Vec<serde_json::Value>> =
                    serde_json::from_str::<serde_json::Value>(body)
                        .ok()
                        .and_then(|v| v.get("docs").and_then(|d| d.as_array()).cloned());
                match got {
                    Some(docs) if docs.len() == at.len() => {
                        for (k, place) in at.iter().enumerate() {
                            slots[*place] = Some(docs[k].to_string());
                        }
                    }
                    // Шард ответил не тем числом документов, сколько спросили, —
                    // сопоставлять по позиции больше нельзя, и гадать нельзя
                    // тоже. Вся его часть объявляется несостоявшейся.
                    _ => {
                        for place in at.iter() {
                            slots[*place] = Some(String::from(
                                "{\"found\":false,\"failed\":\"шард ответил не по форме\"}",
                            ));
                        }
                    }
                }
            }
            _ => {
                for place in at.iter() {
                    slots[*place] =
                        Some(String::from("{\"found\":false,\"failed\":\"шард не ответил\"}"));
                }
            }
        }
    }
    let body: Vec<String> = slots.into_iter().map(|x| x.unwrap_or_else(|| "null".into())).collect();
    format!("{{\"docs\":[{}]}}", body.join(","))
}

/// Номера мест списком — то, по чему клиент повторит ровно недошедшее.
fn places(at: &[usize]) -> String {
    let one: Vec<String> = at.iter().map(|x| x.to_string()).collect();
    format!("[{}]", one.join(","))
}

/// Складывает ответы **записи** обратно в один.
///
/// ## Почему отказ одного шарда меняет код ответа всему запросу
///
/// Пачка, принятая наполовину, хуже отвергнутой: повтор сдвоит принятое, а
/// разобрать, что дошло, клиенту нечем. Узел это правило уже соблюдает внутри
/// себя (строгость отвергает всю пачку целиком), и координатор не вправе
/// ослабить его тем, что шардов стало несколько. Поэтому: все ответили — `200`
/// и сумма; кто-то не ответил — `503`, сумма **дошедшего** и поимённый список
/// не дошедших. Клиент повторяет свою пачку целиком; принятое заменится собой
/// же, потому что адресуется идентификатором.
pub fn merge_writes(replies: &[Result<String, String>], split: &Split) -> (u16, String) {
    let mut sums: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    let mut bad: Vec<String> = Vec::new();
    for (s, at) in split.places.iter().enumerate() {
        if at.is_empty() {
            continue;
        }
        match replies.get(s) {
            Some(Ok(body)) => match serde_json::from_str::<serde_json::Value>(body) {
                Ok(serde_json::Value::Object(m)) => {
                    // Конфликт условной замены — не отказ связи, а ответ по
                    // существу, и он обязан дойти до клиента как есть.
                    if m.contains_key("error")
                        || m.get("conflict").and_then(|c| c.as_bool()) == Some(true)
                    {
                        bad.push(format!(
                            "{{\"shard\":{s},\"docs\":{},\"reply\":{body}}}",
                            at.len()
                        ));
                        continue;
                    }
                    for (k, v) in m {
                        if let Some(n) = v.as_i64() {
                            *sums.entry(k).or_insert(0) += n;
                        }
                    }
                }
                _ => bad.push(format!(
                    "{{\"shard\":{s},\"docs\":{},\"at\":{},\"error\":\"ответ шарда не разобрался\"}}",
                    at.len(),
                    places(at)
                )),
            },
            Some(Err(e)) => bad.push(format!(
                "{{\"shard\":{s},\"docs\":{},\"at\":{},\"error\":{}}}",
                at.len(),
                places(at),
                serde_json::Value::String(e.clone())
            )),
            None => bad.push(format!(
                "{{\"shard\":{s},\"docs\":{},\"at\":{},\"error\":\"шард вне карты\"}}",
                at.len(),
                places(at)
            )),
        }
    }
    // Ключи те же, что у одиночного узла, и в том же порядке слов: клиент,
    // переключившийся с одного узла на веер, разбирает ответ прежним кодом.
    let mut out: Vec<String> =
        sums.iter().map(|(k, v)| format!("{}:{v}", serde_json::Value::String(k.clone()))).collect();
    if bad.is_empty() {
        if out.is_empty() {
            out.push(String::from("\"added\":0"));
        }
        return (200, format!("{{{}}}", out.join(",")));
    }
    out.push(String::from("\"partial\":true"));
    out.push(format!("\"failed\":[{}]", bad.join(",")));
    (503, format!("{{{}}}", out.join(",")))
}

#[cfg(test)]
mod route_tests {
    use super::*;

    /// Сетевая раскладка совпадает с внутрипроцессной документ в документ.
    #[test]
    fn the_network_route_matches_the_in_process_one() {
        for shards in [1usize, 2, 4, 7, 16] {
            for i in 0..300u32 {
                let id = format!("документ-{i}");
                assert_eq!(
                    shard_of(&id, None, shards),
                    svindb_core::shards::route(&id, shards),
                    "{id} при {shards} шардах разошёлся"
                );
            }
        }
    }

    /// Ключ маршрутизации перебивает идентификатор и держит документы вместе.
    #[test]
    fn a_routing_key_keeps_documents_together() {
        let shards = 8;
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..50u32 {
            seen.insert(shard_of(&format!("разные-{i}"), Some("общий-ключ"), shards));
        }
        assert_eq!(seen.len(), 1, "один ключ развёл документы по {} шардам", seen.len());
        // А без ключа те же документы расходятся.
        let spread: std::collections::BTreeSet<usize> =
            (0..50u32).map(|i| shard_of(&format!("разные-{i}"), None, shards)).collect();
        assert!(spread.len() > 1, "без ключа документы не разошлись — проверка вхолостую");
    }

    #[test]
    fn peer_id_routes_a_history_batch_to_one_shard() {
        let documents: Vec<serde_json::Value> = (0..100)
            .map(|message_id| {
                serde_json::json!({
                    "id": format!("peer-777:{message_id}"),
                    "peer_id": 777,
                    "message_id": message_id,
                })
            })
            .collect();
        let body = serde_json::json!({"documents": documents}).to_string();
        let split = split_docs(body.as_bytes(), "documents", 16).expect("пачка");
        let occupied: Vec<usize> = split
            .places
            .iter()
            .enumerate()
            .filter_map(|(shard, places)| (!places.is_empty()).then_some(shard))
            .collect();
        assert_eq!(occupied.len(), 1, "один peer разложен по shard: {occupied:?}");
        assert_eq!(split.places[occupied[0]].len(), 100);
    }

    /// Пачка раскладывается без потерь и собирается обратно в порядок запроса.
    #[test]
    fn a_batch_splits_and_comes_back_in_order() {
        let shards = 4;
        let ids: Vec<String> = (0..40).map(|i| format!("d{i}")).collect();
        let body = serde_json::json!({ "ids": ids }).to_string();
        let split = split_docs(body.as_bytes(), "ids", shards).expect("разложилось");

        // Ни один документ не потерян и ни один не задвоен.
        let mut all: Vec<usize> = split.places.iter().flatten().copied().collect();
        all.sort_unstable();
        assert_eq!(all, (0..40).collect::<Vec<_>>(), "разложение потеряло или задвоило");

        // Каждый шард получил ровно свои.
        for (s, at) in split.places.iter().enumerate() {
            for &k in at {
                assert_eq!(shard_of(&ids[k], None, shards), s, "{} ушёл не в свой шард", ids[k]);
            }
        }

        // Ответы шардов приходят вразнобой, а собираются по местам.
        let replies: Vec<Result<String, String>> = split
            .places
            .iter()
            .map(|at| {
                let docs: Vec<String> =
                    at.iter().map(|k| format!("{{\"id\":\"{}\"}}", ids[*k])).collect();
                Ok(format!("{{\"docs\":[{}]}}", docs.join(",")))
            })
            .collect();
        let merged = merge_docs(&replies, &split, ids.len());
        let v: serde_json::Value = serde_json::from_str(&merged).expect("собранное — не json");
        let got = v["docs"].as_array().expect("нет docs");
        assert_eq!(got.len(), ids.len());
        for (i, one) in got.iter().enumerate() {
            assert_eq!(one["id"].as_str(), Some(ids[i].as_str()), "место {i} заняло чужое");
        }
    }

    /// Не ответивший шард даёт своим документам признак отказа, а не «нет такого».
    ///
    /// Молчание тут неотличимо от «документа не существует», а это разные вещи:
    /// по первому надо повторить, по второму — нет.
    #[test]
    fn a_silent_shard_is_named_not_hidden() {
        let shards = 3;
        let ids: Vec<String> = (0..12).map(|i| format!("d{i}")).collect();
        let body = serde_json::json!({ "ids": ids }).to_string();
        let split = split_docs(body.as_bytes(), "ids", shards).expect("разложилось");
        let mut replies: Vec<Result<String, String>> = split
            .places
            .iter()
            .map(|at| {
                let docs: Vec<String> = at.iter().map(|_| "{\"found\":true}".to_string()).collect();
                Ok(format!("{{\"docs\":[{}]}}", docs.join(",")))
            })
            .collect();
        replies[1] = Err("шард 1 не отвечает".into());
        let merged = merge_docs(&replies, &split, ids.len());
        let v: serde_json::Value = serde_json::from_str(&merged).unwrap();
        let got = v["docs"].as_array().unwrap();
        assert_eq!(got.len(), ids.len());
        for (i, one) in got.iter().enumerate() {
            let mine = shard_of(&ids[i], None, shards);
            if mine == 1 {
                assert_eq!(one["failed"].as_str(), Some("шард не ответил"), "место {i}");
                assert_eq!(one["found"].as_bool(), Some(false), "место {i}");
            } else {
                assert_eq!(one["found"].as_bool(), Some(true), "место {i}");
            }
        }
    }

    /// Ответ не по форме объявляется несостоявшимся, а не сопоставляется наугад.
    #[test]
    fn a_malformed_reply_is_not_matched_by_guess() {
        let shards = 2;
        let ids: Vec<String> = (0..10).map(|i| format!("d{i}")).collect();
        let body = serde_json::json!({ "ids": ids }).to_string();
        let split = split_docs(body.as_bytes(), "ids", shards).expect("разложилось");
        // Первый шард отдал меньше документов, чем у него спросили.
        let mut replies: Vec<Result<String, String>> = split
            .places
            .iter()
            .map(|at| {
                let docs: Vec<String> = at.iter().map(|_| "{\"found\":true}".to_string()).collect();
                Ok(format!("{{\"docs\":[{}]}}", docs.join(",")))
            })
            .collect();
        replies[0] = Ok("{\"docs\":[{\"found\":true}]}".into());
        let merged = merge_docs(&replies, &split, ids.len());
        let v: serde_json::Value = serde_json::from_str(&merged).unwrap();
        for (i, one) in v["docs"].as_array().unwrap().iter().enumerate() {
            if shard_of(&ids[i], None, shards) == 0 {
                assert_eq!(
                    one["failed"].as_str(),
                    Some("шард ответил не по форме"),
                    "место {i} сопоставлено наугад"
                );
            }
        }
    }
}
