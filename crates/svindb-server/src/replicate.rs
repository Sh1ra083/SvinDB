//! Репликация записи: терм, подтверждение и отказ устаревшей реплики.
//!
//! ## Что здесь решается
//!
//! Три разные вещи, которые легко спутать в одну:
//!
//! 1. **Split brain.** Отделённый primary не знает, что его сместили, и
//!    продолжает принимать запись. Реплика обязана его остановить, и остановить
//!    **без чужого разрешения** — по номеру терма, который она уже видела.
//! 2. **Договор подтверждения.** «Принято» значит разное при `one`, `quorum` и
//!    `all`, и разница эта — про то, переживёт ли запись отказ. Она должна быть
//!    названа в ответе, а не подразумеваться.
//! 3. **Устаревшая реплика.** Реплика, пропустившая запись, не вправе молча
//!    принять следующую: между ними дыра, и приняв, реплика станет выглядеть
//!    целой, не будучи ею. Догон — это Ф7.4; здесь дыра только **обнаруживается**
//!    и называется.
//!
//! ## Почему терм долговечен
//!
//! Терм в памяти защищает от split brain ровно до перезапуска. Смещённый
//! primary, переживший рестарт, забыл бы, что видел терм выше своего, и принял
//! бы запись прежнего — то есть защита исчезала бы ровно в том случае, ради
//! которого заведена. Поэтому терм лежит в файле и уносится на диск до того,
//! как запись подтверждена.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Mutex;

/// Имя файла с термом. Рядом с данными: терм принадлежит **этой** копии.
const FILE: &str = "replica-term.tsv";

/// Отказ реплики, названный по причине.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refuse {
    /// Терм ниже виденного: пишет тот, кого уже сместили.
    StaleTerm { seen: u64, got: u64 },
    /// Между принятым и предложенным разрыв: реплика отстала.
    Gap { have: u64, next: u64 },
    /// Файл терма не пишется. Принять запись при этом нельзя: защита от split
    /// brain держится на нём.
    Broken(String),
}

impl Refuse {
    /// Код ответа. Разные коды — потому что разные действия у отправителя:
    /// по `409` повторять бессмысленно, по `503` — можно и нужно.
    pub fn code(&self) -> u16 {
        match self {
            Refuse::StaleTerm { .. } | Refuse::Gap { .. } => 409,
            Refuse::Broken(_) => 503,
        }
    }

    pub fn say(&self) -> String {
        match self {
            Refuse::StaleTerm { seen, got } => {
                format!("устаревший терм {got}: узел уже видел {seen}")
            }
            Refuse::Gap { have, next } => {
                format!("разрыв: у реплики {have}, предложено {next} — нужен догон")
            }
            Refuse::Broken(e) => format!("терм не сохраняется: {e}"),
        }
    }
}

/// Состояние реплики: какой терм видела и до какого номера применила.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Seen {
    pub term: u64,
    /// Номер последней применённой записи репликации. Ноль — не применяла.
    pub upto: u64,
}

/// Долговечная память реплики о терме и номере.
pub struct Terms {
    path: PathBuf,
    at: Mutex<Seen>,
}

impl Terms {
    /// Открывает состояние рядом с данными. Отсутствие файла — не ошибка: узел,
    /// который ещё ничего не реплицировал, начинает с нуля.
    pub fn open(dir: &str) -> Terms {
        let path = PathBuf::from(dir).join(FILE);
        let at = std::fs::read_to_string(&path).ok().and_then(|s| parse(&s)).unwrap_or_default();
        Terms { path, at: Mutex::new(at) }
    }

    pub fn seen(&self) -> Seen {
        *self.at.lock().expect("терм реплики")
    }

    /// Принимает запись терма `term` с номером `at`.
    ///
    /// ## Порядок здесь несущий
    ///
    /// Сначала на диск, потом «да». Обратный порядок означал бы окно, в котором
    /// узел уже подтвердил запись нового терма, но переживёт падение с прежним —
    /// и тогда прежний primary, вернувшись, снова станет для него законным.
    ///
    /// Разрыв в номерах проверяется **только внутри одного терма**. Смена терма
    /// начинает счёт заново: новый primary нумерует своё, и требовать от него
    /// продолжения чужой нумерации значило бы требовать знания, которого у него
    /// нет.
    /// Пускает запись к применению: проверяет терм и разрыв, **не двигая**
    /// отметку применённого.
    ///
    /// ## Почему это две операции, а не одна
    ///
    /// Терм обязан лечь на диск **до** применения: он и есть защита от split
    /// brain, а защита, применяющая раньше, чем защищает, не защищает.
    ///
    /// Отметка применённого — наоборот, **после**. Пока она ставилась вместе с
    /// термом, любой отказ применения (тело не разобралось, приём отравлен,
    /// кончилось место) оставлял копию с записью «применено до N», которой у неё
    /// нет. Дыра при этом становилась вечной: обнаружитель разрыва сравнивает
    /// номера, а номера сходятся — он молчит, и молчит навсегда.
    ///
    /// Цена честная и названная: падение **между** применением и [`Terms::done`]
    /// оставит отметку позади, и primary дошлёт запись ещё раз. Для `put` и
    /// гашения это безвредно, для `add` — второй экземпляр. Обратный порядок
    /// прятал бы дыру при каждом отказе применения, а отказ куда вероятнее
    /// падения ровно в этот промежуток.
    pub fn admit(&self, term: u64, at: u64) -> Result<bool, Refuse> {
        let mut have = self.at.lock().expect("терм реплики");
        if term < have.term {
            return Err(Refuse::StaleTerm { seen: have.term, got: term });
        }
        if term > have.term {
            // Новый терм начинает счёт заново — **с единицы**.
            //
            // Пока принимался любой номер, копия, отсутствовавшая в момент
            // вступления нового primary, брала его N-ю запись за первую и
            // объявляла применённым всё, чего никогда не видела. Теперь она
            // получает разрыв и идёт в догон.
            if at != 1 {
                return Err(Refuse::Gap { have: 0, next: at });
            }
            write(&self.path, Seen { term, upto: 0 }).map_err(Refuse::Broken)?;
            *have = Seen { term, upto: 0 };
            return Ok(true);
        }
        if at == have.upto && at != 0 {
            return Ok(false);
        }
        if at != have.upto + 1 {
            return Err(Refuse::Gap { have: have.upto, next: at });
        }
        Ok(true)
    }

    /// Отмечает запись применённой. Зовётся **после** применения, и только при
    /// успехе.
    pub fn done(&self, term: u64, at: u64) -> Result<(), Refuse> {
        let mut have = self.at.lock().expect("терм реплики");
        if term < have.term {
            return Err(Refuse::StaleTerm { seen: have.term, got: term });
        }
        let next = Seen { term, upto: at };
        write(&self.path, next).map_err(Refuse::Broken)?;
        *have = next;
        Ok(())
    }

    /// Обе половины разом — **только для тестов договора номеров**.
    ///
    /// В живом пути их разделяет применение, и слить их обратно значило бы
    /// вернуть тот самый дефект, ради которого они разделены: отметку
    /// применённого, поставленную раньше применения.
    #[cfg(test)]
    pub fn accept(&self, term: u64, at: u64) -> Result<(Seen, bool), Refuse> {
        let fresh = self.admit(term, at)?;
        if fresh {
            self.done(term, at)?;
        }
        Ok((self.seen(), fresh))
    }

    /// Поднимает терм без применения записи — так primary объявляет о вступлении.
    pub fn observe(&self, term: u64) -> Result<Seen, Refuse> {
        let mut have = self.at.lock().expect("терм реплики");
        if term < have.term {
            return Err(Refuse::StaleTerm { seen: have.term, got: term });
        }
        if term == have.term {
            return Ok(*have);
        }
        let next = Seen { term, upto: 0 };
        write(&self.path, next).map_err(Refuse::Broken)?;
        *have = next;
        Ok(next)
    }
}

fn parse(s: &str) -> Option<Seen> {
    let mut it = s.trim().split('\t');
    let term = it.next()?.parse().ok()?;
    let upto = it.next()?.parse().ok()?;
    Some(Seen { term, upto })
}

/// Пишет через временный файл и переименование: половина строки на диске — это
/// терм, который не читается, а терм, который не читается, — это узел без
/// защиты от split brain.
fn write(path: &PathBuf, at: Seen) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    let body = format!("{}\t{}\n", at.term, at.upto);
    {
        let mut f = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
        f.write_all(body.as_bytes()).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
    }
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())?;
    // Каталог тоже: без него переименование может не пережить питание.
    if let Some(dir) = path.parent() {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

/// Итог рассылки записи держателям.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acked {
    /// Сколько держателей подтвердили, включая primary.
    pub got: usize,
    /// Сколько держателей всего.
    pub of: usize,
    /// Сколько нужно было по договору.
    pub need: usize,
    /// Поимённо — кто не подтвердил и почему.
    pub missing: Vec<(String, String)>,
}

impl Acked {
    pub fn enough(&self) -> bool {
        self.got >= self.need
    }

    /// Ответ клиенту: договор назван, счёт назван, недостача названа поимённо.
    ///
    /// Не «принято» голым словом. Клиент, попросивший `quorum` и получивший
    /// `one`, обязан узнать об этом из ответа, а не из расследования после
    /// потери реплики.
    pub fn suffix(&self, ack: svindb_core::cluster::Ack, quote: fn(&str) -> String) -> String {
        let mut out =
            format!(",\"ack\":{},\"acked\":{},\"of\":{}", quote(ack.name()), self.got, self.of);
        if !self.missing.is_empty() {
            let who: Vec<String> = self
                .missing
                .iter()
                .map(|(a, e)| format!("{{\"node\":{},\"error\":{}}}", quote(a), quote(e)))
                .collect();
            out.push_str(&format!(",\"missing\":[{}]", who.join(",")));
        }
        out
    }
}

/// Кольцо недавно разосланного — то, чем догоняют отставшую копию.
///
/// ## Почему кольцо, а не журнал на диске
///
/// Догон бывает двух размеров, и лечатся они разным. Копия, отсутствовавшая
/// секунды, отстала на десятки записей — их дешевле переслать, чем городить
/// перенос сегментов. Копия, отсутствовавшая час, отстала настолько, что
/// пересылка записей дороже переноса готовых сегментов, и делать её значило бы
/// выбрать заведомо худший способ.
///
/// Кольцо обслуживает первый случай и **честно отказывается** от второго:
/// выпавшее из кольца названо, а не подменено молчанием. Второй случай — перенос
/// сегментов, и он не сделан (см. ADR-0003).
pub struct Journal {
    cap: usize,
    /// Содержимое и его объём — под **одним** замком.
    ///
    /// Двумя они разъезжаются: между снятием первого и второго объём уже не
    /// описывает содержимое, и вытеснение считает не по тому числу. Сейчас
    /// рассылка сериализована и до этого не доходит, но инвариант не должен
    /// держаться на чужой сериализации.
    inside: Mutex<Ring>,
}

#[derive(Default)]
struct Ring {
    /// `(номер, тело)` по возрастанию номера.
    recs: std::collections::VecDeque<(u64, String)>,
    bytes: usize,
}

impl Journal {
    pub fn new(cap: usize) -> Journal {
        Journal { cap, inside: Mutex::new(Ring::default()) }
    }

    fn keep(&self, at: u64, body: &str) {
        let mut r = self.inside.lock().expect("кольцо репликации");
        r.recs.push_back((at, body.to_string()));
        r.bytes += body.len();
        while r.bytes > self.cap && r.recs.len() > 1 {
            if let Some((_, old)) = r.recs.pop_front() {
                r.bytes = r.bytes.saturating_sub(old.len());
            }
        }
    }

    /// Записи строго после `from`, по возрастанию. Пусто — либо догонять нечего,
    /// либо нужное уже вытеснено; различает это [`Journal::has`].
    fn since(&self, from: u64) -> Vec<(u64, String)> {
        let r = self.inside.lock().expect("кольцо репликации");
        r.recs.iter().filter(|(at, _)| *at > from).cloned().collect()
    }

    /// Есть ли в кольце запись `from + 1`. Нет — догон записями невозможен, и
    /// об этом надо сказать, а не молча отправить хвост.
    fn has(&self, from: u64) -> bool {
        let r = self.inside.lock().expect("кольцо репликации");
        r.recs.front().map(|(at, _)| *at <= from + 1).unwrap_or(false)
    }
}

/// Ограничитель полосы догона.
///
/// Догон идёт по той же сети, что и живая запись, и без ограничителя копия,
/// вернувшаяся после часа отсутствия, отняла бы полосу у всех, кто работает.
/// Ноль — без ограничения.
pub struct Throttle {
    per_sec: usize,
    spent: Mutex<(std::time::Instant, usize)>,
}

impl Throttle {
    pub fn new(per_sec: usize, now: std::time::Instant) -> Throttle {
        Throttle { per_sec, spent: Mutex::new((now, 0)) }
    }

    /// Пропускает `n` байт, при нужде подождав. Считает **окном в секунду**, а
    /// не средним за всё время: среднее позволило бы простоявшему часу
    /// выплеснуться разом, то есть ровно тому, ради чего ограничитель заведён.
    fn pass(&self, n: usize) {
        if self.per_sec == 0 {
            return;
        }
        let wait = {
            let mut at = self.spent.lock().expect("полоса догона");
            let now = std::time::Instant::now();
            if now.duration_since(at.0) >= std::time::Duration::from_secs(1) {
                *at = (now, 0);
            }
            at.1 += n;
            if at.1 > self.per_sec {
                let left =
                    std::time::Duration::from_secs(1).saturating_sub(now.duration_since(at.0));
                *at = (now + left, 0);
                left
            } else {
                std::time::Duration::ZERO
            }
        };
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }
}

/// Держатели шарда с точки зрения primary.
///
/// Номер записи считается **здесь**, а не в журнале: журнальный номер узла —
/// его собственная нумерация, у реплики она своя, и совпадать они не обязаны
/// ничем. Реплике нужен порядок отправителя, чтобы заметить дыру, — и это ровно
/// то, что считает `at`.
pub struct Peers {
    /// Адреса реплик. Себя primary в список не включает: он и так применил.
    pub addrs: Vec<String>,
    pub ack: svindb_core::cluster::Ack,
    pub term: u64,
    at: std::sync::atomic::AtomicU64,
    pub timeout: std::time::Duration,
    /// Кольцо разосланного — чем догоняют вернувшуюся копию.
    journal: Journal,
    /// Ограничитель полосы догона. Живой записи не касается: её задерживать
    /// значило бы наказать работающих за возвращение отставшего.
    throttle: Throttle,
    /// Замок на всю рассылку: номер, кольцо и отправка — под ним.
    ///
    /// ## Почему рассылка сериализована, и чего это стоит
    ///
    /// Реплика принимает запись, только если её номер идёт **сразу** за
    /// применённым: на этом стоит обнаружение дыры. Значит порядок доставки —
    /// не пожелание, а условие работы. Без замка две одновременные записи
    /// выдают номера в одном порядке, а доходят в другом, и вторая получает
    /// отказ по разрыву, которого на самом деле нет; хуже — догон по такому
    /// «разрыву» может не найти в кольце запись, номер которой уже выдан, но
    /// тело ещё не положено.
    ///
    /// Цена названа и не скрыта: **приём шарда с репликами упирается в один
    /// круг до самой медленной копии на пачку**. Снимается это конвейером с
    /// буфером переупорядочения на реплике; он не сделан.
    order: Mutex<()>,
}

impl Peers {
    pub fn new(
        addrs: Vec<String>,
        ack: svindb_core::cluster::Ack,
        term: u64,
        from: u64,
        timeout: std::time::Duration,
        keep_bytes: usize,
        catchup_bps: usize,
    ) -> Peers {
        Peers {
            addrs,
            ack,
            term,
            at: std::sync::atomic::AtomicU64::new(from),
            timeout,
            journal: Journal::new(keep_bytes),
            throttle: Throttle::new(catchup_bps, std::time::Instant::now()),
            order: Mutex::new(()),
        }
    }

    /// Спрашивает копии, до какого номера они дошли, и подхватывает счёт.
    ///
    /// ## Зачем это нужно и что было без этого
    ///
    /// Свой исходящий номер primary в памяти и только там: писать его на диск
    /// пришлось бы `fsync`-ом на каждую пачку, то есть платить долговечностью
    /// счётчика по цене долговечности данных. Поэтому при подъёме он его не
    /// знает.
    ///
    /// Пока он начинал с единицы, перезапуск в том же терме уводил его в разрыв
    /// со всеми живыми копиями сразу: они на номере K, он шлёт первый — отказ,
    /// догон из свежего (пустого) кольца ничего не находит, и **вся** дальнейшая
    /// репликация прекращается. При `--ack=one` клиент этого даже не видит:
    /// primary считает себя сам и отвечает `200`.
    ///
    /// Зонд `at = 0` спрашивает у каждой копии её номер, ничего не применяя.
    /// Берётся наибольший: копия, отставшая от него, получит разрыв и пойдёт в
    /// догон — то есть окажется ровно в том положении, для которого догон и
    /// заведён. Молчащая копия в счёт не идёт: она и так догоняется потом.
    pub fn resume(&self) {
        let ask = format!("{{\"term\":{},\"at\":0}}", self.term);
        let mut best = 0u64;
        let seen: Vec<u64> = std::thread::scope(|scope| {
            let hs: Vec<_> = self
                .addrs
                .iter()
                .map(|addr| {
                    let ask = &ask;
                    scope.spawn(move || {
                        let got = crate::httpc::post_json(addr, "/_replicate", ask, self.timeout);
                        match got {
                            Ok(r) if r.code == 200 => {
                                serde_json::from_str::<serde_json::Value>(&r.body)
                                    .ok()
                                    .and_then(|v| v.get("upto").and_then(|x| x.as_u64()))
                            }
                            // `409` — нас сместили. Счёт тут ни при чём, и
                            // подхватывать нечего: писать мы всё равно не
                            // сможем, и скажет об этом первая же запись.
                            _ => None,
                        }
                    })
                })
                .collect();
            hs.into_iter().filter_map(|h| h.join().ok()).flatten().collect()
        });
        for one in seen {
            best = best.max(one);
        }
        if best > 0 {
            self.at.store(best, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Сколько держателей всего, считая primary.
    pub fn holders(&self) -> usize {
        self.addrs.len() + 1
    }

    /// Сколько подтверждений нужно по договору.
    pub fn need(&self) -> usize {
        let n = self.holders();
        match self.ack {
            svindb_core::cluster::Ack::One => 1.min(n),
            svindb_core::cluster::Ack::Quorum => n / 2 + 1,
            svindb_core::cluster::Ack::All => n,
        }
    }

    /// Рассылает **уже применённую** primary запись и считает подтверждения.
    ///
    /// ## Почему считается применённое, а не задуманное
    ///
    /// Реплика получает исход, а не запрос: условия замены primary уже
    /// разобрал, и переразбирать их на реплике значило бы дать двум узлам
    /// возможность рассудить одно условие по-разному. Тогда расхождение не
    /// заметил бы никто — оба ответили бы «принято».
    ///
    /// ## Почему все реплики опрашиваются, даже когда договор уже выполнен
    ///
    /// `one` выполняется одним primary, но реплика, которой не отправили,
    /// осталась бы с дырой в нумерации и отвергла бы **следующую** запись. То
    /// есть экономия одного запроса стоила бы отказа всей копии.
    pub fn send(&self, record: &str) -> Acked {
        let _order = self.order.lock().unwrap_or_else(|e| e.into_inner());
        let (got, ahead) = self.send_once(record);
        if !ahead {
            return got;
        }
        // Копия оказалась **впереди** нас.
        //
        // Так бывает после перезапуска primary, когда при подъёме ни одна копия
        // не отвечала и подхватить счёт было не у кого: он начал с нуля, а копии
        // остались на своём номере. Догон тут бессмыслен — досылать нечего, у нас
        // этих записей нет, — и молчать нельзя: без поправки репликация
        // прекратилась бы навсегда, а `--ack=one` отвечал бы `200`.
        //
        // Поэтому счёт подхватывается **сейчас** и запись уходит заново. Ровно
        // один раз: если и после переспроса кто-то впереди, это уже не отставший
        // подъём, а расхождение, и оно обязано быть видно отказом.
        self.resume();
        self.send_once(record).0
    }

    /// Одна попытка рассылки. Второе значение — «кто-то из копий впереди нас».
    fn send_once(&self, record: &str) -> (Acked, bool) {
        let at = self.at.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let body = format!("{{\"term\":{},\"at\":{at},{}", self.term, &record[1..]);
        self.journal.keep(at, &body);
        let mut missing: Vec<(String, String)> = Vec::new();
        let done: Vec<(usize, Result<(), String>, bool)> = std::thread::scope(|scope| {
            let hs: Vec<_> = self
                .addrs
                .iter()
                .enumerate()
                .map(|(i, addr)| {
                    let body = &body;
                    scope.spawn(move || {
                        let (r, ahead) = self.deliver(addr, body, at);
                        (i, r, ahead)
                    })
                })
                .collect();
            hs.into_iter().filter_map(|h| h.join().ok()).collect()
        });
        // Primary себя считает сам: он применил до рассылки, и не считать его
        // значило бы объявить `one` невыполнимым на шарде без живых реплик.
        let mut got = 1usize;
        let mut ahead = false;
        let mut by: Vec<Option<String>> = (0..self.addrs.len()).map(|_| None).collect();
        for (i, r, was_ahead) in done {
            ahead |= was_ahead;
            match r {
                Ok(()) => got += 1,
                Err(e) => by[i] = Some(e),
            }
        }
        for (i, why) in by.into_iter().enumerate() {
            if let Some(why) = why {
                missing.push((self.addrs[i].clone(), why));
            }
        }
        (Acked { got, of: self.holders(), need: self.need(), missing }, ahead)
    }

    /// Одна доставка: попытка, при разрыве — догон, и повтор той же записи.
    ///
    /// Догон запускается **отказом реплики**, а не опросом по времени. Опрос
    /// стоил бы запроса к каждой копии независимо от того, отстала ли она;
    /// отказ приходит ровно тогда, когда догонять есть что, и приносит с собой
    /// число, с которого догонять.
    fn deliver(&self, addr: &str, body: &str, at: u64) -> (Result<(), String>, bool) {
        match self.once(addr, body) {
            Ok(()) => (Ok(()), false),
            Err(Sent::Refused(e)) => (Err(e), false),
            // Копия впереди нас: досылать нечего, поправлять надо счёт. Решает
            // это `send`, потому что новый номер меняет тело, общее для всех.
            Err(Sent::Behind { upto, why }) if upto >= at => {
                ((Err(format!("{why}; копия впереди нас ({upto} против {at})"))), true)
            }
            Err(Sent::Behind { upto, why }) => {
                // Догон возобновляем по построению: он всегда идёт от того
                // числа, которое реплика назвала **сейчас**. Оборвался на
                // середине — следующая запись принесёт новое число, и догон
                // продолжится с него, а не начнётся заново.
                match self.catch_up(addr, upto) {
                    Err(e) => (Err(format!("{why}; догон не удался: {e}")), false),
                    Ok(sent) => match self.once(addr, body) {
                        Ok(()) => (Ok(()), false),
                        Err(Sent::Refused(e) | Sent::Behind { why: e, .. }) => {
                            (Err(format!("{e} (догнали {sent} записей, не хватило)")), false)
                        }
                    },
                }
            }
        }
    }

    fn once(&self, addr: &str, body: &str) -> Result<(), Sent> {
        match crate::httpc::post_json(addr, "/_replicate", body, self.timeout) {
            Ok(r) if (200..300).contains(&r.code) => Ok(()),
            Ok(r) if r.code == 409 => {
                let why = format!("ответил 409 — {}", first_line(&r.body));
                // Отстала — это `upto` в ответе; смещены мы — `upto` тоже есть,
                // но догон бессмыслен, и различает их наличие слова о терме.
                let stale_term = r.body.contains("устаревший терм");
                match serde_json::from_str::<serde_json::Value>(&r.body)
                    .ok()
                    .and_then(|v| v.get("upto").and_then(|x| x.as_u64()))
                {
                    Some(upto) if !stale_term => Err(Sent::Behind { upto, why }),
                    _ => Err(Sent::Refused(why)),
                }
            }
            Ok(r) => Err(Sent::Refused(format!("ответил {} — {}", r.code, first_line(&r.body)))),
            Err(e) => Err(Sent::Refused(e)),
        }
    }

    /// Досылает копии всё, что она пропустила, по порядку и в пределах полосы.
    ///
    /// Возвращает число досланных записей либо причину, по которой догон
    /// невозможен. «Невозможен» — это отдельный исход, а не ноль досланных:
    /// выпавшее из кольца нельзя восполнить пересылкой, и делать вид, что можно,
    /// значило бы оставить копию с дырой, о которой все замолчали.
    fn catch_up(&self, addr: &str, from: u64) -> Result<usize, String> {
        if !self.journal.has(from) {
            return Err(format!(
                "записи после {from} уже вытеснены из кольца — нужен перенос сегментов"
            ));
        }
        let mut sent = 0usize;
        for (at, body) in self.journal.since(from) {
            self.throttle.pass(body.len());
            match self.once(addr, &body) {
                Ok(()) => sent += 1,
                Err(Sent::Refused(e) | Sent::Behind { why: e, .. }) => {
                    return Err(format!("на записи {at}: {e}"));
                }
            }
        }
        Ok(sent)
    }
}

/// Исход одной попытки доставки. Отдельный тип, потому что «отстала» и
/// «отвергла» требуют от отправителя **разного**, а не разной формулировки.
enum Sent {
    Refused(String),
    Behind { upto: u64, why: String },
}

/// Первая строка чужого ответа — для сообщения о недостаче.
fn first_line(s: &str) -> String {
    let one = s.lines().next().unwrap_or("").trim();
    if one.chars().count() > 150 {
        one.chars().take(150).collect::<String>() + "…"
    } else {
        one.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn dir(tag: &str) -> String {
        let p = std::env::temp_dir().join(format!("svindb-терм-{tag}-{}", std::process::id()));
        std::fs::remove_dir_all(&p).ok();
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().to_string()
    }

    /// Смещённый primary останавливается репликой, а не собственной совестью.
    #[test]
    fn a_lower_term_is_refused() {
        let d = dir("ниже");
        let t = Terms::open(&d);
        assert!(t.accept(7, 1).is_ok());
        assert_eq!(
            t.accept(6, 2),
            Err(Refuse::StaleTerm { seen: 7, got: 6 }),
            "запись прежнего терма принята — это и есть split brain"
        );
    }

    /// Терм переживает перезапуск. Иначе защита исчезает ровно в том случае,
    /// ради которого заведена.
    #[test]
    fn the_term_outlives_the_process() {
        let d = dir("рестарт");
        {
            let t = Terms::open(&d);
            t.accept(9, 1).unwrap();
        }
        let again = Terms::open(&d);
        assert_eq!(again.seen(), Seen { term: 9, upto: 1 });
        assert!(matches!(again.accept(8, 2), Err(Refuse::StaleTerm { .. })));
    }

    /// Повтор той же записи безвреден: отправитель не знает, дошло ли.
    #[test]
    fn a_repeat_of_the_same_record_is_not_a_gap() {
        let d = dir("повтор");
        let t = Terms::open(&d);
        t.accept(3, 1).unwrap();
        t.accept(3, 2).unwrap();
        assert_eq!(
            t.accept(3, 2),
            Ok((Seen { term: 3, upto: 2 }, false)),
            "повтор объявлен разрывом либо предложен к повторному применению"
        );
        assert_eq!(t.seen().upto, 2);
    }

    /// Пропущенная запись названа разрывом, а не принята молча.
    #[test]
    fn a_missed_record_is_named_a_gap() {
        let d = dir("разрыв");
        let t = Terms::open(&d);
        t.accept(3, 1).unwrap();
        assert_eq!(
            t.accept(3, 3),
            Err(Refuse::Gap { have: 1, next: 3 }),
            "реплика приняла запись через дыру и стала выглядеть целой"
        );
        // И состояние не сдвинулось: отвергнутое не считается применённым.
        assert_eq!(t.seen().upto, 1);
    }

    /// Новый терм начинает счёт заново: чужой нумерации он не знает.
    ///
    /// Прежний терм мог уйти сколь угодно далеко — новый primary об этом не
    /// знает и знать не обязан, поэтому его первая запись имеет номер один, а не
    /// «следующий за чужим».
    #[test]
    fn a_new_term_starts_its_own_count() {
        let d = dir("новый-терм");
        let t = Terms::open(&d);
        for at in 1..=40u64 {
            t.accept(3, at).unwrap();
        }
        assert_eq!(t.seen(), Seen { term: 3, upto: 40 });
        assert_eq!(
            t.accept(4, 1),
            Ok((Seen { term: 4, upto: 1 }, true)),
            "новому терму отказали в его собственной первой записи"
        );
    }

    /// Договор подтверждения виден в ответе, а не подразумевается.
    #[test]
    fn the_contract_is_stated_in_the_reply() {
        let a =
            Acked { got: 1, of: 3, need: 2, missing: vec![("б:2".into(), "молчит".into())] };
        assert!(!a.enough());
        let s = a.suffix(svindb_core::cluster::Ack::Quorum, |x| format!("\"{x}\""));
        assert!(s.contains("\"ack\":\"quorum\""), "{s}");
        assert!(s.contains("\"acked\":1"), "{s}");
        assert!(s.contains("\"of\":3"), "{s}");
        assert!(s.contains("б:2"), "недостача не названа поимённо: {s}");
    }
    /// Кольцо отдаёт хвост по возрастанию и признаётся, когда нужное вытеснено.
    #[test]
    fn the_ring_admits_what_it_dropped() {
        let j = Journal::new(64);
        for at in 1..=8u64 {
            j.keep(at, &"x".repeat(16));
        }
        // Держится не всё: кольцо на 64 байта при записях по 16 хранит четыре.
        assert!(!j.has(0), "кольцо обещает догнать то, чего в нём нет");
        assert!(j.has(6), "кольцо отказывается догонять то, что в нём есть");
        let tail: Vec<u64> = j.since(6).into_iter().map(|(at, _)| at).collect();
        assert_eq!(tail, vec![7, 8], "хвост не по возрастанию либо не тот");
    }

    /// Кольцо не выбрасывает последнюю запись, даже если она одна больше предела.
    ///
    /// Иначе один крупный документ опустошал бы кольцо целиком, и догон
    /// становился бы невозможен сразу после самой обычной записи.
    #[test]
    fn one_big_record_does_not_empty_the_ring() {
        let j = Journal::new(8);
        j.keep(1, &"y".repeat(1000));
        assert!(j.has(0), "крупная запись вытеснила саму себя");
        assert_eq!(j.since(0).len(), 1);
    }

    /// Ограничитель считает окном в секунду, а не средним за всё время.
    #[test]
    fn the_throttle_counts_a_window_not_an_average() {
        let t = Throttle::new(1000, std::time::Instant::now());
        let started = std::time::Instant::now();
        t.pass(400);
        t.pass(400);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "ограничитель придержал 800 из 1000"
        );
        t.pass(400);
        // Третий проход перебрал предел — он обязан был подождать.
        assert!(
            started.elapsed() >= Duration::from_millis(500),
            "1200 байт при пределе 1000 прошли без задержки за {:?}",
            started.elapsed()
        );
    }

    /// Без предела ограничитель ничего не задерживает.
    #[test]
    fn a_zero_limit_is_no_limit() {
        let t = Throttle::new(0, std::time::Instant::now());
        let started = std::time::Instant::now();
        for _ in 0..100 {
            t.pass(1 << 20);
        }
        assert!(started.elapsed() < Duration::from_millis(50));
    }
    /// Подъём терма принимает только первую запись терма.
    ///
    /// Пока принимался любой номер, копия, отсутствовавшая в момент вступления
    /// нового primary, брала его N-ю запись за первую и объявляла применённым
    /// всё, чего никогда не видела. Разрыв при этом исчезал навсегда: номера
    /// сходились, и обнаружитель молчал.
    #[test]
    fn a_term_bump_accepts_only_the_first_record_of_that_term() {
        let d = dir("подъём-терма");
        let t = Terms::open(&d);
        t.accept(1, 1).unwrap();
        t.accept(1, 2).unwrap();
        assert_eq!(
            t.accept(2, 40),
            Err(Refuse::Gap { have: 0, next: 40 }),
            "копия приняла сороковую запись нового терма за первую"
        );
        // И состояние не сдвинулось: отвергнутое не считается применённым.
        assert_eq!(t.seen(), Seen { term: 1, upto: 2 });
        // А первая запись терма — принимается.
        assert_eq!(t.accept(2, 1), Ok((Seen { term: 2, upto: 1 }, true)));
    }

    /// Отметка применённого не двигается, пока применение не состоялось.
    ///
    /// `admit` пускает запись к применению и **не** объявляет её применённой.
    /// Обратный порядок прятал дыру при каждом отказе применения: номер на диске
    /// сходился, и разрыва больше никто не видел.
    #[test]
    fn admitting_is_not_applying() {
        let d = dir("допуск");
        let t = Terms::open(&d);
        assert_eq!(t.admit(1, 1), Ok(true));
        assert_eq!(t.seen().upto, 0, "допуск объявил запись применённой");
        // Тот же номер можно допустить снова: применение не состоялось.
        assert_eq!(t.admit(1, 1), Ok(true), "повторный допуск непринятой записи отвергнут");
        t.done(1, 1).unwrap();
        assert_eq!(t.seen().upto, 1);
        // А вот теперь тот же номер — повтор, и применять его второй раз нельзя.
        assert_eq!(t.admit(1, 1), Ok(false));
    }

    /// Терм всё равно ложится на диск до применения: он и есть защита.
    #[test]
    fn the_term_lands_before_the_write_is_applied() {
        let d = dir("терм-раньше");
        {
            let t = Terms::open(&d);
            t.accept(5, 1).unwrap();
            // Допуск более высокого терма — без применения.
            assert_eq!(t.admit(9, 1), Ok(true));
        }
        // Перезапуск: терм 9 виден, хотя запись так и не была применена.
        let again = Terms::open(&d);
        assert_eq!(again.seen().term, 9, "терм не пережил допуск без применения");
        assert!(matches!(again.admit(5, 2), Err(Refuse::StaleTerm { .. })));
    }
}
