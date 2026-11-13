//! Откуда берётся карта кластера: интерфейс и местная реализация.
//!
//! ## Что здесь есть
//!
//! Интерфейс [`Store`] из пяти операций — ровно тот, что назван в
//! `docs/adr/0001`, — и одна реализация поверх файла.
//!
//! ## Чем файловая реализация является и чем **не** является
//!
//! Она годится там, где все участники видят **одну файловую систему**: на одной
//! машине, в проверках, в разработке. Её сравнение-с-обменом стоит на
//! исключительном создании файла-замка и атомарном переименовании — и того, и
//! другого достаточно ровно до тех пор, пока файловая система одна и локальна.
//!
//! Она **не** распределённое хранилище. На NFS исключительное создание файла не
//! исключительно, а разделённые узлы просто не увидят чужих правок. Настоящему
//! кластеру нужен внешний хранитель за этим же интерфейсом — и в этом весь
//! смысл интерфейса: он узкий, и вторая реализация встаёт за него, не трогая
//! ничего построенного сверху.
//!
//! Сказано это здесь, а не в заметке на будущее, потому что разница между
//! «работает на моей машине» и «работает в кластере» тут ровно в этом абзаце.

use crate::cluster::Map;
use std::time::{Duration, SystemTime};

/// Почему хранилище отказало.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreErr {
    /// Карту изменили раньше: перечитать и повторить.
    Conflict { have: u64 },
    /// Аренда потеряна: держатель больше не primary.
    LeaseLost,
    /// Всё прочее — носитель, разбор, права.
    Broken(String),
}

impl std::fmt::Display for StoreErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreErr::Conflict { have } => write!(f, "карта уже поколения {have}"),
            StoreErr::LeaseLost => write!(f, "аренда потеряна"),
            StoreErr::Broken(e) => write!(f, "{e}"),
        }
    }
}

/// Право писать в шард, выданное на срок.
///
/// ## Почему на срок, а не навсегда
///
/// Право, которое нельзя потерять, не защищает ни от чего: отделённый узел
/// продолжал бы считать себя primary и писать в пустоту, пока кластер пишет в
/// другое место. Срок делает потерю связи **наблюдаемой изнутри**: не продлил —
/// перестал быть primary, и узнал об этом сам, ни у кого не спрашивая.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub shard: u32,
    pub node: String,
    /// Терм этого права. Растёт при каждой выдаче новому держателю.
    pub term: u64,
    /// Докуда действует — миллисекунды эпохи.
    pub until_ms: u64,
}

impl Lease {
    /// Действует ли ещё. Запас вычитается **у держателя**, а не прибавляется у
    /// выдающего: держатель обязан отказаться раньше, чем выдающий сочтёт срок
    /// истёкшим, иначе между ними есть окно, где primary двое.
    pub fn valid_at(&self, now_ms: u64, guard: Duration) -> bool {
        self.until_ms > now_ms.saturating_add(guard.as_millis() as u64)
    }
}

/// Пять операций, которых хватает всему распределённому слою.
pub trait Store: Send + Sync {
    /// Карта и её поколение.
    fn load(&self) -> Result<(Map, u64), StoreErr>;

    /// Записать карту, если поколение всё ещё `expect`. Отдаёт новое поколение.
    fn save(&self, next: &Map, expect: u64) -> Result<u64, StoreErr>;

    /// Взять или продлить право писать в шард.
    ///
    /// Один вызов на оба случая намеренно: «взять» и «продлить» отличаются лишь
    /// тем, был ли держатель прежним, и разводить их значило бы дать
    /// вызывающему выбрать неверный — а неверный здесь означает второго primary.
    fn lease(&self, shard: u32, node: &str, ttl: Duration) -> Result<Lease, StoreErr>;

    /// Отдать право добровольно. Отказ здесь не беда: срок истечёт сам.
    fn release(&self, l: &Lease) -> Result<(), StoreErr>;

    /// Изменилась ли карта с этого поколения. `None` — нет.
    ///
    /// Опрос, а не подписка: подписка потребовала бы своего потока и своего
    /// понятия «соединение живо», а опрос по тому же поколению даёт то же
    /// самое и переживает любой разрыв.
    fn changed_since(&self, gen: u64) -> Result<Option<(Map, u64)>, StoreErr>;
}

/// Текущее время в миллисекундах эпохи.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Реализация поверх файла.
// ---------------------------------------------------------------------------

/// Хранилище в одном файле под замком.
pub struct FileStore {
    dir: String,
}

const STATE: &str = "cluster.json";
const LOCK: &str = "cluster.lock";
const MAX_BYTES: u64 = 64 << 20;

/// `u64` хранится строкой: JSON-числа этого проекта представлены `i64`, и
/// приведение значения выше `i64::MAX` превращало поколение или терм в
/// отрицательное. Читатель принимает и старое положительное число.
fn json_u64(value: u64) -> crate::json::Json {
    crate::json::s(&value.to_string())
}

fn optional_u64(object: &crate::json::Json, key: &str) -> Result<Option<u64>, String> {
    match object.get(key) {
        None => Ok(None),
        Some(crate::json::Json::Int(value)) if *value >= 0 => Ok(Some(*value as u64)),
        Some(crate::json::Json::Str(value)) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("у {key:?} нет годного беззнакового числа: {value:?}")),
        Some(_) => Err(format!("у {key:?} нет годного беззнакового числа")),
    }
}

fn optional_u32(object: &crate::json::Json, key: &str) -> Result<Option<u32>, String> {
    optional_u64(object, key)?
        .map(|value| {
            u32::try_from(value).map_err(|_| format!("{key:?}={value} не помещается в u32"))
        })
        .transpose()
}

impl FileStore {
    pub fn new(dir: &str) -> FileStore {
        FileStore { dir: dir.to_string() }
    }

    /// Берёт замок исключительным созданием файла.
    ///
    /// Ожидание с отступом, а не вечное: замок держится микросекунды — чтение,
    /// правка, запись, — и очередь длиннее нескольких попыток означает не
    /// соперничество, а брошенный замок. О нём сказано отказом, а не молчаливым
    /// продолжением без замка.
    fn lock(&self) -> Result<LockHeld, StoreErr> {
        let path = format!("{}/{LOCK}", self.dir);
        for step in 0..200u32 {
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(_) => return Ok(LockHeld { path }),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    std::thread::sleep(Duration::from_millis(1 + u64::from(step) / 8));
                }
                Err(e) => return Err(StoreErr::Broken(format!("{path}: {e}"))),
            }
        }
        Err(StoreErr::Broken(format!("{path}: замок занят слишком долго")))
    }

    fn read_state(&self) -> Result<State, StoreErr> {
        let path = format!("{}/{STATE}", self.dir);
        let bytes =
            match crate::fs::read_regular_limited(std::path::Path::new(&path), MAX_BYTES, false) {
                Ok(b) => b,
                // Файла нет — это пустой кластер, а не поломка: первый же записавший
                // его и заведёт.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(State::default()),
                Err(e) => return Err(StoreErr::Broken(format!("{path}: {e}"))),
            };
        let text =
            String::from_utf8(bytes).map_err(|e| StoreErr::Broken(format!("{path}: {e}")))?;
        State::parse(&text).map_err(|e| StoreErr::Broken(format!("{path}: {e}")))
    }

    fn write_state(&self, s: &State) -> Result<(), StoreErr> {
        let path = format!("{}/{STATE}", self.dir);
        let body = s.to_json();
        if body.len() as u64 > MAX_BYTES {
            return Err(StoreErr::Broken(format!(
                "{path}: состояние весит {} Б при пределе {MAX_BYTES} Б — записанное, оно не прочтётся",
                body.len()
            )));
        }
        crate::fs::atomic_write(std::path::Path::new(&path), body.as_bytes(), 0o600)
            .map_err(|e| StoreErr::Broken(format!("{path}: {e}")))
    }
}

struct LockHeld {
    path: String,
}

impl Drop for LockHeld {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).ok();
    }
}

/// Всё, что лежит в файле: карта, поколение и аренды.
#[derive(Default)]
struct State {
    gen: u64,
    map: Map,
    leases: std::collections::BTreeMap<u32, Lease>,
}

impl State {
    fn to_json(&self) -> String {
        let shards: Vec<crate::json::Json> = self
            .map
            .shards
            .iter()
            .map(|(id, s)| {
                crate::json::obj(vec![
                    ("id", crate::json::Json::Int(*id as i64)),
                    (
                        "primary",
                        s.primary
                            .as_ref()
                            .map(|p| crate::json::s(p))
                            .unwrap_or(crate::json::Json::Null),
                    ),
                    (
                        "replicas",
                        crate::json::Json::Arr(
                            s.replicas.iter().map(|r| crate::json::s(r)).collect(),
                        ),
                    ),
                    ("term", json_u64(s.term)),
                    // Полоса ячеек — часть карты, а не производное от неё.
                    // Восстанови её из числа шардов, и карта после деления
                    // прочиталась бы как равное деление, то есть переселила бы
                    // весь корпус вместо одной половины.
                    ("lo", crate::json::Json::Int(s.lo as i64)),
                    ("hi", crate::json::Json::Int(s.hi as i64)),
                ])
            })
            .collect();
        let leases: Vec<crate::json::Json> = self
            .leases
            .values()
            .map(|l| {
                crate::json::obj(vec![
                    ("shard", crate::json::Json::Int(l.shard as i64)),
                    ("node", crate::json::s(&l.node)),
                    ("term", json_u64(l.term)),
                    ("until_ms", json_u64(l.until_ms)),
                ])
            })
            .collect();
        let mut out = crate::json::obj(vec![
            ("gen", json_u64(self.gen)),
            ("shards", crate::json::Json::Arr(shards)),
            ("leases", crate::json::Json::Arr(leases)),
        ])
        .to_pretty();
        out.push('\n');
        out
    }

    fn parse(text: &str) -> Result<State, String> {
        let v = crate::json::parse(text)?;
        let gen = optional_u64(&v, "gen")?.unwrap_or(0);
        let mut map = Map { shards: Default::default(), gen };
        match v.get("shards") {
            Some(crate::json::Json::Arr(arr)) => {
                if arr.len() > crate::shards::MAX_SHARDS {
                    return Err(format!(
                        "шардов {} при {} ячейках маршрутизации",
                        arr.len(),
                        crate::shards::SLOTS
                    ));
                }
                for one in arr {
                    let id = optional_u32(one, "id")?.ok_or("шард без номера")?;
                    let primary = match one.get("primary") {
                        None | Some(crate::json::Json::Null) => None,
                        Some(crate::json::Json::Str(value)) if !value.is_empty() => {
                            Some(value.clone())
                        }
                        Some(_) => return Err(format!("у шарда {id} негодный primary")),
                    };
                    let replicas = match one.get("replicas") {
                        Some(crate::json::Json::Arr(values)) => values
                            .iter()
                            .map(|value| {
                                value
                                    .as_str()
                                    .filter(|node| !node.is_empty())
                                    .map(str::to_string)
                                    .ok_or_else(|| format!("у шарда {id} негодная реплика"))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                        None => Vec::new(),
                        Some(_) => return Err(format!("у шарда {id} replicas не массив")),
                    };
                    let term = optional_u64(one, "term")?.unwrap_or(0);
                    // Карта без полос — с прежнего формата. Читается как равное
                    // деление: это то, чем она и была, и молчаливой перемены смысла
                    // здесь нет.
                    let n = arr.len().max(1) as u64;
                    let k = map.shards.len() as u64;
                    let lo = optional_u64(one, "lo")?.unwrap_or(k * crate::shards::SLOTS / n);
                    let hi = optional_u64(one, "hi")?.unwrap_or((k + 1) * crate::shards::SLOTS / n);
                    if map
                        .shards
                        .insert(id, crate::cluster::Shard { lo, hi, primary, replicas, term })
                        .is_some()
                    {
                        return Err(format!("шард {id} записан дважды"));
                    }
                }
            }
            None => {}
            Some(_) => return Err(String::from("shards обязан быть массивом")),
        }
        map.validate().map_err(|error| format!("негодная карта шардов: {error:?}"))?;
        let mut leases = std::collections::BTreeMap::new();
        match v.get("leases") {
            Some(crate::json::Json::Arr(arr)) => {
                for one in arr {
                    let shard = optional_u32(one, "shard")?.ok_or("аренда без шарда")?;
                    let node = one
                        .get("node")
                        .and_then(|x| x.as_str())
                        .filter(|node| !node.is_empty())
                        .ok_or("аренда без узла")?
                        .to_string();
                    let lease = Lease {
                        shard,
                        node,
                        term: optional_u64(one, "term")?.unwrap_or(0),
                        until_ms: optional_u64(one, "until_ms")?.unwrap_or(0),
                    };
                    if leases.insert(shard, lease).is_some() {
                        return Err(format!("аренда шарда {shard} записана дважды"));
                    }
                }
            }
            None => {}
            Some(_) => return Err(String::from("leases обязан быть массивом")),
        }
        Ok(State { gen, map, leases })
    }
}

impl Store for FileStore {
    fn load(&self) -> Result<(Map, u64), StoreErr> {
        let s = self.read_state()?;
        Ok((s.map, s.gen))
    }

    fn save(&self, next: &Map, expect: u64) -> Result<u64, StoreErr> {
        let _held = self.lock()?;
        let mut s = self.read_state()?;
        if s.gen != expect {
            return Err(StoreErr::Conflict { have: s.gen });
        }
        // Правила перехода проверяются **до** записи и здесь же: хранилище
        // умеет только сравнение поколений, а «терм не убывает» и «у одного
        // терма один primary» знает карта. Разбор — у `cluster::Map::check`.
        s.map.check(next).map_err(|e| StoreErr::Broken(format!("переход отвергнут: {e:?}")))?;
        s.gen = next.gen;
        s.map = next.clone();
        self.write_state(&s)?;
        Ok(s.gen)
    }

    fn lease(&self, shard: u32, node: &str, ttl: Duration) -> Result<Lease, StoreErr> {
        let _held = self.lock()?;
        let mut s = self.read_state()?;
        let now = now_ms();
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        let until_ms = now.saturating_add(ttl_ms);
        let got = match s.leases.get(&shard) {
            // Держит кто-то другой, и срок ещё идёт — отказ. Это и есть защита
            // от двух primary: второго не бывает, пока первый не отпустил или
            // не просрочил.
            Some(l) if l.node != node && l.until_ms > now => return Err(StoreErr::LeaseLost),
            // Свою продлеваем, не трогая терм: держатель тот же, право то же.
            Some(l) if l.node == node && l.until_ms > now => Lease { until_ms, ..l.clone() },
            // Свободна или просрочена — выдаём новому держателю и **двигаем
            // терм**: всё, что не успел подтвердить прежний, обесценивается.
            Some(l) => Lease {
                shard,
                node: node.to_string(),
                term: l.term.checked_add(1).ok_or_else(|| {
                    StoreErr::Broken(format!(
                        "терм аренды шарда {shard} достиг предела {}",
                        u64::MAX
                    ))
                })?,
                until_ms,
            },
            None => Lease { shard, node: node.to_string(), term: 1, until_ms },
        };
        s.leases.insert(shard, got.clone());
        self.write_state(&s)?;
        Ok(got)
    }

    fn release(&self, l: &Lease) -> Result<(), StoreErr> {
        let _held = self.lock()?;
        let mut s = self.read_state()?;
        // Отпускает только держатель того же терма: опоздавший отпуск чужой
        // аренды снял бы с шарда живого primary.
        if s.leases.get(&l.shard).is_some_and(|c| c.node == l.node && c.term == l.term) {
            // Запись **остаётся**, гаснет только срок.
            //
            // Убрать её целиком значило бы забыть терм, и следующий держатель
            // начал бы с единицы — то есть терм откатился бы назад, ровно то,
            // от чего аренда и заведена. Поймано собственным тестом: после
            // добровольного отпуска второй узел получал терм 1 вместо 2, и
            // подтверждения обоих становились неотличимы.
            s.leases.insert(l.shard, Lease { until_ms: 0, ..l.clone() });
            self.write_state(&s)?;
        }
        Ok(())
    }

    fn changed_since(&self, gen: u64) -> Result<Option<(Map, u64)>, StoreErr> {
        let s = self.read_state()?;
        if s.gen == gen {
            return Ok(None);
        }
        Ok(Some((s.map, s.gen)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::Shard;

    fn tmp(tag: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-store-{tag}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn one_shard() -> Map {
        let mut m = Map::default();
        m.shards.insert(0, Shard::whole(Some("a".into()), vec![]));
        m.gen = 1;
        m
    }

    /// Пустое хранилище — это пустой кластер, а не поломка.
    #[test]
    fn an_empty_store_is_an_empty_cluster() {
        let d = tmp("empty");
        let st = FileStore::new(&d);
        let (m, gen) = st.load().expect("пустое хранилище");
        assert_eq!(gen, 0);
        assert!(m.shards.is_empty());
        assert_eq!(st.changed_since(0).expect("опрос"), None, "пустое объявило изменение");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Запись переживает перечитывание побайтово по смыслу.
    #[test]
    fn what_was_written_reads_back() {
        let d = tmp("roundtrip");
        let st = FileStore::new(&d);
        let m = one_shard();
        assert_eq!(st.save(&m, 0).expect("первая запись"), 1);
        let (back, gen) = st.load().expect("чтение");
        assert_eq!(gen, 1);
        assert_eq!(back.shards[&0].primary.as_deref(), Some("a"));
        assert_eq!(back.shards, m.shards, "карта прочлась не той");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Полный диапазон u64 переживает JSON без превращения в отрицательное.
    #[test]
    fn full_width_generations_terms_and_deadlines_round_trip() {
        let mut leases = std::collections::BTreeMap::new();
        leases.insert(
            u32::MAX,
            Lease {
                shard: u32::MAX, node: "последний".into(), term: u64::MAX, until_ms: u64::MAX
            },
        );
        let state =
            State { gen: u64::MAX, map: Map { shards: Default::default(), gen: u64::MAX }, leases };
        let encoded = state.to_json();
        assert!(encoded.contains(&format!("\"{}\"", u64::MAX)), "u64 записан не строкой");
        let decoded = State::parse(&encoded).expect("полный u64 не прочитан");
        assert_eq!(decoded.gen, u64::MAX);
        assert_eq!(decoded.map.gen, u64::MAX);
        assert_eq!(decoded.leases[&u32::MAX].term, u64::MAX);
        assert_eq!(decoded.leases[&u32::MAX].until_ms, u64::MAX);

        // Прежний файл с положительными JSON-числами остаётся читаемым.
        let old = State::parse(r#"{"gen":7,"shards":[],"leases":[]}"#)
            .expect("старое положительное число отвергнуто");
        assert_eq!(old.gen, 7);
    }

    /// Отрицательные/повторные номера и сломанное покрытие не становятся картой.
    #[test]
    fn malformed_persistent_cluster_numbers_are_refused() {
        let cases = [
            r#"{"gen":0,"shards":[{"id":-1,"lo":0,"hi":4096}],"leases":[]}"#,
            r#"{"gen":0,"shards":[{"id":0,"lo":0,"hi":4096},{"id":0,"lo":0,"hi":4096}],"leases":[]}"#,
            r#"{"gen":0,"shards":[{"id":0,"lo":1,"hi":4096}],"leases":[]}"#,
            r#"{"gen":0,"shards":[],"leases":[{"shard":-1,"node":"a"}]}"#,
            r#"{"gen":0,"shards":[],"leases":[{"shard":1,"node":"a"},{"shard":1,"node":"b"}]}"#,
            r#"{"gen":"18446744073709551616","shards":[],"leases":[]}"#,
        ];
        for text in cases {
            assert!(State::parse(text).is_err(), "повреждённое состояние принято: {text}");
        }
    }

    /// Последний терм не выдаёт следующему primary право с термом ноль.
    #[test]
    fn an_exhausted_lease_term_is_a_stable_refusal() {
        let d = tmp("term-end");
        let st = FileStore::new(&d);
        let mut leases = std::collections::BTreeMap::new();
        leases.insert(7, Lease { shard: 7, node: "old".into(), term: u64::MAX, until_ms: 0 });
        st.write_state(&State { gen: 0, map: Map::default(), leases }).unwrap();
        let error =
            st.lease(7, "new", Duration::from_secs(1)).expect_err("терм после u64::MAX выдан");
        assert!(error.to_string().contains(&u64::MAX.to_string()), "невнятный отказ: {error}");
        let persisted = st.read_state().unwrap();
        assert_eq!(persisted.leases[&7].term, u64::MAX, "отказ изменил терм на диске");
        assert_eq!(persisted.leases[&7].node, "old", "отказ сменил держателя");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Опоздавший записывающий получает конфликт, а не переписывает чужое.
    #[test]
    fn a_late_writer_gets_a_conflict() {
        let d = tmp("cas");
        let st = FileStore::new(&d);
        st.save(&one_shard(), 0).expect("первая");
        // Второй писал по прежнему поколению.
        let mut later = one_shard();
        later.gen = 2;
        assert_eq!(st.save(&later, 0), Err(StoreErr::Conflict { have: 1 }), "опоздавший переписал");
        // А по нынешнему — проходит.
        let (now, gen) = st.load().unwrap();
        let next = now.with_primary(0, Some("b".into()));
        assert_eq!(st.save(&next, gen).expect("по нынешнему"), 2);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Хранилище не пропускает перехода, нарушающего правила карты.
    ///
    /// Само хранилище умеет только сравнение поколений; правила знает карта, и
    /// проверяются они **до** записи — иначе в файле оказалось бы состояние, из
    /// которого нет законного выхода.
    #[test]
    fn the_store_refuses_an_illegal_transition() {
        let d = tmp("illegal");
        let st = FileStore::new(&d);
        st.save(&one_shard(), 0).expect("первая");
        let (now, gen) = st.load().unwrap();
        // Primary сменился, а терм остался — двое на один терм.
        let mut bad = now.clone();
        bad.gen = gen + 1;
        bad.shards.get_mut(&0).unwrap().primary = Some("b".into());
        let got = st.save(&bad, gen);
        assert!(matches!(got, Err(StoreErr::Broken(_))), "незаконный переход записался: {got:?}");
        // И в файле осталось прежнее.
        assert_eq!(st.load().unwrap().0.shards[&0].primary.as_deref(), Some("a"));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Двое не бывают primary одного шарда одновременно.
    ///
    /// Это то самое, ради чего аренда и заведена: пока срок первого идёт,
    /// второй получает отказ, а не второе право.
    #[test]
    fn two_nodes_never_hold_one_shard_at_once() {
        let d = tmp("split");
        let st = FileStore::new(&d);
        let a = st.lease(0, "a", Duration::from_secs(30)).expect("первый взял");
        assert_eq!(a.term, 1);
        assert_eq!(st.lease(0, "b", Duration::from_secs(30)), Err(StoreErr::LeaseLost));

        // Продление своей не двигает терм: держатель тот же, право то же.
        let again = st.lease(0, "a", Duration::from_secs(30)).expect("продлил");
        assert_eq!(again.term, 1, "продление сменило терм");
        assert!(again.until_ms >= a.until_ms, "продление укоротило срок");

        // Отпустил — второй берёт, и терм растёт: всё неподтверждённое прежним
        // обесценивается.
        st.release(&again).expect("отпустил");
        let b = st.lease(0, "b", Duration::from_secs(30)).expect("второй взял");
        assert_eq!(b.term, 2, "новый держатель не сдвинул терм");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Просроченная аренда переходит к другому, и терм двигается.
    #[test]
    fn an_expired_lease_moves_on_with_a_new_term() {
        let d = tmp("expire");
        let st = FileStore::new(&d);
        let a = st.lease(0, "a", Duration::from_millis(1)).expect("взял");
        std::thread::sleep(Duration::from_millis(5));
        let b = st.lease(0, "b", Duration::from_secs(30)).expect("перехватил просроченную");
        assert_eq!(b.term, a.term + 1, "перехват не сдвинул терм");
        assert_eq!(b.node, "b");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Чужой отпуск не снимает живого primary.
    #[test]
    fn a_stale_release_does_not_unseat_the_living() {
        let d = tmp("release");
        let st = FileStore::new(&d);
        let a = st.lease(0, "a", Duration::from_millis(1)).expect("взял");
        std::thread::sleep(Duration::from_millis(5));
        let b = st.lease(0, "b", Duration::from_secs(30)).expect("перехватил");
        // Опоздавший «a» отпускает своё — но своего у него уже нет.
        st.release(&a).expect("отпуск не отказ");
        let after = st.lease(0, "b", Duration::from_secs(30)).expect("держатель на месте");
        assert_eq!(after.term, b.term, "чужой отпуск сдвинул терм живого");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Держатель отказывается от права **раньше**, чем выдающий сочтёт срок истёкшим.
    ///
    /// Без этого запаса между ними есть окно, в котором primary двое: у одного
    /// срок ещё «идёт», а хранилище уже отдало право другому.
    #[test]
    fn the_holder_steps_down_before_the_store_reassigns() {
        let l = Lease { shard: 0, node: "a".into(), term: 1, until_ms: 1_000 };
        // Запас в двести миллисекунд: за сто до конца держатель уже не считает
        // себя primary, а хранилище отдаст право только после тысячи.
        assert!(!l.valid_at(900, Duration::from_millis(200)), "держатель не сложил полномочий");
        assert!(l.valid_at(500, Duration::from_millis(200)), "сложил слишком рано");
        assert!(!l.valid_at(1_000, Duration::from_millis(0)), "истёкшая сочлась годной");
    }

    /// Опрос замечает изменение и молчит, когда его нет.
    #[test]
    fn polling_notices_a_change_and_only_a_change() {
        let d = tmp("watch");
        let st = FileStore::new(&d);
        st.save(&one_shard(), 0).expect("первая");
        assert_eq!(st.changed_since(1).expect("опрос"), None, "изменение объявлено без причины");
        let (now, gen) = st.load().unwrap();
        st.save(&now.with_primary(0, Some("b".into())), gen).expect("вторая");
        let (m, g) = st.changed_since(1).expect("опрос").expect("изменение не замечено");
        assert_eq!(g, 2);
        assert_eq!(m.shards[&0].primary.as_deref(), Some("b"));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Одновременные записи не теряют друг друга: кто-то один побеждает.
    #[test]
    fn concurrent_writers_do_not_lose_each_other() {
        let d = tmp("race");
        let st = std::sync::Arc::new(FileStore::new(&d));
        st.save(&one_shard(), 0).expect("первая");
        let wins = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        std::thread::scope(|sc| {
            for who in 0..8 {
                let (st, wins) = (std::sync::Arc::clone(&st), std::sync::Arc::clone(&wins));
                sc.spawn(move || {
                    let (now, gen) = st.load().expect("чтение");
                    let node = format!("узел{who}");
                    if st.save(&now.with_primary(0, Some(node)), gen).is_ok() {
                        wins.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                });
            }
        });
        let won = wins.load(std::sync::atomic::Ordering::SeqCst);
        assert!(won >= 1, "не победил никто");
        // Поколение сдвинулось ровно на число победителей: ни одна победа не
        // затёрла другую.
        let (_, gen) = st.load().unwrap();
        assert_eq!(gen as usize, 1 + won, "поколение разошлось с числом побед");
        std::fs::remove_dir_all(&d).ok();
    }
}
