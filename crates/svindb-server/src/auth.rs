//! Кто обращается к узлу и что ему позволено.

use crate::config::{Auth, Privilege};
use crate::route::{Endpoint, Route};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use svindb_core::hash;
use svindb_core::json::Json;

const FAILURE_WINDOW: Duration = Duration::from_secs(10);
const MAX_FAILURES_PER_WINDOW: u32 = 12;
const MAX_FAILURE_SOURCES: usize = 4096;

/// Чем кончилась проверка.
pub enum Verdict {
    /// Пропустить. Имя — для журнала.
    Allow(String),
    /// Не представился: 401 с `WWW-Authenticate`.
    Anonymous,
    /// Представился, но прав не хватает: 403.
    Forbidden { who: String, need: Privilege },
    /// Источник либо общий бюджет дорогостоящих проверок исчерпан.
    Throttled,
}

enum Identity {
    Found(String, Vec<Privilege>),
    Missing,
    Throttled,
}

#[derive(Clone, Copy)]
struct Failures {
    since: Instant,
    count: u32,
}

struct WorkPermit<'a>(&'a AtomicUsize);

impl Drop for WorkPermit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// Токен API.
///
/// Секрет лежит **хешем**, как и пароль: файл токенов читают те же бэкапы. Хеш
/// здесь дешёвый — один SHA-256 без повторов, — и это не небрежность: секрет
/// рождается из тридцати двух случайных байтов, подбирать его перебором
/// бессмысленно при любой цене попытки. Повторы нужны там, где секрет придумал
/// человек.
#[derive(Debug, Clone)]
pub struct Key {
    pub id: String,
    pub name: String,
    pub secret_sha: String,
    pub roles: Vec<String>,
    /// Мгновение истечения, миллисекунды от начала эпохи. Ноль — бессрочный.
    pub expires: u64,
    pub revoked: bool,
    pub created: u64,
}

impl Key {
    fn to_json(&self) -> Json {
        Json::Obj(vec![
            ("id".into(), Json::Str(self.id.clone())),
            ("name".into(), Json::Str(self.name.clone())),
            ("secret_sha".into(), Json::Str(self.secret_sha.clone())),
            ("roles".into(), Json::Arr(self.roles.iter().map(|r| Json::Str(r.clone())).collect())),
            ("expires".into(), Json::Int(self.expires as i64)),
            ("revoked".into(), Json::Bool(self.revoked)),
            ("created".into(), Json::Int(self.created as i64)),
        ])
    }

    fn from_json(v: &Json) -> Option<Key> {
        let unsigned = |name: &str| u64::try_from(v.get(name)?.as_i64()?).ok();
        Some(Key {
            id: v.get("id")?.as_str()?.to_string(),
            name: v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            secret_sha: v.get("secret_sha")?.as_str()?.to_string(),
            roles: v
                .get("roles")
                .and_then(|x| x.as_arr())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            expires: unsigned("expires")?,
            revoked: v.get("revoked").and_then(|x| x.as_bool()).unwrap_or(false),
            created: unsigned("created")?,
        })
    }

    pub fn alive(&self, now: u64) -> bool {
        !self.revoked && (self.expires == 0 || self.expires > now)
    }

    /// Вид для выдачи наружу — без хеша секрета.
    pub fn public(&self) -> Json {
        Json::Obj(vec![
            ("id".into(), Json::Str(self.id.clone())),
            ("name".into(), Json::Str(self.name.clone())),
            ("roles".into(), Json::Arr(self.roles.iter().map(|r| Json::Str(r.clone())).collect())),
            ("expires".into(), Json::Int(self.expires as i64)),
            ("revoked".into(), Json::Bool(self.revoked)),
            ("created".into(), Json::Int(self.created as i64)),
        ])
    }
}

/// Проверяющий: настройки прав плюс хранилище токенов.
pub struct Guard {
    pub auth: Auth,
    keys: std::sync::Mutex<Vec<Key>>,
    keys_file: Option<String>,
    dummy_hash: String,
    auth_work: AtomicUsize,
    max_auth_work: usize,
    failures: std::sync::Mutex<HashMap<IpAddr, Failures>>,
}

impl Guard {
    pub fn new(auth: Auth) -> Result<Guard, String> {
        if !(10_000..=hash::MAX_PASSWORD_ITERATIONS).contains(&auth.iterations) {
            return Err(format!(
                "auth.iterations должно быть между 10000 и {}",
                hash::MAX_PASSWORD_ITERATIONS
            ));
        }
        for user in &auth.users {
            let Some(cost) = hash::password_iterations(&user.hash) else {
                return Err(format!("у пользователя {:?} негодный PBKDF2-хеш", user.name));
            };
            if cost < 10_000 {
                return Err(format!("у пользователя {:?} PBKDF2 cost меньше 10000", user.name));
            }
            if cost != auth.iterations {
                return Err(format!(
                    "у пользователя {:?} PBKDF2 cost {cost}, а auth.iterations = {}; одинаковая стоимость нужна, чтобы время входа не выдавало имена",
                    user.name, auth.iterations
                ));
            }
        }
        let keys_file = auth.keys_file.clone();
        let keys = match keys_file.as_deref() {
            Some(p) if std::path::Path::new(p).exists() => load_keys(p)?,
            _ => Vec::new(),
        };
        let dummy_hash = hash::password_hash("пароль отсутствующего пользователя", auth.iterations);
        let max_auth_work =
            std::thread::available_parallelism().map_or(1, usize::from).clamp(1, 32);
        Ok(Guard {
            auth,
            keys: std::sync::Mutex::new(keys),
            keys_file,
            dummy_hash,
            auth_work: AtomicUsize::new(0),
            max_auth_work,
            failures: std::sync::Mutex::new(HashMap::new()),
        })
    }

    pub fn on(&self) -> bool {
        self.auth.on()
    }

    /// Проверяет запрос против нужного права.
    pub fn check(&self, header: Option<&str>, need: Option<Privilege>) -> Verdict {
        self.check_from(header, need, None)
    }

    /// То же, но с адресом источника для ограничения дорогостоящих попыток.
    pub fn check_from(
        &self,
        header: Option<&str>,
        need: Option<Privilege>,
        peer: Option<IpAddr>,
    ) -> Verdict {
        let Some(need) = need else { return Verdict::Allow("аноним".into()) };
        if !self.on() {
            return Verdict::Allow("аноним".into());
        }
        let Some(header) = header else { return Verdict::Anonymous };
        let (who, have) = match self.identify(header, peer) {
            Identity::Found(who, have) => (who, have),
            Identity::Missing => return Verdict::Anonymous,
            Identity::Throttled => return Verdict::Throttled,
        };
        if have.contains(&need) || have.contains(&Privilege::Admin) {
            Verdict::Allow(who)
        } else {
            Verdict::Forbidden { who, need }
        }
    }

    /// Разбирает `Authorization`. Неверный пароль и несуществующий
    /// пользователь дают одно и то же — «не представился»: разные ответы
    /// сообщали бы перебирающему, какие имена в системе есть.
    fn identify(&self, header: &str, peer: Option<IpAddr>) -> Identity {
        let Some((kind, value)) = header.split_once(' ') else { return Identity::Missing };
        let Some(raw) = hash::unb64(value.trim()) else { return Identity::Missing };
        let Ok(text) = String::from_utf8(raw) else { return Identity::Missing };
        let Some((id, secret)) = text.split_once(':') else { return Identity::Missing };
        if kind.eq_ignore_ascii_case("basic") {
            if peer.is_some_and(|p| self.source_throttled(p)) {
                return Identity::Throttled;
            }
            let Some(_permit) = self.reserve_work() else { return Identity::Throttled };
            // Отсутствующее имя проходит ту же PBKDF2 с тем же числом повторов,
            // что и настоящее. Ответ и вычислительная стоимость поэтому не
            // выдают перечень пользователей.
            let user = self.auth.users.iter().find(|u| u.name == id);
            let candidate = user.map_or(self.dummy_hash.as_str(), |u| u.hash.as_str());
            let valid = hash::password_check(candidate, secret);
            if !valid || user.is_none() {
                if let Some(p) = peer {
                    self.record_failure(p);
                }
                return Identity::Missing;
            }
            if let Some(p) = peer {
                self.clear_failures(p);
            }
            let user = user.expect("проверено выше");
            Identity::Found(user.name.clone(), self.auth.privileges_of(user))
        } else if kind.eq_ignore_ascii_case("apikey") {
            let Ok(keys) = self.keys.lock() else { return Identity::Missing };
            let Some(key) = keys.iter().find(|k| k.id == id) else { return Identity::Missing };
            if !key.alive(now_ms()) {
                return Identity::Missing;
            }
            let Some(want) = hash::unb64(&key.secret_sha) else { return Identity::Missing };
            if !hash::same(hash::sha256(secret.as_bytes()).as_slice(), &want) {
                return Identity::Missing;
            }
            let mut out = Vec::new();
            for r in &key.roles {
                if let Some(role) = self.auth.roles.iter().find(|x| &x.name == r) {
                    for p in &role.privileges {
                        if !out.contains(p) {
                            out.push(*p);
                        }
                    }
                }
            }
            Identity::Found(format!("токен {}", key.name), out)
        } else {
            Identity::Missing
        }
    }

    fn reserve_work(&self) -> Option<WorkPermit<'_>> {
        let mut current = self.auth_work.load(Ordering::Acquire);
        loop {
            if current >= self.max_auth_work {
                return None;
            }
            match self.auth_work.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(WorkPermit(&self.auth_work)),
                Err(next) => current = next,
            }
        }
    }

    fn source_throttled(&self, peer: IpAddr) -> bool {
        let Ok(mut all) = self.failures.lock() else { return true };
        let now = Instant::now();
        all.retain(|_, f| now.duration_since(f.since) < FAILURE_WINDOW);
        all.get(&peer).is_some_and(|f| f.count >= MAX_FAILURES_PER_WINDOW)
    }

    fn record_failure(&self, peer: IpAddr) {
        let Ok(mut all) = self.failures.lock() else { return };
        let now = Instant::now();
        if all.len() >= MAX_FAILURE_SOURCES && !all.contains_key(&peer) {
            all.retain(|_, f| now.duration_since(f.since) < FAILURE_WINDOW);
            if all.len() >= MAX_FAILURE_SOURCES {
                return;
            }
        }
        let f = all.entry(peer).or_insert(Failures { since: now, count: 0 });
        if now.duration_since(f.since) >= FAILURE_WINDOW {
            *f = Failures { since: now, count: 1 };
        } else {
            f.count = f.count.saturating_add(1);
        }
    }

    fn clear_failures(&self, peer: IpAddr) {
        if let Ok(mut all) = self.failures.lock() {
            all.remove(&peer);
        }
    }

    /// Заводит токен. Секрет отдаётся **один раз** — в хранилище ложится хеш.
    pub fn issue(
        &self,
        name: &str,
        roles: Vec<String>,
        ttl_ms: u64,
    ) -> Result<(Key, String), String> {
        for r in &roles {
            if !self.auth.roles.iter().any(|x| &x.name == r) {
                return Err(format!("роли {r:?} нет"));
            }
        }
        if roles.is_empty() {
            return Err("токен без ролей ничего не сможет".into());
        }
        let id = hash::b64(&hash::entropy(12)).replace(['+', '/', '='], "");
        let secret = hash::b64(&hash::entropy(32)).replace(['+', '/', '='], "");
        let now = now_ms();
        let key = Key {
            id: id.clone(),
            name: name.to_string(),
            secret_sha: hash::b64(&hash::sha256(secret.as_bytes())),
            roles,
            expires: if ttl_ms == 0 {
                0
            } else {
                now.checked_add(ttl_ms).ok_or("срок токена выходит за u64")?
            },
            revoked: false,
            created: now,
        };
        {
            let mut keys = self.keys.lock().map_err(|_| "хранилище токенов заперто")?;
            let mut next = keys.clone();
            next.push(key.clone());
            self.save(&next)?;
            *keys = next;
        }
        Ok((key, secret))
    }

    /// Отзывает токен. Запись остаётся: удалять её значило бы разрешить выдать
    /// тот же идентификатор заново, а журналы доступа ссылаются на него.
    pub fn revoke(&self, id: &str) -> Result<bool, String> {
        let mut keys = self.keys.lock().map_err(|_| "хранилище токенов заперто")?;
        let mut next = keys.clone();
        let Some(k) = next.iter_mut().find(|k| k.id == id) else { return Ok(false) };
        if k.revoked {
            return Ok(false);
        }
        k.revoked = true;
        self.save(&next)?;
        *keys = next;
        Ok(true)
    }

    pub fn list(&self) -> Vec<Key> {
        self.keys.lock().map(|k| k.clone()).unwrap_or_default()
    }

    fn save(&self, keys: &[Key]) -> Result<(), String> {
        let Some(path) = self.keys_file.as_deref() else {
            return Err(
                "не задан auth.keys_file — токен некуда положить, он пропал бы при перезапуске"
                    .into(),
            );
        };
        let doc = Json::Obj(vec![
            ("format".into(), Json::Int(1)),
            ("keys".into(), Json::Arr(keys.iter().map(|k| k.to_json()).collect())),
        ]);
        svindb_core::fs::atomic_write(std::path::Path::new(path), doc.to_pretty().as_bytes(), 0o600)
            .map_err(|e| format!("{path}: {e}"))
    }
}

fn load_keys(path: &str) -> Result<Vec<Key>, String> {
    let bytes =
        svindb_core::fs::read_regular_limited(std::path::Path::new(path), 64 * 1024 * 1024, true)
            .map_err(|e| format!("{path}: {e}"))?;
    let text = String::from_utf8(bytes).map_err(|e| format!("{path}: {e}"))?;
    let doc = svindb_core::json::parse(&text).map_err(|e| format!("{path}: {e}"))?;
    let arr = doc.get("keys").and_then(|x| x.as_arr()).ok_or(format!("{path}: нет списка keys"))?;
    let mut out = Vec::new();
    for v in arr {
        out.push(Key::from_json(v).ok_or(format!("{path}: запись токена неполна"))?);
    }
    Ok(out)
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Какое право нужно для разобранной цели. `None` — путь открыт.
///
/// ## Почему по перечислению, а не по строке
///
/// Раньше здесь сравнивались суффиксы, а `serve` сравнивал путь точно. Способы
/// расходились, и вместе с ними расходились решения: `POST /_bulk?refresh=true`
/// не совпадал ни с одним суффиксом (строка запроса — часть цели), получал
/// право **читателя** и уходил в поиск. Так пишет всякий клиент
/// Elasticsearch. Разбор цели вынесен в [`crate::route`], и теперь права и
/// выдача читают одно значение.
///
/// ## Почему без ветви `_`
///
/// Умолчание «незнакомому — чтение» осталось, но стало явной ветвью
/// [`Endpoint::Unknown`]. Новая точка маршрутизации **не соберётся**, пока для
/// неё не написано право, — забыть её больше нельзя.
pub fn needed(method: &str, r: &Route) -> Option<Privilege> {
    let writing = matches!(method, "POST" | "PUT" | "DELETE");
    match r.endpoint {
        // Живость узла открыта намеренно: её спрашивает балансировщик, у
        // которого учётной записи нет, а состав индекса она не выдаёт.
        Endpoint::Health => None,
        Endpoint::CatIndices | Endpoint::CatAliases => Some(Privilege::Read),
        Endpoint::Index => Some(if writing { Privilege::Admin } else { Privilege::Read }),
        Endpoint::Metrics | Endpoint::Schema | Endpoint::Validate => Some(Privilege::Read),
        // Сведения об узле и чтение одного документа — то же право, что и
        // поиск: обе точки только читают то, что читатель и так может достать
        // запросом.
        Endpoint::Stats | Endpoint::DocGet | Endpoint::MGet => Some(Privilege::Read),
        // Настройки — наравне с правами и снимками: без этого любой читатель
        // менял бы нарезку сегментов. `/_inspect` здесь же: он называет пути на
        // диске и раскладку файлов, то есть говорит про хозяйство узла, а не
        // про данные.
        // Приём репликации — право записи, а не своё особое. Отдельное право
        // соблазнительно («это же служебное»), но оно означало бы токен,
        // которым можно писать в обход всех проверок записи, и стоил бы такой
        // токен ровно столько же, сколько токен записи.
        Endpoint::Replicate => Some(Privilege::Write),
        // Сужение полосы — право администратора, а не записи. Оно удаляет
        // документы **по принадлежности**, то есть тем же одним запросом может
        // выкосить половину корпуса; токен приёма такого делать не должен.
        Endpoint::Reshard => Some(Privilege::Admin),
        Endpoint::Settings
        | Endpoint::Snapshot
        | Endpoint::Security
        | Endpoint::Inspect
        | Endpoint::Open
        | Endpoint::Close
        | Endpoint::Rollover => Some(Privilege::Admin),
        Endpoint::Alias | Endpoint::Metadata | Endpoint::Blocks => {
            Some(if writing { Privilege::Admin } else { Privilege::Read })
        }
        // Право спрашивается у точки, а метод лишь подтверждает: `GET /_bulk`
        // ничего не пишет и записи не требует.
        Endpoint::Bulk
        | Endpoint::Put
        | Endpoint::Mutate
        | Endpoint::Delete
        | Endpoint::Flush
        | Endpoint::Doc => Some(if writing { Privilege::Write } else { Privilege::Read }),
        Endpoint::Search => Some(Privilege::Read),
        Endpoint::Unknown => Some(Privilege::Read),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Role, User};

    fn guard(tmp: &str) -> Guard {
        let auth = Auth {
            users: vec![
                User {
                    name: "чтец".into(),
                    hash: hash::password_hash("чтец-пароль", 10_000),
                    roles: vec!["r".into()],
                },
                User {
                    name: "главный".into(),
                    hash: hash::password_hash("главный-пароль", 10_000),
                    roles: vec!["a".into()],
                },
            ],
            roles: vec![
                Role { name: "r".into(), privileges: vec![Privilege::Read] },
                Role { name: "a".into(), privileges: vec![Privilege::Admin] },
            ],
            keys_file: Some(tmp.to_string()),
            iterations: 10_000,
        };
        Guard::new(auth).unwrap()
    }

    fn basic(user: &str, pass: &str) -> String {
        format!("Basic {}", hash::b64(format!("{user}:{pass}").as_bytes()))
    }

    fn tmpfile(tag: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("svindb-keys-{tag}-{}.json", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_file(&p).ok();
        p
    }

    #[test]
    fn without_a_key_it_is_401_with_the_wrong_one_too() {
        let g = guard(&tmpfile("401"));
        assert!(matches!(g.check(None, Some(Privilege::Read)), Verdict::Anonymous));
        assert!(matches!(
            g.check(Some(&basic("чтец", "не тот")), Some(Privilege::Read)),
            Verdict::Anonymous
        ));
        // Несуществующий пользователь неотличим от неверного пароля — иначе
        // перебирающий узнавал бы список имён.
        assert!(matches!(
            g.check(Some(&basic("такого нет", "чтец-пароль")), Some(Privilege::Read)),
            Verdict::Anonymous
        ));
    }

    #[test]
    fn the_right_key_passes_and_the_wrong_privilege_is_403() {
        let g = guard(&tmpfile("403"));
        assert!(matches!(
            g.check(Some(&basic("чтец", "чтец-пароль")), Some(Privilege::Read)),
            Verdict::Allow(_)
        ));
        assert!(matches!(
            g.check(Some(&basic("чтец", "чтец-пароль")), Some(Privilege::Write)),
            Verdict::Forbidden { .. }
        ));
        // Управляющему позволено всё, включая чтение.
        assert!(matches!(
            g.check(Some(&basic("главный", "главный-пароль")), Some(Privilege::Write)),
            Verdict::Allow(_)
        ));
    }

    #[test]
    fn a_token_works_until_it_is_revoked() {
        let path = tmpfile("revoke");
        let g = guard(&path);
        let (key, secret) = g.issue("сборщик", vec!["r".into()], 0).unwrap();
        let head = format!("ApiKey {}", hash::b64(format!("{}:{secret}", key.id).as_bytes()));
        assert!(matches!(g.check(Some(&head), Some(Privilege::Read)), Verdict::Allow(_)));
        assert!(g.revoke(&key.id).unwrap());
        assert!(matches!(g.check(Some(&head), Some(Privilege::Read)), Verdict::Anonymous));
        // Второй отзыв ничего не меняет и не врёт, что отозвал.
        assert!(!g.revoke(&key.id).unwrap());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn an_expired_token_stops_working() {
        let path = tmpfile("expiry");
        let g = guard(&path);
        let (key, secret) = g.issue("недолгий", vec!["r".into()], 1).unwrap();
        let head = format!("ApiKey {}", hash::b64(format!("{}:{secret}", key.id).as_bytes()));
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(matches!(g.check(Some(&head), Some(Privilege::Read)), Verdict::Anonymous));
        std::fs::remove_file(&path).ok();
    }

    /// Секрет не лежит в файле. Проверяется буквально: файл читается как текст,
    /// и секрета в нём нет.
    #[test]
    fn the_secret_is_never_written_down() {
        let path = tmpfile("secret");
        let g = guard(&path);
        let (_, secret) = g.issue("тайный", vec!["r".into()], 0).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains(&secret), "секрет попал в файл токенов");
        std::fs::remove_file(&path).ok();
    }

    /// Токены переживают перезапуск: иначе их пришлось бы заводить заново после
    /// каждого обновления узла.
    #[test]
    fn tokens_survive_a_restart() {
        let path = tmpfile("restart");
        let (id, secret) = {
            let g = guard(&path);
            let (k, s) = g.issue("долгий", vec!["r".into()], 0).unwrap();
            (k.id, s)
        };
        let g = guard(&path);
        let head = format!("ApiKey {}", hash::b64(format!("{id}:{secret}").as_bytes()));
        assert!(matches!(g.check(Some(&head), Some(Privilege::Read)), Verdict::Allow(_)));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn failed_key_persistence_never_changes_live_authorization() {
        let missing = std::env::temp_dir()
            .join(format!("svindb-no-parent-{}/keys.json", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(std::path::Path::new(&missing).parent().unwrap()).ok();
        let g = guard(&missing);
        assert!(g.issue("не сохранится", vec!["r".into()], 0).is_err());
        assert!(g.list().is_empty(), "несохранённый токен появился в памяти");

        let path = tmpfile("revoke-failure");
        let g = guard(&path);
        let (key, secret) = g.issue("живой", vec!["r".into()], 0).unwrap();
        let head = format!("ApiKey {}", hash::b64(format!("{}:{secret}", key.id).as_bytes()));
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(g.revoke(&key.id).is_err());
        assert!(matches!(g.check(Some(&head), Some(Privilege::Read)), Verdict::Allow(_)));
        std::fs::remove_dir(&path).ok();
    }

    #[test]
    fn failed_basic_attempts_are_bounded_per_source() {
        let g = guard(&tmpfile("rate"));
        let peer: std::net::IpAddr = "192.0.2.10".parse().unwrap();
        let wrong = basic("чтец", "не тот");
        for _ in 0..MAX_FAILURES_PER_WINDOW {
            assert!(matches!(
                g.check_from(Some(&wrong), Some(Privilege::Read), Some(peer)),
                Verdict::Anonymous
            ));
        }
        assert!(matches!(
            g.check_from(Some(&wrong), Some(Privilege::Read), Some(peer)),
            Verdict::Throttled
        ));
    }

    /// Короткая запись: разобрать цель и спросить право.
    fn need(method: &str, target: &str) -> Option<Privilege> {
        needed(method, &Route::parse(method, target))
    }

    /// Открытых путей ровно столько, сколько перечислено. Всё прочее требует
    /// прав — в том числе то, чего ещё нет.
    #[test]
    fn everything_but_the_named_paths_needs_a_privilege() {
        assert_eq!(need("GET", "/_health"), None);
        for (m, p) in [
            ("GET", "/индекс/_search"),
            ("POST", "/индекс/_search"),
            ("GET", "/"),
            ("GET", "/_завтра_добавят"),
            ("POST", "/что-то-новое"),
        ] {
            assert!(need(m, p).is_some(), "{m} {p} остался открытым");
        }
        assert_eq!(need("POST", "/индекс/_bulk"), Some(Privilege::Write));
        assert_eq!(need("POST", "/_snapshot/ночной"), Some(Privilege::Admin));
        assert_eq!(need("POST", "/_security/api_key"), Some(Privilege::Admin));
        // Настройки меняют нарезку сегментов, поэтому наравне с правами и
        // снимками. Умолчание для незнакомого пути — `Read`, и `PUT` не
        // оканчивается ни на один суффикс записи: без явной строки в `needed`
        // любой читатель менял бы нарезку.
        assert_eq!(need("GET", "/_settings"), Some(Privilege::Admin));
        assert_eq!(need("PUT", "/_settings"), Some(Privilege::Admin));
        assert_eq!(need("GET", "/индекс/_metadata"), Some(Privilege::Read));
        assert_eq!(need("PUT", "/индекс/_metadata"), Some(Privilege::Admin));
        assert_eq!(need("GET", "/индекс/_blocks"), Some(Privilege::Read));
        assert_eq!(need("PUT", "/индекс/_block/write"), Some(Privilege::Admin));
        assert_eq!(need("GET", "/_alias/all"), Some(Privilege::Read));
        assert_eq!(need("POST", "/_aliases"), Some(Privilege::Admin));
        assert_eq!(need("POST", "/all/_rollover"), Some(Privilege::Admin));
    }

    /// Строка запроса не понижает право.
    ///
    /// Это та самая дыра, ради которой заведён [`crate::route`]. Прежде цель
    /// сравнивалась суффиксом, а строка запроса — часть цели, поэтому
    /// `POST /_bulk?refresh=true` не совпадал ни с одним суффиксом записи,
    /// получал право **читателя** и уходил в поиск. Так пишет всякий клиент
    /// Elasticsearch.
    #[test]
    fn a_query_string_does_not_lower_the_privilege() {
        for target in [
            "/_bulk?refresh=true",
            "/индекс/_bulk?refresh=wait_for",
            "/_delete?pretty",
            "/_flush?wait_for_completion=true",
        ] {
            assert_eq!(
                need("POST", target),
                Some(Privilege::Write),
                "{target}: право упало до чтения"
            );
        }
        // И у управляющих путей тоже: `/_settings?pretty` не должен становиться
        // читательским.
        assert_eq!(need("PUT", "/_settings?pretty"), Some(Privilege::Admin));
        assert_eq!(need("GET", "/_snapshot/ночной?pretty"), Some(Privilege::Admin));
    }

    /// Право и точка выдачи не расходятся ни на одной цели.
    ///
    /// Проверка двусторонняя, и в этом весь смысл: она требует **и** того,
    /// чтобы всякая пишущая точка требовала записи, **и** того, чтобы записи не
    /// требовал никто другой. Односторонняя прошла бы у разбора, который на
    /// всякий случай раздаёт `Write` всем подряд.
    #[test]
    fn the_privilege_and_the_endpoint_never_disagree() {
        let targets = [
            "/",
            "/_health",
            "/_metrics",
            "/_settings",
            "/_bulk",
            "/_put",
            "/_delete",
            "/_flush",
            "/_bulk?refresh=true",
            "/индекс/_bulk",
            "/индекс/_search",
            "/_snapshot/ночной",
            "/_security/api_key",
            "/_bulkX",
            "/что-то-новое",
        ];
        for t in targets {
            for m in ["GET", "POST", "PUT", "DELETE"] {
                let r = Route::parse(m, t);
                let writes = r.writes() && matches!(m, "POST" | "PUT" | "DELETE");
                let asked_write = needed(m, &r) == Some(Privilege::Write);
                assert_eq!(
                    writes, asked_write,
                    "{m} {t}: точка пишет — {writes}, а права спрошены как запись — {asked_write}"
                );
            }
        }
    }
}
