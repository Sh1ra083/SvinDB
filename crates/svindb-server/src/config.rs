//! Файл настроек узла.

use svindb_core::json::Json;

#[derive(Debug, Default, Clone)]
pub struct Config {
    pub bind: Option<String>,
    pub port: Option<u16>,
    pub threads: Option<usize>,
    pub cors: Cors,
    pub auth: Auth,
    pub snapshot: Snapshot,
    /// Общий бюджет CPU, памяти и фоновой свежести. Раздел разбирается
    /// строго: неизвестный ключ не должен незаметно оставить узел со старой
    /// admission-политикой.
    pub workload: Workload,
}

/// Производственный профиль единого WorkloadGovernor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workload {
    /// Число CPU units, доступных поиску. Ноль означает «до 12, но не больше
    /// фактической cgroup/affinity ширины».
    pub search_cpu_units: usize,
    /// Верхняя граница units одного interactive search.
    pub interactive_max_units: usize,
    /// Верхняя граница units одного analytics search.
    pub analytics_max_units: usize,
    /// Латентностный резерв в МиБ. Фактический runtime также применяет
    /// минимум 8% от memory.max.
    pub latency_reserve_mb: u64,
    /// Максимальный WAL tail, который можно держать до physical flush.
    pub wal_replay_mb: u64,
    /// Число frozen поколений до in-memory coalescing.
    pub max_frozen_memtables: usize,
    /// Политика фонового merge. Сейчас поддерживается только query_first.
    pub compaction_policy: String,
    /// Максимальная полоса одного фонового compaction, МиБ/с. Ноль отключает
    /// pacing только для лабораторных прогонов; production-профиль использует
    /// положительное значение.
    pub background_io_mbps: u64,
    /// Максимальный burst фонового I/O, МиБ.
    pub background_io_burst_mb: u64,
    /// Максимум одновременно отправленных фоновых I/O операций.
    pub background_io_max_inflight: usize,
    /// Максимальный объём распакованного immutable source cache в МиБ.
    /// Ноль отключает cache; отдельная запись крупнее одного МиБ всё равно
    /// никогда не помещается.
    pub source_cache_mb: u64,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            search_cpu_units: 12,
            interactive_max_units: 8,
            analytics_max_units: 4,
            latency_reserve_mb: 2_048,
            wal_replay_mb: 64,
            max_frozen_memtables: 32,
            compaction_policy: "query_first".to_string(),
            background_io_mbps: 64,
            background_io_burst_mb: 4,
            background_io_max_inflight: 2,
            source_cache_mb: 256,
        }
    }
}

/// Раздача заголовков доступа из браузера.
#[derive(Debug, Clone)]
pub struct Cors {
    /// Пусто — CORS выключен: браузер не получит ни одного заголовка доступа.
    pub origins: Vec<String>,
    /// Разрешать ли обмен с учётными данными (`cookie`, `Authorization`).
    pub credentials: bool,
    /// Сколько браузеру держать ответ на предполётный запрос, секунд.
    pub max_age: u64,
}

impl Default for Cors {
    fn default() -> Self {
        Cors { origins: Vec::new(), credentials: false, max_age: 600 }
    }
}

/// Право: что именно позволено роли.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    /// Поиск, чтение документа, состав индекса.
    Read,
    /// Приём и гашение.
    Write,
    /// Снимки, токены, всё управляющее.
    Admin,
}

impl Privilege {
    pub fn parse(s: &str) -> Option<Privilege> {
        match s {
            "read" => Some(Privilege::Read),
            "write" => Some(Privilege::Write),
            "admin" => Some(Privilege::Admin),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Privilege::Read => "read",
            Privilege::Write => "write",
            Privilege::Admin => "admin",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub privileges: Vec<Privilege>,
}

#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    /// Пароль в виде `pbkdf2$<повторов>$<соль64>$<ключ64>`. Открытого пароля в
    /// файле не бывает: файл читают бэкапы, системы раскладки настроек и всякий,
    /// кому дали посмотреть, «почему узел не поднимается».
    pub hash: String,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Auth {
    pub users: Vec<User>,
    pub roles: Vec<Role>,
    /// Где лежат токены API. Отдельно от настроек намеренно: настройки
    /// принадлежат тому, кто поднимает узел, а токены заводятся в работе, и
    /// узлу приходится писать в этот файл.
    pub keys_file: Option<String>,
    /// Повторов в PBKDF2. Больше — дороже подбор укравшему файл и дороже вход
    /// честному; величина стареет вместе с железом, поэтому она настройка.
    pub iterations: u32,
}

impl Default for Auth {
    fn default() -> Self {
        Auth { users: Vec::new(), roles: Vec::new(), keys_file: None, iterations: 210_000 }
    }
}

impl Auth {
    /// Проверка включена, если заведён хоть кто-то. Отдельного выключателя нет:
    /// он позволял бы файл с пользователями и `"enabled": false`, то есть узел,
    /// который выглядит защищённым и не защищён.
    pub fn on(&self) -> bool {
        !self.users.is_empty()
    }

    /// Права пользователя — объединение прав его ролей.
    pub fn privileges_of(&self, user: &User) -> Vec<Privilege> {
        let mut out = Vec::new();
        for r in &user.roles {
            if let Some(role) = self.roles.iter().find(|x| &x.name == r) {
                for p in &role.privileges {
                    if !out.contains(p) {
                        out.push(*p);
                    }
                }
            }
        }
        out
    }
}

/// Куда можно и куда нельзя писать снимки.
#[derive(Debug, Default, Clone)]
pub struct Snapshot {
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

impl Snapshot {
    /// Проверяет приёмник и отдаёт его канонический путь.
    ///
    /// Путь канонизируется **до** сверки: без этого `/mnt/архив/../../etc`
    /// прошёл бы разрешающий список по началу строки, а символическая ссылка
    /// внутри разрешённого каталога увела бы запись куда угодно.
    ///
    /// Пустой разрешающий список означает «никуда нельзя». Умолчание,
    /// запрещающее всё, при недонастроенном узле даёт отказ; умолчание,
    /// разрешающее всё, даёт запись в корень файловой системы.
    pub fn check(&self, dest: &str) -> Result<String, String> {
        let real = std::fs::canonicalize(dest)
            .map_err(|e| format!("приёмник {dest:?} не открылся: {e}"))?;
        if !real.is_dir() {
            return Err(format!("приёмник {dest:?} — не каталог"));
        }
        let real = real.to_string_lossy().to_string();
        if self.allow.is_empty() {
            return Err(
                "снимки никуда не разрешены: заполните snapshot.allow в файле настроек".into()
            );
        }
        // Запрет сильнее разрешения и проверяется первым: `deny` заводят, чтобы
        // вырезать подкаталог из разрешённого дерева, и обратный порядок сделал
        // бы его бесполезным.
        for d in &self.deny {
            if under(&real, d) {
                return Err(format!("приёмник {real:?} запрещён правилом {d:?}"));
            }
        }
        if self.allow.iter().any(|a| under(&real, a)) {
            Ok(real)
        } else {
            Err(format!("приёмник {real:?} не входит ни в один разрешённый путь"))
        }
    }

    /// Возвращает отдельный приёмник одного индекса внутри уже разрешённого
    /// корня снимков. Один и тот же `name` обязан быть законен у `orders` и у
    /// `users`, а общий `<dest>/<name>` склеил бы их в один namespace.
    pub fn scoped(&self, dest: &str, scope: &str, create: bool) -> Result<String, String> {
        svindb_core::fs::relative_components(scope)
            .map_err(|e| format!("scope снимка {scope:?} негоден: {e}"))?;
        let base = self.check(dest)?;
        let candidate = std::path::Path::new(&base).join("svindb-indices").join(scope);
        let candidate_text = candidate.to_string_lossy().to_string();
        // Проверяем запреты до создания: даже пустой служебный каталог не
        // должен появляться там, куда конфигурация явно запретила писать.
        for denied in &self.deny {
            if under(&candidate_text, denied) {
                return Err(format!("приёмник {candidate_text:?} запрещён правилом {denied:?}"));
            }
        }
        if create {
            svindb_core::fs::create_dir_all_real(&candidate)
                .map_err(|e| format!("приёмник {candidate_text:?} не создаётся: {e}"))?;
        }
        self.check(&candidate_text)
    }
}

/// Лежит ли `path` внутри `root` (или равен ему).
///
/// Правило тоже канонизируется, но если его нет на диске — берётся как есть.
/// Отсутствующий путь в запрещающем списке обязан продолжать запрещать: каталог
/// заведут завтра, а правило написано сегодня.
fn under(path: &str, root: &str) -> bool {
    let root = std::fs::canonicalize(root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| root.trim_end_matches('/').to_string());
    if root.is_empty() {
        return false;
    }
    if path == root {
        return true;
    }
    let with = if root.ends_with('/') { root } else { format!("{root}/") };
    path.starts_with(&with)
}

/// Читает файл настроек.
pub fn load(path: &str) -> Result<Config, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("настройки {path:?}: {e}"))?;
    parse(&text).map_err(|e| format!("настройки {path:?}: {e}"))
}

pub fn parse(text: &str) -> Result<Config, String> {
    let root = svindb_core::json::parse(text)?;
    let kv = root.as_obj().ok_or("верхний уровень обязан быть объектом")?;
    let mut cfg = Config::default();
    for (k, v) in kv {
        match k.as_str() {
            "bind" => cfg.bind = Some(want_str(v, "bind")?.to_string()),
            "port" => cfg.port = Some(want_u32(v, "port")? as u16),
            "threads" => cfg.threads = Some(want_u32(v, "threads")? as usize),
            "cors" => cfg.cors = cors(v)?,
            "auth" => cfg.auth = auth(v)?,
            "snapshot" => cfg.snapshot = snapshot(v)?,
            "workload" => cfg.workload = workload(v)?,
            // Строка-примечание: единственный ключ, который можно не понимать.
            // Формата для примечаний у JSON нет, а объяснять настройку рядом с
            // ней людям нужно.
            "_" | "comment" => {}
            other => {
                return Err(unknown(
                    other,
                    &["bind", "port", "threads", "cors", "auth", "snapshot", "workload"],
                ))
            }
        }
    }
    Ok(cfg)
}

fn workload(v: &Json) -> Result<Workload, String> {
    let kv = v.as_obj().ok_or("workload обязан быть объектом")?;
    let mut out = Workload::default();
    for (k, v) in kv {
        match k.as_str() {
            "search_cpu_units" => {
                out.search_cpu_units = want_u32(v, "workload.search_cpu_units")? as usize
            }
            "interactive_max_units" => {
                out.interactive_max_units = want_u32(v, "workload.interactive_max_units")? as usize
            }
            "analytics_max_units" => {
                out.analytics_max_units = want_u32(v, "workload.analytics_max_units")? as usize
            }
            "latency_reserve_mb" => {
                out.latency_reserve_mb = want_u32(v, "workload.latency_reserve_mb")? as u64
            }
            "wal_replay_mb" => out.wal_replay_mb = want_u32(v, "workload.wal_replay_mb")? as u64,
            "max_frozen_memtables" => {
                out.max_frozen_memtables = want_u32(v, "workload.max_frozen_memtables")? as usize
            }
            "compaction_policy" => {
                out.compaction_policy = want_str(v, "workload.compaction_policy")?.to_string()
            }
            "background_io_mbps" => {
                out.background_io_mbps = want_u32(v, "workload.background_io_mbps")? as u64
            }
            "background_io_burst_mb" => {
                out.background_io_burst_mb = want_u32(v, "workload.background_io_burst_mb")? as u64
            }
            "background_io_max_inflight" => {
                out.background_io_max_inflight =
                    want_u32(v, "workload.background_io_max_inflight")? as usize
            }
            "source_cache_mb" => {
                out.source_cache_mb = want_u32(v, "workload.source_cache_mb")? as u64
            }
            "cpu" => workload_cpu(v, &mut out)?,
            "memory" => workload_memory(v, &mut out)?,
            "compaction" => workload_compaction(v, &mut out)?,
            "io" => workload_io(v, &mut out)?,
            "source_cache" => workload_source_cache(v, &mut out)?,
            other => {
                return Err(unknown(
                    other,
                    &[
                        "search_cpu_units",
                        "interactive_max_units",
                        "analytics_max_units",
                        "latency_reserve_mb",
                        "wal_replay_mb",
                        "max_frozen_memtables",
                        "compaction_policy",
                        "background_io_mbps",
                        "background_io_burst_mb",
                        "background_io_max_inflight",
                        "source_cache_mb",
                        "cpu",
                        "memory",
                        "compaction",
                        "io",
                        "source_cache",
                    ],
                ))
            }
        }
    }
    if out.search_cpu_units > 64 {
        return Err("workload.search_cpu_units выше безопасного предела 64".into());
    }
    if !(1..=8).contains(&out.interactive_max_units) {
        return Err("workload.interactive_max_units должен быть в диапазоне 1..=8".into());
    }
    if !(1..=4).contains(&out.analytics_max_units) {
        return Err("workload.analytics_max_units должен быть в диапазоне 1..=4".into());
    }
    if out.search_cpu_units > 0
        && out.interactive_max_units.saturating_add(out.analytics_max_units) > out.search_cpu_units
    {
        return Err(format!(
            "workload.cpu: interactive_max_units + analytics_max_units превышают search_cpu_units ({}/{}/{})",
            out.interactive_max_units, out.analytics_max_units, out.search_cpu_units
        ));
    }
    if !(128..=1_048_576).contains(&out.latency_reserve_mb) {
        return Err("workload.latency_reserve_mb должен быть в диапазоне 128..=1048576".into());
    }
    if !(1..=4_096).contains(&out.wal_replay_mb) {
        return Err("workload.wal_replay_mb должен быть в диапазоне 1..=4096".into());
    }
    if !(1..=32).contains(&out.max_frozen_memtables) {
        return Err("workload.max_frozen_memtables должен быть в диапазоне 1..=32".into());
    }
    if out.compaction_policy != "query_first" {
        return Err(format!(
            "workload.compaction_policy = {:?}; поддерживается только query_first",
            out.compaction_policy
        ));
    }
    if out.background_io_mbps > 4_096 {
        return Err("workload.io.background_mbps выше безопасного предела 4096".into());
    }
    if !(1..=64).contains(&out.background_io_burst_mb) {
        return Err("workload.io.burst_mb должен быть в диапазоне 1..=64".into());
    }
    if !(1..=8).contains(&out.background_io_max_inflight) {
        return Err("workload.io.max_inflight должен быть в диапазоне 1..=8".into());
    }
    if out.source_cache_mb > 16_384 {
        return Err("workload.source_cache_mb выше безопасного предела 16384".into());
    }
    Ok(out)
}

fn workload_object<'a>(v: &'a Json, name: &str) -> Result<&'a [(String, Json)], String> {
    v.as_obj().ok_or_else(|| format!("workload.{name} обязан быть объектом"))
}

fn workload_cpu(v: &Json, out: &mut Workload) -> Result<(), String> {
    for (key, value) in workload_object(v, "cpu")? {
        match key.as_str() {
            "search_cpu_units" => {
                out.search_cpu_units = want_u32(value, "workload.cpu.search_cpu_units")? as usize
            }
            "interactive_max_units" => {
                out.interactive_max_units =
                    want_u32(value, "workload.cpu.interactive_max_units")? as usize
            }
            "analytics_max_units" => {
                out.analytics_max_units =
                    want_u32(value, "workload.cpu.analytics_max_units")? as usize
            }
            other => {
                return Err(unknown(
                    other,
                    &["search_cpu_units", "interactive_max_units", "analytics_max_units"],
                ))
            }
        }
    }
    Ok(())
}

fn workload_memory(v: &Json, out: &mut Workload) -> Result<(), String> {
    for (key, value) in workload_object(v, "memory")? {
        match key.as_str() {
            "latency_reserve_mb" => {
                out.latency_reserve_mb =
                    want_u32(value, "workload.memory.latency_reserve_mb")? as u64
            }
            "wal_replay_mb" => {
                out.wal_replay_mb = want_u32(value, "workload.memory.wal_replay_mb")? as u64
            }
            "max_frozen_memtables" => {
                out.max_frozen_memtables =
                    want_u32(value, "workload.memory.max_frozen_memtables")? as usize
            }
            other => {
                return Err(unknown(
                    other,
                    &["latency_reserve_mb", "wal_replay_mb", "max_frozen_memtables"],
                ))
            }
        }
    }
    Ok(())
}

fn workload_compaction(v: &Json, out: &mut Workload) -> Result<(), String> {
    for (key, value) in workload_object(v, "compaction")? {
        match key.as_str() {
            "policy" | "compaction_policy" => {
                out.compaction_policy = want_str(value, "workload.compaction.policy")?.to_string()
            }
            other => return Err(unknown(other, &["policy", "compaction_policy"])),
        }
    }
    Ok(())
}

fn workload_io(v: &Json, out: &mut Workload) -> Result<(), String> {
    for (key, value) in workload_object(v, "io")? {
        match key.as_str() {
            "background_mbps" | "background_io_mbps" => {
                out.background_io_mbps = want_u32(value, "workload.io.background_mbps")? as u64
            }
            "burst_mb" | "background_io_burst_mb" => {
                out.background_io_burst_mb = want_u32(value, "workload.io.burst_mb")? as u64
            }
            "max_inflight" | "background_io_max_inflight" => {
                out.background_io_max_inflight =
                    want_u32(value, "workload.io.max_inflight")? as usize
            }
            other => return Err(unknown(other, &["background_mbps", "burst_mb", "max_inflight"])),
        }
    }
    Ok(())
}

fn workload_source_cache(v: &Json, out: &mut Workload) -> Result<(), String> {
    for (key, value) in workload_object(v, "source_cache")? {
        match key.as_str() {
            "max_mb" | "source_cache_mb" => {
                out.source_cache_mb = want_u32(value, "workload.source_cache.max_mb")? as u64
            }
            other => return Err(unknown(other, &["max_mb", "source_cache_mb"])),
        }
    }
    Ok(())
}

fn cors(v: &Json) -> Result<Cors, String> {
    let kv = v.as_obj().ok_or("cors обязан быть объектом")?;
    let mut out = Cors::default();
    for (k, v) in kv {
        match k.as_str() {
            "origins" => out.origins = want_strs(v, "cors.origins")?,
            "credentials" => out.credentials = want_bool(v, "cors.credentials")?,
            "max_age" => out.max_age = want_u32(v, "cors.max_age")? as u64,
            other => return Err(unknown(other, &["origins", "credentials", "max_age"])),
        }
    }
    // Звёздочка с учётными данными — настройка, которой браузер не подчинится:
    // при `credentials` он требует точный источник. Молча оставить её значило
    // бы отдать оператору неработающий доступ и повод искать ошибку в коде.
    if out.credentials && out.origins.iter().any(|o| o == "*") {
        return Err("cors: \"*\" несовместима с credentials — браузер такой ответ отвергнет; \
                    перечислите источники явно"
            .into());
    }
    Ok(out)
}

fn auth(v: &Json) -> Result<Auth, String> {
    let kv = v.as_obj().ok_or("auth обязан быть объектом")?;
    let mut out = Auth::default();
    for (k, v) in kv {
        match k.as_str() {
            "users" => {
                for u in want_arr(v, "auth.users")? {
                    out.users.push(user(u)?);
                }
            }
            "roles" => {
                for r in want_arr(v, "auth.roles")? {
                    out.roles.push(role(r)?);
                }
            }
            "keys_file" => out.keys_file = Some(want_str(v, "auth.keys_file")?.to_string()),
            "iterations" => out.iterations = want_u32(v, "auth.iterations")?,
            other => return Err(unknown(other, &["users", "roles", "keys_file", "iterations"])),
        }
    }
    // Пользователь со ссылкой на несуществующую роль остался бы без прав и
    // получал бы 403 на всё. Отказ при разборе называет причину сразу, а не
    // через час разбирательств «почему администратору нельзя».
    for u in &out.users {
        for r in &u.roles {
            if !out.roles.iter().any(|x| &x.name == r) {
                return Err(format!("auth: у пользователя {:?} роль {r:?}, которой нет", u.name));
            }
        }
        if u.roles.is_empty() {
            return Err(format!("auth: у пользователя {:?} нет ни одной роли", u.name));
        }
    }
    if out.iterations < 10_000 {
        return Err(format!(
            "auth.iterations = {} — слишком мало; подбор по украденному файлу станет дешёвым",
            out.iterations
        ));
    }
    if out.iterations > svindb_core::hash::MAX_PASSWORD_ITERATIONS {
        return Err(format!(
            "auth.iterations = {} — выше безопасного потолка {}",
            out.iterations,
            svindb_core::hash::MAX_PASSWORD_ITERATIONS
        ));
    }
    Ok(out)
}

fn user(v: &Json) -> Result<User, String> {
    let kv = v.as_obj().ok_or("auth.users: запись обязана быть объектом")?;
    let (mut name, mut hash, mut roles) = (String::new(), String::new(), Vec::new());
    for (k, v) in kv {
        match k.as_str() {
            "name" => name = want_str(v, "user.name")?.to_string(),
            "hash" => hash = want_str(v, "user.hash")?.to_string(),
            "roles" => roles = want_strs(v, "user.roles")?,
            other => return Err(unknown(other, &["name", "hash", "roles"])),
        }
    }
    if name.is_empty() {
        return Err("auth.users: запись без имени".into());
    }
    if !hash.starts_with("pbkdf2$") {
        return Err(format!(
            "auth.users[{name:?}]: пароль обязан быть хешем вида pbkdf2$повторов$соль$ключ, \
             а не открытым текстом"
        ));
    }
    Ok(User { name, hash, roles })
}

fn role(v: &Json) -> Result<Role, String> {
    let kv = v.as_obj().ok_or("auth.roles: запись обязана быть объектом")?;
    let (mut name, mut privileges) = (String::new(), Vec::new());
    for (k, v) in kv {
        match k.as_str() {
            "name" => name = want_str(v, "role.name")?.to_string(),
            "privileges" => {
                for p in want_strs(v, "role.privileges")? {
                    privileges.push(Privilege::parse(&p).ok_or_else(|| {
                        format!("auth.roles: право {p:?} неизвестно; есть read, write, admin")
                    })?);
                }
            }
            other => return Err(unknown(other, &["name", "privileges"])),
        }
    }
    if name.is_empty() {
        return Err("auth.roles: запись без имени".into());
    }
    Ok(Role { name, privileges })
}

fn snapshot(v: &Json) -> Result<Snapshot, String> {
    let kv = v.as_obj().ok_or("snapshot обязан быть объектом")?;
    let mut out = Snapshot::default();
    for (k, v) in kv {
        match k.as_str() {
            "allow" => out.allow = want_strs(v, "snapshot.allow")?,
            "deny" => out.deny = want_strs(v, "snapshot.deny")?,
            other => return Err(unknown(other, &["allow", "deny"])),
        }
    }
    for p in out.allow.iter().chain(out.deny.iter()) {
        if !p.starts_with('/') {
            return Err(format!(
                "snapshot: путь {p:?} обязан быть от корня — относительный зависит от того, \
                 откуда узел запущен"
            ));
        }
    }
    Ok(out)
}

fn unknown(got: &str, known: &[&str]) -> String {
    format!("ключ {got:?} неизвестен; ожидались: {}", known.join(", "))
}

fn want_str<'a>(v: &'a Json, at: &str) -> Result<&'a str, String> {
    v.as_str().ok_or_else(|| format!("{at}: ожидалась строка, а не {}", v.kind()))
}

fn want_bool(v: &Json, at: &str) -> Result<bool, String> {
    v.as_bool().ok_or_else(|| format!("{at}: ожидалось да/нет, а не {}", v.kind()))
}

fn want_u32(v: &Json, at: &str) -> Result<u32, String> {
    let n = v.as_i64().ok_or_else(|| format!("{at}: ожидалось целое, а не {}", v.kind()))?;
    u32::try_from(n).map_err(|_| format!("{at}: {n} вне допустимого"))
}

fn want_arr<'a>(v: &'a Json, at: &str) -> Result<&'a [Json], String> {
    v.as_arr().ok_or_else(|| format!("{at}: ожидался список, а не {}", v.kind()))
}

fn want_strs(v: &Json, at: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for x in want_arr(v, at)? {
        out.push(want_str(x, at)?.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_typo_is_refused_not_ignored() {
        let e = parse(r#"{"cors":{"crendentials":true}}"#).unwrap_err();
        assert!(e.contains("crendentials"), "опечатка не названа: {e}");
        let e = parse(r#"{"prot":19300}"#).unwrap_err();
        assert!(e.contains("prot") && e.contains("port"), "{e}");
    }

    #[test]
    fn workload_profile_round_trips_and_rejects_unsafe_values() {
        let cfg = parse(
            r#"{"workload":{"search_cpu_units":12,"interactive_max_units":8,
                "analytics_max_units":4,"latency_reserve_mb":2048,"wal_replay_mb":64,
                "max_frozen_memtables":32,"compaction_policy":"query_first"}}"#,
        )
        .unwrap();
        assert_eq!(cfg.workload, Workload::default());
        let err = parse(r#"{"workload":{"compaction_policy":"merge_first"}}"#).unwrap_err();
        assert!(err.contains("query_first"), "{err}");
        let err = parse(r#"{"workload":{"interactive_max_units":9}}"#).unwrap_err();
        assert!(err.contains("interactive_max_units"), "{err}");
        let err = parse(
            r#"{"workload":{"search_cpu_units":4,"interactive_max_units":3,"analytics_max_units":2}}"#,
        )
        .unwrap_err();
        assert!(err.contains("превышают"), "{err}");
        let err = parse(r#"{"workload":{"future":1}}"#).unwrap_err();
        assert!(err.contains("future"), "{err}");
        let cfg = parse(
            r#"{"workload":{"cpu":{"search_cpu_units":10,"interactive_max_units":6,"analytics_max_units":4},
                "memory":{"latency_reserve_mb":1024},
                "compaction":{"policy":"query_first"},
                "io":{"background_mbps":32,"burst_mb":8,"max_inflight":3}}}"#,
        )
        .unwrap();
        assert_eq!(cfg.workload.search_cpu_units, 10);
        assert_eq!(cfg.workload.latency_reserve_mb, 1024);
        assert_eq!(cfg.workload.background_io_mbps, 32);
        assert_eq!(cfg.workload.background_io_burst_mb, 8);
        assert_eq!(cfg.workload.background_io_max_inflight, 3);
    }

    #[test]
    fn a_plaintext_password_is_refused() {
        let cfg = r#"{"auth":{"roles":[{"name":"a","privileges":["admin"]}],
                      "users":[{"name":"вася","hash":"123456","roles":["a"]}]}}"#;
        let e = parse(cfg).unwrap_err();
        assert!(e.contains("открытым текстом"), "{e}");
    }

    #[test]
    fn a_role_that_does_not_exist_is_refused() {
        let cfg = r#"{"auth":{"users":[{"name":"вася","hash":"pbkdf2$1$a$b","roles":["нет"]}]}}"#;
        assert!(parse(cfg).unwrap_err().contains("которой нет"));
    }

    #[test]
    fn a_star_with_credentials_is_refused() {
        let e = parse(r#"{"cors":{"origins":["*"],"credentials":true}}"#).unwrap_err();
        assert!(e.contains("credentials"), "{e}");
        // Без учётных данных звёздочка законна.
        assert!(parse(r#"{"cors":{"origins":["*"]}}"#).is_ok());
    }

    /// Границы разрешающего списка: по составляющим пути, а не по началу строки.
    #[test]
    fn a_neighbour_directory_does_not_pass_as_the_allowed_one() {
        let base = std::env::temp_dir().join(format!("svindb-cfg-{}", std::process::id()));
        let ok = base.join("архив");
        let sub = ok.join("внутри");
        let secret = ok.join("секрет");
        let near = base.join("архив-чужой");
        for d in [&ok, &sub, &secret, &near] {
            std::fs::create_dir_all(d).unwrap();
        }
        let s = Snapshot {
            allow: vec![ok.to_string_lossy().to_string()],
            deny: vec![secret.to_string_lossy().to_string()],
        };
        assert!(s.check(&ok.to_string_lossy()).is_ok(), "сам разрешённый");
        assert!(s.check(&sub.to_string_lossy()).is_ok(), "внутри разрешённого");
        assert!(s.check(&near.to_string_lossy()).is_err(), "сосед по началу строки прошёл");
        assert!(s.check(&secret.to_string_lossy()).is_err(), "запрет не сработал");
        std::fs::remove_dir_all(&base).ok();
    }

    /// Побег точками не проходит: путь сперва канонизируется.
    #[test]
    fn dots_do_not_escape_the_allowed_root() {
        let base = std::env::temp_dir().join(format!("svindb-cfg-esc-{}", std::process::id()));
        let ok = base.join("архив");
        let out = base.join("наружу");
        for d in [&ok, &out] {
            std::fs::create_dir_all(d).unwrap();
        }
        let s = Snapshot { allow: vec![ok.to_string_lossy().to_string()], deny: vec![] };
        let sneaky = format!("{}/../наружу", ok.to_string_lossy());
        assert!(s.check(&sneaky).is_err(), "точки увели запись из разрешённого дерева");
        std::fs::remove_dir_all(&base).ok();
    }

    /// Пустой разрешающий список запрещает всё, а не разрешает.
    #[test]
    fn an_empty_allow_list_forbids_everything() {
        let s = Snapshot::default();
        let e = s.check("/tmp").unwrap_err();
        assert!(e.contains("никуда не разрешены"), "{e}");
    }
}
