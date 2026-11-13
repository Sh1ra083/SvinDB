//! Долговечный каталог нескольких индексов одного процесса.
//!
//! Здесь нет поисковых структур и писателей. Модуль отвечает только за то,
//! чтобы имя индекса однозначно соответствовало безопасному каталогу, а
//! создание и удаление не оставляли после падения правдоподобный полуиндекс.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use super::{open_local_index, LocalIndexOptions, Node};
use crate::catalog::{self, Catalog, CatalogError, State};

/// Отравленный замок — это паника чужого потока, а не отказ хранилища.
/// В выпуске с `panic = "abort"` его не бывает вовсе.
fn busy(why: &str) -> CatalogError {
    CatalogError::ResourceBusy { why: why.to_string() }
}

/// Имя marker не меняется вместе с форматом намеренно.
///
/// Версия внутри файла уже проверяется на точное равенство, поэтому узел 1.2.0
/// сам откажется открыть `format: 2` и скажет про формат. Переименуй мы файл —
/// тот же узел ушёл бы в ветку «marker отсутствует» и обвинил бы каталог в том,
/// что он старый одноиндексный. Правильный отказ важнее аккуратного имени.
const LAYOUT_FILE: &str = "multi-index-v1.json";
const INDICES: &str = "indices";
const CREATING: &str = ".creating";
const TRASH: &str = ".trash";
const CATALOG: &str = "catalog";
const META: &str = "index.json";
/// Формат, который пишется. `2` добавил durable-каталог с aliases и блоками.
const VERSION: u32 = 2;
/// Самый старый формат, который умеем открыть и обновить.
const VERSION_MIN: u32 = 1;
const META_LIMIT: u64 = 16 * 1024;
static UNIQUE: AtomicU64 = AtomicU64::new(1);

/// Как трактовать каталог до запуска HTTP-runtime.
///
/// Это не содержимое marker, а безопасная классификация верхнего уровня:
/// новый каталог получает современный multi-index layout, опубликованный
/// multi-root открывается тем же runtime, а существующий одноиндексный каталог
/// сохраняет прежний договор без миграции и без потери видимости данных.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayoutKind {
    New,
    Multi,
    Legacy,
}

/// Определяет layout, ничего не создавая и не исправляя.
///
/// Служебные каталоги или незавершённая атомарная запись marker считаются
/// признаками multi-root даже без финального marker. Тогда [`Root::open`]
/// либо безопасно завершит восстановление, либо откажет; передавать такой
/// каталог старому одноиндексному runtime нельзя — тот мог бы дописать в корень
/// собственные schema/WAL и превратить диагностируемую аварию в смешанный
/// формат.
pub fn classify(path: impl AsRef<Path>) -> Result<LayoutKind, String> {
    let path = path.as_ref();
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(LayoutKind::New),
        Err(error) => {
            return Err(format!("каталог данных {} не проверяется: {error}", path.display()))
        }
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(format!("каталог данных {} не является каталогом", path.display()));
        }
        Ok(_) => {}
    }
    if fs::symlink_metadata(path.join(LAYOUT_FILE)).is_ok() {
        return Ok(LayoutKind::Multi);
    }

    let mut any = false;
    for entry in fs::read_dir(path)
        .map_err(|e| format!("каталог данных {} не читается: {e}", path.display()))?
    {
        let entry = entry.map_err(|e| format!("элемент {} не читается: {e}", path.display()))?;
        any = true;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { return Ok(LayoutKind::Legacy) };
        if matches!(name, INDICES | CREATING | TRASH | CATALOG) || marker_temp(name) {
            return Ok(LayoutKind::Multi);
        }
    }
    Ok(if any { LayoutKind::Legacy } else { LayoutKind::New })
}

/// Корень многoиндексного узла.
#[derive(Clone, Debug)]
pub struct Root {
    path: PathBuf,
    /// Формат, найденный на диске. `1` означает корень 1.2.0, ждущий импорта.
    format: u32,
}

impl Root {
    /// Открывает или впервые создаёт layout.
    ///
    /// Непустой каталог без marker не принимается: это почти наверняка старый
    /// одноиндексный каталог. Принять его как корень и создать `indices/`
    /// означало бы поднять пустой кластер рядом с существующими данными.
    pub fn open(path: impl AsRef<Path>, writable: bool) -> Result<Root, String> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            if !writable {
                return Err(format!(
                    "корня нескольких индексов {} нет, а режим только чтение запрещает его создавать",
                    path.display()
                ));
            }
            fs::create_dir_all(&path).map_err(|e| {
                format!("корень нескольких индексов {} не создаётся: {e}", path.display())
            })?;
        }
        if !path.is_dir() {
            return Err(format!("корень нескольких индексов {} не каталог", path.display()));
        }
        let marker = path.join(LAYOUT_FILE);
        let format = if marker.exists() {
            let raw =
                svindb_core::fs::read_regular_limited(&marker, META_LIMIT, false).map_err(|e| {
                    format!("marker нескольких индексов {} не читается: {e}", marker.display())
                })?;
            let layout: serde_json::Value = serde_json::from_slice(&raw).map_err(|e| {
                format!("marker нескольких индексов {} повреждён: {e}", marker.display())
            })?;
            let found = layout.get("format").and_then(serde_json::Value::as_u64);
            // Полоса, а не точное равенство: `1` — это корень, созданный 1.2.0,
            // и мы обязаны уметь его открыть, чтобы обновить. Более новый формат
            // по-прежнему отвергается — тем же способом, каким 1.2.0 отвергнет
            // наш `2`.
            match found {
                Some(v) if (u64::from(VERSION_MIN)..=u64::from(VERSION)).contains(&v) => v as u32,
                other => {
                    return Err(format!(
                        "формат нескольких индексов {} не поддерживается: {other:?}, читаются \
                         {VERSION_MIN}–{VERSION}",
                        marker.display()
                    ))
                }
            }
        } else {
            if writable {
                sweep_marker_temps(&path)?;
            }
            let mut entries = fs::read_dir(&path).map_err(|e| {
                format!("корень нескольких индексов {} не читается: {e}", path.display())
            })?;
            if entries.next().is_some() {
                return Err(format!(
                    "{} не является корнем нескольких индексов: marker {LAYOUT_FILE} отсутствует, а каталог не пуст. Старый одноиндексный каталог распознаётся автоматически; уберите устаревший --multi-index",
                    path.display()
                ));
            }
            if !writable {
                return Err(format!(
                    "в {} нет marker {LAYOUT_FILE}, а режим только чтение запрещает его создавать",
                    path.display()
                ));
            }
            write_json_atomic(&path, LAYOUT_FILE, &serde_json::json!({ "format": VERSION }))?;
            VERSION
        };
        for child in [INDICES, CREATING, TRASH, CATALOG] {
            let dir = path.join(child);
            if writable {
                fs::create_dir_all(&dir).map_err(|e| {
                    format!("служебный каталог {} не создаётся: {e}", dir.display())
                })?;
            } else if !dir.is_dir() && child == INDICES {
                return Err(format!("каталог индексов {} отсутствует", dir.display()));
            }
        }
        let root = Root { path, format };
        if writable {
            root.sweep_incomplete()?;
        }
        Ok(root)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Формат, с которым корень открылся.
    pub fn format(&self) -> u32 {
        self.format
    }

    /// Каталог durable-состава.
    pub fn catalog_dir(&self) -> PathBuf {
        self.path.join(CATALOG)
    }

    /// Поднимает marker до текущего формата.
    ///
    /// Вызывается **последним шагом** импорта: marker `2` означает «каталог
    /// состава лежит и валиден». Обратный порядок оставил бы после аварии
    /// корень, который объявляет себя новым, но состава не имеет.
    pub fn raise_format(&mut self) -> Result<(), String> {
        write_json_atomic(&self.path, LAYOUT_FILE, &serde_json::json!({ "format": VERSION }))?;
        self.format = VERSION;
        Ok(())
    }

    /// Записывает `index.json` уже опубликованного индекса с его UUID.
    ///
    /// Идемпотентно: повторный импорт после аварии видит UUID на месте и не
    /// выдаёт новый, поэтому привязки alias не осиротеют.
    pub fn stamp(&self, name: &str, uuid: &crate::catalog::Uuid) -> Result<(), String> {
        let dir = self.index_path(name)?;
        write_json_atomic(
            &dir,
            META,
            &serde_json::json!({ "format": VERSION, "name": name, "uuid": uuid.text() }),
        )
    }

    pub fn index_path(&self, name: &str) -> Result<PathBuf, String> {
        valid_name(name)?;
        Ok(self.path.join(INDICES).join(encoded(name)))
    }

    /// Перечисляет только полностью опубликованные каталоги.
    ///
    /// С появлением durable-каталога это **перестаёт** быть источником состава
    /// и остаётся только двумя вещами: материалом для однократного импорта
    /// формата 1 и сверкой того, что у каждой записи каталога есть каталог на
    /// диске. Пересобирать состав по листингу при живом format 2 нельзя — это
    /// молча уничтожило бы aliases, блоки и теги.
    pub fn discover(&self) -> Result<Vec<Discovered>, String> {
        let dir = self.path.join(INDICES);
        let mut out = Vec::new();
        for entry in fs::read_dir(&dir)
            .map_err(|e| format!("каталог индексов {} не читается: {e}", dir.display()))?
        {
            let entry = entry.map_err(|e| format!("элемент {} не читается: {e}", dir.display()))?;
            let path = entry.path();
            if !entry
                .file_type()
                .map_err(|e| format!("тип {} не читается: {e}", path.display()))?
                .is_dir()
            {
                return Err(format!("в каталоге индексов найден не каталог: {}", path.display()));
            }
            let raw = svindb_core::fs::read_regular_limited(&path.join(META), META_LIMIT, false)
                .map_err(|e| format!("метаданные индекса {} не читаются: {e}", path.display()))?;
            let meta: serde_json::Value = serde_json::from_slice(&raw)
                .map_err(|e| format!("метаданные индекса {} повреждены: {e}", path.display()))?;
            let name = meta
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("в метаданных индекса {} нет имени", path.display()))?;
            let format = meta.get("format").and_then(serde_json::Value::as_u64);
            valid_name(name)?;
            let known =
                format.is_some_and(|v| (u64::from(VERSION_MIN)..=u64::from(VERSION)).contains(&v));
            if !known || entry.file_name().as_os_str() != std::ffi::OsStr::new(&encoded(name)) {
                return Err(format!(
                    "метаданные индекса {} не соответствуют имени каталога или версии layout",
                    path.display()
                ));
            }
            let uuid = match meta.get("uuid").and_then(serde_json::Value::as_str) {
                None => None,
                Some(text) => Some(crate::catalog::Uuid::parse(text).ok_or_else(|| {
                    format!("в метаданных индекса {} негодный uuid {text:?}", path.display())
                })?),
            };
            out.push(Discovered { name: name.to_string(), path, uuid });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Создаёт staging-каталог. Вызывающий сначала полностью объявляет в нём
    /// схему, затем вызывает [`Root::publish`].
    pub fn stage(&self, name: &str, uuid: &crate::catalog::Uuid) -> Result<PathBuf, String> {
        valid_name(name)?;
        let final_path = self.index_path(name)?;
        if final_path.exists() {
            return Err(format!("индекс {name:?} уже существует"));
        }
        let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
        let stage = self.path.join(CREATING).join(format!(
            "{}-{}-{unique}",
            encoded(name),
            std::process::id()
        ));
        fs::create_dir(&stage)
            .map_err(|e| format!("staging индекса {name:?} не создаётся: {e}"))?;
        if let Err(e) = write_json_atomic(
            &stage,
            META,
            &serde_json::json!({ "format": VERSION, "name": name, "uuid": uuid.text() }),
        ) {
            let _ = fs::remove_dir_all(&stage);
            return Err(e);
        }
        sync_dir(self.path.join(CREATING).as_path())?;
        Ok(stage)
    }

    /// Атомарно делает подготовленный каталог видимым после его полной
    /// инициализации.
    pub fn publish(&self, name: &str, stage: &Path) -> Result<PathBuf, String> {
        valid_name(name)?;
        let creating = self.path.join(CREATING);
        if stage.parent() != Some(creating.as_path()) {
            return Err(format!(
                "staging {} находится вне {}",
                stage.display(),
                creating.display()
            ));
        }
        let final_path = self.index_path(name)?;
        if final_path.exists() {
            return Err(format!("индекс {name:?} уже существует"));
        }
        fs::rename(stage, &final_path).map_err(|e| {
            format!(
                "индекс {name:?} не публикуется из {} в {}: {e}",
                stage.display(),
                final_path.display()
            )
        })?;
        sync_dir(self.path.join(INDICES).as_path())?;
        sync_dir(creating.as_path())?;
        Ok(final_path)
    }

    /// Убирает не опубликованный staging после ошибки проверки схемы или
    /// настроек. Проверка родителя обязательна: вызывающий никогда не получает
    /// универсального `remove_dir_all` по произвольному пути.
    pub fn abort_stage(&self, stage: &Path) -> Result<(), String> {
        let creating = self.path.join(CREATING);
        if stage.parent() != Some(creating.as_path()) {
            return Err(format!(
                "staging {} находится вне {}",
                stage.display(),
                creating.display()
            ));
        }
        if stage.exists() {
            fs::remove_dir_all(stage)
                .map_err(|e| format!("staging {} не удаляется: {e}", stage.display()))?;
            sync_dir(&creating)?;
        }
        Ok(())
    }

    /// Сначала атомарно убирает индекс из видимого каталога, затем удаляет
    /// данные. После успешного rename ни restart, ни новый запрос его уже не
    /// откроют, даже если питание пропадёт во время рекурсивного удаления.
    pub fn remove(&self, name: &str) -> Result<(), String> {
        let from = self.index_path(name)?;
        if !from.is_dir() {
            return Err(format!("индекс {name:?} не существует"));
        }
        let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
        let to = self.path.join(TRASH).join(format!(
            "{}-{}-{unique}",
            encoded(name),
            std::process::id()
        ));
        fs::rename(&from, &to)
            .map_err(|e| format!("индекс {name:?} не переносится в корзину: {e}"))?;
        sync_dir(self.path.join(INDICES).as_path())?;
        sync_dir(self.path.join(TRASH).as_path())?;
        fs::remove_dir_all(&to).map_err(|e| {
            format!("индекс {name:?} скрыт, но корзина {} не очищена: {e}", to.display())
        })?;
        sync_dir(self.path.join(TRASH).as_path())
    }

    fn sweep_incomplete(&self) -> Result<(), String> {
        for child in [CREATING, TRASH] {
            let dir = self.path.join(child);
            if !dir.exists() {
                continue;
            }
            for entry in fs::read_dir(&dir)
                .map_err(|e| format!("служебный каталог {} не читается: {e}", dir.display()))?
            {
                let path = entry
                    .map_err(|e| format!("элемент {} не читается: {e}", dir.display()))?
                    .path();
                if path.is_dir() {
                    fs::remove_dir_all(&path)
                        .map_err(|e| format!("остаток {} не удаляется: {e}", path.display()))?;
                } else {
                    fs::remove_file(&path)
                        .map_err(|e| format!("остаток {} не удаляется: {e}", path.display()))?;
                }
            }
            sync_dir(&dir)?;
        }
        Ok(())
    }
}

/// Опубликованный каталог индекса, найденный на диске.
#[derive(Clone, Debug)]
pub struct Discovered {
    pub name: String,
    pub path: PathBuf,
    /// `None` у индекса, созданного 1.2.0: там UUID ещё не было.
    pub uuid: Option<crate::catalog::Uuid>,
}

/// Сводка индекса для `/_cat/indices` без выдачи внутренних Arc наружу.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub name: String,
    /// Живые документы, как `docs.count` у Elasticsearch.
    pub docs: Option<usize>,
    /// Физически оставшиеся погашенные версии до reclaim.
    pub deleted: Option<usize>,
    pub segments: Option<usize>,
    pub bytes: u64,
    pub writable: bool,
    /// Устойчивое имя индекса. Пусто у корня формата 1, открытого только на
    /// чтение: там UUID ещё не выданы, и выдумывать их нельзя.
    pub uuid: String,
    pub state: State,
    pub blocks: catalog::Blocks,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub aliases: Vec<String>,
    pub write_aliases: Vec<String>,
}

/// Одна атомарная правка alias из HTTP API.
#[derive(Clone, Debug)]
pub enum AliasAction {
    Add { index: String, alias: String, is_write: Option<bool> },
    Remove { index: String, alias: String, must_exist: bool },
    RemoveIndex { index: String },
}

/// Разрешённая цель data-plane на одном поколении каталога.
#[derive(Clone)]
pub struct Target {
    pub name: String,
    pub uuid: catalog::Uuid,
    pub node: Arc<Node>,
}

pub struct RolloverRequest {
    pub new_index: Option<String>,
    pub max_docs: Option<u64>,
    pub max_age_ms: Option<u64>,
    pub max_size: Option<u64>,
    pub dry_run: bool,
    pub settings_patch: Option<Vec<u8>>,
    pub description: Option<Option<String>>,
    pub tags: Option<Vec<String>>,
}

pub struct RolloverOutcome {
    pub old_index: String,
    pub new_index: String,
    pub rolled_over: bool,
    pub dry_run: bool,
    pub conditions: Vec<(String, bool)>,
    pub generation: u64,
}

#[derive(Default)]
pub struct CreateOptions {
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub aliases: Vec<(String, Option<bool>)>,
}

fn target_from_entry(entry: &catalog::Entry, block: Option<u8>) -> Result<Target, CatalogError> {
    match entry.state {
        State::Open => {}
        State::Closed => {
            return Err(CatalogError::IndexClosed { name: entry.name.clone() });
        }
        _ => {
            return Err(CatalogError::ResourceBusy {
                why: format!(
                    "индекс {:?} находится в состоянии {}",
                    entry.name,
                    entry.state.name()
                ),
            });
        }
    }
    if let Some(mask) = block {
        if entry.blocks.has(mask) {
            let block = if mask == catalog::Blocks::READ { "read" } else { "write" };
            return Err(CatalogError::Blocked { index: entry.name.clone(), block });
        }
    }
    let node = entry.node.clone().ok_or_else(|| CatalogError::ResourceBusy {
        why: format!("runtime индекса {:?} ещё не опубликован", entry.name),
    })?;
    Ok(Target { name: entry.name.clone(), uuid: entry.uuid, node })
}

fn metadata_allowed(entry: &catalog::Entry) -> Result<(), CatalogError> {
    if entry.blocks.has(catalog::Blocks::METADATA)
        || entry.blocks.has(catalog::Blocks::READ_ONLY_ALLOW_DELETE)
    {
        return Err(CatalogError::Blocked { index: entry.name.clone(), block: "metadata" });
    }
    Ok(())
}

fn delete_allowed(entry: &catalog::Entry) -> Result<(), CatalogError> {
    if entry.blocks.has(catalog::Blocks::METADATA)
        && !entry.blocks.has(catalog::Blocks::READ_ONLY_ALLOW_DELETE)
    {
        return Err(CatalogError::Blocked { index: entry.name.clone(), block: "metadata" });
    }
    Ok(())
}

/// Маленький glob без regex-зависимости: `*` — любая последовательность,
/// `?` — один Unicode scalar. Имена уже ограничены 255 байтами, поэтому
/// динамика `O(pattern * name)` здесь предсказуема.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti, mut star, mut retry) = (0usize, 0usize, None, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            pi += 1;
            retry = ti;
        } else if let Some(at) = star {
            pi = at + 1;
            retry += 1;
            ti = retry;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn next_rollover_name(current: &str) -> Result<String, CatalogError> {
    let bytes = current.as_bytes();
    let start = bytes.iter().rposition(|byte| !byte.is_ascii_digit()).map_or(0, |at| at + 1);
    if start == bytes.len() {
        return Err(CatalogError::InvalidParameter {
            key: "new_index".to_string(),
            why: format!(
                "write index {current:?} не заканчивается номером поколения; передайте новое имя в URL"
            ),
        });
    }
    let suffix = &current[start..];
    let number = suffix.parse::<u64>().map_err(|_| CatalogError::InvalidParameter {
        key: "new_index".to_string(),
        why: "номер поколения не помещается в u64".to_string(),
    })?;
    let next = number.checked_add(1).ok_or_else(|| CatalogError::LimitExceeded {
        what: "номер поколения".to_string(),
        have: usize::MAX,
        limit: usize::MAX,
    })?;
    let width = suffix.len().max(6);
    Ok(format!("{}{next:0width$}", &current[..start]))
}

/// Процессная доля памяти приёма и слабые ссылки на открытые индексы.
///
/// Ссылки слабые принципиально: бюджет не продлевает жизнь удаляемого runtime.
/// Изменения состава и `flush_mb` сериализуются одной settings-полосой, поэтому
/// два PUT настроек не могут оба пройти проверку против старого состояния и
/// вместе превысить RAM процесса.
#[derive(Default)]
pub struct IngestBudget {
    nodes: Mutex<BTreeMap<String, Weak<Node>>>,
}

impl IngestBudget {
    pub fn replace(&self, nodes: &BTreeMap<String, Arc<Node>>) -> Result<(), String> {
        let mut owned = self.nodes.lock().map_err(|_| "реестр ingest-budget отравлен")?;
        owned.clear();
        owned.extend(nodes.iter().map(|(name, node)| (name.clone(), Arc::downgrade(node))));
        Ok(())
    }

    pub fn insert(&self, name: &str, node: &Arc<Node>) -> Result<(), String> {
        self.nodes
            .lock()
            .map_err(|_| "реестр ingest-budget отравлен")?
            .insert(name.to_string(), Arc::downgrade(node));
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<(), String> {
        self.nodes.lock().map_err(|_| "реестр ingest-budget отравлен")?.remove(name);
        Ok(())
    }

    fn live_nodes(&self) -> Result<Vec<Arc<Node>>, String> {
        Ok(self
            .nodes
            .lock()
            .map_err(|_| "реестр ingest-budget отравлен")?
            .values()
            .filter_map(Weak::upgrade)
            .collect())
    }

    /// Проверяет кандидатное значение до записи settings на диск.
    pub fn validate(&self, replacing_dir: Option<&str>, candidate_mb: u64) -> Result<(), String> {
        let nodes = self.live_nodes()?;
        let total = svindb_core::govern::share(svindb_core::govern::Class::Ingest) as u128;
        if total == 0 {
            return Ok(());
        }
        let mut explicit = 0u128;
        let mut replaced = false;
        for node in &nodes {
            let mb = if replacing_dir == Some(node.dir.as_str()) {
                replaced = true;
                candidate_mb
            } else {
                node.live_settings.read().map_err(|_| "настройки индекса отравлены")?.flush_mb
            };
            if mb > 0 && node.writer.is_some() {
                explicit = explicit.saturating_add(u128::from(mb).saturating_mul(2 << 20));
            }
        }
        if replacing_dir.is_none() || !replaced {
            explicit = explicit.saturating_add(u128::from(candidate_mb).saturating_mul(2 << 20));
        }
        if explicit > total {
            return Err(format!(
                "явные flush_mb резервируют {} МБ при общем ingest-budget {} МБ",
                explicit >> 20,
                total >> 20
            ));
        }
        Ok(())
    }

    /// Применяет общий план ко всем writer'ам после изменения состава или
    /// настроек. Явный предел получает ровно заданное значение, автоматические
    /// делят остаток поровну и держат по два оборотных буфера.
    pub fn rebalance(&self) -> Result<(), String> {
        let nodes = self.live_nodes()?;
        let total = svindb_core::govern::share(svindb_core::govern::Class::Ingest) as usize;
        if total == 0 {
            return Ok(());
        }
        let mut explicit = 0usize;
        let mut automatic = 0usize;
        let mut settings = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let mb = node.live_settings.read().map_err(|_| "настройки индекса отравлены")?.flush_mb;
            if node.writer.is_some() {
                if mb == 0 {
                    automatic += 1;
                } else {
                    explicit = explicit.saturating_add((mb as usize).saturating_mul(2 << 20));
                }
            }
            settings.push((Arc::clone(node), mb));
        }
        if explicit > total {
            return Err(format!(
                "явные flush_mb резервируют {} МБ при общем ingest-budget {} МБ",
                explicit >> 20,
                total >> 20
            ));
        }
        let automatic_bytes =
            total.saturating_sub(explicit).checked_div(automatic).unwrap_or(0) / 2;
        // Автоматический ingest-budget — верхняя граница RAM, а не причина
        // откладывать WAL checkpoint до сотен мегабайт. В 2.0.2 нулевой
        // `flush_mb` означает единый 64-МиБ recovery cap и в single-, и в
        // multi-index runtime; иначе rebalance незаметно отменял бы политику,
        // заданную при открытии индекса.
        let recovery_cap = 64usize << 20;
        for (node, mb) in settings {
            let Some(writer) = node.writer.as_ref() else { continue };
            let bytes = if mb == 0 {
                automatic_bytes.min(recovery_cap).max(1)
            } else {
                (mb as usize).saturating_mul(1 << 20)
            };
            writer.ingest.lock().map_err(|_| "буфер приёма отравлен")?.set_flush_bytes(bytes);
        }
        Ok(())
    }
}

/// Момент создания каталога индекса — по метке файла метаданных.
///
/// Приблизительно для импортированных индексов, и это честнее выдуманной
/// точности: у корня 1.2.0 даты создания на диске просто нет.
fn created_ms(path: &Path) -> u64 {
    std::fs::metadata(path.join(META))
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_millis().min(u128::from(u64::MAX)) as u64)
}

/// Текущее время в миллисекундах эпохи.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u128::from(u64::MAX)) as u64)
}

/// Поднимает состав корня: импорт формата 1 либо чтение durable-каталога.
///
/// ## Инвариант обновления, одной строкой
///
/// Marker поднимается до `2` **только после** того, как валидный каталог стал
/// durable, а каталог читается **только** при marker `2`. Отсюда все три
/// случая аварии сходятся: до создания каталога импорт повторяется с нуля,
/// между каталогом и marker — повторяется и сходится в то же состояние (шаг
/// проставления UUID идемпотентен), после marker — обычное открытие.
fn load_catalog(
    root: &mut Root,
    writable: bool,
) -> Result<(Catalog, Option<svindb_core::rootlog::Log>), String> {
    if root.format() < VERSION {
        return import_catalog(root, writable);
    }
    let dir = root.catalog_dir();
    if !svindb_core::rootlog::exists(&dir) {
        // Единственное исключение — корень, в котором ещё нет ни одного
        // опубликованного индекса. Терять там нечего: без индексов не бывает ни
        // aliases, ни блоков, ни тегов. Так поднимается только что созданный
        // корень и восстанавливается авария между записью marker и созданием
        // каталога.
        let empty = root.discover()?.is_empty();
        if empty && writable {
            let log = svindb_core::rootlog::Log::create(&dir, 0, &[])?;
            return Ok((Catalog::default(), Some(log)));
        }
        // Во всех прочих случаях — отказ. Никакого отката к `discover()`:
        // пересобрать состав по листингу соблазнительно и катастрофично, имена
        // вернулись бы, а aliases, блоки и теги исчезли молча — то есть узел
        // поднялся бы, отвечал бы двумя сотнями и писал бы в архив.
        return Err(format!(
            "корень объявлен форматом {VERSION}, но каталога состава в {} нет, \
             а индексов на диске {}; пересобрать состав по листингу нельзя — это молча \
             уничтожило бы aliases и блоки",
            dir.display(),
            root.discover()?.len()
        ));
    }
    let (log, loaded) = svindb_core::rootlog::Log::open(&dir, writable)?;
    let mut catalog = Catalog::default();
    for rec in &loaded.base {
        catalog.apply(rec.kind, &rec.body)?;
    }
    for (_, rec) in &loaded.replay {
        catalog.apply(rec.kind, &rec.body)?;
    }
    catalog.set_generation(loaded.generation);
    let mut log = log;
    reconcile(root, &mut catalog, &mut log, writable)?;
    Ok((catalog, Some(log)))
}

/// Сводит запись каталога с тем, что лежит на диске.
///
/// Переходные состояния восстанавливаются по одному правилу: каждое
/// возвращается в состояние *до* транзакции. Исключение — `Creating` и
/// `Deleting`: у них и прежнее, и последующее состояние одинаково непригодны,
/// поэтому они восстанавливаются вперёд, в «отсутствует».
fn reconcile(
    root: &Root,
    catalog: &mut Catalog,
    log: &mut svindb_core::rootlog::Log,
    writable: bool,
) -> Result<(), String> {
    let mut fixes: Vec<(u8, Vec<u8>)> = Vec::new();
    for entry in catalog.entries() {
        let path = root.index_path(&entry.name)?;
        let present = path.is_dir();
        match entry.state {
            State::Creating | State::Deleting => {
                if !writable {
                    continue;
                }
                if present {
                    root.remove(&entry.name)?;
                }
                fixes.push((catalog::TXN_INDEX_REMOVE, catalog::body::index_remove(&entry.uuid)));
            }
            // Сброс мог не лечь на диск, поэтому отказ в сторону открытия:
            // потерять подтверждённую запись хуже, чем лишний раз открыть.
            State::Closing if present && writable => {
                fixes.push((
                    catalog::TXN_INDEX_STATE,
                    catalog::body::index_state(&entry.uuid, State::Open),
                ));
            }
            // Runtime не публиковался, значит и открытым индекс не считался.
            State::Opening if writable => fixes.push((
                catalog::TXN_INDEX_STATE,
                catalog::body::index_state(&entry.uuid, State::Closed),
            )),
            State::Open | State::Closed if !present => {
                return Err(format!(
                    "индекс {:?} есть в каталоге состава, но каталога {} на диске нет; \
                     поднять корень с недостающим индексом значило бы молча потерять данные",
                    entry.name,
                    path.display()
                ));
            }
            _ => {}
        }
    }
    if !writable {
        return Ok(());
    }
    for (kind, body) in fixes {
        let generation = catalog.generation().checked_add(1).ok_or("поколения исчерпаны")?;
        catalog.apply(kind, &body)?;
        catalog.set_generation(generation);
        log.append(generation, kind, &body)?;
    }
    // Каталог на диске без записи в составе — остаток завершённого удаления
    // либо staging, опубликованный перед аварией. Ни то, ни другое не должно
    // всплыть само: имя каталога не является объявлением о существовании.
    for found in root.discover()? {
        if catalog.by_name(&found.name).is_none() {
            eprintln!(
                "каталог: {:?} лежит на диске, но не значится в составе — убран как остаток",
                found.name
            );
            root.remove(&found.name)?;
        }
    }
    Ok(())
}

/// Однократный импорт корня 1.2.0 в формат с durable-каталогом.
fn import_catalog(
    root: &mut Root,
    writable: bool,
) -> Result<(Catalog, Option<svindb_core::rootlog::Log>), String> {
    let found = root.discover()?;
    let mut catalog = Catalog::default();
    if !writable {
        // Временное представление без единой записи на диск. UUID не выдаётся:
        // выданный только в памяти, он менялся бы от запуска к запуску, и
        // привязка на него была бы ложью.
        for item in found {
            catalog.apply(
                catalog::TXN_INDEX_ADD,
                &catalog::body::index_add(&catalog::Entry {
                    uuid: catalog::Uuid::default(),
                    name: item.name,
                    state: State::Open,
                    blocks: catalog::Blocks::none(),
                    created_ms: created_ms(&item.path),
                    description: None,
                    tags: Vec::new(),
                    node: None,
                }),
            )?;
        }
        return Ok((catalog, None));
    }
    // Ни переиндексации, ни копирования сегментов: обновляется только состав.
    for item in &found {
        let uuid = match item.uuid {
            Some(uuid) => uuid,
            None => catalog::Uuid::mint().map_err(|e| e.to_string())?,
        };
        root.stamp(&item.name, &uuid)?;
        catalog.apply(
            catalog::TXN_INDEX_ADD,
            &catalog::body::index_add(&catalog::Entry {
                uuid,
                name: item.name.clone(),
                state: State::Open,
                blocks: catalog::Blocks::none(),
                created_ms: created_ms(&item.path),
                description: None,
                tags: Vec::new(),
                node: None,
            }),
        )?;
    }
    catalog.set_generation(1);
    let log = svindb_core::rootlog::Log::create(&root.catalog_dir(), 1, &catalog.to_base())?;
    svindb_core::fail::point("rootcat.upgrade.after_catalog");
    root.raise_format()?;
    svindb_core::fail::point("rootcat.upgrade.after_marker");
    eprintln!("каталог: корень обновлён до формата {VERSION}, индексов {}", found.len());
    Ok((catalog, Some(log)))
}

/// Процессный каталог runtime'ов.
///
/// `current` отвечает и за быстрый выбор на каждом запросе, и за состав.
/// `lifecycle` сериализует редкие CREATE/DELETE и тем самым закрывает гонку
/// одинаковых имён без тяжёлого замка на поисковом пути.
enum RootLease {
    Write { _lock: svindb_core::manifest::WriterLock },
    Read { _lock: svindb_core::manifest::ReaderLock },
}

pub struct Registry {
    root: Root,
    /// Состав корня на текущем поколении.
    ///
    /// `Arc` под `RwLock`, а не `arc-swap`: ровно та же идиома, что у
    /// `node.live`, и она обходится без зависимости. Read-guard живёт столько,
    /// сколько занимает клонирование `Arc`; запрос дальше работает со своим
    /// снимком и не может увидеть цели одного поколения и блоки другого.
    ///
    /// Правило, которое нельзя нарушать: **область write-guard содержит ровно
    /// один оператор**. Следующий каталог собирается снаружи, до `fsync`.
    current: RwLock<Arc<Catalog>>,
    /// Durable-журнал состава.
    ///
    /// `None` только у корня формата 1, открытого на чтение: там каталога
    /// состава ещё нет, а создавать его читающий процесс не вправе.
    log: Mutex<Option<svindb_core::rootlog::Log>>,
    lifecycle: Mutex<()>,
    options: LocalIndexOptions,
    max_indices: usize,
    warm_stop: Arc<AtomicBool>,
    warm_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Межпроцессное право менять каталог. Объявлено последним, чтобы при
    /// штатном Drop освободиться только после дочерних `Node`: новый writer не
    /// должен войти, пока старый процесс ещё закрывает их runtime.
    _root_lock: RootLease,
}

impl Registry {
    pub fn open(
        mut root: Root,
        options: LocalIndexOptions,
        max_indices: usize,
    ) -> Result<Registry, String> {
        let root_lock = if options.writable {
            RootLease::Write {
                _lock: svindb_core::manifest::WriterLock::take(&root.path().to_string_lossy())
                    .map_err(|e| format!("корень нескольких индексов занят: {e}"))?,
            }
        } else {
            RootLease::Read {
                _lock: svindb_core::manifest::ReaderLock::take(&root.path().to_string_lossy())
                    .map_err(|e| format!("корень нескольких индексов занят: {e}"))?,
            }
        };
        let (catalog, log) = load_catalog(&mut root, options.writable)?;
        let opened = catalog.entries().iter().filter(|e| e.state == State::Open).count();
        if opened > max_indices {
            return Err(format!(
                "в корне {} индексов {opened}, а безопасный предел процесса {max_indices}; поднимите --max-indices осознанно либо разнесите каталоги по узлам",
                root.path().display()
            ));
        }
        let mut catalog = catalog;
        let mut nodes = BTreeMap::new();
        for ix in 0..catalog.entries().len() {
            let (name, state) = {
                let entry = &catalog.entries()[ix];
                (entry.name.clone(), entry.state)
            };
            if state != State::Open {
                continue;
            }
            let dir = root.index_path(&name)?.to_string_lossy().to_string();
            let node = open_local_index(&name, &dir, &options)?;
            nodes.insert(name.clone(), Arc::clone(&node));
            catalog.attach(&name, node)?;
        }
        options.ingest_budget.replace(&nodes)?;
        let registry = Registry {
            root,
            current: RwLock::new(Arc::new(catalog)),
            log: Mutex::new(log),
            lifecycle: Mutex::new(()),
            options,
            max_indices,
            warm_stop: Arc::new(AtomicBool::new(false)),
            warm_thread: Mutex::new(None),
            _root_lock: root_lock,
        };
        registry.rebalance_ingest()?;
        Ok(registry)
    }

    /// Снимок состава на одно поколение.
    ///
    /// Отравление не глотается: прежний `self.nodes.read().ok()?` превращал
    /// одну панику писателя в вечный 404 на каждый индекс. Продолжить с
    /// последним согласованным состоянием честнее, а в выпуске с
    /// `panic = "abort"` отравления не бывает вовсе.
    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.current.read().unwrap_or_else(|poisoned| poisoned.into_inner()))
    }

    pub fn all_nodes(&self) -> Vec<(String, Arc<Node>)> {
        self.catalog()
            .entries()
            .iter()
            .filter_map(|e| e.node.clone().map(|node| (e.name.clone(), node)))
            .collect()
    }

    pub fn writable(&self) -> bool {
        self.options.writable
    }

    pub fn fallback_fields(&self) -> &str {
        &self.options.fields_fallback
    }

    pub fn exists(&self, name: &str) -> bool {
        self.catalog().by_name(name).is_some_and(|entry| entry.state.is_settled())
    }

    pub fn list(&self) -> Vec<Info> {
        let catalog = self.catalog();
        catalog
            .entries()
            .iter()
            .filter(|entry| entry.state.is_settled())
            .map(|entry| {
                let (docs, deleted, segments, writable) = match entry.node.as_ref() {
                    Some(node) => {
                        let segs = node.live.read().expect("набор сегментов").clone();
                        let deleted: usize = segs.list.iter().map(|segment| segment.dead()).sum();
                        (
                            Some(segs.docs().saturating_sub(deleted)),
                            Some(deleted),
                            Some(segs.list.len()),
                            node.writer.is_some() && !entry.blocks.has(catalog::Blocks::WRITE),
                        )
                    }
                    None => (None, None, None, false),
                };
                let aliases: Vec<String> =
                    catalog.aliases_of(&entry.uuid).into_iter().map(str::to_string).collect();
                let write_aliases = aliases
                    .iter()
                    .filter(|alias| {
                        catalog.write_target(alias).is_ok_and(|target| target.uuid == entry.uuid)
                    })
                    .cloned()
                    .collect();
                let bytes = self
                    .root
                    .index_path(&entry.name)
                    .ok()
                    .map_or(0, |path| super::host::index_bytes(&path.to_string_lossy()));
                Info {
                    name: entry.name.clone(),
                    docs,
                    deleted,
                    segments,
                    bytes,
                    writable,
                    uuid: if entry.uuid.is_nil() { String::new() } else { entry.uuid.text() },
                    state: entry.state,
                    blocks: entry.blocks,
                    description: entry.description.clone(),
                    tags: entry.tags.clone(),
                    aliases,
                    write_aliases,
                }
            })
            .collect()
    }

    /// Разрешает однозначную цель записи. Alias никогда не переключается на
    /// «первый доступный»: блок или закрытие выбранной цели — явный отказ.
    pub fn write_target(&self, expr: &str) -> Result<Target, CatalogError> {
        self.write_target_inner(expr, true)
    }

    /// Выбранный writer для административного flush: адресуется так же, как
    /// запись, но write-block не запрещает довести уже принятые данные до
    /// checkpoint.
    pub fn flush_target(&self, expr: &str) -> Result<Target, CatalogError> {
        self.write_target_inner(expr, false)
    }

    fn write_target_inner(&self, expr: &str, enforce_block: bool) -> Result<Target, CatalogError> {
        if expr == "_all" || expr.contains([',', '*', '?']) {
            return Err(CatalogError::InvalidTargetForWrite { expr: expr.to_string() });
        }
        let catalog = self.catalog();
        let entry = match catalog.by_name(expr) {
            Some(entry) => entry,
            None => catalog.write_target(expr)?,
        };
        target_from_entry(entry, enforce_block.then_some(catalog::Blocks::WRITE))
    }

    /// Разрешает одну цель чтения для GET/MGET и прочих операций, которым
    /// неоднозначность alias скрыла бы, из какого поколения пришёл документ.
    pub fn one_read_target(&self, expr: &str) -> Result<Target, CatalogError> {
        let targets = self.read_targets(expr)?;
        if targets.len() != 1 {
            return Err(CatalogError::AmbiguousTarget {
                expr: expr.to_string(),
                matched: targets.into_iter().map(|target| target.name).collect(),
            });
        }
        Ok(targets.into_iter().next().expect("ровно одна цель"))
    }

    /// Однозначный открытый runtime без data read-block. Нужен health/stats и
    /// административным точкам, которые должны оставаться способом понять и
    /// снять установленный запрет.
    pub fn one_control_target(&self, expr: &str) -> Result<Target, CatalogError> {
        if expr == "_all" || expr.contains([',', '*', '?']) {
            return Err(CatalogError::AmbiguousTarget {
                expr: expr.to_string(),
                matched: Vec::new(),
            });
        }
        let catalog = self.catalog();
        if let Some(entry) = catalog.by_name(expr) {
            return target_from_entry(entry, None);
        }
        let bindings = catalog
            .bindings(expr)
            .ok_or_else(|| CatalogError::IndexNotFound { name: expr.to_string() })?;
        if bindings.len() != 1 {
            let matched = bindings
                .iter()
                .filter_map(|binding| catalog.by_uuid(&binding.target))
                .map(|entry| entry.name.clone())
                .collect();
            return Err(CatalogError::AmbiguousTarget { expr: expr.to_string(), matched });
        }
        let entry = catalog.by_uuid(&bindings[0].target).ok_or_else(|| {
            CatalogError::AliasTargetNotFound {
                alias: expr.to_string(),
                target: bindings[0].target.text(),
            }
        })?;
        target_from_entry(entry, None)
    }

    pub fn one_metadata_target(&self, expr: &str) -> Result<Target, CatalogError> {
        let target = self.one_control_target(expr)?;
        let catalog = self.catalog();
        let entry = catalog
            .by_uuid(&target.uuid)
            .ok_or_else(|| CatalogError::IndexNotFound { name: target.name.clone() })?;
        metadata_allowed(entry)?;
        Ok(target)
    }

    /// Раскрывает concrete names, aliases, списки, wildcard и `_all` против
    /// одного снимка. Повторяющиеся физические UUID удаляются.
    pub fn read_targets(&self, expr: &str) -> Result<Vec<Target>, CatalogError> {
        self.read_targets_with(expr, false, false, true, false)
    }

    pub fn read_targets_with(
        &self,
        expr: &str,
        ignore_unavailable: bool,
        allow_no_indices: bool,
        expand_open: bool,
        expand_closed: bool,
    ) -> Result<Vec<Target>, CatalogError> {
        let catalog = self.catalog();
        if expr != "_all" && !expr.contains([',', '*', '?']) {
            if let Some(entry) = catalog.by_name(expr) {
                return match target_from_entry(entry, Some(catalog::Blocks::READ)) {
                    Ok(target) => Ok(vec![target]),
                    Err(CatalogError::IndexClosed { .. }) if ignore_unavailable => {
                        if allow_no_indices {
                            Ok(Vec::new())
                        } else {
                            Err(CatalogError::IndexNotFound { name: expr.to_string() })
                        }
                    }
                    Err(error) => Err(error),
                };
            }
            if let Some([binding]) = catalog.bindings(expr) {
                let entry = catalog.by_uuid(&binding.target).ok_or_else(|| {
                    CatalogError::AliasTargetNotFound {
                        alias: expr.to_string(),
                        target: binding.target.text(),
                    }
                })?;
                return match target_from_entry(entry, Some(catalog::Blocks::READ)) {
                    Ok(target) => Ok(vec![target]),
                    Err(CatalogError::IndexClosed { .. }) if ignore_unavailable => {
                        if allow_no_indices {
                            Ok(Vec::new())
                        } else {
                            Err(CatalogError::IndexNotFound { name: expr.to_string() })
                        }
                    }
                    Err(error) => Err(error),
                };
            }
        }
        let mut found: BTreeMap<catalog::Uuid, Target> = BTreeMap::new();
        for token in expr.split(',').filter(|token| !token.is_empty()) {
            let wildcard = token == "_all" || token.contains(['*', '?']);
            if wildcard {
                for entry in catalog.entries() {
                    let expands_state = match entry.state {
                        State::Open => expand_open,
                        State::Closed => expand_closed,
                        State::Creating | State::Deleting | State::Opening | State::Closing => {
                            false
                        }
                    };
                    if expands_state && (token == "_all" || wildcard_match(token, &entry.name)) {
                        let target = match target_from_entry(entry, Some(catalog::Blocks::READ)) {
                            Ok(target) => target,
                            Err(CatalogError::IndexClosed { .. }) if ignore_unavailable => continue,
                            Err(error) => return Err(error),
                        };
                        found.entry(target.uuid).or_insert(target);
                    }
                }
                for (alias, bindings) in catalog.aliases() {
                    if token != "_all" && !wildcard_match(token, alias) {
                        continue;
                    }
                    for binding in bindings {
                        let Some(entry) = catalog.by_uuid(&binding.target) else {
                            return Err(CatalogError::AliasTargetNotFound {
                                alias: alias.clone(),
                                target: binding.target.text(),
                            });
                        };
                        let expands_state = match entry.state {
                            State::Open => expand_open,
                            State::Closed => expand_closed,
                            State::Creating | State::Deleting | State::Opening | State::Closing => {
                                false
                            }
                        };
                        if !expands_state {
                            continue;
                        }
                        let target = match target_from_entry(entry, Some(catalog::Blocks::READ)) {
                            Ok(target) => target,
                            Err(CatalogError::IndexClosed { .. }) if ignore_unavailable => continue,
                            Err(error) => return Err(error),
                        };
                        found.entry(target.uuid).or_insert(target);
                    }
                }
                continue;
            }
            if let Some(entry) = catalog.by_name(token) {
                let target = match target_from_entry(entry, Some(catalog::Blocks::READ)) {
                    Ok(target) => target,
                    Err(CatalogError::IndexClosed { .. }) if ignore_unavailable => continue,
                    Err(error) => return Err(error),
                };
                found.entry(target.uuid).or_insert(target);
                continue;
            }
            if let Some(bindings) = catalog.bindings(token) {
                for binding in bindings {
                    let entry = catalog.by_uuid(&binding.target).ok_or_else(|| {
                        CatalogError::AliasTargetNotFound {
                            alias: token.to_string(),
                            target: binding.target.text(),
                        }
                    })?;
                    let target = match target_from_entry(entry, Some(catalog::Blocks::READ)) {
                        Ok(target) => target,
                        Err(CatalogError::IndexClosed { .. }) if ignore_unavailable => continue,
                        Err(error) => return Err(error),
                    };
                    found.entry(target.uuid).or_insert(target);
                }
                continue;
            }
            if !ignore_unavailable {
                return Err(CatalogError::IndexNotFound { name: token.to_string() });
            }
        }
        if found.is_empty() {
            if allow_no_indices {
                return Ok(Vec::new());
            }
            return Err(CatalogError::IndexNotFound { name: expr.to_string() });
        }
        Ok(found.into_values().collect())
    }

    /// Частично меняет описание и теги. `None` снаружи означает «поле не
    /// прислано», внутренний `None` у description — явное очищение.
    pub fn update_metadata(
        &self,
        name: &str,
        description: Option<Option<String>>,
        tags: Option<Vec<String>>,
    ) -> Result<Arc<Catalog>, CatalogError> {
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let base = self.catalog();
        let entry = base
            .by_name(name)
            .ok_or_else(|| CatalogError::IndexNotFound { name: name.to_string() })?;
        metadata_allowed(entry)?;
        let description = description.unwrap_or_else(|| entry.description.clone());
        let tags = match tags {
            Some(tags) => catalog::normalize_tags(&tags)?,
            None => entry.tags.clone(),
        };
        catalog::check_meta(description.as_deref(), &tags)?;
        self.commit(
            catalog::TXN_INDEX_META,
            &catalog::body::index_meta(&entry.uuid, description.as_deref(), &tags),
        )
        .map_err(CatalogError::Durability)
    }

    /// Добавляет или снимает один именованный блок, не затрагивая остальные.
    pub fn change_block(
        &self,
        name: &str,
        block: &str,
        enable: bool,
    ) -> Result<Arc<Catalog>, CatalogError> {
        let mask =
            catalog::Blocks::mask_of(block).ok_or_else(|| CatalogError::InvalidParameter {
                key: "block".to_string(),
                why: format!("неизвестный блок {block:?}"),
            })?;
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let base = self.catalog();
        let entry = base
            .by_name(name)
            .ok_or_else(|| CatalogError::IndexNotFound { name: name.to_string() })?;
        if entry.state != State::Open && entry.state != State::Closed {
            return Err(busy(&format!("индекс {name:?} находится в переходном состоянии")));
        }
        // Metadata block вправе снять сам себя. Любая иная метаправка под ним
        // запрещена, иначе блок был бы декоративным.
        let removes_metadata = !enable && mask & catalog::Blocks::METADATA != 0;
        if entry.blocks.has(catalog::Blocks::METADATA) && !removes_metadata {
            return Err(CatalogError::Blocked { index: name.to_string(), block: "metadata" });
        }
        let blocks = if enable { entry.blocks.with(mask) } else { entry.blocks.without(mask) };
        self.commit(catalog::TXN_INDEX_BLOCKS, &catalog::body::index_blocks(&entry.uuid, blocks))
            .map_err(CatalogError::Durability)
    }

    /// Применяет пакет alias-действий целиком. Удаляемые runtime исчезают из
    /// нового каталога одной записью; уже начатые запросы доживают на старом
    /// `Arc<Catalog>`, после чего каталоги уходят в `.trash`.
    pub fn aliases(&self, actions: &[AliasAction]) -> Result<Arc<Catalog>, CatalogError> {
        if actions.is_empty() {
            return Err(CatalogError::InvalidParameter {
                key: "actions".to_string(),
                why: "массив пуст".to_string(),
            });
        }
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let base = self.catalog();
        let mut aliases = base.aliases().clone();
        let mut removed: BTreeMap<catalog::Uuid, (String, Option<Arc<Node>>)> = BTreeMap::new();

        for action in actions {
            match action {
                AliasAction::Add { index, alias, is_write } => {
                    valid_name(alias)
                        .map_err(|why| CatalogError::InvalidName { name: alias.clone(), why })?;
                    if base.by_name(alias).is_some() {
                        return Err(CatalogError::AliasIsIndexName { name: alias.clone() });
                    }
                    if aliases.contains_key(index) {
                        return Err(CatalogError::AliasChain { alias: index.clone() });
                    }
                    let entry = base
                        .by_name(index)
                        .ok_or_else(|| CatalogError::IndexNotFound { name: index.clone() })?;
                    metadata_allowed(entry)?;
                    if removed.contains_key(&entry.uuid) {
                        return Err(CatalogError::InvalidParameter {
                            key: "actions".to_string(),
                            why: format!("индекс {index:?} удалён раньше в той же пачке"),
                        });
                    }
                    let bindings = aliases.entry(alias.clone()).or_default();
                    bindings.retain(|binding| binding.target != entry.uuid);
                    bindings.push(catalog::Binding { target: entry.uuid, is_write: *is_write });
                    bindings.sort_by_key(|binding| binding.target);
                    if bindings.iter().filter(|binding| binding.is_write == Some(true)).count() > 1
                    {
                        return Err(CatalogError::MultipleWriteIndices { alias: alias.clone() });
                    }
                }
                AliasAction::Remove { index, alias, must_exist } => {
                    let entry = base
                        .by_name(index)
                        .ok_or_else(|| CatalogError::IndexNotFound { name: index.clone() })?;
                    metadata_allowed(entry)?;
                    let existed = aliases.get_mut(alias).is_some_and(|bindings| {
                        let before = bindings.len();
                        bindings.retain(|binding| binding.target != entry.uuid);
                        before != bindings.len()
                    });
                    aliases.retain(|_, bindings| !bindings.is_empty());
                    if *must_exist && !existed {
                        return Err(CatalogError::AliasNotFound { alias: alias.clone() });
                    }
                }
                AliasAction::RemoveIndex { index } => {
                    let matched: Vec<&catalog::Entry> = base
                        .entries()
                        .iter()
                        .filter(|entry| {
                            if index.contains(['*', '?']) {
                                wildcard_match(index, &entry.name)
                            } else {
                                entry.name == *index
                            }
                        })
                        .collect();
                    if matched.is_empty() {
                        return Err(CatalogError::IndexNotFound { name: index.clone() });
                    }
                    for entry in matched {
                        delete_allowed(entry)?;
                        removed.insert(entry.uuid, (entry.name.clone(), entry.node.clone()));
                        for bindings in aliases.values_mut() {
                            bindings.retain(|binding| binding.target != entry.uuid);
                        }
                    }
                    aliases.retain(|_, bindings| !bindings.is_empty());
                }
            }
        }

        let mut sets: Vec<catalog::AliasSet> = Vec::new();
        let mut names: std::collections::BTreeSet<String> =
            base.aliases().keys().cloned().collect();
        names.extend(aliases.keys().cloned());
        for alias in names {
            let bindings = aliases.get(&alias).cloned().unwrap_or_default();
            if base.bindings(&alias) != Some(bindings.as_slice()) || bindings.is_empty() {
                sets.push(catalog::AliasSet { alias, bindings });
            }
        }
        let batch =
            catalog::AliasBatch { aliases: sets, removed: removed.keys().copied().collect() };
        let next = self
            .commit(catalog::TXN_ALIAS_BATCH, &batch.encode())
            .map_err(CatalogError::Durability)?;
        drop(base);

        if !removed.is_empty() {
            let _settings_lane =
                self.options.settings_lane.lock().map_err(|_| busy("полоса настроек отравлена"))?;
            for (name, _) in removed.values() {
                self.options
                    .ingest_budget
                    .remove(name)
                    .map_err(|why| CatalogError::SettingsRejected { why })?;
            }
            if let Err(error) = self.rebalance_ingest() {
                eprintln!(
                    "каталог: remove_index зафиксирован, но ingest-budget не пересчитан: {error}"
                );
            }
        }

        let mut cleanup_error = None;
        for (uuid, (name, node)) in removed {
            if let Some(node) = node {
                while Arc::strong_count(&node) > 1 {
                    std::thread::sleep(Duration::from_millis(5));
                }
                // Последний `Arc` принадлежит нам; Drop останавливает фоновые
                // потоки до удаления каталога.
                drop(node);
            }
            if let Err(error) = self.root.remove(&name) {
                cleanup_error.get_or_insert(error);
            }
            let _ = uuid;
        }
        if let Some(error) = cleanup_error {
            return Err(CatalogError::Durability(format!(
                "состав aliases зафиксирован, но физическая уборка не завершилась: {error}"
            )));
        }
        Ok(next)
    }

    /// Создаёт новое физическое поколение и одной записью переключает write
    /// alias вместе с блоком старого поколения.
    pub fn rollover(
        &self,
        alias: &str,
        request: RolloverRequest,
    ) -> Result<RolloverOutcome, CatalogError> {
        valid_name(alias)
            .map_err(|why| CatalogError::InvalidName { name: alias.to_string(), why })?;
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let base = self.catalog();
        let retired = base.write_target(alias)?;
        metadata_allowed(retired)?;
        let old_index = retired.name.clone();
        let retired_uuid = retired.uuid;
        let retired_blocks = retired.blocks.with(catalog::Blocks::WRITE);
        let old_node = retired
            .node
            .clone()
            .ok_or_else(|| CatalogError::IndexClosed { name: old_index.clone() })?;
        let segs = old_node.live.read().expect("набор сегментов").clone();
        let deleted: usize = segs.list.iter().map(|segment| segment.dead()).sum();
        let docs = segs.docs().saturating_sub(deleted) as u64;
        let bytes = super::host::index_bytes(&old_node.dir);
        let age_ms = now_ms().saturating_sub(retired.created_ms);
        let mut conditions = Vec::new();
        if let Some(limit) = request.max_docs {
            conditions.push(("max_docs".to_string(), docs >= limit));
        }
        if let Some(limit) = request.max_age_ms {
            conditions.push(("max_age".to_string(), age_ms >= limit));
        }
        if let Some(limit) = request.max_size {
            conditions.push(("max_size".to_string(), bytes >= limit));
        }
        let should_roll = conditions.is_empty() || conditions.iter().any(|(_, met)| *met);
        let new_index = match request.new_index {
            Some(name) => name,
            None => next_rollover_name(&old_index)?,
        };
        valid_name(&new_index)
            .map_err(|why| CatalogError::InvalidName { name: new_index.clone(), why })?;
        if base.name_taken(&new_index) {
            return Err(CatalogError::IndexAlreadyExists { name: new_index });
        }
        if request.dry_run || !should_roll {
            return Ok(RolloverOutcome {
                old_index,
                new_index,
                rolled_over: false,
                dry_run: request.dry_run,
                conditions,
                generation: base.generation(),
            });
        }
        if base.open_count() >= self.max_indices {
            return Err(CatalogError::TooManyOpenIndices {
                open: base.open_count(),
                limit: self.max_indices,
            });
        }

        let settings_lane =
            self.options.settings_lane.lock().map_err(|_| busy("полоса настроек отравлена"))?;
        let mut settings = svindb_core::settings::load(&old_node.dir)
            .map_err(|why| CatalogError::SettingsRejected { why })?;
        if let Some(patch) = request.settings_patch.as_deref() {
            let text =
                std::str::from_utf8(patch).map_err(|error| CatalogError::InvalidParameter {
                    key: "settings".to_string(),
                    why: error.to_string(),
                })?;
            let patch = svindb_core::json::parse(text).map_err(|why| {
                CatalogError::InvalidParameter { key: "settings".to_string(), why }
            })?;
            settings.patch(&patch).map_err(|why| CatalogError::SettingsRejected { why })?;
        }
        settings.check().map_err(|why| CatalogError::SettingsRejected { why })?;
        self.options
            .ingest_budget
            .validate(None, settings.flush_mb)
            .map_err(|why| CatalogError::SettingsRejected { why })?;
        drop(settings_lane);

        let registry = svindb_core::schema::Registry::load(&old_node.dir)
            .map_err(|error| CatalogError::Durability(error.to_string()))?
            .ok_or_else(|| CatalogError::Durability("у write index нет schema.json".to_string()))?;
        let decls: Vec<svindb_core::schema::Decl> = registry
            .live()
            .map(|field| svindb_core::schema::Decl {
                path: field.path.clone(),
                kind: field.kind,
                group: registry.group_path(field.group).unwrap_or_default(),
                boost: field.boost,
                store: field.store,
                positions: field.positions,
                index: field.index,
            })
            .collect();
        let mapping_path = Path::new(&old_node.dir).join(super::mapping::FILE);
        let mapping = match svindb_core::fs::read_regular_limited(&mapping_path, 4 << 20, false) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(CatalogError::Durability(error.to_string())),
        };
        let description = request.description.unwrap_or_else(|| retired.description.clone());
        let tags = match request.tags {
            Some(tags) => catalog::normalize_tags(&tags)?,
            None => retired.tags.clone(),
        };
        catalog::check_meta(description.as_deref(), &tags)?;
        let fresh_uuid = catalog::Uuid::mint()?;
        let fresh = catalog::Entry {
            uuid: fresh_uuid,
            name: new_index.clone(),
            state: State::Open,
            blocks: catalog::Blocks::none(),
            created_ms: now_ms(),
            description,
            tags,
            node: None,
        };

        let stage = self.root.stage(&new_index, &fresh_uuid).map_err(CatalogError::Durability)?;
        let built = (|| -> Result<Arc<Node>, String> {
            let stage_dir = stage.to_string_lossy().to_string();
            svindb_core::settings::save(&stage_dir, &settings)?;
            if let Some(mapping) = mapping.as_deref() {
                super::mapping::Rules::save(&stage_dir, mapping)?;
            }
            svindb_core::schema::declare_all(&stage_dir, &decls)
                .map_err(|error| format!("схема rollover {new_index:?}: {error}"))?;
            let path = self.root.publish(&new_index, &stage)?;
            let dir = path.to_string_lossy().to_string();
            super::host::forget_index_bytes(&dir);
            open_local_index(&new_index, &dir, &self.options)
        })();
        let node = match built {
            Ok(node) => node,
            Err(error) => {
                let _ = self.root.abort_stage(&stage);
                let _ = self.root.remove(&new_index);
                return Err(CatalogError::Durability(error));
            }
        };
        let settings_lane =
            self.options.settings_lane.lock().map_err(|_| busy("полоса настроек отравлена"))?;
        self.options
            .ingest_budget
            .insert(&new_index, &node)
            .map_err(|why| CatalogError::SettingsRejected { why })?;
        if let Err(error) = self.rebalance_ingest() {
            let _ = self.options.ingest_budget.remove(&new_index);
            drop(node);
            let _ = self.root.remove(&new_index);
            return Err(CatalogError::SettingsRejected { why: error });
        }
        drop(settings_lane);

        let mut bindings = base
            .bindings(alias)
            .ok_or_else(|| CatalogError::AliasNotFound { alias: alias.to_string() })?
            .to_vec();
        for binding in &mut bindings {
            binding.is_write = Some(false);
        }
        bindings.push(catalog::Binding { target: fresh_uuid, is_write: Some(true) });
        bindings.sort_by_key(|binding| binding.target);
        let rollover = catalog::Rollover {
            fresh,
            alias: alias.to_string(),
            bindings,
            retired: retired_uuid,
            retired_blocks,
        };
        let body = rollover.encode();
        let committed = self.commit_with(catalog::TXN_ROLLOVER, &body, |next| {
            next.attach(&new_index, Arc::clone(&node))
        });
        let next = match committed {
            Ok(next) => next,
            Err(error) => {
                let _ = self.options.ingest_budget.remove(&new_index);
                drop(node);
                let _ = self.root.remove(&new_index);
                let _ = self.rebalance_ingest();
                return Err(CatalogError::Durability(error));
            }
        };
        Ok(RolloverOutcome {
            old_index,
            new_index,
            rolled_over: true,
            dry_run: false,
            conditions,
            generation: next.generation(),
        })
    }

    /// Закрывает индекс с durable flush и выгрузкой runtime.
    pub fn close(&self, name: &str) -> Result<Arc<Catalog>, CatalogError> {
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let base = self.catalog();
        let entry = base
            .by_name(name)
            .ok_or_else(|| CatalogError::IndexNotFound { name: name.to_string() })?;
        metadata_allowed(entry)?;
        if entry.state == State::Closed {
            return Ok(base);
        }
        if entry.state != State::Open {
            return Err(busy(&format!(
                "индекс {name:?} находится в состоянии {}",
                entry.state.name()
            )));
        }
        if let Some(alias) = base.is_write_index(&entry.uuid) {
            return Err(CatalogError::WriteIndexClosePrevented { alias, index: name.to_string() });
        }
        let uuid = entry.uuid;
        self.commit(catalog::TXN_INDEX_STATE, &catalog::body::index_state(&uuid, State::Closing))
            .map_err(CatalogError::Durability)?;
        drop(base);
        let mut held = None;
        self.mutate(|next| {
            held = next.detach(name)?;
            Ok(())
        })
        .map_err(CatalogError::Durability)?;
        let node = held.ok_or_else(|| busy("runtime уже выгружен"))?;
        while Arc::strong_count(&node) > 1 {
            std::thread::sleep(Duration::from_millis(5));
        }
        let flushed = node
            .writer
            .as_ref()
            .map_or(Ok(true), |writer| writer.request_flush(Duration::from_secs(60)));
        match flushed {
            Ok(true) => {}
            Ok(false) => {
                self.rollback_close(name, uuid, Arc::clone(&node));
                return Err(CatalogError::ResourceBusy {
                    why: "durable flush не завершился за 60 секунд".to_string(),
                });
            }
            Err(error) => {
                self.rollback_close(name, uuid, Arc::clone(&node));
                return Err(CatalogError::Durability(error));
            }
        }
        let _settings_lane = match self.options.settings_lane.lock() {
            Ok(lane) => lane,
            Err(_) => {
                self.rollback_close(name, uuid, Arc::clone(&node));
                return Err(busy("полоса настроек отравлена"));
            }
        };
        if let Err(why) = self.options.ingest_budget.remove(name) {
            self.rollback_close(name, uuid, Arc::clone(&node));
            return Err(CatalogError::SettingsRejected { why });
        }
        drop(node);
        let next = match self
            .commit(catalog::TXN_INDEX_STATE, &catalog::body::index_state(&uuid, State::Closed))
        {
            Ok(next) => next,
            Err(error) => {
                if let Ok(path) = self.root.index_path(name) {
                    let dir = path.to_string_lossy().to_string();
                    if let Ok(node) = open_local_index(name, &dir, &self.options) {
                        self.rollback_close(name, uuid, node);
                    }
                }
                return Err(CatalogError::Durability(error));
            }
        };
        if let Err(error) = self.rebalance_ingest() {
            eprintln!("каталог: индекс {name:?} закрыт, но ingest-budget не пересчитан: {error}");
        }
        Ok(next)
    }

    fn rollback_close(&self, name: &str, uuid: catalog::Uuid, node: Arc<Node>) {
        let _ = self.options.ingest_budget.insert(name, &node);
        let _ = self.mutate(|next| next.attach(name, Arc::clone(&node)));
        let _ =
            self.commit(catalog::TXN_INDEX_STATE, &catalog::body::index_state(&uuid, State::Open));
        let _ = self.rebalance_ingest();
    }

    /// Поднимает закрытый индекс и публикует runtime только после recovery.
    pub fn open_index(&self, name: &str) -> Result<Arc<Catalog>, CatalogError> {
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let base = self.catalog();
        let entry = base
            .by_name(name)
            .ok_or_else(|| CatalogError::IndexNotFound { name: name.to_string() })?;
        metadata_allowed(entry)?;
        if entry.state == State::Open {
            return Ok(base);
        }
        if entry.state != State::Closed {
            return Err(busy(&format!(
                "индекс {name:?} находится в состоянии {}",
                entry.state.name()
            )));
        }
        if base.open_count() >= self.max_indices {
            return Err(CatalogError::TooManyOpenIndices {
                open: base.open_count(),
                limit: self.max_indices,
            });
        }
        let uuid = entry.uuid;
        self.commit(catalog::TXN_INDEX_STATE, &catalog::body::index_state(&uuid, State::Opening))
            .map_err(CatalogError::Durability)?;
        let dir = self
            .root
            .index_path(name)
            .map_err(CatalogError::Durability)?
            .to_string_lossy()
            .to_string();
        let node = match open_local_index(name, &dir, &self.options) {
            Ok(node) => node,
            Err(error) => {
                let _ = self.commit(
                    catalog::TXN_INDEX_STATE,
                    &catalog::body::index_state(&uuid, State::Closed),
                );
                return Err(CatalogError::Durability(error));
            }
        };
        let _settings_lane = match self.options.settings_lane.lock() {
            Ok(lane) => lane,
            Err(_) => {
                drop(node);
                let _ = self.commit(
                    catalog::TXN_INDEX_STATE,
                    &catalog::body::index_state(&uuid, State::Closed),
                );
                return Err(busy("полоса настроек отравлена"));
            }
        };
        if let Err(why) = self.options.ingest_budget.insert(name, &node) {
            drop(node);
            let _ = self.commit(
                catalog::TXN_INDEX_STATE,
                &catalog::body::index_state(&uuid, State::Closed),
            );
            return Err(CatalogError::SettingsRejected { why });
        }
        if let Err(error) = self.mutate(|next| next.attach(name, Arc::clone(&node))) {
            let _ = self.options.ingest_budget.remove(name);
            drop(node);
            let _ = self.commit(
                catalog::TXN_INDEX_STATE,
                &catalog::body::index_state(&uuid, State::Closed),
            );
            return Err(CatalogError::Durability(error));
        }
        let next = match self
            .commit(catalog::TXN_INDEX_STATE, &catalog::body::index_state(&uuid, State::Open))
        {
            Ok(next) => next,
            Err(error) => {
                let _ = self.mutate(|next| next.detach(name).map(|_| ()));
                let _ = self.options.ingest_budget.remove(name);
                drop(node);
                let _ = self.commit(
                    catalog::TXN_INDEX_STATE,
                    &catalog::body::index_state(&uuid, State::Closed),
                );
                return Err(CatalogError::Durability(error));
            }
        };
        if let Err(why) = self.rebalance_ingest() {
            let _ = self.mutate(|next| next.detach(name).map(|_| ()));
            let _ = self.options.ingest_budget.remove(name);
            drop(node);
            let _ = self.commit(
                catalog::TXN_INDEX_STATE,
                &catalog::body::index_state(&uuid, State::Closed),
            );
            return Err(CatalogError::SettingsRejected { why });
        }
        Ok(next)
    }

    /// Durable-транзакция: проверка, запись, публикация — ровно в этом порядке.
    ///
    /// Возврат `append` означает, что запись переживёт отключение питания.
    /// Именно поэтому публикация в память идёт **после** него: обратный порядок
    /// позволил бы подтвердить клиенту переключение, которого на диске нет.
    fn commit(&self, kind: u8, body: &[u8]) -> Result<Arc<Catalog>, String> {
        self.commit_with(kind, body, |_| Ok(()))
    }

    /// То же durable-изменение, но перед публикацией к следующему снимку
    /// можно привязать уже проверенный runtime. Closure исполняется до fsync,
    /// однако меняет только ещё невидимую копию каталога; при любом отказе она
    /// просто уничтожается.
    fn commit_with(
        &self,
        kind: u8,
        body: &[u8],
        prepare: impl FnOnce(&mut Catalog) -> Result<(), String>,
    ) -> Result<Arc<Catalog>, String> {
        let base = self.catalog();
        let generation = base.generation().checked_add(1).ok_or("поколения каталога исчерпаны")?;
        let mut next = (*base).clone();
        // Сборка следующего состава — до единого обращения к диску: негодная
        // транзакция обязана отказать, ничего не записав.
        next.apply(kind, body)?;
        prepare(&mut next)?;
        next.set_generation(generation);
        {
            let mut guard = self.log.lock().map_err(|_| "журнал каталога отравлен")?;
            let log = guard.as_mut().ok_or(
                "корень формата 1 открыт только на чтение: состав меняется после первого \
                 writable-запуска, который выполнит обновление формата",
            )?;
            log.append(generation, kind, body)?;
            if log.due_for_compaction() {
                // Append уже durable и является самой транзакцией. Уплотнение
                // — обслуживание: его отказ нельзя превращать в отказ запроса
                // после fsync, иначе клиент повторит операцию, которая после
                // restart всё равно применится, а текущий процесс останется с
                // поколением журнала впереди памяти.
                if let Err(error) = log.compact(&next.to_base()) {
                    eprintln!("каталог: уплотнение отложено после поколения {generation}: {error}");
                }
            }
        }
        svindb_core::fail::point("rootcat.after_fsync");
        let next = Arc::new(next);
        self.publish(Arc::clone(&next));
        svindb_core::fail::point("rootcat.after_publish");
        Ok(next)
    }

    /// Правка, живущая только в памяти: привязка и отвязка runtime.
    ///
    /// Поколение не растёт и на диск ничего не идёт — присутствие `Node` не
    /// является durable-фактом. Вызывается только под `lifecycle` и только
    /// когда запись находится в переходном состоянии, то есть невидима снаружи.
    fn mutate(
        &self,
        change: impl FnOnce(&mut Catalog) -> Result<(), String>,
    ) -> Result<(), String> {
        let base = self.catalog();
        let mut next = (*base).clone();
        change(&mut next)?;
        self.publish(Arc::new(next));
        Ok(())
    }

    fn publish(&self, next: Arc<Catalog>) {
        *self.current.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
    }

    /// Запускает один последовательный eager-warm на весь process budget.
    /// Поток принадлежит Registry и присоединяется до освобождения root-lock.
    pub fn start_warm(&self) -> Result<(), String> {
        let mut slot = self.warm_thread.lock().map_err(|_| "держатель прогрева отравлен")?;
        if slot.is_some() {
            return Err("общий прогрев уже запущен".into());
        }
        let nodes: Vec<(String, Weak<Node>)> = self
            .all_nodes()
            .into_iter()
            .map(|(name, node)| (name, Arc::downgrade(&node)))
            .collect();
        let stop = Arc::clone(&self.warm_stop);
        let handle = std::thread::Builder::new()
            .name("multi-warm".into())
            .spawn(move || {
                let cap = svindb_core::mem::available().map_or(0, |ram| (ram / 2) as usize);
                let mut budget = svindb_core::mmap::WarmBudget::new(cap, super::WARM_MS);
                for (_, weak) in nodes {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let Some(node) = weak.upgrade() else { continue };
                    let segs = node.live.read().expect("набор сегментов").clone();
                    let _ = segs.warm_within(&mut budget);
                    if node.writer.is_some() && !budget.stopped() {
                        let _ = segs.warm_ids();
                    }
                    if budget.stopped() {
                        break;
                    }
                }
            })
            .map_err(|e| format!("поток общего прогрева не создан: {e}"))?;
        *slot = Some(handle);
        Ok(())
    }

    /// Создаёт индекс от staging до работающего runtime. В карту он попадает
    /// последним: до успешного recovery ни один запрос не увидит полуиндекс.
    pub fn create_with_mapping(
        &self,
        name: &str,
        decls: &[svindb_core::schema::Decl],
        settings: &svindb_core::settings::Settings,
        mapping: Option<&[u8]>,
        create: CreateOptions,
    ) -> Result<Arc<Node>, CatalogError> {
        valid_name(name)
            .map_err(|why| CatalogError::InvalidName { name: name.to_string(), why })?;
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let _settings_lane =
            self.options.settings_lane.lock().map_err(|_| busy("полоса настроек отравлена"))?;
        let base = self.catalog();
        let count = base.open_count();
        if count >= self.max_indices {
            return Err(CatalogError::TooManyOpenIndices { open: count, limit: self.max_indices });
        }
        // Одно пространство имён с aliases: иначе быстрый путь запроса, который
        // сперва ищет индекс по имени, молча затенил бы alias.
        if base.name_taken(name) {
            return Err(CatalogError::IndexAlreadyExists { name: name.to_string() });
        }
        let tags = catalog::normalize_tags(&create.tags)?;
        catalog::check_meta(create.description.as_deref(), &tags)?;
        for (alias, _) in &create.aliases {
            valid_name(alias)
                .map_err(|why| CatalogError::InvalidName { name: alias.clone(), why })?;
            if base.by_name(alias).is_some() || alias == name {
                return Err(CatalogError::AliasIsIndexName { name: alias.clone() });
            }
        }
        settings.check().map_err(|why| CatalogError::SettingsRejected { why })?;
        self.options
            .ingest_budget
            .validate(None, settings.flush_mb)
            .map_err(|why| CatalogError::SettingsRejected { why })?;
        let uuid = catalog::Uuid::mint()?;
        let entry = catalog::Entry {
            uuid,
            name: name.to_string(),
            state: State::Creating,
            blocks: catalog::Blocks::none(),
            created_ms: now_ms(),
            description: create.description,
            tags,
            node: None,
        };
        // Первый durable шаг: имя занято под `Creating`. Авария после него
        // оставляет запись, которую восстановление уберёт вместе с каталогом на
        // диске, а не полуиндекс, всплывающий сам по себе.
        self.commit(catalog::TXN_INDEX_ADD, &catalog::body::index_add(&entry))
            .map_err(CatalogError::Durability)?;
        svindb_core::fail::point("rootcat.create.after_reserve");

        let built = (|| -> Result<(String, Arc<Node>), String> {
            let stage = self.root.stage(name, &uuid)?;
            let published = (|| {
                let stage_dir = stage.to_string_lossy().to_string();
                svindb_core::settings::save(&stage_dir, settings)?;
                if let Some(mapping) = mapping {
                    super::mapping::Rules::save(&stage_dir, mapping)?;
                }
                svindb_core::schema::declare_all(&stage_dir, decls)
                    .map_err(|e| format!("схема индекса {name:?}: {e}"))?;
                self.root.publish(name, &stage)
            })();
            let path = match published {
                Ok(path) => path,
                Err(error) => {
                    let _ = self.root.abort_stage(&stage);
                    return Err(error);
                }
            };
            svindb_core::fail::point("rootcat.create.after_publish_dir");
            let dir = path.to_string_lossy().to_string();
            super::host::forget_index_bytes(&dir);
            let node = open_local_index(name, &dir, &self.options)?;
            self.options.ingest_budget.insert(name, &node)?;
            Ok((dir, node))
        })();
        let node = match built {
            Ok((_, node)) => node,
            Err(error) => {
                // Компенсация: занятое имя обязано освободиться сразу, а не
                // ждать перезапуска.
                let _ = self.options.ingest_budget.remove(name);
                let _ = self.root.remove(name);
                let _ = self.commit(catalog::TXN_INDEX_REMOVE, &catalog::body::index_remove(&uuid));
                return Err(CatalogError::Durability(error));
            }
        };
        let current = self.catalog();
        let mut alias_sets = Vec::with_capacity(create.aliases.len());
        for (alias, is_write) in create.aliases {
            let mut bindings = current.bindings(&alias).unwrap_or_default().to_vec();
            bindings.retain(|binding| binding.target != uuid);
            bindings.push(catalog::Binding { target: uuid, is_write });
            bindings.sort_by_key(|binding| binding.target);
            if bindings.iter().filter(|binding| binding.is_write == Some(true)).count() > 1 {
                let _ = self.options.ingest_budget.remove(name);
                drop(node);
                let _ = self.root.remove(name);
                let _ = self.commit(catalog::TXN_INDEX_REMOVE, &catalog::body::index_remove(&uuid));
                return Err(CatalogError::MultipleWriteIndices { alias });
            }
            alias_sets.push(catalog::AliasSet { alias, bindings });
        }
        let publish = catalog::CreatePublish {
            entry: catalog::Entry { state: State::Open, ..entry.clone() },
            aliases: alias_sets,
        };
        // Второй durable шаг: индекс и aliases становятся видимы вместе и
        // только после recovery/checkpoint нового runtime.
        if let Err(error) =
            self.commit_with(catalog::TXN_INDEX_PUBLISH, &publish.encode(), |next| {
                next.attach(name, Arc::clone(&node))
            })
        {
            let _ = self.options.ingest_budget.remove(name);
            drop(node);
            let _ = self.root.remove(name);
            let _ = self.commit(catalog::TXN_INDEX_REMOVE, &catalog::body::index_remove(&uuid));
            return Err(CatalogError::Durability(error));
        }
        if let Err(error) = self.rebalance_ingest() {
            let _ = self.mutate(|next| next.detach(name).map(|_| ()));
            let _ = self.options.ingest_budget.remove(name);
            drop(node);
            let _ = self.root.remove(name);
            let _ = self.commit(catalog::TXN_INDEX_REMOVE, &catalog::body::index_remove(&uuid));
            return Err(CatalogError::SettingsRejected { why: error });
        }
        Ok(node)
    }

    /// Закрывает runtime, ждёт уже начатые запросы, освобождает writer lock и
    /// только затем атомарно скрывает каталог на диске.
    pub fn delete(&self, name: &str) -> Result<(), CatalogError> {
        valid_name(name)
            .map_err(|why| CatalogError::InvalidName { name: name.to_string(), why })?;
        let _lifecycle = self.lifecycle.lock().map_err(|_| busy("замок каталога отравлен"))?;
        let base = self.catalog();
        let entry = base
            .by_name(name)
            .ok_or_else(|| CatalogError::IndexNotFound { name: name.to_string() })?;
        delete_allowed(entry)?;
        let uuid = entry.uuid;
        if entry.state == State::Closed {
            self.commit(
                catalog::TXN_INDEX_STATE,
                &catalog::body::index_state(&uuid, State::Deleting),
            )
            .map_err(CatalogError::Durability)?;
            drop(base);
            self.root.remove(name).map_err(CatalogError::Durability)?;
            self.commit(catalog::TXN_INDEX_REMOVE, &catalog::body::index_remove(&uuid))
                .map_err(CatalogError::Durability)?;
            return Ok(());
        }
        if entry.state != State::Open {
            return Err(busy(&format!(
                "индекс {name:?} находится в состоянии {}",
                entry.state.name()
            )));
        }
        drop(base);
        let (dir, mut node) = {
            let _settings_lane =
                self.options.settings_lane.lock().map_err(|_| busy("полоса настроек отравлена"))?;
            let dir = self
                .root
                .index_path(name)
                .map_err(CatalogError::Durability)?
                .to_string_lossy()
                .to_string();
            // Сначала durable-пометка, затем публикация: с этого момента новые
            // запросы имени уже не получают, а авария не воскресит удалённое.
            self.commit(
                catalog::TXN_INDEX_STATE,
                &catalog::body::index_state(&uuid, State::Deleting),
            )
            .map_err(CatalogError::Durability)?;
            svindb_core::fail::point("rootcat.delete.after_mark");
            let mut detached = None;
            self.mutate(|next| {
                detached = next.detach(name)?;
                Ok(())
            })
            .map_err(CatalogError::Durability)?;
            let node =
                detached.ok_or_else(|| CatalogError::IndexNotFound { name: name.to_string() })?;
            // Снятие с бюджета до ожидания `Arc`, а не после: пока `Weak`
            // поднимается, `rebalance` продолжал бы считать `flush_mb`
            // уходящего индекса в доле живых.
            if let Err(error) = self.options.ingest_budget.remove(name) {
                let _ = self.mutate(|next| next.attach(name, Arc::clone(&node)));
                let _ = self.commit(
                    catalog::TXN_INDEX_STATE,
                    &catalog::body::index_state(&uuid, State::Open),
                );
                return Err(CatalogError::SettingsRejected { why: error });
            }
            if let Err(error) = self.rebalance_ingest() {
                let _ = self.options.ingest_budget.insert(name, &node);
                let _ = self.mutate(|next| next.attach(name, Arc::clone(&node)));
                let _ = self.commit(
                    catalog::TXN_INDEX_STATE,
                    &catalog::body::index_state(&uuid, State::Open),
                );
                return Err(CatalogError::SettingsRejected { why: error });
            }
            (dir, node)
        };
        // Lifecycle остаётся закрыт до конца DELETE, но полоса settings уже
        // отпущена. Запрос, который получил Arc до скрытия имени и ждал эту
        // полосу, должен завершиться; держать её здесь и одновременно ждать
        // Arc означало бы взаимную блокировку.
        loop {
            match Arc::try_unwrap(node) {
                Ok(node) => {
                    drop(node);
                    break;
                }
                Err(still_shared) => {
                    node = still_shared;
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        }
        super::host::forget_index_bytes(&dir);
        if let Err(error) = self.root.remove(name) {
            // Каталог остался на месте, значит состояние можно восстановить и
            // не оставлять живой процесс с индексом, который появится только
            // после restart.
            let path = self.root.index_path(name).map_err(CatalogError::Durability)?;
            let dir = path.to_string_lossy().to_string();
            if let Ok(reopened) = open_local_index(name, &dir, &self.options) {
                if let Ok(_settings_lane) = self.options.settings_lane.lock() {
                    let _ = self.options.ingest_budget.insert(name, &reopened);
                    let _ = self.mutate(|next| next.attach(name, reopened));
                    let _ = self.rebalance_ingest();
                }
            }
            let _ = self
                .commit(catalog::TXN_INDEX_STATE, &catalog::body::index_state(&uuid, State::Open));
            return Err(CatalogError::Durability(error));
        }
        svindb_core::fail::point("rootcat.delete.after_unlink");
        self.commit(catalog::TXN_INDEX_REMOVE, &catalog::body::index_remove(&uuid))
            .map_err(CatalogError::Durability)?;
        Ok(())
    }

    /// Делит общий ingest-budget между индексами с автоматическим `flush_mb`.
    /// Явные пределы сначала вычитаются из доли; их сумма не может обещать
    /// процессу памяти больше, чем у него есть.
    fn rebalance_ingest(&self) -> Result<(), String> {
        self.options.ingest_budget.rebalance()
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        self.warm_stop.store(true, Ordering::Release);
        let slot = self.warm_thread.get_mut().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(thread) = slot.take() {
            let _ = thread.join();
        }
    }
}

/// Проверяет имя до любого обращения к файловой системе.
///
/// Правила совместимы с безопасным подмножеством Elasticsearch: lowercase,
/// не `.`/`..`, без служебного первого знака, пробелов, разделителей пути и
/// знаков, которые разные файловые системы трактуют по-разному.
pub fn valid_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("имя индекса пусто".to_string());
    }
    if name.len() > 255 {
        return Err(format!("имя индекса длиннее 255 байт: {}", name.len()));
    }
    if name == "." || name == ".." {
        return Err(format!("имя индекса {name:?} запрещено"));
    }
    if name.starts_with(['_', '-', '+']) {
        return Err(format!("имя индекса {name:?} не может начинаться с _, - или +"));
    }
    if name.chars().any(char::is_uppercase) {
        return Err(format!("имя индекса {name:?} должно быть в нижнем регистре"));
    }
    if name.chars().any(|c| c.is_control() || c.is_whitespace() || "\\/*?\"<>|,#:".contains(c)) {
        return Err(format!("имя индекса {name:?} содержит запрещённый знак"));
    }
    Ok(())
}

pub fn encoded(name: &str) -> String {
    let mut out = String::with_capacity(2 + name.len() * 2);
    out.push_str("i-");
    for byte in name.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn write_json_atomic(dir: &Path, name: &str, value: &serde_json::Value) -> Result<(), String> {
    let final_path = dir.join(name);
    let unique = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let temp_path = dir.join(format!(".{name}.tmp-{}-{unique}", std::process::id()));
    let bytes = serde_json::to_vec(value).map_err(|e| format!("{name} не кодируется: {e}"))?;
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|e| format!("{} не создаётся: {e}", temp_path.display()))?;
        file.write_all(&bytes)
            .and_then(|_| file.write_all(b"\n"))
            .and_then(|_| file.sync_all())
            .map_err(|e| format!("{} не записывается: {e}", temp_path.display()))?;
        fs::rename(&temp_path, &final_path).map_err(|e| {
            format!("{} не публикуется как {}: {e}", temp_path.display(), final_path.display())
        })?;
        sync_dir(dir)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp_path);
    }
    result
}

/// Убирает только наши незавершённые попытки первого marker. Произвольный
/// непустой каталог по-прежнему не переосмысляется как multi-root.
fn sweep_marker_temps(path: &Path) -> Result<(), String> {
    let mut changed = false;
    for entry in fs::read_dir(path)
        .map_err(|e| format!("корень нескольких индексов {} не читается: {e}", path.display()))?
    {
        let entry = entry.map_err(|e| format!("элемент {} не читается: {e}", path.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !marker_temp(name) {
            continue;
        }
        let kind = entry
            .file_type()
            .map_err(|e| format!("тип {} не читается: {e}", entry.path().display()))?;
        if kind.is_dir() {
            return Err(format!(
                "остаток marker {} оказался каталогом и не удаляется автоматически",
                entry.path().display()
            ));
        }
        fs::remove_file(entry.path())
            .map_err(|e| format!("остаток marker {} не удаляется: {e}", entry.path().display()))?;
        changed = true;
    }
    if changed {
        sync_dir(path)?;
    }
    Ok(())
}

fn marker_temp(name: &str) -> bool {
    let prefix = format!(".{LAYOUT_FILE}.tmp-");
    name.strip_prefix(&prefix).and_then(|tail| tail.split_once('-')).is_some_and(|(pid, seq)| {
        !pid.is_empty()
            && !seq.is_empty()
            && pid.bytes().all(|b| b.is_ascii_digit())
            && seq.bytes().all(|b| b.is_ascii_digit())
    })
}

fn sync_dir(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|e| format!("каталог {} не синхронизируется: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("svindb-multi-{name}-{}-{n}", std::process::id()))
    }

    #[test]
    fn names_cannot_escape_or_collide_by_case() {
        for bad in [
            "", ".", "..", "_system", "-hidden", "+plus", "Orders", "a/b", "a\\b", "a b", "a,b",
            "a#b",
        ] {
            assert!(valid_name(bad).is_err(), "имя {bad:?} принято");
        }
        for good in ["orders", "logs-2026.08", "заказы", "tenant_42"] {
            assert!(valid_name(good).is_ok(), "имя {good:?} отвергнуто");
        }
    }

    #[test]
    fn a_nonempty_legacy_directory_is_never_reinterpreted() {
        let dir = temp("legacy");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("segments.tsv"), b"seg0001\n").unwrap();
        assert_eq!(classify(&dir).unwrap(), LayoutKind::Legacy);
        let error = Root::open(&dir, true).unwrap_err();
        assert!(error.contains("Старый одноиндексный каталог"), "{error}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_crashed_first_marker_write_is_recovered_but_foreign_content_is_not() {
        let dir = temp("marker-tail");
        fs::create_dir_all(&dir).unwrap();
        let tail = dir.join(format!(".{LAYOUT_FILE}.tmp-999-1"));
        fs::write(&tail, b"{\"format\":").unwrap();
        assert_eq!(classify(&dir).unwrap(), LayoutKind::Multi);
        let root = Root::open(&dir, true).expect("остаток marker не убран");
        assert!(!tail.exists());
        assert!(root.path().join(LAYOUT_FILE).is_file());
        drop(root);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_new_root_defaults_to_multi_and_lost_multi_marker_never_becomes_legacy() {
        let missing = temp("missing-layout");
        assert_eq!(classify(&missing).unwrap(), LayoutKind::New);
        fs::create_dir_all(&missing).unwrap();
        assert_eq!(classify(&missing).unwrap(), LayoutKind::New);
        fs::create_dir(missing.join(INDICES)).unwrap();
        assert_eq!(classify(&missing).unwrap(), LayoutKind::Multi);
        let error = Root::open(&missing, true).unwrap_err();
        assert!(error.contains("marker"), "{error}");
        fs::remove_dir_all(missing).unwrap();
    }

    #[test]
    fn only_published_indices_are_discovered_and_delete_is_final() {
        let dir = temp("lifecycle");
        let root = Root::open(&dir, true).unwrap();
        let uuid = catalog::Uuid::mint().unwrap();
        let stage = root.stage("orders", &uuid).unwrap();
        assert!(root.discover().unwrap().is_empty());
        fs::write(stage.join("payload"), b"ok").unwrap();
        let published = root.publish("orders", &stage).unwrap();
        let found = root.discover().unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "orders");
        assert_eq!(found[0].path, published);
        // UUID переживает публикацию: привязка alias указывает на него, а не
        // на имя, поэтому потерять его здесь значило бы осиротить привязки.
        assert_eq!(found[0].uuid, Some(uuid));
        assert_eq!(fs::read(published.join("payload")).unwrap(), b"ok");
        root.remove("orders").unwrap();
        assert!(root.discover().unwrap().is_empty());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_incomplete_create_is_swept_on_restart() {
        let dir = temp("sweep");
        let root = Root::open(&dir, true).unwrap();
        let stage = root.stage("orders", &catalog::Uuid::mint().unwrap()).unwrap();
        assert!(stage.exists());
        drop(root);
        let root = Root::open(&dir, true).unwrap();
        assert!(!stage.exists());
        assert!(root.discover().unwrap().is_empty());
        fs::remove_dir_all(dir).unwrap();
    }
}
