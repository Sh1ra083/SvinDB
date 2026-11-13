//! Сохранённые правила Elasticsearch mapping, которые не помещаются в `Kind`.
//!
//! Сама схема полей живёт в core registry. Здесь остаётся только семантика
//! проекции: source-path multi-field, `ignore_above`, `constant_keyword` и
//! `scaled_float`. Файл входит в snapshot и читается до открытия порта.

use std::collections::HashMap;
use std::path::Path;

pub const FILE: &str = "mapping.json";
const MAX_BYTES: u64 = 4 << 20;

/// Неизменяемый способ хранения исходного документа.
///
/// Режим принадлежит mapping, а не живым эксплуатационным настройкам: поменять
/// его после появления сегментов означало бы получить индекс, где часть
/// документов имеет `_source`, а часть — нет. Поэтому значение читается из
/// сохранённого тела CREATE и применяется одинаково при первом открытии,
/// restart и rollover.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SourceMode {
    #[default]
    Stored,
    Disabled,
}

impl SourceMode {
    pub fn stores(self) -> bool {
        self == SourceMode::Stored
    }

    pub fn name(self) -> &'static str {
        match self {
            SourceMode::Stored => "stored",
            SourceMode::Disabled => "disabled",
        }
    }

    /// Читает обе совместимые формы Elasticsearch и сводит их к одному режиму:
    /// `mappings._source.enabled` и `settings.index.mapping.source.mode`.
    pub fn from_root(root: &serde_json::Value) -> Result<SourceMode, String> {
        let mapping = root
            .get("mappings")
            .and_then(serde_json::Value::as_object)
            .and_then(|mappings| mappings.get("_source"))
            .map(|source| source_spec(source, "mappings._source"))
            .transpose()?;
        let setting = root
            .get("settings")
            .and_then(serde_json::Value::as_object)
            .and_then(|settings| settings.get("index"))
            .and_then(serde_json::Value::as_object)
            .and_then(|index| index.get("mapping"))
            .and_then(serde_json::Value::as_object)
            .and_then(|mapping| mapping.get("source"))
            .map(|source| source_spec(source, "settings.index.mapping.source"))
            .transpose()?;
        match (mapping, setting) {
            (Some(left), Some(right)) if left != right => {
                Err("mappings._source и settings.index.mapping.source задают разные режимы".into())
            }
            (Some(mode), _) | (_, Some(mode)) => Ok(mode),
            (None, None) => Ok(SourceMode::Stored),
        }
    }
}

fn source_spec(value: &serde_json::Value, path: &str) -> Result<SourceMode, String> {
    let object = value.as_object().ok_or_else(|| format!("{path} должен быть объектом"))?;
    for key in object.keys() {
        if !matches!(key.as_str(), "enabled" | "mode") {
            return Err(format!(
                "{path}.{key} не поддерживается; доступны enabled и mode без source filtering"
            ));
        }
    }
    let enabled = object
        .get("enabled")
        .map(|value| {
            value
                .as_bool()
                .map(|enabled| if enabled { SourceMode::Stored } else { SourceMode::Disabled })
                .ok_or_else(|| format!("{path}.enabled обязан быть bool"))
        })
        .transpose()?;
    let mode = object
        .get("mode")
        .map(|value| {
            let mode = value.as_str().ok_or_else(|| format!("{path}.mode обязан быть строкой"))?;
            match mode {
                "stored" => Ok(SourceMode::Stored),
                "disabled" => Ok(SourceMode::Disabled),
                "synthetic" => Err(format!(
                    "{path}.mode=synthetic пока не поддерживается: SvinDB не будет выдавать реконструированный JSON за lossless source"
                )),
                other => Err(format!(
                    "{path}.mode={other:?} неизвестен; ожидается stored или disabled"
                )),
            }
        })
        .transpose()?;
    match (enabled, mode) {
        (Some(left), Some(right)) if left != right => {
            Err(format!("{path}.enabled и {path}.mode противоречат друг другу"))
        }
        (Some(mode), _) | (_, Some(mode)) => Ok(mode),
        (None, None) => Ok(SourceMode::Stored),
    }
}

#[derive(Clone, Default)]
pub struct Rule {
    pub source_path: String,
    pub ignore_above: Option<usize>,
    pub constant: Option<serde_json::Value>,
    pub scaling_factor: Option<f64>,
}

#[derive(Clone, Default)]
pub struct Rules {
    by_path: HashMap<String, Rule>,
    root: Option<ObjectRule>,
    source_mode: SourceMode,
}

#[derive(Clone)]
struct ObjectRule {
    dynamic: Dynamic,
    fields: HashMap<String, FieldRule>,
}

#[derive(Clone)]
struct FieldRule {
    /// `enabled:false` принимает поддерево только в `_source` и намеренно не
    /// пытается заглянуть внутрь него.
    source_only: bool,
    object: Option<ObjectRule>,
}

/// Как читать путь при временной нормализации индексируемой проекции.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectionPath {
    Indexed,
    SourceOnly,
    Ignored,
}

#[derive(Clone, Copy)]
enum Dynamic {
    Strict,
    False,
}

impl Rules {
    pub fn from_bytes(bytes: &[u8]) -> Result<Rules, String> {
        let root: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|e| format!("mapping.json: {e}"))?;
        let source_mode = SourceMode::from_root(&root)?;
        let Some(mappings) = root.get("mappings").and_then(serde_json::Value::as_object) else {
            return Ok(Rules { source_mode, ..Rules::default() });
        };
        let properties = mappings
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .ok_or("mapping.json: mappings.properties отсутствует")?;
        let dynamic = dynamic_of(mappings.get("dynamic"), None, "mappings")?;
        let mut rules = Rules { source_mode, ..Rules::default() };
        collect(properties, "", None, &mut rules)?;
        rules.root = Some(object_rule(properties, dynamic, "")?);
        Ok(rules)
    }

    pub fn load(dir: &str) -> Result<Rules, String> {
        let path = Path::new(dir).join(FILE);
        if !path.exists() {
            return Ok(Rules::default());
        }
        let bytes = svindb_core::fs::read_regular_limited(&path, MAX_BYTES, false)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        Rules::from_bytes(&bytes)
    }

    pub fn save(dir: &str, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() as u64 > MAX_BYTES {
            return Err(format!("mapping: {} байт при потолке {MAX_BYTES}", bytes.len()));
        }
        // Разобрать до записи: restart не должен впервые узнать, что файл
        // негоден, после уже подтверждённого CREATE.
        Rules::from_bytes(bytes)?;
        svindb_core::fs::atomic_write(&Path::new(dir).join(FILE), bytes, 0o600)
            .map_err(|e| format!("mapping не сохранён: {e}"))
    }

    pub fn get(&self, path: &str) -> Option<&Rule> {
        self.by_path.get(path)
    }

    pub fn transformed(&self) -> usize {
        self.by_path.len()
    }

    pub fn source_mode(&self) -> SourceMode {
        self.source_mode
    }

    /// Проверяет `dynamic` по настоящей иерархии mapping.
    ///
    /// Одной проверки верхних ключей недостаточно: `meta.known` делал весь
    /// объект `meta` известным, и `meta.silently_lost` проходил с HTTP 200.
    /// Теперь каждый уровень либо перечисляет допустимые свойства, либо явно
    /// объявлен `dynamic:false`/`enabled:false`.
    pub(crate) fn projection_path(&self, path: &str) -> ProjectionPath {
        let Some(mut object) = self.root.as_ref() else { return ProjectionPath::Indexed };
        let mut parts = path.split('.').peekable();
        while let Some(part) = parts.next() {
            let Some(field) = object.fields.get(part) else {
                return match object.dynamic {
                    Dynamic::Strict => ProjectionPath::Indexed,
                    Dynamic::False => ProjectionPath::Ignored,
                };
            };
            if field.source_only {
                return ProjectionPath::SourceOnly;
            }
            if parts.peek().is_some() {
                match field.object.as_ref() {
                    Some(child) => object = child,
                    None => return ProjectionPath::Indexed,
                }
            }
        }
        ProjectionPath::Indexed
    }

    pub fn validate_projection(&self, value: &serde_json::Value) -> Result<(), String> {
        let Some(root) = self.root.as_ref() else { return Ok(()) };
        validate_object(value, root, "", true)
    }
}

fn dynamic_of(
    value: Option<&serde_json::Value>,
    inherited: Option<Dynamic>,
    path: &str,
) -> Result<Dynamic, String> {
    match value {
        None => inherited.ok_or_else(|| {
            format!(
                "{path}.dynamic отсутствует: Elasticsearch по умолчанию создаёт поля на ходу, а \
                 SvinDB не будет молча подменять это source-only хранением; задайте strict или false"
            )
        }),
        Some(serde_json::Value::String(value)) if value == "strict" => Ok(Dynamic::Strict),
        Some(serde_json::Value::Bool(false)) => Ok(Dynamic::False),
        Some(serde_json::Value::Bool(true)) | Some(serde_json::Value::String(_)) => Err(format!(
            "{path}.dynamic требует динамического изменения схемы; поддерживаются strict и false"
        )),
        Some(_) => Err(format!("{path}.dynamic должен быть strict или false")),
    }
}

fn object_rule(
    properties: &serde_json::Map<String, serde_json::Value>,
    inherited: Dynamic,
    prefix: &str,
) -> Result<ObjectRule, String> {
    let mut rule =
        ObjectRule { dynamic: inherited, fields: HashMap::with_capacity(properties.len()) };
    // Явный объект разбирается до его dotted-соседей: его dynamic должен
    // наследоваться и полями, объявленными через полный путь.
    let mut properties: Vec<_> = properties.iter().collect();
    properties.sort_by_key(|(left, _)| *left);
    for (name, value) in properties {
        let parts: Vec<_> = name.split('.').collect();
        if parts.iter().any(|part| part.is_empty()) {
            return Err(format!("mapping field {name:?}: пустая часть пути"));
        }
        insert_field_rule(&mut rule, &parts, value, prefix)?;
    }
    Ok(rule)
}

fn insert_field_rule(
    rule: &mut ObjectRule,
    parts: &[&str],
    value: &serde_json::Value,
    prefix: &str,
) -> Result<(), String> {
    let name = parts[0];
    let path = if prefix.is_empty() { name.to_string() } else { format!("{prefix}.{name}") };
    if parts.len() > 1 {
        let field = rule.fields.entry(name.to_string()).or_insert_with(|| FieldRule {
            source_only: false,
            object: Some(ObjectRule { dynamic: rule.dynamic, fields: HashMap::new() }),
        });
        let object = field.object.as_mut().ok_or_else(|| {
            format!("mapping: конфликт пути {path:?} — скаляр или enabled:false не может содержать поля")
        })?;
        return insert_field_rule(object, &parts[1..], value, &path);
    }
    if rule.fields.contains_key(name) {
        return Err(format!("mapping: конфликт пути {path:?} — поле объявлено дважды"));
    }
    let spec = value.as_object().ok_or_else(|| format!("mapping field {path:?} не объект"))?;
    let source_only = spec.get("enabled").and_then(serde_json::Value::as_bool) == Some(false);
    let object = if source_only {
        None
    } else if let Some(children) = spec.get("properties").and_then(serde_json::Value::as_object) {
        let dynamic = dynamic_of(spec.get("dynamic"), Some(rule.dynamic), &path)?;
        Some(object_rule(children, dynamic, &path)?)
    } else {
        None
    };
    rule.fields.insert(name.to_string(), FieldRule { source_only, object });
    Ok(())
}

fn validate_object(
    value: &serde_json::Value,
    rule: &ObjectRule,
    path: &str,
    root: bool,
) -> Result<(), String> {
    match value {
        serde_json::Value::Null => Ok(()),
        serde_json::Value::Array(items) => {
            for (at, item) in items.iter().enumerate() {
                validate_object(item, rule, &format!("{path}[{at}]"), false)?;
            }
            Ok(())
        }
        serde_json::Value::Object(object) => {
            for (name, child) in object {
                // В нативном контракте `id` может находиться в документе как
                // транспортный идентификатор. ES `_doc/{id}` его сюда не
                // добавляет, но запрещать старый атомарный контракт нельзя.
                if root && name == "id" {
                    continue;
                }
                let child_path =
                    if path.is_empty() { name.clone() } else { format!("{path}.{name}") };
                let Some(field) = rule.fields.get(name) else {
                    match rule.dynamic {
                        Dynamic::Strict => {
                            return Err(format!(
                                "dynamic strict: поле {child_path:?} отсутствует в mapping"
                            ))
                        }
                        Dynamic::False => continue,
                    }
                };
                if field.source_only {
                    continue;
                }
                if let Some(nested) = field.object.as_ref() {
                    validate_object(child, nested, &child_path, false)?;
                }
            }
            Ok(())
        }
        // Форму известного object/nested проверит основной разбор, где ошибка
        // названа вместе с документом и полем. Здесь решается только unknown.
        _ => Ok(()),
    }
}

fn collect(
    properties: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
    source_parent: Option<&str>,
    rules: &mut Rules,
) -> Result<(), String> {
    for (name, value) in properties {
        let path = if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
        let spec = value.as_object().ok_or_else(|| format!("mapping field {path:?} не объект"))?;
        let source_path = source_parent.unwrap_or(&path).to_string();
        let ignore_above = spec
            .get("ignore_above")
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| format!("{path}.ignore_above не помещается в usize"))
            })
            .transpose()?;
        let constant = spec.get("value").cloned();
        let scaling_factor = spec.get("scaling_factor").and_then(serde_json::Value::as_f64);
        if source_parent.is_some()
            || ignore_above.is_some()
            || constant.is_some()
            || scaling_factor.is_some()
        {
            rules.by_path.insert(
                path.clone(),
                Rule { source_path: source_path.clone(), ignore_above, constant, scaling_factor },
            );
        }
        if spec.get("enabled").and_then(serde_json::Value::as_bool) != Some(false) {
            if let Some(children) = spec.get("properties").and_then(serde_json::Value::as_object) {
                collect(children, &path, None, rules)?;
            }
        }
        if let Some(fields) = spec.get("fields").and_then(serde_json::Value::as_object) {
            collect(fields, &path, Some(&source_path), rules)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(raw: &str) -> serde_json::Value {
        serde_json::from_str(raw).unwrap()
    }

    #[test]
    fn dotted_and_nested_mappings_accept_both_document_forms_without_losing_source() {
        use svindb_core::index::Kind;
        let properties = [
            serde_json::json!({"@timestamp":{"type":"date"},"host.name":{"type":"keyword","ignore_above":32}}),
            serde_json::json!({"@timestamp":{"type":"date"},"host":{"properties":{"name":{"type":"keyword","ignore_above":32}}}}),
        ];
        let documents = [
            serde_json::json!({"@timestamp":"2026-11-13T10:00:00Z","host.name":"alpha"}),
            serde_json::json!({"@timestamp":"2026-11-13T10:00:00Z","host":{"name":"alpha"}}),
        ];
        for properties in properties {
            let root = serde_json::json!({"mappings":{"dynamic":"strict","properties":properties}});
            let rules = Rules::from_bytes(&serde_json::to_vec(&root).unwrap()).unwrap();
            assert_eq!(rules.get("host.name").unwrap().ignore_above, Some(32));
            let plan = crate::nested::Plan::build_with(
                &["@timestamp".into(), "host.name".into(), crate::nested::SOURCE_FIELD.into()],
                &[Kind::DateIso, Kind::Keyword, Kind::Keyword],
                &[0, 0, 0],
                &[String::new()],
                &rules,
            );
            for source in &documents {
                let mut report = crate::intake::Report::default();
                let doc = plan.take(source, 0, &mut report).unwrap();
                assert!(doc.cells.iter().any(|(field, _, value)| *field == 1 && value == "alpha"));
                let stored = doc.cells.iter().find(|(field, _, _)| *field == 2).unwrap();
                assert_eq!(serde_json::from_str::<serde_json::Value>(&stored.2).unwrap(), *source);
                assert!(report.clean());
            }
            assert!(rules
                .validate_projection(&serde_json::json!({"host":{"unknown":"x"}}))
                .is_err());
        }
    }

    #[test]
    fn dotted_mapping_rejects_scalar_duplicate_and_source_only_path_collisions() {
        for properties in [
            serde_json::json!({"host":{"type":"keyword"},"host.name":{"type":"keyword"}}),
            serde_json::json!({"host":{"enabled":false},"host.name":{"type":"keyword"}}),
            serde_json::json!({"host":{"properties":{"name":{"type":"keyword"}}},"host.name":{"type":"keyword"}}),
            serde_json::json!({"host..name":{"type":"keyword"}}),
        ] {
            let root = serde_json::json!({"mappings":{"dynamic":"strict","properties":properties}});
            assert!(Rules::from_bytes(&serde_json::to_vec(&root).unwrap()).is_err(), "{root}");
        }
        let root = serde_json::json!({"mappings":{"dynamic":"strict","properties":{
            "host":{"dynamic":"strict","properties":{"id":{"type":"keyword"}}},
            "host.name":{"type":"keyword"}
        }}});
        let rules = Rules::from_bytes(&serde_json::to_vec(&root).unwrap()).unwrap();
        rules.validate_projection(&serde_json::json!({"host":{"id":"1","name":"alpha"}})).unwrap();
        assert!(rules.validate_projection(&serde_json::json!({"host":{"unknown":"x"}})).is_err());
    }

    #[test]
    fn dynamic_false_unknown_fields_do_not_skip_declared_strict_objects() {
        let mapping = serde_json::json!({"mappings":{"dynamic":false,"properties":{
            "host":{"dynamic":"strict","properties":{"id":{"type":"keyword"}}},
            "host.name":{"type":"keyword"}
        }}});
        let rules = Rules::from_bytes(&serde_json::to_vec(&mapping).unwrap()).unwrap();
        rules
            .validate_projection(&serde_json::json!({"a_unknown":true,"host":{"name":"alpha"}}))
            .unwrap();
        assert!(rules
            .validate_projection(&serde_json::json!({"a_unknown":true,"host":{"unknown":"x"}}))
            .is_err());
    }

    #[test]
    fn source_mode_accepts_both_static_elasticsearch_forms() {
        let classic = json(r#"{"mappings":{"_source":{"enabled":false}}}"#);
        assert_eq!(SourceMode::from_root(&classic).unwrap(), SourceMode::Disabled);

        let setting = json(r#"{"settings":{"index":{"mapping":{"source":{"mode":"disabled"}}}}}"#);
        assert_eq!(SourceMode::from_root(&setting).unwrap(), SourceMode::Disabled);

        let stored = json(r#"{"mappings":{"_source":{"enabled":true,"mode":"stored"}}}"#);
        assert_eq!(SourceMode::from_root(&stored).unwrap(), SourceMode::Stored);
    }

    #[test]
    fn source_mode_refuses_conflicts_filtering_and_fake_synthetic_source() {
        for raw in [
            r#"{"mappings":{"_source":{"enabled":false,"mode":"stored"}}}"#,
            r#"{"mappings":{"_source":{"enabled":false,"includes":["a"]}}}"#,
            r#"{"mappings":{"_source":{"mode":"synthetic"}}}"#,
            r#"{"mappings":{"_source":{"enabled":false}},"settings":{"index":{"mapping":{"source":{"mode":"stored"}}}}}"#,
        ] {
            assert!(SourceMode::from_root(&json(raw)).is_err(), "принят конфликтный source: {raw}");
        }
    }

    #[test]
    fn persisted_rules_keep_disabled_source_mode() {
        let rules = Rules::from_bytes(
            br#"{"mappings":{"dynamic":"strict","_source":{"enabled":false},"properties":{"body":{"type":"text"}}}}"#,
        )
        .unwrap();
        assert_eq!(rules.source_mode(), SourceMode::Disabled);
    }
}
