//! Разбор документа как он есть: вложенные объекты и массивы объектов.
//!
//! ## Что было
//!
//! Приём брал значение плоским ключом: `d.get("media.duration")` искал ключ,
//! буквально содержащий точку, а не спускался в `{"media":{"duration":…}}`.
//! Массив в скалярном поле отвергался, поле уходило в индекс пустым, и ответ
//! был двести. Массивы объектов формат держал, ядро умело, слияние переносило,
//! проверка целости сверяла — а залить их было нечем.
//!
//! ## Почему обход ведёт схема, а не документ
//!
//! Незаявленный путь не имеет ни номера, ни колонки, ни места в буфере. Обход
//! по документу означал бы, что схема правится на ходу, — а правка схемы у
//! живого узла требует сбросить буфер и пересобрать приёмник, и наполовину
//! подменённая схема опаснее перезапуска. Поэтому документ принимается как
//! есть, но вычитываются из него **объявленные** пути; всё прочее попадает в
//! перечень чужих ключей.
//!
//! ## Один разбор на два пути приёма
//!
//! Приёмных путей два — сетевой `_bulk` и офлайновый `svindb index`, — и они
//! уже расходились однажды: числовой `id` один переводил, другой терял, и
//! один и тот же корпус давал два разных индекса. Разбор поэтому живёт здесь,
//! а не в каждом из них.

use crate::intake::{self, Report, Taken};
use svindb_core::index::Kind;

/// Внутреннее lossless-хранилище исходного JSON.
///
/// Имя намеренно недоступно пользовательской схеме: это часть формата версии
/// документа, а не поле предметной области. Поле лежит в том же буфере,
/// сегменте и WAL, что индексируемая проекция, поэтому подтвердить только одну
/// из половин невозможно.
pub const SOURCE_FIELD: &str = "__svindb_source";

/// Индексируемая проекция, когда она прислана отдельно от `_source`.
///
/// У обычного документа проекция совпадает с source и второй копии нет. Поле
/// заполняется только для формы `{id, fields, _source}`.
pub const PROJECTION_FIELD: &str = "__svindb_projection";

/// Потолок числа элементов массива на документ и группу.
///
/// Взят с запасом в три порядка: в типичных документах самый длинный массив — пять
/// элементов. Потолок нужен не ради них, а против
/// случайного миллиона: полоса арности отказывает только на четырёх
/// гигабайтах, то есть **после** того, как документ принят, и валит уже весь
/// сегмент. Отказать надо на приёме и назвать документ.
pub const MAX_SLOTS: usize = 4096;

/// Какая половина точки лежит в поле-спутнике.
#[derive(Clone, Copy, PartialEq)]
enum GeoPart {
    Lat,
    Lon,
}

/// Поле внутри плана: номер, путь по частям, тип.
struct Leaf {
    at: usize,
    parts: Vec<String>,
    /// Multi-field Elasticsearch (`text.keyword`) читает то же исходное
    /// значение, что родительское поле, если отдельного JSON-пути нет.
    fallback: Option<Vec<String>>,
    ignore_above: Option<usize>,
    constant: Option<serde_json::Value>,
    scaling_factor: Option<f64>,
    kind: Kind,
    /// Номер поля-точки, если это её спутник.
    of_geo: Option<u32>,
    geo_part: Option<GeoPart>,
}

/// Массив объектов: путь до него и поля внутри, с путями от элемента.
struct Group {
    id: u32,
    parts: Vec<String>,
    leaves: Vec<Leaf>,
}

/// Как читать документ по объявленной схеме.
pub struct Plan {
    root: Vec<Leaf>,
    groups: Vec<Group>,
    /// Первые части всех объявленных путей — по ним опознаются чужие ключи.
    tops: Vec<String>,
    /// Сколько всего полей: длина вектора значений у плоского пути.
    nfields: usize,
    /// Сколько групп объявлено, включая корневую.
    ngroups: usize,
    /// Устойчивые места внутренних полей версии документа.
    source_at: Option<usize>,
    projection_at: Option<usize>,
    /// Иерархическое правило `dynamic` проверяет каждый входной документ до
    /// того, как он попадёт в WAL. Хранить его в плане дешевле, чем заново
    /// разбирать mapping на каждой пачке.
    mapping: crate::mapping::Rules,
}

/// Разобранный документ.
#[derive(Debug, Clone)]
pub struct Doc {
    pub id: String,
    pub has_id: bool,
    /// Число элементов по номеру группы. Нулевая всегда единица.
    pub arity: Vec<u32>,
    /// Ячейки: поле, слот, значение.
    pub cells: Vec<(u32, u32, String)>,
}

impl Plan {
    /// Строит план по объявленной схеме.
    pub fn build(
        fields: &[String],
        kinds: &[Kind],
        groups: &[u32],
        group_paths: &[String],
    ) -> Plan {
        Plan::build_with(fields, kinds, groups, group_paths, &crate::mapping::Rules::default())
    }

    pub fn build_with(
        fields: &[String],
        kinds: &[Kind],
        groups: &[u32],
        group_paths: &[String],
        rules: &crate::mapping::Rules,
    ) -> Plan {
        let parts_of = |s: &str| -> Vec<String> { s.split('.').map(str::to_string).collect() };
        let ngroups = group_paths.len().max(1);
        let mut root: Vec<Leaf> = Vec::new();
        let mut source_at = None;
        let mut projection_at = None;
        let mut gs: Vec<Group> = (1..ngroups)
            .map(|g| Group {
                id: g as u32,
                parts: parts_of(group_paths.get(g).map(String::as_str).unwrap_or("")),
                leaves: Vec::new(),
            })
            .collect();
        for (at, path) in fields.iter().enumerate() {
            if path == SOURCE_FIELD {
                source_at = Some(at);
                continue;
            }
            if path == PROJECTION_FIELD {
                projection_at = Some(at);
                continue;
            }
            let kind = kinds.get(at).copied().unwrap_or(Kind::Text);
            let rule = rules.get(path);
            let g = groups.get(at).copied().unwrap_or(0);
            if g == 0 {
                let parts = parts_of(path);
                let fallback = rule
                    .map(|rule| rule.source_path.as_str())
                    .filter(|source| !source.is_empty() && *source != path)
                    .or_else(|| fallback_parts(path, fields))
                    .map(parts_of);
                root.push(Leaf {
                    at,
                    parts,
                    fallback,
                    ignore_above: rule.and_then(|rule| rule.ignore_above),
                    constant: rule.and_then(|rule| rule.constant.clone()),
                    scaling_factor: rule.and_then(|rule| rule.scaling_factor),
                    kind,
                    of_geo: None,
                    geo_part: None,
                });
                continue;
            }
            let Some(slot) = gs.iter_mut().find(|x| x.id == g) else {
                // Группы нет в перечне путей — поле читать неоткуда. Молчать
                // нельзя, но и отказывать на каждом документе незачем: это
                // расхождение схемы, а не данных, и оно уже отвергнуто при
                // объявлении. Здесь поле просто остаётся непрочитанным.
                continue;
            };
            // Путь поля внутри элемента — то, что осталось от пути группы.
            let tail = path
                .strip_prefix(&group_paths[g as usize].clone())
                .map(|r| r.trim_start_matches('.'))
                .unwrap_or(path);
            // Пустой хвост значит, что путь поля совпал с путём массива: так
            // объявляется массив скаляров (`tags` — и группа, и поле).
            let parts = if tail.is_empty() { Vec::new() } else { parts_of(tail) };
            let fallback = rule
                .map(|rule| rule.source_path.as_str())
                .filter(|source| !source.is_empty() && *source != path)
                .or_else(|| fallback_parts(path, fields))
                .map(|parent| {
                    let tail = parent
                        .strip_prefix(&group_paths[g as usize])
                        .map(|rest| rest.trim_start_matches('.'))
                        .unwrap_or(parent);
                    if tail.is_empty() {
                        Vec::new()
                    } else {
                        parts_of(tail)
                    }
                });
            slot.leaves.push(Leaf {
                at,
                parts,
                fallback,
                ignore_above: rule.and_then(|rule| rule.ignore_above),
                constant: rule.and_then(|rule| rule.constant.clone()),
                scaling_factor: rule.and_then(|rule| rule.scaling_factor),
                kind,
                of_geo: None,
                geo_part: None,
            });
        }
        // Спутники точки опознаются по имени: `место.lat` рядом с `место`
        // типа `geo_point`. Опознавать их по типу нельзя — `double` бывает и
        // сам по себе.
        let geo_at: Vec<(String, u32)> = fields
            .iter()
            .enumerate()
            .filter(|(i, _)| kinds.get(*i).copied().unwrap_or(Kind::Text) == Kind::Geo)
            .map(|(i, p)| (p.clone(), i as u32))
            .collect();
        for leaf in root.iter_mut() {
            let path = leaf.parts.join(".");
            let Some((head, tail)) = path.rsplit_once('.') else { continue };
            let part = match tail {
                "lat" => GeoPart::Lat,
                "lon" => GeoPart::Lon,
                _ => continue,
            };
            if let Some((_, src)) = geo_at.iter().find(|(p, _)| p == head) {
                leaf.of_geo = Some(*src);
                leaf.geo_part = Some(part);
            }
        }
        let mut tops: Vec<String> = fields
            .iter()
            .filter(|p| p.as_str() != SOURCE_FIELD && p.as_str() != PROJECTION_FIELD)
            .map(|p| p.split('.').next().unwrap_or(p).to_string())
            .collect();
        tops.sort_unstable();
        tops.dedup();
        Plan {
            root,
            groups: gs,
            tops,
            nfields: fields.len(),
            ngroups,
            source_at,
            projection_at,
            mapping: rules.clone(),
        }
    }

    /// Читает документ. `Err` — документ не принимается вовсе.
    pub fn take(
        &self,
        d: &serde_json::Value,
        at: usize,
        report: &mut Report,
    ) -> Result<Doc, String> {
        let explicit_source = d.get("_source");
        let source = explicit_source.unwrap_or(d);
        if explicit_source.is_some() && !source.is_object() {
            return Err(format!("документ №{at}: _source обязан быть JSON-объектом"));
        }
        let projection = match explicit_source {
            Some(_) => d.get("fields").unwrap_or(source),
            None => d,
        };
        if !projection.is_object() {
            return Err(format!("документ №{at}: fields обязан быть JSON-объектом"));
        }
        let original_projection = projection;
        let normalized = normalize_projection(projection, &self.mapping)
            .map_err(|why| format!("документ №{at}: {why}"))?;
        let projection = normalized.as_ref();
        self.mapping
            .validate_projection(projection)
            .map_err(|why| format!("документ №{at}: {why}"))?;
        let id_value = d.get("id").or_else(|| projection.get("id")).or_else(|| source.get("id"));
        let has_id = id_value.is_some();
        let id = intake::take_cell(id_value, Kind::Keyword).into_text();
        // Отчёт узнаёт, о ком речь, **до** первой потери: иначе он назвал бы
        // предыдущий документ.
        report.at(at, &id, has_id);
        let mut cells: Vec<(u32, u32, String)> = Vec::with_capacity(self.nfields);
        let mut arity = vec![1u32; self.ngroups];

        if let Some(field) = self.source_at {
            let raw = serde_json::to_string(source).map_err(|e| {
                format!("документ {}: _source не сериализуется: {e}", named(&id, has_id, at))
            })?;
            cells.push((field as u32, 0, raw));
        }
        if original_projection != source {
            if let (Some(field), Some(_)) = (self.projection_at, explicit_source) {
                let raw = serde_json::to_string(original_projection).map_err(|e| {
                    format!(
                        "документ {}: индексируемая проекция не сериализуется: {e}",
                        named(&id, has_id, at)
                    )
                })?;
                cells.push((field as u32, 0, raw));
            }
        }

        for leaf in self.root.iter() {
            // Спутники точки заполняет она сама — читать их из документа
            // нечего: в нём их нет.
            if let Some(src) = leaf.of_geo {
                let point = cells
                    .iter()
                    .find(|(f, _, _)| *f == src)
                    .and_then(|(_, _, t)| svindb_core::geo::Point::parse(t).ok());
                let text = match (point, leaf.geo_part) {
                    (Some(p), Some(GeoPart::Lat)) => p.lat.to_string(),
                    (Some(p), Some(GeoPart::Lon)) => p.lon.to_string(),
                    _ => String::new(),
                };
                cells.push((leaf.at as u32, 0, text));
                continue;
            }
            let taken =
                resolve_with_fallback(projection, &leaf.parts, leaf.fallback.as_deref(), leaf.kind)
                    .and_then(|taken| apply_rule(taken, leaf));
            match taken {
                Ok(Taken::Value(text)) => cells.push((leaf.at as u32, 0, text)),
                Ok(Taken::Absent) => {}
                Ok(loss) => report.note(at, &leaf.parts.join("."), &loss),
                Err(why) => {
                    report.note_text(at, &leaf.parts.join("."), &why);
                }
            }
        }

        for g in self.groups.iter() {
            let path = g.parts.join(".");
            let items = match group_items(projection, &g.parts) {
                Ok(items) => items,
                Err(why) => {
                    report.note_text(at, &path, &why);
                    arity[g.id as usize] = 0;
                    continue;
                }
            };
            if items.len() > MAX_SLOTS {
                return Err(format!(
                    "документ {}: в массиве {path:?} {} элементов, а потолок {MAX_SLOTS}",
                    named(&id, has_id, at),
                    items.len()
                ));
            }
            arity[g.id as usize] = items.len() as u32;
            for (slot, item) in items.iter().enumerate() {
                for leaf in g.leaves.iter() {
                    // Пустой путь — массив скаляров: элемент и есть значение.
                    let taken = if leaf.parts.is_empty() {
                        Ok(take_value(Some(item), leaf.kind))
                    } else if item.is_object() {
                        resolve_with_fallback(
                            item,
                            &leaf.parts,
                            leaf.fallback.as_deref(),
                            leaf.kind,
                        )
                    } else {
                        Err(format!(
                            "элемент {slot} — не объект, а поля внутри массива объявлены путями"
                        ))
                    }
                    .and_then(|taken| apply_rule(taken, leaf));
                    match taken {
                        Ok(Taken::Value(text)) => {
                            cells.push((leaf.at as u32, slot as u32, text));
                        }
                        Ok(Taken::Absent) => {}
                        Ok(loss) => report.note(at, &format!("{path}[{slot}]"), &loss),
                        Err(why) => {
                            report.note_text(at, &format!("{path}[{slot}]"), &why);
                        }
                    }
                }
            }
        }

        // Чужие ключи — только на верхнем уровне. Внутрь незаявленного
        // поддерева обход не идёт: назвать верхний ключ достаточно, а обходить
        // чужое дерево целиком значило бы платить за него на горячем пути.
        // В явной форме `{fields,_source}` неизвестное source-поддерево не
        // потеряно: оно лежит lossless и намеренно может быть source-only.
        // Поля, которых нет в compiled projection, не индексируются, но и не
        // исчезают. У обычной формы документ и проекция совпадают, поэтому
        // прежняя строгая проверка сохраняется.
        if explicit_source.is_none() {
            let Some(obj) = projection.as_object() else {
                return Ok(Doc { id, has_id, arity, cells });
            };
            for key in obj.keys() {
                if key != "id"
                    && key != "_source"
                    && key != "fields"
                    && !self.tops.iter().any(|t| t == key)
                {
                    report.unknown_key(key);
                }
            }
        }
        Ok(Doc { id, has_id, arity, cells })
    }
}

/// Родительский путь multi-field, если он сам объявлен полем.
fn fallback_parts<'a>(path: &'a str, fields: &[String]) -> Option<&'a str> {
    let (parent, _) = path.rsplit_once('.')?;
    fields.iter().any(|field| field == parent).then_some(parent)
}

/// Как назвать документ в сообщении: по `id`, если он есть.
fn named(id: &str, has_id: bool, at: usize) -> String {
    if has_id && !id.is_empty() {
        format!("{id:?}")
    } else {
        format!("№{at} (без id)")
    }
}

enum Dig<'a> {
    Found(&'a serde_json::Value),
    /// Пути нет вовсе или он `null` — законно и молча.
    Absent,
    /// Спуск упёрся во что-то, чем путь пройти нельзя.
    Blocked(String),
}

/// Плоские ключи `a.b` и объекты `a: {b: ...}` обозначают один путь.
/// Без точек документ остаётся заимствованным; исходный JSON не изменяется.
pub(crate) fn normalize_projection<'a>(
    value: &'a serde_json::Value,
    rules: &crate::mapping::Rules,
) -> Result<std::borrow::Cow<'a, serde_json::Value>, String> {
    expand_projection(value, rules, "")
}

#[cfg(test)]
fn expand_dotted_keys(
    value: &serde_json::Value,
) -> Result<std::borrow::Cow<'_, serde_json::Value>, String> {
    normalize_projection(value, &crate::mapping::Rules::default())
}

fn expand_projection<'a>(
    value: &'a serde_json::Value,
    rules: &crate::mapping::Rules,
    prefix: &str,
) -> Result<std::borrow::Cow<'a, serde_json::Value>, String> {
    use serde_json::Value;
    use std::borrow::Cow;
    match value {
        Value::Object(object) => {
            let mut result: Option<serde_json::Map<String, Value>> = None;
            for (key, child) in object {
                let path = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
                let expanded = match rules.projection_path(&path) {
                    crate::mapping::ProjectionPath::Ignored
                    | crate::mapping::ProjectionPath::SourceOnly => Cow::Borrowed(child),
                    crate::mapping::ProjectionPath::Indexed => {
                        expand_projection(child, rules, &path)?
                    }
                };
                if key.contains('.') {
                    let parts = projection_parts(key, prefix, rules)?;
                    if parts.len() == 1 {
                        continue;
                    }
                    let out = result.get_or_insert_with(|| object.clone());
                    out.remove(key);
                    insert_dotted(
                        out,
                        &parts.iter().map(String::as_str).collect::<Vec<_>>(),
                        expanded.into_owned(),
                        key,
                    )?;
                } else if let Cow::Owned(expanded) = expanded {
                    result.get_or_insert_with(|| object.clone()).insert(key.clone(), expanded);
                }
            }
            Ok(match result {
                Some(result) => Cow::Owned(Value::Object(result)),
                None => Cow::Borrowed(value),
            })
        }
        Value::Array(items) => {
            let mut result: Option<Vec<Value>> = None;
            for (at, child) in items.iter().enumerate() {
                if let Cow::Owned(expanded) = expand_projection(child, rules, prefix)? {
                    result.get_or_insert_with(|| items.clone())[at] = expanded;
                }
            }
            Ok(match result {
                Some(result) => Cow::Owned(Value::Array(result)),
                None => Cow::Borrowed(value),
            })
        }
        _ => Ok(Cow::Borrowed(value)),
    }
}

/// Раскрывает лишь объявленный префикс, оставляя непрозрачный хвост буквально.
fn projection_parts(
    key: &str,
    prefix: &str,
    rules: &crate::mapping::Rules,
) -> Result<Vec<String>, String> {
    let raw: Vec<_> = key.split('.').collect();
    for end in 1..=raw.len() {
        let local = raw[..end].join(".");
        let path = if prefix.is_empty() { local.clone() } else { format!("{prefix}.{local}") };
        let opaque_from = match rules.projection_path(&path) {
            crate::mapping::ProjectionPath::Indexed => continue,
            crate::mapping::ProjectionPath::Ignored => end - 1,
            crate::mapping::ProjectionPath::SourceOnly => end,
        };
        if opaque_from > 0 {
            svindb_core::schema::valid_field_path(&raw[..opaque_from].join("."))?;
        }
        let mut parts: Vec<String> = raw[..opaque_from].iter().map(|part| (*part).into()).collect();
        if opaque_from < raw.len() {
            parts.push(raw[opaque_from..].join("."));
        }
        return Ok(parts);
    }
    svindb_core::schema::valid_field_path(key)?;
    Ok(raw.into_iter().map(str::to_string).collect())
}

fn insert_dotted(
    object: &mut serde_json::Map<String, serde_json::Value>,
    parts: &[&str],
    value: serde_json::Value,
    path: &str,
) -> Result<(), String> {
    let conflict = || format!("конфликт плоского и вложенного значения пути {path:?}");
    if parts.len() == 1 {
        if let Some(existing) = object.get_mut(parts[0]) {
            merge_dotted(existing, value, path)
        } else {
            object.insert(parts[0].to_string(), value);
            Ok(())
        }
    } else {
        let child = object.entry(parts[0]).or_insert_with(|| serde_json::json!({}));
        let child = child.as_object_mut().ok_or_else(conflict)?;
        insert_dotted(child, &parts[1..], value, path)
    }
}

fn merge_dotted(
    existing: &mut serde_json::Value,
    value: serde_json::Value,
    path: &str,
) -> Result<(), String> {
    if *existing == value {
        return Ok(());
    }
    if let (Some(left), Some(right)) = (existing.as_object_mut(), value.as_object()) {
        for (key, value) in right {
            if let Some(existing) = left.get_mut(key) {
                merge_dotted(existing, value.clone(), path)?;
            } else {
                left.insert(key.clone(), value.clone());
            }
        }
        return Ok(());
    }
    Err(format!("конфликт плоского и вложенного значения пути {path:?}"))
}

/// Спускается по частям пути.
fn dig<'a>(mut v: &'a serde_json::Value, parts: &[String]) -> Dig<'a> {
    for (i, p) in parts.iter().enumerate() {
        match v {
            serde_json::Value::Object(m) => match m.get(p) {
                None | Some(serde_json::Value::Null) => return Dig::Absent,
                Some(next) => v = next,
            },
            // Массив на промежуточной части — это массив объектов, который не
            // объявлен группой. Взять из него одно значение нельзя: элементы
            // неразличимы, и любой выбор был бы правдоподобным и неверным.
            serde_json::Value::Array(_) => {
                return Dig::Blocked(format!(
                    "на части {:?} массив, а он не объявлен: добавьте метку slots",
                    parts[..i].join(".")
                ))
            }
            _ => {
                return Dig::Blocked(format!(
                    "на части {:?} значение, а путь ведёт дальше",
                    parts[..i].join(".")
                ))
            }
        }
    }
    Dig::Found(v)
}

/// Спускается и читает лист.
fn resolve(d: &serde_json::Value, parts: &[String], kind: Kind) -> Result<Taken, String> {
    match dig(d, parts) {
        Dig::Found(v) => Ok(take_value(Some(v), kind)),
        Dig::Absent => Ok(take_value(None, kind)),
        Dig::Blocked(why) => Err(why),
    }
}

fn resolve_with_fallback(
    d: &serde_json::Value,
    parts: &[String],
    fallback: Option<&[String]>,
    kind: Kind,
) -> Result<Taken, String> {
    match dig(d, parts) {
        Dig::Found(value) => Ok(take_value(Some(value), kind)),
        Dig::Absent => match fallback {
            Some(fallback) => resolve(d, fallback, kind),
            None => Ok(take_value(None, kind)),
        },
        Dig::Blocked(why) => match fallback {
            Some(fallback) => resolve(d, fallback, kind),
            None => Err(why),
        },
    }
}

/// Скаляр либо массив скаляров одного Elasticsearch-поля.
///
/// Вектор и geo уже используют JSON-массив как собственное значение и потому
/// проходят старым разбором. Для остальных массив кодируется длинами только до
/// индексатора; наружу он никогда не возвращается — GET/search берут точный
/// JSON из source/projection.
fn take_value(value: Option<&serde_json::Value>, kind: Kind) -> Taken {
    let Some(value) = value else { return Taken::Absent };
    if value.is_array() && kind.dims().is_none() && !kind.dot() && kind != Kind::Geo {
        let mut values = Vec::new();
        for item in value.as_array().expect("проверено") {
            let taken = intake::take_cell(Some(item), kind);
            match taken {
                Taken::Value(value) => values.push(value),
                Taken::Absent => {}
                loss => return loss,
            }
        }
        return if values.is_empty() {
            Taken::Absent
        } else {
            Taken::Value(svindb_core::indexer::encode_multi(&values))
        };
    }
    match intake::take_cell(Some(value), kind) {
        Taken::Value(text) if text.starts_with(svindb_core::indexer::MULTI_PREFIX) => {
            Taken::Value(svindb_core::indexer::encode_multi(&[text]))
        }
        other => other,
    }
}

fn apply_rule(taken: Taken, leaf: &Leaf) -> Result<Taken, String> {
    let expected = leaf.constant.as_ref().map(|value| take_value(Some(value), leaf.kind));
    let mut taken = match (taken, expected) {
        (Taken::Absent, Some(Taken::Value(value))) => Taken::Value(value),
        (Taken::Value(actual), Some(Taken::Value(expected))) if actual != expected => {
            return Err(format!(
                "constant_keyword получил {actual:?}, а mapping требует {expected:?}"
            ));
        }
        (Taken::Value(actual), Some(Taken::Value(_))) => Taken::Value(actual),
        (_, Some(loss)) if loss.is_loss() => {
            return Err("constant_keyword.value не читается объявленным типом".into())
        }
        (value, _) => value,
    };

    if let (Taken::Value(text), Some(factor)) = (&taken, leaf.scaling_factor) {
        if !factor.is_finite() || factor <= 0.0 {
            return Err("scaled_float.scaling_factor должен быть положительным конечным".into());
        }
        let multiple = svindb_core::indexer::multi_parts(text).is_some();
        let values = svindb_core::indexer::multi_parts(text).unwrap_or_else(|| vec![text.as_str()]);
        let mut scaled = Vec::with_capacity(values.len());
        for value in values {
            let number = value
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite())
                .ok_or("scaled_float получил не число")?;
            scaled.push(((number * factor).round() / factor).to_string());
        }
        taken = Taken::Value(if multiple {
            svindb_core::indexer::encode_multi(&scaled)
        } else {
            scaled.pop().unwrap_or_default()
        });
    }

    if let (Taken::Value(text), Some(limit)) = (&taken, leaf.ignore_above) {
        let multiple = svindb_core::indexer::multi_parts(text).is_some();
        let values = svindb_core::indexer::multi_parts(text).unwrap_or_else(|| vec![text.as_str()]);
        let kept: Vec<String> = values
            .into_iter()
            .filter(|value| value.chars().count() <= limit)
            .map(str::to_string)
            .collect();
        taken = if kept.is_empty() {
            Taken::Absent
        } else if multiple {
            Taken::Value(svindb_core::indexer::encode_multi(&kept))
        } else {
            Taken::Value(kept.into_iter().next().unwrap_or_default())
        };
    }
    Ok(taken)
}

/// Все элементы группы, включая путь через массивы родительских групп.
///
/// Например, путь `documents.attributes` сначала разворачивает каждый
/// `documents[]`, затем каждый его `attributes[]`. Поля внутренней группы
/// получают общую последовательность её элементов, а lossless source рядом
/// сохраняет границы родителей.
fn group_items<'a>(
    root: &'a serde_json::Value,
    parts: &[String],
) -> Result<Vec<&'a serde_json::Value>, String> {
    let mut current = vec![root];
    for (at, part) in parts.iter().enumerate() {
        let mut next = Vec::new();
        for value in current {
            let child = match value {
                serde_json::Value::Object(object) => object.get(part),
                _ => {
                    return Err(format!(
                        "на части {:?} значение, а путь группы ведёт дальше",
                        parts[..at].join(".")
                    ))
                }
            };
            let Some(child) = child.filter(|value| !value.is_null()) else { continue };
            match child {
                serde_json::Value::Array(items) => next.extend(items),
                other => next.push(other),
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }
    Ok(current)
}

impl Doc {
    /// Значения по полям, если документ плоский: у всех групп по одному слоту.
    ///
    /// Плоскому индексу этим и хватает, и запись журнала у него остаётся
    /// прежней — то есть журналы существующих индексов читаются как читались.
    pub fn flat(&self, nfields: usize) -> Option<Vec<String>> {
        if self.arity.iter().any(|a| *a != 1) {
            return None;
        }
        let mut out = vec![String::new(); nfields];
        for (f, slot, text) in self.cells.iter() {
            if *slot != 0 {
                return None;
            }
            *out.get_mut(*f as usize)? = text.clone();
        }
        Some(out)
    }

    /// Дописывает прежнее строковое тело WAL прямо в буфер пачки.
    /// `flat` задаёт плоский legacy-вид; без него сохраняются все ячейки.
    pub fn encode_wal_into(&self, out: &mut Vec<u8>, flat: Option<&[String]>) {
        fn text(out: &mut Vec<u8>, value: &str) {
            out.extend((value.len() as u32).to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        fn decimal(out: &mut Vec<u8>, mut value: u32) {
            let mut digits = [0u8; 10];
            let mut start = digits.len();
            loop {
                start -= 1;
                digits[start] = b'0' + (value % 10) as u8;
                value /= 10;
                if value == 0 {
                    break;
                }
            }
            out.extend(((digits.len() - start) as u32).to_le_bytes());
            out.extend_from_slice(&digits[start..]);
        }
        let count = match flat {
            Some(values) => 1 + values.len(),
            None => 1 + self.arity.len() + self.cells.len() * 3,
        };
        out.extend((count as u32).to_le_bytes());
        text(out, &self.id);
        if let Some(values) = flat {
            for value in values {
                text(out, value);
            }
        } else {
            for arity in &self.arity {
                decimal(out, *arity);
            }
            for (field, slot, value) in &self.cells {
                decimal(out, *field);
                decimal(out, *slot);
                text(out, value);
            }
        }
    }

    /// Тело записи журнала: `[id, арность…, «поле», «слот», значение, …]`.
    #[cfg(test)]
    pub fn wal_parts(&self) -> Vec<String> {
        let mut parts = Vec::with_capacity(1 + self.arity.len() + self.cells.len() * 3);
        parts.push(self.id.clone());
        parts.extend(self.arity.iter().map(|a| a.to_string()));
        for (f, slot, text) in self.cells.iter() {
            parts.push(f.to_string());
            parts.push(slot.to_string());
            parts.push(text.clone());
        }
        parts
    }

    /// Обратно из тела записи журнала.
    pub fn from_wal_parts(parts: &[String], ngroups: usize) -> Option<Doc> {
        let id = parts.first()?.clone();
        let arity: Vec<u32> =
            parts.get(1..1 + ngroups)?.iter().map(|s| s.parse().ok()).collect::<Option<_>>()?;
        let rest = parts.get(1 + ngroups..)?;
        if rest.len() % 3 != 0 {
            return None;
        }
        let mut cells = Vec::with_capacity(rest.len() / 3);
        for c in rest.chunks_exact(3) {
            cells.push((c[0].parse().ok()?, c[1].parse().ok()?, c[2].clone()));
        }
        Some(Doc { id, has_id: true, arity, cells })
    }
}

/// Кладёт разобранный документ в приёмник — одним способом для HTTP и CLI.
///
/// Плоский документ идёт прежней дорогой (`add`/`replace` по значениям полей),
/// документ с массивом — ячейками. Держать эту развилку в обоих вызывающих
/// значило бы снова завести два способа понять одну схему: именно так офлайн-
/// индексатор однажды продолжил искать буквальный ключ `author.name`, когда
/// HTTP-приём уже спускался во вложенный объект.
pub fn put_doc(ix: &mut svindb_core::indexer::Indexer, doc: &Doc, replacing: bool) -> usize {
    put_doc_pre(ix, doc, replacing, None)
}

/// То же, но со словами, разобранными заранее и вне замка буфера.
///
/// `pre` действует только на плоском пути: у документа с массивами укладка
/// ячейковая, и подставлять туда плоский разбор нельзя. Замена молча брала бы
/// слова не того поля.
/// Тот же приём, но по **уже развёрнутому** документу.
///
/// ## Зачем отдельная дверь
///
/// Развёртка плоского вида — это `Vec<String>` на документ: выделение в куче и
/// копия каждого значения. Делалась она дважды: один раз снаружи, чтобы
/// разобрать слова вне замка, второй раз здесь, под замком, — и первый
/// результат выбрасывался. То есть самая дорогая часть подготовки честно
/// уезжала из-под замка и тут же возвращалась туда обратно.
///
/// Значения **переставляются**, а не копируются: вызывающему они больше не
/// нужны, а копия вернула бы под замок ровно те выделения, ради выноса которых
/// всё и делалось.
pub fn put_doc_flat(
    ix: &mut svindb_core::indexer::Indexer,
    id: &str,
    values: &[String],
    pre: Option<&mut [svindb_core::analyze::Tokens]>,
) {
    let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    ix.add_pre(id, &refs, pre);
}

pub fn put_doc_pre(
    ix: &mut svindb_core::indexer::Indexer,
    doc: &Doc,
    replacing: bool,
    pre: Option<&mut [svindb_core::analyze::Tokens]>,
) -> usize {
    let nfields = ix.field_names().len();
    // У схемы с массивами короткий путь запрещён, даже когда документ выглядит
    // плоским. Он не записывает арность, и смешанный сегмент потом не открыть.
    let grouped = ix.group_paths().len() > 1;
    let lossless = ix.field_names().iter().any(|field| field == SOURCE_FIELD);
    if let Some(values) = doc.flat(nfields).filter(|_| !grouped && !lossless) {
        let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        return if replacing {
            ix.replace_pre(&doc.id, &refs, pre)
        } else {
            ix.add_pre(&doc.id, &refs, pre);
            0
        };
    }
    let cells: Vec<svindb_core::indexer::Cell> = doc
        .cells
        .iter()
        .map(|(field, slot, text)| svindb_core::indexer::Cell {
            field: *field as usize,
            slot: *slot,
            text: text.as_str(),
        })
        .collect();
    if replacing {
        ix.replace_cells_pre(&doc.id, &doc.arity, &cells, pre)
    } else {
        ix.add_cells_pre(&doc.id, &doc.arity, &cells, pre);
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> Plan {
        Plan::build(
            &[
                "text".into(),
                "author.name".into(),
                "reactions.emoji".into(),
                "reactions.count".into(),
                "tags".into(),
            ],
            &[Kind::Text, Kind::Keyword, Kind::Keyword, Kind::Int32, Kind::Keyword],
            &[0, 0, 1, 1, 2],
            &[String::new(), "reactions".into(), "tags".into()],
        )
    }

    fn take(json: &str) -> (Doc, Report) {
        let v: serde_json::Value = serde_json::from_str(json).expect("тело");
        let mut r = Report::default();
        let d = plan().take(&v, 0, &mut r).expect("документ отвергнут");
        (d, r)
    }

    /// Вложенный объект читается спуском, а не буквальным ключом с точкой.
    #[test]
    fn a_nested_object_is_read_by_descending_not_by_a_literal_dotted_key() {
        let (d, r) = take(r#"{"id":"a","text":"т","author":{"name":"Аня"}}"#);
        let got: Vec<&str> = d.cells.iter().map(|(_, _, t)| t.as_str()).collect();
        assert!(got.contains(&"Аня"), "вложенное значение не прочлось: {got:?}");
        assert!(r.clean(), "чистый документ дал потери: {:?}", r.suffix(|s| format!("{s:?}")));

        // Та же проекция с плоским ключом принимается без потери значения.
        let (d, report) = take(r#"{"id":"a","text":"т","author.name":"Аня"}"#);
        let got: Vec<&str> = d.cells.iter().map(|(_, _, t)| t.as_str()).collect();
        assert!(got.contains(&"Аня"), "ключ с точкой потерялся: {got:?}");
        assert!(report.clean());
    }

    #[test]
    fn dotted_and_nested_keys_accept_equal_values_and_reject_conflicts() {
        let same = serde_json::json!({"author.name": "Аня", "author": {"name": "Аня"}});
        assert!(plan().take(&same, 0, &mut Report::default()).is_ok());
        for conflicting in [
            serde_json::json!({"author.name": "Аня", "author": {"name": "Боря"}}),
            serde_json::json!({"author.name": "Аня", "author": "Боря"}),
            serde_json::json!({"author.name": null, "author": {"name": "Аня"}}),
        ] {
            let error = plan().take(&conflicting, 0, &mut Report::default()).unwrap_err();
            assert!(error.contains("конфликт"), "{error}");
        }
        let nested = serde_json::json!({"items": [{"user.name": "Аня"}]});
        assert_eq!(
            expand_dotted_keys(&nested).unwrap().as_ref(),
            &serde_json::json!({"items": [{"user": {"name": "Аня"}}]})
        );
        let plain = serde_json::json!({"author": {"name": "Аня"}});
        assert!(matches!(expand_dotted_keys(&plain).unwrap(), std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn mapping_guided_normalization_keeps_opaque_payloads_literal() {
        let mapping = serde_json::json!({"mappings":{"dynamic":"strict","properties":{
            "indexed.name":{"type":"keyword"},
            "opaque":{"type":"object","enabled":false},
            "loose":{"type":"object","dynamic":false,"properties":{"known":{"type":"keyword"}}}
        }}});
        let rules =
            crate::mapping::Rules::from_bytes(&serde_json::to_vec(&mapping).unwrap()).unwrap();
        let opaque = serde_json::json!({"a..b":1,"a":2,"x.y":3});
        let source = serde_json::json!({"indexed.name":"alpha","opaque":opaque,"loose":{"known":"ok","unknown":opaque,"bad..key":true}});
        let normalized = normalize_projection(&source, &rules).unwrap();
        assert_eq!(normalized["indexed"]["name"], "alpha");
        assert_eq!(normalized["opaque"], opaque);
        assert_eq!(normalized["loose"]["unknown"], opaque);
        assert_eq!(normalized["loose"]["bad..key"], true);
        rules.validate_projection(&normalized).unwrap();
        let plan = Plan::build_with(
            &["indexed.name".into(), "loose.known".into(), SOURCE_FIELD.into()],
            &[Kind::Keyword, Kind::Keyword, Kind::Keyword],
            &[0, 0, 0],
            &[String::new()],
            &rules,
        );
        let doc = plan.take(&source, 0, &mut Report::default()).unwrap();
        let stored = &doc.cells.iter().find(|(field, _, _)| *field == 2).unwrap().2;
        assert_eq!(serde_json::from_str::<serde_json::Value>(stored).unwrap(), source);
        assert!(normalize_projection(&serde_json::json!({"indexed..name":"bad"}), &rules).is_err());
        let dotted_opaque = serde_json::json!({"opaque.a..b":1,"loose.unknown.a..b":2});
        let normalized = normalize_projection(&dotted_opaque, &rules).unwrap();
        assert_eq!(normalized["opaque"]["a..b"], 1);
        assert_eq!(normalized["loose"]["unknown.a..b"], 2);
        rules.validate_projection(&normalized).unwrap();
    }

    #[test]
    fn dotted_extraction_preserves_the_original_source_and_projection() {
        let plan = Plan::build(
            &["author.name".into(), SOURCE_FIELD.into(), PROJECTION_FIELD.into()],
            &[Kind::Keyword, Kind::Keyword, Kind::Keyword],
            &[0, 0, 0],
            &[String::new()],
        );
        let input = serde_json::json!({
            "fields": {"author.name": "Аня"},
            "_source": {"raw": {"kept.as.is": true}}
        });
        let doc = plan.take(&input, 0, &mut Report::default()).unwrap();
        assert_eq!(doc.cells[0].0, 1);
        let at = |field| doc.cells.iter().find(|(f, _, _)| *f == field).unwrap().2.as_str();
        assert_eq!(at(0), "Аня");
        assert_eq!(serde_json::from_str::<serde_json::Value>(at(1)).unwrap(), input["_source"]);
        assert_eq!(serde_json::from_str::<serde_json::Value>(at(2)).unwrap(), input["fields"]);
    }

    /// Массив объектов раскладывается по слотам, и слот у полей общий.
    #[test]
    fn an_array_of_objects_lands_in_slots_that_line_up() {
        let (d, _) =
            take(r#"{"id":"a","reactions":[{"emoji":"up","count":3},{"emoji":"fire","count":1}]}"#);
        assert_eq!(d.arity[1], 2, "арность массива не та");
        // Поле 2 — emoji, поле 3 — count. У слота 0 обязаны сойтись «up» и «3».
        let at = |f: u32, slot: u32| {
            d.cells
                .iter()
                .find(|(x, s, _)| *x == f && *s == slot)
                .map(|(_, _, t)| t.as_str())
                .unwrap_or("<нет>")
        };
        assert_eq!((at(2, 0), at(3, 0)), ("up", "3"), "нулевой элемент разъехался");
        assert_eq!((at(2, 1), at(3, 1)), ("fire", "1"), "первый элемент разъехался");
    }

    /// Массив скаляров — это группа, у которой путь поля равен пути массива.
    #[test]
    fn an_array_of_scalars_gets_a_slot_per_value() {
        let (d, _) = take(r#"{"id":"a","tags":["один","два","три"]}"#);
        assert_eq!(d.arity[2], 3, "у массива скаляров не та арность");
        let tags: Vec<&str> =
            d.cells.iter().filter(|(f, _, _)| *f == 4).map(|(_, _, t)| t.as_str()).collect();
        assert_eq!(tags, vec!["один", "два", "три"], "значения слиплись или потерялись");
    }

    /// Отсутствующий массив — это ноль элементов, и это не потеря.
    #[test]
    fn a_missing_array_is_zero_elements_and_not_a_loss() {
        let (d, r) = take(r#"{"id":"a","text":"т"}"#);
        assert_eq!(d.arity[1], 0);
        assert_eq!(d.arity[2], 0);
        assert!(r.clean(), "отсутствие массива сочтено потерей");
    }

    /// Массив на пути, который массивом не объявлен, — названная потеря.
    ///
    /// Взять из него одно значение нельзя: элементы неразличимы, и любой выбор
    /// был бы правдоподобным и неверным.
    #[test]
    fn an_unannounced_array_on_the_way_is_named_not_guessed() {
        let (_, r) = take(r#"{"id":"a","author":[{"name":"Аня"},{"name":"Боря"}]}"#);
        assert!(!r.clean(), "массив на пути прошёл молча");
        let s = r.suffix(|x| format!("{x:?}"));
        assert!(s.contains("slots"), "отказ не подсказал, что делать: {s}");
    }

    /// Слишком длинный массив отвергается на приёме, а не валит сегмент.
    #[test]
    fn an_array_over_the_ceiling_is_refused_by_name() {
        let items: Vec<String> = (0..MAX_SLOTS + 1).map(|i| format!(r#""т{i}""#)).collect();
        let body = format!(r#"{{"id":"длинный","tags":[{}]}}"#, items.join(","));
        let v: serde_json::Value = serde_json::from_str(&body).expect("тело");
        let mut r = Report::default();
        let e = plan().take(&v, 0, &mut r).expect_err("массив выше потолка принят");
        assert!(e.contains("длинный"), "отказ не назвал документ: {e}");
        assert!(e.contains(&MAX_SLOTS.to_string()), "отказ не назвал потолок: {e}");
    }

    /// Запись журнала переживает круг «туда — обратно».
    #[test]
    fn a_document_survives_the_journal_round_trip() {
        let (d, _) = take(
            r#"{"id":"a","text":"т","reactions":[{"emoji":"up","count":3}],"tags":["x","y"]}"#,
        );
        let parts = d.wal_parts();
        let back = Doc::from_wal_parts(&parts, d.arity.len()).expect("запись не разобралась");
        assert_eq!(back.id, d.id);
        assert_eq!(back.arity, d.arity, "арность не пережила круг");
        assert_eq!(back.cells, d.cells, "ячейки не пережили круг");
    }

    #[test]
    fn streaming_wal_cells_match_owned_strings_at_decimal_boundaries() {
        let numbers = [0, 1, 9, 10, 99, 100, 999_999_999, 1_000_000_000, u32::MAX];
        let doc = Doc {
            id: "журнал🦔\0".into(),
            has_id: true,
            arity: numbers.to_vec(),
            cells: numbers.iter().map(|&n| (n, n, format!("поле {n}"))).collect(),
        };
        let parts = doc.wal_parts();
        let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
        let expected = svindb_core::wal::encode_strings(&refs);
        let mut actual = vec![17, 23];
        doc.encode_wal_into(&mut actual, None);
        assert_eq!(&actual[..2], &[17, 23]);
        assert_eq!(&actual[2..], expected);
    }

    /// Чужой ключ верхнего уровня назван, а вложенное в объявленный путь — нет.
    #[test]
    fn an_alien_top_level_key_is_named_but_a_declared_subtree_is_not() {
        let (_, r) = take(r#"{"id":"a","опечатка":1,"author":{"name":"Аня","возраст":30}}"#);
        assert_eq!(r.unknown, vec!["опечатка".to_string()], "не тот перечень чужих ключей");
    }
}
