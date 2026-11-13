//! Офлайновая логическая пересборка одного сегмента.
//!
//! Модуль намеренно не подключается к серверному жизненному циклу: вызывающий
//! сам выбирает момент, когда старый сегмент не меняется, и публикует новый
//! состав. Это оставляет старый каталог только для чтения.

use std::fs;
use std::io;
use std::path::Path;

use serde_json::Value;
use svindb_core::index::{self, Kind};
use svindb_core::indexer::{Cell, Indexer};

use crate::{intake::Report, mapping, nested};

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn stored_walk(
    index: &index::Index,
    field: Option<usize>,
) -> Option<svindb_core::store::StoreWalk<'_>> {
    index.fields.get(field?)?.store.as_ref().map(|store| store.walk())
}

fn next_raw(
    walk: &mut Option<svindb_core::store::StoreWalk<'_>>,
    doc: u32,
) -> io::Result<Option<Vec<u8>>> {
    let Some(walk) = walk else { return Ok(None) };
    let (found, bytes) =
        walk.next().ok_or_else(|| invalid("stored поток оборвался до последнего документа"))?;
    if found != doc {
        return Err(invalid("stored поток нарушил порядок физических ID"));
    }
    Ok(Some(bytes.to_vec()))
}

fn finish_raw(walk: &mut Option<svindb_core::store::StoreWalk<'_>>) -> io::Result<()> {
    if let Some(walk) = walk {
        if walk.next().is_some() || walk.broken() {
            return Err(invalid("stored поток содержит лишние данные либо повреждён"));
        }
    }
    Ok(())
}

/// Пересобирает сегмент логически, сохраняя порядок физических документов.
///
/// Важная граница: источник и проекция читаются только из внутренних raw-полей
/// сегмента. Восстановление source из индексированных полей запрещено, поскольку
/// это необратимо теряет неизвестные поля, порядок/форму JSON и `null`.
pub fn rebuild(
    input_index_root: &Path,
    source_segment: &Path,
    output_fresh_root: &Path,
    per_segment: u32,
) -> io::Result<Value> {
    if per_segment == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "per_segment обязан быть больше нуля",
        ));
    }
    if output_fresh_root.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("каталог вывода уже существует: {}", output_fresh_root.display()),
        ));
    }
    let registry = svindb_core::schema::Registry::load(&input_index_root.to_string_lossy())?
        .ok_or_else(|| invalid("schema.json отсутствует"))?;
    if registry.fields().iter().any(|f| f.dead.is_some()) {
        return Err(invalid(
            "схема содержит похоронные поля; миграция обязана сначала явно разрешить их",
        ));
    }
    let rules = mapping::Rules::load(&input_index_root.to_string_lossy()).map_err(invalid)?;
    for entry in fs::read_dir(source_segment)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(invalid(
                "вложенные каталоги, ссылки и специальные файлы сегмента не поддерживаются",
            ));
        }
    }
    let report = svindb_core::check::check(
        &source_segment.to_string_lossy(),
        svindb_core::check::Depth::Full,
    );
    if !report.ok() || report.findings.iter().any(|finding| finding.check == "лишний файл")
    {
        return Err(invalid("исходный сегмент повреждён либо содержит неизвестные файлы"));
    }
    let source = index::load_read_only(&source_segment.to_string_lossy())?;
    if source.docs > u32::MAX as usize {
        return Err(invalid("число документов не представимо локальным u32 ID"));
    }
    let source_at = source.field_of(nested::SOURCE_FIELD).ok_or_else(|| {
        invalid("сегмент не содержит __svindb_source: восстановление из полей запрещено")
    })?;
    let projection_at = source.field_of(nested::PROJECTION_FIELD);

    let fields: Vec<String> = registry.fields().iter().map(|f| f.path.clone()).collect();
    let kinds: Vec<Kind> = registry.fields().iter().map(|f| f.kind).collect();
    let groups: Vec<u32> = registry.fields().iter().map(|f| f.group).collect();
    let group_paths: Vec<String> = registry.groups().iter().map(|g| g.path.clone()).collect();
    let boosts: Vec<f64> = registry.fields().iter().map(|f| f.boost).collect();
    let stored: Vec<bool> = registry.fields().iter().map(|f| f.store).collect();
    let positioned: Vec<bool> =
        registry.fields().iter().map(|f| f.positions && f.kind == Kind::Text).collect();
    let searched: Vec<bool> = registry.fields().iter().map(|f| f.index).collect();
    let fids: Vec<u32> = registry.fields().iter().map(|f| f.id).collect();
    let source_fid_at = registry
        .fields()
        .iter()
        .position(|f| f.path == nested::SOURCE_FIELD)
        .ok_or_else(|| invalid("schema.json не содержит __svindb_source"))?;
    let projection_fid_at =
        registry.fields().iter().position(|f| f.path == nested::PROJECTION_FIELD);
    let plan = nested::Plan::build_with(&fields, &kinds, &groups, &group_paths, &rules);

    fs::create_dir_all(output_fresh_root)?;
    let mut ix = Indexer::typed_positioned(
        &output_fresh_root.to_string_lossy(),
        fields.clone(),
        boosts,
        kinds.clone(),
        stored,
        positioned,
        per_segment,
    );
    ix.set_fids(fids);
    ix.set_groups(groups, group_paths);
    ix.set_searched(searched);
    ix.set_flush_bytes(64 * 1024 * 1024);
    ix.seq = source.seq;

    let mut report = Report::default();
    let mut dead = Vec::with_capacity(source.docs);
    // Каждый сжатый блок распаковывается один раз, а не заново для каждого
    // документа через point-fetch. Память ограничена текущими блоками.
    let mut sources = stored_walk(&source, Some(source_at));
    let mut projections = stored_walk(&source, projection_at);
    for doc_no in 0..source.docs as u32 {
        let raw_source = next_raw(&mut sources, doc_no)?.ok_or_else(|| {
            invalid(format!("документ {doc_no}: отсутствует raw __svindb_source"))
        })?;
        let source_value: Value = serde_json::from_slice(&raw_source)
            .map_err(|e| invalid(format!("документ {doc_no}: битый __svindb_source: {e}")))?;
        if !source_value.is_object() {
            return Err(invalid(format!(
                "документ {doc_no}: __svindb_source обязан быть объектом"
            )));
        }
        let raw_projection_bytes = match projection_at {
            Some(_) => next_raw(&mut projections, doc_no)?
                .filter(|bytes| !bytes.is_empty())
                .map(|bytes| {
                    serde_json::from_slice::<Value>(&bytes).map(|_| bytes).map_err(|e| {
                        invalid(format!("документ {doc_no}: битый __svindb_projection: {e}"))
                    })
                })
                .transpose()?,
            None => None,
        };
        let raw_projection = raw_projection_bytes
            .as_ref()
            .map(|bytes| serde_json::from_slice::<Value>(bytes))
            .transpose()
            .map_err(|e| invalid(format!("документ {doc_no}: битый projection: {e}")))?;
        if let Some(p) = &raw_projection {
            if !p.is_object() {
                return Err(invalid(format!(
                    "документ {doc_no}: __svindb_projection обязан быть объектом"
                )));
            }
        }
        let id = source
            .idmap
            .id_at(doc_no)
            .ok_or_else(|| invalid(format!("документ {doc_no}: отсутствует идентификатор")))?;
        let mut envelope = serde_json::Map::new();
        envelope.insert("id".into(), Value::String(id.clone()));
        envelope.insert("_source".into(), source_value.clone());
        if let Some(p) = &raw_projection {
            envelope.insert("fields".into(), p.clone());
        }
        let mut doc =
            plan.take(&Value::Object(envelope), doc_no as usize, &mut report).map_err(invalid)?;
        if report.skipped != 0 || report.skipped_docs != 0 {
            return Err(invalid(format!(
                "документ {doc_no}: миграция обнаружила потери: {:?}",
                report.first
            )));
        }
        // Plan уже разобрал документ; raw bytes подставляются обратно в ячейки
        // внутренних полей, чтобы канонизация JSON не стала потерей формата.
        let source_text = std::str::from_utf8(&raw_source)
            .map_err(|e| invalid(format!("source UTF-8: {e}")))?
            .to_owned();
        let projection_text = raw_projection_bytes
            .as_ref()
            .map(|bytes| {
                std::str::from_utf8(bytes)
                    .map(str::to_owned)
                    .map_err(|e| invalid(format!("projection UTF-8: {e}")))
            })
            .transpose()?;
        doc.cells.retain(|(field, _, _)| {
            *field as usize != source_fid_at && Some(*field as usize) != projection_fid_at
        });
        doc.cells.push((source_fid_at as u32, 0, source_text));
        if let Some(raw) = projection_text {
            let field =
                projection_fid_at.ok_or_else(|| invalid("projection не объявлена в schema"))?;
            doc.cells.push((field as u32, 0, raw));
        }
        let cells: Vec<Cell<'_>> = doc
            .cells
            .iter()
            .map(|(field, slot, text)| Cell { field: *field as usize, slot: *slot, text })
            .collect();
        ix.add_cells(&id, &doc.arity, &cells);
        dead.push(!source.is_live(doc_no));
    }
    finish_raw(&mut sources)?;
    finish_raw(&mut projections)?;
    ix.flush_now();
    if let Some(why) = ix.poisoned() {
        return Err(invalid(why));
    }
    // Карта живых создаётся после каждого готового выходного куска: до этого
    // Indexer не знает границу chunk-а, а после flush каталоги неизменяемы.
    let mut base = 0usize;
    let mut sources = stored_walk(&source, Some(source_at));
    let mut projections = stored_walk(&source, projection_at);
    for n in 0..ix.segments() {
        let dir = output_fresh_root.join(format!("seg{n:04}"));
        let out = index::load(&dir.to_string_lossy())?;
        let end = base
            .checked_add(out.docs)
            .filter(|end| *end <= dead.len())
            .ok_or_else(|| invalid("выход содержит лишние документы"))?;
        let live = svindb_core::live::with_dead(out.docs, &dead[base..end]);
        svindb_core::fs::atomic_write(&dir.join("live.bin"), &live, 0o600)?;
        let checked = index::load(&dir.to_string_lossy())?;
        let mut output_sources = stored_walk(&checked, checked.field_of(nested::SOURCE_FIELD));
        let mut output_projections =
            stored_walk(&checked, checked.field_of(nested::PROJECTION_FIELD));
        for local in 0..checked.docs as u32 {
            let original = base as u32 + local;
            let want_id = source
                .idmap
                .id_at(original)
                .ok_or_else(|| invalid("выходная проверка: отсутствует исходный id"))?;
            let got_id = checked
                .idmap
                .id_at(local)
                .ok_or_else(|| invalid("выходная проверка: отсутствует выходной id"))?;
            if want_id != got_id || checked.is_live(local) != source.is_live(original) {
                return Err(invalid(format!(
                    "выходная проверка: id/live не совпали у документа {original}"
                )));
            }
            let got_source = next_raw(&mut output_sources, local)?
                .ok_or_else(|| invalid("выходная проверка: отсутствует source"))?;
            let want_source = next_raw(&mut sources, original)?;
            if Some(got_source) != want_source {
                return Err(invalid(format!(
                    "выходная проверка: source не совпал у документа {original}"
                )));
            }
            let got = next_raw(&mut output_projections, local)?.filter(|v| !v.is_empty());
            let want = next_raw(&mut projections, original)?.filter(|v| !v.is_empty());
            if got != want {
                return Err(invalid(format!(
                    "выходная проверка: projection не совпал у документа {original}"
                )));
            }
        }
        finish_raw(&mut output_sources)?;
        finish_raw(&mut output_projections)?;
        base += out.docs;
    }
    finish_raw(&mut sources)?;
    finish_raw(&mut projections)?;
    if base != source.docs {
        return Err(invalid("выход потерял физические документы"));
    }
    std::fs::File::open(output_fresh_root)?.sync_all()?;
    Ok(
        serde_json::json!({"docs": source.docs, "segments": ix.segments(), "seq": source.seq, "dead": dead.iter().filter(|x| **x).count()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_limit_refused_before_reading_source() {
        let error =
            rebuild(Path::new("/нет"), Path::new("/нет"), Path::new("/нет-вывод"), 0).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn existing_output_is_never_overwritten() {
        let root =
            std::env::temp_dir().join(format!("svindb-migration-existing-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let error = rebuild(Path::new("/нет"), Path::new("/нет"), &root, 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        fs::remove_dir(root).unwrap();
    }
}
