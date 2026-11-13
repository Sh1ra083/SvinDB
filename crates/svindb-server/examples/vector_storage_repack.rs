//! Экспериментальная offline-перекладка старых packed-сегментов.
//!
//! Это не миграция production-индекса: вход неизменяем, выход обязан быть новым
//! каталогом, а незавершённый запуск оставляет `.incomplete` для ручного разбора.
//! Вызывающий обязан заранее остановить writer. Поддержаны только прямые поля
//! исходного JSON, без mapping aliases/source_path/constant и nested vectors.

use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const COPY: usize = 1 << 20;
const MARKER: &str = ".incomplete";

fn is_derived_or_temporary(name: &std::ffi::OsStr) -> bool {
    name == svindb_core::catalog::FILE || name == "catalog.bin.tmp"
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn copy_regular(src: &Path, dst: &Path) -> io::Result<u64> {
    let meta = fs::symlink_metadata(src)?;
    if !meta.file_type().is_file() {
        return Err(invalid(format!("{}: ожидается обычный файл", src.display())));
    }
    let mut input = File::open(src)?;
    let mut output = OpenOptions::new().write(true).create_new(true).open(dst)?;
    let mut buf = vec![0u8; COPY];
    let mut total = 0;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        output.write_all(&buf[..n])?;
        total += n as u64;
    }
    output.sync_all()?;
    Ok(total)
}

fn copy_tree(src: &Path, dst: &Path) -> io::Result<u64> {
    let meta = fs::symlink_metadata(src)?;
    if meta.file_type().is_symlink() {
        return Err(invalid(format!("{}: symlink запрещён", src.display())));
    }
    if meta.is_file() {
        return copy_regular(src, dst);
    }
    if !meta.is_dir() {
        return Err(invalid(format!("{}: неизвестный тип", src.display())));
    }
    fs::create_dir(dst)?;
    let mut total = 0;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".writer.lock"
            || name == ".pins"
            || name == ".trash"
            || name == MARKER
            || is_derived_or_temporary(&name)
        {
            continue;
        }
        total += copy_tree(&entry.path(), &dst.join(name))?;
    }
    Ok(total)
}

fn current_representation<'a>(
    source: &'a Value,
    projection: Option<&'a Value>,
) -> Option<&'a serde_json::Map<String, Value>> {
    if let Some(projection) = projection {
        // Явное {} означает пустую поисковую проекцию, а не fallback к source.
        return projection.as_object();
    }
    source.as_object()
}

fn source_presence(
    source_store: &svindb_core::store::Store,
    projection_store: Option<&svindb_core::store::Store>,
    field: &str,
    dims: usize,
    docs: usize,
) -> io::Result<Vec<bool>> {
    let mut flags = vec![false; docs];
    let mut source_walk = source_store.walk();
    let mut projection_walk = projection_store.map(svindb_core::store::Store::walk);
    let mut seen = 0usize;
    let mut seen_docs = vec![false; docs];
    while let Some((doc, raw)) = source_walk.next() {
        let value: Value =
            serde_json::from_slice(raw).map_err(|e| invalid(format!("source doc {doc}: {e}")))?;
        let projection = if let Some(walk) = projection_walk.as_mut() {
            let Some((projection_doc, raw)) = walk.next() else {
                return Err(invalid("projection недоступен полностью"));
            };
            if projection_doc != doc {
                return Err(invalid("source и projection имеют разные номера документов"));
            }
            if raw.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_slice(raw)
                        .map_err(|e| invalid(format!("projection doc {doc}: {e}")))?,
                )
            }
        } else {
            None
        };
        let Some(object) = current_representation(&value, projection.as_ref()) else {
            return Err(invalid("authoritative source недоступен или не является JSON-объектом"));
        };
        let present = match object.get(field) {
            None | Some(Value::Null) => false,
            Some(Value::Array(items)) if items.is_empty() => false,
            Some(Value::Array(items)) => {
                if items.len() != dims
                    || items
                        .iter()
                        .any(|v| !v.is_number() || v.as_f64().is_none_or(|x| !x.is_finite()))
                {
                    return Err(invalid(format!("поле {field}: недействительный вектор")));
                }
                true
            }
            Some(_) => return Err(invalid(format!("поле {field}: в source не массив вектора"))),
        };
        let at = doc as usize;
        if at >= docs || seen_docs[at] {
            return Err(invalid("source содержит повторный или лишний документ"));
        }
        seen_docs[at] = true;
        flags[at] = present;
        seen += 1;
    }
    if source_walk.broken() || seen != docs {
        return Err(invalid("authoritative source недоступен полностью"));
    }
    if projection_walk.as_mut().is_some_and(|walk| walk.next().is_some()) {
        return Err(invalid("projection содержит лишний документ"));
    }
    if projection_walk.as_ref().is_some_and(|walk| walk.broken()) {
        return Err(invalid("projection недоступен полностью"));
    }
    Ok(flags)
}

struct MemberWriter<'a>(&'a mut svindb_core::container::Writer);

impl Write for MemberWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for chunk in bytes.chunks(COPY) {
            self.0.append(chunk)?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn repack_segment(src: &Path, dst: &Path) -> io::Result<(u64, u64, u64)> {
    fs::create_dir(dst)?;
    let source = svindb_core::index::load(src.to_str().ok_or_else(|| invalid("путь не UTF-8"))?)?;
    let source_head = svindb_core::index::read_head(src.to_str().unwrap())?;
    if source_head.container.is_none() {
        return Err(invalid("поддерживаются только packed-сегменты"));
    }
    let mut present_docs = 0u64;
    let mut vector_bytes = 0u64;
    let mut identical = 0u64;
    let mut replaced = std::collections::BTreeSet::new();
    let mut expected = std::collections::BTreeMap::new();
    let mut writer =
        svindb_core::container::Writer::create(dst.to_str().unwrap(), svindb_core::index::LAYOUT)?;
    let source_field = source
        .field_of("__svindb_source")
        .ok_or_else(|| invalid("authoritative __svindb_source отсутствует"))?;
    let projection_field = source.field_of("__svindb_projection");
    let source_store = source.fields[source_field]
        .store
        .as_ref()
        .ok_or_else(|| invalid("authoritative __svindb_source storage отсутствует"))?;
    let projection_store = projection_field.and_then(|i| source.fields[i].store.as_ref());
    for field in &source.fields {
        if field.vectors.is_none() {
            continue;
        }
        if field.group != 0 || field.name.contains('.') {
            return Err(invalid(format!(
                "поле {}: требуется прямое root vector поле без aliases",
                field.name
            )));
        }
        let vectors = field.vectors.as_ref().unwrap();
        let flags = source_presence(
            source_store,
            projection_store,
            &field.name,
            vectors.dims(),
            source.docs,
        )?;
        expected.insert(field.name.clone(), flags.clone());
        present_docs += flags.iter().filter(|x| **x).count() as u64;
        let q8 = format!("{}.q8", field.stem);
        let qm = format!("{}.qm", field.stem);
        if flags.iter().any(|v| *v) {
            writer.begin(&q8)?;
            let meta = vectors
                .compact_with_presence(&|doc| flags[doc as usize], &mut MemberWriter(&mut writer))?
                .ok_or_else(|| invalid("непустая presence карта не создала коды"))?;
            vector_bytes += writer.end()? + meta.len() as u64;
            writer.put(&qm, &meta)?;
        }
        replaced.insert(q8);
        replaced.insert(qm);
    }
    let members: Vec<(String, u64)> =
        svindb_core::container::Table::read(&src.join("seg.dat").to_string_lossy())?
            .members()
            .iter()
            .map(|m| (m.name.clone(), m.len))
            .collect();
    for (name, len) in &members {
        if name == "head.tsv"
            || name == "live.bin"
            || name == "sums.tsv"
            || name == "seg.dat"
            || replaced.contains(name)
        {
            continue;
        }
        let region = source.src.open(name)?;
        writer.begin(name)?;
        for chunk in region.slice(0, region.len_bytes()).chunks(COPY) {
            writer.append(chunk)?;
        }
        writer.end()?;
        identical += *len;
    }
    // head/live лежат рядом с packed seg.dat и не являются его members.
    if src.join("live.bin").exists() {
        copy_regular(&src.join("live.bin"), &dst.join("live.bin"))?;
    }
    let container_len = writer.finish()?;
    let old_head = fs::read_to_string(src.join("head.tsv"))?;
    let mut head_text = String::new();
    for line in old_head.lines() {
        if line.starts_with("layout\t") || line.starts_with("container\t") {
            continue;
        }
        head_text.push_str(line);
        head_text.push('\n');
    }
    head_text = format!("layout\t13\ncontainer\tseg.dat\t{container_len}\n{head_text}");
    let mut head_file =
        OpenOptions::new().write(true).create_new(true).open(dst.join("head.tsv"))?;
    head_file.write_all(head_text.as_bytes())?;
    head_file.sync_all()?;
    svindb_core::sums::write(dst.to_str().unwrap())?;
    let verified = svindb_core::index::load(dst.to_str().unwrap())?;
    for (name, flags) in &expected {
        let field = &verified.fields
            [verified.field_of(name).ok_or_else(|| invalid("vector field исчез"))?];
        match &field.vectors {
            None if flags.iter().any(|x| *x) => {
                return Err(invalid("непустой vector payload исчез"))
            }
            None => continue,
            Some(v) => {
                if v.docs() != source.docs {
                    return Err(invalid("новый vector member имеет другую длину"));
                }
                let old = source
                    .field_of(&field.name)
                    .and_then(|i| source.fields[i].vectors.as_ref())
                    .ok_or_else(|| invalid("vector field исчез при проверке"))?;
                for dim in 0..v.dims() {
                    if v.lo(dim).to_bits() != old.lo(dim).to_bits()
                        || v.step(dim).to_bits() != old.step(dim).to_bits()
                    {
                        return Err(invalid("шкала квантования изменилась"));
                    }
                }
                for doc in 0..source.docs as u32 {
                    if flags[doc as usize] != v.code(doc).is_some() {
                        return Err(invalid("наличие vector значения не совпало с source"));
                    }
                    if let (Some(a), Some(b)) = (old.code(doc), v.code(doc)) {
                        if a != b {
                            return Err(invalid("коды vector значения изменились"));
                        }
                    }
                }
            }
        }
    }
    for (name, _) in &members {
        if replaced.contains(name) {
            continue;
        }
        let before = source.src.open(name)?;
        let after = verified.src.open(name)?;
        if before.len_bytes() != after.len_bytes()
            || before.slice(0, before.len_bytes()) != after.slice(0, after.len_bytes())
        {
            return Err(invalid(format!("не-векторный member {name} изменился")));
        }
    }
    File::open(dst)?.sync_all()?;
    Ok((source.docs as u64, present_docs, vector_bytes + identical))
}

fn rebuild_catalog(output: &Path, segment_names: &[String]) -> io::Result<()> {
    let loaded: Vec<svindb_core::index::Index> = segment_names
        .iter()
        .map(|name| {
            svindb_core::index::load(&output.join(name).to_string_lossy())
                .map_err(|e| invalid(format!("выходной сегмент {name}: {e}")))
        })
        .collect::<io::Result<_>>()?;
    let refs: Vec<&svindb_core::index::Index> = loaded.iter().collect();
    let bytes = svindb_core::catalog::build(&refs);
    let catalog = output.join(svindb_core::catalog::FILE);
    let mut file = OpenOptions::new().write(true).create_new(true).open(&catalog)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    File::open(output)?.sync_all()?;

    // Открытие сверяет stamp с текущими head_id, а locate сверяет каждый
    // адрес с настоящим ID в выходном сегменте. Пустой состав проверяется
    // отдельно: для него IdDirectory закономерно не создаётся.
    if refs.is_empty() {
        let opened = svindb_core::catalog::Catalog::open(
            &output.to_string_lossy(),
            svindb_core::catalog::stamp(&[]),
        )
        .ok_or_else(|| invalid("пустой каталог не открылся"))?;
        if !opened.is_empty() {
            return Err(invalid("пустой каталог содержит записи"));
        }
        return Ok(());
    }
    let directory = svindb_core::maintain::IdDirectory::open(&refs)
        .ok_or_else(|| invalid("ID directory не открылся для выходных head_id"))?;
    for (segment, index) in loaded.iter().enumerate() {
        // Верификация не удерживает ещё одну копию всех ID/ответов корпуса.
        for start in (0..index.docs).step_by(1024) {
            let end = (start + 1024).min(index.docs);
            let query_ids: Vec<String> = (start..end)
                .map(|doc| {
                    index.idmap.id_at(doc as u32).ok_or_else(|| {
                        invalid(format!(
                            "выходной сегмент {segment}: отсутствует ID документа {doc}"
                        ))
                    })
                })
                .collect::<io::Result<_>>()?;
            let hashes: Vec<_> = query_ids.iter().map(|id| svindb_core::bloom::hash(id)).collect();
            let found = directory
                .locate(&refs, &query_ids, &hashes)
                .ok_or_else(|| invalid("ID directory не подтвердил адреса выходных документов"))?;
            for (doc, places) in (start..end).zip(found) {
                if !places.contains(&(segment, doc as u32)) {
                    return Err(invalid("ID directory потерял адрес выходного документа"));
                }
            }
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let input = PathBuf::from(
        args.next()
            .ok_or_else(|| invalid("использование: vector_storage_repack <input> <new-output>"))?,
    );
    let output = PathBuf::from(
        args.next()
            .ok_or_else(|| invalid("использование: vector_storage_repack <input> <new-output>"))?,
    );
    if args.next().is_some() {
        return Err(invalid("ожидаются только input и new-output"));
    }
    let input = fs::canonicalize(input)?;
    if output.exists() {
        return Err(invalid("output root уже существует; overwrite запрещён"));
    }
    let parent =
        output.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
    let output = fs::canonicalize(parent)?
        .join(output.file_name().ok_or_else(|| invalid("output без имени"))?);
    if output.starts_with(&input) || input.starts_with(&output) {
        return Err(invalid("input и output пересекаются"));
    }
    fs::create_dir(&output)?;
    fs::write(output.join(MARKER), b"experimental; conversion in progress\n")?;
    let names = svindb_core::manifest::segments_checked(
        input.to_str().ok_or_else(|| invalid("input не UTF-8"))?,
    )
    .map_err(invalid)?;
    let names_set: std::collections::BTreeSet<_> =
        names.iter().map(|x| Path::new(x).file_name().unwrap().to_os_string()).collect();
    for entry in fs::read_dir(&input)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".writer.lock"
            || name == ".pins"
            || name == ".trash"
            || name == MARKER
            || is_derived_or_temporary(&name)
            || name == "segments.tsv"
            || names_set.contains(&name)
        {
            continue;
        }
        copy_tree(&entry.path(), &output.join(name))?;
    }
    let mut docs = 0u64;
    let mut present = 0u64;
    let mut bytes = 0u64;
    let mut output_segments = Vec::with_capacity(names.len());
    for name in names {
        let src = PathBuf::from(&name);
        let segment_name = src.file_name().ok_or_else(|| invalid("имя сегмента отсутствует"))?;
        let (d, p, b) = repack_segment(&src, &output.join(segment_name))?;
        output_segments.push(segment_name.to_string_lossy().into_owned());
        docs += d;
        present += p;
        bytes += b;
    }
    rebuild_catalog(&output, &output_segments)?;
    if input.join("segments.tsv").exists() {
        copy_regular(&input.join("segments.tsv"), &output.join("segments.tsv"))?;
    }
    fs::remove_file(output.join(MARKER))?;
    File::open(&output)?.sync_all()?;
    println!(
        "{}",
        serde_json::json!({"experimental": true, "documents": docs, "vectors_present": present, "bytes": bytes, "manifest_last": true, "source_wal_preserved": true})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("svindb-repack-{name}-{}", std::process::id()));
        fs::remove_dir_all(&root).ok();
        fs::create_dir_all(&root).expect("тестовый каталог");
        root
    }

    #[test]
    fn explicit_empty_projection_does_not_resurrect_source_vector() {
        let source = serde_json::json!({"embedding": [1.0, 2.0]});
        let empty = serde_json::json!({});
        assert!(current_representation(&source, Some(&empty)).unwrap().is_empty());
        assert!(current_representation(&source, None).unwrap().contains_key("embedding"));
        assert!(current_representation(&source, Some(&Value::Null)).is_none());
    }

    #[test]
    fn rebuilt_catalog_covers_nine_indexer_segments_and_missing_id() {
        let root = test_root("catalog-nine");
        let root_text = root.to_string_lossy().into_owned();
        let mut indexer =
            svindb_core::indexer::Indexer::new(&root_text, vec!["body".into()], vec![1.0], 1);
        for doc in 0..9 {
            indexer.add(&format!("doc-{doc}"), &["тело документа"]);
        }
        indexer.drain();
        let names = svindb_core::manifest::segments_checked(&root_text).expect("manifest");
        assert_eq!(names.len(), 9, "Indexer fixture обязан создать 9 сегментов");
        let names: Vec<String> = names
            .iter()
            .map(|name| Path::new(name).file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        rebuild_catalog(&root, &names).expect("пересборка каталога");
        assert!(root.join(svindb_core::catalog::FILE).is_file());
        let segments = svindb_core::Segments::load(&root_text).unwrap();
        assert!(segments.prepare_id_directory());
        assert_eq!(
            segments.locate_ids_ready_guarded(&["missing".into()], None),
            Some(vec![vec![]])
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rebuilt_catalog_is_safe_for_empty_index() {
        let root = test_root("catalog-empty");
        rebuild_catalog(&root, &[]).expect("пустой каталог");
        let opened = svindb_core::catalog::Catalog::open(
            &root.to_string_lossy(),
            svindb_core::catalog::stamp(&[]),
        )
        .expect("пустой каталог открывается");
        assert!(opened.is_empty());
        fs::remove_dir_all(root).ok();
    }
}
