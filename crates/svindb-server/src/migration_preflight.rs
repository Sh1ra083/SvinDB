//! Read-only предохранитель перед переносом одного legacy-индекса.

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use svindb_core::{manifest, wal};

const HEADER: usize = 17;
const RUNTIME_FILE_LIMIT: u64 = 128 * 1024 * 1024 + HEADER as u64;

/// Осматривает индекс без открытия writer/runtime и без изменения файлов.
pub fn inspect(root: &Path) -> io::Result<Value> {
    let root_s = root
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "путь индекса не UTF-8"))?;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let segments = match manifest::segments_checked(root_s) {
        Ok(paths) => paths,
        Err(error) => {
            errors.push(json!({"code":"manifest", "message":error}));
            Vec::new()
        }
    };
    let mut segment_docs = 0u64;
    let mut published = 0u64;
    let segment_items: Vec<Value> = segments.iter().map(|path| {
            let p = Path::new(path);
            match svindb_core::index::read_head(path) {
                Ok(head) => {
                    if head.docs > 0 && !head.fields.iter().any(|f| f.name == crate::nested::SOURCE_FIELD) {
                        errors.push(json!({"code":"source_missing", "path":path}));
                    }
                    segment_docs = segment_docs.saturating_add(head.docs as u64);
                    published = published.max(head.seq);
                    json!({"name": manifest::name_of(path), "path": path, "exists": p.exists(),
                        "docs": head.docs, "live": Value::Null, "layout": head.layout, "seq": head.seq})
                }
                Err(error) => {
                    errors.push(json!({"code":"segment_head", "path":path, "message":error.to_string()}));
                    json!({"name": manifest::name_of(path), "path": path, "exists": p.exists(),
                        "docs": Value::Null, "live": Value::Null, "layout": Value::Null, "seq": Value::Null})
                }
            }
        })
        .collect();

    let schema = inspect_json(root.join("schema.json"), "schema", &mut errors);
    let indexmetadata =
        inspect_json_optional(root.join("index.json"), "indexmetadata", &mut errors);
    let mapping = inspect_json_optional(root.join("mapping.json"), "mapping", &mut errors);
    let source_disabled = source_disabled(&mapping);
    match svindb_core::schema::Registry::load(root_s) {
        Ok(Some(registry)) if registry.fields().iter().any(|field| field.dead.is_some()) => {
            errors.push(json!({"code":"dead_schema_fields", "message":"требуется отдельная миграция похороненных полей"}));
        }
        Err(error) => errors.push(json!({"code":"schema_registry", "message":error.to_string()})),
        _ => {}
    }
    if source_disabled {
        warnings.push(
            json!({"code":"source_disabled", "message":"mapping отключает сохранение source"}),
        );
        if segment_docs != 0 {
            errors.push(json!({"code":"source_disabled_with_docs","message":"индекс содержит документы, но mapping отключает source"}));
        }
    }

    let checkpoint = match read_checkpoint(root) {
        Ok(value) => value,
        Err(error) => {
            errors.push(json!({"code":"checkpoint", "message":error.to_string()}));
            0
        }
    };
    let mut records = 0u64;
    let mut max_record = 0u64;
    let mut max_seq = 0u64;
    let mut max_file = 0u64;
    let mut last_seq = None;
    let mut files_out = Vec::new();
    let mut clean = errors.is_empty();
    let files = match wal_files_checked(root, root_s) {
        Ok(files) => files,
        Err(error) => {
            errors.push(json!({"code":"wal_files", "message":error.to_string()}));
            clean = false;
            Vec::new()
        }
    };
    for path in files {
        let size = std::fs::symlink_metadata(&path).map(|m| m.len()).unwrap_or(0);
        max_file = max_file.max(size);
        let before_records = records;
        let mut file_error = None;
        match scan_file(
            &path,
            checkpoint,
            &mut records,
            &mut max_record,
            &mut max_seq,
            &mut last_seq,
        ) {
            Ok(()) => {}
            Err(error) => {
                clean = false;
                file_error = Some(error);
            }
        }
        if file_error.is_some() {
            errors.push(json!({"code":"wal", "path":path, "message":file_error.as_ref().map(ToString::to_string)}));
        }
        let file_records = records.saturating_sub(before_records);
        files_out.push(json!({
            "name": Path::new(&path).file_name().and_then(|n| n.to_str()).unwrap_or(""),
            "path": path,
            "bytes": size,
            "records_seen": file_records,
            "runtime_file_compatible": size <= RUNTIME_FILE_LIMIT
        }));
    }
    let no_tail = max_seq <= checkpoint;
    let runtime_file_compatible = max_file <= RUNTIME_FILE_LIMIT;
    let ready = clean && no_tail && runtime_file_compatible;
    // Отсутствие source запрещает логическую пересборку, но не побитовый
    // перенос кодированных значений. Прочие ошибки и WAL-tail не обходятся.
    let encoded_ready = source_disabled
        && no_tail
        && runtime_file_compatible
        && errors.iter().all(|e| {
            matches!(
                e.get("code").and_then(Value::as_str),
                Some("source_missing" | "source_disabled_with_docs")
            )
        });
    Ok(json!({"root":root.display().to_string(), "segments":segment_items,
        "docs":segment_docs, "schema":schema, "indexmetadata":indexmetadata,
        "mapping":mapping, "source_disabled":source_disabled,
        "wal":{"checkpoint":checkpoint, "published":published, "max_seq":max_seq,
            "records":records, "max_file_bytes":max_file, "max_record_body_bytes":max_record,
            "runtime_file_compatible":runtime_file_compatible, "files":files_out},
        "warnings":warnings, "errors":errors, "ready_for_offline_migration":ready,
        "ready_for_encoded_migration":encoded_ready}))
}

fn wal_files_checked(root: &Path, _root_s: &str) -> io::Result<Vec<String>> {
    // Отдельный строгий обход: неизвестный файл нельзя молча пропустить.
    let dir = root.join(wal::DIR);
    match std::fs::symlink_metadata(&dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
        Ok(m) if !m.file_type().is_dir() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{dir:?}: WAL обязан быть каталогом"),
            ))
        }
        Ok(_) => {}
    }
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == wal::CHECKPOINT || name.starts_with(".checkpoint.tmp-") {
            continue;
        }
        if wal::number_of(&name).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{dir:?}/{name}: неизвестное имя WAL-файла"),
            ));
        }
        let kind = std::fs::symlink_metadata(entry.path())?;
        if !kind.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{dir:?}/{name}: WAL-файл не regular"),
            ));
        }
        names.push((wal::number_of(&name).unwrap(), entry.path()));
    }
    names.sort_by_key(|x| x.0);
    Ok(names.into_iter().map(|(_, p)| p.to_string_lossy().into_owned()).collect::<Vec<_>>())
}

fn read_checkpoint(root: &Path) -> io::Result<u64> {
    let path = root.join(wal::DIR).join(wal::CHECKPOINT);
    let meta = match std::fs::symlink_metadata(&path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    if !meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "checkpoint не является regular-файлом",
        ));
    }
    let bytes = svindb_core::fs::read_regular_limited(&path, 64, true)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "checkpoint не UTF-8"))?;
    text.trim()
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "checkpoint не является u64"))
}

fn scan_file(
    path: &str,
    checkpoint: u64,
    records: &mut u64,
    max_record: &mut u64,
    max_seq: &mut u64,
    last_seq: &mut Option<u64>,
) -> io::Result<()> {
    let _meta = std::fs::symlink_metadata(path)?;
    let mut input = BufReader::new(File::open(path)?);
    loop {
        let mut header = [0u8; HEADER];
        let got = read_partial(&mut input, &mut header)?;
        if got == 0 {
            return Ok(());
        }
        if got != HEADER {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "обрезанный WAL-заголовок"));
        }
        let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        if !(9..=wal::MAX_RECORD_BODY).contains(&len) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "негодная длина WAL-записи"));
        }
        let mut raw = vec![0u8; 8 + len];
        raw[..HEADER].copy_from_slice(&header);
        input.read_exact(&mut raw[HEADER..]).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "обрезанное тело WAL-записи")
        })?;
        let scan = wal::scan(&raw);
        let rec = scan.records.first().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "CRC WAL-записи не сходится")
        })?;
        if !matches!(
            rec.kind,
            wal::ADD
                | wal::DELETE
                | wal::PUT
                | wal::ADD_CELLS
                | wal::PUT_CELLS
                | wal::BATCH
                | wal::MUTATION
        ) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "неизвестный вид WAL-записи"));
        }
        let valid_strings = |bytes: &[u8]| {
            wal::decode_strings(bytes).is_some_and(|strings| {
                let refs: Vec<_> = strings.iter().map(String::as_str).collect();
                wal::encode_strings(&refs) == bytes
            })
        };
        let valid_body = match rec.kind {
            wal::BATCH => wal::decode_batch(&rec.body)
                .is_some_and(|(_, bodies)| bodies.iter().all(|body| valid_strings(body))),
            wal::MUTATION => wal::decode_mutation(&rec.body).is_some_and(|(kind, bodies, ids)| {
                bodies.iter().all(|body| valid_strings(body))
                    && wal::encode_mutation(kind, &bodies, &ids) == rec.body
            }),
            _ => valid_strings(&rec.body),
        };
        if !valid_body {
            return Err(io::Error::other("невалидное тело WAL при корректной CRC"));
        }
        if last_seq.is_some_and(|previous| rec.seq <= previous) {
            return Err(io::Error::other("WAL sequence повторён или убывает"));
        }
        if rec.seq > checkpoint
            && rec.seq != last_seq.unwrap_or(checkpoint).max(checkpoint).saturating_add(1)
        {
            return Err(io::Error::other("пропуск seq после checkpoint"));
        }
        *last_seq = Some(rec.seq);
        *records += 1;
        *max_record = (*max_record).max((len - 9) as u64);
        *max_seq = (*max_seq).max(rec.seq);
    }
}

fn read_partial<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        let got = reader.read(&mut buf[n..])?;
        if got == 0 {
            break;
        }
        n += got;
    }
    Ok(n)
}

fn inspect_json(path: PathBuf, name: &str, errors: &mut Vec<Value>) -> Value {
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.file_type().is_file() => {
            errors.push(json!({"code":"metadata_shape","field":name,"message":"файл не regular"}));
            return Value::Null;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            errors.push(json!({"code":"missing_source_field","field":name}));
            return Value::Null;
        }
        Err(e) => {
            errors.push(json!({"code":"metadata_read","field":name,"message":e.to_string()}));
            return Value::Null;
        }
        _ => {}
    }
    let bytes = match svindb_core::fs::read_regular_limited(&path, 4 << 20, false) {
        Ok(v) => v,
        Err(e) => {
            errors.push(json!({"code":"metadata_read","field":name,"message":e.to_string()}));
            return Value::Null;
        }
    };
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(v) if v.is_object() => v,
        Ok(_) => {
            errors.push(
                json!({"code":"metadata_shape","field":name,"message":"ожидался JSON-объект"}),
            );
            Value::Null
        }
        Err(e) => {
            errors.push(json!({"code":"metadata_json","field":name,"message":e.to_string()}));
            Value::Null
        }
    }
}

fn inspect_json_optional(path: PathBuf, name: &str, errors: &mut Vec<Value>) -> Value {
    if std::fs::symlink_metadata(&path).is_err_and(|e| e.kind() == io::ErrorKind::NotFound) {
        return Value::Null;
    }
    inspect_json(path, name, errors)
}

fn source_disabled(mapping: &Value) -> bool {
    mapping
        .get("mappings")
        .and_then(|v| v.get("_source"))
        .and_then(|v| v.get("enabled"))
        .and_then(Value::as_bool)
        == Some(false)
        || mapping.pointer("/settings/index/mapping/source/mode").and_then(Value::as_str)
            == Some("disabled")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate(bytes: &[u8], checkpoint: u64, name: &str) -> io::Result<()> {
        let path =
            std::env::temp_dir().join(format!("wal-preflight-{}-{name}", std::process::id()));
        std::fs::write(&path, bytes)?;
        let result =
            scan_file(&path.to_string_lossy(), checkpoint, &mut 0, &mut 0, &mut 0, &mut None);
        std::fs::remove_file(path)?;
        result
    }

    #[test]
    fn strict_wal_framing_crc_sequence_and_body() {
        let body = wal::encode_strings(&["id", "значение"]);
        let mut bytes = Vec::new();
        wal::append(&mut bytes, 10, wal::ADD, &body);
        wal::append(&mut bytes, 11, wal::PUT, &body);
        assert!(validate(&bytes, 9, "ok").is_ok());
        assert!(validate(&bytes, 8, "gap").is_err());
        assert!(validate(&bytes[..bytes.len() - 1], 9, "torn").is_err());
        let mut corrupt = bytes.clone();
        corrupt[20] ^= 1;
        assert!(validate(&corrupt, 9, "crc").is_err());
        wal::append(&mut bytes, 11, wal::ADD, &body);
        assert!(validate(&bytes, 20, "duplicate").is_err());
        bytes.clear();
        wal::append(&mut bytes, 1, wal::ADD, b"not encoded strings");
        assert!(validate(&bytes, 0, "body").is_err());
    }
}
