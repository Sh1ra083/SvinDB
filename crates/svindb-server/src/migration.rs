//! Офлайновая миграция отделена от сетевого runtime и никогда не меняет вход.
use std::io;
use std::path::Path;

pub const INCOMPLETE: &str = ".migration-incomplete";

pub fn ensure_writable(root: &Path) -> Result<(), String> {
    let absolute = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    for parent in absolute.ancestors() {
        match std::fs::symlink_metadata(parent.join(".migration-read-only")) {
            Ok(_) => return Err("мигрированная копия доступна только для чтения: rollback после новых ACK не подтверждён".into()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

pub fn ensure_ready(root: &Path) -> Result<(), String> {
    let absolute = if root.exists() {
        std::fs::canonicalize(root).map_err(|e| e.to_string())?
    } else if root.is_absolute() {
        root.to_path_buf()
    } else {
        std::env::current_dir().map_err(|e| e.to_string())?.join(root)
    };
    // Проверяется и родитель: индекс внутри незавершённого multi-root нельзя
    // открыть в обход общего gate через --single-index.
    for parent in absolute.ancestors() {
        for marker in [INCOMPLETE, ".incomplete"] {
            match std::fs::symlink_metadata(parent.join(marker)) {
                Ok(_) => {
                    return Err(format!(
                        "{}: миграция не завершена; запуск запрещён",
                        parent.display()
                    ))
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.to_string()),
            }
        }
    }
    Ok(())
}

pub fn command(args: &[String]) -> io::Result<()> {
    let invalid = || {
        io::Error::other("migration-preflight <index>; migration-segment <index> <segment> <new-output> <docs-per-segment>; catalog-build <index> <memory-entries>")
    };
    let root = args.get(2).ok_or_else(invalid)?;
    let result = match args[1].as_str() {
        "migration-preflight" if args.len() == 3 => {
            let _lock = svindb_core::manifest::ReaderLock::take(root)?;
            crate::migration_preflight::inspect(Path::new(root))?
        }
        "migration-segment" | "migration-encoded-segment" if args.len() == 6 => {
            let _lock = svindb_core::manifest::ReaderLock::take(root)?;
            let segment = std::fs::canonicalize(&args[3])?;
            let source = std::fs::canonicalize(root)?;
            let output = Path::new(&args[4]);
            let parent = std::fs::canonicalize(output.parent().unwrap_or(Path::new(".")))?;
            if parent.starts_with(&source) {
                return Err(io::Error::other("output не может находиться внутри источника"));
            }
            if segment.parent() != Some(source.as_path()) {
                return Err(io::Error::other("сегмент обязан принадлежать входному индексу"));
            }
            let listed = svindb_core::manifest::segments_checked(root).map_err(io::Error::other)?;
            if !listed.iter().any(|p| std::fs::canonicalize(p).ok().as_ref() == Some(&segment)) {
                return Err(io::Error::other("сегмент отсутствует в manifest"));
            }
            let count: u32 = args[5].parse().map_err(|_| invalid())?;
            if count == 0 || count > 1_000_000 {
                return Err(invalid());
            }
            if args[1] == "migration-encoded-segment" {
                let preflight = crate::migration_preflight::inspect(&source)?;
                if preflight["ready_for_encoded_migration"] != true {
                    return Err(io::Error::other(
                        "encoded migration требует source-disabled индекс и согласованный WAL",
                    ));
                }
                crate::migration_encoded::rebuild(&source, &segment, Path::new(&args[4]))?
            } else {
                crate::migration_segment::rebuild(&source, &segment, Path::new(&args[4]), count)?
            }
        }
        "catalog-build" if args.len() == 4 => {
            let entries: usize = args[3].parse().map_err(|_| invalid())?;
            if !(1..=4_000_000).contains(&entries) {
                return Err(invalid());
            }
            let _lock = svindb_core::manifest::WriterLock::take(root)?;
            let records = svindb_core::catalog_external::build(root, entries)?;
            serde_json::json!({"records": records, "ready": true})
        }
        _ => return Err(invalid()),
    };
    println!("{}", serde_json::to_string(&result).map_err(io::Error::other)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn incomplete_parent_refuses_nested_index() {
        let path = std::env::temp_dir().join(format!("migration-guard-{}", std::process::id()));
        std::fs::create_dir_all(path.join("indices/i-one")).unwrap();
        std::fs::write(path.join(INCOMPLETE), b"running").unwrap();
        assert!(ensure_ready(&path.join("indices/i-one")).is_err());
        std::fs::remove_file(path.join(INCOMPLETE)).unwrap();
        assert!(ensure_ready(&path.join("indices/i-one")).is_ok());
        std::fs::remove_dir_all(path).unwrap();
    }
}
