//! Безопасные примитивы для файлов, на имена которых может влиять артефакт.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "macos")]
const O_NOFOLLOW: i32 = 0x0000_0100;
#[cfg(target_os = "linux")]
const O_NOFOLLOW: i32 = 0x0002_0000;
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
compile_error!("SvinDB требует определить O_NOFOLLOW для этой Unix-платформы");

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Проверяет, что путь относительный и состоит только из обычных компонентов.
pub fn relative_components(path: &str) -> io::Result<Vec<String>> {
    if path.is_empty() || path.len() > 4096 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "путь пуст или длиннее 4096 байт"));
    }
    let mut out = Vec::new();
    for part in Path::new(path).components() {
        match part {
            Component::Normal(v) => {
                let v = v.to_str().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "путь не в UTF-8")
                })?;
                if v.is_empty() || v.starts_with('.') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("негодный компонент пути {v:?}"),
                    ));
                }
                out.push(v.to_string());
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("путь {path:?} выходит из корня"),
                ))
            }
        }
    }
    if out.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "путь пуст"));
    }
    Ok(out)
}

/// Открывает существующий обычный файл, не следуя по ссылке последнего компонента.
pub fn open_regular(path: &Path, read: bool, write: bool) -> io::Result<File> {
    let file = OpenOptions::new().read(read).write(write).custom_flags(O_NOFOLLOW).open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} — не обычный файл", path.display()),
        ));
    }
    Ok(file)
}

/// То же, но файл обязан иметь единственную жёсткую ссылку.
///
/// Это требуется снимку: иначе обычный с виду член мог бы быть hardlink на
/// файл вне индекса и утечь в архив без единой символической ссылки.
pub fn open_private_regular(path: &Path) -> io::Result<File> {
    open_private_regular_rw(path, true, false)
}

/// Открывает файл с единственной жёсткой ссылкой в требуемом режиме.
pub fn open_private_regular_rw(path: &Path, read: bool, write: bool) -> io::Result<File> {
    let file = open_regular(path, read, write)?;
    if file.metadata()?.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} имеет несколько жёстких ссылок", path.display()),
        ));
    }
    Ok(file)
}

/// Читает обычный файл с проверенной длиной и fallible reservation.
pub fn read_regular(path: &Path, private: bool) -> io::Result<Vec<u8>> {
    read_regular_limited(path, u64::MAX, private)
}

/// То же с явным потолком до резервирования памяти.
pub fn read_regular_limited(path: &Path, max: u64, private: bool) -> io::Result<Vec<u8>> {
    let file = if private { open_private_regular(path)? } else { open_regular(path, true, false)? };
    let len_u64 = file.metadata()?.len();
    if len_u64 > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: {len_u64} байт, потолок {max}", path.display()),
        ));
    }
    let len = usize::try_from(len_u64).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "файл не помещается в адресное пространство")
    })?;
    let mut out = Vec::new();
    out.try_reserve_exact(len)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "файл не помещается в память"))?;
    let mut limited = file.take(len_u64.saturating_add(1));
    limited.read_to_end(&mut out)?;
    if out.len() != len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("размер файла изменился во время чтения: {} вместо {len}", out.len()),
        ));
    }
    Ok(out)
}

/// Атомарно заменяет файл через уникальный inode в том же каталоге.
///
/// `create_new` и уникальное имя не дают заранее подложить symlink. После
/// `rename` синхронизируется каталог, поэтому успешный возврат означает
/// crash-durable публикацию имени.
pub fn atomic_write(path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "у файла нет родительского каталога")
    })?;
    let name = path
        .file_name()
        .and_then(|x| x.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "имя файла не в UTF-8"))?;
    let mut last = None;
    for _ in 0..32 {
        let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = parent.join(format!(".{name}.tmp-{}-{n}", std::process::id()));
        let opened = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .custom_flags(O_NOFOLLOW)
            .open(&tmp);
        let mut file = match opened {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                last = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        };
        let result = (|| {
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
            file.write_all(bytes)?;
            file.sync_all()?;
            std::fs::rename(&tmp, path)?;
            File::open(parent)?.sync_all()
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        return result;
    }
    Err(last.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::AlreadyExists, "не выбрано временное имя")
    }))
}

/// Проверяет, что все уже существующие компоненты под `root` — настоящие
/// каталоги, а не ссылки. Последний компонент может отсутствовать.
pub fn no_symlink_parents(root: &Path, relative: &str) -> io::Result<()> {
    let parts = relative_components(relative)?;
    let mut at = root.to_path_buf();
    for part in parts.iter().take(parts.len().saturating_sub(1)) {
        at.push(part);
        match std::fs::symlink_metadata(&at) {
            Ok(m) if m.file_type().is_dir() => {}
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} — не настоящий каталог", at.display()),
                ))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => break,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Создаёт дерево каталогов, на каждом существующем компоненте проверяя, что
/// это каталог, а не ссылка. В отличие от `create_dir_all`, заранее
/// подложенный symlink не принимается как уже готовый родитель.
pub fn create_dir_all_real(path: &Path) -> io::Result<()> {
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} содержит ..", path.display()),
        ));
    }
    let mut ancestor = path.to_path_buf();
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(&ancestor) {
            Ok(m) if m.file_type().is_dir() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} — не настоящий каталог", ancestor.display()),
                ))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let name = ancestor.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "не найден существующий корень")
                })?;
                missing.push(name.to_os_string());
                ancestor = ancestor
                    .parent()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "нет родителя"))?
                    .to_path_buf();
            }
            Err(e) => return Err(e),
        }
    }
    // Проверять начинаем с ближайшего существующего каталога. Это сохраняет
    // штатные системные aliases вроде macOS `/var -> /private/var`, но ссылка,
    // подложенная именно на место создаваемого `wal`/snapshot, остаётся
    // последним компонентом и отвергается веткой выше.
    for part in missing.iter().rev() {
        ancestor.push(part);
        match std::fs::create_dir(&ancestor) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        if !std::fs::symlink_metadata(&ancestor)?.file_type().is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} — не настоящий каталог", ancestor.display()),
            ));
        }
    }
    Ok(())
}

/// Атомарно пишет новый файл под пустым/контролируемым корнем.
///
/// Промежуточные компоненты обязаны быть настоящими каталогами. Данные сперва
/// попадают в уникальный файл того же каталога, а имя публикуется только после
/// успешной записи и `sync_all`. Даже если локальный соперник успеет положить
/// на конечное имя ссылку, `rename` заменит саму ссылку, не последует по ней.
pub fn write_new_beneath_with(
    root: &Path,
    relative: &str,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let parts = relative_components(relative)?;
    let mut write = Some(write);
    let mut parent = root.to_path_buf();
    for part in parts.iter().take(parts.len() - 1) {
        parent.push(part);
        match std::fs::create_dir(&parent) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        let meta = std::fs::symlink_metadata(&parent)?;
        if !meta.file_type().is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} — не настоящий каталог", parent.display()),
            ));
        }
    }
    let name = parts.last().expect("проверен непустой путь");
    let path = parent.join(name);
    let mut last = None;
    for _ in 0..32 {
        let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = parent.join(format!(".{name}.restore-{}-{n}", std::process::id()));
        let opened = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(O_NOFOLLOW)
            .open(&tmp);
        let mut file = match opened {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                last = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        };
        let write = write.take().expect("писатель вызывается единожды");
        let result = (|| {
            write(&mut file)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            File::open(&parent)?.sync_all()
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        return result;
    }
    Err(last.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::AlreadyExists, "не выбрано временное имя restore")
    }))
}

/// Вариант для уже собранного в памяти содержимого.
pub fn write_new_beneath(root: &Path, relative: &str, bytes: &[u8]) -> io::Result<()> {
    write_new_beneath_with(root, relative, |file| file.write_all(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("svindb-fs-{tag}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn relative_path_rejects_every_escape_component() {
        for bad in ["", "../x", "a/../x", "/x", "./x", ".hidden", "a/.hidden"] {
            assert!(relative_components(bad).is_err(), "{bad:?} прошло");
        }
        assert_eq!(relative_components("seg0001/head.tsv").unwrap(), ["seg0001", "head.tsv"]);
    }

    #[cfg(unix)]
    #[test]
    fn regular_open_does_not_follow_a_symlink() {
        use std::os::unix::fs::symlink;
        let d = tmp("nofollow");
        let target = d.join("target");
        let link = d.join("link");
        std::fs::write(&target, b"secret").unwrap();
        symlink(&target, &link).unwrap();
        assert!(open_regular(&link, true, false).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"secret");
        std::fs::remove_dir_all(d).ok();
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_replaces_a_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;
        let d = tmp("atomic");
        let target = d.join("target");
        let out = d.join("state");
        std::fs::write(&target, b"secret").unwrap();
        symlink(&target, &out).unwrap();
        atomic_write(&out, b"new", 0o600).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"secret");
        assert_eq!(std::fs::read(&out).unwrap(), b"new");
        std::fs::remove_dir_all(d).ok();
    }

    #[test]
    fn directory_creation_refuses_a_symlink_parent() {
        use std::os::unix::fs::symlink;
        let root = tmp("mkdir-link");
        let outside = tmp("mkdir-outside");
        symlink(&outside, root.join("link")).unwrap();
        assert!(create_dir_all_real(&root.join("link/child")).is_err());
        assert!(!outside.join("child").exists());
    }
}
