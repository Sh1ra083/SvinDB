//! Физическая перепаковка packed-сегмента без восстановления source.
//!
//! Для source-disabled индексов невекторные колонки пересобираются из stored
//! ячеек, а векторы переходят в layout 13 без повторного квантования.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

const COPY: usize = 1 << 20;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
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

fn copy_file(source: &Path, target: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.file_type().is_file() {
        return Err(invalid(format!("{}: ожидается обычный файл", source.display())));
    }
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new().write(true).create_new(true).open(target)?;
    let mut buffer = vec![0u8; COPY];
    loop {
        let size = input.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        output.write_all(&buffer[..size])?;
    }
    output.sync_all()
}

fn validate_members(segment: &Path) -> io::Result<Vec<String>> {
    let mut names = Vec::new();
    let entries = fs::read_dir(segment)?;
    for entry in entries {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() {
            return Err(invalid(format!(
                "{}: ссылки, каталоги и специальные файлы запрещены",
                entry.path().display()
            )));
        }
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    if !names.iter().any(|name| name == "head.tsv") {
        return Err(invalid("packed-сегмент не содержит head.tsv"));
    }
    if !names.iter().any(|name| name == "seg.dat") {
        return Err(invalid("packed-сегмент не содержит seg.dat"));
    }
    Ok(names)
}

fn copy_member(
    source: &svindb_core::index::Index,
    writer: &mut svindb_core::container::Writer,
    name: &str,
) -> io::Result<u64> {
    let region = source.src.open(name)?;
    writer.begin(name)?;
    for chunk in region.slice(0, region.len_bytes()).chunks(COPY) {
        writer.append(chunk)?;
    }
    writer.end()
}

/// Пересобирает невекторные значения из сохранённых intake-ячеек, не создавая
/// source. Ограниченный договор: корневые numeric/keyword поля со store=true.
/// Пустую keyword-ячейку нельзя отличить от отсутствующей без source: отказ.
fn rebuild_columns(
    source: &svindb_core::index::Index,
    output: &Path,
) -> io::Result<svindb_core::index::Index> {
    use svindb_core::indexer::{Cell, Indexer};
    if source.docs > 1_000_000 || source.slots.len() != 1 {
        return Err(invalid("encoded rebuild ограничен миллионом документов в сегменте"));
    }
    for field in &source.fields {
        if field.group != 0
            || field.name.starts_with("__svindb_")
            || (field.kind.dims().is_none()
                && (!(field.kind.numeric() || field.kind.keyword())
                    || !field.stored
                    || (field.kind.keyword() && field.store.is_none())))
        {
            return Err(invalid(format!(
                "поле {}: нужны корневые сохранённые numeric/keyword либо dense vector",
                field.name
            )));
        }
    }
    let base = output.join(".encoded-columns");
    fs::create_dir(&base)?;
    let mut ix = Indexer::typed_positioned(
        &base.to_string_lossy(),
        source.fields.iter().map(|f| f.name.clone()).collect(),
        source.fields.iter().map(|f| f.boost).collect(),
        source.fields.iter().map(|f| f.kind).collect(),
        source.fields.iter().map(|f| f.stored).collect(),
        vec![false; source.fields.len()],
        u32::MAX,
    );
    ix.set_fids(source.fields.iter().map(|f| f.fid).collect());
    ix.set_groups(vec![0; source.fields.len()], vec![String::new()]);
    ix.set_searched(source.fields.iter().map(|f| f.searched).collect());
    ix.set_flush_bytes(0);
    ix.seq = source.seq;
    let mut walks: Vec<_> =
        source.fields.iter().map(|f| f.store.as_ref().map(|s| s.walk())).collect();
    let mut payload = 0usize;
    for doc in 0..source.docs as u32 {
        let mut values = Vec::new();
        for (at, walk) in walks.iter_mut().enumerate() {
            if source.fields[at].kind.dims().is_some() {
                if source.fields[at].stored {
                    return Err(invalid(
                        "stored dense vector требует отдельного пути сохранения raw поля",
                    ));
                }
                continue;
            }
            let raw = if let Some(walk) = walk {
                let (found, raw) = walk.next().ok_or_else(|| invalid("stored поток оборван"))?;
                if found != doc {
                    return Err(invalid("stored поток нарушил порядок документов"));
                }
                raw.to_vec()
            } else {
                // Numeric store в старом формате может быть представлен самой
                // колонкой, без второй текстовой копии. Переносим все её значения.
                let field = &source.fields[at];
                let mut parts = Vec::new();
                for value in field.numeric_values(doc) {
                    use svindb_core::index::{float_of, Kind};
                    let text = match field.kind {
                        Kind::Bool => {
                            if value == 0 {
                                "false".into()
                            } else {
                                "true".into()
                            }
                        }
                        Kind::Float | Kind::Double => float_of(value).to_string(),
                        _ => value.to_string(),
                    };
                    if field.kind.parse_value(&text) != Some(value) {
                        return Err(invalid("numeric value не имеет точного round-trip"));
                    }
                    parts.push(text);
                }
                match parts.len() {
                    0 => Vec::new(),
                    1 => parts.pop().unwrap().into_bytes(),
                    _ => svindb_core::indexer::encode_multi(&parts).into_bytes(),
                }
            };
            if raw.is_empty() {
                if source.fields[at].kind.keyword() {
                    return Err(invalid("пустой/missing keyword неоднозначен без source"));
                }
                continue;
            }
            if source.fields[at].kind.keyword()
                && raw.starts_with(svindb_core::indexer::MULTI_PREFIX.as_bytes())
            {
                return Err(invalid(
                    "legacy keyword с зарезервированным multi-prefix требует отдельного oracle",
                ));
            }
            payload =
                payload.checked_add(raw.len()).ok_or_else(|| invalid("stored bytes overflow"))?;
            if payload > 256 * 1024 * 1024 {
                return Err(invalid("stored payload больше 256 МиБ на сегмент"));
            }
            values.push((
                at,
                std::str::from_utf8(&raw).map_err(|_| invalid("stored не UTF-8"))?.to_owned(),
            ));
        }
        let cells: Vec<_> =
            values.iter().map(|(field, text)| Cell { field: *field, slot: 0, text }).collect();
        let id = source.idmap.id_at(doc).ok_or_else(|| invalid("исходный ID отсутствует"))?;
        ix.add_cells(&id, &[1], &cells);
    }
    for walk in walks.iter_mut().flatten() {
        if walk.next().is_some() || walk.broken() {
            return Err(invalid("stored поток содержит лишние/повреждённые данные"));
        }
    }
    ix.flush_now();
    if let Some(error) = ix.poisoned() {
        return Err(invalid(error));
    }
    if ix.segments() != 1 {
        return Err(invalid("encoded columns требуют одного сегмента"));
    }
    let index = svindb_core::index::load_read_only(&base.join("seg0000").to_string_lossy())?;
    if index.docs != source.docs
        || index.fields.iter().any(|f| f.legacy_multivalue || f.legacy_keyword_values)
    {
        return Err(invalid("невекторные колонки не переведены в текущий формат"));
    }
    if index.fields.len() != source.fields.len()
        || index
            .fields
            .iter()
            .zip(&source.fields)
            .any(|(a, b)| a.fid != b.fid || a.stem != b.stem || a.name != b.name)
    {
        return Err(invalid("identity полей изменилась при rebuild columns"));
    }
    for (at, field) in source.fields.iter().enumerate() {
        if field.kind.dims().is_some() {
            continue;
        }
        let Some(old) = field.store.as_ref() else {
            for doc in 0..source.docs as u32 {
                if field.numeric_values(doc).collect::<Vec<_>>()
                    != index.fields[at].numeric_values(doc).collect::<Vec<_>>()
                {
                    return Err(invalid("сохранённые numeric значения изменились"));
                }
            }
            continue;
        };
        let new = index
            .fields
            .get(at)
            .and_then(|f| f.store.as_ref())
            .ok_or_else(|| invalid("stored исчез"))?;
        let (mut a, mut b) = (old.walk(), new.walk());
        for doc in 0..source.docs as u32 {
            match (a.next(), b.next()) {
                (Some((ad, av)), Some((bd, bv))) if ad == doc && bd == doc && av == bv => {}
                _ => return Err(invalid("stored значение изменилось при rebuild columns")),
            }
        }
        if a.next().is_some() || b.next().is_some() || a.broken() || b.broken() {
            return Err(invalid("stored хвост повреждён"));
        }
    }
    for doc in 0..source.docs as u32 {
        if source.idmap.id_at(doc) != index.idmap.id_at(doc) {
            return Err(invalid("ID order изменён"));
        }
    }
    Ok(index)
}

/// Перепаковывает один packed source-disabled сегмент в layout 13.
pub fn rebuild(
    source_index: &Path,
    segment: &Path,
    output: &Path,
) -> io::Result<serde_json::Value> {
    if output.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("каталог вывода уже существует: {}", output.display()),
        ));
    }
    if source_index.is_symlink() || segment.is_symlink() || output.is_symlink() {
        return Err(invalid("symlink запрещён"));
    }
    validate_members(segment)?;
    let report =
        svindb_core::check::check(&segment.to_string_lossy(), svindb_core::check::Depth::Full);
    if !report.ok() || report.findings.iter().any(|finding| finding.check == "лишний файл")
    {
        return Err(invalid("исходный сегмент повреждён либо содержит неизвестные файлы"));
    }
    let source = svindb_core::index::load_read_only(&segment.to_string_lossy())?;
    let head = svindb_core::index::read_head(&segment.to_string_lossy())?;
    if head.layout > svindb_core::index::LAYOUT {
        return Err(invalid("будущая раскладка сегмента не поддерживается"));
    }
    if head.container.is_none() {
        return Err(invalid("поддерживаются только packed-сегменты"));
    }
    fs::create_dir(output)?;
    let columns = rebuild_columns(&source, output)?;
    let destination = output.join("seg0");
    fs::create_dir(&destination)?;
    let mut writer = svindb_core::container::Writer::create(
        &destination.to_string_lossy(),
        svindb_core::index::LAYOUT,
    )?;
    let mut replaced = std::collections::BTreeSet::new();
    let mut vectors = 0u64;
    for field in &source.fields {
        let Some(codec) = field.vectors.as_ref() else { continue };
        let q8 = format!("{}.q8", field.stem);
        let qm = format!("{}.qm", field.stem);
        let present = (0..codec.docs() as u32).filter(|&doc| codec.code(doc).is_some()).count();
        replaced.insert(q8.clone());
        replaced.insert(qm.clone());
        if present == 0 {
            // Пустое поле нового формата не требует q8/qm payload.
            continue;
        }
        writer.begin(&q8)?;
        let meta = codec.compact_with_presence(
            &|doc| codec.code(doc).is_some(),
            &mut MemberWriter(&mut writer),
        )?;
        if let Some(meta) = meta {
            writer.end()?;
            writer.put(&qm, &meta)?;
            vectors += present as u64;
        } else {
            return Err(invalid("непустой vector payload не перекодирован"));
        }
    }
    let table = svindb_core::container::Table::read(&format!("{}/seg.dat", columns.dir))?;
    let mut copied = 0u64;
    for member in table.members() {
        if member.name == "head.tsv"
            || member.name == "live.bin"
            || member.name == "sums.tsv"
            || member.name == "seg.dat"
            || replaced.contains(&member.name)
        {
            continue;
        }
        copied += copy_member(&columns, &mut writer, &member.name)?;
    }
    // Приблизительный индекс копируется без обучения и изменения кластеров.
    for field in &source.fields {
        if field.vectors.is_none() {
            continue;
        }
        for extension in ["ivf", "ivfb"] {
            let name = format!("{}.{}", field.stem, extension);
            if source.src.try_open(&name).is_some() {
                copied += copy_member(&source, &mut writer, &name)?;
            }
        }
    }
    let container_len = writer.finish()?;
    if segment.join("live.bin").exists() {
        copy_file(&segment.join("live.bin"), &destination.join("live.bin"))?;
    }
    let old_head = fs::read_to_string(Path::new(&columns.dir).join("head.tsv"))?;
    let mut body = String::new();
    for line in old_head.lines() {
        if !line.starts_with("layout\t") && !line.starts_with("container\t") {
            body.push_str(line);
            body.push('\n');
        }
    }
    let mut head_file =
        OpenOptions::new().write(true).create_new(true).open(destination.join("head.tsv"))?;
    write!(head_file, "layout\t13\ncontainer\tseg.dat\t{container_len}\n{body}")?;
    head_file.sync_all()?;
    svindb_core::sums::write(&destination.to_string_lossy())?;
    let verified = svindb_core::index::load_read_only(&destination.to_string_lossy())?;
    let check =
        svindb_core::check::check(&destination.to_string_lossy(), svindb_core::check::Depth::Full);
    if !check.ok() {
        return Err(invalid("полная проверка нового сегмента завершилась ошибкой"));
    }
    if source.docs != verified.docs {
        return Err(invalid("число документов изменилось"));
    }
    if segment.join("live.bin").exists()
        && fs::read(segment.join("live.bin"))? != fs::read(destination.join("live.bin"))?
    {
        return Err(invalid("карта удалений изменилась"));
    }
    for field in &source.fields {
        let Some(old) = field.vectors.as_ref() else { continue };
        let fresh =
            verified.field_of(&field.name).and_then(|i| verified.fields[i].vectors.as_ref());
        let Some(fresh) = fresh else {
            if (0..old.docs() as u32).any(|doc| old.code(doc).is_some()) {
                return Err(invalid(format!("непустое vector field исчезло: {}", field.name)));
            }
            continue;
        };
        if old.docs() != fresh.docs() || old.dims() != fresh.dims() {
            return Err(invalid("размер vector изменился"));
        }
        for dim in 0..old.dims() {
            if old.lo(dim).to_bits() != fresh.lo(dim).to_bits()
                || old.step(dim).to_bits() != fresh.step(dim).to_bits()
            {
                return Err(invalid("шкала vector изменилась"));
            }
        }
        for doc in 0..old.docs() as u32 {
            if old.code(doc) != fresh.code(doc) {
                return Err(invalid("коды vector изменились"));
            }
        }
        // Нормы стоят последним массивом f32 как в legacy, так и в versioned qm.
        let name = format!("{}.qm", field.stem);
        let before = source.src.open(&name)?;
        let after = verified.src.open(&name)?;
        let bytes = old.docs().checked_mul(4).ok_or_else(|| invalid("размер норм переполнен"))?;
        if before.len_bytes() < bytes
            || after.len_bytes() < bytes
            || before.slice(before.len_bytes() - bytes, bytes)
                != after.slice(after.len_bytes() - bytes, bytes)
        {
            return Err(invalid("нормы vector изменились"));
        }
    }
    for member in table.members() {
        let name = &member.name;
        if name == "head.tsv"
            || name == "live.bin"
            || name == "sums.tsv"
            || name == "seg.dat"
            || replaced.contains(name)
        {
            continue;
        }
        let before = columns.src.open(name)?;
        let after = verified.src.open(name)?;
        if before.slice(0, before.len_bytes()) != after.slice(0, after.len_bytes()) {
            return Err(invalid(format!("member изменился: {name}")));
        }
    }
    for field in &source.fields {
        if field.vectors.is_none() {
            continue;
        }
        for extension in ["ivf", "ivfb"] {
            let name = format!("{}.{}", field.stem, extension);
            if let Some(before) = source.src.try_open(&name) {
                let after = verified.src.open(&name)?;
                if before.slice(0, before.len_bytes()) != after.slice(0, after.len_bytes()) {
                    return Err(invalid("IVF изменён при перепаковке"));
                }
            }
        }
    }
    File::open(&destination)?.sync_all()?;
    File::open(output)?.sync_all()?;
    Ok(
        serde_json::json!({"documents": source.docs, "vectors_present": vectors, "copied_bytes": copied, "layout": 13}),
    )
}
