//! Внешняя по памяти сборка каталога идентификаторов.
//!
//! В памяти одновременно находится только один сегментный Index и не более
//! memory_entries записей одного run. Остальные записи живут в приватном
//! временном каталоге и сливаются с веером не более 32 файлов.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const ENTRY: usize = 16;
const FAN_IN: usize = 32;
const MAGIC: &[u8; 8] = b"svindbc2";

type Record = (u64, u32, u32);

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn write_record<W: Write>(w: &mut W, r: Record) -> io::Result<()> {
    w.write_all(&r.0.to_le_bytes())?;
    w.write_all(&r.1.to_le_bytes())?;
    w.write_all(&r.2.to_le_bytes())
}

fn read_record<R: Read>(r: &mut R) -> io::Result<Option<Record>> {
    let mut b = [0u8; ENTRY];
    let mut got = 0;
    while got < ENTRY {
        let n = r.read(&mut b[got..])?;
        if n == 0 {
            if got == 0 {
                return Ok(None);
            }
            return Err(invalid("обрыв внешнего run-файла"));
        }
        got += n;
    }
    Ok(Some((
        u64::from_le_bytes(b[0..8].try_into().unwrap()),
        u32::from_le_bytes(b[8..12].try_into().unwrap()),
        u32::from_le_bytes(b[12..16].try_into().unwrap()),
    )))
}

fn unique_workdir(root: &Path) -> io::Result<PathBuf> {
    let pid = std::process::id();
    let tick = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    for n in 0..1000u32 {
        let p = root.join(format!(".catalog-build-{pid}-{tick}-{n}"));
        match fs::DirBuilder::new().mode(0o700).create(&p) {
            Ok(()) => return Ok(p),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "не удалось создать приватный workdir каталога",
    ))
}

fn merge_group(inputs: &[PathBuf], output: &Path) -> io::Result<u64> {
    let mut files: Vec<BufReader<File>> = inputs
        .iter()
        .map(|p| File::open(p).map(|f| BufReader::with_capacity(1 << 20, f)))
        .collect::<io::Result<_>>()?;
    let mut heap = BinaryHeap::new();
    for (i, f) in files.iter_mut().enumerate() {
        if let Some(r) = read_record(f)? {
            heap.push(Reverse((r, i)));
        }
    }
    let file = OpenOptions::new().write(true).create_new(true).open(output)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let mut count = 0u64;
    while let Some(Reverse((r, i))) = heap.pop() {
        write_record(&mut out, r)?;
        count = count.checked_add(1).ok_or_else(|| invalid("слишком много записей каталога"))?;
        if let Some(next) = read_record(&mut files[i])? {
            heap.push(Reverse((next, i)));
        }
    }
    let file = out.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;
    Ok(count)
}

/// Онлайн-накопитель runs с ограниченным числом файлов на каждом уровне.
///
/// После появления FAN_IN файлов одного уровня они немедленно сливаются в
/// один файл следующего уровня. Поэтому даже при memory_entries == 1 число
/// открытых метаданных остаётся логарифмически ограниченным числом записей.
struct RunAccumulator {
    work: PathBuf,
    fan_in: usize,
    levels: Vec<Vec<PathBuf>>,
    serial: u64,
}

impl RunAccumulator {
    fn new(work: &Path) -> Self {
        Self { work: work.to_path_buf(), fan_in: FAN_IN, levels: Vec::new(), serial: 0 }
    }

    #[cfg(test)]
    fn with_fan_in(work: &Path, fan_in: usize) -> Self {
        assert!(fan_in > 1);
        Self { work: work.to_path_buf(), fan_in, levels: Vec::new(), serial: 0 }
    }

    fn add(&mut self, path: PathBuf) -> io::Result<()> {
        self.add_at(0, path)
    }

    fn add_at(&mut self, level: usize, path: PathBuf) -> io::Result<()> {
        if self.levels.len() <= level {
            self.levels.resize_with(level + 1, Vec::new);
        }
        self.levels[level].push(path);
        if self.levels[level].len() < self.fan_in {
            return Ok(());
        }

        let inputs: Vec<_> = self.levels[level].drain(..self.fan_in).collect();
        let output = self.work.join(format!("level-{level:06}-{:016}.run", self.serial));
        self.serial =
            self.serial.checked_add(1).ok_or_else(|| invalid("слишком много runs каталога"))?;
        merge_group(&inputs, &output)?;
        for input in inputs {
            fs::remove_file(input)?;
        }
        self.add_at(level + 1, output)
    }

    fn into_runs(self) -> Vec<PathBuf> {
        self.levels.into_iter().flatten().collect()
    }
}

fn merge_all(mut runs: Vec<PathBuf>, work: &Path) -> io::Result<(PathBuf, u64)> {
    let mut generation = 0usize;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, inputs) in runs.chunks(FAN_IN).enumerate() {
            let output = work.join(format!("merge-{generation:06}-{group:06}.run"));
            merge_group(inputs, &output)?;
            next.push(output);
        }
        for p in runs {
            fs::remove_file(p)?;
        }
        runs = next;
        generation += 1;
    }
    let count = if let Some(path) = runs.first() {
        let len = fs::metadata(path)?.len();
        if len % ENTRY as u64 != 0 {
            return Err(invalid("внешний run-файл имеет некратную длину"));
        }
        len / ENTRY as u64
    } else {
        0
    };
    if runs.is_empty() {
        let empty = work.join("empty.run");
        File::create(&empty)?.sync_all()?;
        Ok((empty, count))
    } else {
        Ok((runs.pop().unwrap(), count))
    }
}

fn current_manifest(dir: &str) -> io::Result<Vec<String>> {
    crate::manifest::segments_checked(dir).map_err(io::Error::other)
}

/// Проверяет опубликованный во временном workdir каталог последовательным
/// чтением. Записи каталога отсортированы по хешу, поэтому для точной проверки
/// физических адресов они сначала перекладываются во внешние runs, где ключом
/// становится `(segment_no, doc_no)`. В памяти остаётся только один bounded
/// run; ни один байт большого каталога не читается случайным mmap-доступом.
fn verify_catalog(
    path: &Path,
    segments: &[String],
    idents: &[(u64, i64)],
    work: &Path,
    memory_entries: usize,
    expected_count: u64,
    expected_stamp: u64,
) -> io::Result<()> {
    let verify_work = work.join("verification");
    fs::create_dir(&verify_work)?;
    eprintln!("catalog verification: sequential sort start ({expected_count} records)");
    let metadata = fs::metadata(path)?;
    let count_usize = usize::try_from(expected_count)
        .map_err(|_| invalid("слишком много записей каталога для этой платформы"))?;
    let expected_len = 32usize
        .checked_add(
            count_usize
                .checked_mul(ENTRY)
                .ok_or_else(|| invalid("переполнение размера каталога"))?,
        )
        .ok_or_else(|| invalid("переполнение размера каталога"))?;
    if metadata.len() != expected_len as u64 {
        return Err(invalid("размер итогового каталога не совпадает с заголовком"));
    }

    let mut input = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut magic = [0u8; 8];
    input.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid("неверная подпись итогового каталога"));
    }
    let mut word = [0u8; 8];
    input.read_exact(&mut word)?;
    if u64::from_le_bytes(word) != expected_count {
        return Err(invalid("число записей итогового каталога изменилось"));
    }
    input.read_exact(&mut word)?;
    if u64::from_le_bytes(word) != expected_stamp {
        return Err(invalid("подпись состава итогового каталога изменилась"));
    }
    input.read_exact(&mut word)?;
    if u64::from_le_bytes(word) != segments.len() as u64 {
        return Err(invalid("число сегментов итогового каталога изменилось"));
    }

    let mut run_set = RunAccumulator::new(&verify_work);
    let mut records = Vec::with_capacity(memory_entries);
    let mut previous = None;
    for _ in 0..expected_count {
        let (hash, segment, doc) =
            read_record(&mut input)?.ok_or_else(|| invalid("обрыв записей итогового каталога"))?;
        let current = (hash, segment, doc);
        if previous.is_some_and(|p| p > current) {
            return Err(invalid("итоговый каталог не отсортирован"));
        }
        previous = Some(current);
        if segment as usize >= segments.len() {
            return Err(invalid("каталог содержит неизвестный номер сегмента"));
        }
        // Адрес сортируется целиком первым полем. Два u32 сохраняют весь
        // u64-хеш без изменения существующего бинарного формата Record.
        let address = ((segment as u64) << 32) | doc as u64;
        records.push((address, hash as u32, (hash >> 32) as u32));
        if records.len() == memory_entries {
            records.sort_unstable();
            let run = verify_work.join(format!("verify-run-{:016}.run", run_set.serial));
            let file = OpenOptions::new().write(true).create_new(true).open(&run)?;
            let mut output = BufWriter::with_capacity(1 << 20, file);
            for record in records.drain(..) {
                write_record(&mut output, record)?;
            }
            let file = output.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
            run_set.serial = run_set
                .serial
                .checked_add(1)
                .ok_or_else(|| invalid("слишком много runs проверки каталога"))?;
            run_set.add(run)?;
        }
    }
    if input.read(&mut [0u8; 1])? != 0 {
        return Err(invalid("итоговый каталог содержит лишние данные"));
    }
    if !records.is_empty() {
        records.sort_unstable();
        let run = verify_work.join(format!("verify-run-{:016}.run", run_set.serial));
        let file = OpenOptions::new().write(true).create_new(true).open(&run)?;
        let mut output = BufWriter::with_capacity(1 << 20, file);
        for record in records.drain(..) {
            write_record(&mut output, record)?;
        }
        let file = output.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        run_set.serial = run_set
            .serial
            .checked_add(1)
            .ok_or_else(|| invalid("слишком много runs проверки каталога"))?;
        run_set.add(run)?;
    }
    drop(records);
    let (sorted, sorted_count) = merge_all(run_set.into_runs(), &verify_work)?;
    eprintln!("catalog verification: sequential sort complete ({sorted_count} records)");
    if sorted_count != expected_count {
        return Err(invalid("число записей проверки каталога разошлось"));
    }

    let mut sorted_input = BufReader::with_capacity(1 << 20, File::open(sorted)?);
    let mut previous = None;
    let mut checked = 0u64;
    for (segment_no, segment) in segments.iter().enumerate() {
        let ix = crate::index::load_read_only(segment)?;
        let ids = crate::ids::Ids::open(&ix.src, ix.docs);
        let mut walk = ids.walk();
        let mut doc = 0u32;
        while let Some(id) = walk.next() {
            if doc as usize >= ix.docs {
                return Err(invalid("карта ID изменилась при проверке каталога"));
            }
            let hash = crate::bloom::hash64(id);
            let address = ((segment_no as u64) << 32) | doc as u64;
            let wanted = (address, hash as u32, (hash >> 32) as u32);
            let got = read_record(&mut sorted_input)?
                .ok_or_else(|| invalid("каталог потерял физический адрес"))?;
            if previous.is_some_and(|p| p == got) {
                return Err(invalid("каталог содержит дубликат физического адреса"));
            }
            if got != wanted {
                return Err(invalid("выходной каталог не совпал с физическими ID"));
            }
            previous = Some(got);
            checked += 1;
            doc += 1;
            if checked.is_multiple_of(1_000_000) {
                eprintln!("catalog verification: compared {checked}/{expected_count} records");
            }
        }
        if doc as usize != ix.docs {
            return Err(invalid("исходные ID изменились"));
        }
    }
    if checked != expected_count || read_record(&mut sorted_input)?.is_some() {
        return Err(invalid("каталог содержит лишние или отсутствующие записи"));
    }
    // `idents` передаётся сюда намеренно: stamp проверяется до сравнения
    // данных, а длина состава — отдельным условием для защиты от усечения.
    if crate::catalog::stamp(idents) != expected_stamp {
        return Err(invalid("исходная подпись состава изменилась"));
    }
    eprintln!("catalog verification: complete ({checked} records)");
    Ok(())
}

/// Собирает catalog.bin внешним слиянием, не занимая память всем корпусом.
/// memory_entries — жёсткий верхний предел числа троек в сортируемом run.
pub fn build(dir: &str, memory_entries: usize) -> io::Result<u64> {
    if memory_entries == 0 {
        return Err(invalid("memory_entries должен быть больше нуля"));
    }
    let root = Path::new(dir);
    let segments = current_manifest(dir)?;
    let work = unique_workdir(root)?;
    let result = (|| {
        let mut run_set = RunAccumulator::new(&work);
        let mut idents = Vec::with_capacity(segments.len());
        let mut records = Vec::with_capacity(memory_entries);
        let mut flush = |records: &mut Vec<Record>| -> io::Result<()> {
            if records.is_empty() {
                return Ok(());
            }
            records.sort_unstable();
            let path = work.join(format!("run-{:016}.run", run_set.serial));
            let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
            let mut f = BufWriter::with_capacity(1 << 20, file);
            for &r in records.iter() {
                write_record(&mut f, r)?;
            }
            let file = f.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
            run_set.serial = run_set
                .serial
                .checked_add(1)
                .ok_or_else(|| invalid("слишком много runs каталога"))?;
            run_set.add(path)?;
            records.clear();
            Ok(())
        };
        for (seg_no, segment) in segments.iter().enumerate() {
            let seg_no = u32::try_from(seg_no)
                .map_err(|_| invalid("слишком много сегментов для segment_no"))?;
            let ix = crate::index::load_read_only(segment)?;
            idents.push(ix.head_id);
            if ix.docs > u32::MAX as usize {
                return Err(invalid(format!("{segment}: слишком много документов для u32")));
            }
            let ids = crate::ids::Ids::open(&ix.src, ix.docs);
            if ix.docs > 0 && (ix.idmap.is_empty() || ids.is_empty()) {
                return Err(invalid(format!(
                    "{segment}: отсутствует физическая карта идентификаторов для непустого сегмента"
                )));
            }
            let mut callback_error = None;
            let mut walk = ids.walk();
            let mut doc = 0u32;
            while let Some(id) = walk.next() {
                if doc as usize >= ix.docs {
                    return Err(invalid(format!("{segment}: карта ID длиннее числа документов")));
                }
                let h = crate::bloom::hash64(id);
                if callback_error.is_some() {
                    break;
                }
                records.push((h, seg_no, doc));
                doc += 1;
                if records.len() == memory_entries {
                    if let Err(e) = flush(&mut records) {
                        callback_error = Some(e);
                    }
                }
            }
            if let Some(e) = callback_error {
                return Err(e);
            }
            if ix.docs > 0 && doc != ix.docs as u32 {
                return Err(invalid(format!("{segment}: idmap не содержит физических ID")));
            }
        }
        flush(&mut records)?;
        let (merged, count) = merge_all(run_set.into_runs(), &work)?;
        let stamp = crate::catalog::stamp(&idents);
        let final_tmp = work.join("catalog.bin");
        let file = OpenOptions::new().write(true).create_new(true).open(&final_tmp)?;
        let mut out = BufWriter::with_capacity(1 << 20, file);
        out.write_all(MAGIC)?;
        out.write_all(&count.to_le_bytes())?;
        out.write_all(&stamp.to_le_bytes())?;
        out.write_all(&(segments.len() as u64).to_le_bytes())?;
        let mut input = BufReader::with_capacity(1 << 20, File::open(&merged)?);
        let mut previous = None;
        let mut checked = 0u64;
        while let Some(r) = read_record(&mut input)? {
            if previous.is_some_and(|p| p > r) {
                return Err(invalid("итоговый каталог не отсортирован"));
            }
            previous = Some(r);
            write_record(&mut out, r)?;
            checked += 1;
        }
        if checked != count {
            return Err(invalid("число записей каталога разошлось при проверке"));
        }
        let file = out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(records);
        // Count и сортировка недостаточны: подтверждаем каждый физический
        // адрес последовательным внешним проходом с bounded-памятью.
        verify_catalog(&final_tmp, &segments, &idents, &work, memory_entries, count, stamp)?;
        if current_manifest(dir)? != segments {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "manifest изменился во время сборки каталога",
            ));
        }
        for (segment, ident) in segments.iter().zip(idents.iter()) {
            if crate::index::head_ident(segment) != *ident {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    format!("заголовок сегмента изменился: {segment}"),
                ));
            }
        }
        fs::rename(&final_tmp, root.join(crate::catalog::FILE))?;
        File::open(root)?.sync_all()?;
        Ok(count)
    })();
    let cleanup = fs::remove_dir_all(&work);
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let p = std::env::temp_dir()
            .join(format!("svindb-catalog-external-{name}-{}", std::process::id()));
        fs::remove_dir_all(&p).ok();
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn пустой_индекс_даёт_корректный_каталог() {
        let dir = temp("empty");
        let path = dir.to_string_lossy().to_string();
        assert_eq!(build(&path, 1).unwrap(), 0);
        let got = fs::read(dir.join(crate::catalog::FILE)).unwrap();
        let want = crate::catalog::encode(Vec::new(), crate::catalog::stamp(&[]));
        // Для пустого состава covered также равен нулю, поэтому формат совпадает
        // с каноническим кодировщиком побайтно.
        assert_eq!(got, want);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn нулевой_лимит_отвергается_до_создания_workdir() {
        let dir = temp("zero");
        let path = dir.to_string_lossy().to_string();
        assert_eq!(build(&path, 0).unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ошибка_manifest_не_затирает_старый_каталог() {
        let dir = temp("preserve");
        let old = "старый каталог".as_bytes();
        fs::write(dir.join(crate::catalog::FILE), old).unwrap();
        fs::write(dir.join(crate::manifest::FILE), "#svindb-manifest\nmissing-seg\n").unwrap();
        let path = dir.to_string_lossy().to_string();
        assert!(build(&path, 2).is_err());
        assert_eq!(fs::read(dir.join(crate::catalog::FILE)).unwrap(), old);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn external_multiple_passes_equal_canonical_catalog() {
        let dir = temp("multipass");
        let path = dir.to_string_lossy().to_string();
        let mut writer = crate::Indexer::new(&path, vec!["body".into()], vec![1.0], 17);
        for i in 0..101 {
            writer.add(&format!("id-{}", i % 73), &["одинаковый текст"]);
        }
        writer.flush_now();
        assert!(writer.poisoned().is_none());
        let names = crate::manifest::segments_checked(&path).unwrap();
        let indices: Vec<_> = names.iter().map(|p| crate::index::load(p).unwrap()).collect();
        let refs: Vec<_> = indices.iter().collect();
        let expected = crate::catalog::build(&refs);
        for entries in [1, 7, 1000] {
            assert_eq!(build(&path, entries).unwrap(), 101);
            assert_eq!(fs::read(dir.join(crate::catalog::FILE)).unwrap(), expected);
        }
        drop(indices);
        drop(writer);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn онлайн_слияние_сохраняет_segment_no_между_runs() {
        let dir = temp("cross-segment");
        let mut accumulator = RunAccumulator::with_fan_in(&dir, 2);
        let expected = vec![(7, 1, 0), (7, 2, 0), (9, 1, 1), (9, 2, 1)];
        for (n, record) in expected.iter().copied().enumerate() {
            let path = dir.join(format!("input-{n}.run"));
            let file = OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
            let mut writer = BufWriter::new(file);
            write_record(&mut writer, record).unwrap();
            writer.into_inner().unwrap().sync_all().unwrap();
            accumulator.add(path).unwrap();
        }
        let (merged, count) = merge_all(accumulator.into_runs(), &dir).unwrap();
        let mut reader = BufReader::new(File::open(merged).unwrap());
        let mut got = Vec::new();
        while let Some(record) = read_record(&mut reader).unwrap() {
            got.push(record);
        }
        assert_eq!(count, expected.len() as u64);
        assert_eq!(got, expected);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn онлайн_слияние_не_накапливает_runs_сверх_уровней() {
        let dir = temp("bounded-levels");
        let mut accumulator = RunAccumulator::with_fan_in(&dir, 2);
        for n in 0..17u64 {
            let path = dir.join(format!("input-{n}.run"));
            let file = OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
            let mut writer = BufWriter::new(file);
            write_record(&mut writer, (n, 0, n as u32)).unwrap();
            writer.into_inner().unwrap().sync_all().unwrap();
            accumulator.add(path).unwrap();
        }
        // Для fan-in=2 после 17 входов остаётся не более одного run на уровень;
        // это тот же инвариант, который в рабочем режиме задаёт fan-in=32.
        assert!(accumulator.levels.iter().all(|level| level.len() <= 1));
        let (merged, count) = merge_all(accumulator.into_runs(), &dir).unwrap();
        assert_eq!(count, 17);
        let mut reader = BufReader::new(File::open(merged).unwrap());
        for n in 0..17u64 {
            assert_eq!(read_record(&mut reader).unwrap(), Some((n, 0, n as u32)));
        }
        assert_eq!(read_record(&mut reader).unwrap(), None);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn каталог_проходит_прежний_предел_числа_runs() {
        let dir = temp("above-old-limit");
        let path = dir.to_string_lossy().to_string();
        let mut writer = crate::Indexer::new(&path, vec!["body".into()], vec![1.0], 20_000);
        for i in 0..16_417 {
            writer.add(&format!("id-{}", i % 16_000), &["текст"]);
        }
        writer.flush_now();
        assert!(writer.poisoned().is_none());
        let names = crate::manifest::segments_checked(&path).unwrap();
        let indices: Vec<_> = names.iter().map(|p| crate::index::load(p).unwrap()).collect();
        let refs: Vec<_> = indices.iter().collect();
        let expected = crate::catalog::build(&refs);
        // Прежняя реализация отказывала на 16 385-м run даже одного сегмента.
        assert_eq!(build(&path, 1).unwrap(), 16_417);
        assert_eq!(fs::read(dir.join(crate::catalog::FILE)).unwrap(), expected);
        drop(indices);
        drop(writer);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn последовательная_проверка_отвергает_порчу_и_неполный_набор_адресов() {
        let dir = temp("verify-corruption");
        let path = dir.to_string_lossy().to_string();
        let mut writer = crate::Indexer::new(&path, vec!["body".into()], vec![1.0], 11);
        for i in 0..43 {
            writer.add(&format!("id-{}", i % 17), &["проверка"]);
        }
        writer.flush_now();
        assert!(writer.poisoned().is_none());
        let names = crate::manifest::segments_checked(&path).unwrap();
        let indices: Vec<_> = names.iter().map(|p| crate::index::load(p).unwrap()).collect();
        let refs: Vec<_> = indices.iter().collect();
        let idents: Vec<_> = indices.iter().map(|i| i.head_id).collect();
        let stamp = crate::catalog::stamp(&idents);
        let good = crate::catalog::build(&refs);
        let mut reader = io::Cursor::new(&good[32..]);
        let mut records = Vec::new();
        while let Some(record) = read_record(&mut reader).unwrap() {
            records.push(record);
        }
        let encode = |mut rows: Vec<Record>| {
            rows.sort_unstable();
            let count = rows.len() as u64;
            let mut bytes = good[..32].to_vec();
            bytes[8..16].copy_from_slice(&count.to_le_bytes());
            for record in rows {
                write_record(&mut bytes, record).unwrap();
            }
            (bytes, count)
        };
        let mut cases = vec![("correct", good.clone(), 43, true)];
        for (label, offset) in [("magic", 0), ("count", 8), ("stamp", 16), ("covered", 24)] {
            let mut bytes = good.clone();
            bytes[offset] ^= 1;
            cases.push((label, bytes, 43, false));
        }
        let mut truncated = good.clone();
        truncated.pop();
        cases.push(("truncated", truncated, 43, false));
        let mut trailing = good.clone();
        trailing.push(0);
        cases.push(("trailing", trailing, 43, false));
        for label in [
            "wrong-hash",
            "high-hash",
            "wrong-doc",
            "wrong-segment",
            "missing",
            "duplicate",
            "extra",
        ] {
            let mut rows = records.clone();
            match label {
                "wrong-hash" => rows[0].0 ^= 1,
                "high-hash" => rows[0].0 ^= 1u64 << 63,
                "wrong-doc" => rows[0].2 = u32::MAX,
                "wrong-segment" => rows[0].1 = names.len() as u32,
                "missing" => {
                    rows.remove(0);
                }
                "duplicate" => rows[1] = rows[0],
                "extra" => rows.push(rows[0]),
                _ => unreachable!(),
            }
            // Размер, count и порядок для этих вариантов корректны: отказ
            // должен обеспечить именно точное сравнение с исходными ID.
            let (bytes, count) = encode(rows);
            cases.push((label, bytes, count, false));
        }
        let mut unsorted = good.clone();
        let first = unsorted[32..48].to_vec();
        let last_at = unsorted.len() - ENTRY;
        let last = unsorted[last_at..].to_vec();
        unsorted[32..48].copy_from_slice(&last);
        unsorted[last_at..].copy_from_slice(&first);
        cases.push(("unsorted", unsorted, 43, false));
        for (label, bytes, count, valid) in cases {
            let work = dir.join(label);
            fs::create_dir(&work).unwrap();
            let candidate = work.join("candidate.bin");
            fs::write(&candidate, bytes).unwrap();
            let result = verify_catalog(&candidate, &names, &idents, &work, 3, count, stamp);
            assert_eq!(result.is_ok(), valid, "{label}: {result:?}");
        }
        drop(indices);
        drop(writer);
        fs::remove_dir_all(dir).unwrap();
    }
}
