//! Временное отображение ordinal исходного словаря в ordinal результата merge.
//!
//! Поток строится одновременно со словарём, поэтому не требует HashMap на
//! миллионы строк или повторного dictionary lookup на каждый документ.
//! В RAM — только ограниченный write buffer; готовая таблица читается через
//! mmap и удаляется до публикации сегмента. Ноль означает отсутствующее слово.

use std::fs::File;
use std::io::{BufWriter, Write};

pub(crate) const BUFFER_BUDGET: usize = 64 << 10;

fn bad(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

pub(crate) struct Writer {
    output: Option<BufWriter<File>>,
    path: String,
    expected: usize,
    written: usize,
}

impl Writer {
    pub fn create(
        directory: &str,
        source: usize,
        terms: usize,
        buffer_bytes: usize,
    ) -> std::io::Result<Self> {
        let path = format!("{directory}/__ordinal-map-{source}.tmp");
        let file = File::create_new(&path)?;
        Ok(Self {
            output: Some(BufWriter::with_capacity(buffer_bytes.max(4), file)),
            path,
            expected: terms,
            written: 0,
        })
    }

    pub fn push(&mut self, old: u32, new: Option<u32>) -> std::io::Result<()> {
        if old as usize != self.written || self.written >= self.expected {
            return Err(bad("ordinal map: исходный словарь нарушил последовательность"));
        }
        let encoded = new
            .map(|ordinal| {
                ordinal
                    .checked_add(1)
                    .ok_or_else(|| bad("ordinal map: переполнение нового ordinal"))
            })
            .transpose()?
            .unwrap_or(0);
        if self.written.is_multiple_of(16_384)
            && !crate::work::compaction_checkpoint(
                self.expected.saturating_sub(self.written).min(16_384) as u64 * 4,
            )
        {
            return Err(bad("ordinal map: compaction остановлен при shutdown"));
        }
        self.output.as_mut().expect("открытый ordinal writer").write_all(&encoded.to_le_bytes())?;
        self.written += 1;
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<Reader> {
        if self.written != self.expected {
            return Err(bad("ordinal map: исходный словарь прочитан не полностью"));
        }
        let mut output = self.output.take().expect("открытый ordinal writer");
        if !crate::work::compaction_checkpoint(0) {
            return Err(bad("ordinal map: compaction остановлен при shutdown"));
        }
        output.flush()?;
        drop(output);
        let map = crate::mmap::Mapped::open(&self.path)?;
        if map.len_bytes()
            != self.expected.checked_mul(4).ok_or_else(|| bad("ordinal map: размер"))?
        {
            return Err(bad("ordinal map: неверная длина файла"));
        }
        Ok(Reader { map: Some(map), path: std::mem::take(&mut self.path), terms: self.expected })
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        drop(self.output.take());
        if !self.path.is_empty() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub(crate) struct Reader {
    map: Option<crate::mmap::Mapped>,
    path: String,
    terms: usize,
}

impl Reader {
    pub fn get(&self, old: i64, field_range: Option<(u32, u32)>) -> std::io::Result<i64> {
        let old = usize::try_from(old).map_err(|_| bad("ordinal map: отрицательное значение"))?;
        if old >= self.terms {
            return Err(bad("ordinal map: значение вне исходного словаря"));
        }
        let encoded = self.map.as_ref().expect("открытый ordinal map").u32_at(old * 4);
        let new = encoded
            .checked_sub(1)
            .ok_or_else(|| bad("ordinal map: слово отсутствует в результате"))?;
        if !field_range.is_some_and(|(from, upto)| from <= new && new < upto) {
            return Err(bad("ordinal map: значение относится к другому полю"));
        }
        Ok(i64::from(new))
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        // На Windows отображённый файл может удерживать дескриптор: сначала
        // закрывается mapping, потом удаляется именно наш временный файл.
        drop(self.map.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mappings_reject_missing_foreign_and_incomplete_values_and_clean_up() {
        let root = std::env::temp_dir().join(format!("svindb-ordmap-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let root = root.to_string_lossy();
        let mut writer = Writer::create(&root, 0, 3, 8).unwrap();
        let path = writer.path.clone();
        writer.push(0, Some(7)).unwrap();
        writer.push(1, None).unwrap();
        writer.push(2, Some(9)).unwrap();
        let reader = writer.finish().unwrap();
        assert_eq!(reader.get(0, Some((7, 10))).unwrap(), 7);
        assert_eq!(reader.get(2, Some((7, 10))).unwrap(), 9);
        assert!(reader.get(1, Some((0, 10))).is_err());
        assert!(reader.get(0, Some((8, 10))).is_err());
        assert!(reader.get(-1, Some((0, 10))).is_err());
        assert!(reader.get(3, Some((0, 10))).is_err());
        drop(reader);
        assert!(!std::path::Path::new(&path).exists());
        let mut incomplete = Writer::create(&root, 1, 2, 8).unwrap();
        let path = incomplete.path.clone();
        assert!(incomplete.push(1, None).is_err());
        incomplete.push(0, None).unwrap();
        assert!(incomplete.finish().is_err());
        assert!(!std::path::Path::new(&path).exists());
        std::fs::remove_dir(&*root).unwrap();
    }
}
