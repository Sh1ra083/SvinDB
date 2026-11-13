//! Разреженные числовые массивы поверх scalar column.
//!
//! Обычное поле с одним значением не платит за offsets. Только единицы с двумя
//! и более значениями имеют запись (unit,start); значения упакованы тем же
//! блочным integer codec, что scalar column. Повторы сохраняются для value
//! count/sum, а порядок исходного JSON остаётся договором authoritative source.

use crate::column::{BlockCache, Column};
use crate::mmap::Region;

const MAGIC: &[u8; 8] = b"SVNUMV01";
const HEADER: usize = 32;
const ENTRY: usize = 8;

#[derive(Default)]
pub struct Builder {
    entries: Vec<(usize, usize)>,
    values: Vec<i64>,
}

/// Keyword ordinals становятся известны после сортировки словаря. Собираем
/// только многозначные единицы, без Vec/HashMap на каждый документ.
#[derive(Default)]
pub(crate) struct OrdinalValues {
    extras: Vec<(u32, i64)>,
    multiple: Vec<u64>,
}

impl OrdinalValues {
    pub fn record(
        &mut self,
        columns: &mut [i64],
        present: &mut [bool],
        unit: usize,
        ordinal: i64,
    ) -> std::io::Result<()> {
        if unit >= columns.len() || unit >= present.len() || unit > u32::MAX as usize || ordinal < 0
        {
            return Err(bad("keyword ordinal вне unit space"));
        }
        if present[unit] && columns[unit] != ordinal {
            if self.multiple.is_empty() {
                self.multiple.resize(columns.len().div_ceil(64), 0);
            }
            let mask = 1u64 << (unit & 63);
            if self.multiple[unit >> 6] & mask == 0 {
                self.extras.push((unit as u32, columns[unit]));
                self.multiple[unit >> 6] |= mask;
            }
            self.extras.push((unit as u32, ordinal));
        }
        columns[unit] = ordinal;
        present[unit] = true;
        Ok(())
    }

    pub fn finish(self) -> Builder {
        let Self { mut extras, multiple } = self;
        drop(multiple);
        extras.sort_unstable();
        // Keyword doc values имеют set semantics, в отличие от numeric arrays.
        extras.dedup();
        let mut out = Builder::default();
        let mut at = 0;
        while at < extras.len() {
            let unit = extras[at].0;
            let mut end = at + 1;
            while end < extras.len() && extras[end].0 == unit {
                end += 1;
            }
            if end - at > 1 {
                out.entries.push((unit as usize, out.values.len()));
                out.values.extend(extras[at..end].iter().map(|(_, value)| *value));
            }
            at = end;
        }
        out
    }
}

impl Builder {
    /// Возвращает добавленные логические bytes для memory admission.
    pub fn add(&mut self, unit: usize, values: &[i64]) -> usize {
        if values.len() <= 1 {
            return 0;
        }
        self.entries.push((unit, self.values.len()));
        self.values.extend_from_slice(values);
        std::mem::size_of::<(usize, usize)>() + std::mem::size_of_val(values)
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn append(&mut self, other: &mut Self, unit_base: usize) {
        let value_base = self.values.len();
        self.entries.extend(other.entries.drain(..).map(|(u, v)| (u + unit_base, v + value_base)));
        self.values.append(&mut other.values);
    }

    pub fn entries(&self) -> usize {
        self.entries.len()
    }

    pub fn values(&self) -> usize {
        self.values.len()
    }

    /// Суффикс единиц с локальной нумерацией; префикс values не копируется.
    pub fn suffix(&self, first_unit: usize) -> Self {
        let at = self.entries.partition_point(|(unit, _)| *unit < first_unit);
        let Some(&(_, start)) = self.entries.get(at) else {
            return Self::default();
        };
        Self {
            entries: self.entries[at..]
                .iter()
                .map(|&(unit, offset)| (unit - first_unit, offset - start))
                .collect(),
            values: self.values[start..].to_vec(),
        }
    }

    pub fn bounds(&self) -> Option<(i64, i64)> {
        Some((*self.values.iter().min()?, *self.values.iter().max()?))
    }

    pub fn encode(&self, units: usize) -> std::io::Result<Option<Vec<u8>>> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        if units > u32::MAX as usize
            || self.values.len() > u32::MAX as usize
            || self.entries.len() > u32::MAX as usize
        {
            return Err(bad("число единиц/значений не помещается в формат u32"));
        }
        // Смещения packed-блоков u32. Проверка до encode/выделения памяти:
        // иначе большой incompressible массив завернул бы offsets writer.
        if self
            .values
            .len()
            .saturating_mul(8)
            .saturating_add(self.values.len().div_ceil(crate::column::BLOCK))
            > u32::MAX as usize
        {
            return Err(bad(
                "typed stream превышает безопасный u32 bound; уменьшите размер сегмента",
            ));
        }
        let mut previous = None;
        for &(unit, start) in &self.entries {
            if unit >= units
                || start >= self.values.len()
                || previous.is_some_and(|(u, s)| unit <= u || start <= s)
            {
                return Err(bad("несогласованные offsets числовых массивов"));
            }
            previous = Some((unit, start));
        }
        let (data, meta) = crate::column::encode(&self.values, &vec![true; self.values.len()]);
        let metadata_bytes =
            u32::try_from(meta.len()).map_err(|_| bad("слишком большая metadata"))?;
        let mut out =
            Vec::with_capacity(HEADER + self.entries.len() * ENTRY + meta.len() + data.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(units as u32).to_le_bytes());
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.values.len() as u32).to_le_bytes());
        out.extend_from_slice(&metadata_bytes.to_le_bytes());
        out.extend_from_slice(&(data.len() as u64).to_le_bytes());
        for &(unit, start) in &self.entries {
            out.extend_from_slice(&(unit as u32).to_le_bytes());
            out.extend_from_slice(&(start as u32).to_le_bytes());
        }
        out.extend_from_slice(&meta);
        out.extend_from_slice(&data);
        Ok(Some(out))
    }
}

fn bad(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("numeric multivalue: {message}"))
}

pub struct MultiColumn {
    map: Region,
    entries: usize,
    values: Column,
}

impl MultiColumn {
    pub fn open(raw: Region, units: usize, entries: usize, values: usize) -> std::io::Result<Self> {
        if raw.len_bytes() < HEADER
            || raw.slice(0, 8) != MAGIC
            || raw.u32_at(8) as usize != units
            || raw.u32_at(12) as usize != entries
            || raw.u32_at(16) as usize != values
            || entries == 0
            || values == 0
        {
            return Err(bad("заголовок не соответствует сегменту"));
        }
        let map_bytes = entries.checked_mul(ENTRY).ok_or_else(|| bad("переполнение map"))?;
        let meta_bytes = raw.u32_at(20) as usize;
        let data_bytes = usize::try_from(raw.u64_at(24)).map_err(|_| bad("переполнение data"))?;
        let meta_start = HEADER.checked_add(map_bytes).ok_or_else(|| bad("переполнение map"))?;
        let data_start =
            meta_start.checked_add(meta_bytes).ok_or_else(|| bad("переполнение metadata"))?;
        if data_start.checked_add(data_bytes) != Some(raw.len_bytes()) {
            return Err(bad("длины частей не совпали"));
        }
        let map = raw.sub(HEADER, map_bytes);
        let mut previous_unit = None;
        for i in 0..entries {
            let unit = map.u32_at(i * ENTRY) as usize;
            let start = map.u32_at(i * ENTRY + 4) as usize;
            let end =
                if i + 1 == entries { values } else { map.u32_at((i + 1) * ENTRY + 4) as usize };
            if unit >= units
                || previous_unit.is_some_and(|u| unit <= u)
                || (i == 0 && start != 0)
                || end > values
                || end.saturating_sub(start) < 2
            {
                return Err(bad("невалидные единицы или offsets"));
            }
            previous_unit = Some(unit);
        }
        let values = Column::from_dense_parts(
            raw.sub(data_start, data_bytes),
            raw.sub(meta_start, meta_bytes),
            values,
        )
        .ok_or_else(|| bad("повреждён packed integer stream"))?;
        Ok(Self { map, entries, values })
    }

    pub fn get(&self, unit: u32) -> Option<Values<'_>> {
        let mut lo = 0usize;
        let mut hi = self.entries;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.map.u32_at(mid * ENTRY).cmp(&unit) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    return Some(self.values_at(mid));
                }
            }
        }
        None
    }

    fn values_at(&self, entry: usize) -> Values<'_> {
        let start = self.map.u32_at(entry * ENTRY + 4);
        let end = if entry + 1 == self.entries {
            self.values.docs() as u32
        } else {
            self.map.u32_at((entry + 1) * ENTRY + 4)
        };
        Values { column: &self.values, at: start, end, cache: BlockCache::default() }
    }

    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Дополняет bitmap scalar range: одна единица попадает в результат один
    /// раз независимо от числа совпавших значений. Память ограничена окном.
    pub fn select_window(
        &self,
        lo: i64,
        hi: i64,
        units: std::ops::Range<usize>,
        out: &mut [u64],
        guard: Option<&crate::work::Guard>,
    ) -> usize {
        assert!(
            units.start <= units.end && out.len() >= (units.end - units.start).div_ceil(64),
            "bitmap не покрывает окно единиц"
        );
        let mut left = 0;
        let mut right = self.entries;
        while left < right {
            let mid = left + (right - left) / 2;
            if (self.map.u32_at(mid * ENTRY) as usize) < units.start {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        let mut found = 0;
        let mut tick = 0u32;
        for entry in left..self.entries {
            let unit = self.map.u32_at(entry * ENTRY) as usize;
            if unit >= units.end {
                break;
            }
            for value in self.values_at(entry) {
                if guard.is_some_and(|g| !g.go(&mut tick)) {
                    return found;
                }
                if lo <= value && value <= hi {
                    let relative = unit - units.start;
                    let mask = 1u64 << (relative % 64);
                    let bits = &mut out[relative / 64];
                    found += usize::from(*bits & mask == 0);
                    *bits |= mask;
                    break;
                }
            }
        }
        found
    }

    pub fn bytes(&self) -> usize {
        self.map.len_bytes() + self.values.bytes() + HEADER
    }
}

pub struct Values<'a> {
    column: &'a Column,
    at: u32,
    end: u32,
    cache: BlockCache,
}

pub enum NumericValues<'a> {
    Multi(Values<'a>),
    Scalar(std::option::IntoIter<i64>),
}

impl Iterator for NumericValues<'_> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        match self {
            Self::Multi(values) => values.next(),
            Self::Scalar(value) => value.next(),
        }
    }
}

impl Iterator for Values<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        if self.at >= self.end {
            return None;
        }
        let value = self.column.get_with(&mut self.cache, self.at);
        self.at += 1;
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_arrays_preserve_sets_across_live_flush_and_merge() {
        use crate::aggs::{Agg, Kind, Over, Space, TermKey, Value};
        use crate::segments::{QuerySpec, Segments};
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("svindb-keyword-multi-{}", std::process::id()));
        let root = dir.to_string_lossy().to_string();
        let mut writer = crate::Indexer::typed_stored(
            &root,
            vec!["tags".into()],
            vec![1.0],
            vec![crate::index::Kind::KeywordExact],
            vec![false],
            1000,
        );
        let agg = [Agg {
            name: "tags".into(),
            kind: Kind::Terms { field: 0, size: 20 },
            over: Over::Docs,
            space: Space::Keyword,
        }];
        let first = crate::indexer::encode_multi(&["red".into(), "blue".into(), "red".into()]);
        writer.add("a", &[&first]);
        let live = Segments::from_parts(
            vec![writer.build_live("keyword-live").unwrap().unwrap()],
            vec![0],
        );
        let initial = live.aggregate(&QuerySpec::default(), &agg);
        assert_eq!(
            initial[0].value,
            Value::Terms {
                top: vec![(TermKey::Text("blue".into()), 1), (TermKey::Text("red".into()), 1)],
                error: 0,
                other: 0,
            }
        );
        writer.flush_now();
        writer.add("b", &["blue"]);
        writer.flush_now();
        drop(writer);
        let segments = Segments::load(&root).unwrap();
        let before = segments.aggregate(&QuerySpec::default(), &agg);
        assert_eq!(
            before[0].value,
            Value::Terms {
                top: vec![(TermKey::Text("blue".into()), 2), (TermKey::Text("red".into()), 1)],
                error: 0,
                other: 0,
            }
        );
        let output = dir.join("merged").to_string_lossy().to_string();
        crate::merge::merge(&segments.list.iter().map(AsRef::as_ref).collect::<Vec<_>>(), &output)
            .unwrap();
        let merged =
            Segments::from_parts(vec![Arc::new(crate::index::load(&output).unwrap())], vec![0]);
        assert_eq!(before[0].value, merged.aggregate(&QuerySpec::default(), &agg)[0].value);
        for kind in [Kind::CardinalityExact { field: 0 }, Kind::Cardinality { field: 0 }] {
            let cardinality =
                [Agg { name: "unique".into(), kind, over: Over::Docs, space: Space::Keyword }];
            for view in [&segments, &merged] {
                let result = view.aggregate(&QuerySpec::default(), &cardinality);
                assert!(
                    matches!(
                        result[0].value,
                        Value::ExactCardinality { value: 2 } | Value::Cardinality { value: 2 }
                    ),
                    "{:?}",
                    result[0].value
                );
            }
        }
        let checked = crate::check::check(&output, crate::check::Depth::Full);
        assert!(checked.ok(), "{:?}", checked.findings.iter().map(|f| &f.text).collect::<Vec<_>>());
        drop(merged);
        drop(segments);
        drop(live);
        // Старый header разрешает чтение, но не доказывает сохранность массивов
        // и старого ordinal remap. Отказ должен предшествовать созданию output.
        let scalar = format!("{root}/seg0001");
        crate::container::unpack(&scalar).unwrap();
        let header = format!("{scalar}/head.tsv");
        let text = std::fs::read_to_string(&header).unwrap();
        let old = text.replace(&format!("layout\t{}\n", crate::index::LAYOUT), "layout\t11\n");
        assert_ne!(text, old);
        std::fs::write(&header, old).unwrap();
        crate::sums::write(&scalar).unwrap();
        let legacy = Arc::new(crate::index::load(&scalar).unwrap());
        assert!(legacy.fields[0].legacy_keyword_values);
        let legacy_view = Segments::from_parts(vec![Arc::clone(&legacy)], vec![0]);
        assert!(legacy_view.numeric_compatibility(&QuerySpec::default(), &[]).is_ok());
        assert!(legacy_view.numeric_compatibility(&QuerySpec::default(), &agg).is_err());
        let blocked = dir.join("blocked-keyword-merge");
        assert!(crate::merge::merge(&[&legacy], &blocked.to_string_lossy()).is_err());
        assert!(!blocked.exists());
        drop((legacy_view, legacy));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn keyword_slots_expand_large_frequencies_without_shifting_the_next_document() {
        use crate::indexer::Cell;
        let dir = std::env::temp_dir().join(format!("svindb-keyword-slots-{}", std::process::id()));
        let mut writer = crate::Indexer::typed_stored(
            &dir.to_string_lossy(),
            vec!["tag".into()],
            vec![1.0],
            vec![crate::index::Kind::KeywordExact],
            vec![false],
            1000,
        );
        writer.set_groups(vec![1], vec![String::new(), "items".into()]);
        let values = crate::indexer::encode_multi(&["red".into(), "blue".into()]);
        let cells: Vec<_> = (0..300).map(|slot| Cell { field: 0, slot, text: &values }).collect();
        writer.add_cells("a", &[1, 300], &cells);
        writer.add_cells("b", &[1, 1], &[Cell { field: 0, slot: 0, text: "red" }]);
        let live = writer.build_live("keyword-slots-live").unwrap().unwrap();
        let field = &live.fields[0];
        let multi = field.multi_column.as_ref().unwrap();
        for unit in 0..300 {
            assert_eq!(multi.get(unit).unwrap().count(), 2);
        }
        assert!(multi.get(300).is_none());
        assert!(field.column.as_ref().unwrap().get(300).is_some());
        drop(live);
        drop(writer);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sparse_arrays_preserve_duplicates_and_full_integer_width() {
        let mut builder = Builder::default();
        assert_eq!(builder.add(0, &[42]), 0);
        let rows = [
            (1, vec![1, 1, 2]),
            (10, vec![i64::MIN, i64::MAX]),
            (129, (0..300).map(|n| n * n - 40000).collect()),
        ];
        for (unit, values) in &rows {
            builder.add(*unit, values);
        }
        let bytes = builder.encode(200).unwrap().unwrap();
        let column = MultiColumn::open(Region::of_bytes(&bytes), 200, 3, 305).unwrap();
        let mut bits = [0u64; 2];
        assert_eq!(column.select_window(i64::MIN, i64::MAX, 1..129, &mut bits, None), 2);
        assert_eq!(bits[0], 1 | (1 << 9));
        assert_eq!(bits[1], 0);
        assert_eq!(
            column.select_window(i64::MIN, i64::MAX, 1..129, &mut bits, None),
            0,
            "повторные значения/порции не должны задваивать документы"
        );
        let cancelled = crate::work::Guard::new(1_000);
        cancelled.cancel();
        let mut cancelled_bits = [0u64; 4];
        assert_eq!(
            column.select_window(i64::MIN, i64::MAX, 0..200, &mut cancelled_bits, Some(&cancelled)),
            0
        );
        assert_eq!(cancelled_bits, [0; 4]);
        assert!(column.get(0).is_none());
        assert!(column.get(130).is_none());
        for (unit, expected) in rows {
            assert_eq!(column.get(unit as u32).unwrap().collect::<Vec<_>>(), expected);
        }
        for end in [0, 7, HEADER, bytes.len() - 1] {
            assert!(MultiColumn::open(Region::of_bytes(&bytes[..end]), 200, 3, 305).is_err());
        }
        let mut bad_width = bytes.clone();
        let data_at =
            HEADER + 3 * ENTRY + u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
        bad_width[data_at] = 65;
        assert!(MultiColumn::open(Region::of_bytes(&bad_width), 200, 3, 305).is_err());
        let mut bad_offset = bytes.clone();
        bad_offset[HEADER + 4..HEADER + 8].copy_from_slice(&1u32.to_le_bytes());
        assert!(MultiColumn::open(Region::of_bytes(&bad_offset), 200, 3, 305).is_err());
        let mut duplicate_unit = bytes.clone();
        duplicate_unit[HEADER + ENTRY..HEADER + ENTRY + 4].copy_from_slice(&1u32.to_le_bytes());
        assert!(MultiColumn::open(Region::of_bytes(&duplicate_unit), 200, 3, 305).is_err());
    }

    #[test]
    fn fragments_shift_units_and_value_offsets() {
        let mut first = Builder::default();
        first.add(1, &[10, 20]);
        let mut second = Builder::default();
        second.add(0, &[30, 40, 30]);
        first.append(&mut second, 3);
        assert_eq!(second.entries(), 0);
        let bytes = first.encode(5).unwrap().unwrap();
        let column = MultiColumn::open(Region::of_bytes(&bytes), 5, 2, 5).unwrap();
        assert_eq!(column.get(3).unwrap().collect::<Vec<_>>(), [30, 40, 30]);
        assert_eq!(first.bounds(), Some((10, 40)));
        first.clear();
        assert!(first.encode(0).unwrap().is_none());
    }

    #[test]
    fn numeric_arrays_survive_live_flush_delete_and_merge_without_stored_fields() {
        use crate::aggs::{Agg, Kind as AggKind, Over, Space, Value};
        use crate::segments::{QuerySpec, Segments, Sort, SpecRange};
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("svindb-numeric-multi-{}", std::process::id()));
        let root = dir.to_string_lossy().to_string();
        let rows = vec![("a", vec![1, 1, 2]), ("b", vec![3]), ("c", vec![4, 5]), ("d", vec![])];
        let verify = |segments: &Segments, expected: &[(&str, Vec<i64>)]| {
            let ids = |spec: &QuerySpec| -> Vec<String> {
                segments
                    .search(spec)
                    .hits
                    .iter()
                    .map(|hit| {
                        let ix = &segments.list[hit.segment];
                        crate::ids::Ids::open(&ix.src, ix.docs).get(hit.local as usize).unwrap()
                    })
                    .collect()
            };
            let spec = QuerySpec {
                k: 20,
                filter: vec![SpecRange { fid: 0, from: 2, to: 4 }],
                ..QuerySpec::default()
            };
            let mut found = ids(&spec);
            found.sort();
            let want: Vec<_> = expected
                .iter()
                .filter(|(_, vs)| vs.iter().any(|v| (2..=4).contains(v)))
                .map(|(id, _)| id.to_string())
                .collect();
            assert_eq!(found, want, "range потерял массив");
            let values: Vec<i64> = expected.iter().flat_map(|(_, vs)| vs.iter().copied()).collect();
            let aggregates = [
                Agg {
                    name: "values".into(),
                    kind: AggKind::Stats { field: Some(0) },
                    over: Over::Docs,
                    space: Space::Int,
                },
                Agg {
                    name: "docs".into(),
                    kind: AggKind::Stats { field: None },
                    over: Over::Docs,
                    space: Space::Int,
                },
                Agg {
                    name: "terms".into(),
                    kind: AggKind::Terms { field: 0, size: 20 },
                    over: Over::Docs,
                    space: Space::Int,
                },
            ];
            let counted = segments.aggregate(&QuerySpec::default(), &aggregates);
            assert_eq!(
                counted[0].value,
                Value::Stats {
                    count: values.len() as u64,
                    min: values.iter().min().copied(),
                    max: values.iter().max().copied(),
                    sum: values.iter().map(|&v| v as i128).sum(),
                    fsum: None
                }
            );
            assert!(
                matches!(counted[1].value, Value::Stats { count, .. } if count == expected.len() as u64)
            );
            if let Value::Terms { top, .. } = &counted[2].value {
                for (key, count) in top {
                    assert_eq!(
                        *count,
                        expected
                            .iter()
                            .filter(|(_, vs)| vs.contains(&key.as_num().unwrap()))
                            .count() as u64
                    );
                }
                let unique: std::collections::HashSet<_> = values.iter().collect();
                assert_eq!(top.len(), unique.len());
            } else {
                panic!("неверный тип terms");
            }
            for desc in [false, true] {
                let mut ordered: Vec<_> = expected.iter().collect();
                ordered.sort_by(|a, b| {
                    let key = |vs: &[i64]| {
                        if desc {
                            vs.iter().max().copied()
                        } else {
                            vs.iter().min().copied()
                        }
                    };
                    match (key(&a.1), key(&b.1)) {
                        (Some(a), Some(b)) => {
                            if desc {
                                b.cmp(&a)
                            } else {
                                a.cmp(&b)
                            }
                        }
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (Some(_), None) => std::cmp::Ordering::Less,
                        _ => a.0.cmp(b.0),
                    }
                });
                let spec =
                    QuerySpec { k: 20, sort: vec![Sort { fid: 0, desc }], ..QuerySpec::default() };
                assert_eq!(
                    ids(&spec),
                    ordered.iter().map(|(id, _)| id.to_string()).collect::<Vec<_>>()
                );
            }
        };
        let mut indexer = crate::Indexer::typed_stored(
            &root,
            vec!["nums".into()],
            vec![1.0],
            vec![crate::index::Kind::Int64],
            vec![false],
            1_000,
        );
        for (i, (id, values)) in rows.iter().enumerate() {
            let text = crate::indexer::encode_multi(
                &values.iter().map(ToString::to_string).collect::<Vec<_>>(),
            );
            indexer.add(id, &[&text]);
            if i == 1 {
                let live = indexer.build_live("numeric-live").unwrap().unwrap();
                verify(&Segments::from_parts(vec![live], vec![0]), &rows[..2]);
                indexer.flush_now();
            }
        }
        indexer.flush_now();
        drop(indexer);
        let segments = Segments::load(&root).unwrap();
        let checked = crate::check::check(&root, crate::check::Depth::Full);
        assert!(
            checked.ok(),
            "{}",
            checked.findings.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("; ")
        );
        verify(&segments, &rows);
        let frozen = Arc::new(segments.list.iter().map(|ix| Arc::new(ix.live_copy())).collect());
        assert!(segments.list[0].live.as_ref().unwrap().kill(0));
        let pit_count = segments.aggregate(
            &QuerySpec { live: Some(frozen), ..QuerySpec::default() },
            &[Agg {
                name: "docs".into(),
                kind: AggKind::Stats { field: None },
                over: Over::Docs,
                space: Space::Int,
            }],
        );
        assert!(
            matches!(pit_count[0].value, Value::Stats { count: 4, .. }),
            "PIT count прочёл текущий live bitmap"
        );
        verify(&segments, &rows[1..]);
        let merged_dir = dir.join("merged").to_string_lossy().to_string();
        crate::merge::merge(
            &segments.list.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
            &merged_dir,
        )
        .unwrap();
        let merged =
            Segments::from_parts(vec![Arc::new(crate::index::load(&merged_dir).unwrap())], vec![0]);
        let checked = crate::check::check(&merged_dir, crate::check::Depth::Full);
        assert!(
            checked.ok(),
            "{}",
            checked.findings.iter().map(|f| f.text.clone()).collect::<Vec<_>>().join("; ")
        );
        verify(&merged, &rows[1..]);
        let scalar_rows = [("a", vec![1]), ("b", vec![]), ("c", vec![3]), ("d", vec![])];
        let scalar_dir = dir.join("scalar").to_string_lossy().to_string();
        let mut scalar = crate::Indexer::typed(
            &scalar_dir,
            vec!["nums".into()],
            vec![1.0],
            vec![crate::index::Kind::Int64],
            1_000,
        );
        for (id, vs) in &scalar_rows {
            scalar.add(id, &[&vs.first().map_or(String::new(), ToString::to_string)]);
        }
        let live = scalar.build_live("scalar-with-missing").unwrap().unwrap();
        verify(&Segments::from_parts(vec![live], vec![0]), &scalar_rows);
        scalar.flush_now();
        drop(scalar);
        // Старый scalar stream не различает missing и потерянный массив.
        // Экспорт по source остаётся возможным, numeric operators/merge —
        // явный migration gate, а не успешный неполный результат.
        let legacy_dir = format!("{scalar_dir}/seg0000");
        crate::container::unpack(&legacy_dir).unwrap();
        let head_path = format!("{legacy_dir}/head.tsv");
        let head = std::fs::read_to_string(&head_path).unwrap();
        let legacy_head =
            head.replace(&format!("layout\t{}\n", crate::index::LAYOUT), "layout\t9\n");
        assert_ne!(legacy_head, head, "fixture не переведён в legacy layout");
        std::fs::write(&head_path, legacy_head).unwrap();
        crate::sums::write(&legacy_dir).unwrap();
        let legacy = Arc::new(crate::index::load(&legacy_dir).unwrap());
        assert!(legacy.fields[0].legacy_multivalue);
        let old = Segments::from_parts(vec![Arc::clone(&legacy)], vec![0]);
        assert!(old.numeric_compatibility(&QuerySpec::default(), &[]).is_ok());
        let range = QuerySpec {
            filter: vec![SpecRange { fid: 0, from: 1, to: 2 }],
            ..QuerySpec::default()
        };
        assert!(old.numeric_compatibility(&range, &[]).is_err());
        let blocked = dir.join("blocked-merge").to_string_lossy().to_string();
        assert!(crate::merge::merge(&[&legacy], &blocked).is_err());
        assert!(!std::path::Path::new(&blocked).exists());
        drop((old, legacy));
        drop((segments, merged));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
