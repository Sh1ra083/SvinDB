use svindb_core::container;
use svindb_core::vector::{self, Built, Vectors};

fn open(codes: &[u8], meta: &[u8]) -> Vectors {
    let src = container::in_memory("test", [("v.q8", codes), ("v.qm", meta)]);
    Vectors::open(&src, "v").expect("валидный вектор должен открыться")
}

fn legacy_from_new(b: &Built, dims: usize, docs: usize) -> (Vec<u8>, Vec<u8>) {
    // В legacy-кодах строка есть для каждого документа; пропущенную строку
    // оставляем нулевой, а норму помечаем старым признаком f32::MAX.
    let bitmap_len = docs.div_ceil(8);
    let rank_len = docs / 256 + 1;
    let values_at = 20 + bitmap_len + rank_len * 4;
    let norms_at = values_at + dims * 8;
    let mut codes = vec![0u8; docs * dims];
    let mut meta = Vec::with_capacity(8 + dims * 8 + docs * 4);
    meta.extend((dims as u32).to_le_bytes());
    meta.extend((docs as u32).to_le_bytes());
    meta.extend(&b.meta[values_at..values_at + dims * 8]);
    let norms = &b.meta[norms_at..norms_at + docs * 4];
    let mut rank = 0;
    for d in 0..docs {
        let yes = b.meta[20 + d / 8] & (1 << (d % 8)) != 0;
        if yes {
            codes[d * dims..(d + 1) * dims]
                .copy_from_slice(&b.codes[rank * dims..(rank + 1) * dims]);
            rank += 1;
        }
        meta.extend(&norms[d * 4..d * 4 + 4]);
    }
    assert_eq!(norms_at, values_at + dims * 8);
    (codes, meta)
}

#[test]
fn sparse_layout_is_compact_at_1025_documents_and_handles_missing_queries() {
    let (n, dims) = (1025, 8);
    let data: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            if i % 20 != 1 {
                vec![]
            } else {
                (0..dims).map(|j| i as f32 + j as f32 / 10.0).collect()
            }
        })
        .collect();
    let built = vector::build(&data, dims).expect("есть заполненные строки");
    let present = data.iter().filter(|v| v.len() == dims).count();
    assert_eq!(&built.meta[..4], b"SVQ1");
    assert_eq!(built.meta[4], 1);
    assert_eq!(built.meta[5], 1, "включён sparse q8");
    assert_eq!(u32::from_le_bytes(built.meta[12..16].try_into().unwrap()) as usize, n);
    assert_eq!(u32::from_le_bytes(built.meta[16..20].try_into().unwrap()) as usize, present);
    assert_eq!(built.codes.len(), present * dims);
    assert_eq!(built.codes.capacity(), present * dims);
    assert!(built.codes.len() < n * dims);
    let vs = open(&built.codes, &built.meta);
    assert_eq!(vs.docs(), n);
    assert_eq!(vs.code(0), None);
    assert_eq!(vs.code(1).unwrap().len(), dims);
    assert!(vs
        .top(&data[1], n + 10, &|_| true)
        .iter()
        .all(|(d, _)| data[*d as usize].len() == dims));
    // Граница первого пропущенного документа и k больше числа присутствующих
    // не должны ни паниковать, ни выдавать phantom-документы.
    let first_missing = data.iter().position(|v| v.is_empty()).unwrap() as u32;
    assert!(vs
        .top_range(&data[1], n, &|_| true, first_missing, n as u32)
        .iter()
        .all(|(d, _)| vs.code(*d).is_some()));
}

#[test]
fn sparse_and_equivalent_legacy_have_identical_exact_results_and_restore() {
    let dims = 4;
    let data: Vec<Vec<f32>> = (0..300)
        .map(|i| {
            if i == 0 || i == 257 {
                vec![]
            } else {
                (0..dims).map(|j| (i * 3 + j) as f32).collect()
            }
        })
        .collect();
    let b = vector::build(&data, dims).unwrap();
    let sparse = open(&b.codes, &b.meta);
    let (legacy_codes, legacy_meta) = legacy_from_new(&b, dims, data.len());
    let legacy = open(&legacy_codes, &legacy_meta);
    assert!(!legacy.uses_versioned_format());
    for q in [1usize, 42, 256, 299] {
        for k in [1, 20, 400] {
            let a = sparse.top(&data[q], k, &|_| true);
            let z = legacy.top(&data[q], k, &|_| true);
            assert_eq!(a, z, "q={q}, k={k}");
        }
    }
    let mut out = Vec::new();
    assert!(!sparse.restore(0, &mut out));
    assert!(sparse.restore(1, &mut out));
    assert_eq!(out.len(), dims);
    assert_eq!(sparse.code(257), None);
    assert_eq!(legacy.code(257), None);
}

#[test]
fn merge_and_compact_preserve_presence_across_repeated_merges_and_deletes() {
    let dims = 3;
    let a: Vec<Vec<f32>> =
        (0..270).map(|i| if i % 7 == 0 { vec![] } else { vec![i as f32, 1.0, 2.0] }).collect();
    let b: Vec<Vec<f32>> = (0..190)
        .map(|i| if i % 11 == 0 { vec![] } else { vec![1000.0 + i as f32, 3.0, 4.0] })
        .collect();
    let ba = vector::build(&a, dims).unwrap();
    let bb = vector::build(&b, dims).unwrap();
    let va = open(&ba.codes, &ba.meta);
    let vb = open(&bb.codes, &bb.meta);
    let keep = |part: usize, doc: u32| {
        !(part == 0 && doc.is_multiple_of(13)) && !(part == 1 && doc.is_multiple_of(17))
    };
    let mut codes = Vec::new();
    let meta =
        vector::merge(&[(Some(&va), a.len()), (Some(&vb), b.len())], dims, &keep, &mut codes)
            .unwrap()
            .unwrap();
    let first = open(&codes, &meta);
    assert_eq!(
        first.docs(),
        a.len() + b.len()
            - (0..a.len()).filter(|d| d % 13 == 0).count()
            - (0..b.len()).filter(|d| d % 17 == 0).count()
    );
    let mut current = open(&codes, &meta);
    for _ in 0..3 {
        let mut next_codes = Vec::new();
        let next_meta = vector::merge(
            &[(Some(&current), current.docs())],
            dims,
            &|_, d| d % 19 != 0,
            &mut next_codes,
        )
        .unwrap()
        .unwrap();
        let next = open(&next_codes, &next_meta);
        assert_eq!(
            next.docs(),
            current.docs() - (0..current.docs()).filter(|d| d % 19 == 0).count()
        );
        assert!(next
            .top(&[1.0, 1.0, 2.0], next.docs() + 1, &|_| true)
            .iter()
            .all(|(d, _)| next.code(*d).is_some()));
        current = next;
    }
    let mut compact_codes = Vec::new();
    let compact_meta = first
        .compact_with_presence(&|d| d % 5 != 0 && first.code(d).is_some(), &mut compact_codes)
        .unwrap()
        .unwrap();
    let compact = open(&compact_codes, &compact_meta);
    assert_eq!(
        compact.docs(),
        first.docs(),
        "compact меняет размещение кодов, но сохраняет номера документов"
    );
    let compact_present =
        (0..first.docs()).filter(|d| d % 5 != 0 && first.code(*d as u32).is_some()).count();
    assert_eq!(compact_codes.len(), compact_present * dims);
    for d in 0..compact.docs() as u32 {
        if d % 5 == 0 || first.code(d).is_none() {
            assert_eq!(compact.code(d), None);
            continue;
        }
        assert!(compact.code(d).is_some());
    }
}

#[test]
fn malformed_versioned_metadata_and_legacy_sizes_are_rejected() {
    let data: Vec<Vec<f32>> = (0..20).map(|i| vec![i as f32, 2.0]).collect();
    let b = vector::build(&data, 2).unwrap();
    let valid = |meta: Vec<u8>, codes: Vec<u8>| {
        Vectors::open(&container::in_memory("x", [("v.q8", codes), ("v.qm", meta)]), "v").is_some()
    };
    for (at, value) in [(4, 2u8), (5, 2u8)] {
        let mut m = b.meta.clone();
        m[at] = value;
        assert!(!valid(m, b.codes.clone()));
    }
    let mut m = b.meta.clone();
    m[20 + 20 / 8] |= 0x80;
    assert!(!valid(m, b.codes.clone()));
    let mut m = b.meta.clone();
    m[23..27].copy_from_slice(&1u32.to_le_bytes());
    assert!(!valid(m, b.codes.clone()));
    let mut m = b.meta.clone();
    let norms_at = 20 + data.len().div_ceil(8) + (data.len() / 256 + 1) * 4 + 2 * 8;
    m[norms_at..norms_at + 4].copy_from_slice(&f32::NAN.to_bits().to_le_bytes());
    assert!(!valid(m, b.codes.clone()));
    let mut m = b.meta.clone();
    m.truncate(m.len() - 1);
    assert!(!valid(m, b.codes.clone()));
    assert!(!valid(vec![2, 0, 0, 0, 20, 0, 0, 0], b.codes.clone()));
    assert!(vector::build(&vec![vec![]; 12], 2).is_none());
    assert!(vector::merge(&[(None, 12)], 2, &|_, _| true, &mut Vec::new()).unwrap().is_none());
}

#[test]
fn overflow_is_an_explicit_error_not_a_successful_unreadable_segment() {
    assert!(vector::build_checked(&[vec![-1e20], vec![1e20]], 1).is_err());
    assert!(vector::build_checked(&[vec![-f32::MAX], vec![f32::MAX]], 1).is_err());
    assert!(vector::build_checked(&[vec![f32::NAN]], 1).is_err());
    let a = vector::build_checked(&[vec![-1e20]], 1).unwrap().unwrap();
    let b = vector::build_checked(&[vec![1e20]], 1).unwrap().unwrap();
    let va = open(&a.codes, &a.meta);
    let vb = open(&b.codes, &b.meta);
    assert!(
        vector::merge(&[(Some(&va), 1), (Some(&vb), 1)], 1, &|_, _| true, &mut Vec::new()).is_err()
    );
}

#[test]
fn legacy_finite_phantom_is_repaired_only_by_external_presence_compaction() {
    let data: Vec<Vec<f32>> = (0..16).map(|i| vec![i as f32, 10.0]).collect();
    let b = vector::build(&data, 2).unwrap();
    let (codes, meta) = legacy_from_new(&b, 2, data.len());
    let mut phantom_meta = meta.clone();
    // Старый файл с конечной нормой у всех строк выглядит полностью заполненным.
    for d in [3usize, 9] {
        let at = 8 + 2 * 8 + d * 4;
        phantom_meta[at..at + 4].copy_from_slice(&1.0f32.to_bits().to_le_bytes());
    }
    let legacy = open(&codes, &phantom_meta);
    assert!(legacy.code(3).is_some());
    let original_code = legacy.code(4).unwrap().to_vec();
    let original_lo = legacy.lo(0);
    let original_step = legacy.step(0);
    let mut compact_codes = Vec::new();
    let compact_meta =
        legacy.compact_with_presence(&|d| d != 3 && d != 9, &mut compact_codes).unwrap().unwrap();
    let compact = open(&compact_codes, &compact_meta);
    assert_eq!(compact.docs(), data.len());
    assert_eq!(compact.lo(0), original_lo);
    assert_eq!(compact.step(0), original_step);
    assert_eq!(compact.code(3), None);
    assert_eq!(compact.code(4), Some(original_code.as_slice()));
}
