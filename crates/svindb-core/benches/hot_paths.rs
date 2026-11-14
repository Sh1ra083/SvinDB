//! Deterministic fixtures are built once, outside measured iterations.
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use svindb_core::aggs::{Agg, Kind as AggKind, Over, Space};
use svindb_core::index::Kind;
use svindb_core::segments::{SpecRange, TrackTotalHits};
use svindb_core::{Indexer, QuerySpec, Segments};

struct Fixture(PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn hot_paths(c: &mut Criterion) {
    let mut crc = c.benchmark_group("wal_crc32");
    for size in [1024, 64 * 1024] {
        let bytes = vec![0xa5; size];
        crc.throughput(Throughput::Bytes(size as u64));
        crc.bench_with_input(BenchmarkId::from_parameter(size), &bytes, |b, bytes| {
            b.iter(|| svindb_core::wal::crc32(black_box(bytes)))
        });
    }
    crc.finish();

    let fixture = Fixture(std::env::temp_dir().join(format!(
        "svindb-hot-paths-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )));
    let dir = fixture.0.to_str().unwrap();
    std::fs::create_dir_all(dir).unwrap();
    let mut indexer = Indexer::typed(
        dir,
        vec!["value".into(), "author".into()],
        vec![1.0; 2],
        vec![Kind::Int64, Kind::Keyword],
        100_000,
    );
    for doc in 0..100_000 {
        indexer.add(&format!("d{doc}"), &[&doc.to_string(), &format!("user{:05}", doc % 50_000)]);
    }
    indexer.flush_now();
    drop(indexer);
    let segments = Segments::load(dir).unwrap();
    let range = QuerySpec {
        k: 0,
        filter: vec![SpecRange { fid: 0, from: 25_000, to: 74_999 }],
        track_total_hits: TrackTotalHits::Exact,
        live: Some(Arc::new(segments.list.iter().map(|ix| Arc::new(ix.live_copy())).collect())),
        ..QuerySpec::default()
    };
    assert!(range.live.as_ref().unwrap().iter().all(|live| live.all_live()));
    assert!(segments.list.iter().all(|ix| ix.fields[0].multi_column.is_none()));
    let (counted, count) = segments.search_count_with_g(&range, 1, None);
    assert_eq!(count, 50_000);
    assert!(counted.visited <= 256, "range benchmark must use covered-block counts");
    c.bench_function("range_count/100k_half", |b| {
        b.iter(|| segments.search_count_with_g(black_box(&range), 1, None))
    });
    let query = QuerySpec { k: 0, ..QuerySpec::default() };
    let aggs = [Agg {
        name: "authors".into(),
        kind: AggKind::Terms { field: 1, size: 10 },
        over: Over::Docs,
        space: Space::Keyword,
    }];
    let result = segments.aggregate(&query, &aggs);
    match &result[0].value {
        svindb_core::aggs::Value::Terms { top, error, other } => {
            assert_eq!(top.len(), 10);
            assert!(top.iter().all(|(_, count)| *count == 2));
            assert_eq!(*error, 0);
            assert_eq!(*other, 99_980);
        }
        value => panic!("unexpected keyword terms result: {value:?}"),
    }
    c.bench_function("keyword_terms/100k_50k_distinct", |b| {
        b.iter(|| segments.aggregate(black_box(&query), black_box(&aggs)))
    });
    drop(segments);
    drop(fixture);
}

criterion_group!(benches, hot_paths);
criterion_main!(benches);
