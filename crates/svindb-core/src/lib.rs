//! SvinDB — ядро полнотекстового поиска.

pub mod aggs;
pub mod alloc;

/// В тестовой или отдельной профилировочной сборке — считающий распределитель.
///
/// Объявлен здесь, потому что `#[global_allocator]` бывает один на весь
/// сборочный объект. В выпуске его нет вовсе: ни ветви, ни счётчика, ни
/// потоко-местной ячейки.
#[cfg(any(test, feature = "alloc-stats"))]
#[global_allocator]
static COUNTING: alloc::Counting = alloc::Counting;
pub mod analyze;
pub mod bits;
pub mod bloom;
pub mod budget;
pub mod catalog;
pub mod catalog_external;
pub mod check;
pub mod cluster;
pub mod clusterstore;
pub mod codec;
pub mod column;
pub mod container;
pub mod dict;
pub mod executor;
pub mod fail;
pub mod fs;
pub mod geo;
pub mod govern;
pub mod hash;
pub mod highlight;
pub mod ids;
pub mod index;
pub mod indexer;
pub mod inspect;
pub mod iso;
pub mod json;
pub mod layout;
pub mod live;
pub mod maintain;
pub mod manifest;
pub mod mem;
pub mod merge;
pub mod metrics;
pub mod mmap;
pub mod multivalue;
pub mod norms;
pub mod order;
mod ordmap;
pub mod phrase;
pub mod rootlog;
pub mod schema;
pub mod search;
pub mod segments;
pub mod settings;
pub mod shards;
pub mod simd;
pub mod slots;
pub mod snapshot;
pub mod sparse;
pub mod stats;
pub mod store;
pub mod sums;
pub mod translit;
pub mod vector;
pub mod wal;
pub mod work;

pub use index::Index;
pub use indexer::Indexer;
pub use search::{Hit, QueryWord, Request};
pub use segments::{QuerySpec, Segments, SpecWord, Term};
