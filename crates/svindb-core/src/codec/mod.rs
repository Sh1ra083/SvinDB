//! Сжатие кусков хранилища. Своё, потому что в ядре зависимостей нет.

pub mod huff;
pub mod lz4;
pub mod pack;

pub use lz4::{compress, decompress};
pub use pack::{DecoderDict, Effort, EncoderDict, Scratch};
