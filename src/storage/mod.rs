//! Storage module for VCTSDB
//! Handles the core storage functionality including data structures and persistence.

pub mod data;
pub mod lsm;
pub mod wal;
pub mod index;

pub use data::{DataError, DataPoint, TimeSeries};
pub use lsm::{MemTable, SSTable, SSTableCatalog};
pub use wal::WriteAheadLog;
pub use index::IndexInfo;

#[cfg(test)]
mod tests {
    
}
