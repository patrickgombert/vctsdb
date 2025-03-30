//! Storage module for VCTSDB
//! Handles the core storage functionality including data structures and persistence.

pub mod data;
pub mod lsm;
pub mod wal;

pub use data::{DataError, DataPoint, TimeSeries};

#[cfg(test)]
mod tests {
    use super::*;
}
