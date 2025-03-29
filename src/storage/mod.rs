//! Storage module for VCTSDB
//! Handles the core storage functionality including data structures and persistence.

pub mod wal;
pub mod lsm;

#[cfg(test)]
mod tests {
    use super::*;
} 