//! VCTSDB - A high-performance time series database
//! 
//! This crate provides a single-node time series database implementation
//! optimized for system metrics with infinite retention and high cardinality support.

pub mod storage;
pub mod ingestion;
pub mod query;
pub mod metrics; 