//! Ingestion module for VCTSDB
//! Handles data ingestion from various formats and sources.

pub mod parser;
pub mod formats;

#[cfg(test)]
mod tests {
    use super::*;
} 