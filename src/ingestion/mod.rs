//! Ingestion module for VCTSDB
//! Handles data ingestion from various formats and sources.

pub mod formats;
pub mod parser;

#[cfg(test)]
mod tests {
    use super::*;
}
