use std::collections::HashMap;
use thiserror::Error;

use crate::storage::data::{DataPoint, DataError};

/// Errors that can occur during parsing
#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Invalid input format: {0}")]
    InvalidFormat(String),
    #[error("Missing required field: {0}")]
    MissingField(String),
    #[error("Invalid field type: {0}")]
    InvalidFieldType(String),
    #[error("Data validation error: {0}")]
    ValidationError(#[from] DataError),
    #[error("Batch processing error: {0}")]
    BatchError(String),
}

/// Result type for parser operations
pub type ParserResult<T> = Result<T, ParserError>;

/// Trait for parsing input data into DataPoints
pub trait Parser {
    /// Parses a single input into a vector of DataPoints
    fn parse(&self, input: &[u8]) -> ParserResult<Vec<DataPoint>>;

    /// Parses a batch of inputs into a vector of DataPoints
    fn parse_batch(&self, inputs: &[&[u8]]) -> ParserResult<Vec<DataPoint>> {
        let mut results = Vec::new();
        let mut errors = Vec::new();

        for (i, input) in inputs.iter().enumerate() {
            match self.parse(input) {
                Ok(points) => results.extend(points),
                Err(e) => errors.push((i, e)),
            }
        }

        if !errors.is_empty() {
            let error_msg = errors
                .into_iter()
                .map(|(i, e)| format!("Input {}: {}", i, e))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ParserError::BatchError(error_msg));
        }

        Ok(results)
    }

    /// Returns the supported input formats
    fn supported_formats(&self) -> Vec<&'static str>;
}
