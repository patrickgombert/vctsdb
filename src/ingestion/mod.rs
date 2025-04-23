//! Ingestion module for VCTSDB
//! Handles data ingestion from various formats and sources.

pub mod formats;
pub mod parser;
pub mod validation;

pub use validation::{ValidationMiddleware, ValidationConfig, ValidationError};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::data::DataPoint;
    use std::collections::HashMap;
    use std::time::Instant;
    use formats::JsonParser;
    use parser::{Parser, ParserError};

    #[test]
    fn test_validation_integration() {
        let mut validator = ValidationMiddleware::new();
        let mut tags = HashMap::new();
        tags.insert("series".to_string(), "test_series".to_string());
        tags.insert("host".to_string(), "server1".to_string());

        let point = DataPoint::new(1000, 42.0, tags);
        assert!(validator.validate(&point).is_ok());
    }

    #[test]
    fn test_malformed_json_handling() {
        let parser = JsonParser::new();
        
        // Test invalid JSON syntax
        let input = r#"{ "timestamp": 1000, "value": 42.5, "series": "test" "#.as_bytes();
        assert!(matches!(
            parser.parse(input),
            Err(ParserError::InvalidFormat(_))
        ));

        // Test invalid value types
        let input = r#"{ "timestamp": "not_a_number", "value": 42.5, "series": "test" }"#.as_bytes();
        assert!(matches!(
            parser.parse(input),
            Err(ParserError::InvalidFieldType(_))
        ));

        // Test missing required fields
        let input = r#"{ "value": 42.5, "series": "test" }"#.as_bytes();
        assert!(matches!(
            parser.parse(input),
            Err(ParserError::MissingField(_))
        ));
    }

    #[test]
    fn test_schema_mismatch_detection() {
        let parser = JsonParser::new();

        // Test with custom field mapping
        let mut field_mapping = HashMap::new();
        field_mapping.insert("timestamp".to_string(), "time".to_string());
        field_mapping.insert("value".to_string(), "measurement".to_string());
        field_mapping.insert("series".to_string(), "metric".to_string());
        
        let custom_parser = JsonParser::with_field_mapping(field_mapping);

        // Test data with default schema (should fail with custom parser)
        let input = r#"{
            "timestamp": 1000,
            "value": 42.5,
            "series": "test"
        }"#.as_bytes();
        assert!(matches!(
            custom_parser.parse(input),
            Err(ParserError::MissingField(_))
        ));

        // Test data with custom schema (should fail with default parser)
        let input = r#"{
            "time": 1000,
            "measurement": 42.5,
            "metric": "test"
        }"#.as_bytes();
        assert!(matches!(
            parser.parse(input),
            Err(ParserError::MissingField(_))
        ));
    }

    #[test]
    fn test_ingestion_throughput() {
        let parser = JsonParser::new();
        let mut validator = ValidationMiddleware::new();
        
        // Prepare a batch of test data
        let test_data = r#"{
            "timestamp": 1000,
            "value": 42.5,
            "series": "test_series"
        }"#.as_bytes();
        
        let batch_size = 10_000;
        let mut inputs = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            inputs.push(test_data);
        }

        // Measure parsing throughput
        let start = Instant::now();
        let points = parser.parse_batch(&inputs).unwrap();
        let parse_duration = start.elapsed();
        let parse_throughput = batch_size as f64 / parse_duration.as_secs_f64();

        // Measure validation throughput
        let start = Instant::now();
        for point in &points {
            validator.validate(point).unwrap();
        }
        let validate_duration = start.elapsed();
        let validate_throughput = batch_size as f64 / validate_duration.as_secs_f64();

        println!("Parsing throughput: {:.2} points/sec", parse_throughput);
        println!("Validation throughput: {:.2} points/sec", validate_throughput);

        // Assert minimum throughput requirements
        assert!(parse_throughput > 1000.0, "Parsing throughput too low");
        assert!(validate_throughput > 1000.0, "Validation throughput too low");
    }
}
