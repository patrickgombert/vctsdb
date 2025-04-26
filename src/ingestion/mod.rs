//! Ingestion module for VCTSDB
//! Handles data ingestion from various formats and sources.

pub mod formats;
pub mod parser;
pub mod registry;
pub mod validation;

pub use validation::{ValidationMiddleware, ValidationConfig, ValidationError};
pub use registry::{ParserRegistry, Priority, RegistryError};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::data::DataPoint;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;
    use formats::{JsonParser, CsvParser};
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

    #[test]
    fn test_registry_format_negotiation() {
        let registry = ParserRegistry::new();
        
        // Create default and custom JSON parsers
        let default_parser = Arc::new(JsonParser::new());
        
        let mut custom_mapping = HashMap::new();
        custom_mapping.insert("timestamp".to_string(), "time".to_string());
        custom_mapping.insert("value".to_string(), "measurement".to_string());
        custom_mapping.insert("series".to_string(), "metric".to_string());
        let custom_parser = Arc::new(JsonParser::with_field_mapping(custom_mapping));
        
        // Register parsers - note the order and priorities here
        // Default parser with normal priority
        registry.register(default_parser.clone(), Priority::Normal).unwrap();
        // Custom parser with high priority - this will be tried first
        registry.register(custom_parser.clone(), Priority::High).unwrap();
        
        // Standard JSON data (works with default parser)
        let standard_data = r#"{
            "timestamp": 1000,
            "value": 42.5,
            "series": "test_series"
        }"#.as_bytes();
        
        // Custom JSON data (works with custom parser)
        let custom_data = r#"{
            "time": 2000,
            "measurement": 43.5,
            "metric": "test_series2"
        }"#.as_bytes();
        
        let standard_points = registry.parse_with_autodiscovery(standard_data).unwrap();
        assert_eq!(standard_points.len(), 1);
        assert_eq!(standard_points[0].timestamp(), 1000);

        let custom_points = registry.parse_with_autodiscovery(custom_data).unwrap();
        assert_eq!(custom_points.len(), 1);
        assert_eq!(custom_points[0].timestamp(), 2000);
    }

    #[test]
    fn test_csv_parser_header_detection() {
        let parser = CsvParser::new();
        
        // CSV with headers
        let input = "timestamp,value,series,region,datacenter\n\
                    1000,42.5,test_series,us-west,dc1\n\
                    2000,43.5,test_series,us-east,dc2"
            .as_bytes();
            
        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].tags().get("region"), Some(&"us-west".to_string()));
        assert_eq!(points[0].tags().get("datacenter"), Some(&"dc1".to_string()));
    }
    
    #[test]
    fn test_csv_parser_type_inference() {
        let parser = CsvParser::new();
        
        // Test different numeric formats
        let input = "timestamp,value,series\n\
                    1000,42.5,test_series\n\
                    2000,43,test_series\n\
                    3000,4.5e1,test_series"
            .as_bytes();
            
        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[1].value(), 43.0);
        assert_eq!(points[2].value(), 45.0);  // Scientific notation
    }
    
    #[test]
    fn test_csv_parser_registry_integration() {
        let registry = ParserRegistry::new();
        
        // Register CSV parser
        registry.register(Arc::new(CsvParser::new()), Priority::Normal).unwrap();
        
        // Get parser for CSV format
        let csv_parser = registry.get_parser("text/csv").unwrap();
        
        // Parse CSV data
        let input = "timestamp,value,series\n\
                    1000,42.5,test_series\n\
                    2000,43.5,test_series2"
            .as_bytes();
            
        let points = csv_parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[1].timestamp(), 2000);
    }
    
    #[test]
    fn test_mixed_format_autodiscovery() {
        let registry = ParserRegistry::new();
        
        // Register both JSON and CSV parsers
        registry.register(Arc::new(JsonParser::new()), Priority::Normal).unwrap();
        registry.register(Arc::new(CsvParser::new()), Priority::Normal).unwrap();
        
        // JSON data
        let json_data = r#"{"timestamp": 1000, "value": 42.5, "series": "test_series"}"#.as_bytes();
        
        // CSV data
        let csv_data = "timestamp,value,series\n1000,42.5,test_series".as_bytes();
        
        // Test autodiscovery for both formats
        let json_points = registry.parse_with_autodiscovery(json_data).unwrap();
        let csv_points = registry.parse_with_autodiscovery(csv_data).unwrap();
        
        assert_eq!(json_points.len(), 1);
        assert_eq!(csv_points.len(), 1);
        
        assert_eq!(json_points[0].timestamp(), 1000);
        assert_eq!(csv_points[0].timestamp(), 1000);
    }
}
