use serde_json::{Value, Error as JsonError};
use std::collections::HashMap;
use std::io::Read;
use csv::{Reader, ReaderBuilder, StringRecord};
use std::str::FromStr;

use super::parser::{Parser, ParserError, ParserResult};
use crate::storage::data::DataPoint;

/// Parser for JSON input format
pub struct JsonParser {
    /// Field mapping configuration
    field_mapping: HashMap<String, String>,
}

impl JsonParser {
    /// Creates a new JsonParser with default field mapping
    pub fn new() -> Self {
        let mut field_mapping = HashMap::new();
        field_mapping.insert("timestamp".to_string(), "timestamp".to_string());
        field_mapping.insert("value".to_string(), "value".to_string());
        field_mapping.insert("series".to_string(), "series".to_string());
        
        Self { field_mapping }
    }

    /// Creates a new JsonParser with custom field mapping
    pub fn with_field_mapping(field_mapping: HashMap<String, String>) -> Self {
        Self { field_mapping }
    }

    /// Extracts a field from JSON value with type coercion
    fn extract_field<T: From<f64>>(&self, value: &Value, field: &str) -> ParserResult<T> {
        let field_name = self.field_mapping.get(field)
            .ok_or_else(|| ParserError::MissingField(field.to_string()))?;

        let field_value = value.get(field_name)
            .ok_or_else(|| ParserError::MissingField(field_name.to_string()))?;

        match field_value {
            Value::Number(n) => n.as_f64()
                .ok_or_else(|| ParserError::InvalidFieldType(format!("{} must be a number", field_name)))
                .map(|f| T::from(f)),
            _ => Err(ParserError::InvalidFieldType(format!("{} must be a number", field_name))),
        }
    }

    /// Extracts a timestamp field from JSON value
    fn extract_timestamp(&self, value: &Value, field: &str) -> ParserResult<i64> {
        let field_name = self.field_mapping.get(field)
            .ok_or_else(|| ParserError::MissingField(field.to_string()))?;

        let field_value = value.get(field_name)
            .ok_or_else(|| ParserError::MissingField(field_name.to_string()))?;

        match field_value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(i)
                } else if let Some(f) = n.as_f64() {
                    Ok(f as i64)
                } else {
                    Err(ParserError::InvalidFieldType(format!("{} must be a number", field_name)))
                }
            }
            _ => Err(ParserError::InvalidFieldType(format!("{} must be a number", field_name))),
        }
    }
}

impl Parser for JsonParser {
    fn parse(&self, input: &[u8]) -> ParserResult<Vec<DataPoint>> {
        let value: Value = serde_json::from_slice(input)
            .map_err(|e| ParserError::InvalidFormat(e.to_string()))?;

        let mut points = Vec::new();

        // Handle both single object and array of objects
        match value {
            Value::Object(obj) => {
                let timestamp: i64 = self.extract_timestamp(&Value::Object(obj.clone()), "timestamp")?;
                let value: f64 = self.extract_field(&Value::Object(obj.clone()), "value")?;
                
                let mut tags = HashMap::new();
                if let Some(series) = obj.get(self.field_mapping.get("series").unwrap()) {
                    if let Some(series_str) = series.as_str() {
                        tags.insert("series".to_string(), series_str.to_string());
                    }
                }

                points.push(DataPoint::new(timestamp, value, tags));
            }
            Value::Array(arr) => {
                for item in arr {
                    if let Value::Object(obj) = item {
                        let timestamp: i64 = self.extract_timestamp(&Value::Object(obj.clone()), "timestamp")?;
                        let value: f64 = self.extract_field(&Value::Object(obj.clone()), "value")?;
                        
                        let mut tags = HashMap::new();
                        if let Some(series) = obj.get(self.field_mapping.get("series").unwrap()) {
                            if let Some(series_str) = series.as_str() {
                                tags.insert("series".to_string(), series_str.to_string());
                            }
                        }

                        points.push(DataPoint::new(timestamp, value, tags));
                    }
                }
            }
            _ => return Err(ParserError::InvalidFormat("Input must be a JSON object or array".to_string())),
        }

        Ok(points)
    }

    fn supported_formats(&self) -> Vec<&'static str> {
        vec!["application/json", "json"]
    }
}

/// Parser for CSV input format
pub struct CsvParser {
    /// Field mapping configuration
    field_mapping: HashMap<String, String>,
    /// Whether to detect headers in the CSV
    has_headers: bool,
    /// Column indices for required fields
    column_indices: HashMap<String, usize>,
    /// Delimiter character
    delimiter: u8,
    /// Additional tag columns to extract
    tag_columns: HashMap<String, usize>,
}

impl CsvParser {
    /// Creates a new CsvParser with default configuration (with headers)
    pub fn new() -> Self {
        let mut field_mapping = HashMap::new();
        field_mapping.insert("timestamp".to_string(), "timestamp".to_string());
        field_mapping.insert("value".to_string(), "value".to_string());
        field_mapping.insert("series".to_string(), "series".to_string());
        
        Self {
            field_mapping,
            has_headers: true,
            column_indices: HashMap::new(),
            delimiter: b',',
            tag_columns: HashMap::new(),
        }
    }

    /// Creates a new CsvParser with explicit column indices (no headers)
    pub fn with_column_indices(timestamp_idx: usize, value_idx: usize, tag_columns: HashMap<String, usize>) -> Self {
        let mut column_indices = HashMap::new();
        column_indices.insert("timestamp".to_string(), timestamp_idx);
        column_indices.insert("value".to_string(), value_idx);
        
        let mut field_mapping = HashMap::new();
        field_mapping.insert("timestamp".to_string(), "timestamp".to_string());
        field_mapping.insert("value".to_string(), "value".to_string());
        field_mapping.insert("series".to_string(), "series".to_string());
        
        Self {
            field_mapping,
            has_headers: false,
            column_indices,
            delimiter: b',',
            tag_columns,
        }
    }

    /// Creates a new CsvParser with custom field mapping
    pub fn with_field_mapping(field_mapping: HashMap<String, String>, has_headers: bool) -> Self {
        Self {
            field_mapping,
            has_headers,
            column_indices: HashMap::new(),
            delimiter: b',',
            tag_columns: HashMap::new(),
        }
    }

    /// Sets the delimiter character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Configure additional tag columns
    pub fn with_tag_columns(mut self, tag_columns: HashMap<String, usize>) -> Self {
        self.tag_columns = tag_columns;
        self
    }

    /// Parse value from string with type inference
    fn parse_value<T: FromStr>(&self, value: &str) -> ParserResult<T> {
        value.parse::<T>().map_err(|_| {
            ParserError::InvalidFieldType(format!("Failed to parse '{}' to the required type", value))
        })
    }

    /// Extract field from a record by name or index
    fn extract_field<T: FromStr>(&self, record: &StringRecord, headers: Option<&StringRecord>, field: &str) -> ParserResult<T> {
        let field_name = self.field_mapping.get(field)
            .ok_or_else(|| ParserError::MissingField(field.to_string()))?;

        let field_value = if self.has_headers && headers.is_some() {
            // Extract by header name
            let header = headers.unwrap();
            let idx = header.iter().position(|h| h == field_name)
                .ok_or_else(|| ParserError::MissingField(field_name.clone()))?;
            
            record.get(idx)
                .ok_or_else(|| ParserError::MissingField(field_name.clone()))?
        } else if let Some(idx) = self.column_indices.get(field) {
            // Extract by predefined column index
            record.get(*idx)
                .ok_or_else(|| ParserError::MissingField(format!("Column index {} not found", idx)))?
        } else {
            return Err(ParserError::MissingField(format!("No mapping for {}", field)));
        };

        self.parse_value(field_value)
    }
    
    /// Detect headers and column indices from the first record
    fn detect_headers(&mut self, reader: &mut Reader<&[u8]>) -> ParserResult<()> {
        if !self.has_headers {
            return Ok(());
        }
        
        let headers = reader.headers()
            .map_err(|e| ParserError::InvalidFormat(format!("Failed to read CSV headers: {}", e)))?;
        
        // Map required fields to column indices
        for field in &["timestamp", "value"] {
            if let Some(mapped_name) = self.field_mapping.get(*field) {
                if let Some(pos) = headers.iter().position(|h| h == mapped_name) {
                    self.column_indices.insert(field.to_string(), pos);
                }
            }
        }
        
        // Check if we found all required fields
        if !self.column_indices.contains_key("timestamp") || !self.column_indices.contains_key("value") {
            return Err(ParserError::InvalidFormat("CSV headers must contain timestamp and value fields".to_string()));
        }
        
        // Detect additional tag columns (any column that isn't timestamp or value)
        for (i, header) in headers.iter().enumerate() {
            if i != self.column_indices["timestamp"] && i != self.column_indices["value"] {
                self.tag_columns.insert(header.to_string(), i);
            }
        }
        
        Ok(())
    }
}

impl Parser for CsvParser {
    fn parse(&self, input: &[u8]) -> ParserResult<Vec<DataPoint>> {
        // Create a CSV reader
        let mut reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .delimiter(self.delimiter)
            .from_reader(input);
        
        // Clone self to detect headers in a mutable copy
        let mut parser_with_headers = self.clone();
        if self.has_headers && self.column_indices.is_empty() {
            parser_with_headers.detect_headers(&mut reader)?;
        }
        
        let headers = if self.has_headers {
            Some(reader.headers()
                .map_err(|e| ParserError::InvalidFormat(format!("Failed to read CSV headers: {}", e)))?.clone())
        } else {
            None
        };
        
        let mut points = Vec::new();
        
        // Process each record
        for result in reader.records() {
            let record = result.map_err(|e| 
                ParserError::InvalidFormat(format!("Failed to read CSV record: {}", e)))?;
            
            let timestamp: i64 = parser_with_headers.extract_field(&record, headers.as_ref(), "timestamp")?;
            let value: f64 = parser_with_headers.extract_field(&record, headers.as_ref(), "value")?;
            
            // Extract tags
            let mut tags = HashMap::new();
            
            // Extract series tag if available
            if let Some(series_idx) = parser_with_headers.column_indices.get("series")
                .or_else(|| parser_with_headers.tag_columns.get(&parser_with_headers.field_mapping["series"])) {
                if let Some(series_value) = record.get(*series_idx) {
                    tags.insert("series".to_string(), series_value.to_string());
                }
            }
            
            // Extract additional tags
            for (tag_name, tag_idx) in &parser_with_headers.tag_columns {
                if let Some(tag_value) = record.get(*tag_idx) {
                    if !tag_value.is_empty() {
                        tags.insert(tag_name.clone(), tag_value.to_string());
                    }
                }
            }
            
            points.push(DataPoint::new(timestamp, value, tags));
        }
        
        Ok(points)
    }
    
    fn supported_formats(&self) -> Vec<&'static str> {
        vec!["text/csv", "csv"]
    }
}

// Add Clone derive for CSVParser
impl Clone for CsvParser {
    fn clone(&self) -> Self {
        Self {
            field_mapping: self.field_mapping.clone(),
            has_headers: self.has_headers,
            column_indices: self.column_indices.clone(),
            delimiter: self.delimiter,
            tag_columns: self.tag_columns.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_parser_single_point() {
        let parser = JsonParser::new();
        let input = r#"{
            "timestamp": 1000,
            "value": 42.5,
            "series": "test_series"
        }"#.as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[0].tags().get("series"), Some(&"test_series".to_string()));
    }

    #[test]
    fn test_json_parser_batch_points() {
        let parser = JsonParser::new();
        let input = r#"[
            {
                "timestamp": 1000,
                "value": 42.5,
                "series": "test_series"
            },
            {
                "timestamp": 2000,
                "value": 43.5,
                "series": "test_series"
            }
        ]"#.as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[1].timestamp(), 2000);
        assert_eq!(points[1].value(), 43.5);
    }

    #[test]
    fn test_json_parser_invalid_input() {
        let parser = JsonParser::new();
        let input = r#"{
            "invalid": "format"
        }"#.as_bytes();

        let result = parser.parse(input);
        assert!(matches!(result, Err(ParserError::MissingField(_))));
    }

    #[test]
    fn test_json_parser_custom_mapping() {
        let mut field_mapping = HashMap::new();
        field_mapping.insert("timestamp".to_string(), "ts".to_string());
        field_mapping.insert("value".to_string(), "val".to_string());
        field_mapping.insert("series".to_string(), "name".to_string());

        let parser = JsonParser::with_field_mapping(field_mapping);
        let input = r#"{
            "ts": 1000,
            "val": 42.5,
            "name": "test_series"
        }"#.as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[0].tags().get("series"), Some(&"test_series".to_string()));
    }

    #[test]
    fn test_csv_parser_with_headers() {
        let parser = CsvParser::new();
        let input = "timestamp,value,series,region\n\
                    1000,42.5,test_series,us-west\n\
                    2000,43.5,test_series,us-east"
            .as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[0].tags().get("series"), Some(&"test_series".to_string()));
        assert_eq!(points[0].tags().get("region"), Some(&"us-west".to_string()));
        assert_eq!(points[1].timestamp(), 2000);
        assert_eq!(points[1].value(), 43.5);
        assert_eq!(points[1].tags().get("region"), Some(&"us-east".to_string()));
    }

    #[test]
    fn test_csv_parser_without_headers() {
        let mut tag_columns = HashMap::new();
        tag_columns.insert("series".to_string(), 2);
        tag_columns.insert("region".to_string(), 3);
        
        let parser = CsvParser::with_column_indices(0, 1, tag_columns);
        let input = "1000,42.5,test_series,us-west\n\
                    2000,43.5,test_series,us-east"
            .as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[0].tags().get("series"), Some(&"test_series".to_string()));
        assert_eq!(points[0].tags().get("region"), Some(&"us-west".to_string()));
    }

    #[test]
    fn test_csv_parser_custom_delimiter() {
        let parser = CsvParser::new().with_delimiter(b';');
        let input = "timestamp;value;series\n\
                    1000;42.5;test_series\n\
                    2000;43.5;test_series2"
            .as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[0].tags().get("series"), Some(&"test_series".to_string()));
        assert_eq!(points[1].tags().get("series"), Some(&"test_series2".to_string()));
    }

    #[test]
    fn test_csv_parser_custom_mapping() {
        let mut field_mapping = HashMap::new();
        field_mapping.insert("timestamp".to_string(), "time".to_string());
        field_mapping.insert("value".to_string(), "measurement".to_string());
        field_mapping.insert("series".to_string(), "metric".to_string());
        
        let parser = CsvParser::with_field_mapping(field_mapping, true);
        let input = "time,measurement,metric\n\
                    1000,42.5,test_series\n\
                    2000,43.5,test_series2"
            .as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[0].tags().get("series"), Some(&"test_series".to_string()));
    }

    #[test]
    fn test_csv_parser_type_inference() {
        let parser = CsvParser::new();
        
        // Test with different numeric formats
        let input = "timestamp,value,series\n\
                    1000,42.5,test_series\n\
                    2000,43,test_series\n\
                    3000,4.5e1,test_series"
            .as_bytes();

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].value(), 42.5);
        assert_eq!(points[1].value(), 43.0);
        assert_eq!(points[2].value(), 45.0);
    }

    #[test]
    fn test_csv_parser_invalid_input() {
        let parser = CsvParser::new();
        
        // Missing required column
        let input = "timestamp,series\n\
                    1000,test_series"
            .as_bytes();
            
        let result = parser.parse(input);
        assert!(matches!(result, Err(ParserError::InvalidFormat(_))));
        
        // Invalid numeric value
        let input = "timestamp,value,series\n\
                    1000,not_a_number,test_series"
            .as_bytes();
            
        let result = parser.parse(input);
        assert!(matches!(result, Err(ParserError::InvalidFieldType(_))));
    }
}
