use serde_json::{Value, Error as JsonError};
use std::collections::HashMap;

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
}
