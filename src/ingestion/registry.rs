use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use thiserror::Error;

use super::parser::{Parser, ParserResult};
use crate::storage::data::DataPoint;

/// Errors that can occur during parser registration and lookup
#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("No parser found for format: {0}")]
    NoParserFound(String),
    #[error("Parser already registered for format: {0}")]
    AlreadyRegistered(String),
    #[error("Invalid format specified: {0}")]
    InvalidFormat(String),
}

/// Result type for registry operations
pub type RegistryResult<T> = Result<T, RegistryError>;

/// Priority level for parser registration
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low = 0,
    Normal = 50,
    High = 100,
}

impl Default for Priority {
    fn default() -> Self {
        Self::Normal
    }
}

/// ParserEntry combines a parser with its priority
struct ParserEntry {
    parser: Arc<dyn Parser + Send + Sync>,
    priority: Priority,
}

/// ParserRegistry manages registered parsers and their priorities
pub struct ParserRegistry {
    /// Map from format name to parser entries
    parsers: RwLock<HashMap<String, Vec<ParserEntry>>>,
    /// Default parsers to try when format is unknown
    default_parsers: RwLock<Vec<ParserEntry>>,
}

impl ParserRegistry {
    /// Creates a new, empty parser registry
    pub fn new() -> Self {
        Self {
            parsers: RwLock::new(HashMap::new()),
            default_parsers: RwLock::new(Vec::new()),
        }
    }

    /// Register a parser for specific formats with a given priority
    pub fn register<P>(
        &self,
        parser: Arc<P>,
        priority: Priority,
    ) -> RegistryResult<()> 
    where 
        P: Parser + Send + Sync + 'static,
    {
        let formats = parser.supported_formats();
        if formats.is_empty() {
            return Err(RegistryError::InvalidFormat(
                "Parser doesn't support any formats".to_string(),
            ));
        }

        let mut parsers_map = self.parsers.write().unwrap();
        
        // Register for each supported format
        for format in formats {
            let format_key = format.to_lowercase();
            let entry = ParserEntry {
                parser: parser.clone(),
                priority,
            };

            parsers_map
                .entry(format_key)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        // Also add to default parsers list
        let mut default_parsers = self.default_parsers.write().unwrap();
        default_parsers.push(ParserEntry {
            parser: parser.clone(),
            priority,
        });

        // Sort entries by priority (highest first)
        for entries in parsers_map.values_mut() {
            entries.sort_by(|a, b| b.priority.cmp(&a.priority));
        }
        
        default_parsers.sort_by(|a, b| b.priority.cmp(&a.priority));

        Ok(())
    }

    /// Get a parser for a specific format
    pub fn get_parser(&self, format: &str) -> RegistryResult<Arc<dyn Parser + Send + Sync>> {
        let parsers_map = self.parsers.read().unwrap();
        let format_key = format.to_lowercase();

        if let Some(entries) = parsers_map.get(&format_key) {
            if !entries.is_empty() {
                return Ok(Arc::clone(&entries[0].parser));
            }
        }

        Err(RegistryError::NoParserFound(format.to_string()))
    }

    /// Parse data with autodiscovery (tries each parser until one succeeds)
    pub fn parse_with_autodiscovery(&self, input: &[u8]) -> ParserResult<Vec<DataPoint>> {
        // First try with known format if provided
        let default_parsers = self.default_parsers.read().unwrap();
        
        if default_parsers.is_empty() {
            return Err(super::parser::ParserError::InvalidFormat(
                "No parsers registered".to_string(),
            ));
        }

        // Try each parser in priority order
        let mut last_error = None;
        for entry in default_parsers.iter() {
            match entry.parser.parse(input) {
                Ok(points) => return Ok(points),
                Err(err) => last_error = Some(err),
            }
        }

        // Return the last error if all parsers failed
        Err(last_error.unwrap_or_else(|| {
            super::parser::ParserError::InvalidFormat("All parsers failed".to_string())
        }))
    }

    /// Parse data using a specific format
    pub fn parse_with_format(&self, format: &str, input: &[u8]) -> ParserResult<Vec<DataPoint>> {
        match self.get_parser(format) {
            Ok(parser) => parser.parse(input),
            Err(err) => Err(super::parser::ParserError::InvalidFormat(err.to_string())),
        }
    }

    /// Unregister a parser
    pub fn unregister<P>(&self, parser: &Arc<P>, format: Option<&str>) -> RegistryResult<()> 
    where 
        P: Parser + Send + Sync + 'static,
    {
        let parser_ptr = Arc::as_ptr(parser) as *const ();
        let mut parsers_map = self.parsers.write().unwrap();
        let mut default_parsers = self.default_parsers.write().unwrap();

        // Remove from default parsers
        default_parsers.retain(|entry| Arc::as_ptr(&entry.parser) as *const () != parser_ptr);

        // If format is specified, only unregister from that format
        if let Some(format_str) = format {
            let format_key = format_str.to_lowercase();
            if let Some(entries) = parsers_map.get_mut(&format_key) {
                entries.retain(|entry| Arc::as_ptr(&entry.parser) as *const () != parser_ptr);
            }
            return Ok(());
        }

        // Otherwise, unregister from all formats
        for entries in parsers_map.values_mut() {
            entries.retain(|entry| Arc::as_ptr(&entry.parser) as *const () != parser_ptr);
        }

        // Clean up empty format entries
        parsers_map.retain(|_, entries| !entries.is_empty());

        Ok(())
    }

    /// List all registered formats
    pub fn list_formats(&self) -> Vec<String> {
        let parsers_map = self.parsers.read().unwrap();
        parsers_map.keys().cloned().collect()
    }
}

impl Default for ParserRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::formats::JsonParser;
    use std::collections::HashMap;

    #[test]
    fn test_register_and_get_parser() {
        let registry = ParserRegistry::new();
        let parser = Arc::new(JsonParser::new());

        // Register parser with normal priority
        registry.register(parser.clone(), Priority::Normal).unwrap();

        // Get parser for supported format
        let retrieved = registry.get_parser("application/json").unwrap();
        
        // Can't directly compare due to type erasure, but we can test that it works
        let test_input = r#"{"timestamp": 1000, "value": 42.5, "series": "test"}"#.as_bytes();
        let points = retrieved.parse(test_input).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp(), 1000);

        // Try to get parser for unsupported format
        let result = registry.get_parser("unsupported/format");
        assert!(matches!(result, Err(RegistryError::NoParserFound(_))));
    }

    #[test]
    fn test_priority_handling() {
        let registry = ParserRegistry::new();
        
        // Create two JSON parsers with different mappings
        let mut field_mapping = HashMap::new();
        field_mapping.insert("timestamp".to_string(), "ts".to_string());
        field_mapping.insert("value".to_string(), "val".to_string());
        field_mapping.insert("series".to_string(), "name".to_string());

        let high_parser = Arc::new(JsonParser::with_field_mapping(field_mapping));
        let low_parser = Arc::new(JsonParser::new());

        // Register parsers with different priorities
        registry.register(low_parser.clone(), Priority::Low).unwrap();
        registry.register(high_parser.clone(), Priority::High).unwrap();

        // Prepare test data for each parser
        let standard_data = r#"{"timestamp": 1000, "value": 42.5, "series": "test"}"#.as_bytes();
        let custom_data = r#"{"ts": 2000, "val": 43.5, "name": "test2"}"#.as_bytes();
        
        // Get parser for JSON format - should use the high priority parser first
        let retrieved = registry.get_parser("application/json").unwrap();
        
        // The high priority parser should successfully parse custom data
        let result = retrieved.parse(custom_data);
        assert!(result.is_ok());
        
        // But fail on standard data
        let result = retrieved.parse(standard_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_autodiscovery() {
        let registry = ParserRegistry::new();
        let parser = Arc::new(JsonParser::new());

        // Register parser
        registry.register(parser.clone(), Priority::Normal).unwrap();

        // Valid JSON input
        let input = r#"{"timestamp": 1000, "value": 42.5, "series": "test_series"}"#.as_bytes();
        
        // Test autodiscovery
        let result = registry.parse_with_autodiscovery(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp(), 1000);
        assert_eq!(result[0].value(), 42.5);
    }

    #[test]
    fn test_unregister() {
        let registry = ParserRegistry::new();
        let parser = Arc::new(JsonParser::new());

        // Register parser
        registry.register(parser.clone(), Priority::Normal).unwrap();
        
        // Verify parser is registered
        assert!(registry.get_parser("application/json").is_ok());
        
        // Unregister for one format
        registry.unregister(&parser, Some("application/json")).unwrap();
        
        // Verify parser is unregistered
        assert!(matches!(
            registry.get_parser("application/json"),
            Err(RegistryError::NoParserFound(_))
        ));
        
        // Should still be registered for "json" format
        assert!(registry.get_parser("json").is_ok());
    }
} 