use std::collections::HashMap;
use thiserror::Error;

use crate::storage::data::{DataPoint, DataError};

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Cardinality limit exceeded for series {0}: {1} > {2}")]
    CardinalityLimitExceeded(String, usize, usize),
    #[error("Value sanity check failed: {0}")]
    ValueSanityCheck(String),
    #[error("Data validation error: {0}")]
    DataError(#[from] DataError),
}

/// Configuration for validation middleware
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Maximum number of unique series allowed
    pub max_series: usize,
    /// Maximum number of unique tag values per tag key
    pub max_tag_values: usize,
    /// Maximum allowed value (for sanity checking)
    pub max_value: f64,
    /// Minimum allowed value (for sanity checking)
    pub min_value: f64,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            max_series: 100_000,
            max_tag_values: 10_000,
            max_value: f64::MAX,
            min_value: f64::MIN,
        }
    }
}

/// Validation middleware for data points
pub struct ValidationMiddleware {
    config: ValidationConfig,
    series_counts: HashMap<String, usize>,
    tag_value_counts: HashMap<String, HashMap<String, usize>>,
}

impl ValidationMiddleware {
    /// Creates a new validation middleware with default configuration
    pub fn new() -> Self {
        Self::with_config(ValidationConfig::default())
    }

    /// Creates a new validation middleware with custom configuration
    pub fn with_config(config: ValidationConfig) -> Self {
        Self {
            config,
            series_counts: HashMap::new(),
            tag_value_counts: HashMap::new(),
        }
    }

    /// Validates a data point against the configured rules
    pub fn validate(&mut self, point: &DataPoint) -> Result<(), ValidationError> {
        // Validate the data point itself
        point.validate()?;

        // Check value sanity
        if point.value() > self.config.max_value {
            return Err(ValidationError::ValueSanityCheck(format!(
                "Value {} exceeds maximum allowed value {}",
                point.value(),
                self.config.max_value
            )));
        }
        if point.value() < self.config.min_value {
            return Err(ValidationError::ValueSanityCheck(format!(
                "Value {} is below minimum allowed value {}",
                point.value(),
                self.config.min_value
            )));
        }

        // Get series name from tags
        let series_name = point.tags().get("series")
            .ok_or_else(|| ValidationError::ValueSanityCheck("Missing series tag".to_string()))?;

        // Check series cardinality
        if !self.series_counts.contains_key(series_name) {
            if self.series_counts.len() >= self.config.max_series {
                return Err(ValidationError::CardinalityLimitExceeded(
                    series_name.clone(),
                    self.series_counts.len(),
                    self.config.max_series
                ));
            }
            self.series_counts.insert(series_name.clone(), 0);
        }
        *self.series_counts.get_mut(series_name).unwrap() += 1;

        // Check tag value cardinality
        for (key, value) in point.tags() {
            if key == "series" {
                continue; // Skip series tag as it's handled separately
            }

            let tag_values = self.tag_value_counts.entry(key.clone())
                .or_insert_with(HashMap::new);
            
            // Check if this is a new unique value for this tag
            if !tag_values.contains_key(value) {
                // Check cardinality limit before adding new value
                if tag_values.len() >= self.config.max_tag_values {
                    return Err(ValidationError::CardinalityLimitExceeded(
                        format!("tag:{}", key),
                        tag_values.len(),
                        self.config.max_tag_values
                    ));
                }
                tag_values.insert(value.clone(), 1);
            } else {
                // Increment count for existing value
                *tag_values.get_mut(value).unwrap() += 1;
            }
        }

        Ok(())
    }

    /// Resets the internal counters
    pub fn reset(&mut self) {
        self.series_counts.clear();
        self.tag_value_counts.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_validation_middleware() {
        let mut validator = ValidationMiddleware::with_config(ValidationConfig {
            max_series: 2,
            max_tag_values: 2,
            max_value: 100.0,
            min_value: 0.0,
            ..Default::default()
        });

        let mut tags = HashMap::new();
        tags.insert("series".to_string(), "test_series".to_string());
        tags.insert("host".to_string(), "server1".to_string());

        // Valid point
        let point = DataPoint::new(1000, 42.0, tags.clone());
        assert!(validator.validate(&point).is_ok());

        // Value exceeds maximum
        let point = DataPoint::new(1000, 150.0, tags.clone());
        assert!(matches!(
            validator.validate(&point),
            Err(ValidationError::ValueSanityCheck(_))
        ));

        // Value below minimum
        let point = DataPoint::new(1000, -1.0, tags.clone());
        assert!(matches!(
            validator.validate(&point),
            Err(ValidationError::ValueSanityCheck(_))
        ));

        // Exceed series cardinality
        let mut tags2 = tags.clone();
        tags2.insert("series".to_string(), "test_series2".to_string());
        let point = DataPoint::new(1000, 42.0, tags2.clone());
        assert!(validator.validate(&point).is_ok());

        let mut tags3 = tags.clone();
        tags3.insert("series".to_string(), "test_series3".to_string());
        let point = DataPoint::new(1000, 42.0, tags3);
        assert!(matches!(
            validator.validate(&point),
            Err(ValidationError::CardinalityLimitExceeded(_, _, _))
        ));

        // Exceed tag value cardinality
        validator.reset();
        
        // Add first point with original host
        let point = DataPoint::new(1000, 42.0, tags.clone());
        assert!(validator.validate(&point).is_ok());
        
        // Add point with second host
        let mut tags4 = tags.clone();
        tags4.insert("host".to_string(), "server2".to_string());
        let point = DataPoint::new(1000, 42.0, tags4.clone());
        assert!(validator.validate(&point).is_ok());

        // Try to add point with third host (should fail)
        let mut tags5 = tags.clone();
        tags5.insert("host".to_string(), "server3".to_string());
        let point = DataPoint::new(1000, 42.0, tags5);
        assert!(matches!(
            validator.validate(&point),
            Err(ValidationError::CardinalityLimitExceeded(_, _, _))
        ));
    }
} 