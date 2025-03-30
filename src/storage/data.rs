use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum DataError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("Invalid series name: {0}")]
    InvalidSeriesName(String),
    #[error("Invalid tag key: {0}")]
    InvalidTagKey(String),
    #[error("Invalid tag value: {0}")]
    InvalidTagValue(String),
    #[error("Timestamp not strictly increasing")]
    NonIncreasingTimestamp,
}

/// Represents a single data point in a time series
#[derive(Debug, Clone)]
pub struct DataPoint {
    /// Timestamp in nanoseconds since epoch
    timestamp: i64,
    /// The actual value
    value: f64,
    /// Key-value pairs of tags
    tags: HashMap<String, String>,
}

impl DataPoint {
    /// Creates a new DataPoint with the given timestamp, value, and tags
    pub fn new(timestamp: i64, value: f64, tags: HashMap<String, String>) -> Self {
        Self {
            timestamp,
            value,
            tags,
        }
    }

    /// Returns the timestamp in nanoseconds
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the value
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Returns a reference to the tags
    pub fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }

    /// Validates the data point
    pub fn validate(&self) -> Result<(), DataError> {
        // Validate timestamp is positive
        if self.timestamp < 0 {
            return Err(DataError::InvalidTimestamp(format!(
                "Timestamp {} is negative",
                self.timestamp
            )));
        }

        // Validate tags
        for (key, value) in &self.tags {
            if !key.chars().all(|c| c.is_ascii()) {
                return Err(DataError::InvalidTagKey(key.clone()));
            }
            if !value.chars().all(|c| c.is_ascii()) {
                return Err(DataError::InvalidTagValue(value.clone()));
            }
        }

        Ok(())
    }
}

/// Represents a time series with a name and collection of data points
#[derive(Debug)]
pub struct TimeSeries {
    /// The name of the time series
    name: String,
    /// Collection of data points, protected by a read-write lock
    points: Arc<RwLock<Vec<DataPoint>>>,
    /// The last timestamp seen in this series
    last_timestamp: Arc<RwLock<i64>>,
}

impl TimeSeries {
    /// Creates a new TimeSeries with the given name
    pub fn new(name: String) -> Result<Self, DataError> {
        // Validate series name
        if name.is_empty() {
            return Err(DataError::InvalidSeriesName(
                "Series name cannot be empty".to_string(),
            ));
        }
        if !name.chars().all(|c| c.is_ascii()) {
            return Err(DataError::InvalidSeriesName(
                "Series name must be ASCII-only".to_string(),
            ));
        }

        Ok(Self {
            name,
            points: Arc::new(RwLock::new(Vec::new())),
            last_timestamp: Arc::new(RwLock::new(0)),
        })
    }

    /// Returns the name of the time series
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Adds a new data point to the time series
    pub async fn add_point(&self, point: DataPoint) -> Result<(), DataError> {
        // Validate the data point
        point.validate()?;

        // Check if timestamp is strictly increasing
        let last_ts = *self.last_timestamp.read().await;
        if point.timestamp <= last_ts {
            return Err(DataError::NonIncreasingTimestamp);
        }

        // Update last timestamp
        *self.last_timestamp.write().await = point.timestamp;

        // Add the point
        let mut points = self.points.write().await;
        points.push(point);

        Ok(())
    }

    /// Returns all data points in the time series
    pub async fn points(&self) -> Vec<DataPoint> {
        self.points.read().await.clone()
    }

    /// Returns the last timestamp seen in this series
    pub async fn last_timestamp(&self) -> i64 {
        *self.last_timestamp.read().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    async fn test_data_point_validation() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        // Valid data point
        let point = DataPoint::new(1000, 42.0, tags.clone());
        assert!(point.validate().is_ok());

        // Invalid timestamp
        let point = DataPoint::new(-1, 42.0, tags.clone());
        assert!(matches!(
            point.validate(),
            Err(DataError::InvalidTimestamp(_))
        ));

        // Invalid tag key (non-ASCII)
        let mut invalid_tags = HashMap::new();
        invalid_tags.insert("høst".to_string(), "server1".to_string());
        let point = DataPoint::new(1000, 42.0, invalid_tags);
        assert!(matches!(point.validate(), Err(DataError::InvalidTagKey(_))));
    }

    #[test]
    async fn test_time_series_creation() {
        // Valid series name
        assert!(TimeSeries::new("valid_series".to_string()).is_ok());

        // Invalid series name (empty)
        assert!(matches!(
            TimeSeries::new("".to_string()),
            Err(DataError::InvalidSeriesName(_))
        ));

        // Invalid series name (non-ASCII)
        assert!(matches!(
            TimeSeries::new("série".to_string()),
            Err(DataError::InvalidSeriesName(_))
        ));
    }

    #[test]
    async fn test_time_series_points() {
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        // Add points
        let point1 = DataPoint::new(1000, 42.0, tags.clone());
        let point2 = DataPoint::new(2000, 43.0, tags.clone());

        series.add_point(point1.clone()).await.unwrap();
        series.add_point(point2.clone()).await.unwrap();

        // Check points
        let points = series.points().await;
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[1].timestamp(), 2000);

        // Check last timestamp
        assert_eq!(series.last_timestamp().await, 2000);

        // Try to add point with non-increasing timestamp
        let invalid_point = DataPoint::new(1500, 44.0, tags);
        assert!(matches!(
            series.add_point(invalid_point).await,
            Err(DataError::NonIncreasingTimestamp)
        ));
    }
}
