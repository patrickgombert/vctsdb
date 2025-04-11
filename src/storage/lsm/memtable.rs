
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug};
use std::collections::HashMap;

use crate::storage::data::{DataPoint, TimeSeries};

/// Represents a single entry in the MemTable
#[derive(Debug, Clone)]
struct MemTableEntry {
    series_name: String,
    point: DataPoint,
}

/// The in-memory table that stores recent writes before they are flushed to disk
pub struct MemTable {
    /// The data stored in the MemTable, organized by series name
    data: Arc<RwLock<HashMap<String, Vec<DataPoint>>>>,
    /// Maximum number of points allowed in the MemTable
    capacity: usize,
    /// Current number of points in the MemTable
    size: Arc<RwLock<usize>>,
}

impl MemTable {
    /// Creates a new MemTable with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            capacity,
            size: Arc::new(RwLock::new(0)),
        }
    }

    /// Returns the capacity of the MemTable
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current data in the MemTable
    pub async fn get_data(&self) -> HashMap<String, Vec<DataPoint>> {
        self.data.read().await.clone()
    }

    /// Inserts a data point into the MemTable
    /// Returns true if the MemTable needs to be flushed
    pub async fn insert(
        &self,
        series: &TimeSeries,
        point: &DataPoint,
    ) -> Result<bool, MemTableError> {
        let mut size = self.size.write().await;
        let mut data = self.data.write().await;

        // Check if we need to flush after this insert
        let needs_flush = (*size + 1) >= self.capacity;

        // Get or create the series vector
        let points = data.entry(series.name().to_string())
            .or_insert_with(Vec::new);

        // Validate timestamp ordering
        if let Some(last_point) = points.last() {
            if point.timestamp() <= last_point.timestamp() {
                return Err(MemTableError::InvalidTimestampOrder);
            }
        }

        // Insert the point
        points.push(point.clone());
        *size += 1;

        debug!(
            "Inserted point into MemTable: series={}, timestamp={}, size={}/{}",
            series.name(),
            point.timestamp(),
            *size,
            self.capacity
        );

        Ok(needs_flush)
    }

    /// Returns all points within a time range
    pub async fn get_range(&self, start: i64, end: i64) -> Vec<(String, DataPoint)> {
        let data = self.data.read().await;
        let mut result = Vec::new();

        for (series_name, points) in data.iter() {
            for point in points {
                if point.timestamp() >= start && point.timestamp() <= end {
                    result.push((series_name.clone(), point.clone()));
                }
            }
        }

        result
    }

    /// Returns all points for a specific series within a time range
    pub async fn get_series_range(
        &self,
        series_name: &str,
        start: i64,
        end: i64,
    ) -> Vec<DataPoint> {
        let data = self.data.read().await;
        if let Some(points) = data.get(series_name) {
            points
                .iter()
                .filter(|p| p.timestamp() >= start && p.timestamp() <= end)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Clears the MemTable and returns all entries
    pub async fn clear(&self) -> Vec<(String, DataPoint)> {
        let mut data = self.data.write().await;
        let mut size = self.size.write().await;

        let mut entries = Vec::new();
        for (series_name, points) in data.drain() {
            for point in points {
                entries.push((series_name.clone(), point));
            }
        }

        *size = 0;
        entries
    }

    /// Returns the current number of entries
    pub async fn size(&self) -> usize {
        *self.size.read().await
    }

    /// Returns true if the MemTable is empty
    pub async fn is_empty(&self) -> bool {
        *self.size.read().await == 0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemTableError {
    #[error("MemTable is full")]
    Full,
    #[error("Invalid timestamp order")]
    InvalidTimestampOrder,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    async fn test_memtable_insert_and_retrieve() {
        let memtable = MemTable::new(1000);
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        // Insert some points
        let points = vec![
            DataPoint::new(1000, 42.0, tags.clone()),
            DataPoint::new(1001, 43.0, tags.clone()),
            DataPoint::new(1002, 44.0, tags.clone()),
        ];

        for point in &points {
            memtable.insert(&series, point).await.unwrap();
        }

        // Retrieve points
        let retrieved = memtable.get_range(1000, 1002).await;
        assert_eq!(retrieved.len(), 3);

        // Verify order and values
        for (i, (name, point)) in retrieved.iter().enumerate() {
            assert_eq!(name, "test_series");
            assert_eq!(point.timestamp(), points[i].timestamp());
            assert_eq!(point.value(), points[i].value());
        }
    }

    #[test]
    async fn test_memtable_capacity() {
        let memtable = MemTable::new(2);
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        // Insert points up to capacity
        let points = vec![
            DataPoint::new(1000, 42.0, tags.clone()),
            DataPoint::new(1001, 43.0, tags.clone()),
            DataPoint::new(1002, 44.0, tags.clone()),
        ];

        for (i, point) in points.iter().enumerate() {
            let needs_flush = memtable.insert(&series, point).await.unwrap();
            assert_eq!(needs_flush, i >= 1); // Should need flush after second insert
        }

        assert_eq!(memtable.size().await, 3);
    }

    #[test]
    async fn test_memtable_clear() {
        let memtable = MemTable::new(1000);
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        // Insert some points
        let points = vec![
            DataPoint::new(1000, 42.0, tags.clone()),
            DataPoint::new(1001, 43.0, tags.clone()),
        ];

        for point in &points {
            memtable.insert(&series, point).await.unwrap();
        }

        // Clear and verify
        let cleared = memtable.clear().await;
        assert_eq!(cleared.len(), 2);
        assert!(memtable.is_empty().await);
    }
}
