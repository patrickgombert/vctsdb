use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::storage::data::{DataPoint, TimeSeries};

/// Represents a single entry in the MemTable
#[derive(Debug, Clone)]
struct MemTableEntry {
    series_name: String,
    point: DataPoint,
}

/// The in-memory table that stores recent writes before they are flushed to disk
pub struct MemTable {
    /// The actual data storage, ordered by timestamp
    data: Arc<RwLock<BTreeMap<i64, MemTableEntry>>>,
    /// Maximum number of entries before triggering a flush
    max_entries: usize,
    /// Current number of entries
    size: Arc<RwLock<usize>>,
}

impl MemTable {
    /// Creates a new MemTable with the specified capacity
    pub fn new(max_entries: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            max_entries,
            size: Arc::new(RwLock::new(0)),
        }
    }

    /// Inserts a data point into the MemTable
    /// Returns true if the MemTable needs to be flushed
    pub async fn insert(&self, series: &TimeSeries, point: &DataPoint) -> Result<bool, MemTableError> {
        // Validate timestamp ordering
        let mut size_guard = self.size.write().await;
        let mut data_guard = self.data.write().await;

        // Check if we need to flush after this insert
        let needs_flush = (*size_guard + 1) >= self.max_entries;

        // Create the entry
        let entry = MemTableEntry {
            series_name: series.name().to_string(),
            point: point.clone(),
        };

        // Insert into the BTreeMap (automatically ordered by timestamp)
        data_guard.insert(point.timestamp(), entry);
        *size_guard += 1;

        debug!(
            "Inserted point into MemTable: series={}, timestamp={}, size={}/{}",
            series.name(),
            point.timestamp(),
            *size_guard,
            self.max_entries
        );

        Ok(needs_flush)
    }

    /// Returns all points within a time range
    pub async fn get_range(&self, start: i64, end: i64) -> Vec<(String, DataPoint)> {
        let data_guard = self.data.read().await;
        data_guard
            .range(start..=end)
            .map(|(_, entry)| (entry.series_name.clone(), entry.point.clone()))
            .collect()
    }

    /// Returns all points for a specific series within a time range
    pub async fn get_series_range(
        &self,
        series_name: &str,
        start: i64,
        end: i64,
    ) -> Vec<DataPoint> {
        let data_guard = self.data.read().await;
        data_guard
            .range(start..=end)
            .filter(|(_, entry)| entry.series_name == series_name)
            .map(|(_, entry)| entry.point.clone())
            .collect()
    }

    /// Clears the MemTable and returns all entries
    pub async fn clear(&self) -> Vec<(String, DataPoint)> {
        let mut data_guard = self.data.write().await;
        let mut size_guard = self.size.write().await;

        let entries: Vec<_> = data_guard
            .iter()
            .map(|(_, entry)| (entry.series_name.clone(), entry.point.clone()))
            .collect();

        data_guard.clear();
        *size_guard = 0;

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