use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::storage::data::{DataPoint, TimeSeries};
use crate::storage::lsm::memtable::MemTable;
use crate::storage::lsm::sstable::{SSTable, DataBlock};

/// Represents a time range with start and end timestamps
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

impl TimeRange {
    /// Creates a new time range
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Checks if this time range overlaps with another time range
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    /// Checks if this time range contains a specific timestamp
    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }
}

/// Represents a query that can be routed to appropriate storage components
#[derive(Debug)]
pub struct Query {
    /// The time range to query
    pub time_range: TimeRange,
    /// Optional series name filter
    pub series_name: Option<String>,
}

impl Query {
    /// Creates a new query with the given time range
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            time_range: TimeRange::new(start, end),
            series_name: None,
        }
    }

    /// Creates a new query with the given time range and series name
    pub fn with_series(start: i64, end: i64, series_name: String) -> Self {
        Self {
            time_range: TimeRange::new(start, end),
            series_name: Some(series_name),
        }
    }
}

/// Manages query routing to appropriate storage components
pub struct QueryRouter {
    /// The active MemTable
    memtable: Arc<RwLock<MemTable>>,
    /// The SSTable catalog
    sstables: Arc<RwLock<Vec<Arc<SSTable>>>>,
}

impl QueryRouter {
    /// Creates a new query router
    pub fn new(memtable: Arc<RwLock<MemTable>>, sstables: Arc<RwLock<Vec<Arc<SSTable>>>>) -> Self {
        Self {
            memtable,
            sstables,
        }
    }

    /// Routes a query to appropriate storage components
    pub async fn route_query(&self, query: &Query) -> Vec<DataPoint> {
        let mut results = Vec::new();
        let mut seen_timestamps = HashSet::new();

        // First, check MemTable for more recent data
        let memtable = self.memtable.read().await;
        let memtable_points = if let Some(series_name) = &query.series_name {
            memtable.get_series_range(series_name, query.time_range.start, query.time_range.end).await
        } else {
            memtable.get_range(query.time_range.start, query.time_range.end).await
                .into_iter()
                .map(|(_, point)| point)
                .collect()
        };
        
        // Add MemTable points first
        for point in memtable_points {
            if query.time_range.contains(point.timestamp()) {
                seen_timestamps.insert(point.timestamp());
                results.push(point);
            }
        }

        // Then check SSTables for older data
        let sstables = self.sstables.read().await;
        for sstable in sstables.iter() {
            for block in sstable.scan_blocks().await {
                if block.start_timestamp <= query.time_range.end {
                    let mut current_timestamp = block.start_timestamp;
                    let filtered_points = block.timestamp_deltas.iter()
                        .zip(block.values.iter())
                        .zip(block.series_names.iter())
                        .filter_map(|((&delta, &value), series_name)| {
                            current_timestamp += delta;
                            if query.time_range.contains(current_timestamp) &&
                               query.series_name.as_ref().map_or(true, |name| series_name == name) &&
                               !seen_timestamps.contains(&current_timestamp) {
                                seen_timestamps.insert(current_timestamp);
                                Some(DataPoint::new(current_timestamp, value, HashMap::new()))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    results.extend(filtered_points);
                }
            }
        }

        // Sort results by timestamp
        results.sort_by_key(|point| point.timestamp());
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_time_range_overlap() {
        let range1 = TimeRange::new(0, 100);
        let range2 = TimeRange::new(50, 150);
        let range3 = TimeRange::new(101, 200);
        let range4 = TimeRange::new(-50, 50);

        assert!(range1.overlaps(&range2));
        assert!(range2.overlaps(&range1));
        assert!(!range1.overlaps(&range3));
        assert!(!range3.overlaps(&range1));
        assert!(range1.overlaps(&range4));
        assert!(range4.overlaps(&range1));
    }

    #[test]
    fn test_time_range_contains() {
        let range = TimeRange::new(0, 100);

        assert!(range.contains(0));
        assert!(range.contains(50));
        assert!(range.contains(100));
        assert!(!range.contains(-1));
        assert!(!range.contains(101));
    }

    #[tokio::test]
    async fn test_multi_level_search() {
        // Create a temporary directory for SSTables
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join("test.sst");

        // Create a MemTable with some data
        let memtable = Arc::new(RwLock::new(MemTable::new(1000)));
        let mut memtable_guard = memtable.write().await;
        
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        memtable_guard.insert(&series, &DataPoint::new(150, 1.0, HashMap::new())).await.unwrap();
        memtable_guard.insert(&series, &DataPoint::new(200, 2.0, HashMap::new())).await.unwrap();
        drop(memtable_guard);

        // Create an SSTable with older data
        let sstable = SSTable::new(&sstable_path).unwrap();
        let block = DataBlock {
            start_timestamp: 100,
            timestamp_deltas: vec![0, 50],
            values: vec![0.5, 1.5],
            series_names: vec!["test_series".to_string(), "test_series".to_string()],
            tags: vec![HashMap::new(), HashMap::new()],
        };
        sstable.write_block(block).await.unwrap();

        // Create query router
        let sstables = Arc::new(RwLock::new(vec![Arc::new(sstable)]));
        let router = QueryRouter::new(memtable, sstables);

        // Query that spans both MemTable and SSTable
        let query = Query::with_series(90, 210, "test_series".to_string());
        let results = router.route_query(&query).await;

        // Verify results
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].timestamp(), 100);
        assert_eq!(results[0].value(), 0.5);
        assert_eq!(results[1].timestamp(), 150);
        assert_eq!(results[1].value(), 1.0); // From MemTable, not SSTable
        assert_eq!(results[2].timestamp(), 200);
        assert_eq!(results[2].value(), 2.0);
    }

    #[tokio::test]
    async fn test_point_query_accuracy() {
        // Create a temporary directory for SSTables
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join("test.sst");

        // Create a MemTable with some data
        let memtable = Arc::new(RwLock::new(MemTable::new(1000)));
        let mut memtable_guard = memtable.write().await;
        
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        memtable_guard.insert(&series, &DataPoint::new(150, 1.0, HashMap::new())).await.unwrap();
        memtable_guard.insert(&series, &DataPoint::new(200, 2.0, HashMap::new())).await.unwrap();
        drop(memtable_guard);

        // Create an SSTable with older data
        let sstable = SSTable::new(&sstable_path).unwrap();
        let block = DataBlock {
            start_timestamp: 100,
            timestamp_deltas: vec![0, 50],
            values: vec![0.5, 1.5],
            series_names: vec!["test_series".to_string(), "test_series".to_string()],
            tags: vec![HashMap::new(), HashMap::new()],
        };
        sstable.write_block(block).await.unwrap();

        // Create query router
        let sstables = Arc::new(RwLock::new(vec![Arc::new(sstable)]));
        let router = QueryRouter::new(memtable, sstables);

        // Test exact point queries
        let query1 = Query::with_series(150, 150, "test_series".to_string());
        let results1 = router.route_query(&query1).await;
        assert_eq!(results1.len(), 1);
        assert_eq!(results1[0].timestamp(), 150);
        assert_eq!(results1[0].value(), 1.0);

        let query2 = Query::with_series(100, 100, "test_series".to_string());
        let results2 = router.route_query(&query2).await;
        assert_eq!(results2.len(), 1);
        assert_eq!(results2[0].timestamp(), 100);
        assert_eq!(results2[0].value(), 0.5);

        // Test non-existent point
        let query3 = Query::with_series(300, 300, "test_series".to_string());
        let results3 = router.route_query(&query3).await;
        assert!(results3.is_empty());
    }

    #[tokio::test]
    async fn test_range_query_completeness() {
        // Create a temporary directory for SSTables
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join("test.sst");

        // Create a MemTable with some data
        let memtable = Arc::new(RwLock::new(MemTable::new(1000)));
        let mut memtable_guard = memtable.write().await;
        
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        memtable_guard.insert(&series, &DataPoint::new(150, 1.0, HashMap::new())).await.unwrap();
        memtable_guard.insert(&series, &DataPoint::new(200, 2.0, HashMap::new())).await.unwrap();
        drop(memtable_guard);

        // Create an SSTable with older data
        let sstable = SSTable::new(&sstable_path).unwrap();
        let block = DataBlock {
            start_timestamp: 100,
            timestamp_deltas: vec![0, 50],
            values: vec![0.5, 1.5],
            series_names: vec!["test_series".to_string(), "test_series".to_string()],
            tags: vec![HashMap::new(), HashMap::new()],
        };
        sstable.write_block(block).await.unwrap();

        // Create query router
        let sstables = Arc::new(RwLock::new(vec![Arc::new(sstable)]));
        let router = QueryRouter::new(memtable, sstables);

        // Test complete range query
        let query = Query::with_series(90, 210, "test_series".to_string());
        let results = router.route_query(&query).await;

        // Verify all points are present and in order
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].timestamp(), 100);
        assert_eq!(results[0].value(), 0.5);
        assert_eq!(results[1].timestamp(), 150);
        assert_eq!(results[1].value(), 1.0);
        assert_eq!(results[2].timestamp(), 200);
        assert_eq!(results[2].value(), 2.0);

        // Test partial range query
        let query2 = Query::with_series(120, 170, "test_series".to_string());
        let results2 = router.route_query(&query2).await;
        assert_eq!(results2.len(), 1);
        assert_eq!(results2[0].timestamp(), 150);
        assert_eq!(results2[0].value(), 1.0);
    }

    #[tokio::test]
    async fn test_index_update_consistency() {
        // Create a temporary directory for SSTables
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join("test.sst");

        // Create a MemTable with some data
        let memtable = Arc::new(RwLock::new(MemTable::new(1000)));
        let mut memtable_guard = memtable.write().await;
        
        let series = TimeSeries::new("test_series".to_string()).unwrap();
        memtable_guard.insert(&series, &DataPoint::new(150, 1.0, HashMap::new())).await.unwrap();
        memtable_guard.insert(&series, &DataPoint::new(200, 2.0, HashMap::new())).await.unwrap();
        drop(memtable_guard);

        // Create an SSTable with older data
        let sstable = SSTable::new(&sstable_path).unwrap();
        let block = DataBlock {
            start_timestamp: 100,
            timestamp_deltas: vec![0, 50],
            values: vec![0.5, 1.5],
            series_names: vec!["test_series".to_string(), "test_series".to_string()],
            tags: vec![HashMap::new(), HashMap::new()],
        };
        sstable.write_block(block).await.unwrap();

        // Create query router
        let sstables = Arc::new(RwLock::new(vec![Arc::new(sstable)]));
        let router = QueryRouter::new(memtable, sstables);

        // Test initial state
        let query1 = Query::with_series(90, 210, "test_series".to_string());
        let results1 = router.route_query(&query1).await;
        assert_eq!(results1.len(), 3);

        // Add new data to MemTable
        let mut memtable_guard = router.memtable.write().await;
        memtable_guard.insert(&series, &DataPoint::new(250, 3.0, HashMap::new())).await.unwrap();
        drop(memtable_guard);

        // Verify new data is immediately available
        let query2 = Query::with_series(90, 260, "test_series".to_string());
        let results2 = router.route_query(&query2).await;
        assert_eq!(results2.len(), 4);
        assert_eq!(results2[3].timestamp(), 250);
        assert_eq!(results2[3].value(), 3.0);

        // Add new SSTable
        let sstable2 = SSTable::new(&temp_dir.path().join("test2.sst")).unwrap();
        let block2 = DataBlock {
            start_timestamp: 300,
            timestamp_deltas: vec![0, 50],
            values: vec![4.0, 5.0],
            series_names: vec!["test_series".to_string(), "test_series".to_string()],
            tags: vec![HashMap::new(), HashMap::new()],
        };
        sstable2.write_block(block2).await.unwrap();

        // Update SSTable list
        let mut sstables_guard = router.sstables.write().await;
        sstables_guard.push(Arc::new(sstable2));
        drop(sstables_guard);

        // Verify new SSTable data is available
        let query3 = Query::with_series(90, 360, "test_series".to_string());
        let results3 = router.route_query(&query3).await;
        assert_eq!(results3.len(), 6);
        assert_eq!(results3[4].timestamp(), 300);
        assert_eq!(results3[4].value(), 4.0);
        assert_eq!(results3[5].timestamp(), 350);
        assert_eq!(results3[5].value(), 5.0);
    }
} 