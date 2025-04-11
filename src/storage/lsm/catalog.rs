use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::storage::lsm::sstable::{SSTable, SSTableError, DataBlock};

/// Represents metadata about an SSTable in the catalog
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    /// Path to the SSTable file
    pub path: PathBuf,
    /// Minimum timestamp in the table
    pub min_timestamp: i64,
    /// Maximum timestamp in the table
    pub max_timestamp: i64,
    /// Series names present in the table
    pub series_names: HashSet<String>,
    /// Total number of points in the table
    pub point_count: u64,
    /// Block metadata for efficient querying
    pub blocks: Vec<BlockInfo>,
}

/// Metadata for a single block in an SSTable
#[derive(Debug, Clone)]
pub struct BlockInfo {
    /// File offset where the block starts
    pub offset: u64,
    /// Number of points in the block
    pub point_count: u32,
    /// Starting timestamp of the block
    pub start_timestamp: i64,
    /// Series names present in this block
    pub series_names: HashSet<String>,
}

/// Manages a collection of SSTables and their metadata
pub struct SSTableCatalog {
    /// Directory where SSTables are stored
    base_dir: PathBuf,
    /// Map of SSTable IDs to their metadata
    tables: Arc<RwLock<HashMap<String, SSTableInfo>>>,
    /// Map of series names to SSTable IDs that contain them
    series_index: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl SSTableCatalog {
    /// Creates a new SSTable catalog in the specified directory
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            series_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Adds a new SSTable to the catalog
    pub async fn add_table(&self, table: &SSTable) -> Result<(), SSTableError> {
        let metadata = table.metadata.read().await;
        
        // Convert block metadata to BlockInfo
        let blocks = metadata.blocks.iter().map(|block| BlockInfo {
            offset: block.offset,
            point_count: block.point_count,
            start_timestamp: block.start_timestamp,
            series_names: HashSet::new(), // Will be populated during block reads
        }).collect();

        // Create SSTableInfo
        let info = SSTableInfo {
            path: table.path.clone(),
            min_timestamp: metadata.min_timestamp,
            max_timestamp: metadata.max_timestamp,
            series_names: metadata.series_names.iter().cloned().collect(),
            point_count: metadata.point_count,
            blocks,
        };

        let table_id = self.generate_table_id(&info);
        
        // Update the main table index
        let mut tables = self.tables.write().await;
        tables.insert(table_id.clone(), info.clone());

        // Update the series index
        let mut series_index = self.series_index.write().await;
        for series_name in &info.series_names {
            series_index
                .entry(series_name.clone())
                .or_insert_with(HashSet::new)
                .insert(table_id.clone());
        }

        debug!(
            "Added SSTable to catalog: id={}, path={}, points={}, series={}",
            table_id,
            table.path.display(),
            info.point_count,
            info.series_names.len()
        );

        Ok(())
    }

    /// Removes an SSTable from the catalog
    pub async fn remove_table(&self, table_id: &str) -> Result<(), SSTableError> {
        let mut tables = self.tables.write().await;
        let mut series_index = self.series_index.write().await;

        if let Some(info) = tables.remove(table_id) {
            // Remove the table from the series index
            for series_name in info.series_names {
                if let Some(tables) = series_index.get_mut(&series_name) {
                    tables.remove(table_id);
                    if tables.is_empty() {
                        series_index.remove(&series_name);
                    }
                }
            }

            debug!("Removed SSTable from catalog: id={}", table_id);
        }

        Ok(())
    }

    /// Returns all SSTables that contain data for the given time range
    pub async fn get_tables_in_range(&self, start: i64, end: i64) -> Vec<SSTableInfo> {
        let tables = self.tables.read().await;
        tables
            .values()
            .filter(|info| {
                // Check if the table's time range overlaps with the query range
                info.min_timestamp <= end && info.max_timestamp >= start
            })
            .cloned()
            .collect()
    }

    /// Returns all SSTables that contain data for the given series
    pub async fn get_tables_for_series(&self, series_name: &str) -> Vec<SSTableInfo> {
        let series_index = self.series_index.read().await;
        let tables = self.tables.read().await;

        if let Some(table_ids) = series_index.get(series_name) {
            table_ids
                .iter()
                .filter_map(|id| tables.get(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Returns all SSTables in the catalog
    pub async fn get_all_tables(&self) -> Vec<SSTableInfo> {
        let tables = self.tables.read().await;
        tables.values().cloned().collect()
    }

    /// Returns the total number of points across all SSTables
    pub async fn total_points(&self) -> u64 {
        let tables = self.tables.read().await;
        tables.values().map(|info| info.point_count).sum()
    }

    /// Returns the number of unique series across all SSTables
    pub async fn unique_series_count(&self) -> usize {
        let series_index = self.series_index.read().await;
        series_index.len()
    }

    /// Generates a unique ID for an SSTable based on its metadata
    fn generate_table_id(&self, info: &SSTableInfo) -> String {
        // Use a combination of min timestamp and path to generate a unique ID
        format!("{}_{}", info.min_timestamp, info.path.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    async fn create_test_sstable(path: &Path, series_names: Vec<String>, start_time: i64, point_count: u32) -> SSTable {
        let sstable = SSTable::new(path).unwrap();
        
        // Create test data block
        let mut timestamp_deltas = Vec::new();
        let mut values = Vec::new();
        let mut block_series_names = Vec::new();
        let mut tags = Vec::new();

        for i in 0..point_count {
            timestamp_deltas.push(i as i64);
            values.push(i as f64);
            block_series_names.push(series_names[0].clone());
            tags.push(HashMap::new());
        }

        let block = DataBlock {
            start_timestamp: start_time,
            timestamp_deltas,
            values,
            series_names: block_series_names,
            tags,
        };

        // Write the block
        sstable.write_block(block).await.unwrap();
        sstable
    }

    #[test]
    async fn test_catalog_add_and_remove() {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = SSTableCatalog::new(temp_dir.path());

        // Create a test SSTable with actual data
        let sstable_path = temp_dir.path().join("test.sst");
        let series_names = vec!["test_series".to_string()];
        let sstable = create_test_sstable(&sstable_path, series_names, 1000, 10).await;

        // Add the SSTable to the catalog
        catalog.add_table(&sstable).await.unwrap();

        // Verify the SSTable was added
        let tables = catalog.get_all_tables().await;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].point_count, 10);

        // Remove the SSTable
        let table_id = catalog.generate_table_id(&tables[0]);
        catalog.remove_table(&table_id).await.unwrap();

        // Verify the SSTable was removed
        let tables = catalog.get_all_tables().await;
        assert_eq!(tables.len(), 0);
    }

    #[test]
    async fn test_catalog_time_range_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = SSTableCatalog::new(temp_dir.path());

        // Create test SSTables with different time ranges
        let sstable1 = create_test_sstable(
            &temp_dir.path().join("table1.sst"),
            vec!["series1".to_string()],
            1000,
            10,
        ).await;

        let sstable2 = create_test_sstable(
            &temp_dir.path().join("table2.sst"),
            vec!["series2".to_string()],
            2000,
            10,
        ).await;

        // Add SSTables to catalog
        catalog.add_table(&sstable1).await.unwrap();
        catalog.add_table(&sstable2).await.unwrap();

        // Query for a specific time range
        let tables = catalog.get_tables_in_range(1000, 2000).await;
        assert_eq!(tables.len(), 2); // Both tables should be returned

        // Query for a non-overlapping range
        let tables = catalog.get_tables_in_range(3000, 4000).await;
        assert_eq!(tables.len(), 0);
    }

    #[test]
    async fn test_catalog_series_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = SSTableCatalog::new(temp_dir.path());

        // Create test SSTables with different series
        let sstable1 = create_test_sstable(
            &temp_dir.path().join("table1.sst"),
            vec!["test_series".to_string()],
            1000,
            10,
        ).await;

        let sstable2 = create_test_sstable(
            &temp_dir.path().join("table2.sst"),
            vec!["other_series".to_string()],
            2000,
            10,
        ).await;

        // Add SSTables to catalog
        catalog.add_table(&sstable1).await.unwrap();
        catalog.add_table(&sstable2).await.unwrap();

        // Query for a specific series
        let tables = catalog.get_tables_for_series("test_series").await;
        assert_eq!(tables.len(), 1);

        // Query for a non-existent series
        let tables = catalog.get_tables_for_series("nonexistent").await;
        assert_eq!(tables.len(), 0);
    }

    #[test]
    async fn test_catalog_metrics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = SSTableCatalog::new(temp_dir.path());

        // Create test SSTables with different series
        let sstable1 = create_test_sstable(
            &temp_dir.path().join("table1.sst"),
            vec!["series1".to_string()],
            1000,
            10,
        ).await;

        let sstable2 = create_test_sstable(
            &temp_dir.path().join("table2.sst"),
            vec!["series2".to_string()],
            2000,
            15,
        ).await;

        // Add SSTables to catalog
        catalog.add_table(&sstable1).await.unwrap();
        catalog.add_table(&sstable2).await.unwrap();

        // Verify metrics
        assert_eq!(catalog.total_points().await, 25); // 10 + 15 points
        assert_eq!(catalog.unique_series_count().await, 2); // series1 and series2
    }
} 