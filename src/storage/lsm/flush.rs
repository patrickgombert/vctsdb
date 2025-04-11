use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{info};


use crate::storage::lsm::memtable::MemTable;
use crate::storage::lsm::sstable::{SSTable, SSTableError, DataBlock};

/// Error type for flush operations
#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SSTable error: {0}")]
    SSTable(#[from] SSTableError),
    #[error("Flush already in progress")]
    FlushInProgress,
    #[error("Flush failed: {0}")]
    FlushFailed(String),
}

/// Manages the process of flushing MemTables to SSTables
pub struct FlushManager {
    /// Path where SSTables are stored
    sstable_dir: PathBuf,
    /// Current flush task if one is running
    flush_task: Option<JoinHandle<Result<(), FlushError>>>,
}

impl FlushManager {
    /// Creates a new FlushManager
    pub fn new(sstable_dir: PathBuf) -> Self {
        Self {
            sstable_dir,
            flush_task: None,
        }
    }

    /// Starts a background flush of the given MemTable to an SSTable
    pub async fn start_flush(
        &mut self,
        memtable: Arc<RwLock<MemTable>>,
    ) -> Result<(), FlushError> {
        // Check if a flush is already in progress
        if self.flush_task.is_some() {
            return Err(FlushError::FlushInProgress);
        }

        // Create a new SSTable for this flush
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let sstable_path = self.sstable_dir.join(format!("{}.sst", timestamp));
        let sstable = SSTable::new(&sstable_path)?;

        // Start the flush task
        let task = tokio::spawn(async move {
            // Take a read lock on the MemTable
            let memtable_guard = memtable.read().await;
            let data = memtable_guard.get_data().await;
            
            // Create a new empty MemTable for atomic swap
            let new_memtable = MemTable::new(memtable_guard.capacity());
            
            // Write all data points to the SSTable
            for (series_name, points) in data {
                let mut start_timestamp = i64::MAX;
                let mut timestamp_deltas = Vec::new();
                let mut values = Vec::new();
                let mut tags = Vec::new();


                // Process points to create a DataBlock
                for point in &points {
                    if start_timestamp == i64::MAX {
                        start_timestamp = point.timestamp();
                    } else {
                        timestamp_deltas.push(point.timestamp() - start_timestamp);
                    }
                    values.push(point.value());
                    tags.push(point.tags().clone());
                }

                let block = DataBlock {
                    start_timestamp,
                    timestamp_deltas,
                    values,
                    series_names: vec![series_name],
                    tags,
                };
                sstable.write_block(block).await?;
            }

            // Atomically swap the MemTables
            drop(memtable_guard);
            let mut memtable_guard = memtable.write().await;
            *memtable_guard = new_memtable;

            info!("Successfully flushed MemTable to {}", sstable_path.display());
            Ok(())
        });

        self.flush_task = Some(task);
        Ok(())
    }

    /// Checks if a flush is in progress
    pub fn is_flushing(&self) -> bool {
        self.flush_task.is_some()
    }

    /// Waits for the current flush to complete and returns the result
    pub async fn wait_for_flush(&mut self) -> Result<(), FlushError> {
        if let Some(task) = self.flush_task.take() {
            task.await.map_err(|e| FlushError::FlushFailed(e.to_string()))?
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::RwLock;
    use crate::storage::TimeSeries;
    use crate::storage::DataPoint;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_flush_process() {
        let temp_dir = tempdir().unwrap();
        let mut flush_manager = FlushManager::new(temp_dir.path().to_path_buf());
        let memtable = Arc::new(RwLock::new(MemTable::new(1000)));

        // Add some test data
        {
            let series = TimeSeries::new("test_series".to_string()).unwrap();
            let point = DataPoint::new(1000, 42.0, HashMap::new());
            memtable.write().await.insert(&series, &point).await.unwrap();
        }

        // Start flush
        flush_manager.start_flush(memtable.clone()).await.unwrap();
        assert!(flush_manager.is_flushing());

        // Wait for flush to complete
        flush_manager.wait_for_flush().await.unwrap();
        assert!(!flush_manager.is_flushing());

        // Verify MemTable is empty after flush
        let memtable_guard = memtable.read().await;
        assert!(memtable_guard.is_empty().await);
    }

    #[tokio::test]
    async fn test_concurrent_flush_prevention() {
        let temp_dir = tempdir().unwrap();
        let mut flush_manager = FlushManager::new(temp_dir.path().to_path_buf());
        let memtable = Arc::new(RwLock::new(MemTable::new(1000)));

        // Start first flush
        flush_manager.start_flush(memtable.clone()).await.unwrap();

        // Try to start another flush while one is in progress
        let result = flush_manager.start_flush(memtable.clone()).await;
        assert!(matches!(result, Err(FlushError::FlushInProgress)));
    }
} 