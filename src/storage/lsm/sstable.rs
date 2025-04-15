use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Magic number for SSTable files
const SSTABLE_MAGIC: u32 = 0x53535442; // "SSTB"
/// Current version of the SSTable format
const SSTABLE_VERSION: u32 = 1;

/// Represents a single block of data in the SSTable
#[derive(Debug, Clone)]
pub struct DataBlock {
    /// Starting timestamp of this block
    pub start_timestamp: i64,
    /// Delta-encoded timestamps
    pub timestamp_deltas: Vec<i64>,
    /// Values corresponding to each timestamp
    pub values: Vec<f64>,
    /// Series names for each point
    pub series_names: Vec<String>,
    /// Tags for each point
    pub tags: Vec<HashMap<String, String>>,
}

/// Represents the metadata for an SSTable
#[derive(Debug)]
pub struct SSTableMetadata {
    /// Total number of points in the table
    pub point_count: u64,
    /// Minimum timestamp in the table
    pub min_timestamp: i64,
    /// Maximum timestamp in the table
    pub max_timestamp: i64,
    /// Series names present in the table
    pub series_names: Vec<String>,
    /// Block metadata
    pub blocks: Vec<BlockMetadata>,
}

/// Metadata for a single block
#[derive(Debug)]
pub struct BlockMetadata {
    /// File offset where the block starts
    pub offset: u64,
    /// Number of points in the block
    pub point_count: u32,
    /// Starting timestamp of the block
    pub start_timestamp: i64,
}

/// The on-disk storage format for time series data
pub struct SSTable {
    /// Path to the SSTable file
    pub path: PathBuf,
    /// Metadata about the SSTable
    pub metadata: Arc<RwLock<SSTableMetadata>>,
    /// File handle for reading/writing
    file: Arc<RwLock<File>>,
}

impl fmt::Debug for SSTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SSTable")
            .field("path", &self.path)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl SSTable {
    /// Creates a new SSTable at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, SSTableError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        // Write file header
        file.write_all(&SSTABLE_MAGIC.to_le_bytes())?;
        file.write_all(&SSTABLE_VERSION.to_le_bytes())?;
        file.flush()?;

        // Initialize metadata
        let metadata = SSTableMetadata {
            point_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            series_names: Vec::new(),
            blocks: Vec::new(),
        };

        Ok(Self {
            path,
            metadata: Arc::new(RwLock::new(metadata)),
            file: Arc::new(RwLock::new(file)),
        })
    }

    /// Opens an existing SSTable at the specified path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SSTableError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Read and verify file header
        let mut magic_bytes = [0u8; 4];
        file.read_exact(&mut magic_bytes)?;
        let magic = u32::from_le_bytes(magic_bytes);
        if magic != SSTABLE_MAGIC {
            return Err(SSTableError::InvalidMagic);
        }

        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != SSTABLE_VERSION {
            return Err(SSTableError::UnsupportedVersion(version));
        }

        // Seek to the end to get the file size
        let _file_size = file.seek(std::io::SeekFrom::End(0))?;

        // Initialize metadata
        let metadata = SSTableMetadata {
            point_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            series_names: Vec::new(),
            blocks: Vec::new(),
        };

        Ok(Self {
            path,
            metadata: Arc::new(RwLock::new(metadata)),
            file: Arc::new(RwLock::new(file)),
        })
    }

    /// Writes a block of data to the SSTable
    pub async fn write_block(&self, block: DataBlock) -> Result<(), SSTableError> {
        let mut file_guard = self.file.write().await;
        let mut metadata_guard = self.metadata.write().await;

        // Get current position for block offset
        let offset = file_guard.stream_position()?;

        // Update metadata
        metadata_guard.point_count += block.timestamp_deltas.len() as u64;
        metadata_guard.min_timestamp = metadata_guard.min_timestamp.min(block.start_timestamp);
        metadata_guard.max_timestamp = metadata_guard
            .max_timestamp
            .max(block.start_timestamp + block.timestamp_deltas.last().unwrap_or(&0));

        // Update series names in metadata
        for series_name in &block.series_names {
            if !metadata_guard.series_names.contains(series_name) {
                metadata_guard.series_names.push(series_name.clone());
            }
        }

        // Write block metadata
        let block_metadata = BlockMetadata {
            offset,
            point_count: block.timestamp_deltas.len() as u32,
            start_timestamp: block.start_timestamp,
        };
        metadata_guard.blocks.push(block_metadata);

        // Write block data
        self.write_block_data(&mut file_guard, &block)?;
        file_guard.flush()?;

        Ok(())
    }

    /// Writes the actual block data to the file
    fn write_block_data(&self, file: &mut File, block: &DataBlock) -> Result<(), SSTableError> {
        // Write block header
        file.write_all(&block.start_timestamp.to_le_bytes())?;
        file.write_all(&(block.timestamp_deltas.len() as u32).to_le_bytes())?;

        // Write delta-encoded timestamps
        for delta in &block.timestamp_deltas {
            file.write_all(&delta.to_le_bytes())?;
        }

        // Write values
        for value in &block.values {
            file.write_all(&value.to_le_bytes())?;
        }

        // Write series names
        for name in &block.series_names {
            let name_bytes = name.as_bytes();
            file.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
            file.write_all(name_bytes)?;
        }

        // Write tags
        for tags in &block.tags {
            let tags_json = serde_json::to_vec(tags)?;
            file.write_all(&(tags_json.len() as u32).to_le_bytes())?;
            file.write_all(&tags_json)?;
        }

        // Flush to ensure all data is written
        file.flush()?;

        Ok(())
    }

    /// Reads a block of data from the SSTable
    pub async fn read_block(&self, block_index: usize) -> Result<DataBlock, SSTableError> {
        let metadata_guard = self.metadata.read().await;
        let mut file_guard = self.file.write().await;

        let block_metadata = metadata_guard
            .blocks
            .get(block_index)
            .ok_or(SSTableError::InvalidBlockIndex)?;

        // Seek to block start
        file_guard.seek(std::io::SeekFrom::Start(block_metadata.offset))?;

        // Read block data
        self.read_block_data(&mut file_guard, block_metadata.point_count)
    }

    /// Reads the actual block data from the file
    fn read_block_data(
        &self,
        file: &mut File,
        point_count: u32,
    ) -> Result<DataBlock, SSTableError> {
        // Read block header
        let mut start_timestamp_bytes = [0u8; 8];
        file.read_exact(&mut start_timestamp_bytes)?;
        let start_timestamp = i64::from_le_bytes(start_timestamp_bytes);

        let mut count_bytes = [0u8; 4];
        file.read_exact(&mut count_bytes)?;
        let actual_point_count = u32::from_le_bytes(count_bytes);

        // Verify point count matches metadata
        if actual_point_count != point_count {
            return Err(SSTableError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Point count mismatch",
            )));
        }

        // Read delta-encoded timestamps
        let mut timestamp_deltas = Vec::with_capacity(point_count as usize);
        for _ in 0..point_count {
            let mut delta_bytes = [0u8; 8];
            file.read_exact(&mut delta_bytes)?;
            timestamp_deltas.push(i64::from_le_bytes(delta_bytes));
        }

        // Read values
        let mut values = Vec::with_capacity(point_count as usize);
        for _ in 0..point_count {
            let mut value_bytes = [0u8; 8];
            file.read_exact(&mut value_bytes)?;
            values.push(f64::from_le_bytes(value_bytes));
        }

        // Read series names
        let mut series_names = Vec::with_capacity(point_count as usize);
        for _ in 0..point_count {
            let mut len_bytes = [0u8; 4];
            file.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut name_bytes = vec![0u8; len];
            file.read_exact(&mut name_bytes)?;
            series_names.push(String::from_utf8(name_bytes)?);
        }

        // Read tags
        let mut tags = Vec::with_capacity(point_count as usize);
        for _ in 0..point_count {
            let mut len_bytes = [0u8; 4];
            file.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut tag_bytes = vec![0u8; len];
            file.read_exact(&mut tag_bytes)?;
            tags.push(serde_json::from_slice(&tag_bytes)?);
        }

        Ok(DataBlock {
            start_timestamp,
            timestamp_deltas,
            values,
            series_names,
            tags,
        })
    }

    /// Scans all blocks in the SSTable
    pub async fn scan_blocks(&self) -> Vec<DataBlock> {
        let metadata_guard = self.metadata.read().await;
        let mut blocks = Vec::new();
        
        for (i, _) in metadata_guard.blocks.iter().enumerate() {
            if let Ok(block) = self.read_block(i).await {
                blocks.push(block);
            }
        }
        
        blocks
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SSTableError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid block index")]
    InvalidBlockIndex,
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid SSTable magic number")]
    InvalidMagic,
    #[error("Unsupported SSTable version: {0}")]
    UnsupportedVersion(u32),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sstable_write_and_read() {
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join("test.sst");
        let sstable = SSTable::new(&sstable_path).unwrap();

        // Create a test block
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        let block = DataBlock {
            start_timestamp: 1000,
            timestamp_deltas: vec![0, 1, 2],
            values: vec![42.0, 43.0, 44.0],
            series_names: vec!["test_series".to_string(); 3],
            tags: vec![tags.clone(); 3],
        };

        // Write the block
        sstable.write_block(block).await.unwrap();

        // Read the block back
        let read_block = sstable.read_block(0).await.unwrap();

        // Verify the data
        assert_eq!(read_block.start_timestamp, 1000);
        assert_eq!(read_block.timestamp_deltas, vec![0, 1, 2]);
        assert_eq!(read_block.values, vec![42.0, 43.0, 44.0]);
        assert_eq!(read_block.series_names, vec!["test_series"; 3]);
        assert_eq!(read_block.tags, vec![tags; 3]);
    }

    #[tokio::test]
    async fn test_sstable_versioning() {
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join("test.sst");

        // Create a new SSTable
        let sstable = SSTable::new(&sstable_path).unwrap();
        drop(sstable);

        // Try to open it
        let sstable = SSTable::open(&sstable_path).unwrap();
        drop(sstable);

        // Try to open a non-existent file
        let bad_path = temp_dir.path().join("nonexistent.sst");
        assert!(matches!(SSTable::open(&bad_path), Err(SSTableError::Io(_))));

        // Try to open a file with invalid magic
        let invalid_path = temp_dir.path().join("invalid.sst");
        let mut file = File::create(&invalid_path).unwrap();
        file.write_all(&0u32.to_le_bytes()).unwrap();
        file.write_all(&SSTABLE_VERSION.to_le_bytes()).unwrap();
        drop(file);
        assert!(matches!(
            SSTable::open(&invalid_path),
            Err(SSTableError::InvalidMagic)
        ));

        // Try to open a file with unsupported version
        let unsupported_path = temp_dir.path().join("unsupported.sst");
        let mut file = File::create(&unsupported_path).unwrap();
        file.write_all(&SSTABLE_MAGIC.to_le_bytes()).unwrap();
        file.write_all(&99u32.to_le_bytes()).unwrap();
        drop(file);
        assert!(matches!(
            SSTable::open(&unsupported_path),
            Err(SSTableError::UnsupportedVersion(99))
        ));
    }
}
