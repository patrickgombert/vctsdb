use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::storage::data::{DataPoint, TimeSeries};

const WAL_MAGIC: u32 = 0x57414C00; // "WAL\0"
const WAL_VERSION: u32 = 1;
const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB
const DEFAULT_SEGMENT_DURATION: u64 = 24 * 60 * 60; // 24 hours

#[derive(Error, Debug)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Invalid WAL file: {0}")]
    InvalidFile(String),
    #[error("CRC mismatch")]
    CrcMismatch,
    #[error("Segment rotation error: {0}")]
    RotationError(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct WalHeader {
    magic: u32,
    version: u32,
    created_at: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WalEntry {
    series_name: String,
    timestamp: i64,
    value: f64,
    tags: std::collections::HashMap<String, String>,
    crc: u32,
}

/// Represents a WAL segment file
#[derive(Debug)]
struct Segment {
    path: PathBuf,
    size: u64,
    created_at: u64,
}

impl Segment {
    fn new(path: PathBuf) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Get initial file size
        let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        Self {
            path,
            size,
            created_at: now,
        }
    }

    fn update_size(&mut self) -> io::Result<()> {
        self.size = fs::metadata(&self.path)?.len();
        println!("updated size: {:?}", self.size);
        Ok(())
    }

    fn is_full(&self, max_size: u64) -> bool {
        self.size >= max_size
    }

    fn is_expired(&self, max_age: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now - self.created_at >= max_age
    }
}

/// Manages the Write-Ahead Log
pub struct WriteAheadLog {
    directory: PathBuf,
    current_segment: Arc<RwLock<Option<Segment>>>,
    max_segment_size: u64,
    max_segment_age: u64,
    crc: Crc<u32>,
}

impl WriteAheadLog {
    /// Creates a new WAL in the specified directory
    pub fn new<P: AsRef<Path>>(directory: P) -> Result<Self, WalError> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;

        Ok(Self {
            directory,
            current_segment: Arc::new(RwLock::new(None)),
            max_segment_size: DEFAULT_SEGMENT_SIZE,
            max_segment_age: DEFAULT_SEGMENT_DURATION,
            crc: Crc::<u32>::new(&CRC_32_ISCSI),
        })
    }

    /// Sets the maximum size for WAL segments
    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    /// Sets the maximum age for WAL segments
    pub fn with_max_segment_age(mut self, age: u64) -> Self {
        self.max_segment_age = age;
        self
    }

    /// Writes a data point to the WAL
    pub async fn write(&self, series: &TimeSeries, point: &DataPoint) -> Result<(), WalError> {
        let mut segment_guard = self.current_segment.write().await;

        // Create new segment if needed
        if segment_guard.is_none() {
            *segment_guard = Some(self.rotate_segment()?);
        }

        // Check if we need to rotate
        let segment = segment_guard.as_ref().unwrap();
        let needs_rotation =
            segment.is_full(self.max_segment_size) || segment.is_expired(self.max_segment_age);

        if needs_rotation {
            *segment_guard = Some(self.rotate_segment()?);
        }

        // Write to the current segment
        let segment = segment_guard.as_mut().unwrap();
        self.write_entry(series.name(), point, &segment.path)?;
        segment.update_size()?;

        Ok(())
    }

    /// Rotates the current segment and creates a new one
    fn rotate_segment(&self) -> Result<Segment, WalError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let filename = format!("segment_{}_{}.wal", timestamp, Uuid::new_v4());
        let path = self.directory.join(filename);

        // Create new segment file with header
        let file = OpenOptions::new().write(true).create(true).open(&path)?;

        let header = WalHeader {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            created_at: timestamp,
        };

        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &header)?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        Ok(Segment::new(path))
    }

    /// Writes a single entry to the WAL file
    fn write_entry(
        &self,
        series_name: &str,
        point: &DataPoint,
        path: &Path,
    ) -> Result<(), WalError> {
        let entry = WalEntry {
            series_name: series_name.to_string(),
            timestamp: point.timestamp(),
            value: point.value(),
            tags: point.tags().clone(),
            crc: 0, // Will be calculated below
        };

        let mut writer = BufWriter::new(OpenOptions::new().append(true).open(path)?);

        // Write entry without CRC
        let entry_json = serde_json::to_string(&entry)?;
        writer.write_all(entry_json.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        // Calculate and write CRC
        let mut digest = self.crc.digest();
        digest.update(&entry_json.as_bytes());
        let crc = digest.finalize();

        writer.write_all(&crc.to_le_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        Ok(())
    }

    /// Reads and validates a WAL entry
    fn read_entry<R: Read>(reader: &mut BufReader<R>) -> Result<WalEntry, WalError> {
        let mut line = String::new();
        reader.read_line(&mut line)?;

        if line.trim().is_empty() {
            return Err(WalError::InvalidFile("Empty line".to_string()));
        }

        let entry: WalEntry = serde_json::from_str(line.trim())?;

        // Read and verify CRC
        let mut crc_bytes = [0u8; 4];
        reader.read_exact(&mut crc_bytes)?;
        let expected_crc = u32::from_le_bytes(crc_bytes);

        // Skip newline after CRC
        let mut newline = [0u8; 1];
        reader.read_exact(&mut newline)?;

        let entry_json = serde_json::to_string(&entry)?;
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&entry_json.as_bytes());
        let actual_crc = digest.finalize();

        if actual_crc != expected_crc {
            return Err(WalError::CrcMismatch);
        }

        Ok(entry)
    }
}

impl fmt::Debug for WriteAheadLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let current_segment = self
            .current_segment
            .try_read()
            .map(|guard| {
                guard
                    .as_ref()
                    .map(|segment| {
                        format!(
                            "Segment {{ path: {:?}, size: {} bytes, created_at: {} }}",
                            segment.path, segment.size, segment.created_at
                        )
                    })
                    .unwrap_or_else(|| "None".to_string())
            })
            .unwrap_or_else(|_| "Locked".to_string());

        f.debug_struct("WriteAheadLog")
            .field("directory", &self.directory)
            .field("current_segment", &current_segment)
            .field(
                "max_segment_size",
                &format!("{} bytes", self.max_segment_size),
            )
            .field(
                "max_segment_age",
                &format!("{} seconds", self.max_segment_age),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::test;

    #[test]
    async fn test_wal_creation_and_write() {
        let dir = tempdir().unwrap();
        let wal = WriteAheadLog::new(dir.path()).unwrap();

        let series = TimeSeries::new("test_series".to_string()).unwrap();
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        let point = DataPoint::new(1000, 42.0, tags);

        wal.write(&series, &point).await.unwrap();

        // Verify segment was created
        let segment = wal.current_segment.read().await;
        assert!(segment.is_some());
        assert!(segment.as_ref().unwrap().path.exists());
    }

    #[test]
    async fn test_wal_segment_rotation() {
        let dir = tempdir().unwrap();
        let wal = WriteAheadLog::new(dir.path())
            .unwrap()
            .with_max_segment_size(50) // Very small size to trigger rotation
            .with_max_segment_age(3600);

        let series = TimeSeries::new("test_series".to_string()).unwrap();
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        // Write enough data to trigger rotation
        for i in 0..20 {
            let point = DataPoint::new(i, i as f64, tags.clone());
            wal.write(&series, &point).await.unwrap();
        }

        // Verify multiple segments were created
        let entries: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
            .collect();

        assert!(
            entries.len() > 1,
            "Expected multiple segments, got {}",
            entries.len()
        );
    }

    #[test]
    async fn test_wal_entry_validation() {
        let dir = tempdir().unwrap();
        let wal = WriteAheadLog::new(dir.path()).unwrap();

        let series = TimeSeries::new("test_series".to_string()).unwrap();
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        let point = DataPoint::new(1000, 42.0, tags);

        wal.write(&series, &point).await.unwrap();

        // Verify entry can be read back
        let segment = wal.current_segment.read().await;
        let file = File::open(segment.as_ref().unwrap().path.clone()).unwrap();
        let mut reader = BufReader::new(file);

        // Skip header
        let mut header_line = String::new();
        reader.read_line(&mut header_line).unwrap();

        let entry = WriteAheadLog::read_entry(&mut reader).unwrap();
        assert_eq!(entry.series_name, "test_series");
        assert_eq!(entry.timestamp, 1000);
        assert_eq!(entry.value, 42.0);
        assert_eq!(entry.tags.get("host").unwrap(), "server1");
    }
}
