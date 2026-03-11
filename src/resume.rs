//! Resume support for interrupted transfers.
//!
//! Tracks completed file ranges in SQLite (WAL mode) for crash-safe resume.
//! One DB file per transfer (path = get_checkpoint_path(file_path)).

use crate::base::FileRange;
use crate::error::TransferError;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Checkpoint format version. Version 1 = no range hashes (treat completed as unverified on load).
/// Version 2 = has range_hashes (trust them after server verification).
pub const CHECKPOINT_VERSION_V1: u8 = 1;
pub const CHECKPOINT_VERSION_V2: u8 = 2;

fn default_checkpoint_version() -> u8 {
    CHECKPOINT_VERSION_V1
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS transfer (
    id              TEXT PRIMARY KEY,
    file_path       TEXT NOT NULL,
    file_size       INTEGER NOT NULL,
    checkpoint_version INTEGER NOT NULL,
    file_hash       BLOB
);
CREATE TABLE IF NOT EXISTS range_state (
    transfer_id     TEXT NOT NULL,
    range_start     INTEGER NOT NULL,
    range_end       INTEGER NOT NULL,
    blake3_hash     BLOB,
    verified        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (transfer_id, range_start)
);
";

fn open_checkpoint_db(path: &Path) -> Result<Connection, TransferError> {
    let conn = Connection::open(path).map_err(|e| TransferError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
    conn.execute_batch(SCHEMA)
        .map_err(|e| TransferError::ProtocolError(format!("SQLite schema: {}", e)))?;
    Ok(conn)
}

/// Transfer checkpoint that tracks completed ranges and optional per-range BLAKE3 hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferCheckpoint {
    /// Checkpoint format version. 1 = old (no range hashes). 2 = has range_hashes.
    #[serde(default = "default_checkpoint_version")]
    pub checkpoint_version: u8,
    /// File size in bytes.
    pub file_size: u64,
    /// Set of completed ranges (encoded as "start-end").
    pub completed_ranges: HashSet<String>,
    /// Per-range BLAKE3 hash; key = "start-end". Only used when checkpoint_version >= 2.
    #[serde(default)]
    pub range_hashes: HashMap<String, [u8; 32]>,
    /// Whole-file hash (set on full completion). Not used for resume verification.
    #[serde(default)]
    pub file_hash: Option<[u8; 32]>,
}

impl TransferCheckpoint {
    /// Create a new checkpoint (version 2 with range hashes).
    pub fn new(file_size: u64) -> Self {
        Self {
            checkpoint_version: CHECKPOINT_VERSION_V2,
            file_size,
            completed_ranges: HashSet::new(),
            range_hashes: HashMap::new(),
            file_hash: None,
        }
    }

    /// Mark a range as completed with its BLAKE3 hash (v2).
    pub fn mark_completed(&mut self, range: FileRange, range_hash: [u8; 32]) {
        let key = format!("{}-{}", range.start, range.end);
        self.completed_ranges.insert(key.clone());
        self.range_hashes.insert(key, range_hash);
    }

    /// Check if a range is completed.
    pub fn is_completed(&self, range: &FileRange) -> bool {
        let key = format!("{}-{}", range.start, range.end);
        self.completed_ranges.contains(&key)
    }

    /// Get all missing ranges that need to be transferred.
    pub fn get_missing_ranges(&self, all_ranges: &[FileRange]) -> Vec<FileRange> {
        all_ranges
            .iter()
            .filter(|range| !self.is_completed(range))
            .cloned()
            .collect()
    }

    /// Check if transfer is complete.
    pub fn is_complete(&self, total_ranges: usize) -> bool {
        self.completed_ranges.len() >= total_ranges
    }

    /// Save checkpoint to SQLite (WAL mode). Creates DB and tables if needed.
    pub fn save(&self, checkpoint_path: &Path) -> Result<(), TransferError> {
        let id = checkpoint_path.to_string_lossy();
        let conn = open_checkpoint_db(checkpoint_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .map_err(|e| TransferError::ProtocolError(format!("SQLite WAL: {}", e)))?;
        conn.execute(
            "INSERT OR REPLACE INTO transfer (id, file_path, file_size, checkpoint_version, file_hash) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                id.as_ref(),
                id.as_ref(),
                self.file_size as i64,
                self.checkpoint_version as i64,
                self.file_hash.as_ref().map(|h| h.as_slice()),
            ],
        )
        .map_err(|e| TransferError::ProtocolError(format!("SQLite transfer: {}", e)))?;
        conn.execute("DELETE FROM range_state WHERE transfer_id = ?1", [id.as_ref()])
            .map_err(|e| TransferError::ProtocolError(format!("SQLite delete ranges: {}", e)))?;
        let mut insert_range = conn.prepare(
            "INSERT INTO range_state (transfer_id, range_start, range_end, blake3_hash, verified) VALUES (?1, ?2, ?3, ?4, 1)",
        )
        .map_err(|e| TransferError::ProtocolError(format!("SQLite prepare: {}", e)))?;
        for (key, hash) in &self.range_hashes {
            let parts: Vec<&str> = key.splitn(2, '-').collect();
            if parts.len() != 2 {
                continue;
            }
            let start: i64 = parts[0].parse().map_err(|_| TransferError::ProtocolError("Invalid range key".to_string()))?;
            let end: i64 = parts[1].parse().map_err(|_| TransferError::ProtocolError("Invalid range key".to_string()))?;
            insert_range
                .execute(rusqlite::params![id.as_ref(), start, end, hash.as_slice()])
                .map_err(|e| TransferError::ProtocolError(format!("SQLite insert range: {}", e)))?;
        }
        Ok(())
    }

    /// Load checkpoint from SQLite. If the file is legacy JSON, load once and migrate to SQLite.
    /// Version 1 checkpoints: completed_ranges and range_hashes are cleared (unverified).
    pub fn load(checkpoint_path: &Path) -> Result<Self, TransferError> {
        if !checkpoint_path.exists() {
            return Err(TransferError::ProtocolError("Checkpoint file does not exist".to_string()));
        }
        let bytes = std::fs::read(checkpoint_path).map_err(|e| TransferError::Io(e))?;
        if bytes.first() == Some(&b'{') {
            let json = String::from_utf8(bytes).map_err(|e| TransferError::ProtocolError(format!("Invalid UTF-8: {}", e)))?;
            let mut checkpoint: Self = serde_json::from_str(&json)
                .map_err(|e| TransferError::ProtocolError(format!("Failed to deserialize checkpoint: {}", e)))?;
            if checkpoint.checkpoint_version == CHECKPOINT_VERSION_V1 {
                checkpoint.completed_ranges.clear();
                checkpoint.range_hashes.clear();
            }
            checkpoint.save(checkpoint_path)?;
            return Ok(checkpoint);
        }
        let id = checkpoint_path.to_string_lossy();
        let conn = Connection::open(checkpoint_path).map_err(|e| TransferError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let (file_size, checkpoint_version, file_hash): (i64, i64, Option<Vec<u8>>) = conn
            .query_row(
                "SELECT file_size, checkpoint_version, file_hash FROM transfer WHERE id = ?1",
                [id.as_ref()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                    ))
                },
            )
            .map_err(|e| TransferError::ProtocolError(format!("SQLite read transfer: {}", e)))?;
        let mut completed_ranges = HashSet::new();
        let mut range_hashes = HashMap::new();
        let mut range_stmt = conn
            .prepare("SELECT range_start, range_end, blake3_hash FROM range_state WHERE transfer_id = ?1")
            .map_err(|e| TransferError::ProtocolError(format!("SQLite prepare: {}", e)))?;
        let rows = range_stmt
            .query_map([id.as_ref()], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, Vec<u8>>(2)?))
            })
            .map_err(|e| TransferError::ProtocolError(format!("SQLite range query: {}", e)))?;
        for row in rows {
            let (start, end, hash): (i64, i64, Vec<u8>) = row.map_err(|e| TransferError::ProtocolError(format!("SQLite row: {}", e)))?;
            if hash.len() != 32 {
                continue;
            }
            let key = format!("{}-{}", start, end);
            completed_ranges.insert(key.clone());
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&hash);
            range_hashes.insert(key, arr);
        }
        let mut checkpoint = TransferCheckpoint {
            checkpoint_version: checkpoint_version as u8,
            file_size: file_size as u64,
            completed_ranges,
            range_hashes,
            file_hash: file_hash.and_then(|v| if v.len() == 32 { let mut a = [0u8; 32]; a.copy_from_slice(&v); Some(a) } else { None }),
        };
        if checkpoint.checkpoint_version == CHECKPOINT_VERSION_V1 {
            checkpoint.completed_ranges.clear();
            checkpoint.range_hashes.clear();
        }
        Ok(checkpoint)
    }

    /// Remove a range from completed (e.g. after server reported verification failed).
    pub fn remove_completed(&mut self, range: &FileRange) {
        let key = format!("{}-{}", range.start, range.end);
        self.completed_ranges.remove(&key);
        self.range_hashes.remove(&key);
    }

    /// Completed ranges in file order (by start), with their hashes. For 0x06 payload.
    pub fn completed_ranges_ordered(&self, all_ranges: &[FileRange]) -> Vec<(FileRange, [u8; 32])> {
        let mut out: Vec<_> = all_ranges
            .iter()
            .filter(|r| self.is_completed(r))
            .filter_map(|r| {
                let key = format!("{}-{}", r.start, r.end);
                self.range_hashes.get(&key).map(|h| (*r, *h))
            })
            .collect();
        out.sort_by_key(|(r, _)| r.start);
        out
    }
}

/// Get checkpoint file path for a given file.
pub fn get_checkpoint_path(file_path: &Path) -> PathBuf {
    let mut checkpoint_path = file_path.to_path_buf();
    checkpoint_path.set_extension("shift_checkpoint");
    checkpoint_path
}

/// Check if a checkpoint exists for a file.
pub fn checkpoint_exists(file_path: &Path) -> bool {
    get_checkpoint_path(file_path).exists()
}

/// Delete checkpoint file (SQLite DB or legacy JSON).
pub fn delete_checkpoint(file_path: &Path) -> Result<(), TransferError> {
    let checkpoint_path = get_checkpoint_path(file_path);
    if checkpoint_path.exists() {
        std::fs::remove_file(&checkpoint_path).map_err(|e| TransferError::Io(e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_creation() {
        let checkpoint = TransferCheckpoint::new(1000);
        assert_eq!(checkpoint.file_size, 1000);
        assert!(checkpoint.completed_ranges.is_empty());
    }

    #[test]
    fn test_checkpoint_mark_completed() {
        let mut checkpoint = TransferCheckpoint::new(1000);
        let range = FileRange::new(0, 100);
        checkpoint.mark_completed(range, [0u8; 32]);
        assert!(checkpoint.is_completed(&range));
    }

    #[test]
    fn test_checkpoint_get_missing_ranges() {
        let mut checkpoint = TransferCheckpoint::new(1000);
        let all_ranges = vec![
            FileRange::new(0, 250),
            FileRange::new(250, 500),
            FileRange::new(500, 750),
            FileRange::new(750, 1000),
        ];
        
        checkpoint.mark_completed(all_ranges[0], [0u8; 32]);
        checkpoint.mark_completed(all_ranges[2], [0u8; 32]);
        
        let missing = checkpoint.get_missing_ranges(&all_ranges);
        assert_eq!(missing.len(), 2);
        assert_eq!(missing[0], all_ranges[1]);
        assert_eq!(missing[1], all_ranges[3]);
    }

    #[test]
    fn test_checkpoint_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.bin");
        
        let mut checkpoint = TransferCheckpoint::new(1000);
        checkpoint.mark_completed(FileRange::new(0, 100), [1u8; 32]);
        
        let checkpoint_path = get_checkpoint_path(&file_path);
        checkpoint.save(&checkpoint_path).unwrap();
        
        let loaded = TransferCheckpoint::load(&checkpoint_path).unwrap();
        assert_eq!(loaded.file_size, 1000);
        assert_eq!(loaded.completed_ranges.len(), 1);
        assert_eq!(loaded.range_hashes.get("0-100"), Some(&[1u8; 32]));
    }
}

