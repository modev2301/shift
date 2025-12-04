//! Resume support for interrupted transfers.
//!
//! Tracks completed file ranges to allow resuming transfers from where they left off.

use crate::base::FileRange;
use crate::error::TransferError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Transfer checkpoint that tracks completed ranges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferCheckpoint {
    /// File size in bytes.
    pub file_size: u64,
    /// Set of completed ranges (encoded as "start-end").
    pub completed_ranges: HashSet<String>,
    /// File hash for verification (optional).
    pub file_hash: Option<String>,
}

impl TransferCheckpoint {
    /// Create a new checkpoint.
    pub fn new(file_size: u64) -> Self {
        Self {
            file_size,
            completed_ranges: HashSet::new(),
            file_hash: None,
        }
    }

    /// Mark a range as completed.
    pub fn mark_completed(&mut self, range: FileRange) {
        let key = format!("{}-{}", range.start, range.end);
        self.completed_ranges.insert(key);
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

    /// Save checkpoint to file.
    pub fn save(&self, checkpoint_path: &Path) -> Result<(), TransferError> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| TransferError::ProtocolError(format!("Failed to serialize checkpoint: {}", e)))?;
        fs::write(checkpoint_path, json)
            .map_err(|e| TransferError::Io(e))?;
        Ok(())
    }

    /// Load checkpoint from file.
    pub fn load(checkpoint_path: &Path) -> Result<Self, TransferError> {
        let json = fs::read_to_string(checkpoint_path)
            .map_err(|e| TransferError::Io(e))?;
        let checkpoint: Self = serde_json::from_str(&json)
            .map_err(|e| TransferError::ProtocolError(format!("Failed to deserialize checkpoint: {}", e)))?;
        Ok(checkpoint)
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

/// Delete checkpoint file.
pub fn delete_checkpoint(file_path: &Path) -> Result<(), TransferError> {
    let checkpoint_path = get_checkpoint_path(file_path);
    if checkpoint_path.exists() {
        fs::remove_file(&checkpoint_path)
            .map_err(|e| TransferError::Io(e))?;
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
        checkpoint.mark_completed(range);
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
        
        checkpoint.mark_completed(all_ranges[0]);
        checkpoint.mark_completed(all_ranges[2]);
        
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
        checkpoint.mark_completed(FileRange::new(0, 100));
        
        let checkpoint_path = get_checkpoint_path(&file_path);
        checkpoint.save(&checkpoint_path).unwrap();
        
        let loaded = TransferCheckpoint::load(&checkpoint_path).unwrap();
        assert_eq!(loaded.file_size, 1000);
        assert_eq!(loaded.completed_ranges.len(), 1);
    }
}

