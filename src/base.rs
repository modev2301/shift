//! Shared functionality for sender and receiver.
//!
//! This module provides common abstractions used by both the sender and receiver
//! components, including transfer configuration, error handling, and state management.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Transfer configuration shared between sender and receiver.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    /// Starting port number for the transfer.
    pub start_port: u16,
    /// Number of parallel streams/threads to use.
    pub num_streams: usize,
    /// Buffer size for I/O operations in bytes.
    pub buffer_size: usize,
    /// Socket send buffer size in bytes (SO_SNDBUF).
    /// If None, uses default system value.
    pub socket_send_buffer_size: Option<usize>,
    /// Socket receive buffer size in bytes (SO_RCVBUF).
    /// If None, uses default system value.
    pub socket_recv_buffer_size: Option<usize>,
    /// Whether compression is enabled.
    pub enable_compression: bool,
    /// Whether encryption is enabled.
    pub enable_encryption: bool,
    /// Encryption key (32 bytes for AES-256-GCM).
    pub encryption_key: Option<[u8; 32]>,
    /// Timeout for network operations in seconds.
    pub timeout_seconds: u64,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            start_port: 8080,
            num_streams: 16,
            buffer_size: 16 * 1024 * 1024,
            socket_send_buffer_size: Some(16 * 1024 * 1024),
            socket_recv_buffer_size: Some(16 * 1024 * 1024),
            enable_compression: false,
            enable_encryption: false,
            encryption_key: None,
            timeout_seconds: 30,
        }
    }
}

/// Transfer statistics tracked during a transfer.
#[derive(Debug, Default)]
pub struct TransferStats {
    /// Total bytes transferred.
    pub bytes_transferred: AtomicU64,
    /// Number of files transferred.
    pub files_transferred: AtomicU64,
    /// Start time of the transfer.
    pub start_time: Option<Instant>,
    /// End time of the transfer.
    pub end_time: Option<Instant>,
}

impl TransferStats {
    /// Create a new transfer statistics tracker.
    pub fn new() -> Self {
        Self {
            bytes_transferred: AtomicU64::new(0),
            files_transferred: AtomicU64::new(0),
            start_time: Some(Instant::now()),
            end_time: None,
        }
    }

    /// Record bytes transferred.
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a file transfer completion.
    pub fn record_file(&self) {
        self.files_transferred.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark the transfer as complete.
    pub fn finish(&mut self) {
        self.end_time = Some(Instant::now());
    }

    /// Get the duration of the transfer.
    pub fn duration(&self) -> Option<Duration> {
        Some(self.end_time?.duration_since(self.start_time?))
    }

    /// Get the throughput in bytes per second.
    pub fn throughput(&self) -> Option<f64> {
        let duration = self.duration()?.as_secs_f64();
        if duration > 0.0 {
            Some(self.bytes_transferred.load(Ordering::Relaxed) as f64 / duration)
        } else {
            None
        }
    }
}

/// File range for parallel transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileRange {
    /// Start offset in bytes.
    pub start: u64,
    /// End offset in bytes (exclusive).
    pub end: u64,
}

impl FileRange {
    /// Create a new file range.
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Get the size of the range in bytes.
    pub fn size(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

/// Split a file into ranges for parallel transfer.
pub fn split_file_ranges(file_size: u64, num_ranges: usize) -> Vec<FileRange> {
    if num_ranges == 0 || file_size == 0 {
        return Vec::new();
    }

    let range_size = file_size / num_ranges as u64;
    let mut ranges = Vec::with_capacity(num_ranges);

    for i in 0..num_ranges {
        let start = i as u64 * range_size;
        let end = if i == num_ranges - 1 {
            file_size
        } else {
            (i + 1) as u64 * range_size
        };
        ranges.push(FileRange::new(start, end));
    }

    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_file_ranges() {
        let ranges = split_file_ranges(1000, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 250);
        assert_eq!(ranges[3].start, 750);
        assert_eq!(ranges[3].end, 1000);
    }

    #[test]
    fn test_file_range_size() {
        let range = FileRange::new(100, 200);
        assert_eq!(range.size(), 100);
    }

    #[test]
    fn test_transfer_stats() {
        let mut stats = TransferStats::new();
        stats.record_bytes(1024);
        stats.record_file();
        stats.finish();

        assert_eq!(stats.bytes_transferred.load(Ordering::Relaxed), 1024);
        assert_eq!(stats.files_transferred.load(Ordering::Relaxed), 1);
        assert!(stats.duration().is_some());
    }
}

