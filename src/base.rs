//! Shared functionality for sender and receiver.
//!
//! This module provides common abstractions used by both the sender and receiver
//! components, including transfer configuration, error handling, and state management.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Metadata connection message types (see docs/INTEGRITY_DESIGN.md).
/// Hash algorithm for integrity is fixed as BLAKE3; capability handshake does not negotiate it.
pub mod msg {
    /// Server: data ports ready, client may connect.
    pub const READY: u8 = 0x01;
    /// Legacy final ACK (replaced by HASH_OK / HASH_MISMATCH for integrity).
    #[allow(dead_code)]
    pub const COMPLETE_LEGACY: u8 = 0x02;
    /// Client → Server: hash message follows (1 + 32 bytes).
    pub const HASH: u8 = 0x03;
    /// Server → Client: hash verified OK.
    pub const HASH_OK: u8 = 0x04;
    /// Server → Client: hash mismatch; client should keep checkpoint.
    pub const HASH_MISMATCH: u8 = 0x05;
    /// Client → Server: per-range hashes for resume verification (sent before data streams).
    /// Wire: 0x06 (1) + num_ranges (8 LE) + for each range in file order: start (8 LE), end (8 LE), blake3 (32).
    pub const RANGE_HASHES: u8 = 0x06;
    /// Server → Client: range verification result. Wire: 0x07 (1) + num_failed (8 LE) + for each failed: start (8 LE), end (8 LE).
    pub const RANGE_VERIFY_RESULT: u8 = 0x07;
    /// Client → Server: no range verify (proceed to data). Sent when no completed ranges to verify.
    pub const NO_RANGE_VERIFY: u8 = 0x00;
}

/// Capability negotiation: client sends, server responds with negotiated (intersection).
/// Both sides use the negotiated values; replaces the assumption that configs match.
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub version: u8,
    /// bit 0 = compression, 1 = encryption, 2 = blake3, 3 = range_hashes, 4 = quic
    pub flags: u8,
    pub max_streams: u16,
    pub max_buffer: u32,
    /// 0 = unknown, 1 = linux, 2 = windows
    pub platform: u8,
    pub reserved: [u8; 4],
}

pub mod cap_flags {
    pub const COMPRESSION: u8 = 1 << 0;
    pub const ENCRYPTION: u8 = 1 << 1;
    pub const BLAKE3: u8 = 1 << 2;
    pub const RANGE_HASHES: u8 = 1 << 3;
    pub const QUIC: u8 = 1 << 4;
}

/// Wire size of Capabilities (LE: version, flags, max_streams, max_buffer, platform, reserved).
pub const CAPABILITIES_WIRE_LEN: usize = 1 + 1 + 2 + 4 + 1 + 4;

impl Capabilities {
    pub fn to_bytes(&self) -> [u8; CAPABILITIES_WIRE_LEN] {
        let mut buf = [0u8; CAPABILITIES_WIRE_LEN];
        buf[0] = self.version;
        buf[1] = self.flags;
        buf[2..4].copy_from_slice(&self.max_streams.to_le_bytes());
        buf[4..8].copy_from_slice(&self.max_buffer.to_le_bytes());
        buf[8] = self.platform;
        buf[9..13].copy_from_slice(&self.reserved);
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < CAPABILITIES_WIRE_LEN {
            return None;
        }
        Some(Self {
            version: buf[0],
            flags: buf[1],
            max_streams: u16::from_le_bytes([buf[2], buf[3]]),
            max_buffer: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            platform: buf[8],
            reserved: [buf[9], buf[10], buf[11], buf[12]],
        })
    }

    /// Intersection: both sides use the minimum of numeric caps and AND of flags.
    pub fn negotiate(client: Capabilities, server: Capabilities) -> Capabilities {
        let flags = client.flags & server.flags;
        Capabilities {
            version: client.version.min(server.version),
            flags,
            max_streams: client.max_streams.min(server.max_streams),
            max_buffer: client.max_buffer.min(server.max_buffer),
            platform: server.platform, // server reports its platform
            reserved: [0; 4],
        }
    }
}

/// Build client capabilities from config. Used by sender.
pub fn capabilities_from_config(config: &TransferConfig) -> Capabilities {
    use cap_flags::{BLAKE3, COMPRESSION, ENCRYPTION, RANGE_HASHES};
    let mut flags = 0u8;
    if config.enable_compression {
        flags |= COMPRESSION;
    }
    if config.enable_encryption {
        flags |= ENCRYPTION;
    }
    flags |= BLAKE3;   // we always support BLAKE3
    flags |= RANGE_HASHES; // we always support range hashes
    let platform = if cfg!(target_os = "linux") {
        1
    } else if cfg!(target_os = "windows") {
        2
    } else {
        0
    };
    Capabilities {
        version: 1,
        flags,
        max_streams: config.num_streams as u16,
        max_buffer: config.buffer_size as u32,
        platform,
        reserved: [0; 4],
    }
}

/// Apply negotiated capabilities to config. Both client and server use this.
pub fn apply_capabilities_to_config(config: &TransferConfig, cap: Capabilities) -> TransferConfig {
    use cap_flags::{COMPRESSION, ENCRYPTION};
    let mut c = config.clone();
    c.num_streams = cap.max_streams as usize;
    c.buffer_size = cap.max_buffer as usize;
    c.enable_compression = (cap.flags & COMPRESSION) != 0;
    c.enable_encryption = (cap.flags & ENCRYPTION) != 0;
    c
}

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

