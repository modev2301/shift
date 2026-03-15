//! Shared functionality for sender and receiver.
//!
//! This module provides common abstractions used by both the sender and receiver
//! components, including transfer configuration, error handling, and state management.

use serde::Serialize;
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
    /// RTT probe: client → server. Wire: 0x08 (1) + timestamp_us (8 LE).
    pub const PING: u8 = 0x08;
    /// RTT probe: server → client. Wire: 0x09 (1) + timestamp_us (8 LE).
    pub const PONG: u8 = 0x09;
    /// Client → Server: skip check (--update). Wire: 0x0A (1) + filename_len (8 LE) + filename + hash (32).
    pub const CHECK_HASH: u8 = 0x0A;
    /// Server → Client: file already present with this hash, skip transfer.
    pub const HAVE_HASH: u8 = 0x0B;
    /// Server → Client: need file (proceed with metadata).
    pub const NEED_FILE: u8 = 0x0C;
    /// Client → Server: bandwidth probe. Wire: 0x0D (1) + size (8 LE) + size bytes of data. Server reads and discards.
    pub const PROBE: u8 = 0x0D;
}
/// Wire size of Ping/Pong: 1 byte type + 8 bytes timestamp_us LE.
pub const PING_PONG_WIRE_LEN: usize = 9;

/// Capability negotiation: client sends, server responds with negotiated (intersection).
/// Both sides use the negotiated values; replaces the assumption that configs match.
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub version: u8,
    /// bit 0 = compression, 1 = encryption, 2 = blake3, 3 = range_hashes, 4 = quic
    pub flags: u8,
    /// Ceiling: max streams we may use this transfer (receiver pre-spawns this many).
    pub max_streams: u16,
    /// Starting count: streams to open initially (sender may grow up to max_streams).
    pub initial_streams: u16,
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
    /// Both sides support RTT probe (PING/PONG) on control channel before metadata.
    pub const RTT_PROBE: u8 = 1 << 5;
    /// Both sides support FEC (RaptorQ) on data channel; frame type 0x02 = FEC block.
    pub const FEC: u8 = 1 << 6;
}

/// Wire size of Capabilities (LE: version, flags, max_streams, initial_streams, max_buffer, platform, reserved).
pub const CAPABILITIES_WIRE_LEN: usize = 1 + 1 + 2 + 2 + 4 + 1 + 4;

impl Capabilities {
    pub fn to_bytes(&self) -> [u8; CAPABILITIES_WIRE_LEN] {
        let mut buf = [0u8; CAPABILITIES_WIRE_LEN];
        buf[0] = self.version;
        buf[1] = self.flags;
        buf[2..4].copy_from_slice(&self.max_streams.to_le_bytes());
        buf[4..6].copy_from_slice(&self.initial_streams.to_le_bytes());
        buf[6..10].copy_from_slice(&self.max_buffer.to_le_bytes());
        buf[10] = self.platform;
        buf[11..15].copy_from_slice(&self.reserved);
        buf
    }

    /// Parses capability wire. Accepts legacy 13-byte (no initial_streams) or new 15-byte format.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        const LEGACY_WIRE_LEN: usize = 1 + 1 + 2 + 4 + 1 + 4; // 13
        if buf.len() < LEGACY_WIRE_LEN {
            return None;
        }
        let max_streams = u16::from_le_bytes([buf[2], buf[3]]);
        let (initial_streams, max_buffer, platform, reserved) = if buf.len() >= CAPABILITIES_WIRE_LEN {
            (
                u16::from_le_bytes([buf[4], buf[5]]),
                u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]),
                buf[10],
                [buf[11], buf[12], buf[13], buf[14]],
            )
        } else {
            (max_streams, u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]), buf[8], [buf[9], buf[10], buf[11], buf[12]])
        };
        Some(Self {
            version: buf[0],
            flags: buf[1],
            max_streams,
            initial_streams,
            max_buffer,
            platform,
            reserved,
        })
    }

    /// Intersection: max_streams = ceiling (min of both). initial_streams = min of both proposals, capped by max_streams (so either side can request fewer than the ceiling).
    pub fn negotiate(client: Capabilities, server: Capabilities) -> Capabilities {
        let flags = client.flags & server.flags;
        let max_streams = client.max_streams.min(server.max_streams);
        let initial_streams = client.initial_streams
            .min(server.initial_streams)
            .min(max_streams)
            .max(1);
        Capabilities {
            version: client.version.min(server.version),
            flags,
            max_streams,
            initial_streams,
            max_buffer: client.max_buffer.min(server.max_buffer),
            platform: server.platform,
            reserved: [0; 4],
        }
    }
}

/// Build client capabilities from config. Used by sender. max_streams = ceiling, initial_streams = starting count.
pub fn capabilities_from_config(config: &TransferConfig) -> Capabilities {
    use cap_flags::{BLAKE3, COMPRESSION, ENCRYPTION, RANGE_HASHES, RTT_PROBE};
    let mut flags = 0u8;
    if config.enable_compression {
        flags |= COMPRESSION;
    }
    if config.enable_encryption {
        flags |= ENCRYPTION;
    }
    flags |= BLAKE3;
    flags |= RANGE_HASHES;
    flags |= RTT_PROBE;
    // Advertise FEC support when built with fec so receiver is ready; enable_fec can be turned on mid-transfer (auto-trigger).
    #[cfg(feature = "fec")]
    {
        flags |= cap_flags::FEC;
    }
    let platform = if cfg!(target_os = "linux") {
        1
    } else if cfg!(target_os = "windows") {
        2
    } else {
        0
    };
    let max_streams = config.max_streams.min(65535) as u16;
    let initial_streams = (config.num_streams as u16).min(max_streams).max(1);
    Capabilities {
        version: 1,
        flags,
        max_streams,
        initial_streams,
        max_buffer: config.buffer_size as u32,
        platform,
        reserved: [0; 4],
    }
}

/// Apply negotiated capabilities to config. num_streams = initial (starting count), max_streams = ceiling.
/// Never increase max_streams above the client's original config so range split matches what the client intended (and what we send in metadata).
pub fn apply_capabilities_to_config(config: &TransferConfig, cap: Capabilities) -> TransferConfig {
    use cap_flags::{COMPRESSION, ENCRYPTION};
    let mut c = config.clone();
    c.num_streams = cap.initial_streams as usize;
    c.max_streams = config.max_streams.min(cap.max_streams as usize);
    c.buffer_size = cap.max_buffer as usize;
    c.enable_compression = (cap.flags & COMPRESSION) != 0;
    c.enable_encryption = (cap.flags & ENCRYPTION) != 0;
    // Only enable FEC at start when both sides support it and sender asked for it; coordinator can set enable_fec_auto later.
    #[cfg(feature = "fec")]
    {
        let fec_supported = (cap.flags & cap_flags::FEC) != 0;
        c.enable_fec = fec_supported && config.enable_fec;
        c.fec_negotiated = fec_supported;
    }
    c
}

/// Transfer configuration shared between sender and receiver.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    /// Starting port number for the transfer.
    pub start_port: u16,
    /// Initial number of streams to open (after handshake). Adaptive can grow up to max_streams.
    pub num_streams: usize,
    /// Ceiling: max streams (receiver pre-spawns this many listeners).
    pub max_streams: usize,
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
    /// If set (and build has `tls` feature), TCP uses mutual TLS; directory must contain ca.pem, server.pem, server-key.pem (server) or ca.pem, client.pem, client-key.pem (client). Generate with `shift tls-keygen <dir>`.
    pub tls_cert_dir: Option<std::path::PathBuf>,
    /// Enable FEC (RaptorQ) for this transfer. Requires build with --features fec.
    pub enable_fec: bool,
    /// FEC block size in bytes (e.g. 65536). Only used when enable_fec is true.
    pub fec_block_size: usize,
    /// FEC repair packets per block (redundancy). Only used when enable_fec is true.
    pub fec_repair_packets: u32,
    /// True when both sides negotiated FEC (receiver can decode 0x02). Used for auto-trigger.
    #[cfg(feature = "fec")]
    pub fec_negotiated: bool,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            start_port: 8080,
            num_streams: 16,
            max_streams: 16,
            buffer_size: 16 * 1024 * 1024,
            socket_send_buffer_size: Some(16 * 1024 * 1024),
            socket_recv_buffer_size: Some(16 * 1024 * 1024),
            enable_compression: false,
            enable_encryption: false,
            encryption_key: None,
            timeout_seconds: 30,
            tls_cert_dir: None,
            enable_fec: false,
            fec_block_size: 65536,
            fec_repair_packets: 4,
            #[cfg(feature = "fec")]
            fec_negotiated: false,
        }
    }
}

/// Status of a single-file transfer for --stats / --json.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum TransferStatus {
    #[default]
    Completed,
    Skipped {
        reason: &'static str,
    },
    Failed,
}

impl serde::Serialize for TransferStatus {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(match self {
            TransferStatus::Completed => "completed",
            TransferStatus::Skipped { .. } => "skipped",
            TransferStatus::Failed => "failed",
        })
    }
}

/// Per-file transfer report for --stats / --json output (scriptable, benchmarkable).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TransferReport {
    #[serde(default)]
    pub status: TransferStatus,
    /// When skipped: human-readable reason (e.g. "unchanged"). Omitted in JSON when not set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// Bytes transferred (0 when skipped).
    pub bytes: u64,
    /// When skipped: file size that was hashed for the check.
    #[serde(default)]
    pub bytes_checked: u64,
    pub duration_ms: u64,
    #[serde(default)]
    pub throughput_mbps: f64,
    #[serde(default)]
    pub streams_initial: usize,
    #[serde(default)]
    pub streams_peak: usize,
    #[serde(default)]
    pub streams_stalled: usize,
    #[serde(default)]
    pub rtt_ms: u64,
    #[serde(default)]
    pub ranges_total: usize,
    #[serde(default)]
    pub ranges_resumed: usize,
    #[serde(default)]
    pub transport: String,
}

impl Default for TransferReport {
    fn default() -> Self {
        Self {
            status: TransferStatus::Completed,
            reason: None,
            bytes: 0,
            bytes_checked: 0,
            duration_ms: 0,
            throughput_mbps: 0.0,
            streams_initial: 0,
            streams_peak: 0,
            streams_stalled: 0,
            rtt_ms: 0,
            ranges_total: 0,
            ranges_resumed: 0,
            transport: String::new(),
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

/// Number of ranges to use for transfer (finer granularity than stream count for better load balance and stall recovery).
/// Used by both sender and receiver so they agree on listener count.
pub fn transfer_num_ranges(max_streams: usize) -> usize {
    (max_streams * 4).min(128).max(1)
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
    fn test_transfer_num_ranges() {
        assert_eq!(transfer_num_ranges(1), 4); // 1*4
        assert_eq!(transfer_num_ranges(8), 32);
        assert_eq!(transfer_num_ranges(32), 128);
        assert_eq!(transfer_num_ranges(64), 128); // capped at 128
        assert_eq!(transfer_num_ranges(256), 128);
    }

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

