//! UDP-based reliable transfer architecture
//! 
//! This module implements a high-performance transfer system using:
//! - UDP sockets with reliable protocol (sequence numbers, ACKs, retransmission)
//! - OS threads with blocking I/O (no async overhead)
//! - Thread-per-port architecture
//! - Sliding window flow control
//! - Out-of-order packet handling

use crate::error::TransferError;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::net::{SocketAddr, UdpSocket};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MAX_PACKET_SIZE: usize = 1400; // MTU-safe (1500 - IP/UDP headers)
const DATA_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - 32; // Reserve space for header
const INITIAL_WINDOW_SIZE: usize = 64; // Initial window size
const MIN_RTO_MS: u64 = 10; // Minimum RTO (10ms)
const MAX_RTO_MS: u64 = 60000; // Maximum RTO (60s)
const ACK_INTERVAL_MS: u64 = 10; // Send ACK every 10ms
const INITIAL_RTT_MS: u64 = 100; // Initial RTT estimate

// Packet types
const PKT_DATA: u8 = 0x01;
const PKT_ACK: u8 = 0x02;
const PKT_METADATA: u8 = 0x04;
const PKT_RANGE: u8 = 0x08;
const PKT_FIN: u8 = 0x10;

/// Packet header (16 bytes)
#[repr(C, packed)]
struct PacketHeader {
    packet_type: u8,
    thread_id: u8,
    flags: u8,
    _reserved: u8,
    sequence: u32,
    ack_sequence: u32,
    offset: u64,
    data_len: u16,
    checksum: u16,
}

impl PacketHeader {
    fn to_bytes(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0] = self.packet_type;
        buf[1] = self.thread_id;
        buf[2] = self.flags;
        buf[3] = self._reserved;
        buf[4..8].copy_from_slice(&self.sequence.to_le_bytes());
        buf[8..12].copy_from_slice(&self.ack_sequence.to_le_bytes());
        buf[12..20].copy_from_slice(&self.offset.to_le_bytes());
        buf[20..22].copy_from_slice(&self.data_len.to_le_bytes());
        buf[22..24].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    fn from_bytes(buf: &[u8]) -> Result<Self, TransferError> {
        if buf.len() < 32 {
            return Err(TransferError::ProtocolError("Packet too small".to_string()));
        }
        Ok(PacketHeader {
            packet_type: buf[0],
            thread_id: buf[1],
            flags: buf[2],
            _reserved: buf[3],
            sequence: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            ack_sequence: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            offset: u64::from_le_bytes([
                buf[12], buf[13], buf[14], buf[15],
                buf[16], buf[17], buf[18], buf[19],
            ]),
            data_len: u16::from_le_bytes([buf[20], buf[21]]),
            checksum: u16::from_le_bytes([buf[22], buf[23]]),
        })
    }
}

fn calculate_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    for chunk in data.chunks(2) {
        if chunk.len() == 2 {
            sum += u16::from_le_bytes([chunk[0], chunk[1]]) as u32;
        } else {
            sum += chunk[0] as u32;
        }
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    !(sum as u16)
}

/// File range assigned to a thread
#[derive(Debug, Clone, Copy)]
pub struct FileRange {
    pub start: u64,
    pub end: u64,
}

/// File metadata (only sent once on thread 0)
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub filename: String,
    pub file_size: u64,
    pub num_threads: usize,
}

/// Configuration for blocking transfer
#[derive(Clone)]
pub struct BlockingTransferConfig {
    pub num_threads: usize,
    pub base_port: u16,
    pub buffer_size: usize,
    pub enable_compression: bool,
}

impl Default for BlockingTransferConfig {
    fn default() -> Self {
        Self {
            num_threads: 8,
            base_port: 8080,
            buffer_size: 8 * 1024 * 1024, // 8MB
            enable_compression: false,
        }
    }
}

/// Encode varint (protobuf-style)
pub fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode varint from byte slice
pub fn decode_varint_from_slice(buf: &[u8]) -> Result<(u64, usize), TransferError> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut bytes_read = 0;
    for &byte in buf {
        bytes_read += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(TransferError::ProtocolError("Varint too large".to_string()));
        }
    }
    Ok((result, bytes_read))
}

/// Split file into ranges for N threads
pub fn split_file_ranges(file_size: u64, num_threads: usize) -> Vec<FileRange> {
    let chunk_size = file_size / num_threads as u64;
    let mut ranges = Vec::with_capacity(num_threads);
    
    for i in 0..num_threads {
        let start = i as u64 * chunk_size;
        let end = if i == num_threads - 1 {
            file_size // Last thread gets remainder
        } else {
            (i + 1) as u64 * chunk_size
        };
        ranges.push(FileRange { start, end });
    }
    
    ranges
}

/// Configure UDP socket for high performance
fn configure_udp_socket(socket: &UdpSocket) -> Result<(), TransferError> {
    #[cfg(target_os = "linux")]
    {
        let fd = socket.as_raw_fd();
        let buf_size: libc::c_int = 25 * 1024 * 1024; // 25MB
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    Ok(())
}

/// RTT estimator (RFC 6298)
struct RttEstimator {
    srtt: Duration,      // Smoothed RTT
    rttvar: Duration,   // RTT variance
    rto: Duration,      // Retransmit timeout
}

impl RttEstimator {
    fn new() -> Self {
        let initial_rtt = Duration::from_millis(INITIAL_RTT_MS);
        Self {
            srtt: initial_rtt,
            rttvar: initial_rtt / 2,
            rto: initial_rtt + initial_rtt, // srtt + 4*rttvar, but start with 2*srtt
        }
    }

    fn update(&mut self, sample: Duration) {
        if sample.is_zero() {
            return;
        }

        // RFC 6298 algorithm
        let alpha = 0.125; // 1/8
        let beta = 0.25;   // 1/4

        let err = if sample > self.srtt {
            sample - self.srtt
        } else {
            self.srtt - sample
        };

        // Update RTT variance
        self.rttvar = Duration::from_secs_f64(
            (1.0 - beta) * self.rttvar.as_secs_f64() + beta * err.as_secs_f64()
        );

        // Update smoothed RTT
        self.srtt = Duration::from_secs_f64(
            (1.0 - alpha) * self.srtt.as_secs_f64() + alpha * sample.as_secs_f64()
        );

        // Calculate RTO
        self.rto = self.srtt + 4 * self.rttvar;
        self.rto = self.rto.max(Duration::from_millis(MIN_RTO_MS));
        self.rto = self.rto.min(Duration::from_millis(MAX_RTO_MS));
    }

    fn rto(&self) -> Duration {
        self.rto
    }
}

/// BBR-style congestion controller
struct CongestionController {
    btl_bw: u64,              // Bottleneck bandwidth (bytes/sec)
    min_rtt: Duration,        // Minimum observed RTT
    pacing_rate: u64,         // Current send rate (bytes/sec)
    cwnd: usize,              // Congestion window (packets)
    delivered: u64,           // Total bytes delivered
    delivered_time: Instant,  // Time of last delivery measurement
    rtt_samples: VecDeque<Duration>,
}

impl CongestionController {
    fn new() -> Self {
        Self {
            btl_bw: 1_000_000, // Start with 1MB/s estimate
            min_rtt: Duration::from_millis(INITIAL_RTT_MS),
            pacing_rate: 1_000_000,
            cwnd: INITIAL_WINDOW_SIZE,
            delivered: 0,
            delivered_time: Instant::now(),
            rtt_samples: VecDeque::with_capacity(10),
        }
    }

    fn on_ack(&mut self, acked_bytes: u64, rtt: Duration) {
        // Update minimum RTT
        self.min_rtt = self.min_rtt.min(rtt);
        self.rtt_samples.push_back(rtt);
        if self.rtt_samples.len() > 10 {
            self.rtt_samples.pop_front();
        }

        // Estimate bandwidth: bytes_delivered / time (EWMA)
        let now = Instant::now();
        let elapsed = now.duration_since(self.delivered_time);
        if elapsed.as_secs_f64() > 0.001 { // At least 1ms elapsed
            let delivery_rate = acked_bytes as f64 / elapsed.as_secs_f64();
            
            // EWMA with alpha=0.1 for smoothing (adapts to both increases and decreases)
            self.btl_bw = ((0.9 * self.btl_bw as f64) + (0.1 * delivery_rate)) as u64;
        }

        self.delivered += acked_bytes;
        self.delivered_time = now;

        // BBR-style: target rate = btl_bw, target inflight = btl_bw * min_rtt
        let bdp = (self.btl_bw as f64 * self.min_rtt.as_secs_f64()) as usize;
        // Increased max window to 100,000 packets (140MB) for high-BDP links
        self.cwnd = (bdp / MAX_PACKET_SIZE).max(INITIAL_WINDOW_SIZE).min(100_000);
        self.pacing_rate = self.btl_bw;
    }

    fn should_send(&self, inflight: usize) -> bool {
        inflight < self.cwnd
    }

    fn pacing_delay(&self) -> Duration {
        if self.pacing_rate == 0 {
            return Duration::from_millis(1);
        }
        let delay_ns = (MAX_PACKET_SIZE as u64 * 1_000_000_000) / self.pacing_rate;
        Duration::from_nanos(delay_ns.max(1000)) // Minimum 1 microsecond
    }

    #[allow(dead_code)]
    fn cwnd(&self) -> usize {
        self.cwnd
    }
}

/// Encryption wrapper for packet encryption/decryption
struct PacketEncryption {
    key: LessSafeKey,
    nonce_counter: u64,
}

impl PacketEncryption {
    #[allow(dead_code)]
    fn new(key_bytes: &[u8; 32]) -> Result<Self, TransferError> {
        let unbound_key = UnboundKey::new(&AES_256_GCM, key_bytes)
            .map_err(|e| TransferError::ProtocolError(format!("Key creation failed: {:?}", e)))?;
        Ok(Self {
            key: LessSafeKey::new(unbound_key),
            nonce_counter: 0,
        })
    }

    fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, TransferError> {
        let nonce_bytes = self.nonce_counter.to_le_bytes();
        self.nonce_counter = self.nonce_counter.wrapping_add(1);
        
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|e| TransferError::ProtocolError(format!("Nonce creation failed: {:?}", e)))?;
        
        let mut in_out = plaintext.to_vec();
        self.key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
            .map_err(|e| TransferError::ProtocolError(format!("Encryption failed: {:?}", e)))?;
        
        // Prepend nonce to ciphertext
        let mut packet = nonce_bytes.to_vec();
        packet.extend(in_out);
        Ok(packet)
    }

    fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, TransferError> {
        if ciphertext.len() < 8 {
            return Err(TransferError::ProtocolError("Ciphertext too short".to_string()));
        }
        
        let nonce_bytes: [u8; 8] = ciphertext[0..8].try_into()
            .map_err(|_| TransferError::ProtocolError("Invalid nonce".to_string()))?;
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|e| TransferError::ProtocolError(format!("Nonce creation failed: {:?}", e)))?;
        
        let mut in_out = ciphertext[8..].to_vec();
        self.key.open_in_place(nonce, Aad::empty(), &mut in_out)
            .map_err(|e| TransferError::ProtocolError(format!("Decryption failed: {:?}", e)))?;
        
        // Remove authentication tag (16 bytes for GCM)
        in_out.truncate(in_out.len() - 16);
        Ok(in_out)
    }
}

/// Reliable UDP sender with sliding window, congestion control, and pacing
struct ReliableSender {
    socket: UdpSocket,
    peer_addr: SocketAddr,
    next_sequence: u32,
    window_base: u32,
    unacked_packets: HashMap<u32, (Vec<u8>, Instant)>,
    rtt_estimator: RttEstimator,
    congestion: CongestionController,
    last_send_time: Instant,
    sent_sequences: HashMap<u32, Instant>, // Track when each packet was sent for RTT
    encryption: Option<PacketEncryption>, // Optional encryption
}

impl ReliableSender {
    fn new(socket: UdpSocket, peer_addr: SocketAddr) -> Result<Self, TransferError> {
        configure_udp_socket(&socket)?;
        // For now, encryption is disabled (use None)
        // In production, derive key from handshake/auth_token
        Ok(Self {
            socket,
            peer_addr,
            next_sequence: 1,
            window_base: 1,
            unacked_packets: HashMap::new(),
            rtt_estimator: RttEstimator::new(),
            congestion: CongestionController::new(),
            last_send_time: Instant::now(),
            sent_sequences: HashMap::new(),
            encryption: None, // TODO: Enable after handshake
        })
    }

    fn send_packet(&mut self, packet_type: u8, thread_id: u8, offset: u64, data: &[u8]) -> Result<(), TransferError> {
        // Check for ACKs first
        self.check_acks()?;

        // Wait for congestion window space
        let inflight = (self.next_sequence - self.window_base) as usize;
        while !self.congestion.should_send(inflight) {
            self.check_acks()?;
            thread::sleep(Duration::from_millis(1));
        }

        // Packet pacing: space out sends to avoid bursts
        let now = Instant::now();
        let target_time = self.last_send_time + self.congestion.pacing_delay();
        if now < target_time {
            thread::sleep(target_time - now);
        }

        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);

        let mut packet = Vec::with_capacity(32 + data.len());
        let header = PacketHeader {
            packet_type,
            thread_id,
            flags: 0,
            _reserved: 0,
            sequence,
            ack_sequence: 0,
            offset,
            data_len: data.len() as u16,
            checksum: 0,
        };
        packet.extend_from_slice(&header.to_bytes());
        packet.extend_from_slice(data);
        
        // Encrypt if enabled
        let final_packet = if let Some(ref mut enc) = self.encryption {
            enc.encrypt(&packet)?
        } else {
            packet
        };
        
        // Calculate checksum on final packet
        let mut packet_with_checksum = final_packet;
        let checksum = calculate_checksum(&packet_with_checksum[24..]);
        packet_with_checksum[22..24].copy_from_slice(&checksum.to_le_bytes());

        // Send packet
        let send_time = Instant::now();
        self.socket.send_to(&packet_with_checksum, self.peer_addr)?;
        self.last_send_time = send_time;

        // Store for retransmission and RTT measurement
        self.unacked_packets.insert(sequence, (packet_with_checksum, send_time));
        self.sent_sequences.insert(sequence, send_time);

        // Retransmit old packets
        self.retransmit_old_packets()?;

        Ok(())
    }

    fn check_acks(&mut self) -> Result<(), TransferError> {
        let mut buf = [0u8; MAX_PACKET_SIZE];
        let mut acked_bytes = 0u64;
        
        while let Ok((size, _)) = self.socket.recv_from(&mut buf) {
            if size < 32 {
                continue;
            }
            let header = PacketHeader::from_bytes(&buf[..32])?;
            if header.packet_type == PKT_ACK {
                let acked = header.ack_sequence;
                
                // Parse SACK blocks if present
                if (header.flags & 0x01) != 0 && size > 32 {
                    let sack_data = &buf[32..size];
                    if !sack_data.is_empty() {
                        let num_blocks = sack_data[0] as usize;
                        let mut offset = 1;
                        for _ in 0..num_blocks {
                            if offset + 8 <= sack_data.len() {
                                let start = u32::from_le_bytes([
                                    sack_data[offset],
                                    sack_data[offset + 1],
                                    sack_data[offset + 2],
                                    sack_data[offset + 3],
                                ]);
                                let end = u32::from_le_bytes([
                                    sack_data[offset + 4],
                                    sack_data[offset + 5],
                                    sack_data[offset + 6],
                                    sack_data[offset + 7],
                                ]);
                                // Mark packets in range [start, end] as received
                                for seq in start..=end {
                                    if let Some((packet, _)) = self.unacked_packets.remove(&seq) {
                                        acked_bytes += (packet.len() - 32) as u64;
                                    }
                                    self.sent_sequences.remove(&seq);
                                }
                                offset += 8;
                            }
                        }
                    }
                }
                
                // Calculate RTT for newly ACKed packets
                if let Some(&send_time) = self.sent_sequences.get(&acked) {
                    let rtt = Instant::now().duration_since(send_time);
                    self.rtt_estimator.update(rtt);
                    
                    // Calculate bytes ACKed
                    if let Some((packet, _)) = self.unacked_packets.get(&acked) {
                        acked_bytes += (packet.len() - 32) as u64; // Exclude header
                    }
                }
                
                // Remove acked packets (cumulative ACK)
                self.unacked_packets.retain(|&seq, _| seq > acked);
                self.sent_sequences.retain(|&seq, _| seq > acked);
                
                // Advance window
                if acked >= self.window_base {
                    self.window_base = acked + 1;
                }
            }
        }
        
        // Update congestion controller
        if acked_bytes > 0 {
            self.congestion.on_ack(acked_bytes, self.rtt_estimator.srtt);
        }
        
        Ok(())
    }

    fn retransmit_old_packets(&mut self) -> Result<(), TransferError> {
        let now = Instant::now();
        let rto = self.rtt_estimator.rto();
        let mut to_retransmit = Vec::new();
        
        for (&seq, (_packet, send_time)) in &self.unacked_packets {
            if now.duration_since(*send_time) > rto {
                to_retransmit.push(seq);
            }
        }

        for seq in to_retransmit {
            if let Some((packet, _)) = self.unacked_packets.get_mut(&seq) {
                let send_time = Instant::now();
                self.socket.send_to(packet, self.peer_addr)?;
                if let Some(entry) = self.unacked_packets.get_mut(&seq) {
                    entry.1 = send_time;
                }
                // Update sent time for RTT measurement
                self.sent_sequences.insert(seq, send_time);
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), TransferError> {
        // Wait for all packets to be ACKed
        while !self.unacked_packets.is_empty() {
            self.check_acks()?;
            self.retransmit_old_packets()?;
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}

/// Selective ACK block (range of received packets)
#[derive(Debug, Clone)]
struct SackBlock {
    start: u32,
    end: u32,
}

/// Reliable UDP receiver with out-of-order handling and SACK
struct ReliableReceiver {
    socket: UdpSocket,
    peer_addr: SocketAddr,
    expected_sequence: u32,
    received_packets: HashMap<u32, (u64, Vec<u8>)>, // sequence -> (offset, data)
    last_ack_time: Instant,
    received_sequences: Vec<u32>, // Track all received sequences for SACK
    encryption: Option<PacketEncryption>, // Optional decryption
}

impl ReliableReceiver {
    fn new(socket: UdpSocket, peer_addr: SocketAddr) -> Result<Self, TransferError> {
        configure_udp_socket(&socket)?;
        Ok(Self {
            socket,
            peer_addr,
            expected_sequence: 1,
            received_packets: HashMap::new(),
            last_ack_time: Instant::now(),
            received_sequences: Vec::new(),
            encryption: None, // TODO: Enable after handshake
        })
    }

    fn generate_sack_blocks(&self) -> Vec<SackBlock> {
        if self.received_sequences.is_empty() {
            return Vec::new();
        }

        let mut sorted: Vec<u32> = self.received_sequences.iter()
            .filter(|&&seq| seq > self.expected_sequence)
            .copied()
            .collect();
        sorted.sort_unstable();
        sorted.dedup();

        if sorted.is_empty() {
            return Vec::new();
        }

        let mut blocks = Vec::new();
        let mut start = sorted[0];
        let mut end = sorted[0];

        for &seq in sorted.iter().skip(1) {
            if seq == end + 1 {
                end = seq;
            } else {
                blocks.push(SackBlock { start, end });
                start = seq;
                end = seq;
            }
        }
        blocks.push(SackBlock { start, end });

        // Limit to 3 blocks (common SACK limit)
        blocks.truncate(3);
        blocks
    }

    fn recv_packet(&mut self, timeout: Duration) -> Result<Option<(u64, Vec<u8>)>, TransferError> {
        // First, deliver any buffered packets that are now in-order
        while let Some((offset, data)) = self.received_packets.remove(&self.expected_sequence) {
            self.expected_sequence = self.expected_sequence.wrapping_add(1);
            self.received_sequences.retain(|&s| s != self.expected_sequence.wrapping_sub(1));
            return Ok(Some((offset, data)));
        }
        
        // Clean up old received sequences periodically
        if self.received_sequences.len() > 1000 {
            self.received_sequences.retain(|&s| s >= self.expected_sequence.wrapping_sub(100));
        }
        let mut buf = [0u8; MAX_PACKET_SIZE];
        self.socket.set_read_timeout(Some(timeout))?;
        
        let (size, _) = match self.socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        if size < 32 {
            return Ok(None);
        }

        // Decrypt if enabled
        let decrypted_buf = if let Some(ref mut enc) = self.encryption {
            enc.decrypt(&buf[..size])?
        } else {
            buf[..size].to_vec()
        };
        
        if decrypted_buf.len() < 32 {
            return Ok(None);
        }
        
        let header = PacketHeader::from_bytes(&decrypted_buf[..32])?;
        
        // Verify checksum
        let checksum = calculate_checksum(&decrypted_buf[24..]);
        if checksum != header.checksum {
            return Ok(None); // Bad checksum, ignore
        }

        // Send ACK
        self.send_ack(header.sequence)?;

        if header.packet_type == PKT_FIN {
            return Ok(None);
        }

        let data = decrypted_buf[32..].to_vec();
        
        // Handle out-of-order packets
        if header.sequence == self.expected_sequence {
            // In-order packet - return it immediately
            // Buffered packets will be returned on subsequent recv_packet calls
            self.expected_sequence = self.expected_sequence.wrapping_add(1);
            self.received_sequences.retain(|&s| s != header.sequence);
            
            // Clean up old received sequences
            self.received_sequences.retain(|&s| s >= self.expected_sequence);
            
            return Ok(Some((header.offset, data)));
        } else if header.sequence > self.expected_sequence {
            // Out-of-order, buffer it
            let seq = header.sequence; // Copy to avoid packed field reference
            if !self.received_packets.contains_key(&seq) {
                self.received_packets.insert(seq, (header.offset, data));
                self.received_sequences.push(seq);
            }
            return Ok(None);
        } else {
            // Old packet, already received - still send ACK
            return Ok(None);
        }
    }

    fn send_ack(&mut self, ack_sequence: u32) -> Result<(), TransferError> {
        let now = Instant::now();
        if now.duration_since(self.last_ack_time).as_millis() < ACK_INTERVAL_MS as u128 {
            return Ok(());
        }
        self.last_ack_time = now;

        // Generate SACK blocks
        let sack_blocks = self.generate_sack_blocks();
        
        // Build ACK packet with SACK
        let mut ack_data = Vec::new();
        ack_data.push(sack_blocks.len() as u8); // Number of SACK blocks
        for block in &sack_blocks {
            ack_data.extend_from_slice(&block.start.to_le_bytes());
            ack_data.extend_from_slice(&block.end.to_le_bytes());
        }

        let header = PacketHeader {
            packet_type: PKT_ACK,
            thread_id: 0,
            flags: if !sack_blocks.is_empty() { 0x01 } else { 0 }, // SACK flag
            _reserved: 0,
            sequence: 0,
            ack_sequence,
            offset: 0,
            data_len: ack_data.len() as u16,
            checksum: 0,
        };
        let mut packet = header.to_bytes().to_vec();
        packet.extend_from_slice(&ack_data);
        let checksum = calculate_checksum(&packet[24..]);
        packet[22..24].copy_from_slice(&checksum.to_le_bytes());
        self.socket.send_to(&packet, self.peer_addr)?;
        Ok(())
    }
}

/// Sender thread: reads file region and sends over UDP
pub fn sender_thread(
    file_path: &Path,
    range: FileRange,
    server_addr: &str,
    port: u16,
    buffer_size: usize,
    thread_id: usize,
    metadata: Option<FileMetadata>,
) -> Result<u64, TransferError> {
    // Open file
    let file = File::open(file_path)?;
    
    // Create UDP socket
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    let peer_addr: SocketAddr = format!("{}:{}", server_addr, port).parse()
        .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;
    
    let mut sender = ReliableSender::new(socket, peer_addr)?;
    
    // Only thread 0 sends file metadata
    if let Some(meta) = metadata {
        let mut meta_data = Vec::new();
        encode_varint(meta.filename.len() as u64, &mut meta_data);
        meta_data.extend_from_slice(meta.filename.as_bytes());
        encode_varint(meta.file_size, &mut meta_data);
        encode_varint(meta.num_threads as u64, &mut meta_data);
        sender.send_packet(PKT_METADATA, thread_id as u8, 0, &meta_data)?;
    }
    
    // Send range header
    let mut range_data = Vec::new();
    encode_varint(range.start, &mut range_data);
    encode_varint(range.end, &mut range_data);
    sender.send_packet(PKT_RANGE, thread_id as u8, 0, &range_data)?;
    
    // Allocate buffer
    let mut buffer = vec![0u8; buffer_size.min(DATA_PAYLOAD_SIZE)];
    let mut offset = range.start;
    let mut total_sent = 0u64;
    
    // Transfer loop: pread → send UDP packets
    while offset < range.end {
        let remaining = (range.end - offset) as usize;
        let read_size = std::cmp::min(buffer.len(), remaining);
        
        // Use pread for thread-safe reads
        #[cfg(target_os = "linux")]
        let bytes_read = {
            let fd = file.as_raw_fd();
            unsafe {
                libc::pread(
                    fd,
                    buffer.as_mut_ptr() as *mut libc::c_void,
                    read_size,
                    offset as libc::off_t,
                )
            }
        };
        #[cfg(not(target_os = "linux"))]
        let bytes_read = {
            let mut file_mut = File::try_clone(&file)?;
            file_mut.seek(std::io::SeekFrom::Start(offset))?;
            file_mut.read(&mut buffer[..read_size])? as i64
        };
        
        if bytes_read <= 0 {
            break;
        }
        
        let data = &buffer[..bytes_read as usize];
        
        // Split into packets if needed
        for chunk in data.chunks(DATA_PAYLOAD_SIZE) {
            sender.send_packet(PKT_DATA, thread_id as u8, offset, chunk)?;
            offset += chunk.len() as u64;
            total_sent += chunk.len() as u64;
        }
    }
    
    // Send FIN
    sender.send_packet(PKT_FIN, thread_id as u8, offset, &[])?;
    sender.finish()?;
    
    Ok(total_sent)
}

/// Receiver worker thread: receives UDP packets and writes to file
fn receiver_worker(
    socket: UdpSocket,
    peer_addr: SocketAddr,
    file: Arc<File>,
    _buffer_size: usize,
) -> Result<u64, TransferError> {
    let mut receiver = ReliableReceiver::new(socket, peer_addr)?;
    
    // Receive metadata or range header first
    let mut start_offset = 0u64;
    let mut end_offset = 0u64;
    let mut received_metadata = false;
    
    #[cfg(target_os = "linux")]
    let file_fd = file.as_raw_fd();
    
    loop {
        match receiver.recv_packet(Duration::from_secs(1))? {
            Some((offset, data)) => {
                if !received_metadata {
                    // First packet is range header
                    let (range_start, len1) = decode_varint_from_slice(&data)?;
                    let (range_end, _len2) = decode_varint_from_slice(&data[len1..])?;
                    start_offset = range_start;
                    end_offset = range_end;
                    received_metadata = true;
                    tracing::debug!("Received range: {} - {}", start_offset, end_offset);
                    continue;
                }
                
                // Write data to file at offset
                #[cfg(target_os = "linux")]
                {
                    unsafe {
                        let result = libc::pwrite(
                            file_fd,
                            data.as_ptr() as *const libc::c_void,
                            data.len(),
                            offset as libc::off_t,
                        );
                        if result < 0 {
                            return Err(TransferError::Io(std::io::Error::last_os_error()));
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let mut file_mut = File::try_clone(&*file)?;
                    file_mut.seek(std::io::SeekFrom::Start(offset))?;
                    file_mut.write_all(&data)?;
                }
            }
            None => {
                // Check if we're done (received FIN)
                if received_metadata && start_offset >= end_offset {
                    break;
                }
            }
        }
    }
    
    Ok(end_offset - start_offset)
}

/// Receiver coordinator - creates file, spawns worker threads
pub fn blocking_server_receive(
    output_dir: &Path,
    config: BlockingTransferConfig,
) -> Result<(String, u64), TransferError> {
    // Bind to base port for metadata
    let meta_addr = format!("0.0.0.0:{}", config.base_port);
    let meta_socket = UdpSocket::bind(&meta_addr)?;
    configure_udp_socket(&meta_socket)?;
    
    tracing::info!("Waiting for metadata on port {}", config.base_port);
    
    // Receive metadata packet
    let mut buf = [0u8; MAX_PACKET_SIZE];
    let (size, peer_addr) = meta_socket.recv_from(&mut buf)?;
    
    if size < 32 {
        return Err(TransferError::ProtocolError("Packet too small".to_string()));
    }
    
    let header = PacketHeader::from_bytes(&buf[..32])?;
    if header.packet_type != PKT_METADATA {
        return Err(TransferError::ProtocolError("Expected metadata packet".to_string()));
    }
    
    // Parse metadata
    let data = &buf[32..size];
    let (filename_len, len_bytes) = decode_varint_from_slice(data)?;
    let filename_start = len_bytes;
    let filename_end = filename_start + filename_len as usize;
    let filename = String::from_utf8(data[filename_start..filename_end].to_vec())
        .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;
    
    let (file_size, size_bytes) = decode_varint_from_slice(&data[filename_end..])?;
    let (num_threads_u64, _) = decode_varint_from_slice(&data[filename_end + size_bytes..])?;
    let num_threads = num_threads_u64 as usize;
    
    tracing::info!(
        "Receiving file: {} ({} bytes) with {} threads",
        filename,
        file_size,
        num_threads
    );
    
    // Create and pre-allocate output file
    let output_path = output_dir.join(&filename);
    let file = File::create(&output_path)?;
    file.set_len(file_size)?;
    let file = Arc::new(file);
    
    // Create sockets for all threads
    let mut sockets = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
        let port = config.base_port + i as u16;
        let addr = format!("0.0.0.0:{}", port);
        let socket = UdpSocket::bind(&addr)?;
        configure_udp_socket(&socket)?;
        sockets.push((i, socket, peer_addr));
    }
    
    // Spawn receiver workers
    let mut handles = Vec::with_capacity(num_threads);
    for (thread_id, socket, peer) in sockets {
        let file_clone = Arc::clone(&file);
        let handle = thread::spawn(move || {
            receiver_worker(socket, peer, file_clone, config.buffer_size)
        });
        handles.push((thread_id, handle));
    }
    
    // Wait for all threads
    let mut total_received = 0u64;
    for (thread_id, handle) in handles {
        match handle.join() {
            Ok(Ok(bytes)) => {
                total_received += bytes;
                tracing::info!("Thread {} received {} bytes", thread_id, bytes);
            }
            Ok(Err(e)) => {
                tracing::error!("Thread {} failed: {}", thread_id, e);
                return Err(e);
            }
            Err(_) => {
                return Err(TransferError::NetworkError(format!(
                    "Thread {} panicked",
                    thread_id
                )));
            }
        }
    }
    
    file.sync_all()?;
    
    tracing::info!("Server transfer complete: {} bytes received", total_received);
    Ok((filename, total_received))
}

/// Blocking client transfer
pub fn blocking_client_transfer(
    file_path: &Path,
    server_addr: &str,
    config: BlockingTransferConfig,
) -> Result<(), TransferError> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size = metadata.len();
    let filename = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();
    
    let ranges = split_file_ranges(file_size, config.num_threads);
    
    tracing::info!(
        "Starting UDP transfer: {} ({} bytes), {} threads",
        filename,
        file_size,
        config.num_threads
    );
    
    let start_time = Instant::now();
    let file_path = Arc::new(file_path.to_path_buf());
    let server_addr = Arc::new(server_addr.to_string());
    
    let metadata = FileMetadata {
        filename: filename.clone(),
        file_size,
        num_threads: config.num_threads,
    };
    
    // Spawn sender threads
    let mut handles = Vec::new();
    for (thread_id, range) in ranges.into_iter().enumerate() {
        let file_path = Arc::clone(&file_path);
        let server_addr = Arc::clone(&server_addr);
        let port = config.base_port + thread_id as u16;
        
        let metadata = if thread_id == 0 {
            Some(metadata.clone())
        } else {
            None
        };
        
        let handle = thread::spawn(move || {
            sender_thread(
                &file_path,
                range,
                &server_addr,
                port,
                config.buffer_size,
                thread_id,
                metadata,
            )
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    let mut total_sent = 0u64;
    for (thread_id, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(bytes)) => {
                total_sent += bytes;
                tracing::info!("Thread {} sent {} bytes", thread_id, bytes);
            }
            Ok(Err(e)) => {
                return Err(TransferError::NetworkError(format!(
                    "Thread {} failed: {}",
                    thread_id, e
                )));
            }
            Err(_) => {
                return Err(TransferError::NetworkError(format!(
                    "Thread {} panicked",
                    thread_id
                )));
            }
        }
    }
    
    let duration = start_time.elapsed();
    let throughput = (total_sent as f64 / duration.as_secs_f64()) / 1_000_000.0;
    
    tracing::info!(
        "Transfer complete: {} bytes in {:.2}s ({:.2} MB/s)",
        total_sent,
        duration.as_secs_f64(),
        throughput
    );
    
    Ok(())
}

/// Blocking server loop
pub fn blocking_server_loop(
    output_dir: &Path,
    config: BlockingTransferConfig,
) -> Result<(), TransferError> {
    std::fs::create_dir_all(output_dir)?;
    
    loop {
        tracing::info!("Waiting for new transfer...");
        match blocking_server_receive(output_dir, config.clone()) {
            Ok((filename, bytes)) => {
                tracing::info!("Transfer completed: {} ({} bytes)", filename, bytes);
            }
            Err(e) => {
                tracing::error!("Transfer failed: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf);
        assert_eq!(buf, vec![0]);
        
        buf.clear();
        encode_varint(127, &mut buf);
        assert_eq!(buf, vec![127]);
        
        buf.clear();
        encode_varint(128, &mut buf);
        assert_eq!(buf, vec![128, 1]);
    }

    #[test]
    fn test_file_range_splitting() {
        let ranges = split_file_ranges(1000, 3);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 333);
        assert_eq!(ranges[1].start, 333);
        assert_eq!(ranges[1].end, 666);
        assert_eq!(ranges[2].start, 666);
        assert_eq!(ranges[2].end, 1000);
    }
}
