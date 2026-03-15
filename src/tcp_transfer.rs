//! TCP-based file transfer with parallel connections.
//!
//! This module implements high-performance file transfer using multiple
//! parallel TCP connections, optimized socket settings, and zero-copy
//! file I/O where possible.

use crate::base::msg;
use crate::error::TransferError;
use crate::base::{split_file_ranges, transfer_num_ranges, FileRange, TransferConfig, TransferReport, TransferStatus};
use crate::metrics::BandwidthEstimator;
use crate::range_queue::{RangeQueue, StreamReport, WorkerId};
use crate::transport::{Stream as TransportStreamTrait, StreamOpener, Transport};
use crate::compression::{compress, decompress, should_compress};
use crate::encryption::{Decryptor, Encryptor};
use crate::file_io::{open_file_optimized, FileReader};
use crate::integrity::{hash_file, hash_file_range_path, BLAKE3_LEN};
use crate::progress::{ProgressHandle, TransferProgress};
use crate::resume::{delete_checkpoint, get_checkpoint_path, TransferCheckpoint};
use crate::utils::{is_file_compressible, optimal_streams_from_bdp, BANDWIDTH_PROBE_SIZE};
use std::collections::HashSet;

/// Data channel frame type: payload is a FEC-encoded block (RaptorQ). Inner payload starts with fec::FEC_PACKET_TYPE.
/// 0x00 = raw, 0x01 = compressed, 0x02 = FEC block.
const DATA_FRAME_FEC: u8 = 0x02;

/// Maximum chunk size we accept from the wire to avoid capacity overflow from corrupted or malicious data.
const MAX_RECEIVE_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Fallback max streams when bandwidth probe fails or we have no BDP (avoid over-scaling on WAN).
const FALLBACK_MAX_STREAMS: usize = 8;

/// Run bandwidth probe: send PROBE + size + data, then wait for PROBE_ACK. Measures end-to-end time so result reflects real link bandwidth.
pub(crate) async fn run_bandwidth_probe<W, R>(writer: &mut W, reader: &mut R) -> Option<u64>
where
    W: tokio::io::AsyncWrite + Unpin,
    R: tokio::io::AsyncRead + Unpin,
{
    const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
    let probe = async {
        let start = Instant::now();
        if let Err(e) = writer.write_all(&[msg::PROBE]).await {
            tracing::debug!(error = %e, "bandwidth probe: write PROBE byte failed");
            return None;
        }
        if let Err(e) = writer.write_all(&(BANDWIDTH_PROBE_SIZE as u64).to_le_bytes()).await {
            tracing::debug!(error = %e, "bandwidth probe: write size failed");
            return None;
        }
        const CHUNK: usize = 64 * 1024;
        let zeros = [0u8; CHUNK];
        let mut remaining = BANDWIDTH_PROBE_SIZE;
        while remaining > 0 {
            let n = remaining.min(CHUNK);
            if let Err(e) = writer.write_all(&zeros[..n]).await {
                tracing::debug!(error = %e, remaining, "bandwidth probe: write data failed");
                return None;
            }
            remaining -= n;
        }
        if let Err(e) = writer.flush().await {
            tracing::debug!(error = %e, "bandwidth probe: flush failed");
            return None;
        }
        let mut ack = [0u8; 1];
        if let Err(e) = reader.read_exact(&mut ack).await {
            tracing::debug!(error = %e, "bandwidth probe: read PROBE_ACK failed");
            return None;
        }
        if ack[0] != msg::PROBE_ACK {
            tracing::debug!(got = ack[0], "bandwidth probe: expected PROBE_ACK");
            return None;
        }
        let elapsed_secs = start.elapsed().as_secs_f64();
        if elapsed_secs < 0.01 {
            tracing::debug!(elapsed_secs, "bandwidth probe: completed too fast (measurement unreliable)");
            return None;
        }
        Some((BANDWIDTH_PROBE_SIZE as f64 / elapsed_secs) as u64)
    };
    match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
        Ok(Some(bps)) => Some(bps),
        Ok(None) => None,
        Err(_) => {
            tracing::debug!("bandwidth probe: timed out after 30s");
            None
        }
    }
}

use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Configure TCP socket for high throughput.
///
/// Sets socket buffer sizes, enables TCP_NODELAY, and configures
/// platform-specific optimizations:
/// - Linux: BBR congestion control, TCP_CORK (write batching), TCP_QUICKACK (immediate ACKs)
/// - All platforms: SO_KEEPALIVE with tuned parameters
pub fn configure_tcp_socket(
    socket: &Socket,
    send_buffer_size: Option<usize>,
    recv_buffer_size: Option<usize>,
) -> Result<(), TransferError> {
    // Set send buffer size if specified
    if let Some(size) = send_buffer_size {
        socket.set_send_buffer_size(size)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
        debug!("Socket send buffer set to {} bytes", size);
    }
    
    // Set receive buffer size if specified
    if let Some(size) = recv_buffer_size {
        socket.set_recv_buffer_size(size)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_RCVBUF: {}", e)))?;
        debug!("Socket receive buffer set to {} bytes", size);
    }
    
    // Enable TCP_NODELAY to disable Nagle's algorithm
    socket.set_nodelay(true)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e)))?;
    
    // Set BBR congestion control on Linux
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        let bbr = CString::new("bbr").map_err(|e| {
            TransferError::NetworkError(format!("Failed to create BBR string: {}", e))
        })?;
        
        use std::os::unix::io::AsRawFd;
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_CONGESTION,
                bbr.as_ptr() as *const libc::c_void,
                bbr.as_bytes().len() as libc::socklen_t,
            )
        };
        
        if result != 0 {
            // BBR might not be available (kernel < 4.9 or module not loaded)
            // This is not a fatal error, just log a debug message
            debug!("Failed to set TCP_CONGESTION to BBR, using default congestion control");
        } else {
            debug!("TCP congestion control set to BBR");
        }
        
        // Enable TCP_CORK for write batching
        // TCP_CORK batches multiple small writes into fewer packets, reducing syscall overhead
        let cork: libc::c_int = 1;
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_CORK,
                &cork as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        
        if result != 0 {
            debug!("Failed to set TCP_CORK, continuing without write batching");
        } else {
            debug!("TCP_CORK enabled for write batching");
        }
        
        // Enable TCP_QUICKACK to send ACKs immediately
        // This reduces latency and improves throughput, especially on the receiver side
        let quickack: libc::c_int = 1;
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_QUICKACK,
                &quickack as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        
        if result != 0 {
            debug!("Failed to set TCP_QUICKACK, continuing with default ACK behavior");
        } else {
            debug!("TCP_QUICKACK enabled for immediate ACKs");
        }
    }
    
    // Enable keepalive to detect dead connections quickly
    socket.set_keepalive(true)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_KEEPALIVE: {}", e)))?;
    
    // Set keepalive parameters (platform-specific)
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let keepidle: libc::c_int = 30; // Start keepalive after 30 seconds of inactivity
        let keepintvl: libc::c_int = 10; // Send keepalive probes every 10 seconds
        let keepcnt: libc::c_int = 3; // Send 3 probes before considering connection dead
        
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &keepidle as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_KEEPINTVL,
                &keepintvl as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_KEEPCNT,
                &keepcnt as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    
    Ok(())
}

/// Transfer a file range over a single TCP connection.
/// Final file hash is computed from per-range BLAKE3 hashes in the coordinator (no streaming hasher).
async fn transfer_range_tcp(
    thread_id: usize,
    range: FileRange,
    file: FileReader,
    mut stream: tokio::net::TcpStream,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
    metrics_tx: Option<mpsc::Sender<StreamReport>>,
    enable_fec_auto: Option<&AtomicBool>,
    cancel: Option<&CancellationToken>,
) -> Result<u64, TransferError> {
    tracing::debug!(
        thread_id,
        start = range.start,
        end = range.end,
        size = range.end - range.start,
        "Transferring file range over TCP"
    );

    let (_reader, mut writer) = stream.split();

    // Send range header: start (8) + end (8) + flags (1 byte: bit 0=compression, bit 1=encryption)
    let mut header = Vec::with_capacity(17);
    header.extend_from_slice(&range.start.to_le_bytes());
    header.extend_from_slice(&range.end.to_le_bytes());
    let mut flags = 0u8;
    if config.enable_compression {
        flags |= 0x01;
    }
    if config.enable_encryption {
        flags |= 0x02;
    }
    header.push(flags);
    writer.write_all(&header).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send range header: {}", e)))?;
    
    // Initialize encryptor if encryption is enabled
    let mut encryptor = if config.enable_encryption {
        config.encryption_key
            .map(|key| Encryptor::new(&key, thread_id as u32))
            .transpose()?
    } else {
        None
    };

    // Read and send file data (same buffer size as transport stream path).
    let mut offset = range.start;
    let buffer_size = config.buffer_size.min(16 * 1024 * 1024);
    let mut buffer = vec![0u8; buffer_size];
    let mut total_sent = 0u64;

    while offset < range.end {
        if cancel.map(|c| c.is_cancelled()).unwrap_or(false) {
            return Err(TransferError::Cancelled);
        }
        let chunk_start = Instant::now();
        let remaining = (range.end - offset) as usize;
        let read_size = buffer.len().min(remaining);

        let bytes_read = file.read_at(&mut buffer[..read_size], offset).await?;

        if bytes_read == 0 {
            break;
        }

        if cancel.map(|c| c.is_cancelled()).unwrap_or(false) {
            return Err(TransferError::Cancelled);
        }

        let use_fec = config.enable_fec
            || enable_fec_auto.map(|a| a.load(Ordering::Relaxed)).unwrap_or(false);
        let (frame_type, data_to_send) = if use_fec {
            #[cfg(feature = "fec")]
            {
                let fec_payload = crate::fec::encode_block(
                    &buffer[..bytes_read],
                    crate::fec::DEFAULT_FEC_SYMBOL_SIZE,
                    config.fec_repair_packets,
                )?;
                (DATA_FRAME_FEC, fec_payload)
            }
            #[cfg(not(feature = "fec"))]
            {
                let mut data = buffer[..bytes_read].to_vec();
                if config.enable_compression && should_compress(&data) {
                    if let Ok(c) = compress(&data) {
                        data = c;
                    }
                }
                if let Some(ref mut enc) = encryptor {
                    enc.encrypt_in_place(&mut data)?;
                }
                let flag = if config.enable_compression && data.len() < bytes_read { 0x01 } else { 0x00 };
                (flag, data)
            }
        } else {
            let mut data_to_send = buffer[..bytes_read].to_vec();
            if config.enable_compression && should_compress(&data_to_send) {
                if let Ok(compressed) = compress(&data_to_send) {
                    data_to_send = compressed;
                }
            }
            if let Some(ref mut enc) = encryptor {
                enc.encrypt_in_place(&mut data_to_send)?;
            }
            let compression_flag = if config.enable_compression && data_to_send.len() < bytes_read {
                0x01
            } else {
                0x00
            };
            (compression_flag, data_to_send)
        };

        // Send packet: frame type (1) + size (8) + data
        let mut packet = Vec::with_capacity(9 + data_to_send.len());
        packet.push(frame_type);
        packet.extend_from_slice(&(data_to_send.len() as u64).to_le_bytes());
        packet.extend_from_slice(&data_to_send);
        
        // Write in chunks; use large chunks to match TCP throughput. Retry on ENOBUFS / WouldBlock / EINVAL.
        const SEND_WRITE_CHUNK: usize = 16 * 1024 * 1024;
        const SEND_RETRY_DELAY_MS: u64 = 50;
        const SEND_RETRY_MAX: u32 = 120; // ~6s of retries then fail
        let mut written = 0usize;
        let mut retries = 0u32;
        while written < packet.len() {
            let chunk = (packet.len() - written).min(SEND_WRITE_CHUNK);
            match writer.write(&packet[written..written + chunk]).await {
                Ok(0) => return Err(TransferError::NetworkError("Connection closed during send".to_string())),
                Ok(n) => {
                    written += n;
                    retries = 0;
                }
                Err(e) => {
                    let retryable = e.raw_os_error() == Some(55) // ENOBUFS
                        || e.raw_os_error() == Some(22) // EINVAL on some kernels
                        || e.kind() == std::io::ErrorKind::WouldBlock;
                    if retryable && retries < SEND_RETRY_MAX {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(SEND_RETRY_DELAY_MS)).await;
                    } else {
                        return Err(TransferError::NetworkError(format!("Failed to send data: {}", e)));
                    }
                }
            }
        }

        offset += bytes_read as u64;
        total_sent += bytes_read as u64;

        if let Some(ref progress) = progress {
            progress.update(bytes_read as u64);
        }
        if let Some(ref tx) = metrics_tx {
            let _ = tx
                .try_send(StreamReport {
                    worker_id: thread_id,
                    bytes_this_interval: bytes_read as u64,
                    elapsed_ms: chunk_start.elapsed().as_millis() as u64,
                });
        }
    }

    // Flush and shutdown write side
    // Note: shutdown() automatically flushes any TCP_CORK batched data on Linux
    writer.shutdown().await
        .map_err(|e| TransferError::NetworkError(format!("Failed to shutdown stream: {}", e)))?;

    debug!(
        thread_id,
        bytes = total_sent,
        "File range transfer completed"
    );

    Ok(total_sent)
}

/// Transfer a file range over a transport stream (TCP or QUIC). Same protocol as transfer_range_tcp.
async fn transfer_range_stream(
    thread_id: usize,
    range: FileRange,
    file: FileReader,
    mut stream: Box<dyn TransportStreamTrait>,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
    metrics_tx: Option<mpsc::Sender<StreamReport>>,
    enable_fec_auto: Option<&AtomicBool>,
    cancel: Option<&CancellationToken>,
) -> Result<u64, TransferError> {
    tracing::debug!(
        thread_id,
        start = range.start,
        end = range.end,
        size = range.end - range.start,
        "Transferring file range over transport stream"
    );

    let mut header = Vec::with_capacity(17);
    header.extend_from_slice(&range.start.to_le_bytes());
    header.extend_from_slice(&range.end.to_le_bytes());
    let mut flags = 0u8;
    if config.enable_compression {
        flags |= 0x01;
    }
    if config.enable_encryption {
        flags |= 0x02;
    }
    header.push(flags);
    stream.write_all(&header).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send range header: {}", e)))?;

    let mut encryptor = if config.enable_encryption {
        config.encryption_key
            .map(|key| Encryptor::new(&key, thread_id as u32))
            .transpose()?
    } else {
        None
    };

    let mut offset = range.start;
    let buffer_size = config.buffer_size.min(16 * 1024 * 1024);
    let mut buffer = vec![0u8; buffer_size];
    let mut total_sent = 0u64;

    const SEND_RETRY_DELAY_MS: u64 = 50;
    const SEND_RETRY_MAX: u32 = 120;

    while offset < range.end {
        if cancel.map(|c| c.is_cancelled()).unwrap_or(false) {
            return Err(TransferError::Cancelled);
        }
        let chunk_start = Instant::now();
        let remaining = (range.end - offset) as usize;
        let read_size = buffer.len().min(remaining);

        let bytes_read = file.read_at(&mut buffer[..read_size], offset).await?;

        if bytes_read == 0 {
            break;
        }

        if cancel.map(|c| c.is_cancelled()).unwrap_or(false) {
            return Err(TransferError::Cancelled);
        }

        let use_fec = config.enable_fec
            || enable_fec_auto.map(|a| a.load(Ordering::Relaxed)).unwrap_or(false);
        let (frame_type, data_to_send) = if use_fec {
            #[cfg(feature = "fec")]
            {
                let fec_payload = crate::fec::encode_block(
                    &buffer[..bytes_read],
                    crate::fec::DEFAULT_FEC_SYMBOL_SIZE,
                    config.fec_repair_packets,
                )?;
                (DATA_FRAME_FEC, fec_payload)
            }
            #[cfg(not(feature = "fec"))]
            {
                let mut data = buffer[..bytes_read].to_vec();
                if config.enable_compression && should_compress(&data) {
                    if let Ok(c) = compress(&data) {
                        data = c;
                    }
                }
                if let Some(ref mut enc) = encryptor {
                    enc.encrypt_in_place(&mut data)?;
                }
                let flag = if config.enable_compression && data.len() < bytes_read { 0x01 } else { 0x00 };
                (flag, data)
            }
        } else {
            let mut data_to_send = buffer[..bytes_read].to_vec();
            if config.enable_compression && should_compress(&data_to_send) {
                if let Ok(compressed) = compress(&data_to_send) {
                    data_to_send = compressed;
                }
            }
            if let Some(ref mut enc) = encryptor {
                enc.encrypt_in_place(&mut data_to_send)?;
            }
            let compression_flag = if config.enable_compression && data_to_send.len() < bytes_read {
                0x01
            } else {
                0x00
            };
            (compression_flag, data_to_send)
        };

        let mut packet = Vec::with_capacity(9 + data_to_send.len());
        packet.push(frame_type);
        packet.extend_from_slice(&(data_to_send.len() as u64).to_le_bytes());
        packet.extend_from_slice(&data_to_send);

        const SEND_WRITE_CHUNK: usize = 16 * 1024 * 1024;
        let mut written = 0usize;
        let mut retries = 0u32;
        while written < packet.len() {
            let chunk = (packet.len() - written).min(SEND_WRITE_CHUNK);
            match stream.write(&packet[written..written + chunk]).await {
                Ok(0) => return Err(TransferError::NetworkError("Connection closed during send".to_string())),
                Ok(n) => {
                    written += n;
                    retries = 0;
                }
                Err(e) => {
                    let retryable = e.raw_os_error() == Some(55)
                        || e.raw_os_error() == Some(22) // EINVAL on some kernels/stacks
                        || e.kind() == std::io::ErrorKind::WouldBlock;
                    if retryable && retries < SEND_RETRY_MAX {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(SEND_RETRY_DELAY_MS)).await;
                    } else {
                        return Err(TransferError::NetworkError(format!("Failed to send data: {}", e)));
                    }
                }
            }
        }

        offset += bytes_read as u64;
        total_sent += bytes_read as u64;

        if let Some(ref progress) = progress {
            progress.update(bytes_read as u64);
        }
        if let Some(ref tx) = metrics_tx {
            let _ = tx.try_send(StreamReport {
                worker_id: thread_id,
                bytes_this_interval: bytes_read as u64,
                elapsed_ms: chunk_start.elapsed().as_millis() as u64,
            });
        }
    }

    stream.shutdown().await
        .map_err(|e| TransferError::NetworkError(format!("Failed to shutdown stream: {}", e)))?;

    debug!(
        thread_id,
        bytes = total_sent,
        "Transport stream range transfer completed"
    );

    Ok(total_sent)
}

/// Coordinator state for one active worker: last activity time, join handle, cancellation token, and consecutive zero-byte intervals (stall only when no progress for N intervals).
struct WorkerState {
    last_active: Instant,
    handle: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
    /// Number of consecutive 2s intervals with no bytes from this worker. Stall only when this >= threshold.
    consecutive_zero_intervals: u32,
}

/// One TCP stream worker: pulls ranges from queue, connects to pre-assigned port, sends range, reports completion or requeues on error.
async fn tcp_stream_worker(
    id: WorkerId,
    queue: Arc<RangeQueue>,
    file: FileReader,
    file_path: PathBuf,
    config: TransferConfig,
    server_ip: std::net::IpAddr,
    base_port: u16,
    progress: Option<ProgressHandle>,
    metrics_tx: Option<mpsc::Sender<StreamReport>>,
    completed_tx: mpsc::Sender<(WorkerId, FileRange, [u8; BLAKE3_LEN])>,
    done_tx: mpsc::Sender<(WorkerId, Result<(), TransferError>)>,
    enable_fec_auto: Option<Arc<AtomicBool>>,
    cancel: CancellationToken,
) {
    let target_port = base_port + 1 + id as u16;
    loop {
        // Check BEFORE popping — a cancelled worker must not take a range (avoids duplicate pop when respawned).
        if cancel.is_cancelled() {
            let _ = done_tx.send((id, Ok(()))).await;
            return;
        }
        let Some(range) = queue.pop_and_mark(id) else { break };
        // Check cancel before any network I/O; if we were just cancelled/requeued, put range back and exit.
        if cancel.is_cancelled() {
            queue.requeue(id);
            let _ = done_tx.send((id, Ok(()))).await;
            return;
        }
        let result = async {
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
            configure_tcp_socket(
                &socket,
                config.socket_send_buffer_size,
                config.socket_recv_buffer_size,
            )?;
            let server_addr = SocketAddr::new(server_ip, target_port);
            socket.connect(&server_addr.into())
                .map_err(|e| TransferError::NetworkError(format!("Connection failed: {}", e)))?;
            socket.set_nonblocking(true)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
            let std_stream = std::net::TcpStream::from(socket);
            let stream = TcpStream::from_std(std_stream)
                .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio stream: {}", e)))?;
            transfer_range_tcp(id, range, file.clone(), stream, config.clone(), progress.clone(), metrics_tx.clone(), enable_fec_auto.as_deref(), Some(&cancel)).await
        }
        .await;
        match result {
            Ok(_) => {
                if cancel.is_cancelled() {
                    let _ = done_tx.send((id, Ok(()))).await;
                    return;
                }
                let range_hash = match hash_file_range_path(&file_path, range.start, range.end) {
                    Ok(h) => h,
                    Err(e) => {
                        queue.requeue(id);
                        let _ = done_tx.send((id, Err(e))).await;
                        return;
                    }
                };
                let _ = completed_tx.send((id, range, range_hash)).await;
                queue.complete(id);
                // Check after completing — before looping back; if cancelled, exit without popping again.
                if cancel.is_cancelled() {
                    let _ = done_tx.send((id, Ok(()))).await;
                    return;
                }
            }
            Err(TransferError::Cancelled) => {
                let _ = done_tx.send((id, Ok(()))).await;
                return;
            }
            Err(e) => {
                queue.requeue(id);
                let _ = done_tx.send((id, Err(e))).await;
                return;
            }
        }
    }
    let _ = done_tx.send((id, Ok(()))).await;
}

/// One transport stream worker (TCP or QUIC via opener): pulls ranges from queue, opens a new stream per range, sends, reports completion or requeues.
async fn transport_stream_worker(
    id: WorkerId,
    opener: Arc<dyn StreamOpener>,
    queue: Arc<RangeQueue>,
    file: FileReader,
    file_path: PathBuf,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
    metrics_tx: mpsc::Sender<StreamReport>,
    completed_tx: mpsc::Sender<(WorkerId, FileRange, [u8; BLAKE3_LEN])>,
    done_tx: mpsc::Sender<(WorkerId, Result<(), TransferError>)>,
    enable_fec_auto: Option<Arc<AtomicBool>>,
    cancel: CancellationToken,
) {
    let mut ranges_handled = 0u32;
    loop {
        // Check BEFORE popping — a cancelled worker must not take a range (avoids duplicate pop when respawned).
        if cancel.is_cancelled() {
            let _ = done_tx.send((id, Ok(()))).await;
            return;
        }
        let Some(range) = queue.pop_and_mark(id) else {
            tracing::debug!(worker_id = id, ranges_handled, "queue empty, exiting");
            break;
        };
        tracing::debug!(worker_id = id, range = ?range, remaining = queue.pending_count(), "popped range");
        // Check cancel before any network I/O; if we were just cancelled/requeued, put range back and exit.
        if cancel.is_cancelled() {
            queue.requeue(id);
            let _ = done_tx.send((id, Ok(()))).await;
            return;
        }
        let stream = match opener.open_stream().await {
            Ok(s) => s,
            Err(e) => {
                queue.requeue(id);
                let _ = done_tx.send((id, Err(e))).await;
                return;
            }
        };
        let stream = stream as Box<dyn TransportStreamTrait>;
        let result = transfer_range_stream(
            id,
            range,
            file.clone(),
            stream,
            config.clone(),
            progress.clone(),
            Some(metrics_tx.clone()),
            enable_fec_auto.as_deref(),
            Some(&cancel),
        )
        .await;
        match result {
            Ok(_) => {
                if cancel.is_cancelled() {
                    let _ = done_tx.send((id, Ok(()))).await;
                    return;
                }
                let range_hash = match hash_file_range_path(&file_path, range.start, range.end) {
                    Ok(h) => h,
                    Err(e) => {
                        queue.requeue(id);
                        let _ = done_tx.send((id, Err(e))).await;
                        return;
                    }
                };
                let _ = completed_tx.send((id, range, range_hash)).await;
                queue.complete(id);
                ranges_handled += 1;
                // Check after completing — before looping back; if cancelled, exit without popping again.
                if cancel.is_cancelled() {
                    let _ = done_tx.send((id, Ok(()))).await;
                    return;
                }
            }
            Err(TransferError::Cancelled) => {
                let _ = done_tx.send((id, Ok(()))).await;
                return;
            }
            Err(e) => {
                queue.requeue(id);
                let _ = done_tx.send((id, Err(e))).await;
                return;
            }
        }
    }
    tracing::debug!(worker_id = id, ranges_handled, "worker finished");
    let _ = done_tx.send((id, Ok(()))).await;
}

/// Receive a file range over a single TCP (or TLS) connection.
/// If `range_hash_tx` is Some, BLAKE3 of this range's plaintext is sent on completion (for optional per-range verification).
pub async fn receive_range_tcp<S>(
    thread_id: usize,
    file: Arc<File>,
    stream: S,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
    range_hash_tx: Option<mpsc::Sender<(FileRange, [u8; BLAKE3_LEN])>>,
) -> Result<u64, TransferError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let (mut reader, _writer) = tokio::io::split(stream);

    // Read range header: start (8) + end (8) + flags (1)
    let mut start_buf = [0u8; 8];
    reader.read_exact(&mut start_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read start offset: {}", e)))?;
    let start_offset = u64::from_le_bytes(start_buf);

    let mut end_buf = [0u8; 8];
    reader.read_exact(&mut end_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read end offset: {}", e)))?;
    let end_offset = u64::from_le_bytes(end_buf);
    
    let mut flags_buf = [0u8; 1];
    reader.read_exact(&mut flags_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read flags: {}", e)))?;
    let flags = flags_buf[0];
    let _compression_enabled = (flags & 0x01) != 0;
    let encryption_enabled = (flags & 0x02) != 0;
    
    // Initialize decryptor if encryption is enabled
    let mut decryptor: Option<Decryptor> = if encryption_enabled {
        config.encryption_key
            .map(|key| Decryptor::new(&key, thread_id as u32))
            .transpose()?
    } else {
        None
    };

    tracing::debug!(
        thread_id,
        start = start_offset,
        end = end_offset,
        size = end_offset - start_offset,
        "Receiving file range over TCP"
    );

    // Receive and write data
    let mut offset = start_offset;
    let mut buffer = vec![0u8; config.buffer_size.min(16 * 1024 * 1024)];
    let mut decompress_buffer = Vec::new();
    let mut total_received = 0u64;

    let range_len = end_offset - start_offset;
    let range = FileRange { start: start_offset, end: end_offset };
    let mut range_hasher = range_hash_tx.is_some().then(blake3::Hasher::new);
    while offset < end_offset {
        let remaining = (end_offset - offset) as usize;
        let read_size = buffer.len().min(remaining);

        // Read packet: compression flag (1) + size (8) + data. EOF/closed here after full range = sender closed, success.
        let mut flag_buf = [0u8; 1];
        match reader.read_exact(&mut flag_buf).await {
            Ok(_) => {}
            Err(e) => {
                // Sender closes stream after last chunk; treat any read error as success if we got the full range.
                if total_received >= range_len {
                    return Ok(total_received);
                }
                return Err(TransferError::NetworkError(format!("Failed to read compression flag: {}", e)));
            }
        }

        let mut size_buf = [0u8; 8];
        reader.read_exact(&mut size_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read data size: {}", e)))?;
        let data_size = u64::from_le_bytes(size_buf) as usize;
        
        if data_size == 0 {
            break;
        }
        if data_size > MAX_RECEIVE_CHUNK_SIZE {
            return Err(TransferError::ProtocolError(format!(
                "Chunk size {} exceeds maximum {} (possible protocol desync or client/server version mismatch)",
                data_size, MAX_RECEIVE_CHUNK_SIZE
            )));
        }
        
        // Read encrypted/compressed data
        let mut data_buf = vec![0u8; data_size];
        reader.read_exact(&mut data_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read data: {}", e)))?;
        
        // Decrypt if enabled
        if let Some(ref mut dec) = decryptor {
            let plaintext_len = dec.decrypt_in_place(&mut data_buf)?;
            data_buf.truncate(plaintext_len);
        }

        // FEC block (0x02): decode then use decoded payload as this chunk
        let (bytes_read, data) = if flag_buf[0] == DATA_FRAME_FEC {
            #[cfg(feature = "fec")]
            {
                let decoded = crate::fec::decode_fec_block(&data_buf)?
                    .ok_or_else(|| TransferError::ProtocolError("FEC decode failed (insufficient packets)".to_string()))?;
                let len = decoded.len();
                decompress_buffer = decoded;
                (len, decompress_buffer[..].as_ref())
            }
            #[cfg(not(feature = "fec"))]
            return Err(TransferError::ProtocolError("Received FEC block but FEC support not compiled in".to_string()));
        } else {
            // Decompress if needed (0x01) or raw (0x00)
            let bytes_read = if flag_buf[0] == 0x01 {
                decompress_buffer = decompress(&data_buf, read_size)?;
                decompress_buffer.len()
            } else {
                let copy_len = data_buf.len().min(buffer.len());
                buffer[..copy_len].copy_from_slice(&data_buf[..copy_len]);
                copy_len
            };

            if bytes_read == 0 {
                break;
            }

            let data: &[u8] = if flag_buf[0] == 0x01 && !decompress_buffer.is_empty() {
                &decompress_buffer[..bytes_read]
            } else {
                &buffer[..bytes_read]
            };
            (bytes_read, data)
        };

        if bytes_read == 0 {
            break;
        }

        if let Some(ref mut h) = range_hasher {
            h.update(data);
        }

        // Use pwrite for thread-safe writes (Unix) or seek+write (other platforms)
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let written = unsafe {
                libc::pwrite(
                    fd,
                    data.as_ptr() as *const libc::c_void,
                    bytes_read,
                    offset as libc::off_t,
                )
            };

            if written < 0 {
                return Err(TransferError::Io(std::io::Error::last_os_error()));
            }

            if written as usize != bytes_read {
                return Err(TransferError::ProtocolError(
                    format!("Partial write: {} != {}", written, bytes_read)
                ));
            }
        }

        #[cfg(not(unix))]
        {
            let mut file_mut = File::try_clone(&*file)?;
            use std::io::SeekFrom;
            file_mut.seek(SeekFrom::Start(offset))?;
            file_mut.write_all(data)?;
        }

        offset += bytes_read as u64;
        total_received += bytes_read as u64;
        
        // Update progress if provided
        if let Some(ref progress) = progress {
            progress.update(bytes_read as u64);
        }
    }

    // Verify we received the expected amount of data
    let expected_size = end_offset - start_offset;
    if total_received != expected_size {
        return Err(TransferError::ProtocolError(
            format!("Range size mismatch: received {} bytes, expected {} bytes (range {} to {})",
                total_received, expected_size, start_offset, end_offset)
        ));
    }

    if let Some(tx) = range_hash_tx {
        if let Some(h) = range_hasher {
            let _ = tx.send((range, *h.finalize().as_bytes())).await;
        }
    }

    // Sync file data to ensure it's written to disk
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        unsafe {
            libc::fsync(fd);
        }
    }

    #[cfg(not(unix))]
    {
        let file_mut = File::try_clone(&*file)?;
        file_mut.sync_all()?;
    }

    tracing::debug!(
        thread_id,
        bytes = total_received,
        expected = expected_size,
        "File range reception completed"
    );

    Ok(total_received)
}

/// Receive a file range over a transport stream (QUIC or TCP). Same protocol as receive_range_tcp; only reads from stream.
pub(crate) async fn receive_range_stream(
    thread_id: usize,
    file: Arc<File>,
    mut stream: Box<dyn TransportStreamTrait>,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
    range_hash_tx: Option<mpsc::Sender<(FileRange, [u8; BLAKE3_LEN])>>,
) -> Result<u64, TransferError> {
    let mut start_buf = [0u8; 8];
    stream.read_exact(&mut start_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read start offset: {}", e)))?;
    let start_offset = u64::from_le_bytes(start_buf);

    let mut end_buf = [0u8; 8];
    stream.read_exact(&mut end_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read end offset: {}", e)))?;
    let end_offset = u64::from_le_bytes(end_buf);

    let mut flags_buf = [0u8; 1];
    stream.read_exact(&mut flags_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read flags: {}", e)))?;
    let flags = flags_buf[0];
    let _compression_enabled = (flags & 0x01) != 0;
    let encryption_enabled = (flags & 0x02) != 0;

    let mut decryptor: Option<Decryptor> = if encryption_enabled {
        config.encryption_key
            .map(|key| Decryptor::new(&key, thread_id as u32))
            .transpose()?
    } else {
        None
    };

    let range = FileRange { start: start_offset, end: end_offset };
    let mut range_hasher = range_hash_tx.is_some().then(blake3::Hasher::new);

    tracing::debug!(
        thread_id,
        start = start_offset,
        end = end_offset,
        size = end_offset - start_offset,
        "Receiving file range over transport stream"
    );

    let mut offset = start_offset;
    let mut buffer = vec![0u8; config.buffer_size.min(16 * 1024 * 1024)];
    let mut decompress_buffer = Vec::new();
    let mut total_received = 0u64;

    while offset < end_offset {
        let remaining = (end_offset - offset) as usize;
        let read_size = buffer.len().min(remaining);

        let mut flag_buf = [0u8; 1];
        stream.read_exact(&mut flag_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read compression flag: {}", e)))?;

        let mut size_buf = [0u8; 8];
        stream.read_exact(&mut size_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read data size: {}", e)))?;
        let data_size = u64::from_le_bytes(size_buf) as usize;

        if data_size == 0 {
            break;
        }
        if data_size > MAX_RECEIVE_CHUNK_SIZE {
            return Err(TransferError::ProtocolError(format!(
                "Chunk size {} exceeds maximum {} (possible protocol desync or client/server version mismatch)",
                data_size, MAX_RECEIVE_CHUNK_SIZE
            )));
        }

        let mut data_buf = vec![0u8; data_size];
        stream.read_exact(&mut data_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read data: {}", e)))?;

        if let Some(ref mut dec) = decryptor {
            let plaintext_len = dec.decrypt_in_place(&mut data_buf)?;
            data_buf.truncate(plaintext_len);
        }

        let (bytes_read, data) = if flag_buf[0] == DATA_FRAME_FEC {
            #[cfg(feature = "fec")]
            {
                let decoded = crate::fec::decode_fec_block(&data_buf)?
                    .ok_or_else(|| TransferError::ProtocolError("FEC decode failed (insufficient packets)".to_string()))?;
                let len = decoded.len();
                decompress_buffer = decoded;
                (len, decompress_buffer[..].as_ref())
            }
            #[cfg(not(feature = "fec"))]
            return Err(TransferError::ProtocolError("Received FEC block but FEC support not compiled in".to_string()));
        } else {
            let bytes_read = if flag_buf[0] == 0x01 {
                decompress_buffer = decompress(&data_buf, read_size)?;
                decompress_buffer.len()
            } else {
                let copy_len = data_buf.len().min(buffer.len());
                buffer[..copy_len].copy_from_slice(&data_buf[..copy_len]);
                copy_len
            };

            if bytes_read == 0 {
                break;
            }

            let data: &[u8] = if flag_buf[0] == 0x01 && !decompress_buffer.is_empty() {
                &decompress_buffer[..bytes_read]
            } else {
                &buffer[..bytes_read]
            };
            (bytes_read, data)
        };

        if bytes_read == 0 {
            break;
        }

        if let Some(ref mut h) = range_hasher {
            h.update(data);
        }

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let written = unsafe {
                libc::pwrite(
                    fd,
                    data.as_ptr() as *const libc::c_void,
                    bytes_read,
                    offset as libc::off_t,
                )
            };
            if written < 0 {
                return Err(TransferError::Io(std::io::Error::last_os_error()));
            }
            if written as usize != bytes_read {
                return Err(TransferError::ProtocolError(
                    format!("Partial write: {} != {}", written, bytes_read)
                ));
            }
        }

        #[cfg(not(unix))]
        {
            let mut file_mut = File::try_clone(&*file)?;
            use std::io::SeekFrom;
            file_mut.seek(SeekFrom::Start(offset))?;
            file_mut.write_all(data)?;
        }

        offset += bytes_read as u64;
        total_received += bytes_read as u64;

        if let Some(ref progress) = progress {
            progress.update(bytes_read as u64);
        }
    }

    let expected_size = end_offset - start_offset;
    if total_received != expected_size {
        return Err(TransferError::ProtocolError(
            format!("Range size mismatch: received {} bytes, expected {} bytes (range {} to {})",
                total_received, expected_size, start_offset, end_offset)
        ));
    }

    if let Some(tx) = range_hash_tx {
        if let Some(h) = range_hasher {
            let _ = tx.send((range, *h.finalize().as_bytes())).await;
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        unsafe { libc::fsync(fd); }
    }
    #[cfg(not(unix))]
    {
        let file_mut = File::try_clone(&*file)?;
        file_mut.sync_all()?;
    }

    tracing::debug!(
        thread_id,
        bytes = total_received,
        expected = expected_size,
        "Transport stream range reception completed"
    );

    Ok(total_received)
}

/// Send a file using parallel TCP connections.
/// When `file_reader` is `Some` (e.g. `FileReader::Uring` from inside `tokio_uring::start()`), it is used for reads; otherwise the file is opened with `open_file_optimized`.
pub async fn send_file_tcp(
    file_path: &std::path::Path,
    server_addr: SocketAddr,
    config: TransferConfig,
    file_reader: Option<FileReader>,
) -> Result<u64, TransferError> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size = metadata.len();

    if file_size == 0 {
        return Err(TransferError::ProtocolError("Cannot transfer empty file".to_string()));
    }

    let filename = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    let mut config = config;
    if !is_file_compressible(file_path) {
        config.enable_compression = false;
    }

    // Only log if multiple files or debug mode
    tracing::debug!(
        file = %filename,
        size = file_size,
        connections = config.num_streams,
        "Starting transfer"
    );

    // Create progress tracker with filename in message
    let progress = TransferProgress::new(file_size, true);
    if let Some(ref pb) = progress.progress_bar {
        pb.set_message(format!("{}", filename));
    }
    let progress_handle = progress.handle();
    
    // Spawn throughput logger (only for debug logging, not visible in normal operation)
    let progress_for_logger = progress.handle();
    let filename_for_logger = filename.clone();
    let total_bytes = file_size;
    let start_time = std::time::Instant::now();
    let logger_handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5)); // Log every 5 seconds instead of 1
        loop {
            interval.tick().await;
            let transferred = progress_for_logger.transferred.load(std::sync::atomic::Ordering::Relaxed);
            if transferred >= total_bytes {
                break; // Transfer complete
            }
            if transferred > 0 {
                let elapsed = start_time.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    let mbps = (transferred as f64 / (1024.0 * 1024.0)) / elapsed;
                    // Use debug level instead of info to reduce noise
                    tracing::debug!(file = %filename_for_logger, throughput_mbps = mbps, transferred = transferred, total = total_bytes, "Transfer progress");
                }
            }
        }
    });

    // Send metadata on base port
    let metadata_socket = Socket::new(Domain::IPV4, Type::STREAM, None)
        .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
    
    // Configure metadata socket with configurable buffer sizes
    if let Some(size) = config.socket_send_buffer_size {
        metadata_socket.set_send_buffer_size(size)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
    }
    if let Some(size) = config.socket_recv_buffer_size {
        metadata_socket.set_recv_buffer_size(size)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_RCVBUF: {}", e)))?;
    }
    metadata_socket.set_nodelay(true)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e)))?;

    metadata_socket.connect(&server_addr.into())
        .map_err(|e| TransferError::NetworkError(format!("Connection failed: {}", e)))?;

    metadata_socket.set_nonblocking(true)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
    
    let std_stream = std::net::TcpStream::from(metadata_socket);
    let mut metadata_stream = TcpStream::from_std(std_stream)
        .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio stream: {}", e)))?;

    let (mut reader, mut writer) = metadata_stream.split();

    // Capability handshake: client sends, server responds with negotiated; both use intersection.
    let client_caps = crate::base::capabilities_from_config(&config);
    writer.write_all(&client_caps.to_bytes()).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send capabilities: {}", e)))?;
    let mut cap_buf = [0u8; crate::base::CAPABILITIES_WIRE_LEN];
    reader.read_exact(&mut cap_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read negotiated capabilities: {}", e)))?;
    let negotiated = crate::base::Capabilities::from_bytes(&cap_buf)
        .ok_or_else(|| TransferError::ProtocolError("Invalid capabilities from server".to_string()))?;
    let config = crate::base::apply_capabilities_to_config(&config, negotiated);

    // RTT probe when both sides set RTT_PROBE. Used for stall threshold and scale-up cooldown.
    let rtt_ms = if (negotiated.flags & crate::base::cap_flags::RTT_PROBE) != 0 {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let mut ping = [0u8; crate::base::PING_PONG_WIRE_LEN];
        ping[0] = crate::base::msg::PING;
        ping[1..9].copy_from_slice(&ts.to_le_bytes());
        writer.write_all(&ping).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send PING: {}", e)))?;
        let mut pong_buf = [0u8; crate::base::PING_PONG_WIRE_LEN];
        reader.read_exact(&mut pong_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read PONG: {}", e)))?;
        if pong_buf[0] != crate::base::msg::PONG {
            return Err(TransferError::ProtocolError("Expected PONG after PING".to_string()));
        }
        let ts_back = u64::from_le_bytes(pong_buf[1..9].try_into().unwrap());
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let ms = (now_us.saturating_sub(ts_back)) / 1000;
        tracing::debug!(rtt_ms = ms, "RTT probe");
        ms
    } else {
        100u64
    };

    // Measurement-driven stream count: bandwidth probe only when server supports BANDWIDTH_PROBE.
    let bandwidth_bps = if (negotiated.flags & crate::base::cap_flags::BANDWIDTH_PROBE) != 0 {
        run_bandwidth_probe(&mut writer, &mut reader).await
    } else {
        None
    };
    let effective_max_streams = if let Some(bw) = bandwidth_bps {
        let bdp_streams = optimal_streams_from_bdp(bw, rtt_ms, config.buffer_size, config.max_streams);
        bdp_streams.max(FALLBACK_MAX_STREAMS).min(config.max_streams)
    } else {
        config.max_streams.min(FALLBACK_MAX_STREAMS)
    };

    // Step 4: Send metadata using effective_max_streams (same value server uses for listeners and ranges).
    let metadata = {
        let filename_bytes = filename.as_bytes();
        let mut m = Vec::new();
        m.extend_from_slice(&(filename_bytes.len() as u64).to_le_bytes());
        m.extend_from_slice(filename_bytes);
        m.extend_from_slice(&file_size.to_le_bytes());
        m.extend_from_slice(&(effective_max_streams as u64).to_le_bytes());
        m
    };
    writer.write_all(&metadata).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send metadata: {}", e)))?;

    // Wait for initial ACK (0x01) - server is ready for data connections
    let mut ack_buf = [0u8; 1];
    reader.read_exact(&mut ack_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to receive initial ack: {}", e)))?;

    if ack_buf[0] != msg::READY {
        return Err(TransferError::ProtocolError("Invalid initial ACK from server".to_string()));
    }

    // Check for existing checkpoint (resume support)
    let checkpoint_path = get_checkpoint_path(file_path);
    let mut checkpoint = if checkpoint_path.exists() {
        tracing::debug!("Found checkpoint file, attempting to resume transfer");
        match TransferCheckpoint::load(&checkpoint_path) {
            Ok(cp) if cp.file_size == file_size => cp,
            Ok(_) => {
                tracing::debug!("Checkpoint file size mismatch, starting fresh transfer");
                delete_checkpoint(file_path)?;
                TransferCheckpoint::new(file_size)
            }
            Err(e) => {
                tracing::debug!(error = %e, "Failed to load checkpoint, starting fresh transfer");
                delete_checkpoint(file_path)?;
                TransferCheckpoint::new(file_size)
            }
        }
    } else {
        TransferCheckpoint::new(file_size)
    };

    // Step 5: Build ranges using effective_max_streams (same value as step 4 metadata).
    let all_ranges = split_file_ranges(file_size, transfer_num_ranges(effective_max_streams));

    // Per-range verification (before opening data streams): send 0x06 if we have completed ranges with hashes.
    let completed_ordered = checkpoint.completed_ranges_ordered(&all_ranges);
    if !completed_ordered.is_empty() {
        writer.write_all(&[msg::RANGE_HASHES]).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to send range hashes type: {}", e))
        })?;
        writer
            .write_all(&(completed_ordered.len() as u64).to_le_bytes())
            .await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send num_ranges: {}", e)))?;
        for (range, hash) in &completed_ordered {
            writer.write_all(&range.start.to_le_bytes()).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to send range start: {}", e))
            })?;
            writer.write_all(&range.end.to_le_bytes()).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to send range end: {}", e))
            })?;
            writer.write_all(hash).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to send range hash: {}", e))
            })?;
        }
        let mut resp = [0u8; 1];
        reader.read_exact(&mut resp).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to read range verify result type: {}", e))
        })?;
        if resp[0] != msg::RANGE_VERIFY_RESULT {
            return Err(TransferError::ProtocolError(format!(
                "Expected range verify result (0x07), got 0x{:02x}",
                resp[0]
            )));
        }
        let mut num_failed_buf = [0u8; 8];
        reader.read_exact(&mut num_failed_buf).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to read num_failed: {}", e))
        })?;
        let num_failed = u64::from_le_bytes(num_failed_buf) as usize;
        for _ in 0..num_failed {
            let mut start_buf = [0u8; 8];
            let mut end_buf = [0u8; 8];
            reader.read_exact(&mut start_buf).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to read failed range start: {}", e))
            })?;
            reader.read_exact(&mut end_buf).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to read failed range end: {}", e))
            })?;
            let start = u64::from_le_bytes(start_buf);
            let end = u64::from_le_bytes(end_buf);
            checkpoint.remove_completed(&FileRange { start, end });
        }
        checkpoint.save(&checkpoint_path)?;
    } else {
        writer.write_all(&[msg::NO_RANGE_VERIFY]).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to send no range verify: {}", e))
        })?;
    }

    let missing_ranges = checkpoint.get_missing_ranges(&all_ranges);
    if missing_ranges.is_empty() {
        delete_checkpoint(file_path)?;
        return Ok(file_size);
    }

    let is_resume = checkpoint.completed_ranges.len() > 0;
    if is_resume {
        tracing::debug!(
            completed = checkpoint.completed_ranges.len(),
            remaining = missing_ranges.len(),
            "Resuming transfer"
        );
    }

    let file = match file_reader {
        Some(r) => r,
        None => FileReader::std(open_file_optimized(file_path, file_size)?),
    };

    // Full transfer: final hash = combine per-range BLAKE3 hashes (no streaming). Resume: full-file hash at start.
    let resume_hash = if is_resume {
        Some(hash_file(file_path, file_size)?)
    } else {
        None
    };

    // Range queue: workers pull from pending, report completion or requeue on error. Coordinator updates checkpoint, stall detection, and smart scale-up.
    let num_pending = missing_ranges.len();
    let queue = Arc::new(RangeQueue::new(missing_ranges));
    let (completed_tx, mut completed_rx) = mpsc::channel::<(WorkerId, FileRange, [u8; BLAKE3_LEN])>(64);
    let (done_tx, mut done_rx) = mpsc::channel::<(WorkerId, Result<(), TransferError>)>(32);
    let (metrics_tx, mut metrics_rx) = mpsc::channel::<StreamReport>(256);
    let base_port = server_addr.port();
    let server_ip = server_addr.ip();
    let file_path_buf = file_path.to_path_buf();
    let num_workers_initial = config.num_streams.min(num_pending).min(effective_max_streams).max(1);
    let mut next_worker_id = num_workers_initial;
    let mut active_workers: std::collections::HashMap<WorkerId, WorkerState> = std::collections::HashMap::new();
    let mut completed_worker_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut completed_ranges: Vec<(FileRange, [u8; BLAKE3_LEN])> = Vec::new();
    let mut stale_completions: HashSet<(WorkerId, u64, u64)> = HashSet::new();
    let mut estimator = BandwidthEstimator::new(10);
    let mut last_change = Instant::now();
    let mut last_scale_up: Option<(Instant, f64)> = None;
    let mut scale_up_disabled = false;

    #[cfg(feature = "fec")]
    let enable_fec_auto: Option<Arc<AtomicBool>> = if config.fec_negotiated {
        Some(Arc::new(AtomicBool::new(false)))
    } else {
        None
    };
    #[cfg(not(feature = "fec"))]
    let enable_fec_auto: Option<Arc<AtomicBool>> = None;

    let spawn_worker = |id: WorkerId,
                        queue: &Arc<RangeQueue>,
                        file: &FileReader,
                        file_path_buf: &PathBuf,
                        config: &TransferConfig,
                        progress_handle: &ProgressHandle,
                        metrics_tx: &mpsc::Sender<StreamReport>,
                        completed_tx: &mpsc::Sender<(WorkerId, FileRange, [u8; BLAKE3_LEN])>,
                        done_tx: &mpsc::Sender<(WorkerId, Result<(), TransferError>)>,
                        enable_fec_auto: &Option<Arc<AtomicBool>>,
                        cancel_child: CancellationToken|
     -> tokio::task::JoinHandle<()> {
        let queue = Arc::clone(queue);
        let file = file.clone();
        let file_path = file_path_buf.clone();
        let config = config.clone();
        let progress = progress_handle.clone();
        let metrics_tx = metrics_tx.clone();
        let completed_tx = completed_tx.clone();
        let done_tx = done_tx.clone();
        let enable_fec_auto = enable_fec_auto.clone();
        tokio::spawn(async move {
            tcp_stream_worker(
                id,
                queue,
                file,
                file_path,
                config,
                server_ip,
                base_port,
                Some(progress),
                Some(metrics_tx),
                completed_tx,
                done_tx,
                enable_fec_auto,
                cancel_child,
            )
            .await
        })
    };

    for id in 0..num_workers_initial {
        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let handle = spawn_worker(
            id,
            &queue,
            &file,
            &file_path_buf,
            &config,
            &progress_handle,
            &metrics_tx,
            &completed_tx,
            &done_tx,
            &enable_fec_auto,
            cancel_child,
        );
        active_workers.insert(id, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
    }

    let mut interval = interval(Duration::from_secs(2));
    loop {
        tokio::select! {
            completed = completed_rx.recv() => {
                match completed {
                    Some((id, range, range_hash)) => {
                        let key = (id, range.start, range.end);
                        if stale_completions.remove(&key) {
                            tracing::debug!("discarding stale completion for requeued range");
                        } else {
                            checkpoint.mark_completed(range, range_hash);
                            checkpoint.save(&checkpoint_path)?;
                            completed_ranges.push((range, range_hash));
                        }
                    }
                    None => break,
                }
            }
            report = metrics_rx.recv() => {
                if let Some(r) = report {
                    estimator.record(r.bytes_this_interval);
                    if let Some(state) = active_workers.get_mut(&r.worker_id) {
                        state.last_active = Instant::now();
                    }
                }
            }
            done = done_rx.recv() => {
                match done {
                    Some((id, result)) => {
                        if let Some(state) = active_workers.remove(&id) {
                            completed_worker_handles.push(state.handle);
                        }
                        if let Err(ref e) = result {
                            tracing::warn!(worker_id = id, error = %e, "transfer failed, range requeued (check firewall allows data ports)");
                            queue.requeue(id);
                            if !queue.is_done() {
                                let cancel = CancellationToken::new();
                                let cancel_child = cancel.child_token();
                                let handle = spawn_worker(id, &queue, &file, &file_path_buf, &config, &progress_handle, &metrics_tx, &completed_tx, &done_tx, &enable_fec_auto, cancel_child);
                                active_workers.insert(id, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
                            }
                        }
                    }
                    None => break,
                }
            }
            _ = interval.tick() => {
                const INTERVAL_DURATION: Duration = Duration::from_secs(2);
                // Release: 15 intervals (30s) so WAN/slow links aren't falsely marked stalled. Debug: 30 for tests.
                let stall_threshold_intervals = if cfg!(debug_assertions) { 30 } else { 15 };
                for state in active_workers.values_mut() {
                    if state.last_active.elapsed() > INTERVAL_DURATION {
                        state.consecutive_zero_intervals = state.consecutive_zero_intervals.saturating_add(1);
                    } else {
                        state.consecutive_zero_intervals = 0;
                    }
                }
                let stalled: Vec<WorkerId> = active_workers
                    .iter()
                    .filter(|(_, s)| s.consecutive_zero_intervals >= stall_threshold_intervals)
                    .map(|(id, _)| *id)
                    .collect();
                #[cfg(feature = "fec")]
                if !stalled.is_empty() {
                    if let Some(ref auto) = enable_fec_auto {
                        if !auto.load(Ordering::Relaxed) {
                            auto.store(true, Ordering::Relaxed);
                            tracing::info!("loss-like stall detected, enabling FEC for remaining chunks");
                        }
                    }
                }
                for id in stalled {
                    if let Some(state) = active_workers.remove(&id) {
                        state.cancel.cancel();
                        state.handle.abort();
                    }
                    if let Some(range) = queue.requeue_returning_range(id) {
                        stale_completions.insert((id, range.start, range.end));
                    }
                    if !queue.is_done() {
                        let cancel = CancellationToken::new();
                        let cancel_child = cancel.child_token();
                        let handle = spawn_worker(id, &queue, &file, &file_path_buf, &config, &progress_handle, &metrics_tx, &completed_tx, &done_tx, &enable_fec_auto, cancel_child);
                        active_workers.insert(id, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
                    }
                    tracing::warn!(worker_id = id, "worker stalled, requeuing range");
                }

                #[cfg(feature = "fec")]
                if stalled.is_empty() && estimator.sudden_drop() {
                    if let Some(ref auto) = enable_fec_auto {
                        if !auto.load(Ordering::Relaxed) {
                            auto.store(true, Ordering::Relaxed);
                            tracing::info!("throughput sudden drop, enabling FEC");
                        }
                    }
                }

                if let Some((t, mbps_at_scale)) = last_scale_up {
                    if t.elapsed() > Duration::from_millis((rtt_ms * 4).max(4000)) {
                        if estimator.estimate_mbps() < mbps_at_scale * 1.05 {
                            scale_up_disabled = true;
                        }
                        last_scale_up = None;
                    }
                }

                let can_scale = !scale_up_disabled
                    && active_workers.len() < effective_max_streams
                    && next_worker_id < effective_max_streams
                    && !queue.is_done();
                let should_scale = estimator.is_underperforming() || estimator.peak_mbps == 0.0;
                let cooldown_ok = last_change.elapsed() > Duration::from_millis(rtt_ms * 2);
                if can_scale && should_scale && cooldown_ok {
                    let id_new = next_worker_id;
                    next_worker_id += 1;
                    let cancel = CancellationToken::new();
                    let cancel_child = cancel.child_token();
                    let handle = spawn_worker(id_new, &queue, &file, &file_path_buf, &config, &progress_handle, &metrics_tx, &completed_tx, &done_tx, &enable_fec_auto, cancel_child);
                    active_workers.insert(id_new, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
                    last_change = Instant::now();
                    last_scale_up = Some((Instant::now(), estimator.estimate_mbps()));
                }

                if queue.is_done() && active_workers.is_empty() {
                    break;
                }
            }
        }
    }

    drop(completed_tx);
    drop(done_tx);
    drop(metrics_tx);
    let worker_ids: Vec<WorkerId> = active_workers.keys().cloned().collect();
    for id in worker_ids {
        if let Some(state) = active_workers.remove(&id) {
            completed_worker_handles.push(state.handle);
        }
    }
    for h in completed_worker_handles {
        let _ = h.await;
    }
    while completed_rx.recv().await.is_some() {}
    while done_rx.recv().await.is_some() {}

    let total_sent = progress_handle.transferred.load(Ordering::Relaxed);

    let file_hash = match resume_hash {
        Some(h) => h,
        None => {
            let path = file_path.to_path_buf();
            tokio::task::spawn_blocking(move || hash_file(&path, file_size))
                .await
                .map_err(|e| TransferError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))))?
                ?
        }
    };
    // Framed hash message: 0x03 (type) + 32 bytes (BLAKE3). See docs/INTEGRITY_DESIGN.md.
    writer.write_all(&[msg::HASH]).await.map_err(|e| {
        TransferError::NetworkError(format!("Failed to send hash message type: {}", e))
    })?;
    writer
        .write_all(&file_hash)
        .await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send file hash: {}", e)))?;

    // Server replies 0x04 (hash OK) or 0x05 (hash MISMATCH). On 0x05 keep checkpoint.
    let mut response_buf = [0u8; 1];
    reader.read_exact(&mut response_buf).await.map_err(|e| {
        TransferError::NetworkError(format!("Failed to receive hash response: {}", e))
    })?;

    match response_buf[0] {
        msg::HASH_OK => {}
        msg::HASH_MISMATCH => {
            return Err(TransferError::IntegrityCheckFailed(
                "Server reported BLAKE3 mismatch; checkpoint kept for retry".to_string(),
            ));
        }
        _ => {
            return Err(TransferError::ProtocolError(format!(
                "Invalid hash response from server: 0x{:02x}",
                response_buf[0]
            )));
        }
    }

    // Delete checkpoint only on hash OK
    delete_checkpoint(file_path)?;

    // Wait for logger to finish
    let _ = logger_handle.await;
    
    progress.finish();
    
    let duration = progress.start_time.elapsed();
    let throughput_mbps = if duration.as_secs_f64() > 0.0 {
        (total_sent as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
    } else {
        0.0
    };

    tracing::debug!(
        file = %filename,
        bytes = total_sent,
        duration_secs = duration.as_secs_f64(),
        throughput_mbps = throughput_mbps,
        "Transfer completed"
    );

    Ok(total_sent)
}

/// Probe bandwidth for a transport: connect, handshake, RTT, bandwidth probe; return bandwidth bps or None.
/// Used for transport auto-selection (pick TCP vs QUIC by measured bandwidth).
pub async fn probe_bandwidth_for_transport(
    transport: &dyn Transport,
    addr: SocketAddr,
    config: &TransferConfig,
) -> Option<u64> {
    let (meta, _opener) = transport.connect_for_transfer(addr, 1, 1).await.ok()?;
    let mut reader = meta.reader;
    let mut writer = meta.writer;

    let client_caps = crate::base::capabilities_from_config(config);
    writer.write_all(&client_caps.to_bytes()).await.ok()?;
    let mut cap_buf = [0u8; crate::base::CAPABILITIES_WIRE_LEN];
    reader.read_exact(&mut cap_buf).await.ok()?;
    let negotiated = crate::base::Capabilities::from_bytes(&cap_buf)?;

    if (negotiated.flags & crate::base::cap_flags::RTT_PROBE) != 0 {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let mut ping = [0u8; crate::base::PING_PONG_WIRE_LEN];
        ping[0] = crate::base::msg::PING;
        ping[1..9].copy_from_slice(&ts.to_le_bytes());
        writer.write_all(&ping).await.ok()?;
        let mut pong_buf = [0u8; crate::base::PING_PONG_WIRE_LEN];
        reader.read_exact(&mut pong_buf).await.ok()?;
        if pong_buf[0] != crate::base::msg::PONG {
            return None;
        }
    }

    if (negotiated.flags & crate::base::cap_flags::BANDWIDTH_PROBE) != 0 {
        run_bandwidth_probe(&mut writer, &mut reader).await
    } else {
        None
    }
}

/// Send a file over a transport (TCP or QUIC). Uses same protocol as send_file_tcp.
/// If `stats_out` is `Some`, it is filled with transfer report data on success (for --stats / --json).
/// If `skip_unchanged` is true, send CHECK_HASH first and skip transfer when server already has this hash (--update).
/// When `file_reader` is `Some` (e.g. `FileReader::Uring` from inside `tokio_uring::start()`), it is used for reads.
pub async fn send_file_over_transport(
    transport: &dyn Transport,
    file_path: &std::path::Path,
    server_addr: SocketAddr,
    config: TransferConfig,
    stats_out: Option<&mut TransferReport>,
    skip_unchanged: bool,
    file_reader: Option<FileReader>,
) -> Result<u64, TransferError> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size = metadata.len();

    if file_size == 0 {
        return Err(TransferError::ProtocolError("Cannot transfer empty file".to_string()));
    }

    let filename = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    let mut config = config;
    if !is_file_compressible(file_path) {
        config.enable_compression = false;
    }

    tracing::debug!(
        file = %filename,
        size = file_size,
        streams = config.num_streams,
        "Starting transfer over transport"
    );

    let progress = TransferProgress::new(file_size, true);
    if let Some(ref pb) = progress.progress_bar {
        pb.set_message(format!("{}", filename));
    }
    let progress_handle = progress.handle();

    let progress_for_logger = progress.handle();
    let filename_for_logger = filename.clone();
    let total_bytes = file_size;
    let start_time = std::time::Instant::now();
    let logger_handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let transferred = progress_for_logger.transferred.load(std::sync::atomic::Ordering::Relaxed);
            if transferred >= total_bytes {
                break;
            }
            if transferred > 0 {
                let elapsed = start_time.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    let mbps = (transferred as f64 / (1024.0 * 1024.0)) / elapsed;
                    tracing::debug!(file = %filename_for_logger, throughput_mbps = mbps, transferred = transferred, total = total_bytes, "Transfer progress");
                }
            }
        }
    });

    // Hash the source file BEFORE opening any connections (unchanged by transfer).
    let file_hash = tokio::task::spawn_blocking({
        let path = file_path.to_path_buf();
        move || hash_file(&path, file_size)
    })
    .await
    .map_err(|e| TransferError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))))?
    ?;

    let (meta, opener) = transport
        .connect_for_transfer(server_addr, config.num_streams, config.max_streams)
        .await?;

    let mut reader = meta.reader;
    let mut writer = meta.writer;

    let client_caps = crate::base::capabilities_from_config(&config);
    writer.write_all(&client_caps.to_bytes()).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send capabilities: {}", e)))?;
    let mut cap_buf = [0u8; crate::base::CAPABILITIES_WIRE_LEN];
    reader.read_exact(&mut cap_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read negotiated capabilities: {}", e)))?;
    let negotiated = crate::base::Capabilities::from_bytes(&cap_buf)
        .ok_or_else(|| TransferError::ProtocolError("Invalid capabilities from server".to_string()))?;
    let config = crate::base::apply_capabilities_to_config(&config, negotiated);

    let rtt_ms = if (negotiated.flags & crate::base::cap_flags::RTT_PROBE) != 0 {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let mut ping = [0u8; crate::base::PING_PONG_WIRE_LEN];
        ping[0] = crate::base::msg::PING;
        ping[1..9].copy_from_slice(&ts.to_le_bytes());
        writer.write_all(&ping).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send PING: {}", e)))?;
        let mut pong_buf = [0u8; crate::base::PING_PONG_WIRE_LEN];
        reader.read_exact(&mut pong_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read PONG: {}", e)))?;
        if pong_buf[0] != crate::base::msg::PONG {
            return Err(TransferError::ProtocolError("Expected PONG after PING".to_string()));
        }
        let ts_back = u64::from_le_bytes(pong_buf[1..9].try_into().unwrap());
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let ms = (now_us.saturating_sub(ts_back)) / 1000;
        tracing::debug!(rtt_ms = ms, "RTT probe");
        ms
    } else {
        100u64
    };

    // Measurement-driven stream count: bandwidth probe (only when server supports it) then BDP-based optimal streams.
    let bandwidth_bps = if (negotiated.flags & crate::base::cap_flags::BANDWIDTH_PROBE) != 0 {
        run_bandwidth_probe(&mut writer, &mut reader).await
    } else {
        None
    };
    let effective_max_streams = if let Some(bw) = bandwidth_bps {
        let bdp_streams = optimal_streams_from_bdp(bw, rtt_ms, config.buffer_size, config.max_streams);
        let streams = bdp_streams.max(FALLBACK_MAX_STREAMS).min(config.max_streams);
        tracing::debug!(bandwidth_bps = bw, rtt_ms, bdp_streams = bdp_streams, effective_streams = streams, "probe BDP stream count");
        streams
    } else {
        config.max_streams.min(FALLBACK_MAX_STREAMS)
    };

    // --update: optional skip check (CHECK_HASH). If server has same hash, skip transfer.
    if skip_unchanged {
        let skip_start = std::time::Instant::now();
        let path_buf = file_path.to_path_buf();
        let file_hash = tokio::task::spawn_blocking(move || hash_file(&path_buf, file_size))
            .await
            .map_err(|e| TransferError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))??;
        writer.write_all(&[msg::CHECK_HASH]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send CHECK_HASH: {}", e)))?;
        writer.write_all(&(filename.len() as u64).to_le_bytes()).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send CHECK_HASH filename len: {}", e)))?;
        writer.write_all(filename.as_bytes()).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send CHECK_HASH filename: {}", e)))?;
        writer.write_all(&file_hash).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send CHECK_HASH hash: {}", e)))?;
        let mut reply = [0u8; 1];
        reader.read_exact(&mut reply).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH reply: {}", e)))?;
        if reply[0] == msg::HAVE_HASH {
            tracing::debug!(file = %filename, "Server already has file (unchanged), skipping");
            eprintln!("Skipped (unchanged): {}", filename);
            let duration_ms = skip_start.elapsed().as_millis() as u64;
            if let Some(out) = stats_out {
                out.status = TransferStatus::Skipped { reason: "unchanged" };
                out.reason = Some("unchanged");
                out.bytes = 0;
                out.bytes_checked = file_size;
                out.duration_ms = duration_ms;
                out.transport = transport.name().to_string();
            }
            return Ok(0);
        }
        if reply[0] != msg::NEED_FILE {
            return Err(TransferError::ProtocolError(format!("Expected NEED_FILE or HAVE_HASH, got 0x{:02x}", reply[0])));
        }
    }

    // Step 4: Send metadata using effective_max_streams (same value server uses for listeners and ranges).
    // Wire order: filename_len (8 LE) | filename | file_size (8 LE) | max_streams (8 LE)
    let metadata_bytes = {
        let filename_bytes = filename.as_bytes();
        let mut m = Vec::new();
        m.extend_from_slice(&(filename_bytes.len() as u64).to_le_bytes());
        m.extend_from_slice(filename_bytes);
        m.extend_from_slice(&file_size.to_le_bytes());
        m.extend_from_slice(&(effective_max_streams as u64).to_le_bytes());
        m
    };
    writer.write_all(&metadata_bytes).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send metadata: {}", e)))?;

    let mut ack_buf = [0u8; 1];
    reader.read_exact(&mut ack_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to receive initial ack: {}", e)))?;

    if ack_buf[0] != msg::READY {
        return Err(TransferError::ProtocolError("Invalid initial ACK from server".to_string()));
    }

    let checkpoint_path = get_checkpoint_path(file_path);
    let mut checkpoint = if checkpoint_path.exists() {
        tracing::debug!("Found checkpoint file, attempting to resume transfer");
        match TransferCheckpoint::load(&checkpoint_path) {
            Ok(cp) if cp.file_size == file_size => cp,
            Ok(_) => {
                tracing::debug!("Checkpoint file size mismatch, starting fresh transfer");
                delete_checkpoint(file_path)?;
                TransferCheckpoint::new(file_size)
            }
            Err(e) => {
                tracing::debug!(error = %e, "Failed to load checkpoint, starting fresh transfer");
                delete_checkpoint(file_path)?;
                TransferCheckpoint::new(file_size)
            }
        }
    } else {
        TransferCheckpoint::new(file_size)
    };

    // Step 5: Build ranges using effective_max_streams (same value as step 4 metadata).
    let all_ranges = split_file_ranges(file_size, transfer_num_ranges(effective_max_streams));
    tracing::debug!(
        num_ranges = all_ranges.len(),
        max_streams = effective_max_streams,
        file_size,
        "sender split file into ranges"
    );
    let completed_ordered = checkpoint.completed_ranges_ordered(&all_ranges);

    if !completed_ordered.is_empty() {
        writer.write_all(&[msg::RANGE_HASHES]).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to send range hashes type: {}", e))
        })?;
        writer
            .write_all(&(completed_ordered.len() as u64).to_le_bytes())
            .await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send num_ranges: {}", e)))?;
        for (range, hash) in &completed_ordered {
            writer.write_all(&range.start.to_le_bytes()).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to send range start: {}", e))
            })?;
            writer.write_all(&range.end.to_le_bytes()).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to send range end: {}", e))
            })?;
            writer.write_all(hash).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to send range hash: {}", e))
            })?;
        }
        let mut resp = [0u8; 1];
        reader.read_exact(&mut resp).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to read range verify result type: {}", e))
        })?;
        if resp[0] != msg::RANGE_VERIFY_RESULT {
            return Err(TransferError::ProtocolError(format!(
                "Expected range verify result (0x07), got 0x{:02x}",
                resp[0]
            )));
        }
        let mut num_failed_buf = [0u8; 8];
        reader.read_exact(&mut num_failed_buf).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to read num_failed: {}", e))
        })?;
        let num_failed = u64::from_le_bytes(num_failed_buf) as usize;
        for _ in 0..num_failed {
            let mut start_buf = [0u8; 8];
            let mut end_buf = [0u8; 8];
            reader.read_exact(&mut start_buf).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to read failed range start: {}", e))
            })?;
            reader.read_exact(&mut end_buf).await.map_err(|e| {
                TransferError::NetworkError(format!("Failed to read failed range end: {}", e))
            })?;
            let start = u64::from_le_bytes(start_buf);
            let end = u64::from_le_bytes(end_buf);
            checkpoint.remove_completed(&FileRange { start, end });
        }
        checkpoint.save(&checkpoint_path)?;
    } else {
        writer.write_all(&[msg::NO_RANGE_VERIFY]).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to send no range verify: {}", e))
        })?;
    }

    let missing_ranges = checkpoint.get_missing_ranges(&all_ranges);
    let ranges_total = all_ranges.len();
    let ranges_resumed = completed_ordered.len();

    if missing_ranges.is_empty() {
        delete_checkpoint(file_path)?;
        return Ok(file_size);
    }

    let is_resume = checkpoint.completed_ranges.len() > 0;
    if is_resume {
        tracing::debug!(
            completed = checkpoint.completed_ranges.len(),
            remaining = missing_ranges.len(),
            "Resuming transfer"
        );
    }

    let file = match file_reader {
        Some(r) => r,
        None => FileReader::std(open_file_optimized(file_path, file_size)?),
    };

    // Unified queue + coordinator (same as send_file_tcp). Opener held here so connection stays alive until workers complete.
    let num_pending = missing_ranges.len();
    let queue = Arc::new(RangeQueue::new(missing_ranges));
    let (completed_tx, mut completed_rx) = mpsc::channel::<(WorkerId, FileRange, [u8; BLAKE3_LEN])>(64);
    let (done_tx, mut done_rx) = mpsc::channel::<(WorkerId, Result<(), TransferError>)>(32);
    let (metrics_tx, mut metrics_rx) = mpsc::channel::<StreamReport>(256);
    let file_path_buf = file_path.to_path_buf();
    let num_workers_initial = config.num_streams.min(num_pending).min(effective_max_streams).max(1);
    let mut next_worker_id = num_workers_initial;
    let mut active_workers: std::collections::HashMap<WorkerId, WorkerState> = std::collections::HashMap::new();
    let mut completed_worker_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut completed_ranges: Vec<(FileRange, [u8; BLAKE3_LEN])> = Vec::new();
    let mut stale_completions: HashSet<(WorkerId, u64, u64)> = HashSet::new();
    let mut streams_peak = num_workers_initial;
    let mut streams_stalled: usize = 0;
    let mut estimator = BandwidthEstimator::new(10);
    let mut last_change = Instant::now();
    let mut last_scale_up: Option<(Instant, f64)> = None;
    let mut scale_up_disabled = false;
    #[cfg(feature = "fec")]
    let enable_fec_auto: Option<Arc<AtomicBool>> = if config.fec_negotiated {
        Some(Arc::new(AtomicBool::new(false)))
    } else {
        None
    };
    #[cfg(not(feature = "fec"))]
    let enable_fec_auto: Option<Arc<AtomicBool>> = None;
    let transport_label = match transport.name() {
        "quic" => "QUIC",
        _ => "TCP",
    };

    let spawn_transport_worker = |id: WorkerId,
                                 opener: &Arc<dyn StreamOpener>,
                                 queue: &Arc<RangeQueue>,
                                 file: &FileReader,
                                 file_path_buf: &PathBuf,
                                 config: &TransferConfig,
                                 progress_handle: &ProgressHandle,
                                 metrics_tx: &mpsc::Sender<StreamReport>,
                                 completed_tx: &mpsc::Sender<(WorkerId, FileRange, [u8; BLAKE3_LEN])>,
                                 done_tx: &mpsc::Sender<(WorkerId, Result<(), TransferError>)>,
                                 enable_fec_auto: &Option<Arc<AtomicBool>>,
                                 cancel_child: CancellationToken|
     -> tokio::task::JoinHandle<()> {
        let opener = Arc::clone(opener);
        let queue = Arc::clone(queue);
        let file = file.clone();
        let file_path = file_path_buf.clone();
        let config = config.clone();
        let progress = progress_handle.clone();
        let metrics_tx = metrics_tx.clone();
        let completed_tx = completed_tx.clone();
        let done_tx = done_tx.clone();
        let enable_fec_auto = enable_fec_auto.clone();
        tokio::spawn(async move {
            transport_stream_worker(id, opener, queue, file, file_path, config, Some(progress), metrics_tx, completed_tx, done_tx, enable_fec_auto, cancel_child).await
        })
    };

    for id in 0..num_workers_initial {
        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let handle = spawn_transport_worker(
            id,
            &opener,
            &queue,
            &file,
            &file_path_buf,
            &config,
            &progress_handle,
            &metrics_tx,
            &completed_tx,
            &done_tx,
            &enable_fec_auto,
            cancel_child,
        );
        active_workers.insert(id, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
    }
    let initial_msg = match bandwidth_bps {
        Some(bw) => format!(
            "{}  {}->{}  ({}ms RTT, {:.1} MB/s probe)",
            transport_label,
            num_workers_initial,
            streams_peak,
            rtt_ms,
            bw as f64 / 1_000_000.0
        ),
        None => format!(
            "{}  {}->{}  ({}ms RTT)",
            transport_label,
            num_workers_initial,
            streams_peak,
            rtt_ms
        ),
    };
    progress_handle.set_message(initial_msg);

    let mut interval = interval(Duration::from_secs(2));
    loop {
        tokio::select! {
            completed = completed_rx.recv() => {
                match completed {
                    Some((id, range, range_hash)) => {
                        let key = (id, range.start, range.end);
                        if stale_completions.remove(&key) {
                            tracing::debug!("discarding stale completion for requeued range");
                        } else {
                            checkpoint.mark_completed(range, range_hash);
                            checkpoint.save(&checkpoint_path)?;
                            completed_ranges.push((range, range_hash));
                        }
                    }
                    None => break,
                }
            }
            report = metrics_rx.recv() => {
                if let Some(r) = report {
                    estimator.record(r.bytes_this_interval);
                    if let Some(state) = active_workers.get_mut(&r.worker_id) {
                        state.last_active = Instant::now();
                    }
                }
            }
            done = done_rx.recv() => {
                match done {
                    Some((id, result)) => {
                        if let Some(state) = active_workers.remove(&id) {
                            completed_worker_handles.push(state.handle);
                        }
                        if let Err(ref e) = result {
                            tracing::warn!(worker_id = id, error = %e, "transfer failed, range requeued (check firewall allows data ports)");
                            queue.requeue(id);
                            if !queue.is_done() {
                                let cancel = CancellationToken::new();
                                let cancel_child = cancel.child_token();
                                let handle = spawn_transport_worker(id, &opener, &queue, &file, &file_path_buf, &config, &progress_handle, &metrics_tx, &completed_tx, &done_tx, &enable_fec_auto, cancel_child);
                                active_workers.insert(id, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
                            }
                        }
                    }
                    None => break,
                }
            }
            _ = interval.tick() => {
                const INTERVAL_DURATION: Duration = Duration::from_secs(2);
                // Release: 15 intervals (30s) so WAN/slow links aren't falsely marked stalled. Debug: 30 for tests.
                let stall_threshold_intervals = if cfg!(debug_assertions) { 30 } else { 15 };
                for state in active_workers.values_mut() {
                    if state.last_active.elapsed() > INTERVAL_DURATION {
                        state.consecutive_zero_intervals = state.consecutive_zero_intervals.saturating_add(1);
                    } else {
                        state.consecutive_zero_intervals = 0;
                    }
                }
                let stalled: Vec<WorkerId> = active_workers
                    .iter()
                    .filter(|(_, s)| s.consecutive_zero_intervals >= stall_threshold_intervals)
                    .map(|(id, _)| *id)
                    .collect();
                // FEC auto-trigger: stall/sudden_drop are loss-detection proxies; QUIC path_stats could be used when connection is available.
                #[cfg(feature = "fec")]
                if !stalled.is_empty() {
                    if let Some(ref auto) = enable_fec_auto {
                        if !auto.load(Ordering::Relaxed) {
                            auto.store(true, Ordering::Relaxed);
                            tracing::info!("loss-like stall detected, enabling FEC for remaining chunks");
                        }
                    }
                }
                for id in stalled {
                    streams_stalled += 1;
                    if let Some(state) = active_workers.remove(&id) {
                        state.cancel.cancel();
                        state.handle.abort();
                    }
                    if let Some(range) = queue.requeue_returning_range(id) {
                        stale_completions.insert((id, range.start, range.end));
                    }
                    if !queue.is_done() {
                        let cancel = CancellationToken::new();
                        let cancel_child = cancel.child_token();
                        let handle = spawn_transport_worker(id, &opener, &queue, &file, &file_path_buf, &config, &progress_handle, &metrics_tx, &completed_tx, &done_tx, &enable_fec_auto, cancel_child);
                        active_workers.insert(id, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
                    }
                    tracing::warn!(worker_id = id, "worker stalled, requeuing range");
                }

                #[cfg(feature = "fec")]
                if stalled.is_empty() && estimator.sudden_drop() {
                    if let Some(ref auto) = enable_fec_auto {
                        if !auto.load(Ordering::Relaxed) {
                            auto.store(true, Ordering::Relaxed);
                            tracing::info!("throughput sudden drop, enabling FEC");
                        }
                    }
                }

                if let Some((t, mbps_at_scale)) = last_scale_up {
                    if t.elapsed() > Duration::from_millis((rtt_ms * 4).max(4000)) {
                        if estimator.estimate_mbps() < mbps_at_scale * 1.05 {
                            scale_up_disabled = true;
                        }
                        last_scale_up = None;
                    }
                }

                let can_scale = !scale_up_disabled
                    && active_workers.len() < effective_max_streams
                    && next_worker_id < effective_max_streams
                    && !queue.is_done();
                let should_scale = estimator.is_underperforming() || estimator.peak_mbps == 0.0;
                let cooldown_ok = last_change.elapsed() > Duration::from_millis(rtt_ms * 2);
                if can_scale && should_scale && cooldown_ok {
                    let id_new = next_worker_id;
                    next_worker_id += 1;
                    let cancel = CancellationToken::new();
                    let cancel_child = cancel.child_token();
                    let handle = spawn_transport_worker(id_new, &opener, &queue, &file, &file_path_buf, &config, &progress_handle, &metrics_tx, &completed_tx, &done_tx, &enable_fec_auto, cancel_child);
                    active_workers.insert(id_new, WorkerState { last_active: Instant::now(), handle, cancel, consecutive_zero_intervals: 0 });
                    last_change = Instant::now();
                    last_scale_up = Some((Instant::now(), estimator.estimate_mbps()));
                }

                streams_peak = streams_peak.max(active_workers.len());
                let live_mbps = estimator.estimate_mbps();
                let msg = if live_mbps > 0.0 {
                    format!(
                        "{}  {}->{}  ({}ms RTT, {:.1} MB/s)",
                        transport_label,
                        num_workers_initial,
                        streams_peak,
                        rtt_ms,
                        live_mbps
                    )
                } else {
                    match bandwidth_bps {
                        Some(bw) => format!(
                            "{}  {}->{}  ({}ms RTT, {:.1} MB/s probe)",
                            transport_label,
                            num_workers_initial,
                            streams_peak,
                            rtt_ms,
                            bw as f64 / 1_000_000.0
                        ),
                        None => format!(
                            "{}  {}->{}  ({}ms RTT)",
                            transport_label,
                            num_workers_initial,
                            streams_peak,
                            rtt_ms
                        ),
                    }
                };
                progress_handle.set_message(msg);

                if queue.is_done() && active_workers.is_empty() {
                    break;
                }
            }
        }
    }

    drop(completed_tx);
    drop(done_tx);
    drop(metrics_tx);
    let worker_ids: Vec<WorkerId> = active_workers.keys().cloned().collect();
    for id in worker_ids {
        if let Some(state) = active_workers.remove(&id) {
            completed_worker_handles.push(state.handle);
        }
    }
    for h in completed_worker_handles {
        let _ = h.await;
    }
    while completed_rx.recv().await.is_some() {}
    while done_rx.recv().await.is_some() {}

    let total_sent = progress_handle.transferred.load(Ordering::Relaxed);

    // file_hash was computed before opening connections; use it for verification
    writer.write_all(&[msg::HASH]).await.map_err(|e| {
        TransferError::NetworkError(format!("Failed to send hash message type: {}", e))
    })?;
    writer
        .write_all(&file_hash)
        .await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send file hash: {}", e)))?;

    let mut response_buf = [0u8; 1];
    reader.read_exact(&mut response_buf).await.map_err(|e| {
        TransferError::NetworkError(format!("Failed to receive hash response: {}", e))
    })?;

    match response_buf[0] {
        msg::HASH_OK => {}
        msg::HASH_MISMATCH => {
            return Err(TransferError::IntegrityCheckFailed(
                "Server reported BLAKE3 mismatch; checkpoint kept for retry".to_string(),
            ));
        }
        _ => {
            return Err(TransferError::ProtocolError(format!(
                "Invalid hash response from server: 0x{:02x}",
                response_buf[0]
            )));
        }
    }

    delete_checkpoint(file_path)?;

    let _ = logger_handle.await;
    progress.finish();

    let duration = progress.start_time.elapsed();
    let duration_ms = duration.as_millis() as u64;
    let throughput_mbps = if duration.as_secs_f64() > 0.0 {
        (total_sent as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
    } else {
        0.0
    };

    if let Some(out) = stats_out {
        out.transport = transport.name().to_string();
        out.bytes = total_sent;
        out.duration_ms = duration_ms;
        out.throughput_mbps = throughput_mbps;
        out.streams_initial = num_workers_initial;
        out.streams_peak = streams_peak;
        out.streams_stalled = streams_stalled;
        out.rtt_ms = rtt_ms;
        out.ranges_total = ranges_total;
        out.ranges_resumed = ranges_resumed;
    }

    tracing::debug!(
        file = %filename,
        bytes = total_sent,
        duration_secs = duration.as_secs_f64(),
        throughput_mbps = throughput_mbps,
        "Transfer over transport completed"
    );

    Ok(total_sent)
}

/// Receive a file using parallel TCP connections.
///
/// **Integrity contract:** Pass `Some(hash)` when the sender sends the integrity hash
/// (current protocol: 0x03 + 32 bytes). The receiver must obtain the expected hash
/// (from the metadata exchange or out-of-band). Pass `None` only for **legacy senders**
/// that do not send a hash (pre-integrity protocol); in that case integrity is not
/// verified. Do not pass `None` when receiving from our own `send_file_tcp` — that
/// bypasses integrity. See docs/INTEGRITY_DESIGN.md §10.
pub async fn receive_file_tcp(
    output_path: &std::path::Path,
    base_port: u16,
    num_connections: usize,
    file_size: u64,
    config: TransferConfig,
    expected_hash: Option<[u8; BLAKE3_LEN]>,
) -> Result<u64, TransferError> {
    info!(
        file = %output_path.display(),
        size = file_size,
        connections = num_connections,
        "Starting TCP file reception"
    );

    // Create output file
    let file = std::fs::File::create(output_path)?;
    file.set_len(file_size)?;
    let file = Arc::new(file);

    // Split file into ranges (finer granularity for load balance)
    let ranges = split_file_ranges(file_size, transfer_num_ranges(num_connections));
    tracing::debug!(
        num_ranges = ranges.len(),
        num_connections,
        file_size,
        "receive_file_tcp split into ranges"
    );

    let (range_hash_tx, range_hash_rx) = if expected_hash.is_some() {
        let (tx, rx) = mpsc::channel::<(FileRange, [u8; BLAKE3_LEN])>(64);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let mut handles = Vec::new();
    for (thread_id, _range) in ranges.into_iter().enumerate() {
        let file = Arc::clone(&file);
        let config = config.clone();
        let listen_port = base_port + thread_id as u16;
        let range_hash_tx = range_hash_tx.clone();

        let handle = tokio::spawn(async move {
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
            configure_tcp_socket(
                &socket,
                config.socket_send_buffer_size,
                config.socket_recv_buffer_size,
            )?;

            let addr: SocketAddr = format!("0.0.0.0:{}", listen_port)
                .parse()
                .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;
            
            socket.bind(&addr.into())
                .map_err(|e| TransferError::NetworkError(format!("Failed to bind: {}", e)))?;

            socket.listen(1)
                .map_err(|e| TransferError::NetworkError(format!("Failed to listen: {}", e)))?;

            socket.set_nonblocking(true)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
            
            let std_listener = std::net::TcpListener::from(socket);
            let listener = TcpListener::from_std(std_listener)
                .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio listener: {}", e)))?;

            let (stream, _) = listener.accept().await
                .map_err(|e| TransferError::NetworkError(format!("Accept failed: {}", e)))?;

            receive_range_tcp(thread_id, file, stream, config, None, range_hash_tx).await
        });

        handles.push(handle);
    }

    let mut total_received = 0u64;
    for (thread_id, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(bytes)) => {
                total_received += bytes;
                info!(thread_id, bytes, "Connection completed");
            }
            Ok(Err(e)) => {
                drop(range_hash_tx);
                return Err(e);
            }
            Err(e) => {
                drop(range_hash_tx);
                return Err(TransferError::NetworkError(format!("Connection panicked: {:?}", e)));
            }
        }
    }

    if let (Some(expected), Some(mut rx)) = (expected_hash, range_hash_rx) {
        drop(range_hash_tx);
        while rx.recv().await.is_some() {}
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let f = std::fs::File::open(output_path)?;
            let fd = f.as_raw_fd();
            unsafe { libc::fsync(fd); }
        }
        #[cfg(not(unix))]
        {
            let f = std::fs::File::open(output_path)?;
            f.sync_all()?;
        }
        let actual_hash = hash_file(output_path, file_size)?;
        if actual_hash != expected {
            return Err(TransferError::IntegrityCheckFailed(
                "BLAKE3 mismatch on standalone receive".to_string(),
            ));
        }
    }

    info!(
        file = %output_path.display(),
        bytes = total_received,
        "TCP reception completed"
    );

    Ok(total_received)
}

