//! TCP-based file transfer with parallel connections.
//!
//! This module implements high-performance file transfer using multiple
//! parallel TCP connections, optimized socket settings, and zero-copy
//! file I/O where possible.

use crate::base::msg;
use crate::error::TransferError;
use crate::base::{split_file_ranges, FileRange, TransferConfig};
use crate::compression::{compress, decompress, should_compress};
use crate::encryption::{Decryptor, Encryptor};
use crate::file_io::{open_file_optimized, read_at};
use crate::integrity::{hash_file, hash_file_range_path, BLAKE3_LEN};
use crate::progress::{ProgressHandle, TransferProgress};
use crate::resume::{delete_checkpoint, get_checkpoint_path, TransferCheckpoint};
use crate::utils::is_file_compressible;
use std::collections::BTreeMap;
use std::fs::File;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, info};

/// Runs in a dedicated task: receives (offset, plaintext) from range senders/receivers,
/// reassembles in file order, and hashes with BLAKE3. Used for both send (bytes sent)
/// and receive (bytes written). Channel must be bounded for backpressure.
/// BTreeMap worst case: one range (file_size/num_streams), e.g. 671 MB for 10 GB/16.
/// See docs/INTEGRITY_DESIGN.md.
pub(crate) async fn run_ordered_hasher(
    mut rx: mpsc::Receiver<(u64, Vec<u8>)>,
    file_size: u64,
) -> Result<[u8; BLAKE3_LEN], TransferError> {
    let mut buf: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut current_pos = 0u64;
    let mut hasher = blake3::Hasher::new();

    fn drain_contiguous(
        buf: &mut BTreeMap<u64, Vec<u8>>,
        hasher: &mut blake3::Hasher,
        current_pos: &mut u64,
    ) {
        while let Some((&pos, _)) = buf.first_key_value() {
            if pos != *current_pos {
                break;
            }
            let data = buf.remove(&pos).expect("just saw it");
            hasher.update(&data);
            *current_pos += data.len() as u64;
        }
    }

    while let Some((offset, data)) = rx.recv().await {
        buf.insert(offset, data);
        drain_contiguous(&mut buf, &mut hasher, &mut current_pos);
    }
    drain_contiguous(&mut buf, &mut hasher, &mut current_pos);

    if current_pos != file_size {
        return Err(TransferError::ProtocolError(format!(
            "Send hasher: expected {} bytes, got {} (gap or duplicate)",
            file_size, current_pos
        )));
    }
    if !buf.is_empty() {
        return Err(TransferError::ProtocolError(
            "Send hasher: leftover buffered chunks".to_string(),
        ));
    }

    let hash = hasher.finalize();
    Ok(*hash.as_bytes())
}

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
/// If `hasher_tx` is Some, the plaintext bytes actually sent (before compression/encryption)
/// are fed to the hasher so the final hash is of bytes-sent, not a pre-send read.
async fn transfer_range_tcp(
    thread_id: usize,
    range: FileRange,
    file: Arc<File>,
    mut stream: tokio::net::TcpStream,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
    hasher_tx: Option<mpsc::Sender<(u64, Vec<u8>)>>,
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

    // Read and send file data
    let mut offset = range.start;
    let buffer_size = config.buffer_size.min(16 * 1024 * 1024);
    let mut buffer = vec![0u8; buffer_size];
    let mut total_sent = 0u64;

    while offset < range.end {
        let remaining = (range.end - offset) as usize;
        let read_size = buffer.len().min(remaining);

        // Use optimized read_at function
        let bytes_read = read_at(&file, &mut buffer[..read_size], offset)?;

        if bytes_read == 0 {
            break;
        }

        // Feed plaintext to hasher: per-read chunk (same granularity as send), not per-range.
        if let Some(ref tx) = hasher_tx {
            let plaintext = buffer[..bytes_read].to_vec();
            let _ = tx.send((offset, plaintext)).await; // backpressure: bounded channel
        }

        let mut data_to_send = buffer[..bytes_read].to_vec();
        
        // Compress if enabled and data is compressible
        if config.enable_compression && should_compress(&data_to_send) {
            match compress(&data_to_send) {
                Ok(compressed) => {
                    data_to_send = compressed;
                }
                Err(_) => {
                    // Compression failed, continue with uncompressed
                }
            }
        }
        
        // Encrypt if enabled
        if let Some(ref mut enc) = encryptor {
            enc.encrypt_in_place(&mut data_to_send)?;
        }
        
        // Send packet: compression flag (1) + size (8) + data
        let mut packet = Vec::with_capacity(9 + data_to_send.len());
        let compression_flag = if config.enable_compression && data_to_send.len() < bytes_read {
            0x01
        } else {
            0x00
        };
        packet.push(compression_flag);
        packet.extend_from_slice(&(data_to_send.len() as u64).to_le_bytes());
        packet.extend_from_slice(&data_to_send);
        
        // Write all data - tokio's write_all already handles partial writes efficiently
        writer.write_all(&packet).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send data: {}", e)))?;

        offset += bytes_read as u64;
        total_sent += bytes_read as u64;
        
        // Update progress if provided
        if let Some(ref progress) = progress {
            progress.update(bytes_read as u64);
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

/// Receive a file range over a single TCP connection.
/// If `hasher_tx` is Some, plaintext bytes written are fed to the receive-side hasher (bytes actually written).
pub async fn receive_range_tcp(
    thread_id: usize,
    file: Arc<File>,
    mut stream: tokio::net::TcpStream,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
    hasher_tx: Option<mpsc::Sender<(u64, Vec<u8>)>>,
) -> Result<u64, TransferError> {
    let (mut reader, _writer) = stream.split();

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

    while offset < end_offset {
        let remaining = (end_offset - offset) as usize;
        let read_size = buffer.len().min(remaining);

        // Read packet: compression flag (1) + size (8) + data
        let mut flag_buf = [0u8; 1];
        reader.read_exact(&mut flag_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read compression flag: {}", e)))?;
        
        let mut size_buf = [0u8; 8];
        reader.read_exact(&mut size_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read data size: {}", e)))?;
        let data_size = u64::from_le_bytes(size_buf) as usize;
        
        if data_size == 0 {
            break;
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
        
        // Decompress if needed
        let bytes_read = if flag_buf[0] == 0x01 {
            decompress_buffer = decompress(&data_buf, read_size)?;
            decompress_buffer.len()
        } else {
            // Copy uncompressed data to buffer
            let copy_len = data_buf.len().min(buffer.len());
            buffer[..copy_len].copy_from_slice(&data_buf[..copy_len]);
            copy_len
        };
        
        if bytes_read == 0 {
            break;
        }
        
        // Determine which buffer to use for writing
        let data = if flag_buf[0] == 0x01 && !decompress_buffer.is_empty() {
            &decompress_buffer[..bytes_read]
        } else {
            &buffer[..bytes_read]
        };

        // Feed plaintext to receive-side hasher (same granularity as write). See docs/INTEGRITY_DESIGN.md.
        if let Some(ref tx) = hasher_tx {
            let _ = tx.send((offset, data.to_vec())).await;
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

/// Send a file using parallel TCP connections.
pub async fn send_file_tcp(
    file_path: &std::path::Path,
    server_addr: SocketAddr,
    config: TransferConfig,
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

    // Send metadata: filename_len (8) + filename + file_size (8) + num_streams (8)
    let metadata = {
        let filename_bytes = filename.as_bytes();
        let mut m = Vec::new();
        m.extend_from_slice(&(filename_bytes.len() as u64).to_le_bytes());
        m.extend_from_slice(filename_bytes);
        m.extend_from_slice(&file_size.to_le_bytes());
        m.extend_from_slice(&(config.num_streams as u64).to_le_bytes());
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

    let all_ranges = split_file_ranges(file_size, config.num_streams);

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

    // Open file with optimizations (O_DIRECT for large files on Linux)
    let file = Arc::new(open_file_optimized(file_path, file_size)?);

    // Full transfer: hash = bytes we actually send (hasher task). Resume: we only send
    // missing ranges so we can't get full-file hash from bytes-sent; use one read at start.
    let resume_hash = if is_resume {
        Some(hash_file(file_path, file_size)?)
    } else {
        None
    };
    // Bounded channel so fast senders can't flood the hasher (backpressure). Each chunk is
    // at most buffer_size; 64 slots cap memory. See docs/INTEGRITY_DESIGN.md.
    let (hasher_tx, hasher_handle) = if resume_hash.is_none() {
        let (tx, rx) = mpsc::channel(64);
        let handle = tokio::spawn(async move { run_ordered_hasher(rx, file_size).await });
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // Establish parallel TCP connections for data (ports base+1, base+2, ...)
    let mut handles = Vec::new();
    let base_port = server_addr.port();
    let server_ip = server_addr.ip();

    // Only transfer missing ranges
    for (idx, range) in missing_ranges.iter().enumerate() {
        let thread_id = idx;
        let range = *range; // Copy the range
        let file = Arc::clone(&file);
        let config = config.clone();
        let target_port = base_port + 1 + thread_id as u16;
        let progress_clone = progress_handle.clone();
        let range_hasher_tx = hasher_tx.clone();

        let handle = tokio::spawn(async move {
            // Create and configure socket using socket2
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
            configure_tcp_socket(
                &socket,
                config.socket_send_buffer_size,
                config.socket_recv_buffer_size,
            )?;

            // Connect to server
            let server_addr = SocketAddr::new(server_ip, target_port);
            socket.connect(&server_addr.into())
                .map_err(|e| TransferError::NetworkError(format!("Connection failed: {}", e)))?;

            // Convert to tokio TcpStream
            socket.set_nonblocking(true)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
            
            let std_stream = std::net::TcpStream::from(socket);
            let stream = TcpStream::from_std(std_stream)
                .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio stream: {}", e)))?;

            transfer_range_tcp(
                thread_id,
                range,
                file,
                stream,
                config,
                Some(progress_clone),
                range_hasher_tx,
            )
            .await
        });

        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut total_sent = 0u64;
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(bytes)) => {
                total_sent += bytes;
                if idx < missing_ranges.len() {
                    let range = missing_ranges[idx];
                    let range_hash = hash_file_range_path(file_path, range.start, range.end)?;
                    checkpoint.mark_completed(range, range_hash);
                    checkpoint.save(&checkpoint_path)?;
                }
                tracing::debug!(
                    thread_id = idx,
                    bytes,
                    "Connection completed"
                );
            }
            Ok(Err(e)) => {
                drop(hasher_tx); // close channel so hasher task exits instead of hanging
                let _ = checkpoint.save(&checkpoint_path);
                return Err(e);
            }
            Err(e) => {
                drop(hasher_tx);
                let _ = checkpoint.save(&checkpoint_path);
                return Err(TransferError::NetworkError(format!("Connection panicked: {:?}", e)));
            }
        }
    }

    // Hash: full transfer = bytes we just sent; resume = full file at start of this run
    let file_hash = match (resume_hash, hasher_handle) {
        (Some(h), _) => h,
        (None, Some(handle)) => {
            drop(hasher_tx);
            handle
                .await
                .map_err(|e| TransferError::NetworkError(format!("Hasher task panicked: {:?}", e)))?
                .map_err(|e| TransferError::ProtocolError(format!("Hasher: {}", e)))?
        }
        (None, None) => unreachable!("full transfer always spawns hasher"),
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

    drop(metadata_stream);

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

    // Split file into ranges
    let ranges = split_file_ranges(file_size, num_connections);

    // Optional receive-side hasher for integrity (symmetric with server path).
    let (hasher_tx, hasher_rx) = if expected_hash.is_some() {
        let (tx, rx) = mpsc::channel(64);
        (Some(tx), Some(tokio::spawn(async move { run_ordered_hasher(rx, file_size).await })))
    } else {
        (None, None)
    };

    let mut handles = Vec::new();
    for (thread_id, _range) in ranges.into_iter().enumerate() {
        let file = Arc::clone(&file);
        let config = config.clone();
        let listen_port = base_port + thread_id as u16;
        let range_hasher_tx = hasher_tx.clone();

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

            receive_range_tcp(thread_id, file, stream, config, None, range_hasher_tx).await
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
                drop(hasher_tx);
                return Err(e);
            }
            Err(e) => {
                drop(hasher_tx);
                return Err(TransferError::NetworkError(format!("Connection panicked: {:?}", e)));
            }
        }
    }

    if let (Some(expected), Some(hasher_handle)) = (expected_hash, hasher_rx) {
        drop(hasher_tx);
        let actual_hash = hasher_handle
            .await
            .map_err(|e| TransferError::NetworkError(format!("Receive hasher task panicked: {:?}", e)))?
            .map_err(|e| TransferError::ProtocolError(format!("Receive hasher: {}", e)))?;
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

