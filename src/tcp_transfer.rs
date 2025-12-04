//! TCP-based file transfer with parallel connections.
//!
//! This module implements high-performance file transfer using multiple
//! parallel TCP connections, optimized socket settings, and zero-copy
//! file I/O where possible.

use crate::error::TransferError;
use crate::base::{split_file_ranges, FileRange, TransferConfig};
use crate::compression::{compress, decompress, should_compress};
use crate::encryption::{Decryptor, Encryptor};
use crate::progress::{ProgressHandle, TransferProgress};
use crate::resume::{delete_checkpoint, get_checkpoint_path, TransferCheckpoint};
use std::fs::File;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::time::interval;
use tracing::{debug, info};

/// Configure TCP socket for high throughput.
pub fn configure_tcp_socket(socket: &Socket) -> Result<(), TransferError> {
    // Set send buffer to 8MB
    socket.set_send_buffer_size(8 * 1024 * 1024)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
    
    // Set receive buffer to 8MB
    socket.set_recv_buffer_size(8 * 1024 * 1024)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_RCVBUF: {}", e)))?;
    
    // Enable TCP_NODELAY to disable Nagle's algorithm
    socket.set_nodelay(true)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e)))?;
    
    Ok(())
}

/// Transfer a file range over a single TCP connection.
async fn transfer_range_tcp(
    thread_id: usize,
    range: FileRange,
    file: Arc<File>,
    mut stream: tokio::net::TcpStream,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
) -> Result<u64, TransferError> {
    info!(
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
    let mut buffer = vec![0u8; config.buffer_size.min(8 * 1024 * 1024)];
    let mut total_sent = 0u64;

    while offset < range.end {
        let remaining = (range.end - offset) as usize;
        let read_size = buffer.len().min(remaining);

        // Use pread for thread-safe reads (Unix) or seek+read (other platforms)
        #[cfg(unix)]
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

        #[cfg(not(unix))]
        let bytes_read = {
            let mut file_mut = File::try_clone(&*file)?;
            use std::io::SeekFrom;
            file_mut.seek(SeekFrom::Start(offset))?;
            file_mut.read(&mut buffer[..read_size])? as i64
        };

        if bytes_read <= 0 {
            break;
        }

        let mut data_to_send = buffer[..bytes_read as usize].to_vec();
        
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
        let compression_flag = if config.enable_compression && data_to_send.len() < bytes_read as usize {
            0x01
        } else {
            0x00
        };
        packet.push(compression_flag);
        packet.extend_from_slice(&(data_to_send.len() as u64).to_le_bytes());
        packet.extend_from_slice(&data_to_send);
        
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
pub async fn receive_range_tcp(
    thread_id: usize,
    file: Arc<File>,
    mut stream: tokio::net::TcpStream,
    config: TransferConfig,
    progress: Option<ProgressHandle>,
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

    info!(
        thread_id,
        start = start_offset,
        end = end_offset,
        size = end_offset - start_offset,
        "Receiving file range over TCP"
    );

    // Receive and write data
    let mut offset = start_offset;
    let mut buffer = vec![0u8; config.buffer_size.min(8 * 1024 * 1024)];
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

        // Use pwrite for thread-safe writes (Unix) or seek+write (other platforms)
        #[cfg(unix)]
        {
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

    info!(
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

    info!(
        file = %filename,
        size = file_size,
        connections = config.num_streams,
        "Starting TCP file transfer"
    );

    // Create progress tracker
    let progress = TransferProgress::new(file_size, true);
    let progress_handle = progress.handle();
    
    // Spawn throughput logger
    let progress_for_logger = progress.handle();
    let filename_for_logger = filename.clone();
    let total_bytes = file_size;
    let start_time = std::time::Instant::now();
    let logger_handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1));
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
                    info!(file = %filename_for_logger, throughput_mbps = mbps, transferred = transferred, total = total_bytes, "Transfer progress");
                }
            }
        }
    });

    // Send metadata on base port
    let metadata_socket = Socket::new(Domain::IPV4, Type::STREAM, None)
        .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
    metadata_socket.set_send_buffer_size(8 * 1024 * 1024)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
    metadata_socket.set_nodelay(true)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e)))?;

    metadata_socket.connect(&server_addr.into())
        .map_err(|e| TransferError::NetworkError(format!("Connection failed: {}", e)))?;

    metadata_socket.set_nonblocking(true)
        .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
    
    let std_stream = std::net::TcpStream::from(metadata_socket);
    let mut metadata_stream = TcpStream::from_std(std_stream)
        .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio stream: {}", e)))?;

    // Send metadata: filename_len (8) + filename + file_size (8) + num_streams (8)
    let filename_bytes = filename.as_bytes();
    let mut metadata = Vec::new();
    metadata.extend_from_slice(&(filename_bytes.len() as u64).to_le_bytes());
    metadata.extend_from_slice(filename_bytes);
    metadata.extend_from_slice(&file_size.to_le_bytes());
    metadata.extend_from_slice(&(config.num_streams as u64).to_le_bytes());

    let (mut reader, mut writer) = metadata_stream.split();
    writer.write_all(&metadata).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send metadata: {}", e)))?;

    // Wait for ACK
    let mut ack_buf = [0u8; 1];
    reader.read_exact(&mut ack_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to receive ack: {}", e)))?;

    if ack_buf[0] != 0x01 {
        return Err(TransferError::ProtocolError("Invalid ACK from server".to_string()));
    }

    drop(metadata_stream);

    // Check for existing checkpoint (resume support)
    let checkpoint_path = get_checkpoint_path(file_path);
    let mut checkpoint = if checkpoint_path.exists() {
        info!("Found checkpoint file, attempting to resume transfer");
        match TransferCheckpoint::load(&checkpoint_path) {
            Ok(cp) if cp.file_size == file_size => cp,
            Ok(_) => {
                info!("Checkpoint file size mismatch, starting fresh transfer");
                delete_checkpoint(file_path)?;
                TransferCheckpoint::new(file_size)
            }
            Err(e) => {
                info!(error = %e, "Failed to load checkpoint, starting fresh transfer");
                delete_checkpoint(file_path)?;
                TransferCheckpoint::new(file_size)
            }
        }
    } else {
        TransferCheckpoint::new(file_size)
    };

    // Split file into ranges
    let all_ranges = split_file_ranges(file_size, config.num_streams);
    let missing_ranges = checkpoint.get_missing_ranges(&all_ranges);
    
    if missing_ranges.is_empty() {
        info!("All ranges already completed, transfer is complete");
        delete_checkpoint(file_path)?;
        return Ok(file_size);
    }

    info!(
        total_ranges = all_ranges.len(),
        completed_ranges = checkpoint.completed_ranges.len(),
        missing_ranges = missing_ranges.len(),
        "Resuming transfer"
    );

    let file = Arc::new(File::open(file_path)?);

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

        let handle = tokio::spawn(async move {
            // Create and configure socket using socket2
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
            configure_tcp_socket(&socket)?;

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

            transfer_range_tcp(thread_id, range, file, stream, config, Some(progress_clone)).await
        });

        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut total_sent = 0u64;
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(bytes)) => {
                total_sent += bytes;
                // Mark range as completed in checkpoint
                if idx < missing_ranges.len() {
                    checkpoint.mark_completed(missing_ranges[idx]);
                    checkpoint.save(&checkpoint_path)?;
                }
                info!(
                    thread_id = idx,
                    bytes,
                    "Connection completed"
                );
            }
            Ok(Err(e)) => {
                // Save checkpoint before returning error
                let _ = checkpoint.save(&checkpoint_path);
                return Err(e);
            }
            Err(e) => {
                // Save checkpoint before returning error
                let _ = checkpoint.save(&checkpoint_path);
                return Err(TransferError::NetworkError(format!("Connection panicked: {:?}", e)));
            }
        }
    }
    
    // Delete checkpoint on successful completion
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

    info!(
        file = %filename,
        bytes = total_sent,
        duration_secs = duration.as_secs_f64(),
        throughput_mbps = throughput_mbps,
        "TCP transfer completed"
    );

    Ok(total_sent)
}

/// Receive a file using parallel TCP connections.
pub async fn receive_file_tcp(
    output_path: &std::path::Path,
    base_port: u16,
    num_connections: usize,
    file_size: u64,
    config: TransferConfig,
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

    // Accept parallel TCP connections
    let mut handles = Vec::new();

    for (thread_id, _range) in ranges.into_iter().enumerate() {
        let file = Arc::clone(&file);
        let config = config.clone();
        let listen_port = base_port + thread_id as u16;

        let handle = tokio::spawn(async move {
            // Create and configure listening socket using socket2
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
            configure_tcp_socket(&socket)?;

            let addr: SocketAddr = format!("0.0.0.0:{}", listen_port)
                .parse()
                .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;
            
            socket.bind(&addr.into())
                .map_err(|e| TransferError::NetworkError(format!("Failed to bind: {}", e)))?;

            socket.listen(1)
                .map_err(|e| TransferError::NetworkError(format!("Failed to listen: {}", e)))?;

            // Convert to tokio TcpListener
            socket.set_nonblocking(true)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
            
            let std_listener = std::net::TcpListener::from(socket);
            let listener = TcpListener::from_std(std_listener)
                .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio listener: {}", e)))?;

            // Accept connection
            let (stream, _) = listener.accept().await
                .map_err(|e| TransferError::NetworkError(format!("Accept failed: {}", e)))?;

            receive_range_tcp(thread_id, file, stream, config, None).await
        });

        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut total_received = 0u64;
    for (thread_id, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(bytes)) => {
                total_received += bytes;
                info!(
                    thread_id,
                    bytes,
                    "Connection completed"
                );
            }
            Ok(Err(e)) => {
                return Err(e);
            }
            Err(e) => {
                return Err(TransferError::NetworkError(format!("Connection panicked: {:?}", e)));
            }
        }
    }

    info!(
        file = %output_path.display(),
        bytes = total_received,
        "TCP reception completed"
    );

    Ok(total_received)
}

