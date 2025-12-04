//! TCP-based file transfer with parallel connections.
//!
//! This module implements high-performance file transfer using multiple
//! parallel TCP connections, optimized socket settings, and zero-copy
//! file I/O where possible.

use crate::error::TransferError;
use crate::base::{split_file_ranges, FileRange, TransferConfig};
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tracing::{debug, info};

/// Configure TCP socket for high throughput.
fn configure_tcp_socket(socket: &Socket) -> Result<(), TransferError> {
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
) -> Result<u64, TransferError> {
    debug!(
        thread_id,
        start = range.start,
        end = range.end,
        "Transferring file range over TCP"
    );

    let (_reader, mut writer) = stream.split();

    // Send range header: start (8 bytes) + end (8 bytes)
    let mut header = Vec::with_capacity(16);
    header.extend_from_slice(&range.start.to_le_bytes());
    header.extend_from_slice(&range.end.to_le_bytes());
    writer.write_all(&header).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to send range header: {}", e)))?;

    // Read and send file data
    let mut offset = range.start;
    let mut buffer = vec![0u8; config.buffer_size.min(8 * 1024 * 1024)];
    let mut total_sent = 0u64;

    while offset < range.end {
        let remaining = (range.end - offset) as usize;
        let read_size = buffer.len().min(remaining);

        // Use pread for thread-safe reads (Linux) or seek+read (other platforms)
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
            let mut file_mut = File::try_clone(&*file)?;
            use std::io::SeekFrom;
            file_mut.seek(SeekFrom::Start(offset))?;
            file_mut.read(&mut buffer[..read_size])? as i64
        };

        if bytes_read <= 0 {
            break;
        }

        let data = &buffer[..bytes_read as usize];
        writer.write_all(data).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send data: {}", e)))?;

        offset += bytes_read as u64;
        total_sent += bytes_read as u64;
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
async fn receive_range_tcp(
    thread_id: usize,
    file: Arc<File>,
    mut stream: tokio::net::TcpStream,
    config: TransferConfig,
) -> Result<u64, TransferError> {
    let (mut reader, _writer) = stream.split();

    // Read range header
    let mut start_buf = [0u8; 8];
    reader.read_exact(&mut start_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read start offset: {}", e)))?;
    let start_offset = u64::from_le_bytes(start_buf);

    let mut end_buf = [0u8; 8];
    reader.read_exact(&mut end_buf).await
        .map_err(|e| TransferError::NetworkError(format!("Failed to read end offset: {}", e)))?;
    let end_offset = u64::from_le_bytes(end_buf);

    debug!(
        thread_id,
        start = start_offset,
        end = end_offset,
        "Receiving file range over TCP"
    );

    // Receive and write data
    let mut offset = start_offset;
    let mut buffer = vec![0u8; config.buffer_size.min(8 * 1024 * 1024)];
    let mut total_received = 0u64;

    while offset < end_offset {
        let remaining = (end_offset - offset) as usize;
        let read_size = buffer.len().min(remaining);

        let bytes_read = reader.read(&mut buffer[..read_size]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read from stream: {}", e)))?;

        if bytes_read == 0 {
            break;
        }

        let data = &buffer[..bytes_read];

        // Use pwrite for thread-safe writes (Linux) or seek+write (other platforms)
        #[cfg(target_os = "linux")]
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

        #[cfg(not(target_os = "linux"))]
        {
            let mut file_mut = File::try_clone(&*file)?;
            use std::io::SeekFrom;
            file_mut.seek(SeekFrom::Start(offset))?;
            file_mut.write_all(data)?;
        }

        offset += bytes_read as u64;
        total_received += bytes_read as u64;
    }

    debug!(
        thread_id,
        bytes = total_received,
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

    // Split file into ranges
    let ranges = split_file_ranges(file_size, config.num_streams);
    let file = Arc::new(File::open(file_path)?);

    // Establish parallel TCP connections for data (ports base+1, base+2, ...)
    let mut handles = Vec::new();
    let base_port = server_addr.port();
    let server_ip = server_addr.ip();

    for (thread_id, range) in ranges.into_iter().enumerate() {
        let file = Arc::clone(&file);
        let config = config.clone();
        let target_port = base_port + 1 + thread_id as u16;

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

            transfer_range_tcp(thread_id, range, file, stream, config).await
        });

        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut total_sent = 0u64;
    for (thread_id, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(bytes)) => {
                total_sent += bytes;
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
        file = %filename,
        bytes = total_sent,
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

            receive_range_tcp(thread_id, file, stream, config).await
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

