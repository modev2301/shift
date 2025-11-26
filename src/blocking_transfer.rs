//! Blocking thread-based transfer (WDT-style architecture)
//! 
//! This module implements a WDT-inspired transfer system using:
//! - OS threads with blocking I/O (no async overhead)
//! - Thread-per-port architecture (zero coordination)
//! - Pre-split file ranges (no round-robin distribution)
//! - Minimal protocol (varint encoding, no per-chunk metadata)

use crate::error::TransferError;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// File range assigned to a thread
#[derive(Debug, Clone, Copy)]
pub struct FileRange {
    pub start: u64,
    pub end: u64,
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

/// Decode varint
pub fn decode_varint<R: Read>(reader: &mut R) -> Result<u64, TransferError> {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        let b = byte[0];
        result |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(TransferError::ProtocolError("Varint too large".to_string()));
        }
    }
    Ok(result)
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

/// Handshake message (minimal protocol)
#[derive(Debug)]
struct Handshake {
    filename: String,
    file_size: u64,
}

impl Handshake {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // [filename_len: varint][filename: bytes][file_size: varint]
        encode_varint(self.filename.len() as u64, &mut buf);
        buf.extend_from_slice(self.filename.as_bytes());
        encode_varint(self.file_size, &mut buf);
        buf
    }
    
    fn decode<R: Read>(reader: &mut R) -> Result<Self, TransferError> {
        let filename_len = decode_varint(reader)? as usize;
        let mut filename_bytes = vec![0u8; filename_len];
        reader.read_exact(&mut filename_bytes)?;
        let filename = String::from_utf8(filename_bytes)
            .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;
        let file_size = decode_varint(reader)?;
        Ok(Handshake { filename, file_size })
    }
}

/// Sender thread: reads file region and sends over socket
pub fn sender_thread(
    file_path: &Path,
    range: FileRange,
    server_addr: &str,
    port: u16,
    buffer_size: usize,
    filename: String,
    file_size: u64,
) -> Result<u64, TransferError> {
    // Open file
    let mut file = File::open(file_path)?;
    
    // Connect to server
    let addr = format!("{}:{}", server_addr, port);
    let mut stream = TcpStream::connect(&addr)?;
    
    // Configure TCP for throughput
    stream.set_nodelay(true)?;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = stream.as_raw_fd();
        // Set TCP buffer sizes
        let buf_size: libc::c_int = 25 * 1024 * 1024; // 25MB
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    
    // Send handshake: [filename][file_size]
    let handshake = Handshake { filename, file_size };
    let handshake_bytes = handshake.encode();
    stream.write_all(&handshake_bytes)?;
    
    // Allocate buffer
    let mut buffer = vec![0u8; buffer_size];
    let mut offset = range.start;
    let mut total_sent = 0u64;
    
    // Send file range header: [start_offset: varint][end_offset: varint]
    let mut header = Vec::new();
    encode_varint(range.start, &mut header);
    encode_varint(range.end, &mut header);
    stream.write_all(&header)?;
    
    // Transfer loop: pread → write (blocking, no async)
    while offset < range.end {
        let remaining = (range.end - offset) as usize;
        let read_size = std::cmp::min(buffer_size, remaining);
        
        // Use pread for thread-safe reads at specific offset
        #[cfg(target_os = "linux")]
        use std::os::unix::io::AsRawFd;
        #[cfg(target_os = "linux")]
        let bytes_read = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buffer.as_mut_ptr() as *mut libc::c_void,
                read_size,
                offset as libc::off_t,
            )
        };
        #[cfg(not(target_os = "linux"))]
        let bytes_read = {
            // Fallback: seek + read (not thread-safe, but works on non-Linux)
            file.seek(std::io::SeekFrom::Start(offset))?;
            file.read(&mut buffer[..read_size])? as i64
        };
        
        if bytes_read < 0 {
            return Err(TransferError::Io(std::io::Error::last_os_error()));
        }
        
        if bytes_read == 0 {
            break;
        }
        
        // Write to socket (blocking)
        stream.write_all(&buffer[..bytes_read as usize])?;
        
        offset += bytes_read as u64;
        total_sent += bytes_read as u64;
    }
    
    // Flush and close
    stream.flush()?;
    
    Ok(total_sent)
}

/// Receiver thread: accepts connection and writes to file
pub fn receiver_thread(
    listener: TcpListener,
    output_dir: &Path,
    buffer_size: usize,
) -> Result<(String, u64), TransferError> {
    // Accept connection
    let (mut stream, addr) = listener.accept()?;
    tracing::info!("Accepted connection from {} on thread", addr);
    
    // Configure TCP
    stream.set_nodelay(true)?;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = stream.as_raw_fd();
        let buf_size: libc::c_int = 25 * 1024 * 1024; // 25MB
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    
    // Read handshake: [filename][file_size]
    let handshake = Handshake::decode(&mut stream)?;
    let output_path = output_dir.join(&handshake.filename);
    
    tracing::info!(
        "Receiving file: {} ({} bytes) -> {}",
        handshake.filename,
        handshake.file_size,
        output_path.display()
    );
    
    // Create output file
    let mut file = File::create(&output_path)?;
    file.set_len(handshake.file_size)?;
    #[cfg(target_os = "linux")]
    use std::os::unix::io::AsRawFd;
    #[cfg(target_os = "linux")]
    let file_fd = file.as_raw_fd();
    
    // Read range header
    let start_offset = decode_varint(&mut stream)?;
    let end_offset = decode_varint(&mut stream)?;
    
    tracing::debug!("Thread received range: {} - {}", start_offset, end_offset);
    
    // Allocate buffer
    let mut buffer = vec![0u8; buffer_size];
    let mut offset = start_offset;
    let mut total_received = 0u64;
    
    // Transfer loop: read → pwrite (blocking, no async)
    while offset < end_offset {
        let remaining = (end_offset - offset) as usize;
        let read_size = std::cmp::min(buffer_size, remaining);
        
        // Read from socket (blocking)
        let bytes_read = stream.read(&mut buffer[..read_size])?;
        
        if bytes_read == 0 {
            break; // EOF
        }
        
        // Use pwrite for thread-safe writes at specific offset
        #[cfg(target_os = "linux")]
        let bytes_written = unsafe {
            libc::pwrite(
                file_fd,
                buffer.as_ptr() as *const libc::c_void,
                bytes_read,
                offset as libc::off_t,
            )
        };
        #[cfg(not(target_os = "linux"))]
        let bytes_written = {
            // Fallback: seek + write (not thread-safe, but works on non-Linux)
            file.seek(std::io::SeekFrom::Start(offset))?;
            file.write(&buffer[..bytes_read])? as i64
        };
        
        if bytes_written < 0 {
            return Err(TransferError::Io(std::io::Error::last_os_error()));
        }
        
        if bytes_written as usize != bytes_read {
            return Err(TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("Partial write: {} != {}", bytes_written, bytes_read),
            )));
        }
        
        offset += bytes_written as u64;
        total_received += bytes_written as u64;
    }
    
    // Sync file
    file.sync_all()?;
    
    Ok((handshake.filename, total_received))
}

/// Blocking client transfer (WDT-style) - Main entry point
pub fn blocking_client_transfer(
    file_path: &Path,
    server_addr: &str,
    config: BlockingTransferConfig,
) -> Result<(), TransferError> {
    // Get file metadata
    let metadata = std::fs::metadata(file_path)?;
    let file_size = metadata.len();
    let filename = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();
    
    // Split file into ranges
    let ranges = split_file_ranges(file_size, config.num_threads);
    
    tracing::info!(
        "Starting blocking transfer: {} ({} bytes), {} threads",
        filename,
        file_size,
        config.num_threads
    );
    
    let start_time = Instant::now();
    let file_path = Arc::new(file_path.to_path_buf());
    let server_addr = Arc::new(server_addr.to_string());
    let filename = Arc::new(filename);
    
    // Spawn sender threads
    let mut handles = Vec::new();
    for (thread_id, range) in ranges.into_iter().enumerate() {
        let file_path = Arc::clone(&file_path);
        let server_addr = Arc::clone(&server_addr);
        let filename = Arc::clone(&filename);
        let port = config.base_port + thread_id as u16;
        
        let handle = thread::spawn(move || {
            sender_thread(
                &file_path,
                range,
                &server_addr,
                port,
                config.buffer_size,
                filename.to_string(),
                file_size,
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
    let throughput = (total_sent as f64 / duration.as_secs_f64()) / 1_000_000.0; // MB/s
    
    tracing::info!(
        "Transfer complete: {} bytes in {:.2}s ({:.2} MB/s)",
        total_sent,
        duration.as_secs_f64(),
        throughput
    );
    
    Ok(())
}

/// Blocking server transfer (WDT-style)
/// Accepts connections and writes to output directory
/// Returns when all threads complete (single file transfer)
pub fn blocking_server_transfer(
    output_dir: &Path,
    config: BlockingTransferConfig,
) -> Result<Vec<(String, u64)>, TransferError> {
    // Create listeners for each port
    let mut listeners = Vec::new();
    for i in 0..config.num_threads {
        let port = config.base_port + i as u16;
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr)?;
        listener.set_nonblocking(false)?; // Blocking mode
        listeners.push((port, listener));
        tracing::info!("Listening on port {} for blocking transfer", port);
    }
    
    // Accept one connection per port (each thread handles one file range)
    let mut handles = Vec::new();
    for (_port, listener) in listeners {
        let output_dir = output_dir.to_path_buf();
        let buffer_size = config.buffer_size;
        
        let handle = thread::spawn(move || {
            receiver_thread(listener, &output_dir, buffer_size)
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    let mut results = Vec::new();
    let mut total_received = 0u64;
    for (thread_id, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok((filename, bytes))) => {
                total_received += bytes;
                results.push((filename, bytes));
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
    
    tracing::info!("Server transfer complete: {} bytes received", total_received);
    Ok(results)
}

/// Blocking server loop - accepts multiple transfers
pub fn blocking_server_loop(
    output_dir: &Path,
    config: BlockingTransferConfig,
) -> Result<(), TransferError> {
    loop {
        tracing::info!("Waiting for new transfer...");
        match blocking_server_transfer(output_dir, config.clone()) {
            Ok(results) => {
                tracing::info!("Transfer completed: {:?}", results);
            }
            Err(e) => {
                tracing::error!("Transfer failed: {}", e);
                // Continue accepting new transfers
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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

