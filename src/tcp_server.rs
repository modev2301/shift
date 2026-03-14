//! TCP server implementation for parallel file transfer.
//!
//! The server listens on multiple ports: port 8080 for metadata,
//! and ports 8081-808N for parallel data connections.

use crate::base::cap_flags;
use crate::base::msg;
use crate::base::{apply_capabilities_to_config, capabilities_from_config, Capabilities, CAPABILITIES_WIRE_LEN};
use crate::error::TransferError;
use crate::base::TransferConfig;
use crate::integrity::{hash_file_range_path, BLAKE3_LEN};
use crate::tcp_transfer::{configure_tcp_socket, receive_range_tcp, run_ordered_hasher};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::error;

/// TCP server that handles file transfers using parallel connections.
pub struct TcpServer {
    base_port: u16,
    output_dir: PathBuf,
    config: TransferConfig,
    /// Path → BLAKE3 hash for completed receives (used by --update skip check).
    completed_hashes: Arc<Mutex<HashMap<PathBuf, [u8; BLAKE3_LEN]>>>,
}

impl TcpServer {
    /// Create a new TCP server. Listener count comes from negotiated max_streams (client sends it in metadata).
    pub fn new(base_port: u16, output_dir: PathBuf, config: TransferConfig) -> Self {
        Self {
            base_port,
            output_dir,
            config,
            completed_hashes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Run the server forever, accepting connections.
    pub async fn run_forever(&self) -> Result<(), TransferError> {
        // Create and configure metadata listener on base port
        let metadata_socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
        
        // Configure socket for high throughput
        if let Some(size) = self.config.socket_send_buffer_size {
            metadata_socket.set_send_buffer_size(size)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
        }
        if let Some(size) = self.config.socket_recv_buffer_size {
            metadata_socket.set_recv_buffer_size(size)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_RCVBUF: {}", e)))?;
        }
        metadata_socket.set_nodelay(true)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e)))?;

        let addr: SocketAddr = format!("0.0.0.0:{}", self.base_port)
            .parse()
            .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;
        
        metadata_socket.bind(&addr.into())
            .map_err(|e| TransferError::NetworkError(format!("Failed to bind: {}", e)))?;
        metadata_socket.listen(128)
            .map_err(|e| TransferError::NetworkError(format!("Failed to listen: {}", e)))?;

        metadata_socket.set_nonblocking(true)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
        
        let std_listener = std::net::TcpListener::from(metadata_socket);
        let listener = TcpListener::from_std(std_listener)
            .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio listener: {}", e)))?;

        eprintln!("Shift server listening on port {}", self.base_port);
        eprintln!("Output directory: {}", self.output_dir.display());

        // Accept metadata connections
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::debug!(remote = %addr, "New metadata connection accepted");
                    let output_dir = self.output_dir.clone();
                    let config = self.config.clone();
                    let base_port = self.base_port;

                    let completed_hashes = self.completed_hashes.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, addr, output_dir, base_port, config, completed_hashes).await {
                            error!(error = %e, "Connection handling failed");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }

    async fn handle_connection(
        mut stream: tokio::net::TcpStream,
        _addr: SocketAddr,
        output_dir: PathBuf,
        base_port: u16,
        config: TransferConfig,
        completed_hashes: Arc<Mutex<HashMap<PathBuf, [u8; BLAKE3_LEN]>>>,
    ) -> Result<(), TransferError> {
        // Capability handshake: read client caps, negotiate, send negotiated, apply to config.
        let mut cap_buf = [0u8; CAPABILITIES_WIRE_LEN];
        stream.read_exact(&mut cap_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read client capabilities: {}", e)))?;
        let client_caps = Capabilities::from_bytes(&cap_buf)
            .ok_or_else(|| TransferError::ProtocolError("Invalid client capabilities".to_string()))?;
        let server_caps = capabilities_from_config(&config);
        let negotiated = Capabilities::negotiate(client_caps, server_caps);
        stream.write_all(&negotiated.to_bytes()).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send negotiated capabilities: {}", e)))?;
        let config = apply_capabilities_to_config(&config, negotiated);

        // After handshake: read 1 byte. If PING (RTT only) echo PONG and read 1 byte again. Then either CHECK_HASH (0x0A) or metadata (filename_len 8 + filename + file_size 8 + max_streams 8).
        let mut first_byte = [0u8; 1];
        if (negotiated.flags & cap_flags::RTT_PROBE) != 0 {
            stream.read_exact(&mut first_byte).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read after handshake: {}", e)))?;
            if first_byte[0] == msg::PING {
                let mut ts_buf = [0u8; 8];
                stream.read_exact(&mut ts_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read PING timestamp: {}", e)))?;
                let mut pong = [0u8; crate::base::PING_PONG_WIRE_LEN];
                pong[0] = msg::PONG;
                pong[1..9].copy_from_slice(&ts_buf);
                stream.write_all(&pong).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send Pong: {}", e)))?;
                stream.read_exact(&mut first_byte).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read after PONG: {}", e)))?;
            }
        } else {
            stream.read_exact(&mut first_byte).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read filename length: {}", e)))?;
        }

        let (filename, file_size, num_streams) = if first_byte[0] == msg::CHECK_HASH {
            // --update skip check: read filename_len (8) + filename + hash (32)
            let mut len_buf = [0u8; 8];
            stream.read_exact(&mut len_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH filename len: {}", e)))?;
            let name_len = u64::from_le_bytes(len_buf) as usize;
            let mut name_buf = vec![0u8; name_len];
            stream.read_exact(&mut name_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH filename: {}", e)))?;
            let check_filename = String::from_utf8(name_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid CHECK_HASH filename: {}", e)))?;
            let mut hash_buf = [0u8; BLAKE3_LEN];
            stream.read_exact(&mut hash_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH hash: {}", e)))?;
            let path = output_dir.join(&check_filename);
            let have = completed_hashes.lock().unwrap().get(&path).map(|h| h == &hash_buf).unwrap_or(false);
            if have {
                stream.write_all(&[msg::HAVE_HASH]).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send HAVE_HASH: {}", e)))?;
                eprintln!("Skipped (unchanged): {}", check_filename);
                return Ok(());
            }
            stream.write_all(&[msg::NEED_FILE]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send NEED_FILE: {}", e)))?;
            // Read full metadata: filename_len (8) + filename + file_size (8) + max_streams (8)
            stream.read_exact(&mut len_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read metadata filename len: {}", e)))?;
            let name_len = u64::from_le_bytes(len_buf) as usize;
            let mut name_buf = vec![0u8; name_len];
            stream.read_exact(&mut name_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read metadata filename: {}", e)))?;
            let filename = String::from_utf8(name_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;
            let mut file_size_buf = [0u8; 8];
            stream.read_exact(&mut file_size_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read file size: {}", e)))?;
            let file_size = u64::from_le_bytes(file_size_buf);
            let mut num_streams_buf = [0u8; 8];
            stream.read_exact(&mut num_streams_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num streams: {}", e)))?;
            let num_streams = u64::from_le_bytes(num_streams_buf) as usize;
            (filename, file_size, num_streams)
        } else {
            let mut filename_len_buf = [0u8; 8];
            filename_len_buf[0] = first_byte[0];
            stream.read_exact(&mut filename_len_buf[1..8]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read filename length: {}", e)))?;
            let filename_len = u64::from_le_bytes(filename_len_buf) as usize;
            let mut filename_buf = vec![0u8; filename_len];
            stream.read_exact(&mut filename_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read filename: {}", e)))?;
            let filename = String::from_utf8(filename_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;
            let mut file_size_buf = [0u8; 8];
            stream.read_exact(&mut file_size_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read file size: {}", e)))?;
            let file_size = u64::from_le_bytes(file_size_buf);
            let mut num_streams_buf = [0u8; 8];
            stream.read_exact(&mut num_streams_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num streams: {}", e)))?;
            let num_streams = u64::from_le_bytes(num_streams_buf) as usize;
            (filename, file_size, num_streams)
        };

        // Minimal logging - just the filename
        eprintln!("Receiving: {}", filename);

        // Ensure output directory exists
        std::fs::create_dir_all(&output_dir)?;

        // Create data listeners BEFORE sending ACK so they're ready when client connects
        let output_path = output_dir.join(&filename);
        let file = std::fs::File::create(&output_path)?;
        file.set_len(file_size)?;
        let file = Arc::new(file);

        let num_ranges = crate::base::transfer_num_ranges(num_streams);
        let ranges = crate::base::split_file_ranges(file_size, num_ranges);
        let mut data_handles = Vec::new();

        // Receive-side hasher: hash bytes as written (symmetric with sender). Bounded channel 64.
        let (hasher_tx, hasher_rx) = mpsc::channel(64);
        let hasher_handle = tokio::spawn(async move { run_ordered_hasher(hasher_rx, file_size).await });

        for (thread_id, _range) in ranges.into_iter().enumerate() {
            let file = Arc::clone(&file);
            let config = config.clone();
            let data_port = base_port + 1 + thread_id as u16;
            let range_hasher_tx = hasher_tx.clone();

            let handle = tokio::spawn(async move {
                // Create and configure listening socket
                let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                    .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
                configure_tcp_socket(
                    &socket,
                    config.socket_send_buffer_size,
                    config.socket_recv_buffer_size,
                )?;

                let addr: SocketAddr = format!("0.0.0.0:{}", data_port)
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

                // Accept connection
                let (data_stream, _) = listener.accept().await
                    .map_err(|e| TransferError::NetworkError(format!("Accept failed: {}", e)))?;

                receive_range_tcp(thread_id, file, data_stream, config, None, Some(range_hasher_tx)).await
            });

            data_handles.push(handle);
        }

        stream.write_all(&[msg::READY]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send initial ack: {}", e)))?;

        // Optional per-range verification (0x06) before data streams. Client sends 0x00 or 0x06.
        let mut range_verify_byte = [0u8; 1];
        stream.read_exact(&mut range_verify_byte).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read range verify byte: {}", e)))?;
        if range_verify_byte[0] == msg::RANGE_HASHES {
            let mut num_ranges_buf = [0u8; 8];
            stream.read_exact(&mut num_ranges_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num_ranges: {}", e)))?;
            let num_ranges = u64::from_le_bytes(num_ranges_buf) as usize;
            let mut failed: Vec<(u64, u64)> = Vec::new();
            for _ in 0..num_ranges {
                let mut start_buf = [0u8; 8];
                let mut end_buf = [0u8; 8];
                let mut expected_hash = [0u8; BLAKE3_LEN];
                stream.read_exact(&mut start_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range start: {}", e)))?;
                stream.read_exact(&mut end_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range end: {}", e)))?;
                stream.read_exact(&mut expected_hash).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range hash: {}", e)))?;
                let start = u64::from_le_bytes(start_buf);
                let end = u64::from_le_bytes(end_buf);
                match hash_file_range_path(&output_path, start, end) {
                    Ok(actual) if actual == expected_hash => {}
                    _ => failed.push((start, end)),
                }
            }
            stream.write_all(&[msg::RANGE_VERIFY_RESULT]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send range verify result: {}", e)))?;
            stream.write_all(&(failed.len() as u64).to_le_bytes()).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send num_failed: {}", e)))?;
            for (start, end) in &failed {
                stream.write_all(&start.to_le_bytes()).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send failed start: {}", e)))?;
                stream.write_all(&end.to_le_bytes()).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send failed end: {}", e)))?;
            }
        }
        // else 0x00: no range verify, proceed to data listeners

        // Wait for all data connections to complete
        let mut total_received = 0u64;
        for (thread_id, handle) in data_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(bytes)) => {
                    total_received += bytes;
                    tracing::debug!(
                        thread_id,
                        bytes,
                        "Data connection completed"
                    );
                }
                Ok(Err(e)) => {
                    drop(hasher_tx);
                    return Err(e);
                }
                Err(e) => {
                    drop(hasher_tx);
                    return Err(TransferError::NetworkError(format!("Data connection panicked: {:?}", e)));
                }
            }
        }

        // Sync file to ensure all data is written to disk
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            use libc;
            let fd = file.as_raw_fd();
            unsafe {
                libc::fsync(fd);
            }
        }

        #[cfg(not(unix))]
        {
            let file_mut = std::fs::File::try_clone(&*file)?;
            file_mut.sync_all()?;
        }

        // Hash comparison order (docs/INTEGRITY_DESIGN.md §9): all data streams are complete
        // and fsync done above. Only then read expected_hash from client, then finalize our hasher.
        let mut hash_type_buf = [0u8; 1];
        stream.read_exact(&mut hash_type_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read hash message type: {}", e)))?;
        if hash_type_buf[0] != msg::HASH {
            return Err(TransferError::ProtocolError(format!(
                "Expected hash message (0x03), got 0x{:02x}",
                hash_type_buf[0]
            )));
        }
        let mut expected_hash = [0u8; BLAKE3_LEN];
        stream.read_exact(&mut expected_hash).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read file hash: {}", e)))?;

        // Finalize receive-side hasher (channel close + drain BTreeMap) → actual_hash.
        drop(hasher_tx);
        let actual_hash = hasher_handle
            .await
            .map_err(|e| TransferError::NetworkError(format!("Receive hasher task panicked: {:?}", e)))?
            .map_err(|e| TransferError::ProtocolError(format!("Receive hasher: {}", e)))?;
        if actual_hash != expected_hash {
            // Tell client to keep checkpoint for retry (0x05), then return error.
            let _ = stream.write_all(&[msg::HASH_MISMATCH]).await;
            return Err(TransferError::IntegrityCheckFailed(format!(
                "BLAKE3 mismatch for {} (transfer may be corrupted)",
                filename
            )));
        }

        stream.write_all(&[msg::HASH_OK]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send hash OK: {}", e)))?;

        completed_hashes.lock().unwrap().insert(output_path.clone(), expected_hash);

        // Minimal completion message
        eprintln!("Received: {} ({} bytes)", filename, total_received);

        Ok(())
    }
}

