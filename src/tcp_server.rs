//! TCP server implementation for parallel file transfer.
//!
//! The server listens on multiple ports: port 8080 for metadata,
//! and ports 8081-808N for parallel data connections.

use crate::error::TransferError;
use crate::base::TransferConfig;
use crate::tcp_transfer::{configure_tcp_socket, receive_range_tcp};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::error;

/// TCP server that handles file transfers using parallel connections.
pub struct TcpServer {
    base_port: u16,
    num_connections: usize,
    output_dir: PathBuf,
    config: TransferConfig,
}

impl TcpServer {
    /// Create a new TCP server.
    pub fn new(
        base_port: u16,
        num_connections: usize,
        output_dir: PathBuf,
        config: TransferConfig,
    ) -> Self {
        Self {
            base_port,
            num_connections,
            output_dir,
            config,
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
                    let num_connections = self.num_connections;
                    let base_port = self.base_port;

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, addr, output_dir, num_connections, base_port, config).await {
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
        _num_connections: usize,
        base_port: u16,
        config: TransferConfig,
    ) -> Result<(), TransferError> {
        // Read metadata: filename_len (8) + filename + file_size (8) + num_streams (8)
        let mut filename_len_buf = [0u8; 8];
        stream.read_exact(&mut filename_len_buf).await
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

        // Minimal logging - just the filename
        eprintln!("Receiving: {}", filename);

        // Ensure output directory exists
        std::fs::create_dir_all(&output_dir)?;

        // Create data listeners BEFORE sending ACK so they're ready when client connects
        let output_path = output_dir.join(&filename);
        let file = std::fs::File::create(&output_path)?;
        file.set_len(file_size)?;
        let file = Arc::new(file);

        let ranges = crate::base::split_file_ranges(file_size, num_streams);
        let mut data_handles = Vec::new();

        for (thread_id, _range) in ranges.into_iter().enumerate() {
            let file = Arc::clone(&file);
            let config = config.clone();
            let data_port = base_port + 1 + thread_id as u16;

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

                receive_range_tcp(thread_id, file, data_stream, config, None).await
            });

            data_handles.push(handle);
        }

        // Now send initial ACK - data ports are ready
        stream.write_all(&[0x01]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send initial ack: {}", e)))?;

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
                    return Err(e);
                }
                Err(e) => {
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

        // Send final ACK after all transfers complete
        stream.write_all(&[0x02]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send final ack: {}", e)))?;

        // Minimal completion message
        eprintln!("Received: {} ({} bytes)", filename, total_received);

        Ok(())
    }
}

