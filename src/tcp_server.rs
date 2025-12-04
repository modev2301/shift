//! TCP server implementation for parallel file transfer.
//!
//! The server listens on multiple ports: port 8080 for metadata,
//! and ports 8081-808N for parallel data connections.

use crate::error::TransferError;
use crate::base::TransferConfig;
use crate::tcp_transfer::receive_file_tcp;
use std::net::SocketAddr;
use std::path::PathBuf;
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

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
        info!(
            port = self.base_port,
            connections = self.num_connections,
            output_dir = %self.output_dir.display(),
            "TCP server started"
        );

        // Create and configure metadata listener on base port
        let metadata_socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
        
        // Configure socket for high throughput
        metadata_socket.set_send_buffer_size(8 * 1024 * 1024)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
        metadata_socket.set_recv_buffer_size(8 * 1024 * 1024)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_RCVBUF: {}", e)))?;
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

        info!("Listening for connections on port {}", self.base_port);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!(remote = %addr, "New connection accepted");
                    let output_dir = self.output_dir.clone();
                    let config = self.config.clone();
                    let num_connections = self.num_connections;

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, addr, output_dir, num_connections, config).await {
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

        info!(
            file = %filename,
            size = file_size,
            streams = num_streams,
            "Receiving file"
        );

        // Send ACK
        stream.write_all(&[0x01]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send ack: {}", e)))?;

        // Close metadata connection
        drop(stream);

        // Receive file data on parallel connections (ports base+1, base+2, ...)
        let output_path = output_dir.join(&filename);
        let base_port = config.start_port;
        
        receive_file_tcp(&output_path, base_port + 1, num_streams, file_size, config).await?;

        // Reconnect to send final ACK (or we could keep metadata connection open)
        // For now, we'll consider the transfer complete when all data connections finish
        info!(
            file = %filename,
            bytes = file_size,
            "File transfer completed"
        );

        Ok(())
    }
}

