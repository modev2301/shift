//! Receiver implementation for server-side file transfers.
//!
//! The receiver accepts incoming QUIC connections and writes received
//! file data to disk. It manages multiple parallel streams to handle
//! concurrent transfers efficiently.

mod thread;

use crate::base::TransferConfig;
use crate::error::TransferError;
use crate::quic::create_server_endpoint;
use quinn::Connection;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::{error, info};

pub use thread::ReceiverThread;

/// Receiver for accepting and managing incoming file transfers.
///
/// The receiver listens for incoming QUIC connections and coordinates
/// multiple parallel streams to receive file data and write it to disk.
pub struct Receiver {
    config: TransferConfig,
    output_dir: PathBuf,
    runtime: Runtime,
}

impl Receiver {
    /// Create a new receiver with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `start_port` - Starting port number for listening
    /// * `num_streams` - Number of parallel streams to accept
    /// * `output_dir` - Directory to write received files to
    pub fn new(
        start_port: u16,
        num_streams: usize,
        output_dir: &Path,
    ) -> Result<Self, TransferError> {
        std::fs::create_dir_all(output_dir)?;

        let config = TransferConfig {
            start_port,
            num_streams,
            buffer_size: 8 * 1024 * 1024,
            enable_compression: false,
            timeout_seconds: 30,
        };

        let runtime = Runtime::new()
            .map_err(|e| TransferError::ProtocolError(format!("Failed to create runtime: {}", e)))?;

        Ok(Self {
            config,
            output_dir: output_dir.to_path_buf(),
            runtime,
        })
    }

    /// Run the receiver in a loop, accepting transfers indefinitely.
    ///
    /// This method blocks until the receiver is stopped or an error occurs.
    pub fn run_forever(&self) -> Result<(), TransferError> {
        info!(
            port = self.config.start_port,
            streams = self.config.num_streams,
            output_dir = %self.output_dir.display(),
            "Receiver started, waiting for transfers"
        );

        self.runtime.block_on(self.run_forever_async())
    }

    async fn run_forever_async(&self) -> Result<(), TransferError> {
        let bind_addr: SocketAddr = format!("0.0.0.0:{}", self.config.start_port)
            .parse()
            .map_err(|e| TransferError::NetworkError(format!("Invalid bind address: {}", e)))?;

        let endpoint = create_server_endpoint(bind_addr)?;

        info!(
            port = self.config.start_port,
            "Listening for connections"
        );

        loop {
            let connecting = endpoint.accept();
            let output_dir = self.output_dir.clone();
            let config = self.config.clone();
            
            let incoming = match connecting.await {
                Some(conn) => conn,
                None => {
                    // Endpoint closed
                    break;
                }
            };
            
            tokio::spawn(async move {
                let connection = match incoming.await {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Connection error: {:?}", e);
                        return;
                    }
                };
                
                if let Err(e) = Self::handle_connection(connection, output_dir, config).await {
                    error!(
                        error = %e,
                        "Connection handling failed"
                    );
                }
            });
        }

        Ok(())
    }

    async fn handle_connection(
        connection: Connection,
        output_dir: PathBuf,
        config: TransferConfig,
    ) -> Result<(), TransferError> {
        info!(
            remote = %connection.remote_address(),
            "New connection accepted"
        );

        // Receive metadata on the first stream
        let (mut send, mut recv) = connection.accept_bi().await
            .map_err(|e| TransferError::NetworkError(format!("Stream error: {}", e)))?;

        // Read metadata
        let mut filename_len_buf = [0u8; 8];
        recv.read_exact(&mut filename_len_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read filename length: {}", e)))?;
        let filename_len = u64::from_le_bytes(filename_len_buf) as usize;

        let mut filename_buf = vec![0u8; filename_len];
        recv.read_exact(&mut filename_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read filename: {}", e)))?;
        let filename = String::from_utf8(filename_buf)
            .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;

        let mut file_size_buf = [0u8; 8];
        recv.read_exact(&mut file_size_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read file size: {}", e)))?;
        let file_size = u64::from_le_bytes(file_size_buf);

        let mut num_streams_buf = [0u8; 8];
        recv.read_exact(&mut num_streams_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read num streams: {}", e)))?;
        let num_streams = u64::from_le_bytes(num_streams_buf) as usize;

        info!(
            file = %filename,
            size = file_size,
            streams = num_streams,
            "Receiving file"
        );

        // Send acknowledgment
        send.write_all(&[0x01]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send ack: {}", e)))?;
        send.finish()
            .map_err(|e| TransferError::NetworkError(format!("Failed to finish ack stream: {:?}", e)))?;

        // Create output file
        let output_path = output_dir.join(&filename);
        let file = std::fs::File::create(&output_path)?;
        file.set_len(file_size)?;
        let file = Arc::new(file);

        // Receive data from all streams
        let mut handles = Vec::new();

        for _ in 0..num_streams {
            let connection = connection.clone();
            let file = Arc::clone(&file);
            let config = config.clone();

            let handle = tokio::spawn(async move {
                let (_send, recv) = connection.accept_bi().await
                    .map_err(|e| TransferError::NetworkError(format!("Stream error: {:?}", e)))?;

                ReceiverThread::receive_range_async(recv, file, config).await
            });

            handles.push(handle);
        }

        // Wait for all streams to complete
        let mut total_received = 0u64;
        for (thread_id, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(bytes)) => {
                    total_received += bytes;
                    info!(
                        thread_id,
                        bytes,
                        "Stream completed"
                    );
                }
                Ok(Err(e)) => {
                    error!(
                        thread_id,
                        error = %e,
                        "Stream failed"
                    );
                    return Err(e);
                }
                Err(e) => {
                    error!(
                        thread_id,
                        "Stream panicked: {:?}", e
                    );
                    return Err(TransferError::NetworkError(format!("Stream panicked: {:?}", e)));
                }
            }
        }

        info!(
            file = %filename,
            bytes = total_received,
            "File transfer completed"
        );

        Ok(())
    }

    /// Accept a single transfer and return when complete.
    ///
    /// Returns the filename and number of bytes received.
    pub fn accept_transfer(&self) -> Result<(String, u64), TransferError> {
        // This would be implemented for single-transfer mode
        // For now, run_forever handles all transfers
        Err(TransferError::ProtocolError(
            "Use run_forever() for accepting transfers".to_string(),
        ))
    }
}
