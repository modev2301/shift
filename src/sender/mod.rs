//! Sender implementation for client-side file transfers.
//!
//! The sender is responsible for initiating transfers and sending file data
//! to a remote receiver. It uses QUIC for reliable, encrypted transport with
//! built-in congestion control.

mod thread;

use crate::base::{split_file_ranges, FileRange, TransferConfig, TransferStats};
use crate::error::TransferError;
use crate::quic::create_client_endpoint;
use quinn::Connection;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use tracing::{error, info};

pub use thread::SenderThread;

/// Sender for initiating and managing file transfers.
///
/// The sender coordinates multiple parallel streams to transfer files
/// efficiently. Each stream handles a portion of the file, allowing
/// maximum utilization of available bandwidth.
pub struct Sender {
    config: TransferConfig,
    stats: Arc<TransferStats>,
    runtime: Runtime,
}

impl Sender {
    /// Create a new sender with the given configuration.
    pub fn new(config: TransferConfig) -> Result<Self, TransferError> {
        let runtime = Runtime::new()
            .map_err(|e| TransferError::ProtocolError(format!("Failed to create runtime: {}", e)))?;

        Ok(Self {
            config,
            stats: Arc::new(TransferStats::new()),
            runtime,
        })
    }

    /// Transfer a single file to the remote receiver.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the file to transfer
    /// * `destination` - Destination address in format "host:port"
    ///
    /// # Returns
    ///
    /// Returns the number of bytes transferred on success.
    pub fn transfer_file(
        &self,
        file_path: &Path,
        destination: &str,
    ) -> Result<u64, TransferError> {
        self.runtime.block_on(self.transfer_file_async(file_path, destination))
    }

    async fn transfer_file_async(
        &self,
        file_path: &Path,
        destination: &str,
    ) -> Result<u64, TransferError> {
        let metadata = std::fs::metadata(file_path)?;
        let file_size = metadata.len();

        if file_size == 0 {
            return Err(TransferError::ProtocolError(
                "Cannot transfer empty file".to_string(),
            ));
        }

        let filename = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        info!(
            file = %filename,
            size = file_size,
            streams = self.config.num_streams,
            "Starting file transfer"
        );

        // Parse destination address
        let server_addr: SocketAddr = destination
            .parse()
            .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;

        // Create QUIC endpoint
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let endpoint = create_client_endpoint(bind_addr)?;

        // Connect to server
        let connection = endpoint
            .connect(server_addr, "localhost")
            .map_err(|e| TransferError::NetworkError(format!("Connection failed: {}", e)))?
            .await
            .map_err(|e| TransferError::NetworkError(format!("Connection error: {}", e)))?;

        info!(
            file = %filename,
            "Connected to receiver"
        );

        // Send metadata (filename and size) on stream 0
        let (mut send, mut recv) = connection.open_bi().await
            .map_err(|e| TransferError::NetworkError(format!("Failed to open stream: {}", e)))?;

        let mut metadata = Vec::new();
        metadata.extend_from_slice(&(filename.len() as u64).to_le_bytes());
        metadata.extend_from_slice(filename.as_bytes());
        metadata.extend_from_slice(&file_size.to_le_bytes());
        metadata.extend_from_slice(&(self.config.num_streams as u64).to_le_bytes());

        send.write_all(&metadata).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send metadata: {}", e)))?;
        send.finish()
            .map_err(|e| TransferError::NetworkError(format!("Failed to finish metadata stream: {:?}", e)))?;

        // Wait for receiver acknowledgment
        let mut ack_buf = [0u8; 1];
        recv.read_exact(&mut ack_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to receive ack: {}", e)))?;

        if ack_buf[0] != 0x01 {
            return Err(TransferError::ProtocolError("Receiver rejected transfer".to_string()));
        }

        // Split file into ranges and transfer in parallel
        let ranges = split_file_ranges(file_size, self.config.num_streams);
        let start_time = Instant::now();

        let file = Arc::new(std::fs::File::open(file_path)?);
        let mut handles = Vec::new();

        for (thread_id, range) in ranges.into_iter().enumerate() {
            let connection = connection.clone();
            let file = Arc::clone(&file);
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                SenderThread::transfer_range_async(thread_id, range, file, connection, config).await
            });

            handles.push(handle);
        }

        // Wait for all streams to complete
        for (thread_id, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(bytes)) => {
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

        connection.close(0u32.into(), b"transfer complete");

        let duration = start_time.elapsed();
        let bytes_per_sec = (file_size as f64 / duration.as_secs_f64()) / 1_000_000.0;

        info!(
            file = %filename,
            bytes = file_size,
            duration_secs = duration.as_secs_f64(),
            throughput_mbps = bytes_per_sec,
            "Transfer completed"
        );

        self.stats.record_bytes(file_size);
        self.stats.record_file();

        Ok(file_size)
    }

    /// Get transfer statistics.
    pub fn stats(&self) -> &TransferStats {
        &self.stats
    }
}
