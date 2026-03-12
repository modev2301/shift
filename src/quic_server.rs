//! QUIC server for file transfer. Same protocol as TCP server over one connection and multiple streams.

use crate::base::msg;
use crate::base::{
    apply_capabilities_to_config, capabilities_from_config, Capabilities, CAPABILITIES_WIRE_LEN,
};
use crate::error::TransferError;
use crate::integrity::{hash_file_range_path, BLAKE3_LEN};
use crate::quinn_transport::QuicTransport;
use crate::tcp_transfer::{receive_range_stream, run_ordered_hasher};
use crate::base::TransferConfig;
use crate::transport::Transport;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::error;

/// QUIC server: one UDP port, one connection per transfer with meta + N data streams.
pub struct QuicServer {
    port: u16,
    output_dir: PathBuf,
    config: TransferConfig,
}

impl QuicServer {
    pub fn new(port: u16, output_dir: PathBuf, config: TransferConfig) -> Self {
        Self {
            port,
            output_dir,
            config,
        }
    }

    pub async fn run_forever(&self) -> Result<(), TransferError> {
        let transport = QuicTransport::new();
        let addr: SocketAddr = ([0, 0, 0, 0], self.port).into();
        let listener = transport.listen(addr).await?;

        eprintln!("Shift QUIC server listening on UDP port {}", self.port);
        eprintln!("Output directory: {}", self.output_dir.display());

        loop {
            match listener.accept().await {
                Ok(conn) => {
                    let output_dir = self.output_dir.clone();
                    let config = self.config.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(conn, output_dir, config).await {
                            error!(error = %e, "QUIC connection handling failed");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "QUIC accept failed");
                }
            }
        }
    }

    async fn handle_connection(
        conn: Box<dyn crate::transport::Connection>,
        output_dir: PathBuf,
        config: TransferConfig,
    ) -> Result<(), TransferError> {
        let mut meta_stream = conn.accept_stream().await?;

        let mut cap_buf = [0u8; CAPABILITIES_WIRE_LEN];
        meta_stream.read_exact(&mut cap_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read client capabilities: {}", e)))?;
        let client_caps = Capabilities::from_bytes(&cap_buf)
            .ok_or_else(|| TransferError::ProtocolError("Invalid client capabilities".to_string()))?;
        let server_caps = capabilities_from_config(&config);
        let negotiated = Capabilities::negotiate(client_caps, server_caps);
        meta_stream.write_all(&negotiated.to_bytes()).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send negotiated capabilities: {}", e)))?;
        let config = apply_capabilities_to_config(&config, negotiated);

        let mut filename_len_buf = [0u8; 8];
        meta_stream.read_exact(&mut filename_len_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read filename length: {}", e)))?;
        let filename_len = u64::from_le_bytes(filename_len_buf) as usize;

        let mut filename_buf = vec![0u8; filename_len];
        meta_stream.read_exact(&mut filename_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read filename: {}", e)))?;
        let filename = String::from_utf8(filename_buf)
            .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;

        let mut file_size_buf = [0u8; 8];
        meta_stream.read_exact(&mut file_size_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read file size: {}", e)))?;
        let file_size = u64::from_le_bytes(file_size_buf);

        let mut num_streams_buf = [0u8; 8];
        meta_stream.read_exact(&mut num_streams_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read num streams: {}", e)))?;
        let num_streams = u64::from_le_bytes(num_streams_buf) as usize;

        eprintln!("Receiving: {}", filename);

        std::fs::create_dir_all(&output_dir)?;

        let output_path = output_dir.join(&filename);
        let file = std::fs::File::create(&output_path)?;
        file.set_len(file_size)?;
        let file = Arc::new(file);

        let ranges = crate::base::split_file_ranges(file_size, num_streams);
        let (hasher_tx, hasher_rx) = mpsc::channel(64);
        let hasher_handle = tokio::spawn(async move { run_ordered_hasher(hasher_rx, file_size).await });

        meta_stream.write_all(&[msg::READY]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send READY: {}", e)))?;

        let mut range_verify_byte = [0u8; 1];
        meta_stream.read_exact(&mut range_verify_byte).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read range verify byte: {}", e)))?;
        if range_verify_byte[0] == msg::RANGE_HASHES {
            let mut num_ranges_buf = [0u8; 8];
            meta_stream.read_exact(&mut num_ranges_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num_ranges: {}", e)))?;
            let num_ranges = u64::from_le_bytes(num_ranges_buf) as usize;
            let mut failed: Vec<(u64, u64)> = Vec::new();
            for _ in 0..num_ranges {
                let mut start_buf = [0u8; 8];
                let mut end_buf = [0u8; 8];
                let mut expected_hash = [0u8; BLAKE3_LEN];
                meta_stream.read_exact(&mut start_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range start: {}", e)))?;
                meta_stream.read_exact(&mut end_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range end: {}", e)))?;
                meta_stream.read_exact(&mut expected_hash).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range hash: {}", e)))?;
                let start = u64::from_le_bytes(start_buf);
                let end = u64::from_le_bytes(end_buf);
                match hash_file_range_path(&output_path, start, end) {
                    Ok(actual) if actual == expected_hash => {}
                    _ => failed.push((start, end)),
                }
            }
            meta_stream.write_all(&[msg::RANGE_VERIFY_RESULT]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send range verify result: {}", e)))?;
            meta_stream.write_all(&(failed.len() as u64).to_le_bytes()).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send num_failed: {}", e)))?;
            for (start, end) in &failed {
                meta_stream.write_all(&start.to_le_bytes()).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send failed start: {}", e)))?;
                meta_stream.write_all(&end.to_le_bytes()).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send failed end: {}", e)))?;
            }
        }

        let mut data_handles = Vec::new();
        for (thread_id, _range) in ranges.into_iter().enumerate() {
            let file = Arc::clone(&file);
            let config = config.clone();
            let range_hasher_tx = hasher_tx.clone();
            let stream = conn.accept_stream().await?;
            let handle = tokio::spawn(async move {
                receive_range_stream(
                    thread_id,
                    file,
                    stream,
                    config,
                    None,
                    Some(range_hasher_tx),
                )
                .await
            });
            data_handles.push(handle);
        }

        let mut total_received = 0u64;
        for (thread_id, handle) in data_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(bytes)) => {
                    total_received += bytes;
                    tracing::debug!(thread_id, bytes, "QUIC data stream completed");
                }
                Ok(Err(e)) => {
                    drop(hasher_tx);
                    return Err(e);
                }
                Err(e) => {
                    drop(hasher_tx);
                    return Err(TransferError::NetworkError(format!("QUIC data task panicked: {:?}", e)));
                }
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
            let file_mut = std::fs::File::try_clone(&*file)?;
            file_mut.sync_all()?;
        }

        let mut hash_type_buf = [0u8; 1];
        meta_stream.read_exact(&mut hash_type_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read hash message type: {}", e)))?;
        if hash_type_buf[0] != msg::HASH {
            return Err(TransferError::ProtocolError(format!(
                "Expected hash message (0x03), got 0x{:02x}",
                hash_type_buf[0]
            )));
        }
        let mut expected_hash = [0u8; BLAKE3_LEN];
        meta_stream.read_exact(&mut expected_hash).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read file hash: {}", e)))?;

        drop(hasher_tx);
        let actual_hash = hasher_handle
            .await
            .map_err(|e| TransferError::NetworkError(format!("Receive hasher task panicked: {:?}", e)))?
            .map_err(|e| TransferError::ProtocolError(format!("Receive hasher: {}", e)))?;
        if actual_hash != expected_hash {
            let _ = meta_stream.write_all(&[msg::HASH_MISMATCH]).await;
            return Err(TransferError::IntegrityCheckFailed(format!(
                "BLAKE3 mismatch for {} (transfer may be corrupted)",
                filename
            )));
        }

        meta_stream.write_all(&[msg::HASH_OK]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send hash OK: {}", e)))?;

        eprintln!("Received: {} ({} bytes)", filename, total_received);

        Ok(())
    }
}
