//! QUIC server for file transfer. Same protocol as TCP server over one connection and multiple streams.

use crate::base::cap_flags;
use crate::base::msg;
use crate::base::{
    apply_capabilities_to_config, capabilities_from_config, Capabilities, CAPABILITIES_WIRE_LEN,
};
use crate::error::TransferError;
use crate::base::FileRange;
use crate::integrity::{hash_file, hash_file_range_path, BLAKE3_LEN};
use crate::quinn_transport::QuicTransport;
use crate::server_cache;
use crate::tcp_transfer::receive_range_stream;
use crate::base::TransferConfig;
use crate::transport::Transport;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::mpsc as std_mpsc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::error;

/// QUIC server: one UDP port, one connection per transfer with meta + N data streams.
pub struct QuicServer {
    port: u16,
    output_dir: PathBuf,
    config: TransferConfig,
    completed_hashes: Arc<Mutex<HashMap<PathBuf, [u8; BLAKE3_LEN]>>>,
    persist_tx: Option<std_mpsc::Sender<(PathBuf, [u8; BLAKE3_LEN])>>,
}

impl QuicServer {
    pub fn new(port: u16, output_dir: PathBuf, config: TransferConfig) -> Self {
        let cache_path = server_cache::server_cache_path();
        let _ = server_cache::ensure_cache_dir(&cache_path);
        let (initial_hashes, persist_tx) = server_cache::load_and_spawn_persist(&cache_path);
        Self {
            port,
            output_dir,
            config,
            completed_hashes: Arc::new(Mutex::new(initial_hashes)),
            persist_tx,
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
                    let completed_hashes = self.completed_hashes.clone();
                    let persist_tx = self.persist_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(conn, output_dir, config, completed_hashes, persist_tx).await {
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
        completed_hashes: Arc<Mutex<HashMap<PathBuf, [u8; BLAKE3_LEN]>>>,
        persist_tx: Option<std_mpsc::Sender<(PathBuf, [u8; BLAKE3_LEN])>>,
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

        let mut first_byte = [0u8; 1];
        if (negotiated.flags & cap_flags::RTT_PROBE) != 0 {
            meta_stream.read_exact(&mut first_byte).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read after handshake: {}", e)))?;
            if first_byte[0] == msg::PING {
                let mut ts_buf = [0u8; 8];
                meta_stream.read_exact(&mut ts_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read PING timestamp: {}", e)))?;
                let mut pong = [0u8; crate::base::PING_PONG_WIRE_LEN];
                pong[0] = msg::PONG;
                pong[1..9].copy_from_slice(&ts_buf);
                meta_stream.write_all(&pong).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send Pong: {}", e)))?;
                meta_stream.read_exact(&mut first_byte).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read after PONG: {}", e)))?;
            }
        } else {
            meta_stream.read_exact(&mut first_byte).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read first byte: {}", e)))?;
        }

        let (filename, file_size, num_streams) = if first_byte[0] == msg::CHECK_HASH {
            let mut len_buf = [0u8; 8];
            meta_stream.read_exact(&mut len_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH filename len: {}", e)))?;
            let name_len = u64::from_le_bytes(len_buf) as usize;
            let mut name_buf = vec![0u8; name_len];
            meta_stream.read_exact(&mut name_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH filename: {}", e)))?;
            let check_filename = String::from_utf8(name_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid CHECK_HASH filename: {}", e)))?;
            let mut hash_buf = [0u8; BLAKE3_LEN];
            meta_stream.read_exact(&mut hash_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH hash: {}", e)))?;
            let path = output_dir.join(&check_filename);
            let have = completed_hashes.lock().unwrap().get(&path).map(|h| h == &hash_buf).unwrap_or(false);
            if have {
                meta_stream.write_all(&[msg::HAVE_HASH]).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send HAVE_HASH: {}", e)))?;
                meta_stream.flush().await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to flush HAVE_HASH: {}", e)))?;
                eprintln!("Skipped (unchanged): {}", check_filename);
                return Ok(());
            }
            meta_stream.write_all(&[msg::NEED_FILE]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send NEED_FILE: {}", e)))?;
            meta_stream.read_exact(&mut len_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read metadata filename len: {}", e)))?;
            let name_len = u64::from_le_bytes(len_buf) as usize;
            let mut name_buf = vec![0u8; name_len];
            meta_stream.read_exact(&mut name_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read metadata filename: {}", e)))?;
            let filename = String::from_utf8(name_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;
            let mut file_size_buf = [0u8; 8];
            meta_stream.read_exact(&mut file_size_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read file size: {}", e)))?;
            let file_size = u64::from_le_bytes(file_size_buf);
            let mut num_streams_buf = [0u8; 8];
            meta_stream.read_exact(&mut num_streams_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num streams: {}", e)))?;
            let num_streams = u64::from_le_bytes(num_streams_buf) as usize;
            (filename, file_size, num_streams)
        } else {
            let mut filename_len_buf = [0u8; 8];
            filename_len_buf[0] = first_byte[0];
            meta_stream.read_exact(&mut filename_len_buf[1..8]).await
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
            (filename, file_size, num_streams)
        };

        eprintln!("Receiving: {}", filename);

        std::fs::create_dir_all(&output_dir)?;

        let output_path = output_dir.join(&filename);
        let file = std::fs::File::create(&output_path)?;
        file.set_len(file_size)?;
        let file = Arc::new(file);

        // Same as TCP: num_streams from metadata (negotiated max_streams); both sides use transfer_num_ranges(num_streams).
        let num_ranges = crate::base::transfer_num_ranges(num_streams);
        let ranges = crate::base::split_file_ranges(file_size, num_ranges);
        let (range_hash_tx, mut range_hash_rx) = mpsc::channel::<(FileRange, [u8; BLAKE3_LEN])>(64);

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
            let range_hash_tx = range_hash_tx.clone();
            let stream = conn.accept_stream().await?;
            let handle = tokio::spawn(async move {
                receive_range_stream(
                    thread_id,
                    file,
                    stream,
                    config,
                    None,
                    Some(range_hash_tx),
                )
                .await
            });
            data_handles.push(handle);
        }

        let mut total_received = 0u64;
        let mut data_error: Option<TransferError> = None;
        for (thread_id, handle) in data_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(bytes)) => {
                    total_received += bytes;
                    tracing::debug!(thread_id, bytes, "QUIC data stream completed");
                }
                Ok(Err(e)) => {
                    if data_error.is_none() {
                        data_error = Some(e);
                    }
                }
                Err(e) => {
                    if data_error.is_none() {
                        data_error = Some(TransferError::NetworkError(format!("QUIC data task panicked: {:?}", e)));
                    }
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

        drop(range_hash_tx);
        let mut received_ranges: Vec<(FileRange, [u8; BLAKE3_LEN])> = Vec::new();
        while let Some(rh) = range_hash_rx.recv().await {
            received_ranges.push(rh);
        }
        let actual_hash = hash_file(&output_path, file_size)?;
        if actual_hash != expected_hash {
            let _ = meta_stream.write_all(&[msg::HASH_MISMATCH]).await;
            return Err(TransferError::IntegrityCheckFailed(format!(
                "BLAKE3 mismatch for {} (transfer may be corrupted)",
                filename
            )));
        }

        meta_stream.write_all(&[msg::HASH_OK]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send hash OK: {}", e)))?;
        meta_stream.flush().await
            .map_err(|e| TransferError::NetworkError(format!("Failed to flush hash OK: {}", e)))?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        if data_error.is_none() {
            completed_hashes.lock().unwrap().insert(output_path.clone(), expected_hash);
            if let Some(ref tx) = persist_tx {
                let _ = tx.send((output_path.clone(), expected_hash));
            }
            eprintln!("Received: {} ({} bytes)", filename, total_received);
        }

        if let Some(e) = data_error {
            return Err(e);
        }
        Ok(())
    }
}
