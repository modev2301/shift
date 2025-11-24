//! Client-side file transfer functionality.
//!
//! This module contains the client implementation for sending files to a server.
//! It includes the transfer manager, message protocol, and session handling.

use crate::compression::{FileChunk, ResumeInfo, SerializableResumeInfo};
use crate::error::TransferError;
use crate::utils::{ChunkPool, RetryManager};
use bytes::Bytes;
use indicatif::{MultiProgress, ProgressBar};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

// Constants for transfer
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;
const MAX_RETRIES: u32 = 3;

// Custom serialization for Bytes
mod bytes_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&bytes[..])
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Handshake {
        session_id: String,
        filename: String,
        file_size: u64,
        chunk_size: usize,
        parallel_streams: usize,
        compression_enabled: bool,
        auth_token: String,
        resume_info: Option<SerializableResumeInfo>,
    },
    FileChunk {
        chunk_id: u64,
        session_id: String,
        #[serde(with = "bytes_serde")]
        data: Bytes,
        is_last: bool,
        is_compressed: bool,
        checksum: u32,
        original_size: usize,
        sequence_number: u64,
        retry_count: u32,
    },
    ChunkAck {
        session_id: String,
        chunk_id: u64,
        success: bool,
        error_message: Option<String>,
    },
    EndOfStream {
        session_id: String,
        stream_id: u64,
    },
    Error {
        session_id: String,
        error_message: String,
    },
}

impl Message {
    pub async fn write_to_stream<T>(&self, writer: &mut T) -> Result<(), TransferError>
    where
        T: AsyncWrite + Unpin,
    {
        match self {
            // Use binary protocol for FileChunk for maximum performance
            Message::FileChunk {
                chunk_id,
                session_id,
                data,
                is_last,
                is_compressed,
                checksum,
                original_size,
                sequence_number: _,
                retry_count: _,
            } => {
                // Binary format: [magic:u8=0xFF][type:u8=0x01][chunk_id:u64][session_len:u8][session][data_len:u32][data][flags:u8][checksum:u32][original_size:u32]
                // Direct write without buffering for maximum performance
                writer.write_all(&[0xFF, 0x01]).await.map_err(|e| TransferError::Io(e))?; // Magic + type
                writer.write_all(&chunk_id.to_be_bytes()).await.map_err(|e| TransferError::Io(e))?;
                writer.write_all(&[session_id.len() as u8]).await.map_err(|e| TransferError::Io(e))?;
                writer.write_all(session_id.as_bytes()).await.map_err(|e| TransferError::Io(e))?;
                writer.write_all(&(data.len() as u32).to_be_bytes()).await.map_err(|e| TransferError::Io(e))?;
                writer.write_all(data).await.map_err(|e| TransferError::Io(e))?;
                let flags = (*is_last as u8) | ((*is_compressed as u8) << 1);
                writer.write_all(&[flags]).await.map_err(|e| TransferError::Io(e))?;
                writer.write_all(&checksum.to_be_bytes()).await.map_err(|e| TransferError::Io(e))?;
                writer.write_all(&(*original_size as u32).to_be_bytes()).await.map_err(|e| TransferError::Io(e))?;
                // Don't flush - let TCP buffer handle batching
            }
            // Keep JSON for control messages (handshake, ACKs) - they're small
            _ => {
                let json = serde_json::to_string(self).map_err(|e| TransferError::Serialization(e))?;
                let length = json.len() as u32;

                writer
                    .write_all(&length.to_be_bytes())
                    .await
                    .map_err(|e| TransferError::Io(e))?;
                writer
                    .write_all(json.as_bytes())
                    .await
                    .map_err(|e| TransferError::Io(e))?;
                writer.flush().await.map_err(|e| TransferError::Io(e))?;
            }
        }

        Ok(())
    }

    pub async fn read_from_stream<T>(reader: &mut T) -> Result<Option<Self>, TransferError>
    where
        T: AsyncRead + Unpin,
    {
        // Peek at first byte to determine format
        // Binary messages start with magic byte 0xFF, JSON messages start with length (4 bytes)
        // CRITICAL: After handshake, all FileChunk messages are binary, control messages are JSON
        let mut magic_byte = [0u8; 1];
        match reader.read_exact(&mut magic_byte).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(TransferError::Io(e)),
        }

        // Binary protocol detection: 0xFF is the magic byte
        // JSON messages never start with 0xFF (they start with '{' or length bytes < 0x80 typically)
        if magic_byte[0] == 0xFF {
            // Binary format - read message type
            let mut type_byte = [0u8; 1];
            reader.read_exact(&mut type_byte).await?;
            
            match type_byte[0] {
                0x01 => {
                    // Binary FileChunk format
                    let mut chunk_id_bytes = [0u8; 8];
                    reader.read_exact(&mut chunk_id_bytes).await?;
                    let chunk_id = u64::from_be_bytes(chunk_id_bytes);
                    
                    let mut session_len_bytes = [0u8; 1];
                    reader.read_exact(&mut session_len_bytes).await?;
                    let session_len = session_len_bytes[0] as usize;
                    
                    let mut session_bytes = vec![0u8; session_len];
                    reader.read_exact(&mut session_bytes).await?;
                    let session_id = String::from_utf8(session_bytes).map_err(|e| {
                        TransferError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
                    })?;
                    
                    let mut data_len_bytes = [0u8; 4];
                    reader.read_exact(&mut data_len_bytes).await?;
                    let data_len = u32::from_be_bytes(data_len_bytes) as usize;
                    
                    // CRITICAL: Sanity check BEFORE allocation to prevent OOM
                    // Max chunk size is 16MB by default, but allow up to 32MB for safety
                    const MAX_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32MB max
                    if data_len > MAX_CHUNK_SIZE {
                        return Err(TransferError::ProtocolError(format!(
                            "Chunk data length too large: {} bytes (max: {}). Possible protocol error.",
                            data_len, MAX_CHUNK_SIZE
                        )));
                    }
                    
                    let mut data = vec![0u8; data_len];
                    reader.read_exact(&mut data).await?;
                    
                    let mut flags_bytes = [0u8; 1];
                    reader.read_exact(&mut flags_bytes).await?;
                    let flags = flags_bytes[0];
                    let is_last = (flags & 0x01) != 0;
                    let is_compressed = (flags & 0x02) != 0;
                    
                    let mut checksum_bytes = [0u8; 4];
                    reader.read_exact(&mut checksum_bytes).await?;
                    let checksum = u32::from_be_bytes(checksum_bytes);
                    
                    let mut original_size_bytes = [0u8; 4];
                    reader.read_exact(&mut original_size_bytes).await?;
                    let original_size = u32::from_be_bytes(original_size_bytes) as usize;
                    
                    Ok(Some(Message::FileChunk {
                        chunk_id,
                        session_id,
                        data: Bytes::from(data),
                        is_last,
                        is_compressed,
                        checksum,
                        original_size,
                        sequence_number: chunk_id,
                        retry_count: 0,
                    }))
                }
                _ => {
                    return Err(TransferError::ProtocolError(format!("Unknown binary message type: {}", type_byte[0])));
                }
            }
        } else {
            // JSON format - the byte we read is the first byte of the length prefix
            // Read remaining 3 bytes
            let mut length_bytes = [magic_byte[0], 0u8, 0u8, 0u8];
            reader.read_exact(&mut length_bytes[1..]).await?;
            let length = u32::from_be_bytes(length_bytes) as usize;
            
            // CRITICAL: Sanity check BEFORE allocation to prevent OOM attacks
            // JSON control messages (handshake, ACKs) are small - max 64KB
            // If length is huge, it's likely a protocol error (binary data misinterpreted as JSON)
            const MAX_JSON_MESSAGE_SIZE: usize = 64 * 1024; // 64KB max for JSON
            if length > MAX_JSON_MESSAGE_SIZE {
                return Err(TransferError::ProtocolError(format!(
                    "JSON message length too large: {} bytes (max: {}). This might be binary data misinterpreted as JSON.",
                    length, MAX_JSON_MESSAGE_SIZE
                )));
            }
            
            let mut json_bytes = vec![0u8; length];
            reader.read_exact(&mut json_bytes).await?;
            
            let json = String::from_utf8(json_bytes).map_err(|e| {
                TransferError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ))
            })?;
            
            let message = serde_json::from_str(&json).map_err(|e| TransferError::Serialization(e))?;
            Ok(Some(message))
        }
    }
}

pub struct TransferSession {
    pub session_id: String,
    pub filename: String,
    pub file_size: u64,
    pub enable_progress_bar: bool,
    pub chunk_size: usize,
    pub completion_tx: oneshot::Sender<Result<PathBuf, TransferError>>,
    pub parallel_streams: usize,
    pub resume_info: Option<ResumeInfo>,
    pub ack_tx: mpsc::Sender<Message>,
    pub chunk_pool: Arc<ChunkPool>,
    pub output_directory: PathBuf,

    // State
    pub received_chunk_ids: HashSet<u64>, // Only track IDs, not data
    pub file_handle: Option<Arc<tokio::sync::Mutex<tokio::fs::File>>>, // Async file handle for writing
    pub completed_streams: HashSet<u64>,
    pub total_streams: usize,
    pub is_completed: AtomicBool,
    pub bytes_received: AtomicU64,
    pub start_time: Instant,
}

impl TransferSession {
    pub fn new(
        session_id: String,
        filename: String,
        file_size: u64,
        enable_progress_bar: bool,
        chunk_size: usize,
        completion_tx: oneshot::Sender<Result<PathBuf, TransferError>>,
        parallel_streams: usize,
        resume_info: Option<ResumeInfo>,
        ack_tx: mpsc::Sender<Message>,
        chunk_pool: Arc<ChunkPool>,
        output_directory: PathBuf,
    ) -> Self {
        let _total_chunks = (file_size + chunk_size as u64 - 1) / chunk_size as u64;

        // Initialize file writer immediately to write chunks as they arrive
        // Convert to absolute path and normalize (remove ./ and ../)
        let output_path = {
            let abs_dir = if output_directory.is_absolute() {
                output_directory.clone()
            } else {
                // If relative, make it absolute based on current working directory
                match std::env::current_dir() {
                    Ok(cwd) => {
                        // Remove leading ./ if present
                        let dir_str = output_directory.to_string_lossy();
                        let clean_dir = if dir_str.starts_with("./") {
                            &dir_str[2..]
                        } else {
                            &dir_str
                        };
                        cwd.join(clean_dir)
                    }
                    Err(_) => output_directory.clone(), // Fallback to relative
                }
            };

            // Normalize the path (remove ./ and ../ components) if directory exists
            let normalized_dir = abs_dir.canonicalize().unwrap_or_else(|_| abs_dir);
            normalized_dir.join(&filename)
        };

        // Ensure output directory exists with proper permissions
        if let Some(parent) = output_path.parent() {
            // Create directory first
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(
                    "Failed to create output directory {}: {}",
                    parent.display(),
                    e
                );
            }

            // Set permissions so root can write (if running as root)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                // Try to set permissions - this will fail if we don't have permission, which is OK
                if let Ok(metadata) = std::fs::metadata(parent) {
                    let mut perms = metadata.permissions();
                    perms.set_mode(0o777); // rwxrwxrwx - allow all users to write
                    match std::fs::set_permissions(parent, perms) {
                        Ok(_) => {
                            tracing::debug!("Set permissions on {} to 777", parent.display());
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to set permissions on {}: {} (may need sudo)",
                                parent.display(),
                                e
                            );
                            // Try to verify if directory is actually writable by attempting to create a test file
                            let test_file = parent.join(".shift_write_test");
                            match std::fs::File::create(&test_file) {
                                Ok(_) => {
                                    let _ = std::fs::remove_file(&test_file);
                                    tracing::debug!(
                                        "Directory {} is writable despite permission set failure",
                                        parent.display()
                                    );
                                }
                                Err(e2) => {
                                    tracing::error!(
                                        "Directory {} is NOT writable: {}",
                                        parent.display(),
                                        e2
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Remove existing file if it exists (to avoid permission issues)
        if output_path.exists() {
            if let Err(e) = std::fs::remove_file(&output_path) {
                tracing::warn!(
                    "Failed to remove existing file {}: {}",
                    output_path.display(),
                    e
                );
            }
        }

        // Use async file I/O instead of memory mapping to avoid permission issues with large files
        // Create file synchronously first, then convert to async handle
        let file_handle = match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&output_path)
        {
            Ok(file) => {
                // Pre-allocate file size (more efficient than letting it grow)
                if let Err(e) = file.set_len(file_size) {
                    tracing::warn!(
                        "Failed to pre-allocate file size {}: {} (will grow dynamically)",
                        file_size,
                        e
                    );
                }
                // Convert to async file handle
                let async_file = tokio::fs::File::from_std(file);
                tracing::info!(
                    "Created output file: {} ({} bytes)",
                    output_path.display(),
                    file_size
                );
                Some(Arc::new(tokio::sync::Mutex::new(async_file)))
            }
            Err(e) => {
                tracing::error!(
                    "Failed to create output file {}: {} (cwd: {:?})",
                    output_path.display(),
                    e,
                    std::env::current_dir()
                );
                None
            }
        };

        Self {
            session_id,
            filename,
            file_size,
            enable_progress_bar,
            chunk_size,
            completion_tx,
            parallel_streams,
            resume_info,
            ack_tx,
            chunk_pool,
            output_directory,
            received_chunk_ids: HashSet::new(),
            file_handle,
            completed_streams: HashSet::new(),
            total_streams: parallel_streams,
            is_completed: AtomicBool::new(false),
            bytes_received: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Processes a file chunk and writes it to disk at the correct offset.
    ///
    /// This function validates the chunk (checksum and size), decompresses if needed,
    /// and writes the data directly to disk. Chunks can be written out of order,
    /// which is important for parallel transfers where chunks may arrive in any sequence.
    ///
    /// # Arguments
    ///
    /// * `chunk` - The file chunk to process and write
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if this was the last chunk and the transfer is complete,
    /// `Ok(false)` if more chunks are expected, or an error if processing failed.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Chunk checksum validation fails
    /// - Decompression fails
    /// - Decompressed size doesn't match expected size
    /// - Chunk size doesn't match expected size for its position
    /// - File I/O operations fail
    pub async fn add_chunk(&mut self, mut chunk: FileChunk) -> Result<bool, TransferError> {
        use tracing::{error, warn};

        // Validate chunk checksum before decompression.
        // The checksum is computed over the compressed data if compression is enabled,
        // so we must validate before decompressing.
        if !chunk.validate() {
            error!(
                chunk_id = chunk.chunk_id,
                "Chunk checksum validation failed"
            );
            return Err(TransferError::ChunkValidationFailed {
                chunk_id: chunk.chunk_id,
            });
        }

        // Decompress if compression was enabled during transfer.
        // After decompression, we verify the size matches the expected original size.
        if chunk.is_compressed {
            chunk.decompress()?;
        }

        // Verify that the decompressed size matches the expected original size.
        // This ensures data integrity after decompression.
        if chunk.data.len() != chunk.original_size {
            error!(
                chunk_id = chunk.chunk_id,
                expected_size = chunk.original_size,
                actual_size = chunk.data.len(),
                "Chunk size mismatch after decompression"
            );
            return Err(TransferError::ChunkValidationFailed {
                chunk_id: chunk.chunk_id,
            });
        }

        // Write chunk directly to disk at the calculated file offset.
        // We use async file I/O rather than memory mapping to avoid permission
        // issues with large files and to support out-of-order writes efficiently.
        if let Some(ref file_handle) = self.file_handle {
            use tokio::io::AsyncSeekExt;
            use tokio::io::AsyncWriteExt;

            // Calculate the file offset for this chunk based on its ID.
            // Chunks are numbered sequentially, so offset = chunk_id * chunk_size.
            let offset = chunk.chunk_id as u64 * self.chunk_size as u64;

            // Determine if this is the last chunk and calculate expected size.
            // The last chunk may be smaller than chunk_size if the file size
            // is not evenly divisible by chunk_size.
            let is_last_chunk = offset + self.chunk_size as u64 >= self.file_size;
            let expected_size = if is_last_chunk {
                self.file_size - offset
            } else {
                self.chunk_size as u64
            };

            // Validate that the chunk size matches what we expect for this position.
            // This prevents data corruption from incorrectly sized chunks.
            if chunk.data.len() as u64 != expected_size {
                error!(
                    chunk_id = chunk.chunk_id,
                    expected_size = expected_size,
                    actual_size = chunk.data.len(),
                    offset = offset,
                    "Chunk size mismatch for position"
                );
                return Err(TransferError::ChunkValidationFailed {
                    chunk_id: chunk.chunk_id,
                });
            }

            // Calculate the actual number of bytes to write.
            // For the last chunk, this may be less than chunk_size.
            let remaining_bytes = self.file_size.saturating_sub(offset);
            let write_size = std::cmp::min(chunk.data.len(), remaining_bytes as usize);

            if write_size == 0 {
                warn!(
                    chunk_id = chunk.chunk_id,
                    offset = offset,
                    file_size = self.file_size,
                    "Chunk has zero write size"
                );
                // Still mark as received to avoid blocking completion
                self.received_chunk_ids.insert(chunk.chunk_id);
                return Ok(false);
            }

            let mut file = file_handle.lock().await;
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(|e| {
                    tracing::error!(
                        "Failed to seek to offset {} for chunk {}: {}",
                        offset,
                        chunk.chunk_id,
                        e
                    );
                    TransferError::Io(e)
                })?;

            // Write only the data that fits
            if write_size < chunk.data.len() {
                file.write_all(&chunk.data[..write_size])
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            "Failed to write chunk {} (truncated to {} bytes): {}",
                            chunk.chunk_id,
                            write_size,
                            e
                        );
                        TransferError::Io(e)
                    })?;
            } else {
                file.write_all(&chunk.data).await.map_err(|e| {
                    tracing::error!("Failed to write chunk {}: {}", chunk.chunk_id, e);
                    TransferError::Io(e)
                })?;
            }

            // Don't flush after every chunk - OS buffers handle this efficiently
            // Only sync_all() at the end for data integrity
            // Removing per-chunk flush saves ~1024 syscalls for 1GB file

            // Remove per-chunk logging - it's expensive and not needed
            // Only log errors, not successful writes
        } else {
            let error_msg = format!("File writer not initialized for chunk {}", chunk.chunk_id);
            tracing::error!("{}", error_msg);
            return Err(TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                error_msg,
            )));
        }

        // Track that we received this chunk ID (not the data)
        self.received_chunk_ids.insert(chunk.chunk_id);

        // Update resume info if available
        if let Some(ref mut _resume_info) = self.resume_info {
            // Resume tracking is handled through received_chunk_ids
        }

        // Update progress
        self.bytes_received
            .fetch_add(chunk.size() as u64, Ordering::Relaxed);

        // Check if transfer is complete
        let total_chunks = (self.file_size + self.chunk_size as u64 - 1) / self.chunk_size as u64;
        if self.received_chunk_ids.len() as u64 >= total_chunks {
            self.complete_transfer().await?;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn handle_end_marker(&self) -> bool {
        // This would track stream completion
        // For now, return false to indicate not all streams are done
        false
    }

    /// Completes the transfer by syncing data to disk and verifying file integrity.
    ///
    /// This function ensures all data is persisted to disk, verifies the final file
    /// size matches expectations, and signals completion to any waiting tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File sync to disk fails
    /// - Final file size doesn't match expected size
    async fn complete_transfer(&mut self) -> Result<(), TransferError> {
        use tracing::{error, info};

        let output_path = self.output_directory.join(&self.filename);

        if let Some(ref file_handle) = self.file_handle {
            let file = file_handle.lock().await;

            // Sync all data and metadata to disk to ensure persistence.
            // This is critical for data integrity - without this, data might only
            // be in OS buffers and could be lost on system crash.
            file.sync_all().await.map_err(|e| {
                error!(
                    filename = %self.filename,
                    error = %e,
                    "Failed to sync file to disk"
                );
                TransferError::Io(e)
            })?;

            // Verify the final file size matches what we expect.
            // This is a final integrity check to catch any corruption or truncation.
            if let Ok(metadata) = file.metadata().await {
                if metadata.len() != self.file_size {
                    error!(
                        filename = %self.filename,
                        expected_size = self.file_size,
                        actual_size = metadata.len(),
                        "File size mismatch after transfer"
                    );
                    return Err(TransferError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "File size mismatch: expected {}, got {}",
                            self.file_size,
                            metadata.len()
                        ),
                    )));
                }
                info!(
                    filename = %self.filename,
                    file_size = metadata.len(),
                    "Transfer completed and synced to disk"
                );
            }
        }

        // Signal completion to any tasks waiting for the transfer to finish.
        // We replace the sender with a dummy to take ownership of the original.
        let completion_tx = std::mem::replace(&mut self.completion_tx, oneshot::channel().0);
        let _ = completion_tx.send(Ok(output_path));

        self.is_completed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

pub struct ClientTransferManager {
    pub file_path: PathBuf,
    pub config: Arc<crate::config::ClientConfig>,
    pub multi_progress: MultiProgress,
    pub session_id: String,
    pub file_size: u64,
    pub chunk_size: usize,
    pub parallel_streams: usize,
    pub progress_bar: Option<ProgressBar>,
    pub resume_info: Arc<Mutex<ResumeInfo>>,
    pub retry_manager: RetryManager,
    pub chunk_pool: Arc<ChunkPool>,
}

impl ClientTransferManager {
    pub async fn new(
        file_path: PathBuf,
        config: Arc<crate::config::ClientConfig>,
        multi_progress: MultiProgress,
    ) -> Result<Self, TransferError> {
        use tokio::fs;

        // Get file metadata
        let metadata = fs::metadata(&file_path)
            .await
            .map_err(|e| TransferError::Io(e))?;
        let file_size = metadata.len();

        // Generate session ID
        let session_id = Uuid::new_v4().to_string();

        // Calculate optimal chunk size
        let chunk_size = config
            .chunk_size
            .unwrap_or_else(|| crate::utils::get_optimal_chunk_size_for_file(file_size));

        // Calculate parallel streams
        let parallel_streams = config.parallel_streams.unwrap_or_else(|| {
            crate::utils::calculate_parallel_streams(file_size, chunk_size as f64)
        });

        // Create progress bar
        let progress_bar = if config.progress_bar_enabled {
            let pb = multi_progress.add(ProgressBar::new(file_size));
            pb.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) ({eta})")
                    .unwrap()
                    .progress_chars("#>-")
            );
            Some(pb)
        } else {
            None
        };

        // Create resume info
        let total_chunks = (file_size + chunk_size as u64 - 1) / chunk_size as u64;
        let resume_info = Arc::new(Mutex::new(ResumeInfo::new(
            session_id.clone(),
            file_path.clone(),
            total_chunks,
            file_size,
            chunk_size,
        )));

        // Create chunk pool
        let chunk_pool = Arc::new(ChunkPool::new(
            config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE),
            2000,
        ));

        Ok(Self {
            file_path,
            config,
            multi_progress,
            session_id,
            file_size,
            chunk_size,
            parallel_streams,
            progress_bar,
            resume_info,
            retry_manager: RetryManager::new(MAX_RETRIES),
            chunk_pool,
        })
    }

    /// Establish TCP connections to server (can be reused across multiple files)
    pub async fn establish_connections(
        config: &crate::config::ClientConfig,
    ) -> Result<Vec<(tokio::io::ReadHalf<tokio::net::TcpStream>, tokio::io::WriteHalf<tokio::net::TcpStream>)>, TransferError> {
        use tokio::net::TcpStream;
        
        let num_connections = config.num_connections.unwrap_or(8);
        let server_addr = format!("{}:{}", config.server_address, config.server_port);
        
        tracing::info!("Establishing {} TCP connections to server for connection pool", num_connections);
        
        let mut connections = Vec::new();
        for i in 0..num_connections {
            let stream = TcpStream::connect(&server_addr)
                .await
                .map_err(|e| TransferError::NetworkError(format!("Failed to connect connection {}: {}", i, e)))?;

            // TCP socket tuning for maximum throughput
            stream.set_nodelay(true).map_err(|e| {
                TransferError::NetworkError(format!("Failed to set TCP_NODELAY on connection {}: {}", i, e))
            })?;
            
            // TCP buffer tuning for maximum throughput
            // Set explicit buffer sizes (2MB send, 2MB receive) for better throughput
            // Use socket2 to set socket options
            use socket2::SockRef;
            let std_stream = stream.into_std().map_err(|e| {
                TransferError::NetworkError(format!("Failed to convert to std stream on connection {}: {}", i, e))
            })?;
            
            let socket = socket2::Socket::from(std_stream);
            let sock_ref = SockRef::from(&socket);
            
            // Set send and receive buffer sizes (50MB each for high-bandwidth networks)
            // For 10Gbps with 10ms RTT: BDP = 12.5MB, use 4x BDP = 50MB
            const TCP_BUFFER_SIZE: i32 = 50 * 1024 * 1024; // 50MB
            if let Err(e) = sock_ref.set_send_buffer_size(TCP_BUFFER_SIZE as usize) {
                tracing::warn!("Failed to set SO_SNDBUF on connection {}: {} (using OS default)", i, e);
            } else {
                tracing::debug!("Set SO_SNDBUF to {} bytes on connection {}", TCP_BUFFER_SIZE, i);
            }
            
            if let Err(e) = sock_ref.set_recv_buffer_size(TCP_BUFFER_SIZE as usize) {
                tracing::warn!("Failed to set SO_RCVBUF on connection {}: {} (using OS default)", i, e);
            } else {
                tracing::debug!("Set SO_RCVBUF to {} bytes on connection {}", TCP_BUFFER_SIZE, i);
            }
            
            // Verify buffer sizes were actually set
            if let Ok(actual_send_buf) = sock_ref.send_buffer_size() {
                if let Ok(actual_recv_buf) = sock_ref.recv_buffer_size() {
                    tracing::debug!(
                        "Connection {} buffer sizes - Send: {} bytes, Recv: {} bytes",
                        i, actual_send_buf, actual_recv_buf
                    );
                }
            }
            
            // Convert back to std::net::TcpStream then tokio
            let std_stream: std::net::TcpStream = socket.into();
            let stream = tokio::net::TcpStream::from_std(std_stream).map_err(|e| {
                TransferError::NetworkError(format!("Failed to convert back to tokio stream on connection {}: {}", i, e))
            })?;

            let (reader, writer) = tokio::io::split(stream);
            connections.push((reader, writer));
            tracing::debug!("Connection {} established", i);
        }
        
        tracing::info!("All {} connections established successfully", num_connections);
        Ok(connections)
    }

    pub async fn run_transfer(&mut self) -> Result<(), TransferError> {
        self.run_transfer_with_connections(None).await
    }

    pub async fn run_transfer_with_connections(
        &mut self,
        mut pre_established_connections: Option<Vec<(tokio::io::ReadHalf<tokio::net::TcpStream>, tokio::io::WriteHalf<tokio::net::TcpStream>)>>,
    ) -> Result<(), TransferError> {
        use crate::compression::FileChunk;
        use crate::zero_copy::ZeroCopyReader;
        use std::sync::Arc;
        use std::time::Instant;

        tracing::info!("Starting file transfer for {}", self.file_path.display());
        tracing::info!("File size: {} bytes", self.file_size);
        tracing::info!("Chunk size: {} bytes", self.chunk_size);
        tracing::info!("Parallel streams: {}", self.parallel_streams);
        tracing::info!("Compression enabled: {}", self.config.enable_compression);

        // 1. Connect to server (reuse connections if provided, otherwise create new)
        let num_connections = self.config.num_connections.unwrap_or(8);
        let connections = if let Some(conns) = pre_established_connections.take() {
            tracing::info!("Reusing {} pre-established connections", conns.len());
            conns
        } else {
            tracing::info!("Establishing {} new TCP connections to server", num_connections);
            Self::establish_connections(&self.config).await?
        };
        
        // 2. Send handshake on ALL connections (required for multi-connection mode)
        let filename = self
            .file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        let resume_info = if self.config.enable_resume {
            let resume = self.resume_info.lock().await;
            Some(resume.to_serializable())
        } else {
            None
        };

        let handshake = Message::Handshake {
            session_id: self.session_id.clone(),
            filename: filename.clone(),
            file_size: self.file_size,
            chunk_size: self.chunk_size,
            parallel_streams: self.parallel_streams,
            compression_enabled: self.config.enable_compression,
            auth_token: "shift_default_token".to_string(),
            resume_info,
        };

        // Send handshake on all connections - server requires handshake before accepting chunks
        let mut handshake_connections = Vec::new();
        for (idx, (mut conn_reader, mut conn_writer)) in connections.into_iter().enumerate() {
            if let Err(e) = handshake.write_to_stream(&mut conn_writer).await {
                return Err(TransferError::NetworkError(format!(
                    "Failed to send handshake on connection {}: {}", idx, e
                )));
            }
            if let Err(e) = conn_writer.flush().await {
                return Err(TransferError::NetworkError(format!(
                    "Failed to flush handshake on connection {}: {}", idx, e
                )));
            }
            handshake_connections.push((conn_reader, conn_writer));
            tracing::debug!("Handshake sent on connection {}", idx);
        }
        
        tracing::info!("Handshake sent on all {} connections", num_connections);
        
        // Use first connection for ACK reading
        let (mut reader, mut writer) = handshake_connections.remove(0);
        let mut additional_connections = handshake_connections;

        // 3. Open file with mmap reader (O_DIRECT disabled - was slower)
        enum FileReader {
            Mmap(ZeroCopyReader),
        }
        
        let mut file_reader: FileReader = FileReader::Mmap(ZeroCopyReader::new(&self.file_path)?);
        
        let total_chunks = (self.file_size + self.chunk_size as u64 - 1) / self.chunk_size as u64;

        // 4. Create channels for each connection (distribute chunks across connections)
        // Each connection gets its own channel for true parallelism
        const CHANNEL_BUFFER_SIZE: usize = 32;
        let mut writer_handles = Vec::new();
        let mut chunk_txs = Vec::new();
        
        // Create writer tasks for each additional connection
        for (conn_idx, (_, mut conn_writer)) in additional_connections.into_iter().enumerate() {
            let (chunk_tx, mut chunk_rx) = mpsc::channel::<Message>(CHANNEL_BUFFER_SIZE);
            chunk_txs.push(chunk_tx);
            let progress_bar_clone = self.progress_bar.clone();
            
            let writer_handle = tokio::spawn(async move {
                let mut error_count = 0;
                let mut chunks_sent = 0u64;
                let mut last_flush = std::time::Instant::now();
                const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
                
                while let Some(chunk_msg) = chunk_rx.recv().await {
                    match chunk_msg {
                        Message::EndOfStream { .. } => {
                            // Send EndOfStream and close connection gracefully
                            if let Err(e) = chunk_msg.write_to_stream(&mut conn_writer).await {
                                // Connection may already be closed - that's OK
                                tracing::debug!("Failed to write EndOfStream on connection {} (may be closed): {}", conn_idx, e);
                                break;
                            }
                            if let Err(e) = conn_writer.flush().await {
                                tracing::debug!("Failed to flush after EndOfStream on connection {}: {}", conn_idx, e);
                                break;
                            }
                            tracing::debug!("EndOfStream sent on connection {}", conn_idx);
                            break; // Exit loop after sending EndOfStream
                        }
                        Message::FileChunk { ref data, .. } => {
                            let chunk_size = data.len();
                            
                            if let Err(e) = chunk_msg.write_to_stream(&mut conn_writer).await {
                                tracing::error!("Failed to write chunk on connection {}: {}", conn_idx, e);
                                error_count += 1;
                                if error_count > 10 {
                                    tracing::error!("Too many write errors on connection {}, exiting", conn_idx);
                                    break;
                                }
                            } else {
                                error_count = 0;
                                chunks_sent += 1;
                                
                                // Update progress bar after actual network send
                                if let Some(ref pb) = progress_bar_clone {
                                    pb.inc(chunk_size as u64);
                                }
                                
                                // Time-based flushing for smoother network behavior
                                if last_flush.elapsed() >= FLUSH_INTERVAL {
                                    if let Err(e) = conn_writer.flush().await {
                                        tracing::warn!("Failed to flush on connection {}: {}", conn_idx, e);
                                    }
                                    last_flush = std::time::Instant::now();
                                }
                            }
                        }
                        _ => {
                            if let Err(e) = chunk_msg.write_to_stream(&mut conn_writer).await {
                                tracing::error!("Failed to write message on connection {}: {}", conn_idx, e);
                                break;
                            }
                        }
                    }
                }
                tracing::debug!("Writer task {} completed (sent {} chunks)", conn_idx, chunks_sent);
            });
            writer_handles.push(writer_handle);
        }
        
        // First connection is used for handshake and ACK reading, also needs a writer for chunks
        let (chunk_tx_main, mut chunk_rx_main) = mpsc::channel::<Message>(CHANNEL_BUFFER_SIZE);
        chunk_txs.push(chunk_tx_main.clone());
        let progress_bar_main = self.progress_bar.clone();
        
        let writer_handle_main = {
            let mut writer_main = writer;
            tokio::spawn(async move {
                let mut error_count = 0;
                let mut eos_sent = false;
                let mut chunks_sent = 0u64;
                let mut last_flush = std::time::Instant::now();
                const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
                
                while let Some(chunk_msg) = chunk_rx_main.recv().await {
                    match chunk_msg {
                        Message::EndOfStream { .. } => {
                            if let Err(e) = chunk_msg.write_to_stream(&mut writer_main).await {
                                tracing::error!("Failed to write EndOfStream: {}", e);
                                break;
                            }
                            if let Err(e) = writer_main.flush().await {
                                tracing::error!("Failed to flush after EndOfStream: {}", e);
                                break;
                            }
                            eos_sent = true;
                            tracing::debug!("EndOfStream sent on main connection");
                            break; // Exit loop after sending EndOfStream
                        }
                        Message::FileChunk { ref data, .. } => {
                            let chunk_size = data.len();
                            
                            if let Err(e) = chunk_msg.write_to_stream(&mut writer_main).await {
                                tracing::error!("Failed to write chunk to stream: {}", e);
                                error_count += 1;
                                if error_count > 10 {
                                    tracing::error!("Too many write errors, exiting writer");
                                    break;
                                }
                            } else {
                                error_count = 0;
                                chunks_sent += 1;
                                
                                // Update progress bar after actual network send
                                if let Some(ref pb) = progress_bar_main {
                                    pb.inc(chunk_size as u64);
                                }
                                
                                // Time-based flushing for smoother network behavior
                                if last_flush.elapsed() >= FLUSH_INTERVAL {
                                    if let Err(e) = writer_main.flush().await {
                                        tracing::warn!("Failed to flush: {}", e);
                                    }
                                    last_flush = std::time::Instant::now();
                                }
                            }
                        }
                        _ => {
                            if let Err(e) = chunk_msg.write_to_stream(&mut writer_main).await {
                                tracing::error!("Failed to write message to stream: {}", e);
                                break;
                            }
                        }
                    }
                }
                if eos_sent {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                tracing::debug!("Main writer task completed (sent {} chunks)", chunks_sent);
            })
        };
        writer_handles.push(writer_handle_main);

        // 6. Spawn minimal ACK reader task (only for errors/completion)
        // We don't send per-chunk ACKs - relies on TCP flow control
        // We only read ACKs for errors or completion, not for every chunk
        let completion_flag = Arc::new(tokio::sync::Notify::new());
        let reader_handle = {
            let _session_id = self.session_id.clone();
            let completion_flag_clone = Arc::clone(&completion_flag);
            tokio::spawn(async move {
                loop {
                    match Message::read_from_stream(&mut reader).await {
                        Ok(Some(Message::ChunkAck {
                            chunk_id,
                            success,
                            error_message,
                            ..
                        })) => {
                            if !success {
                                // Only log errors - successful chunks don't need ACKs
                                tracing::error!(
                                    "Chunk {} failed on server: {:?}",
                                    chunk_id,
                                    error_message
                                );
                            } else {
                                // Completion ACK - transfer done
                                tracing::info!("Received completion ACK for chunk {}", chunk_id);
                                completion_flag_clone.notify_one();
                                break;
                            }
                        }
                        Ok(Some(Message::Error { error_message, .. })) => {
                            tracing::error!("Server error: {}", error_message);
                            break;
                        }
                        Ok(None) => {
                            tracing::debug!("Connection closed by server (EOF)");
                            // Don't break immediately - might be normal after ACK
                            // Let the timeout handle it
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            break;
                        }
                        Err(e) => {
                            // Check if it's a normal connection close or a real error
                            match &e {
                                TransferError::Io(io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof => {
                                    tracing::debug!("Connection closed by server (EOF)");
                                }
                                _ => {
                                    tracing::debug!("ACK read error: {}", e);
                                }
                            }
                            break;
                        }
                        Ok(Some(_)) => {
                            // Other message types - log but continue
                            tracing::debug!("Received unexpected message type from server");
                        }
                    }
                }
                tracing::debug!("ACK reader task completed");
            })
        };

        // 7. Read chunks and process in parallel (compression), then queue for sending
        // Use bounded channel with shared receiver for better concurrency (no semaphore lock contention)
        // Work item includes pre-calculated checksum to avoid redundant calculation
        let (work_tx, work_rx) = mpsc::channel::<(u64, Bytes, bool, u32)>(self.parallel_streams);
        let work_rx = Arc::new(tokio::sync::Mutex::new(work_rx));
        let session_id = self.session_id.clone();
        let enable_compression = self.config.enable_compression;
        let chunk_txs_clone = chunk_txs.clone();
        
        // Spawn worker pool for chunk processing
        let mut worker_handles = Vec::new();
        for _worker_id in 0..self.parallel_streams {
            let work_rx = Arc::clone(&work_rx);
            let session_id = session_id.clone();
            let enable_compression = enable_compression;
            let chunk_txs = chunk_txs_clone.clone();
            
            let handle = tokio::spawn(async move {
                loop {
                    let work = {
                        let mut rx = work_rx.lock().await;
                        rx.recv().await
                    };
                    
                    match work {
                        Some((chunk_id, data, is_last, checksum)) => {
                            // Distribute chunks across connections using round-robin
                            let connection_idx = (chunk_id as usize) % chunk_txs.len();
                            let chunk_tx = &chunk_txs[connection_idx];
                            
                            // Create chunk with pre-calculated checksum
                            let mut chunk = FileChunk::new(
                                chunk_id,
                                session_id.clone(),
                                data,
                                is_last,
                                chunk_id,
                            );
                            chunk.checksum = checksum; // Use pre-calculated checksum from read
                            chunk.original_size = chunk.data.len();
                            
                            // Compress if enabled (will recalculate checksum on compressed data)
                            if enable_compression {
                                if let Err(e) = chunk.compress() {
                                    tracing::warn!("Compression failed for chunk {}: {}", chunk_id, e);
                                }
                            }
                            
                            // Create chunk message
                            let chunk_msg = Message::FileChunk {
                                chunk_id,
                                session_id: session_id.clone(),
                                data: chunk.data,
                                is_last,
                                is_compressed: chunk.is_compressed,
                                checksum: chunk.checksum,
                                original_size: chunk.original_size,
                                sequence_number: chunk_id,
                                retry_count: 0,
                            };
                            
                            // Queue for sending
                            if let Err(e) = chunk_tx.send(chunk_msg).await {
                                tracing::error!("Failed to queue chunk {}: {}", chunk_id, e);
                                return Err(TransferError::NetworkError(format!("Channel closed: {}", e)));
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                Ok::<(), TransferError>(())
            });
            worker_handles.push(handle);
        }
        
        // Read chunks and send to worker pool
        let mut chunk_id = 0u64;
        let start_time = Instant::now();
        let total_chunks = (self.file_size + self.chunk_size as u64 - 1) / self.chunk_size as u64;

        tracing::info!("Starting to read and process {} chunks...", total_chunks);
        
        // Read chunks using mmap reader with checksum calculation (single pass)
        // This avoids redundant checksum calculation in workers
        while let Ok(Some((data, checksum))) = match &mut file_reader {
            FileReader::Mmap(reader) => reader.read_chunk_with_checksum(self.chunk_size),
        } {
            let is_last = chunk_id + 1 >= total_chunks;
            
            // Send work to worker pool with pre-calculated checksum (bounded channel provides backpressure)
            if let Err(e) = work_tx.send((chunk_id, data, is_last, checksum)).await {
                tracing::error!("Failed to send chunk {} to worker pool: {}", chunk_id, e);
                break;
            }
            
            chunk_id += 1;
        }
        
        // Close work channel to signal workers to finish
        drop(work_tx);
        
        // Wait for all workers to complete
        tracing::info!("Waiting for {} chunks to be processed...", chunk_id);
        let mut successful = 0;
        let mut failed = 0;
        for handle in worker_handles {
            match handle.await {
                Ok(Ok(_)) => successful += 1,
                Ok(Err(e)) => {
                    failed += 1;
                    tracing::error!("Worker failed: {}", e);
                }
                Err(e) => {
                    failed += 1;
                    tracing::error!("Worker task panicked: {:?}", e);
                }
            }
        }

        tracing::info!("All {} chunks processed and queued for sending", successful);

        // Send EndOfStream only on the main connection
        // Other connections will naturally close when their channels are dropped
        let eos = Message::EndOfStream {
            session_id: self.session_id.clone(),
            stream_id: 0,
        };
        // Send on the main connection (last one in the list, which is the first connection we kept)
        if let Some(main_tx) = chunk_txs.last() {
            if let Err(e) = main_tx.send(eos.clone()).await {
                tracing::warn!("Failed to send EndOfStream on main connection: {}", e);
            }
        }
        tracing::debug!("EndOfStream queued on all connections, waiting for server ACK...");

        // Calculate timeouts based on file size - larger files need more time
        let file_size_gb = (self.file_size as f64) / (1024.0 * 1024.0 * 1024.0);
        
        // Wait for completion ACK from server - this is the definitive signal
        // Base timeout of 60 seconds, plus 10 seconds per GB
        let timeout_secs = (60.0 + (file_size_gb * 10.0)) as u64;
        let timeout_secs = timeout_secs.min(300); // Cap at 5 minutes for very large files
        let ack_timeout = tokio::time::Duration::from_secs(timeout_secs);
        tracing::debug!("Waiting for completion ACK with {}s timeout (file size: {:.2} GB)", timeout_secs, file_size_gb);
        let ack_received = tokio::time::timeout(ack_timeout, completion_flag.notified()).await.is_ok();

        // Close all channels to signal writer tasks to finish
        drop(chunk_txs);
        tracing::debug!("Closed all writer channels, waiting for writer tasks to finish...");

        // Wait for all writer tasks with a single timeout
        let writer_timeout_secs = (10.0 + (file_size_gb * 5.0)) as u64;
        let writer_timeout_secs = writer_timeout_secs.min(120);
        
        let wait_result = tokio::time::timeout(
            tokio::time::Duration::from_secs(writer_timeout_secs),
            async {
                for (idx, handle) in writer_handles.into_iter().enumerate() {
                    match handle.await {
                        Ok(()) => tracing::debug!("Writer task {} completed", idx),
                        Err(e) => tracing::warn!("Writer task {} panicked: {:?}", idx, e),
                    }
                }
            }
        ).await;

        if wait_result.is_err() {
            tracing::debug!("Writer tasks did not complete within {}s, but transfer succeeded (ACK received)", writer_timeout_secs);
        }

        if !ack_received {
            tracing::error!(
                "Timeout waiting for completion ACK after {}s - transfer may have failed",
                ack_timeout.as_secs()
            );
            return Err(TransferError::NetworkError(
                format!("Timeout waiting for completion ACK after {}s", ack_timeout.as_secs())
            ));
        }

        // Progress bar should already be at 100% from writer tasks, but ensure it's complete
        if let Some(ref pb) = self.progress_bar {
            pb.finish_with_message("Transfer complete!");
        }

        let elapsed = start_time.elapsed();
        let throughput_mbps = (self.file_size as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
        tracing::info!(
            "Transfer completed successfully: {:.2} GB in {:.2}s ({:.1} MB/s)",
            self.file_size as f64 / 1024.0 / 1024.0 / 1024.0,
            elapsed.as_secs_f64(),
            throughput_mbps
        );

        reader_handle.abort();

        tracing::info!("Transfer completed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::FileChunk;
    use crate::error::TransferError;
    use crate::utils::ChunkPool;
    use bytes::Bytes;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    async fn create_test_session(
        file_size: u64,
        chunk_size: usize,
    ) -> (TransferSession, PathBuf, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().to_path_buf();

        let (completion_tx, _completion_rx) = oneshot::channel();
        let (ack_tx, _ack_rx) = mpsc::channel(1000);
        let chunk_pool = Arc::new(ChunkPool::new(64 * 1024, 100));

        let session = TransferSession::new(
            "test-session".to_string(),
            "test.bin".to_string(),
            file_size,
            false,
            chunk_size,
            completion_tx,
            4,
            None,
            ack_tx,
            chunk_pool,
            output_dir.clone(),
        );

        (session, output_dir, temp_dir)
    }

    #[tokio::test]
    async fn test_chunk_write_order() {
        // Test that chunks written out of order still end up in correct positions
        let file_size = 10 * 1024 * 1024; // 10MB
        let chunk_size = 1024 * 1024; // 1MB chunks
        let (mut session, output_dir, _temp_dir) = create_test_session(file_size, chunk_size).await;

        // Create chunks in reverse order
        let mut chunks = Vec::new();
        for i in (0..10).rev() {
            let data = vec![i as u8; chunk_size];
            let mut chunk =
                FileChunk::new(i, "test-session".to_string(), Bytes::from(data), i == 9, i);
            // Calculate checksum for validation
            chunk.original_size = chunk_size;
            chunk.checksum = {
                let mut checksum = crate::simd::SimdChecksum::new();
                checksum.calculate_crc32(&chunk.data[..])
            };
            chunks.push(chunk);
        }

        // Write chunks in reverse order
        for chunk in chunks {
            let result = session.add_chunk(chunk).await;
            assert!(result.is_ok(), "Chunk write should succeed");
        }

        // Verify all chunks received
        assert_eq!(session.received_chunk_ids.len(), 10);

        // Complete transfer
        session.complete_transfer().await.unwrap();

        // Read file and verify data
        let file_path = output_dir.join("test.bin");
        let file_data = std::fs::read(&file_path).unwrap();
        assert_eq!(file_data.len(), file_size as usize);

        // Verify each chunk has correct data
        for i in 0..10 {
            let offset = i * chunk_size;
            let chunk_data = &file_data[offset..offset + chunk_size];
            assert!(
                chunk_data.iter().all(|&b| b == i as u8),
                "Chunk {} should have all bytes equal to {}",
                i,
                i
            );
        }
    }

    #[tokio::test]
    async fn test_chunk_size_validation() {
        let file_size = 5 * 1024 * 1024; // 5MB
        let chunk_size = 1024 * 1024; // 1MB chunks
        let (mut session, _output_dir, _temp_dir) =
            create_test_session(file_size, chunk_size).await;

        // Create chunk with wrong size
        let wrong_data = vec![0u8; chunk_size / 2]; // Half size
        let mut chunk = FileChunk::new(
            0,
            "test-session".to_string(),
            Bytes::from(wrong_data),
            false,
            0,
        );
        chunk.original_size = chunk_size / 2;
        chunk.checksum = {
            let mut checksum = crate::simd::SimdChecksum::new();
            checksum.calculate_crc32(&chunk.data[..])
        };

        let result = session.add_chunk(chunk).await;
        assert!(result.is_err(), "Should reject chunk with wrong size");

        if let Err(TransferError::ChunkValidationFailed { chunk_id }) = result {
            assert_eq!(chunk_id, 0);
        } else {
            panic!("Expected ChunkValidationFailed error");
        }
    }

    #[tokio::test]
    async fn test_last_chunk_size() {
        let file_size = 2 * 1024 * 1024 + 512 * 1024; // 2.5MB = 2,621,440 bytes
        let chunk_size = 1024 * 1024; // 1MB chunks = 1,048,576 bytes
        let (mut session, output_dir, _temp_dir) = create_test_session(file_size, chunk_size).await;

        // Create first chunk (1MB)
        let data1 = vec![1u8; chunk_size];
        let mut chunk1 =
            FileChunk::new(0, "test-session".to_string(), Bytes::from(data1), false, 0);
        chunk1.original_size = chunk_size;
        chunk1.checksum = {
            let mut checksum = crate::simd::SimdChecksum::new();
            checksum.calculate_crc32(&chunk1.data[..])
        };
        session.add_chunk(chunk1).await.unwrap();

        // Create second chunk (1MB) - not last yet
        let data2 = vec![2u8; chunk_size];
        let mut chunk2 =
            FileChunk::new(1, "test-session".to_string(), Bytes::from(data2), false, 1);
        chunk2.original_size = chunk_size;
        chunk2.checksum = {
            let mut checksum = crate::simd::SimdChecksum::new();
            checksum.calculate_crc32(&chunk2.data[..])
        };
        session.add_chunk(chunk2).await.unwrap();

        // Create last chunk (512KB)
        let last_chunk_size = file_size as usize - (2 * chunk_size); // 512KB
        let data3 = vec![3u8; last_chunk_size];
        let mut chunk3 = FileChunk::new(2, "test-session".to_string(), Bytes::from(data3), true, 2);
        chunk3.original_size = last_chunk_size;
        chunk3.checksum = {
            let mut checksum = crate::simd::SimdChecksum::new();
            checksum.calculate_crc32(&chunk3.data[..])
        };
        session.add_chunk(chunk3).await.unwrap();

        // Complete transfer
        session.complete_transfer().await.unwrap();

        // Verify file size
        let file_path = output_dir.join("test.bin");
        let metadata = std::fs::metadata(&file_path).unwrap();
        assert_eq!(metadata.len(), file_size);

        // Verify file content
        let file_data = std::fs::read(&file_path).unwrap();
        assert!(file_data[0..chunk_size].iter().all(|&b| b == 1));
        assert!(file_data[chunk_size..2 * chunk_size]
            .iter()
            .all(|&b| b == 2));
        assert!(file_data[2 * chunk_size..].iter().all(|&b| b == 3));
    }

    #[tokio::test]
    async fn test_concurrent_chunk_writes() {
        // Test that concurrent chunk writes don't corrupt data
        let file_size = 10 * 1024 * 1024;
        let chunk_size = 1024 * 1024;
        let (session, output_dir, _temp_dir) = create_test_session(file_size, chunk_size).await;
        let session = Arc::new(tokio::sync::Mutex::new(session));

        // Spawn tasks to write chunks concurrently
        let mut handles = Vec::new();
        for i in 0..10 {
            let session_clone = Arc::clone(&session);
            let handle = tokio::spawn(async move {
                let data = vec![i as u8; chunk_size];
                let mut chunk =
                    FileChunk::new(i, "test-session".to_string(), Bytes::from(data), i == 9, i);
                chunk.original_size = chunk_size;
                chunk.checksum = {
                    let mut checksum = crate::simd::SimdChecksum::new();
                    checksum.calculate_crc32(&chunk.data[..])
                };

                let mut session = session_clone.lock().await;
                session.add_chunk(chunk).await
            });
            handles.push(handle);
        }

        // Wait for all writes
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        // Complete transfer
        let mut session = session.lock().await;
        session.complete_transfer().await.unwrap();

        // Verify file integrity
        let file_path = output_dir.join("test.bin");
        let file_data = std::fs::read(&file_path).unwrap();
        assert_eq!(file_data.len(), file_size as usize);

        // Verify each chunk
        for i in 0..10 {
            let offset = i * chunk_size;
            let chunk_data = &file_data[offset..offset + chunk_size];
            assert!(chunk_data.iter().all(|&b| b == i as u8));
        }
    }

    #[tokio::test]
    async fn test_file_size_verification() {
        let file_size = 5 * 1024 * 1024;
        let chunk_size = 1024 * 1024;
        let (mut session, output_dir, _temp_dir) = create_test_session(file_size, chunk_size).await;

        // Write all chunks
        for i in 0..5 {
            let data = vec![i as u8; chunk_size];
            let mut chunk =
                FileChunk::new(i, "test-session".to_string(), Bytes::from(data), i == 4, i);
            chunk.original_size = chunk_size;
            chunk.checksum = {
                let mut checksum = crate::simd::SimdChecksum::new();
                checksum.calculate_crc32(&chunk.data[..])
            };
            session.add_chunk(chunk).await.unwrap();
        }

        // Complete transfer - should verify file size
        let result = session.complete_transfer().await;
        assert!(result.is_ok());

        // Verify file exists and has correct size
        let file_path = output_dir.join("test.bin");
        let metadata = std::fs::metadata(&file_path).unwrap();
        assert_eq!(metadata.len(), file_size);
    }
}
