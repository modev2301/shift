//! Sender thread implementation for parallel file transfer.
//!
//! Each sender thread handles a portion of the file, reading from disk
//! using thread-safe pread and sending over a QUIC stream to the receiver.

use crate::base::FileRange;
use crate::error::TransferError;
use crate::TransferConfig;
use quinn::Connection;
use std::fs::File;
use std::io::{Read, Seek};
use std::sync::Arc;
use tracing::debug;
#[cfg(target_os = "linux")]
use {libc, std::os::unix::io::AsRawFd};

/// Sender thread handles a single file range transfer.
pub struct SenderThread;

impl SenderThread {
    /// Transfer the assigned file range over a QUIC stream.
    ///
    /// # Arguments
    ///
    /// * `thread_id` - Thread identifier for logging
    /// * `range` - File range to transfer
    /// * `file` - File handle to read from
    /// * `connection` - QUIC connection
    /// * `config` - Transfer configuration
    ///
    /// # Returns
    ///
    /// Returns the number of bytes transferred.
    pub async fn transfer_range_async(
        thread_id: usize,
        range: FileRange,
        file: Arc<File>,
        connection: Connection,
        config: TransferConfig,
    ) -> Result<u64, TransferError> {
        debug!(
            thread_id,
            start = range.start,
            end = range.end,
            "Transferring file range"
        );

        // Open a new bidirectional stream for this range
        let (mut send, _recv) = connection.open_bi().await
            .map_err(|e| TransferError::NetworkError(format!("Failed to open stream: {}", e)))?;

        // Send range header
        let mut header = Vec::new();
        header.extend_from_slice(&range.start.to_le_bytes());
        header.extend_from_slice(&range.end.to_le_bytes());
        send.write_all(&header).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send range header: {}", e)))?;

        // Read and send file data
        let mut offset = range.start;
        let mut buffer = vec![0u8; config.buffer_size.min(1024 * 1024)];
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
            send.write_all(data).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send data: {}", e)))?;

            offset += bytes_read as u64;
            total_sent += bytes_read as u64;
        }

        // Finish the stream
        send.finish()
            .map_err(|e| TransferError::NetworkError(format!("Failed to finish stream: {:?}", e)))?;

        debug!(
            thread_id,
            bytes = total_sent,
            "File range transfer completed"
        );

        Ok(total_sent)
    }
}
