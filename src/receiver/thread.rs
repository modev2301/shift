//! Receiver thread implementation for parallel file reception.
//!
//! Each receiver thread handles a single QUIC stream, receiving data
//! and writing it to the appropriate file offset using thread-safe pwrite.

use crate::error::TransferError;
use crate::TransferConfig;
use quinn::RecvStream;
use std::fs::File;
use std::io::{Seek, Write};
use std::sync::Arc;
use tracing::debug;
#[cfg(target_os = "linux")]
use {libc, std::os::unix::io::AsRawFd};

/// Receiver thread handles a single stream's data reception.
pub struct ReceiverThread;

impl ReceiverThread {
    /// Receive data for the assigned file range.
    ///
    /// # Arguments
    ///
    /// * `recv` - QUIC receive stream
    /// * `file` - File handle to write to
    /// * `config` - Transfer configuration
    ///
    /// # Returns
    ///
    /// Returns the number of bytes received.
    pub async fn receive_range_async(
        mut recv: RecvStream,
        file: Arc<File>,
        config: TransferConfig,
    ) -> Result<u64, TransferError> {
        // Read range header
        let mut start_buf = [0u8; 8];
        recv.read_exact(&mut start_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read start offset: {}", e)))?;
        let start_offset = u64::from_le_bytes(start_buf);

        let mut end_buf = [0u8; 8];
        recv.read_exact(&mut end_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read end offset: {}", e)))?;
        let end_offset = u64::from_le_bytes(end_buf);

        debug!(
            start = start_offset,
            end = end_offset,
            "Receiving file range"
        );

        // Receive and write data
        let mut offset = start_offset;
        let mut buffer = vec![0u8; config.buffer_size.min(1024 * 1024)];
        let mut total_received = 0u64;

        while offset < end_offset {
            let remaining = (end_offset - offset) as usize;
            let read_size = buffer.len().min(remaining);

            let bytes_read = recv.read(&mut buffer[..read_size]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read from stream: {}", e)))?
                .ok_or_else(|| TransferError::ProtocolError("Unexpected end of stream".to_string()))?;

            if bytes_read == 0 {
                break;
            }

            let data = &buffer[..bytes_read];

            // Use pwrite for thread-safe writes (Linux) or seek+write (other platforms)
            #[cfg(target_os = "linux")]
            {
                let fd = file.as_raw_fd();
                let written = unsafe {
                    libc::pwrite(
                        fd,
                        data.as_ptr() as *const libc::c_void,
                        bytes_read,
                        offset as libc::off_t,
                    )
                };

                if written < 0 {
                    return Err(TransferError::Io(std::io::Error::last_os_error()));
                }

                if written as usize != bytes_read {
                    return Err(TransferError::ProtocolError(
                        format!("Partial write: {} != {}", written, bytes_read)
                    ));
                }
            }

            #[cfg(not(target_os = "linux"))]
            {
                let mut file_mut = File::try_clone(&*file)?;
                use std::io::SeekFrom;
                file_mut.seek(SeekFrom::Start(offset))?;
                file_mut.write_all(data)?;
            }

            offset += bytes_read as u64;
            total_received += bytes_read as u64;
        }

        debug!(
            bytes = total_received,
            "File range reception completed"
        );

        Ok(total_received)
    }
}
