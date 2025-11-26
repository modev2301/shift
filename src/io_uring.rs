//! io_uring-based I/O for maximum throughput on Linux 5.1+
//! 
//! io_uring eliminates syscall overhead by using shared memory rings
//! between userspace and kernel. This can improve throughput by 20-30%
//! for I/O-bound workloads.

#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use crate::error::TransferError;
use bytes::Bytes;

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};

/// Number of submission queue entries - higher = more concurrent I/O
const RING_SIZE: u32 = 256;

/// io_uring-based file reader for zero-syscall reads
#[cfg(target_os = "linux")]
pub struct UringReader {
    ring: IoUring,
    file: File,
    fd: RawFd,
    file_size: u64,
    position: u64,
    // Pre-allocated buffers for registered I/O
    buffers: Vec<Vec<u8>>,
    buffer_size: usize,
}

#[cfg(target_os = "linux")]
impl UringReader {
    pub fn new<P: AsRef<Path>>(path: P, buffer_size: usize) -> Result<Self, TransferError> {
        let file = File::open(path.as_ref()).map_err(TransferError::Io)?;
        let file_size = file.metadata().map_err(TransferError::Io)?.len();
        let fd = file.as_raw_fd();

        // Create io_uring instance with SQPOLL for kernel-side polling
        // This eliminates the need for syscalls to submit I/O
        let ring = IoUring::builder()
            .setup_sqpoll(1000) // Kernel polls for 1000ms before sleeping
            .setup_sqpoll_cpu(0) // Pin to CPU 0
            .build(RING_SIZE)
            .map_err(|e| TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create io_uring: {}", e)
            )))?;

        // Pre-allocate buffers for registered I/O (avoids per-read allocation)
        let num_buffers = RING_SIZE as usize;
        let buffers: Vec<Vec<u8>> = (0..num_buffers)
            .map(|_| vec![0u8; buffer_size])
            .collect();

        Ok(Self {
            ring,
            file,
            fd,
            file_size,
            position: 0,
            buffers,
            buffer_size,
        })
    }

    /// Submit multiple read requests and wait for completion
    /// Returns a batch of chunks for maximum throughput
    pub fn read_batch(&mut self, chunk_size: usize, batch_size: usize) -> Result<Vec<Bytes>, TransferError> {
        let mut results = Vec::with_capacity(batch_size);
        let mut submissions = 0;

        // Submit batch of read operations
        for i in 0..batch_size {
            if self.position >= self.file_size {
                break;
            }

            let remaining = (self.file_size - self.position) as usize;
            let read_size = std::cmp::min(chunk_size, remaining);
            let buffer_idx = i % self.buffers.len();

            // Create read operation
            let read_op = opcode::Read::new(
                types::Fd(self.fd),
                self.buffers[buffer_idx].as_mut_ptr(),
                read_size as u32,
            )
            .offset(self.position)
            .build()
            .user_data(i as u64);

            // Submit to ring
            unsafe {
                self.ring.submission()
                    .push(&read_op)
                    .map_err(|e| TransferError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to submit read: {:?}", e)
                    )))?;
            }

            self.position += read_size as u64;
            submissions += 1;
        }

        if submissions == 0 {
            return Ok(results);
        }

        // Submit all operations at once (single syscall for entire batch)
        self.ring.submit_and_wait(submissions)
            .map_err(|e| TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("io_uring submit failed: {}", e)
            )))?;

        // Collect results
        let cq = self.ring.completion();
        for cqe in cq {
            let idx = cqe.user_data() as usize;
            let bytes_read = cqe.result();
            
            if bytes_read < 0 {
                return Err(TransferError::Io(std::io::Error::from_raw_os_error(-bytes_read)));
            }

            let data = &self.buffers[idx % self.buffers.len()][..bytes_read as usize];
            results.push(Bytes::copy_from_slice(data));
        }

        Ok(results)
    }

    /// Read a single chunk (convenience method)
    pub fn read_chunk(&mut self, chunk_size: usize) -> Result<Option<Bytes>, TransferError> {
        if self.position >= self.file_size {
            return Ok(None);
        }

        let batch = self.read_batch(chunk_size, 1)?;
        Ok(batch.into_iter().next())
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

/// io_uring-based file writer for zero-syscall writes
#[cfg(target_os = "linux")]
pub struct UringWriter {
    ring: IoUring,
    file: File,
    fd: RawFd,
    file_size: u64,
    // Pending writes that haven't been confirmed yet
    pending_writes: usize,
}

#[cfg(target_os = "linux")]
impl UringWriter {
    pub fn new<P: AsRef<Path>>(path: P, file_size: u64) -> Result<Self, TransferError> {
        let file = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())
            .map_err(TransferError::Io)?;

        // Pre-allocate file
        file.set_len(file_size).map_err(TransferError::Io)?;
        
        let fd = file.as_raw_fd();

        let ring = IoUring::builder()
            .setup_sqpoll(1000)
            .build(RING_SIZE)
            .map_err(|e| TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create io_uring: {}", e)
            )))?;

        Ok(Self {
            ring,
            file,
            fd,
            file_size,
            pending_writes: 0,
        })
    }

    /// Write chunk at specific offset (parallel writes supported)
    pub fn write_at(&mut self, data: &[u8], offset: u64) -> Result<(), TransferError> {
        let write_op = opcode::Write::new(
            types::Fd(self.fd),
            data.as_ptr(),
            data.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(offset);

        unsafe {
            self.ring.submission()
                .push(&write_op)
                .map_err(|e| TransferError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to submit write: {:?}", e)
                )))?;
        }

        self.pending_writes += 1;

        // Batch submissions - only actually submit when queue is getting full
        if self.pending_writes >= (RING_SIZE as usize / 2) {
            self.flush_pending()?;
        }

        Ok(())
    }

    /// Flush all pending writes
    pub fn flush_pending(&mut self) -> Result<(), TransferError> {
        if self.pending_writes == 0 {
            return Ok(());
        }

        self.ring.submit_and_wait(self.pending_writes)
            .map_err(|e| TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("io_uring submit failed: {}", e)
            )))?;

        // Drain completion queue
        let cq = self.ring.completion();
        for cqe in cq {
            if cqe.result() < 0 {
                return Err(TransferError::Io(
                    std::io::Error::from_raw_os_error(-cqe.result())
                ));
            }
        }

        self.pending_writes = 0;
        Ok(())
    }

    /// Sync file to disk
    pub fn sync(&mut self) -> Result<(), TransferError> {
        self.flush_pending()?;

        let fsync_op = opcode::Fsync::new(types::Fd(self.fd))
            .build()
            .user_data(0);

        unsafe {
            self.ring.submission()
                .push(&fsync_op)
                .map_err(|e| TransferError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to submit fsync: {:?}", e)
                )))?;
        }

        self.ring.submit_and_wait(1)
            .map_err(|e| TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("fsync failed: {}", e)
            )))?;

        Ok(())
    }
}

/// Feature detection for io_uring support
#[cfg(target_os = "linux")]
pub fn io_uring_available() -> bool {
    // Check kernel version >= 5.1
    if let Ok(uname) = nix::sys::utsname::uname() {
        let release = uname.release().to_string_lossy();
        if let Some(version_str) = release.split('.').take(2).collect::<Vec<_>>().join(".").parse::<f32>().ok() {
            if version_str >= 5.1 {
                // Try to create a ring to verify
                return IoUring::new(8).is_ok();
            }
        }
    }
    
    // Fallback: try to create a ring
    IoUring::new(8).is_ok()
}

#[cfg(not(target_os = "linux"))]
pub fn io_uring_available() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_io_uring_detection() {
        let available = io_uring_available();
        println!("io_uring available: {}", available);
        // Test should pass regardless of availability
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_uring_reader() {
        if !io_uring_available() {
            println!("io_uring not available, skipping test");
            return;
        }

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        
        // Create test file
        let data = vec![0xABu8; 1024 * 1024]; // 1MB
        std::fs::write(&path, &data).unwrap();

        let mut reader = UringReader::new(&path, 64 * 1024).unwrap();
        let chunks = reader.read_batch(64 * 1024, 16).unwrap();
        
        assert!(!chunks.is_empty());
        let total_bytes: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total_bytes, 1024 * 1024);
    }
}

