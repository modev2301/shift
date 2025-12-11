//! Optimized file I/O operations with platform-specific optimizations.
//!
//! This module provides high-performance file I/O operations that leverage
//! platform-specific features like O_DIRECT on Linux to bypass the page cache
//! for large file transfers.

use crate::error::TransferError;
use std::fs::File;
use std::path::Path;

/// Disk block size for alignment (typically 512 bytes or 4KB).
/// O_DIRECT requires buffers and offsets to be aligned to this size.
#[cfg(target_os = "linux")]
const DISK_BLOCK_SIZE: usize = 4096;

/// Opens a file for reading with optimized settings.
///
/// For large files (>100MB), this will attempt to use O_DIRECT on Linux
/// to bypass the page cache. On other platforms or if O_DIRECT fails,
/// it falls back to standard file opening.
///
/// # Arguments
///
/// * `path` - Path to the file to open
/// * `_file_size` - Size of the file in bytes (used to determine if O_DIRECT should be used)
///
/// # Returns
///
/// Returns a `File` handle optimized for high-performance reads, or an error
/// if the file cannot be opened.
pub fn open_file_optimized(path: &Path, _file_size: u64) -> Result<File, TransferError> {
    #[cfg(target_os = "linux")]
    {
        const DIRECT_IO_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB
        if _file_size >= DIRECT_IO_THRESHOLD {
        match open_with_odirect(path) {
            Ok(file) => {
                return Ok(file);
            }
            Err(_e) => {
                // Fall back to standard open
            }
        }
        }
    }

    // Fallback to standard file opening
    File::open(path).map_err(|e| TransferError::Io(e))
}

#[cfg(target_os = "linux")]
fn open_with_odirect(path: &Path) -> Result<File, TransferError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::FromRawFd;

    let path_cstr = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| TransferError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid path: {}", e),
        )))?;

    let fd = unsafe {
        libc::open(
            path_cstr.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECT,
        )
    };

    if fd < 0 {
        return Err(TransferError::Io(std::io::Error::last_os_error()));
    }

    // Safety: We own the file descriptor and will close it when File is dropped
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Reads data from a file at a specific offset using the most efficient method available.
///
/// On Unix systems, this uses `pread` for thread-safe, offset-based reads.
/// On Windows, this uses `seek` + `read`.
///
/// # Arguments
///
/// * `file` - File handle to read from
/// * `buffer` - Buffer to read into
/// * `offset` - Offset in the file to read from
///
/// # Returns
///
/// Returns the number of bytes read, or an error if the read fails.
pub fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> Result<usize, TransferError> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let result = unsafe {
            libc::pread(
                fd,
                buffer.as_mut_ptr() as *mut libc::c_void,
                buffer.len(),
                offset as libc::off_t,
            )
        };

        if result < 0 {
            return Err(TransferError::Io(std::io::Error::last_os_error()));
        }

        Ok(result as usize)
    }

    #[cfg(not(unix))]
    {
        let mut file_mut = File::try_clone(file)?;
        file_mut.seek(SeekFrom::Start(offset))?;
        let bytes_read = file_mut.read(buffer)?;
        Ok(bytes_read)
    }
}

/// Aligns a buffer size for O_DIRECT I/O.
///
/// O_DIRECT requires buffer sizes to be aligned to the disk block size.
/// This function rounds up the requested size to the nearest block boundary.
///
/// # Arguments
///
/// * `size` - Requested buffer size
///
/// # Returns
///
/// Returns the aligned buffer size.
#[cfg(target_os = "linux")]
pub fn align_buffer_size(size: usize) -> usize {
    ((size + DISK_BLOCK_SIZE - 1) / DISK_BLOCK_SIZE) * DISK_BLOCK_SIZE
}

#[cfg(not(target_os = "linux"))]
pub fn align_buffer_size(size: usize) -> usize {
    size
}

/// Aligns an offset for O_DIRECT I/O.
///
/// O_DIRECT requires file offsets to be aligned to the disk block size.
/// This function rounds down the offset to the nearest block boundary.
///
/// # Arguments
///
/// * `offset` - Requested file offset
///
/// # Returns
///
/// Returns the aligned offset and the remainder (how much to skip in the buffer).
#[cfg(target_os = "linux")]
pub fn align_offset(offset: u64) -> (u64, usize) {
    let remainder = (offset % DISK_BLOCK_SIZE as u64) as usize;
    let aligned = offset - remainder as u64;
    (aligned, remainder)
}

#[cfg(not(target_os = "linux"))]
pub fn align_offset(offset: u64) -> (u64, usize) {
    (offset, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    #[cfg(target_os = "linux")]
    fn test_align_buffer_size() {
        assert_eq!(align_buffer_size(100), 4096);
        assert_eq!(align_buffer_size(4096), 4096);
        assert_eq!(align_buffer_size(4097), 8192);
        assert_eq!(align_buffer_size(8192), 8192);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_align_buffer_size() {
        // On non-Linux, align_buffer_size is a no-op
        assert_eq!(align_buffer_size(100), 100);
        assert_eq!(align_buffer_size(4096), 4096);
        assert_eq!(align_buffer_size(4097), 4097);
        assert_eq!(align_buffer_size(8192), 8192);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_align_offset() {
        let (aligned, remainder) = align_offset(0);
        assert_eq!(aligned, 0);
        assert_eq!(remainder, 0);

        let (aligned, remainder) = align_offset(100);
        assert_eq!(aligned, 0);
        assert_eq!(remainder, 100);

        let (aligned, remainder) = align_offset(4096);
        assert_eq!(aligned, 4096);
        assert_eq!(remainder, 0);

        let (aligned, remainder) = align_offset(4097);
        assert_eq!(aligned, 4096);
        assert_eq!(remainder, 1);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_align_offset() {
        // On non-Linux, align_offset is a no-op
        let (aligned, remainder) = align_offset(0);
        assert_eq!(aligned, 0);
        assert_eq!(remainder, 0);

        let (aligned, remainder) = align_offset(100);
        assert_eq!(aligned, 100);
        assert_eq!(remainder, 0);

        let (aligned, remainder) = align_offset(4096);
        assert_eq!(aligned, 4096);
        assert_eq!(remainder, 0);

        let (aligned, remainder) = align_offset(4097);
        assert_eq!(aligned, 4097);
        assert_eq!(remainder, 0);
    }

    #[test]
    fn test_read_at() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let test_data = b"Hello, World! This is a test file.";
        temp_file.write_all(test_data).unwrap();
        temp_file.flush().unwrap();

        let file = temp_file.reopen().unwrap();
        let mut buffer = vec![0u8; 13];

        let bytes_read = read_at(&file, &mut buffer, 0).unwrap();
        assert_eq!(bytes_read, 13);
        assert_eq!(&buffer[..bytes_read], b"Hello, World!");

        // Read from offset 7, but only read what's available
        let bytes_read = read_at(&file, &mut buffer, 7).unwrap();
        // File is 35 bytes, so reading from offset 7 gives us 28 bytes max
        // But buffer is only 13 bytes, so we get 13 bytes
        assert_eq!(bytes_read, 13);
        assert_eq!(&buffer[..bytes_read], b"World! This i");
    }

    #[test]
    fn test_open_file_optimized_small() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"small file").unwrap();
        temp_file.flush().unwrap();

        let path = temp_file.path();
        let file = open_file_optimized(path, 10).unwrap();
        assert!(file.metadata().is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_open_file_optimized_large() {
        let mut temp_file = NamedTempFile::new().unwrap();
        // Write enough data to trigger O_DIRECT
        let data = vec![0u8; 1024 * 1024]; // 1MB
        for _ in 0..200 {
            temp_file.write_all(&data).unwrap();
        }
        temp_file.flush().unwrap();

        let path = temp_file.path();
        let file_size = temp_file.as_file().metadata().unwrap().len();
        let file = open_file_optimized(path, file_size).unwrap();
        assert!(file.metadata().is_ok());
    }
}

