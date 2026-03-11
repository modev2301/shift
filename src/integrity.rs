//! End-to-end integrity verification using BLAKE3.
//!
//! Computes and verifies full-file BLAKE3 hashes so the receiver can confirm
//! the transferred file matches the source (critical on lossy WAN).

use crate::error::TransferError;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const HASH_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB

/// BLAKE3 digest size in bytes.
pub const BLAKE3_LEN: usize = 32;

/// Compute BLAKE3 hash of a file by path.
///
/// Uses a normal read (no O_DIRECT) so it works on all platforms and with
/// any alignment. For large files this is a sequential read pass.
pub fn hash_file(path: &Path, file_size: u64) -> Result<[u8; BLAKE3_LEN], TransferError> {
    let mut file = File::open(path).map_err(TransferError::Io)?;
    hash_file_reader(&mut file, file_size)
}

/// Compute BLAKE3 hash by reading from an open file (from current position).
///
/// Reads exactly `size` bytes. Caller must ensure the file is positioned
/// at the start of the region to hash (e.g. seek(0) for full file).
pub fn hash_file_reader(
    file: &mut File,
    size: u64,
) -> Result<[u8; BLAKE3_LEN], TransferError> {
    let mut hasher = blake3::Hasher::new();
    let mut remaining = size as u64;
    let mut buf = vec![0u8; HASH_CHUNK_SIZE.min(remaining as usize).max(1)];

    while remaining > 0 {
        let to_read = buf.len().min(remaining as usize);
        let n = file.read(&mut buf[..to_read]).map_err(TransferError::Io)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }

    if remaining > 0 {
        return Err(TransferError::ProtocolError(format!(
            "File truncated during hash: {} bytes short",
            remaining
        )));
    }

    let hash = hasher.finalize();
    Ok(*hash.as_bytes())
}

/// Compute BLAKE3 hash of a contiguous range of an open file (start..end).
pub fn hash_file_range(file: &mut File, start: u64, end: u64) -> Result<[u8; BLAKE3_LEN], TransferError> {
    let size = end.saturating_sub(start);
    file.seek(SeekFrom::Start(start)).map_err(TransferError::Io)?;
    hash_file_reader(file, size)
}

/// Compute BLAKE3 hash of a file range by path (opens file, used when sender completes a range).
pub fn hash_file_range_path(path: &Path, start: u64, end: u64) -> Result<[u8; BLAKE3_LEN], TransferError> {
    let mut file = File::open(path).map_err(TransferError::Io)?;
    hash_file_range(&mut file, start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_hash_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let mut f = File::create(&path).unwrap();
        f.write_all(b"hello world").unwrap();
        f.sync_all().unwrap();
        drop(f);

        let hash = hash_file(&path, 11).unwrap();
        let expected = blake3::hash(b"hello world");
        assert_eq!(hash, *expected.as_bytes());
    }
}
