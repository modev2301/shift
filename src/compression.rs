//! Compression support using LZ4.
//!
//! Provides compression and decompression for file transfers, with automatic
//! detection of compressible data.

use crate::error::TransferError;
use lz4::Decoder;
use lz4::EncoderBuilder;

/// Compress data using LZ4.
///
/// # Arguments
///
/// * `data` - The data to compress
///
/// # Returns
///
/// Returns the compressed data, or an error if compression fails.
pub fn compress(data: &[u8]) -> Result<Vec<u8>, TransferError> {
    let mut encoder = EncoderBuilder::new()
        .build(Vec::new())
        .map_err(|e| TransferError::ProtocolError(format!("Failed to create LZ4 encoder: {}", e)))?;
    
    std::io::Write::write_all(&mut encoder, data)
        .map_err(|e| TransferError::ProtocolError(format!("Failed to compress data: {}", e)))?;
    
    let (compressed, result) = encoder.finish();
    result.map_err(|e| TransferError::ProtocolError(format!("LZ4 compression error: {}", e)))?;
    
    Ok(compressed)
}

/// Decompress data using LZ4.
///
/// # Arguments
///
/// * `compressed` - The compressed data
/// * `original_size` - Expected size of decompressed data (hint for allocation)
///
/// # Returns
///
/// Returns the decompressed data, or an error if decompression fails.
pub fn decompress(compressed: &[u8], original_size: usize) -> Result<Vec<u8>, TransferError> {
    let mut decoder = Decoder::new(compressed)
        .map_err(|e| TransferError::ProtocolError(format!("Failed to create LZ4 decoder: {}", e)))?;
    
    let mut decompressed = Vec::with_capacity(original_size);
    std::io::copy(&mut decoder, &mut decompressed)
        .map_err(|e| TransferError::ProtocolError(format!("Failed to decompress data: {}", e)))?;
    
    Ok(decompressed)
}

/// Check if data is compressible (heuristic: if compression saves >10%).
///
/// # Arguments
///
/// * `data` - The data to check
///
/// # Returns
///
/// Returns `true` if the data should be compressed (saves at least 10%).
pub fn should_compress(data: &[u8]) -> bool {
    if data.len() < 1024 {
        return false; // Too small to benefit
    }
    
    match compress(data) {
        Ok(compressed) => {
            let ratio = compressed.len() as f64 / data.len() as f64;
            ratio < 0.9 // Only compress if we save at least 10%
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress() {
        // Use repetitive data that compresses well
        let data = vec![0u8; 2048];
        let compressed = compress(&data).unwrap();
        assert!(compressed.len() < data.len());
        
        let decompressed = decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_should_compress() {
        // Compressible data (repetitive)
        let compressible = vec![0u8; 2048];
        assert!(should_compress(&compressible));
        
        // Small data (shouldn't compress)
        let small = vec![0u8; 100];
        assert!(!should_compress(&small));
    }
}
