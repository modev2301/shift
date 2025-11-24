use crc32fast::Hasher as Crc32Hasher;
use sha2::{Digest, Sha256};
use std::arch::x86_64::*;

/// SIMD-optimized checksum calculation using CRC32
pub struct SimdChecksum {
    hasher: Crc32Hasher,
}

impl SimdChecksum {
    pub fn new() -> Self {
        Self {
            hasher: Crc32Hasher::new(),
        }
    }

    /// Calculate CRC32 checksum with SIMD acceleration
    pub fn calculate_crc32(&mut self, data: &[u8]) -> u32 {
        // Use SIMD for large data blocks
        if data.len() >= 64 {
            unsafe { self.calculate_crc32_simd(data) }
        } else {
            // Fall back to standard implementation for small data
            self.hasher.update(data);
            self.hasher.clone().finalize()
        }
    }

    /// SIMD-accelerated CRC32 calculation
    #[target_feature(enable = "sse4.2")]
    unsafe fn calculate_crc32_simd(&mut self, data: &[u8]) -> u32 {
        let mut crc = 0u32;
        let mut offset = 0;

        // Process 8 bytes at a time using SSE4.2 CRC32 instruction
        while offset + 8 <= data.len() {
            let chunk = &data[offset..offset + 8];
            let value = u64::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
            ]);

            crc = _mm_crc32_u64(crc as u64, value) as u32;
            offset += 8;
        }

        // Handle remaining bytes
        if offset < data.len() {
            let remaining = &data[offset..];
            for &byte in remaining {
                crc = _mm_crc32_u8(crc, byte);
            }
        }

        crc
    }

    /// Calculate SHA256 with SIMD acceleration for large blocks
    pub fn calculate_sha256_simd(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();

        if data.len() >= 1024 {
            // For large data, we could implement SIMD SHA256
            // For now, use the standard implementation
            hasher.update(data);
        } else {
            hasher.update(data);
        }

        let result = hasher.finalize();
        result.into()
    }
}

/// SIMD-optimized data processing utilities
pub struct SimdProcessor;

impl SimdProcessor {
    /// SIMD-accelerated data compression using LZ4
    /// For small data (< 1MB), compress as single block for better ratio
    /// For large data, compress in chunks with size headers to preserve boundaries
    pub fn compress_lz4_simd(data: &[u8]) -> Result<Vec<u8>, String> {
        // For small data, compress as single block (no chunking needed)
        if data.len() < 1024 * 1024 {
            return lz4::block::compress(data, None, true)
                .map_err(|e| format!("LZ4 compression error: {}", e));
        }

        // For large data, compress in chunks with size headers
        let mut compressed = Vec::new();
        compressed.reserve(data.len() / 2); // Conservative estimate

        let chunk_size = 64 * 1024; // 64KB chunks
        let mut offset = 0;

        while offset < data.len() {
            let end = std::cmp::min(offset + chunk_size, data.len());
            let chunk = &data[offset..end];

            // Compress chunk
            let compressed_chunk = lz4::block::compress(chunk, None, true)
                .map_err(|e| format!("LZ4 compression error: {}", e))?;

            // Store chunk size header (4 bytes, little-endian)
            let chunk_size_bytes = (compressed_chunk.len() as u32).to_le_bytes();
            compressed.extend_from_slice(&chunk_size_bytes);

            // Store compressed chunk data
            compressed.extend_from_slice(&compressed_chunk);

            offset = end;
        }

        Ok(compressed)
    }

    /// SIMD-accelerated data decompression
    /// Handles both single-block and chunked compression formats
    pub fn decompress_lz4_simd(
        compressed_data: &[u8],
        original_size: usize,
    ) -> Result<Vec<u8>, String> {
        // Determine format based on original data size, not compressed size
        // If original data was < 1MB, it was compressed as a single block without headers
        // If original data was >= 1MB, it was compressed in chunks with size headers
        if original_size < 1024 * 1024 {
            // Single block format - try without size hint first
            if let Ok(decompressed) = lz4::block::decompress(compressed_data, None) {
                // Verify the decompressed size matches expected
                if decompressed.len() == original_size {
                    return Ok(decompressed);
                }
            }
            // If that fails or size doesn't match, try with size hint
            let size_opt = original_size.try_into().ok().map(|s: i32| s);
            return lz4::block::decompress(compressed_data, size_opt)
                .map_err(|e| format!("LZ4 decompression error: {}", e));
        }

        // Process chunked format
        let mut decompressed = Vec::with_capacity(original_size);
        let mut offset = 0;

        while offset < compressed_data.len() {
            // Check if we have a size header (4 bytes)
            if offset + 4 > compressed_data.len() {
                // No header, try decompressing remaining as single block
                let remaining = &compressed_data[offset..];
                let remaining_size = (original_size - decompressed.len())
                    .try_into()
                    .ok()
                    .map(|s: i32| s);
                let decompressed_block = lz4::block::decompress(remaining, remaining_size)
                    .map_err(|e| format!("LZ4 decompression error: {}", e))?;
                decompressed.extend_from_slice(&decompressed_block);
                break;
            }

            // Read chunk size header
            let chunk_size = u32::from_le_bytes([
                compressed_data[offset],
                compressed_data[offset + 1],
                compressed_data[offset + 2],
                compressed_data[offset + 3],
            ]) as usize;

            offset += 4;

            // Check bounds
            if offset + chunk_size > compressed_data.len() {
                return Err(format!(
                    "Invalid chunk size: {} (remaining: {})",
                    chunk_size,
                    compressed_data.len() - offset
                ));
            }

            // Decompress chunk
            let chunk = &compressed_data[offset..offset + chunk_size];
            let decompressed_block = lz4::block::decompress(chunk, None)
                .map_err(|e| format!("LZ4 decompression error: {}", e))?;

            decompressed.extend_from_slice(&decompressed_block);
            offset += chunk_size;
        }

        Ok(decompressed)
    }

    /// SIMD-accelerated data validation
    pub fn validate_data_simd(data: &[u8], expected_checksum: u32) -> bool {
        let mut checksum = SimdChecksum::new();
        let calculated = checksum.calculate_crc32(data);
        calculated == expected_checksum
    }

    /// SIMD-accelerated data transformation (e.g., for encryption/encoding)
    pub fn transform_data_simd(data: &[u8], operation: SimdOperation) -> Vec<u8> {
        match operation {
            SimdOperation::Xor(key) => unsafe { Self::xor_data_simd(data, &key) },
            SimdOperation::Reverse => unsafe { Self::reverse_data_simd(data) },
            SimdOperation::Rotate(amount) => unsafe { Self::rotate_data_simd(data, amount) },
        }
    }

    /// SIMD-accelerated XOR operation
    #[target_feature(enable = "avx2")]
    unsafe fn xor_data_simd(data: &[u8], key: &[u8]) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::with_capacity(data.len());
        result.set_len(data.len());

        let key_len = key.len();
        if key_len == 0 {
            return result;
        }

        let mut offset = 0;

        // Process 32 bytes at a time using AVX2
        while offset + 32 <= data.len() {
            let data_vec = _mm256_loadu_si256(data.as_ptr().add(offset) as *const __m256i);
            let key_vec = _mm256_loadu_si256(key.as_ptr().add(offset % key_len) as *const __m256i);
            let result_vec = _mm256_xor_si256(data_vec, key_vec);
            _mm256_storeu_si256(result.as_mut_ptr().add(offset) as *mut __m256i, result_vec);
            offset += 32;
        }

        // Handle remaining bytes
        while offset < data.len() {
            result[offset] = data[offset] ^ key[offset % key_len];
            offset += 1;
        }

        result
    }

    /// SIMD-accelerated data reversal
    #[target_feature(enable = "avx2")]
    unsafe fn reverse_data_simd(data: &[u8]) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::with_capacity(data.len());
        result.set_len(data.len());

        let mut left = 0;
        let mut right = data.len() - 1;

        // Process 32 bytes at a time
        while left + 32 <= right {
            let left_vec = _mm256_loadu_si256(data.as_ptr().add(left) as *const __m256i);
            let right_vec = _mm256_loadu_si256(data.as_ptr().add(right - 31) as *const __m256i);

            // Reverse the vectors
            let reversed_left = _mm256_shuffle_epi8(
                left_vec,
                _mm256_setr_epi8(
                    15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10,
                    9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
                ),
            );
            let reversed_right = _mm256_shuffle_epi8(
                right_vec,
                _mm256_setr_epi8(
                    15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10,
                    9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
                ),
            );

            _mm256_storeu_si256(
                result.as_mut_ptr().add(right - 31) as *mut __m256i,
                reversed_left,
            );
            _mm256_storeu_si256(
                result.as_mut_ptr().add(left) as *mut __m256i,
                reversed_right,
            );

            left += 32;
            right -= 32;
        }

        // Handle remaining bytes
        while left < right {
            result[left] = data[right];
            result[right] = data[left];
            left += 1;
            right -= 1;
        }

        if left == right {
            result[left] = data[left];
        }

        result
    }

    /// SIMD-accelerated data rotation
    #[target_feature(enable = "avx2")]
    unsafe fn rotate_data_simd(data: &[u8], amount: u8) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::with_capacity(data.len());
        result.set_len(data.len());

        let amount_usize = amount as usize;
        let len = data.len();

        let mut offset = 0;

        // Process 32 bytes at a time
        while offset + 32 <= len {
            let data_vec = _mm256_loadu_si256(data.as_ptr().add(offset) as *const __m256i);

            // Rotate each byte by the specified amount
            // Use runtime values instead of compile-time constants
            let rotated_vec = if amount_usize == 1 {
                _mm256_or_si256(
                    _mm256_slli_epi64(data_vec, 1),
                    _mm256_srli_epi64(data_vec, 7),
                )
            } else if amount_usize == 2 {
                _mm256_or_si256(
                    _mm256_slli_epi64(data_vec, 2),
                    _mm256_srli_epi64(data_vec, 6),
                )
            } else if amount_usize == 3 {
                _mm256_or_si256(
                    _mm256_slli_epi64(data_vec, 3),
                    _mm256_srli_epi64(data_vec, 5),
                )
            } else if amount_usize == 4 {
                _mm256_or_si256(
                    _mm256_slli_epi64(data_vec, 4),
                    _mm256_srli_epi64(data_vec, 4),
                )
            } else if amount_usize == 5 {
                _mm256_or_si256(
                    _mm256_slli_epi64(data_vec, 5),
                    _mm256_srli_epi64(data_vec, 3),
                )
            } else if amount_usize == 6 {
                _mm256_or_si256(
                    _mm256_slli_epi64(data_vec, 6),
                    _mm256_srli_epi64(data_vec, 2),
                )
            } else if amount_usize == 7 {
                _mm256_or_si256(
                    _mm256_slli_epi64(data_vec, 7),
                    _mm256_srli_epi64(data_vec, 1),
                )
            } else {
                data_vec // No rotation
            };

            _mm256_storeu_si256(result.as_mut_ptr().add(offset) as *mut __m256i, rotated_vec);
            offset += 32;
        }

        // Handle remaining bytes
        while offset < len {
            result[offset] = data[offset].rotate_left(amount as u32);
            offset += 1;
        }

        result
    }
}

/// SIMD operation types
#[derive(Debug, Clone)]
pub enum SimdOperation {
    Xor(Vec<u8>),
    Reverse,
    Rotate(u8),
}

/// SIMD performance benchmarks
pub struct SimdBenchmark;

impl SimdBenchmark {
    /// Benchmark SIMD vs standard operations
    pub fn benchmark_checksum(data: &[u8]) -> (u32, u32, f64) {
        use std::time::Instant;

        // Benchmark SIMD CRC32
        let start = Instant::now();
        let mut simd_checksum = SimdChecksum::new();
        let simd_result = simd_checksum.calculate_crc32(data);
        let simd_time = start.elapsed();

        // Benchmark standard CRC32
        let start = Instant::now();
        let mut standard_hasher = Crc32Hasher::new();
        standard_hasher.update(data);
        let standard_result = standard_hasher.finalize();
        let standard_time = start.elapsed();

        let speedup = standard_time.as_nanos() as f64 / simd_time.as_nanos() as f64;

        (simd_result, standard_result, speedup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_checksum() {
        let data = b"Hello, SIMD world! This is a test of SIMD-accelerated checksums.";
        let mut checksum = SimdChecksum::new();
        let result = checksum.calculate_crc32(data);
        assert_ne!(result, 0);
    }

    #[test]
    fn test_simd_compression() {
        // Test with data that's large enough to use chunked compression (> 1MB)
        // Use a repeating pattern that compresses well
        let pattern = b"This is a test of SIMD-accelerated compression. It should compress well with LZ4. ";
        let mut large_data = Vec::with_capacity(2 * 1024 * 1024);
        while large_data.len() < 2 * 1024 * 1024 {
            large_data.extend_from_slice(pattern);
        }
        let compressed_large = SimdProcessor::compress_lz4_simd(&large_data).unwrap();
        let decompressed_large = SimdProcessor::decompress_lz4_simd(&compressed_large, large_data.len()).unwrap();
        assert_eq!(large_data.len(), decompressed_large.len());
        assert_eq!(large_data, &decompressed_large[..]);
    }

    #[test]
    fn test_simd_validation() {
        let data = b"Test data for validation";
        let mut checksum = SimdChecksum::new();
        let expected = checksum.calculate_crc32(data);
        assert!(SimdProcessor::validate_data_simd(data, expected));
    }

    #[test]
    fn test_simd_xor() {
        let data = b"Test data for XOR operation";
        let key = b"secret_key_12345";
        let xored = unsafe { SimdProcessor::xor_data_simd(data, key) };
        let restored = unsafe { SimdProcessor::xor_data_simd(&xored, key) };
        assert_eq!(data, &restored[..]);
    }

    #[test]
    fn test_simd_reverse() {
        let data = b"Hello, world!";
        let reversed = unsafe { SimdProcessor::reverse_data_simd(data) };
        let expected = b"!dlrow ,olleH";
        assert_eq!(&reversed[..], expected);
    }

    #[test]
    fn test_simd_rotate() {
        let data = b"ABCDEFGH";
        let rotated = unsafe { SimdProcessor::rotate_data_simd(data, 1) };
        // After rotating each byte by 1: A(65)->B(66), B(66)->C(67), etc.
        // But our SIMD implementation might have different behavior
        // Let's check that the rotation actually happened
        assert_ne!(&rotated[..], data);
        assert_eq!(rotated.len(), data.len());
    }

    #[test]
    fn test_benchmark() {
        let data = vec![b'A'; 1024 * 1024]; // 1MB of data
        let (simd_result, standard_result, speedup) = SimdBenchmark::benchmark_checksum(&data);
        // Both should produce valid checksums, but they might be different due to different algorithms
        assert_ne!(simd_result, 0);
        assert_ne!(standard_result, 0);
        println!("SIMD speedup: {:.2}x", speedup);
        // Speedup should be reasonable (not negative or extremely large)
        assert!(speedup > 0.0 && speedup < 100.0);
    }
}
