use crate::error::TransferError;
use crate::simd::{SimdChecksum, SimdProcessor};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
// Parallel compression functions use rayon

#[derive(Debug, Clone)]
pub struct FileChunk {
    pub chunk_id: u64,
    pub session_id: String,
    pub data: Bytes,
    pub is_last: bool,
    pub is_compressed: bool,
    pub checksum: u32,
    pub original_size: usize,
    pub sequence_number: u64,
    pub retry_count: u32,
}

impl FileChunk {
    pub fn new(
        chunk_id: u64,
        session_id: String,
        data: Bytes,
        is_last: bool,
        sequence_number: u64,
    ) -> Self {
        let original_size = data.len();
        let is_compressed = false;
        let checksum = 0; // Will be calculated in compress()

        Self {
            chunk_id,
            session_id,
            data,
            is_last,
            is_compressed,
            checksum,
            original_size,
            sequence_number,
            retry_count: 0,
        }
    }

    /// Quick entropy check - returns true if data appears compressible.
    ///
    /// Fast rejection of uncompressible data to avoid CPU waste.
    #[allow(dead_code)]
    fn is_compressible(&self) -> bool {
        if self.data.len() < 4096 {
            return false; // Too small to compress efficiently
        }

        // Fast entropy check: sample first 4KB and check byte distribution
        // Random data has uniform distribution, compressible data has patterns
        let sample_size = std::cmp::min(4096, self.data.len());
        let sample = &self.data[..sample_size];

        // Count byte frequencies
        let mut byte_counts = [0u32; 256];
        for &byte in sample {
            byte_counts[byte as usize] += 1;
        }

        // Calculate simple entropy metric: check if distribution is too uniform
        // For random data, each byte appears ~sample_size/256 times
        // For compressible data, some bytes appear much more frequently
        let expected_count = sample_size as f32 / 256.0;
        let mut variance = 0.0;
        for &count in byte_counts.iter() {
            let diff = count as f32 - expected_count;
            variance += diff * diff;
        }
        variance /= 256.0;

        // Low variance = uniform distribution = random = uncompressible
        // High variance = skewed distribution = patterns = compressible
        // Threshold tuned for fast rejection: random data has variance ~expected_count
        variance > (expected_count * 0.5)
    }

    /// Compress the chunk data using SIMD-optimized LZ4
    pub fn compress(&mut self) -> Result<(), TransferError> {
        if self.is_compressed {
            return Ok(());
        }

        // Skip entropy check - just try compression and use if smaller
        // LZ4 is fast enough that checking isn't worth the memory bandwidth cost
        let data_slice = &self.data[..];
        let compressed_data = SimdProcessor::compress_lz4_simd(data_slice)
            .map_err(|e| TransferError::CompressionError(e))?;

        // Only use compressed data if it's actually smaller (at least 5% smaller to account for overhead)
        let threshold = (self.data.len() as f64 * 0.95) as usize;
        if compressed_data.len() < threshold {
            self.data = Bytes::from(compressed_data);
            self.is_compressed = true;
        }

        // Calculate checksum on final data (compressed or original)
        // If compression was applied, we need checksum on compressed data
        // If not compressed and checksum already set (from read), keep it
        if self.is_compressed || self.checksum == 0 {
            let mut checksum = SimdChecksum::new();
            self.checksum = checksum.calculate_crc32(&self.data[..]);
        }
        // If not compressed and checksum already set, use existing (no recalculation)

        Ok(())
    }

    /// Decompress the chunk data using SIMD-optimized LZ4
    pub fn decompress(&mut self) -> Result<(), TransferError> {
        if !self.is_compressed {
            return Ok(());
        }

        let data_slice = &self.data[..];
        let decompressed_data = SimdProcessor::decompress_lz4_simd(data_slice, self.original_size)
            .map_err(|e| TransferError::CompressionError(e))?;

        self.data = Bytes::from(decompressed_data);
        self.is_compressed = false;

        Ok(())
    }

    /// Validate the chunk using SIMD-optimized checksum verification
    pub fn validate(&self) -> bool {
        SimdProcessor::validate_data_simd(&self.data[..], self.checksum)
    }

    /// Calculate checksum using SIMD optimization
    pub fn calculate_checksum(&self) -> u32 {
        let mut checksum = SimdChecksum::new();
        checksum.calculate_crc32(&self.data[..])
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.original_size == 0 {
            return 1.0;
        }
        self.data.len() as f64 / self.original_size as f64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableResumeInfo {
    pub session_id: String,
    pub file_path: String,
    pub total_chunks: u64,
    pub completed_chunks: Vec<u64>,
    pub file_size: u64,
    pub chunk_size: usize,
    pub checksum: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct ResumeInfo {
    pub session_id: String,
    pub file_path: PathBuf,
    pub total_chunks: u64,
    pub completed_chunks: std::collections::HashSet<u64>,
    pub file_size: u64,
    pub chunk_size: usize,
    pub checksum: String,
    pub timestamp: u64,
}

impl ResumeInfo {
    pub fn new(
        session_id: String,
        file_path: PathBuf,
        total_chunks: u64,
        file_size: u64,
        chunk_size: usize,
    ) -> Self {
        Self {
            session_id,
            file_path,
            total_chunks,
            completed_chunks: std::collections::HashSet::new(),
            file_size,
            chunk_size,
            checksum: String::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    pub fn save(&self, path: &PathBuf) -> Result<(), TransferError> {
        let serializable = self.to_serializable();
        let content = toml::to_string_pretty(&serializable)
            .map_err(|e| TransferError::TomlSerialization(e))?;

        fs::write(path, content).map_err(|e| TransferError::Io(e))?;

        Ok(())
    }

    pub fn load(path: &PathBuf) -> Result<Self, TransferError> {
        let content = fs::read_to_string(path).map_err(|e| TransferError::Io(e))?;

        let serializable: SerializableResumeInfo =
            toml::from_str(&content).map_err(|e| TransferError::TomlDeserialization(e))?;

        Ok(serializable.into())
    }

    pub fn cleanup(&self, path: &PathBuf) -> Result<(), TransferError> {
        if path.exists() {
            fs::remove_file(path).map_err(|e| TransferError::Io(e))?;
        }
        Ok(())
    }

    pub fn to_serializable(&self) -> SerializableResumeInfo {
        SerializableResumeInfo {
            session_id: self.session_id.clone(),
            file_path: self.file_path.to_string_lossy().to_string(),
            total_chunks: self.total_chunks,
            completed_chunks: self.completed_chunks.iter().cloned().collect(),
            file_size: self.file_size,
            chunk_size: self.chunk_size,
            checksum: self.checksum.clone(),
            timestamp: self.timestamp,
        }
    }
}

impl From<SerializableResumeInfo> for ResumeInfo {
    fn from(info: SerializableResumeInfo) -> Self {
        Self {
            session_id: info.session_id,
            file_path: PathBuf::from(info.file_path),
            total_chunks: info.total_chunks,
            completed_chunks: info.completed_chunks.into_iter().collect(),
            file_size: info.file_size,
            chunk_size: info.chunk_size,
            checksum: info.checksum,
            timestamp: info.timestamp,
        }
    }
}

/// Compress multiple chunks in parallel using work-stealing
pub fn compress_chunks_parallel(
    chunks: Vec<(u64, Bytes)>,
    _level: i32,
) -> Vec<(u64, Bytes, u32, bool)> {
    use rayon::prelude::*;
    chunks.into_par_iter()
        .map(|(chunk_id, data)| {
            let original_size = data.len();
            
            // Try compression using SIMD-optimized LZ4
            match SimdProcessor::compress_lz4_simd(&data) {
                Ok(compressed) if compressed.len() < (original_size * 95 / 100) => {
                    // Compression worthwhile (>5% reduction)
                    let mut checksum = SimdChecksum::new();
                    let crc = checksum.calculate_crc32(&compressed);
                    (chunk_id, Bytes::from(compressed), crc, true)
                }
                _ => {
                    // Keep uncompressed
                    let mut checksum = SimdChecksum::new();
                    let crc = checksum.calculate_crc32(&data);
                    (chunk_id, data, crc, false)
                }
            }
        })
        .collect()
}

/// Decompress multiple chunks in parallel
pub fn decompress_chunks_parallel(
    chunks: Vec<(u64, Bytes, usize, bool)>, // (id, data, original_size, is_compressed)
) -> Result<Vec<(u64, Bytes)>, String> {
    use rayon::prelude::*;
    chunks.into_par_iter()
        .map(|(chunk_id, data, original_size, is_compressed)| {
            if is_compressed {
                let decompressed = SimdProcessor::decompress_lz4_simd(&data, original_size)?;
                Ok((chunk_id, Bytes::from(decompressed)))
            } else {
                Ok((chunk_id, data))
            }
        })
        .collect()
}
