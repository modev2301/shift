use crate::config::DEFAULT_PARALLEL_STREAMS;

/// Format bytes as human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}

/// Format speed as human-readable string
pub fn format_speed(bytes_per_sec: f64) -> String {
    format!("{}/s", format_bytes(bytes_per_sec as u64))
}

/// Calculate estimated time to completion
pub fn calculate_eta(bytes_remaining: u64, bytes_per_sec: f64) -> std::time::Duration {
    if bytes_per_sec <= 0.0 {
        return std::time::Duration::from_secs(0);
    }

    let seconds = bytes_remaining as f64 / bytes_per_sec;
    std::time::Duration::from_secs(seconds as u64)
}

/// Calculate progress percentage
pub fn calculate_progress_percentage(bytes_transferred: u64, total_bytes: u64) -> f64 {
    if total_bytes == 0 {
        return 0.0;
    }
    (bytes_transferred as f64 / total_bytes as f64) * 100.0
}

/// Check if file type is compressible
pub fn is_file_compressible(file_path: &std::path::Path) -> bool {
    if let Some(extension) = file_path.extension() {
        let ext = extension.to_string_lossy().to_lowercase();
        // Files that are already compressed
        !matches!(
            ext.as_str(),
            "zip"
                | "gz"
                | "bz2"
                | "xz"
                | "7z"
                | "rar"
                | "tar"
                | "mp3"
                | "mp4"
                | "avi"
                | "mkv"
                | "jpg"
                | "jpeg"
                | "png"
                | "gif"
                | "pdf"
        )
    } else {
        true
    }
}

/// Get optimal chunk size for file based on size
pub fn get_optimal_chunk_size_for_file(file_size: u64) -> usize {
    const SMALL_FILE_THRESHOLD: u64 = 10 * 1024 * 1024; // 10MB
    const MEDIUM_FILE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB
    const SMALL_FILE_THRESHOLD_PLUS_ONE: u64 = SMALL_FILE_THRESHOLD + 1;

    match file_size {
        0..=SMALL_FILE_THRESHOLD => 256 * 1024, // 256KB for small files
        SMALL_FILE_THRESHOLD_PLUS_ONE..=MEDIUM_FILE_THRESHOLD => 1024 * 1024, // 1MB for medium files
        _ => 16 * 1024 * 1024, // 16MB for large files
    }
}

/// Calculate optimal number of parallel streams based on file size.
///
/// Automatically determines the best number of parallel connections to maximize
/// throughput. Uses file size as the primary factor, with reasonable bounds.
///
/// # Arguments
///
/// * `file_size` - Size of the file in bytes
/// * `estimated_latency_ms` - Optional network latency in milliseconds for high-latency adjustments
///
/// # Returns
///
/// Returns the optimal number of parallel streams, bounded between 4 and 32.
pub fn calculate_optimal_parallel_streams(file_size: u64, estimated_latency_ms: Option<u64>) -> usize {
    // Base calculation: 1 stream per 200MB for large files
    // For small files (< 100MB), use fewer streams to avoid overhead
    let size_based_streams = if file_size < 100 * 1024 * 1024 {
        // Small files: 4-8 streams
        std::cmp::max(4, (file_size / (25 * 1024 * 1024)).max(1) as usize)
    } else if file_size < 1024 * 1024 * 1024 {
        // Medium files (100MB - 1GB): 8-16 streams
        std::cmp::max(8, std::cmp::min(16, (file_size / (100 * 1024 * 1024)).max(1) as usize))
    } else {
        // Large files (> 1GB): 16-32 streams
        std::cmp::max(16, std::cmp::min(32, (file_size / (200 * 1024 * 1024)).max(1) as usize))
    };
    
    // Adjust for high latency: more streams help fill the pipe
    let latency_adjusted = if let Some(latency_ms) = estimated_latency_ms {
        if latency_ms > 50 {
            // High latency (>50ms): increase streams to fill bandwidth-delay product
            let bdp_multiplier = (latency_ms as f64 / 50.0).min(2.0);
            ((size_based_streams as f64) * bdp_multiplier).ceil() as usize
        } else {
            size_based_streams
        }
    } else {
        size_based_streams
    };
    
    // Bound between 4 and 32 streams
    std::cmp::max(4, std::cmp::min(32, latency_adjusted))
}

/// Calculate optimal buffer size based on file size and number of streams.
///
/// Automatically determines the best buffer size to balance memory usage
/// and performance. Larger buffers improve throughput but increase memory usage.
///
/// # Arguments
///
/// * `file_size` - Size of the file in bytes
/// * `num_streams` - Number of parallel streams being used
///
/// # Returns
///
/// Returns the optimal buffer size in bytes, bounded between 1MB and 32MB.
pub fn calculate_optimal_buffer_size(file_size: u64, num_streams: usize) -> usize {
    // Base calculation: scale buffer size with file size
    // Small files (< 100MB): 4-8MB buffers
    // Medium files (100MB - 1GB): 8-16MB buffers
    // Large files (> 1GB): 16-32MB buffers
    
    let base_buffer = if file_size < 100 * 1024 * 1024 {
        // Small files: 4-8MB
        std::cmp::max(4 * 1024 * 1024, std::cmp::min(8 * 1024 * 1024, 
            (file_size / 25).max(4 * 1024 * 1024) as usize))
    } else if file_size < 1024 * 1024 * 1024 {
        // Medium files: 8-16MB
        std::cmp::max(8 * 1024 * 1024, std::cmp::min(16 * 1024 * 1024,
            (file_size / 12).max(8 * 1024 * 1024) as usize))
    } else {
        // Large files: 16-32MB
        std::cmp::max(16 * 1024 * 1024, std::cmp::min(32 * 1024 * 1024,
            (file_size / 8).max(16 * 1024 * 1024) as usize))
    };
    
    // Adjust for number of streams: more streams = slightly smaller buffers per stream
    // to keep total memory usage reasonable
    let stream_adjusted = if num_streams > 16 {
        // Many streams: cap buffer size to avoid excessive memory
        std::cmp::min(base_buffer, 16 * 1024 * 1024)
    } else if num_streams > 8 {
        // Moderate streams: slightly reduce buffer
        std::cmp::min(base_buffer, 24 * 1024 * 1024)
    } else {
        base_buffer
    };
    
    // Round to nearest MB for alignment
    let mb_aligned = ((stream_adjusted + 512 * 1024) / (1024 * 1024)) * (1024 * 1024);
    
    // Bound between 1MB and 32MB
    std::cmp::max(1024 * 1024, std::cmp::min(32 * 1024 * 1024, mb_aligned))
}

/// Calculate optimal number of parallel streams (legacy function for compatibility).
#[deprecated(note = "Use calculate_optimal_parallel_streams instead")]
pub fn calculate_parallel_streams(file_size: u64, network_bandwidth_mbps: f64) -> usize {
    let optimal_streams = (network_bandwidth_mbps / 100.0).ceil() as usize;
    let size_based_streams = (file_size / (100 * 1024 * 1024)).max(1) as usize;

    std::cmp::min(
        std::cmp::max(optimal_streams, size_based_streams),
        DEFAULT_PARALLEL_STREAMS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(1024.0), "1.00 KB/s");
        assert_eq!(format_speed(1024.0 * 1024.0), "1.00 MB/s");
    }

    #[test]
    fn test_calculate_progress_percentage() {
        assert_eq!(calculate_progress_percentage(50, 100), 50.0);
        assert_eq!(calculate_progress_percentage(0, 100), 0.0);
        assert_eq!(calculate_progress_percentage(100, 100), 100.0);
    }

    #[test]
    fn test_is_file_compressible() {
        use std::path::Path;

        assert!(is_file_compressible(Path::new("test.txt")));
        assert!(!is_file_compressible(Path::new("test.zip")));
        assert!(!is_file_compressible(Path::new("test.mp4")));
    }

    #[test]
    fn test_get_optimal_chunk_size_for_file() {
        assert_eq!(get_optimal_chunk_size_for_file(1024), 256 * 1024);
        assert_eq!(
            get_optimal_chunk_size_for_file(50 * 1024 * 1024),
            1024 * 1024
        );
        assert_eq!(
            get_optimal_chunk_size_for_file(200 * 1024 * 1024),
            16 * 1024 * 1024
        );
    }

    #[test]
    fn test_calculate_optimal_parallel_streams() {
        // Small files should use 4-8 streams
        assert!(calculate_optimal_parallel_streams(10 * 1024 * 1024, None) >= 4);
        assert!(calculate_optimal_parallel_streams(10 * 1024 * 1024, None) <= 8);
        
        // Medium files should use 8-16 streams
        assert!(calculate_optimal_parallel_streams(500 * 1024 * 1024, None) >= 8);
        assert!(calculate_optimal_parallel_streams(500 * 1024 * 1024, None) <= 16);
        
        // Large files should use 16-32 streams
        assert!(calculate_optimal_parallel_streams(3 * 1024 * 1024 * 1024, None) >= 16);
        assert!(calculate_optimal_parallel_streams(3 * 1024 * 1024 * 1024, None) <= 32);
        
        // Very large files should cap at 32
        assert_eq!(calculate_optimal_parallel_streams(100 * 1024 * 1024 * 1024, None), 32);
        
        // Very small files should have minimum of 4
        assert!(calculate_optimal_parallel_streams(1024, None) >= 4);
        
        // High latency should increase streams
        let low_latency = calculate_optimal_parallel_streams(500 * 1024 * 1024, Some(10));
        let high_latency = calculate_optimal_parallel_streams(500 * 1024 * 1024, Some(100));
        assert!(high_latency >= low_latency);
    }
}
