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
        _ => 8 * 1024 * 1024, // 8MB for large files
    }
}

/// Calculate optimal number of parallel streams
pub fn calculate_parallel_streams(file_size: u64, network_bandwidth_mbps: f64) -> usize {
    let optimal_streams = (network_bandwidth_mbps / 100.0).ceil() as usize;
    let size_based_streams = (file_size / (100 * 1024 * 1024)).max(1) as usize; // 1 stream per 100MB

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
            8 * 1024 * 1024
        );
    }
}
