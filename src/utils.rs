use crate::config::*;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct ChunkPool {
    pub pool: Arc<Mutex<VecDeque<BytesMut>>>,
    pub chunk_size: usize,
    pub max_pool_size: usize,
}

impl ChunkPool {
    pub fn new(chunk_size: usize, max_pool_size: usize) -> Self {
        // Pre-warm pool with 50% of max capacity to avoid cold allocations
        let mut pool = VecDeque::with_capacity(max_pool_size);
        let pre_warm_count = max_pool_size / 2;
        for _ in 0..pre_warm_count {
            pool.push_back(BytesMut::with_capacity(chunk_size));
        }
        Self {
            pool: Arc::new(Mutex::new(pool)),
            chunk_size,
            max_pool_size,
        }
    }

    pub async fn get_buffer(&self) -> BytesMut {
        let mut pool = self.pool.lock().await;

        if let Some(mut buffer) = pool.pop_front() {
            buffer.clear();
            buffer
        } else {
            BytesMut::with_capacity(self.chunk_size)
        }
    }

    pub async fn return_buffer(&self, mut buffer: BytesMut) {
        let mut pool = self.pool.lock().await;

        if pool.len() < self.max_pool_size {
            buffer.clear();
            pool.push_back(buffer);
        }
    }
}

#[derive(Debug)]
pub struct RetryManager {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryManager {
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        }
    }

    pub async fn execute_with_retry<F, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
        E: std::fmt::Debug + Send + 'static,
    {
        let mut last_error = None;
        let mut delay = self.base_delay;

        for attempt in 0..=self.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);

                    if attempt < self.max_retries {
                        tokio::time::sleep(delay).await;
                        delay = std::cmp::min(delay * 2, self.max_delay);
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }
}

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

pub fn format_speed(bytes_per_sec: f64) -> String {
    format!("{}/s", format_bytes(bytes_per_sec as u64))
}

pub fn calculate_eta(bytes_remaining: u64, bytes_per_sec: f64) -> Duration {
    if bytes_per_sec <= 0.0 {
        return Duration::from_secs(0);
    }

    let seconds = bytes_remaining as f64 / bytes_per_sec;
    Duration::from_secs(seconds as u64)
}

pub fn calculate_progress_percentage(bytes_transferred: u64, total_bytes: u64) -> f64 {
    if total_bytes == 0 {
        return 0.0;
    }
    (bytes_transferred as f64 / total_bytes as f64) * 100.0
}

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

pub fn get_optimal_chunk_size_for_file(file_size: u64) -> usize {
    // Adaptive chunk sizing based on file size for optimal performance
    const SMALL_FILE_THRESHOLD: u64 = 10 * 1024 * 1024; // 10MB
    const SMALL_FILE_THRESHOLD_PLUS_ONE: u64 = SMALL_FILE_THRESHOLD + 1;
    const MEDIUM_FILE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB

    match file_size {
        0..=SMALL_FILE_THRESHOLD => 256 * 1024, // 256KB for small files
        SMALL_FILE_THRESHOLD_PLUS_ONE..=MEDIUM_FILE_THRESHOLD => 1024 * 1024, // 1MB for medium files
        _ => 8 * 1024 * 1024, // 8MB for large files (balanced performance and memory)
    }
}

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
        // Small files (<= 10MB) get 256KB chunks
        assert_eq!(get_optimal_chunk_size_for_file(1024), 256 * 1024);
        // Medium files (10MB - 100MB) get 1MB chunks
        assert_eq!(
            get_optimal_chunk_size_for_file(50 * 1024 * 1024),
            1024 * 1024
        );
        // Large files (> 100MB) get 1MB chunks
        assert_eq!(
            get_optimal_chunk_size_for_file(200 * 1024 * 1024),
            1024 * 1024
        );
    }
}
