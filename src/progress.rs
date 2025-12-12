//! Progress tracking and display for file transfers.

use indicatif::{ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Progress tracker for a file transfer.
pub struct TransferProgress {
    total_bytes: u64,
    transferred: Arc<AtomicU64>,
    pub(crate) start_time: Instant,
    pub(crate) progress_bar: Option<ProgressBar>,
}

impl TransferProgress {
    /// Create a new progress tracker.
    pub fn new(total_bytes: u64, show_bar: bool) -> Self {
        let progress_bar = if show_bar {
            let pb = ProgressBar::new(total_bytes);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{msg:25.25} {bytes:>10}/{total_bytes:>10} {percent:>5}% {bytes_per_sec:>12} {eta:>8}")
                    .unwrap()
                    .progress_chars("█▉▊▋▌▍▎▏ "),
            );
            pb.set_message("Transferring");
            Some(pb)
        } else {
            None
        };

        Self {
            total_bytes,
            transferred: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
            progress_bar,
        }
    }

    /// Get a handle to update progress from another thread.
    pub fn handle(&self) -> ProgressHandle {
        ProgressHandle {
            transferred: Arc::clone(&self.transferred),
            progress_bar: self.progress_bar.as_ref().map(|pb| pb.clone()),
        }
    }

    /// Update progress by the given number of bytes.
    pub fn update(&self, bytes: u64) {
        let prev = self.transferred.fetch_add(bytes, Ordering::Relaxed);
        if let Some(ref pb) = self.progress_bar {
            pb.set_position(prev + bytes);
        }
    }

    /// Get current throughput in MB/s.
    pub fn throughput_mbps(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let bytes = self.transferred.load(Ordering::Relaxed) as f64;
            (bytes / (1024.0 * 1024.0)) / elapsed
        } else {
            0.0
        }
    }

    /// Get current progress percentage.
    pub fn percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        let transferred = self.transferred.load(Ordering::Relaxed) as f64;
        (transferred / self.total_bytes as f64) * 100.0
    }

    /// Finish the progress bar.
    pub fn finish(&self) {
        if let Some(ref pb) = self.progress_bar {
            // Keep the filename in the message
            let msg = pb.message();
            pb.finish_with_message(msg);
        }
    }
}

/// Handle for updating progress from another thread.
#[derive(Clone)]
pub struct ProgressHandle {
    pub(crate) transferred: Arc<AtomicU64>,
    progress_bar: Option<ProgressBar>,
}

impl ProgressHandle {
    /// Update progress by the given number of bytes.
    pub fn update(&self, bytes: u64) {
        let prev = self.transferred.fetch_add(bytes, Ordering::Relaxed);
        if let Some(ref pb) = self.progress_bar {
            pb.set_position(prev + bytes);
        }
    }
}

