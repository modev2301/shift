//! Bandwidth estimation and scale-up triggers for adaptive stream transfer.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Rolling-window bandwidth estimator. Used by the coordinator to decide when to scale up.
pub struct BandwidthEstimator {
    samples: VecDeque<(Instant, u64)>,
    window: Duration,
    pub peak_mbps: f64,
}

impl BandwidthEstimator {
    pub fn new(window_secs: u64) -> Self {
        Self {
            samples: VecDeque::new(),
            window: Duration::from_secs(window_secs),
            peak_mbps: 0.0,
        }
    }

    pub fn record(&mut self, bytes: u64) {
        self.samples.push_back((Instant::now(), bytes));
        self.evict();
        let mbps = self.estimate_mbps();
        if mbps > self.peak_mbps {
            self.peak_mbps = mbps;
        }
    }

    /// Current estimate in MB/s over the window (bytes / (1024*1024) / sec).
    pub fn estimate_mbps(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let total: u64 = self.samples.iter().map(|(_, b)| b).sum();
        let first = self.samples.front().map(|(t, _)| *t).unwrap();
        let last = self.samples.back().map(|(t, _)| *t).unwrap();
        let elapsed_secs = (last - first).as_secs_f64().max(0.001);
        (total as f64) / elapsed_secs / 1_048_576.0
    }

    /// True if current throughput is below 80% of peak — scale-up trigger.
    pub fn is_underperforming(&self) -> bool {
        self.peak_mbps > 0.0 && self.estimate_mbps() < self.peak_mbps * 0.80
    }

    fn evict(&mut self) {
        let cutoff = Instant::now() - self.window;
        while self.samples.front().map_or(false, |(t, _)| *t < cutoff) {
            self.samples.pop_front();
        }
    }
}
