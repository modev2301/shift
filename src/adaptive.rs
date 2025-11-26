//! Adaptive transfer parameters based on network conditions

use std::time::Duration;
use std::collections::VecDeque;

/// Tracks network conditions and adapts transfer parameters
pub struct AdaptiveController {
    // RTT tracking (exponential moving average)
    rtt_ema: f64,
    rtt_samples: VecDeque<Duration>,
    
    // Bandwidth tracking
    bandwidth_samples: VecDeque<f64>, // bytes/sec
    bandwidth_ema: f64,
    
    // Current parameters
    current_chunk_size: usize,
    current_connections: usize,
    current_buffer_size: usize,
    
    // Limits
    min_chunk_size: usize,
    max_chunk_size: usize,
    min_connections: usize,
    max_connections: usize,
}

impl AdaptiveController {
    pub fn new() -> Self {
        Self {
            rtt_ema: 0.0,
            rtt_samples: VecDeque::with_capacity(100),
            bandwidth_samples: VecDeque::with_capacity(100),
            bandwidth_ema: 0.0,
            current_chunk_size: 8 * 1024 * 1024, // Start with 8MB
            current_connections: 8,
            current_buffer_size: 25 * 1024 * 1024,
            min_chunk_size: 256 * 1024,      // 256KB min
            max_chunk_size: 64 * 1024 * 1024, // 64MB max
            min_connections: 1,
            max_connections: 32,
        }
    }

    /// Record a completed chunk transfer
    pub fn record_transfer(&mut self, bytes: usize, duration: Duration, rtt: Option<Duration>) {
        // Update bandwidth estimate
        let bandwidth = bytes as f64 / duration.as_secs_f64();
        self.bandwidth_samples.push_back(bandwidth);
        if self.bandwidth_samples.len() > 100 {
            self.bandwidth_samples.pop_front();
        }
        
        // Exponential moving average (alpha = 0.1 for smoothing)
        const ALPHA: f64 = 0.1;
        self.bandwidth_ema = ALPHA * bandwidth + (1.0 - ALPHA) * self.bandwidth_ema;
        
        // Update RTT if provided
        if let Some(rtt) = rtt {
            self.rtt_samples.push_back(rtt);
            if self.rtt_samples.len() > 100 {
                self.rtt_samples.pop_front();
            }
            self.rtt_ema = ALPHA * rtt.as_secs_f64() * 1000.0 + (1.0 - ALPHA) * self.rtt_ema;
        }
        
        // Adapt parameters based on conditions
        self.adapt();
    }

    /// Adapt transfer parameters based on observed conditions
    fn adapt(&mut self) {
        // Calculate bandwidth-delay product (BDP)
        // BDP = bandwidth (bytes/sec) * RTT (sec)
        let rtt_sec = self.rtt_ema / 1000.0;
        let bdp = self.bandwidth_ema * rtt_sec;
        
        // Optimal buffer size is 2-4x BDP
        let optimal_buffer = (bdp * 3.0) as usize;
        self.current_buffer_size = optimal_buffer
            .max(1 * 1024 * 1024)  // Min 1MB
            .min(100 * 1024 * 1024); // Max 100MB
        
        // Chunk size: larger for high bandwidth, smaller for high latency
        if self.bandwidth_ema > 1_000_000_000.0 {
            // > 1GB/s: use larger chunks
            self.current_chunk_size = 32 * 1024 * 1024;
        } else if self.bandwidth_ema > 100_000_000.0 {
            // > 100MB/s: use medium chunks
            self.current_chunk_size = 16 * 1024 * 1024;
        } else if self.bandwidth_ema > 10_000_000.0 {
            // > 10MB/s: use smaller chunks
            self.current_chunk_size = 8 * 1024 * 1024;
        } else {
            // Low bandwidth: minimize overhead
            self.current_chunk_size = 1 * 1024 * 1024;
        }
        
        // Connection count: more connections help high-latency networks
        if self.rtt_ema > 100.0 {
            // High latency (>100ms): use more connections
            self.current_connections = 16;
        } else if self.rtt_ema > 50.0 {
            // Medium latency
            self.current_connections = 12;
        } else if self.rtt_ema > 10.0 {
            // Low latency
            self.current_connections = 8;
        } else {
            // Very low latency (LAN): fewer connections needed
            self.current_connections = 4;
        }
        
        // Clamp to limits
        self.current_chunk_size = self.current_chunk_size
            .max(self.min_chunk_size)
            .min(self.max_chunk_size);
        self.current_connections = self.current_connections
            .max(self.min_connections)
            .min(self.max_connections);
    }

    /// Get recommended chunk size
    pub fn chunk_size(&self) -> usize {
        self.current_chunk_size
    }

    /// Get recommended connection count
    pub fn connections(&self) -> usize {
        self.current_connections
    }

    /// Get recommended buffer size
    pub fn buffer_size(&self) -> usize {
        self.current_buffer_size
    }

    /// Get estimated bandwidth in bytes/sec
    pub fn bandwidth(&self) -> f64 {
        self.bandwidth_ema
    }

    /// Get estimated RTT in milliseconds
    pub fn rtt_ms(&self) -> f64 {
        self.rtt_ema
    }
}

impl Default for AdaptiveController {
    fn default() -> Self {
        Self::new()
    }
}

/// Measure RTT to server
pub async fn measure_rtt(addr: &str) -> Result<Duration, std::io::Error> {
    use tokio::net::TcpStream;
    use tokio::time::Instant;
    
    let start = Instant::now();
    let stream = TcpStream::connect(addr).await?;
    let connect_time = start.elapsed();
    
    // TCP handshake is roughly 1.5 RTT, so:
    // RTT ≈ connect_time / 1.5
    let rtt = connect_time.mul_f64(2.0 / 3.0);
    
    drop(stream);
    Ok(rtt)
}

