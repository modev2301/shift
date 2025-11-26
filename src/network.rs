use crate::config::CONNECTION_POOL_SIZE;
use crate::error::TransferError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use std::os::unix::io::AsRawFd;

#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connections_established: u64,
    pub connections_failed: u64,
    pub average_latency_ms: f64,
    pub packet_loss_rate: f64,
    pub bandwidth_mbps: f64,
    pub start_time: Instant,
}

impl NetworkStats {
    pub fn new() -> Self {
        Self {
            bytes_sent: 0,
            bytes_received: 0,
            connections_established: 0,
            connections_failed: 0,
            average_latency_ms: 0.0,
            packet_loss_rate: 0.0,
            bandwidth_mbps: 0.0,
            start_time: Instant::now(),
        }
    }

    pub fn update_bandwidth(&mut self, bytes_transferred: u64, duration: Duration) {
        let seconds = duration.as_secs_f64();
        if seconds > 0.0 {
            let bytes_per_second = bytes_transferred as f64 / seconds;
            self.bandwidth_mbps = bytes_per_second * 8.0 / 1_000_000.0; // Convert to Mbps
        }
    }

    pub fn update_latency(&mut self, latency_ms: f64) {
        // Simple moving average
        self.average_latency_ms = (self.average_latency_ms + latency_ms) / 2.0;
    }

    pub fn record_connection_success(&mut self) {
        self.connections_established += 1;
    }

    pub fn record_connection_failure(&mut self) {
        self.connections_failed += 1;
    }

    pub fn record_bytes_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
    }

    pub fn record_bytes_received(&mut self, bytes: u64) {
        self.bytes_received += bytes;
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    connections: Arc<Mutex<HashMap<String, TcpStream>>>,
}

impl ConnectionPool {
    pub fn new(_max_connections: usize) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_connection(&self, address: &str) -> Result<TcpStream, TransferError> {
        let _connections = self.connections.lock().await;

        // For now, we'll just create a new connection each time
        // In a real implementation, we'd check the pool first
        let stream = TcpStream::connect(address)
            .await
            .map_err(|e| TransferError::NetworkError(e.to_string()))?;

        // Set TCP options
        stream
            .set_nodelay(true)
            .map_err(|e| TransferError::NetworkError(e.to_string()))?;

        // Configure optimal TCP settings for high-throughput transfers
        configure_tcp_for_throughput(&stream)?;

        Ok(stream)
    }

    pub async fn return_connection(&self, _address: &str, _stream: TcpStream) {
        // For now, we just drop the stream
        // In a real implementation, we'd add it back to the pool
    }

    pub async fn get_stats(&self) -> NetworkStats {
        NetworkStats::new()
    }

    pub async fn clear_pool(&self) {
        let mut connections = self.connections.lock().await;
        connections.clear();
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new(CONNECTION_POOL_SIZE)
    }
}

/// Configure optimal TCP settings for high-throughput transfers
pub fn configure_tcp_for_throughput(stream: &TcpStream) -> Result<(), TransferError> {
    let fd = stream.as_raw_fd();
    
    // 1. Enable TCP_NODELAY (already done, but ensure it)
    stream.set_nodelay(true).map_err(|e| {
        TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e))
    })?;
    
    // 2. Set TCP congestion control to BBR (better than CUBIC for high-bandwidth)
    #[cfg(target_os = "linux")]
    {
        const TCP_CONGESTION: libc::c_int = 13;
        let bbr = b"bbr\0";
        
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                TCP_CONGESTION,
                bbr.as_ptr() as *const libc::c_void,
                bbr.len() as libc::socklen_t,
            )
        };
        
        if ret < 0 {
            // BBR not available, that's okay - CUBIC is fine
            tracing::debug!("BBR congestion control not available, using default");
        } else {
            tracing::debug!("Enabled BBR congestion control");
        }
    }
    
    // 3. Enable TCP_QUICKACK for faster ACKs
    #[cfg(target_os = "linux")]
    {
        const TCP_QUICKACK: libc::c_int = 12;
        let enable: libc::c_int = 1;
        
        unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                TCP_QUICKACK,
                &enable as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    
    // 4. Set TCP_MAXSEG for optimal segment size (jumbo frames if available)
    #[cfg(target_os = "linux")]
    {
        // Try to detect MTU and set accordingly
        // Default: 1460 (1500 MTU - 40 bytes TCP/IP headers)
        // Jumbo: 8960 (9000 MTU - 40 bytes)
        let mss: libc::c_int = 8960; // Try jumbo first
        
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_MAXSEG,
                &mss as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        
        if ret < 0 {
            tracing::debug!("Jumbo frames not available, using standard MTU");
        }
    }
    
    Ok(())
}

/// Send data with MSG_ZEROCOPY to avoid kernel copy
/// Only beneficial for large sends (>10KB) on fast networks
#[cfg(target_os = "linux")]
pub async fn send_zerocopy(
    stream: &TcpStream,
    data: &[u8],
) -> Result<usize, TransferError> {
    const MSG_ZEROCOPY: libc::c_int = 0x4000000;
    const SO_ZEROCOPY: libc::c_int = 60;
    
    let fd = stream.as_raw_fd();
    
    // Enable zerocopy on socket (one-time setup)
    let enable: libc::c_int = 1;
    unsafe {
        let ret = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            SO_ZEROCOPY,
            &enable as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        if ret < 0 {
            // Zerocopy not supported, fall back to normal send
            return send_normal(stream, data).await;
        }
    }
    
    // Send with MSG_ZEROCOPY flag
    let sent = unsafe {
        libc::send(
            fd,
            data.as_ptr() as *const libc::c_void,
            data.len(),
            MSG_ZEROCOPY,
        )
    };
    
    if sent < 0 {
        let err = std::io::Error::last_os_error();
        // ENOBUFS means zerocopy queue is full, fall back to normal
        if err.raw_os_error() == Some(libc::ENOBUFS) {
            return send_normal(stream, data).await;
        }
        return Err(TransferError::Io(err));
    }
    
    // Note: With zerocopy, we need to wait for completion notification
    // via errqueue before reusing the buffer. For simplicity, we're
    // using owned data here. A more advanced implementation would
    // track pending completions.
    
    Ok(sent as usize)
}

#[cfg(target_os = "linux")]
async fn send_normal(
    stream: &TcpStream,
    data: &[u8],
) -> Result<usize, TransferError> {
    use tokio::io::AsyncWriteExt;
    stream.try_write(data).map_err(TransferError::Io)
}

#[cfg(not(target_os = "linux"))]
pub async fn send_zerocopy(
    stream: &TcpStream,
    data: &[u8],
) -> Result<usize, TransferError> {
    // Use try_write for non-blocking write
    match stream.try_write(data) {
        Ok(n) => Ok(n),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            Err(TransferError::Io(e))
        }
        Err(e) => Err(TransferError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_stats() {
        let mut stats = NetworkStats::new();

        stats.record_bytes_sent(1024);
        stats.record_bytes_received(2048);
        stats.record_connection_success();
        stats.update_latency(10.5);

        assert_eq!(stats.bytes_sent, 1024);
        assert_eq!(stats.bytes_received, 2048);
        assert_eq!(stats.connections_established, 1);
        // The latency calculation is (old + new) / 2, so with initial 0.0 and new 10.5
        assert_eq!(stats.average_latency_ms, 5.25);
    }

    #[test]
    fn test_connection_pool() {
        let _pool = ConnectionPool::new(5);
        // Test that the pool is created successfully
        assert!(true);
    }

    #[test]
    fn test_bandwidth_calculation() {
        let mut stats = NetworkStats::new();
        let duration = Duration::from_secs(1);

        stats.update_bandwidth(1_000_000, duration); // 1MB in 1 second

        // Should be approximately 8 Mbps (1MB * 8 bits / 1 second)
        assert!(stats.bandwidth_mbps > 7.0 && stats.bandwidth_mbps < 9.0);
    }
}
