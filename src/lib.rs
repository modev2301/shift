//! Shift - High-performance file transfer library.
//!
//! Shift is a fast, reliable file transfer system built with Rust.
//! It uses parallel streams, minimal ACKs, and efficient chunking to achieve high throughput.
//!
//! # Features
//!
//! - **High Performance**: Parallel streams, minimal protocol overhead
//! - **Reliable**: Checksum validation, resume support, error recovery
//! - **Efficient**: Large chunks, zero-copy where possible, SIMD optimizations
//! - **Configurable**: Flexible configuration for different use cases
//!
//! # Example
//!
//! ```no_run
//! use shift::{Config, TransferServer};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Arc::new(Config::load_or_create(&"config.toml".into())?);
//! let server = TransferServer::new(Arc::clone(&config.server));
//! server.run().await?;
//! # Ok(())
//! # }
//! ```

pub mod adaptive;
pub mod benchmark;
pub mod blocking_transfer;
pub mod client;
pub mod compression;
pub mod config;
pub mod error;
#[cfg(target_os = "linux")]
pub mod io_uring;
pub mod network;
pub mod server;
pub mod simd;
pub mod utils;
pub mod zero_copy;

pub use benchmark::{BenchmarkResult, PerformanceBenchmark, TestFile};
pub use client::{ClientTransferManager, Message, TransferSession};
pub use compression::FileChunk;
pub use config::Config;
pub use error::TransferError;
pub use network::{ConnectionPool, NetworkStats};
pub use server::TransferServer;
pub use simd::{SimdBenchmark, SimdChecksum, SimdOperation, SimdProcessor};
pub use zero_copy::{DirectIOReader, ZeroCopyBufferPool, ZeroCopyReader, ZeroCopyTransferManager, ZeroCopyWriter};

// Re-export commonly used types for convenience
pub use bytes;
pub use serde;
pub use tokio;
