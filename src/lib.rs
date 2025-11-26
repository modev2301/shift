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
//! use shift::{blocking_transfer::{blocking_server_loop, BlockingTransferConfig}, Config};
//! use std::path::PathBuf;
//!
//! let config = Config::load_or_create(&"config.toml".into())?;
//! let transfer_config = BlockingTransferConfig {
//!     num_threads: 8,
//!     base_port: 8080,
//!     buffer_size: 8 * 1024 * 1024,
//!     enable_compression: false,
//! };
//! let output_dir = PathBuf::from(&config.server.output_directory);
//! blocking_server_loop(&output_dir, transfer_config)?;
//! ```

pub mod benchmark;
pub mod blocking_transfer;
pub mod config;
pub mod error;
pub mod simd;
pub mod utils;

pub use benchmark::{BenchmarkResult, PerformanceBenchmark, TestFile};
pub use blocking_transfer::{BlockingTransferConfig, blocking_client_transfer, blocking_server_loop, blocking_server_receive};
pub use config::Config;
pub use error::TransferError;
pub use simd::{SimdBenchmark, SimdChecksum, SimdOperation, SimdProcessor};

// Re-export commonly used types for convenience
pub use bytes;
pub use serde;
pub use tokio;
