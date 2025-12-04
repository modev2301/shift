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
//! use shift::{receiver::Receiver, Config};
//! use std::path::PathBuf;
//!
//! let config = Config::load_or_create(&"config.toml".into())?;
//! let receiver = Receiver::new(config.server.port, 8, &config.server.output_directory)?;
//! receiver.run_forever()?;
//! ```

pub mod base;
pub mod config;
pub mod error;
pub mod progress;
pub mod tcp_server;
pub mod tcp_transfer;
pub mod utils;

pub use base::{FileRange, TransferConfig, TransferStats};
pub use config::Config;
pub use error::TransferError;

// Re-export commonly used types for convenience
pub use bytes;
pub use serde;
pub use tokio;
