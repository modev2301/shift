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
//! use shift::{tcp_server::TcpServer, Config, TransferConfig};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::load_or_create(&"config.toml".into())?;
//!     let transfer_config = TransferConfig {
//!         start_port: config.server.port,
//!         num_streams: 16,
//!         max_streams: 16,
//!         buffer_size: 16 * 1024 * 1024,
//!         socket_send_buffer_size: Some(16 * 1024 * 1024),
//!         socket_recv_buffer_size: Some(16 * 1024 * 1024),
//!         enable_compression: false,
//!         enable_encryption: false,
//!         encryption_key: None,
//!         timeout_seconds: 30,
//!     };
//!     let server = TcpServer::new(
//!         config.server.port,
//!         config.server.output_directory.into(),
//!         transfer_config,
//!     );
//!     server.run_forever().await?;
//!     Ok(())
//! }
//! ```

pub mod base;
pub mod compression;
pub mod config;
pub mod quic_server;
pub mod quinn_transport;
pub mod transport;
pub mod encryption;
pub mod error;
pub mod file_io;
pub mod integrity;
pub mod metrics;
pub mod progress;
pub mod range_queue;
pub mod resume;
pub mod tcp_server;
pub mod tcp_transfer;
pub mod utils;

pub use base::{FileRange, TransferConfig, TransferReport, TransferStats};
pub use config::Config;
pub use error::TransferError;
pub use transport::{create_transport, Connection, Listener, Platform, Stream, Transport, TcpTransport};
pub use quinn_transport::QuicTransport;

// Re-export commonly used types for convenience
pub use bytes;
pub use serde;
pub use tokio;
