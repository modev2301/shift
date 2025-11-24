# Shift

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

Shift is a high-performance file transfer system designed for reliable, efficient data movement over TCP networks. Built with Rust, Shift uses multiple concurrent connections, binary protocol encoding, and SIMD-optimized operations to maximize throughput while maintaining data integrity.

## Features

- **Multi-connection transfers**: Establishes multiple TCP connections per transfer session to utilize available bandwidth
- **Binary protocol**: Compact binary format for chunk data to eliminate JSON serialization overhead
- **SIMD-optimized operations**: CRC32 checksum calculation and LZ4 compression use SIMD instructions where available
- **Selective compression**: Automatically detects compressible data using entropy analysis
- **Transfer resumption**: Tracks received chunks per session to enable resuming interrupted transfers
- **Checksum validation**: Validates data integrity using CRC32 checksums computed over transmitted data
- **Configurable chunking**: Supports configurable chunk sizes with automatic selection based on file size
- **Zero-copy I/O**: Uses memory-mapped files and zero-copy operations where supported

## Quick Start

### Installation

Build from source:

```bash
cargo build --release
```

The resulting binary is located at `target/release/shift`.

### Usage

#### Start the Server

```bash
./target/release/shift server
```

The server listens on the address and port specified in `config.toml` (default: `0.0.0.0:8080`).

#### Transfer Files

Transfer files using SCP-like syntax:

```bash
# Single file
./target/release/shift file.txt user@host:/path/

# Multiple files
./target/release/shift file1.txt file2.txt host:/backup/

# Recursive directory transfer
./target/release/shift -r ./local_dir/ user@host:/remote/path/

# Using glob patterns
./target/release/shift *.log host:/logs/
```

#### Run Benchmarks

```bash
./target/release/shift benchmark --output-dir ./benchmark_results
```

## Configuration

Configuration is specified in TOML format. Create a `config.toml` file in the working directory:

```toml
[server]
address = "0.0.0.0"
port = 8080
num_connections = 8
output_directory = "./downloads"
max_clients = 10
parallel_streams = 8
buffer_size = 8388608
enable_progress_bar = true
enable_compression = false
enable_resume = true
timeout_seconds = 30
max_file_size = 1073741824
allowed_extensions = ["*"]

[client]
server_address = "127.0.0.1"
server_port = 8080
num_connections = 8
parallel_streams = 8
chunk_size = 8388608
buffer_size = 8388608
enable_compression = false
enable_resume = true
timeout_seconds = 30
retry_attempts = 3
retry_delay_ms = 1000
progress_bar_enabled = true
detailed_logging = true

[security]
auth_token = "shift_default_token"
max_connections_per_ip = 10

[performance]
simd_enabled = true
zero_copy_enabled = true
memory_pool_size = 2000
connection_pool_size = 100
compression_level = 1
metrics_enabled = true
```

### Configuration Options

**Server Configuration**:
- `num_connections`: Number of TCP connections to accept per transfer session (default: 8)
- `parallel_streams`: Number of concurrent chunk processing streams (default: 8)
- `buffer_size`: Buffer size for chunk operations in bytes (default: 16MB)
- `enable_compression`: Enable LZ4 compression for compressible data (default: false)
- `enable_resume`: Enable transfer resumption support (default: true)

**Client Configuration**:
- `num_connections`: Number of TCP connections to establish per transfer (default: 8)
- `chunk_size`: Size of file chunks in bytes (default: 16MB for large files)
- `parallel_streams`: Number of concurrent compression/processing streams (default: 8)
- `enable_compression`: Enable compression for compressible data (default: false)

## Architecture

Shift uses an asynchronous I/O model built on Tokio. The architecture consists of:

**Client Components**:
- File reader using zero-copy I/O where supported
- Parallel chunk processing with configurable stream count
- Multi-connection writer that distributes chunks across TCP connections
- Checksum calculation using SIMD-optimized CRC32

**Server Components**:
- Connection handler that accepts multiple connections per session
- Chunk receiver that validates checksums and handles decompression
- Out-of-order chunk writer that writes chunks to disk at correct offsets
- Session management that tracks received chunks and enables resumption

**Protocol**:
- Binary encoding for chunk data (magic byte 0xFF, type byte 0x01)
- JSON encoding for control messages (handshake, acknowledgments)
- Session-based transfer model with unique session identifiers
- Minimal acknowledgment model relying on TCP flow control

## Implementation Details

**Chunk Distribution**: Chunks are assigned to connections using `chunk_id % num_connections` to ensure even distribution across available connections.

**Checksum Validation**: CRC32 checksums are calculated on the data as transmitted (compressed if compression is enabled, original otherwise). Validation occurs before decompression to detect corruption early.

**Compression**: LZ4 compression is applied only when data is determined to be compressible via entropy analysis. Random or binary data is transmitted uncompressed to avoid CPU overhead.

**Error Handling**: Failed chunks are tracked and can be retried. Transfer sessions maintain state to enable resumption after connection failures.

## Testing

Run the test suite:

```bash
cargo test --release
```

Tests cover error handling, configuration parsing, compression logic, checksum validation, and transfer session management.

## Performance

Performance depends on network conditions, file characteristics, and system resources. Key performance factors:

- **Chunk size**: Larger chunks reduce protocol overhead but increase memory usage
- **Connection count**: Multiple connections enable better bandwidth utilization on high-latency networks
- **Compression**: Only beneficial for compressible data; random data should have compression disabled
- **SIMD operations**: CRC32 and compression benefit from SIMD when available

Network transfer performance should be measured end-to-end under actual network conditions. The system is designed to saturate available bandwidth through parallel connections and efficient protocol design.

## License

MIT License


