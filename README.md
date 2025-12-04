# Shift

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

Shift is a high-performance file transfer system designed for reliable, efficient data movement over TCP. Built with Rust, Shift leverages parallel connections, optimized socket settings, and zero-copy I/O to achieve maximum throughput while maintaining data integrity and security. Cross-platform support for Linux, macOS, and Windows.

## Features

- **Parallel TCP Connections**: Multiple concurrent connections per transfer for maximum bandwidth utilization
- **Thread-Safe I/O**: Uses `pread`/`pwrite` for efficient, thread-safe file operations on Unix systems
- **Resume Support**: Track transfer progress and resume interrupted transfers
- **LZ4 Compression**: Automatic compression of compressible data to reduce transfer time
- **AES-256-GCM Encryption**: Optional authenticated encryption for secure transfers
- **Progress Tracking**: Real-time progress bars and throughput logging
- **High Throughput**: Optimized for wire-speed transfers with minimal overhead
- **Configurable**: Flexible configuration for different network conditions and use cases

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

## Configuration

Configuration is specified in TOML format. Create a `config.toml` file in the working directory:

```toml
[server]
address = "0.0.0.0"
port = 8080
output_directory = "./downloads"
max_clients = 10
buffer_size = 8388608
timeout_seconds = 30
enable_compression = false
enable_resume = true

[client]
server_address = "127.0.0.1"
server_port = 8080
parallel_streams = 8
buffer_size = 8388608
timeout_seconds = 30
retry_attempts = 3
retry_delay_ms = 1000
enable_compression = false
enable_resume = true
progress_bar_enabled = true

[security]
auth_token = "shift_default_token"
max_connections_per_ip = 10

[performance]
simd_enabled = true
zero_copy_enabled = true
memory_pool_size = 2000
connection_pool_size = 100
metrics_enabled = true
```

### Configuration Options

**Server Configuration**:
- `port`: Port number to listen on (default: 8080)
- `output_directory`: Directory to write received files (default: "./downloads")
- `max_clients`: Maximum number of concurrent clients (default: 10)
- `buffer_size`: Buffer size for I/O operations in bytes (default: 8MB)
- `timeout_seconds`: Connection timeout in seconds (default: 30)
- `enable_compression`: Enable LZ4 compression (default: false)
- `enable_resume`: Enable resume support (default: true)

**Client Configuration**:
- `server_address`: Server hostname or IP address
- `server_port`: Server port number (default: 8080)
- `parallel_streams`: Number of parallel TCP connections per transfer (default: 8)
- `buffer_size`: Buffer size for I/O operations in bytes (default: 8MB)
- `timeout_seconds`: Transfer timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts on failure (default: 3)
- `retry_delay_ms`: Delay between retries in milliseconds (default: 1000)
- `enable_compression`: Enable LZ4 compression (default: false)
- `enable_resume`: Enable resume support (default: true)
- `progress_bar_enabled`: Show progress bar during transfers (default: true)

## Architecture

Shift uses parallel TCP connections for transport, providing:

**TCP Benefits**:
- Reliable, ordered delivery
- Widely supported and firewall-friendly
- Optimized socket settings (8MB buffers, TCP_NODELAY)
- Thread-safe file I/O with `pread`/`pwrite`

**Client Components**:
- Parallel TCP connection management for file range distribution
- Thread-safe file reading using `pread` (Unix) or seek-based I/O
- Automatic retry and error recovery
- Resume support with checkpoint tracking
- Optional compression and encryption

**Server Components**:
- TCP server accepting multiple concurrent transfers
- Thread-safe file writing using `pwrite` (Unix) or seek-based I/O
- Connection coordination for parallel data reception
- Checkpoint management for resume support

**Protocol**:
- Binary encoding for file metadata (filename, size, ranges)
- Per-range flags for compression and encryption
- Minimal protocol overhead for maximum throughput

## Implementation Details

**File Range Distribution**: Files are split into ranges, with each range transferred over a separate TCP connection. This enables parallel transfer and optimal bandwidth utilization.

**Thread-Safe I/O**: On Unix systems (Linux, macOS), uses `pread` and `pwrite` for thread-safe, offset-based file operations. On Windows, uses seek-based I/O with file cloning for compatibility.

**Resume Support**: Checkpoint files (`.shift_checkpoint`) track completed ranges, allowing transfers to resume from where they left off after interruption.

**Compression**: LZ4 compression is applied automatically to compressible data chunks, reducing transfer time for text and other compressible content.

**Encryption**: Optional AES-256-GCM encryption provides authenticated encryption with thread-specific nonces to prevent nonce reuse.

**Error Handling**: Failed connections are automatically retried. Transfer sessions maintain state to enable resumption after connection failures.

## Testing

Run the test suite:

```bash
cargo test --release
```

Tests cover error handling, configuration parsing, transfer logic, file I/O operations, compression, encryption, and resume functionality.

## Performance

Performance depends on network conditions, file characteristics, and system resources. Key performance factors:

- **Connection Count**: More parallel connections enable better bandwidth utilization
- **Buffer Size**: Larger buffers reduce system call overhead (default: 8MB)
- **Network Conditions**: TCP adapts to network conditions automatically
- **File I/O**: Thread-safe I/O operations enable efficient parallel transfers
- **Compression**: Can significantly reduce transfer time for compressible data

Network transfer performance should be measured end-to-end under actual network conditions. The system is designed to saturate available bandwidth through parallel connections and optimized socket settings.

## License

MIT License
