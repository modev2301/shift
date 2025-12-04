# Shift

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

Shift is a high-performance file transfer system designed for reliable, efficient data movement over QUIC. Built with Rust, Shift leverages QUIC's built-in congestion control, multiplexing, and TLS 1.3 encryption to achieve maximum throughput while maintaining data integrity and security.

## Features

- **QUIC Transport**: Modern protocol with built-in congestion control, multiplexing, and encryption
- **TLS 1.3 Encryption**: End-to-end encryption with automatic certificate management
- **Parallel Streams**: Multiple concurrent streams per connection for maximum bandwidth utilization
- **Thread-Safe I/O**: Uses `pread`/`pwrite` for efficient, thread-safe file operations
- **High Throughput**: Optimized for wire-speed transfers with minimal overhead
- **Configurable**: Flexible configuration for different network conditions and use cases
- **Resume Support**: Track transfer progress and resume interrupted transfers

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
output_directory = "./downloads"
max_clients = 10
buffer_size = 8388608
timeout_seconds = 30

[client]
server_address = "127.0.0.1"
server_port = 8080
parallel_streams = 8
buffer_size = 8388608
timeout_seconds = 30
retry_attempts = 3
retry_delay_ms = 1000

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

**Client Configuration**:
- `server_address`: Server hostname or IP address
- `server_port`: Server port number (default: 8080)
- `parallel_streams`: Number of parallel QUIC streams per transfer (default: 8)
- `buffer_size`: Buffer size for I/O operations in bytes (default: 8MB)
- `timeout_seconds`: Transfer timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts on failure (default: 3)
- `retry_delay_ms`: Delay between retries in milliseconds (default: 1000)

## Architecture

Shift uses QUIC for transport, providing:

**QUIC Benefits**:
- Built-in congestion control (BBR-style)
- Multiplexing without head-of-line blocking
- TLS 1.3 encryption by default
- Connection migration and resilience
- Low latency with 0-RTT handshakes

**Client Components**:
- QUIC endpoint with optimized transport configuration
- Parallel stream management for file range distribution
- Thread-safe file reading using `pread` (Linux) or seek-based I/O
- Automatic retry and error recovery

**Server Components**:
- QUIC server endpoint with self-signed certificates (development)
- Connection handler accepting multiple concurrent transfers
- Thread-safe file writing using `pwrite` (Linux) or seek-based I/O
- Stream coordination for parallel data reception

**Protocol**:
- Binary encoding for file metadata (filename, size, ranges)
- Stream-based data transfer with automatic flow control
- Minimal protocol overhead for maximum throughput

## Implementation Details

**File Range Distribution**: Files are split into ranges, with each range transferred over a separate QUIC stream. This enables parallel transfer and optimal bandwidth utilization.

**Thread-Safe I/O**: On Linux, uses `pread` and `pwrite` for thread-safe, offset-based file operations. On other platforms, uses seek-based I/O with file cloning.

**Error Handling**: Failed streams are automatically retried. Transfer sessions maintain state to enable resumption after connection failures.

**Certificate Management**: Uses self-signed certificates for development. In production, configure proper TLS certificates for secure communication.

## Testing

Run the test suite:

```bash
cargo test --release
```

Tests cover error handling, configuration parsing, transfer logic, and file I/O operations.

## Performance

Performance depends on network conditions, file characteristics, and system resources. Key performance factors:

- **Stream Count**: More parallel streams enable better bandwidth utilization
- **Buffer Size**: Larger buffers reduce system call overhead
- **Network Conditions**: QUIC adapts to network conditions automatically
- **File I/O**: Thread-safe I/O operations enable efficient parallel transfers

Network transfer performance should be measured end-to-end under actual network conditions. The system is designed to saturate available bandwidth through QUIC's efficient protocol design and parallel stream utilization.

## License

MIT License
