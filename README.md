# Shift

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

Shift is a high-performance file transfer system designed for reliable, efficient data movement over TCP (and optionally QUIC). Built with Rust, it uses parallel connections, optimized socket settings, and zero-copy I/O to maximize throughput while guaranteeing data integrity. Cross-platform: Linux, macOS, and Windows.

## Features

- **Parallel TCP connections** — Multiple concurrent connections per transfer for maximum bandwidth
- **End-to-end integrity** — BLAKE3 hash of bytes actually sent; server verifies and replies OK or mismatch (client keeps checkpoint on mismatch)
- **Resume with per-range verification** — SQLite checkpoint (WAL mode) stores completed ranges and per-range BLAKE3 hashes; server re-verifies before accepting data; safe for retry after disconnect
- **Capability negotiation** — Client sends capabilities (compression, encryption, streams, buffer); server responds with negotiated intersection so both sides use compatible settings
- **Smart compression** — Compression disabled automatically for already-compressed types (zip, gz, mp4, jpg, etc.) via `is_file_compressible()`
- **LZ4 compression** — Optional compression for compressible data
- **AES-256-GCM encryption** — Optional authenticated encryption
- **Progress and resume** — Real-time progress, checkpoint/resume with SQLite (crash-safe), automatic retry
- **Transport abstraction** — `Transport` / `Connection` / `Stream` traits; TCP implemented today; QUIC (quinn) available as optional feature with probe + TCP fallback
- **Configurable** — Buffer sizes, stream count, timeouts, socket options via TOML

## Quick Start

### Installation

```bash
cargo build --release
```

Binary: `target/release/shift`.

Optional QUIC support (crates.io quinn):

```bash
cargo build --release --features quic
```

### Usage

**Server** (listens on address/port from `config.toml`, default `0.0.0.0:8080`):

```bash
./target/release/shift server
```

**Transfer files** (SCP-like):

```bash
# Single file
./target/release/shift file.txt user@host:/path/

# Multiple files
./target/release/shift file1.txt file2.txt host:/backup/

# Recursive directory
./target/release/shift -r ./local_dir/ user@host:/remote/path/

# Glob patterns
./target/release/shift *.log host:/logs/
```

## Configuration

Create `config.toml` in the working directory. Example:

```toml
[server]
address = "0.0.0.0"
port = 8080
output_directory = "./downloads"
max_clients = 100
# parallel_streams = 16
# buffer_size = 16777216
enable_compression = false
timeout_seconds = 30

[client]
server_address = "127.0.0.1"
server_port = 8080
# parallel_streams = 16
# buffer_size = 16777216
enable_compression = false
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
compression_level = 1
metrics_enabled = true
```

See the repo for full option descriptions. Resume, progress, timeouts, and retries use sensible defaults.

## Architecture

- **Protocol** — Metadata on base port: capability handshake (fixed-size `Capabilities`), then filename/size/stream count; server replies with negotiated caps and READY; optional per-range hash verification (0x06/0x07) for resume; then parallel data streams; final BLAKE3 hash (0x03) and server reply 0x04 (OK) or 0x05 (mismatch).
- **Integrity** — Sender hasher task over bytes sent (BTreeMap by offset); hash sent after all data; server hasher on received bytes; comparison order and framing in `docs/INTEGRITY_DESIGN.md`.
- **Resume** — SQLite DB per transfer (`.shift_checkpoint`), WAL mode; `transfer` + `range_state` tables; per-range BLAKE3 stored and verified on reconnect; legacy JSON checkpoints migrated on load.
- **Transport** — `Transport` / `Connection` / `Stream` in `src/transport.rs`; TCP implementation included; optional `quic` feature adds quinn-based QUIC and `create_transport(config, force_tcp)` with probe and TCP fallback.

## Implementation notes

- **File I/O** — Unix: `pread`/`pwrite`; Windows: seek-based I/O.
- **Compression** — Only when `enable_compression` and `is_file_compressible(path)`; skips zip, gz, mp4, jpg, etc.
- **Checkpoints** — One SQLite file per file path (WAL); v1 checkpoints cleared on load (full retransfer).

## Testing

```bash
cargo test
```

Tests cover config, compression, encryption, resume (including SQLite save/load), integrity, and transfer logic.

## License

MIT License
