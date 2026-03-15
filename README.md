# Shift

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

Shift is a high-performance file transfer system for reliable, efficient data movement over TCP and QUIC. Built with Rust, it uses parallel streams, adaptive stream scaling, and BLAKE3 integrity checks. Cross-platform: Linux, macOS, and Windows.

## Features

- **Dual transport** — QUIC (default) with TCP fallback; force TCP with `--tcp`. Same protocol on both.
- **Adaptive streams** — Range queue + coordinator: scale streams up on stall, requeue failed ranges, RTT probe for thresholds.
- **Parallel connections** — Multiple streams per transfer; finer range granularity (max_streams×4) for load balance and resume.
- **End-to-end integrity** — BLAKE3 of bytes sent; server verifies and replies HASH_OK or HASH_MISMATCH (client keeps checkpoint on mismatch).
- **Resume with per-range verification** — SQLite checkpoint (WAL) stores completed ranges and per-range BLAKE3; server re-verifies before accepting data.
- **Skip unchanged (default)** — Sender sends CHECK_HASH; server replies HAVE_HASH (skip) or NEED_FILE. Use `--force` to transfer anyway. Server persists hashes in `~/.shift/server_cache.db`.
- **`--stats` / `--json`** — Per-file transfer report (throughput, streams, RTT, ranges); JSON for scripting/benchmarks.
- **Capability negotiation** — Client and server negotiate compression, encryption, streams, RTT probe, FEC.
- **LZ4 compression** — Optional; auto-disabled for already-compressed types (zip, gz, mp4, jpg, etc.).
- **AES-256-GCM encryption** — Optional authenticated encryption.
- **TCP mutual TLS** — `--tls` with cert dir; build with `--features tls`.
- **FEC (RaptorQ)** — Optional; build with `--features fec`. Manual trigger; auto-trigger (loss heuristic) not yet implemented.
- **Configurable** — Buffer sizes, stream count, timeouts, socket options via TOML.

## Not yet implemented

- **`--dedup`** — CLI flag only; block-level deduplication (rsync-style) is not implemented. Defer until there is a clear use case.
- **FEC auto-trigger** — Loss heuristic in coordinator + signaling receiver; see `docs/ROADMAP.md`.
- **io_uring end-to-end** — Scaffolding only (`--features iouring`); transfer path still uses standard reads. Revisit if disk is proven bottleneck.

## Quick Start

### Installation

```bash
cargo build --release
```

Binary: `target/release/shift`. TCP and QUIC are both built-in.

**Server (TCP or QUIC):**

```bash
./target/release/shift server              # TCP (base port + data ports)
./target/release/shift server --quic        # QUIC (single UDP port)
```

**Client:** Tries QUIC first, then TCP. Use `--tcp` to force TCP.

```bash
./target/release/shift ./file user@host:8080:/
./target/release/shift --tcp ./file user@host:8080:/
```

**TCP server ports:** The server listens on the base port (e.g. 8080) for metadata and on **base_port+1, base_port+2, …** for data streams. Open that range in your firewall (e.g. **8080–8096** for 16 streams). If you can only open a few ports, use **`--streams 4`** so only 5 ports (8080–8084) are needed:

```bash
./target/release/shift --tcp --streams 4 ./file.bin user@host:8080:/
```

### Usage

**Transfer files (SCP-like):**

```bash
# Single file
./target/release/shift file.txt user@host:/path/

# Multiple files
./target/release/shift file1.txt file2.txt host:/backup/

# Recursive directory
./target/release/shift -r ./local_dir/ user@host:/remote/path/

# Skip unchanged is default; use --force to transfer anyway
./target/release/shift ./backup.tar host:/backup/
./target/release/shift --force ./backup.tar host:/backup/

# Force TCP, print stats, JSON output
./target/release/shift --tcp --stats file.bin host:/
./target/release/shift --tcp --json file.bin host:/
```

**Flags:**

| Flag | Description |
|------|-------------|
| `-u`, `--update` | Skip unchanged files (default: on). Server cache: `~/.shift/server_cache.db`. |
| `--force` | Transfer even if file is unchanged on receiver. |
| `--stats` | Print per-file transfer stats (throughput, streams, RTT, ranges). |
| `--json` | Print per-file report as JSON (for scripting/benchmarks). |
| `--tcp` | Force TCP (no QUIC probe). |
| `--streams N` | Use N parallel streams (default: auto). Use 4 when only 5 server ports (8080–8084) are open. |
| `--tls` | Use mutual TLS for TCP (requires `--features tls` and cert dir). |
| `-r`, `--recursive` | Recursive directory transfer. |
| `-d`, `--dedup` | Flag only; deduplication not implemented. |

## Configuration

Create `config.toml` in the working directory. Example:

```toml
[server]
address = "0.0.0.0"
port = 8080
output_directory = "./downloads"
max_clients = 100
enable_compression = false
timeout_seconds = 30

[client]
server_address = "127.0.0.1"
server_port = 8080
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

## Architecture

- **Protocol** — Metadata on base port: capability handshake, optional RTT (PING/PONG), then CHECK_HASH (--update) or metadata (filename/size/max_streams); READY; optional per-range hash verification (0x06/0x07); parallel data streams; final BLAKE3 (0x03) and server reply 0x04 (OK) or 0x05 (mismatch).
- **Integrity** — Sender hasher over bytes sent (BTreeMap by offset); hash sent after all data; server hasher on received bytes. Comparison order and framing: see `docs/INTEGRITY_DESIGN.md`.
- **Resume** — SQLite per transfer (`.shift_checkpoint`), WAL; per-range BLAKE3 stored and verified on reconnect.
- **Transport** — `Transport` / `StreamOpener` in `src/transport.rs`; TCP and QUIC (quinn) with same protocol.

## Testing

```bash
cargo test
```

- **Unit tests** — Config, compression, encryption, resume, integrity, base, utils.
- **Integration test** — `tests/loopback_transfer.rs`: `test_loopback_via_cli` runs the `shift` binary as server and client (full protocol). Run `cargo build --bin shift` first. In-process tests `test_loopback_tcp_transfer_end_to_end` and `test_loopback_tcp_update_skip_unchanged` are `#[ignore]` (run with `cargo test --test loopback_transfer -- --ignored`).

## Docs

- **`docs/ROADMAP.md`** — Done / not done; FEC auto-trigger, io_uring, --dedup; control channel.
- **`docs/ADAPTIVE_STREAMS.md`** — Adaptive stream scaling, coordinator, stall detection.
- **`docs/INTEGRITY_DESIGN.md`** — Hash comparison order and framing.

## License

MIT License
