# Shift

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

Shift is a high-performance file transfer system for reliable, efficient data movement over TCP and QUIC. Built with Rust, it uses parallel streams, adaptive stream scaling, and BLAKE3 integrity checks. Cross-platform: Linux, macOS, and Windows.

## Features

- **Push and pull** — Upload (`shift file host:/path/`) or download (`shift host:/path/file ./`), SCP-style. Pull runs over TCP.
- **Dual transport** — QUIC (default) with TCP fallback; force TCP with `--tcp`. Same protocol on both.
- **Adaptive streams** — RTT probe caps streams on high-latency (WAN) to avoid stalls; scale-up and requeue on low RTT. No manual `--streams` needed for typical use.
- **Parallel connections** — Multiple streams per transfer; finer range granularity (max_streams×4) for load balance and resume.
- **End-to-end integrity** — BLAKE3 of bytes sent; server verifies and replies HASH_OK or HASH_MISMATCH (client keeps checkpoint on mismatch).
- **Resume with per-range verification** — SQLite checkpoint (WAL) stores completed ranges and per-range BLAKE3; server re-verifies before accepting data.
- **Skip unchanged (default)** — Sender sends CHECK_HASH; server replies HAVE_HASH (skip) or NEED_FILE. Use `--force` to transfer anyway. Server persists hashes in `~/.shift/server_cache.db`.
- **`--stats` / `--json`** — Per-file transfer report (throughput, streams, RTT, ranges); JSON for scripting/benchmarks.
- **Capability negotiation** — Client and server negotiate compression, encryption, streams, RTT probe, FEC.
- **LZ4 compression** — Optional; auto-disabled for already-compressed types (zip, gz, mp4, jpg, etc.).
- **AES-256-GCM encryption** — Optional authenticated encryption.
- **TCP mutual TLS** — `--tls` with cert dir; build with `--features tls`.
- **FEC (RaptorQ)** — Optional; build with `--features fec`. Auto-triggers on loss signals (stall, sudden throughput drop, QUIC loss); see `docs/ADAPTIVE_STREAMS.md`.
- **Configurable** — Buffer sizes, stream count, timeouts, socket options via TOML.

## Not yet implemented

- **Block-level deduplication (rsync-style)** — Not implemented; deferred until there is a clear use case. Whole-file skip-unchanged is supported via `--update`.
- **io_uring end-to-end** — Scaffolding only (`--features iouring`); transfer path still uses standard reads. Revisit if disk is proven bottleneck.
- **Pull over TLS or QUIC** — Pull (download) runs over plain TCP; push supports `--tls` and QUIC.

## Quick Start

### Installation

```bash
cargo build --release
```

Binary: `target/release/shift`. TCP and QUIC are both built-in.

**Server:** A single `server` command listens for **both** TCP and QUIC on the configured port (TCP on the base port + data ports; QUIC on the same port over UDP).

```bash
./target/release/shift server               # serves TCP + QUIC
./target/release/shift server --config my.toml
```

**Client:** Probes TCP and QUIC and picks the faster (preferring QUIC when it is within ~10% of TCP). Use `--tcp` to force TCP. Pull (download) always uses TCP.

```bash
# Upload (push)
./target/release/shift ./file user@host:8080:/
./target/release/shift --tcp ./file user@host:8080:/

# Download (pull): remote source, local destination. Runs over TCP.
./target/release/shift user@host:8080:/remote/file.bin ./local_dir/
```

When the destination ends with `/` (or is an existing directory) the remote file
name is used; otherwise the destination is the exact target path.

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

# Download a file from the server (pull)
./target/release/shift host:8080:/backup/data.bin ./
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

## Configuration

Create `config.toml` in the working directory (auto-created with defaults on first run if missing). Stream counts, buffer sizes, and socket buffers are auto-calculated from file size when not set. Example:

```toml
[server]
address = "0.0.0.0"
port = 8080
output_directory = "./downloads"
max_clients = 100
enable_compression = false
# Optional overrides (auto-calculated if omitted):
# parallel_streams = 16
# buffer_size = 16777216
# socket_send_buffer_size = 16777216
# socket_recv_buffer_size = 16777216
# max_file_size = 0
# allowed_extensions = ["bin", "tar"]

[client]
server_address = "127.0.0.1"
server_port = 8080
enable_compression = false
# Optional overrides (auto-calculated if omitted):
# parallel_streams = 16
# buffer_size = 16777216
# socket_send_buffer_size = 16777216
# socket_recv_buffer_size = 16777216

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

- **Protocol** — Metadata on base port: capability handshake, optional RTT (PING/PONG), then CHECK_HASH (--update) or metadata (filename/size/max_streams); READY; optional per-range hash verification (0x06/0x07); parallel data streams; final BLAKE3 (0x03) and server reply 0x04 (OK) or 0x05 (mismatch). Pull (download) uses PULL_REQUEST (0x12) → PULL_OK (0x13, size + hash) / PULL_NOT_FOUND (0x14), then the same data plane in reverse.
- **Integrity** — End-to-end whole-file BLAKE3: the receiver's hash of the written file must equal the sender's hash of the source. Per-range BLAKE3 is used for resume verification. See `docs/INTEGRITY_DESIGN.md`.
- **Resume** — SQLite per transfer (`.shift_checkpoint`), WAL; per-range BLAKE3 stored and verified on reconnect.
- **Transport** — `Transport` / `StreamOpener` in `src/transport.rs`; TCP and QUIC (quinn) with the same protocol for push. Pull runs over TCP.

## Testing

```bash
cargo test
```

Run with all optional features:

```bash
cargo test --features tls,fec
```

- **Unit tests** — Config, compression, encryption, resume, integrity, base, utils, FEC.
- **Integration tests** (`tests/loopback_transfer.rs`) — Real protocol over loopback, serialized so they never contend for data ports:
  - `test_loopback_tcp_transfer_end_to_end` — in-process push, verifies the received file's BLAKE3.
  - `test_loopback_tcp_update_skip_unchanged` — `--update` skips an unchanged file.
  - `test_loopback_detects_corruption` — corrupting the received file forces a re-transfer.
  - `test_loopback_pull_download` — in-process pull (download) over the reverse data plane.
  - `test_loopback_pull_via_cli` — full-binary pull via the `shift` CLI.
  - `test_loopback_via_cli` — full-binary push via the CLI; `#[ignore]`d because the push adaptive coordinator can deadlock the hash-exchange under the timing of a constrained CI host (reproduces on pristine `main`). Run it explicitly with `cargo test -- --ignored`.

  The CLI tests build and run the `shift` binary, so run `cargo build --bin shift` first (or `cargo test`, which builds it).

## Docs

- **`docs/ROADMAP.md`** — Done / not done; persistent connection, manifest, transport selection, FEC auto-trigger.
- **`docs/ADAPTIVE_STREAMS.md`** — Adaptive stream scaling, coordinator, stall detection.
- **`docs/INTEGRITY_DESIGN.md`** — Hash comparison order and framing.

## License

MIT License
