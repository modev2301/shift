# Shift — Roadmap

Priority order after QUIC queue lands.

---

## Immediate — Observability (low effort, high value)

The data is already there. Just needs surfacing.

**UI — protocol and stream count**
```
⚡ QUIC  |  streams 6→8  |  238 MB/s  |  ETA 0:42
🔌 TCP   |  streams 4→6  |  180 MB/s  |  ETA 1:12
```
One line change in `progress.rs`. High user value.

**`--stats` / `--json` output**
```json
{
  "transport": "quic",
  "bytes": 1073741824,
  "duration_ms": 4200,
  "throughput_mbps": 2048,
  "streams_initial": 6,
  "streams_peak": 8,
  "streams_stalled": 1,
  "rtt_ms": 85,
  "ranges_total": 32,
  "ranges_resumed": 4
}
```
Makes shift scriptable and benchmarkable. Feeds into criterion benchmarks later.

---

## Short Term — FEC

Now that the bandwidth estimator exists, FEC has a natural trigger. The estimator already knows when throughput degrades. The question after stall detection rules out slow workers is: is it packet loss?

**What needs building:**
- Loss detection heuristic — stalls without worker errors suggest loss rather than slow disk or CPU
- `raptorq` integration below the transport layer, above the wire
- FEC block size relative to range size
- Redundancy ratio config (5-10%)
- BLAKE3 verify after FEC decode — not before

**Fits here in the pipeline:**
```
Before: chunked ranges → transport streams
After:  chunked ranges → FEC encoder → transport streams
                                ↓
                        receiver: FEC decoder → write
```

The resume layer doesn't change — FEC is transparent to it.

---

## Short Term — TCP Mutual TLS

Currently TCP uses AES-256-GCM via `ring` directly — confidentiality only, no authentication.

**When this matters:**
- Transfers crossing untrusted networks
- Any multi-operator or cross-internet scenario

**What needs building:**
- `rustls` on TCP transport (already used for QUIC via quinn)
- `rcgen` for TCP cert generation (already used for QUIC)
- Mutual auth — both sides present certs
- Key distribution story — pre-shared, or a simple PKI

Low complexity given rustls and rcgen are already in the project.

---

## Medium Term — Structured Benchmarking

You have `criterion` as a dev dependency but it's not wired to anything.

**Hot paths worth benchmarking:**
- BLAKE3 hashing throughput at various chunk sizes
- LZ4 compress/decompress throughput — compressible vs incompressible
- Manual LE framing encode/decode
- `RangeQueue` under contention (multiple workers)
- End-to-end transfer throughput (loopback, then LAN, then WAN)

This is what tells you where to optimize next and whether changes regress performance.

---

## Medium Term — Finer Range Granularity

Currently `split_file_ranges(file_size, max_streams)` — one range per potential stream.

With the queue in place, you can split into `max_streams * 4` smaller ranges. Benefits:
- Better load balancing — fast workers take more ranges, slow ones fall behind without blocking completion
- Stall recovery is cheaper — requeuing a smaller range wastes less work
- More granular resume — fewer bytes to re-transfer on restart

No protocol change needed since range headers carry explicit `(start, end)`.

---

## Longer Term — RDMA / io_uring Full Path

For datacenter use cases pushing toward line rate:

**io_uring on Linux**
Already identified — `tokio-uring` behind a feature flag. Gives true zero-copy on the sender read path. Worth revisiting once benchmarks show disk read as a bottleneck.

**RDMA**
For very high throughput datacenter transfers (100Gbps+). Separate transport implementation behind the `StreamOpener` trait — fits the architecture cleanly. Only relevant if you're targeting HPC or high-end datacenter.

---

## Full Roadmap Table

| Priority | Item | Effort | Depends On |
|----------|------|--------|------------|
| 1 | QUIC queue (in progress) | Medium | Steps 4-6 done ✅ |
| 2 | UI protocol + stream display | Trivial | QUIC queue |
| 3 | `--stats` / `--json` | Low | QUIC queue |
| 4 | criterion benchmarks | Low | `--stats` |
| 5 | FEC via raptorq | Medium | Observability |
| 6 | TCP mutual TLS | Medium | Independent |
| 7 | Finer range granularity | Low | QUIC queue |
| 8 | io_uring on Linux | Medium | Benchmarks show need |
| 9 | RDMA transport | High | Architecture stable |

---

## Current status (as implemented)

| Item | Status |
|------|--------|
| QUIC queue + StreamOpener | ✅ Done |
| UI protocol + stream count | ✅ Done |
| `--stats` / `--json` | ✅ Done |
| criterion benchmarks | ✅ Done |
| Finer range granularity (max_streams×4) | ✅ Done |
| `--update` (skip unchanged by hash) | ✅ Done |
| `--dedup` (block deduplication) | ❌ Not started (CLI flag only) |
| FEC (raptorq) | ✅ Done: `fec` feature, CAP_FEC negotiation, frame type 0x02, encode per chunk in transfer_range_tcp and stream sender; decode in receive_range_tcp and stream receiver |
| FEC auto-trigger (loss heuristic) | ❌ Not started. When revisiting: enable FEC in coordinator when `stall_looks_like_loss(&estimator)`; receiver must know FEC switched on mid-transfer — either negotiate FEC upfront (simpler, small overhead when unused) or add a control message. Prefer upfront; document and revisit after `--update`. |
| TCP mutual TLS | ✅ Done |
| io_uring on Linux | 🔶 Scaffolding: `iouring` feature, `FileReader` (Std + Uring), async `read_at`; Linux-only, transfer path not yet switched to `FileReader` |
| RDMA transport | ❌ Not started |

---

## FEC vs TCP mutual TLS ordering

**Target environment drives the order:**

- **LAN/datacenter only** → TCP mutual TLS first (loss is low; auth/confidentiality matter as soon as you cross trust boundaries).
- **WAN or mixed (LAN + internet)** → FEC first (packet loss dominates; then add mutual TLS).
- **Both matter equally** → FEC, then TCP mutual TLS (fix WAN behavior first; TLS improves security in all environments).

After QUIC queue lands, items 2, 3, and 7 are small and can be done in parallel.

---

## Control channel (for --update and FEC auto-trigger)

**Order today:** Capability handshake → optional RTT (PING/PONG) → **then** either:
- **CHECK_HASH (0x0A)** — client sends filename_len + filename + hash (32). Server checks; HAVE_HASH → skip, NEED_FILE → then full metadata and data streams.
- **Metadata** — first byte = start of filename_len (8 LE), then filename, file_size (8), max_streams (8). Server ACK (READY 0x01), then range-hash exchange if any, then data streams open.

So there **is** metadata exchange (and an existing --update-style CHECK_HASH path) before data streams. Wiring `--update` needs: fast mtime+size check, slow BLAKE3 fallback, and ensuring receiver records BLAKE3 on completion; no new message type required.

---

## Integration tests

- **`tests/loopback_transfer.rs`** — `test_loopback_via_cli`: runs the `shift` binary as server and client over loopback; verifies file size and BLAKE3. Requires `cargo build --bin shift`. In-process tests (`test_loopback_tcp_transfer_end_to_end`, `test_loopback_tcp_update_skip_unchanged`) are `#[ignore]`; run with `-- --ignored` to execute them.

## What to do next

- **--update**: Done (CHECK_HASH, server cache at `~/.shift/server_cache.db`, persist on completion).
- **--dedup**: CLI flag only; block-level deduplication not implemented. Defer until there is evidence of the use case.
- **FEC auto-trigger**: Not started. Loss heuristic in coordinator + notify receiver (upfront FEC negotiation preferred). See table.
- **TCP mutual TLS**: Done.
- **io_uring end-to-end**: Not started. Scaffolding only; wire transfer path to `FileReader` when benchmarks show disk read as bottleneck (e.g. on EC2).
