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
| FEC (raptorq) | ❌ Not started |
| TCP mutual TLS | ❌ Not started |
| io_uring on Linux | ❌ Not started |
| RDMA transport | ❌ Not started |

---

## FEC vs TCP mutual TLS ordering

**Target environment drives the order:**

- **LAN/datacenter only** → TCP mutual TLS first (loss is low; auth/confidentiality matter as soon as you cross trust boundaries).
- **WAN or mixed (LAN + internet)** → FEC first (packet loss dominates; then add mutual TLS).
- **Both matter equally** → FEC, then TCP mutual TLS (fix WAN behavior first; TLS improves security in all environments).

After QUIC queue lands, items 2, 3, and 7 are small and can be done in parallel.

---

## What to do next

- **--dedup**: Block-level deduplication (rolling hash, block index, protocol) — larger feature.
- **FEC**: raptorq + loss heuristic; fits after observability.
- **TCP mutual TLS**: Add `tokio-rustls`, rcgen for certs, mutual auth; then wrap TCP metadata + data streams in TLS when enabled.
- **io_uring**: Feature-flag and optional `tokio-uring` for Linux zero-copy read path.
