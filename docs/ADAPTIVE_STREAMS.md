# Adaptive Streams — Implementation Plan

This document is the locked design and implementation order for adaptive stream count, RTT probing, and coordinator-driven range assignment. It fits existing constraints: whole-range redistribution only, no checkpoint schema change, pre-assigned ports.

---

## Step 1 — Handshake Semantics (tiny change, high leverage)

Change `max_streams` interpretation in `Capabilities::negotiate`:

- Add `initial_streams: u16` — computed from `calculate_optimal_parallel_streams(file_size, None)` at handshake time (or with RTT once Step 2 is in).
- `max_streams` becomes the ceiling (already the min of client/server, keep that).
- Both sides call `apply_capabilities_to_config` with `initial_streams` as the starting count, `max_streams` as the ceiling.

**Receiver change:** Pre-spawn `max_streams` TCP listeners instead of `num_streams`. QUIC loop runs to `max_streams`. Everything else unchanged.

---

## Step 2 — RTT Probe (control channel, before data streams open)

Add two message variants to the control channel protocol:

```
Ping { timestamp_us: u64 }
Pong { timestamp_us: u64 }
```

Sender sends Ping immediately after capability negotiation, before opening any data streams. Receiver echoes as Pong. Sender computes `rtt_ms = (now_us - timestamp_us) / 1000`. Feeds into `calculate_optimal_parallel_streams` as the measured latency, replacing the optional config value.

---

## Step 3 — Range Queue

Replace the current "split all ranges upfront, assign by index" approach with a coordinator-owned queue:

```rust
struct RangeQueue {
    pending: VecDeque<FileRange>,
    in_flight: HashMap<StreamId, FileRange>,
    completed: HashSet<FileRange>,  // loaded from SQLite on start
}
```

On transfer start:

- Load completed ranges from SQLite into `completed`.
- All remaining ranges into `pending`.
- Coordinator pops from `pending` and assigns to streams as they open.

This is the same behavior as today for the fixed-stream case — just explicit instead of implicit. No wire format change.

---

## Step 4 — Per-Stream Metrics

Each stream task owns a `StreamMetrics` and reports to coordinator via a channel every 2 seconds:

```rust
struct StreamReport {
    stream_id: StreamId,
    bytes_this_interval: u64,
    interval_ms: u64,
}
```

Coordinator receives these on a `mpsc::Receiver<StreamReport>`. No shared atomics needed — clean ownership.

---

## Step 5 — Bandwidth Estimator

Simple rolling window, lives in coordinator:

```rust
struct BandwidthEstimator {
    samples: VecDeque<(Instant, u64)>,
    window: Duration,  // 10 seconds
    peak_mbps: f64,    // track peak for scale-up threshold
}
```

`estimate_mbps()` sums bytes in window divided by window duration. Coordinator updates `peak_mbps` whenever current estimate exceeds it. Scale-up threshold is `0.80 × peak_mbps`.

---

## Step 6 — Coordinator Task

```rust
loop {
    select! {
        report = metrics_rx.recv() => {
            estimator.record(report.bytes_this_interval);
            update_stream_last_active(report.stream_id);
        }
        _ = interval.tick() => {
            check_stalls();
            check_scale_up();
        }
        result = stream_completion_rx.recv() => {
            handle_range_complete(result);
        }
    }
}
```

**Stall check:**

- For each in-flight stream: if `now - last_active > 2 × rtt_ms` → cancel stream task, push range back to `pending` (whole range), open new stream immediately (skip cooldown).

**Scale up check:**

- If `estimate_mbps < 0.80 × peak_mbps` AND `pending.len() > 0` AND `active_streams < max_streams` AND `now - last_change > 2 × rtt_ms` → pop range from `pending`, open new stream on next pre-assigned port, set `last_change = now`.

---

## Step 7 — UI / Observability (low effort, high value, do alongside)

Surface in progress output:

```
⚡ Connected via QUIC  |  streams: 6→8  |  238 MB/s  |  ETA 0:42
```

Add `--stats` JSON output after transfer:

```json
{
  "transport": "quic",
  "bytes": 1073741824,
  "duration_ms": 4200,
  "throughput_mbps": 2048,
  "streams_initial": 6,
  "streams_peak": 8,
  "streams_stalled": 1,
  "rtt_ms": 85
}
```

---

## Full Sequence Diagram

```
Sender                          Receiver
  │                                │
  │──── Capabilities ─────────────▶│
  │◀─── Negotiated (max=32,init=6)─│
  │                                │  pre-spawn 32 listeners
  │──── Ping {timestamp} ─────────▶│
  │◀─── Pong {timestamp} ──────────│
  │  rtt = 85ms                    │
  │  initial = 6 streams           │
  │                                │
  │──── open streams 1-6 ─────────▶│  6 listeners activate
  │  [coordinator starts]          │
  │  [metrics every 2s]            │
  │                                │
  │  t=4s: throughput low           │
  │──── open stream 7 ────────────▶│  7th listener activates
  │──── open stream 8 ────────────▶│  8th listener activates
  │                                │
  │  t=18s: stream 3 stalled       │
  │  cancel stream 3               │
  │──── open stream 9 ────────────▶│  range restarted
  │                                │
  │  all ranges complete           │
  │──── done ─────────────────────▶│
```

---

## What Does Not Change

- Wire format for data connections
- SQLite schema
- BLAKE3 verification
- Resume behavior on restart
- Capability negotiation structure
- Port assignment scheme (`base_port + 1 .. base_port + max_streams`)

---

## Implementation Order

- **Steps 1 and 2** — Implement first; independently useful and unblock the rest.
- **Steps 3–6** — Build on each other; implement together.
- **Step 7** — Can go in at any point.
