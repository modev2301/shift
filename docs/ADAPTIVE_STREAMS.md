# Adaptive stream scaling

A push transfer splits the file into ranges and moves them over several parallel
streams. The number of streams adapts to the link so that LAN transfers use many
streams while high-latency WAN links are not driven into tail stalls.

## Choosing the initial stream count

After the capability handshake the client may probe the path:

- **RTT** — `PING`/`PONG` (`0x08`/`0x09`) with a microsecond timestamp; the
  round trip seeds the stall threshold and the scale-up cooldown.
- **Bandwidth** — `PROBE`/`PROBE_ACK` (`0x0D`/`0x0E`): the client sends a fixed
  block and times the drain so the measured rate reflects the real link.

From these, `optimal_streams_from_bdp` sizes streams to fill the
bandwidth-delay product (`bandwidth × RTT ÷ buffer_size`), capped at
`MAX_STREAMS_WAN` (8) when RTT is above `RTT_WAN_CAP_MS` (60 ms) so a lossy WAN
path does not open more streams than it can keep busy. An explicit `--streams N`
is always respected and never overridden by the probe.

Both sides derive the range count from the negotiated stream ceiling with
`transfer_num_ranges` (`streams × 4`, capped at 128). Finer ranges than streams
give better load balancing and cheaper stall recovery.

## The coordinator

The coordinator owns a `RangeQueue`; workers pop ranges, send them, and report
completion (with the range's BLAKE3) or requeue on error. Each tick it:

1. **Estimates throughput** from per-worker `StreamReport`s (`BandwidthEstimator`).
2. **Detects stalls.** A worker that makes no progress for several
   `INTERVAL_DURATION`s accumulates `consecutive_zero_intervals`; once it crosses
   `stall_threshold_intervals` (15 in release, higher in debug so slow CI is not
   flagged) its range is requeued and a fresh worker is spawned. The threshold is
   generous so WAN/slow links are not falsely marked stalled.
3. **Scales up** when there is spare bandwidth and pending ranges, up to the
   negotiated `max_streams`. After a scale-up it waits ~`4×RTT` and disables
   further scale-up if throughput did not improve by ~5%, avoiding pointless
   stream churn.

## Resume

The coordinator persists completed ranges (and their per-range BLAKE3) to a
per-transfer SQLite checkpoint (WAL). On reconnect, completed ranges are
re-verified against the receiver's on-disk bytes (`RANGE_HASHES` /
`RANGE_VERIFY_RESULT`) before being skipped — see `docs/INTEGRITY_DESIGN.md`.

## FEC auto-trigger (`--features fec`)

When both sides negotiate FEC, loss is inferred from throughput-side signals
because stable Rust exposes no per-connection retransmit counter for TCP:

- **Stall** — a loss-like stall (above) sets `enable_fec_auto`, so subsequent
  chunks are sent as RaptorQ blocks (frame type `0x02`).
- **Sudden drop** — `BandwidthEstimator::sudden_drop()` (current < 80% of peak)
  also enables FEC.
- **QUIC loss** — on QUIC the opener can sample `loss_stats()`; a loss rate above
  the threshold enables FEC directly.

Once enabled, repair symbols are added per block (`fec_repair_packets`) so a
lossy link recovers without a full retransmit.
