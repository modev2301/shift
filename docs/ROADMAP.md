# Shift roadmap

Four focus areas: persistent connection, directory + manifest, transport selection, and FEC auto-trigger.

---

## 1. Persistent connection (fixes enterprise parallel TCP detection)

**Goal:** Avoid opening N parallel TCP connections per file so enterprise firewalls/IDS don’t treat the client as a scanner or throttle it.

**Current state:** For each file, the client opens 1 metadata connection + **N data connections** (one per stream; N can be 8–32+). Each data connection is a new TCP connection to `base_port+1`, `base_port+2`, … Server listens on N ports. This “many connections from one host” pattern often triggers parallel-connection limits or alerts.

**Options:**

- **TCP multiplexing:** Use 1 (or a small fixed pool of) data connection(s) and multiplex ranges over it. Wire format: e.g. `[stream_id u16][range_id u32][start u64][len u64][payload...]`. Server accepts M connections (e.g. M=1–4), reads framed messages, and writes to file at `(start, len)`. Client assigns ranges to the same connection(s) instead of opening one connection per range.
- **QUIC:** Already multiplexes over a single connection; no change needed for “persistent connection” when using QUIC. Encouraging QUIC where it performs well (transport probe) helps in enterprise environments that allow one UDP flow.

**Next steps:** Design framed data protocol for TCP (backward compatibility or new capability flag); implement server listener(s) and client opener for multiplexed data; switch TCP path to use multiplexed streams instead of N separate connections.

---

## 2. Directory transfer + manifest (rsync replacement path)

**Goal:** Support directory transfer with a manifest so the receiver knows the full set of files (paths, sizes, optional mtime/hash) before data, enabling a single “session” and rsync-like workflows.

**Current state:** `-r` recursive is implemented: main collects a flat list of files via `walkdir`, then sends each file in sequence. Each file is a separate metadata + data transfer; there is no manifest (no single message describing “these N files with these paths/sizes”).

**Desired:**

- **Manifest:** Before sending file data, client sends a manifest: e.g. count, then for each file: path (relative to transfer root), size, optional mtime, optional blob hash for skip-unchanged.
- **Server:** Receives manifest, creates directory structure, can respond with “which files I already have” (e.g. HAVE_HASH per file) so client can skip.
- **Single session:** One connection (or one QUIC connection) used for manifest + multiple file transfers, instead of one logical session per file.

**Next steps:** Define manifest wire format (new message type in base); add server handler for manifest and per-file HAVE_HASH/NEED_FILE; extend client to send manifest first when transferring a directory, then send only needed files; keep single-file transfer as today (no manifest).

---

## 3. Transport probe auto-selection (never slower than scp by default)

**Goal:** Automatically choose TCP or QUIC so that default behavior is at least as fast as scp (and preferably faster).

**Current state:** When `server_addr` is known and `--tcp` is not set, client runs bandwidth probes for both TCP and QUIC. It chooses **QUIC** only if `quic_bw >= 0.9 * tcp_bw`; otherwise it uses **TCP**. So we already bias toward TCP when QUIC is slower, avoiding “slower than scp” in the common case. If either probe fails, we fall back to TCP.

**Refinements (optional):**

- Log which transport was chosen and the probe results (e.g. “Transport: TCP (probe: TCP 120 MB/s, QUIC 95 MB/s)”).
- If desired, add an optional “scp baseline” probe (e.g. run scp in the background once and compare) for reporting; not required for “never slower” if we keep the 0.9 rule and TCP fallback.

**Next steps:** Add a one-line log when transport is chosen (TCP vs QUIC and probe bps or “probe failed”); document in CLI help that default is “never slower than TCP baseline.”

---

## 4. FEC auto-trigger wired to loss detection

**Goal:** Automatically enable FEC when loss is detected so that lossy links get repair symbols without manual config.

**Current state:** FEC auto-trigger exists and is wired to **throughput-based signals** (both when `fec` feature is enabled):

- **Stall detection:** Workers that make no progress for several coordinator intervals are treated as “loss-like stall”; `enable_fec_auto` is set and FEC is used for subsequent chunks.
- **Throughput drop:** `BandwidthEstimator::sudden_drop()` (current &lt; 80% of peak) sets `enable_fec_auto`.

So “loss” is inferred from stalls and throughput drop, not from an explicit loss counter.

**Improvements:**

- **Explicit loss (QUIC):** When using QUIC, connection stats (e.g. `path_stats().lost_packets` or loss rate) can be read periodically. If loss rate exceeds a threshold (e.g. &gt; 0.5%), set `enable_fec_auto` (and optionally log “FEC auto-enabled due to QUIC loss”).
- **TCP:** No kernel API for per-connection retransmit count in stable Rust; keep using stall + throughput drop as the loss proxy.
- **Thresholds:** Optionally make the 80% threshold (and stall interval count) configurable so FEC triggers earlier on very lossy links.

**Next steps:** When `fec` is enabled and transport is QUIC, in the transfer coordinator (or a background task) sample path stats; if lost_packets or loss rate is above a small threshold, set `enable_fec_auto`. Document in code that stall and sudden_drop are the TCP-side “loss detection” signals.
