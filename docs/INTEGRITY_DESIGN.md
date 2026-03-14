# Integrity design

End-to-end BLAKE3 verification so the receiver can confirm the transferred file matches the source.

## Comparison order

- **Sender:** A dedicated task hashes bytes **actually sent** (after compression/encryption if any). Chunks are tagged by **file offset**. They are fed into `run_ordered_hasher`: a BTreeMap keyed by offset; the hasher drains **contiguous** regions in file order and updates BLAKE3. Result: one hash over the whole file in canonical order.
- **Receiver:** Same shape. Receive tasks write to the file and send `(offset, plaintext)` to a hasher task. The same `run_ordered_hasher` (BTreeMap, drain contiguous, BLAKE3) produces the received-file hash.
- **Comparison:** After all data is received and (optionally) fsync’d, the receiver reads the **expected** hash from the client (wire message 0x03 + 32 bytes), finalizes the receive-side hasher to get **actual** hash, then compares. Only then does it send 0x04 (OK) or 0x05 (mismatch).

So both sides hash in **file order** (by offset); the receiver never trusts the sender’s hash until it has computed its own over the bytes it wrote.

## Wire framing

- **Client → Server (hash):** 1 byte `0x03` (HASH) + 32 bytes BLAKE3 digest.
- **Server → Client (result):** 1 byte `0x04` (HASH_OK) or `0x05` (HASH_MISMATCH). On mismatch, client keeps checkpoint for retry.

Defined in `src/base.rs` (`msg::HASH`, `msg::HASH_OK`, `msg::HASH_MISMATCH`). Implemented in `src/tcp_transfer.rs` (`run_ordered_hasher`, send/receive paths) and mirrored in TCP/QUIC servers.
