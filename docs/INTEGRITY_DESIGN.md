# Integrity design

Shift guarantees end-to-end integrity with [BLAKE3](https://github.com/BLAKE3-team/BLAKE3).
The hash algorithm is fixed; it is not negotiated in the capability handshake.

## Whole-file verification (the guarantee)

A transfer is only considered successful when the receiver's BLAKE3 of the
written file equals the sender's BLAKE3 of the source file.

- **Push:** after all ranges are sent, the client computes the whole-file BLAKE3
  (`hash_file`) and sends it framed as `HASH` (`0x03` + 32 bytes). The server
  computes the BLAKE3 of the file it wrote and replies `HASH_OK` (`0x04`) or
  `HASH_MISMATCH` (`0x05`). On mismatch the client keeps its checkpoint so the
  next run re-sends rather than skipping.
- **Pull:** the server advertises the file size and BLAKE3 in `PULL_OK`
  (`0x13`). After the client has received every range it recomputes the BLAKE3 of
  the downloaded file and fails the transfer (`IntegrityCheckFailed`) on any
  mismatch.

Because the check covers the whole file, a bug anywhere in the range/stream
machinery can only cause a *failed* transfer, never silent corruption.

## Per-range hashes (resume verification)

Each range carries an independent BLAKE3 of its plaintext bytes, computed by the
receiver as it writes (`receive_range_tcp`) and by the sender from disk
(`hash_file_range_path`). These are used for resume, not for the final check:

- On reconnect the client sends `RANGE_HASHES` (`0x06`): `num_ranges` followed by
  `(start, end, blake3)` for every range it believes is already complete.
- The server re-hashes those byte ranges of its on-disk file and replies
  `RANGE_VERIFY_RESULT` (`0x07`) listing the ranges whose bytes no longer match.
  Only verified ranges are skipped; everything else is re-sent.

This makes resume safe even if the partially-written file was modified or
truncated between runs.

## Data-channel framing

Each data connection carries exactly one range:

```
range header : start (u64 LE) | end (u64 LE) | flags (u8)
frame*       : type (u8) | size (u64 LE) | payload[size]
```

`flags` bit 0 = compression enabled, bit 1 = encryption enabled.
Frame `type`: `0x00` raw, `0x01` LZ4-compressed, `0x02` FEC block (RaptorQ, when
built with `--features fec`). The sender shuts down the write half after the last
frame; the receiver stops once it has the range's expected byte count.

## Skip-unchanged (`--update`)

Before sending, the client may send `CHECK_HASH` (`0x0A`) with the filename and
the source BLAKE3. The server replies `HAVE_HASH` (`0x0B`, skip) when its cached
hash, the file on disk, and a fresh re-hash all agree, or `NEED_FILE` (`0x0C`)
otherwise. Server hashes persist in `~/.shift/server_cache.db` across restarts.
For directories the same decision is made per file via the `MANIFEST` (`0x0F`) /
`MANIFEST_ACK` (`0x10`) exchange.
