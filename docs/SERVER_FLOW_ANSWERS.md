# Server flow — answers to the 5 questions

## Question 1: After metadata, what function receives the file data?

**Inline logic in `tcp_server.rs`.** The server does **not** call `receive_file_tcp()`. In `handle_connection` it:

1. Creates the output file and splits into ranges.
2. Spawns one listener per range (each in a `tokio::spawn`).
3. Sends `READY` on the control connection.
4. Optionally does range-verify (0x06) on the control connection.
5. Awaits all data handles; each handle accepted one TCP connection and called **`receive_range_tcp(thread_id, file, data_stream, ...)`** from `tcp_transfer.rs` (one range per connection; range comes from the wire).
6. Reads hash from client, hashes the file, sends `HASH_OK` or `HASH_MISMATCH`.

So data reception is: N listeners → N accepts → N calls to `receive_range_tcp` (range read from each connection’s header).

---

## Question 2: Exact line that splits ranges (server)

In **`tcp_server.rs`** inside `handle_connection`, after reading `(filename, file_size, num_streams)` from metadata:

```rust
let num_ranges = crate::base::transfer_num_ranges(num_streams);
let ranges = crate::base::split_file_ranges(file_size, num_ranges);
```

So: **`num_streams` from metadata** → `transfer_num_ranges(num_streams)` → **`split_file_ranges(file_size, num_ranges)`**.

---

## Question 3: How many data connections does the server accept?

From **metadata**: the 8-byte slot after `file_size` is read as `num_streams`. Then:

- `num_ranges = transfer_num_ranges(num_streams)` (e.g. 16 → 64 ranges).
- Server spawns **`ranges.len()`** listeners (one per range).
- It accepts **exactly one connection per listener** (64 connections for 64 ranges).

So the number of connections is **`transfer_num_ranges(num_streams)`**, where `num_streams` is the value sent by the client in metadata (the client’s `config.max_streams` at the time it built the metadata).

---

## Question 4: What hash does the server send back?

The server **does not send a hash back**. It:

1. Reads **expected_hash** (32 bytes) from the client after the 0x03 type byte.
2. Computes **actual_hash = hash_file(&output_path, file_size)**.
3. If `actual_hash != expected_hash` → sends **one byte `HASH_MISMATCH` (0x05)** and returns error.
4. If equal → sends **one byte `HASH_OK` (0x04)**.

Exact line for the hash it computes: **`let server_hash = hash_file(&output_path, file_size)?;`** in `tcp_server.rs`.

---

## Question 5: Where does the sender send the hash for verification?

In **`send_file_over_transport`** in `tcp_transfer.rs`:

- **Hash is computed once before opening connections:**  
  `let file_hash = tokio::task::spawn_blocking({ let path = file_path.to_path_buf(); move || hash_file(&path, file_size) }).await?...;`
- **Sent after the coordinator loop**, when sending the hash message:  
  `writer.write_all(&[msg::HASH]).await?;`  
  `writer.write_all(&file_hash).await?;`

So the client sends **hash_file(source_path, file_size)** (the same value in both the pre-connect computation and the message).

---

## Why server debug logs might not appear

- **"Receiving: {}"** is `eprintln!` and does appear in the test output → `handle_connection` **is** running.
- **`tracing::debug!`** logs may not appear if the tracing subscriber in the test process doesn’t attach to the server’s spawned task or if `RUST_LOG=shift=debug` isn’t applied as expected.
- **`eprintln!`** added at: metadata received, range split, before/after `hash_file`, and expected hash from client will show on stderr regardless of tracing and confirm which path runs and what hashes are used.
