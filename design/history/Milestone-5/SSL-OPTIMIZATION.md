# SSL Performance with 220 Partitions — Confluent Cloud

## Context

The Rust Kafka producer's 10-partition SSL performance now beats CKPy (400ms latency) after the previous optimizations (reusable coalescing buffer, poll_fn for writes). With 220 partitions against **Confluent Cloud** (same region, same AZ): 33k msg/s / 20s latency / 50% CPU vs CKPy's 64k msg/s / 258ms latency / 123% CPU.

**Why CC changes the analysis**: With real network RTT (~0.5-1ms), `tls_stream.write()` can only accept data at the rate the TCP window allows. Each write fills the TCP send buffer, then the sender waits for ACKs before writing more. The 50% CPU is split between ~50% active work (TLS encryption + TCP writes) and ~50% idle (waiting for TCP ACKs from the remote broker). This is fundamentally different from localhost where TCP loopback is near-instant.

**Per-core efficiency is comparable**: Rust 66k msg/s per core vs CKPy 52k msg/s per core. The gap is CPU utilization (0.5 cores vs 1.23 cores), partly because librdkafka uses multi-threaded I/O.

## Changes

### 1. Fix O(n²) buffer coalescing in SSL write path (HIGH impact)

**Files:** `src/common/network/ssl_transport_layer.rs` (lines 345-372), `src/common/network/byte_buffer_send.rs` (lines 81-128)

**The problem**: With 220 partitions, `ByteBufferSend` has ~441 buffers totaling ~8MB. Each `write_to` call:
1. Builds `Vec<IoSlice>` from ALL remaining buffers (~441 entries)
2. Calls `write_vectored` which coalesces ALL remaining data (~8MB) into `write_buf`
3. Calls `tls_stream.write(&write_buf)` which accepts only what the TCP window allows

With CC network RTT, `tls_stream.write()` accepts a limited amount per call (say 256KB-1MB, depending on TCP window). The remaining 7MB+ of coalesced data is wasted. On the next call, we re-coalesce 7MB for another partial write. Over ~32 rounds:

**Total memcpy: 8 + 7.75 + 7.5 + ... ≈ 128MB for 8MB of actual data (16x amplification).**

**Fix**: Cap the coalescing to a fixed chunk size and limit IoSlice collection to match.

In `ssl_transport_layer.rs` `write_vectored`:
```rust
const MAX_TLS_COALESCE: usize = 256 * 1024; // 256KB - matches typical TCP window chunk

fn write_vectored<'a>(
    &'a mut self,
    srcs: &'a [io::IoSlice<'a>],
) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
    Box::pin(async move {
        match &mut self.state {
            SslState::Ready(tls_stream) => {
                if srcs.len() <= 1 {
                    if srcs.is_empty() { return Ok(0); }
                    return tls_stream.write(&srcs[0]).await;
                }
                let total: usize = srcs.iter().map(|s| s.len()).sum();
                let coalesce_limit = total.min(MAX_TLS_COALESCE);
                self.write_buf.clear();
                self.write_buf.reserve(coalesce_limit);
                let mut remaining = coalesce_limit;
                for src in srcs {
                    if remaining == 0 { break; }
                    let n = src.len().min(remaining);
                    self.write_buf.extend_from_slice(&src[..n]);
                    remaining -= n;
                }
                tls_stream.write(&self.write_buf).await
            },
            // ... existing error branches unchanged ...
        }
    })
}
```

This reduces total memcpy from ~128MB to exactly 8MB (each byte copied once). The `MAX_TLS_COALESCE` constant of 256KB is a reasonable default — it matches typical TCP segment sizes and ensures TLS produces efficient records while limiting waste.

Optionally, also limit IoSlice collection in `byte_buffer_send.rs` `write_to` to avoid building a 441-entry Vec when only a few slices are needed. But this is secondary since the SSL coalescing cap already bounds the real work.

### 2. Replace Duration::ZERO timeout in attempt_read with poll_fn (MEDIUM impact)

**File:** `src/common/network/selector.rs` — line 456

Same pattern already applied to the write path. Each read attempt creates timer wheel infrastructure just to poll once. With CC, reads happen on every selector poll loop iteration for all 3 broker channels — the overhead compounds.

Replace:
```rust
let read_result = tokio::time::timeout(std::time::Duration::ZERO, channel.read()).await;
let bytes = match read_result {
    Ok(Ok(b)) => b,
    Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => 0,
    Ok(Err(e)) => return Err(e),
    Err(_elapsed) => 0,
};
```

With:
```rust
let mut read_fut = std::pin::pin!(channel.read());
let bytes = match std::future::poll_fn(|cx| match read_fut.as_mut().poll(cx) {
    Poll::Ready(result) => Poll::Ready(result),
    Poll::Pending => Poll::Ready(Ok(0)),
}).await {
    Ok(b) => b,
    Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
    Err(e) => return Err(e),
};
```

### 3. Skip select! on partial I/O progress in selector inner loop (MEDIUM impact)

**File:** `src/common/network/selector.rs` — lines 778-835

When SSL writes transfer bytes but don't complete a send, the inner loop enters `tokio::select!` to wait for readiness. With CC, the TCP socket may still have buffer space after a write (especially with large auto-tuned buffers). Skipping the `select!` roundtrip when bytes were actually transferred avoids the epoll/readiness-future overhead for those iterations.

a) Make `poll_channel` return `bool` (whether bytes were transferred):
```rust
async fn poll_channel(&mut self, ...) -> bool {
    // ... existing code, return had_bytes_transferred at the end ...
}
```

b) In the inner poll loop, track partial progress and skip select! when data is flowing:
```rust
loop {
    let mut had_partial_io = false;
    let channel_ids: Vec<String> = self.channels.keys().cloned().collect();
    for id in &channel_ids {
        if self.channels.contains_key(id) {
            let is_immediately = self.immediately_connected_keys.remove(id);
            if self.poll_channel(id, is_immediately, start_select).await {
                had_partial_io = true;
            }
        }
    }
    self.immediately_connected_keys.clear();

    let made_progress = !self.completed_sends.is_empty()
        || !self.completed_receives.is_empty()
        || !self.connected.is_empty()
        || !self.disconnected.is_empty();
    if made_progress { break; }

    // Data was transferred — the socket likely still has space.
    // Loop back immediately to try more I/O without epoll roundtrip.
    if had_partial_io { continue; }

    // No progress at all — wait for readiness from the reactor.
    match deadline {
        // ... existing select! code unchanged ...
    }
}
```

When TCP buffer fills and all channels hit WouldBlock, `had_partial_io` is false and we fall through to `select!` as before. The optimization only fires when writes are actively succeeding.

## What this does NOT address

The remaining throughput gap after these optimizations is primarily architectural:
- **librdkafka uses multi-threaded I/O** (123% CPU = I/O thread + producer thread overlapping encryption with TCP flush). The Rust sender is single-threaded — while waiting for TCP ACKs, it cannot encrypt the next request's data.
- **Serialized sends per broker**: only one send can be in-flight on a channel at a time (`KafkaChannel.set_send` rejects if one exists). This matches Java's design but means encryption and TCP flush are serialized per broker. Pipelining sends (encrypting request N+1 while flushing request N) would require architectural changes to decouple plaintext acceptance from TCP flush completion.
- **rustls vs OpenSSL**: different bulk encryption performance characteristics.

## Verification

1. `cargo test` — all existing tests pass
2. `cargo xtask lint` — no new warnings
3. SSL producer benchmark with 220 partitions against CC — measure msg/s and latency improvement
4. SSL producer benchmark with 10 partitions — verify no regression
