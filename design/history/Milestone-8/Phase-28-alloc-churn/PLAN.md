# Phase 28 — Allocation churn: scratch reuse + zero-free receive reads

**Milestone-8 / Phase-28** · Agent number **N = 28**

Continuation of Phase 27's theme (remove unconditional wasted work Java's
JIT/TLAB absorbs), targeting the allocator block of the post-Phase-27 profile
(76.4% CPU @200k SASL_SSL latency-tuned: malloc/free ~5%, memset 1.4%).
None of the fixes condition on message size, batch size, partition count or
rate — they scale with event rate (Fix #1) and bytes received (Fix #2).

## Fix #1 — `run_once` scratch reuse + one request-managers lock + Java poll order

(commit `dac1372`)

- The PollResult before/after batches and the application-event drain buffer
  are accumulated in scratch Vecs reused across iterations (taken/restored,
  capacity retained) instead of three fresh Vecs + `split_off` per iteration
  (~7k iterations/s). Java's `entries` is a final List built once in the
  constructor; `processApplicationEvents` drains into a GC-nursery LinkedList
  — per-iteration heap allocation was a translation artifact.
- Phase 2 takes the `request_managers` `std::Mutex` once (was twice).
- Poll-call order now matches Java's `entries()` walk: coordinator → commit →
  heartbeat → offsets → … (previously heartbeat/offsets/fetch were polled
  BEFORE coordinator/commit and only the processing was reordered; managers
  interact through state set by network *responses*, not `poll()` itself —
  order-faithfulness, not a behavior change).
- `membership` handle borrowed (`as_ref`) instead of Arc-cloned twice/iter.

## Fix #2 — zero-free appending receive reads

(commit `5eac77e`)

`NetworkReceive` allocated its payload buffer with `vec![0u8; size]` per
response — malloc + memset of every received byte (the full fetch
throughput), immediately overwritten by socket bytes. Java zeroes its
`ByteBuffer.allocate` too, but in TLAB.

- `TransportLayer::try_read_append(buf: &mut Vec<u8>, limit)` — appending
  read, same `Ok(n)` / `Ok(0)`=EOF / `Err(WouldBlock)` contract as `try_read`
  over a whole drain. Default impl drains via `try_read` into bounded
  zero-initialized chunks (64 KB re-zero cap) — plaintext/mock semantics
  preserved.
- `SslTransportLayer` overrides it zero-free: `read_tls` +
  `process_new_packets` as in `try_read`, then
  `Read::take(limit).read_to_end(buf)` appends decrypted plaintext into the
  Vec's spare capacity (`read_to_end` never pre-zeroes; its documented
  contract appends all bytes read even on error). The cloud SASL_SSL path
  performs no payload zeroing at all.
- `NetworkReceive` allocates with `Vec::with_capacity` on try_read-capable
  transports (append model: `len` grows with arrival, `buffer_bytes_read ==
  len`), keeps the zeroed model on the async mock fallback; `complete()`
  compares `bytes_read` against `requested_buffer_size` (equivalent in both
  models).
- New SSL unit test `test_try_read_append_limit_then_eof` (limit cap,
  WouldBlock mapping, EOF).

## Validation matrix (vs Phase-27's 76.4% single-arm)

Four arms on the EC2 rig, same cluster `lkc-devcnvw72vz`, same `/proc`
method: (A) 200k/1KB latency-tuned, (B) 200k 64KB-batched, (C) big-batch
2KB/200p/4MB (vs the 1-hour 41.6% baseline on this cluster), (D) 5k low-rate
idle-regression check (poll-loop changes fail at idle, not under load —
Phase 23/24 lesson). Results recorded in `client-comparison-results.md`.
