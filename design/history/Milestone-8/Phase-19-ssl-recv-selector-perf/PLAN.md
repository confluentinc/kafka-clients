# Phase 19 — SSL receive-path + selector hot-path CPU optimizations

**Milestone-8 / Phase-19** · Agent number **N = 19**

Two CPU optimizations on the consumer receive/poll hot path, identified by a
`perf` profile of the Rust consumer under **SASL_SSL @300k** on an in-region
EC2 vs native librdkafka-C. Findings (profile): `SslTransportLayer::read` =
21.4% of CPU but **AES-GCM decrypt only ~4.6%**; the rest is rustls deframer
churn (`DeframerVecBuffer::read` 11.6%) + many small `read()` syscalls (~19%
kernel) + per-chunk boxed-future allocations. Plus `String::clone` = 2.65%
(→ malloc/free) on the selector poll path. Crypto is fine; the cost is *plumbing*.
Full analysis: see the session memory `cloud_perf_ec2_setup.md` (PROFILE section).

Neither is a correctness bug — both are hot-path inefficiencies (CLAUDE.md §11/§12,
consumer-threading §27). Implement Fix 2 first (low-risk), then Fix 1.

## Fix 2 (do first) — `Arc<str>` channel ids in `Selector`

**Problem:** `Selector` (`src/common/network/selector.rs`) keys channels by
`String` (`channels: HashMap<String, KafkaChannel>`, plus several
`HashSet<String>` / `Vec<String>` / `HashMap<String, _>` tracking sets) and
**clones the channel id on the poll hot path** every iteration
(`channel_id.to_string()`, `keys().cloned()`, `channel_id.clone()`, and in the
readiness-future collection path). Profile: `String::clone` 2.65%, almost all in
`malloc`/`_int_malloc`/`cfree`. With ~tens of broker channels × thousands of poll
iterations/sec, this is steady allocator churn. Helps plaintext AND SSL.

**Fix:** change the channel-id key/element type from `String` to `Arc<str>`
throughout `Selector` (the `channels` map key, the tracking
`HashSet`/`Vec`/`HashMap` of ids, and the `connected`/`failed_sends`/
`channels_with_buffered_read`/`disconnected`/`closing_channels` collections).
Clones become refcount bumps (no allocation). Public method signatures that
take `&str` should stay `&str` (callers pass `&str`; look up via `&str` works
against `Arc<str>` keys with `Borrow<str>`). Per CLAUDE.md §11 ("identifiers
cloned on every message: prefer `Arc<str>`").

**Constraints:** behavior-identical; `HashMap<Arc<str>, _>` lookups by `&str`
must still work (they do — `Arc<str>: Borrow<str>`). Don't change the public
`Selectable`/`Selector` API shape where it takes `&str`.

## Fix 1 (do second) — synchronous `try_read` for `SslTransportLayer`

**Problem:** `NetworkReceive::read_from` Phase 3 (`network_receive.rs`) has a fast
**plaintext** path — a tight sync `loop { channel.try_read(...) }` that drains the
whole available payload in one `read_from` call — gated on
`channel.supports_try_read()`. SSL returns `false` (the `transport_layer.rs:188`
default), so SSL takes the `else` branch: **one `channel.read(...).await` per
`read_from` call, no loop.** `SslTransportLayer::read`
(`ssl_transport_layer.rs:373`) returns `Pin<Box<dyn Future>>` and does one
`read_tls` (one socket read into rustls' deframer) + `process_new_packets` +
drain. So a large fetch over TLS = **dozens of `read_from` re-entries, each
allocating two boxed futures + one socket read + deframer work + a selector
round-trip per chunk.** That is the bulk of the SSL gap.

**Key insight:** rustls does NOT require async here. `read_tls` already uses a
**non-blocking** socket adapter (`TryReadAdapter(&c.tcp)`), and
`process_new_packets()` + `reader().read()` are synchronous. SSL only takes the
slow branch because it advertises `supports_try_read() == false`.

**Fix:** implement a synchronous `try_read` on `SslTransportLayer` and set
`supports_try_read() => true`:

```rust
fn try_read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
    // Ready(c) only; Handshaking/Closed -> WouldBlock/NotConnected as today.
    // 1. non-blocking read_tls via TryReadAdapter (WouldBlock is fine);
    //    track tcp_eof on Ok(0).
    // 2. process_new_packets() (map err -> io::Error::other).
    // 3. drain c.conn.reader().read(dst):
    //      n>0            -> Ok(n)
    //      n==0 && tcp_eof -> Ok(0)        (EOF: plaintext drained AND socket closed)
    //      n==0           -> Err(WouldBlock) (come back later)
    //      WouldBlock     -> tcp_eof ? Ok(0) : Err(WouldBlock)
}
```
This is the **same logic as the existing async `read`**, just sync (no `Box::pin`,
no `.await`). The existing async `read` stays as-is (still used during the
`Handshaking` state / wherever a future is needed). Once `supports_try_read()` is
true, the existing Phase-3 tight-drain loop handles SSL identically to plaintext:
**one `read_from` call drains all currently-available plaintext in a sync loop —
no per-chunk boxed futures, no per-chunk selector round-trip**, and rustls
deframes/decrypts larger batches at once.

## CRITICAL review/correctness invariants (Fix 1 is in the join-stall-sensitive area)

This touches the same network/selector code the Phase-merge critic flagged. The
following MUST be preserved (consumer-threading.md §10, and the
`design/current/consumer-join-stall-rootcause.md` contract):

  - **`has_bytes_buffered()` for SSL must still reflect rustls buffered plaintext.**
    The selector uses it to set `channels_with_buffered_read` (re-poll without
    waiting for socket readiness). The sync drain loop must not strand decoded
    plaintext that `has_bytes_buffered()` then reports — i.e. the loop drains
    until `WouldBlock`, and any leftover buffered plaintext (dst full) is still
    reported by `has_bytes_buffered()` so the channel is re-polled.
  - **No reintroduction of a blocking call**: `try_read` must be truly
    non-blocking (uses `TryReadAdapter` non-blocking `read_tls`; never `.await`,
    never a blocking socket read).
  - **EOF semantics**: socket-closed-with-no-plaintext must surface `Ok(0)` →
    the receive layer turns it into the `UnexpectedEof` it does today; partial
    plaintext then EOF must not be lost.
  - **Wakeup / `Notify` / drain machinery (§10) unchanged** — do not alter the
    selector's `poll()` wakeup path, `any_channel_mid_receive()`,
    `deferred_wakeup`, or the network-poll cancel-safety. Only add the SSL
    `try_read` + flip `supports_try_read()`.
  - The `MAX_TLS_COALESCE` write path and handshake path are untouched.

## Tests (DoD)

  - All existing `selector.rs`, `ssl_transport_layer.rs`, `network_receive.rs`,
    `kafka_channel.rs`, `consumer_network_thread.rs` unit tests pass unchanged.
  - Add a unit test for `SslTransportLayer::try_read`: with buffered decrypted
    plaintext it returns it synchronously; with none it returns `WouldBlock`;
    socket-EOF-after-drain returns `Ok(0)`. (Use the existing SSL test scaffolding
    / a loopback rustls pair if present; otherwise assert via the transport trait.)
  - `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check`
    green for the whole workspace.
  - The existing SSL/SASL integration test (`tests/integration/ssl_sasl_test.rs`)
    and the Phase-17 `sasl_ssl_consumer_test.rs` still pass (Docker-gated; compile
    at minimum, run if Docker available).

## Out of scope / deferred
  - The `memcpy` in `FetchResponse::response_data` (~4.7%) — a separate receive-path
    copy audit; not in this phase.
  - Any change to the producer send path.

## Critic 19 focus
  - Behavior parity: SSL receive produces identical bytes; no records lost/dup;
    EOF + partial-plaintext handled.
  - **Join-stall / §10 invariants intact** (wakeup, drain, cancel-safety, the
    `has_bytes_buffered` → `channels_with_buffered_read` re-poll loop) — the
    highest-risk check.
  - `try_read` is genuinely non-blocking (no `.await`, no blocking socket op).
  - `Arc<str>` change is behavior-identical; `&str` lookups against `Arc<str>`
    keys still work; no new per-poll allocation introduced elsewhere.
  - Tests assert the new `try_read` paths; existing tests unchanged in intent.

## Validation (Manager, post-merge — not the Actor's job)
Re-profile + re-run the EC2 cloud 200k/300k Rust-vs-librdkafka-C comparison;
expect SSL CPU to drop materially toward the plaintext ratio, throughput/latency
unchanged.
