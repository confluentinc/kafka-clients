# Phase 23 — Non-allocating, persistent readiness wait in the Selector poll loop

**Milestone-8 / Phase-23** · Agent number **N = 23**

## Why

A post-Phase-21/22 CPU-weighted `perf` profile of the consumer on EC2 (Confluent
Cloud, SASL_SSL, 200k, KIP-848) shows the cloud CPU gap to the **Java** client
(the parity reference — Java is also a single background thread) is ~15–25%
(Rust 102–112% vs Java 89% latency-tuned; Rust 74% vs Java 62% at 64KB batching).
librdkafka's ~49% is its per-broker-thread model, which neither Java nor this
Java-faithful client uses — so it is NOT the target; **Java is.**

The mechanism: the bg I/O thread blocks ~once per poll (measured 3,681 voluntary
ctxt-switches/s) and on each no-progress wait calls
`Selector::collect_readiness_futures()` (`selector.rs:705`), which **allocates a
fresh `Vec<Pin<Box<dyn Future>>>` and one boxed readiness future per channel**,
then `select_all`s them. With ~9 broker connections that is ~33k heap
allocations/s plus boxed-future poll + atomic churn (the profile's malloc ~6–9%,
`scheduled_io::Readiness::poll` ~2%, atomics ~9%). Java pays none of this: it
registers each channel with one `Selector` once and calls `select(timeout)`.

`tokio::net::TcpStream` (tokio 1.52) exposes poll-style readiness
`poll_read_ready(&self, cx) -> Poll<io::Result<()>>` and `poll_write_ready(...)`,
so we can wait on all channels in a **single non-allocating future** that mirrors
Java's persistent `Selector.select()`.

## Goal

Replace the per-poll `Vec<Box<dyn Future>>` + `select_all` readiness wait with a
single non-allocating future that polls every interested channel's readiness in
one `poll`. **Behavior must be identical** — same interest semantics, same `§11`
wakeup delivery, same `§10` poll-to-completion cancel-safety, same join behavior.
The only observable change is lower CPU / fewer allocations.

## Design

### 1. Extend the `TransportLayer` trait (`transport_layer.rs`)

Add two poll-style, side-effect-free readiness methods (sync `fn`, NOT boxed):

```rust
/// Poll-style read-readiness, mirroring Java NIO's persistent selector
/// registration. Side-effect-free: registers the waker and returns; does NOT
/// consume bytes or mutate connection state, so it is cancel-safe to drop.
fn poll_readable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>>;
fn poll_writable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>>;
```

Keep the existing async `readable()`/`writable()` (still used elsewhere, e.g.
handshake/connect paths) — do NOT remove them in this phase.

Impls:
  - **`plaintext_transport_layer.rs`**: delegate to `self.stream.poll_read_ready(cx)`
    / `poll_write_ready(cx)` on the underlying `TcpStream`.
  - **`ssl_transport_layer.rs`**: delegate to the inner `tcp.poll_read_ready(cx)` /
    `tcp.poll_write_ready(cx)` (readiness is on the underlying socket; decrypted-
    plaintext buffering is handled by the selector via `has_bytes_buffered()`,
    see below). Return `Poll::Ready(Ok(()))` if state is `Closed`/handshaking in a
    way consistent with the current `readable()` future (match its semantics).
  - **Mock transports** in `kafka_channel.rs` and `network_receive.rs` tests:
    implement the two methods consistent with their existing `readable()`/
    `writable()` (e.g. `Poll::Ready(Ok(()))` if they currently return ready).

Add the corresponding pass-throughs on `KafkaChannel`
(`poll_transport_readable`/`poll_transport_writable` or similar) mirroring the
existing `transport_readable()`/`transport_writable()` wrappers.

### 2. Replace the readiness wait in `Selector::poll` (`selector.rs`)

Delete `collect_readiness_futures()` and the `select_all(readiness_futs)` arm.
Replace with a single future built via `std::future::poll_fn(|cx| { ... })` that,
on each `poll`, iterates `&self.channels` and for each channel:

  - Computes the SAME interest as the current `collect_readiness_futures`:
    - `want_read = channel.ready() && (channel.has_bytes_buffered() || !channel.is_muted())
       && !self.has_completed_receive(id) && !self.explicitly_muted_channels.contains(id)`
    - `in_handshake = channel.is_connected() && !channel.ready()`
    - `want_write = (channel.has_send() && channel.ready()) || (in_handshake && channel.has_pending_writes())`
  - If `channel.has_bytes_buffered()` for any interested-readable channel →
    return `Poll::Ready(Ok(()))` immediately (decrypted plaintext already
    available; do not wait on the socket). (Note: the existing code already sets
    `effective_timeout = 0` when `data_in_buffers`, so this mainly guards the
    in-loop case — preserve current behavior.)
  - For read interest (`want_read || in_handshake`): call
    `channel.poll_transport_readable(cx)`; if `Ready` → return `Ready(Ok(()))`.
  - For write interest: call `channel.poll_transport_writable(cx)`; if `Ready`
    → return `Ready(Ok(()))`.
  - Register wakers for all polled channels (achieved naturally by calling each
    `poll_*` with `cx` even after one returns Pending — but once we return
    `Ready` we stop; that's fine, the next loop re-polls).
  - If no channel is ready after the sweep → `Poll::Pending`.

The enclosing `select!` keeps its existing arms and ordering:
```rust
tokio::select! {
    biased;
    _ = notify.notified() => true,         // §11 wakeup — unchanged
    _ = readiness_wait => false,           // the new single poll_fn (was select_all)
    _ = tokio::time::sleep_until(dl) => false,
}
```
When there are no interested channels, keep the existing `notify`-vs-`sleep`-only
form (mirror the current `readiness_futs.is_empty()` branch).

### 3. Invariants the refactor MUST preserve (Critic focus)

  - **`§10` cancel-safety**: the readiness wait is side-effect-free (only waker
    registration). The non-cancel-safe network poll (`poll_channel_reads` →
    `try_read`, `initiate_connect`) stays in pass-1, OUTSIDE the `select!`. Dropping
    the readiness future on a wakeup/deadline loses nothing. Do NOT move any
    network I/O or connection-state mutation into the wait future.
  - **`§11` wakeup**: `notify.notified()` arm unchanged; a `wakeup()` before/while
    parked must still return the poll promptly (the pre-stored-permit behavior).
    `deferred_wakeup` / `any_channel_mid_receive()` handling unchanged.
  - **Interest parity**: the per-channel read/write interest computed in the
    `poll_fn` must be byte-for-byte the same predicate as today's
    `collect_readiness_futures`, including the muted / completed-receive /
    handshake / pending-writes conditions. A divergence here re-introduces the
    busy-spin or the join stall.
  - **No busy-spin**: if a channel is interested but never becomes ready, the
    `poll_fn` returns `Pending` and the task parks on the registered waker (or the
    deadline) — it must NOT return `Ready` spuriously (which would spin the loop).
  - **Waker correctness**: every channel that returns `Pending` must have had its
    waker registered via the `cx` passed to its `poll_*`, so the task is woken
    when ANY of them becomes ready. (tokio's `poll_read_ready` registers on the
    provided `cx`.) Returning early on the first `Ready` is fine.
  - **Join path**: KIP-848 join goes through handshaking channels
    (`in_handshake` read interest) — verify a fresh consumer still joins on cloud
    and locally (the join-stall regression class).

### 4. Out of scope
  - Removing the async `readable()`/`writable()` methods (still used by
    connect/handshake paths).
  - Buffer-reuse / `NetworkReceive` pooling (separate follow-up).
  - Any threading-model change (parity with Java's single bg thread is mandatory).
  - SSL/rustls internals.

## Tests (DoD)
  - All existing tests pass unchanged: `cargo test` (esp. `common::network::selector`,
    `network_client`, `ssl_transport_layer`, `plaintext_transport_layer`,
    `network_receive`, `kafka_channel`).
  - Add a selector unit test that exercises the new wait path: a channel that
    becomes readable wakes the parked `poll`; a `wakeup()` returns the parked
    `poll` promptly; an idle muted channel does not spin (poll returns on deadline,
    not immediately).
  - `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check` green.
  - Docker-gated integration tests at least compile; run the `plaintext_consumer_*`
    if Docker available.

## Validation (Manager, post-review)
  - Re-run the EC2 cloud SASL_SSL 200k sweep (default + 64KB) and compare CPU +
    voluntary ctxt-switch rate + p99/p999 against the Phase-22 baseline
    (Rust 102%/74%, Java 89%/62%). Expect Rust CPU to move toward Java with
    unchanged throughput/latency and fewer malloc/atomic samples in the profile.

## Critic 23 focus
  - `§10` cancel-safety preserved (no side effects in the wait; network poll stays
    in pass-1); `§11` wakeup semantics identical; interest predicate byte-identical
    to the old `collect_readiness_futures`; no busy-spin; waker registration covers
    all Pending channels; join (handshaking read interest) still works.
  - No `Pin<Box<dyn Future>>` per poll on the wait path (the whole point); the
    `poll_fn` must not allocate per channel.
  - The two new trait methods are sync `fn` (poll-style), not `#[async_trait]`,
    consistent with the per-iteration hot-path rule.
