---
name: Phase 5c-2 Selector review patterns
description: Tokio Selector translation gotchas — idle-expiry update scope, Java clear() ordering, ignored connect parameters, LRU O(n) with VecDeque, single-task vs per-channel deviation acceptance
type: project
---

Phase 5c-2 (Selector Tokio rewrite) review notes — load-bearing for
Phase 5d NetworkClient and any future review of single-task event-loop
translations of Java NIO code.

## High-yield review axes for Tokio NIO-Selector translations

1. **Idle-expiry update scope**. Java's `pollSelectionKeys` updates
   `idleExpiryManager.update(nodeId, now)` *only for keys that were
   ready in this poll* — i.e. channels with actual I/O activity.
   A Rust translation that updates ALL open channels at the bottom of
   `poll()` (regardless of activity) silently defeats
   `connections.max.idle.ms`: in production with NetworkClient calling
   `poll(...)` continuously, every channel's `last_active_ns` is
   refreshed every poll, so `poll_expired_connection` never returns it.
   **The single-poll idle test passes anyway** because it does
   `connect → sleep WITHOUT polling → poll once`, so the channel's
   `last_active_ns` is genuinely 150ms stale at the time of the expiry
   sweep. A regression test that drives `poll(0)` continuously through
   the idle window catches the bug; a single-shot test does not.

2. **`clear()` order vs `failedSends` short-circuit**. Java's
   `Selector.clear()` iterates `closingChannels` first, calls
   `failedSends.remove(channel.id())` (consuming the entry) — this
   short-circuits the read attempt for the closing-and-failed channel.
   Then the remaining `failedSends` are drained into `disconnected`.
   A Rust translation that runs the failed_sends drain BEFORE
   `process_closing_channels` empties the set first; the
   `failed_sends.contains(&id)` check in process_closing_channels
   always returns false and the closing channel gets an extra read
   attempt that Java would have skipped. Eventual outcome is the same
   but the order/semantics deviate. **Always check `clear()` ordering
   against the per-poll-output drain.**

3. **`Selectable.connect(send_buffer_size, receive_buffer_size)`
   parameters**. Java's `configureSocketChannel` applies these via
   `socket.setSendBufferSize` / `setReceiveBufferSize` when not equal
   to `USE_DEFAULT_BUFFER_SIZE`. Rust translations that prefix the
   parameters with `_` (unused) silently break `send.buffer.bytes` /
   `receive.buffer.bytes` config keys. Tokio 1.18+ exposes
   `set_send_buffer_size` / `set_recv_buffer_size` on `TcpStream`.

4. **TCP keepalive**. Java sets `SO_KEEPALIVE` unconditionally
   (`socket.setKeepAlive(true)`). Rust's `tokio::net::TcpStream`
   doesn't expose keepalive directly; `socket2` or `TcpSocket` is
   the standard pattern. Practical impact is muted (default kernel
   timer is hours) but it's a documented Java behavior divergence.

5. **`IdleExpiryManager` data structure**. Java uses
   `LinkedHashMap` with `accessOrder=true` — O(1) re-ordering on
   every touch via the doubly-linked map node. A Rust translation
   that uses `HashMap` + `VecDeque` does O(n) linear scan +
   removal on every `update` call. Combined with Issue 1 above
   ("update every channel every poll"), this becomes O(n²) per
   poll. Recommend `linked_hash_map` crate or a `BTreeMap<i64,
   i32>` keyed by last-active timestamp.

## Tokio cancel-safety review checklist (CLAUDE.md 9.6)

For every `tokio::select!` block in a single-task event loop:

1. Does any arm body mutate state if the future is cancelled
   mid-execution? (Side effects must run only on the resolved value
   AFTER the await, not inside the awaited future.)
2. Is the awaited future cancellation-safe?
   - `mpsc::Receiver::recv` — yes
   - `tokio::time::sleep` — yes
   - `TcpStream::readable` — yes
   - `read_exact` / `write_all` — NO (these advance partial state on
     cancellation)
3. Is `biased;` used when ordering matters?
4. Are there `MutexGuard` / `RwLockGuard` held across `.await`?
5. Are there `&mut` borrows on `self.field` that overlap with
   `&mut self` calls on the resolved value?

## Architectural deviation: when to accept single-task vs. per-channel

The Phase 5c-2 brief prescribed per-channel read/write tasks driven by
mpsc; the actor chose single-task because:
1. rustls `ClientConnection` (Phase 5b-2) needs both halves of the TCP
   stream — `into_split()` would force redesigning SslTransportLayer.
2. Per-channel write tasks would race the per-poll `completed_sends`
   drain that Java guarantees as ordered.
3. `try_read` / `try_write_vectored` are non-blocking syscalls — async
   hop only needed for the connect SYN-ACK.

This deviation is defensible **only if**:
- Every `select!` in the design follows CLAUDE.md 9.6.
- No `MutexGuard` is held across `.await`.
- Java's "this class is not thread safe" contract is preserved
  (`&mut self` everywhere).
- The trade-off (sequential per-poll iteration vs. parallel async
  tasks) is documented.

When reviewing similar deviations: don't flag it as Blocking just
because it deviates from the prompt — flag it as Blocking only if
it actually breaks contract or leads to a concrete bug.

## Skip-list audit pattern

The actor often maps multiple Java tests to one Rust test that
covers the same codepath via different fixturing. Verify each
mapping by:
1. Reading the Java test body — what specifically is asserted?
2. Reading the Rust substitute — does it exercise the same codepath?
3. If the Java test uses Mockito mocks for fault injection
   (`when(...).thenThrow(...)` etc.), the Rust equivalent often uses
   a trait stub (e.g. `AlwaysFail` ChannelBuilder). Verify the stub
   surfaces the error in the same place the mock would.

Tests skipped with "kernel-buffer-dependent timing is flaky" should
be verified — usually the Java test exercises a *different* codepath
that's already covered by a more deterministic Rust test, but
sometimes the rationale masks a coverage gap.

## Patterns I verified safe

- `tokio::select! { biased; event = self.connect_rx.recv() => {...},
  _ = tokio::time::sleep(timeout) => {} }` with no MutexGuard, no
  state mutation in arm body before resolution, both arms
  cancellation-safe — accepted.
- Connect-task pattern: short-lived `tokio::spawn` per `connect()`
  call, posts result to internal mpsc, JoinHandle stored only for
  abort on close — preserves Java's OP_CONNECT semantics.
- `wakeup()` as no-op — defensible because Tokio tasks wake naturally
  when their futures resolve.
- Per-poll output collections cleared at top of next `poll()` —
  matches Java semantics.
- `failed_sends` Vec drained into `disconnected` at start of poll —
  matches Java's `clear()` (but watch for ordering vs.
  `closingChannels`, see #2 above).

## Patterns to flag in Phase 5d NetworkClient review

- `NetworkClient::poll` will likely call `selector.poll(timeout)` and
  drain `selector.completed_sends/receives/disconnected/connected`.
  Verify the consumer respects the "cleared at top of next poll"
  contract — taking ownership of the slices into per-call
  `ClientResponse` Vecs.
- `NetworkClient::handleWakeup` calls `selector.wakeup()`. Verify the
  Rust `wakeup` no-op stub doesn't break a control flow that Java
  relied on (e.g. another thread waking the polling thread mid-sleep).
- Verify NetworkClient's connection-state tracking (`ConnectionStates`)
  doesn't double-track the same state the Selector tracks (Selector
  has `channels`, `closing_channels`, `connect_tasks`; NetworkClient
  layers connection backoff/timeout on top — they should not duplicate
  channel-state tracking).
