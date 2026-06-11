# Phase 30 — Per-channel wakers: `selectedKeys()` semantics on tokio (O(ready) readiness dispatch)

**Milestone-8 / Phase-30** · Agent number **N = 30**

## Why

Post-Phase-27/28 cloud profile (76.2% CPU @200k SASL_SSL latency-tuned, CRC on;
librdkafka same-work reference 66.7%): the largest remaining addressable block
is the readiness machinery — `tokio::runtime::io::registration::Registration::
poll_ready` **5.8%** + `Notify::poll_notified` 0.8% + `Selector::channel_interest`
1.0% + readiness-sweep mechanics inlined into `NetworkClient::poll` self-time.

Root cause: tokio's readiness API registers ONE task waker per socket
registration and, on wake, cannot tell us **which** socket fired. Java NIO's
`selector.select()` returns `selectedKeys()` — O(ready). To emulate that,
Phases 22–24 built a sweep: every WAIT entry polls **all** channels × 2
interests (`poll_channel_readiness`, `selector.rs:762`), and every wake
re-sweeps. At ~7k wakeups/s × 24 channels that is ~10⁶ `poll_ready` calls/s of
pure dispatch overhead.

**This phase implements `selectedKeys()` on top of tokio** with per-channel
wakers — the `FuturesUnordered` mechanism, fully idiomatic, no new
dependencies, **no socket-type change, no TLS/SASL/connect changes, no
CLAUDE.md §8 deviation**. (The full-mio conversion was evaluated and parked:
it would force rewriting the TLS-handshake driver, the whole SASL
authenticator, and the connect path into poll-driven state machines in one
phase — see `design/current/client-comparison-results.md` Phase-28 section
and the Phase-29 invalidation for the surrounding context.)

## The design

### Data structures (new, inside `selector.rs`)

```rust
/// Shared between the Selector and every per-channel waker.
struct ReadyQueue {
    /// Channels whose registered interest fired since the last drain.
    /// (token, is_write). Pushed from `ChannelWaker::wake` (same thread in
    /// practice — the current_thread reactor fires wakers during park — but
    /// Waker is Send so keep it a std::sync::Mutex; it is uncontended).
    fired: std::sync::Mutex<Vec<(u32, bool)>>,
    /// Root waker of the WAIT future, registered each poll. Mutex<Option<Waker>>
    /// (do NOT add a deps for AtomicWaker). Take-and-wake outside the lock.
    root: std::sync::Mutex<Option<std::task::Waker>>,
}

/// One per channel per interest direction, created at registration time and
/// cached (so arming is a cheap `Waker::clone`, no per-poll allocation).
struct ChannelWaker { token: u32, is_write: bool, queue: Arc<ReadyQueue> }
impl std::task::Wake for ChannelWaker { /* push (token,is_write); wake root */ }
```

Selector additions:
- `ready_queue: Arc<ReadyQueue>`,
- per-channel registration entry: `token: u32` (monotonic `next_token`),
  cached `read_waker: Waker`, `write_waker: Waker`, and two arming flags
  `armed_read: bool`, `armed_write: bool`,
- `token_to_id: FxHashMap<u32, Arc<str>>` (token → channel id),
- `interest_dirty: FxHashSet<Arc<str>>` — channels whose interest may have
  flipped ON since they were last armed (see "dirty sites" below).

The simplest placement for token/wakers/armed-flags is a small per-channel
side-struct in a `FxHashMap<Arc<str>, ChannelArming>` owned by the Selector
(do NOT put tokio types inside `KafkaChannel` — keep the channel type
transport-focused). Field shape is the Actor's choice; behavior below is not.

### Arming discipline (the correctness core)

A channel-interest is **armed** when `poll_transport_readable/_writable` was
last called with that channel's cached waker and returned `Pending`. tokio
then holds the waker until the readiness event fires it (waker is consumed),
or until `try_read`/`try_write` observes `WouldBlock` (readiness cleared,
prior `Ready` results stale). State machine per (channel, direction):

- **arm**: compute `channel_interest` (the EXACT Phase-23 predicate,
  `selector.rs:730` — byte-for-byte; any divergence re-introduces busy-spin
  or stall). If interest: `poll_transport_*(Context::from_waker(&cached))`.
  `Ready` → channel goes into this iteration's ready set (process now);
  `Pending` → `armed_* = true`.
- **fire** (`ChannelWaker::wake`): push `(token, dir)`, `armed_* = false`
  (flag cleared when the WAIT drains the queue, since wake happens outside
  `&mut Selector`), wake root.
- **consume**: pass-1 processes the channel (`try_read` until `WouldBlock` —
  the existing drain). After processing, **re-arm** (the channel is in the
  processed set).

**Re-arm set per iteration** = (channels processed in pass-1 this iteration)
∪ (`interest_dirty` drained) ∪ (newly registered / immediately-connected).
Channels armed-and-not-fired stay armed — zero per-iteration cost. That is
the whole O(ready) win.

### Dirty sites — every mutation that can flip `channel_interest` from false to true

This list is exhaustive TODAY; the Critic must re-verify it against
`channel_interest`'s inputs (`ready()`, `has_bytes_buffered()`, `is_muted()`,
`has_completed_receive`, `explicitly_muted_channels`, `has_send()`,
`has_pending_writes()`, `is_connected()`):

1. `Selector::send(...)` — queues a send → `want_write` may turn on.
2. `unmute(...)` / `explicitly_muted_channels` removal → `want_read` on.
3. `clear()` / `drain_completed_receives()` — clears `has_completed_receive`
   → `want_read` on for those channels' ids (clear runs at poll top; cheapest
   correct form: mark ALL channels dirty whose completed receive was cleared).
4. `connect(...)` registration + `immediately_connected_keys` handling.
5. Handshake/auth state transitions (`prepare()` / post-handshake ready
   transition) — these channels were processed in pass-1 that iteration, so
   the processed-set re-arm covers them; NO separate hook needed (document).
6. `mute(...)`: interest turns OFF — no dirty mark needed; a stale armed
   waker firing for a now-uninterested channel is harmless (drained id gets
   interest-checked before processing; not re-armed; net effect one spurious
   loop pass).

**Rule for the Actor**: when in doubt whether a site can flip interest ON,
mark dirty — a spurious arm costs one `poll_ready`; a missed arm is a
data-stall bug (the Phase-24 risk-#1 class).

### The WAIT future

`poll_channel_readiness` (sweep) is REPLACED by:

```text
poll_fn(|cx| {
    register root waker (replace; cheap clone_from / will_wake check);
    arm everything in the re-arm set (first poll of this WAIT only);
    drain fired queue into ready_ids (mapping token→id, dropping closed ids);
    if ready_ids non-empty (or anything was Ready during arming) → Ready
    else → Pending
})
```

The surrounding `select! { biased; notify.notified() => wakeup, WAIT => {},
sleep_until(deadline) => {} }` (`selector.rs:1142-1155`), the
deferred-wakeup/mid-receive logic, `process_all` fallbacks, `data_in_buffers`
timeout-0 path, and the Phase-26 `made_progress` break are ALL UNCHANGED.
`ready_ids`/`ready_scratch` keeps its Phase-24 role; only its producer
changes (fired-queue drain instead of sweep).

Cancellation safety: the WAIT body's only effects are waker registration and
queue drains into selector-owned scratch — dropping it on wakeup/deadline
loses nothing **provided** drained-but-unprocessed ready ids are NOT lost:
drain into the persistent `ready_scratch`, never into a future-local. A
wakeup/deadline that cancels the WAIT after a drain must leave those ids
processed on the next iteration (the existing `process_all = true` deferred
-wakeup arm and the next WAIT's queue/`ready_scratch` state must together
guarantee this — Critic: verify this explicitly, it is the subtlest point).

### `has_interested_channel` (`selector.rs:708`)

Currently sweeps all channels per WAIT to pick the select! form. Replace with:
interested ⇔ (any armed flag set) ∨ (re-arm set non-empty) — O(1) bookkeeping
(maintain a counter). Behavior must match the old predicate exactly when
deciding between the two select! forms.

## What does NOT change

- `channel_interest` predicate text (`selector.rs:730`) — reused verbatim.
- Wakeup primitive (`Arc<Notify>`), §10 poll-to-completion, §11 token
  rotation, `run_once` — untouched.
- Transports, TLS handshake, SASL authenticator, connect path — untouched.
- Public API, all trait surfaces — untouched.
- consumer-threading.md rules — no amendments needed (this strengthens the
  Java-NIO analogy; document in code comments referencing selectedKeys()).

## Tests (mutation-tested, Phase-24 style — revert the mechanism, watch them fail)

1. **Send-after-arm wake** (dirty-site #1): connect, settle (3× `poll(20)` —
   see Phase-24/26 notes on `immediately_connected_keys` settling), let a
   WAIT park with read-only interest, then queue a send and `poll` — the
   write must complete without waiting out the deadline.
2. **Unmute redelivery** (dirty-sites #2/#3): mute a channel, have the peer
   send, poll (no delivery), unmute, poll(timeout) — data delivered promptly
   even though the readiness event fired while muted (stale-fire + dirty
   re-arm path).
3. **Skip-idle preserved**: Phase-24's `CountingTransportLayer` test —
   0 `try_read`s on idle channels — must still pass unchanged.
4. **No-busy-spin preserved**: idle `poll(300ms)` parks ~full deadline
   (Phase-26 `SinkServer` pattern); also assert the WAIT does not respin on
   stale fired entries (a muted channel's pending fire must not busy-loop).
5. **Multi-channel ready-set exactness**: N echo channels, send on K —
   exactly K processed (extend the Phase-24 test).
6. **Mid-receive deferred wakeup**: existing tests unchanged and green.
7. Full suite: all 1767 lib tests + all test targets compile.

## Validation gates (Manager runs on the EC2 rig after Critic CLEAN)

A: 200k/1KB latency-tuned vs **76.2%** (expect ~−4 to −7pp; `poll_ready`
symbol < 1% in perf); B: 64KB vs 46.2%; C: big-batch vs 41.7%; D: 5k
low-rate vs 2.9% (busy-spin guard); plus a KIP-848 join over SASL_SSL
(fresh group) — the join path must show no stall across 5 consecutive joins.

## Commit plan

1. Infra + integration: ReadyQueue/ChannelWaker/arming + WAIT replacement +
   dirty sites, all existing tests green.
2. New regression tests (mutation-checked).

Keep `rustfmt --edition 2024` on touched files only; repo has pre-existing
dirty diagnostics (`completed_fetch.rs`, `fetch_collector.rs`, `Cargo.toml`,
comparison doc) — do NOT stage them.
