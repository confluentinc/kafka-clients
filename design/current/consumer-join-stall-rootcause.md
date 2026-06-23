# Consumer join stall — root cause, proof, and fix

Status: **diagnosed, proven, Option A applied (2026-06-05).** See §7 for the
applied change.

Symptom: `AsyncKafkaConsumer` intermittently never joins the group — `poll()`
returns no records for tens of seconds to indefinitely. Reproduced with both the
`consumer-perf` benchmark and the existing `src/bin/consumer_test.rs`. Surface
signature (with `env_logger` wired in): a one-time
`FindCoordinator … server disconnected before a response was received`, then
`network_client_delegate: Node is not ready … FindCoordinator` repeating forever
while the member stays in `JOINING`.

## TL;DR

`delegate.poll_default()` (the network poll, which performs **connection
setup**) is run inside a `tokio::select!` whose other two arms (`wakeup`
token, Phase-14 `event_notify`) **cancel** it. `initiate_connect` sets a node to
`Connecting` (`connection_states.connecting()` — a persisted side effect) and
**then `await`s** `current_address()` / `selector.connect()`. When the `select!`
cancels `poll_default` at that await, the `initiate_connect` future is **dropped
mid-flight**: the node is left in `Connecting` but **the socket is never
created**. It then sits in `Connecting` (so `can_connect == false` and
`is_ready == false`) until the ~10 s connection-setup-timeout disconnects it,
retries, and can be cancelled again. When this strands the bootstrap/coordinator
connection, FindCoordinator/heartbeats never complete → the member never leaves
`JOINING` → the consumer never gets an assignment.

This is a textbook **CLAUDE.md §9.6.1** violation ("Never put operations with
side effects … inside a `select!` arm unless the future is cancellation-safe").
The Phase-14 change (commit `4ff5ad2`, "wake bg task on application-event
enqueue") made it severe: `event_notify` now fires on every enqueued app event,
cancelling `poll_default` constantly.

## The code

`src/consumer/internals/consumer_network_thread.rs:564` (`run_once`, Phase 4):

```rust
let token = self.wakeup_rx.borrow().clone();
let mut delegate_guard = self.network_client_delegate.lock().await;
tokio::select! {
    biased;
    _ = token.cancelled() => { /* wakeup or shutdown */ }
    _ = self.event_notify.notified() => { /* Phase-14: app-event enqueued */ }
    _ = delegate_guard.poll_default(poll_wait_time_ms, current_time_ms) => {}
};
```

`poll_default` → `NetworkClient::poll` → metadata `maybe_update` → `ready()` →
`initiate_connect` (`src/network_client.rs:370`):

```rust
async fn initiate_connect(&mut self, node, now) {
    let id = node.id_string();
    self.connection_states.connecting(id, now, node.host()); // ← side effect: state = Connecting
    match self.connection_states.current_address(id).await {  // ← await (suspension / cancel point)
        Ok(address) => { /* selector.connect(...).await — also an await */ }
        Err(e)      => { /* disconnected() + handle_server_disconnect */ }
    }
}
```

`can_connect` (`src/cluster_connection_states.rs:92`) only returns true when the
node `is_disconnected()` — so a node stuck in `Connecting` is never
re-initiated, and `ready()` keeps returning false → the delegate logs
"Node is not ready" forever.

## Proof (captured with temporary instrumentation, since reverted)

Logger note: the client logs through the `log` facade but **no binary installed
a backend**, so `RUST_LOG` was a no-op. After wiring `env_logger` into
`consumer-perf`, temporary `CONNSTATE-TEMP` traces were added to
`cluster_connection_states.rs`, `network_client.rs`, and `selector.rs` (now
reverted). Captured logs from a stuck run:

1. The dropped connect, with the cancellation in between:

   ```
   21:07:49.687  Network-client poll preempted by application-event notify
   21:07:49.687  connecting(id=-1, host=::1)                       ← state set to Connecting
   21:07:49.687  initiate_connect: resolving address for id=-1     ← about to .await current_address
   21:07:49.687  Network-client poll preempted by application-event notify   ← select! cancels poll_default HERE
   ── no "resolved … calling selector.connect", no socket.connect ── future dropped mid-await
   ```

2. The selector has **no channel** for the node while `connection_states` says
   `Connecting`:

   ```
   poll-block: eff_timeout=50ms readiness_futs=0 channels=[]   (repeated through the stall)
   ```

3. The node is freed only by the setup timeout, ~10 s later:

   ```
   ready->false node=-1 state=Connecting can_connect=false   (every second)
   …
   Disconnecting from node -1 due to socket connection setup timeout. The timeout value is 11836 ms.
   disconnected(id=-1) from state=Connecting
   ```

4. After the cancellation, `event_notify` re-fires in a tight loop
   (~30×/ms — a stored `Notify` permit re-resolves immediately), starving
   `poll_default` so the stranded node is never promptly re-initiated.

### Why it correlates with IPv6 / `localhost`

`localhost` resolves IPv6-first on this host (`[::1, 127.0.0.1]`); the broker
listens dual-stack on IPv6 `*:9092`, so **both** `::1` and `127.0.0.1` connect at
the OS level in <20 ms (verified with `nc`). The bootstrap/coordinator
connections therefore start with `::1`, and that first connect is the one
in-flight when an app-event/wakeup cancels `poll_default`. The IPv6 angle is
**incidental** — the true trigger is `select!` cancellation timing. When the
connect happens to complete before a cancellation, the join succeeds in ~0.5 s;
when cancelled, it stalls ~10 s per attempt, sometimes indefinitely.

### Observed variance (same binary, same broker)

| run | join time |
|---|---|
| consumer-perf (earliest) | 0.5 s ✅ |
| consumer-perf (latest) | 135 s / >150 s / >200 s |
| consumer_test (earliest) | >6 min, never |

## Divergence from Java

Java's `NetworkClient` never cancels its poll. `KafkaConsumer.wakeup()` calls
`Selector.wakeup()`, which makes the in-progress `nioSelector.select()` return
early **without losing state**. And Java's `selector.connect()`
(`SocketChannel.connect`) is **non-blocking** — it registers `OP_CONNECT` and
returns immediately, so `initiateConnect` is fully synchronous; there is no
suspension point between setting `connecting` state and registering the socket.

The Rust translation introduced two divergences that together cause the bug:
1. `selector.connect()` was made `async` (it `.await`s full TCP establishment),
   adding a suspension point inside `initiate_connect`.
2. The network poll was wrapped in a `select!` whose arms **cancel** it
   (per `consumer-threading.md` §10/§11), which is not equivalent to Java's
   `Selector.wakeup()` — tokio cancellation drops the in-progress future.

`consumer-threading.md` §10/§11 prescribes "wrap network poll in `select!`
against the wakeup token" and asserts it "must be cancel-safe" — but
`poll_default` is **not** cancel-safe, so the prescription itself is the trap.

## Proposed fix (NOT applied — for approval)

**Option A (recommended): deliver wakeup via the selector's notify; stop
cancelling `poll_default`.** Mirrors Java's `Selector.wakeup()`.

- The selector already has an internal wakeup: `Selector::wakeup()` →
  `self.notify.notify_one()`, and `Selector::poll` `select!`s on that notify
  (selector.rs:770-783), so it returns early **without** dropping state.
  `delegate.wakeup()` → `client.wakeup()` → `selector.wakeup()` already exists.
- Change `run_once` to simply `delegate_guard.poll_default(...).await` with **no
  cancelling arms**.
- Route the two signals through the selector notify instead of cancellation:
  - Wakeup/shutdown: `Self::wakeup()` already calls `delegate.wakeup()`. Add a
    `delegate.wakeup()` to `signal_close()` (today it only fires the token).
  - Phase-14 app-event enqueue: today the `ApplicationEventHandler` fires only
    `event_notify`. Make the enqueue also trigger the selector notify (e.g. give
    the handler a handle that calls `delegate.wakeup()`, or have `poll_default`
    /`Selector::poll` additionally wait on the shared `event_notify`). Then the
    blocking poll returns early cleanly and the next `run_once` drains the event.
- Net effect: `poll_default` always runs to a safe return point; connection-setup
  side effects are never dropped; the `event_notify` busy-loop disappears
  (the notify is consumed once per poll, not raced 30×).

**Option B (complementary hardening): make `initiate_connect` cancellation-atomic.**
Make `selector.connect()` non-blocking like Java NIO — register the channel in
`Connecting` and return immediately, letting the existing
`poll_channel`/`finish_connect` path complete the connection. Then there is no
`await` between `connection_states.connecting()` and channel registration, so
even a future cancellation cannot strand a node. (`finish_connect` already
handles the not-yet-connected case: `peer_addr() == NotConnected => Ok(false)`.)

Recommendation: **Option A** as the primary fix (smallest, most Java-faithful,
also kills the busy-loop), with **Option B** as defense-in-depth so the network
poll is genuinely cancel-safe regardless of future `select!` usage.

**Rules follow-up:** `consumer-threading.md` §10/§11 should be amended — the
network poll must NOT be made cancellable via `select!` arms (tokio cancellation
≠ Java `Selector.wakeup()`); wakeup must be delivered through the selector's
notify so in-progress connection/I-O state is preserved.

## How to reproduce / watch

```sh
# build the benchmark (installs env_logger)
cargo build -p consumer-perf --release

# run against a local broker; loop a few times to catch a stuck join
RUST_LOG=warn,confluent_kafka::consumer::internals::consumer_network_thread=trace,\
confluent_kafka::consumer::internals::coordinator_request_manager=debug \
  ./target/release/consumer-perf --topic t --no-create-topic --offset-reset earliest \
  --duration 3 --warmup-messages 50 --join-timeout 18
```

A stuck run shows repeated "Network-client poll preempted by application-event
notify" with the member never leaving `JOINING`.

## 7. Applied fix (Option A) — 2026-06-05

Three coordinated changes; the network poll is no longer cancelled, and wakeup
is delivered through the selector's wakeup primitive (Java `Selector.wakeup()`):

1. **`consumer_network_thread.rs` `run_once` Phase 4** — replaced the
   `tokio::select!` that *raced* `poll_default` against the wakeup token /
   `event_notify` with a **pinned** poll future driven by `&mut`. The signal
   arms (guarded by `if !poked`) no longer drop the poll; they fire a lock-free
   `Arc<Notify>` handle to the selector's wakeup and let the poll finish. The
   handle is grabbed via `delegate.wakeup_handle()` under the lock already held
   (the guard is borrowed by `poll_fut` for its duration, so `delegate.wakeup()`
   can't be called concurrently — the `Arc<Notify>` needs no lock).

2. **`selector.rs` `Selector::poll`** — a wakeup (`notify`) now **returns** the
   poll at the wait boundary instead of triggering one more pass and blocking to
   the deadline. This mirrors Java NIO `Selector.wakeup()` and is what makes the
   poke in (1) actually return the in-progress poll promptly (preserving the
   Phase-14 responsiveness without the cancellation hazard).

3. Plumbing for the lock-free handle: `Selectable::wakeup_handle()` (impl in
   `Selector` returns `self.notify.clone()`; `MockSelector` returns an unused
   handle), `KafkaClient::wakeup_handle()` (impl in `NetworkClient` delegates to
   the selector; `MockClient` unused), and `NetworkClientDelegate::wakeup_handle()`.
   The test `CountingClient` returns its existing `poll_release` `Notify`, so the
   `application_event_notify_preempts_blocking_network_poll` regression test now
   exercises the new poke-not-cancel path.

These additions (`wakeup_handle`) are not in Java: there `Selector.wakeup()` is
called directly across threads because `nioSelector.wakeup()` is thread-safe; in
Rust the selector lives behind the delegate's async `Mutex`, so a lock-free
`Arc<Notify>` handle is the equivalent primitive.

**Verification:** full `cargo test --lib` (1706 passed), clippy clean, and a
live loop of repeated cold joins now assigns partitions in ~0.5 s every time
(previously ~1 in 3 stalled for 135 s → never).

**Rules follow-up (NOT done — needs approval):** `consumer-threading.md` §10/§11
still says to wrap the network poll in a cancelling `tokio::select!` and calls it
"cancel-safe". That guidance is what produced this bug; it should be amended to
say the network poll must run to completion and be woken via the selector's
notify (not cancelled). Left for the maintainer per the "avoid changing rules"
policy.
