# Phase 14 — Background-task wakeup on application-event enqueue

## Problem

Against a live broker, the `AsyncKafkaConsumer` delivers fetched records on a
~5-second cadence regardless of when records are produced. Measured: messages
produced 1/sec were delivered in bursts ~5s apart, with the first batch ~10s
after production.

### Root cause

The number 5s is `ConsumerNetworkThread::MAX_POLL_TIMEOUT_MS = 5_000`, the bg
task's max idle network-poll wait.

- `ApplicationEventHandler::add()` (`events/application_event_handler.rs`) only
  did `sender.send(envelope)`. It did **not** wake the bg task.
- The bg task's `run_once` network poll (`consumer_network_thread.rs`) runs
  inside a `tokio::select!` whose only arms were the user-wakeup token and
  `poll_default(poll_wait_time_ms)`. There was **no arm for an incoming
  application event**.

So when the app's `poll()` enqueues a fetch event, the bg task is parked inside
`poll_default` for up to 5s and only observes the event on the next `run_once`
(after the poll times out). Hence the ~5s cadence.

### Java contract (the reference)

`ApplicationEventHandler.add()` is two statements:

```java
applicationEventQueue.add(event);
wakeupNetworkThread();   // → networkThread.wakeup() → networkClientDelegate.wakeup() → client.wakeup() → Selector.wakeup()
```

`Selector.wakeup()` forces the in-progress blocking `poll()` to return
immediately. The Rust port translated the first line (the mpsc send) but not
the second (the wakeup). This is consistent with `consumer-threading.md` §10,
which mandates draining via non-blocking `try_recv` (not `recv().await`) — that
choice severs the automatic wake, so a separate wake primitive is required, and
it was never wired for internal events. The user-facing `wakeup()` (§11) was
translated as a rotating `CancellationToken`, but that must NOT be reused here
(cancelling it makes the user's `poll()` return `WakeupException`).

### Why it wasn't caught

bg-task unit tests use a mock delegate whose `poll_default` returns instantly,
so `run_once` spins fast and `try_recv` picks up events on the next iteration —
correctness holds, only wall-clock promptness is wrong. The 5s gap only
manifests when `poll_default` actually blocks on a live socket.

## Fix (Approach B — dedicated `Arc<tokio::sync::Notify>`)

Mirror Java's `wakeupNetworkThread()` as its own Rust-native signal (consistent
with how §11 translated the user wakeup into a token rather than reusing the
selector).

1. `ApplicationEventHandler`: hold `event_notify: Arc<Notify>`; call
   `event_notify.notify_one()` at the end of `add()` (covers `add_and_get`,
   which routes through `add()`).
2. `ConsumerNetworkThread`: hold the same `Arc<Notify>`; add a third arm to the
   `run_once` poll `select!`: `_ = self.event_notify.notified() => { ... }` so a
   freshly enqueued event preempts the network poll.
3. Production ctor (`async_kafka_consumer.rs`): create one `Arc<Notify>`, pass
   clones to both the handler and the bg task.

### Correctness rationale (for review focus)

- **No lost wakeup**: `notify_one()` stores one permit when no waiter is parked,
  so an event enqueued in the gap between the top-of-loop `try_recv` drain and
  the `select!` is not missed — the next `notified()` resolves immediately.
- **No busy-loop**: at most one stored permit; consumed by one `notified()`,
  costing at most one extra fast iteration per burst. Multiple enqueues coalesce
  into one wake; `try_recv` drains them all (matches Java's coalescing
  `Selector.wakeup()`).
- **Cancel-safety**: when `poll_default` wins the `select!`, the dropped
  `notified()` future does not swallow a pending notification.

## Tests added

- `application_event_handler::tests::add_wakes_the_shared_notify` — asserts
  `add()` fires the shared `Notify`.
- `consumer_network_thread::tests::application_event_notify_preempts_blocking_network_poll`
  — `CountingClient` gains a `poll_block` gate that makes `poll()` block forever;
  the test asserts `run_once` returns promptly via the notify arm (guarded by a
  2s timeout that would fail if the arm were absent).

## Files changed

- `src/consumer/internals/events/application_event_handler.rs`
- `src/consumer/internals/consumer_network_thread.rs`
- `src/consumer/async_kafka_consumer.rs`
- (unrelated, also in tree) `src/bin/consumer_test.rs`, `Cargo.toml` (`signal`
  feature) — the manual smoke-test binary used to diagnose the bug.
