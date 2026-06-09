# Phase 21 — Run the consumer bg task on a dedicated single-thread runtime (CPU)

**Milestone-8 / Phase-21** · Agent number **N = 21**

## Why
`perf`/samply profiling of the plaintext consumer at 300k (CPU-weighted via
`threadCPUDelta`, summing to the measured 84% CPU) shows the CPU is **runtime-
overhead-bound, not compute-bound**:
  - ~54% tokio **multi-thread scheduler park/unpark (`park_condvar`) churn**
  - ~17% `mio::Poll::poll` (kevent) + ~13% main `block_on` park
  - **only ~11% actually running the bg task**, of which the real consume path
    (poll → Selector → try_read → socket read → decode) is ~4–8%.

Root cause: the consumer's **single** bg task (`ConsumerNetworkThread::run_once`
loop) is `tokio::spawn`-ed onto the caller's **multi-thread work-stealing runtime**
(~12 workers). The scheduler churns parking/unparking idle workers to service one
high-frequency IO task. librdkafka (dedicated IO thread) and Java (dedicated
consumer thread) don't pay this — that's the CPU gap.

`current_thread` for the *whole* app stalls the KIP-848 join (the bg task and the
app `poll()` can't share one cooperative thread). So the bg task needs its **own**
thread — but a **single-threaded** one, not a slot in a work-stealing pool.

## Goal
Run the consumer bg task (`ConsumerNetworkThread`) on its **own dedicated
`std::thread` hosting a `current_thread` tokio runtime**, instead of
`tokio::spawn` on the caller's runtime. The app keeps running on the caller's
runtime; the two communicate via the **existing** channels (unchanged). This
removes the work-stealing/park churn for the bg task.

**This is purely an internal execution-strategy change. ZERO behavior divergence:
same channels, same shutdown, same callbacks, same protocol behavior, same public
API. It must be invisible to every test except as lower CPU.**

## Design (the only production change is how `new()` runs the bg loop)

Current (`async_kafka_consumer.rs:~1201`, in `new()` only):
```rust
let join_handle: JoinHandle<()> = tokio::spawn(async move {
    let mut thread = network_thread;
    while thread.is_running() { thread.run_once().await; }
    thread.cleanup().await;
});
let network_thread_close = NetworkThreadCloseHandle::new(signal_close_fn, wakeup_fn, join_handle);
```

New (`new()` production path):
```rust
let (done_tx, done_rx) = oneshot::channel::<()>();
let thread_handle = std::thread::Builder::new()
    .name("kafka-consumer-io".into())
    .spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()            // IO + time drivers REQUIRED (selector/mio + heartbeat/poll timers)
            .build()
            .expect("build consumer io runtime");
        rt.block_on(async move {
            let mut thread = network_thread;
            while thread.is_running() { thread.run_once().await; }
            thread.cleanup().await;
        });
        let _ = done_tx.send(());
    })
    .expect("spawn consumer io thread");
let network_thread_close = NetworkThreadCloseHandle::new_dedicated(signal_close_fn, wakeup_fn, done_rx, thread_handle);
```

`NetworkThreadCloseHandle` gets an internal enum so it supports BOTH the existing
tokio-`JoinHandle` form (kept for tests / `new_with_components`) and the new
dedicated form:
```rust
enum BgJoin {
    Spawned(JoinHandle<()>),                                   // tests' no-op handle — UNCHANGED
    Dedicated { done: Option<oneshot::Receiver<()>>, thread: Option<std::thread::JoinHandle<()>> },
}
```
  - Keep `NetworkThreadCloseHandle::new(signal, wakeup, JoinHandle)` exactly as-is
    (tests at `async_kafka_consumer.rs:4123` + `new_with_components` keep using it →
    **unit tests untouched**).
  - Add `new_dedicated(signal, wakeup, oneshot::Receiver<()>, std::thread::JoinHandle<()>)`.
  - `await_join` (close path): for `Spawned` → `handle.await` (as today); for
    `Dedicated` → `done.await` (bg loop finished + cleanup done), then reap the OS
    thread via `tokio::task::spawn_blocking(move || thread.join())` and map a panic
    to the same `KafkaError::illegal_state` message as today. Must NOT block the
    async close; must NOT hang if the bg already exited.

## Hard constraints (Critic will check)
  - **Behavior identical**: channels, shutdown sequencing, `close()` semantics, the
    `maximum_time_to_wait_ms` shared `Arc<AtomicI64>`, all unchanged. `signal_close_fn`
    + `wakeup_fn` still drive shutdown; the bg loop still exits on `is_running()==false`
    then `cleanup()`.
  - **§10 / §11 / §31 preserved**: the bg runtime is `current_thread` with
    `enable_all()` so the network-poll-to-completion (cancel-safety), the
    `Selector`/`Notify` wakeup, and the time driver (heartbeat/poll timers) all work.
    The §31 listener callbacks STILL run on the app task (they already do — bg only
    enqueues + awaits the oneshot); the dedicated thread does not change that.
  - **No bg-task `tokio::spawn` that needs the multi-thread runtime**: if the bg loop
    spawns sub-tasks, they now land on the current_thread runtime (fine — it supports
    spawn). Verify nothing in the bg path calls `spawn_blocking` expecting the
    multi-thread pool or relies on `Handle::current()` being the app runtime.
  - **Clean shutdown, no thread/leak/hang**: `close()` must join the dedicated thread;
    `Drop` must not leak it (existing Drop behavior must still terminate the bg task).
  - **Unit tests unchanged**: the `Spawned` path + `new_with_components` no-op handle
    keep working verbatim. Do not modify test construction except (if unavoidable)
    purely-mechanical signature adaptation — and prefer NOT to.
  - Public API surface unchanged.

## Tests
  - All existing unit + integration tests pass unchanged. (`cargo test`, plus the
    Docker-gated integration suite at least compiles; run if Docker available — the
    `plaintext_consumer_*` and `sasl_ssl_consumer_test` exercise the real dedicated
    path end-to-end.)
  - Add one test that a consumer built via the production `new()` path joins,
    consumes (against the mock/loopback used by existing integration-style tests if
    feasible), and `close()` cleanly joins the dedicated thread without hanging
    (bounded by a timeout). If a real broker is needed, rely on the integration
    suite instead and note it.
  - `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check` green.

## Out of scope
  - Fix B (batching / wakeup coalescing) — separate follow-up.
  - Any change to `worker_threads` of the caller's runtime (that's the app's choice).
  - Producer; SSL/rustls; the perf harness.

## Critic 21 focus
  - **Zero behavior divergence** — diff the shutdown/close path carefully; the
    `Dedicated` `await_join` must match the `Spawned` semantics (clean exit vs panic
    mapping) and never hang or block the async close.
  - bg runtime has IO+time enabled (`enable_all`); §10 cancel-safety + §11 wakeup +
    §31 app-task callbacks intact.
  - No thread leak on `close()`/`Drop`; no deadlock between app runtime and the
    dedicated runtime via the channels.
  - Unit tests genuinely unchanged (Spawned path); integration tests still valid.

## Validation (Manager, post-review)
Re-measure plaintext CPU at 200k/300k (local broker, samply CPU-weighted): expect
the ~54% `park_condvar` + scheduler churn to drop sharply, CPU toward librdkafka,
throughput/latency unchanged.
