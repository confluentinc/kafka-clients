# .NET binding — FFI marshalling & ownership

Deep-dive rules for marshalling across the C ABI (`confluent_kafka.h`,
`kafka_producer_*` / `kafka_common_KafkaError_*`) from .NET via P/Invoke.
Supplements `bindings/dotnet/CLAUDE.md`. Review ground truth is the **C ABI
header** and the **Kafka Java public API** — never librdkafka or the existing
`confluent-kafka-dotnet` client (that wraps a *different* C ABI).

Each numbered section is one decision: the **Rule**, **Why**, **How to apply**,
**Anti-patterns**, and **Tests required**.

---

## Thread topology & thread-safety (shared context)

The whole-system picture that every numbered section below assumes. The C ABI
hides all Kafka I/O on native threads **owned by the Rust core**; the binding
adds only the caller threads and a single completion pump.

```
   .NET (managed)                    │ C ABI │       Rust core (native, per producer)
   ─────────────                     │       │       ─────────────────────────────────
   caller thread(s):                 │       │   tokio multi-thread runtime (worker POOL)
     SendAsync → Producer_send ──────│──────►│     RecordAccumulator (enqueue, returns fast)
   completion pump (1 bg thread):    │       │   Sender task (spawned once in _new):
     get_all(futures) ─block_on─────►│──────►│     NetworkClient + ONE async Selector
     ◄── per-message metadata/err ───│◄──────│       ↕ multiplexes ALL brokers (event-driven)
   Dispose: join pump → flush/close  │       │   (runtime+task created in KafkaProducer_new,
            → destroy handle         │       │    dropped on Producer_destroy)
```

**Rule:**

  - Behind the C ABI, each real `KafkaProducer` owns a **multi-threaded tokio
    runtime** (a small worker-thread pool) plus **one spawned Sender task** that
    owns the network stack and does all Kafka protocol I/O over a **single async
    Selector** (all brokers multiplexed — NOT one thread per broker). These
    native threads are created in `kafka_producer_KafkaProducer_new` and torn
    down by `kafka_producer_Producer_destroy`.
  - On the .NET side there are only: the **caller thread(s)** and **exactly one
    completion pump thread** (see §1). No per-send threads.
  - **There is no mandatory poll loop.** The core self-drives its I/O; the pump
    exists only to harvest per-message futures. If the pump stalls, sends still
    flow to the broker — only result delivery back to .NET pauses.
  - FFI calls may originate from any .NET thread. The core serializes them behind
    the producer handle's internal `Mutex`, so concurrent `SendAsync` is safe —
    but do NOT add your own lock around FFI calls (the core already locks).
  - `Producer_send` / `_get*` call `block_on` on the **calling** .NET thread.
    Because the runtime is multi-threaded, this parks only that one .NET thread;
    the Sender keeps running on the runtime's worker threads. A blocked pump does
    NOT stall sending.
  - Any native → managed callback runs on a **non-.NET-managed thread**:
    `RecordMetadata_copy`'s callback fires synchronously on whoever called it
    (the pump); a future push-completion callback (see §1 "Future direction")
    would fire on a tokio worker thread. Treat all such callbacks as foreign-
    thread (keep delegates alive, catch every exception, hop user continuations
    off the callback thread).

**Why:** This is Java's single-Selector NIO model translated to tokio
(CLAUDE.md §8) — deliberately unlike librdkafka, which runs a "main" thread plus
**one blocking thread per broker** and *requires* the app to pump
`rd_kafka_poll` (confluent-kafka-dotnet does this with a `LongRunning`
`callbackTask`). Consequences of the difference: our native thread count does
**not** grow with cluster size; there is **no** poll loop to run; and results
arrive as independent per-message futures, not through a shared delivery-report
event queue. The multi-threaded runtime is also why `block_on` from a .NET
thread is safe and cannot deadlock the Sender.

**How to apply:**

  - Keep exactly one pump thread; never spawn a thread (or `Task.Run`) per send.
  - Never dispose the producer `SafeHandle` while the pump may still touch
    futures derived from it — join the pump first (§1, Dispose).
  - Do not model a `confluent-kafka-dotnet`-style poll loop — there is no
    `poll()` in this ABI, and nothing needs pumping to make progress.
  - Do not hold a managed lock across any blocking FFI call (`_get_all`,
    `_flush`, `_close`).
  - Treat every native → managed callback as arriving on a foreign thread (see
    the Callback and §1 Async sections).

**Anti-patterns to flag in review:**

  - A .NET "poll loop" thread modeled on `confluent-kafka-dotnet`'s
    `callbackTask` — there is nothing to poll here.
  - Assuming one native thread per broker, or that native thread count scales
    with cluster size.
  - Wrapping FFI calls in a binding-side lock "for safety" (double-locking; the
    core already serializes via its `Mutex`).
  - Assuming FFI calls are single-threaded, or that completions arrive on the
    caller's thread.
  - Doing user work directly on a callback thread (a tokio worker).

**Tests required:**

  - Concurrent `SendAsync` from multiple .NET threads is safe and correct
    (core `Mutex` serialization holds).
  - A long-blocked pump (slow/paused broker) does not prevent new sends from
    being accepted and enqueued (the self-driving runtime property).
  - `Dispose` joins the pump before destroying the handle — no use-after-free
    (shared with §1).

---

## 1. Async / Future completion — pull-based pump + `TaskCompletionSource`

The C ABI is **pull-based**: the only ways to learn a send resolved are the
blocking `kafka_producer_FutureRecordMetadata_get` / `_get_all` or the
non-blocking poll `kafka_producer_FutureRecordMetadata_is_done`. There is **no
push completion callback** in the ABI today. Java's `Future<RecordMetadata>`
maps to .NET `Task<RecordMetadata>`, completed by a dedicated background
**completion pump** — mirroring the Python binding's `poll_futures_thread`
(see `bindings/python/.claude/rules/python-ffi.md` §6 + "Thread topology").

### Thread topology

```
Caller thread                         Completion pump (one bg thread)
─────────────                         ───────────────────────────────
SendAsync():                          loop:
  pin key/value (call-scoped)           drain a batch of (future, tcs)
  Producer_send() → future handle       get_all(futures[])   ← BLOCKS here
  new TaskCompletionSource (tcs)        for each i:
  enqueue (future, tcs)                   tcs[i].SetResult / SetException
  return tcs.Task  (no thread parked)   destroy_all(futures)
Dispose(): signal + join the pump ◄──── on shutdown: drain, fault pending, exit
```

**Rule:**

  - A method that blocks in Java returns a `Task<T>` backed by a
    `TaskCompletionSource<T>`; the caller-facing method only enqueues work and
    returns. It NEVER calls a blocking core function (`_get` / `_get_all`)
    directly on the caller's thread.
  - Exactly **one** dedicated background pump thread does all blocking waits,
    via the batched `kafka_producer_FutureRecordMetadata_get_all`. The thread
    count is O(1) regardless of in-flight send count.
  - `kafka_producer_Producer_send` runs inline on the caller's thread (it is a
    fast enqueue); only the *wait* is offloaded. Unlike Python, .NET has no GIL,
    so a separate send-batching thread is NOT required. (A send-batching thread
    that uses `_send_batch` is an optional throughput optimization, not a
    baseline requirement.)
  - Construct the `TaskCompletionSource` with
    `TaskCreationOptions.RunContinuationsAsynchronously`.
  - Completion runs on the pump thread, not the caller's — this is the .NET
    analog of Java's send-callback contract. Document the thread affinity.
  - Completion is exactly-once. Guard against completing an already-completed or
    cancelled `Task`, and free the native handles on every path.
  - **Cancellation is best-effort:** cancelling the returned `Task` does NOT
    abort an in-flight send (the record is already enqueued in the core); it
    only discards the result and frees its handles.

**Why:** The core owns its own Tokio runtime and background sender task, so the
"drive the work" loop already lives in Rust — the binding must only bridge each
resolved future to a `Task`. A pull ABI forces the wait onto *some* thread; a
single pump gives bounded threads for unbounded in-flight sends. The
alternative — `Task.Run(() => FutureRecordMetadata_get(...))` per send — parks
one thread-pool thread per in-flight message, which starves the pool under the
high in-flight concurrency a Kafka producer is designed for (sync-over-async
anti-pattern). `RunContinuationsAsynchronously` is required because, by default,
`tcs.SetResult` runs the awaiter's continuation **synchronously on the pump
thread**; a slow user continuation would then stall completion of every other
send. This design matches CLAUDE.md §11 ("avoid per-message spawn; use a shared
completion task with a channel") and keeps the .NET binding consistent with the
Python design.

**How to apply:**

  - `SendAsync`: pin key/value bytes (call-scoped, see the pinning section),
    call `kafka_producer_Producer_send` to get the future handle, check the
    synchronous `out_error` (throw `KafkaException` on non-null), create the
    `TaskCompletionSource`, enqueue `(future, tcs)`, return `tcs.Task`.
  - Use a `System.Threading.Channels.Channel` (or `BlockingCollection`) as the
    hand-off queue; the pump drains a batch (FIFO), calls `_get_all`, and
    completes each `tcs`.
  - Read result fields immediately and free the native handles on the pump
    thread: on success, extract offset/partition/topic/timestamp then
    `RecordMetadata_destroy` (or use `RecordMetadata_copy`, which extracts +
    frees in one call); on error, read `KafkaError_code` / `_message` /
    `_is_retriable` then `KafkaError_destroy`. The managed `RecordMetadata` /
    `KafkaException` hold **copied values**, not handles — so they need no
    `SafeHandle`.
  - Free the future handles with `kafka_producer_FutureRecordMetadata_destroy_all`
    after `_get_all` returns (the futures are NOT consumed by `_get_all`).
  - `Dispose` / `close`: stop accepting sends, let the pump drain (Java
    `close()` flush semantics) or fault the still-pending `Task`s with a
    `KafkaException`, then **join the pump thread**, then
    `kafka_producer_Producer_flush` → `_close` → dispose the producer
    `SafeHandle`. Never destroy the producer handle while the pump may still be
    calling `_get_all` on futures derived from it.
  - Optional fast path: if `kafka_producer_FutureRecordMetadata_is_done` is true
    at send time (auto-complete mock, or an already-acked send), complete
    synchronously and return a completed `ValueTask` without queueing.

**Anti-patterns to flag in review:**

  - `Task.Run(() => ...FutureRecordMetadata_get...)` per send, or any
    one-thread-per-in-flight-message pattern (Option A).
  - A caller-facing `SendAsync` that blocks on `_get` / `_get_all` directly.
  - `TaskCompletionSource` created without `RunContinuationsAsynchronously`.
  - Running user continuations / user code on the pump thread.
  - Completing a cancelled or already-completed `Task` without freeing the
    metadata/error handles.
  - Destroying the producer handle before draining + joining the pump.
  - Assuming `Task` cancellation aborts the in-flight send.
  - Holding a managed lock across a blocking `_get_all` FFI call.

**Tests required:**

  - `SendAsync` `Task` resolves with `RecordMetadata` on success and faults with
    `KafkaException` on error.
  - `Dispose`/`close` called with sends still in flight returns (does not hang) —
    the pump-join regression.
  - Cancelling a pending `Task` is safe and frees its native handles.
  - High-concurrency produce (many more in-flight sends than thread-pool
    threads) completes without thread-pool starvation.

---
