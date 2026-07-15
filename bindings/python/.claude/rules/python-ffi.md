# Python binding — FFI boundary contracts (G3)

Deep-dive rulebook for the correctness contracts across the CPython-extension /
C-ABI boundary — the rules a port must not break. Referenced on demand from
`bindings/python/CLAUDE.md` (G1/G2); **explicitly linked, never auto-loaded**.
Follows the repo's single-themed-rule-file precedent
(`.claude/rules/consumer-threading.md`): numbered sections, each as
**Rule / Why / How to apply / Anti-patterns / Tests required**.

The **producer** (`_confluentkafka.c`, `src/ffi/producer.rs`, `producer.py`) is
the worked reference; the rules are phrased to apply equally to the coming
consumer.

## Thread topology (shared context for §1–§6)

The producer binding is a three-actor system, not a thin wrapper:

```
Python thread(s)          send_thread (C)              poll_futures_thread (C)
──────────────            ───────────────              ───────────────────────
send() ─ INCREF rec+cb     accumulate ≤10ms, steal     per pending batch:
       ─ enqueue (mutex) →  send_batch() → futures[]      get_all()   ← NO GIL
       ─ [off-GIL]          immediate errors:             PyGILState_Ensure
                            fire cb under GIL                fire cb(result,error)
                            push to pending queue ──────►     DECREF rec+cb
close() ─ join(send) ◄───── on close: join(poll)              release GIL
         [GIL RELEASED]                                   free batch node
```

Everything below is a consequence of this shape.

---

## 1. GIL & threading

**Rule:**

  - Background C threads own all blocking work. Python-facing methods (`send`,
    `flush`, `close`) only enqueue/signal and return; they never call a blocking
    core function directly.
  - Every CPython API call made from a background thread MUST be bracketed by
    `PyGILState_Ensure()` / `PyGILState_Release()`.
  - Block on core futures with the GIL **released** — re-acquire it only to
    dispatch the Python callback (`Producer_complete_callbacks`: phase 1
    `get_all` off-GIL, phase 2 dispatch under the GIL).
  - Any Python-facing method that joins or waits on a background thread MUST
    release the GIL first (`Py_BEGIN_ALLOW_THREADS` around `thrd_join` in
    `py_Producer_close`).
  - Completion callbacks run on the background (poll) thread, not the caller's.

**Why:** Only one thread may touch Python objects at a time (the GIL). Calling
CPython off-GIL corrupts memory; holding the GIL across a blocking call freezes
every other Python thread. The join rule is a hard deadlock: if `close` holds
the GIL while joining, the poll thread's `PyGILState_Ensure` can never complete,
so the join never returns. Firing producer completion on the I/O thread (not the
caller) matches Java's `KafkaProducer` send-callback contract.

**How to apply:**

  - Model any new blocking feature as "Python method enqueues → background
    thread does the work → background thread dispatches results under the GIL."
  - Wrap the smallest possible region in `PyGILState_Ensure` / `PyGILState_Release`.
  - Before `thrd_join` (or any wait on a worker) in a Python-facing method, open
    a `Py_BEGIN_ALLOW_THREADS` block.

**Anti-patterns to flag in review:**

  - Any CPython call (`Py_INCREF`, `PyObject_Call*`, `PyLong_*`, …) on a
    background thread without an enclosing `PyGILState_Ensure`.
  - Holding the GIL across a blocking core call or across `thrd_join`.
  - Doing Kafka work synchronously inside a Python-facing method.

**Tests required:**

  - `close()` called with sends still in flight returns (does not hang) — the
    deadlock regression.
  - A completion callback runs while another Python thread makes progress.

---

## 2. Free-threading (PEP 703) — policy

**Rule:**

  - The extension module MUST declare its GIL stance explicitly. Today it uses
    single-phase init and therefore **re-enables the GIL** on a free-threaded
    (3.13t+) interpreter; that behavior must be documented, not accidental.
  - Never rely on the GIL to serialize state shared between C threads. Cross-
    thread state MUST be protected by a mutex or use atomics.

**Why:** The GIL only serializes access to *Python objects* from *Python
threads*. It never protected data shared between the extension's own C threads
(`send_thread` / `poll_futures_thread`). A free-threaded interpreter removes the
GIL entirely, exposing any such reliance; single-phase init silently forces the
GIL back on, which is a surprising performance regression on 3.13t.

**How to apply:**

  - When free-threading support is undertaken: migrate to multi-phase module
    init, add the `Py_mod_gil = Py_MOD_GIL_NOT_USED` slot, then audit every field
    shared across C threads.
  - Until then: guard shared flags/queues with the existing mutexes (they
    already cover the batch queues) and treat plain scalars shared across threads
    as atomics.

**Out of scope for now:** the multi-phase-init migration itself. This section is
the standing policy and audit checklist; implementing free-threading is a
separate, opt-in effort.

**Anti-patterns to flag in review:**

  - Plain `int` flags (e.g. `closed`, `send_completed`) read/written on more than
    one C thread without atomics or mutex coverage.
  - "The GIL makes this safe" reasoning applied to C-thread-to-C-thread state.

**Tests required:** deferred with the migration; the audit checklist above is the
gate until then.

---

## 3. Handle ownership & lifecycle

**Rule:**

  - Every opaque handle is born in Rust (`Box::into_raw`) and freed **exactly
    once** by its matching `_destroy` (`Box::from_raw`). Exactly one owner holds
    it at any time.
  - Ownership **transfers** when a handle is passed into a callback: the receiver
    frees it. Python adopts via `_from_c(_id)` and frees via `_destroy(_id)` —
    including on the cancelled / already-done guard paths.
  - Exactly-once completion + balanced refcounts: `Py_INCREF` the record and
    callback when enqueuing (`py_Producer_send`); `Py_DECREF` each exactly once
    after the callback fires (`Producer_complete_callback`). A record's
    completion callback fires exactly once — the immediate-error path and the
    async path must never both fire for the same record.
  - Objects that outlive the call (the `Producer`'s Python object) are
    `Py_INCREF`'d for their lifetime and `Py_DECREF`'d on close.
  - Teardown order: join all worker threads **before** destroying mutexes,
    condition variables, or the underlying handle.

**Why:** These are the leak / double-free / use-after-free rules. Exactly-once
completion is the callback obligation (root `CLAUDE.md §9.5`): Kafka guarantees
one result per record, so the binding must deliver — and free — exactly one.
Tearing down synchronization primitives while a worker still runs is undefined
behavior.

**How to apply:**

  - For every handle a new function produces, decide its single owner and where
    `_destroy` is called — on every path, including errors and cancellation.
  - When a record can complete through both the immediate-error and async paths,
    make them mutually exclusive (the producer compacts errored records out of
    the batch so the poll thread cannot re-fire them).

**Anti-patterns to flag in review:**

  - Freeing a handle after passing it to a callback (the callback now owns it).
  - A missing `_destroy` on an early-return, error, or cancel path.
  - Double-firing a completion, or an `INCREF` / `DECREF` imbalance.
  - Destroying a mutex / joining out of order relative to the workers.

**Tests required:**

  - Send N records, assert the live handle count returns to baseline (no leak).
  - Error path frees its error handle; success path frees metadata.
  - A double-completion attempt is a safe no-op (no crash, no leak).

---

## 4. Zero-copy & buffer lifetime

**Rule:**

  - Never copy key / value / header bytes across the boundary. Borrow the Python
    buffer directly (`PyBytes_AsString`) into the record struct.
  - Keep the source Python object alive across the whole async window: `INCREF`
    it and hold it on the wrapper (`ProducerRecordObject`), releasing only when
    completion fires (the record `DECREF` in `Producer_complete_callback`).
  - Copying the small fixed-size record *struct* (pointers + lengths) is fine;
    copying the *payload* is not. A short string that C needs null-terminated
    (the topic) may be copied.

**Why:** This is CLAUDE.md §12 on the send side: no avoidable per-message heap
allocation. Because the send is async and batched, the borrowed buffer must
outlive the return of `send()` — releasing it early is a use-after-free while
Rust is still reading it.

**How to apply:**

  - Store the source `bytes` object on the wrapper and tie its lifetime to
    completion; never stash a raw pointer without its owning object.
  - Copy only small, null-termination-required strings; leave payloads borrowed.

**Anti-patterns to flag in review:**

  - Copying the payload into a freshly allocated buffer.
  - Releasing the `bytes` object before the send is confirmed.
  - Per-record allocation attributable to key/value or batch traversal.

**Tests required:**

  - Per-record allocation-budget test (following the producer hot-path
    precedent): no allocations attributable to payload bytes or traversal.
  - A "buffer released early" use-after-free regression.

**Consumer note (forward):** the receive path is the mirror image and the crux
design problem — fetched bytes are owned by one buffer and every record borrows a
slice; crossing those borrowed slices into Python (which wants to own its
`bytes`) may force copies the send path never needed. Resolve before designing
the consumer ABI (`consumer-threading.md §27`).

---

## 5. Error model

**Rule:**

  - Two distinct surfaces:
    1. **Precondition validation** raises a Python exception synchronously
       (`ValueError` / `TypeError` / `RuntimeError`) — bad argument type, closed
       producer, non-dict config.
    2. **Core errors** cross as `kafka_common_KafkaError_t` handles.
  - A **null** error handle means success. Check the out-param, not just a return
    value.
  - Map a core error to a Java-shaped `KafkaError` carrying `code`, `message`,
    `is_retriable`, `is_fatal`. Asynchronous send errors are delivered via the
    Future's `set_exception`.
  - The callback owns and frees the error handle.

**Why:** Java parity of error classification. Two surfaces exist because "you
called it wrong" is a programming error (raise now) while "the broker rejected
it" is a runtime outcome (deliver via the result path). Error message content is
part of the behavioral contract (DoD §3).

**How to apply:**

  - Validate arguments up front and raise the specific Python exception type.
  - For core calls, read the out_error, convert to `KafkaError`, then free the
    handle; deliver async failures through the Future.

**Anti-patterns to flag in review:**

  - Ignoring / dropping an `out_error`.
  - Leaking the error handle after reading it.
  - Collapsing distinct precondition failures into one generic error.

**Tests required:**

  - Each surface exercised (precondition raise; synchronous error; async send
    error).
  - **Error message content asserted**, not just that an error occurred.
  - `is_retriable` / `is_fatal` classification preserved end to end.

---

## 6. Async / Future

**Rule:**

  - A method that blocks in Java maps to a `concurrent.futures.Future` completed
    by the background thread.
  - Completion is exactly-once; guard against setting a result on a cancelled or
    already-done Future (and free the handles on those paths).
  - **Cancellation is best-effort: cancelling the returned Future does NOT abort
    an in-flight send.** The record is already enqueued; cancel only means the
    result is discarded (and its handles freed).
  - Thread affinity: `Future.add_done_callback` runs on the completing
    (background) thread, not the caller — document it.
  - The tokio runtime handle is smuggled into the future handle (`FfiFuture`) so
    the blocking `get` has a runtime to drive.

**Why:** Java `Future` semantics with Rust/Tokio underneath. Users will assume
cancel aborts work and that callbacks run on their thread; both are false here,
so the contract must say so. Per-send task spawning is a hot-path allocation
(CLAUDE.md §11), avoided by the shared background threads.

**How to apply:**

  - Create the Future on the Python side, complete it from the background
    thread's callback, and track pending Futures so `close` can cancel them.
  - Guard the completion callback against cancelled / done before setting a
    result.

**Anti-patterns to flag in review:**

  - Assuming `cancel()` aborts the send.
  - Setting a result on a cancelled / done Future without freeing the handles.
  - `tokio::spawn` per send.

**Tests required:**

  - Future resolves with metadata on success and with an exception on error.
  - Cancelling a pending Future is safe and frees its handles.

---

## Cross-references

  - §1 (GIL) and §3 (refcounts) interlock: the record `INCREF` / `DECREF` in §3
    is what keeps §4's borrowed buffer alive, and both cross the GIL boundary in
    §1.
  - §5 (errors) and §6 (Future) interlock: async errors are delivered as a
    `set_exception` on the §6 Future.
  - Consumer work: pair this file with `consumer-threading.md` §27 (receive-path
    zero-copy) and §31 (listener callback thread — the opposite of §1's producer
    rule).

**Current-code findings are tracked separately** (not as rules): the shared-flag
data race and the missing free-threading module slot belong in review comments
for the Actor/Critic, not encoded here.
