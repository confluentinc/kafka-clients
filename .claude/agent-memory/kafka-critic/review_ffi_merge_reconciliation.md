---
name: review-ffi-merge-reconciliation
description: How to audit a producer-FFI merge reconciliation (UAF/drain/callback) — Critic 60, merge a7e591fd (CFFI←master PR#142)
metadata:
  type: project
---

Reviewing `src/ffi/producer.rs` merge reconciliations (adopt one branch's FFI
base, re-apply the other's txn surface + async-send drain + callback fix). The
CFFI←master merge (commit `a7e591fd`) reviewed clean — no defects. The audit
heuristics that made that verdict fast and safe:

**Why:** this is unsafe FFI with `&'static` refs into a leaked, later-freed
handle; the bug classes are UAF-on-teardown, drain deadlock/counter-drift, and
callback double-free / missed-fire. Each has a mechanical check.

**How to apply (the four checks):**

1. **UAF/teardown:** `grep '\.spawn('` the whole file. For each spawn, decide if
   the task holds an `&'static` into the producer — it does iff it calls
   `producer_static_ref(ptr)` or `&*(ptr as *const ProducerHandle)`. Every such
   task MUST be `register_pending_task`'d (destroy joins them before `drop(kind)`).
   Tasks that only capture **owned** clones (a `KafkaFuture`, `completion_tx`) —
   e.g. `get_async`/`get_all_async` — need NO registration; flagging them is a FP.
   Synchronous `block_on` txn methods (`with_txn_control` + the 5 control fns) do
   NOT spawn, so "no registration needed" is correct — verify by grepping the txn
   line range for `.spawn(`, don't assume.

2. **Drain:** confirm sync `flush`/`close` clone the runtime `Handle` under a
   brief `kind` lock, DROP it, then drain (no lock across the barrier await); the
   async path uses the `.await` drain form (no `block_on` nested in a spawned
   task). Fast path = `queued_sends == 0`. Counter can't drift: `+1` before
   `submit_tx.send`, `-1` via a `QueueDepthGuard` Drop (every exit) or on submit
   failure; the `Barrier` arm must NOT fetch_add or make a QueueDepthGuard.
   Runtime is `new_multi_thread` + completion channel is unbounded
   `std::sync::mpsc`, so `block_on`-drain can't starve the submission task and
   flush-from-callback can't newly deadlock (L9).

3. **Callback exactly-once:** one `fired: Arc<AtomicBool>` per record, shared by
   the callback handed to `send` and the task's `fire_error` re-fire on `Err`;
   `compare_exchange(false,true,AcqRel,Acquire)`, losers return before allocating
   any handle. Covered by a unit test that fires both and asserts 1 invocation,
   AND the C test `test_send_async_on_closed_producer_fires_callback`.

4. **Run `make test-c`, not just cargo.** The callback/drain/txn behavior lives
   in the C `mock_producer` suite (tests `*_drains_async_queued_send`,
   `*_fires_callback`, `transaction_commit_flushes_pending_sends`). The
   documented "HEAD-test + master-impl" hazard means cargo can pass while the C
   suite fails. Also check `target/include/confluent_kafka.h` has the 5 txn fns +
   `txn_requires_abort`.

**Non-bugs that look like bugs (don't file):**
  - Counter is incremented before `submit_tx.send`, so a *cross-thread* flush can
    barrier ahead of a not-yet-enqueued send. Within the documented "concurrent
    sends not covered" contract; the single-thread `send_async();flush()` path is
    always correct (FIFO). Matches Java flush.
  - Txn control taking the `kind` lock (vs a lock-free cache) only adds bounded
    latency behind a concurrent blocking `send`; never deadlock/corruption, and
    plan-directed. See [[review_from_config_patterns]] style: plan-sanctioned.
  - `destroy` destructuring `*handle` then joining tasks that re-deref the raw
    `ptr` (temporary `kind` guard across the join) is master's reviewed pattern —
    out of scope for a merge review; only confirm byte-identity.

**Test-count sanity:** HEAD `ffi::producer` `#[test]` count should equal the
UNION of both parents; `comm` the sorted `fn test_*` name lists to prove no test
was dropped from either side.
