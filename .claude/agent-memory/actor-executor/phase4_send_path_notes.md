---
name: phase4-send-path-notes
description: Milestone-11 Phase 4 (N=44) idempotent send-path integration — the §2 split shape, both-owners batch pool mechanics, and two pre-existing defects it surfaced
metadata:
  type: project
---

Milestone 11 Phase 4 (Actor 44) wired the Phase-3 `TransactionManager` into the live
producer send path across 9 commits (`883ab19`..`7055b7d`).

**Why:** the producer advertised `enable.idempotence=true` by default and delivered
at-least-once. Phase 4 is what makes the default honest.

**How to apply:** the design calls below are the ones a future phase is most likely
to re-derive incorrectly.

## The rules §2 split shape — "queue as a parameter"

`pendingRequests` and `inFlightRequestCorrelationId` are **fields on `Sender`**, and
manager methods that Java implements by touching them take them as parameters. Same
shape rules §7 already uses for `InFlightBatchPool`. The criterion, stated once on
the `TransactionManager` struct docs: a method whose body touches *only*
Sender-confined state moves to `Sender`; one that touches both stays on the manager
and receives the Sender state.

Checkable mechanically: neither name appears as a **field** in
`transaction_manager.rs`. Phase 5 adds the three coordinator fields — they belong on
`Sender` too, for the same reason.

## The both-owners batch pool is a callback, not a returned map

`RecordAccumulator::with_in_flight_batch_pool(partitions, sender_batches, f)`. The
merge has to happen *inside* the accumulator because every `&mut ProducerBatch` in
the pool shares the deque `MutexGuard`s' lifetime, which only exists inside that
call. A caller extending the pool from its own map inside `f` does not type-check
(the `impl FnOnce(&mut InFlightBatchPool<'_>)` bound elaborates to one concrete
lifetime, and an HRTB version would demand `'static` batches).

Three-pass lifetime dance inside: own `Arc<TopicInfo>` per partition → collect
DashMap `Ref`s → lock every deque → build the pool. Each pass borrows the previous
(now immutable) `Vec`.

## Two pre-existing defects Phase 4 surfaced

1. **Fixed:** `PendingProduceRequest` recorded only partitions, so
   `handle_produce_responses` completed the *oldest* batch for a partition rather
   than the one the request carried. Invisible with one request in flight; wrong once
   idempotence allows 5. Now records each batch's `Arc<ProduceRequestResult>` and
   matches with `Arc::ptr_eq`.

2. **Recorded, not fixed (PLAN §9.18):** split-on-`MESSAGE_TOO_LARGE` panics.
   `ProducerBatch::records()` → `take_built_records()` *moves* the built buffer out
   for the zero-copy send, and the split can only run after the send. Affects every
   producer. Reproducer is `#[ignore]`d in `sender.rs`.

## Test-shape lessons

  - Java's `writeIdempotentBatchWithValue` batches are referenced by both the entry
    and the test. In Rust the test owns them and hands them to the epoch bump as an
    `InFlightBatchPool` — driving `run_once()` cannot work, because it only finds
    batches an *owner* holds. Assert accumulator behaviour through `run_once()`;
    assert batch rewrites through the pool call.
  - Hand-built batches handed back to the accumulator must be registered with
    `register_incomplete_for_test`, or `complete_batch` trips its own
    "This should be impossible" assertion.
  - `sequence_has_been_reset()` is cleared by `ProducerBatch::close()` in both
    languages, so it is unobservable after a re-drain.
  - **Two `.lock()` calls in one expression deadlock** the non-reentrant
    `std::sync::Mutex` (both guards are temporaries living to the end of the
    statement). Cost a hung test while debugging.
  - Allocation-budget tests must compare a delta (1-record vs N-record) rather than
    an absolute count, and must be mutation-checked *both* ways: a single sized
    allocation does not fail a count-based tracker; a per-record loop does.

## DoD §3 residual debt

33 idempotence-only `SenderTest.java` methods are translatable against the Phase-4
surface but not written; itemised by name and Java line in a comment block at the end
of `sender.rs`, carried to Phase 8's parity sweep. 17 more are transactional
(Phases 5/6). Stated plainly rather than implied — Phase 4 did **not** achieve
`SenderTest` idempotence parity.
