---
name: phase11-consolidated-patterns
description: Milestone-8 Phase 11 — consolidated cross-cutting patterns from commits 1-11 (AsyncKafkaConsumer end-to-end + §31 regression pair)
metadata:
  type: project
---

# Phase 11 — Cross-cutting patterns (durable summary)

Phase 11 landed the user-facing `AsyncKafkaConsumer<K, V>` struct, its
`Consumer<K, V>` trait impl, the `Box<dyn Consumer<K, V>>` factory
seam, and ~120 inline tests translating `AsyncKafkaConsumerTest.java`.
Below are patterns that recur across the 11 commits and are worth
promoting from per-commit notes for future phase work. Per-commit
detail lives in `phase11_commit_{4-7,8-10}_notes.md` and
`phase11_critic_batch2_patterns.md`.

## 1. `submit_and_drain<T>` is the universal primitive for blocking-style consumer APIs

**Why**: Java's `addAndGet(...)` blocks the caller's thread; Rust's
`add_and_get(...).await` blocks the caller's task. When the bg task
needs to enqueue a `RebalanceListenerCallbackNeeded` (and block on
its ack per consumer-threading.md §31), a deadlock forms — the bg
task can't complete the typed future and the app side can't service
the listener callback because it's blocked on the typed future.

**Solution**: Every blocking-style API routes through
`submit_and_drain<T>(event, receiver, deadline_ms, msg, enable_wakeup)`
or `process_background_events_until<T>(receiver, ...)`. The iterative
loop interleaves bg-event draining (invokes listener callbacks on the
caller's task) with bounded receiver waits.

**Per-API matrix** (`enable_wakeup` flag mirrors Java's `setActiveTask`
call sites — `wakeupTrigger.setActiveTask` in Java source is the
source of truth):

  - `commit_sync` typed wait: **true** (Java line 1716)
  - `commit_inner` offsets-ready wait inside commit_sync: **true**
    (uniform Rust pattern, deliberate over-tightening — Java's
    setActiveTask fires only after commit() returns)
  - `commit_inner` offsets-ready wait inside commit_async: **false**
    (Issue 22 — Java's `commitAsync` is documented non-blocking and
    never throws WakeupException; Java line 1684-1700)
  - `committed_timeout`: **true** (Java line 1176)
  - `partitions_for_timeout`: **true** (Java line 1223)
  - `list_topics_timeout`: **true** (Java line 1251)
  - `position` CheckAndUpdatePositions wait: **true** (Java line 1963)
  - `await_pending_async_commits`: per-call (Java line 1738)
  - everything else (subscribe, assign, seek, pause, resume,
    current_lag, beginning/end_offsets, offsets_for_times,
    leave_group_on_close): **false**

**Anti-pattern**: A shared `commit_inner` that hardcodes
`enable_wakeup=true` is a bug for the `commit_async` arm. Add the
parameter and split the per-arm decision. Issue 22 was exactly this.

## 2. Listener-callback handshake: app side runs the listener, bg side awaits the ack

**§31 contract** (consumer-threading.md §31): `ConsumerRebalanceListener`
callbacks execute on the **caller's task** during `poll()`,
`commit_*()`, `unsubscribe()`, `close()`, and any timed query API
that drains the background event queue. They NEVER execute on the
background task.

**Mechanism**: The bg task enqueues a
`BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded` with a
`tokio::sync::oneshot::Sender<Result<(), KafkaError>>` ack channel.
The bg task awaits the receiver. The app side, inside its drain
loop, invokes the listener inline and sends the result on the ack
channel.

**Regression tests** (Phase 11 commit 11/N):
  - `section_31_commit_sync_from_inside_revoked_callback_succeeds`:
    listener body issues a commit request via a controller channel;
    outer `commit_sync` completes without deadlock.
  - `section_31_rebalance_does_not_advance_until_listener_resolves`:
    listener body awaits a test-held release channel; the ack
    receiver stays pending until release.

**Sanity check**: Test A would deadlock if §31 were broken (i.e., if
the listener were spawned on a separate task without ack
synchronization, or if `process_background_events` were removed from
`commit_sync`'s path). The existing Issue 10 regression test
(`issue_10_commit_sync_drains_listener_callback_while_waiting`)
specifically proves the drain-while-waiting fix; the §31 pair
extends to in-listener consumer invocations.

## 3. Test fixtures: drainer task + groupless variant

**Drainer-task pattern**: Tests that need to observe a specific event
AND let the app-side call return spawn a drainer BEFORE the API call.
The drainer pulls events from the test-side `app_event_rx` and
completes the handle. This stands in for the not-yet-wired bg task.

**Groupless-consumer fixture**: `make_test_consumer_without_group_id()`
clears BOTH `consumer.group_id` AND `consumer.config.group_id` from
the regular fixture. Mutating just `consumer.group_id` post-construction
(the Issue 26 anti-pattern) doesn't exercise the same code paths a
real groupless consumer would; the fixture pre-bakes the future
Phase-12 ctor shape.

**SKIP rationale block**: Each test-translation commit opens with
a comment listing every skipped Java test + one-line rationale,
referencing PLAN deferral numbers explicitly. The Critic walks this
block top-to-bottom looking for "what's covered, what's deferred".
Sprinkling SKIP comments through the file splinters the rationale.

## 4. Per-Java-test exact-message assertion (DoD §3)

**Why**: DoD §3 requires "Error message content is asserted, not
just is_err() — error messages are part of the behavioral contract."
A typo in the production error message slips past `.contains(...)`
substring checks.

**Pattern**:
```rust
let err = consumer.api_call().await.expect_err("must err");
match err {
    KafkaError::Timeout(msg) => assert_eq!(msg, "Failed to get offsets by times in 100ms"),
    other => panic!("expected Timeout, got {other:?}"),
}
```

Avoid `assert!(matches!(err, KafkaError::Timeout(_)))` — that misses
message-text regressions. Issue 25 was exactly that.

## 5. Java `Set.toString()` formatting in error messages

**Why**: Java's `Set<TopicPartition>.toString()` produces `[t-0, t-1]`-style
strings. Rust's `{:?}` on `&[TopicPartition]` produces
`[TopicPartition { topic: "t", partition: 0 }]`-style — incompatible
with Java's exact-message contract.

**Pattern**: Use `format_partitions_for_display(&[TopicPartition])`
helper that delegates to `TopicPartition`'s `Display` impl
(`"{topic}-{partition}"`) joined by `, ` inside `[...]`. Issue 19
was this exact pattern.

## 6. Java `InvalidGroupIdException` → `KafkaError::invalid_group_id`

**Why**: Java's `InvalidGroupIdException extends ApiException` carries
the `Errors::InvalidGroupId` protocol code. The natural Rust
translation as `KafkaError::IllegalArgument` (which corresponds to
Java's `IllegalArgumentException extends RuntimeException`) loses the
discriminator — user code dispatching on error type can't
distinguish "bad argument" from "no group.id configured".

**Pattern**: Add a convenience constructor
`KafkaError::invalid_group_id(message)` that wraps `Generic` with
`Errors::InvalidGroupId`. Tests assert on `err.error() ==
Errors::InvalidGroupId` rather than substring matching. Issue 16
was this.

## 7. `commit_sync` two-timer vs Rust one-timer divergence

**Why**: Java's `commitSync` computes TWO independent timers — one
for the bg-side commit RPC (`calculateDeadlineMs`), one fresh for
the app-side wait (`time.timer(timeout.toMillis())`). Total
worst-case is `~2 * timeout`. Rust uses a single `deadline_ms` for
`~1 * timeout` total.

**Decision**: Document, don't fix. The Rust behaviour is stricter
and arguably more correct (most user code expects "commit_sync
timeout=T should not exceed ~T"). Java's doubling is an artifact of
timer construction rather than a documented contract. Issue 18.

## 8. `Box<dyn Consumer<K, V>>` factory seam pattern

**Java parity**: Java's `KafkaConsumer` constructor accepts
`ConsumerConfig` + deserializer args. Rust analog: `new_consumer<K, V>(config, key_deser, value_deser) -> Result<Box<dyn Consumer<K, V>>, KafkaError>`.

**Phase 11 seam**: The production ctor wiring (350-line Java
primary ctor translation) is deferred to Phase 12. The factory
returns `KafkaError::unsupported_version(...)` for the
`GroupProtocol::Consumer` arm with an error message that explicitly
differentiates "deferred to Phase 12" from "permanently unsupported"
(the `Classic` arm). Tests construct `AsyncKafkaConsumer` directly
via `new_with_components(...)`.

**Trait surface (DoD §11)**:
  - Top-level `Consumer<K, V>`: `#[async_trait]`, dispatched via
    `Box<dyn Consumer<K, V>>`.
  - Per-record `Deserializer<T>`: sync `fn`, no `#[async_trait]`.
  - Per-record `Serializer<T>`: sync `fn`, no `#[async_trait]`.
  - No `block_on`-wrapped sync façade anywhere.

## What Phase 12 (integration test) inherits

  1. **Production ctor**: 350-LOC Java primary constructor at
     `AsyncKafkaConsumer.java:285-600` translation. Wires
     `NetworkClient` + `ChannelBuilder` + `Selector` +
     `ConsumerNetworkThread::run` spawn end-to-end. The factory
     `new_consumer` flips from `unsupported_version` to real
     dispatch.
  2. **MockClient-backed integration**: integration tests in
     `tests/consumer/async_kafka_consumer_test.rs` exercise the full
     bg-task ↔ app-side path with a `MockClient` instead of the
     no-op spawn used in Phase 11 inline tests. The
     drainer-task fixture pattern from Phase 11 becomes
     obsolete — the bg task IS the drainer.
  3. **MemberStateListener wire-up**: `consumer.state_notifier()`
     accessor is in place (Issue 13 seam); Phase 12 ctor registers
     it on the membership manager.
  4. **Pending-async-commit incomplete-future test**
     (`testCloseAwaitPendingAsyncCommitIncomplete`): inherits the
     skip rationale from commit 11/N. With a real bg task the
     incomplete-future timing falls out naturally.
  5. **Reaper-invoked tests**: Phase-10 `ConsumerNetworkThreadTest`
     exercises the bg-task reap path; Phase 12 integration tests
     can verify the close/poll/unsubscribe reap call sites via the
     real bg task.

## Files closing this phase

Production:
  - `src/consumer/async_kafka_consumer.rs` — 700+ LOC struct,
    ~30 public async methods, `Consumer<K, V>` trait impl,
    ~120 inline tests.
  - `src/consumer/mod.rs` — trait, factory, public re-exports.
  - `src/consumer/internals/consumer_rebalance_listener_invoker.rs`
    (commit 1/N).
  - `src/consumer/internals/consumer_utils.rs` (commit 1/N).
  - `src/common/kafka_error.rs` — `invalid_group_id` convenience
    constructor (Issue 16 / commit 11).

Tests: inline test module in `async_kafka_consumer.rs` —
1685 lib tests after Phase 11 close.
