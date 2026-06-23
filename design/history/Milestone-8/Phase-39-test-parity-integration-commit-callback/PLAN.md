# Phase 39 — Integration test parity: commit + rebalance-listener callback (report 07)

Actor 39. Branch `consumer-impl`. Test-parity effort: **prefer test-only
changes**. Production change ONLY for a genuine Java-fidelity bug, and only
if perf/CPU-neutral with the Java line cited.

Authoritative worklist: `design/current/test-translation-review/07-integration.md`.
Java contract (Apache Kafka 4.2):
- `kafka/.../consumer/PlaintextConsumerCommitTest.java`
- `kafka/.../consumer/PlaintextConsumerCallbackTest.java`
- `kafka/.../consumer/ConsumerIntegrationTest.java` (failed-listener pair only)

KIP-848 (`GroupProtocol.CONSUMER`) arm only. `testClassic*` twins are
OUT_OF_SCOPE per `consumer-threading.md` §20.

## Harness facts (decisive for gating + scope)

1. **Clusters are pooled + shared** (`tests/common/cluster_pool.rs`,
   `get_or_create` keyed by `ClusterConfig`). There is **no broker-shutdown
   API** on `KafkaCluster`. Tests that kill brokers cannot run against a
   shared pooled cluster (they would break every co-resident test) and the
   capability does not exist. → drives SKIP of the broker-fault test.
2. **Gating** is identical for all integration files: `#![cfg(feature =
   "integration-tests")]` on `tests/integration/main.rs`, each test
   `#[tokio::test(flavor = "multi_thread")]`. They are wired into
   `main.rs`'s `mod` list. No per-test broker gating beyond that — the
   harness spins a testcontainers cluster on demand. New files are wired in
   the same way (so they run in the same CI path).
3. **Listener structural gap (Issue 8, Phase-13):** the Rust rebalance
   listener has only `&self` (held as `Arc<dyn ConsumerRebalanceListener>`);
   `assign`/`position`/`beginning_offsets`/`seek`/`pause`/`resume` are all
   `async fn(&mut self)` on the consumer. A listener cannot call them. A
   channel-handshake to a driver deadlocks because the driver is blocked
   inside `consumer.poll()` when the listener runs (§31: listener runs on the
   caller's task, which IS the task inside `poll()`). This is already the
   documented `#[ignore]` reason for the poll-suite's
   `test_async_consumer_max_poll_interval_ms_delay_in_revocation`.

## File 1: `plaintext_consumer_commit_test.rs`

Cluster config: 3 brokers, KIP-848, `offsets.topic.num.partitions=1`,
`offsets.topic.replication.factor=3`, `group.min.session.timeout.ms=100`,
`num.partitions=2` (Java `@BeforeEach createTopic(topic, 2, 3)`).
Reuse the poll-suite producer/deserializer/`ensure_topic_with_2_partitions`
helpers (re-implemented locally; integration test files are independent
modules and the existing suites duplicate these helpers per file).

CONSUMER-arm translations (minus metrics):

| Java test | Rust | Notes |
|---|---|---|
| testAsyncConsumerAutoCommitOnClose | test_async_consumer_auto_commit_on_close | seek→close auto-commits; 2nd consumer `committed()` sees 300/500 |
| testAsyncConsumerAutoCommitOnCloseAfterWakeup | test_async_consumer_auto_commit_on_close_after_wakeup | same + `wakeup()` before close |
| testAsyncConsumerCommitMetadata | test_async_consumer_commit_metadata | OffsetAndMetadata leaderEpoch + metadata + null(empty) round-trip via committed() |
| testAsyncConsumerAsyncCommit | test_async_consumer_async_commit | 5× commitAsync, callback success==5, committed==5 |
| testAsyncConsumerCommitSpecifiedOffsets | test_async_consumer_commit_specified_offsets | per-partition commit, position unchanged, async pickup |
| testAsyncConsumerAutoCommitOnRebalance | test_async_consumer_auto_commit_on_rebalance | **see deviation** — pause inside callback is Issue 8; translate without the in-callback pause |
| testAsyncConsumerSubscribeAndCommitSync | test_async_consumer_subscribe_and_commit_sync | member-id propagation; subscribe→seek(0)→commit_sync |
| testAsyncConsumerPositionAndCommit | test_async_consumer_position_and_commit | position() on unassigned tp = IllegalState; commit/position interplay 2 consumers |
| testCommitAsyncCompletedBeforeConsumerCloses | test_commit_async_completed_before_consumer_closes | callback obligation: 2 commitAsync complete before close (success==2) |
| testCommitAsyncCompletedBeforeCommitSyncReturns | test_commit_async_completed_before_commit_sync_returns | async-callback-before-commitSync ordering |

### Deviation — testAutoCommitOnRebalance / pause-inside-callback

Java's listener calls `consumer.pause(partitions)` inside
`onPartitionsAssigned`. That is Issue 8 (a `&mut self` consumer call from an
`Arc<&self>` listener) and is structurally unsupported in Rust. The
*behavioral contract under test* is "auto-commit fires on rebalance; after
rebalance, committed() reflects the seeks." The pause is only there so the
test's own seeks are not overwritten by fetched-position advancement before
the rebalance. In Rust we achieve the same isolation by **not polling for
records between the seek and the triggering subscribe** (we drive the
rebalance with `awaitAssignment` only, and the partitions never fetch past
the sought offset because we seek immediately and immediately re-subscribe).
Documented inline. The auto-commit-on-rebalance + committed() readback — the
actual assertion — is preserved faithfully.

### SKIP (documented)

- testAsyncConsumerAutoCommitIntercept — **SKIP**: `ConsumerInterceptor`
  integration (MockConsumerInterceptor.ON_COMMIT_COUNT) is not in scope for
  this phase; interceptor onCommit wiring is a separate surface (report 07
  flags "no interceptor integration coverage at all"). Also depends on
  pause-inside-callback (Issue 8). Worklist allows skipping if interceptor
  integration is not done — it is not.
- testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose — **SKIP via
  `#[ignore]`**: requires `cluster.shutdownBroker()` on ALL brokers. The
  Rust harness pools+shares clusters and exposes no broker-shutdown API;
  killing brokers would break co-resident pooled tests. The exact-message
  contract ("Failed to commit offsets: Coordinator unknown and consumer is
  closing") + the <1s fast-close are already unit-tested in
  `commit_request_manager.rs` (line 4217+). Translated as an `#[ignore]`d
  test body with the assertion shape preserved so it is wired into CI and
  documents the gap; gated on harness broker-shutdown support.
- testClassic* twins — OUT_OF_SCOPE (§20).

## File 2: `plaintext_consumer_callback_test.rs`

Cluster config: 3 brokers, KIP-848 (no extra serverProperties in Java).

**The entire in-callback-reentrancy surface is Issue 8.** Every CONSUMER-arm
test in this file calls a `&mut self` consumer method
(assign/position/beginningOffsets/seek/pause) *inside* the listener. These
are structurally unsupported in Rust (see Harness fact 3). They are
translated as `#[ignore]`d test bodies citing Issue 8 with the Java
assertion shape preserved, so they are wired into CI and document the gap,
matching the precedent set by the poll suite.

| Java test | Rust | Status |
|---|---|---|
| testAsyncConsumerRebalanceListenerAssignOnPartitionsAssigned | ..._assign_on_partitions_assigned | #[ignore] Issue 8 (assign() in callback) |
| testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned | ..._assignment_on_partitions_assigned | #[ignore] Issue 8 (assignment() in callback)\* |
| testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned | ..._beginning_offsets_on_partitions_assigned | #[ignore] Issue 8 |
| testAsyncConsumerRebalanceListenerAssignOnPartitionsRevoked | ..._assign_on_partitions_revoked | #[ignore] Issue 8 |
| testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked | ..._assignment_on_partitions_revoked | #[ignore] Issue 8 |
| testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked | ..._beginning_offsets_on_partitions_revoked | #[ignore] Issue 8 |
| testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer | test_on_partitions_assigned_called_with_new_partitions_only | **RUNS** — listener only reads its `partitions` arg, no consumer reentrancy |
| testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback | ..._get_position_of_newly_assigned... | #[ignore] Issue 8 (position() in callback) |
| testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback | ..._seek_position_and_pause... | #[ignore] Issue 8 (seek+pause in callback) |

\* assignment() is sync `&self` on the trait, but the listener holds the
consumer as `Arc<dyn Listener>`, NOT a consumer reference — there is no
handle to call `.assignment()` on. Same structural gap. #[ignore].

The one runnable test, `testOnPartitionsAssignedCalledWithNewPartitionsOnly`,
asserts the KIP-848 incremental-assignment contract (assigned-callback
receives only NEWLY-added partitions on a subscription expansion). The
listener captures the `partitions` slice it was handed (via an
`Arc<Mutex<Vec<TopicPartition>>>`), which needs no consumer reentrancy.

## File 3: failed-listener pair → add to `consumer_test.rs`

`consumer_test.rs` is the bespoke E2E suite (report 07 §9) and is the most
fitting home for the two `ConsumerIntegrationTest` failed-listener tests
(they are subscribe→poll→recover flows, not commit/callback-reentrancy).

| Java test | Rust | Status |
|---|---|---|
| testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer | test_fetch_partitions_after_failed_listener | listener throws once on first onPartitionsAssigned; consumer recovers + delivers the record. The listener throws via returning `Err` (no consumer reentrancy needed). **RUNS.** |
| testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolConsumer | test_fetch_partitions_with_always_failed_listener | always-throwing assigned-listener; poll returns 0 records OR `KafkaException("User rebalance callback throws an error")`. **RUNS** (no reentrancy). |

### Fidelity check for failed-listener tests — possible production gap

Java wraps the listener error in `KafkaException("User rebalance callback
throws an error", e)` inside `AsyncKafkaConsumer.invokeRebalanceCallbacks`
(`AsyncKafkaConsumer.java:2334`). The Rust `process_background_events`
surfaces the listener's RAW error (`ConsumerRebalanceListenerInvoker`
returns the listener's error unwrapped). The always-failed test asserts the
exact message "User rebalance callback throws an error" *only inside a
`catch (KafkaException)` that is itself optional* — Java accepts poll
returning 0 records OR throwing that message. The first test (recover-once)
never asserts the message — it only requires recovery.

→ Translate the always-failed test to accept **either** poll returning 0
records **or** an error whose message contains "User rebalance callback
throws an error". If the Rust error is unwrapped (different message), the
test still needs to tolerate the divergence honestly. **Decision:** assert
the recover-once behavior strictly (RUNS, the high-value contract); for the
always-failed test, assert "0 records OR error surfaced" and document that
the exact wrapped message is a known Rust deviation (raw error, not
wrapped). DO NOT make a production change in this phase to add the wrapper
unless the test cannot otherwise pass — that wrapper is in the
already-translated invoke path and altering error text is a behavioral
change that belongs to a dedicated review, not a test-parity phase.
Re-evaluate after first run.

## Process
1. PLAN (this file).
2. Commit "Phase 39: integration commit", "Phase 39: integration callback
   reentrancy", "Phase 39: failed-listener recovery".
3. Verify: `cargo build --tests`, `cargo test --no-run`, `cargo xtask lint`,
   `cargo xtask format-check`. Broker availability — attempt to run; if no
   Docker, report compile-only + identical gating.
4. Self-review vs report 07.

## KNOWN API-CAPABILITY GAP — calling consumer methods from inside a rebalance callback (Issue 8 / Issue 3)

**Status: confirmed structural limitation of the current Rust consumer API.
NOT under-delivery. Tracked separately by the team; do NOT attempt an API
redesign in this phase.** The Critic confirmed this in COMMENTS.39 Issue 3.

### The gap

In the Java client a `ConsumerRebalanceListener` callback may freely call
back into the same consumer instance — `assign()`, `position()`, `seek()`,
`pause()`, `assignment()`, `beginningOffsets()`, etc. are all routinely used
from inside `onPartitionsAssigned` / `onPartitionsRevoked` /
`onPartitionsLost`. Several `PlaintextConsumerCallbackTest` cases exercise
exactly this, and `testAutoCommitOnRebalance` depends on an in-callback
`pause()`.

The Rust API cannot support this with its current shape:

1. `ConsumerRebalanceListener` methods take `&self`, and the listener is held
   as `Arc<dyn ConsumerRebalanceListener>` (it has NO handle to the
   consumer). See `src/consumer/consumer_rebalance_listener.rs`.
2. Per `consumer-threading.md` §31 the listener is invoked INLINE on the
   caller's task inside `process_background_events`, which runs inside
   `poll()` / `commit_*()` while `&mut self` (the whole consumer) is
   exclusively borrowed by that stack frame. The borrow checker therefore
   forbids the listener from holding any `&mut`-capable consumer handle, and
   `Box<dyn Consumer>` is not `Clone`, so there is no second handle to lend.
3. Even the read-only `&self` calls (`assignment()`, `beginning_offsets()`,
   `position()`) cannot be rescued by a driver/controller-channel pattern:
   the driver task is the very task currently blocked inside `poll()`, so it
   could not service the listener's request until `poll()` returns — but
   `poll()` will not return until the listener returns. That is a deadlock,
   which is precisely why §31 mandates inline invocation. So the read-only
   subset is structurally unsupported too.

### Consequence in this phase

The 8 in-callback-reentrancy tests in
`plaintext_consumer_callback_test.rs` (assign / assignment / seek / pause /
position / beginningOffsets called from inside a rebalance callback) are
`#[ignore]`d test bodies that preserve the Java assertion shape and cite
this gap, keeping the Java inventory traceable and wired into CI. This
matches the precedent set by the poll suite (Phase-13a Issue 8).
`testAutoCommitOnRebalance` is translated WITHOUT the in-callback `pause()`
and de-flaked by producing no records to the seeked partition (so no fetch
can advance the seeked position before the rebalance auto-commit captures
it) — see the deviation note on `test_async_consumer_auto_commit_on_rebalance`.

### Future direction (for the separate tracking item — not implemented here)

A future API revision could close the gap, e.g.:
  - a `ConsumerRebalanceListener` variant whose callbacks receive a
    restricted, `&mut`-capable rebalance-context handle (a `RebalanceCallbacks`
    object exposing the subset of consumer operations that are safe to call
    mid-rebalance); or
  - a re-entrant consumer handle design that lets the inline callback issue
    operations against the consumer it is running inside.

Recording the gap here (per consumer-threading.md §31) prevents it from being
silently normalized by the growing set of `#[ignore]`d callback tests.
