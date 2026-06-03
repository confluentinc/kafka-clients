# Phase 11: AsyncKafkaConsumer public impl

## Goal

Translate the user-facing `AsyncKafkaConsumer<K, V>` together with its two
helper classes and wire it into the `Consumer<K, V>` trait + factory from
Phase 2. After Phase 11 lands, the production consumer is constructible
via `new_consumer(config, key_de, value_de)` and exercises every blocking-
in-Java API end-to-end against an in-process `MockClient` test rig.

- `AsyncKafkaConsumer.java` (2368 LOC, pinned commit
  `a18251bae0b825c69794a50dffd4c3100cf5ca5b`)
  → `src/consumer/async_kafka_consumer.rs` implementing
  `Consumer<K, V>` (Phase 2 trait surface).
- `ConsumerRebalanceListenerInvoker.java` (141 LOC)
  → `src/consumer/internals/consumer_rebalance_listener_invoker.rs`.
- `ConsumerUtils.java` (262 LOC)
  → helpers split between
  `src/consumer/internals/consumer_utils.rs` (constants + the few helpers
  the consumer needs: `create_log_context`, `configured_isolation_level`,
  `create_subscription_state`, `create_fetch_metrics_manager`,
  `configured_consumer_interceptors`, `maybe_wrap_as_kafka_error`,
  `refresh_committed_offsets`) and re-exports from `consumer/mod.rs`.

Phase 11 also closes the carry-overs the Phase-10 notes flag:

1. **Bg-task spawn / lifecycle.** Construct `ConsumerNetworkThread`,
   `tokio::spawn(consumer_network_thread.run())`, retain the `JoinHandle`
   in the consumer struct.
2. **`process_background_events` per §31.** Invoked at the TOP of every
   blocking-style API (`poll`, `commit_sync`, `unsubscribe`, `close`,
   `position`, `committed`, `beginning_offsets`, `end_offsets`,
   `offsets_for_times`). Drains the bg-events channel via `try_recv` in
   a `while let` loop, invoking listener callbacks inline on the
   caller's task and completing each `oneshot::Sender` ack.
3. **`consumer.maximum_time_to_wait()` exposure** via `Arc<AtomicI64>`
   shared with the bg task (already in place from Phase 10).
4. **`consumer.close(timeout)` plumbing.** Flush async commits, run
   rebalance callbacks, `leave_group_on_close(operation)`,
   `signal_close()`, await `JoinHandle`.
5. **Auto-commit-sync-before-rebalance invocation.** Wires
   `CommitRequestManager::maybe_auto_commit_sync_before_rebalance` into
   the `consumer_membership_manager.rs:538` TODO marker via the Phase-11
   poll-path scaffolding.
6. **`reset_poll_timer` → `maybe_rejoin_stale_member`.** Called from
   `poll()` epilogue (Phase 8b note). The Phase 8b membership manager
   already has the method; Phase 11 just calls it.
7. **`consumer_membership_manager.leave_group()` /
   `leave_group_on_close()`.** Called from `close()` epilogue.
8. **`AsyncConsumerMetrics`** — out of scope for Phase 11. The three
   metric-bearing tests deferred across Milestone-8 remain deferred (see
   "Skipped tests"). The `AsyncConsumerMetrics` translation is left as a
   future cross-cutting commit, NOT folded into Phase 11.

## Branch / worktree

Lands on `consumer-impl` directly (no worktree). Phase 11 is a serial
bottleneck per `Milestone-8/PLAN.md` "Parallelism plan" — it depends on
Phases 2, 3, 5, 7, 8, 9, 10 (all closed) and only Phase 12 (integration
test) follows.

## Java sources

### Phase 11 production

- `org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java`
  (2368 LOC).
- `org/apache/kafka/clients/consumer/internals/ConsumerRebalanceListenerInvoker.java`
  (141 LOC).
- `org/apache/kafka/clients/consumer/internals/ConsumerUtils.java`
  (262 LOC).

### Out of scope (deferred or excluded)

- **`AsyncConsumerMetrics`** and `KafkaConsumerMetrics` recording calls
  inside `AsyncKafkaConsumer` — Milestone-8-wide metrics deferral. All
  `metrics.record*` / `kafkaConsumerMetrics.record*` calls become no-ops
  with a one-line comment pointing at the Java line.
- **Streams `*RebalanceData` / `StreamsRebalanceListener`** branches in
  `AsyncKafkaConsumer` — out of milestone scope per
  `consumer-threading.md` §20.
- **`ClientTelemetryReporter` / `ClientTelemetryUtils`** plumbing in the
  constructor — telemetry is not a Milestone-8 deliverable. Replace with
  `None` placeholders + a one-line comment.
- **`ConsumerInterceptors.on_consume` / `on_commit`** real invocation:
  the trait + struct exist from Phase 2; the consumer wires them but
  metrics emitted around interceptor failures are no-ops.
- **`acquire()` / `release()` thread-id reentrancy guard.** Java guards
  against multi-thread access via `currentThread` checks +
  `ConcurrentModificationException`. The Rust translation uses `&mut self`
  on the `Consumer` trait — Rust's borrow checker enforces single-caller
  exclusivity at compile time, making the runtime guard redundant. The
  three tests that exercise `ConcurrentModificationException` are not
  translatable (compile-time error in Rust); see "Skipped tests".
- **`ConsumerNetworkClient` + classic-protocol paths inside
  `ConsumerUtils.createConsumerNetworkClient`** — classic-protocol
  scope-deferred per `consumer-threading.md` §20.

### Tests

- `clients/consumer/internals/AsyncKafkaConsumerTest.java` (2226 LOC,
  98 `@Test` / `@ParameterizedTest` methods).
- Two `ConsumerRebalanceListener` regression tests required by
  `consumer-threading.md` §31 (NEW — no Java equivalent):
  - `on_partitions_revoked` calls `consumer.commit_sync()` from inside
    the callback and the commit succeeds.
  - Rebalance does not advance until the listener future resolves
    (listener blocks on a channel held by the test).
- Any `ConsumerRebalanceListenerInvokerTest.java` if present (TBD —
  Actor confirms during commit (1)).
- `ConsumerUtilsTest.java` if present (TBD — Actor confirms during
  commit (2)).

### Skipped tests

`AsyncKafkaConsumerTest`:

- `testFailConstructor` — exercises `Supplier<...>` ctor failure paths
  for fields that the Rust `new_consumer` factory takes as already-
  constructed values. The error path is observed instead via the
  `KafkaError` returned from `new_consumer` on bad config; the explicit
  supplier-failure test does not survive translation. One-line rationale
  carried in the Rust test file.
- `testCloseInvokesStreamsRebalanceListenerOnTasksRevokedWhenMemberEpochPositive`,
  `testCloseInvokesStreamsRebalanceListenerOnAllTasksLostWhenMemberEpochZeroOrNegative`,
  `testCloseWrapsStreamsRebalanceListenerException`,
  `testEmptyStreamRebalanceData`, `testStreamRebalanceData` — Streams
  out of scope per `consumer-threading.md` §20.
- `testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime` —
  `AsyncConsumerMetrics` deferred.
- `testGroupRemoteAssignorInClassicProtocol` — classic-protocol scope
  out per §20. The KIP-848 arm (`testGroupRemoteAssignorUsedInConsumerProtocol`)
  is translated.
- Any `ConcurrentModificationException`-asserting test (search the file
  for `ConcurrentModificationException`; if any are present the
  rationale above applies). Listed by name during commit (8).

All other tests translate as-is. Each skipped Java test method carries
a one-line `// SKIP: <reason>` rationale in the Rust file per DoD §3.

## Rust outputs

### NEW files

```
src/consumer/async_kafka_consumer.rs                # NEW — AsyncKafkaConsumer<K, V>
src/consumer/internals/consumer_rebalance_listener_invoker.rs
                                                    # NEW — invokePartitionsAssigned/Revoked/Lost
src/consumer/internals/consumer_utils.rs            # NEW — constants + helpers
tests/consumer/async_kafka_consumer_test.rs         # NEW — translated AsyncKafkaConsumerTest
tests/consumer/consumer_rebalance_listener_invoker_test.rs
                                                    # NEW — invoker tests (if Java test exists)
tests/consumer/consumer_utils_test.rs               # NEW — helpers tests (if Java test exists)
tests/consumer/consumer_rebalance_listener_regression_test.rs
                                                    # NEW — §31 regression pair
```

### Updated files

```
src/consumer/mod.rs                                  # new_consumer arm: Box::new(AsyncKafkaConsumer::new(...)?)
src/consumer/internals/mod.rs                        # pub(crate) mod {consumer_rebalance_listener_invoker, consumer_utils}
src/consumer/internals/consumer_membership_manager.rs # close out line-538 TODO marker via Phase-11 wiring
```

## Behavior parity

### `AsyncKafkaConsumer<K, V>`

Translated as a single concrete `pub struct AsyncKafkaConsumer<K, V>`
that owns:

```
struct AsyncKafkaConsumer<K, V> {
    // — Shared with the bg task —
    subscriptions:                 Arc<Mutex<SubscriptionState>>,
    metadata:                      Arc<ConsumerMetadata>,
    request_managers:              Arc<Mutex<RequestManagers>>,
    background_event_rx:           Receiver<BackgroundEvent>,
    application_event_handler:     Arc<ApplicationEventHandler>,
    completable_event_reaper:      Arc<Mutex<CompletableEventReaper>>,
    max_time_to_wait_ms:           Arc<AtomicI64>,
    wakeup_trigger:                Arc<WakeupTrigger>,
    network_thread_close:          NetworkThreadCloseHandle, // owns JoinHandle + signal_close
    // — App-side only —
    config:                        ConsumerConfig,
    client_id:                     Arc<str>,
    group_id:                      Option<String>,
    group_metadata:                Arc<Mutex<Option<ConsumerGroupMetadata>>>,
    rebalance_listener_invoker:    ConsumerRebalanceListenerInvoker,
    offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker>,
    deserializers:                 Arc<Deserializers<K, V>>,
    interceptors:                  Arc<Mutex<ConsumerInterceptors<K, V>>>,
    auto_commit_enabled:           bool,
    isolation_level:               IsolationLevel,
    default_api_timeout_ms:        i64,
    inflight_poll:                 Option<AsyncPollEvent>,
    closed:                        AtomicBool,
    // — §31 listener — set inside subscribe_with_listener —
    rebalance_listener:            Mutex<Option<Arc<dyn ConsumerRebalanceListener>>>,
}
```

Key translation notes:

- **`#[async_trait]` impl of `Consumer<K, V>`** (Phase 2 trait surface).
- **`poll(timeout)`** mirrors Java's `do { } while (timer.notExpired())`
  loop with `checkInflightPoll(timer, firstPass)`,
  `pollForFetches(timer)`, and an empty-fetch retry. The
  `AsyncPollEvent` is held in `inflightPoll: Option<AsyncPollEvent>`,
  recycled across `poll()` calls per Java's semantic. Wakeup-trigger
  `maybe_trigger_wakeup()` runs at the top of the loop and after
  empty-fetch decisions.
- **`commit_sync` / `commit_async`** dispatch via
  `applicationEventHandler.add(SyncCommitEvent { ... })` /
  `AsyncCommitEvent { ... }`. `commit_async_with_callback` registers
  the callback on the `OffsetCommitCallbackInvoker` (from Phase 9)
  before submitting the event, so the bg-side resolution drives the
  app-side callback drain on the next blocking-style API call.
- **`subscribe` / `subscribe_with_listener` / `subscribe_pattern` /
  `subscribe_pattern_with_listener`** acquire the
  `SubscriptionState` lock, mutate, drop the guard (§16), then submit
  a `TopicSubscriptionChangeEvent` / `TopicPatternSubscriptionChangeEvent`
  / `TopicRe2JPatternSubscriptionChangeEvent` to the bg task. The
  listener (when supplied) is stored in `self.rebalance_listener` for
  invocation by `process_background_events`.
- **`unsubscribe()`** acquires the lock, mutates, drops the guard,
  submits `UnsubscribeEvent`, awaits the handle, then calls
  `process_background_events` one final time to flush any pending
  rebalance-listener callbacks (Java's `processBackgroundEventsAtClose`
  equivalent inline).
- **`close(timeout)` / `close_with_options(options)`** translates
  Java's `private void close(Duration timeout,
  GroupMembershipOperation membershipOperation, boolean
  swallowException)`. Order of operations matches Java line-for-line:
  1. `kafkaConsumerMetrics.recordClose(...)` — NO-OP (metrics
     deferred).
  2. `applicationEventHandler.add(CommitOnCloseEvent)` if
     `autoCommitEnabled`.
  3. `autoCommitOnClose(timer)` — wait for pending async commits
     via `awaitPendingAsyncCommitsAndExecuteCommitCallbacks`.
  4. `runRebalanceCallbacksOnClose()` — invoke
     `on_partitions_revoked` / `on_partitions_lost` against the
     currently-assigned partitions.
  5. `leaveGroupOnClose(timer, membershipOperation)` →
     `applicationEventHandler.add(LeaveGroupOnCloseEvent {
     operation })` → await event handle.
  6. `stopFindCoordinatorOnClose()` →
     `applicationEventHandler.add(StopFindCoordinatorOnCloseEvent)`.
  7. `closeQuietly(applicationEventHandler)` → drop the handle and
     signal-close.
  8. `closeQuietly(consumerNetworkThread)` → `signal_close()` +
     `wakeup_trigger.wakeup()` + `join_handle.await`.
  9. `closeQuietly(metrics)`, `closeQuietly(interceptors)`,
     `closeQuietly(deserializers)`. Rust analogs are scoped Drop;
     no explicit call needed.
- **`process_background_events`** lives on `AsyncKafkaConsumer`. Per
  §31, it is called at the top of every blocking-style API. Body:
  ```rust
  while let Ok(event) = self.background_event_rx.try_recv() {
      match event {
          BackgroundEvent::Error(err) => { /* throw */ }
          BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded {
              method_name, partitions, ack,
          } => {
              // Invoke listener INLINE on this task. Drop the
              // SubscriptionState guard first (§16).
              let result = self.rebalance_listener_invoker
                  .invoke(method_name, &partitions).await;
              let _ = ack.send(result);
          }
      }
  }
  ```
- **`process_background_events_until(completable, deadline)`** is the
  Java-equivalent of `processBackgroundEvents` with a polled event —
  used by `commit_sync` and similar APIs. Mirrors Java's loop in
  `AsyncKafkaConsumer.java:1726` (`awaitPendingAsyncCommitsAndExecuteCommitCallbacks`):
  drain bg-events, check completion, sleep retry-backoff, repeat
  until completion OR timeout.
- **`wakeup()`** stays sync. Calls
  `self.wakeup_trigger.wakeup()` (already in place from Phase 5/10).
- **`assignment()` / `subscription()` / `paused()`** are sync —
  brief `subscriptions.lock()` → read → drop. NEVER `.await` while
  holding the lock (§16).
- **`group_metadata()`** mirrors Java: returns the cached
  `ConsumerGroupMetadata` updated by the `onMemberEpochUpdated`
  listener (`AsyncKafkaConsumer$MemberStateListener`).
- **Constructor `new(config, key_de, value_de)`** mirrors Java's
  primary constructor sequence (line 285–600). Builds:
  - `LogContext` → `Metrics` (stub) → `ApiVersions` →
    `MetadataRecoveryStrategy`.
  - `SubscriptionState`, `ConsumerMetadata`, `ChannelBuilder`,
    `Selector`, `NetworkClient` (via existing builders).
  - `BackgroundEventHandler`, `ApplicationEventHandler`,
    `CompletableEventReaper`, `WakeupTrigger`.
  - All request managers via `RequestManagers::new(...)`.
  - `ConsumerNetworkThread::new(...)` →
    `tokio::spawn(network_thread.run())` → store
    `NetworkThreadCloseHandle`.
- **Error path on partial construction** mirrors Java's `try-catch
  RuntimeException` block (line 593) that `closeQuietly`s every
  partially-constructed resource before rethrowing. Rust uses
  `?` + explicit cleanup helpers; the same `KafkaError` is
  surfaced.

### `ConsumerRebalanceListenerInvoker`

```
pub(crate) struct ConsumerRebalanceListenerInvoker {
    subscriptions: Arc<Mutex<SubscriptionState>>,
    // metrics_manager: RebalanceCallbackMetricsManager,  // DEFERRED (metrics)
}

impl ConsumerRebalanceListenerInvoker {
    pub(crate) async fn invoke_partitions_assigned(
        &self,
        listener: &Arc<dyn ConsumerRebalanceListener>,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    pub(crate) async fn invoke_partitions_revoked(
        &self,
        listener: &Arc<dyn ConsumerRebalanceListener>,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    pub(crate) async fn invoke_partitions_lost(
        &self,
        listener: &Arc<dyn ConsumerRebalanceListener>,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;
}
```

Differences from Java:

- Java's invoker stores the listener as a field (one listener per
  `AsyncKafkaConsumer`). Rust passes the listener in per-call so the
  invoker is reusable; the consumer holds the listener in its own
  `Mutex<Option<Arc<dyn ConsumerRebalanceListener>>>` field.
- `Exception` return type → `Result<(), KafkaError>`. The caller
  (`process_background_events`) propagates the result through the
  oneshot ack channel.
- Metrics recording inside the invoker is dropped (Milestone-8-wide
  metrics deferral).
- Java's `pop()`-from-`SubscriptionState` calls
  (`subscriptions.markPendingRevocation`,
  `subscriptions.markPendingOnAssignedCallback`) are translated by
  acquiring the `SubscriptionState` lock, mutating, dropping the
  guard, THEN `.await`ing the listener (§16).

### `ConsumerUtils`

Translate to `src/consumer/internals/consumer_utils.rs`:

- Constants: `DEFAULT_CLOSE_TIMEOUT_MS`, `CONSUMER_JMX_PREFIX`,
  `CONSUMER_METRIC_GROUP_PREFIX`, `CONSUMER_METRIC_GROUP`,
  `CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION`,
  `THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED`.
- `create_log_context(config, group_rebalance_config)` — stub
  returning `Arc<str>` of the prefix; consumer logging uses the
  `log` crate.
- `configured_isolation_level(config) -> IsolationLevel` —
  parses the config string.
- `create_subscription_state(config, log_prefix) -> SubscriptionState`
  — `SubscriptionState::new(AutoOffsetResetStrategy::from_str(...))`.
- `create_fetch_metrics_manager(metrics) -> FetchMetricsManager` —
  passes through (existing in `src/consumer/internals/` from
  Phase 7).
- `configured_consumer_interceptors(config) -> Vec<Arc<dyn ConsumerInterceptor<K, V>>>`
  — reads `interceptor.classes` and instantiates via the
  existing classloader pattern; in Rust the user passes
  interceptors via `ConsumerConfig::set_interceptors` (Phase 2
  decision — no Class-from-String reflection). Returns the
  configured list; defaults to empty.
- `maybe_wrap_as_kafka_error(err) -> KafkaError` /
  `maybe_wrap_as_kafka_error_with_msg(err, msg) -> KafkaError` —
  wraps non-`KafkaError` errors.
- `refresh_committed_offsets(offsets_and_metadata, metadata,
  subscriptions)` — used by `CommitRequestManager` continuation
  paths (sync helper).
- `get_result(future, timer/deadline)` — Java's blocking
  `Future.get(timeoutMs)` translates to `tokio::time::timeout` in
  Rust. Used by Phase 11 commit-sync helpers.

`createConsumerNetworkClient` and `createMetrics(... reporters)`
are NOT translated (classic-protocol-only / metrics-deferred per
above).

## Commit plan

Each commit is independently buildable and has its own tests passing.

| # | Title | Files touched | Approx LOC |
|---|---|---|---|
| 1 | `Phase 11 (1/N): ConsumerUtils + ConsumerRebalanceListenerInvoker` | `consumer_utils.rs`, `consumer_rebalance_listener_invoker.rs` + tests | ~250 prod / ~250 tests |
| 2 | `Phase 11 (2/N): AsyncKafkaConsumer — struct + constructor + Consumer state-reads` | `async_kafka_consumer.rs` ctor + `assignment` / `subscription` / `paused` / `client_id` / `group_metadata` / `current_lag` / `wakeup` | ~400 prod / ~150 tests |
| 3 | `Phase 11 (3/N): AsyncKafkaConsumer — subscribe / unsubscribe / assign + §31 listener storage` | `async_kafka_consumer.rs` subscribe variants + `process_background_events` skeleton + listener field plumbing | ~350 prod / ~250 tests |
| 4 | `Phase 11 (4/N): AsyncKafkaConsumer — poll + checkInflightPoll + maybeClearInflightPoll` | `async_kafka_consumer.rs` poll loop + `AsyncPollEvent` lifecycle + `pollForFetches` + `sendPrefetches` | ~400 prod / ~350 tests |
| 5 | `Phase 11 (5/N): AsyncKafkaConsumer — commit (sync + async) + OffsetCommitCallbackInvoker drain` | `async_kafka_consumer.rs` commit_sync / commit_async / commit_sync_offsets / awaitPendingAsyncCommits | ~350 prod / ~300 tests |
| 6 | `Phase 11 (6/N): AsyncKafkaConsumer — seek + position + committed + currentLag + beginning/end-offsets + offsetsForTimes + partitionsFor + listTopics + pause/resume + enforceRebalance` | `async_kafka_consumer.rs` remaining trait methods | ~500 prod / ~400 tests |
| 7 | `Phase 11 (7/N): AsyncKafkaConsumer — close + runRebalanceCallbacksOnClose + leaveGroupOnClose + stopFindCoordinatorOnClose + lifecycle wiring` | `async_kafka_consumer.rs` close path + factory wire-up in `consumer/mod.rs` + `consumer_membership_manager.rs:538` TODO close-out | ~400 prod / ~300 tests |
| 8 | `Phase 11 (8/N): AsyncKafkaConsumerTest — translate test fixture + state-read + subscribe / unsubscribe tests` | `tests/consumer/async_kafka_consumer_test.rs` (first ~30 tests) | ~700 tests |
| 9 | `Phase 11 (9/N): AsyncKafkaConsumerTest — poll / commit / wakeup tests` | `tests/consumer/async_kafka_consumer_test.rs` (next ~30 tests) | ~700 tests |
| 10 | `Phase 11 (10/N): AsyncKafkaConsumerTest — close / metadata / lifecycle tests` | `tests/consumer/async_kafka_consumer_test.rs` (remaining ~35 tests) | ~700 tests |
| 11 | `Phase 11 (11/N): ConsumerRebalanceListener regression tests (§31 pair)` | `tests/consumer/consumer_rebalance_listener_regression_test.rs` | ~300 tests |
| 12 | `Phase 11 (12/N): wire-up + lint pass + agent-memory notes` | final `cargo xtask format-check + lint` + agent memory entry + `design/current/` updates | small |

**Granularity rationale:** Commits 2–7 split the consumer implementation
by Java method group so each commit lands a buildable slice with
matching tests. Commits 8–10 split the 98-test Java file into three
~30-test batches (Phase 10 commit 6/N translated 39 tests in one
commit; the AsyncKafkaConsumerTest tests are heavier per-test because
each exercises the full bg-task + event-processor stack, so splitting
across three commits is the safer cadence). Commit 11 is the §31 pair
that has no Java analog.

**Inter-commit dependencies:** (2) blocks (3)–(7); (3)–(7) can land in
strict order; (8)–(10) require the matching production commits. (11)
can land any time after (7).

## Module structure (final)

```
src/consumer/
├── async_kafka_consumer.rs                  # NEW
├── mod.rs                                    # new_consumer arm wired
└── internals/
    ├── consumer_rebalance_listener_invoker.rs   # NEW
    ├── consumer_utils.rs                         # NEW
    ├── consumer_membership_manager.rs            # line-538 TODO closed
    └── mod.rs                                    # pub(crate) registrations

tests/consumer/
├── async_kafka_consumer_test.rs              # NEW
├── consumer_rebalance_listener_invoker_test.rs  # NEW (if Java has)
├── consumer_utils_test.rs                    # NEW (if Java has)
└── consumer_rebalance_listener_regression_test.rs # NEW (§31 pair)
```

## Definition of Done

Per `definition-of-done.md` plus the Milestone-8 phase additions:

- `cargo build`, `cargo test`, `cargo xtask format-check`,
  `cargo xtask lint`, `cargo xtask check-generated` clean.
- Every in-scope test from `AsyncKafkaConsumerTest` translated and
  passing. Skipped tests listed above with a one-line rationale each
  (DoD §3).
- `cargo test --lib async_kafka_consumer` passes.
- `cargo test --test async_kafka_consumer_test` passes.
- `cargo test --test consumer_rebalance_listener_regression_test`
  passes (§31 pair — required by `consumer-threading.md`).
- The §31 commit-from-inside-on_partitions_revoked test exercises a
  real `commit_sync()` call from inside the listener callback (NOT a
  mock). Asserts the commit completes successfully.
- The §31 rebalance-blocks-on-listener test holds the listener future
  on a `tokio::sync::oneshot::Receiver`, observes that the membership
  state has not advanced past `Reconciling`, releases the channel,
  observes the state advance.
- No `panic!` / `unimplemented!` / `todo!` / `unsafe` in production
  code. The Phase-10-carry-over `TODO:` marker at
  `consumer_membership_manager.rs:538` is closed (the actual flush
  call is wired or the marker is rewritten to a `// Phase-12 note`).
- **Trait surface check (DoD §11):**
  - `Consumer<K, V>` factory returns `Box<dyn Consumer<K, V>>` — verified.
  - `AsyncKafkaConsumer` `impl Consumer<K, V> for ...` is the only
    production impl; `MockConsumer` is the only test impl.
  - No `block_on`-wrapped sync façade.
  - Per-record traits (`Deserializer`) remain sync — verified via
    `grep -n "async fn deserialize"` returning nothing.
- **Lock-discipline audit (§16):**
  - `SubscriptionState` guard never held across `.await`. Walked by
    the Critic in the close-out round.
  - `RequestManagers` guard never held across `.await` (§ Phase-10
    pattern #3).
  - Listener invocations drop both `SubscriptionState` and
    `rebalance_listener` guards before `.await`-ing the callback.
- **§31 invocation thread audit:**
  - `process_background_events` is called at the TOP of every
    blocking-style API. Critic walks each API arm.
  - Listener methods are invoked inline on the caller's task
    (`.await` directly, not `tokio::spawn`).
  - No "drain bg-events on a separate task" anywhere.
- **§11 wakeup audit:**
  - `wakeup()` uses `WakeupTrigger::wakeup()` (rotating-token), NOT
    `AtomicBool`.
  - Every blocking-style API `select!`s `wakeup_token.cancelled()` as
    a branch (via `WakeupTrigger::maybe_trigger_wakeup` /
    `wait_for_response_or_wakeup`).
  - On returning `KafkaError::Wakeup`, the token is rotated.
- **Phase-10 carry-overs closed:**
  - Bg task spawned and joined on `close()`.
  - `process_background_events` per §31 — DONE.
  - `consumer.maximum_time_to_wait()` exposure — DONE.
  - `consumer.close(timeout)` plumbing — DONE.
  - `consumer_membership_manager` auto-commit-before-rebalance
    invocation — DONE (via Phase-11 poll-path scaffolding).
  - `reset_poll_timer` → `maybe_rejoin_stale_member` invocation in
    `poll()` epilogue — DONE.
  - `consumer_membership_manager.leave_group()` /
    `leave_group_on_close()` invocation from `close()` epilogue —
    DONE.

## Parallelism inside Phase 11

Commits (1) and (2) are independent and can be opened in parallel by
separate Actors. After (2) lands, (3)–(7) are strictly serial because
they all extend `async_kafka_consumer.rs`. Test commits (8)–(10) can
parallelize with their matching production commits ONLY if the test
batches are split file-internally (same test file → merge conflicts
likely). Default plan: single Actor running serially.

## Carry-overs to Phase 12 (do NOT attempt here)

- Integration test against a real Kafka 4.2.0 broker via testcontainers
  — Phase 12 owns it. The Phase-11 tests run against the in-process
  `MockClient` rig.
- `AsyncConsumerMetrics` translation — explicitly deferred across
  Milestone-8. The three metric-bearing tests
  (`testRunOnceRecordTimeBetweenNetworkThreadPoll`,
  `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`,
  `testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime`,
  reaper-invocation metric assertions) remain deferred. They will land
  in a separate cross-cutting commit AFTER Phase 12 closes (or a later
  Milestone), NOT in Phase 11.
- C FFI for `AsyncKafkaConsumer` — out of Milestone-8 scope per the
  parent PLAN.md "Out of scope" list.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor agent N=1 spawned to implement commit-by-commit (12 commits).
2. After each batch of 2–3 commits, Critic agent N=1 reviews via
   `cargo xtask await-commit` and writes comments to
   `design/history/Milestone-8/Phase-11/COMMENTS.1.md`.
3. Actor reads `COMMENTS.1.md`, fixes each issue, moves resolved
   comments to `COMMENTS.DONE.1.md`, commits as `fixup!` referencing
   the original commit.
4. Loop until `COMMENTS.1.md` is empty AND every DoD checkbox passes.
5. Phase closes with a Status section appended to this PLAN.md, the
   final commit hash table populated, and the Phase 11 row in
   `Milestone-8/PLAN.md` flipped to closed in
   `design/current/`.
