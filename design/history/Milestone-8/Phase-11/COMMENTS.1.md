# Phase 11 Batch 1 Review — Critic N=1

Reviewing commits:
- `aa29e7c` Phase 11 (1/N): ConsumerUtils + ConsumerRebalanceListenerInvoker
- `77b0cbb` Phase 11 (2/N): AsyncKafkaConsumer struct + ctor + state-read methods
- `0d88bb9` Phase 11 (3/N): subscribe/unsubscribe/assign + §31 process_background_events skeleton

Findings below. All in-scope deferrals per PLAN.md (metrics, classic protocol, Streams,
acquire/release reentrancy guard, ClientTelemetry, ConsumerNetworkClient,
testFailConstructor, testGroupRemoteAssignorInClassicProtocol) are not flagged.

Issues 1, 2, 8 were resolved in commit 4/N — moved to COMMENTS.DONE.1.md.
Issues 3, 4, 9 were resolved in commit 7/N — moved to COMMENTS.DONE.1.md.

---

## Verdict on Actor's flagged uncertainties

1. **`unsubscribe()` lacks iterative `process_background_events` loop.**
   Real deadlock risk under realistic conditions BUT no production caller
   wires this method in commit 3 (no trait impl, no factory). Acceptable
   as a documented seam IF commit 4 lands the iterative loop. **Verify
   in commit-4 review.**  [RESOLVED — moved to COMMENTS.DONE.1.md.]

2. **`process_background_events` does not call `backgroundEventReaper.reap(time)`.**
   Acceptable to defer. Track as commit-4 requirement.  [RESOLVED — moved to
   COMMENTS.DONE.1.md.]

3. **`subscribe_to_empty_list_acts_as_unsubscribe` recurses into `unsubscribe`.**
   Not a bug — `&mut self` + single-task async makes recursion sound.

---

## Overall recommendation (batch 1)

Issues 5, 6, 7 are nits and can be batched with commit 11 or 12.

---

# Phase 11 Batch 2 Review — Critic N=1

Reviewing commits:
- `5f6dee9` Phase 11 (4/N): poll + checkInflightPoll + AsyncPollEvent lifecycle
- `3a7bcf1` Phase 11 (5/N): commit (sync + async) + OffsetCommitCallbackInvoker drain
- `de1d475` Phase 11 (6/N): seek/position/committed/lag/offsets/pause-resume/topic-metadata/enforceRebalance
- `069a6c4` Phase 11 (7/N): close + Consumer trait impl + factory wire-up

New issues continue from Issue 10. Issues from batch 1 still open: 5, 6, 7.

---


## Issue 16: `throwIfGroupIdNotDefined` translated as `IllegalArgument` instead of a dedicated `InvalidGroupId` error variant

- **File**: `src/consumer/async_kafka_consumer.rs:611-619`
- **Severity**: Behavior Mismatch (exception-hierarchy contract)
- **Java Reference**: `AsyncKafkaConsumer.java:1192-1197` —
  `throwIfGroupIdNotDefined` throws `InvalidGroupIdException` (a
  dedicated `ApiException` subclass, `o.a.k.common.errors.InvalidGroupIdException`).
  CLAUDE.md rule 2: "Java `Exception` → Rust `Error`
  (e.g. `TopicAuthorizationException` → `TopicAuthorizationError`)".
- **Description**: Java's `InvalidGroupIdException` is a specific
  exception class. User code catching it (or testing `instanceof
  InvalidGroupIdException`) gets a clear signal that the group.id is
  the problem. Rust collapses this to `KafkaError::IllegalArgument`,
  losing the type discriminator. The Rust test for `commit_sync_without_group_id`
  is in fact asserting on a message-substring (`m.contains("group.id")`)
  rather than the variant — a test smell pointing at the missing
  variant.
- **Expected**: Add `KafkaError::InvalidGroupId(String)` (or equivalent)
  per CLAUDE.md naming convention; update
  `throw_if_group_id_not_defined` to return it.
- **Actual**: Returns `IllegalArgument`; tests downstream rely on a
  substring match of the message instead of the type.

---

## Issue 17: `commit_sync_invokes_interceptor_chain` test does not actually exercise the interceptor — `ConsumerInterceptors` is empty

- **File**: `src/consumer/async_kafka_consumer.rs:3492-3514`
- **Severity**: Test Coverage Gap (DoD §3)
- **Java Reference**: `AsyncKafkaConsumerTest.testInterceptorOnCommit`
  registers a tracking interceptor and asserts `on_commit` was called
  with the committed offsets.
- **Description**: The test name promises "commit_sync invokes
  interceptor chain", but the test fixture's `make_test_consumer_with_channels`
  builds the consumer with an EMPTY `ConsumerInterceptors` (line 2839:
  `ConsumerInterceptors::<Vec<u8>, Vec<u8>>::new(Vec::new())`). The
  test only asserts the completer task saw the `CommitSync` envelope.
  An interceptor that's silently never invoked because the chain is
  empty would still pass.

  This is a trivially-passing test masquerading as interceptor
  coverage. The Java parity is missed.

- **Expected**: Register a tracking interceptor (e.g. an `Arc<Mutex<Vec<HashMap<...>>>>`-backed
  test interceptor) on the consumer, then assert the tracker observed
  the commit offsets after `commit_sync_offsets` returns.
- **Actual**: The interceptor chain is empty; the test trivially passes
  regardless of whether `on_commit` is invoked.

---

## Issue 18: `commit_inner` and `commit_sync_internal` use a single `deadline_ms` where Java creates a fresh `requestTimer` after `commit(...)` returns

- **File**: `src/consumer/async_kafka_consumer.rs:1505-1543`
- **Severity**: Behavior Mismatch (minor — Rust is more conservative)
- **Java Reference**: `AsyncKafkaConsumer.java:1706-1724`
  ```java
  SyncCommitEvent syncCommitEvent = new SyncCommitEvent(offsets, calculateDeadlineMs(time, timeout));
  CompletableFuture<...> commitFuture = commit(syncCommitEvent);
  Timer requestTimer = time.timer(timeout.toMillis()); // FRESH timer here
  awaitPendingAsyncCommitsAndExecuteCommitCallbacks(requestTimer, true);
  wakeupTrigger.setActiveTask(commitFuture);
  ConsumerUtils.getResult(commitFuture, requestTimer);
  ```
- **Description**: Java creates `requestTimer` AFTER `commit(...)`
  returns (i.e. after the `offsets_ready` wait). So the
  `awaitPendingAsyncCommits` + `getResult` block uses a fresh timer
  with the FULL user-supplied `timeout` duration. The total wall-clock
  bound on Java's `commitSync` is therefore up to `2 * timeout`
  (one timeout for `commit_inner`'s offsets_ready, one for the rest).

  Rust uses a single `deadline_ms` computed at the top of
  `commit_sync_internal` (line 1512), so Rust caps the total wall-clock
  at `1 * timeout`. This is more conservative and arguably more correct,
  but it's a documented divergence — Java's behavior is observable in
  user code that times `commit_sync` calls.

- **Expected**: Either (a) match Java by creating a fresh deadline after
  `commit_inner` returns, OR (b) document the divergence in a
  code-comment + the Phase 11 skip-rationale list.
- **Actual**: Single deadline; not documented as a divergence.

---

## Issue 19: `committed_timeout` error-formatting uses Rust's `{:?}` Debug formatting where Java uses `Set.toString()`

- **File**: `src/consumer/async_kafka_consumer.rs:1846-1850`
- **Severity**: Behavior Mismatch (error-message exactness)
- **Java Reference**: `AsyncKafkaConsumer.java:1180-1182`:
  ```java
  throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
      "committed offset for partitions " + partitions + " could be determined. Try tuning " +
      ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + " larger to relax the threshold.");
  ```
  Java's `partitions.toString()` produces `[topic-0, topic-1]`-style
  output. Rust's `{:?}` on `&[TopicPartition]` produces
  `[TopicPartition { topic: "t", partition: 0 }]`-style.
- **Description**: Any Java test that asserts exact error message
  content (e.g. `assertEquals("Timeout of 5000ms expired before the
  last committed offset for partitions [t-0] could be determined. ...",
  e.getMessage())`) would fail in Rust without a custom Display
  implementation. The Java contract for error messages is part of the
  behavioral contract (DoD §3: "Error message content is asserted, not
  just `is_err()` — error messages are part of the behavioral
  contract").

  Same pattern applies to `commit_sync_internal`'s timeout message
  (line 1537-1541: `{:?}` on `offsets: Option<HashMap<...>>`).
- **Expected**: Implement a helper that formats `&[TopicPartition]` as
  Java's `Set.toString()` would (i.e. `[t-0, t-1]`), and use it in all
  error messages.
- **Actual**: Debug-formatted; exact Java-message assertions
  unreachable.

---

## Issue 20: `close_then_apis_error_with_already_closed` test exercises only `commit_sync` and `unsubscribe`; misses 14 other blocking-style APIs

- **File**: `src/consumer/async_kafka_consumer.rs:3886-3918`
- **Severity**: Test Coverage Gap (DoD §3)
- **Java Reference**: `AsyncKafkaConsumerTest.testShouldThrowAfterClose`
  asserts every public method on a closed consumer throws.
- **Description**: The Rust test verifies `commit_sync` and `unsubscribe`
  return `IllegalState` after `close()`. It does NOT verify any of:
  - `poll`, `commit_async`, `commit_sync_offsets`,
    `commit_sync_offsets_timeout`, `commit_async_with_callback`,
    `commit_async_offsets_with_callback`
  - `seek`, `seek_with_metadata`, `seek_to_beginning`, `seek_to_end`
  - `position`, `position_timeout`, `committed`, `committed_timeout`,
    `current_lag_async`
  - `beginning_offsets`, `end_offsets`, `offsets_for_times`,
    `partitions_for`, `list_topics`
  - `pause`, `resume`, `enforce_rebalance`, `close_with_options`

  Java's test pattern (one loop over all methods, asserting each
  throws) catches regressions where a future change forgets to call
  `ensure_open()`. The Rust test misses 25+ APIs.

- **Expected**: Add a single test that calls every blocking-style API
  after close and asserts each returns `IllegalState`.
- **Actual**: Only 2 APIs covered.

---

## Verdict on Actor's flagged uncertainties (batch 2)

1. **Factory `new_consumer` returns `unsupported_version` deferring to
   Phase 12.** Acceptable as a documented seam — the seam is cited in
   the factory's error message AND in the PLAN.md (`Phase 12` is
   explicitly the production-ctor commit). The trait-surface compile
   check (`consumer_trait_impl_compiles`) confirms `Box<dyn
   Consumer<K, V>>` dispatch works for tests. **Verdict: OK to defer.**
   Suggestion: the error message could differentiate "deferred / not
   yet implemented" from "permanently unsupported" so users don't
   confuse this with the `Classic` arm. Lower priority.

2. **`run_rebalance_callbacks_on_close` divergence (`subscriptions.assignedPartitions()`
   vs `groupAssignmentSnapshot.get()`).** Filed as Issue 12. Real
   divergence — Java's snapshot is updated by reconciliation, Rust uses
   subscriptions which includes manual-assign and excludes
   not-yet-revoked partitions. Combined with the missing `update_group_metadata`
   (Issue 13), this means close-time callback dispatch is wrong in
   multiple ways. NOT just nit-level — recommend addressing in commit
   8-12 batch.

3. **Auto-commit-before-rebalance deadline (`current_time_ms.saturating_add`).**
   Verified vs Java `getDeadlineMsForTimeout` (line 922-928).
   `saturating_add` clamps to `i64::MAX` on overflow; Java clamps to
   `Long.MAX_VALUE`. **Verdict: matches Java exactly.** No issue here.

---

## Recommendation

**Block on Issues 10, 11, 13 before commits 8-11.** These are
contractual gaps (deadlock + wakeup-ignore + close-callback-wrong) that
the §31 regression pair tests in commit 11 are specifically designed
to catch — running those tests against the current state will likely
expose the deadlock from Issue 10 directly.

Issues 12, 14, 15 can land alongside the test-translation commits
(8-10) since they're small targeted fixes.

Issues 16-20 are quality / test-coverage nits that can batch with
commit 11 or 12, similar to the open Issues 5-7 from batch 1.

---

# Phase 11 Batch 3 Review — Critic N=1

Reviewing fixup commits (Issues 10-15 closure) + commits 8/N, 9/N, 10/N:
- `4f89c10` fixup! Issues 12 + 13 — MemberStateListener + group_assignment_snapshot
- `b5ce5c5` fixup! Issues 10 + 11 — submit_and_drain + per-API wakeup matrix
- `08620be` fixup! Issues 14 + 15 — close timeout cap + position non-Timeout error propagation
- `3e1b28b` Phase 11 (8/N): fixture + state-read + subscribe/unsubscribe tests
- `139e2da` Phase 11 (9/N): poll / commit / wakeup tests
- `30b48fd` Phase 11 (10/N): close / metadata / lifecycle tests

## Fixup confirmation verdict

- **Issue 10 (submit_and_drain coverage)**: VERIFIED. Every blocking-style API
  routes through `submit_and_drain` or `process_background_events_until`.
  Audited each call site against Java's `addAndGet` sites. The Issue 10
  regression test (`issue_10_commit_sync_drains_listener_callback_while_waiting`)
  exercises the exact deadlock scenario described in the original issue.

- **Issue 11 (per-API wakeup matrix)**: VERIFIED with one caveat.
  Per-API matrix matches Java's `setActiveTask` call sites
  (committed/partitionsFor/listTopics/commitSync/CheckAndUpdatePositions=true,
  everything else=false). The deliberate tightening for `commit_inner`'s
  `offsets_ready_rx` wait (`enable_wakeup=true` even though Java has no
  setActiveTask there) is acknowledged in a code comment — but it has a
  user-visible consequence in `commit_async`: see Issue 22 below.

- **Issue 12 (group_assignment_snapshot)**: VERIFIED. `run_rebalance_callbacks_on_close`
  reads from `group_assignment_snapshot` (matches Java line 1624) with
  early-return on empty (line 2756). Three regression tests cover the
  three Java code paths (snapshot empty, live epoch, unknown epoch).

- **Issue 13 (MemberStateListener)**: VERIFIED as a documented seam.
  `ConsumerStateNotifier` impls `MemberStateListener`, exposed via
  `state_notifier()` accessor. Production wire-up is correctly deferred
  to Phase 12. The 3 notifier unit tests cover the bridge correctness.

- **Issue 14 (position non-Timeout propagation)**: VERIFIED. The
  blanket `.await.ok()` is replaced with an explicit `match` that
  propagates non-Timeout errors. Regression test
  `issue_14_position_propagates_non_timeout_errors` exercises an
  explicit non-Timeout error from the bg task.

- **Issue 15 (close timeout cap at request.timeout.ms)**: VERIFIED.
  `close_internal` line 2616-2619 caps `timeout` at `request_timeout_ms`
  before computing the deadline. Test `close_caps_timeout_at_request_timeout_ms`
  asserts the deadline carried by the event handle.

All six fixups are correctly closed and can be moved to COMMENTS.DONE.1.md.

---

[Issue 21 — RESOLVED — moved to COMMENTS.DONE.1.md.]
[Issue 22 — RESOLVED — moved to COMMENTS.DONE.1.md.]
[Issue 23 — RESOLVED — moved to COMMENTS.DONE.1.md.]

---

## Issue 24: Untranslated Java tests with no skip rationale — close, metadata, listOffsets behaviors

- **File**: `src/consumer/async_kafka_consumer.rs` (test skip sections)
- **Severity**: Test Coverage Gap (DoD §3)
- **Java Reference**: Multiple — listed below.
- **Description**: The following Java tests are NOT translated AND NOT
  listed in any `// SKIP:` rationale in the test module. None of them
  are pre-approved deferrals in PLAN.md. They cover real behavioral
  contracts:

  | Java test | Java line | Behavior under test |
  |---|---|---|
  | `testCloseAwaitPendingAsyncCommitIncomplete` | 1068-1085 | `close(Duration.ZERO)` with a pending async commit must throw `KafkaException` with `TimeoutException` cause |
  | `testCloseLeavesGroupDespiteOnPartitionsLostError` | 707-728 | Close still sends LeaveGroupOnClose when `on_partitions_lost` throws; the listener error becomes the close error |
  | `testCloseLeavesGroupDespiteInterrupt` | 732-755 | Close still enqueues `CommitOnCloseEvent` + `LeaveGroupOnCloseEvent` even when `addAndGet` throws `InterruptException` |
  | `testBeginningOffsetsTimeoutOnEventProcessingTimeout` | 899-908 | Distinct from `testBeginningOffsetsTimeoutException`: this one verifies the event was enqueued via `addAndGet` even when it times out — not the same as the existing `beginning_offsets_propagates_timeout` |
  | `testBeginningOffsetsWithZeroTimeout` | 996-1005 | Zero-timeout path enqueues the event via `add` (not `addAndGet`) and returns empty map. Rust HAS this code path (line 2242-2251); no inline test |
  | `testOffsetsForTimesWithZeroTimeout` | 1007-1017 | Symmetric for `offsetsForTimes`. Rust HAS the code path (line 2317-2333); no inline test |
  | `testOffsetsForTimesFailsOnNegativeTargetTimes` | 917-932 | Three asserts — EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP-1. Rust HAS the negative-validation code (line 2304-2310); no inline test |
  | `testOffsetsForTimesTimeoutException` | 952-963 | Asserts EXACT error message `"Failed to get offsets by times in {timeout}ms"`. Rust constructs this exact message (line 2278-2281); no inline test asserts the exact message |
  | `testGroupRemoteAssignorUsedInConsumerProtocol` | (search the Java file) | Counterpart to the translated `testGroupRemoteAssignorUnusedIfGroupIdUndefined` |

- **Expected**: Either translate each test with the same name + body
  semantics, or add a specific `// SKIP:` rationale for each pointing
  at the inline test that supposedly covers it.
- **Actual**: Neither translated nor justified.

---

## Issue 25: `beginning_offsets_propagates_timeout` test does not assert exact error message — DoD §3 violation

- **File**: `src/consumer/async_kafka_consumer.rs:5942-5963`
- **Severity**: Test Correctness (DoD §3 — exact-message assertion)
- **Java Reference**: `testBeginningOffsetsTimeoutException` (Java line 965-977) asserts:
  ```java
  assertEquals("Failed to get offsets by times in " + timeout + "ms", t.getMessage());
  ```
- **Description**: The Rust test asserts only
  `matches!(err, KafkaError::Timeout(_))`. The exact error message
  (the user-facing error contract documented at DoD §3) is not
  verified. The same applies to the symmetric `testEndOffsetsTimeoutException`
  — the comment on the Rust test claims it covers both, but the message
  assertion is missing entirely.

  This is the same pattern previously flagged as Issue 5 in batch 1 and
  in `feedback_match_argued_framing.md` / DoD §3: "Error message
  content is asserted, not just `is_err()` — error messages are part
  of the behavioral contract."
- **Expected**:
  ```rust
  let err = consumer.beginning_offsets_timeout(&[tp], Duration::from_millis(100)).await.expect_err("must err");
  match err {
      KafkaError::Timeout(msg) => assert_eq!(msg, "Failed to get offsets by times in 100ms"),
      other => panic!("expected Timeout, got {other:?}"),
  }
  ```
- **Actual**: Only the variant is checked.

---

## Issue 26: `unsubscribe_without_group_id_enqueues_event` test mutates private field instead of constructing a no-group consumer

- **File**: `src/consumer/async_kafka_consumer.rs:3914-3921`
- **Severity**: Test Correctness (test fidelity)
- **Java Reference**: `testUnsubscribeWithoutGroupId` (search Java file)
- **Description**: The Rust test constructs a consumer via
  `make_test_consumer_with_channels()` (which always passes
  `group_id=Some("test-group")`), then mutates
  `consumer.group_id = None` post-construction. This works because
  `group_id` is `pub(crate)`, but it does not exercise the same code
  path as constructing the consumer with `group_id=None` from the
  start. In particular, the production path for a groupless consumer
  also skips registering certain request managers — those skip
  decisions are made at construction time and don't change when the
  field is mutated. The test thus only validates
  `unsubscribe()`'s `is_none()` branch, not the broader "no-group
  consumer" contract.

  Compare with `group_id_null_constructs_successfully` (line 3942-3961)
  which DOES construct a no-group consumer via a separate fixture.
  Apply the same pattern here.
- **Expected**: Add an alternative fixture
  `make_test_consumer_without_group_id()` (or extend
  `make_test_consumer_with_channels` with a parameter) that constructs
  the consumer with `config.group_id = None` from the start.
- **Actual**: Post-construction mutation of `consumer.group_id`.

---

## Issue 27: `group_remote_assignor_unused_if_group_id_undefined` test is a trivial stub

- **File**: `src/consumer/async_kafka_consumer.rs:3933-3940`
- **Severity**: Test Coverage Gap (test asserts nothing)
- **Java Reference**: `testGroupRemoteAssignorUnusedIfGroupIdUndefined` (Java line 1554-1563) asserts:
  ```java
  assertTrue(config.unused().contains(GROUP_REMOTE_ASSIGNOR_CONFIG));
  ```
- **Description**: The Rust test:
  ```rust
  async fn group_remote_assignor_unused_if_group_id_undefined() {
      let (consumer, _handles) = make_test_consumer_with_channels();
      drop(consumer);
  }
  ```
  asserts nothing. The body comment honestly admits
  "Full unused-config tracking is a config-side concern; Rust's
  `ConsumerConfig` does not currently expose `unused()`." A test that
  doesn't assert anything observable is at best a compilation guard.

  This should either be (a) a real test once `ConsumerConfig::unused()`
  is implemented, or (b) listed in the skip section with a clear
  rationale (e.g., "deferred — depends on `ConsumerConfig::unused()`
  surface which is out of Phase 11 scope") and removed from the
  inline test list.

  Also: the fixture constructs WITH `group_id=Some("test-group")`,
  contradicting the test name "if_group_id_undefined".
- **Expected**: Remove the stub or convert to a `// SKIP:` rationale.
- **Actual**: Stub test using a fixture with the wrong group_id.

---

## Verdict on Actor's flagged uncertainties (batch 3)

1. **Drainer-task pattern flakiness**: VERIFIED OK for tests that use
   `drop(consumer)` before `drainer.await` (e.g.
   `close_leaves_group_*`). Other drainer-using tests in commits 9 and 10
   either:
   - Hold a return-true guard on the drainer that ensures observed
     state before exit, OR
   - Have a deterministic single-event path (drainer returns after
     seeing one event).
   No additional flake risk identified.

2. **`close_leaves_group_for_timeout_inner` deadlock-free design**:
   VERIFIED. With `timeout=0`, `close_internal` follows the documented
   path:
   - `auto_commit_on_close` calls `commit_sync_timeout(0)` →
     `commit_sync_internal` → `commit_inner` enqueues CommitSync,
     `process_background_events_until` returns Timeout immediately
     (deadline = now), Timeout swallowed via `log::warn!`.
   - `leave_group_on_close` enqueues LeaveGroupOnClose, returns Timeout,
     swallowed by the Timeout catch (line 2819-2828).
   - Other steps continue. `first_error` stays None (both swallow).
   - The drainer (running concurrently) drains the events after close
     yields. `saw_leave=true` asserts the event was visible.

   The test is sound. With `tokio::test` default single-threaded runtime
   the drainer task is scheduled at the first await point after the
   close path enqueues events (typically `await_join` on the noop
   spawn) — which is after both events are in the channel.

3. **`auto_commit_enabled=true` default in test fixture**: VERIFIED
   intentional. `ConsumerConfig::new` defaults `enable_auto_commit=true`
   (line 208). Tests that don't override get auto-commit. This causes
   `close_*` tests to enqueue an auto-commit CommitSync envelope before
   LeaveGroupOnClose. The drainers in those tests correctly handle
   CommitSync as a no-op completion. No hidden test pollution observed.

4. **`commit_async_user_supplied_callback_with_exception_*`**:
   PARTIAL — the test machinery DOES drive the callback (drain
   `last_pending_async_commit` + `invoke_pending_callbacks`). However,
   the `..._group_authz` variant uses the wrong error variant — see
   Issue 23.

---

## Recommendation

- **Issues 21, 22, 23 are real bugs/correctness issues.** They should
  be blocked-on-fix before commit 11 / 12 close out Phase 11.
- **Issue 24 is the largest test-coverage gap.** The 9 untranslated
  tests should be either translated or moved to the skip section with
  clear per-test rationale in commit 11 or 12.
- **Issues 25, 26, 27 are test-quality nits** that can batch with
  commits 11-12 fixup cycle.
- Fixup confirmation for Issues 10-15 is clean — no objections to
  closing those.

Critic verdict: **fix Issues 21-23 before commits 11-12 land; bundle 24-27 as a polish batch.**
