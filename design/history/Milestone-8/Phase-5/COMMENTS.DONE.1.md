# Milestone-8 Phase-5 review (N=1) — RESOLVED items

Issues from `COMMENTS.1.md` that have been addressed. Original review text
is preserved verbatim; each entry ends with a **Resolution** note.

---

## Issue 1: `AssignmentChange` should be a completable event with `current_time_ms` field

- **File**: `src/consumer/internals/events/application_event.rs:52`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/AssignmentChangeEvent.java:25-46`
- **Description**: Java's `AssignmentChangeEvent extends CompletableApplicationEvent<Void>` — it carries a `CompletableFuture<Void>` and a `deadlineMs`. The app side's `assign()` blocks on `future.get(...)` until the bg task confirms the assignment change. Java also carries a `currentTimeMs: long` field for the bg task to know the wall-clock at which `assign()` was called.

  The Rust variant `AssignmentChange { all_partitions: HashSet<TopicPartition> }` is non-completable (no `handle`) and lacks `current_time_ms`. Phase-10 cannot translate the app-side blocking wait nor route the timestamp.
- **Expected**: `AssignmentChange { handle: CompletableEventHandle<()>, current_time_ms: i64, partitions: HashSet<TopicPartition> }`.
- **Actual**: Non-completable, no timestamp field.
- **Note**: The Phase-5 PLAN.md (line 276-279) also marks this variant non-completable, so the plan and the impl agree. The plan was wrong against Java; flagging so it is fixed before Phase 6/10 lands.

**Resolution**: `AssignmentChange` now carries
`handle: CompletableEventHandle<()>`, `current_time_ms: i64`, and
`partitions: HashSet<TopicPartition>`. Test
`assignment_change_carries_current_time_ms` locks in the timestamp field.
See commit `6c6583d` (`fixup! Phase 5 (4/9): reshape ApplicationEvent
variants to match Java`).

---

## Issue 2: `LeaveGroupOnClose` should be completable and carry `membership_operation`, NOT `reason`

- **File**: `src/consumer/internals/events/application_event.rs:57`
- **Severity**: Behavior Mismatch / Missing Requirement
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/LeaveGroupOnCloseEvent.java:33-48`
- **Description**: Java's `LeaveGroupOnCloseEvent extends CompletableApplicationEvent<Void>` and its only payload is `membershipOperation: CloseOptions.GroupMembershipOperation`. The Java javadoc explicitly states "The event is considered complete when the membership manager receives the heartbeat response that it has left the group" — completion is mandatory.

  The Rust variant `LeaveGroupOnClose { reason: String }` is non-completable and carries a `reason` field that does NOT exist in the Java source. The `reason` likely came from the `enforce_rebalance(reason)` API — wrong event.
- **Expected**: `LeaveGroupOnClose { handle: CompletableEventHandle<()>, membership_operation: GroupMembershipOperation }`.
- **Actual**: Non-completable, has a fictional `reason: String` instead of `membership_operation`.

**Resolution**: `LeaveGroupOnClose` is now completable and carries
`membership_operation: GroupMembershipOperation` (re-exported from
`crate::consumer::GroupMembershipOperation`). The fictional `reason` field
was removed. Test `leave_group_on_close_carries_membership_operation`
asserts the field round-trips. See commit `6c6583d`.

---

## Issue 3: `UpdatePatternSubscription` should be a completable event

- **File**: `src/consumer/internals/events/application_event.rs:74`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/UpdatePatternSubscriptionEvent.java:23-28`
- **Description**: Java's `UpdatePatternSubscriptionEvent extends CompletableApplicationEvent<Void>`. The app-side `subscribe(pattern)` path needs to wait for the bg task to confirm the pattern was re-evaluated against the latest metadata before returning.
- **Expected**: `UpdatePatternSubscription { handle: CompletableEventHandle<()> }`.
- **Actual**: Non-completable, no `handle` field.

**Resolution**: `UpdatePatternSubscription` is now completable with
`handle: CompletableEventHandle<()>`. Test
`update_pattern_subscription_is_completable` covers the handle round-trip.
See commit `6c6583d`.

---

## Issue 4: `CurrentLag` variant missing `isolation_level` field

- **File**: `src/consumer/internals/events/application_event.rs:151-154`
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/CurrentLagEvent.java:23-50`
- **Description**: Java's `CurrentLagEvent` has two fields: `partition: TopicPartition` AND `isolationLevel: IsolationLevel`. The isolation level (read_committed vs. read_uncommitted) determines which lag is reported (LSO-based vs. HW-based). The Rust variant has only `partition`, so the bg task cannot honour `isolation.level` when computing the lag.
- **Expected**: `CurrentLag { handle, partition, isolation_level: IsolationLevel }`.
- **Actual**: `CurrentLag { handle, partition }` — isolation level field missing.

**Resolution**: `CurrentLag` now carries
`isolation_level: IsolationLevel` (imported from `crate::common`). Test
`current_lag_carries_isolation_level` asserts the field. See commit
`6c6583d`.

---

## Issue 5: `CommitAsync` / `CommitSync` carry wrong handle type, missing `offsets_ready` future, missing nullable-offsets semantics

- **File**: `src/consumer/internals/events/application_event.rs:78-86`
- **Severity**: Behavior Mismatch (3 sub-issues)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/CommitEvent.java:27-79`, `AsyncCommitEvent.java`, `SyncCommitEvent.java`

  Java's `CommitEvent` (abstract base) declares:
  ```java
  extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndMetadata>>
  private final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets;
  protected final CompletableFuture<Void> offsetsReady = new CompletableFuture<>();
  ```

  Three deviations in the Rust variant:

  1. **Handle type**: Java's main future is `CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>` — the bg task returns the committed offsets so the app side can re-confirm what was committed. The Rust `CompletableEventHandle<()>` discards that data.

  2. **Missing `offsets_ready` future**: Java's `CommitEvent` carries a second future, `offsetsReady`, completed by the bg task BEFORE the actual commit, so the app thread knows the offsets-to-commit have been resolved (when `offsets` is `None`, meaning "commit all consumed"). Used by `markOffsetsReady()`. There is no Rust analog.

  3. **`offsets` must be optional**: Java uses `Optional<Map>` — `None` means "commit all consumed offsets" (the consumer's internal tracked offsets). The Rust enum uses `HashMap<...>` directly. A caller cannot express "commit all consumed" — they must materialize the entire current-position map and pass it, defeating the lazy resolution Java does on the bg side.

- **Expected**: Roughly
  ```rust
  CommitAsync {
      handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
      offsets_ready: CompletableEventHandle<()>,
      offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
  },
  ```
  (and the same shape for `CommitSync`)
- **Actual**: Single handle of `()`, no `offsets_ready`, non-optional offsets.

**Resolution**: `CommitAsync` and `CommitSync` both now carry:
- `handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>` (returns committed offsets),
- `offsets_ready: CompletableEventHandle<()>` (Java's secondary handshake future), and
- `offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>` (`None` = commit all consumed).

Test `commit_async_carries_offsets_ready_and_optional_offsets` exercises
the `None` form and the `offsets_ready` handshake. See commit `6c6583d`.

---

## Issue 6: `AsyncPoll` variant translation is incomplete — Java is not a `CompletableApplicationEvent`

- **File**: `src/consumer/internals/events/application_event.rs:88`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/AsyncPollEvent.java:29-115`
- **Description**: Java's `AsyncPollEvent extends ApplicationEvent` (NOT `CompletableApplicationEvent`) and implements `MetadataErrorNotifiableEvent`. It is intentionally NOT a `CompletableFuture`-based event — it is designed as a **non-blocking** two-stage state machine carrying:
  - `deadlineMs: long`
  - `pollTimeMs: long`
  - `error: volatile KafkaException` (settable via `completeExceptionally`)
  - `isComplete: volatile boolean`
  - `isValidatePositionsComplete: volatile boolean` (settable via `markValidatePositionsComplete`)

  Methods: `isExpired`, `isComplete`, `completeSuccessfully`, `completeExceptionally`, `markValidatePositionsComplete`, `isValidatePositionsComplete`, plus `onMetadataError`.

  This is exactly the design Java references in its class doc — `AsyncKafkaConsumer.poll()` submits the event but does NOT block on `future.get()`. Instead, the app side polls `isComplete` / `error` between iterations.

  The Rust `AsyncPoll { handle: CompletableEventHandle<()> }` represents it as a single-future completable event, losing the two-stage `isValidatePositionsComplete` marker and the poll-time / poll-deadline carrier fields. Phase-10's `poll()` translation will not be able to express the non-blocking semantics.
- **Expected**: A custom variant payload that preserves the Java state machine, e.g.
  ```rust
  AsyncPoll {
      deadline_ms: i64,
      poll_time_ms: i64,
      state: Arc<AsyncPollState>,  // interior-mutable, ~= Java's volatile fields
  }
  ```
  where `AsyncPollState` carries `error: Mutex<Option<KafkaError>>`, `is_complete: AtomicBool`, `is_validate_positions_complete: AtomicBool`.
- **Actual**: Single `CompletableEventHandle<()>` — drops the two-stage state machine and the poll-time / deadline-on-event fields.

**Resolution**: `AsyncPoll` is now a non-completable variant carrying
`deadline_ms`, `poll_time_ms`, and `state: Arc<AsyncPollState>`.
`AsyncPollState` exposes `is_complete`, `is_validate_positions_complete`,
`mark_validate_positions_complete`, `complete_successfully`,
`complete_exceptionally`, and `error()` — mirroring Java's volatile
fields and methods. `ApplicationEvent::async_poll_is_expired(now_ms)`
mirrors Java's `AsyncPollEvent.isExpired(time)`. Tests
`async_poll_state_completes_successfully`,
`async_poll_state_completes_exceptionally`, and
`async_poll_is_expired_compares_deadline` cover the state machine. The
`application_event_handler` tests that previously used `AsyncPoll` as a
test handle-carrier were retargeted to `CreateFetchRequests` (which is a
completable `<()>` variant) since `AsyncPoll` no longer carries a
`CompletableEventHandle`. See commit `6c6583d`.

---

## Issue 7: `MetadataErrorNotifiableEvent` interface not translated

- **File**: `src/consumer/internals/events/*` (no implementation present)
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/MetadataErrorNotifiableEvent.java:27-56`
- **Description**: Java's `MetadataErrorNotifiableEvent` interface is implemented by `CheckAndUpdatePositionsEvent`, `ListOffsetsEvent`, `AsyncPollEvent`, and `AbstractTopicMetadataEvent` (so both `TopicMetadataEvent` and `AllTopicsMetadataEvent`). The bg task calls `onMetadataError(exception)` on these events whenever `NetworkClientDelegate.getAndClearMetadataError()` returns a value, both before processing the event and during reaper-on-close.

  The Phase-5 PLAN.md line 88-89 specifically called out this interface and asked the Actor to "verify whether it's a marker trait or carries behavior; translate accordingly." It carries behavior (one method). No Rust analog was produced.

  Note: this is a real surface for the Phase-10 processor to wire up, and four of the in-scope event variants need to satisfy it. Without the trait, Phase 10 cannot uniformly dispatch metadata errors to the right variants.
- **Expected**: A `pub(crate) trait MetadataErrorNotifiable { fn on_metadata_error(&self, error: KafkaError); }` (or similar) plus annotations on each of the four variant arms identifying them as metadata-error-notifiable. Alternative: a helper free function that does the dispatch via match-arm.
- **Actual**: Trait absent; no per-variant marking.

**Resolution**: Implemented as
`ApplicationEvent::on_metadata_error(&self, KafkaError) -> bool`
(the free-function-via-match-arm alternative). Returns `true` for the
five notifiable variants (`AsyncPoll`, `CheckAndUpdatePositions`,
`ListOffsets`, `TopicMetadata`, `AllTopicsMetadata`) so the caller can
match Java's "do not subsequently process the event" contract; returns
`false` for everything else. Test
`on_metadata_error_dispatches_only_for_notifiable_variants` exercises
all five notifiable arms plus the `false`-returning default. See
commit `6c6583d`.

---

## Issue 8: `ListOffsets` should return `OffsetAndTimestampInternal`, not `OffsetAndTimestamp`

- **File**: `src/consumer/internals/events/application_event.rs:95-99`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/ListOffsetsEvent.java:35` and `OffsetAndTimestampInternal.java:24`
- **Description**: Java's `ListOffsetsEvent` returns `Map<TopicPartition, OffsetAndTimestampInternal>`. The `Internal` type allows negative timestamps and offsets (see Javadoc on `OffsetAndTimestampInternal`); the public `OffsetAndTimestamp` rejects them. The bg task internally uses the negative-allowing form and the app-side `offsets_for_times` translates each result via `buildOffsetAndTimestamp()` before returning to the user.

  The Rust variant currently returns the public `OffsetAndTimestamp` directly, which means the bg task cannot represent a "no offset found" sentinel (Java uses `null` in the map for that case — see `ListOffsetsEvent.emptyResults()`).
- **Expected**: Either translate `OffsetAndTimestampInternal` (Java permits negative values for sentinel purposes) and use it here, or use `Option<OffsetAndTimestamp>` in the result map.
- **Actual**: Bare `OffsetAndTimestamp`, no sentinel support.

**Resolution**: `ListOffsets` now returns
`HashMap<TopicPartition, Option<OffsetAndTimestamp>>` — `None`
preserves Java's `null`-map-value sentinel from
`ListOffsetsEvent.emptyResults()`. Rustdoc explicitly documents the
deviation from Java's `OffsetAndTimestampInternal` and the rationale
(avoid introducing a second internal type). See commit `6c6583d`.

---

## Issue 9: `CompletableEventReaper::reap_on_close` count semantics differ from Java under concurrent completion

- **File**: `src/consumer/internals/events/completable_event_reaper.rs:181-210` (`complete_events_exceptionally_on_close`)
- **Severity**: Behavior Mismatch (minor — only matters under a race)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/CompletableEventReaper.java:186-209`
- **Description**: Java increments `count` BEFORE calling `completeExceptionally`:
  ```java
  if (event.future().isDone()) continue;
  count++;
  ...
  if (event.future().completeExceptionally(error)) { log.debug... } else { log.trace... }
  ```
  So the return value is "events that were not done when we observed them", regardless of whether the concurrent completion won the race.

  The Rust impl increments only when `fail_with_timeout` returns `true`:
  ```rust
  if handle.is_done() { continue; }
  ...
  if handle.fail_with_timeout(error) { ... count += 1; }
  ```
  Under a race where another task completes the handle between the `is_done` check and `fail_with_timeout`, Java counts it; Rust does not. The numeric return value diverges.
- **Expected**: Move the increment before the `fail_with_timeout` call to match Java exactly.
- **Actual**: Increment is conditional on the call's success.
- **Note**: Same applies to `reap(current_time_ms)` (`completable_event_reaper.rs:104-118`) — the `expired_count += 1;` is also gated on the boolean. Java does the increment unconditionally after the past-due check (line 115 of Java).

**Resolution**: Both `reap(current_time_ms)` and
`complete_events_exceptionally_on_close` now increment the counter
BEFORE the `fail_with_timeout` call (matching
`CompletableEventReaper.java:115` and `:194`). See commit `62231dc`.

---

## Issue 10: `reap_on_close` cannot clear the supplied collection

- **File**: `src/consumer/internals/events/completable_event_reaper.rs:140-154`
- **Severity**: Behavior Mismatch (caller-visible)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/CompletableEventReaper.java:143-153` (line 150: `events.clear();`)
- **Description**: Java's `reap(Collection<?> events)` **clears the supplied collection** after expiring its events. The Java test `testIncompleteQueue` (line 167) explicitly asserts `assertEquals(0, queue.size())` after the call. This is part of the public contract: the caller doesn't need to re-drain.

  The Rust signature takes `impl IntoIterator<Item = ...>` which makes this clear physically impossible — the function consumes the iterator but cannot reach the underlying container. The caller must remember to drain the channel before passing the snapshot in.

  The doc comment ("The caller is expected to supply a freshly-drained collection") acknowledges this, but the impedance is non-trivial: in Phase 10 it is easy to forget the drain side-effect, leaving a leaked event in the channel.
- **Expected**: Either take `&mut Vec<Arc<...>>` and call `.clear()` after iteration, or document the requirement on the caller more prominently with a TODO/example.
- **Actual**: Signature elides the clear side-effect; caller has to remember.

**Resolution**: Signature changed to
`reap_on_close(&mut self, unprocessed_events: &mut Vec<Arc<dyn CompletableEventErasedHandle>>) -> u64`
and the function calls `unprocessed_events.clear()` after iteration.
All three test sites (`reap_on_close_expires_tracked_and_extra`,
`reap_on_close_handles_queue_only_events`,
`reap_on_close_handles_tracked_only_events`) now pass `&mut Vec<...>`
and assert `is_empty()` after the call. See commit `62231dc`.

---

## Issue 11: `CompletableEventReaper::contains` is fragile under `erased()` re-creation

- **File**: `src/consumer/internals/events/completable_event_reaper.rs:165-167`
- **Severity**: Design Flaw
- **Java Reference**: `CompletableEventReaper.java:159-161` (`tracked.contains(event)` — relies on Java's object identity / per-event single reference)
- **Description**: Java's `contains` works because the same `CompletableEvent` reference is shared across all call sites — there is only one event object per logical event. The Rust impl uses `Arc::ptr_eq` on `Arc<dyn CompletableEventErasedHandle>`. But `CompletableEventHandle::erased()` creates a **new** `Arc<ErasedHandle<T>>` on every call — two calls to `erased()` on the same handle yield two distinct `Arc`s pointing to two distinct `ErasedHandle` boxes (both sharing the underlying `Arc<HandleInner<T>>`).

  Concretely: if Phase-10 holds an `ApplicationEvent::CommitAsync { handle, ... }` and asks the reaper "do you contain this event?", it has to first call `handle.erased()` — but that produces a *different* Arc than the one originally registered, so `contains` returns `false`. The test (`contains_uses_ptr_eq`) passes only because it uses `Arc::clone(&erased)`.

  Java does not have this footgun.
- **Expected**: Either (a) cache the `Arc<dyn CompletableEventErasedHandle>` inside `HandleInner` so all `erased()` calls return the same `Arc`, or (b) compare on `Arc::as_ptr(&inner)` of the underlying `HandleInner<T>` (requires a different trait method like `inner_ptr() -> *const ()` for type-erased equality).
- **Actual**: Multiple calls to `handle.erased()` produce distinct `Arc`s; `contains` returns spurious `false` for one of them.

**Resolution**: Adopted option (b) — added
`CompletableEventErasedHandle::inner_id(&self) -> *const ()` returning
the stable pointer to the underlying `HandleInner<T>` via `Arc::as_ptr`.
`CompletableEventReaper::contains` now compares on `inner_id()` equality
instead of `Arc::ptr_eq`. Option (a) was attempted with
`OnceLock<Arc<dyn ...>>` cached on `HandleInner`, but rejected because
the cached `Arc` holds the inner alive forever (cycle). New test
`contains_works_across_erased_recreation` proves the fix. See commit
`62231dc`.

---

## Issue 12: `BackgroundEventHandler` is missing `drain_events()`

- **File**: `src/consumer/internals/events/background_event_handler.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/BackgroundEventHandler.java:65-70`
- **Description**: Java's `BackgroundEventHandler` exposes a `drainEvents()` method that the app side calls to pull all pending events at once. Java's `BackgroundEventHandlerTest` indirectly references this (it asserts that after `drainEvents()` the queue size sensor reports 0). The Phase-5 PLAN.md describes only `add(...)` for this class but does not explicitly defer `drainEvents`.

  In Rust, the receiver lives in the app side and the consumer code (Phase 10) will drain via `try_recv` in a `while let` loop directly on the `mpsc::UnboundedReceiver`. So the absence of `drain_events()` on the handler is a deliberate design choice — but it's worth confirming in the plan and documenting on the handler so Phase 10 doesn't add it back.
- **Expected**: Either add a `drain_events(&mut Vec<BackgroundEventEnvelope>)`-style method that pairs sender with the drain side, OR add an explicit comment on the handler stating "drain happens via the raw `mpsc::UnboundedReceiver`; this handler is sender-only by design."
- **Actual**: Method absent and undocumented.

**Resolution**: Documented the sender-only design choice on both the
module-level rustdoc and the `BackgroundEventHandler` struct doc. Phase
10 should drain via the raw `mpsc::UnboundedReceiver` directly. No code
change. See commit `62231dc`.

---

## Issue 13: Display impl on `ApplicationEvent` prints type name twice

- **File**: `src/consumer/internals/events/application_event.rs:209-216`
- **Severity**: Style (low priority — but it's a visible behavioral artifact)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/events/ApplicationEvent.java:72-79`
- **Description**: Java prints `ClassName{type=TYPE, enqueuedMs=N, ...}` — the class name and the `type` enum are distinct concepts (the class is e.g. `AsyncPollEvent`, the type is e.g. `ASYNC_POLL`). The Rust impl outputs `AsyncPoll{type=AsyncPoll}` — same string twice — because the Rust variant name plays both roles. The test (`display_emits_variant_name`) asserts this verbatim, locking in the duplication.

  Minor, but the output is meaningfully less useful than Java's because it always duplicates. Either drop the `type=` field or change one of them to a different representation.
- **Expected**: `AsyncPoll{enqueued_ms=…}` (the envelope), or `AsyncPoll{}` for the bare event. Java only adds `enqueued_ms` because it lives on the abstract base; the Rust envelope has it.
- **Actual**: `AsyncPoll{type=AsyncPoll}` — the type-name string is printed twice.

**Resolution**: `ApplicationEvent`'s `Display` now prints `<variant>{}`
(no redundant `type=` field). The `ApplicationEventEnvelope` gains a
`Display` impl printing `<variant>{enqueued_ms=N}` so the timestamp is
still observable. Test `display_emits_variant_name` updated to assert
the new shape; new test `envelope_display_includes_enqueued_ms` covers
the envelope format. See commit `6c6583d`.

---

## Issue 14: `completed_event_is_removed_but_not_counted_as_expired` test does not assert value preservation

- **File**: `src/consumer/internals/events/completable_event_reaper.rs:243-254`
- **Severity**: Test Coverage Gap
- **Java Reference**: `CompletableEventReaperTest.java:72-98` (`testCompleted` — line 96 asserts `assertNull(ConsumerUtils.getResult(event.future()))`)
- **Description**: The Java `testCompleted` test verifies that an event completed before reap retains its successful value (the `null` payload, since `Void`). The Rust mirror drops `_rx` before the reap and never reads the receiver — so a regression where `reap` clobbered the existing success value with a timeout error would not be caught.
- **Expected**: Keep `rx` alive past the reap and `assert!(matches!(rx.try_recv().unwrap(), Ok(())))` to confirm the Ok value survived.
- **Actual**: Receiver dropped; value-preservation not checked.

**Resolution**: Test now keeps the receiver alive past the reap and
asserts `matches!(rx.try_recv().expect("sender used"), Ok(()))`. The
`expect` confirms the sender was consumed (so the value reached the
receiver) and the `matches!` confirms it was `Ok(())` rather than a
timeout error. (`assert_eq!` was rejected because `KafkaError` doesn't
derive `PartialEq`.) See commit `62231dc`.

---

## Suggested CLAUDE.md / rules updates

(Forwarded to the Manager; the Actor does not action these.)

Two patterns from this review would benefit from a one-line addition somewhere:

1. **Rule for translating `CompletableApplicationEvent<T>` subclasses**: every Java event extending `CompletableApplicationEvent<T>` (or `CompletableBackgroundEvent<T>`) MUST land in Rust as a variant that carries `handle: CompletableEventHandle<T>`. Conversely, every Java event extending bare `ApplicationEvent` / `BackgroundEvent` is non-completable. Five of the issues above (1, 2, 3, 5, 6) stem from missing this mapping. A checklist row in `consumer-threading.md` or a translation rule in `CLAUDE.md` §13 (consumer rules) would prevent it from recurring in Phase 6 / Phase 10 events.

2. **Marker-interface translation rule**: when a Java event implements a marker-like interface with one method (`MetadataErrorNotifiableEvent`), the trait must be translated even when the events themselves get folded into an enum, because the processor still needs to dispatch on it. Adding this as a bullet in CLAUDE.md §2 (Naming Conventions) under "Java interface → Rust trait" would have caught Issue 7.
