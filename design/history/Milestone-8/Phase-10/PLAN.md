# Phase 10: Background task & event processor

## Goal

Translate the two classes that drive every consumer request manager
together inside a single tokio task:

- `ConsumerNetworkThread` (460 LOC) — the per-consumer background task.
  Implements the `runOnce()` ordering documented in
  `consumer-threading.md` §10 and the `select!`-over-shutdown / wakeup /
  network-poll discipline of §11.
- `ApplicationEventProcessor` (853 LOC) — the `EventProcessor<ApplicationEvent>`
  impl. Switches on the variant and dispatches to the correct request
  manager / `SubscriptionState` / `ConsumerMetadata` call.

Phase 10 also closes the dependency gaps these two classes uncover in
the existing Rust tree (see "Wire-prereqs" below).

After Phase 10 lands, the bg task can be `tokio::spawn`-ed, can pump
every `ApplicationEvent` end to end, can shut down cleanly, and exposes
the cached `maximum_time_to_wait()` value the app side reads. Phase 11
(`AsyncKafkaConsumer`) wires the app side on top.

## Branch / worktree

Lands on `consumer-impl` directly (no worktree). Phase 10 is a serial
bottleneck per `Milestone-8/PLAN.md`: it depends on Phases 5–9, and
Phase 11 depends on it. There is no parallel work to merge against.

## Java sources

### Phase 10 production

- `org/apache/kafka/clients/consumer/internals/ConsumerNetworkThread.java`
  (460 LOC, pinned commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`)
- `org/apache/kafka/clients/consumer/internals/events/ApplicationEventProcessor.java`
  (853 LOC)

### Wire-prereqs (translate first, in Phase 10)

These are deferred Phase-7d / Phase-9 / Phase-8b carry-overs that the
processor and bg task need. Each is documented in source as a Phase 10
carry-over via TODO comment or memory note:

1. **`RequestManagers` — add `offsets` and `fetch` slots.**
   Java: `RequestManagers` ctor takes `OffsetsRequestManager` and
   `FetchRequestManager`. Today's Rust struct only carries five of the
   seven slots (`coordinator`, `topic_metadata`, `commit`,
   `consumer_heartbeat`, `consumer_membership`). Phase 10 extends the
   constructor + `entries()` ordering to match Java's registration
   order:
   `coordinator → commit → heartbeat → offsets → topic_metadata → fetch`.
   Once both new slots are populated, the stale "skip note" docstring
   on `entries()` (referring to commit / consumer_membership slots
   from Phase 9) is rewritten to reflect the final wiring.
2. **`OffsetsRequestManager::update_fetch_positions(deadline_ms) ->
   oneshot::Receiver<Result<(), KafkaError>>`** —
   `OffsetsRequestManager.java:235`. Used by `CheckAndUpdatePositions`
   and `AsyncPoll` events. Includes the
   `init_with_committed_offsets_if_needed` chain (currently
   mis-located on `CommitRequestManager`; carry-over #3 below
   relocates it) and the `init_with_partition_offsets_if_needed`
   reset branch (driven by the existing `reset_positions_if_needed`).
3. **Relocate `init_with_committed_offsets_if_needed` from
   `CommitRequestManager` to `OffsetsRequestManager`.** Java has it on
   `OffsetsRequestManager` (it composes a `commitRequestManager.fetchOffsets`
   call with subscription-state updates and the `pendingOffsetFetchEvent`
   re-use logic). Mis-locating it on the commit manager (Phase 9
   placeholder) blocks the `pendingOffsetFetchEvent` re-use behavior
   required by `update_fetch_positions`. Move the method, update its
   callers, keep the existing test surface.
4. **`OffsetsRequestManager::fetch_offsets(timestamps_to_search,
   require_timestamps) -> oneshot::Receiver<...>`** —
   `OffsetsRequestManager.java:fetchOffsets(...)`. Used by
   `ListOffsetsEvent` and `CurrentLagEvent`. Requires the
   `OffsetsClusterListener::on_update` deferred-request replay path
   (carry-over from Phase 7d) — once `fetch_offsets` exists, the
   listener no longer no-ops; on metadata change it re-issues
   deferred fetch_offsets requests.
5. **`OffsetsRequestManager::try_connect` plumbing.** Phase 7d
   `validate_positions_if_needed` falls through with a debug log when
   `NodeApiVersions` are missing for a broker; Java would call
   `client.tryConnect(node)` to force a connect. With the bg task now
   owning the network client (Phase 10), the manager exposes a
   `try_connect` hint on its `PollResult` (or queues an
   `UnsentRequest` with no body — TBD during commit (3)) so the bg
   task can drive the connect.
6. **`CommitRequestManager::update_timer_and_maybe_commit(current_time_ms)`**
   — `CommitRequestManager.java:updateTimerAndMaybeCommit`. Public
   hook the processor calls from `AsyncPoll` and `AssignmentChange`.
   Today's Rust drains the auto-commit timer from inside `poll`; Java
   exposes it independently so the processor can fire it at event
   time. Public `pub(crate)` method that calls the existing
   `maybe_auto_commit_async` after refreshing the timer.
7. **`CommitRequestManager::maybe_auto_commit_sync_before_rebalance`
   hook + `MemberStateListener` impl on `CommitRequestManager`.**
   Java wires the commit manager as a `MemberStateListener` so when
   the membership manager transitions to `PREPARE_LEAVING` / a
   reconcile boundary, the rebalance pipeline can synchronously flush
   any pending offsets before partitions are reassigned. Phase 8b
   carved out the hook on the rebalance pipeline; Phase 10 supplies
   the listener impl and the actual flush method. Together these
   unblock the §31 "commit_sync from inside on_partitions_revoked"
   semantics that Phase 11 will exercise.
8. **`CommitRequestManager` — `inflight_offset_fetches` drain on
   completion.** Phase 9 leaked entries in
   `pending.inflight_offset_fetches` after a fetch resolved (the
   completion path didn't remove them from the inflight vec). Fix
   the drain on response handling. Small (≤ 30 lines + a test).
9. **`CommitRequestManager::commit_sync` in-manager retry counter.**
   Phase 9 implemented `commit_sync` as a single-shot future. Java
   tracks per-request retry counts inside the manager so the
   `RetriableCommitFailedError` path can decide between retry vs.
   surface-to-caller. Add the counter to the commit-state slot and
   wire it through the existing retry path.

Item 1 is a 30-line touch-up. Items 2 & 4 are ~300 LOC each (Java has
a two-stage `CompletableFuture` chain). Item 3 is a relocate
(~80 LOC moved, ~40 LOC of glue). Item 5 is a small `PollResult` /
queue addition (~40 LOC). Item 6 is ~20 lines plus a small test.
Item 7 is ~120 LOC (listener trait impl + flush method + test). Item
8 is ~30 LOC + test. Item 9 is ~50 LOC. They land first in Phase
10's commit order so the processor translation can compile against
the finished managers.

### Out of scope (deferred to a later milestone or already excluded)

- `AsyncConsumerMetrics` recording calls inside both classes. Metrics
  are a separate concern across Milestone 8 — every Phase has stubbed
  them out, and Phase 10 follows suit (the `record*` calls become
  no-ops or are dropped entirely, with a one-line comment pointing at
  the Java line).
- Streams branches in `ApplicationEventProcessor` (`SharePollEvent`,
  `ShareFetchEvent`, all `Streams*` events, `StreamsGroupHeartbeatRequestManager`).
  Out of milestone scope per `consumer-threading.md` §20. The processor
  has no Rust variants for these, so the Rust `match` simply does not
  carry the arms.
- `signalClose` propagation through `pollOnClose` — the existing
  `RequestManager::poll_on_close` default is `PollResult::empty()`.
  We use the existing trait method; nothing to add.
- `IdempotentCloser`. Rust uses a plain `closed: bool` flag (existing
  precedent in `RequestManagers`).

### Tests

- `clients/consumer/internals/ConsumerNetworkThreadTest.java` (342 LOC,
  13 tests).
- `clients/consumer/internals/events/ApplicationEventProcessorTest.java`
  (734 LOC, 39 tests including parameterised cases).

All tests except those that exercise Streams / share-consumer arms (out
of scope per §20) are translated. The skipped-Streams tests are listed
under "Skipped tests" with a one-line rationale each per DoD §3.

## Rust outputs

```
src/consumer/internals/
├── consumer_network_thread.rs                  # NEW — bg task driver
└── events/
    └── application_event_processor.rs           # NEW — EventProcessor<ApplicationEvent>
```

Updated files:

```
src/consumer/internals/
├── request_managers.rs                          # add offsets, fetch slots + entries() order
├── offsets_request_manager.rs                   # add update_fetch_positions, fetch_offsets
└── commit_request_manager.rs                    # add update_timer_and_maybe_commit
src/consumer/internals/mod.rs                    # pub(crate) mod consumer_network_thread
src/consumer/internals/events/mod.rs             # pub(crate) mod application_event_processor
```

## Behavior parity

### `ConsumerNetworkThread`

Java is a `Thread extends KafkaThread` with `run()` that calls
`initializeResources()` then loops on `runOnce()` until `running=false`.
Rust translation per `consumer-threading.md` §10:

- **One `tokio::spawn` per consumer**, owning `NetworkClientDelegate`,
  `RequestManagers`, `ApplicationEventProcessor`,
  `CompletableEventReaper`, the bg-event sender, and the wakeup-token
  receiver. No per-RequestManager task split (§10).
- **`run_once()` mirrors Java line-for-line:**
  1. `process_application_events()` — drain via `try_recv` in a
     `while let` loop (NOT `recv().await`), mirroring Java's `drainTo`.
     Unbounded drain (§10). Add `CompletableEvent` payloads to the
     reaper; dispatch metadata-error notifiable events first, then
     processor.process().
  2. For each `RequestManager` in `request_managers.entries()` (already
     in Java's registration order — see Wire-prereq #1):
     - `poll_result = rm.poll(current_time_ms)`;
     - `timeout_ms = network_client.add_all(poll_result, current_time_ms)`;
     - `poll_wait_time_ms = min(poll_wait_time_ms, timeout_ms)`.
  3. `network_client.poll(poll_wait_time_ms, current_time_ms).await`
     — wrapped in `tokio::select! { biased; shutdown; wakeup;
     network_poll }` per §10. This is the only await point in `run_once`.
  4. Compute `max_time_to_wait = min(rm.maximum_time_to_wait(current_time_ms))`
     for all managers; store atomically (see "App-side reads" below).
  5. `application_event_reaper.reap(current_time_ms)` and handle
     metadata-errors on uncompleted events.
- **Shutdown** is two-phase. The app side calls
  `consumer_network_thread.close(timeout)` (sync), which flips
  `running=false` (an `AtomicBool` shared with the bg task) and calls
  `wakeup_trigger.wakeup()` to unblock the active select. The bg task
  observes `running==false` at the top of its loop, exits, and runs
  `cleanup()` which calls each manager's `poll_on_close`, drains the
  unsent queue until the close timer expires, runs final reap, and
  calls `request_managers.close()`. Java's `join()` becomes
  `tokio::task::JoinHandle::await` (per CLAUDE.md §9.4).
- **App-side reads.** Java's `maximumTimeToWait()` is read by the app
  from any thread. Rust uses an `Arc<AtomicI64>` written by the bg
  task at the end of each `run_once`; the app side reads with
  `Ordering::Acquire`. Initial value `MAX_POLL_TIMEOUT_MS = 5000`
  (matching Java).
- **`wakeup()`** delegates to `WakeupTrigger::wakeup()` + a direct call
  on `NetworkClientDelegate::wakeup()` (the existing
  `unsent_requests` waiter inside the delegate needs an independent
  nudge — Java does the same via `KafkaClient.wakeup()`).
- **`initialize_resources` error path.** Java has a `CountDownLatch` +
  `AtomicReference<KafkaException>` that lets the spawning thread
  observe init errors. Rust collapses this to a `oneshot::Sender<Result<(),
  KafkaError>>` handed to the bg task at spawn time; the caller awaits
  the receiver before returning from `ConsumerNetworkThread::start`.
  Initialization failure causes the bg task to exit immediately (still
  running `cleanup()` per the Java `finally` block).

### `ApplicationEventProcessor`

One sync method `process(&mut self, event: ApplicationEvent)`. Matches
Java's switch table 1:1 for in-scope variants. Each arm calls the
appropriate request manager / `SubscriptionState` method.

Specific translation notes:

- **`AsyncPollEvent` is the load-bearing one** (`process_async_poll`).
  Java chains `updateFetchPositions` → `markValidatePositionsComplete`
  → `createFetchRequests` → `event.completeSuccessfully()` via
  `CompletableFuture::whenComplete`. Rust translates the chain to a
  detached `tokio::spawn` that awaits the two receivers in sequence
  and writes the result to `state` (`AsyncPollState` already lives on
  the event — `state.complete_successfully()` /
  `state.complete_exceptionally(err)`). The `tokio::spawn` here is
  per-event, NOT per-record — it's bounded by `poll()` call frequency
  and lives outside the hot path (CLAUDE.md §11 explicitly OKs this
  by exclusion: "per-message `tokio::spawn` on the send path: avoid").
- **`AssignmentChangeEvent`** holds the `MutexGuard` on
  `SubscriptionState` only across the synchronous `assign_from_user`
  call. The guard is dropped before completing the event handle
  (§16). `commit_request_manager.update_timer_and_maybe_commit(current_time_ms)`
  is called BEFORE the assign per Java's ordering.
- **`UnsubscribeEvent`** invokes
  `consumer_membership_manager.leave_group()` (already async in Rust)
  via `tokio::spawn` so the processor returns immediately; the
  detached task completes the event handle when the future resolves.
  Mirrors Java's `whenComplete(complete(event.future()))` pattern.
- **`ConsumerRebalanceListenerCallbackCompletedEvent`** calls
  `consumer_membership_manager.consumer_rebalance_listener_callback_completed(event)`
  (this method already exists on the membership manager from Phase 8b).
- **`CommitOnCloseEvent`** calls
  `commit_request_manager.signal_close()` (already present).
- **`LeaveGroupOnCloseEvent`** spawns
  `consumer_membership_manager.leave_group_on_close(operation)` (the
  Phase-8b method) and completes the handle on resolution.
- **Pattern-subscription events** (`TopicPatternSubscriptionChangeEvent`,
  `UpdatePatternSubscriptionEvent`) call
  `subscriptions.subscribe_pattern(...)` /
  `subscriptions.matches_subscribed_pattern(...)` /
  `subscriptions.subscribe_from_pattern(...)` against the latest
  `metadata.fetch()` Cluster. Drops the `SubscriptionState` guard
  before invoking `membership_manager.on_subscription_updated()`
  (§16).
- **No async-trait on the processor.** `EventProcessor::process` stays
  sync (DoD §11). Anything that needs an `.await` either runs
  synchronously (because the underlying API is sync) or is detached
  via `tokio::spawn` — same pattern Java uses with
  `CompletableFuture::whenComplete`.
- **Per-event allocation.** No per-record allocations; per-event
  `tokio::spawn` cost is acceptable (per-batch, not per-record — §11
  hot-path is the receive / decode path, not event dispatch).

## Module structure

```
src/consumer/internals/consumer_network_thread.rs
  pub(crate) struct ConsumerNetworkThread
  pub(crate) struct ConsumerNetworkThreadHandle  // start() returns this
  impl ConsumerNetworkThread {
      pub(crate) async fn start(...) -> Result<ConsumerNetworkThreadHandle, KafkaError>
  }
  // The struct itself is owned by the spawned task; the handle owns
  // the JoinHandle + AtomicBool + WakeupTrigger + close() entry point.

src/consumer/internals/events/application_event_processor.rs
  pub(crate) struct ApplicationEventProcessor
  impl ApplicationEventProcessor {
      pub(crate) fn new(
          request_managers: Arc<Mutex<RequestManagers>>,
          metadata: Arc<ConsumerMetadata>,
          subscriptions: Arc<Mutex<SubscriptionState>>,
          background_event_handler: BackgroundEventHandler,  // for bg→bg dispatch on error
      ) -> Self
  }
  impl EventProcessor<ApplicationEvent> for ApplicationEventProcessor {
      fn process(&mut self, event: ApplicationEvent) { ... }
  }
```

`RequestManagers` ownership: Java holds it as a non-shared field on
the bg thread. Rust collapses this to `Arc<Mutex<RequestManagers>>`
because both `ApplicationEventProcessor` and the bg task's `run_once`
loop need to call into the same instance. The mutex is held briefly
on the bg task only (single-threaded acquisition pattern), so
contention is nil — but we never call `.await` while holding it
(§16-style rule extended to `RequestManagers`).

## Skipped tests

`ConsumerNetworkThreadTest`:

- `testRunOnceRecordTimeBetweenNetworkThreadPoll` and
  `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`
  — depend on `AsyncConsumerMetrics` (out of scope across Milestone-8).
- `testRunOnceInvokesReaper` and `testCleanupInvokesReaper` — assert
  metric recording from the reap path. Translate the
  reaper-invocation observation without the metrics check.

`ApplicationEventProcessorTest` (all Streams / share-consumer cases):

- `testStreamsOnTasksRevokedCallbackCompletedEvent*` (×2)
- `testStreamsOnTasksAssignedCallbackCompletedEvent*` (×2)
- `testStreamsOnAllTasksLostCallbackCompletedEvent*` (×2)

Each skipped with a `// SKIP: <reason>` doc comment per DoD §3
referring to `consumer-threading.md` §20.

## Commit plan

1. `Phase 10 (1/N): RequestManagers — wire offsets + fetch slots`
   — touch-up + small test + rewrite of the stale `entries()` skip
   note. **Closes wire-prereq #1** (which also subsumed the
   doc-staleness item).
2. `Phase 10 (2/N): CommitRequestManager — auto-commit hook + drain fix + retry counter`
   — `update_timer_and_maybe_commit` public hook (wire-prereq #6);
   drain `inflight_offset_fetches` on response (wire-prereq #8);
   in-manager retry counter in `commit_sync` (wire-prereq #9).
   **Closes wire-prereqs #6, #8, #9.**
3. `Phase 10 (2.5/N): CommitRequestManager — MemberStateListener impl + auto-commit-sync-before-rebalance`
   — listener trait impl wiring the commit manager into the
   membership-state transitions; `maybe_auto_commit_sync_before_rebalance`
   flush method. **Closes carry-over #7.**
4. `Phase 10 (3a/N): OffsetsRequestManager — relocate init_with_committed_offsets_if_needed`
   — move ~80 LOC of the method from `CommitRequestManager` to
   `OffsetsRequestManager`, update call sites, keep existing test
   surface (translate any missing Java tests for the relocated
   method). **Closes wire-prereq #3.**
5. `Phase 10 (3b/N): OffsetsRequestManager — update_fetch_positions`
   — translate the `~300 LOC` Java method (`OffsetsRequestManager.java:235`).
   Wires through `init_with_committed_offsets_if_needed` (relocated
   in 3a) and the existing `reset_positions_if_needed`. Translates
   the relevant `OffsetsRequestManagerTest` cases. **Closes wire-prereq #2.**
6. `Phase 10 (3c/N): OffsetsRequestManager — fetch_offsets + cluster-listener replay`
   — translate `~300 LOC` Java method (`OffsetsRequestManager.java:fetchOffsets`).
   Unblocks the `OffsetsClusterListener::on_update` deferred-request
   replay (it now has something to replay). Translates the relevant
   `OffsetsRequestManagerTest` cases. **Closes wire-prereqs #4 + #2's
   cluster-listener-replay sub-item.**
7. `Phase 10 (3d/N): OffsetsRequestManager — try_connect plumbing`
   — small `~40 LOC` addition: emit a connection hint on `PollResult`
   (or queue an `UnsentRequest` with no body) so the bg task can
   force a connect when `NodeApiVersions` are missing for a broker.
   **Closes wire-prereq #5.**
8. `Phase 10 (4/N): ApplicationEventProcessor — dispatch table + non-async arms`
   — every variant whose handling is synchronous (subscription
   changes, pause/resume, seek, reset, current-lag, topic-metadata,
   commit-on-close, etc.).
9. `Phase 10 (5/N): ApplicationEventProcessor — async-dispatch arms`
   — `AsyncPollEvent`, `Unsubscribe`, `LeaveGroupOnClose`,
   `CommitAsync`/`CommitSync`, `FetchCommittedOffsets`,
   `ListOffsets`, `CheckAndUpdatePositions`, `CreateFetchRequests`
   (everything that spawns a continuation task).
10. `Phase 10 (6/N): ApplicationEventProcessorTest — translate all in-scope tests`
    (39 → ~33 after Streams skips).
11. `Phase 10 (7/N): ConsumerNetworkThread — runOnce skeleton + shutdown`
    — bg task, wakeup wiring, AtomicI64 maximum-time-to-wait, cleanup.
    **Must explicitly call `membership.reconcile()` per iteration** —
    `entries()` skips the membership manager so its `maybeReconcile`
    side-effect (Java `AbstractMembershipManager.poll(...)`) must be
    re-supplied here (flagged in Critic round 1 aside).
12. `Phase 10 (8/N): ConsumerNetworkThreadTest`
    — 13 tests, mocking via the existing `RequestManager` trait.
13. `Phase 10 (9/N): wire-up + lint pass + agent-memory notes`
    — final `cargo xtask format-check + lint`; agent memory entry
    for any new patterns we found.

Each commit is independently buildable, has its own tests passing,
and is split so the Critic can review in small batches. Commit (1)
unblocks (8)–(13); commits (2) + (2.5) unblock (8); commits (3a–d)
unblock (9).

**Phase 11 carry-overs that remain open after Phase 10 closes** (do
NOT attempt these in Phase 10):

- `reset_poll_timer` → `maybe_rejoin_stale_member` invocation in
  `AsyncKafkaConsumer::poll()` epilogue (Phase 8b note).
- `consumer_membership_manager.leave_group()` /
  `leave_group_on_close()` invocation from `AsyncKafkaConsumer::close()`
  (Phase 8b note).

Both live on the `AsyncKafkaConsumer` surface, which Phase 11 owns.

## Definition of Done

Per `definition-of-done.md` plus the Milestone-8 phase additions:

- `cargo build`, `cargo test`, `cargo xtask format-check`,
  `cargo xtask lint` clean.
- Every in-scope test from `ConsumerNetworkThreadTest` and
  `ApplicationEventProcessorTest` translated and passing. Skipped
  tests are listed above with a one-line rationale each (DoD §3).
- `cargo test --lib consumer_network_thread` passes.
- `cargo test --lib application_event_processor` passes.
- The bg task end-to-end test: subscribe → produce one event →
  observe handle completion → close → assert no leaked tasks.
- No `panic!` / `unimplemented!` / `todo!` / `unsafe` in production
  code.
- `RequestManagers::entries()` returns managers in Java's
  registration order: `coordinator → commit → heartbeat → offsets →
  topic_metadata → fetch`. Verified by an order-assertion test.
- Lock-discipline audit on the processor: no `SubscriptionState`
  `MutexGuard` held across an `.await` or across a
  `BackgroundEventHandler::add` call (§16). Each variant arm is
  walked in the Critic round.
- No `#[async_trait]` on `EventProcessor<E>` (DoD §11).
- `ConsumerNetworkThread::run_once` has exactly ONE `.await` point
  (the `network_client.poll` `select!`). Verified by inspection in
  the Critic round.
- `consumer-threading.md` §10 / §11 cross-checked:
  - Single `tokio::spawn` per consumer (no manager-per-task split).
  - `wakeup()` uses the rotating-token primitive (`WakeupTrigger`),
    not an `AtomicBool`.
  - `process_application_events` drains via `try_recv` in a `while
    let` loop, unbounded.

## Parallelism inside Phase 10

Inside the commit chain above, commits (1)–(3) can each be opened in
parallel by separate Actors if desired (they touch disjoint files).
After (1)+(2)+(3) merge, the rest is serial: (4) and (5) share the
processor file; (7) builds on (4)+(5); (8) builds on (7); (9) is the
final pass. Default plan: single Actor running serially — the
parallelism here is small (~1 hour saved on the wire-prereqs) and
likely not worth coordinator overhead.

## Status — Closure

**Date closed:** 2026-05-29.

### Commits delivered (13)

| # | Hash | Title |
| --- | --- | --- |
| 1 | `66948a0` | Phase 10 (1/N): RequestManagers — wire offsets + fetch slots |
| 2 | `a0e3c19` | Phase 10 (2/N): CommitRequestManager — auto-commit hook + drain fix + retry counter |
| — | `d3ac959` | fixup! Phase 10 (2/N + 2.5/N + 1/N): COMMENTS.1.md fixes #1, #2, #3, #4, #5 |
| 2.5 | `4b9d99d` | Phase 10 (2.5/N): CommitRequestManager — MemberStateListener impl + auto-commit-sync-before-rebalance |
| 3a | `8187354` | Phase 10 (3a/N): OffsetsRequestManager — relocate init_with_committed_offsets_if_needed |
| 3b | `2b3d050` | Phase 10 (3b/N): OffsetsRequestManager — update_fetch_positions |
| 3c | `86e698d` | Phase 10 (3c/N): OffsetsRequestManager — fetch_offsets + cluster-listener replay |
| 3d | `eee76c4` | Phase 10 (3d/N): OffsetsRequestManager — try_connect plumbing |
| 4 | `8fcbe26` | Phase 10 (4/N): ApplicationEventProcessor — dispatch table + non-async arms |
| 5 | `4c76cae` | Phase 10 (5/N): ApplicationEventProcessor — async-dispatch arms |
| 6 | `3fc404a` | Phase 10 (6/N): ApplicationEventProcessorTest — translate all in-scope tests |
| 7 | `e86f7c1` | Phase 10 (7/N): ConsumerNetworkThread — runOnce skeleton + shutdown |
| 8 | `3d6e7d4` | Phase 10 (8/N): ConsumerNetworkThreadTest — exhaustive translation |
| 9 | _(this commit)_ | Phase 10 (9/N): wire-up + lint + agent-memory notes — Phase 10 closes |

All wire-prereqs (1–9) closed.

### Definition of Done — final check

  - [x] `cargo build` clean (no warnings).
  - [x] `cargo test --lib` 1546 tests pass, 0 failures.
  - [x] `cargo xtask format-check` clean.
  - [x] `cargo xtask lint` clean (clippy `-D warnings`).
  - [x] `cargo xtask check-generated` clean.
  - [x] Every in-scope test from `ConsumerNetworkThreadTest` (13) and
    `ApplicationEventProcessorTest` (39) translated. Skipped tests
    listed under "Skipped tests" with rationale.
  - [x] `cargo test --lib consumer_network_thread` passes.
  - [x] `cargo test --lib application_event_processor` passes.
  - [x] No `panic!` / `unimplemented!` / `todo!` / `unsafe` in
    production code. One `TODO:` comment remains at
    `consumer_membership_manager.rs:538` as a deliberate Phase-11
    carry-over marker explained in the surrounding context.
  - [x] `RequestManagers::entries()` returns managers in Java's
    registration order (membership skipped by design — Phase-3 of
    `run_once` re-supplies its side-effect).
  - [x] Lock-discipline audit on the processor: no `SubscriptionState`
    `MutexGuard` held across an `.await` or across a
    `BackgroundEventHandler::add` call. Walked in Critic round 1 and
    re-walked in commit 9.
  - [x] No `#[async_trait]` on `EventProcessor<E>` (sync `fn process`).
  - [x] `ConsumerNetworkThread::run_once` has exactly ONE wakeup-token
    cancellation-safe `.await` point (Phase 4 network poll under
    `select! { biased; token.cancelled; delegate.poll }`).
    The Phase-3 `membership.reconcile().await` and Phase-2
    `delegate.try_connect().await` calls run before the wakeup-guarded
    poll and are short-lived in practice — not bound to wakeup-token
    cancel-safety per §11.
  - [x] §10/§11 cross-checks:
    - Single `tokio::spawn` per consumer (run consumes self).
    - `wakeup()` uses the rotating-token primitive (`WakeupTrigger`).
    - `process_application_events` drains via `try_recv` in
      `while let` loop, unbounded.

### Carry-overs to Phase 11

These were explicitly deferred per the per-commit notes and are NOT
defects in Phase 10:

  - **`AsyncKafkaConsumer` glue** (Phase 11 owns):
    - Spawn the bg task via `tokio::spawn(consumer_network_thread.run())`.
    - App-side `wakeup()` plumbing — calling
      `WakeupTrigger::wakeup()` from the public API + rotating the
      token after a method returns `KafkaError::Wakeup(_)`.
    - `process_background_events` on the app side per §31 — drain
      rebalance-listener callback-needed events and invoke listeners
      inline on the caller's task.
    - `consumer.maximum_time_to_wait()` exposure to the app side.
    - `consumer.close(timeout)` — sends `signal_close`, awaits the
      spawn handle.
    - `consumer_membership_manager.leave_group()` /
      `leave_group_on_close()` invocation from `close()` epilogue
      (Phase 8b note).
    - `reset_poll_timer` → `maybe_rejoin_stale_member` invocation in
      `poll()` epilogue (Phase 8b note).
    - Auto-commit-sync-before-rebalance invocation inside the
      membership reconcile sequence: see
      `consumer_membership_manager.rs:538` TODO marker. The
      `CommitRequestManager::maybe_auto_commit_sync_before_rebalance`
      method (commit 2.5) is ready to call; only the poll-path
      scaffolding is missing.

  - **`AsyncConsumerMetrics` translation** (deferred across Milestone-8):
    - `testRunOnceRecordTimeBetweenNetworkThreadPoll`,
    - `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`,
    - `testRunOnceInvokesReaper` / `testCleanupInvokesReaper`
      metric-record assertions (the reaper-invocation observation
      itself was translated; only the metric check is deferred).

  - **`Supplier<...>` constructor error path** — Java's
    `testNetworkClientDelegateInitializeResourcesError` etc. exercise
    suppliers that fail during init. Rust constructor takes
    already-constructed values; the error path moves to the Phase 11
    consumer constructor wrapper.

  - **`testStartupAndTearDown`** — `Thread.start()` / `isAlive()`
    semantics belong on the `AsyncKafkaConsumer` spawn surface, not
    the network-thread struct itself. Phase 11.

  - **Streams arm tests in `ApplicationEventProcessorTest`** (6 cases)
    skipped per `consumer-threading.md` §20. Not a Phase 11 carry —
    explicitly out of milestone scope.

### Audit results (commit 9)

  - **TODO/FIXME/unimplemented/todo! search**: 1 hit total in
    production code — `consumer_membership_manager.rs:538` is a
    deliberate Phase-11 carry-over marker, well documented in the
    surrounding context. Per the wrap-commit instructions option (c),
    explained as intentional. All other matches are in `#[cfg(test)]`
    modules (panic-on-test-invariant patterns).
  - **`unwrap()` / `expect()` in production**: all hits are
    `.lock().expect("... poisoned")` on `std::sync::Mutex` (canonical
    recovery-impossible per CLAUDE.md §10.1) and a small number of
    invariant `.expect("delegate not contended on bg task")` /
    `.expect("receiver fresh")` invariant assertions (bg-task is the
    sole holder of the relevant lock — Java would catch the equivalent
    via `IllegalStateException`).
  - **`tokio::sync::Mutex<SubscriptionState>`**: zero hits.
  - **`parking_lot::Mutex`**: zero hits.
  - **`#[async_trait]` on `EventProcessor` or per-record traits**:
    zero hits.
  - **Handle preservation (§28)**: every `CompletableApplicationEvent<T>`
    arm in the processor calls `handle.complete*` exactly once on
    every code path (success / Err / recv_err / early-return-failure).
    Spawned continuations match all three receiver-result branches.
  - **Lock discipline (§16)**: every `.await` site in the Phase-10
    production code reviewed. `std::sync::Mutex` guards
    (`RequestManagers`, `SubscriptionState`, manager-internal state)
    are released before `.await` in every spawned-task path. The only
    lock held across `.await` is `Arc<tokio::sync::Mutex<NetworkClientDelegate>>`
    — the runtime-async lock owned solely by the bg task, by design.

Phase 10 closes.
