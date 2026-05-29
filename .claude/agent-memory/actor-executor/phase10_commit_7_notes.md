---
name: phase10-commit-7-notes
description: Milestone-8 Phase 10 (7/N) — ConsumerNetworkThread runOnce skeleton, wakeup wiring, membership.reconcile per iteration, cleanup path, smoke tests
metadata:
  type: project
---

# Phase 10 (7/N) — ConsumerNetworkThread runOnce skeleton + shutdown

## Java → Rust runOnce phase mapping

Java `ConsumerNetworkThread.runOnce()` (lines 210–242) maps to Rust
`ConsumerNetworkThread::run_once`:

| Java | Rust |
| --- | --- |
| `processApplicationEvents()` (line 212) | `self.process_application_events()` (drain via `try_recv`, register completable handles with reaper, per-event `maybeFailOnMetadataError` arm, dispatch via AEP) |
| `time.milliseconds()` + last-poll metric (lines 214-218) | `self.time.milliseconds()` + log-only `time-between-network-thread-poll` |
| For-each `rm.poll(now)` + `delegate.addAll(result)` (lines 222-226) | Collect `PollResult`s under `rm_guard` (sync), drop guard, then iterate under `delegate_guard.lock().await` doing per-pollResult `try_connect` await + `add_all_from_poll_result`. Splits Java's combined loop because `try_connect` is async in Rust |
| **(missing in Java entries fold — implicit via `entries()` containing membership)** | **Phase 3: `membership.reconcile(now).await`** — load-bearing per Critic round-1 aside |
| `networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs)` (line 228) | `tokio::select! { biased; token.cancelled(); delegate_guard.poll_default(timeout, now) }` — only `.await` requiring wakeup-token cancel-safety per §11 |
| For-each `rm.maximumTimeToWait(now)` (lines 230-237) | Re-lock `rm_guard`, fold to `cached_max_time_to_wait_ms: AtomicI64` |
| `reapExpiredApplicationEvents(now)` (line 239) | `reaper.lock().reap(now)` |
| `uncompletedEvents()` + `maybeFailOnMetadataError(uncompletedEvents)` (lines 240-241) | **Deferred to commit 8** — Rust reaper holds erased handles only; cannot match `MetadataErrorNotifiableEvent` after the fact. Per-event arm still works. |

## Where membership.reconcile() is called

Phase 3 of `run_once`, between the for-each `rm.poll(...)` loop (Phase 2)
and the `delegate.poll(...)` `select!` (Phase 4). This is the same phase
point Java's `entries()` loop would visit if `membership` were in
`entries()`:

```
coordinator → commit → heartbeat → [membership] → offsets → topic_metadata → fetch
                                        ↑
                                Java has it here.
                                Rust calls reconcile() at the same spot
                                (between Phase 2 manager poll and Phase 4
                                network poll).
```

Java's `AbstractMembershipManager.poll(...)` body is
`maybeReconcile(false); return PollResult.EMPTY;` — so the side-effect
is the only thing we need to preserve. Errors from `reconcile` are
logged and swallowed (matching Java's surrounding-runOnce catch).

## Wakeup rotation interaction

Bg-task `run_once`:
1. Reads `self.wakeup_rx.borrow().clone()` to get the **current** token
   at the top of the network-poll select. This re-read is essential —
   if the consumer-side rotated the token between iterations, the bg
   task picks up the fresh token automatically.
2. `tokio::select!` arms: `biased; token.cancelled(); delegate.poll(...)`.
3. The token is NOT rotated by the bg task. Rotation is the app side's
   responsibility (Phase 11) — `consumer-threading.md` §11 explicitly
   says rotation happens on the app side after returning
   `KafkaError::wakeup(...)`.

The `WakeupTrigger` was already implemented in `wakeup_trigger.rs`
(Phase 5). The bg task uses `subscribe()` to get a `watch::Receiver`
and `borrow().clone()` to read the current token each iteration.

## Smoke tests added (7 lib tests)

In `consumer_network_thread.rs` inline tests:

1. `signal_close_stops_running` — `is_running()` flips after `signal_close()`.
2. `run_once_happy_path_refreshes_max_time_to_wait` — single iteration
   completes without panic and refreshes `cached_max_time_to_wait_ms`.
3. `run_once_returns_when_wakeup_fires_during_poll` — wakeup mid-poll
   exits the `select!` within 500 ms (vs 5_000 ms `MAX_POLL_TIMEOUT_MS`).
4. `run_once_invokes_membership_reconcile` — with a membership manager
   wired in, `run_once` completes without panic (membership in
   non-Reconciling state → reconcile short-circuits to `Ok(())`).
5. `cleanup_completes_cleanly_when_idle` — `cleanup()` drains a tracked
   reaper event with `reap_on_close` and exits cleanly.
6. `process_events_registers_completable_with_reaper` — direct call to
   `process_application_events` confirms `AssignmentChange` was added
   to the reaper via `reaper.contains(&erased_external)` (inner-id
   comparison survives erased-clone recreation, per Phase 5 contract).
7. `process_events_skips_non_completable_variants` — `CommitOnClose` /
   `NewTopicsMetadataUpdate` are NOT registered with the reaper.
8. `leave_group_event_registered_with_reaper` — confirms `LeaveGroupOnClose`
   completable variant is registered (broadens the coverage of the
   `erased_handle()` match arm).

Plus 2 new tests in `application_event.rs` covering the helper
accessors:

- `metadata_error_notifiable_predicate_matches_on_metadata_error` —
  pins the equivalence of `is_metadata_error_notifiable()` and
  `on_metadata_error()` so the two don't drift.
- `erased_handle_returns_some_for_completable_variants` — confirms
  `AsyncPoll` returns `None` (it's not `CompletableApplicationEvent`)
  despite being metadata-error-notifiable.

## Key design choices / deviations from Java

- **`ApplicationEvent::erased_handle()` added** — Rust's reaper holds
  erased handles; the bg task needs an accessor on the event enum to
  produce `Some(handle.erased())` for completable variants. Java does
  `instanceof CompletableEvent` instead.
- **`ApplicationEvent::is_metadata_error_notifiable()` added** —
  predicate equivalent to Java's `instanceof MetadataErrorNotifiableEvent`.
  Paired with `on_metadata_error()` via the
  `metadata_error_notifiable_predicate_matches_on_metadata_error` test
  to prevent drift.
- **`ThreadTime` trait** — analogous to `FetchCollectorTime` in
  `fetch_collector.rs`; injects a mock clock in tests, defaults to
  `SystemThreadTime` in production. Java has `Time` for the same.
- **`AsyncConsumerMetrics` skipped** — not yet translated. All metric
  call sites in Java become `log::trace!` in Rust with a no-op
  equivalent. Metric tests deferred to a future milestone.
- **`maybeFailOnMetadataError(uncompletedEvents)` deferred** — Java
  filters the reaper's `uncompletedEvents` for
  `MetadataErrorNotifiableEvent`, but the Rust reaper holds erased
  handles only. Per-event arm (inside
  `process_application_events`) still works correctly; the post-poll
  variant is a narrower extra notification, deferred to commit 8.
- **`Mutex<NetworkClientDelegate<K>>`** — wrapped in `tokio::sync::Mutex`
  because the delegate's `poll(...)` is async and held by a single bg
  task in production. Test paths (which run on the same task as the
  thread) use `try_lock()` for the sync read sites.

## Concurrency invariants honored

- §10: single `tokio::spawn` per consumer (the `run` entry consumes
  `self` and is intended for `tokio::spawn`). No per-RM subtasks.
- §11: only `.await` in `run_once` that needs wakeup-token
  cancellation is `network_client.poll(...)`. The membership
  `reconcile(...).await` and `delegate.try_connect(...).await` are
  the two other awaits — these run before the wakeup-guarded poll
  and are short-lived in practice.
- §16: `std::sync::Mutex<RequestManagers>` and
  `std::sync::Mutex<SubscriptionState>` (inside managers) are never
  held across `.await`. Collected `PollResult`s are taken under the
  rm_guard sync, then guard is dropped before the delegate await.
- §28: completable events register erased handles with the reaper
  during the drain.
- §31: the listener-callback handshake is not exercised here (lives
  inside `process_application_events` arms via the AEP); the
  membership reconcile **awaits** the listener oneshot per §31. Bg
  task does not `tokio::spawn` listener invocations.

## What's deferred to commit 8 and Phase 11

**Commit 8 (exhaustive ConsumerNetworkThreadTest):**
- Parameterized poll-time computations test
  (`testConsumerNetworkThreadPollTimeComputations` with three
  `@ValueSource` values).
- `testRequestsTransferFromManagersToClientOnThreadRun`,
  `testMaximumTimeToWait`, `testCleanupInvokesReaper`,
  `testSendUnsentRequests`, `testRunOnceInvokesReaper` —
  Mockito-driven in Java; in Rust these need either a `RequestManager`
  spy or a hand-rolled fake to observe per-call counts.
- `initializeResources` error-path tests
  (`testNetworkClientDelegateInitializeResourcesError`,
  `testRequestManagersInitializeResourcesError`,
  `testNetworkClientDelegateAndRequestManagersInitializeResourcesError`) —
  Rust takes already-constructed values in the constructor instead of
  `Supplier<...>`, so the error path moves to the call site (Phase 11
  consumer constructor). May skip with explanation.
- `runAtClose` test — already covered indirectly by
  `cleanup_completes_cleanly_when_idle`; pin it explicitly.
- `runOnce` `maybeFailOnMetadataError(uncompletedEvents)` arm — needs
  typed reaper or a side-channel from `process_application_events`.
- Metric assertions (`testRunOnceRecordTimeBetweenNetworkThreadPoll`,
  `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`) —
  deferred until `AsyncConsumerMetrics` lands.

**Phase 11 (AsyncKafkaConsumer glue):**
- App-side `wakeup()` plumbing — calling `WakeupTrigger::wakeup()`
  from the public API + rotating after a method returns
  `KafkaError::Wakeup(_)`.
- Spawning `consumer_network_thread.run().await` via `tokio::spawn`
  inside the consumer constructor.
- `process_background_events` on the app side per §31 — drains
  rebalance-listener callback-needed events and invokes listeners
  inline.
- `consumer.maximum_time_to_wait()` exposure.
- `consumer.close(timeout)` — sends `signal_close`, awaits the spawn
  handle.

## Files touched

- New: `src/consumer/internals/consumer_network_thread.rs` (~960 lines
  inc. tests + 10 smoke tests)
- `src/consumer/internals/mod.rs` — register module
- `src/consumer/internals/events/application_event.rs` — added
  `is_metadata_error_notifiable()` and `erased_handle()` accessors +
  2 inline tests
