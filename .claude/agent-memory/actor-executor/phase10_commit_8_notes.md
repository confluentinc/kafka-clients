---
name: phase10-commit-8-notes
description: Milestone-8 Phase 10 (8/N) — ConsumerNetworkThreadTest exhaustive translation; post-poll maybeFailOnMetadataError arm; SpyRequestManager + CountingClient test infra
metadata:
  type: project
---

# Phase 10 (8/N) — ConsumerNetworkThreadTest exhaustive translation

## Tests landed (13 of 13 Java tests classified)

**Translated as Java-parity tests (8 Rust tests for 6 Java tests):**
- `testEnsureCloseStopsRunningThread` → `test_ensure_close_stops_running_thread`
- `testConsumerNetworkThreadPollTimeComputations` (`@ParameterizedTest`
  with `MAX-1`, `MAX`, `MAX+1`) → 3 separate Rust tests preserving
  parameter labels (DoD §3 requirement). Helper:
  `run_poll_time_computations_case(example_time)`.
- `testRequestsTransferFromManagersToClientOnThreadRun` →
  `test_requests_transfer_from_managers_to_client_on_thread_run`
- `testMaximumTimeToWait` → `test_maximum_time_to_wait`
- `testCleanupInvokesReaper` → `test_cleanup_invokes_reaper`
- `testRunOnceInvokesReaper` → `test_run_once_invokes_reaper`
- `testSendUnsentRequests` → `test_send_unsent_requests`

**Skipped with rationale (5):**
- `testStartupAndTearDown` — `Thread.start()`/`isAlive()` semantics
  belong to Phase 11 (`AsyncKafkaConsumer` spawn/join), not the
  network-thread struct itself.
- `testNetworkClientDelegateInitializeResourcesError`,
  `testRequestManagersInitializeResourcesError`,
  `testNetworkClientDelegateAndRequestManagersInitializeResourcesError`
  — exercise Java's `Supplier<...>` constructor indirection. Rust
  constructor takes already-constructed values; error path moves to
  Phase 11 consumer constructor.
- `testRunOnceRecordTimeBetweenNetworkThreadPoll`,
  `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`
  — assert against `AsyncConsumerMetrics` histogram values. Metrics
  framework not yet translated; deferred to that milestone. Test sites
  emit `log::trace!` equivalents currently.

**Smoke tests deleted (3, subsumed):**
- `signal_close_stops_running` → `test_ensure_close_stops_running_thread`.
- `run_once_happy_path_refreshes_max_time_to_wait` →
  `test_maximum_time_to_wait` (broader assertion).
- `cleanup_completes_cleanly_when_idle` → `test_cleanup_invokes_reaper`.

**Smoke tests retained (5, Rust-specific):**
- `run_once_returns_when_wakeup_fires_during_poll`
- `process_events_registers_completable_with_reaper`
- `process_events_skips_non_completable_variants`
- `run_once_invokes_membership_reconcile`
- `leave_group_event_registered_with_reaper`

## Production code added/changed

### `ApplicationEvent::metadata_error_notifiable_handle()`

New accessor returning `Some(erased_handle)` for the intersection of
notifiable AND completable variants:
- `CheckAndUpdatePositions`, `ListOffsets`, `TopicMetadata`,
  `AllTopicsMetadata`.
- `AsyncPoll` is notifiable but NOT completable in Java (does not
  extend `CompletableApplicationEvent`), so it does NOT appear in
  `uncompletedEvents()` and is excluded here too. Per-event arm still
  notifies it via `on_metadata_error`.

Unit-tested with `metadata_error_notifiable_handle_returns_intersection`
in `application_event.rs`.

### `ConsumerNetworkThread::notifiable_handles: Vec<Arc<dyn …>>`

Side-list mirroring Java's
`applicationEventReaper.uncompletedEvents()` filtered for
`MetadataErrorNotifiableEvent`. Populated in
`process_application_events` (only for notifiable+completable
variants), pruned by `is_done()` checks at the post-poll arm.

### `ConsumerNetworkThread::maybe_fail_on_metadata_error_uncompleted()`

New method called as `run_once` Phase 7 (after Phase 6 reap):
1. Prune `notifiable_handles` of done entries via `retain(!is_done)`.
2. If list is empty, return WITHOUT consuming the delegate's
   `metadata_error` (Java's "Don't get-and-clear if no events to
   notify" optimisation — `ConsumerNetworkThread.java:447-449`).
3. `try_lock` the delegate, call `get_and_clear_metadata_error()`.
4. If `Some(err)`, fan it out to each live handle via
   `handle.fail_with_timeout(err.clone())`. `KafkaError` derives
   `Clone`.

Two regression tests:
- `maybe_fail_on_metadata_error_post_poll_fans_out_to_notifiable_events`:
  plants a `topic_authorization` error via
  `metadata.metadata_arc().fatal_error(...)`, drives one `run_once`,
  observes the receiver got `KafkaError::TopicAuthorization(_)`
  (variant preserved, NOT wrapped in Timeout).
- `maybe_fail_on_metadata_error_skips_delegate_when_no_notifiable_events`:
  same setup BUT no handles registered; verifies the delegate's
  `metadata_error` is still `Some` after `run_once` (proves we did
  NOT consume it).

## Test infrastructure added

### `SpyRequestManager` (test-module)

Records `poll` / `maximum_time_to_wait` / `poll_on_close` call counts
via shared `Arc<AtomicUsize>`. Returns scripted `PollResult` / wait-ms.
Replaces Mockito's `mock(RequestManager.class) +
when(rm.poll(anyLong())).thenReturn(...)`.

To inject into `entries()`, added:

### `RequestManagers::with_dyn_managers(Vec<Box<dyn RequestManager>>)`

Test-only constructor (`#[cfg(test)]`) that populates ONLY the
auxiliary `dyn_managers: Vec<Box<dyn RequestManager>>` field.
Production path leaves it empty. `entries()` iterates concrete slots
first (production order) then appends from `dyn_managers`.

### `CountingClient` (test-module)

`KafkaClient` wrapper over `MockClient` that records:
- `poll_call_count: Arc<AtomicUsize>` — number of `poll(...)` awaits.
- `poll_timeouts: Arc<Mutex<Vec<i64>>>` — timeout argument per call.
- `has_in_flight_script: Arc<Mutex<VecDeque<bool>>>` — scripted
  `has_in_flight_requests()` returns; falls back to inner when empty.

Forwards all 22 KafkaClient methods to the inner `MockClient`. Edition
2024 `async fn in trait` works without `#[async_trait]`.

### `NetworkClientDelegate::client_for_test_ref(&self) -> &K`

`#[cfg(test)]` accessor exposing the inner client to read
`CountingClient` counters out of the delegate.

## Lock-discipline audit

Re-walked `run_once` and `cleanup` line-by-line:
- Phase 1 `process_application_events`: reaper Mutex sync only;
  delegate `try_lock` sync only. No `.await` while held.
- Phase 2 manager poll: `rm_guard` sync, drop, then
  `delegate_guard.lock().await`. Disjoint critical sections.
- Phase 3 membership reconcile: `.await` is on the membership
  manager's internal state (NOT on any guard held by the bg task).
- Phase 4 network poll: `delegate_guard.lock().await` then `select!`.
  Only `.await` requiring wakeup-token cancel-safety.
- Phase 5 max time fold: `rm_guard` sync only.
- Phase 6 reap: reaper sync only.
- Phase 7 (new) post-poll metadata-error: `delegate.try_lock()` sync
  only. `KafkaError::clone` is cheap (Arc-y inside the variants).

All checks pass: build, test (16 file-local + 1546 lib-total), format,
clippy.

## Java-source line-mapping (for future Critic review)

| Rust site | Java site |
| --- | --- |
| `run_once` Phase 7 | `ConsumerNetworkThread.java:240-241` |
| `maybe_fail_on_metadata_error_uncompleted` | `ConsumerNetworkThread.java:438-459` |
| `process_application_events` notifiable_handles push | `ConsumerNetworkThread.java:258-260` (registry path) |
| `notifiable_handles` field | side-list for Java's `applicationEventReaper.uncompletedEvents()` filtered for `MetadataErrorNotifiableEvent` |

## What's deferred (still — to Phase 11 / AsyncConsumerMetrics)

- `AsyncKafkaConsumer` spawn-and-join (which is what
  `testStartupAndTearDown` covers in Java).
- The Supplier-style `initializeResources` error path (Phase 11
  consumer constructor wrapping).
- Metric-bearing tests
  (`testRunOnceRecordTimeBetweenNetworkThreadPoll` and
  `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`)
  — wait for `AsyncConsumerMetrics`.

## Files touched

- `src/consumer/internals/consumer_network_thread.rs` — new
  `notifiable_handles` field, `maybe_fail_on_metadata_error_uncompleted`
  method, `push_notifiable_handle_for_test` helper, test-only
  `SpyRequestManager` and `CountingClient`, 10 new tests (8
  Java-parity + 2 metadata-error regression), 3 smoke tests deleted.
- `src/consumer/internals/events/application_event.rs` — new
  `metadata_error_notifiable_handle()` accessor + unit test.
- `src/consumer/internals/network_client_delegate.rs` — new
  `#[cfg(test)] fn client_for_test_ref(&self) -> &K`.
- `src/consumer/internals/request_managers.rs` — new
  `dyn_managers: Vec<Box<dyn RequestManager>>` field and
  `#[cfg(test)] fn with_dyn_managers(...)` constructor. `entries()`
  iterates dyn managers after concrete slots.
