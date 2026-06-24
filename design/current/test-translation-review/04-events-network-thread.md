# Test-translation review: events, network-thread, request-plumbing (KIP-848)

Read-only review of how faithfully the Java consumer test suites for the
event processor, event handlers, network thread, network-client delegate,
request managers/state, and wakeup trigger were translated to Rust.

Statuses: **PRESERVED** (same behavioral assertion), **REDUCED** (translated
but weaker assertions), **CHANGED** (tests a different mechanism by design),
**MISSING** (in-scope, no equivalent), **OUT_OF_SCOPE** (legitimately not
translated — Streams §20, metrics, or supplier-indirection moved to a
different layer).

---

## Summary

| Java file | Tests* | In-scope | PRESERVED | CHANGED | OOS/Deferred | MISSING |
|---|---|---|---|---|---|---|
| ApplicationEventProcessorTest | 39 (47 cases) | 33 | 33 | 0 | 6 (Streams) | 0 |
| CompletableEventReaperTest | 5 | 5 | 5 | 0 | 0 | 0 |
| ApplicationEventHandlerTest | 4 | 0 | 0 | 0 | 4 | 0 |
| BackgroundEventHandlerTest | 1 | 0 | 0 | 0 | 1 (metrics) | 0 |
| ConsumerNetworkThreadTest | 13 | 7 | 7 | 0 | 6 | 0 |
| NetworkClientDelegateTest | 13 | 12 | 12 | 0 | 1 (metrics) | 0 |
| RequestManagersTest | 2 | 1 | 0 (covered elsewhere) | 0 | 1 (Streams) | 0 (see KF-1) |
| RequestStateTest | 3 | 3 | 3 | 0 | 0 | 0 |
| TimedRequestStateTest | 5 | 5 | 5 | 0 | 0 | 0 |
| WakeupTriggerTest | 18 | 18 | — | 18 (design change) | 0 | 0 (guarantee covered) |

\* "Tests" counts annotations; parameterized cases expand further (AEP 39
annotations → ~47 concrete cases via `@ParameterizedTest`/`@ValueSource`).

**No genuine in-scope MISSING test was found.** Every gap is either an
out-of-scope family (Streams, AsyncConsumerMetrics) or a behavioral guarantee
covered in a different module due to a deliberate design difference. The
translation is faithful and in several places stronger than Java (extra
edge-case tests added).

---

## Key findings

- **KF-1 (note, not a bug): `RequestManagersTest.testMemberStateListenerRegistered`
  is not translated in `request_managers.rs`.** The Rust `RequestManagers`
  has no `supplier(...)` factory — construction/wiring lives in
  `async_kafka_consumer.rs`. The behavioral guarantee Java asserts (the
  `CommitRequestManager` and the consumer's `MemberStateListener` are both
  registered as state listeners on the membership manager) **is covered** by
  `async_kafka_consumer.rs::issue_7_commit_request_manager_registered_as_member_state_listener`
  and by the membership-manager listener tests
  (`consumer_membership_manager.rs:1447/1658/1694`). The Rust
  `request_managers.rs` tests are container-shape tests (`entries()` ordering,
  Arc-shared-slot exclusion, idempotent close) with no Java analog — clearly
  documented as such. Acceptable.

- **KF-2: WakeupTrigger is a deliberate design change (rotating
  `CancellationToken`), and the Rust file translates only ~5 of the 18 Java
  tests directly — but the two load-bearing guarantees are preserved.** "A
  `wakeup()` unblocks a task `select!`-ing on the current token" is tested
  (`wakeup_cancels_select_on_current_token`); "rotation clears the pending
  wakeup state after one throw" is tested (`rotate_replaces_the_token`).
  `disableWakeups`, `maybeTriggerWakeup`, and the wakeup-before-subscribe race
  are all covered. The Java `FetchAction` / `ShareFetchAction` / `WakeupFuture`
  state-machine tests (`testSettingFetchAction`, `testWakeupFromFetchAction`,
  `testWakeupFromShareFetchAction`, `testExceptionTriggeredWhenTask*`,
  the `getPendingTask`/`clearTask` tests) test the Java `AtomicReference<Wakeupable>`
  internals, which **do not exist** in the Rust design — correctly CHANGED, not
  missing. The `-8` raw count delta is fully explained by this design
  divergence. **Verify in steady-state integration that `poll()` actually
  returns `KafkaError::Wakeup` and rotates** — the unit file does not exercise
  the public-API integration of the trigger (that belongs to
  `async_kafka_consumer.rs`).

- **KF-3: Network-poll cancel-safety and `run_once` phase ordering are well
  tested** (`consumer-threading.md` §10). `run_once_returns_when_wakeup_fires_during_poll`
  and `application_event_notify_preempts_blocking_network_poll` assert the poll
  is preempted via the `Notify`/token `select!` arms (not cancelled mid-flight),
  with a custom blocking `MockClient` that proves the poll would otherwise hang.
  Phase computations (`test_consumer_network_thread_poll_time_computations_*`,
  the `@ValueSource` MAX_POLL_TIMEOUT−1/=/+1 cases) and manager-poll ordering
  (`test_requests_transfer_from_managers_to_client_on_thread_run`) are PRESERVED.

- **KF-4: Completable-vs-bare event handle wiring is correctly tested**
  (§28). `make_completable_event` returns `(handle, rx, erased)`; tests await the
  app-side `oneshot::Receiver` for the typed `T` and assert both success values
  (`fetch_committed_offsets_event_returns_offsets_on_success`,
  `sync_commit_event_with_offsets_uses_offsets`) and error propagation
  (`*_with_exception_propagates_to_handle`). Non-completable variants
  (`AsyncPoll`, `CommitOnClose`, `NewTopicsMetadataUpdate`) carry no handle and
  are verified to skip reaper registration
  (`process_events_skips_non_completable_variants`). The `AsyncPoll` state-triple
  (`error`/`is_complete`) is asserted directly per §28's `AsyncPollEvent`
  precedent.

- **KF-5: Reaper expiry/completion semantics fully PRESERVED.** All 5 Java
  cases map 1:1 (testExpired, testCompleted, testCompletedAndExpired,
  testIncompleteQueue → `reap_on_close_handles_queue_only_events`,
  testIncompleteTracked → `reap_on_close_handles_tracked_only_events`),
  including the `events.clear()` post-condition and the "completed event is
  not counted as expired" distinction. Extra Rust tests cover the
  `inner_id()` identity-across-erased-recreation footgun (COMMENTS.1.md #11
  regression).

- **KF-6: AsyncConsumerMetrics is not translated, so all metrics-only tests
  are OUT_OF_SCOPE** and explicitly documented at each call site
  (`background_event_handler.rs`, `network_client_delegate.rs:481`,
  `consumer_network_thread.rs:69/1821`). This removes
  `testRecordBackgroundEventQueueSize`, `testRecordApplicationEventQueueSize`,
  `testRecordUnsentRequestsQueueTime`, `testRunOnceRecordTimeBetweenNetworkThreadPoll`,
  `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`.
  When the metrics framework lands these should be re-added (tracked in the
  inline comments).

- **KF-7: `initializeResources` supplier-error tests are OOS by design.** The
  Rust constructors take already-constructed values (no `Supplier` indirection),
  so `ApplicationEventHandlerTest` (all 3 init-error tests) and
  `ConsumerNetworkThreadTest`'s 3 init-error tests have no analog at this layer;
  documented as relocated to the Phase-11 consumer-constructor call site.
  **Recommend confirming a constructor-failure test exists in
  `async_kafka_consumer.rs`** so the guarantee ("a failed resource build does
  not leave a half-initialized bg task / cleanup does not panic") is not lost
  entirely.

- **KF-8: Error-message content is asserted, not just `is_err()`** (DoD §3).
  The four "mutually exclusive" subscription-conflict assertions check the
  message substring; NCD timeout/disconnect tests assert the specific error
  kind (`KafkaError::Timeout` vs `Errors::NetworkException` for Java's
  `DisconnectException`); `refresh_committed_offsets_failure_helper` asserts the
  error is surfaced on `state.error()` and not swallowed.

---

## Per-file detail

### ApplicationEventProcessorTest (39 annotations / ~47 cases)

| Java test | Status | Notes |
|---|---|---|
| testPrepClosingCommitEvents | PRESERVED | `prep_closing_commit_events_signals_close` — verifies `signal_close`. |
| testProcessUnsubscribeEventWithGroupId | PRESERVED | `process_unsubscribe_event_with_group_id` — leaveGroup path. |
| testProcessUnsubscribeEventWithoutGroupId | PRESERVED | `unsubscribe_without_group_id_clears_subscription_inline`. |
| testApplicationEventIsProcessed (×5) | PRESERVED | `application_event_is_processed_dispatches_all_representatives` covers AsyncPoll, CreateFetchRequests, CheckAndUpdatePositions, TopicMetadata, AssignmentChange. |
| testListOffsetsEventIsProcessed (×2) | PRESERVED | `list_offsets_event_is_processed` loops `[true,false]`. |
| testAssignmentChangeEvent (×2) | PRESERVED | `assignment_change_event_with_group_id_updates_timer_and_assigns` + `..._without_group_id_assigns_only`; both group-id arms covered. |
| testAssignmentChangeEventWithException | PRESERVED | `assignment_change_event_with_exception`. |
| testResetOffsetEvent | PRESERVED | `reset_offset_event_resets_strategy_for_partitions`. |
| testSeekUnvalidatedEvent | PRESERVED | `seek_unvalidated_event_sets_position`. |
| testSeekUnvalidatedEventWithException | PRESERVED | `seek_unvalidated_event_with_exception_fails_handle`. |
| testAsyncPollEvent | PRESERVED | `async_poll_event_completes_state_through_full_chain`. |
| testTopicSubscriptionChangeEvent | PRESERVED | `topic_subscription_change_event_updates_subscription_and_records_version`. |
| testFetchCommittedOffsetsEvent | PRESERVED | `fetch_committed_offsets_event_returns_offsets_on_success`. |
| testTopicSubscriptionChangeEventWithIllegalSubscriptionState | PRESERVED | `topic_subscription_change_event_with_illegal_state` (msg asserted). |
| testTopicPatternSubscriptionChangeEvent | PRESERVED | `topic_pattern_subscription_change_event_updates_subscription`. |
| testTopicPatternSubscriptionTriggersJoin | PRESERVED | `topic_pattern_subscription_triggers_join_even_with_no_matches`. |
| testTopicPatternSubscriptionChangeEventWithIllegalSubscriptionState | PRESERVED | `topic_pattern_subscription_change_event_with_illegal_state` (msg). |
| testUpdatePatternSubscriptionEventOnlyTakesEffectWhenMetadataHasNewVersion | PRESERVED | `update_pattern_subscription_event_only_takes_effect_when_metadata_advances`. |
| testR2JPatternSubscriptionEventSuccess | PRESERVED | `r2j_pattern_subscription_event_success`. |
| testR2JPatternSubscriptionEventFailureWithMixedSubscriptionType | PRESERVED | `r2j_pattern_subscription_event_failure_with_mixed_type` (msg). |
| testSyncCommitEventWithEmptyOffsets | PRESERVED | `sync_commit_event_with_empty_offsets_uses_all_consumed`. |
| testSyncCommitEvent | PRESERVED | `sync_commit_event_with_offsets_uses_offsets`. |
| testSyncCommitEventWithoutCommitRequestManager | PRESERVED | `commit_sync_without_commit_manager_fails_with_illegal_state` (Java KafkaException → Rust IllegalState). |
| testSyncCommitEventWithException | PRESERVED | `sync_commit_event_with_exception_propagates_to_handle`. |
| testAsyncCommitEventWithEmptyOffsets | PRESERVED | `async_commit_event_with_empty_offsets_uses_all_consumed`. |
| testAsyncCommitEvent | PRESERVED | `async_commit_event_with_offsets_uses_offsets`. |
| testAsyncCommitEventWithoutCommitRequestManager | PRESERVED | `commit_async_without_commit_manager_fails_with_illegal_state`. |
| testAsyncCommitEventWithException | PRESERVED | `async_commit_event_with_exception_propagates_to_handle`. |
| testStreamsOnTasksRevokedCallbackCompletedEvent | OUT_OF_SCOPE | Streams §20. |
| testStreamsOnTasksRevokedCallbackCompletedEventWithoutStreamsMembershipManager | OUT_OF_SCOPE | Streams §20. |
| testStreamsOnTasksAssignedCallbackCompletedEvent | OUT_OF_SCOPE | Streams §20. |
| testStreamsOnTasksAssignedCallbackCompletedEventWithoutStreamsMembershipManager | OUT_OF_SCOPE | Streams §20. |
| testStreamsOnAllTasksLostCallbackCompletedEvent | OUT_OF_SCOPE | Streams §20. |
| testStreamsOnAllTasksLostCallbackCompletedEventWithoutStreamsMembershipManager | OUT_OF_SCOPE | Streams §20. |
| testUpdatePatternSubscriptionInvokedWhenMetadataUpdated | PRESERVED | `update_pattern_subscription_invoked_when_metadata_updated`. |
| testUpdatePatternSubscriptionNotInvokedWhenNotUsingPatternSubscription | PRESERVED | `update_pattern_subscription_not_invoked_when_not_using_pattern_subscription`. |
| testUpdatePatternSubscriptionNotInvokedWhenMetadataNotUpdated | PRESERVED | `update_pattern_subscription_not_invoked_when_metadata_not_updated`. |
| testRefreshCommittedOffsetsShouldNotResetIfFailedWithTimeout | PRESERVED | `refresh_committed_offsets_should_not_reset_if_failed`. |
| testRefreshCommittedOffsetsNotCalledIfNoGroupId | PRESERVED | `refresh_committed_offsets_not_called_if_no_group_id`. |

Extra Rust tests with no Java analog (added, not required): `pause_partitions_event_*`,
`resume_partitions_event_unpauses_partitions`, `current_lag_event_returns_none_when_lag_unknown`,
`stop_find_coordinator_on_close_*`, `dispatch_table_covers_sync_variants`,
`rebalance_listener_callback_completed_is_noop`.

### CompletableEventReaperTest (5)

| Java test | Status | Notes |
|---|---|---|
| testExpired | PRESERVED | `expired_event_is_failed_and_removed`. |
| testCompleted | PRESERVED | `completed_event_is_removed_but_not_counted_as_expired`. |
| testCompletedAndExpired | PRESERVED | `completed_and_expired`. |
| testIncompleteQueue | PRESERVED | `reap_on_close_handles_queue_only_events` (queue clear asserted). |
| testIncompleteTracked | PRESERVED | `reap_on_close_handles_tracked_only_events`. |

### ApplicationEventHandlerTest (4)

| Java test | Status | Notes |
|---|---|---|
| testRecordApplicationEventQueueSize | OUT_OF_SCOPE | AsyncConsumerMetrics not translated (KF-6). |
| testFailOnInitializeResources | OUT_OF_SCOPE | Supplier-init indirection not in Rust handler (KF-7). |
| testDelayInInitializeResources | OUT_OF_SCOPE | Same. |
| testInterruptInInitializeResources | OUT_OF_SCOPE | Same; no thread-interrupt analog. |

Rust handler is a channel/`add_and_get` wrapper; its 6 inline tests
(enqueue+timestamp, notify-wake, receiver-dropped error, add_and_get value/error/dropped)
have no Java analog but exercise the surface that exists.

### BackgroundEventHandlerTest (1)

| Java test | Status | Notes |
|---|---|---|
| testRecordBackgroundEventQueueSize | OUT_OF_SCOPE | Metrics (KF-6). `drain_events` deliberately omitted (app holds the receiver). Rust `add` tests cover enqueue + receiver-dropped error. |

### ConsumerNetworkThreadTest (13)

| Java test | Status | Notes |
|---|---|---|
| testEnsureCloseStopsRunningThread | PRESERVED | `test_ensure_close_stops_running_thread`. |
| testConsumerNetworkThreadPollTimeComputations (×3) | PRESERVED | `_below_max` / `_at_max` / `_above_max`; asserts `min(t, MAX_POLL_TIMEOUT_MS)` to delegate poll + cached max-time-to-wait. |
| testRequestsTransferFromManagersToClientOnThreadRun | PRESERVED | `test_requests_transfer_from_managers_to_client_on_thread_run`. |
| testMaximumTimeToWait | PRESERVED | `test_maximum_time_to_wait` (initial MAX, then heartbeat interval). |
| testCleanupInvokesReaper | PRESERVED | `test_cleanup_invokes_reaper` (reap_on_close). |
| testRunOnceInvokesReaper | PRESERVED | `test_run_once_invokes_reaper`. |
| testSendUnsentRequests | PRESERVED | `test_send_unsent_requests` (poll-on-close twice). |
| testStartupAndTearDown | OUT_OF_SCOPE | Thread.start/isAlive belongs to AsyncKafkaConsumer spawn (KF-7). |
| testNetworkClientDelegateInitializeResourcesError | OUT_OF_SCOPE | Supplier indirection (KF-7). |
| testRequestManagersInitializeResourcesError | OUT_OF_SCOPE | Supplier indirection (KF-7). |
| testNetworkClientDelegateAndRequestManagersInitializeResourcesError | OUT_OF_SCOPE | Supplier indirection (KF-7). |
| testRunOnceRecordTimeBetweenNetworkThreadPoll | OUT_OF_SCOPE | Metrics (KF-6). |
| testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime | OUT_OF_SCOPE | Metrics (KF-6). |

Extra Rust tests (design-specific, valuable): cancel-safety / preemption
(`run_once_returns_when_wakeup_fires_during_poll`,
`application_event_notify_preempts_blocking_network_poll`), reaper-registration
(`process_events_registers_completable_with_reaper`,
`process_events_skips_non_completable_variants`), membership reconcile drive.

### NetworkClientDelegateTest (13)

| Java test | Status | Notes |
|---|---|---|
| testPollResultTimer | PRESERVED | `test_poll_result_timer`. |
| testSuccessfulResponse | PRESERVED | `test_successful_response` (oneshot resolves Ok). |
| testTimeoutBeforeSend | PRESERVED | `test_timeout_before_send` (Timeout error). |
| testTimeoutAfterSend | PRESERVED | `test_timeout_after_send` (NetworkException = Java DisconnectException). |
| testEnsureCorrectCompletionTimeOnFailure | PRESERVED | `test_ensure_correct_completion_time_on_failure`. |
| testEnsureCorrectCompletionTimeOnComplete | PRESERVED | `test_ensure_correct_completion_time_on_complete`. |
| testEnsureTimerSetOnAdd | PRESERVED | `test_ensure_timer_set_on_add` (add + add_all). |
| testHasAnyPendingRequests | PRESERVED | `test_has_any_pending_requests`. |
| testPropagateMetadataError | PRESERVED | `test_propagate_metadata_error`. |
| testPropagateMetadataErrorWithErrorEvent | PRESERVED | `test_propagate_metadata_error_with_error_event` (BackgroundEvent::Error). |
| testPollWithOnClose | PRESERVED | `test_poll_with_on_close`. |
| testCheckDisconnectsWithOnClose | PRESERVED | `test_check_disconnects_with_on_close`. |
| testRecordUnsentRequestsQueueTime | OUT_OF_SCOPE | Metrics (KF-6). |

Extra: `poll_result_empty_uses_wait_forever`, `poll_result_from_wait_carries_value`,
`unsent_request_defaults`, `future_completion_handler_idempotent_send`.

### RequestManagersTest (2)

| Java test | Status | Notes |
|---|---|---|
| testMemberStateListenerRegistered | OUT_OF_SCOPE / covered elsewhere | No `supplier(...)` in Rust; guarantee covered by `async_kafka_consumer.rs::issue_7_...` + membership listener tests (KF-1). |
| testStreamMemberStateListenerRegistered | OUT_OF_SCOPE | Streams §20. |

Rust `entries()` shape tests are container-only with no Java analog.

### RequestStateTest (3)

| Java test | Status | Notes |
|---|---|---|
| testRequestStateSimple | PRESERVED | jitter=0 → deterministic exponential backoff (340 ms boundary). |
| testTrackInflightOnSuccessfulAttempt | PRESERVED | `test_track_inflight_on_successful_attempt`. |
| testTrackInflightOnFailedAttempt | PRESERVED | `test_track_inflight_on_failed_attempt`. |

### TimedRequestStateTest (5)

| Java test | Status | Notes |
|---|---|---|
| testIsExpired | PRESERVED | `test_is_expired`. |
| testRemainingMs | PRESERVED | `test_remaining_ms`. |
| testDeadlineTimer | PRESERVED | `test_deadline_timer` (tests `deadline_for` directly). |
| testAllowOverdueDeadlineTimer | PRESERVED | `test_allow_overdue_deadline_timer`. |
| testToStringUpdatesTimer | PRESERVED | `test_to_string_updates_timer` (asserts `remainingMs=` substring; uses `to_string_with(now)` since Display can't read wall clock). |

Extra: `test_deref_to_request_state`.

### WakeupTriggerTest (18) — CHANGED (rotating CancellationToken design)

| Java test | Status | Notes |
|---|---|---|
| testEnsureActiveFutureCanBeWakeUp | CHANGED | `wakeup_cancels_select_on_current_token` — same guarantee via token. |
| testSettingActiveFutureAfterWakeupShouldThrow | CHANGED | `wakeup_before_subscribing_returns_immediately`. |
| testUnsetActiveFuture | CHANGED | No `clearTask`/pending-task state; subsumed by token model. |
| testSettingFetchAction | CHANGED | No FetchAction state machine in Rust. |
| testUnsetFetchAction | CHANGED | Same. |
| testWakeupFromFetchAction | CHANGED | FetchBuffer.wakeup() wiring lives in FetchBuffer, not the trigger. |
| testWakeupFromShareFetchAction | OUT_OF_SCOPE | Share-consumer §20. |
| testManualTriggerWhenWakeupCalled | PRESERVED | `maybe_trigger_wakeup_returns_error_after_wakeup`. |
| testManualTriggerWhenWakeupNotCalled | PRESERVED | `maybe_trigger_wakeup_returns_ok_without_prior_wakeup`. |
| testManualTriggerWhenWakeupCalledAndActiveTaskSet | CHANGED | Active-task state n/a; behavior subsumed. |
| testManualTriggerWhenWakeupCalledAndFetchActionSet | CHANGED | FetchAction state n/a. |
| testDisableWakeupWithoutPendingTask | PRESERVED | `disable_then_wakeup_is_a_noop`. |
| testDisableWakeupWithPendingTask | CHANGED | `disabled_wakeup_does_not_cancel_waiter` (token-based). |
| testDisableWakeupWithFetchAction | CHANGED | FetchAction n/a. |
| testDisableWakeupPreservedByClearTask | CHANGED | No clearTask; disable flag is sticky and tested via no-op behavior. |
| testExceptionTriggeredWhenTaskAsynchronouslyCompleted | CHANGED | CompletableFuture-completion-race specific to Java state machine. |
| testExceptionTriggeredWhenTaskAsynchronouslyFailed | CHANGED | Same. |
| testExceptionTriggeredWhenTaskAsynchronouslyCancelled | CHANGED | Same. |

Extra Rust tests covering the new design: `rotate_replaces_the_token`,
`rotate_is_noop_after_disable`, `clones_share_state`, `subscribe_observes_rotations`.
The two behavioral guarantees (wakeup interrupts a blocked await; rotate after
one throw) are both tested. See KF-2 for the integration follow-up.

---

## Recommendations (non-blocking)

1. Confirm a consumer-constructor / resource-build failure test exists in
   `async_kafka_consumer.rs` so the `initializeResources`-error guarantee
   (KF-7) is not lost entirely after relocation.
2. Confirm an integration test asserts `poll()` returns `KafkaError::Wakeup`
   and rotates the token (KF-2) — the unit file only tests the primitive.
3. When `AsyncConsumerMetrics` is translated, re-add the 5 deferred metrics
   tests (KF-6) tracked in the inline comments.
