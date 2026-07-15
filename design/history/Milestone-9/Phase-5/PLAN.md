# Phase 5: Share consume request manager + events

## Goal

Translate the largest share-consumer class — `ShareConsumeRequestManager`
(~1571 Java LOC: fetch + acknowledge orchestration, per-node session state,
in-flight-ack slot routing) — plus the share event family under
`internals/events/` and the `ShareAcknowledgementEventHandler`. Register the
share-manager slots in `request_managers.rs`.

## Branch

`milestone9-share-consumer`.

## Java sources

All paths relative to
`kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/`,
submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

- `ShareConsumeRequestManager.java`
- Events under `events/`: `ShareFetchEvent`, `SharePollEvent`,
  `ShareSubscriptionChangeEvent`, `ShareUnsubscribeEvent`,
  `ShareAcknowledgeAsyncEvent`, `ShareAcknowledgeSyncEvent`,
  `ShareAcknowledgeOnCloseEvent`, `ShareAcknowledgementCommitCallbackRegistrationEvent`,
  and the `ShareAcknowledgementEventHandler`.

Tests: `ShareConsumeRequestManagerTest`.

## Rust output

- `src/consumer/internals/share_consume_request_manager.rs`
- `src/consumer/internals/events/share_fetch_event.rs`,
  `share_poll_event.rs`, `share_subscription_change_event.rs`,
  `share_unsubscribe_event.rs`, `share_acknowledge_async_event.rs`,
  `share_acknowledge_sync_event.rs`, `share_acknowledge_on_close_event.rs`,
  `share_acknowledgement_event.rs`,
  `share_acknowledgement_commit_callback_registration_event.rs`,
  `share_acknowledgement_event_handler.rs`
- Slot registration in `src/consumer/internals/request_managers.rs`

Events mirror Java's `CompletableApplicationEvent<T>` hierarchy (§28): completable
classes carry `CompletableEventHandle<T>` with the correct `T`
(`Void`→`()`, `Map<TopicIdPartition,Acknowledgements>`→`ShareAcknowledgeSyncResult`);
bare `ApplicationEvent` classes are plain structs.

## Design notes

- Per-node single-in-flight-ack invariant (`nodes_with_pending_requests`) makes
  `find_in_flight_ack_slot` routing unique.
- §31 exactly-once callback: every `maybe_send_share_acknowledgement_event` exit
  removes each in-flight ack exactly once (`shift_remove` / `mem::take`) before
  firing — no double-fire, no drop, no `tokio::spawn`.

## Commits

- `0ea0f6d` — share consumer events + acknowledgement event handler
- `e0a9545` — `ShareConsumeRequestManager`
- `503f8ca`, `37d0ad8`, `48f122f`, `497417e`, `8862b4d` —
  `ShareConsumeRequestManagerTest` (49 pass, 1 `#[ignore]`)
- `62d820d` — fixup: COMMIT_ASYNC deadline reset, dropped piggyback test,
  disconnect-retriability documentation

## Deferrals (documented, legitimate)

- Multi-node / KIP-951-leadership / `LinkedHashSet`-ordering tests and
  `testFetchOneNodeAtATimeForRecordLimitMode` — genuinely multi-node
  (2 brokers, per-node `prepareResponseFrom`, order-sensitive wire assertions);
  the routing + leadership-update production code was read and confirmed faithful.
- `testFetchWithLastRecordMissingFromBatch` — needs `MemoryRecords.filterTo` to
  build the compacted input; the exercised logic lives in `ShareCompletedFetch`.
- Metrics tests — KIP-714.

## Verification

- `cargo build`, `cargo test --lib`, `cargo xtask format-check`,
  `cargo xtask lint` — all green (2219 pass / 1 ignore / 0 fail after fixup).
