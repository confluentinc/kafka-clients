---
name: milestone9-phase5-share-consume-request-manager
description: KIP-932 Phase 5 — ShareConsumeRequestManager + share events translation decisions, response-handler wiring, and test-harness gotchas
metadata:
  type: project
---

Milestone 9 Phase 5 (branch `milestone9-share-consumer`): translated
`ShareConsumeRequestManager` (~1571 Java lines) + 10 share event files +
`ShareConsumeRequestManagerTest`. Files:
`src/consumer/internals/share_consume_request_manager.rs`,
`src/consumer/internals/events/share_*.rs`.

**Response-handler wiring (the crux):** Java registers a `whenComplete` lambda
that mutates the manager on the network thread. `RequestManager::poll` takes
`&mut self`, so the completion closure can't hold a second borrow. Solution:
the manager exposes `handle_share_{fetch,acknowledge}_{success,failure}` as
`&mut self` methods; the bg task (Phase 6/7) awaits each request's response and
dispatches into them serially (mirrors `fetch_request_manager` completion
routing). Tests drive them DIRECTLY (no real NetworkClientDelegate) — the
`fetch_request_manager` round-trip precedent (phase37).

**Per-node in-flight ack routing WITHOUT a slot map:** only one acknowledge
request per node is in flight (guarded by `nodes_with_pending_requests`), so the
response routes to the per-node `Tuple`'s state with non-empty
`in_flight_acknowledgements` (async/sync-queue) or the un-`is_processed` close
request. See `find_in_flight_ack_slot`/`ack_slot_state_mut`. No `Rc<RefCell>`
needed — everything is owned `&mut self`; disjoint-field destructures +
`AckBuildCtx<'a>` bundle the borrows for `maybe_build_request`/`build_ack_request`.

**ResultHandler shared across per-node states** via `Arc<ResultHandler>` with
interior `Mutex`/`AtomicI32`. `is_ack_callback_registered` is a shared
`Arc<AtomicBool>` (read live at event-send time — testCallbackHandlerConfig
toggles it). `maybe_send_share_acknowledgement_event` is a FREE fn taking
`&AtomicBool` + `&ShareAcknowledgementEventHandler` so both manager and
ResultHandler call it without a back-reference. `resultCount` lives inside
ResultHandler; manager increments via `increment_remaining()`.

**Time source:** added a `ShareConsumeTime` trait (like `FetchCollectorTime`/
`ThreadTime`) — commit_sync/async/acknowledge_on_close need `now` for
`AcknowledgeRequestState::new` (Java uses `this.time`); poll gets `now` as a param.

**close/commit futures:** `ShareFuture<T> = Arc<CompletableEventHandle<T>>`
(has `is_done`/`complete`). `commit_sync` returns the `oneshot::Receiver`;
`acknowledge_on_close` returns the shared close future. `IdempotentCloser` →
inline `closed: bool` flag.

**Events (§28):** each in its own file, `pub(crate)`. Completable ones
(`ShareSubscriptionChange`/`ShareUnsubscribe`/`ShareAcknowledgeSync`/
`ShareAcknowledgeOnClose`) carry `CompletableEventHandle<T>` and `new(...)`
returns `(Self, receiver)`. Bare ones (`ShareFetch`/`SharePoll`/
`ShareAcknowledgeAsync`/`...CallbackRegistration`) are plain structs.
`ShareAcknowledgementEventHandler` wraps `Arc<Mutex<VecDeque>>` (Clone shares
the queue). App-side enum integration deferred to Phase 6 (ApplicationEvent
enum still excludes share) — documented per §28 deferral allowance.

**RequestManagers registration:** added `share_consume: Option<ShareConsumeRequestManager>`
slot, iterated last in `entries()` (Java RequestManagers.java:127); existing
`new`/`with_dyn_managers` init it `None`. Full share bg-loop wiring (share
heartbeat/membership entries + `for_share` ctor) deferred to Phase 6.

**Metrics:** ALL `ShareFetchMetricsManager`/aggregator recording omitted with
`// metrics: deferred to KIP-714`; logic preserved. Metric-assertion tests
skipped accordingly.

**Test harness (share_consume_request_manager.rs `mod tests`, 49 pass + 1 ignore):**
- `MockClock` AUTO-TICKS +1 per `milliseconds()` read (mirrors Java
  `MockTime(1)`) — needed to clear exact backoff boundaries. But the Rust
  harness makes FEWER clock reads than Java, so backoff-boundary sleeps must be
  1.5x `retryBackoffMs` (jitter reaches 1.2x) — testCallbackHandlerConfig &
  testServerDisconnected were flaky at exactly 1x.
- Collector holds `Arc<ConsumerMetadata>` (Phase-3 deviation) SEPARATE from the
  manager's `Arc<ShareConsumerMetadata>`, both over the SAME subscriptions.
  Partition-level fetch errors requestUpdate on the COLLECTOR metadata; top-level
  on the MANAGER metadata → test helper `update_requested()` ORs both.
- `metadata_expire_ms` must be FINITE (used 300_000) — `i64::MAX` overflows
  `Metadata::time_to_next_update` (`last_successful + expire - now`) where Java
  `long` wraps silently.
- Draining harness wrappers `commit_async/commit_sync/acknowledge_on_close/fetch`
  drain ack events immediately after (Java's Testable handler records on `add`;
  leadership-change failures fire BEFORE any send).
- Disconnect → `Errors::NetworkException` (Rust has no `DisconnectException`);
  Java's ack-path `Errors.forException(DisconnectException)`==UnknownServer is a
  Java artifact — Rust asserts NetworkException uniformly (documented).
- `full_fetch_response`/`ShareFetchResponse::of(error, throttle, Vec<(tip,PartitionData)>, &[Node], acq_timeout)`;
  CRC corruption via flipping buf[32..34] on a records buffer.

**DEFERRED tests (with rationale, for follow-up):**
- `testFetchWithLastRecordMissingFromBatch` — needs `MemoryRecords.filterTo`
  (compaction) not translated.
- `testCloseInternalClosesShareFetchMetricsManager` — pure metrics (KIP-714).
- `testShareFetchWithSubscriptionChangeMultipleNodes` + `...EmptyAcknowledgements`,
  the 3 KIP-951 currentLeader param tests
  (`testWhen{ShareFetchResponse,FetchResponse,LeadershipChangeBetween}...`),
  `testWhenLeadershipChangedAfterDisconnected` — multi-node wire-field assertions
  + per-partition currentLeader responses that are order-sensitive; the harness
  lacks Java's deterministic `LinkedHashSet` partition order. Same reason
  `testFetchOneNodeAtATimeForRecordLimitMode` is `#[ignore]`.
