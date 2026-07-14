---
name: milestone9-phase6-integration
description: KIP-932 Phase 6 (ShareConsumerImpl + public API) — what's done, and the 3 concrete blockers gating ShareConsumerImpl/facade
metadata:
  type: project
---

Milestone 9 Phase 6 (branch `milestone9-share-consumer`): ShareConsumerImpl +
public API + Mock + app-side event wiring. Partial delivery — the public-API
scaffolding + event-enum wiring are committed and green; the big integration
piece (ShareConsumerImpl + KafkaShareConsumer facade + factory + its 2 test
files) is NOT yet done.

**DONE + committed (all green, lint-clean):**
- `share_consumer_config.rs` — ShareConsumerConfig newtype over ConsumerConfig,
  rejects SHARE_GROUP_UNSUPPORTED_CONFIGS. `from_properties` returns
  `Result<Self, KafkaError>` (KafkaError::illegal_argument == Java ConfigException).
  ShareConsumerConfigTest translated.
- `share_consumer.rs` — `ShareConsumer<K,V>` `#[async_trait]` trait. async:
  subscribe/unsubscribe/poll/commit_sync/commit_sync_timeout/commit_async/
  client_instance_id/close/close_timeout. sync: subscription (→Result, Java
  throws if closed)/acknowledge{,_with_type,_by_offset}/
  acquisition_lock_timeout_ms (→Result)/set_acknowledgement_commit_callback/
  wakeup. commit_sync returns `HashMap<TopicIdPartition, Option<KafkaError>>`.
  metrics()/register/unregister OMITTED (KIP-714 deferral, like Consumer trait).
- `mock_share_consumer.rs` — MockShareConsumer, add_record/set_client_instance_id
  inherent (not on trait). MockShareConsumerTest translated (ConsumerRecord is
  NOT Clone → assert offset/key/value fields, not whole-record eq).
- ApplicationEvent enum: 8 share variants wrapping the Phase-5 event structs +
  type_name/erased_handle arms; ApplicationEventProcessor process_share_* arms.
  Added into_parts()/into_handle() to the completable share events.

**THREE CONCRETE BLOCKERS for ShareConsumerImpl (resolve before writing poll):**

1. **Ownership tension in `ShareInFlightBatch` (the big one).** Java's
   `poll` returns `fetch.records()` (shared refs) while keeping `currentFetch`
   holding the same records for later `acknowledge(record)`. Rust `ConsumerRecord`
   is NOT Clone. `ShareInFlightBatch` tracks in-flight via `in_flight_records:
   BTreeMap<i64, ConsumerRecord>`; `acknowledge` checks
   `in_flight_records.contains_key(offset)` and
   `check_all_in_flight_are_acknowledged` compares
   `in_flight_records.len() == acknowledged_records.len()`. `take_in_flight_records`
   DRAINS the map. So poll cannot both hand owned records to the user AND keep
   offset tracking. RESOLUTION (sanctioned Phase-6 carryover, Critic flagged in
   Phase 2/3): add `in_flight_offsets: BTreeSet<i64>` to ShareInFlightBatch,
   populated in add_record/add_gap, retained by take_in_flight_records, and make
   acknowledge/acknowledge_all/check_all_in_flight_are_acknowledged/
   take_acknowledged_records key off `in_flight_offsets` (not the record map).
   RE-RUN share_in_flight_batch + share_fetch + share_completed_fetch +
   share_fetch_collector + share_consume_request_manager tests after. Alternatively,
   #[ignore] testExplicitModeUnacknowledgedRecords / testExplicitModeRenewAndAcknowledgeOnPoll
   with this rationale and use take_records() in poll.

2. **`ShareFuture` vs receiver bridging.** `commit_sync` returns
   `oneshot::Receiver<Result<AcknowledgeResult,_>>` (clean to bridge — spawn task
   → complete event handle). BUT `acknowledge_on_close` returns
   `ShareFuture<()> = Arc<CompletableEventHandle<()>>` (owns the Sender, NO
   receiver) — can't be `.await`ed to bridge to the event handle. In the Phase-6
   processor arm I complete the event handle immediately after dispatch
   (documented simplification); full response bridging needs either a receiver
   from acknowledge_on_close or a shared close future the app awaits.

3. **Share membership/heartbeat NOT in RequestManagers.** Phase 5 left only the
   `share_consume` slot. The processor's SharePoll/ShareSubscriptionChange/
   ShareUnsubscribe arms need `shareHeartbeatRequestManager.membershipManager()`.
   Currently they take Java's Optional.empty() branch (no-op / completeExceptionally).
   Full production share consumer needs share_membership + share_heartbeat Arc
   slots added to RequestManagers + entries() + ConsumerNetworkThread polling +
   a `for_share` supplier/ctor.

**ShareConsumerImpl test design (recommended):** the Java ShareConsumerImplTest
uses `mock(ApplicationEventHandler.class)` + `mock(ShareFetchCollector.class)` +
`doAnswer`. Faithful Rust translation = inject a `pub(crate) trait` event-handler
(real ApplicationEventHandler behind it, test double completes handles inline via
the ApplicationEvent variant's handle) + inject the fetch collector behind a
`pub(crate) trait ShareFetchCollect<K,V>` (real ShareFetchCollector + test fake).
The impl always does `handler.add(event)` + awaits the event's oneshot rx itself
(this is exactly Java's addAndGet + doAnswer-completes-future pattern). §31: the
impl drains acknowledgement events + invokes AcknowledgementCommitCallback INLINE
in handle_completed_acknowledgements at the TOP of poll/commit_sync/commit_async/
close (never spawn). §31 regression tests required (callback on caller task;
fires exactly once per commit).

**KafkaShareConsumerTest** (MockClient full-pipeline heartbeat/fetch/ack
round-trips, uses Thread.sleep) → DEFER to Phase 7 (integration), same category
as the deferred AsyncKafkaConsumer MockClient tests; needs full share bg pipeline
(blocker 3) + response routing.

Java source line refs: ShareConsumerImpl.java (1359 lines, poll at 593,
handleCompletedAcknowledgements at 1124, close at 966, processBackgroundEvents at
1229/1285). ApplicationEventProcessor.java share methods at 230/501-604.
