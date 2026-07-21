---
name: milestone11-phase6-integration
description: KIP-932 Phase 6 (ShareConsumerImpl + public API) — DONE; only production bg-pipeline + KafkaShareConsumerTest deferred to Phase 7
metadata:
  type: project
---

Milestone 11 Phase 6 (branch `milestone11-share-consumer`): ShareConsumerImpl +
public API + Mock + app-side event wiring. Core is DONE, green, committed.

**DONE + committed (all green, lint-clean):**
- `share_consumer_config.rs` — ShareConsumerConfig (+ test). Rejects
  SHARE_GROUP_UNSUPPORTED_CONFIGS; `from_properties` → Result (illegal_argument == ConfigException).
- `share_consumer.rs` — `ShareConsumer<K,V>` #[async_trait] trait (§2). async
  subscribe/unsubscribe/poll/commit_sync{,_timeout}/commit_async/client_instance_id/
  close{,_timeout}; sync subscription(→Result)/acknowledge{,_with_type,_by_offset}/
  acquisition_lock_timeout_ms(→Result)/set_acknowledgement_commit_callback/wakeup.
  metrics()/register/unregister OMITTED (KIP-714, like Consumer trait).
- `mock_share_consumer.rs` — MockShareConsumer (+ MockShareConsumerTest).
- `share_consumer_impl.rs` — ShareConsumerImpl + `impl ShareConsumer for it`.
  Injectable seams `ShareApplicationEventHandler` + `ShareFetchCollect` (Java's
  mock(ApplicationEventHandler)/mock(ShareFetchCollector)); impl does
  handler.add(event)+await the event's oneshot rx (Java addAndGet+doAnswer).
  §31 handle_completed_acknowledgements drains ack queue at TOP of poll/commit_*/
  close and invokes AcknowledgementCommitCallback INLINE on caller task. Timer
  helper + ShareConsumerTime trait. ShareConsumerImplTest: 23 pass + 1 #[ignore].
- `kafka_share_consumer.rs` — KafkaShareConsumer facade (delegates to
  Box<dyn ShareConsumer>) + `new_share_consumer` factory in mod.rs.
- Event enum: 8 share variants + processor process_share_* arms.
- ApplicationEventProcessor share membership arms reach ShareMembershipManager via
  the new RequestManagers `share_heartbeat` slot (.membership_manager()).

**3 BLOCKERS — ALL RESOLVED:**
1. **Ownership tension** → RESOLVED. Added `in_flight_offsets: BTreeSet<i64>` to
   ShareInFlightBatch: poll MOVES records to the user via take_in_flight_records
   (§27 zero-copy, no per-record clone) while offset-level in-flight tracking
   (acknowledge / acknowledge_all / check_all_in_flight_are_acknowledged /
   take_acknowledged_records / take_renewals / merge) survives the move. Covers
   the non-renewal explicit-ack path (testExplicitModeUnacknowledgedRecords passes).
2. **acknowledge_on_close awaitable** → RESOLVED. ShareConsumeRequestManager
   already held a close_future_rx; exposed via `take_close_future_rx()`; the
   processor's ShareAcknowledgeOnClose arm bridges it to the event handle.
3. **share membership/heartbeat in RequestManagers** → RESOLVED. Added
   `share_heartbeat: Option<ShareHeartbeatRequestManager>` slot (default None),
   iterated in entries() after share_consume; processor SharePoll/
   ShareSubscriptionChange/ShareUnsubscribe arms use it (Optional.empty() branch
   when None).

**DEFERRED to Phase 7 (integration) — stated explicitly:**
- Production bg-pipeline assembly inside `new_share_consumer` (NetworkClient +
  channel builder + share RequestManagers::for_share + ConsumerNetworkThread +
  spawn + production ShareApplicationEventHandler). Same scale as
  AsyncKafkaConsumer::new. Until wired, new_share_consumer returns
  unsupported_version (like new_consumer's classic arm). ShareConsumerImpl is
  assemblable via `from_components`.
- `KafkaShareConsumerTest` (MockClient full-pipeline heartbeat/fetch/ack
  round-trips) — needs the production pipeline; same category as the deferred
  AsyncKafkaConsumer MockClient tests.
- ShareConsumerImplTest: `testExplicitModeRenewAndAcknowledgeOnPoll` (#[ignore]:
  renewal needs the record OBJECT to survive user handoff — Java shares refs;
  Rust zero-copy drain can't without ConsumerRecord: Clone + per-poll allocation,
  rejected per §27). Plus testFailConstructor / testConstructorFailsOnNetworkClient
  / testGroupIdNull / testGroupIdOnlyWhitespaces / testProcessBackgroundEvents*×3 /
  testRecordBackgroundEventQueueSize (production-ctor / KIP-714 metrics /
  mock-future timing — not reachable via the injectable-seam ctor).

**Test double harness pattern** (reuse for Phase 7 / share cousins): TestEventHandler
records added event type_names + completes completable-event handles inline
(subscribe→subscribe_to_share_group+complete; unsubscribe→unsubscribe+complete;
ack-on-close→complete). TestFetchCollector returns queued ShareFetch (move, not
clone) + fires wakeup on configured call indices. MockClock auto-advances +1/read
so poll's Timer expires. Mockito verify() → assert over recorded event list.
