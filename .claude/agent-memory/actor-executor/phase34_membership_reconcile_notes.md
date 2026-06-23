---
name: phase34-membership-reconcile-notes
description: Milestone-8 Phase 34 test-parity — metadata-driven membership reconcile tests; Java-mock-to-real-state translation patterns
metadata:
  type: project
---

Phase 34 closed the ConsumerMembershipManagerTest metadata-driven reconcile gap (commits: reconcile-vs-metadata, metadata-resolution, new-assignment-replaces-waiting, delayed-discard, listener-ordering, leave/fatal matrices). Inline tests in `consumer_membership_manager.rs`.

**Why / context:** existing reconcile tests pre-seeded `assigned_topic_names_cache` to bypass metadata; the metadata-resolution/unresolved/delayed-discard logic was untested. Phase 34 drives REAL metadata.

**How to apply (reusable translation patterns for membership/reconcile test parity):**

- Java mocks SubscriptionState + ConsumerMetadata; Rust tests use REAL objects. Translate `verify(subscriptionState).assignFromSubscribedAwaitingCallback(set, added)` → assert real `subs.assigned_partitions()` + `is_fetchable(tp)`. Translate `when(metadata.topicNames()).thenReturn(map)` → `mgr.abstract_mm.metadata.metadata_arc().update_with_current_request_version(&build_metadata_response(&[(name, uuid)]), false, 1000)`. Translate `verify(metadata).requestUpdate(...)` → `metadata.metadata_arc().update_requested()`.
- `Metadata::update` populates `topic_names()` (id→name) from the response's partition topics REGARDLESS of subscription-based retention (metadata_snapshot.rs ~line 209 adds topic ids from add_partitions unconditionally). So seeding works without subscribing. BUT `is_fetchable` gates on subscription membership (`is_fetchable_and_subscribed`) — to assert fetchability you MUST `subscribe_topics(mgr, &["topic1"])` first (real-state nuance Java's mock hides). When re-subscribing, pass `Some(Arc::new(NoopListener))` to preserve the §31 listener (subscribe_topics with None listener REPLACES it → events would short-circuit).
- §31 collapses Java's two-step (CallbackNeeded event + consumerRebalanceListenerCallbackCompleted). Rust: `tokio::spawn(reconcile)`, drain `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { ack, .. }`, `ack.send(Ok/Err)`. Java `performCallback(complete=false)` then later `completeCallback` → capture the `ack` sender, hold it, send later (this is how "stuck on callback" delayed-discard is driven).
- Rust `leave_group()` is LINEARIZED (no CompletableFuture returned). Java `leaveResult.isDone()` → assert state transitions: leave_group→LEAVING, on_heartbeat_request_generated→UNSUBSCRIBED, leave-response→stays UNSUBSCRIBED.
- JOINING valid-prev-states = [Fenced, Unsubscribed, Stale] only. A direct `transition_to_joining()` from RECONCILING ERRORS. "Rejoin while reconciling" must go via the fence path (FENCED→JOINING), which sets `rejoined_while_reconciliation_in_progress=true` (abstract_membership_manager.rs transition_to_joining). That flag → `maybe_abort_reconciliation()` discards the stale in-flight reconcile (mutation-resistance target: deleting the guard makes delayed-discard tests reach ACKNOWLEDGING and fail).
- "topics awaiting reconciliation" has no production accessor; compute in test = target topic ids (`inner.current_target_assignment.partitions.keys()`) minus ids resolvable from metadata/cache. "topic_partitions_awaiting_reconciliation" = target minus current_assignment per topic.
- KafkaError ctors: `wakeup(msg)`, `timeout(msg)`, `illegal_argument(msg)`, `illegal_state(msg)` all take a message arg; NO `interrupt`. `LocalAssignment.partitions` is `HashMap<Uuid, Vec<i32>>` (sort before asserting equality — order not guaranteed).
- not-in-group states for ignore-heartbeat test: force via `inner.lock().state = X` (bypasses transition validity).

**Deferred:** STALE-path tests (testTransitionToLeaving*DueToStaleMember, testStaleMember*, testLeaveGroupWhenMemberIsStale) → Phase 5 (poll-timer-expiry). **Skipped:** all RebalanceMetrics tests (no metrics framework), testPollMustCallsMaybeReconcileWithFalse (Mockito-verify, REDUCED-covered).
