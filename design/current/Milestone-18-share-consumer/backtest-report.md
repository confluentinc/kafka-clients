# Critic 202 — R1–R8 backtest report

Reviewer: Critic 202. Lens: `translation-checklist.md` R1–R8, applied out of scope (to
consumer files) on purpose. Java reference: Kafka 4.4.0 under `kafka/`. Each snapshot
was reviewed on its own; I did not read commit messages, diffs or audit files.

Severity scale: Bug > Behavior Mismatch > Missing Requirement (test) > Design Flaw.

---

## S1 — `d76ddeecd^` (fetch_session_handler.rs, fetch_collector.rs, metadata.rs)

### S1-1 — `swap_remove` on the builder's `next` map reorders it and can drop new partitions from the session
- **Rule(s):** R5 (`LinkedHashMap#remove` → `shift_remove`, never `swap_remove`)
- **File / fn:** `rust/src/fetch_session_handler.rs`, `FetchSessionHandler::build_request` (incremental branch, `builder.next.swap_remove(&topic_partition)`)
- **Claim:** Java's `FetchSessionHandler.Builder#build` calls `next.remove(topicPartition)` on a `LinkedHashMap`, which keeps the order of the other entries. Touched (altered or replaced) partitions are then re-added at the end of `next`. The "add new partitions" loop depends on that layout: it `break`s at the first key that is already in `sessionPartitions` ("once we hit one of them, we know there are no more unseen entries"). `IndexMap::swap_remove` instead moves the **last** entry into the removed slot. A touched partition that was re-appended at the end can be moved in front of not-yet-seen new partitions, so the `break` fires early. Those new partitions are then never inserted into `session_partitions` or `added`, although they are still sent in `to_send`.
  Concrete trace: the session is `[A, B]` and the builder is `[A', N, B]` (A altered, N new, B unchanged). Processing A: `swap_remove(A)` gives `[B, N]`, and re-appending A' gives `[B, N, A']`. Processing B: `swap_remove(B)` moves A' into slot 0, giving `[A', N]`. B is unchanged, so it is not re-added. In the new-partition loop, A' is already in the session, so the loop breaks. **N is never added to the session.** Java ends with session `[A', B, N]` and `toSend` `[N, A']`. Rust ends with session `[A', B]` and `to_send` `[A', N]`.
  Effects: (1) the session is missing N, so the next incremental request treats N as brand new and sends it again; the response then carries N, `verify_incremental_fetch_response_partitions` reports it as an `extraPartitions` error, and the session is closed and re-created, in a loop. (2) The `to_send` wire order differs from Java's, and the broker trims a response from the end of that order when it hits the response size limit.
  The comment at the `break` says the opposite ("swap_remove above ensured those keys are gone from next"), and so does the `new_builder_sized` doc.
- **Java evidence:** `FetchSessionHandler.Builder#build` (lines 300–346: `next.remove`, `next.put` re-append, `break` at the first known key).
- **Confidence:** high. **Severity:** Bug.

### S1-2 — `test_incrementals` / `test_incremental_partition_removal` dropped Java's ordered-map assertions
- **Rule(s):** R1 (assertion fidelity), R5 (tests use non-trivial orders)
- **File / fn:** `rust/src/fetch_session_handler.rs` tests `test_incrementals`, `test_incremental_partition_removal`
- **Claim:** Java uses `assertMapsEqual` / `assertMapEquals`, which walk both maps in step and check the key **and position** of each entry. For example, `data2.toSend()` must be exactly `[bar0, foo1]` and `data2.sessionPartitions()` exactly `[foo0, foo1, bar0]`. The Rust tests check only `len()` and `contains_key`, so they cannot catch S1-1 or any other ordering regression. The PartitionData payload checks on `to_send` / `session_partitions` are gone too. (`testDoubleBuild` is not translated, which is fine because `build_request` consumes the builder at compile time; but nothing at the test notes the skip.) A test with a touched partition **before** a new partition in builder order (see the S1-1 trace) would expose the bug.
- **Java evidence:** `FetchSessionHandlerTest#testIncrementals`, `#testIncrementalPartitionRemoval`, helper `assertMapEquals`.
- **Confidence:** high. **Severity:** Missing Requirement (test).

### S1-3 — `test_fetch_with_metadata_refresh_errors` drops `assertTrue(metadata.updateRequested())`
- **Rule(s):** R1
- **File / fn:** `rust/src/consumer/internals/fetch_collector.rs` test `test_fetch_with_metadata_refresh_errors`
- **Claim:** Java asserts three things for each error: `fetch.isEmpty()`, the preferred read replica is cleared, and `metadata.updateRequested()` is true. The Rust test checks only the first two, so the actual metadata-refresh side effect of `requestMetadataUpdate`, which gives the test its name, is never checked.
- **Java evidence:** `FetchCollectorTest#testFetchWithMetadataRefreshErrors`.
- **Confidence:** high. **Severity:** Missing Requirement (test).

### S1-4 — `test_fetch_normal` drops several Java assertions
- **Rule(s):** R1
- **File / fn:** `fetch_collector.rs` test `test_fetch_normal`
- **Claim:** The Rust test is missing these Java assertions:
  - `assertFalse(completedFetch.isInitialized())` before the collect, and `assertTrue(...isInitialized())` after it;
  - `assertFalse(completedFetch.isConsumed())` after the first collect;
  - on the second collect, `assertEquals(1, fetch.nextOffsets().size())` plus the expected `OffsetAndMetadata` (Java still returns the next offset on an empty second fetch);
  - the final `assertTrue(completedFetch.isConsumed())`.

  In place of the last one there is a comment, "After the second poll, the next-in-line is drained.", with no assertion behind it.
- **Java evidence:** `FetchCollectorTest#testFetchNormal`.
- **Confidence:** high. **Severity:** Missing Requirement (test).

### S1-5 — Leftover diagnostic: INFO log on every `CompletedFetch` initialize
- **Rule(s):** none (ordinary review). Arguably R2: a diagnostic left in place is FIXME-like.
- **File / fn:** `fetch_collector.rs`, `FetchCollector::initialize`: `// DIAGNOSTIC (fetch_diag)` followed by `log::info!(target: "fetch_diag", "app first-touch handoff_ms=...")`, driven by `CompletedFetch::created_at`.
- **Claim:** Java has no counterpart to this log. **Correction (made after checking `completed_fetch.rs` at the S5 rev):** `created_at` is set only when `log_enabled!(target: "fetch_diag", Info)` is true, so the log is opt-in through its own target and costs nothing when that target is off. It still fires once per partition per fetch whenever the global level is INFO with no per-target filter (for example `RUST_LOG=info`). My first draft said "floods logs at the default level", which overstated it.
- **Java evidence:** `FetchCollector#initialize` (no such log).
- **Confidence:** low. This is probably an intentional diagnostic, so treat it as a likely false positive. **Severity:** Design Flaw (minor).

### S1-6 — Lock-order inversion between SubscriptionState and Metadata (ABBA deadlock) — **missed blind, found late**
- **Rule(s):** R4.5 (map every nested lock path where a Java monitor becomes a non-reentrant `std::sync::Mutex`)
- **File / fn:** `fetch_collector.rs` `FetchCollector::update_partition_state` (holds the `SubscriptionState` guard, then calls `self.metadata.metadata_arc().metadata_expire_ms()`); `metadata.rs` `Metadata::metadata_expire_ms` (takes `self.inner.lock()`); `metadata.rs` `Metadata::update` (holds `inner` and calls the `retain_topic_fn` closures, which `ConsumerMetadata` implements by locking `SubscriptionState`).
- **Claim:** The app task locks subscriptions and then Metadata (`update_partition_state`, on the preferred-read-replica branch). The bg task locks Metadata and then subscriptions (`Metadata::update` → `retain`, called from `handle_metadata_response` and from `last_seen_leader_epochs.retain`). Both are blocking `std::sync::Mutex`es held on different OS threads, so a fetch response that carries a preferred read replica (KIP-392 rack-aware fetching), racing a metadata update, deadlocks both tasks for good. Java has no such cycle, because `Metadata#metadataExpireMs` is a plain getter of a `final` field with no `synchronized`.
- **Java evidence:** `Metadata#metadataExpireMs` (not synchronized); `FetchCollector#updatePartitionState` (calls it from inside the `tryUpdatingPreferredReadReplica` supplier); `ConsumerMetadata#retainTopic` (synchronized, calls into `SubscriptionState`).
- **How it was found:** I did **not** find this in the blind S1 pass, although I opened `update_partition_state` and R4.5 names exactly this check. I found it only after seeing the S3 test name `preferred_read_replica_update_does_not_take_the_metadata_lock`. I list it here so the backtest counts it as a miss, not a hit.
- **Confidence:** high. **Severity:** Bug.

### S1 — looked at, not reported
- `metadata.rs` `time_to_allow_update` / `time_to_next_update` / `update` / `handle_metadata_response` / `update_latest_metadata` match `Metadata` 4.4 branch for branch. `request_update(false)` is inlined as `need_full_update = true`, which is equivalent.
- `metadata.rs` `update` panics (`assert!`) after close where Java throws `IllegalStateException`. The only production caller is `NetworkClient`, which also owns the `close()`, so the panic is not reachable. The test is translated as `should_panic`. Not reported.
- `await_update` uses `Duration::from_millis(timeout_ms as u64)`. That is an R5 "`as` wrap" candidate, but the method is producer-only and was not in this review's scope.
- `fetch_collector.rs`: the deferred-position design (`move_positions` / `CollectState`), and the `MissingPosition` → skip divergence, are documented deviations with a *Why*. Not reported (R7 "does not apply to documented deviations").

---

## S2 — `cc2946be8^` (consumer_membership_manager.rs, application_event_processor.rs, consumer_network_thread.rs)

### S2-1 — Rebalance-callback partitions reach the listener in `HashSet` order; Java always passes a `TreeSet` (TOPIC_PARTITION_COMPARATOR)
- **Rule(s):** R5 (`SortedSet` → sorted; "a sorted output stays sorted"; tests use non-trivial orders)
- **File / fn:** `consumer_membership_manager.rs`:
  - `reconcile`: `added` and `revoked` are `HashSet`s;
  - `continue_after_commit`: `revoked.iter().cloned().collect()`;
  - `continue_after_revoke`: `added.iter().cloned().collect()`;
  - `transition_to_fatal` / `transition_to_fenced` / stale: `subs.assigned_partitions().into_iter().collect()`;
  - `signal_member_leaving_group`: same pattern.

  `abstract_membership_manager.rs` `enqueue_rebalance_callback*` / `enqueue_partitions_assigned_event` forward the `Vec` unchanged.
- **Claim:** Java builds every list a listener receives as a `SortedSet`:
  - `maybeReconcile` builds `addedPartitions` / `revokedPartitions` as `new TreeSet<>(TOPIC_PARTITION_COMPARATOR)`;
  - `enqueueConsumerRebalanceListenerCallback` re-sorts into a `TreeSet` for revoked and lost;
  - `invokeOnPartitionsRevokedOrLostToReleaseAssignment` builds `droppedPartitions` as a `TreeSet`;
  - `PartitionsAssignedEvent` carries the `SortedSet addedPartitions`.

  So `onPartitionsRevoked`, `onPartitionsAssigned` and `onPartitionsLost` always see topic-then-partition order in Java, and HashSet-random order in Rust. Listeners that log, commit or build state per partition in iteration order behave differently, and the order is not even stable from run to run. The tests hide this: `expect_callback` collects both sides into a `HashSet` before comparing.
- **Java evidence:** `AbstractMembershipManager#maybeReconcile`; `ConsumerMembershipManager#enqueueConsumerRebalanceListenerCallback`, `#invokeOnPartitionsRevokedOrLostToReleaseAssignment`, `#enqueuePartitionsAssignedEvent`.
- **Confidence:** high. **Severity:** Behavior Mismatch.

### S2-2 — `AsyncPoll` runs `maybeReconcile(true)` / `updateTimerAndMaybeCommit` / `resetPollTimer` in a spawned task, after this iteration's request-manager polls instead of before them
- **Rule(s):** R4 items 1, 3 and 4 (moving a synchronous Java step behind a `spawn` changes when it takes effect)
- **File / fn:** `events/application_event_processor.rs`, `process_async_poll` (the whole body runs inside `tokio::spawn`); `consumer_network_thread.rs` `run_once`.
- **Claim:** In Java, `ApplicationEventProcessor#process(AsyncPollEvent)` runs these steps synchronously, inside `processApplicationEvents()`, which is Phase 1 of `runOnce`: `maybeReconcile(true)`, `markReconciliationCheckComplete`, `updateTimerAndMaybeCommit(pollTimeMs)`, `onConsumerPoll`, `resetPollTimer(pollTimeMs)`, and the `updateFetchPositions` call. Only the `whenComplete` continuations are deferred.

  Rust wraps all of it in `tokio::spawn`. Production runs the bg loop on a dedicated `current_thread` runtime (`spawn_dedicated`). The spawned task can therefore run only when `run_once` actually yields, and the first real suspension is the Phase-4 network poll. By then Phase 2 has already polled the coordinator, commit and heartbeat managers, and Phase 2.5 has run `reconcile(now, false)`. As a result:
  - (a) the auto-commit that `update_timer_and_maybe_commit` queues is not drained by `commit.poll_with_coordinator` until the next iteration;
  - (b) the poll-timer reset and the `maybe_rejoin_stale_member` check land after the heartbeat manager's poll-timer-expiry check for this iteration;
  - (c) `reconcile(true)` runs after `reconcile(false)`, not before it.

  The only wake in the spawned body is `fetch_mgr.wake_background_task()`, after `update_fetch_positions` resolves, so (a) depends on that. Now that `reconcile` is non-blocking (Phase 41), the Java-synchronous prefix could run inline in `process`, with only the `update_rx` / `fetch_rx` continuations spawned. I checked a related worry and ruled it out: concurrent `reconcile` calls racing the check-then-mark of `reconciliation_in_progress` (C2-05 pattern) **cannot** happen in production. The runtime is single-threaded and `reconcile` has no real suspension point.
- **Java evidence:** `ApplicationEventProcessor#process(AsyncPollEvent)` (lines 728–770); `ConsumerNetworkThread#runOnce` (events are processed before `requestManagers.entries()` is polled).
- **Confidence:** medium (the ordering is certain; the user-visible impact is small, mostly one iteration of latency on auto-commit and poll-timer handling). **Severity:** Behavior Mismatch.

### S2-3 — Leaving through `unsubscribe()` does not mark the revoked partitions pending revocation before `onPartitionsRevoked`
- **Rule(s):** R7 (checked the Java body, not the name); arguably R4.4 (a collapsed `revokePartitions` step)
- **File / fn:** `consumer_membership_manager.rs`, `signal_member_leaving_group`
- **Claim:** For a member with epoch > 0, Java's `invokeOnPartitionsRevokedOrLostToReleaseAssignment` calls `revokePartitions(droppedPartitions)`. That method runs `signalPartitionsBeingRevoked`, which logs paused partitions whose pause flag will be lost, then `markPendingRevocationToPauseFetching(revokedPartitions)`, then the FATAL check, and only then enqueues the callback. Rust goes straight to `enqueue_rebalance_callback_with_ack_through_application_events`. So while the user's `onPartitionsRevoked` runs during `unsubscribe()`, the partitions are still fetchable. The fetcher keeps sending fetches and buffering responses for partitions that are being revoked, which is exactly what Java's comment on `markPendingRevocationToPauseFetching` exists to prevent. The paused-partitions INFO log is missing too. The reconcile path does mark the partitions, at step 8, so only the leave path is affected.
- **Java evidence:** `ConsumerMembershipManager#invokeOnPartitionsRevokedOrLostToReleaseAssignment` → `AbstractMembershipManager#revokePartitions` (lines 1191–1232), `ConsumerMembershipManager#signalPartitionsBeingRevoked`.
- **Confidence:** medium-high. **Severity:** Behavior Mismatch.

### S2-4 — `assignPartitions` prunes the topic-name cache at enqueue time in Java; Rust prunes only after a successful `onPartitionsAssigned` ack
- **Rule(s):** R4.4 (a collapsed `CompletableFuture` chain changes when a step runs)
- **File / fn:** `consumer_membership_manager.rs`, `continue_after_assign`, step 15
- **Claim:** Java's `AbstractMembershipManager#assignPartitions` runs `assignedTopicNamesCache.values().retainAll(assignedTopics)` synchronously, right after `signalPartitionsAssigned` enqueues the event, and regardless of how the callback ends. Rust runs it in `continue_after_assign` after the ack, and the callback-error branch returns before it. So it is skipped entirely when `onPartitionsAssigned` fails. The stale cache entries then survive. They are what `revoke_partitions_uses_topic_names_local_cache_when_metadata_not_available` relies on, so a later revocation can resolve a topic that Java would already have dropped.
- **Java evidence:** `AbstractMembershipManager#assignPartitions` (lines 1248–1280).
- **Confidence:** high on the divergence, low on impact. **Severity:** Behavior Mismatch (minor).

### S2 — looked at, not reported
- **AEP per-event `tokio::spawn` continuations (CommitAsync / CommitSync / …).** R4.2 ("no unordered per-callback spawn") flags these at first sight. But `AsyncKafkaConsumer::register_async_commit` chains every continuation on its predecessor before enqueueing the callback, so commit-callback order is restored downstream. Not a bug; see the over-application section.
- **Racing `reconcile` (bg Phase 2.5 vs the spawned AsyncPoll task).** Real on a multi-thread runtime. Not reachable in production, which uses a `current_thread` runtime and a `reconcile` with no real suspension point. Not reported.
- **`cleanup` order vs `ConsumerNetworkThread#cleanup`.** It matches: timer, `runAtClose` under a catch, `sendUnsentRequests` do-while, reap, close managers, close the delegate.
- **Leave-path tests.** They keep Java's `assertFalse(leaveResult.isDone())` checks (`assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave`), so there is no C2-01-style drop.
- **`AfterRevoke` error returns `Err` up into AsyncPoll.** Java does not fail the AsyncPollEvent on a listener error, but the app side rethrows the listener error from `processBackgroundEvents` anyway. Not clearly a divergence, so not reported. **Near-miss, recognised after S4.** In `process_async_poll`, `reconcile(...)` returning `Err` calls `state.complete_with_error(err); return;`. That (a) reports the listener error a second time and (b) skips that poll's `update_timer_and_maybe_commit`, `on_consumer_poll`, poll-timer reset, `update_fetch_positions` and `create_fetch_requests`. Java's `maybeReconcile(true)` is `void` and none of that is skipped. I saw the code path and dismissed it on the "the error reaches the user anyway" reasoning; S4 fixes exactly this. R4.4 ("do not collapse a CompletableFuture chain…") would have caught it if read as "an error inside a chain must not escape into a sibling event". I count this as a miss.

---

## S3 — `d76ddeecd` (same paths as S1)

### S3 status of S1 findings (re-checked independently on this snapshot)
- **S1-1 (`swap_remove`): fixed.** `build_request` now walks `session_partitions` with `retain`, collects touched entries in session order, keeps only brand-new keys in `next` with an order-preserving `retain`, adds them to the session, then appends the touched entries. I traced it by hand against `FetchSessionHandler.Builder#build` for the three new test layouts (`[unchanged,new,altered]`, `[new,altered,unchanged]`, removal + add) and the replaced-id case. `to_send` and the session order match Java in each. The O(n) note is a fair adaptation: same result, not `shift_remove` per key.
- **S1-6 (lock inversion): fixed.** `Metadata::metadata_expire_ms` now reads a plain `i64` field without locking, as Java's `final` field does. `preferred_read_replica_update_does_not_take_the_metadata_lock` pins this, and has a self-check that the metadata lock really is held while the update is parked.
- **S1-2 still open (partly mitigated).** `test_incrementals` and `test_incremental_partition_removal` still assert only `len()`/`contains_key` where Java asserts ordered map equality. The three new Rust-only tests cover the order, so the gap is now R1 fidelity only, not missing coverage.
- **S1-3, S1-4 still open:** `test_fetch_with_metadata_refresh_errors` still lacks `metadata.updateRequested()`, and `test_fetch_normal` still lacks the `isInitialized`/`isConsumed`/second-`nextOffsets` assertions.
- **S1-5 unchanged** (the opt-in diagnostic; low-confidence finding).

### S3-1 — New Rust-only tests cite "audit finding C3-01" in place of a Java test
- **Rule(s):** R1 / R8 (boundary)
- **Claim:** `test_incremental_add_after_altered_partition_keeps_new_partition_in_session` and `test_incremental_to_send_order_matches_java` are Rust-only. They derive their expected values by hand from Java's `Builder#build` and say so in a doc comment. That is the right shape. Not a defect: recorded only so the rule authors can see the case. R1 does not cover these tests and R8 does not apply (no Rust-only *type*), so nothing in R1–R8 asks a reviewer to check the hand-derived expectations. I checked them; they are correct.
- **Confidence:** n/a. **Severity:** none (observation).

No new defects found in S3.

---

## S4 — `cc2946be8` (same paths as S2)

### S4 status of S2 findings
- **S2-2 partly fixed.** Step 1 (`maybeReconcile(true)` + `markReconciliationCheckComplete`) now runs inline on the bg task during the Phase-1 drain, and `reconcile` is a sync `fn`. The comment cites the C2-05 double-start race as the reason. **Steps 2–3 are still inside `tokio::spawn`:** `update_timer_and_maybe_commit(poll_time_ms)`, `on_consumer_poll`, the `poll_timer_is_expired` → `maybe_rejoin_stale_member` check and `reset_poll_timer`. The doc says "Steps 2-4 run on a spawned task because step 3 awaits the `updateFetchPositions` result". But Java runs steps 2–3 synchronously, and only the continuations after `updateFetchPositions` are asynchronous. So the residual S2-2 ordering point stands: on the `current_thread` runtime these run after this iteration's commit and heartbeat polls. **Severity:** Behavior Mismatch (minor). **Confidence:** medium.
- **The S2 near-miss (a reconcile error failing AsyncPoll) is fixed.** `log_async_poll_reconcile_error` logs at DEBUG and carries on, matching `void maybeReconcile`. There is also a second `reconcile` call when the first only resumed a pending leg; this approximates Java's ordering of the callback-completed event ahead of the AsyncPollEvent. I checked that it cannot double-start: the second call only runs when no leg is pending, and the in-progress check still guards it.
- **S2-1 (unsorted callback partitions): still present.** `added`/`revoked` are still `HashSet`s and `signal_member_leaving_group` / the release transitions still `collect()` from `assigned_partitions()`. No sorting at rev `cc2946be8` in `abstract_membership_manager.rs` either.
- **S2-3 (leave path does not mark pending revocation): still present** (`signal_member_leaving_group` unchanged).
- **S2-4 (topic-name cache pruned only after a successful assign ack): still present** (`continue_after_assign` still returns early on the callback error).

No new defects found in S4.

---

## S5 — `150c54b97` (`completed_fetch.rs`, per-record hot path)

### S5-1 — `test_aborted_transaction_records_removed_direct` / `test_committed_transaction_records_included_direct` swap Java's input, and the deviation note justifying it is false
- **Rule(s):** R1 (input swapped), R7 (the stated claim was checked and is wrong)
- **File / fn:** `completed_fetch.rs` tests `test_aborted_transaction_records_removed_direct`, `test_committed_transaction_records_included_direct`
- **Claim:** Java's `newTransactionalRecords(ControlRecordType.ABORT|COMMIT, n)` writes a data batch **followed by an `EndTransactionMarker` control batch**. The Rust tests leave the marker out. Their deviation note says "The Rust `CompletedFetch` does not translate `ControlRecordType` / `containsAbortMarker` (see module docstring) and rejects a control batch from a previously-aborted producer with `UnsupportedVersion`." In this snapshot that is false. The module docstring says the opposite ("Fully translated as of Milestone 11 Phase 8 … `containsAbortMarker` is `CompletedFetch::contains_abort_marker`"), and `load_next_batch` parses the marker through `ControlRecordType` and clears the producer id. So the translated tests never exercise the marker path that Java's versions do: data batch, ABORT marker, `abortedProducerIds.remove`. Other Rust-only tests may cover it, but the translated test should now use Java's input and drop the stale note.
- **Java evidence:** `CompletedFetchTest#testAbortedTransactionRecordsRemoved`, `#testCommittedTransactionRecordsIncluded`, `#newTransactionalRecords`; `CompletedFetch#containsAbortMarker`.
- **Confidence:** high. **Severity:** Missing Requirement (test).

### S5-2 — `testCorruptedMessage` port weakens the structured-field assertions
- **Rule(s):** R1 (widened accepted outcomes)
- **File / fn:** `completed_fetch.rs` tests `test_corrupted_message_key_fails_after_valid_record`, `test_corrupted_message_value_fails_after_valid_record`
- **Claim:** Java asserts exact values on `RecordDeserializationException`: `timestamp()==0` (KEY) and `==10L` (VALUE), `keyBuffer()` bytes `== "key"`, `valueBuffer()` bytes `== "value"` / `"otherValue"`, `headers().toArray().length == 0` (KEY), and `headers` equal to `{hkey: hvalue}` (VALUE). The Rust KEY test asserts `timestamp() >= 0`, `key_buffer().is_some()`, `value_buffer().is_some()`, `headers().is_some()`. The VALUE test checks only the message, and never builds a record with headers or a non-zero timestamp. So a bug that put the wrong record's bytes, the batch timestamp, or dropped headers into the error would pass. Swapping the UUID deserializer for a `MaybeFailingDeserializer` is a reasonable adaptation, since Rust has no `UUIDDeserializer`. But nothing stops the fixture from building Java's exact records (timestamp 10, headers `hkey`) and asserting exact bytes.
- **Java evidence:** `CompletedFetchTest#testCorruptedMessage`.
- **Confidence:** high. **Severity:** Missing Requirement (test).

### S5 — looked at, not reported
- **`aborted_transactions: BinaryHeap<AbortedTxnByFirstOffset>`** with a reversed `Ord` is a faithful `PriorityQueue` with `comparingLong(firstOffset)`. **`aborted_producer_ids: FxHashSet`** stands in for Java's `HashSet`; order is irrelevant, and R5 explicitly says not to swap it for an ordered set.
- **`load_next_batch` / `advance_to_next_fetched_record`** match `CompletedFetch#nextFetchedRecord`: the `lastEpoch` update per batch, ABORT-marker-before-`isBatchAborted` order, `isTransactional` gate, record-level control skip with `offset + 1`, and `currentBatch` tracking through skipped batches.
- **Per-record `.await`** on `DynDeserializer` returns `InlineFuture` (no allocation per call), so it is not a CLAUDE.md §13 `Pin<Box<dyn Future>>`-per-record violation.
- **`contains_abort_marker`, borrowed path:** it reads the first record when `records_count == 0` but bytes remain, where Java's iterator returns `false`. This only matters for a malformed control batch the broker never writes, so I treated it as theoretical and did not report it.

---

## Over-application self-check

These are the points where reading a rule literally pushed me toward a change I believe is wrong or out of scope. I did not report any of them as findings.

1. **R4.6 (yield in loops) vs `CompletedFetch::fetch_records_loop`.** The loop awaits a deserializer per record, and with a ready deserializer it never yields. "A loop that can spin without awaiting I/O must yield, tested on a `current_thread` runtime" reads straight onto it. The rule's own carve-out ("never add `yield_now()` to per-record decode") saves it, but only because the carve-out exists. The loop is bounded by `max.poll.records`, so it is not a thread loop.
2. **R4.6 vs `ConsumerNetworkThread::run`.** It is a `while running { run_once().await }` thread loop, and the literal rule asks for a `current_thread` spin test. Every iteration awaits the selector poll, and there is already a yield after a caught panic. Demanding a new `current_thread` test here would duplicate `run_once_does_not_spin_while_bootstrap_resolution_is_pending`. Item 6 needs a clearer "unless every iteration provably awaits I/O" clause.
3. **R4.2 ("no unordered per-callback spawn") vs the AEP's per-event `tokio::spawn` continuations** (CommitAsync/CommitSync/FetchCommittedOffsets/…). Read literally, it flags every arm. But `AsyncKafkaConsumer::register_async_commit` chains each continuation on its predecessor, so callback order is restored downstream. Reporting the spawns would be a false positive. The rule should say "check where the order is observed", not "no spawn".
4. **R4 (racing continuations, C2-05) vs S2's two `reconcile` callers.** I first wrote this up as a race, and it is real on a multi-thread runtime. I withdrew it after checking that production pins the bg loop to a dedicated `current_thread` runtime and that `reconcile` has no real suspension point. A rule-driven reviewer who never checks the runtime flavour would report a false positive. S4's authors moved the call anyway, with the same race as the rationale, so the change is defensible hardening, but the S2 code was not a production bug. R4 should tell reviewers to establish the runtime flavour first.
5. **R5 (`HashMap` → keep `HashMap`) vs `FxHashSet` / `HashSet` in `CompletedFetch` and the membership manager.** The table rightly says not to swap these for ordered maps. But the S2-1 *sorted output* rule sits next to it, and it is easy to over-apply to every `HashSet` in the membership manager (`owned_set`, `assigned_set`). Only the collections handed to a listener, plus `LocalAssignment`'s per-topic `SortedSet<Integer>`, need ordering.
6. **R5 "saturating arithmetic where Java clamps" vs `Metadata::time_to_allow_update` / `time_to_next_update`.** These add `last_refresh_ms + backoff` and `last_successful_refresh_ms + metadata_expire_ms` without saturation. Java does not clamp there (plain `long` arithmetic, `metadata.max.age.ms` is an `int`), so saturating would be a Rust-only change. R5's "saturate only where Java clamps" handles it, but only if the reviewer checks.
7. **R6 (`close()` explicit) vs `ConsumerNetworkThread::cleanup` and `CompletedFetch::drain`.** R6 is written for `ShareConsumerImpl#close`. Applied to the network thread's `cleanup`, it would push for a step-by-step diff. That diff matches Java here, but a literal reading also pulls in `RequestManagers::close` / `Drop` items that Java closes "quietly". I would not demand the non-Java steps be removed.
8. **R3 (production-path construction) vs `FetchCollector` tests.** The `build_harness` fixture constructs `FetchCollector::new` directly, which is the crate constructor and so satisfies R3. But `set_force_initialize_error` is a `#[cfg(test)]` seam on the production struct. R3's "do not add test seams to production code" reads as condemning it, even though it is the only faithful translation of Java's anonymous-subclass `initialize()` override (`testErrorInInitialize`). Demanding its removal would cost a translated test.
9. **R1 vs `testDoubleBuild`.** It is untranslated because `build_request(self, Builder)` consumes the builder, so a double build cannot compile. R1 says every Java assertion needs a counterpart "unless a deviation note at the test says why", and no note exists. A rule-driven reviewer would demand a note or a compile-fail test. A one-line note is cheap, but a trybuild-style test would be over-application.
10. **R2 (deferral ledger) vs the `fetch_diag` diagnostic.** "A stub closure or a setter production never calls counts as a FIXME" nudged me toward calling an opt-in diagnostic a FIXME (S1-5). It is gated by its own log target and is not a deferred Java path; R2 should not reach it.
11. **R7 applied to code comments, not only to rules.** It was useful (S5-1: a deviation note that claimed a Java-path gap no longer true). But the rule is scoped to "rules, PLANs, Critic comments", so a literal reader would not check code-level deviation notes. Extending R7 to deviation notes in tests looks worthwhile and low-risk.

### Rule-effectiveness notes (for the rule authors)
- **R5 → S1-1:** hit. The `swap_remove` grep found it in under a minute.
- **R4.5 → S1-6:** **miss.** I read the exact function and did not map the lock order. R4.5 says "map every nested lock path where a Java reentrant monitor becomes a non-reentrant mutex". This cycle crosses two *different* monitors (subscriptions → metadata vs metadata → subscriptions), not a reentrant one. The rule's wording steered me toward self-deadlock and away from ABBA. Suggest: "map every pair of locks held at once, in both directions, including a lock taken inside a getter that Java leaves unsynchronized".
- **R4.4 → S2 near-miss:** miss. The error-propagation half of "collapsing a chain" is not spelled out.
- **R1:** produced the most findings: S1-2, S1-3, S1-4, S5-1, S5-2. All are real but low severity (test fidelity).

## Manager scoring (2026-11-18)
Known bugs in the snapshots, caught blind?
| Bug | Fixed in | Caught blind | Rule |
|---|---|---|---|
| C3-01 `swap_remove` drops/reorders fetch-session partitions | #277 | yes (S1-1) | R5 |
| poll vs metadata ABBA deadlock (`update_partition_state` vs `Metadata::update`) | #277 | no — found only from an S3 test name | R4.5 wording covered reentrancy, not lock order |
| C2-05 racing reconciles via spawned AsyncPoll | #279 | partly (S2-2 flagged the spawn; the race itself was withdrawn on a wrong "current_thread" assumption) | R4 |
| reconcile error skips the poll's commit/timer/fetch steps | #279 | no — seen and dismissed | — |
| C2-04 rebalance partitions unsorted (still open on master) | — | yes (S2-1) | R5 |
Controls (S3/S4 post-fix, S5 hot path): 0 rule-driven false positives reported; 1 low-confidence non-rule FP (S1-5). Over-application pressure noted for R4.6, R4.2, R3, R1 (see self-check) — addressed by the rewording below.
New candidate issues on current master (S1-2/3/4, S2-3, S2-4, S5-1, S5-2) are UNVERIFIED and out of M18 scope; listed for the consumer follow-up backlog only.
