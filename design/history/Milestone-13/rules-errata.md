# Milestone-13 — Claude rules Java `file:line` citation errata (4.2.0 → 4.3.1)

**Scope:** read-only survey (Phase 0, agent 60). This file records where the
Java `file:line` citations embedded in the `.claude/rules/*.md` files drifted
when the `kafka/` submodule moved from tag `4.2.0` (`a18251bae0`) to `4.3.1`
(`26b251a451`). **The rules files themselves are NOT edited here** — per
`agent-roles.md`, an Actor proposes rule changes via the suggestion process;
the human applies them. This is a suggestion list plus a Phase-4 placeholder.

**Method:** each citation was calibrated against the `4.2.0` blob (to confirm
the rule's original line was correct) and then re-located in the `4.3.1` tree
by content, not by arithmetic. A "new line" is the actual 4.3.1 location of the
exact code the rule points at.

**Which rules files carry line citations:**

- `producer-transactions.md` — the only rules file with `File.java:NNN`
  citations (canonical form + a few prose `Java NNN` line refs bound to a file
  established in the surrounding text). All errata below come from it.
- `consumer-threading.md` — mentions `Consumer.java` and
  `AsyncKafkaConsumer.java` only as "the Java source is the contract", with **no
  line numbers**. Nothing to drift. (Its §28/§31 need a behavioral amendment —
  see the Phase-4 placeholder below — but that is not a line-citation errata.)
- `admin-client.md` — mentions `AdminMetadataManager.java` only, **no line
  numbers**. Nothing to drift.

## Summary

- **21 drifted citations** across **4 Java files** (all in
  `producer-transactions.md`).
- **13 citations unchanged** (still resolve correctly at 4.3.1).
- **No behavioral drift:** in every case the rule's *claim* about the Java code
  still holds at 4.3.1; only the line numbers moved. Two files had genuine code
  churn near a citation (`TransactionalRequestResult.java` await-overload
  consolidation; `MessageDataGenerator.java` refactor) but the cited construct
  and the rule's reasoning survive — see notes.

## Drifted citations

### `TransactionManager.java` — uniform **+18** line shift

A KAFKA-14831-area block was expanded near line 205 (`@@ -205,6 +205,24 @@`,
net +18); every citation at or after line ~205 shifts by +18. Citations before
that (the coordinator/correlation fields at 136–139) are unchanged.

| Rule § | Cited (4.2.0) | 4.3.1 | Construct |
|---|---|---|---|
| §1 | 234–286 | 252–304 | KAFKA-14831 "poison" javadoc block (`See KAFKA-14831 for more detail.` now at 300) |
| §1 | 287–289 | 305–307 | `shouldPoisonStateOnInvalidTransition()` |
| §2 | 969 | 987 | `void lookupCoordinator(TxnRequestHandler request)` |
| §2 | 1410 | 1428 | `clearInFlightCorrelationId()` call inside `TxnRequestHandler.onComplete` |
| §2 | 1421 | 1439 | `synchronized (TransactionManager.this)` block start in `onComplete` |
| §5 | 1261–1283 | 1279–1301 | `handleCachedTransactionRequestResult(...)` (decl at 1279) |
| §7 | 655 | 673 | `txnPartitionMap.startSequencesAtBeginning(...)` call inside `bumpIdempotentProducerEpoch` (the method decl itself moved 645→663) |
| §7 | 790 | 808 | `removeInFlightBatch(batch)` call inside `handleFailedBatch` |
| §7/§9 | 818 | 836 | `txnPartitionMap.adjustSequencesDueToFailedBatch(batch)` |
| §9 | 799 | 817 | `if (exception instanceof OutOfOrderSequenceException && !isTransactional())` |
| §9 | 806 | 824 | `} else if (exception instanceof UnknownProducerIdException)` |

### `Sender.java` — uniform **+1** line shift

Net +1 line from `@@ -367,7 +367,8 @@`; every citation after ~374 shifts +1.

| Rule § | Cited (4.2.0) | 4.3.1 | Construct |
|---|---|---|---|
| §2 | 522 | 523 | `transactionManager.lookupCoordinator(nextRequestHandler)` |
| §4 | 459–518 | 460–519 | `maybeSendAndPollTransactionalRequest()` (interior refs also +1: poll 462/496/509→463/497/510; `awaitNodeReady` 484→485; `time.sleep` 501/525→502/526) |
| §7 | 685 | 686–687 | MESSAGE_TOO_LARGE split path: `if (transactionManager != null)` guard 685→686, `transactionManager.removeInFlightBatch(batch)` at 687 (the `MESSAGE_TOO_LARGE` test is now at 676) |
| §7 | 750–752 | 751–753 | `reenqueueBatch(ProducerBatch, long)` |
| §7 | 848 | 849 | `transactionManager.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers)` in `failBatch` |
| §7 | 854 | 855 | `maybeRemoveAndDeallocateBatch(batch)` (the one paired with the failBatch above) |

### `TransactionalRequestResult.java` — code churn (`@@ -47,16 +47,12 @@`, net −4)

The `await(...)` region was consolidated. **The §5 semantic claims all still
hold** (`isAcked` is `volatile`, set to `true` only inside `await()`, set
*before* the `error` check so a failed result is still acked, and the
`InterruptException` path has no Rust analogue) — only the lines moved.

| Rule § | Cited (4.2.0) | 4.3.1 | Construct |
|---|---|---|---|
| §5 | 62 | 58 | `isAcked = true;` (field decl moved 30; `isAcked()` getter at 79–80; `await(...)` at 50) |
| §5 | 62–65 | 58–61 | `isAcked = true;` set before the `if (error != null) throw error;` check |
| §5 | 66 | 62–63 | `catch (InterruptedException e)` → `throw new InterruptException(...)` |

### `MessageDataGenerator.java` — large refactor (52/98)

| Rule § | Cited (4.2.0) | 4.3.1 | Construct |
|---|---|---|---|
| §11 | 792 | 776 | `if (!field.ignorable())` guarding `field.generateNonIgnorableFieldCheck(...)` (call now at 777). The rule's claim (the non-default write check is emitted *only* for non-ignorable fields) still holds; the "attempted to write a non-default…" message string lives in `FieldSpec.generateNonIgnorableFieldCheck`, not in `MessageDataGenerator`. |

## Unchanged citations (still resolve at 4.3.1)

- `TransactionManager.java:136–139` (§2) — `inFlightRequestCorrelationId` (136),
  `transactionCoordinator` (137), `consumerGroupCoordinator` (138),
  `coordinatorSupportsBumpingEpoch` (139). Before the +18 insertion; unchanged.
- `RecordAccumulator.java:558–560` (§7) — `insertInSequenceOrder` throw. File's
  only diff is an import-block reshuffle (`@@ -28,13 +28,13 @@`), no line shift.
- `RecordAccumulator.java:877–926` (§3) — sequence-assignment block incl.
  `maybeUpdateProducerIdAndEpoch` (908), `sequenceNumber` (918),
  `incrementSequenceNumber` (919), `addInFlightBatch` (924). Unchanged.
- `TxnPartitionEntry.java:58–61, 62–65, 104–106, 154–161, 163–173` (§6/§8) —
  file's only diff is a one-line license/import edit at ~18; no shift.
- `AbstractRequest.java:46–51` (§12) — file unchanged 4.2.0→4.3.1.
- `OffsetCommitRequest.java:55` (§12) — file unchanged.
- `OffsetFetchRequest.java:64` (§12) — the 4.3.1 change is at line 325+
  (UNKNOWN_TOPIC_ID handling), below the citation; :64 unchanged.
- `AddPartitionsToTxnRequest.java:58` (§12) — file unchanged.
- `ApiVersionsRequest.java:43` (§12) — file unchanged.
- `OffsetsForLeaderEpochRequest.java:57` (§12) — file unchanged.

## §28 / §31 amendment — **placeholder (Phase 4 will draft)**

`consumer-threading.md` §28 (event-variant tables) and §31 (rebalance-callback
handshake steps) describe the **4.2** consumer rebalance handshake. Milestone-13
Phase 4 reshapes it (PLAN §2.1, KAFKA-20106/20321/20332): the bg reconcile now
ends with `signalPartitionsAssigned` → a completable **`PartitionsAssignedEvent`**
(bg→app, sent even with no listener); the app thread replies with a completable
**`ApplyAssignmentEvent`** (app→bg) and awaits it, so `assignment()` mutates only
within `poll()`; revoke/lost become **`PartitionsRemovedEvent`**; `AsyncPollEvent`
gains `markReconciliationCheckComplete()` / `maybeReconcile(canCommit)` gating and
`processBackgroundEvents` gains a `skipAssignmentEvents` flag.

**This section is intentionally left as a placeholder.** Per PLAN §2.1 and §3
(Phase 4 deliverable), **Phase 4's Critic drafts the amendment text for §28/§31
into its COMMENTS file, and the human applies/approves the rules edit.** Phase 0
does not draft it (the code it must describe does not exist yet on this branch).
When Phase 4 lands, append the drafted §28/§31 amendment text here (or reference
the COMMENTS file that holds it) so this errata document is the single index of
rules-doc follow-ups for the milestone.

### Phase 4 implementation notes (input for the §28/§31 amendment draft)

Phase 4 (agent 64) landed the reshape. What changed **vs the current §31 step
list**, for the Critic to fold into the amendment:

- **Event names / shapes (§28 tables).**
  - `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name,
    partitions, ack }` split into:
    - `BackgroundEvent::PartitionsRemoved { method_name, partitions, ack }` —
      revoke/lost only (`method_name` is `OnPartitionsRevoked` /
      `OnPartitionsLost`).
    - `BackgroundEvent::PartitionsAssigned { assigned_partitions,
      added_partitions, ack }` — assign; **sent even with no listener**; no
      `method_name`.
  - New app→bg completable `ApplicationEvent::ApplyAssignment { handle,
    assigned_partitions, added_partitions }`.
  - `AsyncPollState` gained `is_reconciliation_check_complete` /
    `mark_reconciliation_check_complete` + a `Notify`; `complete_successfully`
    / `complete_exceptionally` mark it as a safety net.

- **§31 step list changes (the handshake).**
  - The **assign** path no longer mutates `SubscriptionState` on the bg side
    inside the reconcile continuation. `continue_after_revoke` now enqueues
    `PartitionsAssigned` and stores `PendingReconcile::AfterAssign`; the
    subscription mutation (`assign_from_subscribed_awaiting_callback` +
    `notify_assignment_change`) moved to
    `ConsumerMembershipManager::apply_assignment`, invoked on the bg side by
    the AEP when it processes the app-triggered `ApplyAssignment` event. Net:
    `consumer.assignment()` changes only within `poll()`.
  - The app-side `process(PartitionsAssigned)` = `applyNewAssignment` (send
    `ApplyAssignment` via `add_and_get`, awaited) → run `on_partitions_assigned`
    if a listener exists → reply on `ack`. `process(PartitionsRemoved)` = invoke
    revoke/lost listener → reply on `ack` (unchanged from the 4.2 shape modulo
    the rename). The `AfterAssign` ack is now the `PartitionsAssigned` event's
    ack (completed after applyNewAssignment + callback), not the old
    `CallbackCompleted` ack.
  - `AbstractMembershipManager::transition_to` now fires
    `MemberStateListener::on_member_state_change`; the app-side notifier maps
    `RECONCILING` → `has_pending_reconciliation` (shared `Arc<AtomicBool>`).
  - `collect_fetch` (via `poll_for_fetches`) gates on
    `wait_reconciliation_check`: when `has_pending_reconciliation` and the
    in-flight poll's reconciliation check is not complete, it waits on the
    `AsyncPollState` notify (racing the wakeup token) up to the poll deadline,
    returning an empty fetch on timeout/wakeup. `pollForFetches` computes
    `pollTimeout` **after** the first `collectFetch` (AK 4.3.1 reorder).
  - The reconcile gate moved after computing revoked partitions and is now
    `!can_commit && (auto_commit_enabled || !revoked.is_empty())`.
  - Lost path (`enqueue_release_callback`, the Rust `signalPartitionsLost`)
    marks pending revocation **before** enqueuing the callback (KAFKA-20321).
  - `skip_assignment_events` (Java `skipAssignmentEvents`, KAFKA-20428):
    `process_background_events_inner` takes it as a second flag; when set
    (unsubscribe / close), `PartitionsAssigned` events are completed
    **exceptionally** ("Assignment event skipped because consumer is
    unsubscribing") and NOT recorded into `first_error`. The existing
    `skip_rebalance_callback` (close path) is a separate flag governing
    `PartitionsRemoved` ack-without-listener-invocation.

- **Invariants re-verified (all still hold):** listener callbacks run on the
  caller's task via `process_background_events` (never `tokio::spawn`ed, never
  from `run_once`); the bg loop never blocks on an ack (`AfterRevoke` /
  `AfterAssign` / `PendingRelease` are `try_recv` cross-iteration state); no
  `MutexGuard` held across `.await`; the network poll is not raced in a
  `select!`. The app-side await of `ApplyAssignment` is deadlock-free precisely
  because the bg loop keeps spinning (Phase 41).
