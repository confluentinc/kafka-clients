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

## §28 / §31 amendment — **DRAFTED (Phase 4, agent 64) — PROPOSED, human applies**

`consumer-threading.md` §28 (event-variant tables) and §31 (rebalance-callback
handshake steps) describe the **4.2** consumer rebalance handshake. Milestone-13
Phase 4 reshapes it (PLAN §2.1, KAFKA-20106/20321/20332): the bg reconcile now
ends with `signalPartitionsAssigned` → a completable **`PartitionsAssignedEvent`**
(bg→app, sent even with no listener); the app thread replies with a completable
**`ApplyAssignmentEvent`** (app→bg) and awaits it, so `assignment()` mutates only
within `poll()`; revoke/lost become **`PartitionsRemovedEvent`**; `AsyncPollEvent`
gains `markReconciliationCheckComplete()` / `maybeReconcile(canCommit)` gating and
`processBackgroundEvents` gains a `skipAssignmentEvents` flag.

**PROPOSED amendment text follows (Critic 64 draft; human applies to `.claude/rules/consumer-threading.md`).** Per PLAN §2.1 and §3 the Phase-4 Critic drafts the §28/§31 amendment; the human applies/approves the rules edit. The verbatim draft — copied from the Phase-4 `COMMENTS.64.md` "PROPOSED AMENDMENT (human applies)" section — is reproduced below so this errata document is the single index of rules-doc follow-ups for the milestone. It describes the AK 4.3.1 three-leg handshake as landed in commits `85c0fb0e`.. and preserves the existing invariant statements and anti-pattern lists with updated names. The "Phase 4 implementation notes" subsection that follows is the supporting input for this draft.

## PROPOSED AMENDMENT (human applies) — `consumer-threading.md` §28 & §31

Per PLAN §2.1 this is the Phase-4 Critic deliverable. The text below is drafted to
be applied by a human into `.claude/rules/consumer-threading.md` (and/or appended
to `design/history/Milestone-13/rules-errata.md` per its Phase-4 placeholder). It
describes the AK 4.3.1 three-leg handshake as landed in commits `85c0fb0e`.. and
preserves the existing invariant statements and anti-pattern lists with updated
names.

### §28 amendment — new / renamed event-variant table rows

Replace the §28 references to the single
`ConsumerRebalanceListenerCallbackNeededEvent`/`BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
with the AK 4.3.1 event set. Add these rows:

| Java class | `extends` | Rust variant | Completable? | Fields |
|---|---|---|---|---|
| `PartitionsRemovedEvent` | `CompletableBackgroundEvent<Void>` | `BackgroundEvent::PartitionsRemoved` | yes (`ack: oneshot::Sender<Result<(),KafkaError>>`) | `method_name` (`ON_PARTITIONS_REVOKED`/`ON_PARTITIONS_LOST` only), `partitions` |
| `PartitionsAssignedEvent` | `CompletableBackgroundEvent<Void>` | `BackgroundEvent::PartitionsAssigned` | yes (`ack`) | `assigned_partitions` (full assignment), `added_partitions` (newly added) — **no `method_name`**; **enqueued even when NO listener is registered** |
| `ApplyAssignmentEvent` | `CompletableApplicationEvent<Void>` | `ApplicationEvent::ApplyAssignment` | yes (`handle: CompletableEventHandle<()>`) | `assigned_partitions`, `added_partitions`; `deadlineMs = Long.MAX_VALUE` (`i64::MAX`) |

Notes to fold into §28 prose:
- `PartitionsRemovedEvent` and `PartitionsAssignedEvent` are the AK 4.3.1 split of
  the former single `ConsumerRebalanceListenerCallbackNeededEvent` (KAFKA-20106).
  The revoke/lost path keeps the 4.2 shape (renamed); the assign path is the new
  bg→app leg that carries the full assignment so the app can apply it within
  `poll()`.
- `ApplyAssignmentEvent` is the new app→bg leg. It is a
  `CompletableApplicationEvent<Void>` (not bare) — the app thread `add_and_get`s it
  and awaits, so the `SubscriptionState` mutation runs on the bg AEP but is
  triggered/awaited by the app thread.
- `AsyncPollState` (bare `ApplicationEvent::AsyncPoll` payload, the §28
  `AsyncPollEvent` precedent) gains a reconciliation-check sub-future modeled as
  `AtomicBool is_reconciliation_check_complete` + `tokio::sync::Notify`
  (`mark_reconciliation_check_complete` / `is_reconciliation_check_complete`),
  completed by the AEP right after `maybe_reconcile(true)`, and as a safety net by
  `complete_successfully` / `complete_exceptionally`. This is the async-native
  analog of Java's `reconciliationCheckFuture` (KAFKA-20332/20535).

### §31 amendment — rewritten step list for the AK 4.3.1 handshake

Rewrite §31's step list (the bidirectional handshake mechanism) to the three-leg
flow. Keep every invariant statement and the anti-pattern list; only the event
names and the assign-path steps change.

**Revoke / lost path (renamed, otherwise the 4.2 flow):**
1. The bg reconcile (or a fence/fatal/stale release transition) enqueues a
   `BackgroundEvent::PartitionsRemoved { method_name, partitions, ack }` — where
   `method_name` is `ON_PARTITIONS_REVOKED` or `ON_PARTITIONS_LOST`. For the lost
   path, the lost partitions are marked pending-revocation (fetch paused) BEFORE
   the event is enqueued (KAFKA-20321 — `signal_partitions_lost` /
   `enqueue_release_callback`), even when no listener is registered.
2. The bg loop does NOT block on `ack`; it stores the receiver as cross-iteration
   state (`AfterRevoke` / `PendingRelease`) and keeps spinning (Phase 41).
3. The app side, inside a public blocking-style API, drains `PartitionsRemoved`
   and invokes the revoke/lost listener inline on its own task, then `ack.send`s
   the result and pokes the bg-task wakeup `Notify`.

**Assign path (new three-leg flow, KAFKA-20106):**
1. The bg reconcile ends `continue_after_revoke` by enqueuing a
   `BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions,
   ack }` — **unconditionally, even with no listener** — and stores `AfterAssign`.
   It does NOT mutate `SubscriptionState` itself.
2. The app side, inside `poll()`, drains `PartitionsAssigned` and:
   a. sends `ApplicationEvent::ApplyAssignment { handle, assigned_partitions,
      added_partitions }` via `add_and_get` and **awaits** it — the bg AEP runs
      `ConsumerMembershipManager::apply_assignment`
      (`assign_from_subscribed_awaiting_callback` + `notify_assignment_change`),
      so the subscription mutates on the bg side but is triggered/awaited within
      `poll()`. This is deadlock-free precisely because the bg loop keeps spinning
      (Phase 41).
      - If `ApplyAssignment` fails, the app wraps it "Failed to apply the new
        assignment", `ack.send(Err(..))`, records it as the first error, and does
        NOT run the listener (KAFKA-20382).
   b. runs `on_partitions_assigned(added_partitions)` if a listener is registered,
      else completes with `Ok(())`;
   c. `ack.send`s the result and pokes the bg-task wakeup `Notify`.
3. The bg loop `try_recv`s the `AfterAssign` ack across iterations and, on success,
   resumes the reconcile (enables fetching for the added partitions); on failure it
   keeps the added partitions non-fetchable (guarded by
   `subscriptions.assigned_partitions().contains_all(added)`).

**Invariant (updated statement — unchanged in substance):** `consumer.assignment()`
changes ONLY within `poll()`. It is the app-triggered `ApplyAssignment` (not the bg
reconcile) that mutates the subscription, so a non-`poll()` API (`unsubscribe`,
`close`, timed queries) never applies a new assignment — enforced by
`skip_assignment_events` (KAFKA-20428), which completes any queued
`PartitionsAssigned` EXCEPTIONALLY with
`"Assignment event skipped because consumer is unsubscribing"` and does NOT record
it into `first_error`.

**Reconciliation-check gate (KAFKA-20332/20535):** `collect_fetch` must not return
buffered records until the bg has, for the in-flight poll, checked for pending
reconciliations (triggered commits, marked revoked partitions pending-revocation).
The app tracks member state via `MemberStateListener::on_member_state_change`
(RECONCILING → `has_pending_reconciliation`); `wait_reconciliation_check` waits on
the `AsyncPollState` `Notify` (racing the wakeup token, up to the poll deadline)
ONLY while `has_pending_reconciliation` is set and the check is incomplete —
otherwise it returns immediately (the KAFKA-20535 CPU optimization). `poll_timeout`
is computed AFTER the first `collect_fetch` (AK 4.3.1 reorder).

**Anti-patterns to flag in review (updated names — all still apply):**
- `tokio::spawn(listener.on_partitions_*(...))` anywhere.
- Calling listener methods from inside the bg task's `run_once`.
- `ack_rx.await` inline in the bg loop for `AfterRevoke` / `AfterAssign` /
  `PendingRelease` (freezes the loop, deadlocks reentrant ops) — use `try_recv`
  cross-iteration state.
- The bg reconcile mutating `SubscriptionState` for the assign path (it must go
  through the app-triggered `ApplyAssignment`), or `assignment()` changing outside
  `poll()`.
- A `PartitionsAssigned` variant/event WITHOUT the full `assigned_partitions` set,
  or short-circuiting its enqueue when no listener is registered (it must always be
  sent).
- Busy-spinning the bg loop or shrinking `poll_wait_time_ms` while a
  reconciliation-check or callback ack is pending (use the app-side `Notify` poke).
- A public blocking-style API that does not call `process_background_events` before
  its main wait; a separate task spawned to drain the background-events channel.
- Holding `SubscriptionState`'s `MutexGuard` across the listener call or across any
  `.await`.

**Tests required (updated):** the two §31 regression tests remain mandatory
(`section_31_commit_sync_from_inside_revoked_callback_succeeds`,
`section_31_rebalance_does_not_advance_until_listener_resolves`). Add coverage for
the assign path's apply-failure (`partitions_assigned_event_sends_error_when_apply_assignment_fails`,
KAFKA-20382), the `skip_assignment_events` unsubscribe skip, and the
reconciliation-check gate (`wait_reconciliation_check_*`, KAFKA-20535).

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
