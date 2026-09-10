# Critic 64 — Milestone-13 Phase 4 (consumer rebalance/poll, AK 4.3.1)

Reviewed commits `503d671b..HEAD` (six commits: `85c0fb0e`, `9a62ef80`,
`7b7ae58d`, `75b654a0`, `468610f9`, `3f60cfa3`) against the AK 4.3.1 Java
contract (`kafka/` @ 4.3.1). Build / lib tests / clippy / format-check all green
at HEAD (3709 lib tests pass, 3 ignored — all pre-existing, none from Phase 4).

**Overall: this is a high-fidelity translation of the highest-risk phase in the
milestone. No correctness bugs found.** The three-leg handshake, KAFKA-20321 /
20382 / 20426 / 20428 / 20535 fixes, the §31 regression tests, and the event
reshape all match Java. The items below are minor observations (non-blocking)
plus the two requested adjudications and the required §28/§31 amendment draft.

---

## Verification summary (checklist items 1–9)

- **1. Three-leg handshake** — VERIFIED faithful.
  - (a) `PartitionsAssigned` enqueued unconditionally (no listener short-circuit)
    in `enqueue_partitions_assigned_event` / `continue_after_revoke`; app-side
    `PartitionsAssigned` arm runs `applyNewAssignment` (ApplyAssignmentEvent,
    `add_and_get`, awaited) BEFORE the listener check, so with no listener the
    assignment is still applied within `poll()` and the ack is `Ok`. Matches Java
    `process(PartitionsAssignedEvent)`.
  - (b) Field fidelity matches Java exactly: `PartitionsAssigned{assigned_partitions,
    added_partitions, ack}`, `PartitionsRemoved{method_name, partitions, ack}`,
    `ApplyAssignment{handle, assigned_partitions, added_partitions}`. Completable
    vs bare matches `extends` (all three are completable). No fictional fields.
  - (c) `ApplyAssignment` → `ConsumerMembershipManager::apply_assignment` =
    `assign_from_subscribed_awaiting_callback(assigned, added)` +
    `notify_assignment_change(assigned)` — same method + args + order as Java
    `applyAssignment`.
  - (d) Ordering traced end-to-end: bg reconcile enqueues `PartitionsAssigned` and
    parks `AfterAssign`; app `poll()` drains, sends `ApplyAssignment`, bg AEP
    mutates SubscriptionState, app runs listener, app acks, bg advances. The
    SubscriptionState mutation happens on the bg AEP but is triggered/awaited
    within `poll()` → `assignment()` changes only within `poll()`. No window found
    where assignment changes outside `poll()` or the bg advances state before the
    ack. `continue_after_revoke` no longer mutates the subscription (correctly
    moved to `apply_assignment`).
  - (e) KAFKA-20382: apply failure → `ack.send(Err(wrapped "Failed to apply the
    new assignment"))` + `record_first_error` + `continue` (no listener run);
    AEP `process_apply_assignment` completes the handle exceptionally. Both
    directions covered. Tested by `partitions_assigned_event_sends_error_when_apply_assignment_fails`.

- **2. KAFKA-20321** — VERIFIED. `enqueue_release_callback` (the Rust
  `signalPartitionsLost` analog) calls `mark_pending_revocation(&partitions)`
  BEFORE `enqueue_rebalance_callback`, and does so before the no-listener
  short-circuit (marking happens even with no listener) — exactly as Java's
  `signalPartitionsLost`. The 3 new CMM transition tests
  (`transition_to_{fenced,fatal,stale}_marks_pending_revocation_before_signaling_partitions_lost`)
  and `leave_group_during_reconciliation_then_rejoin` are translated (commit
  `7b7ae58d`) and pass.

- **3. skipAssignmentEvents** — VERIFIED.
  - Message text exact: `"Assignment event skipped because consumer is unsubscribing"`.
  - Skip arm completes the ack EXCEPTIONALLY and does NOT `record_first_error`
    (matches Java completing the future exceptionally without recording into
    `firstError`).
  - `PartitionsRemoved` (revoke/lost) is still processed during unsubscribe
    (`skip_rebalance_callback=false`, `skip_assignment_events=true`), so the user
    can still flush offsets — matches Java.
  - The ack IS answered (bg not left waiting): the skip arm `ack.send(Err(..))` +
    `wake_background_task()`.
  - Call-site nuance (not a defect — see Observation 2): Java passes
    `skipAssignmentEvents=true` only at the *unsubscribe* site; close() never calls
    the flagged `processBackgroundEvents(future,...)` overload at all. Rust passes
    it at BOTH unsubscribe and close, which is a correct Rust adaptation (the Rust
    bg task parks on the `PartitionsAssigned` ack and must be unblocked during
    close, whereas Java simply abandons the reconcile future). Behaviorally
    equivalent.

- **4. KAFKA-20426 / 20428 / 20535** — all VERIFIED landed:
  - 20426 (group.id+assign busy loop): `maximum_time_to_wait` returns `i64::MAX`
    when `state == Unsubscribed` (matches `Long.MAX_VALUE`). Tested by
    `maximum_time_to_wait_returns_max_when_unsubscribed`.
  - 20428 (unsubscribe-failure-with-assignment-updates): `skipAssignmentEvents`
    (see item 3).
  - 20535 (CPU fix / AsyncPoll reconciliation gating): `has_pending_reconciliation`
    `Arc<AtomicBool>` set by `on_member_state_change` (RECONCILING),
    `wait_reconciliation_check` early-returns `true` when the flag is clear (no
    busy-wait), else waits on the `AsyncPollState` `Notify` racing the wakeup
    token up to the poll deadline (create-`notified()`-then-check ordering closes
    the lost-wakeup race). Does not busy-spin (does not shrink `poll_wait_time_ms`)
    and does not starve the bg loop (waits on the app task only; the bg loop keeps
    spinning and completes the check). Matches Java's `reconciliationCheckFuture`
    semantics. 5 `wait_reconciliation_check_*` tests pass.

- **5. §31 invariants + two regression tests** — VERIFIED. Both regression tests
  survive and are adapted (not deleted or gutted):
  `section_31_commit_sync_from_inside_revoked_callback_succeeds` and
  `section_31_rebalance_does_not_advance_until_listener_resolves` (now use
  `PartitionsRemoved`; still assert deadlock-freedom and no-advance-until-resolve).
  Anti-pattern grep clean: no `tokio::spawn` of listener calls, no `ack_rx.await`
  inline in the bg loop (`AfterRevoke`/`AfterAssign`/`PendingRelease` remain
  `try_recv` cross-iteration state), no `MutexGuard` held across `.await` in the
  new paths (`apply_assignment` doesn't await; the assign/skip arms drop the
  `rebalance_listener` lock before awaiting).

- **6. Heartbeat commit `75b654a0`** — VERIFIED + adjudicated below.

- **7. Skip adjudications** — spot-checked; see below.

- **8. Test-delta completeness** — VERIFIED. All 7 new `AsyncKafkaConsumerTest`
  methods and all 4 new `ConsumerMembershipManagerTest` methods are accounted for
  (translated or skipped-with-reason). The two skips
  (`testPollWithManualAssignmentDoesNotBusyLoop`,
  `testStreamsTasksAssignedEventSendsErrorWhenApplyAssignmentFails`) are justified.
  Notably there is **no** Java 4.3.1 dedicated success-path test for the app-side
  `process(PartitionsAssignedEvent)` three-leg — so the absence of a Rust
  success-path consumer-level unit test is NOT a translation gap; Java's coverage
  shape is the same (component-level CMM tests + the error-path test).

- **9. Build/test/lint/format** — GREEN at HEAD.

---

## Observations (minor; non-blocking)

### Observation 1 — `maximum_time_to_wait` omits Java's `pollTimer.update(currentTimeMs)` before the UNSUBSCRIBED short-circuit
`consumer_heartbeat_request_manager.rs` `maximum_time_to_wait`: Java calls
`pollTimer.update(currentTimeMs)` FIRST, then returns `Long.MAX_VALUE` for
UNSUBSCRIBED. The Rust version checks `state == Unsubscribed` and returns
`i64::MAX` *before* touching the poll timer. Behaviorally benign — an UNSUBSCRIBED
member (manual assignment / no group, the exact KAFKA-20426 scenario) has no
active poll-interval enforcement on this path, and the poll timer is updated
elsewhere in the poll loop — so I am not raising this as a defect. Noting it only
as a literal-translation deviation in case a later change makes the pollTimer
advance observable on this path.

### Observation 2 — close path reuses the "consumer is unsubscribing" skip message
The `PartitionsAssigned` skip arm produces
`"Assignment event skipped because consumer is unsubscribing"` and is reached on
BOTH unsubscribe and close (close sets `skip_assignment_events=true`). During
close the "unsubscribing" wording is slightly inaccurate, but this error is
internal (goes on the bg-reconcile ack, completed-exceptionally-not-recorded,
never surfaced to the user). No action needed; recording for completeness.

### Observation 3 — `on_partitions_assigned` receives added partitions unsorted (pre-existing)
`consumer_membership_manager.rs` carries `added` as a `HashSet` and hands it to
`enqueue_partitions_assigned_event` / the app-side `invoke_partitions_assigned`
as a `Vec` (`added.iter().cloned().collect()`), so the callback sees a
nondeterministic order. Java's `signalPartitionsAssigned(assignedPartitions,
addedPartitions)` takes `addedPartitions` as a `SortedSet<TopicPartition>`
(TreeSet, `TOPIC_PARTITION_COMPARATOR`), passing them sorted. This predates
Phase 4 (the 4.2 `enqueue_rebalance_callback(OnPartitionsAssigned, added_vec)`
path had the same shape) and is not part of the 4.2→4.3.1 delta, so it is out of
scope to fix here — recorded only for accuracy. The `PartitionsRemoved` path has
the same property for revoked/lost partitions. Follow-up candidate if strict
Java ordering parity in listener arguments is desired.

### Observation 4 — four import-only *test* files not enumerated in the recorded-skip list
`git -C kafka diff 4.2.0..4.3.1` shows four in-scope consumer *test* files with
small (14–22 line) deltas that are **purely** `record`→`record.internal` import
moves (covered transitively by the Phase-1 module move): `CompletedFetchTest`,
`FetchCollectorTest`, `FetchRequestManagerTest`, `FetcherTest`. The Phase-4
recorded-skip line enumerates the *production* import-move files
(`FetchCollector.java`, etc.) but omits these four test files. No code action;
add a one-line note to the skip list so the Phase-6 completeness audit can tick
them off.

---

## Adjudication — item 6: the Issue-9 GROUP_ID_NOT_FOUND epoch-conditional deviation

**Verdict: DEFENSIBLE and correctly documented. Not a divergence-to-fix in this
phase.**

Reasoning (checked against `kafka/` @ 4.2.0 and 4.3.1):

- In **Java 4.2.0**, `AbstractHeartbeatRequestManager` had **no** `GROUP_ID_NOT_FOUND`
  case — it fell through to `default:` and, since the subclass
  `handleSpecificExceptionInResponse` did not handle it either, it was treated as
  an unexpected/fatal error. So the *fatal* semantics for a non-unsubscribed
  member already existed in 4.2.
- The Rust client's epoch-conditional recovery (retry when epoch==0, fenced-rejoin
  when epoch>0) for `GROUP_ID_NOT_FOUND` is a **pre-existing deliberate deviation**
  from Java (Milestone-8 Issue 9), predating this milestone.
- **Java 4.3.1 did not introduce the fatal treatment fresh.** It made the existing
  fatal behavior *explicit* and *added a new UNSUBSCRIBED-skip arm*
  (`onHeartbeatRequestSkipped`). The genuinely-new half (UNSUBSCRIBED → skip) IS
  translated (`GroupIdNotFound && state==Unsubscribed → on_heartbeat_request_skipped
  → Handled`) and tested (`group_id_not_found_while_unsubscribed_is_skipped`).
- Therefore skipping `testGroupIdNotFoundWhileStableIsFatal` is not dropping a new
  4.3.1 fix — it asserts the fatal behavior the Rust client deliberately does not
  have (Issue 9). The Actor recorded the deviation at the handler site and in the
  PLAN per DoD #7.

Recommendation: none blocking. The standing Issue-9 deviation remains a legitimate
follow-up candidate (the Rust client is knowingly more lenient than the broker
contract for non-unsubscribed `GROUP_ID_NOT_FOUND`), but that is out of scope for
the 4.2→4.3.1 delta and is already tracked.

---

## Adjudication — item 7: skip / N/A claims

Spot-checked 4+ claims; all HOLD:

- **7a WakeupTrigger (fdece9c358) — N/A HOLDS.** The Java fix keeps a `WakeupFuture`
  when `setActiveTask` sees the current task already completed, closing a
  pendingTask/currentTask race in Java's `ActiveFuture`/`WakeupFuture` state
  machine. Rust models wakeup as a rotating `CancellationToken` (§11): a wakeup
  persists as a cancelled token until a public API surfaces `KafkaError::Wakeup`
  and rotates, so the wakeup is structurally never lost — the Java state machine
  (and its race) has no Rust counterpart. The 3 new `WakeupTriggerTest` tests
  exercise that Java-only edge; skip justified.

- **7b Fetch.forPartition mutable-maps (9945592afc) — N/A HOLDS.** Java's bug: pre-4.3.1
  `forPartition` built an immutable `mkMap(mkEntry(..))`, so a later `Fetch.add(..)`
  merge threw `UnsupportedOperationException`; 4.3.1 switched to `new HashMap<>()` +
  `put`. Rust's `FetchCollector::collect_fetch` accumulates directly into ONE owned
  mutable `IndexMap<TopicPartition, Vec<..>>` (`records_by_partition`) via `.entry()`
  — there is no immutable-singleton `forPartition` and no separate `Fetch.add` merge,
  so the hazard cannot occur. FetchTest×5 (`testAdd*`/`testForPartition*`) exercise
  exactly that folded-away machinery; skip justified.

- **7c import-only / javadoc-only claims — spot-checked, HOLD.**
  - `ClientUtils.java` (−27): removes an unused `createNetworkClient` overload; Rust
    has no such factory overload. N/A holds.
  - `MockConsumer.java`: the two real behavior deltas (partition-level
    `!isPaused && isAssigned` gate; `close()` → `close(CloseOptions.timeout(..))`)
    ARE translated in commit `9a62ef80`, faithfully. (This is NOT an import-only
    file — but it was translated, not claimed N/A, so consistent.)
  - `ConsumerRecords.java` KAFKA-20660: the deltas are the tainted deprecated
    `ConsumerRecords(Map)` ctor detection + periodic `nextOffsets()` warning. Rust
    never had that deprecated single-arg ctor, so N/A holds; ConsumerRecordsTest×3
    `testNextOffsets*` skips justified.
  - `AbstractFetch`/`FetchCollector`/`FetchMetricsManager`/`Metadata`/
    `ConsumerNetworkThread`: `record`→`record.internal` import moves + em-dash /
    javadoc-only edits — no behavior change. N/A holds.

---

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

---

## Findings count

**0 correctness findings; 4 minor non-blocking observations.**

1. (Obs) `maximum_time_to_wait` omits Java's `pollTimer.update` before the
   UNSUBSCRIBED short-circuit — benign, literal-translation note.
2. (Obs) close path reuses the "consumer is unsubscribing" skip message —
   internal-only, never user-surfaced.
3. (Obs) `on_partitions_assigned` (and revoke/lost) receive added/removed
   partitions unsorted (`HashSet`→`Vec`) where Java passes a `SortedSet` —
   pre-existing, not in the 4.2→4.3.1 delta.
4. (Obs) four import-only consumer test-file deltas (`CompletedFetchTest`,
   `FetchCollectorTest`, `FetchRequestManagerTest`, `FetcherTest`) not
   enumerated in the Phase-4 recorded-skip list — documentation completeness.

**Adjudications:** item 6 Issue-9 deviation = DEFENSIBLE + documented (pre-existing,
Java 4.3.1 did not introduce the fatal semantics fresh; the new UNSUBSCRIBED-skip
half IS translated + tested). Item 7 skips (WakeupTrigger, Fetch.forPartition,
ClientUtils/ConsumerRecords/import-only, FetchTest×5, ConsumerRecordsTest×3) all
verified N/A-holds.

**Amendment draft:** the §28/§31 amendment text is in this file (section "PROPOSED
AMENDMENT (human applies)" above).

---

## P1 Critic-64 findings — RESOLVED (Actor 64)

### F1 — BRIDGE + abstract set are hand-lists; fail-closed universe was not "all Java exceptions" — RESOLVED
Fixed in `xtask/src/error_hierarchy.rs`. The generator now scans **every** `…Exception`
(plus the two suffix-less exception) class declared under `common/` and `clients/`
(`scan_exception_classes` / `find_class_decl`) and, in `validate_exception_scan` (called
from `build_graph`), fails the build unless each scanned class is one of: a BRIDGE class,
an abstract base, the covered `KafkaException` base, or an explicit `EXCLUSIONS` entry —
so a new Java exception, or a concrete class the core forgot to give an FFI id, can no
longer be silently invisible. `ABSTRACT_CLASSES` is now **derived-and-checked**: the build
fails if the scan finds an in-scope Java `abstract` exception the list omits, or lists one
Java does not mark `abstract`. `EXCLUSIONS` holds the 10 out-of-scope classes (client
internals, consumer-`internals`, share-consumer, internal network signalling, OAuth Bearer
plugins), each citing why; a stale exclusion also fails the build. Added 5 xtask tests
(`scan_finds_the_expected_exception_classes`, `scan_marks_abstract_classes_abstract`,
`universe_check_passes_on_the_real_tree`, `abstract_set_is_derived_from_java_not_hand_listed`,
`every_exclusion_is_a_real_scanned_class`). `cargo test -p xtask` → 38 pass.

### F2 — `to_ffi_id(KafkaError(...))` silently coerced the base fallback to -1 — RESOLVED
Fixed the Java-faithful way (Java's `KafkaException` has no wire code): the base
`KafkaError` now carries **no** `_ffi_id` (`common/errors/_base.py`), so
`to_ffi_id(KafkaError(...))` raises `TypeError` instead of silently returning -1. The
catch-all wire code -1 (`UNKNOWN_SERVER_ERROR`) belongs to its own concrete class
`UnknownServerError`, which is what `from_ffi_error` builds for id -1 and what a mock injects
— documented on `to_ffi_id` and `_base.py`. Added tests
`test_to_ffi_id_rejects_the_bare_base_kafka_error` and
`test_unknown_server_error_owns_wire_code_minus_one` (pins id -1 -> UnknownServerError both
directions); updated `test_from_ffi_error_chains_cause` to exercise the base fallback via an
unknown id.

### N1 / N2 — owner notes (no Actor action)
N1 (fold six sibling-package classes into `common.errors`) is logged as clarification C3;
N2 (PLAN says the legacy modules are "retired"; kept per C5) is logged as C5. Both are
owner-judgment items for the P1 sign-off, not defects.
