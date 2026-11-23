# Milestone 18 — rules promotion proposal (PLAN §6.4 step 5)

**A proposal only.** Nothing here has been applied: CLAUDE.md and `.claude/rules/` change only when
the user decides (agent-roles.md). Drafted in P11, 2026-11-23.

The milestone piloted eight rules, R1–R8 (`translation-checklist.md`), scoped to share-consumer
files. For each rule this file gives the evidence from the reviews, a recommendation, and for a
promotion the exact text and where it goes. §2 drafts four further rules edits the milestone left
pending.

## Evidence base

- Every `COMMENTS.<N>.md` / `COMMENTS.DONE.<N>.md` of the milestone (Critics 201, 210, 220, 240,
  245, 246, 251, 260, 270, 280, 291, 300, 310). The P2 and P9 reviews (220, 291) sit in their
  parallel worktrees (`-m18-p2`, `-m18-p9`), not in this one.
- `backtest-report.md` (the pre-P1 backtest), PLAN §6–§7, `ledger.md` §4, and the git history of
  `translation-checklist.md` and `.claude/rules/share-consumer.md`.

Totals:

- **20 comments carry a rule tag, and the Actor fixed all 20.** None was disputed or withdrawn,
  and none was recorded as a false positive.
- **Tagging was uneven.** Critics 201, 220, 240, 291 and 310 tagged nothing (Critic 220's only rule mention is the
  resolution line pointing at R4 item 8, written after its Issue 1), so each count below is a lower
  bound. Comments that applied a rule without its tag are listed separately.
- **Over-application:** none in the live reviews. The only rule-driven pressure came in the
  backtest, and it led to the rewording in `5259c6e5f`. The scopes held: Critic 260 Issue 7 and
  Critic 270's consumer-gap verdict declined to apply R1 / R7 to pre-existing shared code.
- **No `COMMENTS.FP.md` exists** (Critic 245 noted it; PLAN §6.3 says it never has). The canary of
  §6.4 step 4 therefore ran on the tags and the Actor replies.
- **Rewordings during the milestone:**
  - `8c00ae7ef`: R7's three sub-rules and its scope.
  - `5259c6e5f`, from the backtest: R1, R3, R4.2, R4.5, R4.6, R4.7 and R7.
  - `84ce72128` added R4 item 8, and `34f0ddde8` made it assert the next call.
  - R2, R5, R6 and R8 were never reworded.

## 1. Per rule

| Rule | Tagged (true / false positives) | Recommendation | Target |
|---|---|---|---|
| R1 Assertion fidelity | 7 / 0 | Promote | DoD item 3 |
| R2 Deferral ledger | 1 / 0 | Promote | DoD item 8 |
| R3 Production-path construction | 1 / 0 | Promote (thin evidence) | new DoD item 13 |
| R4 Callback → async checklist | 4 / 0 (the milestone's only High) | Promote, split | CLAUDE.md §11.6 bullet + new §11.8 |
| R5 Java semantics table | 0 tagged; 3 untagged / 0 | Promote | CLAUDE.md §1.4 |
| R6 `close()` is explicit | 1 / 0 | Promote reworded, merged | CLAUDE.md §11.4 |
| R7 Rules are checked against Java | 7 / 0 | Promote | agent-roles.md item 5 |
| R8 Rust-only types get a test plan | 1 / 0 | Promote reworded | DoD item 7 |

### R1 — Assertion fidelity

**Text.** Every Java assertion in a translated test has a Rust counterpart. A translation does
not swap inputs, widen the outcomes a fixture accepts, or drop an `assertFalse(future.isDone())`,
a request-count or a timing check, unless a deviation note at the test says why. Reworded in
`5259c6e5f`: a Java test that cannot compile in Rust (`testDoubleBuild`) is skipped with a
one-line reason, and no compile-fail test is needed.

**Tagged comments (7, all fixed):**

- 245 Issue 4: a test checked an empty set, which holds without the call, where Java has
  `verify(..assignFromSubscribedAwaitingCallback)`.
- 246 Issue 5: `never().markPendingRevocation` was dropped in three tests.
- 260 Issue 2: `assertNotEquals(cluster)` became `!Arc::ptr_eq`.
- 270 Issue 1: `maybeReconcile(true)` was not pinned; mutating it to `false` still passed.
- 280 Issue 2: a test never reached the code it is named after.
- 280 Issue 5 [R1, R7]: Java's middle cause was dropped.
- 280 Issue 6: `.contains` stood in for Java's exact `verify`, and the error classes were not
  asserted.

**Untagged applications:**

- 220 Issue 4: a test note claimed `bootstrap.servers` was required.
- 310 Issue 1: a callback assertion was checked before `close`.
- 310 Issue 2: `let _ =` on a fallback abort.
- 310 Issue 7: Java retries a describe error that Rust panicked on.
- 210, 291 and 300 diffed whole test classes and found nothing.

R1 produced the most findings in the backtest too (S1-2/3/4, S5-1/2). Its timing clause worked as
intended in 280 verdict 6(b): range assertions, not wall-clock sleeps.

**Recommendation: promote** to DoD item 3. It is the most-used rule, with no false positives.
The text adds the two traps Critic 310 found and the "state check instead of a call check" case
from 245 Issue 4.

Append after the last sub-bullet of `.claude/rules/definition-of-done.md` item 3:

```
   - Every Java assertion has a Rust counterpart, diffed test by test. A translation does not swap Java's inputs (record magic, list order, error class), widen what a fixture accepts, compare by pointer where Java compares by value, replace a `verify(..)` of a call with a state check that also holds without the call, or drop an `assertFalse(future.isDone())`, a request count or a timing check, unless a deviation note at the test says why. A Java timing assertion may be adapted to the Rust test clock (a range or a paused clock) with a note; it is never ported as a wall-clock sleep. A Java test that cannot compile in Rust (it relies on a runtime type error the Rust signature rules out) is skipped with a one-line reason; no compile-fail test is needed.
   - Two places where an assertion can hide. Where the Java callback runs on the calling thread and `close` rethrows its errors (the share consumer's acknowledgement callback, the consumer's rebalance listener), an assertion inside it also covers the callbacks `close` runs, so a Rust test that records the outcome checks it after `close`; this does not hold for a callback Java runs on another thread (a producer `Callback` on the Sender thread). And where Java's fallback call inside `catch` is unguarded, its Rust translation propagates the fallback's error instead of discarding it with `let _ =`; where Java guards it (`Utils.closeQuietly`, `Utils.swallow`, a nested `try`), `let _ =` is the faithful translation.
```

### R2 — Deferral ledger

**Text.** "Deferred to Phase N", "documented carry-over", a stub closure or a setter production
never calls counts as a FIXME (DoD §8). Each one gets a row in the deferral ledger, and a phase
cannot close while it owns an open row. Never reworded. Backtest self-check 10 (an opt-in
diagnostic is not a deferral) was not folded in; the proposal below adds it.

**Tagged comment (1, fixed):** 300 Issue 8: the proposed `AcknowledgeType::Renew` contract entry
and its baseline line had no ledger owner.

**Untagged applications:**

- 310 Issue 3: skip reasons were stated as impossibilities, with no deferral row.
- 310 Issue 4: there was no 4.4 CI leg and no owner row. The user settled both with D10.
- 280 verdict 6(c) produced the P8 row for the four-parameter `acknowledge`.

The ledger (`ledger.md` §4, 18 rows) was P11's audit list. Every row is closed or owned: three by
the share-consumer FFI milestone, two by the user.

**Recommendation: promote** into DoD item 8. The failure it prevents, C5-09's "deferred to Phase
11" that nobody picked up, is not specific to this milestone.

Replace `.claude/rules/definition-of-done.md` item 8 with:

```
8. Are there any TODO or FIXME left in the code? In case finish everything that should be done before considering the change done. "Deferred to Phase N", "documented carry-over", a stub closure, a setter production never calls, and a skip reason that waits on someone (a user decision, a later milestone) count as a FIXME too: each one gets a row in the milestone's deferral ledger (item, where in the code, owning phase or person, reason), and a phase does not close while it owns an open row. A CLAUDE.md §7 "fail with an explicit `Error`" stub for an out-of-scope Java path is allowed and still gets a row; an opt-in diagnostic is not a deferral.
```

### R3 — Production-path construction

**Text.** Every config key and every Java constructor parameter the share code reads is exercised
by at least one test that builds the object through its production constructor. Reworded in
`5259c6e5f`: a test-only setter is allowed when it is the faithful port of a Java test-only
override, with a note.

**Tagged comment (1, fixed):** 245 Issue 3: the RackId send-once path was never exercised with a
rack set. That is the C2-07 pattern, which the consumer audit found in the consumer.

**Untagged applications:**

- 300 (a nit): the network-client test now builds through `KafkaShareConsumer::new`.
- P7 added three R3 tests, including
  `new_passes_the_group_id_and_the_rack_id_to_the_share_managers`.

**Recommendation: promote** as a new DoD item. The evidence is thin, but there were no false
positives, and it caught the same class of bug the audit found in the consumer.

Insert after `.claude/rules/definition-of-done.md` item 12:

```
13. **Production-path construction**: every config key and every Java constructor parameter the translated code reads is exercised by at least one test that builds the object through its production constructor (the one production code calls), driven over `MockClient` or the existing mock network fixture, not through a test-only setter or a hand-built field. A test-only setter is allowed only as the faithful port of a Java test-only override (a subclass or anonymous override in the Java test), with a note at the setter. This asks for neither broker-backed unit tests nor new test seams in production code; pure value types whose only constructor is the test's are exempt.
```

### R4 — Callback → async checklist

**Text.** Eight items:

1. Read the clock at completion.
2. Keep completion order.
3. Wake the background task on re-enqueue.
4. Don't collapse a chain so that `isDone()` timing changes.
5. Check lock order (ABBA).
6. A translated thread loop yields.
7. Reason about racing continuations.
8. A Java `finally` around an awaited call needs a drop guard, with a test that asserts the next
   call.

**Rewordings:**

- `5259c6e5f`:
  - item 2 flags a spawn only when its order is observable and not restored downstream;
  - item 5 covers unsynchronized getters (the #277 miss);
  - item 6 exempts loops that await I/O and per-record loops;
  - item 7 is new (`current_thread` is no exemption; this was C2-05's wrong withdrawal).
- `84ce72128` added item 8, from 220 Issue 1.
- `34f0ddde8` made item 8 assert the next call, from Critic 251.

**Tagged comments (4, all fixed):**

- **251 Issue 1 (High):** a dropped `fetch_records` turned the record being decoded into a gap,
  which the broker archives.
- 251 Issue 2: a dropped `take_renewals` lost the renewals.
- 280 Issue 1 [R4.8, R6]: a dropped `close` marked the consumer closed and leaked the background
  task.
- 280 Issue 3: the explicit-mode dropped-`poll` path had no test.

**Untagged applications:**

- 220 Issue 1: `entered_callback` was not reset when the future was dropped (it led to item 8,
  `84ce72128`).
- 251 Issue 3: a stale `delivered_batch` after a drop.
- 280 Issues 7–10: a retried `close` resent the acknowledge-on-close, lost its first error, or
  ignored its own timeout.
- 300 Issue 9: a dropped `close` leaves wakeups disabled.
- 260 verdict (c) used item 7 to reject a per-request spawn.
- The lock-order audits in 251, 260, 270 and 280 found no ABBA.

By severity this is the most productive rule: the comments it covers, tagged and untagged,
hold the milestone's only High (251 Issue 1) and three Mediums (251 Issue 2, 280 Issues 1 and 7);
the rest are Low. No false positives. Not folded in yet: the backtest's R4.4 near-miss, where an error
inside a chain skips a sibling step (the #279 bug). The text below adds it to item 4.

**Recommendation: promote, split by kind.** Item 8 and the close-resume lessons from 280 become a
Tokio-pitfall bullet in CLAUDE.md §11.6. Items 1–7 become a new CLAUDE.md §11.8.

Append under CLAUDE.md §11.6, after the `MutexGuard` bullet:

```
       - Any future can be dropped at any `.await` (a caller's `tokio::time::timeout` or `select!`). A Java `finally`, or any state reset or "done" flag after a call that becomes an `.await`, needs a drop guard or is set only after the step completes, so that the next call resumes the interrupted work instead of skipping or repeating it. Test it by dropping the future mid-await **and asserting what the next call returns** (and what it sends or acknowledges), not only what the guard restored. A `close()` dropped part-way leaves the object closable: it is marked closed only after Java's last step, keeps its progress and first error for the retry, bounds the retry by the retry's own timeout, and never resends a step Java runs once. This `close()` part binds new code and any `close` a change touches; existing code is not swept (`AsyncKafkaConsumer::close` runs every step again on a retry, PLAN §8.1 row 19, a consumer follow-up).
```

Insert after CLAUDE.md §11.7:

```
    8. **Callback → async checklist.** When a Java callback, `CompletableFuture` chain or thread loop becomes async Rust, check each of:
       1. the clock is read at completion, not when the task is spawned;
       2. completion order is preserved where it is observable: an unordered per-callback `spawn` is a defect only when nothing downstream restores Java's order, so trace that first;
       3. work re-enqueued for a background task wakes it, through the component's own wake primitive (in the consumer `ApplicationEventHandler::wake_background_task`, never `WakeupTrigger`, `consumer-threading.md` §31);
       4. collapsing a chain neither changes when `isDone()` becomes true nor lets an error inside the chain skip a sibling step Java still runs;
       5. lock order: for every pair of locks held at the same time, both acquisition orders are checked across all paths (ABBA), including a lock taken inside a getter Java leaves unsynchronized, and every nested path where a reentrant Java monitor becomes a non-reentrant `std::sync::Mutex`;
       6. a loop translated from a Java thread loop that can iterate without awaiting anything that can pend yields, and is tested on a `current_thread` runtime; per-record decode or send loops and loops that await I/O every iteration are exempt (never `yield_now()` there);
       7. interleavings are reasoned about at every `.await` and at loop-iteration boundaries; a `current_thread` runtime is not an exemption, as a spawned continuation still interleaves with the loop at each await.
```

### R5 — Java semantics table

**Text.** A table of translations:

| Java | Rust |
|---|---|
| `LinkedHashMap` | `IndexMap` + `shift_remove` |
| `TreeMap` / `TreeSet` | `BTree*` |
| sorted output | stays sorted |
| clamping arithmetic | saturating arithmetic |
| `addExact` | returns an error |

It also covers `String.trim`, `Matcher.matches` and case-sensitivity rules. The rule was never
reworded.

**Tagged comments:** none.

**Untagged applications (all fixed):**

- Actor 260 cited R5 for the `SubscriptionState` `LinkedHashMap` order. That became consumer
  behaviour change `50e83a71f`.
- 260 Issue 6 (Medium): Java's `TreeSet` order was lost through a `HashSet` hop.
- 260 Issue 7 (in part): `LinkedHashMap` order in tests.

In the backtest, R5 caught C3-01 (`swap_remove`) and C2-04 blind, with no false positives. Its
self-checks 5 and 6 show the guards are needed: "don't replace a `HashMap`" and "saturate only
where Java clamps". C2-04 (listener callbacks get `HashSet` order) is still open in the consumer.

**Recommendation: promote** to CLAUDE.md §1, where the `java.util` equivalents already live.

Insert after CLAUDE.md §1.3:

```
    4. **Same behaviour** includes Java's collection and value semantics; translate them exactly:

       | Java | Rust |
       |---|---|
       | `LinkedHashMap` / `LinkedHashSet` (incl. `remove`) | `IndexMap` / `IndexSet` + `shift_remove` (never `swap_remove`) |
       | `TreeMap` / `TreeSet` / `SortedSet` | `BTreeMap` / `BTreeSet`, or a `Vec` sorted by Java's comparator |
       | a sorted output (e.g. a `TreeSet` handed to a listener or the next step) | stays sorted across every hop, with no `HashSet` in between |
       | `long` ms arithmetic that Java clamps | saturating arithmetic; test `Duration::MAX` |
       | `Math.addExact` and other throwing arithmetic | return the error |
       | `String.trim` | trims chars `<= ' '`, not `str::trim` (Unicode whitespace) |
       | `Matcher.matches` | anchored full match, not `Regex::find` |
       | `equalsIgnoreCase` / case-sensitive `equals` | the same case rule |

       Tests use non-trivial orders, not only ascending input. Do not replace a Java `HashMap` with an ordered map "to be safe", and saturate only where Java clamps. Existing code is not swept for this: it is fixed when a change touches it, in a commit labelled as a behaviour change.
```

### R6 — `close()` is explicit

**Text.** Every step of Java's `close()` happens in Rust's `close()`, in Java's order; a `Drop`
safety net may stay. Never reworded.

**Tagged comment (1, fixed, shared with R4.8):** 280 Issue 1: Java sets `closed` only after the
join. Backtest self-check 7 warned against applying the rule to `ConsumerNetworkThread::cleanup`;
that did not happen in the live reviews.

**Recommendation: promote reworded, merged** into CLAUDE.md §11.4, the join/close rule. Its
cancellation half is already in the R4 §11.6 bullet. As a standalone rule it is too narrow: one
audit finding and one comment.

Append to CLAUDE.md §11.4:

```
       Likewise every step of Java's `close()` is done in Rust's `close()`, in Java's order; a `Drop` impl may stay as a safety net, but Java's steps do not live there.
```

### R7 — Rules are checked against Java

**Text.** A rule's claim about Java cites `Class#method` and is verified against the source.
Three kinds of claim need the method body read. If Java contradicts a rule, the agent escalates.

**Rewordings:**

- `8c00ae7ef` added the three sub-rules (calls; exists / overrides; quantified → explicit list),
  from Critic 201 Issues 1, 3 and 4.
- `5259c6e5f` extended the rule to deviation notes in code and tests (backtest S5-1).

**Tagged comments (7, all fixed):**

- 245 Issue 2: a method-coverage row put the cache trim in the wrong place.
- 245 Issue 5: a false `assert_fence_is_no_op` note.
- 251 Issue 5: a compressed-stream note misstated Java.
- 251 Issue 9: a note said zstd reads its header when opened; it does not.
- 260 Issue 5: a note cited "Java's `DisconnectException.INSTANCE`", but the message differs.
- 280 Issue 5.
- 280 doc nits.

**Untagged applications:**

- 201 Issues 1, 3, 4, 6, 9 and 10: unchecked claims about Java in the plan and the rules.
- 220 Issue 2: a doc said a callback panic is not caught, and it is; 220 Issue 4.
- 270: false phase-order comments, fixed in `c0ddf76bb`.
- 300 Issue 7: the proposed `Renew` entry overstated when `poll` returns the error.
- 310 Issue 3.

**Recommendation: promote** to agent-roles.md. It binds every role and every artifact: rules,
plans, review comments and code notes. It does not duplicate documentation.md §7, which covers only
the citation form.

Add after `.claude/rules/agent-roles.md` item 4:

```
5. Every role: a claim about Java — in a rule, a plan, a review comment, or a deviation note in code or tests — cites the Java member (`Class#method`, documentation.md §7) and is checked against the Java source the CLAUDE.md "Source Reference" names. Three kinds of claim need the method body read, not just its name:
   - "X does / does not call Y" names X, the method whose body was read;
   - "X exists" / "X overrides Y" is checked in the class itself, not inferred from a base class or a sibling;
   - a quantified claim ("every method …", "all paths …") is written as an explicit list of the Java methods it covers.

   A deviation note stays true as the code changes; the Critic reports a stale or false one. When Java contradicts a rule, the agent escalates to the Manager (or the user) and neither follows the rule nor rewrites it alone. This covers claims of Java behaviour, not the choice behind a documented deviation that carries a *Why*.
```

### R8 — Rust-only types get a test plan

**Text.** A type with no Java body to diff needs an explicit test plan in the phase PLAN, reviewed
by the Critic before the code is written, including the mock variant. Never reworded. Applied in
PLAN §4.1 (`926cdd59b`): `DynShareConsumer`, `ShareConsumerHandle`, `AcknowledgeOptions`.

**Tagged comment (1, fixed):** 300 Issue 5: the D5 test pinned one of five forwards, and the
mock's constants masked the rest. The fix mutation-checked all seven forwards.

**Untagged application:** 260 Issue 3: Rust-only tests did not pin the behaviour they were named
after.

**Recommendation: promote reworded** into DoD item 7, which already covers types with no Java
counterpart.

Append to `.claude/rules/definition-of-done.md` item 7:

```
   A type with no Java body to diff (a handle, an adapter, a dyn companion, an options struct) also gets a test plan in the change's plan, reviewed by the Critic before the code: each behaviour the type must have and the test that pins it — one that fails when that behaviour is removed (a forward through a mock that returns a constant pins nothing) — including the mock variant. A small private helper inside a translated method needs none.
```

### Process note

PLAN §6.3 already says `COMMENTS.FP.md` / `COMMENTS.FN.md` "have never existed: create them per
milestone or drop the step". The canary ran without them. If the team keeps
pilot-then-promote, agent-roles.md item 2's FP/FN sentence needs the same decision.

## 2. Further pending rules edits

### 2.1 `.claude/rules/share-consumer.md`: "Pending decisions" are settled

Replace everything from `## Pending decisions` to the end of the file with:

```
## Settled decisions

All share-specific decisions are settled and recorded in
`design/current/Milestone-18-share-consumer/PLAN.md` §7. They are rules for this
module:

  - **D1** (2026-11-18): the Admin share-group RPCs are in scope; translated in P9
    (`rust/src/admin/*share_group*`, the five share-group handlers, the `Admin`
    share methods; `MockAdminClient` returns Java's "Not implemented yet").
  - **D2** (2026-11-20): the KIP-714 methods (`clientInstanceId`,
    `registerMetricForSubscription`, `unregisterMetricFromSubscription`) and
    `MockShareConsumer#setClientInstanceId` are left out, as on `Consumer`; the
    6 `KafkaShareConsumerMetricsTest` cases over them are skipped with that reason
    (ledger §2.1). Implemented in P8 (`rust/src/consumer/share_consumer.rs`).
  - **D3** (2026-11-20, replacing the 2026-11-18 four-PR split): one milestone PR.
    Its body lists the consumer behaviour changes (PLAN §8), since fixup messages
    are lost on squash.
  - **D5** (2026-11-20, option (c), provisional pending team review): each in-flight
    batch keeps one refcounted `Bytes` handle on its raw record batches and
    re-decodes and re-deserializes a renewed record when it is delivered again,
    copying the batch's bytes only once its sole in-flight records are renewed
    ones. The scheme stays private to `ShareInFlightBatch`
    (`rust/src/consumer/internals/share_in_flight_batch.rs`, P3a). The user-visible
    effect is the proposed contract change `crate::consumer::AcknowledgeType::Renew`
    in `design/current/doc-contract-changes.md`, awaiting acceptance.
  - **D6** (2026-11-20): a Rust-only `ShareConsumerHandle`
    (`rust/src/consumer/share_consumer_handle.rs`, `Clone + Send + Sync`, `wakeup`
    only), obtained with `ShareConsumer::handle`, as `Consumer::handle`. Implemented
    in P8; R8 test plan in PLAN §4.1.
  - **D7** (recorded 2026-11-20): `acknowledge*` misuse returns
    `Result<(), Error>` with Java's `IllegalStateException` texts as
    `LocalIllegalStateError` (CLAUDE.md §12.2), not a panic. Implemented in P7
    (`ShareFetch::acknowledge*`, `ShareConsumerImpl`) and P8
    (`ShareConsumer::acknowledge_with_record`, `_with_record_type`,
    `_with_options`).
  - **D8** (2026-11-18): the §3 `AcknowledgementCommitCallback` shape.

D9 and D10 (the P10 harness: JMX meters, broker-version gates, the Java test jars
and the 4.4 CI leg) are test-infrastructure decisions; see PLAN §7.1.
```

The same file still treats these decisions as open in §9 item 10 and in several inline
references. Proposed replacements:

| Where | Replacement |
|---|---|
| §9 item 10 | `**Settled decisions** (below): code follows them; flag a deviation from one.` |
| §1 table, `acknowledge*` row | `sync, Result<(), Error> (D7)` |
| §1 table, KIP-714 row | `not translated (D2)` |
| §1 text on `acknowledge*` | `For acknowledge*, D7: Result<(), Error> with Java's texts.` |
| §3 / §8 on how `wakeup` is reached | `… is D6: through a ShareConsumerHandle.` |
| §4 on record retention | `… is decision D5 (option (c), see Settled decisions).`; drop the anti-pattern "introduced before D5 is decided" |
| §7 on misuse | `The misuse cases return Result (D7), with Java's message text.` |

### 2.2 `.claude/rules/documentation.md` §8: an `errors` line for a proposed contract change

The lint keeps a Java `@throws` that a `WARNING:` names only for an **accepted** entry:
`check_item` in `rust/xtask/src/lint_custom/api_docs.rs` filters `gaps.throws` only when the entry
is accepted, and `test_accepted_warning_accounts_for_a_throw` asserts that a proposed entry leaves
both findings. So `crate::consumer::ShareConsumer::poll`, whose `WARNING:` names
`ArithmeticException`, needs an `errors` line and a `warning` line in `rust/xtask/doc-baseline.txt`.
`AcknowledgeType::Renew`, which involves no throw, needs only the `warning` line. §8 currently
allows only the `warning` line.

In §8, replace "New code never adds lines to it, except a `warning` for a proposed contract change
(§6)." with:

```
New code never adds lines to it, except for a contract change proposed but not yet accepted (§6): its `warning` line, and an `errors` line when its `WARNING:` names a Java `@throws` the item no longer keeps (§4 counts that throw as documented only once the entry is accepted). Both lines are removed when the user accepts the entry; until then they match a finding, so they do not go stale.
```

In §6, flow step 2, replace "The agent leaves the `warning` finding in the baseline (§8)." with:

```
2. The agent leaves the `warning` finding in the baseline, and the `errors` finding for a Java throw the `WARNING:` names (§8).
```

### 2.3 `.claude/rules/admin-client.md` §9: cite the Java method, not the line

§9 says every "Not implemented yet" site "MUST cite the exact Java line that throws". That
conflicts with documentation.md §7: no Java file:line in comments; cite `Class#method`. The code
already follows §7: `rust/src/admin/mock_admin_client.rs` cites `MockAdminClient#createPartitions`,
`#deleteRecords`, `#describeProducers`, `#getResourceDescription` and the five share methods.

Replace the sentence "This is a faithful translation of the Java behavior, not a scope deferral,
and every such site MUST cite the exact Java line that throws so the claim is verifiable." with:

```
This is a faithful translation of the Java behavior, not a scope deferral, and every such site MUST cite the Java method that throws, by member name and never by file line (documentation.md §7) — e.g. `org.apache.kafka.clients.admin.MockAdminClient#createPartitions`, or the helper that throws when it is not the method itself (`MockAdminClient#getResourceDescription`) — so the claim is verifiable.
```

### 2.4 CLAUDE.md: two lines the share consumer made stale

- §2 "Public types" (the `Send` and `Sync` bullet) reads "the consumers (`KafkaConsumer`,
  `MockConsumer`, `DynConsumer`, and the share consumers once translated) … and `ConsumerHandle`
  is their `Sync` cross-thread view". Proposed: "the consumers (`KafkaConsumer`, `MockConsumer`,
  `DynConsumer`, `KafkaShareConsumer`, `MockShareConsumer`, `DynShareConsumer`) … and
  `ConsumerHandle` / `ShareConsumerHandle` are their `Sync` cross-thread views".
- Python Binding Conventions, the config-classes bullet, reads "`ShareConsumerConfig` is not
  translated while the share consumer is not." The Rust share consumer exists, and its binding
  is still deferred. Proposed: "`ShareConsumerConfig` is not translated while the share consumer
  has no C FFI (a later milestone)."
