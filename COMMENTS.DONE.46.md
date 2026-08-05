# Critic 46 — resolved (pass 1)

All seven pass-1 findings are fixed in the fixup commit that accompanies this file.
Verification per finding is in the Actor's report; the load-bearing ones are
mutation-checked (both new lost-join tests fail with the pre-fix line restored, and all
three `testCloseIsForcedOn*` still pass — the Critic's issue-6 analysis reproduced rather
than taken on trust).

**One partial dispute.** The adjudication section's stronger argument for the
`prepare_transaction` refusal — "there is **no `recordPrepareTxn` method anywhere in the
class**" — is factually wrong: it exists at `KafkaProducerMetrics.java:124`. The
*conclusion* stands and the refusal is unaffected; the true and stronger fact is that the
recorder has **zero callers** anywhere in `kafka/` (one occurrence, its own declaration)
against `recordInit`'s three. PLAN §10.9 deviation 2 records the corrected form with both
greps, rather than propagating the claim as relayed.

Original review follows verbatim.

---

# Critic 46 — Milestone 11 Phase 6 (public producer txn API + Sender transactional loop)

Reviewed `c59c09d..9ed5a34` (6 commits). Pass 1: **7 findings** — 0 behavioural in
production code, 5 record/accounting defects, 2 missing tests. The four adjudications
requested all resolve **in the Actor's favour**; the defects are in the records and the
fresh accounting, matching the Phase 5a/5b pattern.

Verified clean and deliberately not filed: see "Adjudications and cleared checks" at the end.

---

## Issue: `sender.rs` module doc still says Phase 6's produce-request wiring is deferred
- **File**: `src/producer/internals/sender.rs:43-44`
- **Severity**: Record Defect
- **Java Reference**: `Sender.java:922-927` (`transactionalId` / `useTransactionV1Version`), `:930`
- **Description**: The module-level doc closes with

  ```
  //! Still deferred: `sendProduceRequest` does not yet set `transactional_id` /
  //! `use_transaction_v1_version` (`Sender.java:922-936`, Phase 6).
  ```

  Phase 6 implemented exactly that, in this same file, at `sender.rs:2248-2273`
  (`git log -S use_transaction_v1_version` → `53d9699`, one of the commits under review).
  `git log c59c09d..9ed5a34 -S "Still deferred" -- src/producer/internals/sender.rs`
  returns nothing, so the doc was never revisited. The file now opens by declaring
  undone the work it performs 2200 lines later.
- **Expected**: Delete the "Still deferred" paragraph, or restate it as done, in the same
  commit that landed the wiring. Note also that the deferral cites `Sender.java:922-936`
  while the code comments below it (correctly) split the range into "Java 922-928" and
  "Java 930-936" — pick one.
- **Actual**: A stale deferral notice contradicting the code beneath it. This is the
  stale-justification class §10.9 deviation 2 names for itself ("a claim about the Java
  source that nobody re-derived"); here it is a claim about the *Rust* source.

---

## Issue: PLAN §9.21's hand-forward still promises `prepare_transaction` to Phase 6
- **File**: `design/history/Milestone-11/PLAN.md:2109-2111` (§9.21, "Handed forward")
- **Severity**: Record Defect
- **Java Reference**: `KafkaProducer.java` (4.2, tag `4.2.0`) — no `prepareTransaction`
- **Description**: §Phase-6's spec line was correctly struck through and annotated
  ("**Wrong, corrected in Phase 6.**"), and §10.9 deviation 2 records the evidence. But
  §9.21's hand-forward sentence is unchanged:

  > Handed forward: 47 `TransactionManagerTest` methods to Phase 8 …, 18 transactional
  > `SenderTest` rows to Phase 6 (rationale-expired evidence), **`prepare_transaction`'s
  > public surface to Phase 6**.

  Phase 6 refused that hand-forward with reason. A reader arriving at §9.21 — the
  natural entry point for "what did Phase 5 owe Phase 6?" — is told Phase 6 owns a
  surface that Phase 6 established does not exist. The brief's own framing applies: a
  refused spec line with no record at *every* site is how §9.15-class drift starts.
- **Expected**: Amend §9.21's hand-forward to record the refusal and point at §10.9
  deviation 2. The same sentence's "18 transactional `SenderTest` rows to Phase 6" is
  also now misleading — the group is 15 (see the adjudication note below); 18 was the
  pre-Phase-5a count and the block itself says so.
- **Actual**: Two of the three hand-forward clauses are stale, in the section whose
  status line reads "DONE — closed on clean passes".

---

## Issue: `senderThreadShouldNotGetStuckWhenThrottledAndAddingPartitionsToTxn` is accounted for nowhere
- **File**: `src/producer/internals/sender.rs:7565-7793` (transactional `SenderTest` accounting block)
- **Severity**: Missing Requirement (DoD §3)
- **Java Reference**: `SenderTest.java:507-508` (`@Test`, live, not `@Disabled`)
- **Description**: This is a transactional `SenderTest` method by the block's own scope
  criterion — it builds a transactional manager at `SenderTest.java:515`
  (`new TransactionManager(logContext, "testUnresolvedSeq", 60000, 100, apiVersions, false)`),
  then `setupWithTransactionState`, `doInitTransactions`, `txnManager.beginTransaction()`
  (`:530`), `txnManager.maybeAddPartition(tp0)` (`:531`), `sender.runOnce()`.

  It has **no Rust counterpart and no justification anywhere**:

  ```
  $ grep -rn "senderThreadShouldNotGetStuck\|sender_thread_should_not_get_stuck\|\
  throttled_and_adding\|ThrottledAndAddingPartitions" src/ tests/ design/ COMMENTS*.md .claude/ | wc -l
  0
  ```

  **Why the shipped completeness check cannot see it.** The scope program's splitter
  requires the `test` name prefix (`/^    (public|private) void test/`). This method
  begins `senderThread`, so it is never emitted. It is the **only** annotated test in
  `SenderTest.java` whose name does not start with `test` — I enumerated all 76
  `@Test`/`@ParameterizedTest` methods and it is the single exception, so this is one
  unique blind spot rather than a broad class. Widening the splitter to
  `void [A-Za-z0-9_]+\(` raises the in-scope set from 52 to 57; four of the five
  additions are private helpers (`assertPartitionState`, `doInitTransactions`,
  `setupWithTransactionState`, `waitForProducerId`) correctly excluded, and the fifth is
  this test. **The in-scope count is 53, not 52.**

  The block's load-bearing `comm -23` "nothing in scope is unplaced" check is **vacuous
  for this method**: both sides are filtered by the same `test`-prefix assumption (Java
  side the `void test` splitter, Rust side the `` `test[A-Za-z]+` `` grep), so a method
  absent from the Java list can never surface as unplaced. The block's *prose* criterion
  ("every `SenderTest` method whose body references a `TransactionManager`") includes it;
  its *program* does not — and the program produced the number.
- **Expected**: Either translate it, or add it to the owed group with a reason and an
  owner. A plausible reason already exists and should be stated rather than assumed: it
  needs `client.advanceTimeDuringPoll(true)` (`SenderTest.java:511`), which has no Rust
  counterpart (`grep advance_time src/mock_client.rs` → nothing). Its other needs are
  already present (`MockClient::throttle` at `mock_client.rs:242`, node-keyed
  `poll_delay_ms` at `:497`), so it belongs beside the two entries already marked
  blocked-on-named-missing-surface. Also widen the splitter so the check can see it, and
  correct 52 → 53.
- **Actual**: A live transactional Java test with no translation, no entry, no owner, and
  a completeness check structurally incapable of reporting it.

---

## Issue: `testPartitionAddedToTransaction` is unaccounted — the denominator is 28, not 27
- **File**: `src/producer/kafka_producer.rs:4015-4137` (PHASE-6 TEST ACCOUNTING block)
- **Severity**: Missing Requirement (DoD §3) + missing coverage
- **Java Reference**: `KafkaProducerTest.java:2422-2443` (`@Test`, live)
- **Description**: The block claims "All 27 transactional `KafkaProducerTest` methods are
  accounted for". The 22 + 1 + 4 = 27 partition is arithmetically correct and **every
  disposition inside it is factually correct** — both shipped derivations execute, the
  awk prints the 28 rows pasted, the `grep -c` prints 4, the three pasted lists rebuild
  byte-identically, and an independent splitter converges on the same 27. The defect is
  the **denominator**.

  `testPartitionAddedToTransaction` is transactional and unaccounted:

  ```java
  assertEquals(future, producer.send(record));
  assertFalse(future.isDone());
  verify(ctx.transactionManager).maybeAddPartition(topicPartition);
  ```

  `grep -rn "PartitionAddedToTransaction\|partition_added_to_transaction"` excluding
  `kafka/` → **0 hits**: not translated, not named, not justified, not deferred.

  It is squarely in Phase 6's declared surface — its subject is `doSend` →
  `transactionManager.maybeAddPartition(tp)`, inside the Phase 6 table's
  `KafkaProducer.java 956–990` range. **And the gap is real coverage, not only
  bookkeeping:** the Rust send path does make that call
  (`kafka_producer.rs:1080`), and it is the *only* reference to `maybe_add_partition` in
  the whole file — no test pins the producer-level wiring. `maybe_add_partition` is
  well covered at the `TransactionManager` level, which is a different assertion.

  **Why the marker set misses it:** it drives the transactional path through
  `KafkaProducerTestContext`, which injects `mock(TransactionManager.class)`
  (`:2601`, passed `:2672`), so it never names a public transactional method or
  `TRANSACTIONAL_ID_CONFIG`. The marker set omits `TransactionManager` /
  `maybeAddPartition`, making mock-injected transactional tests invisible. It is the only
  test in the file asserting on `ctx.transactionManager`; the two other
  `KafkaProducerTestContext` users (`testTransactionV2Produce`,
  `testTransactionV2ProduceWithConcurrentTransactionError`) do not, and are already in
  the 27 — which is why the omission is exactly one.
- **Expected**: 28, with this method translated or named-and-justified. PLAN already
  names "the one method given `mock(TransactionManager)`" as a deferral class, but that
  note is scoped to `SenderTest` and does not cover this method — reuse the reason
  explicitly rather than leaving it inferable. Add `maybeAddPartition` /
  `TransactionManager` to the marker set so the criterion matches its prose.
- **Actual**: A live transactional Java test absent from every artifact, and an untested
  production wiring it exists to pin.

  Two prose-vs-program mismatches in the same block, worth fixing while it is open:
  - The criterion prose says "one of the **three** transactional config keys"; the marker
    set contains **one** (`TRANSACTIONAL_ID_CONFIG`), and the file references only two
    such constants at all. The narrow program is **correct** — adding
    `ENABLE_IDEMPOTENCE_CONFIG` yields 36 rows and pulls in 8 non-transactional tests
    (`testMetadataFetch`, `testMetadataExpiry`, `testMetadataTimeoutWith*`,
    `testMetadataWithPartitionOutOfRange`, `testTopicRefreshInMetadata`,
    `testFlushCompleteSendOfInflightBatches`, `shouldNotInvokeFlushInCallback`) that
    merely set `enable.idempotence=false`. So the prose is what is wrong; "three"
    describes no realizable marker set.
  - The prose says "`emit` walks `MARKERS` in declaration order", but there is no `emit`
    function in this block's program (the logic is inline; `emit` belongs to the sibling
    `sender.rs` / `transaction_manager.rs` blocks). The substance holds — the inline
    `for (i=1;i<=n;i++)` does walk declaration order and output reproduces — but the
    named artifact does not exist here.

---

## Issue: §10.9 deviation 1's app-side call sites pair three of four line numbers with the wrong method
- **File**: `design/history/Milestone-11/PLAN.md` §10.9 deviation 1; `src/producer/kafka_producer.rs:141-144`; `src/producer/internals/sender.rs:276-280`
- **Severity**: Record Defect
- **Java Reference**: `KafkaProducer.java:652, 740, 783, 818`
- **Description**: The four `KafkaProducer.java` line numbers are correct **as a set**,
  but three are attached to the wrong method name, identically in all three artifacts.
  Ground truth (`grep -n "transactionManager\.<m>" KafkaProducer.java`):

  | call statement | true line | §10.9 attaches that line to |
  |---|---|---|
  | `transactionManager.initializeTransactions(false)` | 652 | `initTransactions` (653) — correct |
  | `transactionManager.sendOffsetsToTransaction(..)` | 740 | `commitTransaction` (741) — **wrong** |
  | `transactionManager.beginCommit()` | 783 | `abortTransaction` (784) — **wrong** |
  | `transactionManager.beginAbort()` | 818 | `sendOffsetsToTransaction` (818) — **wrong** |

  The names are rotated one position against the numbers. The `TransactionManager.java`
  targets are all **correct** (`initializeTransactions` 299, `beginCommit` 353,
  `beginAbort` 361, `sendOffsetsToTransaction` 404), which is what makes each arrow false
  as written: line 741 sits inside `sendOffsetsToTransaction`'s body and does not call
  `beginCommit`; 784 sits inside `commitTransaction` and does not call `beginAbort`; 818
  sits inside `abortTransaction` and does not call `sendOffsetsToTransaction`.

  This is not citation slop of the ±1-3 line kind: each wrong citation points into a
  **different method**, which is the class that has been substantive in this milestone
  before (Phase 4 pass 3). It matters more than usual because deviation 1 is the stated
  evidence for a proposed **rules §2 amendment** — the wrong pairings would be copied
  into the rules file, sending the next reviewer to the wrong method to confirm a
  premise the rules then depend on.
- **Expected**: Repair the pairing in all three places: `initTransactions` 652,
  `sendOffsetsToTransaction` 740, `commitTransaction` 783, `abortTransaction` 818 (or the
  +1 body lines consistently, but paired correctly).
- **Actual**: `initTransactions :653, sendOffsetsToTransaction :818, commitTransaction
  :741, abortTransaction :784` (`kafka_producer.rs`) and the same rotation in `sender.rs`
  and PLAN §10.9.

---

## Issue: the lost-join fix is not pinned by any test, and the cited tests do not discriminate
- **File**: `src/producer/kafka_producer.rs:1365-1377`; tests at `:3968`, `:3980`, `:4002`
- **Severity**: Missing Requirement (missing regression test) + incorrect claim
- **Java Reference**: `KafkaProducer.java:1414-1418`; CLAUDE.md §9.4
- **Description**: The fix itself is **correct** — `tokio::time::timeout(timeout, &mut
  join_handle)` plus restore-on-expiry, and `JoinHandle` is `Unpin` so `&mut` suffices.
  The pre-fix line (`dd2b374`) was
  `Some(join_handle) => tokio::time::timeout(timeout, join_handle).await.is_ok()` with
  the handle already `.take()`n from the mutex, so expiry dropped it and left
  `await_sender_handle_indefinitely` with `None`. Real bug, right fix.

  What does not hold is the coverage claim. `dd2b374`'s message says "The three
  `testCloseIsForcedOn*` tests are what made it observable", and §10.9 deviation 3
  repeats "Found by the three `testCloseIsForcedOn*` tests, the only tests that reach the
  force-close-after-timeout path." Those three tests all funnel into
  `assert_close_forces_pending_transactional_request`, whose only discriminating
  assertion is:

  ```rust
  assert!(elapsed < Duration::from_secs(5), "close must be forced after its 1000 ms timeout, …");
  ```

  With the bug reintroduced, `await_sender_handle` returns `false` →
  `sender_still_alive = true` → `force_close()` → `await_sender_handle_indefinitely()`
  finds `None` and returns **immediately**, so `close` returns *sooner* and `elapsed <
  5s` still passes. The second assertion cannot discriminate either: it is guarded by
  `if let Ok(joined) = timeout(500ms, init)`, and `init_transactions` is parked on its
  own `max.block.ms` (60 s by default) in both variants, so the arm is skipped either
  way. And the detached Sender still exits — `force_close()` has already been called —
  so there is no runtime-shutdown hang to fail on.

  So as shipped, all three tests pass with or without the fix. Whatever made the bug
  observable during development is not what the repository now contains, and the two
  written claims assert otherwise.
- **Expected**: Either a direct unit test on the seam — drive `await_sender_handle` with
  a handle that outlives the timeout and assert `self.sender_handle` is `Some` afterwards
  (the field is reachable from the in-module `mod tests`) — or an assertion in the shared
  close helper that actually observes the join (e.g. a flag the spawned Sender sets on
  exit, asserted `true` immediately after `close_timeout` returns). Then correct the two
  claims to describe how the bug was found versus what now prevents its return.
- **Actual**: A production bug fixed with no regression test, and two records stating the
  opposite. Reverting the one-line change should be expected to fail a test; today it
  fails none.

---

## Issue: "Java's own unsynchronized writer is safe for a different reason" overstates what single-caller confinement buys
- **File**: `src/producer/internals/sender.rs:285-287`; PLAN §10.9 deviation 1 (same sentence)
- **Severity**: Record Defect
- **Java Reference**: `TransactionManager.java:969` → `:1191` → `:1188`; `Sender.java:522`
- **Description**: The field doc concludes: "Java's own unsynchronized writer is safe for
  a different reason (only the Sender calls `lookupCoordinator`), and taking this lock
  there costs nothing." The parenthetical is true — `lookupCoordinator(TxnRequestHandler)`
  (`:969`, package-private, **not** `synchronized`) has exactly two callers,
  `Sender.java:522` and `TransactionManager.java:1414`, both Sender-side. But being
  called from one thread does not make the access safe, and the sentence uses it as if it
  did.

  That site reaches `pendingRequests.add` (`:969` → private `lookupCoordinator(type,
  key)` `:1191` → `enqueueRequest` `:1207` → `:1188`) **without holding the monitor**,
  while the application thread performs its own `add`s *under* the monitor via all four
  public entry points. Two threads mutating one `PriorityQueue` with only one of them
  synchronized is an unsynchronized concurrent mutation with no happens-before edge
  between them — there is no other lock, no `volatile`, and `PriorityQueue` is not
  thread-safe. Java's safety here is incidental (the window is narrow and the app-side
  calls are rare), not structural.

  This matters because the sentence feeds the same proposed rules §2 amendment as the
  finding above. As written it invites the conclusion "so `pendingRequests` *is*
  Sender-confined for writes", which is precisely the premise Phase 6 correctly refuted.
  Worth stating plainly instead: the Rust translation takes the lock at that site and is
  therefore **strictly safer than Java**, which is a better argument for the design than
  calling the Java race safe.
- **Expected**: Reword to separate the two claims — `lookupCoordinator` is Sender-only
  (true, and why rules §2 grouped it there), *and* its unsynchronized `add` races the
  app-side synchronized `add`s in Java, which the Rust lock closes at no cost.
- **Actual**: A doc that will guide a rules amendment asserts Java's unsynchronized
  writer is "safe" on grounds that do not establish safety.

---

# Adjudications and cleared checks

The four adjudications resolve **in the Actor's favour**. Recorded so the next pass does
not re-derive them.

**1. The `prepare_transaction` refusal is correct.** Confirmed on the corpus that
`build.rs` and CLAUDE.md point at (`kafka/` at tag `4.2.0`, `a18251bae0`):
`grep -rn prepareTransaction kafka/clients/src/` returns exactly the three hits the Actor
cites, and `grep -c prepareTransaction Producer.java` → 0. Stronger than the Actor's own
case: `KafkaProducerMetrics.java:80` creates `prepareTxnSensor` but there is **no
`recordPrepareTxn` method anywhere in the class** — the sensor is dead, a forward-looking
KIP-939 artifact, so it implies no 4.2 producer method. `completeTransaction` likewise
exists only inside `throwIfInPreparedState`'s message text. Adding either would violate
DoD §7. The substitute surface is right: `throwIfInPreparedState` (`:968-976`) has exactly
two Java call sites, `beginTransaction` (`:677`) and `doSend` (`:989`), and the Rust guard
sits at both (`:679`, `:952`) with Java's message text preserved. The third Rust site
(`:1407`, the zero-copy FFI `send`) is a justified addition, not a divergence — that path
bypasses `do_send` entirely and already carried `ensure_not_closed`, so it owed the
sibling guard; the self-review that caught it (`9ed5a34`) was right. §Phase-6's spec text
**was** amended with a strikethrough correction; only §9.21 was missed (filed above).

**2. The `PendingRequests` reclassification is correct, and the Rust side is clean.** I
walked Java independently. `pendingRequests` is touched from both threads: app side
through `enqueueRequest` at `:325`, `:375`/`:393`, `:433`, `:1829`, all inside
`synchronized` methods; Sender side through `nextRequest` (`:894`, synchronized), `retry`
(`:936`, synchronized), `failPendingRequests`, `authenticationFailed`, `close`, plus the
unsynchronized `lookupCoordinator` path. So rules §2's premise is indeed false of the
class, and `Arc<Mutex<PendingRequests>>` outside the manager is the right shape. On the
ordering question the brief raised: there is **no happens-before** protecting the
Sender-side unsynchronized `add` (see the finding above) — it is a genuine Java race that
Rust's lock closes.

Rust audit results, all clean:
- **No guard held across `.await`.** Scripted sweep of all 411 `let [mut] x =
  ….lock().unwrap();` bindings repo-wide, walking per-character brace depth to the
  binding's scope end: zero real hits (the single candidate is `awaiting_validation`, a
  substring). Complementary statement-level sweep for an inline lock temporary sharing a
  statement with `.await`: no Phase 6 hits — the 22 matches are `tokio::sync::Mutex`
  (`.lock().await`) in pre-existing consumer code, or my splitter joining a test
  assertion to the preceding `run_once().await`. Note my first walker produced a false
  candidate at `sender.rs:1329` purely because `} else {` is brace-neutral on one line;
  the guard there does drop at the `if` block's close, and that method's rustdoc already
  says so.
- **Lock order never inverted.** All 20 `pending_requests` lock sites bind the guard to a
  local *before* locking the manager, exactly as the field doc's receiver-evaluation note
  requires. No site anywhere holds a `TransactionManager` guard and then acquires
  `pending_requests`; `transaction_manager.rs` has no such field and receives
  `&mut PendingRequests` as a parameter, so inversion is not expressible from that side.
  The `with_in_flight_batch_pool` closure (`sender.rs:1115-1125`) is the one deque-holding
  site and takes them in the mandated deque → `pending_requests` → manager order.
- **No hot path takes the lock.** In `kafka_producer.rs` the only lock sites are `:643`,
  `:750`, `:804`, `:844` — the four rare public methods — plus construction. `do_send`
  and the zero-copy `send` never touch it.

**3. The lost-join fix is correct** (verified against the pre-fix line in `dd2b374`), and
**the shape occurs nowhere else in production code**. Sweep of all
`tokio::time::timeout` sites: the only other by-value-handle instances are
`buffer_pool.rs:495`, `:715` and `record_accumulator.rs:3495`, all inside `#[cfg(test)]`
regions and all `.expect(..)`-ing on expiry, so they fail loudly rather than continuing —
not the same defect. The missing regression test is filed above.

**4. The 18-vs-15 discrepancy is a legitimate reconciliation, not a lost entry.**
`18 = 3 + 15`, and the pre-Phase-6 tree says so at `c59c09d:7547-7549`: the pasted
derivation covers "the **eighteen entries this group and the 5a group above** cover — the
three 5a ones print `-`". The three are the ones Critic 45 issue 3 made translatable, all
present: `testInitProducerIdWithMaxInFlightOne` (Java 636) → `sender.rs:4392`,
`testNodeNotReady` (689) → `:4577`, `testDoNotPollWhenNoRequestSent` (2991) → `:4490`.
The header already read `TRANSACTIONAL (15)` **at `c59c09d`**, i.e. before Phase 6, so the
15 was Phase 5b's own reclassification; Phase 6 changed disposition only (4 translated /
11 to Phase 8) and the membership diff is empty. The brief's premise — that Phase 5b
handed 18 rows forward — reads Critic 45's description of the *pre-fix* state as the
post-fix state. Only PLAN §9.21 still carries the bare 18 (filed above). Every shipped
derivation in that block re-ran correctly: Java 52 / Rust 54, `comm -23` empty, `comm
-13` the two named out-of-scope entries.

**Standard sweep — checked and clean:**
- **Trait shape.** 5 methods, matching `Producer.java:42-66` one-for-one (`initTransactions`,
  `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction`, `abortTransaction`);
  no sixth. `begin_transaction` is sync, per the plan's own warning and Java `:674-681`'s
  pure state transition. Plain `async fn` under `#[allow(async_fn_in_trait)]` — no
  `#[async_trait]`, no boxing. `MockProducer` and `MultilanguageProducer` both fail loudly
  with `KafkaError::unsupported_version` and name their owners (Phase 7 with the Java
  fields it needs; PLAN §9.6) — CLAUDE.md §5 satisfied.
- **The five public methods vs Java 648-860.** Guard order matches statement-for-statement
  in all five, including `throwIfInvalidGroupMetadata` **first** in
  `send_offsets_to_transaction` (Java `:734`) ahead of the no-manager and closed checks,
  and the `offsets.is_empty()` early return (Java `:738`). `commitTransaction` /
  `abortTransaction` correctly do **not** carry the prepared-state guard. `wakeup` fires
  after the enqueue and before the await in all four blocking methods, as Java does.
  `maybe_update_transaction_v2_enabled(true)` stays *after* the `?` in `init_transactions`,
  matching Java's post-await placement. Timeout mapping is right:
  `await_result_timeout` returns `KafkaError::timeout` on expiry, and `await_result`
  creates the `notified()` future before checking `completed` (rules §5).
  `transaction_manager_or_error`'s message and `throw_if_invalid_group_metadata`'s message
  both reproduce Java's text verbatim, and `UNKNOWN_MEMBER_ID` is `""` on both sides.
  `Caller::App` is passed at the one site that takes it (`begin_abort`, `:849`); the other
  four manager entry points have a single app-side Java caller each and hardcode it
  internally, which Phase 5b already settled.
- **`sendProduceRequest` wiring** (`sender.rs:2248-2273`) matches Java `:922-927`
  including the TV2 negation (`use_transaction_v1_version = !is_transaction_v2_enabled()`)
  and the non-transactional defaults `(None, false)`.
- **§6.6's method.** Extracted `maybe_send_and_poll_transactional_request` from both
  revisions and diffed: the only changes are the two guard-binding rewrites the
  `PendingRequests` reclassification forces (`&mut self.pending_requests` → a locked
  local). No logic change, so the Actor's "verified statement-by-statement and changed
  nothing" holds for behaviour, though the text did move. `await_node_ready` is
  byte-identical across the range.
- **Timer-starvation diagnosis is accurate.** In tokio's multi-thread scheduler only one
  worker holds the time driver; a worker that picks up a never-yielding task never returns
  to its park loop to drive it, and the others condvar-park — so no timer in the runtime
  fires, exactly as the thread sample showed (`run_once` spinning / `park_condvar`). The
  `spawn_blocking` + private current-thread runtime fix is sound and closer to Java's
  `ioThread`. Its weakening of Java's `assertThrows(KafkaException.class,
  producer::initTransactions)` to a conditional check is also justified: Java submits that
  to an `ExecutorService` and discards both the `Future` and the latch's boolean, so Java
  does not assert it either.
- **DoD.** 2399 lib tests pass, 0 failed, 2 ignored (the `#[ignore]`s are §9.18's
  documented split panic). No `TODO`/`FIXME` in `src/producer/`. No duplicated types.
- **Rule amendments.** Both are legitimate and correctly scoped: rules §2 gaining a third
  "outside the manager but shared" category for `pendingRequests` (the other three fields
  it groups with really are Sender-confined), and CLAUDE.md §9.6.6 gaining the
  never-yielding-task-starves-all-timers hazard. Fix the citations in the first before it
  is copied into the rules file.
- **Not filed deliberately:** `transactional_id().map(str::to_string)` in
  `send_produce_request` allocates a `String` where Java hands over an existing reference,
  but this is per-produce-RPC (not per record) and the generated `ProduceRequestData`
  owns the field, so there is no borrowed form to pass. `(Java A-B)` ranges in "Translated
  from" rustdocs again carry ±1-3 lines of slop; none points at a different method, so
  per Phase 5b's note this is not worth a fix cycle — the §10.9 rotation filed above is a
  different thing, since those citations *do* land in the wrong method.
