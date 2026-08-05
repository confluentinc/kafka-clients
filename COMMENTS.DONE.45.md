# Critic 45 — Milestone 11 Phase 5a: pass 1 resolved

**7 findings, 7 conceded, 0 disputed.** None was a behavioural bug in production
code: five were record/accounting defects (four created or invalidated by Phase 5a
itself) and two were test-fidelity gaps.

| Issue | Class | Fix |
|---|---|---|
| 1 | `CoordinatorNodes`' "only reader" prose contradicted by its own code | `65aa5a9` |
| 2 | `begin_abort` cited a guard this phase deleted; said Phase 6 for 5b work | `65aa5a9` |
| 3 | 18 `SenderTest` deferrals voided by this phase; 2 (in fact 3) now expressible | `d56be1a` |
| 4 | method accounting 91/83/82 → 90/82/81; regex counted the constructor | `ffbd1ad` |
| 5 | test-accounting `awk` did not execute; group B not enumerated | `ffbd1ad` |
| 6 | `initPidResult` assertions redirected to the wrong result object | `65aa5a9` |
| 7 | retry-installs-the-coordinator assertion dropped | `65aa5a9` |

## What the pass actually bought

**Issue 3 was the valuable one, and it under-counted itself.** The Critic named two
`SenderTest` methods whose deferral rationale Phase 5a had deleted. Re-deriving the
group per-entry — instead of writing a new blanket rationale — surfaced a **third**,
`testDoNotPollWhenNoRequestSent`, blocked only by a helper that is entirely 5a
surface. All three are translated. `testNodeNotReady` turned out to be the only
cover for `maybe_find_coordinator_and_retry`'s `else` arm, which no test reached;
`MockClient::delay_ready` had zero callers in the tree.

**Issues 4 and 5 are the same defect twice**: a shipped mechanical check whose prose
claimed more than the code did. §9.20 recorded the first instance of that shape this
milestone; these are the second and third. Both blocks now paste the real output of
every command, and for the `awk` the transcript was produced by extracting the
program *out of the comment block* rather than retyping it — which is the only
version of the check that would have caught the original `delete soft` defect.

Two repairs the findings implied but did not name: the `awk`'s marker iteration was
`for (k in hard)`, whose order awk leaves unspecified, so a pasted transcript could
not be reproducible; and the histogram's `sort -rn` left ties unordered for the same
reason. Both are now deterministic.

**Issue 6's first fix hung under mutation.** With `transitionToFatalError` no longer
failing the pending slot, awaiting a result nothing completes parks forever rather
than failing. The restored assertions keep a non-blocking `is_completed()` probe
ahead of the await, so a regression fails instead of hanging — Java's
`assertThrows(.., ::await)` would hang the same way, and the probe costs no fidelity.

Clippy's `await_holding_lock` fired on two of the new `SenderTest` translations
(rules §4, enforced on test code this time); both now assert inside a braced scope.

## Adjudications the Critic closed in the Actor's favour

Recorded here so a later pass does not reopen them. The `coordinator_supports_bumping_epoch`
§2 deviation was cleared, with the Critic strengthening the argument: every Java
*read* of the field holds the monitor while the sole *write* does not — the inverse
of Sender confinement, and a third case §2's "wrong in both directions" warning does
not anticipate. Also cleared: the priority queue (Java's `isEpochBump` is
`private final`, so snapshot-at-insertion is behaviourally identical, not merely
conservative), rules §5 in full, the other three straddler placements, the fidelity
sweep, and the concurrency re-sweep.

## Rule-update suggestions, filed for the Actor/Manager process

Left recorded rather than acted on; only that process may touch the rules.

  - `.claude/rules/producer-transactions.md` §2 and PLAN §6.5 list four fields as
    "touched exclusively by the Sender thread". True of three, false of
    `coordinatorSupportsBumpingEpoch`. Suggested: move it to the shared side citing
    the `KafkaProducer.java:1066` → `:781` → `:1310` chain, and add the anti-pattern
    *"a field placed on the Sender because §2 names it, without checking the Java
    call chain for an application-thread reader."*
  - `definition-of-done.md` §3: *"a shipped verification command must be executed
    once, from the repo root, on the interpreter available in this environment, and
    its real output pasted beside the claim. A derivation whose stated exclusions are
    not exercised by the code, or that scores a production claim against a
    `#[cfg(test)]` item, does not count as a check."*

---

# Original review (pass 1), verbatim

# Critic 45 — Milestone 11 Phase 5a review (pass 1)

Range reviewed: `9faf0a0..HEAD` (`fe7fe66`, `2538c78`, `1ac7240`, `58e4ad6`, `324da98`, `82cd837`).

Independently verified green: `cargo xtask format-check` exit 0, `cargo xtask lint` exit 0,
`cargo test --lib` 2316 passed / 0 failed / 2 ignored. No `TODO`/`FIXME` in the producer
module (the two in `src/consumer/` are pre-existing and outside this diff).

**Seven findings.** None is a behavioural bug in production code. Five are record/accounting
defects — four of them introduced or invalidated *by this phase* — and two are test-fidelity
gaps. See "Adjudications closed in the Actor's favour" at the end for the §2 deviation, which
I am clearing, plus the rule-update suggestion it implies.

---

## Issue 1: `CoordinatorNodes`' "only reader" justification is false — `handleCoordinatorReady` reads `transactionCoordinator` directly

- **File**: `src/producer/internals/transaction_manager.rs:226-231` and `:991-992`; `design/history/Milestone-11/PLAN.md` §10.7 deviation 1
- **Severity**: Design Flaw (wrong justification for a load-bearing placement decision)
- **Java Reference**: `TransactionManager.java:1103-1106`

**Description.** Three places state that `Sender.java:481` is the *only* reader of the
coordinator nodes, and use that to justify making them Sender-owned:

```
transaction_manager.rs:227-229
///  they are non-volatile, and Java's only reader is
///  `Sender.java:481` while its only writers are `lookupCoordinator` (`:1191`,
///  itself unsynchronized) and `FindCoordinatorHandler.handleResponse` (`:1693`).

transaction_manager.rs:991-992
///  The coordinator *nodes* do live on
///  the `Sender` as §2 requires — `Sender.java:481` is their only reader.

PLAN §10.7 deviation 1
   The coordinator **nodes** do go to the Sender as §2 requires — `Sender.java:481`
   is their only reader, ...
```

There is a second reader, and it is not behind `coordinator(CoordinatorType)`:

```java
// TransactionManager.java:1103-1106
void handleCoordinatorReady() {
    NodeApiVersions nodeApiVersions = transactionCoordinator != null ?
            apiVersions.get(transactionCoordinator.idString()) :
            null;
```

Full enumeration of the two fields in `TransactionManager.java` (`grep -n`): declarations 137,
138; constructor writes 219, 220; reads **961, 963** (inside `coordinator(..)`, whose only
production caller is `Sender.java:481`) and **1104, 1105** (`handleCoordinatorReady`, a direct
field read); writes 1194, 1197 (`lookupCoordinator`) and 1696, 1699
(`FindCoordinatorHandler.handleResponse`). The "only writers" half of the claim is accurate;
the "only reader" half is not.

The code is correct — `handle_coordinator_ready(&mut self, coordinators: &CoordinatorNodes)`
(`:2838`) takes the record as a parameter precisely because it needs to read the node. That
signature is the evidence contradicting the prose beside it, which makes the record
self-inconsistent rather than merely imprecise: a reader re-deriving the split from the stated
premise would conclude no manager method needs the nodes and could "simplify" the parameter
away, breaking `handleCoordinatorReady`. The `:991-992` instance sits in the docs for
`coordinator_supports_bumping_epoch`, i.e. inside the argument for issue-free placement of the
*other* field, where the claim is doing real work.

- **Expected**: name both readers — `coordinator(CoordinatorType)` at Java 961/963, reached
  from `Sender.java:481`, and `handleCoordinatorReady` at Java 1104-1105 — and note that the
  second is why `handle_coordinator_ready` takes `&CoordinatorNodes`. Fix all three sites; PLAN
  §10.7 deviation 1 carries the same sentence.
- **Actual**: all three assert a single reader.

---

## Issue 2: this phase removed `TransactionManager::new`'s guard but `begin_abort` still cites it, and mislabels its own deferral phase

- **File**: `src/producer/internals/transaction_manager.rs:1350-1352` and `:1360-1362`
- **Severity**: Missing Requirement (stale record introduced by this phase; wrong phase in a user-visible error message)
- **Java Reference**: `TransactionManager.java:361-371`; PLAN §Phase-5 split clause (as amended at `9faf0a0`)

**Description.** Deviation 10 records that `TransactionManager::new` no longer refuses a
transactional id, and the diff shows the Actor sweeping the consequent stale comments — five
`// Unreachable while `new` refuses a transactional id` lines are removed in
`9faf0a0..HEAD` (at the pre-image lines 1252, 1293, 1793, 1813, 1864). **The sweep missed one**,
and it is the one attached to a method that now behaves differently as a result:

```rust
// transaction_manager.rs:1349-1352
/// ... the rest of the body
/// (`transitionTo(ABORTING_TRANSACTION)`, `beginCompletingTransaction`, the
/// `EndTxn` handler) is Phase 6 and is unreachable while [`Self::new`] refuses
/// a transactional id.
```

`grep -rn "refuses a transactional id" src/` returns this line and one other (issue 3). Both
halves are now wrong:

1. `Self::new` no longer refuses a transactional id (`:1011-1017` says so explicitly), so the
   body is reachable — which is exactly why the method returns `unsupported_version` rather
   than being dead code.
2. `begin_abort` is **Phase 5b**, not Phase 6. The `9faf0a0` amendment assigns it there
   verbatim: *"the entry points that construct them (`begin_commit`, `begin_abort`,
   `begin_completing_transaction`, `send_offsets_to_transaction`, …)"*. The error text at
   `:1361` therefore tells a user "(Milestone 11, Phase 6)" for work scheduled in 5b, while the
   deferrals this phase newly wrote in `maybe_add_partition` (`:3248`, `:3257`) correctly say
   "Phase 5b". Mixed attribution with no stated criterion is the pattern rules §12 already
   names as a defect in its own domain.

- **Expected**: drop the invalidated clause; attribute the deferral to Phase 5b in both the
  doc and the `unsupported_version` message, matching `maybe_add_partition`'s wording.
- **Actual**: cites a guard this phase deleted, and names Phase 6.

---

## Issue 3: the `SenderTest` accounting's deferral rationale for all 18 transactional tests was voided by this phase, and two of the 18 are now fully expressible

- **File**: `src/producer/internals/sender.rs:7030-7031` (block spanning ~6980-7083)
- **Severity**: Missing Requirement (DoD §3 — a skipped test whose stated reason no longer holds)
- **Java Reference**: `SenderTest.java:636` (`testInitProducerIdWithMaxInFlightOne`), `:689` (`testNodeNotReady`)

**Description.** The `SenderTest` accounting block defers 18 tests under one rationale:

```rust
// sender.rs:7030-7031
// TRANSACTIONAL (18) — not expressible while `TransactionManager::new` refuses a
// transactional id; Phases 5 and 6 own them:
```

This phase removed that guard. The premise is void, and the block was not revisited — even
though the same phase added six transactional tests to the very same file
(`test_transactional_init_producer_id_is_routed_to_the_coordinator`,
`test_lookup_coordinator_on_disconnect_after_send`, `test_disconnect_and_retry`,
`test_lookup_coordinator_on_disconnect_before_send`, `test_unsupported_init_transactions`,
`test_unsupported_find_coordinator`), which is direct evidence that transactional `SenderTest`
methods are now expressible. `grep -n "PHASE 5A\|Phase 5a" src/producer/internals/sender.rs`
returns one hit, in the module header — the accounting block has no 5a section at all.

Two of the 18 need nothing beyond the surface Phase 5a shipped:

**`testNodeNotReady` (Java 689)** — the block's own note for it reads "transactional id +
`coordinator(TRANSACTION)`", both of which 5a now has. Reading the Java body, it needs:
`new TransactionManager(.., "testNodeNotReady", ..)`, `initializeTransactions(false)`,
`client.delayReady(node, REQUEST_TIMEOUT + 20)`, `prepareFindCoordinatorResponse`,
`client.throttle(node, ..)`, `prepareInitProducerResponse`, and
`transactionManager.coordinator(CoordinatorType.TRANSACTION)`. Every one has a Rust
counterpart today: `MockClient::delay_ready` (`src/mock_client.rs:244`),
`MockClient::throttle` (`:238`), `find_coordinator_response` / `init_producer_id_response`
helpers (`sender.rs:3941`, `:3954`), `Sender::coordinator`. It touches no
`AddPartitionsToTxn` / `EndTxn` / `beginTransaction` / TV2 / 2PC.

It is also the only cover for a branch this phase wired that nothing exercises. Java's own
javadoc: *"Tests the code path where the target node to send FindCoordinator **or**
InitProducerId is not ready."* The FindCoordinator half lands in the `else` arm of
`maybe_find_coordinator_and_retry` (`sender.rs:1259-1264`, Java 523-527: `time.sleep` +
`metadata.requestUpdate(false)`), reached only when the handler needs **no** coordinator. Every
existing test that reaches `maybe_find_coordinator_and_retry` carries an `InitProducerId`
handler on a transactional manager, so `needs_coordinator` is true and only the `if` arm runs:
`run_init_transactions` iteration 1, and `test_lookup_coordinator_on_disconnect_before_send`
(the sole user of `set_unreachable`, per `grep`). `MockClient::delay_ready` has **zero** callers
anywhere in the tree.

**`testInitProducerIdWithMaxInFlightOne` (Java 636)** — the block's note reads "builds the
manager with a transactional id and calls `initializeTransactions`", both now available. Its
idempotent twin `testIdempotentInitProducerIdWithMaxInFlightOne` (Java 664) is *already*
translated as `sender.rs:6343 test_idempotent_init_producer_id_with_max_in_flight_one`, using
`MockClient::set_max_in_flight_one` (`:6351`). The two Java tests are 30 lines apart and
differ only by the transactional manager and the extra FindCoordinator round trip, both of
which 5a supplies — so this is a small delta over shipped code, not new surface.

- **Expected**: rewrite the group header with a rationale that is true after this phase
  (per-entry, naming the 5b/6 surface each remaining test actually needs), add a 5a-translated
  group crediting this phase's six new tests, and translate `testNodeNotReady` and
  `testInitProducerIdWithMaxInFlightOne` now — the first because it is the only cover for
  `maybe_find_coordinator_and_retry`'s untested arm.
- **Actual**: 18 tests deferred under a condition this phase deleted, with no re-derivation.

---

## Issue 4: the method-accounting block's 91 / 83 / 82 are each one too high — the constructor is counted contrary to the block's own claim, and validated by a false-positive match against a `#[cfg(test)]` helper

- **File**: `src/producer/internals/transaction_manager.rs:750-808`
- **Severity**: Behavior Mismatch (a shipped derivation that contradicts its stated exclusion; DoD §2)
- **Java Reference**: `TransactionManager.java:209`

**Description.** The block asserts:

```rust
// transaction_manager.rs:753-756
// `TransactionManager.java` declares 91 distinct method names at class level
// (inner-class methods are indented eight spaces and are excluded; the
// `TransactionManager` constructor has no return type and so is not counted).
// After Phase 5a, 83 of the 91 have a Rust `fn`, ...
```

I ran the shipped `python3` derivation verbatim. It reproduces `91` and the nine names exactly
as claimed — but the parenthetical about the constructor is **false of that very regex**. The
modifier group `(?:(?:public|private|…)\s+)*` is `*`, so on

```java
    public TransactionManager(final LogContext logContext,
```

the engine backtracks to zero repetitions, treats `public` as the return type, and captures
`TransactionManager`. Verified directly:

```
ctor line 209 matched?: True
regex on ctor line -> TransactionManager
snake('TransactionManager') = transaction_manager | in defs: True
CORRECTED (ctor removed): 90
```

The phantom entry is then scored **present** by a name collision, not by a translation:
`snake("TransactionManager")` is `transaction_manager`, and `fn transaction_manager` exists at
`src/producer/internals/sender.rs:2565` — inside `#[cfg(test)] mod tests` (opened at `:2249`),
documented *"The shared transaction manager, for tests that assert on manager state."* It is a
`SenderTestContext` accessor. The constructor's actual translation is `TransactionManager::new`
(`transaction_manager.rs:1019`), which the name-matching cannot reach.

Corrected figures, with the owed-9 unchanged:

| | block | actual |
|---|---|---|
| distinct class-level method names | 91 | **90** |
| have a Rust `fn` | 83 | **82** |
| fully translated | 82 | **81** |
| owed to 5b | 9 | 9 |

This is not a missed method — the "no `fn` **and** not owed" set is empty and the owed-9 list is
exactly the eight genuinely-absent names plus `beginAbort` (which the block correctly and
repeatedly discloses as counted in both groups, `:784-786`, `:792-794`, `:805-807`; the
"83 + 9 = 92" reading in the review brief is a mis-summary, not a defect in the block). It is
the §9.20 failure mode: the totals reconcile while one entry is wrong, and the wrongness is
masked by a match against a test fixture.

- **Expected**: either exclude the constructor from the regex and state 90 / 82 / 81, or count
  it and map it explicitly to `TransactionManager::new` — and in either case do not let a
  `#[cfg(test)]` `fn` satisfy a production-method claim (restrict the `defs` scan, or filter
  the `#[cfg(test)]` regions out).
- **Actual**: 91 / 83 / 82, with the excluded item silently included and spuriously satisfied.

---

## Issue 5: the test-accounting block's central `awk` derivation does not execute, so 140 / 33 / 107 — and the whole of group B — are unverifiable by the shipped procedure

- **File**: `src/producer/internals/transaction_manager.rs:6051-6086`
- **Severity**: Missing Requirement (a completeness claim whose mechanical check does not run; DoD §3)

**Description.** The block states its own purpose at `:6027-6031`: the split is *"derived
mechanically rather than asserted in prose — Critic 44 issues 6 and 7 were the two failure
modes of the prose form"*. Saved and run exactly as shipped, the `awk` program dies:

```
awk: can't read value of soft; it's an array name.
 input record number 228, file .../TransactionManagerTest.java
awk exit=2
```

`delete soft` (`:6057`) types `soft` as an array; `soft = 1` (`:6063`) then assigns a scalar to
it. Result, running the five checks as written:

| shipped check | claimed | as shipped |
|---|---|---|
| total | 140 | **0** |
| `grep -c '^    @Test'` | 122 | 122 ✓ |
| `grep -c '^    @ParameterizedTest'` | 18 | 18 ✓ |
| group A | 33 | **0** |
| group B | 107 | **0** |

This machine has only `awk version 20200816` (macOS BWK awk); `gawk` and `mawk` are both
absent, so the shipped command has no working interpreter here. With the one-token repair
`delete soft` → `soft = 0` it runs clean and reproduces **140 / 33 / 107 exactly** — so the
numbers are right and the checker is broken, which is the same shape as issue 4 and worse in
one respect: group B's 107 entries are **not enumerated inline**. The block's only access to
them is this command plus the histogram at `:6179-6189`, so a reviewer following the block
literally gets nothing at all for 107 of the 140 methods.

Everything else in the block checks out and should be left alone: 122 / 18 / 140 verified
independently; the prose GROUP A list is set-equal to the mechanical group-A set (33 vs 33,
`comm` empty both ways — the Critic-44 lost-entry failure does *not* recur); all 29 named Rust
targets exist in the stated file, and 29 + the four `→ paired with` entries = 33; the four
character-identical Java pairs (263≡506, 283≡511, 289≡517, 297≡525) all diff clean; the
15-line `Optional.empty()` check and the `doInitTransactionsWith2PCEnabled` count reproduce as
claimed; the 7 "nevertheless covered" entries are all genuinely group B with no double-count.

- **Expected**: repair the `awk` so the shipped commands run on a plain POSIX awk, and — since
  the group-B claim is the one with no inline record — either enumerate group B or ship a check
  that a reviewer can actually execute.
- **Actual**: three of five checks print `0`.

---

## Issue 6: `test_transactional_id_authorization_failure_in_find_coordinator` drops Java's two `initPidResult` assertions, and the comment excusing them misstates the Java source

- **File**: `src/producer/internals/transaction_manager.rs:5241-5243`
- **Severity**: Missing Requirement (DoD §3 — assertion strength; wrong justification)
- **Java Reference**: `TransactionManagerTest.java:1360-1361`

**Description.** Java asserts on the **`InitProducerId`** result:

```java
// TransactionManagerTest.java:1360-1361
assertFalse(initPidResult.isSuccessful());
assertThrows(TransactionalIdAuthorizationException.class, initPidResult::await);
```

Rust asserts those two properties on the **`FindCoordinator`** result (`:5232`, `:5233-5240`)
and reduces `init_pid_result` to a bare completion check:

```rust
// transaction_manager.rs:5241-5243
// Java asserts on `initPidResult` in the sibling test; the fatal transition
// also fails the pending slot, which is the same result object.
assert!(init_pid_result.is_completed());
```

These are two distinct objects in both languages — `initializeTransactions` builds
`InitProducerIdHandler`'s result, while `lookupCoordinator` builds `FindCoordinatorHandler` via
`super("FindCoordinator")`, i.e. a fresh `TransactionalRequestResult` — so the substitution
does not carry. What is lost is specific: `init_pid_result` is the object installed in
`pending_transition`, and the only thing that fails it is `transition_to_fatal_error`'s
`pending.result.fail(error)` (`:1416-1418`, Java 545-547). `is_completed()` alone passes
whether the slot was failed with the *right* error, the wrong error, or merely `done()`. Java's
`assertThrows(TransactionalIdAuthorizationException.class, …)` is what pins it.

The excusing comment is also wrong on the facts. Java asserts these **in this very method**
(body 1352-1362), not only in a sibling. There *is* a sibling —
`testTransactionalIdAuthorizationFailureInInitProducerId` (Java 1366), asserting the same three
things at 1373-1375 — but it is in GROUP B (blocked on `assertAbortableError`, per the shipped
derivation: `1366 testTransactionalIdAuthorizationFailureInInitProducerId assertAbortableError`)
and `grep -rn` finds its name nowhere in `src/`. So the deferral points at a test Phase 5a does
not translate, and the assertion is covered nowhere end-to-end. (`test_fatal_error_fails_the_pending_transition`
at `:4776` covers the *mechanism* in isolation, not this path.)

- **Expected**: assert `!init_pid_result.is_successful()` and that awaiting it yields
  `Errors::TransactionalIdAuthorizationFailed`, keeping the existing `find_coordinator_result`
  assertions alongside; delete or correct the comment.
- **Actual**: both assertions redirected to a different result object, excused by a claim the
  Java file contradicts.

---

## Issue 7: `test_coordinator_not_available` drops the assertion that the *retry* installs the coordinator

- **File**: `src/producer/internals/transaction_manager.rs:5174-5196`
- **Severity**: Missing Requirement (DoD §3 — assertion strength)
- **Java Reference**: `TransactionManagerTest.java:2018-2019`

**Description.** Java's `testCoordinatorNotAvailable` gates on, then asserts, the coordinator
being installed **after** the retriable failure — that is the point of the test:

```java
// TransactionManagerTest.java:2018-2019
runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
```

The Rust drives the second `complete_find_coordinator` (`:5174-5183`) but never inspects
`coordinators`; it substitutes "the subsequent `InitProducerId` completes" (`:5184-5195`). That
substitution is not equivalent here, because the test resolves the `InitProducerId` by calling
`next_request` + `complete_init_producer_id_with_coordinators` directly rather than through the
`Sender` routing that reads `coordinators`. A retry that installed the node in the wrong slot
(`consumer_group` instead of `transaction`) would leave this test green. The equivalent
assertion exists only for the *first*-attempt path, in
`test_lookup_coordinator_clears_the_node_and_enqueues_a_find_coordinator_first` (`:5118-5121`).

- **Expected**: after the successful retry, assert
  `coordinators.coordinator(CoordinatorType::Transaction) == Some(&broker_node())`.
- **Actual**: no coordinator assertion on the retry path.

---

## Adjudications closed in the Actor's favour (no action needed)

Recorded so the next pass does not re-open them.

1. **The rules §2 deviation for `coordinator_supports_bumping_epoch` is correct — I am clearing
   it.** I walked the Java chain line by line and it holds exactly as claimed:
   `KafkaProducer.java:1066` (`transactionManager.maybeTransitionToErrorState(e)` inside
   `doSend`'s `catch (ApiException e)`, application thread) → `TransactionManager.java:781`
   (`if (needToTriggerEpochBumpFromClient() && !isCompleting())`, inside
   `maybeTransitionToErrorState`, which opens at `:764`) → `:1309-1310`
   (`return coordinatorSupportsBumpingEpoch && !isTransactionV2Enabled`). The Rust call site
   exists at `kafka_producer.rs:773`. Stronger still: every Java *read* of the field
   (`:561`, `:781`, `:1382`) happens with the monitor held, while the sole *write*
   (`handleCoordinatorReady`, `:1110`) is unsynchronized — the opposite of Sender confinement.
   No mirror risk: `grep -rn` shows the only non-test readers are
   `need_to_trigger_epoch_bump_from_client` / `can_handle_abortable_error`, both called from
   manager methods whose callers already hold the guard, so nothing on the Sender's loop takes
   the lock solely for this flag.

2. **The priority queue is correct.** Java's comment at 191-194 reconciles with the numbers:
   `EPOCH_BUMP(4)` sorts *after* `END_TXN(3)`, i.e. the epoch bump goes last, which is the
   abort-then-bump order the comment describes. `Priority`'s derived `Ord` follows declaration
   order and matches the discriminants; `QueuedRequest::cmp` inverts both keys so
   `BinaryHeap`'s max-heap yields Java's min-heap, FIFO within a priority. Snapshot-at-insertion
   is behaviourally identical to Java, not merely conservative: `InitProducerIdHandler.isEpochBump`
   is `private final` (Java 1463), so `priority()` cannot change while the element sits in the
   queue, and `reenqueue`/`retry` re-add only after the element was polled out, which
   `PendingRequests::add` re-keys anyway (pinned by
   `test_reenqueued_request_is_rekeyed_at_its_current_priority`). Three of the five priorities
   are exercised through the queue; `AddPartitionsOrOffsets`/`EndTxn` have no 5a handler kind.

3. **Rules §5 machinery is faithful.** `handle_cached_transaction_request_result` (`:1833-1861`)
   and `throw_if_pending_state` (`:1791-1804`) match Java 1261-1283 and 1249-1258 arm for arm,
   including the `is_acked()` key, the same-`Arc` return, the message text, and leaving the slot
   unset when the supplier errors. `PendingStateTransition` matches Java 1953-1967. The
   `FnOnce(&mut Self) -> Result<…>` translation does not move any effect: Java's supplier body
   runs at `supplier.get()`, after the pending-transition checks and before
   `pendingTransition = …`, and the Rust closure runs at `:1858`, in the same position.
   No `oneshot`/`mpsc` in `src/producer/`.

4. **The other three straddler placements hold.** `maybe_add_partition` preserves Java's
   `if/else if` order with the TV2 arm kept in position (`:3245`), so the chain cannot be
   entered out of sequence. The consistency argument is airtight and mechanically checkable:
   `grep` shows `new_partitions_in_transaction` and `partitions_in_transaction` are only ever
   `HashSet::new()` (`:1035`, `:1037`) or `.clear()` (`:1540`, `:1542`), so both sets are
   provably empty in 5a — which makes `nextRequest`'s first statement dead and
   `is_send_to_partition_allowed` return Java's own answer for an empty set. Together with
   `maybe_add_partition` refusing to register, no transactional send can reach the drain path.
   `is_prepared` / `prepared_transaction_state` are pure reads.

5. **Fidelity sweep found nothing further.** `FindCoordinatorHandler` (Java 1680-1720):
   the no-return fall-through on `size() != 1` is real (Java 1685-1689 — `fatalError` neither
   rethrows nor returns, and `FATAL_ERROR` is unconditionally valid at Java 181-186), and the
   empty-list divergence is only panic-vs-`Err` per CLAUDE.md §10.1. The `SHARE` arm's own
   fall-through differs (Rust returns early, Java continues to `result.done()`) but is
   unreachable in both: `lookupCoordinator` rejects `SHARE` before constructing the handler, so
   `keyType` is only ever 0 or 1. `CoordinatorType` has no custom `toString`, so
   `coordinator_type_name`'s uppercase/lowercase forms are right at all three sites, and the
   three `default:` messages match Java 962 / 1199 / 1702 verbatim. The nine-state transition
   table matches Java 162-188 exactly, including `ABORTABLE_ERROR`'s self-loop and `READY`'s
   absence of one. `transitionTo`, `transitionToFatalError`, `transitionToAbortableError`,
   `maybeTransitionToErrorState`, `maybeFailWithError`, `resetTransactionState`,
   `transitionToUninitialized`, `failPendingRequests`, `authenticationFailed`, `close`,
   `nextRequest`, `maybeTerminateRequestWithError`, both `lookupCoordinator` overloads,
   `handleCoordinatorReady`, `coordinatorType`/`coordinatorKey`/`needsCoordinator` and
   `InitProducerIdHandler.handleResponse` all check out arm for arm. I confirmed no subclasses
   exist for any of the five `instanceof` types in `maybeTransitionToErrorState`
   (`grep -rln 'extends …' common/errors/` is empty), so the flat `matches!` is faithful, and
   `Errors::is_retriable` contains none of the codes tested after the retriable arm, so no arm
   is shadowed. `setKeepPreparedTxn` appears nowhere in `kafka/clients/src` — claim confirmed.
   The four `sender.rs` coordinator arms replaced their deferrals completely; no
   `unsupported_version` remains on the transactional path (`:498` is the legitimate
   `versionMismatch` translation).

6. **Concurrency re-sweep clean.** Every `transaction_manager.lock()` in `sender.rs` is a
   statement-scoped temporary or an explicitly braced block; none spans an `.await`.
   `await_node_ready` (`:1286-1303`) takes the lock strictly after
   `network_client_utils::await_ready(..).await?`, as claimed. `run_transaction_phase`'s
   grouped read at `:926-933` drops its guard before `maybe_abort_batches`, preserving rules §3's
   deque → manager order. No `select!` anywhere near the poll. `Caller` is a literal at every
   new site with the right value: `App` at `kafka_producer.rs:773`, `transaction_manager.rs:1138`
   (guarded by `!is_epoch_bump`, and Java's only path to that branch is
   `KafkaProducer.initTransactions` — the epoch-bump overload's sole Java caller is
   `beginCompletingTransaction` at Java 398, which never takes it) and `:1200`; `Sender` at every
   handler/Sender site.

## Suggested rule update (via `agent-roles.md`, not an edit)

Finding-1 aside, the §2 premise itself needs correcting, and only the Actor/Manager process may
touch the rules:

- **`.claude/rules/producer-transactions.md` §2** lists four fields as "touched exclusively by
  the Sender thread". That is true of three and **false of `coordinatorSupportsBumpingEpoch`**,
  for the reason in adjudication 1 above. §2 also says a Critic reading only the `synchronized`
  keywords "will get this wrong in both directions" — this field is a third direction the rule
  does not anticipate: reads under the monitor from both threads, write without it.
  Suggest amending §2's field list to move `coordinatorSupportsBumpingEpoch` to the shared side
  with the `KafkaProducer.java:1066` → `:781` → `:1310` chain cited, and adding to its
  anti-patterns: *"a field placed on the Sender because §2 names it, without checking the Java
  call chain for an application-thread reader."* PLAN §6.5 carries the same list and needs the
  same correction.
- **`definition-of-done.md`** — issues 4 and 5 are the second and third instances this milestone
  of a shipped mechanical check that does not do what its prose says (§9.20 recorded the first).
  Suggest adding to §3: *"a shipped verification command must be executed once, from the repo
  root, on the interpreter available in this environment, and its real output pasted beside the
  claim. A derivation whose stated exclusions are not exercised by the code, or that scores a
  production claim against a `#[cfg(test)]` item, does not count as a check."*

---

# Critic 45 — pass 2 resolved

**2 findings, 2 conceded, 0 disputed.** Both records, neither behavioural. Pass 2
verified all seven pass-1 fixes genuinely fixed (both derivations re-run from the
shipped text, byte-identical regeneration; the `else`-arm mutation claim proved by
site analysis without mutating; the zero-production-diff claim confirmed per-file;
the timing departure adjudicated acceptable-and-better-than-Java; the
`is_completed()` probe ruled to strictly dominate Java's `assertThrows`), and
withdrew one of its own preliminary flags after running my version of the histogram
sort.

| Issue | Class | Fix |
|---|---|---|
| 1 | new `SenderTest` derivation used `for (k in hard)` — the non-determinism the same commit set repaired next door | `a5918b3` |
| 2 | `test_node_not_ready` claimed Java 689-711 but dropped 708-709 | `a5918b3` |

## Issue 1 — my own rule, broken one file over

The rule I wrote while fixing pass-1 issue 5 says to walk `MARKERS` in declaration
order *because* unspecified order matters when output is pasted. The `SenderTest`
derivation added in the same change did neither. The Critic demonstrated the paste
was hash-ordered (entry 1534 printed positions 8, 7, 1) rather than asserting it.
One-line fix, re-pasted in declaration order.

Two subsidiary points answered rather than left open:

  - **Splitter hazard: checked.** The guard here is shaped differently from the
    sibling's because `SenderTest` declares tests both `public` and `private`, so
    `private void` helpers must be able to *start* a block. Verified rather than
    argued: recomputing every block's marker set from a body delimited by its
    closing `    }` line agrees with the splitter on **all 100 blocks**.
  - **No `ABBREV` table, deliberately.** It exists next door because 107 rows had to
    fit the column limit; 18 rows can carry full marker names, which is worth more
    than cross-block comparability.

## Issue 2 — translated, not documented

The Critic offered either resolution and explicitly disclaimed a coverage hole. I
translated Java 708-709, because on my own stated standard a "minus a named tail"
entry is for a tail whose *surface is missing* (Phase 5b) — and nothing was missing
here: `MockClient::throttle` already existed and `poll_delay_ms` already honoured
`throttled_until_ms`. A note would have been a deferral with no blocker, which is
the shape pass-1 issue 3 was about. No new infrastructure was built.

Making it pass surfaced a precondition worth pinning: one `run_once` too many after
clearing the delay sends the `InitProducerId` early, so the throttle has nothing
queued to block and `maybeSendAndPollTransactionalRequest` returns at
`Sender.java:460-463` without consulting the coordinator. Java is in the same state
at its `assertNotNull`, so the fix is one `run_once` plus explicit
queued/not-in-flight assertions.

Both halves mutation-checked against the assertion that names each: deleting the
`else` arm's `metadata.request_update(false)`; and making `lookup_coordinator` stop
forgetting the TRANSACTION node.

## Process note recorded against myself

My first verification of the issue-1 re-paste reported `IDENTICAL` from a `diff` of
two *empty* files — the `sed` extraction had silently dropped the program's last
line. This is the same class as the defect being fixed. The check now asserts the
extracted program has balanced braces and that both sides are non-empty before
diffing, and that guard is what made the second attempt trustworthy.
