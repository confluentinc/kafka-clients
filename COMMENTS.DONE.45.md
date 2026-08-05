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

---

# Pass 3 (clean — closes 5a, 2026-08-05)

# Critic 45 — Milestone 11 Phase 5a review (pass 3)

Range: `990bb4e..HEAD` (`a5918b3`, `3c24cc7`).

## No findings.

Both pass-2 findings are fixed. Phase 5a closes from my side. Loop: 7 → 2 → **0**.

Verified green independently: `cargo xtask lint` 0, `cargo xtask format-check` 0,
`cargo test --lib` 2319 passed / 0 failed / 2 ignored, and `test_node_not_ready` passing on
its own. Production regions confirmed byte-identical per file, cutting each revision at *its
own* `^#[cfg(test)]` line: `sender.rs` 2249 → 2249 zero-line diff, `transaction_manager.rs`
3413 → 3413 zero-line diff, `mock_client.rs` untouched. Only the `sender.rs` test region and
its accounting block changed.

## Finding 1 — the `SenderTest` derivation

Re-run from the shipped text, with my own extraction guards (see below). The declaration-order
walk is applied:

```awk
n = split(MARKERS, m, ",")
for (i = 1; i <= n; i++) if (m[i] in hard) hits = hits (hits == "" ? "" : "+") m[i]
```

- **exit 0, 100 rows.**
- **All 18 accounted rows regenerate byte-identically** to the inline paste (`diff` clean over
  the extracted `line \t name \t flags` triples).
- **The rule is actually applied, not just claimed.** I checked every one of the 100 rows, not
  only the 18: mapping each flag back to its `MARKERS` index, **0 rows are out of declaration
  order**. The two rows I cited in pass 2 now read `beginTransaction+maybeAddPartition+AddPartitionsToTxn`
  (1, 7, 8) and `beginAbort+mock(TransactionManager` (3, 14).

**The splitter claim checks out, independently.** Rather than take the Actor's word, I
implemented the body-recompute myself — for each `^    (public|private) void …(` line, collect
until the first line that is exactly `    }`, and recompute the marker set from that body.
Result: 100 blocks on both sides, **0 disagreements** in line, name or flags. The differently
shaped guard (`private void` helpers must be able to *start* a block, since `SenderTest`
declares tests both `public` and `private`) is therefore correct as well as documented.

**The no-`ABBREV` decision is sound.** 18 rows carry full marker names inside the column
limit; the sibling's table exists because 107 rows did not. Stated at the site as a
difference rather than an oversight, which is what makes it reviewable.

## Finding 2 — Java 708-709 translated rather than deferred

The right call, and for the reason given: `MockClient::throttle` already existed
(`mock_client.rs:238`) and `poll_delay_ms` / `is_ready` already honour `throttled_until_ms`
(`:73`, `:100-106`), so a note would have been a deferral with no blocker — the shape of
pass-1 issue 3. I verified the second half element by element against Java 689-711; all
eleven statements now have a counterpart, with the two `2 * REQUEST_TIMEOUT` substitutions the
only departures.

**The `run_once` count is right, and Java is the cross-check.** Java has exactly two `runOnce`
between `delayReady` and `assertNotNull` (`:704`, `:705`); the Rust has the else-arm one and
one more. Tracing that second one: it sends the `FindCoordinator`, `poll_and_dispatch` inside
`maybe_send_and_poll_transactional_request` consumes the prepared response, the coordinator is
installed, and `run_once` returns at the `Ok(true)` — leaving the `InitProducerId` queued and
nothing in flight, which the two new assertions pin. The Actor's stated reason for pinning it
holds: with a request already in flight, `maybe_send_and_poll_transactional_request` returns at
Java `:460-463` before ever consulting the coordinator, so the throttle would have nothing to
block and the second half would silently test nothing.

**Both mutation claims verified without mutating any source.**

- *Half one* — `!update_requests_before && ctx.metadata.update_requested()`. `grep` over the
  pre-test region gives exactly three `request_update` sites: `:1263` (the `else` arm), `:1449`
  and `:1873`. The latter two sit in `send_producer_data` and the produce-response path, and
  that iteration returns `Ok(true)` out of `maybe_send_and_poll_transactional_request` →
  `run_transaction_phase` → early return from `run_once`, so neither is reachable. Deleting
  `:1263` leaves the flag false and the assertion fails.
- *Half two* — "an unready coordinator must be forgotten so the lookup can repeat
  (`TransactionManager.java:1194`)". `self.transaction = None` occurs at exactly one place,
  `CoordinatorNodes::clear` (`transaction_manager.rs:304`), whose only caller is
  `lookup_coordinator` (`:2853`). At that point in the test nothing is in flight and the
  `InitProducerId` was never sent, so the response paths that also call `lookup_coordinator`
  are unreachable and `maybe_find_coordinator_and_retry`'s `if` arm is the only way in. Making
  `clear` a no-op therefore leaves the node `Some(..)` and fails that exact assertion. The
  assertion is a true discriminator for the `if` arm, which is what the half exists to cover.

**`2 * REQUEST_TIMEOUT` for the throttle — adjudicated: accept, same as the delay.** Java's
`REQUEST_TIMEOUT + 20` under `MockTime(10)` is a two-tick margin, and
`NetworkClientUtils.awaitReady` closes its window once the clock has advanced
`REQUEST_TIMEOUT`; two incidental clock reads anywhere in `runOnce` would let the node become
ready inside the window and silently exercise the opposite branch. A full-window margin plus
an explicit `sleep` removes the coin flip without changing which branch runs — and here the
branch is proved by assertion (coordinator forgotten) rather than inferred from timing. The
rustdoc says the substitution is the same one for the same reason, which is the right way to
record it.

**Also correct now:** the rustdoc's "Java 689-711" claim is true; both the rustdoc and the
accounting entry describe two halves and two arms; and the "only exercise of
`MockClient::throttle` on the transactional path" claim holds — `grep` finds two callers,
`:3743` (the pre-existing produce-path latency test) and `:4536` (this one).

## The extraction guards

Present in the record (`COMMENTS.DONE.45.md:617-622`) and **real** — I know because my own
first extraction this pass hit the identical failure. A mis-quoted `grep` for the start line
produced an empty program, and my guards (non-empty, balanced `{`/`}`, even single-quote
count) caught it and aborted instead of reporting a vacuous match. I then located the block
properly (`sender.rs:7414-7431`, 18 lines, `{`=5 `}`=5, 10 quotes) and re-ran.

Two notes for the record, neither a finding:

- The guard is a *reviewer-side* procedure, not a shipped artifact, and it does not need to
  be one: the blocks say "Save as `/tmp/scope.awk` and run from the repo root", i.e. they
  prescribe copy-paste. Extraction is my mechanism for checking that the pasted text is the
  text that ran, so the guard belongs with me as much as with the Actor. Recording it against
  itself was the right instinct and it is worth keeping in `producer-transactions.md` if the
  meta-rule from pass 1 is adopted.
- `cargo doc --no-deps` reports 57 distinct unresolved intra-doc links crate-wide. All are
  pre-existing and outside this module — the only one whose text resembles this phase's
  vocabulary is `[`FindCoordinator`]` in
  `src/consumer/internals/coordinator_request_manager.rs:169,239`, from Milestone 8 (`ba37a51`).
  `cargo doc` is not in the gate, and rustdoc does not process `#[cfg(test)]` items, so no link
  in the new test code is checked either way. Mentioned only so the next reviewer does not
  read it as new.

## Standing items (unchanged, not re-filed)

- The six pass-1 adjudications and the two rule-update suggestions are in
  `COMMENTS.DONE.45.md`. The suggestions — correcting `producer-transactions.md` §2 /
  PLAN §6.5 on `coordinatorSupportsBumpingEpoch`, and the `definition-of-done.md` §3 clause on
  executing shipped verification commands — are for the Manager/Actor process, not for me to
  apply.
- PLAN §9.14 stays not-re-filed.
# Critic 45 — Milestone 11 Phase 5b review (pass 1)

Range: `3c24cc7..b70e353` (`334ccc9`, `5600c65`, `00e6f89`, `71e71f7`, `1ccc9ba`, `887c535`,
`dc5182a`, `2c5f9a6`, `b70e353`).

Verified green independently: `cargo xtask lint` 0, `cargo xtask format-check` 0,
`cargo test --lib` 2369 passed / 0 failed / 2 ignored. No stub remains in
`src/producer/` — no `TODO`, `FIXME`, `unimplemented!`, and the only
`not yet implemented` in the module is `kafka_producer.rs`'s Phase-6 public-API guard.

**Four findings.** One is a behavioural divergence in the `Caller` mechanism; three are
records. The four handler orderings, TV2, 2PC and both accounting blocks all check out —
see "Verified clean" for what I ran.

---

## Issue 1: `begin_abort` hardcodes `Caller::App`, but Java reaches it from the Sender task — so a Sender-side invalid transition does not poison

- **File**: `src/producer/internals/transaction_manager.rs:1504`
- **Severity**: Behavior Mismatch
- **Java Reference**: `Sender.java:273`; `TransactionManager.java:361-371`, `:1124-1127`
- **Rule**: `.claude/rules/producer-transactions.md` §1, whose anti-pattern list names this exactly

**Description.** `begin_abort` takes no `caller` parameter and its transition is hardcoded:

```rust
// transaction_manager.rs:1504
manager.transition_to(State::AbortingTransaction, None, Caller::App)?;
```

`beginAbort()` has **two** production call sites in Java (`grep -rn "beginAbort()" producer/`):

| Java | thread |
|---|---|
| `KafkaProducer.java:818` (`abortTransaction`) | application |
| **`Sender.java:273`** (`run`'s shutdown abort loop) | **Sender** |

The Rust already has both. The Sender-side one is live: `Sender::begin_abort`
(`sender.rs`) calls `transaction_manager.lock().unwrap().begin_abort(&mut self.pending_requests)`,
and is invoked from the shutdown loop at `sender.rs:576`. The method's own rustdoc names it —
*"`Sender.run`'s shutdown loop calls this at `Sender.java:273`"* — so the second caller is
known; only the `Caller` does not reflect it.

Rules §1 is explicit on both the requirement and the smell:

> Where a method is reachable from both, it takes `caller` as a parameter and forwards it —
> do NOT default it.
>
> **Anti-patterns to flag in review:** … A method reachable from both sides that hardcodes
> one `Caller`.

**Why it is behavioural, not cosmetic.** `shouldPoisonStateOnInvalidTransition()` is the whole
point of the enum. On an invalid `→ ABORTING_TRANSACTION`, Java on the Sender thread does
(`TransactionManager.java:1124-1127`):

```java
currentState = State.FATAL_ERROR;
lastError = new IllegalStateException(message);
throw lastError;
```

With `Caller::App` the Rust returns the error and leaves `current_state` and `last_error`
untouched. Java deliberately anticipates the throw on this path — `Sender.java:269-271`:
*"It is possible for the transaction manager to throw errors when aborting. Catch these so as
not to interfere with the rest of the shutdown logic"* — and force-closes on it. The states the
shutdown loop's own guard admits (`hasOngoingTransaction() && !isCompleting()`, i.e.
`IN_TRANSACTION` or `ABORTABLE_ERROR`) are both valid sources, so reaching the invalid case
needs the app side to move the state between the guard read and the call. That window exists in
both languages — the Rust drops the manager guard between `has_ongoing_transaction()` /
`is_completing()` and `begin_abort()`, exactly as Java's separate `synchronized` calls do — and
Java's answer inside it is to poison. Latent today only because
`KafkaProducer::from_config` still rejects `transactional.id` until Phase 6; that guard's removal
is what makes it reachable, which is why it wants fixing now rather than being discovered then.

I checked the other new entry points against their Java call sites; `begin_abort` is the only
one affected. `beginCommit` (`KafkaProducer.java:783`), `sendOffsetsToTransaction` (`:740`) and
`maybeAddPartition` (`:1045`) each have exactly one Java caller, all application-side, so their
`Caller::App` is right; `prepareTransaction` has no `clients/src` caller at all;
`reset_transaction_state`, the six `handle_*_response` methods, `fatal_error`,
`abortable_error` and `abortable_error_if_possible` are all Sender-only and correctly hardcode
`Caller::Sender`.

- **Expected**: `begin_abort(&mut self, pending_requests, caller: Caller)`, forwarded to
  `transition_to`; `KafkaProducer` passes `Caller::App`, `Sender::begin_abort` passes
  `Caller::Sender`. A test that a Sender-side invalid `→ ABORTING_TRANSACTION` leaves
  `FATAL_ERROR` and a recorded `last_error`, as the 5a
  `test_invalid_transition_poisons_only_on_the_sender_side` does for its own targets.
- **Actual**: one hardcoded `Caller::App` serving both callers; no poisoning on the Sender path.

---

## Issue 2: `handle_txn_offset_commit_response`'s rustdoc claims the re-enqueued request carries only the outstanding offsets — it carries the full original set, in both languages

- **File**: `src/producer/internals/transaction_manager.rs:4230-4232`
- **Severity**: Design Flaw (a stated mechanism neither language implements; invites a "fix" that would diverge)
- **Java Reference**: `TxnOffsetCommitRequest.java` Builder ctor; `TransactionManager.java:1394`, `:1944-1950`

**Description.** The doc's third structural claim:

```rust
// transaction_manager.rs:4230-4232
/// And `Errors.NONE` *removes* the partition from
/// `pendingTxnOffsetCommits` while the retriable arm leaves it in place, which
/// is what makes the re-enqueued request carry only the outstanding offsets.
```

The first half is right and the code implements it (`:4267` removes, `:4276-4279` continues).
The conclusion does not follow, in either language, because **both builders snapshot the
topic collection at construction**:

```java
// TxnOffsetCommitRequest.java, Builder ctor
this.data = new TxnOffsetCommitRequestData()
        …
        .setTopics(getTopics(pendingTxnOffsetCommits))
```
```rust
// txn_offset_commit_request.rs, TxnOffsetCommitRequestBuilder::new
data.set_transactional_id(..)
    …
    .set_topics(TxnOffsetCommitRequest::get_topics(pending_txn_offset_commits))
```

The Rust builder takes `&HashMap<..>` and copies; it cannot hold a borrow of
`self.pending_txn_offset_commits`, since the handler it lands in outlives the call. Java's
`reenqueue()` (`:1394`) re-adds `this` with that same snapshotted `data`, and the Rust
`self.retry(pending_requests, handler)` (`:4335`) re-enqueues the same handler with the same
builder — nothing between the two mutates `builder.data`. So a retry re-sends the **full
original** offset list, including partitions that already returned `NONE`.

What the map actually governs is the tail (`:4329-4336`, Java `:1944-1950`): whether to clear,
complete, or retry at all — and the contents of any *later*
`txn_offset_commit_handler` construction, e.g. a subsequent `send_offsets_to_transaction`,
which is where leftovers do get folded in. That is the real and worth-documenting property.

The risk is concrete: the claim reads as a specification, and the obvious way to make the code
match it is to rebuild the builder before `retry`. That would send a *reduced* request where
Java sends the full one — a wire-level divergence introduced by trusting the comment.

- **Expected**: state that the retry re-sends the constructed snapshot (as Java does), and that
  the map decides retry-vs-complete and seeds the next construction.
- **Actual**: claims the re-enqueued request is reduced.

---

## Issue 3: PLAN §10.8 deviation 3 counts 35 `expect` call sites; there are 56

- **File**: `design/history/Milestone-11/PLAN.md:2584`
- **Severity**: Missing Requirement (DoD §7 — a deviation record whose number does not match the tree)

**Description.** Deviation 3 justifies `next_request` returning `Result<Option<..>>` and closes:

> … and the 35 test call sites assert as much with
> `.expect("next_request does not fail on this path")`.

Real count, `grep -ro` over `src/`: **56** occurrences, one per line, across three files —
`transaction_manager.rs` 53, `sender.rs` 2, `record_accumulator.rs` 1. The third file is not
mentioned either.

The substantive part of the deviation is sound and I verified it: all 56 sites are inside
`#[cfg(test)]` modules (cut points 4604 / 2268 / 1813; **zero** in any production region), so
there is no panic on a public path; and the unreachability argument holds — the only states
holding a pending `EndTxn` are `COMMITTING_TRANSACTION` and `ABORTING_TRANSACTION`, and Java's
table admits both `→ READY` (sources `INITIALIZING`, `COMMITTING`, `ABORTING`) and
`→ INITIALIZING` (sources `UNINITIALIZED`, `COMMITTING`, `ABORTING`) from either. Only the
count is wrong. Same class as 5a issue 4: a stated number nobody re-derived after the tree grew.

- **Expected**: 56, and name `record_accumulator.rs` alongside the other two.
- **Actual**: 35, across two named files.

---

## Issue 4: two over-claims in the accounting prose — an inert marker presented as an active one, and a miscount in the trap notes

- **Files**: `src/producer/internals/transaction_manager.rs:9853-9854`;
  `.claude/agent-memory/actor-executor/phase5b_txn_requests_notes.md:46`
- **Severity**: Missing Requirement (records; no count is affected)

**(a) `verifyProducerFenced` is named as a driver but contributes nothing.** The justification
for the load-bearing "no owed test is manager-only" check reads:

```
// transaction_manager.rs:9852-9854
// drive the **accumulator or the `Sender`** — `appendToAccumulator`, a produce response, a
// drain, `initiateClose`, or one of the two helpers that do
// (`verifyCommitOrAbortTransactionRetriable`, `verifyProducerFenced`).
```

`verifyProducerFenced(` matches **0 of the 107**. Its only two call sites,
`TransactionManagerTest.java:2077` and `:2101`, are inside the *private* helpers
`verifyProducerFencedForAddPartitionsToTxn` (`:2066`) and
`verifyProducerFencedForAddOffsetsToTxn` (`:2090`), and the classifier's splitter stops
collecting at `^    private `, so no test method's marker set can ever contain it. The named
sibling `verifyCommitOrAbortTransactionRetriable` does hit (4 of the 47). So one of the "two
helpers" is inert, and the sentence over-states the evidence for the phase's most
load-bearing check.

The check itself survives intact — all 47 are still classified by other markers and
`OWED_MGR` is genuinely 0 — so this is the enumeration, not the conclusion. But it is the
enumeration a reviewer would audit first.

**(b) Trap 2's count.** The note reads *"unanchored `grep -n TITLE` returns three line
numbers"*. For `PHASE-5B METHOD ACCOUNTING` it returns **four** (911, 1019, 9825, 9856);
anchored, one. The point — extra hits break the `$(( ))` arithmetic — is right, and anchoring
is genuinely necessary; only the number is off.

- **Expected**: drop `verifyProducerFenced` from the enumeration (or note that it is inert
  because its call sites sit in unscanned private helpers, which is itself the more
  interesting fact); say "three or four" or name the title each count belongs to.
- **Actual**: an inert marker listed as one of two active ones; "three" where one title gives four.

---

## Verified clean

**The four load-bearing orderings — all correct against Java.**

- *AddPartitions* (`:3899-4020` vs Java `:1559-1631`). Every early `return` precedes the
  `pending_partitions_in_transaction` clear at `:3987`, so only the fall-through clears it, as
  Java's `:1620` does. `CONCURRENT_TRANSACTIONS` (`:3941`) precedes the generic
  `error.is_retriable()` (`:3945`), which matters because `Errors::ConcurrentTransactions` **is**
  in the Rust retriable set (`errors.rs:420`) — swap them and
  `maybe_override_retry_backoff_ms` is dead. The override's plumbing is right end to end:
  per-instance value on `TxnRequestHandlerKind::AddPartitionsToTxn`, reset at the top of each
  response (`:3922`, Java `:1567`), lowered only while `partitions_in_transaction.is_empty()`
  (`:4366-4372`, Java `:1641-1646`), and `retry_backoff_ms()` returning
  `self.retry_backoff_ms.min(*retry_backoff_ms)` (`:827`) = Java's
  `Math.min(TransactionManager.this.retryBackoffMs, this.retryBackoffMs)`. On the snapshot
  question you raised: Java's field is `private final long retryBackoffMs` at
  `TransactionManager.java:130`, so it cannot change after construction and the snapshot is
  faithful — not merely conservative.
- *EndTxn* (`:4034-4126` vs Java `:1747-1793`). `is_abort && TransactionAbortable` (`:4104`)
  precedes the plain arm (`:4117`), matching Java `:1783` before `:1787`; swapped, an abort
  would take `abortable_error` and retry itself, which is what Java's comment says must not
  happen. There are no `TransactionAbortableException` subclasses
  (`grep -rln "extends TransactionAbortableException"` empty), so Java's `instanceof` and the
  Rust code equality coincide. The KIP-890 absorption guard is
  `producer_id != RecordBatch::NO_PRODUCER_ID` (`:4068`), the named constant for Java's literal
  `-1`, and it gates `set_producer_id_and_epoch` + `reset_sequence_numbers` exactly as Java does.
- *AddOffsets* (`:4138-4216` vs Java `:1820-1852`). The success arm does **not** complete the
  result: it hands `Arc::clone(&handler.result)` (`:4170`) to `txn_offset_commit_handler`, so the
  caller's handle resolves only after the second round trip — rules §5's same-object contract,
  and `with_result` (`:1349`-equivalent) stores the given `Arc` with `is_retry: false`, adding no
  second slot. `transaction_started = true` is set here, as Java does. Note this handler puts
  `UNKNOWN_PRODUCER_ID` *before* the fenced arm while `EndTxn` puts it after — each matches its
  own Java handler's order, which is the easy thing to homogenise by mistake and was not.
- *TxnOffsetCommit* (`:4233-4337` vs Java `:1904-1951`). All four properties hold: `break` not
  `return` on every terminal arm so the `:1944` tail always runs; `NONE` removes from the map
  (`:4267`); the retriable arm `continue`s and leaves it (`:4276`); `coordinator_reloaded`
  (`:4253`, `:4272`) bounds the group lookup to once per response. See issue 2 for the doc.

**Rules §9.** The only `instanceof` in the four handlers is `RetriableException`, which
`error.is_retriable()` stands in for at four sites, and at each one the specific codes that are
*also* retriable precede it — `CONCURRENT_TRANSACTIONS` / `NOT_COORDINATOR` /
`COORDINATOR_NOT_AVAILABLE` / `REQUEST_TIMED_OUT`, all confirmed present in `errors.rs`'s
retriable set. Java tests the authorization and fenced codes by exact constant, not by
supertype, so §9's authorization clause does not bite here.
`abortable_error_if_possible` (`:3517-3530`) is a faithful translation of Java `:1379-1388`,
and is **not** a drifting duplicate of `transition_to_abortable_error_or_fatal_error`: Java
carries both too, with the same three shared lines, and they differ in arity and in whether a
handler result is failed. The rustdoc says so at the site.

**TV2.** `maybe_update_transaction_v2_enabled` (Java `:492-504`) matches line for line,
including the early return on the features epoch and the
`!on_initialization && !was_enabled && is_enabled` arming of `client_side_epoch_bump_required`.
All three divergence sites are right: `maybe_add_partition`'s TV2 arm registers straight into
`partitions_in_transaction` **and sets `transaction_started` itself** because no
`AddPartitionsToTxn` response will (Java `:448-451`), kept ahead of the already-added
short-circuit as Java has it; `send_offsets_to_transaction` skips `AddOffsetsToTxn` and likewise
self-sets (Java `:415-418`); `EndTxnHandler` absorbs the server pid/epoch.
`begin_completing_transaction` preserves all three orderings Java's own comment calls out — the
builder is constructed *before* `maybe_update_transaction_v2_enabled(false)`, which itself
precedes the `client_side_epoch_bump_required` check. `test_transaction_manager_enables_v2` pins
the rules-§5 interaction you flagged, and pins it at the right place: after the EndTxn the state
is `Initializing` (not `Ready`), the pending request is `Priority::EpochBump`, and the caller's
handle is still incomplete until `complete_init_producer_id` runs — so the assertions would fail
if `begin_completing_transaction` returned the EndTxn's result instead of the epoch bump's.

**2PC.** `prepare_transaction` matches Java `:342-351` statement for statement.
`setKeepPreparedTxn` re-grepped over `clients/src`: **no hits**, so the response arm is
unreachable for Java's own reason. It is translated in full rather than stubbed, with the
rationale that it is the second of `PREPARED_TRANSACTION`'s two Java sources — correct, and the
better call. The two `#[cfg(test)]` doors (`InitProducerIdRequestBuilder::data_mut`,
`TxnRequestHandler::set_keep_prepared_txn_for_test`) each carry the Java reason at the site and
follow the Phase-3 precedent (`force_enqueue_init_producer_id_for_test`, `current_state`);
neither is reachable from production.

**Both accounting blocks — extracted and run, with guards.** Method: `90 1 89` + `['is2PCEnabled']`,
identical to the paste; both exclusions enforced rather than asserted (the constructor line
fails `decl.match`, the `#[cfg(test)]` cut removes `transaction_manager` from the `fn` set while
the full-file set still contains it); the one miss is the snake-case artefact and
`fn is_2pc_enabled` exists at `:1798` in the production region — so 90/90, zero owed. Test: `awk`
exit 0 and all five checks reproduce (140 / 122 / 18 / 33 / 107); the claimed split is
`33 + 60 + 47 = 140` and both pasted tables regenerate byte-identically. The join is real, not
vacuous: the full cross-tab is 57 `HAVE_MGR`, 3 `HAVE_ACC`, 47 `OWED_ACC`, **0 `OWED_MGR`**, and
the classifier's only error direction (it does not scan private helper bodies) can produce false
*MGR* — every one of which is `HAVE` — so it cannot manufacture that zero. Two of the five
documented traps spot-checked and holding, including trap 3, which is load-bearing: without it
the split would read 59/48. The pass-2 determinism fix stays fixed — 0 of 100 `sender.rs` rows
out of `MARKERS` declaration order.

**Both ownership assignments are consistent with the phases' own wording**, so no PLAN
amendment is needed. §Phase-8 reads *"Close out any `TransactionManagerTest` method not landed
in Phases 3/5, so the full 140 are accounted for"* — the 47 **are** Phase-5 leftovers, which is
literally what that clause covers. §Phase-6's Tests line reads *"the 27 transactional tests in
`KafkaProducerTest.java`, plus the transactional subset of `SenderTest.java`"* — which is the 18.
The reclassification's evidence checks out: 0 of the 18 `SenderTest` bodies names
`commitTransaction(` or `abortTransaction(`, so the old "blocked on a Phase-5b/6 entry point"
rationale did expire, and §9.19's status line was updated rather than left stale.

**Concurrency + `Caller`.** No new locking: every `transaction_manager.lock()` in the production
region is a statement-scoped temporary or an explicitly braced block, none spans an `.await`,
and no `select!` goes near the poll. `Sender::begin_abort`'s new `Arc::clone` exists only to
release the borrow on `self.transaction_manager` before `&mut self.pending_requests` — no lock
added. Deque → manager order untouched. `Caller` sites: see issue 1 for the one exception; the
other ~10 new sites are all correct against their Java call chains.

**DoD, all eleven clauses.** §2 closed at 90/90. §3: 60 new translations with the union
covering all 140 and the 47 owed named-and-justified. §5/§8: no stubs or markers left. §6: no
duplicated translation — `TransactionResult` and `CommittedOffset` both exist in Java, and the
only Java-absent additions are two free helper functions (`sorted_partition_errors`,
`format_partition_errors`), both recorded in §10.8, plus a test-local `enum Stage` inside one
`#[tokio::test]`. §10: the `maybe_add_partition` hot-path audit is **accurate**, and I checked
its load-bearing claim rather than taking it — `TopicPartition` is
`{ partition: i32, topic: Arc<str> }` with derived `Clone` (`topic_partition.rs:21-25`), so the
per-record V2 clones are refcount bumps, not heap allocations; and the "optimisation deliberately
not taken" reasoning is sound, since `txn_partition_map` has a `reset` writer that
`partitions_in_transaction` does not. §11: N/A.

**Checked and deliberately not filed.** The `(Java A-B)` ranges in the "Translated from"
rustdocs are loosely `@Test`-line-to-closing-brace with ±1–3 lines of slop (e.g.
`testTransactionManagerDisablesV2` cites `1033-1075` where the brace is at `1078`, stopping just
short of the two payload lines its own prose calls the payload). No citation points at a
*different* method, which is what made the Phase-4 pass-3 finding substantive; these are
locators, and my two scripted attempts to measure them both misfired before I checked by eye. Not
worth a fix cycle — recorded so the next pass does not re-derive it.

---
---

# Critic 45 — Phase 5b pass 1: RESOLVED (fixup commits over `b70e353`)

All four findings conceded; none disputed. Independently re-derived before fixing.

## Issue 1 (behavioural) — `begin_abort` hardcoded `Caller::App`. FIXED.

Confirmed and **worse than reported**: `KafkaProducer::from_config` still rejects
`transactional.id` until Phase 6, so `Sender::begin_abort` is not merely the second
caller — it is the *only live* one. The hardcoded `Caller::App` was wrong for every
caller that exists today.

Re-derived the Critic's sweep rather than trusting it
(`grep -rn '\.<method>(' clients/src/main/java/.../producer/`):

| method | Java callers | verdict |
|---|---|---|
| `beginAbort` | `KafkaProducer.java:818` **and** `Sender.java:273` | takes `caller` |
| `beginCommit` | `KafkaProducer.java:783` | `Caller::App` correct |
| `sendOffsetsToTransaction` | `KafkaProducer.java:740` | `Caller::App` correct |
| `maybeAddPartition` | `KafkaProducer.java:1045` | `Caller::App` correct |
| `initializeTransactions` | `KafkaProducer.java:652` | `Caller::App` correct |
| `beginTransaction` | `KafkaProducer.java:679` | `Caller::App` correct |
| `prepareTransaction` | none in `clients/src` | see below |

The sweep claim holds: `beginAbort` is the only two-caller entry point. One addition
to the Critic's note on `prepareTransaction` — it has no caller, but
`KafkaProducerMetrics.java:80` carries a *"Total time producer has spent in
prepareTransaction"* metric, which is positive evidence that it is an
application-facing operation rather than merely an untaken guess; recorded at the
method.

`begin_abort` now takes `caller: Caller` and forwards it; `Sender::begin_abort`
passes `Caller::Sender`, and the eleven test call sites pass `Caller::App` (each
models the application-side call `assertAbortableError` / `assertFatalError` /
`KafkaProducer.abortTransaction` makes).

Also verified nothing below the transition needs the caller:
`begin_completing_transaction` performs no transition, and the one it can reach
(`initializeTransactions`'s `!isEpochBump` arm) is unreachable from this path because
an abort requires a live transaction and therefore a valid producer id, so
`is_epoch_bump` is always true.

**Two tests, because one is not enough here.**
`test_begin_abort_poisons_only_on_the_sender_side` (`transaction_manager.rs`) pins
that `begin_abort` *forwards* the caller it is given — App leaves `READY` and
`last_error` untouched, Sender poisons to `FATAL_ERROR` with the recorded error.
That test passes whichever caller `sender.rs` chooses, so
`test_sender_begin_abort_poisons_the_state_machine` (`sender.rs`) pins the *choice*.
`READY` is the invalid source in both, because it is the state the shutdown window
actually produces.

Mutation-checked in both directions:

  - `Sender::begin_abort` reverted to `Caller::App` → the `sender.rs` test fails; the
    forwarding test still passes, which is exactly why both exist.
  - `begin_abort` re-hardcoding `Caller::App` internally → does not compile
    (`error: unused variable: caller` under `#![deny(warnings)]`).

## Issue 2 (design flaw / false mechanism) — TxnOffsetCommit retry prose. FIXED.

Conceded in full; re-read both builders to confirm. Java's
`TxnOffsetCommitRequest.Builder` ctor calls `setTopics(getTopics(pendingTxnOffsetCommits))`
and the Rust `TxnOffsetCommitRequestBuilder::new` copies the same way, so
`reenqueue()` / `retry()` re-send the **full original** list.

Two sites carried the false claim, not one — the handler rustdoc *and* the
`pending_txn_offset_commits` field doc ("so a retry re-sends exactly what is still
outstanding"). Both now state that the map governs the tail's three-way choice and
seeds the *next* construction, and the handler doc carries the explicit warning the
Critic asked for: **do not rebuild the builder before `retry`** — it would send a
reduced request where Java sends the full one.

## Issue 3 (record) — PLAN §10.8 deviation 3 count. FIXED.

35 → **56**, and `record_accumulator.rs` named alongside the other two. Per the
meta-rule the entry now ships the derivation with its real output pasted (56 total;
53 / 2 / 1 per file) plus the `#[cfg(test)]` placement check (first match after the
cut point in each file: 4993 > 4671, 3875 > 2274, 1918 > 1813). The prose no longer
states a bare number at all — it says "every test call site" and lets the derivation
supply the count, which is the shape that cannot drift again.

## Issue 4 (records) — two over-claims. FIXED.

**(a)** Re-derived: `verifyProducerFenced(` matches **0 of the 107**, and dropping it
from the classifier's alternation `diff`s empty against the original run — so it is
provably inert. The paragraph now names `verifyCommitOrAbortTransactionRetriable` as
the one reachable helper (4 of the 47) and keeps `verifyProducerFenced(` in the
alternation with the no-op check shipped, because the *reason* it is inert is the
useful part: its call sites sit inside private helpers the splitter never scans.

That is also promoted to the point the Critic implied — it is the classifier's **one
error direction, and the harmless one**: not following private helpers can only
under-report ACC, i.e. produce a false *MGR*, never a false ACC, so it cannot
manufacture `OWED_MGR == 0`. The four methods it mis-labels are the
`verifyProducerFenced` family (Java 2057, 2062, 2081, 2086) and all four are `HAVE`,
verified by re-running the status derivation.

**(b)** Confirmed: `PHASE-5B METHOD ACCOUNTING` unanchored gives four (911, 1019,
9957, 9988), `PHASE-5B TEST ACCOUNTING` three. Rather than pick a number that will
drift with the next prose edit — it already had, between the Critic's read and mine —
both the accounting block and the memory note now say "**more than one**", state that
the count differs per title and moves with edits, and ship the two-line
anchored-vs-unanchored `grep -c` to re-check. The load-bearing claim (extra hits break
`$(( ))`) is unchanged and still true.

## Not acted on, as instructed

The recorded-not-filed `(Java A-B)` range slop stays recorded. Spot-checked the one
example given — `testTransactionManagerDisablesV2` cited as `1033-1075` where the
brace is at `1078` — and it points at the right method, so no range is *wrong* and no
normalisation sweep was launched.

## Verification

`cargo build` 0 · `cargo test` 0 (2371 lib) · `cargo xtask format-check` 0 ·
`cargo xtask lint` 0 · `make verify-sandbox` 0. All accounting derivations re-run
after the prose edits: `90 1 89` / `['is2PCEnabled']`; guard ok; group A owed 0;
60 HAVE / 47 OWED; `OWED_MGR` 0; both pasted tables still diff byte-identically; the
no-stub `awk` still 0. `producer_perf_test` p99 across three serial runs reported in
the fixup commit, since issue 1 changes `transition_to`'s call shape on the Sender
path.
