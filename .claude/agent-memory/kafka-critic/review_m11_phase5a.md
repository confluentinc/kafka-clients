---
name: review-m11-phase5a
description: M11 Phase 5a (txn state machine) critic loop — closed 7→2→0; accounting-block audit technique, extraction guards, rules-premise falsification
metadata:
  type: project
---

Phase 5a (`TransactionManager` transactional state machine, N=45) **closed in 3 passes:
7 → 2 → 0**. All 9 findings real and conceded; zero false positives. Production code was
clean throughout — **every finding was a record/accounting defect**, and 6 of 9 were created
or invalidated by the change under review.

**Why:** this phase's Actor ships large machine-checked accounting blocks as comments. That
shifts the defect surface from code to *claims about* code, and claims rot when the same
commit changes their premises.

**How to apply:**

## 1. Re-run every shipped derivation from the shipped text — extraction needs guards

The blocks embed `python3`/`awk` programs plus pasted output. Extract with `sed`, strip the
comment prefix, run. Three defects found this way that inspection missed:

- An `awk` that **did not execute at all** (`delete soft` types an array, later `soft = 1`
  aborts on macOS BWK awk — the only awk here; no gawk/mawk). 3 of 5 checks printed `0`.
- A `python3` regex whose stated exclusion was false of itself: `(?:MODS\s+)*` is `*`, so the
  engine backtracks to zero reps and reads `public` as a return type — counting the
  constructor it claimed to exclude, then scoring it *present* against a `#[cfg(test)]`
  helper `fn transaction_manager`. Fix: negative lookahead after the modifier run, and cut
  each Rust file at `\n#[cfg(test)]\n` before the `fn` scan.
- `for (k in hard)` in `emit()` — awk leaves the order unspecified, so pasted output is not
  reproducible. Repaired in one block and **reintroduced in a new block in the same commit
  set**. Check `sort` too: bare `sort -rn` vs `sort -k1,1rn -k2,2` changes tie order.

**Guard the extraction itself.** My first attempt produced an empty program from a mis-quoted
`grep` and would have "diffed" two empty files — the same defect class. Assert before running:
non-empty, balanced `{`/`}`, even single-quote count. The Actor independently hit and recorded
this. Verify claimed counts by *rebuilding the artifact independently* (e.g. recompute each
Java test block's marker set from a body delimited by its closing `    }` line) rather than
re-running the same program.

## 2. Audit exhaustiveness claims — "only reader", "in full", "every touch"

`CoordinatorNodes` prose said `Sender.java:481` was the coordinator nodes' *only* reader.
`handleCoordinatorReady` (`TransactionManager.java:1104-1105`) reads `transactionCoordinator`
as a **field**, not through the accessor — and `handle_coordinator_ready(&CoordinatorNodes)`
exists *because* of that reader, so the signature contradicted the prose beside it. Grep the
Java field name and enumerate all sites; accessor-only greps miss direct field reads.

## 3. A phase that deletes a guard invalidates every skip rationale citing it

Phase 5a removed `TransactionManager::new`'s transactional-id guard. Fallout:
- `begin_abort`'s doc still said "unreachable while `new` refuses a transactional id" — the
  sweep removed 5 such comments and missed this one.
- The `SenderTest` accounting deferred **18** tests under "not expressible while
  `TransactionManager::new` refuses a transactional id". Re-deriving per entry made **3**
  translatable with zero new production surface (Java 636, 689, 2991). Two only needed
  `MockClient` features that already existed — `delay_ready` had **zero callers in the tree**,
  which is itself the tell.

Grep the deleted premise's text repo-wide after any guard removal. Distinguish phase labels
by *layer*: `KafkaProducer::from_config`'s guard is Phase 6 (public API); `begin_abort` is 5b
(manager entry point). Both correct, differently.

## 4. Falsify the rules file, don't just apply it

`producer-transactions.md` §2 lists 4 fields as "touched exclusively by the Sender thread".
The Actor deviated for `coordinatorSupportsBumpingEpoch` and **was right**: Java reads it on
the app thread via `KafkaProducer.java:1066` → `:781` → `:1310`. Stronger — every Java *read*
holds the monitor while the sole *write* (`:1110`) does not, the inverse of confinement.
Walk the cited call chain line by line before crediting *or* faulting a §2 deviation; file a
rule-update suggestion through the process rather than editing rules.

## 5. Test-fidelity patterns specific to this phase

- **Assertions redirected to a sibling object.** Java asserted on `initPidResult`; Rust
  asserted the same two properties on the *FindCoordinator* result (a different
  `TransactionalRequestResult`) and reduced the first to `is_completed()`. The excusing
  comment said "Java asserts on `initPidResult` in the sibling test" — Java asserts them in
  that very method, and the named sibling was group-B/untranslated.
- **Half a test body, silently.** `test_node_not_ready` initially covered Java 689-706 +
  710-711, dropping `client.throttle` (`:708`) and its paired second FindCoordinator response
  (`:709`) — while the rustdoc documented two *smaller* timing departures. Java's own javadoc
  ("FindCoordinator **or** InitProducerId") is the checklist.
- **`is_completed()` probe before an await is an improvement, not a weakening.** Java's no-arg
  `await()` is `await(Long.MAX_VALUE, ..)` and hangs when nothing completes the result; the
  probe converts that to a failure. Verify by walking each mutation class (never-failed /
  `done()` / wrong error / right error) and confirm `await_result` checks `completed` before
  parking, so a passing probe cannot precede a hang.
- **Java MockTime auto-tick margins are coin flips.** `MockTime(10)` + `REQUEST_TIMEOUT + 20`
  is a *two-tick* margin against a window that closes after `REQUEST_TIMEOUT` of clock
  advance; two incidental clock reads flip the branch. `2 * REQUEST_TIMEOUT` + explicit
  `sleep` is a sound substitution **when the branch is pinned by an assertion** (e.g. observing
  `metadata.request_update`) rather than inferred from timing.
- **Priority snapshot-at-insertion is exactly Java, not merely conservative:**
  `InitProducerIdHandler.isEpochBump` is `private final`, so `priority()` cannot change while
  the element sits in the queue.

## 6. Arithmetic that "reconciles" can still hide a wrong entry

The brief flagged "91 = 83 + 9 = 92". The real defect was different: the block *did* disclose
`beginAbort` in both groups (91/83/82 were each **one too high** from the phantom constructor).
Derive ground truth independently before accepting either the block's total or the brief's
summary of it.
