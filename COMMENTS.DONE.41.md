# Critic 41 — Milestone 11 Phase 1: REVIEW LOOP CLOSED

Three Critic passes per `agent-roles.md` steps 2-6. Converged clean on pass 3.

| Pass | Findings | Outcome |
|---|---|---|
| 1 | 8 | 7 fixed (`0a8612e`), 1 partially rejected (`COMMENTS.FP.md`) |
| 2 | 1 | Fixed (`363a7c7`) — the reenqueue path; the pass-1 audit was wrong |
| 3 | **0** | **Clean. Phase 1 signed off.** |

## Pass 2 — the finding that mattered

The pass-1 fix for finding 1 added an error when a tracked key has no matching
batch in the caller's pool, justified by an audit claiming no reachable Java flow
hits that state. The audit was wrong: `Sender.reenqueueBatch`
(`Sender.java:750-752`) does not call `transactionManager.removeInFlightBatch`,
unlike the split path at `:685`, so a retried batch leaves the Sender's map while
staying tracked. Java asserts this at `RecordAccumulator.java:558-560`. Erroring
there would have broken idempotent recovery outright.

Writing the regression test then exposed a **second** defect in the pass-1 fix:
it resolved and mutated in one pass, leaving already-visited batches rewritten
when a later key was missing. Now resolves every key before mutating.

## Pass 3 — clean, with three disclosures

The Critic recorded three items it judged **not** to be findings, so the judgement
is visible rather than implicit: the archived PLAN header's "sole owner" phrase
(deferred to the corrected rules), `TxnPartitionMap`'s singular "their owner"
phrasing, and `adjust_sequences_due_to_failed_batch` decrementing `next_sequence`
before the missing-key error (precisely scoped, and mirrors Java).

The first two were tightened anyway in the closing commit — cheap, and they sit
exactly where Phase 4 will read.

It also disclosed that the small Java line-number offsets in its own pass-2
write-up were its error, not the Actor's; the submodule is unchanged at `a18251b`.

---

# Full pass-3 report

# Critic 41 — Milestone 11 Phase 1, third pass (fix verification)

Commit reviewed: `363a7c7` — "fixup! Phase 1: address Critic 41 second-pass
finding — reenqueue path". Java contract: `kafka/` submodule at Apache Kafka
4.2.0. I confirmed the submodule has not moved: `git submodule status` and
`git ls-tree HEAD kafka` both report `a18251bae0b825c69794a50dffd4c3100cf5ca5b`
(4.2.0), the same commit the first two passes were reviewed against.

Verified locally at `363a7c7`: `cargo test --lib` 2194 passed / 0 failed /
1 ignored (22 in `txn_partition_entry`, including the new test),
`cargo xtask format-check` clean, `cargo xtask lint` clean.

# NO FINDINGS

The second-pass finding is fully resolved, the second defect it exposed is
correctly fixed, and I found nothing new. Details of what I verified follow, then
two disclosures that are explicitly **not** findings — recorded because the
coordinator asked pointed questions about them and full disclosure is more useful
than silence.

---

## 1. Documentation corrections — verified

**The "sole owner" claim is gone from every place that governs behaviour.**
I grepped the repo (excluding `kafka/` and the COMMENTS files). Three sites
previously carried it; all three are corrected:

  - `.claude/rules/producer-transactions.md` §7 — now "takes ownership", followed
    by a dedicated paragraph headed "**Ownership alternates — the Sender's map is
    NOT the permanent sole owner.**"
  - `txn_partition_entry.rs:48-56` (type-level doc) — "takes ownership" plus
    "Ownership **alternates**: on the retry path it moves back to the
    accumulator's deque while the batch stays tracked here, so a tracked batch
    lives in either owner. Callers assembling the batch pool must draw from both".
  - `txn_partition_entry.rs:294-320` (`reset_sequence_numbers` doc) — rewritten to
    document **both** mismatch directions under explicit sub-headings
    ("Pool ⊃ tracked", "Tracked ⊃ one owner"), where before it documented only the
    first.

**The corrected text does state the invariant a Phase 3-6 reviewer needs.** It is
actionable in three distinct ways, which is what I was looking for:

  - It names the mechanism (`Sender.reenqueueBatch` calls
    `accumulator.reenqueue(..)` then `maybeRemoveFromInflightBatches(..)` **without**
    `transactionManager.removeInFlightBatch`), and contrasts it with the split path
    that does — so a reviewer can check the Rust equivalent rather than take it on
    faith.
  - It cites Java's own assertion of the invariant
    (`RecordAccumulator.insertInSequenceOrder`), which is the strongest available
    evidence and is stronger than my second-pass write-up cited.
  - It states the obligation as a **MUST** on the caller ("Phase 4 MUST assemble
    the pool from **both** owners") and adds a matching anti-pattern: "Building the
    lookup pool from `Sender::in_flight_batches` alone, omitting reenqueued batches
    sitting in the accumulator's deque." That anti-pattern is the checkable form —
    a Critic reviewing Phase 4 can grep for the pool construction and compare.

**The `:655` / `:652-653` reasoning is accurately represented**, and so is every
other Java citation in the commit and the docs. I re-derived all of them with
`grep -n` rather than by eye this time:

| Cited | Authoritative | Verdict |
|---|---|---|
| `TransactionManager.java:655` (`startSequencesAtBeginning`) | `:655` | exact |
| `:652-653` (the "rewrite all in-flight sequences" comment) | comment at `:653`, loop `:654` | range covers it |
| `Sender.java:750-752` (`reenqueueBatch` body) | signature `:750`, the two cited statements `:751`/`:752` | exact |
| `Sender.java:685` (split-path `removeInFlightBatch`) | guard `:685`, call `:686` | covers the two-line guard |
| `RecordAccumulator.java:558-560` (the assertion) | `if` at `:558`, `throw` at `:559-560` | exact |
| `Sender.java:848` / `:854` (`handleFailedBatch` before dealloc) | `:848` / `:854` | exact |
| `TransactionManager.java:790` / `:818` / `:1058` | all as cited | exact |

Worth recording for the archive: the slight offsets in **my own** second-pass
write-up (`:846`, `:684-685`, `:556-558`, `:749-753`) were my error from counting
inside `sed` output, not a submodule difference. The Actor's numbers are the
correct ones.

## 2. The atomicity fix — verified

`reset_sequence_numbers` (`txn_partition_entry.rs:324-369`) now runs a resolve
pass over `tracked` that returns `Err` on the first unresolvable key, then a
separate mutation pass over the resolved indices.

**Is the error path atomic? Any way to mutate a batch and still return `Err`?**
There are exactly two `Err` exits, and the split puts them on opposite sides of
the mutation boundary:

  - The missing-key `return Err(...)` (`:353-357`) is now unreachable from any
    point after a mutation — the resolve loop touches only `pool`, `tracked` and
    `resolved`, none of which is a batch. **Atomic with respect to the caller's
    batches, and with respect to the entry** (`inflight_batches_by_sequence` is
    assigned only at `:368`, after the mutation loop; `start_sequences_at_beginning`
    assigns `producer_id_and_epoch` / `next_sequence` / `last_acked_sequence` only
    after the `?` at `:220`).
  - `reset(batch)?` (`:365`) can fire mid-mutation. For
    `start_sequences_at_beginning` the closure is infallible (`Ok(())` at `:219`),
    so this cannot fire there at all. For
    `adjust_sequences_due_to_failed_batch` it is the negative-sequence rejection —
    the deliberately non-atomic path.

**Can the `resolved` indices go stale?** No, and this is structurally guaranteed
rather than incidental:

  - `batches: &mut [&mut ProducerBatch]` is a slice — fixed length, so no element
    can be added or removed.
  - `reset: FnMut(&mut ProducerBatch)` receives a single batch, never the slice, so
    it cannot reorder or resize it. Both concrete closures capture only locals
    (`sequence`; `base_sequence` / `record_count` / a cloned `topic_partition`) and
    call `batch.reset_producer_state(..)`.
  - The first-pass `batches.sort_by_key(..)` — the one thing that *did* permute the
    slice — was removed when the pool index was introduced. Nothing permutes the
    slice now.
  - The borrow checker enforces it independently: while `&mut *batches[index]` is
    live, nothing else can reach `batches`.

I also checked that `resolved` cannot contain a duplicate index, which would
double-mutate one batch: `tracked` keys are unique (`BTreeSet`), and each pool
index is inserted exactly once under exactly one key, so index→key is injective
over the reachable entries. (Had it not been, sequential `&mut` reborrows would
still be sound, just semantically wrong.)

**Is the non-atomic negative-sequence path the right call?** Yes — I re-read the
Java to confirm rather than accept the claim.
`TxnPartitionEntry.resetSequenceNumbers` (`:154-161`):

    TreeSet<ProducerBatch> newInflights = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
    for (ProducerBatch inflightBatch : inflightBatchesBySequence) {
        resetSequence.accept(inflightBatch);   // throws here on element k
        newInflights.add(inflightBatch);
    }
    inflightBatchesBySequence = newInflights;  // skipped

When the lambda throws on element *k*, elements 1..k-1 have already had
`resetProducerState` applied, `newInflights` is discarded, the field is not
swapped, and `nextSequence` was already decremented by `decrementSequence` at
`:140`. Java is non-atomic in exactly the way described, so `reset(batch)?`
firing mid-loop is the faithful translation. If this had been "fixed" to be
atomic it *would* have been a new divergence — the Actor's reasoning is right and
inverting it would have been the error.

**Does the change alter the success path?** No. The resolve pass cannot fail when
every key resolves, and it produces indices in `tracked` order — the same order
the single loop used — so the mutation sequence, the resulting `new_inflights`,
and the assignment are bit-identical to before. The only difference is one
`Vec<usize>` allocation of length `|tracked|`. That is not a hot-path concern
under CLAUDE.md §11 / DoD §10: `reset_sequence_numbers` runs per epoch bump or per
fatally-failed batch, not per record or per drain.

## 3. The regression test — verified, and it is genuinely discriminating

`test_reenqueued_batch_stays_tracked_and_must_be_supplied`
(`txn_partition_entry.rs:738-787`).

**It models the scenario faithfully.** Two batches tracked for one partition:
`still_in_flight` = `(pid 1, epoch 0, base 0, 2 records)` and `reenqueued` =
`(pid 1, epoch 0, base 2, 3 records)`, with `next_sequence` driven to 5. That is a
consistent post-drain state — sequences 0-1 and 2-4 assigned contiguously, counter
at 5 — not an arbitrary arrangement. Half one supplies only the Sender's batch and
asserts the error plus untouched entry state; half two supplies both owners'
batches and asserts success.

**The asserted base sequences (0 and 2) are what Java produces.** Java's
`startSequencesAtBeginning` (`:116-125`) starts `sequence` at 0, iterates the
`TreeSet` in comparator order — here `(1,0,0)` then `(1,0,2)` — and for each calls
`resetProducerState(newProducerIdAndEpoch, sequence)` then
`sequence += recordCount`. So the first batch gets base 0, the second gets base
0+2 = 2, and `nextSequence` ends at 2+3 = 5. The test asserts exactly 0, 2 and 5,
plus `producer_id_and_epoch == (7,1)` and two tracked keys. Correct on every value.

**It would catch a regression to the non-atomic version**, and I traced why
precisely. Under the non-atomic body, half one resolves `(1,0,0)`, rewrites
`still_in_flight` to `(7,1,0)`, then hits the missing `(1,0,2)` and errors — so
half one's own assertions still pass, because the entry's fields are assigned only
after the `?`. The failure surfaces in half two: `still_in_flight` is now keyed
`(7,1,0)` while the entry still tracks `(1,0,0)`, so the tracked key no longer
resolves and the `.expect(..)` panics. That is precisely the failure the commit
message reports having hit while writing the test.

One detail worth calling out as good design rather than accident: the test puts
the **missing** key second. Had `reenqueued` been the lower-keyed batch, half one
would error on the very first key before mutating anything, half two would pass,
and the test would *not* distinguish the atomic from the non-atomic version. The
chosen ordering is what makes it a real regression test.

---

# Disclosures — examined and judged not to be findings

Recorded so the judgement is visible rather than implicit. I applied the same bar
I used on the first pass, which excludes precision nits that do not affect
correctness and where the authoritative source is right.

**(a) One "sole owner" phrase survives in an archived plan.**
`design/history/Milestone-11/PLAN.md:15-16` still reads "`Sender::in_flight_batches`
(`sender.rs:122`) is already its sole owner after `drain()` moves it out of
`RecordAccumulator`". Not a finding: the paragraph exists to justify why
`BTreeMap<_, ProducerBatch>` is unimplementable — a conclusion that holds whichever
owner currently holds the batch, since neither permits a second owner of a
non-`Clone` type — it lives under `design/history/` as an archived record, and it
explicitly states it is "superseded by `.claude/rules/producer-transactions.md` §7
and §8" and closes with "see rules file §7". The reader is routed to the corrected
authority. Flagging this would be manufacturing a finding.

**(b) `TxnPartitionMap`'s two pass-through doc comments still say "supplied by
their owner" (singular).** `txn_partition_map.rs:121-122` and `:179-181`. These are
the methods Phase 4 calls directly, so I considered this seriously — the phrase
"the in-flight batches for the partition" is ambiguous in exactly the way that
caused this round, and `Sender::in_flight_batches` is literally named that. I judged
it below the bar because: the statement is not false (for a reenqueued batch, "their
owner" *is* the accumulator); both lines explicitly redirect to
`.claude/rules/producer-transactions.md` §7, which is now correct and carries the
explicit anti-pattern; and the delegate's own doc
(`TxnPartitionEntry::reset_sequence_numbers`) spells out both owners at length. The
governing rule and the anti-pattern are what a Phase-4 review will check against,
and both are right. Tightening the two lines would be a small improvement, not a
correction — worth folding into any future edit of that file, not worth another
round.

**(c) `adjust_sequences_due_to_failed_batch` mutates `next_sequence` before the
missing-key error can fire.** `self.decrement_sequence(record_count)?` runs at
`:252`, before `reset_sequence_numbers`. So on a missing-key error from *that*
entry point, `next_sequence` is already decremented even though no batch was
touched. Not a finding, and not an overclaim by the Actor: both the in-code comment
(`:338-339`) and the commit message scope the guarantee precisely to "the caller's
batches untouched", which is exactly what holds. The ordering mirrors Java
(`TxnPartitionEntry.java:140` decrements before `resetSequenceNumbers`), Java has no
missing-key error to compare against, and the resulting state is no worse than the
state Java reaches via its own negative-sequence throw — which `Sender.failBatch`
swallows at `:849-851` in both languages.

---

# Sign-off

**Phase 1 can be signed off.**

All eight original findings are closed: seven fixed and independently re-verified
across passes two and three, and finding 2 correctly rejected on reachability
grounds with the accepted documentation half fixed. The second-pass finding is
fixed, and the second defect it exposed — the non-atomic error path — is fixed
correctly, with the one path that *should* stay non-atomic left non-atomic for the
right reason. The new regression test covers the matrix cell the first-pass audit
wrongly declared unreachable and genuinely discriminates against the defect it was
written for.

`cargo test --lib` 2194 passed, `cargo xtask format-check` and `cargo xtask lint`
clean. DoD gate 9 (`make verify`) remains unrun for the C and Python suites, which
need Docker and a virtualenv — unchanged from the first pass and independent of
this commit.

---

# Passes 1 and 2 (archived earlier)

# Critic 41 — Milestone 11 Phase 1 review: RESOLVED

All 8 findings addressed in commit `0a8612e`. Seven fixed; finding 2 partially
rejected (see `COMMENTS.FP.md`).

| # | Finding | Resolution |
|---|---|---|
| 1 | `reset_sequence_numbers` took membership from the caller's slice | **FIXED** — rebuild now driven by the tracked key set; pool semantics (untracked ignored, missing tracked = error); `start_sequences_at_beginning` returns `Result`; rules §6/§7 tightened; 5 regression tests |
| 2 | Guard absent from `with_client` — "reachable public bypass" | **PARTIALLY REJECTED** — not externally reachable (`pub(crate)` argument types). Misleading rustdoc and unrecorded plan narrowing were real and are fixed. See `COMMENTS.FP.md` |
| 3 | 3 of 5 cases dropped from the in-flight config test | **FIXED** — all three restored |
| 4 | Flat error codes lose `UnknownProducerId <: OutOfOrderSequence` | **FIXED** — recorded as rules §9 with the concrete Phase 5 match shape |
| 5 | `transaction_aborted()` inherited the wrong rustdoc | **FIXED** — constructors reordered; both documented |
| 6 | Java `log.debug` translated as `kafka_trace!` | **FIXED** — `kafka_debug!` |
| 7 | Import via file module path, not parent re-export | **FIXED** — and one `#[allow(unused_imports)]` became redundant and was removed, as predicted |
| 8 | Rules §5 mandated `Arc<Notify>`; code uses `Notify` | **FIXED** — rules corrected to match the code, plus the `Arc<TransactionalRequestResult>` corollary |

Verification after fixes: 2117 lib tests (5 new) + 152 across the other suites,
`cargo xtask format-check` and `cargo xtask lint` clean. `make verify` still not
fully run — the C and Python suites need Docker and a virtualenv.

---

# Original review follows

# Critic 41 — Milestone 11 Phase 1 review

Range reviewed: `5a19ffc..d14d1ec` (8 commits, 16 files, +3743/-10).
Java contract: `kafka/` submodule at Apache Kafka 4.2.0 (`a18251b`).

Verified locally at the time of review: `cargo test --lib` 2112 passed / 0 failed,
`cargo xtask format-check` clean, `cargo xtask lint` clean. The three later commits
on `master` do not touch any Phase-1 file, so these results apply to `d14d1ec`.

Findings are ordered most severe first.

---

## Issue: `reset_sequence_numbers` replaces the tracked key set from an unvalidated caller slice

- **File**: `src/producer/internals/txn_partition_entry.rs:274-289` (also `:198-217`, `:238-266`)
- **Severity**: Design Flaw (latent Bug in Phase 4)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionEntry.java:154-161`; call-order evidence at `Sender.java:846` vs `:854` and `TransactionManager.java:790` / `:818`

**Description.** Java's `resetSequenceNumbers` iterates **its own** `inflightBatchesBySequence`
and re-adds exactly those elements to the fresh `TreeSet`. Membership is invariant;
only the sort keys change. The Rust translation builds `new_inflights` purely from the
caller-supplied `batches` slice and then assigns it to `self.inflight_batches_by_sequence`,
so the *membership* of the tracked set becomes whatever the caller happens to pass.
There is no validation, no `debug_assert`, and no `Err` path for a mismatch.

Two concrete consequences:

1. `batches = &mut []` **silently clears** a non-empty tracked set. Via
   `start_sequences_at_beginning` it additionally sets `self.next_sequence = sequence`
   (line 215), i.e. `0`, so an empty or short slice silently rewinds the partition's
   next sequence — precisely the corruption this class exists to prevent, and it
   surfaces only later as broker-side `OUT_OF_ORDER_SEQUENCE_NUMBER`. Both public
   entry points invite this shape: `TxnPartitionMap::adjust_sequences_due_to_failed_batch(&failed, &mut [])`
   is exactly what `txn_partition_map.rs:381` does.
2. A slice that is a **superset** silently inserts keys Java had deliberately removed.

**Why this is not hypothetical.** The rules file (§7) names `Sender::in_flight_batches`
(`sender.rs:122`) as the sole owner that will supply `batches`. That set is deliberately
*not* the same as the entry's tracked set at the moment of the call:

- `Sender.failBatch` calls `transactionManager.handleFailedBatch(...)` at `Sender.java:846`,
  and only afterwards `maybeRemoveAndDeallocateBatch(batch)` at `Sender.java:854` removes
  the batch from `Sender.inFlightBatches`.
- `handleFailedBatch` calls `removeInFlightBatch(batch)` first (`TransactionManager.java:790`)
  and *then* `txnPartitionMap.adjustSequencesDueToFailedBatch(batch)` (`:818`).

So at the call moment the failed batch is **already out of** the txn map's tracked set but
**still in** `Sender.inFlightBatches`. If Phase 4 passes the Sender's list verbatim — the
only thing it has — the Rust code will (a) re-insert the failed batch's key, and (b) shift
the failed batch's own base sequence by `record_count`, driving it negative whenever the
failed batch was the lowest in flight. That is the spurious error path the entry's own
tests already document at `txn_partition_entry.rs:481-484` and `:509-523`.

**Expected.** Drive the rebuild from `self.inflight_batches_by_sequence`: build a
`key -> &mut ProducerBatch` index from `batches`, iterate the tracked keys in order,
apply `reset` to the matching batch, and collect the new key. A tracked key with no
matching batch should be a `KafkaError` (or at minimum a `warn!`), and a supplied batch
that is not tracked should be ignored rather than inserted. That reproduces Java's
element-preserving rebuild exactly and makes any caller mismatch loud instead of silent.

**Also.** `.claude/rules/producer-transactions.md` §6 "How to apply" says only "collects,
mutates, and re-inserts under the new keys" and §7 says the two methods "receive mutable
access to the batches from the owner". Neither states that the element set must be
preserved. Since the rules file is the law Phases 3-6 are reviewed against, it should be
tightened along with the code — otherwise the next reviewer has nothing to check against.

**Verified separately (not a finding):** the plan override itself is sound.
`ProducerBatch` is not `Clone` (`producer_batch.rs:85`, no `derive(Clone)`) and
`Sender::in_flight_batches: HashMap<TopicPartition, Vec<ProducerBatch>>` owns batches by
value, so the approved `BTreeMap<(i64,i16,i32), ProducerBatch>` shape genuinely does not
compile without a second owner. The `Arc<Mutex<ProducerBatch>>` rejection is also fairly
characterised. The signature *can* be satisfied at the Phase 3/4 call sites
(`TransactionManager.java:594`, `:655`, `:818`, `:1048` are all reached from the Sender
task, which owns the batches). The defect is the missing membership invariant, not the
ownership decision.

---

## Issue: `MILESTONE-11 GUARD` is absent from `with_client`, leaving a reachable public bypass

- **File**: `src/producer/kafka_producer.rs:250-282` (guard), `:389` (`with_client`), `:156` (`new`)
- **Severity**: Missing Requirement
- **Java Reference**: `KafkaProducer.java:592-620` (`configureTransactionState`), the behaviour the guard stands in for
- **Plan Reference**: `design/history/Milestone-11/Phase-1/PLAN.md:364` ("In `from_config` (line 234) **and `with_client` (356)**") and `design/history/Milestone-11/PLAN.md:675` ("In `KafkaProducer::from_config`/`with_client`")

**Description.** The approved plan places the guard in both `from_config` and `with_client`.
It was implemented only in `from_config`. `with_client` is `pub`, generic over the public
`KafkaClient` trait (`src/lib.rs:65`), and its own rustdoc at `kafka_producer.rs:382` reads
"This corresponds to the primary public constructor in Java's KafkaProducer." An external
caller can therefore construct a `KafkaProducer` from a config with
`transactional_id = Some(..)` or explicit `enable.idempotence=true` and get silent
at-least-once delivery — exactly the CLAUDE.md §5 hole ("silently completing … is worse
than an explicit error") the guard exists to close.

The stated rationale — that `new`/`with_client` return `Self` rather than `Result` and are
test-injection plumbing — does not hold up. The rustdoc contradicts the "plumbing"
characterisation, and the return type is a design choice, not a constraint: `from_config`
already returns `Result` and `with_client` is the function it delegates to. Changing
`with_client` to `Result<Self, KafkaError>` is a source-compatible-with-one-`?` change
inside the crate; the only external callers are hypothetical.

**Expected.** Either move the guard into `with_client` (changing its return type to
`Result`), or state in the code and in the Phase 4/6 plans that `with_client`/`new` are
knowingly unguarded and why that is acceptable. Silently narrowing an approved plan item
from two constructors to one, in the commit that claims to implement it, is the part that
needs correcting regardless of which option is chosen.

---

## Issue: three of five cases dropped from `testInflightRequestsAndIdempotenceForIdempotentProducers`

- **File**: `src/producer/producer_config.rs`, `test_inflight_requests_and_idempotence_for_idempotent_producers`
- **Severity**: Missing Requirement (DoD §3)
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java:413-470`

**Description.** The Java test has five cases; the Rust translation has two
(`validProps` and `invalidProps1`). Missing:

| Java case | Config | Expected |
|---|---|---|
| `invalidProps2` | `max.in.flight=5`, `enable.idempotence=false`, `transactional.id` | `ConfigException` |
| `invalidProps3` | `max.in.flight=6`, `enable.idempotence=true` | `ConfigException` |
| `invalidProps4` | `max.in.flight=6`, `transactional.id` | `ConfigException` |

All three pass against the current implementation, so this is a coverage gap rather than a
live bug — but `invalidProps2` is the only case anywhere in the translated suite that pins
"in-flight exactly at the cap + idempotence explicitly off + transactional id ⇒ error",
i.e. that the in-flight arm does not mask the transactional-id arm. `invalidProps3` and
`invalidProps4` pin that the in-flight arm fires ahead of the retries/acks arms. The other
three config tests (`testAcksAndIdempotence…` 8/8, `testRetriesAndIdempotence…` 5/5,
`testOverwriteAcksAndRetries…` 4/4) and both `ProducerConfigTest` tests are complete —
this is the only gap I found.

---

## Issue: flattening the txn exception hierarchy loses `UnknownProducerIdException <: OutOfOrderSequenceException`

- **File**: `src/common/kafka_error.rs` (commit `3964d64`), and the rationale in that commit message / `design/history/Milestone-11/PLAN.md:1.1`
- **Severity**: Design Flaw (behavioural divergence scheduled to land in Phase 5)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/errors/UnknownProducerIdException.java:26`; dispatch at `TransactionManager.java:799` and `:806`

**Description.** The decision not to add typed structs for the five subclasses is correct
as far as it goes, and I verified every claim behind it: all five wire codes are present in
`src/common/protocol/errors.rs` (`OutOfOrderSequenceNumber=45:77`, `InvalidTxnState=48:80`,
`TransactionalIdAuthorizationFailed=53:85`, `UnknownProducerId=59:91`,
`TransactionAbortable=120:152`), none of the five Java classes carries payload beyond a
message, `TransactionAbortedException` is genuinely absent from `Errors.java` (only
`TRANSACTION_ABORTABLE(120, …)` at `Errors.java:408` exists), and its
`is_api_exception()==true` / `is_kafka_exception()==true` classification is right
(`TransactionAbortedException extends ApiException`, unlike `WakeupException`).

The rationale is nevertheless incomplete: it reasons only about *payload*, while the Java
*hierarchy* is load-bearing here.

- `UnknownProducerIdException extends OutOfOrderSequenceException`. `TransactionManager.handleFailedBatch`
  dispatches on that relation:
  - `:799` `if (exception instanceof OutOfOrderSequenceException && !isTransactional())` →
    `requestIdempotentEpochBumpForPartition`. This branch **also matches an
    UnknownProducerId error**, so the *idempotent* producer takes the epoch-bump path.
  - `:806` `else if (exception instanceof UnknownProducerIdException)` →
    `resetSequenceForPartition`. Only the *transactional* producer ever reaches it.

  With flat codes, `Errors::UnknownProducerId` and `Errors::OutOfOrderSequenceNumber` are
  unrelated values. A literal `match` translation of that `if / else if` chain will route
  the idempotent UnknownProducerId case to `reset_sequence_for_partition` instead of
  `request_idempotent_epoch_bump_for_partition` — a real divergence in the idempotent
  recovery path, in the one place the codes cannot be treated as peers.
- Secondarily, `TransactionalIdAuthorizationException extends AuthorizationException`
  (`:19`), not `ApiException` directly; any `instanceof AuthorizationException` site in
  Phases 5/6 needs the same care.

**Expected.** Record the subtype relation as a rule now, while Phase 1 owns the error
surface — e.g. a `.claude/rules/producer-transactions.md` section stating that
`Errors::UnknownProducerId` must be treated as an `OutOfOrderSequenceException` wherever
Java uses `instanceof`, ideally backed by a helper (`fn is_out_of_order_sequence(&self)`)
so the two call sites cannot drift. Discovering this in Phase 5 means rewriting the
dispatch after the tests are written against the wrong shape.

---

## Issue: `transaction_aborted()` inherited `concurrent_modification`'s rustdoc; `concurrent_modification` lost its own

- **File**: `src/common/kafka_error.rs:409-431`
- **Severity**: Bug (documentation of public API)
- **Java Reference**: `TransactionAbortedException.java:35-37` vs `java.util.ConcurrentModificationException`

**Description.** The two new constructors were inserted **between**
`concurrent_modification`'s doc comment and its `fn` item. The result is that
`transaction_aborted()`'s rendered rustdoc now begins:

> Create a concurrent modification error.
> Corresponds to Java's `ConcurrentModificationException` thrown by `KafkaConsumer.acquire()`
> when the consumer is accessed from more than one thread.
> Create a transaction aborted error with Java's default message. …

and `pub fn concurrent_modification` at line 429 has no documentation at all. Both are
public API. CLAUDE.md §4 requires documentation to be faithful, and this attaches consumer
documentation to a producer-transaction constructor.

**Expected.** Move the two new constructors after `concurrent_modification`'s body (or
move `concurrent_modification`'s doc comment back onto its own item).

---

## Issue: `log.debug` translated as `kafka_trace!`

- **File**: `src/producer/internals/txn_partition_map.rs:194-200`
- **Severity**: Behavior Mismatch (low)
- **Java Reference**: `TxnPartitionMap.java:111-112` — `log.debug("producerId: {}, send to partition {} failed fatally. Reducing future sequence numbers by {}", …)`

**Description.** Java logs this at DEBUG; the translation uses `kafka_trace!`. At any
normal log level the message disappears, and this is the only diagnostic emitted when a
partition's future sequence numbers are shifted after a fatal batch failure — exactly the
event an operator debugging `OUT_OF_ORDER_SEQUENCE_NUMBER` would look for.

This is not a codebase convention: `kafka_debug!` exists (`src/common/utils/log_macros.rs:60`)
and is used elsewhere, and the neighbouring `update_last_acked_offset` correctly uses
`kafka_trace!` for Java's `log.trace` (`TxnPartitionMap.java:98`). So the file gets one of
the two levels right and one wrong.

**Expected.** `kafka_debug!`.

---

## Issue: import via file module path instead of the parent-module re-export

- **File**: `src/producer/internals/txn_partition_map.rs:30`
- **Severity**: Missing Requirement (CLAUDE.md §2; Phase-1 PLAN §4 DoD gate 1 lists it explicitly)

**Description.** `use crate::producer::internals::txn_partition_entry::{InFlightBatchKey, TxnPartitionEntry};`
uses the file module path. CLAUDE.md §2 requires struct imports to go through the parent
module re-export, and `src/producer/internals/mod.rs:44` already provides it. The correct
form is `use crate::producer::internals::{InFlightBatchKey, TxnPartitionEntry};`.

Corroborating signal: `mod.rs:43` needs `#[allow(unused_imports)]` on that very re-export
precisely because nothing consumes it — the one in-crate consumer bypassed it. Fixing the
import removes one of the three `#[allow(unused_imports)]` attributes.

The sibling imports are all correct, including the two that *should* use the file path per
CLAUDE.md §2's constant/static-function rule
(`crate::common::record::default_record_batch::increment_sequence`,
`crate::common::requests::produce_response::INVALID_OFFSET`).

---

## Issue: rules file §5 mandates `notify: Arc<Notify>`; the implementation uses a plain `Notify`

- **File**: `src/producer/internals/transactional_request_result.rs:62` vs `.claude/rules/producer-transactions.md:183` and `design/history/Milestone-11/Phase-1/PLAN.md:159`
- **Severity**: Design Flaw (low — rules/code inconsistency, not a behavioural defect)

**Description.** Both the rules file (written in commit `251ace8` as the design law for
Phases 3-6) and the Phase-1 plan specify the field as `notify: Arc<Notify>`. The
implementation uses a plain `Notify`. The plain field is the *better* choice — the whole
struct must live in an `Arc` anyway, because `handleCachedTransactionRequestResult`
(`TransactionManager.java:1261-1283`) hands the same result object to both the caller and
`pendingTransition`, so an inner `Arc` would be redundant — but the rules file now
contradicts the code it governs, and nothing records the deviation.

**Expected.** Correct §5 of the rules file to `notify: Notify`, and add the corollary that
Phase 3 must hold the result as `Arc<TransactionalRequestResult>` (which is what actually
makes `handle_cached_transaction_request_result`'s "return the *same* result object"
contract expressible). As written, a Phase-3 reviewer checking the code against §5 will
flag the correct implementation.

---

# Areas checked and found clean

Stated explicitly so the absence of a finding is signal rather than silence.

**`ProducerIdAndEpoch`** (`src/common/utils/producer_id_and_epoch.rs`) — complete against
`ProducerIdAndEpoch.java:21-58`. `NONE` reuses `RecordBatch::NO_PRODUCER_ID` /
`NO_PRODUCER_EPOCH` rather than redefining them (DoD §6). `is_valid` correctly translates
`RecordBatch.NO_PRODUCER_ID < producerId` — the `<`-against-sentinel form, not `!= NONE` —
and the test at `:86-93` pins that distinction. `Display` is byte-exact with Java's
`toString` (`:38`), including `(producerId=-1, epoch=-1)` for `NONE`. `derive(PartialEq, Eq, Hash)`
covers both fields, matching Java's explicit `equals`/`hashCode`. `Copy` + stack allocation
satisfies DoD §10, and `test_is_copy` guards it.

**`TransactionResult`** (`src/common/requests/transaction_result.rs`) — complete against
`TransactionResult.java:19-33`; variant order, `id()`, `for_id()` all match. Java's public
`id` field became a method, which is the right call for a `Copy` enum.

**`TransactionalRequestResult`** — I diffed it line-by-line against
`TransactionalRequestResult.java:26-86` as instructed, and every load-bearing property holds:

- `is_acked` is set **only** in `acknowledge()`, reached only from `await_result*`, never
  from `done()`/`fail()`. `test_completed_but_not_acked` pins it.
- The acked-before-error-check ordering (Java `:62-65`) is preserved: `acknowledge()` stores
  `acked = true` and only then inspects `error`, so a failed result is still acked
  (`test_failed_result_is_still_acked`).
- Timeout does **not** set `acked` or `completed` — Java throws before `:62` — so a timed-out
  operation stays retryable, which is what `handleCachedTransactionRequestResult`'s
  `nextState != pendingTransition.state` branch (`:1272`) depends on. `test_timeout_message_content`
  asserts both flags.
- Timeout message is character-identical to Java `:58-59`:
  `"Timeout expired after 10ms while awaiting commitTransaction"`.
- Re-awaitability holds (`test_result_is_re_awaitable`, `test_re_await_of_failed_result_yields_same_error`),
  so nothing about the `Notify`/`AtomicBool` composition breaks the
  `handle_cached_transaction_request_result` contract Phase 5 will build on. (The one
  prerequisite Phase 3 must honour — wrapping in `Arc` — is the subject of the last finding.)
- **The `Notify` race is genuinely closed.** I checked this against the vendored tokio
  1.52.0 source rather than assuming: `Notify::notified()` captures `notify_waiters_calls`
  at *construction* (`notify.rs:565-575`), and `poll_notified`'s `State::Init` arm compares
  against it twice (`:1124`, `:1156`), returning `Ready` if `notify_waiters()` ran in
  between. So `let notified = self.notify.notified(); if completed { return }; notified.await;`
  cannot lose a `notify_waiters()` — `enable()` / `tokio::pin!` are **not** required for
  the broadcast path. `done()` and `fail()` both store `completed` (and `error`) *before*
  calling `notify_waiters()`, so the flag is visible to any woken waiter. The surrounding
  `loop` is a second layer of protection. (I am flagging this explicitly because an earlier
  review of mine recorded the opposite rule; I have corrected that note.)
- `fail()` sets `error` before `completed`, so `is_successful()` cannot observe
  completed-without-error spuriously.

**`TxnPartitionEntry` / `TxnPartitionMap` method completeness (DoD §2)** — every method of
both Java classes is present, including the private `resetSequenceNumbers` (154) and
`decrementSequence` (163). `TxnPartitionMap::get_mut` is the declared `&mut` half of
`get` and is not an invented method in any meaningful sense. `PrimitiveRef` is correctly
*not* translated (CLAUDE.md §1.1), replaced by a `mut` local. The deliberate Java asymmetry
(`get` errors, `get_or_create` inserts, the `last_acked_*` accessors tolerate absence) is
preserved and documented, and the dead null-check at `TxnPartitionMap.java:79` is correctly
noted rather than reproduced. The `is_transactional` parameter and the Java `:90-94` comment
survive. All `IllegalStateException`s became `Result` (CLAUDE.md §10.2) with message text
asserted character-for-character in tests — I checked both messages against Java
`TxnPartitionEntry.java:147-148` / `:167-169` and `TxnPartitionMap.java:45-46`. No interior
`Mutex`, as the rules require.

**Rules file §8 (`decrement_sequence` does not wrap)** — the plan override is **correct and
the plan was wrong**. `TxnPartitionEntry.decrementSequence` (`:163-173`) does plain
`updatedSequence -= decrement` and throws; only `incrementSequence` (`:105`) delegates to
`DefaultRecordBatch.incrementSequence`. The Rust `increment_sequence` does delegate to the
shared helper (`default_record_batch.rs:887`), which I diffed against
`DefaultRecordBatch.java:557-561` — identical. `test_increment_sequence_wraps_at_i32_max`
exercises the wrap correctly, and `test_decrement_does_not_wrap` pins the non-wrapping half.

**Rules file §6 (3-key ordering)** — `InFlightBatchKey = (i64, i16, i32)`; the derived
lexicographic `Ord` is exactly Java's
`comparingLong(producerId).thenComparingInt(producerEpoch).thenComparingInt(baseSequence)`
(`TxnPartitionEntry.java:62-65`). No `Ord` impl reads mutable batch state. The Java `:58-61`
comment and PR link are carried across verbatim. Two tests pin the epoch and producer-id
tie-breaks.

**`ProducerConfig` validation** — I diffed `post_process_and_validate_idempotence_configs`
against `ProducerConfig.java:591-651` clause by clause. All four arms present with the
correct explicit-vs-implicit split; the `max.in.flight` arm is correctly the one that
**always** errors and correctly sits inside the `if idempotence_enabled` block *after* the
retries/acks arms, so `acks=0 + max.in.flight=6` errors rather than silently disabling
(matching Java, where `shouldDisableIdempotence` is applied only after the in-flight check);
`transactional.id` is validated *after* the idempotence override; the 2PC/`transaction.timeout.ms`
check is present with Java's comment. `MAX_IN_FLIGHT_REQUESTS_FOR_IDEMPOTENCE` is used for
the first time rather than re-deriving `5`. Java's `log.info` correctly became `info!` (the
plan's `warn!` was wrong). Error messages are character-identical to Java's format strings
for all four messages, and the tests assert full message equality, not `is_err()`.
`maybe_override_client_id` matches `:579-589`, and the counter is a crate-level
`static AtomicI32` starting at 1, matching Java's `static AtomicInteger` scope and start.
`explicitly_set` faithfully replaces `originals().containsKey(..)` — it captures every
supplied key, including unrecognised ones, which is what `originals()` does. `parse_acks`
already existed and matches `:653-660` (`"all"` → `-1`, trim, `i16`, error message text).
Ordering (idempotence validation, then client-id derivation) matches `postProcessParsedConfig`
`:570-577`. Java's 2PC default of `false` (`:543-546`) is matched.

**Guard blast radius** — I grepped the whole repo (excluding `kafka/` and `design/`) for
`enable.idempotence` and `transactional.id`: no production code, test, example, perf
harness, FFI, or multilanguage config sets either, so the guard cannot break anything that
exists. Every integration test sets `client.id` explicitly, so the `client.id` default
change from `""` to `"producer-N"` is inert there too; in-crate unit tests build configs via
`..Default::default()`, which does not run the derivation at all. `max.in.flight` is only
settable via the `MAX_IN_FLIGHT` env var in `tests/integration/producer_perf_test.rs:226`,
and rejecting `>5` there is Java-faithful.

**`#![allow(dead_code)]`** — the cited precedent is real (`src/network_client.rs:15`) and
the usage is appropriate: all three files are `pub(crate)` staging for Phase 3/4 callers,
and the two public types (`ProducerIdAndEpoch`, `TransactionResult`) correctly do **not**
carry the attribute. It does mean an unwired method in Phase 3 will not warn, so the
attributes should be removed as each file gains callers — only
`transactional_request_result.rs:18` currently commits to that in writing.

**Other DoD gates** — §6: no duplication; sequence arithmetic, `RecordBatch::NO_PRODUCER_ID`,
`NO_PRODUCER_EPOCH` and `INVALID_OFFSET` (`produce_response.rs:26`) are all reused, not
redefined. §7: the declared deviations (`get_mut`, `explicitly_set`, the `Notify`/`AtomicBool`
composition) are the only ones I found; no invented structs or traits. §8: zero TODO/FIXME
across all changed files, and the guard uses the mandated `MILESTONE-11 GUARD:` marker.
§10: no per-message allocation introduced — `ProducerIdAndEpoch` is `Copy`, the `BTreeSet`
key is a scalar tuple, and `TopicPartition::clone()` is an `Arc<str>` refcount bump
(`topic_partition.rs:22-25`), so the clone at `txn_partition_entry.rs:245` and the two in
`get_or_create` do not allocate. §11: not applicable. §7 licence headers: Apache 2.0 /
Confluent Inc on all five new files. `internals` types are all `pub(crate)`; the two
`common` types are `pub`, matching Java's visibility.

**Rules-file citations** — I spot-checked every Java line reference in
`.claude/rules/producer-transactions.md` that Phases 3-6 will rely on: §1
`shouldPoisonStateOnInvalidTransition` at `TransactionManager.java:287-289` ✓; §2 the four
non-volatile fields at `:136-139` ✓; §3 sequence assignment inside `synchronized (deque)`
calling into `synchronized` manager methods at `RecordAccumulator.java:877-926` ✓; §4 the
referenced `design/current/consumer-join-stall-rootcause.md` exists ✓; §5-§8 as covered
above. No incorrect citations found.

---

# Not carried as findings

Two items from the Phase-1 plan's §5 risk table, judged on the merits:

- **The five absent typed error structs are not a defect.** The reasoning was pre-approved
  in the milestone plan §1.1, and I verified the substance independently (see the
  hierarchy finding above for the one part of the reasoning that *is* incomplete — but the
  structs themselves are correctly absent).
- **The four support types shipping without Java-parity tests is not a DoD §3 violation.**
  I confirmed by listing
  `kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/` that no
  `TxnPartitionEntryTest`, `TxnPartitionMapTest`, `TransactionalRequestResultTest` exists,
  and no `ProducerIdAndEpochTest` / `TransactionResultTest` under `common/utils/` or
  `common/requests/`. There is nothing to skip. The Rust-authored tests cover the
  behaviours the plan named (sequence wrap, the 3-key rebuild, `is_acked` vs `is_completed`,
  the `done()`-before-await race, both negative-sequence error messages), which is the
  right response to the absence.

# Process note

DoD gate 9 (`make verify`) was not fully run — the C and Python suites need Docker and a
virtualenv. I re-ran the three gates that are runnable (`cargo test --lib`,
`cargo xtask format-check`, `cargo xtask lint`) and all pass. Given that the `client.id`
derivation now changes an observable value on every `from_properties` path, the FFI and
multilanguage suites are the ones most likely to surface fallout; I found no `client_id`
assertion in `src/ffi/` or `tests/` that would break, but that is grep evidence, not a
green run.
