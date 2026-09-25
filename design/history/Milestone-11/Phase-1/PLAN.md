# Milestone 11 — Phase 1: Support types, error variant, config validation + honesty guard

**Status:** APPROVED (2026-07-31) — in progress. Commits 1 and 8 unblocked by
the §7.1 / §7.3 approvals. See the Milestone PLAN header for two §6.8
corrections that change commits 5 and 6.
**Agent number `N` = 41.** Actor writes to `COMMENTS.41.md` / `COMMENTS.DONE.41.md`.
**Depends on:** nothing (first phase).
**Blocks:** Phases 2–8.

---

## 1. Goal

Land every leaf dependency of `TransactionManager` — the five support types, the
one missing error variant, the producer-config validation Java performs and Rust
currently does not — plus the design-law rules file that Phases 3–6 will be
reviewed against.

**No network code, no `TransactionManager`, no send-path changes.** Everything in
this phase is a self-contained value type or a config-layer function, so every
commit is independently testable. This is deliberate: it front-loads the pieces
whose *shape* Phases 3–6 depend on (especially §3.3
`TransactionalRequestResult`), so a mistake here is caught before it propagates.

---

## 2. Java files to read

**Primary (translate):**

| File | Lines | Note |
|---|---|---|
| `clients/src/main/java/org/apache/kafka/common/utils/ProducerIdAndEpoch.java` | 59 | whole file |
| `clients/src/main/java/org/apache/kafka/common/requests/TransactionResult.java` | 34 | whole file |
| `clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionalRequestResult.java` | 87 | whole file — read carefully, see §3.3 |
| `clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionEntry.java` | 174 | whole file |
| `clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionMap.java` | 131 | whole file |
| `clients/src/main/java/org/apache/kafka/common/errors/TransactionAbortedException.java` | 38 | whole file |
| `clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java` | **579–589** (`maybeOverrideClientId`), **591–651** (`postProcessAndValidateIdempotenceConfigs`), **653–660** (`parseAcks`), **338/350/363** (config-key consts), **527–546** (`define(...)` defaults) | do NOT translate the rest of this file |

**Context (read, do not translate in this phase):**

| File | Lines | Why |
|---|---|---|
| `.../internals/TransactionManager.java` | 95–232 (fields + ctor), 682–743 (sequence methods that call `TxnPartitionMap`), 1345–1459 (`TxnRequestHandler`, which owns a `TransactionalRequestResult`) | tells you the exact call surface Phase 3 will need from the Phase-1 types — do not guess it |
| `.../common/record/DefaultRecordBatch.java` | 557–566 | `incrementSequence` / `decrementSequence` wrapping arithmetic used by `TxnPartitionEntry` |
| `.../clients/producer/KafkaProducer.java` | 592–620 | `configureTransactionState`, for the §3.6 guard placement |

**Java tests to translate:**

| File | Tests | Note |
|---|---|---|
| `clients/src/test/java/org/apache/kafka/clients/producer/ProducerConfigTest.java` | `testUpperboundCheckOfEnableIdempotence` (145), `testTwoPhaseCommitIncompatibleWithTransactionTimeout` (161) | 2 tests |
| `clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java` | `testOverwriteAcksAndRetriesForIdempotentProducers` (222), `testAcksAndIdempotenceForIdempotentProducers` (238), `testRetriesAndIdempotenceForIdempotentProducers` (341), `testInflightRequestsAndIdempotenceForIdempotentProducers` (413) | **These 4 are pure config tests** — verified: they construct `ProducerConfig` from `Properties` and assert on values; no producer instance, no network. They belong in the Rust `producer_config.rs` test module, not in a producer test. |

**No dedicated Java test file exists for `TxnPartitionEntry`, `TxnPartitionMap`,
`TransactionalRequestResult`, or `ProducerIdAndEpoch`** — verified by listing
`clients/src/test/java/org/apache/kafka/clients/producer/internals/`. Their
coverage is indirect, via `TransactionManagerTest`. Per DoD §3 this is *not* a
skipped test; there is nothing to skip. But it does mean these four types ship in
Phase 1 with **Rust-authored unit tests only**, and their Java-parity coverage
arrives in Phases 3/5. Call this out in the commit message so a Critic does not
read it as missing translation. Write direct Rust unit tests for the non-obvious
behaviors — the sequence wrap-around, the `TreeSet` rebuild, `is_acked` vs
`is_completed` — rather than trusting Phase 3 to find the bugs.

---

## 3. Per-commit breakdown

Eight commits. Each must build, pass tests, pass format-check and lint before the
next begins (`agent-roles.md`: commit after each verified step).

### Commit 1 — `.claude/rules/producer-transactions.md`

**Blocked on user approval of Milestone PLAN §7.3.** If the user declined the
rules file, skip this commit and fold the content into this PLAN.md instead.

Create the rules file with the six rules from Milestone PLAN §5, in the same
format `consumer-threading.md` uses (numbered section, **Why**, **How to
apply**, **Anti-patterns to flag in review**):

1. `Caller { App, Sender }` replaces `Thread.currentThread() instanceof
   Sender.SenderThread` in `transition_to`.
2. Lock topology: what is shared behind `Arc<Mutex<TransactionManager>>` vs.
   owned by the Sender task (coordinator nodes, in-flight correlation id,
   `coordinator_supports_bumping_epoch`, pending-request queue).
3. Lock ordering `deque` → `TransactionManager`, never inverted.
4. Never hold the `TransactionManager` guard across `.await`; never race
   `network_client.poll(..)` in `tokio::select!` (cross-reference
   `consumer-threading.md` §10 and
   `design/current/consumer-join-stall-rootcause.md`).
5. `TransactionalRequestResult` keeps `CountDownLatch` semantics — re-awaitable,
   `is_acked` distinct from `is_completed`. A `oneshot` is wrong.
6. No `BTreeSet`/`BTreeMap` keyed on interior-mutable sort keys; key on an
   explicit snapshot tuple and rebuild on mutation.

Do **not** edit `CLAUDE.md` (it says agents must avoid changing it). Adding a new
file under `.claude/rules/` is the sanctioned mechanism.

`git commit -m "docs(rules): add producer transaction design rules for Milestone 11"`

### Commit 2 — `ProducerIdAndEpoch`

`src/common/utils/producer_id_and_epoch.rs`; declare + re-export in
`src/common/utils/mod.rs`.

- `pub struct ProducerIdAndEpoch { pub producer_id: i64, pub epoch: i16 }`.
  `derive(Debug, Clone, Copy, PartialEq, Eq, Hash)` — Java has explicit
  `equals`/`hashCode` (42–57) and the struct is two scalars, so `Copy` is correct
  and satisfies CLAUDE.md §11 (keep on the stack).
- `pub const NONE: ProducerIdAndEpoch` = `(RecordBatch::NO_PRODUCER_ID,
  RecordBatch::NO_PRODUCER_EPOCH)`. Reuse the existing sentinels in
  `src/common/record/record_batch.rs` — do not redefine them.
- `pub fn is_valid(&self) -> bool { RecordBatch::NO_PRODUCER_ID < self.producer_id }`
  — note Java compares `<` against the sentinel, not `!= NONE`.
- `impl Display` producing exactly `(producerId=%d, epoch=%d)` (Java 38) — log
  output is compared in some `TransactionManagerTest` assertions.

`producer_id` is `i64` per CLAUDE.md §2 (Java `long` used in comparison).

**Do not** change `ProducerBatch::set_producer_state` /
`reset_producer_state` to take `ProducerIdAndEpoch` in this commit. They
currently take flat `(i64, i16, i32[, bool])`
(`src/producer/internals/producer_batch.rs:646` and `658`). Destructure at the
call site in Phase 4 instead — changing the signature here would touch the
send path for no Phase-1 benefit.

### Commit 3 — `TransactionResult`

`src/common/requests/transaction_result.rs`; declare + re-export in
`src/common/requests/mod.rs` (alphabetical, per the existing convention in
lines 21–53 / 55–90).

Java is a 2-constant enum with a `boolean id` and a `forId` lookup. Translate as
`pub enum TransactionResult { Abort, Commit }` with `pub fn id(&self) -> bool`
and `pub fn for_id(id: bool) -> Self`. Keep the Java constant order.

### Commit 4 — `TransactionalRequestResult` — **the subtle one**

`src/producer/internals/transactional_request_result.rs`; declare + re-export in
`src/producer/internals/mod.rs`. `pub(crate)` only (CLAUDE.md §2: `internals`).

Java uses `CountDownLatch(1)` + `volatile RuntimeException error` +
**`volatile boolean isAcked`, which is set only inside `await()`**
(`TransactionalRequestResult.java:62`). That asymmetry is load-bearing:
`TransactionManager.handleCachedTransactionRequestResult` (Java 1261–1283) keys
off `isAcked()`, *not* `isCompleted()`, so a `commitTransaction` that completed
but was never awaited must return the **same** result object on retry.

Required shape — **do not use `tokio::sync::oneshot`** (single-consumer, not
re-awaitable, cannot express `is_acked`):

```rust
pub(crate) struct TransactionalRequestResult {
    notify: Arc<Notify>,                     // replaces CountDownLatch
    error: Mutex<Option<KafkaError>>,        // replaces volatile RuntimeException
    completed: AtomicBool,
    acked: AtomicBool,
    operation: String,
}
```

Method mapping:

| Java | Rust | Note |
|---|---|---|
| `fail(RuntimeException)` 41 | `fn fail(&self, error: KafkaError)` | sets `error`, `completed`, then `notify_waiters()` |
| `done()` 46 | `fn done(&self)` | sets `completed`, then `notify_waiters()` |
| `await()` 50 | — | Java's unbounded overload; only used by tests. Provide `async fn await_result(&self)` |
| `await(timeout, unit)` 54 | `async fn await_result_timeout(&self, timeout: Duration) -> Result<(), KafkaError>` | `tokio::time::timeout`; on expiry return `KafkaError::timeout("Timeout expired after {}ms while awaiting {operation}")` — **assert the message text in a test**, DoD §3 requires error messages be verified |
| `error()` 71 | `fn error(&self) -> Option<KafkaError>` | `KafkaError` is `Clone`, so return an owned clone |
| `isSuccessful()` 75 | `fn is_successful(&self) -> bool` | `is_completed() && error.is_none()` |
| `isCompleted()` 79 | `fn is_completed(&self) -> bool` | non-blocking poll |
| `isAcked()` 83 | `fn is_acked(&self) -> bool` | set **only** by `await_result*` |
| `InterruptException` 66 | — | no Rust analogue (no thread interruption). Note in a doc comment. |

**Notify race hazard to handle explicitly:** `Notify::notify_waiters()` does
*not* store a permit, so a waiter that calls `await_result_timeout` *after*
`done()` would block forever. `await_result_timeout` must therefore check
`completed` **before** awaiting, and re-check after
(`notified()` must be created before the first check to close the window — use
`let fut = notify.notified(); if completed { return }; fut.await;`). Add a test
that calls `done()` before any awaiter exists and asserts the subsequent await
returns immediately.

Java sets `isAcked = true` *before* checking `error` (lines 62–65), so a failed
result is still marked acked. Preserve that ordering.

### Commit 5 — `TxnPartitionEntry`

`src/producer/internals/txn_partition_entry.rs`, `pub(crate)`.

Fields mirror Java 34–56 one-to-one: `topic_partition`, `producer_id_and_epoch`,
`next_sequence: i32`, `last_acked_sequence: i32`,
`inflight_batches_by_sequence`, `last_acked_offset: i64`. Constants:
`NO_LAST_ACKED_SEQUENCE_NUMBER: i32 = -1`; initial `last_acked_offset` is
`ProduceResponse::INVALID_OFFSET` (reuse the existing const in
`src/common/requests/produce_response.rs`, do not redefine).

**Per Milestone PLAN §6.8**, translate `SortedSet<ProducerBatch>` as
`BTreeMap<(i64, i16, i32), ProducerBatch>` keyed on an explicit
`(producer_id, producer_epoch, base_sequence)` snapshot — **not**
`BTreeSet<ProducerBatch>` with an `Ord` impl reading the batch's mutable fields.
Java's comparator is exactly those three keys in that order
(`TxnPartitionEntry.java:62–65`); the 3-key form (rather than base-sequence
alone) exists to fix a real bug, documented in the comment at 58–61 — keep that
comment.

Methods (Java line → Rust):

`producer_id_and_epoch` 76, `next_sequence` 80, `last_acked_offset` 84 →
`Option<i64>`, `last_acked_sequence` 90 → `Option<i32>`, `has_inflight_batches`
96, `next_batch_by_sequence` 100 → `Option<&ProducerBatch>`,
`increment_sequence` 104, `add_inflight_batch` 108, `set_last_acked_offset` 112,
`start_sequences_at_beginning` 116, `maybe_update_last_acked_sequence` 127,
`remove_in_flight_batch` 135, `adjust_sequences_due_to_failed_batch` 139,
`reset_sequence_numbers` 154 (private), `decrement_sequence` 163 (private).

Translation notes:
- `increment_sequence` / `decrement_sequence` must call the **existing**
  `increment_sequence` (`src/common/record/default_record_batch.rs:887`) and
  `decrement_sequence` (898) wrapping helpers. Plain `+`/`-` is wrong — Java
  wraps at `Integer.MAX_VALUE` (`DefaultRecordBatch.java:557`).
- `PrimitiveRef.IntRef` (Java 117) is a Java workaround for mutable capture in a
  lambda. Per CLAUDE.md §1.1 use a plain `mut` local in a Rust loop. **Do not
  translate `PrimitiveRef`.**
- `resetSequenceNumbers` rebuilds the `TreeSet` because the sort key mutates
  (Java 154–161). The Rust `BTreeMap` must be rebuilt for the same reason —
  collect, mutate, re-insert under new keys.
- Java throws `IllegalStateException` on negative sequence in `decrementSequence`
  (167) and `adjustSequencesDueToFailedBatch` (147). Per CLAUDE.md §10.2 these
  become `Result` returns, **not** `panic!`. Preserve the message text; a test
  must assert it (DoD §3).

Rust-authored unit tests: sequence wrap-around at `i32::MAX`; ordering across an
epoch bump (two batches, same base sequence, different epoch — proves the 3-key
comparator); `start_sequences_at_beginning` rewriting three in-flight batches
from 0; `adjust_sequences_due_to_failed_batch` shifting only batches at or after
the failed base sequence; negative-sequence error paths.

### Commit 6 — `TxnPartitionMap`

`src/producer/internals/txn_partition_map.rs`, `pub(crate)`.
`HashMap<TopicPartition, TxnPartitionEntry>` + a `LogContext`.

Preserve the deliberate asymmetry (a Critic will flag it as inconsistent if the
rationale is not in a comment): `get` (Java 42) **errors** when absent,
`get_or_create` (51) inserts, and `last_acked_offset` / `last_acked_sequence` /
`maybe_update_last_acked_sequence` tolerate absence with `Option`/sentinel.
Java's `get` throwing `IllegalStateException` → `Result` per CLAUDE.md §10.2.

Methods: `get` 42, `get_or_create` 51, `contains` 55, `reset` 59,
`last_acked_offset` 63, `last_acked_sequence` 70, `start_sequences_at_beginning`
77, `remove` 83, `update_last_acked_offset` 88, `adjust_sequences_due_to_failed_batch`
106, `maybe_update_last_acked_sequence` 117, `next_batch_by_sequence` 124,
`remove_in_flight_batch` 128.

Keep the `is_transactional` parameter on `update_last_acked_offset` and the
comment at Java 90–94 explaining why a missing entry is lazily created for the
idempotent-only case. That comment is the only transaction-awareness in either
file.

Note `startSequencesAtBeginning` (77–81) calls `get()` (which throws) and *then*
null-checks the result — dead code in Java. Translate the reachable behavior and
note the redundancy in a comment rather than reproducing an impossible branch.

No `Mutex` inside either type: Java relies on the caller holding the
`TransactionManager` monitor, and Phase 3 will wrap the whole manager. Adding a
lock here would nest locks for nothing.

### Commit 7 — `KafkaError::TransactionAborted`

`src/common/kafka_error.rs`.

Java's `TransactionAbortedException` extends `ApiException` but has **no wire
error code** — verified absent from `Errors.java`. It is client-side only, thrown
at exactly one place: `Sender.java:469`,
`accumulator.abortUndrainedBatches(new TransactionAbortedException())`.

So it needs a **new enum variant**, following the `Wakeup` precedent (line 280):

```rust
/// Corresponds to Java's `TransactionAbortedException` (extends `ApiException`,
/// carries no error code). Raised when undrained batches are failed because the
/// transaction was aborted.
TransactionAborted(String),
```

Plus a `KafkaError::transaction_aborted()` convenience ctor in the 296–411 block,
defaulting to Java's no-arg message: `"Failing batch since transaction was
aborted"` (`TransactionAbortedException.java:34`).

Update the exhaustive matches: `kafka_error()` 419–433 (returns `None`, like
`Wakeup`), `message()` 454–465, `is_api_exception()` 513–522 — **note this one
returns `true`**, since Java's class extends `ApiException`, unlike `Wakeup`
which extends `KafkaException` — `is_kafka_exception()` 543–548, and `Display`
551–573. Compiler will find them all under `#![deny(warnings)]`.

**Do not** add typed structs for `TransactionAbortableError`,
`InvalidTxnStateError`, `UnknownProducerIdError`,
`TransactionalIdAuthorizationError`, or `OutOfOrderSequenceError`. Per Milestone
PLAN §1.1 those all have wire codes already present in
`src/common/protocol/errors.rs`, and this codebase only creates a typed struct
when the Java subclass carries extra payload (none of these do). They are
constructed as `KafkaError::with_message(Errors::X, ..)` /
`KafkaError::fatal(Errors::X, ..)`. Record this reasoning in the commit message
— it contradicts the milestone briefing and a Critic will otherwise report the
absent structs as missing work.

### Commit 8 — `ProducerConfig` validation + `KafkaProducer` honesty guard

**Blocked on user approval of Milestone PLAN §7.1.** The plan below assumes
option (C).

**8a. `src/producer/producer_config.rs` — add the missing config surface.**

- New key `TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG =
  "transaction.two.phase.commit.enable"` (Java 363), field
  `two_phase_commit_enable: bool` defaulting to `false` (Java 543–546), plus a
  parse arm.
- Translate `postProcessAndValidateIdempotenceConfigs` (Java 591–651)
  **faithfully**, including all four arms:
  1. `retries == 0` → `ConfigException` if the user set `enable.idempotence`
     explicitly, else **silently disable idempotence** with an `info!` log
     (Java 602–608, 627–630).
  2. `acks != -1` → same explicit/implicit split (610–618).
  3. `max.in.flight > 5` with idempotence on → **always** `ConfigException`
     (620–624), never silently disabled. This is the one asymmetric arm.
     `ProducerConfig::MAX_IN_FLIGHT_REQUESTS_FOR_IDEMPOTENCE` (line 235) exists
     and is currently never read — this is its first use.
  4. `transactional.id` set without idempotence → `ConfigException` (633–636);
     `transaction.timeout.ms` set together with 2PC → `ConfigException` (642–650).
- Translate `parseAcks` (653–660): `"all"` → `-1`, else parse as `i16`.
- Translate `maybeOverrideClientId` (579–589): when the user did not set
  `client.id`, derive `"producer-" + transactional_id` or
  `"producer-" + <monotonic sequence>`. Rust currently defaults `client_id` to
  `String::new()` (`producer_config.rs:192`) — no translation exists. Needed for
  `testOverwriteAcksAndRetriesForIdempotentProducers`, which asserts the
  `"producer-transactionalId"` form.

  ⚠ **Behavior change to flag in the commit message:** existing users' `client_id`
  goes from `""` to `"producer-N"`. This is the faithful Java behavior, but it is
  observable in logs and metrics and may break existing Rust assertions. Run the
  full suite and fix any that assert on an empty `client_id`.

  The Java sequence is a `static AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE`
  starting at 1 — use a crate-level `AtomicI32` (not per-config), matching Java's
  static scope.

**Two arms require knowing whether the user set a key explicitly**
(`this.originals().containsKey(..)`). The Rust `ProducerConfig::from_properties`
parser (line ~340) does not currently retain that. Add a private
`explicitly_set: HashSet<String>` (or a small bitflag set for the three keys
actually needed: `enable.idempotence`, `transactional.id`,
`transaction.timeout.ms`, `client.id`, `acks`, `retries`). Keep it private — it
is an implementation detail of validation, not public API.

**8b. `src/producer/kafka_producer.rs` — the temporary guard.**

In `from_config` (line 234) and `with_client` (356), after config validation:
if the effective config has `transactional_id.is_some()` **or** the user set
`enable.idempotence=true` explicitly, return
`KafkaError::with_message(Errors::UnsupportedVersion, ..)` naming the feature and
the milestone. Users who never touched the config keep today's behavior.

Requirements on this guard:
- It goes in `KafkaProducer`, **not** `ProducerConfig` — the config translation
  must mirror Java exactly and stay free of "not yet implemented".
- Mark it with a distinctive comment (e.g. `MILESTONE-11 GUARD:`) so Phases 4
  and 6 can find and delete it. **Do not write `TODO` or `FIXME`** — CLAUDE.md §5
  forbids them and the Critic will flag them.
- Removal is a tracked deliverable: the idempotence arm in Phase 4, the
  transactional arm in Phase 6. This PLAN and the Phase 4/6 plans must both say so.

**8c. Tests.**

Translate the 6 Java config tests named in §2 into the
`src/producer/producer_config.rs` test module (they are pure config tests):

| Java test | Asserts |
|---|---|
| `ProducerConfigTest.testUpperboundCheckOfEnableIdempotence` (145) | `max.in.flight > 5` + idempotence → error; and `= 5` → OK |
| `ProducerConfigTest.testTwoPhaseCommitIncompatibleWithTransactionTimeout` (161) | 2PC + explicit `transaction.timeout.ms` → error |
| `KafkaProducerTest.testOverwriteAcksAndRetriesForIdempotentProducers` (222) | `transactional.id` ⇒ idempotence on, acks `-1`/`all`, retries `i32::MAX`, `client_id == "producer-transactionalId"` |
| `KafkaProducerTest.testAcksAndIdempotenceForIdempotentProducers` (238) | the silent-disable vs. explicit-error matrix for `acks` |
| `KafkaProducerTest.testRetriesAndIdempotenceForIdempotentProducers` (341) | same matrix for `retries` |
| `KafkaProducerTest.testInflightRequestsAndIdempotenceForIdempotentProducers` (413) | the always-error `max.in.flight` arm |

Assert **error message content**, not just `is_err()` (DoD §3). Add Rust tests
for the guard itself: default config constructs successfully; explicit
`enable.idempotence=true` errors; `transactional.id` errors.

---

## 4. Definition of Done gates for Phase 1

Every gate must pass before handoff. From `definition-of-done.md`.

1. **DoD 1 — consistency with CLAUDE.md and rules.** Specifically: `i64` for
   `producer_id` (§2); `internals` types `pub(crate)` (§2); Apache 2.0 header on
   every new file (§7); `IllegalStateException` → `Result`, not `panic!` (§10.2);
   Java `Exception` → Rust `Error` naming (§2); imports via the parent-module
   re-export, not the file module path (§2).
2. **DoD 2 — all methods translated.** Every public and package-private method of
   the 5 support classes. Checklist per commit in §3.
3. **DoD 3 — tests.** The 6 Java config tests translated. The four support types
   have **no Java test file** (verified) — document that in the commit message
   and cover them with Rust-authored unit tests as specified in §3. Error
   messages asserted, not just `is_err()`.
4. **DoD 4 — blockers.** None expected: every dependency
   (`increment_sequence`/`decrement_sequence`, `RecordBatch::NO_PRODUCER_ID`,
   `ProduceResponse::INVALID_OFFSET`, `TopicPartition`, `LogContext`,
   `KafkaError`) already exists and was verified present.
5. **DoD 5 — `cargo test` green.**
6. **DoD 6 — no duplication.** In particular: do **not** redefine sequence
   wrapping arithmetic, the producer-id sentinels, or `INVALID_OFFSET`.
7. **DoD 7 — no invented types.** Two justified deviations, both to be recorded
   in `COMMENTS.DONE.41.md` or a code comment:
   (a) `TransactionalRequestResult`'s internal `Notify`/`AtomicBool` composition
   replaces `CountDownLatch` — same contract, no new public type;
   (b) `ProducerConfig`'s private `explicitly_set` set replaces Java's
   `AbstractConfig.originals()`, which this codebase has no equivalent of.
   `PrimitiveRef` is deliberately **not** translated (CLAUDE.md §1.1).
8. **DoD 8 — no TODO/FIXME.** The §3 commit-8b guard must use a
   `MILESTONE-11 GUARD:` comment, never `TODO`/`FIXME`.
9. **DoD 9 — `make verify`** (unit, integration, Python, C tests, format, lint).
   Watch for fallout from the `client_id` change in commit 8a.
10. **DoD 10 — hot-path allocation audit.** `ProducerIdAndEpoch` must be `Copy`
    and stack-allocated (CLAUDE.md §11). `TxnPartitionEntry`/`Map` are on the
    drain path (per-batch, not per-record) — no `String` clones, no `Box<dyn ..>`.
    The `BTreeMap` key is a `(i64, i16, i32)` tuple, not a heap key.
11. **DoD 11 — consumer trait surface check.** Not applicable (no consumer files).

Plus the explicit commands: `cargo build`, `cargo test`,
`cargo xtask format-check`, `cargo xtask lint`.

---

## 5. Phase 1 risks

| Risk | Severity | Mitigation |
|---|---|---|
| `TransactionalRequestResult` gets `oneshot`-shaped or drops `is_acked` | **High** — the covering tests are in Phase 5, so a wrong shape survives 3 phases and then forces rework of `handle_cached_transaction_request_result` | Rules file item 5 (commit 1); Rust unit tests for `is_acked` vs `is_completed` and for the notify-before-await race; a Critic instruction to diff against Java 41–86 line by line |
| `client_id` default change breaks existing tests/asserts | Medium | Run `make verify` before committing 8a; fix fallout in the same commit |
| The honesty guard is too aggressive and breaks existing producer tests | Medium | Guard only on *explicit* `enable.idempotence=true` or `transactional.id`; the default path is untouched. Verify by running the existing producer suite |
| `BTreeMap` rebuild logic diverges from Java's `TreeSet` rebuild | Medium | Rust unit test with two batches sharing a base sequence across an epoch bump |
| Critic reports the absent typed error structs as missing work | Low but likely | Record the reasoning in commit 7's message and in `COMMENTS.DONE.41.md` |
| Critic reports the four untested support types as violating DoD §3 | Low but likely | State in the commit message that no Java test file exists (verified by directory listing) and that parity coverage lands in Phases 3/5 |

---

## 6. Handoff artifacts

On completion (Manager step 7):

- `design/current/status.md`, `structure.md`, `design.md` refreshed. ⚠ These are
  **stale** — they claim Milestone 3 and ~15k lines; reality is Milestone 10 and
  ~143k. Phase 1's handoff should correct the Milestone/LoC headline rather than
  appending Milestone-11 detail to a wrong baseline.
- `marked_classes.txt`: add `org.apache.kafka.common.utils.ProducerIdAndEpoch`,
  `org.apache.kafka.common.requests.TransactionResult`,
  `org.apache.kafka.clients.producer.internals.TransactionalRequestResult`,
  `org.apache.kafka.clients.producer.internals.TxnPartitionEntry`,
  `org.apache.kafka.clients.producer.internals.TxnPartitionMap`. Remove the same
  five from `remaining_classes.txt` (lines 450, 151, 457, 459, 460).
  **Leave `org.apache.kafka.common.record.EndTransactionMarker` (line 70) in
  `remaining_classes.txt`** — out of scope per Milestone PLAN §1.1.
- `COMMENTS.DONE.41.md` copied to `design/history/Milestone-11/Phase-1/`.
- Root `COMMENTS.41.md` reset for Phase 2.
