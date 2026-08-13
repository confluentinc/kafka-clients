# Milestone 11 — Producer Idempotence and Transactions

**Status:** APPROVED (2026-07-31). Phases 1-3 landed; Phases 4-8 not started.

Decisions §7.1-§7.4 all approved as recommended: faithful idempotence-config
translation plus a temporary `KafkaProducer` guard (§7.1), bindings deferred to a
follow-up milestone (§7.2), `.claude/rules/producer-transactions.md` created
(§7.3), KIP-939 2PC kept in Phase 5 (§7.4).

**Corrections applied during Phase 1** — two items in §6.8 did not survive
contact with the Rust source and are superseded by
`.claude/rules/producer-transactions.md` §7 and §8:

  - `BTreeMap<(i64, i16, i32), ProducerBatch>` is **not implementable**.
    `ProducerBatch` is not `Clone`, and `Sender::in_flight_batches`
    (`sender.rs:122`) takes ownership after `drain()` moves it out of
    `RecordAccumulator` (`record_accumulator.rs:128`). Java relies on GC-shared
    references. `TxnPartitionEntry` therefore stores ordering *keys* only — see
    rules file §7. (Ownership **alternates**: on the retry path it returns to the
    accumulator's deque while the batch stays tracked. Corrected in rules §7 after
    Critic 41's second pass; the conclusion above is unaffected.)
  - "Phase 1 reuses `decrement_sequence`" is **wrong**. Java's
    `TxnPartitionEntry.decrementSequence` (163-173) does plain subtraction and
    throws on negative; it does not call `DefaultRecordBatch.decrementSequence`.
    Only `incrementSequence` delegates to the wrapping helper — see rules
    file §8.
**Scope:** Rust only. C FFI / Python / gRPC multilanguage harness deferred (see §7.2).
**Java source:** Apache Kafka 4.2.0 (`kafka/` submodule at `a18251b`).
**Agent numbers:** 41–48 (highest previously used is 40).

---

## 1. What this milestone delivers

The Java `TransactionManager` and its integration into the producer send path,
giving the Rust client:

- **Idempotent produce** — `enable.idempotence=true` actually acquires a producer
  ID via `InitProducerId`, assigns per-partition sequence numbers, tracks
  in-flight batches by sequence, and recovers from
  `OUT_OF_ORDER_SEQUENCE_NUMBER` / `UNKNOWN_PRODUCER_ID` via epoch bump.
- **Transactions** — the 9-state transaction state machine, the five coordinator
  RPCs, transaction-coordinator discovery, and the public
  `init_transactions` / `begin_transaction` / `commit_transaction` /
  `abort_transaction` / `send_offsets_to_transaction` API on both
  `KafkaProducer` and `MockProducer`.
- **KIP-890 Transaction V2** and **KIP-939 two-phase commit**, both of which are
  present in Kafka 4.2's `TransactionManager` and are not optional if we want
  behavioral parity (see §6.1).

### 1.1 Corrections to the pre-milestone gap analysis

Verified by reading the source. Three items in the briefing were wrong or
overstated, and they change the plan:

| Briefing claim | Reality |
|---|---|
| "Typed errors missing: `TransactionAbortedError`, `TransactionAbortableError`, `TransactionalIdAuthorizationError`, `InvalidTxnStateError`, `UnknownProducerIdError`" | **All the wire error codes already exist** in `src/common/protocol/errors.rs`: `OutOfOrderSequenceNumber=45`, `DuplicateSequenceNumber=46`, `InvalidProducerEpoch=47`, `InvalidTxnState=48`, `InvalidProducerIdMapping=49`, `InvalidTransactionTimeout=50`, `ConcurrentTransactions=51`, `TransactionCoordinatorFenced=52`, `TransactionalIdAuthorizationFailed=53`, `UnknownProducerId=59`, `ProducerFenced=90`, `TransactionalIdNotFound=105`, `TransactionAbortable=120`. This codebase only creates a *typed error struct* when the Java subclass carries extra payload (cf. `TopicAuthorizationError`'s `unauthorized_topics`). None of these do. They become `KafkaError::with_message(Errors::X, ..)` / `KafkaError::fatal(..)`. **Only one new enum variant is needed:** `TransactionAborted`, because Java's `TransactionAbortedException` has *no wire code* (verified absent from `Errors.java`) — it is client-side only, like the existing `Wakeup` variant. |
| "Request/response wrappers for all 5 txn RPC pairs ... `WriteTxnMarkers`" | `WriteTxnMarkers` is **not producer-side**. Its only `clients/src/main` users are `Admin.java` and `admin/internals/AbortTransactionHandler.java`; `grep` over `producer/**` returns nothing. It is a coordinator→broker RPC. **Out of scope** — it belongs to a future Admin milestone. Producer needs exactly 5 pairs. |
| "`EndTransactionMarker` missing" | Also **not producer-side**. Its users are `MemoryRecordsBuilder.writeEndTransactionalMarker` and `MemoryRecords.withEndTransactionMarker` — the *broker* write path for control records. The producer never constructs one. **Out of scope**; leave in `remaining_classes.txt`. |

Two further findings that *reduce* risk:

- **`ApiVersions` finalized-features support already exists** —
  `src/api_versions.rs` has `FinalizedFeaturesInfo { finalized_features_epoch,
  finalized_features }`, `max_finalized_features_epoch()`, and
  `finalized_features_info()`. There is even a test at `api_versions.rs:186`
  asserting on the `"transaction.version"` key. This is the exact prerequisite
  for KIP-890 TV2 detection (`TransactionManager.maybeUpdateTransactionV2Enabled`,
  Java lines 492–504), and it is already done.
- **`ProduceRequest` TV1/TV2 plumbing already exists but is unused** —
  `src/common/requests/produce_request.rs` has
  `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 = 11` (line 37), a
  `transactional_id` cache field (54), `transactional_id()` (97),
  `is_transaction_v2_requested(version)` (102), and
  `ProduceRequestBuilder::builder(data, use_transaction_v1_version)` (260).
  `Sender` never calls the two-arg builder and never sets `transactional_id`.
  Phase 4/6 wires up what is already there.
- **`CoordinatorType::Transaction`** already exists at
  `src/common/requests/find_coordinator_request.rs:50`, and
  `FindCoordinatorRequestBuilder::build_version` (line 215) already rejects
  `version < 1` for the transaction key type. Coordinator discovery reuses the
  consumer's wrapper unchanged.

One finding that *adds* scope: **`MockProducer`'s transactional surface is an
explicitly-deferred debt with 44 named skipped tests**, listed in a comment
block at `src/producer/mock_producer.rs:525–577`. This is not in the briefing's
gap analysis but is squarely in scope for this milestone (DoD §3).

---

## 2. Sequencing decision: idempotence first — validated against the source

The briefing asked us to validate the "idempotence first" recommendation against
the actual Java class decomposition rather than assume it. **It holds, and the
Java code is unusually well-fenced for it.**

Every transactional code path in `TransactionManager.java` is behind either
`ensureTransactional()` (line 1147, throws when `transactionalId == null`) or an
`if (isTransactional())` guard. Nothing on the idempotent path calls a
transaction-only method. Concretely, an idempotence-only slice needs:

- ~~**4 of 9 states**: `UNINITIALIZED`, `INITIALIZING`, `READY`, `FATAL_ERROR`.
  `ABORTABLE_ERROR` is unreachable without a `transactionalId`.
  (`PREPARED_TRANSACTION`, `COMMITTING_*`, `ABORTING_*` likewise.)~~
  **Corrected in Phase 3 — it is 5 of 9.** `ABORTABLE_ERROR` *is* reachable
  without a `transactionalId`: `InitProducerIdHandler.handleResponse` calls
  `abortableError(..)` for `CLUSTER_AUTHORIZATION_FAILED` (Java 1524–1528)
  **without** testing `isTransactional()`, a non-transactional
  `InitProducerId` receives that code when the principal lacks
  `IdempotentWrite` on the cluster, and the manager is in `INITIALIZING` when
  the response arrives — which the table permits as a source for
  `ABORTABLE_ERROR` (Java 180). So Phase 3 also translates
  `transitionToAbortableError` (530), `hasError` (522) and `hasAbortableError`
  (991). See §9.15. (`PREPARED_TRANSACTION`, `COMMITTING_*`, `ABORTING_*` are
  genuinely unreachable.)
- **1 of 6 request handlers**: `InitProducerIdHandler` (Java 1461–1539) plus the
  `TxnRequestHandler` base (1345–1459). `InitProducerIdHandler.coordinatorType()`
  (1482–1488) returns `null` when non-transactional, so **the whole
  FindCoordinator subsystem and the `Priority` priority-queue are unnecessary**
  for idempotence — a plain FIFO holding a single `InitProducerId` suffices.
- **`TxnPartitionMap` + `TxnPartitionEntry` in full.** Despite the `Txn` names
  these two files are *pure idempotence machinery*: sequence numbers, in-flight
  batches by sequence, last-acked sequence/offset. Their only transaction
  awareness is a `boolean isTransactional` parameter threaded into
  `TxnPartitionMap.updateLastAckedOffset` (Java 88–99) that decides whether to
  lazily create a missing entry.

Five methods are *shared but internally forked* on `isTransactional()` and so get
their transactional arms added in Phase 5 rather than being separable functions:
`maybeTransitionToErrorState` (764–786), `handleFailedBatch` (788–822),
`maybeResolveSequences` (850–883), `nextRequest` (894–932), `canRetry`
(1015–1082). All five are strictly shorter in the idempotent-only form, so this
is additive growth, not rework.

**Caveat that the plan must absorb (this is the one place the clean carve
breaks):** the *implementation* carves cleanly but the *test suite does not*.
`TransactionManagerTest.setup()` (line 163) constructs a **transactional**
manager by default; only ~15 call sites re-initialize with `Optional.empty()`
for the idempotent-only case. So most sequence-number tests run through a
transactional manager. Consequence: Phase 3 can only land ~15–20 of the 140
`TransactionManagerTest` methods; the rest are **deferred within this
milestone** to Phases 5 and 8 — not skipped. Phase 3's DoD explicitly does *not*
claim `TransactionManagerTest` parity.

---

## 3. Phase breakdown

| Phase | N | Title | Java main | Java tests |
|---|---|---|---|---|
| 1 | 41 | Support types, error variant, config validation + honesty guard | 4 files (~350 LoC) | `ProducerConfigTest` (idempotence subset) |
| 2 | 42 | Five txn request/response wrapper pairs | 10 files (~1 030 LoC) | 6 dedicated `*Test.java` (695 L) + `RequestResponseTest` txn cases |
| 3 | 43 | `TransactionManager` — idempotence core | `TransactionManager.java` (idempotent slice) | ~15–20 `TransactionManagerTest` methods |
| 4 | 44 | Idempotent send-path integration | `Sender.java`, `RecordAccumulator.java` (deltas) | `SenderTest` + `RecordAccumulatorTest` idempotence subsets |
| 5 | 45 | `TransactionManager` — transactional state machine, 5 handlers, TV2, 2PC | `TransactionManager.java` (remainder) | bulk of `TransactionManagerTest` |
| 6 | 46 | `Producer` trait + `KafkaProducer` txn API + Sender txn request loop | `KafkaProducer.java`, `Producer.java`, `Sender.java` (txn loop) | `KafkaProducerTest` (27 txn tests) |
| 7 | 47 | `MockProducer` transactional surface | `MockProducer.java` (txn methods) | 44 currently-skipped `MockProducerTest` methods |
| 8 | 48 | `TransactionManagerTest` parity sweep + broker integration | — | remainder of `TransactionManagerTest`; new integration tests |

Dependency order is strict: 1 → 2 → 3 → 4 → 5 → 6 → {7, 8}. Phases 7 and 8 are
independent of each other and may run in parallel if desired (different agent
numbers, disjoint files).

---

### Phase 1 (N=41) — Support types, error variant, config validation + honesty guard

| Java source | Lines | Target Rust path |
|---|---|---|
| `common/utils/ProducerIdAndEpoch.java` | 59 | `src/common/utils/producer_id_and_epoch.rs` |
| `common/requests/TransactionResult.java` | 34 | `src/common/requests/transaction_result.rs` |
| `producer/internals/TransactionalRequestResult.java` | 87 | `src/producer/internals/transactional_request_result.rs` |
| `producer/internals/TxnPartitionEntry.java` | 174 | `src/producer/internals/txn_partition_entry.rs` |
| `producer/internals/TxnPartitionMap.java` | 131 | `src/producer/internals/txn_partition_map.rs` |
| `producer/ProducerConfig.java` 591–651 | 60 | `src/producer/producer_config.rs` (extend) |
| `common/errors/TransactionAbortedException.java` | 38 | `src/common/kafka_error.rs` (new enum variant) |

Full per-commit detail in `Phase-1/PLAN.md`.

---

### Phase 2 (N=42) — Five txn request/response wrapper pairs

| Java source | Lines | Target Rust path |
|---|---|---|
| `requests/InitProducerIdRequest.java` | 85 | `src/common/requests/init_producer_id_request.rs` |
| `requests/InitProducerIdResponse.java` | 82 | `src/common/requests/init_producer_id_response.rs` |
| `requests/AddPartitionsToTxnRequest.java` | 203 | `src/common/requests/add_partitions_to_txn_request.rs` |
| `requests/AddPartitionsToTxnResponse.java` | 165 | `src/common/requests/add_partitions_to_txn_response.rs` |
| `requests/AddOffsetsToTxnRequest.java` | 68 | `src/common/requests/add_offsets_to_txn_request.rs` |
| `requests/AddOffsetsToTxnResponse.java` | 81 | `src/common/requests/add_offsets_to_txn_response.rs` |
| `requests/EndTxnRequest.java` | 85 | `src/common/requests/end_txn_request.rs` |
| `requests/EndTxnResponse.java` | 84 | `src/common/requests/end_txn_response.rs` |
| `requests/TxnOffsetCommitRequest.java` | 260 | `src/common/requests/txn_offset_commit_request.rs` |
| `requests/TxnOffsetCommitResponse.java` | 206 | `src/common/requests/txn_offset_commit_response.rs` |

**Tests** (all translatable, all self-contained — this is why Phase 2 carries its
own DoD value independent of `TransactionManager`):
`AddPartitionsToTxnRequestTest` (161), `AddPartitionsToTxnResponseTest` (146),
`EndTxnRequestTest` (95), `EndTxnResponseTest` (54),
`TxnOffsetCommitRequestTest` (172), `TxnOffsetCommitResponseTest` (67).
`InitProducerId` and `AddOffsetsToTxn` have no dedicated test file; their
coverage lives in `RequestResponseTest.java` (21 matching sites) — translate
those cases into the per-wrapper Rust test modules, matching the convention the
existing wrappers use.

**Wiring cost, measured:** adding one variant to `ConcreteRequest`
(`abstract_request.rs:97–123`) requires editing **8 exhaustive `match` sites**
(`version` 127, `api_key` 145, `to_send` 169, `serialize_with_header` 194,
`serialize` 263, `get_error_response` 296, `do_parse_request` 332, `Display`
396). `ConcreteResponse` (`abstract_response.rs:56–82`) requires **10**
(`api_key` 86, `to_send` 110, `serialize_with_header` 134, `serialize` 182,
`error_counts` 210, `throttle_time_ms` 230, `maybe_set_throttle_time_ms` 249,
`should_client_throttle` 267, `parse` 322, `Display` 385). Plus `mod.rs`
declarations (21–53) and re-exports (55–90), alphabetical.

**Design note — no `AbstractRequest` trait exists.** The pattern is: a
`{Name}Request` struct holding `data` + `version` with a fixed inherent surface
(`new`, `data`, `pub(crate) data_mut`, `version`, `api_key`,
`get_error_response`, `parse`), plus a `{Name}RequestBuilder` implementing
`pub trait RequestBuilder` (`abstract_request.rs:64–89`). Responses are plain
structs, duck-typed by the enum arms; the required surface is `new`, `api_key`,
`data`, `pub(crate) data_mut`, `throttle_time_ms`,
`maybe_set_throttle_time_ms`, `should_client_throttle`, `error_counts`, `parse`.
Copy `find_coordinator_{request,response}.rs` as the reference — it is the
closest analogue (version-gated builder, coordinator-typed, `Node` extraction).

**Scope note.** All 5 pairs land in one phase rather than splitting
`InitProducerId` (the only one idempotence needs) into Phase 2 and the other 4
into Phase 5. Rationale: one pass through the 18 exhaustive match sites instead
of two, and the 695 lines of dedicated Java wrapper tests give the phase
independent verification value. Cost: 4 wrapper pairs sit unused until Phase 5
(`internals` modules are already `#![allow(dead_code)]`-tolerant, so this does
not fight `#![deny(warnings)]`).

---

### Phase 3 (N=43) — `TransactionManager`, idempotence core

`src/producer/internals/transaction_manager.rs` — created here, grown in Phase 5.
Per CLAUDE.md §2 (`internal` packages), everything is `pub(crate)`.

Translate from `TransactionManager.java`:

- `State` enum, ~~but only the 4 reachable states,~~ **with the full 9-variant
  `is_transition_valid` table from Java 162–188 written correctly from the
  start** so Phase 5 adds no transition logic. (Target-first table; note the
  `ABORTABLE_ERROR` self-loop and that `READY → READY` is illegal.)
  **Resolved in Phase 3:** these two clauses contradict each other — a
  four-variant enum cannot carry a nine-variant table — so all nine variants
  are declared. That is also the faithful translation (they are all in Java's
  `State`), and it makes Phase 5 purely additive, which is what the second
  clause asks for. Five of the nine are reachable here, not four; see §2 and
  §9.15.
- `transition_to(target, error, caller)` (Java 1114–1145) — see §6.2 for the
  `Caller` parameter that replaces Java's `Thread.currentThread() instanceof
  Sender.SenderThread`.
- Producer-ID lifecycle: `producer_id_and_epoch` (581), `maybe_update_producer_id_and_epoch`
  (585), `set_producer_id_and_epoch` (603), `reset_idempotent_producer_id` (618),
  `request_idempotent_epoch_bump_for_partition` (640), `bump_idempotent_producer_epoch`
  (645), `bump_idempotent_epoch_and_reset_id_if_needed` (663).
- Sequence machinery: `sequence_number` (682), `increment_sequence_number` (693),
  `first_in_flight_sequence` (710), `next_batch_by_sequence` (717),
  `last_acked_sequence` (730), `last_acked_offset` (734),
  `reset_sequence_for_partition` (627), `reset_sequence_numbers` (632),
  `is_next_sequence` (885), `is_next_sequence_for_unresolved_partition` (889),
  `has_stale_producer_id_and_epoch` (828), `has_unresolved_sequences` (832),
  `has_unresolved_sequence` (836), `mark_sequence_unresolved` (840),
  `maybe_resolve_sequences` (850, idempotent arm only).
- Batch bookkeeping: `add_in_flight_batch` (697), `remove_in_flight_batch` (721),
  `has_inflight_batches` (824), `update_last_acked_offset` (738),
  `handle_completed_batch` (745), `handle_failed_batch` (788, idempotent arm),
  `can_retry` (1015, idempotent arms).
- `InitProducerIdHandler` (1461–1539) + the `TxnRequestHandler` base (1345–1459),
  non-transactional path only (`coordinator_type()` → `None`).
- `maybe_fail_with_error` (1152), `transition_to_fatal_error` (541),
  `has_fatal_error` (986), `last_error` (462), `hasProducerId` (476),
  `is_transactional` (480).

**Added during Phase 3, beyond the list above** — each because a listed item or
a named test needs it, not as scope creep:

- `transition_to_abortable_error` (530), `has_error` (522),
  `has_abortable_error` (991) — required by
  `InitProducerIdHandler.handleResponse`'s authorization arms (§9.15).
- `transition_to_uninitialized` (756) and `fail_pending_requests` (944) — the
  **exit** from `ABORTABLE_ERROR`, reached from
  `Sender.shouldHandleAuthorizationError` (`Sender.java:351-360`). Without them
  the translated state machine has no exit from `ABORTABLE_ERROR` at all, so from
  Phase 4 — when the `Sender` wires the manager into `runOnce` — an idempotent
  producer that hit an authorization failure would reject every subsequent send
  forever. Both were listed under Phase 5; that listing is removed. Added after
  Critic 43 issue 1 — see §9.15.
- `authentication_failed` (939) and `close` (949) — the other two methods that
  fail the pending-request queue. Both were **unscheduled in every phase**, both
  have a call site reachable for a purely idempotent producer, and neither needs
  Phase-5 state beyond the `pendingTransition` branch. Neither has any
  behavioural payoff *in Phase 3* — like the two above, their Java call sites are
  in `Sender.runOnce` / `Sender.run`, which Phases 4 and 6 translate. `close`'s
  payoff is specifically Phase 6 and transactional (waking the threads blocked in
  `result.await`, per `Sender.java:288-289`); on the idempotent path it has none.
  Reasoning in §9.15.
- `maybe_add_partition` (437, idempotent arm) and `maybe_transition_to_error_state`
  (764, idempotent arm) — `testFailIfNotReadyForSendIdempotentProducer` and
  `testFailIfNotReadyForSendIdempotentProducerFatalError` (both named under
  **Tests** below) call the first, and `handleFailedBatch` calls the second on
  its first line.
- `next_request` (894, idempotent arm), `enqueue_request` (1186), `retry` (934),
  `maybe_terminate_request_with_error` (1174), `has_pending_requests` (1005),
  `set_in_flight_correlation_id` (973), `clear_in_flight_correlation_id` (977),
  `has_in_flight_request` (981), `needs_coordinator` (1430) — the
  `TxnRequestHandler` base's own surface, without which the handler cannot be
  handed to a Sender or completed. `hasInFlightRequest` is also asserted by
  `testDuplicateSequenceAfterProducerReset` (Phase 4).
- `ensure_transactional` (1147), `transactional_id` (472), `is_2pc_enabled`
  (510), `producer_id_and_epoch_for_partition` (689),
  `maybe_update_last_acked_sequence` (726), `Priority` (195) — trivial
  accessors and the handler's `priority()` return type.

**Tests:** the 15 `TransactionManagerTest` methods that call
`initializeTransactionManager(Optional.empty(), ..)` — Java lines 270, 277, 626,
635, 673, 713, 750, 852, 865, 3041, 3085, 3126, 3246, 3603, 3728.
**12 landed**; the 3 that need the Phase-4 send path
(`testDuplicateSequenceAfterProducerReset` 750,
`testHealthyPartitionRetriesDuringEpochBump` 3603,
`testFailedInflightBatchAfterEpochBump` 3728) are named with their missing
surface in a comment block at the end of the Rust test module, per DoD §3.
Phase 3's DoD does **not** claim `TransactionManagerTest` parity — see §2.

All 12 are `@ParameterizedTest @ValueSource(booleans = {true, false})` on
`transactionV2Enabled`, translated as loops. The flag is **not** observable in
this phase: Java threads it only into `ApiVersions`, and the manager reads
`apiVersions` from just `handleCoordinatorReady` (1104) and
`maybeUpdateTransactionV2Enabled` (493), both Phase 5, so
`isTransactionV2Enabled` stays `false` in both iterations. The loops are kept
because they cost nothing and start discriminating in Phase 5.

---

### Phase 4 (N=44) — Idempotent send-path integration

No new Java classes; deltas to two existing Rust files. Insertion points are
already scaffolded with deferral comments.

| Concern | Java reference | Rust insertion point |
|---|---|---|
| `TransactionManager` field + ctor arg | `Sender.java:123,140,154` | `src/producer/internals/sender.rs:97–131` (struct), `136–168` (`new`) |
| `maybe_resolve_sequences` / fatal check / **abortable-error recovery** / `bump_idempotent_epoch_and_reset_id_if_needed` / `maybeSendAndPollTransactionalRequest` / `authentication_failed` | `Sender.java:310–345` plus the private `shouldHandleAuthorizationError` (`:351–360`) | `sender.rs:213–216` — **replaces the existing `// No transaction manager in this phase` comment at line 214**, must run before `send_producer_data`. The block has **four** early exits and the order is load-bearing: `:322` (fatal), `:326` (abortable + authorization error), `:334` (`maybeSendAndPollTransactionalRequest` returned true) all **return** before `sendProducerData` at `:344`. `:334` is the one easiest to miss: `maybeSendAndPollTransactionalRequest` (`:459–518`) has exactly one `return false` (`:474`, empty queue) and six `return true`, so enqueueing an `InitProducerId` at `:331` *guarantees* `runOnce` returns without producing in that iteration. `transaction_manager.rs`'s test helper `run_sender_transaction_phase` models all four exits and is the reference — but it models `:333–335` as a *predicate* on the pending/in-flight state, not as an actual send, and says so at the site; Phase 4 replaces the predicate with the real call. §9.15 records what goes wrong if `:325` is skipped; Critic 43 issue 6 records what went wrong when `:334` was. The `TransactionManager` side (`fail_pending_requests`, `transition_to_uninitialized`, `authentication_failed`) landed in Phase 3; `maybeAbortBatches` and `client.poll` are this phase's. |
| **Split the Sender-owned request-queue surface off `TransactionManager`** (rules §2) | `TransactionManager.java:136` (`inFlightRequestCorrelationId`), `:224` (`pendingRequests`), `:1406–1428` (`onComplete`, whose `synchronized` block starts only at `:1421` and covers `handleResponse` alone) | `transaction_manager.rs` — see §10.5 deviation 7 for the **thirteen** method signatures this reshapes. Budget it as its own commit: it is a refactor of the same constructor §6.3 already flags, and getting it wrong puts two deliberately-unsynchronized Java fields behind the shared lock. |
| **Per-batch sequence assignment** + `add_in_flight_batch` | `RecordAccumulator.java:900–925` | `record_accumulator.rs` between `deque.pop_front()` (919) and `batch.close()` (923). Must be here, not in `Sender`: `sender.rs:396` (`batch.records()` → `take_built_records()`) serializes the v2 batch header, so producer id / epoch / base sequence must already be set. |
| `should_stop_drain_batches_for_partition` | `RecordAccumulator.java:815–850` | `record_accumulator.rs:858`, alongside the existing `is_muted` check |
| `handle_completed_batch` | `Sender.java:758` | `sender.rs:766` — replaces `// No transaction manager in this phase` |
| `handle_failed_batch(.., adjust_sequence_numbers)` | `Sender.java:848` | `sender.rs:878` — replaces `// No transaction manager handling in this phase`; **wire up the already-present-but-underscore-ignored `_adjust_sequence_numbers` parameter** at `sender.rs:867` |
| `transaction_manager.can_retry` | `Sender.java:881` | `sender.rs:892–897` |
| `remove_in_flight_batch` on `MESSAGE_TOO_LARGE` split | `Sender.java:686` | `sender.rs:658` |
| `mark_sequence_unresolved` on expiry | `Sender.java:374` | `sender.rs:435` (`fail_expired_batches`) |
| `RecordAccumulator` `transaction_manager` field | `RecordAccumulator.java` ctor | `record_accumulator.rs:142–166` (struct), `184`/`223` (ctors) |
| Remove the Phase-1 idempotence guard | — | `kafka_producer.rs` (see §7.1) |

Also in this phase: `KafkaProducer` must gain `configure_transaction_state`
(Java `KafkaProducer.java:592–620`) and share the `TransactionManager` with the
Sender task. **This is a real structural obstacle** — see §6.3.

**Tests:** the idempotence subset of `SenderTest.java` (43 of its 76 tests are
idempotence/txn-related; the idempotence-only ones land here, the transactional
ones in Phase 6) and the idempotence subset of `RecordAccumulatorTest.java`
(which references `TransactionManager` directly).

---

### Phase 5 (N=45) — `TransactionManager`, transactional state machine

Grows `transaction_manager.rs` to full parity. From `TransactionManager.java`:

- All 9 states reachable; `Priority` enum (195–207) and the priority-ordered
  pending-request queue (224). Note `InitProducerIdHandler.priority()` (1477) is
  *dynamic*: `EPOCH_BUMP` when bumping, else `INIT_PRODUCER_ID`. Java's
  `PriorityQueue` is unstable; a Rust `BinaryHeap` matches, but add an
  insertion-sequence tiebreaker if determinism is wanted for tests.
- Entry points: `initialize_transactions` (291/295/299), `begin_transaction`
  (330), `prepare_transaction` (342, 2PC), `begin_commit` (353), `begin_abort`
  (361), `begin_completing_transaction` (373), `send_offsets_to_transaction` (404),
  `maybe_add_partition` (437, transactional arm — the idempotent arm landed in
  Phase 3), `is_send_to_partition_allowed` (466, transactional arm — the
  fatal-error and non-transactional arms landed in Phase 4),
  `reset_transaction_state`
  (1330). ~~`transition_to_uninitialized` (756)~~ **landed in Phase 3** (§9.15);
  Phase 5 adds only its `pendingTransition` branch and the `error` parameter that
  branch consumes.
- `PendingStateTransition` (1953–1967) + `handle_cached_transaction_request_result`
  (1261–1283) + `throw_if_pending_state` (1249). See §6.4 — this is why
  `TransactionalRequestResult` cannot be a `oneshot`.
- Error machine: `transition_to_abortable_error_or_fatal_error` (557),
  `need_to_trigger_epoch_bump_from_client` (1309), `can_handle_abortable_error`
  (1326), `maybe_transition_to_error_state` (764, txn arm).
  ~~`transition_to_abortable_error` (530), `has_abortable_error` (991),
  `fail_pending_requests` (944)~~ **all landed in Phase 3** (§9.15).
- The 5 remaining handlers: `FindCoordinatorHandler` (1651–1721),
  `AddPartitionsToTxnHandler` (1541–1649, incl. the
  `ADD_PARTITIONS_RETRY_BACKOFF_MS = 20` override on first
  `CONCURRENT_TRANSACTIONS`), `EndTxnHandler` (1723–1794),
  `AddOffsetsToTxnHandler` (1796–1854), `TxnOffsetCommitHandler` (1856–1951).
- Coordinator state: `coordinator` (958), `lookup_coordinator` (969/1191),
  `handle_coordinator_ready` (1103), `coordinator_supports_bumping_epoch` (1110).
  See §6.5 for where this state must live in Rust.
- **KIP-890 TV2**: `maybe_update_transaction_v2_enabled` (492–504),
  `is_transaction_v2_enabled` (506). Reads
  `ApiVersions::finalized_features_info()` for `"transaction.version" >= 2`.
  TV2 changes behavior materially: `maybe_add_partition` (448) skips
  `AddPartitionsToTxn` entirely, `send_offsets_to_transaction` (417) skips
  `AddOffsetsToTxn`, and `EndTxnHandler` absorbs a server-returned pid/epoch.
- **KIP-939 2PC**: `enable2PC` (148), `PREPARED_TRANSACTION` state,
  `prepare_transaction` (342), `is_prepared` (1099), `prepared_transaction_state`
  (1976), and the `keep_prepared_txn` branch of `InitProducerIdHandler`
  (1501–1511).

**Tests:** the bulk of `TransactionManagerTest.java` (140 methods total: 122
`@Test` + 18 `@ParameterizedTest`; the `@ParameterizedTest` ones are mostly
`transactionV2Enabled` true/false matrices). Expect this phase's test volume to
exceed its implementation volume. If the phase proves too large in practice,
split at plan time into 5a (state machine + `FindCoordinator` +
`InitProducerId` epoch bump) and 5b (AddPartitions / AddOffsets /
TxnOffsetCommit / EndTxn + TV2 + 2PC).

> **Split invoked, 2026-08-05 (Manager).** Evidence: Phase 4 closed at *smaller*
> scope only after four Critic passes, ~26 commits and one Actor watchdog stall,
> and this phase's own text expects test volume to exceed implementation volume.
> Execution order:
>
> - **5a** — all nine states reachable with the full transition surface; `Priority`
>   + the priority-ordered pending-request queue (`BinaryHeap` + insertion-sequence
>   tiebreaker); `PendingStateTransition` + `handle_cached_transaction_request_result`
>   + `throw_if_pending_state` (rules §5 is the binding contract here);
>   `initialize_transactions`, `begin_transaction`, `reset_transaction_state`; the
>   error machine; `FindCoordinatorHandler` + coordinator state (per §6.5, rules §2:
>   Sender-owned); the transactional `InitProducerId`/epoch-bump path; the
>   `TransactionManagerTest` subset reachable with that surface.
> - **5b** — `AddPartitionsToTxnHandler`, `AddOffsetsToTxnHandler`,
>   `TxnOffsetCommitHandler`, `EndTxnHandler`; the entry points that construct them
>   (`begin_commit`, `begin_abort`, `begin_completing_transaction`,
>   `send_offsets_to_transaction`, `maybe_add_partition` transactional arm); TV2;
>   2PC; the remaining tests.
>
> Where an item straddles the boundary, it lands with its handler and the Actor
> records the placement. Both halves run under N=45 with sequential Actor/Critic
> loops, each closing only on a zero-finding pass.

---

### Phase 6 (N=46) — Public producer API + Sender transactional loop

| Java source | Target Rust path |
|---|---|
| `producer/Producer.java` 43–66 | `src/producer/producer_trait.rs` — add 5 methods, delete the "not included in this phase" note at lines 34–36 |
| `producer/KafkaProducer.java` 592–620, 648–860, 956–990, 1507 | `src/producer/kafka_producer.rs` |
| `producer/internals/Sender.java` 234, 267–292, 459–530, 564–569, 917–926 | `src/producer/internals/sender.rs` |

Translations of note:
- `init_transactions` / `commit_transaction` / `abort_transaction` /
  `send_offsets_to_transaction` are **blocking in Java** (`result.await(maxBlockTimeMs, ..)`
  at `KafkaProducer.java:654, 742, 785, 820`) → `async fn` returning
  `Result<(), KafkaError>`, with `tokio::time::timeout(max_block_ms, ..)`
  mapping expiry to `KafkaError::timeout` (CLAUDE.md §9.1).
- `begin_transaction` does **not** block in Java (line 674–681, pure state
  transition) → stays a **sync** `fn`. Do not make it `async` for symmetry.
- ~~`prepare_transaction` (2PC) is on `KafkaProducer` only, **not** on the
  `Producer` interface — verified: `grep prepareTransaction Producer.java`
  returns nothing. Mirror that: inherent method on `KafkaProducer`, absent from
  the `Producer` trait.~~ **Wrong, corrected in Phase 6.** The grep is right and
  the inference is not: in `kafka/` 4.2 `prepareTransaction` exists only on
  `TransactionManager`, not on `KafkaProducer` either, so there is nothing to
  mirror. The 4.2 producer-side 2PC surface is `throwIfInPreparedState`
  (`KafkaProducer.java:968-976`), wired into `beginTransaction` and `doSend`. See
  §10.9 deviation 2 for the evidence.
- `maybe_send_and_poll_transactional_request` (`Sender.java:459–518`) is the
  riskiest single method in the milestone — see §6.6.
- `sendProduceRequest` (924–926) sets `transactional_id` and
  `use_transaction_v1_version` on the produce request → wire
  `ProduceRequestBuilder::builder(data, !is_transaction_v2_enabled)` at
  `sender.rs:977–982`.
- Remove the Phase-1 transactions guard (§7.1).

**Tests:** the transactional tests in `KafkaProducerTest.java`, plus the transactional
subset of `SenderTest.java`. This line said "the 27" until Phase 6 derived the set
mechanically and found 28 — see the Status note.

**Status: landed.** Every transactional `KafkaProducerTest` method is accounted for
(translated, or named and justified), and so is every transactional `SenderTest`
method (translated, owed with an owner, or blocked on named missing surface).

**The counts deliberately live in one place each, and it is not here.** They are in
the PHASE-6 TEST ACCOUNTING block in `src/producer/kafka_producer.rs` and the
`SenderTest.java` accounting block at the end of `src/producer/internals/sender.rs`,
each of which derives its numbers from a shipped program over the Java source and
pastes that program's real output. Restating them in this prose is how they went stale:
Critic 46 pass 1 moved both denominators (`KafkaProducerTest` 27 → 28,
transactional `SenderTest` 15 → 16) because two live Java tests were invisible to the
scope programs, and the fix updated the blocks while four prose sites kept the pre-fix
figures (pass 2 issue 2). A number with two homes has two chances to be wrong, so this
section now points at the homes instead.

What is stable and worth stating here: the `KafkaProducerTest` set partitions into
methods translated in this phase, exactly one justified non-translation
(`testNullGroupMetadataInSendOffsets`, which passes `null` for a value type), and the
idempotence-config methods Phase 1 already translated in `producer_config.rs`. The
transactional `SenderTest` group splits into the shutdown-path methods Phase 6
translated — the ones that needed `Sender::run`'s transactional tail, so no earlier
phase could have written them — plus the throttle method the repaired scope program
surfaced, with the remainder **handed to Phase 8**, two of those blocked on named
missing surface. Deviations are recorded in §10.9.

Also in this phase, outside the table: `PendingRequests` became shared state
(§10.9 deviation 1), and `await_sender_handle` lost-join bug fixed (deviation 3).

---

### Phase 7 (N=47) — `MockProducer` transactional surface

`src/producer/mock_producer.rs`. Java `MockProducer.java` fields
`transactionInitialized` (70), `transactionInFlight` (71), `sentOffsets` (75),
plus `initTransactions` (145), `beginTransaction` (162),
`sendOffsetsToTransaction` (182), `commitTransaction` (204),
`abortTransaction` (230), `fenceProducer`, and the uncommitted-record /
uncommitted-offset staging that makes
`shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled` pass.

**Tests:** the **44 named skipped tests** enumerated at
`src/producer/mock_producer.rs:525–577`. Delete that comment block as the tests
land. Pure in-memory state, no network — the cheapest full-parity phase in the
milestone, and it independently validates the Phase-6 trait shape.

One test stays skipped with justification already written and still valid:
`shouldThrowClassCastException` (`mock_producer.rs:579–588`) — tests Java type
erasure, which has no Rust analogue.

**Outcome.** The prediction held: all 40 class-level `MockProducer.java` methods in
scope landed, and **43 of the 44** named tests with them. `MockProducerTest.java`'s
55 `@Test` methods now split **53 translated / 2 not applicable**, derived rather
than asserted in the test accounting block at the end of `mock_producer.rs`.

The 44th, `shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction`
(`MockProducerTest.java:430`), joins `shouldThrowClassCastException` as the second
NOT APPLICABLE entry — and **not** for missing surface. Its
`NullPointerException` is not thrown by the mock at all: it comes from evaluating
`new ConsumerGroupMetadata(null)` inside the lambda — the one-arg constructor at
`ConsumerGroupMetadata.java:52`, delegating to the four-arg one declared at `:38`,
whose first statement is `Objects.requireNonNull(groupId, "group.id can't be null")`
at `:42` — before `sendOffsetsToTransaction` is entered — and with `Collections.emptyMap()` for the
offsets there is no other reachable throw. Rust's
`ConsumerGroupMetadata::new(impl Into<String>)` cannot receive null, so there is
nothing to implement (§10.10 deviation 4). The empty-offsets path it incidentally
exercises is covered by `shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata`
(438).

`shouldThrowClassCastException`'s justification was re-checked and stands, with one
addition: beyond type erasure, the Rust mock holds no serializers to mis-apply.

Two pre-existing defects in `Completion::complete` were fixed here (the error-path
callback got no metadata where Java passes a −1-filled `RecordMetadata`, and
`result.done()` preceded the callback instead of following it), and **four** landed
translations that were weaker than their Java originals were brought up to them:
`testManualCompletion` (107, Java compares the cause at 122; the translation asserted
a bare `is_err()`), `testMetadataOnException` (724, dropped all four values Java
checks at 727-731), and `shouldThrowOnSendIfProducerIsClosed` (624) /
`shouldThrowOnFlushProducerIfProducerIsClosed` (673), which matched their message
with `contains` rather than equality. The set is re-derived from the diff in the
in-file accounting block, which also records why the number is the wrong thing to
carry: the first two artifacts to state it both said "three" and named a *different*
three, and no count-level check can see that. Ten
`MockProducer.java` methods remain absent — the telemetry / metrics pair, tracked
as §9.23, blocking **zero** of the 55 tests — plus `partition`, whose absence is
the standing pre-serialized-bytes design decision. Deviations are recorded in
§10.10.

---

### Phase 8 (N=48) — `TransactionManagerTest` parity sweep + broker integration

- Close out any `TransactionManagerTest` method not landed in Phases 3/5, so the
  full 140 are accounted for (translated, or named-and-justified per DoD §3).
- New integration tests under `tests/integration/` against a testcontainers
  broker: idempotent produce with a forced epoch bump; transactional
  commit visible only after commit; abort discards; consume-transform-produce
  with `send_offsets_to_transaction` against the Milestone-8 consumer with
  `isolation.level=read_committed`.
- Final DoD closure: `make verify`, `design/current/` refresh,
  `marked_classes.txt` / `remaining_classes.txt` updates.

**Status: delivered.** What landed, and the three places the spec's expectations were
wrong:

  - **All 140 `TransactionManagerTest` methods are translated**, not
    "accounted for": 33 group A + 107 group B, with the OWED set empty. The
    accounting block in `transaction_manager.rs` carries the re-run derivation
    (`A-OWED=0  B-HAVE=107  B-OWED=0`). The 47 landed in `sender.rs`, beside the six
    group-A entries already there, because Java's `TransactionManagerTest` builds its own
    accumulator + `Sender` + `MockClient` and every one of those bodies drives them.
  - **The `SenderTest` hand-off closed too**, which the spec did not ask for but §9.19
    had assigned here: 10 of the 11 translated, 1 still blocked on §9.18 (re-verified by
    running the reproducer, not assumed). One of the two entries that had cited "missing
    surface" turned out to have its surface already present and already exercised — see
    the lesson in the accounting block.
  - **Two harness gaps in `MockClient` had to be filled first**, both real Java surface
    this port had skipped: `RequestMatcher` support on `prepareResponse` / `respond`
    (every `TransactionManagerTest` helper puts its assertions *inside* a matcher, so a
    port without them silently drops that coverage) and
    `disconnect(node, allowLateResponses)`.
  - **Two defects surfaced, one fixed and one filed.** The abort integration test failed
    on its first run and exposed a *consumer* defect with a false rarity justification —
    fixed, with the lesson, at §9.27. The transactional `SenderTest` group exposed
    §9.25, an empty batch pool on the log-truncation retry, which is filed rather than
    fixed for the reason §9.18 gives for its own gap.
  - **`make verify` cannot complete on this machine and CI is the gate for it.** Verified
    directly rather than repeated: `bindings/python/_confluentkafka.c` includes
    `<threads.h>`, and compiling a two-line probe against this Apple SDK gives
    `fatal error: 'threads.h' file not found`. So `make build-python` → `test-python` →
    `verify` cannot run here. The local gate is `make verify-sandbox`
    (`build-rust build-c format-check lint test-integration test-c`), which is what every
    Phase-8 commit was gated on.

---

## 4. `marked_classes.txt` / `remaining_classes.txt` deltas

Move from `remaining_classes.txt` to `marked_classes.txt` as each phase lands:

```
org.apache.kafka.common.utils.ProducerIdAndEpoch                       (line 450) → Phase 1
org.apache.kafka.common.requests.TransactionResult                     (line 151) → Phase 1
org.apache.kafka.clients.producer.internals.TransactionalRequestResult (line 457) → Phase 1
org.apache.kafka.clients.producer.internals.TxnPartitionEntry          (line 459) → Phase 1
org.apache.kafka.clients.producer.internals.TxnPartitionMap            (line 460) → Phase 1
org.apache.kafka.clients.producer.internals.TransactionManager         (line 471) → Phase 5
```

Plus the 10 new `common.requests.*` wrapper classes in Phase 2. **Leave
`org.apache.kafka.common.record.EndTransactionMarker` (line 70) in
`remaining_classes.txt`** — out of scope per §1.1.

---

## 5. New rules file: **recommended — create `.claude/rules/producer-transactions.md`**

Recommendation: **yes**, in Phase 1, governed the same way
`consumer-threading.md` governs Milestone 8. There are six load-bearing design
calls with no mechanical Java→Rust mapping, and without a written rule a Critic
cannot tell an intentional deviation from a bug:

1. **`Caller` enum replacing thread identity** (§6.2) — correctness-critical,
   zero Java analogue.
2. **Lock topology**: which `TransactionManager` state is shared behind
   `Arc<Mutex<..>>` vs. owned by the Sender task (§6.5). Java's four unguarded
   non-volatile fields are safe *only* by single-thread confinement; naively
   putting them behind the shared lock is wrong, and naively leaving them
   unsynchronized is also wrong.
3. **Lock ordering `deque` → `TransactionManager`** (§6.7), from
   `RecordAccumulator.java:877–926`.
4. **Never hold the `TransactionManager` guard across `.await`**, and **never
   race the network poll in `select!`** — extends `consumer-threading.md` §10's
   hard-won cancellation rule to the producer Sender (§6.6).
5. **`TransactionalRequestResult` keeps latch semantics** — re-awaitable, with
   `is_acked` distinct from `is_completed`. A `oneshot` is *wrong* (§6.4).
6. **`BTreeSet`/`BTreeMap` must not be keyed on interior-mutable sort keys**
   (§6.8).

Items 1, 2, 5 and 6 are exactly the class of decision that produced repeated
Critic churn in Milestone 8 before `consumer-threading.md` existed.

---

## 6. Risks and blockers

### 6.1 KIP-890 (TV2) and KIP-939 (2PC) are in the 4.2 source — not optional

Kafka 4.2's `TransactionManager` carries both. `isTransactionV2Enabled` (line
147) gates *behavioral* forks, not cosmetics: under TV2 the producer sends **no**
`AddPartitionsToTxn` (line 448) and **no** `AddOffsetsToTxn` (line 417), and
`EndTxn` returns a fresh pid/epoch. Implementing only the V1 path would look
correct against a V1 broker and silently misbehave against a modern one.
Mitigation: TV2 detection is *already unblocked* (`ApiVersions` finalized
features exist), and `ProduceRequest` already has the version cap. Both forks
land in Phase 5, tested via the `transactionV2Enabled` parameterized matrix that
`TransactionManagerTest` already provides.

2PC (`enable2PC`, `PREPARED_TRANSACTION`, `prepareTransaction`,
`keepPreparedTxn`) is smaller and self-contained. **Open decision:** could be
deferred (§7.4).

### 6.2 `shouldPoisonStateOnInvalidTransition` has no Rust analogue — highest-risk mapping

`TransactionManager.java:287–289`:

```java
protected boolean shouldPoisonStateOnInvalidTransition() {
    return Thread.currentThread() instanceof Sender.SenderThread;
}
```

On an invalid state transition Java behaves **differently depending on which
thread it is on** (documented at length in Java 234–286, KAFKA-14831):
application thread → throw, state unchanged, user can recover; Sender thread →
set `FATAL_ERROR`, store `lastError`, then throw ("poison"). Rust has no
equivalent of `instanceof SenderThread`, and `tokio::task::id()` is not a stable
substitute (the Sender is a spawned task whose id is not known to the app side
at call time).

Proposed mapping: thread an explicit `Caller { App, Sender }` parameter into
`transition_to`. Every call site must pass its origin literally. Rationale to
record in the rules file: the poison-vs-throw distinction is load-bearing for
the transactional guarantee, so it must be *explicit and reviewable* rather than
inferred. Risk: ~20 call sites, each an opportunity to pass the wrong value; a
Critic must check every one against the Java call chain.

### 6.3 `KafkaProducer` holds no client handle — structural obstacle

At `kafka_producer.rs:392–395` the `Sender` (and therefore the `KafkaClient`) is
**moved into** `tokio::task::spawn`. `KafkaProducer` retains only `accumulator`,
`metadata`, `running`, `force_close`, `wakeup`, `sender_handle`. But
`TransactionManager` is written and read by **both** sides: `KafkaProducer` calls
`initialize_transactions` / `begin_transaction` / `begin_commit` / `begin_abort`
/ `send_offsets_to_transaction` / `maybe_add_partition` /
`maybe_transition_to_error_state`, while the Sender task calls ~25 other
methods, and `RecordAccumulator` calls 12 more.

So `TransactionManager` must be `Arc<...>` shared three ways
(`KafkaProducer`, `Sender`, `RecordAccumulator`) with interior mutability. That
is fine, but it means Phase 4 changes the construction order in
`from_config`/`with_client`: the manager must be built **before** the accumulator
(which needs it) and before the Sender (which needs it), then cloned into all
three. Mitigation: this is a mechanical refactor, but it touches the most
load-bearing constructor in the crate, so Phase 4 should do it as its own commit
with the existing producer test suite green before any sequence logic is added.

### 6.4 `TransactionalRequestResult` is not a `oneshot`

Java uses `CountDownLatch(1)` plus a **separate `volatile boolean isAcked`** that
is set **only inside `await()`** (`TransactionalRequestResult.java:62`).
`handle_cached_transaction_request_result` (Java 1261–1283) keys off
`isAcked()`, not `isCompleted()`: a `commitTransaction` that *completed* but was
never *awaited* must return the **same** result object on retry, and a
`commitTransaction` that timed out must be retryable while rejecting a
*different* operation with `IllegalStateException`.

A `tokio::sync::oneshot` cannot express this (single-consumer, not
re-awaitable). Required shape: a struct holding `Arc<Notify>` (or a
`watch` channel) + `Mutex<Option<KafkaError>>` + `AtomicBool` completed +
`AtomicBool` acked, with `await_result(timeout)`, `done()`, `fail(err)`,
`is_completed()`, `is_acked()`, `is_successful()`. Getting this wrong produces
a hang or a double-commit that no unit test in Phase 1 would catch — the
covering tests are in `TransactionManagerTest` (Phase 5).

### 6.5 Four Java fields are safe only by single-thread confinement

`inFlightRequestCorrelationId` (136), `transactionCoordinator` (137),
`consumerGroupCoordinator` (138), `coordinatorSupportsBumpingEpoch` (139) are
**non-volatile and not consistently guarded**. E.g. `clearInFlightCorrelationId`
is called from `TxnRequestHandler.onComplete` at line 1410 — *outside* the
`synchronized` block that starts at 1421. Likewise `pendingRequests` (a plain
`PriorityQueue`) is mutated via the **unsynchronized** `lookupCoordinator(TxnRequestHandler)`
(969) that `Sender.java:522` calls directly.

These are not bugs in Java: they are touched exclusively by the Sender thread.
The Rust translation must decide deliberately: **coordinator nodes, in-flight
correlation id, `coordinatorSupportsBumpingEpoch`, and the pending-request queue
belong to the Sender task's own (unshared) state**, while the `volatile` fields
and the partition maps go behind the shared lock. Putting everything behind one
mutex would be *safe but slower and less faithful*; leaving them unsynchronized
in Rust would not compile. A Critic reading only the Java `synchronized`
keywords will get this wrong in both directions — hence the rules file.

### 6.6 `maybeSendAndPollTransactionalRequest` — cancellation and blocking

`Sender.java:459–518` contains, on the Sender thread: `client.poll(...)` at 462,
496, 509; `awaitNodeReady(...)` at 484 (which is `NetworkClientUtils.awaitReady`,
blocking); `time.sleep(retryBackoffMs)` at 501 and 525.

In Rust all four become `.await` points inside the Sender task. Two hazards:

- **The network poll is not cancel-safe in this codebase.** This is the exact
  failure documented in `consumer-threading.md` §10 and
  `design/current/consumer-join-stall-rootcause.md`: `initiate_connect` calls
  `connection_states.connecting()` (a persisted side effect) and *then* awaits
  the TCP handshake, so a dropped poll strands the node in `Connecting` until the
  ~10 s connection-setup timeout. The producer Sender must therefore **not**
  race the poll in a `tokio::select!` either. Rule 4 in §5 exists to carry this
  forward; Milestone 8 learned it the hard way.
- **No `MutexGuard` may be held across any of these awaits** (CLAUDE.md §9.6.2).
  Since this method calls ~10 `TransactionManager` methods interleaved with 3
  polls and 2 sleeps, the guard must be acquired and dropped repeatedly. That
  changes atomicity relative to Java, where the Sender thread's view is stable
  simply because it *is* the only writer. Each re-acquire is a place where Java's
  implicit consistency can break. This is the second-highest-risk item after
  §6.2 and should be reviewed method-by-method in Phase 6.

### 6.7 Lock ordering: `deque` → `TransactionManager`

`RecordAccumulator.java:877–926` assigns sequences **inside**
`synchronized (deque)` while calling into `synchronized` `TransactionManager`
methods (`maybeUpdateProducerIdAndEpoch` 908, `sequenceNumber` 918,
`incrementSequenceNumber` 919, `addInFlightBatch` 924). So Java's lock order is
deque → TransactionManager, and it is never inverted.

Rust must preserve that order or deadlock. Good news: the whole block is
CPU-bound with no `await`, so `std::sync::Mutex` is correct (same reasoning as
`consumer-threading.md` §16 for `SubscriptionState`), and the Rust accumulator
already holds per-partition deques as
`DashMap<i32, Mutex<VecDeque<ProducerBatch>>>` (`record_accumulator.rs:126–131`).

### 6.8 `TreeSet` keyed on a mutable sort key

`TxnPartitionEntry.inflightBatchesBySequence` is a `TreeSet<ProducerBatch>`
ordered by `(producerId, producerEpoch, baseSequence)`
(`TxnPartitionEntry.java:62–65`) — **all three of which mutate** via
`resetProducerState`. Java copes by rebuilding the set in `resetSequenceNumbers`
(154–161); the comment at 58–61 records the bug that motivated the 3-key
comparator.

A direct Rust `BTreeSet<ProducerBatch>` whose `Ord` reads interior-mutable
fields violates `BTreeSet`'s invariants (logic corruption, not UB, but
unpredictable). Required: key on an **explicit snapshot tuple** —
`BTreeMap<(i64, i16, i32), ProducerBatch>` — and rebuild the map wherever Java
rebuilds the set.

Related, and already handled: `TxnPartitionEntry.incrementSequence` (104)
delegates to `DefaultRecordBatch.incrementSequence`
(`DefaultRecordBatch.java:557`), which **wraps at `Integer.MAX_VALUE`** rather
than using plain `+`. The Rust equivalents already exist —
`increment_sequence` at `src/common/record/default_record_batch.rs:887` and
`decrement_sequence` at 898 — so Phase 1 reuses them rather than adding them.

### 6.9 Test volume is the dominant cost

Implementation is ~3 400 Java LoC. Tests are ~**8 100** Java LoC across
`TransactionManagerTest` (4 487), the idempotence/txn share of `SenderTest`
(43/76 tests of 4 002 lines), `KafkaProducerTest` txn (27 tests of 2 952),
`MockProducerTest` txn (44 tests of 751), plus 695 lines of wrapper tests.
Roughly **70% of this milestone is test translation.** Any schedule that budgets
implementation only will be wrong by a factor of ~3.

### 6.10 `enable.idempotence` default is currently dishonest

See §7.1 — this is an open decision, not just a risk.

---

## 7. Open decisions for the user

### 7.1 The `enable.idempotence` honesty gap — **recommend: faithful config validation now + explicit failure for the unimplemented path, removed as each phase lands**

Facts established by reading both sides:

- `ProducerConfig::enable_idempotence` defaults to **`true`**
  (`producer_config.rs:204`) but is **never read** by `KafkaProducer`, `Sender`,
  or `RecordAccumulator` (zero reads confirmed). `transactional_id` (line 181)
  likewise. `INIT_PRODUCER_ID` has no use outside `api_keys.rs`. So the producer
  advertises idempotence and delivers at-least-once.
- Java's own behavior is subtler than "default true":
  `postProcessAndValidateIdempotenceConfigs` (`ProducerConfig.java:591–651`)
  **silently disables** idempotence when `retries == 0` or `acks != all`
  *if the user did not explicitly set `enable.idempotence`* (line 628), and
  raises `ConfigException` when the user *did* set it explicitly and the configs
  conflict. It also always rejects `max.in.flight > 5` with idempotence on,
  rejects `transactional.id` without idempotence, and rejects
  `transaction.timeout.ms` together with 2PC. **None of this validation exists in
  Rust** — `ProducerConfig::MAX_IN_FLIGHT_REQUESTS_FOR_IDEMPOTENCE` is declared
  at line 37/235 and never read.

Three options considered:

- **(A) Leave as-is until Phase 4.** Rejected: violates CLAUDE.md §5 for the
  several phases in between, and the config validation is a missing translation
  regardless.
- **(B) Honor the `true` default immediately by rejecting construction.** Rejected:
  the default is `true`, so *every* existing producer user and every existing
  producer test would fail at construction mid-milestone.
- **(C) Recommended.** In Phase 1: translate
  `postProcessAndValidateIdempotenceConfigs` **faithfully** into
  `ProducerConfig` (including the silent-disable arm, which is what keeps
  today's default-config users working and is what Java does), and add the
  missing `transaction.two.phase.commit.enable` key. Then, in
  `KafkaProducer::from_config`/`with_client` — *not* in the config layer — add a
  temporary guard: if the effective config requests idempotence **explicitly**
  or sets `transactional.id`, return
  `KafkaError::with_message(Errors::UnsupportedVersion, ..)` naming the
  unimplemented feature. Users who never touched the config get today's
  behavior (now with Java's silent-disable semantics and a `warn!`); users who
  explicitly ask get an honest error instead of silent non-compliance.

Why the guard belongs in `KafkaProducer` and not `ProducerConfig`: the config
translation should mirror Java exactly, so "not yet implemented" must not
pollute it. Removal is tracked as an explicit deliverable — the idempotence arm
in Phase 4, the transactional arm in Phase 6 — so nothing is left behind
(CLAUDE.md §5 forbids lingering TODOs).

**Decision needed:** confirm (C), or pick (A)/(B).

**Status: complete.** (C) was taken. The idempotence arm of the guard was removed in
Phase 4 and the transactional arm in Phase 6; `from_config` now accepts
`transactional.id` and builds a transactional manager. `configure_transaction_state`
dropped the `Result` the guard was its only user of, and
`test_guard_rejects_transactional_id` was replaced by
`test_transactional_id_builds_a_transactional_manager` plus two message-pinning tests
for `throwIfNoTransactionManager` and `ensureTransactional`.

### 7.2 Bindings deferral — **recommend: defer, as the briefing proposes**

Concur. Rationale to record: it roughly doubles the phase count, and
transactions are the one producer surface where the Milestone-9 FFI access-guard
model needs genuinely new thinking. The guard is "one operation in flight" and
`wakeup()` bypasses it — but a *transaction* spans multiple FFI calls
(`begin` → N× `send` → `commit`). So the guard needs a story for an open
transaction: does it stay held from `begin_transaction` to
`commit_transaction` (which would block the `send` calls that must happen in
between), or does the FFI need transaction-state-aware admission control? That
is a design question, not a translation question, and it deserves its own
milestone rather than being improvised at the tail of this one.

**Decision needed:** confirm deferral. If overturned, add ~6 phases (producer
txn FFI, `producer.py` txn API, both gRPC servers, multilanguage scenarios).

### 7.3 New rules file — **recommend: yes, `.claude/rules/producer-transactions.md` in Phase 1**

See §5 for the six rules and the reasoning. It is cheap (one file, written once)
and it is the difference between a Critic being able to review §6.2/§6.5
correctly or not.

**Decision needed:** confirm, or say to fold these notes into the per-phase
`PLAN.md` files instead (weaker: phase plans are not loaded into agent context
the way `.claude/rules/` files are).

### 7.4 KIP-939 two-phase commit — **recommend: keep in scope, Phase 5, but split it out if Phase 5 overruns**

It is small (one state, `prepareTransaction`, `keepPreparedTxn`,
`preparedTxnState`, `isPrepared`, `is2PCEnabled`, plus the
`throwIfInPreparedState` guards at `KafkaProducer.java:968/989`) and it is
entangled with the state table we are writing anyway (`PREPARED_TRANSACTION`
appears in three transition arms). Deferring it means editing the state machine
twice. Keep it — but it is the natural thing to cut if Phase 5 needs to shrink.

**Decision needed:** confirm in-scope, or defer to a later milestone.

### 7.5 Scope calls found while reading the source — **recommend: accept all three**

Discovered during planning, not in the briefing:

1. **`WriteTxnMarkers` request/response: out of scope.** Admin/coordinator RPC,
   not producer (§1.1). Its two Java test files
   (`WriteTxnMarkersRequestTest`, `WriteTxnMarkersResponseTest`) go out of scope
   with it — justified under DoD §3 as belonging to an untranslated Admin
   surface, not as "irrelevant to Rust."
2. **`EndTransactionMarker`: out of scope.** Broker-side control-record write
   path (§1.1).
3. **`MockProducer` transactional surface: *in* scope**, Phase 7 — 44 named
   skipped tests at `mock_producer.rs:525–577`. This is additional scope beyond
   the briefing and is required by DoD §3.

**Decision needed:** confirm.

---

## 8. Effort estimate

Relative sizing, based on Java LoC actually counted, Java test-method counts
actually counted, and the number of Rust insertion points actually located.
"Units" are comparable to one another, not to wall-clock.

| Phase | N | Impl | Test | Total | Basis |
|---|---|---|---|---|---|
| 1 | 41 | 2 | 1 | **3** | 350 Java LoC, mostly mechanical. `TransactionalRequestResult` is the only subtle part (§6.4). `TxnPartitionEntry`/`Map` have **no dedicated Java test file** — covered indirectly via `TransactionManagerTest`, so Phase 1's own tests are `ProducerConfigTest`'s idempotence cases plus Rust-side unit tests for the two maps. |
| 2 | 42 | 4 | 3 | **7** | 1 030 Java LoC across 10 files + 18 exhaustive match sites + 695 lines of wrapper tests. High volume, low risk — the pattern is fully established by 12 existing pairs. |
| 3 | 43 | 5 | 2 | **7** | The idempotence slice: 4 states, 1 handler, ~25 methods. Only ~15–20 test methods available (§2). Risk concentrated in the `Caller` enum (§6.2). |
| 4 | 44 | 6 | 5 | **11** | Small diff, high blast radius: 10 insertion points across `sender.rs`/`record_accumulator.rs`, the constructor re-ordering of §6.3, plus lock-ordering (§6.7) and hot-path allocation (CLAUDE.md §11 — sequence assignment runs per batch on the drain path). Test side is the idempotence share of `SenderTest` + `RecordAccumulatorTest`. |
| 5 | 45 | 9 | 12 | **21** | **Largest phase.** Remaining 5 states, 5 handlers, priority queue, `PendingStateTransition`, TV2 forks, 2PC — against the bulk of a 4 487-line test file with a `transactionV2Enabled` matrix. Candidate for a 5a/5b split. |
| 6 | 46 | 5 | 4 | **9** | Public API is small, but `maybeSendAndPollTransactionalRequest` is the milestone's hardest async translation (§6.6), and 27 `KafkaProducerTest` tests. |
| 7 | 47 | 2 | 4 | **6** | Pure in-memory state; 44 test methods. Low risk, high test count. |
| 8 | 48 | 1 | 6 | **7** | Test-only sweep plus new broker integration tests. |
| | | **34** | **37** | **71** | |

Two things this estimate is asserting, both worth challenging at approval:

- **Testing is ~52% of total effort** (37/71), and Phase 5 alone is ~30% of the
  milestone. If Phase 5's estimate is wrong, the milestone's estimate is wrong.
- **Phase 4 is under-weighted by LoC and over-weighted by risk.** It is the
  smallest diff of any implementation phase but touches the producer's hottest
  path and its most load-bearing constructor. Historically (Milestone 8) this
  shape of phase generated the most Critic cycles.

Comparison anchor: Milestone 8 required 40 phase-numbers for the consumer
(~1 000 Java test methods). This milestone is roughly one-fifth that scale.

---

## 9. Follow-ups (deferred work, tracked)

Items discovered or decided during Milestone 11 that are deliberately **not**
done inside it. Each records what, why deferred, and how to verify the fix.

### 9.1 Code generator omits Java's non-default-at-unsupported-version guard

**Status:** open. Found in Phase 2 while translating
`RequestResponseTest.testInitProducerIdRequestVersions`.

For a version-gated non-tagged field, Java's generated `write` emits two halves:

```java
if (_version >= 3) { _writable.writeLong(producerId); }
else if (producerId != -1) {
    throw new UnsupportedVersionException(
        "Attempted to write a non-default producerId at version " + _version);
}
```

This project's generator emits only the first. A non-default value at an
unsupported version is therefore **silently dropped** rather than rejected: Java
refuses to encode a message it cannot represent faithfully, while this port
encodes a valid-but-different message and reports nothing. The wire result is a
well-formed *older* request missing the caller's value, so the broker accepts it
— there is no error anywhere in the path.

**Scope:** systemic. Affects every version-gated non-tagged field across all 197
generated message types, not only `InitProducerIdRequest`.

**Severity:** a missing safety net rather than a live fault. The bad branch is
only reached when client code sets a field without checking the negotiated
version — itself a programming error. Java converts that error into an
exception; here it becomes silent wire divergence. Worth fixing precisely
because this milestone's guarantee (no duplicate records) depends on
`producerId` reaching the broker.

**Fix location:** `generator/src/lib.rs`, the field-write emission function
(the `has_version_check` block around lines 1176-1187). Three pieces needed:

  1. The "is this field at its default?" condition. `get_default_check`
     (line ~1645) is close but was written for *tagged*-field semantics
     ("should this be written?"), so it needs adapting rather than reusing
     as-is.
  2. The original spec field name for the message text — Java's wording is
     `producerId`, not `producer_id`.
  3. Emission of the `else if` branch where the code currently just closes the
     version `if`.

Estimated ~20-30 lines in one function.

**Unknown, and the reason this is deferred:** enabling the check regenerates all
197 message types with a new error path. Any existing code that sets a field and
then serializes at a lower version starts failing. That count cannot be derived
by reading — it must be measured by making the change locally and running the
suite (~1 hour).

**Test coverage:** exactly **one** Java test in the whole `clients` module
asserts this behaviour (`RequestResponseTest.testInitProducerIdRequestVersions`),
and it covers `InitProducerId`. Verified: none of the other Phase 2 pairs'
dedicated test files contain such an assertion, so no further phases will
surface additional skips from this gap.

#### Refinement (found in Phase 2, `TxnOffsetCommit`): the check applies only to
#### **non-ignorable** fields

Java's generator emits the non-default-at-unsupported-version check **only** when
the field is not marked ignorable — `MessageDataGenerator.java:792`:

    if (!field.ignorable()) {
        cond.ifNotMember(__ -> {
            field.generateNonIgnorableFieldCheck(...);
        });
    }

So the spec's `"ignorable": true` flag is load-bearing, and the two cases differ:

| Field | `ignorable` | Java | Rust today |
|---|---|---|---|
| `InitProducerIdRequest.ProducerId` (v3+) | absent | **throws** | silently drops ← the gap |
| `TxnOffsetCommitRequest.CommittedLeaderEpoch` (v2+) | `true` | silently drops | silently drops ← **correct** |

This materially narrows the fix: **the generator must consult the `ignorable`
flag, not add the check unconditionally.** Adding it everywhere would start
rejecting legitimate ignorable-field drops that Java accepts — turning a
missing-error bug into a spurious-error bug, across all 197 message types.

Discovered by an all-versions round-trip test in
`txn_offset_commit_request.rs`, which initially failed because it asserted the
leader epoch survived at v0/v1. The expectation was wrong, not the code; the test
is now version-aware and documents why.

**How to verify the fix:** remove the `#[ignore]` from
`test_init_producer_id_request_versions` in
`src/common/requests/init_producer_id_request.rs`. The assertion is already
correct and will pass once the generator is fixed. Do **not** weaken it to match
current behaviour.

**Do NOT bundle this into a transactions phase.** It touches every message type;
mixing it with transaction work makes both changes hard to review and a failure
ambiguous between the two.

### 9.2 Migrate the Java base from 4.2.0 to 4.3.1

**Status:** decided — do it **after** Milestone 11 completes, as its own work.

Evaluated in full during this milestone. There are **no functional changes to
transactions or idempotence** in 4.3.1: the wire protocol, the sequence-number
logic, the state machine table, the class inventory, and the test counts
(122/75/77/55) are all identical. What changed:

  - `TransactionalRequestResult.await()` loses its no-arg overload; the timed
    overload gains a third `expectedTimeoutReason` parameter appended to the
    timeout message. **44 call sites** across `TransactionManagerTest`,
    `SenderTest`, `KafkaProducerTest`, and `KafkaProducer`.
  - `KafkaProducer.throwIfInPreparedState()` deleted as redundant — the manager
    already rejected both guarded operations (invalid `PREPARED_TRANSACTION →
    IN_TRANSACTION` transition; `maybeAddPartition`'s `currentState !=
    IN_TRANSACTION` check). Operations stay rejected; only the message differs.
  - `throwIfPendingState(String)` → `throwIfPendingState(TransactionOperation)`,
    a new 4-value private enum. Messages byte-identical.
  - Four timeout-reason constants added to `KafkaProducer`; one to `Sender` for
    expired batches.
  - `MockProducer`: javadoc only.
  - `ProducerConfig`: import + javadoc only — **Phase 1 commit 8 is unaffected**.

**Cost of deferring: ~1-1.5 days**, roughly 2% of the milestone. Low risk:
deleting the no-arg method and adding a parameter makes **every** call site a
compile error, so none can be missed.

**Why after, not before:** test parity gets proven against one stable base
first. If something breaks post-migration, the cause is unambiguous.

**Accept:** Java line-number citations in commits and
`.claude/rules/producer-transactions.md` are against 4.2.0. `TransactionManager`
references shift by **+18** at 4.3.1 (`shouldPoisonStateOnInvalidTransition`
287→305, `handleFailedBatch` 788→806). `TxnPartitionEntry` is unaffected (163
unchanged). The rules file records its base version to keep citations
unambiguous.

### 9.3 `record` → `record.internal` visibility demotion

**Status:** open, and **independent of transactions** — do not fold into 9.2.

4.3.1 moved the entire `org.apache.kafka.common.record` package to
`record.internal` (35 classes, ~7 500 lines; 83 client files re-imported). Only
`TimestampType` remained public.

CLAUDE.md §2 requires classes in an `internal` package to be `pub(crate)`.
`src/common/record/` is currently `pub mod record` with **13 public
re-exports**, so a faithful 4.3.1 base makes this a **breaking change to this
crate's public API**. Blast radius in Rust today: 32 files, 100 reference lines.

Verified safe: the C FFI does **not** expose these types. The `RecordBatch`
matches in `src/ffi/producer.rs` are its own unrelated local structs
(`RecordBatchCompletion`, `RecordBatchCallbackTarget`), so the C and Python
bindings are unaffected.

Deserves its own review because it is a public-API break unrelated to either
transactions or the 4.3.1 semantic changes.

### 9.4 Remove the `MILESTONE-11 GUARD`

**Status:** DONE. The idempotence arm was removed in **Phase 4**, the guard in
`TransactionManager::new` in **Phase 5a** (§10.7 deviation 10), and the transactional
arm in `from_config` in **Phase 6**. No `MILESTONE-11 GUARD` remains as a live check:
`grep -rn 'MILESTONE-11 GUARD' src/` now returns only comments that record its removal
and the test-block header that explains what those tests cover instead.

`KafkaProducer::from_config` used to reject explicit `enable.idempotence=true` and
any `transactional.id` (`src/producer/kafka_producer.rs`, marked
`MILESTONE-11 GUARD:`). Phase 4 removed the idempotence arm — `enable.idempotence`
is now honoured end to end — and replaced its two rejection tests with
`test_explicit_enable_idempotence_builds_a_transaction_manager` and
`test_disabled_idempotence_builds_no_transaction_manager`, which assert the two
arms of `configureTransactionState` instead. `TransactionManager::new` keeps its
own guard on `transactional_id` (PLAN §10.5 deviation 1) until Phase 5.

Phase 6 removed what was left: the `transactional.id` rejection in `from_config` and
`test_guard_rejects_transactional_id`, replaced by
`test_transactional_id_builds_a_transactional_manager` plus two tests pinning the two
*real* rejection messages a transactional call can hit — `throwIfNoTransactionManager`
when there is no manager at all, and `ensureTransactional` when the manager is merely
idempotent. `configure_transaction_state` also dropped its `Result`, the guard having
been its only error.

### 9.5 Critic review of Phase 1

**Status:** DONE — review loop **closed** 2026-08-04. Three Critic passes per
`agent-roles.md` steps 2-6, converged clean on pass 3. Archived at
`design/history/Milestone-11/Phase-1/COMMENTS.DONE.41.md`; one partial false
positive in `COMMENTS.FP.md`.

| Pass | Findings | Fixes |
|---|---|---|
| 1 | 8 | `0a8612e` (7 fixed) + `6aeb30c` (validation); 1 partially rejected |
| 2 | 1 | `363a7c7` |
| 3 | **0** | — signed off |

**Pass 1's material finding:** `TxnPartitionEntry::reset_sequence_numbers` took
its tracked in-flight membership from the caller's slice rather than its own set,
so a short slice silently cleared tracking and rewound the partition's sequence
counter.

**Pass 2 caught the fix being wrong twice over.** The pass-1 fix added a
missing-batch error justified by an audit claiming the state was unreachable. The
audit was wrong: `Sender.reenqueueBatch` (`Sender.java:750-752`) does not call
`transactionManager.removeInFlightBatch` — unlike the split path at `:685` — so a
retried batch leaves the Sender's map while **staying tracked**, and Java asserts
this at `RecordAccumulator.java:558-560`. Erroring there would have broken
idempotent recovery, which is the very path `bumpIdempotentProducerEpoch` →
`startSequencesAtBeginning` (`:655`) serves. Rules §7's "sole owner" claim was
therefore false and is corrected: **ownership alternates**, and Phase 4 must
assemble the batch pool from both the Sender's map and the accumulator's deque.

Writing that regression test then exposed a *second* defect in the pass-1 fix —
it resolved and mutated in one pass, leaving already-visited batches rewritten
when a later key was missing. Now resolves every key before mutating anything.

**Two rules-file additions came out of the loop:** §9 (the
`UnknownProducerId <: OutOfOrderSequence` subtype relation that flat error codes
lose) and §11 (version-gated field checks apply only to non-`ignorable` fields).

**Still open from this item, and inherent rather than a gap:** four of the five
Phase 1 types have no *Java-parity* test coverage, because Kafka has no test file
for any of them. Fidelity rests on source reading plus Rust-authored tests until
`TransactionManagerTest` lands in Phases 3/5. The six translated `ProducerConfig`
tests are the exception and pass. The Critic verified both Phase 1 plan overrides
(§6.8) as correct with the plan wrong.

**Lesson for later phases:** two of the three passes found a real defect, and the
second one was in the *fix* for the first. Do not treat a single Critic pass as
sufficient — `agent-roles.md` step 6 loops back to step 2 for a reason.

### 9.6 C FFI / Python / gRPC multilanguage harness for transactions

**Status:** deferred to a follow-up milestone (§7.2).

Beyond the phase-count cost, the Milestone 9 FFI access-guard model has no story
for an *open* transaction: a transaction spans `begin` → N×`send` → `commit`,
while the binding is one-operation-in-flight. Needs design, not a mechanical
extension.
### 9.7 Bring nine pre-existing `RequestBuilder`s in line with rules §12

**Status:** open. Identified by Critic 42's third pass while reviewing rules §12.

§12 requires a builder's `latest_allowed_version` to mirror whichever Java
`AbstractRequest.Builder` constructor its counterpart invokes. Nine builders
predating the rule still call `latest_version()`. **All are behaviourally correct
today, but for two different reasons — and conflating them will introduce a bug.**

> ⚠️ **Do NOT mechanically switch all nine to
> `latest_version_with_unstable(false)`.** Four of them must **stay** on
> `latest_version()` because **Java deliberately passes `latestVersion()`**
> (`OffsetCommitRequest.java:55`, `OffsetFetchRequest.java:64`,
> `ApiVersionsRequest.java:43`, `OffsetsForLeaderEpochRequest.java:57`). For two of
> those — `OFFSET_COMMIT` and `OFFSET_FETCH` — switching is not merely unfaithful
> but **actively wrong in this repo**: it caps them at **9 instead of 10**
> (`offset_commit_request.rs:183`, `offset_fetch_request.rs:286`), diverging from
> Java on the consumer's offset-commit and offset-fetch paths.

> **Where that 9-vs-10 comes from — read this before checking the flag.** The two
> accessors differ for those APIs only because **`generator/messages/`** (the corpus
> `build.rs` actually compiles) sets `"latestVersionUnstable": true` for them.
> **Kafka 4.2 does not** — in `kafka/clients/src/main/resources/common/message/`
> only `InitProducerIdRequest.json` sets it true. So a reviewer who checks the flag
> in `kafka/`, as CLAUDE.md's "Source Reference" directs, finds nothing and will
> conclude this warning is false. It is not: the divergence is real in Rust because
> the build corpus is a pre-4.2 snapshot. See §9.9. The instruction above does not
> depend on the flag — it rests on Java's `super(...)` call, which is stable across
> both corpora.

Per-group reasons — each is the group's *own* reason, not a shared one:

  - **Faithful group** (`api_versions`, `offset_commit`, `offset_fetch`,
    `offsets_for_leader_epoch`): correct because **Java's own bound is
    `latestVersion()`**. `latest_version()` is the faithful translation and must
    **stay**. Only the "deliberate" marker is missing. This reason is independent of
    any flag value.
  - **Implicated group** (the five below): correct because their APIs carry
    `latestVersionUnstable: false` **in `generator/messages/`** — verified per
    member, not assumed for the group. These are the ones to switch. (`METADATA`,
    `FIND_COORDINATOR`, `SASL_HANDSHAKE`, `SASL_AUTHENTICATE` and
    `CONSUMER_GROUP_HEARTBEAT` set no flag at all, which defaults to false.)

Two groups:

| Group | Files | Action |
|---|---|---|
| Faithful but unmarked (Java's own bound *is* `latestVersion()`) | `api_versions_request.rs`, `offset_commit_request.rs`, `offset_fetch_request.rs`, `offsets_for_leader_epoch_request.rs` | add the "deliberate" marker §12 requires, so it stays distinguishable from "not yet reached" — do **not** change the expression |
| Implicated by §12 | `metadata_request.rs:191`, `find_coordinator_request.rs:192`, `sasl_handshake_request.rs:116`, `sasl_authenticate_request.rs:122`, `consumer_group_heartbeat_request.rs:138` | switch to `latest_version_with_unstable(false)` |

`consumer_group_heartbeat_request.rs` needs thought rather than a mechanical
change: Java's builder takes `enableUnstableLastVersion` as a **parameter**, which
the Rust builder does not model at all. Decide whether to thread it through or
document why not.

**Deliberately not folded into Milestone 11.** Nine files across the common and
consumer surface with zero behaviour change is a poor fit for a transactions phase,
and mixing it in would make any regression ambiguous between the two. Rules §12
records this scope explicitly so a Critic reviewing pre-existing code cites this
item instead of raising nine findings.

**Consider on landing:** §12 is written as a general rule but lives in
`producer-transactions.md`. Relocating it to a shared rules file would be the
natural move once it governs code outside the producer.

### 9.8 Critic review of Phase 2

**Status:** DONE — loop **closed 2026-08-04 on a clean sixth pass** (zero findings).
Six Critic 42 passes archived at
`design/history/Milestone-11/Phase-2/COMMENTS.DONE.42.md`.

| Pass | Findings | Where | Fixes |
|---|---|---|---|
| 1 | 5 (1 functional) | Phase 2 translation | `1391c69` |
| 2 | 2 (non-behavioural) | the pass-1 fix | `b62e218` |
| 3 | 1 | rules §12 prose, written to fix pass 2 | `e8a91e7` |
| 4 | 2 | §9.7 justification + a §12 citation | `8264450` |
| 5 | 3 | §9.7 / §12 flag anchoring, and §9's structure | `e7442fe` |
| 6 | **0** | — | closes the loop |

Pass 6 verified the pass-5 fixes and re-derived §9.9's figures independently, then
audited **every** consumer of `latestVersionUnstable` in the tree — not just the txn
builders — to confirm §9.9's "no live defect" claim, Streams pair included.

**Phase 2's `src/` has been clean since pass 1** — `git diff --stat e8a91e7 HEAD`
over `*.rs` is empty, confirmed by pass 5. Every round after the first found a
defect in a **fix**, never in the translation.

**The functional finding (pass 1):** `InitProducerIdRequestBuilder` offered v6 where
Java caps at v5, because `latest_version()` hardwires the unstable-inclusive
accessor while Java's `super(apiKey)` passes `false`. Compounds with §9.1 — v6 is
the 2PC version Phase 5 implements, and its two new fields are non-ignorable, so at
a negotiated v5 the client would silently drop them where Java throws. Produced
**rules §12** and the §9.7 follow-up.

**One error class caused three of the later findings** — a claim about
`latestVersionUnstable` not tied to the corpus it was read from:

  - pass 2: the flag's **presence** checked instead of its value → two classes
    wrongly reported as divergent;
  - pass 4: a flag value asserted **across a set** without checking each member →
    §9.7 would have told its executor that switching all nine was inert;
  - pass 5: the corrected claim anchored to **`kafka/`**, where the flag does not
    exist — it is a property of `generator/messages/` (§9.9).

The common root: `generator/messages/` and `kafka/` are both "the specs", CLAUDE.md
names only the latter, and they disagree. §9.9 addresses that; a rule suggestion to
name the build corpus in CLAUDE.md's "Source Reference" is recorded in
`COMMENTS.DONE.42.md` for the CLAUDE.md change process (`agent-roles.md` §2) rather
than applied directly.

**Confirmed by independent re-derivation across passes:** all four broker-side
scoping omissions, the deterministic-sort deviation's safety at every version, all
ten txn dispatch arms plus their `OffsetCommit`/`OffsetFetch` neighbours, all seven
sort sites, and both halves of the `ignorable` distinction.

**Lesson, consistent with §9.5:** six of seven Critic passes across the two phases
found something real. Twice this phase the loop was declared closed without a
zero-finding pass — once on the Critic's own "does not warrant a fourth round"
(pass 3), once on the Actor's self-verification after pass 5 was interrupted. Both
were premature; `agent-roles.md` §2's gate is a clean pass, and nothing else
substitutes for it.

**What the clean pass did NOT establish — added after the fact.** A DoD gap was found
*after* this loop closed: DoD §3's byte-level wire tests do not exist for any of the
ten wrappers (§9.14). Six Critic passes and two Actor self-audits all missed it, and
the phase was twice reported to the user as complete before it surfaced.

The instructive part is *how* it was missed. Every pass verified Phase 2 against the
**Java source** — methods, tests, dispatch arms, error codes — and Phase 2 is faithful
there. Nobody walked the DoD clause by clause. Both the Critic prompts and the Actor's
audits inherited the same frame, so agreement between them carried no independent
information: a shared blind spot is invisible to repetition, and running the loop more
times would never have found this.

Two concrete take-aways for later phases:

  - A review pass should check the **DoD text itself**, clause by clause, not only
    Java fidelity. Faithful-to-Java and DoD-complete are different properties and this
    phase satisfied the first while failing the second.
  - Treat a document's claim that a test exists as a claim to verify. §9.14 records two
    places asserting these tests as an existing constraint; either would have exposed
    the gap if anyone had gone looking for the suite they cite.

### 9.9 `generator/messages/` was a pre-4.2 snapshot diverging from `kafka/`

**Status:** DONE — refreshed from the submodule 2026-08-04. The two trees are now
byte-identical for all 197 schemas (`kafka/`'s `README.md` is not copied; it
documents the schema format for Java's generator and is not an input to ours).

**What it was.** CLAUDE.md's "Source Reference" names `kafka/` (Apache Kafka 4.2) as
the contract, but `build.rs:44` generates all 197 wire types from
**`generator/messages/`**, a separate copy touched by exactly one commit —
`6cd275c Initial branch (#1)`, 31 Mar 2026 — and never refreshed. The copy predates
the `kafka/` submodule (added 10 Apr 2026, `cdb8ae6`), which is why it existed at
all: there was no submodule to read from when the project started. It became
redundant ten days later and was never removed. The two trees had diverged:

| | `generator/messages/` (built) | `kafka/` 4.2 (documented) |
|---|---|---|
| specs differing | **36 of 197** | — |
| …flag-line-only | 2 (`OffsetCommitRequest`, `OffsetFetchRequest`) | — |
| `latestVersionUnstable: true` | 5 specs | **1** (`InitProducerIdRequest`) |
| `ListOffsetsRequest` versions | `1-10` | `1-11` |

The build corpus is consistently **older**.

**Why it matters — reviewability.** A claim about spec content is unverifiable
unless it names its corpus, because the obvious place to check — `kafka/`, per
CLAUDE.md — is not what compiles. This produced three findings in the Phase 2 loop
alone (§9.8), the worst of which was a warning block whose cited evidence
contradicted it: a reviewer verifying it the documented way would have deleted the
warning and made the change it forbids. Until the duplication is resolved, every
spec-derived claim must name the file it came from.

**This item is self-contained.** It is *not* coupled to §9.2 — the divergence exists
against 4.2 today and stands on its own merits, whatever base is chosen later.

**Full classification of the 36 (done, so the refresh no longer needs the review):**

| Category | Count | In scope? |
|---|---|---|
| Comment/doc text only (`"Verison"` → `"Version"`, `"reqestor"` → `"requestor"`) | 16 | no code impact whatsoever |
| New `validVersions`, out of scope | 14 | 10 Share (KIP-932, `consumer-threading.md` §20), 2 Raft controller, 2 `WriteTxnMarkers` (broker-sent) |
| New `validVersions`, **in scope** | 2 | `ListOffsets{Request,Response}` v11 |
| New fields | 2 | `StreamsGroup{Describe,Heartbeat}Response` — Streams, out of scope |
| Flag line only | 2 | `OffsetCommitRequest`, `OffsetFetchRequest` |

`ListOffsets` v11 is the only in-scope functional delta, and its whole diff is the
version range plus a comment — KIP-1023 adds a sentinel *timestamp value*, not a
field. Version negotiation settles on v10, so a v10-capable client is compatible;
the gap is an unexposed capability, not an incompatibility.

**What the refresh actually cost.** Copying the 197 schemas changed 36 files
(+116/−67) and broke the build's **test** compilation — the library itself compiled
clean. Three schemas failed to parse:

    invalid type: string "true", expected a boolean

`WriteShareGroupStateRequest`, `ReadShareGroupStateSummaryResponse` and
`DescribeShareGroupOffsetsResponse` each gained a field writing
`"ignorable": "true"` **as a quoted string**. Java accepts it because Jackson
coerces a `"true"` string to a boolean; `serde` is strict. Across all 197 schemas
`ignorable` appears quoted 3 times and unquoted 177 times, so both forms are
legitimate — only `ignorable` is ever quoted, and `mapKey` / `zeroCopy` /
`latestVersionUnstable` never are.

Fixed by `deserialize_lenient_bool` in `generator/src/message/mod.rs`, applied to
**every** boolean schema property rather than only the one quoted today, because the
strict form fails silently (§9.10). Three tests cover it, including that a
non-boolean string is still rejected — leniency extends to `"true"` / `"false"`
only, so a typo stays a parse error instead of becoming `false`.

After the fix: 197 schemas byte-identical to the submodule, stub count back to the
8 intentional ones, `ListOffsets` v11 and the Share/Raft version bumps all generate,
`delivery_complete_count` present with its `-1` default. **2348 tests passing**,
format-check / lint / check-generated clean.

**The duplication itself remains** — this item refreshed the snapshot but did not
remove the second copy, so it can drift again. Tracked as **§9.11**.

`generator/test-messages/` has the same shape of gap on a smaller scale: 3 files
against Kafka's 4, the 3 shared ones identical, `SimpleRecordsMessage.json` absent.
Not refreshed here — adding a schema adds generated test types, which is a separate
change from syncing existing ones.

**No known live defect from the divergence** — audited by Critic 42's sixth pass
before the refresh, not merely assumed.
`latest_version_unstable()` has no caller outside the generated file. The only
production `latest_version_with_unstable(false)` sites are the five txn builders,
whose specs are flag-identical across corpora. The two other readers
(`is_version_enabled`, `to_api_version_internal`) are reached only from
`api_versions_response.rs`, and **every caller in the tree passes `true`**.
`NodeApiVersions` uses the unstable-inclusive `latest_version()`, faithful to Java's
`ApiVersionsResponse.toApiVersion`. For the Streams pair, `validVersions` is
identical in both corpora (highest = 0), so `latest_version()` returns 0 = Java;
only the uncalled `with_unstable(false)` would yield −1.

The `OffsetCommit` / `OffsetFetch` flag delta was therefore **latent**. It is now
**gone**: after the refresh only `InitProducerIdRequest` sets the flag, matching 4.2,
so the two accessors agree for `OFFSET_COMMIT` / `OFFSET_FETCH` and the §9.7 hazard
no longer exists. §9.7's warning block is now belt-and-braces rather than
load-bearing — the four "faithful group" builders must still keep
`latest_version()` to mirror Java's `super(...)`, but getting it wrong is no longer a
wire-visible regression.

### 9.10 A schema that fails to parse becomes a silent stub

**Status:** open. Found 2026-08-04 while refreshing §9.9.

`generator/src/lib.rs:45-54` catches a per-schema parse error, prints it with
`eprintln!`, writes a **stub** in its place, and continues:

    #[derive(Debug, Clone)]
    pub struct WriteShareGroupStateRequestData {
        // Fields will be generated when spec can be parsed
    }

`generate_messages` then returns `Ok(())`, so `build.rs`'s `panic!` arm never fires
and the build succeeds. Cargo hides build-script stderr unless the build fails, so
the only trace is a line — `Successfully generated 186 out of 197 message types` —
that requires `cargo build -vv` to see.

A stub has no fields, no `read`/`write`, and no version constants. Any code path
using that message type silently does nothing instead of speaking the protocol.

**Why this matters more than the bug it hid.** §9.9's quoted-boolean failure only
surfaced because `message_test.rs` happens to reference
`HIGHEST_SUPPORTED_VERSION` on those three types — a compile error in a *test*. Had
the affected schemas been ones no test names, the refresh would have reported
success while three message types quietly became empty shells.

**8 stubs exist today and are intentional:** `ControlledShutdown`, `LeaderAndIsr`,
`StopReplica`, `UpdateMetadata` (request + response each) declare
`"validVersions": "none"` — they were removed from the protocol in Kafka 4.0 — and
the parser reports "You must specify the version of the X structure". So parse
failure cannot simply be made fatal.

**Fix:** an explicit allowlist of known-unsupported schemas (those 8, with the
reason), and make any *other* parse failure fatal — `generate_messages` returns
`Err`, `build.rs` already panics on it. Loud by default, silent only where silence
was chosen deliberately.

### 9.11 Build from the submodule directly and delete `generator/messages/`

**Status:** open. The structural half of §9.9, which fixed the symptom (a stale
snapshot) but left the cause (two copies of the same 197 schemas).

**Change:** point `build.rs:44` — and the `generate_api_message_type` call on
`build.rs:56` — at `kafka/clients/src/main/resources/common/message/`, then delete
`generator/messages/`. Do the same for `generator/test-messages/` →
`kafka/clients/src/test/resources/common/message/`, which has the same problem at
smaller scale (3 files against Kafka's 4; `SimpleRecordsMessage.json` absent).

**Why this and not "remember to re-sync".** The copy drifted for the project's entire
history — 31 Mar to 4 Aug 2026, one commit, never revisited — and nobody noticed
until a review happened to check the same detail in both trees and get two different
answers. It then cost three findings across a six-pass review loop, the worst of
which was a warning block whose own cited evidence contradicted it. A process that
depends on remembering to copy files has already failed once here; removing the
second copy makes the failure structurally impossible rather than merely documented.

Nothing is lost: both trees hold the same 197 filenames and `generator/messages/`
contains nothing custom — verified byte-identical after §9.9's refresh.

**Cost:** the build gains a prerequisite. `cargo build` in a fresh clone without
`git submodule update --init --recursive` will fail because the schema directory is
empty. This is the deliberate trade — a missing submodule fails immediately and
legibly, whereas a stale copy builds successfully and produces subtly wrong wire
code. Prefer that failure to be a clear message rather than a `NotFound` from
`read_dir`: have `build.rs` check whether the directory exists or is empty and
`panic!` with the exact `git submodule` command to run.

**CI already satisfies the prerequisite** — checked, not assumed. Both pipelines run
`git submodule update --init --depth=1 kafka` (`.semaphore/semaphore.yml:49`,
`.semaphore/plan-approve.yml:19`), so no CI change is needed and the shallow checkout
is sufficient for reading schema files. The cost therefore falls only on a developer
building a fresh clone by hand.

**Publishing is the one real blocker, and it is not currently a constraint.** A
published crate cannot reference a path outside its own tree, so `cargo publish`
would need a vendored copy. `publish` is not set in `Cargo.toml` today and the crate
is not on crates.io, so this does not block the change — but it decides the design if
distribution is ever intended. In that case keep the copy and add a CI step that
fails when it differs from the submodule: that preserves "cannot drift silently"
without the build prerequisite, and is the better option under a publishing
requirement.

**Also audit** `cargo xtask check-generated` and the `message_generator` binary for
their own assumptions about the schema location before deleting anything.

**Verify:** fresh clone without submodules → build fails with the intended message;
with submodules → `cargo build`, full suite, `format-check`, `lint`,
`check-generated` all clean, and the stub count stays at the 8 intentional ones
(§9.10).

### 9.12 Two `records`-field defects, found by the missing test schema

**Status:** DONE — fixed 2026-08-04 in `f6d5fd7`.

`generator/test-messages/` held 3 of Kafka's 4 test schemas. Adding the fourth,
`SimpleRecordsMessage.json`, and translating its test (`RecordsSerdeTest.java`, which
DoD §3 requires) exposed two real defects in `records`-typed fields. Both were live
in `ProduceRequest`, `FetchResponse` and `ShareFetchResponse` — the client's busiest
message types.

(The schema needed `git add -f`: `.gitignore:4` blanket-ignores `*.json`, and the
three existing schemas are tracked only because they predate that rule. Worth knowing
before adding any future schema — a plain `git add` silently does nothing.)

**Defect 1 — wrong default.** `FieldSpec.fieldDefault` returns `"null"`
**unconditionally** for a `records` field (`FieldSpec.java:453-454`): a bare
`else if (type.isRecords()) return "null";` with no nullability or explicit-default
check, unlike the `isBytes()` branch immediately above it, which returns null only
when the spec says `"default": "null"`. Our generator shared one match arm for
`Bytes | Records` — as it does in roughly eight other places — and so gave `records`
the `bytes` rule. An unset record set encoded as a **zero-length** buffer where Java
encodes **null** (−1).

Note this is *not* covered by CLAUDE.md's "nullable string/bytes default to empty"
rule: that rule is about `string` and `bytes`, and `records` is a third case with the
opposite default. The rule is right; `records` simply isn't in its scope.

**Defect 2 — serialising mutated the message.** The write path emitted:

    if let Some(_nv) = self.record_set.take() {

justified in a comment as "zero-copy ownership transfer". `Writable::write_records`
does take `Bytes` by value, but `bytes::Bytes` is a **reference-counted handle** whose
`clone()` bumps a counter and copies no payload — so `.clone()` is equally zero-copy,
while `.take()` leaves `None` behind and **destroys the record set as a side effect of
writing it**. Java's `write` never modifies the message.

One cause, three observable symptoms — worth recording because none of them looks
like the others:

  - `size()` computed after a `write` disagreed with the bytes written (9 vs 90);
  - an explicitly-empty record set round-tripped back as null;
  - null and empty encoded **identically** on the second iteration of a version loop,
    the first having drained the field.

**Neither appears to have been hit in production.** The producer rebuilds
`ProduceRequestData` per send (`sender.rs:391-431`) rather than re-serialising, and a
*first* write is correct. They survived because nothing exercised a `records` field at
all — the test file that would have was the one missing from the corpus. A direct
vindication of DoD §3's "never skip a test present in the Java codebase": the gap in
the test corpus and the gap in the generator were the same gap.

**Test added:** `tests/common/message/records_serde_test.rs` — Java's three cases plus
one not in Java, asserting null and empty record sets encode differently at every
version and do not round-trip into each other. That fourth test is what pins both
fixes; either regression alone makes it fail.

### 9.13 Four unhandled type combinations emit a TODO into generated code

**Status:** open. Surveyed 2026-08-04.

Four `writeln!` sites in `generator/src/lib.rs` write a comment into the *generated
output* instead of working code, and generation continues normally. Same silent-failure
shape as §9.10, one level down: there the whole type becomes a stub, here a single
field's read or write is quietly missing.

| # | Site | Function | Fires for |
|---|---|---|---|
| 1 | `lib.rs:2303` | `generate_tagged_field_read` | a **tagged** field that is an array whose element is `Uint16`, `Uint32`, `String`, `Bytes`, `Records`, `Struct`, or a nested array (handles `Uuid`, `Bool`, `Int8/16/32/64`, `Float64`) |
| 2 | `lib.rs:2917` | `generate_tagged_field_write` | a tagged field of type `Uint16` or `Uint32` (every other variant has an arm) |
| 3 | `lib.rs:3797` | `generate_array_element_read_with_prefix` | reading a length-prefixed array whose element is `Bytes`, `Records`, or a nested array |
| 4 | `lib.rs:4269` | `generate_array_element_write` | writing an **array of arrays** — explicit `FieldType::Array(_)` arm, "Nested array not implemented" |

**None fires today.** Verified against the generated output for all 197 production
schemas and all 4 test schemas: zero occurrences of any of the four strings. So these
are unreached paths, not active defects.

**Why they still matter.** #1 and #3 are asymmetric — the write side of those
combinations is implemented while the read side is not, so data would go out and be
unreadable coming back, rather than failing at both ends. And the failure mode is the
one that cost this session twice: the build succeeds, and the symptom surfaces much
later as "this message type silently doesn't work".

Note that §9.12's two defects were in `records` handling, which is the same family as
#1 and #3 — so the neighbourhood is demonstrably not hypothetical.

**Fix:** implement the four combinations, or — if any is genuinely unreachable by
construction — make the fallback `panic!` with the field name and type, so the
generator refuses to emit code it cannot write correctly. Do NOT leave a fallback that
emits a comment: per CLAUDE.md §5 a TODO in generated output is unfinished work, and
per §9.10's reasoning silence is the wrong default. Pair this with §9.10, which is the
same principle applied to whole-schema failures.

### 9.14 DoD §3's byte-level wire tests do not exist above the varint layer

**Status:** open, and **project-wide** — not introduced by Milestone 11. Found
2026-08-04 while re-auditing Phase 2 against the DoD.

`definition-of-done.md` §3 requires:

> Wire protocol types have byte-level encoding tests against known vectors, not just
> round-trip tests — a consistently wrong encoding passes round-trips but is
> wire-incompatible with Java

**What exists.** Exactly one layer is covered: `tests/common/protocol/flexible_version_test.rs`
has 7 hand-derived byte assertions for varint encoding (`&[0x80, 0x01]`,
`&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]`, …) across its 11 tests. Those are genuine known
vectors and they pass.

**What does not exist.** Any assertion that a *whole message* encodes to specific
bytes:

| Scope | Tests | Whole-message byte vectors |
|---|---|---|
| `tests/common/message/` (4 files) | 58 | **0** |
| Phase 2's 10 txn wrappers | 91 | **0** |
| Every pre-existing request/response wrapper | — | **0** |

Above the primitive layer, correctness rests entirely on round-trips — precisely the
case DoD §3 names as insufficient, since our encoder and decoder agreeing with each
other says nothing about agreeing with Java.

**Why it has not bitten: real brokers have been doing this job empirically.** The
integration suite runs 91 tests against actual Kafka in Docker (produce, fetch,
consumer groups, commits, SASL, SSL). A broker rejects or misreads a wrong encoding,
so for every message type those tests exercise, live interoperability is *stronger*
evidence than a hand-derived vector. That is the real reason this gap has been
survivable, and it is why retrofitting ~190 message types is low urgency.

**Where that mitigation does not reach — the sharp edge.** Phase 2's five transaction
wrappers are **not exercised by anything**: no byte vector, and no integration test,
because they are unused until Phase 5 wires them into `TransactionManager`. Everywhere
else in the client one of the two routes applies. These are the one place where
neither does, so **if `AddPartitionsToTxn` encodes wrongly today, nothing in the repo
can detect it.** They are also the highest-value target for that reason, and the
cheapest — five types, not 190.

**Two documents already cite these tests as though they exist**, which is how the gap
stayed invisible:

  - `add_partitions_to_txn_request.rs` on `build_txn_topic_collection`: the sort exists
    "so the encoding is deterministic, which byte-level tests depend on";
  - `producer-transactions.md` §10, justifying the deterministic-sort rule by
    "`definition-of-done.md` §3 requires byte-level wire tests against known vectors".

The sorting work is correct and worth keeping — determinism is a precondition for the
tests — but both citations point at a test suite that was never written. **Six Critic
passes over Phase 2 never raised it, and neither did two Actor self-audits.**

**How to write them — the easy version is worthless.** Three approaches, only two
worth the effort:

  1. **Assert what our own encoder emits.** Self-referential: passes even when the
     encoding is wrong, which is the exact failure DoD §3 exists to catch. Do NOT do
     this. A test of this shape is worse than no test, because it reads as coverage.
  2. **Hand-derive from the wire format.** Real verification. Laborious per type and
     per version — flexible-version varints, compact vs. classic string/array lengths,
     tagged-field terminators, nullable sentinels.
  3. **Capture bytes from the Java client.** Strongest possible proof, and Kafka's
     source is already in-tree at `kafka/`. Needs a JVM and a small harness that
     serialises a fixed message per version and dumps the bytes; the Rust test then
     asserts against the captured fixture. Highest setup cost, lowest per-type cost,
     and it scales to all 197 types — which makes it the right answer if the
     project-wide gap is ever closed rather than just the Phase 2 slice.

**Sequencing:**

  - **Phase 2's five request types, before Phase 5** — approach 2 or 3. This is the
    only verification they will have, and Phase 5 starts depending on them.
  - **The 190 others** — their own piece of work, approach 3. Do not start it inside a
     transactions phase; a repo-wide test addition would make any regression ambiguous.
  - Responses rank below requests either way: a wrong request is misread by a live
    broker, whereas a response decoding error surfaces in our own round-trips.

### 9.15 `ABORTABLE_ERROR` is reachable without a `transactionalId`

**Status:** DONE — corrected in Phase 3; §2 and §Phase-3 amended in place.

§2 asserted "**4 of 9 states** … `ABORTABLE_ERROR` is unreachable without a
`transactionalId`", and §Phase-3 repeated it. Both are wrong.

`InitProducerIdHandler.handleResponse` (`TransactionManager.java:1524-1528`):

```java
} else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
        error == Errors.CLUSTER_AUTHORIZATION_FAILED) {
    log.info("Abortable authorization error: {}.  Transition the producer state to {}",
        error.message(), State.ABORTABLE_ERROR);
    lastError = error.exception();
    abortableError(error.exception());
```

Three facts make this reachable for a purely idempotent producer:

  1. The arm does **not** test `isTransactional()`, and neither does
     `abortableError` → `transitionToAbortableError` → `transitionTo`. There is
     no `ensureTransactional()` anywhere on the path.
  2. A **non-transactional** `InitProducerId` is answered
     `CLUSTER_AUTHORIZATION_FAILED` when the principal lacks `IdempotentWrite`
     on the cluster. That is the idempotent-producer authorization failure, not
     a transactional one.
  3. The transition is permitted: the manager is in `INITIALIZING` when the
     response arrives (set by `bumpIdempotentEpochAndResetIdIfNeeded`,
     Java 669), and `INITIALIZING` is one of the four sources the table accepts
     for `ABORTABLE_ERROR` (Java 178-180). That arm exists *for* this case —
     the other three sources are transactional.

**Why it matters rather than being a naming curiosity.** Once an idempotent
producer is in `ABORTABLE_ERROR`, `hasError()` is true, so `maybeFailWithError()`
throws and `maybeAddPartition` rejects every subsequent send. Omitting the state
would have made an authorization failure silently non-fatal *and* left sends
succeeding — the opposite of Java in both directions.

**What Java does next: it recovers to `UNINITIALIZED`.** On the following
`Sender.runOnce`, `:325` tests `hasAbortableError()` and calls
`shouldHandleAuthorizationError(lastError)` (`:351-360`):

```java
if (exception instanceof TransactionalIdAuthorizationException ||
                exception instanceof ClusterAuthorizationException) {
    transactionManager.failPendingRequests(new AuthenticationException(exception));
    maybeAbortBatches(exception);
    transactionManager.transitionToUninitialized(exception);
    return true;
}
```

For an idempotent producer that `instanceof` test is **always** satisfied: the
only entry to `ABORTABLE_ERROR` is the authorization arm above, whose `lastError`
is exactly one of the two exceptions it matches. So `runOnce` returns at `:326`,
`transitionToUninitialized` clears `lastError`, and the next iteration reaches
`bumpIdempotentEpochAndResetIdIfNeeded` at `:331` from `UNINITIALIZED` and
enqueues a fresh `InitProducerId`. That is why the table admits
`UNINITIALIZED ← ABORTABLE_ERROR` (Java 165), and the Java comment at
`Sender.java:348-350` states the intent: "transition the state to UNINITIALIZED
so that the user doesn't need to instantiate the producer again."

> **Correction (Critic 43 issue 2).** This paragraph previously claimed that a
> subsequent `bumpIdempotentEpochAndResetIdIfNeeded` attempts
> `ABORTABLE_ERROR → INITIALIZING`, is refused, and poisons to `FATAL_ERROR` on
> the Sender side. Java never does that on this path: `:325` returns before
> `:331` is reached. The claim asserted the opposite outcome — poison rather than
> recover — on the exact path this section exists to document, which is worse
> than no record at all (cf. §9.8, §9.14). The `FATAL_ERROR` outcome would only
> arise for an idempotent `ABORTABLE_ERROR` whose `lastError` is *not* an
> authorization exception, i.e. only through `Errors.TRANSACTION_ABORTABLE`
> (Java 1533), which a broker returns only for transactional requests.

**Consequence for the plan.** Phase 3 translated the three methods that *enter*
the state — `transitionToAbortableError` (530), `hasError` (522),
`hasAbortableError` (991) — and, after Critic 43 issue 1, the ones that *leave*
it or clean up around it:

| Java | Rust | was scheduled | now |
|---|---|---|---|
| `transitionToUninitialized` (756) | `transition_to_uninitialized` | Phase 5 | **Phase 3** |
| `failPendingRequests` (944) | `fail_pending_requests` | Phase 5 | **Phase 3** |
| `authenticationFailed` (939) | `authentication_failed` | *nowhere* | **Phase 3** |
| `close` (949) | `close` | *nowhere* | **Phase 3** |

The first two are removed from the Phase-5 list. All four are called from
`Sender` code that Phase 4 and Phase 6 translate, and all four are implementable
now — none needs Phase-5 state beyond the `pendingTransition` branch, which is
always null for an idempotent producer for the reason already recorded on
`transitionToFatalError`.

`authenticationFailed` was unscheduled in **every** phase, and is reachable
idempotently: `maybeSendAndPollTransactionalRequest` takes the
`coordinatorType == null` branch (`Sender.java:479-484`) and still calls
`awaitNodeReady` → `NetworkClientUtils.awaitReady`, which throws
`AuthenticationException`, caught at `Sender.java:336`.

`close` (949) was also unscheduled everywhere. It is not reachable *from*
`ABORTABLE_ERROR` — it is the `forceClose` shutdown path at
`Sender.java:287-293`, i.e. Phase 6 — but its call site is reachable for a purely
idempotent producer (`transactionManager != null` holds at `:290`). **On the
idempotent path it has no observable effect whatever**, and the scope decision
rests on that being acceptable, not on a payoff:

  - unscheduled in every phase — the same plan gap that dropped
    `authenticationFailed`;
  - twelve lines, with no Phase-5 dependency beyond the always-null
    `pendingTransition` branch;
  - behavioural payoff in **Phase 6**, on the transactional path.

Java names that payoff two lines above the call (`Sender.java:288-289`): "fail all
the incomplete transactional requests and batches and *wake up the threads
waiting on the futures*" — i.e. the threads blocked in
`result.await(maxBlockTimeMs, ..)` (`KafkaProducer.java:654`), which is a path
that only exists once `initializeTransactions` does.

Nothing else in `close` is observable idempotently. The `FATAL_ERROR` it writes is
never read: `close()` is the Sender task's **terminal act**. Its sole call site
(`Sender.java:292`, the only one in the whole client) sits inside
`if (forceClose)` at `:287`, after all three `run()` loops (`:245`, `:258`,
`:267`), followed only by `accumulator.abortIncompleteBatches()` (`:295`) and
`client.close()` (`:298`). Both post-shutdown loops are `!forceClose`-guarded, so
once `forceClose` is set **no `runOnce` executes at all**. (The transition happens
**inside** the loop, so an empty queue means no transition at all — Java's
behaviour, preserved.)

> **Corrections (Critic 43 issues 5 and 7).** `close`'s justification was wrong
> twice, in two different ways, and both are recorded rather than silently edited
> — a scope expansion defended by a mechanism that does not exist cannot be
> reviewed, and this one was the Actor's own initiative.
>
> *Issue 5.* The first revision claimed that omitting `close` would leave a
> pending `InitProducerId`'s `TransactionalRequestResult` never completed — a
> hanging future under CLAUDE.md §5. **That mechanism does not exist on the
> idempotent path.** The handler is built inside
> `bumpIdempotentEpochAndResetIdIfNeeded` (Java 663-676), which returns `void` and
> never lets the result escape; the only method that hands a
> `TransactionalRequestResult` to a caller is `initializeTransactions` via
> `handleCachedTransactionRequestResult`, whose first statement is
> `ensureTransactional()` (Java 1266). On the Rust side `await_result` /
> `await_result_timeout` have no production call site at all —
> `TxnRequestHandler::result()` is read only from tests and the `#[cfg(test)]`
> door. Correctly bounded to Phase 6 above.
>
> *Issue 7.* The second revision then claimed the `FATAL_ERROR` transition "stops
> `Sender.runOnce` at `:318` before `bumpIdempotentEpochAndResetIdIfNeeded` can
> enqueue one". **There is no `runOnce` after `close()`** — see the loop structure
> above. Worse, the thing that transition was credited with preventing is already
> prevented by the `!forceClose` guards at `:258` and `:267`, with or without
> `close`. This claim was refuted by the twenty lines of `Sender.run` immediately
> around the call site the same sentence cited.
>
> **The instructive part is the pattern, not either claim.** Issue 5's fix
> identified the true mechanism (waking blocked awaits), correctly retracted it
> for the idempotent path, and correctly bounded it to Phase 6 — and then reached
> for a *different* present-tense idempotent mechanism instead of concluding there
> is none. The pull toward finding some payoff in the current phase produced both
> wrong answers. "No behavioural payoff until Phase 6, and that is fine because
> the method was unscheduled, is twelve lines, and needs no Phase-5 state" was
> always the sufficient and true justification.

All nine `State` variants are declared (see §Phase-3), so the arithmetic
"4 of 9" no longer appears in the code either way.

**Regression evidence.** Three mutations, each caught:

  - deleting `source == Self::Initializing` from the `AbortableError` arm of the
    Rust table fails
    `test_cluster_authorization_failure_moves_an_idempotent_producer_to_abortable_error`
    — the check that the arm is load-bearing on the idempotent path rather than
    only the transactional one;
  - making `transition_to_uninitialized` a no-op fails
    `test_idempotent_producer_recovers_from_abortable_error_to_uninitialized`;
  - removing the `Sender.java:325` guard from the test harness makes the same
    test fail at `bump_idempotent_epoch_and_reset_id_if_needed` with an
    `ABORTABLE_ERROR → INITIALIZING` rejection — a direct demonstration that the
    guard, not the table, is what keeps Java off the path the stricken paragraph
    described.

**Lesson, same shape as §9.8's, in two parts.**

  1. The original wrong claim was derived from the *guards* on the
     transaction-only entry points (`ensureTransactional`,
     `if (isTransactional())`), which do fence the state machine cleanly — and
     then generalised to the response handlers, which are not fenced the same
     way. A reachability claim has to be checked against every writer of the
     state, not only the entry points that look like they own it.
  2. The correction itself was then applied only half way: to the writers that
     *enter* the newly-reachable state, not the one that *leaves* it. A state a
     client can enter and not exit is worse than a state it never reaches — the
     translated state machine had no exit from `ABORTABLE_ERROR`, so from Phase 4
     an authorization failure would have rejected every subsequent send forever.
     Enumerating exit paths is now a suggested `definition-of-done.md` clause,
     recorded in `COMMENTS.DONE.43.md` for the `agent-roles.md` §2 process.
  3. Three rounds were then spent on one justification (`close`'s), because each
     correction reached for a *different* present-tense behavioural payoff instead
     of concluding there is none. Both replacements were false; the surviving
     reason — unscheduled, small, no later-phase dependency, payoff in Phase 6 —
     had been sufficient all along. A record that overstates *when* an effect
     materialises is the same class of defect as one that overstates *whether* it
     exists, and it is harder to spot because the mechanism is real, just not yet.

### 9.16 Critic review of Phase 3

**Status:** DONE — loop **closed 2026-08-04 on a clean fourth pass** (zero findings).
Archived at `design/history/Milestone-11/Phase-3/COMMENTS.DONE.43.md`.

| Pass | Findings | Where the defect was | Fix |
|---|---|---|---|
| 1 | 3 | **1 real code defect** + 2 records | `1bc8a8c` |
| 2 | 3 | records written by the pass-1 fix | `3131600` |
| 3 | 1 | third wrong justification for `close` | `d982e13`, `1c29bbc` |
| 4 | **0** | — | closes the loop |

All seven findings were real and conceded. The Critic's only errors were two citation
slips it introduced and corrected itself, plus one alternative resolution it withdrew.

**Phase 3 was run twice.** The first attempt had the coordinator acting as its own
Actor; those commits were dropped (`git reset --hard` to `ca75e95`) and the phase redone
through the `agent-roles.md` Actor/Critic model. Pass 1 finding 1 is why that mattered:

Both the coordinator *and* Actor 43 independently concluded — correctly, and against this
plan's stated count of four — that `ABORTABLE_ERROR` is reachable idempotently. Both then
added the methods that **enter** it. **Neither noticed nothing leaves it.** Java always
recovers via `Sender.java:325` → `shouldHandleAuthorizationError` →
`transitionToUninitialized` (`:354`), which is why the table admits
`UNINITIALIZED ← ABORTABLE_ERROR`. Shipped as-is, one authorization failure would have
wedged the producer: `maybe_add_partition` rejecting every send with no path out.

Two independent agents produced the same *half* of a finding. More passes over either
version would not have found the other half — only a reviewer attacking the conclusion
rather than extending it. Consistent with §9.8, where six passes missed a DoD clause
because everyone shared one frame; the remedy there was reading a different document,
here it is a different author.

**Carried to Phase 4:**

  - An all-green suite proves nothing about an exit path nobody tested — the entry-only
    version passed 26 tests and a clean fidelity sweep.
  - A plan scheduling an exit method into a later phase than its entry is itself the
    smell. This plan put `transition_to_uninitialized` / `fail_pending_requests` in
    Phase 5 while §Phase-4's table already claimed their call site.
  - A test harness needs mutation-pinning as much as production code. Issue 6's wrong
    control-flow model was fully green, in an artifact §Phase-4 names as its reference.
    Now pinned in both directions.

### 9.17 `KafkaCluster` leaks broker containers when a run aborts

**Status:** open. Found by Actor 43 while running the gate repeatedly; diagnosis
confirmed by Critic 43 and by direct inspection. **Not a Phase 3 defect** — outside its
diff entirely.

Containers are created inside `tokio::spawn`ed tasks at
`tests/common/kafka_cluster.rs:361-375` and only become owned by
`KafkaCluster::_containers` (`:285`) after the collect loop at `:379-382`. There is no
`impl Drop for KafkaCluster`. An abort between the first container starting and the
struct being built — a panic at `handle.await.expect(..)` (`:381`), or the whole future
dropped on a timeout — detaches the surviving tasks, leaving their containers running.

Because host ports are pre-reserved at `:333`, a survivor collides **deterministically**
on a later run, and the failure surfaces inside `with_mapped_port` as a *test* failure:
`failed to bind host port ... address already in use`. So it mimics a code regression.
It cost one gate run in this phase, and four `apache/kafka:4.2.0` containers were found
up 2-3 hours holding ports.

**Fix:** register teardown as each container starts rather than after all of them do, so
an abort mid-startup still reclaims what already exists.

**Meanwhile:** `docker ps` before trusting an integration failure that mentions port
binding, and `docker rm -f` any orphans.

### 9.18 Split-on-`MESSAGE_TOO_LARGE` panicked: `build()` was not idempotent

**Status:** **FIXED** (loop 50, branch `investigate/split-panic-and-version-gate`).
Found in Phase 4 while translating
`SenderTest.testTooLargeBatchesAreSafelyRemoved` (Java 3004-3036). **Not a Phase 4
defect** — it predated the transaction manager and affected idempotent and
non-idempotent producers alike.

`Sender.completeBatch` splits and re-enqueues a batch when the broker answers
`MESSAGE_TOO_LARGE` (`Sender.java:674-688`). In Rust that path panicked with
`build() called but no records built` (`memory_records_builder.rs:298`).

**Cause.** Java's `MemoryRecordsBuilder.build()` is **idempotent**: it memoises into
`builtRecords`, `close()` returns early once that field is set
(`MemoryRecordsBuilder.java:365-366`), and nothing but `reopenAndRewriteProducerState`
clears it — so `build()` may be called any number of times and returns the same
`MemoryRecords` view (`:238-244`). `ProducerBatch.records()` is a bare
`recordsBuilder.build()` (`ProducerBatch.java:483-485`), which is exactly why Java can
call it once to serialise the produce request and again from `ProducerBatch.split` →
`validateAndGetRecordBatch` (`:334`).

Rust had grown a second accessor, `MemoryRecordsBuilder::take_built_records()`, which
*moved* the memoised value out, and `ProducerBatch::records()` used it. By the time
`split()` called `build()` the field was `None` and the `expect` fired.

Reachability: `completeBatch`'s split arm requires
`recordCount > 1 && !batch.isDone() && (magic >= v2 || isCompressed())`. The existing
`test_expired_batch_does_not_split_on_message_too_large_error` passed only because it
expires the batch first, taking the `!isDone()` branch and skipping the split. No
test covered the live path, which is why this went unnoticed.

**Two claims in the earlier revision of this section were wrong**, and are corrected
above rather than silently dropped:

  - *"That move is deliberate; it is what makes the send path zero-copy under
    CLAUDE.md §12."* It made nothing zero-copy. `close()` already copies the finished
    batch out of the pooled buffer in `take_batch_data`
    (`bytes::Bytes::from(self.buffer[initial_position..].to_vec())`), and
    `MemoryRecords` wraps a refcounted `bytes::Bytes`, so the `build()` clone the move
    was avoiding is an O(1) refcount bump. `take_built_records` was written in
    c128ae16, when `MemoryRecords` still held a `Vec<u8>` and the clone really was a
    byte copy; 55b565e8 made it `Bytes` and nobody revisited the special case. By the
    time this section was written the move was pure cost — its own re-derivation branch
    re-ran `take_batch_data`, copying the whole batch a second time.
  - *"the batch's bytes are already gone"* (the old section title). They were never
    gone: `take_batch_data` copies **out of** `self.buffer` and does not clear it, and
    the `Bytes` handed to the request is refcounted, so two independent recoveries were
    available. The builder had simply dropped its own handle.

**Fix.** Delete `take_built_records` and its `built_size` shadow field; point
`ProducerBatch::records()` at `build()`. `estimated_size_in_bytes()` collapses back to
Java's exact two-arm form. No production code outside those two files changed.

**Why the `built_size` field could go, which is the part the fix commit understated**
(raised by Critic 50). Java has no `closed` field: `isClosed()` *is*
`builtRecords != null` (`MemoryRecordsBuilder.java:885-887`). Rust carries a separate
`closed` flag, and `take_built_records` made the two predicates disagree — it left
`closed == true` with `built_records == None`. `built_size` existed only to keep
`estimated_size_in_bytes()` answering correctly in that impossible-in-Java state. With
the accessor gone, `closed ⇔ built_records.is_some()` holds at every point (`close()`
sets both, `reopen_and_rewrite_producer_state` clears both), so the Rust and Java
predicates are the same predicate again and the shadow field has nothing to shadow.
Deleting it is a consequence of the invariant, not an independent tidy-up.

**DoD §10, measured with `AllocTrackingGuard`** (`producer_batch.rs`
`test_records_allocations_do_not_scale_with_the_record_count`), not argued:

| | `close()` | `records()` #1 | `records()` #2 |
|---|---|---|---|
| before | 2 | 0 | 1 — *a full re-copy of the batch* |
| after | 2 | 1 — *a 3-word `bytes::Shared`* | 0 |

The one extra allocation is the `bytes` promotion on a `Bytes::from(Vec)`'s first
clone. It is per **batch**, never per record — the test pins that by comparing a
1-record batch against a 64-record batch — and it buys the removal of a whole-batch
`memcpy` from the split path. `Bytes::from_owner` would move the same allocation into
`close()` for an identical total, so there is no cheaper shape while the bytes must
stay readable after the send.

**The reproducer was itself unfaithful, and that was not noticed until the fix.**
`sender.rs`'s `test_too_large_batches_are_safely_removed` was built on
`SenderTestContext::idempotent()` where Java builds a *transactional* manager with
`transactional.id = "testSplitBatchAndSend"` (Java 3006); it stopped after the split
instead of driving the retry to completion; it omitted Java's closing
`time.sleep(2000)` + `runOnce()`; and it asserted `deque_size == 2` ("one sub-batch
per record"), an invention — `splitAndReenqueue` targets `this.batchSize`
(`RecordAccumulator.java:517`), 16 KiB here, so both small records land in a single
sub-batch and Java asserts no count at all. It is now a line-by-line translation and
no longer `#[ignore]`d. **Lesson:** a test parked as a reproducer stops being read as
a translation. When it is un-parked, diff it against its Java source first — the
`#[ignore]` reason had been re-verified twice (Phases 6 and 8) by *running* it, which
proves the panic but says nothing about whether the assertions were right.

### 9.19 `SenderTest` methods blocked on missing surface

**Status:** open, with **one** blocked entry. The count has moved four times: three at
Phase 4, five after Phase 6, four after Phase 8 resolved one of them, and one now that
loop 50 fixed §9.18 and translated the three entries it blocked. The injected clock is
the only remaining cause.

Phase 6 built the end-to-end harness the transactional group needed and translated 4 of
its 15 (`testTransactionalRequestsSentOnShutdown`,
`testIncompleteTransactionAbortOnShutdown`,
`testForceShutdownWithIncompleteTransaction`,
`testTransactionAbortedExceptionOnAbortWithoutError` — the ones whose subject is
`Sender::run`'s transactional tail). It handed **11 to Phase 8**, with a per-method reason
each in the `sender.rs` accounting block.

**Phase 8 outcome: 10 of the 11 translated, 1 still blocked.** Of the two that had cited
named missing surface, one really was blocked and one was not:

  - `testTransactionalSplitBatchAndSend` (2385) — was **still blocked** on §9.18's split
    panic, the same gap as `testIdempotentSplitBatchAndSend`. Re-verified in Phase 8 by
    running the reproducer rather than by re-reading this note:
    `test_too_large_batches_are_safely_removed --ignored` still panicked at
    `memory_records_builder.rs:298`. **Translated in loop 50** once that was fixed, as
    `test_transactional_split_batch_and_send`.
  - `testSenderShouldCloseWhenTransactionManagerInErrorState` (3399) — **translated in
    Phase 8; it was never really blocked.** This entry used to say it needed "either a
    `#[cfg(test)]` hook that fails `begin_abort` on demand or a forced state where the real
    machine behaves that way". The second route already existed *and was already exercised
    by a test in `sender.rs` under a Rust-only name*: an idempotent producer left in
    `ABORTABLE_ERROR` satisfies both of Java's stubs, because `hasOngoingTransaction()` is
    true in that state (`TransactionManager.java:1012`) and `beginAbort()`'s
    `ensureTransactional()` guard rejects it. All that was missing was the Java name and
    Java's `verify(transactionManager, times(1)).close()`, for which
    `TransactionManager::close_call_count` is now `#[cfg(test)]`-gated. The lesson is the one
    Phase 8 recorded: a "blocked on missing surface" note is a claim with a shelf life, and
    the cheapest way to test it is to look for the surface rather than to re-read the note.

Of the four entries that were blocked across both groups, three were on §9.18 and are now
translated; the one that remains,
`testSenderShouldRetryWithBackoffOnRetriableError`, was never on §9.18.

One further Phase-8 entry is translated but `#[ignore]`d, and is **not** counted as blocked
because its body is complete and the assertion that fails is a production assertion:
`testTransactionalUnknownProducerHandlingWhenRetentionLimitReached`, the reproducer for
§9.25. And `testReceiveFailedBatchTwiceWithTransactions` is translated by a different
mechanism from Java's, recorded at §9.28.

The `sender.rs` accounting block carries the derivation showing no entry in the group
names `commitTransaction` / `abortTransaction`, which is what made the original
"owed, not blocked" reclassification checkable rather than asserted.

Raised by Critic 44 issue 4, which rejected Phase 4's block deferral of `SenderTest`
methods — correctly, since it named Phase 8 as the owner while §Phase-8's own scope
covers `TransactionManagerTest` only, and one of the deferred tests
(`testCancelInFlightRequestAfterFatalError`) was the test that would have caught the
buffer-pool leak of issue 2.

The per-method accounting lives in a comment block at the end of
`src/producer/internals/sender.rs`, which is the authoritative list and carries the two
shell commands that reproduce both the scope set and the completeness claim. In summary:
**52** `SenderTest` methods reference a `TransactionManager`, of which **33** are
translated, **18** are transactional (Phases 5 and 6), and **3** are blocked below; the
lists carry 2 further entries that are outside the 52 and are marked as such, so
33 + 18 + 3 = 54 entries and 54 − 2 = 52. (An earlier revision wrote "32 are translated"
and "2 are blocked" against that same arithmetic — Critic 48 issue 6. The numbers here are
also Phase-4-era and have since moved; the `sender.rs` accounting block is authoritative
and now records 53 in-scope methods over 55 entries.) This section records only what is
left and why.

**Still blocked on named missing surface (1):**

  - `testSenderShouldRetryWithBackoffOnRetriableError` (Java 3104) asserts the clock
    advances by exactly `RETRY_BACKOFF_MS` between retries. The `Sender`'s clock is an
    injected `Arc<dyn Fn() -> i64>` with no `sleep`, so `sleep_ms` uses
    `tokio::time::sleep` and cannot move a test's `MockTime`. Needs Java's `Time`
    interface threaded through `Sender` — a producer-wide constructor change that
    belongs with the Phase-6 review of `maybeSendAndPollTransactionalRequest`'s two
    sleeps (rules §4). **Untouched by loop 50**, which changed nothing about the clock.

**Unblocked by the §9.18 fix and translated in loop 50 (3):**

  - `testIdempotentSplitBatchAndSend` (2372) → `test_idempotent_split_batch_and_send`.
    It and its transactional twin share Java's driver (2406-2496), translated as
    `drive_split_batch_and_send`.
  - `testTransactionalSplitBatchAndSend` (2385) →
    `test_transactional_split_batch_and_send`.
  - `testNoBufferReuseWhenBatchExpires` (3605) →
    `test_no_buffer_reuse_when_batch_expires`. This one uses no transaction manager, so
    it is outside the 52; it was listed with the blocked group because the same §9.18 gap
    blocked it. **Half of its stated blockage was a false claim and is corrected here:**
    "`BufferPool` does accounting only and does not hand back the same backing array" is
    wrong — `BufferPool` keeps a `free: VecDeque<Vec<u8>>` free list
    (`buffer_pool.rs:58`) that `allocate` pops from when
    `size == poolable_size` (`:166-172`), returning the same allocation. What genuinely
    does not translate is Java's `batch.records().buffer().array()`: Java's
    `MemoryRecords` is a slice *view* of the pooled `ByteBuffer`, while
    `take_batch_data` copies the finished batch out of the pooled `Vec` into a fresh
    `Bytes` (a documented `bytes` 1.x deviation). So both of Java's identity assertions
    — the `assertSame` precondition and the closing `assertNotSame` — are made against
    the pool's free list instead, which is *also* the sounder choice: `assertNotSame` on
    a freshly allocated buffer is not a valid test in Rust, because the expired batch is
    dropped and the system allocator may legitimately hand the same address back. The
    derivation is in the test's doc comment.

**Reclassifications.** Two methods moved on close reading, and Critic 44 issue 7
corrected which was which:

  - `testDoNotPollWhenNoRequestSent` (2991) moved **out** of the idempotence list into
    the transactional group: it calls `doInitTransactions`. An earlier revision of this
    section credited `testUnresolvedSequencesAreNotFatal` with the move, which was
    wrong — that method was in the transactional group from the start (its manager is
    built with a transactional id at `SenderTest.java:1537`).
  - `testProducerBatchRetriesWhenPartitionLeaderChanges` (3308) is outside the 52
    entirely: it builds both the accumulator and the `Sender` with
    `transactionManager = null` (Java 3321 and 3324), so it is neither idempotent nor
    transactional. It was translated anyway rather than argued out of scope, being the
    only end-to-end cover for the leader-change backoff skip.

**Lesson recorded.** Both issues 6 and 7 were failures of an artifact that asserted its
own completeness in prose: the hand-assembled list lost
`testEpochBumpOnOutOfOrderSequenceForNextBatchWhenBatchInFlightFails` (1105) while
claiming nothing was owed, and the counts written beside the lists drifted from them.
The accounting block now states the criterion, gives the commands that derive the set
and diff it against the groups, and derives every count from the lists.


### 9.20 Critic review of Phase 4

**Status:** DONE — loop **closed 2026-08-05 on a clean fourth pass** (zero findings).
Archived at `design/history/Milestone-11/Phase-4/COMMENTS.DONE.44.md`.

| Pass | Findings | Where the defects were | Fix |
|---|---|---|---|
| 1 | 5 | **2 real bugs** (per-send double allocation; buffer-pool leak), response-loop mismatch, rejected 33-test deferral, stale doc | `c1c6e60` `a2ca2f9` `13e6201` + test commits |
| 2 | 4 | 1 lost test, 1 behaviour mismatch (expiry unmute), 2 record classes | `86a2b6c` |
| 3 | 1 | one citation header crediting the sibling test's range | `374ca12` |
| 4 | **0** | — | closes the loop |

All ten findings real and conceded; no false positives. The full pass-by-pass record,
the two bugs' anatomy, the deferral rejection (its covering evidence: deferred item 23
was exactly the test that would have caught the buffer leak), and the lessons are in
the archive header. Headline lessons:

  - An allocation audit must cover the public entry point, not only the inner hot
    loop — the double allocation sat between the Actor's sound drain audit and the
    send API.
  - A "deallocated later" claim needs the holder named; if no structure keeps the
    object reachable until "later", the claim is a leak.
  - A completeness claim over a list needs its mechanical check shipped alongside —
    the accounting lost one entry while its totals still reconciled, and the rebuilt
    self-verifying block then caught the Actor's own audit-regex blind spot.
  - Third invented-Java-mechanism instance this milestone (`client.close()` "runs
    completion callbacks" — it uses `DISCARD_NO_NOTIFY`, `Selector.java:96`). The
    honest record is "Java quietly abandons these; our explicit release is a
    deliberate improvement", and it now says so.

Verified at closure: `make verify-sandbox` exit 0 over `374ca12`; `producer_perf_test`
p99 13-15 ms across six serial runs against the 70 ms budget (no send-path latency
regression); all eleven DoD clauses pass.

### 9.21 Critic review of Phase 5 (5a + 5b)

**Status:** DONE — both halves **closed 2026-08-05 on clean passes** (5a: 7→2→0,
5b: 4→1→0). Archived at `design/history/Milestone-11/Phase-5/COMMENTS.DONE.45.md`.

| Half | Findings | Character | Fixes |
|---|---|---|---|
| 5a pass 1 | 7 | records/accounting (5) + test fidelity (2); §2 deviation adjudicated in the Actor's favour | `65aa5a9` `d56be1a` `ffbd1ad` |
| 5a pass 2 | 2 | records | `a5918b3` |
| 5a pass 3 | **0** | closes 5a | — |
| 5b pass 1 | 4 | **1 behavioural** (`begin_abort` hardcoded `Caller::App` — rules §1's named anti-pattern, wrong for its only live caller) + 3 records | `cc2bd48` `9b73389` |
| 5b pass 2 | 1 | a test justification citing an impossible reachability | `bbbf0ee` |
| 5b pass 3 | **0** | closes 5b and Phase 5 | — |

All 14 findings real and conceded; no false positives. Production code clean after
each half's first pass — the defect surface has moved into the self-auditing records,
and the archive header lists the countermeasures now standard in both accounting
blocks (byte-identical regeneration, extraction guards, no bare numbers,
error-direction proofs, justifications checked as claims).

**Two standing rule-update suggestions** from this loop await the process:
amend rules §2 / PLAN §6.5 to reclassify `coordinatorSupportsBumpingEpoch` as shared
(the Critic proved every Java read holds the monitor while the sole write does not),
and add a DoD §3 clause requiring shipped verification commands to be executed once
with real output pasted (three instances this milestone of a check that did not do
what its prose claimed).

Handed forward: 47 `TransactionManagerTest` methods to Phase 8 (join-proven to need
accumulator/Sender surface), the transactional `SenderTest` group to Phase 6
(rationale-expired evidence), `prepare_transaction`'s public surface to Phase 6.

**Two of those three clauses were amended after Phase 6 (Critic 46 issues 2 and 5);
recorded here because §9.21 is the natural entry point for "what did Phase 5 owe Phase
6?" and a refused hand-forward needs its record at *every* site, not just at §Phase-6:**

  - **What Phase 5 handed forward was 15, not 18** — the sentence originally said 18,
    which was the pre-Phase-5a count. `18 = 3 + 15`, and the block's own pasted
    derivation at `c59c09d:7547-7549` says so: it covers "the eighteen entries **this
    group and the 5a group above** cover", with the three 5a ones printing `-`. The
    header already read `TRANSACTIONAL (15)` at `c59c09d`, i.e. before Phase 6 touched
    it, so the 15 was Phase 5b's own reclassification.

    That is a statement about the **hand-forward**, and is now scoped as one, because
    Phase 6 went on to change the group's membership as well as its disposition: fix 1
    for Critic 46 pass 1 added `senderThreadShouldNotGetStuckWhenThrottledAndAddingPartitionsToTxn`,
    which the old prefix-keyed scope program could not see. An earlier revision of this
    bullet claimed "disposition only … with an empty membership diff" — true when
    written against pass 1, and falsified by fix 1 **in the same commit** (Critic 46
    pass 2 issue 2). The group's current size and split live in the accounting block at
    the end of `src/producer/internals/sender.rs`, which is authoritative; this bullet
    deliberately no longer restates them, so it cannot drift again.
  - **`prepare_transaction` was refused, with reason, and the refusal stands.**
    `KafkaProducer.java` in 4.2 has no such method — only `TransactionManager` does — so
    there was no public surface to hand forward. See §10.9 deviation 2 for the evidence,
    and §Phase-6's struck-through spec line for the correction at the point of
    specification. Critic 46 confirmed it independently, and the metrics hit is dead
    KIP-939 scaffolding — but **only in the corrected form recorded at §10.9
    deviation 2**, which is the authority: `KafkaProducerMetrics.java:80` creates a
    `prepareTxnSensor` and `:124` *does* declare `recordPrepareTxn`, which has **zero
    callers tree-wide** (one occurrence, its own declaration) against `recordInit`'s
    three. The 4.2 producer-side 2PC surface is `throwIfInPreparedState` (`:968-976`),
    which Phase 6 translated at all of its Java call sites plus the zero-copy FFI
    `send` that duplicates `doSend`'s guards.

    An earlier revision of *this* bullet asserted, in Phase 6's own voice, that the
    class has "no `recordPrepareTxn` method" — the false supporting fact §10.9
    deviation 2 refutes, **written into this section by the same commit that wrote the
    refutation** (Critic 46 pass 2 issue 1). Recorded because the lesson is not the
    fact but the *sweep*: refusing to propagate a bad claim into the section where the
    dispute was argued, while restating it in the section a reader hits first, is no
    refusal at all. A claim being corrected has to be corrected everywhere it appears,
    which is checkable —
    `grep -n recordPrepareTxn design/history/Milestone-11/PLAN.md` must not return two
    sections that disagree.

### 9.22 Critic review of Phase 6

**Status:** DONE — **closed 2026-08-05 on a clean fifth pass** (7→4→1→1→0).
Archived at `design/history/Milestone-11/Phase-6/COMMENTS.DONE.46.md`.

Thirteen findings, all real, **none behavioural in production code** — the third
consecutive phase with the translation right on arrival. Three corrections ran
*against* the review chain and were upheld: the Actor refused PLAN's
`prepare_transaction` instruction (no such method in Java 4.2; the §Phase-6 spec
carries the strikethrough and §10.9 deviation 2 the evidence), the Actor disputed a
Critic supporting fact (`recordPrepareTxn` exists — zero callers is the correct
fact), and the Manager's 18-vs-15 alarm was its own premise error.

Real fixes beyond translation: `close(timeout)`'s lost join (CLAUDE.md §9.4),
`PendingRequests` → `Arc<Mutex<..>>` (second rules-§2 correction; Java's own access
races with no happens-before), the tokio timer-starvation hazard (filed for
CLAUDE.md §9.6 via process), and `MockClient::advance_time_during_poll`.

Lesson minted: a paragraph documenting an escape shape is a ready-made adversarial
input for any checker in the same file; and complementary sweep blind spots
(start-only vs paren-requiring) are the case for diffing implementations, not just
results.

Standing rule-update suggestions accumulated for the process: rules §2 on
`pendingRequests` and on `coordinatorSupportsBumpingEpoch` (§10.9, §9.21), the DoD
§3 executed-commands clause, and CLAUDE.md §9.6's timer-starvation pitfall.

### 9.23 `MockProducer`'s telemetry and metrics surface is untranslated, because the trait is

**Status:** open. Found in Phase 7 by its method accounting, which is in
`src/producer/mock_producer.rs` above the struct. **Not a Phase 7 defect** — PLAN
§Phase-7's scope is the transactional surface, and none of this is transactional.

`MockProducer.java` declares 40 class-level methods; 30 have a Rust `fn` and 10 do
not. Nine of the ten are one feature pair:

  - `clientInstanceId` (382), `metrics` (400), `registerMetricForSubscription`
    (589), `unregisterMetricFromSubscription` (594) — these are `Producer`
    *interface* methods, and the Rust `Producer` trait in `producer_trait.rs` does
    not declare them. So the gap is in the trait, not in the mock: an impl would
    have nothing to override. `KafkaProducer` has the same hole.
  - `disableTelemetry` (366), `injectTimeoutException` (373), `setClientInstanceId`
    (377), `setMockMetrics` (407), `addedMetrics` (584) — mock-only knobs that exist
    to drive those four. They only make sense once the four land.

The tenth, `partition` (526), is different and is **not** tracked here: it needs
`Partitioner` plus the two `Serializer`s, and the Rust producer taking
pre-serialized bytes is a deliberate crate-wide design decision, stated at
`MockProducer::new`. Its only cost to test parity is that `testPartitioner`
(`MockProducerTest.java:86`) is translated in adapted form, which its rustdoc says.

**Cost today: none to Phase 7's test parity.** Each of the nine appears **zero**
times in `MockProducerTest.java` — the check is in the method accounting block — so
no Java test is blocked. `KafkaProducerTest`'s telemetry methods are the ones that
would need them, and they are not in Phases 1-8.

**Fix:** add the four `Producer`-trait methods (`KafkaProducer` first, since it owns
the real `ClientTelemetryReporter`), then the five mock knobs, then translate the
`KafkaProducerTest` telemetry group. Sizeable, and orthogonal to transactions.

### 9.24 Critic review of Phase 7

**Status:** DONE — **closed 2026-08-06 on a clean second pass** (4→0), the
milestone's fastest. Archived at
`design/history/Milestone-11/Phase-7/COMMENTS.DONE.47.md`.

Four findings, all records; zero behavioural defects in production code — the fourth
consecutive clean-on-arrival phase. All 22 entry points verified
statement-by-statement against `MockProducer.java` in pass 1.

Delivered: the mock's full transactional surface with Java's staging semantics, 43
of 44 named tests (the 44th proven not-applicable — its NPE fires before the mock is
entered), two pre-existing `Completion::complete` defects fixed (missing -1-filled
error metadata; completion-before-callback), a published C doc contract corrected
per-path after the fix falsified it, and a known reentrancy divergence honestly
recorded in §10.10 (callback re-entering the mock deadlocks where Java's reentrant
monitor allows it).

Lessons: membership-never-count (two lists agreeing on "3" while naming different
threes; a renamed test invisible without a rename map); the
`cargo:rerun-if-changed` footgun keeping the generated C header fresh.

The 10 absent-with-owner methods the Phase-7 accounting names are tracked in
§9.23 (opened by the Actor); all block zero of the 55 tests.

### 9.25 `Sender::can_retry` hands the transactional log-truncation branch an empty batch pool

**Status:** open. Found in Phase 8 while translating
`SenderTest.testTransactionalUnknownProducerHandlingWhenRetentionLimitReached`
(Java 1820-1881). **Not a Phase 8 defect** — it predates the phase; `Sender::can_retry`
has passed `&mut []` since Phase 4.

`TransactionManager.canRetry` (`TransactionManager.java:1015-1060`) has an
`UNKNOWN_PRODUCER_ID` arm that, when the broker's `logStartOffset` has moved past our
last acked offset, concludes the producer state was lost to retention. For a
**transactional** producer it then rewrites the partition's in-flight sequences from zero
(`:1042-1050` → `TxnPartitionEntry.startSequencesAtBeginning`) and retries, so the
transaction need not be aborted. For an idempotent producer it bumps the epoch instead.

`Sender::can_retry` (`sender.rs`) calls that method with an **empty** batch pool. Per
rules §7, `start_sequences_at_beginning` errors when a tracked in-flight batch is not
supplied, so the sequence rewrite does not happen.

**The consequence is worse than a stale sequence counter: the response is abandoned and the
batch is dropped.** An earlier revision of this section said "the producer simply carries
the stale sequence state into the retry"; there is no retry. `complete_batch` reaches
`if self.can_retry(batch, response, now)?`, so the `Err` short-circuits **before**
`BatchAction::Reenqueue` can be produced. It propagates out of `handle_produce_response`
and out of `handle_produce_response_for`'s `?` — at which point the batch has already been
moved out of `in_flight_batches` into that function's local
`batches: HashMap<TopicPartition, ProducerBatch>`, which is then dropped. There is no
`impl Drop` anywhere in `src/producer/`, so:

  - the batch's `ProduceRequestResult` is never `set`, so **every record future on it never
    resolves** — and no timeout owner remains to expire them;
  - its pooled buffer is never returned, so `BufferPool` accounting **leaks**, which
    eventually blocks `send` on `max.block.ms`;
  - `handle_client_responses` logs `Uncaught error in request completion` and `run_once`
    still returns `Ok`, which is why the reproducer's `.expect("run_once")` passes and the
    failure surfaces four assertions later as a stale `last_acked_sequence`.

That is a hang-and-leak on a reachable transactional path, not a sequence-hygiene nicety,
and it is what sizes the fix. Critic 48 issue 5.

**The comment at the call site asserted the branch was unreachable, and that is how it
survived.** It read: "`batches` is empty: it supplies the partition's in-flight batches
for the transactional log-truncation rewrite, which the idempotent path never reaches".
The premise is inverted — it is the *transactional* path that reaches that arm; the
idempotent path takes `requestIdempotentEpochBumpForPartition` beside it. The comment is
corrected in place and now cites this section.

Reachability: exactly one test in the tree drives it, and it did not exist until Phase 8.
`testAbortTransactionAndResetSequenceNumberOnUnknownProducerId` looks similar but answers
with `logStartOffset = 0` against a last acked offset of 0, so `0 < 0` is false and it
takes the fail-the-batch path into `handleFailedBatch` instead.

**Reproducer:** `sender.rs`'s
`test_transactional_unknown_producer_handling_when_retention_limit_reached`, left in place
and `#[ignore]`d with this section cited — the treatment §9.18 gave its own before it was
fixed, and that §9.30 now uses.

**Why it was not fixed in Phase 8.** The pool must contain the failing batch, because
`canRetry` runs *before* any `removeInFlightBatch` and the batch is therefore still
tracked — unlike at `handleFailedBatch`, which removes it first and so legitimately has
`pool ⊃ tracked` in the other direction (rules §7). But `TransactionManager::can_retry`
takes the failing batch as `&ProducerBatch` and the pool as `&mut [&mut ProducerBatch]`,
so putting the same batch in both aliases. Fixing it means changing that signature and
threading a three-way pool (accumulator deques + `Sender::in_flight_batches` + the batch
in hand) through `complete_batch` on the produce-response path — a send/receive-path
change needing its own DoD §10 allocation audit, which is the same reason §9.18 keeps the
split fix out of a transactions phase.

**Fix direction:** have `can_retry` take the pool only, with the failing batch inside it,
and identify it by its ordering key (rules §6) rather than by a separate reference. That
removes the aliasing without a second traversal. `Sender::complete_batch` is already
`&mut self`, and `RecordAccumulator::with_in_flight_batch_pool` already assembles both
owners correctly for `bump_idempotent_epoch_and_reset_id_if_needed` — so the pieces exist;
what is missing is the signature change and its audit.

### 9.26 Three `FetchRequestManagerTest` abort-marker tests are now owed, not blocked

**Status:** open, and **reclassified** by Phase 8. Owner: the consumer's own test-parity
work, not a producer-transactions phase.

`testMultipleAbortMarkers` (`FetchRequestManagerTest.java:2443`),
`testReadCommittedAbortMarkerWithNoData` (`:2492`) and
`testReadCommittedWithCommittedAndAbortedTransactions` (`:2367`) were skipped from
Phase 7a onward on a *production* blocker: a READ_COMMITTED control batch from an
already-aborted producer id returned `KafkaError::unsupported_version`, because
`ControlRecordType` was untranslated and COMMIT could not be told from ABORT.

Phase 8 removed that blocker while fixing the defect its integration test surfaced (see
below): `common/record/control_record_type.rs` translates the class and
`CompletedFetch::contains_abort_marker` implements Java's branch. So these three are no
longer blocked — what they now need is a **test fixture**, a batch builder that emits a
real control batch whose first record's key is a marker.
`fetch_request_manager.rs`'s `build_batch_full` does not, and neither does
`fetch_collector.rs`'s fixture, which is why two further tests there still use plain data
batches and say so.

Also owed with them: `test_consumer_position_updated_when_skipping_aborted_transactions`
omits Java's trailing ABORT marker and asserts a position of 2 rather than 3. The contract
it tests is unaffected, but the literal is now an unnecessary deviation.

**Not done in Phase 8** because it is consumer test parity across two files with a new
fixture builder, and Phase 8's scope is `TransactionManagerTest` plus the producer's broker
integration. The marker path is not uncovered in the meantime: it has broker-level cover in
`tests/integration/producer_transactions_test.rs`
(`test_aborted_transaction_records_are_discarded`), which is stronger evidence than the
unit fixture would be, since it reads a marker a real broker wrote.

### 9.27 `containsAbortMarker` was deferred on a false rarity claim — fixed in Phase 8

**Status:** DONE, fixed in Phase 8. Recorded because the *reason* it survived four phases
is the reusable lesson, not the code.

Phase 7a deferred Java's `containsAbortMarker` branch and had `CompletedFetch` return
`KafkaError::unsupported_version` on any READ_COMMITTED control batch whose producer id was
in the aborted set. That is a correct application of CLAUDE.md §5 — fail explicitly rather
than silently mis-deliver — and the module docstring justified the deferral like this:

> This is conservative; production readers will hit it only if their producers reuse
> producer IDs after an abort, which is rare.

**The claim is false, and the real trigger is much wider than even the rebuttal implies.**
The guard was

    if batch_meta.is_control_batch && self.aborted_producer_ids.contains(&batch_meta.producer_id)

placed *after* `consume_aborted_transactions_up_to(batch_meta.last_offset)`. The ABORT
marker batch is itself a control batch carrying the aborted transaction's own producer id,
and that call has just inserted the id — the response's `AbortedTransaction.first_offset` is
≤ the marker's `last_offset` by construction. So the bail fired on the marker of the very
transaction it had just skipped, in the same fetch. The trigger is therefore **any
`read_committed` fetch that reaches an ABORT marker, i.e. any aborted transaction on the
partition** — a single aborted transaction with nothing after it, or an empty aborted
transaction whose marker is its only batch, was enough. `read_committed` was unusable on any
partition that had ever had an abort. And it surfaced as an `UnsupportedVersion` error rather
than as missing data, so it would have read as a broker compatibility problem.

Producer-id stability remains the rebuttal of Phase 7a's *stated premise*: an id is
allocated once per incarnation and is stable across that producer's transactions, so the
"reuse" the note treated as exotic is what every transactional producer does. But an earlier
revision of this section, and of the three other artifacts describing the fix, let that
stand as the *trigger* — i.e. described a two-transaction pattern where the code needed
only one abort. Critic 48 issue 4. Stating the true scope strengthens this section's own
lesson rather than weakening it: no test in either suite reached a branch that broke every
`read_committed` reader of an aborted partition.

Phase 8's `test_aborted_transaction_records_are_discarded` failed on exactly this on its
first run, which is what exposed it. The fix is the faithful translation:
`ControlRecordType` (`common/record/control_record_type.rs`, its own file per CLAUDE.md §2,
with `InvalidRecordError` for Java's `InvalidRecordException`) plus
`CompletedFetch::contains_abort_marker`, in Java's order — the ABORT marker clears the
producer id *before* `isBatchAborted` is consulted (`CompletedFetch.java:210-218`).

**The lesson:** the deferral note asserted a frequency for a *client* behaviour without
checking what the client does. A rarity claim used to justify a deferral is a factual claim
about a code path and should be checked the same way any other claim is — here, one look at
where `producerId` is assigned would have settled it. The scope of the resulting integration
gap is the tell: no test anywhere reached the branch, in either the producer or the consumer
suite, for four phases.

### 9.28 `MockClient.disconnect(node, allowLateResponses)` does not port — produce responses are routed by correlation id, not by callback

**Status:** open as a recorded divergence; no defect. Found by Critic 48 issue 1, after
Phase 8 shipped a translation of the overload that was **inert** and justified it with a
false statement about Java.

Java's `MockClient.disconnect(String, boolean allowLateResponses)`
(`MockClient.java:200-218`) retains the disconnected request so a later `respond*` answers
it a second time, and that second answer reaches the `Sender` because the routing lives on
the request: `ClientRequest.callback()` (`ClientRequest.java:104-105`) is a plain **getter**,
so the disconnect `ClientResponse` and the late one are handed the same
`RequestCompletionHandler` and `ClientResponse.onComplete` (`:152-154`) fires it both times.
`Sender.sendProduceRequest` installs exactly such a callback, so a produce response really is
handled twice.

This port cannot reproduce the mechanism, and the reason is a deliberate design choice, not
an oversight: `Sender::send_produce_request` passes `None` for the callback ("we process
responses after poll() returns") and routes by correlation id through
`Sender::pending_produce_responses`, which the **first** delivery `remove`s. A retained
request's second answer therefore finds no entry and `handle_produce_response_for` returns
`Ok(())` without touching the batch. The overload would buy nothing but stopping `respond*`
from panicking on an empty queue.

**What Phase 8 shipped first, and why it was wrong.** It translated the overload and
commented that Java's retained request "carries no callback, … which is exactly what Java's
retained `ClientRequest` does after its `request.callback()` was handed to the disconnect
`ClientResponse`". `callback()` does not move; the claim is false, and it made an inert
surface look justified. Both are corrected: the overload is gone,
`MockClient::disconnect_by_id`'s rustdoc records why it is absent with the Java citations,
and `testReceiveFailedBatchTwiceWithTransactions` is translated without it.

**How the test is translated instead.** The property Java pins is that a produce response
for an **already-failed** batch leaves the manager ABORTABLE rather than FATAL. The batch is
failed by the delivery-timeout expiry — which Java also performs, via the same
`time.sleep(2000)` — and that path parks the batch in `Sender::batches_awaiting_response`
*without* consuming its routing entry, so the late `INVALID_TXN_STATE` is genuinely routed
into `handle_produce_response` for a done batch. Only Java's `disconnect` + `backoff` pair is
dropped, and its purpose there is to stop the Sender sending anything new, of which there is
none. Four assertions pin it: both `pending_produce_responses` and
`batches_awaiting_response` hold the batch before the late response and are empty after, and
they drain only by it being handled. Mutation-checked: deleting the response fails the first
assertion, where the previous (inert) revision still passed.

**If this is ever revisited**, the faithful fix is not the overload but the routing:
threading a per-request completion handler through the produce path the way Java does.

**The obstacle is ownership, not a performance budget.** An earlier revision of this
paragraph said "CLAUDE.md §11 warns against per-message callbacks on the hot path", and that
was wrong twice over (Critic 48 issue 14). §11 states no rule about completion callbacks at
all — its four bullets are `Arc<str>` for per-message identifiers, atomics over
`Mutex<i64>`, no `Pin<Box<dyn Future>>` per call on hot paths, and no per-message
`tokio::spawn` on the send path. And §11's own **"Hot path" definition** explicitly *excludes*
the granularity being dismissed: "This does **not** include per-RPC or per-batch top-level API
surfaces". A `RequestCompletionHandler` is **one per produce request**, covering every batch
in it across every partition — per-RPC by construction, i.e. precisely what §11 carves out.
Calling it a "per-message callback" mis-described the alternative being rejected.

The real constraint is structural, and this port documents it three times in the very file
§9.28 is about: **a Rust `RequestCompletionHandler` cannot capture `&mut self`.**

  - `sender.rs:182` (`PendingProduceRequest`'s own doc): "In Java, this data is captured in
    the `RequestCompletionHandler` callback closure. In Rust, because
    `handleProduceResponse` needs `&mut self`, we cannot capture `self` inside the callback.
    Instead, we store the topic-partition set and topic names here and process responses
    after `client.poll()` returns" — and it cites CLAUDE.md §9 as the sanctioned translation
    of a callback into code that runs after the await.
  - `sender.rs:348`: the same reasoning for the *transactional* handler, which Java attaches
    to the `ClientRequest` at `Sender.java:504-505`.
  - `sender.rs:374`: why `batches_awaiting_response` must be an explicit field rather than a
    closure capture — the same ownership limit, and the field that Phase 8's re-translated
    test now asserts on.

So `pending_produce_responses` exists *because* of the ownership model, not as a performance
optimisation, and the double delivery is unreachable as a consequence of that. That is a
stronger reason not to reverse the design than the mis-cited budget was — an ownership
obstacle with known but invasive workarounds (interior mutability over the Sender's state, or
a channel from the handler back into the loop), weighed against Java fidelity in one test.
This entry records the consequence rather than proposing the reversal, but a later phase
weighing it should weigh *that* trade-off.

### 9.29 Critic review of Phase 8 — the milestone's final loop

**Status:** DONE — **closed 2026-08-06 on a clean fourth pass** (11→3→1→0).
Archived at `design/history/Milestone-11/Phase-8/COMMENTS.DONE.48.md`.

Fifteen findings, all real, none disputed. The phase's one production change — the
consumer abort-marker fix, forced by the first real abort marker this repo ever
produced — was correct on arrival and survived Phase-4-depth review; the milestone's
defect surface stayed in the records to the end. Two findings were
**under**-statements corrected in the Actor's favour (the fixed consumer defect was
worse than recorded: `read_committed` unusable on any partition that ever had an
abort; §9.25's batch is dropped outright).

Every account is closed: `TransactionManagerTest` 140/140; `SenderTest` 10/11 with
the last genuinely §9.18-blocked (re-verified); four real-broker transaction
scenarios green with non-vacuous negatives. Open items handed out of the milestone:
§9.25 (can_retry's empty pool), §9.26 (three consumer tests, owed with owner),
§9.18 (the split panic), plus the §9.14 wire-test gap and the §9.17 container leak —
all with owners and evidence.

*(Later: §9.18 was fixed in loop 50, which also translated the three `SenderTest`
methods it blocked — see §9.18 and §9.19, both revised. The Phase-8 narrative above
is left as written.)*

The milestone's closing lesson, in the Critic's words: *"Each rewrite derived the
part it had been faulted on and hand-wrote the part it added. It ended when the
artifact stopped containing anything typed by hand."*

### 9.30 `ProduceRequestBuilder::build_version` drains its builder; Java's `build` does not

**Status:** open. Found in loop 50 while correcting a `mock_client.rs` comment that
had blamed §9.18 for the workaround this causes (Critic 50 issue 1). **Latent, not
live** — the reachability derivation is below, and it is what keeps this out of the
loop that found it.

`ProduceRequestBuilder::build_version` (`src/common/requests/produce_request.rs:300`)
ends with

    std::mem::replace(&mut self.data, ProduceRequestData::new())

so building the same builder twice yields one correct request and one with empty
`topic_data`, silently and with no error. Java's counterpart validates and then
returns `new ProduceRequest(data, version)` sharing the reference
(`ProduceRequest.java:68-74`); the builder is unchanged and may be built any number
of times. (Java *does* null out a `ProduceRequest`'s `data` — `clearPartitionRecords`,
`:94-97` — but that is on the **request**, server-side, after the response is queued;
it is not the builder.)

**It is an outlier, which is the strongest evidence it is unintended.** Of this
crate's 53 `RequestBuilder` impls, this is the only one that drains; every other
clones its data (e.g. `sasl_authenticate_request.rs:142`). A survey is one command:

    for f in $(grep -rln "fn build_version" src/); do \
      awk '/fn build_version/,/^    }/' "$f" | grep -q "mem::replace\|\.take()" && echo "$f"; \
    done
    # -> src/common/requests/produce_request.rs, and nothing else

**Same shape as §9.12 defect 2**, which was ruled a real defect and fixed: there,
`write` drained a `records` field and so *serialising mutated the message*; here,
`build` drains `data` and so *building mutates the builder*. Both were justified as
"zero-copy ownership transfer" and both are unnecessary for that purpose —
`ProduceRequestData` is `Clone`, and its `records` are `bytes::Bytes`, whose clone is
a refcount bump. A clone here copies the `Vec<TopicProduceData>` spine and the topic
name `String`s, not the record payloads.

**Why it is not live.** `network_client.rs:502` is the only production caller of
`build_version`; it builds once per `ClientRequest` and the request is consumed by
`do_send_with_request` immediately after. Retries do not rebuild — the `Sender`
constructs a fresh `ProduceRequestData` per send (the same property §9.12 relied on).
The three test-side callers are each reachable at most once per request:
`mock_client.rs`'s `send` builds only for a matched *future* response, after which the
request never enters `self.requests`; `respond_with_matcher` builds only what is in
`self.requests`; and `sender.rs`'s `send_idempotent_producer_response` answers with
`respond`, which does not build.

So today the divergence costs nothing at runtime — but it has already distorted the
port. `MockClient::send` builds the request *conditionally* where
`MockClient.java:259` builds it unconditionally, and that deviation exists solely to
keep a second build out of reach. Anyone restoring Java's shape there breaks
`respond_with_matcher`.

**Reproducer:** `produce_request.rs`'s `test_build_is_repeatable`, `#[ignore]`d with
this section cited — the treatment §9.18 and §9.25 give theirs. It asserts Java's
contract (two builds, both carrying the topic) and currently fails with
`left: 0, right: 1` on the second.

**Fix direction (not attempted here):** replace the `mem::replace` with
`self.data.clone()`, matching the other 52 builders. That puts a per-request clone on
the send path — a `Vec` spine plus one `String` per topic, no record bytes — so it
needs its own DoD §10 allocation measurement rather than an argument, which is why it
is filed rather than folded into loop 50. If the clone proves measurable, the
alternative is to make `build_version` take `self` by value so a second build cannot
be written; that is a `RequestBuilder` trait change across all 53 impls and should be
priced accordingly.

---

## 10. Recorded translation deviations

`definition-of-done.md` §7 requires every deviation from the Java source to be
justified. Gathered here so a reviewer has one list to check rather than hunting
through commit messages. Each is also documented at its own call site.

### 10.1 `HashMap` iteration → sorted output, wherever Java's order is unspecified

**Rule: where Java groups wire data through a `HashMap` and then serialises it,
the Rust translation sorts before emitting.**

Java's `HashMap` has unspecified iteration order, so the *byte encoding* it
produces for a given logical value is not stable across runs. That is invisible
in Java because the broker treats these collections as sets. It is a problem
here for two reasons:

  1. `definition-of-done.md` §3 requires byte-level wire tests against known
     vectors, not only round-trips. A test cannot assert on bytes whose order is
     nondeterministic.
  2. A nondeterministic encoding makes any future byte-diff against the Java
     client — the most direct way to prove wire compatibility — impossible.

Sorting produces the same logical value with a stable encoding, so it is
behaviour-preserving on the wire while being strictly more testable.

**Applied so far (Phase 2):**

| Site | Sorted by |
|---|---|
| `AddPartitionsToTxnRequestBuilder::build_txn_topic_collection` | topic name |
| `AddPartitionsToTxnResponse::topic_collection_for_errors` | topic name, then partition index |

**How to apply going forward:** any `HashMap`/`HashSet`-grouped collection that
reaches a `write()` gets a deterministic order, and the reason is noted at the
site. `TxnOffsetCommitRequest` (Phase 2) groups offsets by topic the same way and
must follow this. Do NOT sort collections that are *not* serialised — there the
extra work buys nothing.

**Not applicable to** collections whose order Java specifies, or where order is
semantically meaningful (in-flight batch ordering by sequence, for instance,
which is already a `BTreeSet` for exactly that reason).

### 10.2 Broker-side members omitted from translated request classes

`AddPartitionsToTxnRequest` in Java carries `Builder.forBroker`,
`normalizeRequest`, `allVerifyOnlyRequest`, `partitionsByTransaction`, and
`errorResponseForTransaction`. All five construct or inspect a *received* v4+
request, which only a broker does.

Verified callers, none under `clients/src/main`: `forBroker` →
`AddPartitionsToTxnManager.java:343`; `normalizeRequest` → `KafkaApis.scala:1852`;
`partitionsByTransaction` → `:1857`, `:1895`; `errorResponseForTransaction` →
`:1899`, `:1938`; `allVerifyOnlyRequest` → `RequestChannel.scala:228` — a third
file the original write-up omitted (Critic 42 finding 5).

Omitted, consistent with §1.1's exclusion of `WriteTxnMarkers` and
`EndTransactionMarker` from this client-only port. Translating them would add
permanently unreachable code that `#![deny(warnings)]` forces us to mask with
`#[allow(dead_code)]`, which then hides genuinely dead code later.

The corresponding Java tests (`testBatchedRequests`, `testNormalizeRequest`, and
the `version >= 4` half of `testConstructor`) are skipped for the same reason,
recorded in a comment block in the Rust test module per DoD §3.

### 10.3 Java `throws` on a missing lookup → `Option` / `Result`

`AddPartitionsToTxnResponse::get_transaction_topic_results` returns
`Option<&[..]>`. Java calls `find(..)` and dereferences the result, throwing on a
missing transactional id; a Rust caller cannot catch that, so the absence is made
explicit in the return type. Same treatment as `TxnPartitionMap::get`, which
returns `Result` where Java throws `IllegalStateException` (CLAUDE.md §10.2).

### 10.4 Phase 1 deviations (cross-reference)

Recorded in full elsewhere; listed here for completeness:

  - `TxnPartitionEntry` stores ordering keys rather than owning `ProducerBatch` —
    this document's header and `.claude/rules/producer-transactions.md` §7.
  - `TxnPartitionEntry::decrement_sequence` does not wrap — rules §8.
  - `TransactionalRequestResult` composes `Notify` + `AtomicBool` in place of
    `CountDownLatch` — rules §5.
  - `TxnPartitionMap::get_mut` has no Java counterpart (Java references are
    implicitly mutable) — commit `90732c4`.
  - `ProducerConfig::explicitly_set` replaces Java's
    `AbstractConfig.originals()` — commit `d14d1ec`.
  - The five typed txn error structs deliberately not created — §1.1 and rules §9.

### 10.5 Phase 3 deviations (`TransactionManager`, idempotence core)

All six are documented at their call sites in
`src/producer/internals/transaction_manager.rs`.

1. **`TransactionManager::new` returns `Result` and refuses a
   `transactional_id`** (MILESTONE-11 GUARD). Java's constructor accepts one.
   This phase translates no transactional entry point and none of the
   transactional arms of the five internally-forked methods, so accepting a
   transactional id would mean silently taking the idempotent branch where Java
   takes another — which CLAUDE.md §5 forbids. It mirrors the guard already in
   `KafkaProducer::from_config` (§7.1) and is removed in Phase 5. The
   still-unwritten transactional arms return
   `KafkaError::unsupported_version(..)` naming Phase 5, which the guard makes
   unreachable; a test asserts the guard's message.

2. **`TxnRequestHandler` is a struct plus a `TxnRequestHandlerKind` enum**, not
   an abstract class with six subclasses, and `handleResponse` / `onComplete` /
   `coordinatorType` move onto `TransactionManager`. Java's inner classes reach
   the manager through an implicit `TransactionManager.this`; Rust has no
   equivalent, and a `Box<dyn>` handler holding a back-reference to its owner is
   not expressible. This is the same shape the crate already uses for Java's
   `AbstractRequest` / `AbstractResponse` hierarchies (`ConcreteRequest` /
   `ConcreteResponse`), so it introduces no new pattern.

3. **`InFlightBatchPool` type alias** —
   `HashMap<TopicPartition, Vec<&mut ProducerBatch>>` — is the per-partition
   batch pool that `bump_idempotent_producer_epoch` and
   `bump_idempotent_epoch_and_reset_id_if_needed` take, because
   `TxnPartitionEntry` tracks ordering keys rather than owning batches
   (rules §7). It must be keyed by partition: `InFlightBatchKey` is
   `(producer_id, producer_epoch, base_sequence)` and is not partition-scoped,
   so two partitions routinely hold identical keys and a flat pool would let one
   entry rewrite another partition's batch. A type alias adds no struct Java
   lacks (DoD §7).

4. **`producer_id_and_epoch_for_partition`** renames the
   `producerIdAndEpoch(TopicPartition)` overload (Java 689). Rust has no
   overloading and the no-argument form (581) already holds the name.

5. **`maybe_fail_with_error` drops the exception cause.** Java chains `lastError`
   as the cause for the `IllegalStateException` and bare-`KafkaException` cases.
   `KafkaError` has no cause chain; Java's `getMessage()` does not include the
   cause either, so the message text is reproduced byte-for-byte and the cause
   stays reachable through `last_error()`. Preferring exact messages keeps DoD
   §3's message assertions meaningful.

6. **`maybe_resolve_sequences` takes no `Caller`** where every other
   transition-capable method does (rules §1). Its idempotent arm performs no
   transition — it only calls `requestIdempotentEpochBumpForPartition` — so the
   parameter would be dead. Phase 5's transactional arm transitions and adds it.

7. **The rules §2 lock-topology split is deferred to Phase 4, and deviation 2
   raises its price from a field move to thirteen reshaped signatures.** Rules §2
   requires `pendingRequests` and `inFlightRequestCorrelationId` to live on the
   Sender task's own unshared state rather than behind the shared
   `TransactionManager` mutex, because Java touches them from the Sender thread
   only and does so *outside* its `synchronized` blocks —
   `clearInFlightCorrelationId` is called from `onComplete` at
   `TransactionManager.java:1410`, while that method's `synchronized` block only
   begins at `:1421` and wraps `handleResponse` alone. (`:1420` is the
   continuation line of the preceding `log.trace` argument list, not the block
   opener; PLAN §6.5 and `.claude/rules/producer-transactions.md` §2 both cite
   `:1421` correctly.)

   Phase 3 introduces no mutex, so §2 cannot be violated yet. But deviation 2
   hosts `onComplete` / `handleResponse` on the manager, so **every** touch of
   those two fields is now a `&mut TransactionManager` method:

   | Rust method | Java | touches |
   |---|---|---|
   | `enqueue_request` | 1186 | `pending_requests` |
   | `next_request` | 894 | `pending_requests` |
   | `has_pending_requests` | 1005 | `pending_requests` |
   | `maybe_terminate_request_with_error` | 1174 | fails a dequeued handler |
   | `retry` | 934 | `pending_requests` |
   | `fail_pending_requests` | 944 | `pending_requests` |
   | `authentication_failed` | 939 | `pending_requests` |
   | `close` | 949 | `pending_requests` |
   | `set_in_flight_correlation_id` | 973 | correlation id |
   | `clear_in_flight_correlation_id` | 977 | correlation id |
   | `has_in_flight_request` | 981 | correlation id |
   | `on_complete` | 1406 | both |
   | `bump_idempotent_epoch_and_reset_id_if_needed` | 663 | enqueues |

   (Thirteen after issue 1's three additions; the ten the Critic enumerated plus
   `fail_pending_requests`, `authentication_failed` and `close`.)

   So complying in Phase 4 means, at minimum,
   `bump_idempotent_epoch_and_reset_id_if_needed` returning the handler instead of
   enqueuing it, and `on_complete` splitting its correlation-id check from its
   shared-state handling. **Not** complying puts both fields behind the shared
   lock, which is the outcome §2 forbids and which would have `on_complete` hold a
   lock across work Java deliberately leaves unsynchronized. Recorded here rather
   than discovered in Phase 4; §Phase-4's table now carries a row for it, and the
   struct doc in `transaction_manager.rs` points at this deviation instead of
   claiming the rule is merely "not yet engaged". Added after Critic 43 issue 3.

8. **`transition_to_uninitialized` takes no `error`.** Java's parameter
   (Java 756) is passed only to `pendingTransition.result.fail(exception)`
   (`:759`), and `pendingTransition` is `ensureTransactional`-guarded, so for an
   idempotent producer the argument has no consumer. Same treatment and same
   reason as deviation 6. Phase 5 adds the field and the parameter together. The
   redundant `lastError = null` at `:761` is kept even though `transitionTo`
   already clears it on a non-error target, so the translation does not silently
   rely on that.

Not deviations, recorded because a reviewer may read them as such:

  - `pending_requests` is a `VecDeque`, not Java's priority queue. The only
    handler this phase can enqueue is `InitProducerId`, and
    `bumpIdempotentEpochAndResetIdIfNeeded` is guarded on `!hasProducerId()` so
    at most one is pending, making FIFO and priority order identical. `Priority`
    itself is translated in full. §2 anticipated this ("a plain FIFO holding a
    single `InitProducerId` suffices").
  - `maybe_terminate_request_with_error` omits Java's
    `hasAbortableError() && handler instanceof FindCoordinatorHandler` escape
    hatch, because that handler does not exist until Phase 5 and the test could
    only ever be false.
  - Two loops collect a `Vec` of partition keys where Java iterates in place, to
    satisfy the borrow checker. Both are per-`runOnce`, over error-state
    partitions only, and order is not observable because each partition is
    handled independently.
  - ~~`fail_pending_requests`, `authentication_failed` and `close` iterate
    `pending_requests` **by index** where Java uses `forEach`.~~ **No longer true
    as of Phase 4**: once the queue became a parameter rather than a field (§2, see
    Phase-4 deviation 1 below) the queue borrow and `&mut self` are disjoint, so all
    three are direct `forEach` equivalents. Java's choice not to clear the queue is
    still preserved.

### 10.6 Phase 4 deviations (idempotent send-path integration)

Each is documented at its call site as well.

1. **The rules §2 split is "queue as a parameter", not "queue on the Sender's own
   type".** `pendingRequests` and `inFlightRequestCorrelationId` are fields on
   `Sender`, and the manager methods that Java implements by touching them take them
   as parameters — the same shape rules §7 already uses for `InFlightBatchPool`.
   Four methods whose bodies touch *only* Sender-confined state moved to `Sender`
   outright (`hasPendingRequests` 1005, `setInFlightCorrelationId` 973,
   `clearInFlightCorrelationId` 977, `hasInFlightRequest` 981), as did the
   unsynchronized half of `TxnRequestHandler.onComplete` (1406-1420). The criterion
   is stated once, on the `TransactionManager` struct docs. The property a reviewer
   can check mechanically: neither name appears as a **field** in
   `transaction_manager.rs`. Supersedes deviation 7's "thirteen reshaped signatures"
   estimate — it was accurate.

2. **`PendingRequests` type alias** (`VecDeque<TxnRequestHandler>`) so Phase 5's swap
   to a priority queue is one line. Adds no struct Java lacks (DoD §7); same
   justification as `InFlightBatchPool`.

3. **`TransactionPhaseError { Authentication, Other }`**, a private enum in
   `sender.rs`. Java distinguishes the two by exception *type* at two different
   `catch` sites (`Sender.java:336` vs `:248`), and `KafkaError` is flat. Same
   approach `common::network::authentication_error` already takes at the transport
   boundary, and for the same reason.

4. **`RecordAccumulator::with_in_flight_batch_pool` assembles the rules §7 pool from
   both owners.** Java's `TxnPartitionEntry` holds live batch references; Rust's
   tracks keys, so the batches must come from the accumulator's deques *and*
   `Sender::in_flight_batches`. The merge happens inside the accumulator because
   every `&mut ProducerBatch` in the pool must share the deque guards' lifetime,
   which only exists inside that call. Every requested partition's deque lock is held
   for the duration of the closure, and the manager lock is taken inside it —
   preserving rules §3's deque → manager order.

5. **Two accessors Java lacks:**
   `TransactionManager::client_side_epoch_bump_required` and
   `partitions_to_rewrite_sequences`. Java reads both fields from inside the class;
   the Sender needs them to decide whether to build the pool *before* taking the
   manager lock, because building it locks deques and is pure waste on the common
   path. Java needs no equivalent because its entry can reach the batches itself.

6. **`RecordAppendResult::topic_partition`.** Java reports the resolved partition
   through `AppendCallbacks.setPartition` (`KafkaProducer.java:1606`) and reads it
   back as `appendCallbacks.topicPartition()` for `maybeAddPartition`. The Rust
   `append` takes a plain completion `Callback`, not an `AppendCallbacks` trait
   object, so there is nowhere else for it to go.

   It carries the whole `TopicPartition`, not the index. Carrying only the index
   (the first shape, corrected after Critic 44 issue 1) forced
   `KafkaProducer::do_send_bytes` to rebuild it from `topic: &str`, which allocates a
   `String` *and* an `Arc<str>` and copies the topic name twice **per record** on the
   default path — CLAUDE.md §11's named anti-pattern. The accumulator already interns
   one `Arc<str>` per topic, so producing it there costs a refcount increment.
   Measured: the mutation restoring the old construction moves the steady-state
   per-send allocation count from 2 to 4.

7. **`PendingProduceRequest` records each batch's `Arc<ProduceRequestResult>`.**
   Java's callback closes over the batches themselves; identity by `Arc::ptr_eq` is
   the nearest equivalent that survives the batch moving between owners. This fixed
   a real defect — see commit `0b8c3d0`.

8. **`Sender::abort_in_flight_batches` aborts the batches the Sender owns.** Java's
   `abortBatches` (`RecordAccumulator.java:1152`) iterates `incomplete.copyAll()`,
   which returns the batch objects and so covers batches already drained into the
   Sender; `inFlightBatches.clear()` (`Sender.java:536`) then merely drops the map's
   references. Rust's `IncompleteBatches` tracks `ProduceRequestResult`s (a
   `ProducerBatch` has one owner, rules §7), so the accumulator can only reach its
   own deques; dropping the Sender's share un-aborted leaves their record futures
   pending forever (CLAUDE.md §5). Called from both Java sites that need it —
   `maybeAbortBatches` and `run()`'s force-close branch (`Sender.java:294-295`), the
   second added after Critic 44 note 2.

8b. **`Sender::batches_awaiting_response` is the second batch holder Java gets from
   its completion callback.** Three Java paths complete a batch *now* and deallocate
   it *later* — `abortBatches`'s `isInflight()` fork
   (`RecordAccumulator.java:1160-1164`), `failBatch(deallocateBatch=false)` →
   `maybeRemoveAndDeallocateBatchLater` (`Sender.java:177-180`), and
   `abortIncompleteBatches` — and all three rely on the `RequestCompletionHandler`
   closing over `recordsByPartition` (`Sender.java:918`, `:941`) so the response can
   still reach the batch and return its pooled buffer (KAFKA-19012). A Rust callback
   cannot capture `&mut self`, and `PendingProduceRequest` stores only an
   `Arc<ProduceRequestResult>` identity, so that holder is an explicit field.
   `handle_produce_response_for` searches it after `in_flight_batches`.

   `run()` then releases whatever remains after `client.close()`, and that release is
   a **deliberate improvement, not a translation** — an earlier draft of this entry
   claimed Java's `close()` does the same through the aborted requests' callbacks,
   which is false (Critic 44 issue 9). `NetworkClient.close()` is
   `selector.close(); metadataUpdater.close(); telemetrySender.close();`
   (`NetworkClient.java:736-746`); it never walks `inFlightRequests` and never calls
   `completeResponses`, and `Selector.close()` closes each channel with
   `CloseMode.DISCARD_NO_NOTIFY` (`Selector.java:886-892`, mode at `:96`: "no
   disconnect notification"). Java therefore abandons those buffers, unobservably,
   because the `BufferPool` is built inside `KafkaProducer`'s constructor
   (`KafkaProducer.java:438`) and dies with the producer. Rust releases them so the
   pool's accounting is exact for the whole `Sender` lifetime, which is what lets
   `available_memory()` serve as the oracle in the leak regression tests. Added after
   Critic 44 issue 2, which was a permanent `BufferPool::available_memory` shrink on
   every abort of an in-flight batch.

9. **`transaction_completing` is read once per `ready()`**, not once per batch as
   Java does inside `batchReady` (`RecordAccumulator.java:614`). The value is
   partition-independent; hoisting avoids a manager lock per partition and removes
   the (Java-visible) possibility of two partitions in one pass disagreeing.

10. **`begin_abort` translates only `ensureTransactional()`.** Java's shutdown loop
    (`Sender.java:273`) depends on `beginAbort` *throwing* for a non-transactional
    producer, and force-closes when it does. Translating the guard is what makes the
    shutdown path behave as Java's; ~~the transactional body is Phase 6 and is
    unreachable while `TransactionManager::new` refuses a transactional id.~~
    **Corrected in Phase 5a** (Critic 45 issue 2): the body is **Phase 5b**, which the
    `9faf0a0` amendment to §Phase-5 assigns it verbatim, and it is *reachable*, because
    Phase 5a removed that guard — which is why its `unsupported_version` is live code
    rather than dead. The doc comment and the error message both said "Phase 6" until
    the fixup; the Phase-5a sweep of the deleted guard's comments missed this one.

11. **`network_client_utils::await_ready` takes `&(dyn Fn() -> i64 + Send + Sync)`.**
    `&dyn Fn()` is only `Send` when the trait object is `Sync`, and without the bound
    the spawned `Sender` future stops being `Send`. No behaviour change; `await_ready`
    had no other caller.

12. **Two pre-existing gaps fixed because Phase 4 makes them load-bearing:**
    `RecordAccumulator::abort_batches` / `abort_undrained_batches` did not remove the
    aborted batch from `incomplete`, so `has_incomplete()` never fell back to false
    and `maybeAbortBatches` would re-abort every `runOnce`; and
    `ProducerBatch::finalize_split_batches` skipped Java's
    `assignProducerStateToBatches` (`ProducerBatch.java:392`), without which
    `splitAndReenqueue` cannot track an idempotent sub-batch at all.

### 10.7 Phase 5a deviations (`TransactionManager`, transactional state machine)

Each is documented at its call site as well.

1. **`coordinatorSupportsBumpingEpoch` is a `TransactionManager` field, not
   Sender-owned.** Rules §2 and §6.5 above list it with `transactionCoordinator`,
   `consumerGroupCoordinator` and `inFlightRequestCorrelationId` as state "touched
   exclusively by the Sender thread" and therefore belonging on the Sender. That
   premise is true of the other three and **false of this one**: Java reads it on the
   *application* thread on every failed send, through `KafkaProducer.doSend`'s
   `catch (ApiException e)` (`KafkaProducer.java:1066`) →
   `maybeTransitionToErrorState` (`:781`) → `needToTriggerEpochBumpFromClient`
   (`:1310`). That call site already exists in Rust, at `kafka_producer.rs:775`, so a
   Sender-confined field could not serve it.

   Both halves of §2's objection to the shared mutex also fail here. Every reader is
   a `TransactionManager` method whose caller already holds the guard for other
   reasons, so no lock is added; and the only writer, `handle_coordinator_ready`,
   must hold the guard regardless because Java's version reads `apiVersions`
   (`:1104`), a manager field. It runs once per coordinator connection.

   The coordinator **nodes** do go to the Sender as §2 requires: both writers
   (`lookupCoordinator` `:1194`/`:1197` and
   `FindCoordinatorHandler.handleResponse` `:1696`/`:1699`) are unsynchronized, and
   both readers are Sender-side — `coordinator(CoordinatorType)` (`:961`, `:963`),
   whose only production caller is `Sender.java:481`, **and**
   `handleCoordinatorReady` (`:1104-1105`), which reads `transactionCoordinator` as a
   field rather than through the accessor. That second reader is why
   `handle_coordinator_ready` takes `&CoordinatorNodes` (Critic 45 issue 1 — an
   earlier revision of this entry and of the two doc sites claimed a single reader,
   which the parameter it justifies already contradicted).

2. **`CoordinatorNodes`**, a `pub(crate)` struct on `Sender` holding the two
   `Option<Node>` slots, with Java's `coordinator(CoordinatorType)` (`:958`)
   attached. Same justification family as `PendingRequests` and `InFlightBatchPool`
   (deviations 10.5§3, 10.6§2): it is the §2 Sender-owned state that manager methods
   receive from its owner. A struct rather than two parameters because both of Java's
   writers `switch` on a `CoordinatorType` to pick *which* slot to write, so the two
   cannot arrive separately. Adds no concept Java lacks (DoD §7).

   Java's `CoordinatorType` has a `SHARE` variant, so all three `default:` arms
   (`:962` "Received an invalid coordinator type: ", `:1199` "Invalid coordinator
   type: ", `:1702` "Group coordinator lookup failed: Unexpected coordinator type in
   response") are reachable and are translated separately — the three messages
   differ. `coordinator_type_name` is a free function because Java interpolates
   `CoordinatorType.name()` (uppercase) where Rust's `Debug` would print `Share`.

3. **`PendingRequests` becomes a struct with an insertion-sequence tiebreaker.**
   Java's `PriorityQueue` (`:224`) is a min-heap and is *unstable* for equal
   priorities; Rust's `BinaryHeap` is a max-heap. `QueuedRequest::cmp` inverts the
   comparison and adds the tiebreaker, making ties FIFO so tests can assert on them
   (§Phase-5 recommends this). A strict refinement: any order Java may produce for
   equal keys is admissible. The priority is snapshotted at insertion rather than
   read back off the handler — `InitProducerIdHandler.priority()` (`:1477`) is
   dynamic, and rules §6 forbids keying an ordered collection on a field read through
   the element even where that field happens to be immutable.

4. **`maybeAddPartition`'s transactional arm is split across 5a and 5b.** The
   task's boundary places the whole arm in 5b, but the tests that pin its two state
   guards (`testFailIfNotReadyForSend*` / `testNotReadyForSend*`) are named as 5a's
   and those guards depend on nothing 5b owns. So the three branches that are pure
   state validation — `!hasProducerId()` (`:442`), `currentState != IN_TRANSACTION`
   (`:445`) and the already-added short-circuit (`:452`) — are translated, and the
   Transaction V2 arm (`:448`) and the `newPartitionsInTransaction` registration
   (`:456`) return `UnsupportedVersion` naming 5b. Deferring the registration is what
   keeps that set empty, and so keeps `nextRequest`'s first statement (`:895`) and
   `isSendToPartitionAllowed`'s set lookup consistent in 5a.

5. **`initializeTransactions`'s two package-private overloads are renamed**, Rust
   having no overloading: `initializeTransactions(ProducerIdAndEpoch)` (`:291`) →
   `initialize_transactions_with_producer_id_and_epoch`, and
   `initializeTransactions(ProducerIdAndEpoch, boolean)` (`:299`) →
   `initialize_transactions_internal`. Same treatment and same reason as
   `producer_id_and_epoch_for_partition` (deviation 10.5§4). The public
   `initializeTransactions(boolean)` (`:295`) keeps its name.

6. **`handleCachedTransactionRequestResult`'s `Supplier` becomes
   `FnOnce(&mut Self) -> Result<Arc<TransactionalRequestResult>, KafkaError>`.**
   Java's supplier closes over `TransactionManager.this`; a Rust closure cannot
   capture `self` while `self` is borrowed by the method, so the manager is passed
   in. `Result` because Java's suppliers throw — `initializeTransactions`'s calls
   `transitionTo` (`:308`) and `beginCommit`'s calls `maybeFailWithError` (`:354`) —
   and when they do, `pendingTransition` is left unset exactly as in Java.

7. **`FindCoordinatorHandler.handleResponse` diverges on an *empty* coordinator
   list.** Java's `coordinators.size() != 1` branch calls `fatalError(..)` and then
   **falls through** to `coordinators.get(0)` (`:1685-1689`); `fatalError` does not
   rethrow and `FATAL_ERROR` is an unconditionally valid target, so for a
   two-coordinator response Java records the fatal error *and* installs one of them
   and calls `result.done()`. That is preserved. For an empty list Java would raise
   `IndexOutOfBoundsException`; Rust must not panic (CLAUDE.md §10.1) and returns the
   fatal error instead — an error either way, with the same state left behind.

8. **`coordinatorData.key() == null` becomes `.is_empty()`.** The Rust message spec
   defaults a nullable string without an explicit `"default": "null"` to the empty
   string (CLAUDE.md §2), and `FindCoordinatorResponse::coordinators` synthesises
   exactly that key for a v≤3 response — so the empty key *is* the null case Java
   tests for at `:1691`.

9. **`TxnRequestHandler::request_builder()` becomes
   `clone_request_builder() -> Box<dyn RequestBuilder>`** now that two builder types
   exist, with `init_producer_id_request_data()` /
   `find_coordinator_request_data()` for the `&self` reads that used it, and
   `api_key()` so `Sender.java:490`'s log statement need not box a builder. Java
   hands the builder itself to `newClientRequest`, keeping the handler's reference
   for a retry; this crate only accepts `Box<dyn RequestBuilder>` there, so it is
   cloned — once per transactional request, never per record or per batch.
   `is_find_coordinator()` replaces Java's
   `requestHandler instanceof FindCoordinatorHandler` (`:1176`).

10. **`isSendToPartitionAllowed` and `TransactionManager::new` no longer return
    `Result`.** The first because its transactional arm is now translated rather than
    deferred, the second because Phase 3's MILESTONE-11 GUARD is gone. Five call
    sites drop an `.expect`. `KafkaProducer::from_config` keeps its own guard on
    `transactional.id` until Phase 6 (§7.1), so the only way to build a transactional
    manager today is directly.

11. **`resetTransactionState` hardcodes `Caller::Sender`** (rules §1's "called only
    from the Sender task" clause): both Java call sites are Sender-side —
    `nextRequest`'s "EndTxn for a transaction that never started" branch (`:923`) and
    `EndTxnHandler.handleResponse` (`:1767`). Both are Phase 5b, so the method has no
    caller yet; it is translated now because it is the only writer that clears the
    per-transaction sets and `preparedTxnState`, and splitting it from the state
    machine would mean writing it twice. `prepared_txn_state` stays
    `ProducerIdAndEpoch::NONE` throughout Phase 5a, since both of its writers
    (`prepareTransaction` `:342` and the `keepPreparedTxn` response arm `:1507`) are
    KIP-939; `preparedTransactionState()` (`:1976`) is translated so the field has a
    reader.

12. **The Rust `do_init_transactions` / `run_init_transactions` test helpers omit
    Java's `maybeUpdateTransactionV2Enabled(true)`** (`TransactionManagerTest.java:4360`).
    Behaviour-neutral for every Phase-5a test: no in-scope method builds a
    Transaction V2 manager, `transaction.version` is finalized at level 1 in the
    fixture so `isTransactionV2Enabled` stays false, and `onInitialization = true`
    suppresses the call's only side effect (`:500-501`). The claim ships with its
    check, in the PHASE-5A TEST ACCOUNTING block in `transaction_manager.rs`.

13. **`TransactionalRequestResult` derives `Debug`**, so `expect_err` on a
    `Result<Arc<TransactionalRequestResult>, KafkaError>` compiles. No behaviour
    change; Java's class has no `toString` either, and the derived form is only ever
    read from a test failure message.

Not deviations, recorded because a reviewer may read them as such:

  - `keepPreparedTxn` reaches only the two `log.info` statements
    (`:309`, `:312`). Java's `initializeTransactions` does **not** put it on the
    request — the `InitProducerIdRequestData` at `:316-320` sets `transactionalId`,
    `transactionTimeoutMs`, `producerId` and `producerEpoch` only — and
    `setKeepPreparedTxn` appears nowhere in `clients/src` in Apache Kafka 4.2. So
    `builder.data.keepPreparedTxn()`, the condition guarding the two-phase-commit
    response arm at `:1501`, is always false on that path in Java too, and the Rust
    arm's `UnsupportedVersion` guard is unreachable for Java's own reason rather than
    a translation gap.
  - Three of the nine `State` variants are still not *enterable*:
    `PREPARED_TRANSACTION`, `COMMITTING_TRANSACTION` and `ABORTING_TRANSACTION`, each
    blocked on a Phase-5b entry point (`prepareTransaction` / the `keepPreparedTxn`
    response arm, `beginCommit`, `beginAbort`) rather than on transition logic. The
    full 9×9 table has been translated since Phase 3.
  - `isInitializing` (`:1090`) has no caller in either the Java client or its tests.
    Translated because it is part of the class (DoD §2).
  - `nextRequest`'s two transaction-only branches (`:895`, `:913`) stay comments
    rather than explicit failures, unlike the arms named in deviation 4: neither is
    reachable in 5a — nothing can populate `newPartitionsInTransaction`, and
    `isEndTxn()` is false for both 5a handler kinds — so an `UnsupportedVersion`
    return there would be dead code, and `nextRequest` would have to grow a `Result`
    for it.

---

### 10.8 Phase 5b deviations (`TransactionManager`, transactional requests)

Each is documented at its call site as well.

1. **`format_partition_errors` prints each error as its Rust variant identifier, not
   Java's enum constant.** Java's `Errors` is an enum, so interpolating one into the
   `KafkaException` message at `TransactionManager.java:1625` yields
   `Enum.toString()` = `name()`, e.g. `TOPIC_AUTHORIZATION_FAILED`. This crate's
   `Errors` renders `Display` as the human-readable `message()` and has no `name()`,
   so `{:?}` is used and the message carries `TopicAuthorizationFailed`. That
   identifier *is* the translation of Java's constant under CLAUDE.md §2's PascalCase
   rule; no Java test asserts on the message; and adding a 134-arm `name()` to
   `src/common/protocol/errors.rs` for one diagnostic string would be out of
   proportion to a transactions phase. Two `log` statements (`:1610`, and the
   `TxnOffsetCommit` debug at `:1901`) share the treatment.

2. **A malformed `AddPartitionsToTxn` response is an error, not a panic.** Java reads
   `errors().get(V3_AND_BELOW_TXN_ID)` (`:1561`) and iterates it unchecked; `errors()`
   omits that key entirely when the response carries no v3-and-below topic results, so
   a malformed or v4+-shaped reply makes Java raise a `NullPointerException` inside
   `NetworkClient.poll`. Rust returns `KafkaError::IllegalState` instead — the same
   treatment `handle_find_coordinator_response` gives Java's
   `IndexOutOfBoundsException` (§10.7 deviation 7), and unreachable in practice
   because the request is only ever built from a non-empty pending set.

3. **`nextRequest` returns `Result<Option<TxnRequestHandler>>`.** Java's method returns
   the handler and can throw, through `resetTransactionState`'s `transitionTo` on the
   "EndTxn for a transaction that never started" path (`:923`). Keeping `Option` for
   "nothing to send" rather than overloading one return value with both meanings costs
   one `?` at the single production call site, where `Sender` maps it to
   `TransactionPhaseError::Other` — which is where Java's `runOnce` catch-and-log
   receives it. The throw is unreachable on that path (the only states holding a
   pending `EndTxn` are `COMMITTING_TRANSACTION` and `ABORTING_TRANSACTION`, and both
   `→ READY` and `→ INITIALIZING` are valid from either), and every test call site
   asserts as much with `.expect("next_request does not fail on this path")`.

   Their count and placement, derived rather than written down (the first revision of
   this entry said "35 test call sites" across two files, which was neither the count
   nor the file list — Critic 45 5b issue 3, and the same class as 5a issue 4: a
   number nobody re-derived after the tree grew):

   ```
   $ grep -ro '\.expect("next_request does not fail on this path")' src/ | wc -l
   56
   $ grep -rc 'next_request does not fail on this path' \
       src/producer/internals/transaction_manager.rs \
       src/producer/internals/sender.rs \
       src/producer/internals/record_accumulator.rs
   src/producer/internals/transaction_manager.rs:53
   src/producer/internals/sender.rs:2
   src/producer/internals/record_accumulator.rs:1
   ```

   All 56 are inside `#[cfg(test)]` modules — the first match in each file follows its
   `#[cfg(test)]` line (4993 > 4671, 3875 > 2274, 1918 > 1813) and there are no
   further module boundaries below — so no `expect` sits on a production path.

4. **`coordinator_key` takes an explicit lifetime.**
   `coordinatorKey()`'s base implementation returns the manager's `transactionalId`
   while `TxnOffsetCommitHandler`'s override (`:1899`) returns the group id off the
   *handler*. Rust's lifetime elision would tie the result to `&self` alone, so the
   signature is `fn coordinator_key<'a>(&'a self, handler: &'a TxnRequestHandler) ->
   Option<&'a str>`. No behaviour change; Java gets this for free from GC.

5. **`CommitFailedException` is encoded as a bare `KafkaException`, not as
   `ConsumerError::CommitFailed`.** Java's `TxnOffsetCommitHandler` raises
   `new CommitFailedException(..)` at `:1929-1931`, and `CommitFailedException extends
   KafkaException`. The crate already has `ConsumerError::CommitFailed`, but its
   `From<ConsumerError> for KafkaError` impl flattens to `KafkaError::IllegalState` —
   which would send `maybeFailWithError` down Java's `instanceof IllegalStateException`
   branch (`:1167`) instead of its bare-`KafkaException` one, changing the message the
   *next* transactional call reports. So the error is
   `KafkaError::with_message(Errors::UnknownServerError, ..)` carrying Java's exact
   text, and the tests assert that text (`definition-of-done.md` §3).

6. **Per-partition error maps are iterated in sorted order.** Java walks the
   `AddPartitionsToTxn` and `TxnOffsetCommit` error maps in `HashMap` order, and both
   loops contain arms that `return` / `break` mid-walk — so the order decides *which*
   error is reported when a response carries several. `sorted_partition_errors` fixes
   it, which is rules §10's reasoning applied to control flow rather than to an
   encoding. `addPartitionsToTransactionHandler`'s partition list is sorted for the
   encoding reason proper.

7. **Two `#[cfg(test)]` doors exist for the KIP-939 response arm.**
   `InitProducerIdRequestBuilder::data_mut` and
   `TxnRequestHandler::set_keep_prepared_txn_for_test`. Java's
   `initializeTransactions` never calls `setKeepPreparedTxn` in Apache Kafka 4.2, so
   the arm at `:1501` cannot be reached without setting the flag on a built request —
   which is exactly what `TransactionManagerTest.prepareInitPidResponse`'s
   `keepPreparedTxn = true` overload asserts the broker would see. Shipping the branch
   untested is the failure mode §9.16 records.

8. **`do_init_transactions` calls `handleCoordinatorReady`.** Java's helper spins
   `Sender.runOnce`, which connects to the transaction coordinator and so runs
   `handleCoordinatorReady` (`Sender.java:569`) as a side effect. That method is the
   only writer of `coordinatorSupportsBumpingEpoch`, which decides whether
   `abortableErrorIfPossible` recovers or goes fatal (`:1326`) — so a manager-level
   drive that skips it takes the fatal branch Java does not.
   `test_bump_transactional_epoch_on_recoverable_add_partition_request_error` is the
   test that surfaced it. Recorded here because it changes what every test using the
   fixture observes, not just the one.

Not deviations, recorded because a reviewer may read them as such:

  - **The `keepPreparedTxn` response arm is unreachable, for Java's own reason.**
    §10.7's note stands: `setKeepPreparedTxn` appears nowhere in `clients/src`, so
    `builder.data.keepPreparedTxn()` is always false on the `initializeTransactions`
    path in Java too. The arm is translated in full rather than stubbed, because it is
    what a broker-driven recovery takes once a caller does set the flag, and because
    omitting it would leave `PREPARED_TRANSACTION` reachable from only one of its two
    Java sources.
  - **All nine `State` variants are now enterable**, closing §10.7's note on
    `PREPARED_TRANSACTION` / `COMMITTING_TRANSACTION` / `ABORTING_TRANSACTION`.
  - **`TransactionManagerTest` has no two-phase-commit test in 4.2.**
    `prepareTransaction`, `preparedTransactionState` and `enable2pc` appear in no
    method body, and `doInitTransactionsWith2PCEnabled` is declared and never called.
    Phase 5b's 2PC cover is therefore three Rust-side tests plus the
    `KafkaProducerTest` cover §Phase-6 owns.
  - **`RecordAccumulatorTest.testRecordsDrainedWhenTransactionCompleting`
    (Java 976-1019) becomes translatable but is not translated here.** Phase 4
    deferred it because `COMMITTING_TRANSACTION` was unreachable; 5b makes it
    reachable. It is a `RecordAccumulatorTest` method, outside this phase's test
    scope, and its Java form stubs `isCompleting()` with Mockito — the accumulator
    comment block at `record_accumulator.rs` keeps naming Phase 6 as the owner, which
    is still right because that is where the public `commit_transaction` lands.

---

### 10.9 Phase 6 deviations (public producer API + Sender transactional loop)

Each is documented at its call site as well.

1. **`PendingRequests` is shared, not Sender-confined.** Rules §2 lists
   `pendingRequests` with the state "touched exclusively by the Sender thread". That
   was true of every caller through Phase 5b and is **false of the class**: Java
   enqueues into it from the application thread through all four blocking public
   methods, every one of them reaching a `synchronized` manager method. Cited at the
   `transactionManager.<m>(..)` call statement rather than at the enclosing method's
   declaration, so the line and the method cannot drift apart:

   | `KafkaProducer` method | call statement | line | manager method |
   |---|---|---|---|
   | `initTransactions` | `initializeTransactions(false)` | 652 | `:299` |
   | `sendOffsetsToTransaction` | `sendOffsetsToTransaction(..)` | 740 | `:404` |
   | `commitTransaction` | `beginCommit()` | 783 | `:353` |
   | `abortTransaction` | `beginAbort()` | 818 | `:361` |

   Derived, not transcribed:

   ```
   $ grep -n "transactionManager\.\(initializeTransactions\|sendOffsetsToTransaction\|beginCommit\|beginAbort\)" \
       kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java
   652:        TransactionalRequestResult result = transactionManager.initializeTransactions(false);
   740:            TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
   783:        TransactionalRequestResult result = transactionManager.beginCommit();
   818:        TransactionalRequestResult result = transactionManager.beginAbort();
   ```

   An earlier revision of this entry, and of both field docs, paired those four numbers
   with the method names **rotated one position** (Critic 46 issue 6). Each wrong arrow
   landed inside a *different* method, and since this entry is the stated evidence for
   the rules §2 amendment below, the rotation would have been copied into the rules
   file. Hence the shipped grep.

   So it became `Arc<Mutex<PendingRequests>>` shared between `Sender` and
   `KafkaProducer`, kept **outside** the manager exactly as rules §2 requires. Lock
   order is fixed at `pending_requests` → `TransactionManager`, giving the producer a
   full order of deque → `pending_requests` → manager. Rust evaluates a method
   receiver before its arguments, so every site binds the guard to a local first;
   locking inline would invert it.

   **Java's unsynchronized writer races; the Rust lock closes the race.**
   `lookupCoordinator(TxnRequestHandler)` (`:969`) is package-private and not
   `synchronized`, and both its callers are Sender-side (`Sender.java:522`,
   `TransactionManager.java:1414`) — which is why rules §2 grouped the queue with the
   Sender-confined state. But single-caller confinement is not safety, and an earlier
   revision of this entry called that access "safe for a different reason", which proves
   a weaker claim than it states (Critic 46 issue 7). The site reaches
   `pendingRequests.add` (`:969` → `:1191` → `enqueueRequest` `:1207` → `:1188`)
   **without holding the monitor**, while the four public methods above add to the same
   `PriorityQueue` *under* it. No other lock, nothing `volatile`, `PriorityQueue` not
   thread-safe: there is **no happens-before edge** between the two writers, so Java has
   a real race whose narrowness — the app-side calls are rare — is all that keeps it from
   biting. Confinement was plainly the intent; the public entry points void it. Taking
   the lock at that site makes the Rust translation **strictly safer than Java**, at no
   cost (once per transactional request, never per record or batch) — a better argument
   for the design than declaring the Java race safe.

   **Suggested rule amendment:** rules §2's bullet list should move `pendingRequests`
   from the Sender-owned group to a third category, "outside the manager but shared",
   with the four app-side call sites cited. The other three fields it groups with
   (`inFlightRequestCorrelationId`, the two coordinator nodes) are genuinely
   Sender-confined and stay where they are.

2. **`KafkaProducer` has no `prepare_transaction`, because Java 4.2 has none.**
   §Phase-6 above says "`prepare_transaction` (2PC) is on `KafkaProducer` only, **not**
   on the `Producer` interface — verified: `grep prepareTransaction Producer.java`
   returns nothing." The grep is right and the inference is wrong. In the `kafka/`
   submodule this milestone builds against (`a18251bae0 Bump version to 4.2.0`),
   `prepareTransaction` exists **only** on `TransactionManager` (`:342`, translated in
   Phase 5a) plus a metric description in `KafkaProducerMetrics` (`:80`):

   ```
   $ grep -rn "prepareTransaction" kafka/clients/src/
   kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/KafkaProducerMetrics.java:80: ...
   kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java:342: ...
   kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java:344: ...
   ```

   `completeTransaction` likewise appears only inside `throwIfInPreparedState`'s
   message text. Adding either to `KafkaProducer` would violate DoD §7 — a public
   method with no Java counterpart.

   **The metrics hit is dead scaffolding, which independently confirms the refusal —
   but not for the reason first offered.** Critic 46 argued the sensor implies nothing
   because "there is no `recordPrepareTxn` method anywhere in the class". That is false:
   it exists, at `KafkaProducerMetrics.java:124`. The true and stronger fact is that the
   *recorder has no callers at all*:

   ```
   $ grep -rn "recordPrepareTxn" kafka/
   kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/KafkaProducerMetrics.java:124:    public void recordPrepareTxn(long duration) {
   $ grep -rn "recordInit\b" kafka/clients/src/
   .../internals/KafkaProducerMetricsTest.java:52:        producerMetrics.recordInit(METRIC_VALUE);
   .../internals/KafkaProducerMetrics.java:104:    public void recordInit(long duration) {
   .../producer/KafkaProducer.java:655:        producerMetrics.recordInit(time.nanoseconds() - now);
   ```

   One occurrence in the whole tree — its own declaration — against `recordInit`'s three
   (declaration, the `initTransactions` call site, a unit test). A sensor plus a recorder
   that nothing invokes, not even a test, is exactly what a forward-looking KIP-939
   artifact looks like, so it implies no 4.2 producer method. Recorded in this corrected
   form rather than as relayed, because propagating a false supporting fact for a correct
   conclusion is the failure mode deviation 1's citation grep exists to prevent. What **is** the 4.2 producer-side 2PC surface, and
   what Phase 6 therefore translated, is `throwIfInPreparedState`
   (`KafkaProducer.java:968-976`), wired into both `beginTransaction` (`:677`) and
   `doSend` (`:989`).

   This is the same class of error as §9.19's stale rationales: a claim about the Java
   source that nobody re-derived against the corpus actually being built. Compare rules
   §12's "**The corpus matters, so always name it**".

3. **`await_sender_handle` puts the handle back on expiry — a bug fix, not just a
   translation.** It passed the `JoinHandle` by value to `tokio::time::timeout`, so on
   expiry the handle was dropped and `await_sender_handle_indefinitely` had nothing to
   join: `close(Duration)` returned while the Sender task was still running. Java joins
   unconditionally after force-closing (`KafkaProducer.java:1414-1418`) and CLAUDE.md
   §9.4 requires the translation to actually await the handle. Now awaits
   `&mut join_handle` (`JoinHandle` is `Unpin`) and restores it.

   **How it was found is not what prevents its return, and an earlier revision of this
   entry conflated the two** (Critic 46 issue 6). The bug surfaced while reading the
   force-close path for the three `testCloseIsForcedOn*` translations, which are the only
   tests that *reach* it — but reaching a path is not pinning it. The Critic traced every
   assertion in those three and they **pass with the bug reintroduced**: without the
   restore, `await_sender_handle_indefinitely` finds `None` and returns immediately, so
   `close` returns *sooner*, `elapsed < 5s` still holds, and the conditional
   `initTransactions` arm is skipped in both variants because that call is parked on its
   own `max.block.ms`. Claiming the three as the regression cover was therefore false.

   Two Rust-side tests now pin it, both with no Java counterpart — `ioThread.join()`
   cannot lose its thread, so the failure mode is specific to `tokio::time::timeout`
   consuming its future:

     - `test_await_sender_handle_keeps_the_handle_when_it_expires` — the mechanism. An
       expired wait must leave `sender_handle` `Some`, and the retained handle must still
       be awaitable rather than a husk.
     - `test_close_joins_the_sender_after_forcing` — the contract. `close_timeout` must
       not return before the Sender task has finished, made observable by an exit hook the
       harness runs on the Sender's own thread after `Sender::run` returns, following a
       deliberate 300 ms of shutdown cost (instrumentation for the assertion's margin, a
       `MockClient` shutdown otherwise being instant).

   Mutation-checked by restoring the exact pre-fix line: both new tests fail, and all
   three `testCloseIsForcedOn*` still pass — which is the Critic's finding reproduced
   rather than taken on trust.

4. **No metrics.** `producerMetrics.recordInit` / `recordBeginTxn` /
   `recordSendOffsets` / `recordCommitTxn` / `recordAbortTxn`, and the
   `time.nanoseconds()` statements that exist only to feed them, are not translated:
   `KafkaProducerMetrics` and the whole `org.apache.kafka.common.metrics` package are
   listed in `remaining_classes.txt`. Consequence for tests:
   `testMeasureAbortTransactionDuration` and `testMeasureTransactionDurations` keep
   their operation sequences and drop only their `getMetricValue` assertions.

5. **`configure_transaction_state` no longer returns `Result`.** Its only error was
   `from_config`'s temporary `transactional.id` guard (§7.1), which this phase removed;
   Java returns a nullable `TransactionManager`, which is now exactly `Option`.

6. **`throwIfInvalidGroupMetadata`'s null arm is not translated.**
   `ConsumerGroupMetadata` is a value in Rust, so `KafkaProducer.java:1499-1500` is
   enforced by the type system. This is also why
   `KafkaProducerTest.testNullGroupMetadataInSendOffsets` (Java 1943) is the one method
   of the 27 with no Rust counterpart; its sibling
   `testInvalidGenerationIdAndMemberIdCombinedInSendOffsets`, which exercises the arm
   that *is* translated, records the reason.

7. **`send_offsets_to_transaction` takes `offsets` and `group_metadata` by value.**
   CLAUDE.md §12 asks for the most general borrowed form, but the transaction manager
   *moves* both into the `AddOffsetsToTxn` handler that carries them to the coordinator
   (`TransactionManager.java:1635-1643`), so borrowing would force a clone Java does
   not make. `AsyncKafkaConsumer::commit_sync_offsets` already set this convention for
   an offsets map.

8. **`Sender::run_once` is `pub(crate)`**, matching Java's package-private `runOnce()`
   that `SenderTest` calls directly. Needed because the `KafkaProducerTest` harness
   lives in `kafka_producer.rs`.

9. **Two test-harness deviations, both structural rather than stylistic.**

   - `TxnProducerContext` keeps the `Sender` **test-owned** and runs the application
     call concurrently with a `run_once` loop (`drive`, on `tokio::join!` — never
     `select!`, which would drop the losing future). Java's helper spawns a real Sender
     thread and pokes a synchronized `MockClient` beside it; Rust's `Sender` owns its
     client by value, so a spawned Sender takes the mock with it.
   - The three `testCloseIsForcedOn*` methods **do** need a spawned Sender, since their
     subject is `Sender::run`'s force-close tail. `tokio::task::spawn` onto the test's
     runtime deadlocks: `Sender::run` over a `MockClient` never awaits anything pending
     (`MockClient::poll` returns immediately), so the task never yields, and because
     only a worker parks on the time driver, **no timer in the runtime fires** — the
     test's own `sleep` never returns. Diagnosed from a thread sample (one worker
     spinning in `run_once`, the other in `park_condvar`). The harness gives the Sender
     its own OS thread and its own current-thread runtime via `spawn_blocking`, which is
     also closer to Java, where the Sender genuinely is a separate `ioThread`. A real
     `NetworkClient` cannot trigger this, because its `poll` awaits the selector.

     This is a Tokio-specific pitfall with no Java analogue and is worth adding to
     CLAUDE.md §9.6.6: *a task that never awaits anything pending starves every timer in
     a multi-thread runtime, not just its own worker.*

Not deviations, recorded because a reviewer may read them as such:

  - **`testCloseIsForcedOnPendingAddOffsetRequest`'s Java body is identical to
    `testCloseIsForcedOnPendingInitProducerId`'s** in 4.2 — one `FindCoordinator`
    prepared, then `initTransactions` — never reaching an `AddOffsetsToTxn` despite the
    name. Translated as written.
  - **`testSendTxnOffsetsWithGroupId` and `testSendTxnOffsetsWithGroupMetadata` pass an
    empty offsets map**, so `KafkaProducer.java:738` returns before anything is sent and
    three of the responses each queues are never consumed. Preserved, with the request
    count asserted unchanged so the no-op is the thing under test.
  - **A timed-out `TransactionalRequestResult` is not acked.** Java sets `isAcked` only
    after the latch opens (`TransactionalRequestResult.java:53-62`), which is what makes
    `initTransactions` retryable after a timeout *and* return the same result object.
    Rules §5 mandated the shape; two tests now pin the behaviour.
  - **Eleven of the fifteen transactional `SenderTest` methods are still owed**, with
    per-method reasons and Phase 8 named as owner, in the accounting block at the end of
    `src/producer/internals/sender.rs`. Two of the eleven are blocked on named missing
    surface (§9.18's split panic, and the one method given `mock(TransactionManager)`).

---

### 10.10 Phase 7 deviations (`MockProducer` transactional surface)

Each is documented at its call site as well.

1. **`ProducerFencedException` loses its wrapper on the `send` path.** Java's fenced
   `send` throws `KafkaException("MockProducer is fenced.", new
   ProducerFencedException("Fenced"))` (`MockProducer.java:293-295`) while
   `verifyNotFenced` throws the bare `ProducerFencedException("MockProducer is
   fenced.")` (`:256`). `KafkaError` has no cause chain (§10.5 deviation 5), so both
   collapse to one value: `Errors::ProducerFenced` carrying Java's message. That
   keeps both halves `shouldThrowOnSendIfProducerGotFenced` asserts —
   `assertThrows(KafkaException.class, ..)` and `assertInstanceOf(
   ProducerFencedException.class, e.getCause())`. What is lost is the distinction
   between the two Java shapes, and `is_api_exception()`, which reports `true` for
   the flattened value where Java's outer bare `KafkaException` is not an
   `ApiException`. Preserving the wrapper would need a cause chain on `KafkaError`,
   crate-wide and out of scope; preserving the code was the more valuable half,
   since it is what the test checks.

   Separately, the `Completion::complete` fix (error path now passes Java 578's
   −1-filled `RecordMetadata` alongside the error) makes the mock's C-surface
   behaviour match `KafkaProducer`'s `ApiException` path, where it previously
   differed. It also falsified one sentence in a *published* C contract:
   `kafka_producer_Producer_send_async`'s doc claimed "the other argument is null",
   which was already false for `KafkaProducer`'s path before this phase and is now
   false for the mock's too. Java is itself inconsistent across its three callback
   sites — `ProducerBatch.java:315` passes null metadata, `KafkaProducer.java:1060`
   and `MockProducer.java:578` do not — and each Rust site mirrors its own
   counterpart faithfully, so the doc was the thing that was wrong. Corrected to
   state the per-path truth and that the caller must free *every* non-null handle,
   in `src/ffi/producer.rs` and hence in the cbindgen-generated
   `target/include/confluent_kafka.h`.

2. **`flush()` moved to the inner type.** Java's `flush()` is `synchronized` and is
   called from the equally `synchronized` `commitTransaction` (`:214`) and
   `abortTransaction` (`:240`). A Java monitor is reentrant; `std::sync::Mutex` is
   not, so the body lives on `MockProducerInner` and runs with the caller's guard
   already held, with `Producer::flush` as the acquiring entry point. Same for
   `completeNext` / `errorNext`. No behavioural change on the paths the refactor
   touches: every public method now locks exactly once, and the four moved inner
   methods never lock, so no call sequence that worked before can deadlock now.

   **One residual divergence this does not remove, and is not trying to.** The user
   callback fired from `Completion::complete` runs *with the guard held*, so a
   callback that re-enters the mock (`producer.flushed()`, `producer.history()`)
   deadlocks where Java's reentrant monitor would allow it. That predates Phase 7 —
   both `send_with_callback` under `auto_complete` and `error_next` already fired
   callbacks under the guard — no Java test exercises it, and the obvious fix
   (release before the callback) would trade away the atomicity Java's `synchronized`
   guarantees, which is worse. Recorded rather than fixed.

   An earlier revision of this entry justified the refactor with "no `.await`, so
   nothing is held across a suspend point (CLAUDE.md §9.6.2)". That is true but
   answers the *async-runtime* hazard, not the *reentrancy* hazard this deviation is
   about; the two are independent, and only the second is why the body moved. Still
   worth stating on its own, so: the mock's critical sections
   contain no `.await`, so nothing is held across a suspend point (CLAUDE.md §9.6.2).

3. **`uncommittedOffsets()` returns a snapshot, not the live map.** Java hands back
   the field itself (`:484`) — asymmetric with `history()` and
   `uncommittedRecords()`, which copy. Behind the mutex a reference is not
   expressible, so the Rust accessor clones. No Java caller mutates the returned
   map.

4. **`Objects.requireNonNull` guards have no counterpart.** Java's
   `sendOffsetsToTransaction` opens with `Objects.requireNonNull(groupMetadata)`
   (`:184`); the parameter is taken by value and is not an `Option`, so a missing
   metadata is unrepresentable. This is why
   `shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction`
   (`MockProducerTest.java:430`) is one of the two NOT APPLICABLE entries in the
   test accounting block — and note the `NullPointerException` it asserts is not
   even raised by the mock: it comes from `new ConsumerGroupMetadata(null)` in the
   lambda — the one-arg constructor at `ConsumerGroupMetadata.java:52`, delegating to
   the four-arg one declared at `:38`, whose `Objects.requireNonNull(groupId, ..)` is
   at `:42`.

5. **`ConsumerGroupOffsets` type alias.** Spells Java's `Map<String,
   Map<TopicPartition, OffsetAndMetadata>>` (`:63`, `:68`) once instead of four
   times. An alias, not a new type, so it adds no struct absent from Java
   (`definition-of-done.md` §7). Rules §10's sort-for-determinism does not apply:
   these maps are never serialised, and §10 says explicitly not to order collections
   that never reach a `write()`.

6. **Three Rust-only tests.** `test_uncommitted_accessors`,
   `test_clear_resets_staging_but_not_transaction_flags` and
   `test_set_transactional_errors` cover `uncommittedRecords` (`:471`),
   `uncommittedOffsets` (`:483`) and the five transactional `*Exception` fields
   (`:79-83`) — each of which appears **zero** times in `MockProducerTest.java`, its
   Java callers being Kafka Streams tests (out of scope per §1.1). Added rather than
   left untested, following the convention the file already had for the four
   non-transactional `*Exception` knobs.
