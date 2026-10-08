# Milestone 16 — Bring the Rust client up to Apache Kafka 4.4

**Status:** DRAFT 2026-10-07 — awaiting user approval. Phases run through the Manager → Actor → Critic loop.

**Branch:** `milestone-16-ak-4.4` (off `master`). If tracks run in parallel (§4), each track gets its own
branch/worktree off the milestone branch and is merged back with a merge commit — never rebase + force-push.

**Java source:** Apache Kafka **4.4.0-rc4** (`1156b2752a`, tagged 2026-10-06). `4.4.0-rc3..4.4.0-rc4` has
**no** change under `clients/src/main`, so the repo's existing `AUDIENCE_REF` / `DEPRECATION_REFS`
(`4.4.0-rc3`, `rust/xtask/src/java.rs`) stay valid. Phase 13 re-diffs against `4.4.0` final once it is tagged.

**Agent numbers:** 90–103 (Phase N → agent 90+N). The highest number used so far is 86.

## 1. What this milestone delivers

It translates the 4.3.1 → 4.4 Java clients delta for every Java file that has a Rust counterpart. It also
syncs `rust/generator/messages/` to the 4.4 specs, adds the new public API to the C and Python bindings,
and moves the `kafka/` submodule and the docs' source-reference line to 4.4.

**Measured delta** (`git -C kafka log --no-merges --right-only --cherry-pick 4.3.1...4.4.0-rc4`):

- 848 unique commits overall. **190 touch `clients/src`** (168 touch main code). On the M13 metric
  (`-- clients/src/main/java/org/apache/kafka/clients`) there are 116 commits, against 81 for 4.2.0 → 4.3.1.
- **68 commits to port**: 55 behaviour/API/wire (P) and 13 test-only (T). One more, the trunk-only
  KAFKA-20864, is added by decision D3.
- The rest:
  - **56 out of scope:** Streams 24, broker/server 13, Share 11, Classic 5, untranslated areas 3.
  - **37 no-op for Rust.**
  - **21 doc/log-only.**
  - **6 reverted inside the range:** KAFKA-20385/20684 RebalanceListener (it returns in 4.5), and 2PC.
  - **2 already ported in M13**, because they were backported to 4.3.1 under different hashes:
    KAFKA-20426 `44bafc60e7` and KAFKA-20428 `4b9eddc132`.
- `rust/generator/messages/` currently matches 4.3.1 **exactly**: 18 spec files differ from 4.4, 5 of them new.

The per-commit classification table is §6. Phase 13 completes it into the formal audit.

### 1.1 Scope decisions

- **Skip untranslated areas**, as M13 did: Share (KIP-932), Streams-protocol consumer internals and Streams
  admin (`describeStreamsGroups` topology description), the Classic consumer, broker/server code
  (authorizer, quota callback, policy, OAuth broker validator, `FileRecords`), the Raft-voter admin API
  (`RaftVoterEndpoint`), `ListDeserializer`, config providers (`AllowedPaths`), `TopicConfig` constants, and
  telemetry.
- **Include KIP-1332 incremental buffer allocation** (KAFKA-20578), user-confirmed 2026-10-07, with the type
  mapping in §2.3.
- **The KIP-1265 `@InterfaceAudience` annotations are already applied** (`AUDIENCE_REF = 4.4.0-rc3`). This
  milestone adds no audience work beyond keeping the lint green for the new public types.
- **The CLAUDE.md "Source Reference" line edit (4.3.1 → 4.4.0) is human-approved through this plan**, as in
  M13. Only that line changes. Any other CLAUDE.md or rules change goes through the suggestion process
  (drafts go in `design/current/Milestone-16/rules-errata.md`).

### 1.2 Method: per-subsystem tree diff, with the commit list as a checklist

As in M13, Actors translate from the tree diff (`git -C kafka diff 4.3.1 4.4.0-rc4 -- <files>`), not by
replaying commits one by one. Each phase lists its KAFKA-ids so the Actor has the rationale and the Critic
can audit completeness. Every Java hunk in a phase's files is either applied or recorded as a skip with a
reason in that phase's completion notes.

### 1.3 Open decisions (defaults used by this plan unless the user overrides)

| # | Decision | Default |
|---|---|---|
| D1 | Target | Start on **4.4.0-rc4** now; Phase 13 re-diffs and bumps refs to **4.4.0 final**. |
| D2 | Java's `common.utils` → `common.utils.internals` package moves (KAFKA-20297) | **Mirror them** (M13 precedent: `record` → `record::internal`). In Rust this affects only `byte_utils`, `exponential_backoff`, `log_context`, `producer_id_and_epoch`. |
| D3 | KAFKA-20864 (trunk-only fix to the 4.4 incremental-allocation code: closing the wrong batch on extension failure, unbounded retries) | **Include it** as a recorded deviation ahead of 4.4. Otherwise the 4.4 bug would be ported faithfully. |
| D4 | Representing `UnsupportedProtocolFieldException` (crate-private, `extends UnsupportedVersionException`) | Model it as a crate-private kind on the existing `UnsupportedVersion` variant, so the public `Error` surface still matches Java's public view (§2.4). |

## 2. Key findings that shape the phases

### 2.1 KIP-909 (async bootstrap DNS) is the riskiest change

KAFKA-14648 with its follow-ups (KAFKA-20939, the KIP-909 follow-ups) adds:
- `bootstrap.resolve.timeout.ms` and `BootstrapConfiguration`;
- `BootstrapResolutionException`;
- lazy bootstrap in `NetworkClient` / `MetadataUpdater` (`bootstrap`, `isBootstrapped`, `bootstrapFailed`);
- `Metadata.bootstrapFatalError` / `maybeThrowBootstrapFatalException`;
- `AdminMetadataManager` changes, and the removal of `AdminBootstrapAddresses`.

Java needed three follow-up fixes for consumer busy loops it caused (KAFKA-20854/21010/20970).

- **Default:** `0` keeps today's synchronous resolve-at-construction behaviour. A positive value switches
  to asynchronous resolution, and the setting is marked experimental.
- **Rust specifics:** `ClientUtils::resolve` is already `async`. The new risk is Rust-only: async resolution
  must never sit inside a `select!` arm (CLAUDE.md §11.6, consumer-threading §10). The consumer busy-loop
  fixes must be checked against the Rust `maximum_time_to_wait` / poll-timer logic, not just transcribed.

### 2.2 TxnOffsetCommit v6 must not go live before the producer can fill it

KIP-1319 v6 swaps topic **names** for topic **IDs** in TxnOffsetCommit. If the v6 spec were synced in
Phase 0 while the builder still populated names, the client would negotiate v6 and send requests without
topic IDs.

**Rule:** Phase 0 syncs every spec **except** `TxnOffsetCommitRequest.json` / `TxnOffsetCommitResponse.json`.
Those are synced in Phase 5, together with the builder and `TransactionManager` changes. The builder's
`latest_allowed_version` follows Java's `super(...)` call (producer-transactions.md §12).

`DeleteGroups` v3, `ApiVersions` v5 and `DescribeProducers` (`mapKey`) are safe to sync in Phase 0: their
new fields are ignorable, or default to null/-1.

### 2.3 KIP-1332: how Java's subclasses map to Rust

Java adds `ChunkedRecordAccumulator extends RecordAccumulator` and `ChunkedProducerBatch extends ProducerBatch`.
Rust has no inheritance, so each is mapped separately:

- **`ChunkedRecordAccumulator`: a real struct, by composition.** This follows the existing precedent
  `ConsumerHeartbeatRequestManager { inner: AbstractHeartbeatRequestManager }`.
  - The struct is `ChunkedRecordAccumulator { base: Arc<RecordAccumulator>, .. }` in
    `chunked_record_accumulator.rs`, with its own `append` / `try_append`.
  - `Sender` keeps `Arc<RecordAccumulator>` and does not change. Its accumulator calls (`ready`, `drain`,
    expiry, `reenqueue`, `deallocate`) are all inherited unchanged in Java. In production, `append` is
    called only from `kafka_producer.rs` (around `:1861`), and that one site dispatches on the strategy.
  - Java's virtual `tryAppend` / `createProducerBatch` inside the base `appendNewBatch` become parameters
    of the base `append_new_batch`.
- **`ChunkedProducerBatch`: folded into `ProducerBatch` (a justified deviation, recorded per DoD #7).**
  - One partition deque holds both kinds of batch: split batches stay plain even in incremental mode.
  - `ProducerBatch` is not `Clone` and moves by value between the accumulator, `Sender` and
    `TransactionManager` (producer-transactions.md §7).
  - So `ProducerBatch` carries a single-or-chunked buffer. The Java methods live in
    `chunked_producer_batch.rs` as an `impl ProducerBatch` block, and `instanceof ChunkedProducerBatch`
    becomes `batch.is_chunked()`.
- **The builder's buffer abstraction** (a single `Vec<u8>` or a `ChunkedByteBufferOutputStream`) is the one
  new Rust type. It matches Java 4.5's abstract `ByteBufferOutputStream` / `SingleByteBufferOutputStream`
  (KAFKA-20807), so the shape is already right for the next bump.
- **Flatten copy:** the Rust builder already copies once at close (`take_batch_data`, a documented §14
  deviation). The chunked path makes that same single copy, so it is no regression. Do **not** send
  straight from the chunks with `IoSlice` yet: that is Java's future KAFKA-20580, and doing it now would
  change when chunks can be returned to the pool.
- **Rust-only hazards:**
  - A chunk `Vec` must never grow. Java's fixed-capacity `ByteBuffer` throws on overflow, whereas a Rust
    `Vec` would silently break the memory accounting.
  - A cancelled `append` future must refund new-batch and extension chunks (extend `AppendGuard`).
  - `reopen_and_rewrite_producer_state` must rewrite the header of the flattened buffer.
- **Draft a rules note** (in `rules-errata.md`, applied by a human) recording the batch fold, so Critics
  don't flag the missing type.

### 2.4 `UnsupportedVersionException` gains a subclass

KAFKA-18157 adds `common.internals.UnsupportedProtocolFieldException extends UnsupportedVersionException`.
About 15 request builders throw it, and `ConsumerHeartbeatRequestManager` tells it apart from its parent.

- The class is not public. Under D4 the public `Error` keeps one `UnsupportedVersion` variant, with a
  crate-private kind that the heartbeat manager checks.
- The Phase 1 Critic must confirm whether CLAUDE.md §12.4 then requires an `is_unsupported_version_error()`
  predicate (plus a test in both directions and the C FFI twin). If the hidden-kind design is rejected,
  the predicate becomes mandatory.

### 2.5 New public API → bindings

| Item | Rust | C FFI | Python |
|---|---|---|---|
| `Admin.unregisterController(int[, options])` + `UnregisterControllerOptions/Result` | `admin` | sync + async | `admin.py` sync + asyncio |
| `MockConsumer.losePartitions` | `consumer::MockConsumer` | `kafka_consumer_MockConsumer_lose_partitions` | `consumer.py` mock |
| Errors 134 `GROUP_DELETION_FAILED`, 135 `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED`, 136 `CONTROLLER_ID_NOT_REGISTERED`; `BootstrapResolutionException` (non-wire) | `common::errors` | error code / predicates as applicable | `_error_code.py` |
| Configs: `bootstrap.resolve.timeout.ms`, `metadata.cluster.check.enable`, `partitioner.rack.aware`, producer `client.rack`, internal `buffer.memory.allocation.strategy` | config structs | pass-through | pass-through |

Each binding change lands in the phase that adds the Rust API.

The configs must be wired end to end, not just parsed (see the 2026-09-30 config audit,
`design/current/config-audit-master-7ac1391b.md`: silently ignored configs were the main finding class).

## 3. Phases

Every phase works the same way:
- The Actor (agent 90+N) implements and commits step by step.
- The Critic (agent 90+N) reviews into `COMMENTS.9N.md` (`COMMENTS.10N.md` for N ≥ 10).
- The Actor fixes, moves the items to `COMMENTS.DONE.*`, and the loop repeats until the review is clean.
- Gates for every phase: `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`; then
  `make verify` before the phase closes.

### Phase 0 — Reference bump, spec corpus, errors, bookkeeping (agent 90)

- Move the `kafka/` submodule to `4.4.0-rc4` (`1156b2752a`) and commit the gitlink.
- Sync `rust/generator/messages/` from `kafka/clients/src/main/resources/common/message/`: 16 of the 18
  changed files, holding back the TxnOffsetCommit pair (§2.2).
  - New: `UnregisterController{Request,Response}`, `StreamsGroupTopologyDescriptionUpdate{Request,Response}`,
    `AbortedTxn`.
  - Modified: `ApiVersions` v5, `DeleteGroups` v3, `DescribeProducersRequest` `mapKey`,
    `ConsumerGroupHeartbeatResponse`, `ShareGroupHeartbeatResponse`, and the Streams specs.
  - Regenerate, then fix compile fallout. Expected: `DescribeProducersHandler` (keyed collection) and the
    `ApiKeys` / `ConcreteRequest` / `ConcreteResponse` match arms for the two new APIs (admin-client.md §7).
    Only UnregisterController gets a real wrapper, in Phase 12. StreamsGroupTopologyDescriptionUpdate gets
    only whatever minimal wiring the existing Streams APIs have, and that has to be checked.
- `Errors`:
  - add codes 134–136 and the three error types (`GroupDeletionFailedError`,
    `StreamsTopologyDescriptionUpdateFailedError`, `ControllerIdNotRegisteredError`);
  - add `BootstrapResolutionError` (non-wire, needed by Phase 2);
  - change the `FENCED_STATE_EPOCH` message text (8ba75d04cb);
  - update `python/_error_code.py` and any C-side code list.
  - The full-error-code hierarchy tests (`test_retriable_errors_match_java_hierarchy` and its siblings)
    must cover the new codes.
- Update `README.md` and the `CLAUDE.md:93` source line "(Apache Kafka 4.3.1)" → "(Apache Kafka 4.4.0)".
- Rules errata survey: re-check the Java `file:line` citations in `.claude/rules/*.md` against 4.4, chiefly
  `producer-transactions.md` (TransactionManager moves under KIP-1319) and `consumer-threading.md`. Write
  the list to `design/current/Milestone-16/rules-errata.md`.
- Commits covered: 7997c9ebe0 (spec + errors portion only; the Streams RPC itself is out of scope),
  c274a7348f (spec + error portion), 8ba75d04cb, 04c3c00f25, 89ccd6a126 / 6be48c6e54 / 70dfc4236c
  (spec-only syncs).

Phase 0 completion notes (agent 90):

- **Submodule:** `kafka/` gitlink moved to `4.4.0-rc4` (`1156b2752a`).
- **Spec sync:** 16 of the 18 differing specs copied into `rust/generator/messages/`; the corpus now
  differs from 4.4.0-rc4 only in `TxnOffsetCommitRequest.json` / `TxnOffsetCommitResponse.json`, held
  back for Phase 5 (§2.2).
  - **No hand-written compile fallout.** The Rust generator emits every collection as a `Vec` (it parses
    `mapKey` into `StructSpec::has_keys` but never emits a keyed collection), so the
    `DescribeProducersRequest` `mapKey` change does not alter the generated shape and
    `DescribeProducersHandler` is untouched. Java's handler change there is the `CollectionUtils`
    removal (a1c155ee28), a refactor the Rust `HashMap` grouping already matches.
  - **New APIs:** `UnregisterController` (94) and `StreamsGroupTopologyDescriptionUpdate` (93) get the
    wiring the untranslated Streams/Share/broker APIs already have: `ApiKeys` constants
    (`UNREGISTER_CONTROLLER` is `forwardable`, per `ApiKeys.java:142`), entries in `ApiKeys::ALL`, and
    `assert_message_version!` rows in `message_test.rs`. No `ConcreteRequest`/`ConcreteResponse`
    variant: both fall through to the existing "not currently handled" arm, as `STREAMS_GROUP_HEARTBEAT`
    does. **Phase 12** adds the UnregisterController variants and wrappers.
  - `AbortedTxn` is a broker-side data spec; the generated `AbortedTxnData` has no client caller.
  - **Byte-level tests** (DoD #3): ApiVersions request v5 (defaults, both fields set, v4 drops them,
    v5 parse), response v4 == v5; DeleteGroups request v3 == v2, response v3 (message set, null, and
    dropped at v2). Also translated `RequestResponseTest.testDeleteGroupsResponseV3PreservesErrorMessage`.
- **Errors:** 134 `GroupDeletionFailedError`, 135 `StreamsTopologyDescriptionUpdateFailedError`,
  136 `ControllerIdNotRegisteredError` (all `extends ApiException`, not retriable, not fatal) and the
  non-wire `BootstrapResolutionError` (`extends KafkaException`, so only `is_kafka_error`). Each has its
  own file and an `Error` variant. `FENCED_STATE_EPOCH` now carries Java's generic text (8ba75d04cb).
  No new intermediate Java class, so no new hierarchy predicate.
  - **Tests:** every full-code walk now covers `-1..=136` (138 constants), and the FFI tables cover 166
    codes. `test_errors_added_or_reworded_in_kafka_4_4` pins the exact messages.
  - **Bindings:** `kafka_common_ErrorCode_t` gains 134-136 and `BOOTSTRAP_RESOLUTION = -29`, appended at
    the most-negative end because the negatives are ABI. `python/_error_code.py` and
    `rust/tests/common/error_code.rs` are regenerated from it, and the multilanguage decoder maps -29.
- **Docs:** `CLAUDE.md` Source Reference line and `rust/README.md` now say 4.4.0. The repository-root
  `README.md` names no Kafka version, so it has nothing to change.
- **Rules errata:** `design/current/Milestone-16/rules-errata.md`. 46 citations: 36 drifted, 10
  unchanged, and none whose rule claim breaks. It also lists 4.4 changes to code the rules reason about
  without a line cite: KIP-1319 vs producer-transactions §10-§12 (relevant to **Phase 5**), the
  MockAdminClient move and its new in-memory methods vs admin-client §9 (**Phase 12**), and KIP-909
  AdminMetadataManager state vs admin-client §3 (**Phase 2**). It reserves a section for the §2.3
  KIP-1332 note (**Phases 7/8**).
- **Unplanned fallout of the submodule bump: the custom lint.** `cargo xtask lint-custom` resolves every
  `#[doc(alias = "org.apache.kafka...")]` marker against the `kafka/` working tree, so the bump produced
  158 findings. Phase 0 fixed the 113 that are pure relocations, each true at 4.4 on its own:
  - 101 `KafkaAdminClientTest` markers re-pointed to the per-domain classes of 1a443b2d23. **Phase 12:**
    the mapping is done.
  - xtask now also indexes `clients/src/testFixtures` (3b6c8385ca moved `MockTime` and
    `MockAdminClient` there).
  - `Node`'s public constructors are marked with explicit overloads. 7be741d08b added a protected
    `idString` overload; translating it is **Phase 4** work.
  - `SubscriptionState::has_partitions_needing_validation` is removed, mirroring 455cbdfea2.

  **45 findings remain**, each tied to a behavioural or module change that a later phase owns:

  | Findings | Change | Owner |
  |---|---|---|
  | 25 | utils → `utils.internals` moves: `ByteUtils`, `ExponentialBackoff` (+ its test), `LogContext`, `ProducerIdAndEpoch` (D2) | Phase 1 |
  | 5 | `shouldClientThrottle` overrides removed (KAFKA-20828) | Phase 1 |
  | 2 | `TxnOffsetCommitRequest.getErrorResponseTopics` (baa064e422) and `TxnOffsetCommitResponse.errors` (89f3888c87) removed, held back by §2.2 | Phase 5 |
  | 2 | `ProducerBatch.isWritable` and `RecordAccumulator.recordsBuilder` (KAFKA-20578) | Phases 7/8 |
  | 1 | `ConsumerMembershipManager.onHeartbeatSuccess` (KAFKA-20681) | Phase 9 |
  | 10 | `SensorBuilder` → `consumer.internals.metrics` (KAFKA-19542) | Phase 11 |

  So `cargo xtask lint`, and with it `make verify`, cannot pass at the end of Phase 0 without pulling
  in other phases' work, which for the TxnOffsetCommit pair §2.2 forbids. Each owning phase must clear
  its rows. The user's gate decision and the per-finding table are in §5.1.
  - The `ApiVersionsRequest` changes that go with v5 (`setClusterId`/`setNodeId` and the both-or-neither
    check in `isValid`, 0ef4a4c80e) are left to **Phase 4**. Until then the client sends v5 with the
    defaults (`null` / -1), which the check accepts.
  - **Spec portions already applied (Phase 4 Critic, Phase 13 audit: do not count as missing).** The
    corpus sync landed the `ApiVersionsRequest.json` / `ApiVersionsResponse.json` parts of all three
    KAFKA-20246 commits: ede01b871e (v5 with the `ClusterId` / `NodeId` fields, marked unstable, 1/N),
    0ef4a4c80e (the fields' "provide both" docs, 2/N) and 7be741d08b (v5 marked stable, 3/N). What Phase 4 still owes
    from them is the Java code only, exactly as listed in the Phase 4 section below; Phase 0 applied
    none of it.
- **Skips:**
  - Streams-only spec content (StreamsGroupDescribe/Heartbeat v1, the topology description RPC) is
    synced but has no wrapper: out of scope (§1.1).
  - Java `MessageTest` / `RequestResponseTest` deltas for TxnOffsetCommit v6 (**Phase 5**), the
    tagged-field/array caps (**Phase 1**), `shouldClientThrottle` (**Phase 1**), and UnregisterController
    / Streams fixtures (**Phase 12** / out of scope) are left to their phases.
- **Verification:**
  - **Passing:** `cargo build`; `cargo test` (4285 passed, 0 failed, 10 ignored); `cargo xtask
    format-check`; `check-generated`; the three clippy passes and `doc-hygiene`, run one by one because
    `xtask lint` stops at the custom lint.
  - **`make -k verify`** (2026-10-07, macOS):
    - `build-c` fails with `cmake: command not found` (host limitation).
    - `lint` fails with exactly the 45 findings in the table above.
    - `test-rust-all-features`: 4462 passed, 17 failed. All 17 are Docker-backed `integration_tests::*`
      (the Docker daemon was not running), and cargo then skipped the remaining test targets.
    - Python unit tests: 363 passed, 2 skipped. `check-bindings` / format-arity: 29 passed. Soak tests:
      156 passed.
  - **Owed:** a real-broker run of the integration suite, in particular `api_versions_test` /
    `connection_test`, which now negotiate ApiVersions v5 (a pre-4.4 broker answers v5 with
    UNSUPPORTED_VERSION and the client retries at the broker's highest version).

### Phase 1 — Common and wire foundations (agent 91)

- **D2 module moves:** `common::utils::{byte_utils, exponential_backoff, log_context, producer_id_and_epoch}`
  → `common::utils::internals`, crate-private, with the Java package docs carried over. Fix imports across
  the crate. This goes first because it renames imports everywhere.
- **Generated-reader hardening** (f66a67fcef, 1a770734fe):
  - `MessageUtil` array-length limits, plus bounded array and tagged-field allocation in generated readers
    (`rust/generator/src/lib.rs`, `common/protocol/message_util.rs`, schema `ArrayOf` / `CompactArrayOf` if
    a counterpart exists).
  - Tests: `MessageTest` / `SimpleArraysMessageTest` additions, `SimpleKeyedArraysMessage.json`
    (dedicated per-message test file — DoD #3), `RequestHeaderTest`, `RequestContextTest`,
    `ProtocolSerializationTest`.
- **KAFKA-18157** (239a3e4990): `UnsupportedProtocolField` per D4 / §2.4, the ~15 request builders, and
  the `ConsumerHeartbeatRequestManager` branch. Tests: `NetworkClientTest` and
  `ConsumerHeartbeatRequestManagerTest` hunks, with exact error messages asserted.
- **KAFKA-20828** (63f445aaa9): `AbstractResponse` throttle time is derived from the response schema;
  remove the per-response overrides in the Rust wrappers that have counterparts. Test: `RequestResponseTest`
  (+55).
- **KAFKA-20072** (474798afbe): `Uuid::random` must never produce an ID that starts with `-` (or contains
  hyphens, per the Java change). Test: `UuidTest`.
- `KafkaMetric` `Display` (46ad599a6e) + `KafkaMetricTest`; `ByteUtilsTest` split (8b6d31f00c).
- Hot-path note (DoD #10): the reader bounds sit on the receive / response path; confirm there is no new
  per-record allocation.

Phase 1 completion notes (agent 91):

- **D2 moves (KAFKA-20297).** `byte_utils`, `exponential_backoff`, `log_context` and
  `producer_id_and_epoch` now live in `rust/src/common/utils/internals/`, a `pub(crate)` module whose
  docs carry the parent package's description of these classes (Java ships no `package-info.java` for
  `utils.internals`). Every import, the generator's emitted paths and the doc-alias markers use the new
  package. The 25 §5.1 rows are gone.
- **Generated-reader hardening (1a770734fe, f66a67fcef).** `MessageUtil` gains
  `MAX_PREALLOCATED_ARRAY_CAPACITY` / `MAX_ARRAY_LENGTH` / `MAX_TAGGED_FIELD_COUNT`. Every generated array
  reader (plain and tagged, nullable or not) rejects a length above `MAX_ARRAY_LENGTH` after the
  remaining-bytes check and pre-allocates at most 1000 elements. Every tagged-field section read (the
  known-tag path and both unknown-only paths) rejects a negative count, a count above the remaining bytes
  and a count above `MAX_TAGGED_FIELD_COUNT`, with Java's messages. The count is read as `i32`, as Java's
  `int`, so the negative guard is reachable.
  - Tests: `MessageDataGeneratorTest` (3, on emitted source), `MessageTest` (2),
    `SimpleArraysMessageTest` (4) with the new `generator/test-messages/SimpleKeyedArraysMessage.json`,
    `RequestHeaderTest` (1), `RequestContextTest` (3).
  - **DoD #10:** no new allocation. The reader gains two integer compares per array and three per tagged
    section, and the capacity is `min(length, 1000)`. An array of more than 1000 elements now regrows
    while it is read; that is per response, not per record (record payloads are a `records` byte field,
    not an array).
- **KAFKA-18157 (239a3e4990), PLAN D4.** `UnsupportedProtocolFieldException` is a crate-private
  `UnsupportedVersionKind` on the existing `UnsupportedVersionError` (`kafka_error_type!` gains an optional
  `kind:`). Java's class is `common::internals::UnsupportedProtocolFieldError`: its two constructors
  (`with_options`, with an Options builder per §2's three-parameter cap, and `with_message`) and Java's
  `instanceof` (`is_unsupported_protocol_field_error`).
  - **Throw sites ported (14 of Java's 20):** CreateTopics ×2, ConsumerGroupHeartbeat, ElectLeaders,
    FindCoordinator, ListGroups ×2, Metadata ×3, OffsetCommit, OffsetFetch, plus ListTransactions ×2. The
    ListTransactions pair is new code: its 4.3.1 `DurationFilter` / `TransactionalIdPattern` checks had
    never been ported. The 6 skipped sites are listed under the skips below.
  - **Plumbing.** For `instanceof` to work, the error object has to travel as it does in Java. A builder
    carries it inside its `io::Error` (`UnsupportedVersionError::into_io_error` / `from_io_error`, the
    `CorrelationIdMismatchError` precedent). `ClientResponse.version_mismatch` holds an
    `UnsupportedVersionError` instead of a `String`. `NetworkClient`, `NetworkClientDelegate`, `Sender`
    and the admin runnable pass it on unchanged. `ConsumerHeartbeatRequestManager.handleSpecificFailure`
    dispatches on the class, keeps the subclass's message, and wraps the cause as Java does.
  - **CLAUDE.md §12.4: predicate added.** §12.4 says "every **intermediate** (non-leaf) class in Java's
    error hierarchy MUST be recoverable as a predicate on `Error`". It makes no exception for a class whose
    subclasses are not public. `UnsupportedVersionException` has subclasses: the new one, and the
    `NoBatchedFindCoordinatorsException` / `NoBatchedOffsetFetchRequestException` nested in the request
    classes, so it was already intermediate in 4.3.1. D4 only decides how the *subclass* is represented,
    and the hidden kind does not make the parent a leaf. So `Error::is_unsupported_version_error()` exists
    now. It covers exactly the `UnsupportedVersion` variant, every kind included, and nests inside
    `is_invalid_configuration_error`. It is pinned in both directions:
    - over every code in `errors.rs`'s `test_hierarchy_predicates_match_java`;
    - in the nesting tests;
    - as a new column, with rows for both kinds, in `error.rs`'s `variant_predicates_match_java_hierarchy`.

    Its C twin is `kafka_common_Error_is_unsupported_version_error`. It has a Rust test over every code
    and lines in `c/tests/test_mock_producer.c`. Python exposes only retriable / fatal /
    transaction-abortable, so Python has no counterpart to add.
  - Tests: `UnsupportedProtocolFieldExceptionTest` (3); `NetworkClientTest`
    `testUnsupportedVersionDuringInternalMetadataRequest`, which now asserts the recorded failure's kind
    (`TestMetadataUpdater` got a shared failure handle); `ConsumerHeartbeatRequestManagerTest`
    `testUnsupportedVersionFromClient`, both cases plus two Rust-side ones showing the dispatch is on the
    class, not the text; `RequestResponseTest.testCreateTopicRequestV3FailsIfNoPartitionsOrReplicas`; a
    `NetworkClientDelegate` test; exact messages at every ported site.
- **KAFKA-20828 (63f445aaa9).** `ConcreteResponse::should_client_throttle`, the `AbstractResponse`
  dispatch, delegates for the 33 types whose Java class overrides the method. For the other 19 it
  computes the schema default (`throttle_time_ms` present at the version). Those 19 lose their per-type
  methods: the five Java removed and fourteen Rust stand-ins for the old inherited `false`.
  The ones that change from `false` to `true` are ConsumerGroupHeartbeat, ConsumerGroupDescribe,
  OffsetsForLeaderEpoch (v2+), UpdateFeatures, Describe/AlterClientQuotas, DescribeProducers,
  DescribeTransactions and ListTransactions; all match Java 4.4. (DescribeCluster already answered
  `true`, which matches its schema.) The 5 §5.1 rows are gone.
  - Test: `RequestResponseTest.testClientThrottlesResponsesWithThrottleTime` over every `ConcreteResponse`
    type and version (each parsed from its default data). It found a **pre-existing divergence**:
    `DeleteTopicsResponse` throttled from v1, Java from v2 (`DeleteTopicsResponse.java`, unchanged since
    4.3.1). Fixed.
- **KAFKA-20072 (474798afbe).** `Uuid::random_uuid` rejects any base64 string containing `-`;
  `testRandomUuid` keeps its 100 repetitions.
- **KafkaMetric `Display` (46ad599a6e).**
  - Rust has no reflection, so `Measurable` and `Gauge` gain a defaulted `type_name()` (DoD #7).
    It returns `std::any::type_name` of the implementor. The closure adapters answer `None`, as Java
    omits lambdas.
  - Sensor's internal `MeasurableArc` forwards to the stat it shares.
  - `MetricName`'s `Display` now renders tags as Java's `Map.toString` does (`{k=v}`), not as Rust
    `Debug`.
  - Tests: the three `KafkaMetricTest` cases, plus a sensor-registered stat.
- **ByteUtilsTest split (8b6d31f00c).** `Bytes.increment` / `BYTES_LEXICO_COMPARATOR` moved into the
  translated `ByteUtils`, so they are translated with it (DoD #2). `Bytes` has no Rust counterpart, so
  the API takes slices and returns `Vec<u8>`, whose `Ord` is `Bytes`' unsigned lexicographic order.
  - Tests: `testIncrement`, `testIncrementUpperBoundary`, `testIncrementWithSubmap`,
    `testBytesLexicographicCases`.
- **Recorded skips and deviations:**
  - `ProtocolSerializationTest.testReadArrayLengthAboveMaxIsRejected` /
    `testReadCompactArrayLengthAboveMaxIsRejected`, and the `ArrayOf` / `CompactArrayOf` half of
    f66a67fcef: there is no Rust schema reader. `protocol::types::Schema` only describes fields; the
    generated readers are the only decoders, and they carry the same limits.
  - `MessageDataGeneratorTest`: Java asserts the `ArrayList`, `BazCollection` and `BamCollection` forms
    separately. The Rust generator emits every collection as a `Vec`, so the test asserts the one capped
    form three times.
  - `RequestContextTest`: `RequestContext` is the broker's view of a request and has no client
    counterpart. The three cases run against the `ProduceRequestData` reader that
    `RequestContext.parseRequest` drives. Where Java asserts the `InvalidRequestException`'s cause, they
    assert the reader's error; where Java asserts only the class, they assert that the read fails.
  - KAFKA-18157, not ported:
    - the CreateAcls / DeleteAcls / DescribeAcls v0 guards: v0 was removed in Kafka 4.0
      (`validVersions` 1-3), the Rust builders already omitted these unreachable guards, and 4.4
      changes only their exception class;
    - the Heartbeat / JoinGroup / SyncGroup builders: classic protocol, no Rust builder
      (consumer-threading §20);
    - Streams' `InternalTopicManager`: out of scope.
  - `KafkaMetricTest`:
    - `testToStringWithStatProvider` asserts the Rust type path
      (`confluent_kafka::common::metrics::stats::avg::Avg`) where Java names the Java class;
    - `testToStringWithAnonymousClassProvider` uses `ClosureGauge`: Rust has no anonymous classes, and
      an inline provider is a closure adapter;
    - `testConstructorWithNullProvider` stays untranslated: a null provider is unrepresentable.
  - `ByteUtils`:
    - `increment` returns `LocalIllegalArgument` where Java throws `IndexOutOfBoundsException`, the
      crate's precedent for that class (`MockAdminClient`);
    - the comparator's offset/length overload is the slice method called with sub-slices.
- **Review round 1 fixes (COMMENTS.91, all moved to `COMMENTS.DONE.91.md`):**
  - **B1:** `generator/test-messages/SimpleKeyedArraysMessage.json` was git-ignored (`*.json`) and never
    committed. It is now force-added, as its siblings are. No other Phase 1 file is ignored.
    Re-verified from a clean `git archive HEAD rust` export with a fresh `CARGO_TARGET_DIR`:
    `cargo test --lib` builds and passes there (4261 passed, 0 failed, 3 ignored).
  - **L1:** a produce batch failed by a version mismatch now gets Java's message-less
    `PartitionResponse(UNSUPPORTED_VERSION)` (`Sender.java:599`), so the callback sees the default text.
    `testUnsupportedVersionInProduceRequest` asserts it.
  - **L2:** all 15 generated "zero-fill then `read_bytes`" sites now go through `read_array`, which checks
    the remaining bytes before it allocates. Compact string readers gain Java's `> 0x7fff` guard
    ("string field <camelCaseName>[ element] had invalid length <n>", Java's order). There is no new
    allocation (DoD #10): one allocation and one copy where there used to be an allocation, a memset and
    a copy. Covered by a generator test and two runtime tests.
  - **L3:** the stale `utils.ProducerIdAndEpoch` / `common/utils/byte_utils.rs` references are fixed in
    `python/admin.py`, `admin_service.proto`, `status.md` and an integration-test comment. All are
    hand-written; none is generated.
  - **Manager (a):** b69c07c816 is reclassified to Consumer/P (partial) and added to Phase 10, not
    implemented.
  - **Manager (b):** `cargo xtask lint --keep-going` runs every lint step even after one fails, reports
    every failure, and fails at the end if any step did. Its first run on Phase 1 found 3
    `module_path_hygiene` findings: literal `stats::avg::Avg` type paths in the `KafkaMetric` `Display`
    doc and tests. They are fixed. With `--keep-going` the result is: lint-custom fails with exactly the
    15 remaining §5.1 rows; doc-hygiene, module-path-hygiene and all three clippy passes are clean.
    **Later phases should gate on `cargo xtask lint --keep-going`.**
- **Timing log** (2026-10-07, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 1 D2 moves | 16:49 | 16:55 | 6 |
  | 2 reader hardening | 16:55 | 17:03 | 8 |
  | 3 KAFKA-18157 + predicate + FFI | 17:03 | 17:27 | 24 |
  | 4 KAFKA-20828 (+ clippy fixup) | 17:27 | 17:32 | 5 |
  | 5 KAFKA-20072 | 17:32 | 17:33 | 1 |
  | 6 KafkaMetric Display | 17:33 | 17:36 | 3 |
  | 7 ByteUtilsTest split | 17:36 | 17:38 | 2 |
  | 8 gates, notes, `make verify` (11.5 min of it) | 17:38 | 17:56 | 18 |

- **Verification:**
  - `cargo build` passes. `cargo xtask format-check` passes.
  - `cargo test`: 4308 passed, 0 failed, 10 ignored (lib 4259 / 3 ignored, plus 36, 8 and 5 / 7
    ignored).
  - `cargo xtask lint`: the custom lint reports exactly the 15 remaining §5.1 rows (Phase 5 2,
    Phases 7/8 2, Phase 9 1, Phase 11 10), nothing new. The step stops there, so the rest was run one by
    one, and all pass: `doc-hygiene`, both workspace clippy passes (default and `--all-features`), and
    the xtask clippy pass.
  - `make -k verify` (2026-10-07, macOS, 17:43–17:54) fails in three targets:
    - `build-c`: `cmake: command not found`. Environment: cmake is not installed on this host.
    - `lint`: the 15 remaining §5.1 rows only, as above. Expected under the §5.1 gate rule.
    - `test-rust-all-features`: 4486 passed, 17 failed, 3 ignored. All 17 failures are Docker-backed
      `integration_tests::*` ("failed to list networks": the Docker daemon is not running), an
      environment issue.

    Everything else passes: Python unit tests (363 passed, 2 skipped), `check-bindings` / format-arity
    (29 passed), and the soak tests (156 passed).
  - **Owed:** a Docker-backed run of the integration suite, and a `build-c` / C-test run on a host with
    cmake. The C test changes (`kafka_common_Error_is_unsupported_version_error`) are therefore
    compile-checked only through the Rust FFI test.

### Phase 2 — KIP-909 core: bootstrap DNS resolution (agent 92)

- Translate in tree-diff order:
  - `BootstrapConfiguration` (new);
  - `ClientUtils` (`bootstrapConfiguration`, `maybeBootstrapMetadataSynchronously`, `parseAddresses`);
  - `CommonClientConfigs` `BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG` (default 0, experimental doc);
  - `MetadataUpdater` defaults (`bootstrap`, `isBootstrapped`, `bootstrapFailed`, `clusterId`);
  - `NetworkClient` (+216) and `Metadata` (`bootstrapFatalError`, `maybeThrowBootstrapFatalException`);
  - `AdminMetadataManager` (+42), `KafkaAdminClient` (+90/−6), `AdminClientConfig`;
  - `KafkaProducer` / `ProducerConfig` construction;
  - consumer construction and config wiring (`ConsumerConfig`, `KafkaConsumer`, `AsyncKafkaConsumer`,
    `NetworkClientDelegate`, `ConsumerUtils`).
  - Java deletes `AdminBootstrapAddresses`; confirm Rust has no counterpart to delete.
- KAFKA-20939 (0df48ff5c5, 87943b2ff8): a DNS-failure regression fix and the "experimental" marking.
- MINOR 0720ba1141: port validation, dead code.
- Tests: `NetworkClientTest`, `KafkaAdminClientTest`, `KafkaProducerTest`, `KafkaConsumerTest` (non-share,
  non-classic slices), `ClientUtilsTest`, `MetadataTest` hunks. The behaviour of both modes (0 and
  positive) is the contract.
- Integration: a test with an unresolvable bootstrap host under a positive timeout, asserting the
  `BootstrapResolutionError` message.
- Commits covered: 507d01da42 (minus its Share/Classic files), 0df48ff5c5, 87943b2ff8, 0720ba1141.
  Recorded skip: d1c0bd82c0 (ShareConsumerImpl only).

Phase 2 completion notes (agent 92):

- **Commits:** b3d85ec1 (core + admin + producer), 5b2eef8d (consumer), 37617f3c (integration test),
  c81ffa34 (lint), plus this notes commit. All four Java commits are covered: 507d01da42 (minus Share /
  Classic), 0df48ff5c5, 87943b2ff8, 0720ba1141.
- **What landed, in tree-diff order:**
  - `CommonClientConfigs::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG` and `DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS = 0`.
    The `_DOC` text (with 87943b2ff8's "evolving feature" sentence) is rustdoc, following the
    `CLIENT_DNS_LOOKUP_CONFIG` precedent: the crate has no `ConfigDef` docs.
  - `BootstrapConfiguration` (new, crate-private): `DISABLED`, `enabled()` (validates without resolving;
    "Invalid url" / "Invalid port" per 0720ba1141), `is_disabled()`. Java compares against `DISABLED` by
    identity; Rust compares by value, which is equivalent because only `DISABLED` has no `client_dns_lookup`.
  - `ClientUtils`: `resolveAddress` extracted and shared, as in Java, with its two exceptions as the
    private `ResolveAddressError`; `parse_addresses`, `bootstrap_configuration` (with the info log) and
    `maybe_bootstrap_metadata_synchronously`. 0720ba1141's removal of the `(AbstractConfig)` overload has no
    Rust counterpart. Rust configs share no `AbstractConfig`, so the two new functions take the values
    Java reads from it.
  - `Metadata`: `bootstrap_fatal_error`, `maybe_return_bootstrap_fatal_error`. The failure is checked first
    by `maybe_return_fatal_error` and by `clear_errors_and_maybe_return_error`, is never cleared, and wakes
    `await_update` waiters (Java's `notifyAll`).
  - `MetadataUpdater`: `bootstrap_failed` (default no-op), `is_bootstrapped`, `bootstrap`. The test-only
    `ManualMetadataUpdater` / `TestMetadataUpdater` implement Java's `ManualMetadataUpdater` semantics.
  - `NetworkClient`: `ensure_bootstrapped` and its helpers, `handle_empty_node_list`, the
    `&& is_bootstrapped()` rebootstrap gate in `default_maybe_update`, the `DefaultMetadataUpdater`
    `isBootstrapped` / `bootstrap` / `bootstrapFailed` delegation, and `cancel_bootstrap_resolution` on close.
  - Admin: `AdminClientConfig` key (`atLeast(0)`) and getter; `KafkaAdminClient::determine_bootstrap_type`;
    `AdminMetadataManager` `is_bootstrapped` / `bootstrap_fatal_error` / `record_bootstrap_fatal_error` plus
    the updater hooks; `AdminClientRunnable::fail_all_pending_calls` and the loop exit; and the enqueue-side
    check in `CallSender::call` (Java's `runnable.call` → `enqueue`), which `submit`, the
    `AdminApiDriver` path and the per-broker `listGroups` sends all use (review round 1).
  - Producer: `ProducerConfig` key; construction through `maybe_bootstrap_metadata_synchronously` and
    `set_bootstrap_configuration`.
  - Consumer: `ConsumerConfig` key; `AsyncKafkaConsumer` construction as for the producer; `ensure_open`
    (Java's `acquireAndEnsureOpen`) returns the bootstrap failure from every `Result`-returning API (the
    sync accessors cannot; see the deviations); `NetworkClientDelegate`
    `bootstrap_error_propagated` / `propagate_metadata_error`.
  - Rustdoc: the `@throws BootstrapResolutionException` lines on the `Consumer` trait (`poll`, `commit_sync`,
    `committed`, `partitions_for`, `list_topics`, `offsets_for_times`) and the `Producer` trait (`send`,
    `partitions_for`).
  - `AdminBootstrapAddresses` has no Rust class to delete: its `fromConfig` logic lived inline in
    `AdminClientConfig::new`, which now calls `determine_bootstrap_type`.
- **Mode 0 is unchanged, and how that is shown:**
  - Every client's `bootstrap.resolve.timeout.ms` defaults to 0. `bootstrap_configuration(0, ..)` returns
    `DISABLED`, and every new `NetworkClient` path returns at once on `is_disabled()`. The two shared-path
    edits are inert in mode 0:
    - `handle_empty_node_list` still panics with the old message;
    - the rebootstrap gate's `is_bootstrapped()` can only be false when the node list is empty, and then
      `least_loaded_node` has already panicked.
  - `maybe_bootstrap_metadata_synchronously(0, ..)` is the old `parse_and_validate_addresses` +
    `metadata.bootstrap`, with the same error. The admin path keeps its synchronous
    `parse_and_validate_addresses` + `update(Cluster::bootstrap)`.
  - Tests:
    - `test_disabled_bootstrap_configuration_never_resolves` polls a `DISABLED` client at three far-apart
      times and asserts no resolution, no timer and no failure;
    - `test_least_loaded_node_while_bootstrapping` asserts the mode-0 empty-cluster panic message;
    - `test_bootstrap_configuration` and `test_maybe_bootstrap_metadata_synchronously` cover the 0 branch;
    - the three `*ConstructorFailsWithConfigException*WhenTimeoutZero` translations (producer, consumer,
      admin) pin the construction-failure contract;
    - every pre-existing `NetworkClient` / client test still passes unchanged on the `DISABLED` default.
- **Async path (mode > 0) design** (CLAUDE.md §11.6, consumer-threading §10):
  - The resolution runs `ClientUtils::parse_addresses` on a detached OS thread named
    `kafka-bootstrap-dns-resolver` (Java's daemon executor thread, `NetworkClient.java:399`), and its result
    comes back over a `oneshot`. `poll()` only `try_recv`s that result, so the resolution is never awaited,
    is never in a `select!` arm, and never holds up or races the network poll. No runtime owns the thread,
    so no runtime drop waits for it (review round 1, Issue 2).
  - `close()` cancels the attempt (drops the receiver; the thread's `is_interrupted` check, Java's
    `isInterrupted()`, stops it at the next URL) and then waits for the thread as Java's
    `shutdownExecutorServiceQuietly(.., 1, SECONDS)` does: at most 1 s, then at most 1 s more, then it logs
    and returns.
  - When the result lands, the thread pokes the selector's wakeup handle so a blocked poll picks it up
    promptly. That wake is Rust-only and changes latency only.
  - No lock is held across an `.await`: `Metadata` / `AdminMetadataManager` take their std `Mutex` briefly
    and synchronously.
  - The timer starts on the first poll, and an expiry at the same poll as a result loses to the result, as
    in Java.
- **Recorded skips:**
  - d1c0bd82c0 and the Share / Classic hunks of 507d01da42 / 0df48ff5c5 (`ShareConsumerImpl`,
    `KafkaShareConsumer`, `ClassicKafkaConsumer`, `KafkaShareConsumerTest`), plus the `CLASSIC` parameter of
    the two `KafkaConsumerTest` translations: out of scope (§1.1, consumer-threading §20).
  - Connect, core, raft, server, streams, tools and trogdor hunks of 507d01da42 / 0df48ff5c5: not client code.
  - `MetadataUpdater.clusterId`: it comes from 0ef4a4c80e (KAFKA-20246 2/N), which Phase 4 owns. Its only
    callers are Phase 4's.
  - `ClientUtils.createNetworkClient(config, bootstrapServers, ..)` and the `metadata.cluster.check.enable`
    argument: Rust has no `createNetworkClient`, since each client builds its `NetworkClient` itself, and
    the cluster-check flag is Phase 4.
  - `KafkaAdminClient.java:790-797` (`MetadataUpdateNodeIdProvider.provide()`), the `isBootstrapped()` gate on the admin rebootstrap: the Rust admin client
    runs with `MetadataRecoveryStrategy::None` and has no rebootstrap path. `AdminMetadataManager::is_bootstrapped`
    is translated (DoD #2) and tested, and is `cfg_attr(not(test), expect(dead_code))`.
  - `KafkaProducer.awaitTopicMetadata`'s `maybeThrowBootstrapFatalException`, and
    `KafkaProducerTest.testProducerSendOffsetsToTransactionBootstrapResolutionExceptionPropagated` (including
    0720ba1141's mock tweak to it): `awaitTopicMetadata` is KIP-1319, so this is **Phase 5's**. Java's own
    comment says `sendOffsetsToTransaction` "became metadata-aware in KIP-1319".
  - Java's `Thread.interrupted()` → `InterruptException` branch in `ensureBootstrapped`: it tests the
    *application* thread's interrupt flag, which a tokio task does not have. (`parseAddresses`'
    `isInterrupted()` exit **is** translated, as the `is_interrupted` argument; review round 1 corrected an
    earlier note that called it untranslatable.)
  - `NetworkClientTest` fixture plumbing: the `bootstrapConfiguration` argument on every helper, the
    `bootstrapMetadataUpdater` calls, and the extra `client.poll(0, ..)` and metadata re-update in
    `testRebootstrap` / `testRequestTimeout` / `testDefaultRequestTimeout`. Java's tests needed these because
    their fixtures switched to an *enabled* configuration; the Rust fixtures keep the `DISABLED` default,
    which is what the production clients use in mode 0.
  - `KafkaProducerTest.testConstructorFailureCloseResource` / `KafkaConsumerTest.testConstructorClose`
    (bootstrap host and interceptor-class changes) and `doTestHeaders` (bootstrapping the injected metadata):
    no Rust counterpart test. Reflective metric reporters and interceptor classes are not translated.
  - `KafkaAdminClientTest.mockBootstrapCluster` (`createUnresolved`): a fixture with no Rust counterpart.
  - `MetadataTest` (one import-line change) and `ClientUtilsTest` (unchanged): nothing to translate. New
    Rust tests cover `parse_addresses`, `bootstrap_configuration` and `maybe_bootstrap_metadata_synchronously`.
  - `PlaintextConsumerTest.testListTopics` now polls inside its wait. The Rust
    `test_async_consumer_list_topics` already does.
- **Deviations (DoD #7):**
  - `PendingBootstrapResolution` (a `oneshot::Receiver`) replaces Java's `CompletableFuture`, and one
    detached, named OS thread per attempt replaces the `kafka-bootstrap-dns-resolver` single-thread
    executor (attempts never overlap, so it is one thread at a time; `bootstrap_resolver_exit` is the
    executor's `awaitTermination`). Dropping the receiver is `cancel(true)` plus the `shutdownNow()`
    interrupt. A `getaddrinfo` already running cannot be interrupted in either language; the thread exits
    after it. (The first version ran on tokio's blocking pool. Critic 92 showed a running blocking task
    holds a runtime's drop without bound — `close_with_options(1 s)` took 14 s — so that justification was
    withdrawn.)
  - `CallSender` (Rust-only) bundles the admin channel with the state `call` / `enqueue` read, because
    Java's driver closures capture the runnable itself and the Rust runnable is owned by its task.
  - The consumer's sync accessors (`assignment`, `subscription`, `paused`, `group_metadata`,
    `current_lag`) keep returning their value after a permanent bootstrap failure, where Java's
    `acquireAndEnsureOpen()` throws `BootstrapResolutionException`: they have no error channel
    (consumer-threading §1), as for the existing closed-consumer divergence. Documented on each accessor.
  - The bootstrap `Timer` is a deadline (`org.apache.kafka.common.utils.Timer` has no translation).
  - The configuration is a setter rather than a constructor argument.
  - `determineBootstrapType` runs inside `AdminClientConfig::new`, where the old `fromConfig` logic already
    ran, because the Rust config does not keep `bootstrap.controllers`.
  - `ResolveAddressError` stands for the two Java exceptions of `resolveAddress`.
  - `CallSender::call` follows Java's order. It rejects with `IllegalState` only when `close()` has set a
    hard-shutdown time (Java's `hardShutdownTimeMs` check in `call()`), not on `shutdown.closing`, which the
    I/O task also sets when it exits on its own. After that come the `bootstrap.controllers` endpoint check,
    the bootstrap failure, and finally the closed channel, which gives Java's `enqueue` `TimeoutException`.
- **DoD #10: N/A.** The `NetworkClient`, `Metadata` and admin bootstrap paths are per poll and per
  construction, not per record. The only addition to `poll()` in mode 0 is the `is_disabled()` comparison.
- **For Phase 3 (consumer busy loops):** the consumer's async path works end to end
  (`test_consumer_bootstrap_resolution_error_propagated_to_poll`, ~3 s), but Phase 2 did not audit the
  pre-bootstrap poll-timer / `maximum_time_to_wait` behaviour that KAFKA-20854/21010/20970 fix. While
  bootstrapping:
  - `NetworkClient::least_loaded_node` returns an empty node instead of panicking, so request managers
    see `None` there;
  - the default updater's `maybe_update` returns `reconnect_backoff_ms`;
  - the resolver's wake pokes the selector.
  Phase 3's `AbstractFetch` / heartbeat / commit fixes plug in on top. `ensure_open` is the
  `acquireAndEnsureOpen` hook if Phase 3 needs more call sites.
- **For Phase 4 (shared `NetworkClient`):**
  - Follow the `set_bootstrap_configuration` setter precedent for `metadataClusterCheckEnable`.
  - `MetadataUpdater::cluster_id` is still to add, with Java's default `None`.
  - `poll()` now calls `ensure_bootstrapped(now)` right after the poll-time stores, as Java does right after
    `ensureActive()`.
  - `least_loaded_node` goes through `handle_empty_node_list`, so the KAFKA-20393 `stickyNode` work should
    keep that path.
- **For Phase 12 (shared `KafkaAdminClient`):**
  - `AdminClientRunnable` has a `bootstrap_failure_handled` flag. `run_once` returns right after
    `fail_all_pending_calls`, and `process_requests` breaks on the flag.
  - Every app-side and driver enqueue goes through `CallSender::call`, which reads
    `metadata_manager.bootstrap_fatal_error()` once at the top. (Correction, Phase 12 / Critic 102 Issue 1: not
    *every* enqueue — a handler's `HandleResult::NewCall`, the createTopics / createPartitions / deleteTopics
    quota-retry follow-ups, is pushed straight into `pending_calls` by the I/O task and skips `call()`'s checks,
    where Java's follow-ups go through `runnable.call`.) Keep the order; keep that order relative to the closing and controllers checks when adding
    `unregister_controller`. Java's `enqueue` also starts with a `tries > maxRetries` check that the Rust
    enqueue never had (a pre-existing gap, not KIP-909).
  - `fail_all_remaining` closes the call channel before its final drain, so no accepted call is dropped.
  - `bootstrap.controllers` is still rejected in the config, so `determine_bootstrap_type` returning `true`
    becomes the unsupported-version error.
- **For the producer-track merge (Phase 5):** in `KafkaProducer::new_inner` this phase touched only steps 1
  and 5 and added the `set_bootstrap_configuration` call after the `NetworkClient` is built. In
  `ProducerConfig` it added the field, constant, parse arm and test next to `retry.backoff.max.ms`, and the
  `Producer` trait docs gained one line each on `send` / `partitions_for`.
- **Timing log** (2026-10-07, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, plan, Java diffs, Rust counterparts) | 20:29 | 20:45 | 16 |
  | 1 core + NetworkClient + admin + producer, with tests (b3d85ec1) | 20:45 | 20:59 | 14 |
  | 2 consumer, with tests (5b2eef8d) | 20:59 | 21:04 | 5 |
  | 3 integration test (37617f3c) | 21:04 | 21:08 | 4 |
  | 4 gates: format-check, `lint --keep-going` + fixes (c81ffa34), full `cargo test` | 21:08 | 21:18 | 10 |
  | 5 `make -k verify` (10 min) and these notes | 21:18 | 21:30 | 12 |

- **Verification:**
  - `cargo build` passes. `cargo xtask format-check` passes.
  - `cargo test`: 4336 passed, 0 failed, 10 ignored (lib 4287 / 3 ignored, plus 36, 8 and 5 / 7 ignored).
  - `cargo xtask lint --keep-going`: lint-custom reports exactly the 15 remaining §5.1 rows (Phase 5 2,
    Phases 7/8 2, Phase 9 1, Phase 11 10), nothing new. Every other step is clean: doc-hygiene,
    module-path-hygiene, and the default, `--all-features` and xtask clippy passes.
  - `make -k verify` (macOS, 21:18–21:28) fails in three targets, for the same reasons as in Phase 1:
    - `build-c`: `cmake: command not found` (environment);
    - `lint`: the 15 §5.1 rows only;
    - `test-rust-all-features`: the `cargo test --all-features` lib run appears twice in the log, at
      4508 and 4514 passed. Each has 17 failed and 3 ignored, and all 17 are Docker-backed
      `integration_tests::*`; the Docker daemon is not running (environment).

    Everything else passes: Python unit tests (363 passed, 2 skipped), `check-bindings` / format-arity
    (29 passed), and the soak tests (156 passed).
  - The three broker-less `bootstrap_resolution_test` integration tests (producer, consumer, admin with an
    unresolvable host) were run directly and pass.
  - **Owed:** a Docker-backed run of `bootstrap_resolution_test::test_clients_bootstrap_asynchronously_with_positive_timeout`
    and the rest of the integration suite, and a `build-c` / C-test run on a host with cmake. No C or Python
    source changed in this phase.
- **Review round 1 (COMMENTS.92, all moved to `COMMENTS.DONE.92.md`, 2026-10-07 22:02–22:30 IST):**
  - **Issue 1 (Medium), fixup 20185de8:** one shared enqueue, `CallSender::call`, so the 14 driver-backed
    RPCs fail with `BootstrapResolutionError` instead of the retriable "thread has exited" timeout. The
    channel is closed before the final drain, so every accepted call's future completes. Tests: three
    driver RPCs in `test_admin_bootstrap_resolution_error_propagated` (each bounded at 5 s, exact message)
    and `test_call_sender_applies_the_enqueue_checks`.
  - **Issue 2 (Medium), fixup 253f968d:** the detached resolver thread, the `is_interrupted` check and the
    bounded `close()` described above. Tests:
    - `NetworkClient` `close()` with three 5 s lookups in flight: under 3 s;
    - a runtime drop with a lookup in flight: under 2 s;
    - the consumer built outside any runtime, as the bindings build it, with `close_with_options(1 s)`:
      under 5 s (the whole filtered run took 3.2 s; the Critic's probe measured 14 s before);
    - the producer close plus runtime drop, the C / Python destroy path: under 4 s;
    - `parse_addresses` stops once interrupted.

    The slow lookups come from a `cfg(test)` host-name seam (`*.slow-dns.kafka.test`), not real DNS.
  - **Issue 3 (Low), fixup 6b929c66:** the sync-accessor divergence is documented; the `ensure_open` doc is
    narrowed.
  - **Issue 4 (Low), fixup fbfe06c7 plus this file:** citations `:1621-1629` and `:790-797` re-checked at
    4.4.0-rc4; the stale `(AbstractConfig)` sentence and the misplaced `consumer_config.rs` doc comment are
    fixed.
  - **Harness, bc66e31e:** `INTEGRATION_TEST_BROKER_TAG` selects the `apache/kafka` tag (default `4.2.0`).
    Broker runs, against both the default and `4.4.0-rc4` (confirmed via `docker ps`: only
    `apache/kafka:4.4.0-rc4` containers):

    | Suite | 4.2.0 | 4.4.0-rc4 |
    |---|---|---|
    | `--test integration -- bootstrap_resolution_test` | 4 / 4 | 4 / 4 |
    | `--test integration -- client_rebootstrap_test` | 4 / 4 | 4 / 4 |
    | `--lib -- integration_tests::` | 31 / 31 | 31 / 31 |
  - **Round 2 (Issues 5, 6):** `CallSender::call` now rejects with `IllegalState` only when a hard-shutdown
    time is set, as Java's `call()` does. A task that exited on its own (a panic caught in `run`) therefore
    yields Java's `TimeoutException("The AdminClient thread has exited.")` at the closed channel, and a
    bootstrap failure is caught by the next check. Covered in `test_call_sender_applies_the_enqueue_checks`.
    These review notes now sit inside the Phase 2 section, where they belong; they had been appended after §6.

### Phase 3 — KIP-909 consumer follow-ups (agent 93)

- KAFKA-20854 (642e0a5db0): `AbstractFetch` (+61), `FetchRequestManager`, `Fetcher` (classic → skip),
  `RequestManagers`, `SubscriptionState`, `AsyncKafkaConsumer`.
- KAFKA-21010 (d12e95da90): `AbstractHeartbeatRequestManager`, `HeartbeatRequestState`,
  `CommitRequestManager`.
- KAFKA-20970 (cd44c5de0f): `CommitRequestManager`.
- Tests: `FetchRequestManagerTest` (+158), `ConsumerHeartbeatRequestManagerTest` (+121),
  `CommitRequestManagerTest` (+95), `AbstractHeartbeatRequestManagerTest`. Skip the Share / Streams test
  hunks with reasons.
- Busy-loop fixes must be shown not to spin in Rust: assert poll-timer / `maximum_time_to_wait` values;
  don't rely on wall-clock behaviour alone.

Phase 3 completion notes (agent 93):

- **Commits:**
  - 91dcc787 (KAFKA-20854) + fixup 73d04f5f (lint markers);
  - 02ac689c (KAFKA-20253, pulled forward: coordinator + auto-commit hunks) + fixup 150f4170 (coordinator
    forwarder wakes the bg task);
  - e4bf1523 (KAFKA-20970);
  - 98e6c919 (KAFKA-21010 + the KAFKA-20253 heartbeat hunk it rewrites);
  - 7ffcfa5b (the bg loop consults the commit manager);
  - a269808f (selector drains fired readiness every poll iteration);
  - plus this notes commit.
- **What landed:**
  - KAFKA-20854:
    - `SubscriptionState::has_fetchable_partitions`.
    - `AbstractFetch`'s nested `FetchRequestPreparationResult`, returned by `prepare_fetch_requests` and
      `prepare_close_fetch_session_requests`. Close may always wake the buffer.
    - The "wake on any response" moved from `handle_fetch_success` to `remove_pending_fetch_request`.
    - The Phase-26 all-nodes-unfetchable short-circuit computes the same `can_wake` flag as the full path:
      false with nothing buffered, which is the KIP-909 no-nodes window.
    - `FetchRequestManager` takes `retry_backoff_ms` and overrides `maximum_time_to_wait`
      (`retry_backoff_ms` with nothing in flight, else `i64::MAX`).
    - An empty poll wakes the buffer only on `can_wake`. This replaces the Rust-only "wake when nothing is
      in flight" guard, which still woke the buffer when no partition was fetchable.
    - `AsyncKafkaConsumer::poll_for_fetches_timeout_ms` (extracted, Rust-only) gains the "fetchable but
      unbuffered → retry backoff" branch.
  - KAFKA-20970 / KAFKA-21010:
    - `CommitRequestManager::maximum_time_to_wait` returns `retry_backoff_ms` while the coordinator is
      unknown. KAFKA-20970's interval accessor is added and then removed again, as in Java.
    - `ConsumerHeartbeatRequestManager::maximum_time_to_wait` has its 4.4 shape:
      - UNSUBSCRIBED or FATAL → `i64::MAX`;
      - poll timer expired → 0;
      - coordinator unknown or `should_skip_heartbeat()` → `HeartbeatRequestState::retry_backoff_ms()`.
    - `HeartbeatRequestState::retry_backoff_ms()` is new. It reads a new `RequestState::exponential_backoff`
      accessor, which stands in for Java's protected field.
  - Bg loop (`ConsumerNetworkThread::run_once` Phase 5):
    - It now also consults the coordinator and commit handles, which `RequestManagers::entries()` skips.
    - Java's `entries()` includes both. Before this phase the auto-commit timer never bounded the
      application's wait, so the 20970/21010 commit fixes would have been unreachable.
- **Regressions found by `make -k verify` and fixed in this phase:**
  - `consumer_bounce_test::test_async_close` failed 3/3 (baseline ed2e3ce3: 2/2 passed): "Close took too
    long 3001".
    - Cause: the KAFKA-20253 coordinator guard made the bg loop sleep (`i64::MAX`) while FindCoordinator was
      in flight. The FindCoordinator response is applied by a spawned forwarder *after* the network poll
      returns (Java applies it inside the poll), and nothing woke the loop afterwards.
    - Before the guard, the loop's 0 timeout hid this.
    - Fixed in 150f4170: `CoordinatorRequestManager::completion_notify`, wired to `event_notify` as the
      commit and fetch managers already are.
  - Selector stranded fire (a269808f):
    - `drain_fired_queue` ran only inside the WAIT's `readiness_wait`. Its `select!` is `biased` towards the
      wakeup `Notify`, so a poll entered with a stored permit returned without draining.
    - The fired channel then kept `armed_read = true` and was never re-armed or processed. A trace showed a
      coordinator-channel read fire drained 30 s late, after the channel had closed.
    - This phase makes it reachable: the application task now pokes the bg task once per `retry.backoff.ms`
      instead of busy-looping, and the loop no longer polls with timeout 0, whose `process_all` sweep used
      to read every channel anyway.
    - `test_fire_not_stranded_when_every_poll_starts_woken` fails at its 5 s timeout without the new drain.
- **`test_async_close` +30 s (corrected after review, COMMENTS.93 Issue 1):** the earlier "broker-side race"
  diagnosis was wrong.
  - Critic 93's evidence: the broker answered the first heartbeat in about 380 ms, the response sat unread
    in the test's in-process `BrokerProxy`, and a sample showed a test-runtime worker busy in
    `AsyncKafkaConsumer::poll` 99% of the time.
  - Cause: while a JOINING member's first heartbeat is in flight with a zero interval,
    `maximumTimeToWait` is 0, as in Java. `poll_inner` then looped without ever returning `Pending`,
    monopolising its tokio worker and starving the proxy task on that runtime.
  - Fixed in fixup 9a390dd3: `poll_inner` yields once per empty iteration.
    `test_poll_yields_while_maximum_time_to_wait_is_zero` fails without the yield.
  - Result: 9 of 9 logged `test_async_close` runs under the broker lock took about 24.6 s, with no request
    timeouts. Three earlier unlogged runs took 44 s, 111 s and 111 s while the host load average was above
    6 from parallel tracks; they did not recur at load ~3.
- **How each busy loop is shown fixed, by value:**
  - Background task, while bootstrapping:
    - `run_once_does_not_spin_while_bootstrap_resolution_is_pending` runs 40 iterations (50 ms model steps)
      of a group consumer over a client with no nodes: real coordinator, commit (100 ms auto-commit
      interval), heartbeat (a JOINING member with zero heartbeat interval), offsets and fetch.
    - It asserts every network poll timeout == `retry.backoff.ms` (100) and the published
      `cachedMaximumTimeToWait` == 100.
    - Teeth-checked: without the KAFKA-20253 coordinator guard the poll timeout is 0 from iteration 2. With
      the heartbeat returning its (zero) interval, or the commit manager ignoring the coordinator, the bound
      becomes 0 or the timer's remainder.
  - Application task:
    - `test_poll_for_fetches_timeout_bounded_by_retry_backoff` asserts the wait in each state:
      - nothing assigned (the bootstrapping case), no position, or unbuffered fetchable → 100;
      - everything buffered → the full timeout;
      - `maximumTimeToWait` 0 → 0, which the caller turns into an immediate return.
    - `test_poll_with_manual_assignment_does_not_busy_loop` asserts Java's 500.
  - Per manager, at Java's values:
    - FRM: the 8 new tests, plus the pre-4.4 `testEmptyFetchResponseWakesUpBuffer`. They assert
      `maximum_time_to_wait` (`i64::MAX` / 100) and the buffer's pending-wakeup flag
      (`FetchBuffer::is_woken_up_for_test`, cfg(test)) instead of Java's blocked-thread joins.
    - Commit and heartbeat assert `DEFAULT_RETRY_BACKOFF_MS` / `Long.MAX_VALUE` exactly.
    - Both `...DoesNotSpinDuringRealBootstrapDnsResolution` tests drive a real `NetworkClient` that resolves
      `unresolvable.invalid` asynchronously until the `BootstrapResolutionError`, asserting `> 0` at each
      step.
- **Audit of the bootstrapping wait computation (mode > 0):**
  - Besides the three fixed managers:
    - the offsets manager returns `EMPTY` / `with_requests` (`i64::MAX`);
    - the topic-metadata manager returns Java's one-shot `0` only when it emits a request;
    - the membership manager returns `EMPTY`;
    - the real `NetworkClient` caps its selector wait at `maybe_update`'s `reconnect_backoff_ms` (50 ms
      default) while no node is known.
  - The network poll is never raced in a `select!`, and no guard is held across an `.await`: the new reads
    take each std `Mutex` briefly, and the commit manager reads its coordinator before taking its state
    lock.
  - **Mode 0:** the bootstrap path is untouched. The wait changes apply in both modes, as in Java.
- **Recorded skips:**
  - the `Fetcher.java` hunk (classic consumer);
  - the `StreamsGroupHeartbeatRequestManager` hunks of 20970/21010 and their tests, and the
    `ShareHeartbeatRequestManagerTest` hunks of 21010/20253 (consumer-threading §20);
  - checkstyle suppressions;
  - the fixture-only `AsyncKafkaConsumerTest` hunks: the `retryBackoffMs` local in `newConsumer`, and the
    Mockito stub reordering in `testSubscribePatternAgainstBrokerNotSupportingRegex`
    (`AsyncKafkaConsumerTest.java:2482`), which has no Rust counterpart.
- **Deviations (DoD #7):**
  - `FetchRequestPreparationResult` is keyed by node id, with the `Node` alongside.
  - `poll_for_fetches_timeout_ms` is an extracted helper (Java computes it inline).
  - The FRM "in flight" set can lag Java's by one iteration: completions are drained by the next
    `poll(now)`, which the forwarder's `completion_notify` schedules at once.
  - `CoordinatorRequestManager::completion_notify` exists because the Rust forwarder runs outside the
    network poll.
  - Two Rust tests were corrected:
    - the heartbeat "poll timer expired" test answered through `should_heartbeat_now()`, because the Rust
      poll timer is unarmed until the first reset (Issue 9); it now arms the timer;
    - several commit timer tests now wire a known coordinator, as Java's mocks do.
- **DoD #10: N/A** (per-poll / per-iteration paths, not per record). The extra `buffered_partitions()` in the
  wait bound and in the short-circuit allocates only when the buffer holds data. The selector drain is one
  uncontended lock per loop iteration.
- **For Phase 9:**
  - KAFKA-20253 (28de22de34) is **done here in full**: its three main hunks and its
    `ConsumerHeartbeatRequestManagerTest` / `CoordinatorRequestManagerTest` tests (the Share test is
    skipped). Record it as covered by Phase 3.
  - The heartbeat `maximum_time_to_wait` is the final 4.4 version.
  - `AbstractHeartbeatRequestManagerTest.testNoCoordinator`'s 21010 assertion lives in
    `poll_returns_empty_when_no_coordinator`; the e7b0cb7908 file mapping is still Phase 9's decision.
  - The heartbeat file lost its stale `cfg_attr(test, expect(dead_code))`.
  - The heartbeat forwarder has no bg-task wake, unlike the coordinator, commit and fetch forwarders.
    Phase 9 should check whether its responses wait out the poll timeout.
- **For Phase 4:**
  - `NetworkClient::set_mock_time` is now `pub(crate)` (cfg(test)) for the shared fixture
    `network_client_delegate::bootstrapping_network_client_delegate_for_test`.
  - The selector's loop now drains fired readiness at its top (a269808f).
- **For Phase 10:**
  - The FRM wake semantics and `poll_for_fetches_timeout_ms` changed as described above.
  - Production's `is_unavailable` closure is still the constant `false` under the pre-existing
    `FIXME(phase-9-sasl)` in `async_kafka_consumer.rs`, so KAFKA-20854's reconnect-backoff skip is
    reachable only in tests.
- **Timing log** (2026-10-07/08, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, plan, Java diffs, Rust counterparts) | 22:44 | 22:52 | 8 |
  | 1 KAFKA-20854 with tests (91dcc787) | 22:52 | 23:11 | 19 |
  | 2 KAFKA-20253 coordinator + auto-commit (02ac689c) | 23:11 | 23:15 | 4 |
  | 3 KAFKA-20970 with tests (e4bf1523) | 23:15 | 23:20 | 5 |
  | 4 KAFKA-21010 + 20253 heartbeat, with tests (98e6c919) | 23:20 | 23:25 | 5 |
  | 5 bg-loop consult + proof test + teeth checks (7ffcfa5b) | 23:25 | 23:31 | 6 |
  | 6 integration (default + 4.4.0-rc4), format-check, full test, lint + fixup (73d04f5f) | 23:31 | 23:38 | 7 |
  | 7 `make -k verify` #1 | 23:38 | 23:49 | 11 |
  | 8 regression bisect + root causes (baseline worktree, tracing, broker logs) | 23:49 | 01:17 | 88 |
  | 9 fixes 150f4170, a269808f, gates, `make -k verify` #2, re-runs, notes | 01:17 | 01:40 | 23 |

- **Verification (HEAD a269808f):**
  - `cargo build` passes, and so does `cargo xtask format-check`.
  - `cargo test`: 4369 passed, 0 failed, 10 ignored (lib 4320 / 3 ignored, plus 36, 8, and 5 / 7 ignored).
  - `cargo xtask lint --keep-going`: exactly the 15 §5.1 rows; doc-hygiene, module-path and clippy are clean.
  - Broker-backed tests:
    - default 4.2.0 broker, `--test integration -- plaintext_consumer consumer_test base_consumer_test
      bootstrap_resolution_test consumer_topic_creation_test`: 101 / 101 (run before the two late fixes;
      the consumer subset was re-run after them inside verify);
    - `INTEGRATION_TEST_BROKER_TAG=4.4.0-rc4 -- bootstrap_resolution_test`: 4 / 4.
  - `make -k verify` #2 (macOS) fails in three targets:
    - `build-c`: `cmake: command not found` (environment);
    - `lint`: the 15 §5.1 rows only;
    - `test-rust-all-features`: lib 4564 passed. The integration suite had 300 passed and 5 failed, all five
      under full-suite load: `admin_scram_test::…round_trips__rust`, `consumer_bounce_test::test_async_close`,
      `consumer_bounce_test::test_async_subscribe_when_topic_unavailable` ("TopicExists"),
      `consumer_test::test_leader_epoch`, `producer_transactions_test::test_fencing_on_transaction_expiration`.
      Re-run alone they all pass: 8 / 8 including the whole `consumer_bounce_test` module, 2 ignored.

    Verify #1 had 17 integration failures: 15 in `producer_transactions_test` ("Connection refused" to a dead
    cluster; a solo re-run still failed its `grpc_*` variants and three Rust tests on timeouts, none of
    them in code this phase touched), and 2 in `consumer_bounce_test`, one of them the then-real
    `test_async_close` regression.
  - Python unit tests: 363 passed, 2 skipped. `check-bindings`: 29 passed. Soak: 156 passed.

### Phase 4 — KIP-1242 misrouted-connection detection + NetworkClient fixes (agent 94)

- ede01b871e: the `metadata.cluster.check.enable` config in `CommonClientConfigs` and the
  producer / consumer / admin configs.
- 0ef4a4c80e:
  - ApiVersions v5 carries the `ClusterId` / `NodeId` the client expects (`ApiVersionsRequest` setters).
  - `NetworkClient` (+58): send them, and handle a mismatch or `REBOOTSTRAP_REQUIRED` by disconnecting
    and rebootstrapping.
  - `MetadataUpdater.clusterId`.
- 7be741d08b: a new `GroupCoordinatorNode` (`consumer::internals`) so coordinator connections carry the
  real broker id; `Node` changes; `CoordinatorRequestManager`. Skip `AbstractCoordinator` (classic).
- KAFKA-20393 (123ee9e45d): the `stickyNode` stale-IP fix in `NetworkClient`.
- Tests: `NetworkClientTest` (+164 across commits), `GroupCoordinatorNodeTest` (+54),
  `CoordinatorRequestManagerTest`, `KafkaAdminClientTest` / `KafkaConsumerTest` hunks. Byte-level
  ApiVersions v5 encoding test (DoD #3).
- Runs after Phase 2 (both edit `NetworkClient`).

### Phase 5 — Producer: KIP-1319 TxnOffsetCommit v6 with topic IDs (agent 95)

- Sync the held-back `TxnOffsetCommit{Request,Response}.json` (§2.2): v6, `GenerationIdOrMemberEpoch`
  rename, `TopicId`, stable.
- `TxnOffsetCommitRequest`:
  - reshaped Builder: `forTopicNames` / `forTopicIdsOrNames` (7340eefc48, 2342c80dca);
  - `supportsGroupIdNotFoundError` / `supportsStaleMemberEpochError` (723847904b);
  - `getTopics`.
- `TxnOffsetCommitResponse`:
  - reshaped Builder and `newBuilder(useTopicIds)` (20c2450e5b, baa064e422);
  - `useTopicIds`;
  - topic-level structure preserved (89f3888c87);
  - 319dd61cb3's client-side hunk.
- `TransactionManager`:
  - topic IDs wired through (83976543fe);
  - `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH` handling (7f5861817d);
  - v6 marked stable (b9945c8e84).
  - Every new call site must keep the explicit `Caller` (producer-transactions.md §1).
- `KafkaProducer` + `ProducerMetadata`: refresh metadata before TxnOffsetCommit, so topic IDs are known
  (6208dfc014). The await must not hold a `TransactionManager` guard (§4).
- Deterministic topic order before encoding (producer-transactions.md §10).
- Tests: `TxnOffsetCommitRequestTest` / `TxnOffsetCommitResponseTest`, the `RequestResponseTest` hunks,
  `TransactionManagerTest` (+157), `KafkaProducerTest` (+160, incl. the 930ebc5608 deflake), `SenderTest`,
  `MessageTest`. Byte-level v6 encoding test.

Phase 5 completion notes (agent 95):

- **Specs (7562044781, b9945c8e84).** `TxnOffsetCommitRequest.json` / `TxnOffsetCommitResponse.json` synced:
  `rust/generator/messages/` now matches `kafka/` 4.4.0-rc4 exactly. Both files were already tracked, so
  the `*.json` ignore rule did not apply; no new file was added in this phase.
- **How v6 is kept from going out without topic ids.** Three layers, all Java's:
  1. `txn_offset_commit_request::Builder::for_topic_names` caps at v5 (v4 without TV2); only
     `for_topic_ids_or_names` reaches v6.
  2. `TransactionManager::txn_offset_commit_handler` picks `for_topic_ids_or_names` only when every topic
     resolved to a non-zero id in `metadata.topic_ids()` (`TransactionManager.java:1295-1297`).
  3. `build_version` rejects a v6 topic with the zero id ("... does require usage of topic ids.") and a
     v0-5 topic with an empty name ("... does require usage of topic names."). This is where Java enforces
     the two `ignorable` fields, which the generated writer drops silently (producer-transactions §11).
  Step 1 committed the spec together with the reshaped builder and switched the manager to
  `for_topic_names`, so no intermediate commit could negotiate v6.
- **§12.** `for_topic_names` translates Java's constants (`(short) 5`, `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`);
  `for_topic_ids_or_names` translates Java's deliberate `ApiKeys.TXN_OFFSET_COMMIT.latestVersion()`
  (`TxnOffsetCommitRequest.java:94`) as `latest_version()`, marked at the site. v6 is stable, so both
  accessors return 6; a test pins that. The 4.3.1 `build()` clamp is gone, as in Java.
- **§10.** `get_topics` / `get_topics_with_topic_ids` and the response's map constructor sort by topic, then
  partition. `txn_offset_commit_handler` sorts the caller's offsets the same way before its single pass,
  where Java walks the caller's map (rules-errata item 1). The response is now read in wire order, as
  Java does; the old sorted flattening (`errors()`) is gone.
- **§1-§4.** No new transition call site: the handler's arms reuse `abortable_error` / `fatal_error`. The
  manager gains `metadata: Arc<Metadata>`, read under the manager's lock (manager → `Metadata`, as in
  Java). `Metadata::update` does call out under its own lock (`ProducerMetadata`'s closures,
  `ClusterResourceListeners`), but none of those callbacks takes the manager's lock or
  `pending_requests`, so the order cannot invert; the field doc records this invariant (corrected
  after review, COMMENTS.95 L2). `send_offsets_to_transaction`
  awaits the metadata refresh before it takes `pending_requests` and the manager lock.
- **What landed, per class:**
  - `TxnOffsetCommitRequest`: the private constructor plus the two factories (7340eefc48, 2342c80dca);
    `supports_group_id_not_found_error` / `supports_stale_member_epoch_error` (723847904b);
    `get_topics_with_topic_ids`; and the static `getErrorResponse(data, errors)` as
    `get_error_response_with_request`, which carries topic id and name (baa064e422). The Rust-only
    `TxnOffsetCommitRequestBuilderOptions` / `...OptionsBuilder` are deleted with the constructors they served.
  - `TxnOffsetCommitResponse`: `use_topic_ids` (319dd61cb3's only client hunk), `new_builder` and the
    builder surface (20c2450e5b, baa064e422); `errors()` removed (89f3888c87). Module
    `txn_offset_commit_response` is now `pub`, so the nested builder is `txn_offset_commit_response::Builder`.
  - `TransactionManager` (83976543fe, 7f5861817d): topic ids, the id -> name snapshot, the unknown-id skip
    with Java's warning, `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH` as `CommitFailed`, the per-topic
    "stop once completed" break. The request is now built from the offsets **passed in**, not from all of
    `pendingTxnOffsetCommits` (a 4.4 behaviour change, tested).
  - `ProducerMetadata::add_with_topics` and `KafkaProducer::await_topic_metadata` (6208dfc014);
    `configure_transaction_state` passes the metadata (83976543fe).
- **Tests.**
  - `TxnOffsetCommitRequestTest`: all 9 cases, including the 4.4 override of `testGetErrorResponse`, which
    was previously skipped as broker-only.
  - `TxnOffsetCommitResponseTest`: all 8 cases, each builder case over both builder flavours.
  - `RequestResponseTest`: the `createTxnOffsetCommitRequest(version)` /
    `...WithAutoDowngrade` arms, as two tests.
  - `MessageTest`: both TxnOffsetCommit cases, per version.
  - `TransactionManagerTest`: `testGroupMetadataMismatchErrorInTxnOffsetCommit` (both codes, exact
    messages) and the three v6 tests. Both `prepareTxnOffsetCommitResponse` translations (TM and Sender)
    gain `assertTxnOffsetCommitRequestUsesTopicNames`.
  - `KafkaProducerTest`: both 6208dfc014 tests, with 930ebc5608's frozen `MockTime`.
  - `SenderTest`: its hunks are only the extra constructor argument.
  - Byte-level (DoD #3): request v6 (16-byte id, no name) and v5 (compact name, no id), response v6 and v5,
    each derived field by field from the spec in declaration order.
  - Rust-only additions: the unknown-id skip, offsets-passed-in-only with sorting, the metadata-wait
    timeout (asserting `"Failed to update metadata after 100 ms."`), `use_topic_ids` / `supports_*`
    thresholds, and the group-metadata-check-first ordering.
  - Teeth check: making `send_offsets_to_transaction` skip the metadata wait fails all three new
    `KafkaProducer` tests.
- **Recorded skips and deviations:**
  - `awaitTopicMetadata`'s no-refresh branch calls `metadata.maybeThrowBootstrapFatalException()` (KIP-909).
    That method does not exist on this branch: **Phase 2 owns it, and the merge must add the call** in
    `KafkaProducer::await_topic_metadata` (the rustdoc says so).
  - Java's `TxnOffsetCommitResponse.Builder` / `TopicIdBuilder` / `TopicNameBuilder` are one Rust `Builder`
    over a private `TopicIndex` enum (DoD #7, documented on the type): the subclasses differ only in the
    lookup key. The nullable `Uuid` / `String` parameters are `Option`s, and Java's
    `IllegalArgumentException("TopicId cannot be null." / "TopicName cannot be null.")` is
    `LocalIllegalArgument` with the same text. `merge` into an empty builder adopts the data without
    indexing it, exactly as Java does.
  - The static `getErrorResponse` is `get_error_response_with_request` (CLAUDE.md §2: it shares the Java
    name with the instance method; `request` is its discriminating parameter).
  - This crate's `MockClient` has no metadata updater (Java's `MockClient.poll` applies
    `prepareMetadataUpdate` / `updateWithCurrentMetadata`). The `KafkaProducer` tests run a small
    responder beside the operation that does that. The `TransactionManager` tests are manager-level, so a
    helper negotiates the version the way `NetworkClient` does (`latest_usable_version_in_range` against
    node 0).
  - The Sender / RecordAccumulator test fixtures build their `TransactionManager` before the context's
    metadata exists, so each gets a separate empty `Metadata` (`txn_manager_metadata()`, documented). None
    of those tests seeds an id for an offset-commit topic, and Java's shared metadata carries none either,
    so the request is the same v0-5 one.
  - Not Phase 5: the other 4.4 `ProducerMetadata` changes (36a69b49c5 `Timer` `awaitUpdate`, 9f15f3c540
    lock-free `add` fast path and `retainTopic` CAS, the constructor losing `Time`) are classified N in
    §6 and were not touched.
  - Pre-existing, out of scope: `KafkaProducerMetrics::record_send_offsets` has no call site in Rust
    (the module carries an `expect(dead_code)` for the transactional sensors). Java records it after the
    send-offsets wait; 6208dfc014 did not change that line.
  - `testMeasureTransactionDurations`' new `metadata.add("topic", ...)` hunk is already what Rust's
    `TxnProducerContext` does by default, so nothing changed there.
- **DoD #10:** N/A: transactional control requests, not the per-record path.
- **For Phases 6-8 (same track):**
  - `TransactionManager::new` now takes `metadata: Arc<Metadata>` (7 parameters), and
    `KafkaProducer::configure_transaction_state` takes `&Arc<Metadata>`. Test fixtures: TM tests use
    `test_metadata()` / `manager_with_metadata(..)`; Sender and RecordAccumulator tests use
    `txn_manager_metadata()`.
  - `TxnProducerContext::with_metadata_seed(extra, n, tv2, track_topic, with_topic_ids)` and the
    `topic_metadata_update` helper exist in `kafka_producer.rs` tests.
  - Phase 6 (rack-aware) touches `RecordAccumulator::PartitionerConfig` and `KafkaProducer` construction.
    This phase changed only the `configure_transaction_state` call there (one argument).
  - Gate on `cargo xtask lint --keep-going`. `check-java-name` rejects a test whose `#[doc(alias)]` names a
    Java test it is not named after, so a test that translates only part of a Java test should carry no alias.
- **Timing log** (2026-10-07, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, PLAN, errata, Java diffs) | 20:29 | 20:35 | 6 |
  | 1 specs + `TxnOffsetCommitRequest` + MessageTest | 20:35 | 20:43 | 8 |
  | 2 `TxnOffsetCommitResponse` + response by topic | 20:43 | 20:49 | 6 |
  | 3 `TransactionManager` topic ids + new errors + tests | 20:49 | 20:58 | 9 |
  | 4 `KafkaProducer` / `ProducerMetadata` refresh + tests | 20:58 | 21:03 | 5 |
  | 5 gates (format-check, lint + fixup, full `cargo test`) | 21:03 | 21:08 | 5 |
  | 6 `make -k verify` (twice, see below), completion notes | 21:08 | 21:29 | 21 |

- **Verification:**
  - `cargo build` passes. `cargo xtask format-check` passes.
  - `cargo test`: 4330 passed, 0 failed, 10 ignored (lib 4281 / 3 ignored, plus 36, 8 and 5 / 7 ignored).
  - `cargo xtask lint --keep-going`: lint-custom reports exactly the 13 remaining §5.1 rows (Phases 7/8 2,
    Phase 9 1, Phase 11 10), nothing new. Every other step passes: the other custom lints, doc-hygiene,
    module-path-hygiene, and all three clippy passes. A first run found two of this phase's test aliases
    (`RequestResponseTest#testSerialization` on differently named tests); fixed in a fixup.
  - `make -k verify` (macOS, 21:19-21:27) fails in three targets, none of them this phase's:
    - `build-c`: `cmake: command not found`. Environment: cmake is not installed on this host.
    - `lint`: the 13 remaining §5.1 rows only. Expected under the §5.1 gate rule.
    - `test-rust-all-features`: 4508 passed, 17 failed, 3 ignored. All 17 are Docker-backed
      `integration_tests::*` ("failed to list networks": the Docker daemon is not running).
    Everything else passes: Python unit tests (363 passed, 2 skipped), `check-bindings` / format-arity
    (29 passed), and the soak tests (156 passed).
    The first `make -k verify` run (21:08) is discarded: its log file was shared with the parallel Phase 2
    agent and holds that worktree's output.
  - **Owed:** a Docker-backed run of the integration suite, in particular
    `producer_transactions_test` against a 4.4 broker, which now negotiates TxnOffsetCommit v6 when the
    offsets' topics have ids. Also a `build-c` / C-test run on a host with cmake (this phase changed no C
    surface).

Merge of Phase 2 (agent 95, 2026-10-07/08):

- **Merge.** `4b1d6f54` merges exactly `ed2e3ce3` (Phase 2, reviewed clean), not the
  `milestone-16-ak-4.4` tip, into `milestone-16-producer` with `--no-ff`.
  - **The one textual conflict** was in `rust/src/common_client_configs.rs`: Phase 6's `CLIENT_RACK_CONFIG` /
    `DEFAULT_CLIENT_RACK` and Phase 2's `BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG` /
    `DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS` were added at the same spot. Both are kept.
  - **`KafkaProducer` construction auto-merged.** Phase 2's `maybe_bootstrap_metadata_synchronously`
    follows the `ProducerMetadata` creation, and `set_bootstrap_configuration` follows the `NetworkClient`,
    where Java has them. Phase 5's `configure_transaction_state(.., &metadata.metadata_arc(), ..)` and
    Phase 6's `build_accumulator` / partitioner-close structure are unchanged.
  - **`PLAN.md` auto-merged.**
- **Follow-up `335c0b67`** (the Phase 5 merge item):
  - `await_topic_metadata`'s no-refresh branch now calls `Metadata::maybe_return_bootstrap_fatal_error`.
    The refresh branch already surfaces the error through Phase 2's `maybe_return_fatal_error` inside
    `await_update`.
  - `KafkaProducerTest.testProducerSendOffsetsToTransactionBootstrapResolutionExceptionPropagated` is
    translated (Phase 2 had left it to Phase 5), with the exact message asserted.
  - Teeth-checked: without the check, the second call fails with
    "Cannot send offsets if a transaction is not in progress" instead.
- **Gates after the merge:**
  - `cargo test`: 4376 passed, 0 failed, 10 ignored (lib 4327).
  - `cargo xtask format-check` passes.
  - `cargo xtask lint --keep-going`: exactly the 13 §5.1 rows.
- **Broker suites** (`producer_transactions_test`, `transactions_bounce_test`, `admin_transactions_test`,
  `bootstrap_resolution_test`; 56 tests in one parallel run per tag):
  - **4.2.0 (default):** 51 passed, 5 failed (`producer_transactions_test`). All 5 failed with
    load-shaped timeouts: "Timed out waiting for a node assignment" (createTopics / listOffsets) and an
    EndTxn commit timeout.
    - Run alone, serially: 4 passed, and the fifth then hit `TopicExists` on its own create (a retried
      create). That fifth test, run alone twice more, passed both times.
  - **4.4.0-rc4** (`INTEGRATION_TEST_BROKER_TAG=4.4.0-rc4`): 44 passed, 12 failed, all
    `producer_transactions_test`, with the same timeout shapes plus consequent assertion failures.
    - Rerun serially: all 12 passed (16 matched by the filters).
  - In both tags `admin_transactions_test` (11), `transactions_bounce_test` (1) and
    `bootstrap_resolution_test` (4) passed in the parallel run.
  - **Note for the Critic:** Critic 95 saw the 4.2.0 suite pass whole before the merge. The parallel-run
    failures appear only with Phase 2 merged and under load. Several admin logs say "Metadata is not ready
    because it contains bootstrap nodes", so a load interaction with KIP-909's lazy bootstrap is not ruled
    out; serial runs are clean.
- **TxnOffsetCommit on the wire** (`RUST_LOG=confluent_kafka::network_client=debug`, request headers):
  - **4.2.0:** 44 sent at v5, 2 at v4 (TV1), none at v6. The request data carries topic ids, since every
    topic resolved one, but the broker caps the negotiation at v5, so `Name` is what is written.
  - **4.4.0-rc4:** 40 sent at v6 and 2 at v4 (TV1) in the parallel run; 15 at v6 in the serial rerun. A
    v6 request carries `topic_id` for each topic.
  - **v6 responses** (`transaction_manager=debug`, two tests): all 14 `TxnOffsetCommit` responses are
    keyed by id alone (`name: ""`) and resolved through the build-time snapshot, with no "unknown topic id"
    warning. Partition codes: 29 × `NONE`, 24 × `CONCURRENT_TRANSACTIONS` (51, retriable, retried to
    success). Both tests passed.

### Phase 6 — Producer: rack-aware partitioning (agent 96)

- KAFKA-19193 (a3f17327de, 88b48794ea, 165d7ec933, fc18c47efd docs):
  - `partitioner.rack.aware`, and producer `client.rack` in `ProducerConfig`;
  - `BuiltInPartitioner` (+98) with rack-local load stats and trace logging;
  - `RecordAccumulator` `partitionerRackAware` / `rack` and `ConfigException` on an empty rack;
  - `KafkaProducer` wiring.
- Tests: `BuiltInPartitionerTest` (+228), and the `RecordAccumulatorTest` / `SenderTest` hunks.
- DoD #10 applies: the partitioner is on the send path.
- Runs before Phases 7–8 (they also edit `RecordAccumulator`).

Phase 6 completion notes (agent 96):

- **What landed** (06469f7a, d0171254 + fixup 33506759):
  - `ProducerConfig`: `PARTITIONER_RACK_AWARE_CONFIG` (boolean, default false, Java's doc text) and
    `CLIENT_RACK_CONFIG` / `DEFAULT_CLIENT_RACK`. These alias new `CommonClientConfigs::CLIENT_RACK_CONFIG` /
    `DEFAULT_CLIENT_RACK`. `ConsumerConfig::CLIENT_RACK_CONFIG` now reuses the common constant, as Java's
    does. `client.rack` is trimmed like every `Type.STRING`.
  - `BuiltInPartitioner`:
    - the `rack_aware` / `rack` fields and the fc18c47efd parameter docs;
    - `PartitionLoadStatsHolder { total, in_this_rack }`, `create_partition_load_stats_for_this_rack_if_needed`,
      `invert_and_fold_queue_size_array` and `load_stats_in_this_rack_range_end`;
    - the rack-aware uniform choice;
    - the 88b48794ea trace log, behind `log_enabled!(Trace)`, which is Java's `isTraceEnabled()`.
    `update_partition_load_stats` gains `partition_leader_racks: &[Option<&str>]`. A `None` there is a
    leader with no rack, which is Java's `null` array element.
  - `RecordAccumulator`:
    - `partitioner_rack_aware` / `rack: Arc<str>`, handed to each new topic's partitioner through a
      translated `create_built_in_partitioner`;
    - `partition_ready` fills a `partition_leader_racks` vec beside `queue_sizes`, allocated under the
      same condition as Java's `String[]`;
    - `PartitionerConfig::new(adaptive, timeout, rack_aware, rack) -> Result`, which returns
      `Error::Config("client.rack must be provided if partitioner.rack.aware is enabled")` when the rack is
      blank (165d7ec933). Its fields are now **private**, as in Java, so the check can't be bypassed.
      `Default` is Java's no-arg `this(false, 0, false, "")`.
  - `Utils::is_blank`: Java trims chars <= U+0020, not Unicode whitespace.
  - `KafkaProducer` passes both configs whether or not a custom partitioner is set (Java does too), so
    construction fails, wrapped in "Failed to construct kafka producer", even with `RoundRobinPartitioner`.
  - The soak client's `PRODUCER_CONFIG_KEYS` gains both keys.
  - Bindings (§2.5): both are plain string config keys, so they pass through the C/Python config maps
    unchanged. No FFI surface was added.
- **Tests:**
  - `BuiltInPartitionerTest`: all 6 cases. The two `@ParameterizedTest`s loop over their `@CsvSource`
    rows (4 and 3); Java's empty CSV rack (`null`) is `""`.
  - The test `SequentialPartitioner` used to re-implement `nextPartition` in the fixture, which DoD #12
    rules out. It now only replaces `randomPartition()`, through a `#[cfg(test)] mock_random` seam on
    `BuiltInPartitioner`, as Java's subclass does.
  - `RecordAccumulatorTest`: 4.4's hunk only changes `testAdaptiveBuiltInPartitioner` /
    `createTestRecordAccumulator` constructor arguments. `testAdaptiveBuiltInPartitioner` and
    `testUniformBuiltInPartitioner` had never been translated (listed missing since M2 Phase 6). Both are
    translated now, through a `#[cfg(test)] set_mock_random_for_test` that stands in for Java's
    `createBuiltInPartitioner` override.
  - `testUniformBuiltInPartitioner` builds its `Cluster` from a vec, as Java does. A
    `MetadataSnapshot`'s cluster view does not keep partition order.
  - `SenderTest`: the hunk is only the constructor argument (`PartitionerConfig::new(false, 42, false, "")`).
  - `ProducerConfigTest`: no 4.4 hunk.
  - Rust-only additions:
    - a rack-aware run of the adaptive accumulator test, which pins that the racks come from the snapshot's
      leader nodes;
    - the `PartitionerConfig` blank-rack check, with the exact message;
    - config parsing;
    - producer wiring, with the exact cause message;
    - `test_rack_aware_uniform_choice_matches_filtered_list`;
    - `test_partition_switch_does_not_allocate`.
  - Teeth checks:
    - Disabling the rack branch in `next_partition` fails 3 partitioner tests.
    - Writing `None` racks in `partition_ready` fails the accumulator rack test.
    - Collecting a `Vec` in the rack branch fails the allocation test (20 allocations).
- **DoD #10.** The partition choice runs on the send path at every sticky switch, roughly once per
  `batch.size` bytes.
  - Java collects the in-rack partitions into a new list. Rust counts them, then indexes the
    `(random % n)`-th in place: same choice, no allocation. `test_rack_aware_uniform_choice_matches_filtered_list`
    checks this against the filtered list.
  - The rack is an `Arc<str>`, refcounted once per new topic. It is never cloned per record. The
    in-rack test compares `&str` with no copy.
  - Nothing is boxed. `test_partition_switch_does_not_allocate` asserts zero allocations over 20 switches,
    for uniform and adaptive, each with rack-aware off, on, and on with no in-rack partition.
  - The existing producer-level allocation tests send keyed records or explicit partitions, so they can't
    reach the switch. They were not extended, and the partitioner-level test covers this instead.
  - The extra `Vec`s are per topic per `ready()` call (in-rack CFT and ids; the racks vec), not per record.
    Java allocates the same arrays there.
- **Recorded skips and deviations:**
  - No Java test skipped.
  - `with_built_in_partitioner_for_test` (closure over the locked partitioner) stands in for
    `getBuiltInPartitioner`. Because it is Rust-only, it carries no Java marker (the lint fixup).
  - Pre-existing, not fixed: `ProducerConfig::parse_bool` reports `"Invalid value X for configuration K"`.
    It is missing Java's `": Expected value to be either true or false"`, and it is case-sensitive where
    Java's `BOOLEAN` parse is not. This affects every boolean key. The new test asserts the prefix only and
    says why. It is a candidate for the Phase 13 config audit.
- **For Phases 7–8 (`RecordAccumulator`):**
  - `PartitionerConfig` has private fields: build it with `PartitionerConfig::new(..).unwrap()` in tests,
    or `PartitionerConfig::default()`. Struct literals no longer compile.
  - New topic partitioners come only from `create_built_in_partitioner` (Java's override point). Keep any
    new `TopicInfo` construction going through it, or the test seam and the rack settings are lost.
  - `partition_ready` now has three parallel arrays (`queue_sizes`, `partition_ids`,
    `partition_leader_racks`) indexed by `queue_sizes_index`. Keep them in step if the drain or ready loop
    is reshaped.
  - `#[cfg(test)]` fields `mock_random` on `RecordAccumulator` and `BuiltInPartitioner` must be kept in
    any new struct-literal constructor.
  - `KafkaProducer::new_inner` closes a configured custom partitioner when `build_accumulator` fails
    (KAFKA-2121, Critic 96 F1). Put any new fallible constructor step that follows the partitioner's
    `configure` inside `build_accumulator`, e.g. KIP-1332's "does not support compression yet"
    `ConfigException` (`KafkaProducer.java:475-480`), so it is covered.
- **Timing log** (2026-10-07, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, PLAN, Java diffs, Rust sources) | 22:06 | 22:11 | 5 |
  | 1 `BuiltInPartitioner` + `BuiltInPartitionerTest` | 22:11 | 22:17 | 6 |
  | 2 `Utils::is_blank`, `PartitionerConfig`, accumulator, configs, producer, tests, commits | 22:17 | 22:24 | 7 |
  | 3 gates: format-check, lint (+ fixup), full `cargo test` | 22:24 | 22:28 | 4 |
  | 4 producer integration tests (Docker) | 22:28 | 22:31 | 3 |
  | 5 `make -k verify`, rerun of its failures, notes | 22:31 | 22:41 | 10 |

- **Verification:**
  - `cargo build` passes. `cargo xtask format-check` passes.
  - `cargo test`: 4341 passed, 0 failed, 10 ignored (lib 4292 / 3 ignored, plus 36, 8 and 5 / 7 ignored).
  - `cargo xtask lint --keep-going`: exactly the 13 remaining §5.1 rows, nothing new. All other steps pass.
    The first run had a 14th finding, this phase's alias on the test-only helper; fixed in 33506759.
  - `cargo test --features integration-tests --test integration -- producer` (Docker, default broker):
    93 passed, 0 failed.
  - `make -k verify` (22:31-22:39) fails in three targets:
    - `build-c`: `cmake: command not found`. This is the environment: cmake is not installed here.
    - `lint`: the 13 §5.1 rows only, which the §5.1 gate rule expects.
    - `test-rust-all-features`: 4536 lib tests passed; integration 298 passed, 3 failed. The failures are
      `producer_transactions_test::test_fatal_error_after_invalid_producer_id_mapping_with_tv2`
      ("Transaction state never expired."), `..._test_transaction_after_producer_id_expires_with_tv2`, and
      `plaintext_consumer_assign_test::test_async_poll_after_topic_deleted`. All three pass on rerun in
      isolation (3/3), and both transaction tests also passed in the producer-only run above. They depend
      on broker producer-id or transaction expiry timing under full-suite load. None takes the rack-aware
      path, since `partitioner.rack.aware` defaults to false.
    - Everything else passes: Python unit tests (363 passed, 2 skipped), `check-bindings` (29), soak (156).

### Phase 7 — Producer: KIP-1332 part A — chunked pool, stream, builder (agent 97)

- `BufferPool`:
  - `AllocationMode { Full, Incremental }`;
  - `allocate_chunks` (async, FIFO single-waiter fairness, refund on timeout / close / error);
  - the extraction of `await_memory`, `signal_next_waiter_if_memory_available`, `release_reserved_bytes`
    and `record_buffer_exhausted`;
  - the mode guards on `allocate` / `allocate_chunks`;
  - the non-blocking (0 ms) path used by extension.
- `ChunkedByteBufferOutputStream` (new file): `io::Write` across fixed 16 KiB chunks that **never grow**,
  `position()` / `set_position`, `add_buffers`, release of unused chunks on close, flatten on `buffer()`.
- `MemoryRecordsBuilder`: the single/chunked buffer type (§2.3), `estimated_bytes_written_after`, and
  `buffer_stream()`. The reopen/rewrite path must work with the flattened buffer.
- Tests: `BufferPoolChunkAllocationTest` (+402), `ChunkedByteBufferOutputStreamTest` (+312). Rust-only
  tests: a cancelled `allocate_chunks` neither leaks waiters nor memory; no chunk grows past its capacity.
- DoD #10: re-run the hot-path allocation test, and do a throughput A/B on the default `full` path to show
  no regression.

#### Phase 7 completion notes (agent 97)

Commits: `005844bf` (BufferPool), `3271f906` (ChunkedByteBufferOutputStream), `14eec39c`
(MemoryRecordsBuilder), `2139ba84` (Java-name fixup of the three).

- **What landed:**
  - `BufferPool`: `AllocationMode { Full, Incremental }` (`buffer_pool::AllocationMode`, Display
    `FULL` / `INCREMENTAL`), `with_allocation_mode`, `allocation_mode()`, and the mode guards with
    Java's messages. `allocate_chunks` is async, takes one FIFO waiter per request, and refunds on
    timeout, close, a `record_wait_time` error, and (Rust-only) a dropped future (`ChunkWaitGuard`).
    `try_allocate_chunks` is the sync 0 ms path. Extracted: `await_memory`,
    `signal_next_waiter_if_memory_available` (renamed from `maybe_signal_next_waiter`),
    `release_reserved_bytes` (a guard that runs if raw chunk allocation unwinds),
    `record_buffer_exhausted`, `return_if_chunks_needed_exceeds_pool`, and `allocate_byte_buffer`.
    `allocate` behaves as before; its wait now goes through `await_memory`.
  - `ChunkedByteBufferOutputStream` (crate-private, `producer::internals`): chunks are `Box<[u8]>`,
    so a chunk cannot grow. It stores one write position, not one per chunk; chunks before the current
    one are always full (see the struct docs). The flatten cache is a `bytes::Bytes`, and in-place
    header writes go through `rewrite_buffer`. It implements `io::Write` (a short write, then an
    error). `Drop` returns any chunks still attached.
  - `MemoryRecordsBuilder` holds a `ByteBufferOutputStream` (`common::utils::internals`, an enum
    `Single(Vec<u8>)` / `Chunked(..)`). This is the one new type (DoD #7, §2.3).
- **Recorded skips and deviations:**
  - No Java test skipped. In `testConstructorRejectsInvalidChunks`, the `null`-list case has no Rust
    counterpart; the empty list and wrong-capacity cases are translated.
  - `write(ByteBuffer)` is not a separate method. A slice covers it, through `write_with_bytes` and
    `io::Write`.
  - `set_position` (Java `position(int)`) checks capacity before it moves, where Java moves the
    chunk positions first and then throws. It carries no Java marker, because CLAUDE.md §2's setter
    name conflicts with the lint's `position_with_<params>`.
  - `try_allocate_chunks` never enters the waiters queue. Java adds the waiter, times out at 0 ns and
    removes it, so the outcome is the same. It carries no marker (lint).
  - The deadline stays on the tokio clock, as `allocate` already does. `await_memory` returns `()`,
    not the nanos waited.
  - Recycled free-list chunks are not zeroed. They are `resize(capacity)`, which is a no-op for
    chunks; this matches Java's `clear()`.
  - `add_buffers(&mut Vec<Vec<u8>>)` drains only on success, so a caller can refund a refused
    attach (Java's caller keeps its list for its `finally`).
  - `ChunkedByteBufferOutputStream::new` drops its chunks on a validation error. That is reachable
    only with buffers the pool did not allocate.
  - Builder: chunked plus compression appends the compressed output to the stream at close and
    panics if the chunks overflow, like the existing "Failed to finish compression" panic. Phase 8
    must reject compression for the incremental strategy (`KafkaProducer.java:475-480`).
  - `estimated_bytes_written_after` for magic < 2 spells out `LegacyRecord.recordSize` (14 / 22 +
    key + value), since `LegacyRecord` is not translated.
  - `MemoryRecordsBuilder::buffer` is now `&mut self -> Result<&[u8], Error>`, because a chunked
    stream flattens on the first call. `ProducerBatch::buffer` follows (no callers). The dead
    Rust-only `buffer_mut` is removed.
  - §5.1: both shared rows (`ProducerBatch.isWritable`, `RecordAccumulator.recordsBuilder`) are
    removed by Java in `ProducerBatch` / `RecordAccumulator`, which are Phase 8's half, so **neither is
    cleared here**. The lint still has exactly the 13 rows.
- **DoD #10:**
  - Producer allocation tests: `test_send_allocations_do_not_grow_*`: 2 allocations per steady-state
    send, before and after. `test_records_allocations_do_not_scale_with_the_record_count`: first
    `records()` = 1, before and after. Both were measured with a temporary `eprintln` that is not
    committed.
  - Throughput uses a temporary release-mode test that is not committed: 20 000 builders x
    16 KiB, a 10 B key and 100 B values, appended until full and then built, giving 2.72 M records
    per round, best of 5. Before (dbc609d1): 0.0604 s = **45.07 M rec/s**. After (14eec39c):
    0.0582 s = **46.73 M rec/s**. No regression.
  - The chunked path copies once at close (the flatten), and the batch is an O(1) `Bytes` slice of
    it. The reopen path reuses the flattened allocation when nothing else holds it, and copies
    otherwise, as the single path re-copies on every close.
- **Phase 8 API:**
  - Pool: `BufferPool::with_allocation_mode(memory, batch_size, metrics, time, grp,
    AllocationMode::Incremental)`, then `pool.allocation_mode()` for the up-front check.
    - New batch: `pool.allocate_chunks(size: i32, remaining_ms).await -> Result<Vec<Vec<u8>>, Error>`.
      It does **not** record buffer-exhausted; on `ProducerBufferExhausted` the caller calls
      `pool.record_buffer_exhausted()`.
    - Extension: `pool.try_allocate_chunks(size)` (sync).
    - Unattached chunks go back one by one with `pool.deallocate(chunk)`. A bare `Vec<Vec<u8>>`
      has **no** Drop refund, so `AppendGuard` must return them itself.
  - Stream: `ChunkedByteBufferOutputStream::new(chunks, pool.poolable_size(), Some(pool.clone()))?`,
    then:
    - `add_buffers(&mut extension_chunks)?`, which drains on success;
    - `attached_capacity()?`;
    - `deallocate()` / `deallocate_with_pool(Some(&pool))`.
    - Dropping a stream refunds its chunks, which covers a cancelled append holding a
      `NewBatchBuffer`.
  - Builder: `MemoryRecordsBuilder::with_buffer_stream(ByteBufferOutputStream::Chunked(stream),
    RecordBatch::CURRENT_MAGIC_VALUE, compression, TimestampType::CreateTime, 0,
    RecordBatch::NO_TIMESTAMP, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, NO_SEQUENCE, false, false,
    NO_PARTITION_LEADER_EPOCH, max(batch_size, first_record_size), RecordBatch::NO_TIMESTAMP)?`.
    - Use `builder.estimated_bytes_written_after(key, value, headers)` for `extensionBytesNeeded`.
    - `builder.buffer_stream().is_chunked()` is `instanceof`.
    - `builder.buffer_stream_mut().as_chunked_mut()` returns the stream.
  - Returning a batch's chunks: `deallocate_buffer` and `deallocate_inflight_buffer` for a chunked
    batch call `as_chunked_mut().unwrap().deallocate_with_pool(Some(&pool))`.
    - **Never** use `take_buffer()` + `deallocate_with_size(.., initial_capacity())` on a chunked
      batch: `take_buffer` returns an empty `Vec`, and `initial_capacity()` is the chunk size.
    - Unused chunks were already released at `close_for_record_appends`.
  - **Critic 97 notes for Phase 8:**
    - `take_buffer()` on a chunked builder returns an empty `Vec`. Today's
      `RecordAccumulator::deallocate` (`record_accumulator.rs` ~1794-1800) would call
      `deallocate_with_size(Vec::new(), chunk_size)`, crediting a chunk of non-pooled memory, while
      the stream's `Drop` also returns the real chunks. The in-flight branch
      (`vec![0u8; initial_capacity()]`) is wrong the same way. Both over-credit the pool, so
      `buffer.memory` can be exceeded.
      - Phase 8 must branch on `is_chunked()` (Java's `ChunkedProducerBatch.deallocateBuffer` /
        `deallocateInflightBuffer`).
      - A `debug_assert!` in `take_buffer`'s `Chunked` arm would make a missed branch loud.
    - An overflowing chunked append panics: `append_default_record`'s `.expect("I/O error ...")`
      is now reachable through the fallible stream. Java refuses an under-sized first append
      *before* writing (`ChunkedProducerBatch.tryAppend`, `ChunkedProducerBatch.java:82-88`).
      - Translate that refusal as a returned error ahead of the append, and keep the builder panic
        as the last-resort bug guard.
      - The chunked-plus-compression panic at close stays acceptable only while the `ConfigException`
        (`KafkaProducer.java:475-480`) runs before any chunked builder is built, on every
        construction path.
- **Java line-cite convention (Manager decision, Critic 97 L2):** a phase that ports a file's Java
  changes refreshes that file's Java line cites to the new reference in the same phase. Phase 7 did
  this for `buffer_pool.rs` and `memory_records_builder.rs` (and the one `BufferPool.java` cite in
  `ffi/producer.rs`). Its `ProducerBatch.java` cite belongs to Phase 8's file.
- **Critic 97 round 1:** 0 blockers, 2 Low, both fixed.
  - L1: two probe tests were added, for the free-list-before-raw swap and for `set_position` on a
    chunk boundary. Each fails under its mutation. The `test_try_allocate_chunks` docstring
    overclaim is gone. Fixups `b3db22aa` and `f5285aea`.
  - L2: cites refreshed to rc4 in `1d297caa`.
  - Throughput: the Critic's interleaved re-run confirms "no regression"; the +3.7 % above is noise.
- **Timing log** (2026-10-07/08, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, PLAN, Java commit and tests, Rust sources) | 23:58 | 00:09 | 11 |
  | 1 DoD #10 baseline capture (alloc counts, release bench) | 00:09 | 00:12 | 3 |
  | 2 `BufferPool` + `BufferPoolChunkAllocationTest`, teeth check, commit | 00:12 | 00:19 | 7 |
  | 3 `ChunkedByteBufferOutputStream` + its test, commit | 00:19 | 00:22 | 3 |
  | 4 builder buffer type + builder tests, teeth check, commit | 00:22 | 00:26 | 4 |
  | 5 DoD #10 after-measure | 00:26 | 00:29 | 3 |
  | 6 gates (format-check, full test, lint), Java-name fixup | 00:29 | 00:36 | 7 |
  | 7 producer broker tests (under the lock), stopped waiting on a hung test | 00:38 | 01:19 | 41 |
  | 8 Critic 97 round 1 fixes; `make -k verify` | 02:00 | 10:45 | (interrupted; verify 9) |

- **Verification:**
  - `cargo build` passes. `cargo xtask format-check` passes.
  - `cargo test`: 4367 passed, 0 failed, 3 ignored (lib), plus 36, 8 and 5 / 7 ignored.
  - `cargo xtask lint --keep-going`: exactly the 13 §5.1 rows. Clippy is clean. The first run found 6
    of this phase's markers misnamed; `2139ba84` fixed them.
  - `cargo test --features integration-tests --test integration -- producer` (under the broker
    lock): 92 passed, 0 failed, and 1 did not finish.
    `producer_transactions_test::test_read_committed_consumer_should_not_see_undecided_data` was
    still running after 40 minutes.
    - A `sample` of the test process put its CPU (about 50 %) in the **consumer**
      `ConsumerNetworkThread::run_once` → `NetworkClientDelegate::poll` → `NetworkClient::poll`
      (`handle_rebootstrap`, `handle_timed_out_connections`, `maybe_update`), with no
      producer-builder frames.
    - The default `full` path is the only one any producer uses, and it runs the pre-Phase-7 code.
      The other 92 producer tests passed, including every transactional one.
    - So this looks like a consumer-side busy loop (possibly from the KIP-909 / Phase 2 merge) and
      not this phase. It is **unverified**: the process could not be stopped from here to re-run
      the test alone.
  - The hung run was ended externally (SIGTERM, about 10:39). By then it had also reported
    `test_send_offsets_with_group_metadata` FAILED, under three-cluster load. That test passed in the
    verify run below.
  - `make -k verify` (2026-10-08 10:36-10:45): `lockf -t 60` timed out while the hung binary held the
    lock. Per the Manager's instruction it then ran without `lockf`, at load 5.1. Results:
    - lib: 4613 passed, 3 ignored.
    - Integration: 305 passed, 0 failed, 2 ignored. This includes
      `test_read_committed_consumer_should_not_see_undecided_data` and
      `test_send_offsets_with_group_metadata`, both ok.
    - Python: 363 passed, 2 skipped.
    - check-bindings: 29 + 29.
    - soak: 156.
    - It failed only in `build-c` (`cmake: command not found`, the environment) and `lint` (the 13
      §5.1 rows).

### Phase 8 — Producer: KIP-1332 part B — accumulator, batch, producer wiring (agent 98)

- `RecordAccumulator` refactor (+164/−64): `topic_info_for`, `partition_changed`, `set_partition`,
  `update_partition_info_on_append`, `append_new_batch` taking the try-append / create-batch steps,
  `RecordAppendResult::needs_extension`, and batch-level deallocate hooks.
- `ChunkedRecordAccumulator` (new file, composition per §2.3):
  - `append` with the extension path and the new-batch path;
  - a shared `max.block.ms` budget;
  - refund of chunks when the partition changes;
  - the `AppendGuard` extended to new-batch and extension chunks.
- `ProducerBatch` + `chunked_producer_batch.rs`: `extension_bytes_needed`, `add_buffers`, chunk-aware
  `deallocate_buffer` / `deallocate_inflight_buffer`, `is_chunked`. Remove `is_writable` if Rust has it.
- `ProducerConfig` `buffer.memory.allocation.strategy` (internal, default `full`, case-insensitive), plus
  `KafkaProducer` wiring:
  - fall back with a warning when `batch.size` < 16 KiB;
  - `ConfigException` when compression ≠ none.
- **D3:** KAFKA-20864 (`cc6d42206f`, trunk): close the batch only if it is the one being extended; bound
  retries by the remaining `max.block.ms`. This also covers its `RecordAccumulatorTest` (+51) and
  `ChunkedRecordAccumulatorTest` (+567) additions. Record it as ahead-of-4.4.
- Tests: `ChunkedRecordAccumulatorTest` (+716, plus the D3 additions), `ProducerConfigTest` (+35), and the
  `KafkaProducerTest` / `RecordAccumulatorTest` hunks. Integration: rerun the producer send integration
  tests with `buffer.memory.allocation.strategy=incremental`
  (`IncrementalAllocationProducerSendTest` / `BaseProducerSendTest` analog).
- Deliverable: draft the `ProducerBatch`-fold rules note into `rules-errata.md` (§2.3).
- DoD #10 applies.

#### Phase 8 completion notes (agent 98)

Commits: `6442a838` (accumulator, batch fold, chunked accumulator, config, producer wiring),
`6573e08b` (D3, KAFKA-20864), `d529098c` (separate chunked guard for DoD #10; rules note),
`1daace6b` (Java cites to rc4), `5475f4e4` (IncrementalAllocationProducerSendTest analog), plus this
notes commit.

- **How the types compose (§2.3).**
  - `ChunkedRecordAccumulator { base: Arc<RecordAccumulator>, chunked_free: Arc<BufferPool> }` in
    `chunked_record_accumulator.rs`, with its own `append` / `try_append` / `create_producer_batch` /
    `chunked_records_builder` / `allocate_extension_chunks` / `deallocate_extension_chunks`.
    `ChunkedRecordAccumulator::new` builds the base itself (Java's `super(..)`) after Java's two
    checks (incremental pool; compression `none`).
  - `RecordAccumulator::append_new_batch` takes Java's two virtual steps as parameters: a
    `try_append` closure, a records-builder supplier, and a `create_producer_batch` closure. Both
    strategies use it; neither boxes anything.
  - `Sender` is unchanged (`Arc<RecordAccumulator>`). `KafkaProducer` holds `accumulator` (the shared
    base) and `chunked_accumulator: Option<ChunkedRecordAccumulator>`; `do_send_bytes` dispatches
    `append` on it (two concrete awaits, no boxed future). `build_accumulator` picks the strategy
    and returns both.
  - `ChunkedProducerBatch` is `pub(crate) type ChunkedProducerBatch = ProducerBatch;` (carrying the
    Java marker) plus an `impl` block in `chunked_producer_batch.rs` (`new_chunked`, `is_chunked`,
    `extension_bytes_needed`, `add_buffers`, `stream`). The three overrides branch on `is_chunked()`
    inside `ProducerBatch::try_append` / `deallocate_buffer` / `deallocate_inflight_buffer`.
- **DoD #7 deviations.**
  - The `ProducerBatch` fold (above; rules note drafted in `rules-errata.md`).
  - `ChunkedAppendGuard`: the chunked `append`'s `finally` (new-batch stream, extension chunks,
    `appendsInProgress`) as a `Drop` type, for the same cancellation reason as `AppendGuard`. It is a
    separate type so the full path's per-append future does not grow (see DoD #10).
  - Test-only: `ChunkedAccumulatorTestHooks` (stands in for the anonymous `BufferPool` /
    `ChunkedRecordAccumulator` subclasses of `ChunkedRecordAccumulatorTest`),
    `BufferPool::deallocate_observer`, `ProducerBatch::close_for_record_appends_calls`.
- **Other deviations, recorded.**
  - `RecordAppendResult.future` and `.topic_partition` are `Option`s (Java's nullable `future`); the
    static `appended(..)` factory is `appended_result` (no marker: the instance predicate holds the
    name). `KafkaProducer` turns a non-appended result into an `IllegalState` error, not a panic.
  - `setPartition` has no Rust call: the partition reaches the caller as `topic_partition`
    (pre-existing).
  - `partition_changed` keeps the pre-4.4 Rust shape (no `StickyPartitionInfo` identity), now with
    an `unknown_partition` flag for Java's `partitionInfo != null`. Java tests that rely on
    `super.partitionChanged` detecting a moved info have their hook report the move itself.
  - The full path does not refresh `nowMs` after `allocate` (Java does): kept as before, because
    the brief requires the full path behaviour-identical and many unit tests fix `now`. The
    incremental path refreshes it as Java does.
  - `ChunkedRecordAccumulator`'s compression check returns `Error::UnsupportedVersion` (Java's
    `UnsupportedOperationException`; no Rust variant, same mapping as `MockAdminClient`). The
    producer rejects the combination first with Java's `ConfigException`, inside
    `build_accumulator`, so the partitioner-close path covers it (tested).
  - Java's second constructor without `partitionerConfig` is not separate: pass
    `PartitionerConfig::default()`.
  - An under-sized first chunked append is refused before writing (Critic 97 (b)) and surfaces as
    Java's `IllegalStateException` message from `append_new_batch`. The plain-batch "should have
    room" case is now an error too, not a panic.
  - `RecordAccumulator::deallocate` still panics on an inflight batch (pre-existing); the chunked
    test catches the panic and checks the pool is fully restored.
  - The batch-to-extend identity (D3) is the batch's `ProduceRequestResult`, held as an `Arc` so a
    replacement batch cannot reuse the address.
- **D3, KAFKA-20864 (`cc6d42206f`, trunk; ahead of 4.4).** Ported in `6573e08b`:
  `append_deadline_ms`, `remaining_time_to_block_ms`, `return_if_no_more_retries_allowed`, the
  `batch_to_extend` close check, and every retry bounded by the deadline. Its nine
  `ChunkedRecordAccumulatorTest` and two `RecordAccumulatorTest` additions are translated. These
  items carry no Java marker (the rc4 tree the markers resolve against predates them); a later bump
  past `cc6d42206f` should add them.
- **Skips.** None of the Java tests. Notes:
  - `KafkaProducerTest` hunk (`RecordAppendResult.appended(..)` in a Mockito stub of
    `testPartitionAddedToTransaction`): the Rust test uses a real accumulator, so there is no stub to
    rename.
  - `IncrementalAllocationProducerSendTest.testSendCompressedMessageWithCreateTime` is skipped exactly
    as Java skips it.
  - `RecordAccumulator.recordsBuilder` and `ProducerBatch.isWritable` are removed as in Java; the
    `testFull` hunk now asserts `is_full()` instead.
- **Tests.** `ChunkedRecordAccumulatorTest`: 14 (4.4) + 9 (D3), `@ParameterizedTest` as a loop over
  both values. Rust-only: cancelled append refunds and counts out; `ChunkedAppendGuard` refunds
  stream and chunks; no over-credit on deallocate (completed and inflight); undersized first append
  errors without panicking; constructor messages; steady-state incremental append allocates exactly
  what a full one does. `ProducerConfigTest` +3, `RecordAccumulatorTest` +2 and the `testFull` hunk;
  `KafkaProducer` wiring tests (strategy and fallback, compression `ConfigException`, partitioner
  closed on that failure). Integration: 11 tests in
  `producer_test::incremental_allocation_producer_send`.
- **DoD #10.**
  - Producer send path: `test_send_allocations_do_not_grow_*` = 2 allocations per steady send,
    unchanged (measured with a temporary `eprintln`, not committed).
  - Incremental path: a steady append that needs no extension costs exactly what a full one does
    (1 at the accumulator level; pinned by a committed test). Records that extend the batch add about
    3 allocations per 16 KiB chunk (the chunk, the chunk list, the stream's list growth): 100 x 1000 B
    appends cost 124 allocations against 104 for the full strategy, about 0.2 per record. That is the
    on-demand allocation the strategy exists for, per chunk rather than per record.
  - Throughput, full strategy: a temporary release-mode test, not committed (500 000 appends of a
    10 B key and a 100 B value into 16 KiB batches, best of 5), interleaved runs of 3957e76f and
    HEAD. First version: 10.3-10.7 M rec/s before, 9.65-9.94 after (about -7 %). The extra
    `Option`s in `AppendGuard` were most of it; splitting out `ChunkedAppendGuard` (`d529098c`)
    brought it to 10.19-10.47 after vs 10.70-10.78 before (-2 to -5 %, about 3-4 ns per append).
    Allocation counts are unchanged.
  - **Critic 98 F1 (fixup `77183531`).** That accumulator-only figure understated the cost, which
    was +6 % at `do_send_bytes`. The largest single lever was the folded `ChunkedProducerBatch`
    first-append check running inline in every plain `ProducerBatch::try_append`. Three changes
    were needed together:
    - the check moved into a `#[cold] #[inline(never)]` helper;
    - `#[inline]` on five small helpers, and the two full-path `partition_changed` calls guarded
      with `unknown_partition &&`;
    - `RecordAppendResult` back to 40 B, with the extension size sharing `appended_bytes` and read
      through `extension_bytes_needed()` (a test pins the size).

    Behaviour is unchanged. Re-measured with the Critic's harness: clean `git archive` release
    builds with fat LTO, a current-thread runtime, 500 000 appends of a 10 B key and a 100 B value,
    best of 9 per run, 8 runs interleaved with 3957e76f, at load 4-5. Medians, ns per record:

    | | accumulator, explicit | accumulator, sticky | `do_send_bytes`, explicit | `do_send_bytes`, keyed |
    |---|---|---|---|---|
    | base `3957e76f` | 93.39 | 98.72 | 107.95 | 122.63 |
    | after the fixup | 93.92 (+0.6 %) | 99.81 (+1.1 %) | 109.27 (+1.2 %) | 124.38 (+1.4 %) |

    Allocations per steady send are still 2 at the producer and 1 at the accumulator.
    After the fix: `producer::` lib tests 793 passed; producer broker tests in both strategies 105 of
    105 passed, including all 11 incremental ones; lint shows the 11 §5.1 rows; format-check is clean.
- **Cites.** `KafkaProducer.java`, `RecordAccumulator.java` and `ProducerBatch.java` cites across
  `rust/src` refreshed to rc4 by content (204 cites in 13 files, `1daace6b`), including the bare
  `:1056` / `:1072` in `buffer_pool.rs` (now `KafkaProducer.java:1130` / `:1147`). One historical
  statement in `kafka_producer.rs` (`:1449-1450` vs 4.3.1's `:1446`) is left as written. The cite
  `record_accumulator.rs` "the second `try_append` (`:553-575`)" matched no version, so it now names
  the construct (the `try_append` inside `append_new_batch`, `RecordAccumulator.java:419`). The
  `sender.rs` pool cite names both 4.4 pools, `:494` and `:508` (Critic 98 L2, fixup `9769c54b`).
- **Open questions for the human (Critic 98 S1 and S2).** Both are pre-existing, both would change
  default-path behaviour, and both are left open in `COMMENTS.98.md` as "deferred to human decision".
  - **S1 (Medium): a full-strategy batch's creation time predates the blocking `allocate`.**
    - Java refreshes `nowMs = time.milliseconds()` after `free.allocate`
      (`RecordAccumulator.java:336-340`), and that time becomes the batch's `createdMs`. Rust keeps
      the caller's `now_ms` (`record_accumulator.rs`, the full `append_inner`).
    - The Critic's probe: with MockTime and a full pool, an append parked 5000 ms in `allocate` gets
      `created_ms` 5000 ms before the memory arrived. The incremental path gets Java's value.
    - The consequence: time blocked in `send()`, up to `max.block.ms`, is charged against
      `delivery.timeout.ms`. With `max.block.ms >= delivery.timeout.ms` a batch can be born already
      expired. `record-queue-time` is inflated and `linger.ms` counts as elapsed. The two strategies
      now disagree.
    - Proposed fix: one clock read after `allocate` (once per new batch), plus moving
      `RecordAccumulator::new_for_test` and its callers to a `MockTime`, as Java's tests pass `time`.
      Without that fixture, about 15 Sender tests break (seen during this phase).
  - **S2 (Low): `partition_changed` cannot see a concurrent sticky switch.**
    - Java checks `isPartitionChanged(partitionInfo)` by identity first (`RecordAccumulator.java:244-247`,
      `BuiltInPartitioner.java:195-197`). Rust re-reads the partition under the lock, so a switch
      made between the peek and the deque lock goes unseen.
    - The Critic's probe: append A peeks partition 0 and parks in `allocate`, a concurrent switch
      moves the sticky partition to 1, and memory is freed. Rust lands A on 0 and credits its bytes
      to partition 1's sticky info; Java retries and lands on 1.
    - Phase 8 impact: none on accounting. The chunked second block re-checks the same deque, and
      the two tests whose hooks report the move stay faithful to what they assert.
    - Proposed fix: a switch generation counter on `BuiltInPartitioner`, returned by the peek and
      compared under the deque lock. `testPartitionChangeRetriesBoundedByMaxBlockTime` could then
      call the real check. The wrong `is_partition_changed` doc is already corrected (`6d339e83`).
- **§5.1:** the two shared rows (`ProducerBatch.isWritable`, `RecordAccumulator.recordsBuilder`) are
  cleared and removed. Lint shows exactly the 11 remaining rows.
- **Environment.** Docker Desktop's backend was killed at 11:06 (`com.docker.backend ... signal:
  killed`), so the daemon was down. I quit and reopened Docker Desktop at 11:58 before the broker
  runs. No containers were running at the time.
- **Timing log** (2026-10-08, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (PLAN, Java commit + D3, tests, Rust sources) | 10:55 | 11:03 | 8 |
  | 1 refactor, batch fold, chunked accumulator, config, wiring, 4.4 tests, commit | 11:03 | 11:31 | 28 |
  | 2 D3 + its tests, teeth check, commit | 11:31 | 11:37 | 6 |
  | 3 DoD #10 alloc counts, throughput A/B, guard split, commit | 11:37 | 11:56 | 19 |
  | 4 integration analog (written during step 3's builds) | 11:40 | 11:45 | 5 |
  | 5 cite refresh (subagent, in parallel), cherry-pick, leftovers | 11:42 | 11:57 | 15 |
  | 6 Docker recovery | 11:46 | 11:58 | 12 |
  | 7 producer broker tests, both strategies (under the lock), re-run of the one failure | 11:59 | 12:06 | 7 |
  | 8 gates (format-check, full test, lint), `make -k verify`, integration re-run | 12:06 | 12:20 | 14 |

- **Verification.**
  - `cargo build` passes; `cargo xtask format-check` passes.
  - `cargo test`: 4406 passed, 0 failed, 3 ignored (lib), plus 36, 8 and 5 (7 ignored).
  - `cargo xtask lint --keep-going`: clippy clean; exactly the 11 §5.1 rows.
  - `cargo test --features integration-tests --test integration -- producer` (under the lock):
    104 passed, 1 failed out of 105. All 11 incremental-strategy tests passed. The failure,
    `client_rebootstrap_test::test_producer_rebootstrap_disabled`, was a container startup
    timeout under load, and it passed when re-run alone.
  - `make -k verify` (12:08-12:14, under the lock):
    - lib with all features: 4649 passed, 1 failed, 3 ignored. The failure,
      `integration_tests::api_versions_test::test_metadata_api_version_range`, was a container
      startup timeout, and it passed when re-run alone. That failure stopped `cargo test` before the
      integration target, so I ran it separately afterwards.
    - Python: 363 passed, 2 skipped. check-bindings: 29 + 29. soak: 156.
    - `build-c` failed: `cmake: command not found` (environment).
    - `lint` failed on the 11 §5.1 rows.
  - `cargo test --all-features --test integration` (re-run under the lock): 316 passed, 0 failed of
    the native tests, 2 ignored. All 381 failures were `__grpc_*` multilanguage variants, which cannot
    run on this macOS host (Linux-artifact images).

#### Merge of milestone branch at 75be908e (agent 98)

- **Merge `78378d6a`** (`--no-ff`, exactly `75be908e`). It brings in Phase 3, the `origin/master`
  `a4b3311e` merge and the admin-track merge.
  - There was one conflict, in `rules-errata.md`. Both sides had appended independent draft notes:
    the Phase 5/8 producer notes on this side, the Phase 12 `admin-client.md` §11 note on the other.
    I kept both, producer notes first.
  - Everything else auto-merged, including `selector.rs`, `produce_request.rs`, the record files and
    `PLAN.md`.
  - The producer structures are intact: `build_accumulator`, the chunked dispatch in
    `do_send_bytes`, and the F1 layout (cold first-append helper, `#[inline]` hints, 40 B
    `RecordAppendResult`). None of those files changed in the merge.
  - §5.1 still has the 11 rows, with the Phase 5 and Phase 7/8 rows still cleared.
- **Gates.**
  - `cargo test`: 4508 passed, 3 ignored (lib), plus 36, 8 and 5 (7 ignored).
  - `format-check` is clean.
  - `lint --keep-going`: clippy is clean, and lint shows exactly the 11 rows.
- **Broker runs** (under the lock):
  - Producer tests in both strategies plus `transactions_bounce_test`, `admin_transactions_test`
    and `bootstrap_resolution_test`, on 4.2.0: 116 of 117 passed.
    - All 11 incremental-strategy tests passed.
    - The failure, `test_transaction_after_transaction_id_expires_but_producer_id_remains`, is
      sensitive to transactional-id expiry timing under load. It passed when re-run alone.
  - `producer_transactions_test`, `transactions_bounce_test` and `admin_transactions_test` on
    `INTEGRATION_TEST_BROKER_TAG=4.4.0-rc4`: 52 of 52 passed.
  - Lib `integration_tests::`: 32 of 32 passed.
- **Throughput re-check** (the Critic 98 harness, 8 interleaved runs, best of 9).
  - The merged tree measured +4 to +5 % at `do_send_bytes` against both `3957e76f` and the
    pre-merge head `676ccb05`.
  - The producer hot path is identical across the merge: the same functions and the same call sets
    in the disassembly. The difference is the merged test-only `TrackingAllocator`
    (`test_alloc_tracker.rs`, `#[cfg(test)]`): its new `MAX_ALLOC_SIZE` bookkeeping is inlined into
    every allocation site of the test binary that hosts the bench.
  - With the pre-merge tracker swapped into the merged export (`max_allocation` stubbed), the merged
    tree is +1.2 % (explicit) and +1.9 % (keyed) over `676ccb05` at `do_send_bytes`. That is within
    the harness's noise at load 4-5.
  - Production builds do not include the tracker, so the F1 fix is not regressed.

### Phase 9 — Consumer: heartbeat, membership, commit fixes (agent 99)

- ~~KAFKA-20253 (28de22de34): heartbeat CPU spin, in `AbstractHeartbeatRequestManager`,
  `CommitRequestManager` and `CoordinatorRequestManager`.~~ **Covered by Phase 3** (02ac689c, 98e6c919),
  including its consumer tests; do not port it again. See the Phase 3 completion notes.
- KAFKA-20761 (56410c311b): log the group configs defined on the broker.
- b8429b93a7: no-op call removed.
- KAFKA-20681 (6a6b536fbc): heartbeat-success handling consolidated into `AbstractMembershipManager`
  (consumer half only).
- KAFKA-20145 (e320142b7c): no redundant partial heartbeat acks from network-thread reconcile.
- KAFKA-20765 (fe88647935): OffsetFetch stale-epoch retry spinning.
- Test consolidation e7b0cb7908 (`AbstractHeartbeatRequestManagerTest` +330): mirror it if Rust keeps
  separate files, otherwise record how it maps.
- Tests: `ConsumerHeartbeatRequestManagerTest`, `AbstractHeartbeatRequestManagerTest`,
  `ConsumerMembershipManagerTest` (+44), `CommitRequestManagerTest` (+60), `CoordinatorRequestManagerTest`.
- Check every change against the consumer-threading §31 reconcile / ack machinery (Phase 41 design).

### Phase 10 — Consumer: fetch, offsets, poll, MockConsumer (agent 100)

- KAFKA-20187 (65ffe10e3b): the `endOffsetRequested` flag in `OffsetsRequestManager` (+83) and
  `ApplicationEventProcessor` (−33).
- KAFKA-20312 (d0e0ec478c): a null leader during regroup, in `OffsetFetcherUtils` and
  `OffsetsRequestManager`.
- KAFKA-20780 (20e952c783): clear a completed in-flight poll on an empty fetch.
- KAFKA-15529 (5d03ccff57): the `isConsumed` / position race in `CompletedFetch` / `FetchCollector`.
- KAFKA-18812 (8c0ca4ae35): API errors after a background task failure, in `ApplicationEventHandler` and
  `ConsumerNetworkThread`.
- KAFKA-20570 (f7dbf0bf3b): `ConsumerProtocol` deserialization errors become `SchemaError` / `KafkaError`.
- KAFKA-20575 (2768948823): `MockConsumer::lose_partitions` (public), plus the FFI
  `kafka_consumer_MockConsumer_lose_partitions` and the Python mock binding.
- b69c07c816 ("Bound decompressed record size"), **partial**: consumer-side `DefaultRecord.readFrom` checks
  only (the new "Invalid record size: N is negative." message and the `SOFT_MAX_ARRAY_LENGTH` upper bound;
  Rust already rejects a negative size with different text, `default_record.rs`); the broker-side
  `maxRecordBodySize` iterators stay out of scope. Reclassified from Broker/O by the Manager after the
  Phase 1 review (COMMENTS.91 Q2).
  - **After merging master's `a4b3311e` (#205, decode & decompression DoS; agent 93):** master already
    rejects a negative body size in both `DefaultRecord::read_from_buffer` and `read_from_stream` (the text
    is still "Invalid record size: expected non-negative size but got N", not Java's "Invalid record size: N
    is negative."). On the stream path, `read_from_stream` no longer sizes its buffer from the declared size:
    it reads through `take(size)` and fails with Java's end-of-payload text, so the OOM that motivated
    Java's upper bound is already closed. Phase 10 still owes three things. First, Java's negative-size
    message text. Second, the explicit upper-bound check before the read, "Invalid record size: N exceeds
    the configured maximum record size of M." with `M = Records.SOFT_MAX_ARRAY_LENGTH`
    (`Integer.MAX_VALUE - 8`, `Records.java:62`); that is the client default, and Rust has no such constant
    yet. Third, the tests for both, from the consumer-relevant cases in `DefaultRecordTest`. Rust has no
    `readPartiallyFrom`, no `skipKeyValueIterator` / `streamingIterator(…, maxRecordBodySize)` overloads and
    no legacy deep-decode (`legacy_record.rs` holds only constants), so the `AbstractLegacyRecordBatch` and
    iterator parts of b69c07c816 have no Rust counterpart and stay out of scope.
- Test-only commits:
  - a5137f7c38, 624ca392ef, 7c010c7583, 1a46339e90, 16e976ac8e, 159d696005, c75e10d229 (KafkaConsumerTest
    hunks);
  - 36aab4fddd and 40e9fcd742 (pause/re-assign and unsubscribe-no-autocommit tests, plus rustdoc);
  - 0fd8327920 (single in-flight poll event).
- Tests: `OffsetsRequestManagerTest` (+60), `FetchCollectorTest` (+32), `ConsumerProtocolTest` (+85, unit
  tests in `consumer_protocol.rs` per the consumer-threading §20 carve-out), `MockConsumerTest` (+105),
  `ApplicationEventHandlerTest`, `ConsumerNetworkThreadTest`, and the `AsyncKafkaConsumerTest` /
  `KafkaConsumerTest` / `FetchRequestManagerTest` hunks.
- DoD #10 applies (`CompletedFetch` / `FetchCollector` are on the per-record receive path; §27).
- Also: Critic 96 F3: trim consumer `client.rack` (Java trims Type.STRING) and add `ConsumerConfig::DEFAULT_CLIENT_RACK` (`ConsumerConfig.java:267`).

### Phase 11 — Consumer metrics: sensor lifecycle (agent 101)

- KAFKA-19542 (9a28bd23ad):
  - `MetricsLedger` (new);
  - `AbstractConsumerMetricsManager` (new);
  - `SensorBuilder` moved to the `consumer.internals.metrics` package (mirror it if the Rust layout
    mirrors that package);
  - every metrics manager removes its sensors on close;
  - `AsyncKafkaConsumer` / `FetchMetricsManager` wiring.
- KAFKA-20750 (02c0ce9707): divide-by-zero guard in `KafkaConsumerMetrics`.
- c39e2af92c (`ConsumerMetrics` wrapper removed): record N/A if Rust never had it; apply the
  `FetchMetricsRegistry` hunk.
- Tests: the metrics-manager tests, a `KafkaConsumerTest` close-removes-all-sensors assertion, and the
  divide-by-zero test.

### Phase 12 — Admin (agent 102)

- KAFKA-20395 (c274a7348f): `unregister_controller` / `unregister_controller_with_options`
  (admin-client.md §1 shape), `UnregisterControllerOptions/Result`, the
  `UnregisterControllerRequest/Response` wrappers with byte-level tests, the `MockAdminClient` behaviour
  per Java's mock, and C FFI sync + async plus Python `admin.py` sync + asyncio.
- 58f63f448e: complete the `nodeApiVersions` future when `describeFeatures` fails.
- KAFKA-19663 (3b849ff2bd): `InternalDescribeFeaturesResult` (crate-private) and the
  `DescribeFeaturesResult` delta.
- KAFKA-20673 follow-up (cb2f143b0d): skip the stale-leader lookup retry while closing.
- Tests: the `KafkaAdminClientTest` slices (Java split it into per-domain classes in 1a443b2d23; map them
  to the Rust test layout), `RequestResponseTest` hunks, and FFI / Python tests.
- Can run as soon as Phase 2 lands (both edit `KafkaAdminClient`).

Phase 12 completion notes (agent 102):

- **KAFKA-20395 (c274a7348f), unregister controllers.**
  - Wire: `UnregisterControllerRequest` (+ `Builder`, `super(apiKey)` → `latest_version_enable_unstable_last_version(false)`)
    and `UnregisterControllerResponse`, wired through every `ConcreteRequest`/`ConcreteResponse` arm (the Phase 0
    "not currently handled" fallthrough is gone). `getErrorResponse(int, Throwable)` takes the crate `Error`
    (code from `Errors.forException`, message from `getMessage()`), unlike the `&Errors` convention, because the
    translated `RequestResponseTest.testUnregisterControllerResponseWithUnknownServerError` asserts the custom
    message; the dispatcher arm builds the `Error` from its code. `errorCounts` skips NONE, as Java does.
    Byte-level tests: request body and with-header vectors, response with message / null / default (`""`, the
    spec has no `"default": "null"`), parse through both dispatchers.
  - API: `Admin::unregister_controller` (trait default) / `unregister_controller_with_options`; public
    `UnregisterControllerOptions` / `UnregisterControllerResult`. Call: `LeastLoadedBrokerOrActiveKController`
    (a broker forwards; the API is forwardable), NONE completes, REQUEST_TIMED_OUT is a `HandleResult::Retry`
    (Java throws from `handleResponse` into `Call.fail`), everything else logs and fails with the broker message.
  - `MockAdminClient`: Raft controller → completed, else `UnsupportedVersionException("")`. Java's
    `Builder.usingRaftController` becomes `set_using_raft_controller` (the `set_feature_levels` precedent).
  - Bindings: `kafka_admin_AdminClient_unregister_controller` / `_async` with
    `kafka_admin_AdminClient_unregister_controller_callback_t` (no result handle, the B6 void row),
    `kafka_admin_MockAdminClient_set_using_raft_controller`; Python `Admin.unregister_controller`,
    `AsyncAdmin.unregister_controller`, mock mixin `set_using_raft_controller`; gRPC `UnregisterController`
    (`StatusResponse`) in the proto, the C++ server and both Python servers.
- **58f63f448e / KAFKA-19663 (3b849ff2bd).** Crate-private `admin::internals::InternalDescribeFeaturesResult`.
  Rust has no subclassing, so it holds the base by value with `From<..> for DescribeFeaturesResult`;
  `KafkaAdminClient::describe_features_internal` returns it and the trait method upcasts. Both futures complete
  from the error-code branch and from `handleFailure`. `DescribeFeaturesResult::new` was already `pub(crate)`
  (Java's new `protected`), so only its doc changed.
- **KAFKA-20673 follow-up (cb2f143b0d).** The driver call's `handle_node_unavailable` hook returns `false` once a
  hard-shutdown time is set (Java tests `hardShutdownTimeMs`, not `closing`). Java has no test; a Rust one fails
  without the guard with "Cannot accept new calls when AdminClient is closing.".
- **Phase 2's `enqueue` gap.** `CallSender::call` now checks `tries > maxRetries` first in the enqueue part (after
  `call()`'s hard-shutdown and `bootstrap.controllers` checks, before the bootstrap failure), failing through
  `handleTimeoutFailure` with "Exceeded maxRetries after N tries.". `handleTimeoutFailure` moved from the runnable
  onto `Call`, as in Java. Reached by driver specs re-issued with their `tries`; tested at the boundary, for
  ordering, and end to end (deleteRecords, `retries=0`).
- **Recorded skips.**
  - `KafkaAdminClientTest.testUnregisterBroker*` (6, refactored in c274a7348f): Rust has no `unregisterBroker`
    at all (a pre-existing gap, not 4.4 delta). The shared `runUnregisterScenario` is translated for the
    controller arm only; Java's `setNodeApiVersions(UNREGISTER_CONTROLLER, 0, 0)` has no `MockClient` analogue.
  - `ForwardingAdmin.unregisterController`: Rust has no `ForwardingAdmin`.
  - `DescribeFeaturesTest.testApiVersions` (clients-integration-tests): the broker half is translated as the
    in-crate Docker test `src/integration_tests/describe_features_test.rs` (it calls the crate-private
    `describe_features_internal`). Only the controller half is skipped: it needs a `bootstrap.controllers` admin,
    which the Rust config still rejects. (Critic 102 Issue 3.)
  - Broker/tool/metadata parts of c274a7348f and 3b849ff2bd (ControllerApis, ClusterControlManager, ClusterTool,
    MetadataQuorumCommand, KRaftClusterTest): server side, out of scope.
  - The integration **success** arm of `unregisterController`: Java first shuts down a controller of a 3-node
    quorum; the harness can stop neither pooled nodes nor controllers. Covered on the mock in Rust, C and Python.
  - C++ gRPC server handler: not compiled here (no cmake/grpc++ on this host); it mirrors
    `ForceTerminateTransaction`.
- **Integration.** New `admin_controllers_test.rs` (3 scenarios × 4 backends) picks its regime from
  `describeFeatures`' `metadata.version` (level 33 = `IBP_4_4_IV2`): on 4.2.0 every arm is UNSUPPORTED_VERSION; on
  `4.4.0-rc4` (measured: finalized ≥ 33) the unknown id gives CONTROLLER_ID_NOT_REGISTERED "Controller ID 9999 is
  not currently registered." and the combined node's own id gives INVALID_REQUEST "Controller cannot unregister
  itself while it is active.". `__rust`, `__grpc_python`, `__grpc_python_async` green on both brokers (Python
  arms in `MULTILANG_BACKEND_MODE=native`, which needs `--features ffi` on the test build so the cargo-set dylib
  path has the FFI symbols); `__grpc_c` not run (no cmake).
  - `cargo test --features integration-tests --test integration -- admin`: default 4.2.0 82/82 on re-run (first
    run 81/1: `test_delete_records_nonexistent_partition_fails__rust` failed in cleanup with "Timed out waiting
    for a node assignment. Call: deleteTopics" under its 8 s API timeout; the module passed 3/3 alone);
    `4.4.0-rc4` 82/82 on re-run (first run, during the image's cold start, 72/10, all in topic/partition/group
    setup; each module passed alone).
- **Gates.** `cargo build` green; `cargo xtask format-check` clean; full `cargo test` 4371 passed / 0 failed / 10
  ignored (lib 4322 / 3); `cargo xtask lint --keep-going`: lint-custom shows exactly the 15 §5.1 rows (three
  markers this phase added were fixed in ae43a4bf); every other step clean. C mock suite linked by hand (no
  cmake): `test_mock_admin` 160/160. Python unit + static: 395 passed, 2 skipped (pre-existing), from a PyPI venv.
  `make -k verify` (00:01–00:10): fails only `build-c` (`cmake: command not found`, so ctest never ran the new C
  surface in the gate) and `lint` (the 15 §5.1 rows); the Rust (incl. all-features with Docker), Python and
  check-bindings arms passed, 4925 tests, 0 failed. DoD #10: N/A (admin is not a per-record path).
- **Open questions for the human** (from Critic 102; not acted on, Manager decision 2026-10-08):
  1. **`close(timeout)` does not retry during the grace period** (Critic 102 Issue 1, Medium, pre-existing
     since M11 `3e84b9bd`, not a Phase 12 regression). `close_with_timeout` stores `shutdown.closing = true` at
     once; `fail_call` reads it as Java's `runnable.closing` and `should_exit` gates on it. Java's
     `close(Duration)` (`KafkaAdminClient.java:733-777`) publishes only `hardShutdownTimeMs`; `closing`
     (`:1209`) is written only in `run()`'s `finally` (`:1551`), so during the grace period `Call.fail`
     (`:975-979`) still retries via `maybeRetry` (`:1022-1024`), per `Admin.close(Duration)`'s "current
     operations will be allowed to complete" (`Admin.java:162-165`). The Critic's probe: `unregister_controller`
     with prepared `[REQUEST_TIMED_OUT, NONE]` gives `Ok` normally but `Err(Timeout)` after `close(60 s)`; Java
     gives success both times. Related: `HandleResult::NewCall` quota-retry follow-ups bypass `call()`'s
     hard-shutdown rejection (`:1707-1709`), so Rust keeps retrying what Java fails with "Cannot accept new
     calls when AdminClient is closing.". **Proposed fix:** gate the I/O task's exit on the hard-shutdown
     deadline (Java's `curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME`, `:1579-1580`), set `closing` only in
     `fail_all_remaining` (Java's `finally`), apply `call()`'s hard-shutdown rejection to `NewCall`
     follow-ups, change the tests that simulate close by storing `closing` to store only the deadline, and
     translate the probe as a test. It changes close behaviour for every admin RPC, hence the deferral. The
     `a560d9c7` comment was narrowed to "assigned" meanwhile (fa0bdb52).
  2. **`unregisterBroker`** is the only member of its family still untranslated (M11 Tier 4 deferral,
     `design/current/status.md`), though it has `unregisterController`'s shape and shares Java's
     `runUnregisterScenario`: port it as a follow-up, or keep the deferral?
- **Timing log** (2026-10-07/08, local):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, plan, Java diffs, Rust counterparts) | 22:40 | 22:49 | 9 |
  | 1 wire wrappers (1c5fd9ed) | 22:49 | 22:53 | 4 |
  | 2 Admin API + KafkaAdminClient + mock (19d2fbeb) | 22:53 | 23:07 | 14 |
  | 3 describeFeatures node API versions (19c84e34) | 23:07 | 23:10 | 3 |
  | 4 closing guard (a560d9c7) | 23:10 | 23:12 | 2 |
  | 5 retry budget in enqueue (bb03a2b0) | 23:12 | 23:25 | 13 |
  | 6 C FFI + C tests (f9b9bde6) | 23:25 | 23:30 | 5 |
  | 7 Python sync/asyncio (96ea1ec8) | 23:30 | 23:33 | 3 |
  | 8 gRPC harness + scenarios, both brokers (994979fb) | 23:33 | 23:46 | 13 |
  | 9 admin integration runs, both brokers | 23:46 | 23:56 | 10 |
  | 10 gates: format, lint + fix (ae43a4bf), full test | 23:56 | 00:01 | 5 |
  | 11 `make -k verify` and these notes | 00:01 | 00:15 | 14 |

### Phase 13 — Close-out (agent 103)

- **D1:** if `4.4.0` final is tagged, re-diff `4.4.0-rc4..4.4.0` across clients and specs. Port any delta,
  then bump the submodule, `AUDIENCE_REF`, `DEPRECATION_REFS` and the refreshed audience/deprecation lists
  (`cargo xtask fetch-java-refs`).
- Rustdoc sync for the 21 doc-only commits that touch translated types. The largest is 67850b0b68
  (+427 consumer javadoc); also 6ce2682d7b `auto.offset.reset` / `by_duration`, and 05185c1ef8 return
  semantics.
- **Completeness audit:** a table of every commit in §6 mapped to its phase or skip reason, appended here.
- `make verify` (macOS caveats: PIP_INDEX_URL override, Docker arms are Linux-CI-only), and updates to
  `MILESTONES.md` and `design/current/status.md`.
- Hand the drafted `rules-errata.md` amendments to the user.

## 4. Ordering and parallelism

```
0 → 1 ─┬─ network/consumer:  2 → 3 → 4 → 9 → 10 → 11
       ├─ producer:          5 → 6 → 7 → 8
       └─ admin:             (after 2) 12
                                          → 13
```

- **Phase 1 goes first:** its module moves rename imports crate-wide.
- **Network/consumer track (2 → 3 → 4 → 9 → 10 → 11):** this is the critical path. Phase 2 precedes 3 and
  4 because all three edit `NetworkClient` and the request managers. Phases 9 → 10 → 11 run in that order
  because they share `AbstractHeartbeatRequestManager`, `AsyncKafkaConsumer` and the metrics managers.
- **Producer track (5 → 6 → 7 → 8):** independent of the network work, apart from a small `KafkaProducer`
  constructor overlap with Phase 2, which resolves at merge time.
- **Admin (12):** needs only Phase 2.
- **Run in parallel**, the critical path is 0 → 1 → 2 → 3 → 4 → 9 → 10 → 11 → 13: nine loops instead of
  fourteen. Each track uses its own worktree (beware the macOS `/tmp` reaper for scratchpad worktrees) and
  merges into the milestone branch with merge commits.

## 5. DoD notes

- DoD applies in full in every phase:
  - Java tests are translated, with exact error messages asserted.
  - `@RepeatedTest` becomes a loop.
  - Changed encodings get byte-level tests (ApiVersions v5, DeleteGroups v3, TxnOffsetCommit v6,
    UnregisterController).
  - No TODO / FIXME, even where Java leaves TODOs (e.g. KIP-1332's compression TODO becomes an explicit
    `ConfigException` / error path, as Java's constructor does).
- **DoD #10** (hot-path allocation audit) applies to Phases 1 (reader bounds), 6, 7, 8 and 10. State N/A
  explicitly elsewhere.
- **DoD #7:** two justified deviations are recorded up front: the `ProducerBatch` fold and the builder
  buffer type (§2.3). D4 adds a crate-private error kind.
- **Out-of-scope tests** are skipped with the standing reasons. Each phase lists which tests it skipped.
  These include `ShareHeartbeatRequestManagerTest`, `StreamsGroupHeartbeatRequestManagerTest`,
  `KafkaShareConsumerTest`, `ConsumerCoordinatorTest`, `AbstractCoordinatorTest`, `OffsetFetcherTest`
  (classic), `RaftVoterEndpointTest`, the Streams admin tests, and the 1a443b2d23 test-class split
  (a reorganisation).

### 5.1 Known lint findings from the 4.4 submodule bump

`cargo xtask lint-custom` (`check-java-name`) resolves every `#[doc(alias = "org.apache.kafka...")]`
marker against the `kafka/` working tree. Phase 0's bump to 4.4.0-rc4 left the 45 findings below. Each
names a Java class or method that 4.4 moved or deleted as part of a change a later phase ports, so none
can be fixed in Phase 0 (the TxnOffsetCommit pair is barred by §2.2). The other 113 findings the bump
produced were pure relocations and were fixed in Phase 0.

**Gate rule (user decision, 2026-10-07; no tooling change):**
- A phase passes lint if `cargo xtask lint` reports nothing outside this table **and** that phase's own
  rows are gone.
- Each phase deletes its rows here in the commit that fixes them.
- Phase 13 requires this table to be empty and `cargo xtask lint` fully green.

Count by owner: Phase 9 1, Phase 11 10. (Phase 1 cleared its 30: 25 D2 moves + 5 throttle; Phase 5 cleared its 2; Phase 8 cleared the 2 shared with Phase 7.)

| Rust item | Java marker | Cause | Owner |
|---|---|---|---|
| `rust/src/consumer/internals/consumer_membership_manager.rs` `on_heartbeat_success` | `clients.consumer.internals.ConsumerMembershipManager#onHeartbeatSuccess` | Consolidated into `AbstractMembershipManager` (KAFKA-20681, 6a6b536fbc) | Phase 9 |
| `rust/src/consumer/internals/sensor_builder.rs` `SensorBuilder` | `clients.consumer.internals.SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `new` | `clients.consumer.internals.SensorBuilder#SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_tags` | `clients.consumer.internals.SensorBuilder#SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_avg` | `clients.consumer.internals.SensorBuilder#withAvg` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_min` | `clients.consumer.internals.SensorBuilder#withMin` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_max` | `clients.consumer.internals.SensorBuilder#withMax` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_value` | `clients.consumer.internals.SensorBuilder#withValue` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_meter` | `clients.consumer.internals.SensorBuilder#withMeter` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_meter_stat` | `clients.consumer.internals.SensorBuilder#SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `build` | `clients.consumer.internals.SensorBuilder#build` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |

## 6. Commit classification (input to the Phase 13 audit)

The full 190-row table (SHA, component, classification, subject, line counts) is in
[`commits.md`](commits.md), generated from
`git -C kafka log --no-merges --right-only --cherry-pick 4.3.1...4.4.0-rc4 -- clients/src`.
The summary by component:

| Component | Total | Port (P) | Test-only (T) | Doc | No-op | Out of scope (O) / reverted (R) / already in 4.3.1 (A) |
|---|---|---|---|---|---|---|
| Consumer | 50 | 19 | 11 | 6 | 7 | R 5, A 2 |
| Common | 33 | 3 | 1 | 6 | 21 | O 2 |
| Streams | 24 | – | – | – | – | O 24 |
| Wire | 16 | 12 | – | 1 | 3 | – |
| Producer | 14 | 9 | 1 | 1 | 2 | R 1 |
| Broker | 12 | – | – | – | – | O 12 |
| Network | 12 | 8 | – | 3 | 1 | – |
| Share | 11 | – | – | – | – | O 11 |
| Admin | 11 | 4 | – | 4 | 2 | O 1 (Raft voter) |
| Classic | 5 | – | – | – | – | O 5 |
| Security | 2 | – | – | – | 1 | O 1 |
| **Total** | **190** | **55** | **13** | **21** | **37** | **O 56, R 6, A 2** |

Phase → P/T commit counts (sum 68): P0 2 (plus the spec-only syncs), P1 7, P2 4, P3 4, P4 4, P5 13, P6 3,
P7+P8 1 (+1 trunk commit for D3), P9 6, P10 18, P11 2, P12 4. (b69c07c816 moved from Broker/O to Consumer/P for Phase 10 after the
Phase 1 review; 28de22de34 moved from Phase 9 to Phase 3, which needed it for KAFKA-21010 and the
bootstrap-window spin.)

### Merges into milestone-16-ak-4.4 (agent 93)

- **`73badfd9`: `origin/master` merged in.** Master was 1 commit ahead: `a4b3311e`, "Fix untrusted broker
  input - decode & decompression DoS (#205)". There were no textual conflicts. Three files were changed on
  both sides since the merge base `7ccc9ffb`, and each was checked by hand:
  - `common/network/selector.rs`: master's `InvalidReceiveError` routing (`channel_error_log`, WARN
    "Unexpected error …") and Phase 3's fired-queue drain at the top of every poll iteration (`a269808f`)
    sit in disjoint hunks. Both are kept.
  - `default_record.rs`: the milestone only changed the `ByteUtils` import path. Master's bounded
    `take(size)` stream read is kept.
  - `produce_request.rs`: Phase 1's bounded generated-reader tests and master's `validate_records` rewrite
    over `ByteBufferLogInputStream` are independent. Both are kept.
  - The milestone has not touched `completed_fetch.rs` or the other record files, so master's changes
    applied as they are.
  - The b69c07c816 split (what master covers and what Phase 10 still owes) is recorded under Phase 10.
- **`bdaf958a`: `milestone-16-admin` merged in (Phase 12, `852f5063`, forked at `ed2e3ce3`).** There were no
  conflicts. The only file both sides changed is this PLAN, in different sections. Phase 3 did not touch
  `kafka_admin_client.rs`, `call.rs` or `admin_client_runnable.rs`. Phase 12's open questions (the
  close grace period and `unregisterBroker`) stay open.
- **Gates after both merges.**
  - `cargo build` is green, and `cargo xtask format-check` is clean.
  - `cargo test`: 4443 passed, 0 failed, 10 ignored (lib 4394 / 3).
  - `cargo xtask lint --keep-going`: lint-custom shows exactly the 15 §5.1 rows, the same set as Phase 3
    (Phase 5 2, Phases 7/8 2, Phase 9 1, Phase 11 10). Clippy, doc-hygiene and module-path are clean.
- **Broker tests**, on the default broker under `lockf … m16-broker.lock`:
  - the lib suite, `--lib -- integration_tests::`: 32/32;
  - `--test integration -- plaintext_consumer consumer_test base_consumer_test bootstrap_resolution_test
    consumer_topic_creation_test consumer_bounce_test`: 106 passed / 2 ignored;
  - `-- admin`: 82/82.
  - Everything passed on the first run.
- `make verify` was skipped (no cmake). `git status --ignored` shows no new untracked or ignored files.
