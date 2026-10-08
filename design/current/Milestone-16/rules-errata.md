# Milestone-16 — Claude rules Java file:line citation errata (4.3.1 → 4.4.0-rc4)

## Scope

Read-only survey (Phase 0, agent 90). This file records where the Java `file:line` citations embedded
in the `.claude/rules/*.md` files drift when the `kafka/` submodule moves from tag `4.3.1`
(`26b251a451`) to `4.4.0-rc4` (`1156b2752a`), and where the Java code a rule reasons about changed in
4.4. **The rules files are NOT edited here.** Per `agent-roles.md`, rule changes go through the
suggestion process: everything below is a suggestion list for a human to apply.

The precedent is `design/history/Milestone-13/rules-errata.md` (the 4.2.0 → 4.3.1 survey). Its
suggested line updates were never applied to the rules files, so most `producer-transactions.md`
citations still carry their **4.2.0** numbers. This survey therefore shows three columns per citation:
the version the cite was written against, its 4.3.1 location (the Milestone-13 column, re-verified), and
its 4.4.0-rc4 location.

## Method

- Every rules file was searched for both citation forms: canonical `File.java:NNN` / `NNN-MMM` /
  `NNN,MMM`, and prose line refs bound to a file named in the surrounding text ("Java 136", "(1410)",
  "(154-161)", "`:799`", "Java 58-61", ...).
- Each cite was first calibrated against the blob of the version it was written for
  (`git -C kafka show 4.2.0:<path>`; `4.3.1` where 4.2.0 did not match), to confirm which construct it
  points at.
- That construct was then re-located in `4.3.1` and `4.4.0-rc4` **by content**, not by arithmetic.
  For ranges, the cited block was diffed between versions (for example `RecordAccumulator` 4.2.0
  877-926 and 4.4.0-rc4 925-974 are byte-identical).
- `git diff 4.3.1 4.4.0-rc4` was read for every cited file to decide whether the cited code, or code a
  rule reasons about, changed.
- Rust `file:line` references in the rules (`record_accumulator.rs:128`, `sender.rs:122`,
  `metadata_request.rs:191`, ...) are out of scope and were not checked.

"Drifted" means the cite as written in the rules file no longer points at the construct at
4.4.0-rc4. "Unchanged" means it still does. "Content-changed" means the cited code itself changed
between 4.3.1 and 4.4.0-rc4.

## Summary

46 distinct citations (a cite repeated inside the same rule is counted once and the row says so).

| Rules file | Citations | Drifted | Unchanged | Content-changed (cited code) |
|---|---|---|---|---|
| `producer-transactions.md` | 45 | 35 | 10 | 1 cosmetic (javadoc typo inside a cited range); 0 semantic |
| `consumer-threading.md` | 1 | 1 | 0 | 0 |
| `admin-client.md` | 0 | — | — | — |
| `agent-roles.md` | 0 | — | — | — |
| `definition-of-done.md` | 0 (`*Test.java` is a glob, not a cite) | — | — | — |
| **Total** | **46** | **36** | **10** | **1 cosmetic, 0 semantic** |

How the 36 drifted cites break down:

- **13 drifted at 4.3.1 and moved again at 4.4:** 11 `TransactionManager.java`, 1
  `MessageDataGenerator.java`, 1 `AsyncKafkaConsumer.java`.
- **12 drifted at 4.3.1 and did not move again at 4.4** (so the Milestone-13 errata number is already
  right for 4.4): 9 `Sender.java`, 3 `TransactionalRequestResult.java`.
- **11 first drift at 4.4** (they were still right at 4.3.1): 4 `TransactionManager.java` field cites
  (136-139), 6 `RecordAccumulator.java`, 1 `OffsetCommitRequest.java`.

**The claims about the cited code still hold.** In every one of the 46 cases the rule's claim about
the cited construct is still true at 4.4.0-rc4: only line numbers moved. The 4.4 changes that matter are
in code the rules reason about **without** citing a line: `TransactionManager`'s KIP-1319
TxnOffsetCommit path, the new `TxnOffsetCommitRequest.Builder` factories, `MockAdminClient` and
`AdminMetadataManager`. See "Content changes that affect a rule's reasoning".

## Per-file tables

Paths: `TransactionManager`, `Sender`, `RecordAccumulator`, `TransactionalRequestResult` and
`TxnPartitionEntry` are in `clients/src/main/java/org/apache/kafka/clients/producer/internals/`. The
request classes are in `clients/src/main/java/org/apache/kafka/common/requests/`.
`MessageDataGenerator` is in `generator/src/main/java/org/apache/kafka/message/`. `AsyncKafkaConsumer`
is in `clients/src/main/java/org/apache/kafka/clients/consumer/internals/`.

### `TransactionManager.java` (`producer-transactions.md`) — 15 cites, all drifted

The shifts are not uniform:

- **4.2.0 → 4.3.1:** +18 after line ~205; 136-139 did not move.
- **4.3.1 → 4.4.0-rc4:**
  - +1 from the `Metadata` import, +1 from the `Uuid` import, +3 from the TxnOffsetCommit message
    imports and +1 from the new `private final Metadata metadata` field (106). Together these put the
    136-139 fields at +6 (142-145).
  - +2 more from the constructor's `metadata` parameter and assignment. That makes +8 after line ~250.
  - +30 more after the rewritten `txnOffsetCommitHandler` (4.3.1 1239 → 4.4 1247, KIP-1319). That makes
    +38 after line ~1300.

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §1 | `TransactionManager.java:287-289` | 4.2.0 | 305-307 | 313-315 | `protected boolean shouldPoisonStateOnInvalidTransition()` → `return Thread.currentThread() instanceof Sender.SenderThread;`. Body unchanged. |
| §1 | "Java 234-286" (KAFKA-14831) | 4.2.0 | 252-304 | 260-312 | The state-transition / poison javadoc above `shouldPoisonStateOnInvalidTransition`. `See KAFKA-14831 for more detail.` is at 300 in 4.3.1 and 308 in 4.4. **Cosmetic content change** inside the range: line 278 in 4.4 fixes `{@link Producer#commitTransaction()}}` → `{@link Producer#commitTransaction()}`. The claim is unaffected. |
| §2 | "(Java 136)" | 4.2.0 | 136 | 142 | `private int inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;` |
| §2 | "(137)" | 4.2.0 | 137 | 143 | `private Node transactionCoordinator;` |
| §2 | "(138)" | 4.2.0 | 138 | 144 | `private Node consumerGroupCoordinator;` |
| §2 | "(139)" | 4.2.0 | 139 | 145 | `private boolean coordinatorSupportsBumpingEpoch;`. All four are still plain fields with no `volatile`, so the §2 Sender-confinement claim holds. `pendingRequests` is still a plain `PriorityQueue<TxnRequestHandler>` (127). |
| §2 | "`lookupCoordinator(TxnRequestHandler)` (969)" | 4.2.0 | 987 | 995 | `void lookupCoordinator(TxnRequestHandler request)`. Still unsynchronized. |
| §2 | "(1410)" | 4.2.0 | 1428 | 1466 | `clearInFlightCorrelationId();` in `TxnRequestHandler.onComplete`, still before (outside) the `synchronized` block. |
| §2 | "starts at 1421" | 4.2.0 | 1439 | 1477 | `synchronized (TransactionManager.this) {` wrapping `handleResponse(...)` in `onComplete`. |
| §5 | "(Java 1261-1283)" | 4.2.0 | 1279-1301 | 1317-1339 | `private TransactionalRequestResult handleCachedTransactionRequestResult(...)`. Still keys off `pendingTransition.result.isAcked()`, and still throws `IllegalStateException` for a different operation. |
| §7 | `TransactionManager.java:790` | 4.2.0 | 808 | 816 | `removeInFlightBatch(batch);` as the first statement of `handleFailedBatch` (declared at 814 in 4.4). |
| §7 / §9 | "`adjustSequencesDueToFailedBatch` at `:818`" | 4.2.0 | 836 | 844 | `txnPartitionMap.adjustSequencesDueToFailedBatch(batch);` |
| §7 | `TransactionManager.java:655` | 4.2.0 | 673 | 681 | `this.txnPartitionMap.startSequencesAtBeginning(topicPartition, this.producerIdAndEpoch);` in `bumpIdempotentProducerEpoch` (declared at 671 in 4.4). |
| §9 | "`:799`" (cited twice: code block and prose) | 4.2.0 | 817 | 825 | `if (exception instanceof OutOfOrderSequenceException && !isTransactional()) {` |
| §9 | "`:806`" (cited twice: code block and prose) | 4.2.0 | 824 | 832 | `} else if (exception instanceof UnknownProducerIdException) {`. The `if / else if` order is unchanged. `UnknownProducerIdException extends OutOfOrderSequenceException` still holds (`common/errors/UnknownProducerIdException.java:29`). |

### `Sender.java` (`producer-transactions.md`) — 9 cites, all drifted (4.3.1 numbers still valid at 4.4)

The only change from 4.3.1 to 4.4.0-rc4 is that `KafkaThread` and `LogContext` imports move to
`common.utils.internals`, which swaps lines 56-58 with no net shift. Every 4.3.1 number from
Milestone-13 is therefore also the 4.4.0-rc4 number.

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §2 | `Sender.java:522` | 4.2.0 | 523 | 523 | `transactionManager.lookupCoordinator(nextRequestHandler);` |
| §4 | `Sender.java:459-518` | 4.2.0 | 460-519 | 460-519 | `private boolean maybeSendAndPollTransactionalRequest()` |
| §4 | "(462, 496, 509)" | 4.2.0 | 463, 497, 510 | 463, 497, 510 | The three `client.poll(retryBackoffMs, time.milliseconds());` calls. |
| §4 | "`awaitNodeReady` (484)" | 4.2.0 | 485 | 485 | `if (!awaitNodeReady(targetNode, coordinatorType)) {` |
| §4 | "`time.sleep(retryBackoffMs)` (501, 525)" | 4.2.0 | 502, 526 | 502, 526 | `time.sleep(nextRequestHandler.retryBackoffMs());` (502) and `time.sleep(retryBackoffMs);` (526). |
| §7 | `Sender.java:750-752` | 4.2.0 | 751-753 | 751-753 | `private void reenqueueBatch(...)` → `accumulator.reenqueue(..)` → `maybeRemoveFromInflightBatches(batch)`. It still does **not** call `transactionManager.removeInFlightBatch`. |
| §7 | "path at `:685`" | 4.2.0 | 686-687 | 686-687 | The MESSAGE_TOO_LARGE split path: the `if (transactionManager != null)` guard (686) and `transactionManager.removeInFlightBatch(batch);` (687). The `Errors.MESSAGE_TOO_LARGE` test is at 676. |
| §7 | `Sender.java:848` | 4.2.0 | 849 | 849 | `transactionManager.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers);` in `failBatch` |
| §7 | "`maybeRemoveAndDeallocateBatch` (`:854`)" | 4.2.0 | 855 | 855 | `maybeRemoveAndDeallocateBatch(batch);`, still after `handleFailedBatch`. |

### `RecordAccumulator.java` (`producer-transactions.md`) — 6 cites, all newly drifted at 4.4

The file is unchanged in line terms from 4.2.0 to 4.3.1. From 4.3.1 to 4.4.0-rc4 there is heavy churn
(KIP-1332 refactor into protected hooks, rack-aware partitioning, `utils.internals` imports): +45 by
`insertInSequenceOrder` and +48 by the drain block. **Both cited blocks are byte-identical between
4.2.0 and 4.4.0-rc4.**

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §3 | `RecordAccumulator.java:877-926` | 4.2.0 | 877-926 | 925-974 | The `synchronized (deque) {` block in `drainBatchesForOneNode` (declared at 900 in 4.4) that assigns sequence numbers. |
| §3 | "`maybeUpdateProducerIdAndEpoch` (908)" | 4.2.0 | 908 | 956 | `transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);` |
| §3 | "`sequenceNumber` (918)" | 4.2.0 | 918 | 966 | `batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);` |
| §3 | "`incrementSequenceNumber` (919)" | 4.2.0 | 919 | 967 | `transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);` |
| §3 | "`addInFlightBatch` (924)" | 4.2.0 | 924 | 972 | `transactionManager.addInFlightBatch(batch);` |
| §7 | `RecordAccumulator.java:558-560` | 4.2.0 | 558-560 | 603-605 | In `insertInSequenceOrder` (declared at 597 in 4.4): `if (!transactionManager.hasInflightBatches(...)) throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " + ...)`. |

### `TransactionalRequestResult.java` (`producer-transactions.md`) — 3 cites, all drifted (4.3.1 numbers still valid at 4.4)

There is no diff from 4.3.1 to 4.4.0-rc4. The drift is the 4.2.0 → 4.3.1 consolidation of the `await`
overloads already recorded by Milestone-13.

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §5 | `TransactionalRequestResult.java:62` | 4.2.0 | 58 | 58 | `isAcked = true;` inside `await(long, TimeUnit, String)` (declared at 50). The field `private volatile boolean isAcked` is at 30; the `isAcked()` getter is at 79-80. |
| §5 | "(62-65)" | 4.2.0 | 58-61 | 58-61 | `isAcked = true;` is set before `if (error != null) throw error;`, so a failed result is still acked. |
| §5 | "`InterruptException` path (66)" | 4.2.0 | 62-63 | 62-63 | `catch (InterruptedException e)` (62) → `throw new InterruptException("Received interrupt while awaiting " + operation, e)` (63). |

### `TxnPartitionEntry.java` (`producer-transactions.md`) — 5 cites, all unchanged

The only diffs from 4.2.0 to 4.4.0-rc4 are import-line swaps at 21-24 (`record.internal.DefaultRecordBatch`
in 4.3.1; `utils.internals.PrimitiveRef` / `ProducerIdAndEpoch` in 4.4). No line moved.

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §6 | `TxnPartitionEntry.java:62-65` | 4.2.0 | 62-65 | 62-65 | `PRODUCER_BATCH_COMPARATOR`: `comparingLong(producerId).thenComparingInt(producerEpoch).thenComparingInt(baseSequence)` |
| §6 | "(154-161)" | 4.2.0 | 154-161 | 154-161 | `private void resetSequenceNumbers(Consumer<ProducerBatch> resetSequence)`, which rebuilds the `TreeSet` |
| §6 | "Java 58-61" (cited twice) | 4.2.0 | 58-61 | 58-61 | The comment explaining the three-key comparator (PR 12096 link) |
| §8 | "(163-173)" | 4.2.0 | 163-173 | 163-173 | `private boolean decrementSequence(int decrement)`: plain subtraction that throws `IllegalStateException` when the result is negative |
| §8 | "(104-106)" | 4.2.0 | 104-106 | 104-106 | `void incrementSequence(int increment)` → `DefaultRecordBatch.incrementSequence(...)` |

### `MessageDataGenerator.java` (`producer-transactions.md`) — 1 cite, drifted

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §11 | `MessageDataGenerator.java:792` | 4.2.0 | 776 | 815 | `if (!field.ignorable()) {` guarding `cond.ifNotMember(__ -> field.generateNonIgnorableFieldCheck(...))` (816). The +39 at 4.4 comes from the new `generateHashSetIterableConstructor` and from read-side bounds checks (`MAX_TAGGED_FIELD_COUNT`, `MAX_ARRAY_LENGTH`, `MAX_PREALLOCATED_ARRAY_CAPACITY`) that 4.4 now emits in generated `read()` code. Those checks do not touch the write-side ignorable gate, so §11's claim holds. The message text is in `FieldSpec.generateNonIgnorableFieldCheck` (`FieldSpec.java:649`, unchanged): `"Attempted to write a non-default %s at version "`. |

### `common/requests/*.java` builder cites (`producer-transactions.md` §12) — 6 cites, 1 drifted

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §12 | `AbstractRequest.java:46-51` | 4.2.0 | 46-51 | 46-51 | `Builder(ApiKeys apiKey)` javadoc ("any supported and released version") and body `this(apiKey, false)`. The only 4.4 change is two new `parseRequest` cases at ~354. |
| §12 | `OffsetCommitRequest.java:55` (cited twice) | 4.2.0 | 55 | **56** | `return new Builder(data, ApiKeys.OFFSET_COMMIT.oldestVersion(), ApiKeys.OFFSET_COMMIT.latestVersion());`. It moved +1 because of the new `import org.apache.kafka.common.internals.UnsupportedProtocolFieldException;` at 22. The bound is still `latestVersion()`, so the §12 claim holds. |
| §12 | `OffsetFetchRequest.java:64` (cited twice) | 4.2.0 | 64 | 64 | `ApiKeys.OFFSET_FETCH.latestVersion()`. In 4.4 an import is added at 22 and `java.util.Collections` is removed at 36, net 0. |
| §12 | `AddPartitionsToTxnRequest.java:58` | 4.2.0 | 58 | 58 | `return new Builder(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), LAST_CLIENT_VERSION, ...)`. The file is unchanged. |
| §12 | `ApiVersionsRequest.java:43` | 4.2.0 | 43 | 43 | `ApiKeys.API_VERSIONS.latestVersion());`. The 4.4 additions (`setClusterId` / `setNodeId`, v5 validation) are below line 56. |
| §12 | `OffsetsForLeaderEpochRequest.java:57` | 4.2.0 | 57 | 57 | `return new Builder((short) 3, ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(), data);`. The file is unchanged. |

### `AsyncKafkaConsumer.java` (`consumer-threading.md`) — 1 cite, drifted

This cite postdates Milestone-13: it was added in `5fc83544` (PR #191, 2026-09-21), whose
`rust/src/consumer/mod.rs:204` doc comment carries the same pair. The submodule was at 4.3.1 when it
was written, but **the numbers match 4.2.0 content, not 4.3.1**, so the cite was stale when it landed.
At 4.3.1, 2107 and 2131 fall inside unrelated javadoc / `updateAssignmentMetadataIfNeeded`.

| Rule § | Cited (as written) | Version the cite was written against | 4.3.1 | 4.4.0-rc4 | Construct / note |
|---|---|---|---|---|---|
| §1 | `AsyncKafkaConsumer.java:2107,2131` | 4.2.0 (stale on arrival; submodule was 4.3.1) | 2224, 2247-2248 | 2238, 2261-2262 | The two private pattern-subscription paths: `private void subscribeInternal(Pattern pattern, Optional<ConsumerRebalanceListener> listener)`, which sends `TopicPatternSubscriptionChangeEvent` (client-side `java.util.regex`; 2246 in 4.4), and `private void subscribeToRegex(SubscriptionPattern pattern, Optional<ConsumerRebalanceListener> listener)`, which sends `TopicRe2JPatternSubscriptionChangeEvent` (server-side RE2/J; 2268 in 4.4). In 4.2.0, 2131 is the second line of the `subscribeToRegex` declaration. The public overloads are at 2168 (`subscribe(Pattern)`), 2173 (`subscribe(SubscriptionPattern, listener)`), 2180 (`subscribe(SubscriptionPattern)`) and 2185 (`subscribe(Pattern, listener)`) in 4.4. The rule's claim (two client-side `Pattern` overloads, two server-side `SubscriptionPattern` overloads) holds. Suggested replacement: `AsyncKafkaConsumer.java:2238,2261`. The same fix applies to the Rust comment at `rust/src/consumer/mod.rs:204`, which is outside the rules files. |

## Content changes that affect a rule's reasoning

None of the 46 cited constructs changed semantically. The following 4.4 changes are in code that a
rule reasons about **without** a line cite, and they change how the rule applies. Each item gives the
rule, what changed, and the suggested amendment.

1. **`producer-transactions.md` §10 — the TxnOffsetCommit grouping site moved (KIP-1319).**
   - **What the rule says:** "`TxnOffsetCommitRequest` groups offsets by topic the same way — it must
     follow."
   - **4.3.1:** the producer built the request through
     `new TxnOffsetCommitRequest.Builder(..., pendingTxnOffsetCommits, ...)` (`TransactionManager.java:1250`).
     That builder called `setTopics(getTopics(pendingTxnOffsetCommits))` (`TxnOffsetCommitRequest.java:83`),
     grouping through a `HashMap<String, List<...>>`.
   - **4.4.0-rc4:**
     - `TransactionManager.txnOffsetCommitHandler` (1247-1303) builds `TxnOffsetCommitRequestData`
       itself.
     - `topics` is an `ArrayList` in first-seen order over the caller's `offsets` map
       (`requestTopicsByName` is only a lookup), and partitions are appended in the same encounter
       order.
     - `TxnOffsetCommitRequest.getTopics(...)` survives but is no longer on the producer path.
     - The topics now come from the `offsets` argument rather than from the whole
       `pendingTxnOffsetCommits` map, which is still populated.
   - **Effect on the reasoning:** the order still depends on a caller-supplied `Map`'s iteration order,
     so the determinism rationale is unchanged. What changes is the site: the Rust translation should
     sort where `TransactionManager` assembles the request topics (Phase 5), not in a
     `getTopics`-style grouping helper.
   - **Suggested wording:** "`TransactionManager.txnOffsetCommitHandler` assembles TxnOffsetCommit
     topics in caller-map encounter order (4.4, KIP-1319) — it must follow."

2. **`producer-transactions.md` §12 — the `TxnOffsetCommitRequest.Builder` shape changed; there is a
   new `latestVersion()` site.**
   - **4.3.1:** the builder called `super(ApiKeys.TXN_OFFSET_COMMIT)` (`TxnOffsetCommitRequest.java:76`,
     → `latest_version_enable_unstable_last_version(false)`). It clamped to
     `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2` inside `build()` (102) when transaction V2 was disabled.
   - **4.4.0-rc4:**
     - The builder has a private `super(apiKey, oldest, latest)` constructor (71) and two factories.
     - `forTopicNames` (75-84) passes **constants**: `(short) 5`, or
       `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2` (= 4).
     - `forTopicIdsOrNames` (86-97) passes `ApiKeys.TXN_OFFSET_COMMIT.latestVersion()` (94, the
       unstable-inclusive accessor), or the same constant 4.
     - `TransactionManager` picks `forTopicIdsOrNames` only when every topic resolved to a non-zero id
       (1295-1297).
     - The `build()` clamp is gone.
   - **Effect on the reasoning:** the §12 rule ("translate the `latest` expression verbatim") still
     decides the translation, but the rule's list of sites is now incomplete. `forTopicIdsOrNames` is a
     new deliberate `latestVersion()` site, like `OffsetCommitRequest.java:56`. `forTopicNames` is a
     new constant site, like `AddPartitionsToTxnRequest.java:58`.
   - **Suggested addition** to "Where Java deliberately uses the unstable-inclusive `latestVersion()`"
     and to the anti-pattern exceptions: `TxnOffsetCommitRequest.java:94`. Also add to the constant
     example: `TxnOffsetCommitRequest.java:82`.
   - **Scope:** this is a transactions-phase builder, so the "not retroactive" scope note does not
     cover it. PLAN §2.2 already points Phase 5 at §12.

3. **`producer-transactions.md` §11 — new ignorable, version-split fields guarded at the builder.**
   - **4.4.0-rc4 `TxnOffsetCommitRequest.json`:** `Topics[].Name` becomes `"versions": "0-5",
     "ignorable": true`, and a new `Topics[].TopicId` is `"versions": "6+", "ignorable": true`.
     `GenerationId` is renamed to `GenerationIdOrMemberEpoch`.
   - The generator therefore emits **no** non-default check for either field, on both sides. Java
     instead rejects at the builder: `Builder.build` throws `UnsupportedVersionException` ("... does
     require usage of topic ids." / "... topic names.") at `TxnOffsetCommitRequest.java:109,116`.
   - **Effect on the reasoning:** §11's "before treating a dropped field as a defect, check
     `ignorable`" still holds. But it is no longer the whole story for this API: a Critic who sees the
     generator drop `Name` at v6 should look for the builder-level guard, not for a generator check.
   - **Suggested addition** to §11's "Known cases": the two fields above, with the `build()` guard as
     the place Java enforces them.
   - The existing known cases still hold at 4.4: `InitProducerIdRequest.ProducerId` is not ignorable;
     `TxnOffsetCommitRequest.CommittedLeaderEpoch` is ignorable.

4. **`admin-client.md` §9 — `MockAdminClient` moved to test fixtures and implements more methods.**
   - **Where it lives:**
     - 4.3.1: `clients/src/test/java/org/apache/kafka/clients/admin/MockAdminClient.java`.
     - 4.4.0-rc4: `clients/src/testFixtures/java/org/apache/kafka/clients/admin/MockAdminClient.java`
       (commit `3b6c8385ca`, #22201, "Move client test utilities to test fixtures").
     - The class still carries no `@InterfaceAudience.Public`, so the allow-list reasoning holds.
       Suggested wording: "Java ships it in the clients *test-fixtures* source set" instead of "the
       clients *test* jar".
   - **New in-memory implementations:**
     - `describeStreamsGroups` was `throw new UnsupportedOperationException("Not implemented yet")` at
       4.3.1. At 4.4 it is implemented against a new `streamsGroupDescriptions` map, with
       `addStreamsGroupDescription(...)` seeding it.
     - A new `unregisterController` is implemented.
     - The "Not implemented yet" sites drop from 32 to 31.
   - **Effect on the reasoning:** under §9's governing principle ("mirror Java's `MockAdminClient`
     method-for-method"), any Rust mock that exposes `describe_streams_groups` must now implement it
     in memory, and any "Java throws unsupported" justification on that method is now false. Phase 12
     should check this. PLAN marks the Streams RPCs out of scope, so this may be moot if the Rust
     `Admin` trait lacks the method.

5. **`admin-client.md` §3 — `AdminMetadataManager` gained bootstrap state (KIP-909 / KAFKA-14648).**
   - All nine members the rule lists still exist at 4.4.0-rc4: `isReady` 228, `controller` 247,
     `nodeById` 251, `requestUpdate` 255, `metadataFetchDelayMs` 277, `transitionToUpdatePending` 310,
     `updateFailed` 317, `update` 345, `usingBootstrapControllers` 208.
   - 4.4 adds:
     - the field `volatile BootstrapResolutionException bootstrapFatalException`, with its accessors
       `isBootstrapped()` (216), `bootstrapFatalException()` (220) and `recordBootstrapFatalException(...)`;
     - updater overrides `bootstrapFailed`, `isBootstrapped` and `bootstrap(List<InetSocketAddress>)`
       (162-180).
   - `admin/internals/AdminBootstrapAddresses.java` is deleted in 4.4. No rule names it.
   - **Effect on the reasoning:** "mirror `AdminMetadataManager.java` field-for-field" still holds, but
     the enumerated list is no longer complete. Suggest adding the new members (Phase 2 / Phase 12).

### Checked: 4.4 churn near a rule, reasoning unaffected

- **`producer-transactions.md` §3 / §6 / §7 — KIP-1332.**
  - **`ChunkedRecordAccumulator`:** 4.4 adds `ChunkedRecordAccumulator extends RecordAccumulator`,
    which overrides only `append`, `tryAppend` and `createProducerBatch`. The drain block, `reenqueue`,
    `insertInSequenceOrder` and `splitAndReenqueue` are inherited unchanged. Its `append` takes
    `synchronized (dq)` (`ChunkedRecordAccumulator.java:146,197,283`) but makes no `TransactionManager`
    call, so §3's deque → manager ordering is unaffected.
  - **`ChunkedProducerBatch`:** `ProducerBatch` loses `final`, and `ChunkedProducerBatch extends
    ProducerBatch`. `TxnPartitionEntry`'s `TreeSet<ProducerBatch>` comparator is unchanged, so §6 and
    §7 hold.
  - The Rust fold of `ChunkedProducerBatch` into `ProducerBatch` is the PLAN §2.3 note reserved below.
- **`producer-transactions.md` §2 / §5 / §9 — KIP-1319 in `TransactionManager`.** The rewrite is
  confined to `txnOffsetCommitHandler` and `TxnOffsetCommitHandler` (topic-id snapshot, response
  iteration by topic, a new `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH` → `CommitFailedException`
  abort, and a per-topic `if (result.isCompleted()) break;`). None of the cited constructs (state
  machine, poison logic, coordinator fields, `handleFailedBatch`, `handleCachedTransactionRequestResult`)
  changed.
- **`producer-transactions.md` §9 — error codes.** "(59)", "(45)" and "(53)" are wire error codes, not
  line cites. `UNKNOWN_PRODUCER_ID(59)`, `OUT_OF_ORDER_SEQUENCE_NUMBER(45)` and
  `TRANSACTIONAL_ID_AUTHORIZATION_FAILED(53)` are unchanged (`Errors.java:313,279,299`), and so is
  `TransactionalIdAuthorizationException extends AuthorizationException`
  (`TransactionalIdAuthorizationException.java:23`).
- **`consumer-threading.md` §28 / §31 — consumer internals.**
  - `AsyncPollEvent` is unchanged from 4.3.1 to 4.4.0-rc4. It is still a bare `ApplicationEvent`
    carrying `error`, `isComplete`, `isValidatePositionsComplete` and `reconciliationCheckFuture`.
  - The `AbstractMembershipManager` 4.4 changes do not touch the callback handshake: `onHeartbeatSuccess`
    is consolidated into a `final` base method (KAFKA-20681), and there is a same-`localEpoch`
    early-return in reconcile.
  - `ConsumerNetworkThread` now completes a failing `CompletableEvent` exceptionally (KAFKA-18812).
  - The named members still exist at 4.4: `processBackgroundEvents`, `invokeRebalanceCallbacks`,
    `revokeAndAssign`, `maybeReconcile`, `signalPartitionsLost`, `transitionToFenced` / `Fatal` /
    `Stale`, `maybeAbortReconciliation`, `ConsumerNetworkThread.runOnce`, and
    `ApplicationEventHandler.wakeupNetworkThread`.
- **`consumer-threading.md` §20.**
  - `DEFAULT_GROUP_PROTOCOL` is still `classic` (`ConsumerConfig.java:121` at 4.4; 115 at 4.3.1).
  - The admin carve-out still holds: `DescribeConsumerGroupsHandler.handledClassicGroupResponse` (251)
    calls `ConsumerProtocol.deserializeAssignment` (280-281), and
    `DescribeClassicGroupsHandler.handleResponse` (105) calls it at 131.

## Not line citations

Named Java classes, packages and version stamps in the rules, checked against 4.4.0-rc4:

- **`producer-transactions.md` §12 — the spec-corpus claims are stale.**
  - "`rust/generator/messages/` is a pre-4.2 snapshot (36 of 197 specs differ)" is no longer true in
    this worktree. The corpus has 203 specs. It differs from `kafka/` 4.4.0-rc4 only in
    `TxnOffsetCommitRequest.json` and `TxnOffsetCommitResponse.json`, which PLAN §2.2 deliberately
    holds back to Phase 5.
  - `kafka/` itself has 197 specs at 4.2.0, 198 at 4.3.1 and 203 at 4.4.0-rc4.
  - The flag claim still holds in every corpus: only `InitProducerIdRequest.json` sets
    `"latestVersionUnstable": true` (4.2.0, 4.3.1, 4.4.0-rc4, and `rust/generator/messages/`).
  - Suggest restating the paragraph against 4.4.
- **`producer-transactions.md` header and §12 — version stamps.** The header ("Apache Kafka 4.2") and
  §12 ("`kafka/` 4.2") are stale version stamps. The claims they qualify hold at 4.4 (see above).
- **`consumer-threading.md` §2 — version stamp.** "Public method surface mirrors `Consumer.java` (Apache
  Kafka 4.2)" is a stale stamp. From 4.3.1 to 4.4 `Consumer.java` only gains `@InterfaceAudience.Public`
  and a javadoc typo fix, so the method surface is unchanged. KAFKA-20385's `setRebalanceListener` was
  reverted on 4.4 (`79a29b0675`). §20 has the same "in 4.2" stamp.
- **`consumer-threading.md` §28 / §31 — event names, still pending from Milestone-13.** The rules
  still name `RebalanceListenerCallbackNeeded` / `ConsumerRebalanceListenerCallbackNeededEvent`. That
  class does not exist at 4.3.1 or 4.4.0-rc4: it was split into `PartitionsRemovedEvent` /
  `PartitionsAssignedEvent`, plus `ApplyAssignmentEvent`. The Milestone-13 §28/§31 amendment draft
  (`design/history/Milestone-13/rules-errata.md`) is still unapplied. 4.4 adds nothing new to that
  handshake.
- **`admin-client.md` §4, §8 and §6 — package locations still valid at 4.4.**
  - `KafkaFutureImpl` is still `org.apache.kafka.common.internals`.
  - `Utils.from32BitField` is still in `org.apache.kafka.common.utils.Utils` (1386). KAFKA-20297 moved
    many `common.utils` classes to `common.utils.internals` (`LogContext`, `ProducerIdAndEpoch`,
    `KafkaThread`, `ExponentialBackoff`, `CopyOnWriteMap`, ...), but `Utils` stayed.
  - `AdminUtils.validAclOperations` is still `clients.admin.internals.AdminUtils` (30).
  - `TopicCollection` is still `org.apache.kafka.common`.
  - The `KafkaAdminClient` internals named in §2 still exist: `AdminClientRunnable`,
    `processRequests`, `Call`, `ControllerNodeProvider`, `LeastLoadedNodeProvider`,
    `ConstantNodeIdProvider`, `MetadataUpdateNodeIdProvider`, `LeastLoadedBrokerOrActiveKController`,
    and `newCalls` + `client.wakeup()`.
  - The `Admin` overload claims in §1 still hold: no zero-arg `describeTopics`; zero-arg
    `describeUserScramCredentials()` (1436) and `listPartitionReassignments()` (1195); the
    `listConsumerGroupOffsets` overload set; `close()` / `close(Duration)` (155, 169).
- **`producer-transactions.md` §8 — `DefaultRecordBatch` package.** `DefaultRecordBatch` lives in
  `org.apache.kafka.common.record.internal` (moved in 4.3.1, unchanged in 4.4). The rule names it
  without a package, so no edit is needed.
- **`admin-client.md` §9 — `@InterfaceAudience.Public`.** The annotation class
  `org.apache.kafka.common.annotation.InterfaceAudience` is new in 4.4. `MockAdminClient` does not
  carry it.
- **Removed or moved in 4.4, not named in any rules file:**
  - `admin/internals/AdminBootstrapAddresses.java` (deleted; it was used at 4.3.1
    `KafkaAdminClient.java:537`, and in 4.4 bootstrap handling goes through the new
    `clients/BootstrapConfiguration.java` used by `ClientUtils` / `NetworkClient`);
  - `consumer/internals/ConsumerMetrics.java` (deleted, KAFKA-20408);
  - `consumer/internals/SensorBuilder.java` (moved to `consumer/internals/metrics/`);
  - `common/utils/CollectionUtils.java` and `common/utils/internals/BytesUtils.java` (deleted).
  
  No rule change is needed for these.

## Draft rules notes for later phases

Reserved for the PLAN §2.3 KIP-1332 note (the `ChunkedProducerBatch` fold into `ProducerBatch`, so Critics do not flag the missing type) and any other rule amendments later Milestone-16 phases draft. Phase 0 adds none.

### Phase 5 (agent 95): `producer-transactions.md` §2/§3 — the manager → `Metadata` lock edge (KIP-1319)

Drafted amendment, from Critic 95's review (COMMENTS.95 Q3). For a human to apply; `.claude/rules/` is not edited.

- **What changed.** `TransactionManager` now holds `Arc<Metadata>` (Java's new constructor parameter,
  `TransactionManager.java:106`) and reads `metadata.topic_ids()` under the manager's lock, from
  `txn_offset_commit_handler`. Java has the same edge: `synchronized sendOffsetsToTransaction`
  (`:430`) reaches `metadata.topicIds()` (`:1253`).
- **Why the rules should say it.** §2 lists what lives behind the manager's lock and §3 fixes the
  deque → manager order, but neither mentions a lock taken *while* the manager's is held. `Metadata`
  is not a leaf: `Metadata::update` runs `ProducerMetadata`'s retain and request-builder closures and
  the `ClusterResourceListeners` under its own lock. The order is safe only because none of those
  callbacks takes the manager's lock.
- **Suggested text** (append to §2's "How to apply"):
  "`TransactionManager` holds `Arc<Metadata>` (KIP-1319) and reads `topic_ids()` under the manager's
  lock, so the order is manager → `Metadata`. `Metadata` runs `ProducerMetadata`'s closures and
  `ClusterResourceListeners` under its own lock, so none of them may take the manager's lock or
  `pending_requests`."
- **Suggested anti-pattern** (§2/§3 list): a `Metadata` callback, retain closure or cluster listener
  that locks the `TransactionManager` or `pending_requests`.
- **Related (from the same review):** rules-errata content items 1-3 above (KIP-1319 vs §10/§11/§12)
  are now implemented as suggested. The §11 "known cases" addition can cite the enforcement sites
  `TxnOffsetCommitRequest.java:100-118` and `rust/src/common/requests/txn_offset_commit_request.rs`
  `Builder::build_version`.

### Phase 8 (agent 98): `producer-transactions.md` §7 — `ChunkedProducerBatch` is folded into `ProducerBatch` (KIP-1332)

Drafted amendment (PLAN §2.3). For a human to apply; `.claude/rules/` is not edited.

- **What changed.** Java 4.4 adds `ChunkedProducerBatch extends ProducerBatch` (KAFKA-20578) and
  `ChunkedRecordAccumulator extends RecordAccumulator`. Rust has **no** `ChunkedProducerBatch` type:
  `rust/src/producer/internals/chunked_producer_batch.rs` declares
  `pub(crate) type ChunkedProducerBatch = ProducerBatch;` (the alias carries the Java marker) and an
  `impl` block with the methods Java adds (`new_chunked`, `is_chunked`, `extension_bytes_needed`,
  `add_buffers`, `stream`). The three overrides (`tryAppend`, `deallocateBuffer`,
  `deallocateInflightBuffer`) branch on `ProducerBatch::is_chunked()` inside the base methods.
  `instanceof ChunkedProducerBatch` is `batch.is_chunked()`, which reads the builder's stream kind.
- **Why the rules should say it.** §7 explains that a `ProducerBatch` has exactly one owner at a time
  and moves by value between the accumulator deque, the `Sender`'s in-flight map and back. A second
  batch type would need a trait object or an enum in every one of those owners, and one deque holds
  both kinds in incremental mode (a split batch stays plain). A Critic comparing class-by-class
  against Java will otherwise report the missing class, or ask for `Box<dyn ..>` in the deque.
- **`ChunkedRecordAccumulator` is a real struct, by composition** (`base: Arc<RecordAccumulator>`).
  The `Sender` keeps `Arc<RecordAccumulator>` (the shared base); only `KafkaProducer::do_send_bytes`
  dispatches `append` on the strategy (`KafkaProducer::chunked_accumulator`). Java's virtual
  `tryAppend` / `createProducerBatch` inside `appendNewBatch` are that method's step parameters.
- **Suggested text** (append to §7's "How to apply"):
  "KIP-1332's `ChunkedProducerBatch` is not a separate type: it is folded into `ProducerBatch` (one
  single-or-chunked buffer per batch, `is_chunked()` for `instanceof`), so batches of both kinds share
  one deque and keep moving by value. A chunked batch returns its memory through its stream
  (`deallocate_buffer` / `deallocate_inflight_buffer`), never through `take_buffer`."
- **Suggested anti-patterns:** a `ChunkedProducerBatch` struct or a `Box<dyn ..Batch>` deque element;
  `take_buffer()` + `deallocate_with_size(.., initial_capacity())` on a batch without checking
  `is_chunked()` (it would credit a chunk of memory the pool never lent: Critic 97 caution (a)).
- **Suggested anti-pattern (Critic 98 F1):** a folded `ChunkedProducerBatch` override that adds work
  to a plain batch's per-record path. In Java the override costs a plain `ProducerBatch` nothing.
  Put chunk-only checks behind a `#[cold]` out-of-line helper, or at the one call site that needs
  them. Here the inline first-append check alone cost a few ns per record on the default strategy
  until it moved out of line.
