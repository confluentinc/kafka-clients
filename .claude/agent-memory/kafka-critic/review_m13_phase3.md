---
name: review-m13-phase3
description: M13 Phase 3 (consumer offsets/commit, KAFKA-20165) review — partial-results-at-deadline confirmed faithful; relocated maybeUpdateLastSeenEpoch gap
metadata:
  type: project
---

# M13 Phase 3 review (agent 63, commits 2502bc71..0f813ef4) — CLEAN except one pre-existing doc-accuracy finding

**KAFKA-20165 (CommitRequestManager `OffsetFetchResult`) is faithfully
translated.** The load-bearing check — 1b partial-results-at-deadline —
CONFIRMED: Java `handleRetriablePartitionErrors` completes the future with
partial results **as success** (`result.complete(res)`), NOT
`TimeoutException`, when `isExpired() || remainingMs() <=
remainingBackoffMs(now)`. Only the **group-level** error path times out. Rust
`fetch_offsets_with_retries` mirrors: partition-error branch `break Ok(value)`;
group-error branch `break Err(KafkaError::timeout(..))`.

**Retriable partition errors = exactly UNKNOWN_TOPIC_OR_PARTITION +
UNKNOWN_TOPIC_ID** (Java comment states it explicitly). NOT NOT_LEADER, NOT
UNSTABLE_OFFSET_COMMIT (that stays a separate group-fail path).

## The one finding (pre-existing, low): relocated `maybeUpdateLastSeenEpochIfNewer`
Java calls it **inside the CommitRequestManager driver** on ALL fetched
offsets, so both `updateFetchPositions` AND public `committed()` refresh the
metadata leader-epoch cache. Rust relocated it to
`OffsetsRequestManager::refresh_offsets`, which is (a) only on the position-init
path (the AEP `FetchCommittedOffsetsEvent` handler strips+completes, never
touches metadata), and (b) gated on `currently_initializing.contains(tp)`.
So `committed()` never refreshes epochs in Rust. PLAN line 81 skip wording
("both full-success and partial-result paths, idempotent") overstates it.
Pre-existing (predates 4.3.1), so not a regression — flagged for skip-doc
accuracy only.

Related same-root: Rust `committed()` **strips** null/errored partitions
(handle type is `HashMap<TP,OffsetAndMetadata>`, no Option) whereas Java
returns them **present-with-null** via `toOffsetMapWithNulls`. Pre-existing;
KAFKA-20165 widens the silently-absented set to errored partitions.

## Heuristics that paid off this phase
- **"4.3.1 changed dedup" was a probe/red herring.** `addOffsetFetchRequest`
  / `chainFuture` changed **type signatures only** — always diff the Java
  method body, not just trust the task's framing of what changed.
- **Skip-claim verification = grep the Java callers.** `maybeSetPartitionEndOffsetRequest`
  / `clearPartitionEndOffsetRequests` callers are all in `OffsetFetcher.java`
  (classic, §20 untranslated); the async ORM never calls them → skip valid.
  Confirmed the async `process_current_lag` needs no change.
- **A parameterized Java test can gain behavioural teeth via its BODY, not a
  new method.** `testOffsetFetchRequestPartitionDataError` added a 3-partition
  shape (tp1/tp3 error, tp2 clean) in-place; the CommitRequestManagerTest diff
  otherwise added zero net-new `@Test`. Don't conclude "no new test" from the
  absence of a new method — read the changed comment + body.
- **Rust-added tests beyond Java are fine** (the partial-results-on-deadline
  test has no Java mirror) — note it as added coverage, not a false parity
  claim.

## Verified-faithful (don't re-litigate)
create_assignment KIP-1251 widening (HashMap<Uuid,HashMap<i32,i32>> +
.into_keys()); request_all_offsets (topics.is_none(), no version gate);
OffsetFetcherUtils else-warn + lag helpers; SubscriptionState clear_end_offset
/ maybe_clear_partition_end_offset_requested; AEP hunk touches ONLY
FetchCommittedOffsets (no rebalance leak — Phase 4 owns those).
