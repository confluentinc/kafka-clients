---
name: phase36-fetch-collector-test-parity
description: Phase 36 — FetchCollectorTest/CompletedFetchTest parity; Fetch.isEmpty positionAdvanced fidelity bug; Mockito-tryUpdating→false translation; control-marker limitation
metadata:
  type: project
---

# Phase 36 — FetchCollectorTest + CompletedFetchTest parity (Actor 36)

Commits `9ea6e7b` (collector + fidelity fix) and `8a97c7d` (completed_fetch).
14 new tests; all 4 DoD checks green.

## Production fidelity bug found + fixed (perf-neutral)

`AsyncKafkaConsumer.poll` used `ConsumerRecords::is_empty()` (records-only)
where Java uses internal `Fetch.isEmpty()` = `numRecords==0 && !positionAdvanced`
(`AsyncKafkaConsumer.java:861`, `pollForFetches:1879`). The Rust port collapsed
Java's internal `Fetch<K,V>` into `ConsumerRecords<K,V>`, dropping the
`positionAdvanced` flag. Symptom: an all-aborted READ_COMMITTED batch advances
the position with zero records — Java returns promptly, Rust blocked until the
poll timeout.

Fix: added `position_advanced: bool` to `ConsumerRecords` (set at construction,
`new_with_position_advanced` pub(crate) ctor) + `pub(crate) is_fetch_empty()`.
**Public `is_empty()` UNCHANGED** (it correctly mirrors Java's *public*
`ConsumerRecords.isEmpty()` = records-only). Poll loop + `poll_for_fetches`
first/second collect now use `is_fetch_empty()`. No per-record allocation, no
dynamic dispatch — fetch hot path untouched. FetchCollector threads
`position_advanced` from `fetch_records_from_partition` (the flag was already
computed at the Position arm but discarded) via `FetchPartitionOutcome`, ORs it
across partitions (Java's `Fetch.add`).

Pattern: when a Rust port collapses two Java types into one, audit every Java
call site of the *dropped* type's methods (`Fetch.isEmpty` vs public
`ConsumerRecords.isEmpty` share a name but differ).

## Mockito `tryUpdating*→false` → real-state translation

Java's OnNotAssignedPartition tests mock each `tryUpdating{HighWatermark,
LogStartOffset,LastStableOffset,PreferredReplica}→false` independently. In real
`SubscriptionState`, `try_updating_*` returns false IFF the partition is not
assigned (`assigned_state_or_null_mut → None`) — all four share one gate.
To isolate + mutation-test each branch: call `update_partition_state` directly
with an UNASSIGNED partition and set ONLY the target field non-negative
(`PartitionData` defaults: `high_watermark=0`, others `-1`, `preferred=-1`, so
each branch is gated on `field >= 0` / `!= INVALID`). Deleting the targeted
short-circuit makes the fn fall through to skipped branches and return `true` —
fails the assert. The full-collector path can't reach `update_partition_state`
for an unassigned partition (the `has_valid_position` guard short-circuits
`initialize` first) — so per-branch tests hit the method directly; a
collector-level companion asserts the same observable empty-fetch.

## testErrorInInitialize — `#[cfg(test)]` injection hook

Java overrides `initialize()` via anonymous subclass to throw. Rust struct has
no inheritance → added `#[cfg(test)] force_initialize_error: Option<Box<dyn Fn()
-> KafkaError + Send + Sync>>` consulted at the top of `initialize`, plus
`set_force_initialize_error`. Compiled out of release. Queue-state contract:
recordCount==0 ⇒ empty entry polled off (records bytes set to `vec![]` so
`records_size==0`); recordCount>0 ⇒ retained. (`collect_fetch` push_front gate
is `!(fetch_empty && records_size==0)`.)

## Control-marker limitation (affects txn tests)

`CompletedFetch` does NOT translate `ControlRecordType`/`containsAbortMarker`
(module docstring): a control batch from a previously-aborted producer →
`UnsupportedVersion` error. So txn parity tests CANNOT append an
`EndTransactionMarker` to an aborted-producer batch. Use PLAIN transactional
data batches; drive abort via the response `aborted_transactions` list (the
mechanism `isBatchAborted` consults). Record-count contract is identical; only
literal offset values differ (no marker occupies an offset slot) — document the
deviation. For the collector txn test the committed second batch uses a
DIFFERENT producer id to avoid the aborted-set carry-over.

## testFetchWithOtherErrors expansion

`Errors` has no `values()` (adding one = production change, out of scope).
Enumerate the full set test-only by iterating codes `0..=133` via
`Errors::for_code` + dedup `HashSet`. Unassigned codes fold to
`UnknownServerError` (in the handled list, skipped).

## testCorruptedMessage reduction (documented, NOT fixed)

`KafkaError::Serialization(String)` carries only a message → assert origin
(KEY/VALUE), offset, partition string, cached re-raise. CANNOT assert
timestamp / raw key+value buffers / headers (report-01 finding #6). Not a
behavioral defect (consumer behavior correct; only error introspection reduced)
→ error type left unchanged.

## See also
- [[phase13a-fetch-test-notes]] — poll_for_fetches error-swallow gap (Issue 4)
- [[phase16-batch-loading-notes]] — receive-path zero-copy cursor
- `design/history/Milestone-8/Phase-36-test-parity-fetch-collector/PLAN.md`
</content>
