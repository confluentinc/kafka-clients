# Plan: PR #68 — Rename `create_group_tombstone_records` and add `cancel_timers` to `Group` trait

## Apache Kafka Commit

**Commit:** `437337cf370b5204e7ddff3da4550730c1334617`
**Title:** MINOR: update method name from `createGroupTombstoneRecords` to `createGroupTombStoneRecordsAndCancelTimers` (#20949)

## Summary of Java Changes

This is a minor refactoring in the group-coordinator module:

1. **`Group` interface** (`Group.java`): adds a new `cancelTimers(CoordinatorTimer<Void, CoordinatorRecord> timer)` default method (no-op by default) so each group type can cancel its own timers on deletion.

2. **`GroupMetadataManager`** (`GroupMetadataManager.java`):
   - Renames `createGroupTombstoneRecords(String, List)` → `createGroupTombstoneRecordsAndCancelTimers(String, List)`
   - Renames `createGroupTombstoneRecords(Group, List)` → `createGroupTombstoneRecordsAndCancelTimers(Group, List)`
   - In the `(Group, List)` overload, replaces the hard-coded `timer.cancel(streamsInitialRebalanceKey(group.groupId()))` with a polymorphic call `group.cancelTimers(timer)`, so each group type is responsible for cancelling its own timers.

3. **`StreamsGroup`** (`StreamsGroup.java`):
   - Extracts `initialRebalanceTimeoutKey(String groupId)` as a `public static` helper (was previously inlined in `GroupMetadataManager`).
   - Implements the `cancelTimers` override, calling `timer.cancel(initialRebalanceTimeoutKey(groupId))`.

4. **`GroupCoordinatorShard`** (`GroupCoordinatorShard.java`): updates call site from `createGroupTombstoneRecords` to `createGroupTombstoneRecordsAndCancelTimers`.

5. **Tests**: all test mocks and direct calls are updated to use the new method name.

## Rust Translation Status

The group-coordinator subsystem (`GroupMetadataManager`, `Group` trait, `StreamsGroup`, `GroupCoordinatorShard`) has **not yet been translated** to Rust. The current Rust codebase covers the client-side network stack, producer, and common utilities only.

## Plan

### No-op for existing code

Because none of the affected Java classes have a Rust counterpart yet, there is no existing Rust code to rename or modify. This PR is a **no-op** with respect to the current Rust source tree.

### Future work (when group-coordinator is translated)

When the group-coordinator module is translated, the following conventions must be followed:

1. **`Group` trait** (`src/coordinator/group/group.rs`):
   - Add a `cancel_timers` method with a default no-op implementation:
     ```rust
     fn cancel_timers(&self, timer: &mut dyn CoordinatorTimer<(), CoordinatorRecord>) {}
     ```

2. **`GroupMetadataManager`** (`src/coordinator/group/group_metadata_manager.rs`):
   - Name the method `create_group_tombstone_records_and_cancel_timers` (snake_case of the Java name).
   - The `(group_id, records)` overload delegates to the `(group, records)` overload.
   - The `(group, records)` overload calls `group.create_group_tombstone_records(records)` followed by `group.cancel_timers(timer)`.

3. **`StreamsGroup`** (`src/coordinator/group/streams/streams_group.rs`):
   - Add `pub fn initial_rebalance_timeout_key(group_id: &str) -> String` as a free associated function.
   - Implement `cancel_timers` by calling `timer.cancel(&Self::initial_rebalance_timeout_key(&self.group_id))`.

4. **`GroupCoordinatorShard`** (`src/coordinator/group/group_coordinator_shard.rs`):
   - Update the call site in `delete_groups` to use `create_group_tombstone_records_and_cancel_timers`.

5. **Tests**: Mirror the Java test updates — mock expectations and direct calls use `create_group_tombstone_records_and_cancel_timers`.

## Verdict

**No implementation needed at this time.** The affected components do not exist in Rust yet. This plan document records the design intent so it can be applied when the group-coordinator module is translated.
