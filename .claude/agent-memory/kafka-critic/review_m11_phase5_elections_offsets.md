---
name: review-m11-phase5-elections-offsets
description: M11 Tier-1 Phase 5 (electLeaders/alterPartitionReassignments/listPartitionReassignments/listOffsets) review — clean; adjudication heuristics
metadata:
  type: project
---

M11 Tier-1 Phase 5 (elections, reassignment, offsets) reviewed CLEAN — no
Bug/Behavior-Mismatch/Missing-Requirement defects. See [[review_m11_phase4_log_dirs]],
[[review_m11_phase2_admin_driver]].

**Why clean-verdict heuristics that held up:**
- `assertResponseCountMatch` (alterPartitionReassignments count-mismatch, deviation 4):
  Java throws `UnknownServerException` INSIDE handleResponse → propagates to call
  failure → fails ALL futures. Rust replicates by iterating `resp_futures.values()`
  and completing each exceptionally, then `HandleResult::Done`. Verify the guard is
  `errors.values().all(is_none) && received != expected` (matches Java `noneMatch`).
  Quantifier string `"...results.Expected N but received M"` has NO space after the
  period in BOTH Java and Rust — do not flag as typo.
- NOT_CONTROLLER: Rust uses the `handle_not_controller_error(mm, &error_counts())`
  (AbstractResponse-overload) helper even where Java's alter/list uses the
  `Errors`-overload (always-throw). Equivalent because a NOT_CONTROLLER top-level
  error puts NOT_CONTROLLER in error_counts() → helper returns Some → Retry. Not a bug.
- listPartitionReassignments "first-writer-wins": Java completes-exceptionally in the
  default arm then still builds the map and calls `complete(map)` (no-op). Rust
  mirrors with the same comment. Correct.
- ListOffsetsHandler retriable classification: Java `error.exception() instanceof
  RetriableException` → Rust `error.is_retriable()`, AFTER the NotLeaderOrFollower/
  LeaderNotAvailable→unmapped check. Ordering preserved.
- Sanity-check failure uses `Errors::UnknownServerError` for Java's bare
  `ApiException` — both non-retriable, message preserved. Accept.
- ListOffsetsRequest wire type REUSED from Consumer module (deviation, DoD #6):
  `for_consumer_with_features` matches Java `forConsumer` min-version ladder
  (11/9/8/7/2/1); `set_target_times`/`set_timeout_ms` added for admin, no shape hack.

**LOW non-blocking notes filed (no fix cycle):**
1. Skip comment documents only 3 of ~7 untranslated listOffsets Java tests
   (HandlesFulfillmentTimeouts, UnsupportedNonMaxTimestamp,
   NonMaxTimestampDowngradedImmediately, Metadata{Retriable,NonRetriable}Errors,
   With{,MultiplePartitions}LeaderChange). Behaviors ARE covered by
   list_offsets_handler tests + Phase-2 driver/strategy tests +
   test_list_offsets_retriable_errors. Documentation gap only.
2. Mock list_offsets fails only the TimestampSpec partition; Java's
   UnsupportedOperationException aborts the whole call. Mock-only, pre-approved
   sync→async deviation, better translation. Recorded for completeness.

**Byte-level vector tests present** for all 3 net-new wire wrappers (ElectLeaders v2,
AlterPartitionReassignments v0, ListPartitionReassignments v0). Tier 1 is now complete.
