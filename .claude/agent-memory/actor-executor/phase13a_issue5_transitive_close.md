---
name: phase13a-issue5-transitive-close
description: Phase 13a (5/N): Issue 5 (by_duration auto-reset) closed transitively by Issues 7+9 fixes — no code change required; pattern for diagnosing "incomplete feature" symptoms that are actually downstream stalls
metadata:
  type: project
---

Issue 5 documented `auto.offset.reset=by_duration:PT1H` as a "Phase-7 gap" —
the strategy was parsed and accepted by `ConsumerConfig` but appeared not to
trigger a `ListOffsetsByTimestamp` on assignment-change. Symptom: first
`poll()` returned `IllegalState("Missing position for fetchable partition
<topic>-0")`.

**Why:** the verbatim source-code audit before changing anything showed the
wire path was complete and correct:

- `AutoOffsetResetStrategy::timestamp()` returns `Some(now - duration_millis)`
  for `ByDuration` (the same shape Java's `OffsetFetcherUtils.resetPositions`
  uses).
- `OffsetFetcherUtils::get_offset_reset_strategy_for_partitions` is
  strategy-agnostic — it accepts any strategy whose `timestamp().is_some()`.
- `OffsetsRequestManager::send_list_offsets_requests_and_reset_positions`
  builds one request per leader using `strategy.timestamp()` — no special-case
  for `EARLIEST_TIMESTAMP` / `LATEST_TIMESTAMP` versus a positive `now - d`
  timestamp.

**How to apply:** when an issue is filed describing "feature X is incomplete"
with a fast-fail or hang symptom, *first* read the wire path end-to-end before
writing code. If the wire is complete:

1. The original symptom was likely a downstream consequence (Issue 7 race on
   `Missing position` was the original fast-fail; Issue 9's fence-rejoin gap
   was the hang).
2. Run the test once after recent unrelated fixes have landed — issues can
   close transitively.
3. The DoD resolution is then "un-ignore + document in COMMENTS.DONE.<N>.md"
   with a Resolution block citing the wire-path source lines as evidence the
   feature was already correct.

See [[phase13a_issue7_fix_notes]] and [[phase13a_issue9_fix_notes]] for the
fixes that transitively closed this one.
