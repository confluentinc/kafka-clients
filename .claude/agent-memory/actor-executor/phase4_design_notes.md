---
name: phase4_design_notes
description: Milestone-8 Phase 4 SubscriptionState + ConsumerMetadata design choices (PartitionStates, regex semantics, MetadataOverrides extension)
metadata:
  type: project
---

Milestone-8 Phase 4 (commit range 22f6d20..5980678) shipped `PartitionStates<S>`,
`SubscriptionState`, `ConsumerMetadata`, and their tests on the
`consumer-impl` branch.

**Why:** This phase is a foundation for Phases 5-11; every downstream phase
locks `Arc<Mutex<SubscriptionState>>` to read/mutate per-partition state.

**How to apply (load-bearing decisions to remember):**

1. **`MetadataOverrides` extended with `retain_topic_with_id_fn`** — Java
   has two `retainTopic` overloads; the topic-id variant is the one
   actually called from `Metadata.handleMetadataResponse` (line 511). The
   single-arg variant only matters for callers that lack a topic id
   (e.g. `last_seen_leader_epochs` cleanup, partial-update `mergeWith`).
   Without the new field, `ConsumerMetadata` couldn't retain topics
   assigned via broker-side RE2J regex by their id.

2. **`PartitionStates::remove_and_take(tp)`** added so
   `SubscriptionState::assign_from_user` / `assign_from_subscribed` can
   *move* the existing `TopicPartitionState` into the new assignment map
   without cloning. Mirrors Java's `assignment.stateValue(partition)`
   reuse pattern. Critical for tests that re-assign a previously
   positioned partition and expect `position` / `paused` /
   `reset_strategy` to survive.

3. **Java regex `matches()` ≠ Rust regex `is_match()`** — Java requires
   the regex to match the *whole* string; Rust matches any substring.
   The `regex_full_match` helper in `subscription_state.rs` wraps `find()`
   and verifies span coverage. Used by `matches_subscribed_pattern` and
   `check_assignment_matched_subscription`. Always remember this when
   translating any Java `Pattern.matcher(s).matches()` call.

4. **`FetchStates` collapsed from interface + enum to single enum** —
   Java's `FetchState` interface lets each enum constant override
   `validTransitions()`. Rust enums can't override per-variant methods
   cleanly, so we collapse to one enum with a `match`-based transition
   table. Plan §FetchStates explicitly endorses this.

5. **`#![allow(dead_code)]` on the SubscriptionState file** — required
   because the type lands before Phase 11 callers. Add a `pub(crate)`
   re-export in `internals/mod.rs` with
   `#[allow(unused_imports)]` until the first caller wires it up.

6. **Deferred to Phase 7** (Critic should NOT flag as missing):
   - `maybe_validate_position_for_current_leader`
   - `maybe_complete_validation`
   These depend on `EpochEndOffset` /
   `OffsetForLeaderEpoch` which only land with Phase 7's
   `OffsetForLeaderEpochClient`. Per CLAUDE.md §5 no stubs / TODOs / panics
   are left behind — the methods are simply not defined yet. Their
   dedicated tests (`testMaybeCompleteValidation`,
   `testMaybeCompleteValidationAfterPositionChange`,
   `testMaybeCompleteValidationAfterOffsetReset`,
   `testMaybeValidatePositionForCurrentLeader`, all `testTruncationDetection*`,
   and `resetOffsetNoValidation`) defer with them.

7. **`group_subscribe` returns `!groupSubscription.containsAll(subscription)`**
   - that is, the GROUP subscription must NOT include all topics from
   LOCAL. My first implementation had the direction reversed and was
   caught by `testGroupSubscribe`.

8. **Tests for `pub(crate)` types live inline** in `#[cfg(test)] mod tests`
   per the established Phase 2 pattern. The plan suggested
   `tests/consumer/internals/subscription_state_test.rs` but that path
   doesn't work for `pub(crate)` types from the test binary crate.
