# Phase 4 Review — COMMENTS.DONE.1.md

Resolved findings from `COMMENTS.1.md` (Critic agent N=1). All six findings
addressed by Actor agent N=1 across the following fixup commits:

| # | Fix commit                                            | Original commit |
|---|-------------------------------------------------------|-----------------|
| 1 | `fixup! Phase 4 (3/8): SubscriptionState ... #1`      | `814b5c9`       |
| 2 | `fixup! Phase 4 (3/8): SubscriptionState ... #2/#6`   | `814b5c9`       |
| 3 | `fixup! Phase 4 (3/8): SubscriptionState ... #3`      | `814b5c9`       |
| 4 | `fixup! Phase 4 (4/8): ConsumerMetadata ... #4`       | `2cadba7`       |
| 5 | `fixup! Phase 4 (5/8): SubscriptionStateTest ... #5`  | `5980678`       |
| 6 | (same as #2 — single fix covers both Display sites)   | `814b5c9`       |

---

## 1. `TopicPartitionState::transition_state` silently accepts invalid transitions in release builds

- **Severity**: Design flaw / Behavior mismatch
- **Location**: `src/consumer/internals/subscription_state.rs:320-334`
- **Java reference**: `SubscriptionState.java:1040-1051`

### Resolution

Replaced `debug_assert!` with `panic!`. Per CLAUDE.md §10.1 this is a
programmer-error path that cannot be reached on the happy path (the
closure must leave `self.position` consistent with
`next_state.requires_position()`), and the consumer cannot proceed with
an inconsistent fetch state.

Chose Option 2 (panic) over Option 1 (return `Result`) to minimize blast
radius — Option 1 would touch all five call sites and change public
signatures. Java throws `IllegalStateException` (unchecked) → Rust
`panic!` is the faithful translation here.

---

## 2. `SubscriptionState::Display` uses `Vec::Debug` format

- **Severity**: Behavior mismatch (low)
- **Location**: `src/consumer/internals/subscription_state.rs:1463-1471`
- **Java reference**: `SubscriptionState.java:121-128`

### Resolution

Replaced `{:?}` formatting on `Vec<String>` (which produces
`["test-0", "test-1"]` with quotes) with explicit
`format!("[{}]", parts.join(", "))` (no quotes, comma+space), matching
Java's `Collection.toString()` semantics. Affects two sites:

- `to_string_impl` `assignment=` field (line 1465)
- `pretty_string` `UserAssigned` branch (line 1431-1432)

Java prints `TopicPartitionState`'s default `Class@hash` toString in its
`assignment=` field, which is opaque; the Rust port intentionally prints
partition names for usefulness, with a comment documenting the
deliberate divergence.

---

## 3. `partition_lead` silently masks NPE that Java would surface

- **Severity**: Behavior mismatch (low)
- **Location**: `src/consumer/internals/subscription_state.rs:1271-1281`
- **Java reference**: `SubscriptionState.java:670-673`

### Resolution

Matched Java NPE semantics: `state.log_start_offset.map(|lso|
state.position.as_ref().expect(...).offset - lso)`. The
"position-null-while-log-start-set" condition is unreachable on the
happy path per the state-machine invariants; Java NPEs and Rust panics.
Docstring updated to document the unreachable invariant.

---

## 4. `for_topic_ids` ordering: HashSet collect loses determinism

- **Severity**: Behavior mismatch (low)
- **Location**: `src/consumer/internals/consumer_metadata.rs:144-147`
- **Java reference**: `ConsumerMetadata.java:88`

### Resolution

Widened `MetadataRequestBuilder::for_topic_ids` signature from
`&HashSet<Uuid>` to `&BTreeSet<Uuid>` so the wire-order is deterministic
(sorted by `Uuid::Ord`, which matches Java's signed-long `compareTo`).
Updated the only caller (`ConsumerMetadata::request_builder_fn`) to pass
`sub_guard.assigned_topic_ids()` directly — it already returns
`&BTreeSet<Uuid>`, removing the intermediate `HashSet` collect.

Added unit test `test_for_topic_ids_preserves_btreeset_order` asserting
that for multi-id input, wire-order matches sorted Uuid order.

Note on the Java behavior: Java's `forTopicIds(Set<Uuid>)` rewraps in
`new HashSet<>`, losing iteration order. The Rust port is stricter than
Java for reproducibility / wire-byte comparison.

---

## 5. `MockListener` doesn't count invocations

- **Severity**: Test fidelity (nit)
- **Location**: `src/consumer/internals/subscription_state.rs:1705-1721`
- **Java reference**: `SubscriptionStateTest.java:983-996`

### Resolution

Added `revoked_count` / `assigned_count` `AtomicI32` fields and increment
in each callback. Matches Java `MockRebalanceListener`. Phase-11
rebalance-listener regression tests will read these counters; no
Phase-4 test currently does. Java doesn't define `lostCount`, so the
Rust port doesn't either.

---

## 6. Same as #2

Tagged as separate item by Critic but resolved by the single Display /
prettyString fix in commit for #2.

---

## Verification gates after all fixes

- `cargo build` — passes
- `cargo test --lib` — 1000 tests pass (up from 999 baseline; +1 from
  the new `test_for_topic_ids_preserves_btreeset_order` in #4)
- `cargo xtask format-check` — clean
- `cargo xtask lint` — clean
