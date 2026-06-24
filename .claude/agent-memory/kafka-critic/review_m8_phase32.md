---
name: review-m8-phase32
description: Phase 32 offset-query test parity — folded-skip coverage audit, requestUpdate(bool) flag conflation, ORM disconnect-fails-not-reparks
metadata:
  type: project
---

Phase 32 (Critic 32) reviewed ORM/AKC offset-query test translation. Findings worth carrying forward:

**Folded-skip audit heuristic (Issue 1).** When the Actor "folds" a Java test as "same code
path as already-translated test X," enumerate the ACTUAL park/branch triggers, not the surface
behavior. `OffsetsRequestManager::build_list_offsets_requests` has THREE distinct park paths:
  1. ALL leaderless → `Err(StaleMetadata)` (by_node empty) → park whole state.
  2. response-error retriable → `add_partitions_to_retry` from `partitions_to_retry`.
  3. **partial build-time park**: some partitions build (`Ok`), others leaderless go to
     `remaining_to_search`; merge after metadata refresh. This is Java ORM:575-583 error==null
     re-park branch. `testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially`
     exercises (3) — NOT covered by retry tests that only hit (1) or (2). Folding it dropped
     real coverage. **Why:** "same path" claims collapse distinct branches; verify by reading
     the production match/branch arms.

**requestUpdate(true) vs (false) cannot be distinguished by need_full_update flag (Issue 2).**
`Metadata::request_update(bool)` sets `need_full_update = true` for BOTH args (metadata.rs:507);
the bool only resets `equivalent_response_count` (not exposed by any test hook). So
`need_full_update_for_test()` asserts only that *a full update* was requested — it does NOT pin
the boolean. Tests claiming Java-parity for `verify(metadata).requestUpdate(true/false)` overstate
the assertion. **How to apply:** whenever a test stands in for Java Mockito `verify(x).m(ARG)`,
check the Rust observable actually captures ARG, not just "m was called." Argument-erasing hooks
are a recurring test-fidelity smell.

**ORM fetch path FAILS on disconnect (does NOT re-park) (Issue 3).** `handle_fetch_offsets_response`
Err branch → `fail_request_state` (all waiters fail). Faithful to Java ORM:586/600
`globalResult.completeExceptionally`. The classic `OffsetFetcher` disconnect test
(`...DisconnectException`) asserts retry-and-succeed — that's a `ConsumerNetworkClient` retry-layer
property, NOT a KIP-848 ORM property. Skip is correct; Actor's rationale ("disconnect→re-park")
was wrong and conflated reset path with fetch path. **Why:** reset path and fetch path handle
per-node failure differently; don't assume one covers the other.

**Verified-clean patterns (don't re-flag):**
- `build_offsets_for_times_result` (offset_fetcher_utils.rs) seeds every requested tp with None,
  then overwrites fetched → requested-but-unfetched partition correctly surfaces None. The
  unrequested-partition NPE divergence (Java hangs, Rust resolves {tp1:None}) is faithful intent.
- 10-code retriable list, 7-code retry list, 8-row mixed matrix all match Java exactly.
- `need_full_update_for_test` is #[cfg(test)] pub(crate) — does not leak prod API.
