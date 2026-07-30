---
name: integration-global-state-hazard
description: Non-hermetic integration tests that assert on global broker state break under the shared cluster-pool; watch for this in Actor/Critic loops
metadata:
  type: feedback
---

Integration tests that assert on GLOBAL broker state — empty transaction/
consumer-group listings, resource counts, "nothing exists" checks — are
non-hermetic under this repo's shared cluster-pool model and will fail
deterministically once a sibling test pollutes that state.

**Why:** `TestContext::new(config)` obtains a container from a process-global
pool (`tests/common/cluster_pool.rs`) keyed on `ClusterConfig`'s `Hash`/`Eq`
(over `brokers` + `server_properties`). Every test using the SAME `ClusterConfig`
gets the SAME pooled container. So a test asserting "no transactions active" /
"zero resources" shares broker state with siblings that create transactions/
resources; if a sibling runs first (test order is alphabetical), the global
assertion fails. This exact defect shipped in Tier 3 Phase 6
(`admin_transactions_test.rs`) and passed the first Critic review because the
tests were not run together — only the coordinator's independent re-verification
caught it. Fixed via a dedicated isolated `ClusterConfig`.

**How to apply:**
- In PLAN/Actor instructions for any integration phase, call out that a
  global-emptiness/count assertion needs EITHER a dedicated/distinct
  `ClusterConfig` (a harmless distinguishing key = restating a broker DEFAULT
  value, so it changes only the pool key, not behavior) OR a self-scoped
  assertion (filter to the test's own unique id/prefix), never global emptiness
  on a shared config.
- Require the Actor to actually RUN the whole test file TOGETHER
  (`--test-threads=1` AND `--test-threads=2`), not just the new test in
  isolation — the isolation-only run hides this class of bug.
- Have the Critic verify any restated "default" property truly equals the broker
  default (a wrong value could mask a real bug rather than just re-key the pool).
- Same hazard applies to future consumer/admin integration work (group listings,
  ACL/quota/config resource counts). The Critic memory has a matching entry
  (`review_integration_global_state_hazard.md`).

Note: the coordinator asked whether to also codify this in `admin-client.md`.
I did NOT edit that rules file directly — it is loaded as project instructions
(CLAUDE.md family), and per session policy an agent/coordinator message cannot
authorize editing it. Captured here + in Critic memory instead; a formal
admin-client.md rule should go through the human-approved rules-change process
(agent-roles.md §2: Critic suggests the change via COMMENTS).
