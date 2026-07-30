---
name: review-integration-global-state-hazard
description: Non-hermetic global-broker-state assertions in shared-cluster-pool integration tests, and how the ClusterConfig pool key isolates them
metadata:
  type: feedback
---

Integration tests that assert on GLOBAL broker state are non-hermetic under
the shared cluster-pool model. Flag them in review.

**Rule:** A test that asserts "nothing exists" / a global count on broker-wide
state — empty transaction listings (`list_transactions ... is_empty()`), empty
consumer-group listings, total resource/ACL/topic counts, etc. — is a defect if
it shares its `ClusterConfig` with sibling tests that create such state. The
pool (`tests/common/cluster_pool.rs`) is a process-global
`HashMap<ClusterConfig, cluster>`, and `ClusterConfig` derives `Hash`/`Eq` over
`brokers` + `server_properties` (BTreeMap). Every test with an identical
`ClusterConfig` gets the SAME pooled container, so sibling-created state
(transactional ids left in `Empty`, groups, topics) leaks into the global
assertion. Tests run alphabetically within a file, so ordering makes the
failure deterministic, not flaky.

**Two valid fixes (accept either):**
1. Dedicated/distinct `ClusterConfig` → pool hands the test its own isolated
   container. A harmless way to make a distinct key is to restate a broker
   DEFAULT as an explicit property (e.g.
   `KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS=50`, which is Kafka's default) —
   this changes only the pool key, not observable broker behavior. VERIFY the
   restated value truly equals the broker default; if it silently changes
   behavior it could mask a real bug — flag that.
2. Self-scoped assertion: filter the listing to the test's own unique
   prefix/id and assert on that subset, instead of asserting global emptiness.

**Why:** the RPC under test is usually correct (it faithfully returns what the
broker reports); only the test's assertion of GLOBAL emptiness is wrong. Do not
let an Actor "fix" it by weakening the RPC.

**How to apply:** when reviewing any admin/consumer integration test, scan for
`is_empty()` / `len() == N` / "unknown"/"none active" assertions over broker-wide
listings. Confirm the test's `ClusterConfig` is not shared with a sibling that
writes that state, or that the assertion is scoped to the test's own prefix.
Real precedent: `tests/integration/admin_transactions_test.rs`
`test_list_transactions_returns_empty_when_none_active` (fixup 2105176 — fixed
via option 1, `txn_single_broker_isolated`).
