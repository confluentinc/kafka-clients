---
name: m11-tier3-phase7-client-metrics
description: Tier 3 Phase 7 listClientMetricsResources review — clean, reuses ListConfigResources wire path
metadata:
  type: project
---

Tier 3 Phase 7 (`listClientMetricsResources`, commit aaafb94) reviewed CLEAN — no findings.

**Why clean:** deprecated-since-4.1 RPC that piggybacks entirely on Tier-1 `ListConfigResources` wire types (ZERO new wire types). RPC seeds `resource_types=[CLIENT_METRICS.id()]`, response handler filters `resource_type()==ClientMetrics` → maps `.name()` to `ClientMetricsResourceListing`. Faithful to KafkaAdminClient.java:4922.

**How to apply (spot-check heuristics that held here):**
- Mock (admin-client.md §9): Java `MockAdminClient.listClientMetricsResources` maps `clientMetricsConfigs.keySet()` → real logic, NOT unsupported. Rust `mock_admin_client.rs:1075` does the same over `client_metrics_configs` BTreeMap. Confirm mock reads same in-memory state Java uses, not a stub.
- Options getter convention across ALL admin Options: getter `timeout()`, setter `timeout_ms()`. `timeout()` != Java `timeoutMs()` name but is the established codebase pattern — NOT a divergence to flag.
- `ClientMetricsResourceListing` Display = `ClientMetricsResourceListing(name='X')` matches Java toString byte-for-byte; Eq/Hash over single `name` field matches Java.
- NotSupported test asserts BOTH code (UnsupportedVersion) AND message ("The version of API is not supported.") — errors.rs:231 message table is source of truth.
