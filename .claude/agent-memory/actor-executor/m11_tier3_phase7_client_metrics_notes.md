---
name: m11-tier3-phase7-client-metrics
description: M11 Tier3 P7 listClientMetricsResources — zero new wire types, reuses ListConfigResources filtered to CLIENT_METRICS
metadata:
  type: project
---

Tier 3 Phase 7 (`listClientMetricsResources`) landed. Deprecated-since-4.1 RPC.

- ZERO new wire types: reuses `ListConfigResourcesRequestBuilder` seeded with
  `vec![ConfigResourceType::ClientMetrics.id()]` (Java `List.of(CLIENT_METRICS.id())`).
  Response handler filters `config_resources()` by `resource_type() == ClientMetrics`
  and maps `.name()` → `ClientMetricsResourceListing`.
- New POJOs: `client_metrics_resource_listing.rs` (name-only, Display mirrors Java
  `toString` `ClientMetricsResourceListing(name='x')`), `list_client_metrics_resources_result.rs`,
  and options in `options/list_client_metrics_resources_options.rs` (options live in
  `options/` dir per admin-client.md §6, NOT flat in `src/admin/` — task path was approximate).
- All three deprecated types carry `#[deprecated(since="4.1.0", note="Use Admin::list_config_resources instead")]`
  + `#![allow(deprecated)]` at file top; every `use`/impl/test site needs `#[allow(deprecated)]`
  (mirrors the existing `ListConsumerGroups*` deprecation handling).
- Mock got REAL logic (finding #9): `state.client_metrics_configs.keys().map(ClientMetricsResourceListing::new)`,
  mirrors Java `MockAdminClient` L1431-1435. Seed via `incremental_alter_configs` on a CLIENT_METRICS resource.
- Integration: added `test_list_client_metrics_resources_lists_subscription` to the EXISTING
  `admin_cluster_configs_test.rs` (already registered in main.rs → NO main.rs edit, avoids the
  gitignored `admin_smoke_test_manual` staging trap). Creates a KIP-714 subscription via
  incrementalAlterConfigs (`interval.ms`+`metrics`) on apache/kafka:4.2.0, lists, then deletes.
  PASSES against real broker.
