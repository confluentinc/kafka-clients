---
name: phase25-fetch-path-fxhash
description: Milestone-8 Phase 25 — FxHash swap on fetch/subscription/session hot maps; which converted, which boundary-pinned and why
metadata:
  type: project
---

Phase 25 (N=25) swapped default SipHash -> FxHash on hot, internal-keyed consumer
collections (same pattern as Phase 22 selector). `rustc-hash` 2.1.2 is already a
direct dep; `FxBuildHasher` is a unit struct so `IndexMap::with_hasher(FxBuildHasher)`
and `FxHashMap::default()` both work.

**Converted (hot + internal keys + NOT a returned/boundary type):**
- `common/internals/partition_states.rs`: private `map: IndexMap<TopicPartition, S, FxBuildHasher>` — the `assignment` lookup, dominant `get_index_of`. Fully encapsulated (all methods return iterators/borrows/Vec; never exposes the IndexMap type). `set()` still takes a std `HashMap` arg (unchanged boundary).
- `consumer/internals/abstract_fetch.rs`: `nodes_with_pending_fetch_requests: FxHashSet<i32>`, private `session_handlers: FxHashMap<i32,_>`, per-fetch temps `node_targets`/`fetchable_partitions_by_node` (inner IndexMap also FxBuildHasher)/`buffered_nodes`, `compute_buffered_nodes` return, and `#[cfg(test)] pending_fetch_node_ids() -> FxHashSet<i32>` (had to change its return type since it clones the now-Fx field; test callers only use `.contains`).
- `consumer/internals/completed_fetch.rs`: private `aborted_producer_ids: FxHashSet<i64>` (per-record abort check).

**Left as std (boundary-pinned — flagged; converting would leak FxBuildHasher across a pub/pub(crate) signature):**
- `abstract_fetch.rs` `prepare_fetch_requests` RETURN `HashMap<i32,(Node,FetchSessionRequestData)>` and `out`, plus `prepare_close_fetch_session_requests` param/return (close path) — pub(crate) boundaries consumed by fetch_request_manager.
- `fetch_collector.rs`: NONE converted. `records_by_partition` (IndexMap) + `next_offsets` (HashMap) at collect_fetch are handed to the **public** `ConsumerRecords::new` / `ConsumerRecords::next_offsets() -> &HashMap<...>` API. The 656/733 maps are cold error paths with String keys / boundary error-struct types.
- `fetch_session_handler.rs` (`src/fetch_session_handler.rs`, Java `org.apache.kafka.clients.FetchSessionHandler`, genuinely `pub`): NONE converted. Every map is boundary-entangled — handler `session_partitions` IndexMap is `.clone()`d into pub(crate) `FetchSessionRequestData.session_partitions`/`.to_send` which flow to the **public** `FetchRequestBuilder::for_consumer(fetch_data: IndexMap<TopicPartition,PartitionData>)` wire builder; `session_topic_names` exposed via `pub fn -> &HashMap<Uuid,String>`. Converting cascades into the wire layer (out of scope).

**subscription_state.rs:** the `assignment` win comes from PartitionStates' internal map (above). `partition_to_state`/`assigned_partition_states` (assign_from_user/assign_from_subscribed) left std — assignment-change path (not per-fetch) AND they're the `PartitionStates::set(HashMap)` boundary arg. The `HashSet`/`HashMap` returns (partitions_needing_validation, all_consumed, paused_partitions, etc.) left std — they're pub(crate) return boundaries, collected once per poll-cycle not hot lookup tables.

Import-grouping gotcha: rustfmt wants `rustc_hash` in the external-crate group (with `log`), NOT wedged between `std::` lines — placing it right after `use std::collections...` fails format-check.
