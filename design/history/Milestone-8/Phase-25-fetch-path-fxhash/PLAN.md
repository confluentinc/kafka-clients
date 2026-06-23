# Phase 25 — FxHash the fetch / subscription / session hot-path collections

**Milestone-8 / Phase-25** · Agent number **N = 25**

## Why

A fresh CPU profile of the current (Tier-1 + Phase-24) consumer (EC2, Confluent Cloud
SASL_SSL, 200k, KIP-848) shows that — now that the selector cost is gone (Phases
22-24) — a meaningful slice of CPU is **default-SipHash hashing in the fetch /
subscription / session path**, which Phase 22 (selector-only) never touched:

  - `std::collections::HashMap::get` ≈ **2.8%** + `core::hash::sip::Hasher::write`
    ≈ **1.0%** (std HashMaps keyed by `TopicPartition` / node-id).
  - `indexmap::IndexMap::get_index_of` ≈ **1.55%** — `IndexMap` also defaults to
    SipHash; `SubscriptionState.assignment` is a
    `PartitionStates<TopicPartitionState>` whose internal index map is keyed by
    `TopicPartition` and looked up per fetch / per partition.

These keys (`TopicPartition`, broker node id `i32`, producer id `i64`) are **internal,
not attacker-controlled**, so SipHash's DoS-resistance buys nothing while its
per-lookup cost shows up on the per-fetch hot path. Swapping to `FxHash`
(`rustc-hash`, already a direct dependency since Phase 22) is the exact same
behavior-preserving change Phase 22 made for the selector — applied to the consumer
fetch path.

**This is purely a hasher swap — ZERO behavior change.** `HashMap`/`HashSet` semantics
are identical; `IndexMap` preserves insertion order **independent of the hasher** (so
the Java `LinkedHashMap`-equivalent ordering of partition states is unaffected); no
code path may depend on `HashMap` iteration order (it was already randomly-seeded).

## Goal

Replace the **default SipHash** with `FxHash`/`FxBuildHasher` on the **hot-path,
internal-keyed** collections in the consumer fetch / subscription / session code.
Behavior identical; the only observable change is lower CPU (target: the ~3.8% SipHash
+ part of the 1.55% `get_index_of`).

## Scope — convert these (all hot, all internal keys)

Confirm each against the profile / call sites; convert the ones on the per-fetch /
per-record / per-poll path:

  - **`subscription_state.rs`**: `PartitionStates<T>`'s internal index map
    (`IndexMap<TopicPartition, T>` → `IndexMap<TopicPartition, T, FxBuildHasher>`).
    This is the `assignment` lookup — likely the dominant `get_index_of`. Also the
    per-call `assigned_partition_states: HashMap<TopicPartition, _>` (line ~739) and
    `partition_to_state` (~694) if they are rebuilt frequently.
  - **`abstract_fetch.rs`**: `session_handlers: HashMap<i32, FetchSessionHandler>`
    (line 113), `nodes_with_pending_fetch_requests: HashSet<i32>` (108), and the
    per-`prepare_fetch_requests` maps `node_targets: HashMap<i32, Node>` (531),
    `fetchable_partitions_by_node: HashMap<i32, IndexMap<TopicPartition, PartitionData>>`
    (532), and the `out` maps (230, 638) — these are rebuilt every fetch.
  - **`fetch_collector.rs`**: per-call `next_offsets: HashMap<TopicPartition, _>` (169)
    and the per-collect `HashSet<TopicPartition>` / `HashMap` temporaries (656, 733,
    869, 954) that sit on the collect path.
  - **`completed_fetch.rs`**: `aborted_producer_ids: HashSet<i64>` (171) — checked per
    record on the abort path.
  - **`fetch_session_handler.rs`**: any `HashMap<TopicPartition, _>` / `HashMap<i32,_>`
    session-partition maps looked up per fetch.

**Use the existing `rustc_hash::{FxHashMap, FxHashSet, FxBuildHasher}`** (already a
direct dep). For `IndexMap`, use `IndexMap<K, V, FxBuildHasher>` and construct via
`IndexMap::with_hasher(FxBuildHasher::default())` (or `::default()` where the type is
inferred).

## Out of scope (defer — needs separate design)
  - **Algorithmic** fetch-path optimization: caching the fetchable-partition / node
    computation across fetches so `prepare_fetch_requests` does not rebuild
    `node_targets` / `fetchable_partitions_by_node` from scratch every call (the ~2.9%
    `prepare_fetch_requests` body beyond hashing). This is a behavior-sensitive
    restructure — a future phase.
  - rustls `UnbufferedConnection`, buffer pooling, single `mio::Poll`, integer node-id
    keys for the selector.

## Constraints (Critic will check)
  - **Behavior identical**: hasher swap only. No logic change, no API change, no change
    to ordering semantics that any caller relies on (`IndexMap` order is hasher-
    independent — preserve `with_hasher`, do not switch to a plain `HashMap`). Public /
    `pub(crate)` signatures that currently expose `HashMap<K,V>` / `IndexMap<K,V>`
    return types must NOT change their externally-observed type in a breaking way — if
    a converted collection is returned across an API boundary, keep the return type as
    the std type (collect into it at the boundary) OR confirm the `FxBuildHasher` type
    parameter does not leak into a signature that other code/tests depend on. Flag any
    such boundary.
  - **No `clients` in module paths**, `pub(crate)` for `internal` (unchanged).
  - Keep `rustc-hash` usage consistent with Phase 22.

## Tests (DoD)
  - All existing tests pass unchanged: `cargo test` (esp. `consumer::internals::*` —
    abstract_fetch, fetch_collector, subscription_state, completed_fetch,
    fetch_session_handler, fetch_request_manager).
  - If any test asserts a concrete `HashMap<...>` / `IndexMap<...>` type (not just
    contents/order), update it minimally for the hasher type param, or keep the
    boundary as the std type. Do NOT weaken any behavioral assertion.
  - `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check` green.

## Validation (Manager, post-review)
Re-profile + re-run the EC2 cloud 200k default arm. Expect `HashMap::get` +
`sip::Hasher::write` + part of `get_index_of` to shrink, CPU to drop a few points
below the current 79.3%, throughput / latency unchanged.

## Critic 25 focus
  - Pure hasher swap, zero behavior change; `IndexMap` ordering preserved
    (hasher-independent) and still `with_hasher`, not downgraded to `HashMap`.
  - No `FxBuildHasher` type param leaking into a `pub`/`pub(crate)` signature or test
    in a way that changes the contract (or, if it does, it's intentional and noted).
  - Every converted collection is genuinely on a hot path (internal keys) — no
    gratuitous conversions of cold-path maps.
  - All existing behavioral test assertions intact.
