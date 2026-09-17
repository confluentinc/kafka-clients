# Phase 18 — Metadata `Arc<Cluster>` carry-over (perf)

**Milestone-8 / Phase-18** · Agent number **N = 18**

Carry over **PR #10 item #3** (producer repo `kafka-clients`,
commit `c2e9b79` "Arc-wrap MetadataSnapshot and Cluster") into this branch.
This is the single highest-value, lowest-conflict perf win from PR #10 that the
consumer branch does not yet have. The other PR #10 carry-overs (#5 `Arc<str>`
`TopicPartition`, #4 synchronous write fast-path, #9 `UnsupportedVersion` error
masking) are **deferred to the post-#10 rebase** and are explicitly out of scope
here.

## Problem

`Metadata::fetch(&self) -> Cluster` (`src/metadata.rs:389`) deep-clones the
entire `Cluster` (`src/common/cluster.rs:33`, `#[derive(Clone)]` — 8+
`HashMap`/`Vec`/`String` fields) on **every call**:

```rust
pub fn fetch(&self) -> Cluster {
    let inner = self.inner.lock().unwrap();
    inner.metadata_snapshot.cluster().clone()   // O(n) deep clone
}
```

The consumer calls `fetch()` on its **per-poll background loop and per-fetch
path** far more than the producer does:

- `network_client.rs:956, 1113, 3041, 3151` — least-loaded-node / ready checks,
  every poll cycle
- `abstract_fetch.rs:501, 704`, `fetch_request_manager.rs:268` — every fetch
  build/collect
- `offset_fetcher_utils.rs:138`, `application_event_processor.rs:661`,
  `consumer_metadata.rs:215, 272`

`fetch()` has **61 call sites across 10 files** (producer + consumer + common).

## Change (mirror PR #10 `c2e9b79`)

1. **`MetadataSnapshot`**: store the cluster as `Arc<Cluster>` instead of
   `Cluster`. Keep `MetadataSnapshot::cluster(&self) -> &Cluster` (deref the
   `Arc`) so internal `&Cluster` callers are unaffected. Add an accessor that
   hands out the `Arc` (e.g. `cluster_arc(&self) -> Arc<Cluster>` or have
   `fetch()` clone the field directly).
2. **`Metadata::fetch(&self) -> Arc<Cluster>`**: return `Arc::clone(...)` — O(1)
   refcount bump instead of a deep clone.
3. **Call sites (61)**: `Arc<Cluster>` derefs to `Cluster`, so the read-only
   majority (`fetch().nodes()`, `fetch().node_by_id(...)`, `let cluster =
   metadata.fetch(); cluster.nodes()`) compile unchanged. Fix only the sites
   that require an owned `Cluster` by value:
   - Anywhere the `fetch()` result is moved into a field/struct typed `Cluster`
     → either change that field to `Arc<Cluster>` or use `(*cluster).clone()` /
     `cluster.as_ref().clone()` (only where an owned copy is genuinely needed).
   - `cluster: Cluster` cache fields in `kafka_producer.rs:74` and
     `mock_producer.rs:67` are **independent fields**, not the `fetch()` result —
     do NOT change them unless they are assigned from `fetch()`.
4. **`fetch_metadata_snapshot()` (3 call sites)**: secondary. After the cluster
   field is `Arc<Cluster>`, cloning a `MetadataSnapshot` is already cheap
   (Arc bump + small fields). Optionally Arc-wrap the snapshot too if it is
   trivially clean; otherwise leave it. Do not expand scope for this.

## Constraints

- This is **common code** shared with the producer. The producer must still
  build, pass its tests, and keep identical behavior — the consumer is not the
  only consumer of `Metadata`.
- Do NOT introduce a clone that defeats the purpose (e.g. `fetch().as_ref().clone()`
  at a hot site that only reads). The whole point is to eliminate the per-call
  deep clone on the read-only paths.
- No behavior change: `fetch()` still returns a point-in-time snapshot of the
  cluster; `Arc<Cluster>` is immutable-shared, and metadata updates already
  replace the snapshot under the lock (verify the update path swaps the
  `Arc`/snapshot rather than mutating a shared `Cluster` in place).
- License headers / rustdoc unchanged; no TODO/FIXME (DoD §5/§8).

## Tests

- All existing `metadata.rs` unit tests must pass unchanged (they read through
  `cluster()` / `fetch()` — confirm the `Arc` deref keeps assertions valid;
  several already construct `let cluster = metadata.fetch();`).
- Add/keep a test asserting `fetch()` returns shared (`Arc`) state: two `fetch()`
  calls without an intervening metadata update return `Arc`s that point to the
  same allocation (`Arc::ptr_eq`), proving no deep clone occurred.
- After a metadata update, `fetch()` returns a *different* `Arc` reflecting the
  new cluster (point-in-time semantics preserved).
- `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check`
  all green (producer + consumer).

## Out of scope

- PR #10 #5 (`Arc<str>` `TopicPartition`), #4 (sync write fast-path), #9
  (`UnsupportedVersion` error masking) — deferred to the post-#10 rebase.
- SASL_SSL consumer wiring (Phase 17, on hold).

## Critic 18 focus

- No deep `Cluster` clone left on any hot `fetch()` path; the `Arc::ptr_eq`
  invariant holds.
- Point-in-time snapshot semantics preserved (update path swaps, not mutates).
- Producer paths still correct (sender, partitioner, kafka_producer cache field).
- No `(*cluster).clone()` snuck in where a `&Cluster` would do.
