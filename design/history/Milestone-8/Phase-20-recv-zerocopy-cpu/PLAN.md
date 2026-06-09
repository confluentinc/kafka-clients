# Phase 20 — Receive-path copy/clone elimination (CPU)

**Milestone-8 / Phase-20** · Agent number **N = 20**

CPU optimizations from the extensive hot-path review (grounded in the SASL_SSL
`perf` profile). The Rust consumer's ~2–3× CPU vs librdkafka is **redundant deep
copies of the fetch payload (copied up to 3× before a record is yielded) +
per-request struct clones** — not async overhead (Phase 19 ruled that out). Several
violate the consumer-threading.md §27 zero-copy contract. **Every fix removes a
copy/clone with identical observable behavior → throughput-safe.**

Scope = items #1, #2, #3 (highest impact, all copy eliminations). Items #4–#6
(send-side map/header clone, per-poll scratch buffers) are **deferred** to a
follow-up phase to keep this review focused on the §27-sensitive receive path.

Implement in order #1 → #2 → #3, committing + verifying each.

## Fix #1 — `ApiVersions::get()` deep-clones `NodeApiVersions` per request

**Where:** `src/api_versions.rs:96`
```rust
pub fn get(&self, node_id: &str) -> Option<NodeApiVersions> {
    let inner = self.inner.read().unwrap();
    inner.node_api_versions.get(node_id).cloned()   // clones 3 HashMaps + a Vec
}
```
Called on **every request build** at `src/network_client.rs:~443` (`do_send`),
only to call `latest_usable_version_in_range(...)` on the clone, which is then
dropped. `NodeApiVersions` (`node_api_versions.rs`) = 3 `HashMap`s + a `Vec`.

**Fix:** add a method that computes under the read lock and returns the small
result, no clone:
```rust
pub fn latest_usable_version_in_range(
    &self, node_id: &str, api_key: &ApiKeys, oldest: i16, latest: i16,
) -> Option<Result<i16, KafkaError>> {
    let inner = self.inner.read().unwrap();
    inner.node_api_versions.get(node_id)
        .map(|v| v.latest_usable_version_in_range(api_key, oldest, latest))
}
```
Update `network_client.rs` `do_send` to call it instead of `get(...).map(...)`.
Keep the cloning `get` only if an existing test/caller needs it (check; remove if
unused). Profile bucket: `RawTable::clone` + `Vec::clone` + `drop<Option<NodeApiVersions>>`
(~2%). **Lowest risk — do first.**

## Fix #2 — `FetchResponse::response_data()` deep-copies the whole payload (twice/fetch)

**Where:** `src/common/requests/fetch_response.rs:~128` — `partition.clone()` deep-
copies each `PartitionData` **including `records: Option<Vec<u8>>`** (the raw record
bytes). Called twice per fetch:
- **(2a)** `src/fetch_session_handler.rs:~202` `handle_response`: clones the entire
  payload via `response_data()` only to do
  `response_data.keys().cloned().collect::<HashSet<TopicPartition>>()` — i.e. a
  full-payload copy thrown away for its keys. **100% wasted.**
- **(2b)** `src/consumer/internals/abstract_fetch.rs:~327` `handle_fetch_success`:
  the real consumer; `CompletedFetch::new_full` consumes `partition_data` by value.

**Fix 2a:** add `FetchResponse::response_partition_keys(&self, topic_names, version)
-> HashSet<TopicPartition>` (or similar) that iterates `self.data.responses` and
collects `TopicPartition` keys **without** cloning `PartitionData`. Use it in
`handle_response`. Deletes one full-payload copy per fetch.

**Fix 2b:** make the consumer path **move** partitions out instead of cloning. The
`PendingFetchCompletion::Response` envelope (`fetch_request_manager.rs:~100`) **owns**
the `FetchResponse`; `handle_fetch_success` currently takes `&FetchResponse`. Add
`FetchResponse::into_response_data(self, ...) -> IndexMap<TopicPartition, PartitionData>`
that moves partitions out (consuming the response), and have the drain path
(`drain_pending_completions` → `handle_fetch_success`) pass the owned `FetchResponse`
by value so records move into `CompletedFetch` (no copy). Topic name into the
`TopicPartition` should clone the **`Arc<str>`** from `SubscriptionState`/session,
not `String::clone` per partition (§27 topic-name rule).

**§27 invariants (CRITICAL — this is the zero-copy receive contract):**
  - Fetched record bytes are owned by exactly one buffer and **moved**, never copied,
    into `CompletedFetch`. No `Vec<u8>` clone of records anywhere on this path.
  - `handle_fetch_success`'s lookups into `request_data.to_send` / `session_partitions`
    use the **partition key** (`&TopicPartition`), which is unaffected by moving the
    values — verify these still resolve.
  - **No records dropped or duplicated**; partition/offset ordering preserved.
  - The existing per-record allocation-budget test (Phase 6/§27 precedent) must still
    pass; **tighten/extend it** to assert no per-record (and no per-partition payload)
    copy on this path.

## Fix #3 — `handle_completed_receives` copies the payload out with `.to_vec()`

**Where:** `src/network_client.rs:~611`
```rust
let receives: Vec<(String, Option<Vec<u8>>)> = self.selector.completed_receives().iter()
    .map(|r| (Receive::source(*r).to_string(), r.payload().map(|p| p.to_vec()))).collect();
```
`p.to_vec()` copies the whole fetch payload out of the selector's `NetworkReceive`
buffer (`buffer: Option<Vec<u8>>`) just to release the `&self.selector` borrow before
parsing. The selector clears `completed_receives` next poll anyway.

**Fix:** add a selector method that **drains `completed_receives` by move**, returning
owned `(Arc<str> source, Vec<u8> buffer)` (move the `Vec` out of each
`NetworkReceive`), then parse from the moved buffer with zero copy
(`ByteBufferAccessor`/`from_bytes` consuming the `Vec`). Source as `Arc<str>` (selector
already interns ids → drops the `source.to_string()` alloc too).

**Invariants:** completed_receives must not be double-processed; the drain replaces
the current `clear()`-at-next-poll behavior cleanly (it's drained here, so the next
`clear()` is a no-op / still correct). Verify nothing else reads `completed_receives`
after this point in the poll.

## Tests (DoD)
  - All existing tests pass unchanged (network_client, fetch_response, fetch_session_handler,
    abstract_fetch, fetch_request_manager, selector, completed_fetch).
  - **§27 allocation-budget test** (per-record + per-partition) re-asserted/tightened:
    no payload `Vec<u8>` copy on the fetch receive→decode path.
  - Add/extend a test that a fetch response's records survive the move into
    `CompletedFetch` intact (count, offsets, bytes) and that `handle_response`'s
    key extraction matches the old `response_data().keys()`.
  - `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check` green.
  - Docker-gated integration tests at least compile.

## Out of scope (deferred)
  - #4 `create_fetch_request` map clone (`abstract_fetch.rs:272`), #5 header clone
    (`send_builder.rs:96`), #6 per-poll scratch `Vec`/`String` allocations
    (`network_client.rs:588`, `selector.rs:890`) — follow-up phase.
  - SO_RCVBUF / `receive.buffer.bytes` tuning (config probe, not code).
  - Anything in the rustls/crypto path (inherent).

## Critic 20 focus
  - **§27 zero-copy held**: records moved not copied; no per-record/per-partition
    payload copy; no records dropped/duplicated; allocation-budget test proves it.
  - Fix #1: `do_send` picks the same request version as before (behavior parity);
    no functional change beyond removing the clone.
  - Fix #2a: keys-only extraction is exactly equivalent to the old
    `response_data().keys()`.
  - Fix #2b/#3 ownership moves don't break fetch-session bookkeeping or the selector
    `completed_receives` lifecycle (no double-process, no use-after-move).
  - Throughput-safe: tight read-drain loop + poll-completion model untouched.

## Validation (Manager, post-review)
Re-profile + re-run the EC2 cloud 200k/300k Rust-vs-librdkafka-C comparison; expect
the `memcpy`/`from_iter` buckets + the `NodeApiVersions` clone to shrink and overall
SSL CPU to drop (toward librdkafka), throughput/latency unchanged.
