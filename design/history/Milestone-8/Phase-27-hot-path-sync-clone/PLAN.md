# Phase 27 — Hot-path synchronization & clone churn (lock-once, FxHash, no per-poll copies)

**Milestone-8 / Phase-27** · Agent number **N = 27**

Four CPU fixes derived from a fresh line-table profile of the Phase-26 build on
the cloud rig (EC2 Graviton, SASL_SSL, 200k msg/s latency-tuned, same-day A/B):

| | CPU | wakeups/s | µs CPU/wakeup | RSS | p50/p99/p999 |
|---|---|---|---|---|---|
| Rust Phase-26 | **83.2%** | 6,849 | ~121 | 36MB | 4/12/36 |
| librdkafka stock | **52.0%** | 10,232 | ~51 | 25MB | 4/9/25 |

Thread split: bg IO thread 71.1%, app thread 16.7%, all 16 tokio workers 0%.

**The profiling discovery driving this phase:** the residual gap is *not* TLS
(AES 4–5%, rustls `process_new_packets` 1.25%) and not algorithmic — it is
synchronization + clone + allocator churn that Java gets for free:

- `perf annotate` (line tables piercing fat-LTO inlining) showed **~68% of
  `prepare_fetch_requests`' self-time (2.9% total) in `alloc/sync.rs` (Arc
  refcount) + mutex futex atomics**, ~1% in fetch logic.
- `run_once` self-time dominated by futex + atomics: 3–4 `tokio::Mutex`
  acquisitions of the (provably bg-exclusive) `NetworkClientDelegate` per
  iteration + 3 `std::Mutex` acquisitions of `request_managers`.
- **std-SipHash `HashMap<String, _>` lookups + key memcmp ≈ 3.5%** in the
  NetworkClient layer (`in_flight_requests`, `ClusterConnectionStates`,
  `ApiVersions`) — Java's `String` caches its hashCode; Rust rehashes the full
  key per lookup.
- Per-poll heap copies the JVM absorbs in TLAB: the poll-timeout clamp copied
  the whole assignment `HashSet` per `poll()` (~1.3% app thread);
  `prepare_fetch_requests` deep-cloned the metadata topic-ids
  `HashMap<String, Uuid>` per call (even when Phase-26 short-circuited),
  cloned `FetchPosition` (carrying `Node` → heap `String`s) per partition,
  and cloned the buffered set per call.

## Fix #1 — one delegate lock per `run_once` iteration

`ConsumerNetworkThread.network_client_delegate` (`Arc<tokio::Mutex<…>>`) is
locked **only** by the bg task (verified: the app side signals via the
`WakeupTrigger` token and the selector `Notify`; the `Arc` is cloned into
`ConsumerNetworkThread` only). Acquire the guard once at the top of `run_once`
and pass it through; `maybe_fail_on_metadata_error_uncompleted` takes
`&mut NetworkClientDelegate`. Java parity: Java owns `networkClientDelegate`
as a plain field, no lock at all.

## Fix #2 — FxHash the NetworkClient-layer node-id maps

`InFlightRequests.requests`, `ClusterConnectionStates.node_state`,
`ApiVersions.node_api_versions`, `NetworkClient.nodes_needing_api_versions_fetch`
→ `FxHashMap` (Phase 22/25 precedent). All private internals; the public
`connecting_nodes()` accessor's `HashSet` deliberately stays std (boundary rule).

## Fix #3 — allocation-free poll-timeout backoff clamp

`poll_for_fetches` copied `assigned_partitions()` (HashSet + 24 `Arc` clones +
SipHash inserts) per `poll()` to decide the retry-backoff clamp. The predicate
("no assigned partitions, or any partition lacks a valid position") is computed
allocation-free by the existing Java-mirrored `num_assigned_partitions()` /
`has_all_fetch_positions()`. CLAUDE.md §11.

## Fix #4 — `prepare_fetch_requests` holds one SubscriptionState lock, borrows

- One guard for the whole preparation (was 2–3 locks **per partition**); all
  queries borrow through it. No `.await` while held (§16). Java's per-query
  `synchronized` is biased/JIT-elided and returns references — one guard +
  borrows is the closest Rust equivalent.
- Only `Copy` fields (offset, epoch) read out of the position borrow; the
  output `Node` cloned once per distinct node, not per partition.
- Java's `selectReadReplica` / `maybeNodeForPosition` inlined at their only two
  call sites (prepare loop, `compute_buffered_nodes`) to keep borrow scopes
  tractable; stale-replica arm performs `FetchUtils.requestMetadataUpdate`'s
  two actions directly on the held guard. `compute_buffered_nodes` reads node
  ids only.
- Topic id from the `Cluster` snapshot (`cluster.topic_id()`) — same metadata
  snapshot Java's `metadata.topicIds()` references; the per-call deep clone of
  `HashMap<String, Uuid>` is gone, and the Phase-26 short-circuit path now
  performs no allocation at all.

## Explicitly deferred (decide after re-profiling this phase)

- **mio-based readiness** in the Selector's dedicated bg thread (tokio
  `Registration::poll_ready` ≈ 5% — every WAIT polls all 24 channels × 2
  interests ~2×/wakeup; Java NIO's `selectedKeys()` is O(ready)). Deviates
  from CLAUDE.md §8's "Tokio" wording — needs explicit sign-off.
- **rustls `UnbufferedConnection`** (memcpy ≈ 3.7% — buffered copy-out).
  Closer to Java's `SSLEngine` (app-owned buffers); larger change.
- **`bytes::Bytes` receive path / buffer pooling** (allocator ≈ 10% both
  threads incl. `vec![0u8; size]` per response = malloc + memset of ~200MB/s)
  — kept deferred per user decision.

## Validation

Same-day, same-cluster, same 150s-window `/proc` method @200k latency-tuned
(results recorded in `client-comparison-results.md` after the run).
