# Rust consumer CPU optimization analysis (design-preserving)

Rig: us-east-2 ARM (Graviton, AL2023), Confluent Cloud 12-CKU single-AZ (use2-az1),
36-partition topic, 300 MB/s (150k msg/s x 2KB), SASL_SSL, KIP-848. Profile: `perf -F299`
self-time on the live consumer in the low-latency (tight poll) regime (~112% CPU).
Source profile: `rust_perf_selftime.txt` / `rust_perf_callgraph.txt` / `rust_perf.data`.

Constraint: NO changes to the Java-faithful design (single Selector, single network task,
fetch-session protocol, SubscriptionState ownership, public API). All items below are
implementation-level (CLAUDE.md §11 language optimizations).

## Self-time buckets (300 MB/s SASL_SSL)
- TLS / socket receive ~18-20% — INHERENT (aesv8_gcm decrypt 5.1% already HW-8x,
  kernel __arch_copy_to_user 4.1%, memcpy 3.3%, rustls process/deframe ~3%, recv).
  Not addressable without changing the TLS stack; only lever is larger receive.buffer.bytes.
- Allocator churn ~10% (malloc/_int_malloc/_int_free/realloc/consolidate).
- HashMap/IndexMap lookups ~6.5% (HashMap::get 3.3%, contains_key, IndexMap::get_index_of).
- Metrics Sensor::record_internal ~2.4%.
- Fetch decode/collect (the PR #116 zero-copy target) only ~3-4% — which is why the
  zero-copy bytes::Bytes change did not move gross CPU (110%->112%, within noise).

## Addressable WITHOUT design change (priority order)
1. [trivial] Default-SipHash maps on the per-poll/fetch path -> FxHashMap (rustc-hash is
   already a dep): fetch_collector.rs:205 `next_offsets`, abstract_fetch.rs:267 `out`,
   abstract_fetch.rs:425 `partitions_with_updated_leader_info`. Faster hashing on the hot path.
2. [low risk] Reuse per-poll scratch containers instead of fresh allocation each poll:
   fetch_collector collect_fetch:204-206 (IndexMap+HashMap+Vec per poll), selector
   keys().collect() :467/:508 and `extracted: Vec` :677. Extend the existing
   ready_scratch / poll_id_scratch reuse pattern. Cuts allocator churn.
3. [medium] selector::poll_channel_reads looks up the SAME channel_id in the FxHashMap ~6x
   per call (:594,:634,:650,:750,:758,:789) due to borrow conflicts with the muted/buffered
   sets. Borrow-split (channels map vs sets into sub-structs) to hold one &mut channel.
4. [trivial] fetch_collector.rs:780 `tp.topic().to_string()` -> `topic_arc().clone()`
   (Arc inc, no String heap alloc). TopicPartition.topic is already Arc<str>.
5. [medium] metrics Sensor::record_internal ~2.4%: pre-resolve per-partition sensor handles
   instead of per-record tag-map lookups. Keeps Java-parity metric values.

## Not worth it / out of bounds
- More receive-path zero-copy: target is only ~3-4% of CPU.
- TLS crypto: inherent (rustls + aws-lc), already HW-accelerated.
- Anything touching the single-Selector / single-thread / fetch-session / SubscriptionState
  design or the public API.

## Realistic payoff
~5-8 CPU points (112% -> ~104-107%). TLS (~18-20%) is the floor. Items 1 and 4 are
near-zero-risk one-liners. The large 47%<->112% swings observed are the poll-cadence
regime (latency<->CPU tradeoff), NOT these items.
