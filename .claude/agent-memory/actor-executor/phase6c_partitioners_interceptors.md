---
name: Phase 6c partitioners + interceptors + ProducerRecord
description: Patterns kept from Phase 6c — interceptor chain panic isolation requires Clone bound, randomPartition virtual hook via Box<dyn Fn>, ArcSwapOption for volatile state, sticky-partitioner CAS race-resolve via reload.
type: project
---

Translation patterns settled during Phase 6c that are likely to recur in
Phase 6d (RecordAccumulator) and beyond.

**Why:** These shapes are non-obvious from a literal Java→Rust transcription and worth replicating without re-deriving.

**How to apply:** When translating a class that crosses any of these
boundaries, reach for the same pattern.

## ProducerRecord

- `topic: Arc<str>` is mandatory (CLAUDE.md rule 11). `topic()` returns `&str` borrowing from the `Arc`. Constructors take `impl Into<Arc<str>>` so `&str` and `String` both work.
- Five Java constructors collapse to `with_full`, `with_timestamp`, `with_partition_and_headers`, `with_partition`, `with_key`, `new`. The 6-arg one is the canonical entry point.
- `null topic` rejection from Java translates as "empty `Arc<str>`" check at the constructor — Rust's type system can't model `null` literally; the closest semantic is the empty string. Returns `Err(ProducerRecordError::NullTopic)` — never panics.
- `ProducerRecordError` is its own enum (not folded into `KafkaError`) because the constructor path is pre-producer-pipeline; users need to handle it without dragging in the full broker error surface.
- `Hash` impl iterates `self.headers.iter()` and hashes each `RecordHeader` because `RecordHeaders` itself does not implement `Hash` (insertion-order list). Matches Java's `headers.hashCode()` (which is order-dependent).

## Partitioner trait

- Java's `Object key` / `Object value` become `Option<&dyn Any>`. The byte slices are `Option<&[u8]>` — most general borrowed form (CLAUDE.md rule 12).
- `configure(&mut self, &HashMap<String, String>)` has a default no-op impl. Same for `close(&mut self)`.
- Implementations must be `Send + Sync` (the producer hot path will share the partitioner across appender threads).

## RoundRobinPartitioner — atomic-counter pattern

- Per-topic counter: `Mutex<HashMap<Arc<str>, Arc<AtomicI32>>>`. The mutex guards only insert (`computeIfAbsent`); the hot path holds an `Arc<AtomicI32>` and calls `fetch_add` outside the mutex. Pattern: take mutex → look up → drop mutex → atomic op.
- `to_positive` is `n & 0x7fff_ffff`, mirroring Java's `Utils.toPositive`. Inline rather than import (it's a one-liner; saves a cross-module dep).

## ProducerInterceptor trait — owned-record signature

- `on_send(&self, record: ProducerRecord<K, V>) -> ProducerRecord<K, V>` — Java's mutate-and-return pattern translates to owned-in / owned-out. NOT boxed (NOTES.md DoD: "returns the (possibly modified) record by value").
- The 2-arg / 3-arg `onAcknowledgement` overloads in Java collapse to one 3-arg trait method in Rust, since Java's 2-arg default delegates to the 3-arg form anyway.

## ProducerInterceptors — chain with panic isolation

- Implementation requires `K: Clone, V: Clone` because Java passes the record by reference (a panic leaves the reference unchanged) but Rust's owned-record signature requires we save a clone before each interceptor call. If the interceptor panics, the previous good record is forwarded — Java behavior preserved.
- `catch_unwind(AssertUnwindSafe(...))` wraps each call. Same pattern as `ProducerBatch::complete_future_and_fire_callbacks`. Identifier-bearing fields (topic, partition) are captured before the call so the warn-log can fire even if the move happened.
- Test fixtures share `InjectionFlags` via `Arc<AtomicBool>` so tests can flip exception-injection flags after handing ownership of the interceptor to the chain. Avoids the unsafe raw-pointer trick that the first cut needed.

## BuiltInPartitioner — virtual hook + ArcSwap volatile

- Java's package-private override hook (`SequentialPartitioner extends BuiltInPartitioner`, overrides `randomPartition`) translates to a `Box<dyn Fn() -> i32 + Send + Sync>` field constructed via `new_with_random_source`. Default uses `rand::rng().random::<i32>() & 0x7fff_ffff`.
- `volatile PartitionLoadStats` → `ArcSwapOption<PartitionLoadStats>`. `load_full()` returns `Option<Arc<PartitionLoadStats>>` — single atomic load on the hot path. Replacements via `store(Some(Arc::new(…)))`.
- `AtomicReference<StickyPartitionInfo>::compareAndSet(null, new)` → `arc-swap` `compare_and_swap(&None::<Arc<T>>, Some(new.clone()))`. The return type is a `Guard` you cannot move out of `Option`; if the swap loses the race (returned `Some(prev)`), do NOT try to `prev.unwrap()` — call `.load_full()` again to fetch the winner.
- Sticky-partition switch logic guards `producedBytes >= sticky_batch_size && enable_switch || producedBytes >= sticky_batch_size * 2`. The `*2` cap prevents pathological runaway. Translated literally — don't refactor the boolean shape.
- `update_partition_load_stats` mutates the input `&mut [i32]` in place — the cumulative-frequency table is built in-place to avoid allocation. Caller has to snapshot `len()` before the call because the borrow checker can't reason about `Some(&mut q)` and `q.len()` happening on the same line.

## Java Cluster constructor preserves partition-input order

- `Cluster::new(...)` shuffles `nodes` (matches Java) but `partitions_by_topic` and `available_partitions_by_topic` keep input order (entry-or-default-push). The RoundRobinPartitioner test relies on the non-partition-numerical input order `[part1, part2, part0]` to verify the available-partitions list is `[part2, part0]`. That's why the test passes deterministically despite the node shuffle.
