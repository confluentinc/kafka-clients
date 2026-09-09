# Default key partitioner: CRC-32, not murmur2

| | |
|---|---|
| **Status** | Implemented (Phase 1) |
| **Last updated** | 2026-09-08 |
| **Scope** | Producer keyed-record partitioning only. The keyless (sticky, KIP-794) path is unchanged. |
| **Deviation from Java** | Deliberate and user-approved. The default key hash is IEEE CRC-32 (librdkafka `consistent_random`), not Java's murmur2. |

## Why this deviates from the Java client

The Apache Kafka **Java** client hashes a record's key with **murmur2**
(`Utils.toPositive(Utils.murmur2(key)) % numPartitions`,
`BuiltInPartitioner.partitionForKey`). Every other Confluent client — anything
built on **librdkafka** (the C/C++/Go/Python/.NET fleet) — defaults instead to
`consistent_random`, which hashes the key with **IEEE CRC-32**
(`rd_crc32(key) % partition_cnt`). The two hashes send the same key to
different partitions.

This Rust client is intended to co-partition keyed records with the existing
**librdkafka-based fleet**, so a customer migrating a mixed deployment does not
find that records for the same key land in a different partition depending on
which client produced them. It therefore **defaults to CRC-32** for exact
parity with librdkafka's `consistent_random`, and offers murmur2 as an opt-in
for callers who need exact Java-client parity instead.

This is a deliberate, reviewed departure from the "match the Java client
exactly" translation rule (a DoD §7 justified deviation): the keyless sticky
logic is untouched, and only the *keyed* hash changes.

## Semantics: `KeyHasher::Crc32` (default) vs `KeyHasher::Murmur2`

The keyed-record hash is selected by the internal `KeyHasher` enum
(`src/producer/internals/built_in_partitioner.rs`), whose `Default` is
`Crc32`.

| Aspect | `Crc32` (default) | `Murmur2` (opt-in) |
|---|---|---|
| Hash function | IEEE 802.3 / zlib CRC-32, poly `0x04C11DB7` reflected (`crc32fast::hash`, == librdkafka `rd_crc32`, == `zlib.crc32`) | Kafka murmur2 (`Utils.murmur2`) |
| Partition formula | `(crc32fast::hash(key) % (num_partitions as u32)) as i32` | `to_positive(murmur2(key)) % num_partitions` |
| Sign handling | CRC taken **unsigned**; **no** `to_positive` / `& 0x7fffffff` masking (librdkafka does not mask either) | `to_positive` masks the sign bit (`& 0x7fffffff`) exactly as Java does |
| Empty (present, zero-length) key | **Not hashed** — `hashes_key(b"") == false`; falls through to the sticky partitioner, matching librdkafka's "empty key → random/sticky" | **Hashed** — `hashes_key(b"") == true`; a zero-length key is hashed like any other, matching Java |
| Absent (`None`) key | Not hashed (sticky), both hashers — governed by the caller's `key.is_some()` gate, not `hashes_key` | same |
| Parity target | librdkafka `consistent_random` | Apache Kafka Java client |

The **unsigned vs `to_positive`** distinction is load-bearing and is proven by a
unit test: for `key = "a"`, `crc32("a") = 0xE8B7BE43` has its high bit set, so
the unsigned CRC path gives `0xE8B7BE43 % 3 == 0` while a `to_positive`-masked
result would give `1`. The default deliberately takes the **unsigned** path to
match librdkafka.

Golden CRC-32 vectors (pinned in `test_crc32_golden_vectors`, and re-used by the
Python perf-test self-check in
`bindings/python/test/performance/partitioner.py`):

| Input | `crc32` |
|---|---|
| `""` | `0x00000000` |
| `"a"` | `0xE8B7BE43` |
| `"abc"` | `0x352441C2` |
| `"123456789"` | `0xCBF43926` |
| `"The quick brown fox jumps over the lazy dog"` | `0x414FA339` |

## Selecting the hasher: `partitioner.class`

The hasher is chosen with the new `partitioner.class` producer config
(`ProducerConfig::PARTITIONER_CLASS_CONFIG`). Accepted values:

| `partitioner.class` value | `KeyHasher` | Meaning |
|---|---|---|
| *unset* (default) | `Crc32` | CRC-32, librdkafka `consistent_random` parity |
| `ConsistentRandomPartitioner` | `Crc32` | CRC-32, identical to the unset default |
| `Murmur2RandomPartitioner` | `Murmur2` | murmur2, exact Java-client parity |

Any other value is rejected at config-construction time with the exact
Apache Kafka `ConfigException` text (the message is the behavioral contract —
DoD §3). For a value `<value>` the inner message is:

```
Invalid value <value> for configuration partitioner.class: Class <value> could not be found.
```

(The Rust `KafkaError` Display prepends its variant tag, e.g.
`IllegalArgumentError: `, ahead of that inner text; tests assert on the inner
text with `ends_with`.) This mirrors Java's `ConfigDef` reflectively loading the
named class and failing when it cannot be found.

### Exact Java parity

A caller who needs byte-for-byte the same partition assignment as the Apache
Kafka **Java** client sets:

```
partitioner.class=Murmur2RandomPartitioner
```

This routes keyed records through `to_positive(murmur2(key)) % num_partitions`,
which is byte-for-byte the Java `BuiltInPartitioner.partitionForKey` behavior
(including hashing zero-length keys).

## WARNING: mixed-fleet partitioning

A fleet that mixes producers on **different** default partitioners will split a
single key's records across **different partitions**, breaking per-key ordering
and any consumer assumption that "all records for key K are in one partition":

- This Rust client (default) and any librdkafka-based client (default) →
  **CRC-32** → agree with each other.
- The Apache Kafka Java client (default) → **murmur2** → disagrees with both.

So:

- **Migrating from librdkafka** (C/Go/Python/.NET/…): the Rust default already
  co-partitions with your existing fleet. Do nothing.
- **A deployment that also runs Java producers** on the same topics and relies
  on cross-client per-key co-partitioning: either set
  `partitioner.class=Murmur2RandomPartitioner` on the Rust producers (so they
  match Java), **or** switch the Java producers to a CRC-32 partitioner. Do not
  leave the two on different defaults.

There is no runtime detection of a mismatch — the broker accepts whatever
partition the producer chose. The only signal is records for the same key
appearing in more than one partition.

## Phase 2 — pluggable `Partitioner`

Phase 2 translates Java's pluggable partitioner (`Partitioner`,
`RoundRobinPartitioner`) and routes the producer send path through a configured
partitioner, keeping the Phase 1 CRC-32 default and the two built-in hash names
unchanged. It is purely **additive**: a producer with no partitioner configured
behaves exactly as it did after Phase 1.

### The `Partitioner<K, V>` trait

`src/producer/partitioner.rs` translates
`org.apache.kafka.clients.producer.Partitioner`
(`interface Partitioner extends Configurable, Closeable`):

```rust
pub trait Partitioner<K, V>: Send + Sync {
    fn configure(&mut self, _configs: &HashMap<String, String>) {}
    fn partition(
        &self,
        topic: &str,
        key: Option<&K>,
        key_bytes: Option<&[u8]>,
        value: Option<&V>,
        value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32;
    fn close(&self) {}
}
```

Deliberate deviations from the Java interface (recorded in full in the trait's
own rustdoc under "Translation notes", `src/producer/partitioner.rs`):

- **`K, V` generics** replace Java's erased `Object key` / `Object value`. The
  Rust producer is generic, so the partitioner receives typed borrows and never
  downcasts.
- **`partition(&self)` / `close(&self)`, not `&mut self`.** Java's
  `KafkaProducer` is thread-safe and calls the *one* shared partitioner instance
  concurrently, so Java implementations are already internally synchronized —
  `RoundRobinPartitioner` uses a `ConcurrentMap` + `AtomicInteger`. `&self` is
  the faithful translation and keeps the send path lock-free: any per-record
  state is the implementer's job via interior mutability (atomics / a concurrent
  map), exactly as in Java. `close(&self)` also matches `Producer::close(&self)`,
  so the partitioner is closable through the shared reference without wrapping it
  in a `Mutex`.
- **`configure(&mut self)`** with a default no-op mirrors the
  `ConsumerInterceptor::configure` precedent; it is called exactly once, before
  the instance is boxed and shared, so `&mut self` is available then.
- Java's `Plugin<Partitioner>` / `Monitorable` metrics wrapper has **no Rust
  counterpart** and is not translated (KIP-877 metrics plumbing, out of scope).

`RoundRobinPartitioner` (`src/producer/round_robin_partitioner.rs`) is the one
built-in translated: it round-robins across the available partitions of a topic,
keeping a per-topic `AtomicI32` counter, and — like Java — skips to the next
partition when the chosen one has no leader.

### Configuring a partitioner: instance vs. `partitioner.class`

Rust has **no reflection**, so a class *name* can only select a built-in the
crate already knows. Two resolution paths exist, mirroring how Java resolves
`partitioner.class` reflectively:

- **By name (`partitioner.class`)** — `ProducerConfig::resolve_partitioner`
  maps the configured name to a built-in instance. Only `RoundRobinPartitioner`
  resolves to a distinct `Partitioner` object; the two hash names
  (`ConsistentRandomPartitioner`, `Murmur2RandomPartitioner`) and the unset
  default resolve to `None`, meaning "use the built-in key-hash / sticky path"
  (Phase 1). Accepted `RoundRobinPartitioner` spellings are the simple name
  `RoundRobinPartitioner` **and** the Java fully-qualified name
  `org.apache.kafka.clients.producer.RoundRobinPartitioner`. Any *other*
  non-built-in value is already rejected at config-construction time with Java's
  exact `ConfigException` text (Phase 1), so no unknown class name reaches
  `resolve_partitioner`.
- **By instance (`KafkaProducer::from_config_with_partitioner`)** — because a
  user-written partitioner cannot be named reflectively, the caller supplies it
  as a constructed `Box<dyn Partitioner<K, V>>`. An explicit instance **takes
  precedence** over any built-in that `partitioner.class` would otherwise name,
  mirroring Java, where an explicitly passed `Partitioner` wins over the config.

There is intentionally **no custom-partitioner SPI** (registering a
user-written partitioner by class name across the FFI / Python boundary) in
Phase 2 — that is out of scope. A Rust user supplies an instance; a built-in is
reachable by name from any binding (see the FFI/Python note below).

### Send-path behavior

- **`configure` sees the producer originals plus `client.id`.** When a
  partitioner is present (resolved from `partitioner.class` or passed as an
  instance), the constructor configures it exactly once with the producer's
  original configuration map plus the resolved (possibly auto-generated)
  `client.id`, matching Java's `partitioner.configure(...)`.
- **Adaptive partitioning is disabled while a custom partitioner is in use**
  (`enable_adaptive_partitioning = partitioner.is_none() &&
  config.partitioner_adaptive_partitioning_enable`), matching Java's
  `PartitionerConfig` gating: a caller-supplied partition decision must not be
  second-guessed by the built-in adaptive logic.
- **The partitioner is consulted exactly once per record.** On the borrowed
  zero-copy send path the typed `key` / `value` arguments are `None` even when
  `key_bytes` / `value_bytes` are present, because on that path Java's
  `record.key()` *is* the same `byte[]` as `keyBytes`; materializing a typed
  `&Vec<u8>` from the borrowed bytes would allocate and violate the zero-copy
  contract (CLAUDE.md §12). Partitioners that need the key/value read the
  `*_bytes` parameters, which are always supplied when a key/value exists. This
  deviation is recorded on the `partition` method's rustdoc in
  `src/producer/partitioner.rs`.
- **A negative partition is rejected** with Java's exact
  `IllegalArgumentException` text ("The partitioner generated an invalid
  partition number: N. Partition number should always be non-negative."),
  returned as `Err` and propagated out of `do_send` — Java rethrows the same out
  of `doSend`'s `catch (Exception e)`.
- **Close order.** `close()` closes the partitioner after `producer_metrics` and
  `metrics`, mirroring Java's `Utils.closeQuietly(...)` chain
  (`KafkaProducer.java:1441-1446`, which closes the partitioner at `:1446` after
  metrics). Behaviourally irrelevant for the built-ins, whose `close` is a no-op,
  and no producer construction path closes a partitioner mid-build (KAFKA-2121
  holds trivially — every fallible step precedes the infallible `configure`).

### `MockProducer` — and the empty-cluster deviation

`MockProducer` gains the same optional partitioner plus key/value serializers.
Its inherent `partition()` faithfully translates
`MockProducer.partition(ProducerRecord, Cluster)`
(`MockProducer.java:598-616`): an explicit record partition is range-validated
against the topic's partition count and returned as-is (rejecting an
out-of-range value with Java's exact `IllegalArgumentException`); otherwise the
key/value are serialized (so a serializer mismatch surfaces as Java's
`ClassCastException` would) and the partition is chosen by the partitioner, or
by the topic's first partition when none is configured.

**Deviation (empty cluster).** In `MockProducer::send`, when the cluster has
metadata for the record's topic, the send routes through `partition()` above
(Java 309-316). In the empty-cluster `else` branch Java serializes purely for
the `ClassCastException` side effect (Java's own comment at `:313`) and uses
partition `0`. The Rust translation still consults the serializers when present,
but honours an explicit `record.partition()` before falling back to `0`, so the
C FFI empty-cluster mock (`bindings/c/tests/test_mock_producer.c`) keeps
working. This is a DoD §7 justified deviation, recorded in the
`src/producer/mock_producer.rs` file header and inline at the branch.

### Bindings: built-in partitioners reach FFI / Python for free

Because `resolve_partitioner` runs *inside* config construction
(`ProducerConfig::from_properties` → `from_config`), any client built from a
property map — including the C FFI and the Python binding, which both construct
producers from properties — automatically gets a `RoundRobinPartitioner` when
`partitioner.class=RoundRobinPartitioner` is set, and the CRC-32 / murmur2 hash
selection for the two hash names. **No new FFI SPI is required** for the
built-in partitioners; only a user-*written* custom partitioner would need a
cross-language registration mechanism, which remains out of scope.

## Proposed CLAUDE.md addition

Automated agents must not edit `CLAUDE.md` directly (changes go through the
process in `.claude/rules/agent-roles.md`). The following is proposed for a
maintainer to fold into `CLAUDE.md`'s translation rules, so the deviation is
recorded alongside the other Java-parity rules:

> **Default key partitioner deviates from Java by design.** The producer's
> default keyed-record hash is IEEE CRC-32 (`KeyHasher::Crc32`, matching
> librdkafka `consistent_random`), **not** the Java client's murmur2. This is a
> deliberate, approved deviation from "match the Java client exactly" so the
> Rust client co-partitions with the librdkafka-based fleet. murmur2 (exact
> Java parity) remains available via `partitioner.class=Murmur2RandomPartitioner`
> (`ConsistentRandomPartitioner` selects the CRC-32 default). The keyless
> (sticky, KIP-794) path is unchanged. See `design/current/partitioner.md`.
> When reviewing the partitioner, do **not** flag the CRC-32 default as a
> Java-parity defect.
