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

## Phase 2 note (future work)

Phase 1 delivers only the two hashers behind `partitioner.class`
(`ConsistentRandomPartitioner` / `Murmur2RandomPartitioner`) and the CRC-32
default. It does **not** translate Java's pluggable `Partitioner` interface, the
`RoundRobinPartitioner`, or any custom-partitioner SPI. If a genuine need for a
user-supplied partitioner surfaces, a Phase 2 would introduce a `Partitioner`
trait and route `partition_for_key` through it — additive, and without changing
the Phase 1 default or the two built-in names.

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
