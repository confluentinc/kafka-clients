"""Pure-Python model of the Rust/librdkafka default key partitioner.

The Rust producer's default key hasher is IEEE CRC-32 (``KeyHasher::Crc32`` in
``src/producer/internals/built_in_partitioner.rs``), matching librdkafka's
``consistent_random`` partitioner (``rd_crc32(key) % partition_cnt``). This is
a deliberate deviation from the Apache Kafka Java client, whose default
partitioner hashes keys with murmur2
(``Utils.toPositive(Utils.murmur2(key)) % numPartitions``). The deviation lets
the Rust client co-partition keyed records with the librdkafka-based Confluent
client fleet — see ``design/current/partitioner.md`` for the rationale and the
mixed-fleet warning.

``zlib.crc32`` computes the identical IEEE CRC-32 (polynomial 0x04C11DB7,
reflected) as ``crc32fast::hash`` and librdkafka's ``rd_crc32``, returning it
as an unsigned 32-bit int — so ``zlib.crc32(key) % num_partitions`` matches the
Rust client exactly, with no ``toPositive`` sign-bit masking (see
``partition_for_key`` below).

Used by the Python performance test to verify that messages produced with a key
land in the partition the configured default partitioner would have chosen.
"""

import zlib


def partition_for_key(key: bytes, num_partitions: int) -> int:
    """Compute the partition the default (CRC-32) partitioner assigns to `key`.

    Mirrors ``BuiltInPartitioner::partition_for_key`` with ``KeyHasher::Crc32``:

        (crc32fast::hash(key) % (num_partitions as u32)) as i32

    i.e. the CRC is taken **unsigned** and reduced modulo the partition count.
    Unlike the Java murmur2 path there is no ``toPositive`` (``& 0x7fffffff``)
    step — ``zlib.crc32`` already returns an unsigned 32-bit value, exactly as
    librdkafka's ``consistent_random`` does.

    Empty keys never reach here in the perf test (a present key always has
    ``key_size > 0``); the Rust client's ``KeyHasher::Crc32`` skips hashing an
    empty key and defers to the sticky partitioner, matching librdkafka.
    """
    return zlib.crc32(key) % num_partitions


if __name__ == "__main__":
    # Hash vectors cross-checked against
    # built_in_partitioner.rs::test_crc32_golden_vectors
    # (src/producer/internals/built_in_partitioner.rs) and zlib.crc32.
    golden = {
        b"": 0x00000000,
        b"a": 0xE8B7BE43,
        b"abc": 0x352441C2,
        b"123456789": 0xCBF43926,
        b"The quick brown fox jumps over the lazy dog": 0x414FA339,
    }
    for data, expected in golden.items():
        got = zlib.crc32(data)
        assert got == expected, \
            f"crc32({data!r}) = 0x{got:08X}, want 0x{expected:08X}"

    # Key -> partition placements cross-checked against
    # built_in_partitioner.rs::test_crc32_key_to_partition_table.
    partition_cases = [
        (b"a", 3, 0),
        (b"abc", 7, 5),
        (b"kafka", 12, 11),
        (b"hello", 64, 6),
        (b"123456789", 64, 38),
    ]
    for key, num_partitions, expected_p in partition_cases:
        got_p = partition_for_key(key, num_partitions)
        assert got_p == expected_p, \
            f"partition_for_key({key!r}, {num_partitions}) = {got_p}, want {expected_p}"

    print(f"crc32 self-check passed "
          f"({len(golden)} hash vectors, {len(partition_cases)} partition cases)")
