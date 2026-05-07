"""Pure-Python translation of the Kafka Java client default partitioner.

Mirrors:
  - org.apache.kafka.common.utils.Utils.murmur2
    (kafka/clients/src/main/java/org/apache/kafka/common/utils/Utils.java)
  - org.apache.kafka.clients.producer.internals.BuiltInPartitioner.partitionForKey
    (kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java)

Used by the Python performance test to verify that messages produced with a key
land in the partition the broker-side default partitioner would have chosen.
"""

_SEED = 0x9747B28C
_M = 0x5BD1E995
_R = 24
_MASK_32 = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """32-bit Murmur2 hash, returned as an unsigned int (0..2^32-1).

    Byte-for-byte equivalent to Java's Utils.murmur2: 4-byte chunks are read
    little-endian and all arithmetic is masked to 32 bits to mimic Java's
    signed-int overflow semantics.
    """
    length = len(data)
    h = (_SEED ^ length) & _MASK_32
    length4 = length >> 2

    for i in range(length4):
        i4 = i << 2
        k = int.from_bytes(data[i4:i4 + 4], "little", signed=False)
        k = (k * _M) & _MASK_32
        k ^= k >> _R
        k = (k * _M) & _MASK_32
        h = (h * _M) & _MASK_32
        h ^= k

    index = length4 << 2
    tail = length - index
    if tail >= 3:
        h ^= (data[index + 2] & 0xFF) << 16
    if tail >= 2:
        h ^= (data[index + 1] & 0xFF) << 8
    if tail >= 1:
        h ^= data[index] & 0xFF
        h = (h * _M) & _MASK_32

    h ^= (h & _MASK_32) >> 13
    h = (h * _M) & _MASK_32
    h ^= (h & _MASK_32) >> 15
    return h & _MASK_32


def partition_for_key(key: bytes, num_partitions: int) -> int:
    """Compute the partition the Java default partitioner would assign to `key`.

    Equivalent to BuiltInPartitioner.partitionForKey:
        Utils.toPositive(Utils.murmur2(key)) % numPartitions
    where toPositive is `n & 0x7fffffff`.
    """
    return (murmur2(key) & 0x7FFFFFFF) % num_partitions


if __name__ == "__main__":
    # Cross-checked against UtilsTest.testMurmur2
    # (kafka/clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java).
    # Java values are signed; we compare against their unsigned-32 equivalents.
    def _u32(signed: int) -> int:
        return signed & _MASK_32

    cases = {
        b"21": -973932308,
        b"foobar": -790332482,
        b"a-little-bit-long-string": -985981536,
        b"a-little-bit-longer-string": -1486304829,
        b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
        b"abc": 479470107,
    }
    for data, expected_signed in cases.items():
        got = murmur2(data)
        expected = _u32(expected_signed)
        assert got == expected, f"murmur2({data!r}) = {got}, want {expected}"
    print(f"murmur2 self-check passed ({len(cases)} vectors)")
