// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.IO.Hashing;

namespace Confluent.Kafka.Performance;

/// <summary>
/// Port of the Rust core's default key-partitioner hash — the C# analog of
/// <c>bindings/python/test/performance/partitioner.py</c>. Mirrors
/// <c>BuiltInPartitioner::partition_for_key</c> under <c>KeyHasher::Crc32</c>
/// (<c>src/producer/internals/built_in_partitioner.rs</c>): IEEE 802.3 / zlib CRC-32
/// (<c>crc32fast::hash</c>, == librdkafka's <c>rd_crc32</c>, == Python's <c>zlib.crc32</c>),
/// taken <b>unsigned</b> with no sign-bit masking, so the perf suite can verify that a
/// keyed record landed in the partition the client's actual default partitioner would
/// have chosen.
///
/// This is a <b>deliberate deviation from the Apache Kafka Java client</b>, whose default
/// partitioner hashes keys with murmur2 instead (<c>Utils.toPositive(Utils.murmur2(key))
/// % numPartitions</c>) — see <c>design/current/partitioner.md</c> for the rationale. The
/// Rust core's C ABI exposes no way to select a non-default partitioner, so the v3
/// producer under test here always uses CRC-32; there is nothing to make this an opt-in.
/// Cross-checked against the golden vectors in
/// <c>built_in_partitioner.rs::test_crc32_golden_vectors</c> /
/// <c>test_crc32_key_to_partition_table</c> in the xUnit self-check.
/// </summary>
public static class Crc32
{
    /// <summary>
    /// 32-bit IEEE CRC-32, returned as a <see cref="uint"/> — byte-for-byte equivalent to
    /// Rust's <c>crc32fast::hash</c> / Python's <c>zlib.crc32</c>.
    /// </summary>
    public static uint Hash(ReadOnlySpan<byte> data)
    {
        return System.IO.Hashing.Crc32.HashToUInt32(data);
    }

    /// <summary>
    /// Computes the partition the Rust client's default (CRC-32) partitioner assigns to
    /// <paramref name="key"/>: <c>(crc32(key) % numPartitions)</c>, unsigned — <b>no</b>
    /// <c>to_positive</c> / <c>&amp; 0x7fffffff</c> masking (librdkafka's <c>consistent_random</c>
    /// does not mask either; that step belongs only to the murmur2/Java formula).
    /// </summary>
    /// <remarks>
    /// Production never calls this for a <i>present-but-empty</i> key — Rust's
    /// <c>KeyHasher::Crc32.hashes_key(b"")</c> is <c>false</c>, so an empty key falls
    /// through to the sticky partitioner instead of being hashed. The perf harness never
    /// produces that input either (its message generator treats <c>KEY_SIZE=0</c> as "no
    /// key," never a zero-length one), so this method's behavior on an empty span is a
    /// pure math sanity check, not a production-parity claim.
    /// </remarks>
    public static int PartitionForKey(ReadOnlySpan<byte> key, int numPartitions)
    {
        return (int)(Hash(key) % (uint)numPartitions);
    }
}
