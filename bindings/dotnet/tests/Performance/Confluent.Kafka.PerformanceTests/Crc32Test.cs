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

using System.Text;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// Self-check for the <see cref="Crc32"/> port against the exact
/// <c>built_in_partitioner.rs::test_crc32_golden_vectors</c> /
/// <c>test_crc32_key_to_partition_table</c> vectors (also carried by
/// <c>bindings/python/test/performance/partitioner.py</c>'s <c>__main__</c> self-check). This is the
/// Slice-1 automated gate — it needs no broker and no client, and stays a plain
/// <see cref="FactAttribute"/>/<see cref="TheoryAttribute"/> class (not <c>[SkippableFact]</c> — M13/P3
/// D-8 kept this class un-skippable since it needs no broker; the same applies here).
/// </summary>
public sealed class Crc32Test
{
    // built_in_partitioner.rs::test_crc32_golden_vectors, cross-checked against zlib.crc32 /
    // crc32fast::hash. Returned unsigned — no Java-style sign-bit masking anywhere in this path.
    [Theory]
    [InlineData("", 0x00000000u)]
    [InlineData("a", 0xE8B7BE43u)]
    [InlineData("abc", 0x352441C2u)]
    [InlineData("123456789", 0xCBF43926u)]
    [InlineData("The quick brown fox jumps over the lazy dog", 0x414FA339u)]
    public void Hash_MatchesGoldenVectors(string input, uint expected)
    {
        byte[] data = Encoding.UTF8.GetBytes(input);

        uint actual = Crc32.Hash(data);

        Assert.Equal(expected, actual);
    }

    // built_in_partitioner.rs::test_crc32_key_to_partition_table: partition_for_key =
    // crc32(key) % numPartitions, unsigned, no to_positive masking (that step belongs only to the
    // murmur2/Java formula — deliberately NOT applied here, see design/current/partitioner.md).
    [Theory]
    [InlineData("a", 3, 0)]
    [InlineData("abc", 7, 5)]
    [InlineData("kafka", 12, 11)]
    [InlineData("hello", 64, 6)]
    [InlineData("123456789", 64, 38)]
    public void PartitionForKey_MatchesGoldenTable(string input, int numPartitions, int expected)
    {
        byte[] key = Encoding.UTF8.GetBytes(input);

        int actual = Crc32.PartitionForKey(key, numPartitions);

        Assert.Equal(expected, actual);
    }

    // Production never routes a present-but-empty key through this formula (KeyHasher::Crc32 treats an
    // empty key as unhashed, falling through to the sticky partitioner instead), and the perf harness
    // itself never generates that input either. This is therefore a pure math sanity check on the public
    // helper — it does not throw and stays in range — not a claim that it matches production behavior.
    [Fact]
    public void PartitionForKey_EmptyKey_IsInRangeButNotProductionMeaningful()
    {
        int actual = Crc32.PartitionForKey(System.Array.Empty<byte>(), 4);
        Assert.InRange(actual, 0, 3);
    }
}
