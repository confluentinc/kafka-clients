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
/// Checks the <see cref="Crc32"/> port against the Rust core's <c>test_crc32_golden_vectors</c> and
/// <c>test_crc32_key_to_partition_table</c> vectors, which Python's partitioner self-check also carries; no broker.
/// </summary>
public sealed class Crc32Test
{
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

    // Unsigned modulo with no to_positive masking: that step belongs only to Java's murmur2 formula.
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

    // The core treats an empty key as unhashed (sticky partitioner), so this only checks the helper stays in range.
    [Fact]
    public void PartitionForKey_EmptyKey_IsInRangeButNotProductionMeaningful()
    {
        int actual = Crc32.PartitionForKey(System.Array.Empty<byte>(), 4);
        Assert.InRange(actual, 0, 3);
    }
}
