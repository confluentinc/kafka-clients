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
/// Self-check for the <see cref="Murmur2"/> port against the exact
/// <c>org.apache.kafka.common.utils.UtilsTest.testMurmur2</c> vectors (also carried by
/// <c>bindings/python/test/performance/partitioner.py</c>). Java's <c>murmur2</c> returns a signed
/// <c>int</c>; the port returns a <see cref="uint"/>, so each expected value is compared as its
/// unsigned-32 equivalent. This is the Slice-1 automated gate — it needs no broker and no client.
/// </summary>
public sealed class Murmur2Test
{
    // The UtilsTest.testMurmur2 cases: input string -> Java signed-int hash.
    [Theory]
    [InlineData("21", -973932308)]
    [InlineData("foobar", -790332482)]
    [InlineData("a-little-bit-long-string", -985981536)]
    [InlineData("a-little-bit-longer-string", -1486304829)]
    [InlineData("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971)]
    [InlineData("abc", 479470107)]
    public void Hash_MatchesJavaUtilsTestVectors(string input, int expectedSigned)
    {
        byte[] data = Encoding.UTF8.GetBytes(input);
        uint expected = unchecked((uint)expectedSigned);

        uint actual = Murmur2.Hash(data);

        Assert.Equal(expected, actual);
    }

    // BuiltInPartitioner.partitionForKey = toPositive(murmur2(key)) % numPartitions, where
    // toPositive is (n & 0x7fffffff). Verify the composed helper for a few partition counts.
    [Theory]
    [InlineData("21", 4)]
    [InlineData("foobar", 8)]
    [InlineData("abc", 3)]
    [InlineData("a-little-bit-longer-string", 16)]
    public void PartitionForKey_MatchesToPositiveModulo(string input, int numPartitions)
    {
        byte[] key = Encoding.UTF8.GetBytes(input);
        int expected = (int)(Murmur2.Hash(key) & 0x7FFFFFFF) % numPartitions;

        int actual = Murmur2.PartitionForKey(key, numPartitions);

        Assert.Equal(expected, actual);
        Assert.InRange(actual, 0, numPartitions - 1);
    }

    // The empty key is a valid input (murmur2 seeds with the length); ensure it does not throw and
    // yields a stable, in-range partition.
    [Fact]
    public void PartitionForKey_EmptyKey_IsInRange()
    {
        int actual = Murmur2.PartitionForKey(System.Array.Empty<byte>(), 4);
        Assert.InRange(actual, 0, 3);
    }
}
