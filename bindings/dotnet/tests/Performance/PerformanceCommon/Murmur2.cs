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

namespace Confluent.Kafka.Performance;

/// <summary>
/// Port of the Kafka Java client default partitioner's hash — the C# analog of
/// <c>bindings/python/test/performance/partitioner.py</c>. Mirrors
/// <c>org.apache.kafka.common.utils.Utils.murmur2</c> and
/// <c>org.apache.kafka.clients.producer.internals.BuiltInPartitioner.partitionForKey</c>, so the perf
/// suite can verify that a keyed record landed in the partition the broker-side default partitioner
/// would have chosen. Cross-checked against <c>UtilsTest.testMurmur2</c> in the xUnit self-check.
/// </summary>
public static class Murmur2
{
    private const uint Seed = 0x9747B28C;
    private const uint M = 0x5BD1E995;
    private const int R = 24;

    /// <summary>
    /// 32-bit Murmur2 hash, returned as a <see cref="uint"/> — byte-for-byte equivalent to Java's
    /// <c>Utils.murmur2</c>: 4-byte chunks are read little-endian and all arithmetic is 32-bit
    /// (unchecked <see cref="uint"/> wraparound mimics Java's signed-int overflow).
    /// </summary>
    public static uint Hash(ReadOnlySpan<byte> data)
    {
        int length = data.Length;
        uint h = unchecked(Seed ^ (uint)length);
        int length4 = length >> 2;

        for (int i = 0; i < length4; i++)
        {
            int i4 = i << 2;
            uint k = (uint)(data[i4] & 0xFF)
                | ((uint)(data[i4 + 1] & 0xFF) << 8)
                | ((uint)(data[i4 + 2] & 0xFF) << 16)
                | ((uint)(data[i4 + 3] & 0xFF) << 24);
            unchecked
            {
                k *= M;
                k ^= k >> R;
                k *= M;
                h *= M;
                h ^= k;
            }
        }

        int index = length4 << 2;
        int tail = length - index;
        unchecked
        {
            if (tail >= 3)
            {
                h ^= (uint)(data[index + 2] & 0xFF) << 16;
            }

            if (tail >= 2)
            {
                h ^= (uint)(data[index + 1] & 0xFF) << 8;
            }

            if (tail >= 1)
            {
                h ^= (uint)(data[index] & 0xFF);
                h *= M;
            }

            h ^= h >> 13;
            h *= M;
            h ^= h >> 15;
        }

        return h;
    }

    /// <summary>
    /// Computes the partition the Java default partitioner would assign to <paramref name="key"/> —
    /// <c>BuiltInPartitioner.partitionForKey</c>: <c>toPositive(murmur2(key)) % numPartitions</c>, where
    /// <c>toPositive</c> is <c>n &amp; 0x7fffffff</c>.
    /// </summary>
    public static int PartitionForKey(ReadOnlySpan<byte> key, int numPartitions)
    {
        int positive = (int)(Hash(key) & 0x7FFFFFFF);
        return positive % numPartitions;
    }
}
