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
/// The Rust core's default key partitioner (librdkafka's <c>consistent_random</c>, not Java's murmur2), as Python's
/// <c>partitioner.py</c>: the unsigned IEEE CRC-32 of the key modulo the partition count, with no sign masking.
/// </summary>
public static class Crc32
{
    public static uint Hash(ReadOnlySpan<byte> data)
    {
        return System.IO.Hashing.Crc32.HashToUInt32(data);
    }

    public static int PartitionForKey(ReadOnlySpan<byte> key, int numPartitions)
    {
        return (int)(Hash(key) % (uint)numPartitions);
    }
}
