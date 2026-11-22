// Copyright 2026 Confluent Inc.
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
using System.Numerics;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Performance;

/// <summary>
/// What a consumer backend does with each polled record under <c>DO_VERIFY=True</c>, the same for every client: it
/// reads every byte of the key and of the value, counting the bits set, and checks that the partition is not
/// negative. The count makes the read observable and comparable between clients fed the same records. It covers
/// every record polled, the warmup's included, and a backend calls it from one poll at a time.
/// </summary>
public sealed class PolledRecordVerifier
{
    /// <summary>The records verified.</summary>
    public long Records { get; private set; }

    /// <summary>The bits set in the keys and values of the records verified.</summary>
    public long Ones { get; private set; }

    /// <summary>The records verified whose partition was negative.</summary>
    public long Failed { get; private set; }

    /// <summary>The bits set in <paramref name="data"/>, reading each of its bytes once.</summary>
    public static long CountOnes(ReadOnlySpan<byte> data)
    {
        ReadOnlySpan<ulong> words = MemoryMarshal.Cast<byte, ulong>(data);
        long ones = 0;
        foreach (ulong word in words)
        {
            ones += BitOperations.PopCount(word);
        }

        foreach (byte rest in data.Slice(words.Length * sizeof(ulong)))
        {
            ones += BitOperations.PopCount(rest);
        }

        return ones;
    }

    /// <summary>Verifies one polled record; a null key or value is passed as an empty span.</summary>
    public void Verify(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Records++;
        Ones += CountOnes(key) + CountOnes(value);
        if (partition < 0)
        {
            Failed++;
        }
    }
}
