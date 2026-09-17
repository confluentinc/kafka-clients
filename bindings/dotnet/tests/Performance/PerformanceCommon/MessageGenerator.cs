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

/// <summary>One pre-generated key/value pair. <see cref="Key"/> is <see langword="null"/> when the key size is 0.</summary>
public readonly struct PerfMessage
{
    internal PerfMessage(byte[]? key, byte[] value)
    {
        Key = key;
        Value = value;
    }

    /// <summary>The record key, or <see langword="null"/> when the configured key size is 0 (no key).</summary>
    public byte[]? Key { get; }

    /// <summary>The record value (never null; a constant prefix plus a per-message random suffix).</summary>
    public byte[] Value { get; }
}

/// <summary>
/// Pre-generates the cycled message set — the C# analog of <c>message_generator</c> in
/// <c>producer_performance_test.py</c>. Each message is a constant prefix plus a
/// <see cref="Randomness"/>-fraction random suffix; <c>10000</c> messages are generated once and cycled
/// round-robin by the send loop (default value size 2048 B, default key size 0 = no key).
/// </summary>
public static class MessageGenerator
{
    /// <summary>Fraction of each key/value that is a per-message random suffix (the rest is a shared constant prefix).</summary>
    public const double Randomness = 0.5;

    /// <summary>Number of distinct messages pre-generated and then cycled round-robin.</summary>
    public const int DefaultCount = 10000;

    /// <summary>
    /// Generates <paramref name="count"/> messages of <paramref name="valueSize"/> value bytes and
    /// <paramref name="keySize"/> key bytes (0 = no key). Faithful to the Python generator: one constant
    /// prefix is reused across all messages, with a fresh <see cref="Randomness"/>-fraction random suffix
    /// per message.
    /// </summary>
    public static PerfMessage[] Generate(int keySize, int valueSize, int count = DefaultCount)
    {
        if (keySize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(keySize));
        }

        if (valueSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(valueSize));
        }

        var random = new Random();

        int valueRandBytes = (int)(valueSize * Randomness);
        byte[] valueConstant = new byte[valueSize - valueRandBytes];
        random.NextBytes(valueConstant);

        byte[]? keyConstant = null;
        int keyRandBytes = 0;
        if (keySize > 0)
        {
            keyRandBytes = (int)(keySize * Randomness);
            keyConstant = new byte[keySize - keyRandBytes];
            random.NextBytes(keyConstant);
        }

        var messages = new PerfMessage[count];
        for (int i = 0; i < count; i++)
        {
            byte[]? key = null;
            if (keySize > 0)
            {
                key = Concat(keyConstant!, NextBytes(random, keyRandBytes));
            }

            byte[] value = Concat(valueConstant, NextBytes(random, valueRandBytes));
            messages[i] = new PerfMessage(key, value);
        }

        return messages;
    }

    private static byte[] NextBytes(Random random, int length)
    {
        byte[] buffer = new byte[length];
        if (length > 0)
        {
            random.NextBytes(buffer);
        }

        return buffer;
    }

    private static byte[] Concat(byte[] prefix, byte[] suffix)
    {
        byte[] result = new byte[prefix.Length + suffix.Length];
        Array.Copy(prefix, 0, result, 0, prefix.Length);
        Array.Copy(suffix, 0, result, prefix.Length, suffix.Length);
        return result;
    }
}
