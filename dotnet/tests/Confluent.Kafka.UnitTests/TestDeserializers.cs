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
using System.Threading;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Wraps a deserializer and records its calls: how many, the length of the last span, and the thread of the last one.
/// </summary>
internal sealed class RecordingDeserializer<T> : IDeserializer<T>
{
    private readonly IDeserializer<T> _inner;

    internal RecordingDeserializer(IDeserializer<T> inner) => _inner = inner;

    internal int InvocationCount { get; private set; }

    internal int LastSpanLength { get; private set; } = -1;

    internal int LastThreadId { get; private set; }

    internal string? LastThreadName { get; private set; }

    public T Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        LastSpanLength = data.Length;
        LastThreadId = Environment.CurrentManagedThreadId;
        LastThreadName = Thread.CurrentThread.Name;
        return _inner.Deserialize(topic, data);
    }
}

/// <summary>A deserializer that counts its calls and always throws a <see cref="BoomException"/>.</summary>
internal sealed class ThrowingDeserializer<T> : IDeserializer<T>
{
    /// <summary>The distinctive exception this deserializer throws, asserted as the inner exception.</summary>
    internal sealed class BoomException : Exception
    {
        internal BoomException()
            : base("boom-from-deserializer")
        {
        }
    }

    internal int InvocationCount { get; private set; }

    public T Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        throw new BoomException();
    }
}

/// <summary>
/// Returns the span's length without allocating, and records its calls and the last span's length.
/// </summary>
internal sealed class SpanLengthDeserializer : IDeserializer<int>
{
    internal int InvocationCount { get; private set; }

    internal int LastSpanLength { get; private set; } = -1;

    public int Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        LastSpanLength = data.Length;
        return data.Length;
    }
}

/// <summary>
/// Counts its calls over <see cref="Serdes.Int64"/>, so that a tombstone, <see langword="null"/>, differs from a
/// present <c>0</c>.
/// </summary>
internal sealed class NullableInt64Deserializer : IDeserializer<long?>
{
    internal int InvocationCount { get; private set; }

    public long? Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        return Serdes.Int64.Deserialize(topic, data);
    }
}
