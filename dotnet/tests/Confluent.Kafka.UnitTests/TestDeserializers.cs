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

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Test-only <see cref="IDeserializer{T}"/> instruments for the M6/P1b typed-poll tests:
/// they record whether / how the marshaller invoked them (invocation count, the span length
/// they were handed, the thread they ran on) so the three-state null model, the
/// <see cref="SerializationException"/> wrap, and the deserialize-thread can be asserted.
/// Reads happen after an <c>await</c> (or on the same thread for the sync path), so plain
/// fields suffice — the completion barrier publishes the dispatcher-thread writes.
/// </summary>
internal sealed class RecordingDeserializer<T> : IDeserializer<T>
{
    private readonly IDeserializer<T> _inner;

    internal RecordingDeserializer(IDeserializer<T> inner) => _inner = inner;

    /// <summary>How many times <see cref="Deserialize"/> was invoked (0 proves "not called").</summary>
    internal int InvocationCount { get; private set; }

    /// <summary>The length of the last span handed in (0 = present-but-empty), or -1 if never called.</summary>
    internal int LastSpanLength { get; private set; } = -1;

    /// <summary>The managed thread id of the last invocation (caller thread sync; dispatcher async).</summary>
    internal int LastThreadId { get; private set; }

    public T Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        LastSpanLength = data.Length;
        LastThreadId = Environment.CurrentManagedThreadId;
        return _inner.Deserialize(topic, data);
    }
}

/// <summary>
/// A deserializer that always throws (after counting) — proves an absent field does NOT
/// invoke the deserializer (PLAN decision C) and drives the mandatory
/// <see cref="SerializationException"/> wrap (PLAN §6).
/// </summary>
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

    /// <summary>How many times <see cref="Deserialize"/> was invoked (0 proves "not called").</summary>
    internal int InvocationCount { get; private set; }

    public T Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        throw new BoomException();
    }
}

/// <summary>
/// An <see cref="IDeserializer{T}"/> of <see cref="int"/> that returns the span length and
/// allocates nothing — used to prove the receive path is zero-copy (a large value decodes to
/// a tiny <c>int</c> with no value-sized intermediate <c>byte[]</c>) and to observe the
/// present-vs-empty span length.
/// </summary>
internal sealed class SpanLengthDeserializer : IDeserializer<int>
{
    /// <summary>How many times <see cref="Deserialize"/> was invoked.</summary>
    internal int InvocationCount { get; private set; }

    /// <summary>The length of the last span handed in, or -1 if never called.</summary>
    internal int LastSpanLength { get; private set; } = -1;

    public int Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        LastSpanLength = data.Length;
        return data.Length;
    }
}

/// <summary>
/// An <see cref="IDeserializer{T}"/> of <see cref="long"/>? over <see cref="Serdes.Int64"/>
/// — makes a tombstone (absent value → <see langword="null"/>, deserializer not invoked)
/// distinguishable from a present <c>0L</c> (PLAN decision C, the value-type guidance).
/// </summary>
internal sealed class NullableInt64Deserializer : IDeserializer<long?>
{
    /// <summary>How many times <see cref="Deserialize"/> was invoked (0 for an absent value).</summary>
    internal int InvocationCount { get; private set; }

    public long? Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        InvocationCount++;
        return Serdes.Int64.Deserialize(topic, data);
    }
}
