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
/// Test-only <see cref="ISerializer{T}"/> instruments for the M11/P5 typed-send tests — the
/// send-side mirror of <see cref="RecordingDeserializer{T}"/> / <see cref="ThrowingDeserializer{T}"/>.
/// They record whether / how the serialize skin invoked them (invocation count, whether the datum
/// was null, the thread) so the <b>invoke-on-null</b> (Java-faithful, PLAN §3.4) behavior and the
/// synchronous <see cref="SerializationException"/> wrap (PLAN §3.5) can be asserted. Serialize runs
/// on the caller's thread (pre-native, both sync and async), so plain fields suffice.
/// </summary>
internal sealed class RecordingSerializer<T> : ISerializer<T>
{
    private readonly ISerializer<T> _inner;

    internal RecordingSerializer(ISerializer<T> inner) => _inner = inner;

    /// <summary>How many times <see cref="Serialize"/> was invoked (1 on a null datum proves invoke-on-null).</summary>
    internal int InvocationCount { get; private set; }

    /// <summary>Whether the last datum handed in was <see langword="null"/>.</summary>
    internal bool LastDatumWasNull { get; private set; }

    /// <summary>The managed thread id of the last invocation (the caller's thread — never a pump/dispatcher thread).</summary>
    internal int LastThreadId { get; private set; }

    public byte[]? Serialize(string topic, T data)
    {
        InvocationCount++;
        LastDatumWasNull = data is null;
        LastThreadId = Environment.CurrentManagedThreadId;
        return _inner.Serialize(topic, data);
    }
}

/// <summary>
/// A serializer that always throws (after counting) — drives the mandatory
/// <see cref="SerializationException"/> wrap (PLAN §3.5) and proves the throw is <b>synchronous</b>
/// (before any native send / pump enqueue) for both the sync and async <c>Send</c>.
/// </summary>
internal sealed class ThrowingSerializer<T> : ISerializer<T>
{
    /// <summary>The distinctive exception this serializer throws, asserted as the inner exception.</summary>
    internal sealed class BoomException : Exception
    {
        internal BoomException()
            : base("boom-from-serializer")
        {
        }
    }

    /// <summary>How many times <see cref="Serialize"/> was invoked.</summary>
    internal int InvocationCount { get; private set; }

    public byte[]? Serialize(string topic, T data)
    {
        InvocationCount++;
        throw new BoomException();
    }
}

/// <summary>
/// An <see cref="ISerializer{T}"/> of <see cref="long"/>? over <see cref="Serdes.Int64"/> — makes a
/// value-type tombstone (a <see langword="null"/> <c>long?</c> → <c>null</c> bytes → absent)
/// distinguishable from a present <c>0L</c> (8 bytes → present), the value-type guidance in
/// PLAN §3.4. Always invoked (invoke-on-null); a <see langword="null"/> input serializes to
/// <see langword="null"/> bytes.
/// </summary>
internal sealed class NullableInt64Serializer : ISerializer<long?>
{
    /// <summary>How many times <see cref="Serialize"/> was invoked (1 even for a null → invoke-on-null).</summary>
    internal int InvocationCount { get; private set; }

    public byte[]? Serialize(string topic, long? data)
    {
        InvocationCount++;
        return data is null ? null : Serdes.Int64.Serialize(topic, data.Value);
    }
}
