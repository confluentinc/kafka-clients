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
/// Wraps a serializer and records its calls: how many, whether the last datum was <see langword="null"/>, and the
/// thread of the last one.
/// </summary>
internal sealed class RecordingSerializer<T> : ISerializer<T>
{
    private readonly ISerializer<T> _inner;

    internal RecordingSerializer(ISerializer<T> inner) => _inner = inner;

    internal int InvocationCount { get; private set; }

    internal bool LastDatumWasNull { get; private set; }

    internal int LastThreadId { get; private set; }

    public byte[]? Serialize(string topic, T data)
    {
        InvocationCount++;
        LastDatumWasNull = data is null;
        LastThreadId = Environment.CurrentManagedThreadId;
        return _inner.Serialize(topic, data);
    }
}

/// <summary>A serializer that counts its calls and always throws a <see cref="BoomException"/>.</summary>
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

    internal int InvocationCount { get; private set; }

    public byte[]? Serialize(string topic, T data)
    {
        InvocationCount++;
        throw new BoomException();
    }
}

/// <summary>
/// Counts its calls over <see cref="Serdes.Int64"/>, and serializes <see langword="null"/> to <see langword="null"/>
/// bytes, so that a tombstone differs from a present <c>0</c>.
/// </summary>
internal sealed class NullableInt64Serializer : ISerializer<long?>
{
    internal int InvocationCount { get; private set; }

    public byte[]? Serialize(string topic, long? data)
    {
        InvocationCount++;
        return data is null ? null : Serdes.Int64.Serialize(topic, data.Value);
    }
}
