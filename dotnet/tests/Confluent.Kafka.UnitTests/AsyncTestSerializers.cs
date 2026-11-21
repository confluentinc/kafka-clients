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
using System.Threading.Tasks;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Gives a test's synchronous serializer to an asynchronous producer: each of its forms calls the same form of the
/// serializer, the headers form calling the form without headers when the serializer has no headers form. It returns
/// a completed <see cref="ValueTask{TResult}"/>, throws what the serializer throws from the call itself, and disposes
/// a disposable one.
/// </summary>
internal sealed class AsyncSerializerOver<T> : IAsyncSerializerWithHeaders<T>, IAsyncDisposable
{
    private readonly ISerializer<T> _inner;
    private readonly ISerializerWithHeaders<T>? _withHeaders;

    internal AsyncSerializerOver(ISerializer<T> inner)
    {
        _inner = inner;
        _withHeaders = inner as ISerializerWithHeaders<T>;
    }

    public ValueTask<ReadOnlyMemory<byte>?> Serialize(string topic, T? data, CancellationToken cancellationToken) =>
        new ValueTask<ReadOnlyMemory<byte>?>(_inner.Serialize(topic, data));

    public ValueTask<ReadOnlyMemory<byte>?> Serialize(
        string topic, IHeaders headers, T? data, CancellationToken cancellationToken) =>
        new ValueTask<ReadOnlyMemory<byte>?>(_withHeaders is null
            ? _inner.Serialize(topic, data)
            : _withHeaders.Serialize(topic, headers, data));

    // The one interface an asynchronous client disposes a serde through; the inner one's Dispose() does not wait.
    public ValueTask DisposeAsync()
    {
        (_inner as IDisposable)?.Dispose();
        return default;
    }

    // The producer's log names a serde by this, so it names the test's serializer.
    public override string ToString() => _inner.ToString() ?? string.Empty;
}

/// <summary>The asynchronous forms of a test's serializers.</summary>
internal static class AsyncTestSerializers
{
    // A null stays null, for the tests that give a producer no serializer.
    internal static AsyncSerializerOver<T> AsAsync<T>(this ISerializer<T>? serializer) =>
        serializer is null ? null! : new AsyncSerializerOver<T>(serializer);
}
