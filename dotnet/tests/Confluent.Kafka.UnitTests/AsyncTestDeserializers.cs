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
/// Gives a test's synchronous deserializer to an asynchronous consumer: it calls the headers form when the
/// deserializer has one, returns a completed <see cref="ValueTask{TResult}"/>, throws what the deserializer throws
/// from the call itself, and disposes a disposable one.
/// </summary>
internal sealed class AsyncDeserializerOver<T> : IAsyncDeserializer<T>, IAsyncDisposable
{
    private readonly IDeserializer<T> _inner;
    private readonly IDeserializerWithHeaders<T>? _withHeaders;

    internal AsyncDeserializerOver(IDeserializer<T> inner)
    {
        _inner = inner;
        _withHeaders = inner as IDeserializerWithHeaders<T>;
    }

    public ValueTask<T?> Deserialize(
        string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) =>
        new ValueTask<T?>(_withHeaders is null
            ? _inner.Deserialize(topic, data.Span)
            : _withHeaders.Deserialize(topic, headers, data.Span));

    // The one interface an asynchronous client disposes a serde through; the inner one's Dispose() does not wait.
    public ValueTask DisposeAsync()
    {
        (_inner as IDisposable)?.Dispose();
        return default;
    }

    // The consumer's trace names a serde by this, so it names the test's deserializer.
    public override string ToString() => _inner.ToString() ?? string.Empty;
}

/// <summary>The asynchronous forms of a test's deserializers.</summary>
internal static class AsyncTestDeserializers
{
    // A null stays null, for the tests that give a consumer no deserializer.
    internal static AsyncDeserializerOver<T> AsAsync<T>(this IDeserializer<T>? deserializer) =>
        deserializer is null ? null! : new AsyncDeserializerOver<T>(deserializer);
}
