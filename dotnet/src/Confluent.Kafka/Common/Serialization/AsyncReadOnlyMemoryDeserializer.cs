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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// <see cref="ReadOnlyMemoryDeserializer"/> for an asynchronous consumer: it calls that class, so the value is the
/// same. It never waits: the <see cref="ValueTask{TResult}"/> it returns is already completed, the token is not read,
/// and what <see cref="ReadOnlyMemoryDeserializer"/> throws is thrown by the call itself. Its headers form, which a
/// consumer calls, returns the memory as given, without a copy, which <see cref="IAsyncDeserializerWithRetention{T}"/>
/// lets it do; its form without headers copies the bytes, as the span form of that class does.
/// </summary>
public sealed class AsyncReadOnlyMemoryDeserializer
    : IAsyncDeserializerWithRetention<ReadOnlyMemory<byte>?>, IAsyncBuiltInDeserializer<ReadOnlyMemory<byte>?>
{
    private readonly ReadOnlyMemoryDeserializer _inner = new ReadOnlyMemoryDeserializer();

    IDeserializer<ReadOnlyMemory<byte>?> IAsyncBuiltInDeserializer<ReadOnlyMemory<byte>?>.Inner => _inner;

    /// <summary>
    /// Decodes <paramref name="data"/> as <see cref="ReadOnlyMemoryDeserializer"/>'s form without headers does, by
    /// calling it: the value is a copy of the bytes. A consumer calls the headers form of this class, never this one.
    /// </summary>
    /// <inheritdoc cref="IAsyncDeserializer{T}.Deserialize(string, ReadOnlyMemory{byte}, CancellationToken)" path="/param"/>
    /// <returns>A completed task of a copy of the bytes, in a new array, an empty one included.</returns>
    public ValueTask<ReadOnlyMemory<byte>?> Deserialize(
        string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        new ValueTask<ReadOnlyMemory<byte>?>(_inner.Deserialize(topic, data.Span));

    /// <inheritdoc/>
    public ValueTask<ReadOnlyMemory<byte>?> Deserialize(
        string topic, IHeaders headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        new ValueTask<ReadOnlyMemory<byte>?>(_inner.Deserialize(topic, headers, data));
}
