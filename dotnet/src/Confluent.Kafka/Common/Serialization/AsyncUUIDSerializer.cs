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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// <see cref="UUIDSerializer"/> for an asynchronous producer: it calls that class, so the bytes are the same. It never
/// waits: the <see cref="ValueTask{TResult}"/> it returns is already completed, the token is not read, and what
/// <see cref="UUIDSerializer"/> throws is thrown by the call itself.
/// </summary>
public sealed class AsyncUUIDSerializer : IAsyncSerializerWithConfigure<Guid?>, IAsyncBuiltInSerializer<Guid?>
{
    private readonly UUIDSerializer _inner = new UUIDSerializer();

    ISerializer<Guid?> IAsyncBuiltInSerializer<Guid?>.Inner => _inner;

    /// <summary>
    /// Configures this class as <see cref="UUIDSerializer.Configure(IReadOnlyDictionary{string, object}, bool)"/> does, by
    /// calling it. It never waits: the <see cref="ValueTask"/> it returns is already completed, the token is not
    /// read, and what that method throws is thrown by the call itself.
    /// </summary>
    /// <inheritdoc cref="IAsyncSerializerWithConfigure{T}.Configure(IReadOnlyDictionary{string, object}, bool, CancellationToken)" path="/param"/>
    /// <returns>A completed task.</returns>
    public ValueTask Configure(IReadOnlyDictionary<string, object?> configs, bool isKey, CancellationToken cancellationToken)
    {
        _inner.Configure(configs, isKey);
        return default;
    }

    /// <inheritdoc/>
    public ValueTask<ReadOnlyMemory<byte>?> Serialize(
        string topic, Headers headers, Guid? data, CancellationToken cancellationToken) =>
        new ValueTask<ReadOnlyMemory<byte>?>(_inner.Serialize(topic, headers, data));
}
