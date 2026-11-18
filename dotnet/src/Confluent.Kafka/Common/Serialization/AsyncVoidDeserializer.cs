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
/// <see cref="VoidDeserializer"/> for an asynchronous consumer: it calls that class, so the value is the same. It never
/// waits: the <see cref="ValueTask{TResult}"/> it returns is already completed, the token is not read, and what
/// <see cref="VoidDeserializer"/> throws is thrown by the call itself.
/// </summary>
public sealed class AsyncVoidDeserializer : IAsyncDeserializer<object?>, IAsyncBuiltInDeserializer<object?>
{
    private readonly VoidDeserializer _inner = new VoidDeserializer();

    IDeserializer<object?> IAsyncBuiltInDeserializer<object?>.Inner => _inner;

    /// <inheritdoc/>
    public ValueTask<object?> Deserialize(
        string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) =>
        new ValueTask<object?>(_inner.Deserialize(topic, headers, data));
}
