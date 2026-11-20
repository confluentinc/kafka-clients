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
/// <see cref="FloatDeserializer"/> for an asynchronous consumer: it calls that class, so the value is the same. It
/// never waits: the <see cref="ValueTask{TResult}"/> it returns is already completed, the token is not read, and what
/// <see cref="FloatDeserializer"/> throws is thrown by the call itself.
/// </summary>
public sealed class AsyncFloatDeserializer : IAsyncDeserializer<float?>, IAsyncBuiltInDeserializer<float?>
{
    private readonly FloatDeserializer _inner = new FloatDeserializer();

    IDeserializer<float?> IAsyncBuiltInDeserializer<float?>.Inner => _inner;

    /// <inheritdoc/>
    public ValueTask<float?> Deserialize(
        string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) =>
        new ValueTask<float?>(_inner.Deserialize(topic, headers, data.Span));
}
