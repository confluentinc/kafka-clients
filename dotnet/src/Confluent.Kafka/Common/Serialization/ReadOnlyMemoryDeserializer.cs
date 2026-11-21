// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Common.Header;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>ByteBufferDeserializer</c>, named after <see cref="ReadOnlyMemory{T}"/>, the .NET type that stands for
/// Java's <c>ByteBuffer</c>. The memory form, which a consumer calls, returns the bytes as given, without a copy, as
/// Java's wraps the given bytes. The span form, whose bytes are valid only during the call, copies them into a new
/// array, an empty one included.
/// </summary>
public sealed class ReadOnlyMemoryDeserializer : IDeserializerWithReadOnlyMemory<ReadOnlyMemory<byte>?>
{
    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Deserialize(string topic, ReadOnlySpan<byte> data) =>
        new ReadOnlyMemory<byte>(data.ToArray());

    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> data) => data;
}
