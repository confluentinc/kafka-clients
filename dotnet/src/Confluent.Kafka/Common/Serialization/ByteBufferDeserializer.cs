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

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>ByteBufferDeserializer</c>, over <see cref="ReadOnlyMemory{T}"/> for Java's <c>ByteBuffer</c>: the bytes,
/// copied into a new array, an empty one included. Java's wraps the given bytes without a copy; here they are valid only
/// during the call (a consumer passes a view of the fetched batch), and a memory over them could not outlive it.
/// </summary>
public sealed class ByteBufferDeserializer : IDeserializer<ReadOnlyMemory<byte>?>
{
    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Deserialize(string topic, ReadOnlySpan<byte> data) =>
        new ReadOnlyMemory<byte>(data.ToArray());
}
