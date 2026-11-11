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
/// Java's <c>ByteBufferSerializer</c>, over <see cref="ReadOnlyMemory{T}"/> for Java's <c>ByteBuffer</c>: the memory
/// as given, not copied, whether it is an array, a slice of one or native memory, and <see langword="null"/> as
/// <see langword="null"/>, so an empty memory stays apart from a <see langword="null"/> one. The producer reads the
/// bytes where they are, so they must stay unchanged until the send has read them
/// (<see cref="ISerializer{T}.Serialize"/>).
/// </summary>
public sealed class ByteBufferSerializer : ISerializer<ReadOnlyMemory<byte>?>
{
    /// <inheritdoc/>
    public ReadOnlyMemory<byte>? Serialize(string topic, ReadOnlyMemory<byte>? data) => data;
}
