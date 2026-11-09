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

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Converts a <typeparamref name="T"/> to the bytes of a record key or value: Java's
/// <c>Serializer&lt;T&gt;</c>, without its headers overload and its <c>configure</c> and <c>close</c>.
/// </summary>
/// <typeparam name="T">The type encoded.</typeparam>
public interface ISerializer<T>
{
    /// <summary>Encodes <paramref name="data"/>, which may be <see langword="null"/>.</summary>
    /// <param name="topic">The topic the record is sent to.</param>
    /// <param name="data">The value to encode.</param>
    /// <returns>
    /// The bytes, or <see langword="null"/>, which is sent as a null key or value. An empty memory is sent as empty bytes.
    /// A <see langword="null"/> <c>byte[]</c> converts to an empty memory, not to <see langword="null"/>: return
    /// <see langword="null"/> itself.
    /// </returns>
    /// <remarks>
    /// The producer reads the bytes where they are and copies them only into its record batch. It reads them until
    /// <c>Send</c> returns or throws, or, on an async producer, until <c>Send</c> throws or the <c>ValueTask</c> it returns
    /// has completed. Keep them unchanged until then. The key and the value are read after both serializers ran, so they
    /// must not share a buffer that the second call overwrites. A <see cref="System.Buffers.MemoryManager{T}"/> behind the
    /// memory must not call back into the producer: its <c>GetSpan</c> and <c>Pin</c> run inside <c>Send</c>. On an async
    /// producer its <see cref="System.Buffers.MemoryManager{T}.Unpin"/> runs inside <c>Send</c> too when the producer
    /// takes or refuses the record at once, and otherwise later, on the producer's completion thread or on the thread that
    /// closes the producer. If <c>Unpin</c> throws, the send's outcome is unchanged and the failure is written to
    /// <see cref="System.Diagnostics.Trace"/> as a warning.
    /// </remarks>
    ReadOnlyMemory<byte>? Serialize(string topic, T data);
}
