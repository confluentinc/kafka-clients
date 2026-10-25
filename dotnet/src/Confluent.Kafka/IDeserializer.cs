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

namespace Confluent.Kafka;

/// <summary>
/// Converts the bytes of a record key or value to a <typeparamref name="T"/>: Java's
/// <c>Deserializer&lt;T&gt;</c>, without its headers overloads and its <c>configure</c> and <c>close</c>.
/// </summary>
/// <remarks>
/// The bytes are a <see cref="ReadOnlySpan{T}"/> over the fetched batch, where Java passes a <c>byte[]</c>, so a
/// record is not copied before it is deserialized.
/// </remarks>
/// <typeparam name="T">The type the bytes are decoded to.</typeparam>
public interface IDeserializer<T>
{
    /// <summary>Decodes <paramref name="data"/> to a <typeparamref name="T"/>.</summary>
    /// <param name="topic">The topic the record was consumed from.</param>
    /// <param name="data">The bytes, valid only during the call.</param>
    /// <returns>The decoded value.</returns>
    /// <exception cref="SerializationException">The bytes are not a valid <typeparamref name="T"/>.</exception>
    T Deserialize(string topic, ReadOnlySpan<byte> data);
}
