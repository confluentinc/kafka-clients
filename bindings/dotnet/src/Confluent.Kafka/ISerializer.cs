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

namespace Confluent.Kafka;

/// <summary>
/// Converts a value of type <typeparamref name="T"/> to its Kafka wire bytes — the
/// .NET realization of Java's <c>org.apache.kafka.common.serialization.Serializer&lt;T&gt;</c>
/// (<c>serialize(String topic, T data)</c>). A pure managed, binding-/user-layer
/// concern: the C ABI is bytes-only (CLAUDE.md §4), so (de)serialization lives here,
/// not in the Rust core.
/// </summary>
/// <remarks>
/// <para>
/// This is the send-side half of a serde. It is shipped in this milestone
/// <em>ready for</em> the typed producer (<c>Producer&lt;TKey, TValue&gt;</c>, deferred)
/// and is exercised directly by the built-in <see cref="Serdes"/> round-trip tests so
/// it is not dead code; the <em>consumed</em> half this milestone is
/// <see cref="IDeserializer{T}"/>.
/// </para>
/// <para>
/// The <c>configure</c> / <c>close</c> lifecycle hooks on Java's interface are not
/// modeled: the built-in serdes are stateless and hold no resources, and no
/// configuration-driven serde is in scope. They can be added later
/// non-breakingly (a C# default-interface-method) if a configurable serde is needed.
/// </para>
/// </remarks>
/// <typeparam name="T">The value type this serializer encodes.</typeparam>
public interface ISerializer<T>
{
    /// <summary>
    /// Encodes <paramref name="data"/> to its Kafka wire bytes.
    /// </summary>
    /// <param name="topic">
    /// The topic the value is being produced to. Passed through for parity with
    /// Java (topic-aware serializers); the built-in serdes ignore it.
    /// </param>
    /// <param name="data">The value to encode.</param>
    /// <returns>
    /// The encoded bytes. May be <see langword="null"/> — Java's serializers return
    /// <c>null</c> for <c>null</c> input and Java's <c>VoidSerializer</c> always
    /// returns <c>null</c>; a <c>null</c> value maps to a Kafka tombstone on the
    /// produce path (<c>ProducerRecord.Value</c> is nullable), so the nullable return
    /// preserves that distinction.
    /// </returns>
    byte[]? Serialize(string topic, T data);
}
