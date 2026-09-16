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
/// A bidirectional serde — both an <see cref="ISerializer{T}"/> and an
/// <see cref="IDeserializer{T}"/> for the same type — the .NET realization of Java's
/// <c>org.apache.kafka.common.serialization.Serde&lt;T&gt;</c> (what the
/// <see cref="Serdes"/> factory returns; Java's <c>Serdes.String()</c> →
/// <c>Serde&lt;String&gt;</c>).
/// </summary>
/// <remarks>
/// Java's <c>Serde&lt;T&gt;</c> exposes the two directions via
/// <c>serializer()</c> / <c>deserializer()</c> accessors and extends
/// <c>Closeable</c>. Here the serde <em>is</em> both interfaces directly — a serde
/// object can serialize and deserialize with no accessor hop — and there is no
/// <c>Closeable</c>: the built-in serdes are stateless and hold no resources. This
/// composes the two directional interfaces (which are the shipped, individually usable
/// contracts) into the single type callers name when they want both directions.
/// </remarks>
/// <typeparam name="T">The value type this serde encodes and decodes.</typeparam>
public interface ISerde<T> : ISerializer<T>, IDeserializer<T>
{
}
