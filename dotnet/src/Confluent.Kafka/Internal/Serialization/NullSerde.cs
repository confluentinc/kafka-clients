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

namespace Confluent.Kafka.Internal.Serialization;

/// <summary>
/// The "no value" serde — mirrors Java's <c>VoidSerializer</c> / <c>VoidDeserializer</c>.
/// Backs <see cref="Serdes.Null"/>. Typed over <see cref="object"/> because C# has no
/// usable <c>Void</c> type argument (Java's is <c>Serde&lt;Void&gt;</c>).
/// </summary>
internal sealed class NullSerde : ISerde<object?>
{
    // Java VoidSerializer.serialize: always returns null.
    public byte[]? Serialize(string topic, object? data) => null;

    // Java VoidDeserializer yields null. The borrowed span cannot express Java's
    // "data must be null" precondition (there is no null span, only an empty one), so
    // per PLAN §4 this deserializes to default (null) unconditionally.
    public object? Deserialize(string topic, ReadOnlySpan<byte> data) => null;
}
