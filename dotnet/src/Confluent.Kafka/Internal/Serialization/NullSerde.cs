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
/// The serde behind <see cref="Serdes.Null"/>, Java's <c>VoidSerializer</c> and <c>VoidDeserializer</c> over
/// <see cref="object"/>: it serializes to <see langword="null"/> and deserializes any data to <see langword="null"/>,
/// where Java's <c>VoidDeserializer</c> rejects non-null data.
/// </summary>
internal sealed class NullSerde : ISerde<object?>
{
    public byte[]? Serialize(string topic, object? data) => null;

    public object? Deserialize(string topic, ReadOnlySpan<byte> data) => null;
}
