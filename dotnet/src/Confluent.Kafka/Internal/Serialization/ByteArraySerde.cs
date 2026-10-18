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
/// The serde behind <see cref="Serdes.ByteArray"/>: byte arrays unchanged, as Java's <c>ByteArraySerializer</c> and
/// <c>ByteArrayDeserializer</c>; deserializing copies the span into a new array.
/// </summary>
internal sealed class ByteArraySerde : ISerde<byte[]>
{
    public byte[]? Serialize(string topic, byte[] data) => data;

    public byte[] Deserialize(string topic, ReadOnlySpan<byte> data) => data.ToArray();
}
