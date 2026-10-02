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
/// Identity byte-array serde — mirrors Java's <c>ByteArraySerializer</c> /
/// <c>ByteArrayDeserializer</c> (both return the array unchanged). Backs
/// <see cref="Serdes.ByteArray"/>, and is the bytes bridge P1b's
/// <c>&lt;byte[], byte[]&gt;</c> consumer uses.
/// </summary>
internal sealed class ByteArraySerde : ISerde<byte[]>
{
    // Java ByteArraySerializer.serialize: return data (identity, null passes through).
    public byte[]? Serialize(string topic, byte[] data) => data;

    // Java ByteArrayDeserializer.deserialize returns the array as-is; the span here
    // borrows the native batch, so copy it out to an owned array (ffi §B4, PLAN §4).
    public byte[] Deserialize(string topic, ReadOnlySpan<byte> data) => data.ToArray();
}
