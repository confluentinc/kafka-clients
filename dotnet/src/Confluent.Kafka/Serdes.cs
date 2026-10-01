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

using Confluent.Kafka.Internal.Serialization;

namespace Confluent.Kafka;

/// <summary>
/// Factory of the built-in serdes — the .NET realization of Java's
/// <c>org.apache.kafka.common.serialization.Serdes</c>. Each member is a stateless,
/// shared <see cref="ISerde{T}"/> whose wire format is byte-for-byte identical to the
/// matching Java <c>org.apache.kafka.common.serialization.*</c> serializer /
/// deserializer.
/// </summary>
/// <remarks>
/// The instances are cached singletons: the built-in serdes hold no state and no
/// resources, so a single shared instance per type is safe to reuse across threads and
/// records.
/// </remarks>
public static class Serdes
{
    /// <summary>
    /// UTF-8 <see cref="string"/> serde — Java <c>StringSerializer</c> /
    /// <c>StringDeserializer</c>.
    /// </summary>
    public static ISerde<string> String { get; } = new StringSerde();

    /// <summary>
    /// Identity <see cref="byte"/> array serde — Java <c>ByteArraySerializer</c> /
    /// <c>ByteArrayDeserializer</c>. Deserialize copies the borrowed native slice into
    /// an owned array.
    /// </summary>
    public static ISerde<byte[]> ByteArray { get; } = new ByteArraySerde();

    /// <summary>
    /// 32-bit integer serde — Java <c>IntegerSerializer</c> / <c>IntegerDeserializer</c>:
    /// 4 bytes, big-endian.
    /// </summary>
    public static ISerde<int> Int32 { get; } = new Int32Serde();

    /// <summary>
    /// 64-bit integer serde — Java <c>LongSerializer</c> / <c>LongDeserializer</c>:
    /// 8 bytes, big-endian.
    /// </summary>
    public static ISerde<long> Int64 { get; } = new Int64Serde();

    /// <summary>
    /// IEEE-754 double serde — Java <c>DoubleSerializer</c> / <c>DoubleDeserializer</c>:
    /// 8 bytes, big-endian.
    /// </summary>
    public static ISerde<double> Double { get; } = new DoubleSerde();

    /// <summary>
    /// UUID serde — Java <c>UUIDSerializer</c> / <c>UUIDDeserializer</c>. ⚠ Serializes
    /// the UUID's canonical <b>string form</b> as UTF-8 (not the 16 raw bytes), matching
    /// Java exactly.
    /// </summary>
    public static ISerde<System.Guid> Guid { get; } = new GuidSerde();

    /// <summary>
    /// The "no value" serde — Java <c>VoidSerializer</c> / <c>VoidDeserializer</c>:
    /// serializes to <see langword="null"/>, deserializes to the default value.
    /// </summary>
    public static ISerde<object?> Null { get; } = new NullSerde();
}
