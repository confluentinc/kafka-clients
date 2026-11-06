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

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// The built-in serdes, Java's <c>Serdes</c>: each a shared, stateless <see cref="ISerde{T}"/> whose bytes
/// match those of the Java serializer and deserializer it names.
/// </summary>
public static class Serdes
{
    private static readonly ISerde<string> s_string = new StringSerde();
    private static readonly ISerde<byte[]> s_byteArray = new ByteArraySerde();
    private static readonly ISerde<int> s_integer = new Int32Serde();
    private static readonly ISerde<long> s_long = new Int64Serde();
    private static readonly ISerde<double> s_double = new DoubleSerde();
    private static readonly ISerde<System.Guid> s_uuid = new GuidSerde();
    private static readonly ISerde<object?> s_void = new NullSerde();

    /// <summary>UTF-8 strings: Java's <c>StringSerializer</c> and <c>StringDeserializer</c>.</summary>
    public static ISerde<string> String() => s_string;

    /// <summary>
    /// Byte arrays, unchanged; deserializing copies the bytes: Java's <c>ByteArraySerializer</c> and
    /// <c>ByteArrayDeserializer</c>.
    /// </summary>
    public static ISerde<byte[]> ByteArray() => s_byteArray;

    /// <summary>4-byte big-endian integers: Java's <c>IntegerSerializer</c> and <c>IntegerDeserializer</c>.</summary>
    public static ISerde<int> Integer() => s_integer;

    /// <summary>8-byte big-endian integers: Java's <c>LongSerializer</c> and <c>LongDeserializer</c>.</summary>
    public static ISerde<long> Long() => s_long;

    /// <summary>8-byte big-endian IEEE-754 doubles: Java's <c>DoubleSerializer</c> and <c>DoubleDeserializer</c>.</summary>
    public static ISerde<double> Double() => s_double;

    /// <summary>
    /// A UUID as the UTF-8 bytes of its hyphenated string form, not its 16 raw bytes: Java's
    /// <c>UUIDSerializer</c> and <c>UUIDDeserializer</c>.
    /// </summary>
    public static ISerde<System.Guid> UUID() => s_uuid;

    /// <summary>
    /// Serializes to <see langword="null"/> and deserializes to <see langword="null"/>: Java's
    /// <c>VoidSerializer</c> and <c>VoidDeserializer</c>.
    /// </summary>
    public static ISerde<object?> Void() => s_void;
}
