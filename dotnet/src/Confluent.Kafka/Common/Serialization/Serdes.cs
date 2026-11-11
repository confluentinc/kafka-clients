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
using System.Collections.Generic;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Factory for the built-in serdes, Java's <c>Serdes</c>. Each member returns a new serde, whose serializer and
/// deserializer write and read the bytes of the Java classes of the same names. A Java boxed type is a nullable value
/// type here (<c>long?</c> for <c>Long</c>): <see langword="null"/> serializes to <see langword="null"/>, and a
/// consumer reads an absent key or value as <see langword="null"/>.
/// </summary>
public static class Serdes
{
    /// <summary>
    /// A serializer and a deserializer held as a pair, Java's <c>Serdes.WrapperSerde</c>, configured and disposed
    /// together. A serde of a user's type can derive from it.
    /// </summary>
    /// <typeparam name="T">The type encoded and decoded.</typeparam>
    public class WrapperSerde<T> : ISerdeWithConfigure<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly IDeserializer<T> _deserializer;

        /// <summary>Holds the serializer and the deserializer, which are not checked, as in Java.</summary>
        /// <param name="serializer">The serializer <see cref="Serializer()"/> returns.</param>
        /// <param name="deserializer">The deserializer <see cref="Deserializer()"/> returns.</param>
        public WrapperSerde(ISerializer<T> serializer, IDeserializer<T> deserializer)
        {
            _serializer = serializer;
            _deserializer = deserializer;
        }

        /// <inheritdoc/>
        public ISerializer<T> Serializer() => _serializer;

        /// <inheritdoc/>
        public IDeserializer<T> Deserializer() => _deserializer;

        /// <inheritdoc/>
        /// <remarks>
        /// Configures the serializer, then the deserializer, each with
        /// <see cref="SerializerExtensions.Configure{T}(ISerializer{T}, IReadOnlyDictionary{string, object}, bool)"/> and
        /// <see cref="DeserializerExtensions.Configure{T}(IDeserializer{T}, IReadOnlyDictionary{string, object}, bool)"/>,
        /// so a half that has no <c>Configure</c> of its own is left as it is.
        /// </remarks>
        public virtual void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey)
        {
            _serializer.Configure(configs, isKey);
            _deserializer.Configure(configs, isKey);
        }

        /// <inheritdoc/>
        /// <remarks>
        /// Java's <c>close()</c>: disposes the serializer, then the deserializer, each one that is
        /// <see cref="IDisposable"/>. An exception from the serializer's <c>Dispose</c> leaves the deserializer
        /// undisposed, as in Java.
        /// </remarks>
        public virtual void Dispose()
        {
            (_serializer as IDisposable)?.Dispose();
            (_deserializer as IDisposable)?.Dispose();
        }
    }

    /// <summary>Java's <c>Serdes.VoidSerde</c>: a <see cref="VoidSerializer"/> and a <see cref="VoidDeserializer"/>.</summary>
    public sealed class VoidSerde : WrapperSerde<object?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public VoidSerde()
            : base(new VoidSerializer(), new VoidDeserializer())
        {
        }
    }

    /// <summary>Java's <c>Serdes.LongSerde</c>: a <see cref="LongSerializer"/> and a <see cref="LongDeserializer"/>.</summary>
    public sealed class LongSerde : WrapperSerde<long?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public LongSerde()
            : base(new LongSerializer(), new LongDeserializer())
        {
        }
    }

    /// <summary>
    /// Java's <c>Serdes.IntegerSerde</c>: an <see cref="IntegerSerializer"/> and an <see cref="IntegerDeserializer"/>.
    /// </summary>
    public sealed class IntegerSerde : WrapperSerde<int?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public IntegerSerde()
            : base(new IntegerSerializer(), new IntegerDeserializer())
        {
        }
    }

    /// <summary>Java's <c>Serdes.ShortSerde</c>: a <see cref="ShortSerializer"/> and a <see cref="ShortDeserializer"/>.</summary>
    public sealed class ShortSerde : WrapperSerde<short?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public ShortSerde()
            : base(new ShortSerializer(), new ShortDeserializer())
        {
        }
    }

    /// <summary>Java's <c>Serdes.FloatSerde</c>: a <see cref="FloatSerializer"/> and a <see cref="FloatDeserializer"/>.</summary>
    public sealed class FloatSerde : WrapperSerde<float?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public FloatSerde()
            : base(new FloatSerializer(), new FloatDeserializer())
        {
        }
    }

    /// <summary>
    /// Java's <c>Serdes.DoubleSerde</c>: a <see cref="DoubleSerializer"/> and a <see cref="DoubleDeserializer"/>.
    /// </summary>
    public sealed class DoubleSerde : WrapperSerde<double?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public DoubleSerde()
            : base(new DoubleSerializer(), new DoubleDeserializer())
        {
        }
    }

    /// <summary>
    /// Java's <c>Serdes.StringSerde</c>: a <see cref="StringSerializer"/> and a <see cref="StringDeserializer"/>.
    /// </summary>
    public sealed class StringSerde : WrapperSerde<string>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public StringSerde()
            : base(new StringSerializer(), new StringDeserializer())
        {
        }
    }

    /// <summary>
    /// Java's <c>Serdes.ByteBufferSerde</c>: a <see cref="ByteBufferSerializer"/> and a
    /// <see cref="ByteBufferDeserializer"/>.
    /// </summary>
    public sealed class ByteBufferSerde : WrapperSerde<ReadOnlyMemory<byte>?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public ByteBufferSerde()
            : base(new ByteBufferSerializer(), new ByteBufferDeserializer())
        {
        }
    }

    /// <summary>
    /// Java's <c>Serdes.ByteArraySerde</c>: a <see cref="ByteArraySerializer"/> and a
    /// <see cref="ByteArrayDeserializer"/>.
    /// </summary>
    public sealed class ByteArraySerde : WrapperSerde<byte[]>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public ByteArraySerde()
            : base(new ByteArraySerializer(), new ByteArrayDeserializer())
        {
        }
    }

    /// <summary>Java's <c>Serdes.UUIDSerde</c>: a <see cref="UUIDSerializer"/> and a <see cref="UUIDDeserializer"/>.</summary>
    public sealed class UUIDSerde : WrapperSerde<Guid?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public UUIDSerde()
            : base(new UUIDSerializer(), new UUIDDeserializer())
        {
        }
    }

    /// <summary>
    /// Java's <c>Serdes.BooleanSerde</c>: a <see cref="BooleanSerializer"/> and a <see cref="BooleanDeserializer"/>.
    /// </summary>
    public sealed class BooleanSerde : WrapperSerde<bool?>
    {
        /// <summary>Creates the serde over a new serializer and deserializer.</summary>
        public BooleanSerde()
            : base(new BooleanSerializer(), new BooleanDeserializer())
        {
        }
    }

    /// <summary>
    /// A new built-in serde for <typeparamref name="T"/>, Java's <c>serdeFrom(Class)</c>, whose class token is the type
    /// argument here: <see cref="string"/>, <see cref="short"/>?, <see cref="int"/>?, <see cref="long"/>?,
    /// <see cref="float"/>?, <see cref="double"/>?, <c>byte[]</c>, <see cref="ReadOnlyMemory{T}"/>? (Java's
    /// <c>ByteBuffer</c>), <see cref="Guid"/>? (Java's <c>UUID</c>) or <see cref="bool"/>?.
    /// </summary>
    /// <typeparam name="T">The type encoded and decoded. A Java boxed type is the nullable value type: as Java refuses
    /// <c>long.class</c>, <c>SerdeFrom&lt;long&gt;()</c> throws.</typeparam>
    /// <returns>A new serde of the type's built-in serializer and deserializer.</returns>
    /// <exception cref="IllegalArgumentException"><typeparamref name="T"/> has no built-in serde.</exception>
    public static ISerde<T> SerdeFrom<T>()
    {
        Type type = typeof(T);
        if (type == typeof(string))
        {
            return (ISerde<T>)(object)String();
        }

        if (type == typeof(short?))
        {
            return (ISerde<T>)(object)Short();
        }

        if (type == typeof(int?))
        {
            return (ISerde<T>)(object)Integer();
        }

        if (type == typeof(long?))
        {
            return (ISerde<T>)(object)Long();
        }

        if (type == typeof(float?))
        {
            return (ISerde<T>)(object)Float();
        }

        if (type == typeof(double?))
        {
            return (ISerde<T>)(object)Double();
        }

        if (type == typeof(byte[]))
        {
            return (ISerde<T>)(object)ByteArray();
        }

        if (type == typeof(ReadOnlyMemory<byte>?))
        {
            return (ISerde<T>)(object)ByteBuffer();
        }

        if (type == typeof(Guid?))
        {
            return (ISerde<T>)(object)UUID();
        }

        if (type == typeof(bool?))
        {
            return (ISerde<T>)(object)Boolean();
        }

        // Java's list also names Bytes, which has no serde here.
        throw new IllegalArgumentException("Unknown class for built-in serializer. Supported types are: "
            + "String, Short, Integer, Long, Float, Double, ByteArray, ByteBuffer, UUID, Boolean");
    }

    /// <summary>Builds a serde from a serializer and a deserializer, Java's <c>serdeFrom(Serializer, Deserializer)</c>.</summary>
    /// <typeparam name="T">The type encoded and decoded.</typeparam>
    /// <param name="serializer">The serializer; must not be <see langword="null"/>.</param>
    /// <param name="deserializer">The deserializer; must not be <see langword="null"/>.</param>
    /// <returns>A new <see cref="WrapperSerde{T}"/> holding the two.</returns>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="serializer"/> or <paramref name="deserializer"/> is <see langword="null"/>.
    /// </exception>
    public static ISerde<T> SerdeFrom<T>(ISerializer<T> serializer, IDeserializer<T> deserializer)
    {
        if (serializer is null)
        {
            throw new IllegalArgumentException("serializer must not be null");
        }

        if (deserializer is null)
        {
            throw new IllegalArgumentException("deserializer must not be null");
        }

        return new WrapperSerde<T>(serializer, deserializer);
    }

    /// <summary>A new serde of 8-byte big-endian integers for Java's nullable <c>Long</c>.</summary>
    /// <returns>A new <see cref="LongSerde"/>.</returns>
    public static ISerde<long?> Long() => new LongSerde();

    /// <summary>A new serde of 4-byte big-endian integers for Java's nullable <c>Integer</c>.</summary>
    /// <returns>A new <see cref="IntegerSerde"/>.</returns>
    public static ISerde<int?> Integer() => new IntegerSerde();

    /// <summary>A new serde of 2-byte big-endian integers for Java's nullable <c>Short</c>.</summary>
    /// <returns>A new <see cref="ShortSerde"/>.</returns>
    public static ISerde<short?> Short() => new ShortSerde();

    /// <summary>
    /// A new serde of 4-byte big-endian IEEE-754 floats for Java's nullable <c>Float</c>, a NaN's payload kept.
    /// </summary>
    /// <returns>A new <see cref="FloatSerde"/>.</returns>
    public static ISerde<float?> Float() => new FloatSerde();

    /// <summary>A new serde of 8-byte big-endian IEEE-754 doubles for Java's nullable <c>Double</c>.</summary>
    /// <returns>A new <see cref="DoubleSerde"/>.</returns>
    public static ISerde<double?> Double() => new DoubleSerde();

    /// <summary>
    /// A new serde of strings, <see langword="null"/> included, as UTF-8 unless configured with Java's encoding keys
    /// (<see cref="StringSerializer"/>, <see cref="StringDeserializer"/>).
    /// </summary>
    /// <returns>A new <see cref="StringSerde"/>.</returns>
    public static ISerde<string> String() => new StringSerde();

    /// <summary>
    /// A new serde of byte memory for Java's nullable <c>ByteBuffer</c>: serializing sends the memory as given, and
    /// deserializing copies the bytes into a new array.
    /// </summary>
    /// <returns>A new <see cref="ByteBufferSerde"/>.</returns>
    public static ISerde<ReadOnlyMemory<byte>?> ByteBuffer() => new ByteBufferSerde();

    /// <summary>
    /// A new serde of UUIDs as the bytes of their hyphenated string form, not their 16 raw bytes, for Java's nullable
    /// <c>UUID</c>, in UTF-8 unless configured with Java's encoding keys (<see cref="UUIDSerializer"/>,
    /// <see cref="UUIDDeserializer"/>).
    /// </summary>
    /// <returns>A new <see cref="UUIDSerde"/>.</returns>
    public static ISerde<Guid?> UUID() => new UUIDSerde();

    /// <summary>A new serde of one byte, <c>0x01</c> or <c>0x00</c>, for Java's nullable <c>Boolean</c>.</summary>
    /// <returns>A new <see cref="BooleanSerde"/>.</returns>
    public static ISerde<bool?> Boolean() => new BooleanSerde();

    /// <summary>A new serde of byte arrays, unchanged, <see langword="null"/> included; deserializing copies the bytes.</summary>
    /// <returns>A new <see cref="ByteArraySerde"/>.</returns>
    public static ISerde<byte[]> ByteArray() => new ByteArraySerde();

    /// <summary>
    /// A new serde for Java's <c>Void</c>: it serializes every value to <see langword="null"/>, and its deserializer
    /// refuses any data.
    /// </summary>
    /// <returns>A new <see cref="VoidSerde"/>.</returns>
    public static ISerde<object?> Void() => new VoidSerde();
}
