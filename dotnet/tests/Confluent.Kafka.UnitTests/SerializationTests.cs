// Copyright 2026 Confluent Inc.
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
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>SerializationTest</c>, translated test by test, each Java test the serdes shipped cannot run skipped under
/// its name with the reason, and the tests of the serde pair: a <see langword="null"/> through the nullable built-ins
/// on the producer and the consumer, the ByteBuffer serde's bytes on both, a value the Void deserializer refuses on the
/// consumer, a new serde per <see cref="Serdes"/> call, the types <c>SerdeFrom&lt;T&gt;()</c> refuses, and the
/// disposal of each half.
/// </summary>
public sealed class SerializationTests
{
    private const string Topic = "testTopic";

    private const int Partition = 0;

    private const string UnknownClassMessage = "Unknown class for built-in serializer. Supported types are: String, "
        + "Short, Integer, Long, Float, Double, ByteArray, ByteBuffer, UUID, Boolean";

    private const string NoListSerde = "Serdes.ListSerde is not translated: Java's ListSerializer and ListDeserializer "
        + "have no .NET form yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public void SerializationTest_allSerdesShouldRoundtripInput()
    {
        byte[] myString = Encoding.UTF8.GetBytes("my string");
        byte[] larger = new byte[myString.Length + 3];
        myString.CopyTo(larger, 2);
        using NativeMemoryManager direct = new NativeMemoryManager(10);
        myString.CopyTo(direct.GetSpan());

        AssertRoundtrips<string>("String", new string?[] { null, "my string" });
        AssertRoundtrips<short?>("Short", new short?[] { null, 32767, -32768 });
        AssertRoundtrips<int?>("Integer", new int?[] { null, 423412424, -41243432 });
        AssertRoundtrips<long?>("Long", new long?[] { null, 922337203685477580L, -922337203685477581L });
        AssertRoundtrips<float?>("Float", new float?[] { null, 5678567.12312f, -5678567.12341f });
        AssertRoundtrips<double?>("Double", new double?[] { null, 5678567.12312d, -5678567.12341d });
        AssertRoundtrips<byte[]>("byte[]", new byte[]?[] { null, myString }, SameBytes);
        AssertRoundtrips<ReadOnlyMemory<byte>?>(
            "ByteBuffer",
            new ReadOnlyMemory<byte>?[]
            {
                null,
                new ReadOnlyMemory<byte>(myString),
                new ReadOnlyMemory<byte>(larger, 2, myString.Length),
                (ReadOnlyMemory<byte>)direct.Memory,
            },
            SameBytes);
        AssertRoundtrips<Guid?>("UUID", new Guid?[] { null, Guid.NewGuid() });
    }

    [Fact(Skip = "Java's Bytes row, here and in allSerdesShouldSupportNull: Serdes.Bytes() is not translated, since "
        + "Java's org.apache.kafka.common.utils.Bytes has no .NET form yet")]
    public void SerializationTest_allSerdesShouldRoundtripInput_TheBytesRow()
    {
    }

    [Fact]
    public void SerializationTest_allSerdesShouldSupportNull_TheSerializerHalf()
    {
        foreach ((string name, Func<ReadOnlyMemory<byte>?> serializeNull) in new (string, Func<ReadOnlyMemory<byte>?>)[]
        {
            ("String", () => SerializeNull(Serdes.SerdeFrom<string>())),
            ("Short", () => SerializeNull(Serdes.SerdeFrom<short?>())),
            ("Integer", () => SerializeNull(Serdes.SerdeFrom<int?>())),
            ("Long", () => SerializeNull(Serdes.SerdeFrom<long?>())),
            ("Float", () => SerializeNull(Serdes.SerdeFrom<float?>())),
            ("Double", () => SerializeNull(Serdes.SerdeFrom<double?>())),
            ("byte[]", () => SerializeNull(Serdes.SerdeFrom<byte[]>())),
            ("ByteBuffer", () => SerializeNull(Serdes.SerdeFrom<ReadOnlyMemory<byte>?>())),
            ("UUID", () => SerializeNull(Serdes.SerdeFrom<Guid?>())),
            ("Boolean", () => SerializeNull(Serdes.SerdeFrom<bool?>())),
            ("Void", () => SerializeNull(Serdes.Void())),
        })
        {
            Assert.True(serializeNull() is null, "Should support null in " + name + " serialization");
        }
    }

    [Fact(Skip = "Java's deserializer half passes null data, which a ReadOnlySpan<byte> cannot express; a consumer never "
        + "calls a deserializer for a null key or value, and reads it as null (the MockConsumer tests below)")]
    public void SerializationTest_allSerdesShouldSupportNull_TheDeserializerHalf()
    {
    }

    [Fact]
    public void SerializationTest_testSerdeFromUnknown()
    {
        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(() => Serdes.SerdeFrom<DummyClass>());

        Assert.Equal(UnknownClassMessage, e.Message);
    }

    [Fact]
    public void SerdeFrom_ATypeWithoutABuiltInSerde_Throws_ANonNullableValueTypeIncluded_AsJavaRefusesAPrimitiveClass()
    {
        foreach (Func<object> serdeFrom in new Func<object>[]
        {
            () => Serdes.SerdeFrom<long>(),
            () => Serdes.SerdeFrom<int>(),
            () => Serdes.SerdeFrom<short>(),
            () => Serdes.SerdeFrom<float>(),
            () => Serdes.SerdeFrom<double>(),
            () => Serdes.SerdeFrom<bool>(),
            () => Serdes.SerdeFrom<Guid>(),
            () => Serdes.SerdeFrom<ReadOnlyMemory<byte>>(),
            () => Serdes.SerdeFrom<Memory<byte>?>(),
            () => Serdes.SerdeFrom<object>(),
        })
        {
            IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(serdeFrom);
            Assert.Equal(UnknownClassMessage, e.Message);
        }
    }

    [Fact]
    public void SerdeFrom_EachBuiltInType_ReturnsANewSerde_OfJavasNestedClass_OverJavasClasses()
    {
        AssertNewSerde<string, Serdes.StringSerde, StringSerializer, StringDeserializer>(Serdes.SerdeFrom<string>);
        AssertNewSerde<short?, Serdes.ShortSerde, ShortSerializer, ShortDeserializer>(Serdes.SerdeFrom<short?>);
        AssertNewSerde<int?, Serdes.IntegerSerde, IntegerSerializer, IntegerDeserializer>(Serdes.SerdeFrom<int?>);
        AssertNewSerde<long?, Serdes.LongSerde, LongSerializer, LongDeserializer>(Serdes.SerdeFrom<long?>);
        AssertNewSerde<float?, Serdes.FloatSerde, FloatSerializer, FloatDeserializer>(Serdes.SerdeFrom<float?>);
        AssertNewSerde<double?, Serdes.DoubleSerde, DoubleSerializer, DoubleDeserializer>(Serdes.SerdeFrom<double?>);
        AssertNewSerde<byte[], Serdes.ByteArraySerde, ByteArraySerializer, ByteArrayDeserializer>(Serdes.SerdeFrom<byte[]>);
        AssertNewSerde<ReadOnlyMemory<byte>?, Serdes.ByteBufferSerde, ByteBufferSerializer, ByteBufferDeserializer>(
            Serdes.SerdeFrom<ReadOnlyMemory<byte>?>);
        AssertNewSerde<Guid?, Serdes.UUIDSerde, UUIDSerializer, UUIDDeserializer>(Serdes.SerdeFrom<Guid?>);
        AssertNewSerde<bool?, Serdes.BooleanSerde, BooleanSerializer, BooleanDeserializer>(Serdes.SerdeFrom<bool?>);
    }

    [Fact]
    public void SerializationTest_floatDeserializerShouldThrowSerializationExceptionOnZeroBytes()
    {
        AssertFloatSizeRefused(new byte[0]);
    }

    [Fact]
    public void SerializationTest_floatDeserializerShouldThrowSerializationExceptionOnTooFewBytes()
    {
        AssertFloatSizeRefused(new byte[3]);
    }

    [Fact]
    public void SerializationTest_floatDeserializerShouldThrowSerializationExceptionOnTooManyBytes()
    {
        AssertFloatSizeRefused(new byte[5]);
    }

    [Fact]
    public void SerializationTest_floatSerdeShouldPreserveNaNValues()
    {
        int someNaNAsIntBits = 0x7f800001;
        float someNaN = IntBitsToFloat(someNaNAsIntBits);
        int anotherNaNAsIntBits = 0x7f800002;
        float anotherNaN = IntBitsToFloat(anotherNaNAsIntBits);

        using ISerde<float?> serde = Serdes.Float();

        // Both values are signaling NaNs. In a 32-bit x86 process a float returned from a method passes through the
        // x87 stack, which sets the quiet bit and keeps the payload, as Java's Float.intBitsToFloat notes.
        int quietBit = Environment.Is64BitProcess ? 0 : 0x00400000;

        float? roundtrip = serde.Deserializer().Deserialize(Topic, serde.Serializer().Serialize(Topic, someNaN)!.Value.Span);
        Assert.Equal(someNaNAsIntBits | quietBit, FloatToRawIntBits(roundtrip!.Value));
        float? otherRoundtrip =
            serde.Deserializer().Deserialize(Topic, serde.Serializer().Serialize(Topic, anotherNaN)!.Value.Span);
        Assert.Equal(anotherNaNAsIntBits | quietBit, FloatToRawIntBits(otherRoundtrip!.Value));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SerializationTest_testBooleanSerializer(bool dataToSerialize)
    {
        byte[] testData = new byte[1];
        testData[0] = (byte)(dataToSerialize ? 1 : 0);

        ISerde<bool?> booleanSerde = Serdes.Boolean();
        Assert.Equal(testData, booleanSerde.Serializer().Serialize(Topic, dataToSerialize)!.Value.ToArray());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SerializationTest_testBooleanDeserializer(bool dataToDeserialize)
    {
        byte[] testData = new byte[1];
        testData[0] = (byte)(dataToDeserialize ? 1 : 0);

        ISerde<bool?> booleanSerde = Serdes.Boolean();
        Assert.Equal(dataToDeserialize, booleanSerde.Deserializer().Deserialize(Topic, testData));
    }

    [Fact]
    public void SerializationTest_booleanDeserializerShouldThrowOnEmptyInput()
    {
        using ISerde<bool?> serde = Serdes.Boolean();

        SerializationException e = Assert.Throws<SerializationException>(
            () => serde.Deserializer().Deserialize(Topic, new byte[0]));

        Assert.Equal("Size of data received by BooleanDeserializer is not 1", e.Message);
    }

    [Fact]
    public void SerializationTest_testSerdeFromNotNull()
    {
        using ISerde<long?> serde = Serdes.Long();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => Serdes.SerdeFrom(null!, serde.Deserializer()));

        Assert.Equal("serializer must not be null", e.Message);
    }

    [Fact]
    public void SerializationTest_stringSerdeShouldSupportDifferentEncodings()
    {
        string str = "my string";
        string[] encodings = { "UTF-8", "UTF-16" };

        foreach (string encoding in encodings)
        {
            using ISerde<string> serDeser = GetStringSerde(encoding);

            ISerializer<string> serializer = serDeser.Serializer();
            IDeserializer<string> deserializer = serDeser.Deserializer();
            Assert.True(
                str == deserializer.Deserialize(Topic, serializer.Serialize(Topic, str)!.Value.Span),
                "Should get the original string after serialization and deserialization with encoding " + encoding);
        }
    }

    [Fact]
    public void SerializationTest_stringSerdeConfigureThrowsOnUnknownEncoding()
    {
        string encoding = "encoding-does-not-exist";
        using ISerde<string> serDeser = Serdes.String();

        Dictionary<string, object?> serializerConfigs = new Dictionary<string, object?>
        {
            ["key.serializer.encoding"] = encoding,
        };
        SerializationException serializerError = Assert.Throws<SerializationException>(
            () => serDeser.Serializer().Configure(serializerConfigs, true));

        Dictionary<string, object?> deserializerConfigs = new Dictionary<string, object?>
        {
            ["key.deserializer.encoding"] = encoding,
        };
        SerializationException deserializerError = Assert.Throws<SerializationException>(
            () => serDeser.Deserializer().Configure(deserializerConfigs, true));

        foreach (SerializationException e in new[] { serializerError, deserializerError })
        {
            Assert.Equal("Unsupported encoding encoding-does-not-exist", e.Message);
            Assert.Equal(encoding, Assert.IsType<IllegalArgumentException>(e.InnerException).Message);
        }
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldReturnEmptyCollection()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldReturnNull()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripIntPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeSerializerShouldReturnByteArrayOfFixedSizeForIntPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripShortPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeSerializerShouldReturnByteArrayOfFixedSizeForShortPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripFloatPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeSerializerShouldReturnByteArrayOfFixedSizeForFloatPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripLongPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeSerializerShouldReturnByteArrayOfFixedSizeForLongPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripDoublePrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeSerializerShouldReturnByteArrayOfFixedSizeForDoublePrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripUUIDInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeSerializerShouldReturnByteArrayOfFixedSizeForUUIDInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripNonPrimitiveInput()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripPrimitiveInputWithNullEntries()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldRoundtripNonPrimitiveInputWithNullEntries()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldReturnLinkedList()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void SerializationTest_listSerdeShouldReturnStack()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void ListSerializerTest_AllTwelveTests()
    {
    }

    [Fact(Skip = NoListSerde)]
    public void ListDeserializerTest_AllTwentySevenTests()
    {
    }

    [Fact]
    public void SerializationTest_testSerializeVoid()
    {
        using ISerde<object?> serde = Serdes.Void();

        Assert.True(serde.Serializer().Serialize(Topic, null) is null);
    }

    [Fact(Skip = "Java's test passes null data, which a ReadOnlySpan<byte> cannot express; a consumer never calls a "
        + "deserializer for a null key or value")]
    public void SerializationTest_testDeserializeVoid()
    {
    }

    [Fact]
    public void SerializationTest_voidDeserializerShouldThrowOnNotNullValues()
    {
        using ISerde<object?> serde = Serdes.Void();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => serde.Deserializer().Deserialize(Topic, new byte[5]));

        Assert.Equal("Data should be null for a VoidDeserializer.", e.Message);
    }

    [Fact]
    public void SerializationTest_stringDeserializerSupportByteBuffer()
    {
        const string data = "Hello, ByteBuffer!";
        using ISerde<string> serde = Serdes.String();
        ISerializer<string> serializer = serde.Serializer();
        IDeserializer<string> deserializer = serde.Deserializer();
        ReadOnlyMemory<byte>? serialized = serializer.Serialize(Topic, data);
        Assert.True(serialized.HasValue);
        byte[] serializedBytes = serialized.Value.ToArray();

        byte[] heap = new byte[serializedBytes.Length << 1];
        serializedBytes.CopyTo(heap, 0);
        Assert.Equal(data, deserializer.Deserialize(Topic, new Headers(), heap.AsSpan(0, serializedBytes.Length)));

        using NativeMemoryManager direct = new NativeMemoryManager(serializedBytes.Length << 2);
        serializedBytes.CopyTo(direct.GetSpan());
        Assert.Equal(data, deserializer.Deserialize(Topic, new Headers(), direct.GetSpan().Slice(0, serializedBytes.Length)));
    }

    [Fact]
    public void SerdeFrom_NullDeserializer_ThrowsJavasMessage_AfterTheSerializerCheck()
    {
        using ISerde<long?> serde = Serdes.Long();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => Serdes.SerdeFrom(serde.Serializer(), null!));
        IllegalArgumentException both = Assert.Throws<IllegalArgumentException>(
            () => Serdes.SerdeFrom<long?>(null!, null!));

        Assert.Equal("deserializer must not be null", e.Message);
        Assert.Equal("serializer must not be null", both.Message);
    }

    [Fact]
    public void SerdeFrom_ReturnsAWrapperSerde_OverTheGivenHalves()
    {
        StringSerializer serializer = new StringSerializer();
        StringDeserializer deserializer = new StringDeserializer();

        using ISerde<string> serde = Serdes.SerdeFrom(serializer, deserializer);

        Assert.IsType<Serdes.WrapperSerde<string>>(serde);
        Assert.Same(serializer, serde.Serializer());
        Assert.Same(serializer, serde.Serializer());
        Assert.Same(deserializer, serde.Deserializer());
        Assert.Same(deserializer, serde.Deserializer());
    }

    [Fact]
    public void WrapperSerde_TakesItsHalvesUnchecked_AsJavasConstructor()
    {
        Serdes.WrapperSerde<string> serde = new Serdes.WrapperSerde<string>(null!, null!);

        Assert.Null(serde.Serializer());
        Assert.Null(serde.Deserializer());
        serde.Dispose();
    }

    [Fact]
    public void Serdes_EachCall_ReturnsANewSerde_OfJavasNestedClass_OverJavasClasses()
    {
        AssertNewSerde<string, Serdes.StringSerde, StringSerializer, StringDeserializer>(Serdes.String);
        AssertNewSerde<byte[], Serdes.ByteArraySerde, ByteArraySerializer, ByteArrayDeserializer>(Serdes.ByteArray);
        AssertNewSerde<int?, Serdes.IntegerSerde, IntegerSerializer, IntegerDeserializer>(Serdes.Integer);
        AssertNewSerde<long?, Serdes.LongSerde, LongSerializer, LongDeserializer>(Serdes.Long);
        AssertNewSerde<short?, Serdes.ShortSerde, ShortSerializer, ShortDeserializer>(Serdes.Short);
        AssertNewSerde<float?, Serdes.FloatSerde, FloatSerializer, FloatDeserializer>(Serdes.Float);
        AssertNewSerde<double?, Serdes.DoubleSerde, DoubleSerializer, DoubleDeserializer>(Serdes.Double);
        AssertNewSerde<ReadOnlyMemory<byte>?, Serdes.ByteBufferSerde, ByteBufferSerializer, ByteBufferDeserializer>(
            Serdes.ByteBuffer);
        AssertNewSerde<Guid?, Serdes.UUIDSerde, UUIDSerializer, UUIDDeserializer>(Serdes.UUID);
        AssertNewSerde<bool?, Serdes.BooleanSerde, BooleanSerializer, BooleanDeserializer>(Serdes.Boolean);
        AssertNewSerde<object?, Serdes.VoidSerde, VoidSerializer, VoidDeserializer>(Serdes.Void);
    }

    [Fact]
    public void Serdes_String_ReturnsANewObjectPerCall()
    {
        ISerde<string> first = Serdes.String();
        ISerde<string> second = Serdes.String();

        Assert.NotSame(first, second);
        Assert.NotSame(first.Serializer(), second.Serializer());
        Assert.NotSame(first.Deserializer(), second.Deserializer());
    }

    [Fact]
    public void WrapperSerde_Dispose_DisposesTheSerializerThenTheDeserializer()
    {
        List<string> log = new List<string>();
        DisposableSerializer serializer = new DisposableSerializer(log);
        DisposableDeserializer deserializer = new DisposableDeserializer(log);

        Serdes.WrapperSerde<string> serde = new Serdes.WrapperSerde<string>(serializer, deserializer);
        serde.Dispose();

        Assert.Equal(new[] { "serializer", "deserializer" }, log);
        Assert.Equal(1, serializer.Disposals);
        Assert.Equal(1, deserializer.Disposals);
    }

    [Fact]
    public void WrapperSerde_Dispose_SkipsAHalfThatIsNotDisposable()
    {
        List<string> log = new List<string>();
        DisposableDeserializer deserializer = new DisposableDeserializer(log);

        using (ISerde<string> serde = Serdes.SerdeFrom(new StringSerializer(), deserializer))
        {
            Assert.Empty(log);
        }

        Assert.Equal(new[] { "deserializer" }, log);
    }

    [Fact]
    public void WrapperSerde_Dispose_ASerializerThatThrowsLeavesTheDeserializerUndisposed_AsJavasClose()
    {
        List<string> log = new List<string>();
        DisposableSerializer serializer = new DisposableSerializer(log, throws: true);
        DisposableDeserializer deserializer = new DisposableDeserializer(log);
        ISerde<string> serde = Serdes.SerdeFrom(serializer, deserializer);

        InvalidOperationException e = Assert.Throws<InvalidOperationException>(serde.Dispose);

        Assert.Equal("serializer dispose failed", e.Message);
        Assert.Equal(new[] { "serializer" }, log);
        Assert.Equal(0, deserializer.Disposals);
    }

    [Fact]
    public void MockProducer_AKeylessRecordOfALongSerde_SendsANullKey()
    {
        using MockProducer<long?, string> producer =
            new MockProducer<long?, string>(Serdes.Long().Serializer(), Serdes.String().Serializer());

        producer.Send(new ProducerRecord<long?, string>(Topic, "keyless")).Get();
        producer.Send(new ProducerRecord<long?, string>(Topic, "keyed", 7L)).Get();

        Assert.Equal(new[] { true, false }, HistoryKeyIsNull(producer));
    }

    [Fact]
    public async Task AsyncMockProducer_AKeylessRecordOfALongSerde_SendsANullKey()
    {
        AsyncMockProducer<long?, string> producer =
            new AsyncMockProducer<long?, string>(Serdes.Long().Serializer(), Serdes.String().Serializer());
        try
        {
            await (await producer.Send(new ProducerRecord<long?, string>(Topic, "keyless"))).Get();
            await (await producer.Send(new ProducerRecord<long?, string>(Topic, "keyed", 7L))).Get();

            Assert.Equal(new[] { true, false }, HistoryKeyIsNull(producer));
        }
        finally
        {
            await TestTimeout.DisposeAsync(producer);
        }
    }

    [Fact]
    public void MockProducer_AByteBufferSerde_SendsANullKeyForNullOnly()
    {
        byte[] larger = Encoding.UTF8.GetBytes("--my string");
        using MockProducer<ReadOnlyMemory<byte>?, string> producer = new MockProducer<ReadOnlyMemory<byte>?, string>(
            Serdes.ByteBuffer().Serializer(), Serdes.String().Serializer());

        producer.Send(new ProducerRecord<ReadOnlyMemory<byte>?, string>(Topic, "null key")).Get();
        producer.Send(new ProducerRecord<ReadOnlyMemory<byte>?, string>(Topic, "empty key", ReadOnlyMemory<byte>.Empty)).Get();
        producer.Send(new ProducerRecord<ReadOnlyMemory<byte>?, string>(
            Topic, "slice key", new ReadOnlyMemory<byte>(larger, 2, larger.Length - 2))).Get();

        Assert.Equal(new[] { true, false, false }, HistoryKeyIsNull(producer));
    }

    [Fact]
    public void MockConsumer_AByteBufferSerde_ReadsCopiedBytes_AndNullOnlyForAnAbsentKeyOrValue()
    {
        byte[] value = Encoding.UTF8.GetBytes("my string");
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                Serdes.ByteBuffer().Deserializer(), Serdes.ByteBuffer().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), 0L);
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: value);
        consumer.AddRecord(Topic, Partition, offset: 1, key: Array.Empty<byte>(), value: null);

        ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);

        List<ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>> polled =
            new List<ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>>(records);
        Assert.Equal(2, polled.Count);
        Assert.False(polled[0].Key().HasValue);
        ReadOnlyMemory<byte> read = polled[0].Value()!.Value;
        Assert.Equal(value, read.ToArray());
        Assert.True(MemoryMarshal.TryGetArray(read, out ArraySegment<byte> segment));
        Assert.NotSame(value, segment.Array);
        Assert.True(polled[1].Key().HasValue);
        Assert.Equal(0, polled[1].Key()!.Value.Length);
        Assert.False(polled[1].Value().HasValue);
    }

    [Fact]
    public void MockConsumer_ATombstoneOfALongSerde_ReadsANullValue()
    {
        MockConsumer<string, long?> consumer =
            new MockConsumer<string, long?>(Serdes.String().Deserializer(), Serdes.Long().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), 0L);
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: LongBytes(0L));

        ConsumerRecords<string, long?> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);

        AssertTombstoneThenZero(records);
    }

    [Fact]
    public async Task AsyncMockConsumer_ATombstoneOfALongSerde_ReadsANullValue()
    {
        AsyncMockConsumer<string, long?> consumer =
            new AsyncMockConsumer<string, long?>(Serdes.String().Deserializer(), Serdes.Long().Deserializer());
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), 0L), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);
            consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: LongBytes(0L));

            ConsumerRecords<string, long?> records = default!;
            await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);

            AssertTombstoneThenZero(records);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public void MockConsumer_AValueForAVoidDeserializer_ThrowsRecordDeserializationException()
    {
        MockConsumer<string, object?> consumer =
            new MockConsumer<string, object?>(Serdes.String().Deserializer(), Serdes.Void().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), 0L);
        consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: new byte[5]);

        ConsumerRecords<string, object?> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: new byte[5]);
        RecordDeserializationException e = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        AssertTombstoneThenVoidError(records, e);
        Assert.Equal(1L, consumer.Position(new TopicPartition(Topic, Partition)));
    }

    [Fact]
    public async Task AsyncMockConsumer_AValueForAVoidDeserializer_ThrowsRecordDeserializationException()
    {
        AsyncMockConsumer<string, object?> consumer =
            new AsyncMockConsumer<string, object?>(Serdes.String().Deserializer(), Serdes.Void().Deserializer());
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, Partition) }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), 0L), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 0, key: null, value: null);
            consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: new byte[5]);

            ConsumerRecords<string, object?> records = default!;
            await TestTimeout.Run(async () => records = await consumer.Poll(s_pollTimeout), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: new byte[5]);
            RecordDeserializationException e =
                await Assert.ThrowsAsync<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

            AssertTombstoneThenVoidError(records, e);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public void AProducerAndAConsumerGivenTheHalvesOfOneSerde_EachDisposeOnlyTheirHalf()
    {
        DisposableSerializer serializer = new DisposableSerializer(new List<string>());
        DisposableDeserializer deserializer = new DisposableDeserializer(new List<string>());
        ISerde<string> serde = Serdes.SerdeFrom(serializer, deserializer);

        KafkaProducer<string, string> producer = new KafkaProducer<string, string>(
            new Dictionary<string, object?> { ["bootstrap.servers"] = "localhost:1" },
            Serdes.String().Serializer(),
            serde.Serializer());
        TestTimeout.Dispose(producer, s_deadline);

        Assert.Equal(1, serializer.Disposals);
        Assert.Equal(0, deserializer.Disposals);

        KafkaConsumer<string, string> consumer = new KafkaConsumer<string, string>(
            new Dictionary<string, object?>
            {
                ["bootstrap.servers"] = "localhost:1",
                ["group.protocol"] = "consumer",
                ["group.id"] = "serde-halves-group",
                ["enable.auto.commit"] = "false",
            },
            Serdes.String().Deserializer(),
            serde.Deserializer());
        TestTimeout.Dispose(consumer, s_deadline);

        Assert.Equal(1, serializer.Disposals);
        Assert.Equal(1, deserializer.Disposals);
    }

    private static ISerde<string> GetStringSerde(string encoder)
    {
        Dictionary<string, object?> serializerConfigs = new Dictionary<string, object?>
        {
            ["key.serializer.encoding"] = encoder,
        };
        ISerializer<string> serializer = Serdes.String().Serializer();
        serializer.Configure(serializerConfigs, true);

        Dictionary<string, object?> deserializerConfigs = new Dictionary<string, object?>
        {
            ["key.deserializer.encoding"] = encoder,
        };
        IDeserializer<string> deserializer = Serdes.String().Deserializer();
        deserializer.Configure(deserializerConfigs, true);

        return Serdes.SerdeFrom(serializer, deserializer);
    }

    private static ReadOnlyMemory<byte>? SerializeNull<T>(ISerde<T> serde)
    {
        using (serde)
        {
            return serde.Serializer().Serialize(Topic, default);
        }
    }

    // Java's round trip of a null passes the serializer's null to the deserializer; a consumer never calls one for a
    // null key or value, so the null row ends at the serializer. Java's headers half reads a byte[] value as it is and
    // any other through the ByteBuffer overload; both are the one span form here, given empty headers where Java gives
    // null, which .NET's headers form does not take.
    private static void AssertRoundtrips<T>(string name, IEnumerable<T?> values, Func<T?, T?, bool>? equal = null)
    {
        using ISerde<T> serde = Serdes.SerdeFrom<T>();
        foreach (T? value in values)
        {
            ReadOnlyMemory<byte>? serialized = serde.Serializer().Serialize(Topic, value);
            T? roundtrip = serialized is ReadOnlyMemory<byte> bytes
                ? serde.Deserializer().Deserialize(Topic, bytes.Span)
                : default;
            Assert.True(
                equal is null ? Equals(value, roundtrip) : equal(value, roundtrip),
                "Should get the original " + name + " after serialization and deserialization");

            T? withHeaders = value is byte[] array
                ? serde.Deserializer().Deserialize(Topic, new Headers(), array)
                : serialized is ReadOnlyMemory<byte> headersBytes
                    ? serde.Deserializer().Deserialize(Topic, new Headers(), headersBytes.Span)
                    : default;
            Assert.True(
                equal is null ? Equals(value, withHeaders) : equal(value, withHeaders),
                "Should get the original " + name + " after serialization and deserialization");
        }
    }

    private static bool SameBytes(byte[]? expected, byte[]? actual) =>
        expected is null ? actual is null : actual is not null && expected.AsSpan().SequenceEqual(actual);

    private static bool SameBytes(ReadOnlyMemory<byte>? expected, ReadOnlyMemory<byte>? actual) =>
        expected is ReadOnlyMemory<byte> e
            ? actual is ReadOnlyMemory<byte> a && e.Span.SequenceEqual(a.Span)
            : actual is null;

    private static void AssertFloatSizeRefused(byte[] data)
    {
        using ISerde<float?> serde = Serdes.Float();

        SerializationException e = Assert.Throws<SerializationException>(() => serde.Deserializer().Deserialize(Topic, data));

        Assert.Equal("Size of data received by Deserializer is not 4", e.Message);
    }

    private static float IntBitsToFloat(int bits) => BitConverter.ToSingle(BitConverter.GetBytes(bits), 0);

    private static int FloatToRawIntBits(float value) => BitConverter.ToInt32(BitConverter.GetBytes(value), 0);

    private static void AssertNewSerde<T, TSerde, TSerializer, TDeserializer>(Func<ISerde<T>> factory)
        where TSerde : Serdes.WrapperSerde<T>
        where TSerializer : ISerializer<T>
        where TDeserializer : IDeserializer<T>
    {
        using ISerde<T> first = factory();
        using ISerde<T> second = factory();

        Assert.IsType<TSerde>(first);
        Assert.IsType<TSerializer>(first.Serializer());
        Assert.IsType<TDeserializer>(first.Deserializer());
        Assert.NotSame(first, second);
        Assert.Same(first.Serializer(), first.Serializer());
        Assert.Same(first.Deserializer(), first.Deserializer());
    }

    private static byte[] LongBytes(long value) => Serdes.Long().Serializer().Serialize(Topic, value)!.Value.ToArray();

    private static void AssertTombstoneThenZero(ConsumerRecords<string, long?> records)
    {
        List<ConsumerRecord<string, long?>> polled = new List<ConsumerRecord<string, long?>>(records);
        Assert.Equal(2, polled.Count);
        Assert.Null(polled[0].Key());
        Assert.Null(polled[0].Value());
        Assert.Null(polled[1].Key());
        Assert.Equal(0L, polled[1].Value());
    }

    private static void AssertTombstoneThenVoidError(
        ConsumerRecords<string, object?> records, RecordDeserializationException e)
    {
        ConsumerRecord<string, object?> tombstone = Assert.Single(records);
        Assert.Equal(0L, tombstone.Offset());
        Assert.Null(tombstone.Value());
        Assert.Equal(RecordDeserializationException.DeserializationExceptionOrigin.VALUE, e.Origin());
        Assert.Equal(1L, e.Offset());
        Assert.Equal(
            "Error deserializing VALUE for partition " + Topic + "-0 at offset 1. If needed, please seek past the record "
                + "to continue consumption.",
            e.Message);
        IllegalArgumentException cause = Assert.IsType<IllegalArgumentException>(e.InnerException);
        Assert.Equal("Data should be null for a VoidDeserializer.", cause.Message);
    }

    // Whether each record of the core mock's history has a null key. Only the pointer is read: a record holds its
    // key's bytes by reference, and those of a sent record are the producer's, released once the send returned.
    private static bool[] HistoryKeyIsNull(object producer)
    {
        NativeProducer native = (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;
        SafeProducerHandle handle = native.Handle;
        bool added = false;
        try
        {
            IntPtr mock = handle.DangerousAddRefClass(ref added);
            IntPtr outErr = IntPtr.Zero;
            IntPtr history = NativeMethods.MockProducerHistory(mock, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            try
            {
                int count = NativeMethods.ListSize(history, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                bool[] keyIsNull = new bool[count];
                for (int i = 0; i < count; i++)
                {
                    IntPtr record = NativeMethods.ListGet(history, i, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    IntPtr key = ProducerRecordBuildTests.RecordNative.Key(record, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    keyIsNull[i] = key == IntPtr.Zero;
                }

                return keyIsNull;
            }
            finally
            {
                IntPtr destroyErr = IntPtr.Zero;
                NativeMethods.ListDestroy(history, ref destroyErr);
                KafkaException.ThrowIfFfiFailed(ref destroyErr);
            }
        }
        finally
        {
            if (added)
            {
                handle.DangerousRelease();
            }
        }
    }

    /// <summary>A type no built-in serde handles, as Java's test's <c>DummyClass</c>.</summary>
    private sealed class DummyClass
    {
    }

    /// <summary>A serializer that logs its disposals and can throw from one.</summary>
    private sealed class DisposableSerializer : ISerializer<string>, IDisposable
    {
        private readonly List<string> _log;
        private readonly bool _throws;

        internal DisposableSerializer(List<string> log, bool throws = false)
        {
            _log = log;
            _throws = throws;
        }

        internal int Disposals { get; private set; }

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => null;

        public void Dispose()
        {
            Disposals++;
            _log.Add("serializer");
            if (_throws)
            {
                throw new InvalidOperationException("serializer dispose failed");
            }
        }
    }

    /// <summary>A deserializer that logs its disposals.</summary>
    private sealed class DisposableDeserializer : IDeserializer<string>, IDisposable
    {
        private readonly List<string> _log;

        internal DisposableDeserializer(List<string> log) => _log = log;

        internal int Disposals { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public void Dispose()
        {
            Disposals++;
            _log.Add("deserializer");
        }
    }
}
