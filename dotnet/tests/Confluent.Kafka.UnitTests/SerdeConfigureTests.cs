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
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>configure</c> defaults of <c>Serializer</c>, <c>Deserializer</c> and <c>Serde</c>: the extension methods
/// run the object's <c>Configure</c>, or nothing; <see cref="Serdes.WrapperSerde{T}"/> configures its serializer, then
/// its deserializer; the String classes read Java's encoding keys, with Java's precedence and bytes, and refuse an
/// unknown name; the UUID classes store the name and refuse it at use, with Java's messages; a configured built-in is
/// not shared; and no client calls <c>Configure</c> on a serde it is given.
/// </summary>
public sealed class SerdeConfigureTests
{
    private const string Topic = "serde-configure";

    private static readonly Guid s_uuid = new Guid("0f14d0ab-9605-4a62-a9e4-5ed26688389b");

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void SerializerExtensions_Configure_CallsTheObjectsConfigure()
    {
        List<string> log = new List<string>();
        ConfigurableSerializer serializer = new ConfigurableSerializer(log);
        Dictionary<string, object?> configs = new Dictionary<string, object?> { ["a"] = 1 };

        ((ISerializer<string>)serializer).Configure(configs, true);

        Assert.Same(configs, Assert.Single(serializer.Configured).Configs);
        Assert.True(serializer.Configured[0].IsKey);
        Assert.Equal(new[] { "serializer" }, log);
    }

    [Fact]
    public void SerializerExtensions_Configure_RunsJavasDefault_ForASerializerWithoutTheInterface()
    {
        StrayConfigureSerializer serializer = new StrayConfigureSerializer();

        ((ISerializer<string>)serializer).Configure(new Dictionary<string, object?>(), false);
        ((ISerializer<string>)serializer).Configure(null!, false);

        Assert.Equal(0, serializer.ConfigureCalls);
    }

    [Fact]
    public void SerializerExtensions_Configure_NullSerializer_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => SerializerExtensions.Configure<string>(null!, new Dictionary<string, object?>(), true));

        Assert.Equal("serializer", e.ParamName);
    }

    [Fact]
    public void DeserializerExtensions_Configure_CallsTheObjectsConfigure()
    {
        List<string> log = new List<string>();
        ConfigurableDeserializer deserializer = new ConfigurableDeserializer(log);
        Dictionary<string, object?> configs = new Dictionary<string, object?> { ["a"] = 1 };

        ((IDeserializer<string>)deserializer).Configure(configs, false);

        Assert.Same(configs, Assert.Single(deserializer.Configured).Configs);
        Assert.False(deserializer.Configured[0].IsKey);
        Assert.Equal(new[] { "deserializer" }, log);
    }

    [Fact]
    public void DeserializerExtensions_Configure_RunsJavasDefault_ForADeserializerWithoutTheInterface()
    {
        StrayConfigureDeserializer deserializer = new StrayConfigureDeserializer();

        ((IDeserializer<string>)deserializer).Configure(new Dictionary<string, object?>(), true);
        ((IDeserializer<string>)deserializer).Configure(null!, true);

        Assert.Equal(0, deserializer.ConfigureCalls);
    }

    [Fact]
    public void DeserializerExtensions_Configure_NullDeserializer_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => DeserializerExtensions.Configure<string>(null!, new Dictionary<string, object?>(), true));

        Assert.Equal("deserializer", e.ParamName);
    }

    [Fact]
    public void SerdeExtensions_Configure_NullSerde_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => SerdeExtensions.Configure<string>(null!, new Dictionary<string, object?>(), true));

        Assert.Equal("serde", e.ParamName);
    }

    [Fact]
    public void WrapperSerde_Configure_ConfiguresTheSerializerThenTheDeserializer()
    {
        List<string> log = new List<string>();
        ConfigurableSerializer serializer = new ConfigurableSerializer(log);
        ConfigurableDeserializer deserializer = new ConfigurableDeserializer(log);
        ISerde<string> serde = Serdes.SerdeFrom(serializer, deserializer);
        Dictionary<string, object?> configs = new Dictionary<string, object?> { ["a"] = 1 };

        serde.Configure(configs, true);

        Assert.IsAssignableFrom<ISerdeWithConfigure<string>>(serde);
        Assert.Equal(new[] { "serializer", "deserializer" }, log);
        Assert.Same(configs, Assert.Single(serializer.Configured).Configs);
        Assert.Same(configs, Assert.Single(deserializer.Configured).Configs);
        Assert.True(serializer.Configured[0].IsKey);
        Assert.True(deserializer.Configured[0].IsKey);
    }

    [Fact]
    public void WrapperSerde_Configure_LeavesAHalfWithoutConfigureAsItIs()
    {
        List<string> log = new List<string>();
        StrayConfigureSerializer serializer = new StrayConfigureSerializer();
        ConfigurableDeserializer deserializer = new ConfigurableDeserializer(log);
        ISerde<string> serde = new Serdes.WrapperSerde<string>(serializer, deserializer);

        serde.Configure(new Dictionary<string, object?>(), false);

        Assert.Equal(0, serializer.ConfigureCalls);
        Assert.Equal(new[] { "deserializer" }, log);
    }

    [Fact]
    public void SerdeExtensions_Configure_ASerdeWithoutTheInterface_RunsJavasDefault_AndLeavesItsHalvesAsTheyAre()
    {
        List<string> log = new List<string>();
        ConfigurableSerializer serializer = new ConfigurableSerializer(log);
        ConfigurableDeserializer deserializer = new ConfigurableDeserializer(log);
        using PlainSerde serde = new PlainSerde(serializer, deserializer);

        ((ISerde<string>)serde).Configure(new Dictionary<string, object?>(), true);

        Assert.Empty(log);
        Assert.Equal(0, serde.ConfigureCalls);
    }

    [Fact]
    public void Serdes_String_ConfiguringOneDoesNotChangeAnother()
    {
        using ISerde<string> configured = Serdes.String();
        using ISerde<string> other = Serdes.String();

        configured.Configure(new Dictionary<string, object?> { ["serializer.encoding"] = "UTF-16" }, false);

        Assert.Equal(Hex("FE FF 00 78"), Bytes(configured.Serializer().Serialize(Topic, "x")));
        Assert.Equal(Hex("78"), Bytes(other.Serializer().Serialize(Topic, "x")));
    }

    [Fact]
    public void StringSerde_Configure_SetsBothHalves_ThroughTheWrapperSerde()
    {
        using ISerde<string> serde = Serdes.String();

        serde.Configure(
            new Dictionary<string, object?>
            {
                ["key.serializer.encoding"] = "UTF-16",
                ["key.deserializer.encoding"] = "UTF-16",
            },
            true);

        byte[] bytes = Bytes(serde.Serializer().Serialize(Topic, "hi"))!;
        Assert.Equal(Hex("FE FF 00 68 00 69"), bytes);
        Assert.Equal("hi", serde.Deserializer().Deserialize(Topic, bytes));
    }

    [Fact]
    public void StringSerializer_TheKeyOrValueKey_TakesPrecedenceOverSerializerEncoding()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["serializer.encoding"] = "UTF-16",
            ["key.serializer.encoding"] = "US-ASCII",
        };

        Assert.Equal(Hex("3F"), Bytes(Configured(new StringSerializer(), configs, isKey: true).Serialize(Topic, "\u00E9")));
        Assert.Equal(Hex("FE FF 00 E9"), Bytes(Configured(new StringSerializer(), configs, isKey: false).Serialize(Topic, "\u00E9")));
        StringSerializer valueOnly = Configured(
            new StringSerializer(), new Dictionary<string, object?> { ["value.serializer.encoding"] = "UTF-16" }, isKey: false);
        Assert.Equal(Hex("FE FF 00 E9"), Bytes(valueOnly.Serialize(Topic, "\u00E9")));
    }

    [Fact]
    public void StringDeserializer_TheKeyOrValueKey_TakesPrecedenceOverDeserializerEncoding()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["deserializer.encoding"] = "UTF-16",
            ["value.deserializer.encoding"] = "ISO-8859-1",
        };

        Assert.Equal("\u00FF", Configured(new StringDeserializer(), configs, isKey: false).Deserialize(Topic, Hex("FF")));
        Assert.Equal("x", Configured(new StringDeserializer(), configs, isKey: true).Deserialize(Topic, Hex("FE FF 00 78")));
        Assert.Equal(
            "x",
            Configured(new StringDeserializer(), new Dictionary<string, object?> { ["key.deserializer.encoding"] = "UTF-16" }, isKey: true)
                .Deserialize(Topic, Hex("00 78")));
    }

    [Fact]
    public void StringSerializer_ANullKeyOrValueKey_FallsBackToSerializerEncoding()
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["value.serializer.encoding"] = null,
            ["serializer.encoding"] = "UTF-16",
        };

        Assert.Equal(Hex("FE FF 00 78"), Bytes(Configured(new StringSerializer(), configs, isKey: false).Serialize(Topic, "x")));
    }

    [Fact]
    public void StringSerializer_ANonStringValue_IsIgnored_AndStopsTheFallback_AsJavasInstanceofString()
    {
        Dictionary<string, object?> blocking = new Dictionary<string, object?>
        {
            ["value.serializer.encoding"] = 16,
            ["serializer.encoding"] = "UTF-16",
        };
        Dictionary<string, object?> nonString = new Dictionary<string, object?>
        {
            ["serializer.encoding"] = Encoding.Unicode,
        };

        Assert.Equal(Hex("78"), Bytes(Configured(new StringSerializer(), blocking, isKey: false).Serialize(Topic, "x")));
        Assert.Equal(Hex("78"), Bytes(Configured(new StringSerializer(), nonString, isKey: true).Serialize(Topic, "x")));
        Assert.Equal(Hex("78"), Bytes(Configured(new StringSerializer(), new Dictionary<string, object?>(), isKey: true).Serialize(Topic, "x")));
    }

    [Fact]
    public void StringDeserializer_ANonStringValue_IsIgnored_AndStopsTheFallback_AsJavasInstanceofString()
    {
        Dictionary<string, object?> blocking = new Dictionary<string, object?>
        {
            ["key.deserializer.encoding"] = false,
            ["deserializer.encoding"] = "UTF-16",
        };

        Assert.Equal("x", Configured(new StringDeserializer(), blocking, isKey: true).Deserialize(Topic, Hex("78")));
    }

    [Fact]
    public void StringSerializer_AFailedConfigure_LeavesTheEncodingAsItWas()
    {
        StringSerializer serializer = Configured(
            new StringSerializer(), new Dictionary<string, object?> { ["serializer.encoding"] = "UTF-16" }, isKey: false);

        SerializationException e = Assert.Throws<SerializationException>(
            () => serializer.Configure(new Dictionary<string, object?> { ["serializer.encoding"] = "windows-1252" }, false));

        Assert.Equal("Unsupported encoding windows-1252", e.Message);
        Assert.Equal("windows-1252", Assert.IsType<IllegalArgumentException>(e.InnerException).Message);
        Assert.Equal(Hex("FE FF 00 78"), Bytes(serializer.Serialize(Topic, "x")));
    }

    [Fact]
    public void StringDeserializer_AnUnknownEncoding_ThrowsJavasMessage_WithTheLookupFailureAsItsCause()
    {
        StringDeserializer deserializer = new StringDeserializer();

        SerializationException e = Assert.Throws<SerializationException>(
            () => deserializer.Configure(new Dictionary<string, object?> { ["deserializer.encoding"] = "" }, true));

        Assert.Equal("Unsupported encoding ", e.Message);
        Assert.Equal(string.Empty, Assert.IsType<IllegalArgumentException>(e.InnerException).Message);
        Assert.Equal("x", deserializer.Deserialize(Topic, Hex("78")));
    }

    [Fact]
    public void TheStringAndUuidClasses_Configure_NullConfigs_ThrowsArgumentNullException()
    {
        Assert.Equal("configs", Assert.Throws<ArgumentNullException>(() => new StringSerializer().Configure(null!, true)).ParamName);
        Assert.Equal("configs", Assert.Throws<ArgumentNullException>(() => new StringDeserializer().Configure(null!, true)).ParamName);
        Assert.Equal("configs", Assert.Throws<ArgumentNullException>(() => new UUIDSerializer().Configure(null!, true)).ParamName);
        Assert.Equal("configs", Assert.Throws<ArgumentNullException>(() => new UUIDDeserializer().Configure(null!, true)).ParamName);
    }

    [Fact]
    public void StringSerde_PythonsUtf16AndUtf32Vectors()
    {
        StringSerializer utf16 = Configured(new StringSerializer(), SerializerEncoding("UTF-16"), isKey: false);
        Assert.Equal(Hex("FE FF 00 6D 00 79 00 20 00 73 00 74 00 72 00 69 00 6E 00 67"), Bytes(utf16.Serialize(Topic, "my string")));
        Assert.Equal(Hex("FE FF 00 68 00 69"), Bytes(utf16.Serialize(Topic, "hi")));
        Assert.Equal(Array.Empty<byte>(), Bytes(utf16.Serialize(Topic, string.Empty)));
        Assert.Equal(Hex("00 00 00 61"), Bytes(Configured(new StringSerializer(), SerializerEncoding("UTF-32"), isKey: false).Serialize(Topic, "a")));

        StringDeserializer readUtf16 = Configured(new StringDeserializer(), DeserializerEncoding("UTF-16"), isKey: false);
        Assert.Equal("a", readUtf16.Deserialize(Topic, Hex("00 61")));
        Assert.Equal("a", readUtf16.Deserialize(Topic, Hex("FF FE 61 00")));
        Assert.Equal("a", readUtf16.Deserialize(Topic, Hex("FE FF 00 61")));
        Assert.Equal("hi", readUtf16.Deserialize(Topic, Hex("FF FE 68 00 69 00")));
        StringDeserializer readUtf32 = Configured(new StringDeserializer(), DeserializerEncoding("UTF-32"), isKey: false);
        Assert.Equal("a", readUtf32.Deserialize(Topic, Hex("00 00 00 61")));
        Assert.Equal("a", readUtf32.Deserialize(Topic, Hex("FF FE 00 00 61 00 00 00")));
    }

    [Fact]
    public void StringSerde_UnmappableAndMalformedInput_FollowJava()
    {
        Assert.Equal(Hex("61 3F 62"), Bytes(new StringSerializer().Serialize(Topic, "a\uD800b")));
        Assert.Equal(Hex("3F"), Bytes(Configured(new StringSerializer(), SerializerEncoding("ascii"), isKey: false).Serialize(Topic, "\u00E9")));
        Assert.Equal("a\uFFFDb", new StringDeserializer().Deserialize(Topic, Hex("61 FF 62")));
    }

    [Fact]
    public void StringDeserializer_ASurrogateWrittenInUtf8_IsOneUFFFD_AsJavaReadsIt()
    {
        StringDeserializer[] deserializers =
        {
            new StringDeserializer(),
            Configured(new StringDeserializer(), DeserializerEncoding("UTF8"), isKey: false),
            (StringDeserializer)Serdes.String().Deserializer(),
        };

        foreach (StringDeserializer deserializer in deserializers)
        {
            Assert.Equal("\uFFFD", deserializer.Deserialize(Topic, Hex("ED A0 80")));
            Assert.Equal("\uFFFD\uFFFD", deserializer.Deserialize(Topic, Hex("ED A0 80 ED B0 80")));
            Assert.Equal("A\uFFFD", deserializer.Deserialize(Topic, Hex("41 ED A0")));
            Assert.Equal("\uFFFD", deserializer.Deserialize(Topic, Hex("ED BF")));
        }
    }

    [Fact]
    public void UUIDDeserializer_ASurrogateWrittenInUtf8_IsOneUFFFDInTheCauseMessage()
    {
        SerializationException e = Assert.Throws<SerializationException>(
            () => new UUIDDeserializer().Deserialize(Topic, Hex("ED A0 80")));

        Assert.Equal("Error parsing data into UUID", e.Message);
        Assert.Equal("Invalid UUID string: \uFFFD", Assert.IsType<IllegalArgumentException>(e.InnerException).Message);
    }

    [Fact]
    public void UUIDSerializer_WritesTheTextInTheConfiguredEncoding()
    {
        UUIDSerializer serializer = Configured(
            new UUIDSerializer(), new Dictionary<string, object?> { ["value.serializer.encoding"] = "UTF-16BE" }, isKey: false);
        UUIDDeserializer deserializer = Configured(
            new UUIDDeserializer(), new Dictionary<string, object?> { ["key.deserializer.encoding"] = "UTF-16BE" }, isKey: true);

        byte[] bytes = Bytes(serializer.Serialize(Topic, s_uuid))!;

        Assert.Equal(new UnicodeEncoding(bigEndian: true, byteOrderMark: false).GetBytes(s_uuid.ToString("D")), bytes);
        Assert.Equal(s_uuid, deserializer.Deserialize(Topic, bytes));
    }

    [Fact]
    public void UUIDSerializer_AnUnsupportedEncoding_IsStoredUnchecked_AndRefusedAtUse_WithJavasMessageAndNoCause()
    {
        UUIDSerializer serializer = new UUIDSerializer();
        serializer.Configure(new Dictionary<string, object?> { ["value.serializer.encoding"] = "no-such-charset" }, false);

        SerializationException e = Assert.Throws<SerializationException>(() => serializer.Serialize(Topic, Guid.NewGuid()));

        Assert.Equal("Error when serializing UUID to byte[] due to unsupported encoding no-such-charset", e.Message);
        Assert.Null(e.InnerException);
        Assert.True(serializer.Serialize(Topic, null) is null);
    }

    [Fact]
    public void UUIDDeserializer_AnUnsupportedEncoding_IsStoredUnchecked_AndRefusedAtUse_WithJavasConsumerPathMessage()
    {
        UUIDDeserializer deserializer = new UUIDDeserializer();
        deserializer.Configure(new Dictionary<string, object?> { ["deserializer.encoding"] = "no-such-charset" }, true);

        SerializationException e = Assert.Throws<SerializationException>(
            () => deserializer.Deserialize(Topic, Encoding.UTF8.GetBytes("1-1-1-1-1")));

        Assert.Equal("Error when deserializing ByteBuffer to UUID due to unsupported encoding no-such-charset", e.Message);
        Assert.Equal("no-such-charset", Assert.IsType<IllegalArgumentException>(e.InnerException).Message);
    }

    [Fact]
    public void UUIDDeserializer_TheEncodingFailure_ComesBeforeTheParse_AsInJava()
    {
        UUIDDeserializer deserializer = new UUIDDeserializer();
        deserializer.Configure(new Dictionary<string, object?> { ["deserializer.encoding"] = "windows-1252" }, false);

        SerializationException e = Assert.Throws<SerializationException>(
            () => deserializer.Deserialize(Topic, Encoding.UTF8.GetBytes("not a uuid")));

        Assert.Equal("Error when deserializing ByteBuffer to UUID due to unsupported encoding windows-1252", e.Message);
    }

    [Fact]
    public void KafkaProducer_NeverConfiguresItsSerializers()
    {
        List<string> log = new List<string>();
        ConfigurableSerializer key = new ConfigurableSerializer(log);
        ConfigurableSerializer value = new ConfigurableSerializer(log);
        KafkaProducer<string, string> producer = new KafkaProducer<string, string>(ProducerConfigs(), key, value);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        TestTimeout.Run(() => _ = producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k")), s_deadline);
        TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);

        Assert.Empty(log);
        Assert.Equal(2, key.SerializeCalls + value.SerializeCalls);
    }

    [Fact]
    public async Task AsyncKafkaProducer_NeverConfiguresItsSerializers()
    {
        List<string> log = new List<string>();
        ConfigurableSerializer key = new ConfigurableSerializer(log);
        ConfigurableSerializer value = new ConfigurableSerializer(log);
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(ProducerConfigs(), key, value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k"));
        await FutureAssert.ThrowsAsync<Common.Errors.TimeoutException>(
            () => TestTimeout.Run(() => firstStage.Delivery(), s_deadline));
        await TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);

        Assert.Empty(log);
        Assert.Equal(2, key.SerializeCalls + value.SerializeCalls);
    }

    [Fact]
    public void KafkaConsumer_NeverConfiguresItsDeserializers()
    {
        List<string> log = new List<string>();
        KafkaConsumer<string, string> consumer = new KafkaConsumer<string, string>(
            ConsumerConfigs(), new ConfigurableDeserializer(log), new ConfigurableDeserializer(log));
        TestTimeout.Dispose(consumer);

        Assert.Empty(log);
    }

    [Fact]
    public async Task AsyncKafkaConsumer_NeverConfiguresItsDeserializers()
    {
        List<string> log = new List<string>();
        AsyncKafkaConsumer<string, string> consumer = new AsyncKafkaConsumer<string, string>(
            ConsumerConfigs(), new ConfigurableDeserializer(log), new ConfigurableDeserializer(log));
        await TestTimeout.DisposeAsync(consumer);

        Assert.Empty(log);
    }

    private static T Configured<T>(T serde, Dictionary<string, object?> configs, bool isKey)
        where T : class
    {
        switch (serde)
        {
            case ISerializerWithConfigure<string> serializer:
                serializer.Configure(configs, isKey);
                break;
            case IDeserializerWithConfigure<string> deserializer:
                deserializer.Configure(configs, isKey);
                break;
            case ISerializerWithConfigure<Guid?> uuidSerializer:
                uuidSerializer.Configure(configs, isKey);
                break;
            default:
                ((IDeserializerWithConfigure<Guid?>)serde).Configure(configs, isKey);
                break;
        }

        return serde;
    }

    private static Dictionary<string, object?> SerializerEncoding(string name) =>
        new Dictionary<string, object?> { ["serializer.encoding"] = name };

    private static Dictionary<string, object?> DeserializerEncoding(string name) =>
        new Dictionary<string, object?> { ["deserializer.encoding"] = name };

    private static byte[]? Bytes(ReadOnlyMemory<byte>? memory) => memory?.ToArray();

    private static byte[] Hex(string hex)
    {
        string[] parts = hex.Split(' ');
        byte[] bytes = new byte[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            bytes[i] = Convert.ToByte(parts[i], 16);
        }

        return bytes;
    }

    private static Dictionary<string, object?> ProducerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
            ["serializer.encoding"] = "UTF-16",
            ["key.serializer.encoding"] = "UTF-16",
            ["value.serializer.encoding"] = "UTF-16",
        };

    private static Dictionary<string, object?> ConsumerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "serde-configure-group",
            ["enable.auto.commit"] = "false",
            ["deserializer.encoding"] = "UTF-16",
            ["key.deserializer.encoding"] = "UTF-16",
            ["value.deserializer.encoding"] = "UTF-16",
        };

    /// <summary>A string serializer whose <c>Configure</c> records each call in its own list and in a shared log.</summary>
    private sealed class ConfigurableSerializer : ISerializerWithConfigure<string>
    {
        private readonly List<string> _log;

        internal ConfigurableSerializer(List<string> log) => _log = log;

        internal List<(IReadOnlyDictionary<string, object?> Configs, bool IsKey)> Configured { get; } =
            new List<(IReadOnlyDictionary<string, object?> Configs, bool IsKey)>();

        internal int SerializeCalls { get; private set; }

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey)
        {
            Configured.Add((configs, isKey));
            _log.Add("serializer");
        }

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data)
        {
            SerializeCalls++;
            return data is null ? (ReadOnlyMemory<byte>?)null : Encoding.UTF8.GetBytes(data);
        }
    }

    /// <summary>A string deserializer whose <c>Configure</c> records each call in its own list and in a shared log.</summary>
    private sealed class ConfigurableDeserializer : IDeserializerWithConfigure<string>
    {
        private readonly List<string> _log;

        internal ConfigurableDeserializer(List<string> log) => _log = log;

        internal List<(IReadOnlyDictionary<string, object?> Configs, bool IsKey)> Configured { get; } =
            new List<(IReadOnlyDictionary<string, object?> Configs, bool IsKey)>();

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey)
        {
            Configured.Add((configs, isKey));
            _log.Add("deserializer");
        }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => Encoding.UTF8.GetString(data.ToArray());
    }

    /// <summary>A serializer with a public <c>Configure</c> but not the interface that makes the extension call it.</summary>
    private sealed class StrayConfigureSerializer : ISerializer<string>
    {
        internal int ConfigureCalls { get; private set; }

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => ConfigureCalls++;

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => null;
    }

    /// <summary>A deserializer with a public <c>Configure</c> but not the interface that makes the extension call it.</summary>
    private sealed class StrayConfigureDeserializer : IDeserializer<string>
    {
        internal int ConfigureCalls { get; private set; }

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => ConfigureCalls++;

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;
    }

    /// <summary>A serde with a public <c>Configure</c> but not <see cref="ISerdeWithConfigure{T}"/>.</summary>
    private sealed class PlainSerde : ISerde<string>
    {
        private readonly ISerializer<string> _serializer;
        private readonly IDeserializer<string> _deserializer;

        internal PlainSerde(ISerializer<string> serializer, IDeserializer<string> deserializer)
        {
            _serializer = serializer;
            _deserializer = deserializer;
        }

        internal int ConfigureCalls { get; private set; }

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => ConfigureCalls++;

        public ISerializer<string> Serializer() => _serializer;

        public IDeserializer<string> Deserializer() => _deserializer;

        public void Dispose()
        {
        }
    }
}

