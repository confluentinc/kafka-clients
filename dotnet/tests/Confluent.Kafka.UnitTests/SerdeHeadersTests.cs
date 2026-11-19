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
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's headers overloads of <c>Serializer</c> and <c>Deserializer</c>: the extension methods run the object's
/// headers form, or the form without headers; the real producers call the key's, then the value's serializer's headers
/// form with the record's own headers, which they make read-only after it, and the mock producers call each with a new,
/// empty headers object, as Java's <c>MockProducer</c> does; the two mock consumers call the key's and the value's
/// deserializer's headers form with the headers the record then carries, and that a
/// <see cref="RecordDeserializationException"/> carries; a serializer or deserializer without the headers form gets the
/// call without headers. The real consumers need a broker: <c>SerdeHeadersBrokerTests</c>.
/// </summary>
public sealed class SerdeHeadersTests
{
    private const string Topic = "serde-headers";

    private const string SchemaId = "schema-id";

    private const string KeyId = "key-id";

    private const string Closed = "RecordHeaders has been closed.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromSeconds(5);

    private static readonly TopicPartition s_partition = new TopicPartition(Topic, 0);

    [Fact]
    public void SerializerExtensions_Serialize_CallsTheHeadersForm_OfASerializerWithHeaders()
    {
        HeaderAddingSerializer serializer = new HeaderAddingSerializer();
        Headers headers = new Headers();

        ReadOnlyMemory<byte>? bytes = ((ISerializer<string>)serializer).Serialize(Topic, headers, "v");

        Assert.Equal("v", Text(bytes));
        Assert.Same(headers, Assert.Single(serializer.Seen));
        Assert.Equal(Bytes("1"), headers.LastHeader(SchemaId)?.Value());
        Assert.Equal(0, serializer.ShortCalls);
    }

    [Fact]
    public void SerializerExtensions_Serialize_RunsJavasDefault_ForASerializerWithoutTheHeadersForm()
    {
        PlainSerializer serializer = new PlainSerializer();
        Headers headers = new Headers(new[] { new Header("h", Bytes("1")) });

        ReadOnlyMemory<byte>? bytes = serializer.Serialize(Topic, headers, "v");

        Assert.Equal("v", Text(bytes));
        Assert.Equal(new[] { (Topic, (string?)"v") }, serializer.Calls);
        Assert.Single(headers.ToArray());
    }

    [Fact]
    public void SerializerExtensions_Serialize_NullSerializer_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => SerializerExtensions.Serialize<string>(null!, Topic, new Headers(), "v"));

        Assert.Equal("serializer", e.ParamName);
    }

    [Fact]
    public void DeserializerExtensions_Deserialize_CallsTheHeadersForm_OfADeserializerWithHeaders()
    {
        HeaderReadingDeserializer deserializer = new HeaderReadingDeserializer();
        Headers headers = new Headers(new[] { new Header(SchemaId, Bytes("7")) });

        string? value = ((IDeserializer<string>)deserializer).Deserialize(Topic, headers, Bytes("v"));

        Assert.Equal("v|7", value);
        Assert.Same(headers, Assert.Single(deserializer.Seen));
        Assert.Equal(0, deserializer.ShortCalls);
    }

    [Fact]
    public void DeserializerExtensions_Deserialize_RunsJavasDefault_ForADeserializerWithoutTheHeadersForm()
    {
        PlainDeserializer deserializer = new PlainDeserializer();

        string? value = deserializer.Deserialize(Topic, new Headers(new[] { new Header(SchemaId, Bytes("7")) }), Bytes("v"));

        Assert.Equal("v", value);
        Assert.Equal(new[] { (Topic, "v") }, deserializer.Calls);
    }

    [Fact]
    public void DeserializerExtensions_Deserialize_NullDeserializer_ThrowsArgumentNullException()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => DeserializerExtensions.Deserialize<string>(null!, Topic, new Headers(), Bytes("v")));

        Assert.Equal("deserializer", e.ParamName);
    }

    [Fact]
    public void KafkaProducer_CallsTheHeadersForm_WithTheRecordsHeaders_ThenMakesThemReadOnly()
    {
        PlainSerializer key = new PlainSerializer();
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        KafkaProducer<string, string> producer = new KafkaProducer<string, string>(Unreachable(), key, value);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<string, string> record = Record();

        TestTimeout.Run(() => _ = producer.Send(record), s_deadline);

        AssertSerializedWithTheRecordsHeaders(record, key, value);
        AssertReadOnly(record.Headers());
        TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public async Task AsyncKafkaProducer_CallsTheHeadersForm_WithTheRecordsHeaders_ThenMakesThemReadOnly()
    {
        PlainSerializer key = new PlainSerializer();
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(Unreachable(), key, value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record();

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);

        AssertSerializedWithTheRecordsHeaders(record, key, value);
        AssertReadOnly(record.Headers());
        Common.Errors.TimeoutException failure = await FutureAssert.ThrowsAsync<Common.Errors.TimeoutException>(
            () => TestTimeout.Run(() => firstStage.Delivery(), s_deadline));
        Assert.Equal(7, failure.Code);
        await TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public void KafkaProducer_CallsTheKeysHeadersForm_ThenTheValues_WithTheRecordsHeaders()
    {
        HeaderAddingSerializer key = new HeaderAddingSerializer(KeyId);
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        KafkaProducer<string, string> producer = new KafkaProducer<string, string>(Unreachable(), key, value);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<string, string> record = Record();

        TestTimeout.Run(() => _ = producer.Send(record), s_deadline);

        AssertBothSerializedWithTheRecordsHeaders(record, key, value);
        AssertReadOnly(record.Headers());
        TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public async Task AsyncKafkaProducer_CallsTheKeysHeadersForm_ThenTheValues_WithTheRecordsHeaders()
    {
        HeaderAddingSerializer key = new HeaderAddingSerializer(KeyId);
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(Unreachable(), key, value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record();

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);

        AssertBothSerializedWithTheRecordsHeaders(record, key, value);
        AssertReadOnly(record.Headers());
        await FutureAssert.ThrowsAsync<Common.Errors.TimeoutException>(
            () => TestTimeout.Run(() => firstStage.Delivery(), s_deadline));
        await TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public void MockProducer_CallsTheHeadersForm_WithNewEmptyHeaders_AndLeavesTheRecordsUnchanged()
    {
        PlainSerializer key = new PlainSerializer();
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        MockProducer<string, string> producer = new MockProducer<string, string>(key, value);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<string, string> record = Record();

        RecordMetadata metadata = null!;
        TestTimeout.Run(() => metadata = producer.Send(record).Get(), s_deadline);

        Assert.Equal(0L, metadata.Offset());
        AssertSerializedWithNewHeaders(record, key, value);
    }

    [Fact]
    public async Task AsyncMockProducer_CallsTheHeadersForm_WithNewEmptyHeaders_AndLeavesTheRecordsUnchanged()
    {
        PlainSerializer key = new PlainSerializer();
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        AsyncMockProducer<string, string> producer = new AsyncMockProducer<string, string>(key, value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record();

        RecordMetadata metadata = await TestTimeout.Run(() => producer.Send(record).Delivery(), s_deadline);

        Assert.Equal(0L, metadata.Offset());
        AssertSerializedWithNewHeaders(record, key, value);
    }

    [Fact]
    public void MockProducer_CallsTheKeysAndTheValuesHeadersForms_EachWithItsOwnNewEmptyHeaders()
    {
        HeaderAddingSerializer key = new HeaderAddingSerializer(KeyId);
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        MockProducer<string, string> producer = new MockProducer<string, string>(key, value);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<string, string> record = Record();

        TestTimeout.Run(() => _ = producer.Send(record).Get(), s_deadline);

        AssertBothSerializedWithNewHeaders(record, key, value);
    }

    [Fact]
    public async Task AsyncMockProducer_CallsTheKeysAndTheValuesHeadersForms_EachWithItsOwnNewEmptyHeaders()
    {
        HeaderAddingSerializer key = new HeaderAddingSerializer(KeyId);
        HeaderAddingSerializer value = new HeaderAddingSerializer();
        AsyncMockProducer<string, string> producer = new AsyncMockProducer<string, string>(key, value);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record();

        _ = await TestTimeout.Run(() => producer.Send(record).Delivery(), s_deadline);

        AssertBothSerializedWithNewHeaders(record, key, value);
    }

    [Fact]
    public void MockConsumer_CallsTheHeadersForm_WithTheHeadersTheRecordCarries()
    {
        PlainDeserializer key = new PlainDeserializer();
        HeaderReadingDeserializer value = new HeaderReadingDeserializer();
        using RecordBlocks blocks = new RecordBlocks();
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(key, value);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TestTimeout.Run(() => consumer.Assign(new[] { s_partition }), s_deadline);
        TestTimeout.Run(() => consumer.Seek(s_partition, 0), s_deadline);
        blocks.Add(AddRecord(NativeOf(consumer), Bytes("v")));

        ConsumerRecord<string, string> record = Assert.Single(consumer.Poll(s_pollTimeout));

        AssertDeserializedWithTheRecordsHeaders(record, key, value);
    }

    [Fact]
    public async Task AsyncMockConsumer_CallsTheHeadersForm_WithTheHeadersTheRecordCarries()
    {
        PlainDeserializer key = new PlainDeserializer();
        HeaderReadingDeserializer value = new HeaderReadingDeserializer();
        await using RecordBlocks blocks = new RecordBlocks();
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(key, value);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { s_partition }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(s_partition, 0), s_deadline);
        blocks.Add(AddRecord(NativeOf(consumer), Bytes("v")));

        ConsumerRecords<string, string> records = await TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline);

        AssertDeserializedWithTheRecordsHeaders(Assert.Single(records), key, value);
    }

    [Fact]
    public void MockConsumer_CallsTheKeysHeadersForm_WithTheHeadersTheRecordCarries()
    {
        HeaderReadingDeserializer key = new HeaderReadingDeserializer();
        HeaderReadingDeserializer value = new HeaderReadingDeserializer();
        using RecordBlocks blocks = new RecordBlocks();
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(key, value);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TestTimeout.Run(() => consumer.Assign(new[] { s_partition }), s_deadline);
        TestTimeout.Run(() => consumer.Seek(s_partition, 0), s_deadline);
        blocks.Add(AddRecord(NativeOf(consumer), Bytes("v")));

        ConsumerRecord<string, string> record = Assert.Single(consumer.Poll(s_pollTimeout));

        AssertBothDeserializedWithTheRecordsHeaders(record, key, value);
    }

    [Fact]
    public async Task AsyncMockConsumer_CallsTheKeysHeadersForm_WithTheHeadersTheRecordCarries()
    {
        HeaderReadingDeserializer key = new HeaderReadingDeserializer();
        HeaderReadingDeserializer value = new HeaderReadingDeserializer();
        await using RecordBlocks blocks = new RecordBlocks();
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(key, value);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { s_partition }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(s_partition, 0), s_deadline);
        blocks.Add(AddRecord(NativeOf(consumer), Bytes("v")));

        ConsumerRecords<string, string> records = await TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline);

        AssertBothDeserializedWithTheRecordsHeaders(Assert.Single(records), key, value);
    }

    [Fact]
    public void MockConsumer_AHeadersFormThatThrows_GivesTheExceptionTheHeadersItWasGiven()
    {
        HeaderReadingDeserializer value = new HeaderReadingDeserializer(fail: true);
        using RecordBlocks blocks = new RecordBlocks();
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(new PlainDeserializer(), value);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TestTimeout.Run(() => consumer.Assign(new[] { s_partition }), s_deadline);
        TestTimeout.Run(() => consumer.Seek(s_partition, 0), s_deadline);
        blocks.Add(AddRecord(NativeOf(consumer), Bytes("v")));

        RecordDeserializationException e = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        AssertTheFailureCarriesTheHeaders(e, value);
    }

    [Fact]
    public async Task AsyncMockConsumer_AHeadersFormThatThrows_GivesTheExceptionTheHeadersItWasGiven()
    {
        HeaderReadingDeserializer value = new HeaderReadingDeserializer(fail: true);
        await using RecordBlocks blocks = new RecordBlocks();
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(new PlainDeserializer(), value);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { s_partition }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(s_partition, 0), s_deadline);
        blocks.Add(AddRecord(NativeOf(consumer), Bytes("v")));

        RecordDeserializationException e = await Assert.ThrowsAsync<RecordDeserializationException>(
            () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));

        AssertTheFailureCarriesTheHeaders(e, value);
    }

    private static void AssertSerializedWithTheRecordsHeaders(
        ProducerRecord<string, string> record, PlainSerializer key, HeaderAddingSerializer value)
    {
        Assert.Equal(new[] { (Topic, (string?)"key") }, key.Calls);
        Assert.Same(record.Headers(), Assert.Single(value.Seen));
        Assert.Equal(new[] { false }, value.ReadOnlyWhenCalled);
        Assert.Equal(0, value.ShortCalls);
        Assert.Equal(new[] { "h", SchemaId }, Array.ConvertAll(record.Headers().ToArray(), header => header.Key()));
        Assert.Equal(Bytes("1"), record.Headers().LastHeader(SchemaId)?.Value());
    }

    private static void AssertBothSerializedWithTheRecordsHeaders(
        ProducerRecord<string, string> record, HeaderAddingSerializer key, HeaderAddingSerializer value)
    {
        Assert.Same(record.Headers(), Assert.Single(key.Seen));
        Assert.Same(record.Headers(), Assert.Single(value.Seen));
        Assert.Equal(new[] { "h" }, Assert.Single(key.KeysWhenCalled));
        Assert.Equal(new[] { "h", KeyId }, Assert.Single(value.KeysWhenCalled));
        Assert.Equal(0, key.ShortCalls);
        Assert.Equal(0, value.ShortCalls);
        Assert.Equal(new[] { "h", KeyId, SchemaId }, HeaderKeys(record.Headers()));
    }

    private static void AssertSerializedWithNewHeaders(
        ProducerRecord<string, string> record, PlainSerializer key, HeaderAddingSerializer value)
    {
        Assert.Equal(new[] { (Topic, (string?)"key") }, key.Calls);
        Assert.NotSame(record.Headers(), Assert.Single(value.Seen));
        Assert.Empty(Assert.Single(value.KeysWhenCalled));
        Assert.Equal(new[] { false }, value.ReadOnlyWhenCalled);
        Assert.Equal(0, value.ShortCalls);
        Assert.Equal(new[] { "h" }, HeaderKeys(record.Headers()));
        Assert.False(record.Headers().IsReadOnly);
    }

    private static void AssertBothSerializedWithNewHeaders(
        ProducerRecord<string, string> record, HeaderAddingSerializer key, HeaderAddingSerializer value)
    {
        Headers keyHeaders = Assert.Single(key.Seen);
        Headers valueHeaders = Assert.Single(value.Seen);
        Assert.NotSame(record.Headers(), keyHeaders);
        Assert.NotSame(record.Headers(), valueHeaders);
        Assert.NotSame(keyHeaders, valueHeaders);
        Assert.Empty(Assert.Single(key.KeysWhenCalled));
        Assert.Empty(Assert.Single(value.KeysWhenCalled));
        Assert.Equal(0, key.ShortCalls);
        Assert.Equal(0, value.ShortCalls);
        Assert.Equal(new[] { "h" }, HeaderKeys(record.Headers()));
        Assert.False(record.Headers().IsReadOnly);
    }

    private static void AssertReadOnly(Headers headers)
    {
        Assert.True(headers.IsReadOnly);
        IllegalStateException closed = Assert.Throws<IllegalStateException>(() => headers.Add("after", Bytes("x")));
        Assert.Equal(Closed, closed.Message);
    }

    private static void AssertDeserializedWithTheRecordsHeaders(
        ConsumerRecord<string, string> record, PlainDeserializer key, HeaderReadingDeserializer value)
    {
        Assert.Equal(new[] { (Topic, "k") }, key.Calls);
        Assert.Equal("k", record.Key());
        Assert.Equal("v|7", record.Value());
        Assert.Same(record.Headers(), Assert.Single(value.Seen));
        Assert.Equal(0, value.ShortCalls);
    }

    private static void AssertBothDeserializedWithTheRecordsHeaders(
        ConsumerRecord<string, string> record, HeaderReadingDeserializer key, HeaderReadingDeserializer value)
    {
        Assert.Equal("k|7", record.Key());
        Assert.Equal("v|7", record.Value());
        Assert.Same(record.Headers(), Assert.Single(key.Seen));
        Assert.Same(record.Headers(), Assert.Single(value.Seen));
        Assert.Equal(0, key.ShortCalls);
        Assert.Equal(0, value.ShortCalls);
    }

    private static void AssertTheFailureCarriesTheHeaders(RecordDeserializationException e, HeaderReadingDeserializer value)
    {
        Assert.Equal(RecordDeserializationException.DeserializationExceptionOrigin.VALUE, e.Origin());
        Headers seen = Assert.Single(value.Seen);
        Assert.Same(seen, e.Headers());
        Assert.Equal(Bytes("7"), seen.LastHeader(SchemaId)?.Value());
        Assert.Equal("bad v", e.InnerException?.Message);
    }

    private static IDisposable AddRecord(NativeConsumer consumer, byte[] value) =>
        ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, 0, offset: 0, key: Bytes("k"), value: value, (SchemaId, Bytes("7")));

    private static NativeConsumer NativeOf(IConsumerCommon consumer) =>
        (NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;

    private static ProducerRecord<string, string> Record() =>
        new ProducerRecord<string, string>(Topic, value: "value", key: "key", headers: new[] { new Header("h", Bytes("1")) }, partition: null);

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static string[] HeaderKeys(Headers headers) => Array.ConvertAll(headers.ToArray(), header => header.Key());

    private static string? Text(ReadOnlyMemory<byte>? bytes) =>
        bytes is ReadOnlyMemory<byte> memory ? Encoding.UTF8.GetString(memory.ToArray()) : null;

    /// <summary>A string serializer without the headers form, recording each call.</summary>
    private sealed class PlainSerializer : ISerializer<string>
    {
        internal List<(string Topic, string? Data)> Calls { get; } = new List<(string Topic, string? Data)>();

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data)
        {
            Calls.Add((topic, data));
            return data is null ? (ReadOnlyMemory<byte>?)null : Bytes(data);
        }
    }

    /// <summary>
    /// A string serializer whose headers form adds a header (the schema id unless another is named) and records the
    /// headers it was given, with their keys and read-only state at the call.
    /// </summary>
    private sealed class HeaderAddingSerializer : ISerializerWithHeaders<string>
    {
        private readonly string _header;

        internal HeaderAddingSerializer(string header = SchemaId) => _header = header;

        internal List<Headers> Seen { get; } = new List<Headers>();

        internal List<string[]> KeysWhenCalled { get; } = new List<string[]>();

        internal List<bool> ReadOnlyWhenCalled { get; } = new List<bool>();

        internal int ShortCalls { get; private set; }

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data)
        {
            ShortCalls++;
            return data is null ? (ReadOnlyMemory<byte>?)null : Bytes(data);
        }

        public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, string? data)
        {
            Seen.Add(headers);
            KeysWhenCalled.Add(HeaderKeys(headers));
            ReadOnlyWhenCalled.Add(headers.IsReadOnly);
            headers.Add(_header, Bytes("1"));
            return data is null ? (ReadOnlyMemory<byte>?)null : Bytes(data);
        }
    }

    /// <summary>A string deserializer without the headers form, recording each call.</summary>
    private sealed class PlainDeserializer : IDeserializer<string>
    {
        internal List<(string Topic, string Data)> Calls { get; } = new List<(string Topic, string Data)>();

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            string text = Encoding.UTF8.GetString(data.ToArray());
            Calls.Add((topic, text));
            return text;
        }
    }

    /// <summary>
    /// A string deserializer whose headers form appends the schema id header's value and records the headers it was
    /// given, or throws after recording them.
    /// </summary>
    private sealed class HeaderReadingDeserializer : IDeserializerWithHeaders<string>
    {
        private readonly bool _fail;

        internal HeaderReadingDeserializer(bool fail = false) => _fail = fail;

        internal List<Headers> Seen { get; } = new List<Headers>();

        internal int ShortCalls { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            ShortCalls++;
            return Encoding.UTF8.GetString(data.ToArray());
        }

        public string? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data)
        {
            Seen.Add(headers);
            string text = Encoding.UTF8.GetString(data.ToArray());
            if (_fail)
            {
                throw new FormatException("bad " + text);
            }

            byte[]? id = headers.LastHeader(SchemaId)?.Value();
            return text + "|" + (id is null ? "none" : Encoding.UTF8.GetString(id));
        }
    }

    /// <summary>The unmanaged key and value blocks of the records a test added to a mock, freed after the consumer.</summary>
    private sealed class RecordBlocks : IDisposable, IAsyncDisposable
    {
        private readonly List<IDisposable> _blocks = new List<IDisposable>();

        internal void Add(IDisposable block) => _blocks.Add(block);

        public void Dispose()
        {
            foreach (IDisposable block in _blocks)
            {
                block.Dispose();
            }

            _blocks.Clear();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return default;
        }
    }
}
