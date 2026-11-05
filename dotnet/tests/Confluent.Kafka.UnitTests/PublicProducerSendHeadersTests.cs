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
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Java.Lang;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A record's headers across a send: Java's <c>KafkaProducerTest.testHeadersSuccess</c> on both Kafka producers over
/// an unreachable broker (the headers can be changed before the send and are read-only after it, and the serializers
/// run first), a serializer failure leaving them writable, a sent record sent again, and Python's
/// <c>test_mock_producer_leaves_the_headers_writable</c> on both mocks.
/// </summary>
/// <remarks>
/// Java's <c>testHeadersFailure</c> is not translated: Java's <c>doSend</c> waits for the topic's metadata before it
/// serializes and makes the headers read-only, so a send that times out on the metadata leaves them writable; here the
/// serializers run, and the headers turn read-only, on the caller's thread before the record reaches the core, whose
/// metadata wait follows, so the state Java checks is not reached.
/// </remarks>
public sealed class PublicProducerSendHeadersTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "topic";

    private const string Closed = "RecordHeaders has been closed.";

    private const int TimeoutCode = 7;

    [Fact]
    public void KafkaProducer_HeadersSuccess()
    {
        List<(string Topic, string? Data)> calls = new List<(string Topic, string? Data)>();
        CallLog serializer = new CallLog(calls);
        KafkaProducer<string, string> producer = new KafkaProducer<string, string>(Unreachable(), serializer, serializer);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(Topic, "value", "key");

        record.Headers.Add(new Header("test", Bytes("header2")));
        TestTimeout.Run(() => _ = producer.Send(record), s_deadline);

        AssertSealedWithTheOriginalHeader(record);
        Assert.Equal(new[] { (Topic, (string?)"key"), (Topic, (string?)"value") }, calls);
        TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public async Task AsyncKafkaProducer_HeadersSuccess()
    {
        List<(string Topic, string? Data)> calls = new List<(string Topic, string? Data)>();
        CallLog serializer = new CallLog(calls);
        AsyncKafkaProducer<string, string> producer =
            new AsyncKafkaProducer<string, string>(Unreachable(), serializer, serializer);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(Topic, "value", "key");
        record.Headers.Add(new Header("test", Bytes("header2")));

        ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = producer.Send(record);
        AssertSealedWithTheOriginalHeader(record);
        Assert.Equal(new[] { (Topic, (string?)"key"), (Topic, (string?)"value") }, calls);
        await AssertMetadataTimeout(firstStage);
        await TestTimeout.Run(() => producer.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public void KafkaProducer_SerializerFailure_LeavesTheHeadersWritable()
    {
        KafkaProducer<string, string> producer = new KafkaProducer<string, string>(
            Unreachable(), new ThrowingSerializer<string>(), Serdes.String);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<string, string> record = Record();

        Assert.Throws<ThrowingSerializer<string>.BoomException>(() => producer.Send(record));

        AssertWritable(record);
    }

    [Fact]
    public async Task AsyncKafkaProducer_SerializerFailure_LeavesTheHeadersWritable()
    {
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(
            Unreachable(), Serdes.String, new ThrowingSerializer<string>());
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record();

        Assert.Throws<ThrowingSerializer<string>.BoomException>(() => { _ = producer.Send(record); });

        AssertWritable(record);
    }

    [Fact]
    public void KafkaProducer_ASentRecord_IsSentAgain()
    {
        KafkaProducer<string, string> producer = new KafkaProducer<string, string>(
            Unreachable(), Serdes.String, Serdes.String);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<string, string> record = Record();
        KafkaFuture<RecordMetadata> first = default;
        TestTimeout.Run(() => first = producer.Send(record), s_deadline);
        Assert.True(record.Headers.IsReadOnly);

        KafkaFuture<RecordMetadata> again = default;
        TestTimeout.Run(() => again = producer.Send(record), s_deadline);

        Assert.True(record.Headers.IsReadOnly);
        AssertMetadataTimeout(first);
        AssertMetadataTimeout(again);
    }

    [Fact]
    public async Task AsyncKafkaProducer_ASentRecord_IsSentAgain()
    {
        AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(
            Unreachable(), Serdes.String, Serdes.String);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<string, string> record = Record();
        ValueTask<AsyncKafkaFuture<RecordMetadata>> first = producer.Send(record);
        Assert.True(record.Headers.IsReadOnly);

        ValueTask<AsyncKafkaFuture<RecordMetadata>> again = producer.Send(record);

        Assert.True(record.Headers.IsReadOnly);
        await AssertMetadataTimeout(first);
        await AssertMetadataTimeout(again);
    }

    [Fact]
    public void MockProducer_LeavesTheHeadersWritable()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("t", Bytes("v"));

        RecordMetadata metadata = null!;
        TestTimeout.Run(() => metadata = producer.Send(record).Get(), s_deadline);

        Assert.Equal(0L, metadata.Offset);
        Assert.False(record.Headers.IsReadOnly);
        record.Headers.Add("k", Bytes("after"));
    }

    [Fact]
    public async Task AsyncMockProducer_LeavesTheHeadersWritable()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("t", Bytes("v"));

        RecordMetadata metadata = await TestTimeout.Run(() => producer.Send(record).Delivery(), s_deadline);

        Assert.Equal(0L, metadata.Offset);
        Assert.False(record.Headers.IsReadOnly);
        record.Headers.Add("k", Bytes("after"));
    }

    private static void AssertSealedWithTheOriginalHeader(ProducerRecord<string, string> record)
    {
        Assert.True(record.Headers.IsReadOnly);
        IllegalStateException closed = Assert.Throws<IllegalStateException>(
            () => record.Headers.Add(new Header("test", Bytes("test"))));
        Assert.Equal(Closed, closed.Message);

        Assert.Equal(Bytes("header2"), record.Headers.LastHeader("test")?.Value);
        Assert.Single(record.Headers.ToArray());
    }

    private static void AssertWritable(ProducerRecord<string, string> record)
    {
        Assert.False(record.Headers.IsReadOnly);
        record.Headers.Add("after", Bytes("x"));
        Assert.Equal(new[] { "h", "after" }, Array.ConvertAll(record.Headers.ToArray(), header => header.Key));
    }

    private static void AssertMetadataTimeout(KafkaFuture<RecordMetadata> future)
    {
        Common.Errors.TimeoutException failure = FutureAssert.Throws<Common.Errors.TimeoutException>(() => future.Get(s_deadline));
        Assert.Equal(TimeoutCode, failure.Code);
    }

    private static async Task AssertMetadataTimeout(ValueTask<AsyncKafkaFuture<RecordMetadata>> send)
    {
        Common.Errors.TimeoutException failure = await FutureAssert.ThrowsAsync<Common.Errors.TimeoutException>(
            () => TestTimeout.Run(() => send.Delivery(), s_deadline));
        Assert.Equal(TimeoutCode, failure.Code);
    }

    private static ProducerRecord<string, string> Record() =>
        new ProducerRecord<string, string>(Topic, "value", "key", headers: new[] { new Header("h", Bytes("1")) });

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>A string serializer that records each call's topic and datum.</summary>
    private sealed class CallLog : ISerializer<string>
    {
        private readonly List<(string Topic, string? Data)> _calls;

        internal CallLog(List<(string Topic, string? Data)> calls) => _calls = calls;

        public byte[]? Serialize(string topic, string data)
        {
            _calls.Add((topic, data));
            return data is null ? null : Encoding.UTF8.GetBytes(data);
        }
    }
}
