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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The typed send on both producer mocks: a typed round-trip, the serializer invoked on a null key or value as
/// Java's is, a nullable value type's tombstone, and a serializer throw leaving <c>Send</c> synchronously as it is,
/// with nothing sent. The mock exposes only a history count, so the
/// serialized bytes are not read back.
/// </summary>
public sealed class PublicProducerTypedSendTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "typed-send-topic";

    [Fact]
    public void Sync_TypedRoundTrip_StringLong_Succeeds()
    {
        MockProducer<string, long?> producer =
            new MockProducer<string, long?>(Serdes.String().Serializer(), Serdes.Long().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord<string, long?>(Topic, 42L, "k", partition: 3)).Get(),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic());
        Assert.Equal(3, metadata.Partition());
        Assert.Equal(0L, metadata.Offset());
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task Async_TypedRoundTrip_StringLong_Succeeds()
    {
        AsyncMockProducer<string, long?> producer =
            new AsyncMockProducer<string, long?>(Serdes.String().Serializer(), Serdes.Long().Serializer());
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);

        RecordMetadata metadata = null!;
        await TestTimeout.Run(
            async () => metadata = await producer.Send(new ProducerRecord<string, long?>(Topic, 42L, "k", partition: 3)).Delivery(),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic());
        Assert.Equal(3, metadata.Partition());
        Assert.Equal(0L, metadata.Offset());
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public void Sync_TypedRoundTrip_ByteArray_Succeeds()
    {
        // <byte[], byte[]> with Serdes.ByteArray() is the bytes surface.
        MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord<byte[], byte[]>(
                Topic, Encoding.UTF8.GetBytes("v"), Encoding.UTF8.GetBytes("k"), partition: 1)).Get(),
            s_deadline);

        Assert.Equal(1, metadata.Partition());
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public void Sync_NullKeyAndValue_InvokesSerializer_ThenSends()
    {
        RecordingSerializer<string> keySer = new RecordingSerializer<string>(Serdes.String().Serializer());
        RecordingSerializer<string> valueSer = new RecordingSerializer<string>(Serdes.String().Serializer());
        MockProducer<string, string> producer =
            new MockProducer<string, string>(keySer, valueSer);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // The serializer is invoked on a null key and value, as Java's KafkaProducer does; null bytes are absent.
        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord<string, string>(Topic, value: null, key: null)).Get(),
            s_deadline);

        Assert.Equal(1, keySer.InvocationCount);
        Assert.True(keySer.LastDatumWasNull);
        Assert.Equal(1, valueSer.InvocationCount);
        Assert.True(valueSer.LastDatumWasNull);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task Async_NullValue_InvokesSerializer_ThenSends()
    {
        RecordingSerializer<string> valueSer = new RecordingSerializer<string>(Serdes.String().Serializer());
        AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(Serdes.String().Serializer(), valueSer);
        await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);

        await TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: null, key: "k")).Delivery(),
            s_deadline);

        Assert.Equal(1, valueSer.InvocationCount);
        Assert.True(valueSer.LastDatumWasNull);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public void Sync_EmptyValue_InvokesSerializer_ThenSends()
    {
        RecordingSerializer<string> valueSer = new RecordingSerializer<string>(Serdes.String().Serializer());
        MockProducer<string, string> producer =
            new MockProducer<string, string>(Serdes.String().Serializer(), valueSer);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // An empty string serializes to a present, empty byte[], distinct from absent.
        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: string.Empty, key: "k")).Get(),
            s_deadline);

        Assert.Equal(1, valueSer.InvocationCount);
        Assert.False(valueSer.LastDatumWasNull);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public void Sync_ValueTypeTombstone_NullDistinctFromDefault()
    {
        NullableInt64Serializer valueSer = new NullableInt64Serializer();
        MockProducer<string, long?> producer =
            new MockProducer<string, long?>(Serdes.String().Serializer(), valueSer);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // A null long? is a tombstone and a present 0L a value; the serializer is invoked for both.
        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, long?>(Topic, value: null, key: "k")).Get(),
            s_deadline);
        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, long?>(Topic, value: 0L, key: "k")).Get(),
            s_deadline);

        Assert.Equal(2, valueSer.InvocationCount);
        Assert.Equal(2, producer.HistoryCount());
    }

    [Fact]
    public void Sync_ValueSerializerThrows_RethrowsItSynchronously()
    {
        ThrowingSerializer<string> valueSer = new ThrowingSerializer<string>();
        MockProducer<string, string> producer =
            new MockProducer<string, string>(Serdes.String().Serializer(), valueSer);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        ThrowingSerializer<string>.BoomException ex = Assert.Throws<ThrowingSerializer<string>.BoomException>(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k")));

        Assert.Same(valueSer.LastThrown, ex);

        // The throw is before the native send, so nothing was sent.
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public void Sync_KeySerializerThrows_RethrowsItSynchronously()
    {
        ThrowingSerializer<string> keySer = new ThrowingSerializer<string>();
        MockProducer<string, string> producer =
            new MockProducer<string, string>(keySer, Serdes.String().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        ThrowingSerializer<string>.BoomException ex = Assert.Throws<ThrowingSerializer<string>.BoomException>(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k")));

        Assert.Same(keySer.LastThrown, ex);
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public void Async_ValueSerializerThrows_ThrowsSynchronouslyBeforeReturningTask()
    {
        ThrowingSerializer<string> valueSer = new ThrowingSerializer<string>();
        AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(Serdes.String().Serializer(), valueSer);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // The async Send serializes before it returns, so the throw leaves the call (Assert.Throws, not
        // ThrowsAsync); the statement lambda binds Action, not the obsolete Func<Task>.
        ThrowingSerializer<string>.BoomException ex = Assert.Throws<ThrowingSerializer<string>.BoomException>(() =>
        {
            _ = producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k"));
        });

        Assert.Same(valueSer.LastThrown, ex);

        // Nothing was sent.
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public void BuiltInSerdes_NullInput_MapToTombstone()
    {
        // The tombstone path relies on the reference-type built-in serdes returning null for null, as Java's do, and
        // on Serdes.Void() always returning null; a value type cannot be null.
        Assert.Null(Serdes.String().Serializer().Serialize(Topic, null!));
        Assert.Null(Serdes.ByteArray().Serializer().Serialize(Topic, null!));
        Assert.Null(Serdes.Void().Serializer().Serialize(Topic, null));
        Assert.Null(Serdes.Void().Serializer().Serialize(Topic, new object()));
        Assert.NotNull(Serdes.Long().Serializer().Serialize(Topic, 0L));
    }

    [Fact]
    public void ByteArraySerde_IsZeroCopyIdentity()
    {
        // Serdes.ByteArray() returns the caller's array, so the bytes send path adds no value-sized copy.
        byte[] value = Encoding.UTF8.GetBytes("value");
        ReadOnlyMemory<byte>? serialized = Serdes.ByteArray().Serializer().Serialize(Topic, value);
        Assert.True(serialized.HasValue);
        Assert.True(MemoryMarshal.TryGetArray(serialized.Value, out ArraySegment<byte> segment));
        Assert.Same(value, segment.Array);
        Assert.Equal(0, segment.Offset);
        Assert.Equal(value.Length, segment.Count);
    }
}
