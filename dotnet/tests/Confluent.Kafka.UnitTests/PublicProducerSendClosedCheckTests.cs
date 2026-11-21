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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Send checks for a closed producer before it serializes, as Java's doSend does, on all four producer types: the
/// serializer is never invoked, so a stateful one takes no action for a doomed send, and its throw cannot mask
/// the IllegalStateException. As in Java's MockProducer.send, a closed producer, a fenced mock and an injected send
/// exception are all reported before the record is read.
/// </summary>
public sealed class PublicProducerSendClosedCheckTests
{
    private const string Topic = "closed-check-topic";

    private const string MockClosed = "MockProducer is already closed.";

    private const string RealClosed = "Cannot perform operation after producer has been closed";

    private const string MockFenced = "MockProducer is fenced.";

    private static Dictionary<string, object?> RealConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, value: Encoding.UTF8.GetBytes("v"), partition: 0, key: null);

    [Fact]
    public void SyncMockSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, value);
        TestTimeout.Dispose(producer);

        Assert.Throws<IllegalStateException>(() => producer.Send(Record()));
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void SyncRealSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        KafkaProducer<byte[], byte[]> producer = KafkaProducer<byte[], byte[]>.Create(RealConfig(), key, value);
        TestTimeout.Dispose(producer);

        Assert.Throws<IllegalStateException>(() => producer.Send(Record()));
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void AsyncMockSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(key.AsAsync(), value.AsAsync());
        TestTimeout.Dispose(producer);

        // Send throws synchronously (not an async method), so Assert.Throws is the overload.
        Assert.Throws<IllegalStateException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public async Task AsyncRealSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        AsyncKafkaProducer<byte[], byte[]> producer = await AsyncKafkaProducer<byte[], byte[]>.Create(RealConfig(), key.AsAsync(), value.AsAsync());
        TestTimeout.Dispose(producer);

        Assert.Throws<IllegalStateException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void SyncSend_AfterDispose_WithThrowingSerializer_SurfacesIllegalState()
    {
        ThrowingSerializer<byte[]> key = new ThrowingSerializer<byte[]>();
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, Serdes.ByteArray().Serializer());
        TestTimeout.Dispose(producer);

        Assert.Throws<IllegalStateException>(() => producer.Send(Record()));
        Assert.Equal(0, key.InvocationCount);
    }

    [Fact]
    public void AsyncSend_AfterDispose_WithThrowingSerializer_SurfacesIllegalState()
    {
        ThrowingSerializer<byte[]> key = new ThrowingSerializer<byte[]>();
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(key.AsAsync(), new AsyncByteArraySerializer());
        TestTimeout.Dispose(producer);

        Assert.Throws<IllegalStateException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
    }

    [Fact]
    public void Send_NullRecordOnClosedProducer_ThrowsIllegalState()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, Serdes.ByteArray().Serializer());
        TestTimeout.Dispose(producer);

        IllegalStateException ex = Assert.Throws<IllegalStateException>(() => producer.Send(null!));
        Assert.Equal(MockClosed, ex.Message);
        Assert.Equal(0, key.InvocationCount);
    }

    [Fact]
    public async Task EverySend_NullArgumentsOnClosedProducer_ThrowIllegalState_OnAllFourProducers()
    {
        ISerializer<byte[]> bytes = Serdes.ByteArray().Serializer();
        IAsyncSerializer<byte[]> asyncBytes = new AsyncByteArraySerializer();
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(bytes, bytes);
        AsyncMockProducer<byte[], byte[]> asyncMock = new AsyncMockProducer<byte[], byte[]>(asyncBytes, asyncBytes);
        KafkaProducer<byte[], byte[]> real = KafkaProducer<byte[], byte[]>.Create(RealConfig(), bytes, bytes);
        AsyncKafkaProducer<byte[], byte[]> asyncReal =
            await AsyncKafkaProducer<byte[], byte[]>.Create(RealConfig(), asyncBytes, asyncBytes);
        TestTimeout.Dispose(mock);
        TestTimeout.Dispose(asyncMock);
        TestTimeout.Dispose(real);
        TestTimeout.Dispose(asyncReal);

        AssertClosed(MockClosed, () => mock.Send(null!));
        AssertClosed(MockClosed, () => mock.Send(null!, null!));
        AssertClosed(MockClosed, () => { _ = asyncMock.Send(null!); });
        AssertClosed(MockClosed, () => { _ = asyncMock.Send(null!, (ICallback)null!); });
        AssertClosed(MockClosed, () => { _ = asyncMock.Send(null!, (IAsyncCallback)null!); });
        AssertClosed(RealClosed, () => real.Send(null!));
        AssertClosed(RealClosed, () => real.Send(null!, null!));
        AssertClosed(RealClosed, () => { _ = asyncReal.Send(null!); });
        AssertClosed(RealClosed, () => { _ = asyncReal.Send(null!, (ICallback)null!); });
        AssertClosed(RealClosed, () => { _ = asyncReal.Send(null!, (IAsyncCallback)null!); });
    }

    [Fact]
    public async Task EverySend_NullArgumentsOnAFencedMock_ReportTheFenceFirst()
    {
        ISerializer<byte[]> bytes = Serdes.ByteArray().Serializer();
        IAsyncSerializer<byte[]> asyncBytes = new AsyncByteArraySerializer();
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(bytes, bytes);
        AsyncMockProducer<byte[], byte[]> asyncMock = new AsyncMockProducer<byte[], byte[]>(asyncBytes, asyncBytes);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        using IDisposable asyncMockDisposal = TestTimeout.Disposal(asyncMock);
        mock.InitTransactions();
        mock.FenceProducer();
        await asyncMock.InitTransactions();
        asyncMock.FenceProducer();

        Assert.Equal(MockFenced, Assert.Throws<KafkaException>(() => mock.Send(null!)).Message);
        Assert.Equal(MockFenced, Assert.Throws<KafkaException>(() => mock.Send(null!, null!)).Message);
        Assert.Equal(MockFenced, (await Assert.ThrowsAsync<KafkaException>(() => asyncMock.Send(null!).AsTask())).Message);
        Assert.Equal(
            MockFenced,
            (await Assert.ThrowsAsync<KafkaException>(() => asyncMock.Send(null!, (ICallback)null!).AsTask())).Message);
        Assert.Equal(
            MockFenced,
            (await Assert.ThrowsAsync<KafkaException>(() => asyncMock.Send(null!, (IAsyncCallback)null!).AsTask())).Message);
    }

    [Fact]
    public async Task EverySend_NullArgumentsWithAnInjectedSendException_ThrowThatException()
    {
        ISerializer<byte[]> bytes = Serdes.ByteArray().Serializer();
        IAsyncSerializer<byte[]> asyncBytes = new AsyncByteArraySerializer();
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(bytes, bytes);
        AsyncMockProducer<byte[], byte[]> asyncMock = new AsyncMockProducer<byte[], byte[]>(asyncBytes, asyncBytes);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        using IDisposable asyncMockDisposal = TestTimeout.Disposal(asyncMock);
        KafkaException injected = new KafkaException("injected");
        mock.SetSendException(injected);
        asyncMock.SetSendException(injected);

        Assert.Same(injected, Assert.Throws<KafkaException>(() => mock.Send(null!)));
        Assert.Same(injected, Assert.Throws<KafkaException>(() => mock.Send(null!, null!)));
        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => asyncMock.Send(null!).AsTask()));
        Assert.Same(
            injected, await Assert.ThrowsAsync<KafkaException>(() => asyncMock.Send(null!, (ICallback)null!).AsTask()));
        Assert.Same(
            injected,
            await Assert.ThrowsAsync<KafkaException>(() => asyncMock.Send(null!, (IAsyncCallback)null!).AsTask()));
    }

    private static void AssertClosed(string message, Action send)
    {
        IllegalStateException ex = Assert.Throws<IllegalStateException>(send);
        Assert.Equal(message, ex.Message);
    }

    [Fact]
    public void Send_OnLiveProducer_StillSerializesBothSidesOnce()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, value);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        RecordMetadata metadata = producer.Send(Record()).Get();

        Assert.Equal(Topic, metadata.Topic());
        Assert.Equal(1, key.InvocationCount);
        Assert.Equal(1, value.InvocationCount);
    }
}
