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

using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Send checks for a closed producer before it serializes, as Java's doSend does, on all four producer types: the
/// serializer is never invoked, so a stateful one takes no action for a doomed send, and its throw cannot mask
/// the IllegalStateException. The closed check also comes before the null-record check, as in Java.
/// </summary>
public sealed class PublicProducerSendClosedCheckTests
{
    private const string Topic = "closed-check-topic";

    private const string MockClosed = "MockProducer is already closed.";

    private const string RealClosed = "Cannot perform operation after producer has been closed";

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
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(RealConfig(), key, value);
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
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(key, value);
        TestTimeout.Dispose(producer);

        // Send throws synchronously (not an async method), so Assert.Throws is the overload.
        Assert.Throws<IllegalStateException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void AsyncRealSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray().Serializer());
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(RealConfig(), key, value);
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
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(key, Serdes.ByteArray().Serializer());
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
    public void EverySend_NullArgumentsOnClosedProducer_ThrowIllegalState_OnAllFourProducers()
    {
        ISerializer<byte[]> bytes = Serdes.ByteArray().Serializer();
        MockProducer<byte[], byte[]> mock = new MockProducer<byte[], byte[]>(bytes, bytes);
        AsyncMockProducer<byte[], byte[]> asyncMock = new AsyncMockProducer<byte[], byte[]>(bytes, bytes);
        KafkaProducer<byte[], byte[]> real = new KafkaProducer<byte[], byte[]>(RealConfig(), bytes, bytes);
        AsyncKafkaProducer<byte[], byte[]> asyncReal = new AsyncKafkaProducer<byte[], byte[]>(RealConfig(), bytes, bytes);
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
