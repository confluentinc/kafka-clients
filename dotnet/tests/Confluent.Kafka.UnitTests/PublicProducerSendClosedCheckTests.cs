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
/// the ObjectDisposedException. A null record is checked first.
/// </summary>
public sealed class PublicProducerSendClosedCheckTests
{
    private const string Topic = "closed-check-topic";

    private static Dictionary<string, string> RealConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0);

    [Fact]
    public void SyncMockSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, value);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => producer.Send(Record()));
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void SyncRealSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(RealConfig(), key, value);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => producer.Send(Record()));
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void AsyncMockSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(key, value);
        TestTimeout.Dispose(producer);

        // Send throws synchronously (not an async method), so Assert.Throws is the overload.
        Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void AsyncRealSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(RealConfig(), key, value);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    [Fact]
    public void SyncSend_AfterDispose_WithThrowingSerializer_SurfacesObjectDisposed()
    {
        ThrowingSerializer<byte[]> key = new ThrowingSerializer<byte[]>();
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, Serdes.ByteArray);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => producer.Send(Record()));
        Assert.Equal(0, key.InvocationCount);
    }

    [Fact]
    public void AsyncSend_AfterDispose_WithThrowingSerializer_SurfacesObjectDisposed()
    {
        ThrowingSerializer<byte[]> key = new ThrowingSerializer<byte[]>();
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(key, Serdes.ByteArray);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
    }

    [Fact]
    public void Send_NullRecordOnClosedProducer_StillThrowsArgumentNull()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, Serdes.ByteArray);
        TestTimeout.Dispose(producer);

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.Send(null!));
        Assert.Equal("record", ex.ParamName);
        Assert.Equal(0, key.InvocationCount);
    }

    [Fact]
    public void Send_OnLiveProducer_StillSerializesBothSidesOnce()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, value);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        RecordMetadata metadata = producer.Send(Record()).Get();

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, key.InvocationCount);
        Assert.Equal(1, value.InvocationCount);
    }
}
