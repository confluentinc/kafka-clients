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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M11/P8 Minor-8 fix: <c>Send</c> checks whether the producer is closed <b>before</b>
/// serializing, on all four client skins.
/// </summary>
/// <remarks>
/// <para>
/// Java checks in that order — <c>KafkaProducer.throwIfProducerClosed()</c> precedes the key/value
/// serialize in <c>doSend</c>. Serializing first has two user-visible consequences: a <b>stateful</b>
/// serializer runs for a record that can never be sent (a Schema Registry serializer <em>registers a
/// schema</em> — a real, externally visible action taken on behalf of a doomed send), and a
/// serializer throw <b>masks</b> the real <see cref="ObjectDisposedException"/> behind a
/// <see cref="SerializationException"/>, so the caller cannot tell the producer was already closed.
/// </para>
/// <para>
/// The invocation count is the observable: a serializer that is never called cannot have had a side
/// effect. A <see cref="ThrowingSerializer{T}"/> leg additionally proves the masking is gone — with
/// the check after the serialize the caller saw <see cref="SerializationException"/>.
/// </para>
/// <para>
/// The order is null-check → closed-check → serialize. The null-first half is pinned by
/// <c>PublicSyncProducerSendTests.Send_NullRecord_ThrownBeforeDisposedCheck_EvenWhenClosed</c>,
/// which must stay green unchanged.
/// </para>
/// </remarks>
public sealed class PublicProducerSendClosedCheckTests
{
    private const string Topic = "closed-check-topic";

    private static Dictionary<string, string> RealConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0);

    // ---- The serializer is NOT invoked once the producer is closed (all four skins) ----

    [Fact]
    public void SyncMockSend_AfterDispose_DoesNotInvokeSerializers()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, value);
        producer.Dispose();

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
        producer.Dispose();

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
        producer.Dispose();

        // Send throws SYNCHRONOUSLY (the skin is not an async method), so Assert.Throws is correct.
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
        producer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
        Assert.Equal(0, value.InvocationCount);
    }

    // ---- A throwing serializer no longer masks the ObjectDisposedException ----

    [Fact]
    public void SyncSend_AfterDispose_WithThrowingSerializer_SurfacesObjectDisposed()
    {
        ThrowingSerializer<byte[]> key = new ThrowingSerializer<byte[]>();
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, Serdes.ByteArray);
        producer.Dispose();

        // Pre-fix the serializer ran first and the caller saw SerializationException instead.
        Assert.Throws<ObjectDisposedException>(() => producer.Send(Record()));
        Assert.Equal(0, key.InvocationCount);
    }

    [Fact]
    public void AsyncSend_AfterDispose_WithThrowingSerializer_SurfacesObjectDisposed()
    {
        ThrowingSerializer<byte[]> key = new ThrowingSerializer<byte[]>();
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(key, Serdes.ByteArray);
        producer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record()); });
        Assert.Equal(0, key.InvocationCount);
    }

    // ---- The null-record precondition still wins over the closed check (order pin) ----

    [Fact]
    public void Send_NullRecordOnClosedProducer_StillThrowsArgumentNull()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, Serdes.ByteArray);
        producer.Dispose();

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.Send(null!));
        Assert.Equal("record", ex.ParamName);
        Assert.Equal(0, key.InvocationCount);
    }

    // ---- A live producer still serializes exactly once per side (the check is not over-eager) ----

    [Fact]
    public void Send_OnLiveProducer_StillSerializesBothSidesOnce()
    {
        RecordingSerializer<byte[]> key = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        RecordingSerializer<byte[]> value = new RecordingSerializer<byte[]>(Serdes.ByteArray);
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(key, value);

        RecordMetadata metadata = producer.Send(Record());

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, key.InvocationCount);
        Assert.Equal(1, value.InvocationCount);
    }
}
