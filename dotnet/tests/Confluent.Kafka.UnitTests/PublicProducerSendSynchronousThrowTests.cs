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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async <c>Send</c>'s precondition throws (a cancelled token, a null record, a closed producer, a serializer
/// throw) leave the call itself, not the returned <see cref="ValueTask{TResult}"/>, on both overloads and both
/// clients. Each guard discards the returned value unawaited, so it fails if the send path becomes <c>async</c>.
/// </summary>
public sealed class PublicProducerSendSynchronousThrowTests
{
    private const string Topic = "sync-throw-topic";

    private const string Mock = "mock";
    private const string Real = "real";
    private const string Plain = "plain";
    private const string WithCallback = "callback";

    [Theory]
    [InlineData(Mock, Plain)]
    [InlineData(Mock, WithCallback)]
    [InlineData(Real, Plain)]
    [InlineData(Real, WithCallback)]
    public async Task AlreadyCanceledToken_ThrowsSynchronously_AndSendsNothing(string client, string overload)
    {
        using IAsyncProducer<byte[], byte[]> producer = Create(client);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        (OperationCanceledException error, WeakReference value) = SendCanceled(producer, overload, callback, cts.Token);

        // The caller's token, so a `when (e.CancellationToken == ct)` filter matches.
        Assert.Equal(cts.Token, error.CancellationToken);
        await AssertNothingSent(producer, callback);
        if (client == Real)
        {
            AssertCollected(value);
        }
    }

    [Theory]
    [InlineData(Mock, Plain)]
    [InlineData(Real, Plain)]
    [InlineData(Real, WithCallback)]
    public async Task NullRecord_ThrowsSynchronously(string client, string overload)
    {
        using IAsyncProducer<byte[], byte[]> producer = Create(client);
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        // ArgumentNullException sets no custom message, so ParamName is the asserted contract.
        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => { _ = Send(producer, overload, null!, callback, CancellationToken.None); });

        Assert.Equal("record", error.ParamName);
        await AssertNothingSent(producer, callback);
    }

    [Fact]
    public void Disposed_RealClient_CallbackOverload_ThrowsSynchronously()
    {
        IAsyncProducer<byte[], byte[]> producer = Create(Real);
        producer.Dispose();
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        // Post-dispose stays type-only (the consumer norm asserts no ObjectName).
        Assert.Throws<ObjectDisposedException>(
            () => { _ = Send(producer, WithCallback, Record(), callback, CancellationToken.None); });

        Assert.Equal(0, callback.Count);
    }

    [Theory]
    [InlineData(Plain)]
    [InlineData(WithCallback)]
    public void SerializerThrows_RealClient_ThrowsSynchronously(string overload)
    {
        ThrowingSerializer<byte[]> throwing = new ThrowingSerializer<byte[]>();
        using IAsyncProducer<byte[], byte[]> producer = Create(Real, throwing);
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        SerializationException error = Assert.Throws<SerializationException>(
            () => { _ = Send(producer, overload, Record(), callback, CancellationToken.None); });

        Assert.Contains("value", error.Message, StringComparison.Ordinal);
        Assert.Contains(Topic, error.Message, StringComparison.Ordinal);
        Assert.IsType<ThrowingSerializer<byte[]>.BoomException>(error.InnerException);
        Assert.Equal(1, throwing.InvocationCount);
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public void SendViaPump_AlreadyCanceledToken_ThrowsSynchronously()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        try
        {
            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            OperationCanceledException error = Assert.Throws<OperationCanceledException>(
                () => { _ = producer.SendViaPump(SerializedRecord(), callback: null, cts.Token); });

            Assert.Equal(cts.Token, error.CancellationToken);
        }
        finally
        {
            producer.Dispose();
        }
    }

    [Fact]
    public void SendViaPump_DisposedProducer_ThrowsSynchronously()
    {
        // Unreachable through a public client (each checks the closed state first), so this is the only guard on
        // SendViaPump's own check staying synchronous.
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        producer.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => { _ = producer.SendViaPump(SerializedRecord(), callback: null); });
    }

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0);

    private static SerializedProducerRecord SerializedRecord() =>
        new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x53, 0x59, 0x4E });

    private static IAsyncProducer<byte[], byte[]> Create(string client, ISerializer<byte[]>? valueSerializer = null) =>
        client == Mock
            ? new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, valueSerializer ?? Serdes.ByteArray)
            : new AsyncKafkaProducer<byte[], byte[]>(
                new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:1" },
                Serdes.ByteArray,
                valueSerializer ?? Serdes.ByteArray);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (OperationCanceledException Error, WeakReference Value) SendCanceled(
        IAsyncProducer<byte[], byte[]> producer,
        string overload,
        CountingDeliveryCallback callback,
        CancellationToken cancellationToken)
    {
        byte[] value = Encoding.UTF8.GetBytes("canceled");
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(Topic, value, partition: 0);
        OperationCanceledException error = Assert.Throws<OperationCanceledException>(
            () => { _ = Send(producer, overload, record, callback, cancellationToken); });
        return (error, new WeakReference(value));
    }

    private static void AssertCollected(WeakReference reference)
    {
        for (int i = 0; i < 5 && reference.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.False(reference.IsAlive, "the canceled send's value is still pinned: the record reached the producer");
    }

    private static ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        IAsyncProducer<byte[], byte[]> producer,
        string overload,
        ProducerRecord<byte[], byte[]> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken) =>
        overload == Plain
            ? producer.Send(record, cancellationToken)
            : producer.Send(record, callback, cancellationToken);

    // Nothing reached the producer: no callback and, on the mock after a flush, an empty history.
    private static async Task AssertNothingSent(IAsyncProducer<byte[], byte[]> producer, CountingDeliveryCallback callback)
    {
        if (producer is AsyncMockProducer<byte[], byte[]> mock)
        {
            await TestTimeout.Run(() => mock.Flush(), TimeSpan.FromSeconds(30));
            Assert.Equal(0, mock.HistoryCount());
        }

        Assert.Equal(0, callback.Count);
    }

    /// <summary>A delivery callback that counts its calls.</summary>
    private sealed class CountingDeliveryCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, Exception? exception) =>
            Interlocked.Increment(ref _count);
    }
}
