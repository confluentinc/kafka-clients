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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async <c>Send</c>'s precondition throws stay <b>synchronous</b> under the two-stage
/// <c>ValueTask&lt;Task&lt;RecordMetadata&gt;&gt;</c> return (M11/P3.5, test T8).
/// </summary>
/// <remarks>
/// <para>
/// Every guard here calls <c>Send</c> through a statement lambda bound to <see cref="Action"/> and
/// <b>discards</b> the returned value without awaiting it, so <see cref="Assert.Throws{T}(Action)"/>
/// passes only when the exception leaves the call itself. That is the property an <c>async</c> send
/// path would break: an <c>async</c> method captures each of these throws into the returned
/// <see cref="ValueTask{TResult}"/>, and a caller that never awaits it would never see them.
/// </para>
/// <para>
/// The matrix is {already-canceled token, null record, disposed producer, serializer throw} ×
/// {plain, callback-taking overload} × {mock, real async client}. Only the cells not already pinned
/// by a strict (non-awaiting) guard elsewhere are added here; the others are:
/// disposed × plain × mock / real — <c>PublicProducerSendClosedCheckTests.AsyncMockSend_AfterDispose_DoesNotInvokeSerializers</c>
/// and <c>…AsyncRealSend_AfterDispose_DoesNotInvokeSerializers</c>;
/// disposed / serializer throw / null record × callback × mock — the <c>Async</c> rows of
/// <c>PublicProducerDeliveryCallbackTests.NoCallback_OnSynchronousThrow_*</c>;
/// serializer throw × plain × mock — <c>PublicProducerTypedSendTests.Async_ValueSerializerThrows_ThrowsSynchronouslyBeforeReturningTask</c>.
/// </para>
/// <para>
/// The already-canceled-token cells are the ones that discriminate the internal entry point: the
/// token is checked inside <see cref="NativeProducer.SendViaPump"/>, below both client skins, so they
/// are what turns red if that method becomes <c>async</c>. The two
/// <see cref="NativeProducer"/>-level guards at the end pin it directly.
/// </para>
/// </remarks>
public sealed class PublicProducerSendSynchronousThrowTests
{
    private const string Topic = "sync-throw-topic";

    private const string Mock = "mock";
    private const string Real = "real";
    private const string Plain = "plain";
    private const string WithCallback = "callback";

    // ---- Already-canceled token: thrown before anything is appended (every cell is new) ----

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

        OperationCanceledException error = Assert.Throws<OperationCanceledException>(
            () => { _ = Send(producer, overload, Record(), callback, cts.Token); });

        // The caller's token, so `catch (OperationCanceledException e) when (e.CancellationToken == ct)`
        // matches (M11/P8 Minor 9).
        Assert.Equal(cts.Token, error.CancellationToken);
        await AssertNothingSent(producer, callback);
    }

    // ---- Null record ----

    [Theory]
    [InlineData(Mock, Plain)]
    [InlineData(Real, Plain)]
    [InlineData(Real, WithCallback)]
    public async Task NullRecord_ThrowsSynchronously(string client, string overload)
    {
        using IAsyncProducer<byte[], byte[]> producer = Create(client);
        CountingDeliveryCallback callback = new CountingDeliveryCallback();

        // ArgumentNullException(nameof(record)) sets no custom message → ParamName is the contract.
        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => { _ = Send(producer, overload, null!, callback, CancellationToken.None); });

        Assert.Equal("record", error.ParamName);
        await AssertNothingSent(producer, callback);
    }

    // ---- Disposed producer ----

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

    // ---- Serializer throw ----

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

    // ---- The internal entry point itself (below both client skins) ----

    [Fact]
    public void SendViaPump_AlreadyCanceledToken_ThrowsSynchronously()
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        try
        {
            using CancellationTokenSource cts = new CancellationTokenSource();
            cts.Cancel();

            OperationCanceledException error = Assert.Throws<OperationCanceledException>(
                () => { _ = producer.SendViaPump(SerializedRecord(), delivery: null, cts.Token); });

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
        // Unreachable through a public skin (each checks the latch before calling down), so this is
        // the only guard on SendViaPump's own re-check staying synchronous.
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        producer.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => { _ = producer.SendViaPump(SerializedRecord(), delivery: null); });
    }

    // ---- Helpers ----

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0);

    private static SerializedProducerRecord SerializedRecord() =>
        new SerializedProducerRecord(Topic, 0, null, null, new byte[] { 0x53, 0x59, 0x4E });

    private static IAsyncProducer<byte[], byte[]> Create(string client, ISerializer<byte[]>? valueSerializer = null) =>
        client == Mock
            ? new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, valueSerializer ?? Serdes.ByteArray)
            : new AsyncKafkaProducer<byte[], byte[]>(
                new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" },
                Serdes.ByteArray,
                valueSerializer ?? Serdes.ByteArray);

    private static ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        IAsyncProducer<byte[], byte[]> producer,
        string overload,
        ProducerRecord<byte[], byte[]> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken) =>
        overload == Plain
            ? producer.Send(record, cancellationToken)
            : producer.Send(record, callback, cancellationToken);

    /// <summary>
    /// Nothing reached the producer: no callback, and — on the mock, after a flush has drained the
    /// accumulator, so a deferred append cannot hide behind the count — an empty history.
    /// </summary>
    private static async Task AssertNothingSent(IAsyncProducer<byte[], byte[]> producer, CountingDeliveryCallback callback)
    {
        if (producer is AsyncMockProducer<byte[], byte[]> mock)
        {
            await TestTimeout.Run(() => mock.Flush(), TimeSpan.FromSeconds(30));
            Assert.Equal(0, mock.HistoryCount());
        }

        Assert.Equal(0, callback.Count);
    }

    private sealed class CountingDeliveryCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception) =>
            Interlocked.Increment(ref _count);
    }
}
