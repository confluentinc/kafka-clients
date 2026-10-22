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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke test for the delivery-callback overload: Send(record, callback) on both producer mocks,
/// the failure path's non-null placeholder metadata, and the overload on the real clients, with
/// netstandard2.0-safe APIs only so it compiles on net462.
/// </summary>
public sealed class PublicProducerDeliveryCallbackTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-delivery-topic";

    [Fact]
    public async Task AsyncMockProducer_SendWithCallback_RoundTrips()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        SmokeDeliveryCallback callback = new SmokeDeliveryCallback();

        RecordMetadata metadata = default!;
        await TestTimeout.Run(
            async () => metadata = await producer.Send(
                new ProducerRecord<byte[], byte[]>(
                    Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 0),
                callback).Delivery(),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);

        // The callback runs on the completion thread; a bounded wait for it.
        await TestTimeout.Run(() => WaitForCompletions(callback, 1), s_deadline);
        Assert.Null(callback.LastException);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
    }

    [Fact]
    public void MockProducer_SendWithCallback_RoundTrips()
    {
        MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        SmokeDeliveryCallback callback = new SmokeDeliveryCallback();

        RecordMetadata metadata = default!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0),
                callback).Get(),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);

        // The sync flavor runs the callback on the completion thread, before Get returns.
        Assert.Equal(1, callback.Count);
        Assert.Null(callback.LastException);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
    }

    [Fact]
    public async Task AsyncMockProducer_SendWithCallback_FailurePath_DeliversPlaceholderMetadata()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        SmokeDeliveryCallback callback = new SmokeDeliveryCallback();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 1),
            callback).Delivery();
        Assert.True(producer.ErrorNext(new KafkaException(2, "tfm-delivery-error", isRetriable: true)));

        await Assert.ThrowsAsync<KafkaException>(async () => await sendTask);

        await TestTimeout.Run(() => WaitForCompletions(callback, 1), s_deadline);

        // The user callback never sees a null metadata, as Java's Callback contract states.
        Assert.NotNull(callback.LastMetadata);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
        Assert.Equal(1, callback.LastMetadata.Partition);
        Assert.Equal(-1L, callback.LastMetadata.Offset);
        Assert.NotNull(callback.LastException);
        Assert.Equal(2, Assert.IsType<KafkaException>(callback.LastException).Code);
    }

    [Fact]
    public void RealProducers_DeclareTheCallbackOverload()
    {
        // The real clients cannot be driven broker-free, so this pins only that the overload is callable on them.
        Dictionary<string, string> config = new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" };

        AsyncKafkaProducer<byte[], byte[]> asyncProducer =
            new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable asyncProducerDisposal = TestTimeout.Disposal(asyncProducer);
        KafkaProducer<byte[], byte[]> syncProducer =
            new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable syncProducerDisposal = TestTimeout.Disposal(syncProducer);

        Assert.NotNull(typeof(AsyncKafkaProducer<byte[], byte[]>).GetMethod(
            "Send",
            new[]
            {
                typeof(ProducerRecord<byte[], byte[]>),
                typeof(IDeliveryCallback),
                typeof(System.Threading.CancellationToken),
            }));
        Assert.NotNull(typeof(KafkaProducer<byte[], byte[]>).GetMethod(
            "Send",
            new[] { typeof(ProducerRecord<byte[], byte[]>), typeof(IDeliveryCallback) }));
    }

    private static async Task WaitForCompletions(SmokeDeliveryCallback callback, int expected)
    {
        while (callback.Count < expected)
        {
            await Task.Delay(2).ConfigureAwait(false);
        }
    }

    /// <summary>A delivery callback that counts its calls.</summary>
    private sealed class SmokeDeliveryCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => System.Threading.Volatile.Read(ref _count);

        internal RecordMetadata? LastMetadata { get; private set; }

        internal Exception? LastException { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            LastMetadata = metadata;
            LastException = exception;
            System.Threading.Interlocked.Increment(ref _count);
        }
    }
}
