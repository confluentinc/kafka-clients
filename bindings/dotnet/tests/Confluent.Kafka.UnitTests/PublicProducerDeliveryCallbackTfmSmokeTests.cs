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
/// The TFM-matrix smoke test for the M14/P1 delivery-callback overload (plan §9 item 10): the
/// <c>Send(record, callback)</c> round-trip on <b>both</b> producer flavors, plus the failure path's
/// non-null placeholder metadata. Uses only APIs available on the netstandard2.0 floor so it
/// <b>compiles on net462</b> (via ns2.0) as well as net8.0 / net10.0 — the net462 leg's <em>build</em>
/// must pass locally even though its <em>run</em> is Windows/CI-only; net10.0 runs locally.
/// </summary>
public sealed class PublicProducerDeliveryCallbackTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-delivery-topic";

    [Fact]
    public async Task AsyncMockProducer_SendWithCallback_RoundTrips()
    {
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        SmokeDeliveryCallback callback = new SmokeDeliveryCallback();

        RecordMetadata metadata = default!;
        await TestTimeout.Run(
            async () => metadata = await producer.Send(
                new ProducerRecord<byte[], byte[]>(
                    Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 0),
                callback),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);

        // The callback fires on the pump thread, so give it the same bounded wait the behavioural
        // suite uses rather than assuming it has already run when the task resolved.
        await TestTimeout.Run(() => WaitForCompletions(callback, 1), s_deadline);
        Assert.Null(callback.LastException);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
    }

    [Fact]
    public void MockProducer_SendWithCallback_RoundTrips()
    {
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        SmokeDeliveryCallback callback = new SmokeDeliveryCallback();

        RecordMetadata metadata = default!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0),
                callback),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);

        // The sync flavor invokes the callback inline, before Send returned.
        Assert.Equal(1, callback.Count);
        Assert.Null(callback.LastException);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
    }

    [Fact]
    public async Task AsyncMockProducer_SendWithCallback_FailurePath_DeliversPlaceholderMetadata()
    {
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        SmokeDeliveryCallback callback = new SmokeDeliveryCallback();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 1),
            callback);
        Assert.True(producer.ErrorNext(2, "tfm-delivery-error"));

        await Assert.ThrowsAsync<KafkaException>(async () => await sendTask);

        await TestTimeout.Run(() => WaitForCompletions(callback, 1), s_deadline);

        // Java's contract: the user callback never sees a null metadata (Callback.java:28-33).
        Assert.NotNull(callback.LastMetadata);
        Assert.Equal(Topic, callback.LastMetadata!.Topic);
        Assert.Equal(1, callback.LastMetadata.Partition);
        Assert.Equal(-1L, callback.LastMetadata.Offset);
        Assert.NotNull(callback.LastException);
        Assert.Equal(2, callback.LastException!.Code);
    }

    [Fact]
    public void RealProducers_DeclareTheCallbackOverload()
    {
        // The real clients cannot be driven broker-free, so the smoke leg pins that the overload is
        // callable on them (a compile-time + load-time check on every TFM).
        Dictionary<string, string> config = new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" };

        using AsyncKafkaProducer<byte[], byte[]> asyncProducer =
            new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
        using KafkaProducer<byte[], byte[]> syncProducer =
            new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);

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

    private sealed class SmokeDeliveryCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => System.Threading.Volatile.Read(ref _count);

        internal RecordMetadata? LastMetadata { get; private set; }

        internal KafkaException? LastException { get; private set; }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            LastMetadata = metadata;
            LastException = exception;
            System.Threading.Interlocked.Increment(ref _count);
        }
    }
}
