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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Flush</c>, <c>Close</c> and <c>Dispose</c> while a send or a <c>PartitionsFor</c> waits for metadata on an
/// unreachable broker: a flush does not wait for the send, a close fails it, and the destroy runs on the closing
/// thread.
/// </summary>
public sealed class PublicProducerControlOperationsTests
{
    private const string ClosedWhileSending = "Producer closed while send in progress";

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task ControlOperations_DoNotWaitForASendInProgress(string flavor)
    {
        const int maxBlockMs = 2000;
        string topic = Topic("control-operations");
        ProbeCallback callback = new ProbeCallback((_, _) => { });
        if (flavor == SyncFlavor)
        {
            using KafkaProducer<byte[], byte[]> producer = Sync(maxBlockMs);
            Task<KafkaFuture<RecordMetadata>> send = Task.Factory.StartNew(
                () => producer.Send(Record(topic), callback),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            await Task.Delay(Settle);
            Assert.False(send.IsCompleted, "the send did not wait for metadata");

            TestTimeout.Run(producer.Flush, Deadline);
            Assert.False(send.IsCompleted, "Flush waited for the send in progress");

            KafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => send, Deadline);
            AssertMetadataTimeout(Assert.Throws<KafkaException>(() => future.Get(Deadline)), topic, maxBlockMs, callback);
        }
        else
        {
            await using AsyncKafkaProducer<byte[], byte[]> producer = Async(maxBlockMs);
            Task<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(Record(topic), callback).AsTask();
            await Task.Delay(Settle);
            Assert.False(stage.IsCompleted, "the send did not wait for metadata");

            await TestTimeout.Run(() => producer.Flush(), Deadline);
            Assert.False(stage.IsCompleted, "Flush waited for the send in progress");

            AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => stage, Deadline);
            AssertMetadataTimeout(
                await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => future.Get(), Deadline)),
                topic,
                maxBlockMs,
                callback);
        }
    }

    [Theory]
    [InlineData(SyncFlavor, false)]
    [InlineData(SyncFlavor, true)]
    [InlineData(AsyncFlavor, false)]
    [InlineData(AsyncFlavor, true)]
    public async Task Close_WakesASendWaitingForMetadata(string flavor, bool timed)
    {
        const int maxBlockMs = 60000;
        string topic = Topic("close-wakes-send");
        ProbeCallback callback = new ProbeCallback((_, _) => { });
        Stopwatch elapsed = new Stopwatch();
        KafkaException failed;
        if (flavor == SyncFlavor)
        {
            using KafkaProducer<byte[], byte[]> producer = Sync(maxBlockMs);
            Task send = Task.Factory.StartNew(
                () => producer.Send(Record(topic), callback),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            await Task.Delay(Settle);
            Assert.False(send.IsCompleted, "the send did not wait for metadata");

            elapsed.Start();
            if (timed)
            {
                TestTimeout.Run(() => producer.Close(TimeSpan.FromSeconds(1)), Deadline);
            }
            else
            {
                TestTimeout.Run(producer.Close, Deadline);
            }

            elapsed.Stop();
            failed = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send, Deadline));
            Assert.Throws<ObjectDisposedException>(() => producer.Send(Record(topic)));
        }
        else
        {
            await using AsyncKafkaProducer<byte[], byte[]> producer = Async(maxBlockMs);
            Task<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(Record(topic), callback).AsTask();
            await Task.Delay(Settle);
            Assert.False(stage.IsCompleted, "the send did not wait for metadata");

            elapsed.Start();
            await TestTimeout.Run(() => timed ? producer.Close(TimeSpan.FromSeconds(1)) : producer.Close(), Deadline);
            elapsed.Stop();
            failed = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => stage, Deadline));
            Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record(topic)); });
        }

        Assert.Equal(ClosedWhileSending, failed.Message);
        Assert.True(elapsed.ElapsedMilliseconds < 10000, $"Close took {elapsed.ElapsedMilliseconds} ms");
        await Task.Delay(Settle);
        Assert.Equal(0, callback.Calls);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task Dispose_WithAPartitionsForInFlight_DestroysOnTheClosingThread(string flavor)
    {
        const int maxBlockMs = 60000;
        string topic = Topic("dispose-partitions-for");
        Task<IReadOnlyList<PartitionInfo>> partitions;
        DestroyLog destroys;
        int closingThread;
        if (flavor == SyncFlavor)
        {
            KafkaProducer<byte[], byte[]> producer = Sync(maxBlockMs);
            destroys = WatchDestroy(producer);
            partitions = Task.Factory.StartNew(
                () => producer.PartitionsFor(topic),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            await Task.Delay(Settle);
            Assert.False(partitions.IsCompleted, "PartitionsFor did not wait for metadata");

            closingThread = await TestTimeout.Run(
                () => Task.Factory.StartNew(
                    () =>
                    {
                        producer.Dispose();
                        return Environment.CurrentManagedThreadId;
                    },
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default),
                Deadline);
        }
        else
        {
            AsyncKafkaProducer<byte[], byte[]> producer = Async(maxBlockMs);
            destroys = WatchDestroy(producer);
            partitions = producer.PartitionsFor(topic);
            await Task.Delay(Settle);
            Assert.False(partitions.IsCompleted, "PartitionsFor did not wait for metadata");

            await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), Deadline);
            closingThread = -1;
        }

        KafkaException failed = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => partitions, Deadline));
        Assert.Equal("Requested metadata update after close", failed.Message);

        (int destroyThread, string? destroyThreadName) = Assert.Single(destroys.Destroys());
        Assert.NotEqual(NativeProducer.CompletionThreadName, destroyThreadName);
        Assert.NotEqual(NativeProducer.PumpThreadName, destroyThreadName);
        if (flavor == SyncFlavor)
        {
            Assert.Equal(closingThread, destroyThread);
        }
    }

    private static void AssertMetadataTimeout(KafkaException failure, string topic, int maxBlockMs, ProbeCallback callback)
    {
        Assert.Equal(7, failure.Code);
        Assert.Equal(MetadataTimeout(topic, maxBlockMs), failure.Message);
        Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
        Assert.Equal(1, callback.Calls);
        Assert.Same(failure, callback.Delivered);
    }
}
