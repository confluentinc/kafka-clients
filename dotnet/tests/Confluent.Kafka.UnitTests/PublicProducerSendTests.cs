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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The asynchronous send path on <see cref="AsyncMockProducer{TKey, TValue}"/>: the future resolves or faults, absent
/// and empty keys and values, a non-ASCII topic, concurrency, preconditions, cancellation, and teardown with sends
/// in flight. Every awaited operation runs under a <see cref="TestTimeout"/> hang guard.
/// </summary>
public sealed class PublicProducerSendTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "send-topic";

    [Fact]
    public async Task Send_OnAutoCompleteMock_ResolvesWithMetadata()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(
            producer,
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 2));

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(2, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
        // RecordMetadata.ToString mirrors Java's "topic-partition@offset".
        Assert.Equal("send-topic-2@0", metadata.ToString());
    }

    [Fact]
    public async Task Send_SequentialToSamePartition_OffsetsIncrement()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 5; i++)
        {
            RecordMetadata metadata = await SendOf(
                producer,
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"value-{i}"), partition: 0));
            Assert.Equal(0, metadata.Partition);
            Assert.Equal((long)i, metadata.Offset);
        }
    }

    [Fact]
    public async Task Send_ErrorNext_FaultsWithKafkaException()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)).Delivery();

        Assert.True(producer.ErrorNext(new KafkaException(2, "boom-corrupt-message", isRetriable: true)));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => WithTimeout(sendTask));

        Assert.Equal(2, failure.Code);
        Assert.Contains("boom-corrupt-message", failure.Message, StringComparison.Ordinal);
        Assert.True(failure.IsRetriable);
    }

    [Fact]
    public async Task Send_ErrorNext_FaultsWithTheSameInstance()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)).Delivery();

        KafkaException injected = new KafkaException(2, "the injected instance", isRetriable: true);
        Assert.True(producer.ErrorNext(injected));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => WithTimeout(sendTask));
        Assert.Same(injected, failure);
    }

    [Fact]
    public async Task Send_AbsentKeyAndValue_Resolves()
    {
        // Absent key and value: no key, and a tombstone.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(producer, new ProducerRecord<byte[], byte[]>(Topic, value: null, key: null, partition: 1));

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, metadata.Partition);
    }

    [Fact]
    public async Task Send_EmptyKeyAndValue_Resolves()
    {
        // Empty key and value: a non-null pointer with length 0, distinct from absent.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(
            producer,
            new ProducerRecord<byte[], byte[]>(Topic, Array.Empty<byte>(), Array.Empty<byte>(), partition: 1));

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, metadata.Partition);
    }

    [Fact]
    public async Task Send_MutationImmediatelyAfterSend_CompletesCorrectly()
    {
        // The async send borrows the caller's buffers until the hand-over, so a mutation before then may reach the wire,
        // and must stay memory-safe. The mock cannot read the sent bytes back, so the test asserts the send resolves.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        byte[] key = Enumerable.Repeat((byte)0xAB, 32).ToArray();
        byte[] value = Enumerable.Repeat((byte)0xCD, 256 * 1024).ToArray();

        Task<RecordMetadata> sendTask = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0)).Delivery();

        Array.Clear(value, 0, value.Length);
        Array.Clear(key, 0, key.Length);

        RecordMetadata metadata = await WithTimeout(sendTask);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
    }

    [Fact]
    public async Task Send_NonAsciiTopic_RoundTripsToMetadataTopic()
    {
        const string nonAsciiTopic = "témás-topic-日本語-🚀";
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(
            producer,
            new ProducerRecord<byte[], byte[]>(nonAsciiTopic, Encoding.UTF8.GetBytes("v"), partition: 0));

        Assert.Equal(nonAsciiTopic, metadata.Topic);
    }

    [Fact]
    public async Task Send_ManyThreadsConcurrently_AllResolve()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        const int threadCount = 8;
        const int perThread = 50;

        // Each thread sends to its own partition, so the offsets per partition are deterministic.
        Task<RecordMetadata[]>[] threads = Enumerable.Range(0, threadCount)
            .Select(partition => Task.Run(async () =>
            {
                Task<RecordMetadata>[] sends = new Task<RecordMetadata>[perThread];
                for (int i = 0; i < perThread; i++)
                {
                    sends[i] = producer.Send(
                        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{partition}-{i}"), partition: partition)).Delivery();
                }

                return await Task.WhenAll(sends);
            }))
            .ToArray();

        RecordMetadata[][] results = null!;
        await TestTimeout.Run(async () => results = await Task.WhenAll(threads), s_deadline);

        int total = results.Sum(r => r.Length);
        Assert.Equal(threadCount * perThread, total);
        Assert.All(results.SelectMany(r => r), md => Assert.Equal(Topic, md.Topic));
    }

    [Fact]
    public async Task Send_NullRecord_ThrowsArgumentNull()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // ArgumentNullException sets no custom message, so ParamName is the asserted contract.
        ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.Send(null!).Delivery());
        Assert.Equal("record", ex.ParamName);
    }

    [Fact]
    public async Task Send_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"))).Delivery());
    }

    [Fact]
    public async Task Send_PreCanceledToken_ThrowsOperationCanceled()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        // An already-cancelled token throws before any native call.
        OperationCanceledException canceled = await Assert.ThrowsAsync<OperationCanceledException>(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v")), cts.Token).Delivery());

        // The exception carries the caller's token, so a `when (e.CancellationToken == ct)` filter matches.
        Assert.Equal(cts.Token, canceled.CancellationToken);

        // The producer is still usable after a rejected send.
        RecordMetadata metadata = await SendOf(producer, new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));
        Assert.Equal(Topic, metadata.Topic);
    }

    [Fact]
    public async Task Get_TokenCancelsOnlyTheWait_TheSendStillCompletes()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using CancellationTokenSource cts = new CancellationTokenSource();

        AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).AsTask(),
            s_deadline);
        Task<RecordMetadata> wait = future.Get(cts.Token);

        cts.Cancel();
        OperationCanceledException canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WithTimeout(wait));
        Assert.Equal(cts.Token, canceled.CancellationToken);
        Assert.False(future.IsDone);

        Assert.True(producer.CompleteNext());
        RecordMetadata metadata = await WithTimeout(future.Get());
        Assert.Equal(0L, metadata.Offset);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_WithSendsInFlight_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 16; i++)
        {
            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
        }

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WithSendsInFlight_SyncDispose_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 16; i++)
        {
            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
        }

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    // A manual mock: Dispose flushes first, completing a pending send; Close leaves it pending, as Java's
    // MockProducer.close() does.

    [Fact]
    public async Task Dispose_WithUncompletedManualSendInFlight_CompletesTheSend()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Delivery();

        TestTimeout.Run(producer.Dispose, s_deadline);

        await AssertCompletedOnce(pending, callback);
    }

    [Fact]
    public async Task DisposeAsync_WithUncompletedManualSendInFlight_CompletesTheSend()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Delivery();

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        await AssertCompletedOnce(pending, callback);
    }

    [Fact]
    public async Task Close_WithUncompletedManualSendInFlight_Returns_AndTheSendStaysPending()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Delivery();

        await TestTimeout.Run(() => producer.Close(), s_deadline);

        AssertStaysPending(pending, callback);
    }

    [Fact]
    public async Task CloseWithTimeout_WithUncompletedManualSendInFlight_Returns_AndTheSendStaysPending()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Delivery();

        await TestTimeout.Run(() => producer.Close(TimeSpan.FromSeconds(1)), s_deadline);

        AssertStaysPending(pending, callback);
    }

    [Fact]
    public async Task Dispose_AfterClose_DoesNotFlush_AndDoesNotThrow()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Delivery();

        await TestTimeout.Run(() => producer.Close(), s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);

        AssertStaysPending(pending, callback);
    }

    [Fact]
    public async Task DisposeAsync_AfterClose_DoesNotFlush_AndDoesNotThrow()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Delivery();

        await TestTimeout.Run(() => producer.Close(), s_deadline);
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        AssertStaysPending(pending, callback);
    }

    [Fact]
    public void Dispose_WithManyUncompletedManualSendsInFlight_Returns()
    {
        // The teardown flush completes every pending send, so Dispose returns.
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        for (int i = 0; i < 16; i++)
        {
            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
        }

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public void ConcurrentSendAndDispose_DoesNotCrash()
    {
        // Sends racing Dispose under GC pressure: the destroy waits for a send still in the native call, so a
        // use-after-free would crash here. A racing send may throw ObjectDisposedException.
        for (int iteration = 0; iteration < 40; iteration++)
        {
            AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

            TestTimeout.Run(
                () =>
                {
                    Task senders = Task.Run(() =>
                    {
                        for (int i = 0; i < 32; i++)
                        {
                            try
                            {
                                _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
                            }
                            catch (ObjectDisposedException)
                            {
                                // Expected once Dispose wins the latch — not a crash.
                            }
                        }
                    });

                    Task disposer = Task.Run(() => producer.Dispose());

                    Task.WaitAll(senders, disposer);
                },
                s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    [Fact]
    public void Send_CreateSendDisposeMany_Churn_NoLeakOrCrash()
    {
        // Churns create, send, await and dispose under GC pressure: a double free or use-after-free would crash
        // here, and a hang fails the hang guard.
        for (int iteration = 0; iteration < 40; iteration++)
        {
            TestTimeout.Run(
                () =>
                {
                    using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    Task<RecordMetadata>[] sends = new Task<RecordMetadata>[8];
                    for (int i = 0; i < sends.Length; i++)
                    {
                        sends[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0)).Delivery();
                    }

                    Task.WaitAll(sends);
                },
                s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }


    [Fact]
    public void ConcurrentSendAndDispose_OnManualMock_DoesNotHang()
    {
        // Sends racing a manual mock's teardown: a send landing after the teardown flush must neither hang nor crash.
        // Dedicated threads, so every sender is mid-send when Dispose starts. Whether a run lands in that window is
        // timing-dependent, so this is a churn guard, not a proof.
        for (int iteration = 0; iteration < 25; iteration++)
        {
            AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(
                Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

            using CountdownEvent hot = new CountdownEvent(4);
            Thread[] senders = new Thread[4];
            for (int t = 0; t < senders.Length; t++)
            {
                senders[t] = new Thread(() =>
                {
                    bool signalled = false;
                    for (int i = 0; i < 2000; i++)
                    {
                        try
                        {
                            _ = producer.Send(new ProducerRecord<byte[], byte[]>(
                                Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
                        }
                        catch (ObjectDisposedException)
                        {
                            // Expected once Dispose wins the latch — nothing more to push.
                            break;
                        }
                        catch (KafkaException)
                        {
                            // Also expected: a send past the closed check can be refused by the native send after the close.
                            break;
                        }

                        if (!signalled)
                        {
                            signalled = true;
                            hot.Signal();
                        }
                    }

                    if (!signalled)
                    {
                        hot.Signal();
                    }
                })
                { IsBackground = true };
                senders[t].Start();
            }

            // Every sender is mid-flight before teardown begins.
            Assert.True(hot.Wait(s_deadline), "senders never got going");

            TestTimeout.Run(producer.Dispose, s_deadline);

            foreach (Thread sender in senders)
            {
                Assert.True(sender.Join(s_deadline), "a sender thread never finished");
            }
        }
    }

    private static async Task<RecordMetadata> SendOf(IAsyncProducer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record)
    {
        RecordMetadata result = null!;
        await TestTimeout.Run(async () => result = await producer.Send(record).Delivery(), s_deadline);
        return result;
    }

    private static async Task<RecordMetadata> WithTimeout(Task<RecordMetadata> task)
    {
        Task winner = await Task.WhenAny(task, Task.Delay(s_deadline)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException("Send did not complete within the deadline — treated as a hang.");
        }

        return await task.ConfigureAwait(false);
    }

    private static async Task AssertCompletedOnce(Task<RecordMetadata> pending, RecordingCallback callback)
    {
        Assert.True(pending.IsCompleted, "the uncompleted send was still pending when the teardown returned");
        Assert.Equal(1, callback.Count);
        RecordMetadata metadata = await pending;
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        await Task.Delay(200);
        Assert.Equal(1, callback.Count);
    }

    private static void AssertStaysPending(Task<RecordMetadata> pending, RecordingCallback callback)
    {
        Thread.Sleep(200);
        Assert.False(pending.IsCompleted, "the uncompleted send completed at teardown");
        Assert.Equal(0, callback.Count);
    }

    /// <summary>A delivery callback that counts its calls.</summary>
    private sealed class RecordingCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, Exception? exception) => Interlocked.Increment(ref _count);
    }
}
