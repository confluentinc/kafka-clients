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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync producer's teardown: Close and Dispose return without hanging and are idempotent under one latch;
/// Dispose first flushes a manual mock's pending sends, while Close leaves them pending; a call after teardown
/// throws ObjectDisposedException.
/// </summary>
public sealed class PublicSyncProducerTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-teardown-topic";

    [Fact]
    public void Close_OnMock_Succeeds()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(producer.Close, s_deadline);
    }

    [Fact]
    public void Dispose_WhenIdle_Returns()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public void Close_AfterAutoCompleteSend_Returns()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Get(),
            s_deadline);
        Assert.Equal(0L, metadata.Offset);

        TestTimeout.Run(producer.Close, s_deadline);
    }

    [Fact]
    public void Dispose_CalledTwice_IsSafe()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(producer.Dispose, s_deadline);
        // The second call is a no-op: no double close or destroy.
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public void Close_ThenDispose_IsSafe()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Close wins the latch; the following Dispose is a no-op.
        TestTimeout.Run(producer.Close, s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task ConcurrentDispose_IsSafe()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Many threads race Dispose: one wins the latch, the rest are no-ops.
        Task[] disposers = Enumerable
            .Range(0, 8)
            .Select(_ => Task.Run(() => producer.Dispose()))
            .ToArray();

        await TestTimeout.Run(() => Task.WhenAll(disposers), s_deadline);
    }

    [Fact]
    public void Ops_AfterClose_ThrowObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        producer.Close();

        // Every operation throws ObjectDisposedException after teardown; the type only, as for the consumer.
        Assert.Throws<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));
        Assert.Throws<ObjectDisposedException>(() => producer.Flush());
        Assert.Throws<ObjectDisposedException>(() => producer.PartitionsFor(Topic));

        producer.Dispose();
    }

    [Fact]
    public void RealProducer_Dispose_Returns()
    {
        // The real KafkaProducer with no broker tears down without hanging.
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" },
            Serdes.ByteArray,
            Serdes.ByteArray);

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    // A manual mock: Dispose flushes first, completing a pending send; Close leaves it pending, as Java's
    // MockProducer.close() does.

    [Fact]
    public void SyncDispose_WithUncompletedManualSend_CompletesTheSend()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();
        KafkaFuture<RecordMetadata> future =
            producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback);

        TestTimeout.Run(producer.Dispose, s_deadline);

        Assert.True(future.IsDone, "the uncompleted send was still pending when Dispose returned");
        Assert.Equal(1, callback.Count);
        RecordMetadata metadata = future.Get(s_deadline);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Thread.Sleep(200);
        Assert.Equal(1, callback.Count);
    }

    [Fact]
    public void SyncDispose_AfterClose_DoesNotFlush_AndDoesNotThrow()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        RecordingCallback callback = new RecordingCallback();
        KafkaFuture<RecordMetadata> future =
            producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback);

        TestTimeout.Run(producer.Close, s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);

        Thread.Sleep(200);
        Assert.False(future.IsDone, "Dispose after Close completed the send");
        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public async Task SyncClose_WithConcurrentBlockedGet_Returns_AndTheSendStaysPending()
    {
        // Java's MockProducer.close() only marks the mock closed: a send it never completed stays pending, so a
        // Get blocked on it ends only by its own timeout.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
            Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        KafkaFuture<RecordMetadata> future =
            producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));
        Task<bool> timedOut = Task.Run(() =>
        {
            try
            {
                future.Get(TimeSpan.FromSeconds(2));
                return false;
            }
            catch (TimeoutException)
            {
                return true;
            }
        });

        WaitUntilSendRegistered(producer);
        TestTimeout.Run(producer.Close, s_deadline);

        Assert.True(await TestTimeout.Run(() => timedOut, s_deadline), "the blocked Get returned a result");
        Assert.False(future.IsDone);
    }

    // Spins until the mock's history holds a record, i.e. the worker's send has landed and it is blocked in
    // Get; bounded by the file's deadline.
    private static void WaitUntilSendRegistered(MockProducer<byte[], byte[]> producer)
    {
        Stopwatch sw = Stopwatch.StartNew();
        while (producer.HistoryCount() == 0)
        {
            if (sw.Elapsed > s_deadline)
            {
                throw new TimeoutException(
                    "The blocking send never registered within the deadline — treated as a hang.");
            }

            Thread.Sleep(2);
        }
    }

    /// <summary>A delivery callback that counts its calls.</summary>
    private sealed class RecordingCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, Exception? exception) => Interlocked.Increment(ref _count);
    }
}
