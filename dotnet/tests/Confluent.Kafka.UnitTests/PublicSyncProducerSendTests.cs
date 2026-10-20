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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync send path on <see cref="MockProducer{TKey, TValue}"/>: Send returns the future and Get returns the
/// metadata; on a manual mock Get blocks on a worker thread until the test thread resolves the send; a non-ASCII
/// topic round-trips; preconditions throw before any native call. Every blocking wait is bounded.
/// </summary>
public sealed class PublicSyncProducerSendTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-send-topic";

    [Fact]
    public void Send_OnAutoCompleteMock_ReturnsMetadata()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 2)).Get(),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(2, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
        // RecordMetadata.ToString mirrors Java's "topic-partition@offset".
        Assert.Equal("sync-send-topic-2@0", metadata.ToString());
    }

    [Fact]
    public void Send_SequentialToSamePartition_OffsetsIncrement()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 5; i++)
        {
            int captured = i;
            RecordMetadata metadata = null!;
            TestTimeout.Run(
                () => metadata = producer.Send(
                    new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"value-{captured}"), partition: 0)).Get(),
                s_deadline);

            Assert.Equal(0, metadata.Partition);
            Assert.Equal((long)captured, metadata.Offset);
        }
    }

    [Fact]
    public async Task Send_ManualComplete_UnblocksAndReturnsMetadata()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        // Get blocks on a worker thread until CompleteNext resolves it from this thread.
        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Get());

        DriveUntilResolved(producer.CompleteNext);

        RecordMetadata metadata = null!;
        await TestTimeout.Run(async () => metadata = await sendTask, s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
    }

    [Fact]
    public async Task Send_ManualError_FaultsWithKafkaException()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Get());

        DriveUntilResolved(() => producer.ErrorNext(new KafkaException(2, "sync-mock-error", isRetriable: true)));

        // Get throws the injected KafkaException, code and message included.
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => sendTask, s_deadline));
        Assert.Equal(2, failure.Code);
        Assert.Contains("sync-mock-error", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Send_NonAsciiTopic_RoundTrips()
    {
        // A non-ASCII topic guards the hand-written UTF-8 marshalling both ways.
        const string topic = "sync-主题-ünîcödé";
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Get(),
            s_deadline);

        Assert.Equal(topic, metadata.Topic);
    }

    [Fact]
    public void Send_NullRecord_ThrowsArgumentNull()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // ArgumentNullException sets no custom message, so ParamName is the asserted contract.
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.Send(null!));
        Assert.Equal("record", ex.ParamName);
    }

    [Fact]
    public void Send_NullRecord_ThrownBeforeDisposedCheck_EvenWhenClosed()
    {
        // The null check precedes the closed check, so a closed producer with a null record throws
        // ArgumentNullException.
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        producer.Dispose();

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.Send(null!));
        Assert.Equal("record", ex.ParamName);
    }

    [Fact]
    public void Send_AfterDispose_ThrowsObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        producer.Dispose();

        // Post-dispose stays type-only (the consumer norm asserts no ObjectName).
        Assert.Throws<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));
    }

    [Fact]
    public void Send_AfterClose_ThrowsObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        producer.Close();

        Assert.Throws<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));

        producer.Dispose();
    }

    // Retries drive (CompleteNext or ErrorNext) until it resolves a pending send or the deadline passes: an early
    // call returns false until the worker's send has landed.
    private static void DriveUntilResolved(Func<bool> drive)
    {
        Stopwatch sw = Stopwatch.StartNew();
        while (!drive())
        {
            if (sw.Elapsed > s_deadline)
            {
                throw new TimeoutException(
                    "No pending send registered to drive within the deadline — treated as a hang.");
            }

            Thread.Sleep(2);
        }
    }
}
