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
/// The sync mock's control helpers, Java's MockProducer completeNext, errorNext, history and clear; Get blocks, so
/// a pending send is resolved from a second thread.
/// </summary>
public sealed class PublicSyncProducerMockControlTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-mock-control-topic";

    [Fact]
    public async Task CompleteNext_ResolvesPendingSend()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

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
    public void CompleteNext_NoPending_ReturnsFalse()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // No pending send: false, as Java's MockProducer.completeNext() returns.
        Assert.False(producer.CompleteNext());
    }

    [Fact]
    public async Task ErrorNext_FaultsPendingSend()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Get());

        DriveUntilResolved(() => producer.ErrorNext(new KafkaException(2, "sync-mock-error", isRetriable: true)));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => sendTask, s_deadline));
        Assert.Equal(2, failure.Code);
        Assert.Contains("sync-mock-error", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ErrorNext_NoPending_ReturnsFalse()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Assert.False(producer.ErrorNext(new KafkaException(2, "nope", isRetriable: true)));
    }

    [Fact]
    public void HistoryCount_IncrementsPerSend()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Assert.Equal(0, producer.HistoryCount());

        for (int i = 0; i < 3; i++)
        {
            int captured = i;
            TestTimeout.Run(
                () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{captured}"), partition: 0)).Get(),
                s_deadline);
        }

        // The history counts every sent record, completed or not.
        Assert.Equal(3, producer.HistoryCount());
    }

    [Fact]
    public void Clear_ResetsHistory()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Get(),
            s_deadline);
        Assert.Equal(1, producer.HistoryCount());

        producer.Clear();
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public void Helpers_AfterDispose_ThrowObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(producer);

        Assert.Throws<ObjectDisposedException>(() => producer.CompleteNext());
        Assert.Throws<ObjectDisposedException>(() => producer.ErrorNext(new KafkaException(2, "x", isRetriable: true)));
        Assert.Throws<ObjectDisposedException>(() => producer.HistoryCount());
        Assert.Throws<ObjectDisposedException>(() => producer.Clear());
    }

    // Retries drive until it resolves the worker's pending send or the deadline passes: an early call returns
    // false until the worker's send has landed.
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
