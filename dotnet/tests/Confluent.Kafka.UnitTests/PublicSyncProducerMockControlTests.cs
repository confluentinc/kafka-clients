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
/// The <see cref="MockProducer"/> send-control helpers (M11/P4, inherent on the concrete sync mock,
/// not on <see cref="IProducer"/>): <see cref="MockProducer.CompleteNext"/> /
/// <see cref="MockProducer.ErrorNext"/> / <see cref="MockProducer.HistoryCount()"/> /
/// <see cref="MockProducer.Clear"/> — Java <c>MockProducer</c> / Python <c>_MockProducerMixin</c>
/// parity, mirroring <c>PublicProducerMockControlTests</c> for the async mock (PLAN §7). Because sync
/// <see cref="IProducer.Send"/> blocks, the pending-send helpers are driven from a second thread.
/// </summary>
public sealed class PublicSyncProducerMockControlTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-mock-control-topic";

    // ---- CompleteNext / ErrorNext (cross-thread: Send blocks, helper resolves) ----

    [Fact]
    public async Task CompleteNext_ResolvesPendingSend()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));

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
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        // No pending send → false (Java's MockProducer.completeNext() returns false when the
        // completion queue is empty).
        Assert.False(producer.CompleteNext());
    }

    [Fact]
    public async Task ErrorNext_FaultsPendingSend()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));

        DriveUntilResolved(() => producer.ErrorNext(2, "sync-mock-error"));

        // Assert the code AND the message content (DoD §3 / ffi §A5).
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => sendTask, s_deadline));
        Assert.Equal(2, failure.Code);
        Assert.Contains("sync-mock-error", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ErrorNext_NoPending_ReturnsFalse()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Assert.False(producer.ErrorNext(2, "nope"));
    }

    // ---- HistoryCount / Clear (auto-complete sends resolve without blocking) ----

    [Fact]
    public void HistoryCount_IncrementsPerSend()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Assert.Equal(0, producer.HistoryCount());

        for (int i = 0; i < 3; i++)
        {
            int captured = i;
            TestTimeout.Run(
                () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{captured}"), partition: 0)),
                s_deadline);
        }

        // History tracks every sent record (independent of completion), so 3 sends → 3.
        Assert.Equal(3, producer.HistoryCount());
    }

    [Fact]
    public void Clear_ResetsHistory()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)),
            s_deadline);
        Assert.Equal(1, producer.HistoryCount());

        producer.Clear();
        Assert.Equal(0, producer.HistoryCount());
    }

    // ---- Use-after-dispose guard (before any native call) ----

    [Fact]
    public void Helpers_AfterDispose_ThrowObjectDisposed()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        producer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => producer.CompleteNext());
        Assert.Throws<ObjectDisposedException>(() => producer.ErrorNext(2, "x"));
        Assert.Throws<ObjectDisposedException>(() => producer.HistoryCount());
        Assert.Throws<ObjectDisposedException>(() => producer.Clear());
    }

    /// <summary>
    /// Retries <paramref name="drive"/> from the test thread until it resolves the worker thread's
    /// pending, blocking <see cref="IProducer.Send"/> (returns <see langword="true"/>), or the
    /// deadline elapses (a fail-fast hang guard). See
    /// <c>PublicSyncProducerSendTests.DriveUntilResolved</c> for why the spin is needed.
    /// </summary>
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
