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
using System.Text;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The <see cref="AsyncMockProducer"/> send-control helpers (M11/P3, inherent on the concrete mock,
/// not on <see cref="IAsyncProducer"/>): <see cref="AsyncMockProducer.CompleteNext"/> /
/// <see cref="AsyncMockProducer.ErrorNext"/> / <see cref="AsyncMockProducer.HistoryCount()"/> /
/// <see cref="AsyncMockProducer.Clear"/> — Java <c>MockProducer</c> / Python <c>_MockProducerMixin</c>
/// parity (PLAN §5). Drives a manual (<c>autoComplete: false</c>) send to success / failure and
/// checks the history counter.
/// </summary>
public sealed class PublicProducerMockControlTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "mock-control-topic";

    [Fact]
    public async Task CompleteNext_ResolvesPendingSend()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));

        // The send is pending (manual mode) until completeNext resolves it.
        Assert.True(producer.CompleteNext());

        RecordMetadata metadata = null!;
        await TestTimeout.Run(async () => metadata = await sendTask, s_deadline);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
    }

    [Fact]
    public void CompleteNext_NoPending_ReturnsFalse()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        // No pending send → false (Java's MockProducer.completeNext() returns false when the
        // completion queue is empty).
        Assert.False(producer.CompleteNext());
    }

    [Fact]
    public async Task ErrorNext_FaultsPendingSend()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));

        Assert.True(producer.ErrorNext(2, "mock-error"));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            async () => await sendTask);
        Assert.Equal(2, failure.Code);
        Assert.Contains("mock-error", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ErrorNext_NoPending_ReturnsFalse()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Assert.False(producer.ErrorNext(2, "nope"));
    }

    [Fact]
    public async Task HistoryCount_IncrementsPerSend()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Assert.Equal(0, producer.HistoryCount());

        for (int i = 0; i < 3; i++)
        {
            await TestTimeout.Run(
                async () => await producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0)),
                s_deadline);
        }

        // History tracks every sent record (independent of completion), so 3 sends → 3.
        Assert.Equal(3, producer.HistoryCount());
    }

    [Fact]
    public async Task Clear_ResetsHistory()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await TestTimeout.Run(
            async () => await producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)),
            s_deadline);
        Assert.Equal(1, producer.HistoryCount());

        producer.Clear();
        Assert.Equal(0, producer.HistoryCount());
    }

    // ---- Use-after-dispose guard (before any native call) ----

    [Fact]
    public async Task Helpers_AfterDispose_ThrowObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => producer.CompleteNext());
        Assert.Throws<ObjectDisposedException>(() => producer.ErrorNext(2, "x"));
        Assert.Throws<ObjectDisposedException>(() => producer.HistoryCount());
        Assert.Throws<ObjectDisposedException>(() => producer.Clear());
    }
}
