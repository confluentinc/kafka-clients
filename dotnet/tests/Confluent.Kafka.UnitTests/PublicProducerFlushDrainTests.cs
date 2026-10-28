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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async Flush covers the sends handed over before it: when it returns the records have reached the core and
/// the sends still resolve; a Flush with nothing pending still reaches the core; a cancelled token throws first.
/// </summary>
public sealed class PublicProducerFlushDrainTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "flush-drain-topic";

    private const int SendCount = 16;

    [Fact]
    public async Task Flush_DrainsAccumulatorRecords_TheyHaveReachedTheCoreWhenItReturns()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Task<RecordMetadata>[] sends = Fire(producer);

        await TestTimeout.Run(() => producer.Flush(), s_deadline);

        Assert.Equal(SendCount, producer.HistoryCount());

        // The flush did not consume the sends: they still resolve.
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
    }

    [Fact]
    public async Task Flush_WithNothingBuffered_StillFlushesTheCore()
    {
        // A producer that never sent, and one whose sends all completed: both still reach the core's flush.
        AsyncMockProducer<byte[], byte[]> neverSent =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable neverSentDisposal = TestTimeout.Disposal(neverSent);
        await TestTimeout.Run(() => neverSent.Flush(), s_deadline);

        AsyncMockProducer<byte[], byte[]> drained =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable drainedDisposal = TestTimeout.Disposal(drained);
        Task<RecordMetadata>[] sends = Fire(drained);
        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        await TestTimeout.Run(() => drained.Flush(), s_deadline);
        Assert.Equal(SendCount, drained.HistoryCount());
    }

    [Fact]
    public async Task Flush_PreCanceledToken_ThrowsSynchronouslyEvenWithRecordsBuffered()
    {
        // A cancelled token throws before anything happens, with records pending too.
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Task<RecordMetadata>[] sends = Fire(producer);

        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        Func<object> flush = () => producer.Flush(cts.Token);
        Assert.Throws<OperationCanceledException>(flush);

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
    }

    [Fact]
    public async Task Flush_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Task<RecordMetadata>[] sends = Fire(producer);
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        foreach (Task<RecordMetadata> send in sends)
        {
            Assert.True(send.IsCompleted);
        }

        Func<object> flush = () => producer.Flush();
        Assert.Throws<ObjectDisposedException>(flush);
    }

    private static Task<RecordMetadata>[] Fire(AsyncMockProducer<byte[], byte[]> producer)
    {
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[SendCount];
        for (int i = 0; i < SendCount; i++)
        {
            sends[i] = producer.Send(new ProducerRecord<byte[], byte[]>(
                Topic, Encoding.UTF8.GetBytes($"value-{i}"), partition: 0)).Delivery();
        }

        return sends;
    }
}
