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
/// The async mock's control helpers, Java's MockProducer completeNext, errorNext, history and clear: a manual send
/// driven to success or failure, the history count, and the closed guard.
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
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Delivery();

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

        // No pending send: false, as Java's MockProducer.completeNext() returns.
        Assert.False(producer.CompleteNext());
    }

    [Fact]
    public async Task ErrorNext_FaultsPendingSend()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Delivery();

        Assert.True(producer.ErrorNext(new KafkaException(2, "mock-error", isRetriable: true)));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            async () => await sendTask);
        Assert.Equal(2, failure.Code);
        Assert.Contains("mock-error", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ErrorNext_NoPending_ReturnsFalse()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Assert.False(producer.ErrorNext(new KafkaException(2, "nope", isRetriable: true)));
    }

    [Fact]
    public async Task HistoryCount_IncrementsPerSend()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        Assert.Equal(0, producer.HistoryCount());

        for (int i = 0; i < 3; i++)
        {
            await TestTimeout.Run(
                async () => await producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0)).Delivery(),
                s_deadline);
        }

        // The history counts every sent record, completed or not.
        Assert.Equal(3, producer.HistoryCount());
    }

    [Fact]
    public async Task Clear_ResetsHistory()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await TestTimeout.Run(
            async () => await producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)).Delivery(),
            s_deadline);
        Assert.Equal(1, producer.HistoryCount());

        producer.Clear();
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public async Task Helpers_AfterDispose_ThrowObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => producer.CompleteNext());
        Assert.Throws<ObjectDisposedException>(() => producer.ErrorNext(new KafkaException(2, "x", isRetriable: true)));
        Assert.Throws<ObjectDisposedException>(() => producer.HistoryCount());
        Assert.Throws<ObjectDisposedException>(() => producer.Clear());
    }

    [Fact]
    public void MockHelpers_RacingDispose_DoNotCrash()
    {
        // The mock helpers take the SafeHandle, so a teardown racing them cannot free the producer under a live native
        // call. A use-after-free corrupts the allocator, so the assertion is that this churn returns at all.
        for (int iteration = 0; iteration < 60; iteration++)
        {
            AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(
                Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));

            TestTimeout.Run(
                () =>
                {
                    Task driver = Task.Run(() =>
                    {
                        for (int i = 0; i < 64; i++)
                        {
                            try
                            {
                                _ = producer.CompleteNext();
                                _ = producer.HistoryCount();
                                producer.Clear();
                            }
                            catch (ObjectDisposedException)
                            {
                                // Expected once teardown wins, from the closed check or the SafeHandle marshaller.
                                return;
                            }
                        }
                    });

                    Task disposer = Task.Run(() => producer.Dispose());

                    Task.WaitAll(driver, disposer);
                },
                s_deadline);
        }
    }
}
