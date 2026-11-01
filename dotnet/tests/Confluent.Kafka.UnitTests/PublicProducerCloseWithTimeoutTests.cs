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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Close(TimeSpan)</c> on <see cref="KafkaProducer{TKey, TValue}"/> and
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> over an unreachable broker: a negative timeout is refused before
/// the closed check, and a timed close is bounded and fails the sends it could not finish.
/// </summary>
public sealed class PublicProducerCloseWithTimeoutTests
{
    private const string NegativeTimeout = "The timeout cannot be negative.";

    private const int Senders = 5;

    private static readonly TimeSpan s_negative = TimeSpan.FromMilliseconds(-100);

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task Close_NegativeTimeout_ThrowsArgumentOutOfRangeException(string flavor)
    {
        if (flavor == SyncFlavor)
        {
            KafkaProducer<byte[], byte[]> producer = Sync(60000);
            using IDisposable producerDisposal = TestTimeout.Disposal(producer);
            AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => producer.Close(s_negative)));

            Assert.NotNull(producer.Metrics());
            TestTimeout.Run(() => producer.Close(TimeSpan.Zero), Deadline);
            Assert.Throws<ObjectDisposedException>(() => producer.Send(Record(Topic("negative-close"))));
        }
        else
        {
            AsyncKafkaProducer<byte[], byte[]> producer = Async(60000);
            await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
            AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = producer.Close(s_negative); }));

            Assert.NotNull(producer.Metrics());
            await TestTimeout.Run(() => producer.Close(TimeSpan.Zero), Deadline);
            Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record(Topic("negative-close"))); });
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task Close_NegativeTimeout_OnAClosedProducer_StillThrows(string flavor)
    {
        if (flavor == SyncFlavor)
        {
            KafkaProducer<byte[], byte[]> producer = Sync(60000);
            using IDisposable producerDisposal = TestTimeout.Disposal(producer);
            TestTimeout.Run(producer.Close, Deadline);
            AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => producer.Close(s_negative)));
        }
        else
        {
            AsyncKafkaProducer<byte[], byte[]> producer = Async(60000);
            await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
            await TestTimeout.Run(() => producer.Close(), Deadline);
            AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = producer.Close(s_negative); }));
        }
    }

    [Fact]
    public async Task CloseWithTimeout_IsBounded_AndFailsWhatItCouldNotSend()
    {
        const int maxBlockMs = 60000;
        string topic = Topic("timed-close");
        KafkaProducer<byte[], byte[]> producer = Sync(maxBlockMs);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        Task<Exception?>[] sends = new Task<Exception?>[Senders];
        for (int i = 0; i < Senders; i++)
        {
            sends[i] = Task.Factory.StartNew(
                () => SendOutcome(producer, topic),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        await Task.Delay(Settle);
        Assert.DoesNotContain(sends, send => send.IsCompleted);

        Stopwatch elapsed = Stopwatch.StartNew();
        TestTimeout.Run(() => producer.Close(TimeSpan.FromSeconds(1)), Deadline);
        elapsed.Stop();
        Assert.True(elapsed.ElapsedMilliseconds < 10000, $"Close(1 s) took {elapsed.ElapsedMilliseconds} ms");

        Exception?[] outcomes = await TestTimeout.Run(() => Task.WhenAll(sends), Deadline);
        Assert.All(
            outcomes,
            outcome => Assert.True(
                outcome is KafkaException || (outcome is InvalidOperationException && outcome is not ObjectDisposedException),
                $"a send the close ended had {outcome?.GetType().Name ?? "no error"}: {outcome?.Message}"));
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task MockProducer_Close_NegativeTimeout_ClosesAsForZero(string flavor)
    {
        string topic = Topic("mock-negative-close");
        if (flavor == SyncFlavor)
        {
            MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            using IDisposable producerDisposal = TestTimeout.Disposal(producer);
            Assert.Equal(0L, producer.Send(Record(topic)).Get(Deadline).Offset);

            TestTimeout.Run(() => producer.Close(s_negative), Deadline);
            Assert.Throws<ObjectDisposedException>(() => producer.Send(Record(topic)));
            TestTimeout.Run(() => producer.Close(s_negative), Deadline);
        }
        else
        {
            AsyncMockProducer<byte[], byte[]> producer =
                new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            await using IAsyncDisposable producerDisposal = TestTimeout.AsyncDisposal(producer);
            AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => producer.Send(Record(topic)).AsTask(), Deadline);
            Assert.Equal(0L, (await TestTimeout.Run(() => future.Get(), Deadline)).Offset);

            await TestTimeout.Run(() => producer.Close(s_negative), Deadline);
            Assert.Throws<ObjectDisposedException>(() => { _ = producer.Send(Record(topic)); });
            await TestTimeout.Run(() => producer.Close(s_negative), Deadline);
        }
    }

    [Fact(Skip = "transactions are not exposed (dotnet/CLAUDE.md:65)")]
    public void OnlyCanExecuteCloseAfterInitTransactionsTimeout()
    {
    }

    private static void AssertNegativeTimeout(ArgumentOutOfRangeException thrown)
    {
        Assert.Equal("timeout", thrown.ParamName);
        Assert.StartsWith(NegativeTimeout, thrown.Message, StringComparison.Ordinal);
    }

    private static Exception? SendOutcome(KafkaProducer<byte[], byte[]> producer, string topic)
    {
        KafkaFuture<RecordMetadata> future;
        try
        {
            future = producer.Send(Record(topic));
        }
        catch (Exception thrown)
        {
            return thrown;
        }

        try
        {
            future.Get(Deadline);
            return null;
        }
        catch (Exception failed)
        {
            return failed;
        }
    }
}
