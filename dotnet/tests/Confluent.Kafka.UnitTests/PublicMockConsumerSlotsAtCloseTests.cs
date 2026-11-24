// Copyright 2026 Confluent Inc.
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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// When the table that carries a typed record's key and value across the native mock (<c>MockRecordSlots</c>) is
/// emptied: only once the consumer is closed. A close or a dispose that throws, is canceled or is refused leaves
/// the consumer open, and its next <c>Poll</c> returns the record added, with the same key and value objects, as
/// Java's <c>MockConsumer</c> (whose close never touches its records). A close or a dispose from another thread
/// while a <c>Poll</c> runs either loses, and the poll returns its records, or wins, and the poll sees a closed (or
/// woken) consumer; the poll never finds its ids gone.
/// </summary>
public sealed class PublicMockConsumerSlotsAtCloseTests
{
    private const string Topic = "t";

    private const string MissingSlotPrefix = "The mock consumer holds no";

    private const int RaceIterations = 30;

    private const int RaceRecords = 20000;

    private static readonly TopicPartition s_t0 = new TopicPartition(Topic, 0);

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(60);

    private readonly object _key = new object();

    private readonly object _value = new object();

    [Fact]
    public void Sync_CloseWithNullOptions_LeavesTheRecordPollable()
    {
        using MockConsumer<object, object> consumer = SyncWithRecord();

        Assert.Throws<ArgumentNullException>(() => consumer.Close(null!));

        AssertOpenWithTheRecord(consumer);
        AssertPollsTheRecord(consumer.Poll(TimeSpan.Zero));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Sync_CloseOrDisposeRefusedInsideACommitCallback_LeavesTheRecordPollable(bool dispose)
    {
        using MockConsumer<object, object> consumer = SyncWithRecord();
        Exception? caught = null;
        bool ran = false;
        consumer.CommitAsync(new DelegateCallback(() =>
        {
            ran = true;
            try
            {
                if (dispose)
                {
                    consumer.Dispose();
                }
                else
                {
                    consumer.Close();
                }
            }
            catch (Exception exception)
            {
                caught = exception;
            }
        }));

        Assert.True(ran);
        Assert.IsType<ConcurrentModificationException>(caught);
        AssertOpenWithTheRecord(consumer);
        AssertPollsTheRecord(consumer.Poll(TimeSpan.Zero));
    }

    [Fact]
    public async Task Async_CloseWithNullOptions_LeavesTheRecordPollable()
    {
        AsyncMockConsumer<object, object> consumer = await AsyncWithRecord();
        await using IAsyncDisposable disposal = TestTimeout.AsyncDisposal(consumer);

        // The argument check throws at the call, before a task exists.
        Assert.Throws<ArgumentNullException>(() => { _ = consumer.Close((CloseOptions)null!); });

        AssertOpenWithTheRecord(consumer);
        AssertPollsTheRecord(await consumer.Poll(TimeSpan.FromMilliseconds(1)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Async_CloseWithACanceledToken_LeavesTheRecordPollable(bool withOptions)
    {
        AsyncMockConsumer<object, object> consumer = await AsyncWithRecord();
        await using IAsyncDisposable disposal = TestTimeout.AsyncDisposal(consumer);
        CancellationToken canceled = new CancellationToken(true);

        Task closing = withOptions
            ? consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(1)), canceled)
            : consumer.Close(canceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => closing);

        AssertOpenWithTheRecord(consumer);
        AssertPollsTheRecord(await consumer.Poll(TimeSpan.FromMilliseconds(1)));
    }

    [Fact]
    public async Task Async_CloseRefusedInsideACommitCallback_LeavesTheRecordPollable()
    {
        AsyncMockConsumer<object, object> consumer = await AsyncWithRecord();
        await using IAsyncDisposable disposal = TestTimeout.AsyncDisposal(consumer);
        Task? closing = null;
        await consumer.CommitAsync(new DelegateCallback(() => closing = consumer.Close()));

        Assert.NotNull(closing);
        await Assert.ThrowsAsync<ConcurrentModificationException>(() => closing!);

        AssertOpenWithTheRecord(consumer);
        AssertPollsTheRecord(await consumer.Poll(TimeSpan.FromMilliseconds(1)));
    }

    [Fact]
    public async Task Async_CloseRefusedWhileAnotherThreadsPollHoldsTheConsumer_LeavesTheRestPollable()
    {
        // The mock has no hook to hold a poll open, so a large batch on t-0 keeps the poll inside the consumer long
        // enough for a close from another thread to be refused; the test repeats until one is. The record on t-1 is
        // paused, so that poll leaves it in the table.
        TopicPartition t1 = new TopicPartition(Topic, 1);
        for (int iteration = 0; iteration < 100; iteration++)
        {
            AsyncMockConsumer<object, object> consumer = await AsyncWithRecords(RaceRecords, t1);
            try
            {
                consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 1, 0, _key, _value));
                await consumer.Pause(new[] { t1 });
                using ManualResetEventSlim started = new ManualResetEventSlim();
                Task<ConsumerRecords<object, object>> polling = Task.Run(async () =>
                {
                    started.Set();
                    return await consumer.Poll(TimeSpan.FromSeconds(5));
                });
                started.Wait();
                Thread.SpinWait(2000);

                Task closing = Task.Run(() => consumer.Close());
                try
                {
                    await TestTimeout.Run(() => closing, s_deadline);
                    await PollOutcome(polling, wakeAllowed: false);
                    continue;
                }
                catch (ConcurrentModificationException)
                {
                }

                Assert.Equal(RaceRecords, (await TestTimeout.Run(() => polling, s_deadline)).Count());
                Assert.False(consumer.Closed());
                AssertTable(consumer, 1);
                await consumer.Resume(new[] { t1 });
                ConsumerRecord<object, object> record =
                    Assert.Single((await consumer.Poll(TimeSpan.FromMilliseconds(1))).Records(t1));
                Assert.Same(_key, record.Key());
                Assert.Same(_value, record.Value());
                return;
            }
            finally
            {
                await TestTimeout.DisposeAsync(consumer);
            }
        }

        Assert.Fail("No close was refused while a poll held the consumer in 100 attempts.");
    }

    [Fact]
    public void Sync_ASuccessfulCloseWithOptions_EmptiesTheTable()
    {
        using MockConsumer<object, object> consumer = SyncWithRecord();

        consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(1)));

        Assert.True(consumer.Closed());
        AssertTable(consumer, 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Async_ASuccessfulCloseOrDispose_EmptiesTheTable(bool syncDispose)
    {
        AsyncMockConsumer<object, object> consumer = await AsyncWithRecord();

        if (syncDispose)
        {
            TestTimeout.Dispose(consumer);
        }
        else
        {
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(1))), s_deadline);
            Assert.True(consumer.Closed());
        }

        AssertTable(consumer, 0);
    }

    [Fact]
    public async Task Async_ADisposeAsyncThatClosesInsideACommitCallback_EmptiesTheTable()
    {
        AsyncMockConsumer<object, object> consumer = await AsyncWithRecord();
        Task? disposing = null;
        await consumer.CommitAsync(new DelegateCallback(() => disposing = consumer.DisposeAsync().AsTask()));

        Assert.NotNull(disposing);
        await TestTimeout.Run(() => disposing!, s_deadline);

        // Inside a callback the dispose schedules the teardown, which runs once the commit's operation has ended; the
        // table empties when the consumer is torn down.
        Assert.True(SpinWait.SpinUntil(() => TableEntries(consumer) == 0, s_deadline));
        Assert.True(consumer.Closed());
    }

    [Fact]
    public void Sync_CloseFromAnotherThreadWhilePolling_NeverLosesTheBatchsIds()
    {
        for (int iteration = 0; iteration < RaceIterations; iteration++)
        {
            using MockConsumer<object, object> consumer = SyncWithRecords(RaceRecords);
            using ManualResetEventSlim started = new ManualResetEventSlim();
            Task<ConsumerRecords<object, object>> polling = Task.Run(() =>
            {
                started.Set();
                return consumer.Poll(TimeSpan.FromSeconds(5));
            });
            started.Wait();
            Thread.SpinWait(2000);

            TestTimeout.Run(() => consumer.Close(), s_deadline);

            AssertThePollEndedCleanly(polling, wakeAllowed: false);
            AssertTable(consumer, 0);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Async_CloseOrDisposeFromAnotherThreadWhilePolling_NeverLosesTheBatchsIds(bool syncDispose)
    {
        for (int iteration = 0; iteration < RaceIterations; iteration++)
        {
            AsyncMockConsumer<object, object> consumer = await AsyncWithRecords(RaceRecords);
            try
            {
                using ManualResetEventSlim started = new ManualResetEventSlim();
                Task<ConsumerRecords<object, object>> polling = Task.Run(async () =>
                {
                    started.Set();
                    return await consumer.Poll(TimeSpan.FromSeconds(5));
                });
                started.Wait();
                Thread.SpinWait(2000);

                Exception? closeFailure = null;
                try
                {
                    Task closing = syncDispose ? Task.Run(() => consumer.Dispose()) : Task.Run(() => consumer.Close());
                    await TestTimeout.Run(() => closing, s_deadline);
                }
                catch (ConcurrentModificationException exception)
                {
                    // An async close is refused while another thread's call holds the consumer.
                    closeFailure = exception;
                }

                ConsumerRecords<object, object>? polled = await PollOutcome(polling, wakeAllowed: syncDispose);
                if (closeFailure is null)
                {
                    AssertTable(consumer, 0);
                }
                else
                {
                    // The refused close left the consumer open, and the records the poll did not take keep their ids.
                    Assert.False(consumer.Closed());
                    AssertTable(consumer, RaceRecords - (polled?.Count() ?? 0));
                }
            }
            finally
            {
                await TestTimeout.DisposeAsync(consumer);
            }
        }
    }

    private static void AssertThePollEndedCleanly(Task<ConsumerRecords<object, object>> polling, bool wakeAllowed) =>
        PollOutcome(polling, wakeAllowed).GetAwaiter().GetResult();

    // The poll either returned its records or met the closed (or, for a dispose, woken) consumer.
    private static async Task<ConsumerRecords<object, object>?> PollOutcome(
        Task<ConsumerRecords<object, object>> polling,
        bool wakeAllowed)
    {
        try
        {
            return await TestTimeout.Run(() => polling, s_deadline);
        }
        catch (IllegalStateException exception) when (!exception.Message.StartsWith(MissingSlotPrefix, StringComparison.Ordinal))
        {
            Assert.Equal("This consumer has already been closed.", exception.Message);
            return null;
        }
        catch (WakeupException) when (wakeAllowed)
        {
            return null;
        }
    }

    private MockConsumer<object, object> SyncWithRecord()
    {
        MockConsumer<object, object> consumer = new MockConsumer<object, object>("earliest");
        consumer.Assign(new[] { s_t0 });
        consumer.Seek(s_t0, 0);
        consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 0, 0, _key, _value));
        return consumer;
    }

    private static MockConsumer<object, object> SyncWithRecords(int count)
    {
        MockConsumer<object, object> consumer = new MockConsumer<object, object>("earliest");
        consumer.Assign(new[] { s_t0 });
        consumer.Seek(s_t0, 0);
        for (int offset = 0; offset < count; offset++)
        {
            consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 0, offset, offset, offset));
        }

        return consumer;
    }

    private async Task<AsyncMockConsumer<object, object>> AsyncWithRecord()
    {
        AsyncMockConsumer<object, object> consumer = new AsyncMockConsumer<object, object>("earliest");
        await consumer.Assign(new[] { s_t0 });
        await consumer.Seek(s_t0, 0);
        consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 0, 0, _key, _value));
        return consumer;
    }

    private static async Task<AsyncMockConsumer<object, object>> AsyncWithRecords(int count, TopicPartition? alsoAssigned = null)
    {
        AsyncMockConsumer<object, object> consumer = new AsyncMockConsumer<object, object>("earliest");
        await consumer.Assign(alsoAssigned is null ? new[] { s_t0 } : new[] { s_t0, alsoAssigned });
        await consumer.Seek(s_t0, 0);
        if (alsoAssigned is not null)
        {
            await consumer.Seek(alsoAssigned, 0);
        }

        for (int offset = 0; offset < count; offset++)
        {
            consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 0, offset, offset, offset));
        }

        return consumer;
    }

    private static void AssertOpenWithTheRecord(object consumer)
    {
        bool closed = consumer is MockConsumer<object, object> sync
            ? sync.Closed()
            : ((AsyncMockConsumer<object, object>)consumer).Closed();
        Assert.False(closed);
        AssertTable(consumer, 1);
    }

    private void AssertPollsTheRecord(ConsumerRecords<object, object> records)
    {
        ConsumerRecord<object, object> record = Assert.Single(records.Records(s_t0));
        Assert.Equal(0, record.Offset());
        Assert.Same(_key, record.Key());
        Assert.Same(_value, record.Value());
    }

    private static void AssertTable(object consumer, int entries)
    {
        MockRecordSlots<object, object> slots = SlotsOf(consumer);
        Assert.Equal(entries, slots.KeyCount);
        Assert.Equal(entries, slots.ValueCount);
    }

    private static int TableEntries(object consumer)
    {
        MockRecordSlots<object, object> slots = SlotsOf(consumer);
        return slots.KeyCount + slots.ValueCount;
    }

    private static MockRecordSlots<object, object> SlotsOf(object consumer) =>
        (MockRecordSlots<object, object>)consumer.GetType()
            .GetField("_slots", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(consumer)!;

    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action _onComplete;

        internal DelegateCallback(Action onComplete) => _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }
}
