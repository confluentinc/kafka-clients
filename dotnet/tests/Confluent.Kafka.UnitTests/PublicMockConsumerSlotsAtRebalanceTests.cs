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
/// The entries of the table that carries a typed record's key and value across the native mock
/// (<c>MockRecordSlots</c>) leave it when the core discards their records, as Java's <c>MockConsumer</c> frees them:
/// <c>Rebalance</c> discards every record it clears, <c>LosePartitions</c> those of the lost partitions. A record a
/// listener adds during the call keeps its entry, and a call the core refuses before it discards anything keeps them
/// all. Each case runs on <see cref="MockConsumer{TKey, TValue}"/> and <see cref="AsyncMockConsumer{TKey, TValue}"/>.
/// </summary>
public sealed class PublicMockConsumerSlotsAtRebalanceTests
{
    private const string Topic = "t";

    private static readonly TopicPartition s_t0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition s_t1 = new TopicPartition(Topic, 1);

    private static readonly TopicPartition s_t2 = new TopicPartition(Topic, 2);

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(60);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Rebalance_RemovesTheEntriesOfTheRecordsItClears(bool async)
    {
        await using Driver driver = await Driver.Subscribed(async, new Listener());
        await driver.Rebalance(s_t0, s_t1);
        await driver.Seek(s_t0, 0);
        await driver.Seek(s_t1, 0);
        driver.AddRecord(s_t0, 0, new object(), new object());
        driver.AddRecord(s_t1, 0, new object(), new object());
        AssertTable(driver, 2, 2);

        await driver.Rebalance(s_t0, s_t1);

        AssertTable(driver, 0, 0);
        Assert.Empty(await driver.Poll());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Rebalance_AListenersAddRecord_IsRefusedByTheCore_AndLeavesNoEntry(bool async, bool inAssigned)
    {
        // Java's synchronized mock lets its listener add a record; the C mock refuses any call into it while the
        // rebalance holds it, so the record never reaches the core and its entries go at once.
        Listener listener = new Listener();
        await using Driver driver = await Driver.Subscribed(async, listener);
        await driver.Rebalance(s_t0, s_t1);
        await driver.Seek(s_t0, 0);
        driver.AddRecord(s_t0, 0, new object(), new object());
        Exception? refused = null;
        Action<IReadOnlyCollection<TopicPartition>> add =
            _ => refused = Record.Exception(() => driver.AddRecord(s_t0, 0, new object(), new object()));
        if (inAssigned)
        {
            listener.Assigned = add;
        }
        else
        {
            listener.Revoked = add;
        }

        await driver.Rebalance(s_t0);

        Assert.IsType<ConcurrentModificationException>(refused);
        AssertTable(driver, 0, 0);
        object key = new object();
        object value = new object();
        driver.AddRecord(s_t0, 0, key, value);
        ConsumerRecord<object, object> record = Assert.Single((await driver.Poll()).Records(s_t0));
        Assert.Same(key, record.Key());
        Assert.Same(value, record.Value());
        AssertTable(driver, 0, 0);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Rebalance_WithAThrowingListener_StillRemovesTheDiscardedEntries(bool async, bool inAssigned)
    {
        Listener listener = new Listener();
        await using Driver driver = await Driver.Subscribed(async, listener);
        await driver.Rebalance(s_t0, s_t1);
        await driver.Seek(s_t0, 0);
        await driver.Seek(s_t1, 0);
        driver.AddRecord(s_t0, 0, new object(), new object());
        driver.AddRecord(s_t1, 0, new object(), new object());
        InvalidOperationException thrown = new InvalidOperationException("listener failed");
        Action<IReadOnlyCollection<TopicPartition>> fail = _ => throw thrown;
        if (inAssigned)
        {
            listener.Assigned = fail;
        }
        else
        {
            listener.Revoked = fail;
        }

        Exception? caught = await Record.ExceptionAsync(() => driver.Rebalance(s_t0));

        Assert.Same(thrown, caught);
        AssertTable(driver, 0, 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Rebalance_RejectedOnAManualAssignment_RemovesTheEntriesTheCoreCleared(bool async)
    {
        // The core clears the records before assignFromSubscribed rejects a manual assignment, as Java's.
        await using Driver driver = Driver.New(async);
        await driver.Assign(s_t0);
        await driver.Seek(s_t0, 0);
        driver.AddRecord(s_t0, 0, new object(), new object());

        await Assert.ThrowsAsync<IllegalArgumentException>(() => driver.Rebalance(s_t0));

        AssertTable(driver, 0, 0);
        Assert.Empty(await driver.Poll());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LosePartitions_RemovesOnlyTheLostPartitionsEntries(bool async)
    {
        await using Driver driver = await Driver.Subscribed(async, new Listener());
        await driver.Rebalance(s_t0, s_t1);
        await driver.Seek(s_t0, 0);
        await driver.Seek(s_t1, 0);
        driver.AddRecord(s_t0, 0, new object(), new object());
        object key = new object();
        object value = new object();
        driver.AddRecord(s_t1, 0, key, value);

        await driver.LosePartitions(s_t0);

        AssertTable(driver, 1, 1);
        ConsumerRecords<object, object> records = await driver.Poll();
        Assert.Empty(records.Records(s_t0));
        ConsumerRecord<object, object> record = Assert.Single(records.Records(s_t1));
        Assert.Same(key, record.Key());
        Assert.Same(value, record.Value());
        AssertTable(driver, 0, 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LosePartitions_RefusedByTheCore_KeepsEveryEntry(bool async)
    {
        await using Driver driver = await Driver.Subscribed(async, new Listener());
        await driver.Rebalance(s_t0, s_t1);
        await driver.Seek(s_t0, 0);
        await driver.Seek(s_t1, 0);
        object key0 = new object();
        object key1 = new object();
        driver.AddRecord(s_t0, 0, key0, new object());
        driver.AddRecord(s_t1, 0, key1, new object());

        IllegalStateException refused =
            await Assert.ThrowsAsync<IllegalStateException>(() => driver.LosePartitions(s_t0, s_t2));

        Assert.StartsWith("Cannot lose partitions that are not currently assigned", refused.Message);
        AssertTable(driver, 2, 2);
        ConsumerRecords<object, object> records = await driver.Poll();
        Assert.Same(key0, Assert.Single(records.Records(s_t0)).Key());
        Assert.Same(key1, Assert.Single(records.Records(s_t1)).Key());
        AssertTable(driver, 0, 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARecordBehindThePosition_KeepsItsEntryAcrossAPoll_AndASeekBackDeliversIt(bool async)
    {
        await using Driver driver = Driver.New(async);
        await driver.Assign(s_t0);
        await driver.Seek(s_t0, 1);
        object behind = new object();
        object ahead = new object();
        driver.AddRecord(s_t0, 0, behind, behind);
        driver.AddRecord(s_t0, 1, ahead, ahead);

        ConsumerRecord<object, object> first = Assert.Single((await driver.Poll()).Records(s_t0));
        Assert.Same(ahead, first.Key());
        AssertTable(driver, 1, 1);

        await driver.Seek(s_t0, 0);
        ConsumerRecord<object, object> second = Assert.Single((await driver.Poll()).Records(s_t0));
        Assert.Equal(0, second.Offset());
        Assert.Same(behind, second.Key());
        Assert.Same(behind, second.Value());
        AssertTable(driver, 0, 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ANullKeyOrValue_TakesNoEntry_AndRebalanceRemovesTheOthers(bool async)
    {
        await using Driver driver = await Driver.Subscribed(async, new Listener());
        await driver.Rebalance(s_t0);
        await driver.Seek(s_t0, 0);
        driver.AddRecord(s_t0, 0, null, null);
        AssertTable(driver, 0, 0);
        driver.AddRecord(s_t0, 1, null, new object());
        AssertTable(driver, 0, 1);
        driver.AddRecord(s_t0, 2, new object(), null);
        AssertTable(driver, 1, 1);

        await driver.Rebalance(s_t0);

        AssertTable(driver, 0, 0);
    }

    [Fact]
    public async Task Async_ACanceledRebalance_StillRemovesTheEntries()
    {
        BlockingAsyncListener listener = new BlockingAsyncListener();
        AsyncMockConsumer<object, object> consumer = new AsyncMockConsumer<object, object>("earliest");
        await using IAsyncDisposable disposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, listener), s_deadline);
        await TestTimeout.Run(() => consumer.Rebalance(new[] { s_t0, s_t1 }), s_deadline);
        await consumer.Seek(s_t0, 0);
        await consumer.Seek(s_t1, 0);
        consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 0, 0, new object(), new object()));
        consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 1, 0, new object(), new object()));
        listener.Arm();
        using CancellationTokenSource cancel = new CancellationTokenSource();

        Task rebalance = consumer.Rebalance(new[] { s_t0 }, cancel.Token);
        Assert.True(listener.Entered.Wait(s_deadline), "the listener never ran");
        cancel.Cancel();
        listener.Release();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => rebalance, s_deadline));
        Assert.False(consumer.Closed());
        AssertTable(SlotsOf(consumer), 0, 0);
    }

    private static void AssertTable(Driver driver, int keys, int values) => AssertTable(driver.Slots, keys, values);

    private static void AssertTable(MockRecordSlots<object, object> slots, int keys, int values)
    {
        Assert.Equal(keys, slots.KeyCount);
        Assert.Equal(values, slots.ValueCount);
    }

    private static MockRecordSlots<object, object> SlotsOf(object consumer) =>
        (MockRecordSlots<object, object>)consumer.GetType()
            .GetField("_slots", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(consumer)!;

    /// <summary>Runs the same steps on either mock.</summary>
    private sealed class Driver : IAsyncDisposable
    {
        private readonly MockConsumer<object, object>? _sync;

        private readonly AsyncMockConsumer<object, object>? _async;

        private Driver(MockConsumer<object, object>? sync, AsyncMockConsumer<object, object>? async)
        {
            _sync = sync;
            _async = async;
        }

        internal MockRecordSlots<object, object> Slots => SlotsOf((object?)_sync ?? _async!);

        internal static Driver New(bool async) => async
            ? new Driver(null, new AsyncMockConsumer<object, object>("earliest"))
            : new Driver(new MockConsumer<object, object>("earliest"), null);

        internal static async Task<Driver> Subscribed(bool async, IConsumerRebalanceListener listener)
        {
            Driver driver = New(async);
            if (driver._sync is not null)
            {
                driver._sync.Subscribe(new[] { Topic }, listener);
            }
            else
            {
                await TestTimeout.Run(() => driver._async!.Subscribe(new[] { Topic }, listener), s_deadline);
            }

            return driver;
        }

        internal Task Assign(params TopicPartition[] partitions) =>
            Run(() => _sync!.Assign(partitions), () => _async!.Assign(partitions));

        internal Task Seek(TopicPartition partition, long offset) =>
            Run(() => _sync!.Seek(partition, offset), () => _async!.Seek(partition, offset));

        internal Task Rebalance(params TopicPartition[] assignment) =>
            Run(() => _sync!.Rebalance(assignment), () => _async!.Rebalance(assignment));

        internal Task LosePartitions(params TopicPartition[] lost) =>
            Run(() => _sync!.LosePartitions(lost), () => _async!.LosePartitions(lost));

        internal void AddRecord(TopicPartition partition, long offset, object? key, object? value)
        {
            ConsumerRecord<object, object> record =
                new ConsumerRecord<object, object>(partition.Topic(), partition.Partition(), offset, key, value);
            if (_sync is not null)
            {
                _sync.AddRecord(record);
            }
            else
            {
                _async!.AddRecord(record);
            }
        }

        internal Task<ConsumerRecords<object, object>> Poll() => _sync is not null
            ? Task.FromResult(_sync.Poll(TimeSpan.Zero))
            : TestTimeout.Run(() => _async!.Poll(TimeSpan.FromMilliseconds(1)), s_deadline);

        public ValueTask DisposeAsync() => _sync is not null
            ? DisposeSync(_sync)
            : new ValueTask(TestTimeout.DisposeAsync(_async));

        private static ValueTask DisposeSync(MockConsumer<object, object> consumer)
        {
            TestTimeout.Dispose(consumer);
            return default;
        }

        // A synchronous call runs on this thread, so its exception reaches the awaiting test as the call's own.
        private Task Run(Action sync, Func<Task> async)
        {
            if (_sync is null)
            {
                return TestTimeout.Run(async, s_deadline);
            }

            try
            {
                sync();
                return Task.CompletedTask;
            }
            catch (Exception exception)
            {
                return Task.FromException(exception);
            }
        }
    }

    /// <summary>A listener whose methods run the action set for them, if any.</summary>
    private sealed class Listener : IConsumerRebalanceListenerWithPartitionsLost
    {
        internal Action<IReadOnlyCollection<TopicPartition>>? Revoked { get; set; }

        internal Action<IReadOnlyCollection<TopicPartition>>? Assigned { get; set; }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Revoked?.Invoke(partitions);

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Assigned?.Invoke(partitions);

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>An async listener whose revocation (of a non-empty set), once armed, waits until the test releases it.</summary>
    private sealed class BlockingAsyncListener : IAsyncConsumerRebalanceListener
    {
        private readonly TaskCompletionSource<bool> _release =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        private volatile bool _armed;

        internal ManualResetEventSlim Entered { get; } = new ManualResetEventSlim();

        internal void Arm() => _armed = true;

        internal void Release() => _release.TrySetResult(true);

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
            if (!_armed)
            {
                return Task.CompletedTask;
            }

            Entered.Set();
            return _release.Task;
        }

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }
}
