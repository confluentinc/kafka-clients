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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The typed records of <see cref="MockConsumer{TKey, TValue}.AddRecord"/> and
/// <see cref="AsyncMockConsumer{TKey, TValue}.AddRecord"/>, as Java's <c>MockConsumer.addRecord</c> takes them: the
/// polled record holds the very key and value objects added and every field of the record, and the table that
/// carries a key and a value across the native mock (<c>MockRecordSlots</c>) is emptied by delivery and by
/// <c>Close</c>. Each case runs on both mocks.
/// </summary>
public sealed class PublicMockConsumerTypedRecordTests
{
    private const string Topic = "test";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(1);

    private static readonly TopicPartition s_test0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition s_test1 = new TopicPartition(Topic, 1);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AReferenceTypeKeyAndValue_ArePolledAsTheSameObjects(bool async)
    {
        await using Mock<Payload, Payload> consumer = await Assigned<Payload, Payload>(async);
        Payload key = new Payload("k");
        Payload value = new Payload("v");

        consumer.AddRecord(new ConsumerRecord<Payload, Payload>(Topic, 0, 0, key, value));
        ConsumerRecord<Payload, Payload> polled = Assert.Single(await consumer.Poll());

        Assert.Same(key, polled.Key());
        Assert.Same(value, polled.Value());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AValueTypeKeyAndValue_ArePolledUnchanged(bool async)
    {
        await using Mock<int, Guid> consumer = await Assigned<int, Guid>(async);
        Guid value = Guid.NewGuid();

        consumer.AddRecord(new ConsumerRecord<int, Guid>(Topic, 0, 0, 42, value));
        consumer.AddRecord(new ConsumerRecord<int, Guid>(Topic, 0, 1, 0, Guid.Empty));
        ConsumerRecord<int, Guid>[] polled = (await consumer.Poll()).ToArray();

        Assert.Equal(2, polled.Length);
        Assert.Equal(42, polled[0].Key());
        Assert.Equal(value, polled[0].Value());
        Assert.Equal(0, polled[1].Key());
        Assert.Equal(Guid.Empty, polled[1].Value());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ANullKeyAndANullValue_ArePolledAsNull(bool async)
    {
        await using Mock<Payload, Payload> consumer = await Assigned<Payload, Payload>(async);
        Payload value = new Payload("v");
        Payload key = new Payload("k");

        consumer.AddRecord(new ConsumerRecord<Payload, Payload>(Topic, 0, 0, null, value));
        consumer.AddRecord(new ConsumerRecord<Payload, Payload>(Topic, 0, 1, key, null));
        consumer.AddRecord(new ConsumerRecord<Payload, Payload>(Topic, 0, 2, null, null));
        ConsumerRecord<Payload, Payload>[] polled = (await consumer.Poll()).ToArray();

        Assert.Equal(3, polled.Length);
        Assert.Null(polled[0].Key());
        Assert.Same(value, polled[0].Value());
        Assert.Same(key, polled[1].Key());
        Assert.Null(polled[1].Value());
        Assert.Null(polled[2].Key());
        Assert.Null(polled[2].Value());
        Assert.Equal(0, consumer.KeyCount);
        Assert.Equal(0, consumer.ValueCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EveryFieldOfTheRecordAdded_IsPolled(bool async)
    {
        await using Mock<string, string> consumer = await Assigned<string, string>(async);
        IHeaders headers = new RecordHeaders().Add("h1", Bytes("one")).Add("h2", null).Add("h1", Bytes("again"));
        ConsumerRecord<string, string> added = new ConsumerRecord<string, string>(
            Topic, 0, 7, 1_700_000_000_000L, TimestampType.LOG_APPEND_TIME, 11, 13, "key", "value", headers, 5, 3);

        consumer.AddRecord(added);
        ConsumerRecord<string, string> polled = Assert.Single(await consumer.Poll());

        Assert.Equal(Topic, polled.Topic());
        Assert.Equal(0, polled.Partition());
        Assert.Equal(7L, polled.Offset());
        Assert.Equal(1_700_000_000_000L, polled.Timestamp());
        Assert.Equal(TimestampType.LOG_APPEND_TIME, polled.TimestampType());
        Assert.Equal(11, polled.SerializedKeySize());
        Assert.Equal(13, polled.SerializedValueSize());
        Assert.Same(added.Key(), polled.Key());
        Assert.Same(added.Value(), polled.Value());
        Assert.Equal(5, polled.LeaderEpoch());
        Assert.Equal((short)3, polled.DeliveryCount());
        IHeader[] polledHeaders = polled.Headers().ToArray();
        Assert.Equal(3, polledHeaders.Length);
        Assert.Equal("h1", polledHeaders[0].Key());
        Assert.Equal(Bytes("one"), polledHeaders[0].Value());
        Assert.Equal("h2", polledHeaders[1].Key());
        Assert.Null(polledHeaders[1].Value());
        Assert.Equal("h1", polledHeaders[2].Key());
        Assert.Equal(Bytes("again"), polledHeaders[2].Value());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADeliveredRecord_LeavesNoEntryInTheTable(bool async)
    {
        await using Mock<string, string> consumer = await Assigned<string, string>(async);

        consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 0, 0, "k0", "v0"));
        consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 0, 1, null, "v1"));
        // A null key takes no entry: it crosses as NULL.
        Assert.Equal(1, consumer.KeyCount);
        Assert.Equal(2, consumer.ValueCount);

        Assert.Equal(2, (await consumer.Poll()).Count());

        Assert.Equal(0, consumer.KeyCount);
        Assert.Equal(0, consumer.ValueCount);
        Assert.Empty(await consumer.Poll());
    }

    // Java's poll keeps a record below the position, and one for a partition that left the assignment, in its queue
    // and never delivers it; Close is what lets the table go.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARecordTheMockNeverDelivers_LeavesTheTableAtClose(bool async)
    {
        Mock<string, string> consumer = await Assigned<string, string>(async, new[] { s_test0, s_test1 });
        try
        {
            await consumer.Seek(s_test0, 5);
            consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 0, 2, "below", "position"));
            consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 1, 0, "left", "assignment"));
            await consumer.Assign(new[] { s_test0 });

            Assert.Empty(await consumer.Poll());
            Assert.Equal(2, consumer.KeyCount);
            Assert.Equal(2, consumer.ValueCount);

            await consumer.Close();

            Assert.Equal(0, consumer.KeyCount);
            Assert.Equal(0, consumer.ValueCount);
        }
        finally
        {
            await consumer.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisposingTheConsumer_EmptiesTheTable(bool async)
    {
        Mock<string, string> consumer = await Assigned<string, string>(async);
        consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 0, 0, "k", "v"));
        Assert.Equal(1, consumer.KeyCount);

        await consumer.DisposeAsync();

        Assert.Equal(0, consumer.KeyCount);
        Assert.Equal(0, consumer.ValueCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARecordForAnUnassignedPartition_IsRefusedAndLeavesNoEntry(bool async)
    {
        await using Mock<string, string> consumer = await Assigned<string, string>(async);

        IllegalStateException error = Assert.Throws<IllegalStateException>(
            () => consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 1, 0, "k", "v")));

        Assert.Equal("Cannot add records for a partition that is not assigned to the consumer", error.Message);
        Assert.Equal(0, consumer.KeyCount);
        Assert.Equal(0, consumer.ValueCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnderSetMaxPollRecords_OnlyTheDeliveredRecordsLeaveTheTable(bool async)
    {
        await using Mock<string, string> consumer = await Assigned<string, string>(async);
        string[] keys = { "k0", "k1", "k2" };
        for (int offset = 0; offset < keys.Length; offset++)
        {
            consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 0, offset, keys[offset], "v"));
        }

        consumer.SetMaxPollRecords(2L);

        ConsumerRecord<string, string>[] first = (await consumer.Poll()).ToArray();
        Assert.Equal(2, first.Length);
        Assert.Same(keys[0], first[0].Key());
        Assert.Same(keys[1], first[1].Key());
        Assert.Equal(1, consumer.KeyCount);
        Assert.Equal(1, consumer.ValueCount);

        ConsumerRecord<string, string> last = Assert.Single(await consumer.Poll());
        Assert.Same(keys[2], last.Key());
        Assert.Equal(0, consumer.KeyCount);
        Assert.Equal(0, consumer.ValueCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ANullRecord_ThrowsArgumentNullException(bool async)
    {
        await using Mock<string, string> consumer = await Assigned<string, string>(async);

        Assert.Equal("record", Assert.Throws<ArgumentNullException>(() => consumer.AddRecord(null!)).ParamName);
        Assert.Equal(0, consumer.KeyCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AddRecordOnAClosedConsumer_ThrowsIllegalStateException(bool async)
    {
        await using Mock<string, string> consumer = await Assigned<string, string>(async);
        await consumer.Close();

        Assert.Throws<IllegalStateException>(
            () => consumer.AddRecord(new ConsumerRecord<string, string>(Topic, 0, 0, "k", "v")));
        Assert.Equal(0, consumer.KeyCount);
    }

    // Java's methods are synchronized, so a record added by another thread while a poll runs is delivered by that
    // poll or a later one, once, with its own key and value.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AddRecordFromAnotherThreadWhilePolling_DeliversEveryRecordOnce(bool async)
    {
        const int count = 200;
        await using Mock<string, Payload> consumer = await Assigned<string, Payload>(async);
        string[] keys = Enumerable.Range(0, count).Select(i => "k" + i).ToArray();
        Payload[] values = Enumerable.Range(0, count).Select(i => new Payload("v" + i)).ToArray();
        List<ConsumerRecord<string, Payload>> polled = new List<ConsumerRecord<string, Payload>>();
        using CancellationTokenSource stop = new CancellationTokenSource(s_deadline);

        Task adder = Task.Run(() =>
        {
            for (int offset = 0; offset < count; offset++)
            {
                consumer.AddRecord(new ConsumerRecord<string, Payload>(Topic, 0, offset, keys[offset], values[offset]));
            }
        });
        while (polled.Count < count && !stop.IsCancellationRequested)
        {
            polled.AddRange(await consumer.Poll());
        }

        await adder;
        Assert.Equal(count, polled.Count);
        for (int offset = 0; offset < count; offset++)
        {
            Assert.Equal(offset, polled[offset].Offset());
            Assert.Same(keys[offset], polled[offset].Key());
            Assert.Same(values[offset], polled[offset].Value());
        }

        Assert.Equal(0, consumer.KeyCount);
        Assert.Equal(0, consumer.ValueCount);
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static async Task<Mock<TKey, TValue>> Assigned<TKey, TValue>(
        bool async,
        IReadOnlyCollection<TopicPartition>? partitions = null)
    {
        partitions ??= new[] { s_test0 };
        Mock<TKey, TValue> consumer = async ? new AsyncMock<TKey, TValue>() : new SyncMock<TKey, TValue>();
        await consumer.Assign(partitions);
        foreach (TopicPartition partition in partitions)
        {
            await consumer.Seek(partition, 0);
        }

        return consumer;
    }

    private static MockRecordSlots<TKey, TValue> Slots<TKey, TValue>(object consumer) =>
        (MockRecordSlots<TKey, TValue>)consumer.GetType()
            .GetField("_slots", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(consumer)!;

    private sealed class Payload
    {
        internal Payload(string text) => Text = text;

        internal string Text { get; }
    }

    // One shape over both mocks, so each case states its steps once and runs on both.
    private abstract class Mock<TKey, TValue> : IAsyncDisposable
    {
        internal int KeyCount => Slots<TKey, TValue>(Consumer).KeyCount;

        internal int ValueCount => Slots<TKey, TValue>(Consumer).ValueCount;

        protected abstract object Consumer { get; }

        internal abstract void AddRecord(ConsumerRecord<TKey, TValue> record);

        internal abstract void SetMaxPollRecords(long maxPollRecords);

        internal abstract Task Assign(IReadOnlyCollection<TopicPartition> partitions);

        internal abstract Task Seek(TopicPartition partition, long offset);

        internal abstract Task<ConsumerRecords<TKey, TValue>> Poll();

        internal abstract Task Close();

        public abstract ValueTask DisposeAsync();
    }

    private sealed class SyncMock<TKey, TValue> : Mock<TKey, TValue>
    {
        private readonly MockConsumer<TKey, TValue> _consumer = new MockConsumer<TKey, TValue>("latest");

        protected override object Consumer => _consumer;

        internal override void AddRecord(ConsumerRecord<TKey, TValue> record) => _consumer.AddRecord(record);

        internal override void SetMaxPollRecords(long maxPollRecords) => _consumer.SetMaxPollRecords(maxPollRecords);

        internal override Task Assign(IReadOnlyCollection<TopicPartition> partitions)
        {
            _consumer.Assign(partitions);
            return Task.CompletedTask;
        }

        internal override Task Seek(TopicPartition partition, long offset)
        {
            _consumer.Seek(partition, offset);
            return Task.CompletedTask;
        }

        internal override Task<ConsumerRecords<TKey, TValue>> Poll()
        {
            ConsumerRecords<TKey, TValue> records = default!;
            TestTimeout.Run(() => records = _consumer.Poll(s_pollTimeout), s_deadline);
            return Task.FromResult(records);
        }

        internal override Task Close()
        {
            TestTimeout.Run(() => _consumer.Close(), s_deadline);
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync()
        {
            TestTimeout.Dispose(_consumer);
            return default;
        }
    }

    private sealed class AsyncMock<TKey, TValue> : Mock<TKey, TValue>
    {
        private readonly AsyncMockConsumer<TKey, TValue> _consumer = new AsyncMockConsumer<TKey, TValue>("latest");

        protected override object Consumer => _consumer;

        internal override void AddRecord(ConsumerRecord<TKey, TValue> record) => _consumer.AddRecord(record);

        internal override void SetMaxPollRecords(long maxPollRecords) => _consumer.SetMaxPollRecords(maxPollRecords);

        internal override Task Assign(IReadOnlyCollection<TopicPartition> partitions) => _consumer.Assign(partitions);

        internal override Task Seek(TopicPartition partition, long offset) => _consumer.Seek(partition, offset);

        internal override async Task<ConsumerRecords<TKey, TValue>> Poll()
        {
            ConsumerRecords<TKey, TValue> records = default!;
            await TestTimeout.Run(async () => records = await _consumer.Poll(s_pollTimeout), s_deadline);
            return records;
        }

        internal override Task Close() => TestTimeout.Run(() => _consumer.Close(), s_deadline);

        public override ValueTask DisposeAsync() => new ValueTask(TestTimeout.DisposeAsync(_consumer));
    }
}
