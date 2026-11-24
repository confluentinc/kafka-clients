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
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A cancelled <c>AsyncMockConsumer.Poll</c> never loses a record. The C mock removes what a poll delivers, as
/// Java's <c>MockConsumer.poll</c> does, so a batch it has delivered is returned whatever the token; a cancellation
/// that ends the poll before any record is delivered throws <see cref="OperationCanceledException"/> and leaves
/// the records for the next poll.
/// </summary>
public sealed class PublicAsyncMockConsumerCancelledPollTests
{
    private const string Topic = "t";

    private const int Records = 3;

    private static readonly TopicPartition s_t0 = new TopicPartition(Topic, 0);

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private readonly object[] _keys = Enumerable.Range(0, Records + 1).Select(_ => new object()).ToArray();

    private readonly object[] _values = Enumerable.Range(0, Records + 1).Select(_ => new object()).ToArray();

    [Fact]
    public async Task ATokenCancelledAtTheCall_Throws_AndTheRecordsArePolledNext()
    {
        AsyncMockConsumer<object, object> consumer = await WithRecords();
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            cancel.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(
                () => consumer.Poll(TimeSpan.FromSeconds(30), cancel.Token));

            AssertTable(consumer, Records);
            Assert.Equal(0L, await WithDeadline(consumer.Position(s_t0)));
            AssertTheRecords(await WithDeadline(consumer.Poll(TimeSpan.Zero)), 0, Records);
            AssertTable(consumer, 0);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    // The C mock's poll returns at once, so the stall holds its empty result until the token has fired.
    [Fact]
    public async Task ACancelledPollThatDeliveredNoRecord_Throws_AndARecordAddedAfterIsPolled()
    {
        AsyncMockConsumer<object, object> consumer = await WithRecords(count: 0);
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task<ConsumerRecords<object, object>> poll;
            using (ConsumerPumpStall stall = new ConsumerPumpStall(consumer))
            {
                poll = consumer.Poll(TimeSpan.FromSeconds(30), cancel.Token);
                stall.WaitUntilSignaled();
                cancel.Cancel();
            }

            await Assert.ThrowsAsync<OperationCanceledException>(() => WithDeadline(poll));

            AddRecord(consumer, 0);
            AssertTheRecords(await WithDeadline(consumer.Poll(TimeSpan.Zero)), 0, 1);
            AssertTable(consumer, 0);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task ACancelAfterTheMockDeliveredTheBatch_ReturnsIt_AndTheNextPollWorks()
    {
        AsyncMockConsumer<object, object> consumer = await WithRecords();
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            Task<ConsumerRecords<object, object>> poll;
            using (ConsumerPumpStall stall = new ConsumerPumpStall(consumer))
            {
                poll = consumer.Poll(TimeSpan.FromSeconds(30), cancel.Token);
                stall.WaitUntilSignaled();
                cancel.Cancel();
            }

            AssertTheRecords(await WithDeadline(poll), 0, Records);
            AssertTable(consumer, 0);
            Assert.Equal((long)Records, await WithDeadline(consumer.Position(s_t0)));

            Assert.Empty(await WithDeadline(consumer.Poll(TimeSpan.Zero)));
            AddRecord(consumer, Records);
            AssertTheRecords(await WithDeadline(consumer.Poll(TimeSpan.Zero)), Records, 1);
            AssertTable(consumer, 0);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private async Task<AsyncMockConsumer<object, object>> WithRecords(int count = Records)
    {
        AsyncMockConsumer<object, object> consumer = new AsyncMockConsumer<object, object>("earliest");
        await WithDeadline(consumer.Assign(new[] { s_t0 }));
        await WithDeadline(consumer.Seek(s_t0, 0));
        for (int offset = 0; offset < count; offset++)
        {
            AddRecord(consumer, offset);
        }

        return consumer;
    }

    private void AddRecord(AsyncMockConsumer<object, object> consumer, int offset) =>
        consumer.AddRecord(new ConsumerRecord<object, object>(Topic, 0, offset, _keys[offset], _values[offset]));

    private void AssertTheRecords(ConsumerRecords<object, object> records, int first, int count)
    {
        ConsumerRecord<object, object>[] polled = records.Records(s_t0).ToArray();
        Assert.Equal(count, records.Count());
        Assert.Equal(count, polled.Length);
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(first + i, polled[i].Offset());
            Assert.Same(_keys[first + i], polled[i].Key());
            Assert.Same(_values[first + i], polled[i].Value());
        }
    }

    private static void AssertTable(AsyncMockConsumer<object, object> consumer, int entries)
    {
        MockRecordSlots<object, object> slots = (MockRecordSlots<object, object>)typeof(AsyncMockConsumer<object, object>)
            .GetField("_slots", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(consumer)!;
        Assert.Equal(entries, slots.KeyCount);
        Assert.Equal(entries, slots.ValueCount);
    }

    private static async Task<T> WithDeadline<T>(Task<T> task)
    {
        T result = default!;
        await TestTimeout.Run(async () => result = await task, s_deadline);
        return result;
    }

    private static Task WithDeadline(Task task) => TestTimeout.Run(() => task, s_deadline);
}
