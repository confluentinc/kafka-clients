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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Java.Lang;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A negative partition and a null topic reach the consumer's partition calls as Java's do: the binding makes no
/// partition check, so the core answers a negative partition, and a null topic, which cannot cross the C ABI, is
/// refused with the class and message Java gives a partition that is not assigned.
/// </summary>
public sealed class PublicConsumerPartitionArgumentTests
{
    private const string Topic = "partition-args";

    private const string NegativeNotAssigned = "No current assignment for partition partition-args--1";

    private const string NullNotAssigned = "No current assignment for partition null-2";

    private const string PositionNotAssigned = "You can only check the position for partitions assigned to this consumer.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_negative = new TopicPartition(Topic, -1);

    private static readonly TopicPartition s_nullTopic = new TopicPartition(null!, 2);

    public static IEnumerable<object[]> SyncKinds() => new[] { new object[] { "MockConsumer" }, new object[] { "KafkaConsumer" } };

    public static IEnumerable<object[]> AsyncKinds() =>
        new[] { new object[] { "AsyncMockConsumer" }, new object[] { "AsyncKafkaConsumer" } };

    [Theory]
    [MemberData(nameof(SyncKinds))]
    public void ANegativePartition_IsAnsweredByTheCore_WithJavasClassAndMessage(string kind)
    {
        IConsumer<byte[], byte[]> consumer = CreateSync(kind);
        using IDisposable disposal = TestTimeout.Disposal(consumer);

        AssertNotAssigned(NegativeNotAssigned, () => consumer.Seek(s_negative, 0));
        AssertNotAssigned(NegativeNotAssigned, () => consumer.Seek(s_negative, new OffsetAndMetadata(0)));
        AssertNotAssigned(NegativeNotAssigned, () => consumer.Pause(new[] { s_negative }));
        AssertNotAssigned(NegativeNotAssigned, () => consumer.Resume(new[] { s_negative }));
        AssertPositionNotAssigned(kind, Record.Exception(() => consumer.Position(s_negative)));
        if (IsMock(kind))
        {
            // Java's MockConsumer answers 0 for a partition it has no end offset for.
            Assert.Equal(0, consumer.CurrentLag(s_negative));
        }
        else
        {
            AssertNotAssigned(NegativeNotAssigned, () => consumer.CurrentLag(s_negative));
        }
    }

    [Theory]
    [MemberData(nameof(AsyncKinds))]
    public async Task OnTheAsyncConsumers_ANegativePartition_FaultsTheTask_WithJavasClassAndMessage(string kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = CreateAsync(kind);
        using IDisposable disposal = TestTimeout.Disposal(consumer);

        await AssertTaskNotAssigned(NegativeNotAssigned, consumer.Seek(s_negative, 0));
        await AssertTaskNotAssigned(NegativeNotAssigned, consumer.Seek(s_negative, new OffsetAndMetadata(0)));
        await AssertTaskNotAssigned(NegativeNotAssigned, consumer.Pause(new[] { s_negative }));
        await AssertTaskNotAssigned(NegativeNotAssigned, consumer.Resume(new[] { s_negative }));
        Task<long> position = consumer.Position(s_negative);
        AssertPositionNotAssigned(kind, await Record.ExceptionAsync(() => TestTimeout.Run(() => position, s_deadline)));
        Task<long?> lag = consumer.CurrentLag(s_negative);
        if (IsMock(kind))
        {
            Assert.Equal(0, await TestTimeout.Run(() => lag, s_deadline));
        }
        else
        {
            await AssertTaskNotAssigned(NegativeNotAssigned, lag);
        }
    }

    [Fact]
    public async Task OnTheMocks_ANegativePartition_IsAssignedAndCommitted_AsJavasMockAccepts()
    {
        Dictionary<TopicPartition, OffsetAndMetadata> offsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [s_negative] = new OffsetAndMetadata(5) };

        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using (TestTimeout.Disposal(consumer))
        {
            consumer.Assign(new[] { s_negative });
            Assert.Contains(s_negative, consumer.Assignment());
            consumer.Commit(offsets);
            Assert.Equal(5, consumer.Committed(new[] { s_negative })[s_negative]!.Offset);
        }

        AsyncMockConsumer<byte[], byte[]> asyncConsumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using (TestTimeout.Disposal(asyncConsumer))
        {
            await TestTimeout.Run(() => asyncConsumer.Assign(new[] { s_negative }), s_deadline);
            Assert.Contains(s_negative, asyncConsumer.Assignment());
            await TestTimeout.Run(() => asyncConsumer.Commit(offsets), s_deadline);
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                await TestTimeout.Run(() => asyncConsumer.Committed(new[] { s_negative }), s_deadline);
            Assert.Equal(5, committed[s_negative]!.Offset);
        }
    }

    // Java's MockConsumer: beginningOffsets / endOffsets throw IllegalStateException for a partition it has no offset
    // for, and offsetsForTimes throws UnsupportedOperationException.
    [Fact]
    public async Task OnTheMocks_TheOffsetLookups_AnswerANegativePartition_AsJavasMock()
    {
        const string NoBeginning = "The partition partition-args--1 does not have a beginning offset.";
        const string NoEnd = "The partition partition-args--1 does not have an end offset.";
        const string NotImplemented = "Not implemented yet.";
        TopicPartition[] partitions = { s_negative };
        Dictionary<TopicPartition, long> timestamps = new Dictionary<TopicPartition, long> { [s_negative] = 0 };

        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using (TestTimeout.Disposal(consumer))
        {
            AssertNotAssigned(NoBeginning, () => consumer.BeginningOffsets(partitions));
            AssertNotAssigned(NoEnd, () => consumer.EndOffsets(partitions));
            Assert.Equal(
                NotImplemented,
                Assert.Throws<UnsupportedOperationException>(() => consumer.OffsetsForTimes(timestamps)).Message);
        }

        AsyncMockConsumer<byte[], byte[]> asyncConsumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using (TestTimeout.Disposal(asyncConsumer))
        {
            await AssertTaskNotAssigned(NoBeginning, asyncConsumer.BeginningOffsets(partitions));
            await AssertTaskNotAssigned(NoEnd, asyncConsumer.EndOffsets(partitions));
            UnsupportedOperationException unsupported = await Assert.ThrowsAsync<UnsupportedOperationException>(
                () => TestTimeout.Run(() => asyncConsumer.OffsetsForTimes(timestamps), s_deadline));
            Assert.Equal(NotImplemented, unsupported.Message);
        }
    }

    [Fact]
    public void OnKafkaConsumer_ANegativePartition_IsAssigned_AsJavaAccepts()
    {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable disposal = TestTimeout.Disposal(consumer);

        consumer.Assign(new[] { s_negative });

        Assert.Contains(s_negative, consumer.Assignment());
    }

    [Theory]
    [MemberData(nameof(SyncKinds))]
    public void ANullTopic_IsRefusedAsAPartitionThatIsNotAssigned(string kind)
    {
        IConsumer<byte[], byte[]> consumer = CreateSync(kind);
        using IDisposable disposal = TestTimeout.Disposal(consumer);

        AssertNotAssigned(NullNotAssigned, () => consumer.Seek(s_nullTopic, 0));
        AssertNotAssigned(NullNotAssigned, () => consumer.Seek(s_nullTopic, new OffsetAndMetadata(0)));
        AssertNotAssigned(NullNotAssigned, () => consumer.Pause(new[] { s_nullTopic }));
        AssertNotAssigned(NullNotAssigned, () => consumer.Resume(new[] { s_nullTopic }));
        AssertPositionNotAssigned(kind, Record.Exception(() => consumer.Position(s_nullTopic)));
        AssertPositionNotAssigned(kind, Record.Exception(() => consumer.Position(s_nullTopic, TimeSpan.FromSeconds(-1))));
        if (IsMock(kind))
        {
            // Java's MockConsumer answers 0 for a partition it has no end offset for, which a null topic never has.
            Assert.Equal(0, consumer.CurrentLag(s_nullTopic));
        }
        else
        {
            AssertNotAssigned(NullNotAssigned, () => consumer.CurrentLag(s_nullTopic));
        }
    }

    [Theory]
    [MemberData(nameof(AsyncKinds))]
    public async Task OnTheAsyncConsumers_ANullTopic_IsRefusedAtTheCall(string kind)
    {
        IAsyncConsumer<byte[], byte[]> consumer = CreateAsync(kind);
        using IDisposable disposal = TestTimeout.Disposal(consumer);

        AssertNotAssigned(NullNotAssigned, () => { _ = consumer.Seek(s_nullTopic, 0); });
        AssertNotAssigned(NullNotAssigned, () => { _ = consumer.Seek(s_nullTopic, new OffsetAndMetadata(0)); });
        AssertNotAssigned(NullNotAssigned, () => { _ = consumer.Pause(new[] { s_nullTopic }); });
        AssertNotAssigned(NullNotAssigned, () => { _ = consumer.Resume(new[] { s_nullTopic }); });
        AssertPositionNotAssigned(kind, Record.Exception(() => { _ = consumer.Position(s_nullTopic); }));
        if (IsMock(kind))
        {
            Task<long?> lag = consumer.CurrentLag(s_nullTopic);
            Assert.True(lag.IsCompleted);
            Assert.Equal(0, await lag);
        }
        else
        {
            AssertNotAssigned(NullNotAssigned, () => { _ = consumer.CurrentLag(s_nullTopic); });
        }
    }

    [Fact]
    public void OnTheHandle_ANullTopic_IsRefusedAsKafkaConsumerRefusesIt()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        AssertNotAssigned(NullNotAssigned, () => handle.Seek(s_nullTopic, 0));
        AssertNotAssigned(NullNotAssigned, () => handle.Seek(s_nullTopic, new OffsetAndMetadata(0)));
        AssertNotAssigned(NullNotAssigned, () => handle.Pause(new[] { s_nullTopic }));
        AssertNotAssigned(NullNotAssigned, () => handle.Resume(new[] { s_nullTopic }));
        AssertNotAssigned(NullNotAssigned, () => handle.CurrentLag(s_nullTopic));
        AssertNotAssigned(PositionNotAssigned, () => handle.Position(s_nullTopic));

        // Java's seek checks the offset first.
        IllegalArgumentException negative = Assert.Throws<IllegalArgumentException>(() => handle.Seek(s_nullTopic, -1));
        Assert.Equal("seek offset must not be a negative number", negative.Message);
    }

    private static void AssertNotAssigned(string message, Action call)
    {
        IllegalStateException error = Assert.Throws<IllegalStateException>(call);
        Assert.Equal(message, error.Message);
    }

    private static async Task AssertTaskNotAssigned(string message, Task task)
    {
        IllegalStateException error = await Assert.ThrowsAsync<IllegalStateException>(
            () => TestTimeout.Run(() => task, s_deadline));
        Assert.Equal(message, error.Message);
    }

    // Java's KafkaConsumer throws IllegalStateException, Java's MockConsumer IllegalArgumentException.
    private static void AssertPositionNotAssigned(string kind, Exception? thrown)
    {
        Assert.NotNull(thrown);
        Assert.Equal(IsMock(kind) ? typeof(IllegalArgumentException) : typeof(IllegalStateException), thrown.GetType());
        Assert.Equal(PositionNotAssigned, thrown.Message);
    }

    private static bool IsMock(string kind) => kind.Contains("Mock", StringComparison.Ordinal);

    private static IConsumer<byte[], byte[]> CreateSync(string kind) => IsMock(kind)
        ? new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray)
        : new KafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

    private static IAsyncConsumer<byte[], byte[]> CreateAsync(string kind) => IsMock(kind)
        ? new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray)
        : new AsyncKafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

    private static Dictionary<string, object?> Config() => new Dictionary<string, object?>
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = "partition-args-group",
        ["enable.auto.commit"] = "false",
        ["default.api.timeout.ms"] = "2000",
    };
}
