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
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A <see langword="null"/> <see cref="TopicPartition"/>, alone, as an element or as a key, on the four consumers.
/// Where Java checks it, the class and message are Java's; where Java answers it as a partition it does not know, the
/// answer is Java's, <c>null</c> printed as Java prints it; where Java dereferences it, or accepts it but the C ABI
/// cannot carry it, it is an <see cref="ArgumentNullException"/>. Each is answered before any native call: a
/// refusal of the binding's own is thrown at the call, and a mock's answer for any partition faults the
/// <see cref="Task"/> of an asynchronous mock, as that mock's answer for any other partition does.
/// </summary>
public sealed class PublicConsumerNullPartitionTests
{
    private const string Topic = "np";

    private const string NotAssigned = "No current assignment for partition null";

    private const string PositionNotAssigned =
        "You can only check the position for partitions assigned to this consumer.";

    private const string ClosedMessage = "This consumer has already been closed.";

    private const string GroupIdNotDefined =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_second = TimeSpan.FromSeconds(1);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition s_tp1 = new TopicPartition(Topic, 1);

    private static readonly TopicPartition s_tp2 = new TopicPartition(Topic, 2);

    private static readonly TopicPartition[] s_nullAfterTp0 = { s_tp0, null! };

    public static IEnumerable<object[]> AllKinds() => new[]
    {
        new object[] { "KafkaConsumer" },
        new object[] { "AsyncKafkaConsumer" },
        new object[] { "MockConsumer" },
        new object[] { "AsyncMockConsumer" },
    };

    // Java's AsyncKafkaConsumer#assign checks every element for a null or blank topic, a null one included.
    [Theory]
    [InlineData("KafkaConsumer")]
    [InlineData("AsyncKafkaConsumer")]
    public async Task Assign_ANullElement_OnTheRealConsumers_IsJavasIllegalArgumentException(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        (Exception? error, bool atTheCall) = await Outcome(
            consumer, c => c.Assign(s_nullAfterTp0), c => c.Assign(s_nullAfterTp0));

        Assert.IsType<IllegalArgumentException>(error);
        Assert.Equal("Topic partitions to assign to cannot have null or empty topic", error!.Message);
        Assert.True(atTheCall);
        Assert.Empty(Common(consumer).Assignment());
    }

    // Java's MockConsumer#assign dereferences the null in SubscriptionState#assignFromUser.
    [Theory]
    [InlineData("MockConsumer")]
    [InlineData("AsyncMockConsumer")]
    public async Task Assign_ANullElement_OnTheMocks_IsArgumentNullException(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        await Run(consumer, c => c.Assign(new[] { s_tp1 }), c => c.Assign(new[] { s_tp1 }));

        (Exception? error, bool atTheCall) = await Outcome(
            consumer, c => c.Assign(s_nullAfterTp0), c => c.Assign(s_nullAfterTp0));

        AssertMustNotContainNull("partitions", error);
        Assert.True(atTheCall);
        Assert.Equal(new[] { s_tp1 }, Common(consumer).Assignment());
    }

    // Java's SubscriptionState#assignedState throws for a partition that is not assigned, which a null one never is.
    // Java pauses the partitions before the null; the binding answers the null before the call, so none is paused (a
    // recorded difference).
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task PauseAndResume_ANullElement_IsJavasNotAssignedAnswer_AndNothingIsPaused(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        await Run(consumer, c => c.Assign(new[] { s_tp0, s_tp1 }), c => c.Assign(new[] { s_tp0, s_tp1 }));

        await AssertAtTheCall<IllegalStateException>(
            NotAssigned, consumer, c => c.Pause(s_nullAfterTp0), c => c.Pause(s_nullAfterTp0));
        Assert.Empty(Common(consumer).Paused());

        await Run(consumer, c => c.Pause(new[] { s_tp1 }), c => c.Pause(new[] { s_tp1 }));
        TopicPartition[] resumed = { s_tp1, null! };
        await AssertAtTheCall<IllegalStateException>(
            NotAssigned, consumer, c => c.Resume(resumed), c => c.Resume(resumed));
        Assert.Equal(new[] { s_tp1 }, Common(consumer).Paused());
    }

    // Java's SubscriptionState#requestOffsetReset looks every partition up in the assignment.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task SeekToBeginningAndEnd_ANullElement_IsJavasNotAssignedAnswer(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        await Run(consumer, c => c.Assign(new[] { s_tp0 }), c => c.Assign(new[] { s_tp0 }));

        await AssertAtTheCall<IllegalStateException>(
            NotAssigned, consumer, c => c.SeekToBeginning(s_nullAfterTp0), c => c.SeekToBeginning(s_nullAfterTp0));
        await AssertAtTheCall<IllegalStateException>(
            NotAssigned, consumer, c => c.SeekToEnd(s_nullAfterTp0), c => c.SeekToEnd(s_nullAfterTp0));
    }

    // Java's position checks isAssigned first: KafkaConsumer throws IllegalStateException, MockConsumer
    // IllegalArgumentException, before the timeout is read.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task Position_ANullPartition_IsJavasNotAssignedAnswer_BeforeTheTimeout(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        Type expected = IsMock(kind) ? typeof(IllegalArgumentException) : typeof(IllegalStateException);

        foreach (TimeSpan? timeout in new TimeSpan?[] { null, s_second, TimeSpan.FromSeconds(-1) })
        {
            (Exception? error, bool atTheCall) = await Outcome(
                consumer,
                c => _ = timeout is TimeSpan t ? c.Position(null!, t) : c.Position(null!),
                c => timeout is TimeSpan t ? c.Position(null!, t) : c.Position(null!));

            Assert.True(error?.GetType() == expected, timeout + ": " + error);
            Assert.Equal(PositionNotAssigned, error!.Message);
            Assert.True(atTheCall);
        }
    }

    // Java's KafkaConsumer checks a negative offset before acquire(); the seek then looks the partition up in the
    // assignment. Java's MockConsumer checks no offset.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task Seek_ANullPartition_IsJavasNotAssignedAnswer_AfterTheNegativeOffsetCheck(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        await AssertAtTheCall<IllegalStateException>(NotAssigned, consumer, c => c.Seek(null!, 0L), c => c.Seek(null!, 0L));

        (Exception? negative, bool atTheCall) = await Outcome(consumer, c => c.Seek(null!, -1L), c => c.Seek(null!, -1L));
        Assert.True(atTheCall);
        if (IsMock(kind))
        {
            Assert.IsType<IllegalStateException>(negative);
            Assert.Equal(NotAssigned, negative!.Message);
        }
        else
        {
            Assert.IsType<IllegalArgumentException>(negative);
            Assert.Equal("seek offset must not be a negative number", negative!.Message);
        }
    }

    // Java reads the OffsetAndMetadata first. With a leader epoch, Java's KafkaConsumer records it with
    // Metadata#updateLastSeenEpochIfNewer, whose NullPointerException it wraps in a KafkaException; otherwise, and on
    // Java's MockConsumer, the null partition is not assigned.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task SeekWithMetadata_ANullPartition_IsJavasAnswer_WithAndWithoutALeaderEpoch(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        OffsetAndMetadata withEpoch = new OffsetAndMetadata(5, 3, "m");
        OffsetAndMetadata withoutEpoch = new OffsetAndMetadata(5);

        (Exception? nullOffset, bool _) = await Outcome(
            consumer, c => c.Seek(null!, (OffsetAndMetadata)null!), c => c.Seek(null!, (OffsetAndMetadata)null!));
        Assert.Equal("offsetAndMetadata", Assert.IsType<ArgumentNullException>(nullOffset).ParamName);

        await AssertAtTheCall<IllegalStateException>(
            NotAssigned, consumer, c => c.Seek(null!, withoutEpoch), c => c.Seek(null!, withoutEpoch));

        (Exception? error, bool atTheCall) = await Outcome(
            consumer, c => c.Seek(null!, withEpoch), c => c.Seek(null!, withEpoch));
        Assert.True(atTheCall);
        if (IsMock(kind))
        {
            Assert.IsType<IllegalStateException>(error);
            Assert.Equal(NotAssigned, error!.Message);
        }
        else
        {
            KafkaException wrapper = Assert.IsType<KafkaException>(error);
            ArgumentNullException cause = Assert.IsType<ArgumentNullException>(wrapper.InnerException);
            Assert.Equal("partition", cause.ParamName);
            Assert.Equal(new ArgumentNullException("partition").Message, cause.Message);
            Assert.Equal("System.ArgumentNullException: " + cause.Message, wrapper.Message);
        }
    }

    // Java's KafkaConsumer looks the partition up in SubscriptionState#partitionLag; Java's MockConsumer answers 0
    // for a partition it has no end offset for.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task CurrentLag_ANullPartition_IsJavasAnswer(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        if (!IsMock(kind))
        {
            await AssertAtTheCall<IllegalStateException>(
                NotAssigned, consumer, c => _ = c.CurrentLag(null!), c => c.CurrentLag(null!));
        }
        else if (consumer is IConsumer<byte[], byte[]> sync)
        {
            Assert.Equal(0, sync.CurrentLag(null!));
        }
        else
        {
            Task<long?> lag = ((IAsyncConsumer<byte[], byte[]>)consumer).CurrentLag(null!);
            Assert.True(lag.IsCompleted);
            Assert.Equal(0, await lag);
        }
    }

    // Java's KafkaConsumer dereferences a null key on its background thread; Java's MockConsumer stores it, which a
    // native map cannot. Nothing is committed.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task Commit_ANullKey_IsArgumentNullException_InEveryForm_AndNothingIsCommitted(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        EntryMap<TopicPartition, OffsetAndMetadata> offsets = new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(s_tp0, new OffsetAndMetadata(5)),
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));
        IOffsetCommitCallback callback = new NoCallback();
        if (IsMock(kind))
        {
            // Java's MockConsumer#assign clears the committed offsets, so it comes first.
            await Run(consumer, c => c.Assign(new[] { s_tp0 }), c => c.Assign(new[] { s_tp0 }));
        }

        List<(Action<IConsumer<byte[], byte[]>> Sync, Func<IAsyncConsumer<byte[], byte[]>, Task> Async)> forms =
            new List<(Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
            {
                (c => c.Commit(offsets), c => c.Commit(offsets)),
                (c => c.Commit(offsets, s_second), c => c.Commit(offsets, s_second)),
                (c => c.CommitAsync(offsets, callback), c => c.CommitAsync(offsets, callback)),
                (c => c.CommitAsync(offsets, null), c => c.CommitAsync(offsets, (IOffsetCommitCallback?)null)),
            };
        if (consumer is IAsyncConsumer<byte[], byte[]>)
        {
            forms.Add((_ => { }, c => c.CommitAsync(offsets, new NoAsyncCallback())));
        }

        foreach ((Action<IConsumer<byte[], byte[]>> sync, Func<IAsyncConsumer<byte[], byte[]>, Task> async) in forms)
        {
            (Exception? error, bool atTheCall) = await Outcome(consumer, sync, async);
            AssertMustNotContainNull("offsets", error);
            Assert.True(atTheCall);
        }

        if (IsMock(kind))
        {
            Assert.Empty(await Committed(consumer, new[] { s_tp0 }));
        }
    }

    // Java's KafkaConsumer checks the group in commit(), after acquire() and before its background thread reads a key.
    [Theory]
    [InlineData("KafkaConsumer")]
    [InlineData("AsyncKafkaConsumer")]
    public async Task Commit_ANullKey_WithoutAGroupId_IsTheGroupError_BeforeTheNullCheck(string kind)
    {
        object consumer = await Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        EntryMap<TopicPartition, OffsetAndMetadata> offsets = NullKeyOffsets();
        IOffsetCommitCallback callback = new NoCallback();

        List<(Action<IConsumer<byte[], byte[]>> Sync, Func<IAsyncConsumer<byte[], byte[]>, Task> Async)> forms =
            new List<(Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
            {
                (c => c.Commit(offsets), c => c.Commit(offsets)),
                (c => c.Commit(offsets, s_second), c => c.Commit(offsets, s_second)),
                (c => c.Commit(offsets, TimeSpan.FromSeconds(-1)), c => c.Commit(offsets, TimeSpan.FromSeconds(-1))),
                (c => c.CommitAsync(offsets, callback), c => c.CommitAsync(offsets, callback)),
                (c => c.CommitAsync(offsets, null), c => c.CommitAsync(offsets, (IOffsetCommitCallback?)null)),
            };
        if (consumer is IAsyncConsumer<byte[], byte[]>)
        {
            forms.Add((_ => { }, c => c.CommitAsync(offsets, new NoAsyncCallback())));
        }

        foreach ((Action<IConsumer<byte[], byte[]>> sync, Func<IAsyncConsumer<byte[], byte[]>, Task> async) in forms)
        {
            (Exception? error, bool atTheCall) = await Outcome(consumer, sync, async);
            Assert.IsType<InvalidGroupIdException>(error);
            Assert.Equal(GroupIdNotDefined, error!.Message);
            Assert.True(atTheCall);
        }
    }

    // Java's commitSync(Map, Duration) builds its Timer, which checks the timeout, before it waits for the commit that
    // its background thread fails on the null key.
    [Theory]
    [InlineData("KafkaConsumer")]
    [InlineData("AsyncKafkaConsumer")]
    public async Task Commit_ANullKey_WithANegativeTimeout_IsJavasTimeoutError_BeforeTheNullCheck(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        EntryMap<TopicPartition, OffsetAndMetadata> offsets = NullKeyOffsets();
        TimeSpan negative = TimeSpan.FromSeconds(-1);

        await AssertAtTheCall<IllegalArgumentException>(
            "Invalid negative timeout -1000", consumer, c => c.Commit(offsets, negative), c => c.Commit(offsets, negative));
    }

    // Java's KafkaConsumer dereferences a null element on its background thread, after its group check; Java's
    // MockConsumer keeps only the partitions it has a committed offset for, which a null one never has.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task Committed_ANullElement_IsArgumentNullException_OrLeftOutOnTheMocks(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        TopicPartition[] partitions = { null!, s_tp0 };

        if (IsMock(kind))
        {
            Dictionary<TopicPartition, OffsetAndMetadata> offsets =
                new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(5) };
            await Run(consumer, c => c.Assign(new[] { s_tp0 }), c => c.Assign(new[] { s_tp0 }));
            await Run(consumer, c => c.Commit(offsets), c => c.Commit(offsets));

            foreach (TimeSpan? timeout in new TimeSpan?[] { null, s_second })
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                    await Committed(consumer, partitions, timeout);
                Assert.Equal(new[] { s_tp0 }, committed.Keys);
                Assert.Equal(5, committed[s_tp0]!.Offset());
            }

            return;
        }

        foreach (TimeSpan? timeout in new TimeSpan?[] { null, s_second })
        {
            (Exception? error, bool atTheCall) = await Outcome(
                consumer,
                c => _ = timeout is TimeSpan t ? c.Committed(partitions, t) : c.Committed(partitions),
                c => timeout is TimeSpan t ? c.Committed(partitions, t) : c.Committed(partitions));
            AssertMustNotContainNull("partitions", error);
            Assert.True(atTheCall);
        }
    }

    // Java's committed checks the group before it reads the partitions.
    [Theory]
    [InlineData("KafkaConsumer")]
    [InlineData("AsyncKafkaConsumer")]
    public async Task Committed_ANullElement_WithoutAGroupId_IsTheGroupError_BeforeTheNullCheck(string kind)
    {
        object consumer = await Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        TopicPartition[] partitions = { null!, s_tp0 };

        foreach (TimeSpan? timeout in new TimeSpan?[] { null, s_second })
        {
            (Exception? error, bool atTheCall) = await Outcome(
                consumer,
                c => _ = timeout is TimeSpan t ? c.Committed(partitions, t) : c.Committed(partitions),
                c => timeout is TimeSpan t ? c.Committed(partitions, t) : c.Committed(partitions));
            Assert.IsType<InvalidGroupIdException>(error);
            Assert.Equal(GroupIdNotDefined, error!.Message);
            Assert.True(atTheCall);
        }
    }

    // Java's KafkaConsumer dereferences a null key on its background thread; Java's MockConsumer answers every map
    // with UnsupportedOperationException.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task OffsetsForTimes_ANullKey_IsArgumentNullException_OrTheMocksUnsupportedAnswer(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        EntryMap<TopicPartition, long> timestamps = new EntryMap<TopicPartition, long>(
            new KeyValuePair<TopicPartition, long>(s_tp0, 1),
            new KeyValuePair<TopicPartition, long>(null!, 2));

        foreach (TimeSpan? timeout in new TimeSpan?[] { null, s_second })
        {
            (Exception? error, bool atTheCall) = await Outcome(
                consumer,
                c => _ = timeout is TimeSpan t ? c.OffsetsForTimes(timestamps, t) : c.OffsetsForTimes(timestamps),
                c => timeout is TimeSpan t ? c.OffsetsForTimes(timestamps, t) : c.OffsetsForTimes(timestamps));
            if (IsMock(kind))
            {
                Assert.IsType<UnsupportedOperationException>(error);
                Assert.Equal("Not implemented yet.", error!.Message);
                Assert.Equal(consumer is IConsumer<byte[], byte[]>, atTheCall);
            }
            else
            {
                AssertMustNotContainNull("timestampsToSearch", error);
                Assert.True(atTheCall);
            }
        }
    }

    // Java's offsetsForTimes checks every target time at the call, after acquire(), before its background thread reads
    // a key: the first negative one in the map's order is reported, a null key printed as Java prints it.
    [Theory]
    [InlineData("KafkaConsumer")]
    [InlineData("AsyncKafkaConsumer")]
    public async Task OffsetsForTimes_ANullKey_ANegativeTargetTime_IsJavasIllegalArgumentException_First(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        (EntryMap<TopicPartition, long> Map, string Partition, long Time)[] cases =
        {
            (new EntryMap<TopicPartition, long>(new KeyValuePair<TopicPartition, long>(null!, -1)), "null", -1),
            (
                new EntryMap<TopicPartition, long>(
                    new KeyValuePair<TopicPartition, long>(null!, 5),
                    new KeyValuePair<TopicPartition, long>(s_tp0, -2)),
                "np-0",
                -2),
            (
                new EntryMap<TopicPartition, long>(
                    new KeyValuePair<TopicPartition, long>(s_tp1, 1),
                    new KeyValuePair<TopicPartition, long>(null!, 5),
                    new KeyValuePair<TopicPartition, long>(s_tp2, -3),
                    new KeyValuePair<TopicPartition, long>(s_tp0, -4)),
                "np-2",
                -3),
        };

        foreach ((EntryMap<TopicPartition, long> map, string partition, long time) in cases)
        {
            foreach (TimeSpan? timeout in new TimeSpan?[] { null, s_second, TimeSpan.Zero })
            {
                await AssertAtTheCall<IllegalArgumentException>(
                    "The target time for partition " + partition + " is " + time + ". The target time cannot be negative.",
                    consumer,
                    c => _ = timeout is TimeSpan t ? c.OffsetsForTimes(map, t) : c.OffsetsForTimes(map),
                    c => timeout is TimeSpan t ? c.OffsetsForTimes(map, t) : c.OffsetsForTimes(map));
            }
        }
    }

    // With a zero timeout Java's KafkaConsumer returns its empty results, every key mapped to null, without waiting for
    // its background thread: a .NET dictionary cannot hold the null key, so it is refused (a recorded difference).
    [Theory]
    [InlineData("KafkaConsumer")]
    [InlineData("AsyncKafkaConsumer")]
    public async Task OffsetsForTimes_ANullKey_WithAZeroTimeout_IsArgumentNullException(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        EntryMap<TopicPartition, long> timestamps = new EntryMap<TopicPartition, long>(
            new KeyValuePair<TopicPartition, long>(s_tp0, 1),
            new KeyValuePair<TopicPartition, long>(null!, 2));

        (Exception? error, bool atTheCall) = await Outcome(
            consumer,
            c => _ = c.OffsetsForTimes(timestamps, TimeSpan.Zero),
            c => c.OffsetsForTimes(timestamps, TimeSpan.Zero));

        AssertMustNotContainNull("timestampsToSearch", error);
        Assert.True(atTheCall);
    }

    // With a zero timeout Java's KafkaConsumer returns an empty map without waiting for its background thread, which
    // alone reads a null element. A timeout below a millisecond reaches the core as zero, as for any other partition.
    [Theory]
    [InlineData("KafkaConsumer")]
    [InlineData("AsyncKafkaConsumer")]
    public async Task BeginningAndEndOffsets_ANullElement_WithAZeroTimeout_IsJavasEmptyMap(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        foreach (bool beginning in new[] { true, false })
        {
            foreach (TopicPartition[] partitions in new[] { s_nullAfterTp0, new TopicPartition[] { null! } })
            {
                foreach (TimeSpan timeout in new[] { TimeSpan.Zero, TimeSpan.FromTicks(1) })
                {
                    IReadOnlyDictionary<TopicPartition, long> offsets = consumer is IConsumer<byte[], byte[]> sync
                        ? LookUp(sync, partitions, beginning, timeout)
                        : await TestTimeout.Run(
                            () => LookUp((IAsyncConsumer<byte[], byte[]>)consumer, partitions, beginning, timeout), s_deadline);
                    Assert.Empty(offsets);
                }
            }
        }
    }

    // Without a timeout Java's KafkaConsumer passes default.api.timeout.ms, so a zero setting, in any form the
    // configuration accepts, takes the zero path too, as it does for any partition.
    [Theory]
    [InlineData("KafkaConsumer", "0")]
    [InlineData("KafkaConsumer", " +0\t")]
    [InlineData("KafkaConsumer", "-0")]
    [InlineData("KafkaConsumer", 0)]
    [InlineData("AsyncKafkaConsumer", "0")]
    [InlineData("AsyncKafkaConsumer", " +0\t")]
    [InlineData("AsyncKafkaConsumer", "-0")]
    [InlineData("AsyncKafkaConsumer", 0)]
    public async Task BeginningAndEndOffsets_ANullElement_WithoutATimeout_AndAZeroDefaultApiTimeout_IsJavasEmptyMap(
        string kind, object defaultApiTimeoutMs)
    {
        object consumer = await Create(kind, defaultApiTimeoutMs: defaultApiTimeoutMs);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        foreach (bool beginning in new[] { true, false })
        {
            foreach (TopicPartition[] partitions in new[] { s_nullAfterTp0, new TopicPartition[] { null! }, new[] { s_tp0 } })
            {
                IReadOnlyDictionary<TopicPartition, long> offsets = consumer is IConsumer<byte[], byte[]> sync
                    ? LookUp(sync, partitions, beginning, timeout: null)
                    : await TestTimeout.Run(
                        () => LookUp((IAsyncConsumer<byte[], byte[]>)consumer, partitions, beginning, timeout: null), s_deadline);
                Assert.Empty(offsets);
            }
        }
    }

    // Java's KafkaConsumer dereferences a null element on its background thread; Java's MockConsumer has no offset
    // for it.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task BeginningAndEndOffsets_ANullElement_IsArgumentNullException_OrTheMocksNoOffsetAnswer(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        if (consumer is MockConsumer<byte[], byte[]> mock)
        {
            mock.UpdateBeginningOffset(Topic, 0, 0);
            mock.UpdateEndOffset(Topic, 0, 9);
        }
        else if (consumer is AsyncMockConsumer<byte[], byte[]> asyncMock)
        {
            asyncMock.UpdateBeginningOffset(Topic, 0, 0);
            asyncMock.UpdateEndOffset(Topic, 0, 9);
        }

        foreach (bool beginning in new[] { true, false })
        {
            foreach (TimeSpan? timeout in new TimeSpan?[] { null, s_second })
            {
                (Exception? error, bool atTheCall) = await Outcome(
                    consumer,
                    c => _ = LookUp(c, s_nullAfterTp0, beginning, timeout),
                    c => LookUp(c, s_nullAfterTp0, beginning, timeout));
                if (IsMock(kind))
                {
                    Assert.IsType<IllegalStateException>(error);
                    Assert.Equal(
                        beginning
                            ? "The partition null does not have a beginning offset."
                            : "The partition null does not have an end offset.",
                        error!.Message);
                    Assert.Equal(consumer is IConsumer<byte[], byte[]>, atTheCall);
                }
                else
                {
                    AssertMustNotContainNull("partitions", error);
                    Assert.True(atTheCall);
                }
            }
        }
    }

    // The mocks answer a null element before the native call, so a pending offsets exception stays pending. Java's
    // MockConsumer throws that exception first, and checks the partitions in order: the binding answers the null one
    // first (a recorded difference).
    [Theory]
    [InlineData("MockConsumer")]
    [InlineData("AsyncMockConsumer")]
    public async Task BeginningOffsets_ANullElement_OnTheMocks_MakesNoNativeCall(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        KafkaException pending = new KafkaException("pending");
        if (consumer is MockConsumer<byte[], byte[]> mock)
        {
            mock.SetOffsetsException(pending);
        }
        else
        {
            ((AsyncMockConsumer<byte[], byte[]>)consumer).SetOffsetsException(pending);
        }

        TopicPartition[] noOffset = { s_tp2, null! };
        (Exception? error, bool _) = await Outcome(
            consumer, c => _ = c.BeginningOffsets(noOffset), c => c.BeginningOffsets(noOffset));
        Assert.Equal("The partition null does not have a beginning offset.", Assert.IsType<IllegalStateException>(error).Message);

        (Exception? next, bool _) = await Outcome(
            consumer,
            c => _ = c.BeginningOffsets(Array.Empty<TopicPartition>()),
            c => c.BeginningOffsets(Array.Empty<TopicPartition>()));
        Assert.Same(pending, next);
    }

    // Java's MockConsumer#rebalance dereferences the null in PartitionStates#update, after it has cleared the records,
    // called onPartitionsRevoked and cleared the assignment. The binding refuses the null before any of that, so the
    // assignment is kept (a recorded difference).
    [Theory]
    [InlineData("MockConsumer")]
    [InlineData("AsyncMockConsumer")]
    public async Task Rebalance_ANullElement_IsArgumentNullException_AndTheAssignmentIsKept(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        await Run(consumer, c => c.Subscribe(new[] { Topic }), c => c.Subscribe(new[] { Topic }));
        await Rebalance(consumer, new[] { s_tp0 });

        TopicPartition[] newAssignment = { s_tp1, null! };
        Exception? error = consumer is MockConsumer<byte[], byte[]> mock
            ? Record.Exception(() => mock.Rebalance(newAssignment))
            : Record.Exception(() => { _ = ((AsyncMockConsumer<byte[], byte[]>)consumer).Rebalance(newAssignment); });

        AssertMustNotContainNull("newAssignment", error);
        Assert.Equal(new[] { s_tp0 }, Common(consumer).Assignment());
    }

    // Java's MockConsumer#losePartitions lists every lost partition that is not assigned, a null one included.
    [Theory]
    [InlineData("MockConsumer")]
    [InlineData("AsyncMockConsumer")]
    public async Task LosePartitions_ANullElement_IsJavasNotAssignedAnswer_ListingEveryOneOnce(string kind)
    {
        object consumer = await Create(kind);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);
        await Run(consumer, c => c.Subscribe(new[] { Topic }), c => c.Subscribe(new[] { Topic }));
        await Rebalance(consumer, new[] { s_tp0, s_tp1 });

        TopicPartition[] lost = { s_tp0, null!, s_tp2, null!, s_tp0, new TopicPartition(null!, 3) };
        bool atTheCall = true;
        Exception? error;
        if (consumer is MockConsumer<byte[], byte[]> mock)
        {
            error = Record.Exception(() => mock.LosePartitions(lost));
        }
        else
        {
            Task losing = ((AsyncMockConsumer<byte[], byte[]>)consumer).LosePartitions(lost);
            atTheCall = false;
            error = await Record.ExceptionAsync(() => TestTimeout.Run(() => losing, s_deadline));
        }

        Assert.IsType<IllegalStateException>(error);
        Assert.Equal("Cannot lose partitions that are not currently assigned: [null, np-2, null-3]", error!.Message);
        Assert.Equal(consumer is MockConsumer<byte[], byte[]>, atTheCall);
        Assert.Equal(new HashSet<TopicPartition> { s_tp0, s_tp1 }, new HashSet<TopicPartition>(Common(consumer).Assignment()));
    }

    // Java's MockConsumer#updateDurationOffsets stores a null key, which a native map cannot carry; refused before
    // the closed check, as a null topic is.
    [Theory]
    [InlineData("MockConsumer")]
    [InlineData("AsyncMockConsumer")]
    public async Task UpdateDurationOffsets_ANullKey_IsArgumentNullException_BeforeTheClosedCheck(string kind)
    {
        object consumer = await Create(kind);
        TestTimeout.Dispose((IDisposable)consumer);
        EntryMap<TopicPartition, long> newOffsets = new EntryMap<TopicPartition, long>(
            new KeyValuePair<TopicPartition, long>(s_tp0, 1),
            new KeyValuePair<TopicPartition, long>(null!, 2));

        Exception? error = consumer is MockConsumer<byte[], byte[]> mock
            ? Record.Exception(() => mock.UpdateDurationOffsets(newOffsets))
            : Record.Exception(() => ((AsyncMockConsumer<byte[], byte[]>)consumer).UpdateDurationOffsets(newOffsets));

        AssertMustNotContainNull("newOffsets", error);
    }

    // Java's default ConsumerRebalanceListener#onPartitionsLost passes its collection, a null element included, to
    // onPartitionsRevoked.
    [Fact]
    public async Task OnPartitionsLost_TheDefault_PassesANullElementToOnPartitionsRevoked()
    {
        TopicPartition[] partitions = { s_tp0, null! };
        RevokedListener listener = new RevokedListener();
        AsyncRevokedListener asyncListener = new AsyncRevokedListener();

        ((IConsumerRebalanceListener)listener).OnPartitionsLost(partitions);
        await ((IAsyncConsumerRebalanceListener)asyncListener).OnPartitionsLost(partitions);

        Assert.Same(partitions, listener.Revoked);
        Assert.Same(partitions, asyncListener.Revoked);
    }

    // The closed check comes first where Java's acquireAndEnsureOpen() comes before its answer; Java's KafkaConsumer
    // checks a seek's negative offset and OffsetAndMetadata before it.
    [Theory]
    [MemberData(nameof(AllKinds))]
    public async Task OnAClosedConsumer_ANullPartition_ReportsTheClosedConsumerFirst(string kind)
    {
        object consumer = await Create(kind);
        TestTimeout.Dispose((IDisposable)consumer);
        EntryMap<TopicPartition, OffsetAndMetadata> offsets = new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));

        List<(string Call, Action<IConsumer<byte[], byte[]>> Sync, Func<IAsyncConsumer<byte[], byte[]>, Task> Async)> calls =
            new List<(string, Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
            {
                ("Assign", c => c.Assign(s_nullAfterTp0), c => c.Assign(s_nullAfterTp0)),
                ("Pause", c => c.Pause(s_nullAfterTp0), c => c.Pause(s_nullAfterTp0)),
                ("Resume", c => c.Resume(s_nullAfterTp0), c => c.Resume(s_nullAfterTp0)),
                ("SeekToBeginning", c => c.SeekToBeginning(s_nullAfterTp0), c => c.SeekToBeginning(s_nullAfterTp0)),
                ("Position", c => _ = c.Position(null!), c => c.Position(null!)),
                ("Seek", c => c.Seek(null!, 0L), c => c.Seek(null!, 0L)),
                ("SeekWithMetadata", c => c.Seek(null!, new OffsetAndMetadata(1, 2, "")), c => c.Seek(null!, new OffsetAndMetadata(1, 2, ""))),
                ("CurrentLag", c => _ = c.CurrentLag(null!), c => c.CurrentLag(null!)),
                ("Commit", c => c.Commit(offsets), c => c.Commit(offsets)),
                ("Committed", c => _ = c.Committed(s_nullAfterTp0), c => c.Committed(s_nullAfterTp0)),
                ("BeginningOffsets", c => _ = c.BeginningOffsets(s_nullAfterTp0), c => c.BeginningOffsets(s_nullAfterTp0)),
            };
        foreach ((string call, Action<IConsumer<byte[], byte[]>> sync, Func<IAsyncConsumer<byte[], byte[]>, Task> async) in calls)
        {
            (Exception? error, bool atTheCall) = await Outcome(consumer, sync, async);
            Assert.True(error is IllegalStateException { Message: ClosedMessage }, call + ": " + error);
            Assert.True(atTheCall, call);
        }

        if (!IsMock(kind))
        {
            (Exception? negative, bool _) = await Outcome(consumer, c => c.Seek(null!, -1L), c => c.Seek(null!, -1L));
            Assert.IsType<IllegalArgumentException>(negative);
        }

        (Exception? nullOffset, bool _) = await Outcome(
            consumer, c => c.Seek(null!, (OffsetAndMetadata)null!), c => c.Seek(null!, (OffsetAndMetadata)null!));
        Assert.Equal(
            IsMock(kind) ? typeof(IllegalStateException) : typeof(ArgumentNullException), nullOffset?.GetType());
    }

    private static EntryMap<TopicPartition, OffsetAndMetadata> NullKeyOffsets() =>
        new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(s_tp0, new OffsetAndMetadata(5)),
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));

    private static void AssertMustNotContainNull(string paramName, Exception? error)
    {
        ArgumentNullException refused = Assert.IsType<ArgumentNullException>(error);
        Assert.Equal(paramName, refused.ParamName);
        Assert.Equal(new ArgumentNullException(paramName, paramName + " must not contain null").Message, refused.Message);
    }

    private static async Task AssertAtTheCall<TException>(
        string message,
        object consumer,
        Action<IConsumer<byte[], byte[]>> sync,
        Func<IAsyncConsumer<byte[], byte[]>, Task> async)
        where TException : Exception
    {
        (Exception? error, bool atTheCall) = await Outcome(consumer, sync, async);
        Assert.IsType<TException>(error);
        Assert.Equal(message, error!.Message);
        Assert.True(atTheCall);
    }

    // The exception a call gives, and whether it is thrown at the call or faults the returned Task.
    private static async Task<(Exception? Error, bool AtTheCall)> Outcome(
        object consumer,
        Action<IConsumer<byte[], byte[]>> sync,
        Func<IAsyncConsumer<byte[], byte[]>, Task> async)
    {
        if (consumer is IConsumer<byte[], byte[]> syncConsumer)
        {
            return (Record.Exception(() => sync(syncConsumer)), true);
        }

        Task? task = null;
        Exception? thrown = Record.Exception(() => { task = async((IAsyncConsumer<byte[], byte[]>)consumer); });
        if (thrown is not null)
        {
            return (thrown, true);
        }

        return (await Record.ExceptionAsync(() => TestTimeout.Run(() => task!, s_deadline)), false);
    }

    private static Task Run(
        object consumer,
        Action<IConsumer<byte[], byte[]>> sync,
        Func<IAsyncConsumer<byte[], byte[]>, Task> async)
    {
        if (consumer is IConsumer<byte[], byte[]> syncConsumer)
        {
            sync(syncConsumer);
            return Task.CompletedTask;
        }

        return TestTimeout.Run(() => async((IAsyncConsumer<byte[], byte[]>)consumer), s_deadline);
    }

    private static Task Rebalance(object consumer, TopicPartition[] newAssignment)
    {
        if (consumer is MockConsumer<byte[], byte[]> mock)
        {
            mock.Rebalance(newAssignment);
            return Task.CompletedTask;
        }

        return TestTimeout.Run(() => ((AsyncMockConsumer<byte[], byte[]>)consumer).Rebalance(newAssignment), s_deadline);
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Committed(
        object consumer, TopicPartition[] partitions, TimeSpan? timeout = null)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            return timeout is TimeSpan t ? sync.Committed(partitions, t) : sync.Committed(partitions);
        }

        IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)consumer;
        return await TestTimeout.Run(
            () => timeout is TimeSpan t ? async.Committed(partitions, t) : async.Committed(partitions), s_deadline);
    }

    private static IReadOnlyDictionary<TopicPartition, long> LookUp(
        IConsumer<byte[], byte[]> consumer, TopicPartition[] partitions, bool beginning, TimeSpan? timeout) =>
        (beginning, timeout) switch
        {
            (true, TimeSpan t) => consumer.BeginningOffsets(partitions, t),
            (true, null) => consumer.BeginningOffsets(partitions),
            (false, TimeSpan t) => consumer.EndOffsets(partitions, t),
            _ => consumer.EndOffsets(partitions),
        };

    private static Task<IReadOnlyDictionary<TopicPartition, long>> LookUp(
        IAsyncConsumer<byte[], byte[]> consumer, TopicPartition[] partitions, bool beginning, TimeSpan? timeout) =>
        (beginning, timeout) switch
        {
            (true, TimeSpan t) => consumer.BeginningOffsets(partitions, t),
            (true, null) => consumer.BeginningOffsets(partitions),
            (false, TimeSpan t) => consumer.EndOffsets(partitions, t),
            _ => consumer.EndOffsets(partitions),
        };

    private static IConsumerCommon Common(object consumer) => (IConsumerCommon)consumer;

    private static bool IsMock(string kind) => kind.Contains("Mock", StringComparison.Ordinal);

    private static async Task<object> Create(string kind, bool groupId = true, object? defaultApiTimeoutMs = null) => kind switch
    {
        "KafkaConsumer" => KafkaConsumer<byte[], byte[]>.Create(Config(groupId, defaultApiTimeoutMs), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        "AsyncKafkaConsumer" => await AsyncKafkaConsumer<byte[], byte[]>.Create(Config(groupId, defaultApiTimeoutMs), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer()),
        "MockConsumer" => new MockConsumer<byte[], byte[]>("latest"),
        _ => new AsyncMockConsumer<byte[], byte[]>("latest"),
    };

    private static Dictionary<string, object?> Config(bool groupId, object? defaultApiTimeoutMs)
    {
        Dictionary<string, object?> config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["enable.auto.commit"] = "false",
            ["default.api.timeout.ms"] = defaultApiTimeoutMs ?? "2000",
        };
        if (groupId)
        {
            config["group.id"] = "null-partition-group";
        }

        return config;
    }

    /// <summary>A commit callback that does nothing.</summary>
    private sealed class NoCallback : IOffsetCommitCallback
    {
        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
        {
        }
    }

    /// <summary>An asynchronous commit callback that does nothing.</summary>
    private sealed class NoAsyncCallback : IAsyncOffsetCommitCallback
    {
        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            Task.CompletedTask;
    }

    /// <summary>A listener that keeps the partitions it was last told are revoked.</summary>
    private sealed class RevokedListener : IConsumerRebalanceListener
    {
        internal IReadOnlyCollection<TopicPartition>? Revoked { get; private set; }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Revoked = partitions;

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>An asynchronous listener that keeps the partitions it was last told are revoked.</summary>
    private sealed class AsyncRevokedListener : IAsyncConsumerRebalanceListener
    {
        internal IReadOnlyCollection<TopicPartition>? Revoked { get; private set; }

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
            Revoked = partitions;
            return Task.CompletedTask;
        }

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }
}
