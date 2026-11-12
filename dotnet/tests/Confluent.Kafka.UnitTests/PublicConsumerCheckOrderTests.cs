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
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;

using Xunit;

using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The order of the consumer calls' checks, Python's: closed first, then the owner check and the arguments, an
/// argument Python checks before the use coming before the owner check (after the owner wait on the mocks), then
/// <c>group.id</c> and the timeout. The arguments Java's <c>KafkaConsumer</c> checks before <c>acquire()</c>
/// (<c>seek</c>'s negative offset and null <c>OffsetAndMetadata</c>, <c>seekToBeginning</c>'s and <c>seekToEnd</c>'s
/// null collection) come before the closed check too.
/// </summary>
public sealed class PublicConsumerCheckOrderTests
{
    private const string Topic = "check-order-topic";

    private const string Concurrent = "KafkaConsumer is not safe for multi-threaded access.";

    private const string ClosedMessage = "This consumer has already been closed.";

    private const string GroupIdNotDefined =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private const int InvalidGroupIdCode = 24;

    private const int RequestTimedOutCode = 7;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_negative = TimeSpan.FromSeconds(-1);

    private static readonly TimeSpan s_second = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromSeconds(20);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition[] s_partitions = { s_tp0 };

    private static readonly TopicPartition s_negativePartition = new TopicPartition(Topic, -1);

    private static readonly TopicPartition[] s_negativePartitions = { s_negativePartition };

    private static readonly IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> s_nullOffsets = null!;

    private static readonly IOffsetCommitCallback s_callback = new NoOpCommitCallback();

    private static readonly IAsyncOffsetCommitCallback s_asyncCallback = new NoOpAsyncCommitCallback();

    private static readonly IConsumerRebalanceListener s_listener = new AssignedListener(() => { });

    private static readonly IAsyncConsumerRebalanceListener s_asyncListener = new NoOpAsyncListener();

    private static readonly SubscriptionPattern s_nullPattern = new SubscriptionPattern(null!);

    private static readonly SubscriptionPattern s_nulPattern = new SubscriptionPattern("pay\0ments-.*");

    public static IEnumerable<object[]> RealConsumers() => new[]
    {
        new object[] { "KafkaConsumer" },
        new object[] { "AsyncKafkaConsumer" },
    };

    public static IEnumerable<object[]> AllConsumers() => new[]
    {
        new object[] { "KafkaConsumer" },
        new object[] { "AsyncKafkaConsumer" },
        new object[] { "MockConsumer" },
        new object[] { "AsyncMockConsumer" },
    };

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WhileAnotherThreadIsInside_ATimedOverload_ReportsTheConcurrentAccessBeforeItsTimeout(string kind)
    {
        IReadOnlyDictionary<string, Exception?> seen = await WhileAnotherThreadIsInside(Create(kind, groupId: true), consumer =>
            Calls(consumer, new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
            {
                ["Commit"] = (c => c.Commit(s_negative), c => c.Commit(s_negative)),
                ["Commit(offsets)"] = (c => c.Commit(Offsets(), s_negative), c => c.Commit(Offsets(), s_negative)),
                ["Position"] = (c => c.Position(s_tp0, s_negative), c => c.Position(s_tp0, s_negative)),
                ["Committed"] = (c => c.Committed(s_partitions, s_negative), c => c.Committed(s_partitions, s_negative)),
                ["OffsetsForTimes"] = (c => c.OffsetsForTimes(Timestamps(), s_negative), c => c.OffsetsForTimes(Timestamps(), s_negative)),
                ["BeginningOffsets"] = (c => c.BeginningOffsets(s_partitions, s_negative), c => c.BeginningOffsets(s_partitions, s_negative)),
                ["EndOffsets"] = (c => c.EndOffsets(s_partitions, s_negative), c => c.EndOffsets(s_partitions, s_negative)),
                ["PartitionsFor"] = (c => c.PartitionsFor(Topic, s_negative), c => c.PartitionsFor(Topic, s_negative)),
                ["ListTopics"] = (c => c.ListTopics(s_negative), c => c.ListTopics(s_negative)),
            }));

        Assert.Equal(9, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            AssertConcurrent(call.Key, call.Value);
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WhileAnotherThreadIsInside_ANullArgument_IsReportedAfterTheConcurrentAccess_UnlessJavaChecksItBeforeAcquire(string kind)
    {
        IReadOnlyDictionary<string, Exception?> seen = await WhileAnotherThreadIsInside(Create(kind, groupId: true), consumer =>
            AsyncOnly(consumer, NullArgumentCalls(consumer), NullAsyncCallbackCalls()));

        Dictionary<string, (Type Type, string Detail)> before = new Dictionary<string, (Type, string)>
        {
            ["Commit(null)"] = (typeof(ArgumentNullException), "offsets"),
            ["Commit(null, timeout)"] = (typeof(ArgumentNullException), "offsets"),
            ["CommitAsync(null)"] = (typeof(ArgumentNullException), "offsets"),
            ["CommitAsync(null, callback)"] = (typeof(ArgumentNullException), "offsets"),
            ["CommitAsync(null, async callback)"] = (typeof(ArgumentNullException), "offsets"),
            ["SeekToBeginning(null)"] = (typeof(IllegalArgumentException), "Partitions collection cannot be null"),
            ["SeekToEnd(null)"] = (typeof(IllegalArgumentException), "Partitions collection cannot be null"),
            ["Seek(partition, null)"] = (typeof(ArgumentNullException), "offsetAndMetadata"),
            ["Seek(default, negative offset)"] = (typeof(IllegalArgumentException), "seek offset must not be a negative number"),
        };
        Assert.Equal(kind == "AsyncKafkaConsumer" ? 29 : 26, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            if (before.TryGetValue(call.Key, out (Type Type, string Detail) expected))
            {
                Assert.True(call.Value?.GetType() == expected.Type, call.Key + ": " + call.Value);
                Assert.Equal(
                    expected.Detail,
                    call.Value is ArgumentNullException nullArgument ? nullArgument.ParamName : call.Value!.Message);
            }
            else
            {
                AssertConcurrent(call.Key, call.Value);
            }
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WhileAnotherThreadIsInside_ANegativePartition_ReportsTheConcurrentAccess_ButANegativeOffsetComesFirst(string kind)
    {
        IReadOnlyDictionary<string, Exception?> seen = await WhileAnotherThreadIsInside(Create(kind, groupId: true), consumer =>
            Calls(consumer, NegativePartitionCalls()));

        // The binding makes no partition check: the core answers a negative partition after the owner check, as Java
        // does after acquire(). Java's seek checks a negative offset before acquire().
        Assert.Equal(11, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            if (call.Key == "Seek(partition, negative offset)")
            {
                IllegalArgumentException negative = Assert.IsType<IllegalArgumentException>(call.Value);
                Assert.Equal("seek offset must not be a negative number", negative.Message);
            }
            else
            {
                AssertConcurrent(call.Key, call.Value);
            }
        }
    }

    [Fact]
    public async Task OnTheAsyncMock_WhileARebalanceRefusesOtherThreads_ANullArgument_IsReportedAfterTheRefusal()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        try
        {
            Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
            bool returned = false;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    new[] { Topic },
                    new AssignedListener(() =>
                    {
                        Thread other = new Thread(() =>
                        {
                            foreach (KeyValuePair<string, Exception?> call in AsyncOnly(
                                consumer, NullArgumentCalls(consumer), NullAsyncCallbackCalls()))
                            {
                                seen[call.Key] = call.Value;
                            }
                        });
                        other.IsBackground = true;
                        other.Start();
                        returned = other.Join(s_deadline);
                    })),
                s_deadline);

            Exception? rebalanced = await Record.ExceptionAsync(() => TestTimeout.Run(() => consumer.Rebalance(s_partitions), s_deadline));

            Assert.Null(rebalanced);
            Assert.True(returned, "the other thread's calls did not return");
            Assert.Equal(29, seen.Count);
            foreach (KeyValuePair<string, Exception?> call in seen)
            {
                if (call.Key == "CurrentLag(default)")
                {
                    // Java's MockConsumer.currentLag is not synchronized and answers 0 for a partition with no end
                    // offset, which a null topic never has: the call returns its Task.
                    Assert.Equal(
                        "the call returned a Task, RanToCompletion",
                        Assert.IsType<InvalidOperationException>(call.Value).Message);
                }
                else
                {
                    AssertConcurrent(call.Key, call.Value);
                }
            }
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WhileAnotherThreadIsInside_GroupMetadataWithoutAGroupId_ReportsTheConcurrentAccessFirst(string kind)
    {
        IReadOnlyDictionary<string, Exception?> seen = await WhileAnotherThreadIsInside(
            Create(kind, groupId: false),
            consumer => new Dictionary<string, Exception?>
            {
                ["GroupMetadata"] = Record.Exception(() => ((IConsumerCommon)consumer).GroupMetadata()),
            });

        AssertConcurrent("GroupMetadata", seen["GroupMetadata"]);
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void WithoutAGroupId_ACommitOrCommittedWithANegativeTimeout_ReportsTheGroupIdFirst(string kind)
    {
        object consumer = Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        AssertGroupIdNotDefined(Thrown(consumer, c => c.Commit(s_negative), c => c.Commit(s_negative)));
        AssertGroupIdNotDefined(Thrown(consumer, c => c.Commit(Offsets(), s_negative), c => c.Commit(Offsets(), s_negative)));
        AssertGroupIdNotDefined(
            Thrown(consumer, c => c.Committed(s_partitions, s_negative), c => c.Committed(s_partitions, s_negative)));
        AssertNegative(
            Thrown(consumer, c => c.Position(s_tp0, s_negative), c => c.Position(s_tp0, s_negative)),
            "Invalid negative timeout -1000");
    }

    // Java's subscribeInternal runs throwIfGroupIdNotDefined() before it checks the topics.
    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void WithoutAGroupId_SubscribeToANullCollectionOrTopic_ReportsTheGroupIdFirst(string kind)
    {
        object consumer = Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        AssertGroupIdNotDefined(Thrown(consumer, c => c.Subscribe((IReadOnlyCollection<string>)null!), c => c.Subscribe((IReadOnlyCollection<string>)null!)));
        AssertGroupIdNotDefined(Thrown(consumer, c => c.Subscribe(new string[] { null! }), c => c.Subscribe(new string[] { null! })));
        AssertGroupIdNotDefined(Thrown(consumer, c => c.Subscribe((IReadOnlyCollection<string>)null!, s_listener), c => c.Subscribe((IReadOnlyCollection<string>)null!, s_asyncListener)));
    }

    // Java's subscribeToRegex runs throwIfGroupIdNotDefined() before it checks the pattern.
    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void WithoutAGroupId_SubscribeToANullPattern_ReportsTheGroupIdFirst(string kind)
    {
        object consumer = Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        Dictionary<string, Exception?> seen = AsyncOnly(consumer, NullPatternCalls(consumer), NullAsyncPatternCalls());

        Assert.Equal(kind.StartsWith("Async", StringComparison.Ordinal) ? 9 : 6, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            AssertGroupIdNotDefined(call.Value);
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void WithoutAGroupId_CommittedOfANullCollection_ReportsTheGroupIdFirst(string kind)
    {
        object consumer = Create(kind, groupId: false);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        AssertGroupIdNotDefined(Thrown(consumer, c => c.Committed(null!), c => c.Committed(null!)));
        AssertGroupIdNotDefined(Thrown(consumer, c => c.Committed(null!, s_second), c => c.Committed(null!, s_second)));
        AssertGroupIdNotDefined(Thrown(consumer, c => c.Committed(null!, s_negative), c => c.Committed(null!, s_negative)));
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void WithAGroupId_CommittedOfANullCollection_ReportsANegativeTimeoutFirst(string kind)
    {
        object consumer = Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        AssertNegative(
            Thrown(consumer, c => c.Committed(null!, s_negative), c => c.Committed(null!, s_negative)),
            "Timeout must not be negative");
        Assert.Equal(
            "partitions",
            Assert.IsType<ArgumentNullException>(Thrown(consumer, c => c.Committed(null!, s_second), c => c.Committed(null!, s_second))).ParamName);
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void PartitionsForANullTopic_TimesOutAtOnceWithAZeroTimeout_AfterANegativeTimeoutCheck(string kind)
    {
        object consumer = Create(kind, groupId: true);
        using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

        KafkaTimeoutException zero = Assert.IsType<KafkaTimeoutException>(
            Thrown(consumer, c => c.PartitionsFor(null!, TimeSpan.Zero), c => c.PartitionsFor(null!, TimeSpan.Zero)));
        Assert.Equal(RequestTimedOutCode, zero.Code);
        Assert.IsAssignableFrom<Common.Errors.RetriableException>(zero);
        Assert.Equal(string.Empty, zero.Message);
        AssertNegative(
            Thrown(consumer, c => c.PartitionsFor(null!, s_negative), c => c.PartitionsFor(null!, s_negative)),
            "Timeout must not be negative");
        Assert.Equal(
            "topic",
            Assert.IsType<ArgumentNullException>(Thrown(consumer, c => c.PartitionsFor(null!, s_second), c => c.PartitionsFor(null!, s_second))).ParamName);
        Assert.Equal(
            "topic",
            Assert.IsType<ArgumentNullException>(Thrown(consumer, c => c.PartitionsFor(null!), c => c.PartitionsFor(null!))).ParamName);
    }

    [Fact]
    public void OnTheMocks_PartitionsForANullTopic_WithAZeroTimeout_ThrowsArgumentNull()
    {
        foreach (string kind in new[] { "MockConsumer", "AsyncMockConsumer" })
        {
            object consumer = Create(kind, groupId: true);
            using IDisposable disposal = TestTimeout.Disposal((IDisposable)consumer);

            Assert.Equal(
                "topic",
                Assert.IsType<ArgumentNullException>(Thrown(consumer, c => c.PartitionsFor(null!, TimeSpan.Zero), c => c.PartitionsFor(null!, TimeSpan.Zero))).ParamName);
        }
    }

    [Theory]
    [MemberData(nameof(AllConsumers))]
    public void OnAClosedConsumer_AnInvalidArgument_ReportsTheClosedConsumerFirst(string kind)
    {
        object consumer = Create(kind, groupId: true);
        TestTimeout.Dispose((IDisposable)consumer);

        Dictionary<string, Exception?> seen = Calls(consumer, new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
        {
            ["Commit(null)"] = (c => c.Commit(null!), c => c.Commit(null!)),
            ["Commit(null, timeout)"] = (c => c.Commit(null!, s_negative), c => c.Commit(null!, s_negative)),
            ["Position(default)"] = (c => c.Position(default), c => c.Position(default)),
            ["Position(default, timeout)"] = (c => c.Position(default, s_negative), c => c.Position(default, s_negative)),
            ["Committed(null)"] = (c => c.Committed(null!), c => c.Committed(null!)),
            ["Committed(null, timeout)"] = (c => c.Committed(null!, s_negative), c => c.Committed(null!, s_negative)),
            ["OffsetsForTimes(null)"] = (c => c.OffsetsForTimes(null!), c => c.OffsetsForTimes(null!)),
            ["OffsetsForTimes(null, timeout)"] = (c => c.OffsetsForTimes(null!, s_negative), c => c.OffsetsForTimes(null!, s_negative)),
            ["BeginningOffsets(null)"] = (c => c.BeginningOffsets(null!), c => c.BeginningOffsets(null!)),
            ["BeginningOffsets(null, timeout)"] = (c => c.BeginningOffsets(null!, s_negative), c => c.BeginningOffsets(null!, s_negative)),
            ["EndOffsets(null)"] = (c => c.EndOffsets(null!), c => c.EndOffsets(null!)),
            ["EndOffsets(null, timeout)"] = (c => c.EndOffsets(null!, s_negative), c => c.EndOffsets(null!, s_negative)),
            ["PartitionsFor(null)"] = (c => c.PartitionsFor(null!), c => c.PartitionsFor(null!)),
            ["PartitionsFor(null, timeout)"] = (c => c.PartitionsFor(null!, TimeSpan.Zero), c => c.PartitionsFor(null!, TimeSpan.Zero)),
            ["ListTopics(timeout)"] = (c => c.ListTopics(s_negative), c => c.ListTopics(s_negative)),
            ["CommitAsync(null)"] = (c => c.CommitAsync(s_nullOffsets), c => c.CommitAsync(s_nullOffsets)),
            ["CommitAsync(null, callback)"] = (c => c.CommitAsync(null!, s_callback), c => c.CommitAsync(null!, s_callback)),
            ["CommitAsync(null callback)"] = (
                c => c.CommitAsync((IOffsetCommitCallback)null!), c => c.CommitAsync((IOffsetCommitCallback)null!)),
            ["Pause(null)"] = (c => c.Pause(null!), c => c.Pause(null!)),
            ["Resume(null)"] = (c => c.Resume(null!), c => c.Resume(null!)),
            ["SeekToBeginning(null)"] = (c => c.SeekToBeginning(null!), c => c.SeekToBeginning(null!)),
            ["SeekToEnd(null)"] = (c => c.SeekToEnd(null!), c => c.SeekToEnd(null!)),
            ["Seek(default, offset)"] = (c => c.Seek(default, 0L), c => c.Seek(default, 0L)),
            ["Seek(default, negative offset)"] = (c => c.Seek(default, -1L), c => c.Seek(default, -1L)),
            ["Seek(partition, null)"] = (c => c.Seek(s_tp0, (OffsetAndMetadata)null!), c => c.Seek(s_tp0, (OffsetAndMetadata)null!)),
            ["Seek(default, offsetAndMetadata)"] = (
                c => c.Seek(default, new OffsetAndMetadata(1)), c => c.Seek(default, new OffsetAndMetadata(1))),
            ["CurrentLag(default)"] = (c => c.CurrentLag(default), c => c.CurrentLag(default)),
        });
        foreach (KeyValuePair<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)> call in NegativePartitionCalls())
        {
            seen[call.Key] = Thrown(consumer, call.Value.Item1, call.Value.Item2);
        }

        AsyncOnly(consumer, seen, NullAsyncCallbackCalls());

        // Java's KafkaConsumer checks these before acquire(), so before the closed check; Java's MockConsumer after
        // ensureNotClosed().
        Dictionary<string, (Type Type, string Detail)> beforeAcquire = kind.Contains("Mock", StringComparison.Ordinal)
            ? new Dictionary<string, (Type, string)>()
            : new Dictionary<string, (Type, string)>
            {
                ["SeekToBeginning(null)"] = (typeof(IllegalArgumentException), "Partitions collection cannot be null"),
                ["SeekToEnd(null)"] = (typeof(IllegalArgumentException), "Partitions collection cannot be null"),
                ["Seek(default, negative offset)"] = (
                    typeof(IllegalArgumentException), "seek offset must not be a negative number"),
                ["Seek(partition, negative offset)"] = (
                    typeof(IllegalArgumentException), "seek offset must not be a negative number"),
                ["Seek(partition, null)"] = (typeof(ArgumentNullException), "offsetAndMetadata"),
            };
        Assert.Equal(kind.StartsWith("Async", StringComparison.Ordinal) ? 41 : 38, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            if (beforeAcquire.TryGetValue(call.Key, out (Type Type, string Detail) expected))
            {
                Assert.True(call.Value?.GetType() == expected.Type, call.Key + ": " + call.Value);
                Assert.Equal(
                    expected.Detail,
                    call.Value is ArgumentNullException nullArgument ? nullArgument.ParamName : call.Value!.Message);
            }
            else
            {
                Assert.True(call.Value is IllegalStateException, call.Key + ": " + call.Value);
                Assert.Equal(ClosedMessage, call.Value!.Message);
            }
        }
    }

    [Theory]
    [MemberData(nameof(AllConsumers))]
    public void OnAClosedConsumer_ANullSubscribeOrAssign_ReportsTheClosedConsumer(string kind)
    {
        object consumer = Create(kind, groupId: true);
        TestTimeout.Dispose((IDisposable)consumer);

        Dictionary<string, Exception?> seen = AsyncOnly(consumer, NullSubscribeAndAssignCalls(consumer), NullAsyncSubscribeCalls());

        Assert.Equal(kind.StartsWith("Async", StringComparison.Ordinal) ? 6 : 5, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            Assert.True(
                call.Value is IllegalStateException { Message: "This consumer has already been closed." },
                call.Key + ": " + call.Value);
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void OnAClosedConsumer_ANullPattern_ReportsTheClosedConsumerFirst(string kind)
    {
        object consumer = Create(kind, groupId: true);
        TestTimeout.Dispose((IDisposable)consumer);

        Dictionary<string, Exception?> seen = AsyncOnly(consumer, NullPatternCalls(consumer), NullAsyncPatternCalls());

        Assert.Equal(kind.StartsWith("Async", StringComparison.Ordinal) ? 9 : 6, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            Assert.True(call.Value is IllegalStateException { Message: ClosedMessage }, call.Key + ": " + call.Value);
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WhileAnotherThreadIsInside_ANullPattern_ReportsTheConcurrentAccess(string kind)
    {
        IReadOnlyDictionary<string, Exception?> seen = await WhileAnotherThreadIsInside(Create(kind, groupId: true), consumer =>
            AsyncOnly(consumer, NullPatternCalls(consumer), NullAsyncPatternCalls()));

        Assert.Equal(kind.StartsWith("Async", StringComparison.Ordinal) ? 9 : 6, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            AssertConcurrent(call.Key, call.Value);
        }
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public async Task WhileAnotherThreadIsInside_ANullSubscribeOrAssign_ReportsTheConcurrentAccess(string kind)
    {
        IReadOnlyDictionary<string, Exception?> seen = await WhileAnotherThreadIsInside(Create(kind, groupId: true), consumer =>
            AsyncOnly(consumer, NullSubscribeAndAssignCalls(consumer), NullAsyncSubscribeCalls()));

        Assert.Equal(kind.StartsWith("Async", StringComparison.Ordinal) ? 6 : 5, seen.Count);
        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            AssertConcurrent(call.Key, call.Value);
        }
    }

    [Fact]
    public void InsideACallback_WithoutAGroupId_GroupMetadataAndCommitted_ReportTheGroupIdBeforeTheRoute()
    {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(Config(groupId: false), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable disposal = TestTimeout.Disposal(consumer);
        ConsumerCallbackReporter reporter = Reporter(consumer);
        Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();

        ConsumerCallbackReporter.CallbackScope scope = reporter.Enter(asyncCallback: false);
        try
        {
            seen["GroupMetadata"] = Record.Exception(() => consumer.GroupMetadata());
            seen["Committed(null)"] = Record.Exception(() => consumer.Committed(null!));
            seen["Commit(negative)"] = Record.Exception(() => consumer.Commit(s_negative));
        }
        finally
        {
            reporter.Exit(scope);
        }

        foreach (KeyValuePair<string, Exception?> call in seen)
        {
            AssertGroupIdNotDefined(call.Value);
        }
    }

    private static async Task<IReadOnlyDictionary<string, Exception?>> WhileAnotherThreadIsInside(
        object consumer,
        Func<object, IReadOnlyDictionary<string, Exception?>> checks)
    {
        try
        {
            Task polling = StartAPollOnAnotherThread(consumer);
            IReadOnlyDictionary<string, Exception?> seen;
            try
            {
                SpinUntilHeld(consumer, polling);
                seen = checks(consumer);
            }
            finally
            {
                ((IConsumerCommon)consumer).Wakeup();
            }

            Exception? polled = await Record.ExceptionAsync(() => TestTimeout.Run(() => polling, s_deadline));
            Assert.True(polled is WakeupException, polled?.ToString());
            return seen;
        }
        finally
        {
            TestTimeout.Dispose((IDisposable)consumer);
        }
    }

    // The poll's use belongs to the thread that made the call, so the checks the test thread makes meanwhile are
    // another thread's.
    private static Task StartAPollOnAnotherThread(object consumer)
    {
        Task? polling = null;
        Thread other = new Thread(() =>
        {
            if (consumer is IConsumer<byte[], byte[]> sync)
            {
                sync.Assign(s_partitions);
                polling = Task.Factory.StartNew(
                    () => sync.Poll(s_pollTimeout),
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
            }
            else
            {
                IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)consumer;
                async.Assign(s_partitions).GetAwaiter().GetResult();
                polling = async.Poll(s_pollTimeout);
            }
        });
        other.IsBackground = true;
        other.Start();
        Assert.True(other.Join(s_deadline), "the poll was never started");
        return polling!;
    }

    // Reads the owner count rather than calling the consumer, as a call of the test thread's would refuse the poll.
    private static void SpinUntilHeld(object consumer, Task polling)
    {
        FieldInfo uses = typeof(NativeConsumer).GetField("_ownerUses", BindingFlags.NonPublic | BindingFlags.Instance)!;
        NativeConsumer native = Native(consumer);
        Stopwatch elapsed = Stopwatch.StartNew();
        while ((int)uses.GetValue(native)! == 0)
        {
            if (polling.IsCompleted)
            {
                throw new InvalidOperationException("the poll ended before it held the consumer", polling.Exception);
            }

            if (elapsed.Elapsed > s_deadline)
            {
                throw new System.TimeoutException("the poll never held the consumer");
            }

            Thread.Yield();
        }
    }

    private static Dictionary<string, Exception?> Calls(
        object consumer,
        Dictionary<string, (Action<IConsumer<byte[], byte[]>> Sync, Func<IAsyncConsumer<byte[], byte[]>, Task> Async)> calls)
    {
        Dictionary<string, Exception?> seen = new Dictionary<string, Exception?>();
        foreach (KeyValuePair<string, (Action<IConsumer<byte[], byte[]>> Sync, Func<IAsyncConsumer<byte[], byte[]>, Task> Async)> call in calls)
        {
            seen[call.Key] = Thrown(consumer, call.Value.Sync, call.Value.Async);
        }

        return seen;
    }

    // Every check throws before the call returns, on the async consumer too; a Task coming back means the call passed
    // its checks.
    private static Exception? Thrown(
        object consumer,
        Action<IConsumer<byte[], byte[]>> sync,
        Func<IAsyncConsumer<byte[], byte[]>, Task> async)
    {
        if (consumer is IConsumer<byte[], byte[]> syncConsumer)
        {
            return Record.Exception(() => sync(syncConsumer));
        }

        try
        {
            Task task = async((IAsyncConsumer<byte[], byte[]>)consumer);
            _ = task.ContinueWith(
                ended => _ = ended.Exception,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return new InvalidOperationException("the call returned a Task, " + task.Status);
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private static void AssertConcurrent(string call, Exception? thrown)
    {
        Assert.True(
            thrown is ConcurrentModificationException { Message: Concurrent },
            call + ": " + thrown);
    }

    private static void AssertGroupIdNotDefined(Exception? thrown)
    {
        InvalidGroupIdException error = Assert.IsType<InvalidGroupIdException>(thrown);
        Assert.Equal(InvalidGroupIdCode, error.Code);
        Assert.Equal(GroupIdNotDefined, error.Message);
    }

    private static void AssertNegative(Exception? thrown, string message)
    {
        IllegalArgumentException negative = Assert.IsType<IllegalArgumentException>(thrown);
        Assert.Equal(message, negative.Message);
    }

    private static ConsumerCallbackReporter Reporter(object consumer) =>
        (ConsumerCallbackReporter)typeof(NativeConsumer)
            .GetField("_reporter", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(Native(consumer))!;

    private static NativeConsumer Native(object consumer) =>
        (NativeConsumer)consumer.GetType().GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(consumer)!;

    private static object Create(string kind, bool groupId) => kind switch
    {
        "KafkaConsumer" => new KafkaConsumer<byte[], byte[]>(Config(groupId), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        "AsyncKafkaConsumer" => new AsyncKafkaConsumer<byte[], byte[]>(Config(groupId), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        "MockConsumer" => new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        _ => new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
    };

    private static Dictionary<string, object?> Config(bool groupId)
    {
        Dictionary<string, object?> config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["enable.auto.commit"] = "false",
        };
        if (groupId)
        {
            config["group.id"] = "check-order-group";
        }

        return config;
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets() =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(1) };

    private static Dictionary<TopicPartition, long> Timestamps() => new Dictionary<TopicPartition, long> { [s_tp0] = 0 };

    private static Dictionary<string, Exception?> NullArgumentCalls(object consumer) =>
        Calls(consumer, new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
        {
            ["Commit(null)"] = (c => c.Commit(null!), c => c.Commit(null!)),
            ["Commit(null, timeout)"] = (c => c.Commit(null!, s_second), c => c.Commit(null!, s_second)),
            ["Position(default)"] = (c => c.Position(default), c => c.Position(default)),
            ["Position(default, timeout)"] = (c => c.Position(default, s_second), c => c.Position(default, s_second)),
            ["Committed(null)"] = (c => c.Committed(null!), c => c.Committed(null!)),
            ["Committed(null, timeout)"] = (c => c.Committed(null!, s_second), c => c.Committed(null!, s_second)),
            ["OffsetsForTimes(null)"] = (c => c.OffsetsForTimes(null!), c => c.OffsetsForTimes(null!)),
            ["OffsetsForTimes(null, timeout)"] = (c => c.OffsetsForTimes(null!, s_second), c => c.OffsetsForTimes(null!, s_second)),
            ["BeginningOffsets(null)"] = (c => c.BeginningOffsets(null!), c => c.BeginningOffsets(null!)),
            ["BeginningOffsets(null, timeout)"] = (c => c.BeginningOffsets(null!, s_second), c => c.BeginningOffsets(null!, s_second)),
            ["EndOffsets(null)"] = (c => c.EndOffsets(null!), c => c.EndOffsets(null!)),
            ["EndOffsets(null, timeout)"] = (c => c.EndOffsets(null!, s_second), c => c.EndOffsets(null!, s_second)),
            ["PartitionsFor(null)"] = (c => c.PartitionsFor(null!), c => c.PartitionsFor(null!)),
            ["PartitionsFor(null, timeout)"] = (c => c.PartitionsFor(null!, s_second), c => c.PartitionsFor(null!, s_second)),
            ["CommitAsync(null)"] = (c => c.CommitAsync(s_nullOffsets), c => c.CommitAsync(s_nullOffsets)),
            ["CommitAsync(null, callback)"] = (c => c.CommitAsync(null!, s_callback), c => c.CommitAsync(null!, s_callback)),
            ["CommitAsync(null callback)"] = (
                    c => c.CommitAsync((IOffsetCommitCallback)null!), c => c.CommitAsync((IOffsetCommitCallback)null!)),
            ["Pause(null)"] = (c => c.Pause(null!), c => c.Pause(null!)),
            ["Resume(null)"] = (c => c.Resume(null!), c => c.Resume(null!)),
            ["SeekToBeginning(null)"] = (c => c.SeekToBeginning(null!), c => c.SeekToBeginning(null!)),
            ["SeekToEnd(null)"] = (c => c.SeekToEnd(null!), c => c.SeekToEnd(null!)),
            ["Seek(default, offset)"] = (c => c.Seek(default, 0L), c => c.Seek(default, 0L)),
            ["Seek(default, negative offset)"] = (c => c.Seek(default, -1L), c => c.Seek(default, -1L)),
            ["Seek(partition, null)"] = (c => c.Seek(s_tp0, (OffsetAndMetadata)null!), c => c.Seek(s_tp0, (OffsetAndMetadata)null!)),
            ["Seek(default, offsetAndMetadata)"] = (
                    c => c.Seek(default, new OffsetAndMetadata(1)), c => c.Seek(default, new OffsetAndMetadata(1))),
            ["CurrentLag(default)"] = (c => c.CurrentLag(default), c => c.CurrentLag(default)),
        });

    // Java's KafkaConsumer checks these arguments after acquireAndEnsureOpen(); the listener of subscribe(topics,
    // listener) is checked before it, so these calls give a listener.
    private static Dictionary<string, Exception?> NullSubscribeAndAssignCalls(object consumer) =>
        Calls(consumer, new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
        {
            ["Subscribe(null)"] = (c => c.Subscribe((IReadOnlyCollection<string>)null!), c => c.Subscribe((IReadOnlyCollection<string>)null!)),
            ["Subscribe(null, listener)"] = (c => c.Subscribe((IReadOnlyCollection<string>)null!, s_listener), c => c.Subscribe((IReadOnlyCollection<string>)null!, s_listener)),
            ["Subscribe(empty topic)"] = (c => c.Subscribe(new[] { string.Empty }), c => c.Subscribe(new[] { string.Empty })),
            ["Assign(null)"] = (c => c.Assign(null!), c => c.Assign(null!)),
            ["Assign(null topic)"] = (
                c => c.Assign(new[] { new TopicPartition(null!, 0) }), c => c.Assign(new[] { new TopicPartition(null!, 0) })),
        });

    private static Dictionary<string, Func<IAsyncConsumer<byte[], byte[]>, Task>> NullAsyncSubscribeCalls() =>
        new Dictionary<string, Func<IAsyncConsumer<byte[], byte[]>, Task>>
        {
            ["Subscribe(null, async listener)"] = c => c.Subscribe((IReadOnlyCollection<string>)null!, s_asyncListener),
        };

    // Java's KafkaConsumer checks the pattern after acquireAndEnsureOpen() and the group.id check; the listener is
    // checked before them, so these calls give a listener.
    private static Dictionary<string, Exception?> NullPatternCalls(object consumer) =>
        Calls(consumer, new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
        {
            ["Subscribe(null pattern)"] = (c => c.Subscribe((SubscriptionPattern)null!), c => c.Subscribe((SubscriptionPattern)null!)),
            ["Subscribe(null pattern, listener)"] = (
                c => c.Subscribe((SubscriptionPattern)null!, s_listener), c => c.Subscribe((SubscriptionPattern)null!, s_listener)),
            ["Subscribe(null text)"] = (c => c.Subscribe(s_nullPattern), c => c.Subscribe(s_nullPattern)),
            ["Subscribe(null text, listener)"] = (
                c => c.Subscribe(s_nullPattern, s_listener), c => c.Subscribe(s_nullPattern, s_listener)),
            ["Subscribe(NUL text)"] = (c => c.Subscribe(s_nulPattern), c => c.Subscribe(s_nulPattern)),
            ["Subscribe(NUL text, listener)"] = (
                c => c.Subscribe(s_nulPattern, s_listener), c => c.Subscribe(s_nulPattern, s_listener)),
        });

    private static Dictionary<string, Func<IAsyncConsumer<byte[], byte[]>, Task>> NullAsyncPatternCalls() =>
        new Dictionary<string, Func<IAsyncConsumer<byte[], byte[]>, Task>>
        {
            ["Subscribe(null pattern, async listener)"] = c => c.Subscribe((SubscriptionPattern)null!, s_asyncListener),
            ["Subscribe(null text, async listener)"] = c => c.Subscribe(s_nullPattern, s_asyncListener),
            ["Subscribe(NUL text, async listener)"] = c => c.Subscribe(s_nulPattern, s_asyncListener),
        };

    private static Dictionary<string, Func<IAsyncConsumer<byte[], byte[]>, Task>> NullAsyncCallbackCalls() =>
        new Dictionary<string, Func<IAsyncConsumer<byte[], byte[]>, Task>>
        {
            ["CommitAsync(null async callback)"] = c => c.CommitAsync((IAsyncOffsetCommitCallback)null!),
            ["CommitAsync(null, async callback)"] = c => c.CommitAsync(null!, s_asyncCallback),
            ["CommitAsync(offsets, null async callback)"] = c => c.CommitAsync(Offsets(), (IAsyncOffsetCommitCallback)null!),
        };

    private static Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)> NegativePartitionCalls() =>
        new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Func<IAsyncConsumer<byte[], byte[]>, Task>)>
        {
            ["Pause(negative partition)"] = (c => c.Pause(s_negativePartitions), c => c.Pause(s_negativePartitions)),
            ["Resume(negative partition)"] = (c => c.Resume(s_negativePartitions), c => c.Resume(s_negativePartitions)),
            ["SeekToBeginning(negative partition)"] = (
                c => c.SeekToBeginning(s_negativePartitions), c => c.SeekToBeginning(s_negativePartitions)),
            ["SeekToEnd(negative partition)"] = (c => c.SeekToEnd(s_negativePartitions), c => c.SeekToEnd(s_negativePartitions)),
            ["Seek(negative partition, offset)"] = (c => c.Seek(s_negativePartition, 0L), c => c.Seek(s_negativePartition, 0L)),
            ["Seek(negative partition, offsetAndMetadata)"] = (
                c => c.Seek(s_negativePartition, new OffsetAndMetadata(0)), c => c.Seek(s_negativePartition, new OffsetAndMetadata(0))),
            ["Seek(partition, negative offset)"] = (c => c.Seek(s_tp0, -1L), c => c.Seek(s_tp0, -1L)),
            ["CurrentLag(negative partition)"] = (c => c.CurrentLag(s_negativePartition), c => c.CurrentLag(s_negativePartition)),
            ["Commit(negative partition)"] = (c => c.Commit(NegativeOffsets()), c => c.Commit(NegativeOffsets())),
            ["CommitAsync(negative partition)"] = (c => c.CommitAsync(NegativeOffsets()), c => c.CommitAsync(NegativeOffsets())),
            ["Position(negative partition)"] = (c => c.Position(s_negativePartition), c => c.Position(s_negativePartition)),
        };

    private static Dictionary<TopicPartition, OffsetAndMetadata> NegativeOffsets() =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_negativePartition] = new OffsetAndMetadata(1) };

    private static Dictionary<string, Exception?> AsyncOnly(
        object consumer,
        Dictionary<string, Exception?> seen,
        Dictionary<string, Func<IAsyncConsumer<byte[], byte[]>, Task>> calls)
    {
        if (consumer is IAsyncConsumer<byte[], byte[]>)
        {
            foreach (KeyValuePair<string, Func<IAsyncConsumer<byte[], byte[]>, Task>> call in calls)
            {
                seen[call.Key] = Thrown(consumer, _ => { }, call.Value);
            }
        }

        return seen;
    }

    /// <summary>A listener whose assignment runs the given action.</summary>
    private sealed class AssignedListener : IConsumerRebalanceListener
    {
        private readonly Action _onAssigned;

        internal AssignedListener(Action onAssigned) => _onAssigned = onAssigned;

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();
    }

    /// <summary>An asynchronous rebalance listener that does nothing.</summary>
    private sealed class NoOpAsyncListener : IAsyncConsumerRebalanceListener
    {
        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }

    /// <summary>A commit callback that does nothing.</summary>
    private sealed class NoOpCommitCallback : IOffsetCommitCallback
    {
        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
        {
        }
    }

    /// <summary>An async commit callback that does nothing.</summary>
    private sealed class NoOpAsyncCommitCallback : IAsyncOffsetCommitCallback
    {
        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            Task.CompletedTask;
    }
}
