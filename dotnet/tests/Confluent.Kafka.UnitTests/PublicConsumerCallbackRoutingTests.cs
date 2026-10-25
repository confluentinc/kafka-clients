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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A consumer call made inside a callback, on the four consumers: routed to the consumer's own
/// <see cref="ConsumerHandle"/>, refused at once, or sent to the core, inside a listener and a commit callback and
/// after an await.
/// </summary>
public sealed class PublicConsumerCallbackRoutingTests
{
    private const string Topic = "callback-routing-topic";

    private const string Concurrent = "KafkaConsumer is not safe for multi-threaded access.";

    private const string MockHandleUnsupported =
        "ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer directly.";

    private const string CommittedTimeout =
        "Timeout of 500ms expired before the last committed offset for partitions [" + Topic
        + "-0] could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.";

    private const string ShortCommittedTimeout =
        "Timeout of 200ms expired before the last committed offset for partitions [" + Topic
        + "-0] could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.";

    private const string OffsetsTimeout = "Failed to get offsets by times in 500ms";

    private const string ShortOffsetsTimeout = "Failed to get offsets by times in 200ms";

    private const string TimedOut = "KafkaException 7: ";

    private const string TopicMetadataExpired =
        "Timeout expired while fetching topic metadata|Timeout expired while waiting for TopicMetadata";

    private const string AllTopicsMetadataExpired =
        "Timeout expired while fetching topic metadata|Timeout expired while waiting for AllTopicsMetadata";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_second = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan s_negative = TimeSpan.FromMilliseconds(-1);

    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan s_minusASecond = TimeSpan.FromSeconds(-1);

    private static readonly TimeSpan s_wakeableTimeout = TimeSpan.FromMilliseconds(300);

    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromSeconds(8);

    private static readonly TimeSpan s_wellInsideThePoll = TimeSpan.FromSeconds(4);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition[] s_partitions = { s_tp0 };

    private static readonly string[] s_topics = { Topic };

    public static IEnumerable<object[]> Vehicles() => new[]
    {
        new object[] { "KafkaConsumer, a commit callback" },
        new object[] { "MockConsumer, a listener" },
        new object[] { "MockConsumer, a commit callback" },
        new object[] { "AsyncKafkaConsumer, a commit callback" },
        new object[] { "AsyncKafkaConsumer, an async commit callback after an await" },
        new object[] { "AsyncMockConsumer, a listener" },
        new object[] { "AsyncMockConsumer, an async listener after an await" },
        new object[] { "AsyncMockConsumer, a commit callback" },
        new object[] { "AsyncMockConsumer, an async commit callback after an await" },
    };

    public static IEnumerable<object[]> StartedThreads() => new[]
    {
        new object[] { "KafkaConsumer, a commit callback" },
        new object[] { "AsyncKafkaConsumer, a commit callback" },
        new object[] { "AsyncMockConsumer, a listener" },
    };

    [Theory]
    [MemberData(nameof(Vehicles))]
    public async Task InsideACallback_EachCall_IsRoutedToTheHandle_RefusedAtOnce_OrSentToTheCore(string vehicle)
    {
        bool mock = vehicle.Contains("Mock");
        bool async = vehicle.StartsWith("Async", StringComparison.Ordinal);

        IReadOnlyList<string> outcomes = await RunInside(vehicle, Probe);

        AssertOutcomes(Expected(mock, async), outcomes);
    }

    [Theory]
    [MemberData(nameof(StartedThreads))]
    public async Task AThreadASyncCallbackStarts_IsNotInside_SoItsCommitIsAnotherThreadsCall(string vehicle)
    {
        IReadOnlyList<string> outcomes = await RunInside(vehicle, (consumer, _) =>
        {
            string? outcome = null;
            Thread started = new Thread(() => outcome = Commit(consumer));
            started.IsBackground = true;
            started.Start();
            return new[] { started.Join(s_deadline) ? outcome! : "never ended" };
        });

        Assert.Equal(new[] { "throws InvalidOperationException: " + Concurrent }, outcomes);
    }

    [Theory]
    [InlineData("AsyncKafkaConsumer, an async commit callback after an await")]
    [InlineData("AsyncMockConsumer, an async listener after an await")]
    public async Task WorkAnAsyncCallbackStarts_IsInside_SoItsCommitRoutes(string vehicle)
    {
        IReadOnlyList<string> outcomes = await RunInside(vehicle, (consumer, _) =>
            new[] { Task.Run(() => Commit(consumer)).GetAwaiter().GetResult() });

        Assert.Equal(new[] { "ok" }, outcomes);
    }

    [Fact]
    public async Task AsyncMock_WorkAnAsyncListenerStarts_CommitsIntoTheMock()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            string? outcome = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    s_topics,
                    new AsyncListener(async () =>
                    {
                        await Task.Yield();
                        outcome = await Task.Run(() => Call(() => consumer.Commit(Offsets(8))));
                    })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(s_partitions), s_deadline);

            Assert.Equal("ok", outcome);
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                await TestTimeout.Run(() => consumer.Committed(s_partitions), s_deadline);
            Assert.Equal(8, committed[s_tp0]!.Offset);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task SyncMock_AListenerRunningWhileAnotherThreadDisposes_CallsBackThroughTheRouting()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        List<string> outcomes = new List<string>();
        consumer.Subscribe(
            s_topics,
            new SyncListener(() =>
            {
                entered.Set();
                _ = release.Wait(s_deadline);
                outcomes.Add(Call(() => consumer.Commit(Offsets(3))));
                outcomes.Add(Call(() => Format(consumer.Assignment())));
                outcomes.Add(Call(() => consumer.Poll(TimeSpan.Zero)));
            }));

        Task rebalance = Task.Run(() => consumer.Rebalance(s_partitions));
        Assert.True(entered.Wait(s_deadline), "the listener never ran");
        Task disposing = Task.Run(consumer.Dispose);
        try
        {
            SpinUntilClosing(consumer);
        }
        finally
        {
            release.Set();
        }

        await TestTimeout.Run(() => rebalance, s_deadline);
        await TestTimeout.Run(() => disposing, s_deadline);
        Assert.Equal(
            new[] { "ok", "ok [" + Topic + "-0]", "throws InvalidOperationException: " + Concurrent },
            outcomes);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public async Task AsyncMock_AListenerRunningWhileAnotherThreadDisposes_CallsBackThroughTheRouting()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        List<string> outcomes = new List<string>();
        await TestTimeout.Run(
            () => consumer.Subscribe(
                s_topics,
                new SyncListener(() =>
                {
                    entered.Set();
                    _ = release.Wait(s_deadline);
                    outcomes.Add(Call(() => consumer.Commit(Offsets(3))));
                    outcomes.Add(Call(() => Format(consumer.Assignment())));
                    outcomes.Add(Call(() => consumer.Poll(TimeSpan.Zero)));
                })),
            s_deadline);

        Task rebalance = consumer.Rebalance(s_partitions);
        Assert.True(entered.Wait(s_deadline), "the listener never ran");
        Task disposing = Task.Run(() => consumer.DisposeAsync().AsTask());
        try
        {
            SpinUntilClosing(consumer);
        }
        finally
        {
            release.Set();
        }

        await TestTimeout.Run(() => disposing, s_deadline);
        _ = await Record.ExceptionAsync(() => TestTimeout.Run(() => rebalance, s_deadline));
        Assert.Equal(
            new[] { "ok", "ok [" + Topic + "-0]", "throws InvalidOperationException: " + Concurrent },
            outcomes);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public void KafkaConsumer_TheRoutingHandle_IsReleasedAfterTheConsumersDestroy()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        Func<(bool BeforeTheRelease, bool AfterTheRelease)?> released;
        try
        {
            TestTimeout.Run(
                () =>
                {
                    consumer.Assign(s_partitions);
                    consumer.CommitAsync(Empty(), new DelegateCallback(() => consumer.Assignment()));
                    consumer.Commit(Empty());
                },
                s_deadline);
            released = WatchTheRoutingHandle(consumer);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }

        Assert.Equal((false, true), released());
    }

    [Fact]
    public async Task AsyncKafkaConsumer_TheRoutingHandle_IsReleasedAfterTheConsumersDestroy()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        Func<(bool BeforeTheRelease, bool AfterTheRelease)?> released;
        try
        {
            await TestTimeout.Run(() => consumer.Assign(s_partitions), s_deadline);
            await TestTimeout.Run(
                () => consumer.CommitAsync(Empty(), new DelegateCallback(() => consumer.Assignment())),
                s_deadline);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            released = WatchTheRoutingHandle(consumer);
        }
        finally
        {
            await TestTimeout.RunOffThisThread(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }

        Assert.Equal((false, true), released());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task KafkaConsumer_ADisposeFromAnotherThread_EndsThePoll_WhoseCallbackCaughtARoutedCallsWakeup(int routedCalls)
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        List<string> outcomes = new List<string>();
        Task disposing = Task.CompletedTask;
        try
        {
            TestTimeout.Run(
                () =>
                {
                    consumer.Assign(s_partitions);
                    consumer.CommitAsync(Empty(), new DelegateCallback(() =>
                    {
                        entered.Set();
                        _ = release.Wait(s_deadline);
                        for (int i = 0; i < routedCalls; i++)
                        {
                            outcomes.Add(Call(() => consumer.Committed(s_partitions)));
                        }
                    }));
                },
                s_deadline);

            Task<string> polling = Task.Run(() => Call(() => consumer.Poll(s_pollTimeout)));
            Assert.True(entered.Wait(s_deadline), "the commit callback never ran inside the Poll");
            disposing = Task.Run(consumer.Dispose);
            Stopwatch elapsed;
            try
            {
                SpinUntilClosing(consumer);
            }
            finally
            {
                elapsed = Stopwatch.StartNew();
                release.Set();
            }

            await TestTimeout.RunOffThisThread(() => disposing, s_wellInsideThePoll);
            string polled = await TestTimeout.Run(() => polling, s_wellInsideThePoll);

            Assert.True(elapsed.Elapsed < s_wellInsideThePoll, "Dispose took " + elapsed.Elapsed);
            Assert.Equal(Enumerable.Repeat("throws KafkaException -18: ", routedCalls), outcomes);
            Assert.Equal("throws KafkaException -18: ", polled);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            release.Set();
            await TestTimeout.Run(() => disposing, s_deadline);
            TestTimeout.Dispose(consumer);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task AsyncKafkaConsumer_ADisposeAsyncFromAnotherThread_EndsThePoll_WhoseCallbackCaughtARoutedCallsWakeup(
        int routedCalls)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        List<string> outcomes = new List<string>();
        Task disposing = Task.CompletedTask;
        try
        {
            await TestTimeout.Run(() => consumer.Assign(s_partitions), s_deadline);
            await TestTimeout.Run(
                () => consumer.CommitAsync(Empty(), new DelegateCallback(() =>
                {
                    entered.Set();
                    _ = release.Wait(s_deadline);
                    for (int i = 0; i < routedCalls; i++)
                    {
                        outcomes.Add(CallAsync(() => consumer.Committed(s_partitions)));
                    }
                })),
                s_deadline);

            Task polling = consumer.Poll(s_pollTimeout);
            Assert.True(entered.Wait(s_deadline), "the commit callback never ran inside the Poll");
            disposing = Task.Run(() => consumer.DisposeAsync().AsTask());
            Stopwatch elapsed;
            try
            {
                SpinUntilClosing(consumer);
            }
            finally
            {
                elapsed = Stopwatch.StartNew();
                release.Set();
            }

            await TestTimeout.RunOffThisThread(() => disposing, s_wellInsideThePoll);
            Exception? polled = await Record.ExceptionAsync(() => TestTimeout.Run(() => polling, s_wellInsideThePoll));

            Assert.True(elapsed.Elapsed < s_wellInsideThePoll, "DisposeAsync took " + elapsed.Elapsed);
            Assert.Equal(Enumerable.Repeat("faults KafkaException -18: ", routedCalls), outcomes);
            Assert.Equal("KafkaException -18: ", polled is null ? "ok" : Describe(polled));
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            release.Set();
            await TestTimeout.Run(() => disposing, s_deadline);
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Theory]
    [MemberData(nameof(WakeableCalls))]
    public async Task ARoutedCall_EndedByTheConsumersOwnWakeup_IsMadeAgain(string call)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
        try
        {
            await TestTimeout.Run(() => consumer.Assign(s_partitions), s_deadline);
            await CancelACommitAwaitingTheAsyncCallbackItDelivered(consumer);

            string? outcome = null;
            await TestTimeout.Run(
                () => consumer.CommitAsync(Empty(), new DelegateCallback(() => outcome = CallAsync(() => Wakeable(consumer, call)))),
                s_deadline);
            await TestTimeout.Run(() => consumer.CommitAsync(Empty()), s_deadline);

            Assert.NotNull(outcome);
            Assert.StartsWith("faults " + TimedOut, outcome, StringComparison.Ordinal);
            AssertTimedOutMessage(WakeableTimeout(call), outcome.Substring("faults ".Length + TimedOut.Length));
            Assert.Empty(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task ARoutedCall_WithATokenCancelledBefore_IsCancelled_AndCommitsNothing()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            using CancellationTokenSource cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            string? outcome = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    s_topics,
                    new SyncListener(() => outcome = CallAsync(() => consumer.Commit(Offsets(9), cancelled.Token)))),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(s_partitions), s_deadline);

            Assert.Equal("throws OperationCanceledException: The operation was canceled.", outcome);
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                await TestTimeout.Run(() => consumer.Committed(s_partitions), s_deadline);
            Assert.Null(committed.TryGetValue(s_tp0, out OffsetAndMetadata? landed) ? landed : null);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task ARoutedCall_WhoseTokenIsCancelledDuringTheCall_KeepsItsResult()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            using CancellationTokenSource cancel = new CancellationTokenSource();
            bool ran = false;
            string? outcome = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(
                    s_topics,
                    new SyncListener(() => outcome = CallAsync(() => consumer.CommitAsync(
                        Offsets(4),
                        new DelegateCallback(() =>
                        {
                            cancel.Cancel();
                            ran = true;
                        }),
                        cancel.Token)))),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(s_partitions), s_deadline);

            Assert.True(ran, "the commit callback never ran inside the routed call");
            Assert.True(cancel.IsCancellationRequested);
            Assert.Equal("ok", outcome);
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                await TestTimeout.Run(() => consumer.Committed(s_partitions), s_deadline);
            Assert.Equal(4, committed[s_tp0]!.Offset);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    public static IEnumerable<object[]> WakeableCalls() => new[]
    {
        new object[] { "committed" },
        new object[] { "committed_timed" },
        new object[] { "position" },
        new object[] { "position_timed" },
        new object[] { "commit" },
        new object[] { "commit_timed" },
        new object[] { "commit_consumed" },
        new object[] { "commit_consumed_timed" },
        new object[] { "partitions_for" },
        new object[] { "partitions_for_timed" },
        new object[] { "list_topics" },
        new object[] { "list_topics_timed" },
    };

    [Theory]
    [MemberData(nameof(WakeableCalls))]
    public void KafkaConsumer_TheConsumersOwnLeftoverWakeup_IsNotRaisedInsideACallback(string call)
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        string? outcome = null;
        try
        {
            TestTimeout.Run(
                () =>
                {
                    consumer.Assign(s_partitions);
                    consumer.CommitAsync(Empty(), new DelegateCallback(() =>
                    {
                        LeaveTheConsumersOwnWakeup(consumer);
                        outcome = Call(() => WakeableSync(consumer, call));
                    }));
                    consumer.Commit(Empty());
                },
                s_deadline);
        }
        finally
        {
            TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
            TestTimeout.Dispose(consumer);
        }

        Assert.NotNull(outcome);
        Assert.StartsWith("throws " + TimedOut, outcome, StringComparison.Ordinal);
        AssertTimedOutMessage(WakeableTimeout(call), outcome.Substring("throws ".Length + TimedOut.Length));
    }

    [Theory]
    [MemberData(nameof(Vehicles))]
    public async Task InsideACallback_ATimedCall_PassesItsTimeout(string vehicle)
    {
        bool mock = vehicle.Contains("Mock");
        string fails = vehicle.StartsWith("Async", StringComparison.Ordinal) ? "faults " : "throws ";
        Stopwatch elapsed = Stopwatch.StartNew();

        IReadOnlyList<string> outcomes = await RunInside(vehicle, (consumer, _) => ProbeTimed(consumer));

        Assert.True(elapsed.Elapsed < s_wait, "the timed calls took " + elapsed.Elapsed);
        string unsupported = fails + "NotSupportedException: " + MockHandleUnsupported;
        string timedOut = fails + TimedOut;
        string timerNegative = "throws ArgumentOutOfRangeException(timeout): Invalid negative timeout -1000";
        string coreNegative = "throws ArgumentOutOfRangeException(timeout): Timeout must not be negative";
        AssertOutcomes(
            new[]
            {
                "Commit(offsets, 200): " + (mock ? "ok" : timedOut + "Timeout of 200ms expired before successfully committing offsets"),
                "Position(200): " + (mock ? unsupported : timedOut + "Timeout of 200ms expired before the position for partition "
                    + Topic + "-0 could be determined"),
                "Committed(200): " + (mock ? unsupported : timedOut + ShortCommittedTimeout),
                "OffsetsForTimes(200): " + (mock ? unsupported : timedOut + ShortOffsetsTimeout),
                "BeginningOffsets(200): " + (mock ? unsupported : timedOut + ShortOffsetsTimeout),
                "EndOffsets(200): " + (mock ? unsupported : timedOut + ShortOffsetsTimeout),
                "Commit(offsets, -1000): " + timerNegative,
                "Position(-1000): " + timerNegative,
                "Committed(-1000): " + coreNegative,
                "OffsetsForTimes(-1000): " + coreNegative,
                "BeginningOffsets(-1000): " + coreNegative,
                "EndOffsets(-1000): " + coreNegative,
            },
            outcomes);
    }

    [Theory]
    [MemberData(nameof(Vehicles))]
    public async Task InsideACallback_AMetadataCall_GoesThroughTheHandle(string vehicle)
    {
        bool mock = vehicle.Contains("Mock");
        string fails = vehicle.StartsWith("Async", StringComparison.Ordinal) ? "faults " : "throws ";

        IReadOnlyList<string> outcomes = await RunInside(vehicle, (consumer, _) => ProbeMetadata(consumer));

        string unsupported = fails + "NotSupportedException: " + MockHandleUnsupported;
        string timedOut = fails + TimedOut;
        AssertOutcomes(
            new[]
            {
                "PartitionsFor(0): " + (mock ? unsupported
                    : timedOut + "Timeout of 0ms expired before partitions for topic " + Topic + " could be determined"),
                "PartitionsFor(200): " + (mock ? unsupported : timedOut + TopicMetadataExpired),
                "PartitionsFor(): " + (mock ? unsupported : timedOut + TopicMetadataExpired),
                "ListTopics(0): " + (mock ? unsupported
                    : timedOut + "Timeout of 0ms expired before all topics' metadata could be listed"),
                "ListTopics(200): " + (mock ? unsupported : timedOut + AllTopicsMetadataExpired),
                "ListTopics(): " + (mock ? unsupported : timedOut + AllTopicsMetadataExpired),
            },
            outcomes);
    }

    [Theory]
    [MemberData(nameof(Vehicles))]
    public async Task InsideACallback_GroupMetadata_GoesThroughTheHandle_AndAnswersAsOutside(string vehicle)
    {
        IReadOnlyList<string> outcomes = await RunInside(
            vehicle, (consumer, _) => new[] { Call(() => Format(((IConsumerCommon)consumer).GroupMetadata())) });

        Assert.Equal(new[] { "ok " + OutsideGroupMetadata(vehicle) }, outcomes);
    }

    public static IEnumerable<object[]> RefusedInsideACallback()
    {
        foreach (string vehicle in new[] { "KafkaConsumer, a commit callback", "AsyncKafkaConsumer, a commit callback" })
        {
            foreach (string call in new[] { "assign", "subscribe", "subscription", "unsubscribe", "poll", "close" })
            {
                yield return new object[] { vehicle, call };
            }
        }
    }

    [Theory]
    [MemberData(nameof(RefusedInsideACallback))]
    public async Task InsideACallback_WhatTheHandleHasNoFormOf_IsRefused(string vehicle, string call)
    {
        IReadOnlyList<string> outcomes = await RunInside(vehicle, (consumer, _) => new[] { Refusable(consumer, call) });

        string refused = "InvalidOperationException: " + Concurrent;
        Assert.Single(outcomes);
        Assert.EndsWith(refused, outcomes[0], StringComparison.Ordinal);
        Assert.True(
            outcomes[0] == "throws " + refused || outcomes[0] == "faults " + refused,
            outcomes[0]);
    }

    private static List<string> ProbeTimed(object consumer)
    {
        List<string> outcomes = new List<string>();
        foreach (TimeSpan timeout in new[] { s_short, s_minusASecond })
        {
            string at = "(" + Format((long)timeout.TotalMilliseconds) + "): ";
            if (consumer is IConsumer<byte[], byte[]> sync)
            {
                outcomes.Add("Commit(offsets, " + at.Substring(1) + Call(() => sync.Commit(Offsets(1), timeout)));
                outcomes.Add("Position" + at + Call(() => Format(sync.Position(s_tp0, timeout))));
                outcomes.Add("Committed" + at + Call(() => Format(sync.Committed(s_partitions, timeout))));
                outcomes.Add("OffsetsForTimes" + at + Call(() => Format(sync.OffsetsForTimes(Timestamps(), timeout))));
                outcomes.Add("BeginningOffsets" + at + Call(() => Format(sync.BeginningOffsets(s_partitions, timeout))));
                outcomes.Add("EndOffsets" + at + Call(() => Format(sync.EndOffsets(s_partitions, timeout))));
            }
            else
            {
                IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)consumer;
                outcomes.Add("Commit(offsets, " + at.Substring(1) + CallAsync(() => async.Commit(Offsets(1), timeout)));
                outcomes.Add("Position" + at + CallAsync(() => async.Position(s_tp0, timeout), Format));
                outcomes.Add("Committed" + at + CallAsync(() => async.Committed(s_partitions, timeout), Format));
                outcomes.Add("OffsetsForTimes" + at + CallAsync(() => async.OffsetsForTimes(Timestamps(), timeout), Format));
                outcomes.Add("BeginningOffsets" + at + CallAsync(() => async.BeginningOffsets(s_partitions, timeout), Format));
                outcomes.Add("EndOffsets" + at + CallAsync(() => async.EndOffsets(s_partitions, timeout), Format));
            }
        }

        return outcomes;
    }

    private static List<string> ProbeMetadata(object consumer)
    {
        List<string> outcomes = new List<string>();
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            outcomes.Add("PartitionsFor(0): " + Call(() => sync.PartitionsFor(Topic, TimeSpan.Zero)));
            outcomes.Add("PartitionsFor(200): " + Call(() => sync.PartitionsFor(Topic, s_short)));
            outcomes.Add("PartitionsFor(): " + Call(() => sync.PartitionsFor(Topic)));
            outcomes.Add("ListTopics(0): " + Call(() => sync.ListTopics(TimeSpan.Zero)));
            outcomes.Add("ListTopics(200): " + Call(() => sync.ListTopics(s_short)));
            outcomes.Add("ListTopics(): " + Call(() => sync.ListTopics()));
        }
        else
        {
            IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)consumer;
            outcomes.Add("PartitionsFor(0): " + CallAsync(() => async.PartitionsFor(Topic, TimeSpan.Zero)));
            outcomes.Add("PartitionsFor(200): " + CallAsync(() => async.PartitionsFor(Topic, s_short)));
            outcomes.Add("PartitionsFor(): " + CallAsync(() => async.PartitionsFor(Topic)));
            outcomes.Add("ListTopics(0): " + CallAsync(() => async.ListTopics(TimeSpan.Zero)));
            outcomes.Add("ListTopics(200): " + CallAsync(() => async.ListTopics(s_short)));
            outcomes.Add("ListTopics(): " + CallAsync(() => async.ListTopics()));
        }

        return outcomes;
    }

    private static string Refusable(object consumer, string call)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            return call switch
            {
                "assign" => Call(() => sync.Assign(s_partitions)),
                "subscribe" => Call(() => sync.Subscribe(s_topics)),
                "subscription" => Call(() => Format(sync.Subscription())),
                "unsubscribe" => Call(sync.Unsubscribe),
                "poll" => Call(() => sync.Poll(TimeSpan.Zero)),
                _ => Call(() => sync.Close(CloseOptions.Timeout(TimeSpan.Zero))),
            };
        }

        IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)consumer;
        return call switch
        {
            "assign" => CallAsync(() => async.Assign(s_partitions)),
            "subscribe" => CallAsync(() => async.Subscribe(s_topics)),
            "subscription" => Call(() => Format(async.Subscription())),
            "unsubscribe" => CallAsync(() => async.Unsubscribe()),
            "poll" => CallAsync(() => async.Poll(TimeSpan.Zero)),
            _ => CallAsync(() => async.Close(CloseOptions.Timeout(TimeSpan.Zero))),
        };
    }

    private static string OutsideGroupMetadata(string vehicle)
    {
        bool mock = vehicle.Contains("Mock");
        bool async = vehicle.StartsWith("Async", StringComparison.Ordinal);
        IConsumerCommon consumer = (mock, async) switch
        {
            (true, false) => NewMock(),
            (true, true) => NewAsyncMock(),
            (false, false) => NewKafkaConsumer(),
            _ => NewAsyncKafkaConsumer(),
        };
        using (TestTimeout.Disposal((IDisposable)consumer))
        {
            return Format(consumer.GroupMetadata());
        }
    }

    // What the consumer's own wakeup leaves when the call it woke ended first: the wakeup armed in the core, marked as
    // the consumer's own.
    private static void LeaveTheConsumersOwnWakeup(object consumer)
    {
        const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
        NativeConsumer native = (NativeConsumer)consumer.GetType().GetField("_native", Private)!.GetValue(consumer)!;
        typeof(NativeConsumer).GetField("_ownWakeLeft", Private)!.SetValue(native, true);
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerWakeup(native.Handle, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
    }

    private static void WakeableSync(IConsumer<byte[], byte[]> consumer, string call)
    {
        switch (call)
        {
            case "committed":
                _ = consumer.Committed(s_partitions);
                break;
            case "committed_timed":
                _ = consumer.Committed(s_partitions, s_wakeableTimeout);
                break;
            case "position":
                _ = consumer.Position(s_tp0);
                break;
            case "position_timed":
                _ = consumer.Position(s_tp0, s_wakeableTimeout);
                break;
            case "commit":
                consumer.Commit(Offsets(1));
                break;
            case "commit_timed":
                consumer.Commit(Offsets(1), s_wakeableTimeout);
                break;
            case "commit_consumed":
                consumer.Seek(s_tp0, 1);
                consumer.Commit();
                break;
            case "commit_consumed_timed":
                consumer.Seek(s_tp0, 1);
                consumer.Commit(s_wakeableTimeout);
                break;
            case "partitions_for":
                _ = consumer.PartitionsFor("other");
                break;
            case "partitions_for_timed":
                _ = consumer.PartitionsFor("other", s_wakeableTimeout);
                break;
            case "list_topics":
                _ = consumer.ListTopics();
                break;
            default:
                _ = consumer.ListTopics(s_wakeableTimeout);
                break;
        }
    }

    private static List<string> Expected(bool mock, bool async)
    {
        string fails = async ? "faults " : "throws ";
        string unsupported = fails + "NotSupportedException: " + MockHandleUnsupported;
        string refused = "throws InvalidOperationException: " + Concurrent;
        List<string> expected = new List<string>
        {
            "Commit(): ok",
            "Commit(offsets): ok",
            "CommitAsync(): ok",
            "CommitAsync(offsets): ok",
            "Assignment: ok [" + Topic + "-0]",
            "Paused: ok []",
            "CurrentLag: ok " + (mock ? "0" : "null"),
            "Seek: " + (mock ? unsupported : "ok"),
            "Position: " + (mock ? unsupported : "ok 5"),
            "Seek(OffsetAndMetadata): " + (mock ? unsupported : "ok"),
            "Position(timeout): " + (mock ? unsupported : "ok 7"),
            "Pause: " + (mock ? unsupported : "ok"),
            "Resume: " + (mock ? unsupported : "ok"),
            "Committed: " + (mock ? unsupported : fails + "KafkaException 7: " + CommittedTimeout),
            "BeginningOffsets: " + (mock ? unsupported : fails + "KafkaException 7: " + OffsetsTimeout),
            "EndOffsets: " + (mock ? unsupported : fails + "KafkaException 7: " + OffsetsTimeout),
            "OffsetsForTimes: " + (mock ? unsupported : fails + "KafkaException 7: " + OffsetsTimeout),
            "SeekToBeginning: " + (mock ? unsupported : "ok"),
            "SeekToEnd: " + (mock ? unsupported : "ok"),
            "Poll: " + refused,
            "Poll(negative): throws ArgumentOutOfRangeException(timeout): Invalid negative timeout -1",
            "Subscribe: " + refused,
            "Subscribe(listener): " + refused,
            "Unsubscribe: " + refused,
            "Assign: " + refused,
            "Commit(timeout): ok",
            "Commit(negative timeout): throws ArgumentOutOfRangeException(timeout): Invalid negative timeout -1",
            "Commit(offsets, timeout): ok",
            "Committed(timeout): " + (mock ? unsupported : fails + TimedOut + ShortCommittedTimeout),
            "BeginningOffsets(timeout): " + (mock ? unsupported : fails + TimedOut + ShortOffsetsTimeout),
            "EndOffsets(timeout): " + (mock ? unsupported : fails + TimedOut + ShortOffsetsTimeout),
            "OffsetsForTimes(timeout): " + (mock ? unsupported : fails + TimedOut + ShortOffsetsTimeout),
            "PartitionsFor: " + (mock ? unsupported : fails + TimedOut + TopicMetadataExpired),
            "PartitionsFor(timeout): " + (mock ? unsupported : fails + TimedOut + TopicMetadataExpired),
            "ListTopics: " + (mock ? unsupported : fails + TimedOut + AllTopicsMetadataExpired),
            "ListTopics(timeout): " + (mock ? unsupported : fails + TimedOut + AllTopicsMetadataExpired),
            "Subscription: " + refused,
            "GroupMetadata: ok " + (mock ? "dummy.group.id" : "callback-routing-group"),
            "EnforceRebalance: " + refused,
            "Metrics: ok a map",
        };
        return expected;
    }

    // A timed-out call may end with the core reaper's message instead of its own, and a metadata lookup with either of
    // its two own messages.
    private static void AssertOutcomes(IReadOnlyList<string> expected, IReadOnlyList<string> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            int at = expected[i].IndexOf(TimedOut, StringComparison.Ordinal);
            if (at < 0)
            {
                Assert.Equal(expected[i], actual[i]);
                continue;
            }

            int start = at + TimedOut.Length;
            Assert.Equal(expected[i].Substring(0, start), actual[i].Substring(0, Math.Min(start, actual[i].Length)));
            AssertTimedOutMessage(expected[i].Substring(start), actual[i].Substring(start));
        }
    }

    private static void AssertTimedOutMessage(string own, string actual)
    {
        string[] messages = own.Split('|');
        if (Array.IndexOf(messages, actual) < 0)
        {
            TimedOutMessage.AssertCallerOrReaper(messages[0], actual);
        }
    }

    private static List<string> Probe(object consumer, bool mock) => consumer is IConsumer<byte[], byte[]> sync
        ? ProbeSync(sync, mock)
        : ProbeAsync((IAsyncConsumer<byte[], byte[]>)consumer, mock);

    private static List<string> ProbeSync(IConsumer<byte[], byte[]> consumer, bool mock)
    {
        List<string> outcomes = new List<string>();
        void Add(string name, string outcome) => outcomes.Add(name + ": " + outcome);

        Add("Commit()", Call(() => consumer.Commit()));
        Add("Commit(offsets)", Call(() => consumer.Commit(mock ? Offsets(5) : Empty())));
        Add("CommitAsync()", Call(() => consumer.CommitAsync()));
        Add("CommitAsync(offsets)", Call(() => consumer.CommitAsync(mock ? Offsets(6) : Empty())));
        Add("Assignment", Call(() => Format(consumer.Assignment())));
        Add("Paused", Call(() => Format(consumer.Paused())));
        Add("CurrentLag", Call(() => Format(consumer.CurrentLag(s_tp0))));
        Add("Seek", Call(() => consumer.Seek(s_tp0, 5)));
        Add("Position", Call(() => Format(consumer.Position(s_tp0))));
        Add("Seek(OffsetAndMetadata)", Call(() => consumer.Seek(s_tp0, new OffsetAndMetadata(7))));
        Add("Position(timeout)", Call(() => Format(consumer.Position(s_tp0, s_second))));
        Add("Pause", Call(() => consumer.Pause(s_partitions)));
        Add("Resume", Call(() => consumer.Resume(s_partitions)));
        Add("Committed", Call(() => Format(consumer.Committed(s_partitions))));
        Add("BeginningOffsets", Call(() => Format(consumer.BeginningOffsets(s_partitions))));
        Add("EndOffsets", Call(() => Format(consumer.EndOffsets(s_partitions))));
        Add("OffsetsForTimes", Call(() => Format(consumer.OffsetsForTimes(Timestamps()))));
        Add("SeekToBeginning", Call(() => consumer.SeekToBeginning(s_partitions)));
        Add("SeekToEnd", Call(() => consumer.SeekToEnd(s_partitions)));
        Add("Poll", Call(() => consumer.Poll(TimeSpan.Zero)));
        Add("Poll(negative)", Call(() => consumer.Poll(s_negative)));
        Add("Subscribe", Call(() => consumer.Subscribe(s_topics)));
        Add("Subscribe(listener)", Call(() => consumer.Subscribe(s_topics, new SyncListener(() => { }))));
        Add("Unsubscribe", Call(consumer.Unsubscribe));
        Add("Assign", Call(() => consumer.Assign(s_partitions)));
        Add("Commit(timeout)", Call(() => consumer.Commit(s_second)));
        Add("Commit(negative timeout)", Call(() => consumer.Commit(s_negative)));
        Add("Commit(offsets, timeout)", Call(() => consumer.Commit(Empty(), s_second)));
        Add("Committed(timeout)", Call(() => Format(consumer.Committed(s_partitions, s_short))));
        Add("BeginningOffsets(timeout)", Call(() => Format(consumer.BeginningOffsets(s_partitions, s_short))));
        Add("EndOffsets(timeout)", Call(() => Format(consumer.EndOffsets(s_partitions, s_short))));
        Add("OffsetsForTimes(timeout)", Call(() => Format(consumer.OffsetsForTimes(Timestamps(), s_short))));
        Add("PartitionsFor", Call(() => consumer.PartitionsFor(Topic)));
        Add("PartitionsFor(timeout)", Call(() => consumer.PartitionsFor(Topic, s_short)));
        Add("ListTopics", Call(() => consumer.ListTopics()));
        Add("ListTopics(timeout)", Call(() => consumer.ListTopics(s_short)));
        Add("Subscription", Call(() => Format(consumer.Subscription())));
        Add("GroupMetadata", Call(() => consumer.GroupMetadata().GroupId));
        Add("EnforceRebalance", Call(() => consumer.EnforceRebalance()));
        Add("Metrics", Call(() => consumer.Metrics() is null ? "null" : "a map"));
        return outcomes;
    }

    private static List<string> ProbeAsync(IAsyncConsumer<byte[], byte[]> consumer, bool mock)
    {
        List<string> outcomes = new List<string>();
        void Add(string name, string outcome) => outcomes.Add(name + ": " + outcome);

        Add("Commit()", CallAsync(() => consumer.Commit()));
        Add("Commit(offsets)", CallAsync(() => consumer.Commit(mock ? Offsets(5) : Empty())));
        Add("CommitAsync()", CallAsync(() => consumer.CommitAsync()));
        Add("CommitAsync(offsets)", CallAsync(() => consumer.CommitAsync(mock ? Offsets(6) : Empty())));
        Add("Assignment", Call(() => Format(consumer.Assignment())));
        Add("Paused", Call(() => Format(consumer.Paused())));
        Add("CurrentLag", CallAsync(() => consumer.CurrentLag(s_tp0), Format));
        Add("Seek", CallAsync(() => consumer.Seek(s_tp0, 5)));
        Add("Position", CallAsync(() => consumer.Position(s_tp0), Format));
        Add("Seek(OffsetAndMetadata)", CallAsync(() => consumer.Seek(s_tp0, new OffsetAndMetadata(7))));
        Add("Position(timeout)", CallAsync(() => consumer.Position(s_tp0, s_second), Format));
        Add("Pause", CallAsync(() => consumer.Pause(s_partitions)));
        Add("Resume", CallAsync(() => consumer.Resume(s_partitions)));
        Add("Committed", CallAsync(() => consumer.Committed(s_partitions), Format));
        Add("BeginningOffsets", CallAsync(() => consumer.BeginningOffsets(s_partitions), Format));
        Add("EndOffsets", CallAsync(() => consumer.EndOffsets(s_partitions), Format));
        Add("OffsetsForTimes", CallAsync(() => consumer.OffsetsForTimes(Timestamps()), Format));
        Add("SeekToBeginning", CallAsync(() => consumer.SeekToBeginning(s_partitions)));
        Add("SeekToEnd", CallAsync(() => consumer.SeekToEnd(s_partitions)));
        Add("Poll", CallAsync(() => consumer.Poll(TimeSpan.Zero)));
        Add("Poll(negative)", CallAsync(() => consumer.Poll(s_negative)));
        Add("Subscribe", CallAsync(() => consumer.Subscribe(s_topics)));
        Add("Subscribe(listener)", CallAsync(() => consumer.Subscribe(s_topics, new SyncListener(() => { }))));
        Add("Unsubscribe", CallAsync(() => consumer.Unsubscribe()));
        Add("Assign", CallAsync(() => consumer.Assign(s_partitions)));
        Add("Commit(timeout)", CallAsync(() => consumer.Commit(s_second)));
        Add("Commit(negative timeout)", CallAsync(() => consumer.Commit(s_negative)));
        Add("Commit(offsets, timeout)", CallAsync(() => consumer.Commit(Empty(), s_second)));
        Add("Committed(timeout)", CallAsync(() => consumer.Committed(s_partitions, s_short), Format));
        Add("BeginningOffsets(timeout)", CallAsync(() => consumer.BeginningOffsets(s_partitions, s_short), Format));
        Add("EndOffsets(timeout)", CallAsync(() => consumer.EndOffsets(s_partitions, s_short), Format));
        Add("OffsetsForTimes(timeout)", CallAsync(() => consumer.OffsetsForTimes(Timestamps(), s_short), Format));
        Add("PartitionsFor", CallAsync(() => consumer.PartitionsFor(Topic)));
        Add("PartitionsFor(timeout)", CallAsync(() => consumer.PartitionsFor(Topic, s_short)));
        Add("ListTopics", CallAsync(() => consumer.ListTopics()));
        Add("ListTopics(timeout)", CallAsync(() => consumer.ListTopics(s_short)));
        Add("Subscription", Call(() => Format(consumer.Subscription())));
        Add("GroupMetadata", Call(() => consumer.GroupMetadata().GroupId));
        Add("EnforceRebalance", Call(() => consumer.EnforceRebalance()));
        Add("Metrics", Call(() => consumer.Metrics() is null ? "null" : "a map"));
        return outcomes;
    }

    private static string Commit(object consumer) => consumer is IConsumer<byte[], byte[]> sync
        ? Call(sync.Commit)
        : CallAsync(() => ((IAsyncConsumer<byte[], byte[]>)consumer).Commit());

    private static Task Wakeable(IAsyncConsumer<byte[], byte[]> consumer, string call)
    {
        switch (call)
        {
            case "committed":
                return consumer.Committed(s_partitions);
            case "committed_timed":
                return consumer.Committed(s_partitions, s_wakeableTimeout);
            case "position":
                return consumer.Position(s_tp0);
            case "position_timed":
                return consumer.Position(s_tp0, s_wakeableTimeout);
            case "commit":
                return consumer.Commit(Offsets(1));
            case "commit_timed":
                return consumer.Commit(Offsets(1), s_wakeableTimeout);
            case "partitions_for":
                return consumer.PartitionsFor("other");
            case "partitions_for_timed":
                return consumer.PartitionsFor("other", s_wakeableTimeout);
            case "list_topics":
                return consumer.ListTopics();
            case "list_topics_timed":
                return consumer.ListTopics(s_wakeableTimeout);
            default:
                Task seek = consumer.Seek(s_tp0, 1);
                if (!seek.IsCompleted || seek.Status != TaskStatus.RanToCompletion)
                {
                    return seek;
                }

                return call == "commit_consumed_timed" ? consumer.Commit(s_wakeableTimeout) : consumer.Commit();
        }
    }

    private static string WakeableTimeout(string call)
    {
        bool timed = call.EndsWith("_timed", StringComparison.Ordinal);
        string ms = timed ? "300ms" : "500ms";
        if (call.StartsWith("committed", StringComparison.Ordinal))
        {
            return timed ? CommittedTimeout.Replace("500ms", ms) : CommittedTimeout;
        }

        if (call.StartsWith("position", StringComparison.Ordinal))
        {
            return "Timeout of " + ms + " expired before the position for partition " + Topic + "-0 could be determined";
        }

        if (call.StartsWith("partitions_for", StringComparison.Ordinal))
        {
            return TopicMetadataExpired;
        }

        if (call.StartsWith("list_topics", StringComparison.Ordinal))
        {
            return AllTopicsMetadataExpired;
        }

        return "Timeout of " + ms + " expired before successfully committing offsets";
    }

    private static async Task<IReadOnlyList<string>> RunInside(string vehicle, Func<object, bool, IReadOnlyList<string>> probe)
    {
        bool mock = vehicle.Contains("Mock");
        TaskCompletionSource<IReadOnlyList<string>> probed =
            new TaskCompletionSource<IReadOnlyList<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        void Run(object consumer)
        {
            try
            {
                probed.TrySetResult(probe(consumer, mock));
            }
            catch (Exception e)
            {
                probed.TrySetException(e);
            }
        }

        switch (vehicle)
        {
            case "KafkaConsumer, a commit callback":
                {
                    KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
                    using (TestTimeout.Disposal(consumer))
                    {
                        TestTimeout.Run(
                            () =>
                            {
                                consumer.Assign(s_partitions);
                                consumer.CommitAsync(Empty(), new DelegateCallback(() => Run(consumer)));
                                consumer.Commit(Empty());
                                consumer.Close(CloseOptions.Timeout(TimeSpan.Zero));
                            },
                            s_deadline);
                    }
                }

                break;
            case "MockConsumer, a listener":
                {
                    MockConsumer<byte[], byte[]> consumer = NewMock();
                    using (TestTimeout.Disposal(consumer))
                    {
                        consumer.Subscribe(s_topics, new SyncListener(() => Run(consumer)));
                        TestTimeout.Run(() => consumer.Rebalance(s_partitions), s_deadline);
                    }
                }

                break;
            case "MockConsumer, a commit callback":
                {
                    MockConsumer<byte[], byte[]> consumer = NewMock();
                    using (TestTimeout.Disposal(consumer))
                    {
                        consumer.Assign(s_partitions);
                        TestTimeout.Run(
                            () => consumer.CommitAsync(Offsets(1), new DelegateCallback(() => Run(consumer))),
                            s_deadline);
                    }
                }

                break;
            case "AsyncKafkaConsumer, a commit callback":
            case "AsyncKafkaConsumer, an async commit callback after an await":
                {
                    AsyncKafkaConsumer<byte[], byte[]> consumer = NewAsyncKafkaConsumer();
                    try
                    {
                        await TestTimeout.Run(() => consumer.Assign(s_partitions), s_deadline);
                        await TestTimeout.Run(
                            () => vehicle.Contains("after an await")
                                ? consumer.CommitAsync(Empty(), AfterAnAwait(() => Run(consumer)))
                                : consumer.CommitAsync(Empty(), new DelegateCallback(() => Run(consumer))),
                            s_deadline);
                        await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
                    }
                    finally
                    {
                        await CloseAndDispose(consumer);
                    }
                }

                break;
            default:
                {
                    AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
                    try
                    {
                        if (vehicle.Contains("listener"))
                        {
                            await TestTimeout.Run(
                                () => vehicle.Contains("after an await")
                                    ? consumer.Subscribe(s_topics, new AsyncListener(async () =>
                                    {
                                        await Task.Yield();
                                        Run(consumer);
                                    }))
                                    : consumer.Subscribe(s_topics, new SyncListener(() => Run(consumer))),
                                s_deadline);
                            await TestTimeout.Run(() => consumer.Rebalance(s_partitions), s_deadline);
                        }
                        else
                        {
                            await TestTimeout.Run(() => consumer.Assign(s_partitions), s_deadline);
                            await TestTimeout.Run(
                                () => vehicle.Contains("after an await")
                                    ? consumer.CommitAsync(Offsets(1), AfterAnAwait(() => Run(consumer)))
                                    : consumer.CommitAsync(Offsets(1), new DelegateCallback(() => Run(consumer))),
                                s_deadline);
                        }
                    }
                    finally
                    {
                        await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
                    }
                }

                break;
        }

        Assert.True(probed.Task.IsCompleted, "the callback never ran inside the delivering call");
        return await probed.Task;
    }

    private static async Task CancelACommitAwaitingTheAsyncCallbackItDelivered(AsyncKafkaConsumer<byte[], byte[]> consumer)
    {
        TaskCompletionSource<bool> started = NewSignal();
        TaskCompletionSource<bool> release = NewSignal();
        await TestTimeout.Run(
            () => consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async () =>
            {
                started.TrySetResult(true);
                await release.Task;
            })),
            s_deadline);

        using CancellationTokenSource cancel = new CancellationTokenSource();
        Task running = consumer.Commit(Empty(), cancel.Token);
        await TestTimeout.Run(() => started.Task, s_deadline);
        cancel.Cancel();
        release.TrySetResult(true);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => running, s_deadline));
    }

    private static string Call(Action call)
    {
        try
        {
            call();
            return "ok";
        }
        catch (Exception e)
        {
            return "throws " + Describe(e);
        }
    }

    private static string Call(Func<object?> call)
    {
        try
        {
            return call() is string value ? "ok " + value : "ok";
        }
        catch (Exception e)
        {
            return "throws " + Describe(e);
        }
    }

    private static string CallAsync(Func<Task> call) => CallAsync(call, _ => null);

    private static string CallAsync<T>(Func<Task<T>> call, Func<T, string?> format) =>
        CallAsync(call, task => format(((Task<T>)task).GetAwaiter().GetResult()));

    private static string CallAsync(Func<Task> call, Func<Task, string?> format)
    {
        Task task;
        try
        {
            task = call();
        }
        catch (Exception e)
        {
            return "throws " + Describe(e);
        }

        if (!task.IsCompleted)
        {
            _ = task.ContinueWith(
                ended => _ = ended.Exception,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return "pending";
        }

        if (task.IsCanceled)
        {
            return "cancelled";
        }

        if (task.IsFaulted)
        {
            return "faults " + Describe(task.Exception!.InnerException!);
        }

        string? value = format(task);
        return value is null ? "ok" : "ok " + value;
    }

    private static string Describe(Exception e)
    {
        if (e is KafkaException kafka)
        {
            return "KafkaException " + kafka.Code.ToString(CultureInfo.InvariantCulture) + ": " + kafka.Message;
        }

        if (e is ArgumentException argument && argument.ParamName is not null)
        {
            string message = argument.Message;
            int parameter = message.IndexOf(" (Parameter", StringComparison.Ordinal);
            return e.GetType().Name + "(" + argument.ParamName + "): " + (parameter < 0 ? message : message.Substring(0, parameter));
        }

        return e.GetType().Name + ": " + e.Message;
    }

    private static string Format(IReadOnlyCollection<TopicPartition> partitions) =>
        "[" + string.Join(",", partitions.Select(p => p.Topic + "-" + p.Partition.ToString(CultureInfo.InvariantCulture))) + "]";

    private static string Format(IReadOnlyCollection<string> topics) => "[" + string.Join(",", topics) + "]";

    private static string Format(long? value) => value?.ToString(CultureInfo.InvariantCulture) ?? "null";

    private static string Format(long value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Format(ConsumerGroupMetadata metadata) =>
        metadata.GroupId + "/" + metadata.GenerationId.ToString(CultureInfo.InvariantCulture) + "/" + metadata.MemberId
        + "/" + (metadata.GroupInstanceId ?? "null");

    private static string Format<TValue>(IReadOnlyDictionary<TopicPartition, TValue> values) =>
        "{" + string.Join(",", values.Select(entry => entry.Key.Topic + "-" + entry.Key.Partition.ToString(CultureInfo.InvariantCulture)
            + "=" + (entry.Value?.ToString() ?? "null"))) + "}";

    private static Func<(bool BeforeTheRelease, bool AfterTheRelease)?> WatchTheRoutingHandle(object consumer)
    {
        const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;
        NativeConsumer native = (NativeConsumer)consumer.GetType().GetField("_native", Private)!.GetValue(consumer)!;
        object routing = typeof(NativeConsumer).GetField("_callbackHandle", Private)!.GetValue(native)
            ?? throw new InvalidOperationException("no call inside the callback made the routing handle");
        SafeHandle handle = (SafeHandle)typeof(NativeConsumerHandle).GetField("_handle", Private)!.GetValue(routing)!;
        Action? afterDestroy = (Action?)typeof(SafeConsumerHandle).GetField("_afterDestroy", Private)!.GetValue(native.Handle);
        (bool, bool)? seen = null;
        native.Handle.ReleaseAfterDestroy(() =>
        {
            bool before = handle.IsClosed;
            afterDestroy?.Invoke();
            seen = (before, handle.IsClosed);
        });
        return () => seen;
    }

    private static void SpinUntilClosing(IConsumerCommon consumer)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                consumer.Assignment();
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (InvalidOperationException)
            {
            }

            if (elapsed.Elapsed > s_deadline)
            {
                throw new TimeoutException("the consumer never began to close");
            }

            Thread.Yield();
        }
    }

    private static async Task CloseAndDispose(IAsyncConsumer<byte[], byte[]> consumer)
    {
        try
        {
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    private static TaskCompletionSource<bool> NewSignal() =>
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private static IAsyncOffsetCommitCallback AfterAnAwait(Action action) => new AsyncDelegateCallback(async () =>
    {
        await Task.Yield();
        action();
    });

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static Dictionary<TopicPartition, long> Timestamps() => new Dictionary<TopicPartition, long> { [s_tp0] = 0 };

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static Dictionary<string, string> Config() => new Dictionary<string, string>
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = "callback-routing-group",
        ["enable.auto.commit"] = "false",
        ["default.api.timeout.ms"] = "500",
    };

    private static KafkaConsumer<byte[], byte[]> NewKafkaConsumer() =>
        new KafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncKafkaConsumer<byte[], byte[]> NewAsyncKafkaConsumer() =>
        new AsyncKafkaConsumer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);

    /// <summary>A listener that runs the action it is given when partitions are assigned.</summary>
    private sealed class SyncListener : ConsumerRebalanceListenerBase
    {
        private readonly Action _onAssigned;

        internal SyncListener(Action onAssigned) => _onAssigned = onAssigned;

        public override void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public override void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();
    }

    /// <summary>An async listener that runs the function it is given when partitions are assigned.</summary>
    private sealed class AsyncListener : AsyncConsumerRebalanceListenerBase
    {
        private readonly Func<Task> _onAssigned;

        internal AsyncListener(Func<Task> onAssigned) => _onAssigned = onAssigned;

        public override Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public override Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();
    }

    /// <summary>A commit callback that runs the action it is given.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action _onComplete;

        internal DelegateCallback(Action onComplete) => _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }

    /// <summary>An async commit callback that runs the function it is given.</summary>
    private sealed class AsyncDelegateCallback : IAsyncOffsetCommitCallback
    {
        private readonly Func<Task> _onComplete;

        internal AsyncDelegateCallback(Func<Task> onComplete) => _onComplete = onComplete;

        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }
}
