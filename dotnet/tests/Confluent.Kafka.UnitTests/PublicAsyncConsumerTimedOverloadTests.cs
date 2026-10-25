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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>The async consumer's <see cref="TimeSpan"/> overloads: the timeout check, and the bound each one applies.</summary>
public sealed class PublicAsyncConsumerTimedOverloadTests
{
    private const string Topic = "test";

    private const int RequestTimedOutCode = 7;

    private const string CoreNegative = "Timeout must not be negative";

    private const string TimerNegative = "Invalid negative timeout {0}";

    private const string MetadataExpiredByManager = "Timeout expired while fetching topic metadata";

    private const string CommittedTimeout =
        "Timeout of {0}ms expired before the last committed offset for partitions [test-0] could be determined. "
        + "Try tuning default.api.timeout.ms larger to relax the threshold.";

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly Dictionary<string, (Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task> Call, string Message)> s_negative =
        new Dictionary<string, (Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task>, string)>
        {
            ["Commit"] = ((c, t) => c.Commit(t), TimerNegative),
            ["CommitOffsets"] = ((c, t) => c.Commit(Offsets(1), t), TimerNegative),
            ["Position"] = ((c, t) => c.Position(s_tp0, t), TimerNegative),
            ["Committed"] = ((c, t) => c.Committed(new[] { s_tp0 }, t), CoreNegative),
            ["OffsetsForTimes"] = ((c, t) => c.OffsetsForTimes(Timestamps(0), t), CoreNegative),
            ["BeginningOffsets"] = ((c, t) => c.BeginningOffsets(new[] { s_tp0 }, t), CoreNegative),
            ["EndOffsets"] = ((c, t) => c.EndOffsets(new[] { s_tp0 }, t), CoreNegative),
            ["PartitionsFor"] = ((c, t) => c.PartitionsFor(Topic, t), CoreNegative),
            ["ListTopics"] = ((c, t) => c.ListTopics(t), CoreNegative),
        };

    private static readonly Dictionary<string, (Func<IAsyncConsumer<byte[], byte[]>, Task> Untimed, Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task> Timed, string Message)> s_timed =
        new Dictionary<string, (Func<IAsyncConsumer<byte[], byte[]>, Task>, Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task>, string)>
        {
            ["Commit"] = (
                c => c.Commit(Offsets(1)),
                (c, t) => c.Commit(Offsets(1), t),
                "Timeout of {0}ms expired before successfully committing offsets"),
            ["Position"] = (
                c => c.Position(s_tp0),
                (c, t) => c.Position(s_tp0, t),
                "Timeout of {0}ms expired before the position for partition test-0 could be determined"),
            ["Committed"] = (
                c => c.Committed(new[] { s_tp0 }),
                (c, t) => c.Committed(new[] { s_tp0 }, t),
                CommittedTimeout),
            ["OffsetsForTimes"] = (
                c => c.OffsetsForTimes(Timestamps(0)),
                (c, t) => c.OffsetsForTimes(Timestamps(0), t),
                "Failed to get offsets by times in {0}ms"),
            ["BeginningOffsets"] = (
                c => c.BeginningOffsets(new[] { s_tp0 }),
                (c, t) => c.BeginningOffsets(new[] { s_tp0 }, t),
                "Failed to get offsets by times in {0}ms"),
            ["EndOffsets"] = (
                c => c.EndOffsets(new[] { s_tp0 }),
                (c, t) => c.EndOffsets(new[] { s_tp0 }, t),
                "Failed to get offsets by times in {0}ms"),
        };

    private static readonly Dictionary<string, (Func<IAsyncConsumer<byte[], byte[]>, Task> Untimed, Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task> Timed, string Zero, string[] Expired)> s_metadata =
        new Dictionary<string, (Func<IAsyncConsumer<byte[], byte[]>, Task>, Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task>, string, string[])>
        {
            ["PartitionsFor"] = (
                c => c.PartitionsFor(Topic),
                (c, t) => c.PartitionsFor(Topic, t),
                "Timeout of 0ms expired before partitions for topic test could be determined",
                new[] { MetadataExpiredByManager, "Timeout expired while waiting for TopicMetadata" }),
            ["ListTopics"] = (
                c => c.ListTopics(),
                (c, t) => c.ListTopics(t),
                "Timeout of 0ms expired before all topics' metadata could be listed",
                new[] { MetadataExpiredByManager, "Timeout expired while waiting for AllTopicsMetadata" }),
        };

    public static IEnumerable<object[]> NegativeMethods()
    {
        foreach (string name in s_negative.Keys)
        {
            yield return new object[] { name };
        }
    }

    public static IEnumerable<object[]> TimedMethods()
    {
        foreach (string name in s_timed.Keys)
        {
            yield return new object[] { name };
        }
    }

    public static IEnumerable<object[]> MetadataMethods()
    {
        foreach (string name in s_metadata.Keys)
        {
            yield return new object[] { name };
        }
    }

    [Theory]
    [MemberData(nameof(NegativeMethods))]
    public async Task ATimedOverload_RejectsANegativeTimeout_AndLeavesTheConsumerAsItWas(string method)
    {
        (Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task> call, string message) = s_negative[method];
        foreach (bool mock in new[] { true, false })
        {
            IAsyncConsumer<byte[], byte[]> consumer = Create(mock);
            try
            {
                await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);

                AssertNegative(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = call(consumer, TimeSpan.FromSeconds(-1)); }), message, -1000);
                AssertNegative(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = call(consumer, TimeSpan.FromTicks(-5000)); }), message, -1);
                Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
            }
            finally
            {
                await TestTimeout.DisposeAsync(consumer);
            }
        }
    }

    [Theory]
    [MemberData(nameof(NegativeMethods))]
    public async Task ATimedOverload_ChecksTheClosedStateFirst(string method)
    {
        Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task> call = s_negative[method].Call;
        foreach (bool mock in new[] { true, false })
        {
            IAsyncConsumer<byte[], byte[]> consumer = Create(mock);
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);

            foreach (TimeSpan timeout in new[] { TimeSpan.FromSeconds(-1), TimeSpan.FromTicks(-1), TimeSpan.MinValue })
            {
                Assert.Throws<ObjectDisposedException>(() => { _ = call(consumer, timeout); });
            }
        }
    }

    [Fact]
    public async Task Poll_NegativeTimeout_ThrowsArgumentOutOfRange()
    {
        foreach (bool mock in new[] { true, false })
        {
            IAsyncConsumer<byte[], byte[]> consumer = Create(mock);
            try
            {
                AssertNegative(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = consumer.Poll(TimeSpan.FromMilliseconds(-1)); }), TimerNegative, -1);
                AssertNegative(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = consumer.Poll(TimeSpan.FromTicks(-5000)); }), TimerNegative, -1);
            }
            finally
            {
                await TestTimeout.DisposeAsync(consumer);
            }
        }
    }

    [Fact]
    public async Task Poll_NegativeTimeout_ThrownBeforeTheClosedCheck_EvenWhenClosed()
    {
        foreach (bool mock in new[] { true, false })
        {
            IAsyncConsumer<byte[], byte[]> consumer = Create(mock);
            await TestTimeout.DisposeAsync(consumer);

            AssertNegative(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = consumer.Poll(TimeSpan.FromSeconds(-2)); }), TimerNegative, -2000);
            Assert.Throws<ObjectDisposedException>(() => { _ = consumer.Poll(TimeSpan.Zero); });
        }
    }

    [Theory]
    [MemberData(nameof(TimedMethods))]
    public async Task ATimedOverload_WaitsUpToItsTimeout_AndTheUntimedOneUpToTheDefault(string method)
    {
        (Func<IAsyncConsumer<byte[], byte[]>, Task> untimed, Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task> timed, string message) = s_timed[method];
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 300);
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            Stopwatch elapsed = Stopwatch.StartNew();

            AssertTimedOut(await Assert.ThrowsAsync<KafkaException>(() => untimed(consumer)), message, 300);
            AssertTimedOut(await Assert.ThrowsAsync<KafkaException>(() => timed(consumer, TimeSpan.FromMilliseconds(200))), message, 200);
            AssertTimedOut(await Assert.ThrowsAsync<KafkaException>(() => timed(consumer, TimeSpan.FromTicks(1_509_000))), message, 150);

            Assert.True(elapsed.Elapsed < s_wait, $"the timed calls took {elapsed.Elapsed}");
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task ATimedCommitOfTheConsumedPositions_WaitsUpToItsTimeout_AndTheUntimedOneUpToTheDefault()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 300);
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(s_tp0, 1L), s_deadline);
            Stopwatch elapsed = Stopwatch.StartNew();

            AssertTimedOut(await Assert.ThrowsAsync<KafkaException>(() => consumer.Commit()), s_timed["Commit"].Message, 300);
            AssertTimedOut(
                await Assert.ThrowsAsync<KafkaException>(() => consumer.Commit(TimeSpan.FromMilliseconds(200))),
                s_timed["Commit"].Message,
                200);

            Assert.True(elapsed.Elapsed < s_wait, $"the timed commits took {elapsed.Elapsed}");
        }
        finally
        {
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Theory]
    [MemberData(nameof(MetadataMethods))]
    public async Task AMetadataTimedOverload_WaitsUpToItsTimeout_AndTheUntimedOneUpToTheDefault(string method)
    {
        (Func<IAsyncConsumer<byte[], byte[]>, Task> untimed, Func<IAsyncConsumer<byte[], byte[]>, TimeSpan, Task> timed, string zero, string[] expired) = s_metadata[method];
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 300);
        try
        {
            Stopwatch elapsed = Stopwatch.StartNew();

            KafkaException atOnce = await Assert.ThrowsAsync<KafkaException>(() => timed(consumer, TimeSpan.Zero));
            Assert.Equal(RequestTimedOutCode, atOnce.Code);
            Assert.Equal(zero, atOnce.Message);
            AssertExpired(await Assert.ThrowsAsync<KafkaException>(() => timed(consumer, TimeSpan.FromMilliseconds(200))), expired);
            AssertExpired(await Assert.ThrowsAsync<KafkaException>(() => untimed(consumer)), expired);

            Assert.True(elapsed.Elapsed < s_wait, $"the metadata calls took {elapsed.Elapsed}");
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task AZeroTimeout_ReturnsTheOffsetsLookups_AtOnce()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 60_000);
        try
        {
            Stopwatch elapsed = Stopwatch.StartNew();

            KeyValuePair<TopicPartition, OffsetAndTimestamp?> lookup = Assert.Single(
                await TestTimeout.Run(() => consumer.OffsetsForTimes(Timestamps(0), TimeSpan.Zero), s_deadline));
            Assert.Equal(s_tp0, lookup.Key);
            Assert.Null(lookup.Value);
            Assert.Empty(await TestTimeout.Run(() => consumer.BeginningOffsets(new[] { s_tp0 }, TimeSpan.Zero), s_deadline));
            Assert.Empty(await TestTimeout.Run(() => consumer.EndOffsets(new[] { s_tp0 }, TimeSpan.Zero), s_deadline));

            Assert.True(elapsed.Elapsed < s_wait, $"the zero-timeout lookups took {elapsed.Elapsed}");
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task OnTheMock_TheTimedOverloads_AnswerAsTheUntimedOnes()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            IAsyncConsumer<byte[], byte[]> async = consumer;
            await async.Assign(new[] { s_tp0 });
            await async.Seek(s_tp0, 42L);
            consumer.UpdateBeginningOffset(Topic, 0, 5);
            consumer.UpdateEndOffset(Topic, 0, 77);
            consumer.UpdatePartitions(Topic, partitionCount: 2, leaderId: 1, leaderHost: "broker-1", leaderPort: 9092);

            foreach (TimeSpan timeout in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(5) })
            {
                Assert.Equal(await async.Position(s_tp0), await async.Position(s_tp0, timeout));

                await async.Commit(Offsets(7), timeout);
                await async.Commit(timeout);
                OffsetAndMetadata? committed = (await async.Committed(new[] { s_tp0 }, timeout))[s_tp0];
                Assert.Equal((await async.Committed(new[] { s_tp0 }))[s_tp0], committed);

                Assert.Equal(5L, (await async.BeginningOffsets(new[] { s_tp0 }, timeout))[s_tp0]);
                Assert.Equal(77L, (await async.EndOffsets(new[] { s_tp0 }, timeout))[s_tp0]);
                Assert.Equal(2, (await async.PartitionsFor(Topic, timeout)).Count);
                Assert.Equal(2, (await async.ListTopics(timeout))[Topic].Count);

                NotSupportedException untimed = await Assert.ThrowsAsync<NotSupportedException>(() => async.OffsetsForTimes(Timestamps(0)));
                NotSupportedException timed = await Assert.ThrowsAsync<NotSupportedException>(() => async.OffsetsForTimes(Timestamps(0), timeout));
                Assert.Equal(untimed.Message, timed.Message);
            }
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static void AssertNegative(ArgumentOutOfRangeException thrown, string message, long ms)
    {
        Assert.Equal("timeout", thrown.ParamName);
        Assert.StartsWith(string.Format(CultureInfo.InvariantCulture, message, ms), thrown.Message, StringComparison.Ordinal);
    }

    private static void AssertTimedOut(KafkaException thrown, string message, long ms)
    {
        Assert.Equal(RequestTimedOutCode, thrown.Code);
        Assert.Equal(string.Format(CultureInfo.InvariantCulture, message, ms), thrown.Message);
    }

    private static void AssertExpired(KafkaException thrown, string[] expired)
    {
        Assert.Equal(RequestTimedOutCode, thrown.Code);
        Assert.Contains(thrown.Message, expired);
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static Dictionary<TopicPartition, long> Timestamps(long timestamp) =>
        new Dictionary<TopicPartition, long> { [s_tp0] = timestamp };

    private static IAsyncConsumer<byte[], byte[]> Create(bool mock) => mock
        ? new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray)
        : Real(defaultApiTimeoutMs: 60_000);

    private static AsyncKafkaConsumer<byte[], byte[]> Real(int defaultApiTimeoutMs) => new AsyncKafkaConsumer<byte[], byte[]>(
        new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "test-group",
            ["default.api.timeout.ms"] = defaultApiTimeoutMs.ToString(CultureInfo.InvariantCulture),
        },
        Serdes.ByteArray,
        Serdes.ByteArray);
}
