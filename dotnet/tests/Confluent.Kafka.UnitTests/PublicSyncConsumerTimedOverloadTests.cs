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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>The sync consumer's <see cref="TimeSpan"/> overloads: the timeout check, and the bound each one applies.</summary>
public sealed class PublicSyncConsumerTimedOverloadTests
{
    private const string Topic = "test";

    private const int RequestTimedOutCode = 7;

    private const int UnsupportedVersionCode = 35;

    private const string CoreNegative = "Timeout must not be negative";

    private const string TimerNegative = "Invalid negative timeout {0}";

    private const string MetadataExpiredByManager = "Timeout expired while fetching topic metadata";

    private const string CommittedTimeout =
        "Timeout of {0}ms expired before the last committed offset for partitions [test-0] could be determined. "
        + "Try tuning default.api.timeout.ms larger to relax the threshold.";

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly Dictionary<string, (Action<IConsumer<byte[], byte[]>, TimeSpan> Call, string Message)> s_negative =
        new Dictionary<string, (Action<IConsumer<byte[], byte[]>, TimeSpan>, string)>
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

    private static readonly Dictionary<string, (Action<IConsumer<byte[], byte[]>> Untimed, Action<IConsumer<byte[], byte[]>, TimeSpan> Timed, string Message)> s_timed =
        new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Action<IConsumer<byte[], byte[]>, TimeSpan>, string)>
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

    private static readonly Dictionary<string, (Action<IConsumer<byte[], byte[]>> Untimed, Action<IConsumer<byte[], byte[]>, TimeSpan> Timed, string Zero, string[] Expired)> s_metadata =
        new Dictionary<string, (Action<IConsumer<byte[], byte[]>>, Action<IConsumer<byte[], byte[]>, TimeSpan>, string, string[])>
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
    public void ATimedOverload_RejectsANegativeTimeout_AndLeavesTheConsumerAsItWas(string method)
    {
        (Action<IConsumer<byte[], byte[]>, TimeSpan> call, string message) = s_negative[method];
        foreach (bool mock in new[] { true, false })
        {
            IConsumer<byte[], byte[]> consumer = Create(mock);
            using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
            consumer.Assign(new[] { s_tp0 });

            AssertNegative(Assert.Throws<ArgumentOutOfRangeException>(() => call(consumer, TimeSpan.FromSeconds(-1))), message, -1000);
            AssertNegative(Assert.Throws<ArgumentOutOfRangeException>(() => call(consumer, TimeSpan.FromTicks(-5000))), message, -1);
            Assert.Equal(new[] { s_tp0 }, consumer.Assignment());
        }
    }

    [Theory]
    [MemberData(nameof(NegativeMethods))]
    public void ATimedOverload_ChecksTheClosedStateFirst(string method)
    {
        Action<IConsumer<byte[], byte[]>, TimeSpan> call = s_negative[method].Call;
        foreach (bool mock in new[] { true, false })
        {
            IConsumer<byte[], byte[]> consumer = Create(mock);
            TestTimeout.Teardown(consumer, () => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)));

            foreach (TimeSpan timeout in new[] { TimeSpan.FromSeconds(-1), TimeSpan.FromTicks(-1), TimeSpan.MinValue })
            {
                Assert.Throws<ObjectDisposedException>(() => call(consumer, timeout));
            }
        }
    }

    [Theory]
    [MemberData(nameof(TimedMethods))]
    public void ATimedOverload_WaitsUpToItsTimeout_AndTheUntimedOneUpToTheDefault(string method)
    {
        (Action<IConsumer<byte[], byte[]>> untimed, Action<IConsumer<byte[], byte[]>, TimeSpan> timed, string message) = s_timed[method];
        KafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 300);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        Stopwatch elapsed = Stopwatch.StartNew();

        AssertTimedOut(Assert.Throws<KafkaException>(() => untimed(consumer)), message, 300);
        AssertTimedOut(Assert.Throws<KafkaException>(() => timed(consumer, TimeSpan.FromMilliseconds(200))), message, 200);
        AssertTimedOut(Assert.Throws<KafkaException>(() => timed(consumer, TimeSpan.FromTicks(1_509_000))), message, 150);

        Assert.True(elapsed.Elapsed < s_wait, $"the timed calls took {elapsed.Elapsed}");
    }

    [Fact]
    public void ATimedCommitOfTheConsumedPositions_WaitsUpToItsTimeout_AndTheUntimedOneUpToTheDefault()
    {
        KafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 300);
        try
        {
            consumer.Assign(new[] { s_tp0 });
            consumer.Seek(s_tp0, 1L);
            Stopwatch elapsed = Stopwatch.StartNew();

            AssertTimedOut(Assert.Throws<KafkaException>(() => consumer.Commit()), s_timed["Commit"].Message, 300);
            AssertTimedOut(
                Assert.Throws<KafkaException>(() => consumer.Commit(TimeSpan.FromMilliseconds(200))),
                s_timed["Commit"].Message,
                200);

            Assert.True(elapsed.Elapsed < s_wait, $"the timed commits took {elapsed.Elapsed}");
        }
        finally
        {
            TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
            TestTimeout.Dispose(consumer);
        }
    }

    [Theory]
    [MemberData(nameof(MetadataMethods))]
    public void AMetadataTimedOverload_WaitsUpToItsTimeout_AndTheUntimedOneUpToTheDefault(string method)
    {
        (Action<IConsumer<byte[], byte[]>> untimed, Action<IConsumer<byte[], byte[]>, TimeSpan> timed, string zero, string[] expired) = s_metadata[method];
        KafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 300);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Stopwatch elapsed = Stopwatch.StartNew();

        KafkaException atOnce = Assert.Throws<KafkaException>(() => timed(consumer, TimeSpan.Zero));
        Assert.Equal(RequestTimedOutCode, atOnce.Code);
        Assert.Equal(zero, atOnce.Message);
        AssertExpired(Assert.Throws<KafkaException>(() => timed(consumer, TimeSpan.FromMilliseconds(200))), expired);
        AssertExpired(Assert.Throws<KafkaException>(() => untimed(consumer)), expired);

        Assert.True(elapsed.Elapsed < s_wait, $"the metadata calls took {elapsed.Elapsed}");
    }

    [Fact]
    public void AZeroTimeout_ReturnsTheOffsetsLookups_AtOnce()
    {
        KafkaConsumer<byte[], byte[]> consumer = Real(defaultApiTimeoutMs: 60_000);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        Stopwatch elapsed = Stopwatch.StartNew();

        KeyValuePair<TopicPartition, OffsetAndTimestamp?> lookup =
            Assert.Single(consumer.OffsetsForTimes(Timestamps(0), TimeSpan.Zero));
        Assert.Equal(s_tp0, lookup.Key);
        Assert.Null(lookup.Value);
        Assert.Empty(consumer.BeginningOffsets(new[] { s_tp0 }, TimeSpan.Zero));
        Assert.Empty(consumer.EndOffsets(new[] { s_tp0 }, TimeSpan.Zero));

        Assert.True(elapsed.Elapsed < s_wait, $"the zero-timeout lookups took {elapsed.Elapsed}");
    }

    [Fact]
    public void OnTheMock_TheTimedOverloads_AnswerAsTheUntimedOnes()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        IConsumer<byte[], byte[]> sync = consumer;
        sync.Assign(new[] { s_tp0 });
        sync.Seek(s_tp0, 42L);
        consumer.UpdateBeginningOffset(Topic, 0, 5);
        consumer.UpdateEndOffset(Topic, 0, 77);
        consumer.UpdatePartitions(Topic, partitionCount: 2, leaderId: 1, leaderHost: "broker-1", leaderPort: 9092);

        foreach (TimeSpan timeout in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(5) })
        {
            Assert.Equal(sync.Position(s_tp0), sync.Position(s_tp0, timeout));

            sync.Commit(Offsets(7), timeout);
            sync.Commit(timeout);
            OffsetAndMetadata? committed = sync.Committed(new[] { s_tp0 }, timeout)[s_tp0];
            Assert.Equal(sync.Committed(new[] { s_tp0 })[s_tp0], committed);

            Assert.Equal(5L, sync.BeginningOffsets(new[] { s_tp0 }, timeout)[s_tp0]);
            Assert.Equal(77L, sync.EndOffsets(new[] { s_tp0 }, timeout)[s_tp0]);
            Assert.Equal(2, sync.PartitionsFor(Topic, timeout).Count);
            Assert.Equal(2, sync.ListTopics(timeout)[Topic].Count);

            KafkaException untimed = Assert.Throws<KafkaException>(() => sync.OffsetsForTimes(Timestamps(0)));
            KafkaException timed = Assert.Throws<KafkaException>(() => sync.OffsetsForTimes(Timestamps(0), timeout));
            Assert.Equal(UnsupportedVersionCode, timed.Code);
            Assert.Equal(untimed.Message, timed.Message);
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

    private static IConsumer<byte[], byte[]> Create(bool mock) => mock
        ? new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray)
        : Real(defaultApiTimeoutMs: 60_000);

    private static KafkaConsumer<byte[], byte[]> Real(int defaultApiTimeoutMs) => new KafkaConsumer<byte[], byte[]>(
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
