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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async consumer's <see cref="IAsyncConsumer{TKey, TValue}.Close(CloseOptions, System.Threading.CancellationToken)"/>,
/// and an explicit <c>Close</c> while an async call holds the consumer.
/// </summary>
public sealed class PublicAsyncConsumerCloseTests
{
    private const string NegativeTimeout = "The timeout cannot be negative.";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const int WakeupCode = -18;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_bound = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan s_negative = TimeSpan.FromMilliseconds(-1);

    public static IEnumerable<object[]> Operations() => new[]
    {
        new object[] { GroupMembershipOperation.LeaveGroup },
        new object[] { GroupMembershipOperation.RemainInGroup },
        new object[] { GroupMembershipOperation.Default },
    };

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Close_NullOptions_ThrowsArgumentNullException_EvenWhenClosed(bool mock)
    {
        IAsyncConsumer<byte[], byte[]> consumer = Create(mock);
        AssertNullOption(Assert.Throws<ArgumentNullException>(() => { _ = consumer.Close(null!); }));

        await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        AssertNullOption(Assert.Throws<ArgumentNullException>(() => { _ = consumer.Close(null!); }));
    }

    [Fact]
    public async Task Close_NegativeTimeout_OnARealConsumer_ThrowsAndStaysOpen()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real();
        try
        {
            AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = consumer.Close(CloseOptions.Timeout(s_negative)); }));
            AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                _ = consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(-1)).WithGroupMembershipOperation(GroupMembershipOperation.LeaveGroup));
            }));

            Assert.Empty(consumer.Subscription());
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
            Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public async Task Close_NegativeTimeout_OnAClosedRealConsumer_StillThrows()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real();
        await TestTimeout.Run(() => consumer.Close(), s_deadline);

        AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => { _ = consumer.Close(CloseOptions.Timeout(s_negative)); }));
    }

    [Fact]
    public async Task Close_NegativeTimeout_OnTheMock_ClosesAsForZero()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = Mock();

        await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(s_negative)), s_deadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(s_negative)), s_deadline);
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Close_WithEachOperation_Closes(GroupMembershipOperation operation)
    {
        foreach (bool mock in new[] { true, false })
        {
            IAsyncConsumer<byte[], byte[]> consumer = Create(mock);
            await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition("async-close-topic", 0) }), s_deadline);

            await TestTimeout.Run(
                () => consumer.Close(CloseOptions.GroupMembershipOperation(operation).WithTimeout(TimeSpan.Zero)),
                s_deadline);

            Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        }
    }

    [Fact]
    public async Task Close_WithATimeout_BoundsTheCommitOfTheConsumedPositions()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real(groupId: "async-close-group");
        TopicPartition tp = new TopicPartition("async-close-topic", 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { tp }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(tp, 1L), s_deadline);
        Stopwatch elapsed = Stopwatch.StartNew();

        await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.FromMilliseconds(200))), s_deadline);

        Assert.True(elapsed.Elapsed < s_bound, $"Close(CloseOptions) took {elapsed.Elapsed}");
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Close_WithOptions_IsIdempotent_WithCloseAndDispose(bool mock)
    {
        IAsyncConsumer<byte[], byte[]> first = Create(mock);
        await TestTimeout.Run(() => first.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        await TestTimeout.Run(() => first.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        await TestTimeout.Run(() => first.Close(), s_deadline);
        await TestTimeout.DisposeAsync(first);

        IAsyncConsumer<byte[], byte[]> second = Create(mock);
        await TestTimeout.Run(() => second.Close(), s_deadline);
        await TestTimeout.Run(() => second.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);

        IAsyncConsumer<byte[], byte[]> third = Create(mock);
        await TestTimeout.DisposeAsync(third);
        await TestTimeout.Run(() => third.Close(CloseOptions.GroupMembershipOperation(GroupMembershipOperation.LeaveGroup)), s_deadline);
    }

    [Fact]
    public async Task Close_WhileAnAsyncCallHoldsTheConsumer_ThrowsAndStaysOpen()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = Real();
        try
        {
            Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> listing = consumer.ListTopics(TimeSpan.FromSeconds(60));
            await WaitUntilTheConsumerIsHeld(consumer);

            InvalidOperationException plain = await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.Close());
            InvalidOperationException withOptions = await Assert.ThrowsAsync<InvalidOperationException>(
                () => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)));
            Assert.Equal(ConcurrentMessage, plain.Message);
            Assert.Equal(ConcurrentMessage, withOptions.Message);

            consumer.Wakeup();
            Assert.Equal(WakeupCode, (await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => listing, s_deadline))).Code);

            Assert.Empty(consumer.Subscription());
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
            Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static async Task WaitUntilTheConsumerIsHeld(IConsumerCommon consumer)
    {
        Stopwatch waited = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                consumer.Assignment();
            }
            catch (InvalidOperationException e) when (e.Message == ConcurrentMessage)
            {
                return;
            }

            Assert.True(waited.Elapsed < s_deadline, "the async call never held the consumer");
            await Task.Delay(1);
        }
    }

    private static void AssertNullOption(ArgumentNullException thrown) => Assert.Equal("option", thrown.ParamName);

    private static void AssertNegativeTimeout(ArgumentOutOfRangeException thrown)
    {
        Assert.Equal("option", thrown.ParamName);
        Assert.StartsWith(NegativeTimeout, thrown.Message, StringComparison.Ordinal);
    }

    private static IAsyncConsumer<byte[], byte[]> Create(bool mock) => mock ? Mock() : Real();

    private static AsyncMockConsumer<byte[], byte[]> Mock() => new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncKafkaConsumer<byte[], byte[]> Real(string? groupId = null)
    {
        Dictionary<string, string> config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
            ["group.protocol"] = "consumer",
        };
        if (groupId is not null)
        {
            config["group.id"] = groupId;
        }

        return new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
    }
}
