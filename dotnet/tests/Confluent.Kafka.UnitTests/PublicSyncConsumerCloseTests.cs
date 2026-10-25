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
using System.Runtime.CompilerServices;
using System.Threading;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync consumer's <see cref="IConsumer{TKey, TValue}.Close(CloseOptions)"/>, and a <c>Close</c> or
/// <c>Dispose</c> from another thread while a blocking call holds the consumer.
/// </summary>
public sealed class PublicSyncConsumerCloseTests
{
    private const string NegativeTimeout = "The timeout cannot be negative.";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const int WakeupCode = -18;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_loopBudget = TimeSpan.FromSeconds(20);

    private static readonly TimeSpan s_blockingTimeout = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan s_sustainedHold = TimeSpan.FromMilliseconds(200);

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
    public void Close_NullOptions_ThrowsArgumentNullException_EvenWhenClosed(bool mock)
    {
        IConsumer<byte[], byte[]> consumer = Create(mock);
        AssertNullOption(Assert.Throws<ArgumentNullException>(() => consumer.Close(null!)));

        TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        AssertNullOption(Assert.Throws<ArgumentNullException>(() => consumer.Close(null!)));
    }

    [Fact]
    public void Close_NegativeTimeout_OnARealConsumer_ThrowsAndStaysOpen()
    {
        KafkaConsumer<byte[], byte[]> consumer = Real();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Close(CloseOptions.Timeout(s_negative))));
        AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(-1)).WithGroupMembershipOperation(GroupMembershipOperation.LeaveGroup))));

        Assert.Empty(consumer.Subscription());
        TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
    }

    [Fact]
    public void Close_NegativeTimeout_OnAClosedRealConsumer_StillThrows()
    {
        KafkaConsumer<byte[], byte[]> consumer = Real();
        TestTimeout.Run(consumer.Close, s_deadline);

        AssertNegativeTimeout(Assert.Throws<ArgumentOutOfRangeException>(() => consumer.Close(CloseOptions.Timeout(s_negative))));
    }

    [Fact]
    public void Close_NegativeTimeout_OnTheMock_ClosesAsForZero()
    {
        MockConsumer<byte[], byte[]> consumer = Mock();

        TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(s_negative)), s_deadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(s_negative)), s_deadline);
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public void Close_WithEachOperation_Closes(GroupMembershipOperation operation)
    {
        foreach (bool mock in new[] { true, false })
        {
            IConsumer<byte[], byte[]> consumer = Create(mock);
            consumer.Assign(new[] { new TopicPartition("sync-close-topic", 0) });

            TestTimeout.Run(
                () => consumer.Close(CloseOptions.GroupMembershipOperation(operation).WithTimeout(TimeSpan.Zero)),
                s_deadline);

            Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Close_WithANullTimeout_Closes(bool mock)
    {
        IConsumer<byte[], byte[]> consumer = Create(mock);

        TestTimeout.Run(() => consumer.Close(CloseOptions.GroupMembershipOperation(GroupMembershipOperation.Default)), s_deadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Close_WithOptions_IsIdempotent_WithCloseAndDispose(bool mock)
    {
        IConsumer<byte[], byte[]> first = Create(mock);
        TestTimeout.Run(() => first.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        TestTimeout.Run(() => first.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        TestTimeout.Run(first.Close, s_deadline);
        TestTimeout.Dispose(first);

        IConsumer<byte[], byte[]> second = Create(mock);
        TestTimeout.Run(second.Close, s_deadline);
        TestTimeout.Run(() => second.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);

        IConsumer<byte[], byte[]> third = Create(mock);
        TestTimeout.Dispose(third);
        TestTimeout.Run(() => third.Close(CloseOptions.GroupMembershipOperation(GroupMembershipOperation.LeaveGroup)), s_deadline);
    }

    [Theory]
    [InlineData("ListTopics")]
    [InlineData("Poll")]
    public void Close_FromAnotherThread_WhileABlockingCallHoldsTheConsumer_ThrowsAndStaysOpen(string call)
    {
        KafkaConsumer<byte[], byte[]> consumer = Real();
        try
        {
            StrongBox<Exception?> outcome = new StrongBox<Exception?>();
            Thread owner = StartBlockingCall(consumer, call, outcome);

            WaitUntilTheConsumerIsHeld(consumer);
            InvalidOperationException plain = Assert.Throws<InvalidOperationException>(() => consumer.Close());
            InvalidOperationException withOptions = Assert.Throws<InvalidOperationException>(
                () => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)));
            Assert.Equal(ConcurrentMessage, plain.Message);
            Assert.Equal(ConcurrentMessage, withOptions.Message);

            consumer.Wakeup();
            Assert.True(owner.Join(s_deadline), "the blocking call did not end after Wakeup");
            Assert.Equal(WakeupCode, Assert.IsType<KafkaException>(outcome.Value).Code);

            Assert.Empty(consumer.Subscription());
            TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
            Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Theory]
    [InlineData("ListTopics")]
    [InlineData("Poll")]
    public void Dispose_FromAnotherThread_WhileABlockingCallHoldsTheConsumer_EndsTheCall_AndDoesNotThrow(string call)
    {
        KafkaConsumer<byte[], byte[]> consumer = Real();
        StrongBox<Exception?> outcome = new StrongBox<Exception?>();
        Thread owner = StartBlockingCall(consumer, call, outcome);

        WaitUntilTheConsumerIsHeld(consumer);
        Stopwatch elapsed = Stopwatch.StartNew();
        TestTimeout.Run(consumer.Dispose, s_deadline);
        elapsed.Stop();

        Assert.True(owner.Join(s_deadline), "the blocking call did not end after Dispose");
        Assert.True(elapsed.Elapsed < s_blockingTimeout, $"Dispose waited {elapsed.Elapsed} for the blocking call");
        Assert.Equal(WakeupCode, Assert.IsType<KafkaException>(outcome.Value).Code);
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
    }

    private static Thread StartBlockingCall(KafkaConsumer<byte[], byte[]> consumer, string call, StrongBox<Exception?> outcome)
    {
        Action blocking = call == "Poll"
            ? () => consumer.Poll(s_blockingTimeout)
            : () => consumer.ListTopics(s_blockingTimeout);
        if (call == "Poll")
        {
            consumer.Assign(new[] { new TopicPartition("sync-close-topic", 0) });
        }

        Thread owner = new Thread(() =>
        {
            DateTime end = DateTime.UtcNow + s_loopBudget;
            while (true)
            {
                try
                {
                    blocking();
                    outcome.Value = null;
                    return;
                }
                catch (InvalidOperationException e) when (e.Message == ConcurrentMessage && DateTime.UtcNow < end)
                {
                }
                catch (Exception e)
                {
                    outcome.Value = e;
                    return;
                }
            }
        });
        owner.IsBackground = true;
        owner.Start();
        return owner;
    }

    // A timed call's checks hold the consumer for a moment before its call does, so only a hold that lasts is the call.
    private static void WaitUntilTheConsumerIsHeld(IConsumerCommon consumer)
    {
        DateTime end = DateTime.UtcNow + s_loopBudget;
        Stopwatch held = new Stopwatch();
        while (true)
        {
            try
            {
                consumer.Assignment();
                held.Reset();
            }
            catch (InvalidOperationException e) when (e.Message == ConcurrentMessage)
            {
                held.Start();
                if (held.Elapsed >= s_sustainedHold)
                {
                    return;
                }
            }

            Assert.True(DateTime.UtcNow < end, "the blocking call never held the consumer");
            Thread.Sleep(1);
        }
    }

    private static void AssertNullOption(ArgumentNullException thrown) => Assert.Equal("option", thrown.ParamName);

    private static void AssertNegativeTimeout(ArgumentOutOfRangeException thrown)
    {
        Assert.Equal("option", thrown.ParamName);
        Assert.StartsWith(NegativeTimeout, thrown.Message, StringComparison.Ordinal);
    }

    private static IConsumer<byte[], byte[]> Create(bool mock) => mock ? Mock() : Real();

    private static MockConsumer<byte[], byte[]> Mock() => new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static KafkaConsumer<byte[], byte[]> Real() => new KafkaConsumer<byte[], byte[]>(
        new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
            ["group.protocol"] = "consumer",
        },
        Serdes.ByteArray,
        Serdes.ByteArray);
}
