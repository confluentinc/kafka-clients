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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ConsumerHandle"/> through the public API, broker-free: <see cref="IConsumerCommon.Handle"/> on the four
/// consumers, the handle's operations inside and outside a callback, on a mock, after its own <c>Dispose</c> and
/// after its consumer's destroy.
/// </summary>
public sealed class PublicConsumerReentrancyHandleTests
{
    private const string Topic = "reentrancy-handle-topic";

    private const string NonAsciiTopic = "grüße-café-Ω-topic";

    private const int UnsupportedVersionCode = 35;

    private const int WakeupCode = -18;

    private const string MockAsyncUnsupportedMessage =
        "ConsumerHandle async operations are not supported on a MockConsumer handle; " +
        "drive the MockConsumer directly.";

    private const string Concurrent = "KafkaConsumer is not safe for multi-threaded access.";

    private const string OutsideACallback =
        "KafkaConsumer is not safe for multi-threaded access. ConsumerHandle operations are allowed only from inside "
        + "a callback the consumer is running (ConsumerRebalanceListener, OffsetCommitCallback); from another thread "
        + "call wakeup().";

    private const string ConsumerDestroyed = "consumer destroyed";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition s_tp1 = new TopicPartition(Topic, 1);

    [Fact]
    public void InsideAListener_TheConsumersAssignment_RoutesToTheHandle_WhileItsSubscriptionIsRejected()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        IReadOnlyCollection<TopicPartition>? viaHandle = null;
        IReadOnlyCollection<TopicPartition>? viaConsumer = null;
        Exception? subscription = null;

        ProbingListener listener = new ProbingListener(() =>
        {
            viaHandle = handle.Assignment();
            viaConsumer = consumer.Assignment();
            try
            {
                consumer.Subscription();
            }
            catch (Exception e)
            {
                subscription = e;
            }
        });

        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { s_tp0 });

        Assert.True(listener.Fired, "the listener never ran, so neither call was exercised");
        Assert.Equal(new[] { s_tp0 }, viaHandle);
        Assert.Equal(new[] { s_tp0 }, viaConsumer);
        InvalidOperationException rejected = Assert.IsType<InvalidOperationException>(subscription);
        Assert.Equal(Concurrent, rejected.Message);
    }

    [Fact]
    public void OutsideACallback_BothTheHandleAndTheConsumerGettersSucceed()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        Assert.Empty(handle.Assignment());
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void Handle_IsObtainable_FromInsideAListener_WhileTheGuardIsHeld()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ConsumerHandle? obtainedInCallback = null;
        ProbingListener listener = new ProbingListener(() => obtainedInCallback = consumer.Handle());

        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { s_tp0 });

        Assert.NotNull(obtainedInCallback);
        using (obtainedInCallback)
        {
            Assert.Equal(new[] { s_tp0 }, obtainedInCallback!.Assignment());
        }
    }

    [Fact]
    public void SyncMock_Handle_IsReachableThroughIConsumer()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = HandleOf(consumer);

        Assert.Empty(handle.Assignment());
    }

    [Fact]
    public void AsyncMock_Handle_IsReachableThroughIAsyncConsumer()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = HandleOf(consumer);

        Assert.Empty(handle.Assignment());
    }

    [Fact]
    public void SyncKafkaConsumer_Handle_IsReachableThroughIConsumer()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("p8-sync-handle");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = HandleOf(consumer);

        Assert.Empty(handle.Assignment());
    }

    [Fact]
    public async Task AsyncKafkaConsumer_Handle_IsReachableThroughIAsyncConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(Config("p8-async-handle"), Serdes.ByteArray, Serdes.ByteArray);
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            using ConsumerHandle handle = HandleOf(consumer);
            Assert.Empty(handle.Assignment());
        }
    }

    [Fact]
    public void Handles_AreIndependent_DisposingOneLeavesTheOtherUsable()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerHandle first = consumer.Handle();
        using ConsumerHandle second = consumer.Handle();

        Assert.NotSame(first, second);
        first.Dispose();

        Assert.Empty(second.Assignment());
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void Handle_AfterConsumerDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.Handle());
    }

    [Fact]
    public void MockDerivedHandle_WakeupAndTheTwoGetters_Work()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        handle.Wakeup();

        Assert.Empty(handle.Assignment());
        Assert.Empty(handle.Paused());
    }

    [Fact]
    public void MockDerivedHandle_TheGetters_ReadTheMocksState()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0, s_tp1 });
        consumer.Pause(new[] { s_tp1 });
        using ConsumerHandle handle = consumer.Handle();

        Assert.Equal(new[] { s_tp0, s_tp1 }, handle.Assignment().OrderBy(tp => tp.Partition));
        Assert.Equal(new[] { s_tp1 }, handle.Paused());
    }

    [Fact]
    public void MockDerivedHandle_Wakeup_IsRaisedByTheNextPoll()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        using ConsumerHandle handle = consumer.Handle();

        handle.Wakeup();

        KafkaException woken = Assert.Throws<KafkaException>(() => consumer.Poll(TimeSpan.Zero));
        Assert.Equal(WakeupCode, woken.Code);
        Assert.Empty(consumer.Poll(TimeSpan.Zero));
    }

    public static TheoryData<string, Action<ConsumerHandle>> MockUnsupportedOperations()
    {
        IReadOnlyCollection<TopicPartition> one = new[] { s_tp0 };
        IReadOnlyDictionary<TopicPartition, long> timestamps = new Dictionary<TopicPartition, long> { [s_tp0] = 1 };

        return new TheoryData<string, Action<ConsumerHandle>>
        {
            { "Pause", h => h.Pause(one) },
            { "Resume", h => h.Resume(one) },
            { "SeekToBeginning", h => h.SeekToBeginning(one) },
            { "SeekToEnd", h => h.SeekToEnd(one) },
            { "Seek", h => h.Seek(s_tp0, 5) },
            { "Seek(OffsetAndMetadata)", h => h.Seek(s_tp0, new OffsetAndMetadata(5, "m", null)) },
            { "Position", h => h.Position(s_tp0) },
            { "Position(TimeSpan)", h => h.Position(s_tp0, TimeSpan.FromSeconds(1)) },
            { "Committed", h => h.Committed(one) },
            { "BeginningOffsets", h => h.BeginningOffsets(one) },
            { "EndOffsets", h => h.EndOffsets(one) },
            { "OffsetsForTimes", h => h.OffsetsForTimes(timestamps) },
        };
    }

    [Theory]
    [MemberData(nameof(MockUnsupportedOperations))]
    public void MockDerivedHandle_EveryUnwiredOperation_ThrowsUnsupportedVersion(
        string name, Action<ConsumerHandle> operation)
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        using ConsumerHandle handle = consumer.Handle();

        KafkaException failure = Assert.Throws<KafkaException>(() => operation(handle));

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(MockAsyncUnsupportedMessage, failure.Message);
        Assert.False(failure.IsRetriable, name);
    }

    [Fact]
    public void MockDerivedHandle_TheCommits_CommitIntoTheMock()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        using ConsumerHandle handle = consumer.Handle();
        List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception)> seen =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?)>();
        IOffsetCommitCallback recording = new DelegateCallback((o, e) => seen.Add((o, e)));

        handle.Commit(Offsets(3));
        Assert.Equal(3, CommittedOffset(consumer));

        handle.CommitAsync(Offsets(4));
        Assert.Equal(4, CommittedOffset(consumer));

        handle.CommitAsync(Offsets(5), callback: null);
        Assert.Equal(5, CommittedOffset(consumer));

        handle.CommitAsync(Offsets(6), recording);
        Assert.Equal(6, CommittedOffset(consumer));
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? delivered, Exception? exception) = Assert.Single(seen);
        Assert.Equal(6, Assert.Single(delivered!).Value.Offset);
        Assert.Null(exception);

        consumer.Seek(s_tp0, 7);
        handle.Commit();
        Assert.Equal(7, CommittedOffset(consumer));

        consumer.Seek(s_tp0, 8);
        handle.CommitAsync();
        Assert.Equal(8, CommittedOffset(consumer));

        consumer.Seek(s_tp0, 9);
        handle.CommitAsync(recording);
        Assert.Equal(9, CommittedOffset(consumer));
        Assert.Equal(2, seen.Count);
        Assert.Equal(9, Assert.Single(seen[1].Offsets!).Value.Offset);
        Assert.Null(seen[1].Exception);

        Assert.Equal((long?)0, handle.CurrentLag(s_tp0));
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerHandle handle = consumer.Handle();

        handle.Dispose();
        handle.Dispose();
        handle.Dispose();

        Assert.Empty(consumer.Assignment());
    }

    public static TheoryData<string, Action<ConsumerHandle>> GatedOperations()
    {
        TheoryData<string, Action<ConsumerHandle>> data = MockUnsupportedOperations();
        data.Add("CurrentLag", h => h.CurrentLag(s_tp0));
        data.Add("Commit", h => h.Commit());
        data.Add("Commit(offsets)", h => h.Commit(Offsets(7)));
        data.Add("CommitAsync", h => h.CommitAsync());
        data.Add("CommitAsync(callback)", h => h.CommitAsync(new DelegateCallback((_, _) => { })));
        data.Add("CommitAsync(offsets)", h => h.CommitAsync(Offsets(7)));
        data.Add("CommitAsync(offsets, callback)", h => h.CommitAsync(Offsets(7), new DelegateCallback((_, _) => { })));
        return data;
    }

    public static TheoryData<string, Action<ConsumerHandle>> AllOperations()
    {
        TheoryData<string, Action<ConsumerHandle>> data = GatedOperations();
        data.Add("Wakeup", h => h.Wakeup());
        data.Add("Assignment", h => h.Assignment());
        data.Add("Paused", h => h.Paused());
        return data;
    }

    [Theory]
    [MemberData(nameof(AllOperations))]
    public void EveryOperation_AfterDispose_ThrowsObjectDisposed(string name, Action<ConsumerHandle> operation)
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerHandle handle = consumer.Handle();
        handle.Dispose();

        ObjectDisposedException failure = Assert.Throws<ObjectDisposedException>(() => operation(handle));
        Assert.Equal(nameof(ConsumerHandle), failure.ObjectName);
        Assert.NotNull(name);
    }

    [Theory]
    [MemberData(nameof(GatedOperations))]
    public void EveryBlockingOperation_AfterTheConsumersDestroy_ThrowsInvalidOperation(
        string name, Action<ConsumerHandle> operation)
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Assign(new[] { s_tp0 });
        using ConsumerHandle handle = consumer.Handle();
        TestTimeout.Dispose(consumer);

        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(() => operation(handle));
        Assert.Equal(ConsumerDestroyed, failure.Message);
        Assert.NotNull(name);
    }

    [Fact]
    public void KafkaConsumerHandle_AfterTheConsumersDestroy_ThrowsInvalidOperation()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("handle-after-destroy");
        using ConsumerHandle handle = consumer.Handle();
        TestTimeout.Run(consumer.Dispose, s_deadline);

        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(() => handle.Position(s_tp0));
        Assert.Equal(ConsumerDestroyed, failure.Message);
    }

    [Fact]
    public void Position_WithNegativeTimeout_ThrowsTheTimersMessage()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        AssertTimerNegative(Assert.Throws<ArgumentOutOfRangeException>(() => handle.Position(s_tp0, TimeSpan.FromSeconds(-1))), -1000);
        AssertTimerNegative(Assert.Throws<ArgumentOutOfRangeException>(() => handle.Position(s_tp0, TimeSpan.FromMilliseconds(-1))), -1);
        AssertTimerNegative(Assert.Throws<ArgumentOutOfRangeException>(() => handle.Position(s_tp0, TimeSpan.FromTicks(-5000))), -1);
    }

    [Fact]
    public void Position_WithNegativeTimeout_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerHandle handle = consumer.Handle();
        handle.Dispose();

        ObjectDisposedException failure = Assert.Throws<ObjectDisposedException>(
            () => handle.Position(s_tp0, TimeSpan.FromSeconds(-1)));
        Assert.Equal(nameof(ConsumerHandle), failure.ObjectName);
    }

    [Fact]
    public void Position_WithNegativeTimeoutAndNullTopic_ThrowsForTheTimeout()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => handle.Position(new TopicPartition(null!, 0), TimeSpan.FromSeconds(-1)));
        Assert.Equal("timeout", failure.ParamName);
    }

    [Fact]
    public void Pause_WithNullPartitions_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        ArgumentNullException failure = Assert.Throws<ArgumentNullException>(() => handle.Pause(null!));
        Assert.Equal("partitions", failure.ParamName);
    }

    [Fact]
    public void CommitAsync_WithANullCallback_ThrowsArgumentNull()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        ArgumentNullException failure = Assert.Throws<ArgumentNullException>(
            () => handle.CommitAsync((IOffsetCommitCallback)null!));
        Assert.Equal("callback", failure.ParamName);
    }

    [Fact]
    public void Seek_WithNegativeOffset_ThrowsArgumentOutOfRange()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using ConsumerHandle handle = consumer.Handle();

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(() => handle.Seek(s_tp0, -1));

        Assert.Equal("offset", failure.ParamName);
        Assert.Contains("seek offset must not be a negative number", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void LiveHandle_DoesNotDeferTheConsumersNativeDestroy()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle native = consumer.Handle;
        ConsumerHandle handle = consumer.CreateReentrancyHandle();

        TestTimeout.Dispose(consumer);
        Assert.True(native.IsClosed, "a live reentrancy handle must not defer the consumer's destroy");

        handle.Dispose();
    }

    [Fact]
    public void DisposingTheHandleFirst_LeavesTheConsumerAlive_AndItReleasesNormally()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle native = consumer.Handle;
        ConsumerHandle handle = consumer.CreateReentrancyHandle();

        handle.Dispose();
        Assert.False(native.IsClosed, "disposing a handle must not release its consumer");

        TestTimeout.Dispose(consumer);
        Assert.True(native.IsClosed, "the consumer must release on its own Dispose");
    }

    [Fact]
    public void HandleOutstandingAcrossConsumerDispose_ReadsAndWakesButRefusesTheBlockingOperations()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        ConsumerHandle handle = consumer.Handle();

        TestTimeout.Dispose(consumer);

        Assert.Empty(handle.Assignment());
        Assert.Empty(handle.Paused());
        handle.Wakeup();
        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(() => handle.Commit(Offsets(1)));
        Assert.Equal(ConsumerDestroyed, failure.Message);

        handle.Dispose();
    }

    [Fact]
    public void ACommitRegistration_ThatAHandleCommitRefusedAfterTheConsumersDestroy_IsReleasedAtOnce()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();
        TestTimeout.Dispose(consumer);

        InvalidOperationException failure = Assert.Throws<InvalidOperationException>(
            () => handle.CommitAsync(Offsets(1), new DelegateCallback((_, _) => { })));

        Assert.Equal(ConsumerDestroyed, failure.Message);
        CommitCallbackRegistration registration = Assert.IsType<CommitCallbackRegistration>(Registration(consumer));
        Assert.True(registration.IsReleased, "the registration of a commit refused after the destroy was kept");
    }

    [Fact]
    public void SyncConsumerDispose_WithALiveHandleOutstanding_ReturnsWithoutHanging()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        ConsumerHandle handle = consumer.Handle();

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        handle.Dispose();
    }

    [Fact]
    public async Task AsyncConsumerDisposeAsync_WithALiveHandleOutstanding_ReturnsWithoutHanging()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        ConsumerHandle handle = consumer.Handle();

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);

        handle.Dispose();
    }

    [Fact]
    public void ManyHandles_CreatedAndDisposed_LeaveTheConsumerReleasable()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle native = consumer.Handle;

        for (int i = 0; i < 200; i++)
        {
            using ConsumerHandle handle = consumer.CreateReentrancyHandle();
            Assert.Empty(handle.Assignment());
        }

        TestTimeout.Dispose(consumer);
        Assert.True(native.IsClosed, "200 handle round-trips must leave the consumer releasable");
    }

    [Theory]
    [MemberData(nameof(GatedOperations))]
    public void KafkaConsumerHandle_OutsideACallback_RefusesEveryBlockingOperation(
        string name, Action<ConsumerHandle> operation)
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("handle-outside-a-callback");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { s_tp0 });
                using ConsumerHandle handle = consumer.Handle();

                InvalidOperationException refused = Assert.Throws<InvalidOperationException>(() => operation(handle));
                Assert.Equal(OutsideACallback, refused.Message);
                Assert.NotNull(name);
            },
            s_deadline);
    }

    [Fact]
    public void KafkaConsumerHandle_OutsideACallback_WakesAndReads()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("handle-wakes-and-reads");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { s_tp0, s_tp1 });
                consumer.Pause(new[] { s_tp1 });
                using ConsumerHandle handle = consumer.Handle();

                Assert.Equal(new[] { s_tp0, s_tp1 }, handle.Assignment().OrderBy(tp => tp.Partition));
                Assert.Equal(new[] { s_tp1 }, handle.Paused());

                handle.Wakeup();
                KafkaException woken = Assert.Throws<KafkaException>(() => consumer.Poll(TimeSpan.Zero));
                Assert.Equal(WakeupCode, woken.Code);
                Assert.Empty(consumer.Poll(TimeSpan.Zero));
            },
            s_deadline);
    }

    [Fact]
    public void KafkaConsumerHandle_InsideACommitCallback_RunsTheOperations()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("handle-inside-a-callback");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        try
        {
            TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { s_tp0 });
                using ConsumerHandle handle = consumer.Handle();
                long? position = null;
                long? lag = -1;
                IReadOnlyCollection<TopicPartition>? paused = null;
                IReadOnlyCollection<TopicPartition>? resumed = null;
                Exception? failure = null;

                consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                {
                    try
                    {
                        handle.Seek(s_tp0, 5);
                        position = handle.Position(s_tp0);
                        handle.Pause(new[] { s_tp0 });
                        paused = handle.Paused();
                        handle.Resume(new[] { s_tp0 });
                        resumed = handle.Paused();
                        lag = handle.CurrentLag(s_tp0);
                        handle.CommitAsync(Empty());
                        handle.Commit(Empty());
                    }
                    catch (Exception e)
                    {
                        failure = e;
                    }
                }));
                consumer.Commit(Empty());

                Assert.Null(failure);
                Assert.Equal((long?)5, position);
                Assert.Equal(new[] { s_tp0 }, paused);
                Assert.Empty(resumed!);
                Assert.Null(lag);
                Assert.Equal(5, consumer.Position(s_tp0));
            },
            s_deadline);
        }
        finally
        {
            TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        }
    }

    [Fact]
    public void HandleAndConsumer_ReportTheSameCoreError_Identically()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("p8-error-parity");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { s_tp0 });
                using ConsumerHandle handle = consumer.Handle();
                TopicPartition unassigned = new TopicPartition("unassigned-parity-topic", 0);
                Exception? viaHandle = null;

                consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                {
                    try
                    {
                        handle.Position(unassigned);
                    }
                    catch (Exception e)
                    {
                        viaHandle = e;
                    }
                }));
                consumer.Commit(Empty());
                Exception viaConsumer = Assert.ThrowsAny<Exception>(() => consumer.Position(unassigned));

                Assert.NotNull(viaHandle);
                Assert.IsType<InvalidOperationException>(viaConsumer);
                Assert.Equal(viaConsumer.GetType(), viaHandle!.GetType());
                Assert.Equal(
                    "You can only check the position for partitions assigned to this consumer.", viaHandle.Message);
                Assert.Equal(viaConsumer.Message, viaHandle.Message);
            },
            s_deadline);
    }

    [Fact]
    public void NonAsciiTopic_RoundTripsThroughAHandleResult()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("p8-non-ascii");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { new TopicPartition(NonAsciiTopic, 3) });
                using ConsumerHandle handle = consumer.Handle();

                TopicPartition viaHandle = Assert.Single(handle.Assignment());
                Assert.Equal(NonAsciiTopic, viaHandle.Topic);
                Assert.Equal(3, viaHandle.Partition);
                Assert.Equal(consumer.Assignment(), handle.Assignment());
            },
            s_deadline);
    }

    private static void AssertTimerNegative(ArgumentOutOfRangeException thrown, long ms)
    {
        Assert.Equal("timeout", thrown.ParamName);
        Assert.StartsWith("Invalid negative timeout " + ms.ToString(CultureInfo.InvariantCulture), thrown.Message, StringComparison.Ordinal);
    }

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static Dictionary<string, string> Config(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    private static KafkaConsumer<byte[], byte[]> NewKafkaConsumer(string groupId) =>
        new KafkaConsumer<byte[], byte[]>(Config(groupId), Serdes.ByteArray, Serdes.ByteArray);

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static long CommittedOffset(MockConsumer<byte[], byte[]> consumer) =>
        consumer.Committed(new[] { s_tp0 })[s_tp0]!.Offset;

    private static CommitCallbackRegistration? Registration(object consumer) =>
        ((NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!).CurrentCommitCallbackRegistration;

    private static ConsumerHandle HandleOf(IConsumer<byte[], byte[]> consumer) => consumer.Handle();

    private static ConsumerHandle HandleOf(IAsyncConsumer<byte[], byte[]> consumer) => consumer.Handle();

    /// <summary>Runs a probe inside <c>OnPartitionsAssigned</c>.</summary>
    private sealed class ProbingListener : IConsumerRebalanceListener
    {
        private readonly Action _probe;

        internal ProbingListener(Action probe)
        {
            _probe = probe;
        }

        internal bool Fired { get; private set; }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            Fired = true;
            _probe();
        }

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>An <see cref="IOffsetCommitCallback"/> over a delegate.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> _onComplete;

        internal DelegateCallback(Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> onComplete) =>
            _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete(offsets, exception);
    }
}
