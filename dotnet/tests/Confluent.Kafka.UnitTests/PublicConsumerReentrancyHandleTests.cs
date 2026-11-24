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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The core's consumer handle through <see cref="NativeConsumerHandle"/>, the internal handle a consumer routes its
/// in-callback calls to, broker-free: a handle over each of the four consumers, its operations inside and outside a
/// callback, on a mock, and after its consumer's destroy.
/// </summary>
public sealed class PublicConsumerReentrancyHandleTests
{
    private const string Topic = "reentrancy-handle-topic";

    private const string NonAsciiTopic = "grüße-café-Ω-topic";

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
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

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
        ConcurrentModificationException rejected = Assert.IsType<ConcurrentModificationException>(subscription);
        Assert.Equal(Concurrent, rejected.Message);
    }

    [Fact]
    public void OutsideACallback_BothTheHandleAndTheConsumerGettersSucceed()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        Assert.Empty(handle.Assignment());
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void Handle_IsObtainable_FromInsideAListener_WhileTheGuardIsHeld()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        NativeConsumerHandle? obtainedInCallback = null;
        ProbingListener listener = new ProbingListener(() => obtainedInCallback = CoreConsumerHandles.Of(consumer));

        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { s_tp0 });

        Assert.NotNull(obtainedInCallback);
        using (obtainedInCallback)
        {
            Assert.Equal(new[] { s_tp0 }, obtainedInCallback!.Assignment());
        }
    }

    [Fact]
    public void SyncMock_AHandle_ReadsItsAssignment()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        Assert.Empty(handle.Assignment());
    }

    [Fact]
    public void AsyncMock_AHandle_ReadsItsAssignment()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        Assert.Empty(handle.Assignment());
    }

    [Fact]
    public void SyncKafkaConsumer_AHandle_ReadsItsAssignment()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("p8-sync-handle");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        Assert.Empty(handle.Assignment());
    }

    [Fact]
    public async Task AsyncKafkaConsumer_AHandle_ReadsItsAssignment()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            await AsyncKafkaConsumer<byte[], byte[]>.Create(Config("p8-async-handle"), new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
            Assert.Empty(handle.Assignment());
        }
    }

    [Fact]
    public void Handles_AreIndependent_DisposingOneLeavesTheOtherUsable()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        NativeConsumerHandle first = CoreConsumerHandles.Of(consumer);
        using NativeConsumerHandle second = CoreConsumerHandles.Of(consumer);

        Assert.NotSame(first, second);
        first.Dispose();

        Assert.Empty(second.Assignment());
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void MockDerivedHandle_WakeupAndTheTwoGetters_Work()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

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
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        Assert.Equal(new[] { s_tp0, s_tp1 }, handle.Assignment().OrderBy(tp => tp.Partition()));
        Assert.Equal(new[] { s_tp1 }, handle.Paused());
    }

    [Fact]
    public void MockDerivedHandle_Wakeup_IsRaisedByTheNextPoll()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        handle.Wakeup();

        WakeupException woken = Assert.Throws<WakeupException>(() => consumer.Poll(TimeSpan.Zero));
        Assert.Equal(WakeupCode, woken.Code);
        Assert.Empty(consumer.Poll(TimeSpan.Zero));
    }

    // The theories take an operation's name: NativeConsumerHandle is internal, so a public test cannot take the
    // operation.
    public static TheoryData<string> MockUnsupportedOperationsNames() => NamesOf(MockUnsupportedOperations());

    public static TheoryData<string> GatedOperationsNames() => NamesOf(GatedOperations());

    internal static TheoryData<string, Action<NativeConsumerHandle>> MockUnsupportedOperations()
    {
        (string Topic, int Partition)[] one = NativeConsumer.SnapshotPartitions(new[] { s_tp0 });
        NativeConsumer.TimestampsSnapshot timestamps =
            NativeConsumer.SnapshotTimestamps(new Dictionary<TopicPartition, long> { [s_tp0] = 1 });

        return new TheoryData<string, Action<NativeConsumerHandle>>
        {
            { "Pause", h => h.Pause(one) },
            { "Resume", h => h.Resume(one) },
            { "SeekToBeginning", h => h.SeekToBeginning(one) },
            { "SeekToEnd", h => h.SeekToEnd(one) },
            { "Seek", h => h.Seek(Topic, 0, 5) },
            { "Seek(OffsetAndMetadata)", h => h.SeekWithMetadata(Topic, 0, new OffsetAndMetadata(5, null, "m")) },
            { "Position", h => h.Position(Topic, 0, timeoutMs: null) },
            { "Position(TimeSpan)", h => h.Position(Topic, 0, timeoutMs: 1000) },
            { "Committed", h => h.Committed(one, timeoutMs: null) },
            { "BeginningOffsets", h => h.BeginningOffsets(one, timeoutMs: null) },
            { "EndOffsets", h => h.EndOffsets(one, timeoutMs: null) },
            { "OffsetsForTimes", h => h.OffsetsForTimes(timestamps, timeoutMs: null) },
        };
    }

    [Theory]
    [MemberData(nameof(MockUnsupportedOperationsNames))]
    public void MockDerivedHandle_EveryUnwiredOperation_ThrowsUnsupportedOperationException(string name)
    {
        Action<NativeConsumerHandle> operation = OperationOf(MockUnsupportedOperations(), name);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        UnsupportedOperationException failure = Assert.Throws<UnsupportedOperationException>(() => operation(handle));

        Assert.Equal(MockAsyncUnsupportedMessage, failure.Message);
        Assert.True(failure.InnerException is null, name);
    }

    [Fact]
    public void MockDerivedHandle_TheCommits_CommitIntoTheMock()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
        List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception)> seen =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?)>();
        IOffsetCommitCallback recording = new DelegateCallback((o, e) => seen.Add((o, e)));

        handle.CommitSync(OffsetsSnapshot.Of(Offsets(3)), timeoutMs: null);
        Assert.Equal(3, CommittedOffset(consumer));

        handle.CommitAsync(OffsetsSnapshot.Of(Offsets(4)), (IOffsetCommitCallback?)null);
        Assert.Equal(4, CommittedOffset(consumer));

        handle.CommitAsync(OffsetsSnapshot.Of(Offsets(6)), recording);
        Assert.Equal(6, CommittedOffset(consumer));
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? delivered, Exception? exception) = Assert.Single(seen);
        Assert.Equal(6, Assert.Single(delivered!).Value.Offset());
        Assert.Null(exception);

        consumer.Seek(s_tp0, 7);
        handle.CommitSync(timeoutMs: null);
        Assert.Equal(7, CommittedOffset(consumer));

        consumer.Seek(s_tp0, 8);
        handle.CommitAsync();
        Assert.Equal(8, CommittedOffset(consumer));

        consumer.Seek(s_tp0, 9);
        handle.CommitAsync(recording);
        Assert.Equal(9, CommittedOffset(consumer));
        Assert.Equal(2, seen.Count);
        Assert.Equal(9, Assert.Single(seen[1].Offsets!).Value.Offset());
        Assert.Null(seen[1].Exception);

        Assert.Equal((long?)0, handle.CurrentLag(Topic, 0));
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        handle.Dispose();
        handle.Dispose();
        handle.Dispose();

        Assert.Empty(consumer.Assignment());
    }

    internal static TheoryData<string, Action<NativeConsumerHandle>> GatedOperations()
    {
        TheoryData<string, Action<NativeConsumerHandle>> data = MockUnsupportedOperations();
        data.Add("CurrentLag", h => h.CurrentLag(Topic, 0));
        data.Add("Commit", h => h.CommitSync(timeoutMs: null));
        data.Add("Commit(offsets)", h => h.CommitSync(OffsetsSnapshot.Of(Offsets(7)), timeoutMs: null));
        data.Add("CommitAsync", h => h.CommitAsync());
        data.Add("CommitAsync(callback)", h => h.CommitAsync(new DelegateCallback((_, _) => { })));
        data.Add("CommitAsync(offsets)", h => h.CommitAsync(OffsetsSnapshot.Of(Offsets(7)), (IOffsetCommitCallback?)null));
        data.Add(
            "CommitAsync(offsets, callback)",
            h => h.CommitAsync(OffsetsSnapshot.Of(Offsets(7)), new DelegateCallback((_, _) => { })));
        return data;
    }

    private static TheoryData<string> NamesOf(TheoryData<string, Action<NativeConsumerHandle>> operations)
    {
        TheoryData<string> names = new TheoryData<string>();
        foreach (object[] row in operations)
        {
            names.Add((string)row[0]);
        }

        return names;
    }

    private static Action<NativeConsumerHandle> OperationOf(
        TheoryData<string, Action<NativeConsumerHandle>> operations, string name) =>
        (Action<NativeConsumerHandle>)operations.Single(row => (string)row[0] == name)[1];

    [Theory]
    [MemberData(nameof(GatedOperationsNames))]
    public void EveryBlockingOperation_AfterTheConsumersDestroy_ThrowsIllegalState(string name)
    {
        Action<NativeConsumerHandle> operation = OperationOf(GatedOperations(), name);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Assign(new[] { s_tp0 });
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
        TestTimeout.Dispose(consumer);

        IllegalStateException failure = Assert.Throws<IllegalStateException>(() => operation(handle));
        Assert.Equal(ConsumerDestroyed, failure.Message);
        Assert.NotNull(name);
    }

    [Fact]
    public void KafkaConsumerHandle_AfterTheConsumersDestroy_ThrowsIllegalState()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("handle-after-destroy");
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
        TestTimeout.Run(consumer.Dispose, s_deadline);

        IllegalStateException failure =
            Assert.Throws<IllegalStateException>(() => handle.Position(Topic, 0, timeoutMs: null));
        Assert.Equal(ConsumerDestroyed, failure.Message);
    }

    [Fact]
    public void LiveHandle_DoesNotDeferTheConsumersNativeDestroy()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle native = consumer.Handle;
        NativeConsumerHandle handle = NativeConsumerHandle.Create(consumer);

        TestTimeout.Dispose(consumer);
        Assert.True(native.IsClosed, "a live reentrancy handle must not defer the consumer's destroy");

        handle.Dispose();
    }

    [Fact]
    public void DisposingTheHandleFirst_LeavesTheConsumerAlive_AndItReleasesNormally()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle native = consumer.Handle;
        NativeConsumerHandle handle = NativeConsumerHandle.Create(consumer);

        handle.Dispose();
        Assert.False(native.IsClosed, "disposing a handle must not release its consumer");

        TestTimeout.Dispose(consumer);
        Assert.True(native.IsClosed, "the consumer must release on its own Dispose");
    }

    [Fact]
    public void HandleOutstandingAcrossConsumerDispose_ReadsAndWakesButRefusesTheBlockingOperations()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        TestTimeout.Dispose(consumer);

        Assert.Empty(handle.Assignment());
        Assert.Empty(handle.Paused());
        handle.Wakeup();
        IllegalStateException failure = Assert.Throws<IllegalStateException>(
            () => handle.CommitSync(OffsetsSnapshot.Of(Offsets(1)), timeoutMs: null));
        Assert.Equal(ConsumerDestroyed, failure.Message);

        handle.Dispose();
    }

    [Fact]
    public void ACommitRegistration_ThatAHandleCommitRefusedAfterTheConsumersDestroy_IsReleasedAtOnce()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
        TestTimeout.Dispose(consumer);

        IllegalStateException failure = Assert.Throws<IllegalStateException>(
            () => handle.CommitAsync(OffsetsSnapshot.Of(Offsets(1)), new DelegateCallback((_, _) => { })));

        Assert.Equal(ConsumerDestroyed, failure.Message);
        CommitCallbackRegistration registration = Assert.IsType<CommitCallbackRegistration>(Registration(consumer));
        Assert.True(registration.IsReleased, "the registration of a commit refused after the destroy was kept");
    }

    [Fact]
    public void SyncConsumerDispose_WithALiveHandleOutstanding_ReturnsWithoutHanging()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        handle.Dispose();
    }

    [Fact]
    public async Task AsyncConsumerDisposeAsync_WithALiveHandleOutstanding_ReturnsWithoutHanging()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

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
            using NativeConsumerHandle handle = NativeConsumerHandle.Create(consumer);
            Assert.Empty(handle.Assignment());
        }

        TestTimeout.Dispose(consumer);
        Assert.True(native.IsClosed, "200 handle round-trips must leave the consumer releasable");
    }

    [Theory]
    [MemberData(nameof(GatedOperationsNames))]
    public void KafkaConsumerHandle_OutsideACallback_RefusesEveryBlockingOperation(string name)
    {
        Action<NativeConsumerHandle> operation = OperationOf(GatedOperations(), name);
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("handle-outside-a-callback");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { s_tp0 });
                using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

                ConcurrentModificationException refused =
                    Assert.Throws<ConcurrentModificationException>(() => operation(handle));
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
                using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

                Assert.Equal(new[] { s_tp0, s_tp1 }, handle.Assignment().OrderBy(tp => tp.Partition()));
                Assert.Equal(new[] { s_tp1 }, handle.Paused());

                handle.Wakeup();
                WakeupException woken = Assert.Throws<WakeupException>(() => consumer.Poll(TimeSpan.Zero));
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
                using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
                long? position = null;
                long? lag = -1;
                IReadOnlyCollection<TopicPartition>? paused = null;
                IReadOnlyCollection<TopicPartition>? resumed = null;
                Exception? failure = null;

                consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                {
                    try
                    {
                        handle.Seek(Topic, 0, 5);
                        position = handle.Position(Topic, 0, timeoutMs: null);
                        handle.Pause(NativeConsumer.SnapshotPartitions(new[] { s_tp0 }));
                        paused = handle.Paused();
                        handle.Resume(NativeConsumer.SnapshotPartitions(new[] { s_tp0 }));
                        resumed = handle.Paused();
                        lag = handle.CurrentLag(Topic, 0);
                        handle.CommitAsync(OffsetsSnapshot.Of(Empty()), (IOffsetCommitCallback?)null);
                        handle.CommitSync(OffsetsSnapshot.Of(Empty()), timeoutMs: null);
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
                using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
                TopicPartition unassigned = new TopicPartition("unassigned-parity-topic", 0);
                Exception? viaHandle = null;

                consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                {
                    try
                    {
                        handle.Position(unassigned.Topic(), unassigned.Partition(), timeoutMs: null);
                    }
                    catch (Exception e)
                    {
                        viaHandle = e;
                    }
                }));
                consumer.Commit(Empty());
                Exception viaConsumer = Assert.ThrowsAny<Exception>(() => consumer.Position(unassigned));

                Assert.NotNull(viaHandle);
                Assert.IsType<IllegalStateException>(viaConsumer);
                Assert.Equal(viaConsumer.GetType(), viaHandle!.GetType());
                Assert.Equal(
                    "You can only check the position for partitions assigned to this consumer.", viaHandle.Message);
                Assert.Equal(viaConsumer.Message, viaHandle.Message);
            },
            s_deadline);
    }

    [Fact]
    public void MockDerivedHandle_InsideACommitCallback_CurrentLag_IsANegativeValue_OrTheCoreError()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition unassigned = new TopicPartition("unassigned-lag-topic", 0);
        consumer.Assign(new[] { s_tp0 });
        consumer.UpdateEndOffset(s_tp0.Topic(), s_tp0.Partition(), 2);
        consumer.UpdateEndOffset(unassigned.Topic(), unassigned.Partition(), 2);
        consumer.Seek(s_tp0, 5);
        using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
        long? lag = null;
        Exception? failure = null;

        consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
        {
            lag = handle.CurrentLag(Topic, 0);
            failure = Record.Exception(() => handle.CurrentLag(unassigned.Topic(), unassigned.Partition()));
        }));

        Assert.Equal((long?)-3, lag);
        IllegalArgumentException argument = Assert.IsType<IllegalArgumentException>(failure);
        Assert.Equal("You can only check the position for partitions assigned to this consumer.", argument.Message);
    }

    [Fact]
    public void KafkaConsumerHandle_InsideACommitCallback_CurrentLag_OfAnUnassignedPartition_ThrowsTheCoreError()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer("handle-lag-failure");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { s_tp0 });
                using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);
                TopicPartition unassigned = new TopicPartition(Topic, 7);
                long? lag = -1;
                Exception? failure = null;

                consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                {
                    lag = handle.CurrentLag(Topic, 0);
                    failure = Record.Exception(() => handle.CurrentLag(unassigned.Topic(), unassigned.Partition()));
                }));
                consumer.Commit(Empty());

                Assert.Null(lag);
                IllegalStateException invalid = Assert.IsType<IllegalStateException>(failure);
                Assert.Equal($"No current assignment for partition {unassigned}", invalid.Message);
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
                using NativeConsumerHandle handle = CoreConsumerHandles.Of(consumer);

                TopicPartition viaHandle = Assert.Single(handle.Assignment());
                Assert.Equal(NonAsciiTopic, viaHandle.Topic());
                Assert.Equal(3, viaHandle.Partition());
                Assert.Equal(consumer.Assignment(), handle.Assignment());
            },
            s_deadline);
    }

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>("latest");

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>("latest");

    private static Dictionary<string, object?> Config(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    private static KafkaConsumer<byte[], byte[]> NewKafkaConsumer(string groupId) =>
        KafkaConsumer<byte[], byte[]>.Create(Config(groupId), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static long CommittedOffset(MockConsumer<byte[], byte[]> consumer) =>
        consumer.Committed(new[] { s_tp0 })[s_tp0]!.Offset();

    private static CommitCallbackRegistration? Registration(object consumer) =>
        CoreConsumerHandles.NativeOf(consumer).CurrentCommitCallbackRegistration;

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
