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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The rebalance listener through the public API: the <c>Subscribe(topics, listener)</c> overloads on the four
/// consumers, the mocks' <c>Rebalance</c> and <c>LosePartitions</c>, a throwing listener, the async listener, and a
/// rebalance that does not advance until the listener returns.
/// </summary>
public sealed class PublicConsumerRebalanceListenerTests
{
    private const string Topic = "rebalance-listener-topic";

    private const string MockHandleUnsupported =
        "ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer directly.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // Used only for a "has not completed" wait taken after the callback has fired.
    private static readonly TimeSpan s_notYetWindow = TimeSpan.FromMilliseconds(500);

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    [Fact]
    public void ConsumerRebalanceListenerBase_OnPartitionsLost_DelegatesToRevoked()
    {
        // Java's onPartitionsLost default lives on the base class, as netstandard2.0 has no default interface methods.
        RecordingListenerBase listener = new RecordingListenerBase();
        TopicPartition[] partitions = { new TopicPartition(Topic, 1) };

        listener.OnPartitionsLost(partitions);

        Assert.Equal(partitions, Assert.Single(listener.Revoked));
        Assert.Empty(listener.Assigned);
    }

    [Fact]
    public void SyncMock_SubscribeWithListener_ThenRebalance_FiresAssignedWithAddedPartitions()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) });

        Assert.Empty(listener.Revoked);
        TopicPartition[] assigned = Assert.Single(listener.Assigned).OrderBy(tp => tp.Partition).ToArray();
        Assert.Equal(
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) },
            assigned);
        Assert.Equal(2, consumer.Assignment().Count);
    }

    [Fact]
    public async Task AsyncMock_SubscribeWithListener_ThenRebalance_FiresAssignedWithAddedPartitions()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        RecordingListener listener = new RecordingListener();
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, listener), s_deadline);

        await TestTimeout.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }), s_deadline);

        Assert.Equal(new[] { new TopicPartition(Topic, 0) }, Assert.Single(listener.Assigned));
    }

    [Fact]
    public void SyncKafkaConsumer_ExposesTheListenerOverload()
    {
        // The real client needs a broker, so this test checks only the overload's shape.
        Assert.NotNull(
            typeof(IConsumer<byte[], byte[]>).GetMethod(
                nameof(IConsumer<byte[], byte[]>.Subscribe),
                new[] { typeof(IReadOnlyCollection<string>), typeof(IConsumerRebalanceListener) }));
        Assert.NotNull(
            typeof(KafkaConsumer<byte[], byte[]>).GetMethod(
                nameof(KafkaConsumer<byte[], byte[]>.Subscribe),
                new[] { typeof(IReadOnlyCollection<string>), typeof(IConsumerRebalanceListener) }));
    }

    [Fact]
    public void AsyncKafkaConsumer_ExposesTheListenerOverload()
    {
        Assert.NotNull(
            typeof(IAsyncConsumer<byte[], byte[]>).GetMethod(
                nameof(IAsyncConsumer<byte[], byte[]>.Subscribe),
                new[]
                {
                    typeof(IReadOnlyCollection<string>),
                    typeof(IConsumerRebalanceListener),
                    typeof(CancellationToken),
                }));
        Assert.NotNull(
            typeof(AsyncKafkaConsumer<byte[], byte[]>).GetMethod(
                nameof(AsyncKafkaConsumer<byte[], byte[]>.Subscribe),
                new[]
                {
                    typeof(IReadOnlyCollection<string>),
                    typeof(IConsumerRebalanceListener),
                    typeof(CancellationToken),
                }));
    }

    [Fact]
    public void Rebalance_FiresRevokedOnlyWhenSomethingWasRemoved()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) });
        Assert.Empty(listener.Revoked);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(new[] { new TopicPartition(Topic, 1) }, Assert.Single(listener.Revoked));
    }

    [Fact]
    public void Rebalance_FiresAssignedUnconditionally_WithTheAddedListWhichMayBeEmpty()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(2, listener.Assigned.Count);
        Assert.Empty(listener.Assigned[1]);
        Assert.Empty(listener.Revoked);
    }

    [Fact]
    public void Rebalance_NeverFiresLost()
    {
        // As in Java's MockConsumer, only LosePartitions fires the lost callback.
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        consumer.Rebalance(Array.Empty<TopicPartition>());

        Assert.Empty(listener.Lost);
    }

    [Fact]
    public void Rebalance_WithoutAListener_IsANoOpSuccess()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic });

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(new[] { new TopicPartition(Topic, 0) }, consumer.Assignment().ToArray());
    }

    [Fact]
    public void Rebalance_OnAManuallyAssignedConsumer_ThrowsWithTheJavaMessage()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, 0) });

        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        Assert.Equal(
            "Attempt to dynamically assign partitions while manual assignment in use",
            exception.Message);
    }

    [Fact]
    public void LosePartitions_CallsOnPartitionsLost()
    {
        // Java testLosePartitionsCallsOnPartitionsLost.
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { tp0, tp1 });
        consumer.LosePartitions(new[] { tp0 });

        Assert.Equal(new[] { tp0 }, Assert.Single(listener.Lost));
        Assert.Empty(listener.Revoked);
    }

    [Fact]
    public void LosePartitions_RemovesFromAssignment()
    {
        // Java testLosePartitionsRemovesFromAssignment.
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic });

        consumer.Rebalance(new[] { tp0, tp1 });
        consumer.LosePartitions(new[] { tp0 });

        Assert.DoesNotContain(tp0, consumer.Assignment());
        Assert.Contains(tp1, consumer.Assignment());
    }

    [Fact]
    public void LosePartitions_ThrowsIfNotAssigned_WithTheJavaMessage()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic });
        consumer.Rebalance(new[] { tp0 });

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => consumer.LosePartitions(new[] { tp1 }));

        Assert.Equal(
            "Cannot lose partitions that are not currently assigned: [rebalance-listener-topic-1]",
            exception.Message);
        Assert.Equal(new[] { tp0 }, consumer.Assignment().ToArray());
    }

    [Fact]
    public void LosePartitions_ClearsOnlyLostRecords()
    {
        // Java testLosePartitionsClearsOnlyLostRecords.
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic });
        consumer.Rebalance(new[] { tp0, tp1 });
        consumer.UpdateBeginningOffset(Topic, 0, 0);
        consumer.UpdateBeginningOffset(Topic, 1, 0);
        consumer.Seek(tp0, 0);
        consumer.Seek(tp1, 0);
        consumer.AddRecord(Topic, 0, 0, null, null);
        consumer.AddRecord(Topic, 1, 0, null, null);

        consumer.LosePartitions(new[] { tp0 });

        ConsumerRecords<byte[], byte[]> records = null!;
        TestTimeout.Run(() => records = consumer.Poll(TimeSpan.FromMilliseconds(1)), s_deadline);
        ConsumerRecord<byte[], byte[]> record = Assert.Single(records);
        Assert.Equal(tp1, new TopicPartition(record.Topic, record.Partition));
    }

    [Fact]
    public void LosePartitions_ThenRebalance()
    {
        // Java testLosePartitionsThenRebalance.
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        TopicPartition tp2 = new TopicPartition(Topic, 2);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListenerBase listener = new RecordingListenerBase();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { tp0, tp1 });
        listener.Assigned.Clear();

        consumer.LosePartitions(new[] { tp0 });
        consumer.Rebalance(new[] { tp1, tp2 });

        Assert.Equal(new[] { tp2 }, Assert.Single(listener.Assigned));
        Assert.Equal(new[] { tp1, tp2 }, consumer.Assignment().OrderBy(tp => tp.Partition).ToArray());
    }

    [Fact]
    public void LosePartitions_WithAListenerBase_FallsBackToRevoked_JavasDefault()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListenerBase listener = new RecordingListenerBase();
        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { tp0, new TopicPartition(Topic, 1) });

        consumer.LosePartitions(new[] { tp0 });

        Assert.Equal(new[] { tp0 }, Assert.Single(listener.Revoked));
    }

    [Fact]
    public void LosePartitions_DeduplicatesThePartitions()
    {
        // Java collects the argument into a HashSet before calling onPartitionsLost.
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { tp0 });

        consumer.LosePartitions(new[] { tp0, tp0 });

        Assert.Equal(new[] { tp0 }, Assert.Single(listener.Lost));
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public async Task AsyncMock_LosePartitions_CallsOnPartitionsLost()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        RecordingListener listener = new RecordingListener();
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, listener), s_deadline);
        await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0, tp1 }), s_deadline);

        await TestTimeout.Run(() => consumer.LosePartitions(new[] { tp1 }), s_deadline);

        Assert.Equal(new[] { tp1 }, Assert.Single(listener.Lost));
        Assert.Empty(listener.Revoked);
        Assert.Equal(new[] { tp0 }, consumer.Assignment().ToArray());
    }

    [Fact]
    public void LosePartitions_WhenTheListenerThrows_ThrowsTheSameInstance_AndKeepsTheAssignment()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ThrowingOnLostListener listener = new ThrowingOnLostListener("lost handler failed — 失敗");
        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { tp0 });

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => consumer.LosePartitions(new[] { tp0 }));

        Assert.Same(listener.Thrown, exception);
        Assert.Equal("lost handler failed — 失敗", exception.Message);
        Assert.Equal(new[] { tp0 }, consumer.Assignment().ToArray());
    }

    [Fact]
    public void LosePartitions_NullPartitions_ThrowsArgumentNullException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
            () => consumer.LosePartitions(null!));

        Assert.Equal("partitions", exception.ParamName);
    }

    [Fact]
    public void Rebalance_WhenTheListenerThrows_ThrowsTheSameInstance()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ThrowingOnAssignedListener listener = new ThrowingOnAssignedListener("revoke handler failed — 失敗");
        consumer.Subscribe(new[] { Topic }, listener);

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        Assert.Same(listener.Thrown, exception);
        Assert.Equal("revoke handler failed — 失敗", exception.Message);
    }

    [Fact]
    public void Rebalance_AfterAThrowingListener_TheConsumerStaysUsable()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new ThrowingOnAssignedListener("boom"));
        Assert.Throws<InvalidOperationException>(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { new TopicPartition(Topic, 2) });

        Assert.Single(listener.Assigned);
    }

    [Fact]
    public async Task Rebalance_DoesNotAdvanceUntilTheListenerReturns()
    {
        // The listener is known to be inside the callback before the check, so the check cannot pass vacuously.
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new BlockingListener(entered, release));

        Task rebalance = Task.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        Assert.True(entered.Wait(s_deadline), "the listener callback never fired");

        await Task.WhenAny(rebalance, Task.Delay(s_notYetWindow));
        Assert.False(
            rebalance.IsCompleted,
            "Rebalance returned while the listener was still blocked — the rebalance advanced early.");

        release.Set();

        await TestTimeout.Run(() => rebalance, s_deadline);
    }

    [Fact]
    public void SyncMock_AListener_CallsBackIntoTheMock()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        try
        {
            List<string> seen = new List<string>();
            consumer.Subscribe(new[] { Topic }, new DelegateListener(assigned: _ =>
            {
                seen.Add("Assignment: " + Outcome(() => Format(consumer.Assignment())));
                seen.Add("Seek: " + Outcome(() =>
                {
                    consumer.Seek(tp0, 4);
                    return null;
                }));
                seen.Add("Commit: " + Outcome(() =>
                {
                    consumer.Commit(Offsets(tp0, 4));
                    return null;
                }));
                seen.Add("Position: " + Outcome(() => consumer.Position(tp0).ToString(CultureInfo.InvariantCulture)));
            }));

            consumer.Rebalance(new[] { tp0 });

            Assert.Equal(
                new[]
                {
                    "Assignment: ok [" + Topic + "-0]",
                    "Seek: NotSupportedException: " + MockHandleUnsupported,
                    "Commit: ok",
                    "Position: NotSupportedException: " + MockHandleUnsupported,
                },
                seen);
            Assert.Equal(new OffsetAndMetadata(4), consumer.Committed(new[] { tp0 })[tp0]);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void SyncMock_ACommitInsideOnPartitionsRevoked_Succeeds()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        try
        {
            consumer.Subscribe(new[] { Topic }, new DelegateListener(revoked: _ => consumer.Commit(Offsets(tp0, 7))));

            consumer.Rebalance(new[] { tp0 });
            consumer.Rebalance(Array.Empty<TopicPartition>());
            consumer.Rebalance(new[] { tp0 });

            Assert.Equal(new OffsetAndMetadata(7), consumer.Committed(new[] { tp0 })[tp0]);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AsyncListener_FiresEachMethod()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        AsyncRecordingListener listener = new AsyncRecordingListener();
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, listener), s_deadline);

        await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0, tp1 }), s_deadline);
        await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0 }), s_deadline);
        await TestTimeout.Run(() => consumer.LosePartitions(new[] { tp0 }), s_deadline);

        Assert.Equal(new[] { tp0, tp1 }, listener.Assigned[0].OrderBy(tp => tp.Partition).ToArray());
        Assert.Empty(listener.Assigned[1]);
        Assert.Equal(new[] { tp1 }, Assert.Single(listener.Revoked));
        Assert.Equal(new[] { tp0 }, Assert.Single(listener.Lost));
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public async Task AsyncMock_AsyncListener_RebalanceDoesNotAdvanceUntilItsTaskEnds()
    {
        TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, new PendingAsyncListener(entered, release.Task)), s_deadline);

        Task rebalance = consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.True(entered.Wait(s_deadline), "the listener callback never fired");
        await Task.WhenAny(rebalance, Task.Delay(s_notYetWindow));
        Assert.False(
            rebalance.IsCompleted,
            "Rebalance completed while the listener's Task was still pending — the rebalance advanced early.");

        release.SetResult(true);

        await TestTimeout.Run(() => rebalance, s_deadline);
        Assert.Equal(new[] { new TopicPartition(Topic, 0) }, consumer.Assignment().ToArray());
    }

    [Fact]
    public async Task AsyncMock_AsyncListener_DoesNotBlockTheDrainThreadWhileItsTaskRuns()
    {
        TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, new PendingAsyncListener(entered, release.Task)), s_deadline);
        Task rebalance = consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        try
        {
            Assert.True(entered.Wait(s_deadline), "the listener callback never fired");

            Exception? other = await Record.ExceptionAsync(
                () => TestTimeout.Run(() => consumer.Pause(new[] { new TopicPartition(Topic, 0) }), s_deadline));

            Assert.False(rebalance.IsCompleted, "the rebalance ended before the listener's Task did");
            InvalidOperationException rejected = Assert.IsType<InvalidOperationException>(other);
            Assert.Equal("KafkaConsumer is not safe for multi-threaded access.", rejected.Message);
        }
        finally
        {
            release.TrySetResult(true);
        }

        await TestTimeout.Run(() => rebalance, s_deadline);
    }

    [Fact]
    public async Task AsyncMock_AsyncListener_RefusesOtherCallsWhileItRuns_ButNotWakeup()
    {
        const string concurrent = "KafkaConsumer is not safe for multi-threaded access.";
        TopicPartition partition = new TopicPartition(Topic, 0);
        TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        IReadOnlyCollection<TopicPartition>? inside = null;
        PendingAsyncListener listener = new PendingAsyncListener(entered, release.Task, async () =>
        {
            await consumer.Commit(Offsets(partition, 3));
            inside = consumer.Assignment();
        });
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, listener), s_deadline);
        Task rebalance = consumer.Rebalance(new[] { partition });
        try
        {
            Assert.True(entered.Wait(s_deadline), "the listener callback never fired");

            InvalidOperationException close = await Assert.ThrowsAsync<InvalidOperationException>(
                () => TestTimeout.RunOffThisThread(() => consumer.Close(), s_deadline));
            Assert.Equal(concurrent, close.Message);
            InvalidOperationException poll = await Assert.ThrowsAsync<InvalidOperationException>(
                () => TestTimeout.RunOffThisThread(() => consumer.Poll(TimeSpan.Zero), s_deadline));
            Assert.Equal(concurrent, poll.Message);
            InvalidOperationException assignment = await Assert.ThrowsAsync<InvalidOperationException>(
                () => TestTimeout.RunOffThisThread(
                    () =>
                    {
                        consumer.Assignment();
                        return Task.CompletedTask;
                    },
                    s_deadline));
            Assert.Equal(concurrent, assignment.Message);
            InvalidOperationException nested = await Assert.ThrowsAsync<InvalidOperationException>(
                () => TestTimeout.RunOffThisThread(() => consumer.Rebalance(Array.Empty<TopicPartition>()), s_deadline));
            Assert.Equal(concurrent, nested.Message);
            consumer.Wakeup();
        }
        finally
        {
            release.TrySetResult(true);
        }

        await TestTimeout.Run(() => rebalance, s_deadline);
        Assert.Equal(new[] { partition }, inside);
        Assert.Equal(new[] { partition }, consumer.Assignment());
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
            await TestTimeout.Run(() => consumer.Committed(new[] { partition }), s_deadline);
        Assert.Equal(new OffsetAndMetadata(3), committed[partition]);
        KafkaException woken = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        Assert.Equal(-18, woken.Code);
        await TestTimeout.Run(() => consumer.Close(), s_deadline);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public async Task AsyncMock_ACommitInsideAnAsyncOnPartitionsRevoked_Succeeds()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        try
        {
            await TestTimeout.Run(
                () => consumer.Subscribe(new[] { Topic }, new AsyncDelegateListener(revoked: async _ =>
                {
                    await Task.Yield();
                    await consumer.Commit(Offsets(tp0, 7));
                })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0 }), s_deadline);
            await TestTimeout.Run(() => consumer.Rebalance(Array.Empty<TopicPartition>()), s_deadline);
            await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0 }), s_deadline);

            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                await TestTimeout.Run(() => consumer.Committed(new[] { tp0 }), s_deadline);
            Assert.Equal(new OffsetAndMetadata(7), committed[tp0]);
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
        finally
        {
            await TestTimeout.RunOffThisThread(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_LosePartitions_AwaitsAnAsyncListener_ThatReadsTheAssignment()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        TopicPartition tp1 = new TopicPartition(Topic, 1);
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        try
        {
            List<TopicPartition[]> lost = new List<TopicPartition[]>();
            IReadOnlyCollection<TopicPartition>? inside = null;
            await TestTimeout.Run(
                () => consumer.Subscribe(new[] { Topic }, new AsyncDelegateListener(lost: async partitions =>
                {
                    await Task.Yield();
                    lost.Add(partitions.ToArray());
                    inside = consumer.Assignment();
                })),
                s_deadline);
            await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0, tp1 }), s_deadline);

            await TestTimeout.Run(() => consumer.LosePartitions(new[] { tp0 }), s_deadline);

            Assert.Equal(new[] { tp0 }, Assert.Single(lost));
            Assert.Equal(new[] { tp0, tp1 }, inside!.OrderBy(tp => tp.Partition).ToArray());
            Assert.Equal(new[] { tp1 }, consumer.Assignment().ToArray());
        }
        finally
        {
            await TestTimeout.RunOffThisThread(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncMock_AsyncListener_FaultedTask_FaultsTheRebalanceWithTheSameInstance()
    {
        InvalidOperationException thrown = new InvalidOperationException("async assigned failed — 失敗");
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, new AsyncThrowingListener(thrown)), s_deadline);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }), s_deadline));

        Assert.Same(thrown, exception);
    }

    [Fact]
    public async Task AsyncMock_AsyncListener_CanceledTask_FaultsTheRebalanceWithTheCancellation()
    {
        OperationCanceledException thrown = new OperationCanceledException("async assigned canceled");
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, new AsyncThrowingListener(thrown)), s_deadline);

        OperationCanceledException exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }), s_deadline));

        Assert.Same(thrown, exception);
    }

    [Fact]
    public async Task AsyncMock_AsyncListenerBase_LosePartitions_FallsBackToRevoked_JavasDefault()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        AsyncRecordingListenerBase listener = new AsyncRecordingListenerBase();
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, listener), s_deadline);
        await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0, new TopicPartition(Topic, 1) }), s_deadline);

        await TestTimeout.Run(() => consumer.LosePartitions(new[] { tp0 }), s_deadline);

        Assert.Equal(new[] { tp0 }, Assert.Single(listener.Revoked));
    }

    [Fact]
    public async Task AsyncMock_Rebalance_OnAManuallyAssignedConsumer_FaultsWithTheJavaMessage()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Assign(new[] { new TopicPartition(Topic, 0) }), s_deadline);

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(
            () => TestTimeout.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }), s_deadline));

        Assert.Equal(
            "Attempt to dynamically assign partitions while manual assignment in use",
            exception.Message);
    }

    [Fact]
    public async Task AsyncMock_LosePartitions_ThrowsIfNotAssigned_WithTheJavaMessage()
    {
        TopicPartition tp0 = new TopicPartition(Topic, 0);
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }), s_deadline);
        await TestTimeout.Run(() => consumer.Rebalance(new[] { tp0 }), s_deadline);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => consumer.LosePartitions(new[] { new TopicPartition(Topic, 1) }), s_deadline));

        Assert.Equal(
            "Cannot lose partitions that are not currently assigned: [rebalance-listener-topic-1]",
            exception.Message);
        Assert.Equal(new[] { tp0 }, consumer.Assignment().ToArray());
    }

    [Fact]
    public async Task AsyncConsumerRebalanceListenerBase_OnPartitionsLost_DelegatesToRevoked()
    {
        AsyncRecordingListenerBase listener = new AsyncRecordingListenerBase();
        TopicPartition[] partitions = { new TopicPartition(Topic, 1) };

        await listener.OnPartitionsLost(partitions);

        Assert.Equal(partitions, Assert.Single(listener.Revoked));
        Assert.Empty(listener.Assigned);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncMock_Listener_RunsOnTheDrainThread(bool asyncListener)
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        ThreadRecordingListener listener = new ThreadRecordingListener();
        await TestTimeout.Run(
            () => asyncListener
                ? consumer.Subscribe(new[] { Topic }, (IAsyncConsumerRebalanceListener)listener)
                : consumer.Subscribe(new[] { Topic }, (IConsumerRebalanceListener)listener),
            s_deadline);

        await TestTimeout.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }), s_deadline);

        Assert.Equal("confluent-kafka-consumer-pump", Assert.Single(listener.ThreadNames));
    }

    [Fact]
    public void SyncMock_Listener_RunsOnTheCallersThread()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ThreadRecordingListener listener = new ThreadRecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(Environment.CurrentManagedThreadId, Assert.Single(listener.ThreadIds));
    }

    [Fact]
    public void AsyncKafkaConsumer_ExposesTheAsyncListenerOverload()
    {
        Type[] parameters =
        {
            typeof(IReadOnlyCollection<string>),
            typeof(IAsyncConsumerRebalanceListener),
            typeof(CancellationToken),
        };

        Assert.NotNull(typeof(IAsyncConsumer<byte[], byte[]>).GetMethod(nameof(IAsyncConsumer<byte[], byte[]>.Subscribe), parameters));
        Assert.NotNull(typeof(AsyncKafkaConsumer<byte[], byte[]>).GetMethod(nameof(AsyncKafkaConsumer<byte[], byte[]>.Subscribe), parameters));
        Assert.NotNull(typeof(AsyncMockConsumer<byte[], byte[]>).GetMethod(nameof(AsyncMockConsumer<byte[], byte[]>.Subscribe), parameters));
    }

    [Fact]
    public void Subscribe_NullTopics_ThrowsArgumentNullException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
            () => consumer.Subscribe(null!, new RecordingListener()));

        Assert.Equal("topics", exception.ParamName);
    }

    [Fact]
    public void Subscribe_NullListener_ThrowsArgumentNullException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
            () => consumer.Subscribe(new[] { Topic }, null!));

        Assert.Equal("listener", exception.ParamName);
    }

    [Fact]
    public void Subscribe_NullTopicName_ThrowsArgumentException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => consumer.Subscribe(new string?[] { null }!, new RecordingListener()));

        Assert.Equal("topics", exception.ParamName);
        Assert.Contains("Topic names must not be null.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Subscribe_WithListener_AfterDispose_ThrowsObjectDisposedException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(
            () => consumer.Subscribe(new[] { Topic }, new RecordingListener()));
    }

    [Fact]
    public async Task AsyncSubscribe_NullListener_ThrowsArgumentNullException()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);

        ArgumentNullException exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.Subscribe(new[] { Topic }, (IConsumerRebalanceListener)null!));

        Assert.Equal("listener", exception.ParamName);
    }

    [Fact]
    public async Task AsyncSubscribe_NullAsyncListener_ThrowsArgumentNullException()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);

        ArgumentNullException exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.Subscribe(new[] { Topic }, (IAsyncConsumerRebalanceListener)null!));

        Assert.Equal("listener", exception.ParamName);
    }

    [Fact]
    public void Rebalance_NullPartitions_ThrowsArgumentNullException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
            () => consumer.Rebalance(null!));

        Assert.Equal("partitions", exception.ParamName);
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(TopicPartition partition, long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(offset) };

    private static string Outcome(Func<string?> call)
    {
        try
        {
            string? result = call();
            return result is null ? "ok" : "ok " + result;
        }
        catch (KafkaException e)
        {
            return "KafkaException " + e.Code + ": " + e.Message;
        }
        catch (Exception e)
        {
            return e.GetType().Name + ": " + e.Message;
        }
    }

    private static string Format(IReadOnlyCollection<TopicPartition> partitions) =>
        "[" + string.Join(", ", partitions.Select(tp => tp.Topic + "-" + tp.Partition)) + "]";

    /// <summary>A listener that runs the actions it is given for revoked and assigned partitions.</summary>
    private sealed class DelegateListener : IConsumerRebalanceListener
    {
        private readonly Action<IReadOnlyCollection<TopicPartition>>? _revoked;
        private readonly Action<IReadOnlyCollection<TopicPartition>>? _assigned;

        internal DelegateListener(
            Action<IReadOnlyCollection<TopicPartition>>? revoked = null,
            Action<IReadOnlyCollection<TopicPartition>>? assigned = null)
        {
            _revoked = revoked;
            _assigned = assigned;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => _revoked?.Invoke(partitions);

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _assigned?.Invoke(partitions);

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>An async listener that runs the functions it is given for revoked and lost partitions.</summary>
    private sealed class AsyncDelegateListener : IAsyncConsumerRebalanceListener
    {
        private readonly Func<IReadOnlyCollection<TopicPartition>, Task>? _revoked;
        private readonly Func<IReadOnlyCollection<TopicPartition>, Task>? _lost;

        internal AsyncDelegateListener(
            Func<IReadOnlyCollection<TopicPartition>, Task>? revoked = null,
            Func<IReadOnlyCollection<TopicPartition>, Task>? lost = null)
        {
            _revoked = revoked;
            _lost = lost;
        }

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            _revoked?.Invoke(partitions) ?? Task.CompletedTask;

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            _lost?.Invoke(partitions) ?? Task.CompletedTask;
    }

    /// <summary>Records the partitions each listener method receives.</summary>
    private sealed class RecordingListener : IConsumerRebalanceListener
    {
        internal List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        internal List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        internal List<TopicPartition[]> Lost { get; } = new List<TopicPartition[]>();

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            Revoked.Add(partitions.ToArray());

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            Assigned.Add(partitions.ToArray());

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            Lost.Add(partitions.ToArray());
    }

    /// <summary>Records the partitions each method receives, with the base's default for lost partitions.</summary>
    private sealed class RecordingListenerBase : ConsumerRebalanceListenerBase
    {
        internal List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        internal List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        public override void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            Revoked.Add(partitions.ToArray());

        public override void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            Assigned.Add(partitions.ToArray());
    }

    /// <summary>A listener whose assigned method throws an InvalidOperationException.</summary>
    private sealed class ThrowingOnAssignedListener : IConsumerRebalanceListener
    {
        internal ThrowingOnAssignedListener(string message)
        {
            Thrown = new InvalidOperationException(message);
        }

        internal InvalidOperationException Thrown { get; }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            throw Thrown;

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>A listener whose lost method throws an InvalidOperationException.</summary>
    private sealed class ThrowingOnLostListener : IConsumerRebalanceListener
    {
        internal ThrowingOnLostListener(string message)
        {
            Thrown = new InvalidOperationException(message);
        }

        internal InvalidOperationException Thrown { get; }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            throw Thrown;
    }

    /// <summary>An async listener that records, after a yield, the partitions each method receives.</summary>
    private sealed class AsyncRecordingListener : IAsyncConsumerRebalanceListener
    {
        internal List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        internal List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        internal List<TopicPartition[]> Lost { get; } = new List<TopicPartition[]>();

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Record(Revoked, partitions);

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Record(Assigned, partitions);

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Record(Lost, partitions);

        private static async Task Record(List<TopicPartition[]> calls, IReadOnlyCollection<TopicPartition> partitions)
        {
            TopicPartition[] copied = partitions.ToArray();
            await Task.Yield();
            lock (calls)
            {
                calls.Add(copied);
            }
        }
    }

    /// <summary>An async listener with the base's default for lost partitions, recording after a yield.</summary>
    private sealed class AsyncRecordingListenerBase : AsyncConsumerRebalanceListenerBase
    {
        internal List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        internal List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        public override async Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
            TopicPartition[] copied = partitions.ToArray();
            await Task.Yield();
            Revoked.Add(copied);
        }

        public override async Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            TopicPartition[] copied = partitions.ToArray();
            await Task.Yield();
            Assigned.Add(copied);
        }
    }

    /// <summary>An async listener whose assigned method signals, then stays pending until released.</summary>
    private sealed class PendingAsyncListener : IAsyncConsumerRebalanceListener
    {
        private readonly ManualResetEventSlim _entered;
        private readonly Task _release;
        private readonly Func<Task>? _afterRelease;

        internal PendingAsyncListener(ManualResetEventSlim entered, Task release, Func<Task>? afterRelease = null)
        {
            _entered = entered;
            _release = release;
            _afterRelease = afterRelease;
        }

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            _entered.Set();
            return _afterRelease is null ? _release : ReleasedThenAfter();
        }

        private async Task ReleasedThenAfter()
        {
            await _release;
            await _afterRelease!();
        }

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }

    /// <summary>An async listener whose assigned method throws, after a yield, the exception it is given.</summary>
    private sealed class AsyncThrowingListener : IAsyncConsumerRebalanceListener
    {
        private readonly Exception _thrown;

        internal AsyncThrowingListener(Exception thrown)
        {
            _thrown = thrown;
        }

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public async Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            await Task.Yield();
            throw _thrown;
        }

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }

    /// <summary>Records the name and id of the thread each assigned call runs on.</summary>
    private sealed class ThreadRecordingListener : IConsumerRebalanceListener, IAsyncConsumerRebalanceListener
    {
        internal List<string?> ThreadNames { get; } = new List<string?>();

        internal List<int> ThreadIds { get; } = new List<int>();

        void IConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        void IConsumerRebalanceListener.OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => Record();

        void IConsumerRebalanceListener.OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        Task IAsyncConsumerRebalanceListener.OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            Task.CompletedTask;

        Task IAsyncConsumerRebalanceListener.OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            Record();
            return Task.CompletedTask;
        }

        Task IAsyncConsumerRebalanceListener.OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            Task.CompletedTask;

        private void Record()
        {
            ThreadNames.Add(Thread.CurrentThread.Name);
            ThreadIds.Add(Environment.CurrentManagedThreadId);
        }
    }

    /// <summary>Signals that the callback was entered, then blocks inside it until released.</summary>
    private sealed class BlockingListener : IConsumerRebalanceListener
    {
        private readonly ManualResetEventSlim _entered;
        private readonly ManualResetEventSlim _release;

        internal BlockingListener(ManualResetEventSlim entered, ManualResetEventSlim release)
        {
            _entered = entered;
            _release = release;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            _entered.Set();
            _release.Wait();
        }

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }
}
