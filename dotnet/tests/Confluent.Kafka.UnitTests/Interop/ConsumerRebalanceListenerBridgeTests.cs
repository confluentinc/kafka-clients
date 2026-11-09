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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The rebalance-listener registration: each method driven through the core's listener invokers, its report,
/// and when the registration's <c>self_</c> is released.
/// </summary>
public sealed class ConsumerRebalanceListenerBridgeTests
{
    private const string Topic = "listener-bridge-topic";

    private const string MutuallyExclusive = "Subscription to topics, partitions and pattern are mutually exclusive";

    private const string GroupIdNotDefined =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_notYetWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan s_settle = TimeSpan.FromSeconds(2);

    /// <summary>The listener method a theory drives.</summary>
    public enum Method
    {
        Revoked,
        Assigned,
        Lost,
    }

    /// <summary>The subscribe form a theory drives: sync, or async with a listener of either kind.</summary>
    public enum Form
    {
        Sync,
        Async,
        AsyncWithAnAsyncListener,
    }

    [Fact]
    public void PartitionsLost_OnListenerBase_DelegatesToRevoked_JavaDefault()
    {
        RecordingListenerBase listener = new RecordingListenerBase();

        Exception? failure = Invoke(
            ListenerRegistration.Root(listener, Reporter()),
            Method.Lost,
            new TopicPartition(Topic, 0),
            new TopicPartition(Topic, 3));

        Assert.Null(failure);
        Assert.Empty(listener.Lost);
        TopicPartition[] revoked = Assert.Single(listener.Revoked).OrderBy(tp => tp.Partition()).ToArray();
        Assert.Equal(new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 3) }, revoked);
    }

    [Fact]
    public void PartitionsLost_OnDirectInterfaceImplementation_DeliversToLost()
    {
        RecordingListener listener = new RecordingListener();

        Exception? failure = Invoke(
            ListenerRegistration.Root(listener, Reporter()), Method.Lost, new TopicPartition(Topic, 7));

        Assert.Null(failure);
        Assert.Empty(listener.Revoked);
        Assert.Equal(new[] { new TopicPartition(Topic, 7) }, Assert.Single(listener.Lost));
    }

    [Fact]
    public void ListenerCallback_NonAsciiTopic_RoundTripsThroughDeliveredPartitions()
    {
        const string NonAscii = "témas-日本語-🎉";
        RecordingListener listener = new RecordingListener();

        Exception? failure = Invoke(
            ListenerRegistration.Root(listener, Reporter()), Method.Assigned, new TopicPartition(NonAscii, 2));

        Assert.Null(failure);
        TopicPartition delivered = Assert.Single(Assert.Single(listener.Assigned));
        Assert.Equal(NonAscii, delivered.Topic());
        Assert.Equal(2, delivered.Partition());
    }

    [Theory]
    [InlineData(Method.Revoked)]
    [InlineData(Method.Assigned)]
    [InlineData(Method.Lost)]
    public void ThrowingListener_DoesNotUnwind_ReportsTheSameInstance(Method method)
    {
        InvalidOperationException thrown = new InvalidOperationException("listener blew up — 例外");
        ConsumerCallbackReporter reporter = Reporter();

        Exception? failure = Invoke(
            ListenerRegistration.Root(new ThrowingListener(thrown), reporter), method, new TopicPartition(Topic, 0));

        Assert.Same(thrown, failure);
        Assert.False(reporter.InCallback);
    }

    [Fact]
    public void ListenerRegistration_ReleasedTwice_FreesExactlyOnce()
    {
        ListenerRegistration registration = ListenerRegistration.Root(new RecordingListener(), Reporter());

        registration.Release();
        registration.Release();

        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void ListenerRegistration_IsInsideACallbackOnlyWhileTheListenerRuns()
    {
        ConsumerCallbackReporter reporter = Reporter();
        bool? inside = null;
        Probe listener = new Probe(() => inside = reporter.InCallback);

        Exception? failure = Invoke(
            ListenerRegistration.Root(listener, reporter), Method.Assigned, new TopicPartition(Topic, 0));

        Assert.Null(failure);
        Assert.True(inside);
        Assert.False(reporter.InCallback);
    }

    [Fact]
    public async Task AsyncListener_IsReportedWhenItsTaskEnds_NotWhenItReturns()
    {
        TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        ManualResetEventSlim entered = new ManualResetEventSlim();
        AsyncProbe listener = new AsyncProbe(() =>
        {
            entered.Set();
            return release.Task;
        });

        Task<Exception?> invoked = Task.Run(() => Invoke(
            ListenerRegistration.Root(listener, Reporter()), Method.Assigned, new TopicPartition(Topic, 0)));

        Assert.True(entered.Wait(s_deadline), "the listener was not called");
        await Task.WhenAny(invoked, Task.Delay(s_notYetWindow));
        Assert.False(invoked.IsCompleted, "the invoker returned before the listener's Task ended");

        release.SetResult(true);
        await TestTimeout.Run(() => invoked, s_deadline);
        Assert.Null(await invoked);
    }

    [Fact]
    public async Task AsyncListener_FaultedTask_ReportsTheSameInstance()
    {
        InvalidOperationException thrown = new InvalidOperationException("async listener blew up");
        TaskCompletionSource<bool> outcome = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        Task<Exception?> invoked = InvokeOnAPendingTask(outcome, Method.Revoked);
        outcome.SetException(thrown);

        await TestTimeout.Run(() => invoked, s_deadline);
        Assert.Same(thrown, await invoked);
    }

    [Fact]
    public async Task AsyncListener_CanceledTask_ReportsTheCancellation()
    {
        TaskCompletionSource<bool> outcome = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        Task<Exception?> invoked = InvokeOnAPendingTask(outcome, Method.Revoked);
        outcome.SetCanceled();

        await TestTimeout.Run(() => invoked, s_deadline);
        Assert.IsAssignableFrom<OperationCanceledException>(await invoked);
    }

    [Fact]
    public void AsyncListener_ThrowingBeforeItReturnsATask_ReportsTheSameInstance()
    {
        InvalidOperationException thrown = new InvalidOperationException("thrown before the Task");
        AsyncProbe listener = new AsyncProbe(() => throw thrown);

        Exception? failure = Invoke(
            ListenerRegistration.Root(listener, Reporter()), Method.Lost, new TopicPartition(Topic, 0));

        Assert.Same(thrown, failure);
    }

    [Fact]
    public void AsyncListener_ReturningANullTask_IsReportedAsSuccess()
    {
        AsyncProbe listener = new AsyncProbe(() => null!);

        Exception? failure = Invoke(
            ListenerRegistration.Root(listener, Reporter()), Method.Assigned, new TopicPartition(Topic, 0));

        Assert.Null(failure);
    }

    [Fact]
    public void Rebalance_Churned_NoCorruption()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        for (int i = 0; i < 200; i++)
        {
            consumer.Rebalance(new[] { new TopicPartition(Topic, i % 4) });
        }

        Assert.Equal(200, listener.Assigned.Count);
        Assert.False(consumer.CurrentListenerRegistration!.IsReleased);
    }

    [Fact]
    public void LiveRegistration_SurvivesAggressiveGc()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        for (int i = 0; i < 50; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        }

        Assert.Equal(50, listener.Assigned.Count);
    }

    [Fact]
    public void ReplacingListenerlessSubscribe_ReleasesTheRegistration()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;
        Assert.False(registration.IsReleased);

        consumer.Subscribe(new[] { Topic });

        Assert.True(registration.IsReleased);
        Assert.Null(consumer.CurrentListenerRegistration);
    }

    [Fact]
    public async Task ReplacingListenerlessAsyncSubscribe_ReleasesTheRegistration()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;

        await consumer.SubscribeWithCallback(new[] { Topic });

        Assert.True(registration.IsReleased);
        Assert.Null(consumer.CurrentListenerRegistration);
    }

    [Fact]
    public void ReplacingListenerlessPatternSubscribe_ReleasesTheRegistration()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new SubscriptionPattern("t.*"), new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;
        Assert.False(registration.IsReleased);

        consumer.Subscribe(new SubscriptionPattern("t.*"));

        Assert.True(registration.IsReleased);
        Assert.Null(consumer.CurrentListenerRegistration);
    }

    [Fact]
    public async Task ReplacingListenerlessAsyncPatternSubscribe_ReleasesTheRegistration()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await consumer.SubscribeWithCallback(new SubscriptionPattern("t.*"), new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;

        await consumer.SubscribeWithCallback(new SubscriptionPattern("t.*"));

        Assert.True(registration.IsReleased);
        Assert.Null(consumer.CurrentListenerRegistration);
    }

    [Fact]
    public void ReplacingListenerSubscribe_ReleasesTheOldRegistrationOnly()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration first = consumer.CurrentListenerRegistration!;

        RecordingListener second = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, second);
        ListenerRegistration secondRegistration = consumer.CurrentListenerRegistration!;

        Assert.True(first.IsReleased);
        Assert.False(secondRegistration.IsReleased);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        Assert.Single(second.Assigned);
    }

    [Fact]
    public void RefusedSubscribe_KeepsTheNewRegistrationAndTheOld_UntilASubscribeSucceeds()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration kept = consumer.CurrentListenerRegistration!;
        consumer.Unsubscribe();
        consumer.Assign(new[] { new TopicPartition(Topic, 0) });

        IllegalStateException refused = Assert.Throws<IllegalStateException>(
            () => consumer.Subscribe(new[] { Topic }, new RecordingListener()));

        Assert.Equal(MutuallyExclusive, refused.Message);
        ListenerRegistration refusedRegistration = consumer.CurrentListenerRegistration!;
        Assert.NotSame(kept, refusedRegistration);
        Assert.False(kept.IsReleased);
        Assert.False(refusedRegistration.IsReleased);

        consumer.Unsubscribe();
        consumer.Subscribe(new[] { Topic }, new RecordingListener());

        Assert.True(kept.IsReleased);
        Assert.True(refusedRegistration.IsReleased);
        Assert.False(consumer.CurrentListenerRegistration!.IsReleased);
    }

    [Fact]
    public async Task RefusedAsyncSubscribe_KeepsTheNewRegistrationAndTheOld_UntilASubscribeSucceeds()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener());
        ListenerRegistration kept = consumer.CurrentListenerRegistration!;
        await consumer.UnsubscribeWithCallback();
        await consumer.AssignWithCallback(new[] { new TopicPartition(Topic, 0) });

        IllegalStateException refused = await Assert.ThrowsAsync<IllegalStateException>(
            () => consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener()));

        Assert.Equal(MutuallyExclusive, refused.Message);
        ListenerRegistration refusedRegistration = consumer.CurrentListenerRegistration!;
        Assert.NotSame(kept, refusedRegistration);
        Assert.False(kept.IsReleased);
        Assert.False(refusedRegistration.IsReleased);

        await consumer.UnsubscribeWithCallback();
        await consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener());

        Assert.True(kept.IsReleased);
        Assert.True(refusedRegistration.IsReleased);
        Assert.False(consumer.CurrentListenerRegistration!.IsReleased);
    }

    [Fact]
    public void RefusedPatternSwitch_KeepsBothRegistrations_AndTheNextRebalanceFiresTheNewListener()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingListener first = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, first);
        ListenerRegistration firstRegistration = consumer.CurrentListenerRegistration!;
        RecordingListener second = new RecordingListener();

        IllegalStateException refused = Assert.Throws<IllegalStateException>(
            () => consumer.Subscribe(new SubscriptionPattern("t.*"), second));

        Assert.Equal(MutuallyExclusive, refused.Message);
        ListenerRegistration secondRegistration = consumer.CurrentListenerRegistration!;
        Assert.NotSame(firstRegistration, secondRegistration);
        Assert.False(firstRegistration.IsReleased);
        Assert.False(secondRegistration.IsReleased);

        TestTimeout.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }), s_deadline);

        Assert.Empty(first.Assigned);
        Assert.Equal(new[] { new TopicPartition(Topic, 0) }, Assert.Single(second.Assigned));

        consumer.Subscribe(new[] { Topic });

        Assert.True(firstRegistration.IsReleased);
        Assert.True(secondRegistration.IsReleased);
        Assert.Null(consumer.CurrentListenerRegistration);
    }

    [Fact]
    public async Task RefusedAsyncPatternSwitch_KeepsBothRegistrations_AndTheNextRebalanceFiresTheNewListener()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        RecordingListener first = new RecordingListener();
        await consumer.SubscribeWithCallback(new[] { Topic }, first);
        ListenerRegistration firstRegistration = consumer.CurrentListenerRegistration!;
        RecordingListener second = new RecordingListener();

        IllegalStateException refused = await Assert.ThrowsAsync<IllegalStateException>(
            () => consumer.SubscribeWithCallback(new SubscriptionPattern("t.*"), second));

        Assert.Equal(MutuallyExclusive, refused.Message);
        ListenerRegistration secondRegistration = consumer.CurrentListenerRegistration!;
        Assert.NotSame(firstRegistration, secondRegistration);
        Assert.False(firstRegistration.IsReleased);
        Assert.False(secondRegistration.IsReleased);

        await TestTimeout.Run(
            () => consumer.RebalanceWithCallback(new[] { new TopicPartition(Topic, 0) }), s_deadline);

        Assert.Empty(first.Assigned);
        Assert.Equal(new[] { new TopicPartition(Topic, 0) }, Assert.Single(second.Assigned));

        await consumer.SubscribeWithCallback(new[] { Topic });

        Assert.True(firstRegistration.IsReleased);
        Assert.True(secondRegistration.IsReleased);
        Assert.Null(consumer.CurrentListenerRegistration);
    }

    [Fact]
    public void FailedListenerlessSubscribe_ReleasesNothing()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration kept = consumer.CurrentListenerRegistration!;

        IllegalStateException refused = Assert.Throws<IllegalStateException>(
            () => consumer.Subscribe(new SubscriptionPattern("t.*")));

        Assert.Equal(MutuallyExclusive, refused.Message);
        Assert.Same(kept, consumer.CurrentListenerRegistration);
        Assert.False(kept.IsReleased);

        consumer.Subscribe(new[] { Topic }, new RecordingListener());

        Assert.True(kept.IsReleased);
    }

    [Fact]
    public async Task FailedListenerlessAsyncSubscribe_ReleasesNothing()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener());
        ListenerRegistration kept = consumer.CurrentListenerRegistration!;

        IllegalStateException refused = await Assert.ThrowsAsync<IllegalStateException>(
            () => consumer.SubscribeWithCallback(new SubscriptionPattern("t.*")));

        Assert.Equal(MutuallyExclusive, refused.Message);
        Assert.Same(kept, consumer.CurrentListenerRegistration);
        Assert.False(kept.IsReleased);

        await consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener());

        Assert.True(kept.IsReleased);
    }

    [Fact]
    public void Dispose_AfterARefusedSubscribe_ReleasesEveryRegistration()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration first = consumer.CurrentListenerRegistration!;

        IllegalStateException refused = Assert.Throws<IllegalStateException>(
            () => consumer.Subscribe(new SubscriptionPattern("t.*"), new RecordingListener()));

        Assert.Equal(MutuallyExclusive, refused.Message);
        ListenerRegistration second = consumer.CurrentListenerRegistration!;
        Assert.NotSame(first, second);

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        Assert.True(first.IsReleased);
        Assert.True(second.IsReleased);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OnAKafkaConsumer_ARefusedSwitch_KeepsBothRegistrations_UntilDispose(bool async)
    {
        NativeConsumer consumer = NativeConsumer.Create(new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "listener-bridge-group",
        });
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        SubscriptionPattern pattern = new SubscriptionPattern("t.*");
        if (async)
        {
            await TestTimeout.Run(() => consumer.SubscribeWithCallback(pattern, new RecordingListener()), s_deadline);
        }
        else
        {
            consumer.Subscribe(pattern, new RecordingListener());
        }

        ListenerRegistration first = consumer.CurrentListenerRegistration!;

        IllegalStateException refused = async
            ? await Assert.ThrowsAsync<IllegalStateException>(() => TestTimeout.Run(
                () => consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener()), s_deadline))
            : Assert.Throws<IllegalStateException>(() => consumer.Subscribe(new[] { Topic }, new RecordingListener()));

        Assert.Equal(MutuallyExclusive, refused.Message);
        ListenerRegistration second = consumer.CurrentListenerRegistration!;
        Assert.NotSame(first, second);
        Assert.False(first.IsReleased);
        Assert.False(second.IsReleased);

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        Assert.True(first.IsReleased);
        Assert.True(second.IsReleased);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OnAKafkaConsumer_AnEmptySubscribe_KeepsTheRegistration(bool async)
    {
        NativeConsumer consumer = NewKafkaConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await Subscribe(consumer, async ? Form.Async : Form.Sync, new[] { Topic }, new RecordingListener());
        ListenerRegistration first = consumer.CurrentListenerRegistration!;

        await SubscribeListenerless(consumer, async, Array.Empty<string>());

        Assert.Same(first, consumer.CurrentListenerRegistration);
        Assert.False(first.IsReleased);
    }

    [Theory]
    [InlineData(Form.Sync)]
    [InlineData(Form.Async)]
    [InlineData(Form.AsyncWithAnAsyncListener)]
    public async Task OnAKafkaConsumer_AnEmptySubscribeWithAListener_ReleasesOnlyItsOwnRegistration(Form form)
    {
        NativeConsumer consumer = NewKafkaConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        await Subscribe(consumer, form, new[] { Topic }, NewListener(form));
        ListenerRegistration first = consumer.CurrentListenerRegistration!;

        (WeakReference listener, Task subscribed) = SubscribeAnUnreferencedListener(consumer, form, Array.Empty<string>());
        await TestTimeout.Run(() => subscribed, s_deadline);

        Assert.Same(first, consumer.CurrentListenerRegistration);
        Assert.False(first.IsReleased);
        await AssertCollected(listener);
    }

    [Theory]
    [InlineData(Form.Sync)]
    [InlineData(Form.Async)]
    [InlineData(Form.AsyncWithAnAsyncListener)]
    public async Task OnAKafkaConsumer_AFailedEmptySubscribeWithAListener_ReleasesItsRegistration(Form form)
    {
        NativeConsumer consumer = NewKafkaConsumer(withGroupId: false);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        (WeakReference listener, Task subscribed) = SubscribeAnUnreferencedListener(consumer, form, Array.Empty<string>());
        InvalidGroupIdException refused = await Assert.ThrowsAsync<InvalidGroupIdException>(
            () => TestTimeout.Run(() => subscribed, s_deadline));

        Assert.Equal(GroupIdNotDefined, refused.Message);
        Assert.Null(consumer.CurrentListenerRegistration);
        await AssertCollected(listener);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OnAKafkaConsumer_AnEmptySubscribeAfterARefusedSwitch_KeepsBothRegistrations_UntilDispose(bool async)
    {
        NativeConsumer consumer = NewKafkaConsumer();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        SubscriptionPattern pattern = new SubscriptionPattern("t.*");
        if (async)
        {
            await TestTimeout.Run(() => consumer.SubscribeWithCallback(pattern, new RecordingListener()), s_deadline);
        }
        else
        {
            consumer.Subscribe(pattern, new RecordingListener());
        }

        ListenerRegistration first = consumer.CurrentListenerRegistration!;
        IllegalStateException refused = await Assert.ThrowsAsync<IllegalStateException>(
            () => Subscribe(consumer, async ? Form.Async : Form.Sync, new[] { Topic }, new RecordingListener()));
        Assert.Equal(MutuallyExclusive, refused.Message);
        ListenerRegistration second = consumer.CurrentListenerRegistration!;
        Assert.NotSame(first, second);

        await SubscribeListenerless(consumer, async, Array.Empty<string>());

        Assert.Same(second, consumer.CurrentListenerRegistration);
        Assert.False(first.IsReleased);
        Assert.False(second.IsReleased);

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        Assert.True(first.IsReleased);
        Assert.True(second.IsReleased);
    }

    [Theory]
    [InlineData(Form.Sync)]
    [InlineData(Form.Async)]
    [InlineData(Form.AsyncWithAnAsyncListener)]
    public async Task OnTheMocks_AnEmptySubscribeWithAListener_ReplacesTheRegistration(Form form)
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        object firstListener = NewListener(form);
        await Subscribe(consumer, form, new[] { Topic }, firstListener);
        ListenerRegistration first = consumer.CurrentListenerRegistration!;
        object thirdListener = NewListener(form);

        await Subscribe(consumer, form, Array.Empty<string>(), thirdListener);

        ListenerRegistration third = consumer.CurrentListenerRegistration!;
        Assert.NotSame(first, third);
        Assert.True(first.IsReleased);
        Assert.False(third.IsReleased);

        TopicPartition[] assignment = { new TopicPartition(Topic, 0) };
        if (form == Form.Sync)
        {
            TestTimeout.Run(() => consumer.Rebalance(assignment), s_deadline);
        }
        else
        {
            await TestTimeout.Run(() => consumer.RebalanceWithCallback(assignment), s_deadline);
        }

        Assert.Empty(AssignedTo(firstListener));
        Assert.Equal(assignment, Assert.Single(AssignedTo(thirdListener)));
    }

    [Fact]
    public void Unsubscribe_DoesNotReleaseTheRegistration()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;

        consumer.Unsubscribe();

        Assert.False(registration.IsReleased);
    }

    [Fact]
    public void Dispose_WithLiveRegistration_ReturnsWithoutHanging()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        Assert.True(registration.IsReleased);
    }

    [Fact]
    public async Task DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await consumer.SubscribeWithCallback(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;

        await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);

        Assert.True(registration.IsReleased);
    }

    private static ConsumerCallbackReporter Reporter() => new ConsumerCallbackReporter(IntPtr.Zero);

    private static NativeConsumer NewKafkaConsumer(bool withGroupId = true)
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
            ["group.protocol"] = "consumer",
        };
        if (withGroupId)
        {
            configs["group.id"] = "listener-bridge-group";
        }

        return NativeConsumer.Create(configs);
    }

    private static object NewListener(Form form) =>
        form == Form.AsyncWithAnAsyncListener ? new AsyncRecordingListener() : new RecordingListener();

    private static List<TopicPartition[]> AssignedTo(object listener) =>
        listener is RecordingListener recording ? recording.Assigned : ((AsyncRecordingListener)listener).Assigned;

    private static Task Subscribe(NativeConsumer consumer, Form form, IReadOnlyCollection<string> topics, object listener) =>
        TestTimeout.Run(() => StartSubscribe(consumer, form, topics, listener), s_deadline);

    private static Task StartSubscribe(NativeConsumer consumer, Form form, IReadOnlyCollection<string> topics, object listener)
    {
        switch (form)
        {
            case Form.Sync:
                try
                {
                    consumer.Subscribe(topics, (IConsumerRebalanceListener)listener);
                    return Task.CompletedTask;
                }
                catch (Exception exception)
                {
                    return Task.FromException(exception);
                }

            case Form.Async:
                return consumer.SubscribeWithCallback(topics, (IConsumerRebalanceListener)listener);
            default:
                return consumer.SubscribeWithCallback(topics, (IAsyncConsumerRebalanceListener)listener);
        }
    }

    private static Task SubscribeListenerless(NativeConsumer consumer, bool async, IReadOnlyCollection<string> topics)
    {
        if (async)
        {
            return TestTimeout.Run(() => consumer.SubscribeWithCallback(topics), s_deadline);
        }

        consumer.Subscribe(topics);
        return Task.CompletedTask;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Listener, Task Subscribed) SubscribeAnUnreferencedListener(
        NativeConsumer consumer,
        Form form,
        IReadOnlyCollection<string> topics)
    {
        object listener = NewListener(form);
        return (new WeakReference(listener), StartSubscribe(consumer, form, topics, listener));
    }

    private static async Task AssertCollected(WeakReference listener)
    {
        Stopwatch watch = Stopwatch.StartNew();
        do
        {
            await Task.Yield();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
        while (listener.IsAlive && watch.Elapsed < s_settle);

        Assert.False(listener.IsAlive, "the listener's registration was not released");
    }

    private static Task<Exception?> InvokeOnAPendingTask(TaskCompletionSource<bool> outcome, Method method)
    {
        ManualResetEventSlim entered = new ManualResetEventSlim();
        AsyncProbe listener = new AsyncProbe(() =>
        {
            entered.Set();
            return outcome.Task;
        });

        Task<Exception?> invoked = Task.Run(() => Invoke(
            ListenerRegistration.Root(listener, Reporter()), method, new TopicPartition(Topic, 0)));
        Assert.True(entered.Wait(s_deadline), "the listener was not called");
        Assert.False(invoked.Wait(s_notYetWindow), "the invoker returned before the listener's Task ended");
        return invoked;
    }

    private static Exception? Invoke(ListenerRegistration registration, Method method, params TopicPartition[] partitions)
    {
        try
        {
            using NativeCallArguments arguments = new NativeCallArguments();
            IntPtr list = arguments.TopicPartitionList(partitions.Select(tp => (Topic: tp.Topic(), Partition: tp.Partition())).ToArray());
            IntPtr handle = registration.NewHandle();
            IntPtr outErr = IntPtr.Zero;
            try
            {
                switch (method)
                {
                    case Method.Revoked:
                        NativeMethods.ConsumerRebalanceListenerOnPartitionsRevoked(handle, list, ref outErr);
                        break;
                    case Method.Assigned:
                        NativeMethods.ConsumerRebalanceListenerOnPartitionsAssigned(handle, list, ref outErr);
                        break;
                    default:
                        NativeMethods.ConsumerRebalanceListenerOnPartitionsLost(handle, list, ref outErr);
                        break;
                }
            }
            finally
            {
                ListenerRegistration.DestroyHandle(handle);
            }

            return KafkaException.FromFfiError(outErr, owned: true);
        }
        finally
        {
            registration.Release();
        }
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

        internal List<TopicPartition[]> Lost { get; } = new List<TopicPartition[]>();

        public override void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            Revoked.Add(partitions.ToArray());

        public override void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            Assigned.Add(partitions.ToArray());
    }

    /// <summary>Records the partitions an async listener is assigned.</summary>
    private sealed class AsyncRecordingListener : IAsyncConsumerRebalanceListener
    {
        internal List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
            Assigned.Add(partitions.ToArray());
            return Task.CompletedTask;
        }

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => Task.CompletedTask;
    }

    /// <summary>A listener whose every method throws the exception it is given.</summary>
    private sealed class ThrowingListener : IConsumerRebalanceListener
    {
        private readonly Exception _thrown;

        internal ThrowingListener(Exception thrown)
        {
            _thrown = thrown;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => throw _thrown;

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => throw _thrown;

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => throw _thrown;
    }

    /// <summary>A listener whose every method runs the action it is given.</summary>
    private sealed class Probe : IConsumerRebalanceListener
    {
        private readonly Action _call;

        internal Probe(Action call)
        {
            _call = call;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => _call();

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _call();

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => _call();
    }

    /// <summary>An async listener whose every method runs the function it is given.</summary>
    private sealed class AsyncProbe : IAsyncConsumerRebalanceListener
    {
        private readonly Func<Task> _call;

        internal AsyncProbe(Func<Task> call)
        {
            _call = call;
        }

        public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) => _call();

        public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _call();

        public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => _call();
    }
}
