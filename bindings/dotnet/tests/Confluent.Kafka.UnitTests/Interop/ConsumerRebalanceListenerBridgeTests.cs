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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The <b>multi-shot</b> rebalance-listener registration bridge (ffi-marshalling.md §B6) —
/// the one callback family in this binding that is not a one-shot per-operation completion.
/// Covers what the public surface cannot reach: the <c>on_partitions_lost</c> trampoline
/// (which <c>MockConsumer.rebalance</c> never fires, by design), the returned-error-handle
/// contract, the free-exactly-once properties, and the Java-faithful registration-release
/// rules.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why some tests invoke a trampoline directly.</b> <c>MockConsumer_rebalance</c> never
/// fires <c>on_partitions_lost</c> (matching Java), so the only way to exercise that
/// trampoline — and with it the Java default <c>lost → revoked</c> delegation carried by
/// <see cref="ConsumerRebalanceListenerBase"/> — is to call it the way the core would. The
/// Rust side does exactly the same in its own <c>#[cfg(test)]</c> coverage. The delivered
/// list is a genuine owned <c>TopicPartitionList_t</c> obtained from
/// <c>Consumer_assignment</c>, so the callback-owns-the-handle contract is exercised for
/// real rather than simulated.
/// </para>
/// <para>
/// <b>DoD #10 (hot-path allocation audit) is N/A for this phase</b> and is stated rather
/// than skipped: a rebalance listener fires per <em>rebalance</em>, not per record, so there
/// is no per-message hot path to budget.
/// </para>
/// </remarks>
public sealed class ConsumerRebalanceListenerBridgeTests
{
    private const string Topic = "listener-bridge-topic";

    // The core maps any code outside the protocol range to UnknownServerError, whose code
    // is -1 — the value Python pins for a throwing listener.
    private const int ListenerErrorCode = -1;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Drives a mock to a known non-empty assignment and hands back the owned
    /// <c>TopicPartitionList_t</c> the core would deliver to a listener callback. Ownership
    /// transfers to the caller, exactly as it does to a real callback.
    /// </summary>
    private static IntPtr OwnedAssignment(NativeConsumer consumer, params TopicPartition[] partitions)
    {
        consumer.Assign(partitions);
        return NativeMethods.ConsumerAssignment(consumer.Handle);
    }

    // ---- The on_partitions_lost trampoline (unreachable from the mock) ----

    [Fact]
    public void PartitionsLost_OnListenerBase_DelegatesToRevoked_JavaDefault()
    {
        // MockConsumer.rebalance never fires lost, so Java's default `lost -> revoked`
        // delegation — which lives on ConsumerRebalanceListenerBase here, because the
        // netstandard2.0 floor has no default interface methods — is only reachable by
        // invoking the trampoline the way the core does.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingListenerBase listener = new RecordingListenerBase();
        ListenerRegistration registration = ListenerRegistration.Root(listener);
        try
        {
            IntPtr list = OwnedAssignment(consumer, new TopicPartition(Topic, 0), new TopicPartition(Topic, 3));

            IntPtr error = ConsumerCallbacks.PartitionsLost(list, registration.UserData);

            Assert.Equal(IntPtr.Zero, error);
            Assert.Empty(listener.Lost);
            TopicPartition[] revoked = Assert.Single(listener.Revoked).OrderBy(tp => tp.Partition).ToArray();
            Assert.Equal(new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 3) }, revoked);
        }
        finally
        {
            registration.Release();
        }
    }

    [Fact]
    public void PartitionsLost_OnDirectInterfaceImplementation_DeliversToLost()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingListener listener = new RecordingListener();
        ListenerRegistration registration = ListenerRegistration.Root(listener);
        try
        {
            IntPtr list = OwnedAssignment(consumer, new TopicPartition(Topic, 7));

            IntPtr error = ConsumerCallbacks.PartitionsLost(list, registration.UserData);

            Assert.Equal(IntPtr.Zero, error);
            Assert.Empty(listener.Revoked);
            Assert.Equal(new[] { new TopicPartition(Topic, 7) }, Assert.Single(listener.Lost));
        }
        finally
        {
            registration.Release();
        }
    }

    [Fact]
    public void ListenerCallback_NonAsciiTopic_RoundTripsThroughDeliveredPartitions()
    {
        // Guards a MarshalAs(LPStr) mistake, which corrupts non-ASCII silently and hides in
        // ASCII-only tests (ffi §B3). The delivered topic is NUL-terminated (the list's own
        // getter), not the length-delimited receive-path form.
        const string NonAscii = "témas-日本語-🎉";
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingListener listener = new RecordingListener();
        ListenerRegistration registration = ListenerRegistration.Root(listener);
        try
        {
            IntPtr list = OwnedAssignment(consumer, new TopicPartition(NonAscii, 2));

            Assert.Equal(IntPtr.Zero, ConsumerCallbacks.PartitionsAssigned(list, registration.UserData));

            TopicPartition delivered = Assert.Single(Assert.Single(listener.Assigned));
            Assert.Equal(NonAscii, delivered.Topic);
            Assert.Equal(2, delivered.Partition);
        }
        finally
        {
            registration.Release();
        }
    }

    // ---- The throwing-listener contract ----

    [Fact]
    public void ThrowingListener_DoesNotUnwind_ReturnsErrorHandleWithCodeAndVerbatimMessage()
    {
        // A managed exception must never unwind into native (there is no caller frame — it
        // would be UB). It is converted into an owned KafkaError* returned to the core; the
        // observable contract Python pins is code -1 plus the message verbatim.
        const string Message = "listener blew up — 例外";
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        ThrowingListener listener = new ThrowingListener(Message);
        ListenerRegistration registration = ListenerRegistration.Root(listener);
        try
        {
            IntPtr list = OwnedAssignment(consumer, new TopicPartition(Topic, 0));

            IntPtr error = ConsumerCallbacks.PartitionsRevoked(list, registration.UserData);

            Assert.NotEqual(IntPtr.Zero, error);
            try
            {
                Assert.Equal(ListenerErrorCode, NativeMethods.Code(error));
                Assert.Equal(Message, Utf8Marshal.PtrToString(NativeMethods.Message(error)));
            }
            finally
            {
                // The trampoline correctly did NOT destroy the handle it returned (ownership
                // transfers to the core). This test stands in for the core, so it frees it.
                NativeMethods.ErrorDestroy(error);
            }
        }
        finally
        {
            registration.Release();
        }
    }

    // ---- Free exactly once ----

    [Fact]
    public void ListenerRegistration_ReleasedTwice_FreesExactlyOnce()
    {
        // GCHandle.Free() on an already-freed handle throws InvalidOperationException, so
        // "does not throw" is the observable proof that the single free site is
        // idempotent-safe. It has to be: the "native never ran" abandon path and the release
        // hook can both reach it.
        ListenerRegistration registration = ListenerRegistration.Root(new RecordingListener());

        registration.Release();
        registration.Release();

        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void Rebalance_Churned_NoCorruption()
    {
        // The loop guards the registration GCHandle across N fires: it must NOT be freed
        // per fire, and a per-fire free is a use-after-free on iteration 2 (verified — the
        // injection turns this test and five others red).
        //
        // It does NOT assert anything about the delivered TopicPartitionList_t. Freeing
        // that is inherited from the shared, pre-existing CopyOutAndDestroy (which destroys
        // in a finally); nothing here observes native memory, so a leaked list would pass
        // silently. The guard is correct, but this test cannot tell it from a broken one.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        for (int i = 0; i < 200; i++)
        {
            consumer.Rebalance(new[] { new TopicPartition(Topic, i % 4) });
        }

        // Assigned fires unconditionally while a listener is registered; revoked only when
        // something was removed. 200 fires of assigned proves the registration survived.
        Assert.Equal(200, listener.Assigned.Count);
        Assert.False(consumer.CurrentListenerRegistration!.IsReleased);
    }

    // ---- Keep-alive under GC ----

    [Fact]
    public void LiveRegistration_SurvivesAggressiveGc()
    {
        // The three thunks are static readonly (process-rooted) and the registration is
        // rooted by its GCHandle (Normal), so aggressive GC across a LIVE registration must
        // not collect either. Unlike the one-shot bridge's in-flight window, this window is
        // the whole subscription — the registration outlives the subscribe call by design.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
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

    // ---- Registration lifetime (Java-faithful) ----

    [Fact]
    public void ReplacingListenerlessSubscribe_ReleasesTheRegistration()
    {
        // Java's registerRebalanceListener(Optional.empty()): a listener-less subscribe is
        // how you deregister. The core fires the release hook, which is the single site that
        // frees the registration GCHandle.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;
        Assert.False(registration.IsReleased);

        consumer.Subscribe(new[] { Topic });

        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void ReplacingListenerSubscribe_ReleasesTheOldRegistrationOnly()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration first = consumer.CurrentListenerRegistration!;

        RecordingListener second = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, second);
        ListenerRegistration secondRegistration = consumer.CurrentListenerRegistration!;

        Assert.True(first.IsReleased);
        Assert.False(secondRegistration.IsReleased);

        // And the surviving registration is the one that fires.
        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        Assert.Single(second.Assigned);
    }

    [Fact]
    public void Unsubscribe_DoesNotReleaseTheRegistration()
    {
        // Java's SubscriptionState.unsubscribe() clears the subscription but KEEPS the
        // registered listener. Releasing here would be a behaviour change, so it is asserted
        // rather than assumed.
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;

        consumer.Unsubscribe();

        Assert.False(registration.IsReleased);
    }

    // ---- Teardown with a live registration ----

    [Fact]
    public void Dispose_WithLiveRegistration_ReturnsWithoutHanging()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Subscribe(new[] { Topic }, new RecordingListener());
        ListenerRegistration registration = consumer.CurrentListenerRegistration!;

        TestTimeout.Run(() => consumer.Dispose(), s_deadline);

        // Trigger 5: the consumer is destroyed, so the core released the registration.
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

    // ---- Fixtures ----

    /// <summary>
    /// Records every delivered collection. Implements the interface <b>directly</b>, so
    /// <see cref="OnPartitionsLost"/> is a real distinct method (no Java-default delegation).
    /// </summary>
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

    /// <summary>
    /// Derives from <see cref="ConsumerRebalanceListenerBase"/> and deliberately does
    /// <b>not</b> override <c>OnPartitionsLost</c>, so a lost callback must land in
    /// <see cref="Revoked"/> via Java's default delegation.
    /// </summary>
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

    private sealed class ThrowingListener : IConsumerRebalanceListener
    {
        private readonly string _message;

        internal ThrowingListener(string message)
        {
            _message = message;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            throw new InvalidOperationException(_message);

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            throw new InvalidOperationException(_message);

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
            throw new InvalidOperationException(_message);
    }
}
