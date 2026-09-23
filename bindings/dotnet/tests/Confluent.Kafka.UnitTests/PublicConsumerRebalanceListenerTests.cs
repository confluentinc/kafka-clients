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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M9/P6 rebalance-listener surface through the <b>public</b> API, broker-free: the
/// <c>Subscribe(topics, listener)</c> overload on all four consumer types and the mock-only
/// <c>Rebalance(...)</c> driver that fires it. Pins every semantic the core defines for
/// <c>MockConsumer.rebalance</c>, the throwing-listener contract, and
/// <c>consumer-threading.md</c> §31's "the rebalance does not advance until the listener
/// returns" regression.
/// </summary>
/// <remarks>
/// <para>
/// <b>§31 test #1 ("commitSync() from inside onPartitionsRevoked succeeds") is NOT here —
/// it is deferred to the ConsumerHandle phase</b>, with the rationale recorded in the phase
/// record. The consumer's own <c>Commit()</c> is rejected with ConcurrentModification by the
/// core's access guard <em>by design</em> while the application thread drives the rebalance;
/// the sanctioned reentrancy path is <c>ConsumerHandle</c>, which this binding does not yet
/// expose. §31 test #2 is satisfied below.
/// </para>
/// <para>
/// The complementary interop-level coverage (the <c>on_partitions_lost</c> trampoline the
/// mock never fires, the returned-error-handle contract, GC survival, free-exactly-once, and
/// the registration-release rules) lives in
/// <c>Interop/ConsumerRebalanceListenerBridgeTests</c>.
/// </para>
/// </remarks>
public sealed class PublicConsumerRebalanceListenerTests
{
    private const string Topic = "rebalance-listener-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // Long enough that a genuinely-unblocked rebalance would finish inside it, short enough
    // to keep the suite fast. Only ever used for a "must NOT have completed" wait that is
    // already gated on the callback having fired.
    private static readonly TimeSpan s_notYetWindow = TimeSpan.FromMilliseconds(500);

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    // ---- The Java default carried by ConsumerRebalanceListenerBase ----

    [Fact]
    public void ConsumerRebalanceListenerBase_OnPartitionsLost_DelegatesToRevoked()
    {
        // Java's ConsumerRebalanceListener.onPartitionsLost is a DEFAULT METHOD delegating to
        // onPartitionsRevoked. C# default interface methods need .NET Standard 2.1 / C# 8,
        // above this binding's netstandard2.0 floor, so the default lives here instead
        // (P6-D1 option (b)). This is the plain managed proof that it behaves as Java does.
        RecordingListenerBase listener = new RecordingListenerBase();
        TopicPartition[] partitions = { new TopicPartition(Topic, 1) };

        listener.OnPartitionsLost(partitions);

        Assert.Equal(partitions, Assert.Single(listener.Revoked));
        Assert.Empty(listener.Assigned);
    }

    // ---- Subscribe(topics, listener) on all four consumer types ----

    [Fact]
    public void SyncMock_SubscribeWithListener_ThenRebalance_FiresAssignedWithAddedPartitions()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
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
        await using AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        RecordingListener listener = new RecordingListener();
        await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }, listener), s_deadline);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(new[] { new TopicPartition(Topic, 0) }, Assert.Single(listener.Assigned));
    }

    [Fact]
    public void SyncKafkaConsumer_ExposesTheListenerOverload()
    {
        // The real client cannot be driven broker-free, so this pins the SHAPE: the overload
        // exists on the interface and on KafkaConsumer<K,V>, and is reachable through
        // IConsumer<K,V>. Behaviour is covered against the mocks.
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

    // ---- MockConsumer.rebalance semantics, one test per rule ----

    [Fact]
    public void Rebalance_FiresRevokedOnlyWhenSomethingWasRemoved()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) });
        Assert.Empty(listener.Revoked);

        // Drop partition 1 — now something IS removed.
        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(new[] { new TopicPartition(Topic, 1) }, Assert.Single(listener.Revoked));
    }

    [Fact]
    public void Rebalance_FiresAssignedUnconditionally_WithTheAddedListWhichMayBeEmpty()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        // Same assignment again: nothing added, nothing removed — assigned STILL fires, with
        // an EMPTY collection (the added list, not the full assignment).
        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(2, listener.Assigned.Count);
        Assert.Empty(listener.Assigned[1]);
        Assert.Empty(listener.Revoked);
    }

    [Fact]
    public void Rebalance_NeverFiresLost()
    {
        // Matching Java's MockConsumer. This is why the lost trampoline (and the Java-default
        // delegation) is covered at the interop level instead.
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });
        consumer.Rebalance(Array.Empty<TopicPartition>());

        Assert.Empty(listener.Lost);
    }

    [Fact]
    public void Rebalance_WithoutAListener_IsANoOpSuccess()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Subscribe(new[] { Topic });

        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.Equal(new[] { new TopicPartition(Topic, 0) }, consumer.Assignment().ToArray());
    }

    [Fact]
    public void Rebalance_OnAManuallyAssignedConsumer_ThrowsWithTheJavaMessage()
    {
        // Java throws IllegalArgumentException; the core's message is asserted verbatim
        // (definition-of-done.md §3 — error message content is part of the contract).
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Assign(new[] { new TopicPartition(Topic, 0) });

        KafkaException exception = Assert.Throws<KafkaException>(
            () => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        Assert.Equal(
            "Attempt to dynamically assign partitions while manual assignment in use",
            exception.Message);
    }

    // ---- The throwing-listener contract, end to end ----

    [Fact]
    public void Rebalance_WhenTheListenerThrows_SurfacesTheMessageVerbatim()
    {
        // The exception is caught at the interop boundary (never unwound into native),
        // converted into the error the core sees, and propagated out of the operation that
        // triggered the rebalance — here, Rebalance itself.
        const string Message = "revoke handler failed — 失敗";
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Subscribe(new[] { Topic }, new ThrowingOnAssignedListener(Message));

        KafkaException exception = Assert.Throws<KafkaException>(
            () => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        Assert.Equal(Message, exception.Message);
    }

    [Fact]
    public void Rebalance_AfterAThrowingListener_TheConsumerStaysUsable()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Subscribe(new[] { Topic }, new ThrowingOnAssignedListener("boom"));
        Assert.Throws<KafkaException>(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        // Replace the registration with a well-behaved listener and carry on.
        RecordingListener listener = new RecordingListener();
        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { new TopicPartition(Topic, 2) });

        Assert.Single(listener.Assigned);
    }

    // ---- consumer-threading.md §31 test #2 ----

    [Fact]
    public async Task Rebalance_DoesNotAdvanceUntilTheListenerReturns()
    {
        // §31 #2: the rebalance must not complete before the listener does.
        // MockConsumer_rebalance is synchronous all the way through, so this is directly
        // observable: block the listener, drive Rebalance from a worker, and check.
        //
        // MUTATION CHECK (phase-5 notes item 3): a bare "has NOT completed yet" assertion
        // passes vacuously if the callback never fired at all. The `entered` gate is what
        // makes it non-vacuous — we only assert "not completed" AFTER proving the listener is
        // inside the callback. Removing `_release.Wait()` from the fixture makes
        // `rebalance.IsCompleted` true at that point and the test fails; that mutation was
        // run and its failure recorded in the phase record.
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Subscribe(new[] { Topic }, new BlockingListener(entered, release));

        Task rebalance = Task.Run(() => consumer.Rebalance(new[] { new TopicPartition(Topic, 0) }));

        Assert.True(entered.Wait(s_deadline), "the listener callback never fired");

        // The listener is inside OnPartitionsAssigned and has not returned.
        await Task.WhenAny(rebalance, Task.Delay(s_notYetWindow));
        Assert.False(
            rebalance.IsCompleted,
            "Rebalance returned while the listener was still blocked — the rebalance advanced early.");

        release.Set();

        await TestTimeout.Run(() => rebalance, s_deadline);
    }

    // ---- Preconditions (validated before any pin / P-Invoke, ffi §B5) ----

    [Fact]
    public void Subscribe_NullTopics_ThrowsArgumentNullException()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
            () => consumer.Subscribe(null!, new RecordingListener()));

        Assert.Equal("topics", exception.ParamName);
    }

    [Fact]
    public void Subscribe_NullListener_ThrowsArgumentNullException()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
            () => consumer.Subscribe(new[] { Topic }, null!));

        Assert.Equal("listener", exception.ParamName);
    }

    [Fact]
    public void Subscribe_NullTopicName_ThrowsArgumentException()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => consumer.Subscribe(new string?[] { null }!, new RecordingListener()));

        Assert.Equal("topics", exception.ParamName);
        Assert.Contains("Topic names must not be null.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Subscribe_WithListener_AfterDispose_ThrowsObjectDisposedException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => consumer.Subscribe(new[] { Topic }, new RecordingListener()));
    }

    [Fact]
    public async Task AsyncSubscribe_NullListener_ThrowsArgumentNullException()
    {
        await using AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();

        ArgumentNullException exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.Subscribe(new[] { Topic }, null!));

        Assert.Equal("listener", exception.ParamName);
    }

    [Fact]
    public void Rebalance_NullPartitions_ThrowsArgumentNullException()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
            () => consumer.Rebalance(null!));

        Assert.Equal("partitions", exception.ParamName);
    }

    // ---- Fixtures ----

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

    private sealed class RecordingListenerBase : ConsumerRebalanceListenerBase
    {
        internal List<TopicPartition[]> Revoked { get; } = new List<TopicPartition[]>();

        internal List<TopicPartition[]> Assigned { get; } = new List<TopicPartition[]>();

        public override void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            Revoked.Add(partitions.ToArray());

        public override void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            Assigned.Add(partitions.ToArray());
    }

    private sealed class ThrowingOnAssignedListener : IConsumerRebalanceListener
    {
        private readonly string _message;

        internal ThrowingOnAssignedListener(string message)
        {
            _message = message;
        }

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            throw new InvalidOperationException(_message);

        public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>
    /// Signals that the callback was entered, then blocks inside it until released — the
    /// fixture §31 #2 needs. It uses the same primitive production does (a plain synchronous
    /// listener method invoked by the trampoline); nothing about the callback path is
    /// substituted (DoD #12).
    /// </summary>
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
