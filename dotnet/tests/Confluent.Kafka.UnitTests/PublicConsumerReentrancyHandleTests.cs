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

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M9/P8 <see cref="ConsumerHandle"/> surface through the <b>public</b> API, broker-free:
/// <see cref="IConsumerCommon.Handle"/> on all four consumer types, the handle's own method
/// set, its disposal contract, and the two properties the type exists for — that it
/// <b>bypasses the access guard</b> that makes the consumer's own API unusable from inside a
/// callback, and that it <b>keeps its consumer alive</b> rather than dangling a pointer at it.
/// </summary>
/// <remarks>
/// <para>
/// <b>The mock's ceiling is a core contract, not a limitation being worked around.</b> On a
/// <c>MockConsumer</c>-derived handle <c>wakeup</c> and the three sync getters work (the
/// getters return <b>empty</b>) while every async operation fails with
/// <c>UnsupportedVersionError</c> — "the mock has no event pipeline … This is core behavior,
/// not an FFI limitation" (<c>src/ffi/consumer_handle.rs:82-86</c>). Asserting it is coverage.
/// </para>
/// <para>
/// <b><c>consumer-threading.md</c> §31 test #1 is satisfied in two parts.</b> The
/// <b>mechanism proof</b> ships here, as
/// <see cref="ReentrancyHandle_SucceedsInsideAListener_WhereTheConsumersOwnApiIsRejected"/>.
/// The commit-specific <b>end-to-end</b> half needs a real broker and is tracked as follow-up
/// O3 (roadmap §5.10) — a scoped split with both halves owned, not a deferral.
/// </para>
/// </remarks>
public sealed class PublicConsumerReentrancyHandleTests
{
    private const string Topic = "reentrancy-handle-topic";

    // A non-ASCII topic that exercises the UTF-8 boundary in both directions (ffi §B3):
    // an umlaut, an accent, and a Greek capital omega.
    private const string NonAsciiTopic = "grüße-café-Ω-topic";

    // kafka_common_Error code for UnsupportedVersion, measured against the shipped core.
    private const int UnsupportedVersionCode = 35;

    // The core's verbatim message for an async op on a mock-derived handle. Asserted in full
    // (DoD §3) — the code alone would not distinguish this from any other UnsupportedVersion.
    private const string MockAsyncUnsupportedMessage =
        "ConsumerHandle async operations are not supported on a MockConsumer handle; " +
        "drive the MockConsumer directly.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    // A real consumer constructs broker-free (unlike the producer, which rejects an empty
    // bootstrap list); only operations that need the broker would block, and none used here does.
    private static Dictionary<string, string> RealConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    // ---- Test 6 · consumer-threading.md §31 test #1, the mechanism half ----

    /// <summary>
    /// The property the whole type exists for: from inside a rebalance listener,
    /// <see cref="ConsumerHandle.Assignment"/> <b>succeeds</b> while the same listener's
    /// <c>consumer.Assignment()</c> is <b>rejected</b> as concurrent access.
    /// </summary>
    /// <remarks>
    /// <para>
    /// No broker, no threads, no sleeps. <c>MockConsumer_rebalance</c> acquires the core's
    /// single-owner access guard and holds it across the listener invocation
    /// (<c>src/ffi/consumer.rs:1400-1417</c> — <c>acquire(h)</c> then a <c>ReleaseGuard</c>
    /// spanning <c>block_on(mock.rebalance(..))</c>). So inside the callback:
    /// <c>kafka_consumer_Consumer_assignment</c> takes the guard, fails to acquire it, and
    /// returns <b>null</b>, which the binding maps to <see cref="InvalidOperationException"/>;
    /// <c>kafka_consumer_ConsumerHandle_assignment</c> takes no guard at all and is documented
    /// <b>non-null</b>.
    /// </para>
    /// <para>
    /// The consumer's rejection is <b>captured, not thrown</b>, so the rebalance still
    /// completes — a listener that throws would have its exception converted into a
    /// <c>Error*</c> and propagated out of <c>Rebalance</c>, which would tell us nothing
    /// about the handle.
    /// </para>
    /// <para>
    /// <b>Mutation-checked, and here is the mechanism it actually fails by</b> — worth
    /// stating precisely, because the next person reasoning about non-vacuity will trust
    /// it. Swapping the handle call for a second <c>consumer.Assignment()</c> makes the
    /// listener throw (that call is deliberately <em>outside</em> the <c>try</c>), the
    /// trampoline converts the throw into an <c>Error*</c>, and the test dies at the
    /// <c>Rebalance</c> call with <c>KafkaException : KafkaConsumer is not safe for
    /// multi-threaded access</c> — the assertions below are never reached. Detection is
    /// genuine (this test is the sole detector), but it is <b>not</b> "viaHandle stays
    /// null". A hypothetical both-succeed regression fails it by the other route, at the
    /// <see cref="InvalidOperationException"/> assertion, which <em>is</em> reached.
    /// </para>
    /// </remarks>
    [Fact]
    public void ReentrancyHandle_SucceedsInsideAListener_WhereTheConsumersOwnApiIsRejected()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        IReadOnlyCollection<TopicPartition>? viaHandle = null;
        Exception? viaConsumer = null;

        ProbingListener listener = new ProbingListener(() =>
        {
            // No guard -> succeeds, even though the rebalance is holding it.
            viaHandle = handle.Assignment();

            // Guard-protected -> rejected. Captured so the rebalance completes normally.
            try
            {
                consumer.Assignment();
            }
            catch (Exception e)
            {
                viaConsumer = e;
            }
        });

        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.True(listener.Fired, "the listener never ran, so neither call was exercised");

        // The handle call SUCCEEDED (non-null), and is empty because the handle is
        // mock-derived — "Always empty on a MockConsumer-derived handle (core behavior)".
        Assert.NotNull(viaHandle);
        Assert.Empty(viaHandle!);

        // The consumer's own call was REJECTED as concurrent access. The message is
        // asserted, not just the type (DoD §3): it is what proves this is the *guard*
        // rejection rather than some unrelated InvalidOperationException.
        InvalidOperationException rejected = Assert.IsType<InvalidOperationException>(viaConsumer);
        Assert.Contains("not safe for multi-threaded access", rejected.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same guard asymmetry, stated as a plain fact about the two getters outside any
    /// callback: with no operation in flight <b>both</b> succeed. This is the control for the
    /// test above — without it, "the consumer's getter threw" could be read as the getter
    /// simply being broken rather than being guard-rejected.
    /// </summary>
    [Fact]
    public void OutsideACallback_BothTheHandleAndTheConsumerGettersSucceed()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        Assert.Empty(handle.Assignment());
        Assert.Empty(consumer.Assignment());
    }

    /// <summary>
    /// <see cref="IConsumerCommon.Handle"/> itself never fails for concurrency —
    /// <c>Consumer_handle</c> does not acquire the guard — so it is callable from inside a
    /// listener too, not only up front.
    /// </summary>
    [Fact]
    public void Handle_IsObtainable_FromInsideAListener_WhileTheGuardIsHeld()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        ConsumerHandle? obtainedInCallback = null;
        ProbingListener listener = new ProbingListener(() => obtainedInCallback = consumer.Handle());

        consumer.Subscribe(new[] { Topic }, listener);
        consumer.Rebalance(new[] { new TopicPartition(Topic, 0) });

        Assert.NotNull(obtainedInCallback);
        using (obtainedInCallback)
        {
            Assert.Empty(obtainedInCallback!.Assignment());
        }
    }

    // ---- Test 1/P8-D3 · Handle() on all four consumer types, through the interface ----

    [Fact]
    public void SyncMock_Handle_IsReachableThroughIConsumer()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = HandleOf(consumer);

        Assert.Empty(handle.Subscription());
    }

    [Fact]
    public void AsyncMock_Handle_IsReachableThroughIAsyncConsumer()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        using ConsumerHandle handle = HandleOf(consumer);

        Assert.Empty(handle.Subscription());
    }

    [Fact]
    public void SyncKafkaConsumer_Handle_IsReachableThroughIConsumer()
    {
        using KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<byte[], byte[]>(RealConfig("p8-sync-handle"), Serdes.ByteArray, Serdes.ByteArray);
        using ConsumerHandle handle = HandleOf(consumer);

        Assert.Empty(handle.Assignment());
    }

    [Fact]
    public async Task AsyncKafkaConsumer_Handle_IsReachableThroughIAsyncConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(RealConfig("p8-async-handle"), Serdes.ByteArray, Serdes.ByteArray);
        await using (consumer)
        {
            using ConsumerHandle handle = HandleOf(consumer);
            Assert.Empty(handle.Assignment());
        }
    }

    /// <summary>Each call returns an independent handle; destroying one does not affect another.</summary>
    [Fact]
    public void Handles_AreIndependent_DisposingOneLeavesTheOtherUsable()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        ConsumerHandle first = consumer.Handle();
        using ConsumerHandle second = consumer.Handle();

        Assert.NotSame(first, second);
        first.Dispose();

        // "Destroying a handle never affects the owning consumer or any other handle."
        Assert.Empty(second.Assignment());
        Assert.Empty(consumer.Assignment());
    }

    [Fact]
    public void Handle_AfterConsumerDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.Handle());
    }

    // ---- Test 3 · wakeup + the three sync getters work on a mock, and return EMPTY ----

    [Fact]
    public void MockDerivedHandle_WakeupAndTheThreeGetters_Work_AndTheGettersAreEmptyNotNull()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        // Neither blocks nor takes the guard; a no-op here, but it must not throw.
        handle.Wakeup();

        IReadOnlyCollection<TopicPartition> assignment = handle.Assignment();
        IReadOnlyCollection<string> subscription = handle.Subscription();
        IReadOnlyCollection<TopicPartition> paused = handle.Paused();

        // Non-null is the contract ("Returns … a non-null …"); empty is the mock behavior.
        Assert.NotNull(assignment);
        Assert.NotNull(subscription);
        Assert.NotNull(paused);
        Assert.Empty(assignment);
        Assert.Empty(subscription);
        Assert.Empty(paused);
    }

    /// <summary>
    /// The getters stay empty on a mock even after the consumer genuinely has a subscription —
    /// pinning that "always empty" is the documented core behavior rather than an artifact of
    /// an untouched consumer.
    /// </summary>
    [Fact]
    public void MockDerivedHandle_GettersStayEmpty_EvenWithALiveSubscription()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Subscribe(new[] { Topic });
        using ConsumerHandle handle = consumer.Handle();

        // The consumer itself sees the subscription…
        Assert.Equal(new[] { Topic }, consumer.Subscription());

        // …while the mock-derived handle is always empty (core behavior).
        Assert.Empty(handle.Subscription());
    }

    // ---- Test 4 · every async op on a mock handle: UnsupportedVersion + the exact message ----

    public static TheoryData<string, Action<ConsumerHandle>> MockUnsupportedOperations()
    {
        TopicPartition tp = new TopicPartition(Topic, 0);
        IReadOnlyCollection<TopicPartition> one = new[] { tp };
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets =
            new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(7, "m", null) };
        IReadOnlyDictionary<TopicPartition, long> timestamps =
            new Dictionary<TopicPartition, long> { [tp] = 1 };

        return new TheoryData<string, Action<ConsumerHandle>>
        {
            { "Assign", h => h.Assign(one) },
            { "Pause", h => h.Pause(one) },
            { "Resume", h => h.Resume(one) },
            { "SeekToBeginning", h => h.SeekToBeginning(one) },
            { "SeekToEnd", h => h.SeekToEnd(one) },
            { "Seek", h => h.Seek(tp, 5) },
            { "Seek(OffsetAndMetadata)", h => h.Seek(tp, new OffsetAndMetadata(5, "m", null)) },
            { "Position", h => h.Position(tp) },
            { "Position(TimeSpan)", h => h.Position(tp, TimeSpan.FromSeconds(1)) },
            { "Committed", h => h.Committed(one) },
            { "BeginningOffsets", h => h.BeginningOffsets(one) },
            { "EndOffsets", h => h.EndOffsets(one) },
            { "OffsetsForTimes", h => h.OffsetsForTimes(timestamps) },
            { "Commit", h => h.Commit() },
            { "Commit(offsets)", h => h.Commit(offsets) },
            { "CommitAsync", h => h.CommitAsync() },
            { "CommitAsync(offsets)", h => h.CommitAsync(offsets) },
        };
    }

    /// <summary>
    /// Every async operation on a mock-derived handle fails with the documented
    /// <c>UnsupportedVersion</c> code <b>and the exact core message</b> (DoD §3). This is
    /// documented core behavior asserted, not a limitation worked around.
    /// </summary>
    [Theory]
    [MemberData(nameof(MockUnsupportedOperations))]
    public void MockDerivedHandle_EveryAsyncOperation_ThrowsUnsupportedVersion(
        string name, Action<ConsumerHandle> operation)
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        KafkaException failure = Assert.Throws<KafkaException>(() => operation(handle));

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(MockAsyncUnsupportedMessage, failure.Message);
        Assert.False(failure.IsRetriable, name);
    }

    // ---- Test 2 · disposal: idempotent, and use-after-dispose throws ----

    [Fact]
    public void Dispose_IsIdempotent()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        ConsumerHandle handle = consumer.Handle();

        handle.Dispose();
        handle.Dispose();
        handle.Dispose();

        // The consumer is untouched by any of it.
        Assert.Empty(consumer.Assignment());
    }

    public static TheoryData<string, Action<ConsumerHandle>> AllOperations()
    {
        TheoryData<string, Action<ConsumerHandle>> data = MockUnsupportedOperations();
        data.Add("Wakeup", h => h.Wakeup());
        data.Add("Assignment", h => h.Assignment());
        data.Add("Subscription", h => h.Subscription());
        data.Add("Paused", h => h.Paused());
        return data;
    }

    /// <summary>
    /// <b>Every</b> member throws <see cref="ObjectDisposedException"/> after
    /// <see cref="ConsumerHandle.Dispose"/> — including the four that succeed on a live
    /// mock-derived handle, which a spot-check of the throwing ones would miss.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllOperations))]
    public void EveryOperation_AfterDispose_ThrowsObjectDisposed(string name, Action<ConsumerHandle> operation)
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        ConsumerHandle handle = consumer.Handle();
        handle.Dispose();

        ObjectDisposedException failure = Assert.Throws<ObjectDisposedException>(() => operation(handle));
        Assert.Contains(nameof(ConsumerHandle), failure.Message, StringComparison.Ordinal);
        Assert.Equal(nameof(ConsumerHandle), failure.ObjectName);
        Assert.NotNull(name);
    }

    /// <summary>
    /// The one precondition validated in managed code before anything native (ffi §B5) — the
    /// <see cref="TimeSpan"/> overload's negative-timeout guard. Asserts the parameter name
    /// and the custom message, not merely the exception type.
    /// </summary>
    [Fact]
    public void Position_WithNegativeTimeout_ThrowsArgumentOutOfRange()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => handle.Position(new TopicPartition(Topic, 0), TimeSpan.FromMilliseconds(-1)));

        Assert.Equal("timeout", failure.ParamName);
        Assert.Contains("Timeout must not be negative.", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Assign_WithNullPartitions_ThrowsArgumentNull()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        ArgumentNullException failure = Assert.Throws<ArgumentNullException>(() => handle.Assign(null!));
        Assert.Equal("partitions", failure.ParamName);
    }

    [Fact]
    public void Seek_WithNegativeOffset_ThrowsArgumentOutOfRange()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        using ConsumerHandle handle = consumer.Handle();

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => handle.Seek(new TopicPartition(Topic, 0), -1));

        Assert.Equal("offset", failure.ParamName);
        Assert.Contains("seek offset must not be a negative number", failure.Message, StringComparison.Ordinal);
    }

    // ---- Test 5 · the ref-count: a live handle DEFERS the consumer's native destroy ----

    /// <summary>
    /// The P8-D1 contract, as a three-way differential on the consumer's own
    /// <see cref="SafeConsumerHandle"/>: with no handle outstanding the consumer's
    /// <c>Dispose</c> releases immediately; with one outstanding it does <b>not</b>; and the
    /// handle's own <c>Dispose</c> is what completes the release.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>SafeHandle.IsClosed</c> flips only when the reference count actually reaches zero and
    /// <c>ReleaseHandle</c> (→ <c>Consumer_destroy</c>) runs, so it is a direct read of the
    /// ref-count decision rather than a proxy for it. The three cases are asserted together
    /// because only the <b>contrast</b> is meaningful: the baseline is what proves the
    /// deferral is caused by the handle and not by teardown being lazy in general.
    /// </para>
    /// <para>
    /// <b>This is the test that caught a real defect.</b> An earlier draft took the parent
    /// reference twice (once in <c>Create</c>, once again while transferring ownership) against
    /// a single release, so the count never reached zero and <b>every</b> consumer that ever
    /// produced a handle leaked its native resources for the process lifetime. Nothing else in
    /// the suite noticed: all three cases reported <c>IsClosed=False</c>. Re-introducing the
    /// second <c>DangerousAddRef</c> turns the last two assertions red.
    /// </para>
    /// </remarks>
    [Fact]
    public void LiveHandle_DefersTheConsumersNativeDestroy_AndDisposingItCompletesTheRelease()
    {
        // Baseline: no reentrancy handle -> the consumer's Dispose releases immediately.
        NativeConsumer baseline = NativeConsumer.CreateMock();
        SafeConsumerHandle baselineNative = baseline.Handle;
        baseline.Dispose();
        Assert.True(baselineNative.IsClosed, "with no handle outstanding the release must be immediate");

        // With a live handle -> the release is deferred past the consumer's own Dispose.
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle native = consumer.Handle;
        ConsumerHandle handle = consumer.CreateReentrancyHandle();

        consumer.Dispose();
        Assert.False(native.IsClosed, "a live reentrancy handle must defer Consumer_destroy");

        // …and the handle's own Dispose is what completes it.
        handle.Dispose();
        Assert.True(native.IsClosed, "disposing the last handle must complete the release");
    }

    /// <summary>
    /// P8-D1's <b>release-site ordering</b>: the parent reference is released from the
    /// reentrancy <c>SafeHandle</c>'s <c>ReleaseHandle</c>, <b>not</b> from the managed
    /// wrapper's <c>Dispose</c> — so the parent count cannot drop while a handle operation
    /// is still blocked inside the core.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why this test exists: the ordering is otherwise ungradeable.</b> Moving the
    /// release into the wrapper's <c>Dispose</c> leaves every other test in this file green
    /// — the count still balances on every sequential path — so
    /// <see cref="LiveHandle_DefersTheConsumersNativeDestroy_AndDisposingItCompletesTheRelease"/>
    /// and its siblings cannot tell the correct ordering from the broken one. This is the
    /// sole detector.
    /// </para>
    /// <para>
    /// The outstanding <c>DangerousAddRef</c> on the <em>inner</em> handle stands in for the
    /// call-scoped reference the interop marshaller holds while a handle op is executing —
    /// every one of the 21 non-destroy ops passes that <c>SafeHandle</c> as a P/Invoke
    /// parameter, so this models "a handle op is in flight" deterministically, with no
    /// threads and no blocking. With the release in <c>ReleaseHandle</c> the parent survives
    /// until that reference drops; with it in <c>Dispose</c> the parent would be released
    /// while the op was still running — a use-after-free.
    /// </para>
    /// </remarks>
    [Fact]
    public void ParentReference_IsReleasedFromReleaseHandle_NotFromTheWrappersDispose()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle parent = consumer.Handle;
        ConsumerHandle handle = consumer.CreateReentrancyHandle();

        // Stand in for a handle op in flight: the marshaller's call-scoped ref on the inner
        // handle, which defers its ReleaseHandle exactly as a running native call would.
        bool taken = false;
        handle.NativeHandle.DangerousAddRef(ref taken);
        Assert.True(taken);

        try
        {
            consumer.Dispose();
            handle.Dispose();

            // ReleaseHandle has NOT run (the inner ref defers it), so the parent reference
            // is still held and the consumer is still alive. If the release lived in the
            // wrapper's Dispose it would already have fired here — with an op "in flight".
            Assert.False(
                parent.IsClosed,
                "the parent must not be released while a handle operation is still in flight");
        }
        finally
        {
            handle.NativeHandle.DangerousRelease();
        }

        // Dropping the last inner reference runs ReleaseHandle, which releases the parent.
        Assert.True(parent.IsClosed, "dropping the last inner reference must release the parent");
    }

    /// <summary>
    /// The reverse order: disposing the handle first hands the reference back, and the
    /// consumer's own <c>Dispose</c> then releases immediately — the handle must not hold the
    /// reference beyond its own lifetime.
    /// </summary>
    [Fact]
    public void DisposingTheHandleFirst_ReturnsTheReference_SoTheConsumerReleasesNormally()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        SafeConsumerHandle native = consumer.Handle;
        ConsumerHandle handle = consumer.CreateReentrancyHandle();

        handle.Dispose();
        Assert.False(native.IsClosed, "the consumer is still alive; nothing should have released it");

        consumer.Dispose();
        Assert.True(native.IsClosed, "the handle must not retain the reference past its own Dispose");
    }

    /// <summary>
    /// The user-visible consequence of the deferral: a handle outstanding across the
    /// consumer's <c>Dispose</c> is still safe to call. Post-destroy this would be a
    /// use-after-free rather than a clean result.
    /// </summary>
    [Fact]
    public void HandleOutstandingAcrossConsumerDispose_RemainsSafeToCall()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        ConsumerHandle handle = consumer.Handle();

        consumer.Dispose();

        Assert.Empty(handle.Assignment());
        Assert.Empty(handle.Subscription());
        Assert.Empty(handle.Paused());
        handle.Wakeup();

        handle.Dispose();
    }

    // ---- Test 8 · teardown returns without hanging with a live handle outstanding ----

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

        // 200 balanced add/release pairs must leave the count exactly where it started, so the
        // consumer still releases on its own Dispose. An unbalanced pair would leak here.
        consumer.Dispose();
        Assert.True(native.IsClosed, "200 handle round-trips must leave the reference count balanced");
    }

    /// <summary>
    /// The handle and the consumer report the <b>same</b> underlying condition the
    /// <b>same</b> way — a flat <see cref="KafkaException"/> with an identical code and
    /// message. This is the contract M9/P8's fix round settled on (ffi §B5): the core's
    /// <c>illegal_state</c> is <b>not</b> remapped to
    /// <see cref="InvalidOperationException"/> on the handle.
    /// </summary>
    /// <remarks>
    /// <c>ConsumerHandle::position</c> and <c>AsyncKafkaConsumer::position_timeout</c> share
    /// one core implementation, so an unassigned partition yields the identical error on both
    /// — measured: code <c>-1</c>, <i>"You can only check the position for partitions
    /// assigned to this consumer."</i> Remapping only the handle side would make the
    /// reentrancy twin throw a different exception type than the consumer it mirrors, for the
    /// same call. That is the concrete reason the mapping was rejected, so it is the thing
    /// worth pinning: this test fails the moment someone reintroduces it.
    /// </remarks>
    [Fact]
    public void HandleAndConsumer_ReportTheSameCoreError_Identically()
    {
        using KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<byte[], byte[]>(RealConfig("p8-error-parity"), Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(
            () =>
            {
                using ConsumerHandle handle = consumer.Handle();
                TopicPartition unassigned = new TopicPartition("unassigned-parity-topic", 0);

                KafkaException viaConsumer = Assert.Throws<KafkaException>(() => consumer.Position(unassigned));
                KafkaException viaHandle = Assert.Throws<KafkaException>(() => handle.Position(unassigned));

                Assert.Equal(viaConsumer.Code, viaHandle.Code);
                Assert.Equal(viaConsumer.Message, viaHandle.Message);
                Assert.Contains(
                    "You can only check the position for partitions assigned to this consumer.",
                    viaHandle.Message,
                    StringComparison.Ordinal);
            },
            s_deadline);
    }

    // ---- Test 7 · non-ASCII round-trip through a handle result (ffi §B3) ----

    /// <summary>
    /// A non-ASCII topic set on the consumer comes back through a <b>handle</b> result
    /// unchanged — the receive-path UTF-8 marshalling of the handle's own getters, which is a
    /// distinct code path from the consumer's (a different P/Invoke, same marshaller).
    /// </summary>
    /// <remarks>
    /// Driven against a <b>real</b> consumer because a mock-derived handle's getters are always
    /// empty, so the mock cannot carry a topic out through one. <c>Assign</c> is a local
    /// operation and needs no broker.
    /// </remarks>
    [Fact]
    public void NonAsciiTopic_RoundTripsThroughAHandleResult()
    {
        using KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<byte[], byte[]>(RealConfig("p8-non-ascii"), Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(
            () =>
            {
                consumer.Assign(new[] { new TopicPartition(NonAsciiTopic, 3) });
                using ConsumerHandle handle = consumer.Handle();

                TopicPartition viaHandle = Assert.Single(handle.Assignment());
                Assert.Equal(NonAsciiTopic, viaHandle.Topic);
                Assert.Equal(3, viaHandle.Partition);

                // The consumer's own getter agrees — so the handle is reading the same state,
                // not a stale or separately-decoded copy.
                Assert.Equal(consumer.Assignment(), handle.Assignment());
            },
            s_deadline);
    }

    // ---- helpers ----

    // Interface-typed accessors: calling Handle() through IConsumer / IAsyncConsumer is what
    // pins P8-D3 (the member lives on IConsumerCommon, so it is reachable from wherever a user
    // actually holds a consumer). A concrete-typed call would not.
    private static ConsumerHandle HandleOf(IConsumer<byte[], byte[]> consumer) => consumer.Handle();

    private static ConsumerHandle HandleOf(IAsyncConsumer<byte[], byte[]> consumer) => consumer.Handle();

    /// <summary>
    /// Runs <paramref name="probe"/> inside <c>OnPartitionsAssigned</c>, which
    /// <c>MockConsumer.Rebalance</c> fires synchronously while holding the core's access guard.
    /// </summary>
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
}
