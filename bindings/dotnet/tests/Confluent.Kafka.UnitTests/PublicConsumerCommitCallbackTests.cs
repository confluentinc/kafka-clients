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
using System.Text;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M9/P7 offset-commit-callback surface through the <b>public</b> API, broker-free: the
/// two new <c>CommitAsync</c> overloads on <see cref="IConsumerCommon"/> (Java
/// <c>commitAsync(OffsetCommitCallback)</c> / <c>commitAsync(Map, OffsetCommitCallback)</c>,
/// including its legal <c>null</c>-callback form) across all four consumer types.
/// </summary>
/// <remarks>
/// <para>
/// <b>Mock determinism.</b> On a <c>MockConsumer</c> the core invokes the callback
/// <b>inline during the commit call</b> with a null error
/// (<c>confluent_kafka.h:2482-2485</c>), so every assertion below is synchronous — no polling,
/// no <c>wait_for</c>.
/// </para>
/// <para>
/// <b>Reachability limit, stated rather than asserted weakly.</b> Because that mock error is
/// <em>always</em> null, a <b>broker-originated</b> commit failure delivered <em>to</em> the
/// callback has no broker-free vehicle. That channel is covered instead by driving the
/// trampoline the way the core would, in
/// <c>Interop/ConsumerCommitCallbackBridgeTests.CommitTrampoline_NonNullError_...</c> (code +
/// exact message). The other channel — the <b>initiation</b> failure this method throws
/// synchronously — is covered there too, via the ABI marshal-failure path
/// (a negative offset), which the managed <see cref="OffsetAndMetadata"/> constructor makes
/// unreachable from this surface by construction.
/// </para>
/// <para>
/// <b>DoD #10 (hot-path allocation audit): N/A.</b> A commit callback fires per commit, not
/// per record.
/// </para>
/// </remarks>
public sealed class PublicConsumerCommitCallbackTests
{
    private const string Topic = "commit-callback-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    // ---- The callback receives the committed offsets (VALUES, not just non-empty) ----

    [Fact]
    public void CommitAsyncWithOffsetsAndCallback_DeliversThoseOffsetsAndANullException()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        TopicPartition first = new TopicPartition(Topic, 0);
        TopicPartition second = new TopicPartition(Topic, 1);
        consumer.Assign(new[] { first, second });
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [first] = new OffsetAndMetadata(42, "meta-x", 7),
                [second] = new OffsetAndMetadata(9),
            },
            callback);

        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception) completion =
            Assert.Single(callback.Completions);
        Assert.Null(completion.Exception);
        Assert.Equal(2, completion.Offsets.Count);

        // The VALUES, not merely non-empty: offset, metadata and leader epoch all round-trip.
        Assert.Equal(42, completion.Offsets[first].Offset);
        Assert.Equal("meta-x", completion.Offsets[first].Metadata);
        Assert.Equal(7, completion.Offsets[first].LeaderEpoch);
        Assert.Equal(9, completion.Offsets[second].Offset);
        Assert.Equal(string.Empty, completion.Offsets[second].Metadata);
        Assert.Null(completion.Offsets[second].LeaderEpoch);
    }

    [Fact]
    public void CommitAsyncWithCallback_NoOffsets_FiresWithTheCurrentPositions()
    {
        // Java commitAsync(OffsetCommitCallback): commits what has been consumed. A freshly
        // created mock has consumed nothing, so the delivered map is empty — which is exactly
        // the contract ("always non-null; may be empty"), not a missing assertion.
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(callback);

        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception) completion =
            Assert.Single(callback.Completions);
        Assert.Null(completion.Exception);
        Assert.NotNull(completion.Offsets);
        Assert.Empty(completion.Offsets);
    }

    [Fact]
    public async Task AsyncMock_CommitAsyncWithOffsetsAndCallback_DeliversThoseOffsets()
    {
        await using AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        TopicPartition partition = new TopicPartition(Topic, 2);
        await TestTimeout.Run(() => consumer.Assign(new[] { partition }), s_deadline);
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(5, "m") },
            callback);

        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception) completion =
            Assert.Single(callback.Completions);
        Assert.Null(completion.Exception);
        Assert.Equal(5, completion.Offsets[partition].Offset);
        Assert.Equal("m", completion.Offsets[partition].Metadata);
    }

    [Fact]
    public void CommitAsyncWithOffsetsAndCallback_EmptyMap_CommitsNothingAndStillFires()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(new Dictionary<TopicPartition, OffsetAndMetadata>(), callback);

        Assert.Empty(Assert.Single(callback.Completions).Offsets);
    }

    // ---- The discard path: Java's commitAsync(Map, null) ----

    [Fact]
    public void CommitAsyncWithOffsets_NoCallback_StillCommits()
    {
        // The ABI's `callback` parameter is NOT nullable and there is no plain
        // Consumer_commit_async_offsets, so this overload is backed by the CommitDiscard no-op
        // trampoline (C's discard_commit_complete shape). End-to-end proof that the discard
        // path actually commits: read the offsets back.
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        TopicPartition partition = new TopicPartition(Topic, 3);
        consumer.Assign(new[] { partition });

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [partition] = new OffsetAndMetadata(31, "discard-meta", 2),
            });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> committed =
            consumer.Committed(new[] { partition });
        Assert.Equal(31, committed[partition].Offset);
        Assert.Equal("discard-meta", committed[partition].Metadata);
        Assert.Equal(2, committed[partition].LeaderEpoch);
    }

    [Fact]
    public void CommitAsyncWithOffsets_NoCallback_Churned_StaysHealthy()
    {
        // The discard trampoline frees both delivered handles on every fire. A leak is not
        // observable from managed code, but a double free or a missing destroy that corrupts
        // the allocator would surface here rather than in a single-shot test.
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        TopicPartition partition = new TopicPartition(Topic, 4);
        consumer.Assign(new[] { partition });

        for (int i = 0; i < 500; i++)
        {
            consumer.CommitAsync(
                new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(i) });
        }

        Assert.Equal(499, consumer.Committed(new[] { partition })[partition].Offset);
    }

    // ---- A throwing callback is swallowed, and traced ----

    [Fact]
    public void ThrowingCallback_IsSwallowed_AndTheConsumerStaysUsable()
    {
        // Java's onComplete returns void and the ABI typedef returns void, so there is no
        // channel to report a callback's own failure on: it is caught at the boundary (never
        // unwound into native) and swallowed. The commit itself still lands, and the consumer
        // is still usable afterwards.
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        TopicPartition partition = new TopicPartition(Topic, 5);
        consumer.Assign(new[] { partition });
        ThrowingCommitCallback throwing = new ThrowingCommitCallback("boom");

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(3) },
            throwing);

        Assert.True(throwing.WasInvoked);

        // Still usable: another commit with a well-behaved callback works, and both commits
        // landed.
        RecordingCommitCallback recording = new RecordingCommitCallback();
        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(4) },
            recording);
        Assert.Single(recording.Completions);
        Assert.Equal(4, consumer.Committed(new[] { partition })[partition].Offset);
    }

    [Fact]
    public void ThrowingCallback_IsTraced_NotSilentlyDiscarded()
    {
        // P7-D3 option (b): swallow AND write to System.Diagnostics.Trace. Python's adapter
        // logs and swallows; swallowing silently would match only half of that and would leave
        // no trace at all of a user callback that failed. Trace is the whole of the binding's
        // diagnostics — no logging abstraction, no dependency, no public API.
        string marker = "trace-marker-" + Guid.NewGuid().ToString("N");
        CapturingTraceListener listener = new CapturingTraceListener();
        Trace.Listeners.Add(listener);
        try
        {
            using MockConsumer<byte[], byte[]> consumer = NewMock();
            consumer.CommitAsync(new ThrowingCommitCallback(marker));
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        Assert.Contains(listener.Lines, line => line.Contains(marker, StringComparison.Ordinal));
    }

    [Fact]
    public void TraceAttribution_NamesTheUserCallback_OnlyWhenTheUserCallbackThrew()
    {
        // M9/P7 review, finding 2. The trace helper serves THREE catch sites, only one of
        // which is the user's callback. A fixed "an IOffsetCommitCallback threw" message
        // misattributed a marshalling / GCHandle-recovery fault to the user's code — and on
        // the discard path named a callback that does not exist. A confidently-wrong
        // diagnostic is a worse debugging cliff than a silent one, which is the whole thing
        // P7-D3 exists to avoid.
        //
        // Here the USER callback throws, so naming it is correct.
        string marker = "attribution-" + Guid.NewGuid().ToString("N");
        CapturingTraceListener listener = new CapturingTraceListener();
        Trace.Listeners.Add(listener);
        try
        {
            using MockConsumer<byte[], byte[]> consumer = NewMock();
            consumer.CommitAsync(new ThrowingCommitCallback(marker));
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        string line = Assert.Single(listener.Lines, l => l.Contains(marker, StringComparison.Ordinal));
        Assert.Contains("IOffsetCommitCallback", line, StringComparison.Ordinal);
        // ...and NOT blamed on the trampoline, which did its job.
        Assert.DoesNotContain("before reaching the callback", line, StringComparison.Ordinal);
        Assert.DoesNotContain("discard trampoline", line, StringComparison.Ordinal);
    }

    // ---- Non-ASCII round-trip through the delivered offsets (ffi §B3) ----

    [Fact]
    public void NonAsciiTopic_RoundTripsThroughTheDeliveredOffsets()
    {
        // Guards a MarshalAs(LPStr) mistake, which corrupts non-ASCII silently and hides in
        // ASCII-only tests. The delivered map's topic strings are the NUL-terminated,
        // handle-owned form (OffsetMapMarshal), copied out before the map root is destroyed.
        const string NonAscii = "témas-日本語-🎉";
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        TopicPartition partition = new TopicPartition(NonAscii, 0);
        consumer.Assign(new[] { partition });
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [partition] = new OffsetAndMetadata(12, "métadonnées-メタ"),
            },
            callback);

        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception) completion =
            Assert.Single(callback.Completions);
        KeyValuePair<TopicPartition, OffsetAndMetadata> entry = Assert.Single(completion.Offsets);
        Assert.Equal(NonAscii, entry.Key.Topic);
        Assert.Equal(0, entry.Key.Partition);
        Assert.Equal(12, entry.Value.Offset);
        Assert.Equal("métadonnées-メタ", entry.Value.Metadata);
    }

    // ---- Preconditions (validated BEFORE any native call, ffi §B5) ----

    [Fact]
    public void CommitAsync_NullCallback_ThrowsArgumentNullException()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => consumer.CommitAsync((IOffsetCommitCallback)null!));

        Assert.Equal("callback", error.ParamName);
    }

    [Fact]
    public void CommitAsync_NullOffsets_ThrowsArgumentNullException()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => consumer.CommitAsync(
                (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)null!,
                new RecordingCommitCallback()));

        Assert.Equal("offsets", error.ParamName);
    }

    [Fact]
    public void CommitAsync_NegativePartition_ThrowsArgumentOutOfRangeBeforeAnyNativeCall()
    {
        using MockConsumer<byte[], byte[]> consumer = NewMock();
        RecordingCommitCallback callback = new RecordingCommitCallback();

        Assert.Throws<ArgumentOutOfRangeException>(() => consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [new TopicPartition(Topic, -1)] = new OffsetAndMetadata(1),
            },
            callback));

        // Rejected before the P/Invoke, so the callback was never registered and never fires.
        Assert.Empty(callback.Completions);
    }

    [Fact]
    public void CommitAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.CommitAsync(new RecordingCommitCallback()));
        Assert.Throws<ObjectDisposedException>(() => consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>()));
    }

    // ---- The overloads exist on every consumer type and interface ----

    [Theory]
    [InlineData(typeof(IConsumerCommon))]
    [InlineData(typeof(IConsumer<byte[], byte[]>))]
    [InlineData(typeof(IAsyncConsumer<byte[], byte[]>))]
    [InlineData(typeof(KafkaConsumer<byte[], byte[]>))]
    [InlineData(typeof(AsyncKafkaConsumer<byte[], byte[]>))]
    [InlineData(typeof(MockConsumer<byte[], byte[]>))]
    [InlineData(typeof(AsyncMockConsumer<byte[], byte[]>))]
    public void BothOverloads_AreReachableFromEverySurface(Type surface)
    {
        // The real clients cannot be driven broker-free, so this pins the SHAPE across all
        // four implementations plus the three interfaces. Behaviour is covered against the
        // mocks above.
        Assert.NotNull(FindMethod(surface, typeof(IOffsetCommitCallback)));
        Assert.NotNull(FindMethod(
            surface,
            typeof(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>),
            typeof(IOffsetCommitCallback)));
    }

    /// <summary>
    /// Finds a <c>CommitAsync</c> overload on <paramref name="surface"/>, searching inherited
    /// interfaces too. <see cref="Type.GetMethod(string, Type[])"/> on an <em>interface</em>
    /// does not walk base interfaces, so <see cref="IConsumer{TKey, TValue}"/> /
    /// <see cref="IAsyncConsumer{TKey, TValue}"/> would report the inherited
    /// <see cref="IConsumerCommon"/> members as missing — a reflection quirk, not a shape gap.
    /// </summary>
    private static System.Reflection.MethodInfo? FindMethod(Type surface, params Type[] parameters) =>
        surface.GetMethod("CommitAsync", parameters)
        ?? surface.GetInterfaces()
            .Select(i => i.GetMethod("CommitAsync", parameters))
            .FirstOrDefault(m => m is not null);

    [Fact]
    public void CommitAsyncOverloads_ReturnVoid_NotTask()
    {
        // The §4 commit-callback divergence: the callback carries the offsets a Task cannot,
        // and the ABI typedef returns void — so these stay sync `void`, like Java's, rather
        // than being replaced by a Task (which is what §4's "takes a completion callback" row
        // says for every OTHER such callback).
        Assert.Equal(
            typeof(void),
            typeof(IConsumerCommon).GetMethod("CommitAsync", new[] { typeof(IOffsetCommitCallback) })!.ReturnType);
        Assert.Equal(
            typeof(void),
            typeof(IOffsetCommitCallback).GetMethod(nameof(IOffsetCommitCallback.OnComplete))!.ReturnType);
    }

    // ---- Fixtures ----

    private sealed class RecordingCommitCallback : IOffsetCommitCallback
    {
        internal List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Offsets, KafkaException? Exception)>
            Completions
        { get; } =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>, KafkaException?)>();

        public void OnComplete(
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception) =>
            Completions.Add((offsets, exception));
    }

    private sealed class ThrowingCommitCallback : IOffsetCommitCallback
    {
        private readonly string _message;

        internal ThrowingCommitCallback(string message)
        {
            _message = message;
        }

        internal bool WasInvoked { get; private set; }

        public void OnComplete(
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception)
        {
            WasInvoked = true;
            throw new InvalidOperationException(_message);
        }
    }

    /// <summary>
    /// Captures whatever the binding writes to <see cref="Trace"/>. Registered only for the
    /// duration of one test and matched on a per-test unique marker, so assembly-wide parallel
    /// execution cannot make it observe another test's output.
    /// </summary>
    private sealed class CapturingTraceListener : TraceListener
    {
        private readonly StringBuilder _pending = new StringBuilder();

        internal List<string> Lines { get; } = new List<string>();

        public override void Write(string? message) => _pending.Append(message);

        public override void WriteLine(string? message)
        {
            _pending.Append(message);
            lock (Lines)
            {
                Lines.Add(_pending.ToString());
            }

            _pending.Clear();
        }
    }
}
