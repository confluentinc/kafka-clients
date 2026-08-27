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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface tests for the four M5/P4 offset-map query siblings —
/// <see cref="IAsyncConsumer.Committed"/> / <see cref="IAsyncConsumer.OffsetsForTimes"/> /
/// <see cref="IAsyncConsumer.BeginningOffsets"/> / <see cref="IAsyncConsumer.EndOffsets"/> —
/// exercised end to end through the <b>public</b> <see cref="AsyncMockConsumer"/> /
/// <see cref="IAsyncConsumer"/> surface (PLAN §7). The owned-handle <c>OffsetMap_t</c> /
/// <c>OffsetAndTimestampMap_t</c> / <c>LongOffsetMap_t</c> completion bridges, the copy-out
/// marshallers, the two input shapes (TP collection + TP→timestamp map), the faulted paths
/// (message asserted, DoD §3), preconditions, lifecycle, and cancellation.
/// </summary>
/// <remarks>
/// <b>Reachability (PLAN §6).</b> <see cref="IAsyncConsumer.BeginningOffsets"/> /
/// <see cref="IAsyncConsumer.EndOffsets"/> are fully data-testable broker-free via the
/// shipped <see cref="AsyncMockConsumer.UpdateBeginningOffset"/> /
/// <see cref="AsyncMockConsumer.UpdateEndOffset"/>. <see cref="IAsyncConsumer.Committed"/>
/// is empty-only broker-free (the mock's committed map is populated only by the not-yet-wired
/// commit-with-offsets family) — non-empty end-to-end is deferred to the commit phase; the
/// non-empty copy-out is proven by the direct marshaller unit test
/// (<c>OffsetMapMarshalTests</c>). <see cref="IAsyncConsumer.OffsetsForTimes"/> faults with
/// <c>unsupported_version</c> unconditionally on the mock (Java's not-implemented
/// <c>MockConsumer</c>), so only its faulted path is reachable broker-free. Every awaited op
/// runs under a <see cref="TestTimeout"/> hang guard.
/// </remarks>
public sealed class PublicConsumerOffsetQueryTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "offset-query-topic";

    // ---- BeginningOffsets — fully data-testable (PLAN §6/§7 case 1) ----

    [Fact]
    public async Task BeginningOffsets_ReturnsSetOffsets()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdateBeginningOffset(Topic, 0, 5);
        consumer.UpdateBeginningOffset(Topic, 1, 7);

        IReadOnlyDictionary<TopicPartition, long> result = await BeginningOffsetsOf(
            consumer,
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 1) });

        Assert.Equal(2, result.Count);
        Assert.Equal(5, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(7, result[new TopicPartition(Topic, 1)]);
    }

    [Fact]
    public async Task BeginningOffsets_NonAsciiTopic_RoundTripsThroughKeyMarshalling()
    {
        // The map KEY topic is copied out of the borrowed TopicPartition_t element (NUL-scan,
        // §B3); a non-ASCII topic exercises both the input pin AND the receive-path key copy.
        const string nonAscii = "topic-grüße-Ω-🎉";
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdateBeginningOffset(nonAscii, 3, 99);

        IReadOnlyDictionary<TopicPartition, long> result = await BeginningOffsetsOf(
            consumer, new[] { new TopicPartition(nonAscii, 3) });

        Assert.Equal(99, result[new TopicPartition(nonAscii, 3)]);
    }

    [Fact]
    public async Task BeginningOffsets_UnsetPartition_FaultsWithKafkaExceptionMessage()
    {
        // A TP with no beginning offset set faults with the mock's illegal_state message —
        // the behavioral contract (asserted per DoD §3), incl. the TP in the message. Routes
        // through the trampoline's Complete(error) -> FromHandle (error freed once) and FAULTS
        // the Task, not a synchronous throw.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => BeginningOffsetsOf(consumer, new[] { new TopicPartition(Topic, 0) }));

        Assert.Equal(
            $"The partition {Topic}-0 does not have a beginning offset.",
            ex.Message);
    }

    // ---- EndOffsets — symmetric (PLAN §7 case 2) ----

    [Fact]
    public async Task EndOffsets_ReturnsSetOffsets()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdateEndOffset(Topic, 0, 100);
        consumer.UpdateEndOffset(Topic, 2, 250);

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer,
            new[] { new TopicPartition(Topic, 0), new TopicPartition(Topic, 2) });

        Assert.Equal(2, result.Count);
        Assert.Equal(100, result[new TopicPartition(Topic, 0)]);
        Assert.Equal(250, result[new TopicPartition(Topic, 2)]);
    }

    [Fact]
    public async Task EndOffsets_UnsetPartition_FaultsWithKafkaExceptionMessage()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => EndOffsetsOf(consumer, new[] { new TopicPartition(Topic, 4) }));

        Assert.Equal(
            $"The partition {Topic}-4 does not have an end offset.",
            ex.Message);
    }

    [Fact]
    public async Task EndOffsets_AfterFault_ConsumerReusable()
    {
        // The failure is not fatal: after a faulted EndOffsets, a valid query still succeeds
        // (the error handle + GCHandle were freed exactly once, leaving the consumer usable).
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdateEndOffset(Topic, 0, 11);

        await Assert.ThrowsAsync<KafkaException>(
            () => EndOffsetsOf(consumer, new[] { new TopicPartition(Topic, 9) }));

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer, new[] { new TopicPartition(Topic, 0) });
        Assert.Equal(11, result[new TopicPartition(Topic, 0)]);
    }

    // ---- Committed — empty-only broker-free (PLAN §6 Option A / §7 case 4) ----

    [Fact]
    public async Task Committed_EmptyCollection_ReturnsEmptyMap()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = await CommittedOf(
            consumer, Array.Empty<TopicPartition>());

        Assert.Empty(result);
    }

    [Fact]
    public async Task Committed_UncommittedPartition_ReturnsEmptyMap()
    {
        // The mock omits TPs with no committed offset (it loops the requested TPs and includes
        // only those in self.committed). With nothing committed, the result is an empty — but
        // valid, non-null — map (success, not a fault). The non-empty end-to-end assertion,
        // once deferred here to the commit-family phase, is now UNBLOCKED and lives in
        // PublicConsumerCommitTests.Commit_ThenCommitted_RoundTripsOffsetMetadataAndEpoch
        // (M5/P6): Commit(offsets) populates the mock's committed map, then Committed reads
        // the exact value back. The non-empty copy-out in isolation is also proven by
        // OffsetMapMarshalTests.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = await CommittedOf(
            consumer, new[] { new TopicPartition(Topic, 0) });

        Assert.Empty(result);
    }

    // ---- OffsetsForTimes — faulted-only broker-free (PLAN §6 / §7 case 5) ----

    [Fact]
    public async Task OffsetsForTimes_OnMock_FaultsWithUnsupportedVersion()
    {
        // The mock's offsets_for_times returns unsupported_version unconditionally (mirroring
        // Java's not-implemented MockConsumer). Assert the faulted Task + the message content
        // + the UnsupportedVersion code (35). The full member is wired (Java-public); the
        // success/copy-out path is proven by the other two offset-map marshallers of identical
        // shape and by OffsetAndTimestampMapMarshalTests.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = 1_000L,
        };

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => OffsetsForTimesOf(consumer, search));

        Assert.Equal("MockConsumer::offsets_for_times is not implemented", ex.Message);
        Assert.Equal(35, ex.Code); // Errors::UnsupportedVersion
    }

    [Fact]
    public async Task OffsetsForTimes_NegativeTimestamp_Accepted_ThenFaultsOnMock()
    {
        // A NEGATIVE timestamp is a Kafka-valid sentinel (EARLIEST/LATEST) and must NOT be
        // rejected by a precondition — it passes through to the mock, which then faults with
        // unsupported_version (proving the negative value was accepted, not thrown on).
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition(Topic, 0)] = -2L, // LATEST sentinel
        };

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => OffsetsForTimesOf(consumer, search));

        Assert.Equal("MockConsumer::offsets_for_times is not implemented", ex.Message);
    }

    [Fact]
    public async Task OffsetsForTimes_EmptyMap_StillFaultsOnMock()
    {
        // Verified against src/ffi/consumer.rs: offsets_for_times_async does NOT short-circuit
        // empty — it always calls c.offsets_for_times(req), which the mock rejects with
        // unsupported_version even for an empty request (PLAN §7 case 11: the Actor verifies
        // and documents which; the FFI does NOT short-circuit).
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Assert.ThrowsAsync<KafkaException>(
            () => OffsetsForTimesOf(consumer, new Dictionary<TopicPartition, long>()));
    }

    // ---- Empty-input success for the three collection/long-map queries (PLAN §7 case 11) ----

    [Fact]
    public async Task BeginningOffsets_EmptyCollection_ReturnsEmptyMap()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyDictionary<TopicPartition, long> result = await BeginningOffsetsOf(
            consumer, Array.Empty<TopicPartition>());

        Assert.Empty(result);
    }

    [Fact]
    public async Task EndOffsets_EmptyCollection_ReturnsEmptyMap()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer, Array.Empty<TopicPartition>());

        Assert.Empty(result);
    }

    // ---- Preconditions (deterministic, before any native call; PLAN §7 case 8) ----

    [Fact]
    public async Task Committed_NullCollection_ThrowsArgumentNull()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.Committed(null!));
    }

    [Fact]
    public async Task BeginningOffsets_NullCollection_ThrowsArgumentNull()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.BeginningOffsets(null!));
    }

    [Fact]
    public async Task EndOffsets_NullCollection_ThrowsArgumentNull()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.EndOffsets(null!));
    }

    [Fact]
    public async Task OffsetsForTimes_NullMap_ThrowsArgumentNull()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.OffsetsForTimes(null!));
    }

    [Fact]
    public async Task Committed_NullElementTopic_ThrowsArgument()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // default(TopicPartition) has a null Topic (readonly struct) — the reachable way to
        // present a null element topic without TopicPartition's own ctor validation firing.
        await Assert.ThrowsAsync<ArgumentException>(
            () => consumer.Committed(new[] { default(TopicPartition) }));
    }

    [Fact]
    public async Task OffsetsForTimes_NullKeyTopic_ThrowsArgument()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Dictionary<TopicPartition, long> search = new Dictionary<TopicPartition, long>
        {
            [default] = 1L,
        };

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.OffsetsForTimes(search));
    }

    // ---- Lifecycle (PLAN §7 case 9) ----

    [Fact]
    public async Task Committed_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.Committed(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public async Task BeginningOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public async Task EndOffsets_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.EndOffsets(new[] { new TopicPartition(Topic, 0) }));
    }

    [Fact]
    public async Task OffsetsForTimes_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.OffsetsForTimes(new Dictionary<TopicPartition, long>
            {
                [new TopicPartition(Topic, 0)] = 1L,
            }));
    }

    // ---- Cancellation / wakeup (PLAN §7 case 10) ----

    [Fact]
    public async Task BeginningOffsets_PreCanceledToken_ThrowsOperationCanceled()
    {
        // Deterministic: an already-canceled token is honored synchronously BEFORE any native
        // call (OperationCanceledException, distinct from a wakeup KafkaException), via
        // ThrowIfCancellationRequested in SubmitOwnedHandleOperation — user-initiated
        // cancellation, NOT a timeout.
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdateBeginningOffset(Topic, 0, 1);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.BeginningOffsets(new[] { new TopicPartition(Topic, 0) }, cts.Token));
    }

    [Fact]
    public async Task Committed_PreCanceledToken_ThrowsOperationCanceled()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.Committed(new[] { new TopicPartition(Topic, 0) }, cts.Token));
    }

    [Fact]
    public async Task EndOffsets_AfterWakeup_StillUsable()
    {
        // A wakeup() does not corrupt the consumer: the reachable seam (a subsequent EndOffsets
        // on the free guard succeeds) holds, and the consumer stays reusable after a wakeup.
        // (The mock's offset queries do not check-and-clear the wakeup flag, and resolve
        // instantly, so an in-flight overlap is not reproducible broker-free — the D-Q4
        // ceiling, mirroring the Position precedent.)
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.UpdateEndOffset(Topic, 0, 5);

        consumer.Wakeup();

        IReadOnlyDictionary<TopicPartition, long> result = await EndOffsetsOf(
            consumer, new[] { new TopicPartition(Topic, 0) });
        Assert.Equal(5, result[new TopicPartition(Topic, 0)]);
    }

    // ---- Helpers (every awaited op under the TestTimeout hang guard) ----

    private static async Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsetsOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, long> result = null!;
        await TestTimeout.Run(async () => result = await consumer.BeginningOffsets(partitions), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsetsOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, long> result = null!;
        await TestTimeout.Run(async () => result = await consumer.EndOffsets(partitions), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> CommittedOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyCollection<TopicPartition> partitions)
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = null!;
        await TestTimeout.Run(async () => result = await consumer.Committed(partitions), s_deadline);
        return result;
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>> OffsetsForTimesOf(
        IAsyncConsumer<byte[], byte[]> consumer, IReadOnlyDictionary<TopicPartition, long> search)
    {
        IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> result = null!;
        await TestTimeout.Run(async () => result = await consumer.OffsetsForTimes(search), s_deadline);
        return result;
    }
}
