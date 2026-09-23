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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The native-call layer behind the public <see cref="Confluent.Kafka.ConsumerHandle"/> — the
/// reentrancy-handle sibling of <see cref="NativeConsumer"/> (M9/P8). It owns the
/// <see cref="SafeConsumerReentrancyHandle"/>, validates preconditions before every P/Invoke
/// (ffi §B5), pins call-scoped, and copies out every owned container through the marshaller
/// the consumer's own equivalent already uses.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every op here is synchronous, and deliberately does NOT take the access guard.</b>
/// "Nothing in this module acquires the single-owner access guard ... That is deliberate and
/// is the whole reason the type exists" (<c>src/ffi/consumer_handle.rs:29-31</c>). The core
/// drives each future to completion with <c>block_on</c> on the <em>calling</em> thread, which
/// is safe from the core's callback-dispatcher thread and from any plain OS thread, and is
/// <b>not</b> the forbidden managed sync-over-async (the <c>block_on</c> is the core's, inside
/// the sync ABI — the shipped <c>Seek</c> / <c>CurrentLag</c> precedent; CLAUDE.md §4).
/// </para>
/// <para>
/// <b>No callbacks, no <c>GCHandle</c>, no free-site rule.</b> None of the 22 entry points
/// takes a callback or a <c>user_data_destroy</c>, so ffi §B6's free-site rule ("hook present
/// ⇒ the hook is the sole free site; no hook ⇒ the callback frees") does not engage anywhere
/// in this file. There is nothing to root and nothing to release.
/// </para>
/// <para>
/// <b>Shared helpers, not cloned ones.</b> The snapshot / pin / copy-out-then-destroy helpers
/// come from <see cref="NativeConsumer"/> (widened to <c>internal</c> for exactly this) so the
/// handle family cannot silently diverge from the consumer family it mirrors (DoD §6).
/// </para>
/// </remarks>
internal sealed class NativeConsumerHandle : IDisposable
{
    private readonly SafeConsumerReentrancyHandle _handle;

    private NativeConsumerHandle(SafeConsumerReentrancyHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// The inner reentrancy <c>SafeHandle</c>. Exposed so a test can take a
    /// <c>DangerousAddRef</c> on it — standing in for the call-scoped reference the interop
    /// marshaller holds while a handle op is blocked inside the core — and thereby grade the
    /// P8-D1 release-site ordering, which is otherwise undetectable (see
    /// <c>ReentrancyHandleSafeHandle_DefersTheParentRelease_WhileAnOpHoldsIt</c>).
    /// </summary>
    internal SafeConsumerReentrancyHandle NativeHandle => _handle;

    /// <summary>
    /// Creates a reentrancy handle for <paramref name="consumer"/> via
    /// <c>kafka_consumer_Consumer_handle</c>, and hands the parent's ref-count to the returned
    /// <see cref="SafeConsumerReentrancyHandle"/> (P8-D1; see that type's remarks). The ABI
    /// call "never fails with <c>ConcurrentModificationError</c>" and returns a non-null
    /// handle, so there is no error out-param to read.
    /// </summary>
    /// <remarks>
    /// <b>Exactly one</b> <c>DangerousAddRef</c> is taken, here, <b>before</b> the native call —
    /// so the consumer cannot be destroyed across handle creation, and so the count balances
    /// the single <c>DangerousRelease</c> in
    /// <see cref="SafeConsumerReentrancyHandle.ReleaseHandle"/>. The <c>catch</c> releases it on
    /// every failure path, and <see cref="SafeConsumerReentrancyHandle.AdoptParentReference"/>
    /// takes over ownership on the success path (hence <c>parentRefTaken = false</c>, which is
    /// what stops the <c>catch</c> releasing a reference the handle now owns).
    /// </remarks>
    /// <param name="consumer">The owning consumer's handle.</param>
    internal static NativeConsumerHandle Create(SafeConsumerHandle consumer)
    {
        bool parentRefTaken = false;
        SafeConsumerReentrancyHandle? native = null;
        try
        {
            consumer.DangerousAddRef(ref parentRefTaken);
            native = NativeMethods.ConsumerGetHandle(consumer);
            if (native.IsInvalid)
            {
                // The header contracts a non-null return; treat a null as a hard failure
                // rather than handing out a handle whose every op would be undefined.
                throw new InvalidOperationException(
                    "The consumer did not return a reentrancy handle.");
            }

            native.AdoptParentReference(consumer);
            parentRefTaken = false;
            return new NativeConsumerHandle(native);
        }
        catch
        {
            // IsInvalid ⇒ ReleaseHandle is skipped by the CLR, so this is a no-op destroy on
            // the null-return path and a real one if TransferParentReference threw.
            native?.Dispose();
            if (parentRefTaken)
            {
                consumer.DangerousRelease();
            }

            throw;
        }
    }

    // ---- Sync getters (work on a mock; non-null, never a concurrent-null) ----

    /// <summary>
    /// <c>ConsumerHandle_assignment</c> — copied out and freed via
    /// <see cref="TopicPartitionListMarshal.CopyOutAndDestroy"/>. Unlike
    /// <see cref="NativeConsumer.Assignment"/> there is no
    /// <see cref="InvalidOperationException"/> concurrent-null mapping: the ABI documents this
    /// return as non-null because the handle takes no guard, so there is no rejection to
    /// signal. Always empty on a mock-derived handle (core behavior).
    /// </summary>
    internal IReadOnlyCollection<TopicPartition> Assignment() =>
        TopicPartitionListMarshal.CopyOutAndDestroy(NativeMethods.ConsumerHandleAssignment(_handle));

    /// <summary>
    /// <c>ConsumerHandle_subscription</c> — copied out and freed via
    /// <see cref="StringListMarshal.CopyOutAndDestroy"/>. Non-null; always empty on a
    /// mock-derived handle.
    /// </summary>
    internal IReadOnlyCollection<string> Subscription() =>
        StringListMarshal.CopyOutAndDestroy(NativeMethods.ConsumerHandleSubscription(_handle));

    /// <summary>
    /// <c>ConsumerHandle_paused</c> — copied out and freed via
    /// <see cref="TopicPartitionListMarshal.CopyOutAndDestroy"/>. Non-null; always empty on a
    /// mock-derived handle.
    /// </summary>
    internal IReadOnlyCollection<TopicPartition> Paused() =>
        TopicPartitionListMarshal.CopyOutAndDestroy(NativeMethods.ConsumerHandlePaused(_handle));

    /// <summary>
    /// <c>ConsumerHandle_wakeup</c> — wakes the owning consumer. Neither blocks nor takes the
    /// guard, and works on a mock-derived handle.
    /// </summary>
    internal void Wakeup() => NativeMethods.ConsumerHandleWakeup(_handle);

    // ---- Collection ops (error-only) ----

    /// <summary><c>ConsumerHandle_assign</c>. An empty collection is rejected by the core.</summary>
    internal void Assign(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOp(partitions, NativeMethods.ConsumerHandleAssign);

    /// <summary><c>ConsumerHandle_pause</c>.</summary>
    internal void Pause(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOp(partitions, NativeMethods.ConsumerHandlePause);

    /// <summary><c>ConsumerHandle_resume</c>.</summary>
    internal void Resume(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOp(partitions, NativeMethods.ConsumerHandleResume);

    /// <summary><c>ConsumerHandle_seek_to_beginning</c>.</summary>
    internal void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOp(partitions, NativeMethods.ConsumerHandleSeekToBeginning);

    /// <summary><c>ConsumerHandle_seek_to_end</c>.</summary>
    internal void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOp(partitions, NativeMethods.ConsumerHandleSeekToEnd);

    // ---- Single-partition ops ----

    /// <summary>
    /// <c>ConsumerHandle_seek</c>. Carries the same Java-fidelity negative-offset guard as
    /// <see cref="NativeConsumer.Seek(string, int, long)"/> — the exact message is part of the
    /// contract (DoD §3).
    /// </summary>
    internal void Seek(TopicPartition partition, long offset)
    {
        ValidatePartition(partition, nameof(partition));
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset), offset, "seek offset must not be a negative number");
        }

        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(partition.Topic);
        ThrowIfFailed(NativeMethods.ConsumerHandleSeek(
            _handle, topicPin.Pointer, partition.Partition, offset));
    }

    /// <summary>
    /// <c>ConsumerHandle_seek_with_metadata</c>. No offset guard (the
    /// <see cref="OffsetAndMetadata"/> ctor is the upstream gate); a null leader epoch maps to
    /// the ABI's <c>-1</c> "no epoch" sentinel, and <see cref="OffsetAndMetadata.Metadata"/> is
    /// never null, so a valid pointer is always pinned and passed.
    /// </summary>
    internal void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)
    {
        ValidatePartition(partition, nameof(partition));
        if (offsetAndMetadata is null)
        {
            throw new ArgumentNullException(nameof(offsetAndMetadata));
        }

        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(partition.Topic);
        using Utf8Marshal.PinnedUtf8String metadataPin = Utf8Marshal.Pin(offsetAndMetadata.Metadata);
        ThrowIfFailed(NativeMethods.ConsumerHandleSeekWithMetadata(
            _handle,
            topicPin.Pointer,
            partition.Partition,
            offsetAndMetadata.Offset,
            offsetAndMetadata.LeaderEpoch ?? -1,
            metadataPin.Pointer));
    }

    /// <summary>
    /// <c>ConsumerHandle_position</c> — the offset is written to the out-param only on
    /// success, so the error is read and thrown before the (unset) value is returned.
    /// </summary>
    internal long Position(TopicPartition partition)
    {
        ValidatePartition(partition, nameof(partition));

        long position;
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(partition.Topic))
        {
            error = NativeMethods.ConsumerHandlePosition(
                _handle, topicPin.Pointer, partition.Partition, out position);
        }

        ThrowIfFailed(error);
        return position;
    }

    /// <summary>
    /// <c>ConsumerHandle_position_timeout</c> — the timeout-bounded twin of
    /// <see cref="Position(TopicPartition)"/>.
    /// </summary>
    internal long Position(TopicPartition partition, long timeoutMs)
    {
        ValidatePartition(partition, nameof(partition));

        long position;
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(partition.Topic))
        {
            error = NativeMethods.ConsumerHandlePositionTimeout(
                _handle, topicPin.Pointer, partition.Partition, timeoutMs, out position);
        }

        ThrowIfFailed(error);
        return position;
    }

    // ---- Owned-container queries ----

    /// <summary><c>ConsumerHandle_committed</c>.</summary>
    internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(
        IReadOnlyCollection<TopicPartition> partitions) =>
        RunContainerQuery(
            partitions,
            NativeMethods.ConsumerHandleCommitted,
            OffsetMapMarshal.CopyOut,
            NativeMethods.OffsetMapDestroy);

    /// <summary><c>ConsumerHandle_beginning_offsets</c>.</summary>
    internal IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions) =>
        RunContainerQuery(
            partitions,
            NativeMethods.ConsumerHandleBeginningOffsets,
            LongOffsetMapMarshal.CopyOut,
            NativeMethods.LongOffsetMapDestroy);

    /// <summary><c>ConsumerHandle_end_offsets</c>.</summary>
    internal IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions) =>
        RunContainerQuery(
            partitions,
            NativeMethods.ConsumerHandleEndOffsets,
            LongOffsetMapMarshal.CopyOut,
            NativeMethods.LongOffsetMapDestroy);

    /// <summary>
    /// <c>ConsumerHandle_offsets_for_times</c> — the one map-input query; unresolved
    /// partitions are omitted from the result.
    /// </summary>
    internal IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch)
    {
        NativeConsumer.TimestampsSnapshot snapshot = NativeConsumer.SnapshotTimestamps(timestampsToSearch);

        // Pre-init: the ABI leaves *out_map untouched on failure, so the failure path must
        // yield IntPtr.Zero for the null-safe destroy (the sync-query discipline).
        IntPtr error = IntPtr.Zero;
        IntPtr map = IntPtr.Zero;
        NativeConsumer.WithPinnedTopicsAndTimestamps(
            snapshot.Count,
            i => snapshot.Topics[i],
            snapshot.Partitions,
            snapshot.Timestamps,
            (topics, parts, timestamps, count) =>
                error = NativeMethods.ConsumerHandleOffsetsForTimes(
                    _handle, topics, parts, timestamps, count, out map));

        return NativeConsumer.ThrowOrCopyOutAndDestroy(
            error, map, OffsetAndTimestampMapMarshal.CopyOut, NativeMethods.OffsetAndTimestampMapDestroy);
    }

    // ---- Commits ----

    /// <summary><c>ConsumerHandle_commit_sync</c> — Java <c>commitSync()</c>.</summary>
    internal void CommitSync() => ThrowIfFailed(NativeMethods.ConsumerHandleCommitSync(_handle));

    /// <summary><c>ConsumerHandle_commit_sync_offsets</c> — Java <c>commitSync(Map)</c>.</summary>
    internal void CommitSyncOffsets(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        RunCommitOffsets(offsets, NativeMethods.ConsumerHandleCommitSyncOffsets);

    /// <summary><c>ConsumerHandle_commit_async</c> — Java <c>commitAsync()</c>, fire-and-forget.</summary>
    internal void CommitAsync() => ThrowIfFailed(NativeMethods.ConsumerHandleCommitAsync(_handle));

    /// <summary><c>ConsumerHandle_commit_async_offsets</c> — Java <c>commitAsync(Map)</c>.</summary>
    internal void CommitAsyncOffsets(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        RunCommitOffsets(offsets, NativeMethods.ConsumerHandleCommitAsyncOffsets);

    /// <summary>
    /// Releases the reentrancy handle, which destroys it natively and then releases the
    /// consumer ref-count it holds (in that order — see
    /// <see cref="SafeConsumerReentrancyHandle"/>). Idempotent via
    /// <see cref="System.Runtime.InteropServices.SafeHandle"/>.
    /// </summary>
    public void Dispose() => _handle.Dispose();

    // ---- Shared tails ----

    private delegate IntPtr PartitionOp(
        SafeConsumerReentrancyHandle handle, IntPtr[] topics, int[] partitions, int count);

    private delegate IntPtr ContainerQuery(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        int count,
        out IntPtr outMap);

    private delegate IntPtr CommitOffsetsOp(
        SafeConsumerReentrancyHandle handle,
        IntPtr[] topics,
        int[] partitions,
        long[] offsets,
        int[] leaderEpochs,
        IntPtr[] metadata,
        int count);

    private static void ValidatePartition(TopicPartition partition, string parameterName)
    {
        // Preconditions BEFORE any pin / P-Invoke (§B5) — the ABI does not validate them.
        if (partition.Topic is null)
        {
            throw new ArgumentNullException(parameterName, "TopicPartition.Topic must not be null.");
        }

        if (partition.Partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                parameterName, partition.Partition, "Partition must not be negative.");
        }
    }

    private static void ThrowIfFailed(IntPtr error)
    {
        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    private void RunPartitionOp(IReadOnlyCollection<TopicPartition> partitions, PartitionOp submit)
    {
        (string Topic, int Partition)[] snapshot = NativeConsumer.SnapshotPartitions(partitions);

        IntPtr error = IntPtr.Zero;
        NativeConsumer.WithPinnedTopics(
            snapshot.Length,
            i => snapshot[i].Topic,
            NativeConsumer.ExtractPartitions(snapshot),
            (topics, parts, count) => error = submit(_handle, topics, parts, count));

        ThrowIfFailed(error);
    }

    private TResult RunContainerQuery<TResult>(
        IReadOnlyCollection<TopicPartition> partitions,
        ContainerQuery submit,
        Func<IntPtr, TResult> copyOut,
        Action<IntPtr> destroy)
    {
        (string Topic, int Partition)[] snapshot = NativeConsumer.SnapshotPartitions(partitions);

        // Pre-init: the ABI leaves *out_map untouched on failure (the sync-query discipline).
        IntPtr error = IntPtr.Zero;
        IntPtr container = IntPtr.Zero;
        NativeConsumer.WithPinnedTopics(
            snapshot.Length,
            i => snapshot[i].Topic,
            NativeConsumer.ExtractPartitions(snapshot),
            (topics, parts, count) => error = submit(_handle, topics, parts, count, out container));

        return NativeConsumer.ThrowOrCopyOutAndDestroy(error, container, copyOut, destroy);
    }

    private void RunCommitOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, CommitOffsetsOp submit)
    {
        NativeConsumer.CommitOffsetsSnapshot snapshot = NativeConsumer.SnapshotCommitOffsets(offsets);

        IntPtr error = IntPtr.Zero;
        NativeConsumer.WithPinnedCommitOffsets(snapshot, (topics, parts, offs, epochs, meta, count) =>
            error = submit(_handle, topics, parts, offs, epochs, meta, count));

        ThrowIfFailed(error);
    }
}
