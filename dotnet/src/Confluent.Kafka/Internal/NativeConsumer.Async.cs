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
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The async consumer's operations: each calls its <c>_cb</c> twin, whose completion the consumer's drain thread
/// runs, and holds a use of the handle until the awaiting continuation has finished with the result.
/// </summary>
internal sealed partial class NativeConsumer
{
    internal const string PumpThreadName = "confluent-kafka-consumer-pump";

    private static readonly Func<IntPtr, long, bool> s_void = (_, _) => true;

    private static readonly Func<IntPtr, long, long> s_scalar = (_, value) => value;

    private static readonly Func<IntPtr, long, IntPtr> s_records = (records, _) => records;

    private static readonly Func<IntPtr, long, long?> s_lag = (present, value) => present != IntPtr.Zero ? value : null;

    private static readonly NativeMethods.ConsumerVoidCallback s_closed = OnClosed;

    private readonly CompletionThread? _pump;

    /// <summary>The async operations, named for the Java methods they translate.</summary>
    internal enum AsyncOperation
    {
        Subscribe,
        Unsubscribe,
        Assign,
        Pause,
        Resume,
        SeekToBeginning,
        SeekToEnd,
        Seek,
        Poll,
        Commit,
        CommitAsync,
        Position,
        Committed,
        OffsetsForTimes,
        BeginningOffsets,
        EndOffsets,
        PartitionsFor,
        ListTopics,
        CurrentLag,
        Rebalance,
        LosePartitions,
    }

    private delegate void NativePartitionsCb(
        SafeConsumerHandle consumer, IntPtr partitions, NativeMethods.ConsumerVoidCallback cb, IntPtr opaque);

    private delegate void NativeMockPartitionsCb(
        IntPtr mock, IntPtr partitions, NativeMethods.ConsumerVoidCallback cb, IntPtr opaque);

    // Only the operations Java's wakeup() interrupts are woken by a cancel: a wakeup any other operation leaves
    // unconsumed would fail the next wakeable call.
    internal static bool IsWakeable(AsyncOperation operation) =>
        operation is AsyncOperation.Poll
            or AsyncOperation.Commit
            or AsyncOperation.Position
            or AsyncOperation.Committed
            or AsyncOperation.PartitionsFor
            or AsyncOperation.ListTopics;

    internal Task SubscribeWithCallback(
        IReadOnlyCollection<string> topics,
        CancellationToken cancellationToken = default)
    {
        string[] snapshot = SnapshotTopics(topics);
        RefuseInsideACallback();
        return Finish(
            Submit(
                cancellationToken,
                s_void,
                (arguments, opaque) =>
                    NativeMethods.ConsumerSubscribeWithTopicsCb(
                        _handle, arguments.StringList(snapshot), ConsumerCompletion.Void, opaque),
                failed => SettleListener(null, !failed)),
            AsyncOperation.Subscribe,
            cancellationToken);
    }

    internal Task SubscribeWithCallback(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        if (listener is null)
        {
            throw new ArgumentNullException(nameof(listener));
        }

        return SubscribeWithRegistration(
            topics, () => ListenerRegistration.Root(listener, _reporter), cancellationToken);
    }

    internal Task SubscribeWithCallback(
        IReadOnlyCollection<string> topics,
        IAsyncConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        if (listener is null)
        {
            throw new ArgumentNullException(nameof(listener));
        }

        return SubscribeWithRegistration(
            topics, () => ListenerRegistration.Root(listener, _reporter), cancellationToken);
    }

    internal Task RebalanceWithCallback(IReadOnlyCollection<TopicPartition> partitions) =>
        RunMockPartitions(partitions, NativeMethods.MockConsumerRebalanceCb, AsyncOperation.Rebalance);

    internal Task LosePartitionsWithCallback(IReadOnlyCollection<TopicPartition> partitions) =>
        RunMockPartitions(partitions, NativeMethods.MockConsumerLosePartitionsCb, AsyncOperation.LosePartitions);

    private Task SubscribeWithRegistration(
        IReadOnlyCollection<string> topics,
        Func<ListenerRegistration> root,
        CancellationToken cancellationToken)
    {
        string[] snapshot = SnapshotTopics(topics);
        RefuseInsideACallback();
        ListenerRegistration? registration = null;
        ConsumerCompletion<bool> completion;
        try
        {
            completion = Submit(
                cancellationToken,
                s_void,
                (arguments, opaque) =>
                {
                    IntPtr list = arguments.StringList(snapshot);
                    registration = root();
                    IntPtr listenerHandle = registration.NewHandle();
                    try
                    {
                        NativeMethods.ConsumerSubscribeWithTopicsListenerCb(
                            _handle, list, listenerHandle, ConsumerCompletion.Void, opaque);
                    }
                    finally
                    {
                        ListenerRegistration.DestroyHandle(listenerHandle);
                    }
                },
                failed => SettleListener(registration, !failed));
        }
        catch
        {
            registration?.Release();
            throw;
        }

        return Finish(completion, AsyncOperation.Subscribe, cancellationToken);
    }

    private Task RunMockPartitions(
        IReadOnlyCollection<TopicPartition> partitions,
        NativeMockPartitionsCb call,
        AsyncOperation operation)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        return Finish(
            Submit(CancellationToken.None, s_void, (arguments, opaque) =>
            {
                bool added = false;
                try
                {
                    call(
                        _handle.DangerousAddRefClass(ref added),
                        arguments.TopicPartitionList(snapshot),
                        ConsumerCompletion.Void,
                        opaque);
                }
                finally
                {
                    if (added)
                    {
                        _handle.DangerousRelease();
                    }
                }
            },
            refusesOtherThreads: true),
            operation,
            CancellationToken.None);
    }

    internal Task UnsubscribeWithCallback(CancellationToken cancellationToken = default)
    {
        RefuseInsideACallback();
        return Finish(
            Submit(cancellationToken, s_void, (_, opaque) =>
                NativeMethods.ConsumerUnsubscribeCb(_handle, ConsumerCompletion.Void, opaque)),
            AsyncOperation.Unsubscribe,
            cancellationToken);
    }

    internal Task AssignWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        RunPartitions(SnapshotPartitions(partitions), NativeMethods.ConsumerAssignCb, AsyncOperation.Assign, cancellationToken);

    internal Task PauseWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        RunPartitions(
            CheckInAUse(() => SnapshotPartitions(partitions)),
            NativeMethods.ConsumerPauseCb,
            AsyncOperation.Pause,
            cancellationToken,
            (handle, snapshot) => handle.Pause(snapshot));

    internal Task ResumeWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        RunPartitions(
            CheckInAUse(() => SnapshotPartitions(partitions)),
            NativeMethods.ConsumerResumeCb,
            AsyncOperation.Resume,
            cancellationToken,
            (handle, snapshot) => handle.Resume(snapshot));

    internal Task SeekToBeginningWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        RunPartitions(
            CheckWhenOpen(() => SnapshotPartitions(partitions)),
            NativeMethods.ConsumerSeekToBeginningCb,
            AsyncOperation.SeekToBeginning,
            cancellationToken,
            (handle, snapshot) => handle.SeekToBeginning(snapshot));

    internal Task SeekToEndWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        RunPartitions(
            CheckWhenOpen(() => SnapshotPartitions(partitions)),
            NativeMethods.ConsumerSeekToEndCb,
            AsyncOperation.SeekToEnd,
            cancellationToken,
            (handle, snapshot) => handle.SeekToEnd(snapshot));

    internal Task SeekWithCallback(TopicPartition partition, long offset, CancellationToken cancellationToken = default)
    {
        CheckSeekArguments(partition.Topic, partition.Partition, offset);
        if (RoutesToTheHandle)
        {
            return RouteAsync(cancellationToken, handle => handle.Seek(partition.Topic, partition.Partition, offset));
        }

        return Finish(
            Submit(cancellationToken, s_void, (arguments, opaque) =>
                NativeMethods.ConsumerSeekWithOffsetCb(
                    _handle,
                    arguments.TopicPartition(partition.Topic, partition.Partition),
                    offset,
                    ConsumerCompletion.Void,
                    opaque)),
            AsyncOperation.Seek,
            cancellationToken);
    }

    internal Task SeekWithCallback(
        TopicPartition partition,
        OffsetAndMetadata offsetAndMetadata,
        CancellationToken cancellationToken = default)
    {
        CheckSeekArguments(partition.Topic, partition.Partition, offsetAndMetadata);
        if (RoutesToTheHandle)
        {
            return RouteAsync(
                cancellationToken,
                handle => handle.SeekWithMetadata(partition.Topic, partition.Partition, offsetAndMetadata));
        }

        return Finish(
            Submit(cancellationToken, s_void, (arguments, opaque) =>
                NativeMethods.ConsumerSeekWithOffsetAndMetadataCb(
                    _handle,
                    arguments.TopicPartition(partition.Topic, partition.Partition),
                    arguments.OffsetAndMetadata(
                        offsetAndMetadata.Offset, offsetAndMetadata.LeaderEpoch ?? -1, offsetAndMetadata.Metadata),
                    ConsumerCompletion.Void,
                    opaque)),
            AsyncOperation.Seek,
            cancellationToken);
    }

    internal Task<long?> CurrentLagWithCallback(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        CheckInAUse(() => RequireTopicPartition(partition.Topic, partition.Partition), UseKind.UnownedOnMock);
        if (RoutesToTheHandle)
        {
            return RouteAsync(cancellationToken, handle => handle.CurrentLag(partition.Topic, partition.Partition));
        }

        return Finish(
            Submit(
                cancellationToken,
                s_lag,
                (arguments, opaque) => NativeMethods.ConsumerCurrentLagCb(
                    _handle,
                    arguments.TopicPartition(partition.Topic, partition.Partition),
                    ConsumerCompletion.OptionalScalar,
                    opaque),
                kind: UseKind.UnownedOnMock),
            AsyncOperation.CurrentLag,
            cancellationToken);
    }

    internal Task CommitAsyncWithCallback(CancellationToken cancellationToken = default) =>
        RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.CommitAsync())
            : Finish(
                Submit(cancellationToken, s_void, (_, opaque) =>
                    NativeMethods.ConsumerCommitAsyncCb(_handle, ConsumerCompletion.Void, opaque)),
                AsyncOperation.CommitAsync,
                cancellationToken);

    internal Task CommitAsyncWithCallback(IOffsetCommitCallback callback, CancellationToken cancellationToken = default)
    {
        CheckInAUse(() => RequireCallback(callback));

        if (RoutesToTheHandle)
        {
            return RouteAsync(cancellationToken, handle => handle.CommitAsync(callback));
        }

        return SubmitCommitWithRegistration(emptyOffsets => RootCommitCallback(callback, emptyOffsets), null, cancellationToken);
    }

    internal Task CommitAsyncWithCallback(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback,
        CancellationToken cancellationToken = default)
    {
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsetsWhenOpen(offsets);
        if (RoutesToTheHandle)
        {
            return RouteAsync(cancellationToken, handle => handle.CommitAsync(snapshot, callback));
        }

        return SubmitCommitWithRegistration(
            emptyOffsets => RootCommitCallback(callback, emptyOffsets),
            snapshot,
            cancellationToken);
    }

    internal Task CommitAsyncWithCallback(IAsyncOffsetCommitCallback callback, CancellationToken cancellationToken = default)
    {
        CheckInAUse(() => RequireCallback(callback));

        if (RoutesToTheHandle)
        {
            return RouteAsync(cancellationToken, handle => handle.CommitAsync(callback));
        }

        return SubmitCommitWithRegistration(emptyOffsets => RootCommitCallback(callback, emptyOffsets), null, cancellationToken);
    }

    internal Task CommitAsyncWithCallback(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IAsyncOffsetCommitCallback callback,
        CancellationToken cancellationToken = default)
    {
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsetsWhenOpen(offsets);
        CheckInAUse(() => RequireCallback(callback));

        if (RoutesToTheHandle)
        {
            return RouteAsync(cancellationToken, handle => handle.CommitAsync(snapshot, callback));
        }

        return SubmitCommitWithRegistration(
            emptyOffsets => RootCommitCallback(callback, emptyOffsets),
            snapshot,
            cancellationToken);
    }

    private Task SubmitCommitWithRegistration(
        Func<bool, CommitCallbackRegistration> root,
        CommitOffsetsSnapshot? snapshot,
        CancellationToken cancellationToken)
    {
        CommitCallbackRegistration? registration = null;
        ConsumerCompletion<bool> completion;
        try
        {
            completion = Submit(
                cancellationToken,
                s_void,
                (arguments, opaque) =>
                {
                    IntPtr map = snapshot is CommitOffsetsSnapshot offsets ? arguments.OffsetMap(offsets) : IntPtr.Zero;
                    registration = root(snapshot?.Count == 0);
                    IntPtr callbackHandle = registration.NewHandle();
                    try
                    {
                        if (snapshot is null)
                        {
                            NativeMethods.ConsumerCommitAsyncWithCallbackCb(
                                _handle, callbackHandle, ConsumerCompletion.Void, opaque);
                        }
                        else
                        {
                            NativeMethods.ConsumerCommitAsyncWithOffsetsCallbackCb(
                                _handle, map, callbackHandle, ConsumerCompletion.Void, opaque);
                        }
                    }
                    finally
                    {
                        CommitCallbackRegistration.DestroyHandle(callbackHandle);
                    }
                },
                failed =>
                {
                    if (failed)
                    {
                        ParkCommitCallback(registration!);
                    }
                });
        }
        catch
        {
            registration?.Release();
            throw;
        }

        return Finish(completion, AsyncOperation.CommitAsync, cancellationToken);
    }

    internal Task<ConsumerRecords<TKey, TValue>> PollWithCallback<TKey, TValue>(
        TimeSpan timeout,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        CancellationToken cancellationToken = default)
    {
        long timeoutMs = TimerTimeoutMs(timeout);
        RefuseInsideACallback();
        ConsumerCompletion<IntPtr> completion = Submit(
            cancellationToken,
            s_records,
            (_, opaque) => NativeMethods.ConsumerPollCb(_handle, timeoutMs, ConsumerCompletion.Pointer, opaque),
            atPoll: true);
        return CompletePoll(completion, keyDeserializer, valueDeserializer, cancellationToken);
    }

    internal Task<long> PositionWithCallback(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        CheckInAUse(() => RequirePositionPartition(partition));
        return SubmitPosition(partition, timeoutMs: null, cancellationToken);
    }

    internal Task<long> PositionWithCallback(
        TopicPartition partition,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        SubmitPosition(partition, PositionTimeoutMs(partition, timeout), cancellationToken);

    internal Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> CommittedWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default)
    {
        (string Topic, int Partition)[] snapshot = CheckInAUse(() => CommittedPartitions(partitions));
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.Committed(snapshot, timeoutMs: null))
            : SubmitCommitted(snapshot, timeoutMs: null, cancellationToken);
    }

    internal Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> CommittedWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ((string Topic, int Partition)[] snapshot, long timeoutMs) = CommittedArguments(partitions, timeout);
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.Committed(snapshot, timeoutMs))
            : SubmitCommitted(snapshot, timeoutMs, cancellationToken);
    }

    internal Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimesWithCallback(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch,
        CancellationToken cancellationToken = default)
    {
        TimestampsSnapshot snapshot = CheckInAUse(() => SnapshotTimestamps(timestampsToSearch));
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.OffsetsForTimes(snapshot, timeoutMs: null))
            : SubmitOffsetsForTimes(snapshot, timeoutMs: null, cancellationToken);
    }

    internal Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimesWithCallback(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        (TimestampsSnapshot snapshot, long timeoutMs) =
            CheckInAUse(() => (SnapshotTimestamps(timestampsToSearch), TimeoutMs(timeout)));
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.OffsetsForTimes(snapshot, timeoutMs))
            : SubmitOffsetsForTimes(snapshot, timeoutMs, cancellationToken);
    }

    internal Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsetsWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        LookUpOffsetsWithCallback(partitions, timeout: null, beginning: true, cancellationToken);

    internal Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsetsWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        LookUpOffsetsWithCallback(partitions, timeout, beginning: true, cancellationToken);

    internal Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsetsWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        LookUpOffsetsWithCallback(partitions, timeout: null, beginning: false, cancellationToken);

    internal Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsetsWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        LookUpOffsetsWithCallback(partitions, timeout, beginning: false, cancellationToken);

    internal Task CommitWithCallback(CancellationToken cancellationToken = default) =>
        RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.CommitSync(timeoutMs: null))
            : SubmitCommit(timeoutMs: null, cancellationToken);

    internal Task CommitWithCallback(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        long timeoutMs = CommitTimeoutMs(timeout);
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.CommitSync(timeoutMs))
            : SubmitCommit(timeoutMs, cancellationToken);
    }

    internal Task CommitWithCallback(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default)
    {
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsetsWhenOpen(offsets);
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.CommitSync(snapshot, timeoutMs: null))
            : SubmitCommitOffsets(snapshot, timeoutMs: null, cancellationToken);
    }

    internal Task CommitWithCallback(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsetsWhenOpen(offsets);
        long timeoutMs = CommitTimeoutMs(timeout);
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.CommitSync(snapshot, timeoutMs))
            : SubmitCommitOffsets(snapshot, timeoutMs, cancellationToken);
    }

    internal Task<IReadOnlyList<PartitionInfo>> PartitionsForWithCallback(
        string topic,
        CancellationToken cancellationToken = default)
    {
        CheckInAUse(() => RequirePartitionsForTopic(topic, timeoutMs: null));
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.PartitionsFor(topic, timeoutMs: null))
            : SubmitPartitionsFor(topic, timeoutMs: null, cancellationToken);
    }

    internal Task<IReadOnlyList<PartitionInfo>> PartitionsForWithCallback(
        string topic,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        long timeoutMs = PartitionsForTimeoutMs(topic, timeout);
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.PartitionsFor(topic, timeoutMs))
            : SubmitPartitionsFor(topic, timeoutMs, cancellationToken);
    }

    internal Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopicsWithCallback(
        CancellationToken cancellationToken = default) =>
        RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.ListTopics(timeoutMs: null))
            : SubmitListTopics(timeoutMs: null, cancellationToken);

    internal Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopicsWithCallback(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        long timeoutMs = CheckInAUse(() => TimeoutMs(timeout));
        return RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.ListTopics(timeoutMs))
            : SubmitListTopics(timeoutMs, cancellationToken);
    }

    internal ValueTask CloseWithCallback(CancellationToken cancellationToken = default) =>
        StartClose(option: null, cancellationToken);

    internal ValueTask CloseWithCallback(CloseOptions option, CancellationToken cancellationToken = default)
    {
        RequireCloseOptions(option);
        return StartClose(option, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        if (_pump is null || _reporter.InCallback)
        {
            Dispose();
            return default;
        }

        return new ValueTask(DisposeOnThePumpAsync());
    }

    private static NativeConsumer Start(SafeConsumerHandle handle, bool groupIdDefined, bool withPump)
    {
        NativeConsumer consumer = new NativeConsumer(handle, groupIdDefined, withPump);
        try
        {
            if (consumer._pump is not null)
            {
                IntPtr outErr = IntPtr.Zero;
                NativeMethods.ConsumerSetCallbacksNotify(
                    handle, CompletionThread.NotifyHook, consumer._pump.NotifyOpaque, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
        }
        catch
        {
            Interlocked.Exchange(ref consumer._closed, 1);
            consumer.TearDown();
            throw;
        }

        return consumer;
    }

    // The drain holds the handle weakly so that a consumer never closed can still reach its finalizer and be leaked
    // with a warning. It is a use of the handle that a close does not refuse, since the close's own completion needs
    // it; only the destroy does.
    private static Func<int> DrainOf(SafeConsumerHandle handle, HandleUses uses)
    {
        WeakReference<SafeConsumerHandle> weak = new WeakReference<SafeConsumerHandle>(handle);
        return () =>
        {
            if (!weak.TryGetTarget(out SafeConsumerHandle? target) || !uses.TryEnterUnlessDestroyed())
            {
                return 0;
            }

            try
            {
                IntPtr outErr = IntPtr.Zero;
                int ran = NativeMethods.ConsumerExecuteCallbacks(target, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                return ran;
            }
            finally
            {
                if (uses.Exit())
                {
                    uses.Wake();
                }
            }
        };
    }

    private static void OnClosed(IntPtr error, IntPtr opaque)
    {
        try
        {
            GCHandle gcHandle = GCHandle.FromIntPtr(opaque);
            if (gcHandle.Target is not Action<IntPtr> complete)
            {
                return;
            }

            gcHandle.Free();
            complete(error);
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private static void FreeError(IntPtr error) => FreeError(ref error);

    private Task RunPartitions(
        (string Topic, int Partition)[] snapshot,
        NativePartitionsCb call,
        AsyncOperation operation,
        CancellationToken cancellationToken,
        Action<NativeConsumerHandle, (string Topic, int Partition)[]>? routed = null)
    {
        if (RoutesToTheHandle)
        {
            if (routed is null)
            {
                RefuseInsideACallback();
            }
            else
            {
                return RouteAsync(cancellationToken, handle => routed(handle, snapshot));
            }
        }

        return Finish(
            Submit(cancellationToken, s_void, (arguments, opaque) =>
                call(_handle, arguments.TopicPartitionList(snapshot), ConsumerCompletion.Void, opaque)),
            operation,
            cancellationToken);
    }

    private Task<IReadOnlyDictionary<TopicPartition, long>> LookUpOffsetsWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan? timeout,
        bool beginning,
        CancellationToken cancellationToken)
    {
        ((string Topic, int Partition)[] snapshot, long? timeoutMs) =
            CheckInAUse(() => (SnapshotPartitions(partitions), OptionalTimeoutMs(timeout)));
        if (RoutesToTheHandle)
        {
            return RouteAsync(cancellationToken, handle => beginning
                ? handle.BeginningOffsets(snapshot, timeoutMs)
                : handle.EndOffsets(snapshot, timeoutMs));
        }

        return ListOffsets(snapshot, timeoutMs, beginning, cancellationToken);
    }

    private Task<long> SubmitPosition(TopicPartition partition, long? timeoutMs, CancellationToken cancellationToken) =>
        RoutesToTheHandle
            ? RouteAsync(cancellationToken, handle => handle.Position(partition.Topic, partition.Partition, timeoutMs))
            : Finish(
            Submit(cancellationToken, s_scalar, (arguments, opaque) =>
            {
                IntPtr topicPartition = arguments.TopicPartition(partition.Topic, partition.Partition);
                if (timeoutMs is long ms)
                {
                    NativeMethods.ConsumerPositionWithTimeoutCb(
                        _handle, topicPartition, ms, ConsumerCompletion.Scalar, opaque);
                }
                else
                {
                    NativeMethods.ConsumerPositionCb(_handle, topicPartition, ConsumerCompletion.Scalar, opaque);
                }
            }),
            AsyncOperation.Position,
            cancellationToken);

    private Task SubmitCommit(long? timeoutMs, CancellationToken cancellationToken) =>
        Finish(
            Submit(cancellationToken, s_void, (_, opaque) =>
            {
                if (timeoutMs is long ms)
                {
                    NativeMethods.ConsumerCommitSyncWithTimeoutCb(_handle, ms, ConsumerCompletion.Void, opaque);
                }
                else
                {
                    NativeMethods.ConsumerCommitSyncCb(_handle, ConsumerCompletion.Void, opaque);
                }
            }),
            AsyncOperation.Commit,
            cancellationToken);

    private Task SubmitCommitOffsets(
        CommitOffsetsSnapshot snapshot,
        long? timeoutMs,
        CancellationToken cancellationToken) =>
        Finish(
            Submit(cancellationToken, s_void, (arguments, opaque) =>
            {
                IntPtr offsets = arguments.OffsetMap(snapshot);
                if (timeoutMs is long ms)
                {
                    NativeMethods.ConsumerCommitSyncWithOffsetsTimeoutCb(
                        _handle, offsets, ms, ConsumerCompletion.Void, opaque);
                }
                else
                {
                    NativeMethods.ConsumerCommitSyncWithOffsetsCb(_handle, offsets, ConsumerCompletion.Void, opaque);
                }
            }),
            AsyncOperation.Commit,
            cancellationToken);

    private Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> SubmitCommitted(
        (string Topic, int Partition)[] snapshot,
        long? timeoutMs,
        CancellationToken cancellationToken) =>
        Finish(
            Submit(
                cancellationToken,
                (map, _) => CopyCommitted(map, snapshot),
                (arguments, opaque) =>
                {
                    IntPtr list = arguments.TopicPartitionList(snapshot);
                    if (timeoutMs is long ms)
                    {
                        NativeMethods.ConsumerCommittedWithTimeoutCb(
                            _handle, list, ms, ConsumerCompletion.Pointer, opaque);
                    }
                    else
                    {
                        NativeMethods.ConsumerCommittedCb(_handle, list, ConsumerCompletion.Pointer, opaque);
                    }
                }),
            AsyncOperation.Committed,
            cancellationToken);

    private Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> SubmitOffsetsForTimes(
        TimestampsSnapshot snapshot,
        long? timeoutMs,
        CancellationToken cancellationToken) =>
        Finish(
            Submit(
                cancellationToken,
                (map, _) => CopyOffsetsForTimes(map, snapshot),
                (arguments, opaque) =>
                {
                    IntPtr timestamps = arguments.LongMap(
                        snapshot.Topics, snapshot.Partitions, snapshot.Timestamps, snapshot.Count);
                    if (timeoutMs is long ms)
                    {
                        NativeMethods.ConsumerOffsetsForTimesWithTimeoutCb(
                            _handle, timestamps, ms, ConsumerCompletion.Pointer, opaque);
                    }
                    else
                    {
                        NativeMethods.ConsumerOffsetsForTimesCb(_handle, timestamps, ConsumerCompletion.Pointer, opaque);
                    }
                }),
            AsyncOperation.OffsetsForTimes,
            cancellationToken);

    private Task<IReadOnlyDictionary<TopicPartition, long>> ListOffsets(
        (string Topic, int Partition)[] snapshot,
        long? timeoutMs,
        bool beginning,
        CancellationToken cancellationToken) =>
        Finish(
            Submit(
                cancellationToken,
                (map, _) => LongOffsetMapMarshal.CopyOutAndDestroy(map),
                (arguments, opaque) =>
                {
                    IntPtr list = arguments.TopicPartitionList(snapshot);
                    switch ((beginning, timeoutMs))
                    {
                        case (true, long ms):
                            NativeMethods.ConsumerBeginningOffsetsWithTimeoutCb(
                                _handle, list, ms, ConsumerCompletion.Pointer, opaque);
                            break;
                        case (true, null):
                            NativeMethods.ConsumerBeginningOffsetsCb(_handle, list, ConsumerCompletion.Pointer, opaque);
                            break;
                        case (false, long ms):
                            NativeMethods.ConsumerEndOffsetsWithTimeoutCb(
                                _handle, list, ms, ConsumerCompletion.Pointer, opaque);
                            break;
                        default:
                            NativeMethods.ConsumerEndOffsetsCb(_handle, list, ConsumerCompletion.Pointer, opaque);
                            break;
                    }
                }),
            beginning ? AsyncOperation.BeginningOffsets : AsyncOperation.EndOffsets,
            cancellationToken);

    private Task<IReadOnlyList<PartitionInfo>> SubmitPartitionsFor(
        string topic,
        long? timeoutMs,
        CancellationToken cancellationToken) =>
        Finish(
            Submit(
                cancellationToken,
                (list, _) => PartitionInfoListMarshal.CopyOutAndDestroy(list),
                (arguments, opaque) =>
                {
                    IntPtr utf8 = arguments.Utf8(topic);
                    if (timeoutMs is long ms)
                    {
                        NativeMethods.ConsumerPartitionsForWithTimeoutCb(
                            _handle, utf8, ms, ConsumerCompletion.Pointer, opaque);
                    }
                    else
                    {
                        NativeMethods.ConsumerPartitionsForCb(_handle, utf8, ConsumerCompletion.Pointer, opaque);
                    }
                }),
            AsyncOperation.PartitionsFor,
            cancellationToken);

    private Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> SubmitListTopics(
        long? timeoutMs,
        CancellationToken cancellationToken) =>
        Finish(
            Submit(
                cancellationToken,
                (map, _) => TopicPartitionInfoMapMarshal.CopyOutAndDestroy(map),
                (_, opaque) =>
                {
                    if (timeoutMs is long ms)
                    {
                        NativeMethods.ConsumerListTopicsWithTimeoutCb(_handle, ms, ConsumerCompletion.Pointer, opaque);
                    }
                    else
                    {
                        NativeMethods.ConsumerListTopicsCb(_handle, ConsumerCompletion.Pointer, opaque);
                    }
                }),
            AsyncOperation.ListTopics,
            cancellationToken);

    private ValueTask StartClose(CloseOptions? option, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return new ValueTask(Task.FromCanceled(cancellationToken));
        }

        if (_pump is null || _reporter.InCallback)
        {
            try
            {
                if (option is null)
                {
                    CloseSync();
                }
                else
                {
                    CloseSync(option);
                }

                return default;
            }
            catch (Exception e)
            {
                return new ValueTask(Task.FromException(e));
            }
        }

        int thread = Environment.CurrentManagedThreadId;
        try
        {
            ThrowIfHeldByAnotherThread(thread);
        }
        catch (Exception e)
        {
            return new ValueTask(Task.FromException(e));
        }

        return new ValueTask(CloseAsync(option, thread, cancellationToken));
    }

    // A routed call blocks; its Task comes back completed, or faulted with the handle's error. The token is checked
    // before the call only, as nothing interrupts a handle op.
    private Task RouteAsync(CancellationToken cancellationToken, Action<NativeConsumerHandle> call) =>
        RouteAsync(cancellationToken, handle =>
        {
            call(handle);
            return true;
        });

    private Task<T> RouteAsync<T>(CancellationToken cancellationToken, Func<NativeConsumerHandle, T> call)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();
        int owner = EnterUse();
        try
        {
            T result;
            try
            {
                result = call(CallbackHandle());
            }
            catch (Exception exception)
            {
                return Task.FromException<T>(exception);
            }

            return Task.FromResult(result);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    private ConsumerCompletion<T> Submit<T>(
        CancellationToken cancellationToken,
        Func<IntPtr, long, T> convert,
        Action<NativeCallArguments, IntPtr> submit,
        Action<bool>? settle = null,
        UseKind kind = UseKind.Owned,
        bool refusesOtherThreads = false,
        bool atPoll = false)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();
        ThrowKeptForTheNextCall(atPoll);
        int owner = EnterUse(kind);
        if (refusesOtherThreads)
        {
            RefuseOtherThreads(true);
        }

        try
        {
            ConsumerCompletion<T> completion = Start(convert, submit, settle);
            completion.UseOwner = owner;
            completion.RefusesOtherThreads = refusesOtherThreads;
            return completion;
        }
        catch
        {
            EndUse(owner, refusesOtherThreads);
            throw;
        }
    }

    private static ConsumerCompletion<T> Start<T>(
        Func<IntPtr, long, T> convert,
        Action<NativeCallArguments, IntPtr> submit,
        Action<bool>? settle)
    {
        ConsumerCompletion<T> completion = new ConsumerCompletion<T>(convert, settle)
        {
            Again = () => Start(convert, submit, settle),
        };
        IntPtr opaque = completion.Root();
        try
        {
            using NativeCallArguments arguments = new NativeCallArguments();
            submit(arguments, opaque);
        }
        catch
        {
            ConsumerCompletion.Abandon(opaque);
            throw;
        }

        return completion;
    }

    private async Task<T> Finish<T>(
        ConsumerCompletion<T> completion,
        AsyncOperation operation,
        CancellationToken cancellationToken)
    {
        try
        {
            T result;
            try
            {
                result = await Outcome(completion, operation, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (_reporter.HasKeptCallbacks)
            {
                await ThrowKeptError(cancellationToken).ConfigureAwait(false);
                throw;
            }

            if (_reporter.HasKeptCallbacks)
            {
                await ThrowKeptError(cancellationToken).ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            return result;
        }
        finally
        {
            EndUse(completion.UseOwner, completion.RefusesOtherThreads);
        }
    }

    // The first exception of the async commit callbacks kept during the call wins over the call's own outcome and is
    // thrown as is; a cancelled call keeps it for the next call, as a Poll does.
    private async Task ThrowKeptError(CancellationToken cancellationToken)
    {
        await _reporter.WhenKeptCallbacksEnd().ConfigureAwait(false);
        Exception? error = _reporter.TakeKeptError();
        if (error is null)
        {
            return;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            _reporter.KeepForTheNextCall(error);
            return;
        }

        ExceptionDispatchInfo.Capture(error).Throw();
    }

    private async Task KeepKeptError()
    {
        await _reporter.WhenKeptCallbacksEnd().ConfigureAwait(false);
        _reporter.KeepForTheNextCall(_reporter.TakeKeptError());
    }

    private void ThrowKeptForTheNextCall(bool atPoll)
    {
        if (_reporter.InCallback)
        {
            return;
        }

        Exception? error = _reporter.TakeKeptForTheNextCall();
        if (error is null)
        {
            return;
        }

        if (atPoll)
        {
            throw KafkaException.WrapAtPoll(error);
        }

        ExceptionDispatchInfo.Capture(error).Throw();
    }

    private void EndUse(int owner, bool refusedOtherThreads)
    {
        if (refusedOtherThreads)
        {
            RefuseOtherThreads(false);
        }

        ExitUse(owner);
    }

    private async Task<T> Outcome<T>(
        ConsumerCompletion<T> completion,
        AsyncOperation operation,
        CancellationToken cancellationToken)
    {
        bool wakeable = IsWakeable(operation);
        while (true)
        {
            using (WakeOnCancel(completion, wakeable, cancellationToken))
            {
                try
                {
                    T result = await completion.Result.ConfigureAwait(false);
                    if (wakeable)
                    {
                        _ = OwnWakeOutcome(completion.OwnWakeMark, woken: false, ending: false);
                    }

                    return result;
                }
                catch (Exception exception)
                {
                    bool ending = cancellationToken.IsCancellationRequested;
                    bool woken = exception is KafkaException { Code: WakeupCode };
                    if (!wakeable || !OwnWakeOutcome(completion.OwnWakeMark, woken, ending))
                    {
                        if (ending)
                        {
                            throw new OperationCanceledException(cancellationToken);
                        }

                        throw;
                    }
                }
            }

            completion = completion.Again!();
        }
    }

    // The wakeup is sent from the drain thread after a drain, so an operation whose completion was already queued
    // when the token fired is seen as finished and leaves no wakeup armed.
    private CancellationTokenRegistration WakeOnCancel(
        ConsumerCompletion completion,
        bool wakeable,
        CancellationToken cancellationToken)
    {
        if (!wakeable || !cancellationToken.CanBeCanceled)
        {
            return default;
        }

        return cancellationToken.Register(() => _pump!.PostAfterDrain(() =>
        {
            if (!completion.HasArrived)
            {
                completion.OwnWakeMark = OwnWake();
            }
        }));
    }

    // The records are deserialized after the poll's use has ended, so a deserializer may dispose the consumer, and
    // the seeks back take one use of their own. Once the token has fired the poll ends canceled whatever it returned,
    // so its records are dropped and every partition is sought back to its first record, as a failed deserialization
    // seeks back to the first record not returned.
    private async Task<ConsumerRecords<TKey, TValue>> CompletePoll<TKey, TValue>(
        ConsumerCompletion<IntPtr> completion,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        CancellationToken cancellationToken)
    {
        IntPtr records;
        try
        {
            try
            {
                records = await Outcome(completion, AsyncOperation.Poll, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (_reporter.HasKeptCallbacks)
            {
                await KeepKeptError().ConfigureAwait(false);
                throw;
            }

            if (_reporter.HasKeptCallbacks)
            {
                await KeepKeptError().ConfigureAwait(false);
            }

            HoldRecords();
        }
        finally
        {
            ExitUse(completion.UseOwner);
        }

        ConsumerRecords<TKey, TValue>? copied = null;
        RecordDeserializationException? error = null;
        IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind = Array.Empty<KeyValuePair<TopicPartition, long>>();
        IReadOnlyList<KeyValuePair<TopicPartition, long>>? dropped = null;
        try
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                copied = ConsumerRecordsMarshal.CopyOut(
                    records, keyDeserializer, valueDeserializer, out error, out rewind);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                dropped = ConsumerRecordsMarshal.FirstPositions(records);
            }
        }
        finally
        {
            DestroyRecords(records);
        }

        if (dropped is not null)
        {
            await SeekBackAsync(dropped, completion.UseOwner).ConfigureAwait(false);
            throw new OperationCanceledException(cancellationToken);
        }

        if (error is not null)
        {
            await SeekBackAsync(rewind, completion.UseOwner).ConfigureAwait(false);
            if (copied!.Count == 0)
            {
                throw error;
            }
        }

        return copied!;
    }

    // The seeks back are one use of the poll's thread, after the poll's use, whichever thread they run on.
    private async Task SeekBackAsync(IReadOnlyList<KeyValuePair<TopicPartition, long>> positions, int pollOwner)
    {
        if (positions.Count == 0)
        {
            return;
        }

        int owner;
        try
        {
            owner = EnterUse(pollOwner == 0 ? UseKind.Unowned : UseKind.Owned, pollOwner);
        }
        catch (Exception exception)
        {
            TraceSeekBackFailures(positions, exception);
            return;
        }

        try
        {
            foreach (KeyValuePair<TopicPartition, long> position in positions)
            {
                try
                {
                    ConsumerCompletion<bool> completion = Start(
                        s_void,
                        (arguments, opaque) => NativeMethods.ConsumerSeekWithOffsetCb(
                            _handle,
                            arguments.TopicPartition(position.Key.Topic, position.Key.Partition),
                            position.Value,
                            ConsumerCompletion.Void,
                            opaque),
                        settle: null);
                    await completion.Result.ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    TraceSeekBackFailure(position.Key, position.Value, exception);
                }
            }
        }
        finally
        {
            ExitUse(owner);
        }
    }

    private async Task CloseAsync(CloseOptions? option, int thread, CancellationToken cancellationToken)
    {
        try
        {
            await CloseOnceAsync(option, thread).ConfigureAwait(false);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private async Task CloseOnceAsync(CloseOptions? option, int thread)
    {
        IntPtr options = option is null ? IntPtr.Zero : NewCloseOptions(option);
        try
        {
            await _closeGate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!TryBeginExplicitClose(thread))
                {
                    return;
                }

                Exception? kept = _reporter.TakeKeptForTheNextCall();
                try
                {
                    IntPtr error = await CloseOnThePumpAsync(options).ConfigureAwait(false);
                    try
                    {
                        ReopenIfRefused(ref error);
                    }
                    catch
                    {
                        _reporter.KeepForTheNextCall(kept);
                        throw;
                    }

                    await _uses.WhenNone().ConfigureAwait(false);
                    await _reporter.WhenKeptCallbacksEnd().ConfigureAwait(false);
                    Exception? keptDuringTheClose = _reporter.TakeKeptError();
                    TearDownThenThrow(ref error, kept ?? keptDuringTheClose);
                }
                finally
                {
                    EndExplicitClose();
                }
            }
            finally
            {
                _closeGate.Release();
            }
        }
        finally
        {
            DestroyCloseOptions(options);
        }
    }

    private async Task DisposeOnThePumpAsync()
    {
        await _closeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!TryBeginClose())
            {
                return;
            }

            try
            {
                WakeForTheDispose(waking: true);
                try
                {
                    await _uses.WhenNone().ConfigureAwait(false);
                }
                finally
                {
                    WakeForTheDispose(waking: false);
                }

                FreeError(await CloseOnThePumpAsync(IntPtr.Zero).ConfigureAwait(false));
            }
            catch (Exception)
            {
                // Dispose does not throw; Close reports the same error.
            }

            await _reporter.WhenKeptCallbacksEnd().ConfigureAwait(false);
            try
            {
                TearDown();
            }
            catch (Exception)
            {
                // Dispose does not throw.
            }
        }
        finally
        {
            _closeGate.Release();
        }
    }

    private IntPtr CloseOnThePump()
    {
        Completion<IntPtr> closed = new Completion<IntPtr>(FreeError);
        SubmitClose(IntPtr.Zero, closed.Complete);
        _ = closed.Wait(Timeout.InfiniteTimeSpan, out IntPtr error);
        return error;
    }

    private async Task<IntPtr> CloseOnThePumpAsync(IntPtr options)
    {
        AsyncCompletion<IntPtr> closed = new AsyncCompletion<IntPtr>(FreeError);
        SubmitClose(options, closed.Complete);
        _ = await closed.Wait(Timeout.InfiniteTimeSpan, CancellationToken.None).ConfigureAwait(false);
        return closed.Payload;
    }

    private void SubmitClose(IntPtr options, Action<IntPtr> complete)
    {
        IntPtr opaque = GCHandle.ToIntPtr(GCHandle.Alloc(complete));
        try
        {
            if (options == IntPtr.Zero)
            {
                NativeMethods.ConsumerCloseCb(_handle, s_closed, opaque);
            }
            else
            {
                NativeMethods.ConsumerCloseWithOptionsCb(_handle, options, s_closed, opaque);
            }
        }
        catch
        {
            GCHandle.FromIntPtr(opaque).Free();
            throw;
        }
    }
}
