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
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The native consumer behind every consumer class: it owns the consumer's handle and, on the async consumers, its
/// drain thread; checks that one thread uses it at a time; routes a call made inside a callback to the consumer's
/// handle; and tears down in order: refuse new calls, close, join the drain thread, wait for the calls in flight,
/// then destroy on the closing thread.
/// </summary>
internal sealed partial class NativeConsumer : IDisposable, IAsyncDisposable
{
    private const string GroupIdConfig = "group.id";

    private const string DefaultOffsetResetStrategy = "latest";

    private const int LocalConcurrentModification = -2;

    private const int WakeupCode = -18;

    private const int InvalidGroupIdCode = 24;

    private const string CloseNegativeTimeoutMessage = "The timeout cannot be negative.";

    private const string ConcurrentAccessMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const int Open = 0;

    private const int Closing = 1;

    private const int ClosePending = 2;

    private const string GroupIdNotDefinedMessage =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer " +
        "configuration.";

    private readonly SafeConsumerHandle _handle;
    private readonly HandleUses _uses = new HandleUses();
    private readonly SemaphoreSlim _closeGate = new SemaphoreSlim(1, 1);
    private readonly bool _groupIdDefined;
    private readonly List<IntPtr> _recordBytes = new List<IntPtr>();
    private int _recordsHeld;
    private bool _recordBytesReleasable;

    // Closing: a teardown that will close (a Dispose, or a close the core accepted). ClosePending: an explicit Close
    // begun while a call it does not wait for is in flight (its own thread's, or one made inside a callback), which
    // the core may still refuse; a close from inside a callback is a no-op only while Closing.
    private int _closed;

    private int _calls;

    // The owner check, as Java's acquire() and Python's use table: the thread whose calls hold the consumer and
    // how many, and the thread of an explicit Close in progress.
    private readonly object _owner = new object();

    private int _ownerThread;

    private int _ownerUses;

    private int _closingThread;

    // An AsyncMockConsumer's Rebalance or LosePartitions refuses other threads for its whole use, as Python's mock does.
    private int _refusingUses;

    private readonly ConsumerCallbackReporter _reporter;

    // The registered listener's self_: replaced when a subscribe succeeds, kept across Unsubscribe and
    // Assign, released after the consumer's destroy has returned.
    private ListenerRegistration? _listenerRegistration;

    // The commit-callback registrations whose commit reported an error: the core may still fire them, so they are
    // released after the consumer's destroy has returned.
    private readonly List<CommitCallbackRegistration> _parkedCommitCallbacks = new List<CommitCallbackRegistration>();

    private bool _parkedCommitCallbacksReleased;

    private CommitCallbackRegistration? _commitCallbackRegistration;

    private readonly object _wakeups = new object();

    private long _userWakeups;

    // A wakeup a cancel sent that its call ended without consuming: the next wakeable call the core ends with that
    // wakeup is submitted again, as Python does. The user's Wakeup() clears it.
    private bool _ownWakeLeft;

    // While a Dispose waits for the calls in flight, a call made inside a callback that the core ends with the
    // wakeup sends it again, so the op that delivered the callback still ends.
    private bool _disposeWaking;

    // The handle a call made inside a callback is routed to: made on first use, destroyed after the consumer.
    private readonly object _callbackHandleGate = new object();

    private NativeConsumerHandle? _callbackHandle;

    private bool _closeEnded;

    private NativeConsumer(SafeConsumerHandle handle, bool groupIdDefined, bool withPump)
    {
        _handle = handle;
        _groupIdDefined = groupIdDefined;
        _reporter = new ConsumerCallbackReporter(handle.DangerousGetHandle(), PulseOwnerWaiters);
        if (withPump)
        {
            _pump = new CompletionThread(PumpThreadName, DrainOf(handle, _uses));
        }

        handle.ReleaseAfterDestroy(AfterDestroy);
    }

    private delegate void NativePartitionsCall(SafeConsumerHandle consumer, IntPtr partitions, ref IntPtr outErr);

    internal SafeConsumerHandle Handle
    {
        get
        {
            ThrowIfClosed();
            return _handle;
        }
    }

    internal ListenerRegistration? CurrentListenerRegistration => _listenerRegistration;

    internal CommitCallbackRegistration? CurrentCommitCallbackRegistration => Volatile.Read(ref _commitCallbackRegistration);

    internal static NativeConsumer Create(IReadOnlyDictionary<string, string> config, bool withPump = true)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        foreach (KeyValuePair<string, string> entry in config)
        {
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"Configuration value for key '{entry.Key}' must not be null.",
                    nameof(config));
            }
        }

        IntPtr classPointer = NewKafkaConsumer(config);
        IntPtr outErr = IntPtr.Zero;
        IntPtr view = NativeMethods.KafkaConsumerAsConsumer(classPointer, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.KafkaConsumerDestroy(classPointer, ref destroyErr);
            FreeError(ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return Start(new SafeKafkaConsumerHandle(view, classPointer), config.ContainsKey(GroupIdConfig), withPump);
    }

    internal static NativeConsumer CreateMock(string? autoOffsetReset = null, bool withPump = true)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr classPointer;
        using (Utf8Marshal.PinnedUtf8String strategy = Utf8Marshal.Pin(autoOffsetReset ?? DefaultOffsetResetStrategy))
        {
            classPointer = NativeMethods.MockConsumerNew(strategy.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr view = NativeMethods.MockConsumerAsConsumer(classPointer, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MockConsumerDestroy(classPointer, ref destroyErr);
            FreeError(ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return Start(new SafeMockConsumerHandle(view, classPointer), groupIdDefined: false, withPump);
    }

    private static IntPtr NewKafkaConsumer(IReadOnlyDictionary<string, string> config)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(config.Count * 2);
        IntPtr map = IntPtr.Zero;
        IntPtr consumerConfig = IntPtr.Zero;
        IntPtr outErr = IntPtr.Zero;
        try
        {
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> entry in config)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                pins.Add(value);
                NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            consumerConfig = NativeMethods.ConsumerConfigNew(map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr consumer = NativeMethods.KafkaConsumerNew(consumerConfig, IntPtr.Zero, IntPtr.Zero, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return consumer;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            if (consumerConfig != IntPtr.Zero)
            {
                NativeMethods.ConsumerConfigDestroy(consumerConfig, ref destroyErr);
                FreeError(ref destroyErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in pins)
            {
                pin.Dispose();
            }

            if (map != IntPtr.Zero)
            {
                NativeMethods.MapDestroy(map, ref destroyErr);
                FreeError(ref destroyErr);
            }
        }
    }

    internal void Seek(string topic, int partition, long offset)
    {
        RequireTopicPartition(topic, partition);
        RequireSeekOffset(offset);
        if (RoutesToTheHandle)
        {
            Route(handle => handle.Seek(topic, partition, offset));
            return;
        }

        CallSync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerSeekWithOffset(_handle, arguments.TopicPartition(topic, partition), offset, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    private static void RequireSeekOffset(long offset)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset), offset, "seek offset must not be a negative number");
        }
    }

    internal void SeekWithMetadata(string topic, int partition, OffsetAndMetadata offsetAndMetadata)
    {
        RequireTopicPartition(topic, partition);
        if (offsetAndMetadata is null)
        {
            throw new ArgumentNullException(nameof(offsetAndMetadata));
        }

        if (RoutesToTheHandle)
        {
            Route(handle => handle.SeekWithMetadata(topic, partition, offsetAndMetadata));
            return;
        }

        CallSync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerSeekWithOffsetAndMetadata(
                _handle,
                arguments.TopicPartition(topic, partition),
                arguments.OffsetAndMetadata(
                    offsetAndMetadata.Offset, offsetAndMetadata.LeaderEpoch ?? -1, offsetAndMetadata.Metadata),
                ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal long? CurrentLag(string topic, int partition)
    {
        RequireTopicPartition(topic, partition);
        if (RoutesToTheHandle)
        {
            return Route(handle => handle.CurrentLag(topic, partition));
        }

        return QuerySync<long?>(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            long lag = NativeMethods.ConsumerCurrentLag(_handle, arguments.TopicPartition(topic, partition), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return lag < 0 ? (long?)null : lag;
        },
        UseKind.UnownedOnMock);
    }

    internal void CommitAsync()
    {
        if (RoutesToTheHandle)
        {
            Route(handle => handle.CommitAsync());
            return;
        }

        CallSync(_ =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerCommitAsync(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void CommitAsync(IOffsetCommitCallback callback)
    {
        if (callback is null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        if (RoutesToTheHandle)
        {
            Route(handle => handle.CommitAsync(callback));
            return;
        }

        CallSync(_ => CommitWithRegistration(callback, emptyOffsets: false, callbackHandle =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerCommitAsyncWithCallback(_handle, callbackHandle, ref outErr);
            return outErr;
        }));
    }

    internal void CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback)
    {
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsets(offsets);
        if (RoutesToTheHandle)
        {
            Route(handle => handle.CommitAsync(snapshot, callback));
            return;
        }

        CallSync(arguments =>
        {
            IntPtr map = arguments.OffsetMap(snapshot);
            CommitWithRegistration(callback, snapshot.Count == 0, callbackHandle =>
            {
                IntPtr outErr = IntPtr.Zero;
                NativeMethods.ConsumerCommitAsyncWithOffsetsCallback(_handle, map, callbackHandle, ref outErr);
                return outErr;
            });
        });
    }

    internal void CommitWithRegistration(
        IOffsetCommitCallback? callback,
        bool emptyOffsets,
        Func<IntPtr, IntPtr> commit) =>
        CommitWithRegistration(RootCommitCallback(callback, emptyOffsets), commit);

    internal void CommitWithRegistration(
        IAsyncOffsetCommitCallback callback,
        bool emptyOffsets,
        Func<IntPtr, IntPtr> commit) =>
        CommitWithRegistration(RootCommitCallback(callback, emptyOffsets), commit);

    private void CommitWithRegistration(CommitCallbackRegistration registration, Func<IntPtr, IntPtr> commit)
    {
        IntPtr outErr;
        try
        {
            IntPtr callbackHandle = registration.NewHandle();
            try
            {
                outErr = commit(callbackHandle);
            }
            finally
            {
                CommitCallbackRegistration.DestroyHandle(callbackHandle);
            }
        }
        catch
        {
            registration.Release();
            throw;
        }

        if (outErr != IntPtr.Zero)
        {
            ParkCommitCallback(registration);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    internal ConsumerCallbackReporter.HandleCommitScope EnterHandleCommit() => _reporter.EnterHandleCommit();

    private CommitCallbackRegistration RootCommitCallback(IOffsetCommitCallback? callback, bool emptyOffsets) =>
        Track(CommitCallbackRegistration.Root(callback, NullOffsetsFor(emptyOffsets), _reporter));

    private CommitCallbackRegistration RootCommitCallback(IAsyncOffsetCommitCallback callback, bool emptyOffsets) =>
        Track(CommitCallbackRegistration.Root(callback, NullOffsetsFor(emptyOffsets), _reporter));

    // Java's KafkaConsumer hands an explicit empty commit's callback null offsets; its MockConsumer hands the map.
    private bool NullOffsetsFor(bool emptyOffsets) => emptyOffsets && !IsMock;

    private CommitCallbackRegistration Track(CommitCallbackRegistration registration)
    {
        Volatile.Write(ref _commitCallbackRegistration, registration);
        return registration;
    }

    private void ParkCommitCallback(CommitCallbackRegistration registration)
    {
        lock (_parkedCommitCallbacks)
        {
            if (!_parkedCommitCallbacksReleased)
            {
                _parkedCommitCallbacks.Add(registration);
                return;
            }
        }

        registration.Release();
    }

    // The records are deserialized after the poll's use has ended, so a deserializer may dispose the consumer. A
    // record that fails to deserialize ends the batch: the records before it are returned, and every partition is
    // sought back to its first record not returned, the seeks taking one use of their own, so the failing record comes
    // next.
    internal ConsumerRecords<TKey, TValue> PollTyped<TKey, TValue>(
        TimeSpan timeout,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        long timeoutMs = TimerTimeoutMs(timeout);
        RefuseInsideACallback();
        IntPtr records = QuerySync(_ =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr polled = NativeMethods.ConsumerPoll(_handle, timeoutMs, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            HoldRecords();
            return polled;
        });

        ConsumerRecords<TKey, TValue> copied;
        RecordDeserializationException? error;
        IReadOnlyList<KeyValuePair<TopicPartition, long>> rewind;
        try
        {
            copied = ConsumerRecordsMarshal.CopyOut(records, keyDeserializer, valueDeserializer, out error, out rewind);
        }
        finally
        {
            DestroyRecords(records);
        }

        if (error is null)
        {
            return copied;
        }

        SeekBack(rewind);
        if (copied.Count == 0)
        {
            throw error;
        }

        return copied;
    }

    private void SeekBack(IReadOnlyList<KeyValuePair<TopicPartition, long>> positions)
    {
        if (positions.Count == 0)
        {
            return;
        }

        int owner;
        try
        {
            owner = EnterUse();
        }
        catch (Exception exception)
        {
            TraceSeekBackFailures(positions, exception);
            return;
        }

        try
        {
            using NativeCallArguments arguments = new NativeCallArguments();
            foreach (KeyValuePair<TopicPartition, long> position in positions)
            {
                try
                {
                    IntPtr outErr = IntPtr.Zero;
                    NativeMethods.ConsumerSeekWithOffset(
                        _handle,
                        arguments.TopicPartition(position.Key.Topic, position.Key.Partition),
                        position.Value,
                        ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
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

    private static void TraceSeekBackFailures(
        IReadOnlyList<KeyValuePair<TopicPartition, long>> positions,
        Exception exception)
    {
        foreach (KeyValuePair<TopicPartition, long> position in positions)
        {
            TraceSeekBackFailure(position.Key, position.Value, exception);
        }
    }

    private static void TraceSeekBackFailure(TopicPartition partition, long offset, Exception exception) =>
        Trace.TraceError("Could not seek {0} back to offset {1}: {2}", partition, offset, exception);

    internal void Subscribe(IReadOnlyCollection<string> topics)
    {
        string[] topicArray = SnapshotTopics(topics);
        RefuseInsideACallback();
        CallSync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerSubscribeWithTopics(_handle, arguments.StringList(topicArray), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            SettleListener(null, subscribed: true);
        });
    }

    internal void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener listener)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        if (listener is null)
        {
            throw new ArgumentNullException(nameof(listener));
        }

        string[] topicArray = SnapshotTopics(topics);
        RefuseInsideACallback();
        CallSync(arguments =>
        {
            IntPtr list = arguments.StringList(topicArray);
            ListenerRegistration registration = ListenerRegistration.Root(listener, _reporter);
            bool subscribed = false;
            try
            {
                IntPtr listenerHandle = registration.NewHandle();
                try
                {
                    IntPtr outErr = IntPtr.Zero;
                    NativeMethods.ConsumerSubscribeWithTopicsListener(_handle, list, listenerHandle, ref outErr);
                    subscribed = outErr == IntPtr.Zero;
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                }
                finally
                {
                    ListenerRegistration.DestroyHandle(listenerHandle);
                }
            }
            finally
            {
                SettleListener(registration, subscribed);
            }
        });
    }

    internal void Rebalance(IReadOnlyCollection<TopicPartition> partitions)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        CallMock((mock, arguments) =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockConsumerRebalance(mock, arguments.TopicPartitionList(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void LosePartitions(IReadOnlyCollection<TopicPartition> partitions)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        CallMock((mock, arguments) =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockConsumerLosePartitions(mock, arguments.TopicPartitionList(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    private void SettleListener(ListenerRegistration? registration, bool subscribed)
    {
        if (subscribed)
        {
            Interlocked.Exchange(ref _listenerRegistration, registration)?.Release();
        }
        else
        {
            registration?.Release();
        }
    }

    private static string[] SnapshotTopics(IReadOnlyCollection<string> topics)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        string[] topicArray = new string[topics.Count];
        int index = 0;
        foreach (string topic in topics)
        {
            if (topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(topics));
            }

            topicArray[index++] = topic;
        }

        return topicArray;
    }

    internal void Unsubscribe()
    {
        RefuseInsideACallback();
        CallSync(_ =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerUnsubscribe(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void Assign(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionsSync(SnapshotPartitions(partitions), NativeMethods.ConsumerAssign);

    internal void Pause(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionsSync(
            SnapshotPartitions(partitions), NativeMethods.ConsumerPause, (handle, snapshot) => handle.Pause(snapshot));

    internal void Resume(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionsSync(
            SnapshotPartitions(partitions), NativeMethods.ConsumerResume, (handle, snapshot) => handle.Resume(snapshot));

    internal void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionsSync(
            SnapshotPartitions(partitions),
            NativeMethods.ConsumerSeekToBeginning,
            (handle, snapshot) => handle.SeekToBeginning(snapshot));

    internal void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionsSync(
            SnapshotPartitions(partitions), NativeMethods.ConsumerSeekToEnd, (handle, snapshot) => handle.SeekToEnd(snapshot));

    internal long Position(TopicPartition partition)
    {
        RequirePositionPartition(partition);
        return Position(partition, timeoutMs: null);
    }

    internal long Position(TopicPartition partition, TimeSpan timeout)
    {
        RequirePositionPartition(partition);
        ThrowIfClosed();
        return Position(partition, TimerTimeoutMs(timeout));
    }

    internal void CommitSync()
    {
        if (RoutesToTheHandle)
        {
            Route(handle => handle.CommitSync());
            return;
        }

        CommitSync(timeoutMs: null);
    }

    internal void CommitSync(TimeSpan timeout)
    {
        RefuseInsideACallback();
        ThrowIfClosed();
        CommitSync(TimerTimeoutMs(timeout));
    }

    internal void CommitSyncOffsets(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsets(offsets);
        if (RoutesToTheHandle)
        {
            Route(handle => handle.CommitSync(snapshot));
            return;
        }

        CommitSyncOffsets(snapshot, timeoutMs: null);
    }

    internal void CommitSyncOffsets(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, TimeSpan timeout)
    {
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsets(offsets);
        RefuseInsideACallback();
        ThrowIfClosed();
        CommitSyncOffsets(snapshot, TimerTimeoutMs(timeout));
    }

    internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        IReadOnlyCollection<TopicPartition> partitions)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        return RoutesToTheHandle
            ? Route(handle => handle.Committed(snapshot))
            : Committed(snapshot, timeoutMs: null);
    }

    internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        RefuseInsideACallback();
        ThrowIfClosed();
        return Committed(snapshot, TimeoutMs(timeout));
    }

    internal IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        return RoutesToTheHandle
            ? Route(handle => handle.BeginningOffsets(snapshot))
            : ListOffsets(snapshot, timeoutMs: null, beginning: true);
    }

    internal IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        RefuseInsideACallback();
        ThrowIfClosed();
        return ListOffsets(snapshot, TimeoutMs(timeout), beginning: true);
    }

    internal IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        return RoutesToTheHandle
            ? Route(handle => handle.EndOffsets(snapshot))
            : ListOffsets(snapshot, timeoutMs: null, beginning: false);
    }

    internal IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        RefuseInsideACallback();
        ThrowIfClosed();
        return ListOffsets(snapshot, TimeoutMs(timeout), beginning: false);
    }

    internal IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch)
    {
        TimestampsSnapshot snapshot = SnapshotTimestamps(timestampsToSearch);
        return RoutesToTheHandle
            ? Route(handle => handle.OffsetsForTimes(snapshot))
            : OffsetsForTimes(snapshot, timeoutMs: null);
    }

    internal IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, TimeSpan timeout)
    {
        TimestampsSnapshot snapshot = SnapshotTimestamps(timestampsToSearch);
        RefuseInsideACallback();
        ThrowIfClosed();
        return OffsetsForTimes(snapshot, TimeoutMs(timeout));
    }

    internal IReadOnlyList<PartitionInfo> PartitionsFor(string topic)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        RefuseInsideACallback();
        return PartitionsFor(topic, timeoutMs: null);
    }

    internal IReadOnlyList<PartitionInfo> PartitionsFor(string topic, TimeSpan timeout)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        RefuseInsideACallback();
        ThrowIfClosed();
        return PartitionsFor(topic, TimeoutMs(timeout));
    }

    internal IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics()
    {
        RefuseInsideACallback();
        return ListTopics(timeoutMs: null);
    }

    internal IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(TimeSpan timeout)
    {
        RefuseInsideACallback();
        ThrowIfClosed();
        return ListTopics(TimeoutMs(timeout));
    }

    private long Position(TopicPartition partition, long? timeoutMs) =>
        RoutesToTheHandle
            ? Route(handle => handle.Position(partition.Topic, partition.Partition, timeoutMs))
            : QuerySync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr topicPartition = arguments.TopicPartition(partition.Topic, partition.Partition);
            long position = timeoutMs is long ms
                ? NativeMethods.ConsumerPositionWithTimeout(_handle, topicPartition, ms, ref outErr)
                : NativeMethods.ConsumerPosition(_handle, topicPartition, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return position;
        });

    private void CommitSync(long? timeoutMs) =>
        CallSync(_ =>
        {
            IntPtr outErr = IntPtr.Zero;
            if (timeoutMs is long ms)
            {
                NativeMethods.ConsumerCommitSyncWithTimeout(_handle, ms, ref outErr);
            }
            else
            {
                NativeMethods.ConsumerCommitSync(_handle, ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    private void CommitSyncOffsets(CommitOffsetsSnapshot snapshot, long? timeoutMs) =>
        CallSync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr offsets = arguments.OffsetMap(snapshot);
            if (timeoutMs is long ms)
            {
                NativeMethods.ConsumerCommitSyncWithOffsetsTimeout(_handle, offsets, ms, ref outErr);
            }
            else
            {
                NativeMethods.ConsumerCommitSyncWithOffsets(_handle, offsets, ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    private IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        (string Topic, int Partition)[] snapshot, long? timeoutMs) =>
        QuerySync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr partitions = arguments.TopicPartitionList(snapshot);
            IntPtr map = timeoutMs is long ms
                ? NativeMethods.ConsumerCommittedWithTimeout(_handle, partitions, ms, ref outErr)
                : NativeMethods.ConsumerCommitted(_handle, partitions, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return CopyCommitted(map, snapshot);
        });

    // Every requested partition gets an entry, null where it has no committed offset, as Java's
    // KafkaConsumer.committed; the mock keeps Java's MockConsumer.committed, which leaves such a partition out.
    internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> CopyCommitted(
        IntPtr map, (string Topic, int Partition)[] requested) =>
        OffsetMapMarshal.CopyOutAndDestroy(map, IsMock ? null : requested);

    private IReadOnlyDictionary<TopicPartition, long> ListOffsets(
        (string Topic, int Partition)[] snapshot, long? timeoutMs, bool beginning) =>
        QuerySync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr partitions = arguments.TopicPartitionList(snapshot);
            IntPtr map;
            if (timeoutMs is long ms)
            {
                map = beginning
                    ? NativeMethods.ConsumerBeginningOffsetsWithTimeout(_handle, partitions, ms, ref outErr)
                    : NativeMethods.ConsumerEndOffsetsWithTimeout(_handle, partitions, ms, ref outErr);
            }
            else
            {
                map = beginning
                    ? NativeMethods.ConsumerBeginningOffsets(_handle, partitions, ref outErr)
                    : NativeMethods.ConsumerEndOffsets(_handle, partitions, ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return LongOffsetMapMarshal.CopyOutAndDestroy(map);
        });

    private IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        TimestampsSnapshot snapshot, long? timeoutMs) =>
        QuerySync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr timestamps = arguments.LongMap(snapshot.Topics, snapshot.Partitions, snapshot.Timestamps, snapshot.Count);
            IntPtr map = timeoutMs is long ms
                ? NativeMethods.ConsumerOffsetsForTimesWithTimeout(_handle, timestamps, ms, ref outErr)
                : NativeMethods.ConsumerOffsetsForTimes(_handle, timestamps, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return CopyOffsetsForTimes(map, snapshot);
        });

    internal static IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> CopyOffsetsForTimes(
        IntPtr map, TimestampsSnapshot requested) =>
        OffsetAndTimestampMapMarshal.CopyOutAndDestroy(map, requested.Topics, requested.Partitions, requested.Count);

    private IReadOnlyList<PartitionInfo> PartitionsFor(string topic, long? timeoutMs) =>
        QuerySync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr topicPointer = arguments.Utf8(topic);
            IntPtr list = timeoutMs is long ms
                ? NativeMethods.ConsumerPartitionsForWithTimeout(_handle, topicPointer, ms, ref outErr)
                : NativeMethods.ConsumerPartitionsFor(_handle, topicPointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return PartitionInfoListMarshal.CopyOutAndDestroy(list);
        });

    private IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(long? timeoutMs) =>
        QuerySync(_ =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = timeoutMs is long ms
                ? NativeMethods.ConsumerListTopicsWithTimeout(_handle, ms, ref outErr)
                : NativeMethods.ConsumerListTopics(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return TopicPartitionInfoMapMarshal.CopyOutAndDestroy(map);
        });

    private void RunPartitionsSync(
        (string Topic, int Partition)[] snapshot,
        NativePartitionsCall call,
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
                Route(handle => routed(handle, snapshot));
                return;
            }
        }

        CallSync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            call(_handle, arguments.TopicPartitionList(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    // Inside a callback a call the consumer's own ConsumerHandle has goes to it, as Python's binding routes it, and a
    // waiting call it lacks is refused before anything reaches the core.
    private bool RoutesToTheHandle => _reporter.InCallback;

    private void Route(Action<NativeConsumerHandle> call)
    {
        int owner = EnterUse();
        try
        {
            call(CallbackHandle());
        }
        finally
        {
            ExitUse(owner);
        }
    }

    private T Route<T>(Func<NativeConsumerHandle, T> call)
    {
        int owner = EnterUse();
        try
        {
            return call(CallbackHandle());
        }
        finally
        {
            ExitUse(owner);
        }
    }

    private NativeConsumerHandle CallbackHandle()
    {
        lock (_callbackHandleGate)
        {
            return _callbackHandle ??= NativeConsumerHandle.Create(this, _handle);
        }
    }

    private void RefuseInsideACallback()
    {
        if (!_reporter.InCallback)
        {
            return;
        }

        if (IsClosed())
        {
            throw new ObjectDisposedException(nameof(NativeConsumer));
        }

        throw new InvalidOperationException(ConcurrentAccessMessage);
    }

    // Inside a callback a call is refused as closed only once the close has ended, so a listener the close runs can
    // call back in.
    private bool IsClosed() =>
        _reporter.InCallback ? Volatile.Read(ref _closeEnded) : Volatile.Read(ref _closed) != Open;

    private void CallSync(Action<NativeCallArguments> call, UseKind kind = UseKind.Owned)
    {
        int owner = EnterUse(kind);
        try
        {
            ExecuteCallbacks();
            using NativeCallArguments arguments = new NativeCallArguments();
            call(arguments);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    private T QuerySync<T>(Func<NativeCallArguments, T> call, UseKind kind = UseKind.Owned)
    {
        int owner = EnterUse(kind);
        try
        {
            ExecuteCallbacks();
            using NativeCallArguments arguments = new NativeCallArguments();
            return call(arguments);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    private void ExecuteCallbacks()
    {
        if (_pump is not null)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerExecuteCallbacks(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static long TimeoutMs(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "Timeout must not be negative");
        }

        return (long)timeout.TotalMilliseconds;
    }

    // Commit and Position build Java's Timer from the timeout, so they take its message.
    internal static long TimerTimeoutMs(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                timeout,
                "Invalid negative timeout " + ((long)Math.Floor(timeout.TotalMilliseconds)).ToString(CultureInfo.InvariantCulture));
        }

        return (long)timeout.TotalMilliseconds;
    }

    private static void RequireTopicPartition(string topic, int partition)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partition), partition, "Partition must not be negative.");
        }
    }

    private static void RequirePositionPartition(TopicPartition partition)
    {
        if (partition.Topic is null)
        {
            throw new ArgumentNullException(nameof(partition), "TopicPartition.Topic must not be null.");
        }

        if (partition.Partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition.Partition, "Partition must not be negative.");
        }
    }

    internal void CloseSync() => CloseSync(IntPtr.Zero);

    internal void CloseSync(CloseOptions option)
    {
        RequireCloseOptions(option);
        IntPtr options = NewCloseOptions(option);
        try
        {
            CloseSync(options);
        }
        finally
        {
            DestroyCloseOptions(options);
        }
    }

    private void RequireCloseOptions(CloseOptions option)
    {
        if (option is null)
        {
            throw new ArgumentNullException(nameof(option));
        }

        if (option.Timeout() < TimeSpan.Zero && !IsMock)
        {
            throw new ArgumentOutOfRangeException(nameof(option), CloseNegativeTimeoutMessage);
        }
    }

    private static IntPtr NewCloseOptions(CloseOptions option)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr options = NativeMethods.CloseOptionsWithOperation(Operation(option.GroupMembershipOperation()), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (option.Timeout() is TimeSpan timeout)
        {
            try
            {
                NativeMethods.CloseOptionsWithTimeout(options, (long)timeout.TotalMilliseconds, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
            catch
            {
                DestroyCloseOptions(options);
                throw;
            }
        }

        return options;
    }

    private static void DestroyCloseOptions(IntPtr options)
    {
        if (options == IntPtr.Zero)
        {
            return;
        }

        IntPtr destroyErr = IntPtr.Zero;
        NativeMethods.CloseOptionsDestroy(options, ref destroyErr);
        FreeError(ref destroyErr);
    }

    private static IntPtr Operation(GroupMembershipOperation operation)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = operation switch
        {
            GroupMembershipOperation.LeaveGroup => NativeMethods.CloseOptionsLeaveGroup(ref outErr),
            GroupMembershipOperation.RemainInGroup => NativeMethods.CloseOptionsRemainInGroup(ref outErr),
            _ => NativeMethods.CloseOptionsDefault(ref outErr),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    private void CloseSync(IntPtr options)
    {
        if (_reporter.InCallback)
        {
            ThrowUnlessClosing();
            return;
        }

        if (TryEnterUse(out int owner))
        {
            try
            {
                ExecuteCallbacks();
            }
            finally
            {
                ExitUse(owner);
            }
        }

        _closeGate.Wait();
        try
        {
            if (!TryBeginExplicitClose(Environment.CurrentManagedThreadId))
            {
                return;
            }

            try
            {
                IntPtr outErr = IntPtr.Zero;
                if (options == IntPtr.Zero)
                {
                    NativeMethods.ConsumerClose(_handle, ref outErr);
                }
                else
                {
                    NativeMethods.ConsumerCloseWithOptions(_handle, options, ref outErr);
                }

                ReopenIfRefused(ref outErr);
                TearDownThenThrow(ref outErr);
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

    private void ReopenIfRefused(ref IntPtr error)
    {
        if (error != IntPtr.Zero && NativeMethods.Code(error) == LocalConcurrentModification)
        {
            // An op still in the core holds it: Java's close() fails in acquire() and the consumer stays open.
            lock (_owner)
            {
                Volatile.Write(ref _closed, Open);
                _closingThread = 0;
            }

            KafkaException.ThrowIfFfiFailed(ref error);
        }

        Volatile.Write(ref _closed, Closing);
    }

    private void TearDownThenThrow(ref IntPtr error, Exception? kept = null)
    {
        try
        {
            TearDown();
        }
        catch
        {
            FreeError(ref error);
            throw;
        }

        if (kept is not null)
        {
            FreeError(ref error);
            throw KafkaException.WrapAtClose(kept);
        }

        KafkaException.ThrowIfFfiFailed(ref error);
    }

    internal void UpdatePartitions(
        string topic,
        int partitionCount,
        int leaderId,
        string leaderHost,
        int leaderPort)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (leaderHost is null)
        {
            throw new ArgumentNullException(nameof(leaderHost));
        }

        if (partitionCount < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitionCount), partitionCount, "Partition count must not be negative.");
        }

        CallMock((mock, arguments) =>
        {
            IntPtr partitions = arguments.List();
            for (int i = 0; i < partitionCount; i++)
            {
                IntPtr leader = arguments.Node(leaderId, leaderHost, leaderPort, rack: null);
                IntPtr replicas = arguments.List();
                arguments.Add(replicas, leader);
                arguments.Add(partitions, arguments.PartitionInfo(topic, i, leader, replicas, replicas));
            }

            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockConsumerUpdatePartitions(mock, arguments.Utf8(topic), partitions, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void Assign(IReadOnlyList<(string Topic, int Partition)> topicPartitions)
    {
        if (topicPartitions is null)
        {
            throw new ArgumentNullException(nameof(topicPartitions));
        }

        (string Topic, int Partition)[] snapshot = new (string, int)[topicPartitions.Count];
        for (int i = 0; i < snapshot.Length; i++)
        {
            (string topic, int partition) = topicPartitions[i];
            if (topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(topicPartitions));
            }

            if (partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(topicPartitions), partition, "Partition must not be negative.");
            }

            snapshot[i] = (topic, partition);
        }

        RunPartitionsSync(snapshot, NativeMethods.ConsumerAssign);
    }

    internal void AddRecord(
        string topic,
        int partition,
        long offset,
        byte[]? key,
        byte[]? value)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "Partition must not be negative.");
        }

        CallMock((mock, arguments) =>
        {
            IntPtr keyBytes = KeepRecordBytes(key);
            IntPtr valueBytes = KeepRecordBytes(value);
            IntPtr outErr = IntPtr.Zero;
            IntPtr record = NativeMethods.ConsumerRecordNew(
                arguments.Utf8(topic), partition, offset, keyBytes, valueBytes, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            try
            {
                NativeMethods.MockConsumerAddRecord(mock, record, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
            finally
            {
                IntPtr destroyError = IntPtr.Zero;
                NativeMethods.ConsumerRecordDestroy(record, ref destroyError);
                KafkaException.ThrowIfFfiFailed(ref destroyError);
            }
        });
    }

    // The mock shares a record's key and value bytes until the records holding them are destroyed, so each one
    // is copied into an unmanaged kafka_Bytes_t block that lives until the consumer's destroy has returned and no
    // poll holds a records handle any more.
    private IntPtr KeepRecordBytes(byte[]? data)
    {
        if (data is null)
        {
            return IntPtr.Zero;
        }

        int header = Marshal.SizeOf<KafkaBytes>();
        IntPtr block = Marshal.AllocHGlobal(header + Math.Max(data.Length, 1));
        lock (_recordBytes)
        {
            _recordBytes.Add(block);
        }

        IntPtr bytes = block + header;
        Marshal.Copy(data, 0, bytes, data.Length);
        Marshal.StructureToPtr(new KafkaBytes(bytes, data.Length), block, false);
        return block;
    }

    private void AfterDestroy()
    {
        _reporter.MarkDestroyed();
        lock (_callbackHandleGate)
        {
            _callbackHandle?.Dispose();
        }

        Interlocked.Exchange(ref _listenerRegistration, null)?.Release();
        lock (_parkedCommitCallbacks)
        {
            foreach (CommitCallbackRegistration registration in _parkedCommitCallbacks)
            {
                registration.Release();
            }

            _parkedCommitCallbacks.Clear();
            _parkedCommitCallbacksReleased = true;
        }

        _pump?.ReleaseNotifyOpaque();
        lock (_recordBytes)
        {
            _recordBytesReleasable = true;
            if (_recordsHeld == 0)
            {
                FreeRecordBytes();
            }
        }
    }

    private void HoldRecords()
    {
        lock (_recordBytes)
        {
            _recordsHeld++;
        }
    }

    private void DestroyRecords(IntPtr records)
    {
        try
        {
            ConsumerRecordsMarshal.Destroy(records);
        }
        finally
        {
            lock (_recordBytes)
            {
                _recordsHeld--;
                if (_recordsHeld == 0 && _recordBytesReleasable)
                {
                    FreeRecordBytes();
                }
            }
        }
    }

    private void FreeRecordBytes()
    {
        foreach (IntPtr block in _recordBytes)
        {
            Marshal.FreeHGlobal(block);
        }

        _recordBytes.Clear();
    }

    internal void SetPollException(KafkaException? exception) =>
        CallMock((mock, _) =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockConsumerSetPollError(
                mock, exception is null ? IntPtr.Zero : ManagedExceptionCarrier.ToError(exception), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    internal void Wakeup()
    {
        if (!TryEnterUse(out int owner, UseKind.Unowned))
        {
            return;
        }

        try
        {
            CountUserWakeup();
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerWakeup(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    internal void CountUserWakeup()
    {
        lock (_wakeups)
        {
            _userWakeups++;
            _ownWakeLeft = false;
        }
    }

    internal bool OwnWakeRaised(ref IntPtr error)
    {
        bool woken = error != IntPtr.Zero && NativeMethods.Code(error) == WakeupCode;
        if (woken)
        {
            WakeAgainWhileADisposeWaits();
        }

        if (!OwnWakeOutcome(ConsumerCompletion.NoOwnWake, woken, ending: false))
        {
            return false;
        }

        FreeError(ref error);
        return true;
    }

    private void WakeAgainWhileADisposeWaits()
    {
        lock (_wakeups)
        {
            if (!_disposeWaking)
            {
                return;
            }
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerWakeup(_handle, ref outErr);
        FreeError(ref outErr);
    }

    private void WakeForTheDispose(bool waking)
    {
        lock (_wakeups)
        {
            _disposeWaking = waking;
        }

        if (waking)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerWakeup(_handle, ref outErr);
            FreeError(ref outErr);
        }
    }

    private long OwnWake()
    {
        long mark = Interlocked.Read(ref _userWakeups);
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerWakeup(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return mark;
    }

    private bool OwnWakeOutcome(long mark, bool woken, bool ending)
    {
        lock (_wakeups)
        {
            if (woken)
            {
                bool left = _ownWakeLeft;
                _ownWakeLeft = false;
                return left && !ending;
            }

            if (mark != ConsumerCompletion.NoOwnWake && mark == _userWakeups)
            {
                _ownWakeLeft = true;
            }
        }

        return false;
    }

    internal ConsumerGroupMetadata GroupMetadata()
    {
        ThrowIfClosed();
        if (!IsMock && !_groupIdDefined)
        {
            throw new KafkaException(InvalidGroupIdCode, GroupIdNotDefinedMessage, isRetriable: false);
        }

        int owner = EnterUse(UseKind.UnownedOnMock);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr metadata = NativeMethods.ConsumerGroupMetadata(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return ConsumerGroupMetadataMarshal.CopyOutAndDestroy(metadata);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    internal IReadOnlyDictionary<MetricName, IMetric> Metrics()
    {
        int owner = EnterUse();
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = NativeMethods.ConsumerMetrics(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return KafkaMetricMapMarshal.CopyOutAndDestroy(map);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    internal ConsumerHandle CreateReentrancyHandle()
    {
        int owner = EnterUse(UseKind.Unowned);
        try
        {
            return ConsumerHandle.Create(this);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    internal IReadOnlyCollection<TopicPartition> Assignment()
    {
        if (RoutesToTheHandle)
        {
            return Route(handle => handle.Assignment());
        }

        int owner = EnterUse();
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = NativeMethods.ConsumerAssignment(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return TopicPartitionListMarshal.CopyOutAndDestroy(list);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    internal IReadOnlyCollection<string> Subscription()
    {
        int owner = EnterUse();
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = NativeMethods.ConsumerSubscription(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return StringListMarshal.CopyOutAndDestroy(list);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    internal IReadOnlyCollection<TopicPartition> Paused()
    {
        if (RoutesToTheHandle)
        {
            return Route(handle => handle.Paused());
        }

        int owner = EnterUse();
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = NativeMethods.ConsumerPaused(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return TopicPartitionListMarshal.CopyOutAndDestroy(list);
        }
        finally
        {
            ExitUse(owner);
        }
    }

    internal void EnforceRebalance(string? reason) =>
        CallSync(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            if (reason is null)
            {
                NativeMethods.ConsumerEnforceRebalance(_handle, ref outErr);
            }
            else
            {
                NativeMethods.ConsumerEnforceRebalanceWithReason(_handle, arguments.Utf8(reason), ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        },
        UseKind.UnownedOnMock);

    public void Dispose()
    {
        if (_reporter.InCallback)
        {
            DisposeInsideACallback();
            return;
        }

        _closeGate.Wait();
        try
        {
            if (!TryBeginClose())
            {
                return;
            }

            try
            {
                WakeUpThenCloseAfterTheUses();
            }
            catch (Exception)
            {
                // Dispose does not throw; Close reports the same error.
            }

            _reporter.WaitForKeptCallbacks();
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

    private void DisposeInsideACallback()
    {
        // The sync consumer's callback runs inside the op that holds the consumer, so Dispose throws as Close
        // does; the async consumer's Dispose schedules the teardown, which waits for that op to end.
        if (_pump is null)
        {
            ThrowUnlessClosing();
        }
        else
        {
            _ = Task.Run(DisposeOnThePumpAsync);
        }
    }

    private void ThrowUnlessClosing()
    {
        if (Volatile.Read(ref _closed) != Closing)
        {
            throw new InvalidOperationException(ConcurrentAccessMessage);
        }
    }

    private void WakeUpThenCloseAfterTheUses()
    {
        WakeForTheDispose(waking: true);
        try
        {
            _uses.WaitForNone();
        }
        finally
        {
            WakeForTheDispose(waking: false);
        }

        IntPtr outErr = IntPtr.Zero;
        if (_pump is null)
        {
            NativeMethods.ConsumerClose(_handle, ref outErr);
        }
        else
        {
            outErr = CloseOnThePump();
        }

        FreeError(ref outErr);
    }

    private void TearDown()
    {
        lock (_owner)
        {
            Volatile.Write(ref _closeEnded, true);
        }

        if (_pump is not null)
        {
            _uses.WaitForNone();
            _pump.Dispose();
        }

        _uses.WaitForNoneThenMarkDestroyed();
        _handle.CloseExplicitly();
    }

    /// <summary>The validated commit offsets, as the parallel arrays the offset map is built from.</summary>
    internal readonly struct CommitOffsetsSnapshot
    {
        internal CommitOffsetsSnapshot(
            string[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, string[] metadata, int count)
        {
            Topics = topics;
            Partitions = partitions;
            Offsets = offsets;
            LeaderEpochs = leaderEpochs;
            Metadata = metadata;
            Count = count;
        }

        internal string[] Topics { get; }

        internal int[] Partitions { get; }

        internal long[] Offsets { get; }

        internal int[] LeaderEpochs { get; }

        internal string[] Metadata { get; }

        internal int Count { get; }
    }

    internal static CommitOffsetsSnapshot SnapshotCommitOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        if (offsets is null)
        {
            throw new ArgumentNullException(nameof(offsets));
        }

        int count = offsets.Count;
        string[] topics = new string[count];
        int[] partitions = new int[count];
        long[] offsetValues = new long[count];
        int[] leaderEpochs = new int[count];
        string[] metadata = new string[count];
        int index = 0;
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
        {
            TopicPartition tp = entry.Key;
            if (tp.Topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(offsets));
            }

            if (tp.Partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(offsets), tp.Partition, "Partition must not be negative.");
            }

            OffsetAndMetadata value = entry.Value;
            if (value is null)
            {
                throw new ArgumentException("Offset value must not be null.", nameof(offsets));
            }

            topics[index] = tp.Topic;
            partitions[index] = tp.Partition;
            offsetValues[index] = value.Offset;
            leaderEpochs[index] = value.LeaderEpoch ?? -1;
            metadata[index] = value.Metadata ?? string.Empty;
            index++;
        }

        return new CommitOffsetsSnapshot(topics, partitions, offsetValues, leaderEpochs, metadata, count);
    }

    // A negative partition is refused, although Java's assign accepts one.
    internal static (string Topic, int Partition)[] SnapshotPartitions(IReadOnlyCollection<TopicPartition> partitions)
    {
        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        (string Topic, int Partition)[] snapshot = new (string, int)[partitions.Count];
        int index = 0;
        foreach (TopicPartition tp in partitions)
        {
            if (tp.Topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(partitions));
            }

            if (tp.Partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(partitions), tp.Partition, "Partition must not be negative.");
            }

            snapshot[index++] = (tp.Topic, tp.Partition);
        }

        return snapshot;
    }

    /// <summary>The validated timestamps to search, as the parallel arrays the timestamp map is built from.</summary>
    internal readonly struct TimestampsSnapshot
    {
        internal TimestampsSnapshot(string[] topics, int[] partitions, long[] timestamps, int count)
        {
            Topics = topics;
            Partitions = partitions;
            Timestamps = timestamps;
            Count = count;
        }

        internal string[] Topics { get; }

        internal int[] Partitions { get; }

        internal long[] Timestamps { get; }

        internal int Count { get; }
    }

    // A negative timestamp is passed through: ListOffsets uses negative special timestamps.
    internal static TimestampsSnapshot SnapshotTimestamps(IReadOnlyDictionary<TopicPartition, long> timestampsToSearch)
    {
        if (timestampsToSearch is null)
        {
            throw new ArgumentNullException(nameof(timestampsToSearch));
        }

        int count = timestampsToSearch.Count;
        string[] topics = new string[count];
        int[] partitionArray = new int[count];
        long[] timestamps = new long[count];
        int index = 0;
        foreach (KeyValuePair<TopicPartition, long> entry in timestampsToSearch)
        {
            TopicPartition tp = entry.Key;
            if (tp.Topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(timestampsToSearch));
            }

            if (tp.Partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(timestampsToSearch), tp.Partition, "Partition must not be negative.");
            }

            topics[index] = tp.Topic;
            partitionArray[index] = tp.Partition;
            timestamps[index] = entry.Value;
            index++;
        }

        return new TimestampsSnapshot(topics, partitionArray, timestamps, count);
    }

    internal void UpdateBeginningOffset(string topic, int partition, long offset) =>
        UpdateOffset(topic, partition, offset, beginning: true);

    internal void UpdateEndOffset(string topic, int partition, long offset) =>
        UpdateOffset(topic, partition, offset, beginning: false);

    private void UpdateOffset(string topic, int partition, long offset, bool beginning)
    {
        RequireTopicPartition(topic, partition);
        CallMock((mock, arguments) =>
        {
            IntPtr offsets = arguments.LongMap(new[] { topic }, new[] { partition }, new[] { offset }, 1);
            IntPtr outErr = IntPtr.Zero;
            if (beginning)
            {
                NativeMethods.MockConsumerUpdateBeginningOffsets(mock, offsets, ref outErr);
            }
            else
            {
                NativeMethods.MockConsumerUpdateEndOffsets(mock, offsets, ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    private void CallMock(Action<IntPtr, NativeCallArguments> call)
    {
        bool added = false;
        int owner = EnterUse();
        try
        {
            using NativeCallArguments arguments = new NativeCallArguments();
            call(_handle.DangerousAddRefClass(ref added), arguments);
        }
        finally
        {
            if (added)
            {
                _handle.DangerousRelease();
            }

            ExitUse(owner);
        }
    }

    private bool TryBeginClose()
    {
        if (Interlocked.CompareExchange(ref _closed, Closing, Open) != Open)
        {
            return false;
        }

        // A mock call waiting for another thread's use is a new call: it wakes and is refused as closed.
        PulseOwnerWaiters();
        return true;
    }

    private bool TryBeginExplicitClose(int thread)
    {
        lock (_owner)
        {
            while (true)
            {
                if (Volatile.Read(ref _closed) != Open)
                {
                    return false;
                }

                if (!HeldByAnotherThread(UseKind.Owned, thread))
                {
                    break;
                }

                if (RefusedWhileBusy)
                {
                    throw new InvalidOperationException(ConcurrentAccessMessage);
                }

                Monitor.Wait(_owner);
            }

            if (Interlocked.CompareExchange(ref _closed, ClosePending, Open) != Open)
            {
                return false;
            }

            _closingThread = thread;
            if (Volatile.Read(ref _calls) == 0)
            {
                Volatile.Write(ref _closed, Closing);
            }

            return true;
        }
    }

    private void EndExplicitClose()
    {
        lock (_owner)
        {
            _closingThread = 0;
            Monitor.PulseAll(_owner);
        }
    }

    private void PulseOwnerWaiters()
    {
        lock (_owner)
        {
            Monitor.PulseAll(_owner);
        }
    }

    // A call waiting when the window opens is woken, and refused.
    private void RefuseOtherThreads(bool refusing)
    {
        lock (_owner)
        {
            _refusingUses += refusing ? 1 : -1;
            Monitor.PulseAll(_owner);
        }
    }

    private bool IsMock => _handle is SafeMockConsumerHandle;

    private int EnterUse(UseKind kind = UseKind.Owned, int thread = 0)
    {
        if (!TryEnterUse(out int owner, kind, thread))
        {
            throw new ObjectDisposedException(nameof(NativeConsumer));
        }

        return owner;
    }

    // A use records the thread that made the call, or the thread a later step of that call is made for; an
    // unchecked use records none. The closed check comes first, then the owner check, as in Python. On a mock, a call
    // that another thread's use or close keeps out waits for it, as Java's synchronized MockConsumer does, unless an
    // async callback is running, which refuses it as Python's mock does.
    private bool TryEnterUse(out int owner, UseKind kind = UseKind.Owned, int thread = 0)
    {
        owner = ChecksOwner(kind) ? (thread != 0 ? thread : Environment.CurrentManagedThreadId) : 0;
        bool refused = false;
        bool closed = false;
        lock (_owner)
        {
            while (true)
            {
                _uses.Enter();
                Interlocked.Increment(ref _calls);
                bool closingElsewhere = owner != 0 && _closingThread != 0 && _closingThread != owner;
                closed = !closingElsewhere && IsClosed();
                bool heldElsewhere =
                    !closingElsewhere && !closed && owner != 0 && _ownerUses != 0 && _ownerThread != owner;
                if (!closingElsewhere && !heldElsewhere)
                {
                    break;
                }

                if (RefusedWhileBusy)
                {
                    refused = true;
                    break;
                }

                Interlocked.Decrement(ref _calls);
                if (_uses.Exit() && Volatile.Read(ref _closed) != Open)
                {
                    _uses.Wake();
                }

                Monitor.Wait(_owner);
            }

            if (!refused && !closed && owner != 0)
            {
                _ownerThread = owner;
                _ownerUses++;
            }
        }

        if (refused || closed)
        {
            ExitUse(0);
            owner = 0;
            if (refused)
            {
                throw new InvalidOperationException(ConcurrentAccessMessage);
            }

            return false;
        }

        return true;
    }

    private void ExitUse(int owner)
    {
        if (owner != 0)
        {
            lock (_owner)
            {
                if (--_ownerUses == 0)
                {
                    _ownerThread = 0;
                    Monitor.PulseAll(_owner);
                }
            }
        }

        Interlocked.Decrement(ref _calls);
        if (_uses.Exit() && Volatile.Read(ref _closed) != Open)
        {
            _uses.Wake();
        }
    }

    private bool ChecksOwner(UseKind kind) =>
        (kind == UseKind.Owned || (kind == UseKind.UnownedOnMock && !IsMock)) && !_reporter.InCallback;

    private bool RefusedWhileBusy => !IsMock || _reporter.RunsAsyncCallback || Volatile.Read(ref _refusingUses) != 0;

    private bool HeldByAnotherThread(UseKind kind, int thread) =>
        ChecksOwner(kind) && _ownerUses != 0 && _ownerThread != thread;

    private void ThrowIfHeldByAnotherThread(int thread)
    {
        lock (_owner)
        {
            bool closingElsewhere = ChecksOwner(UseKind.Owned) && _closingThread != 0 && _closingThread != thread;
            bool busy = closingElsewhere ||
                (Volatile.Read(ref _closed) == Open && HeldByAnotherThread(UseKind.Owned, thread));
            if (busy && RefusedWhileBusy)
            {
                throw new InvalidOperationException(ConcurrentAccessMessage);
            }
        }
    }

    private static void FreeError(ref IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
            error = IntPtr.Zero;
        }
    }

    private void ThrowIfClosed()
    {
        int thread = Environment.CurrentManagedThreadId;
        bool refused;
        lock (_owner)
        {
            bool closingElsewhere = ChecksOwner(UseKind.Owned) && _closingThread != 0 && _closingThread != thread;
            refused = closingElsewhere && RefusedWhileBusy;
            if (!closingElsewhere && IsClosed())
            {
                throw new ObjectDisposedException(nameof(NativeConsumer));
            }
        }

        if (refused)
        {
            throw new InvalidOperationException(ConcurrentAccessMessage);
        }
    }

    /// <summary>
    /// Whether a use is checked against the owner: <c>Owned</c> always; <c>UnownedOnMock</c>, the three calls Java's
    /// MockConsumer leaves unsynchronized, only on a real consumer; <c>Unowned</c>, Wakeup and Handle, never.
    /// </summary>
    private enum UseKind
    {
        Owned,
        Unowned,
        UnownedOnMock,
    }
}
