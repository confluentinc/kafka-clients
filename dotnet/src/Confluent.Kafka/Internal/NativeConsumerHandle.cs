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
/// Calls a consumer's <c>kafka_consumer_ConsumerHandle_t</c> with arguments its caller has already validated.
/// </summary>
internal sealed class NativeConsumerHandle : IDisposable
{
    private readonly NativeConsumer _owner;
    private readonly SafeConsumerReentrancyHandle _handle;

    private NativeConsumerHandle(NativeConsumer owner, SafeConsumerReentrancyHandle handle)
    {
        _owner = owner;
        _handle = handle;
    }

    internal static NativeConsumerHandle Create(NativeConsumer owner) => Create(owner, owner.Handle);

    internal static NativeConsumerHandle Create(NativeConsumer owner, SafeConsumerHandle consumer)
    {
        IntPtr outErr = IntPtr.Zero;
        SafeConsumerReentrancyHandle handle = NativeMethods.ConsumerGetHandle(consumer, ref outErr);
        if (outErr != IntPtr.Zero || handle.IsInvalid)
        {
            handle.Dispose();
            KafkaException.ThrowIfFfiFailed(ref outErr);
            throw new InvalidOperationException("The consumer did not return a reentrancy handle.");
        }

        return new NativeConsumerHandle(owner, handle);
    }

    internal void Wakeup()
    {
        _owner.CountUserWakeup();
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerHandleWakeup(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    internal IReadOnlyCollection<TopicPartition> Assignment()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ConsumerHandleAssignment(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return TopicPartitionListMarshal.CopyOutAndDestroy(list);
    }

    internal IReadOnlyCollection<TopicPartition> Paused()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.ConsumerHandlePaused(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return TopicPartitionListMarshal.CopyOutAndDestroy(list);
    }

    internal void Pause((string Topic, int Partition)[] partitions) =>
        RunPartitions(partitions, NativeMethods.ConsumerHandlePause);

    internal void Resume((string Topic, int Partition)[] partitions) =>
        RunPartitions(partitions, NativeMethods.ConsumerHandleResume);

    internal void SeekToBeginning((string Topic, int Partition)[] partitions) =>
        RunPartitions(partitions, NativeMethods.ConsumerHandleSeekToBeginning);

    internal void SeekToEnd((string Topic, int Partition)[] partitions) =>
        RunPartitions(partitions, NativeMethods.ConsumerHandleSeekToEnd);

    internal void Seek(string topic, int partition, long offset)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerHandleSeekWithOffset(_handle, arguments.TopicPartition(topic, partition), offset, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    internal void SeekWithMetadata(string topic, int partition, OffsetAndMetadata offsetAndMetadata)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerHandleSeekWithOffsetAndMetadata(
            _handle,
            arguments.TopicPartition(topic, partition),
            arguments.OffsetAndMetadata(
                offsetAndMetadata.Offset, offsetAndMetadata.LeaderEpoch ?? -1, offsetAndMetadata.Metadata),
            ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    internal long Position(string topic, int partition, long? timeoutMs)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr topicPartition = arguments.TopicPartition(topic, partition);
        while (true)
        {
            IntPtr outErr = IntPtr.Zero;
            long position = timeoutMs is long ms
                ? NativeMethods.ConsumerHandlePositionWithTimeout(_handle, topicPartition, ms, ref outErr)
                : NativeMethods.ConsumerHandlePosition(_handle, topicPartition, ref outErr);
            if (_owner.OwnWakeRaised(ref outErr))
            {
                continue;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return position;
        }
    }

    internal long? CurrentLag(string topic, int partition)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr outErr = IntPtr.Zero;
        sbyte present = NativeMethods.ConsumerHandleCurrentLag(
            _handle, arguments.TopicPartition(topic, partition), out long lag, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return present != 0 ? lag : null;
    }

    internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        (string Topic, int Partition)[] partitions, long? timeoutMs)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr list = arguments.TopicPartitionList(partitions);
        while (true)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = timeoutMs is long ms
                ? NativeMethods.ConsumerHandleCommittedWithTimeout(_handle, list, ms, ref outErr)
                : NativeMethods.ConsumerHandleCommitted(_handle, list, ref outErr);
            if (_owner.OwnWakeRaised(ref outErr))
            {
                continue;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return _owner.CopyCommitted(map, partitions);
        }
    }

    internal IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        (string Topic, int Partition)[] partitions, long? timeoutMs)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr list = arguments.TopicPartitionList(partitions);
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = timeoutMs is long ms
            ? NativeMethods.ConsumerHandleBeginningOffsetsWithTimeout(_handle, list, ms, ref outErr)
            : NativeMethods.ConsumerHandleBeginningOffsets(_handle, list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return LongOffsetMapMarshal.CopyOutAndDestroy(map);
    }

    internal IReadOnlyDictionary<TopicPartition, long> EndOffsets((string Topic, int Partition)[] partitions, long? timeoutMs)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr list = arguments.TopicPartitionList(partitions);
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = timeoutMs is long ms
            ? NativeMethods.ConsumerHandleEndOffsetsWithTimeout(_handle, list, ms, ref outErr)
            : NativeMethods.ConsumerHandleEndOffsets(_handle, list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return LongOffsetMapMarshal.CopyOutAndDestroy(map);
    }

    internal IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        NativeConsumer.TimestampsSnapshot timestampsToSearch, long? timeoutMs)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr timestamps = arguments.LongMap(
            timestampsToSearch.Topics, timestampsToSearch.Partitions, timestampsToSearch.Timestamps, timestampsToSearch.Count);
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = timeoutMs is long ms
            ? NativeMethods.ConsumerHandleOffsetsForTimesWithTimeout(_handle, timestamps, ms, ref outErr)
            : NativeMethods.ConsumerHandleOffsetsForTimes(_handle, timestamps, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return NativeConsumer.CopyOffsetsForTimes(map, timestampsToSearch);
    }

    internal IReadOnlyList<PartitionInfo> PartitionsFor(string topic, long? timeoutMs)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr topicPointer = arguments.Utf8(topic);
        while (true)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = timeoutMs is long ms
                ? NativeMethods.ConsumerHandlePartitionsForWithTimeout(_handle, topicPointer, ms, ref outErr)
                : NativeMethods.ConsumerHandlePartitionsFor(_handle, topicPointer, ref outErr);
            if (_owner.OwnWakeRaised(ref outErr))
            {
                continue;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return PartitionInfoListMarshal.CopyOutAndDestroy(list);
        }
    }

    internal IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(long? timeoutMs)
    {
        while (true)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = timeoutMs is long ms
                ? NativeMethods.ConsumerHandleListTopicsWithTimeout(_handle, ms, ref outErr)
                : NativeMethods.ConsumerHandleListTopics(_handle, ref outErr);
            if (_owner.OwnWakeRaised(ref outErr))
            {
                continue;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return TopicPartitionInfoMapMarshal.CopyOutAndDestroy(map);
        }
    }

    internal ConsumerGroupMetadata GroupMetadata()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr metadata = NativeMethods.ConsumerHandleGroupMetadata(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return ConsumerGroupMetadataMarshal.CopyOutAndDestroy(metadata);
    }

    internal void CommitSync(long? timeoutMs)
    {
        using ConsumerCallbackReporter.HandleCommitScope commit = _owner.EnterHandleCommit();
        while (true)
        {
            IntPtr outErr = IntPtr.Zero;
            if (timeoutMs is long ms)
            {
                NativeMethods.ConsumerHandleCommitSyncWithTimeout(_handle, ms, ref outErr);
            }
            else
            {
                NativeMethods.ConsumerHandleCommitSync(_handle, ref outErr);
            }

            if (_owner.OwnWakeRaised(ref outErr))
            {
                continue;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return;
        }
    }

    internal void CommitSync(NativeConsumer.CommitOffsetsSnapshot offsets, long? timeoutMs)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr map = arguments.OffsetMap(offsets);
        using ConsumerCallbackReporter.HandleCommitScope commit = _owner.EnterHandleCommit();
        while (true)
        {
            IntPtr outErr = IntPtr.Zero;
            if (timeoutMs is long ms)
            {
                NativeMethods.ConsumerHandleCommitSyncWithOffsetsTimeout(_handle, map, ms, ref outErr);
            }
            else
            {
                NativeMethods.ConsumerHandleCommitSyncWithOffsets(_handle, map, ref outErr);
            }

            if (_owner.OwnWakeRaised(ref outErr))
            {
                continue;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return;
        }
    }

    internal void CommitAsync()
    {
        using ConsumerCallbackReporter.HandleCommitScope commit = _owner.EnterHandleCommit();
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerHandleCommitAsync(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    internal void CommitAsync(IOffsetCommitCallback callback) =>
        _owner.CommitWithRegistration(callback, emptyOffsets: false, callbackHandle =>
        {
            using ConsumerCallbackReporter.HandleCommitScope commit = _owner.EnterHandleCommit();
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerHandleCommitAsyncWithCallback(_handle, callbackHandle, ref outErr);
            return outErr;
        });

    internal void CommitAsync(NativeConsumer.CommitOffsetsSnapshot offsets, IOffsetCommitCallback? callback)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr map = arguments.OffsetMap(offsets);
        _owner.CommitWithRegistration(callback, offsets.Count == 0, callbackHandle =>
        {
            using ConsumerCallbackReporter.HandleCommitScope commit = _owner.EnterHandleCommit();
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerHandleCommitAsyncWithOffsetsCallback(_handle, map, callbackHandle, ref outErr);
            return outErr;
        });
    }

    internal void CommitAsync(IAsyncOffsetCommitCallback callback) =>
        _owner.CommitWithRegistration(callback, emptyOffsets: false, callbackHandle =>
        {
            using ConsumerCallbackReporter.HandleCommitScope commit = _owner.EnterHandleCommit();
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerHandleCommitAsyncWithCallback(_handle, callbackHandle, ref outErr);
            return outErr;
        });

    internal void CommitAsync(NativeConsumer.CommitOffsetsSnapshot offsets, IAsyncOffsetCommitCallback callback)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr map = arguments.OffsetMap(offsets);
        _owner.CommitWithRegistration(callback, offsets.Count == 0, callbackHandle =>
        {
            using ConsumerCallbackReporter.HandleCommitScope commit = _owner.EnterHandleCommit();
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerHandleCommitAsyncWithOffsetsCallback(_handle, map, callbackHandle, ref outErr);
            return outErr;
        });
    }

    public void Dispose() => _handle.Dispose();

    private delegate void PartitionsCall(SafeConsumerReentrancyHandle handle, IntPtr partitions, ref IntPtr outErr);

    private void RunPartitions((string Topic, int Partition)[] partitions, PartitionsCall call)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr outErr = IntPtr.Zero;
        call(_handle, arguments.TopicPartitionList(partitions), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }
}
