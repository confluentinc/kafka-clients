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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// Calls back into a consumer from inside one of its callbacks (an <see cref="IConsumerRebalanceListener"/> method or an
/// <see cref="IOffsetCommitCallback"/>); the consumer's own calls made there go through such a handle. On a
/// <see cref="KafkaConsumer{TKey, TValue}"/> every operation other than <see cref="Wakeup"/>, <see cref="Assignment"/>
/// and <see cref="Paused"/> works only inside such a callback, and throws <see cref="InvalidOperationException"/>
/// elsewhere. The handle holds its consumer weakly: once the consumer is destroyed, <see cref="Wakeup"/> does nothing,
/// <see cref="Assignment"/> and <see cref="Paused"/> still return, and every other operation throws
/// <see cref="InvalidOperationException"/>.
/// </summary>
public sealed class ConsumerHandle : IDisposable
{
    // IDisposable only, not IAsyncDisposable: every operation, the destroy included, is a synchronous C call.
    private readonly NativeConsumerHandle _native;

    private int _disposed;

    private ConsumerHandle(NativeConsumerHandle native)
    {
        _native = native;
    }

    internal static ConsumerHandle Create(NativeConsumer consumer) =>
        new ConsumerHandle(NativeConsumerHandle.Create(consumer));

    /// <summary>Wakes up the consumer, as <see cref="IConsumerCommon.Wakeup"/> does.</summary>
    public void Wakeup()
    {
        ThrowIfDisposed();
        _native.Wakeup();
    }

    /// <summary>Returns the partitions assigned to the consumer.</summary>
    public IReadOnlyCollection<TopicPartition> Assignment()
    {
        ThrowIfDisposed();
        return _native.Assignment();
    }

    /// <summary>Returns the partitions the consumer has paused.</summary>
    public IReadOnlyCollection<TopicPartition> Paused()
    {
        ThrowIfDisposed();
        return _native.Paused();
    }

    /// <summary>Suspends fetching from <paramref name="partitions"/>.</summary>
    public void Pause(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.Pause(NativeConsumer.SnapshotPartitions(partitions));
    }

    /// <summary>Resumes fetching from <paramref name="partitions"/>.</summary>
    public void Resume(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.Resume(NativeConsumer.SnapshotPartitions(partitions));
    }

    /// <summary>Seeks <paramref name="partitions"/> to their first offset.</summary>
    public void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.SeekToBeginning(NativeConsumer.SnapshotPartitions(partitions));
    }

    /// <summary>Seeks <paramref name="partitions"/> to their last offset.</summary>
    public void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.SeekToEnd(NativeConsumer.SnapshotPartitions(partitions));
    }

    /// <summary>Sets the offset the consumer fetches next from <paramref name="partition"/>.</summary>
    public void Seek(TopicPartition partition, long offset)
    {
        ThrowIfDisposed();
        ValidatePartition(partition);
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "seek offset must not be a negative number");
        }

        _native.Seek(partition.Topic, partition.Partition, offset);
    }

    /// <summary>
    /// Sets the offset the consumer fetches next from <paramref name="partition"/>, with its leader epoch and metadata.
    /// </summary>
    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)
    {
        ThrowIfDisposed();
        ValidatePartition(partition);
        if (offsetAndMetadata is null)
        {
            throw new ArgumentNullException(nameof(offsetAndMetadata));
        }

        _native.SeekWithMetadata(partition.Topic, partition.Partition, offsetAndMetadata);
    }

    /// <summary>Returns the offset the consumer fetches next from <paramref name="partition"/>.</summary>
    public long Position(TopicPartition partition)
    {
        ThrowIfDisposed();
        ValidatePartition(partition);
        return _native.Position(partition.Topic, partition.Partition, timeoutMs: null);
    }

    /// <summary>
    /// Returns the offset the consumer fetches next from <paramref name="partition"/>, waiting at most
    /// <paramref name="timeout"/>.
    /// </summary>
    public long Position(TopicPartition partition, TimeSpan timeout)
    {
        ThrowIfDisposed();
        long timeoutMs = NativeConsumer.TimerTimeoutMs(timeout);
        ValidatePartition(partition);
        return _native.Position(partition.Topic, partition.Partition, timeoutMs);
    }

    /// <summary>
    /// Returns how far the consumer's position in <paramref name="partition"/> is behind its end, or
    /// <see langword="null"/> when the consumer does not know.
    /// </summary>
    public long? CurrentLag(TopicPartition partition)
    {
        ThrowIfDisposed();
        ValidatePartition(partition);
        return _native.CurrentLag(partition.Topic, partition.Partition);
    }

    /// <summary>
    /// Returns the last committed offset of each of <paramref name="partitions"/>, <see langword="null"/> where it has
    /// none.
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        return _native.Committed(NativeConsumer.SnapshotPartitions(partitions));
    }

    /// <summary>Returns the first offset of each of <paramref name="partitions"/>.</summary>
    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        return _native.BeginningOffsets(NativeConsumer.SnapshotPartitions(partitions));
    }

    /// <summary>Returns the end offset of each of <paramref name="partitions"/>.</summary>
    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        return _native.EndOffsets(NativeConsumer.SnapshotPartitions(partitions));
    }

    /// <summary>
    /// Returns, for each partition, the earliest offset whose timestamp is at least the given one, <see langword="null"/>
    /// where there is none.
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch)
    {
        ThrowIfDisposed();
        return _native.OffsetsForTimes(NativeConsumer.SnapshotTimestamps(timestampsToSearch));
    }

    /// <summary>Commits the consumed offsets and waits until the commit completes.</summary>
    public void Commit()
    {
        ThrowIfDisposed();
        _native.CommitSync();
    }

    /// <summary>Commits <paramref name="offsets"/> and waits until the commit completes.</summary>
    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        ThrowIfDisposed();
        _native.CommitSync(NativeConsumer.SnapshotCommitOffsets(offsets));
    }

    /// <summary>Starts a commit of the consumed offsets and returns without waiting for it.</summary>
    public void CommitAsync()
    {
        ThrowIfDisposed();
        _native.CommitAsync();
    }

    /// <summary>
    /// Starts a commit of the consumed offsets and returns without waiting for it; <paramref name="callback"/> receives
    /// its outcome.
    /// </summary>
    public void CommitAsync(IOffsetCommitCallback callback)
    {
        ThrowIfDisposed();
        if (callback is null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        _native.CommitAsync(callback);
    }

    /// <summary>Starts a commit of <paramref name="offsets"/> and returns without waiting for it.</summary>
    public void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets) =>
        CommitAsync(offsets, callback: null);

    /// <summary>
    /// Starts a commit of <paramref name="offsets"/> and returns without waiting for it; <paramref name="callback"/>, when
    /// given, receives its outcome.
    /// </summary>
    public void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, IOffsetCommitCallback? callback)
    {
        ThrowIfDisposed();
        _native.CommitAsync(NativeConsumer.SnapshotCommitOffsets(offsets), callback);
    }

    /// <summary>Destroys the handle. The consumer is unaffected; a second call does nothing.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _native.Dispose();
    }

    private static void ValidatePartition(TopicPartition partition)
    {
        if (partition.Topic is null)
        {
            throw new ArgumentNullException(nameof(partition), "TopicPartition.Topic must not be null.");
        }

        if (partition.Partition < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partition), partition.Partition, "Partition must not be negative.");
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(ConsumerHandle));
        }
    }
}
