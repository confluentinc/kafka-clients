// Copyright 2026 Confluent Inc.
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

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// The sync flavour's consumer behind the factory seam: a <see cref="MockConsumer{TKey, TValue}"/>
/// (auto offset reset <c>earliest</c>, like the gRPC consumer servicer's mock) that applies the
/// <see cref="ConsumerScript"/>'s queued steps on the loop thread at the start of each poll, and
/// records the calls the servicer makes. Every call reaches the real mock (DoD §12).
/// </summary>
internal sealed class SyncScriptedConsumer : IConsumer<byte[], byte[]>, IMockConsumerOperations
{
    private readonly MockConsumer<byte[], byte[]> _inner =
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");

    private readonly ConsumerScript _script;

    internal SyncScriptedConsumer(ConsumerScript script)
    {
        _script = script;
    }

    /// <inheritdoc/>
    public ConsumerRecords<byte[], byte[]> Poll(TimeSpan timeout)
    {
        _script.ApplyPending(this);
        return _inner.Poll(timeout);
    }

    /// <inheritdoc/>
    public void Subscribe(IReadOnlyCollection<string> topics) => _inner.Subscribe(topics);

    /// <inheritdoc/>
    public void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener listener) =>
        _inner.Subscribe(topics, listener);

    /// <inheritdoc/>
    public void Unsubscribe() => _inner.Unsubscribe();

    /// <inheritdoc/>
    public void Assign(IReadOnlyCollection<TopicPartition> partitions) => _inner.Assign(partitions);

    /// <inheritdoc/>
    public void Pause(IReadOnlyCollection<TopicPartition> partitions) => _inner.Pause(partitions);

    /// <inheritdoc/>
    public void Resume(IReadOnlyCollection<TopicPartition> partitions) => _inner.Resume(partitions);

    /// <inheritdoc/>
    public void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions) => _inner.SeekToBeginning(partitions);

    /// <inheritdoc/>
    public void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions) => _inner.SeekToEnd(partitions);

    /// <inheritdoc/>
    public long Position(TopicPartition partition) => _inner.Position(partition);

    /// <inheritdoc/>
    public void Commit()
    {
        _script.OnCall("Commit");
        _inner.Commit();
    }

    /// <inheritdoc/>
    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        _script.OnCall("Commit(offsets)");
        _inner.Commit(offsets);
    }

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(IReadOnlyCollection<TopicPartition> partitions)
    {
        _script.OnCall("Committed");
        return _inner.Committed(partitions);
    }

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch) => _inner.OffsetsForTimes(timestampsToSearch);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions) =>
        _inner.BeginningOffsets(partitions);

    /// <inheritdoc/>
    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions) =>
        _inner.EndOffsets(partitions);

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _inner.PartitionsFor(topic);

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics() => _inner.ListTopics();

    /// <inheritdoc/>
    public void Close()
    {
        _script.OnCall("Close");
        _script.HandleUsableAtClose = IsHandleUsable();
        _inner.Close();
    }

    /// <inheritdoc/>
    public void Wakeup() => _inner.Wakeup();

    /// <inheritdoc/>
    public ConsumerGroupMetadata GroupMetadata() => _inner.GroupMetadata();

    /// <inheritdoc/>
    public IReadOnlyCollection<TopicPartition> Assignment() => _inner.Assignment();

    /// <inheritdoc/>
    public IReadOnlyCollection<string> Subscription() => _inner.Subscription();

    /// <inheritdoc/>
    public IReadOnlyCollection<TopicPartition> Paused() => _inner.Paused();

    /// <inheritdoc/>
    public void EnforceRebalance(string? reason = null) => _inner.EnforceRebalance(reason);

    /// <inheritdoc/>
    public void CommitAsync()
    {
        _script.OnCall("CommitAsync");
        _inner.CommitAsync();
    }

    /// <inheritdoc/>
    public void CommitAsync(IOffsetCommitCallback callback)
    {
        _script.OnCall("CommitAsync");
        _inner.CommitAsync(callback);
    }

    /// <inheritdoc/>
    public void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, IOffsetCommitCallback? callback = null)
    {
        _script.OnCall("CommitAsync(offsets)");
        _inner.CommitAsync(offsets, callback);
    }

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, long offset) => _inner.Seek(partition, offset);

    /// <inheritdoc/>
    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) => _inner.Seek(partition, offsetAndMetadata);

    /// <inheritdoc/>
    public long? CurrentLag(TopicPartition partition) => _inner.CurrentLag(partition);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _inner.Metrics();

    /// <inheritdoc/>
    public string ClientId() => _inner.ClientId();

    /// <inheritdoc/>
    public ConsumerHandle Handle()
    {
        ConsumerHandle handle = _inner.Handle();
        _script.Handle = handle;
        return handle;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _script.OnCall("Dispose");
        _inner.Dispose();
    }

    void IMockConsumerOperations.UpdateBeginningOffset(string topic, int partition, long offset) =>
        _inner.UpdateBeginningOffset(topic, partition, offset);

    void IMockConsumerOperations.Rebalance(IReadOnlyCollection<TopicPartition> partitions) => _inner.Rebalance(partitions);

    void IMockConsumerOperations.AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        _inner.AddRecord(topic, partition, offset, key, value);

    void IMockConsumerOperations.SetPollError(string message) => _inner.SetPollError(message);

    private bool IsHandleUsable()
    {
        try
        {
            _script.Handle?.Assignment();
            return _script.Handle is not null;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }
}
