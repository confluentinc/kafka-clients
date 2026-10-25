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
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// The sync flavour's consumer behind the factory seam: a <see cref="MockConsumer{TKey, TValue}"/> (auto
/// offset reset <c>earliest</c>, as the consumer servicer's mock) that applies the script's steps on the
/// loop thread at the start of each poll and records the servicer's calls; every call reaches the real mock.
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

    public ConsumerRecords<byte[], byte[]> Poll(TimeSpan timeout)
    {
        _script.ApplyPendingAsync(this).GetAwaiter().GetResult();
        ConsumerRecords<byte[], byte[]> records = _inner.Poll(timeout);
        if (records.Count == 0)
        {
            // The mock returns at once when it has nothing, where a real consumer waits out the
            // timeout: waiting here keeps an idle loop from spinning, without changing what any
            // poll returns.
            Thread.Sleep(timeout);
        }

        return records;
    }

    public void Subscribe(IReadOnlyCollection<string> topics) => _inner.Subscribe(topics);

    public void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener listener) =>
        _inner.Subscribe(topics, listener);

    public void Unsubscribe() => _inner.Unsubscribe();

    public void Assign(IReadOnlyCollection<TopicPartition> partitions) => _inner.Assign(partitions);

    public void Pause(IReadOnlyCollection<TopicPartition> partitions) => _inner.Pause(partitions);

    public void Resume(IReadOnlyCollection<TopicPartition> partitions) => _inner.Resume(partitions);

    public void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions) => _inner.SeekToBeginning(partitions);

    public void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions) => _inner.SeekToEnd(partitions);

    public long Position(TopicPartition partition) => _inner.Position(partition);

    public long Position(TopicPartition partition, TimeSpan timeout) => _inner.Position(partition, timeout);

    public void Commit()
    {
        _script.OnCall("Commit");
        _inner.Commit();
    }

    public void Commit(TimeSpan timeout)
    {
        _script.OnCall("Commit(timeout)");
        _inner.Commit(timeout);
    }

    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        _script.OnCall("Commit(offsets)");
        _inner.Commit(offsets);
    }

    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, TimeSpan timeout)
    {
        _script.OnCall("Commit(offsets, timeout)");
        _inner.Commit(offsets, timeout);
    }

    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(IReadOnlyCollection<TopicPartition> partitions)
    {
        _script.OnCall("Committed");
        return _inner.Committed(partitions);
    }

    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout)
    {
        _script.OnCall("Committed(timeout)");
        return _inner.Committed(partitions, timeout);
    }

    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch) => _inner.OffsetsForTimes(timestampsToSearch);

    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, TimeSpan timeout) =>
        _inner.OffsetsForTimes(timestampsToSearch, timeout);

    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions) =>
        _inner.BeginningOffsets(partitions);

    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => _inner.BeginningOffsets(partitions, timeout);

    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions) =>
        _inner.EndOffsets(partitions);

    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => _inner.EndOffsets(partitions, timeout);

    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _inner.PartitionsFor(topic);

    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic, TimeSpan timeout) =>
        _inner.PartitionsFor(topic, timeout);

    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics() => _inner.ListTopics();

    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(TimeSpan timeout) =>
        _inner.ListTopics(timeout);

    public void Close()
    {
        _script.OnCall("Close");
        _inner.Close();
    }

    public void Close(CloseOptions option)
    {
        _script.OnCall("Close(option)");
        _inner.Close(option);
    }

    public void Wakeup() => _inner.Wakeup();

    public ConsumerGroupMetadata GroupMetadata() => _inner.GroupMetadata();

    public IReadOnlyCollection<TopicPartition> Assignment() => _inner.Assignment();

    public IReadOnlyCollection<string> Subscription() => _inner.Subscription();

    public IReadOnlyCollection<TopicPartition> Paused() => _inner.Paused();

    public void EnforceRebalance(string? reason = null) => _inner.EnforceRebalance(reason);

    public void CommitAsync()
    {
        _script.OnCall("CommitAsync");
        _inner.CommitAsync();
    }

    public void CommitAsync(IOffsetCommitCallback callback)
    {
        _script.OnCall("CommitAsync");
        _inner.CommitAsync(callback);
    }

    public void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, IOffsetCommitCallback? callback = null)
    {
        _script.OnCall("CommitAsync(offsets)");
        _inner.CommitAsync(offsets, callback);
    }

    public void Seek(TopicPartition partition, long offset) => _inner.Seek(partition, offset);

    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) => _inner.Seek(partition, offsetAndMetadata);

    public long? CurrentLag(TopicPartition partition) => _inner.CurrentLag(partition);

    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _inner.Metrics();

    public ConsumerHandle Handle()
    {
        ConsumerHandle handle = _inner.Handle();
        _script.Handle = handle;
        return handle;
    }

    public void Dispose()
    {
        _script.OnCall("Dispose");
        _inner.Dispose();
    }

    void IMockConsumerOperations.UpdateBeginningOffset(string topic, int partition, long offset) =>
        _inner.UpdateBeginningOffset(topic, partition, offset);

    Task IMockConsumerOperations.Rebalance(IReadOnlyCollection<TopicPartition> partitions)
    {
        _inner.Rebalance(partitions);
        return Task.CompletedTask;
    }

    void IMockConsumerOperations.AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        _inner.AddRecord(topic, partition, offset, key, value);

    void IMockConsumerOperations.SetPollException(KafkaException? exception) => _inner.SetPollException(exception);
}
