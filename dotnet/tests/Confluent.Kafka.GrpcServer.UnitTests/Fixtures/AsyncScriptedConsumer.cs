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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// The async flavour's consumer behind the factory seam: an <see cref="AsyncMockConsumer{TKey, TValue}"/>
/// (auto offset reset <c>earliest</c>, as the consumer servicer's mock) that applies the script's steps at
/// the start of each poll and records the servicer's calls and tokens; every call reaches the real mock.
/// </summary>
internal sealed class AsyncScriptedConsumer : IAsyncConsumer<byte[], byte[]>, IMockConsumerOperations
{
    private readonly AsyncMockConsumer<byte[], byte[]> _inner =
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");

    private readonly ConsumerScript _script;

    internal AsyncScriptedConsumer(ConsumerScript script)
    {
        _script = script;
    }

    public async Task<ConsumerRecords<byte[], byte[]>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Poll), cancellationToken);

        // Before the inner poll: the servicer awaits each operation before it starts the next, so no
        // other operation of this consumer is in flight while a step runs.
        await _script.ApplyPendingAsync(this).ConfigureAwait(false);
        ConsumerRecords<byte[], byte[]> records = await _inner.Poll(timeout, cancellationToken).ConfigureAwait(false);
        if (records.Count == 0)
        {
            // The mock returns at once when it has nothing, where a real consumer waits out the
            // timeout: waiting here keeps an idle loop from spinning, without changing what any
            // poll returns.
            await Task.Delay(timeout, CancellationToken.None).ConfigureAwait(false);
        }

        return records;
    }

    public Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Subscribe), cancellationToken);
        return _inner.Subscribe(topics, cancellationToken);
    }

    public Task Subscribe(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Subscribe), cancellationToken);
        return _inner.Subscribe(topics, listener, cancellationToken);
    }

    public Task Subscribe(
        IReadOnlyCollection<string> topics,
        IAsyncConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Subscribe), cancellationToken);
        return _inner.Subscribe(topics, listener, cancellationToken);
    }

    public Task Unsubscribe(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Unsubscribe), cancellationToken);
        return _inner.Unsubscribe(cancellationToken);
    }

    public Task Assign(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Assign), cancellationToken);
        return _inner.Assign(partitions, cancellationToken);
    }

    public Task Pause(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Pause), cancellationToken);
        return _inner.Pause(partitions, cancellationToken);
    }

    public Task Resume(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Resume), cancellationToken);
        return _inner.Resume(partitions, cancellationToken);
    }

    public Task SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(SeekToBeginning), cancellationToken);
        return _inner.SeekToBeginning(partitions, cancellationToken);
    }

    public Task SeekToEnd(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(SeekToEnd), cancellationToken);
        return _inner.SeekToEnd(partitions, cancellationToken);
    }

    public Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Position), cancellationToken);
        return _inner.Position(partition, cancellationToken);
    }

    public Task<long> Position(TopicPartition partition, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Position), cancellationToken);
        return _inner.Position(partition, timeout, cancellationToken);
    }

    public Task Commit(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Commit), cancellationToken);
        _script.OnCall("Commit");
        return _inner.Commit(cancellationToken);
    }

    public Task Commit(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Commit), cancellationToken);
        _script.OnCall("Commit");
        return _inner.Commit(timeout, cancellationToken);
    }

    public Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Commit), cancellationToken);
        _script.OnCall("Commit(offsets)");
        return _inner.Commit(offsets, cancellationToken);
    }

    public Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Commit), cancellationToken);
        _script.OnCall("Commit(offsets)");
        return _inner.Commit(offsets, timeout, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Committed(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Committed), cancellationToken);
        _script.OnCall("Committed");
        return _inner.Committed(partitions, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Committed(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Committed), cancellationToken);
        _script.OnCall("Committed");
        return _inner.Committed(partitions, timeout, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(OffsetsForTimes), cancellationToken);
        return _inner.OffsetsForTimes(timestampsToSearch, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(OffsetsForTimes), cancellationToken);
        return _inner.OffsetsForTimes(timestampsToSearch, timeout, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(BeginningOffsets), cancellationToken);
        return _inner.BeginningOffsets(partitions, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(BeginningOffsets), cancellationToken);
        return _inner.BeginningOffsets(partitions, timeout, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(EndOffsets), cancellationToken);
        return _inner.EndOffsets(partitions, cancellationToken);
    }

    public Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(EndOffsets), cancellationToken);
        return _inner.EndOffsets(partitions, timeout, cancellationToken);
    }

    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(PartitionsFor), cancellationToken);
        return _inner.PartitionsFor(topic, cancellationToken);
    }

    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(
        string topic,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(PartitionsFor), cancellationToken);
        return _inner.PartitionsFor(topic, timeout, cancellationToken);
    }

    public Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(ListTopics), cancellationToken);
        return _inner.ListTopics(cancellationToken);
    }

    public Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(ListTopics), cancellationToken);
        return _inner.ListTopics(timeout, cancellationToken);
    }

    public Task Close(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Close), cancellationToken);
        _script.OnCall("Close");
        return _inner.Close(cancellationToken);
    }

    public Task Close(CloseOptions option, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Close), cancellationToken);
        _script.OnCall("Close(option)");
        return _inner.Close(option, cancellationToken);
    }

    public void Wakeup() => _inner.Wakeup();

    public ConsumerGroupMetadata GroupMetadata() => _inner.GroupMetadata();

    public IReadOnlyCollection<TopicPartition> Assignment() => _inner.Assignment();

    public IReadOnlyCollection<string> Subscription() => _inner.Subscription();

    public IReadOnlyCollection<TopicPartition> Paused() => _inner.Paused();

    public void EnforceRebalance(string? reason = null) => _inner.EnforceRebalance(reason);

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(CommitAsync), cancellationToken);
        _script.OnCall("CommitAsync");
        return _inner.CommitAsync(cancellationToken);
    }

    public Task CommitAsync(IOffsetCommitCallback callback, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(CommitAsync), cancellationToken);
        _script.OnCall("CommitAsync");
        return _inner.CommitAsync(callback, cancellationToken);
    }

    public Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback = null,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(CommitAsync), cancellationToken);
        _script.OnCall("CommitAsync(offsets)");
        return _inner.CommitAsync(offsets, callback, cancellationToken);
    }

    public Task CommitAsync(IAsyncOffsetCommitCallback callback, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(CommitAsync), cancellationToken);
        _script.OnCall("CommitAsync");
        return _inner.CommitAsync(callback, cancellationToken);
    }

    public Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IAsyncOffsetCommitCallback callback,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(CommitAsync), cancellationToken);
        _script.OnCall("CommitAsync(offsets)");
        return _inner.CommitAsync(offsets, callback, cancellationToken);
    }

    public Task Seek(TopicPartition partition, long offset, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Seek), cancellationToken);
        return _inner.Seek(partition, offset, cancellationToken);
    }

    public Task Seek(
        TopicPartition partition,
        OffsetAndMetadata offsetAndMetadata,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Seek), cancellationToken);
        return _inner.Seek(partition, offsetAndMetadata, cancellationToken);
    }

    public Task<long?> CurrentLag(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(CurrentLag), cancellationToken);
        return _inner.CurrentLag(partition, cancellationToken);
    }

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

    public ValueTask DisposeAsync()
    {
        _script.OnCall("Dispose");
        _script.DisposedAsync = true;
        return _inner.DisposeAsync();
    }

    void IMockConsumerOperations.UpdateBeginningOffset(string topic, int partition, long offset) =>
        _inner.UpdateBeginningOffset(topic, partition, offset);

    Task IMockConsumerOperations.Rebalance(IReadOnlyCollection<TopicPartition> partitions) => _inner.Rebalance(partitions);

    void IMockConsumerOperations.AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        _inner.AddRecord(topic, partition, offset, key, value);

    void IMockConsumerOperations.SetPollException(KafkaException? exception) => _inner.SetPollException(exception);
}
