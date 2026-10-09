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
/// The async flavour's consumer behind the factory seam: an
/// <see cref="AsyncMockConsumer{TKey, TValue}"/> (auto offset reset <c>earliest</c>, like the gRPC
/// consumer servicer's mock) that applies the <see cref="ConsumerScript"/>'s queued steps at the
/// start of each poll, records the calls the servicer makes under the same names the sync double
/// uses, and records every token the servicer passes (T12). Every call reaches the real mock
/// (DoD §12).
/// </summary>
/// <remarks>
/// The steps are applied before the inner poll is submitted, so no operation of this consumer is
/// in flight while they run: the servicer awaits each operation before it starts the next, and the
/// core releases its access guard before it completes the previous one. That is the async form of
/// the single-owner rule the script documents.
/// </remarks>
internal sealed class AsyncScriptedConsumer : IAsyncConsumer<byte[], byte[]>, IMockConsumerOperations
{
    private readonly AsyncMockConsumer<byte[], byte[]> _inner =
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");

    private readonly ConsumerScript _script;

    internal AsyncScriptedConsumer(ConsumerScript script)
    {
        _script = script;
    }

    /// <inheritdoc/>
    public async Task<ConsumerRecords<byte[], byte[]>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Poll), cancellationToken);
        _script.ApplyPending(this);
        ConsumerRecords<byte[], byte[]> records = await _inner.Poll(timeout, cancellationToken).ConfigureAwait(false);
        if (records.Count == 0)
        {
            // The mock returns at once when it has nothing (rust/src/consumer/mock_consumer.rs
            // poll never waits); a real consumer waits out the timeout first. Waiting here keeps
            // an idle workload's loop from spinning, without changing what any poll returns.
            await Task.Delay(timeout, CancellationToken.None).ConfigureAwait(false);
        }

        return records;
    }

    /// <inheritdoc/>
    public Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Subscribe), cancellationToken);
        return _inner.Subscribe(topics, cancellationToken);
    }

    /// <inheritdoc/>
    public Task Subscribe(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Subscribe), cancellationToken);
        return _inner.Subscribe(topics, listener, cancellationToken);
    }

    /// <inheritdoc/>
    public Task Unsubscribe(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Unsubscribe), cancellationToken);
        return _inner.Unsubscribe(cancellationToken);
    }

    /// <inheritdoc/>
    public Task Assign(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Assign), cancellationToken);
        return _inner.Assign(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task Pause(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Pause), cancellationToken);
        return _inner.Pause(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task Resume(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Resume), cancellationToken);
        return _inner.Resume(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(SeekToBeginning), cancellationToken);
        return _inner.SeekToBeginning(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task SeekToEnd(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(SeekToEnd), cancellationToken);
        return _inner.SeekToEnd(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Position), cancellationToken);
        return _inner.Position(partition, cancellationToken);
    }

    /// <inheritdoc/>
    public Task Commit(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Commit), cancellationToken);
        _script.OnCall("Commit");
        return _inner.Commit(cancellationToken);
    }

    /// <inheritdoc/>
    public Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Commit), cancellationToken);
        _script.OnCall("Commit(offsets)");
        return _inner.Commit(offsets, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> Committed(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Committed), cancellationToken);
        _script.OnCall("Committed");
        return _inner.Committed(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(OffsetsForTimes), cancellationToken);
        return _inner.OffsetsForTimes(timestampsToSearch, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(BeginningOffsets), cancellationToken);
        return _inner.BeginningOffsets(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(EndOffsets), cancellationToken);
        return _inner.EndOffsets(partitions, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(PartitionsFor), cancellationToken);
        return _inner.PartitionsFor(topic, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(ListTopics), cancellationToken);
        return _inner.ListTopics(cancellationToken);
    }

    /// <inheritdoc/>
    public Task Close(CancellationToken cancellationToken = default)
    {
        _script.Tokens.Record(nameof(Close), cancellationToken);
        _script.OnCall("Close");
        _script.HandleUsableAtClose = IsHandleUsable();
        return _inner.Close(cancellationToken);
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

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        _script.OnCall("Dispose");
        _script.DisposedAsync = true;
        return _inner.DisposeAsync();
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
