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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// A <see cref="MockConsumer{TKey, TValue}"/> with what Python's tests use and the .NET mock lacks: poll tasks
/// (Java's <c>MockConsumer.schedulePollTask</c>, one run at the start of each poll), commits that throw the errors
/// given, one per call, the committed offsets, subscription and options at close, and a count of wakeups.
/// </summary>
internal sealed class ScriptedConsumer : IConsumer<string, string>
{
    private readonly Queue<Action> _pollTasks = new Queue<Action>();

    private readonly Queue<Exception> _commitErrors;

    private int _wakeups;

    internal ScriptedConsumer(params Exception[] commitErrors)
    {
        Mock = new MockConsumer<string, string>(new StringDeserializer(), new StringDeserializer(), "earliest");
        _commitErrors = new Queue<Exception>(commitErrors);
    }

    internal MockConsumer<string, string> Mock { get; }

    internal List<Dictionary<TopicPartition, OffsetAndMetadata>> Commits { get; } =
        new List<Dictionary<TopicPartition, OffsetAndMetadata>>();

    internal List<CloseOptions> CloseOptionsGiven { get; } = new List<CloseOptions>();

    internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>? CommittedAtClose { get; private set; }

    internal IReadOnlyCollection<string>? SubscriptionAtClose { get; private set; }

    internal int Wakeups => Volatile.Read(ref _wakeups);

    internal void SchedulePollTask(Action task) => _pollTasks.Enqueue(task);

    public ConsumerRecords<string, string> Poll(TimeSpan timeout)
    {
        if (_pollTasks.TryDequeue(out Action? task))
        {
            task();
        }

        return Mock.Poll(timeout);
    }

    public void Subscribe(IReadOnlyCollection<string> topics) => Mock.Subscribe(topics);

    public void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener callback) =>
        Mock.Subscribe(topics, callback);

    public void Subscribe(SubscriptionPattern pattern) => Mock.Subscribe(pattern);

    public void Subscribe(SubscriptionPattern pattern, IConsumerRebalanceListener callback) =>
        Mock.Subscribe(pattern, callback);

    public void Unsubscribe() => Mock.Unsubscribe();

    public void Assign(IReadOnlyCollection<TopicPartition> partitions) => Mock.Assign(partitions);

    public void Pause(IReadOnlyCollection<TopicPartition> partitions) => Mock.Pause(partitions);

    public void Resume(IReadOnlyCollection<TopicPartition> partitions) => Mock.Resume(partitions);

    public void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions) => Mock.SeekToBeginning(partitions);

    public void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions) => Mock.SeekToEnd(partitions);

    public long Position(TopicPartition partition) => Mock.Position(partition);

    public long Position(TopicPartition partition, TimeSpan timeout) => Mock.Position(partition, timeout);

    public void Commit() => Mock.Commit();

    public void Commit(TimeSpan timeout) => Mock.Commit(timeout);

    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        Commits.Add(new Dictionary<TopicPartition, OffsetAndMetadata>(offsets));
        if (_commitErrors.TryDequeue(out Exception? error))
        {
            throw error;
        }

        Mock.Commit(offsets);
    }

    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, TimeSpan timeout) =>
        Mock.Commit(offsets, timeout);

    public void CommitAsync() => Mock.CommitAsync();

    public void CommitAsync(IOffsetCommitCallback callback) => Mock.CommitAsync(callback);

    public void CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback) =>
        Mock.CommitAsync(offsets, callback);

    public void Seek(TopicPartition partition, long offset) => Mock.Seek(partition, offset);

    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) =>
        Mock.Seek(partition, offsetAndMetadata);

    public long? CurrentLag(TopicPartition topicPartition) => Mock.CurrentLag(topicPartition);

    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        IReadOnlyCollection<TopicPartition> partitions) => Mock.Committed(partitions);

    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => Mock.Committed(partitions, timeout);

    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch) => Mock.OffsetsForTimes(timestampsToSearch);

    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, TimeSpan timeout) =>
        Mock.OffsetsForTimes(timestampsToSearch, timeout);

    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions) =>
        Mock.BeginningOffsets(partitions);

    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => Mock.BeginningOffsets(partitions, timeout);

    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions) =>
        Mock.EndOffsets(partitions);

    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout) => Mock.EndOffsets(partitions, timeout);

    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => Mock.PartitionsFor(topic);

    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic, TimeSpan timeout) =>
        Mock.PartitionsFor(topic, timeout);

    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics() => Mock.ListTopics();

    public IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(TimeSpan timeout) =>
        Mock.ListTopics(timeout);

    public void Close() => Mock.Close();

    public void Close(CloseOptions option)
    {
        CloseOptionsGiven.Add(option);
        CommittedAtClose = Mock.Committed(Mock.Assignment());
        SubscriptionAtClose = Mock.Subscription();
        Mock.Close(option);
    }

    public void Wakeup()
    {
        Interlocked.Increment(ref _wakeups);
        Mock.Wakeup();
    }

    public ConsumerGroupMetadata GroupMetadata() => Mock.GroupMetadata();

    public IReadOnlyCollection<TopicPartition> Assignment() => Mock.Assignment();

    public IReadOnlyCollection<string> Subscription() => Mock.Subscription();

    public IReadOnlyCollection<TopicPartition> Paused() => Mock.Paused();

    public void EnforceRebalance() => Mock.EnforceRebalance();

    public void EnforceRebalance(string? reason) => Mock.EnforceRebalance(reason);

    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => Mock.Metrics();

    public void Dispose() => Mock.Dispose();
}
