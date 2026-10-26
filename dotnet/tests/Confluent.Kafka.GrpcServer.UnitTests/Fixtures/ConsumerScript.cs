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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// Drives the consumer behind the factory seam for every flavour: the test queues mock steps here, each
/// flavour's double applies them on the workload's own loop at the start of its next poll, as a
/// broker-driven rebalance arrives during a real poll, and the script records what the double observed.
/// </summary>
internal sealed class ConsumerScript
{
    private readonly ConcurrentQueue<Step> _steps = new ConcurrentQueue<Step>();
    private readonly ConcurrentQueue<Exception> _stepFailures = new ConcurrentQueue<Exception>();
    private readonly List<string> _calls = new List<string>();
    private int _polls;
    private bool _disposedAsync;

    internal IReadOnlyDictionary<string, string>? Config { get; set; }

    internal ConsumerHandle? Handle { get; set; }

    internal bool DisposedAsync
    {
        get => Volatile.Read(ref _disposedAsync);
        set => Volatile.Write(ref _disposedAsync, value);
    }

    internal TokenLog Tokens { get; } = new TokenLog();

    internal int Polls => Volatile.Read(ref _polls);

    internal IReadOnlyList<Exception> StepFailures => _stepFailures.ToList();

    internal IReadOnlyList<string> Calls
    {
        get
        {
            lock (_calls)
            {
                return _calls.ToList();
            }
        }
    }

    internal void Rebalance(params TopicPartition[] partitions) => _steps.Enqueue(new Step(StepKind.Rebalance, partitions, null));

    internal void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        _steps.Enqueue(new Step(StepKind.AddRecord, null, new MockRecord(topic, partition, offset, key, value)));

    internal void SetPollError(string message) =>
        _steps.Enqueue(new Step(StepKind.PollError, null, new MockRecord(message, 0, 0, null, null)));

    // Called by the double inside its poll: from the test thread a mock call would wait for the
    // running poll, or be refused while AsyncMockConsumer.Rebalance runs. Only the steps queued
    // before the call are applied, each after the one before it ended; a step queued meanwhile
    // waits for the next poll.
    internal async Task ApplyPendingAsync(IMockConsumerOperations mock)
    {
        Interlocked.Increment(ref _polls);
        int queued = _steps.Count;
        for (int i = 0; i < queued && _steps.TryDequeue(out Step? step); i++)
        {
            try
            {
                switch (step.Kind)
                {
                    case StepKind.Rebalance:
                        foreach (TopicPartition partition in step.Partitions!)
                        {
                            mock.UpdateBeginningOffset(partition.Topic, partition.Partition, 0);
                        }

                        await mock.Rebalance(step.Partitions!).ConfigureAwait(false);
                        break;
                    case StepKind.AddRecord:
                        MockRecord record = step.Record!;
                        mock.AddRecord(record.Topic, record.Partition, record.Offset, record.Key, record.Value);
                        break;
                    case StepKind.PollError:
                        mock.SetPollException(new KafkaException(step.Record!.Topic));
                        break;
                }
            }
            catch (Exception e)
            {
                _stepFailures.Enqueue(e);
            }
        }
    }

    internal void OnCall(string name)
    {
        lock (_calls)
        {
            _calls.Add(name);
        }
    }

    /// <summary>What a queued step does to the mock consumer.</summary>
    private enum StepKind
    {
        Rebalance,
        AddRecord,
        PollError,
    }

    /// <summary>One queued step: a rebalance's partitions, or the record a record or poll-error step carries.</summary>
    private sealed record Step(StepKind Kind, TopicPartition[]? Partitions, MockRecord? Record);

    /// <summary>A queued record's fields; a poll-error step carries its message in <c>Topic</c>.</summary>
    private sealed record MockRecord(string Topic, int Partition, long Offset, byte[]? Key, byte[]? Value);
}

/// <summary>The mock-only helpers a script step needs, common to both flavours' mock consumers.</summary>
internal interface IMockConsumerOperations
{
    void UpdateBeginningOffset(string topic, int partition, long offset);

    Task Rebalance(IReadOnlyCollection<TopicPartition> partitions);

    void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value);

    void SetPollException(KafkaException? exception);
}
