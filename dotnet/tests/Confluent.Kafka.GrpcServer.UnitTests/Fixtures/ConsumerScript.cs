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

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// Drives the consumer behind the factory seam, stated once for every flavour: the test queues
/// mock steps here and each flavour's double (<see cref="SyncScriptedConsumer"/>, and from S3 the
/// async one) applies them <b>on the workload's own loop</b>, at the start of its next poll.
/// </summary>
/// <remarks>
/// The consumer is single-owner: the core rejects a <c>Rebalance</c> / <c>AddRecord</c> issued
/// from the test thread while the loop is inside <c>Poll</c> (ConcurrentModification). Applying
/// the steps from inside the loop's own call keeps every mock call on the owning thread, exactly
/// as a broker-driven rebalance arrives during a real poll. The script also records what the
/// double observed, for the close-sequence and cancellation tests.
/// </remarks>
internal sealed class ConsumerScript
{
    private readonly ConcurrentQueue<Step> _steps = new ConcurrentQueue<Step>();
    private readonly ConcurrentQueue<Exception> _stepFailures = new ConcurrentQueue<Exception>();
    private readonly List<string> _calls = new List<string>();
    private int _polls;

    /// <summary>The config the factory was called with.</summary>
    internal IReadOnlyDictionary<string, string>? Config { get; set; }

    /// <summary>The reentrancy handle the double handed the servicer.</summary>
    internal ConsumerHandle? Handle { get; set; }

    /// <summary>Whether that handle still worked when <c>Close</c> was entered (null = not closed yet).</summary>
    internal bool? HandleUsableAtClose { get; set; }

    /// <summary>How many polls the loop has made.</summary>
    internal int Polls => Volatile.Read(ref _polls);

    /// <summary>Steps that threw when applied; a test asserts this stays empty.</summary>
    internal IReadOnlyList<Exception> StepFailures => _stepFailures.ToList();

    /// <summary>The servicer-facing calls the double saw, in order (Commit, CommitAsync, Committed, Close, Dispose).</summary>
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

    /// <summary>Queues a mock rebalance onto <paramref name="partitions"/> (seeding each beginning offset to 0).</summary>
    internal void Rebalance(params TopicPartition[] partitions) => _steps.Enqueue(new Step(StepKind.Rebalance, partitions, null));

    /// <summary>Queues a record onto an assigned partition.</summary>
    internal void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value) =>
        _steps.Enqueue(new Step(StepKind.AddRecord, null, new MockRecord(topic, partition, offset, key, value)));

    /// <summary>Queues a poll error: the next poll throws it.</summary>
    internal void SetPollError(string message) =>
        _steps.Enqueue(new Step(StepKind.PollError, null, new MockRecord(message, 0, 0, null, null)));

    /// <summary>Applies every queued step through the flavour's mock operations; called by the double inside its poll.</summary>
    internal void ApplyPending(IMockConsumerOperations mock)
    {
        Interlocked.Increment(ref _polls);
        while (_steps.TryDequeue(out Step? step))
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

                        mock.Rebalance(step.Partitions!);
                        break;
                    case StepKind.AddRecord:
                        MockRecord record = step.Record!;
                        mock.AddRecord(record.Topic, record.Partition, record.Offset, record.Key, record.Value);
                        break;
                    case StepKind.PollError:
                        mock.SetPollError(step.Record!.Topic);
                        break;
                }
            }
            catch (Exception e)
            {
                _stepFailures.Enqueue(e);
            }
        }
    }

    /// <summary>Records a servicer-facing call.</summary>
    internal void OnCall(string name)
    {
        lock (_calls)
        {
            _calls.Add(name);
        }
    }

    private enum StepKind
    {
        Rebalance,
        AddRecord,
        PollError,
    }

    private sealed record Step(StepKind Kind, TopicPartition[]? Partitions, MockRecord? Record);

    private sealed record MockRecord(string Topic, int Partition, long Offset, byte[]? Key, byte[]? Value);
}

/// <summary>The mock-only helpers a script step needs, common to both flavours' mock consumers.</summary>
internal interface IMockConsumerOperations
{
    void UpdateBeginningOffset(string topic, int partition, long offset);

    void Rebalance(IReadOnlyCollection<TopicPartition> partitions);

    void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value);

    void SetPollError(string message);
}
