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
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// The rebalance listener of a sync chaos consumer, Python's <c>_ChaosRebalanceListener</c>: it reports
/// each callback; on a revoke it commits and, unless <see cref="Closing"/>, reads the commit back, both
/// calls routed by the binding to the consumer's <see cref="ConsumerHandle"/>. Failures are reported.
/// </summary>
internal sealed class ChaosRebalanceListener : IConsumerRebalanceListener
{
    private readonly Action<Proto.WorkloadEvent> _emit;
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly bool _checkCommits;
    private volatile bool _closing;

    internal ChaosRebalanceListener(Action<Proto.WorkloadEvent> emit, IConsumer<byte[], byte[]> consumer, bool checkCommits)
    {
        _emit = emit;
        _consumer = consumer;
        _checkCommits = checkCommits;
    }

    // Set just before the workload closes the consumer: once closing, the client only drains
    // commits, so a revoke's read-back would wait out the API timeout; the workload's own read-back
    // covers that state.
    internal bool Closing
    {
        get => _closing;
        set => _closing = value;
    }

    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        EmitRebalance(_emit, Proto.RebalanceKind.Assigned, partitions, observedAt);
    }

    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        EmitRebalance(_emit, Proto.RebalanceKind.Revoked, partitions, observedAt);
        try
        {
            _consumer.Commit();
        }
        catch (Exception e)
        {
            _emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.RevokeCommit, e));
            return;
        }

        if (_checkCommits && !_closing)
        {
            try
            {
                ChaosEvents.EmitCommitted(_consumer.Committed(partitions), _emit);
            }
            catch (Exception e)
            {
                _emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.ReadCommitted, e));
            }
        }
    }

    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        EmitRebalance(_emit, Proto.RebalanceKind.Lost, partitions, observedAt);
    }

    // A failure to build the event, a server-side fault, is reported as Failed rather than thrown
    // into the binding, which would fail the rebalance.
    internal static void EmitRebalance(
        Action<Proto.WorkloadEvent> emit,
        Proto.RebalanceKind kind,
        IReadOnlyCollection<TopicPartition> partitions,
        long observedAt)
    {
        Proto.WorkloadEvent rebalance;
        try
        {
            rebalance = ChaosEvents.Rebalance(kind, partitions, observedAt);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos consumer: building a {kind} rebalance event failed: {e}");
            rebalance = ChaosEvents.Failed(e);
        }

        emit(rebalance);
    }
}

/// <summary>
/// The <c>CommitAsync</c> callback of a chaos consumer, Python's <c>_commit_callback</c>: it reports a
/// failed commit, which nobody would otherwise see, since <c>CommitAsync</c> only initiates the commit.
/// </summary>
internal sealed class ChaosCommitCallback : IOffsetCommitCallback
{
    private readonly Action<Proto.WorkloadEvent> _emit;

    internal ChaosCommitCallback(Action<Proto.WorkloadEvent> emit)
    {
        _emit = emit;
    }

    public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
    {
        if (exception is not null)
        {
            _emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Commit, exception));
        }
    }
}
