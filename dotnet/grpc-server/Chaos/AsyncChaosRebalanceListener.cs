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

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// <see cref="ChaosRebalanceListener"/> for an async chaos consumer, Python's
/// <c>_AsyncChaosRebalanceListener</c>: its revoke awaits <c>Commit</c> and <c>Committed</c>, which the
/// binding routes to the consumer's <see cref="ConsumerHandle"/> inside the callback, after an await too.
/// </summary>
internal sealed class AsyncChaosRebalanceListener : IAsyncConsumerRebalanceListener
{
    private readonly Action<Proto.WorkloadEvent> _emit;
    private readonly IAsyncConsumer<byte[], byte[]> _consumer;
    private readonly bool _checkCommits;
    private volatile bool _closing;

    internal AsyncChaosRebalanceListener(Action<Proto.WorkloadEvent> emit, IAsyncConsumer<byte[], byte[]> consumer, bool checkCommits)
    {
        _emit = emit;
        _consumer = consumer;
        _checkCommits = checkCommits;
    }

    internal bool Closing
    {
        get => _closing;
        set => _closing = value;
    }

    public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        ChaosRebalanceListener.EmitRebalance(_emit, Proto.RebalanceKind.Assigned, partitions, observedAt);
        return Task.CompletedTask;
    }

    public async Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        ChaosRebalanceListener.EmitRebalance(_emit, Proto.RebalanceKind.Revoked, partitions, observedAt);
        try
        {
            await _consumer.Commit(CancellationToken.None).ConfigureAwait(false);
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
                ChaosEvents.EmitCommitted(
                    await _consumer.Committed(partitions, CancellationToken.None).ConfigureAwait(false), _emit);
            }
            catch (Exception e)
            {
                _emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.ReadCommitted, e));
            }
        }
    }

    public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        ChaosRebalanceListener.EmitRebalance(_emit, Proto.RebalanceKind.Lost, partitions, observedAt);
        return Task.CompletedTask;
    }
}
