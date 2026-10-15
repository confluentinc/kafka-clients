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

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// The rebalance listener every chaos consumer registers — the .NET twin of Python's
/// <c>_ChaosRebalanceListener</c> (<c>python/grpc_chaos.py</c>), itself the Rust harness's
/// <c>ChaosRebalanceListener</c> (<c>rust/tests/chaos/workload.rs</c>). Shared by both
/// flavours (PLAN §3.4, §5.6).
/// </summary>
/// <remarks>
/// <para>
/// It reports each callback, and in <see cref="OnPartitionsRevoked"/> commits through the
/// consumer's <see cref="ConsumerHandle"/> — the reentrant path a listener has back into its
/// consumer — then reads the commit back, unless the consumer is <see cref="Closing"/>: once
/// closing, the client only drains commits, so an offset fetch queued from here would wait out
/// the API timeout, and the workload's own read-back after its final commit already covers that
/// state.
/// </para>
/// <para>
/// <see cref="OnPartitionsLost"/> is implemented, not inherited: this type implements
/// <see cref="IConsumerRebalanceListener"/> directly (whose methods have no default), so a
/// fenced member's callback is reported as lost and never commits.
/// </para>
/// <para>
/// <b>Threading.</b> The methods are sync <c>void</c> in both flavours and run on the core's
/// callback-dispatcher thread (CLAUDE.md §4, the rebalance-listener divergence); the handle's
/// <c>Commit</c> / <c>Committed</c> are blocking calls meant for exactly that thread (ffi §B1).
/// Each body only builds events and queues them, and is a catch-all: a failed commit is
/// <b>reported, never raised</b>. A failure to build an event — a server-side fault, not the
/// client's — is logged and reported as <c>Failed</c> rather than handed to the binding (which
/// would turn it into a failed rebalance and blame the client); Python lets it propagate, but
/// no event builder here can throw on the binding's inputs, so the two agree in practice.
/// </para>
/// <para>Test-server scaffolding with no Java class (DoD §7).</para>
/// </remarks>
internal sealed class ChaosRebalanceListener : IConsumerRebalanceListener
{
    private readonly Action<Proto.WorkloadEvent> _emit;
    private readonly ConsumerHandle _handle;
    private readonly bool _checkCommits;
    private volatile bool _closing;

    /// <param name="emit">Queues an event on the workload's stream.</param>
    /// <param name="handle">The consumer's reentrancy handle; it must outlive the consumer's close.</param>
    /// <param name="checkCommits">Whether to read a revoke-time commit back (Python <c>_check_commits</c>).</param>
    internal ChaosRebalanceListener(Action<Proto.WorkloadEvent> emit, ConsumerHandle handle, bool checkCommits)
    {
        _emit = emit;
        _handle = handle;
        _checkCommits = checkCommits;
    }

    /// <summary>
    /// Set by the workload just before it closes the consumer (Python <c>listener.closing</c>);
    /// read on the dispatcher thread, hence <see langword="volatile"/>.
    /// </summary>
    internal bool Closing
    {
        get => _closing;
        set => _closing = value;
    }

    /// <inheritdoc/>
    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        EmitRebalance(Proto.RebalanceKind.Assigned, partitions, observedAt);
    }

    /// <inheritdoc/>
    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        EmitRebalance(Proto.RebalanceKind.Revoked, partitions, observedAt);
        try
        {
            _handle.Commit();
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
                ChaosEvents.EmitCommitted(_handle.Committed(partitions), _emit);
            }
            catch (Exception e)
            {
                _emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.ReadCommitted, e));
            }
        }
    }

    /// <inheritdoc/>
    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
    {
        long observedAt = ChaosEvents.UnixNanosNow();
        EmitRebalance(Proto.RebalanceKind.Lost, partitions, observedAt);
    }

    private void EmitRebalance(Proto.RebalanceKind kind, IReadOnlyCollection<TopicPartition> partitions, long observedAt)
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

        _emit(rebalance);
    }
}

/// <summary>
/// The <c>CommitAsync</c> completion callback (Python <c>_commit_callback</c>): reports a failed
/// commit, which nobody would otherwise see — the call itself only initiates the commit. Runs
/// on the core's dispatcher thread; the binding already makes it no-throw (ffi §B6), and it
/// only queues an event.
/// </summary>
internal sealed class ChaosCommitCallback : IOffsetCommitCallback
{
    private readonly Action<Proto.WorkloadEvent> _emit;

    internal ChaosCommitCallback(Action<Proto.WorkloadEvent> emit)
    {
        _emit = emit;
    }

    /// <inheritdoc/>
    public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception)
    {
        if (exception is not null)
        {
            _emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Commit, exception));
        }
    }
}
