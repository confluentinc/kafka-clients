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
using System.Threading.Channels;
using System.Threading.Tasks;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// <c>workload_id → workload</c>, shared by the <c>Run*</c>, <c>StopWorkload</c> and
/// <c>MarkWorkload</c> RPCs of one servicer — the .NET twin of Python's <c>_Registry</c>
/// (<c>python/grpc_chaos.py</c>) (M18/P1 §5.2).
/// </summary>
/// <remarks>
/// Test-server scaffolding with no Java class (DoD §7). One difference from Python, which
/// removes by id: <see cref="Remove"/> removes an entry only if it is still the one the caller
/// added, so a stream that ends after its id was re-registered cannot remove its successor.
/// </remarks>
internal sealed class ChaosRegistry
{
    private readonly object _lock = new object();
    private readonly Dictionary<string, ChaosWorkload> _workloads = new Dictionary<string, ChaosWorkload>(StringComparer.Ordinal);

    /// <summary>Registers <paramref name="workload"/>; <see langword="false"/> when its id is taken.</summary>
    internal bool TryAdd(ChaosWorkload workload)
    {
        lock (_lock)
        {
            return _workloads.TryAdd(workload.Id, workload);
        }
    }

    /// <summary>The running workload registered as <paramref name="workloadId"/>, or <see langword="null"/>.</summary>
    internal ChaosWorkload? Get(string workloadId)
    {
        lock (_lock)
        {
            return _workloads.TryGetValue(workloadId, out ChaosWorkload? workload) ? workload : null;
        }
    }

    /// <summary>
    /// Asks the workload to stop and drain (Python <c>_Registry.stop</c>). Returns at once; an
    /// unknown id is a no-op (the harness may race the stream's end).
    /// </summary>
    internal void Stop(string workloadId) => Get(workloadId)?.RequestStop();

    /// <summary>
    /// Queues a <c>Marker</c> behind every event the workload queued so far (Python
    /// <c>_Registry.mark</c>); whether it was running.
    /// </summary>
    internal bool Mark(string workloadId, ulong marker)
    {
        ChaosWorkload? workload = Get(workloadId);
        if (workload is null)
        {
            return false;
        }

        workload.Emit(ChaosEvents.Marker(marker));
        return true;
    }

    /// <summary>Removes <paramref name="workload"/>, but only while it is the registered one.</summary>
    internal void Remove(ChaosWorkload workload)
    {
        lock (_lock)
        {
            if (_workloads.TryGetValue(workload.Id, out ChaosWorkload? current) && ReferenceEquals(current, workload))
            {
                _workloads.Remove(workload.Id);
            }
        }
    }

    /// <summary>
    /// Asks every registered workload to stop and returns them, so a shutdown can wait for their
    /// drains (§5.8).
    /// </summary>
    internal IReadOnlyList<ChaosWorkload> StopAll()
    {
        List<ChaosWorkload> snapshot;
        lock (_lock)
        {
            snapshot = new List<ChaosWorkload>(_workloads.Values);
        }

        foreach (ChaosWorkload workload in snapshot)
        {
            workload.RequestStop();
        }

        return snapshot;
    }
}

/// <summary>
/// One running workload: its id, its event channel (the stream's FIFO, PLAN §5.2), its stop
/// signal (§5.3 item 7) and the completion of its loop — Python's <c>(stop, events.put)</c>
/// registry pair plus the worker it starts.
/// </summary>
/// <remarks>
/// <para>
/// <b>Many writers, one reader.</b> The workload's own thread or task, the producer's pump
/// thread (delivery callbacks), the consumer's dispatcher thread (listener and commit
/// callbacks) and the <c>MarkWorkload</c> handler all <see cref="Emit"/>; only the RPC handler
/// reads. <see cref="ChannelWriter{T}.TryWrite"/> on an unbounded channel is linearizable and
/// never blocks, so the stream order is the order the client observed events in (the proto's
/// ordering rule), and no binding thread waits on the harness. Unbounded like Python's
/// <c>SimpleQueue</c> (R13). <c>AllowSynchronousContinuations = false</c> is load-bearing: it
/// keeps the reader's continuation — the gRPC write — off the pump and dispatcher threads, the
/// hazard <c>RunContinuationsAsynchronously</c> guards in the binding (ffi §A7 / §B7).
/// </para>
/// <para>
/// <b>The stop signal never resumes a waiter inline</b> on the thread that requests the stop
/// (the <c>StopWorkload</c> handler or the RPC-cancellation callback, §5.3 item 7). A sync loop
/// waits on a <see cref="ManualResetEventSlim"/>, whose <c>Set</c> only wakes a waiting thread;
/// an async loop awaits <see cref="StopRequested"/>, a <see cref="TaskCompletionSource"/> built
/// with <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>. The event is never
/// disposed: <see cref="RequestStop"/> can race the stream's end from a handler that looked the
/// workload up a moment earlier, and an undisposed <see cref="ManualResetEventSlim"/> whose
/// wait handle was never materialized holds nothing native.
/// </para>
/// </remarks>
internal sealed class ChaosWorkload
{
    private readonly Channel<Proto.WorkloadEvent> _events = Channel.CreateUnbounded<Proto.WorkloadEvent>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

    private readonly ManualResetEventSlim _stopHandle = new ManualResetEventSlim(false);
    private readonly TaskCompletionSource _stopRequested =
        new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    private Task _completion = Task.CompletedTask;

    internal ChaosWorkload(string workloadId)
    {
        Id = workloadId;
    }

    /// <summary>The harness-chosen <c>workload_id</c>.</summary>
    internal string Id { get; }

    /// <summary>The stream's read side (single reader: the RPC handler).</summary>
    internal ChannelReader<Proto.WorkloadEvent> Events => _events.Reader;

    /// <summary>Whether a stop was requested (Python <c>stop.is_set()</c>).</summary>
    internal bool IsStopRequested => _stopHandle.IsSet;

    /// <summary>Completes, asynchronously, once a stop is requested (the async loops' wait).</summary>
    internal Task StopRequested => _stopRequested.Task;

    /// <summary>
    /// The loop's completion: finished once the client is closed and the terminal event queued.
    /// Never faults (the loop is guarded, <see cref="ChaosStream"/>). Set by the RPC handler right
    /// after it starts the loop, before anything can wait on it.
    /// </summary>
    internal Task Completion
    {
        get => Volatile.Read(ref _completion);
        set => Volatile.Write(ref _completion, value);
    }

    /// <summary>
    /// Queues <paramref name="workloadEvent"/> on the stream, FIFO behind everything queued
    /// before (Python <c>events.put</c>). Never blocks and never throws.
    /// </summary>
    internal void Emit(Proto.WorkloadEvent workloadEvent) => _events.Writer.TryWrite(workloadEvent);

    /// <summary>
    /// Asks the loop to stop and drain. Idempotent, returns at once, and never runs a waiter's
    /// continuation on the calling thread (see the type remarks).
    /// </summary>
    internal void RequestStop()
    {
        _stopHandle.Set();
        _stopRequested.TrySetResult();
    }

    /// <summary>
    /// Blocks the calling (dedicated workload) thread for up to <paramref name="timeout"/>,
    /// returning early on a stop (Python <c>stop.wait(timeout)</c>).
    /// </summary>
    /// <returns>Whether a stop was requested.</returns>
    internal bool WaitForStop(TimeSpan timeout) => _stopHandle.Wait(timeout);
}
