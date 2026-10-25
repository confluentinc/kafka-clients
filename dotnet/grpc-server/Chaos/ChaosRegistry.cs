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
/// <c>workload_id</c> to workload, shared by one servicer's <c>Run*</c>, <c>StopWorkload</c> and
/// <c>MarkWorkload</c> RPCs, the .NET twin of Python's <c>_Registry</c>.
/// </summary>
internal sealed class ChaosRegistry
{
    private readonly object _lock = new object();
    private readonly Dictionary<string, ChaosWorkload> _workloads = new Dictionary<string, ChaosWorkload>(StringComparer.Ordinal);

    internal bool TryAdd(ChaosWorkload workload)
    {
        lock (_lock)
        {
            return _workloads.TryAdd(workload.Id, workload);
        }
    }

    internal ChaosWorkload? Get(string workloadId)
    {
        lock (_lock)
        {
            return _workloads.TryGetValue(workloadId, out ChaosWorkload? workload) ? workload : null;
        }
    }

    internal void Stop(string workloadId) => Get(workloadId)?.RequestStop();

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

    // Unlike Python, which removes by id: only while it is the registered one, so a stream that
    // ends after its id was re-registered cannot remove its successor.
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
/// One running workload: its id, its event channel, its stop signal and its loop's completion, Python's
/// <c>(stop, events.put)</c> registry pair plus the worker it starts.
/// </summary>
internal sealed class ChaosWorkload
{
    // Many writers (the loop, the clients' callback threads, MarkWorkload), one reader (the RPC
    // handler). TryWrite never blocks, so no client thread waits on the harness, and no synchronous
    // continuation runs the reader's gRPC write on a writer's thread.
    private readonly Channel<Proto.WorkloadEvent> _events = Channel.CreateUnbounded<Proto.WorkloadEvent>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

    // Neither stop signal resumes a waiter on the thread that requests the stop. The event is never
    // disposed: RequestStop can race the stream's end, and an event whose wait handle was never
    // materialized holds nothing native.
    private readonly ManualResetEventSlim _stopHandle = new ManualResetEventSlim(false);
    private readonly TaskCompletionSource _stopRequested =
        new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    private Task _completion = Task.CompletedTask;

    internal ChaosWorkload(string workloadId)
    {
        Id = workloadId;
    }

    internal string Id { get; }

    internal ChannelReader<Proto.WorkloadEvent> Events => _events.Reader;

    internal bool IsStopRequested => _stopHandle.IsSet;

    internal Task StopRequested => _stopRequested.Task;

    // Set by the RPC handler right after it starts the loop, before anything waits on it; it never
    // faults.
    internal Task Completion
    {
        get => Volatile.Read(ref _completion);
        set => Volatile.Write(ref _completion, value);
    }

    internal void Emit(Proto.WorkloadEvent workloadEvent) => _events.Writer.TryWrite(workloadEvent);

    internal void RequestStop()
    {
        _stopHandle.Set();
        _stopRequested.TrySetResult();
    }

    internal bool WaitForStop(TimeSpan timeout) => _stopHandle.Wait(timeout);

    // Not Task.Delay over a token RequestStop cancels: a cancellation runs its callbacks inline on
    // the cancelling thread. A delay that loses to the stop expires on its own, costing at most one
    // interval.
    internal Task WaitForStopAsync(TimeSpan timeout) =>
        IsStopRequested ? Task.CompletedTask : Task.WhenAny(Task.Delay(timeout), StopRequested);
}
