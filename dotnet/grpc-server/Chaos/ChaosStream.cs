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

using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// The body of every <c>RunProducer</c> / <c>RunConsumer</c> RPC in both flavours, Python's
/// <c>ChaosWorkloadService._run</c> / <c>_guarded</c>: register, send the headers, stream the events in
/// batches until a terminal one, then stop, deregister and await the loop. Only it writes to the stream.
/// </summary>
internal static class ChaosStream
{
    internal static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    internal static async Task Run(
        ChaosRegistry registry,
        string workloadId,
        IServerStreamWriter<Proto.WorkloadEventBatch> responseStream,
        ServerCallContext context,
        Func<ChaosWorkload, Task> start)
    {
        ChaosWorkload workload = new ChaosWorkload(workloadId);
        if (!registry.TryAdd(workload))
        {
            await responseStream.WriteAsync(ChaosEvents.DuplicateId(workloadId)).ConfigureAwait(false);
            return;
        }

        CancellationToken cancellation = context.CancellationToken;
        Task? completion = null;
        try
        {
            // Headers now, not with the first event, so the harness's call resolves even for a
            // consumer that never gets an assignment.
            await context.WriteResponseHeadersAsync(new Metadata()).ConfigureAwait(false);

            // A cancelled RPC stops the workload, which then drains and closes its client.
            // RequestStop never runs a waiter inline.
            using CancellationTokenRegistration onCancel =
                cancellation.Register(static state => ((ChaosWorkload)state!).RequestStop(), workload);

            completion = Guarded(workload, start);
            workload.Completion = completion;

            await StreamUntilTerminal(workload, responseStream, cancellation).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            // The harness went away mid-stream. The finally still drains the workload; there is
            // nobody left to write to, so the handler just ends.
        }
        finally
        {
            workload.RequestStop();
            registry.Remove(workload);
            if (completion is not null)
            {
                // No token: the drain, and the client's close, finish before the handler returns,
                // as in C and Python's async server (Python's sync handler does not wait). The
                // completion never faults.
                await completion.ConfigureAwait(false);
            }
        }
    }

    // A dedicated thread, not the pool: Send, Poll and Close block, and parked pool threads would
    // starve Kestrel. The completion's continuations run off the workload thread.
    internal static Task StartDedicatedThread(ChaosWorkload workload, Action<ChaosWorkload> loop)
    {
        TaskCompletionSource done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Thread thread = new Thread(() =>
        {
            try
            {
                RunGuarded(workload, loop);
            }
            finally
            {
                done.TrySetResult();
            }
        })
        {
            IsBackground = true,
            Name = $"chaos-{workload.Id}",
        };
        thread.Start();
        return done.Task;
    }

    // The loop's last await can complete on a client's callback thread, where completing a Task.Run
    // proxy would run the handler's finally inline; the source keeps it on the pool. The Task.Run
    // task cannot fault, so it is not observed.
    internal static Task StartTask(ChaosWorkload workload, Func<ChaosWorkload, Task> loop)
    {
        TaskCompletionSource done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = Task.Run(async () =>
        {
            try
            {
                await RunGuardedAsync(workload, loop).ConfigureAwait(false);
            }
            finally
            {
                done.TrySetResult();
            }
        });
        return done.Task;
    }

    internal static async Task RunGuardedAsync(ChaosWorkload workload, Func<ChaosWorkload, Task> loop)
    {
        try
        {
            await loop(workload).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos workload {workload.Id} died: {e}");
            workload.Emit(ChaosEvents.Failed(e));
        }
    }

    internal static void RunGuarded(ChaosWorkload workload, Action<ChaosWorkload> loop)
    {
        try
        {
            loop(workload);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos workload {workload.Id} died: {e}");
            workload.Emit(ChaosEvents.Failed(e));
        }
    }

    internal static bool StopAllAndWait(ChaosRegistry registry, TimeSpan timeout)
    {
        IReadOnlyList<ChaosWorkload> workloads = registry.StopAll();
        if (workloads.Count == 0)
        {
            return true;
        }

        Task[] completions = new Task[workloads.Count];
        for (int i = 0; i < completions.Length; i++)
        {
            completions[i] = workloads[i].Completion;
        }

        // A plain wait on completions the loops' threads or tasks complete, not a sync-over-async
        // wrap, from the synchronous shutdown path, which has no synchronization context to
        // deadlock.
        return Task.WaitAll(completions, timeout);
    }

    private static Task Guarded(ChaosWorkload workload, Func<ChaosWorkload, Task> start)
    {
        try
        {
            return start(workload);
        }
        catch (Exception e)
        {
            // The loop could not even start (e.g. no thread): never leave the stream open.
            ChaosEvents.Log($"chaos workload {workload.Id} could not start: {e}");
            workload.Emit(ChaosEvents.Failed(e));
            return Task.CompletedTask;
        }
    }

    private static async Task StreamUntilTerminal(
        ChaosWorkload workload,
        IServerStreamWriter<Proto.WorkloadEventBatch> responseStream,
        CancellationToken cancellation)
    {
        while (true)
        {
            Proto.WorkloadEvent first = await workload.Events.ReadAsync(cancellation).ConfigureAwait(false);
            Proto.WorkloadEventBatch batch = new Proto.WorkloadEventBatch();
            batch.Events.Add(first);
            bool terminal = ChaosEvents.IsTerminal(first);
            while (batch.Events.Count < ChaosEvents.MaxBatch && workload.Events.TryRead(out Proto.WorkloadEvent? next))
            {
                batch.Events.Add(next);
                terminal |= ChaosEvents.IsTerminal(next);
            }

            await responseStream.WriteAsync(batch).ConfigureAwait(false);
            if (terminal)
            {
                return;
            }
        }
    }
}
