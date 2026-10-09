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
/// The body every <c>RunProducer</c> / <c>RunConsumer</c> RPC shares, in both flavours (PLAN
/// §5.3) — the .NET twin of Python's <c>ChaosWorkloadService._run</c> / <c>_guarded</c>
/// (<c>python/grpc_chaos.py</c>), plus the C server's wait for its worker.
/// </summary>
/// <remarks>
/// <para>
/// <b>The lifecycle</b> (chaos_service.proto steps 1–4): register the id (a duplicate gets one
/// <c>Failed</c> batch, written before any header, as in Python); send the response headers at
/// once, so the harness's call resolves even for a consumer that never gets an assignment;
/// make the RPC's cancellation stop the workload; start the loop; stream its events in batches
/// of up to <see cref="ChaosEvents.MaxBatch"/> until a terminal one; and, in the
/// <c>finally</c>, stop the workload, deregister it, and <b>await its completion with no
/// token</b>. On a normal end that completion has already happened; after a cancellation the
/// loop is draining — the producer closing, the consumer committing and closing — so the
/// client is closed before the handler returns (C / Python-async parity; §3.5 item 6, which
/// is a deliberate difference from Python sync, whose handler does not wait).
/// </para>
/// <para>
/// <b>One write at a time</b>, as gRPC requires: only this method writes to the stream, and
/// it awaits each write before reading the next batch. Test-server scaffolding with no Java
/// class (DoD §7).
/// </para>
/// </remarks>
internal static class ChaosStream
{
    /// <summary>
    /// The bound on a servicer's shutdown wait for its workloads' drains (PLAN §5.8), matching
    /// the clients' own close timeouts.
    /// </summary>
    internal static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Runs one workload's RPC (see the type remarks). <paramref name="start"/> starts the loop
    /// for the registered workload and returns its completion; it is called once, after the
    /// headers are out and the cancellation hook is in place.
    /// </summary>
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
            // Headers now, not with the first event (proto lifecycle step 1; backend_pool.rs
            // streaming_channel() has no timeout because of this).
            await context.WriteResponseHeadersAsync(new Metadata()).ConfigureAwait(false);

            // The harness going away (a cancelled RPC) stops the workload, which then drains
            // and closes its client. RequestStop never runs a waiter inline (ChaosWorkload).
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
                // No token: the drain must finish (and the client close) before the handler
                // returns. The completion never faults (Guarded).
                await completion.ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Starts <paramref name="loop"/> on a dedicated background thread named
    /// <c>chaos-&lt;id&gt;</c> (the sync flavour, D8; PLAN §5.7) and returns its completion, a
    /// <see cref="TaskCompletionSource"/> built with
    /// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> so the handler's
    /// continuation never runs on the workload thread.
    /// </summary>
    /// <remarks>
    /// A dedicated thread, not the pool: <c>Send</c> (on <c>buffer.memory</c>), <c>Poll</c> and
    /// <c>Close</c> block, and parking pool threads would also starve Kestrel. There is no
    /// worker cap (§3.5 item 1): one thread per workload, and the RPC handler holds none while
    /// it waits on the channel.
    /// </remarks>
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

    /// <summary>
    /// Python <c>_guarded</c>: runs <paramref name="loop"/>, and turns anything that escapes it
    /// into <c>Failed</c>, so a dying loop never leaves the stream open.
    /// </summary>
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

    /// <summary>
    /// A servicer's shutdown drain (PLAN §5.8): stop every registered workload, then wait, up to
    /// <paramref name="timeout"/>, for their loops to finish draining. Only container
    /// <c>SIGTERM</c> reaches this — the native servers are SIGKILLed by <c>backend_pool</c>.
    /// </summary>
    /// <returns>Whether every drain finished within <paramref name="timeout"/>.</returns>
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

        // A plain wait on completions that dedicated threads (sync) or pool tasks (async)
        // complete — not a sync-over-async wrap of an async API — from the synchronous shutdown
        // path, which has no synchronization context to deadlock.
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
