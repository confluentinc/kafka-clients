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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.GrpcServer.Chaos;

using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// <c>ChaosWorkloadService</c> over the binding's <b>asynchronous</b> clients
/// (<see cref="AsyncKafkaProducer{TKey, TValue}"/> / <see cref="AsyncKafkaConsumer{TKey, TValue}"/>,
/// both <c>&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>) — the .NET twin of
/// Python's <c>AsyncChaosWorkloadService</c> (<c>python/grpc_chaos.py</c>
/// <c>_run_producer_async</c> / <c>_run_consumer_async</c>), hosted by the async flavour
/// (<c>CONSUMER_FLAVOR=async</c>, the <c>dotnet-async</c> harness backend; M18/P1).
/// </summary>
/// <remarks>
/// <para>
/// Each workload's loop is one pool task (D8, <see cref="ChaosStream.StartTask"/>); the RPC
/// handler is the same shared body as the sync flavour's (<see cref="ChaosStream.Run"/>). The
/// loops are <see cref="ChaosWorkloadServiceImpl"/>'s, step for step, with each blocking call
/// awaited instead (PLAN §3.3 / §3.4): the producer awaits only <c>Send</c>'s admission stage and
/// drops the <see cref="AsyncKafkaFuture{T}"/>, reporting each outcome from its delivery callback.
/// </para>
/// <para>
/// <b>No token that can fire reaches the client</b> (D6 / D7). Every client call passes
/// <see cref="CancellationToken.None"/>; stop is checked between sends and between polls, as in
/// Python. A caller token that fires while <c>Send</c>'s admission stage waits ends that stage
/// with an <see cref="OperationCanceledException"/> although the record is still sent and its
/// callback still fires (M11/P3.5 D2 (c)), which would report one record twice; and a token on
/// <c>Poll</c> / <c>Commit</c> / <c>Committed</c> / <c>Close</c> would mix the consumer's
/// wakeup-based cancel into the drain. So a stop requested during an admission wait takes effect
/// once the admission completes, and stop latency is at most one rate wait or one
/// <c>poll_timeout_ms</c>.
/// </para>
/// <para>
/// <b>The rate wait resumes early on stop</b> (<see cref="ChaosWorkload.WaitForStopAsync"/>),
/// where Python's async loop sleeps a whole interval: a deliberate difference that only shortens
/// stop latency at a low <c>--rps</c> (§3.5 item 3). Unlike Python there is no "yield every N
/// records" (§3.5 item 2): the loop runs on the multi-threaded pool, so a loop that never yields
/// delays no other workload.
/// </para>
/// <para>
/// <b>Test seam (D10, PLAN §5.1).</b> As on the sync servicer, the <see langword="internal"/>
/// constructor takes the two client factories so the unit tests can drive these same loops over
/// test doubles around <see cref="AsyncMockProducer{TKey, TValue}"/> /
/// <see cref="AsyncMockConsumer{TKey, TValue}"/> (DoD §12). DI activates only public
/// constructors, so production always builds the real clients from the request's <c>config</c>
/// verbatim.
/// </para>
/// <para>
/// <b>Singleton</b> (Program.cs), for the shared registry; <see cref="Dispose"/> is the shutdown
/// drain (PLAN §5.8).
/// </para>
/// </remarks>
internal sealed class AsyncChaosWorkloadServiceImpl : Proto.ChaosWorkloadService.ChaosWorkloadServiceBase, IDisposable
{
    private readonly ChaosRegistry _registry = new ChaosRegistry();
    private readonly Func<IReadOnlyDictionary<string, string>, IAsyncProducer<byte[], byte[]>> _producerFactory;
    private readonly Func<IReadOnlyDictionary<string, string>, IAsyncConsumer<byte[], byte[]>> _consumerFactory;

    /// <summary>The production constructor: real clients, built from each request's config.</summary>
    public AsyncChaosWorkloadServiceImpl()
        : this(
            static config => new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray),
            static config => new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray))
    {
    }

    /// <summary>The test seam (see the type remarks): the loops are unchanged, only construction is swapped.</summary>
    internal AsyncChaosWorkloadServiceImpl(
        Func<IReadOnlyDictionary<string, string>, IAsyncProducer<byte[], byte[]>> producerFactory,
        Func<IReadOnlyDictionary<string, string>, IAsyncConsumer<byte[], byte[]>> consumerFactory)
    {
        _producerFactory = producerFactory;
        _consumerFactory = consumerFactory;
    }

    /// <summary>
    /// The shutdown drain (PLAN §5.8): stop every running workload, then wait for their drains,
    /// bounded by <see cref="ChaosStream.DrainTimeout"/>. Synchronous, because the host's
    /// shutdown path is (Program.cs <c>DrainServicer</c>); it waits on the workloads' own
    /// completions, not on a wrapped async API.
    /// </summary>
    public void Dispose()
    {
        if (!ChaosStream.StopAllAndWait(_registry, ChaosStream.DrainTimeout))
        {
            ChaosEvents.Log($"chaos: shutdown drain did not finish within {ChaosStream.DrainTimeout.TotalSeconds} s");
        }
    }

    /// <inheritdoc/>
    public override Task RunProducer(
        Proto.RunProducerRequest request,
        IServerStreamWriter<Proto.WorkloadEventBatch> responseStream,
        ServerCallContext context) =>
        ChaosStream.Run(
            _registry,
            request.WorkloadId,
            responseStream,
            context,
            workload => ChaosStream.StartTask(workload, w => RunProducerLoop(request, w)));

    /// <inheritdoc/>
    public override Task RunConsumer(
        Proto.RunConsumerRequest request,
        IServerStreamWriter<Proto.WorkloadEventBatch> responseStream,
        ServerCallContext context) =>
        ChaosStream.Run(
            _registry,
            request.WorkloadId,
            responseStream,
            context,
            workload => ChaosStream.StartTask(workload, w => RunConsumerLoop(request, w)));

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> StopWorkload(Proto.StopWorkloadRequest request, ServerCallContext context)
    {
        // Returns at once: it only signals the stop, and never waits for (or runs) the drain —
        // the loop's waits resume on the pool, not here (ChaosWorkload, T6b).
        _registry.Stop(request.WorkloadId);
        return Task.FromResult(new Proto.StatusResponse());
    }

    /// <inheritdoc/>
    public override Task<Proto.MarkWorkloadResponse> MarkWorkload(Proto.MarkWorkloadRequest request, ServerCallContext context) =>
        Task.FromResult(new Proto.MarkWorkloadResponse { Found = _registry.Mark(request.WorkloadId, request.Marker) });

    /// <summary>Python <c>_run_producer_async</c>, as the workload's pool task.</summary>
    private async Task RunProducerLoop(Proto.RunProducerRequest request, ChaosWorkload workload)
    {
        IAsyncProducer<byte[], byte[]> producer;
        try
        {
            producer = _producerFactory(new Dictionary<string, string>(request.Config));
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos producer {workload.Id}: construction failed: {e}");
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        string topic = request.Topic;
        uint msgSize = request.MsgSize;
        ChaosRecordOutcomes outcomes = new ChaosRecordOutcomes(workload.Emit, workload.Id);
        Stopwatch clock = Stopwatch.StartNew();
        ChaosRateSchedule schedule = new ChaosRateSchedule(request.TargetRps, clock.Elapsed.TotalSeconds);
        ulong index = 0;
        try
        {
            while (!workload.IsStopRequested)
            {
                // Fresh key and value arrays per record: the async Send borrows them until the
                // record is delivered (ffi §A4, deferred send; PLAN §5.5.4).
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
                    topic, ChaosEvents.Value(index, msgSize), ChaosEvents.Key(index));

                // Open the record's in-flight window before handing it over; its delivery
                // callback's event closes it.
                workload.Emit(ChaosEvents.Sent(index));
                ChaosRecordOutcomes.RecordCallback callback = outcomes.ForRecord(index);
                try
                {
                    // Awaits admission only (it completes synchronously while the bound has room);
                    // the AsyncKafkaFuture is dropped, since the outcome arrives through the
                    // callback (R6). CancellationToken.None, never the stop token (D6).
                    _ = await producer.Send(record, callback, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    outcomes.SendRaised(callback, e);
                }

                index++;
                TimeSpan wait = schedule.Advance(clock.Elapsed.TotalSeconds);
                if (wait > TimeSpan.Zero)
                {
                    await workload.WaitForStopAsync(wait).ConfigureAwait(false);
                }
            }

            workload.Emit(ChaosEvents.Stats(index, Math.Max(clock.Elapsed.TotalSeconds, 1e-9)));
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos producer {workload.Id}: send loop died: {e}");
            await CloseQuietly(producer).ConfigureAwait(false);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        // Close waits for every buffered record's outcome, so every delivery callback has fired
        // when it completes; DisposeAsync then releases the native producer (a no-op after a
        // clean close). A close error ends the workload with Failed.
        Exception? closeError = null;
        try
        {
            await producer.Close(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            closeError = e;
        }

        await DisposeQuietly(producer).ConfigureAwait(false);
        workload.Emit(closeError is null ? ChaosEvents.Finished() : ChaosEvents.Failed(closeError));
    }

    /// <summary>Python <c>_run_consumer_async</c>, as the workload's pool task.</summary>
    private async Task RunConsumerLoop(Proto.RunConsumerRequest request, ChaosWorkload workload)
    {
        IAsyncConsumer<byte[], byte[]> consumer;
        try
        {
            consumer = _consumerFactory(new Dictionary<string, string>(request.Config));
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos consumer {workload.Id}: construction failed: {e}");
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        bool checkCommits = request.CommitCheckIntervalMs > 0;
        ConsumerHandle? handle = null;
        ChaosRebalanceListener listener;
        try
        {
            // The handle before Subscribe: the listener commits through it, including during
            // the close-time revoke, so it lives until after Close. The listener runs on the
            // core's dispatcher thread in this flavour too, where the handle's blocking calls
            // belong (ffi §B1).
            handle = consumer.Handle();
            listener = new ChaosRebalanceListener(workload.Emit, handle, checkCommits);
            await consumer.Subscribe(new List<string>(request.Topics), listener, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await CloseQuietly(consumer, handle).ConfigureAwait(false);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        try
        {
            await PollUntilStopped(request, workload, consumer, checkCommits).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // A server-side fault inside the loop (the client's own errors are all reported as
            // ConsumerError). Close the client so a shared server does not leak it.
            ChaosEvents.Log($"chaos consumer {workload.Id}: poll loop died: {e}");
            await CloseQuietly(consumer, handle).ConfigureAwait(false);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        // The drain: a final confirming commit (in both commit modes), its read-back, then close.
        try
        {
            await consumer.Commit(CancellationToken.None).ConfigureAwait(false);
            if (checkCommits)
            {
                await ReadBack(consumer, workload).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Commit, e));
        }

        listener.Closing = true;
        workload.Emit(ChaosEvents.Closing());
        try
        {
            // The close-time OnPartitionsRevoked still commits through the live handle.
            await consumer.Close(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // The consumer's error, not the workload's: it drained and is closed.
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Close, e));
        }

        // No close-spec split (§3.5 item 4): the handle ref-counts its consumer, so disposing it
        // after Close and then the consumer is the plain, correct order.
        DisposeHandleQuietly(handle);
        await DisposeQuietly(consumer).ConfigureAwait(false);
        workload.Emit(ChaosEvents.Closed());
        workload.Emit(ChaosEvents.Finished());
    }

    private static async Task PollUntilStopped(
        Proto.RunConsumerRequest request,
        ChaosWorkload workload,
        IAsyncConsumer<byte[], byte[]> consumer,
        bool checkCommits)
    {
        TimeSpan pollTimeout = TimeSpan.FromMilliseconds(request.PollTimeoutMs);
        double checkIntervalSeconds = request.CommitCheckIntervalMs / 1000.0;
        bool syncCommit = request.CommitMode == Proto.CommitMode.Sync;
        uint msgSize = request.MsgSize;
        ChaosCommitCallback onCommit = new ChaosCommitCallback(workload.Emit);
        Stopwatch clock = Stopwatch.StartNew();
        double lastCheck = clock.Elapsed.TotalSeconds;

        while (!workload.IsStopRequested)
        {
            ConsumerRecords<byte[], byte[]> records;
            try
            {
                // No stop token (D7): stop is checked between polls.
                records = await consumer.Poll(pollTimeout, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Poll, e));
                await workload.WaitForStopAsync(ChaosEvents.PollErrorBackoff).ConfigureAwait(false);
                continue;
            }

            if (records.Count == 0)
            {
                continue;
            }

            foreach (ConsumerRecord<byte[], byte[]> record in records)
            {
                workload.Emit(ChaosEvents.ConsumedOrCorrupted(record, msgSize));
            }

            try
            {
                if (syncCommit)
                {
                    await consumer.Commit(CancellationToken.None).ConfigureAwait(false);
                }
                else
                {
                    // A sync call in both flavours: it only initiates the commit, whose failure
                    // arrives through onCommit on the dispatcher thread.
                    consumer.CommitAsync(onCommit);
                }
            }
            catch (Exception e)
            {
                workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Commit, e));
                continue;
            }

            // Read back only after a sync commit: an async one may not have reached the broker.
            if (syncCommit && checkCommits && clock.Elapsed.TotalSeconds - lastCheck >= checkIntervalSeconds)
            {
                lastCheck = clock.Elapsed.TotalSeconds;
                await ReadBack(consumer, workload).ConfigureAwait(false);
            }
        }
    }

    /// <summary>The assignment's committed offsets as <c>Committed</c> events (Python <c>read_back</c>).</summary>
    private static async Task ReadBack(IAsyncConsumer<byte[], byte[]> consumer, ChaosWorkload workload)
    {
        try
        {
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> committed =
                await consumer.Committed(consumer.Assignment(), CancellationToken.None).ConfigureAwait(false);
            ChaosEvents.EmitCommitted(committed, workload.Emit);
        }
        catch (Exception e)
        {
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.ReadCommitted, e));
        }
    }

    /// <summary>Python <c>_close_quietly</c>, for the producer after a failed loop.</summary>
    private static async Task CloseQuietly(IAsyncProducer<byte[], byte[]> producer)
    {
        try
        {
            await producer.Close(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: close after a failed loop threw: {e}");
        }

        await DisposeQuietly(producer).ConfigureAwait(false);
    }

    /// <summary>
    /// Python <c>_close_consumer_async</c> after a failed setup or loop: close, then the handle,
    /// then the consumer.
    /// </summary>
    private static async Task CloseQuietly(IAsyncConsumer<byte[], byte[]> consumer, ConsumerHandle? handle)
    {
        try
        {
            await consumer.Close(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: close after a failed loop threw: {e}");
        }

        DisposeHandleQuietly(handle);
        await DisposeQuietly(consumer).ConfigureAwait(false);
    }

    /// <summary>
    /// The handle's disposal: <see cref="ConsumerHandle"/> is <see cref="IDisposable"/> only,
    /// every handle operation being a synchronous C call (CLAUDE.md §4 reentrancy row).
    /// </summary>
    private static void DisposeHandleQuietly(ConsumerHandle? handle)
    {
        try
        {
            handle?.Dispose();
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: dispose threw: {e}");
        }
    }

    private static async Task DisposeQuietly(IAsyncDisposable disposable)
    {
        try
        {
            await disposable.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: dispose threw: {e}");
        }
    }
}
