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
using System.Threading.Tasks;

using Confluent.Kafka.GrpcServer.Chaos;

using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// <c>ChaosWorkloadService</c> over the binding's <b>synchronous</b> clients
/// (<see cref="KafkaProducer{TKey, TValue}"/> / <see cref="KafkaConsumer{TKey, TValue}"/>, both
/// <c>&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>) — the .NET twin of
/// Python's sync <c>ChaosWorkloadService</c> (<c>python/grpc_chaos.py</c>
/// <c>_run_producer_sync</c> / <c>_run_consumer_sync</c>), hosted by the sync flavour
/// (<c>CONSUMER_FLAVOR</c> unset or <c>sync</c>, the <c>dotnet</c> harness backend; M18/P1).
/// </summary>
/// <remarks>
/// <para>
/// Each workload runs its loop on its own dedicated thread (D8, <see cref="ChaosStream.StartDedicatedThread"/>);
/// the RPC handler is async and only streams the workload's events (<see cref="ChaosStream.Run"/>).
/// The loops follow the anchor step for step (PLAN §3.3 / §3.4): the producer sends on an
/// absolute schedule without waiting for each record and reports each outcome from its delivery
/// callback; the consumer subscribes with <see cref="ChaosRebalanceListener"/>, reports each
/// record, commits after every non-empty poll, and drains by committing, reading back, closing.
/// </para>
/// <para>
/// <b>No stop token reaches the client</b> (D6 / D7): the sync surface takes none, and stop is
/// checked between sends and between polls, so stop latency is at most one rate wait or one
/// <c>poll_timeout_ms</c>, as in Python.
/// </para>
/// <para>
/// <b>Test seam (D10, PLAN §5.1).</b> The <see langword="internal"/> constructor takes the two
/// client factories so the gRPC-server unit tests can drive these same loops over
/// <see cref="MockProducer{TKey, TValue}"/> / <see cref="MockConsumer{TKey, TValue}"/> (DoD §12:
/// only construction is swapped). DI activates only public constructors, so production always
/// gets the real clients, built from the request's <c>config</c> verbatim with no mock switch.
/// </para>
/// <para>
/// <b>Singleton</b> (Program.cs): the registry is shared by every RPC, so a
/// <c>StopWorkload</c> / <c>MarkWorkload</c> can find the workload a <c>Run*</c> registered.
/// <see cref="Dispose"/> is the shutdown drain (PLAN §5.8).
/// </para>
/// </remarks>
internal sealed class ChaosWorkloadServiceImpl : Proto.ChaosWorkloadService.ChaosWorkloadServiceBase, IDisposable
{
    private readonly ChaosRegistry _registry = new ChaosRegistry();
    private readonly Func<IReadOnlyDictionary<string, string>, IProducer<byte[], byte[]>> _producerFactory;
    private readonly Func<IReadOnlyDictionary<string, string>, IConsumer<byte[], byte[]>> _consumerFactory;

    /// <summary>The production constructor: real clients, built from each request's config.</summary>
    public ChaosWorkloadServiceImpl()
        : this(
            static config => new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray),
            static config => new KafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray))
    {
    }

    /// <summary>The test seam (see the type remarks): the loops are unchanged, only construction is swapped.</summary>
    internal ChaosWorkloadServiceImpl(
        Func<IReadOnlyDictionary<string, string>, IProducer<byte[], byte[]>> producerFactory,
        Func<IReadOnlyDictionary<string, string>, IConsumer<byte[], byte[]>> consumerFactory)
    {
        _producerFactory = producerFactory;
        _consumerFactory = consumerFactory;
    }

    /// <summary>
    /// The shutdown drain (PLAN §5.8): stop every running workload, then wait for their drains,
    /// bounded by <see cref="ChaosStream.DrainTimeout"/>. Best-effort, like the other servicers'.
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
            workload => ChaosStream.StartDedicatedThread(workload, w => RunProducerLoop(request, w)));

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
            workload => ChaosStream.StartDedicatedThread(workload, w => RunConsumerLoop(request, w)));

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> StopWorkload(Proto.StopWorkloadRequest request, ServerCallContext context)
    {
        // Returns at once: it only signals the stop, and never waits for (or runs) the drain.
        _registry.Stop(request.WorkloadId);
        return Task.FromResult(new Proto.StatusResponse());
    }

    /// <inheritdoc/>
    public override Task<Proto.MarkWorkloadResponse> MarkWorkload(Proto.MarkWorkloadRequest request, ServerCallContext context) =>
        Task.FromResult(new Proto.MarkWorkloadResponse { Found = _registry.Mark(request.WorkloadId, request.Marker) });

    /// <summary>Python <c>_run_producer_sync</c>, on the workload's dedicated thread.</summary>
    private void RunProducerLoop(Proto.RunProducerRequest request, ChaosWorkload workload)
    {
        IProducer<byte[], byte[]> producer;
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
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
                    topic, ChaosEvents.Value(index, msgSize), ChaosEvents.Key(index));

                // Open the record's in-flight window before handing it over; its delivery
                // callback's event closes it.
                workload.Emit(ChaosEvents.Sent(index));
                ChaosRecordOutcomes.RecordCallback callback = outcomes.ForRecord(index);
                try
                {
                    // The KafkaFuture is dropped: the outcome arrives through the callback, which
                    // lets the binding batch and pipeline. Send returns once the record is
                    // accepted, blocking only on metadata or buffer.memory (<= max.block.ms).
                    _ = producer.Send(record, callback);
                }
                catch (Exception e)
                {
                    outcomes.SendRaised(callback, e);
                }

                index++;
                TimeSpan wait = schedule.Advance(clock.Elapsed.TotalSeconds);
                if (wait > TimeSpan.Zero)
                {
                    workload.WaitForStop(wait);
                }
            }

            workload.Emit(ChaosEvents.Stats(index, Math.Max(clock.Elapsed.TotalSeconds, 1e-9)));
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos producer {workload.Id}: send loop died: {e}");
            CloseQuietly(producer);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        // Close waits for every buffered record's outcome, so every delivery callback has fired
        // when it returns; Dispose then releases the native producer (a no-op after a clean
        // close). A close error ends the workload with Failed.
        Exception? closeError = null;
        try
        {
            producer.Close();
        }
        catch (Exception e)
        {
            closeError = e;
        }

        DisposeQuietly(producer);
        workload.Emit(closeError is null ? ChaosEvents.Finished() : ChaosEvents.Failed(closeError));
    }

    /// <summary>Python <c>_run_consumer_sync</c>, on the workload's dedicated thread.</summary>
    private void RunConsumerLoop(Proto.RunConsumerRequest request, ChaosWorkload workload)
    {
        IConsumer<byte[], byte[]> consumer;
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
            // the close-time revoke, so it lives until after Close.
            handle = consumer.Handle();
            listener = new ChaosRebalanceListener(workload.Emit, handle, checkCommits);
            consumer.Subscribe(new List<string>(request.Topics), listener);
        }
        catch (Exception e)
        {
            CloseQuietly(consumer, handle);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        try
        {
            PollUntilStopped(request, workload, consumer, checkCommits);
        }
        catch (Exception e)
        {
            // A server-side fault inside the loop (the client's own errors are all reported as
            // ConsumerError above). Close the client so a shared server does not leak it.
            ChaosEvents.Log($"chaos consumer {workload.Id}: poll loop died: {e}");
            CloseQuietly(consumer, handle);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        // The drain: a final sync commit (in both commit modes), its read-back, then close.
        try
        {
            consumer.Commit();
            if (checkCommits)
            {
                ReadBack(consumer, workload);
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
            consumer.Close();
        }
        catch (Exception e)
        {
            // The consumer's error, not the workload's: it drained and is closed.
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Close, e));
        }

        // No close-spec split (§3.5 item 4): the handle ref-counts its consumer, so disposing it
        // after Close and then the consumer is the plain, correct order.
        DisposeQuietly(handle);
        DisposeQuietly(consumer);
        workload.Emit(ChaosEvents.Closed());
        workload.Emit(ChaosEvents.Finished());
    }

    private static void PollUntilStopped(
        Proto.RunConsumerRequest request,
        ChaosWorkload workload,
        IConsumer<byte[], byte[]> consumer,
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
                records = consumer.Poll(pollTimeout);
            }
            catch (Exception e)
            {
                workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Poll, e));
                workload.WaitForStop(ChaosEvents.PollErrorBackoff);
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
                    consumer.Commit();
                }
                else
                {
                    // Only initiates the commit; its failure arrives through onCommit.
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
                ReadBack(consumer, workload);
            }
        }
    }

    /// <summary>The assignment's committed offsets as <c>Committed</c> events (Python <c>read_back</c>).</summary>
    private static void ReadBack(IConsumer<byte[], byte[]> consumer, ChaosWorkload workload)
    {
        try
        {
            ChaosEvents.EmitCommitted(consumer.Committed(consumer.Assignment()), workload.Emit);
        }
        catch (Exception e)
        {
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.ReadCommitted, e));
        }
    }

    /// <summary>Python <c>_close_quietly</c>, for the producer after a failed loop.</summary>
    private static void CloseQuietly(IProducer<byte[], byte[]> producer)
    {
        try
        {
            producer.Close();
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: close after a failed loop threw: {e}");
        }

        DisposeQuietly(producer);
    }

    /// <summary>The consumer's close after a failed setup or loop: close, then the handle, then the consumer.</summary>
    private static void CloseQuietly(IConsumer<byte[], byte[]> consumer, ConsumerHandle? handle)
    {
        try
        {
            consumer.Close();
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: close after a failed loop threw: {e}");
        }

        DisposeQuietly(handle);
        DisposeQuietly(consumer);
    }

    private static void DisposeQuietly(IDisposable? disposable)
    {
        try
        {
            disposable?.Dispose();
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: dispose threw: {e}");
        }
    }
}
