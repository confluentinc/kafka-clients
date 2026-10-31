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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.GrpcServer.Chaos;

using Confluent.Kafka.Producer;
using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// <c>ChaosWorkloadService</c> over the binding's asynchronous clients, the .NET twin of Python's
/// <c>AsyncChaosWorkloadService</c>: <see cref="ChaosWorkloadServiceImpl"/>'s loops with each call
/// awaited, one pool task per workload.
/// </summary>
internal sealed class AsyncChaosWorkloadServiceImpl : Proto.ChaosWorkloadService.ChaosWorkloadServiceBase, IDisposable
{
    private readonly ChaosRegistry _registry = new ChaosRegistry();
    private readonly Func<IReadOnlyDictionary<string, string>, IAsyncProducer<byte[], byte[]>> _producerFactory;
    private readonly Func<IReadOnlyDictionary<string, string>, IAsyncConsumer<byte[], byte[]>> _consumerFactory;

    public AsyncChaosWorkloadServiceImpl()
        : this(
            static config => new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray),
            static config => new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray))
    {
    }

    // The unit tests' seam: DI activates only the public constructor, so production always gets the
    // real clients.
    internal AsyncChaosWorkloadServiceImpl(
        Func<IReadOnlyDictionary<string, string>, IAsyncProducer<byte[], byte[]>> producerFactory,
        Func<IReadOnlyDictionary<string, string>, IAsyncConsumer<byte[], byte[]>> consumerFactory)
    {
        _producerFactory = producerFactory;
        _consumerFactory = consumerFactory;
    }

    // Synchronous, as the host's shutdown drain is: it waits on the workloads' own completions, not
    // on a wrapped async API.
    public void Dispose()
    {
        if (!ChaosStream.StopAllAndWait(_registry, ChaosStream.DrainTimeout))
        {
            ChaosEvents.Log($"chaos: shutdown drain did not finish within {ChaosStream.DrainTimeout.TotalSeconds} s");
        }
    }

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

    public override Task<Proto.StatusResponse> StopWorkload(Proto.StopWorkloadRequest request, ServerCallContext context)
    {
        // Returns at once: it only signals the stop, and never waits for (or runs) the drain.
        _registry.Stop(request.WorkloadId);
        return Task.FromResult(new Proto.StatusResponse());
    }

    public override Task<Proto.MarkWorkloadResponse> MarkWorkload(Proto.MarkWorkloadRequest request, ServerCallContext context) =>
        Task.FromResult(new Proto.MarkWorkloadResponse { Found = _registry.Mark(request.WorkloadId, request.Marker) });

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
                // producer has taken the record.
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
                    topic, ChaosEvents.Value(index, msgSize), ChaosEvents.Key(index));

                // Open the record's in-flight window before handing it over; its delivery
                // callback's event closes it.
                workload.Emit(ChaosEvents.Sent(index));
                ChaosRecordOutcomes.RecordCallback callback = outcomes.ForRecord(index);
                try
                {
                    // Awaits only the first stage and drops the AsyncKafkaFuture: the outcome
                    // arrives through the callback. No token: one firing in the first stage would
                    // end it although the record is still sent, reporting one record twice.
                    _ = await producer.Send(record, callback, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    outcomes.SendRaised(callback, e);
                }

                index++;

                // Unlike Python's async loop, the rate wait ends early on stop, and there is no
                // periodic yield: the loop runs on the thread pool, so it delays no other workload.
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
        // when it completes.
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
        AsyncChaosRebalanceListener listener = new AsyncChaosRebalanceListener(workload.Emit, consumer, checkCommits);
        try
        {
            await consumer.Subscribe(new List<string>(request.Topics), listener, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await CloseQuietly(consumer).ConfigureAwait(false);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        try
        {
            await PollUntilStopped(request, workload, consumer, checkCommits).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // A server-side fault inside the loop: close the client so a shared server does not
            // leak it (Python leaves its consumer open here). Closing is set first, as in the
            // drain, so the close-time revoke queues no read-back.
            ChaosEvents.Log($"chaos consumer {workload.Id}: poll loop died: {e}");
            listener.Closing = true;
            await CloseQuietly(consumer).ConfigureAwait(false);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

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
            await consumer.Close(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // The consumer's error, not the workload's: it drained and is closed.
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Close, e));
        }

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
                // No stop token: stop is checked between polls, as in Python.
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
                    // Only initiates the commit; its failure arrives through onCommit.
                    await consumer.CommitAsync(onCommit).ConfigureAwait(false);
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

    private static async Task ReadBack(IAsyncConsumer<byte[], byte[]> consumer, ChaosWorkload workload)
    {
        try
        {
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                await consumer.Committed(consumer.Assignment(), CancellationToken.None).ConfigureAwait(false);
            ChaosEvents.EmitCommitted(committed, workload.Emit);
        }
        catch (Exception e)
        {
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.ReadCommitted, e));
        }
    }

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

    private static async Task CloseQuietly(IAsyncConsumer<byte[], byte[]> consumer)
    {
        try
        {
            await consumer.Close(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: close after a failed loop threw: {e}");
        }

        await DisposeQuietly(consumer).ConfigureAwait(false);
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
