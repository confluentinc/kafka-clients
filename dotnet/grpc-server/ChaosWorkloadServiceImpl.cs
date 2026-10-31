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

using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.GrpcServer.Chaos;

using Confluent.Kafka.Producer;
using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// <c>ChaosWorkloadService</c> over the binding's synchronous clients, the .NET twin of Python's sync
/// <c>ChaosWorkloadService</c>: each workload loop runs on its own thread; the RPC streams its events.
/// </summary>
internal sealed class ChaosWorkloadServiceImpl : Proto.ChaosWorkloadService.ChaosWorkloadServiceBase, IDisposable
{
    private readonly ChaosRegistry _registry = new ChaosRegistry();
    private readonly Func<IReadOnlyDictionary<string, string>, IProducer<byte[], byte[]>> _producerFactory;
    private readonly Func<IReadOnlyDictionary<string, string>, IConsumer<byte[], byte[]>> _consumerFactory;

    public ChaosWorkloadServiceImpl()
        : this(
            static config => new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray),
            static config => new KafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray))
    {
    }

    // The unit tests' seam: DI activates only the public constructor, so production always gets the
    // real clients.
    internal ChaosWorkloadServiceImpl(
        Func<IReadOnlyDictionary<string, string>, IProducer<byte[], byte[]>> producerFactory,
        Func<IReadOnlyDictionary<string, string>, IConsumer<byte[], byte[]>> consumerFactory)
    {
        _producerFactory = producerFactory;
        _consumerFactory = consumerFactory;
    }

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
            workload => ChaosStream.StartDedicatedThread(workload, w => RunProducerLoop(request, w)));

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

    public override Task<Proto.StatusResponse> StopWorkload(Proto.StopWorkloadRequest request, ServerCallContext context)
    {
        // Returns at once: it only signals the stop, and never waits for (or runs) the drain.
        _registry.Stop(request.WorkloadId);
        return Task.FromResult(new Proto.StatusResponse());
    }

    public override Task<Proto.MarkWorkloadResponse> MarkWorkload(Proto.MarkWorkloadRequest request, ServerCallContext context) =>
        Task.FromResult(new Proto.MarkWorkloadResponse { Found = _registry.Mark(request.WorkloadId, request.Marker) });

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
                    // The returned future is not waited on: the outcome arrives through the
                    // callback, which lets the binding batch and pipeline.
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
        // when it returns.
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
        ChaosRebalanceListener listener = new ChaosRebalanceListener(workload.Emit, consumer, checkCommits);
        try
        {
            consumer.Subscribe(new List<string>(request.Topics), listener);
        }
        catch (Exception e)
        {
            CloseQuietly(consumer);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

        try
        {
            PollUntilStopped(request, workload, consumer, checkCommits);
        }
        catch (Exception e)
        {
            // A server-side fault inside the loop: close the client so a shared server does not
            // leak it (Python leaves its consumer open here). Closing is set first, as in the
            // drain, so the close-time revoke queues no read-back.
            ChaosEvents.Log($"chaos consumer {workload.Id}: poll loop died: {e}");
            listener.Closing = true;
            CloseQuietly(consumer);
            workload.Emit(ChaosEvents.Failed(e));
            return;
        }

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
            consumer.Close();
        }
        catch (Exception e)
        {
            // The consumer's error, not the workload's: it drained and is closed.
            workload.Emit(ChaosEvents.ConsumerError(Proto.ConsumerOp.Close, e));
        }

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

    private static void CloseQuietly(IConsumer<byte[], byte[]> consumer)
    {
        try
        {
            consumer.Close();
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos: close after a failed loop threw: {e}");
        }

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
