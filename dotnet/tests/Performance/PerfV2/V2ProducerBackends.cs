// Copyright 2025 Confluent Inc.
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

namespace Confluent.Kafka.Performance.V2;

/// <summary>Builds the ckd producer both v2 backends drive.</summary>
internal static class V2ProducerFactory
{
    internal static IProducer<byte[], byte[]> Build(IReadOnlyDictionary<string, string> config, string label)
    {
        if (PerfEnv.GetString("PERF_LRK_STATS", string.Empty) != "1")
        {
            return new ProducerBuilder<byte[], byte[]>(config).Build();
        }

        var conf = new Dictionary<string, string>();
        foreach (KeyValuePair<string, string> kv in config)
        {
            conf[kv.Key] = kv.Value;
        }

        conf["statistics.interval.ms"] = "10000";
        return new ProducerBuilder<byte[], byte[]>(conf)
            .SetStatisticsHandler((_, json) => Console.Error.WriteLine($"[STATS-DOTNET-{label}] {json}"))
            .Build();
    }
}

/// <summary>
/// Adapts ckd's producer to the sync <see cref="IProducerBackend"/>, as Python's <c>CompatibleProducer</c>: a
/// fire-and-forget <c>Produce</c> whose delivery report, served by a background poll thread, reports to the engine's
/// callback; the warmup send's handle waits for the delivery.
/// </summary>
internal sealed class V2SyncProducerBackend : IProducerBackend
{
    private static readonly Func<object, PerfRecordMetadata> s_get = static state =>
        ((TaskCompletionSource<PerfRecordMetadata>)state).Task.GetAwaiter().GetResult();

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly Thread _pollThread;
    private volatile bool _closed;

    internal V2SyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = V2ProducerFactory.Build(config, "sync");

        _pollThread = new Thread(PollLoop) { IsBackground = true, Name = "v2-producer-poll" };
        _pollThread.Start();
    }

    public PerfSendHandle Send(string topic, byte[]? key, byte[]? value)
    {
        // The report fires on the poll thread, which must not run the warmup's continuation.
        var fut = new TaskCompletionSource<PerfRecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

        Action<DeliveryReport<byte[], byte[]>> deliveryReport = report =>
        {
            if (report.Error.IsError)
            {
                fut.TrySetException(new KafkaException(report.Error));
            }
            else
            {
                fut.TrySetResult(new PerfRecordMetadata(
                    report.Topic, report.Partition.Value, report.Offset.Value, report.Message.Timestamp.UnixTimestampMs));
            }
        };

        if (!Produce(topic, key, value, deliveryReport))
        {
            // Python leaves the future unresolved here; cancelling it lets the warmup's wait return on shutdown.
            fut.TrySetCanceled();
        }

        return new PerfSendHandle(s_get, fut);
    }

    public void Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs)
    {
        Action<DeliveryReport<byte[], byte[]>> deliveryReport = report =>
        {
            if (report.Error.IsError)
            {
                callback.OnCompletion(sendStartMs, default, new KafkaException(report.Error));
            }
            else
            {
                callback.OnCompletion(
                    sendStartMs,
                    new PerfRecordMetadata(report.Topic, report.Partition.Value, report.Offset.Value, report.Message.Timestamp.UnixTimestampMs),
                    null);
            }
        };

        if (!Produce(topic, key, value, deliveryReport))
        {
            // No delivery report will come, so this is a send that threw (Python's callback never runs here).
            throw new OperationCanceledException("Terminated before the producer accepted the record.");
        }
    }

    // Unlike Python's flush, the poll thread keeps running: ckd's Flush serves the remaining reports alongside it.
    public void Flush(CancellationToken cancellationToken) => _producer.Flush(cancellationToken);

    private bool Produce(string topic, byte[]? key, byte[]? value, Action<DeliveryReport<byte[], byte[]>> deliveryReport)
    {
        // ckd's Key and Value accept null at runtime (no key, a tombstone) but are not nullable-annotated.
        var message = new Message<byte[], byte[]> { Key = key!, Value = value! };

        // Python's BufferError retry (ckd reports a QUEUE_FULL ProduceException); checking Terminating on each attempt
        // lets a shutdown end a sustained QUEUE_FULL.
        while (!PerfSignals.Terminating)
        {
            try
            {
                _producer.Produce(topic, message, deliveryReport);
                return true;
            }
            catch (ProduceException<byte[], byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                Thread.Sleep(1);
            }
        }

        return false;
    }

    public void Close()
    {
        // Python's close does not flush; this flushes any residual (normally none, as the engine has flushed), then
        // joins the poll thread.
        _producer.Flush(TimeSpan.FromSeconds(30));
        _closed = true;
        if (_pollThread.IsAlive)
        {
            _pollThread.Join();
        }
    }

    public void Dispose()
    {
        _closed = true;
        if (_pollThread.IsAlive)
        {
            _pollThread.Join();
        }

        _producer.Dispose();
    }

    private void PollLoop()
    {
        while (!_closed)
        {
            try
            {
                _producer.Poll(TimeSpan.FromSeconds(1));
            }
            catch (Exception)
            {
                // A transient poll error must not stop the reports; a record's own error arrives in its report.
            }
        }
    }
}

/// <summary>
/// Adapts ckd's producer to the async <see cref="IAsyncProducerBackend"/> over <c>ProduceAsync</c>, as ckd has no
/// AIO-style producer (Python's v2 wraps <c>AIOProducer</c>); the measured send reports the task's outcome to the
/// engine's callback, as Python's v2 <c>add_done_callback</c> does.
/// </summary>
internal sealed class V2AsyncProducerBackend : IAsyncProducerBackend
{
    // Static, so a send allocates no closure for it; the callback and start time ride in the state.
    private static readonly Action<Task<DeliveryResult<byte[], byte[]>>, object?> s_report = static (delivery, state) =>
    {
        var s = (DeliveryState)state!;
        if (delivery.IsCanceled)
        {
            s.Callback.OnCompletion(s.SendStartMs, default, new OperationCanceledException());
        }
        else if (delivery.IsFaulted)
        {
            // A full queue lands here too, as a ProduceException whose message ("Local: Queue full") the
            // engine's QUEUE_FULL check matches.
            Exception e = delivery.Exception!;
            s.Callback.OnCompletion(s.SendStartMs, default, e.InnerException ?? e);
        }
        else
        {
            DeliveryResult<byte[], byte[]> result = delivery.Result;
            s.Callback.OnCompletion(
                s.SendStartMs,
                new PerfRecordMetadata(result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Timestamp.UnixTimestampMs),
                null);
        }
    };

    private readonly IProducer<byte[], byte[]> _producer;

    internal V2AsyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = V2ProducerFactory.Build(config, "async");
    }

    // ckd's ProduceAsync has no separate accepted stage: the record is accepted inside the call, so the
    // first stage is always already complete and every failure surfaces through the delivery task.
    public ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value) =>
        new ValueTask<Task<PerfRecordMetadata>>(Produce(topic, key, value));

    private async Task<PerfRecordMetadata> Produce(string topic, byte[]? key, byte[]? value)
    {
        // ProduceAsync enqueues synchronously, preserving send order; a failure faults the returned Task.
        DeliveryResult<byte[], byte[]> result = await _producer
            .ProduceAsync(topic, new Message<byte[], byte[]> { Key = key!, Value = value! })
            .ConfigureAwait(false);
        return new PerfRecordMetadata(
            result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Timestamp.UnixTimestampMs);
    }

    public ValueTask Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs)
    {
        _producer
            .ProduceAsync(topic, new Message<byte[], byte[]> { Key = key!, Value = value! })
            .ContinueWith(s_report, new DeliveryState(callback, sendStartMs), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return default;
    }

    public Task Flush(CancellationToken cancellationToken)
    {
        _producer.Flush(cancellationToken);
        return Task.CompletedTask;
    }

    public Task Close()
    {
        _producer.Flush(TimeSpan.FromSeconds(30));
        return Task.CompletedTask;
    }

    public void Dispose() => _producer.Dispose();

    public ValueTask DisposeAsync()
    {
        _producer.Dispose();
        return default;
    }
}

/// <summary>The engine's callback and the send's start time, carried to the delivery continuation.</summary>
internal sealed class DeliveryState
{
    internal DeliveryState(IPerfDeliveryCallback callback, long sendStartMs)
    {
        Callback = callback;
        SendStartMs = sendStartMs;
    }

    internal IPerfDeliveryCallback Callback { get; }

    internal long SendStartMs { get; }
}
