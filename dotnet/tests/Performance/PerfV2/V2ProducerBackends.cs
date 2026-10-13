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

// ckd's types (IProducer, ProducerBuilder, Message, DeliveryResult, ProduceException, ErrorCode,
// KafkaException, ...) live in the `Confluent.Kafka` namespace, which is an ANCESTOR of this file's
// `Confluent.Kafka.Performance.V2` namespace, so they are in scope with no `using Confluent.Kafka;`
// (the PerfV3 precedent — its backends reach our binding's Confluent.Kafka types the same way). Adding
// the redundant using would trip IDE0005 under EnforceCodeStyleInBuild.

/// <summary>Builds the ckd producer both v2 backends drive.</summary>
internal static class V2ProducerFactory
{
    /// <summary>
    /// Builds the producer. With <c>PERF_LRK_STATS=1</c> it also enables librdkafka's statistics every 10 s
    /// (per-broker rtt / int_latency / outbuf_latency, per-partition msgq, for stage debugging) and prints
    /// each JSON document to stderr as <c>[STATS-DOTNET-&lt;label&gt;] {json}</c> (Python
    /// <c>_maybe_enable_lrk_stats</c>, whose prefix is <c>STATS-PY</c>). Added after the configuration is
    /// printed, as in Python.
    /// </summary>
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
/// Adapts ckd's <see cref="IProducer{TKey, TValue}"/> to the sync <see cref="IProducerBackend"/>: a
/// line-for-line port of Python's <c>CompatibleProducer</c>. Neither ckd nor confluent-kafka-python has a sync
/// producer, so "V2 sync" means ckd's fire-and-forget <c>Produce(topic, message, deliveryHandler)</c>, with a
/// background <c>Poll</c> thread serving the delivery reports. The measured send reports each delivery to the
/// engine's callback from the delivery report, as Python's <c>CompatibleProducer.send(record, callback)</c>
/// does; the warmup send hands back a handle whose <see cref="PerfSendHandle.Get"/> waits the delivery. Bytes
/// flow over ckd's built-in <c>Serializers.ByteArray</c> (ckd auto-selects it for
/// <c>&lt;byte[], byte[]&gt;</c>).
/// </summary>
internal sealed class V2SyncProducerBackend : IProducerBackend
{
    // fut.result(): blocks the engine's warmup until the delivery report resolves the future.
    private static readonly Func<object, PerfRecordMetadata> s_get = static state =>
        ((TaskCompletionSource<PerfRecordMetadata>)state).Task.GetAwaiter().GetResult();

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly Thread _pollThread;
    private volatile bool _closed;

    internal V2SyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = V2ProducerFactory.Build(config, "sync");

        // Background poll loop serves the fire-and-forget delivery callbacks (Python CompatibleProducer's
        // poll_producer thread). IsBackground so a startup crash still lets the process exit.
        _pollThread = new Thread(PollLoop) { IsBackground = true, Name = "v2-producer-poll" };
        _pollThread.Start();
    }

    public PerfSendHandle Send(string topic, byte[]? key, byte[]? value)
    {
        // fut = Future(). RunContinuationsAsynchronously: the delivery report fires on the poll thread, and
        // completing the TCS there must not run a continuation on that thread. The handle's Get() parks
        // the warmup in a synchronous wait, which is released either way.
        var fut = new TaskCompletionSource<PerfRecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

        // def delivery_report(err, msg): set_exception on an error, set_result otherwise.
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
            // Bailed out on termination before Produce ever accepted the record. Python leaves the
            // Future unresolved here (a latent hang for whoever waits it); cancel instead so the warmup's
            // wait returns promptly rather than hanging the shutdown path.
            fut.TrySetCanceled();
        }

        // return fut
        return new PerfSendHandle(s_get, fut);
    }

    public void Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs)
    {
        // delivery_report(err, msg) → callback(msg, None) / callback(None, exc), on the poll thread at the
        // broker ack (Python CompatibleProducer.send with a callback).
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
            // Bailed out on termination before Produce accepted the record: no delivery report will come, so
            // this is a send that threw (Python's future here is never resolved and its callback never runs).
            throw new OperationCanceledException("Terminated before the producer accepted the record.");
        }
    }

    // Python's CompatibleProducer.flush drives every pending delivery; ckd's Flush serves the remaining
    // delivery reports alongside the poll thread.
    public void Flush(CancellationToken cancellationToken) => _producer.Flush(cancellationToken);

    // Returns whether the producer accepted the record.
    private bool Produce(string topic, byte[]? key, byte[]? value, Action<DeliveryReport<byte[], byte[]>> deliveryReport)
    {
        // ckd's Message.Key/Value are byte[] (not nullable-annotated) but accept null at runtime (a null
        // key = no key, a null value = tombstone), so the null-forgiving operator passes the value through
        // while suppressing CS8601 — the correct interop idiom for ckd's un-annotated surface.
        var message = new Message<byte[], byte[]> { Key = key!, Value = value! };

        // while not terminating: produce(...); break / except BufferError: time.sleep(0.001). ckd reports
        // Python's BufferError as a QUEUE_FULL ProduceException. The termination check on every attempt
        // keeps a shutdown during a sustained QUEUE_FULL condition (e.g. an unreachable broker) from
        // spinning forever, so the process exits promptly on Ctrl+C/SIGTERM.
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
        // CompatibleProducer.close does not flush; this keeps a flush for any residual (the engine has
        // already flushed and waited every send's delivery, so normally nothing is pending). Then stop and
        // join the poll thread (closed=True; thread.join()).
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
                // Best-effort: a transient poll error must not kill the delivery pump (the harness
                // contract — a per-callback error is surfaced through the send's TCS, not here).
            }
        }
    }
}

/// <summary>
/// Adapts ckd's <see cref="IProducer{TKey, TValue}"/> to the async (pipelined)
/// <see cref="IAsyncProducerBackend"/>. ckd has <b>no</b> AIO-style async producer (Python's async v2 wraps
/// <c>confluent_kafka.aio.AIOProducer</c>); the async v2 backend therefore <b>wraps <c>ProduceAsync</c></b>
/// (documented deviation — M13/P1 PLAN §1.2). The measured send reports the <see cref="Task"/>'s outcome to
/// the engine's callback, as Python's <c>async_main</c> does with <c>add_done_callback</c>; the warmup send
/// hands the <see cref="Task"/> back. ckd serves <c>ProduceAsync</c> delivery reports on its own internal
/// poll, so no background poll thread is needed on this path (unlike the sync
/// <see cref="V2SyncProducerBackend"/>).
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
        // ProduceAsync enqueues synchronously (preserving send order) then completes on delivery. A
        // full-queue / delivery failure throws (ProduceException) — because this method is `async`, that
        // throw is captured into the returned Task, and the engine's warmup catches it.
        // Key/Value null-forgiving: ckd's Message.Key/Value accept null at runtime but are not
        // nullable-annotated (see the sync backend note).
        DeliveryResult<byte[], byte[]> result = await _producer
            .ProduceAsync(topic, new Message<byte[], byte[]> { Key = key!, Value = value! })
            .ConfigureAwait(false);
        return new PerfRecordMetadata(
            result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Timestamp.UnixTimestampMs);
    }

    public ValueTask Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs)
    {
        // As the warmup send: ProduceAsync has no separate accepted stage, so this is always complete and
        // every failure, a full queue included, reaches the callback through the delivery task.
        _producer
            .ProduceAsync(topic, new Message<byte[], byte[]> { Key = key!, Value = value! })
            .ContinueWith(s_report, new DeliveryState(callback, sendStartMs), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return default;
    }

    // ckd's Flush is synchronous; it serves the remaining delivery reports on the calling thread.
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
