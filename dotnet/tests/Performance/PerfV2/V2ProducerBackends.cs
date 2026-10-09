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

/// <summary>
/// Adapts ckd's <see cref="IProducer{TKey, TValue}"/> to the sync <see cref="IProducerBackend"/>: a
/// line-for-line port of Python's <c>CompatibleProducer</c>. Neither ckd nor confluent-kafka-python has a sync
/// producer, so "V2 sync" means ckd's fire-and-forget <c>Produce(topic, message, deliveryHandler)</c>, with a
/// background <c>Poll</c> thread serving the delivery reports, and each send's delivery waited in send order
/// by the shared engine's recorder (<see cref="PerfSendHandle.Get"/>), as Python's <c>main</c> waits the
/// <c>Future</c> that <c>CompatibleProducer.send</c> returns. Bytes flow over ckd's built-in
/// <c>Serializers.ByteArray</c> (ckd auto-selects it for <c>&lt;byte[], byte[]&gt;</c>).
/// </summary>
internal sealed class V2SyncProducerBackend : IProducerBackend
{
    // fut.result(): blocks the engine's recorder thread until the delivery report resolves the future.
    private static readonly Func<object, PerfRecordMetadata> s_get = static state =>
        ((TaskCompletionSource<PerfRecordMetadata>)state).Task.GetAwaiter().GetResult();

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly Thread _pollThread;
    private volatile bool _closed;

    internal V2SyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = new ProducerBuilder<byte[], byte[]>(config).Build();

        // Background poll loop serves the fire-and-forget delivery callbacks (Python CompatibleProducer's
        // poll_producer thread). IsBackground so a startup crash still lets the process exit.
        _pollThread = new Thread(PollLoop) { IsBackground = true, Name = "v2-producer-poll" };
        _pollThread.Start();
    }

    public PerfSendHandle Send(string topic, byte[]? key, byte[]? value)
    {
        // fut = Future(). RunContinuationsAsynchronously: the delivery report fires on the poll thread, and
        // completing the TCS there must not run a continuation on that thread. The handle's Get() parks
        // the recorder thread in a synchronous wait, which is released either way.
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

        // ckd's Message.Key/Value are byte[] (not nullable-annotated) but accept null at runtime (a null
        // key = no key, a null value = tombstone), so the null-forgiving operator passes the value through
        // while suppressing CS8601 — the correct interop idiom for ckd's un-annotated surface.
        var message = new Message<byte[], byte[]> { Key = key!, Value = value! };

        // while not terminating: produce(...); break / except BufferError: time.sleep(0.001). ckd reports
        // Python's BufferError as a QUEUE_FULL ProduceException. The termination check on every attempt
        // keeps a shutdown during a sustained QUEUE_FULL condition (e.g. an unreachable broker) from
        // spinning forever, so the process exits promptly on Ctrl+C/SIGTERM.
        bool produced = false;
        while (!PerfSignals.Terminating)
        {
            try
            {
                _producer.Produce(topic, message, deliveryReport);
                produced = true;
                break;
            }
            catch (ProduceException<byte[], byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                Thread.Sleep(1);
            }
        }

        if (!produced)
        {
            // Bailed out on termination before Produce ever accepted the record. Python leaves the
            // Future unresolved here (a latent hang for the recorder that waits it); cancel instead so the
            // recorder's wait returns promptly rather than hanging the shutdown path.
            fut.TrySetCanceled();
        }

        // return fut
        return new PerfSendHandle(s_get, fut);
    }

    public void Close()
    {
        // CompatibleProducer.close does not flush; this keeps a flush for any residual (the engine's recorder
        // has already waited every send's delivery, so normally nothing is pending). Then stop and join the
        // poll thread (closed=True; thread.join()).
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
/// (documented deviation — M13/P1 PLAN §1.2), which returns a <see cref="Task"/> the shared engine's recorder
/// awaits. ckd serves <c>ProduceAsync</c> delivery reports on its own internal poll, so no background poll
/// thread is needed on this path (unlike the sync <see cref="V2SyncProducerBackend"/>).
/// </summary>
internal sealed class V2AsyncProducerBackend : IAsyncProducerBackend
{
    private readonly IProducer<byte[], byte[]> _producer;

    internal V2AsyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = new ProducerBuilder<byte[], byte[]>(config).Build();
    }

    // ckd's ProduceAsync has no separate accepted stage: the record is accepted inside the call, so the
    // first stage is always already complete and every failure surfaces through the delivery task.
    public ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value) =>
        new ValueTask<Task<PerfRecordMetadata>>(Produce(topic, key, value));

    private async Task<PerfRecordMetadata> Produce(string topic, byte[]? key, byte[]? value)
    {
        // ProduceAsync enqueues synchronously (preserving send order) then completes on delivery. A
        // full-queue / delivery failure throws (ProduceException) — because this method is `async`, that
        // throw is captured into the returned Task, and the engine's recorder catches it (its IsQueueFull
        // check matches ckd's "Local: Queue full" reason text).
        // Key/Value null-forgiving: ckd's Message.Key/Value accept null at runtime but are not
        // nullable-annotated (see the sync backend note).
        DeliveryResult<byte[], byte[]> result = await _producer
            .ProduceAsync(topic, new Message<byte[], byte[]> { Key = key!, Value = value! })
            .ConfigureAwait(false);
        return new PerfRecordMetadata(
            result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Timestamp.UnixTimestampMs);
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
