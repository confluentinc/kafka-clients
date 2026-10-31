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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// Hands one record's delivery from the binding's <see cref="IDeliveryCallback"/> to the engine's
/// <see cref="IPerfDeliveryCallback"/>, with the send's start time; one per measured send.
/// </summary>
internal sealed class V3Delivery : IDeliveryCallback
{
    private readonly IPerfDeliveryCallback _callback;
    private readonly long _sendStartMs;

    internal V3Delivery(IPerfDeliveryCallback callback, long sendStartMs)
    {
        _callback = callback;
        _sendStartMs = sendStartMs;
    }

    public void OnCompletion(RecordMetadata metadata, Exception? exception) =>
        _callback.OnCompletion(
            _sendStartMs,
            new PerfRecordMetadata(metadata.Topic, metadata.Partition, metadata.Offset, metadata.Timestamp),
            exception);
}

/// <summary>Producer-metric prints shared by the two v3 producer backends.</summary>
internal static class V3ProducerMetrics
{
    internal static void PrintCompressionRateAvg(Func<IReadOnlyDictionary<MetricName, IMetric>> metrics)
    {
        try
        {
            foreach (KeyValuePair<MetricName, IMetric> kv in metrics())
            {
                if (kv.Key.Name == "compression-rate-avg")
                {
                    Console.WriteLine($"[METRIC] compression-rate-avg = {FormatMetricValue(kv.Value.Value)}");
                }
            }
        }
        catch (Exception e)
        {
            // Diagnostic only: it must not stop the producer from being closed after it.
            Console.WriteLine($"Producer metrics read failed: {e.Message}");
        }
    }

    // As Java's harness prints it (Double.toString): an integral value keeps its ".0".
    private static string FormatMetricValue(object value)
    {
        if (value is double d)
        {
            string s = d.ToString("R", CultureInfo.InvariantCulture);
            return double.IsNaN(d) || double.IsInfinity(d) || s.IndexOfAny(new[] { '.', 'E' }) >= 0 ? s : s + ".0";
        }

        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }
}

/// <summary>
/// Adapts this binding's <see cref="KafkaProducer{TKey, TValue}"/> to the sync <see cref="IProducerBackend"/>: the
/// measured send is Java's <c>send(record, callback)</c>, and the warmup send's handle waits on its
/// <see cref="KafkaFuture{T}"/>, as Java's <c>future.get()</c>.
/// </summary>
internal sealed class V3SyncProducerBackend : IProducerBackend
{
    // Static, so a send allocates no closure; the KafkaFuture rides in the handle's state.
    private static readonly Func<object, PerfRecordMetadata> s_get = static state =>
    {
        RecordMetadata meta = ((KafkaFuture<RecordMetadata>)state).Get();
        return new PerfRecordMetadata(meta.Topic, meta.Partition, meta.Offset, meta.Timestamp);
    };

    private readonly KafkaProducer<byte[], byte[]> _producer;

    internal V3SyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
    }

    public PerfSendHandle Send(string topic, byte[]? key, byte[]? value) =>
        new PerfSendHandle(s_get, _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value, key)));

    public void Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs) =>
        _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value, key), new V3Delivery(callback, sendStartMs));

    public void Flush(CancellationToken cancellationToken) => _producer.Flush();

    internal void PrintCompressionRateAvg() => V3ProducerMetrics.PrintCompressionRateAvg(_producer.Metrics);

    public void Close() => _producer.Close();

    public void Dispose() => _producer.Dispose();
}

/// <summary>
/// Adapts this binding's <see cref="AsyncKafkaProducer{TKey, TValue}"/> to the async
/// <see cref="IAsyncProducerBackend"/>: a send completes when the producer has accepted the record; the measured send
/// reports to the binding's callback, and the warmup send returns the delivery <see cref="Task"/>.
/// </summary>
internal sealed class V3AsyncProducerBackend : IAsyncProducerBackend
{
    private readonly AsyncKafkaProducer<byte[], byte[]> _producer;

    internal V3AsyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
    }

    public ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value)
    {
        // Send hands the record over synchronously, preserving send order; its first stage is usually complete on
        // return, so the common path builds no state machine.
        ValueTask<AsyncKafkaFuture<RecordMetadata>> send = _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value, key));
        return send.IsCompletedSuccessfully
            ? new ValueTask<Task<PerfRecordMetadata>>(Deliver(send.Result.Get()))
            : AwaitAccepted(send);
    }

    private static async ValueTask<Task<PerfRecordMetadata>> AwaitAccepted(ValueTask<AsyncKafkaFuture<RecordMetadata>> send) =>
        Deliver((await send.ConfigureAwait(false)).Get());

    private static async Task<PerfRecordMetadata> Deliver(Task<RecordMetadata> delivery)
    {
        RecordMetadata meta = await delivery.ConfigureAwait(false);
        return new PerfRecordMetadata(meta.Topic, meta.Partition, meta.Offset, meta.Timestamp);
    }

    public ValueTask Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartMs)
    {
        // The ValueTask is consumed, as it must be, and dropped: the callback reports the delivery.
        ValueTask<AsyncKafkaFuture<RecordMetadata>> send =
            _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value, key), new V3Delivery(callback, sendStartMs));
        if (send.IsCompletedSuccessfully)
        {
            _ = send.Result;
            return default;
        }

        return AwaitAcceptance(send);
    }

    private static async ValueTask AwaitAcceptance(ValueTask<AsyncKafkaFuture<RecordMetadata>> send) =>
        await send.ConfigureAwait(false);

    public Task Flush(CancellationToken cancellationToken) => _producer.Flush(cancellationToken);

    internal void PrintCompressionRateAvg() => V3ProducerMetrics.PrintCompressionRateAvg(_producer.Metrics);

    public Task Close() => _producer.Close();

    public void Dispose() => _producer.Dispose();

    public ValueTask DisposeAsync() => _producer.DisposeAsync();
}

/// <summary>
/// Adapts our binding's <see cref="KafkaConsumer{TKey, TValue}"/> to the sync <see cref="IConsumerBackend"/>,
/// normalizing each polled record to <see cref="PolledRecord"/> (timestamp + value/key length).
/// </summary>
internal sealed class V3SyncConsumerBackend : IConsumerBackend
{
    private readonly KafkaConsumer<byte[], byte[]> _consumer;
    private readonly TimeSpan _timeout;

    internal V3SyncConsumerBackend(IReadOnlyDictionary<string, string> config, int pollTimeoutMs)
    {
        _consumer = new KafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
        _timeout = TimeSpan.FromMilliseconds(pollTimeoutMs);
    }

    public void Subscribe(string topic) => _consumer.Subscribe(new[] { topic });

    public bool Assigned()
    {
        try
        {
            return _consumer.Assignment().Count > 0;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public IReadOnlyList<PolledRecord> PollBatch()
    {
        ConsumerRecords<byte[], byte[]> records = _consumer.Poll(_timeout);
        var list = new List<PolledRecord>(records.Count);
        foreach (ConsumerRecord<byte[], byte[]> r in records)
        {
            list.Add(new PolledRecord(r.Timestamp, PolledRecordBytes(r)));
        }

        return list;
    }

    // The binding has no single-message poll, so POLL_SINGLE uses the batch poll, as Python's does.
    public IReadOnlyList<PolledRecord> PollSingle() => PollBatch();

    public void Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();

    internal static int PolledRecordBytes(ConsumerRecord<byte[], byte[]> r) =>
        (r.Value?.Length ?? 0) + (r.Key?.Length ?? 0);
}

/// <summary>
/// Adapts this binding's <see cref="AsyncKafkaConsumer{TKey, TValue}"/> to the async
/// <see cref="IAsyncConsumerBackend"/>: subscribe, poll and close are tasks; the assignment read is sync.
/// </summary>
internal sealed class V3AsyncConsumerBackend : IAsyncConsumerBackend
{
    private readonly AsyncKafkaConsumer<byte[], byte[]> _consumer;
    private readonly TimeSpan _timeout;

    internal V3AsyncConsumerBackend(IReadOnlyDictionary<string, string> config, int pollTimeoutMs)
    {
        _consumer = new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
        _timeout = TimeSpan.FromMilliseconds(pollTimeoutMs);
    }

    public Task Subscribe(string topic) => _consumer.Subscribe(new[] { topic });

    public bool Assigned()
    {
        try
        {
            return _consumer.Assignment().Count > 0;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<PolledRecord>> PollBatch()
    {
        ConsumerRecords<byte[], byte[]> records = await _consumer.Poll(_timeout).ConfigureAwait(false);
        var list = new List<PolledRecord>(records.Count);
        foreach (ConsumerRecord<byte[], byte[]> r in records)
        {
            list.Add(new PolledRecord(r.Timestamp, V3SyncConsumerBackend.PolledRecordBytes(r)));
        }

        return list;
    }

    public Task<IReadOnlyList<PolledRecord>> PollSingle() => PollBatch();

    public Task Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();

    public ValueTask DisposeAsync() => _consumer.DisposeAsync();
}
