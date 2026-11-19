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
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// Hands one record's delivery from the binding's <see cref="ICallback"/> to the engine's
/// <see cref="IPerfDeliveryCallback"/>, with the send's start time; one per measured send.
/// </summary>
internal sealed class V3Delivery : ICallback
{
    private readonly IPerfDeliveryCallback _callback;
    private readonly long _sendStartTicks;

    internal V3Delivery(IPerfDeliveryCallback callback, long sendStartTicks)
    {
        _callback = callback;
        _sendStartTicks = sendStartTicks;
    }

    public void OnCompletion(RecordMetadata metadata, Exception? exception) =>
        _callback.OnCompletion(
            _sendStartTicks,
            new PerfRecordMetadata(metadata.Topic(), metadata.Partition(), metadata.Offset(), metadata.Timestamp()),
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
                if (kv.Key.Name() == "compression-rate-avg")
                {
                    Console.WriteLine($"[METRIC] compression-rate-avg = {FormatMetricValue(kv.Value.MetricValue())}");
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
        return new PerfRecordMetadata(meta.Topic(), meta.Partition(), meta.Offset(), meta.Timestamp());
    };

    private readonly KafkaProducer<byte[], byte[]> _producer;

    internal V3SyncProducerBackend(IReadOnlyDictionary<string, string> config)
    {
        _producer = KafkaProducer<byte[], byte[]>.Create(V3Config.ClientConfigs(config), Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
    }

    public PerfSendHandle Send(string topic, byte[]? key, byte[]? value) =>
        new PerfSendHandle(s_get, _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value: value, key: key)));

    public void Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartTicks) =>
        _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value: value, key: key), new V3Delivery(callback, sendStartTicks));

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

    private V3AsyncProducerBackend(AsyncKafkaProducer<byte[], byte[]> producer) => _producer = producer;

    internal static async Task<V3AsyncProducerBackend> Create(IReadOnlyDictionary<string, string> config) =>
        new V3AsyncProducerBackend(
            await AsyncKafkaProducer<byte[], byte[]>
                .Create(V3Config.ClientConfigs(config), new AsyncByteArraySerializer(), new AsyncByteArraySerializer())
                .ConfigureAwait(false));

    public ValueTask<Task<PerfRecordMetadata>> Send(string topic, byte[]? key, byte[]? value)
    {
        // Send hands the record over synchronously, preserving send order; its first stage is usually complete on
        // return, so the common path builds no state machine.
        ValueTask<AsyncKafkaFuture<RecordMetadata>> send = _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value: value, key: key));
        return send.IsCompletedSuccessfully
            ? new ValueTask<Task<PerfRecordMetadata>>(Deliver(send.Result.Get()))
            : AwaitAccepted(send);
    }

    private static async ValueTask<Task<PerfRecordMetadata>> AwaitAccepted(ValueTask<AsyncKafkaFuture<RecordMetadata>> send) =>
        Deliver((await send.ConfigureAwait(false)).Get());

    private static async Task<PerfRecordMetadata> Deliver(Task<RecordMetadata> delivery)
    {
        RecordMetadata meta = await delivery.ConfigureAwait(false);
        return new PerfRecordMetadata(meta.Topic(), meta.Partition(), meta.Offset(), meta.Timestamp());
    }

    public ValueTask Send(string topic, byte[]? key, byte[]? value, IPerfDeliveryCallback callback, long sendStartTicks)
    {
        // The ValueTask is consumed, as it must be, and dropped: the callback reports the delivery.
        ValueTask<AsyncKafkaFuture<RecordMetadata>> send =
            _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value: value, key: key), new V3Delivery(callback, sendStartTicks));
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
/// What both consumer arms read from a record: its key and value are memories over the fetched batch, copying
/// nothing, as Python's Rust-core arm reads memoryviews over the batch. A null key or value counts as 0 bytes.
/// Under <c>DO_VERIFY=True</c> the verifier reads the bytes of both in place and checks the partition.
/// </summary>
internal static class PolledBytes
{
    internal static PolledRecord Read(ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record, PolledRecordVerifier? verifier)
    {
        ReadOnlyMemory<byte> key = record.Key().GetValueOrDefault();
        ReadOnlyMemory<byte> value = record.Value().GetValueOrDefault();
        verifier?.Verify(record.Partition(), key.Span, value.Span);
        return new PolledRecord(record.Timestamp(), key.Length + value.Length);
    }
}

/// <summary>
/// Adapts our binding's <see cref="KafkaConsumer{TKey, TValue}"/> to the sync <see cref="IConsumerBackend"/>,
/// normalizing each polled record to <see cref="PolledRecord"/> (timestamp + value/key length). Keys and values
/// come from the built-in <see cref="ReadOnlyMemoryDeserializer"/>.
/// </summary>
internal sealed class V3SyncConsumerBackend : IConsumerBackend
{
    private readonly KafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> _consumer;
    private readonly TimeSpan _timeout;
    private readonly PolledRecordVerifier? _verifier;

    internal V3SyncConsumerBackend(
        IReadOnlyDictionary<string, string> config, int pollTimeoutMs, PolledRecordVerifier? verifier)
    {
        _consumer = KafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>.Create(
            V3Config.ClientConfigs(config), new ReadOnlyMemoryDeserializer(), new ReadOnlyMemoryDeserializer());
        _timeout = TimeSpan.FromMilliseconds(pollTimeoutMs);
        _verifier = verifier;
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
        ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> records = _consumer.Poll(_timeout);
        var list = new List<PolledRecord>(records.Count());
        foreach (ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> r in records)
        {
            list.Add(PolledBytes.Read(r, _verifier));
        }

        return list;
    }

    // The binding has no single-message poll, so POLL_SINGLE uses the batch poll, as Python's does.
    public IReadOnlyList<PolledRecord> PollSingle() => PollBatch();

    public void Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();
}

/// <summary>
/// Adapts this binding's <see cref="AsyncKafkaConsumer{TKey, TValue}"/> to the async
/// <see cref="IAsyncConsumerBackend"/>: subscribe, poll and close are tasks; the assignment read is sync.
/// Keys and values come from the built-in <see cref="AsyncReadOnlyMemoryDeserializer"/>.
/// </summary>
internal sealed class V3AsyncConsumerBackend : IAsyncConsumerBackend
{
    private readonly AsyncKafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> _consumer;
    private readonly TimeSpan _timeout;
    private readonly PolledRecordVerifier? _verifier;

    private V3AsyncConsumerBackend(
        AsyncKafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer, int pollTimeoutMs, PolledRecordVerifier? verifier)
    {
        _consumer = consumer;
        _timeout = TimeSpan.FromMilliseconds(pollTimeoutMs);
        _verifier = verifier;
    }

    internal static async Task<V3AsyncConsumerBackend> Create(
        IReadOnlyDictionary<string, string> config, int pollTimeoutMs, PolledRecordVerifier? verifier) =>
        new V3AsyncConsumerBackend(
            await AsyncKafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>
                .Create(
                    V3Config.ClientConfigs(config),
                    new AsyncReadOnlyMemoryDeserializer(),
                    new AsyncReadOnlyMemoryDeserializer())
                .ConfigureAwait(false),
            pollTimeoutMs,
            verifier);

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
        ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> records =
            await _consumer.Poll(_timeout).ConfigureAwait(false);
        var list = new List<PolledRecord>(records.Count());
        foreach (ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> r in records)
        {
            list.Add(PolledBytes.Read(r, _verifier));
        }

        return list;
    }

    public Task<IReadOnlyList<PolledRecord>> PollSingle() => PollBatch();

    public Task Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();

    public ValueTask DisposeAsync() => _consumer.DisposeAsync();
}
