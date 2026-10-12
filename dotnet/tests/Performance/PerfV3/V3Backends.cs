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
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// Adapts our binding's <see cref="KafkaProducer{TKey, TValue}"/> to the sync <see cref="IProducerBackend"/>:
/// <see cref="Send"/> serializes and sends, returning once the producer has accepted the record (Java
/// <c>send(record)</c>) with the <see cref="KafkaFuture{T}"/> wrapped in a <see cref="PerfSendHandle"/>, whose
/// <c>Get()</c> the engine's recorder calls (Java <c>future.get()</c>). Bytes go over
/// <c>&lt;byte[], byte[]&gt;</c> + <see cref="Serdes.ByteArray"/> (generic-only surface, M11/P5).
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

    public void Close() => _producer.Close();

    public void Dispose() => _producer.Dispose();
}

/// <summary>
/// Adapts our binding's <see cref="AsyncKafkaProducer{TKey, TValue}"/> to the async (pipelined)
/// <see cref="IAsyncProducerBackend"/>: <see cref="Send"/> completes when the producer has accepted the
/// record and hands back the delivery <see cref="Task"/> the engine's recorder awaits.
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
        // The Send call hands the record to the producer synchronously (preserving send order). Its first
        // stage is usually complete when it returns, so the common path builds no state machine for it.
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

    // POLL_SINGLE has no single-message API in the binding — delegate to the batch poll (documented parity).
    public IReadOnlyList<PolledRecord> PollSingle() => PollBatch();

    public void Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();

    internal static int PolledRecordBytes(ConsumerRecord<byte[], byte[]> r) =>
        (r.Value?.Length ?? 0) + (r.Key?.Length ?? 0);
}

/// <summary>
/// Adapts our binding's <see cref="AsyncKafkaConsumer{TKey, TValue}"/> to the async
/// <see cref="IAsyncConsumerBackend"/>. The blocking-in-Java ops are tasks; <see cref="Assigned"/> stays sync
/// (<c>Assignment()</c> is a non-blocking read on both flavors).
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

    // The async consumer's Close takes only a CancellationToken (no timed variant, CLAUDE.md §1).
    public Task Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();

    public ValueTask DisposeAsync() => _consumer.DisposeAsync();
}
