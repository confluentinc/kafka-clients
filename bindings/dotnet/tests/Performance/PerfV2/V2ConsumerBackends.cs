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

namespace Confluent.Kafka.Performance.V2;

// ckd's types (IConsumer, ConsumerBuilder, ConsumeResult, ...) live in the `Confluent.Kafka` namespace,
// an ANCESTOR of this file's `Confluent.Kafka.Performance.V2` namespace, so they are in scope with no
// `using Confluent.Kafka;` (same as the V2 producer backends / the PerfV3 precedent).

/// <summary>
/// Adapts ckd's <see cref="IConsumer{TKey, TValue}"/> to the sync <see cref="IConsumerBackend"/> — the .NET
/// analog of Python's <c>_LibrdkafkaConsumer</c>. ckd's <c>Consume(timeout)</c> returns <b>one</b> message
/// per call (there is no many-at-once batch consume in ckd), so <see cref="PollBatch"/> <b>loops</b>
/// <c>Consume</c> up to the batch size to approximate v3's batch <c>Poll</c> (documented adapter difference
/// — PLAN §1.2). The first <c>Consume</c> blocks up to the poll timeout; the rest drain non-blocking
/// (<c>TimeSpan.Zero</c>) so a batch never costs <c>batchSize × timeout</c>. Bytes flow over ckd's built-in
/// <c>Deserializers.ByteArray</c> (ckd auto-selects it for <c>&lt;byte[], byte[]&gt;</c>).
/// </summary>
internal sealed class V2SyncConsumerBackend : IConsumerBackend
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly TimeSpan _timeout;
    private readonly int _batch;

    internal V2SyncConsumerBackend(IReadOnlyDictionary<string, string> config, int pollTimeoutMs, int batchSize)
    {
        _consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
        _timeout = TimeSpan.FromMilliseconds(pollTimeoutMs);
        _batch = batchSize;
    }

    public void Subscribe(string topic) => _consumer.Subscribe(topic);

    public bool Assigned()
    {
        try
        {
            return _consumer.Assignment.Count > 0;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public IReadOnlyList<PolledRecord> PollBatch()
    {
        var list = new List<PolledRecord>();

        // Approximate v3's batch Poll: the first Consume blocks up to the poll timeout for a record; the
        // rest drain whatever is already buffered without blocking, up to the batch size. ckd has no
        // many-at-once consume, so this loop-Consume is the closest equivalent.
        TimeSpan timeout = _timeout;
        for (int i = 0; i < _batch; i++)
        {
            ConsumeResult<byte[], byte[]> result = _consumer.Consume(timeout);
            timeout = TimeSpan.Zero;

            // A null result (nothing within the timeout) or a partition-EOF marker carries no record —
            // stop draining this batch (Python skips None / error results).
            if (result is null || result.IsPartitionEOF || result.Message is null)
            {
                break;
            }

            Message<byte[], byte[]> message = result.Message;
            int nbytes = (message.Value?.Length ?? 0) + (message.Key?.Length ?? 0);
            list.Add(new PolledRecord(message.Timestamp.UnixTimestampMs, nbytes));
        }

        return list;
    }

    // POLL_SINGLE: consume one message at a time (Python _LibrdkafkaConsumer.poll_single).
    public IReadOnlyList<PolledRecord> PollSingle()
    {
        ConsumeResult<byte[], byte[]> result = _consumer.Consume(_timeout);
        if (result is null || result.IsPartitionEOF || result.Message is null)
        {
            return Array.Empty<PolledRecord>();
        }

        Message<byte[], byte[]> message = result.Message;
        int nbytes = (message.Value?.Length ?? 0) + (message.Key?.Length ?? 0);
        return new[] { new PolledRecord(message.Timestamp.UnixTimestampMs, nbytes) };
    }

    public void Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();
}

/// <summary>
/// Adapts ckd's sync <see cref="IConsumer{TKey, TValue}"/> to the async <see cref="IAsyncConsumerBackend"/>.
/// ckd has <b>no</b> async consumer (no <c>AIOConsumer</c> analog — Python's async v2 wraps
/// <c>confluent_kafka.aio.AIOConsumer</c>), so the async v2 consumer backend wraps the sync
/// <see cref="V2SyncConsumerBackend"/>, offloading each blocking poll to a worker thread via
/// <see cref="Task.Run{TResult}(Func{TResult})"/> (documented deviation — the manual-comparison async v2
/// baseline, analogous to the producer's ProduceAsync-as-async note). The assignment read stays sync
/// (a non-blocking local read).
/// </summary>
internal sealed class V2AsyncConsumerBackend : IAsyncConsumerBackend
{
    private readonly V2SyncConsumerBackend _inner;

    internal V2AsyncConsumerBackend(IReadOnlyDictionary<string, string> config, int pollTimeoutMs, int batchSize)
    {
        _inner = new V2SyncConsumerBackend(config, pollTimeoutMs, batchSize);
    }

    public Task Subscribe(string topic)
    {
        _inner.Subscribe(topic);
        return Task.CompletedTask;
    }

    public bool Assigned() => _inner.Assigned();

    public Task<IReadOnlyList<PolledRecord>> PollBatch() => Task.Run(() => _inner.PollBatch());

    public Task<IReadOnlyList<PolledRecord>> PollSingle() => Task.Run(() => _inner.PollSingle());

    public Task Close()
    {
        _inner.Close();
        return Task.CompletedTask;
    }

    public void Dispose() => _inner.Dispose();

    public ValueTask DisposeAsync()
    {
        _inner.Dispose();
        return default;
    }
}
