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

/// <summary>
/// Adapts ckd's consumer to the sync <see cref="IConsumerBackend"/>, as Python's <c>_LibrdkafkaConsumer</c>: ckd
/// consumes one message per call, so <see cref="PollBatch"/> blocks for the first and drains the rest without blocking.
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

        TimeSpan timeout = _timeout;
        for (int i = 0; i < _batch; i++)
        {
            ConsumeResult<byte[], byte[]> result;
            try
            {
                result = _consumer.Consume(timeout);
            }
            catch (ConsumeException)
            {
                // A consume error carries no record: skip it and keep draining, as Python skips an error message.
                continue;
            }
            finally
            {
                // In the finally, so only the first Consume blocks, on the skip path too.
                timeout = TimeSpan.Zero;
            }

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

    public IReadOnlyList<PolledRecord> PollSingle()
    {
        ConsumeResult<byte[], byte[]> result;
        try
        {
            result = _consumer.Consume(_timeout);
        }
        catch (ConsumeException)
        {
            // A consume error carries no record: skip it, as Python's poll_single does.
            return Array.Empty<PolledRecord>();
        }

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
/// Adapts ckd's consumer to the async <see cref="IAsyncConsumerBackend"/>: ckd has no async consumer (Python's v2
/// wraps <c>AIOConsumer</c>), so each poll runs the sync backend's on a worker thread, a deliberate deviation.
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
