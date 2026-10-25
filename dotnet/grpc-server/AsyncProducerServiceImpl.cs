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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// The async twin of <see cref="ProducerServiceImpl"/>: serves <c>ProducerService</c> over
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> and <see cref="AsyncMockProducer{TKey, TValue}"/>
/// (<c>&lt;byte[], byte[]&gt;</c>, <see cref="Serdes.ByteArray"/>), the C# port of the producer half of
/// Python's <c>grpc_server_async.py</c>. Handlers await the binding's tasks and take no per-id lock.
/// </summary>
internal sealed class AsyncProducerServiceImpl : Proto.ProducerService.ProducerServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, IAsyncProducer<byte[], byte[]>> _producers =
        new ConcurrentDictionary<ulong, IAsyncProducer<byte[], byte[]>>();

    private readonly CallbackLog _callbackLog = new CallbackLog();

    private readonly TimeSpan _sendTimeout = TimeSpan.FromSeconds(120);

    private long _nextId;

    public void Dispose()
    {
        foreach (KeyValuePair<ulong, IAsyncProducer<byte[], byte[]>> pair in _producers)
        {
            if (!_producers.TryRemove(pair.Key, out IAsyncProducer<byte[], byte[]>? producer))
            {
                continue;
            }

            try
            {
                producer.Dispose();
            }
            catch (Exception)
            {
                // One failing producer must not abort the sweep.
            }
        }
    }

    public override Task<Proto.CreateProducerResponse> CreateProducer(Proto.CreateProducerRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        IAsyncProducer<byte[], byte[]> producer;
        try
        {
            // An empty or all-blank config selects an AsyncMockProducer, as Python's servicer does.
            if (IsEmptyConfig(config))
            {
                producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            }
            else
            {
                producer = new AsyncKafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
            }
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.CreateProducerResponse { ProducerId = 0, Error = Translate.ToProto(ex) });
        }

        ulong id = (ulong)Interlocked.Increment(ref _nextId);
        _producers[id] = producer;
        return Task.FromResult(new Proto.CreateProducerResponse { ProducerId = id });
    }

    public override async Task<Proto.SendResponse> Send(Proto.SendRequest request, ServerCallContext context)
    {
        IAsyncProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return new Proto.SendResponse { Error = Translate.UnknownProducer(request.ProducerId) };
        }

        Task<RecordMetadata>? send = null;
        try
        {
            ProducerRecord<byte[], byte[]> record = Translate.ProducerRecordFromProto(request.Record);

            // The delivery callback runs before the record's future completes, so its entry is
            // logged before the await below resumes. The 120 s bound covers both stages of the
            // send, so a stuck send fails the RPC instead of hanging the scenario.
            send = Delivered(request.WithCallback
                ? producer.Send(record, new LoggingDeliveryCallback(_callbackLog, request.ProducerId))
                : producer.Send(record));

            RecordMetadata metadata = await send.WaitAsync(_sendTimeout).ConfigureAwait(false);
            return new Proto.SendResponse { Metadata = Translate.MetadataToProto(metadata) };
        }
        catch (TimeoutException) when (send is { IsCompleted: false })
        {
            // Only the server's own bound is code 7: a send that failed with a TimeoutException
            // (the core's -5) is completed, and goes out through ToProto.
            return new Proto.SendResponse { Error = Translate.SendTimedOut() };
        }
        catch (Exception ex)
        {
            return new Proto.SendResponse { Error = Translate.ToProto(ex) };
        }
    }

    private static async Task<RecordMetadata> Delivered(ValueTask<AsyncKafkaFuture<RecordMetadata>> send)
    {
        AsyncKafkaFuture<RecordMetadata> future = await send.ConfigureAwait(false);
        return await future.Get().ConfigureAwait(false);
    }

    public override async Task<Proto.StatusResponse> Flush(Proto.FlushRequest request, ServerCallContext context)
    {
        IAsyncProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return new Proto.StatusResponse { Error = Translate.UnknownProducer(request.ProducerId) };
        }

        try
        {
            await producer.Flush().ConfigureAwait(false);
            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.PartitionsForResponse> PartitionsFor(Proto.PartitionsForRequest request, ServerCallContext context)
    {
        IAsyncProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return new Proto.PartitionsForResponse { Error = Translate.UnknownProducer(request.ProducerId) };
        }

        try
        {
            Proto.PartitionsForResponse response = new Proto.PartitionsForResponse();
            IReadOnlyList<PartitionInfo> infos = await producer.PartitionsFor(request.Topic).ConfigureAwait(false);
            foreach (PartitionInfo info in infos)
            {
                response.Partitions.Add(Translate.PartitionInfoToProto(info));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.PartitionsForResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override Task<Proto.MetricsResponse> Metrics(Proto.MetricsRequest request, ServerCallContext context)
    {
        IAsyncProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return Task.FromResult(new Proto.MetricsResponse { Error = Translate.UnknownProducer(request.ProducerId) });
        }

        try
        {
            Proto.MetricList list = new Proto.MetricList();

            foreach (KeyValuePair<MetricName, IMetric> pair in producer.Metrics())
            {
                list.Metrics.Add(Translate.MetricToProto(pair.Key, pair.Value));
            }

            return Task.FromResult(new Proto.MetricsResponse { Metrics = list });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.MetricsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.StatusResponse> Close(Proto.CloseRequest request, ServerCallContext context) =>
        CloseProducer(request.ProducerId, null);

    public override Task<Proto.StatusResponse> CloseTimeout(Proto.CloseTimeoutRequest request, ServerCallContext context) =>
        CloseProducer(request.ProducerId, TimeSpan.FromMilliseconds(request.TimeoutMs));

    private async Task<Proto.StatusResponse> CloseProducer(ulong producerId, TimeSpan? timeout)
    {
        if (!_producers.TryRemove(producerId, out IAsyncProducer<byte[], byte[]>? producer))
        {
            // An unknown id closes silently, as in Python.
            return new Proto.StatusResponse();
        }

        try
        {
            await (timeout.HasValue ? producer.Close(timeout.Value) : producer.Close()).ConfigureAwait(false);
            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override Task<Proto.CallbackLogResponse> GetCallbackLog(Proto.ProducerCallbackLogRequest request, ServerCallContext context) =>
        Task.FromResult(_callbackLog.Response(request.ProducerId));

    private static bool IsEmptyConfig(IReadOnlyDictionary<string, string> config)
    {
        if (config.Count == 0)
        {
            return true;
        }

        foreach (string value in config.Values)
        {
            if (!string.IsNullOrEmpty(value))
            {
                return false;
            }
        }

        return true;
    }

    private IAsyncProducer<byte[], byte[]>? Get(ulong producerId) =>
        _producers.TryGetValue(producerId, out IAsyncProducer<byte[], byte[]>? producer) ? producer : null;
}
