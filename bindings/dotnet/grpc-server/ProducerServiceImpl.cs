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
/// Maps the 6 <c>ProducerService</c> RPCs onto the binding's synchronous
/// <see cref="KafkaProducer{TKey, TValue}"/> / <see cref="MockProducer{TKey, TValue}"/>
/// (both <c>&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>) — the .NET port
/// of <c>grpc_server.py</c>'s <c>ProducerService</c> half (M12/P1). Each RPC resolves a
/// server-local <c>producer_id</c>, calls the matching sync binding method on the gRPC
/// handler thread (Python's model — the blocking <c>Send</c> parks the handler thread; no
/// <c>Task.Run</c>), and maps the result into the proto response, translating any operational
/// <see cref="KafkaException"/> via <see cref="Translate"/>. Bridging through
/// <c>&lt;byte[], byte[]&gt;</c> + <see cref="Serdes.ByteArray"/> exercises the shipped generic
/// serialize path end-to-end over the wire.
/// </summary>
/// <remarks>
/// <para>
/// <b>No per-id gate (PLAN §4 — divergence from the consumer).</b> Unlike
/// <c>ConsumerServiceImpl</c> (whose native consumer is single-owner / not thread-safe, so it
/// takes a per-id gate), the producer is thread-safe: the core's internal <c>Mutex</c>
/// serializes concurrent <c>Send</c> (<c>ffi-marshalling.md §A1</c> — "don't add your own
/// lock"). So this servicer needs ONLY a thread-safe
/// <see cref="ConcurrentDictionary{TKey, TValue}"/> id -&gt; producer map (plus an
/// <see cref="Interlocked"/> id counter) to make <c>CreateProducer</c> / <c>Close</c> races
/// safe — and NO per-op <c>lock</c> / <see cref="SemaphoreSlim"/>.
/// </para>
/// <para>
/// <b>Singleton (PLAN §3).</b> The servicer MUST be registered as a singleton (Program.cs):
/// it owns the id -&gt; producer map every RPC shares (a <c>CreateProducer</c> id must be
/// resolvable by the following <c>Send</c> / <c>Flush</c> / ...). Mirrors the consumer servicer
/// and Python's single-servicer-instance model.
/// </para>
/// <para>
/// <b><c>CloseTimeout</c> ignores <c>timeout_ms</c> (PLAN §2/§5).</b> The .NET producer has no
/// timed close at any layer (<c>Producer_close</c> takes no timeout; <see cref="IProducer{TKey, TValue}"/>
/// has only <c>Close()</c>), so <see cref="CloseTimeout"/> delegates to the plain
/// <see cref="Close"/> — a faithful port of the Python server and behaviorally invisible to the
/// harness (no <c>multilanguage_test!</c> scenario exercises <c>close_timeout</c>)
/// </para>
/// </remarks>
internal sealed class ProducerServiceImpl : Proto.ProducerService.ProducerServiceBase
{
    private readonly ConcurrentDictionary<ulong, IProducer<byte[], byte[]>> _producers =
        new ConcurrentDictionary<ulong, IProducer<byte[], byte[]>>();

    private long _nextId;

    /// <inheritdoc/>
    public override Task<Proto.CreateProducerResponse> CreateProducer(Proto.CreateProducerRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        IProducer<byte[], byte[]> producer;
        try
        {
            // Empty (or all-blank) config selects a broker-free MockProducer (Python parity,
            // grpc_server.py). Otherwise a real KafkaProducer.
            if (IsEmptyConfig(config))
            {
                producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            }
            else
            {
                producer = new KafkaProducer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
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

    /// <inheritdoc/>
    public override Task<Proto.SendResponse> Send(Proto.SendRequest request, ServerCallContext context)
    {
        IProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return Task.FromResult(new Proto.SendResponse { Error = Translate.UnknownProducer(request.ProducerId) });
        }

        // with_callback is a hint only — the callback closure stays Rust-side; the unary
        // response IS the resolved future (producer_service.proto). Nothing to do here.
        try
        {
            ProducerRecord<byte[], byte[]> record = Translate.ProducerRecordFromProto(request.Record);
            // BLOCKS on the handler thread until the producer's future resolves (Java
            // send(record).get(); the sync-consumer-poll precedent). No Task.Run.
            RecordMetadata metadata = producer.Send(record);
            return Task.FromResult(new Proto.SendResponse { Metadata = Translate.MetadataToProto(metadata) });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.SendResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Flush(Proto.FlushRequest request, ServerCallContext context)
    {
        IProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.UnknownProducer(request.ProducerId) });
        }

        try
        {
            producer.Flush();
            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.PartitionsForResponse> PartitionsFor(Proto.PartitionsForRequest request, ServerCallContext context)
    {
        IProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return Task.FromResult(new Proto.PartitionsForResponse { Error = Translate.UnknownProducer(request.ProducerId) });
        }

        try
        {
            Proto.PartitionsForResponse response = new Proto.PartitionsForResponse();
            IReadOnlyList<PartitionInfo> infos = producer.PartitionsFor(request.Topic);
            foreach (PartitionInfo info in infos)
            {
                response.Partitions.Add(Translate.PartitionInfoToProto(info));
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.PartitionsForResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Close(Proto.CloseRequest request, ServerCallContext context)
    {
        if (!_producers.TryRemove(request.ProducerId, out IProducer<byte[], byte[]>? producer))
        {
            // Close is idempotent — silent success on an unknown id (Python / Java parity).
            return Task.FromResult(new Proto.StatusResponse());
        }

        try
        {
            // The binding's Close() gracefully closes AND releases the native handle
            // (Producer_close -> Producer_destroy), so no separate Dispose is needed.
            producer.Close();
            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> CloseTimeout(Proto.CloseTimeoutRequest request, ServerCallContext context) =>
        // The .NET producer has no timed close (PLAN §2/§5): ignore timeout_ms and delegate to
        // the plain Close() — a faithful port of grpc_server.py's CloseTimeout, behaviorally
        // invisible to the harness (no scenario exercises close_timeout).
        Close(new Proto.CloseRequest { ProducerId = request.ProducerId }, context);

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

    private IProducer<byte[], byte[]>? Get(ulong producerId) =>
        _producers.TryGetValue(producerId, out IProducer<byte[], byte[]>? producer) ? producer : null;
}
