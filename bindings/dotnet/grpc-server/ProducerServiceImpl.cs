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
/// Maps the 8 <c>ProducerService</c> RPCs onto the binding's synchronous
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
/// harness (no <c>multilanguage_test!</c> scenario exercises <c>close_timeout</c>).
/// </para>
/// </remarks>
internal sealed class ProducerServiceImpl : Proto.ProducerService.ProducerServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, IProducer<byte[], byte[]>> _producers =
        new ConcurrentDictionary<ulong, IProducer<byte[], byte[]>>();

    /// <summary>
    /// The per-producer-id delivery-callback log served by <see cref="GetCallbackLog"/> (M14/P2).
    /// Owned by the SERVICER, never by the <c>id -&gt; producer</c> entry: <see cref="Close"/>
    /// <c>TryRemove</c>s that entry, so a log hung off it would vanish exactly when the proto's
    /// "entries survive Close" contract needs it (<c>CallbackLog</c> remarks). It carries its own
    /// lock; the thread topology that lock exists for is recorded in those same remarks.
    /// </summary>
    private readonly CallbackLog _callbackLog = new CallbackLog();

    private long _nextId;

    /// <summary>
    /// Drains the producer registry: remove each remaining entry and dispose its producer
    /// (M11/P8, Minor 14 — the producer twin of <c>ConsumerServiceImpl.Dispose</c>). The
    /// <c>id -&gt; producer</c> map is emptied only by the <c>Close</c> RPC, and this backend
    /// process is SHARED across scenarios, so any scenario that skips <c>Close</c> leaves a live
    /// native producer (tokio runtime + Sender task + send-pump thread) behind for the rest of the
    /// run. Each teardown is individually guarded so one failing producer cannot abort the sweep
    /// and strand the rest. Idempotent and safe on an empty registry.
    /// </summary>
    public void Dispose()
    {
        // Remove-then-dispose per entry so a concurrent Close RPC and this sweep cannot both claim
        // the same producer (Dispose is itself idempotent, but the sweep should not have to rely on
        // that).
        foreach (KeyValuePair<ulong, IProducer<byte[], byte[]>> pair in _producers)
        {
            if (!_producers.TryRemove(pair.Key, out IProducer<byte[], byte[]>? producer))
            {
                continue;
            }

            try
            {
                // Dispose, not Close: it swallows the close error, which is what a best-effort
                // shutdown sweep wants. It still routes through the graceful Producer_close before
                // Producer_destroy. (The triage said "Close() each entry"; the consumer template
                // deliberately uses Dispose and says why — mirror the template.)
                producer.Dispose();
            }
            catch (Exception)
            {
                // One failing producer must not abort the sweep — the remaining entries still hold
                // live native handles.
            }
        }
    }

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

        // with_callback => register a REAL delivery callback through the binding's own second
        // Send overload (Java's send(record, Callback); M14/P1), and record each invocation in
        // _callbackLog for GetCallbackLog to read. That is what the flag is FOR
        // (producer_service.proto:31-37): the closure the Rust client passes its own producer can
        // only prove the client-side plumbing, so the assertion has to read what THIS binding's
        // callback delivered. Python does exactly this (grpc_server.py:136-139 -> on_delivery=).
        //
        // (This comment used to read "with_callback is a hint only ... Nothing to do here", which
        // is what left test_delivery_callback_logs_metadata__grpc_dotnet red: the RPC below was
        // implemented but the flag was dropped on the floor.)
        try
        {
            ProducerRecord<byte[], byte[]> record = Translate.ProducerRecordFromProto(request.Record);
            // BLOCKS on the handler thread until the producer's future resolves (Java
            // send(record).get(); the sync-consumer-poll precedent). No Task.Run.
            //
            // This wait is INTENTIONALLY UNBOUNDED (M11/P8 decision D-3 / option S2) — unlike the
            // async servicer, which caps at 120 s, and unlike Python's future.result(timeout=120).
            //
            // Why: the sync surface exposes no cancellation or interruption primitive (the producer
            // has no wakeup(), so IProducer.Send takes no CancellationToken — M11/P4 decision #4).
            // The only way to bound it here is to offload onto a pool thread with Task.Run and
            // WaitAsync, which would (a) break this servicer's stated no-Task.Run contract two lines
            // up, and (b) leave the abandoned pool thread parked until the send resolves anyway —
            // buying a structured error at the cost of the very thread-parking it claims to avoid.
            //
            // Accepted consequence: a permanently stuck send on the SYNC dotnet arm hangs that RPC,
            // and therefore that harness scenario, rather than returning a diagnosable TIMEOUT.
            // Accepted for now; revisit if it is ever observed. This is a recorded deviation from
            // Python and from the async servicer, not an oversight.
            //
            // On the SYNC surface the delivery callback fires INLINE on this handler thread, before
            // Send returns (M14/P1) — so by the time the response below is built, the entry is
            // already in _callbackLog.
            RecordMetadata metadata = request.WithCallback
                ? producer.Send(record, new LoggingDeliveryCallback(_callbackLog, request.ProducerId))
                : producer.Send(record);
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

    /// <summary>
    /// The <c>Metrics</c> RPC — the producer's <c>metrics()</c> snapshot as a proto
    /// <c>MetricList</c> (Python <c>grpc_server.py</c> / C++ <c>server.cc</c> parity). Mirrors
    /// <see cref="ConsumerServiceImpl.Metrics"/> but WITHOUT its <c>lock (entry.Gate)</c>: the
    /// producer is thread-safe (the core's <c>Mutex</c> serializes; ffi §A1 "don't add your own
    /// lock"), so this servicer deliberately has no per-id gate to take.
    /// </summary>
    public override Task<Proto.MetricsResponse> Metrics(Proto.MetricsRequest request, ServerCallContext context)
    {
        IProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return Task.FromResult(new Proto.MetricsResponse { Error = Translate.UnknownProducer(request.ProducerId) });
        }

        try
        {
            Proto.MetricList list = new Proto.MetricList();

            // Metrics() is a SYNCHRONOUS state read on IProducer — call it directly.
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

    /// <summary>
    /// Reads (without clearing) the producer's delivery-callback log (M14/P2) — the producer twin
    /// of <c>ConsumerServiceImpl.GetCallbackLog</c>, and the RPC whose absence made both
    /// <c>test_delivery_callback_logs_metadata__grpc_dotnet[_async]</c> fail with
    /// <c>Unimplemented</c>: <c>ProducerService</c> declares it
    /// (<c>producer_service.proto:71</c>) but neither .NET producer servicer overrode it, so the
    /// generated base answered.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Takes no lock, because this servicer has none to take.</b> The consumer servicers
    /// document <c>GetCallbackLog</c> as an exemption from their per-id op gate; here there is no
    /// gate at all — the producer is thread-safe and this servicer deliberately adds no per-op
    /// lock (see the type remarks, ffi-marshalling.md §A1). <see cref="CallbackLog"/> carries its
    /// own lock, which is the only synchronization this read needs, and it is what lets the
    /// harness's poll loop alternate <c>Send</c> and <c>GetCallbackLog</c> freely.
    /// </para>
    /// <para>
    /// <b>Readable AFTER <see cref="Close"/>, on purpose.</b> The log outlives the producer
    /// (<see cref="CallbackLog"/> is owned by the servicer, not by the <c>id -&gt; producer</c>
    /// entry <see cref="Close"/> removes), and the entry a delivery callback appends while a
    /// close flushes is exactly the interesting one — Python states the same reason at its own
    /// handler (<c>grpc_server.py:223-227</c>). An unknown or closed <c>producer_id</c> yields an
    /// empty response, not an error: "unknown producer" is simply not an error condition for this
    /// RPC.
    /// </para>
    /// </remarks>
    /// <inheritdoc/>
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

    private IProducer<byte[], byte[]>? Get(ulong producerId) =>
        _producers.TryGetValue(producerId, out IProducer<byte[], byte[]>? producer) ? producer : null;
}
