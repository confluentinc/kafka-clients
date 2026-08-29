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
/// The <b>async</b> twin of <see cref="ProducerServiceImpl"/> (M12/P1): maps the 7
/// <c>ProducerService</c> RPCs onto the binding's <em>asynchronous</em>
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> / <see cref="AsyncMockProducer{TKey, TValue}"/>
/// (both <c>&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>) — the .NET analog of
/// <c>python</c> vs <c>python_async</c>. Selected at process start by <c>CONSUMER_FLAVOR=async</c>
/// (Program.cs). Each RPC resolves a server-local <c>producer_id</c>, <b>awaits</b> the matching
/// <see cref="Task"/>-returning binding method on the gRPC handler task, and maps the result into
/// the proto response, translating any operational <see cref="KafkaException"/> via
/// <see cref="Translate"/> (reused verbatim from the sync backend). Bridging through
/// <c>&lt;byte[], byte[]&gt;</c> + <see cref="Serdes.ByteArray"/> exercises the shipped generic
/// serialize path end-to-end over the wire.
/// </summary>
/// <remarks>
/// <para>
/// <b>Value over the sync backend.</b> The sync <see cref="ProducerServiceImpl"/> blocks on the
/// binding's <c>RecordMetadata Send(record)</c>. This async servicer drives the .NET completion
/// bridge end-to-end — the <c>TaskCompletionSource</c>-backed <c>Task&lt;RecordMetadata&gt;</c>
/// completed by the send pump (ffi-marshalling.md §A7) — the exact machinery the sync path never
/// covers.
/// </para>
/// <para>
/// <b>No per-id gate (PLAN §4 — divergence from the consumer).</b> The producer is thread-safe:
/// the core's internal <c>Mutex</c> serializes concurrent <c>Send</c> (<c>ffi-marshalling.md §A1</c>).
/// So this servicer needs ONLY a thread-safe <see cref="ConcurrentDictionary{TKey, TValue}"/>
/// id -&gt; producer map (plus an <see cref="Interlocked"/> id counter) for <c>CreateProducer</c> /
/// <c>Close</c> races — and NO per-op gate / <see cref="SemaphoreSlim"/>.
/// </para>
/// <para>
/// <b>No sync-over-async.</b> Handlers <c>await</c> the binding's <see cref="Task"/> directly —
/// never <c>Task.Run</c> / <c>.Result</c> / <c>.GetAwaiter().GetResult()</c> (ffi-marshalling.md §A7).
/// </para>
/// <para>
/// <b><c>CloseTimeout</c> ignores <c>timeout_ms</c> (PLAN §2/§5).</b> <see cref="AsyncKafkaProducer{TKey, TValue}"/>
/// has no timed close (only <c>Close(CancellationToken)</c>), so <see cref="CloseTimeout"/> delegates to
/// <c>await Close()</c> — a faithful port of the Python server, behaviorally invisible to the harness
/// (no scenario exercises <c>close_timeout</c>).
/// </para>
/// </remarks>
internal sealed class AsyncProducerServiceImpl : Proto.ProducerService.ProducerServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, IAsyncProducer<byte[], byte[]>> _producers =
        new ConcurrentDictionary<ulong, IAsyncProducer<byte[], byte[]>>();

    private long _nextId;

    /// <summary>
    /// The bound on the <c>Send</c> wait — 120 s, matching <c>grpc_server.py</c>'s
    /// <c>future.result(timeout=120)</c>. Only the ASYNC servicer bounds it; the sync one stays
    /// unbounded by decision (see <see cref="ProducerServiceImpl.Send"/>).
    /// </summary>
    private static readonly TimeSpan s_sendTimeout = TimeSpan.FromSeconds(120);

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
        foreach (KeyValuePair<ulong, IAsyncProducer<byte[], byte[]>> pair in _producers)
        {
            if (!_producers.TryRemove(pair.Key, out IAsyncProducer<byte[], byte[]>? producer))
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
        IAsyncProducer<byte[], byte[]> producer;
        try
        {
            // Empty (or all-blank) config selects a broker-free AsyncMockProducer (Python parity,
            // grpc_server_async.py). Otherwise a real AsyncKafkaProducer.
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

    /// <inheritdoc/>
    public override async Task<Proto.SendResponse> Send(Proto.SendRequest request, ServerCallContext context)
    {
        IAsyncProducer<byte[], byte[]>? producer = Get(request.ProducerId);
        if (producer is null)
        {
            return new Proto.SendResponse { Error = Translate.UnknownProducer(request.ProducerId) };
        }

        // with_callback is a hint only — the callback closure stays Rust-side; the unary
        // response IS the resolved future (producer_service.proto). Nothing to do here.
        try
        {
            ProducerRecord<byte[], byte[]> record = Translate.ProducerRecordFromProto(request.Record);

            // Bounded at 120 s for Python parity (grpc_server.py's future.result(timeout=120)):
            // a stuck future must fail the harness DIAGNOSABLY instead of hanging the unary RPC and
            // therefore the whole scenario. Task.WaitAsync is available because this project targets
            // net8.0. Never .Result / .GetAwaiter().GetResult() — that would be sync-over-async.
            RecordMetadata metadata = await producer.Send(record)
                .WaitAsync(s_sendTimeout).ConfigureAwait(false);
            return new Proto.SendResponse { Metadata = Translate.MetadataToProto(metadata) };
        }
        catch (TimeoutException)
        {
            // Structured TIMEOUT, matching Python's shape byte for byte (variant/code/flags).
            return new Proto.SendResponse
            {
                Error = new Proto.KafkaError
                {
                    Variant = Proto.KafkaError.Types.Variant.Timeout,
                    Code = 7,
                    Message = "dotnet server: producer future timed out after 120s",
                    IsRetriable = true,
                    IsFatal = false,
                },
            };
        }
        catch (Exception ex)
        {
            return new Proto.SendResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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

    /// <summary>
    /// The <c>Metrics</c> RPC — the producer's <c>metrics()</c> snapshot as a proto
    /// <c>MetricList</c> (Python <c>grpc_server.py</c> / C++ <c>server.cc</c> parity). Mirrors
    /// <see cref="AsyncConsumerServiceImpl.Metrics"/> but WITHOUT its <c>lock (entry.Gate)</c>: the
    /// producer is thread-safe (the core's <c>Mutex</c> serializes; ffi §A1 "don't add your own
    /// lock"), so this servicer deliberately has no per-id gate to take.
    /// </summary>
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

            // Metrics() is a SYNCHRONOUS state read on IAsyncProducer too (it does not block in Java, CLAUDE.md §4) — call it directly.
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
    public override async Task<Proto.StatusResponse> Close(Proto.CloseRequest request, ServerCallContext context)
    {
        if (!_producers.TryRemove(request.ProducerId, out IAsyncProducer<byte[], byte[]>? producer))
        {
            // Close is idempotent — silent success on an unknown id (Python / Java parity).
            return new Proto.StatusResponse();
        }

        try
        {
            // The binding's Close() gracefully closes AND releases the native handle
            // (Producer_close_async -> Producer_destroy). timeout_ms is not modeled on the async
            // Close (only Close(CancellationToken)); the timed RPC is CloseTimeout below.
            await producer.Close().ConfigureAwait(false);
            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> CloseTimeout(Proto.CloseTimeoutRequest request, ServerCallContext context) =>
        // The async producer has no timed close (PLAN §2/§5): ignore timeout_ms and delegate to
        // await Close() — a faithful port of grpc_server.py's CloseTimeout, behaviorally invisible
        // to the harness (no scenario exercises close_timeout).
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

    private IAsyncProducer<byte[], byte[]>? Get(ulong producerId) =>
        _producers.TryGetValue(producerId, out IAsyncProducer<byte[], byte[]>? producer) ? producer : null;
}
