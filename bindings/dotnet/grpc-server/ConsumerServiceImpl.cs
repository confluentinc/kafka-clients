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
/// Maps the 23 <c>ConsumerService</c> RPCs onto the binding's synchronous
/// <see cref="KafkaConsumer{TKey, TValue}"/> / <see cref="MockConsumer{TKey, TValue}"/>
/// (both <c>&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>) — the .NET port
/// of <c>grpc_server.py</c>'s <c>ConsumerService</c> half. Each RPC resolves a server-local
/// <c>consumer_id</c>, calls the matching sync binding method on the gRPC handler thread
/// (Python's model — the blocking <c>Poll</c> parks the handler thread; no <c>Task.Run</c>),
/// and maps the result into the proto response, translating any operational
/// <see cref="KafkaException"/> via <see cref="Translate"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Per-id serialization (PLAN §5 decision, recorded in COMMENTS.DONE.23).</b> The native
/// consumer is single-owner / not thread-safe — a concurrent op faults the core. The harness
/// drives one op in flight sequentially, but as cheap defense-in-depth every op that touches
/// a consumer takes that consumer's <see cref="ConsumerEntry.Gate"/>, so a stray concurrent
/// RPC queues instead of faulting. <see cref="Wakeup"/> deliberately does <b>not</b> take the
/// gate — it is the one cross-thread member (it interrupts a blocked <see cref="Poll"/> on
/// another thread), so gating it would deadlock behind the very poll it must wake.
/// </para>
/// <para>
/// <b>Registry drain at shutdown (M9/P4 M5).</b> The <see cref="Close"/> RPC is the only thing
/// that removes an entry, so any scenario that creates a consumer and never sends <c>Close</c>
/// — a failed assertion, a panicking Rust test, a dropped client — left a live native consumer
/// in the map forever. The Rust test client has <c>close()</c> but no <c>Drop</c> impl, and the
/// gRPC backend process is <b>shared across scenarios</b>, so those leaks (each a tokio runtime
/// + <c>ConsumerNetworkThread</c> + a dedicated dispatcher thread) accumulated for the process
/// lifetime. This type is now <see cref="IDisposable"/> and <see cref="Dispose"/> drains the
/// registry, and <c>Program</c> calls it explicitly on shutdown — implementing
/// <see cref="IDisposable"/> without a path that actually calls it would fix nothing. An
/// idle-eviction sweep is deliberately out of scope: a timer in test infrastructure is a new
/// failure mode, and shutdown disposal covers the accumulation.
/// </para>
/// </remarks>
internal sealed class ConsumerServiceImpl : Proto.ConsumerService.ConsumerServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, ConsumerEntry> _consumers = new ConcurrentDictionary<ulong, ConsumerEntry>();
    private long _nextId;

    /// <summary>
    /// Drains the consumer registry: remove each remaining entry and dispose its consumer
    /// (M9/P4 M5). Each teardown is individually guarded so one failing consumer cannot abort
    /// the sweep and strand the rest. Idempotent and safe on an empty registry.
    /// </summary>
    public void Dispose()
    {
        // Remove-then-dispose per entry so a concurrent Close RPC and this sweep cannot both
        // claim the same consumer (Dispose is itself idempotent, but the sweep should not have
        // to rely on that).
        foreach (KeyValuePair<ulong, ConsumerEntry> pair in _consumers)
        {
            if (!_consumers.TryRemove(pair.Key, out ConsumerEntry? entry))
            {
                continue;
            }

            try
            {
                // Dispose, not Close: it swallows the close error, which is what a best-effort
                // shutdown sweep wants. It still routes through the graceful Consumer_close
                // before Consumer_destroy.
                entry.Consumer.Dispose();
            }
            catch (Exception)
            {
                // One failing consumer must not abort the sweep — the remaining entries still
                // hold live native handles.
            }
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.CreateConsumerResponse> CreateConsumer(Proto.CreateConsumerRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        IConsumer<byte[], byte[]> consumer;
        try
        {
            // Empty (or all-blank) config selects a broker-free MockConsumer (Python parity,
            // grpc_server.py). Otherwise a real KIP-848 KafkaConsumer.
            if (IsEmptyConfig(config))
            {
                consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
            }
            else
            {
                consumer = new KafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
            }
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.CreateConsumerResponse { ConsumerId = 0, Error = Translate.ToProto(ex) });
        }

        ulong id = (ulong)Interlocked.Increment(ref _nextId);
        _consumers[id] = new ConsumerEntry(consumer);
        return Task.FromResult(new Proto.CreateConsumerResponse { ConsumerId = id });
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Subscribe(Proto.SubscribeRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Subscribe(new List<string>(request.Topics)));

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Unsubscribe(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Unsubscribe());

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Assign(Proto.AssignRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Assign(ToTopicPartitions(request.Partitions)));

    /// <inheritdoc/>
    public override Task<Proto.PollResponse> Poll(Proto.PollRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.PollResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            Proto.ConsumerRecordList records = new Proto.ConsumerRecordList();
            lock (entry.Gate)
            {
                ConsumerRecords<byte[], byte[]> polled = entry.Consumer.Poll(TimeSpan.FromMilliseconds(request.TimeoutMs));
                foreach (ConsumerRecord<byte[], byte[]> record in polled)
                {
                    records.Records.Add(Translate.RecordToProto(record));
                }
            }

            return Task.FromResult(new Proto.PollResponse { Records = records });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.PollResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> CommitSync(Proto.CommitSyncRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c =>
        {
            // Empty offsets => commit the current consumed positions; otherwise commit these.
            if (request.Offsets.Count > 0)
            {
                Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>();
                foreach (Proto.OffsetMapEntry entry in request.Offsets)
                {
                    int? leaderEpoch = entry.Offset.HasLeaderEpoch ? entry.Offset.LeaderEpoch : (int?)null;
                    offsets[Translate.Tp(entry.Partition)] = new OffsetAndMetadata(entry.Offset.Offset, entry.Offset.Metadata, leaderEpoch);
                }

                c.Commit(offsets);
            }
            else
            {
                c.Commit();
            }
        });

    /// <inheritdoc/>
    public override Task<Proto.CommittedResponse> Committed(Proto.CommittedRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.CommittedResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            Proto.OffsetMap map = new Proto.OffsetMap();
            lock (entry.Gate)
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result = entry.Consumer.Committed(ToTopicPartitions(request.Partitions));
                foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> pair in result)
                {
                    map.Entries.Add(new Proto.OffsetMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = Translate.OamToProto(pair.Value) });
                }
            }

            return Task.FromResult(new Proto.CommittedResponse { Offsets = map });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.CommittedResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.PositionResponse> Position(Proto.PositionRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.PositionResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            long offset;
            lock (entry.Gate)
            {
                offset = entry.Consumer.Position(Translate.Tp(request.Partition));
            }

            return Task.FromResult(new Proto.PositionResponse { Offset = offset });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.PositionResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Seek(Proto.SeekRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c =>
        {
            TopicPartition partition = Translate.Tp(request.Partition);
            // metadata / leader_epoch present => Seek(tp, OffsetAndMetadata); else Seek(tp, offset).
            if (request.HasMetadata || request.HasLeaderEpoch)
            {
                int? leaderEpoch = request.HasLeaderEpoch ? request.LeaderEpoch : (int?)null;
                string metadata = request.HasMetadata ? request.Metadata : string.Empty;
                c.Seek(partition, new OffsetAndMetadata(request.Offset, metadata, leaderEpoch));
            }
            else
            {
                c.Seek(partition, request.Offset);
            }
        });

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> SeekToBeginning(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.SeekToBeginning(ToTopicPartitions(request.Partitions)));

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> SeekToEnd(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.SeekToEnd(ToTopicPartitions(request.Partitions)));

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Pause(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Pause(ToTopicPartitions(request.Partitions)));

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Resume(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Resume(ToTopicPartitions(request.Partitions)));

    /// <inheritdoc/>
    public override Task<Proto.LongOffsetsResponse> BeginningOffsets(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        LongOffsets(request.ConsumerId, request.Partitions, end: false);

    /// <inheritdoc/>
    public override Task<Proto.LongOffsetsResponse> EndOffsets(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        LongOffsets(request.ConsumerId, request.Partitions, end: true);

    /// <inheritdoc/>
    public override Task<Proto.OffsetAndTimestampResponse> OffsetsForTimes(Proto.OffsetsForTimesRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.OffsetAndTimestampResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            Dictionary<TopicPartition, long> spec = new Dictionary<TopicPartition, long>();
            foreach (Proto.TimestampSpecEntry entry2 in request.Timestamps)
            {
                spec[Translate.Tp(entry2.Partition)] = entry2.Timestamp;
            }

            Proto.OffsetAndTimestampMap map = new Proto.OffsetAndTimestampMap();
            lock (entry.Gate)
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> result = entry.Consumer.OffsetsForTimes(spec);
                foreach (KeyValuePair<TopicPartition, OffsetAndTimestamp> pair in result)
                {
                    map.Entries.Add(new Proto.OffsetAndTimestampMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = Translate.OatToProto(pair.Value) });
                }
            }

            return Task.FromResult(new Proto.OffsetAndTimestampResponse { Offsets = map });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.OffsetAndTimestampResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.PartitionsForResponse> PartitionsFor(Proto.ConsumerPartitionsForRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.PartitionsForResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            Proto.PartitionsForResponse response = new Proto.PartitionsForResponse();
            lock (entry.Gate)
            {
                IReadOnlyList<PartitionInfo> infos = entry.Consumer.PartitionsFor(request.Topic);
                foreach (PartitionInfo info in infos)
                {
                    response.Partitions.Add(Translate.PartitionInfoToProto(info));
                }
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.PartitionsForResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.ListTopicsResponse> ListTopics(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.ListTopicsResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            Proto.TopicListing listing = new Proto.TopicListing();
            lock (entry.Gate)
            {
                IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> topics = entry.Consumer.ListTopics();
                foreach (KeyValuePair<string, IReadOnlyList<PartitionInfo>> pair in topics)
                {
                    Proto.TopicPartitionInfoEntry topicEntry = new Proto.TopicPartitionInfoEntry { Topic = pair.Key };
                    foreach (PartitionInfo info in pair.Value)
                    {
                        topicEntry.Partitions.Add(Translate.PartitionInfoToProto(info));
                    }

                    listing.Topics.Add(topicEntry);
                }
            }

            return Task.FromResult(new Proto.ListTopicsResponse { Topics = listing });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.ListTopicsResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.TopicPartitionListResponse> Assignment(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        TopicPartitionListRead(request.ConsumerId, c => c.Assignment());

    /// <inheritdoc/>
    public override Task<Proto.TopicPartitionListResponse> Paused(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        TopicPartitionListRead(request.ConsumerId, c => c.Paused());

    /// <inheritdoc/>
    public override Task<Proto.MetricsResponse> Metrics(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.MetricsResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            Proto.MetricList list = new Proto.MetricList();
            lock (entry.Gate)
            {
                // Metrics() is a SYNCHRONOUS state read (IConsumerCommon) — call it directly.
                foreach (KeyValuePair<MetricName, IMetric> pair in entry.Consumer.Metrics())
                {
                    list.Metrics.Add(Translate.MetricToProto(pair.Key, pair.Value));
                }
            }

            return Task.FromResult(new Proto.MetricsResponse { Metrics = list });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.MetricsResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.SubscriptionResponse> Subscription(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.SubscriptionResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            Proto.StringList topics = new Proto.StringList();
            lock (entry.Gate)
            {
                foreach (string topic in entry.Consumer.Subscription())
                {
                    topics.Values.Add(topic);
                }
            }

            return Task.FromResult(new Proto.SubscriptionResponse { Topics = topics });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.SubscriptionResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Wakeup(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        // Cross-thread interrupt of a blocked Poll — MUST NOT take the per-id gate (see the
        // type remarks). Idempotent on an unknown id (Python parity).
        Get(request.ConsumerId)?.Consumer.Wakeup();
        return Task.FromResult(new Proto.StatusResponse());
    }

    /// <summary>
    /// Closes and evicts a consumer. Ordering is <b>resolve → validate → close → evict</b>
    /// (M9/P4 H2), matching the non-destructive <c>Get</c> + <c>lock</c> shape every other RPC
    /// uses.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why the order matters.</b> This used to <c>TryRemove</c> <em>first</em> and only then
    /// try to close, with two throw sites sitting between the eviction and any native call:
    /// <c>TimeSpan.FromMilliseconds</c> itself (for a <c>timeout_ms</c> above ~9.22e15), and the
    /// binding's own precondition — <c>Close(TimeSpan)</c> throws
    /// <see cref="ArgumentOutOfRangeException"/> ("Timeout must not be negative.") before any
    /// native call. The proto field is <c>optional int64 timeout_ms</c>, so a negative value
    /// both survives the wire and sets <c>HasTimeoutMs</c>. Either throw left the entry already
    /// gone, the <c>catch</c> restoring nothing and disposing nothing, so the consumer became
    /// <b>unreachable</b>: <c>Consumer_close</c> and <c>Consumer_destroy</c> never ran, a whole
    /// native consumer (tokio runtime + <c>ConsumerNetworkThread</c> + dispatcher thread) leaked,
    /// a retried <c>Close</c> hit the <c>TryRemove</c> miss and reported <b>silent success</b>,
    /// and <c>Wakeup</c> became a silent no-op for that id.
    /// </para>
    /// <para>
    /// <b>It was latent, not active</b> — the shipped Rust harness client hardcodes
    /// <c>timeout_ms: None</c> in both close variants, so <c>HasTimeoutMs</c> is always false and
    /// the throwing path is unreachable from the suite. Fixed anyway: it is a genuine
    /// precondition-ordering defect, the harness client could plumb a timeout at any time, and
    /// the registry-drain sweep depends on the same discipline.
    /// </para>
    /// <para>
    /// A <em>failing</em> <c>Consumer_close</c> is NOT a leak — the binding releases the handle
    /// in a <c>finally</c> — so the fix is purely about ordering, not about adding a second
    /// teardown.
    /// </para>
    /// </remarks>
    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Close(Proto.ConsumerCloseRequest request, ServerCallContext context)
    {
        // Non-destructive resolve, like every other RPC.
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            // Close stays idempotent — silent success on an unknown id (Python / Java parity);
            // the harness relies on it.
            return Task.FromResult(new Proto.StatusResponse());
        }

        try
        {
            // The binding's Close() gracefully closes AND releases the native handle
            // (Consumer_close -> Consumer_destroy), so no separate Dispose is needed here.
            lock (entry.Gate)
            {
                if (request.HasTimeoutMs)
                {
                    entry.Consumer.Close(TimeSpan.FromMilliseconds(request.TimeoutMs));
                }
                else
                {
                    entry.Consumer.Close();
                }
            }

            // Evict only after the close actually ran. Close/Dispose are documented idempotent,
            // so a duplicate Close RPC is safe either way; what must never happen is eviction
            // WITHOUT a close, which orphans the consumer.
            _consumers.TryRemove(request.ConsumerId, out _);
            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            // Never orphan: dispose, then evict, so the native handle is released even when the
            // close failed or its preconditions rejected the request. Dispose before evict so a
            // racing retry cannot find the entry after it has been torn down.
            try
            {
                entry.Consumer.Dispose();
            }
            catch (Exception)
            {
                // Best-effort teardown; the original failure is what the caller needs.
            }

            _consumers.TryRemove(request.ConsumerId, out _);
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
    }

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

    private static List<TopicPartition> ToTopicPartitions(IEnumerable<Proto.TopicPartition> partitions)
    {
        List<TopicPartition> result = new List<TopicPartition>();
        foreach (Proto.TopicPartition partition in partitions)
        {
            result.Add(Translate.Tp(partition));
        }

        return result;
    }

    private ConsumerEntry? Get(ulong consumerId) =>
        _consumers.TryGetValue(consumerId, out ConsumerEntry? entry) ? entry : null;

    private Task<Proto.StatusResponse> RunStatus(ulong consumerId, Action<IConsumer<byte[], byte[]>> operation)
    {
        ConsumerEntry? entry = Get(consumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.UnknownConsumer(consumerId) });
        }

        try
        {
            lock (entry.Gate)
            {
                operation(entry.Consumer);
            }

            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
    }

    private Task<Proto.LongOffsetsResponse> LongOffsets(ulong consumerId, IEnumerable<Proto.TopicPartition> partitions, bool end)
    {
        ConsumerEntry? entry = Get(consumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.LongOffsetsResponse { Error = Translate.UnknownConsumer(consumerId) });
        }

        try
        {
            List<TopicPartition> parts = ToTopicPartitions(partitions);
            Proto.LongOffsetMap map = new Proto.LongOffsetMap();
            lock (entry.Gate)
            {
                IReadOnlyDictionary<TopicPartition, long> result = end ? entry.Consumer.EndOffsets(parts) : entry.Consumer.BeginningOffsets(parts);
                foreach (KeyValuePair<TopicPartition, long> pair in result)
                {
                    map.Entries.Add(new Proto.LongOffsetMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = pair.Value });
                }
            }

            return Task.FromResult(new Proto.LongOffsetsResponse { Offsets = map });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.LongOffsetsResponse { Error = Translate.ToProto(ex) });
        }
    }

    private Task<Proto.TopicPartitionListResponse> TopicPartitionListRead(
        ulong consumerId,
        Func<IConsumer<byte[], byte[]>, IReadOnlyCollection<TopicPartition>> read)
    {
        ConsumerEntry? entry = Get(consumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.TopicPartitionListResponse { Error = Translate.UnknownConsumer(consumerId) });
        }

        try
        {
            Proto.TopicPartitionList list = new Proto.TopicPartitionList();
            lock (entry.Gate)
            {
                foreach (TopicPartition partition in read(entry.Consumer))
                {
                    list.Partitions.Add(Translate.TpToProto(partition));
                }
            }

            return Task.FromResult(new Proto.TopicPartitionListResponse { Partitions = list });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.TopicPartitionListResponse { Error = Translate.ToProto(ex) });
        }
    }

    /// <summary>A consumer plus its per-id serialization gate (see the type remarks).</summary>
    private sealed class ConsumerEntry
    {
        internal ConsumerEntry(IConsumer<byte[], byte[]> consumer)
        {
            Consumer = consumer;
        }

        internal IConsumer<byte[], byte[]> Consumer { get; }

        internal object Gate { get; } = new object();
    }
}
