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
/// </remarks>
internal sealed class ConsumerServiceImpl : Proto.ConsumerService.ConsumerServiceBase
{
    private readonly ConcurrentDictionary<ulong, ConsumerEntry> _consumers = new ConcurrentDictionary<ulong, ConsumerEntry>();
    private long _nextId;

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

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Close(Proto.ConsumerCloseRequest request, ServerCallContext context)
    {
        if (!_consumers.TryRemove(request.ConsumerId, out ConsumerEntry? entry))
        {
            // Close is idempotent — silent success on an unknown id (Python / Java parity).
            return Task.FromResult(new Proto.StatusResponse());
        }

        try
        {
            // The binding's Close() gracefully closes AND releases the native handle
            // (Consumer_close -> Consumer_destroy), so no separate Dispose is needed.
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

            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
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
