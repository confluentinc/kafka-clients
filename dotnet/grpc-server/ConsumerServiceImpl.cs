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
/// Serves <c>ConsumerService</c> over the sync <see cref="KafkaConsumer{TKey, TValue}"/> and
/// <see cref="MockConsumer{TKey, TValue}"/> (<c>&lt;byte[], byte[]&gt;</c>, <see cref="Serdes.ByteArray"/>),
/// the C# port of the consumer half of Python's <c>grpc_server.py</c>.
/// </summary>
internal sealed class ConsumerServiceImpl : Proto.ConsumerService.ConsumerServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, ConsumerEntry> _consumers = new ConcurrentDictionary<ulong, ConsumerEntry>();

    private readonly CallbackLog _callbackLog = new CallbackLog();

    private readonly GroupMetadataStore _groupMetadata;

    private long _nextId;

    public ConsumerServiceImpl(GroupMetadataStore groupMetadata)
    {
        _groupMetadata = groupMetadata;
    }

    public void Dispose()
    {
        foreach (KeyValuePair<ulong, ConsumerEntry> pair in _consumers)
        {
            if (!_consumers.TryRemove(pair.Key, out ConsumerEntry? entry))
            {
                continue;
            }

            try
            {
                entry.Consumer.Dispose();
            }
            catch (Exception)
            {
                // One failing consumer must not abort the sweep.
            }
        }
    }

    public override Task<Proto.CreateConsumerResponse> CreateConsumer(Proto.CreateConsumerRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        IConsumer<byte[], byte[]> consumer;
        try
        {
            // An empty or all-blank config selects a MockConsumer, as Python's servicer does.
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

    // Without with_listener the plain Subscribe(topics) replaces, and so releases, a listener an
    // earlier subscribe registered, as in Python.
    public override Task<Proto.StatusResponse> Subscribe(Proto.SubscribeRequest request, ServerCallContext context)
    {
        // Pattern subscription is not ported: answer -31 rather than subscribe to no topic.
        if (request.HasPattern)
        {
            return RunStatus(request.ConsumerId, _ => throw new NotSupportedException(Translate.PatternSubscribeUnsupported));
        }

        List<string> topics = new List<string>(request.Topics);
        if (request.WithListener)
        {
            LoggingRebalanceListener listener = new LoggingRebalanceListener(_callbackLog, request.ConsumerId);
            return RunStatus(request.ConsumerId, c => c.Subscribe(topics, listener));
        }

        return RunStatus(request.ConsumerId, c => c.Subscribe(topics));
    }

    public override Task<Proto.StatusResponse> Unsubscribe(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Unsubscribe());

    public override Task<Proto.StatusResponse> Assign(Proto.AssignRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Assign(ToTopicPartitions(request.Partitions)));

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

    public override Task<Proto.StatusResponse> CommitSync(Proto.CommitSyncRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c =>
        {
            TimeSpan? timeout = Translate.ConsumerTimeout(request.HasTimeoutMs, request.TimeoutMs);

            // Empty offsets => commit the current consumed positions; otherwise commit these.
            if (request.Offsets.Count > 0)
            {
                Dictionary<TopicPartition, OffsetAndMetadata> offsets = Translate.ProtoOffsetsToDictionary(request.Offsets);
                if (timeout.HasValue)
                {
                    c.Commit(offsets, timeout.Value);
                }
                else
                {
                    c.Commit(offsets);
                }
            }
            else if (timeout.HasValue)
            {
                c.Commit(timeout.Value);
            }
            else
            {
                c.Commit();
            }
        });

    public override Task<Proto.StatusResponse> CommitAsync(Proto.CommitAsyncRequest request, ServerCallContext context)
    {
        IOffsetCommitCallback? callback = request.WithCallback
            ? new LoggingCommitCallback(_callbackLog, request.ConsumerId)
            : null;

        return RunStatus(request.ConsumerId, c =>
        {
            if (request.Offsets.Count > 0)
            {
                // This overload honours a null callback (Java's commitAsync(Map, null)).
                c.CommitAsync(Translate.ProtoOffsetsToDictionary(request.Offsets), callback);
            }
            else if (callback is not null)
            {
                c.CommitAsync(callback);
            }
            else
            {
                c.CommitAsync();
            }
        });
    }

    public override Task<Proto.CommittedResponse> Committed(Proto.CommittedRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.CommittedResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            List<TopicPartition> partitions = ToTopicPartitions(request.Partitions);
            TimeSpan? timeout = Translate.ConsumerTimeout(request.HasTimeoutMs, request.TimeoutMs);
            Proto.OffsetMap map = new Proto.OffsetMap();
            lock (entry.Gate)
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> result = timeout.HasValue
                    ? entry.Consumer.Committed(partitions, timeout.Value)
                    : entry.Consumer.Committed(partitions);
                foreach (KeyValuePair<TopicPartition, OffsetAndMetadata?> pair in result)
                {
                    if (pair.Value is not null)
                    {
                        map.Entries.Add(new Proto.OffsetMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = Translate.OamToProto(pair.Value) });
                    }
                }
            }

            return Task.FromResult(new Proto.CommittedResponse { Offsets = map });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.CommittedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.PositionResponse> Position(Proto.PositionRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.PositionResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            TopicPartition partition = Translate.Tp(request.Partition);
            TimeSpan? timeout = Translate.ConsumerTimeout(request.HasTimeoutMs, request.TimeoutMs);
            long offset;
            lock (entry.Gate)
            {
                offset = timeout.HasValue
                    ? entry.Consumer.Position(partition, timeout.Value)
                    : entry.Consumer.Position(partition);
            }

            return Task.FromResult(new Proto.PositionResponse { Offset = offset });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.PositionResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.StatusResponse> Seek(Proto.SeekRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c =>
        {
            TopicPartition partition = Translate.Tp(request.Partition);
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

    public override Task<Proto.StatusResponse> SeekToBeginning(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.SeekToBeginning(ToTopicPartitions(request.Partitions)));

    public override Task<Proto.StatusResponse> SeekToEnd(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.SeekToEnd(ToTopicPartitions(request.Partitions)));

    public override Task<Proto.StatusResponse> Pause(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Pause(ToTopicPartitions(request.Partitions)));

    public override Task<Proto.StatusResponse> Resume(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c => c.Resume(ToTopicPartitions(request.Partitions)));

    public override Task<Proto.LongOffsetsResponse> BeginningOffsets(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        LongOffsets(request, end: false);

    public override Task<Proto.LongOffsetsResponse> EndOffsets(Proto.TopicPartitionListRequest request, ServerCallContext context) =>
        LongOffsets(request, end: true);

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

            TimeSpan? timeout = Translate.ConsumerTimeout(request.HasTimeoutMs, request.TimeoutMs);
            Proto.OffsetAndTimestampMap map = new Proto.OffsetAndTimestampMap();
            lock (entry.Gate)
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> result = timeout.HasValue
                    ? entry.Consumer.OffsetsForTimes(spec, timeout.Value)
                    : entry.Consumer.OffsetsForTimes(spec);
                foreach (KeyValuePair<TopicPartition, OffsetAndTimestamp?> pair in result)
                {
                    if (pair.Value is not null)
                    {
                        map.Entries.Add(new Proto.OffsetAndTimestampMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = Translate.OatToProto(pair.Value) });
                    }
                }
            }

            return Task.FromResult(new Proto.OffsetAndTimestampResponse { Offsets = map });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.OffsetAndTimestampResponse { Error = Translate.ToProto(ex) });
        }
    }

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

    public override Task<Proto.TopicPartitionListResponse> Assignment(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        TopicPartitionListRead(request.ConsumerId, c => c.Assignment());

    public override Task<Proto.TopicPartitionListResponse> Paused(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        TopicPartitionListRead(request.ConsumerId, c => c.Paused());

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

    public override Task<Proto.GroupMetadataResponse> GroupMetadata(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.GroupMetadataResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            ConsumerGroupMetadata groupMetadata;
            lock (entry.Gate)
            {
                groupMetadata = entry.Consumer.GroupMetadata();
            }

            return Task.FromResult(new Proto.GroupMetadataResponse
            {
                GroupMetadataId = _groupMetadata.Add(groupMetadata),
                GroupMetadata = Translate.GroupMetadataToProto(groupMetadata),
            });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.GroupMetadataResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.StatusResponse> ReleaseGroupMetadata(Proto.ReleaseGroupMetadataRequest request, ServerCallContext context)
    {
        _groupMetadata.Release(request.GroupMetadataId);
        return Task.FromResult(new Proto.StatusResponse());
    }

    public override Task<Proto.StatusResponse> Wakeup(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        // No gate: it interrupts a Poll blocked on another thread. An unknown id is a no-op.
        Get(request.ConsumerId)?.Consumer.Wakeup();
        return Task.FromResult(new Proto.StatusResponse());
    }

    public override Task<Proto.StatusResponse> Close(Proto.ConsumerCloseRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            // An unknown id closes silently, as in Python; the harness relies on it.
            return Task.FromResult(new Proto.StatusResponse());
        }

        // An unknown operation name throws here, outside the handler's catch, so gRPC answers
        // UNKNOWN, as Python's Close lets its KeyError escape.
        CloseOptions? options = Translate.CloseOptionsFromProto(request);
        try
        {
            lock (entry.Gate)
            {
                if (options is null)
                {
                    entry.Consumer.Close();
                }
                else
                {
                    entry.Consumer.Close(options);
                }
            }

            // Evict only after the close ran: an eviction without a close would orphan the
            // consumer.
            _consumers.TryRemove(request.ConsumerId, out _);
            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            // Dispose, then evict, so a failed close never orphans the consumer.
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

    public override Task<Proto.CurrentLagResponse> CurrentLag(Proto.CurrentLagRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.CurrentLagResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            long? lag;
            lock (entry.Gate)
            {
                lag = entry.Consumer.CurrentLag(Translate.Tp(request.Partition));
            }

            return Task.FromResult(Translate.CurrentLagResponse(lag));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.CurrentLagResponse { Error = Translate.ToProto(ex) });
        }
    }

    // No gate, and an unknown or closed id reads an empty log, not an error: the entries outlive
    // their consumer, as in Python.
    public override Task<Proto.CallbackLogResponse> GetCallbackLog(Proto.CallbackLogRequest request, ServerCallContext context) =>
        Task.FromResult(_callbackLog.Response(request.ConsumerId));

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

    private Task<Proto.LongOffsetsResponse> LongOffsets(Proto.TopicPartitionListRequest request, bool end)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return Task.FromResult(new Proto.LongOffsetsResponse { Error = Translate.UnknownConsumer(request.ConsumerId) });
        }

        try
        {
            List<TopicPartition> parts = ToTopicPartitions(request.Partitions);
            TimeSpan? timeout = Translate.ConsumerTimeout(request.HasTimeoutMs, request.TimeoutMs);
            Proto.LongOffsetMap map = new Proto.LongOffsetMap();
            lock (entry.Gate)
            {
                IReadOnlyDictionary<TopicPartition, long> result = timeout is TimeSpan t
                    ? (end ? entry.Consumer.EndOffsets(parts, t) : entry.Consumer.BeginningOffsets(parts, t))
                    : (end ? entry.Consumer.EndOffsets(parts) : entry.Consumer.BeginningOffsets(parts));
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

    /// <summary>A consumer plus the gate that serializes the RPCs on it.</summary>
    private sealed class ConsumerEntry
    {
        internal ConsumerEntry(IConsumer<byte[], byte[]> consumer)
        {
            Consumer = consumer;
        }

        internal IConsumer<byte[], byte[]> Consumer { get; }

        // A stray concurrent RPC waits here instead of being refused by KafkaConsumer's owner check.
        internal object Gate { get; } = new object();
    }
}
