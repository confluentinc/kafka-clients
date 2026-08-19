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
/// The <b>async</b> twin of <see cref="ConsumerServiceImpl"/> (M8/P2): maps the 23
/// <c>ConsumerService</c> RPCs onto the binding's <em>asynchronous</em>
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> / <see cref="AsyncMockConsumer{TKey, TValue}"/>
/// (both <c>&lt;byte[], byte[]&gt;</c> with <see cref="Serdes.ByteArray"/>) — the .NET
/// analog of <c>python</c> vs <c>python_async</c>. Selected at process start by
/// <c>CONSUMER_FLAVOR=async</c> (Program.cs). Each RPC resolves a server-local
/// <c>consumer_id</c>, <b>awaits</b> the matching <see cref="Task"/>-returning binding
/// method on the gRPC handler task, and maps the result into the proto response,
/// translating any operational <see cref="KafkaException"/> via <see cref="Translate"/>
/// (reused verbatim from the sync backend).
/// </summary>
/// <remarks>
/// <para>
/// <b>Value over the sync backend.</b> The sync <see cref="ConsumerServiceImpl"/> calls the
/// sync C ABI (<c>block_on</c> inside the core). This async servicer drives the .NET
/// completion bridge end-to-end — <c>TaskCompletionSource</c>, the foreign
/// callback-dispatcher thread, <c>RunContinuationsAsynchronously</c>
/// (ffi-marshalling.md §B6/§B7) — the exact machinery the sync path never covers.
/// </para>
/// <para>
/// <b>Per-id serialization — async gate (PLAN §4).</b> The native consumer is single-owner /
/// not thread-safe; a concurrent op faults the core. The harness drives one op in flight
/// sequentially, but as cheap defense-in-depth every op that touches a consumer takes that
/// consumer's per-id gate so a stray concurrent RPC queues instead of faulting. The gate is
/// a <see cref="SemaphoreSlim"/> (not a monitor <c>lock</c>): a monitor cannot be held across
/// an <c>await</c>, so we <c>await gate.WaitAsync()</c> / <c>gate.Release()</c> around the
/// awaited op. <see cref="Wakeup"/> deliberately does <b>not</b> take the gate — it is the one
/// cross-thread member (it interrupts a blocked <see cref="Poll"/> on another task), so gating
/// it would deadlock behind the very poll it must wake.
/// </para>
/// <para>
/// <b>Close ignores <c>timeout_ms</c> (PLAN §2.1).</b> <see cref="AsyncKafkaConsumer{TKey, TValue}"/>
/// has no timed async close (only <c>Close(CancellationToken)</c>), so the async <see cref="Close"/>
/// RPC awaits <c>Close()</c> and ignores the proto's optional <c>timeout_ms</c>. Documented
/// divergence, behaviorally invisible to the harness scenarios (none assert close-timeout).
/// </para>
/// <para>
/// <b>No sync-over-async.</b> Handlers <c>await</c> the binding's <see cref="Task"/> directly —
/// never <c>Task.Run</c> / <c>.Result</c> / <c>.GetAwaiter().GetResult()</c>
/// (ffi-marshalling.md §B7). The genuinely synchronous members (<c>Seek</c> both overloads,
/// <c>Wakeup</c>, <c>Assignment</c>, <c>Subscription</c>, <c>Paused</c>) are called directly,
/// with no <c>await</c>.
/// </para>
/// </remarks>
internal sealed class AsyncConsumerServiceImpl : Proto.ConsumerService.ConsumerServiceBase
{
    private readonly ConcurrentDictionary<ulong, ConsumerEntry> _consumers = new ConcurrentDictionary<ulong, ConsumerEntry>();
    private long _nextId;

    /// <inheritdoc/>
    public override Task<Proto.CreateConsumerResponse> CreateConsumer(Proto.CreateConsumerRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        IAsyncConsumer<byte[], byte[]> consumer;
        try
        {
            // Empty (or all-blank) config selects a broker-free AsyncMockConsumer (Python parity,
            // grpc_server_async.py). Otherwise a real KIP-848 AsyncKafkaConsumer.
            if (IsEmptyConfig(config))
            {
                consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
            }
            else
            {
                consumer = new AsyncKafkaConsumer<byte[], byte[]>(config, Serdes.ByteArray, Serdes.ByteArray);
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
    public override async Task<Proto.PollResponse> Poll(Proto.PollRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.PollResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            Proto.ConsumerRecordList records = new Proto.ConsumerRecordList();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                ConsumerRecords<byte[], byte[]> polled =
                    await entry.Consumer.Poll(TimeSpan.FromMilliseconds(request.TimeoutMs)).ConfigureAwait(false);
                foreach (ConsumerRecord<byte[], byte[]> record in polled)
                {
                    records.Records.Add(Translate.RecordToProto(record));
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.PollResponse { Records = records };
        }
        catch (Exception ex)
        {
            return new Proto.PollResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> CommitSync(Proto.CommitSyncRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, async c =>
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

                await c.Commit(offsets).ConfigureAwait(false);
            }
            else
            {
                await c.Commit().ConfigureAwait(false);
            }
        });

    /// <inheritdoc/>
    public override async Task<Proto.CommittedResponse> Committed(Proto.CommittedRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.CommittedResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            Proto.OffsetMap map = new Proto.OffsetMap();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> result =
                    await entry.Consumer.Committed(ToTopicPartitions(request.Partitions)).ConfigureAwait(false);
                foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> pair in result)
                {
                    map.Entries.Add(new Proto.OffsetMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = Translate.OamToProto(pair.Value) });
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.CommittedResponse { Offsets = map };
        }
        catch (Exception ex)
        {
            return new Proto.CommittedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.PositionResponse> Position(Proto.PositionRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.PositionResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            long offset;
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                offset = await entry.Consumer.Position(Translate.Tp(request.Partition)).ConfigureAwait(false);
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.PositionResponse { Offset = offset };
        }
        catch (Exception ex)
        {
            return new Proto.PositionResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Seek(Proto.SeekRequest request, ServerCallContext context) =>
        RunStatus(request.ConsumerId, c =>
        {
            // Seek (both overloads) is SYNCHRONOUS on IConsumerCommon (Python-parity §4 divergence),
            // so call it directly (no await); return a completed Task to reuse the awaited gate helper.
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

            return Task.CompletedTask;
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
    public override async Task<Proto.OffsetAndTimestampResponse> OffsetsForTimes(Proto.OffsetsForTimesRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.OffsetAndTimestampResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            Dictionary<TopicPartition, long> spec = new Dictionary<TopicPartition, long>();
            foreach (Proto.TimestampSpecEntry entry2 in request.Timestamps)
            {
                spec[Translate.Tp(entry2.Partition)] = entry2.Timestamp;
            }

            Proto.OffsetAndTimestampMap map = new Proto.OffsetAndTimestampMap();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> result =
                    await entry.Consumer.OffsetsForTimes(spec).ConfigureAwait(false);
                foreach (KeyValuePair<TopicPartition, OffsetAndTimestamp> pair in result)
                {
                    map.Entries.Add(new Proto.OffsetAndTimestampMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = Translate.OatToProto(pair.Value) });
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.OffsetAndTimestampResponse { Offsets = map };
        }
        catch (Exception ex)
        {
            return new Proto.OffsetAndTimestampResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.PartitionsForResponse> PartitionsFor(Proto.ConsumerPartitionsForRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.PartitionsForResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            Proto.PartitionsForResponse response = new Proto.PartitionsForResponse();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                IReadOnlyList<PartitionInfo> infos = await entry.Consumer.PartitionsFor(request.Topic).ConfigureAwait(false);
                foreach (PartitionInfo info in infos)
                {
                    response.Partitions.Add(Translate.PartitionInfoToProto(info));
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.PartitionsForResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.ListTopicsResponse> ListTopics(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.ListTopicsResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            Proto.TopicListing listing = new Proto.TopicListing();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> topics =
                    await entry.Consumer.ListTopics().ConfigureAwait(false);
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
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.ListTopicsResponse { Topics = listing };
        }
        catch (Exception ex)
        {
            return new Proto.ListTopicsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.TopicPartitionListResponse> Assignment(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        TopicPartitionListRead(request.ConsumerId, c => c.Assignment());

    /// <inheritdoc/>
    public override Task<Proto.TopicPartitionListResponse> Paused(Proto.ConsumerIdRequest request, ServerCallContext context) =>
        TopicPartitionListRead(request.ConsumerId, c => c.Paused());

    /// <inheritdoc/>
    public override async Task<Proto.MetricsResponse> Metrics(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.MetricsResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            Proto.MetricList list = new Proto.MetricList();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                // Metrics() is a SYNCHRONOUS state read (IConsumerCommon, inherited by
                // IAsyncConsumer) — call it directly (no await).
                foreach (KeyValuePair<MetricName, IMetric> pair in entry.Consumer.Metrics())
                {
                    list.Metrics.Add(Translate.MetricToProto(pair.Key, pair.Value));
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.MetricsResponse { Metrics = list };
        }
        catch (Exception ex)
        {
            return new Proto.MetricsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.SubscriptionResponse> Subscription(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            return new Proto.SubscriptionResponse { Error = Translate.UnknownConsumer(request.ConsumerId) };
        }

        try
        {
            Proto.StringList topics = new Proto.StringList();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                // Subscription() is a SYNCHRONOUS state read (IConsumerCommon) — call it directly.
                foreach (string topic in entry.Consumer.Subscription())
                {
                    topics.Values.Add(topic);
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.SubscriptionResponse { Topics = topics };
        }
        catch (Exception ex)
        {
            return new Proto.SubscriptionResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Wakeup(Proto.ConsumerIdRequest request, ServerCallContext context)
    {
        // Cross-thread interrupt of a blocked Poll — MUST NOT take the per-id gate (see the
        // type remarks). Wakeup() is synchronous. Idempotent on an unknown id (Python parity).
        Get(request.ConsumerId)?.Consumer.Wakeup();
        return Task.FromResult(new Proto.StatusResponse());
    }

    /// <inheritdoc/>
    public override async Task<Proto.StatusResponse> Close(Proto.ConsumerCloseRequest request, ServerCallContext context)
    {
        if (!_consumers.TryRemove(request.ConsumerId, out ConsumerEntry? entry))
        {
            // Close is idempotent — silent success on an unknown id (Python / Java parity).
            return new Proto.StatusResponse();
        }

        try
        {
            // The binding's Close() gracefully closes AND releases the native handle
            // (Consumer_close -> Consumer_destroy), so no separate Dispose is needed.
            // AsyncKafkaConsumer.Close has no TimeSpan overload (only Close(CancellationToken)),
            // so timeout_ms is IGNORED here (PLAN §2.1) — behaviorally invisible to the harness.
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                await entry.Consumer.Close().ConfigureAwait(false);
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
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

    private async Task<Proto.StatusResponse> RunStatus(ulong consumerId, Func<IAsyncConsumer<byte[], byte[]>, Task> operation)
    {
        ConsumerEntry? entry = Get(consumerId);
        if (entry is null)
        {
            return new Proto.StatusResponse { Error = Translate.UnknownConsumer(consumerId) };
        }

        try
        {
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                await operation(entry.Consumer).ConfigureAwait(false);
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    private async Task<Proto.LongOffsetsResponse> LongOffsets(ulong consumerId, IEnumerable<Proto.TopicPartition> partitions, bool end)
    {
        ConsumerEntry? entry = Get(consumerId);
        if (entry is null)
        {
            return new Proto.LongOffsetsResponse { Error = Translate.UnknownConsumer(consumerId) };
        }

        try
        {
            List<TopicPartition> parts = ToTopicPartitions(partitions);
            Proto.LongOffsetMap map = new Proto.LongOffsetMap();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                IReadOnlyDictionary<TopicPartition, long> result = end
                    ? await entry.Consumer.EndOffsets(parts).ConfigureAwait(false)
                    : await entry.Consumer.BeginningOffsets(parts).ConfigureAwait(false);
                foreach (KeyValuePair<TopicPartition, long> pair in result)
                {
                    map.Entries.Add(new Proto.LongOffsetMapEntry { Partition = Translate.TpToProto(pair.Key), Offset = pair.Value });
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.LongOffsetsResponse { Offsets = map };
        }
        catch (Exception ex)
        {
            return new Proto.LongOffsetsResponse { Error = Translate.ToProto(ex) };
        }
    }

    private async Task<Proto.TopicPartitionListResponse> TopicPartitionListRead(
        ulong consumerId,
        Func<IAsyncConsumer<byte[], byte[]>, IReadOnlyCollection<TopicPartition>> read)
    {
        ConsumerEntry? entry = Get(consumerId);
        if (entry is null)
        {
            return new Proto.TopicPartitionListResponse { Error = Translate.UnknownConsumer(consumerId) };
        }

        try
        {
            Proto.TopicPartitionList list = new Proto.TopicPartitionList();
            await entry.Gate.WaitAsync().ConfigureAwait(false);
            try
            {
                // read (Assignment / Paused) is a SYNCHRONOUS state read (IConsumerCommon).
                foreach (TopicPartition partition in read(entry.Consumer))
                {
                    list.Partitions.Add(Translate.TpToProto(partition));
                }
            }
            finally
            {
                entry.Gate.Release();
            }

            return new Proto.TopicPartitionListResponse { Partitions = list };
        }
        catch (Exception ex)
        {
            return new Proto.TopicPartitionListResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <summary>A consumer plus its per-id async serialization gate (see the type remarks).</summary>
    private sealed class ConsumerEntry
    {
        internal ConsumerEntry(IAsyncConsumer<byte[], byte[]> consumer)
        {
            Consumer = consumer;
        }

        internal IAsyncConsumer<byte[], byte[]> Consumer { get; }

        /// <summary>
        /// Async per-id gate (<see cref="SemaphoreSlim"/>, not a monitor <c>lock</c>): a monitor
        /// cannot be held across an <c>await</c>, so ops <c>await Gate.WaitAsync()</c> /
        /// <c>Gate.Release()</c>. <see cref="Wakeup"/> is gate-exempt.
        /// </summary>
        internal SemaphoreSlim Gate { get; } = new SemaphoreSlim(1, 1);
    }
}
