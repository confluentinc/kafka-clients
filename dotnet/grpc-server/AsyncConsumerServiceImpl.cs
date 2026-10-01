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
/// The <b>async</b> twin of <see cref="ConsumerServiceImpl"/> (M8/P2): maps all 26
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
/// awaited op.
/// </para>
/// <para>
/// <b>Two RPCs are gate-exempt.</b> <see cref="Wakeup"/> is the original exemption: it is the
/// one cross-thread member (it interrupts a blocked <see cref="Poll"/> on another task), so
/// gating it would deadlock behind the very poll it must wake.
/// <see cref="GetCallbackLog"/> is the second (M9/P9): it touches only the servicer's
/// <see cref="CallbackLog"/> — never the consumer — and the harness's <c>poll_until_kind</c>
/// loop alternates <c>Poll</c> and <c>GetCallbackLog</c> while waiting for a callback, so
/// gating it would serialise a log read behind a multi-second poll and defeat the loop. The
/// log carries its own lock for exactly this reason (see <see cref="CallbackLog"/>), which is
/// what both reference servers do.
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
/// <para>
/// <b>Registry drain at shutdown (M9/P4 M5).</b> Same problem and same fix as the sync
/// servicer: <see cref="Close"/> was the only thing that removed an entry, so a scenario that
/// never sent <c>Close</c> leaked a live native consumer for the shared backend process's whole
/// lifetime. This type is now <see cref="IAsyncDisposable"/> (primary) plus
/// <see cref="IDisposable"/> (sync fallback), and <c>Program</c> calls it explicitly on
/// shutdown. The drain releases the <em>consumer</em> only — see
/// <see cref="ConsumerEntry.Gate"/> for why the per-entry gate is deliberately not disposed.
/// </para>
/// </remarks>
internal sealed class AsyncConsumerServiceImpl
    : Proto.ConsumerService.ConsumerServiceBase, IAsyncDisposable, IDisposable
{
    private readonly ConcurrentDictionary<ulong, ConsumerEntry> _consumers = new ConcurrentDictionary<ulong, ConsumerEntry>();

    /// <summary>
    /// The user-callback log, keyed by consumer id (M9/P9).
    /// </summary>
    /// <remarks>
    /// Held by the <b>servicer</b>, deliberately not by <see cref="ConsumerEntry"/>:
    /// <see cref="Close"/> evicts the entry on both its success and failure paths, so a log
    /// hung off the entry would vanish with the consumer and break the proto's "entries survive
    /// <c>Close</c>" contract. Session-lifetime and never evicted — see
    /// <see cref="CallbackLog"/> for the full reasoning, including why it is unbounded.
    /// </remarks>
    private readonly CallbackLog _callbackLog = new CallbackLog();

    private long _nextId;

    /// <summary>
    /// Drains the consumer registry asynchronously (the primary path, M9/P4 M5): remove each
    /// remaining entry and <c>await DisposeAsync()</c> its consumer. Each teardown is
    /// individually guarded so one failing consumer cannot abort the sweep. Idempotent and safe
    /// on an empty registry. The entry's gate is deliberately not disposed — see
    /// <see cref="ConsumerEntry.Gate"/>.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        foreach (KeyValuePair<ulong, ConsumerEntry> pair in _consumers)
        {
            if (!_consumers.TryRemove(pair.Key, out ConsumerEntry? entry))
            {
                continue;
            }

            try
            {
                // DisposeAsync, not Close: it swallows the close error, which is what a
                // best-effort shutdown sweep wants, and it is the graceful async path
                // (close_async joins the bg task) before Consumer_destroy.
                await entry.Consumer.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // One failing consumer must not abort the sweep.
            }
        }
    }

    /// <summary>
    /// Synchronous fallback drain (M9/P4 M5) for hosts that only call
    /// <see cref="IDisposable.Dispose"/>. Uses the consumers' blocking
    /// <see cref="IDisposable.Dispose"/> rather than blocking on
    /// <see cref="DisposeAsync"/> — sync-over-async at shutdown would be the very footgun
    /// ffi §B7 forbids.
    /// </summary>
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

    /// <summary>
    /// Subscribes, optionally registering a real <see cref="LoggingRebalanceListener"/> whose
    /// invocations land in the callback log (M9/P9) — the async mirror of
    /// <c>ConsumerServiceImpl.Subscribe</c>.
    /// </summary>
    /// <remarks>
    /// <b>Registration is per-subscribe.</b> When <c>with_listener</c> is set this awaits the
    /// listener-taking overload; when it is not, it awaits the plain <c>Subscribe(topics)</c> —
    /// and that is not merely "the same thing without a listener", it is what <b>releases</b> a
    /// previously registered one, because a listener is bound to a subscription and only a
    /// <em>replacing</em> subscribe clears it (<c>Unsubscribe</c> does not). Python makes the
    /// same distinction by passing <c>listener=None</c> explicitly
    /// (<c>grpc_server_async.py:281-292</c>). The harness's rebalance test depends on this: it
    /// re-subscribes <em>with</em> the listener to observe the revoke.
    /// </remarks>
    /// <inheritdoc/>
    public override Task<Proto.StatusResponse> Subscribe(Proto.SubscribeRequest request, ServerCallContext context)
    {
        List<string> topics = new List<string>(request.Topics);
        if (request.WithListener)
        {
            LoggingRebalanceListener listener = new LoggingRebalanceListener(_callbackLog, request.ConsumerId);
            return RunStatus(request.ConsumerId, c => c.Subscribe(topics, listener));
        }

        return RunStatus(request.ConsumerId, c => c.Subscribe(topics));
    }

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
                await c.Commit(Translate.ProtoOffsetsToDictionary(request.Offsets)).ConfigureAwait(false);
            }
            else
            {
                await c.Commit().ConfigureAwait(false);
            }
        });

    /// <summary>
    /// Java's <c>commitAsync</c> — returns as soon as the commit is <em>initiated</em>,
    /// optionally with a real <see cref="LoggingCommitCallback"/> (M9/P9).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>⚠ Not awaited, and identical to the sync servicer's body.</b> <c>CommitAsync</c> lives
    /// on <see cref="IConsumerCommon"/> and is sync <c>void</c> on <em>both</em> consumer
    /// flavors — Java's <c>commitAsync</c> is non-blocking, so the idiom map keeps it
    /// synchronous (CLAUDE.md §4). So this handler calls it directly and returns
    /// <see cref="Task.CompletedTask"/> to reuse the awaited gate helper, exactly as
    /// <see cref="Seek"/> does for the other sync-on-the-async-consumer member. Python's async
    /// server bypasses <c>_run_status</c>'s await for the same reason
    /// (<c>grpc_server_async.py:324-341</c>).
    /// </para>
    /// <para>
    /// <b>Four-way branch</b>, matching the ABI's overload set: <c>{empty, explicit} offsets</c>
    /// x <c>{with, without} callback</c>. The offsets-taking overload accepts a
    /// <see langword="null"/> callback (Java's legal <c>commitAsync(Map, null)</c>), but the
    /// callback-only overload does <b>not</b> — it throws <see cref="ArgumentNullException"/> on
    /// null (a recorded, deliberately-stricter-than-Java divergence, CLAUDE.md §4) — so the
    /// no-offsets/no-callback case must route to the bare <c>CommitAsync()</c>.
    /// </para>
    /// <para>
    /// <b>No <c>discard_commit_complete</c> analogue is needed here</b>, unlike the C server
    /// (<c>server.cc:376-380</c>): M9/P7's binding absorbed the ABI's non-nullable-callback
    /// problem internally (the discard trampoline lives inside <c>NativeConsumer</c>), so this
    /// server passes <see langword="null"/> and the binding does the rest.
    /// </para>
    /// </remarks>
    /// <inheritdoc/>
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

            return Task.CompletedTask;
        });
    }

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

    /// <summary>
    /// Closes and evicts a consumer. Ordering is <b>resolve → close → evict</b> (M9/P4 H2), the
    /// async mirror of <c>ConsumerServiceImpl.Close</c>.
    /// </summary>
    /// <remarks>
    /// This servicer cannot hit the negative-timeout throw that motivated the fix — it ignores
    /// <c>timeout_ms</c> entirely (see below) — but the eviction-before-close shape and the
    /// orphaning <c>catch</c> were identical, and the registry-drain sweep needs both servicers
    /// consistent. So the reorder is applied symmetrically: evict only after the close ran, and
    /// on failure dispose then evict so the native handle is never orphaned.
    /// </remarks>
    /// <inheritdoc/>
    public override async Task<Proto.StatusResponse> Close(Proto.ConsumerCloseRequest request, ServerCallContext context)
    {
        // Non-destructive resolve, like every other RPC.
        ConsumerEntry? entry = Get(request.ConsumerId);
        if (entry is null)
        {
            // Close is idempotent — silent success on an unknown id (Python / Java parity).
            return new Proto.StatusResponse();
        }

        try
        {
            // The binding's Close() gracefully closes AND releases the native handle
            // (Consumer_close -> Consumer_destroy), so no separate Dispose is needed here.
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

            // Evict only after the close actually ran — eviction WITHOUT a close orphans the
            // consumer (Close/DisposeAsync are idempotent, so a duplicate Close RPC is safe).
            _consumers.TryRemove(request.ConsumerId, out _);
            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            // Never orphan: dispose, then evict, so the native handle is released even when the
            // close failed.
            try
            {
                await entry.Consumer.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Best-effort teardown; the original failure is what the caller needs.
            }

            _consumers.TryRemove(request.ConsumerId, out _);
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <summary>
    /// Reads (without clearing) the consumer's user-callback log (M9/P9) — the async mirror of
    /// <c>ConsumerServiceImpl.GetCallbackLog</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Gate-exempt</b>, the second exemption after <see cref="Wakeup"/> (see the type
    /// remarks): it touches only <see cref="_callbackLog"/>, which carries its own lock, and
    /// gating it would serialise it behind a long <see cref="Poll"/> — precisely the two calls
    /// the harness's <c>poll_until_kind</c> loop alternates.
    /// </para>
    /// <para>
    /// <b>⚠ An unknown or closed <c>consumer_id</c> returns an EMPTY response, not an error</b>
    /// — deliberately unlike every other handler here, which returns
    /// <see cref="Translate.UnknownConsumer"/>. This inconsistency is load-bearing and must not
    /// be "fixed": the log <em>outlives</em> its consumer by design, so once <see cref="Close"/>
    /// has evicted the registry entry there is no consumer left to resolve, and an
    /// unknown-consumer error would make every post-close read fail — breaking the proto's
    /// "entries survive <c>Close</c>" contract for the callbacks a close itself drives. "Unknown
    /// consumer" is simply not an error condition for this RPC. Python behaves identically and
    /// for the same reason (<c>grpc_server_async.py:520-523</c>).
    /// </para>
    /// </remarks>
    /// <inheritdoc/>
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
        /// <remarks>
        /// <b>Deliberately never disposed</b> (M9/P4, Critic 41 finding 1). A
        /// <see cref="SemaphoreSlim"/> holds no OS handle unless
        /// <see cref="SemaphoreSlim.AvailableWaitHandle"/> is read — which nothing in this
        /// repository ever does — and it registers no finalizer, so dropping the entry reclaims
        /// it by GC alone. Disposing it would buy no resource safety and would add two failure
        /// modes for concurrent same-id RPCs: <c>Release()</c> on a disposed gate throws
        /// <see cref="ObjectDisposedException"/> (breaking <see cref="Close"/>'s documented
        /// idempotence), and <see cref="SemaphoreSlim.Dispose()"/> drops pending
        /// <c>WaitAsync()</c> waiters without completing or faulting them, hanging that RPC to
        /// the client deadline. Both need ≥2 (resp. ≥3) concurrent gated RPCs on one
        /// <c>consumer_id</c>, so they were latent, not active — but the shape is the defect.
        /// The sync servicer is symmetric by construction: its gate is a monitor over a plain
        /// <c>object</c>, so there is nothing there to dispose either.
        /// </remarks>
        internal SemaphoreSlim Gate { get; } = new SemaphoreSlim(1, 1);
    }
}
