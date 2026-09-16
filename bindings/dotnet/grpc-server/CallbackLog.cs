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

using System.Collections.Generic;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Thread-safe, per-consumer-id log of user-callback invocations — the C# port of
/// <c>grpc_translate.py</c>'s <c>CallbackLog</c> (<c>:287-332</c>) and the C server's
/// <c>CallbackLog</c> (<c>bindings/c/grpc_server/server.cc:214-252</c>). One instance per
/// servicer, read back over the wire by the <c>GetCallbackLog</c> RPC.
/// </summary>
/// <remarks>
/// <para>
/// <b>The lock is mandatory, not defensive.</b> Rebalance-listener and offset-commit
/// callbacks are invoked on the Rust core's <em>callback-dispatcher thread</em>
/// (ffi-marshalling.md §B6 — a foreign thread), while <c>GetCallbackLog</c> is served on a
/// gRPC worker thread, and the app thread driving the triggering op is a third context (in
/// the async servicer the request task is a fourth). None of those are the same thread, so
/// the log needs real synchronization.
/// </para>
/// <para>
/// <b>It is deliberately its OWN lock, separate from <c>ConsumerEntry.Gate</c></b> (the
/// per-consumer-id op gate). Both anchors do the same — Python a dedicated
/// <c>threading.Lock</c> (<c>grpc_translate.py:299</c>), C a mutex separate from the id-map's
/// (<c>server.cc:216-219</c>). Sharing the op gate would make a callback firing mid-<c>Poll</c>
/// queue behind that very poll, and would serialise <c>GetCallbackLog</c> behind it too — which
/// is exactly what the harness's <c>poll_until_kind</c> loop alternates.
/// </para>
/// <para>
/// <b>Ownership: this lives on the SERVICER, never on <c>ConsumerEntry</c>.</b> Both servicers'
/// <c>Close</c> handlers <c>TryRemove</c> the <c>ConsumerEntry</c> from the registry (on the
/// success path <em>and</em> the failure path), so a log hung off the entry would vanish the
/// moment a consumer closed — silently breaking the proto's "entries survive Close" contract
/// and every post-close read. The map below is keyed by consumer id and is never evicted; the
/// callbacks a <c>close()</c> drives (a final commit callback, an <c>on_partitions_lost</c>)
/// are precisely the ones a test wants to read afterwards. This is the concrete form of the C
/// server's <c>9465e197</c> use-after-free fix, which made its per-client <c>LogState</c>
/// session-lifetime for the same reason.
/// </para>
/// <para>
/// <b>Unbounded on purpose.</b> Entries are never dropped and the map is never cleared, matching
/// both anchors (<c>grpc_translate.py:322-328</c>, <c>server.cc:226-238</c>). The reasoning is
/// recorded here rather than only in a design doc because "a map that is never cleared" reads as
/// a leak on sight: this process's lifetime is <b>one test session</b>, ids are handed out one
/// per <c>CreateConsumer</c>, and each entry is a few small protobuf messages — so the map cannot
/// grow meaningfully. A per-id cap was considered and rejected: it diverges from both anchors and
/// could silently evict the very entry a test is polling for, turning a real failure into a
/// baffling one.
/// </para>
/// </remarks>
internal sealed class CallbackLog
{
    private readonly object _gate = new object();
    private readonly Dictionary<ulong, List<Proto.CallbackLogEntry>> _entries =
        new Dictionary<ulong, List<Proto.CallbackLogEntry>>();

    /// <summary>
    /// Records one callback invocation for <paramref name="consumerId"/>, mirroring Python's
    /// <c>append(client_id, kind, partitions=(), offsets=None, error="")</c>.
    /// </summary>
    /// <param name="consumerId">The server-local consumer id the callback belongs to.</param>
    /// <param name="kind">One of the <c>Translate.Kind*</c> constants.</param>
    /// <param name="partitions">
    /// The partitions the callback was invoked with; <see langword="null"/> for none.
    /// </param>
    /// <param name="offsets">
    /// <c>"&lt;topic&gt;-&lt;partition&gt;"</c> -&gt; offset; <see langword="null"/> (the
    /// rebalance kinds) for none.
    /// </param>
    /// <param name="error">
    /// The callback's error message, or <see langword="null"/> when it saw no error — which is
    /// normalized to the <b>empty string</b>. The proto field is
    /// "empty-string-not-absent" (<c>producer_service.proto</c>), so it is always set: the Rust
    /// harness asserts <c>error.is_empty()</c> and every backend must agree.
    /// </param>
    internal void Append(
        ulong consumerId,
        string kind,
        IEnumerable<TopicPartition>? partitions = null,
        IReadOnlyDictionary<string, long>? offsets = null,
        string? error = null)
    {
        // Build outside the lock — the critical section is just the append (C does the same,
        // moving a fully-built entry in).
        Proto.CallbackLogEntry entry = new Proto.CallbackLogEntry
        {
            Kind = kind,
            Error = error ?? string.Empty,
        };

        if (partitions is not null)
        {
            foreach (TopicPartition partition in partitions)
            {
                entry.Partitions.Add(Translate.CallbackLogPartitionToProto(partition));
            }
        }

        if (offsets is not null)
        {
            foreach (KeyValuePair<string, long> pair in offsets)
            {
                // Indexer, not Add: this runs on the core's dispatcher thread where a throw has
                // nowhere to go (the commit callback's is swallowed; the listener's would fail the
                // rebalance), so never risk MapField.Add's duplicate-key throw.
                entry.Offsets[pair.Key] = pair.Value;
            }
        }

        lock (_gate)
        {
            if (!_entries.TryGetValue(consumerId, out List<Proto.CallbackLogEntry>? list))
            {
                list = new List<Proto.CallbackLogEntry>();
                _entries[consumerId] = list;
            }

            list.Add(entry);
        }
    }

    /// <summary>
    /// The consumer's log as a <c>CallbackLogResponse</c>, <b>chronological, oldest first</b>.
    /// Reading does <b>not</b> clear the log (proto contract), and an id with nothing logged —
    /// including one whose consumer has been closed and evicted, or one that never existed —
    /// yields an empty response rather than an error.
    /// </summary>
    internal Proto.CallbackLogResponse Response(ulong consumerId)
    {
        Proto.CallbackLogResponse response = new Proto.CallbackLogResponse();
        lock (_gate)
        {
            if (_entries.TryGetValue(consumerId, out List<Proto.CallbackLogEntry>? list))
            {
                // Copies the element references into the response's own repeated field, so the
                // response is a snapshot: later appends to `list` cannot mutate it. Entries are
                // never mutated after Append builds them, so sharing the messages is safe —
                // identical to Python's `list(self._entries.get(client_id, ()))`.
                response.Entries.Add(list);
            }
        }

        return response;
    }
}

/// <summary>
/// A real <see cref="IConsumerRebalanceListener"/> that records each invocation in a
/// <see cref="CallbackLog"/> — the C# port of <c>grpc_translate.py</c>'s
/// <c>LoggingRebalanceListener</c> (<c>:344-372</c>). Registered by the <c>Subscribe</c> RPC
/// when <c>SubscribeRequest.with_listener</c> is set, so what the harness reads back is what
/// the <em>binding's own</em> listener plumbing actually delivered.
/// </summary>
/// <remarks>
/// <para>
/// <b><see cref="OnPartitionsLost"/> is implemented EXPLICITLY, and must stay that way.</b>
/// This type implements <see cref="IConsumerRebalanceListener"/> directly rather than deriving
/// from <see cref="ConsumerRebalanceListenerBase"/>, whose Java-faithful default delegates
/// <c>OnPartitionsLost</c> to <c>OnPartitionsRevoked</c>. That default is right for users and
/// wrong here: inheriting it would log a <c>lost</c> as <c>revoked</c>, making the two
/// indistinguishable in the log and quietly destroying the only signal that separates them.
/// Python is emphatic about the same point (<c>grpc_translate.py:366-369</c>).
/// </para>
/// <para>
/// <b>The methods are sync <c>void</c> and never touch the consumer.</b> They fire on the core's
/// callback-dispatcher thread with the rebalance blocked until they return (CLAUDE.md §4, the
/// rebalance-listener divergence), so they do the minimum: format and append. Calling back into
/// the owning consumer would be rejected by the core's access guard anyway — the sanctioned
/// escape hatch (<c>ConsumerHandle</c>) is out of scope here and deliberately unused.
/// </para>
/// </remarks>
internal sealed class LoggingRebalanceListener : IConsumerRebalanceListener
{
    private readonly CallbackLog _log;
    private readonly ulong _consumerId;

    internal LoggingRebalanceListener(CallbackLog log, ulong consumerId)
    {
        _log = log;
        _consumerId = consumerId;
    }

    /// <inheritdoc/>
    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
        _log.Append(_consumerId, Translate.KindAssigned, partitions);

    /// <inheritdoc/>
    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
        _log.Append(_consumerId, Translate.KindRevoked, partitions);

    /// <inheritdoc/>
    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        _log.Append(_consumerId, Translate.KindLost, partitions);
}

/// <summary>
/// A real <see cref="IOffsetCommitCallback"/> that records the committed offsets in a
/// <see cref="CallbackLog"/> — the C# port of <c>grpc_translate.py</c>'s
/// <c>make_logging_commit_callback</c> (<c>:375-388</c>). Passed by the <c>CommitAsync</c> RPC
/// when <c>CommitAsyncRequest.with_callback</c> is set.
/// </summary>
/// <remarks>
/// <para>
/// Emits a <c>"commit"</c> entry whose <c>partitions</c> are the committed partitions and whose
/// <c>offsets</c> map each of them to the committed offset under the shared
/// <c>"&lt;topic&gt;-&lt;partition&gt;"</c> key (<see cref="Translate.OffsetKey"/>).
/// </para>
/// <para>
/// <b>The two error surfaces are distinct</b> (ffi-marshalling.md §B5): a
/// <see cref="KafkaException"/> thrown <em>synchronously</em> by <c>CommitAsync</c> is a
/// commit-<b>initiation</b> failure and is reported by the RPC's <c>StatusResponse</c>; the
/// <paramref name="exception"/> delivered here is the commit's own outcome and is what lands in
/// the entry's <c>error</c> field (empty string when null, mirroring Java's "exception == null
/// means success").
/// </para>
/// <para>
/// Runs on the core's dispatcher thread, where a throw is caught and <b>swallowed</b> by the
/// binding — so the body is kept allocation-simple and non-throwing rather than relying on that.
/// </para>
/// </remarks>
internal sealed class LoggingCommitCallback : IOffsetCommitCallback
{
    private readonly CallbackLog _log;
    private readonly ulong _consumerId;

    internal LoggingCommitCallback(CallbackLog log, ulong consumerId)
    {
        _log = log;
        _consumerId = consumerId;
    }

    /// <inheritdoc/>
    public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception)
    {
        List<TopicPartition> partitions = new List<TopicPartition>(offsets.Count);
        Dictionary<string, long> committed = new Dictionary<string, long>(offsets.Count);
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> pair in offsets)
        {
            partitions.Add(pair.Key);
            committed[Translate.OffsetKey(pair.Key.Topic, pair.Key.Partition)] = pair.Value.Offset;
        }

        _log.Append(_consumerId, Translate.KindCommit, partitions, committed, exception?.Message);
    }
}
