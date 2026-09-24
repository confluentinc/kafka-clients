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
/// Thread-safe, per-client-id log of user-callback invocations — the C# port of
/// <c>grpc_translate.py</c>'s <c>CallbackLog</c> (<c>:287-332</c>) and the C server's
/// <c>CallbackLog</c> (<c>bindings/c/grpc_server/server.cc:214-252</c>). One instance per
/// servicer, read back over the wire by that servicer's <c>GetCallbackLog</c> RPC.
/// </summary>
/// <remarks>
/// <para>
/// <b>It serves BOTH services (M14/P2).</b> The class started out consumer-only (M9/P9) and its
/// documentation was written that way; the producer servicers now use it too, for
/// <see cref="LoggingDeliveryCallback"/>. Nothing about the mechanism is consumer-specific, so
/// the class is <em>shared, not forked</em> — the key is a <b>client id</b>, meaning a
/// <c>consumer_id</c> on <c>ConsumerService</c> and a <c>producer_id</c> on
/// <c>ProducerService</c>, exactly as Python names the same parameter <c>client_id</c> for the
/// same reason (<c>grpc_translate.py:287-332</c>). The two id spaces cannot collide: each
/// servicer owns its own instance and its own <see cref="System.Threading.Interlocked"/> id
/// counter, so a producer id never indexes a consumer's log.
/// </para>
/// <para>
/// <b>The lock is mandatory, not defensive</b> — and each service reaches it from a different
/// set of threads. Rebalance-listener and offset-commit callbacks are invoked on the Rust core's
/// <em>callback-dispatcher thread</em> (ffi-marshalling.md §B6 — a foreign thread). The
/// delivery callback is <em>managed-only</em> (ffi-marshalling.md §A6 form C) and fires on
/// whichever thread reads the send's completion: the producer's <b>send-completion pump
/// thread</b> on the async servicer, and a gRPC handler thread blocked inside <c>Send</c> on the
/// sync one — one such thread per concurrent send, since concurrent <c>Send</c> on one
/// <c>IProducer</c> is supported and unsynchronized. Meanwhile <c>GetCallbackLog</c> is served on
/// a gRPC worker thread, and the thread driving the triggering op is a further context. None of
/// those are reliably the same thread, so the log needs real synchronization. What the lock covers
/// is <em>appends arriving from those threads onto this one shared log</em> — the contended state
/// is the log itself, not any callback instance.
/// </para>
/// <para>
/// <b>It is deliberately its OWN lock, separate from the consumer servicers'
/// <c>ConsumerEntry.Gate</c></b> (their per-consumer-id op gate; the producer servicers have no
/// gate at all — the producer is thread-safe, ffi-marshalling.md §A1). Both anchors do the same —
/// Python a dedicated <c>threading.Lock</c> (<c>grpc_translate.py:299</c>), C a mutex separate
/// from the id-map's (<c>server.cc:216-219</c>). Sharing the op gate would make a callback firing
/// mid-<c>Poll</c> queue behind that very poll, and would serialise <c>GetCallbackLog</c> behind
/// it too — which is exactly what the harness's <c>poll_until_kind</c> loop alternates.
/// </para>
/// <para>
/// <b>Ownership: this lives on the SERVICER, never on the id-map entry.</b> Every servicer's
/// <c>Close</c> handler <c>TryRemove</c>s its registry entry — the consumer servicers on the
/// success path <em>and</em> the failure path, the producer servicers before closing at all — so
/// a log hung off the entry would vanish the moment a client closed, silently breaking the
/// proto's "entries survive Close" contract and every post-close read. The map below is keyed by
/// client id and is never evicted; the callbacks a close itself drives (a final commit callback,
/// an <c>on_partitions_lost</c>, a delivery callback for a record the close's flush completes)
/// are precisely the ones a test wants to read afterwards — Python says the same at its own
/// <c>GetCallbackLog</c> (<c>grpc_server.py:223-227</c>). This is the concrete form of the C server's
/// <c>9465e197</c> use-after-free fix, which made its per-client <c>LogState</c> session-lifetime
/// for the same reason.
/// </para>
/// <para>
/// <b>Unbounded on purpose.</b> Entries are never dropped and the map is never cleared, matching
/// both anchors (<c>grpc_translate.py:322-328</c>, <c>server.cc:226-238</c>). The reasoning is
/// recorded here rather than only in a design doc because "a map that is never cleared" reads as
/// a leak on sight: this process's lifetime is <b>one test session</b>, ids are handed out one
/// per <c>CreateConsumer</c> / <c>CreateProducer</c>, and each entry is a few small protobuf
/// messages — so the map cannot grow meaningfully. A per-id cap was considered and rejected: it
/// diverges from both anchors and could silently evict the very entry a test is polling for,
/// turning a real failure into a baffling one.
/// </para>
/// </remarks>
internal sealed class CallbackLog
{
    private readonly object _gate = new object();
    private readonly Dictionary<ulong, List<Proto.CallbackLogEntry>> _entries =
        new Dictionary<ulong, List<Proto.CallbackLogEntry>>();

    /// <summary>
    /// Records one callback invocation for <paramref name="clientId"/>, mirroring Python's
    /// <c>append(client_id, kind, partitions=(), offsets=None, error="")</c>.
    /// </summary>
    /// <param name="clientId">
    /// The server-local client id the callback belongs to — a <c>consumer_id</c> on
    /// <c>ConsumerService</c>, a <c>producer_id</c> on <c>ProducerService</c> (see the type
    /// remarks).
    /// </param>
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
        ulong clientId,
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
            if (!_entries.TryGetValue(clientId, out List<Proto.CallbackLogEntry>? list))
            {
                list = new List<Proto.CallbackLogEntry>();
                _entries[clientId] = list;
            }

            list.Add(entry);
        }
    }

    /// <summary>
    /// The client's log as a <c>CallbackLogResponse</c>, <b>chronological, oldest first</b>.
    /// Reading does <b>not</b> clear the log (proto contract), and an id with nothing logged —
    /// including one whose consumer or producer has been closed and evicted, or one that never
    /// existed — yields an empty response rather than an error.
    /// </summary>
    /// <param name="clientId">
    /// The server-local client id to read — a <c>consumer_id</c> on <c>ConsumerService</c>, a
    /// <c>producer_id</c> on <c>ProducerService</c> (see the type remarks).
    /// </param>
    internal Proto.CallbackLogResponse Response(ulong clientId)
    {
        Proto.CallbackLogResponse response = new Proto.CallbackLogResponse();
        lock (_gate)
        {
            if (_entries.TryGetValue(clientId, out List<Proto.CallbackLogEntry>? list))
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

/// <summary>
/// A real <see cref="IDeliveryCallback"/> that records the delivered record's metadata in a
/// <see cref="CallbackLog"/> — the C# port of <c>grpc_translate.py</c>'s
/// <c>make_logging_delivery_callback</c> (<c>:391-415</c>). Passed by the <c>Send</c> RPC of
/// <em>both</em> producer servicers when <c>SendRequest.with_callback</c> is set, so what the
/// harness reads back is what the <em>binding's own</em> delivery-callback plumbing actually
/// delivered — the stated purpose of the flag (<c>producer_service.proto:31-37</c>: the closure
/// the Rust client passes "can only prove the client-side plumbing").
/// </summary>
/// <remarks>
/// <para>
/// Emits a <c>"delivery"</c> entry in the shape the proto's <c>CallbackLogEntry</c> table pins
/// (<c>producer_service.proto:214-222</c>): <c>partitions</c> is the delivered-to
/// <c>(topic, partition)</c>, <c>offsets</c> maps that partition's shared
/// <c>"&lt;topic&gt;-&lt;partition&gt;"</c> key (<see cref="Translate.OffsetKey"/>) to the
/// offset, and <c>error</c> is the exception's message or the empty string. The Rust harness
/// rebuilds the identical key (<c>tests/common/callback_log.rs</c>) and asserts
/// <c>error.is_empty()</c> on the success path.
/// </para>
/// <para>
/// <b>Divergence from Python, decided here (M14/P2): the guard is on the <em>partition
/// sentinel</em>, not on the error.</b> Python's helper guards with
/// <c>if metadata is not None</c> and, when it is <see langword="null"/>, appends
/// <c>partitions=()</c> / <c>offsets=None</c> (<c>grpc_translate.py:400-406</c>) — because
/// <c>producer.py</c> hands its <c>on_delivery</c> a <c>None</c> on failure. That exact guard has
/// no reachable branch here: <see cref="IDeliveryCallback"/>'s <c>metadata</c> is <b>never</b>
/// <see langword="null"/>, since on failure the binding substitutes Java's placeholder (M14/P1
/// decisions D2/D6; <c>KafkaProducer.java:1597-1599</c>, contract at <c>Callback.java:28-33</c>).
/// But the placeholder's partition is the record's explicit partition <em>or</em> <c>-1</c> when
/// the record let the producer choose (<c>record.Partition ?? -1</c>), and <c>-1</c> there is a
/// <b>"no partition" sentinel, not a partition index</b>. So the guard this type needs is
/// <c>metadata.Partition &gt;= 0</c>, and that is what it applies.
/// </para>
/// <para>
/// Three consequences, none of them the same as "suppress on error":
/// <list type="bullet">
/// <item><b>Success:</b> identical to Python — the resolved partition and its offset.</item>
/// <item><b>Failure, producer was to choose the partition:</b> identical to Python — no
/// partition, no offset, only the <c>error</c>. There genuinely is no partition to name.</item>
/// <item><b>Failure, the record named a partition explicitly:</b> <em>more</em> informative than
/// Python, which reports neither field. .NET reports that partition with offset <c>-1</c>,
/// because the record was demonstrably destined for it and the placeholder carries it.</item>
/// </list>
/// </para>
/// <para>
/// <b>Why the sentinel is not passed through instead.</b> It is not expressible: the binding's
/// <see cref="TopicPartition"/> — the value type <see cref="CallbackLog.Append"/> takes — rejects
/// a negative partition with <see cref="System.ArgumentOutOfRangeException"/> by Java-parity
/// design, so carrying <c>-1</c> would mean either widening that shared signature or fabricating
/// an index the producer never assigned. A <c>-1</c> on the wire would also read as a partition
/// to any cross-backend comparison, which is the opposite of informative.
/// </para>
/// <para>
/// ⚠ <b>And the guard is load-bearing, not defensive.</b> Without it this method <em>throws</em>
/// on every failed send whose record let the producer choose — and the binding <b>swallows</b> a
/// throwing delivery callback (M14/P1 decision D4), so the entry would be silently <em>absent</em>
/// rather than differently shaped. That is strictly worse than either shape and no managed
/// assertion in this project would catch it; it was caught in this phase's self-review, after a
/// first cut that constructed the <see cref="TopicPartition"/> unconditionally.
/// </para>
/// <para>
/// The in-scope conformance test exercises only the success path, so no assertion distinguishes
/// the failure shapes today: the failure-path behaviour above is a recorded decision, not a
/// tested one.
/// </para>
/// <para>
/// <b>The body cannot throw, and takes no lock of its own.</b> Its one branch is the sentinel
/// guard above; everything else is formatting and an append. Both its fields are
/// <see langword="readonly"/> and it keeps nothing between invocations, so there is no state here
/// to guard; the append is guarded by <see cref="CallbackLog"/>'s lock, whose thread topology is
/// recorded in that type's remarks. A second lock here would add nothing.
/// </para>
/// </remarks>
internal sealed class LoggingDeliveryCallback : IDeliveryCallback
{
    private readonly CallbackLog _log;
    private readonly ulong _producerId;

    internal LoggingDeliveryCallback(CallbackLog log, ulong producerId)
    {
        _log = log;
        _producerId = producerId;
    }

    /// <inheritdoc/>
    public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
    {
        List<TopicPartition>? partitions = null;
        Dictionary<string, long>? offsets = null;

        // metadata is never null (M14/P1 D2/D6), so Python's `if metadata is not None` has no
        // branch here. What DOES need guarding is the placeholder's partition: it is -1 when the
        // record let the producer choose, and -1 is a "no partition" sentinel that TopicPartition
        // rejects (ArgumentOutOfRangeException). Constructing it unconditionally would throw, and
        // the binding swallows a throwing delivery callback — so the entry would go MISSING on
        // every such failure instead of merely being shaped differently. See the type remarks.
        if (metadata.Partition >= 0)
        {
            partitions = new List<TopicPartition>(1)
            {
                new TopicPartition(metadata.Topic, metadata.Partition),
            };

            offsets = new Dictionary<string, long>(1)
            {
                [Translate.OffsetKey(metadata.Topic, metadata.Partition)] = metadata.Offset,
            };
        }

        _log.Append(_producerId, Translate.KindDelivery, partitions, offsets, exception?.Message);
    }
}
