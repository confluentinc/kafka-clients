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

namespace Confluent.Kafka.Internal;

/// <summary>
/// The per-send carrier behind one <see cref="IDeliveryCallback"/> — the user's callback plus the
/// two record fields Java's placeholder metadata needs (topic and partition). Allocated
/// <b>only</b> when a callback was supplied (M14/P1 decision D11), so the plain
/// <c>Send(record)</c> path stays allocation-identical.
/// </summary>
/// <remarks>
/// <para>
/// <b>A THIRD callback family: managed-only, never crossing the ABI (ffi §A6).</b> The binding's
/// other two families both cross the C boundary — the ABI's synchronous
/// <c>RecordMetadata_copy</c> convenience (ffi §A6) and the <c>*_async</c> push completions
/// (ffi §A7 / §B6). This one does not: the delivery callback rides on the <b>existing</b> pull
/// completion path (ffi §A7 Option C — the singular <c>Producer_send</c> plus the batched
/// <c>FutureRecordMetadata_get_all</c> pump, or the sync blocking <c>FutureRecordMetadata_get</c>),
/// so there is <b>no</b> <c>[UnmanagedFunctionPointer]</c> delegate, no <c>GCHandle</c>, no
/// <c>user_data</c>, no <c>user_data_destroy</c> hook and <b>no new <c>[DllImport]</c></b> — the
/// whole family is Mode A. Consequently none of §A6's keep-alive / no-unwind-into-native
/// machinery applies to it; the only ABI-adjacent obligation it inherits is that
/// <see cref="Fire"/> must not throw, because it is called from the pump thread's batch loop
/// (where an escaping exception would fault every other send in the batch) and from inside the
/// sync send's handle-freeing <c>try</c>/<c>finally</c>. That guard lives <b>here and nowhere
/// else</b>: no call site wraps <see cref="Fire"/> in a second <c>try</c>/<c>catch</c>, which
/// would shadow this one without adding anything.
/// </para>
/// <para>
/// <b><see cref="Fire"/> is the single firing site for both producer flavors.</b> The sync and
/// async send paths are separate code (the sync send blocks on the record's own future; the async
/// one hands it to <see cref="SendCompletionPump"/>), which makes "fixed in one flavor only" the
/// natural bug here. Routing both through this one method means the ordering (D3), the non-null
/// placeholder metadata (D2/D6), the exception coercion, and the swallow-and-trace policy (D4)
/// cannot diverge between them.
/// </para>
/// </remarks>
internal sealed class DeliveryRegistration
{
    private readonly IDeliveryCallback _callback;
    private readonly string _topic;
    private readonly int _partition;

    /// <summary>
    /// Captures the callback and the two fields the failure-path placeholder metadata needs.
    /// </summary>
    /// <param name="callback">The user callback (non-null; the public <c>Send</c> skin rejects null, D8).</param>
    /// <param name="topic">The record's topic.</param>
    /// <param name="partition">
    /// The record's explicit partition, or <c>-1</c> when it let the producer choose (D6).
    /// </param>
    internal DeliveryRegistration(IDeliveryCallback callback, string topic, int partition)
    {
        _callback = callback;
        _topic = topic;
        _partition = partition;
    }

    /// <summary>
    /// Invokes the user callback with the send's outcome — <b>a total no-throw boundary</b>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Non-null metadata on every path (D2/D6).</b> Java's user callback never sees a null:
    /// <c>AppendCallbacks.onCompletion</c> substitutes
    /// <c>new RecordMetadata(topicPartition(), -1, -1, NO_TIMESTAMP, -1, -1)</c> before forwarding
    /// (<c>KafkaProducer.java:1597-1599</c>), and <c>Callback.java:28-33</c> documents that as the
    /// contract. So a null <paramref name="metadata"/> here is replaced with the placeholder built
    /// from the captured topic / partition. The core's error carries no resolved partition, so the
    /// record's explicit partition is used when it had one and <c>-1</c> otherwise — a recorded
    /// deviation from Java, whose <c>topicPartition()</c> prefers the resolved partition. The
    /// placeholder is allocated <b>only</b> on the failure path.
    /// </para>
    /// <para>
    /// <b>Swallow and trace (D4).</b> Java logs and swallows a throwing user callback
    /// (<c>ProducerBatch.java:318-320</c>); Python logs and swallows
    /// (<c>producer.py:108-117</c>); this binding's own <see cref="IOffsetCommitCallback"/>
    /// adapter already does exactly this. A throw here must not (a) abort the pump's batch loop and
    /// strand the remaining indices' <see cref="System.Threading.Tasks.TaskCompletionSource{TResult}"/>s,
    /// (b) escape into the pump loop's own <c>catch</c> — which faults the <em>whole</em> batch,
    /// turning one user bug into N failed sends — or (c) become the exception the caller of the
    /// sync <c>Send</c> sees. Catching <em>inside</em> this method is what makes the per-index
    /// guard the plan calls for exist exactly once: callers must NOT add a second
    /// <c>try</c>/<c>catch</c> around <see cref="Fire"/>, which would shadow this one without
    /// adding anything.
    /// </para>
    /// <para>
    /// The whole body — not just the user call — is guarded, because the placeholder construction
    /// and the exception coercion run on the same threads and have the same nowhere-to-surface
    /// problem. The trace itself is no-throw: a host-installed
    /// <see cref="System.Diagnostics.TraceListener"/> can throw, and diagnostics must never
    /// escalate into a stalled pump.
    /// </para>
    /// </remarks>
    /// <param name="metadata">
    /// The published metadata on success, or <see langword="null"/> to deliver the placeholder.
    /// </param>
    /// <param name="failure">
    /// The send's failure, or <see langword="null"/> on success. A <see cref="KafkaException"/> — every
    /// ordinary delivery failure — is passed through <b>unchanged</b>, so the callback and the awaiter
    /// receive the identical object. A non-<see cref="KafkaException"/> (only reachable if marshalling
    /// an already-resolved success fails, e.g. an <see cref="OutOfMemoryException"/> decoding the
    /// topic) is <b>wrapped</b>, because the signature carries <see cref="KafkaException"/>: the
    /// callback then sees a <see cref="KafkaException"/> whose
    /// <see cref="Exception.InnerException"/> is the original, while the awaiter sees the original
    /// itself. So the two surfaces report the same <em>failure</em> on every path, but not the same
    /// <em>object</em> on that one.
    /// </param>
    internal void Fire(RecordMetadata? metadata, Exception? failure)
    {
        try
        {
            // Java's placeholder, built only when there is no real metadata (the failure path).
            RecordMetadata delivered = metadata ?? new RecordMetadata(_topic, _partition, -1L, -1L);

            KafkaException? exception = failure switch
            {
                null => null,
                KafkaException kafkaFailure => kafkaFailure,
                _ => new KafkaException(
                    "The send completed but its result could not be marshalled.", failure),
            };

            _callback.OnCompletion(delivered, exception);
        }
        catch (Exception callbackFailure)
        {
            TraceSwallowed(callbackFailure);
        }
    }

    /// <summary>
    /// Writes a swallowed delivery-callback failure to <see cref="System.Diagnostics.Trace"/> —
    /// the binding's only diagnostics sink, deliberately minimal: no logging abstraction, no
    /// dependency, no public API. Same shape as the offset-commit callback's swallow site
    /// (<c>ConsumerCallbacks.TraceSwallowed</c>, M9/P7 decision P7-D3 option (b)) so the two are
    /// greppable together; only the reason clause differs, because naming
    /// <c>OffsetCommitCallback</c> here would be a confidently-wrong diagnostic — the exact defect
    /// that site's own remarks warn against.
    /// </summary>
    /// <remarks>
    /// Itself totally no-throw, because it runs from the <c>catch</c> of a no-throw boundary: a
    /// host-installed <see cref="System.Diagnostics.TraceListener"/> can throw, and diagnostics
    /// must never escalate into a stalled completion pump.
    /// </remarks>
    private static void TraceSwallowed(Exception exception)
    {
        try
        {
            System.Diagnostics.Trace.TraceError(
                "Confluent.Kafka: an IDeliveryCallback (OnCompletion) threw; the exception was " +
                "swallowed. Java's Callback.onCompletion returns void, so there is no channel to " +
                "report it on. Details: {0}",
                exception);
        }
        catch (Exception)
        {
            // See the remarks: a throwing TraceListener must not stall the send-completion pump.
        }
    }
}
