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
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Internal lifecycle wrapper over an owned <c>kafka_producer_Producer_t</c>: it
/// orchestrates config marshalling → construction → graceful-close → destroy, and owns the
/// <see cref="SafeProducerHandle"/> (M11/P1 foundation; M11/P2 peripherals; M11/P2.1 teardown
/// collapse). It lives under <c>Internal/</c> — not <c>Internal/Interop/</c> — because it uses
/// only the safe managed <see cref="Utf8Marshal.Pin(string)"/>,
/// <see cref="SafeProducerPropertiesHandle"/>, and
/// <see cref="System.Runtime.InteropServices.SafeHandle"/> APIs, so it needs no
/// <c>unsafe</c> (that stays quarantined to <c>Internal/Interop/</c>, CLAUDE.md §2).
/// The producer twin of <c>NativeConsumer</c> — the same one-layer construct → teardown shape,
/// mirrored method-for-method on the producer ABI.
/// </summary>
/// <remarks>
/// <para>
/// This is <b>not</b> the public client. The public <c>IAsyncProducer</c> /
/// <c>AsyncKafkaProducer</c> / <c>AsyncMockProducer</c> types are <b>thin forwarders</b> over this
/// wrapper (<c>Close(ct) =&gt; _native.CloseWithCallback(ct)</c>, <c>Dispose() =&gt; _native.Dispose()</c>,
/// <c>DisposeAsync() =&gt; _native.DisposeAsync()</c>), exactly as the consumer's public types
/// forward to <c>NativeConsumer</c>. M11/P1 built the interop + lifecycle foundation; M11/P2 added
/// the async PERIPHERALS — <see cref="FlushWithCallback"/> / <see cref="PartitionsForWithCallback"/>
/// over the shipped push completion bridge (<see cref="OperationCompletionSource"/>, ffi §A7 push
/// option). M11/P3 added the SEND surface — <see cref="SendViaPump"/> over the inline pull-pump
/// (<see cref="SendCompletionPump"/>, ffi §A7 Option C: singular <c>Producer_send</c> inline +
/// batched <c>get_all</c> on one pump thread) plus the mock send-control helpers
/// (<see cref="MockCompleteNext"/> / <see cref="MockErrorNext"/> / <see cref="MockHistoryCount"/> /
/// <see cref="MockClear"/>). M14/P1 threaded the optional <see cref="DeliveryRegistration"/> through
/// both send methods — Java's second <c>send(record, Callback)</c> signature — firing it on the pump
/// thread (async) or inline on the caller's thread (sync); it is managed-only and adds no
/// <c>[DllImport]</c>.
/// </para>
/// <para>
/// <b>Teardown — one layer, three flavors, graceful-close-before-destroy (M11/P2.1).</b> This
/// wrapper owns the whole teardown, mirroring <c>NativeConsumer</c> method-for-method (the M11/P2
/// two-layer split via the now-deleted <c>ProducerTeardown</c> is collapsed here):
/// <list type="bullet">
/// <item><see cref="Dispose"/> — sync graceful close (<c>Producer_close</c>) then
/// <c>Producer_destroy</c>, <b>swallowing</b> the close error (the
/// blocking fallback; a best-effort teardown has no caller to hand a failure to).</item>
/// <item><see cref="DisposeAsync"/> — async graceful close (<see cref="CloseWithCallbackInternal"/> →
/// <c>Producer_close_async</c>) then destroy, <b>swallowing</b> the close error (the primary
/// path).</item>
/// <item><see cref="CloseWithCallback"/> — async graceful close then destroy, <b>surfacing</b> the close error
/// (Java <c>Producer.close()</c>; behind the public <c>Close(CancellationToken)</c>).</item>
/// </list>
/// There is no <c>Producer_close_with_timeout</c> ABI (unlike the consumer), so the producer has no
/// timed-close flavor — the M11/P2 <c>Close(TimeSpan)</c> overload + its .NET-side timer race were
/// removed in M11/P2.1 for strict Python-producer parity (Python's producer <c>close</c> has no
/// timeout param). The <see cref="CloseWithCallbackInternal"/> async bridge and the inline sync
/// <c>Producer_close</c> in <see cref="Dispose"/> carry the swallow-vs-surface split; the async
/// bridge also takes the span-the-op <see cref="SafeProducerHandle"/> ref.
/// </para>
/// <para>
/// <b>Finalizer avoidance (ffi §A2).</b> <c>Producer_destroy</c> blocks (it drops the runtime,
/// waiting for the background Sender task), which is wrong on the finalizer thread, so the producer
/// closes via <see cref="Dispose"/> / <see cref="DisposeAsync"/> and never a finalizer. There is no
/// finalizer on this type; the owned <see cref="SafeProducerHandle"/> retains the runtime's
/// critical-finalizer safety net for the leaked-without-Dispose case.
/// </para>
/// <para>
/// <b>Idempotent, ObjectDisposedException-guarded — one merged latch.</b> The atomic
/// <see cref="_closed"/> latch (the M11/P2 wrapper's <c>_closed</c> merged with M11/P1's
/// idempotent-dispose guard) makes <see cref="CloseWithCallback"/> / <see cref="Dispose"/> /
/// <see cref="DisposeAsync"/> mutually one-shot — the first caller wins via
/// <see cref="TryBeginClose"/> and runs close→destroy; later / concurrent callers no-op — and gates
/// use-after-teardown (every op + <see cref="Handle"/> throws
/// <see cref="ObjectDisposedException"/> via <see cref="ThrowIfClosed"/> once closed). An
/// <c>int</c> + <see cref="Interlocked"/> rather than a plain <c>bool</c> because a torn read/write
/// is a race .NET has and Python's GIL hides — the same discipline as the consumer's
/// <c>_closed</c> flag.
/// </para>
/// </remarks>
internal sealed class NativeProducer : IDisposable, IAsyncDisposable
{
    private readonly SafeProducerHandle _handle;

    // Thread-safe closed latch (the teardown gate + use-after-teardown guard): 0 = open,
    // 1 = closing/closed. Makes CloseWithCallback / Dispose / DisposeAsync mutually one-shot and gates
    // use-after-teardown. Atomic (not a plain bool) to avoid a torn read/write race — .NET has
    // it, Python's GIL hides it. Mirrors NativeConsumer._closed.
    private int _closed;

    // The send-completion pump (ffi §A7 Option C), started lazily on the first Send so a
    // send-less producer (peripherals only) never spins a thread. Guarded by _pumpLock, which is
    // ordered against the _closed latch: EnsurePump refuses (ThrowIfClosed) once the latch is won,
    // and teardown reads _pump under the lock AFTER winning the latch — so no pump is created after
    // teardown starts (PLAN §6.3).
    private readonly object _pumpLock = new object();
    private SendCompletionPump? _pump;

    // The binding-side send accumulator + its batch thread (M11/P3.1), started lazily with the pump
    // on the first ASYNC send. A sync-only producer starts neither. Guarded by the same _pumpLock
    // and the same ordering against the _closed latch, so teardown never races one into existence.
    private SendAccumulator? _accumulator;

    // One permanently-pinned NUL-terminated UTF-8 buffer per DISTINCT topic name, for the deferred
    // async send path (M11/P3.1 §4.1). Interning rather than a per-record topic pin keeps the cost
    // at O(distinct topics) permanent pins instead of O(records) transient ones; a topic beyond the
    // cache's cap falls back to a per-record pin the send releases itself. Not used by the sync
    // send, which still pins the topic call-scoped (§3.1).
    private readonly PinnedTopicCache _topics = new PinnedTopicCache();

    // How long teardown waits for the send-batch thread's final drain (§3.8 step 4, §6.3). Long
    // enough that a healthy drain finishes even when send_batch is briefly parked on the core's
    // buffer.memory, short enough that Dispose returns rather than inheriting max.block.ms (60 s by
    // default) for a whole chunk of records. On expiry the outcome is DEFINED, not a hang — see
    // StopAccumulator and SendAccumulator.Stop.
    private static readonly TimeSpan s_accumulatorDrainTimeout = TimeSpan.FromSeconds(30);

    // How long teardown waits for the completion pump to take the LAST queued groups off its queue,
    // after the gate is closed and the teardown flush has resolved them (M11/P3.2 S4, decision D3).
    // The same 30 s as the accumulator's drain bound above and for the same reason: long enough that
    // a healthy pump always wins (it needs only to be scheduled — the flush already resolved what it
    // will read), short enough that Dispose returns. On expiry the outcome is DEFINED, not a hang —
    // SendCompletionPump.Stop faults the remainder exactly as it did before S4.
    private static readonly TimeSpan s_pumpDrainTimeout = TimeSpan.FromSeconds(30);

    // The send-admission wait's bound: the user's `max.block.ms`, resolved ONCE at construction
    // (M11/P3.3). Every other accumulator setting still comes from the environment, read when the
    // accumulator is first created.
    //
    // ⚠ Read EAGERLY, and the producer retains no reference to the caller's config map (Critic 72
    // finding 72.9). It used to hold the IReadOnlyDictionary and let SendAccumulatorSettings read
    // the key inside EnsureAccumulator — i.e. on the first async send. IReadOnlyDictionary does not
    // stop its owner mutating the underlying Dictionary, so a user editing their config map between
    // constructing the producer and its first send silently changed the effective max.block.ms
    // while the core had already been configured from the values read here; it also pinned the
    // user's dictionary for the producer's lifetime. Every other key in the map is consumed eagerly
    // in Create, so this one is now too. (The ENVIRONMENT overrides stay late-bound on purpose —
    // SendAccumulatorSettings documents why — but a user config value is not in that class.)
    private readonly int _maxBlockMs;

    private NativeProducer(SafeProducerHandle handle, int maxBlockMs)
    {
        _handle = handle;
        _maxBlockMs = maxBlockMs;
    }

    /// <summary>
    /// The owned producer handle. Throws <see cref="ObjectDisposedException"/> once
    /// closed (the use-after-teardown guard). Exposed for the interop tests, which
    /// drive the raw ABI against it; the public client will not expose the handle.
    /// </summary>
    internal SafeProducerHandle Handle
    {
        get
        {
            ThrowIfClosed();
            return _handle;
        }
    }

    /// <summary>
    /// Creates a real producer from a config map: each entry becomes a
    /// <c>ProducerProperties_put</c> (keys are the Java dotted names, CLAUDE.md §4),
    /// then <c>KafkaProducer_new</c> reads the properties (the caller retains
    /// ownership of the properties handle and frees it afterward, ffi §A2). A
    /// construction failure surfaces as a <see cref="KafkaException"/>.
    /// </summary>
    /// <param name="config">Config keyed by Java dotted names; values are strings.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    internal static NativeProducer Create(IReadOnlyDictionary<string, string> config)
    {
        // Preconditions BEFORE any pin/marshal/P-Invoke (ffi §A5): the ABI does not
        // validate them and panics on violation (UB across FFI).
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        foreach (KeyValuePair<string, string> entry in config)
        {
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"Configuration value for key '{entry.Key}' must not be null.",
                    nameof(config));
            }
        }

        SafeProducerHandle handle;
        IntPtr error;

        SafeProducerPropertiesHandle props = SafeProducerPropertiesHandle.Create();
        try
        {
            foreach (KeyValuePair<string, string> entry in config)
            {
                using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                NativeMethods.ProducerPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
            }

            // props is passed as the SafeHandle, so the marshaller keeps it alive
            // across the call; the ABI does NOT consume it (we free it below, per the
            // header's "caller must free it separately"). The producer handle arrives
            // ALREADY WRAPPED — the marshaller invokes SafeProducerHandle's private
            // ctor and sets the pointer atomically (M2/P2), closing the
            // create→SetHandle allocation-gap window.
            handle = NativeMethods.KafkaProducerNew(props, out error);
        }
        finally
        {
            // Header: the caller retains props ownership → free it after the call.
            props.Dispose();
        }

        // Operational error: non-null handle = failure. FromHandle frees it exactly
        // once and returns null on success (IntPtr.Zero).
        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            // On the null native return the marshaller handed back an IsInvalid
            // SafeProducerHandle; dispose it (ReleaseHandle is skipped for an
            // IsInvalid handle, so NO spurious Producer_destroy) before throwing.
            handle.Dispose();
            throw failure;
        }

        // Defensive guard (ABI contract: a null out_error implies a non-null handle,
        // per the header). Never store an IsInvalid handle — the Handle property
        // would then hand back a null pointer to callers. A (null handle, null error)
        // return would be a core contract violation, surfaced as a KafkaException.
        if (handle.IsInvalid)
        {
            handle.Dispose();
            throw new KafkaException(
                "kafka_producer_KafkaProducer_new returned a null handle without an error.");
        }

        // `max.block.ms` is read HERE, once, like every other key in the map (see _maxBlockMs); the
        // map itself is not retained.
        return new NativeProducer(handle, SendAccumulatorSettings.ReadMaxBlockMs(config));
    }

    /// <summary>
    /// Creates a broker-less mock producer.
    /// </summary>
    /// <param name="autoComplete">
    /// When <see langword="true"/> (the default), the mock resolves sends
    /// automatically; when <see langword="false"/>, sends are resolved manually via
    /// the mock helpers (a later phase). Passed as a C <c>bool</c>
    /// (<c>[MarshalAs(I1)]</c>).
    /// </param>
    internal static NativeProducer CreateMock(bool autoComplete = true)
    {
        // Non-fallible: MockProducer_new always returns a valid owned handle,
        // already wrapped by the marshaller (M2/P2). No out_error, no IsInvalid guard.
        SafeProducerHandle handle = NativeMethods.MockProducerNew(autoComplete);

        // No config map: MockProducer_new takes none, so the accumulator's `max.block.ms` falls
        // back to the Kafka default (60 s), exactly as it does for a real producer that omits it.
        return new NativeProducer(handle, SendAccumulatorSettings.DefaultMaxBlockMs);
    }

    /// <summary>The submit shape shared by the two void-result async peripherals.</summary>
    private delegate void NativeSubmit(
        IntPtr producer,
        ProducerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// The submit shape for the owned-handle async peripheral (<c>partitions_for</c>). Passes
    /// only <c>(producer, userData)</c>; the op closes over its own strongly-typed rooted
    /// callback at the call site (mirrors <c>NativeConsumer.NativeOwnedHandleSubmit</c>).
    /// </summary>
    private delegate void NativeOwnedHandleSubmit(IntPtr producer, IntPtr userData);

    /// <summary>
    /// Flushes all pending records (async; Java <c>Producer.flush()</c>). The returned
    /// <see cref="Task"/> completes when the core resolves the flush (successfully for a
    /// <c>MockProducer</c> — no pending sends → immediate success), or faults with a
    /// <see cref="KafkaException"/>. Reuses the shipped void completion bridge
    /// (<see cref="OperationCompletionSource"/> + <see cref="ProducerCallbacks.Operation"/>)
    /// over <c>Producer_flush_async</c> (ffi §A7 push).
    /// </summary>
    /// <remarks>
    /// <b>Cancellation is best-effort — the .NET wait only.</b> The producer has no
    /// <c>wakeup()</c> (unlike the consumer), so a canceled <paramref name="cancellationToken"/>
    /// cancels the awaiter directly (<see cref="OperationCompletionSource{TResult}.CancelAwaiter"/>
    /// wired via <see cref="OperationCompletionSource{TResult}.RegisterCancellation"/>); the
    /// native flush continues to completion and frees its own rooting on the dispatcher thread
    /// (the straggler callback's <c>TrySet*</c> on the already-canceled TCS is a safe no-op).
    /// There is no native abort path (ffi §A7 nuance).
    /// </remarks>
    /// <param name="cancellationToken">Best-effort cancellation of the .NET wait (no native abort).</param>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task FlushWithCallback(CancellationToken cancellationToken = default)
    {
        SendAccumulator? accumulator = AccumulatorToStop();
        if (accumulator is null)
        {
            // No async send ever started an accumulator — nothing is buffered on this side, so this
            // is the unchanged core flush.
            return FlushCore(cancellationToken);
        }

        // Preconditions must still be SYNCHRONOUS (ffi §A5), so they run here rather than inside the
        // async continuation below.
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();
        return FlushAfterDrain(accumulator, cancellationToken);
    }

    /// <summary>
    /// Drains the accumulator, then flushes the core (M11/P3.1 §3.5) — the deliberate divergence
    /// <b>toward Java</b> that the Python anchor lacks.
    /// </summary>
    /// <remarks>
    /// Python's <c>flush()</c> never signals its send thread, so it can return while records are
    /// still buffered inside the binding — arguably violating Java's contract that <c>flush()</c>
    /// blocks until every <em>previously sent</em> record completes, since from the caller's view
    /// those records <b>were</b> sent (<c>send()</c> returned). Draining first is what makes
    /// "flushed" mean the same thing on both sides of the accumulator. The teardown flush needs no
    /// equivalent change: <see cref="StopAccumulator"/> already drains at step 4, before it.
    /// </remarks>
    private async Task FlushAfterDrain(SendAccumulator accumulator, CancellationToken cancellationToken)
    {
        await accumulator.DrainPendingAsync(cancellationToken).ConfigureAwait(false);
        await FlushCore(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The core half of the flush: <c>Producer_flush_async</c> over the completion bridge.</summary>
    private Task FlushCore(CancellationToken cancellationToken)
    {
        return SubmitVoidOperation(cancellationToken, (producer, callback, userData) =>
            NativeMethods.ProducerFlushAsync(producer, callback, userData));
    }

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (async; Java
    /// <c>Producer.partitionsFor(String)</c>) — the owned-handle <c>PartitionInfoList_t</c>
    /// completion, reusing the consumer's shared list type + the shipped
    /// <see cref="Interop.PartitionInfoListMarshal"/> (copy-out) and
    /// <see cref="ProducerCallbacks.PartitionsFor"/> trampoline. The returned
    /// <see cref="Task{TResult}"/> resolves with an owned <see cref="IReadOnlyList{PartitionInfo}"/>
    /// (the whole borrowed tree copied out on the dispatcher thread before the root destroy,
    /// ffi §B2/§6.4), or faults with a <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// <b>Mock reachability (honest caveat, PLAN §2).</b> On a <c>MockProducer</c> this succeeds
    /// broker-free but returns an <b>empty</b> list for every topic: the only mock ctor
    /// (<c>MockProducer_new</c>) builds an empty <c>Cluster</c>, so <c>partitions_for</c> returns
    /// <c>Ok(&lt;empty&gt;)</c>. A populated list is integration-only (a real
    /// <c>KafkaProducer.partitionsFor</c> does live metadata). This is a success with an empty
    /// result, not a fault — and NOT a reason to add a mock-seeding ctor (that would be Mode B).
    /// <b>Empty topic is forwarded, not rejected</b> (Java/Python-faithful): the binding guards
    /// only null (FFI panic-safety, §A5). The single topic is pinned <b>call-scoped</b> — the
    /// core copies it synchronously during the submit (ffi §A3). Best-effort cancellation as
    /// <see cref="FlushWithCallback"/>.
    /// </remarks>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <param name="cancellationToken">Best-effort cancellation of the .NET wait (no native abort).</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<IReadOnlyList<PartitionInfo>> PartitionsForWithCallback(
        string topic,
        CancellationToken cancellationToken = default)
    {
        // Precondition BEFORE any pin / P-Invoke (ffi §A5): the ABI does not null-check `topic`
        // (it would panic across FFI). An EMPTY topic is NOT rejected — Java/Python do no topic
        // validation; the binding guards only null (the consumer PartitionsFor precedent).
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        return SubmitOwnedHandleOperation<IReadOnlyList<PartitionInfo>>(
            cancellationToken,
            (producer, userData) =>
            {
                // Call-scoped pin: partitions_for_async copies the topic synchronously during the
                // submit (the header requires only a valid C string for the call's duration — no
                // borrow past the return), so the buffer is freed once the native call returns
                // (ffi §A3). A single topic → the scoped Pin, not an array helper.
                using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
                NativeMethods.ProducerPartitionsForAsync(
                    producer, topicPin.Pointer, ProducerCallbacks.PartitionsFor, userData);
            });
    }

    /// <summary>
    /// Sends a single record on the async surface, returning a <see cref="Task{TResult}"/> (the async
    /// send worker; Java <c>Producer.send(record)</c>). Named <c>SendViaPump</c>, <b>not</b>
    /// <c>SendWithCallback</c> like the peripherals: there is no native <c>Producer_send_async</c>
    /// callback — completion arrives via the pump's batched <c>get_all</c> (ffi §A7's pull surface),
    /// so a <c>WithCallback</c> suffix would misdescribe the mechanism (M11/P3 PLAN §6.2). The
    /// <c>ViaPump</c> suffix names that real mechanism, distinguishing it from the blocking sync
    /// <see cref="Send"/> (which has no pump).
    /// <para>
    /// <b>M11/P3.1 — the send SUBMISSION side is now the Python binding's shape (slice S3).</b> This
    /// method no longer touches the core at all: it pins the record's buffers, appends them to the
    /// binding-side <see cref="SendAccumulator"/>, and returns the <see cref="Task{TResult}"/>
    /// immediately. A dedicated batch thread drains the accumulated chain — on a threshold or a
    /// free-running window — into a blittable <see cref="Interop.ProducerRecordNative"/> array,
    /// issues <c>kafka_producer_Producer_send_batch</c> per chunk, unpins, and hands the resulting
    /// futures to the <b>unchanged</b> <see cref="SendCompletionPump"/>. The anchor is
    /// <c>bindings/python/_confluentkafka.c</c>'s <c>Producer_send_thread</c>.
    /// </para>
    /// <para>
    /// <b>The SYNC <see cref="Send"/> is deliberately NOT moved</b> (M11/P3.1 §3.1): it returns a
    /// fully-materialized <see cref="RecordMetadata"/>, so routing it through a 0–10 ms accumulator
    /// window would add that window to every sync send. It still calls the singular
    /// <c>Producer_send</c> inline with call-scoped <c>fixed</c> pins.
    /// </para>
    /// <para>
    /// <b>M11/P3.2 — records reach <c>send_batch</c> in the order a caller called this method</b>
    /// (§F1). Deferring the append opened a window in which a send that found capacity could
    /// overtake one still waiting for it; the routing now runs through
    /// <see cref="SendAccumulator.SubmitAdmitted"/> — the accumulator's own
    /// <c>TrySubmitInline</c> / <c>SubmitQueued</c> pair, private since M11/P3.3 — which refuses the
    /// inline path while anything is
    /// queued ahead and appends everything queued from a single FIFO submitter. Java documents
    /// ordering as preserved in the default configuration
    /// (<c>ProducerConfig.java:274</c>), and the reorder happened <em>before</em> the core saw the
    /// records, so no core-side setting could have restored it.
    /// </para>
    /// <para>
    /// <b>M11/P3.3 — admission is bounded, and this method BLOCKS the calling thread when the bound
    /// is saturated.</b> Once the producer holds
    /// <see cref="SendAccumulatorSettings.MaxAdmittedRecords"/> records that have been accepted but
    /// not yet handed to the batch thread, this call parks the caller for up to the configured
    /// <c>max.block.ms</c>; if the bound is still saturated when that elapses, the record is refused
    /// with a <b>retriable</b> <see cref="KafkaException"/> delivered through the returned
    /// <see cref="Task{TResult}"/> — and through the delivery callback — rather than thrown, which is
    /// Java's own outcome for buffer exhaustion (<c>KafkaProducer.java:1049-1061</c>; the citation
    /// chain is at <see cref="SendAccumulator.SubmitAdmitted"/>). Blocking up to
    /// <c>max.block.ms</c> is Java's shape too — <c>KafkaProducer.send()</c> blocks that long once
    /// the accumulator is full — and it is the only thing that throttles: the caller is handed the
    /// record's delivery
    /// <see cref="Task{TResult}"/> rather than an admission handle (M11/P3.2 deviation DV-1), so an
    /// asynchronous admission wait bounds a container while the accepted population grows without
    /// limit (measured: 2.05 M records in flight, p50 3,524 ms, RSS 2.04 GiB). The bound covers
    /// <b>both</b> submission routes, because capping one container only relocates the pile-up
    /// (§2.3, measured). The <b>sync</b> <see cref="Send"/> has no such window — it hands the record
    /// to the core inside the call and blocks on the core's own <c>buffer.memory</c>.
    /// </para>
    /// </summary>
    /// <remarks>
    /// <b>Preconditions (ffi §A5).</b> An already-canceled <paramref name="cancellationToken"/> →
    /// <see cref="OperationCanceledException"/>; a closed producer → <see cref="ObjectDisposedException"/>
    /// — both before any pin. The null-record and serializer-throw preconditions run in
    /// the generic client's <c>Send</c> skin above this carrier (M11/P5, PLAN §5.3), and the
    /// null-topic / negative-partition preconditions live in the
    /// <see cref="ProducerRecord{TKey, TValue}"/> constructor (Java-faithful), so
    /// <paramref name="record"/> is an already-valid, already-serialized value here.
    /// <para>
    /// <b>⚠ The caller must not mutate the key / value buffers after this returns</b> (decision D6,
    /// §4.7). The binding borrows them until the batch thread hands the record to the core, so a
    /// mutation in that window IS visible on the wire. This is inherent to deferring a zero-copy
    /// send — the alternative is the per-record copy CLAUDE.md §12 forbids — and it matches the
    /// anchor, where Python likewise borrows the buffer until its drain. It applies to the
    /// <b>async surface only</b>; the sync <see cref="Send"/> has no such window.
    /// </para>
    /// <para>
    /// <b>Cancellation is best-effort — the .NET wait only.</b> The producer has no <c>wakeup()</c>,
    /// so a token that fires after the record is appended cancels the returned <see cref="Task"/>
    /// (<see cref="TaskCompletionSource{TResult}.TrySetCanceled(CancellationToken)"/> — cancelled
    /// WITH the token, so <c>OperationCanceledException.CancellationToken</c> matches it, exactly as
    /// the async peripherals do); the record is still sent and the later
    /// <c>TrySetResult</c> / <c>TrySetException</c> on the already-canceled TCS is a safe no-op
    /// (ffi §A7). The registration is disposed when the task completes. No registration is created
    /// for a non-cancelable token — the common send path allocates nothing beyond the TCS (DoD §10).
    /// </para>
    /// </remarks>
    /// <param name="record">The already-serialized record to send.</param>
    /// <param name="delivery">
    /// The user's delivery-callback carrier (M14/P1), or <see langword="null"/> on the plain
    /// <c>Send(record)</c> path. It is invoked immediately before the returned
    /// <see cref="Task{TResult}"/> completes (decision D3) — on the pump thread for a record the core
    /// accepted, on the batch thread for one the core rejected per-record, and on <em>this</em>
    /// thread for a record refused by the <c>max.block.ms</c> admission expiry (M11/P3.3, the
    /// Java-faithful buffer-exhausted outcome). A synchronous throw out
    /// of this method fires <b>nothing</b> (decision D5), and under the accumulator every such throw
    /// is a case where the record never reached the core at all: the disposed guard (directly, and
    /// again inside <see cref="EnsureAccumulator"/> under its lock), the already-canceled token, a
    /// pump or accumulator that could not be started, and the accumulator refusing an append because
    /// teardown closed it.
    /// </param>
    /// <param name="cancellationToken">
    /// Best-effort cancellation of the .NET wait (no native abort). It also aborts the admission
    /// wait above, in which case the record is <b>not</b> sent and the cancellation is reported
    /// synchronously, carrying this token.
    /// </param>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was already canceled, or fired while this send was
    /// blocked on the admission bound.
    /// </exception>
    internal Task<RecordMetadata> SendViaPump(
        SerializedProducerRecord record,
        DeliveryRegistration? delivery,
        CancellationToken cancellationToken = default)
    {
        // Preconditions BEFORE any pin (ffi §A5): the ABI does not validate them and panics on
        // violation (UB across FFI). The null-record + serializer-throw preconditions run in the
        // generic client's Send skin (above this carrier — M11/P5, PLAN §5.3), so `record` here is
        // an already-serialized value type; this layer applies only the disposed + already-canceled
        // guards.
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        // Start (or reuse) the accumulator + its completion pump before appending, so an accepted
        // record always has a live drain. Refuses once the producer is closing (ThrowIfClosed under
        // _pumpLock).
        SendAccumulator accumulator = EnsureAccumulator();

        // RunContinuationsAsynchronously is MANDATORY (ffi §A7): otherwise a slow awaiter
        // continuation runs on the pump thread and stalls every other completion.
        TaskCompletionSource<RecordMetadata> completion =
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Best-effort cancellation: cancel the WAIT, never abort the enqueued send. Only wire it
        // for a cancelable token so the common Send(record) path stays allocation-free (DoD §10).
        //
        // Hoisted out of the `if` so the fast path below can dispose it when Submit throws: the
        // disposing continuation is chained to `completion`, which that throw leaves unsettled, so
        // the registration would otherwise stay alive on the caller's token for the token's whole
        // lifetime. `default` is the not-cancelable case, whose Dispose is a no-op.
        CancellationTokenRegistration cancellationRegistration = default;
        if (cancellationToken.CanBeCanceled)
        {
            // Cancel WITH the token (M11/P8, Minor 9), so the resulting OperationCanceledException
            // carries it and the idiomatic .NET catch —
            // `catch (OperationCanceledException e) when (e.CancellationToken == ct)` — matches.
            //
            // The token rides in the state object because the clean overload
            // (Register(Action<object?, CancellationToken>, object?)) is .NET 5+ and the library
            // floor is netstandard2.0. TrySetCanceled(CancellationToken) itself IS available there.
            // The extra allocation is one boxed value tuple on the CANCELABLE path only — the
            // CanBeCanceled guard keeps plain Send(record) allocation-free, so the DoD §10 send-path
            // budget is unaffected.
            cancellationRegistration = cancellationToken.Register(
                static state =>
                {
                    (TaskCompletionSource<RecordMetadata> tcs, CancellationToken token) =
                        ((TaskCompletionSource<RecordMetadata>, CancellationToken))state!;
                    tcs.TrySetCanceled(token);
                },
                (completion, cancellationToken));

            // Dispose the registration once the task settles (by the pump or by cancellation) so a
            // long-lived token does not retain it.
            completion.Task.ContinueWith(
                static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
                cancellationRegistration,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        // Backpressure FIRST, pins second (§4.4/§4.6). Taking a permit before pinning is what keeps
        // a blocked sender from holding pins while it waits — which would turn a bound on RECORDS
        // into an unbounded pin window. Submit does the pinning and the append, and releases both
        // the pins and the permit if the append is refused.
        //
        // ⚠ THE ROUTING DECISION IS THE ACCUMULATOR'S, NOT THIS METHOD'S (M11/P3.2 §F1). It used to
        // read "permit? append inline : await a permit, then append", which let a later send that
        // found capacity overtake an earlier one still waiting for it — the records then reached
        // send_batch, and so the wire, out of call order. TrySubmitInline refuses the inline path
        // while anything is queued ahead, and SubmitQueued puts this send behind it in a FIFO drained
        // by a single appender. Both live on SendAccumulator so the test fixture routes through the
        // same decision (DoD §12) — a fixture with its own copy of the rule would make the ordering
        // test a proof about the fixture.
        //
        // ⚠ THIS METHOD MUST NOT BECOME `async`. It has to stay synchronous so a serializer throw
        // (raised above this carrier, before the call) and the precondition throws above still
        // surface synchronously rather than as a faulted Task — the reason
        // AsyncKafkaProducer.SendValidated is deliberately not `async` either.
        //
        // ⚠ AND THE ADMISSION BOUND IS WHY THAT MATTERS RATHER THAN MERELY BEING TRUE (M11/P3.3).
        // SubmitAdmitted can now BLOCK this caller's thread, for up to max.block.ms, when the
        // producer already holds MaxAdmittedRecords records accepted but not yet handed to the
        // batch thread (the permit comes back at TakeChainLocked, before send_batch runs, so the
        // accepted-but-unsent POPULATION is larger than the bound by the in-flight chain — the
        // overshoot arithmetic is at _admission). That is the fix:
        // the caller does not await admission (it is handed the record's delivery Task), so an
        // ASYNCHRONOUS admission wait throttles nobody and just parks continuations — measured at
        // 3.0 GB / p50 10 s on a sibling branch, and the unbounded shape this replaces measured
        // 2.04 GiB / p50 3.5 s. A blocking SemaphoreSlim.Wait(timeout, token) is a genuine
        // synchronous primitive, NOT an async operation being blocked on, so it is not the
        // sync-over-async footgun ffi §B7 / CLAUDE.md §4 forbid — the same distinction CLAUDE.md §4
        // draws for the consumer's sync Seek. Making this method `async` to "await admission" would
        // break the synchronous-throw contract above AND reintroduce the defect.
        try
        {
            accumulator.SubmitAdmitted(record, completion, delivery, cancellationToken);
        }
        catch (Exception)
        {
            // What reaches here is "nothing reached the core", and so fires no delivery callback
            // (D5). The `catch` is deliberately broad — an unexpected managed failure (an
            // OutOfMemoryException inside the pinning, say) reaches it too, and the conclusion holds
            // for those as well, which is why this note describes the paths rather than counting
            // them (Critic 72 finding 72.8; it used to open "Three synchronous failures"):
            //
            //   * `cancellationToken` fired while this send was blocked on admission ->
            //     OperationCanceledException carrying the CALLER's token;
            //   * teardown closed the accumulator (either while blocked on admission, or between
            //     the guards above and an inline append) -> ObjectDisposedException.
            //
            // ⚠ The max.block.ms expiry is NOT here. It faults `completion` and fires the delivery
            // callback instead of throwing — Java's own outcome for buffer exhaustion
            // (KafkaProducer.java:1049-1061); see SendAccumulator.SubmitAdmitted. That path
            // therefore returns normally through the line below, and the disposing continuation
            // chained to the now-faulted `completion` runs on its own.
            //
            // Each path that DOES throw leaves `completion` unsettled — or settled only by the
            // registration below — so the disposing continuation chained to it may never run.
            // Release the registration here instead; a send routed to the QUEUE and failed there is
            // faulted through `completion` and so disposes it that way.
            cancellationRegistration.Dispose();
            throw;
        }

        // The SAME awaitable on both routes — the record's delivery future, never a submission
        // handle. (It only held before because the slow path re-awaited `completion.Task` itself.)
        return completion.Task;
    }

    /// <summary>
    /// Sends a single record and <b>blocks</b> until the cluster acknowledges it, returning the
    /// resolved <see cref="RecordMetadata"/> directly (the sync producer's worker; Java
    /// <c>producer.send(record).get()</c> — M11/P4 decision #1/#2). Runs entirely on the caller's
    /// thread with <b>no pump / TCS / callback</b>: preconditions → <c>Producer_send</c>
    /// (call-scoped pinning, the core copies key/value synchronously, ffi §A4) → the <b>blocking</b>
    /// <c>FutureRecordMetadata_get</c> → copy-out → free every handle.
    /// </summary>
    /// <remarks>
    /// <b>Direct sync ABI, not sync-over-async (ffi §A1).</b> The blocking <c>get</c>'s
    /// <c>block_on</c> runs inside the Rust core's own multi-thread runtime, parking only this
    /// caller thread (deadlock-free — the Sender keeps running on the runtime's worker pool); it is
    /// never routed through the async binding API. This is the sync-consumer precedent
    /// (<c>consumer-threading.md §1.1</c>) applied to the send path — the sync producer starts
    /// <b>no</b> completion pump (only the async <see cref="SendViaPump"/> does), so a sync-only
    /// <see cref="NativeProducer"/> spins no background thread and its teardown degenerates to the
    /// pump-less path (<see cref="Close"/>).
    /// <para>
    /// <b>Preconditions (ffi §A5), before any pin / P-Invoke.</b> A closed producer →
    /// <see cref="ObjectDisposedException"/>. The null-record and serializer-throw preconditions run
    /// in the generic client's <c>Send</c> skin above this carrier (M11/P5, PLAN §5.3), and the
    /// null-topic / negative-partition preconditions live in the
    /// <see cref="ProducerRecord{TKey, TValue}"/> constructor (Java-faithful), so
    /// <paramref name="record"/> is an already-valid, already-serialized value here. No
    /// <see cref="System.Threading.CancellationToken"/> (the producer has no <c>wakeup()</c> and the
    /// sync surface takes none — decision #4).
    /// </para>
    /// <para>
    /// <b>Frees every handle on every path (ffi §A2).</b> The future is destroyed in the outer
    /// <c>finally</c> (get does not consume it); the metadata in the inner <c>finally</c>; the error
    /// (on failure) by <see cref="KafkaException.FromHandle(IntPtr)"/>. On the failure branch the
    /// metadata handle is null (exactly one of metadata / error is non-null), so its
    /// <see cref="NativeMethods.RecordMetadataDestroy(IntPtr)"/> is a null-safe no-op.
    /// </para>
    /// <para>
    /// <b>Single-owner TEARDOWN (decision #6).</b> Concurrent <see cref="Send"/> itself is
    /// supported (the core's <c>Mutex</c> serializes, ffi §A1); it is teardown that is one-shot.
    /// A blocked <see cref="Send"/> plus a concurrent
    /// <see cref="Dispose"/> from another thread is misuse (like the sync consumer), yet still
    /// memory-safe: the future is Arc-backed and independent of the producer's lifetime, and
    /// <c>Producer_destroy</c> tolerates outstanding futures (P3 round-2 finding). The manual-mock
    /// completion pattern (thread A blocked here, thread B calling
    /// <see cref="MockCompleteNext"/> / <see cref="MockErrorNext"/>) is the intended cross-thread use
    /// and is serialized by the core's producer mutex (ffi §A1), exactly as the async pump's
    /// <c>get_all</c> is unblocked by <c>complete_next</c> today.
    /// </para>
    /// <para>
    /// <b>This is the sync surface's delivery-callback firing site (M14/P1), and it is the SAME
    /// contract as the pump's — decision D5 spelled out.</b> There is no pump here, so
    /// <paramref name="delivery"/> is invoked <b>inline on this caller's thread</b>, after the
    /// blocking <c>get</c> has resolved and <b>before</b> this method returns or throws (decision
    /// D3). The throw sources are deliberately <em>not</em> equivalent:
    /// <list type="bullet">
    /// <item><c>ProducerSendMarshal.Send</c> throwing on the <b>synchronous</b> <c>out_error</c> →
    /// <b>no callback</b>. Nothing was accepted, and Java's <c>doSend</c> likewise re-throws from
    /// <c>catch (KafkaException)</c> without invoking the callback
    /// (<c>KafkaProducer.java:1073-1076</c>). The same holds, further up, for the
    /// <see cref="ObjectDisposedException"/> here and for the null-record / null-callback /
    /// serializer preconditions in the public <c>Send</c> skin.</item>
    /// <item><c>FutureRecordMetadata_get</c> returning an error → <b>callback fires</b>, with Java's
    /// <c>-1</c> placeholder metadata, and <em>then</em> the throw. Java has already fired on the
    /// I/O thread by the time <c>produceFuture.done()</c> releases the caller's
    /// <c>future.get()</c>.</item>
    /// <item>an unexpected managed or native failure in the narrow window <em>between</em> the
    /// blocking <c>get</c> and <see cref="DeliveryRegistration.Fire"/> → <b>no callback</b>, and the
    /// record <em>was</em> accepted. This surface's share of <b>recorded residual 3</b>
    /// (<see cref="IDeliveryCallback"/>), in both of its conditions: the
    /// <see cref="KafkaException.FromHandle(IntPtr)"/> read of a reported error can fail for
    /// sub-case (a)'s reason (a completion was in hand), and <c>FutureRecordMetadata_get</c> is its
    /// own separate native entry point, so the P/Invoke itself can fail for sub-case (b)'s reason
    /// (none was). It is <b>not</b> a separate residual site: there is no batch here to fault
    /// wholesale, so the throw simply propagates out of this method. The mirror-image window on the
    /// async surface is <see cref="SendCompletionPump"/>'s batch-fault path.</item>
    /// </list>
    /// .NET has <b>no analogue of Java's <c>catch (ApiException)</c> row</b> — "the callback fires
    /// inline <em>and</em> a failed future is returned without throwing"
    /// (<c>KafkaProducer.java:1056-1068</c>) — because the core surfaces those failures through the
    /// record's future, not through the synchronous out-param. That is a deviation forced by the
    /// ABI, not a choice.
    /// </para>
    /// </remarks>
    /// <param name="record">The already-serialized record to send.</param>
    /// <param name="delivery">
    /// The user's delivery-callback carrier (M14/P1), or <see langword="null"/> on the plain
    /// <c>Send(record)</c> path. See the remarks for exactly which outcomes invoke it.
    /// </param>
    /// <returns>The published record's metadata.</returns>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a synchronous send failure or a delivery failure.</exception>
    internal RecordMetadata Send(SerializedProducerRecord record, DeliveryRegistration? delivery)
    {
        // Preconditions BEFORE any pin / P-Invoke (ffi §A5): the null-record + serializer-throw
        // preconditions run in the generic client's Send skin above this carrier (M11/P5,
        // PLAN §5.3), so `record` here is an already-serialized value type; this layer applies only
        // the disposed guard. No CancellationToken on the sync surface (decision #4).
        ThrowIfClosed();

        // The ABI maps null partition/timestamp to its own -1 sentinels.
        int partition = record.Partition ?? -1;
        long timestamp = record.Timestamp ?? -1L;

        // Inline call-scoped-pinned send (throws a KafkaException synchronously on out_error). Passes
        // the SafeProducerHandle straight through — the P/Invoke marshaler auto-DangerousAddRef/
        // Releases it around the synchronous Producer_send (sync-op auto-ref, ffi §A2/§A4). Shared
        // verbatim with the async Send path.
        IntPtr future = ProducerSendMarshal.Send(
            _handle, record.Topic, partition, timestamp, record.Key, record.Value);

        try
        {
            // Blocking get on THIS thread (block_on in the core's runtime — deadlock-free, ffi §A1).
            // On success: metadata non-null, getError null. On failure: metadata null, getError
            // non-null (exactly one is non-null, per the header).
            IntPtr metadata = NativeMethods.FutureRecordMetadataGet(future, out IntPtr getError);

            KafkaException? failure = KafkaException.FromHandle(getError);
            if (failure is not null)
            {
                // D5: the completion ARRIVED and reported a failure, so the delivery callback is
                // owed — fired with Java's -1 placeholder metadata (D2/D6), BEFORE the throw (D3),
                // exactly as Java has already fired on the I/O thread before the caller's
                // future.get() unblocks. metadata is null on this branch → nothing to copy out or
                // free (the future is freed in the outer finally).
                delivery?.Fire(null, failure);
                throw failure;
            }

            RecordMetadata result;
            try
            {
                // Copy every field out (topic before the handle dies, ffi §A3) — the result holds no
                // native-backed reference.
                result = RecordMetadataMarshal.CopyOut(metadata);
            }
            catch (Exception exception)
            {
                // The completion arrived (the send succeeded) but its metadata could not be
                // marshalled. Symmetric with the pump's marshal-failure site: the callback is still
                // owed, and the two surfaces report the same FAILURE but not the same object here —
                // this method's caller gets `exception` raw (`throw;` preserves the original stack),
                // while Fire coerces it into the KafkaException its signature demands (the original
                // survives as InnerException). The ONLY path where the two differ; every
                // KafkaException outcome is passed to both unwrapped.
                // Note this catch guards CopyOut ONLY — it deliberately
                // does not enclose the success-path Fire below, so it cannot shadow Fire's own
                // no-throw guard (ffi §A6 form C: exactly one guard, inside Fire).
                delivery?.Fire(null, exception);
                throw;
            }
            finally
            {
                NativeMethods.RecordMetadataDestroy(metadata);
            }

            // D3: the callback runs BEFORE this method returns.
            delivery?.Fire(result, null);
            return result;
        }
        finally
        {
            // get does NOT consume the future (ffi §A2) — free it on every path (singular destroy,
            // no 1-element array).
            NativeMethods.FutureRecordMetadataDestroy(future);
        }
    }

    /// <summary>
    /// Flushes all pending records and <b>blocks</b> until the core resolves the flush (the sync
    /// producer's <see cref="Confluent.Kafka.KafkaProducer{TKey, TValue}.Flush"/> worker; Java
    /// <c>Producer.flush()</c> — M11/P4). Calls the <b>sync</b> <c>Producer_flush</c> directly
    /// (call-scoped <see cref="SafeProducerHandle"/> auto-ref, decision #3) and <b>surfaces</b> a
    /// flush error via <see cref="KafkaException.FromHandle(IntPtr)"/> — unlike teardown's swallow.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The core reported a flush failure, or the binding-side send accumulator did not drain within
    /// its bound so records buffered in the binding are not covered by this flush.
    /// </exception>
    internal void Flush() => FlushWithAccumulatorDrainBound(s_accumulatorDrainTimeout);

    /// <summary>
    /// <see cref="Flush()"/> with the accumulator-drain bound supplied rather than defaulted — the
    /// <c>internal</c> seam that makes the expiry branch below reachable without a broker.
    /// </summary>
    /// <remarks>
    /// <b>Why a seam at all.</b> The expiry fires whenever
    /// <see cref="SendAccumulator.DrainPending"/> returns <see langword="false"/>, which needs no
    /// broker — only an accumulator that is not empty-and-idle at the deadline. What made it
    /// untestable was purely that <see cref="s_accumulatorDrainTimeout"/> is a private static, so a
    /// test could not shorten it. This follows the precedent
    /// <see cref="SendAccumulatorSettings"/> already set with its <c>internal</c> constructor: a
    /// test reaches an explicit value through an <c>internal</c> entry point instead of mutating
    /// process-wide state that a parallel test class could observe.
    /// <para>
    /// Production behaviour is unchanged: <see cref="Flush()"/> is the only production caller and it
    /// passes the same <see cref="s_accumulatorDrainTimeout"/> the body used to read directly.
    /// </para>
    /// </remarks>
    /// <param name="accumulatorDrainTimeout">
    /// How long to wait for the binding-side accumulator to reach empty-and-idle. <b>Not</b> a flush
    /// timeout — the core flush below is unbounded, as Java's <c>flush()</c> is.
    /// </param>
    internal void FlushWithAccumulatorDrainBound(TimeSpan accumulatorDrainTimeout)
    {
        ThrowIfClosed();

        // Drain the accumulator first (§3.5), for the same reason the async flush does. Reachable
        // only when the same NativeProducer was driven through BOTH surfaces — the sync producer
        // never starts an accumulator — but a flush that silently skipped buffered records would be
        // wrong there too, and the blocking form is the right one on an already-blocking method.
        //
        // The expiry is SURFACED, not discarded. Returning success here with records still buffered
        // in the binding is precisely §3.5's Observation-1 failure — flush() returning while records
        // the caller believes were sent have not reached the core — which this drain exists to fix,
        // so reintroducing it on a bounded timer would be the same defect with a 30 s fuse.
        //
        // The asymmetry with FlushAfterDrain (which awaits DrainPendingAsync with NO timeout) is
        // deliberate and one-directional: the async caller has a CancellationToken to bound its own
        // wait with, so an unbounded drain there is Java-faithful (flush() blocks until every
        // previously-sent record completes) and still escapable. This surface has neither a token
        // nor any other escape, so it takes a bound — and a bound that expires has to say so.
        if (AccumulatorToStop()?.DrainPending(accumulatorDrainTimeout) == false)
        {
            throw new KafkaException(
                "The producer's send accumulator did not drain within " +
                $"{accumulatorDrainTimeout.TotalSeconds:0} seconds, so records buffered in the " +
                "binding have not reached the core and this flush did not include them.");
        }

        NativeMethods.ProducerFlush(_handle, out IntPtr error);
        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> and <b>blocks</b> until the core
    /// resolves it (the sync producer's <see cref="Confluent.Kafka.KafkaProducer{TKey, TValue}.PartitionsFor"/>
    /// worker; Java <c>Producer.partitionsFor(String)</c> — M11/P4). Calls the <b>sync</b>
    /// <c>Producer_partitions_for</c> directly (call-scoped <see cref="SafeProducerHandle"/> auto-ref,
    /// decision #3), copies the owned list out via <see cref="Interop.PartitionInfoListMarshal"/>
    /// (nothing native-backed escapes, ffi §B2/§6.4), and destroys the list root on every path.
    /// </summary>
    /// <remarks>
    /// <b>Mock reachability (honest caveat, PLAN §2), empty topic forwarded.</b> On a
    /// <c>MockProducer</c> this succeeds broker-free but returns an <b>empty</b> list for every topic
    /// (the mock ctor builds an empty cluster) — a success with an empty result, not a fault, exactly
    /// as the async <see cref="PartitionsForWithCallback"/>. The binding guards only null
    /// (FFI panic-safety, §A5); an empty topic is forwarded (Java/Python-faithful). The topic is
    /// pinned call-scoped — the core copies it synchronously during the call (ffi §A3).
    /// </remarks>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <returns>The topic's partitions (an empty list on a <c>MockProducer</c>).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a failure.</exception>
    internal IReadOnlyList<PartitionInfo> PartitionsFor(string topic)
    {
        // Precondition BEFORE any pin / P-Invoke (ffi §A5): the ABI does not null-check `topic`
        // (it would panic across FFI). An EMPTY topic is NOT rejected — Java/Python do no topic
        // validation (the async PartitionsForWithCallback precedent).
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        ThrowIfClosed();

        // Call-scoped topic pin: Producer_partitions_for copies the topic synchronously during the
        // call (ffi §A3). A single topic → the scoped Pin.
        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
        IntPtr error = NativeMethods.ProducerPartitionsFor(_handle, topicPin.Pointer, out IntPtr list);
        try
        {
            KafkaException? failure = KafkaException.FromHandle(error);
            if (failure is not null)
            {
                // On failure the core leaves `list` == IntPtr.Zero (it writes out_list only on Ok);
                // the finally's null-safe destroy is a no-op.
                throw failure;
            }

            // Copy the whole borrowed tree out BEFORE the root is destroyed (ffi §B2/§6.4).
            return PartitionInfoListMarshal.CopyOut(list);
        }
        finally
        {
            // Free the owned root exactly once on every path (null-safe on the failure branch).
            NativeMethods.PartitionInfoListDestroy(list);
        }
    }

    /// <summary>
    /// Returns a point-in-time snapshot of the producer's metrics (Java
    /// <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c>) — a <b>synchronous state read</b>
    /// (CLAUDE.md §4 lists producer <c>Metrics</c> under "stays sync"; the generated header agrees:
    /// "<c>metrics()</c> does not block in Java, so this takes no runtime"). Marshals the owned
    /// (Category-3) <c>kafka_producer_MetricMap_t</c> borrow-root into an owned
    /// <see cref="IReadOnlyDictionary{TKey, TValue}"/> and frees the root exactly once
    /// (§A2/§A3 via <see cref="ProducerMetricMapMarshal"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Concurrency — NOT the consumer's model (M11/P8 decision D-5).</b> The producer is
    /// multi-writer by design: the core serializes through the producer's internal <c>Mutex</c>
    /// (ffi §A1 — "concurrent <c>Send</c> is safe, don't add your own lock"), so this call has no
    /// concurrent-access <em>rejection</em> to surface. It deliberately does <b>not</b> reuse the
    /// consumer's <c>ThrowIfConcurrentNull</c> mapping
    /// (<c>InvalidOperationException("KafkaConsumer is not safe for multi-threaded access.")</c>):
    /// the consumer ABI documents "null on a concurrent-access rejection",
    /// <c>kafka_producer_Producer_metrics</c> documents no such null, so copying that mapping
    /// would state a contract the producer ABI does not have (and name the wrong type).
    /// </para>
    /// <para>
    /// <b>Sync, but it can park.</b> Reading the snapshot takes the coarse core producer
    /// <c>Mutex</c>, so this can block behind an in-flight <c>Send</c> — the same characteristic
    /// every other sync producer op has (the block happens inside the core's own multi-thread
    /// runtime, ffi §A1), not a defect.
    /// </para>
    /// <para>
    /// <b>The null guard is defensive only.</b> The ABI returns null solely for a <b>null</b>
    /// producer handle, which is unreachable here by construction: <see cref="ThrowIfClosed"/>
    /// runs first, and the handle is passed as the <see cref="SafeProducerHandle"/> so the
    /// marshaller holds a call-scoped ref (a closed handle would surface
    /// <see cref="ObjectDisposedException"/> from the marshaller, never a null pointer). The
    /// guard exists so a future ABI change cannot turn a null into a
    /// <see cref="NullReferenceException"/> deep inside the marshaller.
    /// </para>
    /// <para>
    /// A <c>MockProducer</c> returns an <b>empty</b> map broker-free (Java
    /// <c>MockProducer.metrics()</c> returns its <c>mockMetrics</c>, empty unless seeded — and the
    /// ABI exposes no seeding entry point).
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core returned no metric map (unreachable by construction — see the remarks).
    /// </exception>
    internal IReadOnlyDictionary<MetricName, IMetric> Metrics()
    {
        ThrowIfClosed();

        // Sync native call → pass the SafeProducerHandle (ffi §A2): the marshaller's call-scoped
        // auto-ref keeps ReleaseHandle → Producer_destroy from running underneath the call.
        IntPtr map = NativeMethods.ProducerMetrics(_handle);
        if (map == IntPtr.Zero)
        {
            // Defensive only (see the remarks): the ABI nulls solely on a null handle. The message
            // is producer-accurate — it does NOT claim thread-unsafety, which would be false here.
            throw new InvalidOperationException("The producer returned no metrics snapshot.");
        }

        // The marshaller copies every entry out then frees the root exactly once in its own
        // finally (even if a read throws).
        return ProducerMetricMapMarshal.CopyOutAndDestroy(map);
    }

    /// <summary>
    /// Returns the send accumulator, starting it — and the completion pump it feeds — on first use
    /// (lazy: a producer driven only through the sync surface spins neither thread). Refuses to
    /// start once the producer is closing: the <see cref="ThrowIfClosed"/> under
    /// <see cref="_pumpLock"/> is ordered against the <see cref="TryBeginClose"/> latch so teardown
    /// never races a fresh thread into existence (M11/P3 PLAN §6.3).
    /// </summary>
    /// <remarks>
    /// The pump is created <b>first</b> and handed to the accumulator, so the accumulator can never
    /// observe a null drain: by the time its batch thread can produce a future, the pump that owns
    /// that future exists. The settings are read once, here, per producer (§3.2).
    /// </remarks>
    private SendAccumulator EnsureAccumulator()
    {
        SendAccumulator? accumulator = Volatile.Read(ref _accumulator);
        if (accumulator is not null)
        {
            return accumulator;
        }

        lock (_pumpLock)
        {
            // Re-check the latch under the lock: if teardown won it, refuse (no accumulator for a
            // closing producer — teardown would never join its thread).
            ThrowIfClosed();
            SendCompletionPump pump = _pump ??= new SendCompletionPump();

            // The already-resolved `max.block.ms` is passed so the accumulator's admission wait is
            // bounded by the user's own value rather than by a binding-invented number (M11/P3.3);
            // every other setting still comes from the environment, read here.
            return _accumulator ??= new SendAccumulator(
                _handle, _topics, pump, SendAccumulatorSettings.FromEnvironment(_maxBlockMs));
        }
    }

    /// <summary>
    /// <b>Teardown step 1 of the §3.8 handshake: close the accumulator and drain it, BEFORE the
    /// pump's gate closes.</b> Closes it to new appends, wakes the batch thread and joins it; the
    /// thread performs one final drain first, so every record accepted before teardown reaches the
    /// core and its future reaches the <b>still-open</b> pump.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The ordering is load-bearing, not incidental.</b> Doing this after
    /// <see cref="SendCompletionPump.CloseGate"/> would deliver the final drain's futures to a closed
    /// gate, where <see cref="SendCompletionPump.Enqueue"/> faults them in place and fires <b>no</b>
    /// delivery callback — recorded residual 1. Routing normal teardown through a recorded residual
    /// is silent data loss dressed up as an accepted limitation.
    /// </para>
    /// <para>
    /// <b>The §6.3 re-enumeration, with the accumulator in place.</b> The recorded pump-orphan
    /// finding rests on the premise "flush resolves every pending send"; the handshake adds two more
    /// premises, and <b>neither is assumed</b> — each is a bounded wait with a stated outcome:
    /// <list type="bullet">
    /// <item><i>"the accumulator always reaches empty"</i> — it does not have to. The batch thread
    /// can be parked inside <c>send_batch</c> on a full <c>buffer.memory</c> for up to
    /// <c>max.block.ms</c>, and it can in principle fail outright. <c>Stop</c>'s wait is bounded by
    /// <see cref="s_accumulatorDrainTimeout"/> and its expiry outcome is defined (abandon the thread,
    /// which stays memory-safe and still settles every record it holds); an unhandled failure of the
    /// thread itself settles its own chain and closes the accumulator.</item>
    /// <item><i>"every drained node's futures reach the pump before <c>CloseGate</c>"</i> — true
    /// exactly when the drain completed, which is what the return value reports. When it did not,
    /// the late futures take <see cref="SendCompletionPump.Enqueue"/>'s fault-in-place branch:
    /// faulted and freed, never stranded or leaked, at the cost of residual 1's notification.</item>
    /// </list>
    /// The drain result also decides one memory-safety question the caller cannot defer: whether the
    /// interned topic buffers may be freed eagerly. They may only if nothing can still be pointing at
    /// them, i.e. only if the drain completed — otherwise the cache falls back to its finalizer.
    /// </para>
    /// </remarks>
    /// <returns><see langword="true"/> if the accumulator drained and its thread exited.</returns>
    private bool StopAccumulator() => AccumulatorToStop()?.Stop(s_accumulatorDrainTimeout) ?? true;

    /// <summary>
    /// <b>Teardown's last step:</b> frees the interned topic buffers — but <b>only when the
    /// accumulator's drain completed</b>.
    /// </summary>
    /// <remarks>
    /// These pointers are raw and un-ref-counted, and they are what a record's
    /// <c>ProducerRecord_t.topic</c> points at. If the drain completed, every record that ever held
    /// one has been handed to the core and the core has copied the topic
    /// (<c>to_string_lossy().into_owned()</c>), so freeing is safe by construction. If it did not,
    /// the abandoned batch thread may still pass one to <c>send_batch</c>, and freeing here would be
    /// a use-after-free — so the cache is left to its finalizer, which cannot run while the producer
    /// (and therefore the cache) is still reachable from that thread. Deliberately unconditional in
    /// the other direction: a producer that never sent asynchronously has an empty cache, so this is
    /// a no-op rather than a branch to skip.
    /// </remarks>
    private void ReleaseTopicCache(bool accumulatorDrained)
    {
        if (accumulatorDrained)
        {
            _topics.Dispose();
        }
    }

    /// <summary>
    /// Reads the started accumulator (under <see cref="_pumpLock"/>), or <see langword="null"/> if
    /// no async send ever started one. The same latch ordering as <see cref="PumpToStop"/>.
    /// </summary>
    private SendAccumulator? AccumulatorToStop()
    {
        lock (_pumpLock)
        {
            return _accumulator;
        }
    }

    /// <summary>
    /// Blocks until every async send made so far has been handed to the core and its future enqueued
    /// to the completion pump — the accumulator's "drain now and wait" primitive (§3.5/§9). Returns
    /// immediately when no async send ever started an accumulator.
    /// </summary>
    /// <returns><see langword="true"/> if the accumulator reached empty-and-idle within the timeout.</returns>
    internal bool DrainPendingSends(TimeSpan timeout) =>
        AccumulatorToStop()?.DrainPending(timeout) ?? true;

    /// <summary>
    /// <b>Test observation point (M11/P3.1 §3.8), not public API:</b> how many sends the completion
    /// pump has taken off its queue — i.e. how many reached <see cref="SendCompletionPump.Enqueue"/>
    /// while the gate was still <b>open</b>.
    /// </summary>
    /// <remarks>
    /// The teardown ordering (drain the accumulator, THEN close the pump's gate) has no other
    /// observable: get it the wrong way round and every drained future takes <c>Enqueue</c>'s
    /// fault-in-place branch, whose exception type and message are the same ones a send legitimately
    /// gets when the pump's loop lost the race to <c>Stop</c>'s terminal drain — so the accepted
    /// outcome and the defect share an observable and no assertion on the send's <see cref="Task"/>
    /// can separate them. This counter can: it is the number that were <em>queued</em>, which the
    /// inverted ordering drives to zero. Deliberately readable <b>after</b> teardown (no closed
    /// guard), because after teardown is the only useful time to read it.
    /// </remarks>
    internal long DrainedSendCount => PumpToStop()?.DrainedSendCount ?? 0;

    /// <summary>
    /// Completes the next pending mock send successfully (Java <c>MockProducer.completeNext()</c> /
    /// Python <c>complete_next()</c>). Mock only; returns <see langword="false"/> if there is no
    /// pending completion. Inherent on the public <c>AsyncMockProducer</c>, not on the interface.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    internal bool MockCompleteNext()
    {
        ThrowIfClosed();
        return NativeMethods.MockProducerCompleteNext(_handle);
    }

    /// <summary>
    /// Completes the next pending mock send with an error (Java <c>MockProducer.errorNext(...)</c> /
    /// Python <c>error_next(code, message)</c>). Mock only; returns <see langword="false"/> if there
    /// is no pending completion. A null <paramref name="message"/> uses the default message for
    /// <paramref name="code"/> (the ABI's null convention). The message is pinned call-scoped.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    internal bool MockErrorNext(int code, string? message)
    {
        ThrowIfClosed();

        if (message is null)
        {
            return NativeMethods.MockProducerErrorNext(_handle, code, IntPtr.Zero);
        }

        using Utf8Marshal.PinnedUtf8String pinnedMessage = Utf8Marshal.Pin(message);
        return NativeMethods.MockProducerErrorNext(_handle, code, pinnedMessage.Pointer);
    }

    /// <summary>
    /// The number of records in the mock's sent history (Java <c>MockProducer.history().size()</c> /
    /// Python <c>history_count()</c>). Mock only.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    internal int MockHistoryCount()
    {
        ThrowIfClosed();
        return NativeMethods.MockProducerHistoryCount(_handle);
    }

    /// <summary>
    /// Clears the mock's sent history and pending completions (Java <c>MockProducer.clear()</c> /
    /// Python <c>clear()</c>). Mock only.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    internal void MockClear()
    {
        ThrowIfClosed();
        NativeMethods.MockProducerClear(_handle);
    }

    /// <summary>
    /// Reads the started pump (under <see cref="_pumpLock"/>), or <see langword="null"/> if no send
    /// ever started it. The lock is ordered against the close latch: teardown wins the latch before
    /// calling this, so <see cref="EnsureAccumulator"/> cannot create a new pump afterward (its
    /// <see cref="ThrowIfClosed"/> under the same lock throws). Shared by the sync
    /// <see cref="StopPump"/> and the async <see cref="StopPumpAsync"/>.
    /// </summary>
    private SendCompletionPump? PumpToStop()
    {
        lock (_pumpLock)
        {
            return _pump;
        }
    }

    /// <summary>
    /// <b>Sync teardown gate + flush + pump-join</b> (the blocking <see cref="Dispose"/> path).
    /// Closes the pump's enqueue gate, flushes pending sends via the <b>sync</b>
    /// <c>Producer_flush</c>, then stops and joins the pump — the producer-outlives-pump ordering
    /// step (ffi §A2/§A7): runs after the close latch is won and before <c>Producer_close</c> /
    /// <c>Producer_destroy</c>, so no future handle is in use when the producer is destroyed.
    /// <b>The flush ALWAYS runs</b> (M11/P8, Blocker 2); only the gate-close and the join are
    /// skipped when no send ever started a pump.
    /// </summary>
    /// <remarks>
    /// <b>Flush BEFORE the join — the manual-mock no-hang fix (M11/P3).</b> The pump may be blocked
    /// inside a <c>FutureRecordMetadata_get_all</c> on a not-yet-resolved send, and <c>get_all</c>
    /// cannot be interrupted, so <c>_thread.Join()</c> would hang until that future resolves. The
    /// core's <c>Producer_close</c> does <b>not</b> drive pending sends — it only marks the producer
    /// closed (verified <c>src/producer/mock_producer.rs</c>: <c>close</c> sets a flag; only
    /// <c>flush</c> drains and completes the pending completions) — so close cannot unblock the
    /// in-flight <c>get_all</c>. <c>Producer_flush</c> can and does: for a <c>MockProducer</c> it
    /// completes pending sends (their futures resolve, so <c>get_all</c> returns); for a real
    /// producer it delivers-or-times-out (the accepted Option-C bounded residual — Java's
    /// <c>close()</c> flushes pending records too). The sync flush is used only on the sync
    /// <see cref="Dispose"/> path (the async paths await <c>Producer_flush_async</c> via
    /// <see cref="StopPumpAsync"/> to avoid sync-over-async, ffi §A7); its error is swallowed
    /// (teardown is best-effort, and the per-flavor close step carries any surfaced error).
    /// <para>
    /// <b>The flush is unconditional (M11/P8, Blocker 2).</b> It used to sit below an early
    /// <c>pump is null</c> return, so it never ran for a producer driven only through the sync
    /// surface — which never starts a pump, yet CAN have a concurrently blocked <see cref="Send"/>
    /// (the mock-driving pattern <c>IProducer</c> documents as intended). Only the <em>join</em>
    /// has anything to do with the pump's existence; the flush does not. Real-producer cost: one
    /// redundant <c>Producer_flush</c> before <c>Producer_close</c> (which already drains) — Java's
    /// <c>close()</c> flushes too, so this is <em>more</em> Java-faithful, not a new hazard.
    /// </para>
    /// <para>
    /// <b>Gate before flush (M11/P8, Major 5).</b> <see cref="SendCompletionPump.CloseGate"/> runs
    /// FIRST so there is no window in which a send lands after the flush returned but before the
    /// pump is told to stop — such a send would be queued, block the loop in an uninterruptible
    /// <c>get_all</c> nothing resolves, and hang <c>_thread.Join()</c>.
    /// </para>
    /// </remarks>
    private void StopPump()
    {
        bool accumulatorDrained = StopAccumulator();

        SendCompletionPump? pump = PumpToStop();

        // Major 5: close the enqueue gate BEFORE the flush, so a send racing teardown either
        // (a) was enqueued before the gate closed -> the flush below resolves it, or (b) arrives
        // after -> Enqueue faults it in place. No third case, and no window where a send is queued
        // into a pump that is about to be joined.
        pump?.CloseGate();

        try
        {
            // Blocker 2: resolve pending sends FIRST, independent of whether a pump exists. This
            // used to sit BELOW the `pump is null` return, so it never ran for a producer used only
            // through the sync surface (only the async SendViaPump starts a pump) — yet such a
            // producer CAN have a concurrently blocked Send, which IProducer.cs blesses as the
            // intended cross-thread mock-driving use. Flushing unconditionally releases it.
            //
            // The handle is valid here — teardown is single-winner (the latch) and Producer_destroy
            // runs only after StopPump; consistent with the sync Producer_close in Dispose.
            // ProducerFlush takes the SafeProducerHandle (M11/P4 decision #3): the marshaler's
            // call-scoped auto-ref is safe and strictly preferable here too.
            NativeMethods.ProducerFlush(_handle, out IntPtr flushError);
            _ = KafkaException.FromHandle(flushError);
        }
        catch (Exception)
        {
            // Minor 12(a): best-effort teardown — swallow ANY flush error so pump.Stop() below
            // ALWAYS runs and the pump thread is never leaked. This is the StopPumpAsync rationale
            // applied to the sync twin, and it matters more after the hoist above: the flush now
            // runs on strictly more paths.
        }

        // S4 (D3): the flush has now resolved everything the pump will read, so give the pump a
        // BOUNDED chance to take the last queued groups off its queue BEFORE Stop sets _stopping —
        // otherwise a group enqueued by StopAccumulator above is faulted merely because the pump
        // thread was not scheduled in time. Monotone (the gate is closed, so nothing can be added)
        // and bounded, so it cannot hang; on expiry Stop faults the remainder as before. The two
        // rejected shapes — draining from THIS thread, and moving RunLoop's _stopping check below
        // the drain — are argued at WaitForQueueDrain.
        pump?.WaitForQueueDrain(s_pumpDrainTimeout);

        // No pump thread to join when no async send ever started one — but the flush above already
        // ran (Blocker 2). Join outside the lock (the pump's terminal drain does not touch _pumpLock,
        // and holding it across a thread join is needless).
        pump?.Stop();

        ReleaseTopicCache(accumulatorDrained);
    }

    /// <summary>
    /// <b>Async teardown flush + pump-join</b> (the <see cref="DisposeAsync"/> / <see cref="CloseWithCallback"/>
    /// paths). Identical to <see cref="StopPump"/> except the pending-send flush is the <b>async</b>
    /// <c>Producer_flush_async</c> bridge (<see cref="FlushInternal"/>), <c>await</c>ed — so an async
    /// teardown never blocks the caller thread on the sync flush (sync-over-async is forbidden on the
    /// async paths, ffi §A7). The flush still runs <b>before</b> the join, preserving the Issue-1
    /// no-hang property: it resolves the pending sends so the pump's blocking <c>get_all</c> returns
    /// and the join cannot hang (see <see cref="StopPump"/>'s remarks for why close cannot do this and
    /// flush can). It takes the same two M11/P8 moves as its sync twin: the enqueue gate closes
    /// <b>before</b> the flush (Major 5), and the flush runs <b>unconditionally</b>, above the
    /// <c>pump is null</c> return (Blocker 2).
    /// </summary>
    /// <remarks>
    /// <b>The join and destroy stay blocking by design.</b> Only the flush is made async here; the
    /// shared join+destroy tail (<c>pump.Stop()</c> → <c>_thread.Join()</c>, then the flavor's
    /// <c>Producer_destroy</c>) stays synchronous — making the pump-join / destroy awaitable is
    /// deliberately out of scope (it would be over-engineering; the join is a short thread join once
    /// the flush has unblocked <c>get_all</c>). The flush error is swallowed (best-effort teardown;
    /// the per-flavor close step carries any surfaced error).
    /// </remarks>
    private async Task StopPumpAsync()
    {
        bool accumulatorDrained = StopAccumulator();

        SendCompletionPump? pump = PumpToStop();

        // Major 5: close the enqueue gate BEFORE the flush (see StopPump for the full argument).
        pump?.CloseGate();

        try
        {
            // Blocker 2: async flush (Producer_flush_async) FIRST, independent of whether a pump
            // exists — hoisted above the `pump is null` return for the same reason as the sync twin.
            // Consequence: an async teardown of a never-sent-async producer now issues one extra
            // Producer_flush_async round-trip (a GCHandle + a span-the-op ref). That is correct and
            // symmetric with Dispose, and the try/swallow below covers it.
            await FlushInternal().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best-effort teardown — swallow ANY flush error so pump.Stop() below ALWAYS runs and the
            // pump thread is never leaked. Broadened from catch (KafkaException): FlushInternal can
            // also throw a non-KafkaException (ObjectDisposedException from DangerousGetHandle, OOM
            // from GCHandle.Alloc); if that escaped, pump.Stop() would be skipped and the pump thread
            // would leak. This swallows only the FLUSH error — the graceful Producer_close's error is
            // surfaced later by the caller (CloseWithCallback), so the close-error-surfacing contract
            // is preserved.
        }

        // S4 (D3), identical to the sync twin: a BOUNDED, non-draining wait for the pump to take the
        // last queued groups before Stop sets _stopping, so teardown completes what the core
        // accepted instead of faulting it. Monotone behind the closed gate; on expiry it degrades to
        // the pre-S4 fault-the-remainder behaviour. It stays SYNCHRONOUS here for the same reason
        // the join does (see this method's remarks): it is a short wait on an already-resolved
        // backlog, not an I/O await, and making it awaitable would buy nothing.
        pump?.WaitForQueueDrain(s_pumpDrainTimeout);

        // No pump thread to join when no async send ever started one — the flush above already ran.
        // The join stays blocking by design.
        pump?.Stop();

        ReleaseTopicCache(accumulatorDrained);
    }

    /// <summary>
    /// Submits a void-result async peripheral (<c>flush</c>): root the per-op context via a
    /// <see cref="GCHandle"/>, take a span-the-op ref on the producer
    /// <see cref="SafeProducerHandle"/> (so <c>Producer_destroy</c> cannot run until the op's
    /// completion callback releases it — closing the destroy-vs-in-flight-op use-after-free,
    /// ffi §A2/§A7), wire best-effort cancellation, then run <paramref name="submit"/>.
    /// Ownership of the <see cref="GCHandle"/> transfers to the completion callback (its sole
    /// owner) the moment native is entered; if <paramref name="submit"/> throws before that, the
    /// context is abandoned (handle freed) here. Mirrors <c>NativeConsumer.SubmitVoidOperation</c>.
    /// </summary>
    private Task SubmitVoidOperation(CancellationToken cancellationToken, NativeSubmit submit)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count: hold a reference on the producer SafeHandle for the whole async
            // op so ReleaseHandle → Producer_destroy cannot run until the op's completion callback
            // releases it (in FreeGcHandle). Closes the destroy-vs-in-flight-op use-after-free
            // (ffi §A2/§A7) by deferring the native destroy past the op.
            //
            // INSIDE the try (M11/P8, Major 4) — this is the canonical site; the other three submit
            // helpers mirror it, and it mirrors the consumer's own canonical site
            // (NativeConsumer.SubmitVoidOperation, M9/P4 M3). DangerousAddRef throws
            // ObjectDisposedException when a concurrent teardown closed the handle between
            // ThrowIfClosed and here — and the producer is explicitly multi-writer (ffi §A1:
            // "concurrent Send is safe — don't add your own lock"), so this is reachable on a REAL
            // producer, not just a mock. That is a "native never ran" path, so it MUST route through
            // AbandonBeforeSubmit; with the AddRef above the try the throw escaped and the GCHandle
            // allocated two lines up stayed rooted for the PROCESS LIFETIME, silently leaking the
            // OperationCompletionSource + its TaskCompletionSource behind a perfectly plausible
            // ObjectDisposedException.
            //
            // No new free site: AbandonBeforeSubmit → FreeGcHandle is Interlocked-guarded and also
            // does _handleRef?.DangerousRelease() — it releases the reference when the AddRef
            // succeeded and submit then threw, and releases nothing when the AddRef itself threw
            // (SetHandleRef never ran). AbandonBeforeSubmit stays reachable only when native never
            // ran, so the completion callback cannot double-free.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            // No native wakeup() on the producer → cancellation cancels the .NET wait directly
            // (CancelAwaiter); the native op continues + frees its own rooting (ffi §A7 nuance).
            context.RegisterCancellation(cancellationToken, context.CancelAwaiter);
            submit(_handle.DangerousGetHandle(), ProducerCallbacks.Operation, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback will never fire → we own cleanup.
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Submits the owned-handle async peripheral (<c>partitions_for</c>) — the result-returning
    /// analog of <see cref="SubmitVoidOperation"/>. Roots the per-op context, takes the
    /// span-the-op handle ref, wires best-effort cancellation, then runs
    /// <paramref name="submit"/> (which pins the topic call-scoped and P/Invokes with the
    /// owned-handle callback captured at the call site). The result copy-out happens in the
    /// callback on the dispatcher thread (ffi §6.4), not here. Mirrors
    /// <c>NativeConsumer.SubmitOwnedHandleOperation</c>.
    /// </summary>
    private Task<TResult> SubmitOwnedHandleOperation<TResult>(
        CancellationToken cancellationToken,
        NativeOwnedHandleSubmit submit)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        OperationCompletionSource<TResult> context = new OperationCompletionSource<TResult>();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count, INSIDE the try (M11/P8, Major 4) — see
            // SubmitVoidOperation for the full rationale (an AddRef throw must route through
            // AbandonBeforeSubmit or the GCHandle is rooted for the process lifetime).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            context.RegisterCancellation(cancellationToken, context.CancelAwaiter);
            submit(_handle.DangerousGetHandle(), GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback will never fire → we own cleanup.
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Graceful <b>async</b> close (Java <c>Producer.close()</c>) that <b>surfaces</b> the close
    /// error — the public <c>Close(CancellationToken)</c>'s worker. Observes an already-canceled
    /// <paramref name="cancellationToken"/> up front, takes the one-shot <see cref="TryBeginClose"/>
    /// latch (shared with <see cref="Dispose"/> / <see cref="DisposeAsync"/> — idempotent), closes
    /// via <see cref="CloseWithCallbackInternal"/> (<c>Producer_close_async</c>), then releases the
    /// handle (→ <c>Producer_destroy</c>) in a <c>finally</c> — destroy runs exactly once even on a
    /// close error. A subsequent teardown loses the latch and no-ops. Mirrors
    /// <c>NativeConsumer.CloseWithCallback</c>.
    /// </summary>
    /// <remarks>
    /// <b>No timeout (ABI-verified).</b> There is no <c>Producer_close_async_with_timeout</c> ABI —
    /// <c>Producer_close_async</c> takes only a callback; the M11/P2 <c>Close(TimeSpan)</c> overload
    /// (a .NET-side timer race, not a native timed close) was removed in M11/P2.1 for Python-producer
    /// parity. So this takes only a <see cref="CancellationToken"/>. Unlike <see cref="DisposeAsync"/>
    /// (which swallows the close error, best-effort), this <b>throws</b> it — <c>close()</c> reports
    /// failures.
    /// </remarks>
    /// <param name="cancellationToken">
    /// Observed only before the close is submitted (a canceled token throws
    /// <see cref="OperationCanceledException"/> before taking the latch); once close is in flight
    /// the graceful join runs to completion.
    /// </param>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal async Task CloseWithCallback(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!TryBeginClose())
        {
            // A prior teardown already won the latch — closing again would double-close /
            // double-destroy. No-op, matching the idempotent teardown contract.
            return;
        }

        // Async flush + stop/join the send pump before close/destroy (producer-outlives-pump,
        // ffi §A2/§A7). Async flush avoids sync-over-async on this async path (ffi §A7); the join
        // stays blocking by design.
        await StopPumpAsync().ConfigureAwait(false);

        try
        {
            // Once the close is in flight the graceful join runs to completion (no cancellation
            // token wired into the op). Surface any close error (unlike disposal).
            await CloseWithCallbackInternal().ConfigureAwait(false);
        }
        finally
        {
            // ReleaseHandle → Producer_destroy, exactly once — even if the close threw.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Graceful <b>synchronous</b> close (Java <c>close()</c>) that <b>surfaces</b> the close error —
    /// the sync producer's <see cref="Confluent.Kafka.KafkaProducer{TKey, TValue}.Close"/> worker (M11/P4). Takes
    /// the one-shot <see cref="TryBeginClose"/> latch (shared with <see cref="Dispose"/> /
    /// <see cref="DisposeAsync"/> / <see cref="CloseWithCallback"/> — idempotent), stops the send pump
    /// (a no-op for a sync-only producer — see the remarks), closes via the sync <c>Producer_close</c>,
    /// then releases the handle (→ <c>Producer_destroy</c>) in a <c>finally</c> so destroy runs exactly
    /// once even on a close error. A subsequent teardown loses the latch and no-ops. Mirrors
    /// <c>NativeConsumer.CloseSync</c>.
    /// </summary>
    /// <remarks>
    /// <b>Pump-less teardown (decision #6, corrected in M11/P8).</b> A producer used only through the
    /// sync surface never starts the send pump (only the async <see cref="SendViaPump"/> does), so
    /// <see cref="StopPump"/> finds <c>_pump == null</c> and skips the <em>join</em> — there is no
    /// pump thread to join. It does <b>not</b> skip the <em>flush</em>: that was the Blocker-2 bug
    /// (decision #6's original wording justified skipping the join, but the code silently skipped the
    /// flush too, which is a different question). The flush now always runs, so a concurrently blocked
    /// <see cref="Send"/> is released on the sync teardown path exactly as it is on the async one.
    /// Unlike <see cref="Dispose"/> (which swallows the close error, best-effort), this <b>throws</b>
    /// it — <c>close()</c> reports failures.
    /// </remarks>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    internal void Close()
    {
        if (!TryBeginClose())
        {
            // A prior teardown already won the latch — no-op (idempotent).
            return;
        }

        // Gate + flush + join the send pump BEFORE close/destroy (producer-outlives-pump ordering).
        // For a sync-only producer no send ever started the pump, so only the JOIN is skipped — the
        // flush still runs and releases a concurrently blocked Send (decision #6, M11/P8 Blocker 2).
        StopPump();

        try
        {
            NativeMethods.ProducerClose(_handle.DangerousGetHandle(), out IntPtr error);
            KafkaException? failure = KafkaException.FromHandle(error);
            if (failure is not null)
            {
                throw failure;
            }
        }
        finally
        {
            // ReleaseHandle → Producer_destroy, exactly once — even if the close threw.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Graceful <b>synchronous</b> teardown (the blocking fallback; Java <c>close()</c> flavor that
    /// <b>swallows</b> the close error): take the one-shot <see cref="TryBeginClose"/> latch, close
    /// synchronously (the plain sync <c>Producer_close</c>), then release the handle
    /// (→ <c>Producer_destroy</c>) in a <c>finally</c>. Idempotent and safe under concurrent /
    /// double calls (the atomic <see cref="_closed"/> latch). The async teardown
    /// (<see cref="DisposeAsync"/>) is the primary path.
    /// </summary>
    /// <remarks>
    /// <b>Graceful close-before-destroy (M11/P2.1).</b> <c>Producer_destroy</c> alone blocks + joins
    /// the background Sender task but skips the graceful <c>Producer_close</c>; this closes first
    /// then destroys, mirroring <c>NativeConsumer.Dispose</c>. Dispose consumes the close error
    /// (freeing the handle via <see cref="KafkaException.FromHandle(IntPtr)"/>) but does NOT
    /// rethrow — Dispose must not throw, and a best-effort
    /// teardown has no caller to hand a failure to (that is <see cref="CloseWithCallback"/>'s job).
    /// </remarks>
    public void Dispose()
    {
        // Idempotent: the first caller wins the latch; later / concurrent calls no-op.
        if (!TryBeginClose())
        {
            return;
        }

        // Stop + join the send pump BEFORE close/destroy so no future handle is in use when the
        // producer is destroyed (producer-outlives-pump ordering, ffi §A2/§A7).
        StopPump();

        try
        {
            // Graceful sync close first (best-effort, swallow): Producer_destroy alone joins the
            // Sender but skips the graceful Producer_close. The handle is not released until the
            // finally below, so its raw value is valid here (single-owner: no concurrent destroy).
            // No Producer_close_with_timeout ABI (unlike the consumer) → the plain Producer_close.
            // Read+free the error via FromHandle, then swallow — Dispose must not throw, and a
            // best-effort teardown has no caller to hand a failure to. Mirrors NativeConsumer.Dispose.
            NativeMethods.ProducerClose(_handle.DangerousGetHandle(), out IntPtr error);
            _ = KafkaException.FromHandle(error);
        }
        finally
        {
            // ReleaseHandle → Producer_destroy, exactly once.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Graceful <b>async</b> teardown (the primary path; Java <c>close()</c> flavor that
    /// <b>swallows</b> the close error): take the one-shot latch, close via
    /// <see cref="CloseWithCallbackInternal"/> (<c>Producer_close_async</c>, resolved through the completion
    /// bridge), then release the handle (→ <c>Producer_destroy</c>). Idempotent and safe under
    /// concurrent / double calls (the atomic <see cref="_closed"/> latch). Surfacing the close error
    /// is <see cref="CloseWithCallback"/>'s job.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!TryBeginClose())
        {
            return;
        }

        // Async flush + stop/join the send pump before close/destroy (producer-outlives-pump,
        // ffi §A2/§A7). Async flush avoids sync-over-async on this async path (ffi §A7); the join
        // stays blocking by design.
        await StopPumpAsync().ConfigureAwait(false);

        try
        {
            await CloseWithCallbackInternal().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best-effort teardown — DisposeAsync must not surface a close error; that is
            // Close()'s job. A close failure still proceeds to destroy in the finally.
            //
            // Minor 12(b): broadened from catch (KafkaException). CloseWithCallbackInternal can
            // also throw a NON-KafkaException — ObjectDisposedException (DangerousAddRef /
            // DangerousGetHandle) or OutOfMemoryException (GCHandle.Alloc) — and letting that
            // escape would make `await using` throw from a disposal path, contradicting the
            // contract stated one line above. The finally already guarantees the destroy runs, so
            // broadening costs nothing and makes the stated contract true (same rationale as
            // StopPumpAsync's swallow).
        }
        finally
        {
            // ReleaseHandle → Producer_destroy, exactly once.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Bridges <c>Producer_close_async</c> to a <see cref="Task"/> via the shared completion
    /// callback — the async building block behind <see cref="CloseWithCallback"/> / <see cref="DisposeAsync"/>.
    /// Roots the per-op context via a <see cref="GCHandle"/> and takes the span-the-op
    /// <see cref="SafeProducerHandle"/> ref (so the subsequent <c>Producer_destroy</c> is deferred
    /// past the close's completion callback — destroy-vs-in-flight-close use-after-free safety,
    /// ffi §A2/§A7). Takes no latch and does not destroy — the calling teardown flavor owns both.
    /// If the submitting P/Invoke throws before native could fire the callback, the context is
    /// abandoned (its <c>GCHandle</c> freed) here. Mirrors <c>NativeConsumer.CloseWithCallbackInternal</c>.
    /// </summary>
    private Task CloseWithCallbackInternal()
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count, INSIDE the try (M11/P8, Major 4) — see
            // SubmitVoidOperation for the full rationale (an AddRef throw must route through
            // AbandonBeforeSubmit or the GCHandle is rooted for the process lifetime).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            NativeMethods.ProducerCloseAsync(
                _handle.DangerousGetHandle(), ProducerCallbacks.Operation, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Bridges <c>Producer_flush_async</c> to a <see cref="Task"/> via the shared void completion
    /// callback — the async teardown flush behind <see cref="StopPumpAsync"/> (and the same bridge
    /// the public <see cref="FlushWithCallback"/> uses, minus its latch / cancellation wiring). It
    /// resolves pending sends so the completion pump's blocking <c>get_all</c> returns (the Issue-1
    /// no-hang property on the async path). Roots the per-op context via a <see cref="GCHandle"/> and
    /// takes the span-the-op <see cref="SafeProducerHandle"/> ref (so <c>Producer_destroy</c> is
    /// deferred past the flush's completion callback, ffi §A2/§A7). Takes no latch and does not
    /// destroy — the calling teardown flavor owns both. Mirrors <see cref="CloseWithCallbackInternal"/>,
    /// swapping <c>Producer_close_async</c> for <c>Producer_flush_async</c>.
    /// </summary>
    private Task FlushInternal()
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count, INSIDE the try (M11/P8, Major 4) — see
            // SubmitVoidOperation for the full rationale (an AddRef throw must route through
            // AbandonBeforeSubmit or the GCHandle is rooted for the process lifetime).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            NativeMethods.ProducerFlushAsync(
                _handle.DangerousGetHandle(), ProducerCallbacks.Operation, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Transitions from open to closing exactly once. Returns <see langword="true"/>
    /// for the first caller (which performs teardown), <see langword="false"/> for
    /// any concurrent or subsequent caller (a no-op).
    /// </summary>
    private bool TryBeginClose() => Interlocked.CompareExchange(ref _closed, 1, 0) == 0;

    /// <summary>
    /// Throws <see cref="ObjectDisposedException"/> once the close latch is won — the
    /// use-after-teardown guard every op runs first.
    /// </summary>
    /// <remarks>
    /// <b><c>internal</c>, not <c>private</c> (M11/P8, Minor 8).</b> The four public <c>Send</c>
    /// skins call it BEFORE serializing, so a stateful serializer (a Schema-Registry serializer
    /// <em>registers a schema</em> as a side effect) does not run for a record that can never be
    /// sent, and a serializer throw cannot mask the real <see cref="ObjectDisposedException"/>.
    /// Java checks in that order too — <c>KafkaProducer.throwIfProducerClosed()</c> precedes the
    /// key/value serialize. Same assembly, so this is not a public-surface change. The inner call
    /// on the native path STAYS: it is the race re-check, not a duplicate.
    /// </remarks>
    internal void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new ObjectDisposedException(nameof(NativeProducer));
        }
    }
}
