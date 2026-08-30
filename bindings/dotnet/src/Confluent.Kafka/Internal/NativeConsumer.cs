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
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Internal lifecycle wrapper over an owned <c>kafka_consumer_Consumer_t</c>: it
/// orchestrates config marshalling → construction → async ops → graceful close →
/// destroy, and owns the <see cref="SafeConsumerHandle"/> (PLAN D4). It lives under
/// <c>Internal/</c> — not <c>Internal/Interop/</c> — because it uses only the safe
/// managed <see cref="Utf8Marshal.Pin(string)"/>, <see cref="GCHandle"/>, and
/// <see cref="System.Runtime.InteropServices.SafeHandle"/> APIs, so it needs no
/// <c>unsafe</c> (that stays quarantined to <c>Internal/Interop/</c>, CLAUDE.md §2).
/// </summary>
/// <remarks>
/// <para>
/// This is <b>not</b> the public client. The public <c>IAsyncConsumer</c> /
/// <c>AsyncKafkaConsumer</c> / <c>AsyncMockConsumer</c> types compose this wrapper; it
/// is the internal proving ground for the create → async op → close → destroy lifecycle,
/// the completion bridge (<see cref="OperationCompletionSource"/>), and the
/// operational / precondition / wakeup / concurrent error surfaces.
/// </para>
/// <para>
/// <b>Single-owner, not thread-safe (M3/P2).</b> A Kafka consumer is single-owner —
/// at most one operation in flight — and this wrapper mirrors the in-repo Python
/// sibling's contract exactly (<c>bindings/python/consumer.py</c>: "The Rust
/// consumer is single-owner (one operation in flight). Concurrent use surfaces as a
/// <c>KafkaError</c> (ConcurrentModification) or, for the non-blocking state reads, a
/// <c>RuntimeError</c>."). The <b>Rust core's own access guard</b> is the serializer;
/// there is <b>no managed mirror</b> (M3/P1's <c>ConsumerAccessGuard</c> +
/// in-flight tracking were removed here as .NET-only additions on top of that
/// model — a localized, reversible simplification). Concurrency therefore surfaces
/// the core's way: a concurrent <b>async op</b> (<see cref="SubscribeWithCallback(IReadOnlyCollection{string}, CancellationToken)"/> /
/// <see cref="PollWithCallback"/>) is rejected by the core inline and surfaces as a
/// <b>faulted <see cref="Task"/></b> carrying a <see cref="KafkaException"/>
/// (ConcurrentModification); a concurrent <b>sync state read</b>
/// (<see cref="GroupId"/>) surfaces as <see cref="InvalidOperationException"/> from
/// the core's null-handle rejection path (ffi §B5). <see cref="Wakeup"/> is the one
/// cross-thread call by design.
/// </para>
/// <para>
/// The 5 kept invariants (the managed-side safety that does not depend on a managed
/// guard, ffi §B6/§B7): (1) a per-op self-rooting <see cref="GCHandle"/>; (2) the
/// completion callback is the <b>sole owner</b> of the <see cref="GCHandle"/> free
/// (<see cref="OperationCompletionSource"/>); (3) the atomic <see cref="_closed"/>
/// teardown gate; (4) <see cref="SafeConsumerHandle"/> + <c>TaskCompletionSource</c>
/// thread-safety; (5) <c>RunContinuationsAsynchronously</c> on the completion
/// (§B7) — the callback fires on the core's foreign dispatcher thread.
/// </para>
/// <para>
/// <b>Teardown (single-owner).</b> Both paths are gated by the atomic
/// <see cref="_closed"/> flag. <see cref="DisposeAsync"/> is <b>primary</b>
/// (<c>close_async → destroy</c>): it closes gracefully — <c>close_async</c> joins
/// the background task via the completion bridge — before destroy.
/// <see cref="Dispose"/> is the blocking fallback (<c>close_with_timeout →
/// destroy</c>). Under the single-owner model the awaiter of an op <b>is</b> its
/// disposer, so neither path drains a <em>separately-submitted</em> op — there is no
/// concurrent submitter to drain. This matches the Python sibling (<c>close()</c>
/// drains its <em>own</em> awaited op, then bare <c>_destroy</c>).
/// </para>
/// <para>
/// <b>Accepted residuals — the list as of M9/P4.</b> This is what a reader consults to know
/// what is <em>knowingly</em> unsafe, so it is kept honest in both directions: two of the
/// three entries it used to carry are now <b>closed</b>, and the one that remains says
/// something different from what it used to say. <see cref="DisposeAsync"/> on the awaiting
/// task remains the clean path.
/// </para>
/// <list type="bullet">
/// <item>
/// <b>#1 — Teardown with an unawaited in-flight op: REWRITTEN, and it no longer says what it
/// used to.</b> The old text claimed the <see cref="Task"/> strands and the per-op
/// <see cref="GCHandle"/> leaks once. Both are <b>wrong</b> post-M9/P3 <c>073252f3</c>: the op
/// holds a span-the-op reference on the <see cref="SafeConsumerHandle"/>, so it runs to
/// completion and its callback fires and frees the handle. What is actually accepted is
/// different and, in M9/P4, wider: <see cref="Dispose"/> stopped being a <b>deterministic</b>
/// native release. If an operation is in flight, teardown returns having destroyed nothing;
/// the release happens when that operation completes, bounded by its own (caller-supplied)
/// timeout, and the destroy may then run on the core's own dispatcher thread. Accepted per
/// decision <b>Q1</b>. The eventual deferred destroy is also <b>bare</b> — no preceding
/// graceful close, because the core's one-op guard rejected it — accepted <b>permanently</b>
/// per decision <b>Q3</b>, with <b>no follow-up item filed, scheduled or tracked</b>. The full
/// five-point argument, the three safe-by-construction citations for the dispatcher-thread
/// destroy, and the explicit statement that the core-side clean fix is <b>not pursued and not
/// tracked</b> all live on <see cref="Dispose"/>. H1 <em>widens the reach</em> of this residual
/// to the synchronous surface (a sync call now also holds a reference for its duration); it
/// does not create it. The M3/P1 managed fault machinery (<c>FaultTaskOnly</c>) is still
/// deliberately not re-added.
/// </item>
/// <item>
/// <b>#2 — <see cref="Wakeup"/> / <see cref="GroupId"/> handle TOCTOU: CLOSED (M9/P4 H1).</b>
/// It was also badly understated — it applied to ~31 synchronous call sites, several of which
/// block for a <em>caller-supplied</em> timeout, not to two members. Every synchronous consumer
/// P/Invoke now declares its handle parameter as the <see cref="SafeConsumerHandle"/>, so the
/// marshaller holds a reference for the whole native call (ffi §A2); <see cref="Wakeup"/>
/// additionally swallows the marshaller's <see cref="ObjectDisposedException"/> to preserve its
/// documented no-op contract. What remains is <b>not</b> a residual: the close family
/// (<c>Consumer_close</c> / <c>_close_with_timeout</c>) deliberately keeps a raw pointer, and is
/// safe by the one-shot <see cref="TryBeginClose"/> latch — the winner closes and then releases
/// the handle on the same thread in program order, and <c>Consumer_destroy</c> is reachable only
/// from that release, so no concurrent destroy can race those three sites. Each is commented in
/// place; do not "finish the job" there (it would change close-before-destroy ordering).
/// </item>
/// <item>
/// <b>#3 — Submit-vs-<c>destroy</c> handle race: CLOSED, and its text was already obsolete when
/// written.</b> All five async op-submit sites — the four <c>Submit*</c> helpers
/// (<see cref="SubmitVoidOperation"/>, <c>SubmitTypedPollOperation</c>,
/// <c>SubmitScalarOperation</c>, <c>SubmitOwnedHandleOperation</c>) plus
/// <see cref="CloseWithCallbackInternal"/> — take an explicit span-the-op
/// <c>DangerousAddRef</c>, released in <c>FreeGcHandle</c>, so the destroy is deferred past the
/// op. The one gap that survived — a <c>DangerousAddRef</c> throw leaking the just-allocated
/// <see cref="GCHandle"/> because the <c>AddRef</c> sat outside the <c>try</c> — is fixed in
/// M9/P4 M3: the <c>AddRef</c> now sits inside, so every "native never ran" path routes through
/// <c>AbandonBeforeSubmit</c>.
/// </item>
/// </list>
/// <para>
/// The M3/P1 op-submit-vs-teardown <em>publish window</em> (a strand+leak race that
/// required in-flight tracking fields) is <b>eliminated</b> by M3/P2: with no
/// tracking fields there is nothing to publish and no window.
/// </para>
/// </remarks>
internal sealed class NativeConsumer : IDisposable, IAsyncDisposable
{
    // Fixed graceful-close budget for the synchronous Dispose. A never-joined
    // consumer closes near-instantly; a user-supplied timeout arrives with the
    // future public Close(TimeSpan) overload.
    private const long DefaultCloseTimeoutMilliseconds = 5_000;

    private readonly SafeConsumerHandle _handle;

    // Thread-safe closed flag (invariant #3, the teardown gate — NOT the removed
    // managed access guard): 0 = open, 1 = closing/closed. Makes double / concurrent
    // Dispose safe and gates use-after-dispose. A plain bool would be a torn-read
    // race .NET has and Python's GIL hides, so this stays atomic.
    private int _closed;

    // The most recent rebalance-listener registration created by this consumer — the
    // managed mirror of Python's `self._listener_adapter` (consumer.py:578-582), kept for
    // parity and observability.
    //
    // ⚠ It is deliberately NOT the keep-alive, and NOT a free site. The registration's own
    // GCHandle roots it, and the single sanctioned free is the core's user_data_destroy hook
    // (ListenerRegistration). Nothing here clears or replaces the field on a later subscribe:
    // whether a given subscribe actually replaced the previous registration is a rule the
    // header explicitly warns cannot be inferred from a return code
    // (confluent_kafka.h:2105-2130 — a subscribe rejected AFTER registration keeps the
    // registration, and an empty-topic-list subscribe releases the listener while returning
    // SUCCESS). ListenerRegistration.IsReleased, set by the hook, is the authoritative signal.
    private ListenerRegistration? _listenerRegistration;

    private NativeConsumer(SafeConsumerHandle handle)
    {
        _handle = handle;
    }

    /// <summary>The submit shape shared by every void-result async op.</summary>
    private delegate void NativeSubmit(
        IntPtr consumer,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <c>_async</c> ABI shape shared by all five void ops on a partition collection
    /// (<c>assign</c> / <c>pause</c> / <c>resume</c> / <c>seekToBeginning</c> /
    /// <c>seekToEnd</c>, M5/P3) — the parallel <c>(topics[], partitions[], count)</c> arrays
    /// plus the shared void-result <c>op_callback_t</c>. A method group reference to each
    /// <c>NativeMethods.Consumer&lt;Op&gt;Async</c> binds to this, so
    /// <see cref="SubmitPartitionOp"/> marshals once and dispatches to any of the five.
    /// </summary>
    private delegate void NativePartitionOpSubmit(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.OperationCallback callback,
        IntPtr userData);

    /// <summary>
    /// The submit shape shared by every owned-handle (result-returning) async op —
    /// the poll analog of <see cref="NativeSubmit"/>. Takes the poll completion
    /// callback (ffi §B6/§B7); the five future owned-handle ops reuse this shape.
    /// </summary>
    private delegate void NativeResultSubmit(
        IntPtr consumer,
        ConsumerCallbacks.PollCallback callback,
        IntPtr userData);

    /// <summary>
    /// The submit shape shared by every <b>scalar</b> (result-in-callback) async op —
    /// the <c>position</c> analog of <see cref="NativeResultSubmit"/> (M5/P2). Takes the
    /// scalar completion callback (ffi §B6/§B7), whose unmanaged signature
    /// (<c>(int64_t, error*, ud)</c>) differs from the poll callback's
    /// (<c>(records*, error*, ud)</c>), so it needs its own delegate type rather than
    /// reusing <see cref="NativeResultSubmit"/>.
    /// </summary>
    private delegate void NativeScalarSubmit(
        IntPtr consumer,
        ConsumerCallbacks.PositionCallback callback,
        IntPtr userData);

    /// <summary>
    /// The submit shape shared by the M5/P4 owned-handle offset-map ops
    /// (<c>committed</c> / <c>offsetsForTimes</c> / <c>beginning|endOffsets</c>). Unlike
    /// <see cref="NativeResultSubmit"/> (which takes the poll callback as a parameter),
    /// this passes only <c>(consumer, userData)</c>: each op closes over its own
    /// strongly-typed rooted callback (<see cref="ConsumerCallbacks.OffsetMapCallback"/>
    /// / <see cref="ConsumerCallbacks.OffsetAndTimestampMapCallback"/> /
    /// <see cref="ConsumerCallbacks.LongOffsetMapCallback"/>) at the call site, so the
    /// four <c>_async</c> DllImports keep their distinct, self-documenting delegate
    /// parameter types. Added as a parallel helper (rather than reshaping the proven poll
    /// <see cref="NativeResultSubmit"/> / <see cref="SubmitTypedPollOperation{TKey, TValue}"/>) so the
    /// shipped poll / void / scalar submit paths are left byte-for-byte untouched
    /// (PLAN §4.2 — the "clone a parallel submit helper" option).
    /// </summary>
    private delegate void NativeOwnedHandleSubmit(IntPtr consumer, IntPtr userData);

    /// <summary>
    /// The <c>_async</c> ABI shape shared by <c>beginning_offsets_async</c> and
    /// <c>end_offsets_async</c> (both take the parallel <c>(topics[], partitions[],
    /// count)</c> arrays plus the shared <c>long_offsets_callback_t</c> and return a
    /// <c>LongOffsetMap_t</c>). A method-group reference to each
    /// <c>NativeMethods.Consumer{Beginning,End}OffsetsAsync</c> binds to this, so
    /// <see cref="SubmitLongOffsetsOp"/> marshals once and dispatches to either.
    /// </summary>
    private delegate void NativeLongOffsetsSubmit(
        IntPtr consumer,
        IntPtr[] topics,
        int[] partitions,
        int count,
        ConsumerCallbacks.LongOffsetMapCallback callback,
        IntPtr userData);

    /// <summary>
    /// The <b>sync</b> ABI shape shared by the five void partition-collection ops
    /// (<c>assign</c> / <c>pause</c> / <c>resume</c> / <c>seek_to_beginning</c> /
    /// <c>seek_to_end</c>, M5/P8a) — the parallel <c>(topics[], partitions[], count)</c> arrays
    /// returning a <c>KafkaError*</c> handle (null = success). The synchronous analog of
    /// <see cref="NativePartitionOpSubmit"/>: a method-group reference to each
    /// <c>NativeMethods.Consumer&lt;Op&gt;</c> binds to this, so <see cref="RunPartitionOpSync"/>
    /// marshals once and dispatches to any of the five (the async
    /// <see cref="SubmitPartitionOp"/> precedent, without the callback / <c>GCHandle</c>).
    /// The first parameter is the <see cref="SafeConsumerHandle"/>, not a raw
    /// <see cref="IntPtr"/> (M9/P4 H1b): the sync convention is a call-scoped marshaller
    /// AddRef (ffi §A2), so the delegate type must carry it through or the five
    /// <c>NativeMethods.Consumer&lt;Op&gt;</c> method groups would no longer bind. Do NOT
    /// mirror this on the <em>async</em> <see cref="NativePartitionOpSubmit"/> — that one
    /// needs a span-the-op reference, which a call-scoped AddRef cannot express.
    /// </summary>
    private delegate IntPtr NativePartitionOpSync(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count);

    /// <summary>
    /// The <b>sync</b> ABI shape shared by the three collection-input query ops
    /// (<c>committed</c> / <c>beginning_offsets</c> / <c>end_offsets</c>, M5/P8b) — the parallel
    /// <c>(topics[], partitions[], count)</c> arrays plus an <b>out-param owned-container
    /// handle</b>, returning a <c>KafkaError*</c> (null = success). The query analog of
    /// <see cref="NativePartitionOpSync"/> (which has no result handle): a method-group reference
    /// to each <c>NativeMethods.Consumer{Committed,BeginningOffsets,EndOffsets}</c> binds to
    /// this, so <see cref="RunContainerQuerySync{TResult}"/> marshals + copies-out once and
    /// dispatches to any of the three. The first parameter is the
    /// <see cref="SafeConsumerHandle"/> (M9/P4 H1b, ffi §A2 sync convention); the trailing
    /// <c>out IntPtr</c> is why this must stay a genuine <c>delegate</c> rather than a
    /// <c>Func&lt;&gt;</c>, and it survives the retype unchanged — the owned-container
    /// out-param is still pre-initialized to <see cref="IntPtr.Zero"/> by the caller so the
    /// null-safe <c>_destroy</c> is a no-op on the failure path (invariant I4).
    /// </summary>
    private delegate IntPtr NativeCollectionQuerySync(
        SafeConsumerHandle consumer, IntPtr[] topics, int[] partitions, int count, out IntPtr outHandle);

    /// <summary>
    /// The owned consumer handle. Throws <see cref="ObjectDisposedException"/> once
    /// closed (the use-after-dispose guard). Exposed for the interop tests, which
    /// drive the raw ABI against it; the public client will not expose the handle.
    /// </summary>
    internal SafeConsumerHandle Handle
    {
        get
        {
            ThrowIfClosed();
            return _handle;
        }
    }

    /// <summary>
    /// The most recent rebalance-listener registration created by this consumer, or
    /// <see langword="null"/> if none. Exposed so the tests can pin the Java-faithful
    /// release rules through <see cref="ListenerRegistration.IsReleased"/> — a replacing
    /// subscribe releases the registration, an <c>Unsubscribe</c> does not.
    /// </summary>
    internal ListenerRegistration? CurrentListenerRegistration => _listenerRegistration;

    /// <summary>
    /// Creates a real (KIP-848) consumer from a config map: each entry becomes a
    /// <c>ConsumerProperties_put</c> (keys are the Java dotted names, CLAUDE.md §4),
    /// then <c>KafkaConsumer_new</c> consumes the properties. A construction failure
    /// surfaces as a <see cref="KafkaException"/>.
    /// </summary>
    /// <param name="config">Config keyed by Java dotted names; values are strings.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    internal static NativeConsumer Create(IReadOnlyDictionary<string, string> config)
    {
        // Preconditions BEFORE any pin/marshal/P-Invoke (ffi §A5/§B5): the ABI does
        // not validate them and panics on violation (UB across FFI).
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

        SafeConsumerHandle handle;
        IntPtr error;

        SafeConsumerPropertiesHandle props = SafeConsumerPropertiesHandle.Create();
        try
        {
            foreach (KeyValuePair<string, string> entry in config)
            {
                using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                NativeMethods.ConsumerPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
            }

            // D6: props is passed as the SafeHandle, so the marshaller keeps it
            // alive across the call; the ABI does not consume it (we free it below).
            // The consumer handle arrives ALREADY WRAPPED — the marshaller invokes
            // SafeConsumerHandle's private ctor and sets the pointer atomically
            // (M2/P2), closing the create→SetHandle allocation-gap window.
            handle = NativeMethods.KafkaConsumerNew(props, out error);
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
            // SafeConsumerHandle; dispose it (ReleaseHandle is skipped for an
            // IsInvalid handle, so NO spurious Consumer_destroy) before throwing.
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
                "kafka_consumer_KafkaConsumer_new returned a null handle without an error.");
        }

        return new NativeConsumer(handle);
    }

    /// <summary>
    /// Creates a broker-less mock consumer.
    /// </summary>
    /// <param name="autoOffsetReset">
    /// The reset-strategy name (<c>"earliest"</c> / <c>"latest"</c> / <c>"none"</c>
    /// / <c>"by_duration:&lt;ISO-8601&gt;"</c>), or <see langword="null"/> for the
    /// default (<c>"latest"</c>).
    /// </param>
    internal static NativeConsumer CreateMock(string? autoOffsetReset = null)
    {
        // Non-fallible: MockConsumer_new always returns a valid owned handle,
        // already wrapped by the marshaller (M2/P2). No out_error, no IsInvalid guard.
        SafeConsumerHandle handle;
        if (autoOffsetReset is null)
        {
            handle = NativeMethods.MockConsumerNew(IntPtr.Zero);
        }
        else
        {
            using Utf8Marshal.PinnedUtf8String strategy = Utf8Marshal.Pin(autoOffsetReset);
            handle = NativeMethods.MockConsumerNew(strategy.Pointer);
        }

        return new NativeConsumer(handle);
    }

    /// <summary>
    /// Subscribes to <paramref name="topics"/> (async). The returned
    /// <see cref="Task"/> completes when the core resolves the op — successfully for
    /// a <c>MockConsumer</c>, or faulted with a <see cref="KafkaException"/>. A
    /// concurrent second op is rejected by the core inline and faults the
    /// <see cref="Task"/> with a <see cref="KafkaException"/> (ConcurrentModification,
    /// ffi §B5) — the single-owner contract, delivered by the core.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task SubscribeWithCallback(
        IReadOnlyCollection<string> topics,
        CancellationToken cancellationToken = default)
    {
        // Snapshot + validate BEFORE native (ffi §B5 preconditions).
        string[] topicArray = SnapshotTopics(topics);

        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
            // Call-scoped pins: subscribe_async reads the topic strings synchronously into an
            // owned Vec<String> before spawning, so the buffers are freed once the native call
            // returns (ffi §A4 call-scoped pin). WithPinnedTopicsOnly is shared verbatim with the
            // sync Subscribe (M5/P8a) — one topics-only pin path, no duplication (DoD §6).
            WithPinnedTopicsOnly(topicArray.Length, i => topicArray[i], (pointers, cnt) =>
                NativeMethods.ConsumerSubscribeAsync(consumer, pointers, cnt, callback, userData)));
    }

    /// <summary>
    /// Subscribes to <paramref name="topics"/> with a rebalance listener (async; Java
    /// <c>subscribe(Collection, ConsumerRebalanceListener)</c>) — the listener-taking sibling
    /// of <see cref="SubscribeWithCallback(IReadOnlyCollection{string}, CancellationToken)"/>,
    /// over <c>Consumer_subscribe_with_listener_async</c> and the same void completion bridge.
    /// </summary>
    /// <remarks>
    /// <b>The listener handle is consumed unconditionally</b>, success or failure
    /// (<c>confluent_kafka.h:2104-2113</c>) — it is never destroyed after the submit. The one
    /// exception is a submit that <em>threw</em>, where native never ran, so the handle was
    /// never consumed and must be destroyed (which fires the release hook and frees the
    /// registration).
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> or <paramref name="listener"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task SubscribeWithCallback(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        if (listener is null)
        {
            throw new ArgumentNullException(nameof(listener));
        }

        string[] topicArray = SnapshotTopics(topics);

        ListenerRegistration registration = ListenerRegistration.Root(listener);
        bool consumed = false;
        Task task;
        try
        {
            task = SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
                WithPinnedTopicsOnly(topicArray.Length, i => topicArray[i], (pointers, cnt) =>
                {
                    IntPtr listenerHandle = NewListenerHandle(registration);
                    try
                    {
                        NativeMethods.ConsumerSubscribeWithListenerAsync(
                            consumer, pointers, cnt, listenerHandle, callback, userData);
                    }
                    catch
                    {
                        // Native never ran → the listener was NOT consumed → destroying it is
                        // correct (and fires the release hook). Doing this after a successful
                        // submit would be a double free.
                        NativeMethods.ConsumerRebalanceListenerDestroy(listenerHandle);
                        throw;
                    }

                    consumed = true;
                }));
        }
        catch
        {
            // Only when the listener handle never reached native — otherwise the core owns
            // the registration and the hook is the sole releaser. Release() is idempotent, so
            // the "destroyed above, then rethrown" path is safe too.
            if (!consumed)
            {
                registration.Release();
            }

            throw;
        }

        _listenerRegistration = registration;
        return task;
    }

    /// <summary>
    /// Unsubscribes from all topics / partitions (async). The returned
    /// <see cref="Task"/> completes when the core resolves the op (successfully for a
    /// <c>MockConsumer</c>), or faults with a <see cref="KafkaException"/>. Reuses the
    /// void completion bridge over <c>Consumer_unsubscribe_async</c> — the one new wire
    /// this phase (§B7).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task UnsubscribeWithCallback(CancellationToken cancellationToken = default)
    {
        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
            NativeMethods.ConsumerUnsubscribeAsync(consumer, callback, userData));
    }

    /// <summary>
    /// Seeks <c>(topic, partition)</c> to <paramref name="offset"/> (<b>sync</b>; M5/P7) —
    /// the .NET realization of Java <c>seek(TopicPartition, long)</c>, calling the sync ABI
    /// <c>Consumer_seek</c> directly (not the async bridge; PLAN §1/§2). Structurally
    /// identical to the shipped <see cref="EnforceRebalance"/> / <see cref="CommitAsync"/>
    /// sync-op discipline: preconditions BEFORE any pin / P-Invoke, then
    /// <see cref="ThrowIfClosed"/>, a call-scoped topic pin, the P/Invoke, then
    /// <see cref="KafkaException.FromHandle(IntPtr)"/> throw-iff-non-null. No
    /// <see cref="GCHandle"/>, no completion bridge, no <see cref="CancellationToken"/>. On a
    /// <c>MockConsumer</c>, seeking an <b>unassigned</b> partition is a genuine broker-free
    /// failure → a synchronous <see cref="KafkaException"/> (replacing the old faulted-Task).
    /// </summary>
    /// <remarks>
    /// <b>Deliberate divergence from Python (Q1 = KEEP).</b> The negative-offset guard is the
    /// ONE place .NET is deliberately stricter than Python (whose sync <c>seek</c> does no
    /// offset validation): Java's <c>AsyncKafkaConsumer.seek</c> throws
    /// <c>IllegalArgumentException("seek offset must not be a negative number")</c> before the
    /// event round-trip, so we throw <see cref="ArgumentOutOfRangeException"/> with that exact
    /// message BEFORE the P/Invoke — even when the consumer is closed (the argument check
    /// precedes <see cref="ThrowIfClosed"/>). The exact message is asserted by the tests
    /// (DoD §3). Blocking: the sync ABI parks the caller inside the core's <c>block_on</c>
    /// (deadlock-free — multi-thread runtime, ffi §B1); it is NOT a <c>Task.Run</c>
    /// sync-over-async wrapper.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partition"/> is negative, or <paramref name="offset"/> is negative
    /// (Java: <c>"seek offset must not be a negative number"</c>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure (e.g. an unassigned partition).</exception>
    internal void Seek(string topic, int partition, long offset)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "Partition must not be negative.");
        }

        // Q1 = KEEP the Java-fidelity negative-offset guard. Java AsyncKafkaConsumer.seek
        // throws IllegalArgumentException("seek offset must not be a negative number") BEFORE
        // the call; per the CLAUDE.md idiom map (IllegalArgumentException →
        // ArgumentOutOfRangeException, validated before the FFI call) and the locked PLAN
        // decision Q1. This is the ONE place .NET is deliberately stricter than Python
        // (Python's sync seek does no offset validation). The exact message is asserted by
        // the tests (DoD §3). Thrown even when the consumer is closed (before ThrowIfClosed).
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset), offset, "seek offset must not be a negative number");
        }

        ThrowIfClosed();

        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
        KafkaException? failure = KafkaException.FromHandle(
            NativeMethods.ConsumerSeek(_handle, topicPin.Pointer, partition, offset));
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Seeks <c>(topic, partition)</c> to <paramref name="offsetAndMetadata"/>'s offset,
    /// carrying its commit metadata + leader epoch (<b>sync</b>; M5/P7) — the .NET realization
    /// of Java <c>seek(TopicPartition, OffsetAndMetadata)</c>, calling the sync ABI
    /// <c>Consumer_seek_with_metadata</c> directly. Same sync-op discipline as
    /// <see cref="Seek(string, int, long)"/> (preconditions → <see cref="ThrowIfClosed"/> →
    /// call-scoped pins → P/Invoke → <see cref="KafkaException.FromHandle(IntPtr)"/>).
    /// </summary>
    /// <remarks>
    /// <b>No offset guard here (unlike <see cref="Seek(string, int, long)"/>).</b> The
    /// <see cref="OffsetAndMetadata"/> constructor already rejects a negative offset (with
    /// <c>"Invalid negative offset"</c>), so a negative offset cannot reach this method — the
    /// ctor is the upstream gate. <see cref="OffsetAndMetadata.Metadata"/> is never null (the
    /// ctor coerces null → <c>""</c>), so a valid pointer is always pinned and passed
    /// (matching Python's <c>offset.metadata or ""</c>); a null leader epoch maps to the ABI's
    /// <c>-1</c> "no epoch" sentinel (Python's <c>epoch or -1</c>). Both strings are pinned
    /// call-scoped.
    /// </remarks>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topic"/> or <paramref name="offsetAndMetadata"/> is null.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure (e.g. an unassigned partition).</exception>
    internal void SeekWithMetadata(string topic, int partition, OffsetAndMetadata offsetAndMetadata)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "Partition must not be negative.");
        }

        if (offsetAndMetadata is null)
        {
            throw new ArgumentNullException(nameof(offsetAndMetadata));
        }

        ThrowIfClosed();

        // Python parity (offset.metadata or ""; epoch or -1): a null leader epoch → the ABI's
        // -1 "no epoch" sentinel. Metadata is never null by construction (the OffsetAndMetadata
        // ctor coerces null → ""), so a valid pointer is always pinned and passed.
        int leaderEpoch = offsetAndMetadata.LeaderEpoch ?? -1;

        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
        using Utf8Marshal.PinnedUtf8String metadataPin = Utf8Marshal.Pin(offsetAndMetadata.Metadata);
        KafkaException? failure = KafkaException.FromHandle(
            NativeMethods.ConsumerSeekWithMetadata(
                _handle,
                topicPin.Pointer,
                partition,
                offsetAndMetadata.Offset,
                leaderEpoch,
                metadataPin.Pointer));
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Returns the current lag of <c>(topic, partition)</c> (<b>sync</b>; M5/P7) — the .NET
    /// realization of Java <c>currentLag(TopicPartition)</c>, calling the sync ABI
    /// <c>Consumer_current_lag</c> directly (a non-blocking local read). Maps Java's
    /// <c>OptionalLong.empty</c> → <c>null</c>. No error handle, no completion bridge, no
    /// <see cref="CancellationToken"/>.
    /// </summary>
    /// <remarks>
    /// <b>Python-parity <c>false</c> → <c>null</c> (no concurrent-read split).</b> The ABI's
    /// <c>false</c> means <em>either</em> the lag is unknown <em>or</em> the access guard could
    /// not be acquired (a concurrent read) — the binding maps <b>both</b> to <c>null</c>,
    /// matching the Python sibling (<c>bindings/python/consumer.py:279</c> returns the raw
    /// value, <c>None</c> on false). <c>CurrentLag</c> deliberately does NOT get the
    /// <see cref="InvalidOperationException"/> concurrent-state-read treatment that
    /// <see cref="GroupId"/> / <see cref="Assignment"/> use (those return a null owned handle
    /// on rejection; <c>current_lag</c> conflates the two into a bare <c>false</c>, so the
    /// split is not observable). The check-then-use handle TOCTOU vs a concurrent teardown
    /// that used to be an accepted residual here is <b>closed</b> (M9/P4 H1):
    /// <c>Consumer_current_lag</c> takes the <see cref="SafeConsumerHandle"/>, so the
    /// marshaller holds a reference for the whole native call.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    internal long? CurrentLag(string topic, int partition)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "Partition must not be negative.");
        }

        ThrowIfClosed();

        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
        return NativeMethods.ConsumerCurrentLag(
            _handle, topicPin.Pointer, partition, out long lag)
            ? lag
            : (long?)null;
    }

    /// <summary>
    /// Assigns the consumer to <paramref name="partitions"/> (async; Java
    /// <c>assign(Collection)</c>) — the public async assignment. An <b>empty</b> collection
    /// clears the assignment (Java parity), NOT a no-op error. Reuses the void completion
    /// bridge over <c>Consumer_assign_async</c>. On a <c>MockConsumer</c> the op resolves
    /// broker-free (<c>assign_from_user</c> is infallible).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task AssignWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        SubmitPartitionOp(partitions, cancellationToken, NativeMethods.ConsumerAssignAsync);

    /// <summary>
    /// Pauses fetching for <paramref name="partitions"/> (async; Java
    /// <c>pause(Collection)</c>). An <b>empty</b> collection is a no-op success (Java
    /// iterates an empty collection). Reuses the void completion bridge over
    /// <c>Consumer_pause_async</c>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task PauseWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        SubmitPartitionOp(partitions, cancellationToken, NativeMethods.ConsumerPauseAsync);

    /// <summary>
    /// Resumes fetching for <paramref name="partitions"/> (async; Java
    /// <c>resume(Collection)</c>). An <b>empty</b> collection is a no-op success. Reuses the
    /// void completion bridge over <c>Consumer_resume_async</c>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task ResumeWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        SubmitPartitionOp(partitions, cancellationToken, NativeMethods.ConsumerResumeAsync);

    /// <summary>
    /// Requests an EARLIEST offset reset for <paramref name="partitions"/> (async; Java
    /// <c>seekToBeginning(Collection)</c>). An <b>empty</b> collection is a no-op success.
    /// Reuses the void completion bridge over <c>Consumer_seek_to_beginning_async</c>. On a
    /// <c>MockConsumer</c> this sets only the reset <em>strategy</em> and resolves broker-free
    /// with no offset setup; the reset offset is consulted lazily on the next poll (from the
    /// map populated by <see cref="UpdateBeginningOffset"/>).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task SeekToBeginningWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        SubmitPartitionOp(partitions, cancellationToken, NativeMethods.ConsumerSeekToBeginningAsync);

    /// <summary>
    /// Requests a LATEST offset reset for <paramref name="partitions"/> (async; Java
    /// <c>seekToEnd(Collection)</c>). An <b>empty</b> collection is a no-op success. Reuses
    /// the void completion bridge over <c>Consumer_seek_to_end_async</c> — the LATEST analog
    /// of <see cref="SeekToBeginningWithCallback"/> (lazily consults the map populated by
    /// <see cref="UpdateEndOffset"/>).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task SeekToEndWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        SubmitPartitionOp(partitions, cancellationToken, NativeMethods.ConsumerSeekToEndAsync);

    /// <summary>
    /// Polls for records (async, <b>typed</b>; PLAN M6/P1b §5) — the owned-handle completion
    /// bridge over <c>poll_async</c>, deserializing each record's key/value with
    /// <paramref name="keyDeserializer"/> / <paramref name="valueDeserializer"/>. The
    /// returned <see cref="Task{TResult}"/> resolves with an owned
    /// <see cref="ConsumerRecords{TKey, TValue}"/> — copied out (and deserialized) on the
    /// core's foreign dispatcher thread, §6.4, a non-null result with <c>Count == 0</c> for
    /// an empty poll — or faults with a <see cref="KafkaException"/> on failure (e.g. a
    /// <c>MockConsumer</c> with an injected poll error) or a <see cref="SerializationException"/>
    /// if a deserializer throws (PLAN §6). A concurrent second op is rejected by the core
    /// inline and faults the <see cref="Task"/> with a <see cref="KafkaException"/>
    /// (ConcurrentModification, ffi §B5).
    /// </summary>
    /// <remarks>
    /// <b>Zero-copy typed path (the crux).</b> The deserialize runs inside the poll
    /// completion callback (<see cref="TypedPollCallbacks{TKey, TValue}"/>) on the
    /// dispatcher thread, reading a span directly over the native batch <em>before</em> the
    /// batch is destroyed — no intermediate per-record <c>byte[]</c>. A user-deserializer
    /// throw is wrapped in a <see cref="SerializationException"/> and faults the
    /// <see cref="Task"/>; it never unwinds into native (the callback's no-throw boundary).
    /// The serdes are captured in the per-op <see cref="GCHandle"/> context
    /// (<see cref="TypedPollCompletionSource{TKey, TValue}"/>) alongside the
    /// <c>TaskCompletionSource</c>.
    /// </remarks>
    /// <param name="timeout">
    /// The poll timeout (Java <c>Duration</c> → <c>int64_t</c> ms). Must be
    /// non-negative.
    /// </param>
    /// <param name="keyDeserializer">The key deserializer.</param>
    /// <param name="valueDeserializer">The value deserializer.</param>
    /// <param name="cancellationToken">Best-effort cancellation → <c>wakeup()</c> (ffi §B7).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<ConsumerRecords<TKey, TValue>> PollWithCallback<TKey, TValue>(
        TimeSpan timeout,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        CancellationToken cancellationToken = default)
    {
        // Precondition BEFORE any P/Invoke (ffi §B5): a negative timeout is a
        // programmer error, not a Kafka outcome.
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout), timeout, "Timeout must not be negative.");
        }

        long timeoutMs = (long)timeout.TotalMilliseconds;

        return SubmitTypedPollOperation(
            keyDeserializer,
            valueDeserializer,
            cancellationToken,
            (consumer, callback, userData) =>
                NativeMethods.ConsumerPollAsync(consumer, timeoutMs, callback, userData));
    }

    /// <summary>
    /// Returns the current position of <paramref name="partition"/> (async) — the M5/P2
    /// proof of the <b>scalar</b> completion bridge (ffi §B6/§B7), the third bridge shape
    /// after the void and owned-handle forms. The returned <see cref="Task{TResult}"/>
    /// resolves with the offset (an <see cref="long"/> carried directly in the callback —
    /// no owned handle, no copy-out), or faults with a <see cref="KafkaException"/> on
    /// failure. The canonical broker-free failure is a position query for an
    /// <b>unassigned</b> partition — the mock core returns an <c>illegal_argument</c> error
    /// (<c>"You can only check the position for partitions assigned to this consumer."</c>).
    /// A concurrent second op is rejected by the core inline and faults the
    /// <see cref="Task"/> with a <see cref="KafkaException"/> (ConcurrentModification,
    /// ffi §B5).
    /// </summary>
    /// <remarks>
    /// <b>Async, blocks-in-Java (CLAUDE.md §4 idiom map).</b> Java's
    /// <c>AsyncKafkaConsumer.position</c> blocks — it does a cross-thread event round-trip
    /// (<c>updateFetchPositions</c>) — so it maps to a <see cref="Task"/>. Only the async
    /// ABI form (<c>position_async</c>, no timeout param) is used; the sync
    /// <c>Consumer_position</c> is deliberately NOT declared (wrapping it in
    /// <c>Task.Run</c> would be the forbidden sync-over-async, ffi §B7). Java's
    /// <c>position(tp, Duration)</c> timeout overload is deferred until a timed
    /// <c>position_async</c> ABI exists (the shipped <c>Close</c> precedent). The
    /// <paramref name="cancellationToken"/> is <b>user-initiated cancellation only, not a
    /// timeout</b> — it maps to <c>wakeup()</c> (best-effort, ffi §B7), mirroring
    /// <see cref="PollWithCallback"/>.
    /// </remarks>
    /// <param name="partition">The topic-partition whose position to read.</param>
    /// <param name="cancellationToken">Best-effort cancellation → <c>wakeup()</c> (ffi §B7).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<long> PositionWithCallback(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        // Preconditions BEFORE any pin / P-Invoke (ffi §B5): the ABI does not validate
        // them, and a null topic / negative partition are programmer errors, not Kafka
        // outcomes. Match the shipped Seek / Assign precedent exactly.
        if (partition.Topic is null)
        {
            throw new ArgumentNullException(nameof(partition), "TopicPartition.Topic must not be null.");
        }

        if (partition.Partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition.Partition, "Partition must not be negative.");
        }

        return SubmitScalarOperation<long>(cancellationToken, (consumer, callback, userData) =>
        {
            // Call-scoped pin: position_async reads/copies the topic string synchronously
            // during the submit call (the header's safety note requires only a valid C
            // string for the call's duration — no borrow past the return), so the buffer
            // is freed once the native call returns (ffi §A3/§B3 call-scoped pin), matching
            // the shipped Seek topic marshalling.
            using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(partition.Topic);
            NativeMethods.ConsumerPositionAsync(
                consumer, topicPin.Pointer, partition.Partition, callback, userData);
        });
    }

    /// <summary>
    /// Returns the last committed offset for each of <paramref name="partitions"/> (async;
    /// Java <c>committed(Set&lt;TopicPartition&gt;)</c>) — the M5/P4 proof of the
    /// owned-handle <c>OffsetMap_t</c> completion. The returned
    /// <see cref="Task{TResult}"/> resolves with an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}"/> (copied out of
    /// the native map on the dispatcher thread, §6.4) — an <b>empty</b> dictionary for
    /// uncommitted / unassigned partitions (the mock omits absent TPs) — or faults with a
    /// <see cref="KafkaException"/>. A concurrent second op faults the <see cref="Task"/>
    /// (ConcurrentModification, ffi §B5).
    /// </summary>
    /// <remarks>
    /// <b>Reachability (mock).</b> The mock's <c>committed</c> map is populated only by the
    /// commit-with-offsets family, which is not yet wired in the binding — so broker-free a
    /// non-empty result is not observable end-to-end this phase; the non-empty copy-out is
    /// exercised by the direct <c>OffsetMapMarshal</c> unit test, and the non-empty
    /// end-to-end assertion is deferred to the commit-family phase (which reuses
    /// <see cref="OffsetAndMetadata"/>). No <c>TimeSpan</c> overload — the async ABI has no
    /// timeout (the shipped <c>Position</c> / <c>Close</c> precedent); the
    /// <paramref name="cancellationToken"/> is user cancellation → <c>wakeup()</c>, not a
    /// deadline.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> CommittedWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        int count = snapshot.Length;
        int[] partitionArray = ExtractPartitions(snapshot);

        return SubmitOwnedHandleOperation<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>(
            cancellationToken,
            (consumer, userData) =>
                WithPinnedTopics(count, i => snapshot[i].Topic, partitionArray, (pointers, parts, cnt) =>
                    NativeMethods.ConsumerCommittedAsync(
                        consumer, pointers, parts, cnt, ConsumerCallbacks.Committed, userData)));
    }

    /// <summary>
    /// Looks up the offset of the first record at or after each timestamp in
    /// <paramref name="timestampsToSearch"/> (async; Java
    /// <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;)</c>) — the M5/P4 owned-handle
    /// <c>OffsetAndTimestampMap_t</c> completion with the one <b>map input</b> shape. The
    /// returned <see cref="Task{TResult}"/> resolves with an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, OffsetAndTimestamp}"/> — or faults
    /// with a <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// <b>Reachability (mock).</b> The mock's <c>offsets_for_times</c> returns
    /// <c>unsupported_version</c> unconditionally (mirroring Java's not-implemented
    /// <c>MockConsumer</c>), so every broker-free call on <c>AsyncMockConsumer</c> faults
    /// the <see cref="Task"/> — even for an empty map (the FFI does not short-circuit empty
    /// before the mock call). The full member is still wired (Java-public; the success /
    /// copy-out path is proven by the other two offset-map marshallers of identical shape).
    /// A <b>negative timestamp</b> is a Kafka-valid sentinel (EARLIEST/LATEST special
    /// timestamps) and is passed through, NOT rejected. No <c>TimeSpan</c> overload (the
    /// async ABI has no timeout).
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>> OffsetsForTimesWithCallback(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch,
        CancellationToken cancellationToken = default)
    {
        // Validate + snapshot BEFORE any pin / P-Invoke (ffi §B5), via the shared
        // SnapshotTimestamps helper — reused verbatim by the sync OffsetsForTimes (M5/P8b), so
        // the map-input validation is NOT duplicated (DoD §6, the shared SnapshotCommitOffsets /
        // WithPinnedTopicsOnly precedent). null map rejected; empty map valid (count == 0); a
        // NEGATIVE timestamp is a Kafka-valid sentinel (EARLIEST/LATEST) passed through, not
        // rejected.
        TimestampsSnapshot snapshot = SnapshotTimestamps(timestampsToSearch);

        return SubmitOwnedHandleOperation<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>>(
            cancellationToken,
            (consumer, userData) =>
                WithPinnedTopicsAndTimestamps(
                    snapshot.Count, i => snapshot.Topics[i], snapshot.Partitions, snapshot.Timestamps,
                    (pointers, parts, times, cnt) =>
                        NativeMethods.ConsumerOffsetsForTimesAsync(
                            consumer, pointers, parts, times, cnt, ConsumerCallbacks.OffsetsForTimes, userData)));
    }

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/>
    /// (async; Java <c>beginningOffsets(Collection&lt;TopicPartition&gt;)</c>) — an
    /// owned-handle <c>LongOffsetMap_t</c> completion. The returned
    /// <see cref="Task{TResult}"/> resolves with an owned
    /// <see cref="IReadOnlyDictionary{TopicPartition, Int64}"/> — or faults with a
    /// <see cref="KafkaException"/>.
    /// </summary>
    /// <remarks>
    /// <b>Reachability (mock).</b> Fully data-testable broker-free: set an offset via the
    /// shipped <see cref="UpdateBeginningOffset"/>, then this returns it; a TP with no
    /// offset set faults with <c>illegal_state</c> (<c>"The partition &lt;tp&gt; does not
    /// have a beginning offset."</c>). No <c>TimeSpan</c> overload (the async ABI has no
    /// timeout).
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsetsWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        SubmitLongOffsetsOp(partitions, cancellationToken, NativeMethods.ConsumerBeginningOffsetsAsync);

    /// <summary>
    /// Returns the latest offset (log-end offset) for each of <paramref name="partitions"/>
    /// (async; Java <c>endOffsets(Collection&lt;TopicPartition&gt;)</c>) — the LATEST analog
    /// of <see cref="BeginningOffsetsWithCallback"/>, sharing the same
    /// <c>long_offsets_callback_t</c> and <c>LongOffsetMap_t</c> result.
    /// </summary>
    /// <remarks>
    /// <b>Reachability (mock).</b> Symmetric to <see cref="BeginningOffsetsWithCallback"/>:
    /// set an offset via the shipped <see cref="UpdateEndOffset"/>, then this returns it; a
    /// TP with no offset set faults with <c>illegal_state</c> (<c>"The partition &lt;tp&gt;
    /// does not have an end offset."</c>).
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsetsWithCallback(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken = default) =>
        SubmitLongOffsetsOp(partitions, cancellationToken, NativeMethods.ConsumerEndOffsetsAsync);

    /// <summary>
    /// Shared body for <see cref="BeginningOffsetsWithCallback"/> /
    /// <see cref="EndOffsetsWithCallback"/> (they share <c>long_offsets_callback_t</c> and
    /// the <c>LongOffsetMap_t</c> result). Validates + snapshots the collection (§B5), then
    /// runs the owned-handle bridge with <paramref name="submit"/> (the correct
    /// <c>beginning</c> / <c>end</c> <c>_async</c> fn) and the shared
    /// <see cref="ConsumerCallbacks.LongOffsets"/> trampoline.
    /// </summary>
    private Task<IReadOnlyDictionary<TopicPartition, long>> SubmitLongOffsetsOp(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken,
        NativeLongOffsetsSubmit submit)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        int count = snapshot.Length;
        int[] partitionArray = ExtractPartitions(snapshot);

        return SubmitOwnedHandleOperation<IReadOnlyDictionary<TopicPartition, long>>(
            cancellationToken,
            (consumer, userData) =>
                WithPinnedTopics(count, i => snapshot[i].Topic, partitionArray, (pointers, parts, cnt) =>
                    submit(consumer, pointers, parts, cnt, ConsumerCallbacks.LongOffsets, userData)));
    }

    /// <summary>
    /// Commits the current positions (async; Java <c>commitSync()</c>) — the M5/P6
    /// <b>confirming</b> commit with no explicit offsets. The returned <see cref="Task"/>
    /// completes when the core resolves the commit (successfully for a <c>MockConsumer</c>,
    /// which stores the current positions broker-free), or faults with a
    /// <see cref="KafkaException"/>. Reuses the shipped void completion bridge
    /// (<see cref="SubmitVoidOperation"/> + <see cref="ConsumerCallbacks.Operation"/>) over
    /// <c>Consumer_commit_sync_async</c> — a one-line clone of
    /// <see cref="UnsubscribeWithCallback"/> (the no-arg void-bridge precedent); NO new
    /// bridge / callback this phase. A concurrent second op is rejected by the core inline
    /// and faults the <see cref="Task"/> (ConcurrentModification, ffi §B5).
    /// </summary>
    /// <remarks>
    /// <b>Async-bridged, blocks-in-Java (CLAUDE.md §4).</b> Java's <c>commitSync</c> blocks,
    /// so the idiom map maps it to a <see cref="Task"/>; bridging it over the void
    /// <c>op_callback_t</c> (rather than a blocking-thread <c>CommitSync</c> façade) avoids
    /// the blocking-thread footgun. The <paramref name="cancellationToken"/> is user-initiated
    /// cancellation → <c>wakeup()</c> (best-effort, ffi §B7), not a timeout.
    /// </remarks>
    /// <param name="cancellationToken">Best-effort cancellation → <c>wakeup()</c> (ffi §B7).</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task CommitWithCallback(CancellationToken cancellationToken = default)
    {
        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
            NativeMethods.ConsumerCommitSyncAsync(consumer, callback, userData));
    }

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> (async; Java
    /// <c>commitSync(Map&lt;TopicPartition, OffsetAndMetadata&gt;)</c>) — the M5/P6
    /// <b>confirming</b> commit with explicit offsets. The returned <see cref="Task"/>
    /// completes when the core resolves the commit (a <c>MockConsumer</c> stores them into
    /// its committed map broker-free, so a subsequent <see cref="CommittedWithCallback"/> on
    /// an <b>assigned</b> partition reads the exact value back), or faults with a
    /// <see cref="KafkaException"/>. Reuses the shipped void completion bridge
    /// (<see cref="SubmitVoidOperation"/> + <see cref="ConsumerCallbacks.Operation"/>) over
    /// <c>Consumer_commit_sync_offsets_async</c> plus the new
    /// <see cref="WithPinnedCommitOffsets"/> 5-array marshaller; NO new bridge / callback.
    /// </summary>
    /// <remarks>
    /// Same async-bridged mapping and cancellation semantics as the no-offsets
    /// <see cref="CommitWithCallback(CancellationToken)"/>. An <b>empty</b> map commits
    /// nothing (<c>count == 0</c>, a valid pass-through — never a throw, §B5). The offsets are
    /// validated + snapshotted (§B5) BEFORE any pin / P-Invoke via
    /// <see cref="SnapshotCommitOffsets"/>.
    /// </remarks>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="cancellationToken">Best-effort cancellation → <c>wakeup()</c> (ffi §B7).</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task CommitWithCallback(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default)
    {
        // Snapshot + validate BEFORE any pin / P-Invoke (ffi §B5): the ABI does not validate
        // preconditions and panics/mismaps on violation. Produces the five parallel arrays.
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsets(offsets);

        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
            WithPinnedCommitOffsets(snapshot, (topics, parts, offs, epochs, meta, cnt) =>
                NativeMethods.ConsumerCommitSyncOffsetsAsync(
                    consumer, topics, parts, offs, epochs, meta, cnt, callback, userData)));
    }

    /// <summary>
    /// Commits the consumed offsets fire-and-forget (Java <c>commitAsync()</c>) — the M5/P6
    /// best-effort commit. <b>Sync-returning and non-blocking</b>: it returns the instant the
    /// core has initiated the async commit (the ABI's <c>Consumer_commit_async</c> "returns
    /// once the async commit is initiated"). Structurally identical to the shipped
    /// <see cref="EnforceRebalance"/> sync-op path: <see cref="ThrowIfClosed"/>, the FFI call,
    /// then <see cref="KafkaException.FromHandle(IntPtr)"/> throw-iff-non-null — no pin, no
    /// <see cref="GCHandle"/>, no completion bridge, no <see cref="CancellationToken"/>
    /// (fire-and-forget; nothing to cancel, matching Python's no-arg <c>commit_async()</c>).
    /// </summary>
    /// <remarks>
    /// <b>Concurrency (single-owner).</b> A concurrent op leaves the core's sync path
    /// returning a non-null error handle → a thrown <see cref="KafkaException"/>
    /// (ConcurrentModification), the sync analog of the async ops' faulted <see cref="Task"/>
    /// (ffi §B5); there is no managed guard (M3/P2). The same check-then-use handle TOCTOU vs
    /// a concurrent teardown as <see cref="EnforceRebalance"/> is the accepted single-owner
    /// residual.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit-initiation failure.</exception>
    internal void CommitAsync()
    {
        ThrowIfClosed();

        // Uniform sync-op discipline (ffi §B5): FromHandle frees the handle exactly once and
        // returns null on success. Throw only for a non-null error. Identical shape to
        // EnforceRebalance — no pin, no GCHandle, no bridge.
        KafkaException? failure = KafkaException.FromHandle(
            NativeMethods.ConsumerCommitAsync(_handle));
        if (failure is not null)
        {
            throw failure;
        }
    }

    // ---- Synchronous consumer surface (the blocking mirror of the async ops) — M5/P8a ----
    //
    // Every method here calls the SYNC C ABI DIRECTLY (no completion callback, no GCHandle,
    // no CancellationToken): the core's block_on parks the caller thread inside the Rust
    // multi-thread runtime (deadlock-free, ffi §B1) — the shipped Seek / CurrentLag /
    // EnforceRebalance sync-op precedent, NOT sync-over-async. There is NO managed block_on
    // over the async binding API — the sync and async families are siblings over this one
    // NativeConsumer, not one wrapping the other. Discipline: preconditions BEFORE any pin /
    // P-Invoke (§B5) → ThrowIfClosed → call-scoped pin (reusing WithPinnedTopics /
    // WithPinnedTopicsOnly / WithPinnedCommitOffsets) → P/Invoke → KafkaException.FromHandle
    // throw-iff-non-null (poll: copy-out-then-destroy first). A concurrent op from another
    // thread is rejected by the core inline (the sync path returns a ConcurrentModification
    // error handle) → a synchronous KafkaException — the single-owner contract, delivered by
    // the core (there is no managed guard, M3/P2).

    /// <summary>
    /// Polls for records (<b>sync, typed</b>; Java <c>poll(Duration)</c>; PLAN M6/P1b §5) —
    /// the sync mirror of <see cref="PollWithCallback"/>. Calls the sync ABI
    /// <c>Consumer_poll</c> directly, then <b>deserializes + copies out</b> the owned batch on
    /// the <b>caller's</b> thread via <see cref="ConsumerRecordsMarshal.CopyOut"/> (a span
    /// directly over the native batch — no intermediate per-record <c>byte[]</c>) and destroys
    /// it in a <c>finally</c> (the §6.4 copy-out default; <see cref="NativeMethods.ConsumerRecordsDestroy"/>
    /// is null-safe, so the failure path — where the returned handle is null — is a no-op).
    /// Returns an owned <see cref="ConsumerRecords{TKey, TValue}"/> (a non-null result with
    /// <c>Count == 0</c> for an empty poll), or throws a <see cref="KafkaException"/> on
    /// failure, or a <see cref="SerializationException"/> if a deserializer throws (PLAN §6 —
    /// on the sync path it surfaces as a synchronous throw). A <c>Wakeup()</c> from another
    /// thread makes a blocking poll return a Wakeup <see cref="KafkaException"/> (one-shot; the
    /// block_on drives the same <c>poll()</c> future the async path awaits, and
    /// <c>Consumer_wakeup</c> fires the same rotating token).
    /// </summary>
    /// <param name="timeout">The poll timeout (Java <c>Duration</c> → <c>int64_t</c> ms). Must be non-negative.</param>
    /// <param name="keyDeserializer">The key deserializer.</param>
    /// <param name="valueDeserializer">The value deserializer.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative (checked before any native call, even when closed).</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a poll failure (or a <c>Wakeup()</c> interrupted it).</exception>
    /// <exception cref="SerializationException">A deserializer threw (PLAN §6).</exception>
    internal ConsumerRecords<TKey, TValue> PollTyped<TKey, TValue>(
        TimeSpan timeout,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer)
    {
        // Precondition BEFORE any P/Invoke (ffi §B5), matching PollWithCallback: a negative
        // timeout is a programmer error, not a Kafka outcome — thrown even when closed (the
        // argument check precedes ThrowIfClosed).
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout), timeout, "Timeout must not be negative.");
        }

        ThrowIfClosed();

        long timeoutMs = (long)timeout.TotalMilliseconds;
        IntPtr records = NativeMethods.ConsumerPoll(_handle, timeoutMs, out IntPtr error);
        try
        {
            // On failure the ABI returns a null batch + a non-null error; FromHandle frees the
            // error and returns the exception. On success error is null and records is a
            // non-null owned borrow-root — deserialize + copy out on THIS (caller) thread,
            // then destroy.
            KafkaException? failure = KafkaException.FromHandle(error);
            if (failure is not null)
            {
                throw failure;
            }

            return ConsumerRecordsMarshal.CopyOut(records, keyDeserializer, valueDeserializer);
        }
        finally
        {
            // Free the batch exactly once, on every path (§B2/§6.4). Null-safe: a no-op on the
            // failure path (records is null); a real free after the copy-out on success. The
            // copy-out retains no borrowed pointer, so the destroy is safe. A
            // SerializationException from the copy-out still runs this finally (the batch is
            // freed) and then propagates as a synchronous throw.
            NativeMethods.ConsumerRecordsDestroy(records);
        }
    }

    /// <summary>
    /// Subscribes to <paramref name="topics"/> (<b>sync</b>; Java <c>subscribe(Collection)</c>)
    /// — the sync mirror of <see cref="SubscribeWithCallback(IReadOnlyCollection{string}, CancellationToken)"/>. Calls the sync ABI
    /// <c>Consumer_subscribe</c> directly. The topic strings are pinned call-scoped via the
    /// shared <see cref="WithPinnedTopicsOnly"/> (the core copies them synchronously during the
    /// call, ffi §A3/§A4).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    internal void Subscribe(IReadOnlyCollection<string> topics)
    {
        // Snapshot + validate BEFORE native (ffi §B5), matching SubscribeWithCallback.
        string[] topicArray = SnapshotTopics(topics);

        ThrowIfClosed();

        IntPtr error = IntPtr.Zero;
        WithPinnedTopicsOnly(topicArray.Length, i => topicArray[i], (pointers, cnt) =>
            error = NativeMethods.ConsumerSubscribe(_handle, pointers, cnt));

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Subscribes to <paramref name="topics"/> with a rebalance listener (<b>sync</b>; Java
    /// <c>subscribe(Collection, ConsumerRebalanceListener)</c>) — the sync mirror of
    /// <see cref="SubscribeWithCallback(IReadOnlyCollection{string}, IConsumerRebalanceListener, CancellationToken)"/>,
    /// calling the sync ABI <c>Consumer_subscribe_with_listener</c> directly.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Sync → sync ABI (a deliberate, declared parity break with Python).</b> Python's
    /// synchronous <c>Consumer.subscribe</c> routes through the <em>async</em> ABI
    /// (<c>consumer.py:786-788</c>) for one Python-specific reason: it must not hold the GIL
    /// across a callback dispatch. .NET has no GIL, and this binding's shipped sync
    /// <see cref="Subscribe(IReadOnlyCollection{string})"/> already calls the sync ABI, so the
    /// mechanism diverges while the observable behaviour does not. Ruled as P6-D2 option (a).
    /// </para>
    /// <para>
    /// <b>The listener handle is consumed unconditionally</b>, success or failure
    /// (<c>confluent_kafka.h:2104-2113</c>) — never destroy it after this call returns. The
    /// narrow exception is the P/Invoke itself throwing (the <see cref="SafeConsumerHandle"/>
    /// marshaller can throw <see cref="ObjectDisposedException"/> against a concurrent
    /// teardown), where native never ran and the handle must be destroyed.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> or <paramref name="listener"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a subscribe failure.</exception>
    internal void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener listener)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        if (listener is null)
        {
            throw new ArgumentNullException(nameof(listener));
        }

        string[] topicArray = SnapshotTopics(topics);

        ThrowIfClosed();

        ListenerRegistration registration = ListenerRegistration.Root(listener);
        IntPtr error = IntPtr.Zero;
        bool consumed = false;
        try
        {
            WithPinnedTopicsOnly(topicArray.Length, i => topicArray[i], (pointers, cnt) =>
            {
                IntPtr listenerHandle = NewListenerHandle(registration);
                try
                {
                    // SafeHandle-param (ffi §A2; M9/P4 H1): the marshaller AddRefs for the whole
                    // call. Load-bearing here — the listener callbacks fire INSIDE this call, and
                    // that reference is what keeps a concurrent teardown from racing them (the
                    // precondition Consumer_destroy places on its caller,
                    // src/ffi/consumer.rs:513-517).
                    error = NativeMethods.ConsumerSubscribeWithListener(_handle, pointers, cnt, listenerHandle);
                }
                catch
                {
                    NativeMethods.ConsumerRebalanceListenerDestroy(listenerHandle);
                    throw;
                }

                consumed = true;
            });
        }
        catch
        {
            if (!consumed)
            {
                registration.Release();
            }

            throw;
        }

        _listenerRegistration = registration;

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Simulates a rebalance to the <b>new full assignment</b> <paramref name="partitions"/>
    /// on a <c>MockConsumer</c> (mock-only; Java <c>MockConsumer.rebalance(Collection)</c>) —
    /// the broker-free driver for <see cref="IConsumerRebalanceListener"/>. Reuses the shared
    /// sync partition-op path verbatim (<see cref="RunPartitionOpSync"/>): the ABI takes the
    /// same parallel <c>(topics[], partitions[], count)</c> arrays as <c>assign</c>, so no new
    /// marshalling is introduced (DoD §6).
    /// </summary>
    /// <remarks>
    /// It <b>does not return until the listener callbacks have returned</b>, and a callback's
    /// error becomes this call's <see cref="KafkaException"/>. Requires a topic subscription:
    /// a manually assigned consumer fails with "manual assignment in use".
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">A real consumer, no topic subscription, or a listener threw.</exception>
    internal void Rebalance(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOpSync(partitions, NativeMethods.MockConsumerRebalance);

    /// <summary>
    /// Validates and snapshots a topic collection before any pin / P-Invoke (ffi §B5) — the
    /// one shared topic-list precondition path for all four subscribe entry points (DoD §6).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    private static string[] SnapshotTopics(IReadOnlyCollection<string> topics)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        string[] topicArray = new string[topics.Count];
        int index = 0;
        foreach (string topic in topics)
        {
            if (topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(topics));
            }

            topicArray[index++] = topic;
        }

        return topicArray;
    }

    /// <summary>
    /// Builds the ABI listener handle for <paramref name="registration"/>: all three
    /// trampolines (process-rooted <c>static readonly</c> delegates, §B6 keep-alive), the
    /// registration's <c>user_data</c> pointer, and the release hook that is the single
    /// sanctioned free site for that pointer (<see cref="ListenerRegistration"/>).
    /// </summary>
    /// <remarks>
    /// <c>on_partitions_lost</c> is always supplied rather than left NULL. The ABI's NULL
    /// reproduces Java's default (delegate to revoked) inside the core, but on the
    /// netstandard2.0 floor that default lives on
    /// <see cref="ConsumerRebalanceListenerBase"/> instead (no default interface methods), so
    /// <see cref="IConsumerRebalanceListener.OnPartitionsLost"/> is always a real managed
    /// method worth dispatching to.
    /// </remarks>
    private static IntPtr NewListenerHandle(ListenerRegistration registration) =>
        NativeMethods.ConsumerRebalanceListenerNew(
            ConsumerCallbacks.PartitionsRevoked,
            ConsumerCallbacks.PartitionsAssigned,
            ConsumerCallbacks.PartitionsLost,
            registration.UserData,
            ConsumerCallbacks.ListenerUserDataDestroy);

    /// <summary>
    /// Unsubscribes from all topics / partitions (<b>sync</b>; Java <c>unsubscribe()</c>) — the
    /// sync mirror of <see cref="UnsubscribeWithCallback"/>. Calls the sync ABI
    /// <c>Consumer_unsubscribe</c> directly.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    internal void Unsubscribe()
    {
        ThrowIfClosed();

        KafkaException? failure = KafkaException.FromHandle(
            NativeMethods.ConsumerUnsubscribe(_handle));
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Assigns the consumer to <paramref name="partitions"/> (<b>sync</b>; Java
    /// <c>assign(Collection)</c>) — the sync mirror of <see cref="AssignWithCallback"/>. An
    /// <b>empty</b> collection clears the assignment (Java parity); a null collection is
    /// rejected. Distinct from the tuple-form driver
    /// <see cref="Assign(IReadOnlyList{ValueTuple{string, int}})"/> (a mock test helper) — both
    /// route through the shared <see cref="RunPartitionOpSync"/> over <c>Consumer_assign</c>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an assignment failure.</exception>
    internal void Assign(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOpSync(partitions, NativeMethods.ConsumerAssign);

    /// <summary>
    /// Pauses fetching for <paramref name="partitions"/> (<b>sync</b>; Java
    /// <c>pause(Collection)</c>) — the sync mirror of <see cref="PauseWithCallback"/>. An empty
    /// collection is a no-op success.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a pause failure (e.g. an unassigned partition).</exception>
    internal void Pause(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOpSync(partitions, NativeMethods.ConsumerPause);

    /// <summary>
    /// Resumes fetching for <paramref name="partitions"/> (<b>sync</b>; Java
    /// <c>resume(Collection)</c>) — the sync mirror of <see cref="ResumeWithCallback"/>. An
    /// empty collection is a no-op success.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a resume failure (e.g. an unassigned partition).</exception>
    internal void Resume(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOpSync(partitions, NativeMethods.ConsumerResume);

    /// <summary>
    /// Requests an EARLIEST offset reset for <paramref name="partitions"/> (<b>sync</b>; Java
    /// <c>seekToBeginning(Collection)</c>) — the sync mirror of
    /// <see cref="SeekToBeginningWithCallback"/>. An empty collection is a no-op success.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    internal void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOpSync(partitions, NativeMethods.ConsumerSeekToBeginning);

    /// <summary>
    /// Requests a LATEST offset reset for <paramref name="partitions"/> (<b>sync</b>; Java
    /// <c>seekToEnd(Collection)</c>) — the sync mirror of <see cref="SeekToEndWithCallback"/>.
    /// An empty collection is a no-op success.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    internal void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions) =>
        RunPartitionOpSync(partitions, NativeMethods.ConsumerSeekToEnd);

    /// <summary>
    /// Returns the current position of <paramref name="partition"/> (<b>sync</b>; Java
    /// <c>position(TopicPartition)</c>) — the sync mirror of <see cref="PositionWithCallback"/>.
    /// Calls the sync ABI <c>Consumer_position</c> directly; on success the offset is written to
    /// the out param, on failure a non-null error handle is returned. The canonical broker-free
    /// failure is a query for an <b>unassigned</b> partition (a synchronous
    /// <see cref="KafkaException"/>).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a position failure (e.g. an unassigned partition).</exception>
    internal long Position(TopicPartition partition)
    {
        // Preconditions BEFORE any pin / P-Invoke (ffi §B5), matching PositionWithCallback.
        if (partition.Topic is null)
        {
            throw new ArgumentNullException(nameof(partition), "TopicPartition.Topic must not be null.");
        }

        if (partition.Partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition.Partition, "Partition must not be negative.");
        }

        ThrowIfClosed();

        long position;
        KafkaException? failure;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(partition.Topic))
        {
            // On failure the ABI leaves out_position untouched, so read the error first and
            // throw before returning the (unset) offset.
            failure = KafkaException.FromHandle(
                NativeMethods.ConsumerPosition(
                    _handle, topicPin.Pointer, partition.Partition, out position));
        }

        if (failure is not null)
        {
            throw failure;
        }

        return position;
    }

    /// <summary>
    /// Commits the current positions (<b>sync</b>; Java <c>commitSync()</c>) — the confirming
    /// commit with no explicit offsets. The sync mirror of
    /// <see cref="CommitWithCallback(CancellationToken)"/>; calls the sync ABI
    /// <c>Consumer_commit_sync</c> directly.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    internal void CommitSync()
    {
        ThrowIfClosed();

        KafkaException? failure = KafkaException.FromHandle(
            NativeMethods.ConsumerCommitSync(_handle));
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> (<b>sync</b>; Java
    /// <c>commitSync(Map)</c>) — the confirming commit with explicit offsets. The sync mirror of
    /// <see cref="CommitWithCallback(IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, CancellationToken)"/>;
    /// calls the sync ABI <c>Consumer_commit_sync_offsets</c> directly. Reuses
    /// <see cref="SnapshotCommitOffsets"/> (validate before native) + the five-array
    /// <see cref="WithPinnedCommitOffsets"/> marshaller. An <b>empty</b> map commits nothing (a
    /// valid pass-through, never a throw).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    internal void CommitSyncOffsets(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        // Validate + snapshot BEFORE any pin / P-Invoke (ffi §B5), reusing the shipped
        // SnapshotCommitOffsets (the async CommitWithCallback(offsets) precedent).
        CommitOffsetsSnapshot snapshot = SnapshotCommitOffsets(offsets);

        ThrowIfClosed();

        IntPtr error = IntPtr.Zero;
        WithPinnedCommitOffsets(snapshot, (topics, parts, offs, epochs, meta, cnt) =>
            error = NativeMethods.ConsumerCommitSyncOffsets(
                _handle, topics, parts, offs, epochs, meta, cnt));

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    // ---- Synchronous consumer query family (the blocking mirror of the async queries) — M5/P8b ----
    //
    // The sync mirror of the six async query wrappers (CommittedWithCallback / ... /
    // ListTopicsWithCallback). Each calls the SYNC C ABI DIRECTLY (no completion callback, no
    // GCHandle, no CancellationToken): the core's block_on parks the caller thread inside the
    // Rust multi-thread runtime (deadlock-free, ffi §B1) — the shipped M5/P8a sync core-loop
    // precedent, NOT sync-over-async. The sync ABI returns a KafkaError* handle (null = success)
    // AND writes an owned-container handle to an out-param. Discipline: preconditions BEFORE any
    // pin / P-Invoke (§B5, reusing SnapshotPartitions / SnapshotTimestamps) → ThrowIfClosed →
    // call-scoped pin (reusing WithPinnedTopics / WithPinnedTopicsAndTimestamps / Utf8Marshal.Pin)
    // → P/Invoke → copy-out-then-destroy via the EXISTING marshaller (§6.4). A concurrent op from
    // another thread is rejected by the core inline (a ConcurrentModification error handle) → a
    // synchronous KafkaException (no managed guard, M3/P2).
    //
    // ⚠ OUT-PARAM PRE-INIT (correctness). The sync query FFI writes *out_handle ONLY on success
    // and LEAVES IT UNTOUCHED on failure (verified in src/ffi/consumer.rs — the error arm does
    // `return box_error(e)` without writing the out-param). Unlike Consumer_poll (which writes
    // out_error on BOTH paths), a blittable `out IntPtr` marshalled here would be pinned-in-place
    // over the managed local's storage, so an untouched native write leaves whatever was there.
    // Every wrapper therefore PRE-INITIALIZES its out-local to IntPtr.Zero before the call, so the
    // failure path yields IntPtr.Zero and the null-safe container _destroy is a no-op.

    /// <summary>
    /// Returns the last committed offset for each of <paramref name="partitions"/> (<b>sync</b>;
    /// Java <c>committed(Set&lt;TopicPartition&gt;)</c>) — the sync mirror of
    /// <see cref="CommittedWithCallback"/>. Calls the sync ABI <c>Consumer_committed</c> directly,
    /// then copies out the owned <c>OffsetMap_t</c> on the caller's thread via
    /// <see cref="OffsetMapMarshal.CopyOut"/> and destroys it in a <c>finally</c> (§6.4). Returns
    /// an <b>empty</b> dictionary for uncommitted / unassigned partitions (the mock omits absent
    /// TPs). On a <c>MockConsumer</c>, an <b>assigned</b> partition reads back the exact value a
    /// prior <see cref="CommitSyncOffsets"/> stored (offset, metadata, and leader epoch).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a committed-query failure.</exception>
    internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(
        IReadOnlyCollection<TopicPartition> partitions) =>
        RunContainerQuerySync(
            partitions, NativeMethods.ConsumerCommitted, OffsetMapMarshal.CopyOut, NativeMethods.OffsetMapDestroy);

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/>
    /// (<b>sync</b>; Java <c>beginningOffsets(Collection)</c>) — the sync mirror of
    /// <see cref="BeginningOffsetsWithCallback"/>. Calls the sync ABI
    /// <c>Consumer_beginning_offsets</c> directly, then copies out the owned
    /// <c>LongOffsetMap_t</c> via <see cref="LongOffsetMapMarshal.CopyOut"/> and destroys it in a
    /// <c>finally</c> (§6.4). On a <c>MockConsumer</c>: set an offset via
    /// <see cref="UpdateBeginningOffset"/>, then this returns it; a TP with no offset set throws
    /// <see cref="KafkaException"/> (<c>illegal_state</c>: "The partition &lt;tp&gt; does not have
    /// a beginning offset.").
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a beginning-offsets failure (e.g. an unset partition).</exception>
    internal IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions) =>
        RunContainerQuerySync(
            partitions,
            NativeMethods.ConsumerBeginningOffsets,
            LongOffsetMapMarshal.CopyOut,
            NativeMethods.LongOffsetMapDestroy);

    /// <summary>
    /// Returns the latest offset (log-end offset) for each of <paramref name="partitions"/>
    /// (<b>sync</b>; Java <c>endOffsets(Collection)</c>) — the sync mirror of
    /// <see cref="EndOffsetsWithCallback"/> and the LATEST analog of
    /// <see cref="BeginningOffsets"/> (same <c>LongOffsetMap_t</c> result). On a
    /// <c>MockConsumer</c>: set an offset via <see cref="UpdateEndOffset"/>, then this returns it;
    /// a TP with no offset set throws <see cref="KafkaException"/> (<c>illegal_state</c>: "The
    /// partition &lt;tp&gt; does not have an end offset.").
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an end-offsets failure (e.g. an unset partition).</exception>
    internal IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions) =>
        RunContainerQuerySync(
            partitions,
            NativeMethods.ConsumerEndOffsets,
            LongOffsetMapMarshal.CopyOut,
            NativeMethods.LongOffsetMapDestroy);

    /// <summary>
    /// Looks up the offset of the first record at or after each timestamp in
    /// <paramref name="timestampsToSearch"/> (<b>sync</b>; Java
    /// <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;)</c>) — the sync mirror of
    /// <see cref="OffsetsForTimesWithCallback"/>. Calls the sync ABI
    /// <c>Consumer_offsets_for_times</c> directly, then copies out the owned
    /// <c>OffsetAndTimestampMap_t</c> via <see cref="OffsetAndTimestampMapMarshal.CopyOut"/> and
    /// destroys it in a <c>finally</c> (§6.4). Reuses the shared <see cref="SnapshotTimestamps"/>
    /// validation (DoD §6).
    /// </summary>
    /// <remarks>
    /// <b>Reachability (mock).</b> The mock's <c>offsets_for_times</c> returns
    /// <c>unsupported_version</c> unconditionally (mirroring Java's not-implemented
    /// <c>MockConsumer</c>), so every broker-free call on <see cref="MockConsumer{TKey, TValue}"/> throws a
    /// <see cref="KafkaException"/> — even for an empty map (the FFI does not short-circuit empty).
    /// A <b>negative timestamp</b> is a Kafka-valid sentinel (EARLIEST/LATEST) and is passed
    /// through, NOT rejected. The success / copy-out path is proven by the two other offset-map
    /// marshallers of identical shape (and their sync siblings).
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an offsets-for-times failure (on the mock, always <c>unsupported_version</c>).</exception>
    internal IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch)
    {
        // Validate + snapshot BEFORE any pin / P-Invoke (ffi §B5), reusing the shared
        // SnapshotTimestamps (also backing the async OffsetsForTimesWithCallback — DoD §6).
        TimestampsSnapshot snapshot = SnapshotTimestamps(timestampsToSearch);

        ThrowIfClosed();

        // Pre-init to IntPtr.Zero: the FFI leaves *out_map untouched on failure (see the ⚠ note
        // above), so the failure path must yield a null handle for the no-op destroy.
        IntPtr error = IntPtr.Zero;
        IntPtr map = IntPtr.Zero;
        WithPinnedTopicsAndTimestamps(
            snapshot.Count, i => snapshot.Topics[i], snapshot.Partitions, snapshot.Timestamps,
            (pointers, parts, times, cnt) =>
                error = NativeMethods.ConsumerOffsetsForTimes(
                    _handle, pointers, parts, times, cnt, out map));

        return ThrowOrCopyOutAndDestroy(
            error, map, OffsetAndTimestampMapMarshal.CopyOut, NativeMethods.OffsetAndTimestampMapDestroy);
    }

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (<b>sync</b>; Java
    /// <c>partitionsFor(String)</c>) — the sync mirror of <see cref="PartitionsForWithCallback"/>.
    /// Calls the sync ABI <c>Consumer_partitions_for</c> directly, then copies out the whole
    /// borrowed tree via <see cref="PartitionInfoListMarshal.CopyOut"/> before destroying the root
    /// in a <c>finally</c> (§6.4/§B2). An <b>empty</b> list for a topic with no registered
    /// partitions (the mock returns empty for an unregistered topic).
    /// </summary>
    /// <remarks>
    /// <b>Empty topic is forwarded, NOT rejected (Java/Python-faithful).</b> The binding guards
    /// only <see langword="null"/> (FFI panic-safety, §B5); an empty topic is passed straight to
    /// the core (the <see cref="PartitionsForWithCallback"/> precedent). The single topic is
    /// pinned <b>call-scoped</b> (the core copies it synchronously during the call, ffi §A3/§B3).
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a partitions-for failure.</exception>
    internal IReadOnlyList<PartitionInfo> PartitionsFor(string topic)
    {
        // Precondition BEFORE any pin / P-Invoke (ffi §B5): guard only null (FFI panic-safety); an
        // EMPTY topic is FORWARDED, not rejected (the async PartitionsForWithCallback precedent).
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        ThrowIfClosed();

        // Pre-init list to IntPtr.Zero (the FFI leaves *out_list untouched on failure).
        IntPtr error;
        IntPtr list = IntPtr.Zero;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic))
        {
            error = NativeMethods.ConsumerPartitionsFor(_handle, topicPin.Pointer, out list);
        }

        return ThrowOrCopyOutAndDestroy(
            error, list, PartitionInfoListMarshal.CopyOut, NativeMethods.PartitionInfoListDestroy);
    }

    /// <summary>
    /// Returns metadata for all topics the consumer is authorized to view (<b>sync</b>; Java
    /// <c>listTopics()</c>) — the sync mirror of <see cref="ListTopicsWithCallback"/>, the one
    /// query with <b>no input</b>. Calls the sync ABI <c>Consumer_list_topics</c> directly, then
    /// copies out the whole borrowed tree via <see cref="TopicPartitionInfoMapMarshal.CopyOut"/>
    /// before destroying the root in a <c>finally</c> (§6.4/§B2). An <b>empty</b> dictionary when
    /// no topics are registered.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a list-topics failure.</exception>
    internal IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics()
    {
        ThrowIfClosed();

        // Pre-init map to IntPtr.Zero (the FFI leaves *out_map untouched on failure).
        IntPtr map = IntPtr.Zero;
        IntPtr error = NativeMethods.ConsumerListTopics(_handle, out map);

        return ThrowOrCopyOutAndDestroy(
            error, map, TopicPartitionInfoMapMarshal.CopyOut, NativeMethods.TopicPartitionInfoMapDestroy);
    }

    /// <summary>
    /// The shared body for the three collection-input sync queries
    /// (<see cref="Committed"/> / <see cref="BeginningOffsets"/> / <see cref="EndOffsets"/>) — the
    /// sync analog of the async <see cref="SubmitOwnedHandleOperation{TResult}"/> generic-over-
    /// result pattern. Validates + snapshots the collection (§B5, via
    /// <see cref="SnapshotPartitions"/>), pins the topics call-scoped (via
    /// <see cref="WithPinnedTopics"/>), P/Invokes <paramref name="submit"/> (the correct
    /// <c>committed</c> / <c>beginning</c> / <c>end</c> sync fn), then copies out via
    /// <paramref name="copyOut"/> and destroys the root via <paramref name="destroy"/>
    /// (<see cref="ThrowOrCopyOutAndDestroy{TResult}"/>). A null collection is rejected; an
    /// <b>empty</b> collection is a valid pass-through (<c>count == 0</c>).
    /// </summary>
    private TResult RunContainerQuerySync<TResult>(
        IReadOnlyCollection<TopicPartition> partitions,
        NativeCollectionQuerySync submit,
        Func<IntPtr, TResult> copyOut,
        Action<IntPtr> destroy)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        int count = snapshot.Length;
        int[] partitionArray = ExtractPartitions(snapshot);

        ThrowIfClosed();

        // Pre-init handle to IntPtr.Zero: the FFI leaves *out_handle untouched on failure (the ⚠
        // note above), so the failure path must yield null for the no-op destroy.
        IntPtr error = IntPtr.Zero;
        IntPtr handle = IntPtr.Zero;
        // SafeHandle-param (ffi §A2; M9/P4 H1b): the marshaller AddRefs for the whole native
        // call, so a concurrent teardown cannot free the consumer mid-query.
        WithPinnedTopics(count, i => snapshot[i].Topic, partitionArray, (pointers, parts, cnt) =>
            error = submit(_handle, pointers, parts, cnt, out handle));

        return ThrowOrCopyOutAndDestroy(error, handle, copyOut, destroy);
    }

    /// <summary>
    /// The shared copy-out-then-destroy tail for every sync query (§6.4): throw iff
    /// <paramref name="error"/> is non-null (<see cref="KafkaException.FromHandle(IntPtr)"/> frees
    /// it), else copy out the owned container via <paramref name="copyOut"/> on the caller's
    /// thread and return the owned managed value — always destroying the root via
    /// <paramref name="destroy"/> in a <c>finally</c> (null-safe, so a no-op on the failure path
    /// where <paramref name="handle"/> is <see cref="IntPtr.Zero"/>). The copy-out retains no
    /// borrowed pointer, so the destroy is safe (§B2). The sync analog of the async completion
    /// trampolines' copy-out-then-<c>_destroy</c> discipline.
    /// </summary>
    private static TResult ThrowOrCopyOutAndDestroy<TResult>(
        IntPtr error, IntPtr handle, Func<IntPtr, TResult> copyOut, Action<IntPtr> destroy)
    {
        try
        {
            KafkaException? failure = KafkaException.FromHandle(error);
            if (failure is not null)
            {
                throw failure;
            }

            return copyOut(handle);
        }
        finally
        {
            destroy(handle);
        }
    }

    /// <summary>
    /// Graceful <b>synchronous</b> close (Java <c>close()</c>) that <b>surfaces</b> the close
    /// error — the public sync <c>Close()</c>'s worker. Takes the one-shot
    /// <see cref="TryBeginClose"/> latch (shared with <see cref="Dispose"/> /
    /// <see cref="DisposeAsync"/> / <see cref="CloseWithCallback"/> — idempotent), calls the
    /// sync ABI <c>Consumer_close</c>, then releases the handle (→ <c>Consumer_destroy</c>) in a
    /// <c>finally</c> so destroy runs exactly once even on a close error. A subsequent teardown
    /// loses the latch and no-ops.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="Dispose"/> (which swallows the close error, best-effort), this
    /// <b>throws</b> it — <c>close()</c> reports failures. No separate-op drain (single-owner:
    /// the awaiter of an op is its disposer). <c>Consumer_close</c> is the graceful bg-task join
    /// (a bare <c>Consumer_destroy</c> would be fire-and-forget), so it precedes destroy.
    /// </remarks>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    internal void CloseSync()
    {
        if (!TryBeginClose())
        {
            // A prior teardown already won the latch — no-op (idempotent).
            return;
        }

        try
        {
            // ⚠ DELIBERATELY NOT the SafeHandle-param form (M9/P4 decision Q2, plan §3.4) —
            // do NOT "finish the job" here. This site has already won the one-shot
            // TryBeginClose latch, and the finally below releases the handle on THIS thread
            // in program order, so the close provably precedes its own destroy. Since
            // Consumer_destroy is reachable only from SafeConsumerHandle.ReleaseHandle ←
            // _handle.Dispose() ← the latch winner, no concurrent destroy can race this call
            // — the hazard H1 protects against does not exist here. Converting it would
            // change invariant I2 (close-before-destroy teardown ordering) for no gain.
            KafkaException? failure = KafkaException.FromHandle(
                NativeMethods.ConsumerClose(_handle.DangerousGetHandle()));
            if (failure is not null)
            {
                throw failure;
            }
        }
        finally
        {
            // ReleaseHandle → Consumer_destroy, exactly once — even if the close threw.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Graceful <b>synchronous</b> close with a timeout (Java <c>close(Duration)</c>) that
    /// <b>surfaces</b> the close error — the public sync <c>Close(TimeSpan)</c>'s worker. The
    /// timed analog of <see cref="CloseSync"/>: same one-shot latch + <c>finally</c>-destroy,
    /// over the sync ABI <c>Consumer_close_with_timeout</c>.
    /// </summary>
    /// <param name="timeoutMs">The close timeout in milliseconds (non-negative; validated by the caller).</param>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    internal void CloseSyncWithTimeout(long timeoutMs)
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            // ⚠ DELIBERATELY NOT the SafeHandle-param form (M9/P4 decision Q2, plan §3.4) —
            // same latch argument as CloseSync above: the latch is already won and the
            // finally below releases the handle on this thread, so no concurrent destroy can
            // race this call. Do NOT convert (it would change invariant I2).
            KafkaException? failure = KafkaException.FromHandle(
                NativeMethods.ConsumerCloseWithTimeout(_handle.DangerousGetHandle(), timeoutMs));
            if (failure is not null)
            {
                throw failure;
            }
        }
        finally
        {
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (async; Java
    /// <c>partitionsFor(String)</c>) — the M5/P5 owned-handle <c>PartitionInfoList_t</c>
    /// completion (Category E2). The returned <see cref="Task{TResult}"/> resolves with an
    /// owned <see cref="IReadOnlyList{PartitionInfo}"/> (the whole borrowed tree copied out
    /// on the dispatcher thread before the root destroy, §6.4/§B2) — an <b>empty</b> list for
    /// a topic with no registered partitions (the mock returns empty for an unregistered
    /// topic) — or faults with a <see cref="KafkaException"/>. A concurrent second op faults
    /// the <see cref="Task"/> (ConcurrentModification, ffi §B5).
    /// </summary>
    /// <remarks>
    /// <b>Empty topic is forwarded, NOT rejected (Java/Python-faithful, PLAN §8.2).</b> The
    /// binding guards only <see langword="null"/> (for FFI panic-safety, §B5); an empty /
    /// whitespace topic is passed straight to the core (the Python sibling does zero topic
    /// validation). The single topic is pinned <b>call-scoped</b> — the core copies it
    /// synchronously during the submit (ffi §A3/§B3), so a scoped <see cref="Utf8Marshal.Pin"/>
    /// is the fit (not the array-shaped <see cref="WithPinnedTopics"/>). No <c>TimeSpan</c>
    /// overload (the async ABI has no timeout — the <c>Position</c> / <c>Close</c> precedent);
    /// the <paramref name="cancellationToken"/> is user cancellation → <c>wakeup()</c>, not a
    /// deadline.
    /// </remarks>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <param name="cancellationToken">Best-effort cancellation → <c>wakeup()</c> (ffi §B7).</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<IReadOnlyList<PartitionInfo>> PartitionsForWithCallback(
        string topic,
        CancellationToken cancellationToken = default)
    {
        // Precondition BEFORE any pin / P-Invoke (ffi §B5): the ABI does not null-check
        // `topic` (it would panic across FFI). An EMPTY topic is NOT rejected — Java/Python
        // do no topic validation; the binding guards only null (PLAN §8.2). Match the shipped
        // Seek / Position null-topic precedent.
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        return SubmitOwnedHandleOperation<IReadOnlyList<PartitionInfo>>(
            cancellationToken,
            (consumer, userData) =>
            {
                // Call-scoped pin: partitions_for_async copies the topic string synchronously
                // during the submit (the header's safety note requires only a valid C string
                // for the call's duration — no borrow past the return), so the buffer is freed
                // once the native call returns (ffi §A3/§B3), matching the shipped Position /
                // Seek topic marshalling. A single topic → the scoped Pin, not the array helper.
                using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
                NativeMethods.ConsumerPartitionsForAsync(
                    consumer, topicPin.Pointer, ConsumerCallbacks.PartitionsFor, userData);
            });
    }

    /// <summary>
    /// Returns metadata for all topics the consumer is authorized to view (async; Java
    /// <c>listTopics()</c>) — the M5/P5 owned-handle <c>TopicPartitionInfoMap_t</c>
    /// completion, the one query with <b>no input</b>. The returned
    /// <see cref="Task{TResult}"/> resolves with an owned
    /// <see cref="IReadOnlyDictionary{String, IReadOnlyList}"/> of topic →
    /// <see cref="PartitionInfo"/> list (the whole borrowed tree copied out on the dispatcher
    /// thread before the root destroy) — an <b>empty</b> dictionary when no topics are
    /// registered — or faults with a <see cref="KafkaException"/>. A concurrent second op
    /// faults the <see cref="Task"/> (ConcurrentModification, ffi §B5).
    /// </summary>
    /// <remarks>
    /// No input to marshal — the submit just P/Invokes <c>list_topics_async</c>. No
    /// <c>TimeSpan</c> overload (the async ABI has no timeout); the
    /// <paramref name="cancellationToken"/> is user cancellation → <c>wakeup()</c>, not a
    /// deadline.
    /// </remarks>
    /// <param name="cancellationToken">Best-effort cancellation → <c>wakeup()</c> (ffi §B7).</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopicsWithCallback(
        CancellationToken cancellationToken = default)
    {
        return SubmitOwnedHandleOperation<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>>(
            cancellationToken,
            (consumer, userData) =>
                NativeMethods.ConsumerListTopicsAsync(consumer, ConsumerCallbacks.ListTopics, userData));
    }

    /// <summary>
    /// Registers partition metadata for <paramref name="topic"/> on a <c>MockConsumer</c>
    /// (mock-only driver; mirrors Java <c>updatePartitions</c>) so
    /// <see cref="PartitionsForWithCallback"/> / <see cref="ListTopicsWithCallback"/> return
    /// data broker-free. Each of <paramref name="partitionCount"/> partitions is built with a
    /// single leader node <c>(leaderId, leaderHost, leaderPort)</c> that is also its sole
    /// replica and in-sync replica (offline replicas empty, no rack — a reachable-slice limit
    /// of the mock). Errors (via <see cref="KafkaException"/>) on a real consumer
    /// (<c>illegal_state</c>). Both strings are pinned call-scoped.
    /// </summary>
    /// <param name="topic">The topic to register partition metadata for.</param>
    /// <param name="partitionCount">The number of partitions to register (non-negative).</param>
    /// <param name="leaderId">The leader node id for every partition.</param>
    /// <param name="leaderHost">The leader host for every partition.</param>
    /// <param name="leaderPort">The leader port for every partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> or <paramref name="leaderHost"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionCount"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the update (e.g. a real consumer).</exception>
    internal void UpdatePartitions(
        string topic,
        int partitionCount,
        int leaderId,
        string leaderHost,
        int leaderPort)
    {
        // Preconditions BEFORE the P/Invoke (ffi §B5): the ABI does not null-check the two
        // required strings (it would panic across FFI), and a negative partition count is a
        // programmer error, not a Kafka outcome.
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (leaderHost is null)
        {
            throw new ArgumentNullException(nameof(leaderHost));
        }

        if (partitionCount < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitionCount), partitionCount, "Partition count must not be negative.");
        }

        ThrowIfClosed();

        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic))
        using (Utf8Marshal.PinnedUtf8String hostPin = Utf8Marshal.Pin(leaderHost))
        {
            error = NativeMethods.MockConsumerUpdatePartitions(
                _handle,
                topicPin.Pointer,
                partitionCount,
                leaderId,
                hostPin.Pointer,
                leaderPort);
        }

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Assigns the consumer to <paramref name="topicPartitions"/> (sync; works on both
    /// the async and mock consumers). Used to make a partition eligible for
    /// <see cref="AddRecord"/> on a <c>MockConsumer</c> — a broker-free driver. The
    /// parallel <c>(topic, partition)</c> arrays are pinned call-scoped (ffi §A4).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topicPartitions"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the assignment.</exception>
    internal void Assign(IReadOnlyList<(string Topic, int Partition)> topicPartitions)
    {
        if (topicPartitions is null)
        {
            throw new ArgumentNullException(nameof(topicPartitions));
        }

        int count = topicPartitions.Count;
        int[] partitions = new int[count];
        for (int i = 0; i < count; i++)
        {
            (string topic, int partition) = topicPartitions[i];
            if (topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(topicPartitions));
            }

            if (partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(topicPartitions), partition, "Partition must not be negative.");
            }

            partitions[i] = partition;
        }

        // Shared sync-op tail (ThrowIfClosed → call-scoped pin → P/Invoke → throw-iff-error)
        // reused by the public sync Assign(IReadOnlyCollection<TopicPartition>) and the five
        // sync partition ops (M5/P8a) — the native call + error handling is not duplicated
        // (DoD §6).
        InvokePartitionOpSync(count, i => topicPartitions[i].Topic, partitions, NativeMethods.ConsumerAssign);
    }

    /// <summary>
    /// Queues a record on a <c>MockConsumer</c> (a broker-free driver; the partition
    /// must already be assigned via <see cref="Assign(IReadOnlyList{ValueTuple{string, int}})"/>).
    /// <paramref name="key"/> /
    /// <paramref name="value"/> are pinned call-scoped; a <see langword="null"/> array is
    /// an absent key / tombstone value. Errors (via <see cref="KafkaException"/>) on a
    /// real consumer or an unassigned partition.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the record (e.g. unassigned partition).</exception>
    internal void AddRecord(
        string topic,
        int partition,
        long offset,
        byte[]? key,
        byte[]? value)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "Partition must not be negative.");
        }

        ThrowIfClosed();

        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic))
        {
            // Call-scoped pins for the key/value byte arrays (ffi §A4): MockConsumer
            // copies them into the record synchronously during the call. A null array
            // → (IntPtr.Zero, -1) (absent); an empty array → a non-null pinned pointer
            // + len 0 (a genuine empty key/value, distinct from absent).
            GCHandle keyPin = default;
            GCHandle valuePin = default;
            try
            {
                (IntPtr keyPtr, int keyLen) = PinBytes(key, ref keyPin);
                (IntPtr valuePtr, int valueLen) = PinBytes(value, ref valuePin);

                error = NativeMethods.MockConsumerAddRecord(
                    _handle,
                    topicPin.Pointer,
                    partition,
                    offset,
                    keyPtr,
                    keyLen,
                    valuePtr,
                    valueLen);
            }
            finally
            {
                if (keyPin.IsAllocated)
                {
                    keyPin.Free();
                }

                if (valuePin.IsAllocated)
                {
                    valuePin.Free();
                }
            }
        }

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Injects an error to be returned by the <b>next</b> poll on a
    /// <c>MockConsumer</c> (mirrors Java <c>setPollException</c>) — the broker-free
    /// FAILURE driver. Errors (via <see cref="KafkaException"/>) on a real consumer.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the injection (e.g. real consumer).</exception>
    internal void SetPollError(string message)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        ThrowIfClosed();

        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String messagePin = Utf8Marshal.Pin(message))
        {
            error = NativeMethods.MockConsumerSetPollError(_handle, messagePin.Pointer);
        }

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Interrupts the in-flight op (Java <c>wakeup()</c>). Sync and cross-thread — the
    /// consumer is single-owner, so this is the one method deliberately callable from
    /// another thread (ffi §B5 / consumer-threading §11). Best-effort: a no-op once
    /// closing/closed (the handle may be about to be destroyed). This <c>_closed</c>
    /// check is strictly safer than Python (whose <c>wakeup</c> has no closed check at
    /// all) and is kept as a deliberate divergence in the safe direction.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The former check-then-use TOCTOU is FIXED (M9/P4 H1d).</b> The closed-flag read
    /// and the handle deref were never atomic, so a concurrent teardown landing between them
    /// freed the handle and the subsequent <c>Consumer_wakeup</c> ran on freed memory. It was
    /// documented as an accepted single-owner residual, but it was not confined to
    /// cross-thread <em>misuse</em>: the .NET gRPC test-harness server reaches it from a
    /// different RPC thread by design — <c>Wakeup</c> is deliberately exempt from the
    /// per-consumer gate there (gating it would deadlock behind the very poll it must wake),
    /// so the reachable caller was the server, not a misbehaving user. The fix is the
    /// <c>SafeHandle</c>-param form below plus the catch; no gRPC change was needed.
    /// </para>
    /// <para>
    /// <b>Why the <c>catch</c> is required, not defensive.</b> <c>Wakeup</c> is documented as
    /// best-effort and a no-op once closing/closed. A bare conversion would make the
    /// marshaller throw <see cref="ObjectDisposedException"/> in exactly the race window this
    /// is closing — surfacing a new exception from a documented no-op, and into a gRPC
    /// handler that does not expect one. Swallowing it preserves the contract: losing the
    /// race with teardown is indistinguishable from arriving after it, and both are no-ops.
    /// </para>
    /// </remarks>
    internal void Wakeup()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            return;
        }

        try
        {
            // SafeHandle-param: the marshaller AddRefs for the duration of the call, so a
            // concurrent teardown cannot free the consumer mid-wakeup (ffi §A2). This closes
            // the documented Wakeup handle TOCTOU.
            NativeMethods.ConsumerWakeup(_handle);
        }
        catch (ObjectDisposedException)
        {
            // Lost the race with a concurrent teardown: the handle closed between the flag
            // read above and the marshaller's AddRef. Wakeup is best-effort and a no-op once
            // closed, so swallow — do NOT surface a new exception from a documented no-op.
        }
    }

    /// <summary>
    /// Reads the configured consumer group id — a representative <b>synchronous state
    /// read</b>. Marshals a Category-3 owned metadata handle and frees it (§B2).
    /// </summary>
    /// <remarks>
    /// <b>Concurrency (single-owner).</b> If the core's own access guard rejects
    /// concurrent access it returns a <b>null</b> metadata handle; this maps to
    /// <see cref="InvalidOperationException"/> ("KafkaConsumer is not safe for
    /// multi-threaded access."), mirroring the Python sibling's
    /// <c>None → RuntimeError</c> (<c>_concurrent_error</c>) and the CLAUDE.md §3
    /// idiom map (concurrent sync state read → <see cref="InvalidOperationException"/>).
    /// The check-then-use handle TOCTOU vs teardown that used to be listed here as an
    /// accepted residual is <b>closed</b> (M9/P4 H1): the underlying
    /// <c>Consumer_group_metadata</c> declaration takes the
    /// <see cref="SafeConsumerHandle"/>, so the marshaller holds a reference for the whole
    /// native call.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for
    /// multi-threaded access).
    /// </exception>
    internal string? GroupId()
    {
        ThrowIfClosed();

        IntPtr metadata = GetGroupMetadataHandleOrThrow();
        try
        {
            return Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(metadata));
        }
        finally
        {
            NativeMethods.ConsumerGroupMetadataDestroy(metadata);
        }
    }

    /// <summary>
    /// Reads the full consumer group metadata (Java <c>groupMetadata()</c>) — a
    /// <b>synchronous state read</b>. Marshals the owned (Category-3) metadata handle
    /// into a public <see cref="ConsumerGroupMetadata"/> (all four fields) and frees the
    /// handle exactly once (§B2/§B3 via <see cref="ConsumerGroupMetadataMarshal"/>).
    /// </summary>
    /// <remarks>
    /// <b>Concurrency (single-owner).</b> If the core's own access guard rejects
    /// concurrent access it returns a <b>null</b> metadata handle; this maps to
    /// <see cref="InvalidOperationException"/> ("KafkaConsumer is not safe for
    /// multi-threaded access."), mirroring the Python sibling's <c>None → RuntimeError</c>
    /// and the CLAUDE.md §3 idiom map (concurrent sync state read →
    /// <see cref="InvalidOperationException"/>). The check-then-use handle TOCTOU vs
    /// teardown that used to be listed here as an accepted residual is <b>closed</b>
    /// (M9/P4 H1) — see <see cref="GroupId"/>.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for
    /// multi-threaded access).
    /// </exception>
    internal ConsumerGroupMetadata GroupMetadata()
    {
        ThrowIfClosed();

        IntPtr metadata = GetGroupMetadataHandleOrThrow();

        // The marshaller reads all four fields and frees the handle exactly once in its
        // own finally (even if a read throws).
        return ConsumerGroupMetadataMarshal.CopyOutAndDestroy(metadata);
    }

    /// <summary>
    /// Returns a point-in-time snapshot of the consumer's metrics (Java
    /// <c>Map&lt;MetricName, ? extends Metric&gt; metrics()</c>) — a <b>synchronous state
    /// read</b>. Marshals an owned (Category-3) <c>MetricMap_t</c> borrow-root into an
    /// owned <see cref="IReadOnlyDictionary{MetricName, IMetric}"/> and frees the root
    /// exactly once (§B2/§B3 via <see cref="MetricMapMarshal"/>).
    /// </summary>
    /// <remarks>
    /// <b>Concurrency (single-owner).</b> If the core's own access guard rejects concurrent
    /// access it returns a <b>null</b> map handle; this maps to
    /// <see cref="InvalidOperationException"/> ("KafkaConsumer is not safe for
    /// multi-threaded access.") via the shared <see cref="ThrowIfConcurrentNull"/> — the
    /// exact shipped <see cref="GroupMetadata"/> / <see cref="Assignment"/> mapping (ffi
    /// §B5, CLAUDE.md §3), matching the Python sibling's <c>None → RuntimeError</c>. A
    /// <c>MockConsumer</c> returns an <b>empty</b> map broker-free (Java parity). The
    /// check-then-use handle TOCTOU vs teardown is <b>closed</b> (M9/P4 H1c) — this call
    /// passes the <see cref="SafeConsumerHandle"/> as the P/Invoke parameter, so the
    /// marshaller holds a call-scoped reference for the duration of the native call and a
    /// concurrent <c>Consumer_destroy</c> cannot free the handle underneath it. It is
    /// <b>no longer</b> an accepted residual; see <see cref="Wakeup"/>.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for multi-threaded
    /// access).
    /// </exception>
    internal IReadOnlyDictionary<MetricName, IMetric> Metrics()
    {
        ThrowIfClosed();

        IntPtr map = ThrowIfConcurrentNull(NativeMethods.ConsumerMetrics(_handle));

        // The marshaller copies every entry out then frees the root exactly once in its
        // own finally (even if a read throws).
        return MetricMapMarshal.CopyOutAndDestroy(map);
    }

    /// <summary>
    /// Returns the consumer's client id (Python <c>client_id()</c>) — a <b>synchronous
    /// state read</b>. The ABI returns an <b>owned</b> NUL-terminated <c>char*</c>; this
    /// copies it out then frees it with <c>string_destroy</c> exactly once (§B2/§B3).
    /// </summary>
    /// <remarks>
    /// <b>Beyond-Java + stricter-than-Python (recorded deviations, PLAN D4/D5).</b> Java's
    /// <c>clientId()</c> is package-private on <c>KafkaConsumer</c> (not on the
    /// <c>Consumer</c> interface), so exposing it is a deliberate <b>Python-parity addition
    /// beyond the Java shape</b>. The client id is always known, so a <b>null</b> return can
    /// only mean the core rejected concurrent access — mapped to
    /// <see cref="InvalidOperationException"/> ("KafkaConsumer is not safe for
    /// multi-threaded access.") via the shared <see cref="ThrowIfConcurrentNull"/>. This is
    /// deliberately <b>stricter than Python's unguarded <c>client_id()</c></b> (which would
    /// return <c>None</c>): the non-nullable <see cref="string"/> return contract is
    /// preserved. The check-then-use handle TOCTOU vs teardown is <b>closed</b> (M9/P4 H1c)
    /// — this call passes the <see cref="SafeConsumerHandle"/> as the P/Invoke parameter, so
    /// the marshaller holds a call-scoped reference for the duration of the native call. It
    /// is <b>no longer</b> an accepted residual; see <see cref="Wakeup"/>.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for multi-threaded
    /// access).
    /// </exception>
    internal string ClientId()
    {
        ThrowIfClosed();

        // Owned char* (Category-3): null == concurrent-access rejection → InvalidOperation
        // (the shared sync-read mapping); a real client id is always known, so null never
        // means "absent".
        IntPtr raw = ThrowIfConcurrentNull(NativeMethods.ConsumerClientId(_handle));
        try
        {
            // Copy out BEFORE string_destroy — the pointer dies with the free (§B3). A
            // non-null owned char* is always a valid (possibly empty) string, never null.
            return Utf8Marshal.PtrToString(raw) ?? string.Empty;
        }
        finally
        {
            NativeMethods.ConsumerStringDestroy(raw);
        }
    }

    /// <summary>
    /// Fetches the owned group-metadata handle, mapping the core's concurrent-access
    /// rejection (a null handle) to <see cref="InvalidOperationException"/> (ffi §B5,
    /// CLAUDE.md §3). Shared by <see cref="GroupId"/> and <see cref="GroupMetadata"/>.
    /// The caller owns the returned non-null handle and must destroy it exactly once.
    /// </summary>
    private IntPtr GetGroupMetadataHandleOrThrow()
    {
        return ThrowIfConcurrentNull(NativeMethods.ConsumerGroupMetadata(_handle));
    }

    /// <summary>
    /// Returns the current assignment (Java <c>assignment()</c>) — a <b>synchronous state
    /// read</b>. Marshals an owned (Category-3) <c>TopicPartitionList_t</c> borrow-root
    /// into an owned <see cref="TopicPartition"/> snapshot and frees the root exactly once
    /// (§B2/§B3 via <see cref="TopicPartitionListMarshal"/>).
    /// </summary>
    /// <remarks>
    /// <b>Concurrency (single-owner).</b> If the core's own access guard rejects concurrent
    /// access it returns a <b>null</b> list handle; this maps to
    /// <see cref="InvalidOperationException"/> ("KafkaConsumer is not safe for
    /// multi-threaded access.") via <see cref="ThrowIfConcurrentNull"/> — the exact shipped
    /// <see cref="GroupMetadata"/> mapping (ffi §B5, CLAUDE.md §3).
    /// <para>
    /// <b>The check-then-use handle TOCTOU vs teardown is closed (M9/P4 H1c)</b>, as it is for
    /// <see cref="Wakeup"/> / <see cref="GroupMetadata"/>: this call passes the
    /// <see cref="SafeConsumerHandle"/> as the P/Invoke parameter, so the marshaller holds a
    /// call-scoped reference for the duration of the native call and a concurrent
    /// <c>Consumer_destroy</c> cannot free the handle mid-call. It is <b>no longer</b> an
    /// accepted residual — the only residual left on this type is the deferred destroy
    /// documented on <see cref="Dispose"/>. Do not re-file it.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for multi-threaded
    /// access).
    /// </exception>
    internal IReadOnlyCollection<TopicPartition> Assignment()
    {
        ThrowIfClosed();

        IntPtr list = ThrowIfConcurrentNull(NativeMethods.ConsumerAssignment(_handle));

        // The marshaller copies every element out then frees the root exactly once in its
        // own finally (even if a read throws).
        return TopicPartitionListMarshal.CopyOutAndDestroy(list);
    }

    /// <summary>
    /// Returns the current topic subscription (Java <c>subscription()</c>) — a
    /// <b>synchronous state read</b>. Marshals an owned (Category-3) <c>StringList_t</c>
    /// borrow-root into an owned <see cref="string"/> snapshot and frees the root exactly
    /// once (§B2/§B3 via <see cref="StringListMarshal"/>).
    /// </summary>
    /// <remarks>
    /// Same concurrency contract as <see cref="Assignment"/> (null handle →
    /// <see cref="InvalidOperationException"/>), including its <b>closed</b> handle TOCTOU:
    /// this call passes the <see cref="SafeConsumerHandle"/> (M9/P4 H1c), so there is no
    /// accepted residual here.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for multi-threaded
    /// access).
    /// </exception>
    internal IReadOnlyCollection<string> Subscription()
    {
        ThrowIfClosed();

        IntPtr list = ThrowIfConcurrentNull(NativeMethods.ConsumerSubscription(_handle));
        return StringListMarshal.CopyOutAndDestroy(list);
    }

    /// <summary>
    /// Returns the currently paused partitions (Java <c>paused()</c>) — a <b>synchronous
    /// state read</b>. Marshals an owned (Category-3) <c>TopicPartitionList_t</c>
    /// borrow-root into an owned <see cref="TopicPartition"/> snapshot and frees the root
    /// exactly once (§B2/§B3 via <see cref="TopicPartitionListMarshal"/>).
    /// </summary>
    /// <remarks>
    /// Same concurrency contract as <see cref="Assignment"/>, including its <b>closed</b>
    /// handle TOCTOU: this call passes the <see cref="SafeConsumerHandle"/> (M9/P4 H1c), so
    /// there is no accepted residual here. A
    /// <b>non-empty</b> result is now reachable broker-free via the public <c>Pause</c>
    /// (M5/P3): <c>Pause</c> a partition, then <c>Paused()</c> returns it (previously the
    /// mock's <c>paused()</c> could only be empty until a public <c>Pause</c> landed).
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for multi-threaded
    /// access).
    /// </exception>
    internal IReadOnlyCollection<TopicPartition> Paused()
    {
        ThrowIfClosed();

        IntPtr list = ThrowIfConcurrentNull(NativeMethods.ConsumerPaused(_handle));
        return TopicPartitionListMarshal.CopyOutAndDestroy(list);
    }

    /// <summary>
    /// Triggers a rebalance (Java <c>enforceRebalance()</c> / <c>enforceRebalance(String)</c>
    /// collapsed to one method with an optional <paramref name="reason"/>). A
    /// <b>synchronous</b> non-blocking action (CLAUDE.md §4 "stays sync").
    /// </summary>
    /// <remarks>
    /// <b>KIP-848 logged no-op (returns success, never throws a
    /// <see cref="KafkaException"/> on this path).</b> Java's
    /// <c>AsyncKafkaConsumer.enforceRebalance</c> is a pure logged no-op that throws
    /// nothing, and the Rust core's <c>enforce_rebalance</c> returns <c>Ok(())</c> — so the
    /// ABI returns a null error handle under the current group protocol. The uniform
    /// sync-op error discipline (<see cref="KafkaException.FromHandle(IntPtr)"/>, throw iff
    /// non-null; ffi §B5) is still applied because a future classic-protocol arm could
    /// return a real error here without a .NET change; under KIP-848 the handle is always
    /// null, so this is observably a no-op that returns normally.
    /// <para>
    /// ⚠ The header/Rust-FFI doc comment on <c>enforce_rebalance</c> claims it "returns an
    /// unsupported-version error"; that comment is <b>stale</b> — the code it wraps returns
    /// <c>Ok(())</c> and Java throws nothing. The mapping follows the actual behavior
    /// (no-op success), not the stale comment. (A one-line Rust-core doc fix is a separate
    /// dependency, out of scope for the C#-only binding.)
    /// </para>
    /// <paramref name="reason"/> is pinned call-scoped when non-null; a
    /// <see langword="null"/> maps to <see cref="IntPtr.Zero"/> (the ABI accepts a null
    /// reason).
    /// </remarks>
    /// <param name="reason">An optional human-readable reason, or <see langword="null"/>.</param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The core reported a rebalance failure (not reachable under the current KIP-848
    /// no-op; reserved for a future protocol arm).
    /// </exception>
    internal void EnforceRebalance(string? reason)
    {
        ThrowIfClosed();

        IntPtr error;
        if (reason is null)
        {
            error = NativeMethods.ConsumerEnforceRebalance(_handle, IntPtr.Zero);
        }
        else
        {
            using Utf8Marshal.PinnedUtf8String reasonPin = Utf8Marshal.Pin(reason);
            error = NativeMethods.ConsumerEnforceRebalance(_handle, reasonPin.Pointer);
        }

        // Uniform sync-op discipline (ffi §B5): FromHandle frees the handle and returns
        // null on success (the KIP-848 no-op path). Throw only for a non-null error.
        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Maps the core's concurrent-access rejection (a null owned-result handle) to
    /// <see cref="InvalidOperationException"/> (ffi §B5, CLAUDE.md §3), shared by every
    /// synchronous state read (<see cref="GroupId"/> / <see cref="GroupMetadata"/> /
    /// <see cref="Assignment"/> / <see cref="Subscription"/> / <see cref="Paused"/>). The
    /// caller owns the returned non-null handle and must destroy it exactly once. This is
    /// the one concurrency contract for all sync reads — do not introduce a new one.
    /// </summary>
    private static IntPtr ThrowIfConcurrentNull(IntPtr handle)
    {
        if (handle == IntPtr.Zero)
        {
            // The core's own access guard rejected concurrent access (null handle).
            // Surface it the Python way: a concurrent sync state read is an
            // InvalidOperationException, not a silent null (ffi §B5, CLAUDE.md §3).
            throw new InvalidOperationException(
                "KafkaConsumer is not safe for multi-threaded access.");
        }

        return handle;
    }

    /// <summary>
    /// Graceful synchronous teardown (blocking fallback): <c>Consumer_close_with_timeout</c>
    /// then releases the handle (→ <c>Consumer_destroy</c>). Idempotent and safe under
    /// concurrent / double calls (the atomic closed flag). The async teardown
    /// (<see cref="DisposeAsync"/>) is the primary path.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Deterministic native release requires that no operation is in flight — await
    /// your operations before disposing.</b> Releasing the
    /// <see cref="SafeConsumerHandle"/> runs <c>Consumer_destroy</c> only when the
    /// handle's internal reference count reaches zero, and every operation that touches
    /// the consumer holds a count while it does so: an <b>async</b> op holds one for its
    /// whole duration (the span-the-op <c>DangerousAddRef</c>, released in
    /// <c>FreeGcHandle</c>), and a <b>sync</b> call holds one for the duration of the
    /// native call (the <c>SafeHandle</c>-as-parameter marshaller AddRef, M9/P4 H1,
    /// ffi §A2). On the clean path — <c>await</c> (or return from) the operation, then
    /// dispose — the count is 1 at <see cref="Dispose"/> and the native release is
    /// immediate and deterministic. See <b>Deferred destroy</b> below for the other case.
    /// </para>
    /// <para>
    /// <b>Single-owner: no separate-op drain.</b> Under the not-thread-safe contract
    /// the awaiter of an op is its disposer, so there is no concurrent submitter for
    /// teardown to drain — <see cref="Dispose"/> simply closes gracefully then
    /// destroys, matching the Python sibling (drain its <em>own</em> awaited op, then
    /// bare <c>_destroy</c>). The M3/P1 fault machinery (<c>FaultTaskOnly</c>) is
    /// deliberately not re-added; <see cref="DisposeAsync"/> on the awaiting task is
    /// the clean path.
    /// </para>
    /// <para>
    /// <b>Deferred destroy — ACCEPTED (M9/P4 decision Q1). Not a defect, not parked
    /// work.</b> Submit an operation, do <em>not</em> await it, then dispose: the
    /// operation still holds a handle reference, so <see cref="Dispose"/> attempts the
    /// graceful close (which the core's one-op-at-a-time guard rejects, since the
    /// in-flight op holds the slot), swallows that error, and takes the reference count
    /// from 2 to 1 — <b>destroying nothing</b>. The native consumer, its tokio runtime,
    /// its <c>ConsumerNetworkThread</c>, its dispatcher thread and its sockets stay
    /// alive until the operation finishes; the operation's completion then drops the
    /// last reference and <c>Consumer_destroy</c> runs from there. Three properties make
    /// this acceptable rather than a leak:
    /// </para>
    /// <list type="number">
    /// <item>
    /// <b>The retention is bounded, not unbounded in time.</b> The window is bounded by
    /// the in-flight operation's own completion, and for the poll family that is a
    /// <em>caller-supplied</em> timeout — <c>Poll(5 minutes)</c> retaining the consumer
    /// for at most 5 minutes is "as long as the operation the caller started".
    /// </item>
    /// <item>
    /// <b>It triggers only on the documented-misuse path</b> (submit, do not await,
    /// dispose). The documented path — await, then dispose — releases deterministically.
    /// </item>
    /// <item>
    /// <b>The relocated destroy is safe by construction</b> (next paragraph), so the
    /// only consequence is the timing.
    /// </item>
    /// </list>
    /// <para>
    /// The alternative — forcing the destroy while an operation is in flight — <em>is</em>
    /// the pre-<c>073252f3</c> use-after-free, so it is not available; and the only safe
    /// way to force it would be a Rust-core cancel-then-join on <c>Consumer_destroy</c>,
    /// which is <b>Mode B and explicitly not pursued and not tracked</b> (decision Q1).
    /// </para>
    /// <para>
    /// <b><c>Consumer_destroy</c> on the core's own dispatcher thread is safe by
    /// construction.</b> When the deferred release happens on an operation's completion,
    /// destroy runs on the Rust core's foreign dispatcher thread rather than the caller's
    /// teardown thread. That is a shape ffi §B2 does not contemplate, and it is safe on
    /// three citable grounds:
    /// </para>
    /// <list type="number">
    /// <item>
    /// <b>No self-join.</b> <c>Consumer_destroy</c> explicitly does <em>not</em> join the
    /// dispatcher — <c>src/ffi/consumer.rs:518-522</c> drops the <c>JoinHandle</c> with
    /// the comment "detach the dispatcher (do NOT join — outstanding completion jobs may
    /// still hold a cloned <c>completion_tx</c>, and the dispatcher exits once all clones
    /// are released)". Dropping a <c>JoinHandle</c> for the current thread is a no-op
    /// detach.
    /// </item>
    /// <item>
    /// <b>No producer can block.</b> The completion queue is an <b>unbounded</b>
    /// <c>std::sync::mpsc::channel</c> (<c>src/ffi/common.rs:224</c>), so nothing the
    /// internal bg task does while <c>drop(consumer)</c> joins it can block on the
    /// dispatcher; <c>runtime.shutdown_background()</c> (<c>consumer.rs:515</c>) is
    /// non-blocking by definition.
    /// </item>
    /// <item>
    /// <b>No use-after-free of the dispatcher's own state.</b> <c>completion_rx</c> is
    /// moved into the dispatcher closure (<c>src/ffi/common.rs:224-231</c>), separate from
    /// the <c>FfiConsumerHandle</c> box being freed; after the box is gone the
    /// dispatcher's <c>while let Ok(job) = completion_rx.recv()</c> still holds a valid
    /// receiver and exits cleanly once the running job returns and releases the last
    /// sender clone.
    /// </item>
    /// </list>
    /// <para>
    /// <b>The deferred destroy is BARE (no preceding graceful close) — ACCEPTED
    /// PERMANENTLY (M9/P4 decision Q3). No follow-up item is filed, scheduled or
    /// tracked.</b> In the race above the graceful close is rejected and swallowed, so
    /// the eventual deferred <c>Consumer_destroy</c> has no graceful close in front of
    /// it. That reads like a violation of the close-before-destroy ordering (ffi §B2),
    /// and it is deliberately carved out of it. The full argument, in five points,
    /// because with no tracked item this comment is the only place it exists:
    /// </para>
    /// <list type="number">
    /// <item>
    /// <b>It is not new, and not caused by M9/P4.</b> It is the pre-existing consequence
    /// of the core's one-op-at-a-time guard on the unawaited-op path — the same mechanism
    /// the async scenario above describes, which predates <c>073252f3</c>. H1 widens its
    /// reach to the synchronous surface; it does not create it.
    /// </item>
    /// <item>
    /// <b>It is reachable only on the documented-misuse path</b> (submit, do not await,
    /// dispose). On the clean path the reference count is 1 at <see cref="Dispose"/>, the
    /// graceful close succeeds, and close-before-destroy holds exactly as ffi §B2 states.
    /// </item>
    /// <item>
    /// <b>The alternative is strictly worse.</b> The only ways to guarantee a graceful
    /// close here are to <em>block</em> teardown until the in-flight op finishes (turning
    /// <see cref="Dispose"/> into an unbounded wait on an operation the caller abandoned)
    /// or to destroy underneath the live op (the use-after-free <c>073252f3</c> fixed).
    /// Deferring a bare destroy is the least-bad of the three.
    /// </item>
    /// <item>
    /// <b>What is lost is bounded and small.</b> A bare <c>Consumer_destroy</c> skips the
    /// graceful bg-task join, but it still frees every native resource:
    /// <c>runtime.shutdown_background()</c>, <c>drop(consumer)</c> (which joins the
    /// <em>internal</em> bg task) and the dispatcher detach all run
    /// (<c>src/ffi/consumer.rs:512-522</c>). The loss is the graceful
    /// leave-group / commit-on-close courtesy, on a path where the caller already
    /// abandoned an operation.
    /// </item>
    /// <item>
    /// <b>A core-side close-then-destroy on the deferred path would be the theoretical
    /// clean fix — and it is explicitly NOT being pursued and NOT tracked</b> (decision
    /// Q3). It is named here only so a reader understands the shape of what is given up,
    /// not as a hint of pending work. Do not file it, schedule it, or add it to a
    /// candidate-hardening list.
    /// </item>
    /// </list>
    /// </remarks>
    public void Dispose()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            // Graceful close first (ffi §B2/§B7): destroy alone is fire-and-forget.
            // The handle is not released until after this returns, so its raw value
            // is valid here. Dispose consumes the close error (freeing the handle
            // via FromHandle) but does NOT rethrow — Dispose must not throw, and a
            // best-effort teardown has no caller to hand a failure to.
            //
            // ⚠ DELIBERATELY NOT the SafeHandle-param form (M9/P4 decision Q2, plan §3.4) —
            // do NOT "finish the job" here. Two reasons, both load-bearing: (1) the latch is
            // already won and the finally below releases the handle on this thread in program
            // order, so no concurrent destroy can race this call (invariant I2); and (2) the
            // marshaller would throw ObjectDisposedException from inside this try, and the
            // finally does not swallow — so it would propagate out of Dispose, violating the
            // .NET Dispose must-not-throw contract.
            IntPtr error = NativeMethods.ConsumerCloseWithTimeout(
                _handle.DangerousGetHandle(),
                DefaultCloseTimeoutMilliseconds);
            _ = KafkaException.FromHandle(error);
        }
        finally
        {
            // ReleaseHandle → Consumer_destroy, exactly once.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Graceful <b>async</b> teardown (the primary path): <c>Consumer_close_async</c>
    /// (joins the background task via the completion bridge), then release the handle
    /// (→ <c>Consumer_destroy</c>). Idempotent and safe under concurrent / double
    /// calls (the atomic closed flag). Best-effort: it swallows the close error
    /// (surfacing it is <see cref="CloseWithCallback"/> / the public <c>Close()</c>'s job).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Deterministic native release requires that no operation is in flight — await
    /// your operations before disposing.</b> Same reference-counting rule as
    /// <see cref="Dispose"/>: the handle release performs <c>Consumer_destroy</c> only at
    /// count zero, and an unawaited in-flight operation still holds a count. Awaiting the
    /// operation and <em>then</em> calling <see cref="DisposeAsync"/> is the clean,
    /// deterministic path.
    /// </para>
    /// <para>
    /// <b>Single-owner: no separate-op drain.</b> Under the not-thread-safe contract
    /// the awaiter of an op is its disposer, so <see cref="DisposeAsync"/> does not
    /// wake+await a <em>separately-submitted</em> in-flight op — there is no concurrent
    /// submitter to drain. It closes gracefully (<c>close_async</c> joins the bg task)
    /// then destroys, matching the Python sibling's <c>close()</c>.
    /// </para>
    /// <para>
    /// <b>Deferred destroy + bare deferred destroy: accepted (decisions Q1 / Q3).</b>
    /// The unawaited-op teardown race behaves identically here, including the
    /// dispatcher-thread destroy and the bare (close-less) deferred destroy. The full
    /// argument — the bounded retention, the three safe-by-construction citations, and
    /// the five-point permanent acceptance of the bare destroy with <b>no follow-up item
    /// filed or tracked</b> — is documented once, on <see cref="Dispose"/>. Do not
    /// re-derive or re-file it.
    /// </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            // Graceful async close (joins the bg task) via the void bridge. Under
            // single-owner there is nothing to drain first: the awaiter of any op is
            // this disposer, so the core guard is free for the close op.
            await CloseWithCallbackInternal().ConfigureAwait(false);
        }
        catch (KafkaException)
        {
            // Best-effort teardown — Dispose/DisposeAsync must not surface a close
            // error; that is the public Close()'s job.
        }
        finally
        {
            // ReleaseHandle → Consumer_destroy, exactly once.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Graceful <b>async</b> close (Java <c>close()</c>) that <b>surfaces</b> the close
    /// error — the public <c>Close()</c>'s worker. Takes the one-shot
    /// <see cref="TryBeginClose"/> latch, closes via <see cref="CloseWithCallbackInternal"/>
    /// (<c>close_async</c>, joining the background task), then releases the handle
    /// (→ <c>Consumer_destroy</c>) in a <c>finally</c> — destroy runs exactly once even
    /// on a close error. A subsequent <see cref="Dispose"/> / <see cref="DisposeAsync"/>
    /// loses the latch and no-ops (closed-flag idempotence). Calling this after a
    /// teardown that already won the latch is a no-op (returns without closing again).
    /// </summary>
    /// <remarks>
    /// <b>No timeout (ABI-verified, PLAN decision 6).</b> The ABI has no async close
    /// with a timeout — <c>Consumer_close_async</c> takes only a callback; the only
    /// timeout-accepting close is the sync <c>Consumer_close_with_timeout</c>. So this
    /// takes only a <see cref="CancellationToken"/>; a faithful <c>Close(TimeSpan)</c>
    /// is deferred to an additive overload once a Rust-core <c>close_async_with_timeout</c>
    /// exists (Mode-B, out of scope). Unlike <see cref="DisposeAsync"/> (which swallows
    /// the close error), this <b>throws</b> it — <c>close()</c> reports failures.
    /// </remarks>
    /// <param name="cancellationToken">
    /// Cancellation is observed only before the close is submitted (a canceled token
    /// throws <see cref="OperationCanceledException"/> before taking the latch); once
    /// close is in flight the graceful join runs to completion.
    /// </param>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal async ValueTask CloseWithCallback(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!TryBeginClose())
        {
            // A prior teardown already won the latch — closing again would double-close
            // / double-destroy. No-op, matching the idempotent teardown contract.
            return;
        }

        try
        {
            // Graceful async close (joins the bg task); surface any close error (unlike
            // DisposeAsync). Under single-owner there is nothing to drain first.
            await CloseWithCallbackInternal().ConfigureAwait(false);
        }
        finally
        {
            // ReleaseHandle → Consumer_destroy, exactly once — even if the close threw.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Bridges <c>Consumer_close_async</c> to a <see cref="Task"/> via the shared
    /// completion callback. If the submitting P/Invoke throws before native could
    /// fire the callback, the context is abandoned (its <c>GCHandle</c> freed) here.
    /// </summary>
    private Task CloseWithCallbackInternal()
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count, INSIDE the try so a DangerousAddRef throw routes through
            // AbandonBeforeSubmit (M9/P4 M3 — see SubmitVoidOperation for the full rationale and
            // the invariant-I1 argument). Holds a reference on the consumer SafeHandle for the
            // whole async op, so ReleaseHandle → Consumer_destroy cannot run until the op's
            // completion callback releases it in FreeGcHandle (ffi §B2/§B7). Here the AddRef
            // cannot realistically throw — this runs only after TryBeginClose has been won, so no
            // other path can have released the handle — but the shape is kept identical to the
            // other four helpers: a divergent shape across the family is itself a defect.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            NativeMethods.ConsumerCloseAsync(
                _handle.DangerousGetHandle(), ConsumerCallbacks.Operation, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Submits a void-result async op: root the per-op context via a
    /// <see cref="GCHandle"/> (invariant #1), wire cancellation, then run
    /// <paramref name="submit"/> (which pins its args call-scoped and P/Invokes).
    /// Ownership of the <see cref="GCHandle"/> transfers to the completion callback
    /// (the sole owner of its free, invariant #2) the moment native is entered; if
    /// <paramref name="submit"/> throws before that, the context is abandoned (handle
    /// freed) here.
    /// </summary>
    /// <remarks>
    /// The consumer is single-owner: op serialization is the <b>Rust core's</b> job,
    /// not a managed guard. A concurrent second op is rejected by the core inline
    /// (it fires the callback on the caller thread with a ConcurrentModification
    /// error), which the bridge surfaces as a faulted <see cref="Task"/> (ffi §B5) —
    /// so there is no managed pre-check to throw here, and no in-flight tracking to
    /// publish (which is why the M3/P1 publish window no longer exists).
    /// </remarks>
    private Task SubmitVoidOperation(CancellationToken cancellationToken, NativeSubmit submit)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count: hold a reference on the consumer SafeHandle for the whole
            // async op so ReleaseHandle → Consumer_destroy cannot run until the op's completion
            // callback releases it (in FreeGcHandle). Closes the destroy-vs-in-flight-op
            // use-after-free (ffi §B2/§B7) by deferring the guardless native destroy past the op.
            //
            // INSIDE the try (M9/P4 M3) — this is the canonical site; the other four submit
            // helpers mirror it. DangerousAddRef throws ObjectDisposedException when a concurrent
            // teardown closed the handle between ThrowIfClosed above and here. That is a "native
            // never ran" path, so it MUST route through AbandonBeforeSubmit — otherwise the
            // GCHandle allocated two lines up is rooted for the process lifetime, silently
            // leaking the context (and, on the poll path, the two deserializers travelling on
            // it) behind a perfectly plausible ObjectDisposedException (ffi §B6/§B7).
            //
            // Safe with respect to invariant I1 ("the completion callback is the sole owner of
            // the GCHandle free"): AbandonBeforeSubmit → FreeGcHandle is Interlocked-guarded and
            // ALSO does _handleRef?.DangerousRelease(), so it releases the reference when the
            // AddRef succeeded and submit then threw, and releases nothing when the AddRef itself
            // threw (SetHandleRef never ran). No new free site is introduced — this only widens
            // the reachability of the existing one, and AbandonBeforeSubmit is still reachable
            // only when native never ran, so the callback cannot fire for this op.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            context.RegisterCancellation(cancellationToken, Wakeup);
            submit(_handle.DangerousGetHandle(), ConsumerCallbacks.Operation, GCHandle.ToIntPtr(gcHandle));
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
    /// Submits one of the five void ops on a partition collection (M5/P3): validate the
    /// collection preconditions (§B5) BEFORE any pin/P-Invoke, then run the shared void
    /// bridge (<see cref="SubmitVoidOperation"/> + <see cref="ConsumerCallbacks.Operation"/>
    /// — reused verbatim, NO new bridge/callback this phase). The submit lambda marshals the
    /// collection into the ABI's parallel <c>(topics[], partitions[], count)</c> arrays via
    /// the shared <see cref="WithPinnedTopics"/>, pinned <b>call-scoped</b> (freed at submit
    /// return — the core copies during the call, ffi §A4/§B4), and P/Invokes
    /// <paramref name="submit"/>. An <b>empty</b> collection passes <c>count == 0</c> through
    /// (a clear for <c>assign</c>, a no-op for the others) — never a spurious throw (§B5).
    /// </summary>
    private Task SubmitPartitionOp(
        IReadOnlyCollection<TopicPartition> partitions,
        CancellationToken cancellationToken,
        NativePartitionOpSubmit submit)
    {
        // Preconditions BEFORE any pin / P-Invoke (ffi §B5): the ABI does not validate them
        // and panics/mismaps on violation (a negative partition is silently mapped to
        // "unset"). A null collection is rejected; an EMPTY collection is valid (Java: NPE
        // on null vs no-op / clear on empty), so we do NOT reject empty. SnapshotPartitions
        // is the shared validate-and-snapshot reused by the M5/P4 collection-input queries
        // (PLAN §3 — the validation is not copy-pasted).
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        int count = snapshot.Length;
        int[] partitionArray = ExtractPartitions(snapshot);

        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
            WithPinnedTopics(count, i => snapshot[i].Topic, partitionArray, (pointers, parts, cnt) =>
                submit(consumer, pointers, parts, cnt, callback, userData)));
    }

    /// <summary>
    /// Submits an owned-handle <b>typed poll</b> async op (PLAN M6/P1b §5) — the typed
    /// analog of <see cref="SubmitVoidOperation"/>. Builds a
    /// <see cref="TypedPollCompletionSource{TKey, TValue}"/> carrying the two deserializers,
    /// roots it via a <see cref="GCHandle"/> (invariant #1), wires cancellation, then runs
    /// <paramref name="submit"/> with this closed generic type's rooted <c>Poll</c> callback
    /// (<see cref="TypedPollCallbacks{TKey, TValue}"/>). Ownership of the
    /// <see cref="GCHandle"/> transfers to the completion callback (the sole owner of its
    /// free, invariant #2) the moment native is entered; if <paramref name="submit"/> throws
    /// before that, the context is abandoned (handle freed) here. The deserialize + copy-out
    /// of the result happens in the callback on the dispatcher thread (ffi §6.4), not here —
    /// the serdes travel to it on the <see cref="TypedPollCompletionSource{TKey, TValue}"/>.
    /// </summary>
    private Task<ConsumerRecords<TKey, TValue>> SubmitTypedPollOperation<TKey, TValue>(
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        CancellationToken cancellationToken,
        NativeResultSubmit submit)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        TypedPollCompletionSource<TKey, TValue> context =
            new TypedPollCompletionSource<TKey, TValue>(keyDeserializer, valueDeserializer);
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count, INSIDE the try so a DangerousAddRef throw routes through
            // AbandonBeforeSubmit (M9/P4 M3 — see SubmitVoidOperation for the full rationale and
            // the invariant-I1 argument). Holds a reference on the consumer SafeHandle for the
            // whole async op, so ReleaseHandle → Consumer_destroy cannot run until the op's
            // completion callback releases it in FreeGcHandle (ffi §B2/§B7). On this path the
            // leak would also root the two deserializers travelling on the context.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            context.RegisterCancellation(cancellationToken, Wakeup);
            submit(_handle.DangerousGetHandle(), TypedPollCallbacks<TKey, TValue>.Poll, GCHandle.ToIntPtr(gcHandle));
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
    /// Submits a <b>scalar</b> (result-in-callback) async op — the <c>position</c> analog
    /// of <see cref="SubmitTypedPollOperation{TKey, TValue}"/> (M5/P2). Roots the per-op context via a
    /// <see cref="GCHandle"/> (invariant #1), wires cancellation, then runs
    /// <paramref name="submit"/> (which pins its args call-scoped and P/Invokes with the
    /// scalar callback). Ownership of the <see cref="GCHandle"/> transfers to the
    /// completion callback (the sole owner of its free, invariant #2) the moment native is
    /// entered; if <paramref name="submit"/> throws before that, the context is abandoned
    /// (handle freed) here. A line-for-line clone of <see cref="SubmitTypedPollOperation{TKey, TValue}"/>
    /// with the typed poll callback → <see cref="ConsumerCallbacks.Position"/>
    /// — added as a parallel helper (rather than generalizing
    /// <see cref="SubmitTypedPollOperation{TKey, TValue}"/> to take the callback type as a parameter) so
    /// the proven poll / void submit paths are left byte-for-byte untouched (PLAN §1.3).
    /// The scalar result needs no marshalling in the callback (it is blittable), unlike the
    /// owned-handle path's copy-out.
    /// </summary>
    private Task<TResult> SubmitScalarOperation<TResult>(
        CancellationToken cancellationToken,
        NativeScalarSubmit submit)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        OperationCompletionSource<TResult> context = new OperationCompletionSource<TResult>();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op ref-count, INSIDE the try so a DangerousAddRef throw routes through
            // AbandonBeforeSubmit (M9/P4 M3 — see SubmitVoidOperation for the full rationale and
            // the invariant-I1 argument). Holds a reference on the consumer SafeHandle for the
            // whole async op, so ReleaseHandle → Consumer_destroy cannot run until the op's
            // completion callback releases it in FreeGcHandle (ffi §B2/§B7).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            context.RegisterCancellation(cancellationToken, Wakeup);
            submit(_handle.DangerousGetHandle(), ConsumerCallbacks.Position, GCHandle.ToIntPtr(gcHandle));
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
    /// Submits an owned-handle offset-map async op — the M5/P4 analog of
    /// <see cref="SubmitTypedPollOperation{TKey, TValue}"/> for the four offset-map queries. Roots the
    /// per-op context via a <see cref="GCHandle"/> (invariant #1), wires cancellation,
    /// then runs <paramref name="submit"/>, which pins its input arrays call-scoped and
    /// P/Invokes the correct <c>_async</c> fn <b>with its own strongly-typed rooted
    /// callback captured at the call site</b> (so this helper stays callback-type-agnostic
    /// and passes only <c>(consumer, userData)</c>). Ownership of the <see cref="GCHandle"/>
    /// transfers to the completion callback (the sole owner of its free, invariant #2) the
    /// moment native is entered; if <paramref name="submit"/> throws before that, the
    /// context is abandoned (handle freed) here. The result copy-out happens in the
    /// callback on the dispatcher thread (ffi §6.4), not here. A structural clone of
    /// <see cref="SubmitTypedPollOperation{TKey, TValue}"/> — the poll / void / scalar submit paths are
    /// left byte-for-byte untouched (PLAN §4.2).
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
            // Span-the-op ref-count, INSIDE the try so a DangerousAddRef throw routes through
            // AbandonBeforeSubmit (M9/P4 M3 — see SubmitVoidOperation for the full rationale and
            // the invariant-I1 argument). Holds a reference on the consumer SafeHandle for the
            // whole async op, so ReleaseHandle → Consumer_destroy cannot run until the op's
            // completion callback releases it in FreeGcHandle (ffi §B2/§B7).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            context.RegisterCancellation(cancellationToken, Wakeup);
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
    /// Snapshots + validates a <see cref="TopicPartition"/> collection (§B5), then runs the
    /// shared sync partition-op tail (<see cref="InvokePartitionOpSync"/>) over
    /// <paramref name="submit"/> — the sync analog of <see cref="SubmitPartitionOp"/>, driving
    /// the five public sync collection ops (<see cref="Assign(IReadOnlyCollection{TopicPartition})"/>
    /// / <see cref="Pause"/> / <see cref="Resume"/> / <see cref="SeekToBeginning"/> /
    /// <see cref="SeekToEnd"/>). A null collection is rejected; an <b>empty</b> collection is a
    /// valid pass-through (<c>count == 0</c>).
    /// </summary>
    private void RunPartitionOpSync(IReadOnlyCollection<TopicPartition> partitions, NativePartitionOpSync submit)
    {
        (string Topic, int Partition)[] snapshot = SnapshotPartitions(partitions);
        InvokePartitionOpSync(snapshot.Length, i => snapshot[i].Topic, ExtractPartitions(snapshot), submit);
    }

    /// <summary>
    /// The shared sync partition-op tail: <see cref="ThrowIfClosed"/> → call-scoped topic pin
    /// (via <see cref="WithPinnedTopics"/>) → P/Invoke <paramref name="submit"/> →
    /// <see cref="KafkaException.FromHandle(IntPtr)"/> throw-iff-non-null. Reused by the five
    /// sync collection ops (through <see cref="RunPartitionOpSync"/>) and the tuple-form driver
    /// <see cref="Assign(IReadOnlyList{ValueTuple{string, int}})"/> — the native call + error
    /// handling is not duplicated (DoD §6). The blittable <c>int[]</c>
    /// <paramref name="partitions"/> is passed straight through; no per-element copy beyond the
    /// UTF-8 encode.
    /// </summary>
    private void InvokePartitionOpSync(
        int count,
        Func<int, string> topicAt,
        int[] partitions,
        NativePartitionOpSync submit)
    {
        ThrowIfClosed();

        IntPtr error = IntPtr.Zero;
        // SafeHandle-param (ffi §A2; M9/P4 H1b): the marshaller AddRefs for the whole native
        // call, so a concurrent teardown cannot free the consumer mid-op.
        WithPinnedTopics(count, topicAt, partitions, (pointers, parts, cnt) =>
            error = submit(_handle, pointers, parts, cnt));

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// The topics-only variant of <see cref="WithPinnedTopics"/> (no partitions array) — the one
    /// shared topic-list pin path for <c>subscribe</c>, used by both the sync
    /// <see cref="Subscribe(IReadOnlyCollection{string})"/> and the async
    /// <see cref="SubscribeWithCallback(IReadOnlyCollection{string}, CancellationToken)"/> (DoD §6, no duplication). Pins <paramref name="count"/>
    /// topic strings <b>call-scoped</b> (the core copies them synchronously during the call, ffi
    /// §A3/§A4 — freed the moment <paramref name="body"/> returns), fills the parallel
    /// <c>IntPtr[]</c> pointer array, runs <paramref name="body"/> with <c>(topics, count)</c>,
    /// then unpins in a <c>finally</c>. A <paramref name="count"/> of 0 runs
    /// <paramref name="body"/> with an empty array (§B5).
    /// </summary>
    private static void WithPinnedTopicsOnly(int count, Func<int, string> topicAt, Action<IntPtr[], int> body)
    {
        Utf8Marshal.PinnedUtf8String?[] pins = new Utf8Marshal.PinnedUtf8String?[count];
        IntPtr[] pointers = new IntPtr[count];
        try
        {
            for (int i = 0; i < count; i++)
            {
                Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(topicAt(i));
                pins[i] = pin;
                pointers[i] = pin.Pointer;
            }

            body(pointers, count);
        }
        finally
        {
            for (int i = 0; i < pins.Length; i++)
            {
                pins[i]?.Dispose();
            }
        }
    }

    /// <summary>
    /// The one shared collection→parallel-array marshaller for every partition op — the
    /// sync <see cref="Assign(IReadOnlyList{ValueTuple{string, int}})"/> and all five async
    /// ops (<see cref="AssignWithCallback"/> / <see cref="PauseWithCallback"/> /
    /// <see cref="ResumeWithCallback"/> / <see cref="SeekToBeginningWithCallback"/> /
    /// <see cref="SeekToEndWithCallback"/>). Pins <paramref name="count"/> topic strings
    /// <b>call-scoped</b> (ffi §A3/§A4: the core copies them synchronously during the call —
    /// verified for both <c>Consumer_assign</c> and each <c>_async</c> op's
    /// <c>read_topic_partitions</c> — so the pins are freed the moment
    /// <paramref name="body"/> returns; never held across the returned <see cref="Task"/>),
    /// fills the parallel <c>IntPtr[] topics</c> pointer array, runs
    /// <paramref name="body"/> with <c>(topics, <paramref name="partitions"/>, count)</c>,
    /// then unpins in a <c>finally</c>. <b>No per-element copy beyond the UTF-8 encode</b>
    /// (§A4/§B4): the topic bytes are pinned, not copied; the blittable <c>int[]</c>
    /// partitions are passed straight through. A <paramref name="count"/> of 0 (an empty
    /// collection) runs <paramref name="body"/> with empty arrays and <c>count == 0</c> — a
    /// valid pass-through, never a throw (§B5).
    /// </summary>
    /// <param name="count">The number of topic-partitions (may be 0).</param>
    /// <param name="topicAt">The topic string at a given index (validated non-null by the caller).</param>
    /// <param name="partitions">The blittable partition array (length <paramref name="count"/>).</param>
    /// <param name="body">The P/Invoke to run with the pinned parallel arrays.</param>
    private static void WithPinnedTopics(
        int count,
        Func<int, string> topicAt,
        int[] partitions,
        Action<IntPtr[], int[], int> body)
    {
        Utf8Marshal.PinnedUtf8String?[] pins = new Utf8Marshal.PinnedUtf8String?[count];
        IntPtr[] pointers = new IntPtr[count];
        try
        {
            for (int i = 0; i < count; i++)
            {
                Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(topicAt(i));
                pins[i] = pin;
                pointers[i] = pin.Pointer;
            }

            body(pointers, partitions, count);
        }
        finally
        {
            for (int i = 0; i < pins.Length; i++)
            {
                pins[i]?.Dispose();
            }
        }
    }

    /// <summary>
    /// The <see cref="WithPinnedTopics"/> variant for the one <b>map-input</b> query
    /// (<c>offsetsForTimes</c>): it additionally passes a blittable <c>long[]</c>
    /// <paramref name="timestamps"/> alongside the pinned topic pointers and partitions,
    /// matching the ABI's parallel <c>(topics[], partitions[], timestamps[], count)</c>.
    /// The topic strings are pinned <b>call-scoped</b> (freed at <paramref name="body"/>
    /// return — the core reads them synchronously before spawning, ffi §A4/§B4); the
    /// blittable <c>int[]</c> / <c>long[]</c> arrays are passed straight through (no
    /// per-element copy beyond the UTF-8 encode). A <paramref name="count"/> of 0 runs
    /// <paramref name="body"/> with empty arrays (§B5).
    /// </summary>
    private static void WithPinnedTopicsAndTimestamps(
        int count,
        Func<int, string> topicAt,
        int[] partitions,
        long[] timestamps,
        Action<IntPtr[], int[], long[], int> body)
    {
        Utf8Marshal.PinnedUtf8String?[] pins = new Utf8Marshal.PinnedUtf8String?[count];
        IntPtr[] pointers = new IntPtr[count];
        try
        {
            for (int i = 0; i < count; i++)
            {
                Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(topicAt(i));
                pins[i] = pin;
                pointers[i] = pin.Pointer;
            }

            body(pointers, partitions, timestamps, count);
        }
        finally
        {
            for (int i = 0; i < pins.Length; i++)
            {
                pins[i]?.Dispose();
            }
        }
    }

    /// <summary>
    /// The validated, snapshotted commit-offsets input — the five parallel arrays the ABI's
    /// <c>commit_sync_offsets_async</c> takes, produced by <see cref="SnapshotCommitOffsets"/>
    /// and consumed by <see cref="WithPinnedCommitOffsets"/>. <c>Topics</c> and
    /// <c>Metadata</c> are the two string arrays (both pinned call-scoped); <c>Partitions</c>
    /// / <c>Offsets</c> / <c>LeaderEpochs</c> are blittable and passed straight through.
    /// <c>Count</c> is the entry count (may be 0 for an empty map).
    /// </summary>
    private readonly struct CommitOffsetsSnapshot
    {
        internal CommitOffsetsSnapshot(
            string[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, string[] metadata, int count)
        {
            Topics = topics;
            Partitions = partitions;
            Offsets = offsets;
            LeaderEpochs = leaderEpochs;
            Metadata = metadata;
            Count = count;
        }

        internal string[] Topics { get; }

        internal int[] Partitions { get; }

        internal long[] Offsets { get; }

        internal int[] LeaderEpochs { get; }

        internal string[] Metadata { get; }

        internal int Count { get; }
    }

    /// <summary>
    /// Validates a commit-offsets map (§B5) and snapshots it into the five parallel arrays —
    /// the commit analog of <see cref="SnapshotPartitions"/>, validated BEFORE any pin /
    /// P-Invoke (the ABI does not validate preconditions and panics/mismaps on violation,
    /// CLAUDE.md §3). A <see langword="null"/> map is rejected; an <b>empty</b> map is valid
    /// (yields zero-length arrays, passed through as <c>Count == 0</c>). Per the Python
    /// <c>_commit_spec</c> convention (and the ABI header contract): a null
    /// <see cref="OffsetAndMetadata.LeaderEpoch"/> becomes the sentinel <c>-1</c> ("no
    /// epoch"), and <see cref="OffsetAndMetadata.Metadata"/> (never null by construction —
    /// the public ctor coerces null → <c>""</c>) is passed through, with a defensive
    /// null-coalesce to <c>""</c>. A negative offset inside an <see cref="OffsetAndMetadata"/>
    /// cannot reach here — the public ctor rejects <c>offset &lt; 0</c> at construction — so
    /// no offset re-validation is needed.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// A key topic is null, or a value (<see cref="OffsetAndMetadata"/>) is null.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    private static CommitOffsetsSnapshot SnapshotCommitOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        if (offsets is null)
        {
            throw new ArgumentNullException(nameof(offsets));
        }

        int count = offsets.Count;
        string[] topics = new string[count];
        int[] partitions = new int[count];
        long[] offsetValues = new long[count];
        int[] leaderEpochs = new int[count];
        string[] metadata = new string[count];
        int index = 0;
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
        {
            TopicPartition tp = entry.Key;
            if (tp.Topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(offsets));
            }

            if (tp.Partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(offsets), tp.Partition, "Partition must not be negative.");
            }

            OffsetAndMetadata value = entry.Value;
            if (value is null)
            {
                // A reference-type dictionary value; a null OffsetAndMetadata must be
                // rejected before deref (§B5). Java stores never-null values.
                throw new ArgumentException("Offset value must not be null.", nameof(offsets));
            }

            topics[index] = tp.Topic;
            partitions[index] = tp.Partition;
            offsetValues[index] = value.Offset;
            // Python _commit_spec convention: null leader epoch → the -1 "no epoch" sentinel
            // (the ABI header: `leader_epoch < 0 means no epoch`).
            leaderEpochs[index] = value.LeaderEpoch ?? -1;
            // Metadata is never null by construction (the public ctor coerces null → "");
            // the coalesce is defensive.
            metadata[index] = value.Metadata ?? string.Empty;
            index++;
        }

        return new CommitOffsetsSnapshot(topics, partitions, offsetValues, leaderEpochs, metadata, count);
    }

    /// <summary>
    /// The commit-offsets analog of <see cref="WithPinnedTopics"/> /
    /// <see cref="WithPinnedTopicsAndTimestamps"/> — the one marshaller with <b>two</b> string
    /// arrays (topics + metadata). Pins <b>both</b> the topic strings and the metadata strings
    /// <b>call-scoped</b> (the core copies them synchronously during the submit call —
    /// <c>commit_sync_offsets_async</c>'s <c>read_offset_map</c> reads every string into an
    /// owned <c>OffsetAndMetadata</c> before dispatching — so the pins are freed the moment
    /// <paramref name="body"/> returns, never held across the returned <see cref="Task"/>;
    /// ffi §A3/§A4/§B3), fills the two parallel <see cref="IntPtr"/>[] pointer arrays, passes
    /// the three blittable numeric arrays (<c>partitions</c> / <c>offsets</c> /
    /// <c>leader_epochs</c>) straight through, runs <paramref name="body"/>, and releases
    /// <b>all</b> pins in a single <c>finally</c>. <b>No per-element copy beyond the UTF-8
    /// encode</b> (§A4/§B4). A <c>Count</c> of 0 (empty map) runs <paramref name="body"/> with
    /// empty arrays and <c>count == 0</c> — a valid pass-through, never a throw (§B5). Added
    /// as a NEW parallel helper (rather than generalizing <see cref="WithPinnedTopics"/>) so
    /// the shipped M5/P3–P5 partition-op / offset-query pin paths stay byte-for-byte untouched
    /// (PLAN §3.1 — the "clone a parallel helper" discipline); the two-string-array shape has
    /// no existing helper to reuse.
    /// </summary>
    /// <param name="snapshot">The validated five-array input from <see cref="SnapshotCommitOffsets"/>.</param>
    /// <param name="body">
    /// The P/Invoke to run with the pinned <c>(topics, partitions, offsets, leaderEpochs,
    /// metadata, count)</c>.
    /// </param>
    private static void WithPinnedCommitOffsets(
        CommitOffsetsSnapshot snapshot,
        Action<IntPtr[], int[], long[], int[], IntPtr[], int> body)
    {
        int count = snapshot.Count;
        Utf8Marshal.PinnedUtf8String?[] topicPins = new Utf8Marshal.PinnedUtf8String?[count];
        Utf8Marshal.PinnedUtf8String?[] metadataPins = new Utf8Marshal.PinnedUtf8String?[count];
        IntPtr[] topicPointers = new IntPtr[count];
        IntPtr[] metadataPointers = new IntPtr[count];
        try
        {
            for (int i = 0; i < count; i++)
            {
                Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(snapshot.Topics[i]);
                topicPins[i] = topicPin;
                topicPointers[i] = topicPin.Pointer;

                Utf8Marshal.PinnedUtf8String metadataPin = Utf8Marshal.Pin(snapshot.Metadata[i]);
                metadataPins[i] = metadataPin;
                metadataPointers[i] = metadataPin.Pointer;
            }

            body(topicPointers, snapshot.Partitions, snapshot.Offsets, snapshot.LeaderEpochs, metadataPointers, count);
        }
        finally
        {
            // Release ALL pins (both string arrays) in one finally — no leak.
            for (int i = 0; i < count; i++)
            {
                topicPins[i]?.Dispose();
                metadataPins[i]?.Dispose();
            }
        }
    }

    /// <summary>
    /// Validates a <see cref="TopicPartition"/> collection (§B5) and snapshots it into a
    /// <c>(Topic, Partition)</c> array — the shared precondition + snapshot for the three
    /// collection-input offset-map queries (<see cref="CommittedWithCallback"/> /
    /// <see cref="BeginningOffsetsWithCallback"/> / <see cref="EndOffsetsWithCallback"/>),
    /// the same validation <see cref="SubmitPartitionOp"/> performs inline. A
    /// <see langword="null"/> collection is rejected; an <b>empty</b> collection is valid
    /// (yields a zero-length snapshot, passed through as <c>count == 0</c>). Validated
    /// BEFORE any pin / P-Invoke, because the ABI does not validate preconditions and
    /// panics/mismaps on violation (CLAUDE.md §3).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    private static (string Topic, int Partition)[] SnapshotPartitions(IReadOnlyCollection<TopicPartition> partitions)
    {
        if (partitions is null)
        {
            throw new ArgumentNullException(nameof(partitions));
        }

        (string Topic, int Partition)[] snapshot = new (string, int)[partitions.Count];
        int index = 0;
        foreach (TopicPartition tp in partitions)
        {
            if (tp.Topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(partitions));
            }

            if (tp.Partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(partitions), tp.Partition, "Partition must not be negative.");
            }

            snapshot[index++] = (tp.Topic, tp.Partition);
        }

        return snapshot;
    }

    /// <summary>
    /// Projects the partition indices out of a <c>(Topic, Partition)</c> snapshot into a
    /// blittable <c>int[]</c> (the parallel-array partitions passed to the ABI).
    /// </summary>
    private static int[] ExtractPartitions((string Topic, int Partition)[] snapshot)
    {
        int[] partitionArray = new int[snapshot.Length];
        for (int i = 0; i < snapshot.Length; i++)
        {
            partitionArray[i] = snapshot[i].Partition;
        }

        return partitionArray;
    }

    /// <summary>
    /// The validated, snapshotted <c>offsetsForTimes</c> input — the three parallel arrays the
    /// ABI's <c>offsets_for_times[_async]</c> takes, produced by <see cref="SnapshotTimestamps"/>
    /// and consumed by <see cref="WithPinnedTopicsAndTimestamps"/>. <c>Topics</c> is pinned
    /// call-scoped; <c>Partitions</c> / <c>Timestamps</c> are blittable and passed straight
    /// through. <c>Count</c> is the entry count (may be 0 for an empty map). The map-input
    /// analog of <see cref="CommitOffsetsSnapshot"/>.
    /// </summary>
    private readonly struct TimestampsSnapshot
    {
        internal TimestampsSnapshot(string[] topics, int[] partitions, long[] timestamps, int count)
        {
            Topics = topics;
            Partitions = partitions;
            Timestamps = timestamps;
            Count = count;
        }

        internal string[] Topics { get; }

        internal int[] Partitions { get; }

        internal long[] Timestamps { get; }

        internal int Count { get; }
    }

    /// <summary>
    /// Validates a <c>(TopicPartition → timestamp)</c> map (§B5) and snapshots it into the three
    /// parallel arrays — the map-input analog of <see cref="SnapshotPartitions"/> /
    /// <see cref="SnapshotCommitOffsets"/>, validated BEFORE any pin / P-Invoke (the ABI does not
    /// validate preconditions and panics/mismaps on violation, CLAUDE.md §3). <b>Shared</b> by
    /// the async <see cref="OffsetsForTimesWithCallback"/> and the sync
    /// <see cref="OffsetsForTimes"/> so the validation is not duplicated (DoD §6). A
    /// <see langword="null"/> map is rejected; an <b>empty</b> map is valid (yields zero-length
    /// arrays, passed through as <c>Count == 0</c>). A <b>negative</b> timestamp is a Kafka-valid
    /// sentinel (EARLIEST/LATEST special timestamps are negative in ListOffsets) — passed through
    /// as an opaque <c>i64</c>, never rejected.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    private static TimestampsSnapshot SnapshotTimestamps(IReadOnlyDictionary<TopicPartition, long> timestampsToSearch)
    {
        if (timestampsToSearch is null)
        {
            throw new ArgumentNullException(nameof(timestampsToSearch));
        }

        int count = timestampsToSearch.Count;
        string[] topics = new string[count];
        int[] partitionArray = new int[count];
        long[] timestamps = new long[count];
        int index = 0;
        foreach (KeyValuePair<TopicPartition, long> entry in timestampsToSearch)
        {
            TopicPartition tp = entry.Key;
            if (tp.Topic is null)
            {
                throw new ArgumentException("Topic names must not be null.", nameof(timestampsToSearch));
            }

            if (tp.Partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(timestampsToSearch), tp.Partition, "Partition must not be negative.");
            }

            topics[index] = tp.Topic;
            partitionArray[index] = tp.Partition;
            timestamps[index] = entry.Value;
            index++;
        }

        return new TimestampsSnapshot(topics, partitionArray, timestamps, count);
    }

    /// <summary>
    /// Sets the beginning (EARLIEST) offset used by a subsequent <c>SeekToBeginning</c>
    /// reset on a <c>MockConsumer</c> (mock-only driver; mirrors Java
    /// <c>updateBeginningOffsets</c>, one entry). Makes a <c>SeekToBeginning</c> observable
    /// via a follow-up poll (§6.6). Errors (via <see cref="KafkaException"/>) on a real
    /// consumer. The topic is pinned call-scoped.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the update (e.g. a real consumer).</exception>
    internal void UpdateBeginningOffset(string topic, int partition, long offset) =>
        UpdateOffset(topic, partition, offset, NativeMethods.MockConsumerUpdateBeginningOffsets);

    /// <summary>
    /// Sets the end (LATEST) offset used by a subsequent <c>SeekToEnd</c> reset on a
    /// <c>MockConsumer</c> (mock-only driver; mirrors Java <c>updateEndOffsets</c>, one
    /// entry). The LATEST analog of <see cref="UpdateBeginningOffset"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core rejected the update (e.g. a real consumer).</exception>
    internal void UpdateEndOffset(string topic, int partition, long offset) =>
        UpdateOffset(topic, partition, offset, NativeMethods.MockConsumerUpdateEndOffsets);

    /// <summary>
    /// Shared body for the two mock offset-update forwarders: preconditions (§B5) BEFORE the
    /// P/Invoke, pin the topic call-scoped, invoke <paramref name="update"/>, and map the
    /// returned error handle (null = success) the uniform sync-op way (§B5).
    /// </summary>
    private void UpdateOffset(
        string topic,
        int partition,
        long offset,
        Func<SafeConsumerHandle, IntPtr, int, long, IntPtr> update)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "Partition must not be negative.");
        }

        ThrowIfClosed();

        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic))
        {
            // SafeHandle-param (ffi §A2; M9/P4 H1b): the marshaller AddRefs for the whole
            // native call, so a concurrent teardown cannot free the consumer mid-update.
            error = update(_handle, topicPin.Pointer, partition, offset);
        }

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Pins a byte array call-scoped (ffi §A4) for a <c>(ptr, len)</c> ABI parameter,
    /// returning its address and length. A <see langword="null"/> array →
    /// <c>(<see cref="IntPtr.Zero"/>, -1)</c> (the ABI's absent sentinel); an empty
    /// array → a non-null pinned pointer + length 0 (a genuine empty value). The pin is
    /// written to <paramref name="pin"/> for the caller to free in a <c>finally</c>.
    /// </summary>
    private static (IntPtr Pointer, int Length) PinBytes(byte[]? data, ref GCHandle pin)
    {
        if (data is null)
        {
            return (IntPtr.Zero, -1);
        }

        pin = GCHandle.Alloc(data, GCHandleType.Pinned);

        // AddrOfPinnedObject is non-null even for an empty array on current runtimes;
        // the ABI accepts (non-null ptr, len 0) as an empty value (distinct from the
        // (null, -1) absent case).
        return (pin.AddrOfPinnedObject(), data.Length);
    }

    /// <summary>
    /// Transitions from open to closing exactly once. Returns <see langword="true"/>
    /// for the first caller (which performs teardown), <see langword="false"/> for
    /// any concurrent or subsequent caller (a no-op).
    /// </summary>
    private bool TryBeginClose() => Interlocked.CompareExchange(ref _closed, 1, 0) == 0;

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new ObjectDisposedException(nameof(NativeConsumer));
        }
    }
}
