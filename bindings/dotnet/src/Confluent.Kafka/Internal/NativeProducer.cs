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
/// Internal lifecycle wrapper over an owned <c>kafka_producer_Producer_t</c>: it
/// orchestrates config marshalling → construction → destroy, and owns the
/// <see cref="SafeProducerHandle"/> (M11/P1 foundation). It lives under
/// <c>Internal/</c> — not <c>Internal/Interop/</c> — because it uses only the safe
/// managed <see cref="Utf8Marshal.Pin(string)"/>,
/// <see cref="SafeProducerPropertiesHandle"/>, and
/// <see cref="System.Runtime.InteropServices.SafeHandle"/> APIs, so it needs no
/// <c>unsafe</c> (that stays quarantined to <c>Internal/Interop/</c>, CLAUDE.md §2).
/// The producer twin of <c>NativeConsumer</c> — the same construct → dispose shape,
/// mirrored field-for-field on the producer ABI.
/// </summary>
/// <remarks>
/// <para>
/// This is <b>not</b> the public client. The public <c>IAsyncProducer</c> /
/// <c>AsyncKafkaProducer</c> / <c>AsyncMockProducer</c> types compose this wrapper. M11/P1
/// built the interop + lifecycle foundation; <b>M11/P2 adds the async PERIPHERALS</b> —
/// <see cref="FlushWithCallback"/> / <see cref="CloseWithCallback"/> /
/// <see cref="PartitionsForWithCallback"/> over the shipped push completion bridge
/// (<see cref="OperationCompletionSource"/>, ffi §A7 push option) plus the sync
/// <see cref="CloseSync"/> for the graceful blocking <c>Dispose</c> upgrade. Still NOT here:
/// the SEND surface (<c>ProducerRecord</c> / <c>RecordMetadata</c> / <c>Producer_send</c> /
/// the pull-pump / the §A7 pull-vs-push decision) — deferred to the later send phase.
/// </para>
/// <para>
/// <b>Teardown — the pinned M11/P1 sequence, LEFT INTACT: <c>Producer_destroy</c> only,
/// routed through the <see cref="SafeProducerHandle"/>.</b> <see cref="Dispose"/> /
/// <see cref="DisposeAsync"/> release the handle (whose <c>ReleaseHandle</c> calls
/// <c>Producer_destroy</c>) and do <b>no</b> graceful <c>Producer_close</c>-first here.
/// <c>Producer_destroy</c> already blocks + joins the background Sender task (ffi §A2). The
/// <b>graceful close-before-destroy (Dispose upgrade)</b> is layered <em>above</em> this, in
/// the public <c>AsyncKafkaProducer</c> wrapper (which orchestrates
/// <see cref="CloseWithCallback"/> / <see cref="CloseSync"/> → <see cref="Dispose"/>), exactly
/// as the consumer's wrapper orchestrates close→destroy — so P1's teardown pin stays
/// byte-for-byte, the upgrade is purely additive at the public layer, and the (still-absent)
/// pump-drain stays deferred to the send phase.
/// </para>
/// <para>
/// <b>Finalizer avoidance (ffi §A2).</b> <c>Producer_destroy</c> blocks, which is
/// wrong on the finalizer thread, so the producer closes via <see cref="Dispose"/>
/// and never a finalizer. There is no finalizer on this type; the owned
/// <see cref="SafeProducerHandle"/> retains the runtime's critical-finalizer safety
/// net for the leaked-without-Dispose case.
/// </para>
/// <para>
/// <b>Idempotent, ObjectDisposedException-guarded.</b> The atomic
/// <see cref="_disposed"/> latch makes double / concurrent <see cref="Dispose"/>
/// safe (the first caller wins; later calls no-op) and gates use-after-dispose
/// (<see cref="Handle"/> throws <see cref="ObjectDisposedException"/> once closed).
/// An <c>int</c> + <see cref="Interlocked"/> rather than a plain <c>bool</c> because a
/// torn read/write is a race .NET has and Python's GIL hides — the same discipline as
/// the consumer's <c>_closed</c> flag.
/// </para>
/// </remarks>
internal sealed class NativeProducer : IDisposable, IAsyncDisposable
{
    private readonly SafeProducerHandle _handle;

    // Thread-safe disposed latch: 0 = open, 1 = closing/closed. Makes double /
    // concurrent Dispose safe and gates use-after-dispose. Atomic (not a plain bool)
    // to avoid a torn read/write race — .NET has it, Python's GIL hides it.
    private int _disposed;

    private NativeProducer(SafeProducerHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// The owned producer handle. Throws <see cref="ObjectDisposedException"/> once
    /// closed (the use-after-dispose guard). Exposed for the interop tests, which
    /// drive the raw ABI against it; the public client will not expose the handle.
    /// </summary>
    internal SafeProducerHandle Handle
    {
        get
        {
            ThrowIfDisposed();
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

        return new NativeProducer(handle);
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
        return new NativeProducer(handle);
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
        return SubmitVoidOperation(cancellationToken, (producer, callback, userData) =>
            NativeMethods.ProducerFlushAsync(producer, callback, userData));
    }

    /// <summary>
    /// Closes the producer gracefully (async; Java <c>Producer.close()</c>) — the graceful
    /// close leg of the Dispose upgrade (ffi §A7), which the public wrapper follows with
    /// <see cref="Dispose"/> (<c>Producer_destroy</c>). The returned <see cref="Task"/>
    /// completes when the core resolves the close (successfully + idempotently for a
    /// <c>MockProducer</c>), or faults with a <see cref="KafkaException"/>. Reuses the shipped
    /// void completion bridge over <c>Producer_close_async</c>. Does <b>not</b> itself destroy
    /// the handle — the public wrapper orchestrates close→destroy (keeping P1's teardown pin
    /// intact).
    /// </summary>
    /// <remarks>
    /// Same best-effort cancellation as <see cref="FlushWithCallback"/> (no native abort — the
    /// producer has no <c>wakeup()</c>).
    /// </remarks>
    /// <param name="cancellationToken">Best-effort cancellation of the .NET wait (no native abort).</param>
    /// <exception cref="ObjectDisposedException">The producer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task CloseWithCallback(CancellationToken cancellationToken = default)
    {
        return SubmitVoidOperation(cancellationToken, (producer, callback, userData) =>
            NativeMethods.ProducerCloseAsync(producer, callback, userData));
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
    /// Closes the producer <b>synchronously</b>, best-effort — the sync graceful-close leg of
    /// the blocking <c>Dispose</c> upgrade (ffi §A7), run by the public wrapper before
    /// <see cref="Dispose"/> (<c>Producer_destroy</c>). There is no <c>Producer_close_with_timeout</c>
    /// ABI (unlike the consumer), so this is the plain synchronous <c>Producer_close</c>. Blocks
    /// inside the core's runtime (deadlock-free, ffi §A1). Any close error is read-and-freed
    /// then <b>swallowed</b> — a close failure must not prevent the subsequent destroy, and a
    /// best-effort teardown has no caller to hand a failure to (the public <c>Dispose</c> is
    /// non-throwing).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The producer is already closed.</exception>
    internal void CloseSync()
    {
        ThrowIfDisposed();

        // The handle is not released until the public wrapper calls Dispose() after this, so its
        // raw value is valid here (single-owner: no concurrent destroy). Read+free the error via
        // FromHandle, then discard it (best-effort).
        NativeMethods.ProducerClose(_handle.DangerousGetHandle(), out IntPtr error);
        _ = KafkaException.FromHandle(error);
    }

    /// <summary>
    /// Submits a void-result async peripheral (<c>flush</c> / <c>close</c>): root the per-op
    /// context via a <see cref="GCHandle"/>, take a span-the-op ref on the producer
    /// <see cref="SafeProducerHandle"/> (so <c>Producer_destroy</c> cannot run until the op's
    /// completion callback releases it — closing the destroy-vs-in-flight-op use-after-free,
    /// ffi §A2/§A7), wire best-effort cancellation, then run <paramref name="submit"/>.
    /// Ownership of the <see cref="GCHandle"/> transfers to the completion callback (its sole
    /// owner) the moment native is entered; if <paramref name="submit"/> throws before that, the
    /// context is abandoned (handle freed) here. Mirrors <c>NativeConsumer.SubmitVoidOperation</c>.
    /// </summary>
    private Task SubmitVoidOperation(CancellationToken cancellationToken, NativeSubmit submit)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        // Span-the-op ref-count: hold a reference on the producer SafeHandle for the whole async
        // op so ReleaseHandle → Producer_destroy cannot run until the op's completion callback
        // releases it (in FreeGcHandle). Closes the destroy-vs-in-flight-op use-after-free
        // (ffi §A2/§A7) by deferring the native destroy past the op.
        bool handleRefAdded = false;
        _handle.DangerousAddRef(ref handleRefAdded);
        if (handleRefAdded)
        {
            context.SetHandleRef(_handle);
        }
        try
        {
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
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        OperationCompletionSource<TResult> context = new OperationCompletionSource<TResult>();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        // Span-the-op ref-count (see SubmitVoidOperation).
        bool handleRefAdded = false;
        _handle.DangerousAddRef(ref handleRefAdded);
        if (handleRefAdded)
        {
            context.SetHandleRef(_handle);
        }
        try
        {
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
    /// Tears the producer down (the pinned M11/P1 sequence): release the handle,
    /// whose <c>ReleaseHandle</c> calls <c>Producer_destroy</c>. Idempotent and safe
    /// under concurrent / double calls (the atomic <see cref="_disposed"/> latch).
    /// </summary>
    /// <remarks>
    /// <b>Producer_destroy only — no graceful close / flush / pump-join.</b> P1 has no
    /// send path, so there are no pending records to flush and no completion pump to
    /// join; <c>Producer_destroy</c> (which blocks + joins the background Sender per
    /// ffi §A2) is the minimal-correct subset. The graceful <c>Producer_close</c>-first
    /// + flush + pump-join land additively with the later send/flush phases.
    /// </remarks>
    public void Dispose()
    {
        // Idempotent: the first caller wins the latch; later / concurrent calls no-op.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        // ReleaseHandle → Producer_destroy, exactly once (SafeHandle guarantees it).
        _handle.Dispose();
    }

    /// <summary>
    /// Asynchronous teardown. In M11/P1 there is no async teardown work yet (no
    /// completion pump to join, no in-flight send to drain), so it delegates to the
    /// synchronous <see cref="Dispose"/> and returns a completed
    /// <see cref="ValueTask"/>. The graceful async close arrives with the later
    /// send/flush phases (ffi §A7).
    /// </summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(NativeProducer));
        }
    }
}
