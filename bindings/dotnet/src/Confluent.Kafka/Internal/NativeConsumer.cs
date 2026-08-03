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
/// This is <b>not</b> the public client. The public <c>IConsumer</c> /
/// <c>KafkaConsumer</c> / <c>MockConsumer</c> types land later; this wrapper is the
/// internal proving ground for the create → async op → close → destroy lifecycle,
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
/// the core's way: a concurrent <b>async op</b> (<see cref="SubscribeAsync"/> /
/// <see cref="SeekAsync"/>) is rejected by the core inline and surfaces as a
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
/// <b>Accepted residuals (misuse-only, Python parity, not reachable while
/// internal-only).</b> These are explicitly accepted under the not-thread-safe
/// contract; <see cref="DisposeAsync"/> on the awaiting task is the clean path:
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Teardown with an unawaited in-flight op → strand + one-time
/// <see cref="GCHandle"/>/context leak.</b> An op that is submitted and then not
/// awaited across a teardown may strand its <see cref="Task"/> and leak its per-op
/// <see cref="GCHandle"/> once (the following <c>Consumer_destroy</c> cancels the op,
/// so its callback never fires). Python accepts the same (bare <c>_destroy</c> after
/// draining its <em>own</em> awaited op). The M3/P1 managed fault machinery
/// (<c>FaultTaskOnly</c>) that papered over this is deliberately <b>not</b>
/// re-added — it was exactly what M3/P2 removed.
/// </item>
/// <item>
/// <b><see cref="Wakeup"/> / <see cref="GroupId"/> handle TOCTOU vs a concurrent
/// teardown → use-after-free.</b> The closed-flag check and the
/// <c>DangerousGetHandle()</c> deref are not atomic, so a concurrent teardown
/// between them could free the handle. Reachable only under cross-thread misuse;
/// Python has the same, more exposed (its <c>wakeup</c> has no closed check at all).
/// </item>
/// <item>
/// <b>Submit-vs-<c>destroy</c> handle race → use-after-free.</b> The
/// <c>DangerousGetHandle()</c> in <see cref="SubmitVoidOperation"/> vs a concurrent
/// <c>Consumer_destroy</c>. Reachable only under cross-thread misuse.
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
    // public CloseAsync(TimeSpan) once the public client lands.
    private const long DefaultCloseTimeoutMilliseconds = 5_000;

    private readonly SafeConsumerHandle _handle;

    // Thread-safe closed flag (invariant #3, the teardown gate — NOT the removed
    // managed access guard): 0 = open, 1 = closing/closed. Makes double / concurrent
    // Dispose safe and gates use-after-dispose. A plain bool would be a torn-read
    // race .NET has and Python's GIL hides, so this stays atomic.
    private int _closed;

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
    internal Task SubscribeAsync(
        IReadOnlyCollection<string> topics,
        CancellationToken cancellationToken = default)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        // Snapshot + validate BEFORE native (ffi §B5 preconditions).
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

        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
        {
            // Call-scoped pins: subscribe_async reads the topic strings synchronously
            // into an owned Vec<String> before spawning, so the buffers are freed
            // once the native call returns (ffi §A4 call-scoped pin).
            Utf8Marshal.PinnedUtf8String?[] pins = new Utf8Marshal.PinnedUtf8String?[topicArray.Length];
            IntPtr[] pointers = new IntPtr[topicArray.Length];
            try
            {
                for (int i = 0; i < topicArray.Length; i++)
                {
                    Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(topicArray[i]);
                    pins[i] = pin;
                    pointers[i] = pin.Pointer;
                }

                NativeMethods.ConsumerSubscribeAsync(consumer, pointers, topicArray.Length, callback, userData);
            }
            finally
            {
                for (int i = 0; i < pins.Length; i++)
                {
                    pins[i]?.Dispose();
                }
            }
        });
    }

    /// <summary>
    /// Seeks <c>(topic, partition)</c> to <paramref name="offset"/> (async). On a
    /// <c>MockConsumer</c>, seeking an <b>unassigned</b> partition is a genuine
    /// broker-free failure — the returned <see cref="Task"/> faults with a
    /// <see cref="KafkaException"/> (the void bridge's error path).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task SeekAsync(
        string topic,
        int partition,
        long offset,
        CancellationToken cancellationToken = default)
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

        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
        {
            using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
            NativeMethods.ConsumerSeekAsync(consumer, topicPin.Pointer, partition, offset, callback, userData);
        });
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
    /// <b>Accepted residual (single-owner):</b> the closed-flag check and the
    /// <c>DangerousGetHandle()</c> deref are not atomic, so a concurrent teardown that
    /// runs between them could free the handle first — a check-then-use TOCTOU
    /// (use-after-free) reachable only under cross-thread misuse. Accepted-by-design
    /// under the not-thread-safe contract; the canonical <c>wakeup()</c> usage
    /// (thread A blocked, thread B wakes it, thread A then disposes) does not race
    /// wakeup against dispose. Any future hardening (per-call
    /// <c>SafeHandle.DangerousAddRef</c>) renumbers to N≥7.
    /// </remarks>
    internal void Wakeup()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            return;
        }

        NativeMethods.ConsumerWakeup(_handle.DangerousGetHandle());
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
    /// <b>Accepted residual:</b> the same check-then-use handle TOCTOU vs teardown as
    /// <see cref="Wakeup"/> (accepted-by-design; N≥7 if ever hardened).
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for
    /// multi-threaded access).
    /// </exception>
    internal string? GroupId()
    {
        ThrowIfClosed();

        IntPtr metadata = NativeMethods.ConsumerGroupMetadata(_handle.DangerousGetHandle());
        if (metadata == IntPtr.Zero)
        {
            // The core's own access guard rejected concurrent access (null handle).
            // Surface it the Python way: a concurrent sync state read is an
            // InvalidOperationException, not a silent null (ffi §B5, CLAUDE.md §3).
            throw new InvalidOperationException(
                "KafkaConsumer is not safe for multi-threaded access.");
        }

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
    /// Graceful synchronous teardown (blocking fallback): <c>Consumer_close_with_timeout</c>
    /// then releases the handle (→ <c>Consumer_destroy</c>). Idempotent and safe under
    /// concurrent / double calls (the atomic closed flag). The async teardown
    /// (<see cref="DisposeAsync"/>) is the primary path.
    /// </summary>
    /// <remarks>
    /// <b>Single-owner: no separate-op drain.</b> Under the not-thread-safe contract
    /// the awaiter of an op is its disposer, so there is no concurrent submitter for
    /// teardown to drain — <see cref="Dispose"/> simply closes gracefully then
    /// destroys, matching the Python sibling (drain its <em>own</em> awaited op, then
    /// bare <c>_destroy</c>). <b>Accepted residual:</b> an <em>unawaited</em> in-flight
    /// op across a synchronous <see cref="Dispose"/> may strand its <see cref="Task"/>
    /// and leak its per-op <see cref="GCHandle"/> once (the following
    /// <c>Consumer_destroy</c> cancels the op, so its callback never fires). The M3/P1
    /// fault machinery (<c>FaultTaskOnly</c>) that masked this is deliberately not
    /// re-added; <see cref="DisposeAsync"/> on the awaiting task is the clean path.
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
    /// (surfacing it is the future <c>CloseAsync(TimeSpan)</c>'s job).
    /// </summary>
    /// <remarks>
    /// <b>Single-owner: no separate-op drain.</b> Under the not-thread-safe contract
    /// the awaiter of an op is its disposer, so <see cref="DisposeAsync"/> does not
    /// wake+await a <em>separately-submitted</em> in-flight op — there is no concurrent
    /// submitter to drain. It closes gracefully (<c>close_async</c> joins the bg task)
    /// then destroys, matching the Python sibling's <c>close()</c>. The same accepted
    /// unawaited-op residual as <see cref="Dispose"/> applies, but the clean path is
    /// exactly to <c>await</c> the op and then <see cref="DisposeAsync"/>.
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
            await CloseAsyncInternal().ConfigureAwait(false);
        }
        catch (KafkaException)
        {
            // Best-effort teardown — Dispose/DisposeAsync must not surface a close
            // error; that is CloseAsync(TimeSpan)'s job.
        }
        finally
        {
            // ReleaseHandle → Consumer_destroy, exactly once.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Bridges <c>Consumer_close_async</c> to a <see cref="Task"/> via the shared
    /// completion callback. If the submitting P/Invoke throws before native could
    /// fire the callback, the context is abandoned (its <c>GCHandle</c> freed) here.
    /// </summary>
    private Task CloseAsyncInternal()
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
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
