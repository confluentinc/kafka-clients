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

using Confluent.Kafka.ShareConsumer.Internal.Interop;

namespace Confluent.Kafka.ShareConsumer.Internal;

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
/// The consumer is <b>one operation in flight</b> (ffi §B5): a
/// <see cref="ConsumerAccessGuard"/> mirrors the core guard so concurrent misuse
/// surfaces as the right .NET exception before the P/Invoke. Async ops
/// (<see cref="SubscribeAsync"/> / <see cref="SeekAsync"/>) bridge the C completion
/// callback to a <see cref="Task"/>; the completion fires on the core's foreign
/// dispatcher thread (ffi §B6/§B7). <see cref="Wakeup"/> is the one cross-thread
/// call — it bypasses the guard by design.
/// </para>
/// <para>
/// Teardown has two paths, both gated by a thread-safe closed flag (folds in the
/// N=5-deferred teardown-thread-safety hardening):
/// <see cref="DisposeAsync"/> is <b>primary</b> — it drains the in-flight op
/// (wakeup + await) then closes <em>asynchronously</em> (joins the bg task) before
/// destroy; <see cref="Dispose"/> is the blocking fallback (<c>close_with_timeout</c>
/// → destroy). A bare <c>Consumer_destroy</c> before the in-flight callback fires
/// would cancel it (the callback never fires → the <c>Task</c> hangs and the
/// <c>GCHandle</c> leaks), which is why the async path drains first (ffi §B7). The
/// sync path cannot drain (its guarded close is rejected while the op holds the core
/// guard), so instead it faults the pending op's <c>Task</c> after destroy so a
/// fire-and-forget awaiter cannot strand — but it does <b>not</b> free the
/// <c>GCHandle</c>: the completion callback stays the sole owner of that free
/// (aligning with the in-repo Python + confluent-kafka-dotnet siblings). A callback
/// already queued before destroy still fires afterward and frees the handle itself;
/// only if destroy cancels the op before its callback is queued does that one op's
/// <c>GCHandle</c> leak — a rare, one-time, teardown-only residual (unawaited op +
/// sync <c>Dispose</c>) that <see cref="DisposeAsync"/> avoids by draining.
/// </para>
/// </remarks>
internal sealed class NativeConsumer : IDisposable, IAsyncDisposable
{
    // Fixed graceful-close budget for the synchronous Dispose. A never-joined
    // consumer closes near-instantly; a user-supplied timeout arrives with the
    // public CloseAsync(TimeSpan) once the public client lands.
    private const long DefaultCloseTimeoutMilliseconds = 5_000;

    private readonly SafeConsumerHandle _handle;

    // Mirrors the core's one-op-in-flight guard so concurrent misuse throws the
    // right .NET exception type before the P/Invoke (ffi §B5).
    private readonly ConsumerAccessGuard _guard = new ConsumerAccessGuard();

    // Thread-safe closed flag (replaces the M2 non-atomic bool): 0 = open, 1 =
    // closing/closed. Makes double / concurrent Dispose safe and gates
    // use-after-dispose. Folds in the N=5-deferred teardown-thread-safety item.
    private int _closed;

    // The most recently submitted op's Task. Ops are serialized by the guard, so at
    // most one is pending; teardown drains it (wakeup + await/wait) before destroy.
    private Task? _inFlightOperation;

    // The most recently submitted op's completion context (its TCS + rooting
    // GCHandle). Tracked so the SYNCHRONOUS Dispose can fault its Task after destroy:
    // its guarded close_with_timeout is rejected (no drain) while the op holds the
    // core guard, and the following destroy cancels the op's callback — so without a
    // fault the awaiter's Task would strand (ffi §B7). Dispose faults the Task ONLY
    // (FaultTaskOnly); it does NOT free the GCHandle — the completion callback is the
    // sole owner of that free, so a callback queued before destroy can still fire
    // after it and recover a live handle (no use-after-free). DisposeAsync drains
    // instead (the callback fires normally), so it does not use this field.
    private OperationCompletionSource? _inFlightContext;

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
    /// a <c>MockConsumer</c>, or faulted with a <see cref="KafkaException"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">Another operation is already in flight.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task SubscribeAsync(
        IReadOnlyCollection<string> topics,
        CancellationToken cancellationToken = default)
    {
        if (topics is null)
        {
            throw new ArgumentNullException(nameof(topics));
        }

        // Snapshot + validate BEFORE the guard / native (ffi §B5 preconditions).
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
    /// <exception cref="KafkaException">Another operation is already in flight.</exception>
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
    /// Interrupts the in-flight op (Java <c>wakeup()</c>). Sync and cross-thread —
    /// it <b>bypasses</b> the access guard by design (ffi §B5 / consumer-threading
    /// §11). Best-effort: a no-op once closing/closed (the handle may be about to be
    /// destroyed); the thread-safe closed check is the teardown guard, not a
    /// per-call <c>SafeHandle</c> AddRef (per the N=5-deferred guidance).
    /// </summary>
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
    /// read</b> that participates in the access guard: a concurrent op makes this
    /// throw <see cref="InvalidOperationException"/> ("not safe for multi-threaded
    /// access", ffi §B5). Marshals a Category-3 owned metadata handle and frees it
    /// (§B2).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">An operation is already in flight.</exception>
    internal string? GroupId()
    {
        ThrowIfClosed();
        _guard.EnterStateRead();
        try
        {
            IntPtr metadata = NativeMethods.ConsumerGroupMetadata(_handle.DangerousGetHandle());
            if (metadata == IntPtr.Zero)
            {
                // The core's own guard rejected concurrent access (should not happen
                // while the managed state-read guard is held; defensive).
                return null;
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
        finally
        {
            _guard.Release();
        }
    }

    /// <summary>
    /// Graceful synchronous teardown (blocking fallback): <c>Consumer_close_with_timeout</c>
    /// then releases the handle (→ <c>Consumer_destroy</c>), and finally faults any
    /// pending in-flight op's <c>Task</c> so a fire-and-forget awaiter cannot strand.
    /// Idempotent and safe under concurrent / double calls (the thread-safe closed
    /// flag). The drain-first async teardown (<c>DisposeAsync</c>) is the primary path.
    /// </summary>
    /// <remarks>
    /// <c>close_with_timeout</c> is a <em>guarded</em> sync op: while an async op is
    /// genuinely in flight it holds the core access guard, so the close is rejected
    /// (ConcurrentModification) and does <b>not</b> drain the op — the following
    /// <c>Consumer_destroy</c> then cancels the op's future, so in the common case its
    /// completion callback never fires. Without a fault, the awaiter's <c>Task</c>
    /// would strand (ffi §B7). So, <em>after</em> destroy, this faults the op's
    /// <c>Task</c> (<see cref="OperationCompletionSource.FaultTaskOnly"/>) — via
    /// idempotent primitives, so a callback that fired before destroy makes it a
    /// harmless no-op (no new race, not sync-over-async: it never waits on the op
    /// <c>Task</c>). <b>It does not free the <c>GCHandle</c>:</b> the completion
    /// callback is the sole owner of that free (aligning with the in-repo Python +
    /// confluent-kafka-dotnet siblings). A completion job already queued before destroy
    /// still fires afterward (the ABI drains queued dispatcher jobs without joining)
    /// and must recover a live handle, so freeing it here would be a use-after-free
    /// (Critic N=5 Finding 3). Accepted residual: if destroy cancels the op before its
    /// callback is queued, that one op's <c>GCHandle</c> leaks — a rare, one-time,
    /// teardown-only misuse-case leak; <c>DisposeAsync</c> drains (wakeup + await) so
    /// the callback fires normally and there is no leak.
    /// </remarks>
    public void Dispose()
    {
        if (!TryBeginClose())
        {
            return;
        }

        // Snapshot any pending op's context BEFORE teardown so it can be reclaimed
        // after destroy cancels its callback (see the reclaim below).
        OperationCompletionSource? pending = Volatile.Read(ref _inFlightContext);

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

        // Fault the pending in-flight op's Task so a fire-and-forget awaiter cannot
        // strand (ffi §B7). Ordered AFTER destroy and built on idempotent primitives
        // (TrySetException no-ops if completed), so a callback that fired before
        // destroy makes this a no-op — race-safe, and NOT sync-over-async (no
        // await/Wait on the op Task). This faults the Task ONLY: it does NOT free the
        // GCHandle. The completion callback is the sole owner of that free — a job
        // queued before destroy still fires afterward and must recover a live handle
        // (freeing it here would be a use-after-free, Critic N=5 Finding 3). See the
        // <remarks> for the accepted case-A residual leak.
        pending?.FaultTaskOnly(new ObjectDisposedException(nameof(NativeConsumer)));
    }

    /// <summary>
    /// Graceful <b>async</b> teardown (the primary path): drain the in-flight op
    /// (<c>wakeup</c> + <c>await</c>), then <c>Consumer_close_async</c> (joins the
    /// background task) via the completion bridge, then release the handle
    /// (→ <c>Consumer_destroy</c>). Draining first is essential — a bare destroy
    /// while an op is in flight cancels its callback (the callback never fires, so
    /// the <c>Task</c> would hang and the <c>GCHandle</c> would leak, ffi §B7).
    /// Idempotent and safe under concurrent / double calls (the thread-safe closed
    /// flag). Best-effort: it swallows the in-flight op's fault and the close error
    /// (surfacing the latter is the future <c>CloseAsync(TimeSpan)</c>'s job).
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!TryBeginClose())
        {
            return;
        }

        // Drain: wakeup (best-effort abort) + await the in-flight op so its callback
        // fires (releasing the core guard + freeing its GCHandle) before we close.
        // Call the native wakeup directly — the public Wakeup() no-ops once closed.
        Task? pending = Volatile.Read(ref _inFlightOperation);
        if (pending is not null)
        {
            NativeMethods.ConsumerWakeup(_handle.DangerousGetHandle());
            try
            {
                await pending.ConfigureAwait(false);
            }
            catch
            {
                // Drain: the op's own fault (e.g. a wakeup/seek error) is not the
                // teardown's concern — it was already surfaced to that op's awaiter.
            }
        }

        try
        {
            // Graceful async close (joins the bg task) via the same void bridge; no
            // managed guard (the op is drained, so the core guard is free).
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
    /// completion callback, with <b>no</b> managed access guard (close is a
    /// lifecycle op run after the in-flight op is drained). If the submitting
    /// P/Invoke throws before native could fire the callback, the context is
    /// abandoned (its <c>GCHandle</c> freed) here.
    /// </summary>
    private Task CloseAsyncInternal()
    {
        OperationCompletionSource context = new OperationCompletionSource(guard: null);
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
    /// Submits a void-result async op: enter the one-op guard, root the per-op
    /// context via a <see cref="GCHandle"/>, wire cancellation, then run
    /// <paramref name="submit"/> (which pins its args call-scoped and P/Invokes).
    /// Ownership of the guard + <see cref="GCHandle"/> transfers to the completion
    /// callback the moment native is entered; if <paramref name="submit"/> throws
    /// before that, the context is abandoned (guard released, handle freed) here.
    /// </summary>
    private Task SubmitVoidOperation(CancellationToken cancellationToken, NativeSubmit submit)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        // Reject a concurrent async op with a KafkaException (ConcurrentModification)
        // BEFORE allocating anything — no leak on rejection.
        _guard.EnterOperation();

        OperationCompletionSource context = new OperationCompletionSource(_guard);
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

        // Publish the context BEFORE the Task: a concurrent sync Dispose reads the
        // context to fault its Task, so it must see a non-null context whenever it
        // could see the Task. Faulting a long-completed op is a harmless no-op
        // (idempotent — see OperationCompletionSource.FaultTaskOnly).
        Task task = context.Task;
        Volatile.Write(ref _inFlightContext, context);
        Volatile.Write(ref _inFlightOperation, task);
        return task;
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
