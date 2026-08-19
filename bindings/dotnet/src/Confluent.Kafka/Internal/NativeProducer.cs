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
/// <c>KafkaProducer</c> / <c>MockProducer</c> types (later phases) will compose this
/// wrapper; M11/P1 builds only the interop + lifecycle foundation they sit on — no
/// send / flush / close / partitions-for surface, no <c>ProducerRecord</c> /
/// <c>RecordMetadata</c>, no completion pump / async bridge (the ffi §A7 pull-vs-push
/// decision is untouched here).
/// </para>
/// <para>
/// <b>Teardown — the pinned M11/P1 sequence: <c>Producer_destroy</c> only, routed
/// through the <see cref="SafeProducerHandle"/>.</b> <see cref="Dispose"/> releases
/// the handle (whose <c>ReleaseHandle</c> calls <c>Producer_destroy</c>) and does
/// <b>no</b> graceful <c>Producer_close</c>-first, flush, or pump-join. That is the
/// minimal-correct subset for the foundation: P1 has no send path, so there are no
/// pending records to flush and no completion pump to join, and
/// <c>Producer_destroy</c> already blocks + joins the background Sender task
/// (ffi §A2). The graceful close-first + flush + pump-join arrive additively with the
/// later send/flush phases — this mirrors what Python's fuller teardown
/// (<c>_cancel</c> → <c>Producer_shutdown</c> → <c>Producer_close_async</c> →
/// <c>Producer_destroy</c>) reduces to when send/pump do not exist yet.
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
