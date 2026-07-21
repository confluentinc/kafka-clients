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

using Confluent.Kafka.ShareConsumer.Internal.Interop;

namespace Confluent.Kafka.ShareConsumer.Internal;

/// <summary>
/// Internal lifecycle wrapper over an owned <c>kafka_consumer_Consumer_t</c>: it
/// orchestrates config marshalling → construction → graceful close → destroy, and
/// owns the <see cref="SafeConsumerHandle"/> (PLAN D4). It lives under
/// <c>Internal/</c> — not <c>Internal/Interop/</c> — because it uses only the safe
/// managed <see cref="Utf8Marshal.Pin(string)"/> and <see cref="System.Runtime.InteropServices.SafeHandle"/>
/// APIs, so it needs no <c>unsafe</c> (that stays quarantined to
/// <c>Internal/Interop/</c>, CLAUDE.md §2).
/// </summary>
/// <remarks>
/// <para>
/// This is <b>not</b> the public client. The public <c>IConsumer</c> /
/// <c>KafkaConsumer</c> / <c>MockConsumer</c> types land with poll/subscribe and
/// the completion bridge; this wrapper exercises only the create → close → destroy
/// lifecycle and the operational/precondition error surfaces.
/// </para>
/// <para>
/// Disposal is <b>synchronous only</b> this phase (<see cref="IDisposable"/>);
/// <c>IAsyncDisposable</c> is deferred with the completion bridge (PLAN D3,
/// a recorded deviation from CLAUDE.md §4's "both" default — the only close
/// primitive in scope is the synchronous <c>Consumer_close_with_timeout</c>).
/// </para>
/// </remarks>
internal sealed class NativeConsumer : IDisposable
{
    // Fixed graceful-close budget for the synchronous Dispose. A never-joined
    // consumer closes near-instantly; a user-supplied timeout arrives with the
    // public CloseAsync(TimeSpan) once the completion bridge lands (PLAN D3).
    private const long DefaultCloseTimeoutMilliseconds = 5_000;

    private readonly SafeConsumerHandle _handle;
    private bool _disposed;

    private NativeConsumer(SafeConsumerHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// The owned consumer handle. Throws <see cref="ObjectDisposedException"/> after
    /// <see cref="Dispose"/> (the use-after-dispose guard). Exposed for the interop
    /// tests, which drive the raw ABI (e.g. the group-metadata round-trip) against
    /// it; the public client will not expose the handle.
    /// </summary>
    internal SafeConsumerHandle Handle
    {
        get
        {
            ThrowIfDisposed();
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

        IntPtr rawConsumer;
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
            rawConsumer = NativeMethods.KafkaConsumerNew(props, out error);
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
            throw failure;
        }

        return new NativeConsumer(SafeConsumerHandle.FromRaw(rawConsumer));
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
        IntPtr rawConsumer;
        if (autoOffsetReset is null)
        {
            rawConsumer = NativeMethods.MockConsumerNew(IntPtr.Zero);
        }
        else
        {
            using Utf8Marshal.PinnedUtf8String strategy = Utf8Marshal.Pin(autoOffsetReset);
            rawConsumer = NativeMethods.MockConsumerNew(strategy.Pointer);
        }

        return new NativeConsumer(SafeConsumerHandle.FromRaw(rawConsumer));
    }

    /// <summary>
    /// Graceful synchronous teardown: <c>Consumer_close_with_timeout</c> (joins the
    /// background task) then releases the handle (→ <c>Consumer_destroy</c>).
    /// Idempotent; safe to call more than once.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        try
        {
            // Graceful close first (ffi §B2/§B7): destroy alone is fire-and-forget.
            // The handle is not released until after this returns, so its raw value
            // is valid here. Dispose consumes the close error (freeing the handle
            // via FromHandle) but does NOT rethrow — Dispose must not throw, and a
            // best-effort teardown has no caller to hand a failure to. Surfacing
            // close errors is the future CloseAsync(TimeSpan)'s job (PLAN D3).
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

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(NativeConsumer));
        }
    }
}
