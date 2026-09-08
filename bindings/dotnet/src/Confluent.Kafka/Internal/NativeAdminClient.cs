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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The shared native surface behind <see cref="KafkaAdminClient"/> and
/// <see cref="MockAdminClient"/>: it owns the <see cref="SafeAdminHandle"/>, submits
/// every RPC, and orchestrates teardown. The two public clients differ only in how the
/// handle is constructed — the ABI hands back the <em>same</em>
/// <c>kafka_admin_AdminClient_t*</c> for both — so keeping the RPC surface here is what
/// stops it being written twice, exactly as <c>NativeConsumer</c> / <c>NativeProducer</c>
/// do for their pairs.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every RPC drives the <c>_async</c> ABI entry point, even though the C# method is
/// synchronous.</b> That reads backwards at first glance and is deliberate: "synchronous"
/// describes the C# method's own return behaviour — it hands back a <c>*Result</c>
/// without waiting, as Java's non-blocking <c>Admin</c> methods do — and the
/// <c>_async</c> ABI is what makes that possible. The sync ABI twin blocks until every
/// per-key future resolves, so calling it would invent the blocking Java does not have,
/// and wrapping it in <c>Task.Run</c> would be sync-over-async (ffi §B7). The 46
/// synchronous ABI entry points are simply unused by this binding.
/// </para>
/// <para>
/// <b>No access guard, and none is wanted.</b> Unlike the consumer, the admin ABI
/// permits concurrent operations, so there is no single-op-in-flight assumption here and
/// no managed mirror of one. What each in-flight operation <em>does</em> hold is a
/// span-the-op reference on the client handle — see <see cref="AdminOperation"/>.
/// </para>
/// </remarks>
internal sealed class NativeAdminClient : IDisposable
{
    /// <summary>
    /// A negative <c>timeout_ms</c> means <b>unset</b> — the client default applies —
    /// and for <c>close</c> it means Java's no-argument <c>close()</c> (wait
    /// indefinitely). It does <b>not</b> mean a zero timeout, which is why a null
    /// <c>TimeSpan</c>/<c>int</c> maps here rather than to 0.
    /// </summary>
    private const int UnsetTimeoutMs = -1;

    private readonly SafeAdminHandle _handle;
    private int _closed;

    private NativeAdminClient(SafeAdminHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// The ABI shape of <c>create_topics_async</c>. A method-group reference to
    /// <see cref="NativeMethods.AdminClientCreateTopicsAsync"/> binds to it directly, so
    /// production passes the real P/Invoke while a test can pass a stand-in that
    /// captures <c>user_data</c> and drives the <em>production</em> trampoline at a
    /// moment of its choosing — the only way to make "an operation is in flight"
    /// deterministic without a broker or a sleep.
    /// </summary>
    internal delegate void NativeCreateTopicsSubmit(
        IntPtr admin,
        IntPtr[] topics,
        int count,
        int timeoutMs,
        bool validateOnly,
        bool retryOnQuotaViolation,
        AdminCallbacks.CreateTopicsCallback callback,
        IntPtr userData);

    /// <summary>
    /// The owned client handle. Exposed for the interop tests, which read
    /// <see cref="SafeHandle.IsClosed"/> to observe when the native release actually
    /// happened; the public clients never expose it.
    /// </summary>
    internal SafeAdminHandle Handle => _handle;

    /// <summary>
    /// Creates a real admin client from a config map: each entry becomes an
    /// <c>AdminClientProperties_put</c> (keys are the Java dotted names), then
    /// <c>AdminClient_new</c> reads the properties. A construction failure surfaces as a
    /// <see cref="KafkaException"/>.
    /// </summary>
    /// <param name="config">Config keyed by Java dotted names; values are strings.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A config value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    internal static NativeAdminClient Create(IReadOnlyDictionary<string, string> config)
    {
        // Preconditions BEFORE any pin/marshal/P-Invoke (ffi §B5): the ABI does not
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

        SafeAdminHandle handle;
        IntPtr error;

        SafeAdminPropertiesHandle props = SafeAdminPropertiesHandle.Create();
        try
        {
            foreach (KeyValuePair<string, string> entry in config)
            {
                using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                NativeMethods.AdminClientPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
            }

            // props is passed as the SafeHandle so the marshaller keeps it alive across
            // the call; the ABI does not consume it (freed below). The client handle
            // arrives ALREADY WRAPPED — the marshaller invokes SafeAdminHandle's private
            // ctor and sets the pointer atomically, closing the allocation-gap window.
            handle = NativeMethods.AdminClientNew(props, out error);
        }
        finally
        {
            // Header: the caller retains props ownership → free it after the call.
            props.Dispose();
        }

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            // On the null native return the marshaller handed back an IsInvalid handle;
            // disposing it skips ReleaseHandle, so there is no spurious destroy.
            handle.Dispose();
            throw failure;
        }

        if (handle.IsInvalid)
        {
            // (null handle, null error) would be a core contract violation. Never store an
            // IsInvalid handle — every later call would hand native a null pointer.
            handle.Dispose();
            throw new KafkaException("kafka_admin_AdminClient_new returned a null handle without an error.");
        }

        return new NativeAdminClient(handle);
    }

    /// <summary>
    /// Creates a broker-less mock admin client. The ABI returns the same handle type as
    /// the real constructor, so the whole RPC surface works against it unchanged.
    /// </summary>
    /// <param name="numBrokers">The number of brokers to simulate; at least 1.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="numBrokers"/> is less than 1.</exception>
    /// <exception cref="KafkaException">The core could not create the mock.</exception>
    internal static NativeAdminClient CreateMock(int numBrokers)
    {
        // Validate BEFORE the call (ffi §B5). The ABI returns null for num_brokers < 1 —
        // Java's MockAdminClient.Builder.build() throw expressed in the FFI idiom — and a
        // null must be mapped, never dereferenced. Rejecting it here gives the caller the
        // .NET exception the mistake deserves instead of an opaque core error.
        if (numBrokers < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(numBrokers), numBrokers, "A mock admin client requires at least one broker.");
        }

        SafeAdminHandle handle = NativeMethods.MockAdminClientNew(numBrokers);
        if (handle.IsInvalid)
        {
            // Reachable only if the core could not create its tokio runtime — the guard
            // above already excluded the num_brokers case. There is no out_error on this
            // entry point, so the null return is all the ABI gives us.
            handle.Dispose();
            throw new KafkaException("kafka_admin_MockAdminClient_new returned a null handle.");
        }

        return new NativeAdminClient(handle);
    }

    /// <summary>
    /// Submits <c>createTopics</c> and returns immediately with one awaitable per topic
    /// (Java's non-blocking <c>createTopics</c>).
    /// </summary>
    /// <param name="newTopics">The topics to create.</param>
    /// <param name="options">Request options, or <see langword="null"/> for Java's defaults.</param>
    internal CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options) =>
        CreateTopics(newTopics, options, NativeMethods.AdminClientCreateTopicsAsync);

    /// <summary>
    /// The <c>createTopics</c> submit, with the native call injectable. Production calls
    /// the overload above, which supplies the real P/Invoke; the interop tests supply a
    /// stand-in so the in-flight window is deterministic. Everything else — validation,
    /// the per-key sources, the <c>GCHandle</c>, the span-the-op reference, the input
    /// handles' lifetime — is the one production path either way, so a test cannot
    /// accidentally prove a property of its own fixture.
    /// </summary>
    internal CreateTopicsResult CreateTopics(
        IEnumerable<NewTopic> newTopics,
        CreateTopicsOptions? options,
        NativeCreateTopicsSubmit submit)
    {
        ThrowIfClosed();

        // ---- Preconditions, BEFORE any pin / marshal / P-Invoke (ffi §B5) ----
        if (newTopics is null)
        {
            throw new ArgumentNullException(nameof(newTopics));
        }

        int timeoutMs = UnsetTimeoutMs;
        bool validateOnly = false;
        bool retryOnQuotaViolation = true;
        if (options is not null)
        {
            if (options.TimeoutMs is < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options.TimeoutMs,
                    "CreateTopicsOptions.TimeoutMs must not be negative; leave it null to use the client default.");
            }

            timeoutMs = options.TimeoutMs ?? UnsetTimeoutMs;
            validateOnly = options.ValidateOnly;
            retryOnQuotaViolation = options.RetryOnQuotaViolation;
        }

        // Java keys its result on a Map and skips a repeated name (KafkaAdminClient
        // populates `topicFutures` only `if (!topicFutures.containsKey(...))`), so a
        // duplicate is one entry here too — and the request array is de-duplicated with
        // it, so the two sides cannot disagree about how many topics were asked for.
        List<NewTopic> requested = new List<NewTopic>();
        List<string> keys = new List<string>();
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (NewTopic topic in newTopics)
        {
            if (topic is null)
            {
                throw new ArgumentException("The topics to create must not contain a null element.", nameof(newTopics));
            }

            if (topic.Configs is not null)
            {
                foreach (KeyValuePair<string, string> entry in topic.Configs)
                {
                    if (entry.Value is null)
                    {
                        throw new ArgumentException(
                            $"Configuration value for key '{entry.Key}' on topic '{topic.Name}' must not be null.",
                            nameof(newTopics));
                    }
                }
            }

            if (seen.Add(topic.Name))
            {
                requested.Add(topic);
                keys.Add(topic.Name);
            }
        }

        // ---- Publish everything the callback needs BEFORE the call ----
        // The header requires it, and the inline-callback path makes it real: the callback
        // can run on this very thread before the entry point returns.
        KeyedAdminOperation<TopicMetadataAndConfig> operation =
            new KeyedAdminOperation<TopicMetadataAndConfig>("createTopics", keys);
        GCHandle gcHandle = GCHandle.Alloc(operation, GCHandleType.Normal);
        operation.SetGcHandle(gcHandle);

        IntPtr[] handles = new IntPtr[requested.Count];
        try
        {
            // Span-the-op reference, INSIDE the try so a DangerousAddRef throw routes
            // through AbandonBeforeSubmit rather than rooting the GCHandle forever. It
            // keeps the native client alive from here until the completion callback
            // releases it — the binding's whole defence against the ABI's unguarded
            // AdminClient_destroy (see AdminOperation / SafeAdminHandle).
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                operation.SetHandleRef(_handle);
            }

            for (int i = 0; i < requested.Count; i++)
            {
                handles[i] = NewTopicMarshal.Build(requested[i]);
            }

            submit(
                _handle.DangerousGetHandle(),
                handles,
                handles.Length,
                timeoutMs,
                validateOnly,
                retryOnQuotaViolation,
                AdminCallbacks.CreateTopics,
                GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            // Native never ran → the callback can never fire → we own the cleanup.
            // Idempotent, so it is harmless even in the (unreachable) case where an
            // inline callback already ran before the throw.
            operation.AbandonBeforeSubmit();
            throw;
        }
        finally
        {
            // The ABI copies out during the submit and "the caller retains ownership" of
            // the input entries, so they are destroyed here — after the call, on every
            // path, including a partially built array. Null-safe.
            foreach (IntPtr handle in handles)
            {
                NativeMethods.NewTopicDestroy(handle);
            }
        }

        return new CreateTopicsResult(operation.Tasks);
    }

    /// <summary>
    /// Validates and converts Java's <c>close(Duration)</c> timeout, then closes. Shared
    /// by both public clients so the guard and the millisecond conversion exist once.
    /// </summary>
    /// <param name="timeout">
    /// How long to wait for the background task. <see cref="TimeSpan.Zero"/> is valid.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    internal Task Close(TimeSpan timeout)
    {
        // Validate BEFORE the native call (ffi §B5). A negative timeout must not simply
        // be forwarded: the ABI reads a negative timeout_ms as "wait indefinitely", so
        // passing one through would turn a caller mistake into an unbounded wait.
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "Timeout must not be negative.");
        }

        return Close((long)timeout.TotalMilliseconds);
    }

    /// <summary>
    /// Closes the client, awaiting the background task for up to
    /// <paramref name="timeoutMs"/> (negative = wait indefinitely, Java's no-argument
    /// <c>close()</c>), then releases the handle. Idempotent: a second call is a no-op.
    /// </summary>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    internal async Task Close(long timeoutMs)
    {
        if (!TryBeginClose())
        {
            // A prior teardown already won the latch — closing again would double-close.
            return;
        }

        try
        {
            await CloseInternal(timeoutMs).ConfigureAwait(false);
        }
        finally
        {
            // Requests ReleaseHandle → AdminClient_destroy. It runs when the reference
            // count reaches zero, which is immediately when nothing is in flight and
            // deferred to the last in-flight operation's callback otherwise.
            _handle.Dispose();
        }
    }

    /// <summary>
    /// The graceful asynchronous teardown: <c>close_async</c> (which joins the
    /// background task) then the handle release. Unlike <see cref="Close(long)"/> a close
    /// failure is swallowed — <c>DisposeAsync</c> must not throw out of a
    /// <c>using</c> block during unwinding.
    /// </summary>
    internal async ValueTask DisposeAsync()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            await CloseInternal(UnsetTimeoutMs).ConfigureAwait(false);
        }
        catch (KafkaException)
        {
            // Teardown: surfacing a close failure from DisposeAsync would replace whatever
            // exception is already unwinding. Close(TimeSpan) is the surface that reports it.
        }
        finally
        {
            _handle.Dispose();
        }
    }

    /// <summary>
    /// The blocking teardown fallback: the <b>synchronous</b> <c>AdminClient_close</c>
    /// with Java's no-argument <c>close()</c> semantics, then the handle release.
    /// </summary>
    /// <remarks>
    /// The sync ABI is called directly — no <c>Task.Run</c>, no
    /// <c>GetAwaiter().GetResult()</c>. The wait happens inside the core's own
    /// multi-thread runtime, so the calling thread simply parks; that is the shipped
    /// sync-op precedent, not the sync-over-async this binding forbids. The handle is
    /// passed as the <see cref="SafeHandle"/> so the marshaller holds a call-scoped
    /// reference for the whole blocking call.
    /// </remarks>
    public void Dispose()
    {
        if (!TryBeginClose())
        {
            return;
        }

        try
        {
            NativeMethods.AdminClientClose(_handle, UnsetTimeoutMs);
        }
        finally
        {
            _handle.Dispose();
        }
    }

    /// <summary>
    /// Bridges <c>close_async</c> to a <see cref="Task"/> via the shared void completion
    /// bridge (admin's one genuinely single-awaiter operation, so it reuses
    /// <see cref="OperationCompletionSource"/> rather than the per-key
    /// <see cref="KeyedAdminOperation{TValue}"/>).
    /// </summary>
    private Task CloseInternal(long timeoutMs)
    {
        OperationCompletionSource context = new OperationCompletionSource();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            // Span-the-op reference, inside the try for the same reason as in
            // CreateTopics: an AddRef throw must route through AbandonBeforeSubmit.
            bool handleRefAdded = false;
            _handle.DangerousAddRef(ref handleRefAdded);
            if (handleRefAdded)
            {
                context.SetHandleRef(_handle);
            }

            NativeMethods.AdminClientCloseAsync(
                _handle.DangerousGetHandle(), timeoutMs, AdminCallbacks.Close, GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            context.AbandonBeforeSubmit();
            throw;
        }

        return context.Task;
    }

    /// <summary>
    /// Wins the one-shot teardown latch, so exactly one of
    /// <see cref="Dispose"/> / <see cref="DisposeAsync"/> / <see cref="Close(long)"/>
    /// performs the close and the handle release.
    /// </summary>
    private bool TryBeginClose() => Interlocked.Exchange(ref _closed, 1) == 0;

    /// <summary>The use-after-dispose guard for every RPC.</summary>
    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new ObjectDisposedException(nameof(NativeAdminClient));
        }
    }
}
