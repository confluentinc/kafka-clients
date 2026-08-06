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
/// the core's way: a concurrent <b>async op</b> (<see cref="SubscribeWithCallback"/> /
/// <see cref="SeekWithCallback"/>) is rejected by the core inline and surfaces as a
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
    // future public Close(TimeSpan) overload.
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
    internal Task SubscribeWithCallback(
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
    /// Seeks <c>(topic, partition)</c> to <paramref name="offset"/> (async). On a
    /// <c>MockConsumer</c>, seeking an <b>unassigned</b> partition is a genuine
    /// broker-free failure — the returned <see cref="Task"/> faults with a
    /// <see cref="KafkaException"/> (the void bridge's error path).
    /// </summary>
    /// <remarks>
    /// <b>Async, Java-faithful (deliberate divergence from Python's sync <c>seek</c>).</b>
    /// Java's <c>AsyncKafkaConsumer.seek()</c> returns <c>void</c> but calls a blocking
    /// cross-thread <c>applicationEventHandler.addAndGet(new SeekUnvalidatedEvent(...))</c>
    /// — it blocks — so the CLAUDE.md idiom map maps it to a <see cref="Task"/> (§4). We
    /// promote the existing async mechanism over <c>Consumer_seek_async</c> unchanged
    /// (never the sync <c>Consumer_seek</c>). Python exposes <c>seek</c> synchronously;
    /// that is a deliberate Python divergence, and we choose Java fidelity.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partition"/> is negative, or <paramref name="offset"/> is negative
    /// (Java: <c>"seek offset must not be a negative number"</c>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task SeekWithCallback(
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

        // Java-fidelity: AsyncKafkaConsumer.seek() throws IllegalArgumentException
        // ("seek offset must not be a negative number") on offset < 0 BEFORE the
        // blocking addAndGet. Per the CLAUDE.md idiom map (IllegalArgumentException →
        // ArgumentOutOfRangeException, validated before the FFI call) and locked
        // PLAN decision 11 — the exact message is asserted by the tests (DoD §3).
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset), offset, "seek offset must not be a negative number");
        }

        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
        {
            using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
            NativeMethods.ConsumerSeekAsync(consumer, topicPin.Pointer, partition, offset, callback, userData);
        });
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
    /// Polls for records (async) — the M3/P3 proof of the <b>owned-handle</b> completion
    /// bridge (ffi §B6/§B7). The returned <see cref="Task{TResult}"/> resolves with an
    /// owned <see cref="ConsumerRecords"/> (copied out of the native batch on the
    /// dispatcher thread, §6.4) — a non-null result with <c>Count == 0</c> for an empty
    /// poll — or faults with a <see cref="KafkaException"/> on failure (e.g. a
    /// <c>MockConsumer</c> with an injected poll error). A concurrent second op is
    /// rejected by the core inline and faults the <see cref="Task"/> with a
    /// <see cref="KafkaException"/> (ConcurrentModification, ffi §B5).
    /// </summary>
    /// <param name="timeout">
    /// The poll timeout (Java <c>Duration</c> → <c>int64_t</c> ms). Must be
    /// non-negative.
    /// </param>
    /// <param name="cancellationToken">Best-effort cancellation → <c>wakeup()</c> (ffi §B7).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was already canceled.</exception>
    internal Task<ConsumerRecords> PollWithCallback(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        // Precondition BEFORE any P/Invoke (ffi §B5): a negative timeout is a
        // programmer error, not a Kafka outcome.
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout), timeout, "Timeout must not be negative.");
        }

        long timeoutMs = (long)timeout.TotalMilliseconds;

        return SubmitOperation<ConsumerRecords>(cancellationToken, (consumer, callback, userData) =>
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

        ThrowIfClosed();

        IntPtr error = IntPtr.Zero;
        WithPinnedTopics(topicPartitions.Count, i => topicPartitions[i].Topic, partitions, (pointers, parts, cnt) =>
            error = NativeMethods.ConsumerAssign(_handle.DangerousGetHandle(), pointers, parts, cnt));

        KafkaException? failure = KafkaException.FromHandle(error);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Queues a record on a <c>MockConsumer</c> (a broker-free driver; the partition
    /// must already be assigned via <see cref="Assign"/>). <paramref name="key"/> /
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
                    _handle.DangerousGetHandle(),
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
            error = NativeMethods.MockConsumerSetPollError(_handle.DangerousGetHandle(), messagePin.Pointer);
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
    /// <b>Accepted residual (single-owner):</b> the closed-flag check and the
    /// <c>DangerousGetHandle()</c> deref are not atomic, so a concurrent teardown that
    /// runs between them could free the handle first — a check-then-use TOCTOU
    /// (use-after-free) reachable only under cross-thread misuse. Accepted-by-design
    /// under the not-thread-safe contract; the canonical <c>wakeup()</c> usage
    /// (thread A blocked, thread B wakes it, thread A then disposes) does not race
    /// wakeup against dispose. Any future hardening (per-call
    /// <c>SafeHandle.DangerousAddRef</c>) renumbers to N≥8 (M3/P3 took N=7).
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
    /// <see cref="Wakeup"/> (accepted-by-design; N≥8 if ever hardened — M3/P3 took N=7).
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
    /// <see cref="InvalidOperationException"/>). <b>Accepted residual:</b> the same
    /// check-then-use handle TOCTOU vs teardown as <see cref="Wakeup"/> (accepted-by-
    /// design; a candidate N=9 follow-up if ever hardened).
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
    /// Fetches the owned group-metadata handle, mapping the core's concurrent-access
    /// rejection (a null handle) to <see cref="InvalidOperationException"/> (ffi §B5,
    /// CLAUDE.md §3). Shared by <see cref="GroupId"/> and <see cref="GroupMetadata"/>.
    /// The caller owns the returned non-null handle and must destroy it exactly once.
    /// </summary>
    private IntPtr GetGroupMetadataHandleOrThrow()
    {
        return ThrowIfConcurrentNull(NativeMethods.ConsumerGroupMetadata(_handle.DangerousGetHandle()));
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
    /// <see cref="GroupMetadata"/> mapping (ffi §B5, CLAUDE.md §3). <b>Accepted residual:</b>
    /// the same check-then-use handle TOCTOU vs teardown as <see cref="Wakeup"/> /
    /// <see cref="GroupMetadata"/> (accepted-by-design under the not-thread-safe contract).
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for multi-threaded
    /// access).
    /// </exception>
    internal IReadOnlyCollection<TopicPartition> Assignment()
    {
        ThrowIfClosed();

        IntPtr list = ThrowIfConcurrentNull(NativeMethods.ConsumerAssignment(_handle.DangerousGetHandle()));

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
    /// Same concurrency contract and accepted residual as <see cref="Assignment"/> (null
    /// handle → <see cref="InvalidOperationException"/>).
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">
    /// The core rejected concurrent access (the consumer is not safe for multi-threaded
    /// access).
    /// </exception>
    internal IReadOnlyCollection<string> Subscription()
    {
        ThrowIfClosed();

        IntPtr list = ThrowIfConcurrentNull(NativeMethods.ConsumerSubscription(_handle.DangerousGetHandle()));
        return StringListMarshal.CopyOutAndDestroy(list);
    }

    /// <summary>
    /// Returns the currently paused partitions (Java <c>paused()</c>) — a <b>synchronous
    /// state read</b>. Marshals an owned (Category-3) <c>TopicPartitionList_t</c>
    /// borrow-root into an owned <see cref="TopicPartition"/> snapshot and frees the root
    /// exactly once (§B2/§B3 via <see cref="TopicPartitionListMarshal"/>).
    /// </summary>
    /// <remarks>
    /// Same concurrency contract and accepted residual as <see cref="Assignment"/>. A
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

        IntPtr list = ThrowIfConcurrentNull(NativeMethods.ConsumerPaused(_handle.DangerousGetHandle()));
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
            error = NativeMethods.ConsumerEnforceRebalance(_handle.DangerousGetHandle(), IntPtr.Zero);
        }
        else
        {
            using Utf8Marshal.PinnedUtf8String reasonPin = Utf8Marshal.Pin(reason);
            error = NativeMethods.ConsumerEnforceRebalance(_handle.DangerousGetHandle(), reasonPin.Pointer);
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
    /// (surfacing it is <see cref="CloseWithCallback"/> / the public <c>Close()</c>'s job).
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
        // on null vs no-op / clear on empty), so we do NOT reject empty. Snapshot the
        // (topic, partition) pairs while validating.
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
                throw new ArgumentException(
                    "Topic names must not be null.", nameof(partitions));
            }

            if (tp.Partition < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(partitions), tp.Partition, "Partition must not be negative.");
            }

            snapshot[index++] = (tp.Topic, tp.Partition);
        }

        int count = snapshot.Length;
        int[] partitionArray = new int[count];
        for (int i = 0; i < count; i++)
        {
            partitionArray[i] = snapshot[i].Partition;
        }

        return SubmitVoidOperation(cancellationToken, (consumer, callback, userData) =>
            WithPinnedTopics(count, i => snapshot[i].Topic, partitionArray, (pointers, parts, cnt) =>
                submit(consumer, pointers, parts, cnt, callback, userData)));
    }

    /// <summary>
    /// Submits an owned-handle (result-returning) async op — the poll analog of
    /// <see cref="SubmitVoidOperation"/>. Roots the per-op context via a
    /// <see cref="GCHandle"/> (invariant #1), wires cancellation, then runs
    /// <paramref name="submit"/> (which P/Invokes with the poll callback). Ownership of
    /// the <see cref="GCHandle"/> transfers to the completion callback (the sole owner
    /// of its free, invariant #2) the moment native is entered; if
    /// <paramref name="submit"/> throws before that, the context is abandoned (handle
    /// freed) here. The marshalling of the result happens in the callback on the
    /// dispatcher thread (ffi §6.4), not here.
    /// </summary>
    private Task<TResult> SubmitOperation<TResult>(
        CancellationToken cancellationToken,
        NativeResultSubmit submit)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        OperationCompletionSource<TResult> context = new OperationCompletionSource<TResult>();
        GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
        context.SetGcHandle(gcHandle);
        try
        {
            context.RegisterCancellation(cancellationToken, Wakeup);
            submit(_handle.DangerousGetHandle(), ConsumerCallbacks.Poll, GCHandle.ToIntPtr(gcHandle));
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
    /// of <see cref="SubmitOperation{TResult}"/> (M5/P2). Roots the per-op context via a
    /// <see cref="GCHandle"/> (invariant #1), wires cancellation, then runs
    /// <paramref name="submit"/> (which pins its args call-scoped and P/Invokes with the
    /// scalar callback). Ownership of the <see cref="GCHandle"/> transfers to the
    /// completion callback (the sole owner of its free, invariant #2) the moment native is
    /// entered; if <paramref name="submit"/> throws before that, the context is abandoned
    /// (handle freed) here. A line-for-line clone of <see cref="SubmitOperation{TResult}"/>
    /// with <see cref="ConsumerCallbacks.Poll"/> → <see cref="ConsumerCallbacks.Position"/>
    /// — added as a parallel helper (rather than generalizing
    /// <see cref="SubmitOperation{TResult}"/> to take the callback type as a parameter) so
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
        Func<IntPtr, IntPtr, int, long, IntPtr> update)
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
            error = update(_handle.DangerousGetHandle(), topicPin.Pointer, partition, offset);
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
