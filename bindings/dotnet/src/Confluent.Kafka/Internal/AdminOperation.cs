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
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The rooting half of a per-operation admin bridge context: it owns the
/// <see cref="GCHandle"/> that keeps this context alive for native across the whole
/// operation, and the <b>span-the-op</b> reference on the client's
/// <see cref="SafeHandle"/> that keeps the native client alive for exactly as long.
/// The completion half — how an operation's awaiters are resolved — is the derived
/// <see cref="KeyedAdminOperation{TKey, TValue}"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why admin needs its own rooting type at all.</b> The consumer's
/// <see cref="OperationCompletionSource{TResult}"/> couples the same rooting
/// machinery to a <em>single</em> <see cref="TaskCompletionSource{TResult}"/>; an
/// admin RPC resolves <b>N</b> awaiters — one per key — from N independent callbacks,
/// so the completion half differs even though every rooting invariant is identical.
/// (The admin <c>close</c>, which really is a single void completion, reuses
/// <see cref="OperationCompletionSource"/> unchanged rather than duplicating it here.)
/// </para>
/// <para>
/// <b>Rooting (ffi §B6).</b> From submit until the callback fires — the whole
/// operation, not a synchronous call — this context is kept alive by a
/// <see cref="GCHandle"/> allocated by the submitter and passed to native as
/// <c>user_data</c>. The delegate itself is rooted separately by a
/// <c>static readonly</c> field (see <c>AdminCallbacks</c>).
/// </para>
/// <para>
/// <b>Who frees the <see cref="GCHandle"/> — admin is the hookless one-shot family.</b>
/// No admin entry point takes a <c>user_data_destroy</c> (there is no such typedef for
/// admin at all), so ffi §B6's decisive question answers "the callback": the completion
/// callback is the <b>sole owner</b> of the free, on <em>every</em> path — including
/// the two inline ones, where the callback runs synchronously on the submitting thread
/// before the entry point returns. <see cref="AbandonBeforeSubmit"/> is the only other
/// free site and is reachable only when the submitting P/Invoke threw, so native never
/// ran and the callback can never fire.
/// </para>
/// <para>
/// ⚠ <b>The span-the-op reference is load-bearing, not a nicety.</b>
/// <c>kafka_admin_AdminClient_destroy</c> is not ref-counted and does not drain, and
/// the header makes "do not destroy concurrently with an in-flight <c>_async</c>
/// operation" a caller precondition. Holding a reference on the client's
/// <c>SafeAdminHandle</c> from submit until this context is freed is what
/// upholds it: <c>ReleaseHandle</c> → <c>AdminClient_destroy</c> runs only at count
/// zero, so a <c>Dispose</c> racing an in-flight operation defers the destroy rather
/// than freeing the client under the operation. See
/// <see cref="Interop.SafeAdminHandle"/>.
/// </para>
/// </remarks>
internal abstract class AdminOperation
{
    private GCHandle _gcHandle;
    private int _gcHandleFreed;
    private int _pendingCallbacks;
    private SafeHandle? _handleRef;

    /// <summary>
    /// Records the <see cref="GCHandle"/> that roots this context for native. Set by
    /// the submitter immediately after allocation and <b>before</b> the P/Invoke — the
    /// header requires everything the callback needs to be published before the call,
    /// because the callback can fire inline on the submitting thread.
    /// </summary>
    internal void SetGcHandle(GCHandle handle) => _gcHandle = handle;

    /// <summary>
    /// Records the client <see cref="SafeHandle"/> whose reference count the submitter
    /// bumped (<see cref="SafeHandle.DangerousAddRef(ref bool)"/>) for the lifetime of
    /// this operation. <see cref="FreeGcHandle"/> releases it exactly once on
    /// completion. Set by the submitter immediately after <see cref="SetGcHandle"/>.
    /// </summary>
    internal void SetHandleRef(SafeHandle handle) => _handleRef = handle;

    /// <summary>
    /// Arms the per-key countdown with the number of callbacks native will make, plus
    /// <b>one token for the submit itself</b>. Called before the P/Invoke, like
    /// <see cref="SetGcHandle"/>.
    /// </summary>
    /// <param name="callbacks">
    /// The callback count taken from <em>that RPC's own ABI doc comment</em> — not the key
    /// count and not blindly the request-array length (<c>incremental_alter_configs</c>
    /// fires once per distinct resource; <c>create_topics</c> fires <c>count</c> times
    /// minus NULL entries).
    /// </param>
    /// <remarks>
    /// ⚠ <b>The submit token is how <paramref name="callbacks"/> == 0 is made safe, once,
    /// here</b> — an empty key collection means native never calls back, so a bare
    /// countdown would never reach zero and the <see cref="GCHandle"/> plus the
    /// span-the-op reference would be held for the process lifetime, deferring
    /// <c>AdminClient_destroy</c> forever. The submitter always owns one release
    /// (<see cref="ReleaseSubmitToken"/>, after the P/Invoke returns), so zero callbacks
    /// releases at the submit boundary and no per-RPC call site decides anything. It also
    /// keeps the client reference held across the submit when every callback fires inline
    /// on the submitting thread.
    /// </remarks>
    internal void SetPendingCallbacks(int callbacks) =>
        Volatile.Write(ref _pendingCallbacks, (callbacks < 0 ? 0 : callbacks) + 1);

    /// <summary>
    /// One per-key callback has finished resolving its own key. At zero — the last of the
    /// N callbacks and the submit token, in whatever order they land — the operation is
    /// finalized (<see cref="OnAllCallbacksComplete"/>) and then released.
    /// </summary>
    internal void ReleaseOne()
    {
        if (Interlocked.Decrement(ref _pendingCallbacks) == 0)
        {
            try
            {
                OnAllCallbacksComplete();
            }
            finally
            {
                FreeGcHandle();
            }
        }
    }

    /// <summary>
    /// The submitter's own release, owed <b>after the P/Invoke returns</b> on the
    /// non-throwing path. See <see cref="SetPendingCallbacks"/> for why it exists.
    /// </summary>
    internal void ReleaseSubmitToken() => ReleaseOne();

    /// <summary>
    /// Cleanup for the case where the submitting P/Invoke threw before native could
    /// have fired the callback (so ownership never transferred): frees the
    /// <see cref="GCHandle"/> and releases the span-the-op reference. The awaiters are
    /// faulted by the submitter's rethrow. Safe with respect to the "callback is the
    /// sole owner" invariant precisely because native never ran here.
    /// </summary>
    internal void AbandonBeforeSubmit() => FreeGcHandle();

    /// <summary>
    /// Frees the rooting <see cref="GCHandle"/> and releases the span-the-op client
    /// reference exactly once, on every completion path. Idempotent
    /// (<see cref="Interlocked"/>-guarded), so the inline-callback,
    /// <see cref="ReleaseOne"/>-countdown and <see cref="AbandonBeforeSubmit"/> paths
    /// cannot double-free. Releasing the reference here — at operation completion — is
    /// what lets a deferred <c>AdminClient_destroy</c> finally run.
    /// </summary>
    /// <remarks>
    /// Still the direct free site for every aggregate-callback RPC, whose single callback
    /// is its own countdown of one.
    /// </remarks>
    internal void FreeGcHandle()
    {
        if (Interlocked.Exchange(ref _gcHandleFreed, 1) == 0)
        {
            if (_gcHandle.IsAllocated)
            {
                _gcHandle.Free();
            }

            _handleRef?.DangerousRelease();
            _handleRef = null;
        }
    }

    /// <summary>
    /// Runs once, when the last per-key callback and the submit token have both landed —
    /// the only point at which "a key has no outcome" can be distinguished from "a key's
    /// callback has not arrived yet".
    /// </summary>
    protected virtual void OnAllCallbacksComplete()
    {
    }
}

/// <summary>
/// The per-key completion bridge for a multi-key admin RPC: <b>one</b>
/// <see cref="TaskCompletionSource{TResult}"/> per key, all created up front so the
/// C# method can return its Java-shaped <c>*Result</c> synchronously, and all resolved
/// by the single aggregate completion callback.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why one callback can serve N awaiters.</b> The ABI's <c>*Result_t</c> is fully
/// settled when the callback fires — the core harvests every per-key future and awaits
/// the join before enqueuing the completion job — so a single pass over the result
/// table carries every key's own outcome.
/// </para>
/// <para>
/// ⚠ <b><typeparamref name="TKey"/> is generic because the ABI's key is not always a
/// string, and not always a single accessor (M15/P2a).</b> Java keys these results by
/// topic <em>name</em> (<c>createTopics</c>), by <em>topic id</em>
/// (<c>deleteTopics(TopicCollection.ofTopicIds(…))</c> → <c>Map&lt;Uuid, …&gt;</c>), and
/// by <em>topic-partition</em> (<c>deleteRecords</c>). The last one is why the key is
/// read by a <c>Func&lt;IntPtr, int, TKey&gt;</c> over the result handle and the index
/// rather than parsed from a string: <c>kafka_admin_DeleteRecordsResult_t</c> has
/// <b>no</b> <c>get_key</c> at all — its key is composed from <c>get_topic(i)</c> and
/// <c>get_partition(i)</c> — so a string-to-key parser could not express it. See
/// <see cref="Interop.KeyedResultMarshal"/>.
/// </para>
/// <para>
/// ⚠ <b>The key comparer is a required constructor argument, never inferred.</b> Falling
/// back to <see cref="EqualityComparer{T}.Default"/> would silently give string keys
/// culture-sensitive-looking behaviour that differs from the <see cref="StringComparer"/>
/// the public <c>*Result</c> views use, and the two would then disagree about whether a
/// key is present. Every caller passes the comparer the matching <c>*Result</c> will use,
/// and <see cref="KeyComparer"/> hands it back so the result type cannot pick a different
/// one.
/// </para>
/// <para>
/// Per-key <em>granularity</em> and per-key <em>timing independence</em> are both
/// preserved: each <see cref="Task"/> carries exactly that key's value or that key's
/// error, and it resolves when that key's own callback fires — so a fast topic's
/// awaiter completes before a slow one's, as in Java. (Before M15/P9 the ABI resolved
/// every key from one aggregate callback and the timing half was a recorded deviation;
/// the per-key ABI removed it.)
/// </para>
/// <para>
/// <b>Foreign-thread completion.</b> Every source is built with
/// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>. This is
/// <b>mandatory</b>, and sharper here than for the consumer: an admin callback can fire
/// <em>synchronously on the submitting thread, before the entry point returns</em> — so
/// without it an awaiter's continuation would run inside the caller's own P/Invoke. The
/// header documents that inline path for every admin entry point (trigger: a NULL
/// <c>admin</c> handle; the by-id entry points add "an unparseable or NULL base64 topic
/// id"), and the wider family-wide trigger set is stated in
/// <c>src/ffi/admin.rs:56-63</c>, which cbindgen does not emit — see
/// <see cref="Interop.AdminCallbacks"/>.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The managed per-key key type — <c>string</c> or <see cref="Uuid"/> today.</typeparam>
/// <typeparam name="TValue">The already-marshalled managed per-key result type.</typeparam>
internal class KeyedAdminOperation<TKey, TValue> : AdminOperation
    where TKey : notnull
{
    private readonly Dictionary<TKey, TaskCompletionSource<TValue>> _perKey;
    private readonly Dictionary<TKey, Task<TValue>> _tasks;
    private readonly IEqualityComparer<TKey> _keyComparer;
    private readonly string _operationName;

    /// <summary>
    /// Creates one source per distinct key in <paramref name="keys"/>, before anything
    /// is submitted.
    /// </summary>
    /// <param name="operationName">
    /// The Java method name (e.g. <c>createTopics</c>), used only to make the
    /// "result carried no entry" message name the operation it came from.
    /// </param>
    /// <param name="keys">
    /// The requested keys, already de-duplicated by the caller (Java keys its result on
    /// a <c>Map</c>, so a repeated key is one entry).
    /// </param>
    /// <param name="keyComparer">
    /// The equality comparer for <typeparamref name="TKey"/> — required, so it can never
    /// silently fall back to <see cref="EqualityComparer{T}.Default"/>. See the type
    /// remarks.
    /// </param>
    internal KeyedAdminOperation(
        string operationName,
        IReadOnlyCollection<TKey> keys,
        IEqualityComparer<TKey> keyComparer)
    {
        _operationName = operationName;
        _keyComparer = keyComparer;
        _perKey = new Dictionary<TKey, TaskCompletionSource<TValue>>(keys.Count, keyComparer);
        _tasks = new Dictionary<TKey, Task<TValue>>(keys.Count, keyComparer);
        foreach (TKey key in keys)
        {
            TaskCompletionSource<TValue> source =
                new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);
            _perKey[key] = source;
            _tasks[key] = source.Task;
        }
    }

    /// <summary>
    /// The per-key awaitables, in the Java <c>Map&lt;K, KafkaFuture&lt;V&gt;&gt;</c>
    /// shape the public <c>*Result</c> exposes. Populated before the submit, so the
    /// synchronous C# method can hand it back immediately.
    /// </summary>
    internal IReadOnlyDictionary<TKey, Task<TValue>> Tasks => _tasks;

    /// <summary>
    /// The comparer <see cref="Tasks"/> is keyed by, so a <c>*Result</c> deriving a view
    /// from it uses the same one and the two cannot disagree about key identity.
    /// </summary>
    internal IEqualityComparer<TKey> KeyComparer => _keyComparer;

    /// <summary>Resolves one key successfully with its marshalled value.</summary>
    internal void SetResult(TKey key, TValue value)
    {
        if (_perKey.TryGetValue(key, out TaskCompletionSource<TValue>? source))
        {
            source.TrySetResult(value);
        }
    }

    /// <summary>Faults one key with its own error.</summary>
    internal void SetException(TKey key, Exception exception)
    {
        if (_perKey.TryGetValue(key, out TaskCompletionSource<TValue>? source))
        {
            source.TrySetException(exception);
        }
    }

    /// <summary>
    /// Faults <b>every</b> key with the same exception — the top-level submit-failure
    /// path, where the callback's <c>error</c> parameter is non-null and no result
    /// table exists, and the callback's no-throw boundary.
    /// </summary>
    internal void FailAll(Exception exception)
    {
        foreach (KeyValuePair<TKey, TaskCompletionSource<TValue>> entry in _perKey)
        {
            entry.Value.TrySetException(exception);
        }
    }

    /// <summary>
    /// Faults any key the result did not account for, so <b>no <see cref="Task"/> can
    /// ever hang</b>. A no-op on every normal path (the ABI returns one entry per
    /// requested key); it exists because a caller holding a never-completing
    /// <see cref="Task"/> is a worse failure than an explicit error.
    /// </summary>
    internal void FailUncompleted()
    {
        foreach (KeyValuePair<TKey, TaskCompletionSource<TValue>> entry in _perKey)
        {
            if (!entry.Value.Task.IsCompleted)
            {
                entry.Value.TrySetException(new KafkaException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "The {0} result contained no entry for '{1}'.",
                        _operationName,
                        entry.Key)));
            }
        }
    }

    /// <summary>
    /// ⚠ <b>Countdown zero, never per callback.</b> Faulting an unaccounted key while other
    /// keys' callbacks are still in flight would fault keys that are merely late.
    /// </summary>
    protected override void OnAllCallbacksComplete() => FailUncompleted();
}

/// <summary>
/// The <b>void</b> per-key specialization (result shape 2): a
/// <c>KeyedAdminOperation&lt;TKey, bool&gt;</c> whose success carries no value, mirroring
/// the consumer bridge's <see cref="OperationCompletionSource"/> / <c>&lt;bool&gt;</c>
/// precedent. Used by every RPC whose Java per-key future is
/// <c>KafkaFuture&lt;Void&gt;</c> — for those the ABI exposes no <c>_get_value</c>, so
/// a null per-key error <em>is</em> the success value.
/// </summary>
/// <remarks>
/// ⚠ <b>This type is load-bearing, not a convenience alias (M15/P2b).</b> It is what
/// <see cref="Interop.KeyedResultMarshal"/>'s value-less <c>Complete</c> overload accepts,
/// so "this result has no per-key value" is a fact the <em>type system</em> carries. A
/// value-carrying <see cref="KeyedAdminOperation{TKey, TValue}"/> cannot be routed down
/// that path and have its value silently dropped — which is exactly what a nullable value
/// channel allowed before P2b removed it.
/// </remarks>
/// <typeparam name="TKey">The managed per-key key type.</typeparam>
internal sealed class VoidKeyedAdminOperation<TKey> : KeyedAdminOperation<TKey, bool>
    where TKey : notnull
{
    private IReadOnlyCollection<TKey> _keysWithNoRequest = Array.Empty<TKey>();

    /// <inheritdoc cref="KeyedAdminOperation{TKey, TValue}(string, IReadOnlyCollection{TKey}, IEqualityComparer{TKey})"/>
    internal VoidKeyedAdminOperation(
        string operationName,
        IReadOnlyCollection<TKey> keys,
        IEqualityComparer<TKey> keyComparer)
        : base(operationName, keys, keyComparer)
    {
    }

    /// <summary>
    /// Records the keys that the ABI request could not carry, so the completion can resolve
    /// them locally instead of leaving them for <c>FailUncompleted</c>.
    /// </summary>
    /// <param name="keys">The keys that contributed no row to the request.</param>
    /// <remarks>
    /// ⚠ <b>This exists for one shape: a per-key result whose key set comes from the
    /// caller's map while the ABI request is ROW-flattened</b>, so a key mapped to an empty
    /// collection flattens to zero rows and is absent from the request — and therefore from
    /// the result. Java keys its futures on the <em>resource collection</em>, which travels
    /// alongside the ops map (<c>KafkaAdminClient.java:2889-2896</c>), so such a key
    /// completes there. Without this the result would carry no entry for it and the
    /// awaitable would fault, reporting a defect where Java reports success (M15/P3 round 3,
    /// finding 69.6).
    /// <para>
    /// The divergence this leaves is enumerated at the registering call site — it is not
    /// nothing, and it must not be re-derived from this method alone.
    /// </para>
    /// </remarks>
    internal void SetKeysWithNoRequest(IReadOnlyCollection<TKey> keys) => _keysWithNoRequest = keys;

    /// <summary>
    /// Resolves every key registered by <see cref="SetKeysWithNoRequest"/> with the void
    /// success token.
    /// </summary>
    /// <remarks>
    /// ⚠ On the <b>aggregate</b> path this is called only when the call succeeded: a failed
    /// call has no result table and <c>FailAll</c> has already faulted every awaitable,
    /// including these — Java's outcome too, since the resource really is in the request it
    /// sends. Under the <b>per-key</b> ABI there is no whole-call channel, so it runs
    /// unconditionally at countdown zero and these keys succeed even when every requested
    /// key faulted; that divergence is enumerated at the registering call site.
    /// </remarks>
    internal void CompleteKeysWithNoRequest()
    {
        foreach (TKey key in _keysWithNoRequest)
        {
            SetResult(key, true);
        }
    }

    /// <summary>
    /// ⚠ <b>Countdown zero, never per callback</b> — and before the base's
    /// <c>FailUncompleted</c> sees the no-request keys, exactly as the aggregate
    /// trampoline orders the two.
    /// </summary>
    protected override void OnAllCallbacksComplete()
    {
        CompleteKeysWithNoRequest();
        base.OnAllCallbacksComplete();
    }
}

/// <summary>
/// The completion bridge for a <b>non-keyed</b> admin RPC (result shape 3): <b>one</b>
/// <see cref="TaskCompletionSource{TResult}"/> for the whole call, mirroring Java's single
/// <c>KafkaFuture&lt;Map&lt;K, V&gt;&gt;</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the per-key bridge cannot express this.</b>
/// <see cref="KeyedAdminOperation{TKey, TValue}"/> creates one source per key
/// <em>before</em> the submit, so the C# method can hand back its Java-shaped
/// <c>*Result</c> synchronously — which requires knowing the keys up front.
/// <c>kafka_admin_AdminClient_list_topics</c> takes <b>no key array</b>: the keys are
/// discovered from the response. And <c>kafka_admin_ListTopicsResult_t</c> exposes no
/// <c>get_error</c> at all, the ABI's way of saying there is no per-key failure to
/// attribute. One future is therefore the faithful shape, not a simplification of N.
/// </para>
/// <para>
/// <b>Everything about rooting is inherited unchanged.</b> The <see cref="GCHandle"/>,
/// the span-the-op <see cref="SafeHandle"/> reference,
/// <see cref="AdminOperation.AbandonBeforeSubmit"/>
/// and the free-exactly-once discipline all come from <see cref="AdminOperation"/>; only
/// the completion payload differs. The source uses
/// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> for the same reason
/// the per-key sources do — an admin callback can fire synchronously on the submitting
/// thread, so without it a continuation would run inside the caller's own P/Invoke.
/// </para>
/// </remarks>
/// <typeparam name="TValue">The already-marshalled managed result type.</typeparam>
internal sealed class SingleAdminOperation<TValue> : AdminOperation
{
    private readonly TaskCompletionSource<TValue> _source =
        new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly string _operationName;

    /// <summary>Creates the single source, before anything is submitted.</summary>
    /// <param name="operationName">
    /// The Java method name (e.g. <c>listTopics</c>), used only to make the
    /// "result was never delivered" message name the operation it came from.
    /// </param>
    internal SingleAdminOperation(string operationName)
    {
        _operationName = operationName;
    }

    /// <summary>
    /// The awaitable, in the Java <c>KafkaFuture&lt;T&gt;</c> shape the public
    /// <c>*Result</c> exposes. Available before the submit, so the synchronous C# method
    /// can hand it back immediately.
    /// </summary>
    internal Task<TValue> Task => _source.Task;

    /// <summary>Resolves the call successfully with its marshalled value.</summary>
    internal void SetResult(TValue value) => _source.TrySetResult(value);

    /// <summary>
    /// Faults the call. Used both for the callback's own top-level <c>error</c> and for
    /// any failure during the walk — shape 3 has no per-key channel, so every failure is
    /// a call failure.
    /// </summary>
    internal void SetException(Exception exception) => _source.TrySetException(exception);

    /// <summary>
    /// Faults the awaiter if nothing completed it, so the <see cref="Task"/> <b>cannot
    /// hang</b>. A no-op on every normal path; it exists because a caller holding a
    /// never-completing <see cref="Task"/> is a worse failure than an explicit error.
    /// The keyed bridge's <c>FailUncompleted</c>, for one source instead of N.
    /// </summary>
    internal void FailUncompleted()
    {
        if (!_source.Task.IsCompleted)
        {
            _source.TrySetException(new KafkaException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "The {0} call completed without delivering a result.",
                    _operationName)));
        }
    }
}

/// <summary>
/// The completion bridge for result shape <b>4c</b>: the ABI fans out <b>one callback per
/// key</b> while Java holds <b>one</b> <c>KafkaFuture&lt;Map&lt;K, V&gt;&gt;</c>. Per-key
/// arrivals are accumulated and the single awaiter is resolved once, when the last key has
/// landed.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>A per-key error is the map's VALUE here, not a fault</b> — the caller chooses
/// <typeparamref name="TValue"/> accordingly. Routing it as a fault is the
/// <c>ElectLeaders</c> mistake M15/P4 measured at 10 failing tests; the Java return type
/// decides, as always (see <see cref="Interop.KeyedResultMarshal"/>).
/// </para>
/// <para>
/// ⚠ <b>The accumulator is mutated from N callback threads, so it is locked.</b> Every
/// callback must <see cref="Add"/> <em>before</em> its
/// <see cref="AdminOperation.ReleaseOne"/>, which is what makes the countdown-zero
/// snapshot complete.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The managed key type.</typeparam>
/// <typeparam name="TValue">The managed map-value type.</typeparam>
internal sealed class FanInAdminOperation<TKey, TValue> : AdminOperation
    where TKey : notnull
{
    private readonly TaskCompletionSource<IReadOnlyDictionary<TKey, TValue>> _source =
        new TaskCompletionSource<IReadOnlyDictionary<TKey, TValue>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly Dictionary<TKey, TValue> _entries;
    private readonly IEqualityComparer<TKey> _keyComparer;
    private readonly object _gate = new object();

    /// <summary>Creates the single source, before anything is submitted.</summary>
    /// <param name="expectedKeys">The requested key count, used only to size the accumulator.</param>
    /// <param name="keyComparer">
    /// The comparer the assembled map is keyed by — required for the same reason
    /// <see cref="KeyedAdminOperation{TKey, TValue}"/>'s is.
    /// </param>
    internal FanInAdminOperation(int expectedKeys, IEqualityComparer<TKey> keyComparer)
    {
        _keyComparer = keyComparer;
        _entries = new Dictionary<TKey, TValue>(Math.Max(expectedKeys, 0), keyComparer);
    }

    /// <summary>
    /// The awaitable, in the Java <c>KafkaFuture&lt;Map&lt;K, V&gt;&gt;</c> shape.
    /// Available before the submit.
    /// </summary>
    internal Task<IReadOnlyDictionary<TKey, TValue>> Task => _source.Task;

    /// <summary>Records one key's outcome. Thread-safe; call before <see cref="AdminOperation.ReleaseOne"/>.</summary>
    /// <remarks>
    /// <c>Add</c>, not the indexer: a repeated key means the ABI fired twice for it, and
    /// faulting the one awaiter loudly beats collapsing two outcomes into one — the
    /// <see cref="Interop.KeyedResultMarshal.CompleteAggregate{TKey, TValue}"/> precedent.
    /// </remarks>
    internal void Add(TKey key, TValue value)
    {
        lock (_gate)
        {
            _entries.Add(key, value);
        }
    }

    /// <summary>
    /// Faults the one awaiter. Shape 4c has no per-key fault channel, so every failure —
    /// a submit failure or a marshalling throw — is a call failure.
    /// </summary>
    internal void SetException(Exception exception) => _source.TrySetException(exception);

    /// <summary>
    /// Resolves the single awaiter with everything accumulated. Runs once, at countdown
    /// zero, so the <see cref="Task"/> cannot hang even when native reports nothing.
    /// </summary>
    protected override void OnAllCallbacksComplete()
    {
        Dictionary<TKey, TValue> snapshot;
        lock (_gate)
        {
            snapshot = new Dictionary<TKey, TValue>(_entries, _keyComparer);
        }

        _source.TrySetResult(snapshot);
    }
}
