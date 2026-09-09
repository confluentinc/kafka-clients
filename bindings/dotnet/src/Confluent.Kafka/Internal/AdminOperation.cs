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
/// admin RPC resolves <b>N</b> awaiters — one per key — from one aggregate callback,
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
    /// (<see cref="Interlocked"/>-guarded), so the inline-callback and
    /// <see cref="AbandonBeforeSubmit"/> paths cannot double-free. Releasing the
    /// reference here — at operation completion — is what lets a deferred
    /// <c>AdminClient_destroy</c> finally run.
    /// </summary>
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
/// <b>Deviation, recorded (<c>definition-of-done.md</c> §7).</b> Per-key
/// <em>granularity</em> is fully preserved: each <see cref="Task"/> carries exactly
/// that key's value or that key's error. Per-key <em>timing independence</em> is not —
/// all N complete at the same instant, because the ABI resolved them together and the
/// C ABI has no <c>KafkaFuture</c> type to express independent timing. In Java a fast
/// topic's future can complete before a slow one's. Nothing observable depends on this
/// for correctness, and the Python sibling has the identical limitation for the
/// identical reason.
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
    /// Resolves one key successfully when the result carries <b>no value</b> — result
    /// shape 2, where Java's per-key future is <c>KafkaFuture&lt;Void&gt;</c> and the
    /// ABI exposes no <c>_get_value</c> at all, so a null error <em>is</em> the success
    /// value. The value-carrying base cannot fabricate a <typeparamref name="TValue"/>,
    /// so it defers to <see cref="VoidKeyedAdminOperation{TKey}"/>; a shape-1 operation
    /// never reaches here.
    /// </summary>
    internal virtual void CompleteWithSuccessNoValue(TKey key)
    {
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
}

/// <summary>
/// The <b>void</b> per-key specialization (result shape 2): a
/// <c>KeyedAdminOperation&lt;TKey, bool&gt;</c> whose success carries no value, mirroring
/// the consumer bridge's <see cref="OperationCompletionSource"/> / <c>&lt;bool&gt;</c>
/// precedent. Used by every RPC whose Java per-key future is
/// <c>KafkaFuture&lt;Void&gt;</c> — for those the ABI exposes no <c>_get_value</c>, so
/// a null per-key error <em>is</em> the success value.
/// </summary>
/// <typeparam name="TKey">The managed per-key key type.</typeparam>
internal sealed class VoidKeyedAdminOperation<TKey> : KeyedAdminOperation<TKey, bool>
    where TKey : notnull
{
    /// <inheritdoc cref="KeyedAdminOperation{TKey, TValue}(string, IReadOnlyCollection{TKey}, IEqualityComparer{TKey})"/>
    internal VoidKeyedAdminOperation(
        string operationName,
        IReadOnlyCollection<TKey> keys,
        IEqualityComparer<TKey> keyComparer)
        : base(operationName, keys, keyComparer)
    {
    }

    /// <inheritdoc/>
    internal override void CompleteWithSuccessNoValue(TKey key) => SetResult(key, true);
}
