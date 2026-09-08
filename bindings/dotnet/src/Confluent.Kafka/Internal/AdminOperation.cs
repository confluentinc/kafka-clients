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

namespace Confluent.Kafka.Internal;

/// <summary>
/// The rooting half of a per-operation admin bridge context: it owns the
/// <see cref="GCHandle"/> that keeps this context alive for native across the whole
/// operation, and the <b>span-the-op</b> reference on the client's
/// <see cref="SafeHandle"/> that keeps the native client alive for exactly as long.
/// The completion half — how an operation's awaiters are resolved — is the derived
/// <see cref="KeyedAdminOperation{TValue}"/>.
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
/// <em>synchronously on the submitting thread, before the entry point returns</em>, on
/// ordinary bad input — so without it an awaiter's continuation would run inside the
/// caller's own P/Invoke.
/// </para>
/// </remarks>
/// <typeparam name="TValue">The already-marshalled managed per-key result type.</typeparam>
internal class KeyedAdminOperation<TValue> : AdminOperation
{
    private readonly Dictionary<string, TaskCompletionSource<TValue>> _perKey;
    private readonly Dictionary<string, Task<TValue>> _tasks;
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
    internal KeyedAdminOperation(string operationName, IReadOnlyCollection<string> keys)
    {
        _operationName = operationName;
        _perKey = new Dictionary<string, TaskCompletionSource<TValue>>(keys.Count, StringComparer.Ordinal);
        _tasks = new Dictionary<string, Task<TValue>>(keys.Count, StringComparer.Ordinal);
        foreach (string key in keys)
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
    internal IReadOnlyDictionary<string, Task<TValue>> Tasks => _tasks;

    /// <summary>Resolves one key successfully with its marshalled value.</summary>
    internal void SetResult(string key, TValue value)
    {
        if (_perKey.TryGetValue(key, out TaskCompletionSource<TValue>? source))
        {
            source.TrySetResult(value);
        }
    }

    /// <summary>Faults one key with its own error.</summary>
    internal void SetException(string key, Exception exception)
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
    /// so it defers to <see cref="VoidKeyedAdminOperation"/>; a shape-1 operation never
    /// reaches here.
    /// </summary>
    internal virtual void CompleteWithSuccessNoValue(string key)
    {
    }

    /// <summary>
    /// Faults <b>every</b> key with the same exception — the top-level submit-failure
    /// path, where the callback's <c>error</c> parameter is non-null and no result
    /// table exists, and the callback's no-throw boundary.
    /// </summary>
    internal void FailAll(Exception exception)
    {
        foreach (KeyValuePair<string, TaskCompletionSource<TValue>> entry in _perKey)
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
        foreach (KeyValuePair<string, TaskCompletionSource<TValue>> entry in _perKey)
        {
            if (!entry.Value.Task.IsCompleted)
            {
                entry.Value.TrySetException(new KafkaException(
                    $"The {_operationName} result contained no entry for '{entry.Key}'."));
            }
        }
    }
}

/// <summary>
/// The <b>void</b> per-key specialization (result shape 2): a
/// <c>KeyedAdminOperation&lt;bool&gt;</c> whose success carries no value, mirroring the
/// consumer bridge's <see cref="OperationCompletionSource"/> / <c>&lt;bool&gt;</c>
/// precedent. Used by every RPC whose Java per-key future is
/// <c>KafkaFuture&lt;Void&gt;</c> — for those the ABI exposes no <c>_get_value</c>, so
/// a null per-key error <em>is</em> the success value.
/// </summary>
internal sealed class VoidKeyedAdminOperation : KeyedAdminOperation<bool>
{
    /// <inheritdoc cref="KeyedAdminOperation{TValue}(string, IReadOnlyCollection{string})"/>
    internal VoidKeyedAdminOperation(string operationName, IReadOnlyCollection<string> keys)
        : base(operationName, keys)
    {
    }

    /// <inheritdoc/>
    internal override void CompleteWithSuccessNoValue(string key) => SetResult(key, true);
}
