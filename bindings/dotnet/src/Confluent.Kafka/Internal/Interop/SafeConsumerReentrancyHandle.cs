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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Owned handle over a <c>kafka_consumer_ConsumerHandle_t</c> — the in-callback reentrancy
/// handle returned <b>directly</b> by <c>kafka_consumer_Consumer_handle</c>. A new
/// ffi-marshalling.md §B2 category: <b>caller-owned, independently destroyed, and
/// ref-counting its parent</b> — distinct from Category 3 (owned by a callback, freed
/// in-call) and Category 5 (consumed by the callee). Like
/// <see cref="SafeConsumerHandle"/> the interop marshaller invokes the private
/// parameterless ctor and sets the handle atomically on return, so the binding never wraps
/// a raw pointer itself.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this handle ref-counts its parent (P8-D1 / roadmap Q7-D5).</b> The ABI contract is
/// explicit — "A handle is usable only while its consumer is alive. Destroy every handle
/// <b>before</b> destroying the consumer" (<c>src/ffi/consumer_handle.rs:67-70</c>) — and .NET
/// cannot force user ordering. Python documents the ordering and relies on the user
/// (<c>consumer.py:591-592</c>); .NET deliberately does not, because a long-lived raw pointer
/// outliving a concurrent destroy is exactly the use-after-free class M9/P4 closed for every
/// other path. So this handle takes a <c>DangerousAddRef</c> on the consumer's
/// <see cref="SafeConsumerHandle"/> for its whole lifetime. Since M9/P4's
/// <c>ReleaseHandle</c> runs <c>Consumer_destroy</c> only at count zero, a live reentrancy
/// handle <b>defers</b> the consumer destroy rather than dangling it. The accepted cost: a
/// <em>leaked</em> reentrancy handle defers <c>Consumer_destroy</c> indefinitely — a leak,
/// where the alternative is corruption.
/// </para>
/// <para>
/// <b>The parent reference is owned by this <see cref="System.Runtime.InteropServices.SafeHandle"/>, not by the public
/// wrapper — and that ordering is load-bearing.</b> The release happens in
/// <see cref="ReleaseHandle"/>, <em>after</em> <c>ConsumerHandle_destroy</c> returns. That is
/// what makes "the parent count is released only once every in-flight handle op has returned"
/// true by construction: every one of the 22 handle ops takes <c>this</c> as a
/// <c>SafeHandle</c> parameter, so the interop marshaller holds a call-scoped reference on
/// <em>this</em> handle for the duration of the native call, and the CLR cannot run
/// <see cref="ReleaseHandle"/> until that reference drops. Releasing the parent from the
/// public wrapper's <c>Dispose</c> instead would break it: <c>SafeHandle.Dispose</c> only
/// <em>requests</em> release, so the parent count could be dropped — and the consumer
/// destroyed — while a handle op was still blocked inside the core.
/// </para>
/// <para>
/// <b>P8-D2 — which thread may run the resulting <c>Consumer_destroy</c>, and why that is
/// safe.</b> When this handle holds the last count, its release triggers
/// <c>Consumer_destroy</c> on whatever thread disposed it. That is <b>not a new trigger</b>:
/// <see cref="SafeConsumerHandle.ReleaseHandle"/> already runs <c>Consumer_destroy</c>
/// synchronously on whatever thread called <c>Dispose</c> at count 1→0, which is the ordinary
/// clean path taken by every consumer in the binding. ffi-marshalling.md §B6's "the
/// deferred-destroy path fires from <c>FreeGcHandle</c> on the dispatcher thread" is scoped
/// to the <em>deferred</em> path precisely because the immediate path was always
/// arbitrary-thread. Neither §B6 property is weakened:
/// </para>
/// <para>
/// (1) The <b>ref-count clause holds unchanged, and it is what carries the safety.</b>
/// <c>Consumer_destroy</c> runs only at count zero. For the <b>rebalance listener</b> that
/// closes it outright: a listener callback only ever runs inside an operation that holds a
/// count, and its dispatched job is enqueued and drained inside the very operation that
/// produced it, before that operation's completion releases its count — so at the moment of
/// this type's destroy no listener callback is running and none is queued, regardless of
/// thread.
/// ⚠ <b>That "runs inside a counted operation" claim is true of the listener only — do not
/// extend it to the commit callback.</b> <c>CommitAsync(callback)</c> reaches the ABI as a
/// <em>synchronous</em> call, so its marshaller AddRef is call-scoped and is gone by the
/// time the callback fires from the dispatcher; <c>CommitCallbackRegistration</c> takes no
/// <c>DangerousAddRef</c> of its own. That family is fire-and-forget (ffi §B7) and holds no
/// managed count. It is nonetheless <b>unaffected by this type</b>, on a simpler ground:
/// the destroy here is structurally the <b>same shape as ffi §B2 path 1</b> —
/// <c>Dispose</c> at count 1, destroy on the disposing thread — which has shipped since
/// M9/P1 and was never dispatcher-covered either. Whatever makes a queued commit callback
/// safe against a path-1 destroy makes it safe against this one; this path adds no
/// exposure, only a later moment.
/// (2) The <b>dispatcher clause is not weakened, but this path does not rely on it.</b> P8
/// adds no callback, no <c>user_data_destroy</c> and no dispatcher work, so nothing about
/// the dispatcher changes. What <em>does</em> change is which holder releases last: with a
/// live handle, an in-flight async op releases at <c>FreeGcHandle</c> down to 1 and the
/// destroy then runs later from <see cref="ReleaseHandle"/> on an arbitrary thread, where
/// before P8 that same scenario destroyed on the dispatcher thread. The destroy is therefore
/// delayed <em>and relocated</em>. That is a real change to §B6's second clause for this
/// path, which is why the safety argument above rests on the <b>first</b> clause alone.
/// An arbitrary-thread destroy is in any case the <em>contracted</em> case: the core states a
/// release hook "may fire on <b>any thread</b> — whichever one drops the adapter (a tokio
/// worker ..., the dispatcher thread, or the C thread calling <c>_destroy</c>)"
/// (<c>src/ffi/common.rs:422-425</c>), which is why both hook free sites
/// (<c>ListenerRegistration.Release</c>, <c>CommitCallbackRegistration.Release</c>) are
/// already <c>Interlocked</c>-guarded and thread-agnostic.
/// </para>
/// <para>
/// ⚠ <b>Do not restate this as "adding a holder can only delay a destroy, so nothing needs
/// checking".</b> That is true but insufficient: it does not entail that the same holder
/// releases last, and §B6's second clause is about exactly that — <em>which thread</em> runs
/// the destroy. The monotonicity argument would wave through a relocation it cannot see.
/// The enumeration in <c>ListenerRegistration</c>'s remarks (path 5) is the authoritative
/// statement of this path.
/// </para>
/// </remarks>
internal sealed class SafeConsumerReentrancyHandle : SafeHandleZeroIsInvalid
{
    /// <summary>
    /// The parent consumer handle whose ref-count this handle holds, or <see langword="null"/>
    /// if the reference was never transferred in (the construction-failure path, where
    /// <see cref="ReleaseHandle"/> must free the native handle without touching the parent).
    /// </summary>
    private SafeConsumerHandle? _consumer;

    private SafeConsumerReentrancyHandle()
    {
    }

    /// <summary>
    /// <b>Adopts</b> the parent reference the caller already holds, making
    /// <see cref="ReleaseHandle"/> its single release site. Called once, immediately after
    /// construction, before the handle is published to the public wrapper.
    /// </summary>
    /// <remarks>
    /// ⚠ This does <b>not</b> take a reference of its own — it takes ownership of the caller's.
    /// <see cref="NativeConsumerHandle.Create"/> holds exactly one <c>DangerousAddRef</c>,
    /// taken <em>before</em> the native call so the consumer provably cannot be destroyed
    /// across handle creation, and hands it over here. An <c>AddRef</c> in this method as well
    /// would make it <b>two</b> references against one <see cref="ReleaseHandle"/> — the count
    /// would never reach zero, <c>Consumer_destroy</c> would never run, and every consumer that
    /// ever produced a reentrancy handle would leak its native resources for the process
    /// lifetime. That bug was real and is caught by the deferred-release tests; keep this a
    /// plain assignment.
    /// </remarks>
    /// <param name="consumer">The owning consumer's handle, whose reference this handle adopts.</param>
    internal void AdoptParentReference(SafeConsumerHandle consumer) => _consumer = consumer;

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        try
        {
            // ⚠ Raw `handle`, not `this` — the same structural exclusion from the
            // SafeHandle-param convention as SafeConsumerHandle.ReleaseHandle: passing `this`
            // would make the marshaller DangerousAddRef a handle that is already mid-release.
            NativeMethods.ConsumerHandleDestroy(handle);
        }
        finally
        {
            // Release the parent count AFTER the native destroy, and on every path (including
            // a throwing destroy) — the ABI's "destroy every handle before destroying the
            // consumer" ordering, enforced rather than documented. This may be the last count,
            // in which case Consumer_destroy runs here, on this thread (P8-D2, see remarks).
            _consumer?.DangerousRelease();
        }

        return true;
    }
}
