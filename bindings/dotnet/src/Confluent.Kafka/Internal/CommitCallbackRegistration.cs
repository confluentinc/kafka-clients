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
using System.Runtime.InteropServices;
using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The managed context behind one <b>offset-commit callback registration</b> — the
/// <c>user_data</c> the commit trampoline recovers when the commit completes. Rooted for the
/// life of the registration by a <see cref="GCHandle"/> whose <see cref="IntPtr"/> form is what
/// the ABI actually carries.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is a ONE-SHOT completion with a MULTI-SHOT free site — a third family, not either
/// of the two already in the binding.</b> The callback fires <em>exactly once per successful
/// call</em> (<c>confluent_kafka.h:2466-2467</c>), so it belongs to the one-shot family with
/// <see cref="OperationCompletionSource"/> and the seven other async ops. But the one-shot
/// rule's <em>free site</em> — "the callback frees the <see cref="GCHandle"/>" — is
/// <b>wrong here</b>, and using it would leak. See below; ffi-marshalling.md §B6 now carries
/// this as its own Rule.
/// </para>
/// <para>
/// <b>Why the callback is the wrong free site (the evidence).</b> The existing one-shot ops
/// have no <c>user_data_destroy</c> hook at all, and their callback fires on <em>every</em>
/// path including the core's inline guard rejection — so "the callback frees it" is total for
/// them. This registration has a hook, and its callback is <b>not</b> invoked on failure:
/// <c>src/ffi/consumer.rs:4001-4009</c> builds the callback adapter (which takes ownership of
/// <c>user_data</c>) at <c>:4004</c>, <em>before</em> the fallible <c>read_offset_map</c> at
/// <c>:4005</c>, precisely so that the early return at <c>:4007</c> drops the adapter and
/// fires the hook. The header states the consequence outright: "if the offsets fail to marshal
/// (e.g. a negative offset) this returns the error <b>without registering the callback</b> —
/// the callback never fires, but <c>user_data_destroy</c> still does", and, for both entry
/// points, that the hook "fires <b>even when this function returns an error</b> (the transfer
/// is unconditional)".
/// </para>
/// <para>
/// <b>Therefore <see cref="Release"/>, driven only by the ABI's <c>user_data_destroy</c> hook,
/// is the single sanctioned free site</b> — the only one that runs on every path. Freeing from
/// the trampoline would leak this <see cref="GCHandle"/> on the marshal-failure path, and on
/// the success path would free it while the core still holds the raw <c>user_data</c> pointer
/// it is about to hand to the hook.
/// </para>
/// <para>
/// <b>Why freeing from the hook is safe.</b> Same two properties as
/// <see cref="ListenerRegistration"/>, and for the same reason — <b>not</b> an owned
/// <c>Arc</c> held across the callback, which is disproved (<c>FfiRebalanceListener::invoke</c>
/// copies the pointer out before dispatching, <c>src/ffi/consumer.rs:3122</c>): (1) the
/// <b>ref-counted <c>SafeConsumerHandle</c></b> (M9/P4 H1) — the submitting call takes a
/// call-scoped marshaller AddRef, and on a <c>MockConsumer</c> the callback fires inline
/// inside that call; and (2) the core's <b>single serialised dispatcher thread</b> — a real
/// consumer's callback and the M9/P4 deferred-destroy path both run on it, so they are
/// serialised rather than concurrent. <b>If either property is ever weakened, this choice must
/// be re-derived.</b>
/// </para>
/// <para>
/// <b>Accepted residual.</b> <c>Consumer_destroy</c> tears the runtime down with
/// <c>runtime.shutdown_background()</c>, which does not specify whether a cancelled task is
/// dropped or leaked. If it is leaked the registration is retained, the hook never fires, and
/// this <see cref="GCHandle"/> stays allocated for the process lifetime — a bounded leak, not
/// a use-after-free. If it is dropped the hook fires and frees, which is safe by (1)/(2)
/// above. Identical to the residual recorded on <see cref="ListenerRegistration"/>.
/// </para>
/// <para>
/// The <b>no-callback</b> commit (Java's <c>commitAsync(Map, null)</c>) has no registration at
/// all: it passes a null <c>user_data</c> and a null hook, and the discard trampoline reads
/// neither. See <c>ConsumerCallbacks.CommitDiscard</c>.
/// </para>
/// </remarks>
internal sealed class CommitCallbackRegistration
{
    private GCHandle _gcHandle;

    // 0 = rooted, 1 = released. Interlocked because the release hook "may run on any thread"
    // (confluent_kafka.h:521-537) — the app thread making the call when the marshal fails, a
    // consumer worker, or the dispatcher thread. A second GCHandle.Free() is a hard failure,
    // so the exchange is what makes the single free site idempotent-safe alongside the
    // "native never ran" abandon path.
    private int _released;

    private CommitCallbackRegistration(IOffsetCommitCallback callback)
    {
        Callback = callback;
    }

    /// <summary>The user-supplied callback the commit trampoline dispatches to.</summary>
    internal IOffsetCommitCallback Callback { get; }

    /// <summary>
    /// The <c>user_data</c> pointer handed to <c>Consumer_commit_async_with_callback</c> /
    /// <c>Consumer_commit_async_offsets_with_callback</c>. Valid only between
    /// <see cref="Root"/> and <see cref="Release"/>.
    /// </summary>
    internal IntPtr UserData => GCHandle.ToIntPtr(_gcHandle);

    /// <summary>
    /// Whether the core has released this registration (the hook fired). Observable so tests
    /// can pin the free site on <b>both</b> paths — the successful commit, and the
    /// marshal-failure path where the callback never fires at all.
    /// </summary>
    internal bool IsReleased => Volatile.Read(ref _released) != 0;

    /// <summary>
    /// Allocates the rooting <see cref="GCHandle"/>. Called before the registration is handed
    /// to native, so <see cref="UserData"/> is valid by the time the callback or the release
    /// hook can observe it.
    /// </summary>
    internal static CommitCallbackRegistration Root(IOffsetCommitCallback callback)
    {
        CommitCallbackRegistration registration = new CommitCallbackRegistration(callback);
        registration._gcHandle = GCHandle.Alloc(registration, GCHandleType.Normal);
        return registration;
    }

    /// <summary>
    /// Recovers a registration from the <c>user_data</c> pointer a trampoline was handed.
    /// </summary>
    internal static CommitCallbackRegistration FromUserData(IntPtr userData) =>
        (CommitCallbackRegistration)GCHandle.FromIntPtr(userData).Target!;

    /// <summary>
    /// Frees the rooting <see cref="GCHandle"/> — <b>the single sanctioned free site</b> (see
    /// the remarks on the type). Idempotent: a second call frees nothing, which is what keeps
    /// the "native never ran" abandon path safe alongside the hook.
    /// </summary>
    internal void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) != 0)
        {
            return;
        }

        if (_gcHandle.IsAllocated)
        {
            _gcHandle.Free();
        }
    }
}
