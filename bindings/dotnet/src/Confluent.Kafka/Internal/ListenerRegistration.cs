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
/// The managed context behind one <b>rebalance-listener registration</b> — the
/// <c>user_data</c> the three listener trampolines recover on every fire. Rooted for the
/// life of the registration by a <see cref="GCHandle"/> whose <see cref="IntPtr"/> form is
/// what the ABI actually carries.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is NOT the one-shot completion pattern, and the one-shot invariant does not
/// transfer.</b> Every other trampoline in this binding is a per-operation completion whose
/// <see cref="GCHandle"/> the callback itself frees (<see cref="OperationCompletionSource"/>,
/// ffi-marshalling.md §B7). A rebalance listener is <b>multi-shot</b>: it is registered once
/// and fires N times, bound to a <em>subscription</em> rather than to an operation. Freeing
/// from a listener callback would be a use-after-free on every subsequent fire. So the
/// listener trampolines <b>never</b> free; <see cref="Release"/> is the single free site and
/// is driven only by the ABI's release hook.
/// </para>
/// <para>
/// <b>The single sanctioned free site is the ABI's <c>user_data_destroy</c> hook</b>
/// (P6-D3 option (a), <c>confluent_kafka.h:553-605</c>). The core fires that hook
/// <b>exactly once</b>, on every one of its five release triggers — including the two the
/// binding could never infer from a return code (a subscribe rejected <em>before</em>
/// registration versus one rejected <em>after</em> it, and an empty-topic-list subscribe
/// that returns <b>success</b> while releasing the listener). Letting the core say when a
/// registration is dead is the whole reason the hook exists; the alternative (retain every
/// superseded registration until consumer teardown) trades a bounded, core-driven free for
/// a managed guess the header explicitly warns against.
/// </para>
/// <para>
/// <b>Why freeing from the hook is safe (the P6-D3 evidence chain).</b> The hook can only
/// fire from the Rust <c>CallbackTarget</c>'s <c>Drop</c>
/// (<c>src/ffi/common.rs:442-451</c>), i.e. when the last reference to the listener object
/// is dropped. The load-bearing guarantee is the <b>ref-counted
/// <c>SafeConsumerHandle</c></b> (M9/P4 H1) plus the core's <b>single serialised
/// dispatcher thread</b> — see the two paragraphs below. Read them in that order; the
/// <c>Arc</c> argument that comes first is real but narrower than it looks.
/// </para>
/// <para>
/// <b>What the <c>Arc</c> argument does and does not prove.</b> Every core invocation site
/// clones the listener <c>Arc</c> out of its mutex into a local binding and holds that
/// owned clone <b>across the await</b> of the callback
/// (<c>src/consumer/mock_consumer.rs:260-262</c> and <c>:271-272</c>;
/// <c>src/consumer/async_kafka_consumer.rs:3273-3297</c> and <c>:5317-5333</c> — the
/// complete production set). That rules out a <b>replacing subscribe</b> firing the hook
/// mid-callback, because a replacement does not drop the invoking future. It does
/// <b>not</b> rule out the hook firing while a dispatched job is still pending:
/// <c>FfiRebalanceListener::invoke</c> copies the pointer out first
/// (<c>src/ffi/consumer.rs:3122</c>, <c>let user_data = SendUserData(self.target.user_data)</c>),
/// so the job handed to <c>dispatch_and_wait</c> carries only a <b>raw</b> <c>user_data</c>
/// copy — no reference that keeps this registration alive. <c>dispatch_and_wait</c>
/// enqueues and <em>then</em> awaits, and the dispatcher runs every queued job before
/// exiting, so dropping the awaiting future would drop the <c>Arc</c> in the same instant
/// while a job could still run. The <c>Arc</c> therefore protects the callback only for as
/// long as the awaiting future lives.
/// </para>
/// <para>
/// <b>What actually closes the window.</b> Dropping that future means destroying the
/// consumer, and <c>Consumer_destroy</c> is governed by an explicit ABI precondition —
/// "destroying concurrently with an in-flight op is a C lifetime precondition the caller
/// must uphold" (<c>src/ffi/consumer.rs:513-517</c>) — which this binding upholds
/// structurally: release is <b>ref-counted</b>, and a listener callback only ever runs
/// inside a consumer operation (the rebalance blocks on it), so <c>ReleaseHandle</c> →
/// <c>Consumer_destroy</c> cannot run while one is in flight. The four .NET paths to
/// <c>Consumer_destroy</c> were enumerated: (1) a sync call holds a call-scoped marshaller
/// AddRef via its <c>SafeConsumerHandle</c> parameter; (2) an async op holds a span-the-op
/// <c>DangerousAddRef</c> released in <c>FreeGcHandle</c>; (3) the M9/P4 Q1/Q3
/// deferred-destroy path fires from <c>FreeGcHandle</c> <b>on the dispatcher thread</b> —
/// the same single thread that would run a queued listener job, so the two are serialised,
/// never concurrent, and the op cannot complete before its own listener callbacks have
/// returned; (4) <c>Consumer_close_with_timeout</c> is not a second dropper — it passes the
/// timeout <em>into</em> <c>close_with_options</c> under <c>block_on</c>
/// (<c>src/ffi/consumer.rs:4057-4064</c>) rather than wrapping a droppable future.
/// <b>If either the ref-count or the single-dispatcher serialisation is ever weakened, this
/// choice must be re-derived.</b>
/// </para>
/// <para>
/// <b>Accepted residual — both branches.</b> <c>Consumer_destroy</c> tears the runtime down
/// with <c>runtime.shutdown_background()</c>, which does not specify whether a cancelled
/// task is dropped or leaked, so both outcomes must be analysed.
/// <em>Task leaked:</em> it retains a listener reference, the hook never fires, and this
/// registration's <see cref="GCHandle"/> stays allocated for the process lifetime — a
/// bounded leak, not a use-after-free.
/// <em>Task dropped:</em> the <c>Arc</c> drops, the hook fires and frees the
/// <see cref="GCHandle"/>; safety then rests <b>entirely</b> on no listener job being
/// in flight or queued at that moment, which is exactly what the ref-count above
/// guarantees (destroy runs only at count zero, and a listener callback implies a
/// counted operation). Neither branch is reachable outside the already-documented
/// "dispose without awaiting your own operation" misuse path
/// (ffi-marshalling.md §B2 carve-out).
/// </para>
/// </remarks>
internal sealed class ListenerRegistration
{
    private GCHandle _gcHandle;

    // 0 = rooted, 1 = released. Interlocked because the release hook "may run on any
    // thread" (confluent_kafka.h:557) — including the thread calling a replacing
    // Subscribe, and the finalizer thread when SafeConsumerHandle.ReleaseHandle runs
    // without an explicit Dispose. A second GCHandle.Free() is a hard failure, so the
    // exchange is what makes the single free site idempotent-safe.
    private int _released;

    internal ListenerRegistration(IConsumerRebalanceListener listener)
    {
        Listener = listener;
    }

    /// <summary>The user-supplied listener every trampoline dispatches to.</summary>
    internal IConsumerRebalanceListener Listener { get; }

    /// <summary>
    /// The <c>user_data</c> pointer handed to <c>ConsumerRebalanceListener_new</c>.
    /// Valid only between <see cref="Root"/> and <see cref="Release"/>.
    /// </summary>
    internal IntPtr UserData => GCHandle.ToIntPtr(_gcHandle);

    /// <summary>
    /// Whether the core has released this registration (the hook fired). Observable so
    /// tests can pin the Java-faithful release rules — a replacing subscribe releases, an
    /// <c>Unsubscribe</c> does not.
    /// </summary>
    internal bool IsReleased => Volatile.Read(ref _released) != 0;

    /// <summary>
    /// Allocates the rooting <see cref="GCHandle"/>. Called before the registration is
    /// handed to native, so <see cref="UserData"/> is valid by the time any callback or the
    /// release hook can observe it.
    /// </summary>
    internal static ListenerRegistration Root(IConsumerRebalanceListener listener)
    {
        ListenerRegistration registration = new ListenerRegistration(listener);
        registration._gcHandle = GCHandle.Alloc(registration, GCHandleType.Normal);
        return registration;
    }

    /// <summary>
    /// Recovers a registration from the <c>user_data</c> pointer a trampoline was handed.
    /// </summary>
    internal static ListenerRegistration FromUserData(IntPtr userData) =>
        (ListenerRegistration)GCHandle.FromIntPtr(userData).Target!;

    /// <summary>
    /// Frees the rooting <see cref="GCHandle"/> — <b>the single sanctioned free site for a
    /// registration</b> (see the remarks on the type). Idempotent: a second call frees
    /// nothing, which is what keeps the "native never ran" abandon path safe alongside the
    /// hook.
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
