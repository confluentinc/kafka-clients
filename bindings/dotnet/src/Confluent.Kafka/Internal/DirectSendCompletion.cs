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
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The per-send context of the async flavor's <b>direct</b> send path (send-approach-2 POC,
/// <see cref="NativeProducer.SendDirect"/>): the record's delivery <see cref="Task{TResult}"/>, the
/// optional user <see cref="DeliveryRegistration"/>, and the <see cref="GCHandle"/> that roots this
/// object while the core holds it as the delivery callback's <c>user_data</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>It IS the <see cref="TaskCompletionSource{TResult}"/></b> rather than holding one, so the direct
/// send allocates one object (plus the TCS's own <see cref="Task"/>) per record on the plain
/// <c>Send(record)</c> path — the same count as the pump path's TCS, and no accumulator node (DoD §10).
/// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> is mandatory (ffi §A7): the
/// completion runs on the core's single dispatcher thread, and an awaiter continuation run inline
/// there would stall every other record's completion.
/// </para>
/// <para>
/// <b>Ownership of the root.</b> <see cref="Root"/> is called once, before the native send. After the
/// send returns, exactly one party frees it: the send itself when the core reports
/// <c>out_callback_pending == false</c> (the callback will never fire), otherwise the dispatcher-thread
/// trampoline (<see cref="Interop.ProducerCallbacks"/>) after it has delivered. <see cref="Unroot"/>
/// is idempotent so a defensive second call cannot double-free.
/// </para>
/// <para>
/// <b>Delivery is at most once</b> (<see cref="Deliver"/>), and it fires the user callback
/// <b>before</b> completing the task (M14/P1 decision D3) — the same order the sync send and the pump
/// use, and Java's: <c>ProducerBatch.completeFutureAndFireCallbacks</c> runs the callbacks before
/// <c>produceFuture.done()</c> releases a waiting <c>get()</c>.
/// </para>
/// </remarks>
internal sealed class DirectSendCompletion : TaskCompletionSource<RecordMetadata>
{
    private readonly DeliveryRegistration? _delivery;
    private GCHandle _root;
    private int _rootFreed;
    private int _delivered;

    /// <param name="delivery">The user's delivery callback carrier, or <see langword="null"/>.</param>
    internal DirectSendCompletion(DeliveryRegistration? delivery)
        : base(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        _delivery = delivery;
    }

    /// <summary>
    /// Roots this object and returns the <c>user_data</c> pointer handed to the core. Called exactly
    /// once, before the native send.
    /// </summary>
    internal IntPtr Root()
    {
        _root = GCHandle.Alloc(this, GCHandleType.Normal);
        return GCHandle.ToIntPtr(_root);
    }

    /// <summary>Frees the root exactly once; later calls are no-ops.</summary>
    internal void Unroot()
    {
        if (Interlocked.Exchange(ref _rootFreed, 1) == 0 && _root.IsAllocated)
        {
            _root.Free();
        }
    }

    /// <summary>
    /// Delivers the send's outcome: the user callback first (D3), then the task. At most once — a
    /// second call is a no-op. Never throws: <see cref="DeliveryRegistration.Fire"/> is a total
    /// no-throw boundary and the <c>TrySet*</c> calls do not throw.
    /// </summary>
    /// <param name="metadata">The published metadata on success; <see langword="null"/> on failure.</param>
    /// <param name="failure">The failure, or <see langword="null"/> on success.</param>
    internal void Deliver(RecordMetadata? metadata, Exception? failure)
    {
        if (Interlocked.Exchange(ref _delivered, 1) != 0)
        {
            return;
        }

        _delivery?.Fire(metadata, failure);

        if (failure is null)
        {
            TrySetResult(metadata!);
        }
        else
        {
            TrySetException(failure);
        }
    }
}
