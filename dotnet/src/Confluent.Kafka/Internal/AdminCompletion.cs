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
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// One admin key's completion, as the admin bridge sees it (M15/P14 PLAN §3.1, D7): the operation resolves it
/// through this interface without knowing which flavour it is.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>Two implementations, one per <see cref="AdminFlavour"/>.</b> <see cref="TaskAdminCompletion{T}"/> for the async
/// flavour and <see cref="SyncCompletion{T}"/> for the sync one. The callbacks in <c>AdminCallbacks</c> cast
/// <c>GCHandle.Target</c> to the operation types, which are unchanged, so nothing on the callback side knows about the
/// flavour either.
/// </description></item>
/// <item><description>
/// <b>Exactly once.</b> The first <see cref="TrySetResult"/> or <see cref="TrySetException"/> completes it and returns
/// <see langword="true"/>; every later one returns <see langword="false"/> and changes nothing.
/// </description></item>
/// <item><description>
/// <b>Cost.</b> One interface dispatch per key. Admin has no per-record path (<c>admin-client.md</c> §10, DoD #10
/// N/A).
/// </description></item>
/// </list>
/// </remarks>
/// <typeparam name="T">The key's value.</typeparam>
internal interface IAdminCompletion<T>
{
    /// <summary>Whether the completion has been completed, successfully or not. Never blocks.</summary>
    bool IsCompleted { get; }

    /// <summary>Completes it with <paramref name="result"/>, unless it is already completed.</summary>
    /// <returns><see langword="true"/> if this call completed it; <see langword="false"/> if an earlier call had.</returns>
    bool TrySetResult(T result);

    /// <summary>Completes it with <paramref name="exception"/>, unless it is already completed.</summary>
    /// <returns><see langword="true"/> if this call completed it; <see langword="false"/> if an earlier call had.</returns>
    bool TrySetException(Exception exception);
}

/// <summary>
/// The async flavour's per-key completion: a <see cref="TaskCompletionSource{TResult}"/> built with
/// <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>, exactly the source every admin key used before
/// M15/P14.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> is mandatory.</b> An admin callback can run on
/// the handle's dispatcher thread, on a tokio worker, or synchronously on the submitting thread before the entry point
/// returns (<c>rust/src/ffi/admin.rs:59-87</c>). Without the option an awaiter's continuation would run on that thread:
/// it would stall the dispatcher, or run inside the caller's own P/Invoke.
/// </para>
/// <para>
/// <b>Same allocation as before.</b> The type adds no field to <see cref="TaskCompletionSource{TResult}"/>, so a key
/// costs the source and its <see cref="Task{TResult}"/>, as it did. <see cref="TaskCompletionSource{TResult}.TrySetResult"/>
/// and <see cref="TaskCompletionSource{TResult}.TrySetException(Exception)"/> implement the interface as they are.
/// </para>
/// </remarks>
/// <typeparam name="T">The key's value.</typeparam>
internal sealed class TaskAdminCompletion<T> : TaskCompletionSource<T>, IAdminCompletion<T>
{
    /// <summary>Creates a completion that is not yet completed.</summary>
    internal TaskAdminCompletion()
        : base(TaskCreationOptions.RunContinuationsAsynchronously)
    {
    }

    /// <inheritdoc/>
    bool IAdminCompletion<T>.IsCompleted => Task.IsCompleted;
}
