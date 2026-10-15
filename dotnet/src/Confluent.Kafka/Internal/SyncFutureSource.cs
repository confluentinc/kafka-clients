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

namespace Confluent.Kafka.Internal;

/// <summary>
/// What a sync future waits on (M15/P14 S1): the source behind a <see cref="KafkaFuture"/>, and the base of every
/// <see cref="SyncFutureSource{T}"/> behind a <see cref="KafkaFuture{T}"/>. It is a latch (<see cref="SyncCompletion{T}"/>)
/// or a composite over other sources (a projection, an all-of, a gathering all-of).
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>Blocking, on the caller's thread.</b> <see cref="Wait"/> parks the thread that calls it; no implementation uses
/// a <c>Task</c>, a <c>TaskCompletionSource</c>, <c>Task.Run</c> or <c>Task.WhenAll</c>, so a sync future never needs a
/// thread-pool thread and is never sync-over-async (M15/P14 PLAN §3.3).
/// </description></item>
/// <item><description>
/// <b>Same outcome on every call.</b> Once completed, every <see cref="Wait"/> returns, or rethrows the <b>same</b>
/// exception instance, on any thread, any number of times.
/// </description></item>
/// <item><description>
/// <b>No state here.</b> Neither base declares an instance field, so deriving from them does not grow a latch: the
/// producer's per-send <see cref="SyncCompletion{T}"/> stays the size its allocation budget measures.
/// </description></item>
/// </list>
/// </remarks>
internal abstract class SyncFutureSource
{
    /// <summary>
    /// Whether the source has completed, successfully or not — Java's <c>isDone()</c>. Never blocks.
    /// </summary>
    internal abstract bool IsDone { get; }

    /// <summary>
    /// Blocks the calling thread until the source has completed; if it failed, rethrows its exception (the same
    /// instance on every call, not wrapped).
    /// </summary>
    internal abstract void Wait();
}

/// <summary>
/// A <see cref="SyncFutureSource"/> that completes with a value: the source behind a <see cref="KafkaFuture{T}"/>.
/// </summary>
/// <typeparam name="T">The value the source completes with.</typeparam>
internal abstract class SyncFutureSource<T> : SyncFutureSource
{
    /// <summary>
    /// Blocks the calling thread until the source has completed, then returns its value (the same instance on every
    /// call); if it failed, rethrows its exception (the same instance on every call, not wrapped).
    /// </summary>
    internal abstract T Get();

    /// <inheritdoc/>
    /// <remarks>Waiting for a value-carrying source is <see cref="Get"/> with the value discarded.</remarks>
    internal sealed override void Wait()
    {
        _ = Get();
    }
}
