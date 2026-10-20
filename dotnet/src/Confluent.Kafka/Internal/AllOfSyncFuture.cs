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
using System.Runtime.ExceptionServices;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A sync all-of over an ordered list of sources (M15/P14 S1, PLAN §3.3): it completes when every source has — Java's
/// <c>KafkaFuture.allOf</c>, the sync twin of <c>await Task.WhenAll(...)</c>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>
/// <b>Every source, in list order.</b> <see cref="Wait"/> waits for each source in turn, on the calling thread, and
/// does not return early when one has failed: like <c>Task.WhenAll</c>, it completes only when all of them have.
/// </description></item>
/// <item><description>
/// <b>The first failure in list order.</b> Then, if any failed, it rethrows the failure of the <b>earliest</b> failed
/// source in the list — whichever failed first in time — as that source's own exception instance. That is the
/// exception <c>await Task.WhenAll(...)</c> surfaces (its faults are in input order) and what Java's <c>allOf</c>
/// completes with. The list is the operation's key order, which both admin flavours share.
/// </description></item>
/// <item><description>
/// <b>No memo, no lock.</b> The sources are memoized latches, so waiting again re-derives the same outcome; the list is
/// copied once at construction and never changes. An empty list is complete from the start.
/// </description></item>
/// <item><description>
/// <b>Admin sources only.</b> They carry no D9 owner (M15/P14 PLAN §3.4), so a source's <see cref="Wait"/> throws
/// exactly when that source has failed — never to refuse the caller — which is what lets a throw be read as a failure.
/// </description></item>
/// </list>
/// </remarks>
internal sealed class AllOfSyncFuture : SyncFutureSource
{
    private readonly SyncFutureSource[] _sources;

    /// <summary>Creates an all-of over <paramref name="sources"/>, in their order.</summary>
    /// <param name="sources">The sources, in the operation's key order. Copied; later changes are not seen.</param>
    internal AllOfSyncFuture(IReadOnlyList<SyncFutureSource> sources)
    {
        if (sources is null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        _sources = new SyncFutureSource[sources.Count];
        for (int index = 0; index < _sources.Length; index++)
        {
            _sources[index] = sources[index];
        }
    }

    /// <inheritdoc/>
    internal override bool IsDone => AreAllDone(_sources);

    /// <inheritdoc/>
    internal override void Wait() => WaitForEvery(_sources)?.Throw();

    /// <summary>Whether every source has completed, successfully or not. Never blocks.</summary>
    internal static bool AreAllDone(IReadOnlyList<SyncFutureSource> sources)
    {
        for (int index = 0; index < sources.Count; index++)
        {
            if (!sources[index].IsDone)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Waits for every source, in list order, on the calling thread — past a failure, never stopping at it — and
    /// returns the first failure in list order, or <see langword="null"/> when every source succeeded.
    /// </summary>
    /// <remarks>Shared with <see cref="GatheringAllOfSyncFuture{TKey, TValue}"/>, so the two cannot diverge.</remarks>
    internal static ExceptionDispatchInfo? WaitForEvery(IReadOnlyList<SyncFutureSource> sources)
    {
        ExceptionDispatchInfo? firstFailure = null;
        for (int index = 0; index < sources.Count; index++)
        {
            try
            {
                sources[index].Wait();
            }
            catch (Exception failure)
            {
                // Keep the earliest in list order; a later one is still waited for, then dropped.
                firstFailure ??= ExceptionDispatchInfo.Capture(failure);
            }
        }

        return firstFailure;
    }
}
