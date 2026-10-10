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

using System.Collections.Generic;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.CreatePartitions"/> — the .NET realization of Java's
/// <c>CreatePartitionsResult</c>: one future <b>per topic</b>, handed back the moment
/// the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java publishes <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt; values()</c>
/// (<c>CreatePartitionsResult.java:39</c>), so waiting for one of these tells you whether
/// that topic's partitions were created and hands back nothing; a topic that fails fails
/// only <em>its own</em> <see cref="KafkaFuture"/>.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its topic (or, for
/// <see cref="All"/>, every topic) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncCreatePartitionsResult"/>
public sealed class CreatePartitionsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture> _values;

    /// <summary>
    /// Presents the bridge's latches as Java's <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void
    /// bridge's internal success token (result shape 2 — the ABI has no <c>get_value</c> here, so a null per-key error
    /// <em>is</em> the success value) and must not reach the public surface.
    /// </summary>
    /// <remarks>
    /// Each entry is a <see cref="KafkaFuture"/> over the <b>same</b> latch as the bridge's — no second completion
    /// (<see cref="SyncAdminFutures.Erase{TKey}"/>). It reuses the bridge's own comparer so the view and the sources
    /// cannot disagree about key identity.
    /// </remarks>
    internal CreatePartitionsResult(
        IReadOnlyDictionary<string, SyncFutureSource<bool>> values, IEqualityComparer<string> comparer)
    {
        _values = SyncAdminFutures.Erase(values, comparer);
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static CreatePartitionsResult From(VoidKeyedAdminOperation<string> operation) =>
        new CreatePartitionsResult(operation.SyncSources, operation.KeyComparer);

    /// <summary>
    /// One future per requested topic, keyed by topic name — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<string, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> topic's partitions have been created, and fails with
    /// the first failure in key order if any topic failed — Java's <c>all()</c>
    /// (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every topic.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
