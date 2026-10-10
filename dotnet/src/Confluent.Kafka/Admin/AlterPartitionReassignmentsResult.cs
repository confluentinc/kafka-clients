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
/// The result of <see cref="IAdmin.AlterPartitionReassignments"/> — the .NET realization
/// of Java's <c>AlterPartitionReassignmentsResult</c>: one future <b>per partition</b>,
/// handed back the moment the request is submitted
/// (<c>AlterPartitionReassignmentsResult.java:45, :52</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>This result's ABI accessor set is byte-identical to
/// <see cref="ElectLeadersResult"/>'s, and the two shapes are different.</b> Both expose
/// <c>count</c>, <c>get_topic</c>, <c>get_partition</c>, <c>get_error</c>,
/// <c>destroy</c>. Here Java's field is
/// <c>Map&lt;TopicPartition, KafkaFuture&lt;Void&gt;&gt;</c> (<c>:29</c>), so
/// <c>get_error(i)</c> is that partition's <b>failure</b> and fails its own
/// <see cref="KafkaFuture"/> — the ordinary per-key shape. In
/// <see cref="ElectLeadersResult"/> the same accessor is the map's <em>value</em>. The
/// Java return type is what separates them.
/// </para>
/// <para>
/// Waiting for one of these tells you whether that partition's reassignment was
/// <em>initiated</em> and hands back nothing — Java's per-partition future is
/// a <see cref="KafkaFuture"/>, which carries no value, and its javadoc lists the per-partition error codes
/// (<c>INVALID_REPLICA_ASSIGNMENT</c>, <c>NO_REASSIGNMENT_IN_PROGRESS</c>, …). The
/// per-partition error is <b>owned</b>: it arrives with that partition's own callback and is
/// freed once it has been copied into the <see cref="KafkaException"/>.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its partition (or, for
/// <see cref="All"/>, every partition) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncAlterPartitionReassignmentsResult"/>
public sealed class AlterPartitionReassignmentsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, KafkaFuture> _values;

    /// <summary>
    /// Wraps one future per partition — Java's package-private
    /// <c>AlterPartitionReassignmentsResult(Map&lt;TopicPartition, KafkaFuture&lt;Void&gt;&gt;)</c>. The dictionary
    /// is held by reference, as Java holds its map.
    /// </summary>
    internal AlterPartitionReassignmentsResult(IReadOnlyDictionary<TopicPartition, KafkaFuture> values)
    {
        _values = values;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// Presents the bridge's <c>bool</c> latches as valueless <see cref="KafkaFuture"/>s: the <c>bool</c> is the
    /// void bridge's internal success token (result shape 2 — the ABI declares no <c>get_value</c> here, so a null
    /// per-partition error <em>is</em> the success value) and must not reach the public surface. Each void future is
    /// erased over its key's <b>same</b> latch — no second completion — with the bridge's own comparer, so the view
    /// and the sources cannot disagree about key identity.
    /// </remarks>
    internal static AlterPartitionReassignmentsResult From(VoidKeyedAdminOperation<TopicPartition> operation) =>
        new AlterPartitionReassignmentsResult(SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer));

    /// <summary>
    /// One future per requested partition, keyed by partition — Java's <c>values()</c>
    /// (<c>:45</c>).
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> reassignment was initiated, and fails with the first
    /// failure in key order if any partition failed — Java's <c>all()</c> (<c>:52</c>,
    /// <c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every partition.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
