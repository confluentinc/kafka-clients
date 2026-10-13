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
/// The result of <see cref="IAdmin.AlterReplicaLogDirs"/> — the .NET realization of Java's
/// <c>AlterReplicaLogDirsResult</c>: one future <b>per replica</b>, handed back the
/// moment the request is submitted (<c>AlterReplicaLogDirsResult.java:67, :75</c>).
/// </summary>
/// <remarks>
/// <para>
/// Java publishes <c>Map&lt;TopicPartitionReplica, KafkaFuture&lt;Void&gt;&gt; values()</c>,
/// so waiting for one tells you whether <em>that</em> replica's move was accepted and hands
/// back nothing. Java documents the failures it can carry (<c>:53-64</c>) —
/// authorization, an over-long topic name, an unknown log directory, a replica that does
/// not exist on the broker, a disk error.
/// </para>
/// <para>
/// The per-replica error is <b>owned</b>: it arrives with that replica's own callback and is
/// freed once it has been copied into the <see cref="KafkaException"/>. The callback
/// carries no value, which the header states outright — "Java's per-replica future is
/// <c>KafkaFuture&lt;Void&gt;</c>, so a null error <em>is</em> the success value".
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its replica (or, for
/// <see cref="All"/>, every replica) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncAlterReplicaLogDirsResult"/>
public sealed class AlterReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture> _values;

    /// <summary>
    /// Wraps one future per replica — Java's package-private
    /// <c>AlterReplicaLogDirsResult(Map&lt;TopicPartitionReplica, KafkaFuture&lt;Void&gt;&gt;)</c> (<c>:41</c>).
    /// The dictionary is held by reference, as Java holds its map.
    /// </summary>
    internal AlterReplicaLogDirsResult(IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture> values)
    {
        _values = values;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// Presents the bridge's <c>bool</c> latches as valueless <see cref="KafkaFuture"/>s: the <c>bool</c> is the
    /// void bridge's internal success token and must not reach the public surface. Each void future is erased over
    /// its key's <b>same</b> latch — no second completion — with the bridge's own comparer, so the view and the
    /// sources cannot disagree about key identity.
    /// </remarks>
    internal static AlterReplicaLogDirsResult From(VoidKeyedAdminOperation<TopicPartitionReplica> operation) =>
        new AlterReplicaLogDirsResult(SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer));

    /// <summary>
    /// One future per requested replica — Java's <c>values()</c> (<c>:67</c>).
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> replica move has been accepted, and fails with the
    /// first failure in key order if any was rejected — Java's <c>all()</c> (<c>:75</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every replica.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
