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
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AlterReplicaLogDirs"/> — the .NET realization of Java's
/// <c>AlterReplicaLogDirsResult</c>: one awaitable <b>per replica</b>, handed back the
/// moment the request is submitted (<c>AlterReplicaLogDirsResult.java:67, :75</c>).
/// </summary>
/// <remarks>
/// <para>
/// Java publishes <c>Map&lt;TopicPartitionReplica, KafkaFuture&lt;Void&gt;&gt; values()</c>,
/// so awaiting one tells you whether <em>that</em> replica's move was accepted and hands
/// back nothing. Java documents the failures it can carry (<c>:53-64</c>) —
/// authorization, an over-long topic name, an unknown log directory, a replica that does
/// not exist on the broker, a disk error.
/// </para>
/// <para>
/// The per-replica error is <b>borrowed</b> from the result root and is never destroyed;
/// there is no <c>_get_value</c>, which the header states outright — "Java's per-replica
/// future is <c>KafkaFuture&lt;Void&gt;</c>, so a null error <em>is</em> the success value"
/// (result shape 2).
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-replica <em>granularity</em> is fully
/// preserved, but per-replica <em>timing independence</em> is not — the same recorded
/// limitation as <see cref="CreatePartitionsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class AlterReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, Task> _values;

    /// <summary>
    /// Presents the bridge's <c>Task&lt;bool&gt;</c> sources as Java's
    /// <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void bridge's internal success
    /// token and must not reach the public surface.
    /// </summary>
    /// <remarks>
    /// The erasure is a reference upcast, so each entry is the <b>same</b>
    /// <see cref="Task"/> instance as the bridge's — no per-key allocation, and no second
    /// task whose fault could go unobserved. It reuses the bridge's own comparer so the
    /// view and the sources cannot disagree about key identity.
    /// </remarks>
    internal AlterReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, Task<bool>> values,
        IEqualityComparer<TopicPartitionReplica> comparer)
    {
        Dictionary<TopicPartitionReplica, Task> erased =
            new Dictionary<TopicPartitionReplica, Task>(values.Count, comparer);
        foreach (KeyValuePair<TopicPartitionReplica, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        _values = erased;
    }

    /// <summary>
    /// One awaitable per requested replica — Java's <c>values()</c> (<c>:67</c>).
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, Task> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> replica move has been accepted, and faults with the
    /// first failure if any was rejected — Java's <c>all()</c> (<c>:75</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_values.Values);
}
