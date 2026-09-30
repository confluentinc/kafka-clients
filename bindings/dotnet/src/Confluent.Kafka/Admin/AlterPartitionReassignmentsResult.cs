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
/// The result of <see cref="IAdmin.AlterPartitionReassignments"/> — the .NET realization
/// of Java's <c>AlterPartitionReassignmentsResult</c>: one awaitable <b>per partition</b>,
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
/// <c>get_error(i)</c> is that partition's <b>failure</b> and faults its own
/// <see cref="Task"/> — the ordinary per-key shape. In
/// <see cref="ElectLeadersResult"/> the same accessor is the map's <em>value</em>. The
/// Java return type is what separates them.
/// </para>
/// <para>
/// Awaiting one of these tells you whether that partition's reassignment was
/// <em>initiated</em> and hands back nothing — Java's per-partition future is
/// <c>KafkaFuture&lt;Void&gt;</c>, and its javadoc lists the per-partition error codes
/// (<c>INVALID_REPLICA_ASSIGNMENT</c>, <c>NO_REASSIGNMENT_IN_PROGRESS</c>, …). The
/// per-partition error is <b>borrowed</b> from the result root and is never destroyed.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-partition <em>granularity</em> is fully
/// preserved, but per-partition <em>timing independence</em> is not — the same recorded
/// limitation as <see cref="CreatePartitionsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class AlterPartitionReassignmentsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, Task> _values;

    /// <summary>
    /// Presents the bridge's <c>Task&lt;bool&gt;</c> sources as Java's
    /// <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void bridge's internal
    /// success token (result shape 2 — the ABI declares no <c>get_value</c> here, so a null
    /// per-partition error <em>is</em> the success value) and must not reach the public
    /// surface.
    /// </summary>
    /// <remarks>
    /// The erasure is a reference upcast, so each entry is the <b>same</b>
    /// <see cref="Task"/> instance as the bridge's — no per-key allocation, and no second
    /// task whose fault could go unobserved. It reuses the bridge's own comparer so the
    /// view and the sources cannot disagree about key identity.
    /// </remarks>
    internal AlterPartitionReassignmentsResult(
        IReadOnlyDictionary<TopicPartition, Task<bool>> values, IEqualityComparer<TopicPartition> comparer)
    {
        Dictionary<TopicPartition, Task> erased = new Dictionary<TopicPartition, Task>(values.Count, comparer);
        foreach (KeyValuePair<TopicPartition, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        _values = erased;
    }

    /// <summary>
    /// One awaitable per requested partition, keyed by partition — Java's <c>values()</c>
    /// (<c>:45</c>).
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, Task> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> reassignment was initiated, and faults if any partition
    /// failed — Java's <c>all()</c> (<c>:52</c>, <c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_values.Values);
}
