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
/// The result of <see cref="IAdmin.ListPartitionReassignments"/> — the .NET realization of
/// Java's <c>ListPartitionReassignmentsResult</c>: <b>one</b> awaitable over the whole
/// listing (<c>ListPartitionReassignmentsResult.java:31, :40</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The name is a false parallel with
/// <see cref="AlterPartitionReassignmentsResult"/> — the shapes are different.</b> That one
/// holds <c>Map&lt;TopicPartition, KafkaFuture&lt;Void&gt;&gt;</c>, one awaitable per
/// partition with a per-partition error. This one holds a <b>single</b>
/// <c>KafkaFuture&lt;Map&lt;TopicPartition, PartitionReassignment&gt;&gt;</c>
/// (<c>:31</c>), and the ABI agrees: <c>kafka_admin_ListPartitionReassignmentsResult_t</c>
/// declares a <c>get_value</c> and <b>no <c>get_error</c> at all</b>, which the header
/// states as "Java holds a single future here rather than one per partition, so unlike the
/// per-key RPCs there is no <c>_get_error(i)</c>: any failure is a call failure".
/// </para>
/// <para>
/// ⚠ <b>The map can be SHORTER than the request</b> — the header: "Only partitions with an
/// ongoing reassignment appear in the result". A requested partition that is not being
/// reassigned is simply absent, which is not an error and is why nothing here
/// pre-registers the caller's keys.
/// </para>
/// <para>
/// <b>Java publishes exactly one accessor</b> (<c>:40</c>), so there is no second view
/// here either.
/// </para>
/// </remarks>
public sealed class ListPartitionReassignmentsResult
{
    private readonly Task<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>ListPartitionReassignmentsResult(KafkaFuture&lt;Map&lt;…&gt;&gt;)</c>
    /// (<c>:33-35</c>).
    /// </summary>
    internal ListPartitionReassignmentsResult(
        Task<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> future)
    {
        _future = future;
    }

    /// <summary>
    /// Each partition's ongoing reassignment — Java's <c>reassignments()</c>
    /// (<c>:40</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the map, or faulting with the call's own
    /// <see cref="KafkaException"/>. Partitions with no ongoing reassignment are absent
    /// rather than present-and-empty.
    /// </returns>
    /// <remarks>
    /// This is the underlying future itself, so every call returns the <b>same</b>
    /// <see cref="Task"/> instance — matching Java, whose accessor returns the stored
    /// future field.
    /// </remarks>
    public Task<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> Reassignments() => _future;
}
