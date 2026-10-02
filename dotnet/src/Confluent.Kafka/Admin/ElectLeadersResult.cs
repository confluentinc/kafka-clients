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
/// The result of <see cref="IAdmin.ElectLeaders"/> — the .NET realization of Java's
/// <c>ElectLeadersResult</c>: <b>one</b> awaitable over a map whose values are the
/// per-partition outcomes (<c>ElectLeadersResult.java:47, :54</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>A per-partition failure is a map VALUE here, not a faulted awaitable.</b> Java's
/// field is
/// <c>KafkaFuture&lt;Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;&gt;</c>
/// (<c>:36</c>), and the javadoc on <c>partitions()</c> spells the semantics out: "If the
/// election succeeded then the value for a topic partition will be the empty Optional.
/// Otherwise the election failed and the Optional will be set with the error"
/// (<c>:43-46</c>). So a failed partition resolves the <em>same</em> successful task as
/// every other partition, carrying its error as
/// <c>Optional&lt;Throwable&gt;</c> → <see cref="KafkaException"/>?.
/// </para>
/// <para>
/// ⚠ <b>The ABI cannot tell you this, and reading it alone gets it wrong.</b>
/// <c>kafka_admin_ElectLeadersResult_t</c> and
/// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> expose <b>byte-identical</b>
/// accessor sets — <c>count</c>, <c>get_topic</c>, <c>get_partition</c>,
/// <c>get_error</c>, <c>destroy</c> — yet
/// <see cref="AlterPartitionReassignmentsResult"/> really is the ordinary per-key
/// shape whose <c>get_error(i)</c> faults that partition's own awaitable. The Java
/// return type is what separates them. See <c>AdminCallbacks.ElectLeadersOptionalError</c>
/// for the reader that carries this, and the swap-detection tests that pin it.
/// </para>
/// <para>
/// <b>Why one awaitable and not one per partition.</b> Java holds a single future, and
/// the ABI agrees for a second reason: the request may name <em>no</em> partitions at all
/// (<see cref="IAdmin.ElectLeaders"/> with a <see langword="null"/> selection means
/// "every partition in the cluster"), so the result's keys are discovered from the
/// response and cannot be pre-registered. The per-key bridge, which creates one source
/// per requested key before the submit, cannot express that.
/// </para>
/// </remarks>
public sealed class ElectLeadersResult
{
    private readonly Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>ElectLeadersResult(KafkaFuture&lt;Map&lt;TopicPartition, Optional&lt;Throwable&gt;&gt;&gt;)</c>
    /// (<c>:38-40</c>).
    /// </summary>
    internal ElectLeadersResult(Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> future)
    {
        _future = future;
    }

    /// <summary>
    /// The partitions an election was attempted for, each mapped to its outcome — Java's
    /// <c>partitions()</c> (<c>:47</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the map. A <see langword="null"/> value means that partition's
    /// election <b>succeeded</b> (Java's empty <c>Optional</c>); a non-null value is that
    /// partition's own failure and does <b>not</b> fault this task. The task itself faults
    /// only when the election could not be run at all.
    /// </returns>
    /// <remarks>
    /// This is the underlying future itself, so every call returns the <b>same</b>
    /// <see cref="Task"/> instance — matching Java, whose accessor returns the stored
    /// future field. A method rather than a property, following the shape every
    /// future-returning admin accessor uses (<see cref="ListTopicsResult.NamesToListings"/>,
    /// <see cref="DescribeClusterResult.Nodes"/>).
    /// </remarks>
    public Task<IReadOnlyDictionary<TopicPartition, KafkaException?>> Partitions() => _future;

    /// <summary>
    /// Completes when every partition's election succeeded, and faults with the first
    /// failure among them otherwise — Java's <c>all()</c> (<c>:54</c>).
    /// </summary>
    /// <returns>A task representing the whole election.</returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Not an aggregate over N awaitables.</b> Java does not call
    /// <c>KafkaFuture.allOf</c> here — it cannot, because there is only one future. It
    /// chains <c>whenComplete</c> on <c>partitions()</c>, forwards a call-level failure,
    /// and otherwise walks the map's <em>values</em> completing exceptionally with the
    /// first present <c>Optional</c> (<c>:56-68</c>). That loop is what is translated
    /// below.
    /// </para>
    /// <para>
    /// <b>Which failure "first" names.</b> Java iterates a <c>HashMap</c>, whose order it
    /// does not specify, so Java itself does not promise <em>which</em> failure a mixed
    /// batch reports. Here the map is built in the order the ABI delivers its entries,
    /// which the header documents as sorted by topic name then partition id — so the
    /// choice is at least reproducible. Callers that need every failure read
    /// <see cref="Partitions"/>, where they all remain.
    /// </para>
    /// <para>
    /// A fresh task per call, as Java allocates a fresh <c>KafkaFutureImpl</c> per
    /// <c>all()</c> call. Admin is batch/administrative, so there is no per-record path to
    /// budget (<c>admin-client.md</c> §10).
    /// </para>
    /// </remarks>
    public async Task All()
    {
        // A call-level failure propagates by the await itself — Java's `throwable != null`
        // branch (:59-60).
        IReadOnlyDictionary<TopicPartition, KafkaException?> partitions = await _future.ConfigureAwait(false);

        foreach (KafkaException? exception in partitions.Values)
        {
            if (exception is not null)
            {
                // Java's `result.completeExceptionally(exception.get()); return;` (:63-65)
                // — the FIRST present Optional wins and the rest are not inspected.
                throw exception;
            }
        }
    }
}
