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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeClientQuotas"/> — the .NET realization of Java's
/// <c>DescribeClientQuotasResult</c>: <b>one</b> future over the whole entity-to-quota
/// map (<c>DescribeClientQuotasResult.java:31</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>There is no <c>All()</c>.</b> Java publishes only <c>entities()</c> (<c>:46</c>), and
/// <c>kafka_admin_DescribeClientQuotasResult_t</c> declares no <c>get_error</c> of any kind —
/// so there is no per-entity outcome to aggregate and any failure fails this single future.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the description has completed, and
/// needs no thread-pool thread: the future here is the operation's own latch, never a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeClientQuotasResult"/>
public sealed class DescribeClientQuotasResult
{
    private readonly KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> _future;

    /// <summary>
    /// Wraps the single future — Java's <b>public</b>
    /// <c>DescribeClientQuotasResult(KafkaFuture&lt;Map&lt;ClientQuotaEntity, Map&lt;String, Double&gt;&gt;&gt;)</c>
    /// (<c>:39</c>).
    /// </summary>
    /// <remarks>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>.
    /// </remarks>
    /// <param name="entities">The future over the entity-to-quota map.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="entities"/> is a default value, bound to no latch — this constructor's null, where Java
    /// stores the null and throws <c>NullPointerException</c> on first use.
    /// </exception>
    internal DescribeClientQuotasResult(
        KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> entities)
    {
        _future = entities != default ? entities : throw new ArgumentNullException(nameof(entities));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeClientQuotasResult From(
        SingleAdminOperation<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> operation) =>
        new DescribeClientQuotasResult(
            new KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                operation.SyncSource));

    /// <summary>
    /// The quotas of every entity matching the filter — Java's <c>entities()</c>
    /// (<c>:46</c>). A quota type an entity has no value for is simply absent.
    /// </summary>
    /// <returns>
    /// A future yielding the map, or failing with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>
    /// This is the stored future itself, so every call returns an <b>equal</b> future over the operation's own latch —
    /// no second completion.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> Entities() =>
        _future;
}
