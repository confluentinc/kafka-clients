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
/// The result of <see cref="IAdmin.AlterClientQuotas"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.AlterClientQuotasResult</c>: one future <b>per quota
/// entity</b>, handed back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java stores <c>Map&lt;ClientQuotaEntity, KafkaFuture&lt;Void&gt;&gt; futures</c>
/// (<c>AlterClientQuotasResult.java:31</c>), so waiting for one of these tells you whether that
/// entity's quotas were altered and hands back nothing; an entity that fails fails only
/// <em>its own</em> <see cref="KafkaFuture"/>.
/// </para>
/// <para>
/// ⚠ The dictionary is keyed by <see cref="ClientQuotaEntity"/> itself, so a lookup works
/// with any value-equal entity.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its entity (or, for
/// <see cref="All"/>, every entity) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncAlterClientQuotasResult"/>
public sealed class AlterClientQuotasResult
{
    private readonly IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> _values;

    /// <summary>
    /// Wraps one future per entity — Java's <b>public</b>
    /// <c>AlterClientQuotasResult(Map&lt;ClientQuotaEntity, KafkaFuture&lt;Void&gt;&gt;)</c>
    /// (<c>AlterClientQuotasResult.java:38</c>).
    /// </summary>
    /// <remarks>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionary is held by reference, as Java holds its map, so
    /// <see cref="Values"/> and <see cref="All"/> read whatever it holds when they are called.
    /// </remarks>
    /// <param name="futures">One future per entity, keyed by the entity.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="futures"/> is null — where Java stores the null and throws
    /// <c>NullPointerException</c> on first use.
    /// </exception>
    internal AlterClientQuotasResult(IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// Presents the bridge's <c>bool</c> latches as valueless <see cref="KafkaFuture"/>s: the <c>bool</c> is the
    /// void bridge's internal success token (result shape 2 — the ABI has no <c>get_value</c> here, so a null
    /// per-key error <em>is</em> the success value) and must not reach the public surface. Each void future is erased
    /// over its key's <b>same</b> latch — no second completion — with the bridge's own comparer, so the view and the
    /// sources cannot disagree about key identity.
    /// </remarks>
    internal static AlterClientQuotasResult From(VoidKeyedAdminOperation<ClientQuotaEntity> operation) =>
        new AlterClientQuotasResult(SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer));

    /// <summary>
    /// One future per requested entity, keyed by the entity — Java's <c>values()</c>
    /// (<c>:45</c>).
    /// </summary>
    public IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> entity's quotas have been altered, and fails with the
    /// first failure in key order if any entity failed — Java's <c>all()</c> (<c>:52</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every entity.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
