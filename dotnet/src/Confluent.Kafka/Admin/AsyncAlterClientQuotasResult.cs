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
using System.Threading.Tasks;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.AlterClientQuotas"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.AlterClientQuotasResult</c>: one awaitable <b>per quota
/// entity</b>, handed back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java stores <c>Map&lt;ClientQuotaEntity, KafkaFuture&lt;Void&gt;&gt; futures</c>
/// (<c>AlterClientQuotasResult.java:31</c>), so awaiting one of these tells you whether that
/// entity's quotas were altered and hands back nothing; an entity that fails faults only
/// <em>its own</em> <see cref="AsyncKafkaFuture"/>.
/// </para>
/// <para>
/// ⚠ The dictionary is keyed by <see cref="ClientQuotaEntity"/> itself, so a lookup works
/// with any value-equal entity.
/// </para>
/// </remarks>
/// <seealso cref="AlterClientQuotasResult"/>
public sealed class AsyncAlterClientQuotasResult
{
    private readonly IReadOnlyDictionary<ClientQuotaEntity, AsyncKafkaFuture> _values;

    /// <summary>
    /// Wraps one awaitable per entity — Java's <b>public</b>
    /// <c>AlterClientQuotasResult(Map&lt;ClientQuotaEntity, KafkaFuture&lt;Void&gt;&gt;)</c>
    /// (<c>AlterClientQuotasResult.java:38</c>), which a caller can use to fabricate a result,
    /// for a test double say.
    /// </summary>
    /// <remarks>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionary is held by reference, as Java holds its map, so
    /// <see cref="Values"/> and <see cref="All"/> read whatever it holds when they are called.
    /// </remarks>
    /// <param name="futures">One awaitable per entity, keyed by the entity.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="futures"/> is null — where Java stores the null and throws
    /// <c>NullPointerException</c> on first use.
    /// </exception>
    internal AsyncAlterClientQuotasResult(IReadOnlyDictionary<ClientQuotaEntity, AsyncKafkaFuture> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Presents the bridge's <c>Task&lt;bool&gt;</c> sources as Java's
    /// <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void bridge's internal success
    /// token (result shape 2 — the ABI has no <c>get_value</c> here, so a null per-key error
    /// <em>is</em> the success value) and must not reach the public surface.
    /// </summary>
    /// <remarks>
    /// Each entry wraps the <b>same</b> task as the bridge's — no second task whose fault could
    /// go unobserved. It reuses the bridge's own comparer so the view and the sources cannot
    /// disagree about key identity.
    /// </remarks>
    internal AsyncAlterClientQuotasResult(
        IReadOnlyDictionary<ClientQuotaEntity, Task<bool>> values,
        IEqualityComparer<ClientQuotaEntity> comparer)
    {
        Dictionary<ClientQuotaEntity, AsyncKafkaFuture> erased =
            new Dictionary<ClientQuotaEntity, AsyncKafkaFuture>(values.Count, comparer);
        foreach (KeyValuePair<ClientQuotaEntity, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
        }

        _values = erased;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncAlterClientQuotasResult From(VoidKeyedAdminOperation<ClientQuotaEntity> operation) =>
        new AsyncAlterClientQuotasResult(operation.Tasks, operation.KeyComparer);

    /// <summary>
    /// One awaitable per requested entity, keyed by the entity — Java's <c>values()</c>
    /// (<c>:45</c>).
    /// </summary>
    public IReadOnlyDictionary<ClientQuotaEntity, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> entity's quotas have been altered, and faults with the
    /// first failure if any entity failed — Java's <c>all()</c> (<c>:52</c>).
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(AsyncAdminFutures.WhenAll(_values.Values));
}
