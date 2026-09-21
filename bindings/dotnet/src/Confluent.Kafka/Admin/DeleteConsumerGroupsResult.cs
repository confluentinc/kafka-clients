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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DeleteConsumerGroups"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DeleteConsumerGroupsResult</c>: one awaitable
/// <b>per group id</b>, handed back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java stores <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt; futures</c>
/// (<c>DeleteConsumerGroupsResult.java:30</c>), so awaiting one of these tells you whether
/// that group was deleted and hands back nothing; a group that fails faults only
/// <em>its own</em> <see cref="Task"/> — Java's <c>deletedGroups()</c> (<c>:36-44</c>).
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-group <em>granularity</em> is fully preserved,
/// but per-group <em>timing independence</em> is not: all the <see cref="Task"/>s complete
/// at the same instant, because the C ABI has no future type and resolves every key together
/// before reporting. The same recorded limitation as <see cref="CreateTopicsResult"/> and
/// <see cref="CreatePartitionsResult"/>.
/// </para>
/// </remarks>
public sealed class DeleteConsumerGroupsResult
{
    private readonly IReadOnlyDictionary<string, Task> _deletedGroups;

    /// <summary>
    /// Presents the bridge's <c>Task&lt;bool&gt;</c> sources as Java's
    /// <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void bridge's internal success
    /// token (result shape 2 — the ABI has no <c>get_value</c> here, so a null per-key error
    /// <em>is</em> the success value) and must not reach the public surface.
    /// </summary>
    /// <remarks>
    /// The erasure is a reference upcast, so each entry is the <b>same</b> <see cref="Task"/>
    /// instance as the bridge's — no per-key allocation, and no second task whose fault could
    /// go unobserved. It reuses the bridge's own comparer so the view and the sources cannot
    /// disagree about key identity.
    /// </remarks>
    internal DeleteConsumerGroupsResult(
        IReadOnlyDictionary<string, Task<bool>> values, IEqualityComparer<string> comparer)
    {
        Dictionary<string, Task> erased = new Dictionary<string, Task>(values.Count, comparer);
        foreach (KeyValuePair<string, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        _deletedGroups = erased;
    }

    /// <summary>
    /// One awaitable per requested group id, keyed by group id — Java's <c>deletedGroups()</c>.
    /// </summary>
    public IReadOnlyDictionary<string, Task> DeletedGroups => _deletedGroups;

    /// <summary>
    /// Completes when <b>every</b> group has been deleted, and faults with the first failure
    /// if any group failed — Java's <c>all()</c> (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_deletedGroups.Values);
}
