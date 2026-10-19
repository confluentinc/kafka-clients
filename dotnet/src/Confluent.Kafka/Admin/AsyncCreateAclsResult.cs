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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.CreateAcls"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.CreateAclsResult</c>: one future <b>per ACL
/// binding</b>, handed back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java stores <c>Map&lt;AclBinding, KafkaFuture&lt;Void&gt;&gt; futures</c>
/// (<c>CreateAclsResult.java:30</c>), so awaiting one of these tells you whether that
/// binding was created and hands back nothing; a binding that fails faults only
/// <em>its own</em> <see cref="AsyncKafkaFuture"/>.
/// </para>
/// <para>
/// ⚠ The dictionary is keyed by <see cref="AclBinding"/> itself, so a lookup works with any
/// value-equal binding — <c>Values[new AclBinding(pattern, entry)]</c> hits the entry you
/// requested.
/// </para>
/// </remarks>
/// <seealso cref="CreateAclsResult"/>
public sealed class AsyncCreateAclsResult
{
    private readonly IReadOnlyDictionary<AclBinding, AsyncKafkaFuture> _values;

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
    internal AsyncCreateAclsResult(
        IReadOnlyDictionary<AclBinding, Task<bool>> values, IEqualityComparer<AclBinding> comparer)
    {
        Dictionary<AclBinding, AsyncKafkaFuture> erased = new Dictionary<AclBinding, AsyncKafkaFuture>(values.Count, comparer);
        foreach (KeyValuePair<AclBinding, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
        }

        _values = erased;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncCreateAclsResult From(VoidKeyedAdminOperation<AclBinding> operation) =>
        new AsyncCreateAclsResult(operation.Tasks, operation.KeyComparer);

    /// <summary>
    /// One future per requested binding, keyed by the binding — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<AclBinding, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> binding has been created, and faults with the first
    /// failure if any binding failed — Java's <c>all()</c> (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(AsyncAdminFutures.WhenAll(_values.Values));
}
