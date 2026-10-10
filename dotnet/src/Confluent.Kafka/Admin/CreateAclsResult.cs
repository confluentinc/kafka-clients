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
/// The result of <see cref="IAdmin.CreateAcls"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.CreateAclsResult</c>: one future <b>per ACL
/// binding</b>, handed back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java stores <c>Map&lt;AclBinding, KafkaFuture&lt;Void&gt;&gt; futures</c>
/// (<c>CreateAclsResult.java:30</c>), so waiting for one of these tells you whether that
/// binding was created and hands back nothing; a binding that fails fails only
/// <em>its own</em> <see cref="KafkaFuture"/>.
/// </para>
/// <para>
/// ⚠ The dictionary is keyed by <see cref="AclBinding"/> itself, so a lookup works with any
/// value-equal binding — <c>Values[new AclBinding(pattern, entry)]</c> hits the entry you
/// requested.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its binding (or, for
/// <see cref="All"/>, every binding) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncCreateAclsResult"/>
public sealed class CreateAclsResult
{
    private readonly IReadOnlyDictionary<AclBinding, KafkaFuture> _values;

    /// <summary>
    /// Wraps one future per binding — Java's package-private
    /// <c>CreateAclsResult(Map&lt;AclBinding, KafkaFuture&lt;Void&gt;&gt;)</c>. The dictionary is held by
    /// reference, as Java holds its map.
    /// </summary>
    internal CreateAclsResult(IReadOnlyDictionary<AclBinding, KafkaFuture> values)
    {
        _values = values;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// Presents the bridge's <c>bool</c> latches as Java's <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the
    /// void bridge's internal success token (result shape 2 — the ABI has no <c>get_value</c> here, so a null
    /// per-key error <em>is</em> the success value) and must not reach the public surface. Each void future is erased
    /// over its key's <b>same</b> latch — no second completion — with the bridge's own comparer, so the view and the
    /// sources cannot disagree about key identity.
    /// </remarks>
    internal static CreateAclsResult From(VoidKeyedAdminOperation<AclBinding> operation) =>
        new CreateAclsResult(SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer));

    /// <summary>
    /// One future per requested binding, keyed by the binding — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<AclBinding, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> binding has been created, and fails with the first
    /// failure in key order if any binding failed — Java's <c>all()</c> (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every binding.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
