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
/// The result of <see cref="IAdmin.IncrementalAlterConfigs"/> — the .NET realization of
/// Java's <c>AlterConfigsResult</c>: one future <b>per resource</b>, handed back the
/// moment the request is submitted (<c>AlterConfigsResult.java:39, :46</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="All"/> here is a bare <see cref="KafkaFuture"/></b> — Java's
/// <c>KafkaFuture&lt;Void&gt;</c> — whereas <see cref="DescribeConfigsResult.All"/> yields
/// the whole map. The two results are otherwise the same shape.
/// </para>
/// <para>
/// ⚠ <b>The type is named for Java's return type, not for the RPC.</b> Java's signature is
/// <c>AlterConfigsResult incrementalAlterConfigs(...)</c> (<c>Admin.java:501, :530</c>),
/// and the ABI agrees — its result handle is
/// <c>kafka_admin_AlterConfigsResult_t</c>, with no symbol named for
/// <c>IncrementalAlterConfigs</c> at all.
/// </para>
/// <para>
/// Java publishes <c>Map&lt;ConfigResource, KafkaFuture&lt;Void&gt;&gt; values()</c>, so
/// waiting for one of these tells you whether that resource was altered and hands back
/// nothing; a resource that fails fails only <em>its own</em> <see cref="KafkaFuture"/>. The
/// per-resource error is <b>owned</b>: it arrives with that resource's own callback and is
/// freed once it has been copied into the <see cref="KafkaException"/>.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its resource (or, for
/// <see cref="All"/>, every resource) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncAlterConfigsResult"/>
public sealed class AlterConfigsResult
{
    private readonly IReadOnlyDictionary<ConfigResource, KafkaFuture> _values;

    /// <summary>
    /// Wraps one future per resource — Java's package-private
    /// <c>AlterConfigsResult(Map&lt;ConfigResource, KafkaFuture&lt;Void&gt;&gt;)</c> (<c>:32</c>). The dictionary is
    /// held by reference, as Java holds its map.
    /// </summary>
    internal AlterConfigsResult(IReadOnlyDictionary<ConfigResource, KafkaFuture> values)
    {
        _values = values;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// Presents the bridge's <c>bool</c> latches as Java's <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the
    /// void bridge's internal success token (result shape 2 — the ABI has no <c>get_value</c> here, which its own
    /// header states, so a null per-key error <em>is</em> the success value) and must not reach the public surface.
    /// Each void future is erased over its key's <b>same</b> latch — no second completion — with the bridge's own
    /// comparer, so the view and the sources cannot disagree about key identity.
    /// </remarks>
    internal static AlterConfigsResult From(VoidKeyedAdminOperation<ConfigResource> operation) =>
        new AlterConfigsResult(SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer));

    /// <summary>
    /// One future per requested resource, keyed by resource — Java's <c>values()</c>
    /// (<c>:39</c>).
    /// </summary>
    public IReadOnlyDictionary<ConfigResource, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> resource has been altered, and fails with the first
    /// failure in key order if any resource failed — Java's <c>all()</c> (<c>:46</c>,
    /// <c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every resource.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
