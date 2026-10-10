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
/// The result of <see cref="IAdmin.DescribeConfigs"/> — the .NET realization of Java's
/// <c>DescribeConfigsResult</c>: one future <b>per resource</b>, handed back the moment
/// the request is submitted (<c>DescribeConfigsResult.java:43, :50</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="All"/> here carries the whole map</b>
/// (<c>KafkaFuture&lt;Map&lt;ConfigResource, Config&gt;&gt;</c>), whereas
/// <see cref="AlterConfigsResult.All"/> is a bare <c>KafkaFuture&lt;Void&gt;</c>. The two
/// results are otherwise the same shape, so the difference is easy to swap by accident.
/// </para>
/// <para>
/// A resource that fails fails only <em>its own</em> <see cref="KafkaFuture{T}"/> — the header says
/// so: "a per-resource failure is not a call failure". The per-resource error is
/// <b>owned</b>: it arrives with that resource's own callback and is freed once it has
/// been copied into the <see cref="KafkaException"/>.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its resource (or, for
/// <see cref="All"/>, every resource) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeConfigsResult"/>
public sealed class DescribeConfigsResult
{
    private readonly IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>> _values;
    private readonly IEqualityComparer<ConfigResource> _comparer;

    /// <summary>
    /// Wraps the per-resource futures — Java's <c>protected</c>
    /// <c>DescribeConfigsResult(Map&lt;ConfigResource, KafkaFuture&lt;Config&gt;&gt;)</c>
    /// (<c>:35</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The comparer is threaded through deliberately, resolving the inconsistency the
    /// plan flagged.</b> Two shipped patterns exist — <c>DescribeTopicsResult</c> takes the
    /// bridge's comparer, <c>DeleteRecordsResult</c> does not — and the stricter one is
    /// chosen here for a concrete reason: <see cref="ConfigResource"/> is a reference type
    /// with custom value equality, so an aggregate built with a different comparer than the
    /// per-key map could disagree about whether a key is present. Passing the bridge's own
    /// comparer makes that unrepresentable rather than merely unlikely.
    /// </remarks>
    internal DescribeConfigsResult(
        IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>> values, IEqualityComparer<ConfigResource> comparer)
    {
        _values = values;
        _comparer = comparer;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeConfigsResult From(KeyedAdminOperation<ConfigResource, Config> operation) =>
        new DescribeConfigsResult(SyncAdminFutures.Wrap(operation.SyncSources), operation.KeyComparer);

    /// <summary>
    /// One future per requested resource, keyed by resource — Java's <c>values()</c>
    /// (<c>:43</c>).
    /// </summary>
    public IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>> Values => _values;

    /// <summary>
    /// Every resource's configuration in one map, succeeding only if every resource
    /// succeeded — Java's <c>all()</c> (<c>:50</c>).
    /// </summary>
    /// <returns>A future yielding the whole map, or failing with the first failure in key order.</returns>
    /// <remarks>
    /// Every future is waited for first, so a failure fails the aggregate with the first failure in key order exactly
    /// as Java's <c>allOf(...).thenApply(...)</c> chain does, rather than whichever key a reader happens to reach
    /// first. The map is keyed with the bridge's own comparer (see the constructor). Each call builds a new aggregate.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<ConfigResource, Config>> All() => SyncAdminFutures.GatherAll(_values, _comparer);
}
