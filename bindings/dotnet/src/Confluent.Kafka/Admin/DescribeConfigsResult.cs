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
/// The result of <see cref="IAdmin.DescribeConfigs"/> — the .NET realization of Java's
/// <c>DescribeConfigsResult</c>: one awaitable <b>per resource</b>, handed back the moment
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
/// A resource that fails faults only <em>its own</em> <see cref="Task"/> — the header says
/// so: "a per-resource failure is not a call failure". The per-resource error is
/// <b>borrowed</b> from the result root and is never destroyed.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-resource <em>granularity</em> is fully
/// preserved, but per-resource <em>timing independence</em> is not: all the
/// <see cref="Task"/>s complete at the same instant, because the C ABI has no future type
/// and resolves every key together before reporting. The same recorded limitation as
/// <see cref="CreateTopicsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class DescribeConfigsResult
{
    private readonly IReadOnlyDictionary<ConfigResource, Task<Config>> _values;
    private readonly IEqualityComparer<ConfigResource> _comparer;

    /// <summary>
    /// Wraps the per-resource awaitables — Java's <c>protected</c>
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
        IReadOnlyDictionary<ConfigResource, Task<Config>> values, IEqualityComparer<ConfigResource> comparer)
    {
        _values = values;
        _comparer = comparer;
    }

    /// <summary>
    /// One awaitable per requested resource, keyed by resource — Java's <c>values()</c>
    /// (<c>:43</c>).
    /// </summary>
    public IReadOnlyDictionary<ConfigResource, Task<Config>> Values => _values;

    /// <summary>
    /// Every resource's configuration in one map, succeeding only if every resource
    /// succeeded — Java's <c>all()</c> (<c>:50</c>).
    /// </summary>
    /// <returns>A task yielding the whole map, or faulting with the first failure.</returns>
    public Task<IReadOnlyDictionary<ConfigResource, Config>> All() => Gather(_values, _comparer);

    private static async Task<IReadOnlyDictionary<ConfigResource, Config>> Gather(
        IReadOnlyDictionary<ConfigResource, Task<Config>> values, IEqualityComparer<ConfigResource> comparer)
    {
        // WhenAll first, so a failure faults this task with the first failure exactly as
        // Java's `allOf(...).thenApply(...)` chain does, rather than surfacing whichever
        // key happens to be enumerated first.
        await Task.WhenAll(values.Values).ConfigureAwait(false);

        Dictionary<ConfigResource, Config> configs =
            new Dictionary<ConfigResource, Config>(values.Count, comparer);
        foreach (KeyValuePair<ConfigResource, Task<Config>> entry in values)
        {
            configs.Add(entry.Key, entry.Value.Result);
        }

        return configs;
    }
}
