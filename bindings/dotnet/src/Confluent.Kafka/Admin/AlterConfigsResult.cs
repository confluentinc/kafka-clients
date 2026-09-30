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
/// The result of <see cref="IAdmin.IncrementalAlterConfigs"/> — the .NET realization of
/// Java's <c>AlterConfigsResult</c>: one awaitable <b>per resource</b>, handed back the
/// moment the request is submitted (<c>AlterConfigsResult.java:39, :46</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="All"/> here is a bare <see cref="Task"/></b> — Java's
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
/// awaiting one of these tells you whether that resource was altered and hands back
/// nothing; a resource that fails faults only <em>its own</em> <see cref="Task"/>. The
/// per-resource error is <b>borrowed</b> from the result root and is never destroyed.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-resource <em>granularity</em> is fully
/// preserved, but per-resource <em>timing independence</em> is not — the same recorded
/// limitation as <see cref="CreatePartitionsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class AlterConfigsResult
{
    private readonly IReadOnlyDictionary<ConfigResource, Task> _values;

    /// <summary>
    /// Presents the bridge's <c>Task&lt;bool&gt;</c> sources as Java's
    /// <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void bridge's internal
    /// success token (result shape 2 — the ABI has no <c>get_value</c> here, which its own
    /// header states, so a null per-key error <em>is</em> the success value) and must not
    /// reach the public surface.
    /// </summary>
    /// <remarks>
    /// The erasure is a reference upcast, so each entry is the <b>same</b>
    /// <see cref="Task"/> instance as the bridge's — no per-key allocation, and no second
    /// task whose fault could go unobserved. It reuses the bridge's own comparer so the
    /// view and the sources cannot disagree about key identity.
    /// </remarks>
    internal AlterConfigsResult(
        IReadOnlyDictionary<ConfigResource, Task<bool>> values, IEqualityComparer<ConfigResource> comparer)
    {
        Dictionary<ConfigResource, Task> erased = new Dictionary<ConfigResource, Task>(values.Count, comparer);
        foreach (KeyValuePair<ConfigResource, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        _values = erased;
    }

    /// <summary>
    /// One awaitable per requested resource, keyed by resource — Java's <c>values()</c>
    /// (<c>:39</c>).
    /// </summary>
    public IReadOnlyDictionary<ConfigResource, Task> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> resource has been altered, and faults with the first
    /// failure if any resource failed — Java's <c>all()</c> (<c>:46</c>,
    /// <c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_values.Values);
}
