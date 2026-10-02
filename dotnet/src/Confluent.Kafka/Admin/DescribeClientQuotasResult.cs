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
/// The result of <see cref="IAdmin.DescribeClientQuotas"/> — the .NET realization of Java's
/// <c>DescribeClientQuotasResult</c>: <b>one</b> awaitable over the whole entity-to-quota
/// map (<c>DescribeClientQuotasResult.java:31</c>).
/// </summary>
/// <remarks>
/// ⚠ <b>There is no <c>All()</c>.</b> Java publishes only <c>entities()</c> (<c>:46</c>), and
/// <c>kafka_admin_DescribeClientQuotasResult_t</c> declares no <c>get_error</c> of any kind —
/// so there is no per-entity outcome to aggregate and any failure faults this single task.
/// </remarks>
public sealed class DescribeClientQuotasResult
{
    private readonly Task<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>DescribeClientQuotasResult(KafkaFuture&lt;Map&lt;ClientQuotaEntity, Map&lt;String, Double&gt;&gt;&gt;)</c>
    /// (<c>:38</c>).
    /// </summary>
    internal DescribeClientQuotasResult(
        Task<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> future)
    {
        _future = future;
    }

    /// <summary>
    /// The quotas of every entity matching the filter — Java's <c>entities()</c>
    /// (<c>:46</c>). A quota type an entity has no value for is simply absent.
    /// </summary>
    /// <returns>
    /// A task yielding the map, or faulting with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>
    /// This is the underlying task itself, so every call returns the <b>same</b> instance —
    /// no second task whose fault could go unobserved.
    /// </remarks>
    public Task<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> Entities() =>
        _future;
}
