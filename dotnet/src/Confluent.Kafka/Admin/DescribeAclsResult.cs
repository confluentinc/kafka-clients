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
/// The result of <see cref="IAdmin.DescribeAcls"/> — the .NET realization of Java's
/// <c>DescribeAclsResult</c>: <b>one</b> awaitable over the whole matching collection
/// (<c>DescribeAclsResult.java:30</c>).
/// </summary>
/// <remarks>
/// ⚠ <b>There is no <c>All()</c>.</b> Java publishes only <c>values()</c> (<c>:39</c>), and
/// <c>kafka_admin_DescribeAclsResult_t</c> declares no <c>get_error</c> of any kind — so
/// there is no per-key outcome to aggregate and any failure faults this single task.
/// </remarks>
public sealed class DescribeAclsResult
{
    private readonly Task<IReadOnlyCollection<AclBinding>> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>DescribeAclsResult(KafkaFuture&lt;Collection&lt;AclBinding&gt;&gt;)</c>
    /// (<c>:34</c>).
    /// </summary>
    internal DescribeAclsResult(Task<IReadOnlyCollection<AclBinding>> future)
    {
        _future = future;
    }

    /// <summary>
    /// The ACL bindings matching the filter, in the order the broker reported them —
    /// Java's <c>values()</c> (<c>:39</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the bindings, or faulting with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>
    /// This is the underlying task itself, so every call returns the <b>same</b> instance —
    /// no second task whose fault could go unobserved.
    /// </remarks>
    public Task<IReadOnlyCollection<AclBinding>> Values() => _future;
}
