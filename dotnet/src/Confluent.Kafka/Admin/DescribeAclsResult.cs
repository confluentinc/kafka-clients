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
/// The result of <see cref="IAdmin.DescribeAcls"/> — the .NET realization of Java's
/// <c>DescribeAclsResult</c>: <b>one</b> future over the whole matching collection
/// (<c>DescribeAclsResult.java:30</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>There is no <c>All()</c>.</b> Java publishes only <c>values()</c> (<c>:39</c>), and
/// <c>kafka_admin_DescribeAclsResult_t</c> declares no <c>get_error</c> of any kind — so
/// there is no per-key outcome to aggregate and any failure fails this single future.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the description has completed, and
/// needs no thread-pool thread: the future here is the operation's own latch, never a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeAclsResult"/>
public sealed class DescribeAclsResult
{
    private readonly KafkaFuture<IReadOnlyCollection<AclBinding>> _future;

    /// <summary>
    /// Wraps the single future — Java's package-private
    /// <c>DescribeAclsResult(KafkaFuture&lt;Collection&lt;AclBinding&gt;&gt;)</c>
    /// (<c>:34</c>).
    /// </summary>
    internal DescribeAclsResult(KafkaFuture<IReadOnlyCollection<AclBinding>> future)
    {
        _future = future;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeAclsResult From(SingleAdminOperation<IReadOnlyCollection<AclBinding>> operation) =>
        new DescribeAclsResult(new KafkaFuture<IReadOnlyCollection<AclBinding>>(operation.SyncSource));

    /// <summary>
    /// The ACL bindings matching the filter, in the order the broker reported them —
    /// Java's <c>values()</c> (<c>:39</c>).
    /// </summary>
    /// <returns>
    /// A future yielding the bindings, or failing with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>
    /// This is a future over the operation's own latch, so every call returns an <b>equal</b> future — no second
    /// completion.
    /// </remarks>
    public KafkaFuture<IReadOnlyCollection<AclBinding>> Values() => _future;
}
