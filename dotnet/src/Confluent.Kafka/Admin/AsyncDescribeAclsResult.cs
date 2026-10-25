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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeAcls"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeAclsResult</c>: one future over the matching ACLs.
/// </summary>
/// <remarks>
/// Java publishes only <c>values()</c>, so there is no <c>All()</c>; any failure faults this one
/// future.
/// </remarks>
/// <seealso cref="DescribeAclsResult"/>
public sealed class AsyncDescribeAclsResult
{
    private readonly KafkaFutureImpl<IReadOnlyCollection<AclBinding>> _values;
    private readonly SafeAdminResultHandle _native;

    private AsyncDescribeAclsResult(KafkaFutureImpl<IReadOnlyCollection<AclBinding>> values, SafeAdminResultHandle native)
    {
        _values = values;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// The ACL bindings matching the filter, in the order the broker reported them —
    /// Java's <c>values()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the bindings, or faulting with the call's own
    /// <see cref="KafkaException"/>; on <see cref="AsyncMockAdminClient"/>, which does not implement the
    /// call, with a <see cref="System.NotSupportedException"/>.
    /// </returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture<IReadOnlyCollection<AclBinding>> Values() =>
        new AsyncKafkaFuture<IReadOnlyCollection<AclBinding>>(_values);

    internal static AsyncDescribeAclsResult OfClient(
        (KafkaFutureImpl<IReadOnlyCollection<AclBinding>> Values, SafeAdminResultHandle Result) native) =>
        new AsyncDescribeAclsResult(native.Values, native.Result);
}
