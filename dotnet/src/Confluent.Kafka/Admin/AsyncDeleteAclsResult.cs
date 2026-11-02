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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DeleteAcls"/> — Java's
/// <c>org.apache.kafka.clients.admin.DeleteAclsResult</c>: one future per filter, each resolving
/// to the ACLs that filter matched.
/// </summary>
/// <remarks>
/// A filter that failed faults its own future. A matched ACL that could not be deleted is reported
/// as a value, in <see cref="DeleteAclsResult.FilterResult.Exception"/>, inside a
/// <see cref="DeleteAclsResult.FilterResults"/> whose future completed, so <c>Values[filter]</c>
/// can succeed while <see cref="All"/> faults.
/// </remarks>
/// <seealso cref="DeleteAclsResult"/>
public sealed class AsyncDeleteAclsResult
{
    private readonly IReadOnlyDictionary<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>> _futures;
    private readonly IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> _nativeFutures;
    private readonly SafeAdminResultHandle _native;

    private AsyncDeleteAclsResult(
        IReadOnlyDictionary<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>> futures,
        IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> nativeFutures,
        SafeAdminResultHandle native)
    {
        _futures = futures;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested filter, keyed by the filter — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>> Values => _futures;

    /// <summary>
    /// Every ACL deleted across every filter — Java's <c>all()</c>.
    /// </summary>
    /// <returns>
    /// The deleted bindings. Faults if any <em>filter</em> failed (with the first failure to complete), or with
    /// the first <see cref="DeleteAclsResult.FilterResult.Exception"/> encountered in filter order. Filters that
    /// matched nothing are not an error — the result is then a
    /// successful empty collection.
    /// </returns>
    public AsyncKafkaFuture<IReadOnlyCollection<AclBinding>> All() =>
        new AsyncKafkaFuture<IReadOnlyCollection<AclBinding>>(DeleteAclsResult.AllOf(_nativeFutures));

    internal static AsyncDeleteAclsResult OfClient(
        (IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> Values,
        SafeAdminResultHandle Result) native)
    {
        Dictionary<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>> futures =
            new Dictionary<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>>(native.Values.Count);
        foreach (KeyValuePair<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> entry in native.Values)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<DeleteAclsResult.FilterResults>(entry.Value));
        }

        return new AsyncDeleteAclsResult(futures, native.Values, native.Result);
    }
}
