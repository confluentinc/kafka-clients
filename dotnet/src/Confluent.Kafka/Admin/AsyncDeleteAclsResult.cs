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
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DeleteAcls"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DeleteAclsResult</c>: one awaitable <b>per filter</b>,
/// each resolving to the list of ACLs that filter matched.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>There are two independent error channels, and only one of them is a fault.</b> A
/// <em>filter</em> that failed — nothing was deleted for it — faults that filter's
/// <see cref="AsyncKafkaFuture"/>. An individual matched ACL that could not be deleted is reported as a
/// <b>value</b>, in <see cref="DeleteAclsResult.FilterResult.Exception"/>, inside a <see cref="DeleteAclsResult.FilterResults"/>
/// whose <see cref="AsyncKafkaFuture"/> completed <em>successfully</em>. So
/// <c>Values[filter]</c> succeeding and <see cref="All"/> faulting is the correct, intended
/// outcome of an inner failure — the two are supposed to disagree.
/// </para>
/// </remarks>
/// <seealso cref="DeleteAclsResult"/>
public sealed class AsyncDeleteAclsResult
{
    private readonly IReadOnlyDictionary<AclBindingFilter, Task<DeleteAclsResult.FilterResults>> _values;
    private readonly IReadOnlyDictionary<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>> _futures;

    /// <summary>Wraps the bridge's per-filter awaitables, which are already Java-shaped.</summary>
    internal AsyncDeleteAclsResult(IReadOnlyDictionary<AclBindingFilter, Task<DeleteAclsResult.FilterResults>> values)
    {
        _values = values;
        _futures = AsyncAdminFutures.Wrap(values);
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncDeleteAclsResult From(
        KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> operation) =>
        new AsyncDeleteAclsResult(operation.Tasks);

    /// <summary>
    /// One awaitable per requested filter, keyed by the filter — Java's <c>values()</c>
    /// (<c>:91</c>).
    /// </summary>
    public IReadOnlyDictionary<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>> Values => _futures;

    /// <summary>
    /// Every ACL deleted across every filter — Java's <c>all()</c> (<c>:99</c>).
    /// </summary>
    /// <returns>
    /// The deleted bindings. Faults if any <em>filter</em> failed, or with the first
    /// <see cref="DeleteAclsResult.FilterResult.Exception"/> encountered in filter order. Filters that matched
    /// nothing are not an error (<c>DeleteAclsResult.java:97-98</c>) — the result is then a
    /// successful empty collection.
    /// </returns>
    public AsyncKafkaFuture<IReadOnlyCollection<AclBinding>> All() =>
        new AsyncKafkaFuture<IReadOnlyCollection<AclBinding>>(AllCore());

    private async Task<IReadOnlyCollection<AclBinding>> AllCore()
    {
        // Java derives all() from the per-filter futures rather than re-reading the result:
        // allOf(futures.values()).thenApply(v -> getAclBindings(futures)) (:99-121).
        Task<DeleteAclsResult.FilterResults>[] pending = _values.Values.ToArray();
        DeleteAclsResult.FilterResults[] resolved = await Task.WhenAll(pending).ConfigureAwait(false);

        List<AclBinding> bindings = new List<AclBinding>();
        foreach (DeleteAclsResult.FilterResults results in resolved)
        {
            foreach (DeleteAclsResult.FilterResult result in results.Values)
            {
                if (result.Exception is not null)
                {
                    // Java: `if (result.exception() != null) throw result.exception();`
                    throw result.Exception;
                }

                bindings.Add(result.Binding!);
            }
        }

        return bindings;
    }
}
