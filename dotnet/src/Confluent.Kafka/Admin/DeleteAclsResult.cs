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
/// The result of <see cref="IAdmin.DeleteAcls"/> — Java's
/// <c>org.apache.kafka.clients.admin.DeleteAclsResult</c>: one future per filter, each resolving
/// to the ACLs that filter matched.
/// </summary>
/// <remarks>
/// A filter that failed fails its own future. A matched ACL that could not be deleted is reported
/// as a value, in <see cref="FilterResult.Exception"/>, inside a <see cref="FilterResults"/> whose
/// future completed, so <c>Values[filter]</c> can succeed while <see cref="All"/> fails.
/// </remarks>
/// <seealso cref="AsyncDeleteAclsResult"/>
public sealed class DeleteAclsResult
{
    private readonly IReadOnlyDictionary<AclBindingFilter, KafkaFuture<FilterResults>> _values;
    private readonly IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<FilterResults>> _nativeFutures;
    private readonly SafeAdminResultHandle _native;

    private DeleteAclsResult(
        IReadOnlyDictionary<AclBindingFilter, KafkaFuture<FilterResults>> values,
        IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<FilterResults>> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested filter, keyed by the filter — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<AclBindingFilter, KafkaFuture<FilterResults>> Values => _values;

    /// <summary>
    /// Every ACL deleted across every filter — Java's <c>all()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the deleted bindings. It fails if any <em>filter</em> failed (with the first failure to
    /// complete), or with the first <see cref="FilterResult.Exception"/> encountered in filter order. Filters that
    /// matched nothing are not an error — the result is then a successful empty collection.
    /// </returns>
    public KafkaFuture<IReadOnlyCollection<AclBinding>> All() =>
        new KafkaFuture<IReadOnlyCollection<AclBinding>>(AllOf(_nativeFutures));

    internal static KafkaFutureImpl<IReadOnlyCollection<AclBinding>> AllOf(
        IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<FilterResults>> futures) =>
        KafkaFutureImpl.GatherAll(futures, EqualityComparer<AclBindingFilter>.Default)
            .ThenApply<IReadOnlyCollection<AclBinding>>(resolved => AclBindings(futures.Keys, resolved));

    internal static DeleteAclsResult OfClient(
        (IReadOnlyDictionary<AclBindingFilter, KafkaFutureImpl<FilterResults>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<AclBindingFilter, KafkaFuture<FilterResults>> values =
            new Dictionary<AclBindingFilter, KafkaFuture<FilterResults>>(native.Values.Count);
        foreach (KeyValuePair<AclBindingFilter, KafkaFutureImpl<FilterResults>> entry in native.Values)
        {
            values.Add(entry.Key, new KafkaFuture<FilterResults>(entry.Value));
        }

        return new DeleteAclsResult(values, native.Values, native.Result);
    }

    private static IReadOnlyCollection<AclBinding> AclBindings(
        IEnumerable<AclBindingFilter> filters, IReadOnlyDictionary<AclBindingFilter, FilterResults> resolved)
    {
        List<AclBinding> bindings = new List<AclBinding>();
        foreach (AclBindingFilter filter in filters)
        {
            foreach (FilterResult result in resolved[filter].Values)
            {
                if (result.Exception is not null)
                {
                    throw result.Exception;
                }

                bindings.Add(result.Binding!);
            }
        }

        return bindings;
    }

    /// <summary>
    /// One ACL a filter matched: the matched binding, and the reason deleting it failed if
    /// it did — Java's nested <c>FilterResult</c>.
    /// </summary>
    /// <remarks>
    /// A matched ACL whose delete failed carries both its binding and its error, as Java's client
    /// builds it, so <see cref="Exception"/>, not a null <see cref="Binding"/>, says the delete failed.
    /// </remarks>
    public sealed class FilterResult
    {
        internal FilterResult(AclBinding? binding, KafkaException? exception)
        {
            Binding = binding;
            Exception = exception;
        }

        /// <summary>
        /// The ACL the filter matched — deleted when <see cref="Exception"/> is
        /// <see langword="null"/>, and still reported when deleting it failed — Java's
        /// <c>binding()</c>. <see langword="null"/> only when the matched ACL could
        /// not be decoded.
        /// </summary>
        public AclBinding? Binding { get; }

        /// <summary>
        /// Why deleting this ACL failed, or <see langword="null"/> if it was deleted —
        /// Java's <c>exception()</c>.
        /// </summary>
        public KafkaException? Exception { get; }
    }

    /// <summary>
    /// Everything one filter matched — Java's nested <c>FilterResults</c>.
    /// </summary>
    public sealed class FilterResults
    {
        internal FilterResults(IReadOnlyList<FilterResult> values) => Values = values;

        /// <summary>
        /// One entry per matched ACL — Java's <c>values()</c>. Empty when the
        /// filter matched nothing, which is not an error.
        /// </summary>
        public IReadOnlyList<FilterResult> Values { get; }
    }
}
