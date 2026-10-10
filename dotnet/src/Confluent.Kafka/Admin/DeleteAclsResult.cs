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
/// The result of <see cref="IAdmin.DeleteAcls"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DeleteAclsResult</c>: one future <b>per filter</b>,
/// each resolving to the list of ACLs that filter matched.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>There are two independent error channels, and only one of them is a failure.</b> A
/// <em>filter</em> that failed — nothing was deleted for it — fails that filter's
/// <see cref="KafkaFuture{T}"/>. An individual matched ACL that could not be deleted is reported as a
/// <b>value</b>, in <see cref="FilterResult.Exception"/>, inside a <see cref="FilterResults"/>
/// whose <see cref="KafkaFuture{T}"/> completed <em>successfully</em>. So
/// <c>Values[filter]</c> succeeding and <see cref="All"/> failing is the correct, intended
/// outcome of an inner failure — the two are supposed to disagree.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its filter (or, for
/// <see cref="All"/>, every filter) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDeleteAclsResult"/>
public sealed class DeleteAclsResult
{
    private readonly IReadOnlyDictionary<AclBindingFilter, KafkaFuture<FilterResults>> _values;
    private readonly IEqualityComparer<AclBindingFilter> _comparer;

    /// <summary>
    /// Wraps one future per filter — Java's package-private
    /// <c>DeleteAclsResult(Map&lt;AclBindingFilter, KafkaFuture&lt;FilterResults&gt;&gt;)</c>. The dictionary is
    /// held by reference, as Java holds its map; <paramref name="comparer"/> is the one its keys were built with,
    /// which <see cref="All"/> gathers by.
    /// </summary>
    internal DeleteAclsResult(
        IReadOnlyDictionary<AclBindingFilter, KafkaFuture<FilterResults>> values,
        IEqualityComparer<AclBindingFilter> comparer)
    {
        _values = values;
        _comparer = comparer;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DeleteAclsResult From(
        KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> operation) =>
        new DeleteAclsResult(SyncAdminFutures.Wrap(operation.SyncSources), operation.KeyComparer);

    /// <summary>
    /// One future per requested filter, keyed by the filter — Java's <c>values()</c>
    /// (<c>:91</c>).
    /// </summary>
    public IReadOnlyDictionary<AclBindingFilter, KafkaFuture<FilterResults>> Values => _values;

    /// <summary>
    /// Every ACL deleted across every filter — Java's <c>all()</c> (<c>:99</c>).
    /// </summary>
    /// <returns>
    /// A future yielding the deleted bindings. Its <c>Get()</c> waits for every filter, then fails if any
    /// <em>filter</em> failed (the first in filter order), or with the first <see cref="FilterResult.Exception"/>
    /// encountered in filter order. Filters that matched nothing are not an error
    /// (<c>DeleteAclsResult.java:97-98</c>) — the result is then a successful empty collection.
    /// </returns>
    /// <remarks>
    /// Built per call, and its walk runs once, on the first <c>Get()</c>'s thread.
    /// </remarks>
    public KafkaFuture<IReadOnlyCollection<AclBinding>> All() =>
        // Java derives all() from the per-filter futures rather than re-reading the result:
        // allOf(futures.values()).thenApply(v -> getAclBindings(futures)) (:99-121).
        SyncAdminFutures.Project(SyncAdminFutures.GatherAll(_values, _comparer), AclBindings);

    private IReadOnlyCollection<AclBinding> AclBindings(IReadOnlyDictionary<AclBindingFilter, FilterResults> resolved)
    {
        List<AclBinding> bindings = new List<AclBinding>();
        foreach (AclBindingFilter filter in _values.Keys)
        {
            foreach (FilterResult result in resolved[filter].Values)
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

    /// <summary>
    /// One ACL a filter matched: the matched binding, and the reason deleting it failed if
    /// it did — Java's nested <c>FilterResult</c> (<c>:39</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Both can be non-null.</b> A matched ACL whose delete failed carries its binding
    /// <em>and</em> its error: Java builds every entry as
    /// <c>new FilterResult(aclBinding, aclError.exception(matchingAcl.errorMessage()))</c>
    /// (<c>KafkaAdminClient.java:2705-2708</c>), storing the decoded binding unconditionally
    /// and an error that is null only on success. So <see cref="Exception"/>, not a null
    /// <see cref="Binding"/>, is what says the delete failed. The binding is read independently
    /// of the error for the same reason (M15/P13.2 G4-1). Java's own <c>binding()</c> Javadoc
    /// ("or null if there was an error", <c>:49</c>) does not match the entries its client
    /// builds; this follows what the client builds.
    /// </remarks>
    public sealed class FilterResult
    {
        /// <summary>
        /// Java's package-private <c>FilterResult(AclBinding, ApiException)</c> (<c>:43</c>):
        /// both are stored as given. The error is a stored value, never a fault.
        /// </summary>
        internal FilterResult(AclBinding? binding, KafkaException? exception)
        {
            Binding = binding;
            Exception = exception;
        }

        /// <summary>
        /// The ACL the filter matched — deleted when <see cref="Exception"/> is
        /// <see langword="null"/>, and still reported when deleting it failed — Java's
        /// <c>binding()</c> (<c>:51</c>). <see langword="null"/> only when the matched ACL could
        /// not be decoded, which then always comes with an <see cref="Exception"/>: an entry
        /// carrying neither fails its filter's future as a malformed result.
        /// </summary>
        public AclBinding? Binding { get; }

        /// <summary>
        /// Why deleting this ACL failed, or <see langword="null"/> if it was deleted —
        /// Java's <c>exception()</c> (<c>:58</c>).
        /// </summary>
        /// <remarks>
        /// Named after Java's accessor <c>exception()</c> (M15/P13.5 G4-5); the type is the
        /// binding's flat <see cref="KafkaException"/>.
        /// </remarks>
        public KafkaException? Exception { get; }
    }

    /// <summary>
    /// Everything one filter matched — Java's nested <c>FilterResults</c> (<c>:66</c>).
    /// </summary>
    public sealed class FilterResults
    {
        /// <summary>Wraps the already-owned per-ACL outcomes, in the ABI's own order.</summary>
        internal FilterResults(IReadOnlyList<FilterResult> values) => Values = values;

        /// <summary>
        /// One entry per matched ACL — Java's <c>values()</c> (<c>:76</c>). Empty when the
        /// filter matched nothing, which is not an error.
        /// </summary>
        public IReadOnlyList<FilterResult> Values { get; }
    }
}
