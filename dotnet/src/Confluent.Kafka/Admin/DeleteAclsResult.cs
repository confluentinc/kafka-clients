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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DeleteAcls"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DeleteAclsResult</c>: one awaitable <b>per filter</b>,
/// each resolving to the list of ACLs that filter matched.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>There are two independent error channels, and only one of them is a fault.</b> A
/// <em>filter</em> that failed — nothing was deleted for it — faults that filter's
/// <see cref="Task"/>. An individual matched ACL that could not be deleted is reported as a
/// <b>value</b>, in <see cref="FilterResult.Error"/>, inside a <see cref="FilterResults"/>
/// whose <see cref="Task"/> completed <em>successfully</em>. So
/// <c>Values[filter]</c> succeeding and <see cref="All"/> faulting is the correct, intended
/// outcome of an inner failure — the two are supposed to disagree.
/// </para>
/// </remarks>
public sealed class DeleteAclsResult
{
    private readonly IReadOnlyDictionary<AclBindingFilter, Task<FilterResults>> _values;

    /// <summary>Wraps the bridge's per-filter awaitables, which are already Java-shaped.</summary>
    internal DeleteAclsResult(IReadOnlyDictionary<AclBindingFilter, Task<FilterResults>> values) =>
        _values = values;

    /// <summary>
    /// One awaitable per requested filter, keyed by the filter — Java's <c>values()</c>
    /// (<c>:91</c>).
    /// </summary>
    public IReadOnlyDictionary<AclBindingFilter, Task<FilterResults>> Values => _values;

    /// <summary>
    /// Every ACL deleted across every filter — Java's <c>all()</c> (<c>:99</c>).
    /// </summary>
    /// <returns>
    /// The deleted bindings. Faults if any <em>filter</em> failed, or with the first
    /// <see cref="FilterResult.Error"/> encountered in filter order. Filters that matched
    /// nothing are not an error (<c>DeleteAclsResult.java:97-98</c>) — the result is then a
    /// successful empty collection.
    /// </returns>
    public async Task<IReadOnlyCollection<AclBinding>> All()
    {
        // Java derives all() from the per-filter futures rather than re-reading the result:
        // allOf(futures.values()).thenApply(v -> getAclBindings(futures)) (:99-121).
        Task<FilterResults>[] pending = _values.Values.ToArray();
        FilterResults[] resolved = await Task.WhenAll(pending).ConfigureAwait(false);

        List<AclBinding> bindings = new List<AclBinding>();
        foreach (FilterResults results in resolved)
        {
            foreach (FilterResult result in results.Values)
            {
                if (result.Error is not null)
                {
                    // Java: `if (result.exception() != null) throw result.exception();`
                    throw result.Error;
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
    /// and an error that is null only on success. So <see cref="Error"/>, not a null
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
        internal FilterResult(AclBinding? binding, KafkaException? error)
        {
            Binding = binding;
            Error = error;
        }

        /// <summary>
        /// The ACL the filter matched — deleted when <see cref="Error"/> is
        /// <see langword="null"/>, and still reported when deleting it failed — Java's
        /// <c>binding()</c> (<c>:51</c>). <see langword="null"/> only when the matched ACL could
        /// not be decoded, which then always comes with an <see cref="Error"/>: an entry
        /// carrying neither faults its filter's <see cref="Task"/> as a malformed result.
        /// </summary>
        public AclBinding? Binding { get; }

        /// <summary>
        /// Why deleting this ACL failed, or <see langword="null"/> if it was deleted —
        /// Java's <c>exception()</c> (<c>:58</c>).
        /// </summary>
        public KafkaException? Error { get; }
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
