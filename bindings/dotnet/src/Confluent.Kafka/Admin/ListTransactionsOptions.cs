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

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for listing transactions — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListTransactionsOptions</c>
/// (<c>ListTransactionsOptions.java:29</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// ⚠ <b>Three different neutral encodings.</b> The two collection filters conflate null with
/// empty and both mean "every transaction"; <see cref="FilteredDuration"/> is neutral at
/// <c>-1</c>, so a <c>0</c> is a real filter; and <see cref="FilteredTransactionalIdPattern"/>
/// distinguishes <see langword="null"/> (no filter) from <c>""</c> (a pattern the broker
/// evaluates). Java's javadoc (<c>:119-120</c>) says the pattern's "empty" means no filter, but
/// the field has no initializer, so the default is <see langword="null"/> — the code is the
/// contract.
/// </para>
/// <para>
/// Deviation: Java's <c>filteredStates()</c> (<c>:93</c>) / <c>filteredProducerIds()</c>
/// (<c>:103</c>) hand back the live mutable <c>HashSet</c>; these properties expose read-only
/// de-duplicated copies, so a caller cannot mutate the stored filter through the returned
/// collection.
/// </para>
/// </remarks>
public sealed class ListTransactionsOptions
{
    private IReadOnlyCollection<TransactionState> _filteredStates = Array.Empty<TransactionState>();
    private IReadOnlyCollection<long> _filteredProducerIds = Array.Empty<long>();

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// The transaction states to filter on, or an empty collection — the default — to return
    /// transactions in every state. Java's <c>filterStates</c> (<c>:43</c>) and
    /// <c>filteredStates()</c> (<c>:93</c>).
    /// </summary>
    /// <value>Never <see langword="null"/>; a de-duplicated read-only copy.</value>
    /// <exception cref="ArgumentNullException">
    /// The value is <see langword="null"/> — Java's <c>new HashSet&lt;&gt;(states)</c> (<c>:44</c>)
    /// raises a <c>NullPointerException</c>. There is no way to ask for "no states": empty means all.
    /// </exception>
    public IReadOnlyCollection<TransactionState> FilteredStates
    {
        get => _filteredStates;
        set => _filteredStates = CopyOf(value, nameof(value));
    }

    /// <summary>
    /// The producer ids to filter on, or an empty collection — the default — to return
    /// transactions from every producer. Java's <c>filterProducerIds</c> (<c>:56</c>) and
    /// <c>filteredProducerIds()</c> (<c>:103</c>).
    /// </summary>
    /// <value>Never <see langword="null"/>; a de-duplicated read-only copy.</value>
    /// <exception cref="ArgumentNullException">
    /// The value is <see langword="null"/> — Java's <c>new HashSet&lt;&gt;(producerIdFilters)</c>
    /// (<c>:57</c>) raises a <c>NullPointerException</c>.
    /// </exception>
    public IReadOnlyCollection<long> FilteredProducerIds
    {
        get => _filteredProducerIds;
        set => _filteredProducerIds = CopyOf(value, nameof(value));
    }

    /// <summary>
    /// Return only transactions running longer than this many milliseconds; negative — the
    /// default <c>-1</c> (<c>:33</c>) — applies no duration filter. Java's
    /// <c>filterOnDuration</c> (<c>:69</c>) and <c>filteredDuration()</c> (<c>:112</c>).
    /// </summary>
    /// <remarks>⚠ <c>0</c> is a real filter ("longer than 0 ms"), not the neutral value.</remarks>
    public long FilteredDuration { get; set; } = -1L;

    /// <summary>
    /// A transactional-id regular-expression pattern to filter on, or <see langword="null"/> —
    /// the default — for no filter. Java's <c>filterOnTransactionalIdPattern</c> (<c>:82</c>) and
    /// <c>filteredTransactionalIdPattern()</c> (<c>:122</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <see langword="null"/> and <c>""</c> are distinct at the boundary: the empty string is a
    /// legal pattern the broker evaluates, so it must not be normalized to <see langword="null"/>
    /// or vice versa.
    /// </remarks>
    public string? FilteredTransactionalIdPattern { get; set; }

    /// <summary>
    /// Value equality over the four filters, deliberately <em>excluding</em> the timeout — Java's
    /// <c>equals</c> (<c>:138</c>), unlike <see cref="DescribeProducersOptions.Equals(object?)"/>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two options carry the same filters.</returns>
    public override bool Equals(object? obj) =>
        obj is ListTransactionsOptions other
        && FilteredDuration == other.FilteredDuration
        && string.Equals(
            FilteredTransactionalIdPattern,
            other.FilteredTransactionalIdPattern,
            StringComparison.Ordinal)
        && SetEquals(_filteredStates, other._filteredStates)
        && SetEquals(_filteredProducerIds, other._filteredProducerIds);

    /// <summary>The hash of the four filters, excluding the timeout — Java's <c>hashCode</c> (<c>:149</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 0;
            foreach (TransactionState state in _filteredStates)
            {
                hash += (int)state;
            }

            foreach (long producerId in _filteredProducerIds)
            {
                hash += producerId.GetHashCode();
            }

            hash = (hash * 31) + FilteredDuration.GetHashCode();
            return (hash * 31)
                + (FilteredTransactionalIdPattern is null
                    ? 0
                    : StringComparer.Ordinal.GetHashCode(FilteredTransactionalIdPattern));
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c> (<c>:127</c>), which — unlike
    /// <see cref="Equals(object?)"/> — does include the timeout.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ListTransactionsOptions(filteredStates=[{0}], filteredProducerIds=[{1}]"
                + ", filteredDuration={2}, filteredTransactionalIdPattern={3}, timeoutMs={4})",
            string.Join(", ", _filteredStates),
            string.Join(", ", _filteredProducerIds),
            FilteredDuration,
            FilteredTransactionalIdPattern ?? "null",
            TimeoutMs.HasValue ? TimeoutMs.Value.ToString(CultureInfo.InvariantCulture) : "null");

    /// <summary>Java's <c>new HashSet&lt;&gt;(x)</c> — de-duplicating, null-rejecting.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The caller's collection.</param>
    /// <param name="parameterName">The setter's parameter name, for the exception.</param>
    /// <returns>An immutable, de-duplicated copy.</returns>
    private static IReadOnlyCollection<T> CopyOf<T>(IReadOnlyCollection<T> value, string parameterName)
        where T : struct
    {
        if (value is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        return value.Count == 0
            ? (IReadOnlyCollection<T>)Array.Empty<T>()
            : new ReadOnlyCollection<T>(new List<T>(new HashSet<T>(value)));
    }

    /// <summary>Order-independent comparison, matching the <c>Set</c> Java compares.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="left">The first collection.</param>
    /// <param name="right">The second collection.</param>
    /// <returns>Whether the two hold the same elements.</returns>
    private static bool SetEquals<T>(IReadOnlyCollection<T> left, IReadOnlyCollection<T> right)
        where T : struct =>
        left.Count == right.Count && new HashSet<T>(left).SetEquals(right);
}
