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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for listing transactions — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListTransactionsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class ListTransactionsOptions
{
    private IReadOnlyCollection<TransactionState> _filteredStates = Array.Empty<TransactionState>();
    private IReadOnlyCollection<long> _filteredProducerIds = Array.Empty<long>();
    private int? _timeoutMs;
    private long _filteredDuration = -1L;
    private string? _filteredTransactionalIdPattern;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs
    {
        get => _timeoutMs;
        set
        {
            _timeoutMs = value;
            Setters.Ran(nameof(TimeoutMs));
        }
    }

    /// <summary>
    /// The transaction states to filter on, or an empty collection — the default — to return
    /// transactions in every state. Java's <c>filterStates</c> and
    /// <c>filteredStates()</c>.
    /// </summary>
    /// <value>Never <see langword="null"/>; a de-duplicated read-only copy.</value>
    /// <exception cref="ArgumentNullException">
    /// The value is <see langword="null"/>, which Java's <c>new HashSet&lt;&gt;(states)</c>
    /// rejects. An empty collection means every state.
    /// </exception>
    public IReadOnlyCollection<TransactionState> FilteredStates
    {
        get => _filteredStates;
        set
        {
            _filteredStates = CopyOf(value, nameof(value));
            Setters.Ran(nameof(FilteredStates));
        }
    }

    /// <summary>
    /// The producer ids to filter on, or an empty collection — the default — to return
    /// transactions from every producer. Java's <c>filterProducerIds</c> and
    /// <c>filteredProducerIds()</c>.
    /// </summary>
    /// <value>Never <see langword="null"/>; a de-duplicated read-only copy.</value>
    /// <exception cref="ArgumentNullException">
    /// The value is <see langword="null"/>, which Java's
    /// <c>new HashSet&lt;&gt;(producerIdFilters)</c> rejects.
    /// </exception>
    public IReadOnlyCollection<long> FilteredProducerIds
    {
        get => _filteredProducerIds;
        set
        {
            _filteredProducerIds = CopyOf(value, nameof(value));
            Setters.Ran(nameof(FilteredProducerIds));
        }
    }

    /// <summary>
    /// Return only transactions running longer than this many milliseconds; negative — the
    /// default <c>-1</c> — applies no duration filter. Java's
    /// <c>filterOnDuration</c> and <c>filteredDuration()</c>.
    /// </summary>
    /// <remarks><c>0</c> is a filter ("longer than 0 ms"), not the neutral value.</remarks>
    public long FilteredDuration
    {
        get => _filteredDuration;
        set
        {
            _filteredDuration = value;
            Setters.Ran(nameof(FilteredDuration));
        }
    }

    /// <summary>
    /// A transactional-id regular-expression pattern to filter on, or <see langword="null"/> —
    /// the default — for no filter. Java's <c>filterOnTransactionalIdPattern</c> and
    /// <c>filteredTransactionalIdPattern()</c>.
    /// </summary>
    /// <remarks>
    /// <see langword="null"/> and <c>""</c> are unequal values, as in Java's <c>equals</c>, but
    /// both mean "no pattern filter", as Java's javadoc says.
    /// </remarks>
    public string? FilteredTransactionalIdPattern
    {
        get => _filteredTransactionalIdPattern;
        set
        {
            _filteredTransactionalIdPattern = value;
            Setters.Ran(nameof(FilteredTransactionalIdPattern));
        }
    }

    /// <summary>
    /// Value equality over the four filters, without the timeout — Java's <c>equals</c>.
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

    /// <summary>The hash of the four filters, excluding the timeout — Java's <c>hashCode</c>.</summary>
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
    /// A diagnostic rendering matching Java's <c>toString()</c>, which includes the timeout.
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

    private static bool SetEquals<T>(IReadOnlyCollection<T> left, IReadOnlyCollection<T> right)
        where T : struct =>
        left.Count == right.Count && new HashSet<T>(left).SetEquals(right);

    internal OptionSetters Setters { get; } = new OptionSetters();
}
