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
using System.Linq;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

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

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public ListTransactionsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// The transaction states to filter on, or an empty collection — the default — to return
    /// transactions in every state. Java's <c>filterStates</c> and
    /// <c>filteredStates()</c>.
    /// </summary>
    /// <returns>Never <see langword="null"/>; a de-duplicated read-only copy.</returns>
    public IReadOnlyCollection<TransactionState> FilteredStates() => _filteredStates;

    /// <summary>Sets <see cref="FilteredStates()"/>.</summary>
    /// <param name="states">The transaction states to filter on, or an empty collection — the default — to return
    /// transactions in every state.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    /// <exception cref="ArgumentNullException">
    /// The value is <see langword="null"/>, which Java's <c>new HashSet&lt;&gt;(states)</c>
    /// rejects. An empty collection means every state.
    /// </exception>
    public ListTransactionsOptions FilterStates(IReadOnlyCollection<TransactionState> states)
    {
        _filteredStates = CopyOf(states, nameof(states));
        Setters.Ran(nameof(FilterStates));
        return this;
    }

    /// <summary>
    /// The producer ids to filter on, or an empty collection — the default — to return
    /// transactions from every producer. Java's <c>filterProducerIds</c> and
    /// <c>filteredProducerIds()</c>.
    /// </summary>
    /// <returns>Never <see langword="null"/>; a de-duplicated read-only copy.</returns>
    public IReadOnlyCollection<long> FilteredProducerIds() => _filteredProducerIds;

    /// <summary>Sets <see cref="FilteredProducerIds()"/>.</summary>
    /// <param name="producerIdFilters">The producer ids to filter on, or an empty collection — the default — to
    /// return transactions from every producer.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    /// <exception cref="ArgumentNullException">
    /// The value is <see langword="null"/>, which Java's
    /// <c>new HashSet&lt;&gt;(producerIdFilters)</c> rejects.
    /// </exception>
    public ListTransactionsOptions FilterProducerIds(IReadOnlyCollection<long> producerIdFilters)
    {
        _filteredProducerIds = CopyOf(producerIdFilters, nameof(producerIdFilters));
        Setters.Ran(nameof(FilterProducerIds));
        return this;
    }

    /// <summary>
    /// Return only transactions running longer than this many milliseconds; negative — the
    /// default <c>-1</c> — applies no duration filter. Java's
    /// <c>filterOnDuration</c> and <c>filteredDuration()</c>.
    /// </summary>
    /// <returns>Return only transactions running longer than this many milliseconds; negative — the default
    /// <c>-1</c> — applies no duration filter.</returns>
    /// <remarks><c>0</c> is a filter ("longer than 0 ms"), not the neutral value.</remarks>
    public long FilteredDuration() => _filteredDuration;

    /// <summary>Sets <see cref="FilteredDuration()"/>.</summary>
    /// <param name="durationMs">Return only transactions running longer than this many milliseconds; negative — the
    /// default <c>-1</c> — applies no duration filter.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public ListTransactionsOptions FilterOnDuration(long durationMs)
    {
        _filteredDuration = durationMs;
        Setters.Ran(nameof(FilterOnDuration));
        return this;
    }

    /// <summary>
    /// A transactional-id regular-expression pattern to filter on, or <see langword="null"/> —
    /// the default — for no filter. Java's <c>filterOnTransactionalIdPattern</c> and
    /// <c>filteredTransactionalIdPattern()</c>.
    /// </summary>
    /// <returns>A transactional-id regular-expression pattern to filter on, or <see langword="null"/> — the default
    /// — for no filter.</returns>
    /// <remarks>
    /// <see langword="null"/> and <c>""</c> are unequal values, as in Java's <c>equals</c>, but
    /// both mean "no pattern filter", as Java's javadoc says.
    /// </remarks>
    public string? FilteredTransactionalIdPattern() => _filteredTransactionalIdPattern;

    /// <summary>Sets <see cref="FilteredTransactionalIdPattern()"/>.</summary>
    /// <param name="pattern">A transactional-id regular-expression pattern to filter on, or <see langword="null"/>
    /// — the default — for no filter.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public ListTransactionsOptions FilterOnTransactionalIdPattern(string? pattern)
    {
        _filteredTransactionalIdPattern = pattern;
        Setters.Ran(nameof(FilterOnTransactionalIdPattern));
        return this;
    }

    /// <summary>
    /// Value equality over the four filters, without the timeout — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two options carry the same filters.</returns>
    public override bool Equals(object? obj) =>
        obj is ListTransactionsOptions other
        && _filteredDuration == other._filteredDuration
        && string.Equals(
            _filteredTransactionalIdPattern,
            other._filteredTransactionalIdPattern,
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

            hash = (hash * 31) + _filteredDuration.GetHashCode();
            return (hash * 31)
                + (_filteredTransactionalIdPattern is null
                    ? 0
                    : StringComparer.Ordinal.GetHashCode(_filteredTransactionalIdPattern));
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
            string.Join(
                ", ",
                _filteredStates.Select(static state => TransactionMarshal.NameFromState(state) ?? state.ToString())),
            string.Join(", ", _filteredProducerIds),
            _filteredDuration,
            _filteredTransactionalIdPattern ?? "null",
            _timeoutMs.HasValue ? _timeoutMs.Value.ToString(CultureInfo.InvariantCulture) : "null");

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
