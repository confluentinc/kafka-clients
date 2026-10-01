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
using System.Globalization;
using System.Linq;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The partitions assigned to one group member — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.MemberAssignment</c>
/// (<c>MemberAssignment.java:29, :37, :42, :52, :59, :64</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Java's <c>Set&lt;TopicPartition&gt;</c> is exposed as an
/// <c>IReadOnlyCollection&lt;T&gt;</c>.</b> <c>IReadOnlySet&lt;T&gt;</c> post-dates the
/// netstandard2.0 floor, so it is unavailable on the oldest target framework — the same
/// substitution <see cref="TopicDescription.AuthorizedOperations"/> and the consumer
/// surface already make. The <em>set</em> semantics are not lost: the constructor
/// deduplicates, and <see cref="Equals(object)"/> / <see cref="GetHashCode"/> below are
/// order-insensitive, exactly as a Java <c>Set</c> is.
/// </para>
/// <para>
/// ⚠ <b>A null argument is accepted and becomes an empty assignment</b>, which is
/// <em>not</em> the strict <see cref="ArgumentNullException"/> guard
/// <see cref="ConsumerGroupListing"/> and <see cref="TopicDescription"/> apply. The
/// difference is Java's, not this binding's: Java coalesces here
/// (<c>topicPartitions == null ? Collections.emptySet() : ...</c>, <c>:38</c>) where those
/// classes reject, so mirroring the guard would reject a call Java accepts.
/// </para>
/// <para>
/// ⚠ <b>The collection is copied defensively</b>, mirroring Java's <c>Set.copyOf</c>
/// (<c>:38</c>): a caller that keeps mutating the collection it passed in cannot change an
/// assignment that has already been constructed.
/// </para>
/// <para>
/// ⚠ <b><see cref="ToString"/> renders in construction order, where Java's renders in
/// <c>Set</c> iteration order.</b> Java joins <c>topicPartitions.stream()</c> (<c>:65</c>),
/// whose order <c>Set</c> does not specify — so Java's own rendering of a given assignment
/// is not reproducible run to run. Both spellings carry the same elements; fixing the
/// order here is what makes the rendering assertable, and no Java-specified order is being
/// overridden because Java specifies none.
/// </para>
/// </remarks>
public sealed class MemberAssignment
{
    // Deduplicated at construction and never mutated afterwards, so it is a set that also
    // remembers the order it was given — see the ToString note in the type remarks.
    private readonly List<TopicPartition> _topicPartitions;

    /// <summary>
    /// Creates an assignment over the given partitions — Java's only constructor
    /// (<c>MemberAssignment.java:37</c>).
    /// </summary>
    /// <param name="topicPartitions">
    /// The assigned partitions, or <see langword="null"/> for none. Duplicates are
    /// collapsed and the collection is copied; see the type remarks.
    /// </param>
    public MemberAssignment(IEnumerable<TopicPartition>? topicPartitions)
    {
        if (topicPartitions is null)
        {
            // Java: Collections.emptySet() (:38).
            _topicPartitions = new List<TopicPartition>();
            return;
        }

        // Java: Set.copyOf(...) (:38) — a defensive copy that also collapses duplicates.
        var seen = new HashSet<TopicPartition>();
        _topicPartitions = new List<TopicPartition>();
        foreach (var topicPartition in topicPartitions)
        {
            if (seen.Add(topicPartition))
            {
                _topicPartitions.Add(topicPartition);
            }
        }
    }

    /// <summary>
    /// The partitions assigned to the group member — Java's <c>topicPartitions()</c>
    /// (<c>:59</c>). Never <see langword="null"/>; empty means "assigned nothing".
    /// </summary>
    public IReadOnlyCollection<TopicPartition> TopicPartitions => _topicPartitions;

    /// <summary>
    /// Compares the assigned partitions as a set — Java's <c>equals</c> (<c>:42</c>),
    /// which delegates to <c>Set.equals</c> and is therefore order-insensitive.
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when <paramref name="obj"/> assigns the same partitions.</returns>
    /// <remarks>
    /// Java's <c>getClass() != o.getClass()</c> check (<c>:44</c>) is met by the type being
    /// <see langword="sealed"/>: no subclass can reach this comparison.
    /// </remarks>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj is MemberAssignment other
            && _topicPartitions.Count == other._topicPartitions.Count
            && new HashSet<TopicPartition>(_topicPartitions).SetEquals(other._topicPartitions);
    }

    /// <summary>
    /// The sum of the assigned partitions' hashes — Java's <c>hashCode</c> (<c>:52</c>),
    /// which returns <c>topicPartitions.hashCode()</c>, and <c>Set.hashCode()</c> is
    /// specified as that sum.
    /// </summary>
    /// <returns>The hash code.</returns>
    /// <remarks>
    /// The 17/31 fold this binding uses elsewhere is deliberately <b>not</b> applied: a
    /// positional fold is order-sensitive, which would let two equal assignments hash
    /// differently. The sum is order-insensitive, matching <see cref="Equals(object)"/>.
    /// </remarks>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 0;
            foreach (var topicPartition in _topicPartitions)
            {
                hash += topicPartition.GetHashCode();
            }

            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:64</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// Java joins each partition's own <c>toString()</c> with <c>","</c> and wraps the
    /// result in <c>(topicPartitions=...)</c>; an empty assignment therefore renders as
    /// <c>(topicPartitions=)</c>. See the ordering note in the type remarks.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(topicPartitions={0})",
            string.Join(",", _topicPartitions.Select(static topicPartition => topicPartition.ToString())));
}
