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
using System.Globalization;
using System.Linq;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Which of one consumer group's committed offsets to list — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec</c>
/// (<c>ListConsumerGroupOffsetsSpec.java:28</c>), one per group in the map handed to
/// <c>listConsumerGroupOffsets</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Java's fluent setter <c>topicPartitions(Collection)</c> (<c>:39</c>) and its
/// same-named getter <c>topicPartitions()</c> (<c>:48</c>) collapse into the single
/// property <see cref="TopicPartitions"/></b>, the pairing
/// <see cref="CreateTopicsOptions"/> rules for every options type in this binding. This
/// class is not an options type — it takes no timeout and extends no
/// <c>AbstractOptions</c> — but its shape is identical, so it takes the identical
/// treatment.
/// </para>
/// <para>
/// ⚠ <b><see langword="null"/> is load-bearing and is not the same as an empty
/// collection.</b> Java's field is left uninitialized (<c>:30</c>) and both javadocs spell
/// out what that means: <c>null</c> "includes all topic partitions" (<c>:34</c>) /
/// "indicates that offsets of all partitions of the group are to be listed" (<c>:46</c>).
/// An explicitly empty collection is the opposite request — list nothing. The C ABI carries
/// the distinction as a per-group <c>all_partitions</c> boolean, so the later submit slice
/// must be able to tell the two apart; that is why <see cref="TopicPartitions"/> is
/// nullable rather than defaulting to an empty collection, the same absent-is-not-empty
/// discipline <see cref="ConsumerGroupDescription.AuthorizedOperations"/> follows.
/// </para>
/// <para>
/// ⚠ <b>The collection is copied on assignment, where Java stores the reference</b>
/// (<c>:40</c>). <c>IReadOnlyCollection&lt;T&gt;</c> is a read-only <em>view</em>, not an
/// immutable collection — the <c>List&lt;T&gt;</c> a caller passes stays mutable through
/// the caller's own reference. Copying is what makes <see cref="Equals(object)"/>,
/// <see cref="GetHashCode"/> and <see cref="ToString"/> stable, and what stops the later
/// submit slice from marshalling a selection different from the one that was assigned. The
/// copy preserves order and keeps duplicates, because Java's <c>Collection</c> is not a
/// <c>Set</c>. This is the type's one deliberate deviation from Java.
/// </para>
/// <para>
/// ⚠ <b><see cref="Equals(object)"/> compares element-by-element in order</b>, where Java
/// delegates to <c>Objects.equals</c> (<c>:61</c>) and therefore to whatever the supplied
/// collection's own <c>equals</c> happens to be — unspecified for the <c>Collection</c>
/// interface, order-sensitive for the <c>List</c> the setter's javadoc names (<c>:36</c>),
/// order-insensitive for a <c>Set</c>. Fixing it to the <c>List</c> behaviour is what makes
/// the comparison well defined at all; the alternative would be reference equality, which
/// would make two specs built from equal lists unequal.
/// </para>
/// </remarks>
public sealed class ListConsumerGroupOffsetsSpec
{
    // Null means "every partition the group has committed offsets for" — see the type
    // remarks. Copied on assignment and never mutated afterwards.
    private IReadOnlyList<TopicPartition>? _topicPartitions;

    /// <summary>
    /// The partitions whose offsets are to be listed for the group, or
    /// <see langword="null"/> for every partition the group has committed offsets for —
    /// Java's <c>topicPartitions()</c> (<c>:48</c>) over the field that starts out unset
    /// (<c>:30</c>).
    /// </summary>
    /// <remarks>
    /// <see langword="null"/> and an empty collection are different requests: see the type
    /// remarks. The value read back is a copy of what was assigned, not the same instance.
    /// </remarks>
    public IReadOnlyCollection<TopicPartition>? TopicPartitions
    {
        get => _topicPartitions;
        set => _topicPartitions = value is null ? null : new List<TopicPartition>(value);
    }

    /// <summary>
    /// Compares the selected partitions — Java's <c>equals</c> (<c>:53</c>).
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns>True when <paramref name="obj"/> selects the same partitions.</returns>
    /// <remarks>
    /// An unset selection equals only another unset selection — never an empty one, which
    /// is the distinction the whole type exists to carry. Java's
    /// <c>instanceof</c> check (<c>:57</c>) is met by the type being <see langword="sealed"/>.
    /// The comparison is order-sensitive; see the type remarks for why.
    /// </remarks>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj is not ListConsumerGroupOffsetsSpec other)
        {
            return false;
        }

        if (_topicPartitions is null || other._topicPartitions is null)
        {
            // Java: Objects.equals(null, null) is true, and null never equals a collection
            // — including an empty one (:61).
            return _topicPartitions is null && other._topicPartitions is null;
        }

        return _topicPartitions.SequenceEqual(other._topicPartitions);
    }

    /// <summary>
    /// Hashes the selected partitions — Java's <c>hashCode</c> (<c>:65</c>).
    /// </summary>
    /// <returns>The hash code.</returns>
    /// <remarks>
    /// Java's <c>Objects.hash(topicPartitions)</c> folds the collection's own hash, which
    /// for a <c>List</c> is an order-sensitive fold over the elements; this mirrors that
    /// structure with the 17/31 fold used across this binding, so it agrees with the
    /// order-sensitive <see cref="Equals(object)"/>. The numeric value differs from Java's
    /// because <see cref="TopicPartition.GetHashCode"/> is .NET's, not Java's — only the
    /// equal-implies-same-hash contract is portable.
    /// </remarks>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            if (_topicPartitions is null)
            {
                // Java: Objects.hash folds 0 for a null element (:66).
                return (hash * 31) + 0;
            }

            int elements = 1;
            foreach (var topicPartition in _topicPartitions)
            {
                elements = (elements * 31) + topicPartition.GetHashCode();
            }

            return (hash * 31) + elements;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:70</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// Java concatenates the collection directly, so an unset selection renders as
    /// <c>null</c> and a set one as Java's <c>AbstractCollection</c> rendering,
    /// <c>[first, second]</c> — an empty selection therefore renders as <c>[]</c>, visibly
    /// different from <c>null</c>.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ListConsumerGroupOffsetsSpec(topicPartitions={0})",
            _topicPartitions is null
                ? "null"
                : "[" + string.Join(", ", _topicPartitions.Select(static topicPartition => topicPartition.ToString())) + "]");
}
