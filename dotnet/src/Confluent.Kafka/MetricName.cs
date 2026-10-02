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
using System.Text;

namespace Confluent.Kafka;

/// <summary>
/// The name of a metric — the .NET realization of Java's
/// <c>org.apache.kafka.common.MetricName</c>. It carries a <see cref="Name"/>,
/// <see cref="Group"/>, <see cref="Description"/>, and a set of <see cref="Tags"/>, and it
/// is the key type of the <see cref="IReadOnlyDictionary{TKey, TValue}"/> returned by
/// <see cref="IConsumerCommon.Metrics"/> (Java <c>Map&lt;MetricName, ? extends Metric&gt;
/// metrics()</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Value identity over <c>(Name, Group, Tags)</c> — Description excluded.</b> Two
/// <see cref="MetricName"/>s are equal when their <see cref="Name"/>, <see cref="Group"/>,
/// and <see cref="Tags"/> match; <see cref="Description"/> is deliberately <b>not</b> part
/// of equality or the hash, mirroring Java's <c>MetricName.equals</c> /
/// <c>hashCode</c> exactly (Java compares <c>group</c>, <c>name</c>, <c>tags</c>). This is
/// what makes per-partition metrics — which share a name and group and differ only by a
/// tag such as <c>topic</c> / <c>partition</c> — distinct dictionary keys.
/// </para>
/// <para>
/// <b>Tag comparison is order-independent and hand-implemented</b> (it does <b>not</b> rely
/// on <see cref="IReadOnlyDictionary{TKey, TValue}"/>'s own <c>Equals</c>, which is
/// reference equality): two tag sets are equal when they hold the same key/value pairs
/// regardless of enumeration order, matching Java's <c>Map.equals</c> (a set-of-entries
/// comparison). <see cref="GetHashCode"/> is computed order-independently over the same
/// three fields so it stays consistent with <see cref="Equals(MetricName)"/>.
/// </para>
/// <para>
/// <b>Defensive tag copy (deviation from Java, recorded).</b> The constructor copies the
/// incoming <c>tags</c> into an internal ordinal-keyed snapshot, so a
/// <see cref="MetricName"/> is immutable and its equality is independent of the caller's
/// dictionary comparer (Java stores the reference; the observable content-equality contract
/// is unchanged). All string comparisons are ordinal (Java's <see cref="string"/> /
/// <c>HashMap</c> semantics).
/// </para>
/// </remarks>
public sealed class MetricName : IEquatable<MetricName>
{
    private readonly Dictionary<string, string> _tags;

    // Java caches its hash (MetricName.hash); the same lazy cache here. 0 = not yet
    // computed. A genuine hash of 0 recomputes each call — harmless and rare.
    private int _hash;

    /// <summary>
    /// Initializes a new <see cref="MetricName"/> (Java
    /// <c>MetricName(String, String, String, Map&lt;String, String&gt;)</c>).
    /// </summary>
    /// <param name="name">The metric name.</param>
    /// <param name="group">The logical group the metric belongs to.</param>
    /// <param name="description">A human-readable description (excluded from equality).</param>
    /// <param name="tags">The metric's key/value tag attributes (copied).</param>
    /// <exception cref="ArgumentNullException">
    /// Any of <paramref name="name"/>, <paramref name="group"/>,
    /// <paramref name="description"/>, or <paramref name="tags"/> is null (Java requires all
    /// four non-null).
    /// </exception>
    public MetricName(string name, string group, string description, IReadOnlyDictionary<string, string> tags)
    {
        // Java: Objects.requireNonNull on all four (description included).
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Group = group ?? throw new ArgumentNullException(nameof(group));
        Description = description ?? throw new ArgumentNullException(nameof(description));
        if (tags is null)
        {
            throw new ArgumentNullException(nameof(tags));
        }

        // Defensive, ordinal-keyed copy: immutable snapshot, comparer-independent equality.
        _tags = new Dictionary<string, string>(tags.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> tag in tags)
        {
            _tags[tag.Key] = tag.Value;
        }
    }

    /// <summary>The metric name (Java <c>name()</c>).</summary>
    public string Name { get; }

    /// <summary>The logical group the metric belongs to (Java <c>group()</c>).</summary>
    public string Group { get; }

    /// <summary>
    /// A human-readable description (Java <c>description()</c>). <b>Not</b> part of
    /// <see cref="Equals(MetricName)"/> / <see cref="GetHashCode"/>.
    /// </summary>
    public string Description { get; }

    /// <summary>
    /// The metric's key/value tag attributes (Java <c>tags()</c>) — an immutable,
    /// ordinal-keyed snapshot.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags => _tags;

    /// <summary>Determines whether two <see cref="MetricName"/> values are equal.</summary>
    public static bool operator ==(MetricName? left, MetricName? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>Determines whether two <see cref="MetricName"/> values are not equal.</summary>
    public static bool operator !=(MetricName? left, MetricName? right) => !(left == right);

    /// <summary>
    /// Determines whether this equals <paramref name="other"/> — <see cref="Name"/>,
    /// <see cref="Group"/>, and <see cref="Tags"/> all match (order-independent tags,
    /// ordinal strings). <see cref="Description"/> is excluded, mirroring Java.
    /// </summary>
    public bool Equals(MetricName? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return string.Equals(Name, other.Name, StringComparison.Ordinal)
            && string.Equals(Group, other.Group, StringComparison.Ordinal)
            && TagsEqual(_tags, other._tags);
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is MetricName other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        if (_hash != 0)
        {
            return _hash;
        }

        // Mirror Java's structure (prime 31, over group, name, tags — description
        // excluded). The tag term is an order-independent aggregate so the hash stays
        // consistent with the order-independent Equals.
        unchecked
        {
            const int prime = 31;
            int result = 1;
            result = (prime * result) + StringComparer.Ordinal.GetHashCode(Group);
            result = (prime * result) + StringComparer.Ordinal.GetHashCode(Name);
            result = (prime * result) + TagsHashCode(_tags);
            _hash = result;
            return result;
        }
    }

    /// <summary>Returns the Java-style diagnostic string.</summary>
    public override string ToString()
    {
        StringBuilder builder = new StringBuilder();
        builder.Append("MetricName [name=").Append(Name)
            .Append(", group=").Append(Group)
            .Append(", description=").Append(Description)
            .Append(", tags={");
        bool first = true;
        foreach (KeyValuePair<string, string> tag in _tags)
        {
            if (!first)
            {
                builder.Append(", ");
            }

            builder.Append(tag.Key).Append('=').Append(tag.Value);
            first = false;
        }

        builder.Append("}]");
        return builder.ToString();
    }

    // Order-independent set-of-entries comparison (Java Map.equals). Both maps are ordinal-
    // keyed, so TryGetValue lookups are ordinal; values are compared ordinally.
    private static bool TagsEqual(Dictionary<string, string> a, Dictionary<string, string> b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a.Count != b.Count)
        {
            return false;
        }

        foreach (KeyValuePair<string, string> entry in a)
        {
            if (!b.TryGetValue(entry.Key, out string? value)
                || !string.Equals(entry.Value, value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    // Order-independent aggregate (Java Map.hashCode = sum of entry hashes, an entry hash
    // being key.hashCode() ^ value.hashCode()). Summation is commutative, so enumeration
    // order does not affect the result.
    private static int TagsHashCode(Dictionary<string, string> tags)
    {
        int hash = 0;
        foreach (KeyValuePair<string, string> tag in tags)
        {
            unchecked
            {
                hash += StringComparer.Ordinal.GetHashCode(tag.Key) ^ StringComparer.Ordinal.GetHashCode(tag.Value);
            }
        }

        return hash;
    }
}
