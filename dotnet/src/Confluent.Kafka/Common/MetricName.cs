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

namespace Confluent.Kafka.Common;

/// <summary>
/// The name of a metric, Java's <c>MetricName</c>, and the key type of the
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> a client's <c>Metrics()</c> returns: two are equal when their
/// <see cref="Name"/>, <see cref="Group"/> and <see cref="Tags"/> match, as in Java, which ignores
/// <see cref="Description"/>.
/// </summary>
/// <remarks>The constructor copies <c>tags</c> into an ordinal-keyed dictionary, where Java keeps the caller's map.</remarks>
public sealed class MetricName : IEquatable<MetricName>
{
    private readonly Dictionary<string, string> _tags;

    private int _hash;

    /// <summary>
    /// Initializes a metric name; throws <see cref="ArgumentNullException"/> if any argument is
    /// <see langword="null"/>.
    /// </summary>
    public MetricName(string name, string group, string description, IReadOnlyDictionary<string, string> tags)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Group = group ?? throw new ArgumentNullException(nameof(group));
        Description = description ?? throw new ArgumentNullException(nameof(description));
        if (tags is null)
        {
            throw new ArgumentNullException(nameof(tags));
        }

        _tags = new Dictionary<string, string>(tags.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> tag in tags)
        {
            _tags[tag.Key] = tag.Value;
        }
    }

    /// <summary>The metric's name.</summary>
    public string Name { get; }

    /// <summary>The group the metric belongs to.</summary>
    public string Group { get; }

    /// <summary>A description of the metric, which equality ignores.</summary>
    public string Description { get; }

    /// <summary>The metric's tags.</summary>
    public IReadOnlyDictionary<string, string> Tags => _tags;

    /// <summary>Determines whether two <see cref="MetricName"/> values are equal.</summary>
    public static bool operator ==(MetricName? left, MetricName? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>Determines whether two <see cref="MetricName"/> values differ.</summary>
    public static bool operator !=(MetricName? left, MetricName? right) => !(left == right);

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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
