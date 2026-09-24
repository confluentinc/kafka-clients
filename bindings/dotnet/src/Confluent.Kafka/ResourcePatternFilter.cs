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
using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// A filter selecting resource patterns — the .NET realization of Java's
/// <c>org.apache.kafka.common.resource.ResourcePatternFilter</c> (<c>:52</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="Name"/> is nullable and <c>null</c> is NOT the same as <c>""</c>.</b>
/// <c>null</c> means "match any resource name"; <c>""</c> filters on the literal empty name.
/// Nothing may normalize one into the other — see PLAN §2.1.
/// </para>
/// <para>
/// Java's matching family (<c>matches</c>, <c>matchesAtMostOne</c>, <c>findIndefiniteField</c>)
/// is deliberately <b>not</b> ported: ACL pattern-matching semantics are Kafka behavior and
/// live once, in the Rust core (<c>bindings/CLAUDE.md §2.6</c>). Recorded deviation,
/// <c>definition-of-done.md</c> §7.
/// </para>
/// </remarks>
public sealed class ResourcePatternFilter
{
    /// <summary>
    /// A filter matching every resource pattern — Java's <c>ANY</c> (<c>:31</c>).
    /// </summary>
    public static ResourcePatternFilter Any { get; } =
        new ResourcePatternFilter(ResourceType.Any, null, PatternType.Any);

    /// <summary>
    /// Creates a resource pattern filter — Java's
    /// <c>ResourcePatternFilter(ResourceType, String, PatternType)</c> (<c>:52</c>). Unlike
    /// <see cref="ResourcePattern"/>, no combination is rejected.
    /// </summary>
    /// <param name="resourceType">
    /// The resource type, or <see cref="ResourceType.Any"/> to ignore the pattern's type.
    /// </param>
    /// <param name="name">
    /// The resource name, or <c>null</c> to match any name. <c>""</c> is a filter on the
    /// literal empty name, not a wildcard.
    /// </param>
    /// <param name="patternType">
    /// The pattern type, or <see cref="PatternType.Any"/> / <see cref="PatternType.Match"/>.
    /// </param>
    public ResourcePatternFilter(ResourceType resourceType, string? name, PatternType patternType)
    {
        ResourceType = resourceType;
        Name = name;
        PatternType = patternType;
    }

    /// <summary>The resource type this filter matches — Java's <c>resourceType()</c> (<c>:68</c>).</summary>
    public ResourceType ResourceType { get; }

    /// <summary>
    /// The resource name, or <c>null</c> to match any name — Java's <c>name()</c> (<c>:75</c>).
    /// </summary>
    public string? Name { get; }

    /// <summary>The pattern type this filter matches — Java's <c>patternType()</c> (<c>:82</c>).</summary>
    public PatternType PatternType { get; }

    /// <summary>
    /// Whether any component is <c>Unknown</c> — Java's <c>isUnknown()</c> (<c>:61</c>).
    /// </summary>
    public bool IsUnknown => ResourceType == ResourceType.Unknown || PatternType == PatternType.Unknown;

    /// <summary>
    /// Value equality over all three components — Java's <c>equals</c> (<c>:149</c>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two are the same filter. A <c>null</c> name never equals <c>""</c>.</returns>
    public override bool Equals(object? obj) =>
        obj is ResourcePatternFilter other
        && ResourceType == other.ResourceType
        && string.Equals(Name, other.Name, StringComparison.Ordinal)
        && PatternType == other.PatternType;

    /// <summary>
    /// The hash of all three components — Java's <c>hashCode</c> (<c>:162</c>), same fields and
    /// same <c>31 *</c> fold as <c>Objects.hash</c> (a null name hashes to 0, as in Java).
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + ((int)ResourceType).GetHashCode();
            hash = (hash * 31) + (Name is null ? 0 : StringComparer.Ordinal.GetHashCode(Name));
            hash = (hash * 31) + ((int)PatternType).GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c> (<c>:144</c>) — which labels
    /// itself <c>ResourcePattern(…)</c>, a Java quirk kept for parity.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ResourcePattern(resourceType={0}, name={1}, patternType={2})",
            ResourceType,
            Name ?? "<any>",
            PatternType);
}
