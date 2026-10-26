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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// A pattern naming the resource(s) an ACL applies to — Java's
/// <c>org.apache.kafka.common.resource.ResourcePattern</c>.
/// </summary>
/// <remarks>
/// A concrete pattern: <see cref="Name"/> is never null, and the filter-only values
/// (<see cref="ResourceType.Any"/>, <see cref="PatternType.Any"/>, <see cref="PatternType.Match"/>)
/// are rejected by the constructor, as in Java; use <see cref="ResourcePatternFilter"/> for them.
/// <see cref="ResourceType"/> and <see cref="PatternType"/> only ever hold a defined member, as a
/// Java pattern does.
/// </remarks>
public sealed class ResourcePattern
{
    /// <summary>
    /// The literal resource name meaning "all resources of this type" — Java's
    /// <c>WILDCARD_RESOURCE</c>.
    /// </summary>
    public const string WildcardResource = "*";

    /// <summary>Creates a resource pattern — Java's constructor.</summary>
    /// <param name="resourceType">
    /// The specific resource type; <see cref="ResourceType.Any"/> is rejected. A value that is not
    /// a defined <see cref="Confluent.Kafka.ResourceType"/> member is stored as
    /// <see cref="ResourceType.Unknown"/>, as Java's <c>ResourceType.fromCode</c> maps a code it
    /// has no member for, so a request and its answer key the same pattern.
    /// </param>
    /// <param name="name">The resource name, which may be <see cref="WildcardResource"/>.</param>
    /// <param name="patternType">
    /// The specific pattern type; <see cref="PatternType.Any"/> and <see cref="PatternType.Match"/>
    /// are rejected. A value that is not a defined <see cref="Confluent.Kafka.PatternType"/> member
    /// is stored as <see cref="PatternType.Unknown"/>, as Java's <c>PatternType.fromCode</c> maps
    /// it.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="resourceType"/> is <see cref="ResourceType.Any"/>, or
    /// <paramref name="patternType"/> is <see cref="PatternType.Any"/> or
    /// <see cref="PatternType.Match"/>.
    /// </exception>
    public ResourcePattern(ResourceType resourceType, string name, PatternType patternType)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));

        resourceType = AclEnumCodes.DefinedOrUnknown(resourceType);
        patternType = AclEnumCodes.DefinedOrUnknown(patternType);

        if (resourceType == ResourceType.Any)
        {
            // Java's message, with the C# member name.
            throw new ArgumentException("resourceType must not be Any", nameof(resourceType));
        }

        if (patternType == PatternType.Match || patternType == PatternType.Any)
        {
            throw new ArgumentException(
                string.Format(CultureInfo.InvariantCulture, "patternType must not be {0}", patternType),
                nameof(patternType));
        }

        ResourceType = resourceType;
        PatternType = patternType;
    }

    /// <summary>The resource type this pattern matches — Java's <c>resourceType()</c>.</summary>
    public ResourceType ResourceType { get; }

    /// <summary>The resource name — Java's <c>name()</c>.</summary>
    public string Name { get; }

    /// <summary>The pattern type — Java's <c>patternType()</c>.</summary>
    public PatternType PatternType { get; }

    /// <summary>Whether any component is <c>Unknown</c> — Java's <c>isUnknown()</c>.</summary>
    public bool IsUnknown => ResourceType == ResourceType.Unknown || PatternType == PatternType.Unknown;

    /// <summary>A filter matching only this pattern — Java's <c>toFilter()</c>.</summary>
    /// <returns>The filter.</returns>
    public ResourcePatternFilter ToFilter() => new ResourcePatternFilter(ResourceType, Name, PatternType);

    /// <summary>Value equality over all three components — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same pattern.</returns>
    public override bool Equals(object? obj) =>
        obj is ResourcePattern other
        && ResourceType == other.ResourceType
        && string.Equals(Name, other.Name, StringComparison.Ordinal)
        && PatternType == other.PatternType;

    /// <summary>The hash of all three components — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + ((int)ResourceType).GetHashCode();
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Name);
            hash = (hash * 31) + ((int)PatternType).GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering in the shape of Java's <c>toString()</c>; the types print with
    /// their .NET names (<c>Topic</c>, <c>Literal</c>), where Java prints its constants
    /// (<c>TOPIC</c>, <c>LITERAL</c>).
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ResourcePattern(resourceType={0}, name={1}, patternType={2})",
            ResourceType,
            Name,
            PatternType);
}
