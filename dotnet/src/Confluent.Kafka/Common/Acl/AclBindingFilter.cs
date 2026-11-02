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
using Confluent.Kafka.Common.Resource;

namespace Confluent.Kafka.Common.Acl;

/// <summary>
/// A filter selecting ACL bindings — Java's <c>org.apache.kafka.common.acl.AclBindingFilter</c>.
/// </summary>
/// <remarks>
/// <para>
/// Filters are dictionary keys on the ACL RPC results, so their value equality decides every
/// lookup.
/// </para>
/// <para>
/// Java's matching members (<c>matches</c>, <c>matchesAtMostOne</c>,
/// <c>findIndefiniteField</c>) are not ported, a recorded deviation: ACL matching is Kafka
/// behaviour and lives in the core.
/// </para>
/// </remarks>
public sealed class AclBindingFilter
{
    /// <summary>A filter matching every ACL binding — Java's <c>ANY</c>.</summary>
    public static AclBindingFilter Any { get; } =
        new AclBindingFilter(ResourcePatternFilter.Any, AccessControlEntryFilter.Any);

    /// <summary>Creates a binding filter — Java's constructor.</summary>
    /// <param name="patternFilter">The resource pattern filter.</param>
    /// <param name="entryFilter">The access control entry filter.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="patternFilter"/> or <paramref name="entryFilter"/> is null.
    /// </exception>
    public AclBindingFilter(ResourcePatternFilter patternFilter, AccessControlEntryFilter entryFilter)
    {
        PatternFilter = patternFilter ?? throw new ArgumentNullException(nameof(patternFilter));
        EntryFilter = entryFilter ?? throw new ArgumentNullException(nameof(entryFilter));
    }

    /// <summary>The resource pattern filter — Java's <c>patternFilter()</c>.</summary>
    public ResourcePatternFilter PatternFilter { get; }

    /// <summary>The access control entry filter — Java's <c>entryFilter()</c>.</summary>
    public AccessControlEntryFilter EntryFilter { get; }

    /// <summary>
    /// Whether either half has an <c>Unknown</c> component — Java's <c>isUnknown()</c>.
    /// </summary>
    public bool IsUnknown => PatternFilter.IsUnknown || EntryFilter.IsUnknown;

    /// <summary>Value equality over both halves — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two are the same filter.</returns>
    public override bool Equals(object? obj) =>
        obj is AclBindingFilter other
        && PatternFilter.Equals(other.PatternFilter)
        && EntryFilter.Equals(other.EntryFilter);

    /// <summary>The hash of both halves — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + PatternFilter.GetHashCode();
            hash = (hash * 31) + EntryFilter.GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering in the shape of Java's <c>toString()</c>, with the enums' .NET names,
    /// as in <see cref="ResourcePatternFilter.ToString"/> and
    /// <see cref="AccessControlEntryFilter.ToString"/>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(patternFilter={0}, entryFilter={1})",
            PatternFilter,
            EntryFilter);
}
