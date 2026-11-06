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
/// A binding between a resource pattern and an access control entry — Java's
/// <c>org.apache.kafka.common.acl.AclBinding</c>.
/// </summary>
/// <remarks>
/// Bindings are dictionary keys on the ACL RPC results, so their value equality decides every
/// lookup.
/// </remarks>
public sealed class AclBinding
{
    private readonly ResourcePattern _pattern;
    private readonly AccessControlEntry _entry;

    /// <summary>Creates a binding — Java's constructor.</summary>
    /// <param name="pattern">The resource pattern.</param>
    /// <param name="entry">The access control entry.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="pattern"/> or <paramref name="entry"/> is null.
    /// </exception>
    public AclBinding(ResourcePattern pattern, AccessControlEntry entry)
    {
        _pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
        _entry = entry ?? throw new ArgumentNullException(nameof(entry));
    }

    /// <summary>The resource pattern — Java's <c>pattern()</c>.</summary>
    /// <returns>The resource pattern.</returns>
    public ResourcePattern Pattern() => _pattern;

    /// <summary>The access control entry — Java's <c>entry()</c>.</summary>
    /// <returns>The access control entry.</returns>
    public AccessControlEntry Entry() => _entry;

    /// <summary>
    /// Whether either half has an <c>UNKNOWN</c> component — Java's <c>isUnknown()</c>.
    /// </summary>
    /// <returns>Whether either half has an <c>UNKNOWN</c> component.</returns>
    public bool IsUnknown() => Pattern().IsUnknown() || Entry().IsUnknown();

    /// <summary>A filter matching only this binding — Java's <c>toFilter()</c>.</summary>
    /// <returns>The filter.</returns>
    public AclBindingFilter ToFilter() => new AclBindingFilter(Pattern().ToFilter(), Entry().ToFilter());

    /// <summary>Value equality over both halves — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same binding.</returns>
    public override bool Equals(object? obj) =>
        obj is AclBinding other
        && Pattern().Equals(other.Pattern())
        && Entry().Equals(other.Entry());

    /// <summary>The hash of both halves — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + Pattern().GetHashCode();
            hash = (hash * 31) + Entry().GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering in the shape of Java's <c>toString()</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "(pattern={0}, entry={1})", Pattern(), Entry());
}
