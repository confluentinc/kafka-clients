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
/// A binding between a resource pattern and an access control entry — the .NET realization of
/// Java's <c>org.apache.kafka.common.acl.AclBinding</c> (<c>:37</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Nested, although the C ABI is flat.</b> The ABI exposes seven flat accessors and no
/// <c>ResourcePattern</c> / <c>AccessControlEntry</c> types at all; restoring Java's two-level
/// shape is the binding's job (PLAN D36, <c>bindings/CLAUDE.md §1.2</c>). Read
/// <c>binding.Pattern.Name</c>, never a flattened <c>binding.ResourceName</c>.
/// </para>
/// <para>
/// ⚠ <b>Value equality is load-bearing, not decorative.</b> This type is a dictionary
/// <b>key</b> on the ACL RPC results, so a wrong <see cref="Equals(object?)"/> /
/// <see cref="GetHashCode"/> makes every caller lookup miss <em>silently</em> (PLAN D39).
/// </para>
/// </remarks>
public sealed class AclBinding
{
    /// <summary>
    /// Creates a binding — Java's <c>AclBinding(ResourcePattern, AccessControlEntry)</c>
    /// (<c>:37</c>).
    /// </summary>
    /// <param name="pattern">The resource pattern.</param>
    /// <param name="entry">The access control entry.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="pattern"/> or <paramref name="entry"/> is null (Java's <c>:38-39</c>).
    /// </exception>
    public AclBinding(ResourcePattern pattern, AccessControlEntry entry)
    {
        Pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
        Entry = entry ?? throw new ArgumentNullException(nameof(entry));
    }

    /// <summary>The resource pattern — Java's <c>pattern()</c> (<c>:52</c>).</summary>
    public ResourcePattern Pattern { get; }

    /// <summary>The access control entry — Java's <c>entry()</c> (<c>:59</c>).</summary>
    public AccessControlEntry Entry { get; }

    /// <summary>
    /// Whether either half has an <c>Unknown</c> component — Java's <c>isUnknown()</c>
    /// (<c>:45</c>).
    /// </summary>
    public bool IsUnknown => Pattern.IsUnknown || Entry.IsUnknown;

    /// <summary>
    /// A filter matching only this binding — Java's <c>toFilter()</c> (<c>:66</c>).
    /// </summary>
    /// <returns>The filter.</returns>
    public AclBindingFilter ToFilter() => new AclBindingFilter(Pattern.ToFilter(), Entry.ToFilter());

    /// <summary>
    /// Value equality over both halves — Java's <c>equals</c> (<c>:76</c>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same binding.</returns>
    public override bool Equals(object? obj) =>
        obj is AclBinding other
        && Pattern.Equals(other.Pattern)
        && Entry.Equals(other.Entry);

    /// <summary>
    /// The hash of both halves — Java's <c>hashCode</c> (<c>:85</c>), same fields and same
    /// <c>31 *</c> fold as <c>Objects.hash</c>.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + Pattern.GetHashCode();
            hash = (hash * 31) + Entry.GetHashCode();
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:71</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "(pattern={0}, entry={1})", Pattern, Entry);
}
