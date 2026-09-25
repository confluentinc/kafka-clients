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
/// A filter selecting access control entries — the .NET realization of Java's
/// <c>org.apache.kafka.common.acl.AccessControlEntryFilter</c> (<c>:42</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see cref="Principal"/> and <see cref="Host"/> are nullable and <c>null</c> is NOT the
/// same as <c>""</c>.</b> <c>null</c> means "match any"; <c>""</c> filters on the literal empty
/// value. Nothing may normalize one into the other — see PLAN §2.1.
/// </para>
/// <para>
/// Java's matching family (<c>matches</c>, <c>matchesAtMostOne</c>, <c>findIndefiniteField</c>)
/// is deliberately <b>not</b> ported: ACL matching semantics are Kafka behavior and live once,
/// in the Rust core (<c>bindings/CLAUDE.md §2.6</c>). Recorded deviation,
/// <c>definition-of-done.md</c> §7.
/// </para>
/// </remarks>
public sealed class AccessControlEntryFilter
{
    /// <summary>
    /// A filter matching every access control entry — Java's <c>ANY</c> (<c>:31</c>).
    /// </summary>
    public static AccessControlEntryFilter Any { get; } =
        new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any);

    /// <summary>
    /// Creates an access control entry filter — Java's
    /// <c>AccessControlEntryFilter(String, String, AclOperation, AclPermissionType)</c>
    /// (<c>:42</c>). Unlike <see cref="AccessControlEntry"/>, no combination is rejected.
    /// </summary>
    /// <param name="principal">
    /// The principal, or <c>null</c> to match any. <c>""</c> filters on the empty principal.
    /// </param>
    /// <param name="host">
    /// The host, or <c>null</c> to match any. <c>""</c> filters on the empty host.
    /// </param>
    /// <param name="operation">The operation, or <see cref="AclOperation.Any"/>.</param>
    /// <param name="permissionType">
    /// The permission type, or <see cref="AclPermissionType.Any"/>.
    /// </param>
    public AccessControlEntryFilter(
        string? principal,
        string? host,
        AclOperation operation,
        AclPermissionType permissionType)
    {
        Principal = principal;
        Host = host;
        Operation = operation;
        PermissionType = permissionType;
    }

    /// <summary>
    /// The principal, or <c>null</c> to match any — Java's <c>principal()</c> (<c>:60</c>).
    /// </summary>
    public string? Principal { get; }

    /// <summary>The host, or <c>null</c> to match any — Java's <c>host()</c> (<c>:67</c>).</summary>
    public string? Host { get; }

    /// <summary>The operation — Java's <c>operation()</c> (<c>:74</c>).</summary>
    public AclOperation Operation { get; }

    /// <summary>The permission type — Java's <c>permissionType()</c> (<c>:81</c>).</summary>
    public AclPermissionType PermissionType { get; }

    /// <summary>
    /// Whether the operation or permission type is <c>Unknown</c> — Java's <c>isUnknown()</c>
    /// (<c>:93</c>).
    /// </summary>
    public bool IsUnknown =>
        Operation == AclOperation.Unknown || PermissionType == AclPermissionType.Unknown;

    /// <summary>
    /// Value equality over all four components — Java's <c>equals</c> (<c>:127</c>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>
    /// Whether the two are the same filter. A <c>null</c> principal or host never equals <c>""</c>.
    /// </returns>
    public override bool Equals(object? obj) =>
        obj is AccessControlEntryFilter other
        && string.Equals(Principal, other.Principal, StringComparison.Ordinal)
        && string.Equals(Host, other.Host, StringComparison.Ordinal)
        && Operation == other.Operation
        && PermissionType == other.PermissionType;

    /// <summary>
    /// The hash of all four components — Java's <c>hashCode</c> (<c>:135</c>), same fields and
    /// same <c>31 *</c> fold as <c>Objects.hash</c> (a null component hashes to 0, as in Java).
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + (Principal is null ? 0 : StringComparer.Ordinal.GetHashCode(Principal));
            hash = (hash * 31) + (Host is null ? 0 : StringComparer.Ordinal.GetHashCode(Host));
            hash = (hash * 31) + ((int)Operation).GetHashCode();
            hash = (hash * 31) + ((int)PermissionType).GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c> (<c>:86</c> →
    /// <c>AccessControlEntryData:76</c>), which renders a null component as <c>&lt;any&gt;</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(principal={0}, host={1}, operation={2}, permissionType={3})",
            Principal ?? "<any>",
            Host ?? "<any>",
            Operation,
            PermissionType);
}
