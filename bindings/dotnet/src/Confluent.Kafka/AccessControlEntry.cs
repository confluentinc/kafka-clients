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
/// The "who may do what, from where" half of an ACL — the .NET realization of Java's
/// <c>org.apache.kafka.common.acl.AccessControlEntry</c> (<c>:36</c>).
/// </summary>
/// <remarks>
/// A <b>concrete</b> entry: <see cref="Principal"/> and <see cref="Host"/> are never null, and
/// <see cref="AclOperation.Any"/> / <see cref="AclPermissionType.Any"/> are rejected by the
/// constructor, as in Java. Use <see cref="AccessControlEntryFilter"/> where those are wanted.
/// </remarks>
public sealed class AccessControlEntry
{
    /// <summary>
    /// Creates an access control entry — Java's
    /// <c>AccessControlEntry(String, String, AclOperation, AclPermissionType)</c> (<c>:36</c>).
    /// </summary>
    /// <param name="principal">The principal, e.g. <c>User:alice</c>.</param>
    /// <param name="host">The host, or <c>*</c> for all hosts.</param>
    /// <param name="operation">The operation; <see cref="AclOperation.Any"/> is rejected.</param>
    /// <param name="permissionType">
    /// The permission type; <see cref="AclPermissionType.Any"/> is rejected.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="principal"/> or <paramref name="host"/> is null (Java's <c>:37-38</c>).
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="operation"/> is <see cref="AclOperation.Any"/> (Java's <c>:40-41</c>), or
    /// <paramref name="permissionType"/> is <see cref="AclPermissionType.Any"/> (Java's <c>:43-44</c>).
    /// </exception>
    public AccessControlEntry(string principal, string host, AclOperation operation, AclPermissionType permissionType)
    {
        // Java's null checks come first, then the two ANY rejections, in this order.
        Principal = principal ?? throw new ArgumentNullException(nameof(principal));
        Host = host ?? throw new ArgumentNullException(nameof(host));

        if (operation == AclOperation.Any)
        {
            // Java's text with the C# member spelling (bindings/CLAUDE.md §2.2 casing rule).
            throw new ArgumentException("operation must not be Any", nameof(operation));
        }

        if (permissionType == AclPermissionType.Any)
        {
            throw new ArgumentException("permissionType must not be Any", nameof(permissionType));
        }

        Operation = operation;
        PermissionType = permissionType;
    }

    /// <summary>The principal — Java's <c>principal()</c> (<c>:51</c>).</summary>
    public string Principal { get; }

    /// <summary>The host, or <c>*</c> for all hosts — Java's <c>host()</c> (<c>:58</c>).</summary>
    public string Host { get; }

    /// <summary>
    /// The operation; never <see cref="AclOperation.Any"/> — Java's <c>operation()</c> (<c>:65</c>).
    /// </summary>
    public AclOperation Operation { get; }

    /// <summary>
    /// The permission type; never <see cref="AclPermissionType.Any"/> — Java's
    /// <c>permissionType()</c> (<c>:72</c>).
    /// </summary>
    public AclPermissionType PermissionType { get; }

    /// <summary>
    /// Whether the operation or permission type is <c>Unknown</c> — Java's <c>isUnknown()</c>
    /// (<c>:91</c>).
    /// </summary>
    public bool IsUnknown =>
        Operation == AclOperation.Unknown || PermissionType == AclPermissionType.Unknown;

    /// <summary>
    /// A filter matching only this entry — Java's <c>toFilter()</c> (<c>:79</c>).
    /// </summary>
    /// <returns>The filter.</returns>
    public AccessControlEntryFilter ToFilter() =>
        new AccessControlEntryFilter(Principal, Host, Operation, PermissionType);

    /// <summary>
    /// Value equality over all four components — Java's <c>equals</c> (<c>:96</c>, delegating to
    /// <c>AccessControlEntryData.equals</c> <c>:91</c>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same entry.</returns>
    public override bool Equals(object? obj) =>
        obj is AccessControlEntry other
        && string.Equals(Principal, other.Principal, StringComparison.Ordinal)
        && string.Equals(Host, other.Host, StringComparison.Ordinal)
        && Operation == other.Operation
        && PermissionType == other.PermissionType;

    /// <summary>
    /// The hash of all four components — Java's <c>hashCode</c> (<c>:104</c> →
    /// <c>AccessControlEntryData:102</c>), same fields and same <c>31 *</c> fold as
    /// <c>Objects.hash</c>.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Principal);
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Host);
            hash = (hash * 31) + ((int)Operation).GetHashCode();
            hash = (hash * 31) + ((int)PermissionType).GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering matching Java's <c>toString()</c> (<c>:84</c> →
    /// <c>AccessControlEntryData:76</c>).
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(principal={0}, host={1}, operation={2}, permissionType={3})",
            Principal,
            Host,
            Operation,
            PermissionType);
}
