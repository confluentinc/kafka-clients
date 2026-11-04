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

namespace Confluent.Kafka.Common.Acl;

/// <summary>
/// The "who may do what, from where" half of an ACL — Java's
/// <c>org.apache.kafka.common.acl.AccessControlEntry</c>.
/// </summary>
/// <remarks>
/// A concrete entry: <see cref="Principal"/> and <see cref="Host"/> are never null, and
/// <see cref="AclOperation.ANY"/> and <see cref="AclPermissionType.ANY"/> are rejected, as in
/// Java; <see cref="AccessControlEntryFilter"/> takes them. <see cref="Operation"/> and
/// <see cref="PermissionType"/> only ever hold a defined member, as a Java entry does.
/// </remarks>
public sealed class AccessControlEntry
{
    /// <summary>Creates an access control entry — Java's constructor.</summary>
    /// <param name="principal">The principal, e.g. <c>User:alice</c>.</param>
    /// <param name="host">The host, or <c>*</c> for all hosts.</param>
    /// <param name="operation">
    /// The operation; <see cref="AclOperation.ANY"/> is rejected. A value that is not a defined
    /// member is stored as <see cref="AclOperation.UNKNOWN"/>, as Java's <c>fromCode</c> maps a code
    /// it has no member for.
    /// </param>
    /// <param name="permissionType">
    /// The permission type; <see cref="AclPermissionType.ANY"/> is rejected. A value that is not a
    /// defined member is stored as <see cref="AclPermissionType.UNKNOWN"/>, as Java's
    /// <c>fromCode</c> maps it.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="principal"/> or <paramref name="host"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="operation"/> is <see cref="AclOperation.ANY"/>, or
    /// <paramref name="permissionType"/> is <see cref="AclPermissionType.ANY"/>.
    /// </exception>
    public AccessControlEntry(string principal, string host, AclOperation operation, AclPermissionType permissionType)
    {
        Principal = principal ?? throw new ArgumentNullException(nameof(principal));
        Host = host ?? throw new ArgumentNullException(nameof(host));

        operation = AclEnumCodes.DefinedOrUnknown(operation);
        permissionType = AclEnumCodes.DefinedOrUnknown(permissionType);

        if (operation == AclOperation.ANY)
        {
            throw new ArgumentException("operation must not be ANY", nameof(operation));
        }

        if (permissionType == AclPermissionType.ANY)
        {
            throw new ArgumentException("permissionType must not be ANY", nameof(permissionType));
        }

        Operation = operation;
        PermissionType = permissionType;
    }

    /// <summary>The principal — Java's <c>principal()</c>.</summary>
    public string Principal { get; }

    /// <summary>The host, or <c>*</c> for all hosts — Java's <c>host()</c>.</summary>
    public string Host { get; }

    /// <summary>
    /// The operation; never <see cref="AclOperation.ANY"/> — Java's <c>operation()</c>.
    /// </summary>
    public AclOperation Operation { get; }

    /// <summary>
    /// The permission type; never <see cref="AclPermissionType.ANY"/> — Java's
    /// <c>permissionType()</c>.
    /// </summary>
    public AclPermissionType PermissionType { get; }

    /// <summary>
    /// Whether the operation or permission type is <c>UNKNOWN</c> — Java's <c>isUnknown()</c>.
    /// </summary>
    public bool IsUnknown =>
        Operation == AclOperation.UNKNOWN || PermissionType == AclPermissionType.UNKNOWN;

    /// <summary>A filter matching only this entry — Java's <c>toFilter()</c>.</summary>
    /// <returns>The filter.</returns>
    public AccessControlEntryFilter ToFilter() =>
        new AccessControlEntryFilter(Principal, Host, Operation, PermissionType);

    /// <summary>Value equality over all four components — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same entry.</returns>
    public override bool Equals(object? obj) =>
        obj is AccessControlEntry other
        && string.Equals(Principal, other.Principal, StringComparison.Ordinal)
        && string.Equals(Host, other.Host, StringComparison.Ordinal)
        && Operation == other.Operation
        && PermissionType == other.PermissionType;

    /// <summary>The hash of all four components — Java's <c>hashCode</c>.</summary>
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
    /// A diagnostic rendering in the shape of Java's <c>toString()</c>.
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
