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
/// A filter selecting access control entries — Java's
/// <c>org.apache.kafka.common.acl.AccessControlEntryFilter</c>.
/// </summary>
/// <remarks>
/// <para>
/// A null <see cref="Principal()"/> or <see cref="Host()"/> matches any; <c>""</c> filters on the
/// literal empty value.
/// </para>
/// <para>
/// Java's matching members (<c>matches</c>, <c>matchesAtMostOne</c>,
/// <c>findIndefiniteField</c>) are not ported, a recorded deviation: ACL matching is Kafka
/// behaviour and lives in the core.
/// </para>
/// <para>
/// <see cref="Operation()"/> and <see cref="PermissionType()"/> only ever hold a defined member, as a
/// Java filter does.
/// </para>
/// </remarks>
public sealed class AccessControlEntryFilter
{
    private readonly string? _principal;
    private readonly string? _host;
    private readonly AclOperation _operation;
    private readonly AclPermissionType _permissionType;

    /// <summary>A filter matching every access control entry — Java's <c>ANY</c>.</summary>
    public static readonly AccessControlEntryFilter ANY =
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);

    /// <summary>
    /// Creates an access control entry filter — Java's constructor. Unlike
    /// <see cref="AccessControlEntry"/>, no combination is rejected.
    /// </summary>
    /// <param name="principal">
    /// The principal, or <c>null</c> to match any. <c>""</c> filters on the empty principal.
    /// </param>
    /// <param name="host">
    /// The host, or <c>null</c> to match any. <c>""</c> filters on the empty host.
    /// </param>
    /// <param name="operation">
    /// The operation, or <see cref="AclOperation.ANY"/>. A value that is not a defined member is
    /// stored as <see cref="AclOperation.UNKNOWN"/>, as Java's <c>fromCode</c> maps a code it has
    /// no member for.
    /// </param>
    /// <param name="permissionType">
    /// The permission type, or <see cref="AclPermissionType.ANY"/>. A value that is not a defined
    /// member is stored as <see cref="AclPermissionType.UNKNOWN"/>, as Java's <c>fromCode</c>
    /// maps it.
    /// </param>
    public AccessControlEntryFilter(
        string? principal,
        string? host,
        AclOperation operation,
        AclPermissionType permissionType)
    {
        _principal = principal;
        _host = host;

        _operation = AclEnumCodes.DefinedOrUnknown(operation);
        _permissionType = AclEnumCodes.DefinedOrUnknown(permissionType);
    }

    /// <summary>The principal, or <c>null</c> to match any — Java's <c>principal()</c>.</summary>
    /// <returns>The principal, or <c>null</c> to match any.</returns>
    public string? Principal() => _principal;

    /// <summary>The host, or <c>null</c> to match any — Java's <c>host()</c>.</summary>
    /// <returns>The host, or <c>null</c> to match any.</returns>
    public string? Host() => _host;

    /// <summary>The operation — Java's <c>operation()</c>.</summary>
    /// <returns>The operation.</returns>
    public AclOperation Operation() => _operation;

    /// <summary>The permission type — Java's <c>permissionType()</c>.</summary>
    /// <returns>The permission type.</returns>
    public AclPermissionType PermissionType() => _permissionType;

    /// <summary>
    /// Whether the operation or permission type is <c>UNKNOWN</c> — Java's <c>isUnknown()</c>.
    /// </summary>
    /// <returns>Whether the operation or permission type is <c>UNKNOWN</c>.</returns>
    public bool IsUnknown() =>
        Operation() == AclOperation.UNKNOWN || PermissionType() == AclPermissionType.UNKNOWN;

    /// <summary>Value equality over all four components — Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>
    /// Whether the two are the same filter. A <c>null</c> principal or host never equals <c>""</c>.
    /// </returns>
    public override bool Equals(object? obj) =>
        obj is AccessControlEntryFilter other
        && string.Equals(Principal(), other.Principal(), StringComparison.Ordinal)
        && string.Equals(Host(), other.Host(), StringComparison.Ordinal)
        && Operation() == other.Operation()
        && PermissionType() == other.PermissionType();

    /// <summary>The hash of all four components — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + (_principal is null ? 0 : StringComparer.Ordinal.GetHashCode(_principal));
            hash = (hash * 31) + (_host is null ? 0 : StringComparer.Ordinal.GetHashCode(_host));
            hash = (hash * 31) + ((int)_operation).GetHashCode();
            hash = (hash * 31) + ((int)_permissionType).GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering in the shape of Java's <c>toString()</c>, which renders a null
    /// component as <c>&lt;any&gt;</c>.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(principal={0}, host={1}, operation={2}, permissionType={3})",
            Principal() ?? "<any>",
            Host() ?? "<any>",
            Operation(),
            PermissionType());
}
