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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Acl;

/// <summary>
/// Whether an ACL grants or denies — Java's <c>org.apache.kafka.common.acl.AclPermissionType</c>.
/// Each member's value is the Kafka wire code (Java's <c>code()</c>), not an ordinal.
/// </summary>
public enum AclPermissionType
{
    /// <summary>A permission type this client does not recognize — Java's <c>UNKNOWN</c> (code 0).</summary>
    UNKNOWN = 0,

    /// <summary>
    /// In a filter, matches any permission type — Java's <c>ANY</c> (code 1). Rejected by
    /// <see cref="AccessControlEntry"/>'s constructor; legal on <see cref="AccessControlEntryFilter"/>.
    /// </summary>
    ANY = 1,

    /// <summary>Disallows the operation — Java's <c>DENY</c> (code 2).</summary>
    DENY = 2,

    /// <summary>Allows the operation — Java's <c>ALLOW</c> (code 3).</summary>
    ALLOW = 3,
}

/// <summary>
/// The members Java declares on the <see cref="AclPermissionType"/> enum itself, which a C# enum
/// cannot carry. From C# 14 the static ones are called on the enum
/// (<c>AclPermissionType.FromString(…)</c>); older C# calls them on this class.
/// </summary>
public static class AclPermissionTypeMethods
{
    extension(AclPermissionType)
    {
        /// <summary>
        /// Parse the given string as an ACL permission — Java's <c>fromString(String)</c>: the string upper-cased
        /// as Java's <c>toUpperCase(Locale.ROOT)</c> does, matched exactly against the constant names.
        /// </summary>
        /// <param name="str">The string to parse.</param>
        /// <returns>
        /// The <see cref="AclPermissionType"/>, or <see cref="AclPermissionType.UNKNOWN"/> if the string could not be
        /// matched.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="str"/> is <see langword="null"/>.</exception>
        public static AclPermissionType FromString(string str) =>
            JavaText.ToUpperRoot(str ?? throw new ArgumentNullException(nameof(str))) switch
            {
                nameof(AclPermissionType.UNKNOWN) => AclPermissionType.UNKNOWN,
                nameof(AclPermissionType.ANY) => AclPermissionType.ANY,
                nameof(AclPermissionType.DENY) => AclPermissionType.DENY,
                nameof(AclPermissionType.ALLOW) => AclPermissionType.ALLOW,
                _ => AclPermissionType.UNKNOWN,
            };

        /// <summary>
        /// Return the <see cref="AclPermissionType"/> with the provided code or
        /// <see cref="AclPermissionType.UNKNOWN"/> if one cannot be found — Java's <c>fromCode(byte)</c>.
        /// </summary>
        /// <param name="code">The code.</param>
        /// <returns>The permission type; never throws.</returns>
        public static AclPermissionType FromCode(byte code) =>
            code <= (byte)AclPermissionType.ALLOW ? (AclPermissionType)code : AclPermissionType.UNKNOWN;
    }

    extension(AclPermissionType permissionType)
    {
        /// <summary>Return the code of this permission type — Java's <c>code()</c>.</summary>
        /// <returns>
        /// The code, which is the member's value. Of a value no member has, only the low byte is kept:
        /// <c>((AclPermissionType)259).Code()</c> is 3.
        /// </returns>
        public byte Code() => unchecked((byte)permissionType);

        /// <summary>Return true if this permission type is UNKNOWN — Java's <c>isUnknown()</c>.</summary>
        /// <returns>Whether this permission type is <see cref="AclPermissionType.UNKNOWN"/>.</returns>
        public bool IsUnknown() => permissionType == AclPermissionType.UNKNOWN;
    }
}
