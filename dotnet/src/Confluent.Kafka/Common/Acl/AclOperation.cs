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
/// An operation that can be authorized on a Kafka resource — Java's
/// <c>org.apache.kafka.common.acl.AclOperation</c>.
/// </summary>
/// <remarks>
/// Each member's value is the Kafka wire code (Java's <c>code()</c>), not an ordinal.
/// </remarks>
public enum AclOperation
{
    /// <summary>
    /// Represents any operation the client does not recognize — Java's <c>UNKNOWN</c>
    /// (code 0). A code the broker sends that this client has no member for is mapped
    /// here, mirroring Java's <c>fromCode</c>.
    /// </summary>
    UNKNOWN = 0,

    /// <summary>In a filter, matches any operation — Java's <c>ANY</c> (code 1).</summary>
    ANY = 1,

    /// <summary>All operations — Java's <c>ALL</c> (code 2).</summary>
    ALL = 2,

    /// <summary>READ — Java's <c>READ</c> (code 3).</summary>
    READ = 3,

    /// <summary>WRITE — Java's <c>WRITE</c> (code 4).</summary>
    WRITE = 4,

    /// <summary>CREATE — Java's <c>CREATE</c> (code 5).</summary>
    CREATE = 5,

    /// <summary>DELETE — Java's <c>DELETE</c> (code 6).</summary>
    DELETE = 6,

    /// <summary>ALTER — Java's <c>ALTER</c> (code 7).</summary>
    ALTER = 7,

    /// <summary>DESCRIBE — Java's <c>DESCRIBE</c> (code 8).</summary>
    DESCRIBE = 8,

    /// <summary>CLUSTER_ACTION — Java's <c>CLUSTER_ACTION</c> (code 9).</summary>
    CLUSTER_ACTION = 9,

    /// <summary>DESCRIBE_CONFIGS — Java's <c>DESCRIBE_CONFIGS</c> (code 10).</summary>
    DESCRIBE_CONFIGS = 10,

    /// <summary>ALTER_CONFIGS — Java's <c>ALTER_CONFIGS</c> (code 11).</summary>
    ALTER_CONFIGS = 11,

    /// <summary>IDEMPOTENT_WRITE — Java's <c>IDEMPOTENT_WRITE</c> (code 12).</summary>
    IDEMPOTENT_WRITE = 12,

    /// <summary>CREATE_TOKENS — Java's <c>CREATE_TOKENS</c> (code 13).</summary>
    CREATE_TOKENS = 13,

    /// <summary>DESCRIBE_TOKENS — Java's <c>DESCRIBE_TOKENS</c> (code 14).</summary>
    DESCRIBE_TOKENS = 14,

    /// <summary>TWO_PHASE_COMMIT — Java's <c>TWO_PHASE_COMMIT</c> (code 15).</summary>
    TWO_PHASE_COMMIT = 15,
}

/// <summary>
/// The members Java declares on the <see cref="AclOperation"/> enum itself, which a C# enum
/// cannot carry. From C# 14 the static ones are called on the enum
/// (<c>AclOperation.FromString(…)</c>); older C# calls them on this class.
/// </summary>
public static class AclOperationMethods
{
    extension(AclOperation)
    {
        /// <summary>
        /// Parse the given string as an ACL operation — Java's <c>fromString(String)</c>: the string upper-cased
        /// as Java's <c>toUpperCase(Locale.ROOT)</c> does, matched exactly against the constant names.
        /// </summary>
        /// <param name="str">The string to parse.</param>
        /// <returns>
        /// The <see cref="AclOperation"/>, or <see cref="AclOperation.UNKNOWN"/> if the string could not be matched.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="str"/> is <see langword="null"/>.</exception>
        public static AclOperation FromString(string str) =>
            JavaText.ToUpperRoot(str ?? throw new ArgumentNullException(nameof(str))) switch
            {
                nameof(AclOperation.UNKNOWN) => AclOperation.UNKNOWN,
                nameof(AclOperation.ANY) => AclOperation.ANY,
                nameof(AclOperation.ALL) => AclOperation.ALL,
                nameof(AclOperation.READ) => AclOperation.READ,
                nameof(AclOperation.WRITE) => AclOperation.WRITE,
                nameof(AclOperation.CREATE) => AclOperation.CREATE,
                nameof(AclOperation.DELETE) => AclOperation.DELETE,
                nameof(AclOperation.ALTER) => AclOperation.ALTER,
                nameof(AclOperation.DESCRIBE) => AclOperation.DESCRIBE,
                nameof(AclOperation.CLUSTER_ACTION) => AclOperation.CLUSTER_ACTION,
                nameof(AclOperation.DESCRIBE_CONFIGS) => AclOperation.DESCRIBE_CONFIGS,
                nameof(AclOperation.ALTER_CONFIGS) => AclOperation.ALTER_CONFIGS,
                nameof(AclOperation.IDEMPOTENT_WRITE) => AclOperation.IDEMPOTENT_WRITE,
                nameof(AclOperation.CREATE_TOKENS) => AclOperation.CREATE_TOKENS,
                nameof(AclOperation.DESCRIBE_TOKENS) => AclOperation.DESCRIBE_TOKENS,
                nameof(AclOperation.TWO_PHASE_COMMIT) => AclOperation.TWO_PHASE_COMMIT,
                _ => AclOperation.UNKNOWN,
            };

        /// <summary>
        /// Return the <see cref="AclOperation"/> with the provided code or <see cref="AclOperation.UNKNOWN"/> if
        /// one cannot be found — Java's <c>fromCode(byte)</c>.
        /// </summary>
        /// <param name="code">The code.</param>
        /// <returns>The operation; never throws.</returns>
        public static AclOperation FromCode(byte code) =>
            code <= (byte)AclOperation.TWO_PHASE_COMMIT ? (AclOperation)code : AclOperation.UNKNOWN;
    }

    extension(AclOperation operation)
    {
        /// <summary>Return the code of this operation — Java's <c>code()</c>.</summary>
        /// <returns>
        /// The code, which is the member's value. Of a value no member has, only the low byte is kept:
        /// <c>((AclOperation)259).Code()</c> is 3.
        /// </returns>
        public byte Code() => unchecked((byte)operation);

        /// <summary>Return true if this operation is UNKNOWN — Java's <c>isUnknown()</c>.</summary>
        /// <returns>Whether this operation is <see cref="AclOperation.UNKNOWN"/>.</returns>
        public bool IsUnknown() => operation == AclOperation.UNKNOWN;
    }
}
