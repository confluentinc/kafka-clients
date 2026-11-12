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

namespace Confluent.Kafka.Common.Resource;

/// <summary>
/// The type of resource an ACL applies to — Java's
/// <c>org.apache.kafka.common.resource.ResourceType</c>. Each member's value is the Kafka wire
/// code (Java's <c>code()</c>), which crosses the C ABI as an <c>int32_t</c>.
/// </summary>
public enum ResourceType
{
    /// <summary>A resource type this client does not recognize — Java's <c>UNKNOWN</c> (code 0).</summary>
    UNKNOWN = 0,

    /// <summary>
    /// In a filter, matches any resource type — Java's <c>ANY</c> (code 1). Rejected by
    /// <see cref="ResourcePattern"/>'s constructor; legal on <see cref="ResourcePatternFilter"/>.
    /// </summary>
    ANY = 1,

    /// <summary>A topic — Java's <c>TOPIC</c> (code 2).</summary>
    TOPIC = 2,

    /// <summary>A consumer group — Java's <c>GROUP</c> (code 3).</summary>
    GROUP = 3,

    /// <summary>The cluster — Java's <c>CLUSTER</c> (code 4).</summary>
    CLUSTER = 4,

    /// <summary>A transactional id — Java's <c>TRANSACTIONAL_ID</c> (code 5).</summary>
    TRANSACTIONAL_ID = 5,

    /// <summary>A delegation token — Java's <c>DELEGATION_TOKEN</c> (code 6).</summary>
    DELEGATION_TOKEN = 6,

    /// <summary>A user — Java's <c>USER</c> (code 7).</summary>
    USER = 7,
}

/// <summary>
/// The members Java declares on the <see cref="ResourceType"/> enum itself, which a C# enum
/// cannot carry. From C# 14 the static ones are called on the enum
/// (<c>ResourceType.FromString(…)</c>); older C# calls them on this class.
/// </summary>
public static class ResourceTypeMethods
{
    extension(ResourceType)
    {
        /// <summary>
        /// Parse the given string as an ACL resource type — Java's <c>fromString(String)</c>: the string upper-cased
        /// as Java's <c>toUpperCase(Locale.ROOT)</c> does, matched exactly against the constant names.
        /// </summary>
        /// <param name="str">The string to parse.</param>
        /// <returns>
        /// The <see cref="ResourceType"/>, or <see cref="ResourceType.UNKNOWN"/> if the string could not be matched.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="str"/> is <see langword="null"/>.</exception>
        public static ResourceType FromString(string str) =>
            JavaText.ToUpperRoot(str ?? throw new ArgumentNullException(nameof(str))) switch
            {
                nameof(ResourceType.UNKNOWN) => ResourceType.UNKNOWN,
                nameof(ResourceType.ANY) => ResourceType.ANY,
                nameof(ResourceType.TOPIC) => ResourceType.TOPIC,
                nameof(ResourceType.GROUP) => ResourceType.GROUP,
                nameof(ResourceType.CLUSTER) => ResourceType.CLUSTER,
                nameof(ResourceType.TRANSACTIONAL_ID) => ResourceType.TRANSACTIONAL_ID,
                nameof(ResourceType.DELEGATION_TOKEN) => ResourceType.DELEGATION_TOKEN,
                nameof(ResourceType.USER) => ResourceType.USER,
                _ => ResourceType.UNKNOWN,
            };

        /// <summary>
        /// Return the <see cref="ResourceType"/> with the provided code or <see cref="ResourceType.UNKNOWN"/> if
        /// one cannot be found — Java's <c>fromCode(byte)</c>.
        /// </summary>
        /// <param name="code">The code.</param>
        /// <returns>The resource type; never throws.</returns>
        public static ResourceType FromCode(byte code) =>
            code <= (byte)ResourceType.USER ? (ResourceType)code : ResourceType.UNKNOWN;
    }

    extension(ResourceType resourceType)
    {
        /// <summary>Return the code of this resource — Java's <c>code()</c>.</summary>
        /// <returns>
        /// The code, which is the member's value. Of a value no member has, only the low byte is kept:
        /// <c>((ResourceType)259).Code()</c> is 3.
        /// </returns>
        public byte Code() => unchecked((byte)resourceType);

        /// <summary>Return whether this resource type is UNKNOWN — Java's <c>isUnknown()</c>.</summary>
        /// <returns>Whether this resource type is <see cref="ResourceType.UNKNOWN"/>.</returns>
        public bool IsUnknown() => resourceType == ResourceType.UNKNOWN;
    }
}
