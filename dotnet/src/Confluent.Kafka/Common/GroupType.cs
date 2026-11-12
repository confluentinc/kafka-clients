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
using System.Collections.Generic;
using System.Globalization;

namespace Confluent.Kafka.Common;

/// <summary>
/// The type of a group — Java's <c>org.apache.kafka.common.GroupType</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's five constants, in Java's order. <see cref="UNKNOWN"/> is a real member, not an
/// error: a broker newer than this client can name a type this client does not know.
/// </para>
/// <para>
/// The value crossing the C ABI is Java's <c>toString()</c> spelling (<c>"Consumer"</c>,
/// <c>"Classic"</c>, <c>"Share"</c>, <c>"Streams"</c>, <c>"Unknown"</c>), not the underlying
/// <c>int</c>, which means nothing outside this assembly; do not persist it.
/// </para>
/// <para>
/// <c>ToString()</c> gives the Java constant name (<c>CONSUMER</c>), not Java's <c>toString()</c>
/// text (<c>"Consumer"</c>): a recorded deviation. Java's text is <c>GroupType.ToString(type)</c> on
/// C# 14, and <see cref="GroupTypeMethods.ToString(GroupType)"/> on older C#.
/// </para>
/// </remarks>
public enum GroupType
{
    /// <summary>
    /// The type is not known to this client — Java's <c>UNKNOWN</c>, the type for a name this
    /// client does not recognise.
    /// </summary>
    UNKNOWN,

    /// <summary>A consumer group using the KIP-848 protocol — Java's <c>CONSUMER</c>.</summary>
    CONSUMER,

    /// <summary>A consumer group using the classic protocol — Java's <c>CLASSIC</c>.</summary>
    CLASSIC,

    /// <summary>A share group — Java's <c>SHARE</c>.</summary>
    SHARE,

    /// <summary>A Kafka Streams group — Java's <c>STREAMS</c>.</summary>
    STREAMS,
}

/// <summary>
/// The static methods Java declares on the <see cref="GroupType"/> enum itself, which a C# enum cannot carry. From
/// C# 14 they are called on the enum (<c>GroupType.Parse(…)</c>); older C# calls them on this class.
/// </summary>
public static class GroupTypeMethods
{
    private static readonly Dictionary<string, GroupType> s_byLowerCaseText = ByLowerCaseText();

    extension(GroupType)
    {
        /// <summary>
        /// The type whose Java <c>toString()</c> text is <paramref name="name"/>, in any case — Java's
        /// <c>parse(String)</c>: both are lower-cased.
        /// </summary>
        /// <param name="name">The text, such as <c>"Consumer"</c>.</param>
        /// <returns>
        /// The type, or <see cref="GroupType.UNKNOWN"/> when <paramref name="name"/> is <see langword="null"/> or no
        /// type has that text.
        /// </returns>
        public static GroupType Parse(string? name) =>
            name is not null && s_byLowerCaseText.TryGetValue(name.ToLowerInvariant(), out GroupType type)
                ? type
                : GroupType.UNKNOWN;

        /// <summary>Java's <c>toString()</c> text of <paramref name="type"/>, such as <c>"Consumer"</c>.</summary>
        /// <param name="type">The type.</param>
        /// <returns>The text, or the number for a value that is no constant.</returns>
        public static string ToString(GroupType type) =>
            TextOf(type) ?? ((int)type).ToString(CultureInfo.InvariantCulture);
    }

    internal static string? TextOf(GroupType type) =>
        type switch
        {
            GroupType.UNKNOWN => "Unknown",
            GroupType.CONSUMER => "Consumer",
            GroupType.CLASSIC => "Classic",
            GroupType.SHARE => "Share",
            GroupType.STREAMS => "Streams",
            _ => null,
        };

    private static Dictionary<string, GroupType> ByLowerCaseText()
    {
        var table = new Dictionary<string, GroupType>(StringComparer.Ordinal);
        foreach (GroupType type in (GroupType[])Enum.GetValues(typeof(GroupType)))
        {
            table.Add(TextOf(type)!.ToLowerInvariant(), type);
        }

        return table;
    }
}
