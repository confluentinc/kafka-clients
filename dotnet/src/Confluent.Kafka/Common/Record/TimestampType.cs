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

using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Common.Record;

/// <summary>
/// The kind of a <see cref="ConsumerRecord{TKey, TValue}"/>'s timestamp, Java's <c>TimestampType</c>, each
/// value its Java id.
/// </summary>
/// <remarks>
/// <para>
/// <c>ToString()</c> gives the Java constant name (<c>CREATE_TIME</c>), not Java's <c>toString()</c>
/// text (<c>"CreateTime"</c>): a recorded deviation. Java's text is <c>TimestampType.ToString(type)</c> on C# 14,
/// and <see cref="TimestampTypeMethods.ToString(TimestampType)"/> on older C#.
/// </para>
/// <para>
/// <see cref="Enum.GetValues(Type)"/> orders the members by value read as unsigned, so <c>NO_TIMESTAMP_TYPE</c>
/// (-1) comes last, where Java's <c>values()</c> puts it first.
/// </para>
/// </remarks>
public enum TimestampType
{
    /// <summary>No timestamp: Java's <c>NO_TIMESTAMP_TYPE</c>.</summary>
    NO_TIMESTAMP_TYPE = -1,

    /// <summary>The time the record was produced: Java's <c>CREATE_TIME</c>.</summary>
    CREATE_TIME = 0,

    /// <summary>The time the broker appended the record to the log: Java's <c>LOG_APPEND_TIME</c>.</summary>
    LOG_APPEND_TIME = 1,
}

/// <summary>
/// The static methods Java declares on the <see cref="TimestampType"/> enum itself, which a C# enum cannot carry.
/// From C# 14 they are called on the enum (<c>TimestampType.ForName(…)</c>); older C# calls them on this class.
/// </summary>
public static class TimestampTypeMethods
{
    // Java's values(), written out: Enum.GetValues(Type) is not Native AOT safe.
    private static readonly TimestampType[] s_values =
    {
        TimestampType.NO_TIMESTAMP_TYPE,
        TimestampType.CREATE_TIME,
        TimestampType.LOG_APPEND_TIME,
    };

    extension(TimestampType)
    {
        /// <summary>
        /// The timestamp type whose Java <c>toString()</c> text is exactly <paramref name="name"/> — Java's
        /// <c>forName(String)</c>.
        /// </summary>
        /// <param name="name">The text, such as <c>"CreateTime"</c>.</param>
        /// <returns>The timestamp type.</returns>
        /// <exception cref="NoSuchElementException">
        /// No timestamp type has that text, with Java's message "Invalid timestamp type &lt;name&gt;" (<c>null</c>
        /// for a <see langword="null"/> name).
        /// </exception>
        public static TimestampType ForName(string name)
        {
            foreach (TimestampType type in s_values)
            {
                if (string.Equals(TextOf(type), name, StringComparison.Ordinal))
                {
                    return type;
                }
            }

            throw new NoSuchElementException("Invalid timestamp type " + (name ?? "null"));
        }

        /// <summary>Java's <c>toString()</c> text of <paramref name="type"/>, such as <c>"CreateTime"</c>.</summary>
        /// <param name="type">The timestamp type.</param>
        /// <returns>The text, or the number for a value that is no constant.</returns>
        public static string ToString(TimestampType type) =>
            TextOf(type) ?? ((int)type).ToString(CultureInfo.InvariantCulture);
    }

    private static string? TextOf(TimestampType type) =>
        type switch
        {
            TimestampType.NO_TIMESTAMP_TYPE => "NoTimestampType",
            TimestampType.CREATE_TIME => "CreateTime",
            TimestampType.LOG_APPEND_TIME => "LogAppendTime",
            _ => null,
        };
}
