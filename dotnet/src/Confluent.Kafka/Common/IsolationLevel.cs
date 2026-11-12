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

using System.Globalization;

namespace Confluent.Kafka.Common;

/// <summary>
/// The isolation of a read, Java's <c>IsolationLevel</c>, used by
/// <see cref="Admin.ListOffsetsOptions.IsolationLevel()"/>.
/// </summary>
/// <remarks>
/// Each value is Java's <c>IsolationLevel.id()</c>, which the C ABI takes unchanged.
/// <c>ToString()</c> gives the Java constant name (<c>READ_COMMITTED</c>), not Java's
/// <c>toString()</c> text (<c>"read_committed"</c>): a recorded deviation. Java's text is
/// <c>IsolationLevel.ToString(level)</c> on C# 14, and
/// <see cref="IsolationLevelMethods.ToString(IsolationLevel)"/> on older C#.
/// </remarks>
public enum IsolationLevel
{
    /// <summary>Read every record, including those of open transactions: Java's <c>READ_UNCOMMITTED</c>.</summary>
    READ_UNCOMMITTED = 0,

    /// <summary>Read only the records of committed transactions: Java's <c>READ_COMMITTED</c>.</summary>
    READ_COMMITTED = 1,
}

/// <summary>
/// The members Java declares on the <see cref="IsolationLevel"/> enum itself, which a C# enum cannot carry. From
/// C# 14 the static ones are called on the enum (<c>IsolationLevel.ForId(…)</c>); older C# calls them on this class.
/// </summary>
public static class IsolationLevelMethods
{
    extension(IsolationLevel)
    {
        /// <summary>Return the isolation level with the given id — Java's <c>forId(byte)</c>.</summary>
        /// <param name="id">The id.</param>
        /// <returns>The isolation level.</returns>
        /// <exception cref="IllegalArgumentException">
        /// No isolation level has <paramref name="id"/>: <c>Unknown isolation level &lt;id&gt;</c>.
        /// </exception>
        public static IsolationLevel ForId(byte id) => id switch
        {
            (byte)IsolationLevel.READ_UNCOMMITTED => IsolationLevel.READ_UNCOMMITTED,
            (byte)IsolationLevel.READ_COMMITTED => IsolationLevel.READ_COMMITTED,
            _ => throw UnknownIsolationLevel(id),
        };

        /// <summary>
        /// Java's <c>toString()</c> text of <paramref name="level"/>: the constant name in lower case, such as
        /// <c>"read_committed"</c>.
        /// </summary>
        /// <param name="level">The isolation level.</param>
        /// <returns>The text, or the number for a value that is no constant.</returns>
        public static string ToString(IsolationLevel level) =>
            level switch
            {
                IsolationLevel.READ_UNCOMMITTED => "read_uncommitted",
                IsolationLevel.READ_COMMITTED => "read_committed",
                _ => ((int)level).ToString(CultureInfo.InvariantCulture),
            };
    }

    extension(IsolationLevel level)
    {
        /// <summary>Return the id of this isolation level — Java's <c>id()</c>.</summary>
        /// <returns>
        /// The id, which is the member's value. Of a value no member has, only the low byte is kept:
        /// <c>((IsolationLevel)259).Id()</c> is 3.
        /// </returns>
        public byte Id() => unchecked((byte)level);
    }

    // An id outside a byte, which Java's byte cannot hold, gets ForId's message with the whole number.
    internal static IsolationLevel RequireDefined(IsolationLevel level)
    {
        int id = (int)level;
        return id is >= byte.MinValue and <= byte.MaxValue ? IsolationLevel.ForId((byte)id) : throw UnknownIsolationLevel(id);
    }

    private static IllegalArgumentException UnknownIsolationLevel(int id) =>
        new IllegalArgumentException("Unknown isolation level " + id.ToString(CultureInfo.InvariantCulture));
}
