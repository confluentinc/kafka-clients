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
/// The kind of leader election to conduct — Java's <c>org.apache.kafka.common.ElectionType</c>,
/// passed to <see cref="Admin.IAdmin.ElectLeaders(ElectionType, System.Collections.Generic.IReadOnlyCollection{TopicPartition}, Admin.ElectLeadersOptions)"/>.
/// </summary>
/// <remarks>
/// <para>
/// Each member's value is Java's <c>ElectionType.value</c> byte, not an ordinal. The value does
/// not cross the C ABI: the call passes the matching <c>kafka_common_ElectionType_t</c>
/// singleton.
/// </para>
/// <para>
/// Java's <c>value</c> field is not ported: a cast gives the value. Java's static
/// <c>valueOf(byte)</c> is <see cref="ElectionTypeMethods.ValueOf"/>.
/// <see cref="Admin.IAdmin.ElectLeaders(ElectionType, System.Collections.Generic.IReadOnlyCollection{TopicPartition}, Admin.ElectLeadersOptions)"/> reads a value cast in from outside the two members as
/// <see cref="ElectionTypeMethods.ValueOf"/> reads it, and throws its <see cref="IllegalArgumentException"/>:
/// <c>Value &lt;value&gt; must be one of [PREFERRED, UNCLEAN]</c>, with the whole number for one outside a byte.
/// </para>
/// </remarks>
public enum ElectionType
{
    /// <summary>
    /// Elect the preferred replica — the first replica in the partition's assignment —
    /// as leader. Java's <c>PREFERRED</c> (value 0).
    /// </summary>
    PREFERRED = 0,

    /// <summary>
    /// Elect any in-sync-or-not replica as leader, accepting possible data loss. Java's
    /// <c>UNCLEAN</c> (value 1).
    /// </summary>
    UNCLEAN = 1,
}

/// <summary>
/// The members Java declares on the <see cref="ElectionType"/> enum itself, which a C# enum cannot carry. From
/// C# 14 the static one is called on the enum (<c>ElectionType.ValueOf(…)</c>); older C# calls it on this class.
/// </summary>
public static class ElectionTypeMethods
{
    private const string ValuesText = "[" + nameof(ElectionType.PREFERRED) + ", " + nameof(ElectionType.UNCLEAN) + "]";

    extension(ElectionType)
    {
        /// <summary>Return the election type with the given value — Java's <c>valueOf(byte)</c>.</summary>
        /// <param name="value">The value.</param>
        /// <returns>The election type.</returns>
        /// <exception cref="IllegalArgumentException">
        /// No election type has <paramref name="value"/>:
        /// <c>Value &lt;value&gt; must be one of [PREFERRED, UNCLEAN]</c>.
        /// </exception>
        public static ElectionType ValueOf(byte value) => value switch
        {
            (byte)ElectionType.PREFERRED => ElectionType.PREFERRED,
            (byte)ElectionType.UNCLEAN => ElectionType.UNCLEAN,
            _ => throw NotOneOf(value),
        };
    }

    // A value outside a byte, which Java's byte cannot hold, gets ValueOf's message with the whole number.
    internal static ElectionType RequireDefined(ElectionType type)
    {
        int value = (int)type;
        return value is >= byte.MinValue and <= byte.MaxValue ? ElectionType.ValueOf((byte)value) : throw NotOneOf(value);
    }

    private static IllegalArgumentException NotOneOf(int value) =>
        new IllegalArgumentException(
            "Value " + value.ToString(CultureInfo.InvariantCulture) + " must be one of " + ValuesText);
}
