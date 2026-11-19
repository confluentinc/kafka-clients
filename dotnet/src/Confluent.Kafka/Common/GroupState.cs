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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common;

/// <summary>
/// The state of a group — Java's <c>org.apache.kafka.common.GroupState</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's nine constants, in Java's order. Not every state is reachable for every
/// <see cref="GroupType"/>: see <see cref="GroupStateMethods.GroupStatesForType"/>.
/// </para>
/// <para>
/// The value crossing the C ABI is Java's <c>toString()</c> spelling (<c>"Stable"</c>,
/// <c>"NotReady"</c>, …), not the underlying <c>int</c>, which means nothing outside this
/// assembly; do not persist it.
/// </para>
/// <para>
/// <c>ToString()</c> gives the Java constant name (<c>STABLE</c>), not Java's <c>toString()</c>
/// text (<c>"Stable"</c>): a recorded deviation. Java's text is <c>GroupState.ToString(state)</c> on
/// C# 14, and <see cref="GroupStateMethods.ToString(GroupState)"/> on older C#.
/// </para>
/// </remarks>
public enum GroupState
{
    /// <summary>
    /// The state is not known to this client — Java's <c>UNKNOWN</c>, the state for a name this
    /// client does not recognise.
    /// </summary>
    UNKNOWN,

    /// <summary>Java's <c>PREPARING_REBALANCE</c>.</summary>
    PREPARING_REBALANCE,

    /// <summary>Java's <c>COMPLETING_REBALANCE</c>.</summary>
    COMPLETING_REBALANCE,

    /// <summary>Java's <c>STABLE</c>.</summary>
    STABLE,

    /// <summary>Java's <c>DEAD</c>.</summary>
    DEAD,

    /// <summary>Java's <c>EMPTY</c>.</summary>
    EMPTY,

    /// <summary>Java's <c>ASSIGNING</c>.</summary>
    ASSIGNING,

    /// <summary>Java's <c>RECONCILING</c>.</summary>
    RECONCILING,

    /// <summary>Java's <c>NOT_READY</c>.</summary>
    NOT_READY,
}

/// <summary>
/// The static methods Java declares on the <see cref="GroupState"/> enum itself, which a C#
/// <see langword="enum"/> cannot carry. From C# 14 they are called on the enum
/// (<c>GroupState.Parse(…)</c>); older C# calls them on this class.
/// </summary>
/// <remarks>
/// A recorded deviation from "shape, not logic": the state sets of
/// <see cref="GroupStatesForType"/> are Kafka knowledge kept here, because the C ABI has nothing
/// that answers the question. They go stale if Kafka adds a state to a type.
/// </remarks>
public static class GroupStateMethods
{
    // Java's values(), written out: Enum.GetValues(Type) is not Native AOT safe.
    private static readonly GroupState[] s_values =
    {
        GroupState.UNKNOWN,
        GroupState.PREPARING_REBALANCE,
        GroupState.COMPLETING_REBALANCE,
        GroupState.STABLE,
        GroupState.DEAD,
        GroupState.EMPTY,
        GroupState.ASSIGNING,
        GroupState.RECONCILING,
        GroupState.NOT_READY,
    };

    private static readonly Dictionary<string, GroupState> s_byUpperCaseText = ByUpperCaseText();

    extension(GroupState)
    {
        /// <summary>
        /// The state whose Java <c>toString()</c> text is <paramref name="name"/>, in any case — Java's
        /// <c>parse(String)</c>: both are upper-cased as Java's <c>toUpperCase(Locale.ROOT)</c> does.
        /// </summary>
        /// <param name="name">The text, such as <c>"Stable"</c>.</param>
        /// <returns>The state, or <see cref="GroupState.UNKNOWN"/> when no state has that text.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
        public static GroupState Parse(string name) =>
            s_byUpperCaseText.TryGetValue(
                JavaText.ToUpperRoot(name ?? throw new ArgumentNullException(nameof(name))),
                out GroupState state)
                ? state
                : GroupState.UNKNOWN;

        /// <summary>
        /// The states a group of <paramref name="type"/> can be in — Java's
        /// <c>groupStatesForType</c>.
        /// </summary>
        /// <param name="type">The group type to tabulate.</param>
        /// <returns>A new collection on each call, as Java's <c>Set.of(…)</c>.</returns>
        /// <exception cref="IllegalArgumentException">
        /// <paramref name="type"/> is <see cref="GroupType.UNKNOWN"/> or is not a defined
        /// <see cref="GroupType"/>, with Java's message "Group type not known".
        /// </exception>
        /// <remarks>
        /// <see cref="GroupState.UNKNOWN"/> is in none of the sets, as in Java's code.
        /// </remarks>
        public static IReadOnlyCollection<GroupState> GroupStatesForType(GroupType type)
        {
            switch (type)
            {
                case GroupType.CLASSIC:
                    return new HashSet<GroupState>
                    {
                        GroupState.PREPARING_REBALANCE,
                        GroupState.COMPLETING_REBALANCE,
                        GroupState.STABLE,
                        GroupState.DEAD,
                        GroupState.EMPTY,
                    };

                case GroupType.CONSUMER:
                    return new HashSet<GroupState>
                    {
                        GroupState.PREPARING_REBALANCE,
                        GroupState.COMPLETING_REBALANCE,
                        GroupState.STABLE,
                        GroupState.DEAD,
                        GroupState.EMPTY,
                        GroupState.ASSIGNING,
                        GroupState.RECONCILING,
                    };

                case GroupType.STREAMS:
                    return new HashSet<GroupState>
                    {
                        GroupState.STABLE,
                        GroupState.DEAD,
                        GroupState.EMPTY,
                        GroupState.ASSIGNING,
                        GroupState.RECONCILING,
                        GroupState.NOT_READY,
                    };

                case GroupType.SHARE:
                    return new HashSet<GroupState>
                    {
                        GroupState.STABLE,
                        GroupState.DEAD,
                        GroupState.EMPTY,
                    };

                default:
                    throw new IllegalArgumentException("Group type not known");
            }
        }

        /// <summary>Java's <c>toString()</c> text of <paramref name="state"/>, such as <c>"Stable"</c>.</summary>
        /// <param name="state">The state.</param>
        /// <returns>The text, or the number for a value that is no constant.</returns>
        public static string ToString(GroupState state) =>
            TextOf(state) ?? ((int)state).ToString(CultureInfo.InvariantCulture);
    }

    internal static string? TextOf(GroupState state) =>
        state switch
        {
            GroupState.UNKNOWN => "Unknown",
            GroupState.PREPARING_REBALANCE => "PreparingRebalance",
            GroupState.COMPLETING_REBALANCE => "CompletingRebalance",
            GroupState.STABLE => "Stable",
            GroupState.DEAD => "Dead",
            GroupState.EMPTY => "Empty",
            GroupState.ASSIGNING => "Assigning",
            GroupState.RECONCILING => "Reconciling",
            GroupState.NOT_READY => "NotReady",
            _ => null,
        };

    private static Dictionary<string, GroupState> ByUpperCaseText()
    {
        var table = new Dictionary<string, GroupState>(StringComparer.Ordinal);
        foreach (GroupState state in s_values)
        {
            table.Add(JavaText.ToUpperRoot(TextOf(state)!), state);
        }

        return table;
    }
}
