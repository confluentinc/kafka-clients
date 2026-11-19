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
/// The state of a classic group — Java's <c>org.apache.kafka.common.ClassicGroupState</c>.
/// </summary>
/// <remarks>
/// <para>
/// All six Java constants, in Java's order. <see cref="UNKNOWN"/> is a real member: a state too
/// new for this client reads as it, as Java's <c>parse</c> maps it.
/// </para>
/// <para>
/// A separate enum from <see cref="GroupState"/>, as in Java: <see cref="GroupState.NOT_READY"/>,
/// <see cref="GroupState.ASSIGNING"/> and <see cref="GroupState.RECONCILING"/> have no
/// counterpart here, so the two are not intercastable.
/// </para>
/// <para>
/// The wire value is Java's <c>toString()</c> spelling (<c>"Stable"</c>,
/// <c>"PreparingRebalance"</c>, …); Java's constants carry no numeric id, so never persist the
/// underlying <see langword="int"/>.
/// </para>
/// <para>
/// <c>ToString()</c> gives the Java constant name (<c>STABLE</c>), not Java's <c>toString()</c>
/// text (<c>"Stable"</c>): a recorded deviation. Java's text is <c>ClassicGroupState.ToString(state)</c>
/// on C# 14, and <see cref="ClassicGroupStateMethods.ToString(ClassicGroupState)"/> on older C#.
/// </para>
/// </remarks>
public enum ClassicGroupState
{
    /// <summary>
    /// The state is not known to this client — Java's <c>UNKNOWN</c>, the value <c>parse</c>
    /// yields for a name it does not recognise.
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
}

/// <summary>
/// The static methods Java declares on the <see cref="ClassicGroupState"/> enum itself, which a C# enum cannot
/// carry. From C# 14 they are called on the enum (<c>ClassicGroupState.Parse(…)</c>); older C# calls them on this
/// class.
/// </summary>
public static class ClassicGroupStateMethods
{
    private static readonly Dictionary<string, ClassicGroupState> s_byUpperCaseText = ByUpperCaseText();

    extension(ClassicGroupState)
    {
        /// <summary>
        /// The state whose Java <c>toString()</c> text is <paramref name="name"/>, in any case — Java's
        /// <c>parse(String)</c>: both are upper-cased as Java's <c>toUpperCase(Locale.ROOT)</c> does.
        /// </summary>
        /// <param name="name">The text, such as <c>"PreparingRebalance"</c>.</param>
        /// <returns>
        /// The state, or <see cref="ClassicGroupState.UNKNOWN"/> when no state has that text.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
        public static ClassicGroupState Parse(string name) =>
            s_byUpperCaseText.TryGetValue(
                JavaText.ToUpperRoot(name ?? throw new ArgumentNullException(nameof(name))),
                out ClassicGroupState state)
                ? state
                : ClassicGroupState.UNKNOWN;

        /// <summary>Java's <c>toString()</c> text of <paramref name="state"/>, such as <c>"Stable"</c>.</summary>
        /// <param name="state">The state.</param>
        /// <returns>The text, or the number for a value that is no constant.</returns>
        public static string ToString(ClassicGroupState state) =>
            TextOf(state) ?? ((int)state).ToString(CultureInfo.InvariantCulture);
    }

    internal static string? TextOf(ClassicGroupState state) =>
        state switch
        {
            ClassicGroupState.UNKNOWN => "Unknown",
            ClassicGroupState.PREPARING_REBALANCE => "PreparingRebalance",
            ClassicGroupState.COMPLETING_REBALANCE => "CompletingRebalance",
            ClassicGroupState.STABLE => "Stable",
            ClassicGroupState.DEAD => "Dead",
            ClassicGroupState.EMPTY => "Empty",
            _ => null,
        };

    private static Dictionary<string, ClassicGroupState> ByUpperCaseText()
    {
        var table = new Dictionary<string, ClassicGroupState>(StringComparer.Ordinal);
        foreach (ClassicGroupState state in (ClassicGroupState[])Enum.GetValues(typeof(ClassicGroupState)))
        {
            table.Add(JavaText.ToUpperRoot(TextOf(state)!), state);
        }

        return table;
    }
}
