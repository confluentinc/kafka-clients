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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The state of a transaction — Java's <c>org.apache.kafka.clients.admin.TransactionState</c>.
/// </summary>
/// <remarks>
/// <para>
/// The wire value is Java's <c>toString()</c> spelling (<c>"PrepareAbort"</c>, …), not
/// <c>name()</c>, in both directions: <see cref="ListTransactionsOptions.FilteredStates()"/> writes
/// it.
/// </para>
/// <para>
/// The broker's <c>DEAD</c> state has no client counterpart: it reads as
/// <see cref="UNKNOWN"/>, as Java's <c>parse</c> does.
/// </para>
/// <para>
/// Members are in Java's declaration order, so <c>default(TransactionState)</c> is
/// <see cref="ONGOING"/> rather than <see cref="UNKNOWN"/>. Never persist the underlying
/// <see langword="int"/>: Java's constants carry no numeric id.
/// </para>
/// <para>
/// <c>ToString()</c> gives the Java constant name (<c>PREPARE_ABORT</c>), not Java's
/// <c>toString()</c> text (<c>"PrepareAbort"</c>): a recorded deviation. Java's text is
/// <c>TransactionState.ToString(state)</c> on C# 14, and
/// <see cref="TransactionStateMethods.ToString(TransactionState)"/> on older C#.
/// </para>
/// </remarks>
public enum TransactionState
{
    /// <summary>Java's <c>ONGOING</c>; wire spelling <c>"Ongoing"</c>.</summary>
    ONGOING,

    /// <summary>Java's <c>PREPARE_ABORT</c>; wire spelling <c>"PrepareAbort"</c>.</summary>
    PREPARE_ABORT,

    /// <summary>Java's <c>PREPARE_COMMIT</c>; wire spelling <c>"PrepareCommit"</c>.</summary>
    PREPARE_COMMIT,

    /// <summary>Java's <c>COMPLETE_ABORT</c>; wire spelling <c>"CompleteAbort"</c>.</summary>
    COMPLETE_ABORT,

    /// <summary>Java's <c>COMPLETE_COMMIT</c>; wire spelling <c>"CompleteCommit"</c>.</summary>
    COMPLETE_COMMIT,

    /// <summary>Java's <c>EMPTY</c>; wire spelling <c>"Empty"</c>.</summary>
    EMPTY,

    /// <summary>Java's <c>PREPARE_EPOCH_FENCE</c>; wire spelling <c>"PrepareEpochFence"</c>.</summary>
    PREPARE_EPOCH_FENCE,

    /// <summary>
    /// Java's <c>UNKNOWN</c>; wire spelling <c>"Unknown"</c>, and the value <c>parse</c> yields
    /// for any name it does not recognise.
    /// </summary>
    UNKNOWN,
}

/// <summary>
/// The static methods Java declares on the <see cref="TransactionState"/> enum itself, which a C# enum cannot carry.
/// From C# 14 they are called on the enum (<c>TransactionState.Parse(…)</c>); older C# calls them on this class.
/// </summary>
public static class TransactionStateMethods
{
    private static readonly Dictionary<string, TransactionState> s_byText = ByText();

    extension(TransactionState)
    {
        /// <summary>Java's <c>toString()</c> text of <paramref name="state"/>, such as <c>"PrepareAbort"</c>.</summary>
        /// <param name="state">The state.</param>
        /// <returns>The text, or the number for a value that is no constant.</returns>
        public static string ToString(TransactionState state) =>
            TextOf(state) ?? ((int)state).ToString(CultureInfo.InvariantCulture);

        /// <summary>
        /// The state whose Java <c>toString()</c> text is exactly <paramref name="name"/> — Java's
        /// <c>parse(String)</c>.
        /// </summary>
        /// <param name="name">The text, such as <c>"PrepareAbort"</c>.</param>
        /// <returns>
        /// The state, or <see cref="TransactionState.UNKNOWN"/> when no state has that text.
        /// </returns>
        public static TransactionState Parse(string name) =>
            name is not null && s_byText.TryGetValue(name, out TransactionState state)
                ? state
                : TransactionState.UNKNOWN;
    }

    internal static string? TextOf(TransactionState state) =>
        state switch
        {
            TransactionState.ONGOING => "Ongoing",
            TransactionState.PREPARE_ABORT => "PrepareAbort",
            TransactionState.PREPARE_COMMIT => "PrepareCommit",
            TransactionState.COMPLETE_ABORT => "CompleteAbort",
            TransactionState.COMPLETE_COMMIT => "CompleteCommit",
            TransactionState.EMPTY => "Empty",
            TransactionState.PREPARE_EPOCH_FENCE => "PrepareEpochFence",
            TransactionState.UNKNOWN => "Unknown",
            _ => null,
        };

    private static Dictionary<string, TransactionState> ByText()
    {
        var table = new Dictionary<string, TransactionState>(StringComparer.Ordinal);
        foreach (TransactionState state in (TransactionState[])Enum.GetValues(typeof(TransactionState)))
        {
            table.Add(TextOf(state)!, state);
        }

        return table;
    }
}
