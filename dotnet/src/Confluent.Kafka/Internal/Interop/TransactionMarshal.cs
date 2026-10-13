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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The bidirectional <see cref="TransactionState"/> ⇄ wire-name table.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The wire name is Java's <c>toString()</c>, not <c>name()</c></b>
/// (<c>TransactionState.java:25-32, :43-45</c>) — <c>"PrepareAbort"</c>, not
/// <c>"PREPARE_ABORT"</c>. The C# identifiers coincide with those spellings today, so
/// <see cref="Enum.ToString()"/> would <em>happen</em> to be right; the coincidence is not the
/// contract, and a later rename would silently change the wire format rather than fail to
/// compile.
/// </para>
/// <para>
/// ⚠ The read direction is wrong outright with <see cref="Enum.Parse(Type, string)"/>, in two
/// ways this table avoids: it <b>throws</b> on a name it does not recognise where Java's
/// <c>parse</c> returns <c>UNKNOWN</c> (<c>:47-49</c>) — the broker-side <c>DEAD</c> is a live
/// example — and it accepts a <b>numeric</b> string, so a <c>"3"</c> on the wire would decode
/// to <see cref="TransactionState.CompleteAbort"/> although Java's constants carry no numeric
/// id at all.
/// </para>
/// <para>
/// Both directions are needed — <c>describeTransactions</c> reads a state,
/// <see cref="ListTransactionsOptions.FilteredStates"/> writes one — and the parse table is
/// <em>derived</em> from the writer, exactly as Java derives <c>NAME_TO_ENUM</c> from
/// <c>values()</c> (<c>:34-35</c>), so the two cannot disagree.
/// </para>
/// </remarks>
internal static class TransactionMarshal
{
    private static readonly Dictionary<string, TransactionState> s_byWireName = BuildParseTable();

    /// <summary>Java's <c>toString()</c> for one state.</summary>
    /// <param name="state">The state, including a value outside the declared set.</param>
    /// <returns>The wire spelling; <c>"Unknown"</c> for an undeclared value.</returns>
    internal static string WireName(TransactionState state) =>
        state switch
        {
            TransactionState.Ongoing => "Ongoing",
            TransactionState.PrepareAbort => "PrepareAbort",
            TransactionState.PrepareCommit => "PrepareCommit",
            TransactionState.CompleteAbort => "CompleteAbort",
            TransactionState.CompleteCommit => "CompleteCommit",
            TransactionState.Empty => "Empty",
            TransactionState.PrepareEpochFence => "PrepareEpochFence",
            _ => "Unknown",
        };

    /// <summary>Java's <c>parse</c> (<c>:47-49</c>): case-sensitive, unrecognised → Unknown.</summary>
    /// <param name="name">A wire spelling, or <see langword="null"/>.</param>
    /// <returns>The state, or <see cref="TransactionState.Unknown"/>.</returns>
    internal static TransactionState Parse(string? name) =>
        name is not null && s_byWireName.TryGetValue(name, out TransactionState state)
            ? state
            : TransactionState.Unknown;

    /// <summary>Decodes a borrowed, NUL-terminated state name from a result root.</summary>
    /// <param name="borrowedName">The borrowed <c>const char*</c>; never freed.</param>
    /// <returns>The state, or <see cref="TransactionState.Unknown"/> when null or unrecognised.</returns>
    internal static TransactionState ReadState(IntPtr borrowedName) =>
        Parse(Utf8Marshal.PtrToString(borrowedName));

    private static Dictionary<string, TransactionState> BuildParseTable()
    {
        Dictionary<string, TransactionState> table =
            new Dictionary<string, TransactionState>(StringComparer.Ordinal);
        foreach (TransactionState state in (TransactionState[])Enum.GetValues(typeof(TransactionState)))
        {
            table[WireName(state)] = state;
        }

        return table;
    }
}
