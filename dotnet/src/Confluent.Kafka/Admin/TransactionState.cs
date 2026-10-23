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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The state of a transaction — Java's
/// <c>org.apache.kafka.clients.admin.TransactionState</c> (<c>:24-32</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The wire value is Java's <c>toString()</c> display spelling</b> (<c>"PrepareAbort"</c>,
/// <c>"CompleteCommit"</c>, …, <c>:39-46</c>), <em>not</em> <c>name()</c> — and this enum is a
/// writer as well as a reader, since <see cref="ListTransactionsOptions.FilteredStates"/>
/// crosses the boundary. The members below are spelled to match, so a renamed member would
/// change the wire format silently; the bidirectional table that owns the spelling lives in
/// <c>Confluent.Kafka.Internal.Interop.TransactionMarshal</c>, which also states why
/// <c>Enum.Parse</c> is wrong in the read direction.
/// </para>
/// <para>
/// ⚠ The broker-side <c>coordinator.transaction.TransactionState</c> has a ninth constant
/// <c>DEAD</c> with no client counterpart; it is deliberately absent here, and decodes to
/// <see cref="Unknown"/> as Java's <c>parse</c> (<c>:48-49</c>) does.
/// </para>
/// <para>
/// Members are in Java's declaration order, so <c>default(TransactionState)</c> is
/// <see cref="Ongoing"/> rather than <see cref="Unknown"/>. Never persist the underlying
/// <see langword="int"/>: Java's constants carry no numeric id.
/// </para>
/// </remarks>
public enum TransactionState
{
    /// <summary>Java's <c>ONGOING</c> (<c>:25</c>); wire spelling <c>"Ongoing"</c>.</summary>
    Ongoing,

    /// <summary>Java's <c>PREPARE_ABORT</c> (<c>:26</c>); wire spelling <c>"PrepareAbort"</c>.</summary>
    PrepareAbort,

    /// <summary>Java's <c>PREPARE_COMMIT</c> (<c>:27</c>); wire spelling <c>"PrepareCommit"</c>.</summary>
    PrepareCommit,

    /// <summary>Java's <c>COMPLETE_ABORT</c> (<c>:28</c>); wire spelling <c>"CompleteAbort"</c>.</summary>
    CompleteAbort,

    /// <summary>Java's <c>COMPLETE_COMMIT</c> (<c>:29</c>); wire spelling <c>"CompleteCommit"</c>.</summary>
    CompleteCommit,

    /// <summary>Java's <c>EMPTY</c> (<c>:30</c>); wire spelling <c>"Empty"</c>.</summary>
    Empty,

    /// <summary>Java's <c>PREPARE_EPOCH_FENCE</c> (<c>:31</c>); wire spelling <c>"PrepareEpochFence"</c>.</summary>
    PrepareEpochFence,

    /// <summary>
    /// Java's <c>UNKNOWN</c> (<c>:32</c>); wire spelling <c>"Unknown"</c>, and the value
    /// <c>parse</c> yields for any name it does not recognise.
    /// </summary>
    Unknown,
}
