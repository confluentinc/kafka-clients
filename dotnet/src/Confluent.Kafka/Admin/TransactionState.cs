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
/// <c>toString()</c> text (<c>"PrepareAbort"</c>): a recorded deviation.
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
