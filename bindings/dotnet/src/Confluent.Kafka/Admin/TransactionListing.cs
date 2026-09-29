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

namespace Confluent.Kafka.Admin;

/// <summary>
/// One transaction in a listing — Java's
/// <c>org.apache.kafka.clients.admin.TransactionListing</c> (<c>:21</c>).
/// </summary>
public sealed class TransactionListing
{
    /// <summary>Creates a transaction listing — Java's constructor (<c>:26-34</c>).</summary>
    /// <param name="transactionalId">The transactional id.</param>
    /// <param name="producerId">The producer id.</param>
    /// <param name="state">The transaction state.</param>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    public TransactionListing(string transactionalId, long producerId, TransactionState state)
    {
        TransactionalId = transactionalId ?? throw new ArgumentNullException(nameof(transactionalId));
        ProducerId = producerId;
        State = state;
    }

    /// <summary>The transactional id — Java's <c>transactionalId()</c> (<c>:36</c>).</summary>
    public string TransactionalId { get; }

    /// <summary>The producer id — Java's <c>producerId()</c> (<c>:40</c>).</summary>
    public long ProducerId { get; }

    /// <summary>
    /// The transaction state — Java's <c>state()</c> (<c>:44</c>). Named for the accessor,
    /// not for Java's <c>transactionState</c> field (<c>:24</c>).
    /// </summary>
    public TransactionState State { get; }

    /// <summary>Value equality over all three fields — Java's <c>equals</c> (<c>:49</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same listing.</returns>
    public override bool Equals(object? obj) =>
        obj is TransactionListing other
        && ProducerId == other.ProducerId
        && string.Equals(TransactionalId, other.TransactionalId, StringComparison.Ordinal)
        && State == other.State;

    /// <summary>The hash of all three fields — Java's <c>hashCode</c> (<c>:59</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = StringComparer.Ordinal.GetHashCode(TransactionalId);
            hash = (hash * 31) + ProducerId.GetHashCode();
            return (hash * 31) + (int)State;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:64</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "TransactionListing(transactionalId='{0}', producerId={1}, transactionState={2})",
            TransactionalId,
            ProducerId,
            State);
}
