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
/// Reads one broker's row of the <c>listTransactions</c> table — the nested <c>(i, j)</c> walk
/// over that broker's transaction listings.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The inner walk is bounded by <c>get_listing_count(i)</c>, never by the outer
/// <c>count</c>.</b> The outer count is the number of <em>brokers</em>; the inner is that one
/// broker's listings, and is <c>0</c> for a broker whose listing failed
/// (<c>confluent_kafka.h:10225-10228</c>).
/// </para>
/// <para>
/// ⚠ The state crosses as Java's <c>toString()</c> spelling and is decoded by
/// <see cref="TransactionMarshal"/> — the same table <c>describeTransactions</c> reads and
/// <see cref="ListTransactionsOptions.FilteredStates"/> writes.
/// </para>
/// <para>
/// The accessor set is a <b>parameter</b> for the <see cref="UserScramCredentialMarshal"/>
/// reason: both mocks fail this RPC's whole call, so the ABI offers no way to construct a
/// populated result to drive the walk on.
/// </para>
/// </remarks>
internal static class TransactionListingMarshal
{
    /// <summary>The production <c>kafka_admin_ListTransactionsResult_*</c> set.</summary>
    internal static readonly Accessors NativeAccessors = new Accessors(
        NativeMethods.ListTransactionsResultGetListingCount,
        NativeMethods.ListTransactionsResultGetTransactionalId,
        NativeMethods.ListTransactionsResultGetProducerId,
        NativeMethods.ListTransactionsResultGetState);

    /// <summary>Reads broker row <paramref name="index"/> through the production accessors.</summary>
    /// <param name="result">The owned result root.</param>
    /// <param name="index">The broker row index, inside the result's own count.</param>
    /// <returns>That broker's copied-out listings.</returns>
    internal static IReadOnlyCollection<TransactionListing> ReadListings(IntPtr result, int index) =>
        ReadListings(result, index, NativeAccessors);

    /// <summary>Reads broker row <paramref name="index"/> through an injected accessor set.</summary>
    /// <param name="result">The result root, or a stand-in under an injected set.</param>
    /// <param name="index">The broker row index.</param>
    /// <param name="accessors">The four accessors to decode it with.</param>
    /// <returns>That broker's copied-out listings.</returns>
    /// <exception cref="KafkaException">
    /// A listing inside the row's own count carried no transactional id.
    /// </exception>
    internal static IReadOnlyCollection<TransactionListing> ReadListings(
        IntPtr result, int index, Accessors accessors)
    {
        // ⚠ Its own count — never the outer broker count. See the type remarks.
        int listingCount = accessors.GetListingCount(result, index);
        List<TransactionListing> listings =
            new List<TransactionListing>(Math.Max(listingCount, 0));
        for (int listing = 0; listing < listingCount; listing++)
        {
            string transactionalId = Utf8Marshal.PtrToString(
                    accessors.GetTransactionalId(result, index, listing))
                ?? throw new KafkaException(
                    "The listTransactions result produced no transactional id for a listing "
                    + "within its own count.");

            listings.Add(
                new TransactionListing(
                    transactionalId,
                    accessors.GetProducerId(result, index, listing),
                    TransactionMarshal.ReadState(accessors.GetState(result, index, listing))));
        }

        return listings;
    }

    /// <summary>Reads a borrowed string indexed by <c>(broker, listing)</c>.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The broker row index.</param>
    /// <param name="listingIndex">The listing index within that broker.</param>
    /// <returns>The borrowed pointer, or <c>IntPtr.Zero</c> when either index is out of range.</returns>
    internal delegate IntPtr NestedStringAccessor(IntPtr result, int index, int listingIndex);

    /// <summary>Reads an <c>int64_t</c> indexed by <c>(broker, listing)</c>.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The broker row index.</param>
    /// <param name="listingIndex">The listing index within that broker.</param>
    /// <returns>The value, or <c>-1</c> when either index is out of range.</returns>
    internal delegate long NestedInt64Accessor(IntPtr result, int index, int listingIndex);

    /// <summary>The four per-broker accessors, as one set.</summary>
    internal sealed class Accessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        /// <param name="getListingCount">That broker's listing count — the inner bound.</param>
        /// <param name="getTransactionalId">One listing's transactional id, borrowed.</param>
        /// <param name="getProducerId">One listing's producer id.</param>
        /// <param name="getState">One listing's state name, borrowed.</param>
        internal Accessors(
            Func<IntPtr, int, int> getListingCount,
            NestedStringAccessor getTransactionalId,
            NestedInt64Accessor getProducerId,
            NestedStringAccessor getState)
        {
            GetListingCount = getListingCount;
            GetTransactionalId = getTransactionalId;
            GetProducerId = getProducerId;
            GetState = getState;
        }

        /// <summary><c>get_listing_count(i)</c> — the inner walk's bound.</summary>
        internal Func<IntPtr, int, int> GetListingCount { get; }

        /// <summary><c>get_transactional_id(i, j)</c> — borrowed.</summary>
        internal NestedStringAccessor GetTransactionalId { get; }

        /// <summary><c>get_producer_id(i, j)</c>.</summary>
        internal NestedInt64Accessor GetProducerId { get; }

        /// <summary><c>get_state(i, j)</c> — borrowed, Java's <c>toString()</c> spelling.</summary>
        internal NestedStringAccessor GetState { get; }
    }
}
