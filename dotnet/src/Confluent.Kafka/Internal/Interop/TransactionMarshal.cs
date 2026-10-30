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
/// Marshals the listTransactions values: a <see cref="TransactionState"/> filter as the C singleton its Java
/// <c>toString()</c> name parses to, and the listings a broker's future delivers as <see cref="TransactionListing"/>s.
/// </summary>
internal static class TransactionMarshal
{
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

    internal static IntPtr Singleton(TransactionState state)
    {
        using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(WireName(state));
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = NativeMethods.TransactionStateParse(name.Pointer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    internal static IReadOnlyCollection<TransactionListing> CopyOutListings(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no transaction listings.");
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        TransactionListing[] listings = new TransactionListing[count];
        for (int i = 0; i < count; i++)
        {
            IntPtr listing = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr transactionalId = NativeMethods.TransactionListingTransactionalId(listing, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            long producerId = NativeMethods.TransactionListingProducerId(listing, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr state = NativeMethods.TransactionListingState(listing, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            listings[i] = new TransactionListing(
                Utf8Marshal.PtrToString(transactionalId)
                    ?? throw new KafkaException("The admin result produced no transactional id for a listing."),
                producerId,
                TransactionDescriptionMarshal.StateFrom(state));
        }

        return listings;
    }
}
