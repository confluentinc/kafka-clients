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
using System.Linq;
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The per-broker futures of a <c>listTransactions</c> <c>byBrokerId()</c> value, over client-less completed C futures,
/// whose <c>get_cb</c> delivers inline: the map is built once, whether a state read or the delivery settles the outer
/// future first, and the delivery settles every broker of that map.
/// </summary>
public sealed class AdminListTransactionsBrokerListingsTests
{
    [Fact]
    public void AStateReadBeforeTheDelivery_BuildsTheMap_AndTheDeliverySettlesThatMap()
    {
        using ByBrokerIdValue value = new ByBrokerIdValue();
        SafeKafkaFutureHandle outer = SafeKafkaFutureHandle.Owned(value.Outer);
        NativeFutureSource<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> source =
            NativeAdminClient.BrokerListings.Create(outer);

        IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> early = source.Future.GetNow(null!);

        Assert.NotNull(early);
        Assert.Equal(new[] { 1, 2 }, early.Keys.OrderBy(broker => broker));
        Assert.All(early.Values, broker => Assert.False(broker.IsSettled, "a broker settled before the delivery"));
        Assert.False(outer.IsClosed);

        source.Issue();

        Assert.Same(early, source.Future.GetNow(null!));
        ByBrokerIdValue.AssertSettled(early);
        Assert.True(outer.IsClosed, "a per-broker future built and never delivered still holds the outer future");
    }

    [Fact]
    public void WithoutAStateRead_TheDeliveryBuildsTheMap_AndSettlesEveryBroker()
    {
        using ByBrokerIdValue value = new ByBrokerIdValue();
        SafeKafkaFutureHandle outer = SafeKafkaFutureHandle.Owned(value.Outer);
        NativeFutureSource<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> source =
            NativeAdminClient.BrokerListings.Create(outer);

        source.Issue();

        Assert.True(source.Future.IsSettled);
        IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> brokers = source.Future.GetNow(null!);
        Assert.Equal(new[] { 1, 2 }, brokers.Keys.OrderBy(broker => broker));
        ByBrokerIdValue.AssertSettled(brokers);
        Assert.True(outer.IsClosed, "a per-broker future built and never delivered still holds the outer future");
    }

    /// <summary>
    /// A <c>byBrokerId()</c> C value built through the header's constructors: brokers 1 and 2, each a completed future of
    /// one listing, in a map the outer completed future holds; frees everything but the outer future, which the test's
    /// handle owns.
    /// </summary>
    private sealed class ByBrokerIdValue : IDisposable
    {
        private readonly int[] _keys = { 1, 2 };
        private readonly GCHandle _keyPin;
        private readonly List<IntPtr> _listings = new List<IntPtr>();
        private readonly List<IntPtr> _lists = new List<IntPtr>();
        private readonly List<IntPtr> _brokers = new List<IntPtr>();
        private readonly IntPtr _map;

        internal ByBrokerIdValue()
        {
            _keyPin = GCHandle.Alloc(_keys, GCHandleType.Pinned);
            IntPtr outErr = IntPtr.Zero;
            _map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            for (int i = 0; i < _keys.Length; i++)
            {
                IntPtr broker = CompletedFuture(ListOf(ListingOf(_keys[i])));
                NativeMethods.MapPut(_map, IntPtr.Add(_keyPin.AddrOfPinnedObject(), i * sizeof(int)), broker, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            Outer = NativeMethods.KafkaFutureCompletedFuture(_map, IntPtr.Zero, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        internal IntPtr Outer { get; }

        internal static void AssertSettled(
            IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> brokers)
        {
            foreach (KeyValuePair<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> broker in brokers)
            {
                Assert.True(broker.Value.IsSettled, $"broker {broker.Key} was not settled by the delivery");
                Assert.Equal(Expected(broker.Key), Assert.Single(broker.Value.GetNow(null!)));
            }
        }

        public void Dispose()
        {
            IntPtr outErr = IntPtr.Zero;
            foreach (IntPtr broker in _brokers)
            {
                NativeMethods.KafkaFutureDestroy(broker, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            NativeMethods.MapDestroy(_map, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
            foreach (IntPtr list in _lists)
            {
                NativeMethods.ListDestroy(list, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr listing in _listings)
            {
                Native.TransactionListingDestroy(listing, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            _keyPin.Free();
        }

        private static TransactionListing Expected(int broker) =>
            new TransactionListing(
                "txn-ø-" + broker, 100L + broker, broker == 1 ? TransactionState.Ongoing : TransactionState.PrepareCommit);

        private IntPtr ListingOf(int broker)
        {
            TransactionListing expected = Expected(broker);
            using Utf8Marshal.PinnedUtf8String id = Utf8Marshal.Pin(expected.TransactionalId);
            IntPtr outErr = IntPtr.Zero;
            IntPtr listing = Native.TransactionListingNew(
                id.Pointer, expected.ProducerId, TransactionMarshal.Singleton(expected.State), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            _listings.Add(listing);
            return listing;
        }

        private IntPtr ListOf(IntPtr listing)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = NativeMethods.ListNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            _lists.Add(list);
            NativeMethods.ListAdd(list, listing, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return list;
        }

        private IntPtr CompletedFuture(IntPtr value)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr future = NativeMethods.KafkaFutureCompletedFuture(value, IntPtr.Zero, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            _brokers.Add(future);
            return future;
        }
    }

    /// <summary>The listing constructor and destructor no production path calls.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionListing_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TransactionListingNew(
            IntPtr transactionalId, long producerId, IntPtr transactionState, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_TransactionListing_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void TransactionListingDestroy(IntPtr self, ref IntPtr outErr);
    }
}
