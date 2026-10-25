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
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly GroupIdsRpc s_adminDescribeTransactionsWithOptions =
        NativeMethods.AdminDescribeTransactionsWithOptions;

    private static readonly GroupIdsRpc s_adminFenceProducersWithOptions = NativeMethods.AdminFenceProducersWithOptions;

    private static readonly RpcWithOptions s_adminListTransactionsWithOptions =
        NativeMethods.AdminListTransactionsWithOptions;

    private static readonly KeyedResultFuture s_describeProducersPartitionResult =
        NativeMethods.DescribeProducersResultPartitionResult;

    private static readonly KeyedResultFuture s_describeTransactionsDescription =
        NativeMethods.DescribeTransactionsResultDescription;

    private static readonly KeyedResultFuture s_fenceProducersEpochId = NativeMethods.FenceProducersResultEpochId;

    private static readonly KeyedResultFuture s_fenceProducersProducerId = NativeMethods.FenceProducersResultProducerId;

    private static readonly ListSetter s_listTransactionsOptionsFilterStates =
        NativeMethods.ListTransactionsOptionsFilterStates;

    private static readonly AdminOptionsMarshal.New s_newDescribeProducersOptions = NativeMethods.DescribeProducersOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeProducersOptions> s_setDescribeProducersOption =
        SetDescribeProducersOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeProducersOptions =
        NativeMethods.DescribeProducersOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeProducersResult =
        NativeMethods.DescribeProducersResultDestroy;

    private static readonly Func<IntPtr, DescribeProducersResult.PartitionProducerState> s_copyOutPartitionProducerState =
        PartitionProducerStateMarshal.CopyOut;

    private static readonly AdminOptionsMarshal.New s_newDescribeTransactionsOptions =
        NativeMethods.DescribeTransactionsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeTransactionsOptions> s_setDescribeTransactionsOption =
        SetDescribeTransactionsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeTransactionsOptions =
        NativeMethods.DescribeTransactionsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeTransactionsResult =
        NativeMethods.DescribeTransactionsResultDestroy;

    private static readonly Func<IntPtr, TransactionDescription> s_copyOutTransactionDescription =
        TransactionDescriptionMarshal.CopyOut;

    private static readonly AdminOptionsMarshal.New s_newAbortTransactionOptions = NativeMethods.AbortTransactionOptionsNew;

    private static readonly AdminOptionsMarshal.Set<AbortTransactionOptions> s_setAbortTransactionOption =
        SetAbortTransactionOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyAbortTransactionOptions =
        NativeMethods.AbortTransactionOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyAbortTransactionResult =
        NativeMethods.AbortTransactionResultDestroy;

    private static readonly ResultFuture s_abortTransactionAll = NativeMethods.AbortTransactionResultAll;

    private static readonly AdminOptionsMarshal.New s_newFenceProducersOptions = NativeMethods.FenceProducersOptionsNew;

    private static readonly AdminOptionsMarshal.Set<FenceProducersOptions> s_setFenceProducersOption =
        SetFenceProducersOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyFenceProducersOptions =
        NativeMethods.FenceProducersOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyFenceProducersResult =
        NativeMethods.FenceProducersResultDestroy;

    private static readonly Func<IntPtr, long> s_copyOutProducerId = static value => Marshal.ReadInt64(value);

    private static readonly Func<IntPtr, short> s_copyOutEpoch = static value => Marshal.ReadInt16(value);

    private static readonly AdminOptionsMarshal.New s_newTerminateTransactionOptions =
        NativeMethods.TerminateTransactionOptionsNew;

    private static readonly AdminOptionsMarshal.Set<TerminateTransactionOptions> s_setTerminateTransactionOption =
        SetTerminateTransactionOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyTerminateTransactionOptions =
        NativeMethods.TerminateTransactionOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyTerminateTransactionResult =
        NativeMethods.TerminateTransactionResultDestroy;

    private static readonly ResultFuture s_terminateTransactionResult = NativeMethods.TerminateTransactionResultResult;

    private static readonly AdminOptionsMarshal.New s_newListTransactionsOptions = NativeMethods.ListTransactionsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ListTransactionsOptions> s_setListTransactionsOption =
        SetListTransactionsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyListTransactionsOptions =
        NativeMethods.ListTransactionsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyListTransactionsResult =
        NativeMethods.ListTransactionsResultDestroy;

    private static readonly ResultFuture s_listTransactionsByBrokerId = NativeMethods.ListTransactionsResultByBrokerId;

    private static readonly Func<TransactionState, IntPtr> s_transactionStateSingleton = TransactionMarshal.Singleton;

    private static readonly Func<IntPtr, IReadOnlyCollection<TransactionListing>> s_copyOutTransactionListings =
        TransactionMarshal.CopyOutListings;

    internal (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>> Partitions, SafeAdminResultHandle Result)
        DescribeProducers(IReadOnlyCollection<TopicPartition> partitions, DescribeProducersOptions? options)
    {
        EnterUse();
        try
        {
            if (partitions is null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            List<TopicPartition> keys = DistinctPartitions(partitions, nameof(partitions));
            IntPtr[] natives = new IntPtr[keys.Count];
            IntPtr list = IntPtr.Zero;
            try
            {
                for (int i = 0; i < keys.Count; i++)
                {
                    natives[i] = NewNativeTopicPartition(keys[i]);
                }

                list = NewList(natives);
                SafeAdminResultHandle result = CallWithOptions(
                    NewDescribeProducersOptions(options),
                    s_destroyDescribeProducersOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeProducersWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyDescribeProducersResult);
                return (StartPartitionProducerStates(result, keys, natives), result);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    DestroyTopicPartition(native);
                }
            }
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeProducersOptions(DescribeProducersOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeProducersOptions,
            options,
            options?.Setters,
            s_setDescribeProducersOption,
            s_destroyDescribeProducersOptions);

    internal static void FreeDescribeProducersOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeProducersOptions);

    private static void SetDescribeProducersOption(
        IntPtr native, DescribeProducersOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeProducersOptions.TimeoutMs):
                NativeMethods.DescribeProducersOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(DescribeProducersOptions.BrokerId):
                NativeMethods.DescribeProducersOptionsSetBrokerId(native, options.BrokerId!.Value, ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeProducersOptions has no C setter for " + setter + ".");
        }
    }

    private static Dictionary<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>>
        StartPartitionProducerStates(SafeAdminResultHandle result, List<TopicPartition> keys, IntPtr[] partitions)
    {
        try
        {
            Dictionary<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>> futures =
                new Dictionary<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>>(keys.Count);
            for (int i = 0; i < keys.Count; i++)
            {
                IntPtr outErr = IntPtr.Zero;
                IntPtr future = s_describeProducersPartitionResult(
                    result.DangerousGetHandle(), partitions[i], ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                futures.Add(
                    keys[i],
                    NativeFutureSource<DescribeProducersResult.PartitionProducerState>.Start(
                        SafeKafkaFutureHandle.Owned(future), s_copyOutPartitionProducerState));
            }

            return futures;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<TransactionDescription>> Descriptions, SafeAdminResultHandle Result)
        DescribeTransactions(IReadOnlyCollection<string> transactionalIds, DescribeTransactionsOptions? options)
    {
        EnterUse();
        try
        {
            if (transactionalIds is null)
            {
                throw new ArgumentNullException(nameof(transactionalIds));
            }

            List<string> keys = DistinctNames(transactionalIds, "transactional ids", nameof(transactionalIds));
            SafeAdminResultHandle result = CallWithGroupIds(
                keys,
                () => NewDescribeTransactionsOptions(options),
                s_destroyDescribeTransactionsOptions,
                s_adminDescribeTransactionsWithOptions,
                s_destroyDescribeTransactionsResult);
            return (StartTransactionDescriptions(result, keys), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeTransactionsOptions(DescribeTransactionsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeTransactionsOptions,
            options,
            options?.Setters,
            s_setDescribeTransactionsOption,
            s_destroyDescribeTransactionsOptions);

    internal static void FreeDescribeTransactionsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeTransactionsOptions);

    private static void SetDescribeTransactionsOption(
        IntPtr native, DescribeTransactionsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeTransactionsOptions.TimeoutMs):
                NativeMethods.DescribeTransactionsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeTransactionsOptions has no C setter for " + setter + ".");
        }
    }

    private static Dictionary<string, KafkaFutureImpl<TransactionDescription>> StartTransactionDescriptions(
        SafeAdminResultHandle result, List<string> transactionalIds)
    {
        try
        {
            Dictionary<string, KafkaFutureImpl<TransactionDescription>> futures =
                new Dictionary<string, KafkaFutureImpl<TransactionDescription>>(transactionalIds.Count, StringComparer.Ordinal);
            foreach (string transactionalId in transactionalIds)
            {
                IntPtr future;
                using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(transactionalId))
                {
                    IntPtr outErr = IntPtr.Zero;
                    future = s_describeTransactionsDescription(
                        result.DangerousGetHandle(), pinned.Pointer, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                }

                futures.Add(
                    transactionalId,
                    NativeFutureSource<TransactionDescription>.Start(
                        SafeKafkaFutureHandle.Owned(future), s_copyOutTransactionDescription));
            }

            return futures;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    internal (KafkaFutureImpl<object?> All, SafeAdminResultHandle Result) AbortTransaction(
        AbortTransactionSpec spec, AbortTransactionOptions? options)
    {
        EnterUse();
        try
        {
            if (spec is null)
            {
                throw new ArgumentNullException(nameof(spec));
            }

            if (spec.TopicPartition.Topic is null)
            {
                throw new ArgumentException(
                    "The abort transaction spec must not carry a topic partition with a null topic.",
                    nameof(spec));
            }

            AdminStrings.Validate(spec.TopicPartition.Topic, nameof(spec));
            IntPtr nativeSpec = NewAbortTransactionSpec(spec);
            try
            {
                SafeAdminResultHandle result = CallWithOptions(
                    NewAbortTransactionOptions(options),
                    s_destroyAbortTransactionOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminAbortTransactionWithOptions(admin, nativeSpec, nativeOptions, ref outErr),
                    s_destroyAbortTransactionResult);
                return (StartOwnedFuture(result, s_abortTransactionAll, s_noValue), result);
            }
            finally
            {
                DestroyAbortTransactionSpec(nativeSpec);
            }
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewAbortTransactionSpec(AbortTransactionSpec spec)
    {
        IntPtr partition = NewNativeTopicPartition(spec.TopicPartition);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr native = NativeMethods.AbortTransactionSpecNew(
                partition, spec.ProducerId, spec.ProducerEpoch, spec.CoordinatorEpoch, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        finally
        {
            DestroyTopicPartition(partition);
        }
    }

    internal static void DestroyAbortTransactionSpec(IntPtr spec)
    {
        if (spec == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.AbortTransactionSpecDestroy(spec, ref outErr);
        ReleaseError(outErr);
    }

    internal static IntPtr NewAbortTransactionOptions(AbortTransactionOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newAbortTransactionOptions,
            options,
            options?.Setters,
            s_setAbortTransactionOption,
            s_destroyAbortTransactionOptions);

    internal static void FreeAbortTransactionOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyAbortTransactionOptions);

    private static void SetAbortTransactionOption(
        IntPtr native, AbortTransactionOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(AbortTransactionOptions.TimeoutMs):
                NativeMethods.AbortTransactionOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("AbortTransactionOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> Producers, SafeAdminResultHandle Result)
        FenceProducers(IReadOnlyCollection<string> transactionalIds, FenceProducersOptions? options)
    {
        EnterUse();
        try
        {
            if (transactionalIds is null)
            {
                throw new ArgumentNullException(nameof(transactionalIds));
            }

            List<string> keys = DistinctNames(transactionalIds, "transactional ids", nameof(transactionalIds));
            SafeAdminResultHandle result = CallWithGroupIds(
                keys,
                () => NewFenceProducersOptions(options),
                s_destroyFenceProducersOptions,
                s_adminFenceProducersWithOptions,
                s_destroyFenceProducersResult);
            return (StartFencedProducers(result, keys), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewFenceProducersOptions(FenceProducersOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newFenceProducersOptions,
            options,
            options?.Setters,
            s_setFenceProducersOption,
            s_destroyFenceProducersOptions);

    internal static void FreeFenceProducersOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyFenceProducersOptions);

    // As Python builds it: once both futures settle, the producer id's failure wins, then the epoch's.
    internal static KafkaFutureImpl<ProducerIdAndEpoch> CombineProducerIdAndEpoch(
        KafkaFutureImpl<long> producerId, KafkaFutureImpl<short> epoch)
    {
        KafkaFutureImpl<ProducerIdAndEpoch> combined = new KafkaFutureImpl<ProducerIdAndEpoch>();
        void Settle(KafkaFutureImpl _)
        {
            if (!producerId.IsSettled || !epoch.IsSettled)
            {
                return;
            }

            if (producerId.Failure is { } producerIdFailure)
            {
                combined.CompleteWithError(producerIdFailure);
            }
            else if (epoch.Failure is { } epochFailure)
            {
                combined.CompleteWithError(epochFailure);
            }
            else
            {
                combined.Complete(new ProducerIdAndEpoch(producerId.GetNow(default), epoch.GetNow(default)));
            }
        }

        producerId.AddListener(Settle);
        epoch.AddListener(Settle);
        return combined;
    }

    private static void SetFenceProducersOption(
        IntPtr native, FenceProducersOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(FenceProducersOptions.TimeoutMs):
                NativeMethods.FenceProducersOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("FenceProducersOptions has no C setter for " + setter + ".");
        }
    }

    private static Dictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> StartFencedProducers(
        SafeAdminResultHandle result, List<string> transactionalIds)
    {
        try
        {
            Dictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> futures =
                new Dictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>>(transactionalIds.Count, StringComparer.Ordinal);
            foreach (string transactionalId in transactionalIds)
            {
                using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(transactionalId))
                {
                    IntPtr outErr = IntPtr.Zero;
                    IntPtr producerId = s_fenceProducersProducerId(
                        result.DangerousGetHandle(), pinned.Pointer, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    KafkaFutureImpl<long> producerIdFuture = NativeFutureSource<long>.Start(
                        SafeKafkaFutureHandle.Owned(producerId), s_copyOutProducerId);

                    IntPtr epoch = s_fenceProducersEpochId(
                        result.DangerousGetHandle(), pinned.Pointer, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    KafkaFutureImpl<short> epochFuture = NativeFutureSource<short>.Start(
                        SafeKafkaFutureHandle.Owned(epoch), s_copyOutEpoch);

                    futures.Add(transactionalId, CombineProducerIdAndEpoch(producerIdFuture, epochFuture));
                }
            }

            return futures;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    internal (KafkaFutureImpl<object?> Future, SafeAdminResultHandle Result) ForceTerminateTransaction(
        string transactionalId, TerminateTransactionOptions? options)
    {
        EnterUse();
        try
        {
            if (transactionalId is null)
            {
                throw new ArgumentNullException(nameof(transactionalId));
            }

            AdminStrings.Validate(transactionalId, nameof(transactionalId));
            using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(transactionalId);
            SafeAdminResultHandle result = CallWithOptions(
                NewTerminateTransactionOptions(options),
                s_destroyTerminateTransactionOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    NativeMethods.AdminForceTerminateTransactionWithOptions(admin, pinned.Pointer, nativeOptions, ref outErr),
                s_destroyTerminateTransactionResult);
            return (StartOwnedFuture(result, s_terminateTransactionResult, s_noValue), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewTerminateTransactionOptions(TerminateTransactionOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newTerminateTransactionOptions,
            options,
            options?.Setters,
            s_setTerminateTransactionOption,
            s_destroyTerminateTransactionOptions);

    internal static void FreeTerminateTransactionOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyTerminateTransactionOptions);

    private static void SetTerminateTransactionOption(
        IntPtr native, TerminateTransactionOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(TerminateTransactionOptions.TimeoutMs):
                NativeMethods.TerminateTransactionOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("TerminateTransactionOptions has no C setter for " + setter + ".");
        }
    }

    internal (KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> ByBrokerId, SafeAdminResultHandle Result)
        ListTransactions(ListTransactionsOptions? options)
    {
        EnterUse();
        try
        {
            AdminStrings.Validate(options?.FilteredTransactionalIdPattern, nameof(options));
            SafeAdminResultHandle result = CallWithOptions(
                NewListTransactionsOptions(options),
                s_destroyListTransactionsOptions,
                s_adminListTransactionsWithOptions,
                s_destroyListTransactionsResult);
            return (StartBrokerListings(result), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewListTransactionsOptions(ListTransactionsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newListTransactionsOptions,
            options,
            options?.Setters,
            s_setListTransactionsOption,
            s_destroyListTransactionsOptions);

    internal static void FreeListTransactionsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyListTransactionsOptions);

    private static void SetListTransactionsOption(
        IntPtr native, ListTransactionsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ListTransactionsOptions.TimeoutMs):
                NativeMethods.ListTransactionsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(ListTransactionsOptions.FilteredStates):
                SetSingletonList(
                    native,
                    options.FilteredStates,
                    s_transactionStateSingleton,
                    s_listTransactionsOptionsFilterStates,
                    ref outErr);
                break;
            case nameof(ListTransactionsOptions.FilteredProducerIds):
                SetFilteredProducerIds(native, options.FilteredProducerIds, ref outErr);
                break;
            case nameof(ListTransactionsOptions.FilteredDuration):
                NativeMethods.ListTransactionsOptionsFilterOnDuration(native, options.FilteredDuration, ref outErr);
                break;
            case nameof(ListTransactionsOptions.FilteredTransactionalIdPattern):
                SetFilteredTransactionalIdPattern(native, options.FilteredTransactionalIdPattern, ref outErr);
                break;
            default:
                throw new InvalidOperationException("ListTransactionsOptions has no C setter for " + setter + ".");
        }
    }

    private static void SetFilteredProducerIds(IntPtr native, IReadOnlyCollection<long> producerIds, ref IntPtr outErr)
    {
        long[] values = new long[producerIds.Count];
        int next = 0;
        foreach (long producerId in producerIds)
        {
            values[next++] = producerId;
        }

        GCHandle pin = GCHandle.Alloc(values, GCHandleType.Pinned);
        IntPtr list = IntPtr.Zero;
        try
        {
            IntPtr first = pin.AddrOfPinnedObject();
            IntPtr[] elements = new IntPtr[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                elements[i] = IntPtr.Add(first, i * sizeof(long));
            }

            list = NewList(elements);
            NativeMethods.ListTransactionsOptionsFilterProducerIds(native, list, ref outErr);
        }
        finally
        {
            DestroyList(list);
            pin.Free();
        }
    }

    private static void SetFilteredTransactionalIdPattern(IntPtr native, string? pattern, ref IntPtr outErr)
    {
        if (pattern is null)
        {
            NativeMethods.ListTransactionsOptionsFilterOnTransactionalIdPattern(native, IntPtr.Zero, ref outErr);
            return;
        }

        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(pattern);
        NativeMethods.ListTransactionsOptionsFilterOnTransactionalIdPattern(native, pinned.Pointer, ref outErr);
    }

    private static KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>>
        StartBrokerListings(SafeAdminResultHandle result)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr outer = s_listTransactionsByBrokerId(result.DangerousGetHandle(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            NativeFutureSource<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> source =
                BrokerListings.Create(SafeKafkaFutureHandle.Owned(outer));
            source.Issue();
            return source.Future;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Builds the per-broker futures of a <c>byBrokerId()</c> value once, whether a state read or the delivery comes
    /// first, and issues their <c>get_cb</c>s from the delivery, while the value is alive.
    /// </summary>
    internal sealed class BrokerListings
    {
        private readonly object _lock = new object();
        private readonly SafeKafkaFutureHandle _outer;
        private Dictionary<int, NativeFutureSource<IReadOnlyCollection<TransactionListing>>>? _sources;
        private IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>? _futures;

        private BrokerListings(SafeKafkaFutureHandle outer)
        {
            _outer = outer;
        }

        internal static NativeFutureSource<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>>
            Create(SafeKafkaFutureHandle outer)
        {
            BrokerListings listings = new BrokerListings(outer);
            return NativeFutureSource<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>>
                .Create(outer, listings.Build, listings.Issue);
        }

        private IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> Build(IntPtr map)
        {
            lock (_lock)
            {
                if (_futures is not null)
                {
                    return _futures;
                }

                Dictionary<int, NativeFutureSource<IReadOnlyCollection<TransactionListing>>> sources =
                    new Dictionary<int, NativeFutureSource<IReadOnlyCollection<TransactionListing>>>();
                List<SafeKafkaFutureHandle> handles = new List<SafeKafkaFutureHandle>();
                try
                {
                    IntPtr outErr = IntPtr.Zero;
                    int count = NativeMethods.MapSize(map, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    for (int i = 0; i < count; i++)
                    {
                        IntPtr key = NativeMethods.MapKey(map, i, ref outErr);
                        KafkaException.ThrowIfFfiFailed(ref outErr);
                        IntPtr future = NativeMethods.MapValue(map, i, ref outErr);
                        KafkaException.ThrowIfFfiFailed(ref outErr);
                        SafeKafkaFutureHandle handle = SafeKafkaFutureHandle.Borrowed(future, _outer);
                        handles.Add(handle);
                        sources.Add(
                            Marshal.ReadInt32(key),
                            NativeFutureSource<IReadOnlyCollection<TransactionListing>>.Create(handle, s_copyOutTransactionListings));
                    }
                }
                catch
                {
                    foreach (SafeKafkaFutureHandle handle in handles)
                    {
                        handle.Dispose();
                    }

                    throw;
                }

                Dictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> futures =
                    new Dictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>(sources.Count);
                foreach (KeyValuePair<int, NativeFutureSource<IReadOnlyCollection<TransactionListing>>> entry in sources)
                {
                    futures.Add(entry.Key, entry.Value.Future);
                }

                _sources = sources;
                _futures = futures;
                return futures;
            }
        }

        private void Issue(IntPtr map)
        {
            Build(map);
            foreach (NativeFutureSource<IReadOnlyCollection<TransactionListing>> source in _sources!.Values)
            {
                try
                {
                    source.Issue();
                }
                catch (Exception e)
                {
                    source.Future.CompleteWithError(e);
                }
            }
        }
    }
}
