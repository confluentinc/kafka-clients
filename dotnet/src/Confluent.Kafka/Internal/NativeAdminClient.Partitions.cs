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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly ResultFuture s_createPartitionsValues = NativeMethods.CreatePartitionsResultValues;

    private static readonly ResultFuture s_deleteRecordsLowWatermarks = NativeMethods.DeleteRecordsResultLowWatermarks;

    private static readonly KeyedResultFuture s_listOffsetsPartitionResult =
        NativeMethods.ListOffsetsResultPartitionResult;

    private static readonly AdminOptionsMarshal.New s_newCreatePartitionsOptions = NativeMethods.CreatePartitionsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<CreatePartitionsOptions> s_setCreatePartitionsOption =
        SetCreatePartitionsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyCreatePartitionsOptions =
        NativeMethods.CreatePartitionsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyCreatePartitionsResult =
        NativeMethods.CreatePartitionsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDeleteRecordsOptions = NativeMethods.DeleteRecordsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DeleteRecordsOptions> s_setDeleteRecordsOption = SetDeleteRecordsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDeleteRecordsOptions =
        NativeMethods.DeleteRecordsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDeleteRecordsResult =
        NativeMethods.DeleteRecordsResultDestroy;

    private static readonly Func<IntPtr, TopicPartition> s_topicPartitionKey = TopicPartitionListMarshal.CopyOutElement;

    private static readonly Func<IntPtr, DeletedRecords> s_copyOutDeletedRecords = CopyOutDeletedRecords;

    private static readonly AdminOptionsMarshal.New s_newListOffsetsOptions = NativeMethods.ListOffsetsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ListOffsetsOptions> s_setListOffsetsOption = SetListOffsetsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyListOffsetsOptions =
        NativeMethods.ListOffsetsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyListOffsetsResult =
        NativeMethods.ListOffsetsResultDestroy;

    private static readonly Func<IntPtr, ListOffsetsResult.ListOffsetsResultInfo> s_copyOutListOffsetsResultInfo =
        ListOffsetsResultInfoMarshal.CopyOut;

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) CreatePartitions(
        IReadOnlyDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions? options)
    {
        EnterUse();
        try
        {
            if (newPartitions is null)
            {
                throw new ArgumentNullException(nameof(newPartitions));
            }

            List<string> keys = new List<string>(newPartitions.Count);
            List<NewPartitions> requested = new List<NewPartitions>(newPartitions.Count);
            HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, NewPartitions> entry in newPartitions)
            {
                if (entry.Key is null)
                {
                    throw new ArgumentException(
                        "The new-partitions map must not contain a null topic name.", nameof(newPartitions));
                }

                AdminStrings.Validate(entry.Key, nameof(newPartitions));
                if (entry.Value is null)
                {
                    throw new ArgumentException(
                        $"The new-partitions entry for topic '{entry.Key}' must not be null.", nameof(newPartitions));
                }

                if (seen.Add(entry.Key))
                {
                    keys.Add(entry.Key);
                    requested.Add(entry.Value);
                }
            }

            List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(keys.Count);
            IntPtr[] natives = new IntPtr[requested.Count];
            IntPtr map = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                map = NewMap();
                for (int i = 0; i < natives.Length; i++)
                {
                    Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(keys[i]);
                    pins.Add(key);
                    natives[i] = NewPartitionsMarshal.Build(requested[i]);
                    MapPut(map, key.Pointer, natives[i]);
                }

                result = CallWithOptions(
                    NewCreatePartitionsOptions(options),
                    s_destroyCreatePartitionsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminCreatePartitionsWithOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyCreatePartitionsResult);
            }
            finally
            {
                DestroyMap(map);
                foreach (IntPtr native in natives)
                {
                    NewPartitionsMarshal.Destroy(native);
                }

                ReleasePins(pins);
            }

            return (
                StartMapFutures(
                    result, s_createPartitionsValues, s_stringKey, StringComparer.Ordinal, s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewCreatePartitionsOptions(CreatePartitionsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newCreatePartitionsOptions,
            options,
            options?.Setters,
            s_setCreatePartitionsOption,
            s_destroyCreatePartitionsOptions);

    internal static void FreeCreatePartitionsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyCreatePartitionsOptions);

    private static void SetCreatePartitionsOption(
        IntPtr native, CreatePartitionsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(CreatePartitionsOptions.TimeoutMs):
                NativeMethods.CreatePartitionsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(CreatePartitionsOptions.ValidateOnly):
                NativeMethods.CreatePartitionsOptionsSetValidateOnly(
                    native, options.ValidateOnly ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            case nameof(CreatePartitionsOptions.RetryOnQuotaViolation):
                NativeMethods.CreatePartitionsOptionsSetRetryOnQuotaViolation(
                    native, options.RetryOnQuotaViolation ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("CreatePartitionsOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<DeletedRecords>> LowWatermarks, SafeAdminResultHandle Result)
        DeleteRecords(IReadOnlyDictionary<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions? options)
    {
        EnterUse();
        try
        {
            if (recordsToDelete is null)
            {
                throw new ArgumentNullException(nameof(recordsToDelete));
            }

            List<TopicPartition> keys = new List<TopicPartition>(recordsToDelete.Count);
            List<long> offsets = new List<long>(recordsToDelete.Count);
            HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
            foreach (KeyValuePair<TopicPartition, RecordsToDelete> entry in recordsToDelete)
            {
                if (entry.Key.Topic is null)
                {
                    throw new ArgumentException(
                        "The records-to-delete map must not contain a topic partition with a null topic.",
                        nameof(recordsToDelete));
                }

                AdminStrings.Validate(entry.Key.Topic, nameof(recordsToDelete));
                if (entry.Value is null)
                {
                    throw new ArgumentException(
                        $"The records-to-delete entry for '{entry.Key}' must not be null.", nameof(recordsToDelete));
                }

                if (seen.Add(entry.Key))
                {
                    keys.Add(entry.Key);
                    offsets.Add(entry.Value.BeforeOffset());
                }
            }

            IntPtr[] partitions = new IntPtr[keys.Count];
            IntPtr[] records = new IntPtr[keys.Count];
            IntPtr map = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                map = NewMap();
                for (int i = 0; i < keys.Count; i++)
                {
                    partitions[i] = NewNativeTopicPartition(keys[i]);
                    IntPtr outErr = IntPtr.Zero;
                    records[i] = NativeMethods.RecordsToDeleteWithOffset(offsets[i], ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    MapPut(map, partitions[i], records[i]);
                }

                result = CallWithOptions(
                    NewDeleteRecordsOptions(options),
                    s_destroyDeleteRecordsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDeleteRecordsWithOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyDeleteRecordsResult);
            }
            finally
            {
                DestroyMap(map);
                for (int i = 0; i < keys.Count; i++)
                {
                    DestroyTopicPartition(partitions[i]);
                    DestroyRecordsToDelete(records[i]);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_deleteRecordsLowWatermarks,
                    s_topicPartitionKey,
                    EqualityComparer<TopicPartition>.Default,
                    s_copyOutDeletedRecords),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDeleteRecordsOptions(DeleteRecordsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDeleteRecordsOptions, options, options?.Setters, s_setDeleteRecordsOption, s_destroyDeleteRecordsOptions);

    internal static void FreeDeleteRecordsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDeleteRecordsOptions);

    private static void SetDeleteRecordsOption(IntPtr native, DeleteRecordsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DeleteRecordsOptions.TimeoutMs):
                NativeMethods.DeleteRecordsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DeleteRecordsOptions has no C setter for " + setter + ".");
        }
    }

    internal static DeletedRecords CopyOutDeletedRecords(IntPtr deletedRecords)
    {
        IntPtr outErr = IntPtr.Zero;
        long lowWatermark = NativeMethods.DeletedRecordsLowWatermark(deletedRecords, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new DeletedRecords(lowWatermark);
    }

    internal (
        IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> Futures,
        SafeAdminResultHandle Result) ListOffsets(
        IReadOnlyDictionary<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions? options)
    {
        EnterUse();
        try
        {
            if (topicPartitionOffsets is null)
            {
                throw new ArgumentNullException(nameof(topicPartitionOffsets));
            }

            if (options is not null
                && options.IsolationLevel != IsolationLevel.ReadUncommitted
                && options.IsolationLevel != IsolationLevel.ReadCommitted)
            {
                throw new ArgumentOutOfRangeException(
                    "options",
                    options.IsolationLevel,
                    "ListOffsetsOptions.IsolationLevel must be IsolationLevel.ReadUncommitted or "
                    + "IsolationLevel.ReadCommitted.");
            }

            List<TopicPartition> keys = new List<TopicPartition>(topicPartitionOffsets.Count);
            List<OffsetSpec?> specs = new List<OffsetSpec?>(topicPartitionOffsets.Count);
            HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
            foreach (KeyValuePair<TopicPartition, OffsetSpec> entry in topicPartitionOffsets)
            {
                if (entry.Key.Topic is null)
                {
                    throw new ArgumentException(
                        "The offsets map must not contain a topic partition with a null topic.",
                        nameof(topicPartitionOffsets));
                }

                AdminStrings.Validate(entry.Key.Topic, nameof(topicPartitionOffsets));
                CheckOffsetSpec(entry.Value, nameof(topicPartitionOffsets));
                if (seen.Add(entry.Key))
                {
                    keys.Add(entry.Key);
                    specs.Add(entry.Value);
                }
            }

            IntPtr[] partitions = new IntPtr[keys.Count];
            IntPtr[] ownedSpecs = new IntPtr[keys.Count];
            IntPtr map = IntPtr.Zero;
            try
            {
                map = NewMap();
                for (int i = 0; i < keys.Count; i++)
                {
                    partitions[i] = NewNativeTopicPartition(keys[i]);
                    IntPtr spec = NativeOffsetSpec(specs[i], out ownedSpecs[i]);
                    MapPut(map, partitions[i], spec);
                }

                SafeAdminResultHandle result = CallWithOptions(
                    NewListOffsetsOptions(options),
                    s_destroyListOffsetsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminListOffsetsWithOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyListOffsetsResult);
                return (StartPartitionResults(result, keys, partitions), result);
            }
            finally
            {
                DestroyMap(map);
                for (int i = 0; i < keys.Count; i++)
                {
                    DestroyTopicPartition(partitions[i]);
                    DestroyOffsetSpec(ownedSpecs[i]);
                }
            }
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewListOffsetsOptions(ListOffsetsOptions? options)
    {
        AdminOptionsMarshal.New create = options is not null
            && options.Setters.InOrder.Contains(nameof(ListOffsetsOptions.IsolationLevel))
                ? (ref IntPtr outErr) =>
                    NativeMethods.ListOffsetsOptionsWithIsolationLevel(NativeIsolationLevel(options.IsolationLevel), ref outErr)
                : s_newListOffsetsOptions;
        return AdminOptionsMarshal.Build(
            create, options, options?.Setters, s_setListOffsetsOption, s_destroyListOffsetsOptions);
    }

    internal static void FreeListOffsetsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyListOffsetsOptions);

    private static void SetListOffsetsOption(IntPtr native, ListOffsetsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ListOffsetsOptions.TimeoutMs):
                NativeMethods.ListOffsetsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(ListOffsetsOptions.IsolationLevel):
                break;
            default:
                throw new InvalidOperationException("ListOffsetsOptions has no C setter for " + setter + ".");
        }
    }

    private static IntPtr NativeIsolationLevel(IsolationLevel isolationLevel)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr level = isolationLevel == IsolationLevel.ReadCommitted
            ? NativeMethods.IsolationLevelReadCommitted(ref outErr)
            : NativeMethods.IsolationLevelReadUncommitted(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return level;
    }

    private static void CheckOffsetSpec(OffsetSpec? spec, string parameterName)
    {
        switch (spec)
        {
            case null:
            case OffsetSpec.LatestSpec:
            case OffsetSpec.EarliestSpec:
            case OffsetSpec.MaxTimestampSpec:
            case OffsetSpec.EarliestLocalSpec:
            case OffsetSpec.LatestTieredSpec:
            case OffsetSpec.EarliestPendingUploadSpec:
            case OffsetSpec.TimestampSpec:
                return;
            default:
                throw new ArgumentException($"Unsupported offset spec '{spec.GetType().Name}'.", parameterName);
        }
    }

    internal static IntPtr NativeOffsetSpec(OffsetSpec? spec, out IntPtr owned)
    {
        owned = IntPtr.Zero;
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = spec switch
        {
            OffsetSpec.EarliestSpec => NativeMethods.OffsetSpecEarliest(ref outErr),
            OffsetSpec.MaxTimestampSpec => NativeMethods.OffsetSpecMaxTimestamp(ref outErr),
            OffsetSpec.EarliestLocalSpec => NativeMethods.OffsetSpecEarliestLocal(ref outErr),
            OffsetSpec.LatestTieredSpec => NativeMethods.OffsetSpecLatestTiered(ref outErr),
            OffsetSpec.EarliestPendingUploadSpec => NativeMethods.OffsetSpecEarliestPendingUpload(ref outErr),
            OffsetSpec.TimestampSpec timestamp => NativeMethods.OffsetSpecForTimestamp(timestamp.Timestamp, ref outErr),
            _ => NativeMethods.OffsetSpecLatest(ref outErr),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (spec is OffsetSpec.TimestampSpec)
        {
            owned = native;
        }

        return native;
    }

    private static Dictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> StartPartitionResults(
        SafeAdminResultHandle result, List<TopicPartition> keys, IntPtr[] partitions)
    {
        try
        {
            Dictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> futures =
                new Dictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>>(keys.Count);
            for (int i = 0; i < keys.Count; i++)
            {
                IntPtr outErr = IntPtr.Zero;
                IntPtr future = s_listOffsetsPartitionResult(
                    result.DangerousGetHandle(), partitions[i], ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                futures.Add(
                    keys[i],
                    NativeFutureSource<ListOffsetsResult.ListOffsetsResultInfo>.Start(
                        SafeKafkaFutureHandle.Owned(future), s_copyOutListOffsetsResultInfo));
            }

            return futures;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    private static IntPtr NewMap()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = NativeMethods.MapNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return map;
    }

    private static void MapPut(IntPtr map, IntPtr key, IntPtr value)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MapPut(map, key, value, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static IntPtr NewNativeTopicPartition(TopicPartition partition)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(partition.Topic))
        {
            native = NativeMethods.TopicPartitionNew(topic.Pointer, partition.Partition, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static void DestroyTopicPartition(IntPtr partition)
    {
        if (partition == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.TopicPartitionDestroy(partition, ref outErr);
        ReleaseError(outErr);
    }

    private static void DestroyRecordsToDelete(IntPtr records)
    {
        if (records == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.RecordsToDeleteDestroy(records, ref outErr);
        ReleaseError(outErr);
    }

    internal static void DestroyOffsetSpec(IntPtr spec)
    {
        if (spec == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.OffsetSpecDestroy(spec, ref outErr);
        ReleaseError(outErr);
    }
}
