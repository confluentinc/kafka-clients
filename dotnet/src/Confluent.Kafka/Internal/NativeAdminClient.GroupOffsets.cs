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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly GroupIdsRpc s_adminDeleteConsumerGroupsWithOptions =
        NativeMethods.AdminDeleteConsumerGroupsWithOptions;

    private static readonly ResultFuture s_alterConsumerGroupOffsetsAll =
        NativeMethods.AlterConsumerGroupOffsetsResultAll;

    private static readonly KeyedResultFuture s_alterConsumerGroupOffsetsPartitionResult =
        NativeMethods.AlterConsumerGroupOffsetsResultPartitionResult;

    private static readonly ResultFuture s_deleteConsumerGroupOffsetsAll =
        NativeMethods.DeleteConsumerGroupOffsetsResultAll;

    private static readonly KeyedResultFuture s_deleteConsumerGroupOffsetsPartitionResult =
        NativeMethods.DeleteConsumerGroupOffsetsResultPartitionResult;

    private static readonly ResultFuture s_deleteConsumerGroupsDeletedGroups =
        NativeMethods.DeleteConsumerGroupsResultDeletedGroups;

    private static readonly KeyedResultFuture s_listConsumerGroupOffsetsPartitionsToOffsetAndMetadataWithGroupId =
        NativeMethods.ListConsumerGroupOffsetsResultPartitionsToOffsetAndMetadataWithGroupId;

    private static readonly ResultFuture s_removeMembersFromConsumerGroupAll =
        NativeMethods.RemoveMembersFromConsumerGroupResultAll;

    private static readonly KeyedResultFuture s_removeMembersFromConsumerGroupMemberResult =
        NativeMethods.RemoveMembersFromConsumerGroupResultMemberResult;

    private static readonly AdminOptionsMarshal.New s_newListConsumerGroupOffsetsOptions =
        NativeMethods.ListConsumerGroupOffsetsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ListConsumerGroupOffsetsOptions> s_setListConsumerGroupOffsetsOption =
        SetListConsumerGroupOffsetsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyListConsumerGroupOffsetsOptions =
        NativeMethods.ListConsumerGroupOffsetsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyListConsumerGroupOffsetsResult =
        NativeMethods.ListConsumerGroupOffsetsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newAlterConsumerGroupOffsetsOptions =
        NativeMethods.AlterConsumerGroupOffsetsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<AlterConsumerGroupOffsetsOptions> s_setAlterConsumerGroupOffsetsOption =
        SetAlterConsumerGroupOffsetsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyAlterConsumerGroupOffsetsOptions =
        NativeMethods.AlterConsumerGroupOffsetsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyAlterConsumerGroupOffsetsResult =
        NativeMethods.AlterConsumerGroupOffsetsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDeleteConsumerGroupOffsetsOptions =
        NativeMethods.DeleteConsumerGroupOffsetsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DeleteConsumerGroupOffsetsOptions> s_setDeleteConsumerGroupOffsetsOption =
        SetDeleteConsumerGroupOffsetsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDeleteConsumerGroupOffsetsOptions =
        NativeMethods.DeleteConsumerGroupOffsetsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDeleteConsumerGroupOffsetsResult =
        NativeMethods.DeleteConsumerGroupOffsetsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDeleteConsumerGroupsOptions =
        NativeMethods.DeleteConsumerGroupsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DeleteConsumerGroupsOptions> s_setDeleteConsumerGroupsOption =
        SetDeleteConsumerGroupsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDeleteConsumerGroupsOptions =
        NativeMethods.DeleteConsumerGroupsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDeleteConsumerGroupsResult =
        NativeMethods.DeleteConsumerGroupsResultDestroy;

    private static readonly AdminOptionsMarshal.Set<RemoveMembersFromConsumerGroupOptions> s_setRemoveMembersFromConsumerGroupOption =
        SetRemoveMembersFromConsumerGroupOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyRemoveMembersFromConsumerGroupOptions =
        NativeMethods.RemoveMembersFromConsumerGroupOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyRemoveMembersFromConsumerGroupResult =
        NativeMethods.RemoveMembersFromConsumerGroupResultDestroy;

    private static readonly Func<IntPtr, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> s_copyOutGroupOffsets =
        OffsetMapMarshal.CopyOutNullable;

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> Futures, SafeAdminResultHandle Result)
        ListConsumerGroupOffsets(string groupId, ListConsumerGroupOffsetsOptions? options)
    {
        EnterUse();
        try
        {
            if (groupId is null)
            {
                throw new ArgumentNullException(nameof(groupId));
            }

            AdminStrings.Validate(groupId, nameof(groupId));
            SafeAdminResultHandle result;
            using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(groupId))
            {
                IntPtr pointer = pinned.Pointer;
                result = CallWithOptions(
                    NewListConsumerGroupOffsetsOptions(options),
                    s_destroyListConsumerGroupOffsetsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminListConsumerGroupOffsetsWithGroupIdOptions(admin, pointer, nativeOptions, ref outErr),
                    s_destroyListConsumerGroupOffsetsResult);
            }

            return (StartGroupOffsetsFutures(result, new List<string> { groupId }), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> Futures, SafeAdminResultHandle Result)
        ListConsumerGroupOffsets(
            IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions? options)
    {
        EnterUse();
        try
        {
            List<KeyValuePair<string, ListConsumerGroupOffsetsSpec>> specs = RequestedGroupSpecs(groupSpecs);
            List<string> keys = new List<string>(specs.Count);
            List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(specs.Count);
            List<IntPtr> nativeSpecs = new List<IntPtr>(specs.Count);
            IntPtr map = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                map = NewMap();
                foreach (KeyValuePair<string, ListConsumerGroupOffsetsSpec> entry in specs)
                {
                    keys.Add(entry.Key);
                    Utf8Marshal.PinnedUtf8String pin = Utf8Marshal.Pin(entry.Key);
                    pins.Add(pin);
                    IntPtr spec = NewNativeGroupSpec(entry.Value);
                    nativeSpecs.Add(spec);
                    MapPut(map, pin.Pointer, spec);
                }

                result = CallWithOptions(
                    NewListConsumerGroupOffsetsOptions(options),
                    s_destroyListConsumerGroupOffsetsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminListConsumerGroupOffsetsWithGroupSpecsOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyListConsumerGroupOffsetsResult);
            }
            finally
            {
                DestroyMap(map);
                foreach (IntPtr spec in nativeSpecs)
                {
                    DestroyGroupSpec(spec);
                }

                ReleasePins(pins);
            }

            return (StartGroupOffsetsFutures(result, keys), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewListConsumerGroupOffsetsOptions(ListConsumerGroupOffsetsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newListConsumerGroupOffsetsOptions,
            options,
            options?.Setters,
            s_setListConsumerGroupOffsetsOption,
            s_destroyListConsumerGroupOffsetsOptions);

    internal static void FreeListConsumerGroupOffsetsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyListConsumerGroupOffsetsOptions);

    private static void SetListConsumerGroupOffsetsOption(
        IntPtr native, ListConsumerGroupOffsetsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ListConsumerGroupOffsetsOptions.TimeoutMs):
                NativeMethods.ListConsumerGroupOffsetsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(ListConsumerGroupOffsetsOptions.RequireStable):
                NativeMethods.ListConsumerGroupOffsetsOptionsSetRequireStable(
                    native, options.RequireStable ? (sbyte)1 : (sbyte)0, ref outErr);
                break;
            default:
                throw new InvalidOperationException("ListConsumerGroupOffsetsOptions has no C setter for " + setter + ".");
        }
    }

    private static List<KeyValuePair<string, ListConsumerGroupOffsetsSpec>> RequestedGroupSpecs(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs)
    {
        if (groupSpecs is null)
        {
            throw new ArgumentNullException(nameof(groupSpecs));
        }

        List<KeyValuePair<string, ListConsumerGroupOffsetsSpec>> specs =
            new List<KeyValuePair<string, ListConsumerGroupOffsetsSpec>>(groupSpecs.Count);
        HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, ListConsumerGroupOffsetsSpec> entry in groupSpecs)
        {
            if (entry.Key is null)
            {
                throw new ArgumentException("The group specs must not contain a null group id.", nameof(groupSpecs));
            }

            AdminStrings.Validate(entry.Key, nameof(groupSpecs));
            if (!seen.Add(entry.Key))
            {
                throw new ArgumentException(
                    $"The group specs must not contain the group id '{entry.Key}' more than once.", nameof(groupSpecs));
            }

            if (entry.Value is null)
            {
                throw new ArgumentException($"The spec for group id '{entry.Key}' must not be null.", nameof(groupSpecs));
            }

            IReadOnlyCollection<TopicPartition>? selected = entry.Value.TopicPartitions;
            if (selected is not null)
            {
                foreach (TopicPartition partition in selected)
                {
                    if (partition.Topic is null)
                    {
                        throw new ArgumentException(
                            $"The spec for group id '{entry.Key}' must not select a topic partition with a null topic.",
                            nameof(groupSpecs));
                    }

                    AdminStrings.Validate(partition.Topic, nameof(groupSpecs));
                }
            }

            specs.Add(entry);
        }

        return specs;
    }

    internal static IntPtr NewNativeGroupSpec(ListConsumerGroupOffsetsSpec spec)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.ListConsumerGroupOffsetsSpecNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IReadOnlyCollection<TopicPartition>? selected = spec.TopicPartitions;
        if (selected is null)
        {
            return native;
        }

        IntPtr[] partitions = new IntPtr[selected.Count];
        IntPtr list = IntPtr.Zero;
        try
        {
            int next = 0;
            foreach (TopicPartition partition in selected)
            {
                partitions[next++] = NewNativeTopicPartition(partition);
            }

            list = NewList(partitions);
            NativeMethods.ListConsumerGroupOffsetsSpecSetTopicPartitions(native, list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        catch
        {
            DestroyGroupSpec(native);
            throw;
        }
        finally
        {
            DestroyList(list);
            foreach (IntPtr partition in partitions)
            {
                DestroyTopicPartition(partition);
            }
        }

        return native;
    }

    internal static void DestroyGroupSpec(IntPtr spec)
    {
        if (spec == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListConsumerGroupOffsetsSpecDestroy(spec, ref outErr);
        ReleaseError(outErr);
    }

    private static Dictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>
        StartGroupOffsetsFutures(SafeAdminResultHandle result, List<string> groupIds)
    {
        try
        {
            Dictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures =
                new Dictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(
                    groupIds.Count, StringComparer.Ordinal);
            foreach (string groupId in groupIds)
            {
                IntPtr future;
                using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(groupId))
                {
                    IntPtr outErr = IntPtr.Zero;
                    future = s_listConsumerGroupOffsetsPartitionsToOffsetAndMetadataWithGroupId(
                        result.DangerousGetHandle(), pinned.Pointer, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                }

                futures[groupId] = NativeFutureSource<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>.Start(
                    SafeKafkaFutureHandle.Owned(future), s_copyOutGroupOffsets);
            }

            return futures;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    internal SafeAdminResultHandle AlterConsumerGroupOffsets(
        string groupId,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions? options)
    {
        EnterUse();
        try
        {
            if (groupId is null)
            {
                throw new ArgumentNullException(nameof(groupId));
            }

            AdminStrings.Validate(groupId, nameof(groupId));
            if (offsets is null)
            {
                throw new ArgumentNullException(nameof(offsets));
            }

            List<KeyValuePair<TopicPartition, OffsetAndMetadata>> entries =
                new List<KeyValuePair<TopicPartition, OffsetAndMetadata>>(offsets.Count);
            foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in offsets)
            {
                if (entry.Key.Topic is null)
                {
                    throw new ArgumentException(
                        "The offsets map must not contain a topic partition with a null topic.", nameof(offsets));
                }

                if (entry.Value is null)
                {
                    throw new ArgumentException("Offset value must not be null.", nameof(offsets));
                }

                AdminStrings.Validate(entry.Key.Topic, nameof(offsets));
                AdminStrings.Validate(entry.Value.Metadata, nameof(offsets));
                entries.Add(entry);
            }

            List<IntPtr> partitions = new List<IntPtr>(entries.Count);
            List<IntPtr> values = new List<IntPtr>(entries.Count);
            IntPtr map = IntPtr.Zero;
            try
            {
                map = NewMap();
                foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> entry in entries)
                {
                    IntPtr partition = NewNativeTopicPartition(entry.Key);
                    partitions.Add(partition);
                    IntPtr value = NewNativeOffsetAndMetadata(entry.Value);
                    values.Add(value);
                    MapPut(map, partition, value);
                }

                using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(groupId);
                IntPtr group = pinned.Pointer;
                return CallWithOptions(
                    NewAlterConsumerGroupOffsetsOptions(options),
                    s_destroyAlterConsumerGroupOffsetsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminAlterConsumerGroupOffsetsWithOptions(admin, group, map, nativeOptions, ref outErr),
                    s_destroyAlterConsumerGroupOffsetsResult);
            }
            finally
            {
                DestroyMap(map);
                foreach (IntPtr partition in partitions)
                {
                    DestroyTopicPartition(partition);
                }

                foreach (IntPtr value in values)
                {
                    DestroyOffsetAndMetadata(value);
                }
            }
        }
        finally
        {
            ExitUse();
        }
    }

    internal static KafkaFutureImpl<object?> AlterConsumerGroupOffsetsPartitionResult(
        SafeAdminResultHandle result, TopicPartition partition)
    {
        IntPtr native = NewResultTopicPartition(partition);
        try
        {
            return StartResultFuture(
                result,
                (IntPtr self, ref IntPtr outErr) =>
                    s_alterConsumerGroupOffsetsPartitionResult(self, native, ref outErr),
                s_noValue);
        }
        finally
        {
            DestroyTopicPartition(native);
        }
    }

    internal static KafkaFutureImpl<object?> AlterConsumerGroupOffsetsAll(SafeAdminResultHandle result) =>
        StartResultFuture(result, s_alterConsumerGroupOffsetsAll, s_noValue);

    internal static IntPtr NewAlterConsumerGroupOffsetsOptions(AlterConsumerGroupOffsetsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newAlterConsumerGroupOffsetsOptions,
            options,
            options?.Setters,
            s_setAlterConsumerGroupOffsetsOption,
            s_destroyAlterConsumerGroupOffsetsOptions);

    internal static void FreeAlterConsumerGroupOffsetsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyAlterConsumerGroupOffsetsOptions);

    private static void SetAlterConsumerGroupOffsetsOption(
        IntPtr native, AlterConsumerGroupOffsetsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(AlterConsumerGroupOffsetsOptions.TimeoutMs):
                NativeMethods.AlterConsumerGroupOffsetsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("AlterConsumerGroupOffsetsOptions has no C setter for " + setter + ".");
        }
    }

    private static IntPtr NewNativeOffsetAndMetadata(OffsetAndMetadata value)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        using (Utf8Marshal.PinnedUtf8String metadata = Utf8Marshal.Pin(value.Metadata ?? string.Empty))
        {
            native = NativeMethods.OffsetAndMetadataWithLeaderEpochMetadata(
                value.Offset, value.LeaderEpoch ?? -1, metadata.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static void DestroyOffsetAndMetadata(IntPtr value)
    {
        if (value == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.OffsetAndMetadataDestroy(value, ref outErr);
        ReleaseError(outErr);
    }

    // A null topic cannot cross the C ABI, so a result lookup rejects it at the call.
    private static IntPtr NewResultTopicPartition(TopicPartition partition)
    {
        if (partition.Topic is null)
        {
            throw new ArgumentException("The partition must not have a null topic.", nameof(partition));
        }

        AdminStrings.Validate(partition.Topic, nameof(partition));
        return NewNativeTopicPartition(partition);
    }

    internal SafeAdminResultHandle DeleteConsumerGroupOffsets(
        string groupId, IReadOnlyCollection<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions? options)
    {
        EnterUse();
        try
        {
            if (groupId is null)
            {
                throw new ArgumentNullException(nameof(groupId));
            }

            AdminStrings.Validate(groupId, nameof(groupId));
            if (partitions is null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            List<TopicPartition> requested = new List<TopicPartition>(partitions.Count);
            HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
            foreach (TopicPartition partition in partitions)
            {
                if (partition.Topic is null)
                {
                    throw new ArgumentException(
                        "The partitions collection must not contain a topic partition with a null topic.",
                        nameof(partitions));
                }

                AdminStrings.Validate(partition.Topic, nameof(partitions));
                if (seen.Add(partition))
                {
                    requested.Add(partition);
                }
            }

            IntPtr[] natives = new IntPtr[requested.Count];
            IntPtr list = IntPtr.Zero;
            try
            {
                for (int i = 0; i < natives.Length; i++)
                {
                    natives[i] = NewNativeTopicPartition(requested[i]);
                }

                list = NewList(natives);
                using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(groupId);
                IntPtr group = pinned.Pointer;
                return CallWithOptions(
                    NewDeleteConsumerGroupOffsetsOptions(options),
                    s_destroyDeleteConsumerGroupOffsetsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDeleteConsumerGroupOffsetsWithOptions(admin, group, list, nativeOptions, ref outErr),
                    s_destroyDeleteConsumerGroupOffsetsResult);
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

    internal static KafkaFutureImpl<object?> DeleteConsumerGroupOffsetsPartitionResult(
        SafeAdminResultHandle result, TopicPartition partition)
    {
        IntPtr native = NewResultTopicPartition(partition);
        try
        {
            return StartResultFuture(
                result,
                (IntPtr self, ref IntPtr outErr) =>
                    s_deleteConsumerGroupOffsetsPartitionResult(self, native, ref outErr),
                s_noValue);
        }
        finally
        {
            DestroyTopicPartition(native);
        }
    }

    internal static KafkaFutureImpl<object?> DeleteConsumerGroupOffsetsAll(SafeAdminResultHandle result) =>
        StartResultFuture(result, s_deleteConsumerGroupOffsetsAll, s_noValue);

    internal static IntPtr NewDeleteConsumerGroupOffsetsOptions(DeleteConsumerGroupOffsetsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDeleteConsumerGroupOffsetsOptions,
            options,
            options?.Setters,
            s_setDeleteConsumerGroupOffsetsOption,
            s_destroyDeleteConsumerGroupOffsetsOptions);

    internal static void FreeDeleteConsumerGroupOffsetsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDeleteConsumerGroupOffsetsOptions);

    private static void SetDeleteConsumerGroupOffsetsOption(
        IntPtr native, DeleteConsumerGroupOffsetsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DeleteConsumerGroupOffsetsOptions.TimeoutMs):
                NativeMethods.DeleteConsumerGroupOffsetsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DeleteConsumerGroupOffsetsOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> DeletedGroups, SafeAdminResultHandle Result)
        DeleteConsumerGroups(IReadOnlyCollection<string> groupIds, DeleteConsumerGroupsOptions? options)
    {
        EnterUse();
        try
        {
            if (groupIds is null)
            {
                throw new ArgumentNullException(nameof(groupIds));
            }

            List<string> keys = DistinctNames(groupIds, "group ids", nameof(groupIds));
            SafeAdminResultHandle result = CallWithGroupIds(
                keys,
                () => NewDeleteConsumerGroupsOptions(options),
                s_destroyDeleteConsumerGroupsOptions,
                s_adminDeleteConsumerGroupsWithOptions,
                s_destroyDeleteConsumerGroupsResult);
            return (
                StartMapFutures(
                    result,
                    s_deleteConsumerGroupsDeletedGroups,
                    s_stringKey,
                    StringComparer.Ordinal,
                    s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDeleteConsumerGroupsOptions(DeleteConsumerGroupsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDeleteConsumerGroupsOptions,
            options,
            options?.Setters,
            s_setDeleteConsumerGroupsOption,
            s_destroyDeleteConsumerGroupsOptions);

    internal static void FreeDeleteConsumerGroupsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDeleteConsumerGroupsOptions);

    private static void SetDeleteConsumerGroupsOption(
        IntPtr native, DeleteConsumerGroupsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DeleteConsumerGroupsOptions.TimeoutMs):
                NativeMethods.DeleteConsumerGroupsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DeleteConsumerGroupsOptions has no C setter for " + setter + ".");
        }
    }

    internal SafeAdminResultHandle RemoveMembersFromConsumerGroup(
        string groupId, RemoveMembersFromConsumerGroupOptions options)
    {
        EnterUse();
        try
        {
            if (groupId is null)
            {
                throw new ArgumentNullException(nameof(groupId));
            }

            if (options is null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            AdminStrings.Validate(groupId, nameof(groupId));
            AdminStrings.Validate(options.Reason, nameof(options));
            foreach (MemberToRemove member in options.Members)
            {
                AdminStrings.Validate(member.GroupInstanceId, nameof(options));
            }

            using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(groupId);
            IntPtr group = pinned.Pointer;
            return CallWithOptions(
                NewRemoveMembersFromConsumerGroupOptions(options),
                s_destroyRemoveMembersFromConsumerGroupOptions,
                (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                    NativeMethods.AdminRemoveMembersFromConsumerGroupWithOptions(admin, group, nativeOptions, ref outErr),
                s_destroyRemoveMembersFromConsumerGroupResult);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static KafkaFutureImpl<object?> RemoveMembersFromConsumerGroupMemberResult(
        SafeAdminResultHandle result, MemberToRemove member)
    {
        AdminStrings.Validate(member.GroupInstanceId, nameof(member));
        IntPtr native = NewNativeMemberToRemove(member);
        try
        {
            return StartResultFuture(
                result,
                (IntPtr self, ref IntPtr outErr) =>
                    s_removeMembersFromConsumerGroupMemberResult(self, native, ref outErr),
                s_noValue);
        }
        finally
        {
            DestroyMemberToRemove(native);
        }
    }

    internal static KafkaFutureImpl<object?> RemoveMembersFromConsumerGroupAll(SafeAdminResultHandle result) =>
        StartResultFuture(result, s_removeMembersFromConsumerGroupAll, s_noValue);

    internal static IntPtr NewRemoveMembersFromConsumerGroupOptions(RemoveMembersFromConsumerGroupOptions options) =>
        AdminOptionsMarshal.Build(
            (ref IntPtr outErr) => NewRemoveMembersOptionsWithMembers(options, ref outErr),
            options,
            options.Setters,
            s_setRemoveMembersFromConsumerGroupOption,
            s_destroyRemoveMembersFromConsumerGroupOptions);

    internal static void FreeRemoveMembersFromConsumerGroupOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyRemoveMembersFromConsumerGroupOptions);

    // The no-argument constructor is Java's "remove every member"; otherwise the members cross at creation.
    private static IntPtr NewRemoveMembersOptionsWithMembers(RemoveMembersFromConsumerGroupOptions options, ref IntPtr outErr)
    {
        if (options.RemoveAll)
        {
            return NativeMethods.RemoveMembersFromConsumerGroupOptionsNew(ref outErr);
        }

        List<IntPtr> members = new List<IntPtr>(options.Members.Count);
        IntPtr list = IntPtr.Zero;
        try
        {
            foreach (MemberToRemove member in options.Members)
            {
                members.Add(NewNativeMemberToRemove(member));
            }

            list = NewList(members.ToArray());
            return NativeMethods.RemoveMembersFromConsumerGroupOptionsWithMembers(list, ref outErr);
        }
        finally
        {
            DestroyList(list);
            foreach (IntPtr member in members)
            {
                DestroyMemberToRemove(member);
            }
        }
    }

    private static void SetRemoveMembersFromConsumerGroupOption(
        IntPtr native, RemoveMembersFromConsumerGroupOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(RemoveMembersFromConsumerGroupOptions.Reason):
                if (options.Reason is null)
                {
                    NativeMethods.RemoveMembersFromConsumerGroupOptionsSetReason(native, IntPtr.Zero, ref outErr);
                }
                else
                {
                    using Utf8Marshal.PinnedUtf8String reason = Utf8Marshal.Pin(options.Reason);
                    NativeMethods.RemoveMembersFromConsumerGroupOptionsSetReason(native, reason.Pointer, ref outErr);
                }

                break;
            case nameof(RemoveMembersFromConsumerGroupOptions.TimeoutMs):
                NativeMethods.RemoveMembersFromConsumerGroupOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("RemoveMembersFromConsumerGroupOptions has no C setter for " + setter + ".");
        }
    }

    private static IntPtr NewNativeMemberToRemove(MemberToRemove member)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        using (Utf8Marshal.PinnedUtf8String groupInstanceId = Utf8Marshal.Pin(member.GroupInstanceId))
        {
            native = NativeMethods.MemberToRemoveNew(groupInstanceId.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static void DestroyMemberToRemove(IntPtr member)
    {
        if (member == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MemberToRemoveDestroy(member, ref outErr);
        ReleaseError(outErr);
    }
}
