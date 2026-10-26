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
    private static readonly ResultFuture s_alterPartitionReassignmentsValues =
        NativeMethods.AlterPartitionReassignmentsResultValues;

    private static readonly ResultFuture s_electLeadersPartitions = NativeMethods.ElectLeadersResultPartitions;

    private static readonly ResultFuture s_listPartitionReassignmentsReassignments =
        NativeMethods.ListPartitionReassignmentsResultReassignments;

    private static readonly AdminOptionsMarshal.New s_newElectLeadersOptions = NativeMethods.ElectLeadersOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ElectLeadersOptions> s_setElectLeadersOption = SetElectLeadersOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyElectLeadersOptions =
        NativeMethods.ElectLeadersOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyElectLeadersResult =
        NativeMethods.ElectLeadersResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newAlterPartitionReassignmentsOptions =
        NativeMethods.AlterPartitionReassignmentsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<AlterPartitionReassignmentsOptions>
        s_setAlterPartitionReassignmentsOption = SetAlterPartitionReassignmentsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyAlterPartitionReassignmentsOptions =
        NativeMethods.AlterPartitionReassignmentsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyAlterPartitionReassignmentsResult =
        NativeMethods.AlterPartitionReassignmentsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newListPartitionReassignmentsOptions =
        NativeMethods.ListPartitionReassignmentsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<ListPartitionReassignmentsOptions>
        s_setListPartitionReassignmentsOption = SetListPartitionReassignmentsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyListPartitionReassignmentsOptions =
        NativeMethods.ListPartitionReassignmentsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyListPartitionReassignmentsResult =
        NativeMethods.ListPartitionReassignmentsResultDestroy;

    private static readonly Func<IntPtr, IReadOnlyDictionary<TopicPartition, KafkaException?>> s_copyOutElectionPartitions =
        ElectLeadersMarshal.CopyOutPartitions;

    private static readonly Func<IntPtr, IReadOnlyDictionary<TopicPartition, PartitionReassignment>> s_copyOutReassignments =
        PartitionReassignmentMarshal.CopyOutReassignments;

    internal (KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> Partitions, SafeAdminResultHandle Result)
        ElectLeaders(
            ElectionType electionType, IReadOnlyCollection<TopicPartition>? partitions, ElectLeadersOptions? options)
    {
        EnterUse();
        try
        {
            if (electionType != ElectionType.Preferred && electionType != ElectionType.Unclean)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(electionType),
                    electionType,
                    "The election type must be ElectionType.Preferred or ElectionType.Unclean.");
            }

            IntPtr nativeElectionType = NativeElectionType(electionType);
            IntPtr list = NewElectionPartitions(partitions, out IntPtr[] natives);
            SafeAdminResultHandle result;
            try
            {
                result = CallWithOptions(
                    NewElectLeadersOptions(options),
                    s_destroyElectLeadersOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr err) =>
                        NativeMethods.AdminElectLeadersWithOptions(admin, nativeElectionType, list, nativeOptions, ref err),
                    s_destroyElectLeadersResult);
            }
            finally
            {
                FreeElectionPartitions(list, natives);
            }

            return (StartOwnedFuture(result, s_electLeadersPartitions, s_copyOutElectionPartitions), result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NativeElectionType(ElectionType electionType)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = electionType == ElectionType.Preferred
            ? NativeMethods.ElectionTypePreferred(ref outErr)
            : NativeMethods.ElectionTypeUnclean(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    internal static IntPtr NewElectionPartitions(IReadOnlyCollection<TopicPartition>? partitions, out IntPtr[] natives)
    {
        List<TopicPartition> selection = DistinctPartitions(partitions, nameof(partitions));
        natives = new IntPtr[selection.Count];
        if (partitions is null)
        {
            return IntPtr.Zero;
        }

        try
        {
            for (int i = 0; i < selection.Count; i++)
            {
                natives[i] = NewNativeTopicPartition(selection[i]);
            }

            return NewList(natives);
        }
        catch
        {
            FreeElectionPartitions(IntPtr.Zero, natives);
            throw;
        }
    }

    internal static void FreeElectionPartitions(IntPtr list, IntPtr[] natives)
    {
        DestroyList(list);
        foreach (IntPtr native in natives)
        {
            DestroyTopicPartition(native);
        }
    }

    internal static IntPtr NewElectLeadersOptions(ElectLeadersOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newElectLeadersOptions,
            options,
            options?.Setters,
            s_setElectLeadersOption,
            s_destroyElectLeadersOptions);

    internal static void FreeElectLeadersOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyElectLeadersOptions);

    private static void SetElectLeadersOption(IntPtr native, ElectLeadersOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ElectLeadersOptions.TimeoutMs):
                NativeMethods.ElectLeadersOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("ElectLeadersOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result)
        AlterPartitionReassignments(
            IReadOnlyDictionary<TopicPartition, NewPartitionReassignment?> reassignments,
            AlterPartitionReassignmentsOptions? options)
    {
        EnterUse();
        try
        {
            if (reassignments is null)
            {
                throw new ArgumentNullException(nameof(reassignments));
            }

            List<TopicPartition> keys = new List<TopicPartition>(reassignments.Count);
            List<NewPartitionReassignment?> targets = new List<NewPartitionReassignment?>(reassignments.Count);
            HashSet<TopicPartition> seen = new HashSet<TopicPartition>();
            foreach (KeyValuePair<TopicPartition, NewPartitionReassignment?> entry in reassignments)
            {
                if (entry.Key.Topic is null)
                {
                    throw new ArgumentException(
                        "The reassignments map must not contain a topic partition with a null topic.",
                        nameof(reassignments));
                }

                AdminStrings.Validate(entry.Key.Topic, nameof(reassignments));
                if (seen.Add(entry.Key))
                {
                    keys.Add(entry.Key);
                    targets.Add(entry.Value);
                }
            }

            IntPtr[] partitions = new IntPtr[keys.Count];
            IntPtr[] values = new IntPtr[keys.Count];
            IntPtr map = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                map = NewMap();
                for (int i = 0; i < keys.Count; i++)
                {
                    partitions[i] = NewNativeTopicPartition(keys[i]);

                    // A null value is the cancel of a pending reassignment, as Java's empty Optional.
                    NewPartitionReassignment? target = targets[i];
                    if (target is not null)
                    {
                        values[i] = NewNativePartitionReassignment(target);
                    }

                    MapPut(map, partitions[i], values[i]);
                }

                result = CallWithOptions(
                    NewAlterPartitionReassignmentsOptions(options),
                    s_destroyAlterPartitionReassignmentsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminAlterPartitionReassignmentsWithOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyAlterPartitionReassignmentsResult);
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
                    DestroyNewPartitionReassignment(value);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_alterPartitionReassignmentsValues,
                    s_topicPartitionKey,
                    EqualityComparer<TopicPartition>.Default,
                    s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewAlterPartitionReassignmentsOptions(AlterPartitionReassignmentsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newAlterPartitionReassignmentsOptions,
            options,
            options?.Setters,
            s_setAlterPartitionReassignmentsOption,
            s_destroyAlterPartitionReassignmentsOptions);

    internal static void FreeAlterPartitionReassignmentsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyAlterPartitionReassignmentsOptions);

    private static void SetAlterPartitionReassignmentsOption(
        IntPtr native, AlterPartitionReassignmentsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(AlterPartitionReassignmentsOptions.TimeoutMs):
                NativeMethods.AlterPartitionReassignmentsOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            case nameof(AlterPartitionReassignmentsOptions.AllowReplicationFactorChange):
                NativeMethods.AlterPartitionReassignmentsOptionsSetAllowReplicationFactorChange(
                    native, (sbyte)(options.AllowReplicationFactorChange ? 1 : 0), ref outErr);
                break;
            default:
                throw new InvalidOperationException("AlterPartitionReassignmentsOptions has no C setter for " + setter + ".");
        }
    }

    private static IntPtr NewNativePartitionReassignment(NewPartitionReassignment target)
    {
        IReadOnlyList<int> replicas = target.TargetReplicas;
        int[] ids = new int[replicas.Count];
        for (int i = 0; i < ids.Length; i++)
        {
            ids[i] = replicas[i];
        }

        GCHandle pinned = GCHandle.Alloc(ids, GCHandleType.Pinned);
        IntPtr list = IntPtr.Zero;
        try
        {
            IntPtr first = pinned.AddrOfPinnedObject();
            IntPtr[] elements = new IntPtr[ids.Length];
            for (int i = 0; i < ids.Length; i++)
            {
                elements[i] = IntPtr.Add(first, i * sizeof(int));
            }

            list = NewList(elements);
            IntPtr outErr = IntPtr.Zero;
            IntPtr native = NativeMethods.NewPartitionReassignmentNew(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        finally
        {
            DestroyList(list);
            pinned.Free();
        }
    }

    private static void DestroyNewPartitionReassignment(IntPtr reassignment)
    {
        if (reassignment == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.NewPartitionReassignmentDestroy(reassignment, ref outErr);
        ReleaseError(outErr);
    }

    internal (
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> Reassignments,
        SafeAdminResultHandle Result) ListPartitionReassignments(
        IReadOnlyCollection<TopicPartition>? partitions, ListPartitionReassignmentsOptions? options)
    {
        EnterUse();
        try
        {
            List<TopicPartition> selection = DistinctPartitions(partitions, nameof(partitions));
            IntPtr[] natives = new IntPtr[selection.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                if (partitions is null)
                {
                    result = CallWithOptions(
                        NewListPartitionReassignmentsOptions(options),
                        s_destroyListPartitionReassignmentsOptions,
                        (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                            NativeMethods.AdminListPartitionReassignmentsWithOptions(admin, nativeOptions, ref outErr),
                        s_destroyListPartitionReassignmentsResult);
                }
                else
                {
                    for (int i = 0; i < selection.Count; i++)
                    {
                        natives[i] = NewNativeTopicPartition(selection[i]);
                    }

                    list = NewList(natives);
                    result = CallWithOptions(
                        NewListPartitionReassignmentsOptions(options),
                        s_destroyListPartitionReassignmentsOptions,
                        (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                            NativeMethods.AdminListPartitionReassignmentsWithPartitionsOptions(
                                admin, list, nativeOptions, ref outErr),
                        s_destroyListPartitionReassignmentsResult);
                }
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    DestroyTopicPartition(native);
                }
            }

            return (
                StartOwnedFuture(result, s_listPartitionReassignmentsReassignments, s_copyOutReassignments),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewListPartitionReassignmentsOptions(ListPartitionReassignmentsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newListPartitionReassignmentsOptions,
            options,
            options?.Setters,
            s_setListPartitionReassignmentsOption,
            s_destroyListPartitionReassignmentsOptions);

    internal static void FreeListPartitionReassignmentsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyListPartitionReassignmentsOptions);

    private static void SetListPartitionReassignmentsOption(
        IntPtr native, ListPartitionReassignmentsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(ListPartitionReassignmentsOptions.TimeoutMs):
                NativeMethods.ListPartitionReassignmentsOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("ListPartitionReassignmentsOptions has no C setter for " + setter + ".");
        }
    }
}
