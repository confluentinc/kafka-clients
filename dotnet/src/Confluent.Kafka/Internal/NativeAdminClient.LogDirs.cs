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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

internal sealed partial class NativeAdminClient
{
    private static readonly ResultFuture s_alterReplicaLogDirsValues = NativeMethods.AlterReplicaLogDirsResultValues;

    private static readonly ResultFuture s_describeLogDirsDescriptions =
        NativeMethods.DescribeLogDirsResultDescriptions;

    private static readonly ResultFuture s_describeReplicaLogDirsValues =
        NativeMethods.DescribeReplicaLogDirsResultValues;

    private static readonly AdminOptionsMarshal.New s_newDescribeLogDirsOptions = NativeMethods.DescribeLogDirsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeLogDirsOptions> s_setDescribeLogDirsOption =
        SetDescribeLogDirsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeLogDirsOptions =
        NativeMethods.DescribeLogDirsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeLogDirsResult =
        NativeMethods.DescribeLogDirsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newAlterReplicaLogDirsOptions =
        NativeMethods.AlterReplicaLogDirsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<AlterReplicaLogDirsOptions> s_setAlterReplicaLogDirsOption =
        SetAlterReplicaLogDirsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyAlterReplicaLogDirsOptions =
        NativeMethods.AlterReplicaLogDirsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyAlterReplicaLogDirsResult =
        NativeMethods.AlterReplicaLogDirsResultDestroy;

    private static readonly AdminOptionsMarshal.New s_newDescribeReplicaLogDirsOptions =
        NativeMethods.DescribeReplicaLogDirsOptionsNew;

    private static readonly AdminOptionsMarshal.Set<DescribeReplicaLogDirsOptions> s_setDescribeReplicaLogDirsOption =
        SetDescribeReplicaLogDirsOption;

    private static readonly AdminOptionsMarshal.Destroy s_destroyDescribeReplicaLogDirsOptions =
        NativeMethods.DescribeReplicaLogDirsOptionsDestroy;

    private static readonly SafeAdminResultHandle.Destroy s_destroyDescribeReplicaLogDirsResult =
        NativeMethods.DescribeReplicaLogDirsResultDestroy;

    private static readonly Func<IntPtr, int> s_int32Key = Marshal.ReadInt32;

    private static readonly Func<IntPtr, TopicPartitionReplica> s_replicaKey = LogDirMarshal.CopyOutReplica;

    private static readonly Func<IntPtr, IReadOnlyDictionary<string, LogDirDescription>> s_copyOutLogDirDescriptions =
        LogDirMarshal.CopyOutDescriptions;

    private static readonly Func<IntPtr, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> s_copyOutReplicaLogDirInfo =
        LogDirMarshal.CopyOutReplicaLogDirInfo;

    internal (
        IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions,
        SafeAdminResultHandle Result) DescribeLogDirs(IReadOnlyCollection<int> brokers, DescribeLogDirsOptions? options)
    {
        EnterUse();
        try
        {
            if (brokers is null)
            {
                throw new ArgumentNullException(nameof(brokers));
            }

            List<int> keys = new List<int>(brokers.Count);
            HashSet<int> seen = new HashSet<int>();
            foreach (int broker in brokers)
            {
                if (seen.Add(broker))
                {
                    keys.Add(broker);
                }
            }

            int[] ids = keys.ToArray();
            GCHandle pinned = GCHandle.Alloc(ids, GCHandleType.Pinned);
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                IntPtr first = pinned.AddrOfPinnedObject();
                IntPtr[] elements = new IntPtr[ids.Length];
                for (int i = 0; i < ids.Length; i++)
                {
                    elements[i] = IntPtr.Add(first, i * sizeof(int));
                }

                list = NewList(elements);
                result = CallWithOptions(
                    NewDescribeLogDirsOptions(options),
                    s_destroyDescribeLogDirsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeLogDirsWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyDescribeLogDirsResult);
            }
            finally
            {
                DestroyList(list);
                pinned.Free();
            }

            return (
                StartMapFutures(
                    result,
                    s_describeLogDirsDescriptions,
                    s_int32Key,
                    EqualityComparer<int>.Default,
                    s_copyOutLogDirDescriptions),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeLogDirsOptions(DescribeLogDirsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeLogDirsOptions,
            options,
            options?.Setters,
            s_setDescribeLogDirsOption,
            s_destroyDescribeLogDirsOptions);

    internal static void FreeDescribeLogDirsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeLogDirsOptions);

    private static void SetDescribeLogDirsOption(
        IntPtr native, DescribeLogDirsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeLogDirsOptions.TimeoutMs):
                NativeMethods.DescribeLogDirsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeLogDirsOptions has no C setter for " + setter + ".");
        }
    }

    internal (IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result)
        AlterReplicaLogDirs(
            IReadOnlyDictionary<TopicPartitionReplica, string> replicaAssignment, AlterReplicaLogDirsOptions? options)
    {
        EnterUse();
        try
        {
            if (replicaAssignment is null)
            {
                throw new ArgumentNullException(nameof(replicaAssignment));
            }

            List<TopicPartitionReplica> keys = new List<TopicPartitionReplica>(replicaAssignment.Count);
            List<string> logDirs = new List<string>(replicaAssignment.Count);
            HashSet<TopicPartitionReplica> seen = new HashSet<TopicPartitionReplica>();
            foreach (KeyValuePair<TopicPartitionReplica, string> entry in replicaAssignment)
            {
                if (entry.Key is null)
                {
                    throw new ArgumentException(
                        "The replica assignment must not contain a null replica.", nameof(replicaAssignment));
                }

                AdminStrings.Validate(entry.Key.Topic, nameof(replicaAssignment));
                if (entry.Value is null)
                {
                    throw new ArgumentException(
                        $"The log directory for '{entry.Key}' must not be null.", nameof(replicaAssignment));
                }

                AdminStrings.Validate(entry.Value, nameof(replicaAssignment));
                if (seen.Add(entry.Key))
                {
                    keys.Add(entry.Key);
                    logDirs.Add(entry.Value);
                }
            }

            List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(logDirs.Count);
            IntPtr[] replicas = new IntPtr[keys.Count];
            IntPtr map = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                map = NewMap();
                for (int i = 0; i < keys.Count; i++)
                {
                    replicas[i] = LogDirMarshal.NewReplica(keys[i]);
                    Utf8Marshal.PinnedUtf8String logDir = Utf8Marshal.Pin(logDirs[i]);
                    pins.Add(logDir);
                    MapPut(map, replicas[i], logDir.Pointer);
                }

                result = CallWithOptions(
                    NewAlterReplicaLogDirsOptions(options),
                    s_destroyAlterReplicaLogDirsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminAlterReplicaLogDirsWithOptions(admin, map, nativeOptions, ref outErr),
                    s_destroyAlterReplicaLogDirsResult);
            }
            finally
            {
                DestroyMap(map);
                foreach (IntPtr replica in replicas)
                {
                    LogDirMarshal.DestroyReplica(replica);
                }

                ReleasePins(pins);
            }

            return (
                StartMapFutures(
                    result,
                    s_alterReplicaLogDirsValues,
                    s_replicaKey,
                    EqualityComparer<TopicPartitionReplica>.Default,
                    s_noValue),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewAlterReplicaLogDirsOptions(AlterReplicaLogDirsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newAlterReplicaLogDirsOptions,
            options,
            options?.Setters,
            s_setAlterReplicaLogDirsOption,
            s_destroyAlterReplicaLogDirsOptions);

    internal static void FreeAlterReplicaLogDirsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyAlterReplicaLogDirsOptions);

    private static void SetAlterReplicaLogDirsOption(
        IntPtr native, AlterReplicaLogDirsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(AlterReplicaLogDirsOptions.TimeoutMs):
                NativeMethods.AlterReplicaLogDirsOptionsSetTimeoutMs(native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("AlterReplicaLogDirsOptions has no C setter for " + setter + ".");
        }
    }

    internal (
        IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> Values,
        SafeAdminResultHandle Result) DescribeReplicaLogDirs(
        IReadOnlyCollection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions? options)
    {
        EnterUse();
        try
        {
            if (replicas is null)
            {
                throw new ArgumentNullException(nameof(replicas));
            }

            List<TopicPartitionReplica> keys = new List<TopicPartitionReplica>(replicas.Count);
            HashSet<TopicPartitionReplica> seen = new HashSet<TopicPartitionReplica>();
            foreach (TopicPartitionReplica replica in replicas)
            {
                if (replica is null)
                {
                    throw new ArgumentException("The replicas must not contain a null element.", nameof(replicas));
                }

                AdminStrings.Validate(replica.Topic, nameof(replicas));
                if (seen.Add(replica))
                {
                    keys.Add(replica);
                }
            }

            IntPtr[] natives = new IntPtr[keys.Count];
            IntPtr list = IntPtr.Zero;
            SafeAdminResultHandle result;
            try
            {
                for (int i = 0; i < keys.Count; i++)
                {
                    natives[i] = LogDirMarshal.NewReplica(keys[i]);
                }

                list = NewList(natives);
                result = CallWithOptions(
                    NewDescribeReplicaLogDirsOptions(options),
                    s_destroyDescribeReplicaLogDirsOptions,
                    (SafeAdminClientHandle admin, IntPtr nativeOptions, ref IntPtr outErr) =>
                        NativeMethods.AdminDescribeReplicaLogDirsWithOptions(admin, list, nativeOptions, ref outErr),
                    s_destroyDescribeReplicaLogDirsResult);
            }
            finally
            {
                DestroyList(list);
                foreach (IntPtr native in natives)
                {
                    LogDirMarshal.DestroyReplica(native);
                }
            }

            return (
                StartMapFutures(
                    result,
                    s_describeReplicaLogDirsValues,
                    s_replicaKey,
                    EqualityComparer<TopicPartitionReplica>.Default,
                    s_copyOutReplicaLogDirInfo),
                result);
        }
        finally
        {
            ExitUse();
        }
    }

    internal static IntPtr NewDescribeReplicaLogDirsOptions(DescribeReplicaLogDirsOptions? options) =>
        AdminOptionsMarshal.Build(
            s_newDescribeReplicaLogDirsOptions,
            options,
            options?.Setters,
            s_setDescribeReplicaLogDirsOption,
            s_destroyDescribeReplicaLogDirsOptions);

    internal static void FreeDescribeReplicaLogDirsOptions(IntPtr native) =>
        AdminOptionsMarshal.Free(native, s_destroyDescribeReplicaLogDirsOptions);

    private static void SetDescribeReplicaLogDirsOption(
        IntPtr native, DescribeReplicaLogDirsOptions options, string setter, ref IntPtr outErr)
    {
        switch (setter)
        {
            case nameof(DescribeReplicaLogDirsOptions.TimeoutMs):
                NativeMethods.DescribeReplicaLogDirsOptionsSetTimeoutMs(
                    native, ToNativeTimeoutMs(options.TimeoutMs), ref outErr);
                break;
            default:
                throw new InvalidOperationException("DescribeReplicaLogDirsOptions has no C setter for " + setter + ".");
        }
    }
}
