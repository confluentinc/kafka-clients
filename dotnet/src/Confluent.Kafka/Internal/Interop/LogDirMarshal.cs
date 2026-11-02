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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the <c>kafka_common_TopicPartitionReplica_t</c> inputs and copies the borrowed log-directory values a
/// future delivers into owned managed objects.
/// </summary>
internal static class LogDirMarshal
{
    internal static IntPtr NewReplica(TopicPartitionReplica replica)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native;
        using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(replica.Topic))
        {
            native = NativeMethods.TopicPartitionReplicaNew(topic.Pointer, replica.Partition, replica.BrokerId, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    internal static void DestroyReplica(IntPtr replica)
    {
        if (replica == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.TopicPartitionReplicaDestroy(replica, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }

    internal static TopicPartitionReplica CopyOutReplica(IntPtr replica)
    {
        IntPtr outErr = IntPtr.Zero;
        string? topic = Utf8Marshal.PtrToString(NativeMethods.TopicPartitionReplicaTopic(replica, ref outErr));
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int partition = NativeMethods.TopicPartitionReplicaPartition(replica, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int brokerId = NativeMethods.TopicPartitionReplicaBrokerId(replica, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new TopicPartitionReplica(topic ?? string.Empty, partition, brokerId);
    }

    internal static IReadOnlyDictionary<string, LogDirDescription> CopyOutDescriptions(IntPtr descriptions)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(descriptions, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<string, LogDirDescription> copied = new Dictionary<string, LogDirDescription>(count, StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            IntPtr path = NativeMethods.MapKey(descriptions, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr description = NativeMethods.MapValue(descriptions, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied[Utf8Marshal.PtrToString(path) ?? string.Empty] = CopyOutDescription(description);
        }

        return copied;
    }

    internal static LogDirDescription CopyOutDescription(IntPtr description)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr borrowedError = NativeMethods.LogDirDescriptionErrorOf(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        // A directory error that is not a Kafka error fails the broker's future, as Python's conversion raises it.
        Exception? error = KafkaException.FromFfiError(borrowedError, owned: false);
        if (error is not null and not KafkaException)
        {
            throw error;
        }

        long totalBytes = NativeMethods.LogDirDescriptionTotalBytesOf(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long usableBytes = NativeMethods.LogDirDescriptionUsableBytesOf(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        bool isCordoned = NativeMethods.LogDirDescriptionIsCordonedOf(description, ref outErr) != 0;
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new LogDirDescription(
            (KafkaException?)error, CopyOutReplicaInfos(description), totalBytes, usableBytes, isCordoned);
    }

    internal static DescribeReplicaLogDirsResult.ReplicaLogDirInfo CopyOutReplicaLogDirInfo(IntPtr info)
    {
        if (info == IntPtr.Zero)
        {
            throw new KafkaException("The describeReplicaLogDirs future delivered no log-directory information.");
        }

        IntPtr outErr = IntPtr.Zero;
        string? currentLogDir = Utf8Marshal.PtrToString(
            NativeMethods.ReplicaLogDirInfoCurrentReplicaLogDirOf(info, ref outErr));
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long currentOffsetLag = NativeMethods.ReplicaLogDirInfoCurrentReplicaOffsetLagOf(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        string? futureLogDir = Utf8Marshal.PtrToString(NativeMethods.ReplicaLogDirInfoFutureReplicaLogDirOf(info, ref outErr));
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long futureOffsetLag = NativeMethods.ReplicaLogDirInfoFutureReplicaOffsetLagOf(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new DescribeReplicaLogDirsResult.ReplicaLogDirInfo(currentLogDir, currentOffsetLag, futureLogDir, futureOffsetLag);
    }

    private static Dictionary<TopicPartition, ReplicaInfo> CopyOutReplicaInfos(IntPtr description)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr infos = NativeMethods.LogDirDescriptionReplicaInfos(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.MapSize(infos, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Dictionary<TopicPartition, ReplicaInfo> replicas = new Dictionary<TopicPartition, ReplicaInfo>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr partition = NativeMethods.MapKey(infos, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr info = NativeMethods.MapValue(infos, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                long size = NativeMethods.ReplicaInfoSize(info, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                long offsetLag = NativeMethods.ReplicaInfoOffsetLag(info, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                bool isFuture = NativeMethods.ReplicaInfoIsFuture(info, ref outErr) != 0;
                KafkaException.ThrowIfFfiFailed(ref outErr);
                replicas[TopicPartitionListMarshal.CopyOutElement(partition)] = new ReplicaInfo(size, offsetLag, isFuture);
            }

            return replicas;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MapDestroy(infos, ref destroyErr);
            if (destroyErr != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(destroyErr);
            }
        }
    }
}
