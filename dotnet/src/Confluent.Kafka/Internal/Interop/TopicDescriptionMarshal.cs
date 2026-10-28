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
/// Copies a borrowed <c>kafka_admin_TopicDescription_t</c> into a <see cref="TopicDescription"/> through its owned
/// <c>topic_id</c>, <c>partitions()</c>, node and <c>authorizedOperations()</c> copies, which it frees; a null list is
/// Java's null.
/// </summary>
internal static class TopicDescriptionMarshal
{
    internal static TopicDescription CopyOut(IntPtr description)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr name = NativeMethods.TopicDescriptionName(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte isInternal = NativeMethods.TopicDescriptionIsInternal(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr topicId = NativeMethods.TopicDescriptionTopicId(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Uuid id = UuidMarshal.CopyOutAndDestroy(topicId);
        List<TopicPartitionInfo> partitions = CopyOutPartitions(description);
        IntPtr operations = NativeMethods.TopicDescriptionAuthorizedOperations(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new TopicDescription(
            Utf8Marshal.PtrToString(name) ?? string.Empty,
            isInternal != 0,
            partitions,
            AuthorizedOperationsMarshal.CopyOutAndDestroy(operations),
            id);
    }

    private static List<TopicPartitionInfo> CopyOutPartitions(IntPtr description)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list = NativeMethods.TopicDescriptionPartitions(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<TopicPartitionInfo> partitions = new List<TopicPartitionInfo>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr partition = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                partitions.Add(CopyOutPartition(partition));
            }

            return partitions;
        }
        finally
        {
            DestroyList(list);
        }
    }

    private static TopicPartitionInfo CopyOutPartition(IntPtr partition)
    {
        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.TopicPartitionInfoPartition(partition, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr leader = NativeMethods.TopicPartitionInfoLeader(partition, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Node? leaderNode = NodeMarshal.CopyOut(leader);
        IntPtr replicas = NativeMethods.TopicPartitionInfoReplicas(partition, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<Node> replicaNodes = CopyOutNodes(replicas) ?? new List<Node>();
        IntPtr isr = NativeMethods.TopicPartitionInfoIsr(partition, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<Node> isrNodes = CopyOutNodes(isr) ?? new List<Node>();
        IntPtr elr = NativeMethods.TopicPartitionInfoElr(partition, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<Node>? elrNodes = CopyOutNodes(elr);
        IntPtr lastKnownElr = NativeMethods.TopicPartitionInfoLastKnownElr(partition, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<Node>? lastKnownElrNodes = CopyOutNodes(lastKnownElr);

        // One reported ELR set without the other reads as both absent, as Python's binding does.
        return elrNodes is null || lastKnownElrNodes is null
            ? new TopicPartitionInfo(id, leaderNode, replicaNodes, isrNodes)
            : new TopicPartitionInfo(id, leaderNode, replicaNodes, isrNodes, elrNodes, lastKnownElrNodes);
    }

    private static List<Node>? CopyOutNodes(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            return NodeMarshal.CopyOutList(list);
        }
        finally
        {
            DestroyList(list);
        }
    }

    private static void DestroyList(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListDestroy(list, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
