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
/// Copies a borrowed <c>kafka_admin_TopicDescription_t</c> — and the
/// <c>TopicPartitionInfo</c> / <c>Node</c> tree hanging off it — into fully owned managed
/// objects, so nothing survives the <c>DescribeTopicsResult_destroy</c> that follows the
/// walk (ffi §B2 Category 4 / §B4).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Three <c>has_*</c> discriminants are read here, and each one is load-bearing.</b>
/// <c>has_authorized_operations</c>, <c>has_elr</c> and <c>has_last_known_elr</c> each
/// separate "the broker reported nothing" (Java <see langword="null"/>) from "reported,
/// and the set is empty" — a distinction the counts cannot carry, since both are 0. The
/// ABI added them for exactly that reason, so deriving nullability from
/// <c>count == 0</c> instead would collapse the two and be invisible to any test that
/// only checks emptiness.
/// </para>
/// <para>
/// The leader / replica / ISR / ELR nodes reuse <see cref="NodeMarshal"/>: the admin ABI
/// hands back the same <c>kafka_common_Node_t</c> the consumer surface already copies out,
/// so there is no second node marshaller and no second <see cref="Node"/> type.
/// </para>
/// </remarks>
internal static class TopicDescriptionMarshal
{
    /// <summary>
    /// Copies one borrowed description out.
    /// </summary>
    /// <param name="description">
    /// The borrowed <c>get_value(i)</c> pointer. Valid only until the result root is
    /// destroyed.
    /// </param>
    /// <returns>The owned description.</returns>
    internal static TopicDescription CopyOut(IntPtr description)
    {
        // NUL-terminated, borrowed (ffi §B3 row 2) — copied out here.
        string name = Utf8Marshal.PtrToString(NativeMethods.TopicDescriptionName(description)) ?? string.Empty;

        string topicIdText =
            Utf8Marshal.PtrToString(NativeMethods.TopicDescriptionTopicId(description)) ?? string.Empty;

        // Same reading as TopicMetadataAndConfigMarshal: an empty id is the
        // metadata-unavailable spelling and becomes Uuid.Zero (Java's ZERO_UUID default),
        // while a genuinely malformed one throws out of Parse and faults just this key
        // through KeyedResultMarshal's per-key catch.
        Uuid topicId = topicIdText.Length == 0 ? Uuid.Zero : Uuid.Parse(topicIdText);

        bool isInternal = NativeMethods.TopicDescriptionIsInternal(description);

        int partitionCount = NativeMethods.TopicDescriptionPartitionCount(description);
        List<TopicPartitionInfo> partitions = new List<TopicPartitionInfo>(Math.Max(partitionCount, 0));
        for (int index = 0; index < partitionCount; index++)
        {
            IntPtr partition = NativeMethods.TopicDescriptionPartition(description, index);
            if (partition == IntPtr.Zero)
            {
                // Guarded by `partition_count`, so unreachable; skipping is the safe reading.
                continue;
            }

            partitions.Add(CopyOutPartition(partition));
        }

        return new TopicDescription(name, isInternal, partitions, CopyOutAuthorizedOperations(description), topicId);
    }

    /// <summary>
    /// Java's <c>authorizedOperations()</c>: <see langword="null"/> when the broker
    /// reported no set at all, an (possibly empty) owned collection when it did.
    /// </summary>
    private static IReadOnlyCollection<AclOperation>? CopyOutAuthorizedOperations(IntPtr description)
    {
        // ⚠ The discriminant, NOT the count — see the class remarks.
        if (!NativeMethods.TopicDescriptionHasAuthorizedOperations(description))
        {
            return null;
        }

        int count = NativeMethods.TopicDescriptionAuthorizedOperationCount(description);
        List<AclOperation> operations = new List<AclOperation>(Math.Max(count, 0));
        for (int index = 0; index < count; index++)
        {
            // The ABI hands back the Kafka wire code, and AclOperation's members ARE those
            // codes. An unrecognised code becomes Unknown, mirroring Java's
            // `AclOperation.fromCode` (AclOperation.java:151-157) rather than producing an
            // enum value with no name.
            int code = NativeMethods.TopicDescriptionAuthorizedOperation(description, index);
            operations.Add(FromCode(code));
        }

        return operations;
    }

    /// <summary>
    /// Java's <c>AclOperation.fromCode</c>: a code with no matching member becomes
    /// <see cref="AclOperation.Unknown"/> rather than an unnamed enum value. Also absorbs
    /// the ABI's own <c>-1</c> out-of-range return.
    /// </summary>
    private static AclOperation FromCode(int code) =>
        code >= (int)AclOperation.Unknown && code <= (int)AclOperation.TwoPhaseCommit
            ? (AclOperation)code
            : AclOperation.Unknown;

    private static TopicPartitionInfo CopyOutPartition(IntPtr partition)
    {
        int id = NativeMethods.TopicPartitionInfoPartition(partition);

        // Borrowed node → owned Node (or null when the partition has no leader).
        Node? leader = NodeMarshal.CopyOut(NativeMethods.TopicPartitionInfoLeader(partition));

        List<Node> replicas = CopyOutNodes(
            partition,
            NativeMethods.TopicPartitionInfoReplicaCount(partition),
            NativeMethods.TopicPartitionInfoReplica);

        List<Node> inSyncReplicas = CopyOutNodes(
            partition,
            NativeMethods.TopicPartitionInfoIsrCount(partition),
            NativeMethods.TopicPartitionInfoIsr);

        // ⚠ The discriminants, NOT the counts — see the class remarks.
        List<Node>? elr = NativeMethods.TopicPartitionInfoHasElr(partition)
            ? CopyOutNodes(
                partition,
                NativeMethods.TopicPartitionInfoElrCount(partition),
                NativeMethods.TopicPartitionInfoElr)
            : null;

        List<Node>? lastKnownElr = NativeMethods.TopicPartitionInfoHasLastKnownElr(partition)
            ? CopyOutNodes(
                partition,
                NativeMethods.TopicPartitionInfoLastKnownElrCount(partition),
                NativeMethods.TopicPartitionInfoLastKnownElr)
            : null;

        return new TopicPartitionInfo(id, leader, replicas, inSyncReplicas, elr, lastKnownElr);
    }

    private static List<Node> CopyOutNodes(
        IntPtr partition, int count, Func<IntPtr, int, IntPtr> accessor)
    {
        List<Node> nodes = new List<Node>(Math.Max(count, 0));
        for (int index = 0; index < count; index++)
        {
            Node? node = NodeMarshal.CopyOut(accessor(partition, index));
            if (node is null)
            {
                // Guarded by the count, so unreachable; skipping is the safe reading.
                continue;
            }

            nodes.Add(node);
        }

        return nodes;
    }
}
