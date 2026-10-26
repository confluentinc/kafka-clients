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

using System.Collections.Generic;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of
/// <see cref="IAsyncAdmin.ListPartitionReassignments(IReadOnlyCollection{TopicPartition}?, ListPartitionReassignmentsOptions?)"/>
/// — Java's <c>org.apache.kafka.clients.admin.ListPartitionReassignmentsResult</c>: one future over
/// the whole listing.
/// </summary>
/// <remarks>
/// Any failure faults this one future. Only partitions with an ongoing reassignment appear in the
/// map, so it can be shorter than the request.
/// </remarks>
/// <seealso cref="ListPartitionReassignmentsResult"/>
public sealed class AsyncListPartitionReassignmentsResult
{
    private readonly KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> _reassignments;
    private readonly SafeAdminResultHandle _native;

    private AsyncListPartitionReassignmentsResult(
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> reassignments,
        SafeAdminResultHandle native)
    {
        _reassignments = reassignments;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// Each partition's ongoing reassignment — Java's <c>reassignments()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the map, or faulting with the call's own
    /// <see cref="KafkaException"/>. Partitions with no ongoing reassignment are absent
    /// rather than present-and-empty.
    /// </returns>
    /// <remarks>Every call returns the same future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> Reassignments() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(_reassignments);

    internal static AsyncListPartitionReassignmentsResult OfClient(
        (KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> Reassignments,
            SafeAdminResultHandle Result) native) =>
        new AsyncListPartitionReassignmentsResult(native.Reassignments, native.Result);
}
