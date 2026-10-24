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
/// The result of <see cref="IAsyncAdmin.AlterPartitionReassignments"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult</c>: one future per
/// partition.
/// </summary>
/// <remarks>
/// A partition's future completes when its reassignment was initiated, and faults with that
/// partition's error (<c>INVALID_REPLICA_ASSIGNMENT</c>, <c>NO_REASSIGNMENT_IN_PROGRESS</c>, …).
/// </remarks>
/// <seealso cref="AlterPartitionReassignmentsResult"/>
public sealed class AsyncAlterPartitionReassignmentsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl> _nativeFutures;
    private readonly SafeAdminResultHandle _native;

    private AsyncAlterPartitionReassignmentsResult(
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested partition, keyed by partition — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every reassignment was initiated, and faults with the first failure to
    /// complete if any partition failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures));

    internal static AsyncAlterPartitionReassignmentsResult OfClient(
        (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartition, AsyncKafkaFuture> values =
            new Dictionary<TopicPartition, AsyncKafkaFuture>(native.Values.Count);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AsyncAlterPartitionReassignmentsResult(values, nativeFutures, native.Result);
    }
}
