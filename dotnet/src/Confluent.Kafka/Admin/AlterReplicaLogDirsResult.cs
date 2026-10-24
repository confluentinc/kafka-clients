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
/// The result of <see cref="IAdmin.AlterReplicaLogDirs"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterReplicaLogDirsResult</c>: one future per replica.
/// </summary>
/// <remarks>
/// A replica's future completes when its move was accepted, and fails with that replica's
/// error: authorization, an over-long topic name, an unknown log directory, a replica that does
/// not exist on the broker, a disk error.
/// </remarks>
/// <seealso cref="AsyncAlterReplicaLogDirsResult"/>
public sealed class AlterReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    // The dictionary is held by reference, as Java holds its map.
    internal AlterReplicaLogDirsResult(IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture> values)
    {
        _values = values;
    }

    private AlterReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested replica — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every replica move has been accepted, and fails with the first failure
    /// to complete if any was rejected — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every replica.</returns>
    public KafkaFuture All() =>
        _nativeFutures is not null
            ? new KafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures))
            : SyncAdminFutures.AllOf(_values.Values);

    internal static AlterReplicaLogDirsResult OfClient(
        (IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartitionReplica, KafkaFuture> values =
            new Dictionary<TopicPartitionReplica, KafkaFuture>(native.Values.Count);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<TopicPartitionReplica, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new KafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AlterReplicaLogDirsResult(values, nativeFutures, native.Result);
    }
}
