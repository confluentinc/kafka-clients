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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.AlterReplicaLogDirs"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterReplicaLogDirsResult</c>: one future per replica.
/// </summary>
/// <remarks>
/// A replica's future completes when its move was accepted, and faults with that replica's
/// error: authorization, an over-long topic name, an unknown log directory, a replica that does
/// not exist on the broker, a disk error.
/// </remarks>
/// <seealso cref="AlterReplicaLogDirsResult"/>
public sealed class AsyncAlterReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, AsyncKafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl> _nativeFutures;
    private readonly SafeAdminResultHandle _native;


    private AsyncAlterReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, AsyncKafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested replica — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every replica move has been accepted, and faults with the first failure
    /// to complete if any was rejected — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() =>
        new AsyncKafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures));

    internal static AsyncAlterReplicaLogDirsResult OfClient(
        (IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartitionReplica, AsyncKafkaFuture> values =
            new Dictionary<TopicPartitionReplica, AsyncKafkaFuture>(native.Values.Count);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<TopicPartitionReplica, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AsyncAlterReplicaLogDirsResult(values, nativeFutures, native.Result);
    }
}
