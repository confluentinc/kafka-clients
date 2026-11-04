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
/// The result of <see cref="IAsyncAdmin.DescribeReplicaLogDirs"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult</c>: one future per replica.
/// </summary>
/// <remarks>
/// A replica that fails faults only its own future.
/// </remarks>
/// <seealso cref="DescribeReplicaLogDirsResult"/>
public sealed class AsyncDescribeReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> _futures;
    private readonly IEqualityComparer<TopicPartitionReplica> _comparer;
    private readonly IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> _nativeValues;
    private readonly SafeAdminResultHandle _native;


    private AsyncDescribeReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> nativeValues, SafeAdminResultHandle native)
    {
        IEqualityComparer<TopicPartitionReplica> comparer = EqualityComparer<TopicPartitionReplica>.Default;
        Dictionary<TopicPartitionReplica, AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values =
            new Dictionary<TopicPartitionReplica, AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>(nativeValues.Count, comparer);
        foreach (KeyValuePair<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> entry in nativeValues)
        {
            values.Add(entry.Key, new AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(entry.Value));
        }

        _futures = values;
        _comparer = comparer;
        _nativeValues = nativeValues;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested replica — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> Values => _futures;

    /// <summary>
    /// Every replica's log-directory information in one map, succeeding only if every
    /// replica succeeded — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or faulting with the first failure to complete.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>(KafkaFutureImpl.GatherAll(_nativeValues, _comparer));

    internal static AsyncDescribeReplicaLogDirsResult OfClient(
        (IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> Values, SafeAdminResultHandle Result) native) =>
        new AsyncDescribeReplicaLogDirsResult(native.Values, native.Result);
}
