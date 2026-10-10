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
using System.Threading.Tasks;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeReplicaLogDirs"/> — the .NET realization of
/// Java's <c>DescribeReplicaLogDirsResult</c>: one future <b>per replica</b>
/// (<c>DescribeReplicaLogDirsResult.java:41, :48</c>).
/// </summary>
/// <remarks>
/// <para>
/// The key is a <b>3-part</b> <see cref="TopicPartitionReplica"/>, reassembled from the
/// topic, partition id and broker id that each replica's own callback delivers — the
/// widest composite key in M15.
/// </para>
/// <para>
/// The per-replica error is <b>owned</b>: it arrives with that replica's own callback and is
/// freed once it has been copied into the <see cref="KafkaException"/>.
/// </para>
/// </remarks>
/// <seealso cref="DescribeReplicaLogDirsResult"/>
public sealed class AsyncDescribeReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, Task<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> _values;
    private readonly IReadOnlyDictionary<TopicPartitionReplica, AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> _futures;
    private readonly IEqualityComparer<TopicPartitionReplica> _comparer;

    /// <summary>
    /// Wraps the per-replica futures — Java's package-private constructor (<c>:34</c>).
    /// </summary>
    /// <remarks>
    /// The bridge's own comparer is threaded through so the aggregate and the per-key map
    /// cannot disagree about key identity — the same reasoning as
    /// <see cref="AsyncDescribeConfigsResult"/>, and it matters for the same reason:
    /// <see cref="TopicPartitionReplica"/> is a reference type with custom value equality.
    /// </remarks>
    internal AsyncDescribeReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, Task<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values,
        IEqualityComparer<TopicPartitionReplica> comparer)
    {
        _values = values;
        _futures = AsyncAdminFutures.Wrap(values);
        _comparer = comparer;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncDescribeReplicaLogDirsResult From(
        KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> operation) =>
        new AsyncDescribeReplicaLogDirsResult(operation.Tasks, operation.KeyComparer);

    /// <summary>
    /// One future per requested replica — Java's <c>values()</c> (<c>:41</c>).
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> Values => _futures;

    /// <summary>
    /// Every replica's log-directory information in one map, succeeding only if every
    /// replica succeeded — Java's <c>all()</c> (<c>:48</c>).
    /// </summary>
    /// <returns>A future yielding the whole map, or faulting with the first failure.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>(Gather(_values, _comparer));

    private static async Task<IReadOnlyDictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> Gather(
        IReadOnlyDictionary<TopicPartitionReplica, Task<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values,
        IEqualityComparer<TopicPartitionReplica> comparer)
    {
        // WhenAll first, so a failure faults this task with the first failure exactly as
        // Java's `allOf(...).thenApply(...)` chain does.
        await Task.WhenAll(values.Values).ConfigureAwait(false);

        Dictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> infos =
            new Dictionary<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(values.Count, comparer);
        foreach (KeyValuePair<TopicPartitionReplica, Task<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> entry in values)
        {
            infos.Add(entry.Key, entry.Value.Result);
        }

        return infos;
    }
}
