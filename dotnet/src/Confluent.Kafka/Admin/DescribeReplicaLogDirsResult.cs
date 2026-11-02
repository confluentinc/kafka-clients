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
using System.Globalization;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeReplicaLogDirs"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult</c>: one future per replica.
/// </summary>
/// <remarks>
/// A replica that fails fails only its own future.
/// </remarks>
/// <seealso cref="AsyncDescribeReplicaLogDirsResult"/>
public sealed class DescribeReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> _values;
    private readonly IEqualityComparer<TopicPartitionReplica> _comparer;
    private readonly IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>? _nativeValues;
    private readonly SafeAdminResultHandle? _native;

    internal DescribeReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> values,
        IEqualityComparer<TopicPartitionReplica> comparer)
    {
        _values = values;
        _comparer = comparer;
    }

    private DescribeReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> nativeValues, SafeAdminResultHandle native)
    {
        IEqualityComparer<TopicPartitionReplica> comparer = EqualityComparer<TopicPartitionReplica>.Default;
        Dictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> values =
            new Dictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>>(nativeValues.Count, comparer);
        foreach (KeyValuePair<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> entry in nativeValues)
        {
            values.Add(entry.Key, new KafkaFuture<ReplicaLogDirInfo>(entry.Value));
        }

        _values = values;
        _comparer = comparer;
        _nativeValues = nativeValues;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested replica — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> Values => _values;

    /// <summary>
    /// Every replica's log-directory information in one map, succeeding only if every
    /// replica succeeded — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or failing with the first failure to complete.</returns>
    public KafkaFuture<IReadOnlyDictionary<TopicPartitionReplica, ReplicaLogDirInfo>> All() =>
        _nativeValues is not null
            ? new KafkaFuture<IReadOnlyDictionary<TopicPartitionReplica, ReplicaLogDirInfo>>(KafkaFutureImpl.GatherAll(_nativeValues, _comparer))
            : SyncAdminFutures.GatherAll(_values, _comparer);

    internal static DescribeReplicaLogDirsResult OfClient(
        (IReadOnlyDictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> Values, SafeAdminResultHandle Result) native) =>
        new DescribeReplicaLogDirsResult(native.Values, native.Result);

    /// <summary>
    /// Where one replica's log lives, and where it is being moved to — Java's nested
    /// <c>DescribeReplicaLogDirsResult.ReplicaLogDirInfo</c>.
    /// </summary>
    /// <remarks>Its accessors keep Java's <c>get</c> prefix, a recorded decision.</remarks>
    public sealed class ReplicaLogDirInfo
    {
        internal ReplicaLogDirInfo(
            string? currentReplicaLogDir,
            long currentReplicaOffsetLag,
            string? futureReplicaLogDir,
            long futureReplicaOffsetLag)
        {
            CurrentReplicaLogDir = currentReplicaLogDir;
            CurrentReplicaOffsetLag = currentReplicaOffsetLag;
            FutureReplicaLogDir = futureReplicaLogDir;
            FutureReplicaOffsetLag = futureReplicaOffsetLag;
        }

        private string? CurrentReplicaLogDir { get; }

        private long CurrentReplicaOffsetLag { get; }

        private string? FutureReplicaLogDir { get; }

        private long FutureReplicaOffsetLag { get; }

        /// <summary>
        /// The replica's current log directory on that broker, or <see langword="null"/>
        /// when the broker hosts no replica of that partition — Java's
        /// <c>getCurrentReplicaLogDir()</c>.
        /// </summary>
        /// <returns>The directory, or <see langword="null"/>.</returns>
        public string? GetCurrentReplicaLogDir() => CurrentReplicaLogDir;

        /// <summary>
        /// <c>max(partition high watermark − replica log end offset, 0)</c> — Java's
        /// <c>getCurrentReplicaOffsetLag()</c>.
        /// </summary>
        /// <returns>The lag.</returns>
        public long GetCurrentReplicaOffsetLag() => CurrentReplicaOffsetLag;

        /// <summary>
        /// The directory the replica is being moved to, or <see langword="null"/> when it is
        /// not being moved — Java's <c>getFutureReplicaLogDir()</c>.
        /// </summary>
        /// <returns>The destination directory, or <see langword="null"/>.</returns>
        public string? GetFutureReplicaLogDir() => FutureReplicaLogDir;

        /// <summary>
        /// The future replica's offset lag, or <c>-1</c> when there is no replica or no move
        /// in progress — Java's <c>getFutureReplicaOffsetLag()</c>.
        /// </summary>
        /// <returns>The lag.</returns>
        public long GetFutureReplicaOffsetLag() => FutureReplicaOffsetLag;

        /// <summary>
        /// A diagnostic rendering matching Java's <c>toString()</c>, whose shape depends on
        /// whether a move is in progress.
        /// </summary>
        /// <returns>The rendering.</returns>
        public override string ToString() =>
            FutureReplicaLogDir is not null
                ? string.Format(
                    CultureInfo.InvariantCulture,
                    "(currentReplicaLogDir={0}, futureReplicaLogDir={1}, futureReplicaOffsetLag={2})",
                    CurrentReplicaLogDir,
                    FutureReplicaLogDir,
                    FutureReplicaOffsetLag)
                : string.Format(
                    CultureInfo.InvariantCulture,
                    "ReplicaLogDirInfo(currentReplicaLogDir={0})",
                    CurrentReplicaLogDir);
    }
}
