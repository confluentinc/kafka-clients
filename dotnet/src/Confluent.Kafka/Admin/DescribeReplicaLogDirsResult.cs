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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeReplicaLogDirs"/> — the .NET realization of
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
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its replica (or, for
/// <see cref="All"/>, every replica) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeReplicaLogDirsResult"/>
public sealed class DescribeReplicaLogDirsResult
{
    private readonly IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> _values;
    private readonly IEqualityComparer<TopicPartitionReplica> _comparer;

    /// <summary>
    /// Wraps the per-replica futures — Java's package-private constructor (<c>:34</c>).
    /// </summary>
    /// <remarks>
    /// The bridge's own comparer is threaded through so the aggregate and the per-key map
    /// cannot disagree about key identity — the same reasoning as
    /// <see cref="DescribeConfigsResult"/>, and it matters for the same reason:
    /// <see cref="TopicPartitionReplica"/> is a reference type with custom value equality.
    /// </remarks>
    internal DescribeReplicaLogDirsResult(
        IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> values,
        IEqualityComparer<TopicPartitionReplica> comparer)
    {
        _values = values;
        _comparer = comparer;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeReplicaLogDirsResult From(
        KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> operation) =>
        new DescribeReplicaLogDirsResult(SyncAdminFutures.Wrap(operation.SyncSources), operation.KeyComparer);

    /// <summary>
    /// One future per requested replica — Java's <c>values()</c> (<c>:41</c>).
    /// </summary>
    public IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> Values => _values;

    /// <summary>
    /// Every replica's log-directory information in one map, succeeding only if every
    /// replica succeeded — Java's <c>all()</c> (<c>:48</c>).
    /// </summary>
    /// <returns>A future yielding the whole map, or failing with the first failure in key order.</returns>
    /// <remarks>
    /// Every future is waited for first, so a failure fails the aggregate with the first failure in key order exactly
    /// as Java's <c>allOf(...).thenApply(...)</c> chain does. The map is keyed with the bridge's own comparer (see the
    /// constructor). Each call builds a new aggregate.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<TopicPartitionReplica, ReplicaLogDirInfo>> All() =>
        SyncAdminFutures.GatherAll(_values, _comparer);

    /// <summary>
    /// Where one replica's log lives, and where it is being moved to — the .NET realization
    /// of Java's nested <c>DescribeReplicaLogDirsResult.ReplicaLogDirInfo</c>
    /// (<c>DescribeReplicaLogDirsResult.java:64-132</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Nested, matching Java (decision D17).</b> Nothing forces it out: the enclosing
    /// type has no member named <c>ReplicaLogDirInfo</c>, so there is no <c>CS0102</c>
    /// collision of the kind that moved <see cref="AlterConfigOpType"/> out of
    /// <see cref="AlterConfigOp"/>.
    /// </para>
    /// <para>
    /// ⚠ <b>The <c>Get*</c> prefixes are kept (D17).</b> Java names them
    /// <c>getCurrentReplicaLogDir()</c> and friends rather than the
    /// <c>currentReplicaLogDir()</c> style the rest of the admin API uses, and D17 preserves
    /// that rather than silently normalising it. The deviation is held to these four by
    /// <c>PublicAdminLogDirsShapeParityTests.TheBeanAccessorStyle_IsConfinedToReplicaLogDirInfo</c>,
    /// which sweeps the exported surface — so the claim is enforced rather than narrated.
    /// </para>
    /// <para>
    /// ⚠ <b>Both log-directory accessors are genuinely nullable.</b> Java says so:
    /// <c>getCurrentReplicaLogDir()</c> is "null if no replica is not found for this
    /// partition on the given broker" (<c>:86-88</c>), and
    /// <c>getFutureReplicaLogDir()</c> is "null if the replica of this partition is not
    /// being moved" (<c>:101-103</c>) — the ordinary case for a replica at rest. A
    /// <see langword="null"/> must be preserved, never substituted with <c>""</c>.
    /// </para>
    /// </remarks>
    public sealed class ReplicaLogDirInfo
    {
        /// <summary>
        /// Initializes the info — Java's package-private 4-argument constructor
        /// (<c>:75</c>), so likewise not public here.
        /// </summary>
        /// <param name="currentReplicaLogDir">The current log directory, or <see langword="null"/>.</param>
        /// <param name="currentReplicaOffsetLag">The current replica's offset lag.</param>
        /// <param name="futureReplicaLogDir">The destination log directory, or <see langword="null"/>.</param>
        /// <param name="futureReplicaOffsetLag">The future replica's offset lag.</param>
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
        /// <c>getCurrentReplicaLogDir()</c> (<c>:89</c>).
        /// </summary>
        /// <returns>The directory, or <see langword="null"/>.</returns>
        public string? GetCurrentReplicaLogDir() => CurrentReplicaLogDir;

        /// <summary>
        /// <c>max(partition high watermark − replica log end offset, 0)</c> — Java's
        /// <c>getCurrentReplicaOffsetLag()</c> (<c>:96</c>).
        /// </summary>
        /// <returns>The lag.</returns>
        public long GetCurrentReplicaOffsetLag() => CurrentReplicaOffsetLag;

        /// <summary>
        /// The directory the replica is being moved to, or <see langword="null"/> when it is
        /// not being moved — Java's <c>getFutureReplicaLogDir()</c> (<c>:104</c>).
        /// </summary>
        /// <returns>The destination directory, or <see langword="null"/>.</returns>
        public string? GetFutureReplicaLogDir() => FutureReplicaLogDir;

        /// <summary>
        /// The future replica's offset lag, or <c>-1</c> when there is no replica or no move
        /// in progress — Java's <c>getFutureReplicaOffsetLag()</c> (<c>:112</c>).
        /// </summary>
        /// <returns>The lag.</returns>
        /// <remarks>
        /// ⚠ Java documents <c>-1</c> here as a <b>value</b> the accessor returns, not as an
        /// "absent" marker to be mapped away — unlike
        /// <see cref="LogDirDescription.TotalBytes"/>, whose <c>-1</c> <em>is</em> Java's
        /// empty <c>OptionalLong</c>. The two conventions sit in one stage; the header and
        /// Java's javadoc decide which applies per accessor.
        /// </remarks>
        public long GetFutureReplicaOffsetLag() => FutureReplicaOffsetLag;

        /// <summary>
        /// A diagnostic rendering matching Java's <c>toString()</c> (<c>:116</c>), which
        /// takes a <b>different shape</b> depending on whether a move is in progress.
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
