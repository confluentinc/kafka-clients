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
using System.Globalization;
using System.Linq;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>listConsumerGroupOffsets</c> call — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult</c>
/// (<c>ListConsumerGroupOffsetsResult.java:36</c>): one future <b>per group id</b>,
/// each yielding that group's committed offsets keyed by partition.
/// </summary>
/// <remarks>
/// <para>
/// A group that fails fails only <em>its own</em> <see cref="KafkaFuture{T}"/>; a per-group
/// failure is not a call failure. <see cref="All"/> is the aggregate over those per-group futures
/// and succeeds only if every one of them did.
/// </para>
/// <para>
/// ⚠ <b>This result publishes no per-key map</b>, unlike
/// <see cref="DescribeConsumerGroupsResult"/> and <see cref="DescribeClassicGroupsResult"/>.
/// Java's <c>futures</c> field is package-private with no getter (<c>:38</c>); the only ways
/// in are the two <c>partitionsToOffsetAndMetadata</c> forms and <c>all()</c>. Those three
/// are mirrored as three methods, and no map property is invented to sit beside them.
/// </para>
/// <para>
/// ⚠ <b>The two <c>PartitionsToOffsetAndMetadata</c> forms are real overloads, not one
/// method with a defaulted argument.</b> Collapsing them would erase the no-argument form's
/// own contract: it is valid <em>only</em> when the result holds exactly one group, and
/// throws otherwise (<c>:50-53</c>). An optional parameter cannot express "the argument may
/// be omitted only when the receiver has a particular shape", so both forms ship.
/// </para>
/// <para>
/// ⚠ <b>A partition present with a <see langword="null"/> value is not the same as a
/// partition that is absent.</b> Java's javadoc is explicit (<c>:47</c>, <c>:59-60</c>):
/// when the group has no committed offset for a listed partition, the partition still
/// appears in the map and its value is <c>null</c>. The C ABI carries the distinction as a
/// per-partition <c>has_offset</c> flag, so the value type is
/// <c>OffsetAndMetadata?</c> rather than a partition silently dropped from the map — the
/// same absent-is-not-empty discipline
/// <see cref="ConsumerGroupDescription.AuthorizedOperations"/> and
/// <see cref="ListConsumerGroupOffsetsSpec.TopicPartitions"/> follow.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its group (or, for
/// <see cref="All"/>, every group) has completed, and needs no thread-pool thread: the futures here are composed on
/// the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncListConsumerGroupOffsetsResult"/>
public sealed class ListConsumerGroupOffsetsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> _futures;

    /// <summary>
    /// Wraps one future per group id — Java's <b>package-private</b>
    /// <c>ListConsumerGroupOffsetsResult(Map&lt;CoordinatorKey, KafkaFuture&lt;...&gt;&gt;)</c>
    /// (<c>:40</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ Java's constructor is package-private and is mirrored as <see langword="internal"/>
    /// rather than widened — unlike <see cref="DescribeClassicGroupsResult"/>, whose Java
    /// constructor really is public (and is internal here only as a recorded deviation, FU-5). So the bridge is the only thing that can build one, and
    /// the map is a bridge invariant rather than caller input: no null guard is written, the
    /// same call <see cref="CreateTopicsResult"/> and <see cref="DescribeConfigsResult"/>
    /// already make for their own internal constructors.
    /// </para>
    /// <para>
    /// Java also unwraps <c>CoordinatorKey</c> to its <c>idValue</c> here (<c>:41-42</c>);
    /// that type is an internal admin key with no .NET counterpart, so the map arrives
    /// already keyed by group id.
    /// </para>
    /// </remarks>
    /// <param name="futures">One future per requested group id.</param>
    internal ListConsumerGroupOffsetsResult(
        IReadOnlyDictionary<string, KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures)
    {
        _futures = futures;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// A view over the operation's own latches, keyed with its own (ordinal) comparer, so a group's future is equal
    /// on every read.
    /// </remarks>
    internal static ListConsumerGroupOffsetsResult From(
        KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> operation) =>
        new ListConsumerGroupOffsetsResult(SyncAdminFutures.Wrap(operation.SyncSources));

    /// <summary>
    /// The single requested group's committed offsets keyed by partition — Java's no-argument
    /// <c>partitionsToOffsetAndMetadata()</c> (<c>:49</c>).
    /// </summary>
    /// <returns>A future yielding that group's committed offsets.</returns>
    /// <exception cref="InvalidOperationException">
    /// Offsets for more than one group — or for no group at all — were requested, so there is
    /// no single group for this form to mean. Java throws <c>IllegalStateException</c> on the
    /// same condition (<c>:50-53</c>).
    /// </exception>
    /// <remarks>
    /// A partition the group has no committed offset for is present with a
    /// <see langword="null"/> value rather than absent; see the note on the class.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> PartitionsToOffsetAndMetadata()
    {
        // Java's guard is `futures.size() != 1`, so an EMPTY result throws here too — the
        // message names only the multiple-group case, and the text is Java's verbatim.
        if (_futures.Count != 1)
        {
            throw new InvalidOperationException(
                "Offsets from multiple consumer groups were requested. " +
                "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.");
        }

        // Java's `futures.values().iterator().next()` (:54).
        return _futures.Values.First();
    }

    /// <summary>
    /// One group's committed offsets keyed by partition — Java's
    /// <c>partitionsToOffsetAndMetadata(String)</c> (<c>:62</c>).
    /// </summary>
    /// <param name="groupId">A group id from the request this result came from.</param>
    /// <returns>A future yielding that group's committed offsets.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupId"/> was not part of this request. Java throws
    /// <c>IllegalArgumentException</c> on the same condition (<c>:63-64</c>).
    /// </exception>
    /// <remarks>
    /// Argument validation is synchronous, so a caller mistake surfaces at the call site rather than only when the
    /// returned future's <c>Get()</c> is called. That matches Java, which throws out of the method: a usage error is
    /// not an operation outcome.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> PartitionsToOffsetAndMetadata(string groupId)
    {
        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

        if (!_futures.TryGetValue(
                groupId, out KafkaFuture<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> future))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Offsets for consumer group '{0}' were not requested.",
                    groupId),
                nameof(groupId));
        }

        return future;
    }

    /// <summary>
    /// Every group's committed offsets in one map, succeeding only if every group succeeded —
    /// Java's <c>all()</c> (<c>:72</c>).
    /// </summary>
    /// <returns>
    /// A future yielding the whole map, or failing with the first failure in key order. Its <c>Get()</c> waits for
    /// every group.
    /// </returns>
    /// <remarks>
    /// The aggregate is built with <see cref="StringComparer.Ordinal"/> — what the bridge keys
    /// group ids by, and how Kafka compares them. Every group is waited for first, so a failure fails this future with
    /// the first failure in key order, exactly as Java's <c>allOf(...).thenApply(...)</c> chain does. Each call starts
    /// a fresh gather and hands back a new future, which is why this is a method and not a property; within one such
    /// future the map is built once, on the first <c>Get()</c>'s thread.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> All() =>
        SyncAdminFutures.GatherAll(_futures, StringComparer.Ordinal);
}
