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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>describeConsumerGroups</c> call — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeConsumerGroupsResult</c>
/// (<c>DescribeConsumerGroupsResult.java:30</c>): one future <b>per group id</b>, handed
/// back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// A group that fails fails only <em>its own</em> <see cref="KafkaFuture{T}"/>; a per-group
/// failure is not a call failure. <see cref="All"/> is the aggregate over those per-group futures
/// and succeeds only if every one of them did.
/// </para>
/// <para>
/// ⚠ <b>Unlike <see cref="DescribeTopicsResult"/>, this one <em>does</em> have an
/// <see cref="All"/></b> — and unlike <see cref="DescribeConfigsResult"/> it has exactly one
/// keyed map, so there is no null-versus-map short-circuit to reproduce: both accessors
/// always answer. The asymmetry between Java's describe results is Java's, not ours to
/// smooth, and a test asserts the shape so a later slice cannot quietly align them.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Java's <c>describedGroups()</c> hands back
/// <c>new HashMap&lt;&gt;(futures)</c> (<c>:42</c>) — a defensive copy, because Java's
/// <c>Map</c> is mutable and a caller could otherwise clear the result's own map.
/// <see cref="DescribedGroups"/> returns the stored instance instead: the declared type is
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/>, which already denies the mutation the
/// copy exists to prevent, so copying per call would buy nothing and allocate on every
/// read. This is the call <see cref="DescribeConfigsResult.Values"/> already makes.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its group (or, for
/// <see cref="All"/>, every group) has completed, and needs no thread-pool thread: the futures here are composed on
/// the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeConsumerGroupsResult"/>
public sealed class DescribeConsumerGroupsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture<ConsumerGroupDescription>> _futures;

    /// <summary>
    /// Wraps one future per group id — Java's <b>public</b>
    /// <c>DescribeConsumerGroupsResult(Map&lt;String, KafkaFuture&lt;ConsumerGroupDescription&gt;&gt;)</c>
    /// (<c>:34</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ Java declares this constructor <b>public</b>, unlike most <c>*Result</c> constructors, which are
    /// package-private. <b>It is internal here on purpose</b>: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionary is held by reference, as Java holds its map.
    /// </para>
    /// <para>
    /// No key comparer is threaded in from the bridge the way
    /// <see cref="DescribeConfigsResult(IReadOnlyDictionary{ConfigResource, KafkaFuture{Config}}, IEqualityComparer{ConfigResource})"/>
    /// does: Java's constructor takes the map alone, so this one does too. See <see cref="All"/> for what that means
    /// for the aggregate.
    /// </para>
    /// </remarks>
    /// <param name="futures">One future per requested group id.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is null.</exception>
    internal DescribeConsumerGroupsResult(IReadOnlyDictionary<string, KafkaFuture<ConsumerGroupDescription>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeConsumerGroupsResult From(KeyedAdminOperation<string, ConsumerGroupDescription> operation) =>
        new DescribeConsumerGroupsResult(SyncAdminFutures.Wrap(operation.SyncSources));

    /// <summary>
    /// One future per requested group id — Java's <c>describedGroups()</c>
    /// (<c>:41</c>).
    /// </summary>
    /// <remarks>
    /// Returns the stored instance rather than Java's defensive copy; see the note on the
    /// class for why.
    /// </remarks>
    public IReadOnlyDictionary<string, KafkaFuture<ConsumerGroupDescription>> DescribedGroups => _futures;

    /// <summary>
    /// Every group's description in one map, succeeding only if every group succeeded —
    /// Java's <c>all()</c> (<c>:48</c>).
    /// </summary>
    /// <returns>
    /// A future yielding the whole map, or failing with the first failure in key order. Its <c>Get()</c> waits for
    /// every group.
    /// </returns>
    /// <remarks>
    /// The aggregate is built with <see cref="StringComparer.Ordinal"/> — what the bridge
    /// keys group ids by, and how Kafka compares them. Ordinal is the <em>strictest</em>
    /// choice, so keys already distinct in <see cref="DescribedGroups"/> stay distinct here
    /// and the gather can never collide; a caller who built the map with a looser comparer
    /// keeps every entry but should look it up the same way it was keyed. Every group is waited for first, so a
    /// failure fails this future with the first failure in key order, exactly as Java's
    /// <c>allOf(...).thenApply(...)</c> chain does; the map is built once per call to this method, on the first
    /// <c>Get()</c>'s thread.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<string, ConsumerGroupDescription>> All() =>
        SyncAdminFutures.GatherAll(_futures, StringComparer.Ordinal);
}
