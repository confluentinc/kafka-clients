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
/// The result of
/// <see cref="IAdmin.DescribeTopics(TopicCollection, DescribeTopicsOptions?)"/> — the .NET
/// realization of Java's
/// <c>DescribeTopicsResult</c>: one future <b>per topic</b>, handed back the moment the
/// request is submitted, keyed by whichever identifier the request used.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Exactly one of <see cref="TopicIdValues"/> and <see cref="TopicNameValues"/> is
/// non-null</b> — and, for the same reason, exactly one of
/// <see cref="AllTopicNames"/> / <see cref="AllTopicIds"/> returns a future rather than
/// <see langword="null"/>. Java's javadoc says so twice
/// (<c>DescribeTopicsResult.java:54-72</c>), and its private aggregate helper opens with
/// <c>if (futures == null) return null;</c> (<c>:98</c>), so the mismatched aggregate is
/// <see langword="null"/> too — not an empty map and not a throw. See the identical note
/// on <see cref="DeleteTopicsResult"/> for why smoothing it would break a caller ported
/// from Java.
/// </para>
/// <para>
/// ⚠ <b>There is deliberately no <c>All()</c> here.</b> Java's two topic results are
/// asymmetric: <see cref="DeleteTopicsResult.All"/> exists and has no typed aggregate,
/// while this one has <see cref="AllTopicNames"/> / <see cref="AllTopicIds"/> and no
/// <c>all()</c>. The asymmetry is Java's, not ours to smooth; a test asserts both
/// absences, because an absence nothing asserts is one a later phase "completes" without
/// anything going red.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its topic (or, for an aggregate,
/// every topic) has completed, and needs no thread-pool thread: the futures here are composed on the waiting thread,
/// never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeTopicsResult"/>
public sealed class DescribeTopicsResult
{
    private readonly IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>? _topicIdValues;
    private readonly IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>? _topicNameValues;
    private readonly IEqualityComparer<Uuid> _topicIdComparer;
    private readonly IEqualityComparer<string> _topicNameComparer;

    /// <summary>
    /// Wraps one future per topic, keyed by id <b>or</b> by name — Java's
    /// <c>protected DescribeTopicsResult(Map&lt;Uuid, KafkaFuture&lt;TopicDescription&gt;&gt; topicIdFutures,
    /// Map&lt;String, KafkaFuture&lt;TopicDescription&gt;&gt; nameFutures)</c>
    /// (<c>DescribeTopicsResult.java:37-44</c>, marked <c>// VisibleForTesting</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Exactly one of the two must be non-null, and that is <b>checked</b> here with
    /// Java's own two messages, verbatim (<c>:38-41</c>).
    /// </para>
    /// <para>
    /// The aggregate a result built here hands back from <see cref="AllTopicNames"/> /
    /// <see cref="AllTopicIds"/> is keyed with <see cref="StringComparer.Ordinal"/> /
    /// <see cref="EqualityComparer{T}.Default"/> — Java's own key semantics for a
    /// <c>String</c> / <see cref="Uuid"/> <c>HashMap</c>. Its handling of an equal key is
    /// Java's too: the aggregate is filled with <c>HashMap.put</c> semantics
    /// (<c>DescribeTopicsResult.java:105</c>), so where the supplied dictionary's comparer
    /// is finer than that — reference equality holding two distinct instances of one name,
    /// say — the entries collapse to one and the last-enumerated value wins, rather than
    /// failing. (A result built by the client keys it with the bridge's own comparer
    /// instead, so the aggregate and <see cref="TopicNameValues"/> cannot disagree about a
    /// key.)
    /// </para>
    /// <para>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionaries are held by reference, as Java holds its maps.
    /// </para>
    /// </remarks>
    /// <param name="topicIdFutures">One future per topic id, or <see langword="null"/> for a by-name result.</param>
    /// <param name="nameFutures">One future per topic name, or <see langword="null"/> for a by-id result.</param>
    /// <exception cref="ArgumentException">
    /// Both are non-null (<c>"topicIdFutures and nameFutures cannot both be specified."</c>)
    /// or both are null (<c>"topicIdFutures and nameFutures cannot both be null."</c>) —
    /// Java's <c>IllegalArgumentException</c>, with no parameter name because neither
    /// argument is wrong on its own.
    /// </exception>
    internal DescribeTopicsResult(
        IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>? topicIdFutures,
        IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>? nameFutures)
        : this(topicIdFutures, EqualityComparer<Uuid>.Default, nameFutures, StringComparer.Ordinal)
    {
    }

    /// <summary>
    /// The one constructor body — the internal constructor and the two factories all reach
    /// the exactly-one check through it; only the aggregate's key comparers differ.
    /// </summary>
    private DescribeTopicsResult(
        IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>? topicIdFutures,
        IEqualityComparer<Uuid> topicIdComparer,
        IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>? nameFutures,
        IEqualityComparer<string> topicNameComparer)
    {
        if (topicIdFutures is not null && nameFutures is not null)
        {
            throw new ArgumentException("topicIdFutures and nameFutures cannot both be specified.");
        }

        if (topicIdFutures is null && nameFutures is null)
        {
            throw new ArgumentException("topicIdFutures and nameFutures cannot both be null.");
        }

        _topicIdValues = topicIdFutures;
        _topicIdComparer = topicIdComparer;
        _topicNameValues = nameFutures;
        _topicNameComparer = topicNameComparer;
    }

    /// <summary>
    /// One future per requested topic id, or <see langword="null"/> if the request
    /// used topic <em>names</em> — Java's <c>topicIdValues()</c>.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="DeleteTopicsResult"/> these carry a value: Java publishes
    /// <c>Map&lt;Uuid, KafkaFuture&lt;TopicDescription&gt;&gt;</c>. A topic that failed
    /// fails only its own <see cref="KafkaFuture{T}"/>.
    /// </remarks>
    public IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>? TopicIdValues => _topicIdValues;

    /// <summary>
    /// One future per requested topic name, or <see langword="null"/> if the request
    /// used topic <em>ids</em> — Java's <c>topicNameValues()</c>.
    /// </summary>
    /// <inheritdoc cref="TopicIdValues" path="/remarks"/>
    public IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>? TopicNameValues => _topicNameValues;

    /// <summary>
    /// Every description keyed by topic <b>name</b>, or <see langword="null"/> if the
    /// request used topic ids — Java's <c>allTopicNames()</c>. Succeeds only if every
    /// topic succeeded, and otherwise fails with the first failure in key order.
    /// </summary>
    /// <returns>
    /// A future yielding the descriptions, or <see langword="null"/> when the request was
    /// not by name (Java's <c>all(null)</c> short-circuit at
    /// <c>DescribeTopicsResult.java:98</c>).
    /// </returns>
    public KafkaFuture<IReadOnlyDictionary<string, TopicDescription>>? AllTopicNames() =>
        All(_topicNameValues, _topicNameComparer);

    /// <summary>
    /// Every description keyed by topic <b>id</b>, or <see langword="null"/> if the
    /// request used topic names — Java's <c>allTopicIds()</c>. Succeeds only if every
    /// topic succeeded, and otherwise fails with the first failure in key order.
    /// </summary>
    /// <returns>
    /// A future yielding the descriptions, or <see langword="null"/> when the request was
    /// not by id.
    /// </returns>
    public KafkaFuture<IReadOnlyDictionary<Uuid, TopicDescription>>? AllTopicIds() =>
        All(_topicIdValues, _topicIdComparer);

    /// <summary>
    /// Builds the by-id result — Java's package-private <c>ofTopicIds</c>.
    /// </summary>
    internal static DescribeTopicsResult OfTopicIds(
        IReadOnlyDictionary<Uuid, SyncFutureSource<TopicDescription>> values, IEqualityComparer<Uuid> comparer) =>
        new DescribeTopicsResult(SyncAdminFutures.Wrap(values), comparer, null, StringComparer.Ordinal);

    /// <summary>
    /// Builds the by-name result — Java's package-private <c>ofTopicNames</c>.
    /// </summary>
    internal static DescribeTopicsResult OfTopicNames(
        IReadOnlyDictionary<string, SyncFutureSource<TopicDescription>> values, IEqualityComparer<string> comparer) =>
        new DescribeTopicsResult(null, EqualityComparer<Uuid>.Default, SyncAdminFutures.Wrap(values), comparer);

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeTopicsResult From(
        (KeyedAdminOperation<string, TopicDescription>? ByName, KeyedAdminOperation<Uuid, TopicDescription>? ById) operation) =>
        operation.ByName is not null
            ? OfTopicNames(operation.ByName.SyncSources, operation.ByName.KeyComparer)
            : OfTopicIds(operation.ById!.SyncSources, operation.ById.KeyComparer);

    /// <summary>
    /// Java's private <c>all(Map)</c> helper (<c>:97-114</c>): <see langword="null"/> in,
    /// <see langword="null"/> out; otherwise wait for every future and gather the results
    /// into one map.
    /// </summary>
    /// <remarks>
    /// Every future is waited for first, so a failure fails the aggregate with the first failure in key order exactly
    /// as Java's <c>allOf(...).thenApply(...)</c> chain does. The map is built with the comparer the constructor
    /// recorded — the bridge's own for a client-built result, so a lookup in the aggregate and a lookup in
    /// <see cref="TopicNameValues"/> can never disagree about a key — and an equal key replaces the earlier one, as
    /// Java's <c>descriptions.put(...)</c> (<c>:105</c>) does, rather than throwing.
    /// </remarks>
    private static KafkaFuture<IReadOnlyDictionary<TKey, TopicDescription>>? All<TKey>(
        IReadOnlyDictionary<TKey, KafkaFuture<TopicDescription>>? values, IEqualityComparer<TKey> comparer)
        where TKey : notnull =>
        values is null ? null : SyncAdminFutures.GatherAll(values, comparer);
}
