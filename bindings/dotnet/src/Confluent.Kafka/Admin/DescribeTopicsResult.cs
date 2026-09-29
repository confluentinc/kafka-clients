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
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeTopics"/> — the .NET realization of Java's
/// <c>DescribeTopicsResult</c>: one awaitable <b>per topic</b>, handed back the moment the
/// request is submitted, keyed by whichever identifier the request used.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Exactly one of <see cref="TopicIdValues"/> and <see cref="TopicNameValues"/> is
/// non-null</b> — and, for the same reason, exactly one of
/// <see cref="AllTopicNames"/> / <see cref="AllTopicIds"/> returns a task rather than
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
/// <b>Deviation from Java, recorded.</b> Per-topic <em>granularity</em> is fully
/// preserved, but per-topic <em>timing independence</em> is not — the same recorded
/// limitation as <see cref="CreateTopicsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class DescribeTopicsResult
{
    private readonly IReadOnlyDictionary<Uuid, Task<TopicDescription>>? _topicIdValues;
    private readonly IReadOnlyDictionary<string, Task<TopicDescription>>? _topicNameValues;
    private readonly IEqualityComparer<Uuid> _topicIdComparer;
    private readonly IEqualityComparer<string> _topicNameComparer;

    /// <summary>
    /// Wraps one awaitable per topic, keyed by id <b>or</b> by name — Java's
    /// <c>protected DescribeTopicsResult(Map&lt;Uuid, KafkaFuture&lt;TopicDescription&gt;&gt; topicIdFutures,
    /// Map&lt;String, KafkaFuture&lt;TopicDescription&gt;&gt; nameFutures)</c>
    /// (<c>DescribeTopicsResult.java:37-44</c>, marked <c>// VisibleForTesting</c>), so a
    /// test or a mock can fabricate a result.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Exactly one of the two must be non-null, and that is <b>checked</b> here with
    /// Java's own two messages, verbatim (<c>:38-41</c>).
    /// </para>
    /// <para>
    /// The aggregate a user-built result hands back from <see cref="AllTopicNames"/> /
    /// <see cref="AllTopicIds"/> is keyed with <see cref="StringComparer.Ordinal"/> /
    /// <see cref="EqualityComparer{T}.Default"/> — Java's own key semantics for a
    /// <c>String</c> / <see cref="Uuid"/> <c>HashMap</c>. (A result built by the client
    /// keys it with the bridge's own comparer instead, so the aggregate and
    /// <see cref="TopicNameValues"/> cannot disagree about a key.)
    /// </para>
    /// <para>
    /// <b>Public rather than <c>protected</c>, on a <see langword="sealed"/> type</b>
    /// (M15/P13.2, D5) — see <see cref="CreateTopicsResult(IReadOnlyDictionary{string, Task{TopicMetadataAndConfig}})"/>.
    /// The parameter types are the ones <see cref="TopicIdValues"/> and
    /// <see cref="TopicNameValues"/> publish, and the dictionaries are held by reference, as
    /// Java holds its maps.
    /// </para>
    /// </remarks>
    /// <param name="topicIdFutures">One awaitable per topic id, or <see langword="null"/> for a by-name result.</param>
    /// <param name="nameFutures">One awaitable per topic name, or <see langword="null"/> for a by-id result.</param>
    /// <exception cref="ArgumentException">
    /// Both are non-null (<c>"topicIdFutures and nameFutures cannot both be specified."</c>)
    /// or both are null (<c>"topicIdFutures and nameFutures cannot both be null."</c>) —
    /// Java's <c>IllegalArgumentException</c>, with no parameter name because neither
    /// argument is wrong on its own.
    /// </exception>
    public DescribeTopicsResult(
        IReadOnlyDictionary<Uuid, Task<TopicDescription>>? topicIdFutures,
        IReadOnlyDictionary<string, Task<TopicDescription>>? nameFutures)
        : this(topicIdFutures, EqualityComparer<Uuid>.Default, nameFutures, StringComparer.Ordinal)
    {
    }

    /// <summary>
    /// The one constructor body — the public constructor and the two factories all reach
    /// the exactly-one check through it; only the aggregate's key comparers differ.
    /// </summary>
    private DescribeTopicsResult(
        IReadOnlyDictionary<Uuid, Task<TopicDescription>>? topicIdFutures,
        IEqualityComparer<Uuid> topicIdComparer,
        IReadOnlyDictionary<string, Task<TopicDescription>>? nameFutures,
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
    /// One awaitable per requested topic id, or <see langword="null"/> if the request
    /// used topic <em>names</em> — Java's <c>topicIdValues()</c>.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="DeleteTopicsResult"/> these carry a value: Java publishes
    /// <c>Map&lt;Uuid, KafkaFuture&lt;TopicDescription&gt;&gt;</c>. A topic that failed
    /// faults only its own <see cref="Task"/>.
    /// </remarks>
    public IReadOnlyDictionary<Uuid, Task<TopicDescription>>? TopicIdValues => _topicIdValues;

    /// <summary>
    /// One awaitable per requested topic name, or <see langword="null"/> if the request
    /// used topic <em>ids</em> — Java's <c>topicNameValues()</c>.
    /// </summary>
    /// <inheritdoc cref="TopicIdValues" path="/remarks"/>
    public IReadOnlyDictionary<string, Task<TopicDescription>>? TopicNameValues => _topicNameValues;

    /// <summary>
    /// Every description keyed by topic <b>name</b>, or <see langword="null"/> if the
    /// request used topic ids — Java's <c>allTopicNames()</c>. Succeeds only if every
    /// topic succeeded.
    /// </summary>
    /// <returns>
    /// A task yielding the descriptions, or <see langword="null"/> when the request was
    /// not by name (Java's <c>all(null)</c> short-circuit at
    /// <c>DescribeTopicsResult.java:98</c>).
    /// </returns>
    public Task<IReadOnlyDictionary<string, TopicDescription>>? AllTopicNames() =>
        All(_topicNameValues, _topicNameComparer);

    /// <summary>
    /// Every description keyed by topic <b>id</b>, or <see langword="null"/> if the
    /// request used topic names — Java's <c>allTopicIds()</c>. Succeeds only if every
    /// topic succeeded.
    /// </summary>
    /// <returns>
    /// A task yielding the descriptions, or <see langword="null"/> when the request was
    /// not by id.
    /// </returns>
    public Task<IReadOnlyDictionary<Uuid, TopicDescription>>? AllTopicIds() =>
        All(_topicIdValues, _topicIdComparer);

    /// <summary>
    /// Builds the by-id result — Java's package-private <c>ofTopicIds</c>.
    /// </summary>
    internal static DescribeTopicsResult OfTopicIds(
        IReadOnlyDictionary<Uuid, Task<TopicDescription>> values, IEqualityComparer<Uuid> comparer) =>
        new DescribeTopicsResult(values, comparer, null, StringComparer.Ordinal);

    /// <summary>
    /// Builds the by-name result — Java's package-private <c>ofTopicNames</c>.
    /// </summary>
    internal static DescribeTopicsResult OfTopicNames(
        IReadOnlyDictionary<string, Task<TopicDescription>> values, IEqualityComparer<string> comparer) =>
        new DescribeTopicsResult(null, EqualityComparer<Uuid>.Default, values, comparer);

    /// <summary>
    /// Java's private <c>all(Map)</c> helper (<c>:97-114</c>): <see langword="null"/> in,
    /// <see langword="null"/> out; otherwise wait for every future and gather the results
    /// into one map.
    /// </summary>
    /// <remarks>
    /// The map is built with the comparer the constructor recorded — the bridge's own for a
    /// client-built result, so a lookup in the aggregate and a lookup in
    /// <see cref="TopicNameValues"/> can never disagree about a key; Java's key semantics
    /// for a user-built one.
    /// </remarks>
    private static Task<IReadOnlyDictionary<TKey, TopicDescription>>? All<TKey>(
        IReadOnlyDictionary<TKey, Task<TopicDescription>>? values, IEqualityComparer<TKey> comparer)
        where TKey : notnull
    {
        if (values is null)
        {
            return null;
        }

        return Gather(values, comparer);

        static async Task<IReadOnlyDictionary<TKey, TopicDescription>> Gather(
            IReadOnlyDictionary<TKey, Task<TopicDescription>> values, IEqualityComparer<TKey> comparer)
        {
            // WhenAll first, so a failure faults this task with the first failure exactly
            // as Java's `allOf(...).thenApply(...)` chain does, rather than surfacing
            // whichever key happens to be enumerated first.
            await Task.WhenAll(values.Values).ConfigureAwait(false);

            Dictionary<TKey, TopicDescription> descriptions =
                new Dictionary<TKey, TopicDescription>(values.Count, comparer);
            foreach (KeyValuePair<TKey, Task<TopicDescription>> entry in values)
            {
                descriptions.Add(entry.Key, entry.Value.Result);
            }

            return descriptions;
        }
    }
}
