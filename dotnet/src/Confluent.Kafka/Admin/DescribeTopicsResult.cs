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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeTopics(TopicCollection, DescribeTopicsOptions?)"/> —
/// Java's <c>org.apache.kafka.clients.admin.DescribeTopicsResult</c>: one future per topic, keyed
/// by whichever identifier the request used.
/// </summary>
/// <remarks>
/// Exactly one of <see cref="TopicIdValues"/> and <see cref="TopicNameValues"/> is non-null, and
/// exactly one of <see cref="AllTopicNames"/> and <see cref="AllTopicIds"/> returns a future; the
/// other returns <see langword="null"/>, as in Java. As in Java, there is no <c>All()</c>.
/// </remarks>
/// <seealso cref="AsyncDescribeTopicsResult"/>
public sealed class DescribeTopicsResult
{
    private readonly IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>? _topicIdValues;
    private readonly IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>? _topicNameValues;
    private readonly IEqualityComparer<Uuid> _topicIdComparer;
    private readonly IEqualityComparer<string> _topicNameComparer;
    private readonly IReadOnlyDictionary<Uuid, KafkaFutureImpl<TopicDescription>>? _nativeById;
    private readonly IReadOnlyDictionary<string, KafkaFutureImpl<TopicDescription>>? _nativeByName;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's protected constructor: a recorded deviation. Exactly one of the two
    // must be non-null, checked with Java's two messages; the dictionaries are held by reference,
    // as Java holds its maps. The aggregates key with Java's key semantics and keep the last of two
    // equal keys, as HashMap.put does.
    internal DescribeTopicsResult(
        IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>? topicIdFutures,
        IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>? nameFutures)
        : this(topicIdFutures, EqualityComparer<Uuid>.Default, nameFutures, StringComparer.Ordinal)
    {
    }

    private DescribeTopicsResult(
        IReadOnlyDictionary<string, KafkaFutureImpl<TopicDescription>>? byName,
        IReadOnlyDictionary<Uuid, KafkaFutureImpl<TopicDescription>>? byId,
        SafeAdminResultHandle native)
        : this(
            byId is null ? null : Views(byId, EqualityComparer<Uuid>.Default),
            EqualityComparer<Uuid>.Default,
            byName is null ? null : Views(byName, StringComparer.Ordinal),
            StringComparer.Ordinal)
    {
        _nativeById = byId;
        _nativeByName = byName;
        _native = native;
    }

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
    /// A topic that failed fails only its own future.
    /// </remarks>
    public IReadOnlyDictionary<Uuid, KafkaFuture<TopicDescription>>? TopicIdValues => _topicIdValues;

    /// <summary>
    /// One future per requested topic name, or <see langword="null"/> if the request
    /// used topic <em>ids</em> — Java's <c>topicNameValues()</c>.
    /// </summary>
    /// <inheritdoc cref="TopicIdValues" path="/remarks"/>
    public IReadOnlyDictionary<string, KafkaFuture<TopicDescription>>? TopicNameValues => _topicNameValues;

    /// <summary>
    /// Every description keyed by topic name, or <see langword="null"/> if the
    /// request used topic ids — Java's <c>allTopicNames()</c>. Succeeds only if every
    /// topic succeeded, and otherwise fails with the first failure to complete.
    /// </summary>
    /// <returns>
    /// A future yielding the descriptions, or <see langword="null"/> when the request was not by
    /// name.
    /// </returns>
    public KafkaFuture<IReadOnlyDictionary<string, TopicDescription>>? AllTopicNames() =>
        _native is null ? All(_topicNameValues, _topicNameComparer) : NativeAll(_nativeByName, _topicNameComparer);

    /// <summary>
    /// Every description keyed by topic id, or <see langword="null"/> if the
    /// request used topic names — Java's <c>allTopicIds()</c>. Succeeds only if every
    /// topic succeeded, and otherwise fails with the first failure to complete.
    /// </summary>
    /// <returns>
    /// A future yielding the descriptions, or <see langword="null"/> when the request was
    /// not by id.
    /// </returns>
    public KafkaFuture<IReadOnlyDictionary<Uuid, TopicDescription>>? AllTopicIds() =>
        _native is null ? All(_topicIdValues, _topicIdComparer) : NativeAll(_nativeById, _topicIdComparer);

    internal SafeAdminResultHandle? Native => _native;

    internal static DescribeTopicsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<TopicDescription>>? ByName,
            IReadOnlyDictionary<Uuid, KafkaFutureImpl<TopicDescription>>? ById,
            SafeAdminResultHandle Result) native) =>
        new DescribeTopicsResult(native.ByName, native.ById, native.Result);

    private static Dictionary<TKey, KafkaFuture<TopicDescription>> Views<TKey>(
        IReadOnlyDictionary<TKey, KafkaFutureImpl<TopicDescription>> futures, IEqualityComparer<TKey> comparer)
        where TKey : notnull
    {
        Dictionary<TKey, KafkaFuture<TopicDescription>> views =
            new Dictionary<TKey, KafkaFuture<TopicDescription>>(futures.Count, comparer);
        foreach (KeyValuePair<TKey, KafkaFutureImpl<TopicDescription>> entry in futures)
        {
            views.Add(entry.Key, new KafkaFuture<TopicDescription>(entry.Value));
        }

        return views;
    }

    private static KafkaFuture<IReadOnlyDictionary<TKey, TopicDescription>>? NativeAll<TKey>(
        IReadOnlyDictionary<TKey, KafkaFutureImpl<TopicDescription>>? futures, IEqualityComparer<TKey> comparer)
        where TKey : notnull =>
        futures is null
            ? null
            : new KafkaFuture<IReadOnlyDictionary<TKey, TopicDescription>>(
                KafkaFutureImpl.GatherAll(futures, comparer));

    private static KafkaFuture<IReadOnlyDictionary<TKey, TopicDescription>>? All<TKey>(
        IReadOnlyDictionary<TKey, KafkaFuture<TopicDescription>>? values, IEqualityComparer<TKey> comparer)
        where TKey : notnull =>
        values is null ? null : SyncAdminFutures.GatherAll(values, comparer);
}
