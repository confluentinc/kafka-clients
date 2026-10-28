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
/// The result of <see cref="IAsyncAdmin.DeleteTopics(TopicCollection, DeleteTopicsOptions?)"/> —
/// Java's <c>org.apache.kafka.clients.admin.DeleteTopicsResult</c>: one future per topic, keyed
/// by whichever identifier the request used.
/// </summary>
/// <remarks>
/// Exactly one of <see cref="TopicIdValues"/> and <see cref="TopicNameValues"/> is non-null, as in
/// Java; the other is <see langword="null"/>, not an empty dictionary.
/// </remarks>
/// <seealso cref="DeleteTopicsResult"/>
public sealed class AsyncDeleteTopicsResult
{
    private readonly IReadOnlyDictionary<Uuid, AsyncKafkaFuture>? _topicIdValues;
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture>? _topicNameValues;
    private readonly IReadOnlyList<KafkaFutureImpl>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's protected constructor: a recorded deviation. Exactly one of the two
    // must be non-null, checked with Java's two messages; the dictionaries are held by reference,
    // as Java holds its maps.
    internal AsyncDeleteTopicsResult(
        IReadOnlyDictionary<Uuid, AsyncKafkaFuture>? topicIdFutures,
        IReadOnlyDictionary<string, AsyncKafkaFuture>? nameFutures)
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
        _topicNameValues = nameFutures;
    }

    private AsyncDeleteTopicsResult(
        IReadOnlyDictionary<Uuid, AsyncKafkaFuture>? topicIdFutures,
        IReadOnlyDictionary<string, AsyncKafkaFuture>? nameFutures,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
        : this(topicIdFutures, nameFutures)
    {
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested topic id, or <see langword="null"/> if the request used topic
    /// names — Java's <c>topicIdValues()</c>.
    /// </summary>
    /// <remarks>
    /// A topic's future completes when the topic was deleted; a topic that failed faults only its
    /// own future.
    /// </remarks>
    public IReadOnlyDictionary<Uuid, AsyncKafkaFuture>? TopicIdValues => _topicIdValues;

    /// <summary>
    /// One future per requested topic name, or <see langword="null"/> if the request
    /// used topic <em>ids</em> — Java's <c>topicNameValues()</c>.
    /// </summary>
    /// <inheritdoc cref="TopicIdValues" path="/remarks"/>
    public IReadOnlyDictionary<string, AsyncKafkaFuture>? TopicNameValues => _topicNameValues;

    /// <summary>
    /// Completes when every topic has been deleted, and faults with the first failure if any
    /// topic failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    /// <remarks>
    /// As in Java, there is no typed aggregate here, while
    /// <see cref="AsyncDescribeTopicsResult"/> has <see cref="AsyncDescribeTopicsResult.AllTopicNames"/>
    /// and <see cref="AsyncDescribeTopicsResult.AllTopicIds"/> and no <c>All</c>.
    /// </remarks>
    public AsyncKafkaFuture All() =>
        _nativeFutures is not null
            ? new AsyncKafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures))
            : AsyncAdminFutures.AllOf(_topicIdValues is not null ? _topicIdValues.Values : _topicNameValues!.Values);

    internal static AsyncDeleteTopicsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<object?>>? ByName,
            IReadOnlyDictionary<Uuid, KafkaFutureImpl<object?>>? ById,
            SafeAdminResultHandle Result) native)
    {
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>();
        return new AsyncDeleteTopicsResult(
            native.ById is null ? null : Views(native.ById, EqualityComparer<Uuid>.Default, nativeFutures),
            native.ByName is null ? null : Views(native.ByName, StringComparer.Ordinal, nativeFutures),
            nativeFutures,
            native.Result);
    }

    private static Dictionary<TKey, AsyncKafkaFuture> Views<TKey>(
        IReadOnlyDictionary<TKey, KafkaFutureImpl<object?>> futures,
        IEqualityComparer<TKey> comparer,
        List<KafkaFutureImpl> nativeFutures)
        where TKey : notnull
    {
        Dictionary<TKey, AsyncKafkaFuture> views = new Dictionary<TKey, AsyncKafkaFuture>(futures.Count, comparer);
        foreach (KeyValuePair<TKey, KafkaFutureImpl<object?>> entry in futures)
        {
            views.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return views;
    }
}
