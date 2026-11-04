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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.CreateTopics"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateTopicsResult</c>: one future per topic.
/// </summary>
/// <remarks>
/// A topic that fails faults only its own future.
/// </remarks>
/// <seealso cref="CreateTopicsResult"/>
public sealed class AsyncCreateTopicsResult
{
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>? _values;

    private readonly CreateTopicsRefinements? _native;

    // Internal, unlike Java's protected constructor: a recorded deviation. The dictionary is held
    // by reference, as Java holds its map, and every view reads it when called.
    internal AsyncCreateTopicsResult(IReadOnlyDictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    private AsyncCreateTopicsResult(CreateTopicsRefinements native)
    {
        _native = native;
    }

    internal static AsyncCreateTopicsResult OfClient(CreateTopicsRefinements native) => new AsyncCreateTopicsResult(native);

    internal SafeAdminResultHandle? Native => _native?.Result;

    /// <summary>
    /// One future per requested topic, keyed by topic name — Java's <c>values()</c>.
    /// </summary>
    /// <remarks>
    /// A topic's future completes when the topic was created and faults with that topic's error. It
    /// carries no metadata, as in Java: read that through <see cref="Config"/>, <see cref="TopicId"/>,
    /// <see cref="NumPartitions"/> or <see cref="ReplicationFactor"/>. Each access returns a new
    /// dictionary, as Java's <c>values()</c> collects a new map on every call.
    /// </remarks>
    public IReadOnlyDictionary<string, AsyncKafkaFuture> Values
    {
        get
        {
            // The client's result hands out a fresh dependant of each topic's future per access, as Java's
            // `thenApply(v -> null)`, so cancelling one leaves the topic's future, which All() and the typed
            // accessors read, alone.
            if (_native is not null)
            {
                Dictionary<string, AsyncKafkaFuture> futures =
                    new Dictionary<string, AsyncKafkaFuture>(_native.Values.Count, StringComparer.Ordinal);
                foreach (KeyValuePair<string, KafkaFutureImpl<object?>> entry in _native.Values)
                {
                    futures.Add(entry.Key, new AsyncKafkaFuture(entry.Value.ThenApply(static _ => (object?)null)));
                }

                return futures;
            }

            // Rebuilt from the caller's map on every access; `Add` throws on two ordinally equal keys,
            // as Java's `Collectors.toMap` does.
            Dictionary<string, AsyncKafkaFuture> erased =
                new Dictionary<string, AsyncKafkaFuture>(_values!.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> entry in _values)
            {
                erased.Add(entry.Key, entry.Value.Erase());
            }

            return erased;
        }
    }

    /// <summary>
    /// Completes when every topic has been created, and faults with the first failure if any
    /// topic failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() =>
        _native is null
            ? AsyncAdminFutures.AllOf(_values!.Values)
            : new AsyncKafkaFuture(KafkaFutureImpl.AllOf(new List<KafkaFutureImpl>(_native.Values.Values)));

    /// <summary>
    /// The topic's configuration once it is created — Java's <c>config(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic's configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public AsyncKafkaFuture<Config> Config(string topic) =>
        _native is null ? Apply(topic, static value => value.Config()) : new AsyncKafkaFuture<Config>(_native.Config(topic));

    /// <summary>
    /// The topic's id once it is created — Java's <c>topicId(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic id the broker assigned.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public AsyncKafkaFuture<Uuid> TopicId(string topic) =>
        _native is null ? Apply(topic, static value => value.TopicId()) : new AsyncKafkaFuture<Uuid>(_native.TopicId(topic));

    /// <summary>
    /// The topic's partition count once it is created — Java's
    /// <c>numPartitions(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The number of partitions the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public AsyncKafkaFuture<int> NumPartitions(string topic) =>
        _native is null ? Apply(topic, static value => value.NumPartitions()) : new AsyncKafkaFuture<int>(_native.NumPartitions(topic));

    /// <summary>
    /// The topic's replication factor once it is created — Java's
    /// <c>replicationFactor(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The replication factor the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public AsyncKafkaFuture<int> ReplicationFactor(string topic) =>
        _native is null ? Apply(topic, static value => value.ReplicationFactor()) : new AsyncKafkaFuture<int>(_native.ReplicationFactor(topic));

    private AsyncKafkaFuture<TResult> Apply<TResult>(string topic, Func<CreateTopicsResult.TopicMetadataAndConfig, TResult> map)
    {
        // The topic is checked at the call: a usage error is not an operation outcome.
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (!_values!.TryGetValue(topic, out AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig> source))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Topic '{0}' was not part of this createTopics request.",
                    topic),
                nameof(topic));
        }

        return new AsyncKafkaFuture<TResult>(source.Impl.ThenApply(map));
    }
}
