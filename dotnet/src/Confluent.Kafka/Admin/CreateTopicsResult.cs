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
/// The result of <see cref="IAdmin.CreateTopics"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateTopicsResult</c>: one future per topic.
/// </summary>
/// <remarks>
/// A topic that fails fails only its own future.
/// </remarks>
/// <seealso cref="AsyncCreateTopicsResult"/>
public sealed class CreateTopicsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture<TopicMetadataAndConfig>>? _values;

    private readonly CreateTopicsRefinements? _native;

    // Internal, unlike Java's protected constructor: a recorded deviation. The dictionary is held
    // by reference, as Java holds its map, and every view reads it when called.
    internal CreateTopicsResult(IReadOnlyDictionary<string, KafkaFuture<TopicMetadataAndConfig>> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    private CreateTopicsResult(CreateTopicsRefinements native)
    {
        _native = native;
    }

    internal static CreateTopicsResult OfClient(CreateTopicsRefinements native) => new CreateTopicsResult(native);

    internal SafeAdminResultHandle? Native => _native?.Result;

    /// <summary>
    /// One future per requested topic, keyed by topic name — Java's <c>values()</c>.
    /// </summary>
    /// <remarks>
    /// A topic's future completes when the topic was created and fails with that topic's error. It
    /// carries no metadata, as in Java: read that through <see cref="Config"/>, <see cref="TopicId"/>,
    /// <see cref="NumPartitions"/> or <see cref="ReplicationFactor"/>. Each access returns a new
    /// dictionary, as Java's <c>values()</c> collects a new map on every call.
    /// </remarks>
    public IReadOnlyDictionary<string, KafkaFuture> Values
    {
        get
        {
            // The client's result hands out a fresh dependant of each topic's future per access, as Java's
            // `thenApply(v -> null)`, so cancelling one leaves the topic's future, which All() and the typed
            // accessors read, alone.
            if (_native is not null)
            {
                Dictionary<string, KafkaFuture> futures =
                    new Dictionary<string, KafkaFuture>(_native.Values.Count, StringComparer.Ordinal);
                foreach (KeyValuePair<string, KafkaFutureImpl<object?>> entry in _native.Values)
                {
                    futures.Add(entry.Key, new KafkaFuture(entry.Value.ThenApply(static _ => (object?)null)));
                }

                return futures;
            }

            // Rebuilt from the caller's map on every access; `Add` throws on two ordinally equal keys,
            // as Java's `Collectors.toMap` does.
            Dictionary<string, KafkaFuture> erased =
                new Dictionary<string, KafkaFuture>(_values!.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, KafkaFuture<TopicMetadataAndConfig>> entry in _values)
            {
                erased.Add(entry.Key, entry.Value.Erase());
            }

            return erased;
        }
    }

    /// <summary>
    /// Completes when every topic has been created, and fails with the first failure if any
    /// topic failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public KafkaFuture All() =>
        _native is null
            ? SyncAdminFutures.AllOf(_values!.Values)
            : new KafkaFuture(KafkaFutureImpl.AllOf(new List<KafkaFutureImpl>(_native.Values.Values)));

    /// <summary>
    /// The topic's configuration once it is created — Java's <c>config(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic's configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public KafkaFuture<Config> Config(string topic) =>
        _native is null ? Apply(topic, static value => value.Config()) : new KafkaFuture<Config>(_native.Config(topic));

    /// <summary>
    /// The topic's id once it is created — Java's <c>topicId(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic id the broker assigned.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public KafkaFuture<Uuid> TopicId(string topic) =>
        _native is null ? Apply(topic, static value => value.TopicId()) : new KafkaFuture<Uuid>(_native.TopicId(topic));

    /// <summary>
    /// The topic's partition count once it is created — Java's
    /// <c>numPartitions(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The number of partitions the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public KafkaFuture<int> NumPartitions(string topic) =>
        _native is null ? Apply(topic, static value => value.NumPartitions()) : new KafkaFuture<int>(_native.NumPartitions(topic));

    /// <summary>
    /// The topic's replication factor once it is created — Java's
    /// <c>replicationFactor(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The replication factor the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <remarks>The future fails with <see cref="ArgumentException"/> when <paramref name="topic"/> was not part of this request.</remarks>
    public KafkaFuture<int> ReplicationFactor(string topic) =>
        _native is null ? Apply(topic, static value => value.ReplicationFactor()) : new KafkaFuture<int>(_native.ReplicationFactor(topic));

    private KafkaFuture<TResult> Apply<TResult>(string topic, Func<TopicMetadataAndConfig, TResult> map)
    {
        // The topic is checked at the call: a usage error is not an operation outcome.
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (!_values!.TryGetValue(topic, out KafkaFuture<TopicMetadataAndConfig> source))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Topic '{0}' was not part of this createTopics request.",
                    topic),
                nameof(topic));
        }

        return new KafkaFuture<TResult>(source.Impl.ThenApply(map));
    }

    /// <summary>
    /// The metadata a broker returned for a newly created topic — Java's
    /// <c>CreateTopicsResult.TopicMetadataAndConfig</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Creation can succeed while the metadata is unavailable (an older broker, or a
    /// <c>validateOnly</c> request): the per-topic future then succeeds with an instance whose
    /// accessors throw, as Java's do. So each accessor is a method; <see cref="HasMetadata"/> tests
    /// for that state without the throw.
    /// </para>
    /// <para>
    /// Each throw is a fresh <see cref="KafkaException"/> with the stored error's
    /// <see cref="KafkaException.Code"/>, message and <see cref="KafkaException.IsRetriable"/>, and
    /// the stored error as its <see cref="System.Exception.InnerException"/>, where Java rethrows the
    /// stored exception itself: a recorded deviation, so that each call has its own stack trace.
    /// </para>
    /// </remarks>
    public sealed class TopicMetadataAndConfig
    {
        private readonly KafkaException? _exception;
        private readonly Uuid _topicId;
        private readonly int _numPartitions;

        private readonly int _replicationFactor;
        private readonly Config? _config;

        /// <summary>
        /// Initializes an instance carrying the broker's metadata — Java's four-argument constructor.
        /// </summary>
        /// <param name="topicId">The topic id.</param>
        /// <param name="numPartitions">The partition count.</param>
        /// <param name="replicationFactor">The replication factor.</param>
        /// <param name="config">The topic's configuration.</param>
        /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
        public TopicMetadataAndConfig(Uuid topicId, int numPartitions, int replicationFactor, Config config)
        {
            _topicId = topicId;
            _numPartitions = numPartitions;
            _replicationFactor = replicationFactor;
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Initializes an instance for a topic that was created but whose metadata the broker did
        /// not return — Java's <c>TopicMetadataAndConfig(ApiException)</c>. Every accessor then
        /// throws.
        /// </summary>
        /// <param name="exception">The reason the metadata is unavailable.</param>
        /// <exception cref="ArgumentNullException"><paramref name="exception"/> is null.</exception>
        public TopicMetadataAndConfig(KafkaException exception)
        {
            _exception = exception ?? throw new ArgumentNullException(nameof(exception));
        }

        /// <summary>
        /// Whether the broker returned metadata, so that the accessors return rather than throw.
        /// Java has no counterpart.
        /// </summary>
        public bool HasMetadata => _exception is null;

        /// <summary>The topic id — Java's <c>topicId()</c>.</summary>
        /// <returns>The topic id the broker assigned.</returns>
        /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
        public Uuid TopicId()
        {
            EnsureSuccess();
            return _topicId;
        }

        /// <summary>The partition count — Java's <c>numPartitions()</c>.</summary>
        /// <returns>The number of partitions the topic was created with.</returns>
        /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
        public int NumPartitions()
        {
            EnsureSuccess();
            return _numPartitions;
        }

        /// <summary>The replication factor — Java's <c>replicationFactor()</c>.</summary>
        /// <returns>The replication factor the topic was created with.</returns>
        /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
        public int ReplicationFactor()
        {
            EnsureSuccess();
            return _replicationFactor;
        }

        /// <summary>The topic's configuration — Java's <c>config()</c>.</summary>
        /// <returns>The configuration the broker reported for the topic.</returns>
        /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
        public Config Config()
        {
            EnsureSuccess();
            return _config!;
        }

        private void EnsureSuccess()
        {
            if (_exception is not null)
            {
                throw new KafkaException(_exception.Code, _exception.Message, _exception.IsRetriable, _exception);
            }
        }
    }
}
