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

namespace Confluent.Kafka.Admin;

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
