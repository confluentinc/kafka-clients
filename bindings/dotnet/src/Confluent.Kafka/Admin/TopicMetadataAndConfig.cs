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
/// The metadata a broker returned for a newly created topic — the .NET realization of
/// Java's <c>CreateTopicsResult.TopicMetadataAndConfig</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Creation can succeed while the metadata is unavailable</b> — the broker may
/// create the topic and still return nothing about it (an older broker, or a
/// <c>validateOnly</c> request). Java models that with a second, inner error: the
/// per-topic future completes <em>successfully</em> with a
/// <c>TopicMetadataAndConfig</c> whose accessors then throw
/// (<c>ensureSuccess()</c>). This type does the same: every accessor is a
/// <b>method</b>, not a property, precisely because it can throw
/// <see cref="KafkaException"/> — the same reason the consumer's
/// <c>Assignment()</c> / <c>Subscription()</c> / <c>Paused()</c> are methods.
/// </para>
/// <para>
/// Use <see cref="HasMetadata"/> to test for that state without provoking the throw.
/// It has no Java counterpart (Java offers only the throwing accessors), and is the one
/// addition here: a .NET caller should not have to use exceptions for control flow to
/// answer a question the object already knows the answer to.
/// </para>
/// </remarks>
public sealed class TopicMetadataAndConfig
{
    private readonly KafkaException? _exception;
    private readonly Uuid _topicId;
    private readonly int _numPartitions;

    // `int`, not `short` — Java's RESULT side is `int replicationFactor`
    // (CreateTopicsResult.java:112/115/141) and the ABI agrees (`int32_t`). `short` is
    // the REQUEST side only (NewTopic.replicationFactor()).
    private readonly int _replicationFactor;
    private readonly Config? _config;

    /// <summary>
    /// Initializes a successful instance carrying the broker's metadata.
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
    /// Initializes an instance for a topic that <b>was created</b> but whose metadata
    /// the broker did not return — Java's <c>TopicMetadataAndConfig(Throwable)</c>.
    /// Every accessor then throws.
    /// </summary>
    /// <param name="exception">The reason the metadata is unavailable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is null.</exception>
    public TopicMetadataAndConfig(KafkaException exception)
    {
        _exception = exception ?? throw new ArgumentNullException(nameof(exception));
    }

    /// <summary>
    /// Whether the broker returned metadata, i.e. whether the accessors below will
    /// return rather than throw. No Java counterpart — see the type remarks.
    /// </summary>
    public bool HasMetadata => _exception is null;

    /// <summary>The topic id (Java's <c>topicId()</c>).</summary>
    /// <returns>The topic id the broker assigned.</returns>
    /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
    public Uuid TopicId()
    {
        EnsureSuccess();
        return _topicId;
    }

    /// <summary>The partition count (Java's <c>numPartitions()</c>).</summary>
    /// <returns>The number of partitions the topic was created with.</returns>
    /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
    public int NumPartitions()
    {
        EnsureSuccess();
        return _numPartitions;
    }

    /// <summary>The replication factor (Java's <c>replicationFactor()</c>).</summary>
    /// <returns>The replication factor the topic was created with.</returns>
    /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
    public int ReplicationFactor()
    {
        EnsureSuccess();
        return _replicationFactor;
    }

    /// <summary>The topic's configuration (Java's <c>config()</c>).</summary>
    /// <returns>The configuration the broker reported for the topic.</returns>
    /// <exception cref="KafkaException">The broker returned no metadata for this topic.</exception>
    public Config Config()
    {
        EnsureSuccess();
        return _config!;
    }

    /// <summary>
    /// Java's <c>ensureSuccess()</c>: throws the metadata failure.
    /// </summary>
    /// <remarks>
    /// Java rethrows the stored exception <b>itself</b> (<c>throw exception;</c>,
    /// <c>CreateTopicsResult.java:151-154</c>), so every accessor surfaces the same
    /// instance with its identity fields intact. The .NET shape is a deliberate, recorded
    /// deviation (M15/P13.4 D7): it throws a <b>fresh</b> <see cref="KafkaException"/>
    /// that copies the stored error's <see cref="KafkaException.Code"/>,
    /// <see cref="System.Exception.Message"/> and <see cref="KafkaException.IsRetriable"/>,
    /// and carries the stored instance as
    /// <see cref="System.Exception.InnerException"/>. Rethrowing one stored instance from
    /// several call sites would overwrite its stack trace on every throw; the fresh
    /// exception gives each accessor call its own trace while a caller that branches on
    /// the code or the retriable flag sees exactly what Java's caller would.
    /// </remarks>
    private void EnsureSuccess()
    {
        if (_exception is not null)
        {
            throw new KafkaException(_exception.Code, _exception.Message, _exception.IsRetriable, _exception);
        }
    }
}
