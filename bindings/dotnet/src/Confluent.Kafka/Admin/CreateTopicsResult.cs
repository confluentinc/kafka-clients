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
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.CreateTopics"/> — the .NET realization of Java's
/// <c>CreateTopicsResult</c>: one awaitable <b>per topic</b>, handed back the moment the
/// request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java's <c>createTopics</c> does not block; it returns a result object holding one
/// <c>KafkaFuture</c> per topic, and the caller decides whether and when to wait. That
/// shape is restored here: <see cref="IAdmin.CreateTopics"/> is a plain synchronous
/// method, and the waiting lives on the <see cref="Task"/>s inside this object. So you
/// can await one topic without awaiting the others, and each carries <em>its own</em>
/// outcome — a topic that failed faults only its own <see cref="Task"/>.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-topic <em>granularity</em> is fully
/// preserved, but per-topic <em>timing independence</em> is not: all the
/// <see cref="Task"/>s complete at the same instant, because the C ABI has no future
/// type and resolves every key together before reporting. In Java a fast topic's future
/// can complete before a slow one's. Nothing observable depends on this for correctness
/// — <see cref="All"/> and per-topic awaiting behave identically either way — and the
/// Python binding has the same limitation for the same reason.
/// </para>
/// </remarks>
public sealed class CreateTopicsResult
{
    private readonly IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>> _values;

    internal CreateTopicsResult(IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>> values)
    {
        _values = values;
    }

    /// <summary>
    /// One awaitable per requested topic, keyed by topic name — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>> Values => _values;

    /// <summary>
    /// Completes when <b>every</b> topic has been created, and faults with the first
    /// failure if any topic failed — Java's <c>all()</c> (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_values.Values);

    /// <summary>
    /// The topic's configuration once it is created — Java's <c>config(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic's configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public Task<Config> Config(string topic) => Apply(topic, static value => value.Config());

    /// <summary>
    /// The topic's id once it is created — Java's <c>topicId(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic id the broker assigned.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public Task<Uuid> TopicId(string topic) => Apply(topic, static value => value.TopicId());

    /// <summary>
    /// The topic's partition count once it is created — Java's
    /// <c>numPartitions(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The number of partitions the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public Task<int> NumPartitions(string topic) => Apply(topic, static value => value.NumPartitions());

    /// <summary>
    /// The topic's replication factor once it is created — Java's
    /// <c>replicationFactor(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The replication factor the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public Task<short> ReplicationFactor(string topic) => Apply(topic, static value => value.ReplicationFactor());

    /// <summary>
    /// The .NET spelling of Java's <c>KafkaFuture.thenApply</c>: derive a narrower
    /// awaitable from one topic's own awaitable. A failure of the source propagates
    /// unchanged, and a throw from <paramref name="map"/> — which is exactly how Java's
    /// accessors report "created, but the broker sent no metadata" — faults the derived
    /// task rather than the caller.
    /// </summary>
    private Task<TResult> Apply<TResult>(string topic, Func<TopicMetadataAndConfig, TResult> map)
    {
        // Argument validation is SYNCHRONOUS — this method is deliberately not `async`,
        // so a caller mistake surfaces at the call site rather than only when the
        // returned Task is awaited (which may be much later, or never). That matches
        // Java, where `futures.get(topic)` throws NullPointerException immediately, and
        // is the .NET guidance for a Task-returning method: a usage error is not an
        // operation outcome.
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (!_values.TryGetValue(topic, out Task<TopicMetadataAndConfig>? source))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Topic '{0}' was not part of this createTopics request.",
                    topic),
                nameof(topic));
        }

        return Project(source, map);

        static async Task<TResult> Project(
            Task<TopicMetadataAndConfig> source, Func<TopicMetadataAndConfig, TResult> map) =>
            map(await source.ConfigureAwait(false));
    }
}
