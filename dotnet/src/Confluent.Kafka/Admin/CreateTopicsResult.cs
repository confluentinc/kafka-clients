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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.CreateTopics"/> — the .NET realization of Java's
/// <c>CreateTopicsResult</c>: one future <b>per topic</b>, handed back the moment the
/// request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// Java's <c>createTopics</c> does not block; it returns a result object holding one
/// <see cref="KafkaFuture{T}"/> per topic, and the caller decides whether and when to wait. That
/// shape is restored here: <see cref="IAdmin.CreateTopics"/> is a plain synchronous
/// method, and the waiting lives on the <see cref="KafkaFuture"/>s inside this object. So you
/// can wait for one topic without waiting for the others, and each carries <em>its own</em>
/// outcome — a topic that failed fails only its own <see cref="KafkaFuture"/>.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its topic (or, for
/// <see cref="All"/>, every topic) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncCreateTopicsResult"/>
public sealed class CreateTopicsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture<TopicMetadataAndConfig>> _values;

    /// <summary>
    /// Wraps one future per topic — Java's
    /// <c>protected CreateTopicsResult(Map&lt;String, KafkaFuture&lt;TopicMetadataAndConfig&gt;&gt; futures)</c>
    /// (<c>CreateTopicsResult.java:35</c>).
    /// </summary>
    /// <remarks>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionary is held by reference, as Java holds its map
    /// (<c>CreateTopicsResult.java:35-37</c>), and every view reads it when called, <see cref="Values"/> included.
    /// </remarks>
    /// <param name="futures">One future per topic, keyed by topic name.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="futures"/> is null — where Java stores the null and throws
    /// <c>NullPointerException</c> on first use.
    /// </exception>
    internal CreateTopicsResult(IReadOnlyDictionary<string, KafkaFuture<TopicMetadataAndConfig>> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static CreateTopicsResult From(KeyedAdminOperation<string, TopicMetadataAndConfig> operation) =>
        new CreateTopicsResult(SyncAdminFutures.Wrap(operation.SyncSources));

    /// <summary>
    /// One future per requested topic, keyed by topic name — Java's
    /// <c>values()</c>, whose declared type is
    /// <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Waiting for one of these tells you <b>whether that topic was created</b> and nothing
    /// more: its <c>Get()</c> returns on success and throws that topic's own
    /// <see cref="KafkaException"/> on failure. That is Java's contract exactly —
    /// <c>CreateTopicsResult.java:43-46</c> derives this view from its private
    /// <c>Map&lt;String, KafkaFuture&lt;TopicMetadataAndConfig&gt;&gt;</c> with
    /// <c>thenApply(v -&gt; null)</c>, so a Java caller never receives the metadata from
    /// <c>values()</c> either. Read the metadata through <see cref="Config"/>,
    /// <see cref="TopicId"/>, <see cref="NumPartitions"/> or
    /// <see cref="ReplicationFactor"/>.
    /// </para>
    /// <para>
    /// <b>Each access returns a fresh snapshot of the live map, as in Java.</b> Java's
    /// <c>values()</c> keeps no view: it collects a new map from the stored
    /// <c>futures</c> map on every call (<c>:43-46</c>). This property does the same with
    /// the dictionary the constructor was given, so its membership follows the caller's
    /// map. An entry added, replaced or removed after construction shows in the next
    /// access, as it does in <see cref="All"/> and the four typed accessors, and a snapshot
    /// already taken keeps the entries it was taken with. Read it once into a local when
    /// you need it more than once.
    /// </para>
    /// <para>
    /// <b>The snapshot is keyed ordinally, whatever comparer the caller's map uses.</b>
    /// In the JDK, Java's <c>Collectors.toMap</c> collects into a <c>HashMap</c>, which
    /// looks keys up with <c>String.equals</c>, and <see cref="StringComparer.Ordinal"/> is
    /// the .NET counterpart. The typed accessors look the topic up in the caller's map
    /// instead, as Java's accessors call <c>futures.get(topic)</c> (<c>:66</c>,
    /// <c>:79</c>, <c>:92</c>, <c>:105</c>). So a caller map with a non-ordinal comparer,
    /// such as <see cref="StringComparer.OrdinalIgnoreCase"/>, splits the two: for a topic
    /// stored as <c>"t"</c>, <c>Values["T"]</c> misses while <c>NumPartitions("T")</c>
    /// finds it. A Java caller gets the same split from a case-insensitive map, so it is
    /// Java's behavior, not a defect.
    /// </para>
    /// </remarks>
    public IReadOnlyDictionary<string, KafkaFuture> Values
    {
        get
        {
            // Rebuilt on every access, from the map the caller handed in — not cached at
            // construction, which would freeze membership while All() and the accessors
            // go on reading the live map.
            //
            // The erasure wraps the SAME latch as the caller's map holds, so a topic's
            // void future completes exactly when its typed one does, with the same failure.
            // The metadata stays reachable only through the typed accessors.
            //
            // `Add` throws on two ordinally equal keys, which only a caller comparer that
            // tells ordinally equal strings apart (reference equality, say) can produce.
            // Java's `Collectors.toMap` also throws on that input, with
            // IllegalStateException rather than ArgumentException.
            Dictionary<string, KafkaFuture> erased =
                new Dictionary<string, KafkaFuture>(_values.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, KafkaFuture<TopicMetadataAndConfig>> entry in _values)
            {
                erased.Add(entry.Key, entry.Value.Erase());
            }

            return erased;
        }
    }

    /// <summary>
    /// Completes when <b>every</b> topic has been created, and fails with the first
    /// failure in key order if any topic failed — Java's <c>all()</c> (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every topic.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);

    /// <summary>
    /// The topic's configuration once it is created — Java's <c>config(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic's configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public KafkaFuture<Config> Config(string topic) => Apply(topic, static value => value.Config());

    /// <summary>
    /// The topic's id once it is created — Java's <c>topicId(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The topic id the broker assigned.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public KafkaFuture<Uuid> TopicId(string topic) => Apply(topic, static value => value.TopicId());

    /// <summary>
    /// The topic's partition count once it is created — Java's
    /// <c>numPartitions(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The number of partitions the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public KafkaFuture<int> NumPartitions(string topic) => Apply(topic, static value => value.NumPartitions());

    /// <summary>
    /// The topic's replication factor once it is created — Java's
    /// <c>replicationFactor(String)</c>.
    /// </summary>
    /// <param name="topic">A topic name from <see cref="Values"/>.</param>
    /// <returns>The replication factor the topic was created with.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> was not part of this request.</exception>
    public KafkaFuture<int> ReplicationFactor(string topic) => Apply(topic, static value => value.ReplicationFactor());

    /// <summary>
    /// The .NET spelling of Java's <c>KafkaFuture.thenApply</c>: derive a narrower
    /// future from one topic's own future. A failure of the source propagates
    /// unchanged, and a throw from <paramref name="map"/> — which is exactly how Java's
    /// accessors report "created, but the broker sent no metadata" — fails the derived
    /// future rather than the caller. The map runs once, on the first <c>Get()</c>'s thread, and its
    /// value or exception is what every later <c>Get()</c> returns or rethrows.
    /// </summary>
    private KafkaFuture<TResult> Apply<TResult>(string topic, Func<TopicMetadataAndConfig, TResult> map)
    {
        // Argument validation is SYNCHRONOUS, at the call site, rather than only when the
        // returned future is waited for (which may be much later, or never). That matches
        // Java, where `futures.get(topic)` throws NullPointerException immediately: a usage
        // error is not an operation outcome.
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        if (!_values.TryGetValue(topic, out KafkaFuture<TopicMetadataAndConfig> source))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Topic '{0}' was not part of this createTopics request.",
                    topic),
                nameof(topic));
        }

        return SyncAdminFutures.Project(source, map);
    }
}
