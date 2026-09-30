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
/// </remarks>
public sealed class CreateTopicsResult
{
    private readonly IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>> _values;

    /// <summary>
    /// Wraps one awaitable per topic — Java's
    /// <c>protected CreateTopicsResult(Map&lt;String, KafkaFuture&lt;TopicMetadataAndConfig&gt;&gt; futures)</c>
    /// (<c>CreateTopicsResult.java:35</c>), so a test or a mock can fabricate a result.
    /// </summary>
    /// <remarks>
    /// <b>Public rather than <c>protected</c>, on a <see langword="sealed"/> type</b>
    /// (M15/P13.2, D5). Every admin result in this binding is sealed and none of its
    /// accessors is virtual, so Java's subclass-and-override route would let a user
    /// subclass but override nothing; fabricating a result — the Java use case — needs only
    /// the constructor. <see cref="DeleteRecordsResult"/> is the precedent. The parameter's
    /// type is the one the typed accessors read, and the dictionary is held by reference,
    /// as Java holds its map (<c>CreateTopicsResult.java:35-37</c>). Every view reads it
    /// when called, <see cref="Values"/> included, so a change the caller makes to it
    /// later is visible through all of them.
    /// </remarks>
    /// <param name="futures">One awaitable per topic, keyed by topic name.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="futures"/> is null — where Java stores the null and throws
    /// <c>NullPointerException</c> on first use.
    /// </exception>
    public CreateTopicsResult(IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// One awaitable per requested topic, keyed by topic name — Java's
    /// <c>values()</c>, whose declared type is
    /// <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Awaiting one of these tells you <b>whether that topic was created</b> and nothing
    /// more: it completes on success and faults with that topic's own
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
    public IReadOnlyDictionary<string, Task> Values
    {
        get
        {
            // Rebuilt on every access, from the map the caller handed in — not cached at
            // construction, which would freeze membership while All() and the accessors
            // go on reading the live map.
            //
            // The erasure is a reference upcast: each entry is the SAME Task instance as the
            // caller's map holds, so no second Task is created whose fault could go
            // unobserved. The metadata stays reachable only through the typed accessors.
            //
            // `Add` throws on two ordinally equal keys, which only a caller comparer that
            // tells ordinally equal strings apart (reference equality, say) can produce.
            // Java's `Collectors.toMap` also throws on that input, with
            // IllegalStateException rather than ArgumentException.
            Dictionary<string, Task> erased =
                new Dictionary<string, Task>(_values.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, Task<TopicMetadataAndConfig>> entry in _values)
            {
                erased.Add(entry.Key, entry.Value);
            }

            return erased;
        }
    }

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
    public Task<int> ReplicationFactor(string topic) => Apply(topic, static value => value.ReplicationFactor());

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
