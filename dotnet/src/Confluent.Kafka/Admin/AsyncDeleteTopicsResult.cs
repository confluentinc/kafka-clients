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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DeleteTopics(TopicCollection, DeleteTopicsOptions?)"/> —
/// the .NET realization of Java's
/// <c>DeleteTopicsResult</c>: one future <b>per topic</b>, handed back the moment the
/// request is submitted, keyed by whichever identifier the request used.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Exactly one of <see cref="TopicIdValues"/> and <see cref="TopicNameValues"/> is
/// non-null; the other is <see langword="null"/>.</b> That is Java's contract, stated
/// twice in its javadoc — <em>"a map from topic IDs to futures … <b>if the deleteTopics
/// request used topic IDs. Otherwise return null.</b>"</em>
/// (<c>DeleteTopicsResult.java:51-67</c>) — and it is deliberately <b>not</b> smoothed
/// into a throw or an empty dictionary here. Both smoothings would break a caller ported
/// from Java: an exception turns their <c>if (result.topicIdValues() != null)</c> into a
/// crash, and an empty map turns it into a loop that silently does nothing. The
/// <c>?</c> annotation makes Java's contract visible in the signature, so the compiler
/// warns instead — stricter than Java, without changing the shape.
/// </para>
/// </remarks>
/// <seealso cref="DeleteTopicsResult"/>
public sealed class AsyncDeleteTopicsResult
{
    private readonly IReadOnlyDictionary<Uuid, AsyncKafkaFuture>? _topicIdValues;
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture>? _topicNameValues;

    /// <summary>
    /// Wraps one future per topic, keyed by id <b>or</b> by name — Java's
    /// <c>protected DeleteTopicsResult(Map&lt;Uuid, KafkaFuture&lt;Void&gt;&gt; topicIdFutures,
    /// Map&lt;String, KafkaFuture&lt;Void&gt;&gt; nameFutures)</c>
    /// (<c>DeleteTopicsResult.java:34-41</c>), so a test or a mock can fabricate a result.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Exactly one of the two must be non-null, and that is <b>checked</b> here with
    /// Java's own two messages, verbatim (<c>:35-38</c>). The factories below satisfy it by
    /// construction and go through the same check.
    /// </para>
    /// <para>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionaries are held by reference, as Java holds its maps.
    /// </para>
    /// </remarks>
    /// <param name="topicIdFutures">One future per topic id, or <see langword="null"/> for a by-name result.</param>
    /// <param name="nameFutures">One future per topic name, or <see langword="null"/> for a by-id result.</param>
    /// <exception cref="ArgumentException">
    /// Both are non-null (<c>"topicIdFutures and nameFutures cannot both be specified."</c>)
    /// or both are null (<c>"topicIdFutures and nameFutures cannot both be null."</c>) —
    /// Java's <c>IllegalArgumentException</c>, with no parameter name because neither
    /// argument is wrong on its own.
    /// </exception>
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

    /// <summary>
    /// One future per requested topic id, or <see langword="null"/> if the request
    /// used topic <em>names</em> — Java's <c>topicIdValues()</c>.
    /// </summary>
    /// <remarks>
    /// Java publishes <c>Map&lt;Uuid, KafkaFuture&lt;Void&gt;&gt;</c>, so awaiting one of
    /// these tells you whether that topic was deleted and hands back nothing; a topic that
    /// failed faults only its own <see cref="AsyncKafkaFuture"/>. See the null-versus-non-null note in
    /// the type remarks.
    /// </remarks>
    public IReadOnlyDictionary<Uuid, AsyncKafkaFuture>? TopicIdValues => _topicIdValues;

    /// <summary>
    /// One future per requested topic name, or <see langword="null"/> if the request
    /// used topic <em>ids</em> — Java's <c>topicNameValues()</c>.
    /// </summary>
    /// <inheritdoc cref="TopicIdValues" path="/remarks"/>
    public IReadOnlyDictionary<string, AsyncKafkaFuture>? TopicNameValues => _topicNameValues;

    /// <summary>
    /// Completes when <b>every</b> topic has been deleted, and faults with the first
    /// failure if any topic failed — Java's <c>all()</c> (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    /// <remarks>
    /// ⚠ Java gives <c>DeleteTopicsResult</c> an <c>all()</c> and <b>no</b> typed
    /// aggregate, while giving <see cref="AsyncDescribeTopicsResult"/> the opposite —
    /// <c>allTopicNames()</c>/<c>allTopicIds()</c> and no <c>all()</c>
    /// (<c>DeleteTopicsResult.java:72</c> versus
    /// <c>DescribeTopicsResult.java:80,:90</c>). The asymmetry is Java's, and mirroring it
    /// is deliberate: it is recorded here and asserted by a test so a later phase does not
    /// "complete" either side.
    /// </remarks>
    public AsyncKafkaFuture All() =>
        // Java's own ternary, literally (:73-74): whichever map exists is the one to
        // aggregate. The null-forgiving operator is the constructor's exactly-one check.
        new AsyncKafkaFuture(
            AsyncAdminFutures.WhenAll(_topicIdValues is not null ? _topicIdValues.Values : _topicNameValues!.Values));

    /// <summary>
    /// Builds the by-id result — Java's package-private <c>ofTopicIds</c>.
    /// </summary>
    internal static AsyncDeleteTopicsResult OfTopicIds(
        IReadOnlyDictionary<Uuid, Task<bool>> values, IEqualityComparer<Uuid> comparer) =>
        new AsyncDeleteTopicsResult(Erase(values, comparer), null);

    /// <summary>
    /// Builds the by-name result — Java's package-private <c>ofTopicNames</c>.
    /// </summary>
    internal static AsyncDeleteTopicsResult OfTopicNames(
        IReadOnlyDictionary<string, Task<bool>> values, IEqualityComparer<string> comparer) =>
        new AsyncDeleteTopicsResult(null, Erase(values, comparer));

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncDeleteTopicsResult From(
        (VoidKeyedAdminOperation<string>? ByName, VoidKeyedAdminOperation<Uuid>? ById) operation) =>
        operation.ByName is not null
            ? OfTopicNames(operation.ByName.Tasks, operation.ByName.KeyComparer)
            : OfTopicIds(operation.ById!.Tasks, operation.ById.KeyComparer);

    /// <summary>
    /// Presents the bridge's <c>Task&lt;bool&gt;</c> sources as Java's
    /// <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void bridge's internal
    /// success token (result shape 2 — the ABI has no <c>get_value</c> here, so a null
    /// per-key error <em>is</em> the success value) and must not reach the public surface.
    /// </summary>
    /// <remarks>
    /// Each entry wraps the <b>same</b> task as the bridge's — no second
    /// task whose fault could go unobserved. Same shape as
    /// <see cref="AsyncCreateTopicsResult.Values"/>, and it reuses the bridge's own comparer so
    /// the view and the sources cannot disagree about key identity.
    /// </remarks>
    private static IReadOnlyDictionary<TKey, AsyncKafkaFuture> Erase<TKey>(
        IReadOnlyDictionary<TKey, Task<bool>> values, IEqualityComparer<TKey> comparer)
        where TKey : notnull
    {
        Dictionary<TKey, AsyncKafkaFuture> erased = new Dictionary<TKey, AsyncKafkaFuture>(values.Count, comparer);
        foreach (KeyValuePair<TKey, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
        }

        return erased;
    }
}
