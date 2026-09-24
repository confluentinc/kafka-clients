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

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DeleteTopics"/> — the .NET realization of Java's
/// <c>DeleteTopicsResult</c>: one awaitable <b>per topic</b>, handed back the moment the
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
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-topic <em>granularity</em> is fully
/// preserved, but per-topic <em>timing independence</em> is not: all the
/// <see cref="Task"/>s complete at the same instant, because the C ABI has no future
/// type and resolves every key together before reporting. The same recorded limitation as
/// <see cref="CreateTopicsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class DeleteTopicsResult
{
    private readonly IReadOnlyDictionary<Uuid, Task>? _topicIdValues;
    private readonly IReadOnlyDictionary<string, Task>? _topicNameValues;

    /// <summary>
    /// The invariant is structural rather than checked: the only two callers are the
    /// factories below, and each supplies exactly one map. (Java checks, because its
    /// constructor is <c>protected</c> and therefore reachable by a subclass; this class
    /// is <see langword="sealed"/> with a private constructor, so there is no caller to
    /// check against.)
    /// </summary>
    private DeleteTopicsResult(
        IReadOnlyDictionary<Uuid, Task>? topicIdValues,
        IReadOnlyDictionary<string, Task>? topicNameValues)
    {
        _topicIdValues = topicIdValues;
        _topicNameValues = topicNameValues;
    }

    /// <summary>
    /// One awaitable per requested topic id, or <see langword="null"/> if the request
    /// used topic <em>names</em> — Java's <c>topicIdValues()</c>.
    /// </summary>
    /// <remarks>
    /// Java publishes <c>Map&lt;Uuid, KafkaFuture&lt;Void&gt;&gt;</c>, so awaiting one of
    /// these tells you whether that topic was deleted and hands back nothing; a topic that
    /// failed faults only its own <see cref="Task"/>. See the null-versus-non-null note in
    /// the type remarks.
    /// </remarks>
    public IReadOnlyDictionary<Uuid, Task>? TopicIdValues => _topicIdValues;

    /// <summary>
    /// One awaitable per requested topic name, or <see langword="null"/> if the request
    /// used topic <em>ids</em> — Java's <c>topicNameValues()</c>.
    /// </summary>
    /// <inheritdoc cref="TopicIdValues" path="/remarks"/>
    public IReadOnlyDictionary<string, Task>? TopicNameValues => _topicNameValues;

    /// <summary>
    /// Completes when <b>every</b> topic has been deleted, and faults with the first
    /// failure if any topic failed — Java's <c>all()</c> (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    /// <remarks>
    /// ⚠ Java gives <c>DeleteTopicsResult</c> an <c>all()</c> and <b>no</b> typed
    /// aggregate, while giving <see cref="DescribeTopicsResult"/> the opposite —
    /// <c>allTopicNames()</c>/<c>allTopicIds()</c> and no <c>all()</c>
    /// (<c>DeleteTopicsResult.java:72</c> versus
    /// <c>DescribeTopicsResult.java:80,:90</c>). The asymmetry is Java's, and mirroring it
    /// is deliberate: it is recorded here and asserted by a test so a later phase does not
    /// "complete" either side.
    /// </remarks>
    public Task All() =>
        // Java's own ternary, literally (:73-74): whichever map exists is the one to
        // aggregate. The null-forgiving operator is the structural invariant above.
        Task.WhenAll(_topicIdValues is not null ? _topicIdValues.Values : _topicNameValues!.Values);

    /// <summary>
    /// Builds the by-id result — Java's package-private <c>ofTopicIds</c>.
    /// </summary>
    internal static DeleteTopicsResult OfTopicIds(
        IReadOnlyDictionary<Uuid, Task<bool>> values, IEqualityComparer<Uuid> comparer) =>
        new DeleteTopicsResult(Erase(values, comparer), null);

    /// <summary>
    /// Builds the by-name result — Java's package-private <c>ofTopicNames</c>.
    /// </summary>
    internal static DeleteTopicsResult OfTopicNames(
        IReadOnlyDictionary<string, Task<bool>> values, IEqualityComparer<string> comparer) =>
        new DeleteTopicsResult(null, Erase(values, comparer));

    /// <summary>
    /// Presents the bridge's <c>Task&lt;bool&gt;</c> sources as Java's
    /// <c>KafkaFuture&lt;Void&gt;</c>: the <c>bool</c> is the void bridge's internal
    /// success token (result shape 2 — the ABI has no <c>get_value</c> here, so a null
    /// per-key error <em>is</em> the success value) and must not reach the public surface.
    /// </summary>
    /// <remarks>
    /// The erasure is a reference upcast, so each entry is the <b>same</b>
    /// <see cref="Task"/> instance as the bridge's — no per-key allocation, and no second
    /// task whose fault could go unobserved. Same shape as
    /// <see cref="CreateTopicsResult.Values"/>, and it reuses the bridge's own comparer so
    /// the view and the sources cannot disagree about key identity.
    /// </remarks>
    private static IReadOnlyDictionary<TKey, Task> Erase<TKey>(
        IReadOnlyDictionary<TKey, Task<bool>> values, IEqualityComparer<TKey> comparer)
        where TKey : notnull
    {
        Dictionary<TKey, Task> erased = new Dictionary<TKey, Task>(values.Count, comparer);
        foreach (KeyValuePair<TKey, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        return erased;
    }
}
