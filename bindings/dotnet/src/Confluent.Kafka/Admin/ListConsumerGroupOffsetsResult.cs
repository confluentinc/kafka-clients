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
using System.Linq;
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>listConsumerGroupOffsets</c> call — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult</c>
/// (<c>ListConsumerGroupOffsetsResult.java:36</c>): one awaitable <b>per group id</b>,
/// each yielding that group's committed offsets keyed by partition.
/// </summary>
/// <remarks>
/// <para>
/// A group that fails faults only <em>its own</em> <see cref="Task"/>; a per-group failure
/// is not a call failure. <see cref="All"/> is the aggregate over those per-group tasks and
/// succeeds only if every one of them did.
/// </para>
/// <para>
/// ⚠ <b>This result publishes no per-key map</b>, unlike
/// <see cref="DescribeConsumerGroupsResult"/> and <see cref="DescribeClassicGroupsResult"/>.
/// Java's <c>futures</c> field is package-private with no getter (<c>:38</c>); the only ways
/// in are the two <c>partitionsToOffsetAndMetadata</c> forms and <c>all()</c>. Those three
/// are mirrored as three methods, and no map property is invented to sit beside them.
/// </para>
/// <para>
/// ⚠ <b>The two <c>PartitionsToOffsetAndMetadata</c> forms are real overloads, not one
/// method with a defaulted argument.</b> Collapsing them would erase the no-argument form's
/// own contract: it is valid <em>only</em> when the result holds exactly one group, and
/// throws otherwise (<c>:50-53</c>). An optional parameter cannot express "the argument may
/// be omitted only when the receiver has a particular shape", so both forms ship.
/// </para>
/// <para>
/// ⚠ <b>A partition present with a <see langword="null"/> value is not the same as a
/// partition that is absent.</b> Java's javadoc is explicit (<c>:47</c>, <c>:59-60</c>):
/// when the group has no committed offset for a listed partition, the partition still
/// appears in the map and its value is <c>null</c>. The C ABI carries the distinction as a
/// per-partition <c>has_offset</c> flag, so the value type is
/// <c>OffsetAndMetadata?</c> rather than a partition silently dropped from the map — the
/// same absent-is-not-empty discipline
/// <see cref="ConsumerGroupDescription.AuthorizedOperations"/> and
/// <see cref="ListConsumerGroupOffsetsSpec.TopicPartitions"/> follow.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded (<c>definition-of-done.md</c> §7).</b> Per-group
/// <em>granularity</em> is fully preserved, but per-group <em>timing independence</em> is
/// not: all the <see cref="Task"/>s complete at the same instant, because the C ABI has no
/// future type and resolves every key together before reporting. The same recorded
/// limitation as <see cref="CreateTopicsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class ListConsumerGroupOffsetsResult
{
    private readonly IReadOnlyDictionary<string, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> _futures;

    /// <summary>
    /// Wraps one awaitable per group id — Java's <b>package-private</b>
    /// <c>ListConsumerGroupOffsetsResult(Map&lt;CoordinatorKey, KafkaFuture&lt;...&gt;&gt;)</c>
    /// (<c>:40</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ Java's constructor is package-private and is mirrored as <see langword="internal"/>
    /// rather than widened — unlike <see cref="DescribeClassicGroupsResult"/>, whose Java
    /// constructor really is public. So the bridge is the only thing that can build one, and
    /// the map is a bridge invariant rather than caller input: no null guard is written, the
    /// same call <see cref="CreateTopicsResult"/> and <see cref="DescribeConfigsResult"/>
    /// already make for their own internal constructors.
    /// </para>
    /// <para>
    /// Java also unwraps <c>CoordinatorKey</c> to its <c>idValue</c> here (<c>:41-42</c>);
    /// that type is an internal admin key with no .NET counterpart, so the map arrives
    /// already keyed by group id.
    /// </para>
    /// </remarks>
    /// <param name="futures">One awaitable per requested group id.</param>
    internal ListConsumerGroupOffsetsResult(
        IReadOnlyDictionary<string, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures)
    {
        _futures = futures;
    }

    /// <summary>
    /// The single requested group's committed offsets keyed by partition — Java's no-argument
    /// <c>partitionsToOffsetAndMetadata()</c> (<c>:49</c>).
    /// </summary>
    /// <returns>A task yielding that group's committed offsets.</returns>
    /// <exception cref="InvalidOperationException">
    /// Offsets for more than one group — or for no group at all — were requested, so there is
    /// no single group for this form to mean. Java throws <c>IllegalStateException</c> on the
    /// same condition (<c>:50-53</c>).
    /// </exception>
    /// <remarks>
    /// A partition the group has no committed offset for is present with a
    /// <see langword="null"/> value rather than absent; see the note on the class.
    /// </remarks>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> PartitionsToOffsetAndMetadata()
    {
        // Java's guard is `futures.size() != 1`, so an EMPTY result throws here too — the
        // message names only the multiple-group case, and the text is Java's verbatim.
        if (_futures.Count != 1)
        {
            throw new InvalidOperationException(
                "Offsets from multiple consumer groups were requested. " +
                "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.");
        }

        // Java's `futures.values().iterator().next()` (:54).
        return _futures.Values.First();
    }

    /// <summary>
    /// One group's committed offsets keyed by partition — Java's
    /// <c>partitionsToOffsetAndMetadata(String)</c> (<c>:62</c>).
    /// </summary>
    /// <param name="groupId">A group id from the request this result came from.</param>
    /// <returns>A task yielding that group's committed offsets.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="groupId"/> was not part of this request. Java throws
    /// <c>IllegalArgumentException</c> on the same condition (<c>:63-64</c>).
    /// </exception>
    /// <remarks>
    /// Argument validation is synchronous — this method is deliberately not <c>async</c>, so
    /// a caller mistake surfaces at the call site rather than only when the returned
    /// <see cref="Task"/> is awaited. That matches Java, which throws out of the method, and
    /// is the .NET guidance for a <see cref="Task"/>-returning method: a usage error is not
    /// an operation outcome.
    /// </remarks>
    public Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> PartitionsToOffsetAndMetadata(string groupId)
    {
        if (groupId is null)
        {
            throw new ArgumentNullException(nameof(groupId));
        }

        if (!_futures.TryGetValue(
                groupId, out Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>? future))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Offsets for consumer group '{0}' were not requested.",
                    groupId),
                nameof(groupId));
        }

        return future;
    }

    /// <summary>
    /// Every group's committed offsets in one map, succeeding only if every group succeeded —
    /// Java's <c>all()</c> (<c>:72</c>).
    /// </summary>
    /// <returns>A task yielding the whole map, or faulting with the first failure.</returns>
    /// <remarks>
    /// The aggregate is built with <see cref="StringComparer.Ordinal"/> — what the bridge keys
    /// group ids by, and how Kafka compares them. Each call starts a fresh gather and hands
    /// back a new <see cref="Task"/>, which is why this is a method and not a property.
    /// </remarks>
    public Task<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> All() =>
        Gather(_futures);

    private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>
        Gather(IReadOnlyDictionary<string, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures)
    {
        // WhenAll first, so a failure faults this task with the first failure exactly as
        // Java's `allOf(...).thenApply(...)` chain does, rather than surfacing whichever key
        // happens to be enumerated first.
        await Task.WhenAll(futures.Values).ConfigureAwait(false);

        Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> offsets =
            new Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
                futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> entry in futures)
        {
            offsets.Add(entry.Key, entry.Value.Result);
        }

        return offsets;
    }
}
