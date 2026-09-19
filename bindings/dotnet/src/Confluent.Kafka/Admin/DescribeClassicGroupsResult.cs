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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>describeClassicGroups</c> call — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeClassicGroupsResult</c>
/// (<c>DescribeClassicGroupsResult.java:30</c>): one awaitable <b>per group id</b>, handed
/// back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// A group that fails faults only <em>its own</em> <see cref="Task"/>; a per-group failure
/// is not a call failure. <see cref="All"/> is the aggregate over those per-group tasks and
/// succeeds only if every one of them did.
/// </para>
/// <para>
/// ⚠ <b>This is the older group generation's describe result</b>, so its value type is
/// <see cref="ClassicGroupDescription"/> rather than
/// <see cref="ConsumerGroupDescription"/>. Java's two classes are otherwise
/// line-for-line identical, and they stay two types here for the reason Java keeps them
/// two: they carry different descriptions and either may later diverge.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded (<c>definition-of-done.md</c> §7).</b> Per-group
/// <em>granularity</em> is fully preserved, but per-group <em>timing independence</em> is
/// not: all the <see cref="Task"/>s complete at the same instant, because the C ABI has no
/// future type and resolves every key together before reporting. The same recorded
/// limitation as <see cref="CreateTopicsResult"/>, for the same reason.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Java's <c>describedGroups()</c> hands back
/// <c>new HashMap&lt;&gt;(futures)</c> (<c>:42</c>) — a defensive copy, because Java's
/// <c>Map</c> is mutable and a caller could otherwise clear the result's own map.
/// <see cref="DescribedGroups"/> returns the stored instance instead: the declared type is
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/>, which already denies the mutation the
/// copy exists to prevent, so copying per call would buy nothing and allocate on every
/// read. This is the call <see cref="DescribeConsumerGroupsResult.DescribedGroups"/>
/// already makes.
/// </para>
/// </remarks>
public sealed class DescribeClassicGroupsResult
{
    private readonly IReadOnlyDictionary<string, Task<ClassicGroupDescription>> _futures;

    /// <summary>
    /// Wraps one awaitable per group id — Java's <b>public</b>
    /// <c>DescribeClassicGroupsResult(Map&lt;String, KafkaFuture&lt;ClassicGroupDescription&gt;&gt;)</c>
    /// (<c>:34</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ Java declares this constructor <b>public</b>, unlike most <c>*Result</c>
    /// constructors, which are package-private — so it is mirrored as public rather than
    /// narrowed, the same call <see cref="DescribeConsumerGroupsResult"/> already makes. It
    /// follows that no key comparer is threaded in from the bridge the way
    /// <see cref="DescribeConfigsResult(IReadOnlyDictionary{ConfigResource, Task{Config}}, IEqualityComparer{ConfigResource})"/>
    /// does: the map here can come from a caller, so there is no bridge comparer to
    /// thread. See <see cref="All"/> for what that means for the aggregate.
    /// </remarks>
    /// <param name="futures">One awaitable per requested group id.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is null.</exception>
    public DescribeClassicGroupsResult(IReadOnlyDictionary<string, Task<ClassicGroupDescription>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// One awaitable per requested group id — Java's <c>describedGroups()</c>
    /// (<c>:41</c>).
    /// </summary>
    /// <remarks>
    /// Returns the stored instance rather than Java's defensive copy; see the note on the
    /// class for why.
    /// </remarks>
    public IReadOnlyDictionary<string, Task<ClassicGroupDescription>> DescribedGroups => _futures;

    /// <summary>
    /// Every group's description in one map, succeeding only if every group succeeded —
    /// Java's <c>all()</c> (<c>:48</c>).
    /// </summary>
    /// <returns>A task yielding the whole map, or faulting with the first failure.</returns>
    /// <remarks>
    /// The aggregate is built with <see cref="StringComparer.Ordinal"/> — what the bridge
    /// keys group ids by, and how Kafka compares them. Ordinal is the <em>strictest</em>
    /// choice, so keys already distinct in <see cref="DescribedGroups"/> stay distinct here
    /// and the gather can never collide; a caller who built the map with a looser comparer
    /// keeps every entry but should look it up the same way it was keyed.
    /// </remarks>
    public Task<IReadOnlyDictionary<string, ClassicGroupDescription>> All() => Gather(_futures);

    private static async Task<IReadOnlyDictionary<string, ClassicGroupDescription>> Gather(
        IReadOnlyDictionary<string, Task<ClassicGroupDescription>> futures)
    {
        // WhenAll first, so a failure faults this task with the first failure exactly as
        // Java's `allOf(...).thenApply(...)` chain does, rather than surfacing whichever
        // key happens to be enumerated first.
        await Task.WhenAll(futures.Values).ConfigureAwait(false);

        Dictionary<string, ClassicGroupDescription> descriptions =
            new Dictionary<string, ClassicGroupDescription>(futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, Task<ClassicGroupDescription>> entry in futures)
        {
            descriptions.Add(entry.Key, entry.Value.Result);
        }

        return descriptions;
    }
}
