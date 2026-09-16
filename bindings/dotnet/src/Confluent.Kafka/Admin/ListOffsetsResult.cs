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
/// The result of <see cref="IAdmin.ListOffsets"/> — the .NET realization of Java's
/// <c>ListOffsetsResult</c>: one awaitable <b>per partition</b>
/// (<c>ListOffsetsResult.java:32, :41, :54</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Shape 1, not shape 3 — despite sharing a "list" name with
/// <see cref="ListPartitionReassignmentsResult"/>.</b> Java stores
/// <c>Map&lt;TopicPartition, KafkaFuture&lt;ListOffsetsResultInfo&gt;&gt;</c>
/// (<c>:32</c>), so each partition has its own awaitable carrying its own value or its own
/// failure; the ABI agrees by declaring <b>both</b> a <c>get_error</c> and a
/// <c>get_value</c>. A partition that fails faults only <em>its own</em> awaitable.
/// </para>
/// <para>
/// ⚠ <b><see cref="PartitionResult"/> THROWS for a partition that was not requested</b>,
/// rather than returning <see langword="null"/> — Java's <c>:43-46</c>. See its own
/// remarks.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded (<c>definition-of-done.md</c> §7).</b> Per-partition
/// <em>granularity</em> is fully preserved, but per-partition <em>timing independence</em>
/// is not — the same recorded limitation as <see cref="CreatePartitionsResult"/>, for the
/// same reason: the ABI settles the whole result together and has no <c>KafkaFuture</c>
/// type with which to express independent timing.
/// </para>
/// </remarks>
public sealed class ListOffsetsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, Task<ListOffsetsResultInfo>> _futures;

    /// <summary>
    /// Wraps one awaitable per topic partition — Java's <b>public</b>
    /// <c>ListOffsetsResult(Map&lt;TopicPartition, KafkaFuture&lt;ListOffsetsResultInfo&gt;&gt;)</c>
    /// (<c>:34</c>). ⚠ Java declares this one <b>public</b>, unlike most <c>*Result</c>
    /// constructors, which are package-private — so it is mirrored as public rather than
    /// narrowed. (Counted, because an earlier draft of this sentence claimed there were
    /// exactly two such classes: across Java's 60 <c>*Result</c> types <b>9</b> have a
    /// public constructor. Among the ones this binding has bound so far the other is
    /// <see cref="DeleteRecordsResult"/>, but that is a fact about coverage, not about
    /// Java.)
    /// </summary>
    /// <param name="futures">One awaitable per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is null.</exception>
    public ListOffsetsResult(IReadOnlyDictionary<TopicPartition, Task<ListOffsetsResultInfo>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// The awaitable for one requested partition — Java's <c>partitionResult(TopicPartition)</c>
    /// (<c>:41</c>).
    /// </summary>
    /// <param name="partition">A partition that was part of the request.</param>
    /// <returns>That partition's awaitable, carrying its own value or its own failure.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partition"/> was not requested. ⚠ <b>Java throws here rather than
    /// returning null</b> (<c>:43-46</c>, <c>IllegalArgumentException</c>), and the message
    /// is carried across verbatim — returning <see langword="null"/> would turn a
    /// programming error into a <see cref="NullReferenceException"/> somewhere else.
    /// </exception>
    public Task<ListOffsetsResultInfo> PartitionResult(TopicPartition partition)
    {
        if (!_futures.TryGetValue(partition, out Task<ListOffsetsResultInfo>? future))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "List Offsets for partition \"{0}\" was not attempted",
                    partition),
                nameof(partition));
        }

        return future;
    }

    /// <summary>
    /// Completes when <b>every</b> requested partition has resolved, yielding the whole
    /// map — Java's <c>all()</c> (<c>:54</c>).
    /// </summary>
    /// <returns>A task yielding every partition's offset information.</returns>
    /// <remarks>
    /// ⚠ Java's <c>all()</c> yields the <b>map</b>
    /// (<c>KafkaFuture&lt;Map&lt;TopicPartition, ListOffsetsResultInfo&gt;&gt;</c>,
    /// <c>allOf(...).thenApply(...)</c> at <c>:55-67</c>) — unlike
    /// <see cref="DeleteRecordsResult.All"/>, whose Java counterpart is a bare
    /// <c>KafkaFuture&lt;Void&gt;</c>. The two look alike and are not.
    /// </remarks>
    public Task<IReadOnlyDictionary<TopicPartition, ListOffsetsResultInfo>> All() => Gather(_futures);

    private static async Task<IReadOnlyDictionary<TopicPartition, ListOffsetsResultInfo>> Gather(
        IReadOnlyDictionary<TopicPartition, Task<ListOffsetsResultInfo>> futures)
    {
        // WhenAll first, so a failure faults this task with the first failure exactly as
        // Java's `allOf(...).thenApply(...)` chain does, rather than surfacing whichever
        // key happens to be enumerated first.
        await Task.WhenAll(futures.Values).ConfigureAwait(false);

        Dictionary<TopicPartition, ListOffsetsResultInfo> offsets =
            new Dictionary<TopicPartition, ListOffsetsResultInfo>(futures.Count);
        foreach (KeyValuePair<TopicPartition, Task<ListOffsetsResultInfo>> entry in futures)
        {
            offsets.Add(entry.Key, entry.Value.Result);
        }

        return offsets;
    }

    /// <summary>
    /// One partition's offset information — the .NET realization of Java's
    /// <c>ListOffsetsResult.ListOffsetsResultInfo</c> (<c>:70-99</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>NESTED, because Java declares it <c>public static class</c> inside
    /// <c>ListOffsetsResult</c> at <c>:70</c></b> (decision D17) — the same treatment
    /// <see cref="DescribeReplicaLogDirsResult.ReplicaLogDirInfo"/> gets. The roadmap lists
    /// it as a flat type; <b>Java wins</b>.
    /// </para>
    /// <para>
    /// ⚠ <b><see cref="LeaderEpoch"/> absence is carried STRUCTURALLY, never by a
    /// sentinel.</b> Java's field is <c>Optional&lt;Integer&gt;</c> (<c>:74</c>), and the
    /// ABI signals it with a <c>bool</c> return —
    /// <c>bool kafka_admin_ListOffsetsResultInfo_leader_epoch(info, int32_t *out_epoch)</c>
    /// returns <see langword="false"/> for <c>Optional.empty()</c>. A negative epoch is a
    /// <em>present</em> value, so "negative means absent" would be a wrong answer.
    /// Contrast M15/P3's <c>total_bytes</c>, where <c>-1</c> genuinely <em>was</em> the
    /// documented sentinel — different accessors, different conventions, and the header is
    /// what distinguishes them.
    /// </para>
    /// <para>
    /// <b>Accessors are properties</b> (decision D18): each is a pure managed field read.
    /// </para>
    /// </remarks>
    public sealed class ListOffsetsResultInfo
    {
        /// <summary>
        /// Creates offset information — Java's <b>public</b>
        /// <c>ListOffsetsResultInfo(long, long, Optional&lt;Integer&gt;)</c> (<c>:76</c>).
        /// </summary>
        /// <param name="offset">The offset.</param>
        /// <param name="timestamp">
        /// The timestamp associated with it, or <c>-1</c> when the broker reported none —
        /// which is what every non-<c>ForTimestamp</c> query returns.
        /// </param>
        /// <param name="leaderEpoch">
        /// The leader epoch, or <see langword="null"/> for Java's <c>Optional.empty()</c>.
        /// ⚠ A negative value here is <b>present</b>, not absent.
        /// </param>
        public ListOffsetsResultInfo(long offset, long timestamp, int? leaderEpoch)
        {
            Offset = offset;
            Timestamp = timestamp;
            LeaderEpoch = leaderEpoch;
        }

        /// <summary>The offset — Java's <c>offset()</c> (<c>:82</c>).</summary>
        public long Offset { get; }

        /// <summary>
        /// The timestamp associated with the offset, or <c>-1</c> when the broker reported
        /// none — Java's <c>timestamp()</c> (<c>:86</c>).
        /// </summary>
        public long Timestamp { get; }

        /// <summary>
        /// The leader epoch, or <see langword="null"/> when Java's <c>leaderEpoch()</c> is
        /// <c>Optional.empty()</c> (<c>:90</c>).
        /// </summary>
        /// <remarks>
        /// ⚠ <see langword="null"/> is the <em>only</em> spelling of absence — see the
        /// structural-absence note in the type remarks.
        /// </remarks>
        public int? LeaderEpoch { get; }

        /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:95-98</c>).</summary>
        /// <returns>The rendering.</returns>
        /// <remarks>
        /// Java interpolates an <c>Optional</c>, whose <c>toString</c> is
        /// <c>Optional[5]</c> or <c>Optional.empty</c>; that spelling is reproduced so the
        /// two clients' diagnostics read alike.
        /// </remarks>
        public override string ToString() =>
            string.Format(
                CultureInfo.InvariantCulture,
                "ListOffsetsResultInfo(offset={0}, timestamp={1}, leaderEpoch={2})",
                Offset,
                Timestamp,
                LeaderEpoch is null
                    ? "Optional.empty"
                    : string.Format(CultureInfo.InvariantCulture, "Optional[{0}]", LeaderEpoch.Value));
    }
}
