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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.ListOffsets"/> — the .NET realization of Java's
/// <c>ListOffsetsResult</c>: one awaitable <b>per partition</b>
/// (<c>ListOffsetsResult.java:32, :41, :54</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Shape 1, not shape 3 — despite sharing a "list" name with
/// <see cref="AsyncListPartitionReassignmentsResult"/>.</b> Java stores
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
/// </remarks>
/// <seealso cref="ListOffsetsResult"/>
public sealed class AsyncListOffsetsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> _futures;

    /// <summary>
    /// Wraps one awaitable per topic partition — Java's <b>public</b>
    /// <c>ListOffsetsResult(Map&lt;TopicPartition, KafkaFuture&lt;ListOffsetsResultInfo&gt;&gt;)</c>
    /// (<c>:34</c>).
    /// </summary>
    /// <remarks>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>.
    /// </remarks>
    /// <param name="futures">One awaitable per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is null.</exception>
    internal AsyncListOffsetsResult(
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncListOffsetsResult From(
        KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> operation) =>
        new AsyncListOffsetsResult(AsyncAdminFutures.Wrap(operation.Tasks));

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
    public AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> PartitionResult(TopicPartition partition)
    {
        if (!_futures.TryGetValue(partition, out AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> future))
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
    /// <returns>A future yielding every partition's offset information.</returns>
    /// <remarks>
    /// ⚠ Java's <c>all()</c> yields the <b>map</b>
    /// (<c>KafkaFuture&lt;Map&lt;TopicPartition, ListOffsetsResultInfo&gt;&gt;</c>,
    /// <c>allOf(...).thenApply(...)</c> at <c>:55-67</c>) — unlike
    /// <see cref="AsyncDeleteRecordsResult.All"/>, whose Java counterpart is a bare
    /// <c>KafkaFuture&lt;Void&gt;</c>. The two look alike and are not.
    /// </remarks>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>>(
            Gather(_futures));

    private static async Task<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> Gather(
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures)
    {
        // WhenAll first, so a failure faults this task with the first failure exactly as
        // Java's `allOf(...).thenApply(...)` chain does, rather than surfacing whichever
        // key happens to be enumerated first.
        await AsyncAdminFutures.WhenAll(futures.Values).ConfigureAwait(false);

        Dictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
            new Dictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>(futures.Count);
        foreach (KeyValuePair<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> entry in futures)
        {
            offsets.Add(entry.Key, entry.Value.Get().Result);
        }

        return offsets;
    }
}
