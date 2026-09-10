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
/// The result of <see cref="IAdmin.DeleteRecords"/> — the .NET realization of Java's
/// <c>DeleteRecordsResult</c>: one awaitable <b>per topic partition</b>, handed back the
/// moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The key is a composite the ABI never spells out.</b>
/// <c>kafka_admin_DeleteRecordsResult_t</c> declares <b>no <c>get_key</c></b>: entry
/// <c>i</c>'s key is <c>(get_topic(i), get_partition(i))</c>, reassembled here into the
/// shipped <see cref="Confluent.Kafka.TopicPartition"/> that Java keys this map by.
/// </para>
/// <para>
/// ⚠ <b>A low watermark of <c>-1</c> is a success, not a failure.</b> The ABI's
/// <c>get_low_watermark</c> returns <c>-1</c> for a failed partition, for an
/// out-of-range index, <em>and</em> for a partition whose watermark genuinely is
/// <c>-1</c> — so it cannot be a verdict. The authoritative signal is the entry's own
/// <c>get_error</c>: a partition with a non-null error faults its
/// <see cref="Task"/>; one with a null error completes with its
/// <see cref="DeletedRecords"/>, whatever the number.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-partition <em>granularity</em> is fully
/// preserved, but per-partition <em>timing independence</em> is not: all the
/// <see cref="Task"/>s complete at the same instant, because the C ABI has no future type
/// and resolves every key together before reporting. The same recorded limitation as
/// <see cref="CreateTopicsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class DeleteRecordsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, Task<DeletedRecords>> _futures;

    /// <summary>
    /// Wraps one awaitable per topic partition — Java's <b>public</b>
    /// <c>DeleteRecordsResult(Map&lt;TopicPartition, KafkaFuture&lt;DeletedRecords&gt;&gt;)</c>
    /// (<c>DeleteRecordsResult.java:32</c>). ⚠ Java declares it <b>public</b>, unlike most
    /// <c>*Result</c> constructors, which are package-private. (An earlier wording called
    /// it "the one" such constructor; M15/P4 Stage 2 bound a second —
    /// <see cref="ListOffsetsResult"/> — and a count over Java's 60 <c>*Result</c> types
    /// finds <b>9</b>.)
    /// </summary>
    /// <param name="lowWatermarks">One awaitable per topic partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="lowWatermarks"/> is null.</exception>
    public DeleteRecordsResult(IReadOnlyDictionary<TopicPartition, Task<DeletedRecords>> lowWatermarks)
    {
        _futures = lowWatermarks ?? throw new ArgumentNullException(nameof(lowWatermarks));
    }

    /// <summary>
    /// One awaitable per requested topic partition, each carrying that partition's own
    /// <see cref="DeletedRecords"/> or its own failure — Java's <c>lowWatermarks()</c>.
    /// </summary>
    public IReadOnlyDictionary<TopicPartition, Task<DeletedRecords>> LowWatermarks => _futures;

    /// <summary>
    /// Completes when <b>every</b> partition's deletion has succeeded, and faults with
    /// the first failure if any partition failed — Java's <c>all()</c>
    /// (<c>KafkaFuture.allOf</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    /// <remarks>
    /// Java's <c>all()</c> is a <c>KafkaFuture&lt;Void&gt;</c>: it reports only whether
    /// everything succeeded, and the watermarks are read from
    /// <see cref="LowWatermarks"/>. The non-generic <see cref="Task"/> is that shape.
    /// </remarks>
    public Task All() => Task.WhenAll(_futures.Values);
}
