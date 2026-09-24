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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Information about the records deleted from one partition — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.DeletedRecords</c>.
/// </summary>
public sealed class DeletedRecords
{
    /// <summary>
    /// Initializes an instance — Java's <c>DeletedRecords(long lowWatermark)</c>.
    /// </summary>
    /// <param name="lowWatermark">
    /// The partition's low watermark after the deletion.
    /// </param>
    public DeletedRecords(long lowWatermark)
    {
        LowWatermark = lowWatermark;
    }

    /// <summary>
    /// The "low watermark" for the topic partition on which the deletion was executed —
    /// Java's <c>lowWatermark()</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b><c>-1</c> here is a value, not a failure.</b> The ABI's
    /// <c>get_low_watermark</c> documents <c>-1</c> for three different situations at
    /// once — the partition failed, the index was out of range, or the watermark really
    /// is <c>-1</c> — so the binding never reads it as a verdict. A partition that failed
    /// faults its own <see cref="System.Threading.Tasks.Task"/> in
    /// <see cref="DeleteRecordsResult.LowWatermarks"/> and produces no
    /// <see cref="DeletedRecords"/> at all; if you are holding one, its partition
    /// succeeded and this is its watermark.
    /// </para>
    /// <para>
    /// A property rather than Java's method, matching <see cref="TopicDescription.Name"/>
    /// and <see cref="TopicListing.Name"/>: it is a pure managed field read that does no
    /// P/Invoke and cannot throw, which is CLAUDE.md §3's "non-blocking getter → sync
    /// property" row. (Its sibling <see cref="RecordsToDelete.BeforeOffset()"/> stays a
    /// method — there it collides with the static factory of the same name, exactly as in
    /// Java.)
    /// </para>
    /// </remarks>
    public long LowWatermark { get; }
}
