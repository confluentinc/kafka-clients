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
/// Information about the records deleted from one partition — Java's
/// <c>org.apache.kafka.clients.admin.DeletedRecords</c>.
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
    /// <c>-1</c> is a value, not a failure: a partition that failed fails its own future in
    /// <see cref="DeleteRecordsResult.LowWatermarks"/> and produces no <see cref="DeletedRecords"/>.
    /// </remarks>
    public long LowWatermark { get; }
}
