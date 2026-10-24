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
/// Which offset <see cref="IAdmin.ListOffsets"/> should retrieve for a partition — Java's
/// <c>org.apache.kafka.clients.admin.OffsetSpec</c>.
/// </summary>
/// <remarks>
/// <para>
/// A spec crosses to the core as its own handle, never as a bare number: the six no-argument
/// kinds travel as the <c>ListOffsets</c> sentinels <c>-1</c> to <c>-6</c>, and
/// <see cref="ForTimestamp"/> as the timestamp itself, so <c>ForTimestamp(-2)</c> and
/// <see cref="Earliest"/> would collapse onto one number.
/// </para>
/// <para>
/// The hierarchy is closed, a recorded deviation: Java's is open, and Java queries an
/// unrecognised subclass silently as <see cref="Latest"/>. The seven kinds here are exhaustive.
/// </para>
/// </remarks>
public class OffsetSpec
{
    private protected OffsetSpec()
    {
    }

    /// <summary>The earliest available offset — Java's <c>EarliestSpec</c>.</summary>
    public sealed class EarliestSpec : OffsetSpec
    {
        internal EarliestSpec()
        {
        }
    }

    /// <summary>The latest available offset — Java's <c>LatestSpec</c>.</summary>
    public sealed class LatestSpec : OffsetSpec
    {
        internal LatestSpec()
        {
        }
    }

    /// <summary>
    /// The offset with the largest timestamp — Java's <c>MaxTimestampSpec</c>. Because
    /// timestamps may be set client-side this need not equal the log end offset
    /// <see cref="LatestSpec"/> returns.
    /// </summary>
    public sealed class MaxTimestampSpec : OffsetSpec
    {
        internal MaxTimestampSpec()
        {
        }
    }

    /// <summary>
    /// The local log start offset — Java's <c>EarliestLocalSpec</c>. Without
    /// tiered storage this behaves as <see cref="EarliestSpec"/>.
    /// </summary>
    public sealed class EarliestLocalSpec : OffsetSpec
    {
        internal EarliestLocalSpec()
        {
        }
    }

    /// <summary>
    /// The highest offset stored in remote storage — Java's <c>LatestTieredSpec</c>. Without
    /// tiered storage the broker returns an unknown offset.
    /// </summary>
    public sealed class LatestTieredSpec : OffsetSpec
    {
        internal LatestTieredSpec()
        {
        }
    }

    /// <summary>
    /// The earliest offset of records pending upload to remote storage — Java's
    /// <c>EarliestPendingUploadSpec</c>.
    /// </summary>
    public sealed class EarliestPendingUploadSpec : OffsetSpec
    {
        internal EarliestPendingUploadSpec()
        {
        }
    }

    /// <summary>
    /// The earliest offset whose timestamp is at or after a given one — Java's
    /// <c>TimestampSpec</c>.
    /// </summary>
    /// <remarks>
    /// Distinct from every no-argument kind even when the numbers coincide.
    /// </remarks>
    public sealed class TimestampSpec : OffsetSpec
    {
        internal TimestampSpec(long timestamp)
        {
            Timestamp = timestamp;
        }

        internal long Timestamp { get; }
    }

    /// <summary>Retrieve the latest offset — Java's <c>latest()</c>.</summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec Latest() => new LatestSpec();

    /// <summary>Retrieve the earliest offset — Java's <c>earliest()</c>.</summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec Earliest() => new EarliestSpec();

    /// <summary>
    /// Retrieve the earliest offset whose timestamp is at or after
    /// <paramref name="timestamp"/> — Java's <c>forTimestamp(long)</c>.
    /// </summary>
    /// <param name="timestamp">The timestamp, in milliseconds.</param>
    /// <returns>The spec.</returns>
    /// <remarks>
    /// Any value is a timestamp here: <c>ForTimestamp(-2)</c> is a timestamp query, not
    /// <see cref="Earliest"/>.
    /// </remarks>
    public static OffsetSpec ForTimestamp(long timestamp) => new TimestampSpec(timestamp);

    /// <summary>
    /// Retrieve the offset with the largest timestamp — Java's <c>maxTimestamp()</c>.
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec MaxTimestamp() => new MaxTimestampSpec();

    /// <summary>
    /// Retrieve the local log start offset — Java's <c>earliestLocal()</c>.
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec EarliestLocal() => new EarliestLocalSpec();

    /// <summary>
    /// Retrieve the highest offset in remote storage — Java's <c>latestTiered()</c>.
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec LatestTiered() => new LatestTieredSpec();

    /// <summary>
    /// Retrieve the earliest offset pending upload to remote storage — Java's
    /// <c>earliestPendingUpload()</c>.
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec EarliestPendingUpload() => new EarliestPendingUploadSpec();
}
