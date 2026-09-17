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
/// Which offset <see cref="IAdmin.ListOffsets"/> should retrieve for a partition — the
/// .NET realization of Java's <c>org.apache.kafka.clients.admin.OffsetSpec</c>
/// (<c>OffsetSpec.java:24-103</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>There are SEVEN kinds, and the wire encoding of six of them is NOT injective —
/// which is why the ABI carries a separate flag.</b> The six no-argument kinds travel as
/// <c>ListOffsets</c> wire sentinels (<c>-1</c> latest, <c>-2</c> earliest, <c>-3</c>
/// max-timestamp, <c>-4</c> earliest-local, <c>-5</c> latest-tiered, <c>-6</c>
/// earliest-pending-upload), and <see cref="ForTimestamp"/> travels as the timestamp
/// itself — so <c>ForTimestamp(-2)</c> and <see cref="Earliest"/> would collapse onto the
/// same number. The header states the consequence outright: they "both yield <c>-2</c>,
/// yet Java treats them differently up to that point". The ABI therefore takes an
/// <c>is_timestamp[i]</c> flag beside the value, and nothing on this path may drop it.
/// </para>
/// <para>
/// <b>Recorded deviation (<c>definition-of-done.md</c> §7): the hierarchy is CLOSED.</b>
/// Java's <c>OffsetSpec</c> and its seven nested classes are public and extensible, and
/// Java's own encoder has no branch for <c>LatestSpec</c> — an unrecognised subclass
/// (including a bare <c>new OffsetSpec()</c>) falls through
/// <c>KafkaAdminClient.getOffsetFromSpec</c>'s <c>if/else</c> chain to
/// <c>return ListOffsetsRequest.LATEST_TIMESTAMP</c> (<c>KafkaAdminClient.java:5190</c>)
/// and is <em>silently</em> queried as <see cref="Latest"/>. The constructor here is
/// <c>private protected</c>, so the seven kinds are exhaustive and that silent
/// substitution is not expressible. Nothing Java can express through the seven factories
/// is lost; what is removed is a footgun whose only reachable effect was a wrong answer
/// with no diagnostic. <see cref="Latest"/> is encoded explicitly rather than by
/// fall-through, and lands on the same <c>-1</c> Java's fall-through produces.
/// </para>
/// </remarks>
public class OffsetSpec
{
    /// <summary>
    /// Restricts derivation to the seven nested kinds — see the recorded deviation in the
    /// type remarks.
    /// </summary>
    private protected OffsetSpec()
    {
    }

    /// <summary>The earliest available offset — Java's <c>EarliestSpec</c> (<c>:26</c>).</summary>
    public sealed class EarliestSpec : OffsetSpec
    {
        internal EarliestSpec()
        {
        }
    }

    /// <summary>The latest available offset — Java's <c>LatestSpec</c> (<c>:27</c>).</summary>
    public sealed class LatestSpec : OffsetSpec
    {
        internal LatestSpec()
        {
        }
    }

    /// <summary>
    /// The offset with the largest timestamp — Java's <c>MaxTimestampSpec</c>
    /// (<c>:28</c>). Because timestamps may be set client-side this need not equal the
    /// log end offset <see cref="LatestSpec"/> returns.
    /// </summary>
    public sealed class MaxTimestampSpec : OffsetSpec
    {
        internal MaxTimestampSpec()
        {
        }
    }

    /// <summary>
    /// The local log start offset — Java's <c>EarliestLocalSpec</c> (<c>:29</c>). Without
    /// tiered storage this behaves as <see cref="EarliestSpec"/>.
    /// </summary>
    public sealed class EarliestLocalSpec : OffsetSpec
    {
        internal EarliestLocalSpec()
        {
        }
    }

    /// <summary>
    /// The highest offset stored in remote storage — Java's <c>LatestTieredSpec</c>
    /// (<c>:30</c>). Without tiered storage the broker returns an unknown offset.
    /// </summary>
    public sealed class LatestTieredSpec : OffsetSpec
    {
        internal LatestTieredSpec()
        {
        }
    }

    /// <summary>
    /// The earliest offset of records pending upload to remote storage — Java's
    /// <c>EarliestPendingUploadSpec</c> (<c>:31</c>).
    /// </summary>
    public sealed class EarliestPendingUploadSpec : OffsetSpec
    {
        internal EarliestPendingUploadSpec()
        {
        }
    }

    /// <summary>
    /// The earliest offset whose timestamp is at or after a given one — Java's
    /// <c>TimestampSpec</c> (<c>:32-42</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Distinct from every no-argument kind even when the numbers coincide</b>, which
    /// is the whole reason the ABI carries <c>is_timestamp</c> — see the type remarks.
    /// </remarks>
    public sealed class TimestampSpec : OffsetSpec
    {
        internal TimestampSpec(long timestamp)
        {
            Timestamp = timestamp;
        }

        /// <summary>
        /// The epoch-millisecond timestamp. <c>internal</c> because Java's
        /// <c>timestamp()</c> is package-private (<c>:39</c>) — it is read by the submit
        /// path, not by callers.
        /// </summary>
        internal long Timestamp { get; }
    }

    /// <summary>Retrieve the latest offset — Java's <c>latest()</c> (<c>:47</c>).</summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec Latest() => new LatestSpec();

    /// <summary>Retrieve the earliest offset — Java's <c>earliest()</c> (<c>:54</c>).</summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec Earliest() => new EarliestSpec();

    /// <summary>
    /// Retrieve the earliest offset whose timestamp is at or after
    /// <paramref name="timestamp"/> — Java's <c>forTimestamp(long)</c> (<c>:63</c>).
    /// </summary>
    /// <param name="timestamp">The timestamp, in milliseconds.</param>
    /// <returns>The spec.</returns>
    /// <remarks>
    /// ⚠ <b>Any value at all is a timestamp here, including the sentinels.</b>
    /// <c>ForTimestamp(-2)</c> is a genuine timestamp query and is <b>not</b>
    /// <see cref="Earliest"/>; the two produce different calls.
    /// </remarks>
    public static OffsetSpec ForTimestamp(long timestamp) => new TimestampSpec(timestamp);

    /// <summary>
    /// Retrieve the offset with the largest timestamp — Java's <c>maxTimestamp()</c>
    /// (<c>:72</c>).
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec MaxTimestamp() => new MaxTimestampSpec();

    /// <summary>
    /// Retrieve the local log start offset — Java's <c>earliestLocal()</c> (<c>:83</c>).
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec EarliestLocal() => new EarliestLocalSpec();

    /// <summary>
    /// Retrieve the highest offset in remote storage — Java's <c>latestTiered()</c>
    /// (<c>:92</c>).
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec LatestTiered() => new LatestTieredSpec();

    /// <summary>
    /// Retrieve the earliest offset pending upload to remote storage — Java's
    /// <c>earliestPendingUpload()</c> (<c>:101</c>).
    /// </summary>
    /// <returns>The spec.</returns>
    public static OffsetSpec EarliestPendingUpload() => new EarliestPendingUploadSpec();
}
