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

using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// An offset with the timestamp it was resolved from and its optional leader epoch —
/// the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.OffsetAndTimestamp</c>. A dictionary
/// <em>value</em> in the result of <c>IAsyncConsumer.OffsetsForTimes(...)</c>; the
/// fields are owned copies read out of a borrowed (Category-4) ABI element of an owned
/// <c>OffsetAndTimestampMap_t</c>, which is destroyed after the copy-out
/// (ffi-marshalling.md §B2/§B3).
/// </summary>
/// <remarks>
/// A <c>sealed class</c> (not a <c>readonly struct</c>), matching
/// <see cref="OffsetAndMetadata"/> and the <see cref="ConsumerGroupMetadata"/>
/// precedent — a result payload read once and placed into a dictionary, never a
/// hot-path map key. Immutable.
/// </remarks>
public sealed class OffsetAndTimestamp
{
    /// <summary>
    /// Initializes a new instance from the values copied out of the ABI element.
    /// </summary>
    /// <param name="offset">The resolved offset.</param>
    /// <param name="timestamp">The timestamp (milliseconds since the Unix epoch).</param>
    /// <param name="leaderEpoch">
    /// The leader epoch, or <see langword="null"/> when absent.
    /// </param>
    internal OffsetAndTimestamp(long offset, long timestamp, int? leaderEpoch)
    {
        Offset = offset;
        Timestamp = timestamp;
        LeaderEpoch = leaderEpoch;
    }

    /// <summary>The offset of the first record at or after <see cref="Timestamp"/>.</summary>
    public long Offset { get; }

    /// <summary>
    /// The timestamp the offset was resolved from, in milliseconds since the Unix epoch.
    /// </summary>
    public long Timestamp { get; }

    /// <summary>
    /// The leader epoch of the record at <see cref="Offset"/>, or <see langword="null"/>
    /// when absent (Java's <c>Optional&lt;Integer&gt; leaderEpoch()</c>).
    /// </summary>
    public int? LeaderEpoch { get; }

    /// <summary>
    /// Returns a debug string of the form
    /// <c>"OffsetAndTimestamp{offset=…, timestamp=…, leaderEpoch=…}"</c>.
    /// </summary>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "OffsetAndTimestamp{{offset={0}, timestamp={1}, leaderEpoch={2}}}",
            Offset,
            Timestamp,
            LeaderEpoch is null ? "null" : LeaderEpoch.Value.ToString(CultureInfo.InvariantCulture));
}
