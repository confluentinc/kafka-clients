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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// An offset with the timestamp it was found for and its leader epoch, Java's
/// <c>org.apache.kafka.clients.consumer.OffsetAndTimestamp</c>, as <c>OffsetsForTimes</c> returns it. Immutable.
/// </summary>
public sealed class OffsetAndTimestamp
{
    /// <summary>
    /// Creates an offset and the timestamp it was found for, with no leader epoch, Java's
    /// <c>OffsetAndTimestamp(long, long)</c>.
    /// </summary>
    /// <param name="offset">The offset.</param>
    /// <param name="timestamp">The timestamp, in milliseconds since the Unix epoch.</param>
    /// <exception cref="IllegalArgumentException"><paramref name="offset"/> or <paramref name="timestamp"/> is negative.</exception>
    public OffsetAndTimestamp(long offset, long timestamp)
        : this(offset, timestamp, null)
    {
    }

    /// <summary>
    /// Creates an offset and the timestamp it was found for, Java's
    /// <c>OffsetAndTimestamp(long, long, Optional&lt;Integer&gt;)</c>.
    /// </summary>
    /// <param name="offset">The offset.</param>
    /// <param name="timestamp">The timestamp, in milliseconds since the Unix epoch.</param>
    /// <param name="leaderEpoch">The leader epoch of the record at the offset, or <see langword="null"/>.</param>
    /// <exception cref="IllegalArgumentException"><paramref name="offset"/> or <paramref name="timestamp"/> is negative.</exception>
    public OffsetAndTimestamp(long offset, long timestamp, int? leaderEpoch)
    {
        if (offset < 0)
        {
            throw new IllegalArgumentException("Invalid negative offset");
        }

        if (timestamp < 0)
        {
            throw new IllegalArgumentException("Invalid negative timestamp");
        }

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

    /// <summary>Value equality over the timestamp, the offset and the leader epoch, Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two are the same offset and timestamp.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is OffsetAndTimestamp other
            && Timestamp == other.Timestamp
            && Offset == other.Offset
            && LeaderEpoch == other.LeaderEpoch);

    /// <summary>The hash of the members <see cref="Equals(object)"/> compares, Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + Timestamp.GetHashCode();
            hash = (hash * 31) + Offset.GetHashCode();
            hash = (hash * 31) + (LeaderEpoch?.GetHashCode() ?? 0);
            return hash;
        }
    }

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
