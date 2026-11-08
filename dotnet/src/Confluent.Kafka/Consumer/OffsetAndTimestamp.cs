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
    private readonly long _offset;
    private readonly long _timestamp;
    private readonly int? _leaderEpoch;

    internal OffsetAndTimestamp(long offset, long timestamp, int? leaderEpoch)
    {
        _offset = offset;
        _timestamp = timestamp;
        _leaderEpoch = leaderEpoch;
    }

    /// <summary>The offset of the first record at or after <see cref="Timestamp()"/>.</summary>
    /// <returns>The offset of the first record at or after <see cref="Timestamp()"/>.</returns>
    public long Offset() => _offset;

    /// <summary>
    /// The timestamp the offset was resolved from, in milliseconds since the Unix epoch.
    /// </summary>
    /// <returns>The timestamp the offset was resolved from, in milliseconds since the Unix epoch.</returns>
    public long Timestamp() => _timestamp;

    /// <summary>
    /// The leader epoch of the record at <see cref="Offset()"/>, or <see langword="null"/>
    /// when absent (Java's <c>Optional&lt;Integer&gt; leaderEpoch()</c>).
    /// </summary>
    /// <returns>The leader epoch of the record at <see cref="Offset()"/>, or <see langword="null"/> when
    /// absent.</returns>
    public int? LeaderEpoch() => _leaderEpoch;

    /// <summary>Value equality over the timestamp, the offset and the leader epoch, Java's <c>equals</c>.</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two are the same offset and timestamp.</returns>
    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj)
        || (obj is OffsetAndTimestamp other
            && _timestamp == other._timestamp
            && _offset == other._offset
            && _leaderEpoch == other._leaderEpoch);

    /// <summary>The hash of the members <see cref="Equals(object)"/> compares, Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + _timestamp.GetHashCode();
            hash = (hash * 31) + _offset.GetHashCode();
            hash = (hash * 31) + (_leaderEpoch?.GetHashCode() ?? 0);
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
            _offset,
            _timestamp,
            _leaderEpoch is null ? "null" : _leaderEpoch.Value.ToString(CultureInfo.InvariantCulture));
}
