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
/// An offset with the timestamp it was found for and its leader epoch, Java's
/// <c>org.apache.kafka.clients.consumer.OffsetAndTimestamp</c>, as <c>OffsetsForTimes</c> returns it. Immutable.
/// </summary>
public sealed class OffsetAndTimestamp
{
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
