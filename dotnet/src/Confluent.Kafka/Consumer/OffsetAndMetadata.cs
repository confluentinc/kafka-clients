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
using System.Globalization;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// An offset with its metadata and leader epoch, Java's <c>org.apache.kafka.clients.consumer.OffsetAndMetadata</c>:
/// what <c>Committed</c> returns and what <c>Commit(offsets)</c> takes.
/// </summary>
/// <remarks>
/// Immutable, with Java's value equality over the offset, the metadata and the leader epoch. Like Java, it
/// declares no <c>==</c> operator, so <c>==</c> compares references.
/// </remarks>
public sealed class OffsetAndMetadata : IEquatable<OffsetAndMetadata>
{
    /// <summary>
    /// Initializes an offset to commit (Java's <c>OffsetAndMetadata(long, Optional&lt;Integer&gt;, String)</c>).
    /// </summary>
    /// <param name="offset">The offset to commit (must be non-negative).</param>
    /// <param name="metadata">
    /// The commit metadata, or <see langword="null"/> (coerced to the empty string).
    /// </param>
    /// <param name="leaderEpoch">
    /// The leader epoch, or <see langword="null"/> when absent. A negative value is stored as
    /// <see langword="null"/>.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="offset"/> is negative (Java: <c>"Invalid negative offset"</c>).
    /// </exception>
    public OffsetAndMetadata(long offset, string? metadata = null, int? leaderEpoch = null)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Invalid negative offset");
        }

        Offset = offset;
        Metadata = metadata ?? string.Empty;
        // Java's leaderEpoch() reports a negative epoch as absent; .NET has no raw accessor, so the epoch is
        // normalised once, here, and Equals, GetHashCode and ToString see it as Java's do.
        LeaderEpoch = leaderEpoch < 0 ? null : leaderEpoch;
    }

    /// <summary>The committed offset.</summary>
    public long Offset { get; }

    /// <summary>The commit metadata; the empty string when none was set, never <see langword="null"/>.</summary>
    public string Metadata { get; }

    /// <summary>
    /// The leader epoch of the record at <see cref="Offset"/>, or <see langword="null"/> when absent (Java's
    /// <c>Optional&lt;Integer&gt; leaderEpoch()</c>). A negative epoch is absent.
    /// </summary>
    public int? LeaderEpoch { get; }

    /// <summary>
    /// Value equality over <see cref="Offset"/>, <see cref="Metadata"/> (ordinal) and <see cref="LeaderEpoch"/>,
    /// as Java's <c>equals</c>.
    /// </summary>
    /// <param name="other">The instance to compare with.</param>
    /// <returns>Whether the two describe the same committed offset.</returns>
    public bool Equals(OffsetAndMetadata? other) =>
        other is not null
        && Offset == other.Offset
        && string.Equals(Metadata, other.Metadata, StringComparison.Ordinal)
        && LeaderEpoch == other.LeaderEpoch;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as OffsetAndMetadata);

    /// <summary>
    /// The hash of the fields <see cref="Equals(OffsetAndMetadata?)"/> compares, as Java's <c>hashCode</c>.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = Offset.GetHashCode();
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Metadata);
            return (hash * 31) + (LeaderEpoch?.GetHashCode() ?? 0);
        }
    }

    /// <summary>
    /// Returns <c>"OffsetAndMetadata{offset=..., leaderEpoch=..., metadata='...'}"</c>, Java's format.
    /// </summary>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "OffsetAndMetadata{{offset={0}, leaderEpoch={1}, metadata='{2}'}}",
            Offset,
            LeaderEpoch is null ? "null" : LeaderEpoch.Value.ToString(CultureInfo.InvariantCulture),
            Metadata);
}
