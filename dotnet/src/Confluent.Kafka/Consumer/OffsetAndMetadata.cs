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
    private readonly long _offset;
    private readonly string _metadata;
    private readonly int? _leaderEpoch;

    /// <summary>
    /// Construct a new OffsetAndMetadata object for committing through <see cref="KafkaConsumer{K, V}"/>.
    /// </summary>
    /// <param name="offset">The offset to be committed.</param>
    /// <param name="leaderEpoch">
    /// Optional leader epoch of the last consumed record: <see langword="null"/> when absent. A negative epoch is
    /// absent.
    /// </param>
    /// <param name="metadata">Non-null metadata. A <see langword="null"/> is stored as the empty string.</param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="offset"/> is negative, with the message <c>"Invalid negative offset"</c>.
    /// </exception>
    public OffsetAndMetadata(long offset, int? leaderEpoch, string? metadata)
    {
        if (offset < 0)
        {
            throw new IllegalArgumentException("Invalid negative offset");
        }

        _offset = offset;
        _metadata = metadata ?? string.Empty;
        // Java's leaderEpoch() reports a negative epoch as absent; .NET has no raw accessor, so the epoch is
        // normalised once, here, and Equals, GetHashCode and ToString see it as Java's do.
        _leaderEpoch = leaderEpoch < 0 ? null : leaderEpoch;
    }

    /// <summary>
    /// Construct a new OffsetAndMetadata object for committing through <see cref="KafkaConsumer{K, V}"/>.
    /// </summary>
    /// <param name="offset">The offset to be committed.</param>
    /// <param name="metadata">Non-null metadata. A <see langword="null"/> is stored as the empty string.</param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="offset"/> is negative, with the message <c>"Invalid negative offset"</c>.
    /// </exception>
    public OffsetAndMetadata(long offset, string? metadata)
        : this(offset, null, metadata)
    {
    }

    /// <summary>
    /// Construct a new OffsetAndMetadata object for committing through <see cref="KafkaConsumer{K, V}"/>. The metadata
    /// associated with the commit will be empty.
    /// </summary>
    /// <param name="offset">The offset to be committed.</param>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="offset"/> is negative, with the message <c>"Invalid negative offset"</c>.
    /// </exception>
    public OffsetAndMetadata(long offset)
        : this(offset, string.Empty)
    {
    }

    /// <summary>The committed offset.</summary>
    /// <returns>The committed offset.</returns>
    public long Offset() => _offset;

    /// <summary>The commit metadata; the empty string when none was set, never <see langword="null"/>.</summary>
    /// <returns>The commit metadata; the empty string when none was set, never <see langword="null"/>.</returns>
    public string Metadata() => _metadata;

    /// <summary>
    /// The leader epoch of the record at <see cref="Offset()"/>, or <see langword="null"/> when absent (Java's
    /// <c>Optional&lt;Integer&gt; leaderEpoch()</c>). A negative epoch is absent.
    /// </summary>
    /// <returns>The leader epoch of the record at <see cref="Offset()"/>, or <see langword="null"/> when
    /// absent.</returns>
    public int? LeaderEpoch() => _leaderEpoch;

    /// <summary>
    /// Value equality over <see cref="Offset()"/>, <see cref="Metadata()"/> (ordinal) and <see cref="LeaderEpoch()"/>,
    /// as Java's <c>equals</c>.
    /// </summary>
    /// <param name="other">The instance to compare with.</param>
    /// <returns>Whether the two describe the same committed offset.</returns>
    public bool Equals(OffsetAndMetadata? other) =>
        other is not null
        && Offset() == other.Offset()
        && string.Equals(Metadata(), other.Metadata(), StringComparison.Ordinal)
        && LeaderEpoch() == other.LeaderEpoch();

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
            int hash = Offset().GetHashCode();
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Metadata());
            return (hash * 31) + (LeaderEpoch()?.GetHashCode() ?? 0);
        }
    }

    /// <summary>
    /// Returns <c>"OffsetAndMetadata{offset=..., leaderEpoch=..., metadata='...'}"</c>, Java's format.
    /// </summary>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "OffsetAndMetadata{{offset={0}, leaderEpoch={1}, metadata='{2}'}}",
            _offset,
            _leaderEpoch is null ? "null" : _leaderEpoch.Value.ToString(CultureInfo.InvariantCulture),
            _metadata);
}
