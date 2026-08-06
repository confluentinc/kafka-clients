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
/// A committed offset with its optional metadata and leader epoch — the .NET
/// realization of Java's <c>org.apache.kafka.clients.consumer.OffsetAndMetadata</c>.
/// A dictionary <em>value</em> in the result of <c>IAsyncConsumer.Committed(...)</c>;
/// the fields are owned copies read out of a borrowed (Category-4) ABI element of an
/// owned <c>OffsetMap_t</c>, which is destroyed after the copy-out (ffi-marshalling.md
/// §B2/§B3).
/// </summary>
/// <remarks>
/// A <c>sealed class</c> (not a <c>readonly struct</c>) — it is a result payload read
/// once and placed into a dictionary, never a hot-path map key, and it carries a
/// reference-type <see cref="Metadata"/>; a class matches the
/// <see cref="ConsumerGroupMetadata"/> precedent and avoids the <c>default(struct)</c>
/// footgun that <see cref="TopicPartition"/> (a struct only because it is a hot map key)
/// documents. Immutable.
/// </remarks>
public sealed class OffsetAndMetadata
{
    /// <summary>
    /// Initializes a new instance from the values copied out of the ABI element.
    /// </summary>
    /// <param name="offset">The committed offset.</param>
    /// <param name="metadata">
    /// The commit metadata (never <see langword="null"/>; the empty string when unset).
    /// </param>
    /// <param name="leaderEpoch">
    /// The leader epoch, or <see langword="null"/> when absent.
    /// </param>
    internal OffsetAndMetadata(long offset, string metadata, int? leaderEpoch)
    {
        Offset = offset;
        Metadata = metadata;
        LeaderEpoch = leaderEpoch;
    }

    /// <summary>The committed offset.</summary>
    public long Offset { get; }

    /// <summary>
    /// The commit metadata — an application-defined string associated with the commit.
    /// Never <see langword="null"/>: the empty string when no metadata was set (Java's
    /// <c>OffsetAndMetadata.metadata()</c> defaults to <c>""</c>, never null).
    /// </summary>
    public string Metadata { get; }

    /// <summary>
    /// The leader epoch of the record at <see cref="Offset"/>, or <see langword="null"/>
    /// when absent (Java's <c>Optional&lt;Integer&gt; leaderEpoch()</c>).
    /// </summary>
    public int? LeaderEpoch { get; }

    /// <summary>
    /// Returns a debug string of the form
    /// <c>"OffsetAndMetadata{offset=…, metadata='…', leaderEpoch=…}"</c>.
    /// </summary>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "OffsetAndMetadata{{offset={0}, metadata='{1}', leaderEpoch={2}}}",
            Offset,
            Metadata,
            LeaderEpoch is null ? "null" : LeaderEpoch.Value.ToString(CultureInfo.InvariantCulture));
}
