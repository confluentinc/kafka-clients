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

namespace Confluent.Kafka;

/// <summary>
/// A committed offset with its optional metadata and leader epoch — the .NET
/// realization of Java's <c>org.apache.kafka.clients.consumer.OffsetAndMetadata</c>.
/// Used both ways: as a dictionary <em>value</em> in the result of
/// <c>IAsyncConsumer.Committed(...)</c> (the fields are owned copies read out of a
/// borrowed (Category-4) ABI element of an owned <c>OffsetMap_t</c>, destroyed after
/// the copy-out — ffi-marshalling.md §B2/§B3), and as a user-constructed <em>input</em>
/// to <c>IAsyncConsumer.Commit(offsets, …)</c> (M5/P6) via the public constructor.
/// </summary>
/// <remarks>
/// <para>
/// A <c>sealed class</c> (not a <c>readonly struct</c>) — it is a result payload read
/// once and placed into a dictionary, never a hot-path map key, and it carries a
/// reference-type <see cref="Metadata"/>; a class matches the
/// <see cref="ConsumerGroupMetadata"/> precedent and avoids the <c>default(struct)</c>
/// footgun that <see cref="TopicPartition"/> (a struct only because it is a hot map key)
/// documents. Immutable.
/// </para>
/// <para>
/// <b>One constructor (M5/P6 deviation from the plan's "keep the internal ctor" note).</b>
/// The plan sketched a public <c>(long, string?, int?)</c> ctor <em>plus</em> the shipped
/// <c>internal (long, string, int?)</c> ctor, but C# treats <c>string</c> and
/// <c>string?</c> as the same parameter type for overload resolution, so the two would
/// collide (CS0111). Resolved by <b>collapsing to the single public constructor</b>: it is
/// the one field-assignment site (the plan's stated goal), and the receive-path marshaller
/// (<c>OffsetMapMarshal</c>) now calls it directly — safely, because the marshaller always
/// passes a non-null (normalized) metadata and a non-negative offset, for which the ctor's
/// validate/coerce is a no-op. Recorded in <c>COMMENTS.DONE.15.md</c>.
/// </para>
/// </remarks>
public sealed class OffsetAndMetadata
{
    /// <summary>
    /// Initializes a new instance for committing <paramref name="offset"/> (Java's
    /// <c>OffsetAndMetadata(long, Optional&lt;Integer&gt;, String)</c>) — the input form
    /// used by <c>IAsyncConsumer.Commit(offsets, …)</c> (M5/P6).
    /// </summary>
    /// <remarks>
    /// Mirrors Java's canonical constructor validation exactly (verified in
    /// <c>org.apache.kafka.clients.consumer.OffsetAndMetadata</c>): a negative
    /// <paramref name="offset"/> throws with the message <c>"Invalid negative offset"</c>
    /// (CLAUDE.md idiom map: Java's <c>IllegalArgumentException</c> on a bad numeric value →
    /// <see cref="ArgumentOutOfRangeException"/>, consistent with the shipped negative-offset
    /// precedent on <c>Seek</c>); a <see langword="null"/> <paramref name="metadata"/> is
    /// coerced to the empty string (Java's <c>Objects.requireNonNullElse(metadata,
    /// NO_METADATA)</c> where <c>NO_METADATA == ""</c>), keeping <see cref="Metadata"/>
    /// never-null. <paramref name="leaderEpoch"/> is passed through unvalidated (Java's
    /// <c>Optional&lt;Integer&gt;</c>; <see langword="null"/> = absent). The parameter order
    /// <c>(offset, metadata, leaderEpoch)</c> matches Java's most-used 2-arg
    /// <c>(offset, metadata)</c> overload and the Python sibling's
    /// <c>OffsetAndMetadata(offset, metadata="", leader_epoch=None)</c> exactly.
    /// </remarks>
    /// <param name="offset">The offset to commit (must be non-negative).</param>
    /// <param name="metadata">
    /// The commit metadata, or <see langword="null"/> (coerced to the empty string).
    /// </param>
    /// <param name="leaderEpoch">The leader epoch, or <see langword="null"/> when absent.</param>
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
        // Java's Objects.requireNonNullElse(metadata, NO_METADATA) where NO_METADATA == "":
        // Metadata stays never-null. The receive-path marshaller already passes a non-null
        // (normalized) metadata + a non-negative offset, so this validate/coerce is a no-op
        // for that path — see the type remarks (M5/P6 deviation on the ctor merge).
        Metadata = metadata ?? string.Empty;
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
