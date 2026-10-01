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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Converts between the group enums and the <b>names</b> the ABI speaks. None of
/// <see cref="GroupType"/>, <see cref="GroupState"/>, <c>ConsumerGroupState</c> or
/// <see cref="ClassicGroupState"/> has a numeric id in Java — the header states this at
/// every accessor — so Java's <c>toString()</c> spelling is the wire value in both
/// directions.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The tables below are spelled out rather than derived from
/// <see cref="Enum.ToString()"/>, although the two agree today.</b> Every C# member name
/// here happens to equal Java's <c>toString()</c> value, so <c>ToString()</c> would
/// "work" — until a member is renamed for C# style, or a trimmer strips enum metadata and
/// <c>ToString()</c> starts yielding the number. An explicit switch makes the mapping a
/// reviewable fact and makes a rename a compile error rather than a wire change.
/// </para>
/// <para>
/// ⚠ <b>A name this client does not recognise decodes to <c>Unknown</c>, never to a
/// failure.</b> That is Java's <c>parse</c> in all four classes
/// (<c>GroupType.java:44-50</c>, <c>GroupState.java:71-74</c>,
/// <c>ConsumerGroupState.java:53-56</c>, <c>ClassicGroupState.java:49-52</c>) and it is
/// load-bearing: a newer broker naming a state this client has never heard of must still
/// produce a listing — Java's <c>ClassicGroupDescription.state()</c> javadoc says so in as
/// many words ("or UNKNOWN if the state is too new for us to parse"). Matching is
/// case-insensitive, as Java's lookup maps are (<c>GroupType.java:32-33</c> lower-cases,
/// <c>GroupState.java:59-60</c>, <c>ConsumerGroupState.java:41-42</c> and
/// <c>ClassicGroupState.java:37-38</c> upper-case — the same relation) and as the header
/// promises.
/// </para>
/// <para>
/// ⚠ <b>A recognised name and an absent one are different answers.</b> Every
/// <c>*FromName</c> below returns <see langword="null"/> — Java's <c>Optional.empty()</c> —
/// only for a <b>null pointer</b>. A non-null pointer always yields a value, falling back
/// to <c>Unknown</c>. Collapsing the two would make "the broker did not report a state"
/// indistinguishable from "it reported one I cannot name".
/// </para>
/// <para>
/// <b>The encode direction is total in Java and partial here.</b> Java cannot hold an
/// enum value outside its constants; a C# cast can. So
/// <see cref="NameFromType(GroupType)"/> / <see cref="NameFromState(GroupState)"/> /
/// <c>NameFromConsumerState</c> / <see cref="NameFromClassicState(ClassicGroupState)"/>
/// return <see langword="null"/> for an undefined value and leave the throw to the caller,
/// which knows the parameter name to blame. Validation happens before any native call
/// (ffi-marshalling.md §B5).
/// </para>
/// <para>
/// ⚠ <b>The <c>CS0618</c> suppressions below cover the <c>ConsumerGroupState</c> members
/// only.</b> Java deprecates that enum and does <em>not</em> deprecate
/// <see cref="ClassicGroupState"/> (nor <c>ClassicGroupDescription.state()</c>, which
/// returns it), so the classic members sit outside every suppressed region. A suppression
/// widened to cover them would silence a warning that cannot arise and would misstate the
/// Java contract.
/// </para>
/// <para>
/// <b>Member naming drops the shared <c>Group</c>, which the class already carries:</b>
/// <c>GroupType</c> → <c>…Type…</c>, <c>GroupState</c> → <c>…State…</c>,
/// <c>ConsumerGroupState</c> → <c>…ConsumerState…</c>, <c>ClassicGroupState</c> →
/// <c>…ClassicState…</c>.
/// </para>
/// </remarks>
internal static class GroupMarshal
{
    /// <summary>
    /// Decodes a borrowed, NUL-terminated <c>GroupType</c> name into
    /// <see cref="GroupType"/>, or <see langword="null"/> when the ABI reports Java's
    /// <c>Optional.empty()</c> as a null pointer.
    /// </summary>
    /// <param name="name">The borrowed name pointer, read before its root is destroyed.</param>
    internal static GroupType? TypeFromName(IntPtr name)
    {
        string? text = Utf8Marshal.PtrToString(name);
        return text is null ? null : ParseType(text);
    }

    /// <summary>
    /// Decodes a borrowed, NUL-terminated <c>GroupState</c> name into
    /// <see cref="GroupState"/>, or <see langword="null"/> when the ABI reports Java's
    /// <c>Optional.empty()</c> as a null pointer.
    /// </summary>
    /// <param name="name">The borrowed name pointer, read before its root is destroyed.</param>
    internal static GroupState? StateFromName(IntPtr name)
    {
        string? text = Utf8Marshal.PtrToString(name);
        return text is null ? null : ParseState(text);
    }

#pragma warning disable CS0618 // Java deprecates the state enum itself; mirrored, not avoided.

    /// <summary>
    /// Decodes a borrowed, NUL-terminated <c>ConsumerGroupState</c> name into
    /// <see cref="ConsumerGroupState"/>, or <see langword="null"/> when the ABI reports
    /// Java's <c>Optional.empty()</c> as a null pointer.
    /// </summary>
    /// <param name="name">The borrowed name pointer, read before its root is destroyed.</param>
    internal static ConsumerGroupState? ConsumerStateFromName(IntPtr name)
    {
        string? text = Utf8Marshal.PtrToString(name);
        return text is null ? null : ParseConsumerState(text);
    }

#pragma warning restore CS0618

    /// <summary>
    /// Decodes a borrowed, NUL-terminated <c>ClassicGroupState</c> name into
    /// <see cref="ClassicGroupState"/>, or <see langword="null"/> when the ABI reports
    /// Java's <c>Optional.empty()</c> as a null pointer.
    /// </summary>
    /// <param name="name">The borrowed name pointer, read before its root is destroyed.</param>
    /// <remarks>
    /// ⚠ <b>No <c>CS0618</c> suppression here, unlike its
    /// <see cref="ConsumerStateFromName(IntPtr)"/> neighbour.</b> Java deprecates
    /// <c>ConsumerGroupState</c> and does not deprecate <c>ClassicGroupState</c>, so
    /// suppressing around this member would silence a warning that cannot arise and would
    /// misstate the Java contract.
    /// </remarks>
    internal static ClassicGroupState? ClassicStateFromName(IntPtr name)
    {
        string? text = Utf8Marshal.PtrToString(name);
        return text is null ? null : ParseClassicState(text);
    }

    /// <summary>
    /// Java's <c>GroupType.toString()</c> for a defined member, or <see langword="null"/>
    /// for a value no member defines.
    /// </summary>
    /// <param name="type">The type to encode.</param>
    internal static string? NameFromType(GroupType type) =>
        type switch
        {
            GroupType.Unknown => "Unknown",
            GroupType.Consumer => "Consumer",
            GroupType.Classic => "Classic",
            GroupType.Share => "Share",
            GroupType.Streams => "Streams",
            _ => null,
        };

    /// <summary>
    /// Java's <c>GroupState.toString()</c> for a defined member, or <see langword="null"/>
    /// for a value no member defines.
    /// </summary>
    /// <param name="state">The state to encode.</param>
    internal static string? NameFromState(GroupState state) =>
        state switch
        {
            GroupState.Unknown => "Unknown",
            GroupState.PreparingRebalance => "PreparingRebalance",
            GroupState.CompletingRebalance => "CompletingRebalance",
            GroupState.Stable => "Stable",
            GroupState.Dead => "Dead",
            GroupState.Empty => "Empty",
            GroupState.Assigning => "Assigning",
            GroupState.Reconciling => "Reconciling",
            GroupState.NotReady => "NotReady",
            _ => null,
        };

#pragma warning disable CS0618 // Java deprecates the state enum itself; mirrored, not avoided.

    /// <summary>
    /// Java's <c>ConsumerGroupState.toString()</c> (<c>:58-61</c>) for a defined member, or
    /// <see langword="null"/> for a value no member defines.
    /// </summary>
    /// <param name="state">The state to encode.</param>
    /// <remarks>
    /// ⚠ <b>There is deliberately no <c>NotReady</c> arm.</b> Java's
    /// <c>ConsumerGroupState</c> has no such constant — only <see cref="GroupState"/> does
    /// (<c>GroupState.java:57</c>). The two enums are separate axes, not intercastable.
    /// </remarks>
    internal static string? NameFromConsumerState(ConsumerGroupState state) =>
        state switch
        {
            ConsumerGroupState.Unknown => "Unknown",
            ConsumerGroupState.PreparingRebalance => "PreparingRebalance",
            ConsumerGroupState.CompletingRebalance => "CompletingRebalance",
            ConsumerGroupState.Stable => "Stable",
            ConsumerGroupState.Dead => "Dead",
            ConsumerGroupState.Empty => "Empty",
            ConsumerGroupState.Assigning => "Assigning",
            ConsumerGroupState.Reconciling => "Reconciling",
            _ => null,
        };

#pragma warning restore CS0618

    /// <summary>
    /// Java's <c>ClassicGroupState.toString()</c> (<c>ClassicGroupState.java:54-57</c>) for
    /// a defined member, or <see langword="null"/> for a value no member defines.
    /// </summary>
    /// <param name="state">The state to encode.</param>
    /// <remarks>
    /// ⚠ <b>There are deliberately no <c>Assigning</c>, <c>Reconciling</c> or
    /// <c>NotReady</c> arms.</b> Java's <c>ClassicGroupState</c> has none of those
    /// constants — the first two belong to <see cref="GroupState"/> and
    /// <see cref="ConsumerGroupState"/>, the third to <see cref="GroupState"/> alone
    /// (<c>GroupState.java:57</c>). The three enums are separate axes, not intercastable.
    /// </remarks>
    internal static string? NameFromClassicState(ClassicGroupState state) =>
        state switch
        {
            ClassicGroupState.Unknown => "Unknown",
            ClassicGroupState.PreparingRebalance => "PreparingRebalance",
            ClassicGroupState.CompletingRebalance => "CompletingRebalance",
            ClassicGroupState.Stable => "Stable",
            ClassicGroupState.Dead => "Dead",
            ClassicGroupState.Empty => "Empty",
            _ => null,
        };

    /// <summary>Java's <c>GroupType.parse</c> (<c>GroupType.java:44-50</c>).</summary>
    /// <param name="name">A non-null type name in any casing.</param>
    private static GroupType ParseType(string name) =>
        name.ToUpperInvariant() switch
        {
            "CONSUMER" => GroupType.Consumer,
            "CLASSIC" => GroupType.Classic,
            "SHARE" => GroupType.Share,
            "STREAMS" => GroupType.Streams,
            _ => GroupType.Unknown,
        };

    /// <summary>Java's <c>GroupState.parse</c> (<c>GroupState.java:71-74</c>).</summary>
    /// <param name="name">A non-null state name in any casing.</param>
    /// <remarks>
    /// ⚠ <b>This one is <c>internal</c> where <see cref="ParseType(string)"/> is
    /// <c>private</c>, for the reason <see cref="ParseConsumerState(string)"/> is:</b> it
    /// has a second caller outside the decode path.
    /// <c>Confluent.Kafka.Admin.ListConsumerGroupsOptions.States</c>'s <em>setter</em>
    /// composes it with <see cref="NameFromConsumerState(ConsumerGroupState)"/> to reproduce
    /// Java's <c>states.stream().map(state -&gt; GroupState.parse(state.toString()))</c>
    /// (<c>ListConsumerGroupsOptions.java:60</c>) — the inverse of the projection
    /// <see cref="ParseConsumerState(string)"/> serves. Sharing this table is the point: the
    /// two directions must agree, and a second mapping written there could drift from the
    /// one the ABI path uses.
    /// </remarks>
    internal static GroupState ParseState(string name) =>
        name.ToUpperInvariant() switch
        {
            "PREPARINGREBALANCE" => GroupState.PreparingRebalance,
            "COMPLETINGREBALANCE" => GroupState.CompletingRebalance,
            "STABLE" => GroupState.Stable,
            "DEAD" => GroupState.Dead,
            "EMPTY" => GroupState.Empty,
            "ASSIGNING" => GroupState.Assigning,
            "RECONCILING" => GroupState.Reconciling,
            "NOTREADY" => GroupState.NotReady,
            _ => GroupState.Unknown,
        };

#pragma warning disable CS0618 // Java deprecates the state enum itself; mirrored, not avoided.

    /// <summary>
    /// Java's <c>ConsumerGroupState.parse</c> (<c>ConsumerGroupState.java:53-56</c>).
    /// </summary>
    /// <param name="name">A non-null state name in any casing.</param>
    /// <remarks>
    /// <para>
    /// <c>"UNKNOWN"</c> has no arm of its own, as in
    /// <see cref="ParseState(string)"/>: it is what the default yields, which is also what
    /// Java's map lookup yields for it. An explicit arm would be a second spelling of the
    /// same answer.
    /// </para>
    /// <para>
    /// ⚠ <b>This one is <c>internal</c> where <see cref="ParseType(string)"/> is
    /// <c>private</c>, because it has callers outside the decode path:</b>
    /// <c>Confluent.Kafka.Admin.ConsumerGroupListing.State</c> and
    /// <c>Confluent.Kafka.Admin.ListConsumerGroupsOptions.States</c>'s <em>getter</em> each
    /// compose it with <see cref="NameFromState(GroupState)"/> to reproduce Java's
    /// <c>groupState.map(state0 -&gt; ConsumerGroupState.parse(state0.toString()))</c>
    /// (<c>ConsumerGroupListing.java:143</c>) and its set-valued twin
    /// (<c>ListConsumerGroupsOptions.java:86</c>). Sharing this table is the point — a
    /// second mapping written there could drift from the one the ABI path uses.
    /// </para>
    /// </remarks>
    internal static ConsumerGroupState ParseConsumerState(string name) =>
        name.ToUpperInvariant() switch
        {
            "PREPARINGREBALANCE" => ConsumerGroupState.PreparingRebalance,
            "COMPLETINGREBALANCE" => ConsumerGroupState.CompletingRebalance,
            "STABLE" => ConsumerGroupState.Stable,
            "DEAD" => ConsumerGroupState.Dead,
            "EMPTY" => ConsumerGroupState.Empty,
            "ASSIGNING" => ConsumerGroupState.Assigning,
            "RECONCILING" => ConsumerGroupState.Reconciling,
            _ => ConsumerGroupState.Unknown,
        };

#pragma warning restore CS0618

    /// <summary>
    /// Java's <c>ClassicGroupState.parse</c> (<c>ClassicGroupState.java:49-52</c>).
    /// </summary>
    /// <param name="name">A non-null state name in any casing.</param>
    /// <remarks>
    /// <para>
    /// <c>"UNKNOWN"</c> has no arm of its own, as in <see cref="ParseState(string)"/>: it is
    /// what the default yields, which is also what Java's map lookup yields for it. An
    /// explicit arm would be a second spelling of the same answer.
    /// </para>
    /// <para>
    /// ⚠ <b>This one is <c>private</c>, as <see cref="ParseType(string)"/> is and unlike
    /// <see cref="ParseState(string)"/> / <see cref="ParseConsumerState(string)"/>:</b> it
    /// has no caller outside the decode path. <c>ClassicGroupDescription.state()</c> is a
    /// <em>stored field</em> (<c>ClassicGroupDescription.java:38</c>), not a projection over
    /// another accessor, so there is no second direction to reproduce — the
    /// <c>parse(other.toString())</c> composition the two <c>internal</c> siblings exist to
    /// share has no counterpart here.
    /// </para>
    /// </remarks>
    private static ClassicGroupState ParseClassicState(string name) =>
        name.ToUpperInvariant() switch
        {
            "PREPARINGREBALANCE" => ClassicGroupState.PreparingRebalance,
            "COMPLETINGREBALANCE" => ClassicGroupState.CompletingRebalance,
            "STABLE" => ClassicGroupState.Stable,
            "DEAD" => ClassicGroupState.Dead,
            "EMPTY" => ClassicGroupState.Empty,
            _ => ClassicGroupState.Unknown,
        };
}
