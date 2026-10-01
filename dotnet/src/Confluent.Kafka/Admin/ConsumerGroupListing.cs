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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A listing of one consumer group in the cluster — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ConsumerGroupListing</c>
/// (<c>ConsumerGroupListing.java:33, :45, :88, :104, :119, :126, :133, :142, :151, :156,
/// :166, :171</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The type itself is deprecated in Java</b>, not merely the RPC that produces it:
/// <c>@Deprecated(since = "4.1", forRemoval = true)</c> at <c>:32</c>, directing callers to
/// <see cref="IAdmin.ListGroups"/> and <see cref="GroupListing"/>. So it carries
/// <see cref="ObsoleteAttribute"/> here — <b>mirrored, not avoided</b>, the same call
/// <see cref="ClientMetricsResourceListing"/> and <see cref="ConsumerGroupState"/> already
/// make. The attribute is a <b>warning</b>, not an error, although Java says
/// <c>forRemoval = true</c>: <c>forRemoval</c> raises javac's removal-warning category, it
/// does not reject the call, and <c>error: true</c> would make the type uncallable —
/// stricter than Java, and self-defeating for a surface whose whole job is to be callable.
/// </para>
/// <para>
/// ⚠ <b>Java's five deprecated-and-current constructors ship as three.</b> The two Java
/// constructors taking <c>Optional&lt;ConsumerGroupState&gt;</c> (<c>:58</c>, <c>:72</c>)
/// are <b>deliberately not translated</b> — a standing ruling for this phase. They are
/// themselves <c>@Deprecated</c> <em>within</em> an already-deprecated class, and each is a
/// pure forwarder that maps its argument through <c>GroupState.parse</c> before calling
/// <c>:104</c>; a caller holding a <see cref="ConsumerGroupState"/> can perform that same
/// map and use <c>:104</c>'s translation directly. Omitting them loses no expressible
/// value. The three current constructors ship in full.
/// </para>
/// <para>
/// <b>Java's <c>Optional&lt;T&gt;</c> becomes a C# nullable.</b> <see cref="GroupState"/>
/// and <see cref="Type"/> are <c>Optional</c> in Java because an older broker may not
/// report them at all; <see langword="null"/> here is that <c>Optional.empty()</c>. It is
/// distinct from <see cref="Confluent.Kafka.GroupState.Unknown"/> /
/// <see cref="GroupType.Unknown"/>, which mean "the broker named something this client does
/// not recognise" — the distinction <see cref="GroupListing"/> draws at length, for the
/// same two fields.
/// </para>
/// <para>
/// ⚠ <b>Java's <c>Objects.requireNonNull(groupState)</c> / <c>(type)</c> (<c>:111-112</c>)
/// have no analogue.</b> They reject a null <em>Optional reference</em>, not an empty one —
/// and C# has no null <c>Optional</c> to reject: <see langword="null"/> is precisely the
/// empty case, which Java accepts. So there is deliberately no guard on either.
/// </para>
/// <para>
/// ⚠ <b><see cref="IsSimpleConsumerGroup"/> is a stored field here, unlike its namesake on
/// <see cref="GroupListing"/>, which derives it.</b> The two classes genuinely differ:
/// Java's <c>GroupListing</c> computes the answer from its type and protocol
/// (<c>GroupListing.java:82</c>), whereas Java's <c>ConsumerGroupListing</c> stores what the
/// caller passed (<c>:35</c>, <c>:113</c>) and has no protocol to derive from. Mirrored
/// as-is in both.
/// </para>
/// <para>
/// <b>Every accessor is a property although Java spells them as methods</b> — each is a
/// pure managed read that does no P/Invoke and cannot throw (CLAUDE.md §3's "non-blocking
/// getter → sync property" row), the same reading <see cref="GroupListing.GroupId"/> and
/// <see cref="ClientMetricsResourceListing.Name"/> already make.
/// </para>
/// </remarks>
[Obsolete(
    "Deprecated in Kafka since 4.1. Use IAdmin.ListGroups and GroupListing instead.")]
public sealed class ConsumerGroupListing
{
    /// <summary>
    /// Creates a listing with neither state nor type — Java's
    /// <c>ConsumerGroupListing(String, boolean)</c> (<c>:45</c>), which forwards two
    /// <c>Optional.empty()</c> to the canonical constructor.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="isSimpleConsumerGroup">Whether the group is a simple consumer group.</param>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    public ConsumerGroupListing(string groupId, bool isSimpleConsumerGroup)
        : this(groupId, null, null, isSimpleConsumerGroup)
    {
    }

    /// <summary>
    /// Creates a listing with a state but no type — Java's
    /// <c>ConsumerGroupListing(String, Optional&lt;GroupState&gt;, boolean)</c>
    /// (<c>:88</c>), which forwards <c>Optional.empty()</c> for the type.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="groupState">
    /// The group state, or <see langword="null"/> for Java's <c>Optional.empty()</c>.
    /// </param>
    /// <param name="isSimpleConsumerGroup">Whether the group is a simple consumer group.</param>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    public ConsumerGroupListing(string groupId, GroupState? groupState, bool isSimpleConsumerGroup)
        : this(groupId, groupState, null, isSimpleConsumerGroup)
    {
    }

    /// <summary>
    /// Creates a listing — Java's canonical
    /// <c>ConsumerGroupListing(String, Optional&lt;GroupState&gt;, Optional&lt;GroupType&gt;,
    /// boolean)</c> (<c>:104</c>), which the other two delegate to.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="groupState">
    /// The group state, or <see langword="null"/> for Java's <c>Optional.empty()</c>.
    /// </param>
    /// <param name="type">
    /// The group type, or <see langword="null"/> for Java's <c>Optional.empty()</c>.
    /// </param>
    /// <param name="isSimpleConsumerGroup">Whether the group is a simple consumer group.</param>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is null.</exception>
    /// <remarks>
    /// ⚠ The <paramref name="groupId"/> guard is <b>deliberately stricter than Java</b>,
    /// which does not check it (<c>:110</c> assigns it raw). <see cref="GroupId"/> is a
    /// non-nullable <c>string</c> under <c>#nullable enable</c>, so accepting null would
    /// falsify the annotation — the same call <see cref="GroupListing"/> and
    /// <see cref="TopicListing"/> already make.
    /// </remarks>
    public ConsumerGroupListing(
        string groupId,
        GroupState? groupState,
        GroupType? type,
        bool isSimpleConsumerGroup)
    {
        GroupId = groupId ?? throw new ArgumentNullException(nameof(groupId));
        GroupState = groupState;
        Type = type;
        IsSimpleConsumerGroup = isSimpleConsumerGroup;
    }

    /// <summary>The group id — Java's <c>groupId()</c> (<c>:119</c>).</summary>
    public string GroupId { get; }

    /// <summary>
    /// Whether the group is a simple consumer group — Java's
    /// <c>isSimpleConsumerGroup()</c> (<c>:126</c>).
    /// </summary>
    public bool IsSimpleConsumerGroup { get; }

    /// <summary>
    /// The group state, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>groupState()</c> (<c>:133</c>).
    /// </summary>
    public GroupState? GroupState { get; }

    /// <summary>
    /// The group type, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>type()</c> (<c>:151</c>).
    /// </summary>
    public GroupType? Type { get; }

    /// <summary>
    /// The group state on the superseded <see cref="Confluent.Kafka.ConsumerGroupState"/>
    /// axis, or <see langword="null"/> when the broker did not report one — Java's
    /// deprecated <c>state()</c> (<c>:142</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>This is a lossy, one-way <em>projection</em> of <see cref="GroupState"/>, not a
    /// second stored field</b>, exactly as Java computes it at <c>:143</c>:
    /// <c>groupState.map(state0 -&gt; ConsumerGroupState.parse(state0.toString()))</c>. The
    /// C ABI agrees in as many words —
    /// <c>kafka_admin_ConsumerGroupListing_state</c> is documented as "Java's deprecated
    /// <c>state()</c>, which maps <c>groupState()</c> through
    /// <c>ConsumerGroupState.parse(...)</c>; the two therefore differ only for the group
    /// states <c>ConsumerGroupState</c> does not model". So the two accessors the ABI
    /// exports are one axis viewed through two enums, <b>not</b> two independently settable
    /// values: C cannot express Java's <c>.map()</c>, so the flattened boundary spells the
    /// projection as a second entry point.
    /// </para>
    /// <para>
    /// <b>The two therefore agree everywhere except one member, where they must not.</b>
    /// <see cref="Confluent.Kafka.GroupState.NotReady"/> has no counterpart on
    /// <see cref="Confluent.Kafka.ConsumerGroupState"/> (Java never added one), so it
    /// projects to <see cref="Confluent.Kafka.ConsumerGroupState.Unknown"/> — Java's
    /// <c>parse</c> answering <c>UNKNOWN</c> for a name it does not recognise
    /// (<c>ConsumerGroupState.java:53-56</c>). That is why this member is kept rather than
    /// collapsed into <see cref="GroupState"/>: the projection loses information and cannot
    /// be run backwards.
    /// </para>
    /// <para>
    /// Routing through <c>GroupMarshal</c> — Java's <c>toString()</c> then its
    /// <c>parse</c>, composed — keeps the name table in one place rather than adding a
    /// second mapping here that could drift from the one the ABI path already uses. A
    /// <see cref="GroupState"/> value no member defines names nothing this client knows, so
    /// it takes the same <c>Unknown</c> answer.
    /// </para>
    /// </remarks>
    [Obsolete("Deprecated in Kafka since 4.0. Use GroupState instead.")]
    public ConsumerGroupState? State =>
        GroupState is null
            ? null
            : GroupMarshal.ParseConsumerState(GroupMarshal.NameFromState(GroupState.Value) ?? "Unknown");

    /// <summary>
    /// Value equality over the four stored members — Java's <c>equals</c> (<c>:171</c>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same listing.</returns>
    /// <remarks>
    /// <para>
    /// <see cref="State"/> is deliberately absent, as it is from Java's own <c>equals</c>
    /// (<c>:175-178</c>): it is derived from <see cref="GroupState"/>, so including it
    /// would state the same fact twice.
    /// </para>
    /// <para>
    /// The id is compared <b>ordinally</b>, which is what Java's <c>Objects.equals</c> on a
    /// <c>String</c> does and what the rest of this binding keys strings by.
    /// </para>
    /// </remarks>
    public override bool Equals(object? obj) =>
        obj is ConsumerGroupListing other
        && IsSimpleConsumerGroup == other.IsSimpleConsumerGroup
        && string.Equals(GroupId, other.GroupId, StringComparison.Ordinal)
        && GroupState == other.GroupState
        && Type == other.Type;

    /// <summary>
    /// The hash of the same four members — Java's <c>Objects.hash(...)</c> (<c>:167</c>),
    /// whose 31-fold this reproduces in Java's argument order.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(GroupId);
            hash = (hash * 31) + IsSimpleConsumerGroup.GetHashCode();
            hash = (hash * 31) + (GroupState is null ? 0 : ((int)GroupState.Value).GetHashCode());
            hash = (hash * 31) + (Type is null ? 0 : ((int)Type.Value).GetHashCode());
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:156</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>The quotes around the id are balanced here, unlike on
    /// <see cref="GroupListing"/> and <see cref="ClientMetricsResourceListing"/>.</b> Java
    /// closes this one — <c>:158</c> appends the <c>char</c> literal <c>'\''</c> after the
    /// id, which those two classes omit. Worth stating because it looks like the same
    /// rendering and is not: both spellings are mirrored from their own Java, neither is
    /// normalized toward the other.
    /// </para>
    /// <para>
    /// ⚠ <b>An absent state or type renders as <c>Optional.empty</c>, and a present one as
    /// <c>Optional[Stable]</c> — not as the <c>none</c> that <see cref="GroupListing"/>
    /// emits.</b> The two classes differ in Java, and this mirrors each: Java's
    /// <c>GroupListing.toString()</c> chooses the literal itself, via
    /// <c>orElse("none")</c>, whereas Java's <c>ConsumerGroupListing.toString()</c>
    /// concatenates the <c>Optional</c> objects raw (<c>:160-161</c>) and so inherits
    /// <c>Optional.toString()</c>. Reproducing that spelling — Java type name and all —
    /// keeps the rendering Java specifies, which is the rule this binding has applied to
    /// <c>toString()</c> throughout; correcting it here would silently make a diagnostic
    /// string that no longer matches the Java client's.
    /// </para>
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(groupId='{0}', isSimpleConsumerGroup={1}, groupState={2}, type={3})",
            GroupId,
            IsSimpleConsumerGroup ? "true" : "false",
            RenderOptional(GroupState is null ? null : GroupMarshal.NameFromState(GroupState.Value) ?? "Unknown"),
            RenderOptional(Type is null ? null : GroupMarshal.NameFromType(Type.Value) ?? "Unknown"));

    /// <summary>
    /// Renders one value the way Java's <c>Optional.toString()</c> does —
    /// <c>Optional[X]</c> for a present value, <c>Optional.empty</c> for an absent one.
    /// </summary>
    /// <param name="name">The present value's Java <c>toString()</c> spelling, or null.</param>
    /// <remarks>
    /// A present-but-undefined enum value has no Java spelling; it renders through
    /// <c>Unknown</c>, the answer the name tables give it everywhere else.
    /// </remarks>
    private static string RenderOptional(string? name) =>
        name is null
            ? "Optional.empty"
            : string.Format(CultureInfo.InvariantCulture, "Optional[{0}]", name);
}
