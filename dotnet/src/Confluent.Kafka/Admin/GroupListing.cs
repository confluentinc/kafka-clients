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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A listing of one group in the cluster — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.GroupListing</c>
/// (<c>GroupListing.java:12, :26, :38, :52, :61, :74, :81, :86, :96, :117</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Java's <c>Optional&lt;T&gt;</c> becomes a C# nullable.</b> <see cref="Type"/> and
/// <see cref="GroupState"/> are <c>Optional</c> in Java because an older broker may not
/// report them at all; <see langword="null"/> here is that <c>Optional.empty()</c>. It is
/// distinct from <see cref="GroupType.Unknown"/> / <see cref="GroupState.Unknown"/>, which
/// mean "the broker named something this client does not recognise" — Java draws the same
/// distinction, at length, at <c>:44-48</c> and <c>:67-71</c>.
/// </para>
/// <para>
/// ⚠ <b>Java's <c>Objects.requireNonNull(type)</c> (<c>:28</c>) has no analogue.</b> It
/// rejects a null <em>Optional reference</em>, not an empty one — and C# has no null
/// <c>Optional</c> to reject: <see langword="null"/> is precisely the empty case, which
/// Java accepts. So there is deliberately no guard on <see cref="Type"/>.
/// </para>
/// <para>
/// <b>Every accessor is a property although Java spells them as methods</b> — each is a
/// pure managed field read that does no P/Invoke and cannot throw (CLAUDE.md §3's
/// "non-blocking getter → sync property" row), the same reading
/// <see cref="ClientMetricsResourceListing.Name"/> and <see cref="TopicListing.Name"/>
/// already make.
/// </para>
/// </remarks>
public sealed class GroupListing
{
    /// <summary>
    /// Creates a listing — Java's
    /// <c>GroupListing(String, Optional&lt;GroupType&gt;, String, Optional&lt;GroupState&gt;)</c>
    /// (<c>:26</c>).
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="type">The group type, or <see langword="null"/> for Java's <c>Optional.empty()</c>.</param>
    /// <param name="protocol">
    /// The group protocol — the empty string for a classic group that is not using one.
    /// </param>
    /// <param name="groupState">The group state, or <see langword="null"/> for Java's <c>Optional.empty()</c>.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="groupId"/> or <paramref name="protocol"/> is null.
    /// </exception>
    /// <remarks>
    /// ⚠ The two null guards are <b>deliberately stricter than Java</b>, which guards
    /// neither. <see cref="GroupId"/> and <see cref="Protocol"/> are non-nullable
    /// <c>string</c> under <c>#nullable enable</c>, so accepting null would falsify the
    /// annotation — the call <see cref="ClientMetricsResourceListing"/> already makes. Java
    /// is not permissive here by design either: its own
    /// <see cref="IsSimpleConsumerGroup"/> dereferences <c>protocol</c> unguarded
    /// (<c>:82</c>), so a null protocol is a latent <c>NullPointerException</c> there.
    /// </remarks>
    public GroupListing(string groupId, GroupType? type, string protocol, GroupState? groupState)
    {
        GroupId = groupId ?? throw new ArgumentNullException(nameof(groupId));
        Type = type;
        Protocol = protocol ?? throw new ArgumentNullException(nameof(protocol));
        GroupState = groupState;
    }

    /// <summary>The group id — Java's <c>groupId()</c> (<c>:38</c>).</summary>
    public string GroupId { get; }

    /// <summary>
    /// The group type, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>type()</c> (<c>:52</c>).
    /// </summary>
    public GroupType? Type { get; }

    /// <summary>The group protocol — Java's <c>protocol()</c> (<c>:61</c>).</summary>
    public string Protocol { get; }

    /// <summary>
    /// The group state, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>groupState()</c> (<c>:74</c>).
    /// </summary>
    public GroupState? GroupState { get; }

    /// <summary>
    /// Whether this is a simple consumer group — Java's <c>isSimpleConsumerGroup()</c>
    /// (<c>:81</c>): a classic group with an empty protocol.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Derived from the two fields, not read from the ABI</b>, although
    /// <c>kafka_admin_GroupListing_is_simple_consumer_group</c> exists and answers the same
    /// question. Java derives it (<c>:82</c>) and its public constructor takes only the four
    /// fields, so a caller-built listing must derive it too — reading the native flag would
    /// make the two construction paths disagree, and would need a fifth constructor
    /// parameter Java does not have (<c>definition-of-done.md</c> §7). The ABI export is
    /// therefore deliberately left undeclared.
    /// </remarks>
    public bool IsSimpleConsumerGroup => Type == GroupType.Classic && Protocol.Length == 0;

    /// <summary>
    /// Value equality over all four fields — Java's <c>equals</c> (<c>:117</c>).
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same listing.</returns>
    /// <remarks>
    /// The strings are compared <b>ordinally</b>, which is what Java's
    /// <c>Objects.equals</c> on a <c>String</c> does and what the rest of this binding keys
    /// strings by.
    /// </remarks>
    public override bool Equals(object? obj) =>
        obj is GroupListing other
        && string.Equals(GroupId, other.GroupId, StringComparison.Ordinal)
        && Type == other.Type
        && string.Equals(Protocol, other.Protocol, StringComparison.Ordinal)
        && GroupState == other.GroupState;

    /// <summary>
    /// The hash of all four fields — Java's <c>Objects.hash(...)</c> (<c>:96</c>), whose
    /// 31-fold this reproduces.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(GroupId);
            hash = (hash * 31) + (Type is null ? 0 : ((int)Type.Value).GetHashCode());
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(Protocol);
            hash = (hash * 31) + (GroupState is null ? 0 : ((int)GroupState.Value).GetHashCode());
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:86</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// ⚠ The unbalanced quotes around <c>groupId</c> and <c>protocol</c> are <b>Java's
    /// own</b> (<c>:88, :90</c>): each opens with <c>'</c> and closes with <c>,</c> or
    /// <c>)</c>. Mirrored rather than corrected. An absent type or state renders as
    /// <c>none</c>, Java's <c>orElse("none")</c>.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(groupId='{0}, type={1}, protocol='{2}, groupState={3})",
            GroupId,
            Type?.ToString() ?? "none",
            Protocol,
            GroupState?.ToString() ?? "none");
}
