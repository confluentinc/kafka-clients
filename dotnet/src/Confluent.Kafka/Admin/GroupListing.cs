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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A listing of one group in the cluster — Java's
/// <c>org.apache.kafka.clients.admin.GroupListing</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's <c>Optional</c> becomes a C# nullable: <see langword="null"/> in <see cref="Type()"/> or
/// <see cref="GroupState()"/> is Java's <c>Optional.empty()</c>, for a broker that did not report
/// it. It differs from <see cref="GroupType.UNKNOWN"/> and <see cref="GroupState.UNKNOWN"/>, which
/// mean the broker named a value this client does not recognise, as in Java.
/// </para>
/// <para>
/// Java's <c>Objects.requireNonNull(type)</c> has no counterpart: it rejects a null
/// <c>Optional</c> reference, and <see langword="null"/> here is the empty case, which Java
/// accepts.
/// </para>
/// </remarks>
public sealed class GroupListing
{
    private readonly string _groupId;
    private readonly GroupType? _type;
    private readonly string _protocol;
    private readonly GroupState? _groupState;

    /// <summary>
    /// Creates a listing — Java's constructor.
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
    /// Both null checks are stricter than Java, which has neither: a recorded deviation that keeps
    /// <see cref="GroupId()"/> and <see cref="Protocol()"/> non-nullable, as on
    /// <see cref="TopicListing"/>.
    /// </remarks>
    public GroupListing(string groupId, GroupType? type, string protocol, GroupState? groupState)
    {
        _groupId = groupId ?? throw new ArgumentNullException(nameof(groupId));
        _type = type;
        _protocol = protocol ?? throw new ArgumentNullException(nameof(protocol));
        _groupState = groupState;
    }

    /// <summary>The group id — Java's <c>groupId()</c>.</summary>
    /// <returns>The group id.</returns>
    public string GroupId() => _groupId;

    /// <summary>
    /// The group type, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>type()</c>.
    /// </summary>
    /// <returns>The group type, or <see langword="null"/> when the broker did not report one.</returns>
    public GroupType? Type() => _type;

    /// <summary>The group protocol — Java's <c>protocol()</c>.</summary>
    /// <returns>The group protocol.</returns>
    public string Protocol() => _protocol;

    /// <summary>
    /// The group state, or <see langword="null"/> when the broker did not report one —
    /// Java's <c>groupState()</c>.
    /// </summary>
    /// <returns>The group state, or <see langword="null"/> when the broker did not report one.</returns>
    public GroupState? GroupState() => _groupState;

    /// <summary>
    /// Whether this is a simple consumer group — Java's <c>isSimpleConsumerGroup()</c>: a classic
    /// group with an empty protocol.
    /// </summary>
    /// <returns>Whether this is a simple consumer group — Java's <c>isSimpleConsumerGroup()</c>: a classic group
    /// with an empty protocol.</returns>
    /// <remarks>
    /// Derived from <see cref="Type()"/> and <see cref="Protocol()"/>, as Java derives it, so a listing
    /// the caller builds answers as one the client returns; the core's
    /// <c>kafka_admin_GroupListing_is_simple_consumer_group</c> is not used.
    /// </remarks>
    public bool IsSimpleConsumerGroup() => Type() == GroupType.CLASSIC && Protocol().Length == 0;

    /// <summary>
    /// Value equality over all four fields — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same listing.</returns>
    /// <remarks>
    /// The strings are compared ordinally, as Java's <c>Objects.equals</c> compares a
    /// <c>String</c>.
    /// </remarks>
    public override bool Equals(object? obj) =>
        obj is GroupListing other
        && string.Equals(GroupId(), other.GroupId(), StringComparison.Ordinal)
        && Type() == other.Type()
        && string.Equals(Protocol(), other.Protocol(), StringComparison.Ordinal)
        && GroupState() == other.GroupState();

    /// <summary>The hash of all four fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(_groupId);
            hash = (hash * 31) + (_type is null ? 0 : ((int)_type.Value).GetHashCode());
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(_protocol);
            hash = (hash * 31) + (_groupState is null ? 0 : ((int)_groupState.Value).GetHashCode());
            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// An absent type or state renders as <c>none</c>, as Java's <c>orElse("none")</c>.
    /// </remarks>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(groupId='{0}', type={1}, protocol='{2}', groupState={3})",
            _groupId,
            _type is null ? "none" : GroupMarshal.NameFromType(_type.Value) ?? _type.Value.ToString(),
            _protocol,
            _groupState is null ? "none" : GroupMarshal.NameFromState(_groupState.Value) ?? _groupState.Value.ToString());
}
