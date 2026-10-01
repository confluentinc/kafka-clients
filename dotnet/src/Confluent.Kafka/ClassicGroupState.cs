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

namespace Confluent.Kafka;

/// <summary>
/// The state of a classic group — the .NET realization of Java's
/// <c>org.apache.kafka.common.ClassicGroupState</c> (<c>ClassicGroupState.java:29-35</c>).
/// </summary>
/// <remarks>
/// <para>
/// All six Java constants ship, in Java's declaration order. <c>UNKNOWN</c> is a real
/// member of the set, not an error sentinel: a broker newer than this client can name a
/// state this client does not know, and Java's <c>parse</c> maps that to <c>UNKNOWN</c>
/// rather than failing (<c>:49-52</c>). Java's javadoc on the accessor that carries it
/// says exactly that — "the classic group state, or UNKNOWN if the state is too new for us
/// to parse".
/// </para>
/// <para>
/// ⚠ <b>Nothing here is deprecated, unlike <see cref="ConsumerGroupState"/>.</b> Java
/// carries no <c>@Deprecated</c> on this enum, and none on
/// <c>ClassicGroupDescription.state()</c>, which returns it. So this type takes no
/// <see cref="System.ObsoleteAttribute"/>, and no caller of it needs a <c>CS0618</c>
/// suppression — the two states look alike and are on opposite sides of that line.
/// </para>
/// <para>
/// ⚠ <b>This is a third axis, not a view of either of the other two.</b>
/// <c>ClassicGroupDescription.state()</c> is a <em>stored field</em>
/// (<c>ClassicGroupDescription.java:38</c>, and it participates in <c>equals</c> /
/// <c>hashCode</c> / <c>toString</c>), not a projection over a <c>groupState()</c>
/// accessor — the shape <c>ConsumerGroupListing</c> and <c>ConsumerGroupDescription</c>
/// use for <see cref="ConsumerGroupState"/>. Correspondingly <c>ClassicGroupDescription</c>
/// has <c>state()</c> and <b>no</b> <c>groupState()</c>. The asymmetry with the
/// consumer-group description is Java's; it is mirrored, not smoothed over.
/// </para>
/// <para>
/// ⚠ <b>Nor is it member-for-member equal to either.</b> <see cref="GroupState.NotReady"/>,
/// <c>Assigning</c> and <c>Reconciling</c> have no counterpart here, because Java's
/// <c>ClassicGroupState</c> never gained them. The three enums are therefore not
/// intercastable, and the underlying <c>int</c> of a member of one says nothing about the
/// others.
/// </para>
/// <para>
/// ⚠ <b>The name is the contract, not an ordinal.</b> Java's <c>ClassicGroupState</c>
/// carries no numeric id, so the value crossing the boundary is Java's <c>toString()</c>
/// spelling (<c>"Stable"</c>, <c>"PreparingRebalance"</c>, …; <c>:54-57</c>) — the ABI
/// accessor returns it as a borrowed <c>const char*</c>, documented as "the
/// <c>ClassicGroupState</c> name (borrowed)". It is read and written by
/// <c>Confluent.Kafka.Internal.Interop.GroupMarshal</c>. The underlying <c>int</c> of these
/// members is meaningless outside this assembly; do not persist it.
/// </para>
/// <para>
/// <b>Java's <c>parse(String)</c> is deliberately not published</b>, for the reason given on
/// <see cref="GroupType"/>, <see cref="GroupState"/> and <see cref="ConsumerGroupState"/>:
/// it decodes a wire spelling the caller never sees.
/// </para>
/// </remarks>
public enum ClassicGroupState
{
    /// <summary>
    /// The state is not known to this client — Java's <c>UNKNOWN</c> (<c>:30</c>), the
    /// value <c>parse</c> yields for a name it does not recognise.
    /// </summary>
    Unknown,

    /// <summary>Java's <c>PREPARING_REBALANCE</c> (<c>:31</c>).</summary>
    PreparingRebalance,

    /// <summary>Java's <c>COMPLETING_REBALANCE</c> (<c>:32</c>).</summary>
    CompletingRebalance,

    /// <summary>Java's <c>STABLE</c> (<c>:33</c>).</summary>
    Stable,

    /// <summary>Java's <c>DEAD</c> (<c>:34</c>).</summary>
    Dead,

    /// <summary>Java's <c>EMPTY</c> (<c>:35</c>).</summary>
    Empty,
}
