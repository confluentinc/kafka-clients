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
using System.Collections.Generic;

namespace Confluent.Kafka;

/// <summary>
/// The state of a group — the .NET realization of Java's
/// <c>org.apache.kafka.common.GroupState</c> (<c>GroupState.java:48-57</c>).
/// </summary>
/// <remarks>
/// <para>
/// All nine Java constants ship, in Java's declaration order. Not every state is reachable
/// for every <see cref="GroupType"/>; Java tabulates the correspondence in its class
/// javadoc (<c>:30-46</c>) and encodes it in
/// <see cref="GroupStateExtensions.GroupStatesForType"/>.
/// </para>
/// <para>
/// ⚠ <b>The name is the contract, not an ordinal.</b> Java's <c>GroupState</c> carries no
/// numeric id — the ABI header says so in as many words — so the value crossing the
/// boundary is Java's <c>toString()</c> spelling (<c>"Stable"</c>, <c>"NotReady"</c>, …),
/// read and written by <c>Confluent.Kafka.Internal.Interop.GroupMarshal</c>. The
/// underlying <c>int</c> of these members is therefore meaningless outside this assembly;
/// do not persist it.
/// </para>
/// <para>
/// ⚠ <b><see cref="ConsumerGroupState"/> is a separate, deprecated axis — not a subset
/// view of this one.</b> A group listing or description carries both, each read from its
/// own ABI entry point; neither is derived from the other.
/// </para>
/// <para>
/// <b>Java's <c>parse(String)</c> is deliberately not published</b>, for the reason given
/// on <see cref="GroupType"/>: it decodes a wire spelling the caller never sees.
/// </para>
/// </remarks>
public enum GroupState
{
    /// <summary>
    /// The state is not known to this client — Java's <c>UNKNOWN</c> (<c>:49</c>), the
    /// value <c>parse</c> yields for a name it does not recognise.
    /// </summary>
    Unknown,

    /// <summary>Java's <c>PREPARING_REBALANCE</c> (<c>:50</c>).</summary>
    PreparingRebalance,

    /// <summary>Java's <c>COMPLETING_REBALANCE</c> (<c>:51</c>).</summary>
    CompletingRebalance,

    /// <summary>Java's <c>STABLE</c> (<c>:52</c>).</summary>
    Stable,

    /// <summary>Java's <c>DEAD</c> (<c>:53</c>).</summary>
    Dead,

    /// <summary>Java's <c>EMPTY</c> (<c>:54</c>).</summary>
    Empty,

    /// <summary>Java's <c>ASSIGNING</c> (<c>:55</c>).</summary>
    Assigning,

    /// <summary>Java's <c>RECONCILING</c> (<c>:56</c>).</summary>
    Reconciling,

    /// <summary>Java's <c>NOT_READY</c> (<c>:57</c>).</summary>
    NotReady,
}

/// <summary>
/// Hosts Java's <c>GroupState.groupStatesForType(GroupType)</c> static
/// (<c>GroupState.java:76-88</c>), which a C# <see langword="enum"/> cannot declare itself.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>This is a named, deliberate exception to <c>bindings/CLAUDE.md</c> §2.6 ("shape,
/// not logic"), not a case that satisfies it.</b> The four sets below are Kafka knowledge
/// — which states each group type can be in — and they are hardcoded here rather than
/// asked of the Rust core, because the ABI exports nothing that answers the question: it
/// carries group states only as names attached to a listing or description. The exception
/// was ruled rather than assumed, and its cost is real: if Kafka adds a state to a type,
/// this table goes stale and only a Java-side diff will notice. Do not generalize from it
/// — a second copy of Kafka behavior in this binding needs its own ruling.
/// </para>
/// <para>
/// <b>Why an extension class rather than a member of the type.</b> Java's home for this
/// method is <c>GroupState</c>, and the binding keeps Java's name and parameter exactly.
/// But <see cref="GroupState"/> is a C# <see langword="enum"/> — required, so that the
/// nine constants are the type rather than fields on a class — and a C# enum cannot host
/// static members at all. An extension class is the only host that keeps the Java call
/// shape readable: <c>GroupType.Consumer.GroupStatesForType()</c> instance-style, or
/// <c>GroupStateExtensions.GroupStatesForType(GroupType.Consumer)</c> static-style, which
/// is Java's <c>GroupState.groupStatesForType(type)</c> modulo the host's name. It lives
/// in this file, beside the enum, because it is one Java class.
/// </para>
/// </remarks>
public static class GroupStateExtensions
{
    /// <summary>
    /// The states a group of <paramref name="type"/> can be in — Java's
    /// <c>groupStatesForType</c> (<c>GroupState.java:76-88</c>).
    /// </summary>
    /// <param name="type">The group type to tabulate.</param>
    /// <returns>
    /// A freshly minted collection of the states valid for <paramref name="type"/>. Minted
    /// per call, as Java's <c>Set.of(…)</c> is, so no caller can reach another caller's
    /// collection.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="type"/> is <see cref="GroupType.Unknown"/> or is not a defined
    /// <see cref="GroupType"/> — Java's <c>IllegalArgumentException("Group type not
    /// known")</c> (<c>:86</c>).
    /// </exception>
    /// <remarks>
    /// <para>
    /// ⚠ <b><see cref="GroupState.Unknown"/> is in none of the four sets, although Java's
    /// class javadoc table says otherwise</b> (<c>:36</c> marks <c>UNKNOWN</c> "Yes" for
    /// all four types). The code at <c>:78-85</c> omits it from every set, and the code is
    /// what callers observe; the table is mirrored on <see cref="GroupState"/> as Java's
    /// own documentation, not as this method's contract.
    /// </para>
    /// <para>
    /// ⚠ <b><see cref="GroupType.Unknown"/> throws</b> — it is a defined member of the
    /// enum but not a type with a known state set, so it falls into Java's <c>else</c>
    /// (<c>:85-87</c>) exactly as an unrecognised value does. Java's other throwing input,
    /// a <c>null</c> type, has no analogue here: <see cref="GroupType"/> is a
    /// non-nullable value type, and its <c>default</c> is
    /// <see cref="GroupType.Unknown"/>, which throws anyway.
    /// </para>
    /// </remarks>
    public static IReadOnlyCollection<GroupState> GroupStatesForType(this GroupType type)
    {
        switch (type)
        {
            case GroupType.Classic:
                return new HashSet<GroupState>
                {
                    GroupState.PreparingRebalance,
                    GroupState.CompletingRebalance,
                    GroupState.Stable,
                    GroupState.Dead,
                    GroupState.Empty,
                };

            case GroupType.Consumer:
                return new HashSet<GroupState>
                {
                    GroupState.PreparingRebalance,
                    GroupState.CompletingRebalance,
                    GroupState.Stable,
                    GroupState.Dead,
                    GroupState.Empty,
                    GroupState.Assigning,
                    GroupState.Reconciling,
                };

            case GroupType.Streams:
                return new HashSet<GroupState>
                {
                    GroupState.Stable,
                    GroupState.Dead,
                    GroupState.Empty,
                    GroupState.Assigning,
                    GroupState.Reconciling,
                    GroupState.NotReady,
                };

            case GroupType.Share:
                return new HashSet<GroupState>
                {
                    GroupState.Stable,
                    GroupState.Dead,
                    GroupState.Empty,
                };

            default:
                throw new ArgumentException("Group type not known", nameof(type));
        }
    }
}
