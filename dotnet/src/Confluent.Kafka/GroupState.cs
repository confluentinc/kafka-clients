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
/// The state of a group — Java's <c>org.apache.kafka.common.GroupState</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's nine constants, in Java's order. Not every state is reachable for every
/// <see cref="GroupType"/>: see <see cref="GroupStateExtensions.GroupStatesForType"/>.
/// </para>
/// <para>
/// The value crossing the C ABI is Java's <c>toString()</c> spelling (<c>"Stable"</c>,
/// <c>"NotReady"</c>, …), not the underlying <c>int</c>, which means nothing outside this
/// assembly; do not persist it.
/// </para>
/// <para>
/// Java's <c>parse(String)</c> is not published (a recorded deviation): it decodes a wire
/// spelling the caller never sees.
/// </para>
/// </remarks>
public enum GroupState
{
    /// <summary>
    /// The state is not known to this client — Java's <c>UNKNOWN</c>, the state for a name this
    /// client does not recognise.
    /// </summary>
    Unknown,

    /// <summary>Java's <c>PREPARING_REBALANCE</c>.</summary>
    PreparingRebalance,

    /// <summary>Java's <c>COMPLETING_REBALANCE</c>.</summary>
    CompletingRebalance,

    /// <summary>Java's <c>STABLE</c>.</summary>
    Stable,

    /// <summary>Java's <c>DEAD</c>.</summary>
    Dead,

    /// <summary>Java's <c>EMPTY</c>.</summary>
    Empty,

    /// <summary>Java's <c>ASSIGNING</c>.</summary>
    Assigning,

    /// <summary>Java's <c>RECONCILING</c>.</summary>
    Reconciling,

    /// <summary>Java's <c>NOT_READY</c>.</summary>
    NotReady,
}

/// <summary>
/// Hosts Java's <c>GroupState.groupStatesForType(GroupType)</c>, which a C#
/// <see langword="enum"/> cannot declare itself.
/// </summary>
/// <remarks>
/// A recorded deviation from "shape, not logic": the state sets are Kafka knowledge kept here,
/// because the C ABI has nothing that answers the question. They go stale if Kafka adds a state
/// to a type.
/// </remarks>
public static class GroupStateExtensions
{
    /// <summary>
    /// The states a group of <paramref name="type"/> can be in — Java's
    /// <c>groupStatesForType</c>.
    /// </summary>
    /// <param name="type">The group type to tabulate.</param>
    /// <returns>A new collection on each call, as Java's <c>Set.of(…)</c>.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="type"/> is <see cref="GroupType.Unknown"/> or is not a defined
    /// <see cref="GroupType"/>, with Java's message "Group type not known".
    /// </exception>
    /// <remarks>
    /// <see cref="GroupState.Unknown"/> is in none of the sets, as in Java's code.
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
