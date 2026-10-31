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

namespace Confluent.Kafka.Common;

/// <summary>
/// The state of a classic group — Java's <c>org.apache.kafka.common.ClassicGroupState</c>.
/// </summary>
/// <remarks>
/// <para>
/// All six Java constants, in Java's order. <see cref="Unknown"/> is a real member: a state too
/// new for this client reads as it, as Java's <c>parse</c> maps it.
/// </para>
/// <para>
/// A separate enum from <see cref="GroupState"/>, as in Java: <see cref="GroupState.NotReady"/>,
/// <c>Assigning</c> and <c>Reconciling</c> have no counterpart here, so the two are not
/// intercastable.
/// </para>
/// <para>
/// The wire value is Java's <c>toString()</c> spelling (<c>"Stable"</c>,
/// <c>"PreparingRebalance"</c>, …); Java's constants carry no numeric id, so never persist the
/// underlying <see langword="int"/>.
/// </para>
/// <para>
/// Java's <c>parse(String)</c> is not published, a recorded deviation: it decodes a wire
/// spelling the caller never sees.
/// </para>
/// </remarks>
public enum ClassicGroupState
{
    /// <summary>
    /// The state is not known to this client — Java's <c>UNKNOWN</c>, the value <c>parse</c>
    /// yields for a name it does not recognise.
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
}
