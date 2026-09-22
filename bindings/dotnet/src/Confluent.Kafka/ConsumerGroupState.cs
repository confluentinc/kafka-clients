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

namespace Confluent.Kafka;

/// <summary>
/// The state of a consumer group — the .NET realization of Java's
/// <c>org.apache.kafka.common.ConsumerGroupState</c> (<c>ConsumerGroupState.java:31-39</c>).
/// </summary>
/// <remarks>
/// <para>
/// All eight Java constants ship, in Java's declaration order. <c>UNKNOWN</c> is a real
/// member of the set, not an error sentinel: a broker newer than this client can name a
/// state this client does not know, and Java's <c>parse</c> maps that to <c>UNKNOWN</c>
/// rather than failing (<c>:53-56</c>).
/// </para>
/// <para>
/// ⚠⚠ <b>Java deprecates this type, and the binding ships it anyway — a named, ruled
/// exception, not an oversight.</b> Java's <c>@Deprecated(since = "4.0", forRemoval =
/// true)</c> (<c>:30</c>) directs callers to <see cref="GroupState"/>. But the deprecated
/// admin surface that carries it — <c>ListConsumerGroupsOptions</c> and
/// <c>ConsumerGroupListing</c> — is itself part of the Java shape this binding restores,
/// and those types name <em>this</em> enum in their signatures. Dropping it would leave
/// them unexpressible. So the deprecation is <b>mirrored, not avoided</b>: the type carries
/// <see cref="ObsoleteAttribute"/> with Java's own wording, exactly as
/// <c>ClientMetricsResourceListing</c> and its two companions do for Java's 4.1
/// deprecation.
/// </para>
/// <para>
/// ⚠ <b><see cref="ObsoleteAttribute"/> is a warning here, not an error, although Java says
/// <c>forRemoval = true</c>.</b> <c>forRemoval</c> raises javac's <em>warning</em>
/// category (removal rather than deprecation); it does not reject the call. Passing
/// <c>error: true</c> to the attribute would, which would make the deprecated admin surface
/// above uncompilable by any caller — stricter than Java, and self-defeating.
/// </para>
/// <para>
/// ⚠ <b>This is a separate axis from <see cref="GroupState"/>, not a subset view of it.</b>
/// A group listing carries both, each read from its own ABI entry point; neither is derived
/// from the other. The two enums are also <em>not</em> member-for-member equal —
/// <see cref="GroupState.NotReady"/> has no counterpart here, because Java's
/// <c>ConsumerGroupState</c> never gained it. So they are not intercastable, and the
/// underlying <c>int</c> of a member of one says nothing about the other.
/// </para>
/// <para>
/// ⚠ <b>The name is the contract, not an ordinal.</b> Java's <c>ConsumerGroupState</c>
/// carries no numeric id — the ABI header says so in as many words — so the value crossing
/// the boundary is Java's <c>toString()</c> spelling (<c>"Stable"</c>,
/// <c>"PreparingRebalance"</c>, …; <c>:58-61</c>), read and written by
/// <c>Confluent.Kafka.Internal.Interop.GroupMarshal</c>. The underlying <c>int</c> of these
/// members is therefore meaningless outside this assembly; do not persist it.
/// </para>
/// <para>
/// <b>Java's <c>parse(String)</c> is deliberately not published</b>, for the reason given on
/// <see cref="GroupType"/> and <see cref="GroupState"/>: it decodes a wire spelling the
/// caller never sees.
/// </para>
/// </remarks>
[Obsolete("Deprecated in Kafka since 4.0. Use GroupState instead.")]
public enum ConsumerGroupState
{
    /// <summary>
    /// The state is not known to this client — Java's <c>UNKNOWN</c> (<c>:32</c>), the
    /// value <c>parse</c> yields for a name it does not recognise.
    /// </summary>
    Unknown,

    /// <summary>Java's <c>PREPARING_REBALANCE</c> (<c>:33</c>).</summary>
    PreparingRebalance,

    /// <summary>Java's <c>COMPLETING_REBALANCE</c> (<c>:34</c>).</summary>
    CompletingRebalance,

    /// <summary>Java's <c>STABLE</c> (<c>:35</c>).</summary>
    Stable,

    /// <summary>Java's <c>DEAD</c> (<c>:36</c>).</summary>
    Dead,

    /// <summary>Java's <c>EMPTY</c> (<c>:37</c>).</summary>
    Empty,

    /// <summary>Java's <c>ASSIGNING</c> (<c>:38</c>).</summary>
    Assigning,

    /// <summary>Java's <c>RECONCILING</c> (<c>:39</c>).</summary>
    Reconciling,
}
