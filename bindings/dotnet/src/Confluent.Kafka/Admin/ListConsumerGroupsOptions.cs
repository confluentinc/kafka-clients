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
using System.Collections.ObjectModel;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for listing consumer groups — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupsOptions</c>
/// (<c>ListConsumerGroupsOptions.java:35</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// ⚠ <b>The type itself is deprecated in Java</b>, not merely the RPC it configures:
/// <c>@Deprecated(since = "4.1")</c> at <c>:33</c>, directing callers to
/// <see cref="IAdmin.ListGroups"/> and <see cref="ListGroupsOptions"/> — of which this class
/// is the generation-older sibling. So it carries <see cref="ObsoleteAttribute"/> here —
/// <b>mirrored, not avoided</b>, the same call <see cref="ConsumerGroupListing"/>,
/// <see cref="ClientMetricsResourceListing"/> and <see cref="ConsumerGroupState"/> already
/// make — and the attribute is a <b>warning</b> rather than an error, because a surface
/// whose whole job is to be callable must stay callable.
/// </para>
/// <para>
/// <b>Java's three fluent setters and three getters collapse into three properties.</b>
/// <c>inGroupStates</c> (<c>:45</c>) / <c>groupStates()</c> (<c>:76</c>) become
/// <see cref="GroupStates"/>, <c>inStates</c> (<c>:57</c>) / <c>states()</c> (<c>:85</c>)
/// become <see cref="States"/>, and <c>withTypes</c> (<c>:68</c>) / <c>types()</c>
/// (<c>:92</c>) become <see cref="Types"/> — the pairing <see cref="CreateTopicsOptions"/>
/// rules for every options type in this binding (a settable POCO, not a builder) and
/// <see cref="ListGroupsOptions"/> already applied to this class's successor.
/// <b>No member is dropped</b>: each Java setter's <em>behavior</em> lives on in its
/// property's setter, which normalizes exactly as Java's body does.
/// </para>
/// <para>
/// ⚠⚠ <b><see cref="States"/> and <see cref="GroupStates"/> are two views of <em>one</em>
/// stored filter, not two independent axes — and that was read off the Java rather than
/// assumed.</b> Java declares exactly two fields, <c>Set&lt;GroupState&gt; groupStates</c>
/// (<c>:37</c>) and <c>Set&lt;GroupType&gt; types</c> (<c>:38</c>); there is no
/// <c>states</c> field. The deprecated setter writes <em>into</em> the one state field,
/// mapping each element through <c>GroupState.parse(state.toString())</c> (<c>:58-60</c>),
/// and the deprecated getter reads <em>out of</em> it, mapping back through
/// <c>ConsumerGroupState.parse(groupState.toString())</c> (<c>:86</c>). So assigning either
/// property <b>replaces</b> what the other reports; they never accumulate. This is the same
/// one-axis-two-enums relation <see cref="ConsumerGroupListing.State"/> documents, here made
/// writable from both sides because Java's setter pair is.
/// </para>
/// <para>
/// ⚠ <b>The projection to <see cref="Confluent.Kafka.ConsumerGroupState"/> is lossy, so the
/// round trip is not the identity.</b>
/// <see cref="Confluent.Kafka.GroupState.NotReady"/> has no counterpart on the deprecated
/// enum (Java never added one), so it reads back as
/// <see cref="Confluent.Kafka.ConsumerGroupState.Unknown"/> — Java's <c>parse</c> answering
/// <c>UNKNOWN</c> for a name it does not recognise. Two group states can therefore collapse
/// into one deprecated state, which Java's <c>Collectors.toSet()</c> (<c>:86</c>)
/// de-duplicates and so does this. The opposite direction loses nothing: every
/// <see cref="Confluent.Kafka.ConsumerGroupState"/> member names a
/// <see cref="Confluent.Kafka.GroupState"/> member.
/// </para>
/// <para>
/// <b>Both projections route through <c>GroupMarshal</c></b> — Java's <c>toString()</c> then
/// its <c>parse</c>, composed — rather than through a mapping written here, which is the
/// call <see cref="ConsumerGroupListing.State"/> already made: a second table could drift
/// from the one the ABI decode path uses.
/// </para>
/// <para>
/// <b>Java declares no static factories and no constructors on this class</b> — unlike
/// <see cref="ListGroupsOptions"/>, whose three <c>for*Groups()</c> factories (<c>:38</c>,
/// <c>:48</c>, <c>:57</c>) arrived a generation later. Their absence here is Java's, not an
/// omission, and the standing ruling that skips a Java <em>deprecated constructor</em>
/// (applied to <see cref="ConsumerGroupListing"/>) has nothing to skip: this class has none.
/// </para>
/// <para>
/// <b>Java's <c>Set&lt;T&gt;</c> becomes <see cref="IReadOnlyCollection{T}"/></b> — the
/// binding's standing substitute, since <c>IReadOnlySet&lt;T&gt;</c> post-dates the
/// netstandard2.0 floor, and the type
/// <see cref="GroupStateExtensions.GroupStatesForType"/> already returns.
/// </para>
/// <para>
/// ⚠ <b>Every stored filter is immutable, although only one of Java's three bodies is.</b>
/// Java uses <c>Set.copyOf</c> for <c>inGroupStates</c> (<c>:46</c>) but a mutable
/// <c>new HashSet&lt;&gt;</c> for <c>withTypes</c> (<c>:69</c>) and a mutable
/// <c>Collectors.toSet()</c> for <c>inStates</c> (<c>:60</c>). All three are defensive
/// copies — the difference is only whether the copy can later be written — and this binding
/// freezes all three, exactly as <see cref="ListGroupsOptions"/> does. A retained
/// collection that a caller can downcast and mutate would rewrite this instance's filter
/// behind its back; Java's strictest of the three is the one worth mirroring.
/// </para>
/// </remarks>
[Obsolete(
    "Deprecated in Kafka since 4.1. Use IAdmin.ListGroups and ListGroupsOptions instead.")]
public sealed class ListConsumerGroupsOptions
{
    private IReadOnlyCollection<GroupState> _groupStates = Array.Empty<GroupState>();
    private IReadOnlyCollection<GroupType> _types = Array.Empty<GroupType>();

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// The group states to filter on, or an empty collection — the default — to return
    /// groups in every state. Java's <c>inGroupStates(Set)</c> (<c>:45</c>) and
    /// <c>groupStates()</c> (<c>:76</c>).
    /// </summary>
    /// <value>
    /// Never <see langword="null"/>: setting <see langword="null"/> or an empty collection
    /// stores the empty set, which is Java's <c>Collections.emptySet()</c> and its
    /// <c>(x == null || x.isEmpty()) ? emptySet() : …</c> ternary (<c>:46</c>).
    /// </value>
    /// <remarks>
    /// This filter is supported by brokers with version 2.6.0 or later, as Java's javadoc
    /// notes (<c>:43</c>). The stored value is a de-duplicated, immutable copy, so a later
    /// mutation of the collection you passed is not observed here — Java's
    /// <c>Set.copyOf</c>. Assigning it also replaces what <see cref="States"/> reports:
    /// the two are one filter (see the class remarks).
    /// </remarks>
    public IReadOnlyCollection<GroupState> GroupStates
    {
        get => _groupStates;
        set => _groupStates = CopyOf(value);
    }

    /// <summary>
    /// The same filter on the superseded <see cref="Confluent.Kafka.ConsumerGroupState"/>
    /// axis. Java's deprecated <c>inStates(Set)</c> (<c>:57</c>) and <c>states()</c>
    /// (<c>:85</c>).
    /// </summary>
    /// <value>
    /// Never <see langword="null"/>, and never a stored collection: the getter projects
    /// <see cref="GroupStates"/> per call, as Java's <c>stream().map(…).collect(…)</c>
    /// (<c>:86</c>) mints a set per call. Setting <see langword="null"/> or an empty
    /// collection stores the empty set (<c>:58</c>).
    /// </value>
    /// <remarks>
    /// <para>
    /// ⚠ <b>This writes and reads <see cref="GroupStates"/>; it is not a second filter.</b>
    /// Assigning it replaces the group-state filter with the projection of what you passed,
    /// and assigning <see cref="GroupStates"/> replaces what this returns. The class
    /// remarks cite the Java that establishes it.
    /// </para>
    /// <para>
    /// ⚠ <b>The read direction is lossy</b> —
    /// <see cref="Confluent.Kafka.GroupState.NotReady"/> has no counterpart here and reads
    /// back as <see cref="Confluent.Kafka.ConsumerGroupState.Unknown"/> — so a value written
    /// through <see cref="GroupStates"/> need not survive a round trip through this
    /// property. The write direction loses nothing.
    /// </para>
    /// </remarks>
    [Obsolete("Deprecated in Kafka since 4.0. Use GroupStates instead.")]
    public IReadOnlyCollection<ConsumerGroupState> States
    {
        get => ToConsumerStates(_groupStates);
        set => _groupStates = ToGroupStates(value);
    }

    /// <summary>
    /// The group types to filter on, or an empty collection — the default — to return
    /// groups of every type. Java's <c>withTypes(Set)</c> (<c>:68</c>) and <c>types()</c>
    /// (<c>:92</c>).
    /// </summary>
    /// <value>
    /// Never <see langword="null"/>; <see langword="null"/> and empty both store the empty
    /// set, exactly as <see cref="GroupStates"/> does (<c>:69</c>).
    /// </value>
    public IReadOnlyCollection<GroupType> Types
    {
        get => _types;
        set => _types = CopyOf(value);
    }

    /// <summary>
    /// Java's <c>(x == null || x.isEmpty()) ? emptySet() : &lt;copy&gt;</c> for the two
    /// filters that are assigned as themselves.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The caller's collection, possibly null or empty.</param>
    /// <returns>An immutable, de-duplicated copy — or the empty set.</returns>
    /// <remarks>
    /// Spelled here rather than shared with <see cref="ListGroupsOptions"/>'s identical
    /// private helper: hoisting it would mint an <c>internal</c> utility type that neither
    /// Java nor this binding's public surface has, to save five lines
    /// (<c>definition-of-done.md</c> §7).
    /// </remarks>
    private static IReadOnlyCollection<T> CopyOf<T>(IReadOnlyCollection<T> value)
        where T : struct =>
        value is null || value.Count == 0
            ? (IReadOnlyCollection<T>)Array.Empty<T>()
            : new ReadOnlyCollection<T>(new List<T>(new HashSet<T>(value)));

    /// <summary>
    /// Java's <c>states.stream().map(state -&gt; GroupState.parse(state.toString()))
    /// .collect(Collectors.toSet())</c> (<c>:60</c>), plus the null/empty ternary that
    /// guards it (<c>:58</c>).
    /// </summary>
    /// <param name="value">The caller's collection, possibly null or empty.</param>
    /// <returns>An immutable, de-duplicated projection — or the empty set.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="value"/> contains a <see cref="Confluent.Kafka.ConsumerGroupState"/>
    /// value no member defines (reachable only via a cast). Rejected here rather than
    /// silently projected to <see cref="Confluent.Kafka.GroupState.Unknown"/> — the same
    /// reject-an-undefined-cast contract <see cref="GroupStates"/> and <see cref="Types"/>
    /// get for free from <c>NativeAdminClient.FilterNames</c> at submit time. This setter has
    /// no submit-time check of its own to fall back on, so it must throw here.
    /// </exception>
    private static IReadOnlyCollection<GroupState> ToGroupStates(
        IReadOnlyCollection<ConsumerGroupState> value)
    {
        if (value is null || value.Count == 0)
        {
            return Array.Empty<GroupState>();
        }

        HashSet<GroupState> projected = new HashSet<GroupState>();
        foreach (ConsumerGroupState state in value)
        {
            string? name = GroupMarshal.NameFromConsumerState(state);
            if (name is null)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(value),
                    state,
                    $"{nameof(ListConsumerGroupsOptions)}.{nameof(States)} must contain only "
                        + $"defined {nameof(ConsumerGroupState)} members.");
            }

            projected.Add(GroupMarshal.ParseState(name));
        }

        return new ReadOnlyCollection<GroupState>(new List<GroupState>(projected));
    }

    /// <summary>
    /// Java's <c>groupStates.stream().map(groupState -&gt;
    /// ConsumerGroupState.parse(groupState.toString())).collect(Collectors.toSet())</c>
    /// (<c>:86</c>).
    /// </summary>
    /// <param name="groupStates">The stored filter, never null.</param>
    /// <returns>An immutable, de-duplicated projection — or the empty set.</returns>
    /// <remarks>
    /// De-duplication is load-bearing rather than incidental here: the projection is lossy,
    /// so two stored members can yield one — <see cref="Confluent.Kafka.GroupState.NotReady"/>
    /// and <see cref="Confluent.Kafka.GroupState.Unknown"/> both name
    /// <see cref="Confluent.Kafka.ConsumerGroupState.Unknown"/>.
    /// </remarks>
    private static IReadOnlyCollection<ConsumerGroupState> ToConsumerStates(
        IReadOnlyCollection<GroupState> groupStates)
    {
        if (groupStates.Count == 0)
        {
            return Array.Empty<ConsumerGroupState>();
        }

        HashSet<ConsumerGroupState> projected = new HashSet<ConsumerGroupState>();
        foreach (GroupState groupState in groupStates)
        {
            projected.Add(GroupMarshal.ParseConsumerState(GroupMarshal.NameFromState(groupState) ?? "Unknown"));
        }

        return new ReadOnlyCollection<ConsumerGroupState>(new List<ConsumerGroupState>(projected));
    }
}
