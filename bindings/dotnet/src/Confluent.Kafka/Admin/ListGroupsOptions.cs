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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for listing groups — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListGroupsOptions</c> (<c>ListGroupsOptions.java:28</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// <b>Java's three fluent setters and three getters collapse into three properties.</b>
/// <c>inGroupStates</c> (<c>:67</c>) / <c>groupStates()</c> (<c>:93</c>) become
/// <see cref="GroupStates"/>, <c>withProtocolTypes</c> (<c>:76</c>) /
/// <c>protocolTypes()</c> (<c>:100</c>) become <see cref="ProtocolTypes"/>, and
/// <c>withTypes</c> (<c>:85</c>) / <c>types()</c> (<c>:107</c>) become <see cref="Types"/>
/// — the pairing <see cref="CreateTopicsOptions"/> already rules for every options type in
/// this binding (a settable POCO, not a builder) and <see cref="GroupListing"/> rules for
/// every Java getter (a property, not a method). <b>No member is dropped</b>: each Java
/// setter's <em>behavior</em> lives on in its property's setter, which normalizes exactly as
/// Java's body does.
/// </para>
/// <para>
/// ⚠ <b>The three static factories stay methods</b> (<c>:38</c>, <c>:48</c>, <c>:57</c>).
/// They mint a configured instance rather than read one, which no property default can
/// express, and a parameterless property that returns a fresh object each get would misread
/// as a cached one.
/// </para>
/// <para>
/// <b>Java's <c>Set&lt;T&gt;</c> becomes <see cref="IReadOnlyCollection{T}"/></b> — the
/// binding's standing substitute, since <c>IReadOnlySet&lt;T&gt;</c> post-dates the
/// netstandard2.0 floor, and the type
/// <see cref="GroupStateExtensions.GroupStatesForType"/> already returns.
/// </para>
/// </remarks>
public sealed class ListGroupsOptions
{
    private IReadOnlyCollection<GroupState> _groupStates = Array.Empty<GroupState>();
    private IReadOnlyCollection<GroupType> _types = Array.Empty<GroupType>();
    private IReadOnlyCollection<string> _protocolTypes = Array.Empty<string>();

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// The group states to filter on, or an empty collection — the default — to return
    /// groups in every state. Java's <c>inGroupStates(Set)</c> (<c>:67</c>) and
    /// <c>groupStates()</c> (<c>:93</c>).
    /// </summary>
    /// <value>
    /// Never <see langword="null"/>: setting <see langword="null"/> or an empty collection
    /// stores the empty set, which is Java's <c>Set.of()</c> and its
    /// <c>(x == null || x.isEmpty()) ? Set.of() : …</c> ternary (<c>:68</c>).
    /// </value>
    /// <remarks>
    /// This filter is supported by brokers with version 2.6.0 or later, as Java's javadoc
    /// notes (<c>:65</c>). The stored value is a de-duplicated, immutable copy, so a later
    /// mutation of the collection you passed is not observed here — Java's
    /// <c>Set.copyOf</c>.
    /// </remarks>
    public IReadOnlyCollection<GroupState> GroupStates
    {
        get => _groupStates;
        set => _groupStates = CopyOf(value);
    }

    /// <summary>
    /// The group protocol types to filter on, or an empty collection — the default — to
    /// return groups of every protocol type. Java's <c>withProtocolTypes(Set)</c>
    /// (<c>:76</c>) and <c>protocolTypes()</c> (<c>:100</c>).
    /// </summary>
    /// <value>
    /// Never <see langword="null"/>; <see langword="null"/> and empty both store the empty
    /// set, exactly as <see cref="GroupStates"/> does.
    /// </value>
    /// <exception cref="ArgumentException">
    /// The collection contains a <see langword="null"/> element. Java's <c>Set.copyOf</c>
    /// rejects one with a <c>NullPointerException</c>, and the element type here is
    /// non-nullable <see cref="string"/>, so accepting one would falsify the annotation —
    /// the call <see cref="GroupListing"/> makes for its own non-nullable strings.
    /// </exception>
    /// <remarks>
    /// The empty string is a meaningful member of this set, not a placeholder: it is the
    /// protocol type of a classic group that is not using one, which is why
    /// <see cref="ForConsumerGroups"/> asks for it alongside <c>"consumer"</c>. Strings are
    /// compared ordinally, which is what Java's <c>Set&lt;String&gt;</c> does and what the
    /// rest of this binding keys strings by.
    /// </remarks>
    public IReadOnlyCollection<string> ProtocolTypes
    {
        get => _protocolTypes;
        set => _protocolTypes = CopyOfProtocolTypes(value);
    }

    /// <summary>
    /// The group types to filter on, or an empty collection — the default — to return
    /// groups of every type. Java's <c>withTypes(Set)</c> (<c>:85</c>) and <c>types()</c>
    /// (<c>:107</c>).
    /// </summary>
    /// <value>
    /// Never <see langword="null"/>; <see langword="null"/> and empty both store the empty
    /// set, exactly as <see cref="GroupStates"/> does.
    /// </value>
    public IReadOnlyCollection<GroupType> Types
    {
        get => _types;
        set => _types = CopyOf(value);
    }

    /// <summary>
    /// Options that return only consumer groups — Java's <c>forConsumerGroups()</c>
    /// (<c>:38</c>), which sets the group-type and protocol-type filters that select them.
    /// </summary>
    /// <returns>A new instance filtered to consumer groups.</returns>
    /// <remarks>
    /// ⚠ <b>Both filters are set, and both are load-bearing.</b> A classic group that is not
    /// using a protocol reports the empty protocol type, so asking for <c>"consumer"</c>
    /// alone would miss it.
    /// </remarks>
    public static ListGroupsOptions ForConsumerGroups() =>
        new ListGroupsOptions
        {
            Types = new[] { GroupType.Classic, GroupType.Consumer },

            // "consumer" is ConsumerProtocol.PROTOCOL_TYPE (ConsumerProtocol.java:45),
            // inlined as its literal: that constant lives in Java's
            // clients.consumer.internals package, which this binding does not surface, and
            // introducing a ConsumerProtocol type to hold one string would add public
            // surface Java's own public API does not have (definition-of-done.md §7).
            ProtocolTypes = new[] { "", "consumer" },
        };

    /// <summary>
    /// Options that return only share groups — Java's <c>forShareGroups()</c> (<c>:48</c>),
    /// which sets the group-type filter that selects them.
    /// </summary>
    /// <returns>A new instance filtered to share groups.</returns>
    public static ListGroupsOptions ForShareGroups() =>
        new ListGroupsOptions { Types = new[] { GroupType.Share } };

    /// <summary>
    /// Options that return only Kafka Streams groups — Java's <c>forStreamsGroups()</c>
    /// (<c>:57</c>), which sets the group-type filter that selects them.
    /// </summary>
    /// <returns>A new instance filtered to Kafka Streams groups.</returns>
    public static ListGroupsOptions ForStreamsGroups() =>
        new ListGroupsOptions { Types = new[] { GroupType.Streams } };

    /// <summary>
    /// Java's <c>(x == null || x.isEmpty()) ? Set.of() : Set.copyOf(x)</c> for the two
    /// enum-valued filters.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The caller's collection, possibly null or empty.</param>
    /// <returns>
    /// An immutable, de-duplicated copy — or the empty set. Immutable rather than a bare
    /// <see cref="HashSet{T}"/> because this collection is <em>retained</em>: a caller who
    /// downcast a stored <see cref="HashSet{T}"/> could mutate this instance's filter behind
    /// its back, whereas Java's <c>Set.copyOf</c> result throws instead.
    /// </returns>
    private static IReadOnlyCollection<T> CopyOf<T>(IReadOnlyCollection<T> value)
        where T : struct =>
        value is null || value.Count == 0
            ? (IReadOnlyCollection<T>)Array.Empty<T>()
            : new ReadOnlyCollection<T>(new List<T>(new HashSet<T>(value)));

    /// <summary>
    /// <see cref="CopyOf{T}"/> for <see cref="ProtocolTypes"/>, which additionally pins the
    /// ordinal comparison and rejects the null element Java's <c>Set.copyOf</c> rejects.
    /// </summary>
    /// <param name="value">The caller's collection, possibly null or empty.</param>
    /// <returns>An immutable, ordinally de-duplicated copy — or the empty set.</returns>
    /// <exception cref="ArgumentException">A member of the collection is null.</exception>
    private static IReadOnlyCollection<string> CopyOfProtocolTypes(IReadOnlyCollection<string> value)
    {
        if (value is null || value.Count == 0)
        {
            return Array.Empty<string>();
        }

        HashSet<string> deduplicated = new HashSet<string>(StringComparer.Ordinal);
        foreach (string protocolType in value)
        {
            if (protocolType is null)
            {
                throw new ArgumentException(
                    "A protocol type must not be null; Java's Set.copyOf rejects a null element.",
                    nameof(value));
            }

            deduplicated.Add(protocolType);
        }

        return new ReadOnlyCollection<string>(new List<string>(deduplicated));
    }
}
