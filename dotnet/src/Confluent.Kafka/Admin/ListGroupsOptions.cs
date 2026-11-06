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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for listing groups — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListGroupsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class ListGroupsOptions
{
    private IReadOnlyCollection<GroupState> _groupStates = Array.Empty<GroupState>();
    private IReadOnlyCollection<GroupType> _types = Array.Empty<GroupType>();
    private IReadOnlyCollection<string> _protocolTypes = Array.Empty<string>();
    private int? _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public ListGroupsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// The group states to filter on, or an empty collection — the default — to return
    /// groups in every state. Java's <c>inGroupStates(Set)</c> and
    /// <c>groupStates()</c>.
    /// </summary>
    /// <returns>
    /// Never <see langword="null"/>: <see langword="null"/> and an empty collection both store
    /// the empty set, as Java's does. The stored value is a de-duplicated, immutable copy,
    /// Java's <c>Set.copyOf</c>.
    /// </returns>
    /// <remarks>Brokers support this filter from version 2.6.0.</remarks>
    public IReadOnlyCollection<GroupState> GroupStates() => _groupStates;

    /// <summary>
    /// If groupStates is set, only groups in these states will be returned by <c>ListGroups()</c>. Otherwise, all
    /// groups are returned. This operation is supported by brokers with version 2.6.0 or later.
    /// </summary>
    /// <param name="groupStates">The group states, or <see langword="null"/> or empty for all.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public ListGroupsOptions InGroupStates(IReadOnlyCollection<GroupState>? groupStates)
    {
        _groupStates = CopyOf(groupStates);
        Setters.Ran(nameof(InGroupStates));
        return this;
    }

    /// <summary>
    /// The group protocol types to filter on, or an empty collection — the default — to
    /// return groups of every protocol type. Java's <c>withProtocolTypes(Set)</c>
    /// and <c>protocolTypes()</c>.
    /// </summary>
    /// <returns>
    /// Never <see langword="null"/>; <see langword="null"/> and empty both store the empty
    /// set, exactly as <see cref="GroupStates()"/> does.
    /// </returns>
    /// <remarks>
    /// The empty string is the protocol type of a classic group that uses none. Strings
    /// compare ordinally, as in Java's <c>Set&lt;String&gt;</c>.
    /// </remarks>
    public IReadOnlyCollection<string> ProtocolTypes() => _protocolTypes;

    /// <summary>
    /// If protocol types is set, only groups of these protocol types will be returned by <c>ListGroups()</c>.
    /// Otherwise, all groups are returned.
    /// </summary>
    /// <param name="protocolTypes">The protocol types, or <see langword="null"/> or empty for all.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    /// <exception cref="ArgumentException">
    /// The collection contains a <see langword="null"/> element, which Java's
    /// <c>Set.copyOf</c> rejects.
    /// </exception>
    public ListGroupsOptions WithProtocolTypes(IReadOnlyCollection<string>? protocolTypes)
    {
        _protocolTypes = CopyOfProtocolTypes(protocolTypes);
        Setters.Ran(nameof(WithProtocolTypes));
        return this;
    }

    /// <summary>
    /// The group types to filter on, or an empty collection — the default — to return
    /// groups of every type. Java's <c>withTypes(Set)</c> and <c>types()</c>.
    /// </summary>
    /// <returns>
    /// Never <see langword="null"/>; <see langword="null"/> and empty both store the empty
    /// set, exactly as <see cref="GroupStates()"/> does.
    /// </returns>
    public IReadOnlyCollection<GroupType> Types() => _types;

    /// <summary>
    /// If types is set, only groups of these types will be returned by <c>ListGroups()</c>. Otherwise, all groups are
    /// returned.
    /// </summary>
    /// <param name="types">The group types, or <see langword="null"/> or empty for all.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public ListGroupsOptions WithTypes(IReadOnlyCollection<GroupType>? types)
    {
        _types = CopyOf(types);
        Setters.Ran(nameof(WithTypes));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();

    /// <summary>
    /// Options that return only consumer groups — Java's <c>forConsumerGroups()</c>,
    /// which sets the group-type and protocol-type filters that select them.
    /// </summary>
    /// <returns>A new instance filtered to consumer groups.</returns>
    /// <remarks>
    /// Both filters are set: a classic group that uses no protocol reports the empty
    /// protocol type, which <c>"consumer"</c> alone would miss.
    /// </remarks>
    public static ListGroupsOptions ForConsumerGroups() =>
        new ListGroupsOptions()
            .WithTypes(new[] { GroupType.CLASSIC, GroupType.CONSUMER })

            // "consumer" is Java's internal ConsumerProtocol.PROTOCOL_TYPE, inlined.
            .WithProtocolTypes(new[] { "", "consumer" });

    /// <summary>
    /// Options that return only share groups — Java's <c>forShareGroups()</c>,
    /// which sets the group-type filter that selects them.
    /// </summary>
    /// <returns>A new instance filtered to share groups.</returns>
    public static ListGroupsOptions ForShareGroups() =>
        new ListGroupsOptions().WithTypes(new[] { GroupType.SHARE });

    /// <summary>
    /// Options that return only Kafka Streams groups — Java's <c>forStreamsGroups()</c>,
    /// which sets the group-type filter that selects them.
    /// </summary>
    /// <returns>A new instance filtered to Kafka Streams groups.</returns>
    public static ListGroupsOptions ForStreamsGroups() =>
        new ListGroupsOptions().WithTypes(new[] { GroupType.STREAMS });

    // Immutable, as Java's Set.copyOf: a retained HashSet could be downcast and mutated.
    private static IReadOnlyCollection<T> CopyOf<T>(IReadOnlyCollection<T>? value)
        where T : struct =>
        value is null || value.Count == 0
            ? (IReadOnlyCollection<T>)Array.Empty<T>()
            : new ReadOnlyCollection<T>(new List<T>(new HashSet<T>(value)));

    private static IReadOnlyCollection<string> CopyOfProtocolTypes(IReadOnlyCollection<string>? protocolTypes)
    {
        if (protocolTypes is null || protocolTypes.Count == 0)
        {
            return Array.Empty<string>();
        }

        HashSet<string> deduplicated = new HashSet<string>(StringComparer.Ordinal);
        foreach (string protocolType in protocolTypes)
        {
            if (protocolType is null)
            {
                throw new ArgumentException(
                    "A protocol type must not be null; Java's Set.copyOf rejects a null element.",
                    nameof(protocolTypes));
            }

            deduplicated.Add(protocolType);
        }

        return new ReadOnlyCollection<string>(new List<string>(deduplicated));
    }
}
