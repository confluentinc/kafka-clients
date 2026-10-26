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
/// A partition's ongoing reassignment, returned by
/// <see cref="IAdmin.ListPartitionReassignments(IReadOnlyCollection{TopicPartition}?, ListPartitionReassignmentsOptions?)"/>
/// — Java's <c>org.apache.kafka.clients.admin.PartitionReassignment</c>.
/// </summary>
/// <remarks>
/// <para>
/// Not to be confused with <see cref="NewPartitionReassignment"/>, the input carrying only the
/// target replicas: this is the broker's report of a reassignment in flight.
/// </para>
/// <para>
/// The three lists are copied on construction and exposed read-only, as Java wraps each in
/// <c>Collections.unmodifiableList</c>.
/// </para>
/// </remarks>
public sealed class PartitionReassignment
{
    private readonly ReadOnlyCollection<int> _replicas;
    private readonly ReadOnlyCollection<int> _addingReplicas;
    private readonly ReadOnlyCollection<int> _removingReplicas;

    /// <summary>Creates a reassignment report — Java's constructor.</summary>
    /// <param name="replicas">The brokers this partition currently resides on.</param>
    /// <param name="addingReplicas">The brokers being added, a subset of <paramref name="replicas"/>.</param>
    /// <param name="removingReplicas">The brokers being removed, a subset of <paramref name="replicas"/>.</param>
    /// <exception cref="ArgumentNullException">
    /// Any argument is null, where Java throws <c>NullPointerException</c>. An empty list is
    /// the ordinary case for a reassignment moving replicas one way.
    /// </exception>
    public PartitionReassignment(
        IReadOnlyList<int> replicas, IReadOnlyList<int> addingReplicas, IReadOnlyList<int> removingReplicas)
    {
        if (replicas is null)
        {
            throw new ArgumentNullException(nameof(replicas));
        }

        if (addingReplicas is null)
        {
            throw new ArgumentNullException(nameof(addingReplicas));
        }

        if (removingReplicas is null)
        {
            throw new ArgumentNullException(nameof(removingReplicas));
        }

        _replicas = new List<int>(replicas).AsReadOnly();
        _addingReplicas = new List<int>(addingReplicas).AsReadOnly();
        _removingReplicas = new List<int>(removingReplicas).AsReadOnly();
    }

    /// <summary>The brokers this partition currently resides on — Java's <c>replicas()</c>.</summary>
    public IReadOnlyList<int> Replicas => _replicas;

    /// <summary>
    /// The brokers being added as part of the reassignment, a subset of
    /// <see cref="Replicas"/> — Java's <c>addingReplicas()</c>.
    /// </summary>
    public IReadOnlyList<int> AddingReplicas => _addingReplicas;

    /// <summary>
    /// The brokers being removed as part of the reassignment, a subset of
    /// <see cref="Replicas"/> — Java's <c>removingReplicas()</c>.
    /// </summary>
    public IReadOnlyList<int> RemovingReplicas => _removingReplicas;

    /// <summary>A diagnostic rendering — Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>Each list renders as Java's <c>List.toString</c> does, <c>[1, 2, 3]</c>.</remarks>
    public override string ToString() =>
        "PartitionReassignment(replicas=" + Render(_replicas)
        + ", addingReplicas=" + Render(_addingReplicas)
        + ", removingReplicas=" + Render(_removingReplicas)
        + ")";

    private static string Render(ReadOnlyCollection<int> replicas) => "[" + string.Join(", ", replicas) + "]";
}
