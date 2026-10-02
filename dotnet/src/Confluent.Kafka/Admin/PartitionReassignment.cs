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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A partition's ongoing reassignment — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.PartitionReassignment</c>
/// (<c>PartitionReassignment.java:32, :41, :49, :57</c>), returned by
/// <see cref="IAdmin.ListPartitionReassignments"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Not to be confused with <see cref="NewPartitionReassignment"/>.</b> That one is an
/// <em>input</em> carrying only the target replicas; this is the broker's <em>report</em>
/// of a reassignment in flight, carrying three lists.
/// <see cref="AddingReplicas"/> and <see cref="RemovingReplicas"/> are each documented by
/// Java as a subset of <see cref="Replicas"/>.
/// </para>
/// <para>
/// <b>All three lists are copied on construction</b>, as Java wraps each in
/// <c>Collections.unmodifiableList</c> (<c>:33-35</c>), so a caller mutating the list it
/// passed cannot change a constructed value.
/// </para>
/// <para>
/// <b>Accessors are properties</b> (decision D18) — each is a pure managed field read that
/// does no P/Invoke and cannot throw, and no same-named static factory forces the method
/// form.
/// </para>
/// <para>
/// The ABI reads the three lists through <b>six flattened accessors</b> —
/// <c>replica_count</c>/<c>replica(i)</c> and the adding/removing pairs — with no child
/// handle per list, the same pattern as <see cref="ReplicaInfo"/>.
/// </para>
/// </remarks>
public sealed class PartitionReassignment
{
    private readonly List<int> _replicas;
    private readonly List<int> _addingReplicas;
    private readonly List<int> _removingReplicas;

    /// <summary>
    /// Creates a reassignment report — Java's
    /// <c>PartitionReassignment(List&lt;Integer&gt;, List&lt;Integer&gt;, List&lt;Integer&gt;)</c>
    /// (<c>:32</c>).
    /// </summary>
    /// <param name="replicas">The brokers this partition currently resides on.</param>
    /// <param name="addingReplicas">The brokers being added, a subset of <paramref name="replicas"/>.</param>
    /// <param name="removingReplicas">The brokers being removed, a subset of <paramref name="replicas"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    /// <remarks>
    /// ⚠ Java's constructor throws <c>NullPointerException</c> from
    /// <c>Collections.unmodifiableList(null)</c> for a null list; the .NET idiom for that
    /// is <see cref="ArgumentNullException"/>, which names the offending parameter.
    /// <b>Empty is not null here</b> — an empty adding/removing list is the ordinary case
    /// for a partition whose reassignment is only moving replicas one way.
    /// </remarks>
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

        _replicas = new List<int>(replicas);
        _addingReplicas = new List<int>(addingReplicas);
        _removingReplicas = new List<int>(removingReplicas);
    }

    /// <summary>
    /// The brokers this partition currently resides on — Java's <c>replicas()</c>
    /// (<c>:41</c>).
    /// </summary>
    public IReadOnlyList<int> Replicas => _replicas;

    /// <summary>
    /// The brokers being added as part of the reassignment, a subset of
    /// <see cref="Replicas"/> — Java's <c>addingReplicas()</c> (<c>:49</c>).
    /// </summary>
    public IReadOnlyList<int> AddingReplicas => _addingReplicas;

    /// <summary>
    /// The brokers being removed as part of the reassignment, a subset of
    /// <see cref="Replicas"/> — Java's <c>removingReplicas()</c> (<c>:57</c>).
    /// </summary>
    public IReadOnlyList<int> RemovingReplicas => _removingReplicas;

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:62-68</c>).</summary>
    /// <returns>The rendering.</returns>
    /// <remarks>
    /// Java interpolates a <c>List&lt;Integer&gt;</c>, whose <c>toString</c> is
    /// <c>[1, 2, 3]</c>; that bracket-and-comma form is reproduced here rather than C#'s
    /// default, which would print the type name.
    /// </remarks>
    public override string ToString() =>
        "PartitionReassignment(replicas=" + Render(_replicas)
        + ", addingReplicas=" + Render(_addingReplicas)
        + ", removingReplicas=" + Render(_removingReplicas)
        + ")";

    private static string Render(List<int> replicas) => "[" + string.Join(", ", replicas) + "]";
}
