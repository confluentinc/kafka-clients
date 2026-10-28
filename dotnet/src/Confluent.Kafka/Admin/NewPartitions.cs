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
using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// New partitions for one topic in a call to <see cref="IAdmin.CreatePartitions"/> — Java's
/// <c>org.apache.kafka.clients.admin.NewPartitions</c>.
/// </summary>
/// <remarks>
/// <para>
/// <see langword="null"/> assignments and empty assignments are different requests: with
/// <see langword="null"/> the broker places the replicas, while an empty list, legal in Java,
/// is sent as a present but empty list, which the broker rejects with
/// <c>INVALID_REPLICA_ASSIGNMENT</c>.
/// </para>
/// <para>
/// A <see langword="null"/> assignment list is the same request as
/// <see cref="IncreaseTo(int)"/>, as in Java. A <see langword="null"/> inner list throws,
/// because the core would drop it, which would shift every later partition's assignment.
/// </para>
/// </remarks>
public sealed class NewPartitions
{
    private readonly List<IReadOnlyList<int>>? _newAssignments;

    private NewPartitions(int totalCount, List<IReadOnlyList<int>>? newAssignments)
    {
        TotalCount = totalCount;
        _newAssignments = newAssignments;
    }

    /// <summary>
    /// The total number of partitions after the operation succeeds — Java's
    /// <c>totalCount()</c>. This is the total, not the number added.
    /// </summary>
    public int TotalCount { get; }

    /// <summary>
    /// The replica assignments for the new partitions, or <see langword="null"/> if the
    /// controller will decide — Java's <c>assignments()</c>.
    /// </summary>
    /// <remarks>
    /// An empty list is a request of its own: see the type remarks.
    /// </remarks>
    public IReadOnlyList<IReadOnlyList<int>>? Assignments => _newAssignments;

    /// <summary>
    /// Increases the topic's partition count to <paramref name="totalCount"/>, letting
    /// the broker decide where the new replicas go — Java's
    /// <c>increaseTo(int totalCount)</c>.
    /// </summary>
    /// <param name="totalCount">The total number of partitions after the operation.</param>
    /// <returns>The request entry, with <see cref="Assignments"/> <see langword="null"/>.</returns>
    public static NewPartitions IncreaseTo(int totalCount) => new NewPartitions(totalCount, null);

    /// <summary>
    /// Increases the topic's partition count to <paramref name="totalCount"/>, assigning
    /// the new partitions explicitly — Java's
    /// <c>increaseTo(int totalCount, List&lt;List&lt;Integer&gt;&gt; newAssignments)</c>.
    /// </summary>
    /// <param name="totalCount">The total number of partitions after the operation.</param>
    /// <param name="newAssignments">
    /// One inner list per <em>new</em> partition (existing partitions are not
    /// reassigned), each holding that partition's replica broker ids with the preferred
    /// leader first. Its length should be <paramref name="totalCount"/> minus the topic's
    /// current partition count, and each inner list should have the topic's replication
    /// factor many entries. An empty outer list is a request of its own (see the type
    /// remarks); <see langword="null"/> lets the broker decide, as <see cref="IncreaseTo(int)"/>
    /// does.
    /// </param>
    /// <returns>
    /// The request entry; its <see cref="Assignments"/> is <see langword="null"/> when
    /// <paramref name="newAssignments"/> is.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="newAssignments"/> contains a null inner list.
    /// </exception>
    public static NewPartitions IncreaseTo(int totalCount, IReadOnlyList<IReadOnlyList<int>>? newAssignments)
    {
        if (newAssignments is null)
        {
            return new NewPartitions(totalCount, null);
        }

        List<IReadOnlyList<int>> copy = new List<IReadOnlyList<int>>(newAssignments.Count);
        foreach (IReadOnlyList<int> assignment in newAssignments)
        {
            if (assignment is null)
            {
                throw new ArgumentNullException(
                    nameof(newAssignments), "A replica assignment must not be a null broker-id list.");
            }

            copy.Add(new List<int>(assignment));
        }

        return new NewPartitions(totalCount, copy);
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "(totalCount={0}, newAssignments={1})",
            TotalCount,
            Render(_newAssignments));

    private static string Render(List<IReadOnlyList<int>>? assignments)
    {
        if (assignments is null)
        {
            return "null";
        }

        List<string> inner = new List<string>(assignments.Count);
        foreach (IReadOnlyList<int> assignment in assignments)
        {
            inner.Add("[" + string.Join(", ", assignment) + "]");
        }

        return "[" + string.Join(", ", inner) + "]";
    }
}
