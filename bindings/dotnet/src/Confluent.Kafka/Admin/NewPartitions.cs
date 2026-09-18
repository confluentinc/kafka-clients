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
/// New partitions for one topic in a call to <see cref="IAdmin.CreatePartitions"/> — the
/// .NET realization of Java's <c>org.apache.kafka.clients.admin.NewPartitions</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b><see langword="null"/> assignments and <em>empty</em> assignments are two
/// different requests, and the difference reaches the broker.</b>
/// <see cref="IncreaseTo(int)"/> leaves <see cref="Assignments"/> <see langword="null"/>
/// — the broker chooses the replica placement. <see cref="IncreaseTo(int, IReadOnlyList{IReadOnlyList{int}})"/>
/// with an <b>empty</b> list is legal Java (<c>increaseTo(n, emptyList())</c>) and sends a
/// <em>present but empty</em> assignment list, which the broker <b>rejects</b> with
/// <c>INVALID_REPLICA_ASSIGNMENT</c> where a null one succeeds:
/// <c>CreatePartitionsRequest.json:36</c> marks <c>Assignments</c>
/// <c>"nullableVersions": "0+"</c>. The ABI carries the distinction as an explicit
/// <c>has_assignments</c> discriminant for exactly this reason, so nothing on this path
/// may coalesce the two — a <c>?? Array.Empty&lt;…&gt;()</c> would silently turn a
/// broker-rejected request into an accepted one.
/// </para>
/// <para>
/// <b>Recorded sub-divergence (<c>definition-of-done.md</c> §7): a
/// <see langword="null"/> assignment list is rejected, where Java's
/// <c>increaseTo(totalCount, null)</c> is accepted</b> and behaves as
/// <c>increaseTo(totalCount)</c>. The parameter is non-nullable under
/// <c>#nullable enable</c>, <see cref="IncreaseTo(int)"/> already spells "let the broker
/// decide", and ffi §B5 mandates precondition validation before the FFI call — so
/// accepting <see langword="null"/> would add a second spelling of an existing factory
/// while weakening the annotation. Same family as the consumer's
/// <c>CommitAsync(callback)</c> null guard: deliberately stricter than the reference,
/// recorded rather than silent.
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
    /// <c>totalCount()</c>. This is the total, <b>not</b> the number added.
    /// </summary>
    public int TotalCount { get; }

    /// <summary>
    /// The replica assignments for the new partitions, or <see langword="null"/> if the
    /// controller will decide — Java's <c>assignments()</c>.
    /// </summary>
    /// <remarks>
    /// An <b>empty</b> (non-null) list is a distinct, meaningful state — see the
    /// null-versus-empty note in the type remarks.
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
    /// factor many entries. An <b>empty</b> outer list is meaningful and preserved — see
    /// the null-versus-empty note in the type remarks.
    /// </param>
    /// <returns>The request entry, with <see cref="Assignments"/> non-null.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="newAssignments"/> is null, or contains a null inner list. Pass no
    /// assignments at all by calling <see cref="IncreaseTo(int)"/> — see the recorded
    /// sub-divergence in the type remarks.
    /// </exception>
    public static NewPartitions IncreaseTo(int totalCount, IReadOnlyList<IReadOnlyList<int>> newAssignments)
    {
        if (newAssignments is null)
        {
            throw new ArgumentNullException(nameof(newAssignments));
        }

        // Copy + validate BEFORE anything native (ffi §B5): the ABI silently no-ops on a
        // null broker-id array, so a null inner list would be dropped rather than
        // reported — and dropping one assignment shifts every later partition's.
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

        // NOT `copy.Count == 0 ? null : copy` — an empty list is the present-but-empty
        // request, which is not the same as no assignments at all (type remarks).
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
            // Java prints the null list as "null" (String.valueOf on the field).
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
