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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the ABI's <c>kafka_admin_NewPartitions_t</c> input handle from the public
/// <see cref="NewPartitions"/>. The second and last <b>input</b> handle type in the whole
/// admin ABI (every other input crosses as parallel arrays).
/// </summary>
/// <remarks>
/// <para>
/// <b>Ownership: the caller retains it.</b> The ABI copies out of these entries during
/// the submit and the header states the caller keeps ownership — so every handle built
/// here must be destroyed after the submit returns, or each call leaks one per topic.
/// Same contract as <see cref="NewTopicMarshal"/>.
/// </para>
/// <para>
/// ⚠ <b>The <c>has_assignments</c> discriminant is taken from whether
/// <see cref="NewPartitions.Assignments"/> is <see langword="null"/>, never from how many
/// assignments there are.</b> <c>increaseTo(n, emptyList())</c> is legal Java and is a
/// <em>different wire request</em> from <c>increaseTo(n)</c>: the broker rejects a
/// present-but-empty assignment list with <c>INVALID_REPLICA_ASSIGNMENT</c> where a null
/// one succeeds. So an empty list must still set the flag — deriving it from
/// <c>Count &gt; 0</c> would silently rewrite the caller's request into the other one.
/// </para>
/// </remarks>
internal static class NewPartitionsMarshal
{
    /// <summary>
    /// The ABI's <c>has_assignments</c> discriminant for one request entry: <b>presence</b>
    /// of the assignment list, never its length.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Named and separated out because it is the single decision that distinguishes
    /// <c>increaseTo(n)</c> from <c>increaseTo(n, emptyList())</c> — two different wire
    /// requests, one of which the broker rejects with <c>INVALID_REPLICA_ASSIGNMENT</c>.
    /// </para>
    /// <para>
    /// ⚠ <b>It is also the only place that decision is observable from managed code.</b>
    /// The ABI exposes no getter on <c>kafka_admin_NewPartitions_t</c>, so once the flag
    /// has crossed into native nothing can read it back — a test can only assert the value
    /// <see cref="Build"/> is about to pass. Keeping that value behind a named function
    /// production itself calls is what lets the test pin it rather than reimplement it
    /// (<c>definition-of-done.md</c> §12).
    /// </para>
    /// </remarks>
    /// <param name="partitions">The request entry.</param>
    /// <returns>
    /// <see langword="true"/> when Java's <c>newAssignments</c> is non-null — including
    /// when it is present and <b>empty</b>.
    /// </returns>
    internal static bool HasAssignments(NewPartitions partitions) => partitions.Assignments is not null;

    /// <summary>
    /// Builds one native entry. On any failure after allocation the handle is destroyed
    /// before rethrowing, so a partially built entry never leaks.
    /// </summary>
    /// <param name="partitions">The request entry. Already validated by its factory.</param>
    /// <returns>An owned <c>kafka_admin_NewPartitions_t</c> the caller must destroy.</returns>
    /// <exception cref="KafkaException">The ABI refused to allocate the entry.</exception>
    internal static IntPtr Build(NewPartitions partitions)
    {
        IReadOnlyList<IReadOnlyList<int>>? assignments = partitions.Assignments;

        // The discriminant is presence, NOT count — see HasAssignments and the class remarks.
        IntPtr handle = NativeMethods.NewPartitionsNew(partitions.TotalCount, HasAssignments(partitions));
        if (handle == IntPtr.Zero)
        {
            // The header documents a non-null return unconditionally, so reaching here is
            // a core contract violation — surfaced as an operational error rather than a
            // null dereference.
            throw new KafkaException(
                $"kafka_admin_NewPartitions_new returned a null handle for a total count of {partitions.TotalCount}.");
        }

        try
        {
            if (assignments is not null)
            {
                foreach (IReadOnlyList<int> assignment in assignments)
                {
                    int[] brokerIds = new int[assignment.Count];
                    for (int i = 0; i < brokerIds.Length; i++)
                    {
                        brokerIds[i] = assignment[i];
                    }

                    // The blittable int[] is pinned by the marshaller for the duration of
                    // the call; the ABI copies out during it (ffi §A4 call-scoped).
                    NativeMethods.NewPartitionsAddAssignment(handle, brokerIds, brokerIds.Length);
                }
            }
        }
        catch
        {
            NativeMethods.NewPartitionsDestroy(handle);
            throw;
        }

        return handle;
    }
}
