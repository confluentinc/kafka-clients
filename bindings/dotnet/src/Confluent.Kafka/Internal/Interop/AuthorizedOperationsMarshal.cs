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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Turns the ABI's <c>has_authorized_operations</c> / <c>…_count</c> /
/// <c>…_operation(i)</c> triple into Java's nullable <c>Set&lt;AclOperation&gt;</c>.
/// <b>Route a result carrying <c>authorizedOperations()</c> through here rather than
/// reading the triple at the marshaller</b> — see the remarks for what reading the count
/// gets wrong.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The gate is a separate boolean, NOT a count of zero — and that is the whole reason
/// this lives in one place.</b> Every such ABI accessor set documents the same thing: a
/// count of <c>0</c> covers both "the broker did not report them" (Java yields
/// <see langword="null"/>) and "reported, but none authorized", so only the
/// <c>has_*</c> discriminant separates them. Deriving nullability from
/// <c>count == 0</c> instead collapses the two — a behavioural divergence that no test
/// checking mere emptiness can see. Single-sourcing the gate is what makes a result type
/// that carries one consistent by construction rather than by imitation.
/// </para>
/// <para>
/// <b>The accessors are parameters because they differ per result type</b>
/// (<c>TopicDescription_*</c> vs <c>DescribeClusterResult_*</c>), while the <em>rule</em>
/// does not. Callers hoist them into <c>static readonly</c> fields so a copy-out allocates
/// no delegates.
/// </para>
/// </remarks>
internal static class AuthorizedOperationsMarshal
{
    /// <summary>
    /// Copies one result's authorized operations out into an owned collection, or returns
    /// <see langword="null"/> when the broker reported no set at all.
    /// </summary>
    /// <param name="owner">
    /// The borrowed handle the accessors read — a <c>TopicDescription_t</c>, a
    /// <c>DescribeClusterResult_t</c>, … Valid only until its owning root is destroyed.
    /// </param>
    /// <param name="has">That type's <c>has_authorized_operations</c> — the gate.</param>
    /// <param name="count">That type's <c>authorized_operation_count</c>.</param>
    /// <param name="read">That type's <c>authorized_operation(i)</c>, yielding a wire code.</param>
    /// <returns>
    /// The owned operations, possibly empty; or <see langword="null"/> when
    /// <paramref name="has"/> says the broker reported nothing.
    /// </returns>
    internal static IReadOnlyCollection<AclOperation>? CopyOut(
        IntPtr owner,
        Func<IntPtr, bool> has,
        Func<IntPtr, int> count,
        Func<IntPtr, int, int> read)
    {
        // ⚠ The discriminant, NOT the count — see the class remarks.
        if (!has(owner))
        {
            return null;
        }

        int total = count(owner);
        List<AclOperation> operations = new List<AclOperation>(Math.Max(total, 0));
        for (int index = 0; index < total; index++)
        {
            // The ABI hands back the Kafka wire code, and AclOperation's members ARE those
            // codes.
            operations.Add(FromCode(read(owner, index)));
        }

        return operations;
    }

    /// <summary>
    /// Java's <c>AclOperation.fromCode</c> (<c>AclOperation.java:151-157</c>): a code with
    /// no matching member becomes <see cref="AclOperation.Unknown"/> rather than an
    /// unnamed enum value. Also absorbs the ABI's own <c>-1</c> out-of-range return.
    /// </summary>
    /// <param name="code">The wire code.</param>
    /// <returns>The operation, or <see cref="AclOperation.Unknown"/>.</returns>
    internal static AclOperation FromCode(int code) =>
        code >= (int)AclOperation.Unknown && code <= (int)AclOperation.TwoPhaseCommit
            ? (AclOperation)code
            : AclOperation.Unknown;
}
