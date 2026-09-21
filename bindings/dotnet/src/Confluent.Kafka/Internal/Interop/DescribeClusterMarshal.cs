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
/// Copies a <c>kafka_admin_DescribeClusterResult_t</c> — result shape 5, a handful of
/// scalars and borrowed children hanging off <b>one</b> root — into a fully owned
/// <see cref="DescribeClusterSnapshot"/>, so nothing survives the
/// <c>DescribeClusterResult_destroy</c> that follows (ffi §B2 Category 3/4, §B4).
/// </summary>
/// <remarks>
/// <para>
/// <b>Not a table walk, so not a <see cref="KeyedResultMarshal"/> callable.</b> There are
/// no keys and no per-index rows to attribute failures to; the result is four attributes
/// of one cluster. It therefore gets its own marshaller, exactly as
/// <see cref="TopicDescriptionMarshal"/> is <c>describeTopics</c>'.
/// </para>
/// <para>
/// ⚠ <b>Two nullable outcomes, both contract.</b> The controller pointer is null when the
/// cluster reports none (header: "or null if there is none (Java's <c>controller()</c>
/// yields null)"), and the authorized-operation set is absent when the broker did not
/// report it — separated from "reported, but empty" by a boolean gate rather than by the
/// count, which is why <see cref="AuthorizedOperationsMarshal"/> owns that rule.
/// </para>
/// <para>
/// <b>The nodes reuse <see cref="NodeMarshal"/>:</b> the ABI hands back the same
/// <c>kafka_common_Node_t</c> the consumer surface already copies out, so there is no
/// second node marshaller and no second <see cref="Node"/> type.
/// </para>
/// </remarks>
internal static class DescribeClusterMarshal
{
    /// <summary>
    /// This result type's authorized-operation gate, hoisted so a copy-out allocates no
    /// delegates — and so a test can substitute <em>only</em> this reader while every other
    /// accessor stays production's, which is the one way to exercise the
    /// broker-did-not-report branch (see <see cref="CopyOut(IntPtr, Func{IntPtr, bool})"/>).
    /// </summary>
    internal static readonly Func<IntPtr, bool> HasAuthorizedOperations =
        NativeMethods.DescribeClusterResultHasAuthorizedOperations;

    private static readonly Func<IntPtr, int> s_authorizedOperationCount =
        NativeMethods.DescribeClusterResultAuthorizedOperationCount;

    private static readonly Func<IntPtr, int, int> s_authorizedOperation =
        NativeMethods.DescribeClusterResultAuthorizedOperation;

    /// <summary>
    /// Copies the whole result out, reading the authorized-operation gate from the ABI.
    /// </summary>
    /// <param name="result">
    /// The owned result root. Every value read here borrows from it, so this must complete
    /// before the caller destroys it.
    /// </param>
    /// <returns>The owned snapshot.</returns>
    internal static DescribeClusterSnapshot CopyOut(IntPtr result) =>
        CopyOut(result, HasAuthorizedOperations);

    /// <summary>
    /// The body, with the authorized-operation gate as a parameter.
    /// </summary>
    /// <param name="result">The owned result root.</param>
    /// <param name="hasAuthorizedOperations">
    /// The gate that separates Java's <see langword="null"/> from a reported-but-empty set.
    /// </param>
    /// <returns>The owned snapshot.</returns>
    /// <remarks>
    /// ⚠ <b>The gate is a parameter for testability, and that is not gold-plating.</b> The
    /// only broker-less vehicle for this RPC is the Rust <c>MockAdminClient</c>, whose
    /// <c>describe_cluster</c> always completes with <c>Some(BTreeSet::new())</c> — i.e.
    /// the gate is always <c>true</c> — so the <c>false</c> branch is unreachable through
    /// it. Substituting <em>only</em> this reader over a real result root, with every other
    /// accessor left as production's, isolates the branch under test; it is the same A/B
    /// shape M15/P2b used to prove that <c>deleteRecords</c>' <c>-1</c> watermark is not a
    /// verdict. Without it, an implementation that read the count alone — collapsing
    /// <see langword="null"/> into empty — would pass every test that can be written.
    /// </remarks>
    internal static DescribeClusterSnapshot CopyOut(IntPtr result, Func<IntPtr, bool> hasAuthorizedOperations)
    {
        int nodeCount = NativeMethods.DescribeClusterResultNodeCount(result);
        List<Node> nodes = new List<Node>(Math.Max(nodeCount, 0));
        for (int index = 0; index < nodeCount; index++)
        {
            Node? node = NodeMarshal.CopyOut(NativeMethods.DescribeClusterResultGetNode(result, index));
            if (node is null)
            {
                // Guarded by `node_count`, so unreachable; skipping is the safe reading.
                continue;
            }

            nodes.Add(node);
        }

        // ⚠ A null controller pointer is a SUCCESSFUL outcome carrying null, not a failure:
        // Java's controller() yields null when the controller id is NO_CONTROLLER_ID
        // (KafkaAdminClient.java:2531-2534). NodeMarshal.CopyOut already maps null → null.
        Node? controller = NodeMarshal.CopyOut(NativeMethods.DescribeClusterResultController(result));

        // NUL-terminated, borrowed (ffi §B3 row 2) — copied out here. Java's clusterId() is
        // a String, so a defensive null normalizes to empty rather than falsifying the
        // non-nullable annotation.
        string clusterId =
            Utf8Marshal.PtrToString(NativeMethods.DescribeClusterResultClusterId(result)) ?? string.Empty;

        IReadOnlyCollection<AclOperation>? authorizedOperations = AuthorizedOperationsMarshal.CopyOut(
            result, hasAuthorizedOperations, s_authorizedOperationCount, s_authorizedOperation);

        return new DescribeClusterSnapshot(nodes, controller, clusterId, authorizedOperations);
    }
}
