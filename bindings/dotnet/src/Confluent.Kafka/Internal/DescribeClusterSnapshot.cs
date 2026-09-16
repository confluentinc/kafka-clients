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

using System.Collections.Generic;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The four already-marshalled values one <c>describeCluster</c> completion carries, so
/// the RPC can settle a single <see cref="SingleAdminOperation{TValue}"/> and the public
/// <c>DescribeClusterResult</c> can project its four Java-shaped tasks out of it.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Internal on purpose — there is no public <c>ClusterDescription</c> (M15/P3
/// decision D12).</b> Java has no such class: <c>DescribeClusterResult</c> holds four
/// independent <c>KafkaFuture</c> fields and exposes them directly
/// (<c>DescribeClusterResult.java:31-34, :49, :59, :66, :74</c>), and the Rust core
/// mirrors that. Publishing a four-field aggregate would add a type the Java API does not
/// have (<c>definition-of-done.md</c> §7); keeping it internal makes it pure plumbing
/// between the ABI's single settled result root and the four public projections.
/// </para>
/// <para>
/// <b>Fully owned.</b> Every field is copied out of the borrowed result root <em>before</em>
/// <c>DescribeClusterResult_destroy</c> runs, so nothing native-backed survives
/// (ffi §B2 Category 3/4, §B4).
/// </para>
/// </remarks>
internal sealed class DescribeClusterSnapshot
{
    /// <summary>Creates the snapshot from already-owned managed values.</summary>
    /// <param name="nodes">The cluster's nodes — Java's <c>nodes()</c>.</param>
    /// <param name="controller">
    /// The controller, or <see langword="null"/> when there is none — Java's
    /// <c>controller()</c>, which yields null when the controller id is
    /// <c>NO_CONTROLLER_ID</c> (<c>KafkaAdminClient.java:2531-2534</c>).
    /// </param>
    /// <param name="clusterId">The cluster id — Java's <c>clusterId()</c>.</param>
    /// <param name="authorizedOperations">
    /// The authorized operations, or <see langword="null"/> when the broker did not report
    /// them at all — Java's <c>authorizedOperations()</c>, whose javadoc says the value "will
    /// be non-null if the broker supplied this information, and null otherwise"
    /// (<c>DescribeClusterResult.java:71-73</c>).
    /// </param>
    internal DescribeClusterSnapshot(
        IReadOnlyCollection<Node> nodes,
        Node? controller,
        string clusterId,
        IReadOnlyCollection<AclOperation>? authorizedOperations)
    {
        Nodes = nodes;
        Controller = controller;
        ClusterId = clusterId;
        AuthorizedOperations = authorizedOperations;
    }

    /// <inheritdoc cref="DescribeClusterSnapshot(IReadOnlyCollection{Node}, Node, string, IReadOnlyCollection{AclOperation})"/>
    internal IReadOnlyCollection<Node> Nodes { get; }

    /// <inheritdoc cref="Nodes"/>
    internal Node? Controller { get; }

    /// <inheritdoc cref="Nodes"/>
    internal string ClusterId { get; }

    /// <inheritdoc cref="Nodes"/>
    internal IReadOnlyCollection<AclOperation>? AuthorizedOperations { get; }
}
