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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeCluster"/> — the .NET realization of Java's
/// <c>DescribeClusterResult</c>: four futures over the cluster's attributes, handed
/// back the moment the request is submitted
/// (<c>DescribeClusterResult.java:49, :59, :66, :74</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Three of the four are genuinely nullable, and that is contract rather than an edge
/// case.</b> <see cref="Controller"/> yields <see langword="null"/> when the cluster
/// reports no controller (Java completes the future with the node looked up from
/// <c>controllerId</c>, and returns <see langword="null"/> for
/// <c>NO_CONTROLLER_ID</c> — <c>KafkaAdminClient.java:2531-2534</c>).
/// <see cref="AuthorizedOperations"/> yields <see langword="null"/> when the broker did
/// not report the set at all, which Java's javadoc states outright: the value "will be
/// non-null if the broker supplied this information, and null otherwise" (<c>:71-73</c>).
/// The ABI carries that distinction in a <em>separate</em> gate rather than in a count —
/// <c>kafka_admin_DescribeClusterResult_has_authorized_operations</c>, because a count of
/// 0 covers both "not reported" and "reported, none authorized" — so reading the count
/// alone would silently collapse null into empty. <see cref="ClusterId"/> yields
/// <see langword="null"/> where Java's <c>clusterId()</c> does: a broker too old for
/// <c>DescribeCluster</c> is answered through the <c>Metadata</c> fallback, whose cluster id
/// is nullable (header, <c>kafka_admin_DescribeClusterResult_cluster_id</c>; M15/P13.3 D15).
/// </para>
/// <para>
/// ⚠ <b>Four projections of ONE completion, not four requests</b> — and this is the
/// recorded deviation (<c>definition-of-done.md</c> §7). Java holds four independent
/// <see cref="KafkaFuture{T}"/> fields which the client completes separately; the C ABI settles the
/// whole result together and has no future type with which to express
/// independent timing, so the four futures here derive from one source. The consequence is
/// <em>stronger</em> than Java's, not weaker: four projections of one completion cannot
/// report four different cluster states.
/// </para>
/// <para>
/// <b>There is deliberately no public <c>ClusterDescription</c> type (D12).</b> The
/// aggregate the four projections derive from is
/// <see cref="Internal.DescribeClusterSnapshot"/>, which is <c>internal</c>: Java has no
/// such class, and publishing one would add public surface the Java API does not have.
/// </para>
/// <para>
/// <b>Each projection is created once, in the constructor, and cached</b> — so repeated
/// calls return an <b>equal</b> <see cref="KafkaFuture{T}"/>, over the same projection, matching Java, whose
/// accessors return the stored future field every time. (Contrast
/// <see cref="ListTopicsResult.Listings"/>, which builds a new projection per call because Java's
/// <c>listings()</c> is a fresh <c>thenApply</c> each call — the difference is Java's.) A projection runs on the
/// first <c>Get()</c>'s thread and at most once, so every <c>Get()</c> of one accessor's future returns the same
/// value instance.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the description has completed, and
/// needs no thread-pool thread: the futures here are composed on the waiting thread, never over a <c>Task</c>. A
/// failed call fails all four with the call's own <see cref="KafkaException"/>; a future that is never read raises
/// nothing, so the async twin's unobserved-task consideration has no counterpart here.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeClusterResult"/>
public sealed class DescribeClusterResult
{
    private readonly KafkaFuture<IReadOnlyCollection<Node>> _nodes;
    private readonly KafkaFuture<Node?> _controller;
    private readonly KafkaFuture<string?> _clusterId;
    private readonly KafkaFuture<IReadOnlyCollection<AclOperation>?> _authorizedOperations;

    /// <summary>
    /// Wraps the single completion — Java's package-private four-future constructor
    /// (<c>DescribeClusterResult.java:36-44</c>), collapsed onto one source per the
    /// recorded deviation above.
    /// </summary>
    internal DescribeClusterResult(KafkaFuture<DescribeClusterSnapshot> future)
    {
        // Java's `thenApply`, once per accessor: each projection is a field read of the snapshot.
        _nodes = SyncAdminFutures.Project(future, static snapshot => snapshot.Nodes);
        _controller = SyncAdminFutures.Project(future, static snapshot => snapshot.Controller);
        _clusterId = SyncAdminFutures.Project(future, static snapshot => snapshot.ClusterId);
        _authorizedOperations = SyncAdminFutures.Project(future, static snapshot => snapshot.AuthorizedOperations);
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeClusterResult From(SingleAdminOperation<DescribeClusterSnapshot> operation) =>
        new DescribeClusterResult(new KafkaFuture<DescribeClusterSnapshot>(operation.SyncSource));

    /// <summary>The cluster's nodes — Java's <c>nodes()</c> (<c>:49</c>).</summary>
    /// <returns>A future yielding the nodes, or failing with the call's own <see cref="KafkaException"/>.</returns>
    /// <remarks>
    /// Java returns a <c>Collection&lt;Node&gt;</c>, so this is an
    /// <c>IReadOnlyCollection&lt;Node&gt;</c> directly rather than by the
    /// <c>IReadOnlySet</c> substitution the name-set accessors need.
    /// </remarks>
    public KafkaFuture<IReadOnlyCollection<Node>> Nodes() => _nodes;

    /// <summary>
    /// The current controller node, or <see langword="null"/> when the cluster reports
    /// none — Java's <c>controller()</c> (<c>:59</c>).
    /// </summary>
    /// <returns>A future yielding the controller or <see langword="null"/>.</returns>
    /// <remarks>
    /// ⚠ <b>A null controller is a successful outcome, not a failure</b> — the future
    /// completes with <see langword="null"/> rather than failing.
    /// </remarks>
    public KafkaFuture<Node?> Controller() => _controller;

    /// <summary>
    /// The cluster id, or <see langword="null"/> when the cluster reported none — Java's
    /// <c>clusterId()</c> (<c>:66</c>).
    /// </summary>
    /// <returns>A future yielding the cluster id or <see langword="null"/>.</returns>
    /// <remarks>
    /// ⚠ <b>A null id is a successful outcome, not a failure</b>, and it is not the empty
    /// string: the future completes with <see langword="null"/> when the answer came through the
    /// old-broker <c>Metadata</c> fallback without an id (see the type remarks).
    /// </remarks>
    public KafkaFuture<string?> ClusterId() => _clusterId;

    /// <summary>
    /// The cluster's authorized operations, or <see langword="null"/> when the broker did
    /// not supply them — Java's <c>authorizedOperations()</c> (<c>:74</c>).
    /// </summary>
    /// <returns>
    /// A future yielding the operations, an <b>empty</b> collection when the broker reported
    /// an empty set, or <see langword="null"/> when it reported nothing at all. The three
    /// are distinct; see the type remarks.
    /// </returns>
    /// <remarks>
    /// Java returns a <c>Set&lt;AclOperation&gt;</c>;
    /// <c>IReadOnlySet&lt;T&gt;</c> post-dates the netstandard2.0 floor, so this is an
    /// <c>IReadOnlyCollection&lt;AclOperation&gt;</c> — the same substitution
    /// <see cref="TopicDescription.AuthorizedOperations"/> and
    /// <see cref="ListTopicsResult.Names"/> already make (CLAUDE.md §3's idiom map).
    /// </remarks>
    public KafkaFuture<IReadOnlyCollection<AclOperation>?> AuthorizedOperations() => _authorizedOperations;
}
