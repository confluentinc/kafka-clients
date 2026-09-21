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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeCluster"/> — the .NET realization of Java's
/// <c>DescribeClusterResult</c>: four awaitables over the cluster's attributes, handed
/// back the moment the request is submitted
/// (<c>DescribeClusterResult.java:49, :59, :66, :74</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Two of the four are genuinely nullable, and that is contract rather than an edge
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
/// alone would silently collapse null into empty.
/// </para>
/// <para>
/// ⚠ <b>Four projections of ONE completion, not four requests</b> — and this is the
/// recorded deviation (<c>definition-of-done.md</c> §7). Java holds four independent
/// <c>KafkaFuture</c> fields which the client completes separately; the C ABI settles the
/// whole result together and has no <c>KafkaFuture</c> type with which to express
/// independent timing, so the four tasks here derive from one source. The consequence is
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
/// calls return the <b>same</b> <see cref="Task"/> instance, matching Java, whose
/// accessors return the stored future field every time. (Contrast
/// <see cref="ListTopicsResult.Listings"/>, which allocates per call because Java's
/// <c>listings()</c> is a fresh <c>thenApply</c> each call — the difference is Java's.)
/// </para>
/// <para>
/// ⚠ <b>The unobserved-faulted-task consideration, and how it was resolved.</b> Creating
/// all four eagerly means a failed call produces four faulted tasks, of which a caller
/// awaiting only <see cref="Nodes"/> observes one; the other three would raise
/// <c>TaskScheduler.UnobservedTaskException</c> when finalized. That is accepted here, for
/// three reasons. It is exactly Java's own situation — four futures fault together and a
/// caller awaits the one it wants. Unobserved task exceptions are non-fatal by default on
/// every target framework in this binding's matrix (net462 needs an explicit
/// <c>ThrowUnobservedTaskExceptions</c> opt-in; .NET Core never rethrows). And the
/// alternative — creating each projection lazily on first access — buys only a smaller
/// count of unobserved tasks in exchange for a synchronization dance on a public accessor,
/// which is a poor trade for a benign, Java-identical condition. Eager creation also makes
/// "the same instance every call" true by construction rather than by a lock.
/// </para>
/// </remarks>
public sealed class DescribeClusterResult
{
    private readonly Task<IReadOnlyCollection<Node>> _nodes;
    private readonly Task<Node?> _controller;
    private readonly Task<string> _clusterId;
    private readonly Task<IReadOnlyCollection<AclOperation>?> _authorizedOperations;

    /// <summary>
    /// Wraps the single completion — Java's package-private four-future constructor
    /// (<c>DescribeClusterResult.java:36-44</c>), collapsed onto one source per the
    /// recorded deviation above.
    /// </summary>
    internal DescribeClusterResult(Task<DescribeClusterSnapshot> future)
    {
        _nodes = ProjectNodes(future);
        _controller = ProjectController(future);
        _clusterId = ProjectClusterId(future);
        _authorizedOperations = ProjectAuthorizedOperations(future);
    }

    /// <summary>The cluster's nodes — Java's <c>nodes()</c> (<c>:49</c>).</summary>
    /// <returns>A task yielding the nodes, or faulting with the call's own <see cref="KafkaException"/>.</returns>
    /// <remarks>
    /// Java returns a <c>Collection&lt;Node&gt;</c>, so this is an
    /// <c>IReadOnlyCollection&lt;Node&gt;</c> directly rather than by the
    /// <c>IReadOnlySet</c> substitution the name-set accessors need.
    /// </remarks>
    public Task<IReadOnlyCollection<Node>> Nodes() => _nodes;

    /// <summary>
    /// The current controller node, or <see langword="null"/> when the cluster reports
    /// none — Java's <c>controller()</c> (<c>:59</c>).
    /// </summary>
    /// <returns>A task yielding the controller or <see langword="null"/>.</returns>
    /// <remarks>
    /// ⚠ <b>A null controller is a successful outcome, not a failure</b> — the task
    /// completes with <see langword="null"/> rather than faulting.
    /// </remarks>
    public Task<Node?> Controller() => _controller;

    /// <summary>The cluster id — Java's <c>clusterId()</c> (<c>:66</c>).</summary>
    /// <returns>A task yielding the cluster id.</returns>
    public Task<string> ClusterId() => _clusterId;

    /// <summary>
    /// The cluster's authorized operations, or <see langword="null"/> when the broker did
    /// not supply them — Java's <c>authorizedOperations()</c> (<c>:74</c>).
    /// </summary>
    /// <returns>
    /// A task yielding the operations, an <b>empty</b> collection when the broker reported
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
    public Task<IReadOnlyCollection<AclOperation>?> AuthorizedOperations() => _authorizedOperations;

    // Java's `thenApply`, in C#: an `async` method rather than ContinueWith, so a faulted
    // projection carries the same KafkaException as the source instead of an extra
    // AggregateException wrapper — the ListTopicsResult.Project precedent.
    private static async Task<IReadOnlyCollection<Node>> ProjectNodes(Task<DescribeClusterSnapshot> future) =>
        (await future.ConfigureAwait(false)).Nodes;

    private static async Task<Node?> ProjectController(Task<DescribeClusterSnapshot> future) =>
        (await future.ConfigureAwait(false)).Controller;

    private static async Task<string> ProjectClusterId(Task<DescribeClusterSnapshot> future) =>
        (await future.ConfigureAwait(false)).ClusterId;

    private static async Task<IReadOnlyCollection<AclOperation>?> ProjectAuthorizedOperations(
        Task<DescribeClusterSnapshot> future) =>
        (await future.ConfigureAwait(false)).AuthorizedOperations;
}
