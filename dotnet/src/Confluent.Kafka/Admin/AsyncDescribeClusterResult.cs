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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeCluster"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeClusterResult</c>: four futures over the cluster's
/// attributes.
/// </summary>
/// <remarks>
/// <see cref="Controller"/> yields <see langword="null"/> when the cluster reports no controller,
/// and <see cref="AuthorizedOperations"/> when the broker did not supply the set, as in Java. Each
/// accessor returns the same future every time, as Java's accessors return their stored field.
/// </remarks>
/// <seealso cref="DescribeClusterResult"/>
public sealed class AsyncDescribeClusterResult
{
    private readonly AsyncKafkaFuture<IReadOnlyCollection<Node>> _nodes;
    private readonly AsyncKafkaFuture<Node?> _controller;
    private readonly AsyncKafkaFuture<string> _clusterId;
    private readonly AsyncKafkaFuture<IReadOnlyCollection<AclOperation>?> _authorizedOperations;

    internal AsyncDescribeClusterResult(
        AsyncKafkaFuture<IReadOnlyCollection<Node>> nodes,
        AsyncKafkaFuture<Node?> controller,
        AsyncKafkaFuture<string> clusterId,
        AsyncKafkaFuture<IReadOnlyCollection<AclOperation>?> authorizedOperations,
        SafeAdminResultHandle? native = null)
    {
        _nodes = nodes;
        _controller = controller;
        _clusterId = clusterId;
        _authorizedOperations = authorizedOperations;
        Native = native;
    }

    internal SafeAdminResultHandle? Native { get; }

    internal static AsyncDescribeClusterResult OfClient(
        (KafkaFutureImpl<IReadOnlyCollection<Node>> Nodes,
            KafkaFutureImpl<Node?> Controller,
            KafkaFutureImpl<string> ClusterId,
            KafkaFutureImpl<IReadOnlyCollection<AclOperation>?> AuthorizedOperations,
            SafeAdminResultHandle Result) native) =>
        new AsyncDescribeClusterResult(
            new AsyncKafkaFuture<IReadOnlyCollection<Node>>(native.Nodes),
            new AsyncKafkaFuture<Node?>(native.Controller),
            new AsyncKafkaFuture<string>(native.ClusterId),
            new AsyncKafkaFuture<IReadOnlyCollection<AclOperation>?>(native.AuthorizedOperations),
            native.Result);

    /// <summary>The cluster's nodes — Java's <c>nodes()</c>.</summary>
    /// <returns>A future yielding the nodes, or faulting with the call's own <see cref="KafkaException"/>.</returns>
    public AsyncKafkaFuture<IReadOnlyCollection<Node>> Nodes() => _nodes;

    /// <summary>
    /// The current controller node, or <see langword="null"/> when the cluster reports
    /// none — Java's <c>controller()</c>.
    /// </summary>
    /// <returns>A future yielding the controller or <see langword="null"/>.</returns>
    /// <remarks>A null controller is a success: the future completes with <see langword="null"/>.</remarks>
    public AsyncKafkaFuture<Node?> Controller() => _controller;

    /// <summary>
    /// The cluster id — Java's <c>clusterId()</c>.
    /// </summary>
    /// <returns>A future yielding the cluster id.</returns>
    public AsyncKafkaFuture<string> ClusterId() => _clusterId;

    /// <summary>
    /// The cluster's authorized operations, or <see langword="null"/> when the broker did
    /// not supply them — Java's <c>authorizedOperations()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the operations: an empty collection when the broker reported an empty set,
    /// <see langword="null"/> when it reported nothing.
    /// </returns>
    public AsyncKafkaFuture<IReadOnlyCollection<AclOperation>?> AuthorizedOperations() => _authorizedOperations;
}
