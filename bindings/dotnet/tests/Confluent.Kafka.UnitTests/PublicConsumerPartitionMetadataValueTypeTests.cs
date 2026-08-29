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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Unit tests for the two new public value types <see cref="Node"/> /
/// <see cref="PartitionInfo"/> (M5/P5, PLAN §4). Their <c>internal</c> constructors are
/// reachable from the test project via <c>InternalsVisibleTo</c> — this proves the field
/// storage, the nullable <see cref="Node.Rack"/> / <see cref="PartitionInfo.Leader"/>
/// mapping (present vs absent — the mock always populates a leader and never a rack, so the
/// absent cases are proven here at the value level), and the Java-mirroring <c>ToString</c>
/// forms. The native-driven field values are asserted end to end in
/// <see cref="PublicConsumerPartitionMetadataTests"/>.
/// </summary>
public sealed class PublicConsumerPartitionMetadataValueTypeTests
{
    // ---- Node ----

    [Fact]
    public void Node_StoresFields_WithRack()
    {
        Node node = new Node(1, "host-a", 9092, "rack-1");

        Assert.Equal(1, node.Id);
        Assert.Equal("host-a", node.Host);
        Assert.Equal(9092, node.Port);
        Assert.Equal("rack-1", node.Rack);
    }

    [Fact]
    public void Node_AbsentRack_IsNull()
    {
        // The absent rack maps to null (Java's nullable rack()). The mock always produces this
        // shape (3-arg Node::new → rack None), so this is the value-level proof of the null case
        // the end-to-end tests observe on Node.Rack.
        Node node = new Node(2, "host-b", 9093, null);

        Assert.Null(node.Rack);
    }

    [Fact]
    public void Node_ToString_MirrorsJavaFormat_WithRack()
    {
        // Java: host + ":" + port + " (id: " + id + " rack: " + rack + ")".
        Node node = new Node(5, "kafka-1", 9092, "us-east-1a");

        Assert.Equal("kafka-1:9092 (id: 5 rack: us-east-1a)", node.ToString());
    }

    [Fact]
    public void Node_ToString_AbsentRack_RendersNullLiteral()
    {
        // Java renders an absent rack as the literal "null" (rack is interpolated directly).
        Node node = new Node(5, "kafka-1", 9092, null);

        Assert.Equal("kafka-1:9092 (id: 5 rack: null)", node.ToString());
    }

    // ---- PartitionInfo ----

    [Fact]
    public void PartitionInfo_StoresFields()
    {
        Node leader = new Node(1, "h", 9092, null);
        Node[] replicas = { leader };
        Node[] isr = { leader };
        Node[] offline = Array.Empty<Node>();

        PartitionInfo info = new PartitionInfo("t", 3, leader, replicas, isr, offline);

        Assert.Equal("t", info.Topic);
        Assert.Equal(3, info.Partition);
        Assert.Same(leader, info.Leader);
        Assert.Same(replicas, info.Replicas);
        Assert.Same(isr, info.InSyncReplicas);
        Assert.Same(offline, info.OfflineReplicas);
    }

    [Fact]
    public void PartitionInfo_NullLeader_IsNull()
    {
        // The ABI _leader may be null (a partition with no leader); the type must not assume a
        // leader. The mock always populates one, so this null case is proven here at the value
        // level (NodeMarshal.CopyOut(Zero) → null → PartitionInfo.Leader null).
        PartitionInfo info = new PartitionInfo(
            "t", 0, leader: null, Array.Empty<Node>(), Array.Empty<Node>(), Array.Empty<Node>());

        Assert.Null(info.Leader);
    }

    [Fact]
    public void PartitionInfo_ToString_MirrorsJavaFormat()
    {
        // Java: Partition(topic = %s, partition = %d, leader = %s, replicas = %s, isr = %s,
        // offlineReplicas = %s) — nodes render as their id lists, the leader as its id.
        Node n0 = new Node(0, "h0", 9092, null);
        Node n1 = new Node(1, "h1", 9092, null);
        PartitionInfo info = new PartitionInfo(
            "topic-x",
            2,
            leader: n0,
            replicas: new List<Node> { n0, n1 },
            inSyncReplicas: new List<Node> { n0 },
            offlineReplicas: Array.Empty<Node>());

        Assert.Equal(
            "Partition(topic = topic-x, partition = 2, leader = 0, replicas = [0,1], isr = [0], offlineReplicas = [])",
            info.ToString());
    }

    [Fact]
    public void PartitionInfo_ToString_NullLeader_RendersNone()
    {
        PartitionInfo info = new PartitionInfo(
            "topic-x", 0, leader: null, Array.Empty<Node>(), Array.Empty<Node>(), Array.Empty<Node>());

        Assert.Equal(
            "Partition(topic = topic-x, partition = 0, leader = none, replicas = [], isr = [], offlineReplicas = [])",
            info.ToString());
    }
}
