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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="Node"/> and <see cref="PartitionInfo"/>: their fields, the null rack and leader the mock never
/// produces, <see cref="Node.IsFenced"/>, and Java's <c>ToString</c> forms.
/// </summary>
public sealed class PublicConsumerPartitionMetadataValueTypeTests
{
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
        Node node = new Node(2, "host-b", 9093, null);

        Assert.Null(node.Rack);
    }

    [Fact]
    public void Node_ToString_MirrorsJavaFormat_WithRack()
    {
        Node node = new Node(5, "kafka-1", 9092, "us-east-1a");

        Assert.Equal("kafka-1:9092 (id: 5 rack: us-east-1a isFenced: false)", node.ToString());
    }

    [Fact]
    public void Node_ToString_AbsentRack_RendersNullLiteral()
    {
        Node node = new Node(5, "kafka-1", 9092, null);

        Assert.Equal("kafka-1:9092 (id: 5 rack: null isFenced: false)", node.ToString());
    }

    [Fact]
    public void Node_ToString_FencedNode_RendersLowercaseTrue()
    {
        Node node = new Node(5, "kafka-1", 9092, "us-east-1a", isFenced: true);

        Assert.Equal("kafka-1:9092 (id: 5 rack: us-east-1a isFenced: true)", node.ToString());
    }

    [Fact]
    public void Node_IsFenced_IsAGetOnlyPublicBool_AndJavasThreeConstructorsArePublic()
    {
        PropertyInfo? property = typeof(Node).GetProperty(nameof(Node.IsFenced), BindingFlags.Public | BindingFlags.Instance);

        Assert.NotNull(property);
        Assert.Equal(typeof(bool), property!.PropertyType);
        Assert.NotNull(property.GetGetMethod(nonPublic: false));
        Assert.Null(property.GetSetMethod(nonPublic: true));

        Assert.Equal(
            new[] { "Int32,String,Int32", "Int32,String,Int32,String", "Int32,String,Int32,String,Boolean" },
            typeof(Node).GetConstructors()
                .Select(static c => string.Join(",", c.GetParameters().Select(static p => p.ParameterType.Name)))
                .OrderBy(static s => s.Length));
    }

    [Fact]
    public void Node_NullHost_ThrowsArgumentNullException_FromEveryConstructor()
    {
        Assert.Equal("host", Assert.Throws<ArgumentNullException>(() => new Node(1, null!, 9092)).ParamName);
        Assert.Equal("host", Assert.Throws<ArgumentNullException>(() => new Node(1, null!, 9092, "r")).ParamName);
        Assert.Equal("host", Assert.Throws<ArgumentNullException>(() => new Node(1, null!, 9092, null, true)).ParamName);
    }

    [Fact]
    public void Node_FourArgumentConstructor_IsUnfenced()
    {
        Assert.False(new Node(5, "kafka-1", 9092, "r").IsFenced);
        Assert.False(new Node(5, "kafka-1", 9092, "r", isFenced: false).IsFenced);
        Assert.True(new Node(5, "kafka-1", 9092, "r", isFenced: true).IsFenced);
    }

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
        PartitionInfo info = new PartitionInfo(
            "t", 0, leader: null, Array.Empty<Node>(), Array.Empty<Node>(), Array.Empty<Node>());

        Assert.Null(info.Leader);
    }

    [Fact]
    public void PartitionInfo_ToString_MirrorsJavaFormat()
    {
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
