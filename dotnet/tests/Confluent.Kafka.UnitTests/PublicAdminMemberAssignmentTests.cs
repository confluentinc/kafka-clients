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
using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Shape and behaviour of <see cref="MemberAssignment"/> against Java's
/// <c>org.apache.kafka.clients.admin.MemberAssignment</c>.
/// </summary>
public sealed class PublicAdminMemberAssignmentTests
{
    [Fact]
    public void Constructor_IsJavasSingleOne()
    {
        var parameters = typeof(MemberAssignment)
            .GetConstructors()
            .Select(static constructor => constructor.GetParameters().Select(static p => p.ParameterType).ToArray())
            .ToArray();

        var only = Assert.Single(parameters);
        Assert.Equal(new[] { typeof(IEnumerable<TopicPartition>) }, only);
    }

    [Fact]
    public void Accessor_IsJavasSingleOne_AsAReadOnlyProperty()
    {
        var property = typeof(MemberAssignment).GetProperty(nameof(MemberAssignment.TopicPartitions));

        Assert.NotNull(property);
        Assert.Equal(typeof(IReadOnlyCollection<TopicPartition>), property!.PropertyType);
        Assert.Null(property.SetMethod);
    }

    [Fact]
    public void TheTypeIsNotDeprecated_BecauseJavasIsNot()
    {
        // Java's MemberAssignment carries no @Deprecated; neither does this type. The
        // attribute is mirrored only where Java's own member carries it.
        Assert.Null(typeof(MemberAssignment).GetCustomAttribute<ObsoleteAttribute>());
    }

    [Fact]
    public void NullPartitions_BecomeAnEmptyAssignment()
    {
        // Java :38 — topicPartitions == null ? Collections.emptySet() : ...
        var assignment = new MemberAssignment(null);

        Assert.Empty(assignment.TopicPartitions);
        Assert.Equal("(topicPartitions=)", assignment.ToString());
    }

    [Fact]
    public void ThePartitionsAreCopiedDefensively()
    {
        // Java :38 — Set.copyOf(...).
        var source = new List<TopicPartition> { new TopicPartition("orders", 0) };
        var assignment = new MemberAssignment(source);

        source.Add(new TopicPartition("orders", 1));

        Assert.Equal(new[] { new TopicPartition("orders", 0) }, assignment.TopicPartitions);
    }

    [Fact]
    public void DuplicatePartitions_CollapseLikeAJavaSet()
    {
        var assignment = new MemberAssignment(new[]
        {
            new TopicPartition("orders", 0),
            new TopicPartition("orders", 0),
            new TopicPartition("orders", 1),
        });

        Assert.Equal(
            new[] { new TopicPartition("orders", 0), new TopicPartition("orders", 1) },
            assignment.TopicPartitions);
    }

    [Fact]
    public void Equality_IsOrderInsensitive_AsAJavaSetIs()
    {
        var forwards = new MemberAssignment(new[]
        {
            new TopicPartition("orders", 0),
            new TopicPartition("payments", 7),
        });
        var backwards = new MemberAssignment(new[]
        {
            new TopicPartition("payments", 7),
            new TopicPartition("orders", 0),
        });

        Assert.True(forwards.Equals(backwards));
        Assert.Equal(forwards.GetHashCode(), backwards.GetHashCode());
    }

    [Fact]
    public void Equality_DiscriminatesOnTheAssignedPartitions()
    {
        var assignment = new MemberAssignment(new[] { new TopicPartition("orders", 0) });

        Assert.True(assignment.Equals(new MemberAssignment(new[] { new TopicPartition("orders", 0) })));
        Assert.True(assignment.Equals(assignment));

        // A different partition, a different topic, a superset and the empty set all differ.
        Assert.False(assignment.Equals(new MemberAssignment(new[] { new TopicPartition("orders", 1) })));
        Assert.False(assignment.Equals(new MemberAssignment(new[] { new TopicPartition("payments", 0) })));
        Assert.False(assignment.Equals(new MemberAssignment(new[]
        {
            new TopicPartition("orders", 0),
            new TopicPartition("orders", 1),
        })));
        Assert.False(assignment.Equals(new MemberAssignment(null)));

        Assert.False(assignment.Equals(null));
        Assert.False(assignment.Equals("(topicPartitions=orders-0)"));
    }

    [Fact]
    public void GetHashCode_IsTheSumOfTheElementHashes_AsJavasSetHashCodeIs()
    {
        // Java :52 returns topicPartitions.hashCode(), and Set.hashCode() is specified as
        // the sum of the element hashes.
        var partitions = new[] { new TopicPartition("orders", 0), new TopicPartition("payments", 7) };
        var expected = unchecked(partitions[0].GetHashCode() + partitions[1].GetHashCode());

        Assert.Equal(expected, new MemberAssignment(partitions).GetHashCode());
        Assert.Equal(0, new MemberAssignment(null).GetHashCode());
    }

    [Fact]
    public void ToString_MirrorsJavasRendering()
    {
        // Java :64-66 — "(topicPartitions=" + joined-with-"," + ")".
        var assignment = new MemberAssignment(new[]
        {
            new TopicPartition("orders", 0),
            new TopicPartition("payments", 7),
        });

        Assert.Equal("(topicPartitions=orders-0,payments-7)", assignment.ToString());
    }
}
