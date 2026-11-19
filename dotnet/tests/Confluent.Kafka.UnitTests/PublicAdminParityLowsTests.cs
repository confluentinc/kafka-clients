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
using System.Reflection;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Two small Java-parity points: the result collections Java returns as unmodifiable views, and the closed SCRAM
/// alteration hierarchy.
/// </summary>
public sealed class PublicAdminParityLowsTests
{
    [Fact]
    public void G3_11_PartitionReassignment_ListsAreReadOnly()
    {
        PartitionReassignment reassignment =
            new PartitionReassignment(new[] { 1, 2, 3 }, new[] { 3 }, new[] { 1 });

        foreach (IReadOnlyList<int> list in new[]
        {
            reassignment.Replicas(),
            reassignment.AddingReplicas(),
            reassignment.RemovingReplicas(),
        })
        {
            IList<int> mutable = Assert.IsAssignableFrom<IList<int>>(list);
            Assert.True(mutable.IsReadOnly);
            Assert.Throws<NotSupportedException>(() => mutable.Add(9));
            Assert.Throws<NotSupportedException>(() => mutable.Clear());
        }

        Assert.Equal(new[] { 1, 2, 3 }, reassignment.Replicas());
        Assert.Equal(new[] { 3 }, reassignment.AddingReplicas());
        Assert.Equal(new[] { 1 }, reassignment.RemovingReplicas());
    }

    [Fact]
    public void G3_11_LogDirDescription_ReplicaInfosIsAReadOnlyView()
    {
        TopicPartition first = new TopicPartition("t", 0);
        TopicPartition second = new TopicPartition("t", 1);
        Dictionary<TopicPartition, ReplicaInfo> replicas = new Dictionary<TopicPartition, ReplicaInfo>
        {
            [first] = new ReplicaInfo(1, 2, false),
        };

        LogDirDescription description = new LogDirDescription(null, replicas);

        IDictionary<TopicPartition, ReplicaInfo> mutable =
            Assert.IsAssignableFrom<IDictionary<TopicPartition, ReplicaInfo>>(description.ReplicaInfos());
        Assert.True(mutable.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => mutable.Clear());
        Assert.Throws<NotSupportedException>(() => mutable.Add(second, new ReplicaInfo(3, 4, true)));
        Assert.Single(description.ReplicaInfos());

        replicas[second] = new ReplicaInfo(3, 4, true);
        Assert.Equal(2, description.ReplicaInfos().Count);
    }

    [Fact]
    public void G4_13_UserScramCredentialAlteration_IsClosedToOtherAssemblies()
    {
        ConstructorInfo constructor = Assert.Single(
            typeof(UserScramCredentialAlteration).GetConstructors(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic));

        Assert.True(constructor.IsFamilyAndAssembly, "expected a private protected constructor");
        Assert.True(typeof(UserScramCredentialUpsertion).IsSealed);
        Assert.True(typeof(UserScramCredentialDeletion).IsSealed);
    }
}
