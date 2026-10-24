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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks <see cref="TopicCollection"/>, Java's <c>TopicCollection</c>: the closed hierarchy that keeps topic names and
/// topic ids apart, its copying factories and read-only accessors, its namespace, and <see cref="AclOperation"/>'s
/// wire codes.
/// </summary>
public sealed class PublicTopicCollectionTests
{
    [Fact]
    public void Factories_ProduceTheTwoSealedSubclasses()
    {
        TopicCollection names = TopicCollection.OfTopicNames(new[] { "alpha" });
        TopicCollection ids = TopicCollection.OfTopicIds(new[] { new Uuid(1L, 2L) });

        Assert.IsType<TopicCollection.TopicNameCollection>(names);
        Assert.IsType<TopicCollection.TopicIdCollection>(ids);

        Assert.Equal(
            typeof(TopicCollection.TopicNameCollection),
            typeof(TopicCollection).GetMethod(nameof(TopicCollection.OfTopicNames))!.ReturnType);
        Assert.Equal(
            typeof(TopicCollection.TopicIdCollection),
            typeof(TopicCollection).GetMethod(nameof(TopicCollection.OfTopicIds))!.ReturnType);
    }

    [Fact]
    public void NoConstructorIsPubliclyReachable()
    {
        Assert.Empty(typeof(TopicCollection).GetConstructors());
        Assert.Empty(typeof(TopicCollection.TopicNameCollection).GetConstructors());
        Assert.Empty(typeof(TopicCollection.TopicIdCollection).GetConstructors());

        Assert.True(typeof(TopicCollection).IsAbstract);
        Assert.True(typeof(TopicCollection.TopicNameCollection).IsSealed);
        Assert.True(typeof(TopicCollection.TopicIdCollection).IsSealed);

        // The private base constructor is what blocks a subclass outside this assembly.
        ConstructorInfo baseCtor = Assert.Single(
            typeof(TopicCollection).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(baseCtor.IsPrivate);
    }

    [Fact]
    public void Accessors_AreMethodsReturningReadOnlyCollections()
    {
        MethodInfo names = typeof(TopicCollection.TopicNameCollection)
            .GetMethod(nameof(TopicCollection.TopicNameCollection.TopicNames), Type.EmptyTypes)!;
        MethodInfo ids = typeof(TopicCollection.TopicIdCollection)
            .GetMethod(nameof(TopicCollection.TopicIdCollection.TopicIds), Type.EmptyTypes)!;

        Assert.Equal(typeof(IReadOnlyCollection<string>), names.ReturnType);
        Assert.Equal(typeof(IReadOnlyCollection<Uuid>), ids.ReturnType);

        Assert.DoesNotContain(
            typeof(TopicCollection.TopicNameCollection).GetProperties(),
            property => property.Name is "TopicNames" or "TopicIds");
    }

    [Fact]
    public void Factories_CopyTheCallersCollection()
    {
        List<string> mutableNames = new List<string> { "alpha" };
        TopicCollection.TopicNameCollection names = TopicCollection.OfTopicNames(mutableNames);
        mutableNames.Add("beta");

        Assert.Equal(new[] { "alpha" }, names.TopicNames());

        List<Uuid> mutableIds = new List<Uuid> { new Uuid(1L, 2L) };
        TopicCollection.TopicIdCollection ids = TopicCollection.OfTopicIds(mutableIds);
        mutableIds.Add(new Uuid(3L, 4L));

        Assert.Equal(new[] { new Uuid(1L, 2L) }, ids.TopicIds());
    }

    [Fact]
    public void Accessors_ReturnViewsThatCannotBeCastBackAndMutated()
    {
        TopicCollection.TopicNameCollection names = TopicCollection.OfTopicNames(new[] { "alpha", "beta" });

        IReadOnlyCollection<string> nameView = names.TopicNames();
        Assert.False(nameView is List<string>);
        ICollection<string> nameCollection = Assert.IsAssignableFrom<ICollection<string>>(nameView);
        Assert.True(nameCollection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => nameCollection.Add("x"));
        Assert.Equal(new[] { "alpha", "beta" }, names.TopicNames());

        Uuid first = new Uuid(1L, 2L);
        Uuid second = new Uuid(3L, 4L);
        TopicCollection.TopicIdCollection ids = TopicCollection.OfTopicIds(new[] { first, second });

        IReadOnlyCollection<Uuid> idView = ids.TopicIds();
        Assert.False(idView is List<Uuid>);
        ICollection<Uuid> idCollection = Assert.IsAssignableFrom<ICollection<Uuid>>(idView);
        Assert.True(idCollection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => idCollection.Add(new Uuid(5L, 6L)));
        Assert.Equal(new[] { first, second }, ids.TopicIds());
    }

    [Fact]
    public void Factories_RejectANullCollection_NamingTheirOwnParameter()
    {
        // Without the factory's own guard, the copy into a List<T> would throw naming "collection".
        ArgumentNullException byName =
            Assert.Throws<ArgumentNullException>(() => TopicCollection.OfTopicNames(null!));
        Assert.Equal("topics", byName.ParamName);

        ArgumentNullException byId =
            Assert.Throws<ArgumentNullException>(() => TopicCollection.OfTopicIds(null!));
        Assert.Equal("topics", byId.ParamName);
    }

    [Fact]
    public void LivesAtTheRootNamespace_LikeItsJavaPackage()
    {
        Assert.Equal("Confluent.Kafka", typeof(TopicCollection).Namespace);
        Assert.Equal("Confluent.Kafka", typeof(AclOperation).Namespace);
        Assert.Equal("Confluent.Kafka", typeof(TopicPartitionInfo).Namespace);

        Assert.Equal("Confluent.Kafka.Admin", typeof(Admin.TopicDescription).Namespace);
    }

    [Fact]
    public void AclOperation_MembersCarryJavasWireCodes()
    {
        Assert.Equal(0, (int)AclOperation.Unknown);
        Assert.Equal(1, (int)AclOperation.Any);
        Assert.Equal(2, (int)AclOperation.All);
        Assert.Equal(3, (int)AclOperation.Read);
        Assert.Equal(4, (int)AclOperation.Write);
        Assert.Equal(5, (int)AclOperation.Create);
        Assert.Equal(6, (int)AclOperation.Delete);
        Assert.Equal(7, (int)AclOperation.Alter);
        Assert.Equal(8, (int)AclOperation.Describe);
        Assert.Equal(9, (int)AclOperation.ClusterAction);
        Assert.Equal(10, (int)AclOperation.DescribeConfigs);
        Assert.Equal(11, (int)AclOperation.AlterConfigs);
        Assert.Equal(12, (int)AclOperation.IdempotentWrite);
        Assert.Equal(13, (int)AclOperation.CreateTokens);
        Assert.Equal(14, (int)AclOperation.DescribeTokens);
        Assert.Equal(15, (int)AclOperation.TwoPhaseCommit);

        Assert.Equal(16, Enum.GetValues(typeof(AclOperation)).Cast<AclOperation>().Count());
    }
}
