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
/// <see cref="TopicCollection"/> — Java's <c>org.apache.kafka.common.TopicCollection</c>.
/// Its whole job is to make "topic names <b>xor</b> topic ids" unrepresentable to violate,
/// so most of what is asserted here is a <em>closed</em> type hierarchy rather than
/// behaviour.
/// </summary>
public sealed class PublicTopicCollectionTests
{
    /// <summary>
    /// The two factories produce the two sealed subclasses — Java's <c>ofTopicNames</c> /
    /// <c>ofTopicIds</c>, whose return types are the subclasses, not the base.
    /// </summary>
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

    /// <summary>
    /// <b>No constructor is publicly reachable</b>, on the base or either subclass, so the
    /// factories are the only way in and no third subclass can exist outside this
    /// assembly. That is Java's <em>"subclassing this class beyond the classes provided
    /// here is not supported"</em>, expressed as a compile-time fact.
    /// </summary>
    /// <remarks>
    /// The base constructor is <c>private</c> exactly as Java's is; the two nested ones
    /// are <c>internal</c> rather than <c>private</c> because C#'s private access is not
    /// symmetric the way Java's is — a containing type cannot reach a nested type's
    /// private constructor (CS0122). What the caller can see is identical either way,
    /// which is what this asserts.
    /// </remarks>
    [Fact]
    public void NoConstructorIsPubliclyReachable()
    {
        Assert.Empty(typeof(TopicCollection).GetConstructors());
        Assert.Empty(typeof(TopicCollection.TopicNameCollection).GetConstructors());
        Assert.Empty(typeof(TopicCollection.TopicIdCollection).GetConstructors());

        Assert.True(typeof(TopicCollection).IsAbstract);
        Assert.True(typeof(TopicCollection.TopicNameCollection).IsSealed);
        Assert.True(typeof(TopicCollection.TopicIdCollection).IsSealed);

        // The base ctor Java also keeps private — the one that actually blocks an external
        // subclass, since a subclass must be able to call it.
        ConstructorInfo baseCtor = Assert.Single(
            typeof(TopicCollection).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(baseCtor.IsPrivate);
    }

    /// <summary>
    /// The accessors are <b>methods</b>, not properties — Java's <c>topicNames()</c> /
    /// <c>topicIds()</c> are methods, matching the shipped
    /// <c>Assignment()</c>/<c>Subscription()</c>/<c>Paused()</c> precedent for a returned
    /// snapshot.
    /// </summary>
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

    /// <summary>
    /// The factories <b>copy</b>, as Java's <c>new ArrayList&lt;&gt;(topics)</c> does — a
    /// later mutation of the caller's list must not be able to change a request that has
    /// already been built from it.
    /// </summary>
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

    /// <summary>
    /// A null collection is rejected at the factory, as Java's <c>new ArrayList&lt;&gt;</c>
    /// throws <c>NullPointerException</c> — and it is rejected naming <b>this API's own</b>
    /// parameter.
    /// </summary>
    /// <remarks>
    /// <c>ParamName</c> is asserted, not just the exception type. Without the explicit
    /// guard the throw comes out of <c>new List&lt;T&gt;(collection)</c> and reports
    /// <c>"collection"</c> — the BCL copy constructor's parameter, which this API does not
    /// have — so a type-only assertion passes either way. That is exactly how the defect
    /// survived: every other precondition site in this binding asserts the name (the
    /// admin surface alone does so for <c>"topics"</c>, <c>"newTopics"</c>, <c>"name"</c>
    /// and <c>"config"</c>), and a caller filtering with
    /// <c>when (e.ParamName == "topics")</c> would not have matched.
    /// </remarks>
    [Fact]
    public void Factories_RejectANullCollection_NamingTheirOwnParameter()
    {
        ArgumentNullException byName =
            Assert.Throws<ArgumentNullException>(() => TopicCollection.OfTopicNames(null!));
        Assert.Equal("topics", byName.ParamName);

        ArgumentNullException byId =
            Assert.Throws<ArgumentNullException>(() => TopicCollection.OfTopicIds(null!));
        Assert.Equal("topics", byId.ParamName);
    }

    /// <summary>
    /// <see cref="TopicCollection"/> lives at the root namespace, not under
    /// <c>Confluent.Kafka.Admin</c>, because Java's package is
    /// <c>org.apache.kafka.<b>common</b></c> — the same rule that placed
    /// <see cref="Uuid"/> and <see cref="Node"/> there. Pinned because moving a public
    /// type's namespace after publish is breaking.
    /// </summary>
    [Fact]
    public void LivesAtTheRootNamespace_LikeItsJavaPackage()
    {
        Assert.Equal("Confluent.Kafka", typeof(TopicCollection).Namespace);
        Assert.Equal("Confluent.Kafka", typeof(AclOperation).Namespace);
        Assert.Equal("Confluent.Kafka", typeof(TopicPartitionInfo).Namespace);

        // …while the admin-package types stay under Admin.
        Assert.Equal("Confluent.Kafka.Admin", typeof(Admin.TopicDescription).Namespace);
    }

    /// <summary>
    /// Every <see cref="AclOperation"/> member's numeric value is the Kafka <b>wire</b>
    /// code (Java's <c>AclOperation.code()</c>, <c>AclOperation.java:45-120</c>), not a
    /// C#-assigned ordinal. The C ABI transports these as bare <c>int32_t</c>, so an
    /// auto-assigned value would mislabel every operation the broker reports — and the
    /// first eight happen to coincide with the ordinals, so a partial check would pass.
    /// </summary>
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

        // Java has exactly these sixteen; a seventeenth added without a code would be
        // auto-assigned and silently wrong on the wire.
        Assert.Equal(16, Enum.GetValues(typeof(AclOperation)).Cast<AclOperation>().Count());
    }
}
