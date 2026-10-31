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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public shape of <c>DescribeCluster</c> and <c>ListConfigResources</c>, their options, results and value types
/// (<see cref="ConfigResource"/> and <see cref="ConfigResourceType"/>), against the Java classes they mirror.
/// </summary>
/// <remarks>
/// These are reflection assertions. Return nullability follows Java's javadoc and body: the
/// <c>AuthorizedOperations</c> of <c>DescribeClusterResult</c> is null when the broker did not supply it, and its
/// <c>Controller</c> is null when there is no controller id.
/// </remarks>
public sealed class PublicAdminP3ShapeParityTests
{
    [Fact]
    public void IAdmin_TheNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo describeCluster = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeCluster))!;
        MethodInfo listConfigResources = typeof(IAdmin).GetMethod(nameof(IAdmin.ListConfigResources))!;

        Assert.Equal(typeof(DescribeClusterResult), describeCluster.ReturnType);
        Assert.Equal(typeof(ListConfigResourcesResult), listConfigResources.ReturnType);

        Assert.Equal(
            new[] { typeof(DescribeClusterOptions) },
            describeCluster.GetParameters().Select(parameter => parameter.ParameterType));

        // IReadOnlySet<T> post-dates netstandard2.0, so Java's Set parameter is an IReadOnlyCollection.
        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<ConfigResourceType>), typeof(ListConfigResourcesOptions) },
            listConfigResources.GetParameters().Select(parameter => parameter.ParameterType));

        foreach (MethodInfo rpc in new[] { describeCluster, listConfigResources })
        {
            foreach (ParameterInfo parameter in rpc.GetParameters())
            {
                Assert.True(parameter.IsOptional, $"{rpc.Name}.{parameter.Name} must be optional");
                Assert.Equal(NullableAnnotation.Annotated, NullableFlag(parameter));
            }
        }

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void DescribeClusterResult_PublishesJavasFourAccessors()
    {
        MethodInfo nodes = typeof(DescribeClusterResult).GetMethod(
            nameof(DescribeClusterResult.Nodes), Type.EmptyTypes)!;
        MethodInfo controller = typeof(DescribeClusterResult).GetMethod(
            nameof(DescribeClusterResult.Controller), Type.EmptyTypes)!;
        MethodInfo clusterId = typeof(DescribeClusterResult).GetMethod(
            nameof(DescribeClusterResult.ClusterId), Type.EmptyTypes)!;
        MethodInfo authorizedOperations = typeof(DescribeClusterResult).GetMethod(
            nameof(DescribeClusterResult.AuthorizedOperations), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<Node>>), nodes.ReturnType);
        Assert.Equal(typeof(KafkaFuture<Node>), controller.ReturnType);
        Assert.Equal(typeof(KafkaFuture<string>), clusterId.ReturnType);
        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<AclOperation>>), authorizedOperations.ReturnType);

        // The nullability under test is the future's value, position 1 of the flattened return type; the future is a struct,
        // so position 0 carries no annotation.

        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(nodes.ReturnParameter, 1));
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(clusterId.ReturnParameter, 1));

        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(controller.ReturnParameter, 1));
        Assert.Equal(
            NullableAnnotation.Annotated, NullableAnnotation.Flag(authorizedOperations.ReturnParameter, 1));

#if NET8_0_OR_GREATER
        // The same reading through the runtime's own decoder (.NET 6+), as an independent check.
        NullabilityInfo clusterIdInfo = new NullabilityInfoContext().Create(clusterId.ReturnParameter);
        Assert.Equal(NullabilityState.NotNull, clusterIdInfo.ReadState);
        Assert.Equal(NullabilityState.NotNull, Assert.Single(clusterIdInfo.GenericTypeArguments).ReadState);
        NullabilityInfo nodesInfo = new NullabilityInfoContext().Create(nodes.ReturnParameter);
        Assert.Equal(NullabilityState.NotNull, Assert.Single(nodesInfo.GenericTypeArguments).ReadState);
#endif

        Assert.Empty(typeof(DescribeClusterResult).GetProperties());

        Assert.Empty(typeof(DescribeClusterResult).GetConstructors());
    }

    [Fact]
    public void NoPublicClusterDescriptionType_Exists()
    {
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("ClusterDescription", StringComparison.Ordinal) >= 0);
    }

    [Fact]
    public void TheListResult_PublishesExactlyJavasSingleAccessor()
    {
        MethodInfo configResources = typeof(ListConfigResourcesResult).GetMethod(
            nameof(ListConfigResourcesResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<ConfigResource>>), configResources.ReturnType);

        // Position 1, the future's value: Java's all() always carries a collection.
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(configResources.ReturnParameter, 1));
        Assert.Empty(typeof(ListConfigResourcesResult).GetProperties());
        Assert.Empty(typeof(ListConfigResourcesResult).GetConstructors());
        Assert.Single(DeclaredPublicMethods(typeof(ListConfigResourcesResult)));
    }

    [Theory]
    [InlineData(ConfigResourceType.Unknown, 0)]
    [InlineData(ConfigResourceType.Topic, 2)]
    [InlineData(ConfigResourceType.Broker, 4)]
    [InlineData(ConfigResourceType.BrokerLogger, 8)]
    [InlineData(ConfigResourceType.ClientMetrics, 16)]
    [InlineData(ConfigResourceType.Group, 32)]
    public void ConfigResourceType_CarriesJavasIds(ConfigResourceType type, int id)
    {
        Assert.Equal(id, (int)type);
        Assert.Equal(type, (ConfigResourceType)id);
    }

    [Fact]
    public void ConfigResourceType_HasExactlyJavasSixMembers()
    {
        Assert.Equal(
            new[] { "Broker", "BrokerLogger", "ClientMetrics", "Group", "Topic", "Unknown" },
            Enum.GetNames(typeof(ConfigResourceType)).OrderBy(name => name, StringComparer.Ordinal));
    }

    [Fact]
    public void ConfigResource_MirrorsJavasShapeAndValueEquality()
    {
        ConstructorInfo only = Assert.Single(typeof(ConfigResource).GetConstructors());
        Assert.Equal(
            new[] { typeof(ConfigResourceType), typeof(string) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            typeof(ConfigResourceType), typeof(ConfigResource).GetProperty(nameof(ConfigResource.Type))!.PropertyType);
        Assert.Equal(typeof(string), typeof(ConfigResource).GetProperty(nameof(ConfigResource.Name))!.PropertyType);
        Assert.Equal(typeof(bool), typeof(ConfigResource).GetProperty(nameof(ConfigResource.IsDefault))!.PropertyType);
        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableFlag(typeof(ConfigResource).GetProperty(nameof(ConfigResource.Name))!));

        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, "t");
        ConfigResource sameTopic = new ConfigResource(ConfigResourceType.Topic, "t");
        ConfigResource sameNameOtherType = new ConfigResource(ConfigResourceType.Broker, "t");
        ConfigResource otherName = new ConfigResource(ConfigResourceType.Topic, "u");

        Assert.Equal(topic, sameTopic);
        Assert.Equal(topic.GetHashCode(), sameTopic.GetHashCode());
        Assert.NotEqual(topic, sameNameOtherType);
        Assert.NotEqual(topic, otherName);

        Dictionary<ConfigResource, int> byResource = new Dictionary<ConfigResource, int>
        {
            [topic] = 1,
            [sameNameOtherType] = 2,
            [otherName] = 3,
        };
        Assert.Equal(3, byResource.Count);
        Assert.Equal(1, byResource[sameTopic]);

        // isDefault(): the default resource of a type has an empty name.
        Assert.True(new ConfigResource(ConfigResourceType.Broker, string.Empty).IsDefault);
        Assert.False(topic.IsDefault);

        // Java's constant name, not the .NET member name.
        Assert.Equal("ConfigResource(type=TOPIC, name='t')", topic.ToString());
        Assert.Throws<ArgumentNullException>(() => new ConfigResource(ConfigResourceType.Topic, null!));
    }

    [Theory]
    [InlineData(64)]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(-1)]
    [InlineData(255)]
    [InlineData(int.MaxValue)]
    public void ConfigResource_AnUndefinedType_IsStoredAsUnknown(int undefinedId)
    {
        Assert.False(Enum.IsDefined(typeof(ConfigResourceType), (ConfigResourceType)undefinedId));

        ConfigResource resource = new ConfigResource((ConfigResourceType)undefinedId, "x");

        Assert.Equal(ConfigResourceType.Unknown, resource.Type);
        Assert.Equal("x", resource.Name);
    }

    [Fact]
    public void ConfigResource_EveryDefinedType_IsPreserved()
    {
        ConfigResourceType[] defined = (ConfigResourceType[])Enum.GetValues(typeof(ConfigResourceType));
        Assert.Equal(6, defined.Length);

        foreach (ConfigResourceType type in defined)
        {
            Assert.Equal(type, new ConfigResource(type, "x").Type);
        }
    }

    [Fact]
    public void ConfigResource_AnUndefinedType_EqualsAndRendersAsUnknown()
    {
        ConfigResource undefined = new ConfigResource((ConfigResourceType)64, "x");
        ConfigResource unknown = new ConfigResource(ConfigResourceType.Unknown, "x");

        Assert.Equal(unknown, undefined);
        Assert.Equal(unknown.GetHashCode(), undefined.GetHashCode());
        Assert.Equal("ConfigResource(type=UNKNOWN, name='x')", undefined.ToString());

        Dictionary<ConfigResource, int> byResource = new Dictionary<ConfigResource, int>
        {
            [undefined] = 1,
            [unknown] = 2,
        };
        Assert.Equal(2, Assert.Single(byResource).Value);
    }

    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        DescribeClusterOptions cluster = new DescribeClusterOptions();
        Assert.Null(cluster.TimeoutMs);
        Assert.False(cluster.IncludeAuthorizedOperations);
        Assert.False(cluster.IncludeFencedBrokers);
        AssertOptionNames(
            typeof(DescribeClusterOptions),
            nameof(DescribeClusterOptions.IncludeAuthorizedOperations),
            nameof(DescribeClusterOptions.IncludeFencedBrokers),
            nameof(cluster.TimeoutMs));

        // Java's ListConfigResourcesOptions declares no members of its own, so the inherited timeout is the whole surface.
        ListConfigResourcesOptions configResources = new ListConfigResourcesOptions();
        Assert.Null(configResources.TimeoutMs);
        AssertOptionNames(typeof(ListConfigResourcesOptions), nameof(configResources.TimeoutMs));

        foreach (Type type in new[]
                 {
                     typeof(DescribeClusterOptions),
                     typeof(ListConfigResourcesOptions),
                 })
        {
            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);
        }
    }

    private static MethodInfo[] DeclaredPublicMethods(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);

    private static void AssertOptionNames(Type optionsType, params string[] expected) =>
        Assert.Equal(
            expected.OrderBy(name => name, StringComparer.Ordinal),
            optionsType.GetProperties().Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal));

    private static byte NullableFlag(MemberInfo member) => NullableAnnotation.Flag(member);

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}
