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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the <b>public shape</b> of M15/P3 Stage 1's surface against the Java classes it
/// mirrors.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>C# upcasts and widens silently, so these have to be reflection assertions.</b> A
/// behavioural test passes equally against <c>Task&lt;Node&gt;</c> and
/// <c>Task&lt;Node?&gt;</c>, against a property and a method, and against a collection and
/// a set — M15/P1 shipped three shape defects through a green build, 883 green tests and a
/// 0-High first review.
/// </para>
/// <para>
/// ⚠ <b>Return-nullability is read from the Java <em>javadoc and body</em>, not the
/// signature.</b> Java has no nullable-reference annotations. This slice has <b>two</b>
/// nullable returns and each cites the sentence that decides it:
/// <c>DescribeClusterResult.authorizedOperations()</c>'s javadoc says the value "will be
/// non-null if the broker supplied this information, and null otherwise"
/// (<c>DescribeClusterResult.java:71-73</c>), and <c>controller()</c>'s <em>body</em>
/// decides the other — <c>KafkaAdminClient.java:2531-2534</c> returns <c>null</c> when the
/// controller id is <c>NO_CONTROLLER_ID</c>.
/// </para>
/// </remarks>
public sealed class PublicAdminP3ShapeParityTests
{
    /// <summary>
    /// The three new RPCs return their <c>*Result</c> <b>synchronously</b> — Java's
    /// <c>Admin</c> methods do not block, so the <see cref="Task"/> mapping belongs on the
    /// futures inside the result, never on the method (<c>admin-client.md</c> §1). This is
    /// DoD §11's spirit for admin.
    /// </summary>
    [Fact]
    public void IAdmin_TheThreeNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo describeCluster = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeCluster))!;
        MethodInfo listConfigResources = typeof(IAdmin).GetMethod(nameof(IAdmin.ListConfigResources))!;
#pragma warning disable CS0618 // Java deprecates this RPC; the deprecation is what is asserted.
        MethodInfo listClientMetrics = typeof(IAdmin).GetMethod(nameof(IAdmin.ListClientMetricsResources))!;

        Assert.Equal(typeof(DescribeClusterResult), describeCluster.ReturnType);
        Assert.Equal(typeof(ListConfigResourcesResult), listConfigResources.ReturnType);
        Assert.Equal(typeof(ListClientMetricsResourcesResult), listClientMetrics.ReturnType);

        // describeCluster(DescribeClusterOptions)
        Assert.Equal(
            new[] { typeof(DescribeClusterOptions) },
            describeCluster.GetParameters().Select(parameter => parameter.ParameterType));

        // listConfigResources(Set<ConfigResource.Type>, ListConfigResourcesOptions) —
        // IReadOnlySet<T> post-dates the netstandard2.0 floor (CLAUDE.md §3's idiom map).
        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<ConfigResourceType>), typeof(ListConfigResourcesOptions) },
            listConfigResources.GetParameters().Select(parameter => parameter.ParameterType));

        // listClientMetricsResources(ListClientMetricsResourcesOptions)
        Assert.Equal(
            new[] { typeof(ListClientMetricsResourcesOptions) },
            listClientMetrics.GetParameters().Select(parameter => parameter.ParameterType));

        foreach (MethodInfo rpc in new[] { describeCluster, listConfigResources, listClientMetrics })
#pragma warning restore CS0618
        {
            foreach (ParameterInfo parameter in rpc.GetParameters())
            {
                Assert.True(parameter.IsOptional, $"{rpc.Name}.{parameter.Name} must be optional");
                Assert.Equal(NullableAnnotation.Annotated, NullableFlag(parameter));
            }
        }

        // Close remains the ONLY Task-returning member on IAdmin — the P1/P2a/P2b
        // invariant, re-asserted because three new members just landed beside it.
        Assert.Equal(
            new[] { nameof(IAdmin.Close) },
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// <see cref="DescribeClusterResult"/> publishes Java's four accessors as <b>methods</b>
    /// yielding tasks, two of them nullable.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The two nullable returns are the point of this test.</b> A binding that read the
    /// authorized-operations <em>count</em> instead of the gate would still compile against
    /// a non-nullable <c>Task&lt;IReadOnlyCollection&lt;AclOperation&gt;&gt;</c>, so the
    /// annotation is what forces the null to remain expressible at all.
    /// </remarks>
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

        Assert.Equal(typeof(Task<IReadOnlyCollection<Node>>), nodes.ReturnType);
        Assert.Equal(typeof(Task<Node>), controller.ReturnType);
        Assert.Equal(typeof(Task<string>), clusterId.ReturnType);
        Assert.Equal(typeof(Task<IReadOnlyCollection<AclOperation>>), authorizedOperations.ReturnType);

        // ⚠ The nullability under test is the TASK'S VALUE, i.e. position 1 of the
        // flattened return type — position 0 is the Task itself, which is never null and
        // is identical for Task<Node> and Task<Node?>. An assertion written against
        // position 0 here would be green however the value is widened.
        foreach (MethodInfo accessor in new[] { nodes, controller, clusterId, authorizedOperations })
        {
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableFlag(accessor.ReturnParameter));
        }

        // Non-null values: Java's nodes() and clusterId() futures always carry one.
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(nodes.ReturnParameter, 1));
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(clusterId.ReturnParameter, 1));

        // Nullable values: see the class remarks for the Java citation behind each.
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(controller.ReturnParameter, 1));
        Assert.Equal(
            NullableAnnotation.Annotated, NullableAnnotation.Flag(authorizedOperations.ReturnParameter, 1));

        // Java's result has no properties and no other accessor.
        Assert.Empty(typeof(DescribeClusterResult).GetProperties());

        // Java's constructor is package-private; nothing public may build one.
        Assert.Empty(typeof(DescribeClusterResult).GetConstructors());
    }

    /// <summary>
    /// ⚠ <b>There is no public <c>ClusterDescription</c> type (M15/P3 decision D12).</b>
    /// Java has no such class, so publishing one would be a
    /// <c>definition-of-done.md</c> §7 violation. The aggregate the four projections derive
    /// from is <c>internal</c>.
    /// </summary>
    /// <remarks>
    /// The whole public surface of the assembly is swept, not just the two admin
    /// namespaces, so the type cannot reappear somewhere else and satisfy a narrower check.
    /// </remarks>
    [Fact]
    public void NoPublicClusterDescriptionType_Exists()
    {
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("ClusterDescription", StringComparison.Ordinal) >= 0);
    }

    /// <summary>
    /// The two list results publish Java's <b>single</b> accessor each, over a
    /// <em>collection</em>, and neither invents a second view.
    /// </summary>
    [Fact]
    public void TheTwoListResults_PublishExactlyJavasSingleAccessor()
    {
        MethodInfo configResources = typeof(ListConfigResourcesResult).GetMethod(
            nameof(ListConfigResourcesResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(Task<IReadOnlyCollection<ConfigResource>>), configResources.ReturnType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableFlag(configResources.ReturnParameter));

        // Position 1 — the task's VALUE. Java's all() always carries a collection.
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(configResources.ReturnParameter, 1));
        Assert.Empty(typeof(ListConfigResourcesResult).GetProperties());
        Assert.Empty(typeof(ListConfigResourcesResult).GetConstructors());
        Assert.Single(DeclaredPublicMethods(typeof(ListConfigResourcesResult)));

#pragma warning disable CS0618 // Java deprecates this result type; the deprecation is what is asserted.
        MethodInfo clientMetrics = typeof(ListClientMetricsResourcesResult).GetMethod(
            nameof(ListClientMetricsResourcesResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(Task<IReadOnlyCollection<ClientMetricsResourceListing>>), clientMetrics.ReturnType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableFlag(clientMetrics.ReturnParameter));
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(clientMetrics.ReturnParameter, 1));
        Assert.Empty(typeof(ListClientMetricsResourcesResult).GetProperties());
        Assert.Empty(typeof(ListClientMetricsResourcesResult).GetConstructors());
        Assert.Single(DeclaredPublicMethods(typeof(ListClientMetricsResourcesResult)));
#pragma warning restore CS0618
    }

    /// <summary>
    /// Java's <c>@Deprecated(since = "4.1")</c> is carried onto <b>every</b> member and type
    /// Java marks, not only the RPC — so a later refactor cannot silently drop the
    /// deprecation.
    /// </summary>
    /// <remarks>
    /// Java deprecates the RPC with <c>forRemoval = true</c>
    /// (<c>Admin.java:1821-1824</c>, <c>:1833-1836</c>) and the three supporting types
    /// without it (<c>ListClientMetricsResourcesResult.java:30</c>,
    /// <c>ListClientMetricsResourcesOptions.java:24</c>,
    /// <c>ClientMetricsResourceListing.java:21</c>). All four carry
    /// <see cref="ObsoleteAttribute"/> here, each naming
    /// <c>ListConfigResources</c> as the replacement — the guidance Java's own
    /// <c>@deprecated</c> tag gives.
    /// </remarks>
    [Fact]
    public void TheDeprecatedClientMetricsSurface_IsMarkedObsolete()
    {
        MemberInfo[] deprecated =
        {
            typeof(IAdmin).GetMethod(nameof(IAdmin.ListClientMetricsResources))!,
            typeof(KafkaAdminClient).GetMethod(nameof(KafkaAdminClient.ListClientMetricsResources))!,
            typeof(MockAdminClient).GetMethod(nameof(MockAdminClient.ListClientMetricsResources))!,
#pragma warning disable CS0618 // These types being obsolete is exactly the assertion.
            typeof(ListClientMetricsResourcesResult),
            typeof(ListClientMetricsResourcesOptions),
            typeof(ClientMetricsResourceListing),
#pragma warning restore CS0618
        };

        foreach (MemberInfo member in deprecated)
        {
            ObsoleteAttribute? obsolete = member.GetCustomAttribute<ObsoleteAttribute>();
            Assert.NotNull(obsolete);
            Assert.Contains("4.1", obsolete!.Message, StringComparison.Ordinal);
            Assert.Contains("ListConfigResources", obsolete.Message, StringComparison.Ordinal);
        }

        // …and the replacement surface is NOT deprecated.
        Assert.Null(
            typeof(IAdmin).GetMethod(nameof(IAdmin.ListConfigResources))!
                .GetCustomAttribute<ObsoleteAttribute>());
        Assert.Null(typeof(ListConfigResourcesResult).GetCustomAttribute<ObsoleteAttribute>());
    }

    /// <summary>
    /// <see cref="ConfigResourceType"/>'s members carry Java's <c>Type.id()</c> values, not
    /// C#-assigned ordinals — they cross the ABI as <c>int32_t</c> in <b>both</b>
    /// directions.
    /// </summary>
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

    /// <summary>The enum has exactly Java's six members, and no more.</summary>
    [Fact]
    public void ConfigResourceType_HasExactlyJavasSixMembers()
    {
        Assert.Equal(
            new[] { "Broker", "BrokerLogger", "ClientMetrics", "Group", "Topic", "Unknown" },
            Enum.GetNames(typeof(ConfigResourceType)).OrderBy(name => name, StringComparer.Ordinal));

        // The root namespace, beside Node / AclOperation / TopicCollection (D13/D16) — NOT
        // Confluent.Kafka.Admin, which is where confluent-kafka-dotnet puts its own.
        Assert.Equal("Confluent.Kafka", typeof(ConfigResourceType).Namespace);
        Assert.Equal("Confluent.Kafka", typeof(ConfigResource).Namespace);
    }

    /// <summary>
    /// <see cref="ConfigResource"/> mirrors Java's constructor and three accessors, and its
    /// <b>value equality</b> — which Stage 2 keys dictionaries on.
    /// </summary>
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

        // The dictionary use Stage 2 depends on: distinct keys stay distinct.
        Dictionary<ConfigResource, int> byResource = new Dictionary<ConfigResource, int>
        {
            [topic] = 1,
            [sameNameOtherType] = 2,
            [otherName] = 3,
        };
        Assert.Equal(3, byResource.Count);
        Assert.Equal(1, byResource[sameTopic]);

        // isDefault(): the default resource of a type has an EMPTY name (ConfigResource.java:96).
        Assert.True(new ConfigResource(ConfigResourceType.Broker, string.Empty).IsDefault);
        Assert.False(topic.IsDefault);

        Assert.Equal("ConfigResource(type=Topic, name='t')", topic.ToString());
        Assert.Throws<ArgumentNullException>(() => new ConfigResource(ConfigResourceType.Topic, null!));
    }

    /// <summary>
    /// <see cref="ClientMetricsResourceListing"/> mirrors Java's constructor, its one
    /// accessor and its value equality.
    /// </summary>
    [Fact]
    public void ClientMetricsResourceListing_MirrorsJavasShape()
    {
#pragma warning disable CS0618 // Java deprecates the listing type; mirrored, not avoided.
        ConstructorInfo only = Assert.Single(typeof(ClientMetricsResourceListing).GetConstructors());
        Assert.Equal(
            new[] { typeof(string) }, only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            typeof(string),
            typeof(ClientMetricsResourceListing).GetProperty(nameof(ClientMetricsResourceListing.Name))!.PropertyType);

        ClientMetricsResourceListing listing = new ClientMetricsResourceListing("sub-1");
        Assert.Equal("sub-1", listing.Name);
        Assert.Equal(new ClientMetricsResourceListing("sub-1"), listing);
        Assert.Equal(new ClientMetricsResourceListing("sub-1").GetHashCode(), listing.GetHashCode());
        Assert.NotEqual(new ClientMetricsResourceListing("sub-2"), listing);

        // Java's toString() leaves the opening quote unclosed (:48-50); mirrored verbatim.
        Assert.Equal("ClientMetricsResourceListing(name='sub-1)", listing.ToString());

        Assert.Throws<ArgumentNullException>(() => new ClientMetricsResourceListing(null!));
#pragma warning restore CS0618
    }

    /// <summary>
    /// The three new options types match Java's fields and defaults exactly, so
    /// <c>options: null</c> at a call site behaves like a freshly constructed instance.
    /// </summary>
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

        // Java's ListConfigResourcesOptions declares NO members of its own — it is an empty
        // subclass of AbstractOptions, so the inherited timeout is the whole surface.
        ListConfigResourcesOptions configResources = new ListConfigResourcesOptions();
        Assert.Null(configResources.TimeoutMs);
        AssertOptionNames(typeof(ListConfigResourcesOptions), nameof(configResources.TimeoutMs));

#pragma warning disable CS0618 // Java deprecates this options type; mirrored, not avoided.
        ListClientMetricsResourcesOptions clientMetrics = new ListClientMetricsResourcesOptions();
        Assert.Null(clientMetrics.TimeoutMs);
        AssertOptionNames(typeof(ListClientMetricsResourcesOptions), nameof(clientMetrics.TimeoutMs));

        foreach (Type type in new[]
                 {
                     typeof(DescribeClusterOptions),
                     typeof(ListConfigResourcesOptions),
                     typeof(ListClientMetricsResourcesOptions),
                 })
#pragma warning restore CS0618
        {
            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);
        }
    }

    /// <summary>
    /// The type's own public instance methods — <see cref="BindingFlags.DeclaredOnly"/>, so
    /// <see cref="object"/>'s inherited members do not count towards "exactly one accessor".
    /// </summary>
    private static MethodInfo[] DeclaredPublicMethods(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);

    private static void AssertOptionNames(Type optionsType, params string[] expected) =>
        Assert.Equal(
            expected.OrderBy(name => name, StringComparer.Ordinal),
            optionsType.GetProperties().Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal));

    private static byte NullableFlag(MemberInfo member) => NullableAnnotation.Flag(member);

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}
