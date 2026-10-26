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
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public shape of <c>ListPartitionReassignments</c> and <c>ListOffsets</c>, their options, results and value
/// types, against the Java classes they mirror, with all seven <see cref="OffsetSpec"/> kinds walked explicitly.
/// </summary>
/// <remarks>
/// Three shapes across two similarly named families: <c>AlterPartitionReassignments</c> is per-key void,
/// <c>ListPartitionReassignments</c> one aggregate future, and <c>ListOffsets</c> per key with a value and an error,
/// so the member sets are asserted.
/// </remarks>
public sealed class PublicAdminP4Stage2ShapeParityTests
{
    [Fact]
    public void IAdmin_TheTwoNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo listReassignments = typeof(IAdmin).GetMethod(
            nameof(IAdmin.ListPartitionReassignments),
            new[] { typeof(IReadOnlyCollection<TopicPartition>), typeof(ListPartitionReassignmentsOptions) })!;
        MethodInfo listOffsets = typeof(IAdmin).GetMethod(nameof(IAdmin.ListOffsets))!;

        Assert.Equal(typeof(ListPartitionReassignmentsResult), listReassignments.ReturnType);
        Assert.Equal(typeof(ListOffsetsResult), listOffsets.ReturnType);

        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<TopicPartition>), typeof(ListPartitionReassignmentsOptions) },
            listReassignments.GetParameters().Select(parameter => parameter.ParameterType));
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, OffsetSpec>), typeof(ListOffsetsOptions) },
            listOffsets.GetParameters().Select(parameter => parameter.ParameterType));

        // The selection is nullable and optional: null is Java's Optional.empty(), every ongoing reassignment, and Java's
        // zero-argument listPartitionReassignments() is () here.
        ParameterInfo selection = listReassignments.GetParameters()[0];
        Assert.True(selection.IsOptional, "the selection defaults, as Java's zero-arg overload");
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(selection));

        // ListOffsets' map is required and non-nullable, as in Java.
        ParameterInfo offsets = listOffsets.GetParameters()[0];
        Assert.False(offsets.IsOptional);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(offsets, 0));

        foreach (MethodInfo rpc in new[] { listReassignments, listOffsets })
        {
            ParameterInfo options = rpc.GetParameters()[1];
            Assert.True(options.IsOptional, $"{rpc.Name}'s options must be optional");
            Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(options));
        }

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void ListPartitionReassignmentsResult_PublishesExactlyOneAccessor()
    {
        MethodInfo reassignments = typeof(ListPartitionReassignmentsResult).GetMethod(
            nameof(ListPartitionReassignmentsResult.Reassignments), Type.EmptyTypes)!;

        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>),
            reassignments.ReturnType);

        // Nullability positions: 0 = the KafkaFuture (a struct, so oblivious), 1 = the dictionary, 2 = its value.
        Assert.Equal(NullableAnnotation.Oblivious, NullableAnnotation.Flag(reassignments.ReturnParameter, 0));
        for (int position = 1; position <= 2; position++)
        {
            Assert.Equal(
                NullableAnnotation.NotAnnotated,
                NullableAnnotation.Flag(reassignments.ReturnParameter, position));
        }

        Assert.Equal(
            new[] { nameof(ListPartitionReassignmentsResult.Reassignments) },
            DeclaredPublicMethodNames(typeof(ListPartitionReassignmentsResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(ListPartitionReassignmentsResult)));
    }

    [Fact]
    public void ListOffsetsResult_PublishesJavasTwoAccessors_AndItsConstructorIsInternal()
    {
        Assert.Empty(typeof(ListOffsetsResult).GetConstructors());
        ConstructorInfo only = Assert.Single(
            typeof(ListOffsetsResult).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance),
            candidate => candidate.IsAssembly);
        // KafkaFuture<Info>, not Info: Java's is Map<TopicPartition, KafkaFuture<Info>>, so the futures are the value.
        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>),
            },
            only.GetParameters().Select(parameter => parameter.ParameterType));
        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(() => new ListOffsetsResult(null!));
        Assert.Equal("futures", rejected.ParamName);

        MethodInfo partitionResult = typeof(ListOffsetsResult).GetMethod(
            nameof(ListOffsetsResult.PartitionResult))!;
        MethodInfo all = typeof(ListOffsetsResult).GetMethod(nameof(ListOffsetsResult.All), Type.EmptyTypes)!;

        Assert.Equal(
            typeof(KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>), partitionResult.ReturnType);
        Assert.Equal(new[] { typeof(TopicPartition) }, partitionResult.GetParameters().Select(p => p.ParameterType));

        // Java's all() is KafkaFuture<Map<...>>, not KafkaFuture<Void>.
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>>),
            all.ReturnType);
        Assert.Equal(typeof(KafkaFuture), typeof(DeleteRecordsResult).GetMethod(nameof(DeleteRecordsResult.All))!.ReturnType);

        Assert.Equal(
            new[] { nameof(ListOffsetsResult.All), nameof(ListOffsetsResult.PartitionResult) },
            DeclaredPublicMethodNames(typeof(ListOffsetsResult)));
    }

    [Fact]
    public void ListOffsetsResultInfo_IsNested_WithJavasThreeAccessors()
    {
        Type info = typeof(ListOffsetsResult.ListOffsetsResultInfo);

        Assert.Same(typeof(ListOffsetsResult), info.DeclaringType);
        Assert.True(info.IsNested, "Java declares it nested (ListOffsetsResult.java:70)");
        Assert.True(info.IsSealed);
        Assert.Equal("Confluent.Kafka.Admin", info.Namespace);

        ConstructorInfo only = Assert.Single(info.GetConstructors());
        Assert.Equal(
            new[] { typeof(long), typeof(long), typeof(int?) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(typeof(long), info.GetProperty(nameof(ListOffsetsResult.ListOffsetsResultInfo.Offset))!.PropertyType);
        Assert.Equal(typeof(long), info.GetProperty(nameof(ListOffsetsResult.ListOffsetsResultInfo.Timestamp))!.PropertyType);

        // int? is Java's Optional<Integer>, which a plain int cannot express.
        Assert.Equal(
            typeof(int?), info.GetProperty(nameof(ListOffsetsResult.ListOffsetsResultInfo.LeaderEpoch))!.PropertyType);

        Assert.Equal(
            new[] { "LeaderEpoch", "Offset", "Timestamp" }, DeclaredPublicPropertyNames(info));

        Assert.Empty(DeclaredPublicMethodNames(info));

        // ToString is overridden, as Java declares one; the helper filters object's members, so it is pinned here.
        Assert.Same(info, info.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        // A present epoch of -1 is present.
        ListOffsetsResult.ListOffsetsResultInfo negative =
            new ListOffsetsResult.ListOffsetsResultInfo(5, 7, -1);
        Assert.Equal(-1, negative.LeaderEpoch);
        Assert.Equal("ListOffsetsResultInfo(offset=5, timestamp=7, leaderEpoch=Optional[-1])", negative.ToString());

        ListOffsetsResult.ListOffsetsResultInfo absent =
            new ListOffsetsResult.ListOffsetsResultInfo(5, 7, null);
        Assert.Null(absent.LeaderEpoch);
        Assert.Equal("ListOffsetsResultInfo(offset=5, timestamp=7, leaderEpoch=Optional.empty)", absent.ToString());
    }

    [Fact]
    public void OffsetSpec_PublishesAllSevenJavaKinds()
    {
        Type[] nested = typeof(OffsetSpec)
            .GetNestedTypes(BindingFlags.Public)
            .OrderBy(type => type.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[]
            {
                nameof(OffsetSpec.EarliestLocalSpec),
                nameof(OffsetSpec.EarliestPendingUploadSpec),
                nameof(OffsetSpec.EarliestSpec),
                nameof(OffsetSpec.LatestSpec),
                nameof(OffsetSpec.LatestTieredSpec),
                nameof(OffsetSpec.MaxTimestampSpec),
                nameof(OffsetSpec.TimestampSpec),
            },
            nested.Select(type => type.Name));

        Assert.All(nested, type => Assert.Same(typeof(OffsetSpec), type.BaseType));
        Assert.All(nested, type => Assert.True(type.IsSealed, $"{type.Name} must be sealed"));

        Assert.IsType<OffsetSpec.LatestSpec>(OffsetSpec.Latest());
        Assert.IsType<OffsetSpec.EarliestSpec>(OffsetSpec.Earliest());
        Assert.IsType<OffsetSpec.MaxTimestampSpec>(OffsetSpec.MaxTimestamp());
        Assert.IsType<OffsetSpec.EarliestLocalSpec>(OffsetSpec.EarliestLocal());
        Assert.IsType<OffsetSpec.LatestTieredSpec>(OffsetSpec.LatestTiered());
        Assert.IsType<OffsetSpec.EarliestPendingUploadSpec>(OffsetSpec.EarliestPendingUpload());
        Assert.IsType<OffsetSpec.TimestampSpec>(OffsetSpec.ForTimestamp(42));

        Assert.Equal(
            new[]
            {
                "Earliest", "EarliestLocal", "EarliestPendingUpload", "ForTimestamp", "Latest",
                "LatestTiered", "MaxTimestamp",
            },
            typeof(OffsetSpec)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Where(method => !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java's TimestampSpec.timestamp() is package-private, so the kinds carry no public instance surface.
        Assert.All(nested, type => Assert.Empty(DeclaredPublicMethodNames(type)));
        Assert.All(nested, type => Assert.Empty(DeclaredPublicPropertyNames(type)));

        // The recorded deviation: the hierarchy is closed, so an external subclass, which Java's encoder would query as
        // latest(), is not expressible.
        Assert.Empty(
            typeof(OffsetSpec).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
    }

    [Theory]
    [InlineData(IsolationLevel.ReadUncommitted, 0)]
    [InlineData(IsolationLevel.ReadCommitted, 1)]
    public void IsolationLevel_CarriesJavasIds(IsolationLevel level, int id)
    {
        Assert.Equal(id, (int)level);
        Assert.Equal(level, (IsolationLevel)id);
    }

    [Fact]
    public void IsolationLevel_HasExactlyJavasTwoMembers()
    {
        Assert.Equal(
            new[] { "ReadCommitted", "ReadUncommitted" },
            Enum.GetNames(typeof(IsolationLevel)).OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal("Confluent.Kafka", typeof(IsolationLevel).Namespace);
    }

    [Fact]
    public void PartitionReassignment_MirrorsJavasShape()
    {
        ConstructorInfo only = Assert.Single(typeof(PartitionReassignment).GetConstructors());
        Assert.Equal(
            new[] { typeof(IReadOnlyList<int>), typeof(IReadOnlyList<int>), typeof(IReadOnlyList<int>) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[] { "AddingReplicas", "RemovingReplicas", "Replicas" },
            DeclaredPublicPropertyNames(typeof(PartitionReassignment)));
        Assert.Empty(DeclaredPublicMethodNames(typeof(PartitionReassignment)));

        // Java declares a toString; pinned directly, since the helper filters object's members.
        Assert.Same(
            typeof(PartitionReassignment),
            typeof(PartitionReassignment).GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        List<int> replicas = new List<int> { 1, 2, 3 };
        PartitionReassignment reassignment =
            new PartitionReassignment(replicas, new[] { 3 }, Array.Empty<int>());
        Assert.Equal(new[] { 1, 2, 3 }, reassignment.Replicas);
        Assert.Equal(new[] { 3 }, reassignment.AddingReplicas);
        Assert.Empty(reassignment.RemovingReplicas);

        replicas.Add(9);
        Assert.Equal(new[] { 1, 2, 3 }, reassignment.Replicas);

        Assert.Equal(
            "PartitionReassignment(replicas=[1, 2, 3], addingReplicas=[3], removingReplicas=[])",
            reassignment.ToString());

        foreach (string parameter in new[] { "replicas", "addingReplicas", "removingReplicas" })
        {
            Assert.Equal(
                parameter,
                Assert.Throws<ArgumentNullException>(() => Build(parameter)).ParamName);
        }

        Assert.NotEqual(typeof(NewPartitionReassignment), typeof(PartitionReassignment));
    }

    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        ListPartitionReassignmentsOptions reassignments = new ListPartitionReassignmentsOptions();
        Assert.Null(reassignments.TimeoutMs);
        Assert.Equal(
            new[] { nameof(ListPartitionReassignmentsOptions.TimeoutMs) },
            PropertyNames(typeof(ListPartitionReassignmentsOptions)));

        // ListOffsetsOptions defaults to READ_UNCOMMITTED, which is Java's default.
        ListOffsetsOptions offsets = new ListOffsetsOptions();
        Assert.Null(offsets.TimeoutMs);
        Assert.Equal(IsolationLevel.ReadUncommitted, offsets.IsolationLevel);
        Assert.Equal(
            new[] { nameof(ListOffsetsOptions.IsolationLevel), nameof(ListOffsetsOptions.TimeoutMs) },
            PropertyNames(typeof(ListOffsetsOptions)));

        foreach (Type type in new[] { typeof(ListPartitionReassignmentsOptions), typeof(ListOffsetsOptions) })
        {
            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);
        }
    }

    private static PartitionReassignment Build(string nullParameter) => new PartitionReassignment(
        nullParameter == "replicas" ? null! : Array.Empty<int>(),
        nullParameter == "addingReplicas" ? null! : Array.Empty<int>(),
        nullParameter == "removingReplicas" ? null! : Array.Empty<int>());

    private static string[] DeclaredPublicMethodNames(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method =>
                !method.IsSpecialName
                && !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
                && method.GetBaseDefinition().DeclaringType != typeof(object))
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    private static string[] DeclaredPublicPropertyNames(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(property => !property.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            .Select(property => property.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    private static string[] PropertyNames(Type type) =>
        type.GetProperties().Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
}
