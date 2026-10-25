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
/// The public shape of <c>ElectLeaders</c> and <c>AlterPartitionReassignments</c>, their options, results and value
/// types, against the Java classes they mirror.
/// </summary>
/// <remarks>
/// These are reflection assertions. The two results are the pair most easily confused:
/// <see cref="ElectLeadersResult"/> has one future over a map whose values are the per-partition outcomes,
/// <see cref="AlterPartitionReassignmentsResult"/> one future per partition, and neither carries the other's member.
/// </remarks>
public sealed class PublicAdminP4ShapeParityTests
{
    [Fact]
    public void IAdmin_TheTwoNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo elect = typeof(IAdmin).GetMethod(nameof(IAdmin.ElectLeaders))!;
        MethodInfo reassign = typeof(IAdmin).GetMethod(nameof(IAdmin.AlterPartitionReassignments))!;

        Assert.Equal(typeof(ElectLeadersResult), elect.ReturnType);
        Assert.Equal(typeof(AlterPartitionReassignmentsResult), reassign.ReturnType);

        Assert.Equal(
            new[]
            {
                typeof(ElectionType),
                typeof(IReadOnlyCollection<TopicPartition>),
                typeof(ElectLeadersOptions),
            },
            elect.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<TopicPartition, NewPartitionReassignment>),
                typeof(AlterPartitionReassignmentsOptions),
            },
            reassign.GetParameters().Select(parameter => parameter.ParameterType));

        // The partition selection is required, as Java has no electLeaders overload without it; only the options collapse
        // Java's convenience overload.
        ParameterInfo partitions = elect.GetParameters()[1];
        Assert.False(partitions.IsOptional, "the partition selection mirrors Java's required Set parameter");
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(partitions));

        Assert.True(elect.GetParameters()[2].IsOptional);
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(elect.GetParameters()[2]));
        Assert.True(reassign.GetParameters()[1].IsOptional);
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(reassign.GetParameters()[1]));

        // The reassignment value is nullable: null is Java's Optional.empty(), "revert this partition". Position 1 of the
        // flattened parameter type: position 0 is the dictionary, and TopicPartition, a value type, takes no position.
        Assert.Equal(
            NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(reassign.GetParameters()[0], 0));
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(reassign.GetParameters()[0], 1));

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void ElectLeadersResult_PublishesOneFutureOverAMapOfNullableErrors()
    {
        MethodInfo partitions = typeof(ElectLeadersResult).GetMethod(
            nameof(ElectLeadersResult.Partitions), Type.EmptyTypes)!;
        MethodInfo all = typeof(ElectLeadersResult).GetMethod(nameof(ElectLeadersResult.All), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture<IReadOnlyDictionary<TopicPartition, KafkaException>>), partitions.ReturnType);
        Assert.Equal(typeof(KafkaFuture), all.ReturnType);

        // Nullability positions: 0 = the KafkaFuture (a struct, so oblivious), 1 = the dictionary, 2 = the map's value.
        Assert.Equal(NullableAnnotation.Oblivious, NullableAnnotation.Flag(partitions.ReturnParameter, 0));
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(partitions.ReturnParameter, 1));
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(partitions.ReturnParameter, 2));

        Assert.Equal(
            new[] { nameof(ElectLeadersResult.All), nameof(ElectLeadersResult.Partitions) },
            DeclaredPublicMethodNames(typeof(ElectLeadersResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(ElectLeadersResult)));
    }

    [Fact]
    public void AlterPartitionReassignmentsResult_PublishesOneFuturePerPartition()
    {
        PropertyInfo values = typeof(AlterPartitionReassignmentsResult).GetProperty(
            nameof(AlterPartitionReassignmentsResult.Values))!;
        MethodInfo all = typeof(AlterPartitionReassignmentsResult).GetMethod(
            nameof(AlterPartitionReassignmentsResult.All), Type.EmptyTypes)!;

        Assert.Equal(typeof(IReadOnlyDictionary<TopicPartition, KafkaFuture>), values.PropertyType);
        Assert.Equal(typeof(KafkaFuture), all.ReturnType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(values));
        Assert.Null(values.SetMethod);

        Assert.Equal(
            new[] { nameof(AlterPartitionReassignmentsResult.All) },
            DeclaredPublicMethodNames(typeof(AlterPartitionReassignmentsResult)));
        Assert.Equal(
            new[] { nameof(AlterPartitionReassignmentsResult.Values) },
            DeclaredPublicPropertyNames(typeof(AlterPartitionReassignmentsResult)));
    }

    [Fact]
    public void TheAccessorSetTwins_PublishDisjointFutureShapes()
    {
        string[] elect = DeclaredPublicMethodNames(typeof(ElectLeadersResult))
            .Concat(DeclaredPublicPropertyNames(typeof(ElectLeadersResult)))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
        string[] reassign = DeclaredPublicMethodNames(typeof(AlterPartitionReassignmentsResult))
            .Concat(DeclaredPublicPropertyNames(typeof(AlterPartitionReassignmentsResult)))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(new[] { "All", "Partitions" }, elect);
        Assert.Equal(new[] { "All", "Values" }, reassign);

        // All is the one shared member and differs in meaning: the first present error of a map, or Java's allOf over the
        // futures.
        Assert.Equal(new[] { "All" }, elect.Intersect(reassign, StringComparer.Ordinal));
    }

    [Theory]
    [InlineData(ElectionType.Preferred, 0)]
    [InlineData(ElectionType.Unclean, 1)]
    public void ElectionType_CarriesJavasValues(ElectionType type, int value)
    {
        Assert.Equal(value, (int)type);
        Assert.Equal(type, (ElectionType)value);
    }

    [Fact]
    public void ElectionType_HasExactlyJavasTwoMembers()
    {
        Assert.Equal(
            new[] { "Preferred", "Unclean" },
            Enum.GetNames(typeof(ElectionType)).OrderBy(name => name, StringComparer.Ordinal));

        // Java's package is org.apache.kafka.common, so the root namespace, not Confluent.Kafka.Admin.
        Assert.Equal("Confluent.Kafka", typeof(ElectionType).Namespace);
    }

    [Fact]
    public void NewPartitionReassignment_MirrorsJavasShape_AndRejectsAnEmptyList()
    {
        ConstructorInfo only = Assert.Single(typeof(NewPartitionReassignment).GetConstructors());
        Assert.Equal(
            new[] { typeof(IReadOnlyList<int>) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[] { nameof(NewPartitionReassignment.TargetReplicas) },
            DeclaredPublicPropertyNames(typeof(NewPartitionReassignment)));
        Assert.Empty(DeclaredPublicMethodNames(typeof(NewPartitionReassignment)));
        Assert.Equal(
            typeof(IReadOnlyList<int>),
            typeof(NewPartitionReassignment).GetProperty(
                nameof(NewPartitionReassignment.TargetReplicas))!.PropertyType);

        Assert.Equal("Confluent.Kafka.Admin", typeof(NewPartitionReassignment).Namespace);

        List<int> replicas = new List<int> { 3, 1, 2 };
        NewPartitionReassignment reassignment = new NewPartitionReassignment(replicas);
        Assert.Equal(new[] { 3, 1, 2 }, reassignment.TargetReplicas);

        replicas.Add(9);
        Assert.Equal(new[] { 3, 1, 2 }, reassignment.TargetReplicas);

        // Java throws IllegalArgumentException for both; the .NET family splits null out.
        ArgumentException empty =
            Assert.Throws<ArgumentException>(() => new NewPartitionReassignment(Array.Empty<int>()));
        Assert.Equal("targetReplicas", empty.ParamName);
        Assert.Contains("Cannot create a new partition reassignment without any replicas", empty.Message);

        ArgumentNullException missing =
            Assert.Throws<ArgumentNullException>(() => new NewPartitionReassignment(null!));
        Assert.Equal("targetReplicas", missing.ParamName);
        Assert.Contains("Cannot create a new partition reassignment without any replicas", missing.Message);

        Assert.IsAssignableFrom<ArgumentException>(missing);
    }

    [Fact]
    public void G3_6_TargetReplicas_IsAReadOnlyCopy()
    {
        List<int> source = new List<int> { 4, 5 };
        NewPartitionReassignment reassignment = new NewPartitionReassignment(source);
        IReadOnlyList<int> replicas = reassignment.TargetReplicas;

        Assert.False(replicas is List<int>);
        ICollection<int> asCollection = Assert.IsAssignableFrom<ICollection<int>>(replicas);
        Assert.True(asCollection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => asCollection.Clear());
        Assert.Equal(new[] { 4, 5 }, reassignment.TargetReplicas);

        source.Clear();
        source.Add(9);
        Assert.Equal(new[] { 4, 5 }, reassignment.TargetReplicas);
    }

    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        // Java's ElectLeadersOptions declares no members of its own, so the inherited timeout is the whole surface.
        ElectLeadersOptions elect = new ElectLeadersOptions();
        Assert.Null(elect.TimeoutMs);
        Assert.Equal(new[] { nameof(ElectLeadersOptions.TimeoutMs) }, PropertyNames(typeof(ElectLeadersOptions)));

        // allowReplicationFactorChange defaults to true, not the C# default for its type.
        AlterPartitionReassignmentsOptions reassign = new AlterPartitionReassignmentsOptions();
        Assert.Null(reassign.TimeoutMs);
        Assert.True(reassign.AllowReplicationFactorChange);
        Assert.Equal(
            new[]
            {
                nameof(AlterPartitionReassignmentsOptions.AllowReplicationFactorChange),
                nameof(AlterPartitionReassignmentsOptions.TimeoutMs),
            },
            PropertyNames(typeof(AlterPartitionReassignmentsOptions)));

        foreach (Type type in new[] { typeof(ElectLeadersOptions), typeof(AlterPartitionReassignmentsOptions) })
        {
            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);
        }
    }

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
