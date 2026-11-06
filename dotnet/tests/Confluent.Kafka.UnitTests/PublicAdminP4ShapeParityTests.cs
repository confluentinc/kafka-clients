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

using Confluent.Kafka.Common;
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

        Assert.Equal(typeof(KafkaFuture<IReadOnlyDictionary<TopicPartition, Exception>>), partitions.ReturnType);
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
    [InlineData(ElectionType.PREFERRED, 0)]
    [InlineData(ElectionType.UNCLEAN, 1)]
    public void ElectionType_CarriesJavasValues(ElectionType type, int value)
    {
        Assert.Equal(value, (int)type);
        Assert.Equal(type, (ElectionType)value);
    }

    [Fact]
    public void ElectionType_HasExactlyJavasTwoMembers()
    {
        Assert.Equal(
            new[] { "PREFERRED", "UNCLEAN" },
            Enum.GetNames(typeof(ElectionType)).OrderBy(name => name, StringComparer.Ordinal));
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
            DeclaredPublicMethodNames(typeof(NewPartitionReassignment)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(NewPartitionReassignment)));
        Assert.Equal(
            typeof(IReadOnlyList<int>),
            typeof(NewPartitionReassignment).GetMethod(
                nameof(NewPartitionReassignment.TargetReplicas), Type.EmptyTypes)!.ReturnType);

        List<int> replicas = new List<int> { 3, 1, 2 };
        NewPartitionReassignment reassignment = new NewPartitionReassignment(replicas);
        Assert.Equal(new[] { 3, 1, 2 }, reassignment.TargetReplicas());

        replicas.Add(9);
        Assert.Equal(new[] { 3, 1, 2 }, reassignment.TargetReplicas());

        IllegalArgumentException empty =
            Assert.Throws<IllegalArgumentException>(() => new NewPartitionReassignment(Array.Empty<int>()));
        Assert.Equal("Cannot create a new partition reassignment without any replicas", empty.Message);

        IllegalArgumentException missing =
            Assert.Throws<IllegalArgumentException>(() => new NewPartitionReassignment(null!));
        Assert.Equal("Cannot create a new partition reassignment without any replicas", missing.Message);
    }

    [Fact]
    public void G3_6_TargetReplicas_IsAReadOnlyCopy()
    {
        List<int> source = new List<int> { 4, 5 };
        NewPartitionReassignment reassignment = new NewPartitionReassignment(source);
        IReadOnlyList<int> replicas = reassignment.TargetReplicas();

        Assert.False(replicas is List<int>);
        ICollection<int> asCollection = Assert.IsAssignableFrom<ICollection<int>>(replicas);
        Assert.True(asCollection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => asCollection.Clear());
        Assert.Equal(new[] { 4, 5 }, reassignment.TargetReplicas());

        source.Clear();
        source.Add(9);
        Assert.Equal(new[] { 4, 5 }, reassignment.TargetReplicas());
    }

    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        // Java's ElectLeadersOptions declares no members of its own, so the inherited timeout is the whole surface.
        ElectLeadersOptions elect = new ElectLeadersOptions();
        Assert.Null(elect.TimeoutMs());
        SetterMethods.AssertGettersAndSetters(typeof(ElectLeadersOptions), nameof(ElectLeadersOptions.TimeoutMs));

        // allowReplicationFactorChange defaults to true, not the C# default for its type.
        AlterPartitionReassignmentsOptions reassign = new AlterPartitionReassignmentsOptions();
        Assert.Null(reassign.TimeoutMs());
        Assert.True(reassign.AllowReplicationFactorChange());
        SetterMethods.AssertGettersAndSetters(
            typeof(AlterPartitionReassignmentsOptions),
            nameof(AlterPartitionReassignmentsOptions.AllowReplicationFactorChange),
            nameof(AlterPartitionReassignmentsOptions.TimeoutMs));

        foreach (Type type in new[] { typeof(ElectLeadersOptions), typeof(AlterPartitionReassignmentsOptions) })
        {
            Assert.Equal(typeof(int?), Getters.Get(type, "TimeoutMs").ReturnType);
            Assert.Equal(typeof(int?), SetterMethods.ParameterTypeOf(type, "TimeoutMs"));
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
}
