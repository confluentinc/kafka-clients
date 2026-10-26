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
/// The declared public signatures of <c>DeleteTopics</c> and <c>DescribeTopics</c>, their options, results and value
/// types, against the Java classes they mirror, alongside <see cref="PublicAdminShapeParityTests"/>.
/// </summary>
/// <remarks>
/// These are reflection assertions: a widened signature still compiles and passes every behavioural test. Two of the
/// facts are absences: <see cref="DescribeTopicsResult"/> has no <c>All()</c> and <see cref="DeleteTopicsResult"/> has
/// no typed aggregate.
/// </remarks>
public sealed class PublicAdminP2aShapeParityTests
{
    [Fact]
    public void DeleteTopicsResult_ValuesAreVoidFutures_AndNullable()
    {
        AssertNullableDictionary<Uuid, KafkaFuture>(typeof(DeleteTopicsResult), nameof(DeleteTopicsResult.TopicIdValues));
        AssertNullableDictionary<string, KafkaFuture>(typeof(DeleteTopicsResult), nameof(DeleteTopicsResult.TopicNameValues));
    }

    [Fact]
    public void DescribeTopicsResult_ValuesCarryTheDescription_AndAreNullable()
    {
        AssertNullableDictionary<Uuid, KafkaFuture<TopicDescription>>(
            typeof(DescribeTopicsResult), nameof(DescribeTopicsResult.TopicIdValues));
        AssertNullableDictionary<string, KafkaFuture<TopicDescription>>(
            typeof(DescribeTopicsResult), nameof(DescribeTopicsResult.TopicNameValues));
    }

    [Fact]
    public void TheAggregateAsymmetryIsJavas_AndIsPinnedInBothDirections()
    {
        Assert.NotNull(typeof(DeleteTopicsResult).GetMethod("All", Type.EmptyTypes));
        Assert.Equal(typeof(KafkaFuture), typeof(DeleteTopicsResult).GetMethod("All", Type.EmptyTypes)!.ReturnType);
        Assert.DoesNotContain(
            typeof(DeleteTopicsResult).GetMethods(BindingFlags.Public | BindingFlags.Instance),
            method => method.Name is "AllTopicNames" or "AllTopicIds");

        Assert.Null(typeof(DescribeTopicsResult).GetMethod("All", Type.EmptyTypes));
        Assert.DoesNotContain(
            typeof(DescribeTopicsResult).GetMethods(BindingFlags.Public | BindingFlags.Instance),
            method => method.Name == "All");

        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<string, TopicDescription>>?),
            typeof(DescribeTopicsResult).GetMethod("AllTopicNames", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<Uuid, TopicDescription>>?),
            typeof(DescribeTopicsResult).GetMethod("AllTopicIds", Type.EmptyTypes)!.ReturnType);
    }

    [Fact]
    public void DescribeTopicsResult_TypedAggregatesAreNullable()
    {
        AssertNullableFutureReturn<IReadOnlyDictionary<string, TopicDescription>>(
            typeof(DescribeTopicsResult).GetMethod("AllTopicNames", Type.EmptyTypes)!);
        AssertNullableFutureReturn<IReadOnlyDictionary<Uuid, TopicDescription>>(
            typeof(DescribeTopicsResult).GetMethod("AllTopicIds", Type.EmptyTypes)!);
    }

    [Fact]
    public void AbsentVersusEmpty_IsVisibleInTheSignatures()
    {
        AssertNullableProperty(typeof(TopicDescription), nameof(TopicDescription.AuthorizedOperations));
        AssertNullableProperty(typeof(TopicPartitionInfo), nameof(TopicPartitionInfo.Elr));
        AssertNullableProperty(typeof(TopicPartitionInfo), nameof(TopicPartitionInfo.LastKnownElr));
        AssertNullableProperty(typeof(TopicPartitionInfo), nameof(TopicPartitionInfo.Leader));

        AssertNonNullableProperty(typeof(TopicPartitionInfo), nameof(TopicPartitionInfo.Replicas));
        AssertNonNullableProperty(typeof(TopicPartitionInfo), nameof(TopicPartitionInfo.InSyncReplicas));
        AssertNonNullableProperty(typeof(TopicDescription), nameof(TopicDescription.Partitions));

        // Java's partitions() is a List and authorizedOperations() a Set; IReadOnlySet post-dates netstandard2.0, so the set
        // becomes IReadOnlyCollection, but the list stays a list.
        Assert.Equal(
            typeof(IReadOnlyList<TopicPartitionInfo>),
            typeof(TopicDescription).GetProperty(nameof(TopicDescription.Partitions))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyCollection<AclOperation>),
            typeof(TopicDescription).GetProperty(nameof(TopicDescription.AuthorizedOperations))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyList<Node>),
            typeof(TopicPartitionInfo).GetProperty(nameof(TopicPartitionInfo.Replicas))!.PropertyType);
    }

    [Fact]
    public void IAdmin_RpcsAreSynchronousAndTakeATopicCollection()
    {
        MethodInfo delete = typeof(IAdmin).GetMethod(
            nameof(IAdmin.DeleteTopics), new[] { typeof(TopicCollection), typeof(DeleteTopicsOptions) })!;
        MethodInfo describe = typeof(IAdmin).GetMethod(
            nameof(IAdmin.DescribeTopics), new[] { typeof(TopicCollection), typeof(DescribeTopicsOptions) })!;

        Assert.Equal(typeof(DeleteTopicsResult), delete.ReturnType);
        Assert.Equal(typeof(DescribeTopicsResult), describe.ReturnType);

        Assert.Equal(
            new[] { typeof(TopicCollection), typeof(DeleteTopicsOptions) },
            delete.GetParameters().Select(parameter => parameter.ParameterType));
        Assert.Equal(
            new[] { typeof(TopicCollection), typeof(DescribeTopicsOptions) },
            describe.GetParameters().Select(parameter => parameter.ParameterType));

        // `options` is optional, collapsing Java's one-argument `default` overload.
        Assert.True(delete.GetParameters()[1].IsOptional);
        Assert.True(describe.GetParameters()[1].IsOptional);

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void DescribeTopicsOptions_MatchJavasDefaults()
    {
        DescribeTopicsOptions options = new DescribeTopicsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.False(options.IncludeAuthorizedOperations);
        Assert.Equal(2000, options.PartitionSizeLimitPerResponse);

        Assert.Equal(
            typeof(int),
            typeof(DescribeTopicsOptions)
                .GetProperty(nameof(DescribeTopicsOptions.PartitionSizeLimitPerResponse))!.PropertyType);

        DeleteTopicsOptions deleteOptions = new DeleteTopicsOptions();
        Assert.Null(deleteOptions.TimeoutMs);

        // The one option whose Java default is not the C# default for its type.
        Assert.True(deleteOptions.RetryOnQuotaViolation);
    }

    [Fact]
    public void ValueTypes_PublishExactlyJavasConstructors()
    {
        Assert.Equal(3, typeof(TopicDescription).GetConstructors().Length);
        Assert.Equal(2, typeof(TopicPartitionInfo).GetConstructors().Length);

        // Java's 3-arg TopicDescription passes an empty set: reported but empty, not null.
        TopicDescription bare = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());
        Assert.NotNull(bare.AuthorizedOperations);
        Assert.Empty(bare.AuthorizedOperations!);
        Assert.Equal(Uuid.Zero, bare.TopicId);

        // Java renders the boolean in lowercase, where bool.ToString() gives "True" and "False".
        Assert.StartsWith("(name=t, internal=false, ", bare.ToString(), StringComparison.Ordinal);
        Assert.StartsWith(
            "(name=t, internal=true, ",
            new TopicDescription("t", true, Array.Empty<TopicPartitionInfo>()).ToString(),
            StringComparison.Ordinal);

        // Java's 4-arg TopicPartitionInfo leaves elr and lastKnownElr null.
        TopicPartitionInfo four = new TopicPartitionInfo(
            0, null, Array.Empty<Node>(), Array.Empty<Node>());
        Assert.Null(four.Elr);
        Assert.Null(four.LastKnownElr);
    }

    [Fact]
    public void TopicPartitionInfo_SixArgumentForm_RefusesANullElrSet_InJavasArgumentOrder()
    {
        Node[] none = Array.Empty<Node>();

        Assert.Equal("elr", Assert.Throws<ArgumentNullException>(
            () => new TopicPartitionInfo(0, null, none, none, null!, none)).ParamName);
        Assert.Equal("lastKnownElr", Assert.Throws<ArgumentNullException>(
            () => new TopicPartitionInfo(0, null, none, none, none, null!)).ParamName);
        Assert.Equal("replicas", Assert.Throws<ArgumentNullException>(
            () => new TopicPartitionInfo(0, null, null!, none, null!, null!)).ParamName);
        Assert.Equal("inSyncReplicas", Assert.Throws<ArgumentNullException>(
            () => new TopicPartitionInfo(0, null, none, null!, null!, null!)).ParamName);

        TopicPartitionInfo six = new TopicPartitionInfo(0, null, none, none, none, none);
        Assert.NotNull(six.Elr);
        Assert.Empty(six.Elr!);
        Assert.NotNull(six.LastKnownElr);
        Assert.Empty(six.LastKnownElr!);
    }

    private static void AssertNullableDictionary<TKey, TValue>(Type declaring, string propertyName)
    {
        PropertyInfo property = declaring.GetProperty(propertyName)!;

        Assert.Equal(typeof(IReadOnlyDictionary<TKey, TValue>), property.PropertyType);
        AssertNullableProperty(declaring, propertyName);
    }

    private static void AssertNullableProperty(Type declaring, string propertyName) =>
        Assert.Equal(2, NullableFlag(declaring.GetProperty(propertyName)!));

    private static void AssertNonNullableProperty(Type declaring, string propertyName) =>
        Assert.Equal(1, NullableFlag(declaring.GetProperty(propertyName)!));

    private static void AssertNullableFutureReturn<TValue>(MethodInfo method)
    {
        Assert.Equal(typeof(KafkaFuture<TValue>), Nullable.GetUnderlyingType(method.ReturnType));
        Assert.Equal(1, NullableAnnotation.Flag(method.ReturnParameter, 1));
    }

    private static byte NullableFlag(MemberInfo member) => NullableAnnotation.Flag(member);

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}
