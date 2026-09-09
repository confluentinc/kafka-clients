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
/// Pins the <b>declared</b> public signatures M15/P2a adds, against the Java classes they
/// mirror — the P2a half of <see cref="PublicAdminShapeParityTests"/>.
/// </summary>
/// <remarks>
/// <para>
/// These are reflection assertions for the reason P1 learned the hard way: C# upcasts and
/// widens silently, so a widened signature still compiles and still passes every
/// behavioural test. Three shape defects survived P1's green build, 883 green tests
/// <b>and</b> a 0-High first review; only assertions like these can turn red.
/// </para>
/// <para>
/// Two of the facts pinned here are <b>absences</b>, which nothing else can protect:
/// <see cref="DescribeTopicsResult"/> has no <c>All()</c> and
/// <see cref="DeleteTopicsResult"/> has no typed aggregate. An absence nobody asserts is
/// one a later phase "completes" with nothing going red.
/// </para>
/// </remarks>
public sealed class PublicAdminP2aShapeParityTests
{
    /// <summary>
    /// Java publishes <c>Map&lt;Uuid, KafkaFuture&lt;Void&gt;&gt; topicIdValues()</c> and
    /// <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt; topicNameValues()</c>
    /// (<c>DeleteTopicsResult.java:56,:65</c>) — <b>Void</b>, so the .NET side is the
    /// non-generic <see cref="Task"/>. The bridge resolves these with a <c>bool</c>
    /// success token internally (result shape 2); leaking that into the signature would
    /// publish a value Java does not have.
    /// </summary>
    [Fact]
    public void DeleteTopicsResult_ValuesAreVoidFutures_AndNullable()
    {
        AssertNullableDictionary<Uuid, Task>(typeof(DeleteTopicsResult), nameof(DeleteTopicsResult.TopicIdValues));
        AssertNullableDictionary<string, Task>(typeof(DeleteTopicsResult), nameof(DeleteTopicsResult.TopicNameValues));
    }

    /// <summary>
    /// Java publishes <c>Map&lt;K, KafkaFuture&lt;TopicDescription&gt;&gt;</c>
    /// (<c>DescribeTopicsResult.java:60,:70</c>) — unlike <c>deleteTopics</c>, these carry
    /// a value.
    /// </summary>
    [Fact]
    public void DescribeTopicsResult_ValuesCarryTheDescription_AndAreNullable()
    {
        AssertNullableDictionary<Uuid, Task<TopicDescription>>(
            typeof(DescribeTopicsResult), nameof(DescribeTopicsResult.TopicIdValues));
        AssertNullableDictionary<string, Task<TopicDescription>>(
            typeof(DescribeTopicsResult), nameof(DescribeTopicsResult.TopicNameValues));
    }

    /// <summary>
    /// ⚠ <b>Java's two topic results are deliberately asymmetric</b>, and mirroring the
    /// asymmetry — rather than smoothing it — is decision D9.
    /// <c>DeleteTopicsResult.all()</c> exists (<c>:72</c>) and there is no typed
    /// aggregate; <c>DescribeTopicsResult</c> has <c>allTopicNames()</c>/
    /// <c>allTopicIds()</c> (<c>:80,:90</c>) and <b>no</b> <c>all()</c>.
    /// </summary>
    [Fact]
    public void TheAggregateAsymmetryIsJavas_AndIsPinnedInBothDirections()
    {
        // deleteTopics HAS all(), and has no typed aggregate.
        Assert.NotNull(typeof(DeleteTopicsResult).GetMethod("All", Type.EmptyTypes));
        Assert.Equal(typeof(Task), typeof(DeleteTopicsResult).GetMethod("All", Type.EmptyTypes)!.ReturnType);
        Assert.DoesNotContain(
            typeof(DeleteTopicsResult).GetMethods(BindingFlags.Public | BindingFlags.Instance),
            method => method.Name is "AllTopicNames" or "AllTopicIds");

        // describeTopics has NO all(), and has the two typed aggregates.
        Assert.Null(typeof(DescribeTopicsResult).GetMethod("All", Type.EmptyTypes));
        Assert.DoesNotContain(
            typeof(DescribeTopicsResult).GetMethods(BindingFlags.Public | BindingFlags.Instance),
            method => method.Name == "All");

        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<string, TopicDescription>>),
            typeof(DescribeTopicsResult).GetMethod("AllTopicNames", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<Uuid, TopicDescription>>),
            typeof(DescribeTopicsResult).GetMethod("AllTopicIds", Type.EmptyTypes)!.ReturnType);
    }

    /// <summary>
    /// Both typed aggregates are <b>nullable</b>: Java's private helper opens with
    /// <c>if (futures == null) return null;</c> (<c>DescribeTopicsResult.java:98</c>), so
    /// the aggregate whose key type does not match the request is <c>null</c> — exactly as
    /// the matching <c>*Values</c> map is. The phase plan's sketch omitted the
    /// <c>?</c>; the Java source is the contract, so the annotation is here.
    /// </summary>
    [Fact]
    public void DescribeTopicsResult_TypedAggregatesAreNullable()
    {
        AssertNullableReturn(typeof(DescribeTopicsResult).GetMethod("AllTopicNames", Type.EmptyTypes)!);
        AssertNullableReturn(typeof(DescribeTopicsResult).GetMethod("AllTopicIds", Type.EmptyTypes)!);
    }

    /// <summary>
    /// <see cref="TopicDescription.AuthorizedOperations"/> is nullable and
    /// <see cref="TopicPartitionInfo.Elr"/> / <see cref="TopicPartitionInfo.LastKnownElr"/>
    /// are nullable, while <see cref="TopicPartitionInfo.Replicas"/> /
    /// <see cref="TopicPartitionInfo.InSyncReplicas"/> are <b>not</b>. That asymmetry is
    /// the absent-versus-empty discriminant the ABI carries explicitly (<c>has_*</c>
    /// beside each count) and Java expresses as <c>null</c>; widening the two
    /// non-nullable ones, or narrowing any of the three nullable ones, would erase it.
    /// </summary>
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

        // Java's `partitions()` is a List (ordered, index == partition id) while
        // `authorizedOperations()` is a Set; IReadOnlySet post-dates netstandard2.0, so the
        // set becomes IReadOnlyCollection — but the list must NOT be flattened the same way.
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

    /// <summary>
    /// The two RPCs take a <see cref="TopicCollection"/> and return their <c>*Result</c>
    /// <b>synchronously</b> — Java's <c>Admin</c> methods do not block, so the
    /// <see cref="Task"/> mapping belongs on the futures inside the result, never on the
    /// method (<c>admin-client.md</c> §1). <c>Close</c> stays the one
    /// <see cref="Task"/>-returning member.
    /// </summary>
    [Fact]
    public void IAdmin_RpcsAreSynchronousAndTakeATopicCollection()
    {
        MethodInfo delete = typeof(IAdmin).GetMethod(nameof(IAdmin.DeleteTopics))!;
        MethodInfo describe = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeTopics))!;

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

        // Close remains the only Task-returning member on IAdmin.
        Assert.Equal(
            new[] { nameof(IAdmin.Close) },
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// <see cref="DescribeTopicsOptions.PartitionSizeLimitPerResponse"/> is a plain
    /// <c>int</c> defaulting to Java's <b>2000</b>
    /// (<c>DescribeTopicsOptions.java:28</c>), not a nullable one: Java's field is an
    /// <c>int</c> with a real default, unlike <c>timeoutMs</c>, whose <c>Integer</c> null
    /// genuinely means "unset".
    /// </summary>
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

    /// <summary>
    /// <see cref="TopicDescription"/> and <see cref="TopicPartitionInfo"/> publish exactly
    /// the constructors Java does — three and two — and no more. Publishing a constructor
    /// Java lacks was P1's Finding 3.
    /// </summary>
    [Fact]
    public void ValueTypes_PublishExactlyJavasConstructors()
    {
        Assert.Equal(3, typeof(TopicDescription).GetConstructors().Length);
        Assert.Equal(2, typeof(TopicPartitionInfo).GetConstructors().Length);

        // Java's 3-arg TopicDescription passes Collections.emptySet(), i.e. a
        // reported-but-EMPTY set — not null. Behavioural, because it is exactly the
        // distinction the nullable annotation above exists for.
        TopicDescription bare = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());
        Assert.NotNull(bare.AuthorizedOperations);
        Assert.Empty(bare.AuthorizedOperations!);
        Assert.Equal(Uuid.Zero, bare.TopicId);

        // Java's 4-arg TopicPartitionInfo leaves elr/lastKnownElr NULL.
        TopicPartitionInfo four = new TopicPartitionInfo(
            0, null, Array.Empty<Node>(), Array.Empty<Node>());
        Assert.Null(four.Elr);
        Assert.Null(four.LastKnownElr);
    }

    private static void AssertNullableDictionary<TKey, TValue>(Type declaring, string propertyName)
    {
        PropertyInfo property = declaring.GetProperty(propertyName)!;

        Assert.Equal(typeof(IReadOnlyDictionary<TKey, TValue>), property.PropertyType);
        AssertNullableProperty(declaring, propertyName);
    }

    /// <summary>
    /// Reads the compiler-emitted nullable annotation rather than trusting the source
    /// text: <c>IReadOnlyDictionary&lt;K,V&gt;</c> and
    /// <c>IReadOnlyDictionary&lt;K,V&gt;?</c> are the <em>same</em>
    /// <see cref="Type"/>, so <see cref="PropertyInfo.PropertyType"/> alone cannot tell
    /// them apart — and the annotation is the whole of decision D8.
    /// </summary>
    private static void AssertNullableProperty(Type declaring, string propertyName) =>
        Assert.Equal(2, NullableFlag(declaring.GetProperty(propertyName)!));

    private static void AssertNonNullableProperty(Type declaring, string propertyName) =>
        Assert.Equal(1, NullableFlag(declaring.GetProperty(propertyName)!));

    private static void AssertNullableReturn(MethodInfo method) =>
        Assert.Equal(2, NullableFlag(method.ReturnParameter));

    /// <summary>
    /// 1 = not-annotated-nullable, 2 = nullable, in the C# compiler's
    /// <c>NullableAttribute</c> encoding — decoded by
    /// <see cref="NullableAnnotation"/>, which resolves a member carrying no
    /// attribute of its own against the nearest enclosing
    /// <c>NullableContextAttribute</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The enclosing-context walk is what keeps
    /// <see cref="AssertNonNullableProperty"/> reading metadata rather than a default.</b>
    /// This file originally decoded only a member's own attribute and defaulted to 1, on
    /// the stated premise that the project sets the context to 1 everywhere — false, since
    /// the compiler picks the context <em>per declaration</em>. All 3 of the
    /// <see cref="AssertNonNullableProperty"/> call sites resolved through that default
    /// without reading any metadata; each is now verified to fail when its property is
    /// widened. See <see cref="NullableAnnotation"/> for the measurement and for the
    /// sibling assertion that could not fail at all.
    /// </remarks>
    private static byte NullableFlag(MemberInfo member) => NullableAnnotation.Flag(member);

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}
