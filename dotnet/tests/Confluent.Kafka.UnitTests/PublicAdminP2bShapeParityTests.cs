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
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public shape of <c>ListTopics</c>, <c>CreatePartitions</c> and <c>DeleteRecords</c>, their options, results
/// and value types, against the Java classes they mirror.
/// </summary>
/// <remarks>
/// These are reflection assertions: a behavioural test passes equally against a property and a method, or against
/// <c>T</c> and <c>T?</c>. Return nullability follows Java's javadoc and body: <c>NewPartitions.Assignments</c> is the
/// one nullable return, as Java's javadoc says ("or null if the assignment will be done by the controller").
/// </remarks>
public sealed class PublicAdminP2bShapeParityTests
{
    [Fact]
    public void IAdmin_TheThreeNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo listTopics = typeof(IAdmin).GetMethod(nameof(IAdmin.ListTopics))!;
        MethodInfo createPartitions = typeof(IAdmin).GetMethod(nameof(IAdmin.CreatePartitions))!;
        MethodInfo deleteRecords = typeof(IAdmin).GetMethod(nameof(IAdmin.DeleteRecords))!;

        Assert.Equal(typeof(ListTopicsResult), listTopics.ReturnType);
        Assert.Equal(typeof(CreatePartitionsResult), createPartitions.ReturnType);
        Assert.Equal(typeof(DeleteRecordsResult), deleteRecords.ReturnType);

        Assert.Equal(
            new[] { typeof(ListTopicsOptions) },
            listTopics.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<string, NewPartitions>), typeof(CreatePartitionsOptions) },
            createPartitions.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, RecordsToDelete>), typeof(DeleteRecordsOptions) },
            deleteRecords.GetParameters().Select(parameter => parameter.ParameterType));

        // The options are optional, collapsing Java's one-argument default overload, and nullable: null means Java's
        // defaults.
        foreach (MethodInfo rpc in new[] { listTopics, createPartitions, deleteRecords })
        {
            ParameterInfo options = rpc.GetParameters()[rpc.GetParameters().Length - 1];
            Assert.True(options.IsOptional, $"{rpc.Name}'s options parameter must be optional");
            Assert.Equal(2, NullableFlag(options));
        }

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void ListTopicsResult_PublishesJavasThreeProjections()
    {
        MethodInfo namesToListings = typeof(ListTopicsResult).GetMethod(
            nameof(ListTopicsResult.NamesToListings), Type.EmptyTypes)!;
        MethodInfo listings = typeof(ListTopicsResult).GetMethod(
            nameof(ListTopicsResult.Listings), Type.EmptyTypes)!;
        MethodInfo names = typeof(ListTopicsResult).GetMethod(
            nameof(ListTopicsResult.Names), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture<IReadOnlyDictionary<string, TopicListing>>), namesToListings.ReturnType);
        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<TopicListing>>), listings.ReturnType);
        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<string>>), names.ReturnType);

        // Non-null: Java's future and its two thenApply projections never carry null. The future is a struct, so position 0
        // carries no annotation and the value's own sits at position 1.
        Assert.Equal(1, NullableAnnotation.Flag(namesToListings.ReturnParameter, 1));
        Assert.Equal(1, NullableAnnotation.Flag(listings.ReturnParameter, 1));
        Assert.Equal(1, NullableAnnotation.Flag(names.ReturnParameter, 1));

        Assert.Null(typeof(ListTopicsResult).GetMethod("All", Type.EmptyTypes));
        Assert.Empty(typeof(ListTopicsResult).GetProperties());

        Assert.Empty(typeof(ListTopicsResult).GetConstructors());
    }

    [Fact]
    public void TopicListing_MirrorsJavasShape()
    {
        ConstructorInfo only = Assert.Single(typeof(TopicListing).GetConstructors());
        Assert.Equal(
            new[] { typeof(string), typeof(Uuid), typeof(bool) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(typeof(string), typeof(TopicListing).GetProperty(nameof(TopicListing.Name))!.PropertyType);
        Assert.Equal(typeof(Uuid), typeof(TopicListing).GetProperty(nameof(TopicListing.TopicId))!.PropertyType);
        Assert.Equal(typeof(bool), typeof(TopicListing).GetProperty(nameof(TopicListing.IsInternal))!.PropertyType);

        // Java's topicId() is a Uuid, not the base64 string the ABI transports.
        Assert.Equal(1, NullableFlag(typeof(TopicListing).GetProperty(nameof(TopicListing.Name))!));

        TopicListing listing = new TopicListing("t", new Uuid(1L, 2L), isInternal: true);
        Assert.Equal("t", listing.Name);
        Assert.True(listing.IsInternal);
        // Java renders the boolean in lowercase, where bool.ToString() gives "True".
        Assert.Equal("(name=t, topicId=" + listing.TopicId + ", internal=true)", listing.ToString());
        Assert.Equal(
            "(name=t, topicId=" + listing.TopicId + ", internal=false)",
            new TopicListing("t", new Uuid(1L, 2L), isInternal: false).ToString());

        Assert.Throws<ArgumentNullException>(() => new TopicListing(null!, Uuid.Zero, false));
    }

    [Fact]
    public void CreatePartitionsResult_PublishesVoidFutures()
    {
        PropertyInfo values = typeof(CreatePartitionsResult).GetProperty(
            nameof(CreatePartitionsResult.Values))!;

        Assert.Equal(typeof(IReadOnlyDictionary<string, KafkaFuture>), values.PropertyType);
        Assert.Equal(1, NullableFlag(values));
        Assert.Null(values.SetMethod);

        MethodInfo all = typeof(CreatePartitionsResult).GetMethod(
            nameof(CreatePartitionsResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(KafkaFuture), all.ReturnType);

        Assert.Single(typeof(CreatePartitionsResult).GetProperties());

        Assert.Empty(typeof(CreatePartitionsResult).GetConstructors());
    }

    [Fact]
    public void DeleteRecordsResult_IsKeyedByTopicPartition_AndItsConstructorIsInternal()
    {
        PropertyInfo lowWatermarks = typeof(DeleteRecordsResult).GetProperty(
            nameof(DeleteRecordsResult.LowWatermarks))!;

        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>>),
            lowWatermarks.PropertyType);
        Assert.Equal(1, NullableFlag(lowWatermarks));
        Assert.Null(lowWatermarks.SetMethod);

        MethodInfo all = typeof(DeleteRecordsResult).GetMethod(
            nameof(DeleteRecordsResult.All), Type.EmptyTypes)!;

        // Java's all() is KafkaFuture<Void> — it reports success, not the watermarks.
        Assert.Equal(typeof(KafkaFuture), all.ReturnType);

        Assert.Empty(typeof(DeleteRecordsResult).GetConstructors());
        ConstructorInfo only = Assert.Single(
            typeof(DeleteRecordsResult).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance),
            candidate => candidate.IsAssembly);
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>>) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Throws<ArgumentNullException>(() => new DeleteRecordsResult(null!));
    }

    [Fact]
    public void NewPartitions_AssignmentsIsNullable_AndTheFactoriesMirrorJava()
    {
        PropertyInfo assignments = typeof(NewPartitions).GetProperty(nameof(NewPartitions.Assignments))!;

        Assert.Equal(typeof(IReadOnlyList<IReadOnlyList<int>>), assignments.PropertyType);
        Assert.Equal(2, NullableFlag(assignments));

        Assert.Equal(typeof(int), typeof(NewPartitions).GetProperty(nameof(NewPartitions.TotalCount))!.PropertyType);

        Assert.Empty(typeof(NewPartitions).GetConstructors());

        MethodInfo[] factories = typeof(NewPartitions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(method => method.Name == nameof(NewPartitions.IncreaseTo))
            .OrderBy(method => method.GetParameters().Length)
            .ToArray();

        Assert.Equal(2, factories.Length);
        Assert.Equal(new[] { typeof(int) }, factories[0].GetParameters().Select(p => p.ParameterType));
        Assert.Equal(
            new[] { typeof(int), typeof(IReadOnlyList<IReadOnlyList<int>>) },
            factories[1].GetParameters().Select(p => p.ParameterType));

        // The outer list is nullable, as Java's increaseTo(n, null) is increaseTo(n); an inner broker-id list is not
        // (position 1 of the flattened type).
        ParameterInfo newAssignments = factories[1].GetParameters()[1];
        Assert.Equal("newAssignments", newAssignments.Name);
        Assert.Equal(2, NullableAnnotation.Flag(newAssignments, 0));
        Assert.Equal(1, NullableAnnotation.Flag(newAssignments, 1));

        NewPartitions viaNull = NewPartitions.IncreaseTo(3, null);
        Assert.Equal(3, viaNull.TotalCount);
        Assert.Null(viaNull.Assignments);
        Assert.Equal(NewPartitions.IncreaseTo(3).ToString(), viaNull.ToString());
        Assert.Equal("(totalCount=3, newAssignments=null)", viaNull.ToString());

        NewPartitions viaEmpty = NewPartitions.IncreaseTo(3, Array.Empty<IReadOnlyList<int>>());
        Assert.NotNull(viaEmpty.Assignments);
        Assert.Empty(viaEmpty.Assignments!);
        Assert.Equal("(totalCount=3, newAssignments=[])", viaEmpty.ToString());

        // A null INNER list stays rejected: the ABI would drop it, shifting every later
        // partition's assignment.
        ArgumentNullException nullInner = Assert.Throws<ArgumentNullException>(
            () => NewPartitions.IncreaseTo(3, new IReadOnlyList<int>[] { new[] { 1 }, null! }));
        Assert.Equal("newAssignments", nullInner.ParamName);
        Assert.StartsWith(
            "A replica assignment must not be a null broker-id list.",
            nullInner.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void RecordsToDelete_MirrorsJavasFactoryAccessorPair_AndValueEquality()
    {
        MethodInfo factory = typeof(RecordsToDelete).GetMethod(
            nameof(RecordsToDelete.BeforeOffset), BindingFlags.Public | BindingFlags.Static)!;
        Assert.Equal(typeof(RecordsToDelete), factory.ReturnType);
        Assert.Equal(new[] { typeof(long) }, factory.GetParameters().Select(p => p.ParameterType));

        MethodInfo accessor = typeof(RecordsToDelete).GetMethod(
            nameof(RecordsToDelete.BeforeOffset), BindingFlags.Public | BindingFlags.Instance)!;
        Assert.Equal(typeof(long), accessor.ReturnType);
        Assert.Empty(accessor.GetParameters());

        // A method, not a property: C# forbids a property and a method sharing a name, and the static factory owns it.
        Assert.Empty(typeof(RecordsToDelete).GetProperties());
        Assert.Empty(typeof(RecordsToDelete).GetConstructors());

        RecordsToDelete five = RecordsToDelete.BeforeOffset(5);
        Assert.Equal(5, five.BeforeOffset());
        Assert.Equal(five, RecordsToDelete.BeforeOffset(5));
        Assert.NotEqual(five, RecordsToDelete.BeforeOffset(6));
        Assert.Equal(RecordsToDelete.BeforeOffset(5).GetHashCode(), five.GetHashCode());
        Assert.Equal("(beforeOffset = 5)", five.ToString());

        // Java's hashCode() is (int) offset, a truncation: asserted with a value whose low 32 bits differ from the whole.
        Assert.Equal(1, RecordsToDelete.BeforeOffset((1L << 32) + 1).GetHashCode());

        // -1 is Java's "truncate to the high watermark": a value, not a rejected sentinel.
        Assert.Equal(-1, RecordsToDelete.BeforeOffset(-1).BeforeOffset());

        Assert.False(five.Equals(null));
        Assert.False(five.Equals("5"));
    }

    [Fact]
    public void DeletedRecords_CarriesTheWatermark_IncludingMinusOne()
    {
        ConstructorInfo only = Assert.Single(typeof(DeletedRecords).GetConstructors());
        Assert.Equal(new[] { typeof(long) }, only.GetParameters().Select(p => p.ParameterType));

        PropertyInfo watermark = typeof(DeletedRecords).GetProperty(nameof(DeletedRecords.LowWatermark))!;
        Assert.Equal(typeof(long), watermark.PropertyType);
        Assert.Null(watermark.SetMethod);

        Assert.Equal(7, new DeletedRecords(7).LowWatermark);
        Assert.Equal(-1, new DeletedRecords(-1).LowWatermark);
    }

    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        ListTopicsOptions list = new ListTopicsOptions();
        Assert.Null(list.TimeoutMs);
        Assert.False(list.ListInternal);
        AssertOptionNames(typeof(ListTopicsOptions), nameof(ListTopicsOptions.ListInternal), nameof(list.TimeoutMs));

        CreatePartitionsOptions create = new CreatePartitionsOptions();
        Assert.Null(create.TimeoutMs);
        Assert.False(create.ValidateOnly);

        // The one option whose Java default is not the C# default for its type.
        Assert.True(create.RetryOnQuotaViolation);
        AssertOptionNames(
            typeof(CreatePartitionsOptions),
            nameof(CreatePartitionsOptions.RetryOnQuotaViolation),
            nameof(create.TimeoutMs),
            nameof(CreatePartitionsOptions.ValidateOnly));

        // Java's DeleteRecordsOptions declares no members of its own, so the inherited timeout is the whole surface.
        DeleteRecordsOptions delete = new DeleteRecordsOptions();
        Assert.Null(delete.TimeoutMs);
        AssertOptionNames(typeof(DeleteRecordsOptions), nameof(delete.TimeoutMs));

        foreach (Type type in new[]
                 {
                     typeof(ListTopicsOptions), typeof(CreatePartitionsOptions), typeof(DeleteRecordsOptions),
                 })
        {
            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);
        }
    }

    [Fact]
    public void G1_13_ListTopicsOptions_EqualityIgnoresTheTimeout()
    {
        ListTopicsOptions shortTimeout = new ListTopicsOptions { ListInternal = true, TimeoutMs = 10 };
        ListTopicsOptions longTimeout = new ListTopicsOptions { ListInternal = true, TimeoutMs = 99_000 };
        ListTopicsOptions noTimeout = new ListTopicsOptions { ListInternal = true };

        Assert.True(shortTimeout.Equals(longTimeout));
        Assert.True(shortTimeout.Equals(noTimeout));
        Assert.Equal(shortTimeout.GetHashCode(), longTimeout.GetHashCode());
        Assert.Equal(shortTimeout.GetHashCode(), noTimeout.GetHashCode());
        Assert.True(new ListTopicsOptions().Equals(new ListTopicsOptions { TimeoutMs = 5 }));

        Assert.False(shortTimeout.Equals(new ListTopicsOptions { ListInternal = false, TimeoutMs = 10 }));
        Assert.False(shortTimeout.Equals(null));
        Assert.False(shortTimeout.Equals(new DeleteRecordsOptions()));

        Type type = typeof(ListTopicsOptions);
        Assert.Same(type, type.GetMethod(nameof(Equals), new[] { typeof(object) })!.DeclaringType);
        Assert.Same(type, type.GetMethod(nameof(GetHashCode), Type.EmptyTypes)!.DeclaringType);
    }

    [Fact]
    public void G1_17_ListTopicsOptions_ToString_MatchesJava()
    {
        Assert.Equal("ListTopicsOptions(listInternal=false)", new ListTopicsOptions().ToString());
        Assert.Equal(
            "ListTopicsOptions(listInternal=true)",
            new ListTopicsOptions { ListInternal = true, TimeoutMs = 10 }.ToString());
    }

    private static void AssertOptionNames(Type optionsType, params string[] expected) =>
        Assert.Equal(
            expected.OrderBy(name => name, StringComparer.Ordinal),
            optionsType.GetProperties().Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal));

    private static byte NullableFlag(MemberInfo member) => NullableAnnotation.Flag(member);

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}
