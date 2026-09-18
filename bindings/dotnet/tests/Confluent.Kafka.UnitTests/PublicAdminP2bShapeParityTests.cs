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
/// Pins the <b>public shape</b> of M15/P2b's surface against the Java classes it mirrors.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>C# upcasts and widens silently, so these have to be reflection assertions.</b> A
/// behavioural test passes equally against <c>IReadOnlyDictionary&lt;string, Task&gt;</c>
/// and <c>IReadOnlyDictionary&lt;string, Task&lt;bool&gt;&gt;</c>, against a property and
/// a method, and against <c>T</c> and <c>T?</c> — M15/P1 shipped three shape defects
/// through a green build, 883 green tests and a 0-High first review.
/// </para>
/// <para>
/// ⚠ <b>Return-nullability is read from the Java <em>javadoc and body</em>, not the
/// signature.</b> Java has no nullable-reference annotations, so a signature alone cannot
/// say whether <c>null</c> is a legal return. Every nullable assertion below cites the
/// sentence that decides it — <c>NewPartitions.assignments()</c> is the one member of this
/// slice that returns <c>null</c>, and its javadoc says so outright
/// (<c>NewPartitions.java:83</c>: "or null if the assignment will be done by the
/// controller"). The others were checked the same way and are non-null.
/// </para>
/// </remarks>
public sealed class PublicAdminP2bShapeParityTests
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
        MethodInfo listTopics = typeof(IAdmin).GetMethod(nameof(IAdmin.ListTopics))!;
        MethodInfo createPartitions = typeof(IAdmin).GetMethod(nameof(IAdmin.CreatePartitions))!;
        MethodInfo deleteRecords = typeof(IAdmin).GetMethod(nameof(IAdmin.DeleteRecords))!;

        Assert.Equal(typeof(ListTopicsResult), listTopics.ReturnType);
        Assert.Equal(typeof(CreatePartitionsResult), createPartitions.ReturnType);
        Assert.Equal(typeof(DeleteRecordsResult), deleteRecords.ReturnType);

        // listTopics(ListTopicsOptions) — no key array in Java, and none at the ABI.
        Assert.Equal(
            new[] { typeof(ListTopicsOptions) },
            listTopics.GetParameters().Select(parameter => parameter.ParameterType));

        // createPartitions(Map<String, NewPartitions>, CreatePartitionsOptions)
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<string, NewPartitions>), typeof(CreatePartitionsOptions) },
            createPartitions.GetParameters().Select(parameter => parameter.ParameterType));

        // deleteRecords(Map<TopicPartition, RecordsToDelete>, DeleteRecordsOptions)
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, RecordsToDelete>), typeof(DeleteRecordsOptions) },
            deleteRecords.GetParameters().Select(parameter => parameter.ParameterType));

        // `options` is optional on each, collapsing Java's one-argument `default` overload,
        // and nullable — `null` means "Java's defaults".
        foreach (MethodInfo rpc in new[] { listTopics, createPartitions, deleteRecords })
        {
            ParameterInfo options = rpc.GetParameters()[rpc.GetParameters().Length - 1];
            Assert.True(options.IsOptional, $"{rpc.Name}'s options parameter must be optional");
            Assert.Equal(2, NullableFlag(options));
        }

        // Close remains the ONLY Task-returning member on IAdmin — the P1/P2a invariant,
        // re-asserted because three new members just landed beside it.
        Assert.Equal(
            new[] { nameof(IAdmin.Close) },
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// <see cref="ListTopicsResult"/> publishes Java's three <b>methods</b>, each yielding
    /// a task — and <see cref="ListTopicsResult.Names"/> is an
    /// <c>IReadOnlyCollection&lt;string&gt;</c> because <c>IReadOnlySet&lt;T&gt;</c>
    /// post-dates the netstandard2.0 floor (CLAUDE.md §3's idiom map), not because Java
    /// returns a collection — Java returns a <c>Set</c>.
    /// </summary>
    [Fact]
    public void ListTopicsResult_PublishesJavasThreeProjections()
    {
        MethodInfo namesToListings = typeof(ListTopicsResult).GetMethod(
            nameof(ListTopicsResult.NamesToListings), Type.EmptyTypes)!;
        MethodInfo listings = typeof(ListTopicsResult).GetMethod(
            nameof(ListTopicsResult.Listings), Type.EmptyTypes)!;
        MethodInfo names = typeof(ListTopicsResult).GetMethod(
            nameof(ListTopicsResult.Names), Type.EmptyTypes)!;

        Assert.Equal(typeof(Task<IReadOnlyDictionary<string, TopicListing>>), namesToListings.ReturnType);
        Assert.Equal(typeof(Task<IReadOnlyCollection<TopicListing>>), listings.ReturnType);
        Assert.Equal(typeof(Task<IReadOnlyCollection<string>>), names.ReturnType);

        // Non-null: Java's namesToListings() returns the future field, and the two
        // projections are thenApply over it (ListTopicsResult.java:39-55) — none can be
        // null.
        Assert.Equal(1, NullableFlag(namesToListings.ReturnParameter));
        Assert.Equal(1, NullableFlag(listings.ReturnParameter));
        Assert.Equal(1, NullableFlag(names.ReturnParameter));

        // Java's result has no `all()` and no per-key map: one future is the whole shape.
        Assert.Null(typeof(ListTopicsResult).GetMethod("All", Type.EmptyTypes));
        Assert.Empty(typeof(ListTopicsResult).GetProperties());

        // Java's constructor is package-private; nothing public may build one.
        Assert.Empty(typeof(ListTopicsResult).GetConstructors());
    }

    /// <summary>
    /// <see cref="TopicListing"/> mirrors Java's three accessors and its one public
    /// constructor.
    /// </summary>
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
        Assert.Equal("(name=t, topicId=" + listing.TopicId + ", internal=True)", listing.ToString());

        Assert.Throws<ArgumentNullException>(() => new TopicListing(null!, Uuid.Zero, false));
    }

    /// <summary>
    /// <see cref="CreatePartitionsResult"/> is Java's shape-2 pair — a
    /// <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c> property plus <c>all()</c>
    /// (<c>CreatePartitionsResult.java:39, :46</c>). The <c>Task&lt;bool&gt;</c> the void
    /// bridge uses internally must <b>not</b> reach the public surface.
    /// </summary>
    [Fact]
    public void CreatePartitionsResult_PublishesVoidFutures()
    {
        PropertyInfo values = typeof(CreatePartitionsResult).GetProperty(
            nameof(CreatePartitionsResult.Values))!;

        Assert.Equal(typeof(IReadOnlyDictionary<string, Task>), values.PropertyType);
        Assert.Equal(1, NullableFlag(values));
        Assert.Null(values.SetMethod);

        MethodInfo all = typeof(CreatePartitionsResult).GetMethod(
            nameof(CreatePartitionsResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(Task), all.ReturnType);

        // Unlike DeleteTopicsResult there is no by-id form, so neither accessor is
        // nullable and there is exactly one map.
        Assert.Single(typeof(CreatePartitionsResult).GetProperties());

        // Java's constructor is package-private.
        Assert.Empty(typeof(CreatePartitionsResult).GetConstructors());
    }

    /// <summary>
    /// <see cref="DeleteRecordsResult"/> is keyed by the shipped
    /// <see cref="TopicPartition"/> and carries <see cref="DeletedRecords"/> per key
    /// (<c>DeleteRecordsResult.java:40, :47</c>) — and, uniquely among the admin results
    /// bound so far, Java makes its constructor <b>public</b>
    /// (<c>DeleteRecordsResult.java:32</c>).
    /// </summary>
    [Fact]
    public void DeleteRecordsResult_IsKeyedByTopicPartition_AndHasJavasPublicConstructor()
    {
        PropertyInfo lowWatermarks = typeof(DeleteRecordsResult).GetProperty(
            nameof(DeleteRecordsResult.LowWatermarks))!;

        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartition, Task<DeletedRecords>>),
            lowWatermarks.PropertyType);
        Assert.Equal(1, NullableFlag(lowWatermarks));
        Assert.Null(lowWatermarks.SetMethod);

        MethodInfo all = typeof(DeleteRecordsResult).GetMethod(
            nameof(DeleteRecordsResult.All), Type.EmptyTypes)!;

        // Java's all() is KafkaFuture<Void> — it reports success, not the watermarks.
        Assert.Equal(typeof(Task), all.ReturnType);

        ConstructorInfo only = Assert.Single(typeof(DeleteRecordsResult).GetConstructors());
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, Task<DeletedRecords>>) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Throws<ArgumentNullException>(() => new DeleteRecordsResult(null!));
    }

    /// <summary>
    /// ⚠ <b><see cref="NewPartitions.Assignments"/> is the one nullable member of this
    /// slice, and the annotation is load-bearing.</b> Java's javadoc
    /// (<c>NewPartitions.java:83</c>) reads "The replica assignments for the new
    /// partitions, <b>or null if the assignment will be done by the controller</b>", and
    /// the body confirms it — <c>increaseTo(int)</c> passes <c>null</c>
    /// (<c>:44</c>). Collapsing that to an empty list would change the wire request.
    /// </summary>
    [Fact]
    public void NewPartitions_AssignmentsIsNullable_AndTheFactoriesMirrorJava()
    {
        PropertyInfo assignments = typeof(NewPartitions).GetProperty(nameof(NewPartitions.Assignments))!;

        Assert.Equal(typeof(IReadOnlyList<IReadOnlyList<int>>), assignments.PropertyType);
        Assert.Equal(2, NullableFlag(assignments));

        Assert.Equal(typeof(int), typeof(NewPartitions).GetProperty(nameof(NewPartitions.TotalCount))!.PropertyType);

        // Java's two static factories and no public constructor.
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

        // The list parameter is NON-nullable: `IncreaseTo(int)` already spells "no
        // assignments", so accepting null would be a second spelling of it (recorded
        // sub-divergence — Java's increaseTo(n, null) is accepted).
        Assert.Equal(1, NullableFlag(factories[1].GetParameters()[1]));
        Assert.Throws<ArgumentNullException>(() => NewPartitions.IncreaseTo(3, null!));
    }

    /// <summary>
    /// <see cref="RecordsToDelete"/> keeps Java's static-factory / instance-accessor pair
    /// of the <em>same name</em>, plus the value-equality members
    /// (<c>RecordsToDelete.java:39-69</c>) — including Java's truncating
    /// <c>hashCode()</c> of <c>(int) offset</c>.
    /// </summary>
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

        // A METHOD, not a property: C# forbids a property and a method sharing a name, and
        // the static factory already owns it. Java has the identical pair.
        Assert.Empty(typeof(RecordsToDelete).GetProperties());
        Assert.Empty(typeof(RecordsToDelete).GetConstructors());

        RecordsToDelete five = RecordsToDelete.BeforeOffset(5);
        Assert.Equal(5, five.BeforeOffset());
        Assert.Equal(five, RecordsToDelete.BeforeOffset(5));
        Assert.NotEqual(five, RecordsToDelete.BeforeOffset(6));
        Assert.Equal(RecordsToDelete.BeforeOffset(5).GetHashCode(), five.GetHashCode());
        Assert.Equal("(beforeOffset = 5)", five.ToString());

        // Java's hashCode() is `(int) offset` — a truncation, not a mix. Asserted with a
        // value whose low 32 bits differ from the whole, so a "better" hash goes red.
        Assert.Equal(1, RecordsToDelete.BeforeOffset((1L << 32) + 1).GetHashCode());

        // -1 is Java's documented "truncate to the high watermark" — a value, not a
        // rejected sentinel.
        Assert.Equal(-1, RecordsToDelete.BeforeOffset(-1).BeforeOffset());

        Assert.False(five.Equals(null));
        Assert.False(five.Equals("5"));
    }

    /// <summary>
    /// <see cref="DeletedRecords"/> mirrors Java's single-argument constructor, and
    /// <b>-1 is a value it carries</b> rather than a failure it represents.
    /// </summary>
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

    /// <summary>
    /// The three new options types carry exactly Java's fields and Java's defaults, so
    /// <c>options: null</c> at a call site behaves like a freshly constructed instance.
    /// </summary>
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

        // Java's DeleteRecordsOptions declares NO members of its own — it is an empty
        // subclass of AbstractOptions, so the inherited timeout is the whole surface.
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

    private static void AssertOptionNames(Type optionsType, params string[] expected) =>
        Assert.Equal(
            expected.OrderBy(name => name, StringComparer.Ordinal),
            optionsType.GetProperties().Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal));

    /// <summary>
    /// 1 = not-annotated-nullable, 2 = nullable, in the C# compiler's
    /// <c>NullableAttribute</c> encoding — decoded by
    /// <see cref="NullableAnnotation"/>, which resolves a member carrying no
    /// attribute of its own against the nearest enclosing
    /// <c>NullableContextAttribute</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ Reading this rather than the <see cref="Type"/> is the whole point:
    /// <c>IReadOnlyList&lt;T&gt;</c> and <c>IReadOnlyList&lt;T&gt;?</c> are the
    /// <em>same</em> <see cref="Type"/>, so <see cref="PropertyInfo.PropertyType"/> alone
    /// cannot tell them apart.
    /// </para>
    /// <para>
    /// ⚠ <b>The enclosing-context walk is not a detail.</b> This file originally decoded
    /// only a member's own attribute and defaulted to 1, on the stated premise that the
    /// project sets the context to 1 everywhere. That premise was false — the compiler
    /// picks the context <em>per declaration</em>, and picks 2 where nullable positions
    /// dominate — and all 7 of the non-nullable assertions here resolved through that
    /// default without reading any metadata. Widening each one showed 6 would have been
    /// caught by an attribute the compiler happened to emit, while
    /// <c>TopicListing.Name</c> could <em>not</em> fail at all. See
    /// <see cref="NullableAnnotation"/> for the full measurement and the split.
    /// </para>
    /// </remarks>
    private static byte NullableFlag(MemberInfo member) => NullableAnnotation.Flag(member);

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}
