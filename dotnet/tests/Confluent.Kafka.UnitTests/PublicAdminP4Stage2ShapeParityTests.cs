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
/// Pins the <b>public shape</b> of M15/P4 Stage 2's surface —
/// <c>listPartitionReassignments</c> and <c>listOffsets</c> — against the Java classes it
/// mirrors.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>Stage 2 is Stage 1's hazard inverted, and this file is where that is pinned.</b>
/// In Stage 1 two result types had <em>identical accessor sets</em> and different shapes.
/// Here two RPCs have <em>near-identical names</em> and differ in both:
/// <c>alterPartitionReassignments</c> is per-key void,
/// <c>listPartitionReassignments</c> is one aggregate future with no per-key error, and
/// <c>listOffsets</c> is per-key with <b>both</b> a value and an error. Three shapes across
/// two similarly-named families, so the member sets are asserted rather than assumed.
/// </para>
/// <para>
/// ⚠ <b>All seven <see cref="OffsetSpec"/> kinds are walked explicitly.</b> Six of them
/// project onto overlapping numbers, so a sampled check would very plausibly miss the one
/// collision that matters.
/// </para>
/// </remarks>
public sealed class PublicAdminP4Stage2ShapeParityTests
{
    /// <summary>
    /// The two new RPCs return their <c>*Result</c> <b>synchronously</b>, with Java's
    /// parameter shape — <c>admin-client.md</c> §1, and DoD §11's spirit for admin.
    /// </summary>
    [Fact]
    public void IAdmin_TheTwoNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo listReassignments = typeof(IAdmin).GetMethod(nameof(IAdmin.ListPartitionReassignments))!;
        MethodInfo listOffsets = typeof(IAdmin).GetMethod(nameof(IAdmin.ListOffsets))!;

        Assert.Equal(typeof(ListPartitionReassignmentsResult), listReassignments.ReturnType);
        Assert.Equal(typeof(ListOffsetsResult), listOffsets.ReturnType);

        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<TopicPartition>), typeof(ListPartitionReassignmentsOptions) },
            listReassignments.GetParameters().Select(parameter => parameter.ParameterType));
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, OffsetSpec>), typeof(ListOffsetsOptions) },
            listOffsets.GetParameters().Select(parameter => parameter.ParameterType));

        // ⚠ The selection is NULLABLE and REQUIRED: null is Java's Optional.empty() —
        // "every ongoing reassignment" — and Java has no overload that omits the argument.
        ParameterInfo selection = listReassignments.GetParameters()[0];
        Assert.False(selection.IsOptional, "the selection mirrors Java's required parameter");
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(selection));

        // …whereas listOffsets' map is required AND non-nullable, matching Java.
        ParameterInfo offsets = listOffsets.GetParameters()[0];
        Assert.False(offsets.IsOptional);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(offsets, 0));

        foreach (MethodInfo rpc in new[] { listReassignments, listOffsets })
        {
            ParameterInfo options = rpc.GetParameters()[1];
            Assert.True(options.IsOptional, $"{rpc.Name}'s options must be optional");
            Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(options));
        }

        // Close remains the ONLY Task-returning member on IAdmin.
        Assert.Equal(
            new[] { nameof(IAdmin.Close) },
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// <see cref="ListPartitionReassignmentsResult"/> publishes Java's <b>single</b>
    /// accessor and nothing else (<c>ListPartitionReassignmentsResult.java:40</c>).
    /// </summary>
    [Fact]
    public void ListPartitionReassignmentsResult_PublishesExactlyOneAccessor()
    {
        MethodInfo reassignments = typeof(ListPartitionReassignmentsResult).GetMethod(
            nameof(ListPartitionReassignmentsResult.Reassignments), Type.EmptyTypes)!;

        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>),
            reassignments.ReturnType);

        // Positions of Task<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>:
        // 0 = the Task, 1 = the dictionary, 2 = the VALUE (TopicPartition, a value type,
        // takes no position). None is nullable — a missing reassignment is an absent KEY,
        // not a null value.
        for (int position = 0; position <= 2; position++)
        {
            Assert.Equal(
                NullableAnnotation.NotAnnotated,
                NullableAnnotation.Flag(reassignments.ReturnParameter, position));
        }

        // ⚠ No `Values`, no `All` — Java declares one accessor and the plan says not to add
        // a second view.
        Assert.Equal(
            new[] { nameof(ListPartitionReassignmentsResult.Reassignments) },
            DeclaredPublicMethodNames(typeof(ListPartitionReassignmentsResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(ListPartitionReassignmentsResult)));
    }

    /// <summary>
    /// <see cref="ListOffsetsResult"/> publishes Java's public constructor,
    /// <c>partitionResult</c> and <c>all</c> — and <c>all</c> yields the MAP, unlike
    /// <see cref="DeleteRecordsResult.All"/>.
    /// </summary>
    [Fact]
    public void ListOffsetsResult_PublishesJavasConstructorAndTwoAccessors()
    {
        ConstructorInfo only = Assert.Single(typeof(ListOffsetsResult).GetConstructors());
        // ⚠ Task<Info>, not Info — Java's is Map<TopicPartition, KafkaFuture<Info>> (:34).
        // The futures ARE the value; a map of plain values would be a different contract.
        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<TopicPartition, Task<ListOffsetsResult.ListOffsetsResultInfo>>),
            },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        MethodInfo partitionResult = typeof(ListOffsetsResult).GetMethod(
            nameof(ListOffsetsResult.PartitionResult))!;
        MethodInfo all = typeof(ListOffsetsResult).GetMethod(nameof(ListOffsetsResult.All), Type.EmptyTypes)!;

        Assert.Equal(
            typeof(Task<ListOffsetsResult.ListOffsetsResultInfo>), partitionResult.ReturnType);
        Assert.Equal(new[] { typeof(TopicPartition) }, partitionResult.GetParameters().Select(p => p.ParameterType));

        // ⚠ Java's all() is KafkaFuture<Map<…>>, NOT KafkaFuture<Void> — the opposite of
        // DeleteRecordsResult.All(), which they otherwise resemble.
        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>>),
            all.ReturnType);
        Assert.Equal(typeof(Task), typeof(DeleteRecordsResult).GetMethod(nameof(DeleteRecordsResult.All))!.ReturnType);

        Assert.Equal(
            new[] { nameof(ListOffsetsResult.All), nameof(ListOffsetsResult.PartitionResult) },
            DeclaredPublicMethodNames(typeof(ListOffsetsResult)));
    }

    /// <summary>
    /// <c>ListOffsetsResultInfo</c> is <b>NESTED</b> inside <see cref="ListOffsetsResult"/>
    /// (decision D17), with the three Java accessors as properties (D18) and a nullable
    /// leader epoch.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The roadmap lists it as a flat type; Java declares it <c>public static class</c>
    /// at <c>ListOffsetsResult.java:70</c>, and Java wins.</b> The nesting is asserted
    /// structurally so a later "tidy-up" that flattens it fails here.
    /// </remarks>
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

        // ⚠ int? — Java's Optional<Integer>. A plain int could not express Optional.empty()
        // at all, which is what forces the ABI's bool to be read rather than a sentinel.
        Assert.Equal(
            typeof(int?), info.GetProperty(nameof(ListOffsetsResult.ListOffsetsResultInfo.LeaderEpoch))!.PropertyType);

        Assert.Equal(
            new[] { "LeaderEpoch", "Offset", "Timestamp" }, DeclaredPublicPropertyNames(info));

        // No Java-shaped methods beyond the three properties…
        Assert.Empty(DeclaredPublicMethodNames(info));

        // …and ToString is overridden, which the helper deliberately does not report
        // because it filters `object`'s members. Java declares one at :95, so it is pinned
        // here rather than left to the filtered set.
        Assert.Same(info, info.GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        // A present epoch of -1 is PRESENT. This is the type-level half of the claim; the
        // marshaller half is in Interop.AdminP4Stage2ResultMarshalTests.
        ListOffsetsResult.ListOffsetsResultInfo negative =
            new ListOffsetsResult.ListOffsetsResultInfo(5, 7, -1);
        Assert.Equal(-1, negative.LeaderEpoch);
        Assert.Equal("ListOffsetsResultInfo(offset=5, timestamp=7, leaderEpoch=Optional[-1])", negative.ToString());

        ListOffsetsResult.ListOffsetsResultInfo absent =
            new ListOffsetsResult.ListOffsetsResultInfo(5, 7, null);
        Assert.Null(absent.LeaderEpoch);
        Assert.Equal("ListOffsetsResultInfo(offset=5, timestamp=7, leaderEpoch=Optional.empty)", absent.ToString());
    }

    /// <summary>
    /// ⚠⚠ <see cref="OffsetSpec"/> publishes <b>all seven</b> of Java's kinds and
    /// <b>all seven</b> factories, and each factory yields its own kind.
    /// </summary>
    /// <remarks>
    /// Walked exhaustively rather than sampled: six of the seven project onto overlapping
    /// wire numbers, so the one pair that collides is exactly what a sample would miss.
    /// </remarks>
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

        // Every kind derives from OffsetSpec and is sealed — Java's are leaf classes.
        Assert.All(nested, type => Assert.Same(typeof(OffsetSpec), type.BaseType));
        Assert.All(nested, type => Assert.True(type.IsSealed, $"{type.Name} must be sealed"));

        // The seven factories, each yielding its own kind. A factory wired to the wrong
        // kind would send the wrong sentinel and is caught right here.
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

        // ⚠ Java's TimestampSpec.timestamp() is package-private (:39), so the value is not
        // public here either — the seven kinds carry no public instance surface at all.
        Assert.All(nested, type => Assert.Empty(DeclaredPublicMethodNames(type)));
        Assert.All(nested, type => Assert.Empty(DeclaredPublicPropertyNames(type)));

        // The recorded deviation: the hierarchy is CLOSED, so an external subclass — which
        // Java's own encoder would silently query as latest() — is not expressible.
        Assert.Empty(
            typeof(OffsetSpec).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
    }

    /// <summary>
    /// <see cref="IsolationLevel"/>'s members carry Java's <c>id()</c> values, not
    /// C#-assigned ordinals — they cross the ABI as <c>int32_t</c> and any other id is
    /// rejected.
    /// </summary>
    [Theory]
    [InlineData(IsolationLevel.ReadUncommitted, 0)]
    [InlineData(IsolationLevel.ReadCommitted, 1)]
    public void IsolationLevel_CarriesJavasIds(IsolationLevel level, int id)
    {
        Assert.Equal(id, (int)level);
        Assert.Equal(level, (IsolationLevel)id);
    }

    /// <summary>The enum has exactly Java's two members, in the root namespace (D13).</summary>
    [Fact]
    public void IsolationLevel_HasExactlyJavasTwoMembers()
    {
        Assert.Equal(
            new[] { "ReadCommitted", "ReadUncommitted" },
            Enum.GetNames(typeof(IsolationLevel)).OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal("Confluent.Kafka", typeof(IsolationLevel).Namespace);
    }

    /// <summary>
    /// <see cref="PartitionReassignment"/> mirrors Java's constructor and three accessors —
    /// and is a <b>different type</b> from <see cref="NewPartitionReassignment"/>, which
    /// Stage 1 bound.
    /// </summary>
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

        // Java declares a toString at :62; pinned directly, since the helper filters
        // `object`'s members.
        Assert.Same(
            typeof(PartitionReassignment),
            typeof(PartitionReassignment).GetMethod("ToString", Type.EmptyTypes)!.DeclaringType);

        List<int> replicas = new List<int> { 1, 2, 3 };
        PartitionReassignment reassignment =
            new PartitionReassignment(replicas, new[] { 3 }, Array.Empty<int>());
        Assert.Equal(new[] { 1, 2, 3 }, reassignment.Replicas);
        Assert.Equal(new[] { 3 }, reassignment.AddingReplicas);
        Assert.Empty(reassignment.RemovingReplicas);

        // Collections.unmodifiableList semantics: the caller's list cannot mutate the value.
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

        // The Stage-1 input type and this report type are distinct, despite the names.
        Assert.NotEqual(typeof(NewPartitionReassignment), typeof(PartitionReassignment));
    }

    /// <summary>
    /// The two new options types match Java's fields and defaults exactly.
    /// </summary>
    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        // Java's ListPartitionReassignmentsOptions declares NO members of its own.
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

    /// <inheritdoc cref="PublicAdminP4ShapeParityTests"/>
    private static string[] DeclaredPublicMethodNames(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method =>
                !method.IsSpecialName
                && !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
                && method.GetBaseDefinition().DeclaringType != typeof(object))
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    /// <inheritdoc cref="DeclaredPublicMethodNames"/>
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
