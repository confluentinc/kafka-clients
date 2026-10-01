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
/// Pins the <b>public shape</b> of M15/P4 Stage 1's surface — <c>electLeaders</c> and
/// <c>alterPartitionReassignments</c> — against the Java classes it mirrors.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>C# upcasts and widens silently, so these have to be reflection assertions.</b> A
/// behavioural test passes equally against a property and a method, against
/// <c>KafkaException</c> and <c>KafkaException?</c>, and against a collection and a set.
/// </para>
/// <para>
/// ⚠⚠ <b>This file also carries half of the phase's swap detection.</b>
/// <c>kafka_admin_ElectLeadersResult_t</c> and
/// <c>kafka_admin_AlterPartitionReassignmentsResult_t</c> expose byte-identical accessor
/// sets — <c>count</c>, <c>get_topic</c>, <c>get_partition</c>, <c>get_error</c>,
/// <c>destroy</c> — while their Java shapes differ, so the two are exactly the pair a
/// maintainer could "correct" into each other. The structural half is here: one result
/// publishes a <b>single</b> awaitable over a map whose values are the outcomes, the other
/// publishes <b>one awaitable per partition</b>, and neither carries the other's member.
/// The behavioural half — that the two walker routings really do produce different
/// outcomes for the same per-partition error — is in
/// <c>Interop.AdminP4ResultMarshalTests</c>, and the wiring half, that each RPC reaches
/// the routing it is supposed to, is in <c>Interop.AdminP4SubmitArgumentTests</c>.
/// </para>
/// </remarks>
public sealed class PublicAdminP4ShapeParityTests
{
    /// <summary>
    /// The two new RPCs return their <c>*Result</c> <b>synchronously</b> — Java's
    /// <c>Admin</c> methods do not block, so the <see cref="Task"/> mapping belongs on the
    /// futures inside the result, never on the method (<c>admin-client.md</c> §1). This is
    /// DoD §11's spirit for admin.
    /// </summary>
    [Fact]
    public void IAdmin_TheTwoNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo elect = typeof(IAdmin).GetMethod(nameof(IAdmin.ElectLeaders))!;
        MethodInfo reassign = typeof(IAdmin).GetMethod(nameof(IAdmin.AlterPartitionReassignments))!;

        Assert.Equal(typeof(ElectLeadersResult), elect.ReturnType);
        Assert.Equal(typeof(AlterPartitionReassignmentsResult), reassign.ReturnType);

        // electLeaders(ElectionType, Set<TopicPartition>, ElectLeadersOptions) —
        // IReadOnlySet<T> post-dates the netstandard2.0 floor (CLAUDE.md §3's idiom map).
        Assert.Equal(
            new[]
            {
                typeof(ElectionType),
                typeof(IReadOnlyCollection<TopicPartition>),
                typeof(ElectLeadersOptions),
            },
            elect.GetParameters().Select(parameter => parameter.ParameterType));

        // alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>>,
        // AlterPartitionReassignmentsOptions) — Optional<T> becomes a nullable value.
        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<TopicPartition, NewPartitionReassignment>),
                typeof(AlterPartitionReassignmentsOptions),
            },
            reassign.GetParameters().Select(parameter => parameter.ParameterType));

        // ⚠ The partition selection is REQUIRED, because Java's is: `electLeaders` has no
        // overload that omits the Set. Only `options` collapses Java's convenience overload.
        ParameterInfo partitions = elect.GetParameters()[1];
        Assert.False(partitions.IsOptional, "the partition selection mirrors Java's required Set parameter");
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(partitions));

        Assert.True(elect.GetParameters()[2].IsOptional);
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(elect.GetParameters()[2]));
        Assert.True(reassign.GetParameters()[1].IsOptional);
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(reassign.GetParameters()[1]));

        // ⚠ The reassignment VALUE is nullable — that null is Java's Optional.empty(),
        // i.e. "revert this partition". Position 1 of the flattened parameter type:
        // position 0 is the dictionary itself, and TopicPartition, being a value type,
        // takes no position at all. An assertion at position 0 would be green however the
        // value is widened or narrowed.
        Assert.Equal(
            NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(reassign.GetParameters()[0], 0));
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(reassign.GetParameters()[0], 1));

        // Close remains the ONLY Task-returning member on IAdmin — the P1..P3 invariant,
        // re-asserted because two new members just landed beside it.
        Assert.Equal(
            new[] { nameof(IAdmin.Close) },
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// <see cref="ElectLeadersResult"/> publishes Java's <b>two</b> accessors, and its
    /// <c>partitions()</c> map value is the <b>nullable</b>
    /// <see cref="KafkaException"/> that Java spells <c>Optional&lt;Throwable&gt;</c>.
    /// </summary>
    /// <remarks>
    /// ⚠⚠ <b>The nullable map value is the phase's central shape claim.</b> A binding that
    /// routed this RPC through the per-key bridge would have no map at all — it would
    /// publish <c>Values</c> and fault a partition's own task — so the two assertions
    /// below (a <c>Partitions</c> method, and no <c>Values</c> member) are what makes that
    /// "correction" visible. And a binding that kept the map but made its value
    /// non-nullable could not express Java's empty <c>Optional</c>, i.e. success.
    /// </remarks>
    [Fact]
    public void ElectLeadersResult_PublishesOneFutureOverAMapOfNullableErrors()
    {
        MethodInfo partitions = typeof(ElectLeadersResult).GetMethod(
            nameof(ElectLeadersResult.Partitions), Type.EmptyTypes)!;
        MethodInfo all = typeof(ElectLeadersResult).GetMethod(nameof(ElectLeadersResult.All), Type.EmptyTypes)!;

        Assert.Equal(typeof(Task<IReadOnlyDictionary<TopicPartition, KafkaException>>), partitions.ReturnType);
        Assert.Equal(typeof(Task), all.ReturnType);

        // Flattened positions of Task<IReadOnlyDictionary<TopicPartition, KafkaException?>>:
        // 0 = the Task (never null), 1 = the dictionary, 2 = the map's VALUE. TopicPartition
        // is a value type and takes no position. Position 2 is the one under test — an
        // assertion at 0 or 1 is green however the value is widened.
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(partitions.ReturnParameter, 0));
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(partitions.ReturnParameter, 1));
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(partitions.ReturnParameter, 2));

        // Java's two accessors and nothing else. No `Values`: this result has no per-key
        // future, which is precisely what separates it from its accessor-set twin.
        Assert.Equal(
            new[] { nameof(ElectLeadersResult.All), nameof(ElectLeadersResult.Partitions) },
            DeclaredPublicMethodNames(typeof(ElectLeadersResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(ElectLeadersResult)));
    }

    /// <summary>
    /// <see cref="AlterPartitionReassignmentsResult"/> publishes Java's <c>values()</c> map
    /// of per-partition futures and <c>all()</c> — the ordinary per-key shape its
    /// accessor-set twin is not.
    /// </summary>
    [Fact]
    public void AlterPartitionReassignmentsResult_PublishesOneFuturePerPartition()
    {
        PropertyInfo values = typeof(AlterPartitionReassignmentsResult).GetProperty(
            nameof(AlterPartitionReassignmentsResult.Values))!;
        MethodInfo all = typeof(AlterPartitionReassignmentsResult).GetMethod(
            nameof(AlterPartitionReassignmentsResult.All), Type.EmptyTypes)!;

        // Java's per-partition future is KafkaFuture<Void>, so the erased Task carries no
        // value — the void bridge's internal Task<bool> must not reach the public surface.
        Assert.Equal(typeof(IReadOnlyDictionary<TopicPartition, Task>), values.PropertyType);
        Assert.Equal(typeof(Task), all.ReturnType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(values));
        Assert.Null(values.SetMethod);

        Assert.Equal(
            new[] { nameof(AlterPartitionReassignmentsResult.All) },
            DeclaredPublicMethodNames(typeof(AlterPartitionReassignmentsResult)));
        Assert.Equal(
            new[] { nameof(AlterPartitionReassignmentsResult.Values) },
            DeclaredPublicPropertyNames(typeof(AlterPartitionReassignmentsResult)));
    }

    /// <summary>
    /// ⚠⚠ <b>The accessor-set twins are not interchangeable at the public surface.</b>
    /// Neither result type carries the other's member, so the "correction" the identical
    /// ABI accessor sets invite cannot be made without changing a signature this file
    /// pins.
    /// </summary>
    /// <remarks>
    /// Written as a member-set difference rather than as two <c>Assert.Null</c> calls so
    /// that a member added to either type in future has to be classified deliberately.
    /// </remarks>
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

        // `All` is the only member they share, and even it differs in meaning: one walks a
        // map's values for the first present error (ElectLeadersResult.java:57-70), the
        // other is KafkaFuture.allOf over N futures (:52).
        Assert.Equal(new[] { "All" }, elect.Intersect(reassign, StringComparer.Ordinal));
    }

    /// <summary>
    /// <see cref="ElectionType"/>'s members carry Java's <c>ElectionType.value</c> bytes,
    /// not C#-assigned ordinals — they cross the ABI as <c>int32_t</c>.
    /// </summary>
    [Theory]
    [InlineData(ElectionType.Preferred, 0)]
    [InlineData(ElectionType.Unclean, 1)]
    public void ElectionType_CarriesJavasValues(ElectionType type, int value)
    {
        Assert.Equal(value, (int)type);
        Assert.Equal(type, (ElectionType)value);
    }

    /// <summary>The enum has exactly Java's two members, and no more.</summary>
    [Fact]
    public void ElectionType_HasExactlyJavasTwoMembers()
    {
        Assert.Equal(
            new[] { "Preferred", "Unclean" },
            Enum.GetNames(typeof(ElectionType)).OrderBy(name => name, StringComparer.Ordinal));

        // Java's package is org.apache.kafka.common, so the root namespace (D13) — beside
        // AclOperation / Node / TopicCollection, NOT Confluent.Kafka.Admin.
        Assert.Equal("Confluent.Kafka", typeof(ElectionType).Namespace);
    }

    /// <summary>
    /// <see cref="NewPartitionReassignment"/> mirrors Java's constructor and its one
    /// accessor, rejects an empty replica list as Java does, and copies the list.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The empty-list rejection is what keeps "cancel" expressible.</b> If an empty
    /// list were accepted it would become a second, silent spelling of a request the ABI
    /// rejects — and the temptation to read it as "revert" is exactly the collapse the
    /// dedicated <c>cancel</c> flag exists to prevent.
    /// </remarks>
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

        // Java's package is org.apache.kafka.clients.admin.
        Assert.Equal("Confluent.Kafka.Admin", typeof(NewPartitionReassignment).Namespace);

        List<int> replicas = new List<int> { 3, 1, 2 };
        NewPartitionReassignment reassignment = new NewPartitionReassignment(replicas);
        Assert.Equal(new[] { 3, 1, 2 }, reassignment.TargetReplicas);

        // List.copyOf semantics: mutating the caller's list cannot change a built request.
        replicas.Add(9);
        Assert.Equal(new[] { 3, 1, 2 }, reassignment.TargetReplicas);

        // Java throws IllegalArgumentException for BOTH, with this message
        // (NewPartitionReassignment.java:33-35); the .NET family splits null out.
        ArgumentException empty =
            Assert.Throws<ArgumentException>(() => new NewPartitionReassignment(Array.Empty<int>()));
        Assert.Equal("targetReplicas", empty.ParamName);
        Assert.Contains("Cannot create a new partition reassignment without any replicas", empty.Message);

        ArgumentNullException missing =
            Assert.Throws<ArgumentNullException>(() => new NewPartitionReassignment(null!));
        Assert.Equal("targetReplicas", missing.ParamName);
        Assert.Contains("Cannot create a new partition reassignment without any replicas", missing.Message);

        // …and the null case is still catchable the way Java's single catch clause is.
        Assert.IsAssignableFrom<ArgumentException>(missing);
    }

    /// <summary>
    /// The two new options types match Java's fields and defaults exactly, so
    /// <c>options: null</c> at a call site behaves like a freshly constructed instance.
    /// </summary>
    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        // Java's ElectLeadersOptions declares NO members of its own — an empty final
        // subclass of AbstractOptions, so the inherited timeout is the whole surface.
        ElectLeadersOptions elect = new ElectLeadersOptions();
        Assert.Null(elect.TimeoutMs);
        Assert.Equal(new[] { nameof(ElectLeadersOptions.TimeoutMs) }, PropertyNames(typeof(ElectLeadersOptions)));

        // allowReplicationFactorChange defaults to TRUE
        // (AlterPartitionReassignmentsOptions.java:27) — not the C# default for its type.
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

    /// <summary>
    /// The type's own public instance methods, by name.
    /// </summary>
    /// <remarks>
    /// ⚠ The set is selected by <see cref="BindingFlags"/> plus
    /// <see cref="CompilerGeneratedAttribute"/> and <see cref="MethodBase.IsSpecialName"/>
    /// — never by a name predicate (decision D20). Property accessors and operators carry
    /// <c>IsSpecialName</c>; anything the compiler emits carries the attribute. A
    /// <c>StartsWith("get_")</c> filter would also swallow a real Java bean accessor, which
    /// <c>ReplicaLogDirInfo</c>'s four <c>Get*</c> methods already show is a live shape in
    /// this assembly.
    /// </remarks>
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
