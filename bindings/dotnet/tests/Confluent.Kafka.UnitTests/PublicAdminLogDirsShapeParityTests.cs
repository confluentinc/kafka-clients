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
/// Pins the <b>public shape</b> of M15/P3 Stage 3's surface against the Java classes it
/// mirrors.
/// </summary>
/// <remarks>
/// ⚠ <b>C# upcasts and widens silently, so these have to be reflection assertions.</b> A
/// behavioural test passes equally against <c>Task</c> and <c>Task&lt;Map&gt;</c>, against a
/// property and a method, and against <c>T</c> and <c>T?</c>.
/// </remarks>
public sealed class PublicAdminLogDirsShapeParityTests
{
    /// <summary>
    /// The three new RPCs return their <c>*Result</c> <b>synchronously</b>
    /// (<c>admin-client.md</c> §1), with Java's parameter shape.
    /// </summary>
    [Fact]
    public void IAdmin_TheThreeNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo describeLogDirs = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeLogDirs))!;
        MethodInfo alter = typeof(IAdmin).GetMethod(nameof(IAdmin.AlterReplicaLogDirs))!;
        MethodInfo describeReplicas = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeReplicaLogDirs))!;

        Assert.Equal(typeof(DescribeLogDirsResult), describeLogDirs.ReturnType);
        Assert.Equal(typeof(AlterReplicaLogDirsResult), alter.ReturnType);
        Assert.Equal(typeof(DescribeReplicaLogDirsResult), describeReplicas.ReturnType);

        // describeLogDirs(Collection<Integer>, DescribeLogDirsOptions) — Java's element type
        // is Integer, so the .NET element is the value type int, not a nullable or a string.
        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<int>), typeof(DescribeLogDirsOptions) },
            describeLogDirs.GetParameters().Select(parameter => parameter.ParameterType));

        // alterReplicaLogDirs(Map<TopicPartitionReplica, String>, AlterReplicaLogDirsOptions)
        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<TopicPartitionReplica, string>),
                typeof(AlterReplicaLogDirsOptions),
            },
            alter.GetParameters().Select(parameter => parameter.ParameterType));

        // describeReplicaLogDirs(Collection<TopicPartitionReplica>, DescribeReplicaLogDirsOptions)
        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyCollection<TopicPartitionReplica>),
                typeof(DescribeReplicaLogDirsOptions),
            },
            describeReplicas.GetParameters().Select(parameter => parameter.ParameterType));

        // The options parameter is optional and nullable on each; the required one is not.
        foreach (MethodInfo rpc in new[] { describeLogDirs, alter, describeReplicas })
        {
            ParameterInfo[] parameters = rpc.GetParameters();
            Assert.False(parameters[0].IsOptional, $"{rpc.Name}'s input must be required");
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(parameters[0]));
            Assert.True(parameters[1].IsOptional, $"{rpc.Name}'s options must be optional");
            Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(parameters[1]));
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
    /// ⚠ <b><see cref="DescribeLogDirsResult"/>'s two accessors are named for the
    /// DESCRIPTIONS, not <c>values</c>/<c>all</c></b> — Java names them
    /// <c>descriptions()</c> and <c>allDescriptions()</c>
    /// (<c>DescribeLogDirsResult.java:41, :50</c>) and has no others, so borrowing the
    /// sibling results' names would invent a surface Java does not have.
    /// </summary>
    [Fact]
    public void DescribeLogDirsResult_MatchesJavasTwoAccessors()
    {
        PropertyInfo descriptions =
            typeof(DescribeLogDirsResult).GetProperty(nameof(DescribeLogDirsResult.Descriptions))!;

        // ⚠ The key is a BARE int — Java's Map<Integer, …> — not a TopicPartition or a
        // string, and the value is a whole map per broker.
        Assert.Equal(
            typeof(IReadOnlyDictionary<int, Task<IReadOnlyDictionary<string, LogDirDescription>>>),
            descriptions.PropertyType);
        Assert.Null(descriptions.SetMethod);

        MethodInfo allDescriptions = typeof(DescribeLogDirsResult).GetMethod(
            nameof(DescribeLogDirsResult.AllDescriptions), Type.EmptyTypes)!;
        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>>),
            allDescriptions.ReturnType);

        // Java's constructor is package-private; nothing public builds one.
        Assert.Empty(typeof(DescribeLogDirsResult).GetConstructors());

        // ⚠ The WHOLE declared surface, named — so a stray `Values`/`All` borrowed from a
        // sibling result turns this red rather than merely changing a count.
        Assert.Equal(
            new[] { nameof(DescribeLogDirsResult.AllDescriptions) },
            DeclaredPublicMethodNames(typeof(DescribeLogDirsResult)));
        Assert.Equal(
            new[] { nameof(DescribeLogDirsResult.Descriptions) },
            PropertyNames(typeof(DescribeLogDirsResult)));
    }

    /// <summary>
    /// ⚠ <b>The two replica results differ exactly as Java's do</b>:
    /// <see cref="AlterReplicaLogDirsResult"/> is <c>KafkaFuture&lt;Void&gt;</c> per replica
    /// while <see cref="DescribeReplicaLogDirsResult"/> carries a value. Both shapes are
    /// pinned together, because that is where the confusion lives.
    /// </summary>
    [Fact]
    public void TheTwoReplicaResults_DifferExactlyAsJavasDo()
    {
        PropertyInfo alterValues =
            typeof(AlterReplicaLogDirsResult).GetProperty(nameof(AlterReplicaLogDirsResult.Values))!;

        // ⚠ Task, not Task<bool>: the void bridge's success token must not leak out.
        Assert.Equal(typeof(IReadOnlyDictionary<TopicPartitionReplica, Task>), alterValues.PropertyType);
        Assert.Null(alterValues.SetMethod);

        MethodInfo alterAll =
            typeof(AlterReplicaLogDirsResult).GetMethod(nameof(AlterReplicaLogDirsResult.All), Type.EmptyTypes)!;

        // ⚠ A BARE Task — this is the line that differs from the describe result.
        Assert.Equal(typeof(Task), alterAll.ReturnType);

        PropertyInfo describeValues =
            typeof(DescribeReplicaLogDirsResult).GetProperty(nameof(DescribeReplicaLogDirsResult.Values))!;
        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartitionReplica, Task<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>),
            describeValues.PropertyType);
        Assert.Null(describeValues.SetMethod);

        MethodInfo describeAll = typeof(DescribeReplicaLogDirsResult).GetMethod(
            nameof(DescribeReplicaLogDirsResult.All), Type.EmptyTypes)!;
        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<TopicPartitionReplica,
                DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>),
            describeAll.ReturnType);

        foreach (Type type in new[] { typeof(AlterReplicaLogDirsResult), typeof(DescribeReplicaLogDirsResult) })
        {
            // Java's constructors are package-private; nothing public builds one.
            Assert.Empty(type.GetConstructors());

            // The whole declared surface, named — Java's values() and all(), nothing else.
            Assert.Equal(new[] { "All" }, DeclaredPublicMethodNames(type));
            Assert.Equal(new[] { "Values" }, PropertyNames(type));
        }
    }

    /// <summary>
    /// ⚠ <b><c>ReplicaLogDirInfo</c> stays NESTED, and keeps Java's <c>Get*</c>
    /// prefixes</b> — decision D17.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Nesting is Java's (<c>DescribeReplicaLogDirsResult.java:64</c>), and nothing forces
    /// it out: the enclosing type has no member named <c>ReplicaLogDirInfo</c>, so there is
    /// no <c>CS0102</c> collision of the kind that moved <see cref="AlterConfigOpType"/> out
    /// of <see cref="AlterConfigOp"/>. That forcing condition is asserted rather than
    /// asserted-in-prose.
    /// </para>
    /// <para>
    /// ⚠ They are <b>methods</b> rather than properties — Java names them
    /// <c>getCurrentReplicaLogDir()</c> and friends where the rest of the admin API uses the
    /// bare <c>foo()</c> style, and D17 preserves that rather than silently normalising it.
    /// Turning them into <c>CurrentReplicaLogDir</c> properties would be a rename, not a
    /// casing adjustment. That this bean style stays confined to these four is not narrated
    /// but <b>asserted</b>, by
    /// <see cref="TheBeanAccessorStyle_IsConfinedToReplicaLogDirInfo"/>.
    /// </para>
    /// </remarks>
    [Fact]
    public void ReplicaLogDirInfo_IsNested_WithJavasBeanAccessors()
    {
        Type info = typeof(DescribeReplicaLogDirsResult.ReplicaLogDirInfo);

        Assert.Same(typeof(DescribeReplicaLogDirsResult), info.DeclaringType);
        Assert.Same(info, typeof(DescribeReplicaLogDirsResult).GetNestedType("ReplicaLogDirInfo"));

        // The forcing condition that would have moved it out, asserted: there is none.
        Assert.Null(typeof(DescribeReplicaLogDirsResult).GetProperty("ReplicaLogDirInfo"));
        Assert.Null(typeof(DescribeReplicaLogDirsResult).GetMethod("ReplicaLogDirInfo"));

        // ⚠ Methods, not properties — and no property may shadow one of them.
        Assert.Empty(info.GetProperties());
        Assert.Equal(
            new[]
            {
                "GetCurrentReplicaLogDir", "GetCurrentReplicaOffsetLag", "GetFutureReplicaLogDir",
                "GetFutureReplicaOffsetLag", "ToString",
            },
            DeclaredPublicMethodNames(info));

        // Java's package-private constructor; nothing public builds one.
        Assert.Empty(info.GetConstructors());

        // ⚠ Both directory accessors are genuinely nullable (Java :86-88, :101-103); both
        // lags are plain longs, and -1 there is a VALUE, not an absence marker.
        Assert.Equal(typeof(string), info.GetMethod("GetCurrentReplicaLogDir")!.ReturnType);
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(info.GetMethod("GetCurrentReplicaLogDir")!.ReturnParameter));
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(info.GetMethod("GetFutureReplicaLogDir")!.ReturnParameter));
        Assert.Equal(typeof(long), info.GetMethod("GetCurrentReplicaOffsetLag")!.ReturnType);
        Assert.Equal(typeof(long), info.GetMethod("GetFutureReplicaOffsetLag")!.ReturnType);
    }

    /// <summary>
    /// ⚠ <b>The Java-bean <c>Get*</c> style is confined to <c>ReplicaLogDirInfo</c>'s four
    /// accessors</b> — asserted over the whole exported surface, so D17's deviation stays a
    /// deviation instead of spreading.
    /// </summary>
    /// <remarks>
    /// The rest of the binding maps Java's <c>foo()</c> accessors to properties (decision
    /// D18), so a stray <c>GetFoo()</c> elsewhere would mean either a second, unrecorded
    /// deviation or a Java bean name copied by reflex.
    /// <para>
    /// ⚠ <b>Two kinds of <c>Get*</c> are excluded, and both structurally (decision D20) —
    /// never by listing their names.</b> <see cref="object.GetHashCode"/> and friends are
    /// excluded by asking whether the method's base definition is declared on
    /// <see cref="object"/>; <c>GetEnumerator</c> is excluded by asking whether the method
    /// <em>implements a member of a <c>System.*</c> interface</em>, read off the type's
    /// interface map. The second is the load-bearing one: those names are chosen by the
    /// framework contract, not by this binding, so they are not a naming decision at all.
    /// A <c>name != "GetEnumerator"</c> filter would have been the easy way to green, and it
    /// would also hide a genuine bean accessor that happened to be called that.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheBeanAccessorStyle_IsConfinedToReplicaLogDirInfo()
    {
        List<string> beanAccessors = new List<string>();
        foreach (Type type in typeof(IAdmin).Assembly.GetExportedTypes())
        {
            HashSet<MethodInfo> frameworkContract = FrameworkInterfaceImplementations(type);

            foreach (MethodInfo method in type.GetMethods(
                BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                if (method.IsSpecialName
                    || method.GetBaseDefinition().DeclaringType == typeof(object)
                    || frameworkContract.Contains(method))
                {
                    continue;
                }

                if (method.Name.Length > 3
                    && method.Name.StartsWith("Get", StringComparison.Ordinal)
                    && char.IsUpper(method.Name[3]))
                {
                    beanAccessors.Add($"{type.Name}.{method.Name}");
                }
            }
        }

        Assert.Equal(
            new[]
            {
                "ReplicaLogDirInfo.GetCurrentReplicaLogDir",
                "ReplicaLogDirInfo.GetCurrentReplicaOffsetLag",
                "ReplicaLogDirInfo.GetFutureReplicaLogDir",
                "ReplicaLogDirInfo.GetFutureReplicaOffsetLag",
            },
            beanAccessors.OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// The type's methods that exist to satisfy a <c>System.*</c> interface — their names
    /// are the framework's choice, not this binding's.
    /// </summary>
    /// <param name="type">The type to inspect.</param>
    /// <returns>The implementing methods.</returns>
    private static HashSet<MethodInfo> FrameworkInterfaceImplementations(Type type)
    {
        HashSet<MethodInfo> implementations = new HashSet<MethodInfo>();
        if (type.IsInterface)
        {
            return implementations;
        }

        foreach (Type contract in type.GetInterfaces())
        {
            if (contract.Namespace is null
                || !contract.Namespace.StartsWith("System", StringComparison.Ordinal))
            {
                continue;
            }

            foreach (MethodInfo target in type.GetInterfaceMap(contract).TargetMethods)
            {
                implementations.Add(target);
            }
        }

        return implementations;
    }

    /// <summary>
    /// ⚠⚠ <b><c>IsCordoned</c> IS ABSENT, and this test is what keeps it absent</b>
    /// (decision D15, §15 Gap 1).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java has <c>isCordoned()</c> (<c>LogDirDescription.java:94</c>) and the Rust core
    /// implements it, but the C ABI exports no accessor for it, so the binding cannot know
    /// the value. A stubbed <c>false</c> would assert <em>"this log directory is not
    /// cordoned"</em> when the truth is <em>"the binding cannot know"</em> — and a wrong
    /// answer is worse than an absent one, because a caller cannot tell it apart from a real
    /// one. <b>Absence is the honest encoding.</b>
    /// </para>
    /// <para>
    /// ⚠ <b>The absence has to be pinned, because the natural "fix" is to add the fake.</b>
    /// A later phase reading <c>definition-of-done.md</c> §2 ("are all methods implemented?")
    /// would see a missing Java member and complete it with the only value available. This
    /// turns that red.
    /// </para>
    /// <para>
    /// ⚠ <b>The sweep carries its own positive control.</b> An absence assertion over a
    /// member walk is vacuously true if the walk finds nothing at all, so
    /// <see cref="LogDirDescription.TotalBytes"/> is asserted to be found by the <em>same</em>
    /// walk. Without that, deleting the type's members would leave this test green.
    /// </para>
    /// <para>
    /// ⚠ <b>The constructor is <see langword="internal"/> although Java's three are
    /// public</b>, and that is part of the same decision rather than an oversight: Java's
    /// two shorter constructors (<c>:38</c>, <c>:42</c>) default <c>isCordoned</c> to false,
    /// so a public C# constructor would either take a parameter the type cannot store or
    /// silently bake in the fake this decision exists to avoid. Only the result marshaller
    /// builds one.
    /// </para>
    /// </remarks>
    [Fact]
    public void LogDirDescription_HasNoCordonedMember_AndTheSweepThatProvesItFindsTheOthers()
    {
        const BindingFlags Everything =
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static
            | BindingFlags.DeclaredOnly;

        string[] members = typeof(LogDirDescription).GetMembers(Everything)
            .Select(member => member.Name)
            .ToArray();

        // ⚠ THE ASSERTION THIS TEST EXISTS FOR — nothing named for the flag, in any casing,
        // at any accessibility, including a backing field.
        Assert.DoesNotContain(
            members,
            name => name.IndexOf("Cordon", StringComparison.OrdinalIgnoreCase) >= 0);

        // The positive control: the same walk does find the members that are there, so the
        // assertion above is about absence rather than about an empty walk.
        Assert.Contains(members, name => string.Equals(name, nameof(LogDirDescription.TotalBytes), StringComparison.Ordinal));
        Assert.Contains(members, name => string.Equals(name, nameof(LogDirDescription.Error), StringComparison.Ordinal));

        // Nor may it reappear anywhere else on the exported surface under another owner.
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("Cordon", StringComparison.OrdinalIgnoreCase) >= 0);

        // The constructor's accessibility is part of the same decision — see the remarks.
        Assert.Empty(typeof(LogDirDescription).GetConstructors());
    }

    /// <summary>
    /// The three copied-out value types expose Java's accessors as <b>properties</b>
    /// (decision D18) — each is a pure managed field read that does no P/Invoke and cannot
    /// throw, which is CLAUDE.md §3's "non-blocking getter → sync property" row.
    /// </summary>
    /// <remarks>
    /// ⚠ The one deliberate exception in this stage is
    /// <c>ReplicaLogDirInfo</c>'s four <c>Get*</c> methods (D17), pinned separately by
    /// <see cref="ReplicaLogDirInfo_IsNested_WithJavasBeanAccessors"/>. Two conventions in
    /// one stage is intentional, and each is asserted where it applies.
    /// </remarks>
    [Fact]
    public void TheValueTypes_ExposeJavasAccessorsAsProperties()
    {
        Assert.Equal(
            new[] { "BrokerId", "Partition", "Topic" },
            PropertyNames(typeof(TopicPartitionReplica)));
        Assert.Equal(typeof(string), typeof(TopicPartitionReplica).GetProperty("Topic")!.PropertyType);
        Assert.Equal(typeof(int), typeof(TopicPartitionReplica).GetProperty("Partition")!.PropertyType);
        Assert.Equal(typeof(int), typeof(TopicPartitionReplica).GetProperty("BrokerId")!.PropertyType);

        Assert.Equal(new[] { "IsFuture", "OffsetLag", "Size" }, PropertyNames(typeof(ReplicaInfo)));
        Assert.Equal(typeof(long), typeof(ReplicaInfo).GetProperty("Size")!.PropertyType);
        Assert.Equal(typeof(long), typeof(ReplicaInfo).GetProperty("OffsetLag")!.PropertyType);
        Assert.Equal(typeof(bool), typeof(ReplicaInfo).GetProperty("IsFuture")!.PropertyType);

        Assert.Equal(
            new[] { "Error", "ReplicaInfos", "TotalBytes", "UsableBytes" },
            PropertyNames(typeof(LogDirDescription)));

        // ⚠ Java's OptionalLong has no netstandard2.0 equivalent, so the mapping is long? —
        // and the ABI's -1 becomes null. A plain long would erase the empty case.
        Assert.Equal(typeof(long?), typeof(LogDirDescription).GetProperty("TotalBytes")!.PropertyType);
        Assert.Equal(typeof(long?), typeof(LogDirDescription).GetProperty("UsableBytes")!.PropertyType);

        // Java's error() is an ApiException, mapped to this binding's flat KafkaException
        // (ffi §A5) and nullable, because a healthy directory has none.
        Assert.Equal(typeof(KafkaException), typeof(LogDirDescription).GetProperty("Error")!.PropertyType);
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(typeof(LogDirDescription).GetProperty("Error")!));

        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartition, ReplicaInfo>),
            typeof(LogDirDescription).GetProperty("ReplicaInfos")!.PropertyType);

        // Every one is read-only: Java's are accessors over final fields.
        foreach (Type type in new[] { typeof(TopicPartitionReplica), typeof(ReplicaInfo), typeof(LogDirDescription) })
        {
            Assert.All(type.GetProperties(), property => Assert.Null(property.SetMethod));
        }
    }

    /// <summary>
    /// ⚠ <b><see cref="ReplicaInfo"/>'s constructor is PUBLIC and
    /// <see cref="LogDirDescription"/>'s is not</b>, and the split follows Java plus D15
    /// rather than a house style.
    /// </summary>
    [Fact]
    public void TheConstructorVisibility_FollowsJavaExceptWhereD15Intervenes()
    {
        // Java's ReplicaInfo(long, long, boolean) is public (ReplicaInfo.java:28), and this
        // type can carry every field it takes.
        ConstructorInfo replicaInfo = Assert.Single(typeof(ReplicaInfo).GetConstructors());
        Assert.Equal(
            new[] { typeof(long), typeof(long), typeof(bool) },
            replicaInfo.GetParameters().Select(parameter => parameter.ParameterType));

        // Java's TopicPartitionReplica(String, int, int) is public (:33).
        ConstructorInfo replica = Assert.Single(typeof(TopicPartitionReplica).GetConstructors());
        Assert.Equal(
            new[] { typeof(string), typeof(int), typeof(int) },
            replica.GetParameters().Select(parameter => parameter.ParameterType));

        // …and LogDirDescription's is internal — see the D15 test's remarks.
        Assert.Empty(typeof(LogDirDescription).GetConstructors());
    }

    /// <summary>
    /// The three new options types match Java's fields and defaults exactly — each is an
    /// empty <c>AbstractOptions</c> subclass in Java, so the inherited timeout is the whole
    /// surface.
    /// </summary>
    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        foreach (Type type in new[]
                 {
                     typeof(DescribeLogDirsOptions),
                     typeof(AlterReplicaLogDirsOptions),
                     typeof(DescribeReplicaLogDirsOptions),
                 })
        {
            Assert.Equal(new[] { "TimeoutMs" }, PropertyNames(type));

            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);

            // Java's default is "use the client default", which is null here rather than 0.
            Assert.Null((int?)timeout.GetValue(Activator.CreateInstance(type)));
        }
    }

    /// <summary>
    /// ⚠ <b><see cref="TopicPartitionReplica"/> sits at the ROOT namespace</b> — Java's
    /// package is <c>org.apache.kafka.common</c>, not <c>clients.admin</c> (decision D13) —
    /// while everything else this stage adds is under <c>Confluent.Kafka.Admin</c>.
    /// </summary>
    [Fact]
    public void TheNamespaceSplit_FollowsJavasPackages()
    {
        Assert.Equal("Confluent.Kafka", typeof(TopicPartitionReplica).Namespace);

        foreach (Type type in new[]
                 {
                     typeof(LogDirDescription), typeof(ReplicaInfo), typeof(DescribeLogDirsResult),
                     typeof(AlterReplicaLogDirsResult), typeof(DescribeReplicaLogDirsResult),
                     typeof(DescribeReplicaLogDirsResult.ReplicaLogDirInfo), typeof(DescribeLogDirsOptions),
                     typeof(AlterReplicaLogDirsOptions), typeof(DescribeReplicaLogDirsOptions),
                 })
        {
            Assert.Equal("Confluent.Kafka.Admin", type.Namespace);
        }
    }

    /// <summary>
    /// ⚠ <b>No <c>ReplicaInfo_t</c>-shaped handle type leaked into the public surface</b>,
    /// and no <c>LogDirDescriptionMap</c> type was invented for the ABI's intermediate
    /// handle. Java has neither: the map is a plain <c>Map&lt;String, LogDirDescription&gt;</c>.
    /// </summary>
    [Fact]
    public void NoAbiIntermediateType_LeakedIntoThePublicSurface()
    {
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("LogDirDescriptionMap", StringComparison.Ordinal) >= 0);
    }

    /// <summary>
    /// The type's own public instance methods — <see cref="BindingFlags.DeclaredOnly"/>, so
    /// <see cref="object"/>'s inherited members do not count, and without the compiler's
    /// property accessors, which are methods too.
    /// </summary>
    /// <remarks>
    /// ⚠ The accessors are excluded <b>structurally</b>, by
    /// <see cref="MethodBase.IsSpecialName"/> — the flag the compiler stamps on
    /// <c>get_</c>/<c>set_</c> accessors and operators — and never by a name predicate
    /// (decision D20). A <c>StartsWith("get_")</c> filter would also swallow a real Java
    /// accessor: <c>ReplicaLogDirInfo</c>'s four <c>Get*</c> methods are exactly the shape
    /// such a filter mis-reads.
    /// </remarks>
    private static string[] DeclaredPublicMethodNames(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => !method.IsSpecialName)
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    private static string[] PropertyNames(Type type) =>
        type.GetProperties().Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal).ToArray();
}
